"""Backend: runs on cloud virtual machines, managed by Ray."""
from collections.abc import Iterable
import contextlib
import copy
import dataclasses
import enum
import json
import math
import os
import pathlib
import random
import re
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import typing
from typing import Any, Optional

import colorama
import psutil

from sky import backends
from sky import check as sky_check
from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky import jobs as managed_jobs
from sky import optimizer
from sky import provision as provision_lib
from sky import resources as resources_lib
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_file_sync
from sky.backends import skylet_client
from sky.backends import task_codegen
from sky.backends import wheel_utils
from sky.clouds import cloud as sky_cloud
from sky.clouds import kubernetes as k8s_cloud
from sky.clouds.utils import gcp_utils
from sky.container_images import consumers as container_image_consumers
from sky.container_images import errors as container_image_errors
from sky.container_images import placement as container_image_placement
from sky.container_images import runtime as container_image_runtime
from sky.dag import DEFAULT_EXECUTION
from sky.data import storage as storage_lib
from sky.provision import capacity_cache
from sky.provision import capacity_policy
from sky.provision import common as provision_common
from sky.provision import constants as provision_constants
from sky.provision import failover_error_policy
from sky.provision import instance_setup
from sky.provision import metadata_utils
from sky.provision import provisioner
from sky.provision.kubernetes import config as config_lib
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.provision.slurm import utils as slurm_utils
from sky.serve import constants as serve_constants
from sky.server import common as server_common
from sky.server.requests import requests as requests_lib
from sky.skylet import autostop_lib
from sky.skylet import constants
from sky.skylet import job_lib
from sky.skylet import log_lib
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import cluster_utils
from sky.utils import command_runner
from sky.utils import common
from sky.utils import common_utils
from sky.utils import context_utils
from sky.utils import controller_utils
from sky.utils import directory_utils
from sky.utils import env_options
from sky.utils import lock_events
from sky.utils import locks
from sky.utils import log_utils
from sky.utils import message_utils
from sky.utils import operator_notifications
from sky.utils import registry
from sky.utils import resources_utils
from sky.utils import rich_utils
from sky.utils import status_lib
from sky.utils import subprocess_utils
from sky.utils import timeline
from sky.utils import ux_utils
from sky.utils import volume as volume_lib
from sky.utils import yaml_utils
from sky.utils.plugin_extensions import ExternalFailureSource

metrics_utils = adaptors_common.LazyImport('sky.metrics.utils')
serve_placement_history = adaptors_common.LazyImport(
    'sky.serve.placement_history')

if typing.TYPE_CHECKING:
    import grpc

    from sky import dag
    from sky.schemas.generated import autostopv1_pb2
    from sky.schemas.generated import healthv1_pb2
    from sky.schemas.generated import jobsv1_pb2
    from sky.schemas.generated import managed_jobsv1_pb2
    from sky.schemas.generated import servev1_pb2
else:
    # To avoid requiring grpcio to be installed on the client side.
    grpc = adaptors_common.LazyImport(
        'grpc',
        # https://github.com/grpc/grpc/issues/37642 to avoid spam in console
        set_loggers=lambda: os.environ.update({'GRPC_VERBOSITY': 'NONE'})
        if not env_options.Options.SHOW_DEBUG_INFO.get() else None)
    autostopv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.autostopv1_pb2')
    healthv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.healthv1_pb2')
    jobsv1_pb2 = adaptors_common.LazyImport('sky.schemas.generated.jobsv1_pb2')
    servev1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.servev1_pb2')
    managed_jobsv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.managed_jobsv1_pb2')

Path = str

SKY_REMOTE_APP_DIR = backend_utils.SKY_REMOTE_APP_DIR
SKY_REMOTE_WORKDIR = constants.SKY_REMOTE_WORKDIR

logger = sky_logging.init_logger(__name__)


def _resolve_container_image_for_placement(
    resources: resources_lib.Resources,
    *,
    consumer_kind: str,
    consumer_owner: str,
    controller_epoch: str,
    controller_sequence: int | None,
    allow_epoch_advance: bool,
    consumer_metadata: dict[str, Any],
    ensure: bool = True,
) -> resources_lib.Resources:
    """Pins a managed image after optimization and before provisioning."""
    if resources.container_image is None:
        return resources
    workspace = (skypilot_config.get_active_workspace() or
                 constants.SKYPILOT_DEFAULT_WORKSPACE)
    try:
        placement = container_image_placement.classify(resources, workspace)
        return container_image_runtime.resolve_for_placement(
            resources,
            placement,
            workspace=workspace,
            consumer_kind=consumer_kind,
            consumer_owner=consumer_owner,
            controller_epoch=controller_epoch,
            controller_sequence=controller_sequence,
            allow_epoch_advance=allow_epoch_advance,
            consumer_metadata=consumer_metadata,
            ensure=ensure)
    except container_image_runtime.ContainerImageWarmingError as e:
        safe_error = container_image_errors.from_exception(e)
        raise exceptions.ResourcesUnavailableError(str(safe_error),
                                                   no_failover=True) from e
    except container_image_runtime.ContainerImagePreparationFailedError as e:
        safe_error = container_image_errors.from_exception(e)
        raise exceptions.ResourcesUnavailableError(str(safe_error),
                                                   no_failover=False) from e
    except Exception as e:  # pylint: disable=broad-except
        # Catalog, profile, and policy errors are not capacity failures. A
        # different placement cannot repair them, so fail without cycling the
        # fleet through every candidate. Convert at this boundary so provider,
        # registry, and credential values never enter request state or logs.
        safe_error = container_image_errors.from_exception(e)
        raise exceptions.ResourcesUnavailableError(str(safe_error),
                                                   no_failover=True) from e


# Timeout (seconds) for provision progress: if in this duration no new nodes
# are launched, abort and failover.
_NODES_LAUNCHING_PROGRESS_TIMEOUT = {
    clouds.AWS: 90,
    clouds.Azure: 90,
    clouds.GCP: 240,
    clouds.Lambda: 300,
    clouds.IBM: 160,
    clouds.OCI: 300,
    clouds.Paperspace: 600,
    clouds.Kubernetes: 300,
    clouds.Shadeform: 300,
    clouds.Vsphere: 240,
}

# Time gap between retries after failing to provision in all possible places.
# Used only if --retry-until-up is set.
_RETRY_UNTIL_UP_INIT_GAP_SECONDS = 30

# The maximum retry count for fetching IP address.
_FETCH_IP_MAX_ATTEMPTS = 3

# How many times to query the cloud provider to make sure instances are
# stopping/terminating, and how long to wait between each query.
_TEARDOWN_WAIT_MAX_ATTEMPTS = 10
_TEARDOWN_WAIT_BETWEEN_ATTEMPS_SECONDS = 1

_TEARDOWN_FAILURE_MESSAGE = (
    f'\n{colorama.Fore.RED}Failed to terminate '
    '{cluster_name}. {extra_reason}'
    'If you want to ignore this error and remove the cluster '
    'from the status table, use `sky down --purge`.'
    f'{colorama.Style.RESET_ALL}\n'
    '**** STDOUT ****\n'
    '{stdout}\n'
    '**** STDERR ****\n'
    '{stderr}')

_TEARDOWN_PURGE_WARNING = (
    f'{colorama.Fore.YELLOW}'
    'WARNING: Received non-zero exit code from {reason}. '
    'Make sure resources are manually deleted.\n'
    'Details: {details}'
    f'{colorama.Style.RESET_ALL}')

_TPU_NOT_FOUND_ERROR = 'ERROR: (gcloud.compute.tpus.delete) NOT_FOUND'

_MAX_RAY_UP_RETRY = 5

# Number of retries for getting zones.
_MAX_GET_ZONE_RETRY = 3

_JOB_ID_SSM_RECONNECT_MAX_ATTEMPTS = 6
_JOB_ID_SSM_RECONNECT_INITIAL_BACKOFF_SECONDS = 1
_JOB_ID_SSM_RECONNECT_MAX_BACKOFF_SECONDS = 8

_JOB_ID_PATTERN = re.compile(r'Job ID: ([0-9]+)')
_JOB_IDS_PATTERN = re.compile(r'Job IDs: ([0-9,]+)')
_LOG_DIR_PATTERN = re.compile(r'Log Dir: ([^ ]+)')

# Path to the monkey-patched ray up script.
# We don't do import then __file__ because that script needs to be filled in
# (so import would fail).
_RAY_UP_WITH_MONKEY_PATCHED_HASH_LAUNCH_CONF_PATH = (
    pathlib.Path(directory_utils.get_sky_dir()) / 'backends' /
    'monkey_patches' / 'monkey_patch_ray_up.py')

_EXCEPTION_MSG_AND_RETURNCODE_FOR_DUMP_INLINE_SCRIPT = [
    ('too long', 255),
    ('request-uri too large', 1),
    ('request header fields too large', 1),
    ('400 bad request', 1),  # CloudFlare 400 error
]

_RESOURCES_UNAVAILABLE_LOG = (
    'Reasons for provision failures (for details, please check the log above):')


def _get_kubernetes_hint(reason: str, context: str | None = None) -> str | None:
    """Return a hint for the given Kubernetes failure reason, or None.

    Sources the canonical hint table from
    ``kubernetes_utils.KUBERNETES_FAILURE_HINTS``. Hints may contain a literal
    `{dashboard_url}` token, which is replaced with the SkyPilot dashboard
    infra page URL — scoped to the failing context when one is available. If
    URL resolution fails for any reason, the token is replaced with a generic
    fallback so we never raise from failure-rendering code (which would mask
    the original provision error).

    Only called from the Kubernetes branch of
    ``_format_provision_failure_blocks`` to avoid false positives on other
    clouds' error messages (e.g., AWS "InsufficientInstanceCapacity").
    """
    hint = kubernetes_utils.match_kubernetes_failure_hint(reason)
    if hint is None:
        return None
    if '{dashboard_url}' in hint:
        try:
            starting_page = (f'infra/{context}' if context else 'infra')
            dashboard_url = server_common.get_dashboard_url(
                server_common.get_server_url(), starting_page=starting_page)
        except Exception:  # pylint: disable=broad-except
            dashboard_url = 'the SkyPilot dashboard infra page'
        hint = hint.replace('{dashboard_url}', dashboard_url)
    return hint


def _format_provision_failure_blocks(
    resource_exceptions: dict['resources_lib.Resources', Exception],) -> str:
    """Format provision failures as blocks instead of a table."""
    num_infra = len(resource_exceptions)
    lines = [f'Provision failures (tried {num_infra} infra):\n']
    for resource, exception in resource_exceptions.items():
        infra = resource.infra.formatted_str()
        resource_str = resources_utils.format_resource(resource,
                                                       simplified_only=True)[0]
        reason = str(exception)
        lines.append(f'\u2717 {infra} \u2014 {resource_str}')
        lines.append(textwrap.indent(reason, '  '))
        if isinstance(resource.cloud, clouds.Kubernetes):
            hint = _get_kubernetes_hint(reason, context=resource.region)
            if hint:
                lines.append(f'  Hint: {hint}')
        lines.append('')
    return '\n'.join(lines).rstrip() + '\n'


# Number of seconds to wait locking the cluster before communicating with user.
_CLUSTER_LOCK_TIMEOUT = 5.0


def _is_message_too_long(returncode: int,
                         output: str | None = None,
                         file_path: str | None = None) -> bool:
    """Check if the message sent to the remote is too long.

    We use inline script to run the setup or run command, i.e. the script will
    be part of the message sent to the remote cluster. There is a chance that
    the command is too long, when people has very long run or setup commands, or
    there is a cloudflare proxy in front of the remote blocking the long
    message. Several common causes are:
    - SSH returning: `too long` in the error message.
    - Cloudflare proxy returning: `414 Request-URI Too Large` or
      `431 Request Header Fields Too Large` error.

    We use a general length limit check before but it could be inaccurate on
    some systems, e.g. cloudflare proxy, so this is necessary.

    Args:
        returncode: The return code of the setup command.
        output: The output of the setup command.
        file_path: The path to the setup log file.
    """
    assert (output is None) != (file_path is None), (
        'Either output or file_path must be provided.', output, file_path)
    to_check = []
    for (match_str,
         desired_rc) in _EXCEPTION_MSG_AND_RETURNCODE_FOR_DUMP_INLINE_SCRIPT:
        if desired_rc == returncode:
            to_check.append(match_str)
    if not to_check:
        return False

    def _check_output_for_match_str(output: str) -> bool:
        for match_str in to_check:
            if match_str.lower() in output.lower():
                return True
        return False

    if file_path is not None:
        try:
            with open(os.path.expanduser(file_path), encoding='utf-8') as f:
                content = f.read()
                return _check_output_for_match_str(content)
        except Exception as e:  # pylint: disable=broad-except
            # We don't crash the setup if we cannot read the log file.
            # Instead, we should retry the setup with dumping the script
            # to a file to be safe.
            logger.debug(f'Failed to read setup log file {file_path}: {e}')
            return True
    else:
        assert output is not None, (output, file_path)
        return _check_output_for_match_str(output)


def _get_cluster_config_template(cloud):
    cloud_to_template = {
        clouds.AWS: 'aws-ray.yml.j2',
        clouds.Azure: 'azure-ray.yml.j2',
        clouds.Cudo: 'cudo-ray.yml.j2',
        clouds.GCP: 'gcp-ray.yml.j2',
        clouds.Lambda: 'lambda-ray.yml.j2',
        clouds.IBM: 'ibm-ray.yml.j2',
        clouds.SCP: 'scp-ray.yml.j2',
        clouds.Slurm: 'slurm-ray.yml.j2',
        clouds.OCI: 'oci-ray.yml.j2',
        clouds.Paperspace: 'paperspace-ray.yml.j2',
        clouds.PrimeIntellect: 'primeintellect-ray.yml.j2',
        clouds.DO: 'do-ray.yml.j2',
        clouds.RunPod: 'runpod-ray.yml.j2',
        clouds.Kubernetes: 'kubernetes-ray.yml.j2',
        clouds.SSH: 'kubernetes-ray.yml.j2',
        clouds.Shadeform: 'shadeform-ray.yml.j2',
        clouds.Vsphere: 'vsphere-ray.yml.j2',
        clouds.Vast: 'vast-ray.yml.j2',
        clouds.Fluidstack: 'fluidstack-ray.yml.j2',
        clouds.Nebius: 'nebius-ray.yml.j2',
        clouds.Hyperbolic: 'hyperbolic-ray.yml.j2',
        clouds.Seeweb: 'seeweb-ray.yml.j2',
        clouds.Yotta: 'yotta-ray.yml.j2',
        clouds.Mithril: 'mithril-ray.yml.j2',
        clouds.Verda: 'verda-ray.yml.j2',
    }
    return cloud_to_template[type(cloud)]


def write_ray_up_script_with_patched_launch_hash_fn(
    cluster_config_path: str | None,
    ray_up_kwargs: dict[str, bool],
) -> str:
    """Writes a Python script that runs `ray up` with our launch hash func.

    Our patched launch hash has one difference from the non-patched version: it
    does not include any `ssh_proxy_command` under `auth` as part of the hash
    calculation.
    """
    with open(_RAY_UP_WITH_MONKEY_PATCHED_HASH_LAUNCH_CONF_PATH,
              encoding='utf-8') as f:
        ray_up_no_restart_script = f.read().format(
            ray_yaml_path=repr(cluster_config_path),
            ray_up_kwargs=ray_up_kwargs)
    with tempfile.NamedTemporaryFile('w',
                                     prefix='skypilot_ray_up_',
                                     suffix='.py',
                                     delete=False) as f:
        f.write(ray_up_no_restart_script)
        logger.debug(f'`ray up` script: {f.name}')
    return f.name


class GangSchedulingStatus(enum.Enum):
    """Enum for gang scheduling status."""
    CLUSTER_READY = 0
    GANG_FAILED = 1
    HEAD_FAILED = 2


class _ResourcesFeaturesUnsupportedError(Exception):
    """Internal marker: `to_provision` cannot satisfy the requested features.

    Raised (chained onto the original NotSupportedError) inside the
    provision retry loop so the failure is handled per-RESOURCE -- the
    loop tail blocks exactly the failing candidate -- instead of the
    broad NotSupportedError handler's cloud-wide block, which is only
    correct for genuinely cloud-global failures.
    """


# Direct aliases preserve historical imports, monkeypatching, and pickle lookup
# while provider failure policy is owned by the provision package.
_RSYNC_NOT_FOUND_MESSAGE = (failover_error_policy._RSYNC_NOT_FOUND_MESSAGE)  # pylint: disable=protected-access
_add_to_blocked_resources = (failover_error_policy._add_to_blocked_resources)  # pylint: disable=protected-access
FailoverCloudErrorHandlerV1 = (
    failover_error_policy.FailoverCloudErrorHandlerV1)
FailoverCloudErrorHandlerV2 = (
    failover_error_policy.FailoverCloudErrorHandlerV2)


def _record_capacity_metric(reason: str, action: str) -> None:
    try:
        if metrics_utils.METRICS_ENABLED:
            metrics_utils.SKY_PROVISION_CAPACITY_EVENTS_TOTAL.labels(
                reason=reason, action=action).inc()
    except Exception as e:  # pylint: disable=broad-except
        # Observability must never alter the provisioning result.
        logger.debug('Capacity metric update failed: '
                     f'{common_utils.format_exception(e)}')


# Direct aliases preserve the historical backend import and pickle identities
# while capacity policy is owned by the provision package.
_CAPACITY_ERROR_CODES = capacity_policy._CAPACITY_ERROR_CODES  # pylint: disable=protected-access
_QUOTA_ERROR_CODES = capacity_policy._QUOTA_ERROR_CODES  # pylint: disable=protected-access
_PROVIDER_QUOTA_ERROR_CODES = capacity_policy._PROVIDER_QUOTA_ERROR_CODES  # pylint: disable=protected-access
_PLACEMENT_CAPACITY_ERROR_CODES = capacity_policy._PLACEMENT_CAPACITY_ERROR_CODES  # pylint: disable=protected-access
_NEUTRAL_PLACEMENT_ERROR_CODES = capacity_policy._NEUTRAL_PLACEMENT_ERROR_CODES  # pylint: disable=protected-access
_GCP_CAPACITY_ERROR_CODES = capacity_policy._GCP_CAPACITY_ERROR_CODES  # pylint: disable=protected-access
_GCP_QUOTA_ERROR_CODES = capacity_policy._GCP_QUOTA_ERROR_CODES  # pylint: disable=protected-access
_MAX_TERMINAL_FAILOVER_HISTORY_DEPTH = (
    capacity_policy._MAX_TERMINAL_FAILOVER_HISTORY_DEPTH)  # pylint: disable=protected-access
_MAX_TERMINAL_FAILOVER_HISTORY_NODES = (
    capacity_policy._MAX_TERMINAL_FAILOVER_HISTORY_NODES)  # pylint: disable=protected-access
_GCP_IDENTITY_PROJECT_RE = capacity_policy._GCP_IDENTITY_PROJECT_RE  # pylint: disable=protected-access
_iter_error_chain = capacity_policy._iter_error_chain  # pylint: disable=protected-access
_provider_error_codes = capacity_policy._provider_error_codes  # pylint: disable=protected-access
_classify_capacity_error = capacity_policy._classify_capacity_error  # pylint: disable=protected-access
_terminal_failover_leaves = capacity_policy._terminal_failover_leaves  # pylint: disable=protected-access
_terminal_leaf_cause_nodes = capacity_policy._terminal_leaf_cause_nodes  # pylint: disable=protected-access
classify_resources_unavailable_error = (
    capacity_policy.classify_resources_unavailable_error)
_is_quota_error = capacity_policy._is_quota_error  # pylint: disable=protected-access
_canonical_accelerators = capacity_policy._canonical_accelerators  # pylint: disable=protected-access
_capacity_cache_cloud_name = capacity_policy._capacity_cache_cloud_name  # pylint: disable=protected-access
_capacity_cache_account = capacity_policy._capacity_cache_account  # pylint: disable=protected-access
_capacity_cache_key = capacity_policy._capacity_cache_key  # pylint: disable=protected-access
_quota_cooldown_key = capacity_policy._quota_cooldown_key  # pylint: disable=protected-access
_fully_created_fresh_demand = capacity_policy._fully_created_fresh_demand  # pylint: disable=protected-access
_failure_requested_full_demand = capacity_policy._failure_requested_full_demand  # pylint: disable=protected-access
_placement_error_code = capacity_policy._placement_error_code  # pylint: disable=protected-access
_placement_outcome = capacity_policy._placement_outcome  # pylint: disable=protected-access

for _capacity_policy_symbol in (
        _iter_error_chain,
        _provider_error_codes,
        _classify_capacity_error,
        _terminal_failover_leaves,
        _terminal_leaf_cause_nodes,
        classify_resources_unavailable_error,
        _is_quota_error,
        _canonical_accelerators,
        _capacity_cache_cloud_name,
        _capacity_cache_account,
        _capacity_cache_key,
        _quota_cooldown_key,
        _fully_created_fresh_demand,
        _failure_requested_full_demand,
        _placement_error_code,
        _placement_outcome,
):
    _capacity_policy_symbol.__module__ = __name__
del _capacity_policy_symbol


def _record_insufficient_quota_notification(
        resources: 'resources_lib.Resources') -> bool:
    """Record actionable quota context without including workload identity."""
    try:
        resource_description = resources.instance_type
        if resource_description is None and resources.accelerators:
            resource_description = ', '.join(
                f'{name}:{count}'
                for name, count in sorted(resources.accelerators.items()))
        if resource_description is None:
            resource_description = 'the requested resources'
        purchase_option = 'spot' if resources.use_spot else 'on-demand'
        region = resources.region or 'an unspecified region'
        message = (f'Insufficient {resources.cloud} quota for '
                   f'{resource_description} {purchase_option} capacity in '
                   f'{region}. Request a quota increase or choose different '
                   'resources.')
        return operator_notifications.record_notification(
            operator_notifications.OperatorNotificationCategory.
            INSUFFICIENT_QUOTA,
            message,
            dedupe_window_seconds=operator_notifications.
            INSUFFICIENT_QUOTA_DEDUPE_WINDOW_SECONDS)
    except Exception as e:  # pylint: disable=broad-except
        # Operator observability must never alter provisioning behavior.
        logger.debug('Operator notification failed: '
                     f'{common_utils.format_exception(e)}')
        return False


def _capacity_cache_exhausted_zone_names(
        to_provision: 'resources_lib.Resources', region: 'clouds.Region',
        zones: list['clouds.Zone'] | None, num_nodes: int,
        account: str | None) -> set[str]:
    """Returns the attempted zone when its short-lived hint is active."""
    key = _capacity_cache_key(to_provision, region, zones, num_nodes, account)
    if key is None:
        return set()
    try:
        active = capacity_cache.active_exhausted_keys([key])
    except Exception as e:  # pylint: disable=broad-except
        _record_capacity_metric('capacity', 'cache_error')
        logger.debug('Capacity-cache read failed: '
                     f'{common_utils.format_exception(e)}')
        return set()
    _record_capacity_metric('capacity', 'hit' if active else 'miss')
    return {active_key.zone for active_key in active}


def _quota_cooldown_is_active(
        key: Optional['capacity_cache.QuotaCooldownKey']) -> bool:
    if key is None:
        return False
    try:
        active = capacity_cache.is_quota_cooldown_active(key)
    except Exception as e:  # pylint: disable=broad-except
        _record_capacity_metric('quota', 'cache_error')
        logger.debug('Quota-cooldown read failed: '
                     f'{common_utils.format_exception(e)}')
        return False
    _record_capacity_metric('quota', 'hit' if active else 'miss')
    return active


def _get_workload_attribution(
        task: task_lib.Task,
        cluster_name: str,
        workload_type: str,
        launch_context: dict[str, Any] | None = None) -> tuple[str, int | None]:
    """Returns scalar workload attribution without any external lookup."""
    workload_id = cluster_name
    workload_task_id = None
    task_envs = task.envs or {}
    if workload_type in ('service', 'pool'):
        service_name = (launch_context or {}).get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
        if isinstance(service_name, str) and service_name:
            service_version = (launch_context or {}).get(
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY)
            if type(service_version) is int and service_version > 0:
                workload_task_id = service_version
            return service_name, workload_task_id
        replica_id = task_envs.get(serve_constants.REPLICA_ID_ENV_VAR)
        replica_suffix = f'-{replica_id}' if replica_id is not None else None
        if replica_suffix and cluster_name.endswith(replica_suffix):
            workload_id = cluster_name[:-len(replica_suffix)]
        return workload_id, workload_task_id
    if workload_type != 'managed_job':
        return workload_id, workload_task_id

    managed_job_id = task_envs.get(constants.MANAGED_JOB_ID_ENV_VAR)
    if managed_job_id:
        workload_id = str(managed_job_id)

    global_task_id = task_envs.get(constants.TASK_ID_ENV_VAR, '')
    task_id_match = re.search(r'-(\d+)$', global_task_id)
    if task_id_match is not None:
        workload_task_id = int(task_id_match.group(1))
    return workload_id, workload_task_id


def _get_image_demand_attribution(
    task: task_lib.Task, cluster_name: str, workload_type: str,
    launch_context: dict[str, Any] | None
) -> container_image_consumers.ImageConsumerContext:
    """Collapses physical launches onto one logical image target owner."""
    return container_image_consumers.derive(task, cluster_name, workload_type,
                                            launch_context)


def _record_service_placement_event(
    *,
    task: task_lib.Task,
    cluster_name: str,
    workload_type: str,
    launch_context: dict[str, Any] | None,
    resources: resources_lib.Resources,
    region: 'clouds.Region',
    zones: list['clouds.Zone'] | None,
    num_nodes: int,
    outcome: str,
    error: Exception | None = None,
) -> None:
    """Capture one Serve placement outcome without database I/O."""
    if workload_type != 'service':
        return
    launch_context = launch_context or {}
    service_name = launch_context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
    service_hash = launch_context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(service_hash, str) or not service_hash):
        return
    replica_id = None
    raw_replica_id = (task.envs or {}).get(serve_constants.REPLICA_ID_ENV_VAR)
    try:
        if raw_replica_id is not None:
            replica_id = int(raw_replica_id)
    except (TypeError, ValueError):
        pass
    hourly_price = None
    try:
        hourly_price = resources.get_cost(seconds=3600)
    except Exception:  # pylint: disable=broad-except
        # Placement visibility must never affect provisioning when catalog
        # pricing is temporarily unavailable or a provider implementation
        # raises an unexpected error.
        pass
    zone = zones[0].name if zones is not None and len(zones) == 1 else None
    try:
        serve_placement_history.record_event(
            service_name=service_name,
            service_hash=service_hash,
            request_id=common_utils.get_current_request_id(),
            replica_id=replica_id,
            cluster_name=cluster_name,
            outcome=outcome,
            provider=str(resources.cloud)
            if resources.cloud is not None else None,
            region=region.name,
            zone=zone,
            instance_type=resources.instance_type,
            accelerators=resources.accelerators,
            use_spot=resources.use_spot,
            num_nodes=num_nodes,
            hourly_price=hourly_price,
            error_code=(_placement_error_code(error)
                        if error is not None else None),
            error_summary=(common_utils.format_exception(error)
                           if error is not None else None),
        )
    except Exception as history_error:  # pylint: disable=broad-except
        logger.debug('Placement-event capture failed: '
                     f'{common_utils.format_exception(history_error)}')


def _capacity_service_observation(
    workload_type: str,
    launch_context: dict[str, Any] | None,
) -> capacity_cache.ServiceObservation | None:
    if workload_type != 'service':
        return None
    launch_context = launch_context or {}
    service_name = launch_context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
    service_hash = launch_context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(service_hash, str) or not service_hash):
        return None
    return capacity_cache.ServiceObservation(service_name, service_hash)


class RetryingVmProvisioner:
    """A provisioner that retries different cloud/regions/zones."""

    class ToProvisionConfig:
        """Resources to be provisioned."""

        def __init__(
            self,
            cluster_name: str,
            resources: resources_lib.Resources,
            num_nodes: int,
            prev_cluster_status: status_lib.ClusterStatus | None,
            prev_handle: Optional['CloudVmRayResourceHandle'],
            prev_cluster_ever_up: bool,
            prev_config_hash: str | None,
            prev_cluster_hash: str | None = None,
        ) -> None:
            assert cluster_name is not None, 'cluster_name must be specified.'
            self.cluster_name = cluster_name
            self.resources = resources
            self.num_nodes = num_nodes
            self.prev_cluster_status = prev_cluster_status
            self.prev_handle = prev_handle
            self.prev_cluster_ever_up = prev_cluster_ever_up
            self.prev_config_hash = prev_config_hash
            self.prev_cluster_hash = prev_cluster_hash

    def __init__(self,
                 log_dir: str,
                 dag: 'dag.Dag',
                 optimize_target: 'common.OptimizeTarget',
                 requested_features: set[clouds.CloudImplementationFeatures],
                 local_wheel_path: pathlib.Path,
                 wheel_hash: str,
                 blocked_resources: Iterable[resources_lib.Resources] |
                 None = None,
                 is_managed: bool | None = None,
                 *,
                 extra_launch_context: dict[str, Any],
                 is_launched_by_jobs_controller: bool = False,
                 workload_type: str = 'cluster'):
        self._blocked_resources: set[resources_lib.Resources] = set()
        if blocked_resources:
            # blocked_resources is not None and not empty.
            self._blocked_resources.update(blocked_resources)

        self.log_dir = os.path.expanduser(log_dir)
        self._dag = dag
        self._optimize_target = optimize_target
        self._requested_features = requested_features
        self._local_wheel_path = local_wheel_path
        self._wheel_hash = wheel_hash
        self._is_managed = is_managed
        self._extra_launch_context: dict[str, Any] = extra_launch_context
        self._is_launched_by_jobs_controller = is_launched_by_jobs_controller
        self._workload_type = workload_type
        self._active_cluster_hash: str | None = None

    @property
    def active_cluster_hash(self) -> str | None:
        """Returns the cluster generation owned by this provisioning run."""
        return self._active_cluster_hash

    def _yield_zones(
            self, to_provision: resources_lib.Resources, num_nodes: int,
            cluster_name: str,
            prev_cluster_status: status_lib.ClusterStatus | None,
            prev_cluster_ever_up: bool) -> Iterable[list[clouds.Zone] | None]:
        """Yield zones within the given region to try for provisioning.

        Yields:
            Zones to try for provisioning within the given to_provision.region.
              - None means the cloud does not support zones, but the region does
                offer the requested resources (so the outer loop should issue a
                request to that region).
              - Non-empty list means the cloud supports zones, and the zones
                do offer the requested resources. If a list is yielded, it is
                guaranteed to be non-empty.
              - Nothing yielded means the region does not offer the requested
                resources.
        """
        assert (to_provision.cloud is not None and
                to_provision.region is not None and to_provision.instance_type
                is not None), (to_provision,
                               'cloud, region and instance_type must have been '
                               'set by optimizer')
        cloud = to_provision.cloud
        region = clouds.Region(to_provision.region)
        zones = None

        def _get_previously_launched_zones() -> list[clouds.Zone] | None:
            # When the cluster exists, the to_provision should have been set
            # to the previous cluster's resources.
            zones = [
                clouds.Zone(name=to_provision.zone),
            ] if to_provision.zone is not None else None
            if zones is None:
                # Reuse the zone field in the ray yaml as the
                # prev_resources.zone field may not be set before the previous
                # cluster is launched.
                handle = global_user_state.get_handle_from_cluster_name(
                    cluster_name,
                    existing_cluster_hash=self._active_cluster_hash)
                if handle is None:
                    raise exceptions.ClusterDoesNotExist(
                        f'Cluster {cluster_name!r} was removed or replaced '
                        'while provisioning was in progress.')
                assert isinstance(handle, CloudVmRayResourceHandle), (
                    'handle should be CloudVmRayResourceHandle (found: '
                    f'{type(handle)}) {cluster_name!r}')
                config = global_user_state.get_cluster_yaml_dict(
                    handle.cluster_yaml)
                # This is for the case when the zone field is not set in the
                # launched resources in a previous launch (e.g., ctrl-c during
                # launch and multi-node cluster before PR #1700).
                zones_str = config.get('provider', {}).get('availability_zone')
                if zones_str is not None:
                    zones = [
                        clouds.Zone(name=zone) for zone in zones_str.split(',')
                    ]
            return zones

        if prev_cluster_status is not None:
            # If the cluster is previously launched, we should relaunch in the
            # same region and zone.
            zones = _get_previously_launched_zones()

            if prev_cluster_status != status_lib.ClusterStatus.UP:
                logger.info(
                    f'{colorama.Style.DIM}Cluster {cluster_name!r} (status: '
                    f'{prev_cluster_status.value}) was previously in '
                    f'{cloud} ({region.name}). Restarting.'
                    f'{colorama.Style.RESET_ALL}')
            yield zones

            # If it reaches here: the cluster status in the database gets
            # set to either STOPPED or None, since a launch request was issued
            # but failed, and the provisioning loop (_retry_zones()) stopped the
            # cluster if `cluster_ever_up` is True; or terminated the cluster
            # otherwise.
            if prev_cluster_ever_up:
                message = (f'Failed to launch cluster {cluster_name!r} '
                           f'(previous status: {prev_cluster_status.value}). '
                           'To retry launching the cluster, run: '
                           f'sky start {cluster_name}')
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.ResourcesUnavailableError(message,
                                                               no_failover=True)

            assert (prev_cluster_status == status_lib.ClusterStatus.INIT
                   ), prev_cluster_status
            message = (f'Failed to launch cluster {cluster_name!r} '
                       f'(previous status: {prev_cluster_status.value}) '
                       f'with the original resources: {to_provision}.')
            # We attempted re-launching a previously INIT cluster with the
            # same cloud/region/resources, but failed. Here no_failover=False,
            # so we will retry provisioning it with the current requested
            # resources in the outer loop.
            #
            # This condition can be triggered for previously INIT cluster by
            # (1) launch, after answering prompt immediately ctrl-c;
            # (2) launch again.
            # After (1), the cluster exists with INIT, and may or may not be
            # live.  And if it hits here, it's definitely not alive (because
            # step (2) failed).  Hence it's ok to retry with different
            # cloud/region and with current resources.
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesUnavailableError(message)

        # If it reaches here, it means the cluster did not exist, as all the
        # cases when the cluster exists have been handled above (either the
        # provision succeeded in the caller and no need to retry, or this
        # function raised an ResourcesUnavailableError).
        for zones in cloud.zones_provision_loop(
                region=to_provision.region,
                num_nodes=num_nodes,
                instance_type=to_provision.instance_type,
                accelerators=to_provision.accelerators,
                use_spot=to_provision.use_spot,
        ):
            if zones is None:
                yield None
            else:
                assert zones, (
                    'Either None or a non-empty list of zones should '
                    'be yielded')
                # Only retry requested region/zones or all if not specified.
                zone_names = [zone.name for zone in zones]
                if not to_provision.valid_on_region_zones(
                        region.name, zone_names):
                    continue
                if to_provision.zone is not None:
                    zones = [clouds.Zone(name=to_provision.zone)]
                yield zones

    def _insufficient_resources_msg(
        self,
        to_provision: resources_lib.Resources,
        requested_resources: set[resources_lib.Resources],
        insufficient_resources: list[str] | None,
        last_error_reason: str | None = None,
    ) -> str:
        insufficent_resource_msg = ('' if insufficient_resources is None else
                                    f' ({", ".join(insufficient_resources)})')
        message = f'Failed to acquire resources{insufficent_resource_msg} '
        if to_provision.zone is not None:
            message += (f'in {to_provision.zone} for {requested_resources}. ')
        elif to_provision.region is not None and to_provision.cloud is not None:
            # For public clouds, provision.region is always set.
            if clouds.SSH().is_same_cloud(to_provision.cloud):
                ssh_node_pool_name = common_utils.removeprefix(
                    to_provision.region, 'ssh-')
                message += (
                    f'in SSH Node Pool ({ssh_node_pool_name}) '
                    f'for {requested_resources}. The SSH Node Pool may not '
                    'have enough resources.')
            elif clouds.Kubernetes().is_same_cloud(to_provision.cloud):
                message += (f'in context {to_provision.region} for '
                            f'{requested_resources}. ')
            else:
                message += (f'in all zones in {to_provision.region} for '
                            f'{requested_resources}. ')
        else:
            message += (f'{to_provision.cloud} for {requested_resources}. ')
        if last_error_reason:
            message = message.rstrip() + f'\nReason: {last_error_reason}'
        return message

    def _retry_zones(  # pylint: disable=line-too-long
        self,
        to_provision: resources_lib.Resources,
        num_nodes: int,
        requested_resources: set[resources_lib.Resources],
        dryrun: bool,
        stream_logs: bool,
        cluster_name: str,
        cloud_user_identity: list[str] | None,
        prev_cluster_status: status_lib.ClusterStatus | None,
        prev_handle: Optional['CloudVmRayResourceHandle'],
        prev_cluster_ever_up: bool,
        skip_if_config_hash_matches: str | None,
        volume_mounts: list[volume_lib.VolumeMount] | None,
        task: task_lib.Task,
    ) -> dict[str, Any]:
        """The provision retry loop.

        Returns a config_dict with the following fields:
        All fields from backend_utils.write_cluster_config(). See its
          docstring.
        - 'provisioning_skipped': True if provisioning was short-circuited
          by skip_if_config_hash_matches, False otherwise.
        - 'handle': The provisioned cluster handle.
        - 'provision_record': (Only if using the new skypilot provisioner) The
          record returned by provisioner.bulk_provision().
        - 'resources_vars': (Only if using the new skypilot provisioner) The
          resources variables given by make_deploy_resources_variables().
        """
        # Get log_path name
        log_path = os.path.join(self.log_dir, 'provision.log')
        log_abs_path = os.path.abspath(log_path)
        if not dryrun:
            os.makedirs(os.path.expanduser(self.log_dir), exist_ok=True)
            os.system(f'touch {log_path}')

        rich_utils.force_update_status(
            ux_utils.spinner_message('Launching',
                                     log_path,
                                     cluster_name=cluster_name))

        # Get previous cluster status
        cluster_exists = prev_cluster_status is not None

        to_provision = to_provision.assert_launchable()

        assert to_provision.region is not None, (
            to_provision, 'region should have been set by the optimizer.')
        region = clouds.Region(to_provision.region)

        capacity_cache_account = _capacity_cache_account(
            to_provision.cloud, cloud_user_identity)
        quota_cooldown_key = _quota_cooldown_key(to_provision, region,
                                                 num_nodes,
                                                 capacity_cache_account)
        if (not cluster_exists and not dryrun and
                _quota_cooldown_is_active(quota_cooldown_key)):
            _add_to_blocked_resources(
                self._blocked_resources,
                to_provision.copy(region=region.name, zone=None))
            raise exceptions.ResourcesUnavailableError(
                'Skipping a Spot demand still in a brief quota-failure '
                f'cooldown for {to_provision.instance_type} in {region.name}.')

        # Optimization - check if user has non-zero quota for
        # the instance type in the target region. If not, fail early
        # instead of trying to provision and failing later.
        try:
            need_provision = to_provision.cloud.check_quota_available(
                to_provision)

        except Exception as e:  # pylint: disable=broad-except
            need_provision = True
            logger.info(f'Error occurred when trying to check quota. '
                        f'Proceeding assuming quotas are available. Error: '
                        f'{common_utils.format_exception(e, use_bracket=True)}')

        if not need_provision:
            # if quota is found to be zero, raise exception and skip to
            # the next region
            if to_provision.use_spot:
                instance_descriptor = 'spot'
            else:
                instance_descriptor = 'on-demand'
            _record_insufficient_quota_notification(to_provision)
            raise exceptions.ResourcesUnavailableError(
                f'{colorama.Fore.YELLOW}Found no quota for '
                f'{to_provision.instance_type} {instance_descriptor} '
                f'instances in region {to_provision.region} '
                f'in {to_provision.cloud}. '
                f'{colorama.Style.RESET_ALL}'
                f'To request quotas, check the instruction: '
                f'https://docs.skypilot.co/en/latest/cloud-setup/quota.html.')

        # Quota has passed and the region is now a real launch attempt. This
        # is the first point where ensure-on-use may create materialization
        # intents. Dry runs remain read-only.
        launch_context = self._extra_launch_context
        if self._workload_type == 'cluster' and prev_handle is not None:
            previous_resources = prev_handle.launched_resources
            previous_resolution = previous_resources.resolved_container_image
            launch_context = (
                container_image_consumers.reuse_persisted_cluster_epoch(
                    launch_context, previous_resolution))
        image_demand = _get_image_demand_attribution(task, cluster_name,
                                                     self._workload_type,
                                                     launch_context)
        to_provision = typing.cast(
            resources_lib.LaunchableResources,
            _resolve_container_image_for_placement(
                to_provision,
                consumer_kind=image_demand.consumer_kind,
                consumer_owner=image_demand.consumer_owner,
                controller_epoch=image_demand.controller_epoch,
                controller_sequence=image_demand.controller_sequence,
                allow_epoch_advance=image_demand.allow_epoch_advance,
                consumer_metadata=image_demand.metadata,
                ensure=not dryrun))

        insufficient_resources = None
        last_error_reason: str | None = None
        # Preserve the structured provider failures that explain why every
        # yielded zone was rejected.  The outer provisioning retry loop stores
        # this ResourcesUnavailableError in its failover history; without the
        # nested evidence, callers can only see the generic summary below and
        # cannot safely distinguish capacity/quota from an unrelated failure.
        provision_failures: list[Exception] = []
        for zones in self._yield_zones(to_provision, num_nodes, cluster_name,
                                       prev_cluster_status,
                                       prev_cluster_ever_up):
            # Filter out zones that are blocked, if any.
            # This optimize the provision loop by skipping zones that are
            # indicated to be unavailable from previous provision attempts.
            # It can happen for the provisioning on GCP, as the
            # yield_region_zones will return zones from a region one by one,
            # but the optimizer that does the filtering will not be involved
            # until the next region.
            if zones is not None:
                remaining_unblocked_zones = copy.deepcopy(zones)
                for zone in zones:
                    for blocked_resources in self._blocked_resources:
                        if to_provision.copy(
                                region=region.name,
                                zone=zone.name).should_be_blocked_by(
                                    blocked_resources):
                            remaining_unblocked_zones.remove(zone)
                            break
                if (not cluster_exists and not dryrun and
                        remaining_unblocked_zones):
                    exhausted_zone_names = _capacity_cache_exhausted_zone_names(
                        to_provision, region, remaining_unblocked_zones,
                        num_nodes, capacity_cache_account)
                    if exhausted_zone_names:
                        logger.info(
                            'Skipping a recently capacity-exhausted spot '
                            f'attempt in {sorted(exhausted_zone_names)}.')
                        remaining_unblocked_zones = [
                            zone for zone in remaining_unblocked_zones
                            if zone.name not in exhausted_zone_names
                        ]
                if not remaining_unblocked_zones:
                    # Skip the region if all zones are blocked.
                    continue
                zones = remaining_unblocked_zones

            if zones is None:
                # For clouds that don't have a zone concept or cloud
                # provisioners that do not support zone-based provisioning
                # (e.g., Azure, Lambda).
                zone_str = ''
            else:
                zone_str = ','.join(z.name for z in zones)
                zone_str = f' ({zone_str})'

            # Let the registered Provisioner (if any) override the cluster
            # config template by returning a ``TemplateSpec``; otherwise
            # fall back to the default template for the cloud.
            template_override = provision_lib.get_provisioner_template_override(
                to_provision.cloud.canonical_name())
            template_spec = (template_override(
                task,
                to_provision,
                _extra_launch_context=self._extra_launch_context,
                _is_launched_by_jobs_controller=self.
                _is_launched_by_jobs_controller,
            ) if template_override is not None else None)
            if template_spec is not None:
                template = template_spec.template_path
                extra_vars = template_spec.variables
            else:
                template = _get_cluster_config_template(to_provision.cloud)
                extra_vars = None

            for failover_overrides in to_provision.cloud.yield_cloud_specific_failover_overrides(
                    region=to_provision.region):
                try:
                    config_dict = backend_utils.write_cluster_config(
                        to_provision,
                        num_nodes,
                        template,
                        cluster_name,
                        self._local_wheel_path,
                        self._wheel_hash,
                        region=region,
                        zones=zones,
                        dryrun=dryrun,
                        keep_launch_fields_in_existing_config=cluster_exists,
                        volume_mounts=volume_mounts,
                        cloud_specific_failover_overrides=failover_overrides,
                        extra_template_variables=extra_vars,
                    )
                except exceptions.ResourcesUnavailableError as e:
                    # Failed due to catalog issue, e.g. image not found, or
                    # GPUs are requested in a Kubernetes cluster but the cluster
                    # does not have nodes labeled with GPU types.
                    provision_failures.append(e)
                    logger.info(f'{e}')
                    continue
                except exceptions.InvalidCloudCredentials as e:
                    # Failed due to invalid cloud credentials.
                    logger.warning(f'{common_utils.format_exception(e)}')
                    # We should block the entire cloud for invalid cloud credentials
                    _add_to_blocked_resources(
                        self._blocked_resources,
                        to_provision.copy(region=None, zone=None))
                    raise exceptions.ResourcesUnavailableError(
                        f'Failed to provision on cloud {to_provision.cloud} due to '
                        f'invalid cloud credentials: '
                        f'{common_utils.format_exception(e)}')
                except exceptions.InvalidCloudConfigs as e:
                    # Failed due to invalid user configs in ~/.sky/config.yaml.
                    logger.warning(f'{common_utils.format_exception(e)}')
                    # We should block the entire cloud if the user config is
                    # invalid.
                    _add_to_blocked_resources(
                        self._blocked_resources,
                        to_provision.copy(region=None, zone=None))
                    raise exceptions.ResourcesUnavailableError(
                        f'Failed to provision on cloud {to_provision.cloud} due to '
                        f'invalid cloud config: {common_utils.format_exception(e)}'
                    )

                if ('config_hash' in config_dict and skip_if_config_hash_matches
                        == config_dict['config_hash']):
                    logger.debug(
                        'Skipping provisioning of cluster with matching '
                        'config hash.')
                    config_dict['provisioning_skipped'] = True
                    config_dict['cluster_hash'] = self._active_cluster_hash
                    return config_dict
                config_dict['provisioning_skipped'] = False

                if dryrun:
                    return config_dict

                cluster_config_file = config_dict['ray']

                launched_resources = to_provision.copy(region=region.name)
                if zones and len(zones) == 1:
                    launched_resources = launched_resources.copy(
                        zone=zones[0].name)

                prev_cluster_ips, prev_ssh_ports, prev_cluster_info = (None,
                                                                       None,
                                                                       None)
                if prev_handle is not None:
                    prev_cluster_ips = prev_handle.stable_internal_external_ips
                    prev_ssh_ports = prev_handle.stable_ssh_ports
                    prev_cluster_info = prev_handle.cached_cluster_info
                # Record early, so if anything goes wrong, 'sky status' will show
                # the cluster name and users can appropriately 'sky down'.  It also
                # means a second 'sky launch -c <name>' will attempt to reuse.
                handle = CloudVmRayResourceHandle(
                    cluster_name=cluster_name,
                    # Backward compatibility will be guaranteed by the underlying
                    # backend_utils.write_cluster_config, which gets the cluster
                    # name on cloud from the ray yaml file, if the previous cluster
                    # exists.
                    cluster_name_on_cloud=config_dict['cluster_name_on_cloud'],
                    cluster_yaml=cluster_config_file,
                    launched_nodes=num_nodes,
                    # OK for this to be shown in CLI as status == INIT.
                    launched_resources=launched_resources,
                    # Use the previous cluster's IPs and ports if available to
                    # optimize the case where the cluster is restarted, i.e., no
                    # need to query IPs and ports from the cloud provider.
                    stable_internal_external_ips=prev_cluster_ips,
                    stable_ssh_ports=prev_ssh_ports,
                    cluster_info=prev_cluster_info,
                )
                usage_lib.messages.usage.update_final_cluster_status(
                    status_lib.ClusterStatus.INIT)

                # This sets the status to INIT (even for a normal, UP cluster).
                workload_id, workload_task_id = _get_workload_attribution(
                    task, cluster_name, self._workload_type,
                    self._extra_launch_context)
                try:
                    self._active_cluster_hash = (
                        global_user_state.add_or_update_cluster(
                            cluster_name,
                            cluster_handle=handle,
                            requested_resources=requested_resources,
                            ready=False,
                            is_managed=self._is_managed,
                            provision_log_path=log_abs_path,
                            workload_type=self._workload_type,
                            workload_id=workload_id,
                            workload_task_id=workload_task_id,
                            existing_cluster_hash=self._active_cluster_hash,
                        ))
                except ValueError as e:
                    raise exceptions.ResourcesUnavailableError(
                        'The selected image route changed before the launch '
                        f'was committed: {e}') from e
                config_dict['cluster_hash'] = self._active_cluster_hash

                # Add cluster event for actual provisioning start.
                global_user_state.add_cluster_event(
                    cluster_name,
                    status_lib.ClusterStatus.INIT,
                    f'Provisioning on {to_provision.cloud.display_name()} ' +
                    f'in {to_provision.region}',
                    global_user_state.ClusterEventType.STATUS_CHANGE,
                    existing_cluster_hash=self._active_cluster_hash)

                global_user_state.set_owner_identity_for_cluster(
                    cluster_name,
                    cloud_user_identity,
                    existing_cluster_hash=self._active_cluster_hash)

                if (to_provision.cloud.PROVISIONER_VERSION ==
                        clouds.ProvisionerVersion.SKYPILOT):
                    # TODO (suquark): Gradually move the other clouds to
                    #  the new provisioner once they are ready.
                    assert to_provision.region == region.name, (to_provision,
                                                                region)
                    num_nodes = handle.launched_nodes
                    # Some clouds, like RunPod, only support exposing ports during
                    # launch. For those clouds, we pass the ports to open in the
                    # `bulk_provision` to expose the ports during provisioning.
                    # If the `bulk_provision` is to apply on an existing cluster,
                    # it should be ignored by the underlying provisioner impl
                    # as it will only apply to newly-created instances.
                    ports_to_open_on_launch = (
                        list(
                            resources_utils.port_ranges_to_set(
                                to_provision.ports))
                        if to_provision.cloud.OPEN_PORTS_VERSION
                        <= clouds.OpenPortsVersion.LAUNCH_ONLY else None)
                    try:
                        controller = controller_utils.Controllers.from_name(
                            cluster_name)
                        controller_str = ('' if controller is None else
                                          f' {controller.value.name}')
                        if isinstance(to_provision.cloud, clouds.Kubernetes):
                            suffix = '.'
                            if region.name.startswith('ssh-'):
                                ssh_node_pool_name = common_utils.removeprefix(
                                    region.name, 'ssh-')
                                suffix = f' ({ssh_node_pool_name})'
                            logger.info(
                                ux_utils.starting_message(
                                    f'Launching{controller_str} on '
                                    f'{to_provision.cloud}{suffix}'))
                        else:
                            logger.info(
                                ux_utils.starting_message(
                                    f'Launching{controller_str} on '
                                    f'{to_provision.cloud} '
                                    f'{region.name}{colorama.Style.RESET_ALL}'
                                    f'{zone_str}.'))
                        assert handle.cluster_yaml is not None
                        provision_record = provisioner.bulk_provision(
                            to_provision.cloud,
                            region,
                            zones,
                            resources_utils.ClusterName(
                                cluster_name, handle.cluster_name_on_cloud),
                            num_nodes=num_nodes,
                            cluster_yaml=handle.cluster_yaml,
                            prev_cluster_ever_up=prev_cluster_ever_up,
                            log_dir=self.log_dir,
                            ports_to_open_on_launch=ports_to_open_on_launch)
                        # NOTE: We will handle the logic of '_ensure_cluster_ray_started'
                        # in 'provision_utils.post_provision_runtime_setup()' in the
                        # caller.
                        resources_vars = (
                            to_provision.cloud.make_deploy_resources_variables(
                                to_provision,
                                resources_utils.ClusterName(
                                    cluster_name, handle.cluster_name_on_cloud),
                                region, zones, num_nodes))
                        config_dict['provision_record'] = provision_record
                        config_dict['resources_vars'] = resources_vars
                        config_dict['handle'] = handle
                        # A full fresh create proves that this exact demand can
                        # launch now. Reusing orphaned/resumed nodes, or only
                        # creating the missing subset, proves neither the
                        # original num_nodes capacity nor current quota.
                        if (capacity_cache_account is not None and
                                _fully_created_fresh_demand(
                                    provision_record, num_nodes,
                                    cluster_exists)):
                            succeeded_zones = None
                            if provision_record.zone is not None:
                                succeeded_zones = [
                                    clouds.Zone(provision_record.zone)
                                ]
                            success_capacity_key = _capacity_cache_key(
                                to_provision, region, succeeded_zones,
                                num_nodes, capacity_cache_account)
                            if success_capacity_key is not None:
                                try:
                                    capacity_cache.clear(success_capacity_key)
                                    _record_capacity_metric('capacity', 'clear')
                                except Exception as cache_error:  # pylint: disable=broad-except
                                    _record_capacity_metric(
                                        'capacity', 'cache_error')
                                    logger.debug(
                                        'Capacity-cache clear failed: '
                                        f'{common_utils.format_exception(cache_error)}'
                                    )
                            if quota_cooldown_key is not None:
                                try:
                                    capacity_cache.clear_quota_cooldown(
                                        quota_cooldown_key)
                                    _record_capacity_metric('quota', 'clear')
                                except Exception as cache_error:  # pylint: disable=broad-except
                                    _record_capacity_metric(
                                        'quota', 'cache_error')
                                    logger.debug(
                                        'Quota-cooldown clear failed: '
                                        f'{common_utils.format_exception(cache_error)}'
                                    )
                        _record_service_placement_event(
                            task=task,
                            cluster_name=cluster_name,
                            workload_type=self._workload_type,
                            launch_context=self._extra_launch_context,
                            resources=launched_resources,
                            region=region,
                            zones=zones,
                            num_nodes=num_nodes,
                            outcome='succeeded')
                        return config_dict
                    except provision_common.StopFailoverError:
                        with ux_utils.print_exception_no_traceback():
                            raise
                    except exceptions.InconsistentHighAvailabilityError:
                        # No teardown happens for this error.
                        with ux_utils.print_exception_no_traceback():
                            raise
                    except exceptions.ExecutionPausedError:
                        # Pausing to wait on an external condition: keep the
                        # resources for resume, do not tear down or fail over.
                        raise
                    except config_lib.KubernetesError as e:
                        provision_failures.append(e)
                        if e.insufficent_resources:
                            insufficient_resources = e.insufficent_resources
                        last_error_reason = str(e)
                        # NOTE: We try to cleanup the cluster even if the previous
                        # cluster does not exist. Also we are fast at
                        # cleaning up clusters now if there is no existing node.
                        CloudVmRayBackend().post_teardown_cleanup(
                            handle,
                            terminate=not prev_cluster_ever_up,
                            remove_from_db=False,
                            failover=True,
                        )
                        # TODO(suquark): other clouds may have different zone
                        #  blocking strategy. See '_update_blocklist_on_error'
                        #  for details.
                        FailoverCloudErrorHandlerV2.update_blocklist_on_error(
                            self._blocked_resources, to_provision, region,
                            zones, e)
                        _record_service_placement_event(
                            task=task,
                            cluster_name=cluster_name,
                            workload_type=self._workload_type,
                            launch_context=self._extra_launch_context,
                            resources=launched_resources,
                            region=region,
                            zones=zones,
                            num_nodes=num_nodes,
                            outcome=('capacity_failed'
                                     if e.insufficent_resources else 'failed'),
                            error=e)
                        continue
                    except Exception as e:  # pylint: disable=broad-except
                        provision_failures.append(e)
                        capacity_reason = _classify_capacity_error(
                            to_provision.cloud, e)
                        if _is_quota_error(e):
                            _record_insufficient_quota_notification(
                                to_provision)
                        failure_requested_full_demand = (
                            capacity_reason is not None and
                            _failure_requested_full_demand(e, num_nodes))
                        capacity_key = None
                        service_observation = _capacity_service_observation(
                            self._workload_type, self._extra_launch_context)
                        if capacity_reason == 'capacity' and not cluster_exists:
                            capacity_key = _capacity_cache_key(
                                to_provision, region, zones, num_nodes,
                                capacity_cache_account)
                            if (capacity_key is not None and
                                    failure_requested_full_demand):
                                try:
                                    capacity_cache.mark_exhausted(
                                        capacity_key, service_observation)
                                    _record_capacity_metric('capacity', 'mark')
                                except Exception as cache_error:  # pylint: disable=broad-except
                                    _record_capacity_metric(
                                        'capacity', 'cache_error')
                                    logger.debug(
                                        'Capacity-cache write failed: '
                                        f'{common_utils.format_exception(cache_error)}'
                                    )
                        elif (capacity_reason == 'quota' and
                              not cluster_exists and
                              quota_cooldown_key is not None and
                              failure_requested_full_demand):
                            try:
                                capacity_cache.mark_quota_failure(
                                    quota_cooldown_key, service_observation)
                                _record_capacity_metric('quota', 'mark')
                            except Exception as cache_error:  # pylint: disable=broad-except
                                _record_capacity_metric('quota', 'cache_error')
                                logger.debug(
                                    'Quota-cooldown write failed: '
                                    f'{common_utils.format_exception(cache_error)}'
                                )
                        if capacity_reason is not None:
                            _record_capacity_metric(capacity_reason,
                                                    'probe_failure')
                        _record_service_placement_event(
                            task=task,
                            cluster_name=cluster_name,
                            workload_type=self._workload_type,
                            launch_context=self._extra_launch_context,
                            resources=launched_resources,
                            region=region,
                            zones=zones,
                            num_nodes=num_nodes,
                            outcome=_placement_outcome(e, capacity_reason),
                            error=e)
                        # NOTE: We try to cleanup the cluster even if the previous
                        # cluster does not exist. Also we are fast at
                        # cleaning up clusters now if there is no existing node..
                        CloudVmRayBackend().post_teardown_cleanup(
                            handle,
                            terminate=not prev_cluster_ever_up,
                            remove_from_db=False,
                            failover=True)
                        # TODO(suquark): other clouds may have different zone
                        #  blocking strategy. See '_update_blocklist_on_error'
                        #  for details.
                        FailoverCloudErrorHandlerV2.update_blocklist_on_error(
                            self._blocked_resources, to_provision, region,
                            zones, e)
                        if capacity_key is not None:
                            break
                        if capacity_reason == 'quota':
                            _add_to_blocked_resources(
                                self._blocked_resources,
                                to_provision.copy(region=region.name,
                                                  zone=None))
                            break
                        continue
                    # NOTE: The code below in the loop should not be reachable
                    # with the new provisioner.

                logging_info = {
                    'cluster_name': cluster_name,
                    'region_name': region.name,
                    'zone_str': zone_str,
                }

                status, stdout, stderr, head_internal_ip, head_external_ip = (
                    self._gang_schedule_ray_up(to_provision.cloud,
                                               cluster_config_file, handle,
                                               log_abs_path, stream_logs,
                                               logging_info,
                                               to_provision.use_spot))

                if status == GangSchedulingStatus.CLUSTER_READY:
                    # We must query the IPs from the cloud provider, when the
                    # provisioning is done, to make sure the cluster IPs are
                    # up-to-date.
                    # The staled IPs may be caused by the node being restarted
                    # manually or by the cloud provider.
                    # Optimize the case where the cluster's head IPs can be parsed
                    # from the output of 'ray up'.
                    if handle.launched_nodes == 1:
                        handle.update_cluster_ips(
                            max_attempts=_FETCH_IP_MAX_ATTEMPTS,
                            internal_ips=[head_internal_ip],
                            external_ips=[head_external_ip])
                    else:
                        handle.update_cluster_ips(
                            max_attempts=_FETCH_IP_MAX_ATTEMPTS)
                    handle.update_ssh_ports(max_attempts=_FETCH_IP_MAX_ATTEMPTS)
                    if cluster_exists:
                        # Guard against the case where there's an existing cluster
                        # with ray runtime messed up (e.g., manually killed) by (1)
                        # querying ray status (2) restarting ray if needed.
                        #
                        # The above 'ray up' will not restart it automatically due
                        # to 'ray up # --no-restart' flag.
                        #
                        # NOTE: this is performance sensitive and has been observed
                        # to take 9s. Only do this for existing clusters, not
                        # freshly launched ones (which should have ray runtime
                        # started).
                        self._ensure_cluster_ray_started(handle, log_abs_path)

                    config_dict['handle'] = handle
                    logger.info(
                        ux_utils.finishing_message(
                            f'Cluster launched: {cluster_name!r}.',
                            log_path,
                            cluster_name=cluster_name))
                    _record_service_placement_event(
                        task=task,
                        cluster_name=cluster_name,
                        workload_type=self._workload_type,
                        launch_context=self._extra_launch_context,
                        resources=launched_resources,
                        region=region,
                        zones=zones,
                        num_nodes=num_nodes,
                        outcome='succeeded')
                    return config_dict

                # The cluster is not ready. We must perform error recording and/or
                # cleanup.

                # If cluster was ever up, stop it; otherwise terminate.
                terminate_or_stop = not prev_cluster_ever_up
                definitely_no_nodes_launched = False
                if status == GangSchedulingStatus.HEAD_FAILED:
                    # ray up failed for the head node.
                    definitely_no_nodes_launched = (
                        FailoverCloudErrorHandlerV1.update_blocklist_on_error(
                            self._blocked_resources, to_provision, region,
                            zones, stdout, stderr))
                else:
                    # gang scheduling failed.
                    assert status == GangSchedulingStatus.GANG_FAILED, status
                    # The stdout/stderr of ray up is not useful here, since
                    # head node is successfully provisioned.
                    definitely_no_nodes_launched = (
                        FailoverCloudErrorHandlerV1.update_blocklist_on_error(
                            self._blocked_resources,
                            to_provision,
                            region,
                            zones=zones,
                            stdout=None,
                            stderr=None))
                    # GANG_FAILED means head is up, workers failed.
                    assert definitely_no_nodes_launched is False, (
                        definitely_no_nodes_launched)

                    # Only log the errors for GANG_FAILED, since HEAD_FAILED may
                    # not have created any resources (it can happen however) and
                    # HEAD_FAILED can happen in "normal" failover cases.
                    logger.error('*** Failed provisioning the cluster. ***')
                    terminate_str = ('Terminating'
                                     if terminate_or_stop else 'Stopping')
                    logger.error(f'*** {terminate_str} the failed cluster. ***')

                # If these conditions hold, it *should* be safe to skip the cleanup
                # action. This is a UX optimization.
                #
                # We want to skip mainly for VPC/subnets errors thrown during node
                # provider bootstrapping: if users encountered "No VPC with name
                # 'xxx' is found in <region>.", then going ahead to down the
                # non-existent cluster will itself print out a (caught, harmless)
                # error with the same message.  This was found to be
                # confusing. Thus we skip termination.
                skip_cleanup = not cluster_exists and definitely_no_nodes_launched
                if skip_cleanup:
                    continue

                # There may exist partial nodes (e.g., head node) so we must
                # terminate or stop before moving on to other regions.
                #
                # NOTE: even HEAD_FAILED could've left a live head node there,
                # so we must terminate/stop here too. E.g., node is up, and ray
                # autoscaler proceeds to setup commands, which may fail:
                #   ERR updater.py:138 -- New status: update-failed
                CloudVmRayBackend().teardown_no_lock(
                    handle, terminate=terminate_or_stop, remove_from_db=False)

        message = self._insufficient_resources_msg(
            to_provision,
            requested_resources,
            insufficient_resources,
            last_error_reason=last_error_reason)
        # Do not failover to other locations if the cluster was ever up, since
        # the user can have some data on the cluster.
        raise exceptions.ResourcesUnavailableError(
            message,
            no_failover=prev_cluster_ever_up,
            failover_history=provision_failures)

    # TODO(suquark): Deprecate this method
    # once the `provision_utils` is adopted for all the clouds.
    @timeline.event
    def _gang_schedule_ray_up(
        self, to_provision_cloud: clouds.Cloud, cluster_config_file: str,
        cluster_handle: 'backends.CloudVmRayResourceHandle', log_abs_path: str,
        stream_logs: bool, logging_info: dict, use_spot: bool
    ) -> tuple[GangSchedulingStatus, str, str, str | None, str | None]:
        """Provisions a cluster via 'ray up' and wait until fully provisioned.

        Returns:
            (GangSchedulingStatus; stdout; stderr;
                optional head_internal_ip; optional head_external_ip).
        """
        # FIXME(zhwu,zongheng): ray up on multiple nodes ups the head node then
        # waits for all workers; turn it into real gang scheduling.
        # FIXME: refactor code path to remove use of stream_logs
        del stream_logs

        def ray_up():
            # Runs `ray up <kwargs>` with our monkey-patched launch hash
            # calculation. See the monkey patch file for why.
            #
            # NOTE: --no-restart solves the following bug.  Without it, if 'ray
            # up' (sky launch) twice on a cluster with >1 node, the worker node
            # gets disconnected/killed by ray autoscaler; the whole task will
            # just freeze.  (Doesn't affect 1-node clusters.)  With this flag,
            # ray processes no longer restart and this bug doesn't show.
            # Downside is existing tasks on the cluster will keep running
            # (which may be ok with the semantics of 'sky launch' twice).
            # Tracked in https://github.com/ray-project/ray/issues/20402.
            # Ref: https://github.com/ray-project/ray/blob/releases/2.4.0/python/ray/autoscaler/sdk/sdk.py#L16-L49  # pylint: disable=line-too-long
            script_path = write_ray_up_script_with_patched_launch_hash_fn(
                cluster_config_file, ray_up_kwargs={'no_restart': True})

            # Redirect stdout/err to the file and streaming (if stream_logs).
            # With stdout/err redirected, 'ray up' will have no color and
            # different order from directly running in the console. The
            # `--log-style` and `--log-color` flags do not work. To reproduce,
            # `ray up --log-style pretty --log-color true | tee tmp.out`.
            returncode, stdout, stderr = log_lib.run_with_log(
                [sys.executable, script_path],
                log_abs_path,
                stream_logs=False,
                start_streaming_at='Shared connection to',
                line_processor=log_utils.RayUpLineProcessor(
                    log_abs_path, cluster_name=cluster_handle.cluster_name),
                # Reduce BOTO_MAX_RETRIES from 12 to 5 to avoid long hanging
                # time during 'ray up' if insufficient capacity occurs.
                env=dict(
                    os.environ,
                    BOTO_MAX_RETRIES='5',
                    # Use environment variables to disable the ray usage collection
                    # (to avoid overheads and potential issues with the usage)
                    # as sdk does not take the argument for disabling the usage
                    # collection.
                    RAY_USAGE_STATS_ENABLED='0'),
                require_outputs=True,
                # Disable stdin to avoid ray outputs mess up the terminal with
                # misaligned output when multithreading/multiprocessing are used
                # Refer to: https://github.com/ray-project/ray/blob/d462172be7c5779abf37609aed08af112a533e1e/python/ray/autoscaler/_private/subprocess_output_util.py#L264  # pylint: disable=line-too-long
                stdin=subprocess.DEVNULL)
            return returncode, stdout, stderr

        region_name = logging_info['region_name']
        zone_str = logging_info['zone_str']
        if isinstance(to_provision_cloud, clouds.Kubernetes):
            logger.info(
                ux_utils.starting_message(
                    f'Launching on {to_provision_cloud}.'))
        else:
            logger.info(
                ux_utils.starting_message(f'Launching on {to_provision_cloud} '
                                          f'{region_name}{zone_str}.'))
        start = time.time()

        # Edge case: /tmp/ray does not exist, so autoscaler can't create/store
        # cluster lock and cluster state.
        os.makedirs('/tmp/ray', exist_ok=True)

        # Launch the cluster with ray up

        # Retry if the any of the following happens:
        # 1. Failed due to timeout when fetching head node for Azure.
        # 2. Failed due to file mounts, because it is probably has too
        # many ssh connections and can be fixed by retrying.
        # This is required when using custom image for GCP.
        def need_ray_up(
                ray_up_return_value: tuple[int, str, str] | None) -> bool:

            # Indicates the first ray up.
            if ray_up_return_value is None:
                return True

            returncode, stdout, stderr = ray_up_return_value
            if returncode == 0:
                return False

            if isinstance(to_provision_cloud, clouds.Lambda):
                if 'Your API requests are being rate limited.' in stderr:
                    logger.info(
                        'Retrying due to Lambda API rate limit exceeded.')
                    return True

            if 'rsync: command not found' in stderr:
                logger.info('Skipping retry due to `rsync` not found in '
                            'the specified image.')
                return False

            if ('Processing file mounts' in stdout and
                    'Running setup commands' not in stdout and
                    'Failed to setup head node.' in stderr):
                logger.info(
                    'Retrying runtime setup due to ssh connection issue.')
                return True

            if ('ConnectionResetError: [Errno 54] Connection reset by peer'
                    in stderr):
                logger.info('Retrying due to Connection reset by peer.')
                return True
            return False

        retry_cnt = 0
        ray_up_return_value = None
        # 5 seconds to 180 seconds. We need backoff for e.g., rate limit per
        # minute errors.
        backoff = common_utils.Backoff(initial_backoff=5,
                                       max_backoff_factor=180 // 5)
        while (retry_cnt < _MAX_RAY_UP_RETRY and
               need_ray_up(ray_up_return_value)):
            retry_cnt += 1
            if retry_cnt > 1:
                sleep = backoff.current_backoff()
                logger.info(f'Retrying launching in {sleep:.1f} seconds.')
                time.sleep(sleep)
            # TODO(zhwu): when we retry ray up, it is possible that the ray
            # cluster fail to start because --no-restart flag is used.
            ray_up_return_value = ray_up()

        assert ray_up_return_value is not None
        returncode, stdout, stderr = ray_up_return_value

        logger.debug(f'`ray up` takes {time.time() - start:.1f} seconds with '
                     f'{retry_cnt} retries.')
        if returncode != 0:
            return GangSchedulingStatus.HEAD_FAILED, stdout, stderr, None, None

        # Only 1 node or head node provisioning failure.
        if cluster_handle.launched_nodes == 1 and returncode == 0:
            # Optimization: Try parse head ip from 'ray up' stdout.
            # Last line looks like: 'ssh ... <user>@<public head_ip>\n'
            position = stdout.rfind('@')
            # Use a regex to extract the IP address.
            external_ip_list = re.findall(backend_utils.IP_ADDR_REGEX,
                                          stdout[position + 1:])
            head_internal_ip, head_external_ip = None, None
            if len(external_ip_list) == 1:
                head_external_ip = external_ip_list[0]

            # Optimization: Try parse internal head ip from 'ray start' stdout.
            # The line looks like: 'Local node IP: <internal head_ip>\n'
            position = stdout.rfind('Local node IP')
            line = stdout[position:].partition('\n')[0]
            internal_ip_list = re.findall(backend_utils.IP_ADDR_REGEX,
                                          common_utils.remove_color(line))
            if len(internal_ip_list) == 1:
                head_internal_ip = internal_ip_list[0]

            logger.debug(f'Get head ips from ray up stdout: {head_internal_ip} '
                         f'{head_external_ip}')
            return (GangSchedulingStatus.CLUSTER_READY, stdout, stderr,
                    head_internal_ip, head_external_ip)

        # All code below is handling num_nodes > 1.
        # FIXME(zongheng): the below requires ray processes are up on head. To
        # repro it failing: launch a 2-node cluster, log into head and ray
        # stop, then launch again.
        cluster_ready, _ = backend_utils.wait_until_ray_cluster_ready(
            cluster_config_file,
            num_nodes=cluster_handle.launched_nodes,
            log_path=log_abs_path,
            nodes_launching_progress_timeout=_NODES_LAUNCHING_PROGRESS_TIMEOUT[
                type(to_provision_cloud)])
        if cluster_ready:
            cluster_status = GangSchedulingStatus.CLUSTER_READY
            # ray up --no-restart again with upscaling_speed=0 after cluster is
            # ready to ensure cluster will not scale up after preemption (spot).
            # Skip for non-spot as this takes extra time to provision (~1min).
            if use_spot:
                ray_config = global_user_state.get_cluster_yaml_dict(
                    cluster_config_file)
                ray_config['upscaling_speed'] = 0
                yaml_utils.dump_yaml(cluster_config_file, ray_config)
                start = time.time()
                returncode, stdout, stderr = ray_up()
                logger.debug(
                    f'Upscaling reset takes {time.time() - start} seconds.')
                if returncode != 0:
                    return (GangSchedulingStatus.GANG_FAILED, stdout, stderr,
                            None, None)
        else:
            cluster_status = GangSchedulingStatus.GANG_FAILED

        # Do not need stdout/stderr if gang scheduling failed.
        # gang_succeeded = False, if head OK, but workers failed.
        return cluster_status, '', '', None, None

    def _ensure_cluster_ray_started(self, handle: 'CloudVmRayResourceHandle',
                                    log_abs_path) -> None:
        """Ensures ray processes are up on a just-provisioned cluster."""
        if handle.launched_nodes > 1:
            # FIXME(zongheng): this has NOT been tested with multinode
            # clusters; mainly because this function will not be reached in
            # that case.  See #140 for details.  If it were reached, the
            # following logic might work:
            #   - get all node ips
            #   - for all nodes: ray stop
            #   - ray up --restart-only
            return
        backend = CloudVmRayBackend()

        returncode, output, _ = backend.run_on_head(
            handle,
            instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
            require_outputs=True)
        while returncode == 0 and 'No cluster status' in output:
            # Retry until ray status is ready. This is to avoid the case where
            # ray cluster is just started but the ray status is not ready yet.
            logger.info('Waiting for ray cluster to be ready remotely.')
            time.sleep(1)
            returncode, output, _ = backend.run_on_head(
                handle,
                instance_setup.RAY_STATUS_WITH_SKY_RAY_PORT_COMMAND,
                require_outputs=True)
        if returncode == 0:
            return
        backend.run_on_head(handle, f'{constants.SKY_RAY_CMD} stop')

        # Runs `ray up <kwargs>` with our monkey-patched launch hash
        # calculation. See the monkey patch file for why.
        script_path = write_ray_up_script_with_patched_launch_hash_fn(
            handle.cluster_yaml, ray_up_kwargs={'restart_only': True})
        log_lib.run_with_log(
            [sys.executable, script_path],
            log_abs_path,
            stream_logs=False,
            # Use environment variables to disable the ray usage collection
            # (to avoid overheads and potential issues with the usage)
            # as sdk does not take the argument for disabling the usage
            # collection.
            env=dict(os.environ, RAY_USAGE_STATS_ENABLED='0'),
            # Disable stdin to avoid ray outputs mess up the terminal with
            # misaligned output when multithreading/multiprocessing is used.
            # Refer to: https://github.com/ray-project/ray/blob/d462172be7c5779abf37609aed08af112a533e1e/python/ray/autoscaler/_private/subprocess_output_util.py#L264 # pylint: disable=line-too-long
            stdin=subprocess.DEVNULL)

    @timeline.event
    def provision_with_retries(
        self,
        task: task_lib.Task,
        to_provision_config: ToProvisionConfig,
        dryrun: bool,
        stream_logs: bool,
        skip_unnecessary_provisioning: bool,
    ) -> dict[str, Any]:
        """Provision with retries for all launchable resources.

        Returns the config_dict from _retry_zones() - see its docstring for
        details.
        """
        cluster_name = to_provision_config.cluster_name
        to_provision = to_provision_config.resources
        num_nodes = to_provision_config.num_nodes
        prev_cluster_status = to_provision_config.prev_cluster_status
        prev_handle = to_provision_config.prev_handle
        prev_cluster_ever_up = to_provision_config.prev_cluster_ever_up
        self._active_cluster_hash = to_provision_config.prev_cluster_hash
        launchable_retries_disabled = (self._dag is None or
                                       self._optimize_target is None)
        skip_if_config_hash_matches = (to_provision_config.prev_config_hash if
                                       skip_unnecessary_provisioning else None)

        failover_history: list[Exception] = list()
        resource_exceptions: dict[resources_lib.Resources, Exception] = dict()
        # If the user is using local credentials which may expire, the
        # controller may leak resources if the credentials expire while a job
        # is running. Here we check the enabled clouds and expiring credentials
        # and raise a warning to the user.
        if task.is_controller_task():
            enabled_clouds = sky_check.get_cached_enabled_clouds_or_refresh(
                sky_cloud.CloudCapability.COMPUTE)
            expirable_clouds = backend_utils.get_expirable_clouds(
                enabled_clouds)

            if len(expirable_clouds) > 0:
                warnings = (f'\033[93mWarning: Credentials used for '
                            f'{expirable_clouds} may expire. Clusters may be '
                            f'leaked if the credentials expire while jobs '
                            f'are running. It is recommended to use credentials'
                            f' that never expire or a service account.\033[0m')
                logger.warning(warnings)

        to_provision = to_provision.assert_launchable()
        # Retrying launchable resources.
        while True:
            try:
                # Recheck cluster name as the 'except:' block below may
                # change the cloud assignment.
                common_utils.check_cluster_name_is_valid(cluster_name)

                if dryrun:
                    cloud_user = None
                elif isinstance(to_provision.cloud, clouds.Kubernetes):
                    # Region is guaranteed to be set by optimizer.
                    assert to_provision.region is not None
                    cloud_user = clouds.Kubernetes.get_identity_from_context_name(  # pylint: disable=line-too-long
                        to_provision.region)
                else:
                    cloud_user = to_provision.cloud.get_active_user_identity()

                requested_features = self._requested_features.copy()
                # Skip stop feature for Kubernetes and RunPod controllers.
                if (isinstance(to_provision.cloud,
                               (clouds.Kubernetes, clouds.RunPod)) and
                        controller_utils.Controllers.from_name(cluster_name)
                        is not None):
                    # If autostop is disabled in config, the feature may not be
                    # requested, so use discard() instead of remove().
                    requested_features.discard(
                        clouds.CloudImplementationFeatures.AUTOSTOP)
                    # Non-down autostop also requests STOP (see
                    # execution.autostop_requested_features); controllers
                    # on Kubernetes/RunPod get force-converted to
                    # autodown/no-op by set_autostop, so the same
                    # carve-out applies.
                    requested_features.discard(
                        clouds.CloudImplementationFeatures.STOP)

                # Skip if to_provision.cloud does not support requested
                # features. Feature support can be RESOURCE-dependent
                # (e.g. STOP is unsupported for AWS one-time spot but
                # fine on-demand), so the failure is re-raised as the
                # internal marker below and handled by its own except
                # clause -- the broad handler's cloud-wide block would
                # poison sibling candidates on the same cloud (and,
                # because Resources.use_spot is never None, a bare
                # Resources(cloud=...) block matches only NON-spot
                # candidates, breaking any_of [spot, on-demand] fallback
                # entirely).
                try:
                    to_provision.cloud.check_features_are_supported(
                        to_provision, requested_features)
                except exceptions.NotSupportedError as e:
                    # NOTE: raised unconditionally (never a bare
                    # re-raise): this inner handler is nested INSIDE the
                    # outer try, so a re-raised NotSupportedError would
                    # be swallowed by the broad sibling handler below and
                    # take the cloud-wide-block path. The marker handler
                    # decides what to do (including propagating cleanly
                    # for existing clusters -- an exception raised from
                    # within an outer-level handler is NOT caught by its
                    # sibling except clauses).
                    raise _ResourcesFeaturesUnsupportedError() from e

                config_dict = self._retry_zones(
                    to_provision,
                    num_nodes,
                    requested_resources=set(task.resources),
                    dryrun=dryrun,
                    stream_logs=stream_logs,
                    cluster_name=cluster_name,
                    cloud_user_identity=cloud_user,
                    prev_cluster_status=prev_cluster_status,
                    prev_handle=prev_handle,
                    prev_cluster_ever_up=prev_cluster_ever_up,
                    skip_if_config_hash_matches=skip_if_config_hash_matches,
                    volume_mounts=task.volume_mounts,
                    task=task,
                )
                if dryrun:
                    return config_dict
            except _ResourcesFeaturesUnsupportedError as e:
                # Resource-dependent feature failure (e.g. non-down
                # autostop on a one-time spot candidate).
                cause = e.__cause__
                assert isinstance(cause, exceptions.NotSupportedError), e
                init_never_up = (prev_cluster_status
                                 == status_lib.ClusterStatus.INIT and
                                 not prev_cluster_ever_up)
                if prev_cluster_status is not None and not init_never_up:
                    # Existing UP/STOPPED clusters -- and EVER-UP INIT
                    # clusters -- relaunched with a config their (fixed)
                    # launched resources cannot satisfy: failover cannot
                    # help (for ever-up clusters _yield_zones forbids it
                    # outright to preserve data), and a stop-teardown
                    # would be attempted on exactly the resources that
                    # tripped this check. Only NEVER-UP INIT clusters
                    # (e.g. a failed first spot attempt) fall through:
                    # INIT is their retryable state, and the tail resets
                    # to a fresh launch so the task can fail over to
                    # candidates that DO support the feature (e.g.
                    # on-demand).
                    # Raised from within this handler, the error
                    # propagates out of the function (sibling except
                    # clauses of the same try do not catch it) -- the
                    # same clean error `sky autostop` gives, wrapped
                    # with the cluster name and the actionable fix like
                    # core.autostop's message. `from None` suppresses
                    # the implicit __context__ chain so the internal
                    # marker class never shows up in the debug
                    # stacktrace serialized to API clients.
                    raise exceptions.NotSupportedError(
                        f'The requested configuration for cluster '
                        f'{cluster_name!r} is not supported by its '
                        f'launched resources: {cause}\n'
                        'To fix: drop the unsupported request, e.g. for '
                        'autostop on unstoppable resources (such as '
                        'spot) use autodown instead '
                        '(`-i <minutes> --down`).') from None
                if init_never_up and prev_handle is not None:
                    # The loop tail's INIT branch resets to a fresh
                    # launch assuming _retry_zones() already terminated
                    # the old cluster -- but the feature check fails
                    # BEFORE _retry_zones() runs, so the failed
                    # attempt's partial resources would leak while
                    # failover proceeds. Do the equivalent cleanup here.
                    # Unconditional terminate: only never-up clusters
                    # reach this branch (ever-up ones took the clean
                    # error above), and a STOP teardown would be
                    # impossible on exactly the resources that tripped
                    # the feature check.
                    CloudVmRayBackend().teardown_no_lock(prev_handle,
                                                         terminate=True,
                                                         remove_from_db=False)
                # Fall through to the loop tail: for a NEW launch it
                # blocks exactly `to_provision` and records the cause --
                # so sibling candidates on the same cloud (e.g.
                # on-demand after a spot candidate failed a STOP
                # requirement) still get their turn; for INIT the tail
                # resets to a fresh launch and re-optimizes.
                logger.warning(common_utils.format_exception(cause))
                failover_history.append(cause)
            except (exceptions.InvalidClusterNameError,
                    exceptions.NotSupportedError,
                    exceptions.CloudUserIdentityError) as e:
                # InvalidClusterNameError: cluster name is invalid,
                # NotSupportedError: cloud does not support requested features,
                # CloudUserIdentityError: cloud user identity is invalid.
                # The exceptions above should be applicable to the whole
                # cloud, so we do add the cloud to the blocked resources.
                logger.warning(common_utils.format_exception(e))
                _add_to_blocked_resources(
                    self._blocked_resources,
                    resources_lib.Resources(cloud=to_provision.cloud))
                failover_history.append(e)
            except exceptions.ResourcesUnavailableError as e:
                failover_history.append(e)
                if e.no_failover:
                    raise e.with_failover_history(failover_history)
                if launchable_retries_disabled:
                    logger.warning(
                        'DAG and optimize_target needs to be registered first '
                        'to enable cross-cloud retry. '
                        'To fix, call backend.register_info(dag=dag, '
                        'optimize_target=sky.OptimizeTarget.COST)')
                    raise e.with_failover_history(failover_history)

                logger.warning(common_utils.format_exception(e))
            else:
                # Provisioning succeeded.
                return config_dict

            if prev_cluster_status is None:
                # Add failed resources to the blocklist, only when it
                # is in fallback mode.
                _add_to_blocked_resources(self._blocked_resources, to_provision)
                assert len(failover_history) > 0
                resource_exceptions[to_provision] = failover_history[-1]
            else:
                # If we reach here, it means that the existing cluster must have
                # a previous status of INIT, because other statuses (UP,
                # STOPPED) will not trigger the failover due to `no_failover`
                # flag; see _yield_zones(). Also, the cluster should have been
                # terminated by _retry_zones().
                assert (prev_cluster_status == status_lib.ClusterStatus.INIT
                       ), prev_cluster_status
                logger.info(
                    ux_utils.retry_message(
                        f'Retrying provisioning with requested resources: '
                        f'{task.num_nodes}x {task.resources}'))
                # Retry with the current, potentially "smaller" resources:
                # to_provision == the current new resources (e.g., V100:1),
                # which may be "smaller" than the original (V100:8).
                # num_nodes is not part of a Resources so must be updated
                # separately.
                num_nodes = task.num_nodes
                prev_cluster_status = None
                prev_handle = None

            retry_message = ux_utils.retry_message(
                'Trying other potential resources.')
            logger.warning(f'\n{retry_message}')
            log_path = os.path.join(self.log_dir, 'provision.log')
            rich_utils.force_update_status(
                ux_utils.spinner_message('Looking for resources', log_path))
            # Set to None so that sky.optimize() will assign a new one
            # (otherwise will skip re-optimizing this task).
            # TODO: set all remaining tasks' best_resources to None.
            task.best_resources = None
            try:
                self._dag = optimizer.Optimizer.optimize(
                    self._dag,
                    minimize=self._optimize_target,
                    blocked_resources=self._blocked_resources)
            except exceptions.ResourcesUnavailableError as e:
                # Optimizer failed to find a feasible resources for the task,
                # either because the previous failovers have blocked all the
                # possible resources or the requested resources is too
                # restrictive. If we reach here, our failover logic finally
                # ends here.
                blocks = _format_provision_failure_blocks(resource_exceptions)
                raise exceptions.ResourcesUnavailableError(
                    _RESOURCES_UNAVAILABLE_LOG + '\n' + blocks,
                    failover_history=failover_history) from e
            best_resources = task.best_resources
            assert task in self._dag.tasks, 'Internal logic error.'
            assert best_resources is not None, task
            to_provision = best_resources


@dataclasses.dataclass
class SSHTunnelInfo:
    port: int
    pid: int


def _is_tunnel_healthy(tunnel: SSHTunnelInfo) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            s.connect(('localhost', tunnel.port))
        return True
    except OSError as e:
        logger.warning(f'Failed to connect to tunnel on port {tunnel.port}: '
                       f'{common_utils.format_exception(e)}')
        return False


class CloudVmRayResourceHandle(backends.backend.ResourceHandle):
    """A pickle-able handle to a cluster created by CloudVmRayBackend.

    The handle object will last for the whole lifecycle of the cluster.

    - (required) Cluster name.
    - (required) Cluster name on cloud (different from the cluster name, as we
        append user hash to avoid conflict b/t multiple users in the same
        organization/account, and truncate the name for length limit). See
        design_docs/cluster_name.md for details.
    - (required) Path to a cluster.yaml file.
    - (optional) A cached head node public IP.  Filled in after a
        successful provision().
    - (optional) A cached stable list of (internal IP, external IP) tuples
        for all nodes in a cluster. Filled in after successful task execution.
    - (optional) Launched num nodes
    - (optional) Launched resources
    - (optional) Docker user name
    - (optional) If TPU(s) are managed, a path to a deletion script.
    - (optional) Skylet SSH tunnel info.
    """
    # Bump if any fields get added/removed/changed, and add backward
    # compatibility logic in __setstate__ and/or __getstate__.
    _VERSION = 13

    # Set by from_dict() since cached_cluster_info is not available
    # when reconstructing from a dict.
    _ssh_user: str | None = None

    def __init__(
            self,
            *,
            cluster_name: str,
            cluster_name_on_cloud: str,
            cluster_yaml: str | None,
            launched_nodes: int,
            launched_resources: resources_lib.Resources,
            stable_internal_external_ips: list[tuple[str, str]] | None = None,
            stable_ssh_ports: list[int] | None = None,
            cluster_info: provision_common.ClusterInfo | None = None) -> None:
        self._version = self._VERSION
        self.cluster_name = cluster_name
        self.cluster_name_on_cloud = cluster_name_on_cloud
        # Replace the home directory with ~ for better robustness across systems
        # with different home directories.
        if cluster_yaml is not None and cluster_yaml.startswith(
                os.path.expanduser('~')):
            cluster_yaml = cluster_yaml.replace(os.path.expanduser('~'), '~', 1)
        self._cluster_yaml = cluster_yaml
        # List of (internal_ip, feasible_ip) tuples for all the nodes in the
        # cluster, sorted by the feasible ips. The feasible ips can be either
        # internal or external ips, depending on the use_internal_ips flag.
        self.stable_internal_external_ips = stable_internal_external_ips
        self.stable_ssh_ports = stable_ssh_ports
        self.cached_cluster_info = cluster_info
        self.launched_nodes = launched_nodes
        self.launched_resources = launched_resources
        self.docker_user: str | None = None
        self.is_grpc_enabled = True
        # A handle is created before provisioning runs, so nothing is set up
        # on the cluster yet; bulk_provision() fills in the real record on
        # completion. Defaulting to no-Ray keeps teardown of a cluster that
        # crashed or recovered mid-provisioning from attempting `ray stop` on
        # a cluster where Ray never started.
        self.provision_runtime_metadata = (
            provision_common.ProvisionRuntimeMetadata(
                has_ray=False,
                has_skylet=False,
                has_job_queue=False,
                ssh_available=False,
            ))

    def __repr__(self):
        return (f'ResourceHandle('
                f'\n\tcluster_name={self.cluster_name},'
                f'\n\tcluster_name_on_cloud={self.cluster_name_on_cloud},'
                f'\n\thead_ip={self.head_ip},'
                '\n\tstable_internal_external_ips='
                f'{self.stable_internal_external_ips},'
                '\n\tstable_ssh_ports='
                f'{self.stable_ssh_ports},'
                '\n\tcluster_yaml='
                f'{self.cluster_yaml}, '
                f'\n\tlaunched_resources={self.launched_nodes}x '
                f'{self.launched_resources}, '
                f'\n\tdocker_user={self.docker_user},'
                f'\n\tssh_user={self.ssh_user},'
                f'\n\tis_grpc_enabled={self.is_grpc_enabled},')

    def get_cluster_name(self):
        return self.cluster_name

    def get_cluster_name_on_cloud(self):
        return self.cluster_name_on_cloud

    def _use_internal_ips(self):
        """Returns whether to use internal IPs for SSH connections."""
        # Directly load the `use_internal_ips` flag from the cluster yaml
        # instead of `skypilot_config` as the latter can be changed after the
        # cluster is UP.
        return global_user_state.get_cluster_yaml_dict(self.cluster_yaml).get(
            'provider', {}).get('use_internal_ips', False)

    def update_ssh_ports(self, max_attempts: int = 1) -> None:
        """Fetches and sets the SSH ports for the cluster nodes.

        Use this method to use any cloud-specific port fetching logic.
        """
        del max_attempts  # Unused.
        if self.cached_cluster_info is not None:
            self.stable_ssh_ports = self.cached_cluster_info.get_ssh_ports()
            return

        head_ssh_port = 22
        self.stable_ssh_ports = (
            [head_ssh_port] + [22] *
            (self.num_ips_per_node * self.launched_nodes - 1))

    def _update_cluster_info(self):
        # When a cluster is on a cloud that does not support the new
        # provisioner, we should skip updating cluster_info.
        if (self.launched_resources.cloud is not None and
                self.launched_resources.cloud.PROVISIONER_VERSION
                >= clouds.ProvisionerVersion.SKYPILOT):
            provider_name = str(self.launched_resources.cloud).lower()
            config = {}
            # It is possible that the cluster yaml is not available when
            # the handle is unpickled for service replicas from the
            # controller with older version.
            yaml_str = global_user_state.get_cluster_yaml_str(self.cluster_yaml)
            if yaml_str is None:
                # If the cluster yaml is not available,
                # we skip updating the cluster info.
                return
            config = yaml_utils.safe_load(yaml_str)
            try:
                cluster_info = provision_lib.get_cluster_info(
                    provider_name,
                    region=self.launched_resources.region,
                    cluster_name_on_cloud=self.cluster_name_on_cloud,
                    provider_config=config.get('provider', None))
            except Exception as e:  # pylint: disable=broad-except
                # This could happen when the VM is not fully launched, and a
                # user is trying to terminate it with `sky down`.
                logger.debug('Failed to get cluster info for '
                             f'{self.cluster_name} from the new provisioner '
                             f'with {common_utils.format_exception(e)}.')
                raise exceptions.FetchClusterInfoError(
                    exceptions.FetchClusterInfoError.Reason.HEAD) from e
            if cluster_info.num_instances != self.launched_nodes:
                logger.debug(
                    f'Available nodes in the cluster {self.cluster_name} '
                    'do not match the number of nodes requested ('
                    f'{cluster_info.num_instances} != '
                    f'{self.launched_nodes}).')
                raise exceptions.FetchClusterInfoError(
                    exceptions.FetchClusterInfoError.Reason.HEAD)
            self.cached_cluster_info = cluster_info

    def update_cluster_ips(
            self,
            max_attempts: int = 1,
            internal_ips: list[str | None] | None = None,
            external_ips: list[str | None] | None = None,
            cluster_info: provision_common.ClusterInfo | None = None) -> None:
        """Updates the cluster IPs cached in the handle.

        We cache the cluster IPs in the handle to avoid having to retrieve
        them from the cloud provider every time we need them. This method
        updates the cached IPs.

        Optimizations:
            1) If the external IPs are provided (e.g. from the provision logs),
                we use them instead of retrieving them from the cloud provider.
            2) If the cached external IPs match the provided (fetched) external
                IPs, we don't need to update the internal IPs.
            3) If the internal IPs are provided (e.g. from the provision logs),
                we use them instead of retrieving them from the cloud provider.

        Args:
            max_attempts: The maximum number of attempts to get the head IP.
            internal_ips: The internal IPs to use for the cluster. It is an
                optimization to avoid retrieving the internal IPs from the
                cloud provider. Typically, it can be parsed from the provision
                logs.
            external_ips: The external IPs to use for the cluster. Similar to
                internal_ips, it is an optimization to avoid retrieving the
                external IPs from the cloud provider.

        Raises:
            exceptions.FetchClusterInfoError: if we failed to get the cluster
                infos. e.reason is HEAD or WORKER.
        """
        if cluster_info is not None:
            self.cached_cluster_info = cluster_info
            cluster_feasible_ips = self.cached_cluster_info.get_feasible_ips()
            cluster_internal_ips = self.cached_cluster_info.get_feasible_ips(
                force_internal_ips=True)
        else:
            # For clouds that do not support the SkyPilot Provisioner API.
            # TODO(zhwu): once all the clouds are migrated to SkyPilot
            # Provisioner API, we should remove this else block
            def is_provided_ips_valid(ips: list[str | None] | None) -> bool:
                return (ips is not None and len(ips)
                        == self.num_ips_per_node * self.launched_nodes and
                        all(ip is not None for ip in ips))

            use_internal_ips = self._use_internal_ips()

            # cluster_feasible_ips is the list of IPs of the nodes in the
            # cluster which can be used to connect to the cluster. It is a list
            # of external IPs if the cluster is assigned public IPs, otherwise
            # it is a list of internal IPs.
            if is_provided_ips_valid(external_ips):
                logger.debug(f'Using provided external IPs: {external_ips}')
                cluster_feasible_ips = typing.cast(list[str], external_ips)
            else:
                cluster_feasible_ips = backend_utils.get_node_ips(
                    self.cluster_yaml,
                    self.launched_nodes,
                    head_ip_max_attempts=max_attempts,
                    worker_ip_max_attempts=max_attempts,
                    get_internal_ips=use_internal_ips)

            if self.cached_external_ips == cluster_feasible_ips:
                logger.debug(
                    'Skipping the fetching of internal IPs as the cached '
                    'external IPs matches the newly fetched ones.')
                # Optimization: If the cached external IPs are the same as the
                # retrieved feasible IPs, then we can skip retrieving internal
                # IPs since the cached IPs are up-to-date.
                return

            logger.debug(
                'Cached external IPs do not match with the newly fetched ones: '
                f'cached ({self.cached_external_ips}), new '
                f'({cluster_feasible_ips})')

            if use_internal_ips:
                # Optimization: if we know use_internal_ips is True (currently
                # only exposed for AWS and GCP), then our provisioner is
                # guaranteed to not assign public IPs, thus the first list of
                # IPs returned above are already private IPs. So skip the second
                # query.
                cluster_internal_ips = list(cluster_feasible_ips)
            elif is_provided_ips_valid(internal_ips):
                logger.debug(f'Using provided internal IPs: {internal_ips}')
                cluster_internal_ips = typing.cast(list[str], internal_ips)
            else:
                cluster_internal_ips = backend_utils.get_node_ips(
                    self.cluster_yaml,
                    self.launched_nodes,
                    head_ip_max_attempts=max_attempts,
                    worker_ip_max_attempts=max_attempts,
                    get_internal_ips=True)

        if len(cluster_feasible_ips) != len(cluster_internal_ips):
            raise AssertionError(
                f'Cluster {self.cluster_name!r}:'
                f'Expected same number of internal IPs {cluster_internal_ips}'
                f' and external IPs {cluster_feasible_ips}.')

        # List of (internal_ip, feasible_ip) tuples for all the nodes in the
        # cluster, sorted by the feasible ips. The feasible ips can be either
        # internal or external ips, depending on the use_internal_ips flag.
        internal_external_ips: list[tuple[str, str]] = list(
            # Length equality is checked immediately above.
            zip(cluster_internal_ips, cluster_feasible_ips))  # noqa: B905

        # Ensure head node is the first element, then sort based on the
        # external IPs for stableness. Skip for k8s nodes since pods
        # worker ids are already mapped.
        if (cluster_info is not None and
                cluster_info.provider_name == 'kubernetes'):
            stable_internal_external_ips = internal_external_ips
        else:
            stable_internal_external_ips = [internal_external_ips[0]] + sorted(
                internal_external_ips[1:], key=lambda x: x[1])
        self.stable_internal_external_ips = stable_internal_external_ips

    @context_utils.cancellation_guard
    # we expect different request to be acting on different clusters
    # (= different handles) so we have no real expectation of cache hit
    # across requests.
    # Do not change this cache to global scope
    # without understanding https://github.com/skypilot-org/skypilot/pull/6908
    @annotations.lru_cache(scope='request', maxsize=10)
    @timeline.event
    def get_command_runners(self,
                            force_cached: bool = False,
                            avoid_ssh_control: bool = False
                           ) -> list[command_runner.CommandRunner]:
        """Returns a list of command runners for the cluster."""
        ssh_credentials = backend_utils.ssh_credential_from_yaml(
            self.cluster_yaml, self.docker_user, self.ssh_user)
        if avoid_ssh_control:
            ssh_credentials.pop('ssh_control_name', None)

        launched_resources = self.launched_resources.assert_launchable()
        updated_to_skypilot_provisioner_after_provisioned = (
            launched_resources.cloud.PROVISIONER_VERSION
            >= clouds.ProvisionerVersion.SKYPILOT and
            self.cached_external_ips is not None and
            self.cached_cluster_info is None)
        if updated_to_skypilot_provisioner_after_provisioned:
            logger.debug(
                f'{launched_resources.cloud} has been updated to the new '
                f'provisioner after cluster {self.cluster_name} was '
                f'provisioned. Cached IPs are used for connecting to the '
                'cluster.')
        if (clouds.ProvisionerVersion.RAY_PROVISIONER_SKYPILOT_TERMINATOR
                >= launched_resources.cloud.PROVISIONER_VERSION or
                updated_to_skypilot_provisioner_after_provisioned):
            ip_list = (self.cached_external_ips
                       if force_cached else self.external_ips())
            if ip_list is None:
                return []
            # Potentially refresh the external SSH ports, in case the existing
            # cluster before #2491 was launched without external SSH ports
            # cached.
            port_list = self.external_ssh_ports()
            if len(ip_list) != len(port_list):
                raise ValueError(
                    f'Cluster {self.cluster_name!r}: expected the same number '
                    f'of SSH ports {port_list} and IPs {ip_list}.')
            runners = command_runner.SSHCommandRunner.make_runner_list(
                # Length equality is checked immediately above.
                zip(ip_list, port_list),  # noqa: B905
                **ssh_credentials)
            return runners
        if self.cached_cluster_info is None:
            # We have `and self.cached_external_ips is None` here, because
            # when a cluster's cloud is just upgraded to the new provsioner,
            # although it has the cached_external_ips, the cached_cluster_info
            # can be None. We need to update it here, even when force_cached is
            # set to True.
            # TODO: We can remove `self.cached_external_ips is None` after
            # all clouds moved to new provisioner.
            if force_cached and self.cached_external_ips is None:
                raise RuntimeError(
                    'Tried to use cached cluster info, but it\'s missing for '
                    f'cluster "{self.cluster_name}"')
            self._update_cluster_info()
        # For Kubernetes, `KubernetesCommandRunner` want to get the pod names
        # to run the command. But for high availability serve controller,
        # the controller pod is part of a deployment, and once the pod is
        # killed and a new one is created, the pod name changes, so we need
        # to manually update the cluster info here.
        # TODO(andyl): See if we can prevent this refresh. Like pass in
        # deployment name as identifier for KubernetesCommandRunner. Now this
        # is required for rsync as using deployment in rsync seems to cause
        # some unknown issues.
        # TODO(andyl): Should check through the real cluster info. Same as
        # the TODO in kubernetes/instance.py:terminate_instances
        if (isinstance(self.launched_resources.cloud, clouds.Kubernetes) and
                controller_utils.high_availability_specified(
                    self.cluster_name)):
            self._update_cluster_info()

        assert self.cached_cluster_info is not None, self
        runners = provision_lib.get_command_runners(
            self.cached_cluster_info.provider_name, self.cached_cluster_info,
            **ssh_credentials)
        return runners

    @property
    def cached_internal_ips(self) -> list[str] | None:
        if self.stable_internal_external_ips is not None:
            return [ips[0] for ips in self.stable_internal_external_ips]
        return None

    def internal_ips(self,
                     max_attempts: int = _FETCH_IP_MAX_ATTEMPTS) -> list[str]:
        internal_ips = self.cached_internal_ips
        if internal_ips is not None:
            return internal_ips
        self.update_cluster_ips(max_attempts=max_attempts)
        internal_ips = self.cached_internal_ips
        assert internal_ips is not None, 'update_cluster_ips failed.'
        return internal_ips

    @property
    def cached_external_ips(self) -> list[str] | None:
        if self.stable_internal_external_ips is not None:
            return [ips[1] for ips in self.stable_internal_external_ips]
        return None

    def external_ips(self,
                     max_attempts: int = _FETCH_IP_MAX_ATTEMPTS) -> list[str]:
        external_ips = self.cached_external_ips
        if external_ips is not None:
            return external_ips
        self.update_cluster_ips(max_attempts=max_attempts)
        external_ips = self.cached_external_ips
        assert external_ips is not None, 'update_cluster_ips failed.'
        return external_ips

    @property
    def cached_external_ssh_ports(self) -> list[int] | None:
        if self.stable_ssh_ports is not None:
            return self.stable_ssh_ports
        return None

    def external_ssh_ports(self,
                           max_attempts: int = _FETCH_IP_MAX_ATTEMPTS
                          ) -> list[int]:
        cached_ssh_ports = self.cached_external_ssh_ports
        if cached_ssh_ports is not None:
            return cached_ssh_ports
        self.update_ssh_ports(max_attempts=max_attempts)
        cached_ssh_ports = self.cached_external_ssh_ports
        assert cached_ssh_ports is not None, 'update_ssh_ports failed.'
        return cached_ssh_ports

    def get_hourly_price(self) -> float:
        hourly_cost = (self.launched_resources.get_cost(3600) *
                       self.launched_nodes)
        return hourly_cost

    def setup_docker_user(self, cluster_config_file: str):
        ip_list = self.external_ips()
        assert ip_list is not None
        docker_user = backend_utils.get_docker_user(ip_list[0],
                                                    cluster_config_file)
        self.docker_user = docker_user

    def _get_skylet_ssh_tunnel(self) -> SSHTunnelInfo | None:
        metadata = global_user_state.get_cluster_skylet_ssh_tunnel_metadata(
            self.cluster_name)
        if metadata is None:
            return None
        return SSHTunnelInfo(port=metadata[0], pid=metadata[1])

    def _set_skylet_ssh_tunnel(self, tunnel: SSHTunnelInfo | None) -> None:
        global_user_state.set_cluster_skylet_ssh_tunnel_metadata(
            self.cluster_name,
            (tunnel.port, tunnel.pid) if tunnel is not None else None)

    def close_skylet_ssh_tunnel(self) -> None:
        """Terminate the SSH tunnel process and clear its metadata."""
        tunnel = self._get_skylet_ssh_tunnel()
        if tunnel is None:
            return
        logger.debug('Closing Skylet SSH tunnel for cluster %r on port %d',
                     self.cluster_name, tunnel.port)
        try:
            self._terminate_ssh_tunnel_process(tunnel)
        finally:
            self._set_skylet_ssh_tunnel(None)

    def get_grpc_channel(self) -> 'grpc.Channel':
        grpc_options = [
            # The task YAMLs can be large, so the default
            # max_receive_message_length of 4MB might not be enough.
            ('grpc.max_receive_message_length', -1),
            # Keepalive so half-dead TCP connections (cloud LB / proxy /
            # NAT silently dropping a connection mid-stream) get failed by
            # gRPC instead of stalling a worker thread inside __next__.
            ('grpc.keepalive_time_ms', 30_000),
            ('grpc.keepalive_timeout_ms', 10_000),
            ('grpc.keepalive_permit_without_calls', 1),
        ]
        # It's fine to not grab the lock here, as we're only reading,
        # and writes are very rare.
        # It's acceptable to read while another process is opening a tunnel,
        # because it will only happen on:
        # 1. A new cluster who has no tunnel yet, or
        # 2. A cluster with an unhealthy tunnel
        # For (2), for processes that read the "stale" tunnel, it will fail
        # and on the next retry, it will call get_grpc_channel again
        # and get the new tunnel.
        tunnel = self._get_skylet_ssh_tunnel()
        if tunnel is not None:
            if _is_tunnel_healthy(tunnel):
                return grpc.insecure_channel(f'localhost:{tunnel.port}',
                                             options=grpc_options)
            logger.debug('Failed to connect to SSH tunnel for cluster '
                         f'{self.cluster_name!r} on port {tunnel.port}')

        lock_id = backend_utils.cluster_tunnel_lock_id(self.cluster_name)
        start_time = time.perf_counter()
        deadline = (start_time +
                    backend_utils.CLUSTER_TUNNEL_LOCK_TIMEOUT_SECONDS)
        attempt = 1

        def _get_remaining_timeout() -> float:
            return max(0.0, deadline - time.perf_counter())

        remaining_timeout = _get_remaining_timeout()
        while remaining_timeout > 0:
            logger.debug(
                'Attempting to acquire exclusive lock for %s (attempt %d)',
                lock_id, attempt)
            exclusive_lock = locks.get_lock(lock_id, remaining_timeout)
            try:
                with exclusive_lock.acquire(blocking=False):
                    wait_elapsed = time.perf_counter() - start_time
                    logger.debug(f'Acquired exclusive lock for {lock_id} after '
                                 f'{wait_elapsed:.2f}s')
                    # Another process may have refreshed the tunnel after our
                    # lock-free fast-path check but before we acquired the
                    # exclusive lock. Recheck while holding the lock to avoid
                    # replacing that new tunnel with a second one.
                    tunnel = self._get_skylet_ssh_tunnel()
                    if tunnel is not None and _is_tunnel_healthy(tunnel):
                        return grpc.insecure_channel(f'localhost:{tunnel.port}',
                                                     options=grpc_options)
                    try:
                        tunnel = self._open_and_update_skylet_tunnel()
                        return grpc.insecure_channel(f'localhost:{tunnel.port}',
                                                     options=grpc_options)
                    except Exception as e:  # pylint: disable=broad-except
                        # Failed to open tunnel, release the lock and retry.
                        logger.warning(f'Failed to open tunnel for cluster '
                                       f'{self.cluster_name!r}: '
                                       f'{common_utils.format_exception(e)}')
                        remaining_timeout = _get_remaining_timeout()
                        attempt += 1
                        continue
            except locks.LockTimeout:
                pass

            remaining_timeout = _get_remaining_timeout()
            logger.debug(f'Could not acquire exclusive lock for {lock_id}, '
                         f'waiting on shared lock (attempt {attempt})')
            try:
                # Use shared lock so that concurrent readers can
                # proceed in parallel.
                shared_lock = locks.get_lock(lock_id,
                                             remaining_timeout,
                                             shared_lock=True)
                # Wait for the exclusive lock to be released.
                shared_lock.acquire(blocking=True)
                # We only need the lock for signalling that the new tunnel has
                # been opened, not for checking the tunnel health.
                # Same reasoning as why we don't need to grab the lock in
                # the fast path at the start of this function.
                shared_lock.release()
                wait_elapsed = time.perf_counter() - start_time
                logger.debug(f'Acquired shared lock for {lock_id} after '
                             f'{wait_elapsed:.2f}s')
            except locks.LockTimeout as e:
                raise RuntimeError(
                    f'Failed to get gRPC channel for cluster '
                    f'{self.cluster_name!r} due to a timeout when waiting '
                    'for the SSH tunnel to be opened. Please try again or '
                    f'manually remove the lock at {lock_id}. '
                    f'{common_utils.format_exception(e)}') from e

            # Add small jitter before probing to smoothen the effects
            # of many readers waking up simultaneously.
            jitter = min(random.uniform(0.01, 0.05), _get_remaining_timeout())
            time.sleep(jitter)

            # Re-read the tunnel metadata and verify it's healthy.
            tunnel = self._get_skylet_ssh_tunnel()
            if tunnel is not None:
                if _is_tunnel_healthy(tunnel):
                    return grpc.insecure_channel(f'localhost:{tunnel.port}',
                                                 options=grpc_options)
                logger.debug('Failed to connect to SSH tunnel for cluster '
                             f'{self.cluster_name!r} on port {tunnel.port}')
            # Tunnel is still unhealthy or missing, try again with updated
            # timeout. This could happen in the case where the thread who
            # held the exclusive lock to open the tunnel crashed.
            remaining_timeout = _get_remaining_timeout()
            attempt += 1
        raise RuntimeError('Timeout waiting for gRPC channel for cluster '
                           f'{self.cluster_name!r} to be ready.')

    def _terminate_ssh_tunnel_process(self, tunnel_info: SSHTunnelInfo) -> None:
        """Terminate the SSH tunnel process."""
        try:
            proc = psutil.Process(tunnel_info.pid)
            if proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
                logger.debug(
                    f'Terminating SSH tunnel process {tunnel_info.pid}')
                subprocess_utils.kill_children_processes(proc.pid)
        except psutil.NoSuchProcess:
            pass
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                f'Failed to cleanup SSH tunnel process {tunnel_info.pid}: {e}')

    def _open_and_update_skylet_tunnel(self) -> SSHTunnelInfo:
        """Opens an SSH tunnel to the Skylet on the head node,
        updates the cluster handle, and persists it to the database."""
        max_attempts = 3
        tunnel_info = None
        # There could be a race condition here, as multiple processes may
        # attempt to open the same port at the same time.
        for attempt in range(max_attempts):
            runners = self.get_command_runners()
            head_runner = runners[0]
            local_port = random.randint(10000, 65535)
            try:
                ssh_tunnel_proc = backend_utils.open_ssh_tunnel(
                    head_runner, (local_port, constants.SKYLET_GRPC_PORT))
            except exceptions.CommandError as e:
                # Don't retry if the error is due to timeout,
                # connection refused, Kubernetes pods not found,
                # or an in-progress termination.
                if (e.detailed_reason is not None and
                    (backend_utils.SSH_CONNECTION_ERROR_PATTERN.search(
                        e.detailed_reason) or
                     backend_utils.K8S_PODS_NOT_FOUND_PATTERN.search(
                         e.detailed_reason) or attempt == max_attempts - 1)):
                    raise e
                logger.warning(
                    f'Failed to open SSH tunnel on port {local_port} '
                    f'({attempt + 1}/{max_attempts}). '
                    f'{e.error_msg}\n{e.detailed_reason}')
                continue
            tunnel_info = SSHTunnelInfo(port=local_port,
                                        pid=ssh_tunnel_proc.pid)
            break
        else:
            raise RuntimeError('Failed to open an SSH tunnel after '
                               f'{max_attempts} attempts.')
        if tunnel_info is None:
            raise RuntimeError('Failed to open an SSH tunnel.')

        try:
            grpc.channel_ready_future(
                grpc.insecure_channel(f'localhost:{tunnel_info.port}')).result(
                    timeout=constants.SKYLET_GRPC_TIMEOUT_SECONDS)
            # Clean up existing tunnel before setting up the new one.
            old_tunnel = self._get_skylet_ssh_tunnel()
            if old_tunnel is not None:
                self._terminate_ssh_tunnel_process(old_tunnel)
            self._set_skylet_ssh_tunnel(tunnel_info)
            return tunnel_info
        except grpc.FutureTimeoutError as e:
            self._terminate_ssh_tunnel_process(tunnel_info)
            logger.warning(
                f'Skylet gRPC channel for cluster {self.cluster_name} not '
                f'ready after {constants.SKYLET_GRPC_TIMEOUT_SECONDS}s')
            raise e
        except Exception as e:
            self._terminate_ssh_tunnel_process(tunnel_info)
            raise e

    @property
    def cluster_yaml(self) -> str | None:
        if self._cluster_yaml is None:
            return None
        return os.path.expanduser(self._cluster_yaml)

    @cluster_yaml.setter
    def cluster_yaml(self, value: str | None):
        self._cluster_yaml = value

    @property
    def instance_ids(self):
        if self.cached_cluster_info is not None:
            return self.cached_cluster_info.instance_ids()
        return None

    @property
    def ssh_user(self):
        if self.cached_cluster_info is not None:
            # Overload ssh_user with the user stored in cluster_info, which is
            # useful for kubernetes case, where the ssh_user can depend on the
            # container image used. For those clusters launched with ray
            # autoscaler, we directly use the ssh_user in yaml config.
            return self.cached_cluster_info.ssh_user
        return getattr(self, '_ssh_user', None)

    @property
    def head_ip(self):
        external_ips = self.cached_external_ips
        if external_ips is not None:
            return external_ips[0]
        return None

    @property
    def head_ssh_port(self):
        external_ssh_ports = self.cached_external_ssh_ports
        if external_ssh_ports:
            return external_ssh_ports[0]
        return None

    @property
    def num_ips_per_node(self) -> int:
        """Returns number of IPs per node in the cluster, handling TPU Pod."""
        is_tpu_vm_pod = gcp_utils.is_tpu_vm_pod(self.launched_resources)
        if is_tpu_vm_pod:
            num_ips = len(self.internal_ips())
        else:
            num_ips = 1
        return num_ips

    @property
    def is_grpc_enabled_with_flag(self) -> bool:
        """Returns whether this handle has gRPC enabled and gRPC flag is set."""
        return (env_options.Options.ENABLE_GRPC.get() and
                self.is_grpc_enabled and
                not isinstance(self.launched_resources.cloud, clouds.Slurm))

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict."""
        return {
            'cluster_name': self.cluster_name,
            'cluster_name_on_cloud': self.cluster_name_on_cloud,
            'cluster_yaml': self._cluster_yaml,
            'launched_nodes': self.launched_nodes,
            'launched_resources':
                (self.launched_resources.to_yaml_config()
                 if self.launched_resources is not None else None),
            'stable_internal_external_ips': self.stable_internal_external_ips,
            'stable_ssh_ports': self.stable_ssh_ports,
            'docker_user': self.docker_user,
            'is_grpc_enabled': self.is_grpc_enabled,
            'ssh_user': self.ssh_user,
            'provision_runtime_metadata': dataclasses.asdict(
                self.provision_runtime_metadata),
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'CloudVmRayResourceHandle':
        """Reconstruct from a dict produced by to_dict()."""
        resources_dict = d.get('launched_resources')
        launched_resources: resources_lib.Resources | None
        if resources_dict is not None:
            launched_resources = (
                resources_lib.Resources._from_yaml_config_single(  # pylint: disable=protected-access
                    resources_dict.copy(),
                    _allow_resolved_container_image=True))
        else:
            launched_resources = None

        handle = cls.__new__(cls)
        handle._version = cls._VERSION
        handle.cluster_name = d['cluster_name']
        handle.cluster_name_on_cloud = d.get('cluster_name_on_cloud', '')
        handle._cluster_yaml = d.get('cluster_yaml')
        handle.launched_nodes = d.get('launched_nodes', 0)
        handle.launched_resources = launched_resources  # type: ignore
        handle.stable_internal_external_ips = d.get(
            'stable_internal_external_ips')
        handle.stable_ssh_ports = d.get('stable_ssh_ports')
        handle.docker_user = d.get('docker_user')
        handle.is_grpc_enabled = d.get('is_grpc_enabled', True)
        handle.cached_cluster_info = None
        handle._ssh_user = d.get('ssh_user')
        runtime_metadata = d.get('provision_runtime_metadata')
        if runtime_metadata is not None:
            known = {
                f.name for f in dataclasses.fields(
                    provision_common.ProvisionRuntimeMetadata)
            }
            handle.provision_runtime_metadata = (
                provision_common.ProvisionRuntimeMetadata(**{
                    k: v for k, v in runtime_metadata.items() if k in known
                }))
        else:
            handle.provision_runtime_metadata = (
                provision_common.ProvisionRuntimeMetadata())
        return handle

    def __getstate__(self):
        state = self.__dict__.copy()
        # For backwards compatibility. Refer to
        # https://github.com/skypilot-org/skypilot/pull/7133
        state.setdefault('skylet_ssh_tunnel', None)
        # Serialize provision_runtime_metadata as a plain dict.
        runtime_metadata = state.get('provision_runtime_metadata')
        if isinstance(runtime_metadata,
                      provision_common.ProvisionRuntimeMetadata):
            state['provision_runtime_metadata'] = dataclasses.asdict(
                runtime_metadata)
        return state

    def __setstate__(self, state):
        self._version = self._VERSION

        version = state.pop('_version', None)
        if version is None:
            version = -1
            state.pop('cluster_region', None)
        if version < 2:
            state['_cluster_yaml'] = state.pop('cluster_yaml')
        head_ip = None
        if version < 3:
            head_ip = state.pop('head_ip', None)
            state['stable_internal_external_ips'] = None
        if version < 4:
            # Version 4 adds self.stable_ssh_ports for Kubernetes support
            state['stable_ssh_ports'] = None
        if version < 5:
            state['docker_user'] = None

        if version < 6:
            state['cluster_name_on_cloud'] = state['cluster_name']

        if version < 8:
            self.cached_cluster_info = None

        if version < 9:
            # For backward compatibility, we should update the region of a
            # SkyPilot cluster on Kubernetes to the actual context it is using.
            # pylint: disable=import-outside-toplevel
            launched_resources = state['launched_resources']
            if isinstance(launched_resources.cloud, clouds.Kubernetes):
                yaml_config = global_user_state.get_cluster_yaml_dict(
                    os.path.expanduser(state['_cluster_yaml']))
                context = kubernetes_utils.get_context_from_config(
                    yaml_config['provider'])
                state['launched_resources'] = launched_resources.copy(
                    region=context)

        if version < 10:
            # In #4660, we keep the cluster entry in the database even when it
            # is in the transition from one region to another during the
            # failover. We allow `handle.cluster_yaml` to be None to indicate
            # that the cluster yaml is intentionally removed. Before that PR,
            # the `handle.cluster_yaml` is always not None, even if it is
            # intentionally removed.
            #
            # For backward compatibility, we set the `_cluster_yaml` to None
            # if the file does not exist, assuming all the removal of the
            # _cluster_yaml for existing clusters are intentional by SkyPilot.
            # are intentional by SkyPilot.
            if state['_cluster_yaml'] is not None and not os.path.exists(
                    os.path.expanduser(state['_cluster_yaml'])):
                state['_cluster_yaml'] = None

        if version < 11:
            state['is_grpc_enabled'] = False
            state['skylet_ssh_tunnel'] = None

        if version >= 12:
            # DEPRECATED in favor of skylet_ssh_tunnel_metadata column in the DB
            state.pop('skylet_ssh_tunnel', None)

        # provision_runtime_metadata is serialized as a plain dict (see
        # __getstate__). Reconstruct the dataclass here, defaulting if absent.
        runtime_metadata = state.get('provision_runtime_metadata')
        if isinstance(runtime_metadata, dict):
            known = {
                f.name for f in dataclasses.fields(
                    provision_common.ProvisionRuntimeMetadata)
            }
            state['provision_runtime_metadata'] = (
                provision_common.ProvisionRuntimeMetadata(**{
                    k: v for k, v in runtime_metadata.items() if k in known
                }))
        elif runtime_metadata is None:
            state['provision_runtime_metadata'] = (
                provision_common.ProvisionRuntimeMetadata())

        self.__dict__.update(state)

        # Because the update_cluster_ips and update_ssh_ports
        # functions use the handle, we call it on the current instance
        # after the state is updated.
        if version < 3 and head_ip is not None:
            try:
                self.update_cluster_ips()
            except exceptions.FetchClusterInfoError:
                # This occurs when an old cluster from was autostopped,
                # so the head IP in the database is not updated.
                pass
        if version < 4:
            self.update_ssh_ports()

        if version < 8:
            try:
                self._update_cluster_info()
            except exceptions.FetchClusterInfoError:
                # This occurs when an old cluster from was autostopped,
                # so the head IP in the database is not updated.
                pass


class LocalResourcesHandle(CloudVmRayResourceHandle):
    """A handle for local resources."""

    def __init__(
            self,
            *,
            cluster_name: str,
            cluster_name_on_cloud: str,
            cluster_yaml: str | None,
            launched_nodes: int,
            launched_resources: resources_lib.Resources,
            stable_internal_external_ips: list[tuple[str, str]] | None = None,
            stable_ssh_ports: list[int] | None = None,
            cluster_info: provision_common.ClusterInfo | None = None) -> None:
        super().__init__(
            cluster_name=cluster_name,
            cluster_name_on_cloud=cluster_name_on_cloud,
            cluster_yaml=cluster_yaml,
            launched_nodes=launched_nodes,
            launched_resources=launched_resources,
            stable_internal_external_ips=stable_internal_external_ips,
            stable_ssh_ports=stable_ssh_ports,
            cluster_info=cluster_info)
        # TODO (kyuds): handle jobs consolidation mode. Currently,
        # jobs consolidation mode will not run a skylet, hence
        # grpc server will not run. In the future, we should
        # figure out a way to start grpc in consolidation mode.
        self.is_grpc_enabled = False

    @context_utils.cancellation_guard
    # we expect different request to be acting on different clusters
    # (= different handles) so we have no real expectation of cache hit
    # across requests.
    # Do not change this cache to global scope
    # without understanding https://github.com/skypilot-org/skypilot/pull/6908
    @annotations.lru_cache(scope='request', maxsize=10)
    @timeline.event
    def get_command_runners(self,
                            force_cached: bool = False,
                            avoid_ssh_control: bool = False
                           ) -> list[command_runner.CommandRunner]:
        """Returns a list of local command runners."""
        del force_cached, avoid_ssh_control  # Unused.
        return [command_runner.LocalProcessCommandRunner()]


_CancelAwareStub = skylet_client._CancelAwareStub  # pylint: disable=protected-access
SkyletClient = skylet_client.SkyletClient


@registry.BACKEND_REGISTRY.type_register(name='cloudvmray')
class CloudVmRayBackend(backends.Backend['CloudVmRayResourceHandle']):
    """Backend: runs on cloud virtual machines, managed by Ray.

    Changing this class may also require updates to:
      * Cloud providers' templates under config/
      * Cloud providers' implementations under clouds/
    """

    NAME = 'cloudvmray'

    # Backward compatibility, with the old name of the handle.
    ResourceHandle = CloudVmRayResourceHandle  # type: ignore

    def __init__(self):
        self.run_timestamp = sky_logging.get_run_timestamp()
        # NOTE: do not expanduser() here, as this '~/...' path is used for
        # remote as well to be expanded on the remote side.
        self.log_dir = os.path.join(constants.SKY_LOGS_DIRECTORY,
                                    self.run_timestamp)
        # Do not make directories to avoid create folder for commands that
        # do not need it (`sky status`, `sky logs` ...)
        # os.makedirs(self.log_dir, exist_ok=True)

        self._dag = None
        self._optimize_target = None
        self._requested_features = set()
        self._dump_final_script = False
        self._is_managed = False
        self._extra_launch_context: dict[str, Any] = {}
        self._is_launched_by_jobs_controller = False
        self._workload_type = 'cluster'
        # Optional planner (via register_info): used under the per-cluster lock
        # to produce a fresh concrete plan when neither a reusable snapshot nor
        # a caller plan is available.
        self._planner = None

        # Command for running the setup script. It is only set when the
        # setup needs to be run outside the self._setup() and as part of
        # a job (detach_setup, default).
        self._setup_cmd = None

    # --- Implementation of Backend APIs ---

    def register_info(self, **kwargs) -> None:
        self._dag = kwargs.pop('dag', self._dag)
        self._optimize_target = kwargs.pop(
            'optimize_target',
            self._optimize_target) or common.OptimizeTarget.COST
        self._requested_features = kwargs.pop('requested_features',
                                              self._requested_features)
        self._dump_final_script = kwargs.pop('dump_final_script', False)
        self._is_managed = kwargs.pop('is_managed', False)
        self._extra_launch_context = kwargs.pop('extra_launch_context', {})
        self._is_launched_by_jobs_controller = kwargs.pop(
            'is_launched_by_jobs_controller', False)
        self._workload_type = kwargs.pop('workload_type', 'cluster')
        # Optional planner callback for a fresh plan under lock when no
        # reusable snapshot/caller plan exists. Keeps optimizer in upper layer.
        self._planner = kwargs.pop('planner', self._planner)
        assert not kwargs, f'Unexpected kwargs: {kwargs}'

    def check_resources_fit_cluster(
        self,
        handle: CloudVmRayResourceHandle,
        task: task_lib.Task,
        check_ports: bool = False,
    ) -> resources_lib.Resources:
        """Check if resources requested by the task fit the cluster.

        The resources requested by the task should be smaller than the existing
        cluster.
        If multiple resources are specified, this checking will pass when
        at least one resource fits the cluster.

        Raises:
            exceptions.ResourcesMismatchError: If the resources in the task
                does not match the existing cluster.
        """

        launched_resources = handle.launched_resources
        cluster_name = handle.cluster_name

        # Usage Collection:
        usage_lib.messages.usage.update_cluster_resources(
            handle.launched_nodes, launched_resources)
        status = global_user_state.get_status_from_cluster_name(cluster_name)
        if status is not None:
            usage_lib.messages.usage.update_cluster_status(status)

        assert launched_resources.region is not None, handle

        mismatch_str = (f'To fix: specify a new cluster name, or down the '
                        f'existing cluster first: sky down {cluster_name}')
        valid_resource = None
        requested_resource_list = []
        for resource in task.resources:
            if (task.num_nodes <= handle.launched_nodes and
                    resource.less_demanding_than(
                        launched_resources,
                        requested_num_nodes=task.num_nodes,
                        check_ports=check_ports)):
                valid_resource = resource
                break
            else:
                requested_resource_list.append(f'{task.num_nodes}x {resource}')

        if valid_resource is None:
            for example_resource in task.resources:
                if (example_resource.region is not None and
                        example_resource.region != launched_resources.region):
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.ResourcesMismatchError(
                            f'Task requested resources {example_resource} in region '  # pylint: disable=line-too-long
                            f'{example_resource.region!r}'
                            ', but the existing cluster '
                            f'is in region {launched_resources.region!r}.')
                if (example_resource.zone is not None and
                        example_resource.zone != launched_resources.zone):
                    zone_str = (f'is in zone {launched_resources.zone!r}.'
                                if launched_resources.zone is not None else
                                'does not have zone specified.')
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.ResourcesMismatchError(
                            f'Task requested resources {example_resource} in zone '  # pylint: disable=line-too-long
                            f'{example_resource.zone!r},'
                            'but the existing cluster '
                            f'{zone_str}')
                if (example_resource.requires_fuse and
                        not launched_resources.requires_fuse):
                    # Will not be reached for non-k8s case since the
                    # less_demanding_than only fails fuse requirement when
                    # the cloud is Kubernetes AND the cluster doesn't have fuse.
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.ResourcesMismatchError(
                            'Task requires FUSE support for mounting object '
                            'stores, but the existing cluster with '
                            f'{launched_resources!r} does not support FUSE '
                            f'mounting. Launch a new cluster to run this task.')
            requested_resource_str = ', '.join(requested_resource_list)
            if isinstance(task.resources, list):
                requested_resource_str = f'[{requested_resource_str}]'
            elif isinstance(task.resources, set):
                requested_resource_str = f'{{{requested_resource_str}}}'
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesMismatchError(
                    'Requested resources do not match the existing '
                    'cluster.\n'
                    f'  Requested:\t{requested_resource_str}\n'
                    f'  Existing:\t{handle.launched_nodes}x '
                    f'{handle.launched_resources}\n'
                    f'{mismatch_str}')
        else:
            # For fractional acc count clusters, we round up the number of accs
            # to 1 (sky/utils/resources_utils.py::make_ray_custom_resources_str)
            # Here we scale the required acc count to (required / launched) * 1
            # so the total number of accs is the same as the requested number.
            launched_accs = launched_resources.accelerators
            if (launched_accs is not None and
                    valid_resource.accelerators is not None):
                for _, count in launched_accs.items():
                    if isinstance(count, float) and not count.is_integer():
                        valid_resource = valid_resource.copy(
                            accelerators={
                                k: v / count
                                for k, v in valid_resource.accelerators.items()
                            })
        return valid_resource

    def _provision(
        self,
        task: task_lib.Task,
        to_provision: resources_lib.Resources | None,
        dryrun: bool,
        stream_logs: bool,
        cluster_name: str,
        retry_until_up: bool = False,
        skip_unnecessary_provisioning: bool = False,
    ) -> tuple[CloudVmRayResourceHandle | None, bool]:
        """Provisions the cluster, or re-provisions an existing cluster.

        Use the SKYPILOT provisioner if it's supported by the cloud, otherwise
        use 'ray up'.

        See also docstring for Backend.provision().

        Raises:
            exceptions.ClusterOwnerIdentityMismatchError: if the cluster
                'cluster_name' exists and is owned by another user.
            exceptions.InvalidClusterNameError: if the cluster name is invalid.
            exceptions.ResourcesMismatchError: if the requested resources
                do not match the existing cluster.
            exceptions.ResourcesUnavailableError: if the requested resources
                cannot be satisfied. The failover_history of the exception
                will be set as at least 1 exception from either our pre-checks
                (e.g., cluster name invalid) or a region/zone throwing
                resource unavailability.
            exceptions.CommandError: any ssh command error.
            RuntimeError: raised when 'rsync' is not installed.
            # TODO(zhwu): complete the list of exceptions.
        """
        # FIXME: ray up for Azure with different cluster_names will overwrite
        # each other.
        # When rsync is not installed in the user's machine, Ray will
        # silently retry to up the node for _MAX_RAY_UP_RETRY number
        # of times. This is time consuming so we fail early.
        backend_utils.check_rsync_installed()
        # Check if the cluster is owned by the current user. Raise
        # exceptions.ClusterOwnerIdentityMismatchError
        backend_utils.check_owner_identity(cluster_name)
        lock_id = backend_utils.cluster_status_lock_id(cluster_name)
        communicated_with_user = False

        while True:
            try:
                return self._locked_provision(lock_id, task, to_provision,
                                              dryrun, stream_logs, cluster_name,
                                              retry_until_up,
                                              skip_unnecessary_provisioning)
            except locks.LockTimeout:
                if not communicated_with_user:
                    rich_utils.force_update_status(
                        ux_utils.spinner_message('Launching - blocked by ' +
                                                 'other requests ' +
                                                 colorama.Style.RESET_ALL +
                                                 colorama.Style.DIM +
                                                 'Check concurrent requests: ' +
                                                 'sky api status -v | grep '
                                                 f'{cluster_name}'))

    def _maybe_clear_external_cluster_failures(
            self, cluster_name: str,
            prev_cluster_status: status_lib.ClusterStatus | None) -> None:
        """Clear any existing cluster failures when reusing a cluster.

        Clear any existing cluster failures when reusing a cluster. This ensures
        that when a cluster failure is detected (causing the cluster to be
        marked as INIT), the user can recover the cluster via `sky start` or
        `sky launch` and clear the failure.
        """
        if prev_cluster_status is not None:
            failures = ExternalFailureSource.clear(cluster_name=cluster_name)
            if failures:
                failure_details = [f'"{f["failure_mode"]}"' for f in failures]
                plural = 's' if len(failures) > 1 else ''
                logger.info(f'{colorama.Style.DIM}Cleared {len(failures)} '
                            f'existing cluster failure{plural} for cluster '
                            f'{cluster_name!r}: {", ".join(failure_details)}'
                            f'{colorama.Style.RESET_ALL}')

    def check_skylet_running(self, handle: CloudVmRayResourceHandle):
        # For backward compatibility and robustness of skylet, it is checked
        # and restarted if necessary.
        logger.debug('Checking if skylet is running on the head node.')
        with rich_utils.safe_status(
                ux_utils.spinner_message('Preparing SkyPilot runtime')):
            # We need to source bashrc for skylet to make sure the autostop
            # event can access the path to the cloud CLIs.
            self.run_on_head(handle,
                             instance_setup.MAYBE_SKYLET_RESTART_CMD,
                             source_bashrc=True)

    def _locked_provision(
        self,
        lock_id: str,
        task: task_lib.Task,
        to_provision: resources_lib.Resources | None,
        dryrun: bool,
        stream_logs: bool,
        cluster_name: str,
        retry_until_up: bool = False,
        skip_unnecessary_provisioning: bool = False,
    ) -> tuple[CloudVmRayResourceHandle | None, bool]:
        with contextlib.ExitStack() as lock_stack:
            lock_stack.enter_context(
                lock_events.DistributedLockEvent(lock_id,
                                                 _CLUSTER_LOCK_TIMEOUT))
            if not dryrun:
                resource_lock_id = (
                    backend_utils.cluster_resource_operation_lock_id(
                        cluster_name))
                lock_stack.enter_context(
                    lock_events.DistributedLockEvent(resource_lock_id,
                                                     _CLUSTER_LOCK_TIMEOUT))
            # Reset spinner message to remove any mention of being blocked
            # by other requests.
            rich_utils.force_update_status(
                ux_utils.spinner_message('Launching'))

            # Try to launch the exiting cluster first. If no existing
            # cluster, this function will create a to_provision_config
            # with required resources.
            to_provision_config = self._check_existing_cluster(
                task, to_provision, cluster_name, dryrun)
            assert to_provision_config.resources is not None, (
                'to_provision should not be None', to_provision_config)

            prev_cluster_status = to_provision_config.prev_cluster_status
            usage_lib.messages.usage.update_cluster_resources(
                to_provision_config.num_nodes, to_provision_config.resources)
            usage_lib.messages.usage.update_cluster_status(prev_cluster_status)

            self._maybe_clear_external_cluster_failures(cluster_name,
                                                        prev_cluster_status)

            # TODO(suquark): once we have sky on PyPI, we should directly
            # install sky from PyPI.
            # NOTE: can take ~2s.
            with timeline.Event('backend.provision.wheel_build'):
                # TODO(suquark): once we have sky on PyPI, we should directly
                # install sky from PyPI.
                local_wheel_path, wheel_hash = wheel_utils.build_sky_wheel()
            while True:
                # For on-demand instances, RetryingVmProvisioner will retry
                # within the given region first, then optionally retry on all
                # other clouds and regions (if backend.register_info()
                # has been called).
                # For spot instances, each provisioning request is made for a
                # single zone and the provisioner will retry on all other
                # clouds, regions, and zones.
                # See optimizer.py#_make_launchables_for_valid_region_zones()
                # for detailed reasons.

                # After this "round" of optimization across clouds, provisioning
                # may still have not succeeded. This while loop will then kick
                # in if retry_until_up is set, which will kick off new "rounds"
                # of optimization infinitely.
                retry_provisioner: RetryingVmProvisioner | None = None
                try:
                    retry_provisioner = RetryingVmProvisioner(
                        self.log_dir,
                        self._dag,  # type: ignore[arg-type]
                        self._optimize_target,  # type: ignore[arg-type]
                        self._requested_features,
                        local_wheel_path,
                        wheel_hash,
                        blocked_resources=task.blocked_resources,
                        is_managed=self._is_managed,
                        extra_launch_context=self._extra_launch_context,
                        is_launched_by_jobs_controller=(
                            self._is_launched_by_jobs_controller),
                        workload_type=self._workload_type)
                    log_path = os.path.join(self.log_dir, 'provision.log')
                    rich_utils.force_update_status(
                        ux_utils.spinner_message('Launching',
                                                 log_path,
                                                 cluster_name=cluster_name))
                    config_dict = retry_provisioner.provision_with_retries(
                        task, to_provision_config, dryrun, stream_logs,
                        skip_unnecessary_provisioning)
                    break
                except exceptions.ResourcesUnavailableError as e:
                    failed_cluster_hash = (retry_provisioner.active_cluster_hash
                                           if retry_provisioner is not None else
                                           None)
                    log_path = os.path.join(self.log_dir, 'provision.log')

                    error_message = (
                        f'{colorama.Fore.RED}Failed to provision all '
                        f'possible launchable resources.'
                        f'{colorama.Style.RESET_ALL}'
                        ' Relax the task\'s resource requirements: '
                        f'{task.num_nodes}x {list(task.resources)[0]}')
                    if e.no_failover:
                        error_message = str(e)

                    if retry_until_up:
                        gap_seconds = _RETRY_UNTIL_UP_INIT_GAP_SECONDS
                        retry_message = ux_utils.retry_message(
                            f'Retry after {gap_seconds:.0f}s ')
                        hint_message = (
                            f'\n{retry_message} '
                            f'{ux_utils.provision_hint(cluster_name)}'
                            f'{colorama.Style.RESET_ALL}')

                        # Add cluster event for retry only if this run owns a
                        # cluster generation.
                        if failed_cluster_hash is not None:
                            global_user_state.add_cluster_event(
                                cluster_name,
                                status_lib.ClusterStatus.INIT,
                                f'Retrying provisioning after '
                                f'{gap_seconds:.0f}s',
                                global_user_state.ClusterEventType.
                                STATUS_CHANGE,
                                existing_cluster_hash=failed_cluster_hash)

                        raise exceptions.ExecutionRetryableError(
                            error_message,
                            hint=hint_message,
                            retry_wait_seconds=gap_seconds)
                    # Clean up the cluster's entry in `sky status`.
                    # Do not remove the stopped cluster from the global state
                    # if failed to start.
                    if not e.no_failover:
                        if failed_cluster_hash is not None:
                            global_user_state.add_cluster_event(
                                cluster_name,
                                None,
                                'Provision failed: ' + str(e),
                                global_user_state.ClusterEventType.
                                STATUS_CHANGE,
                                nop_if_duplicate=True,
                                existing_cluster_hash=failed_cluster_hash)
                            global_user_state.remove_cluster(
                                cluster_name,
                                terminate=True,
                                existing_cluster_hash=failed_cluster_hash)
                        usage_lib.messages.usage.update_final_cluster_status(
                            None)
                    logger.error(
                        ux_utils.error_message(
                            'Failed to provision resources. '
                            f'{ux_utils.provision_hint(cluster_name)}'))
                    error_message += (
                        '\nTo keep retrying until the cluster is up, use '
                        'the `--retry-until-up` flag.')
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.ResourcesUnavailableError(
                            error_message + '\n' + str(e),
                            failover_history=e.failover_history) from None
            if dryrun:
                handle = global_user_state.get_handle_from_cluster_name(
                    cluster_name)
                return handle if handle is not None else None, False

            if config_dict['provisioning_skipped']:
                # Skip further provisioning.
                # In this case, we won't have certain fields in the config_dict
                # ('handle', 'provision_record', 'resources_vars')
                # We need to return the handle - but it should be the existing
                # handle for the cluster.
                cluster_hash = config_dict.get('cluster_hash')
                handle = global_user_state.get_handle_from_cluster_name(
                    cluster_name, existing_cluster_hash=cluster_hash)
                if handle is None:
                    raise exceptions.ClusterDoesNotExist(
                        f'Cluster {cluster_name!r} was removed or replaced '
                        'while provisioning was in progress.')
                return handle, True

            config_hash = config_dict.get('config_hash')
            cluster_hash = config_dict['cluster_hash']
            if 'provision_record' in config_dict:
                # New provisioner is used here.
                handle = config_dict['handle']
                provision_record = config_dict['provision_record']
                runtime_metadata = provision_record.runtime_metadata
                handle.provision_runtime_metadata = runtime_metadata
                resources_vars = config_dict['resources_vars']
                if runtime_metadata.runtime_setup_done:
                    logger.info('Skipping runtime setup: provisioner reported '
                                'SkyPilot runtime is ready.')
                    logger.info(
                        ux_utils.finishing_message(
                            f'Cluster launched: {handle.cluster_name}.',
                            cluster_name=str(handle.cluster_name)))
                    config_from_yaml = global_user_state.get_cluster_yaml_dict(
                        handle.cluster_yaml)
                    cluster_info = provision_lib.get_cluster_info(
                        repr(handle.launched_resources.cloud),
                        provision_record.region,
                        handle.cluster_name_on_cloud,
                        provider_config=config_from_yaml.get('provider'))
                else:
                    # Setup SkyPilot runtime after the cluster is provisioned
                    # 1. Wait for SSH to be ready.
                    # 2. Mount the cloud credentials, skypilot wheel,
                    #    and other necessary files to the VM.
                    # 3. Run setup commands to install dependencies.
                    # 4. Starting ray cluster and skylet.

                    # Add cluster event for runtime setup start
                    global_user_state.add_cluster_event(
                        handle.cluster_name,
                        status_lib.ClusterStatus.INIT,
                        'Setting up SkyPilot runtime on cluster',
                        global_user_state.ClusterEventType.STATUS_CHANGE,
                        existing_cluster_hash=cluster_hash)

                    cluster_info = provisioner.post_provision_runtime_setup(
                        handle.launched_resources,
                        resources_utils.ClusterName(
                            handle.cluster_name, handle.cluster_name_on_cloud),
                        handle.cluster_yaml,
                        provision_record=provision_record,
                        custom_resource=resources_vars.get('custom_resources'),
                        log_dir=self.log_dir,
                        existing_cluster_hash=cluster_hash)
                # We use the IPs from the cluster_info to update_cluster_ips,
                # when the provisioning is done, to make sure the cluster IPs
                # are up-to-date.
                # The staled IPs may be caused by the node being restarted
                # manually or by the cloud provider.
                # Optimize the case where the cluster's IPs can be retrieved
                # from cluster_info.
                handle.cached_cluster_info = cluster_info
                handle.docker_user = cluster_info.docker_user
                handle.update_cluster_ips(max_attempts=_FETCH_IP_MAX_ATTEMPTS,
                                          cluster_info=cluster_info)
                handle.update_ssh_ports(max_attempts=_FETCH_IP_MAX_ATTEMPTS)

                # Update launched resources.
                handle.launched_resources = handle.launched_resources.copy(
                    region=provision_record.region, zone=provision_record.zone)

                self._update_after_cluster_provisioned(
                    handle, to_provision_config.prev_handle, task,
                    prev_cluster_status, config_hash, cluster_hash)
                return handle, False

            cluster_config_file = config_dict['ray']
            handle = config_dict['handle']

            ip_list = handle.external_ips()
            ssh_port_list = handle.external_ssh_ports()
            assert ip_list is not None, handle
            assert ssh_port_list is not None, handle
            config = global_user_state.get_cluster_yaml_dict(
                cluster_config_file)
            if 'docker' in config:
                handle.setup_docker_user(cluster_config_file)

            # Get actual zone info and save it into handle.
            # NOTE: querying zones is expensive, observed 1node GCP >=4s.
            zone = handle.launched_resources.zone
            if zone is None:
                get_zone_cmd = (
                    handle.launched_resources.cloud.get_zone_shell_cmd())
                # zone is None for Azure
                if get_zone_cmd is not None:
                    runners = handle.get_command_runners()

                    def _get_zone(runner):
                        retry_count = 0
                        backoff = common_utils.Backoff(initial_backoff=1,
                                                       max_backoff_factor=3)
                        while True:
                            returncode, stdout, stderr = runner.run(
                                get_zone_cmd,
                                require_outputs=True,
                                stream_logs=False)
                            if returncode == 0:
                                break
                            retry_count += 1
                            if retry_count <= _MAX_GET_ZONE_RETRY:
                                time.sleep(backoff.current_backoff())
                                continue
                        subprocess_utils.handle_returncode(
                            returncode,
                            get_zone_cmd,
                            f'Failed to get zone for {cluster_name!r}',
                            stderr=stderr,
                            stream_logs=stream_logs)
                        return stdout.strip()

                    zones = subprocess_utils.run_in_parallel(_get_zone, runners)
                    if len(set(zones)) == 1:
                        # zone will be checked during Resources cls
                        # initialization.
                        handle.launched_resources = (
                            handle.launched_resources.copy(zone=zones[0]))
                    # If the number of zones > 1, nodes in the cluster are
                    # launched in different zones (legacy clusters before
                    # #1700), leave the zone field of handle.launched_resources
                    # to None.

            # For backward compatibility and robustness of skylet, it is checked
            # and restarted if necessary.
            self.check_skylet_running(handle)

            self._update_after_cluster_provisioned(
                handle, to_provision_config.prev_handle, task,
                prev_cluster_status, config_hash, cluster_hash)
            return handle, False

    def _open_ports(self, handle: CloudVmRayResourceHandle) -> None:
        cloud = handle.launched_resources.cloud
        logger.debug(
            f'Opening ports {handle.launched_resources.ports} for {cloud}')
        config = global_user_state.get_cluster_yaml_dict(handle.cluster_yaml)
        provider_config = config['provider']
        cluster_config_overrides = (
            handle.launched_resources.cluster_config_overrides)
        if cluster_config_overrides:
            provider_config['cluster_config_overrides'] = (
                cluster_config_overrides)
        provision_lib.open_ports(repr(cloud), handle.cluster_name_on_cloud,
                                 handle.launched_resources.ports,
                                 provider_config)

    def _update_after_cluster_provisioned(
            self, handle: CloudVmRayResourceHandle,
            prev_handle: CloudVmRayResourceHandle | None, task: task_lib.Task,
            prev_cluster_status: status_lib.ClusterStatus | None,
            config_hash: str | None, cluster_hash: str) -> None:
        usage_lib.messages.usage.update_cluster_resources(
            handle.launched_nodes, handle.launched_resources)
        usage_lib.messages.usage.update_final_cluster_status(
            status_lib.ClusterStatus.UP)

        runtime_metadata = handle.provision_runtime_metadata

        # Update job queue to avoid stale jobs (when restarted), before
        # setting the cluster to be ready.
        if (prev_cluster_status == status_lib.ClusterStatus.INIT and
                runtime_metadata.has_job_queue):
            # update_status will query the ray job status for all INIT /
            # PENDING / RUNNING jobs for the real status, since we do not
            # know the actual previous status of the cluster.
            logger.debug('Update job queue on remote cluster.')
            with rich_utils.safe_status(
                    ux_utils.spinner_message('Preparing SkyPilot runtime')):
                use_legacy = not handle.is_grpc_enabled_with_flag

                if not use_legacy:
                    try:
                        request = jobsv1_pb2.UpdateStatusRequest()
                        backend_utils.invoke_skylet_with_retries(
                            lambda: SkyletClient(handle.get_grpc_channel()
                                                ).update_status(request))
                    except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                        logger.debug(f'gRPC failed, falling back to SSH: {e}')
                        use_legacy = True

                if use_legacy:
                    cmd = job_lib.JobLibCodeGen.update_status()
                    returncode, _, stderr = self.run_on_head(
                        handle, cmd, require_outputs=True)
                    subprocess_utils.handle_returncode(
                        returncode, cmd, 'Failed to update job status.', stderr)
        if (prev_cluster_status == status_lib.ClusterStatus.STOPPED and
                runtime_metadata.has_job_queue):
            # Safely set all the previous jobs to FAILED since the cluster
            # is restarted
            # An edge case here due to racing:
            # 1. A job finishes RUNNING, but right before it update itself
            # to SUCCEEDED, the cluster is STOPPED by `sky stop`.
            # 2. On next `sky start`, it gets reset to FAILED.
            use_legacy = not handle.is_grpc_enabled_with_flag

            if not use_legacy:
                try:
                    fail_request = jobsv1_pb2.FailAllInProgressJobsRequest()
                    backend_utils.invoke_skylet_with_retries(
                        lambda: SkyletClient(handle.get_grpc_channel(
                        )).fail_all_in_progress_jobs(fail_request))
                except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                    logger.debug(f'gRPC failed, falling back to SSH: {e}')
                    use_legacy = True

            if use_legacy:
                cmd = job_lib.JobLibCodeGen.fail_all_jobs_in_progress()
                returncode, stdout, stderr = self.run_on_head(
                    handle, cmd, require_outputs=True)
                subprocess_utils.handle_returncode(
                    returncode, cmd,
                    'Failed to set previously in-progress jobs to FAILED',
                    stdout + stderr)

        prev_ports = None
        if prev_handle is not None:
            prev_ports = prev_handle.launched_resources.ports
        current_ports = handle.launched_resources.ports
        current_ports_set = resources_utils.port_ranges_to_set(current_ports)
        ports_to_reconcile = (current_ports_set -
                              resources_utils.port_ranges_to_set(prev_ports))
        if (prev_cluster_status == status_lib.ClusterStatus.INIT and
                current_ports_set):
            launched_resources = (handle.launched_resources.assert_launchable())
            # An INIT handle records desired ports before provider
            # reconciliation completes. Replay the full desired set only when
            # the provider guarantees that doing so is safe.
            if (launched_resources.cloud.OPEN_PORTS_VERSION ==
                    clouds.OpenPortsVersion.RECONCILABLE):
                ports_to_reconcile = current_ports_set
        if ports_to_reconcile:
            launched_resources = handle.launched_resources.assert_launchable()
            if not (launched_resources.cloud.OPEN_PORTS_VERSION
                    <= clouds.OpenPortsVersion.LAUNCH_ONLY):
                with rich_utils.safe_status(
                        ux_utils.spinner_message(
                            'Launching - Opening new ports')):
                    self._open_ports(handle)

        # Capture task YAML and command
        user_specified_task_config = None
        if task is not None:
            user_specified_task_config = task.to_yaml_config(
                use_user_specified_yaml=True)

        # The INIT transaction pinned the pull plan before it was rendered
        # into the runtime. The READY transaction preserves that exact plan;
        # a newer distribution revision applies on a later launch or restart.
        with timeline.Event('backend.provision.post_process'):
            global_user_state.add_or_update_cluster(
                handle.cluster_name,
                handle,
                set(task.resources),
                ready=True,
                config_hash=config_hash,
                task_config=user_specified_task_config,
                existing_cluster_hash=cluster_hash,
            )

            # Add cluster event for successful provisioning.
            global_user_state.add_cluster_event(
                handle.cluster_name,
                status_lib.ClusterStatus.UP,
                'Cluster successfully provisioned with ' +
                f'{handle.launched_nodes} nodes',
                global_user_state.ClusterEventType.STATUS_CHANGE,
                existing_cluster_hash=cluster_hash)

            usage_lib.messages.usage.update_final_cluster_status(
                status_lib.ClusterStatus.UP)
            # We still add the cluster to ssh config file on API server, this
            # is helpful for people trying to use `sky launch`'ed cluster for
            # ssh proxy jump. Skip if the cluster is not SSH-reachable in the
            # way the SSH config entry would assume.
            if runtime_metadata.ssh_available:
                auth_config = backend_utils.ssh_credential_from_yaml(
                    handle.cluster_yaml,
                    ssh_user=handle.ssh_user,
                    docker_user=handle.docker_user)
                cluster_utils.SSHConfigHelper.add_cluster(
                    handle.cluster_name, handle.cluster_name_on_cloud,
                    handle.cached_external_ips, auth_config,
                    handle.cached_external_ssh_ports, handle.docker_user,
                    handle.ssh_user)

    def _sync_workdir(self, handle: CloudVmRayResourceHandle,
                      workdir: Path | dict[str, Any],
                      envs_and_secrets: dict[str, str]) -> None:
        cloud_vm_ray_file_sync.sync_workdir(handle, workdir, envs_and_secrets,
                                            self.log_dir)

    def _sync_file_mounts(
        self,
        handle: CloudVmRayResourceHandle,
        all_file_mounts: dict[Path, Path] | None,
        storage_mounts: dict[Path, storage_lib.Storage] | None,
    ) -> None:
        """Mounts all user files to the remote nodes.

        Note: This does not handle COPY storage_mounts. These should have
        already been translated into file_mounts by task.sync_storage_mounts().

        TODO: Delete COPY storage_mounts in task.sync_storage_mounts(), and
        assert here that all storage_mounts are MOUNT mode.
        """
        if handle.provision_runtime_metadata.file_mounts_synced:
            logger.info('Skipping file mounts sync: provisioner reported '
                        'ready.')
            return

        launched_resources = handle.launched_resources.assert_launchable()
        with rich_utils.safe_status(ux_utils.spinner_message('Syncing files')):
            controller_utils.replace_skypilot_config_path_in_file_mounts(
                launched_resources.cloud, all_file_mounts)
            cloud_vm_ray_file_sync.execute_file_mounts(handle, all_file_mounts,
                                                       self.log_dir)
            self._execute_storage_mounts(handle, storage_mounts)
            self._set_storage_mounts_metadata(handle.cluster_name,
                                              storage_mounts)

    def _get_num_gpus(self, task: task_lib.Task) -> int:
        if task.resources is not None:
            for resource in task.resources:
                if (resource.accelerators is not None and
                        isinstance(resource.accelerators, dict)):
                    if len(resource.accelerators) > 0:
                        return math.ceil(
                            list(resource.accelerators.values())[0])
        return 0

    def _setup(self, handle: CloudVmRayResourceHandle, task: task_lib.Task,
               detach_setup: bool) -> None:

        start = time.time()

        if handle.provision_runtime_metadata.setup_done:
            logger.info('Skipping setup: provisioner reported done.')
            return

        if task.setup is None:
            return
        setup = task.setup
        # Sync the setup script up and run it.
        internal_ips = handle.internal_ips()
        remote_setup_file_name = f'/tmp/sky_setup_{self.run_timestamp}'
        # Need this `-i` option to make sure `source ~/.bashrc` work
        setup_cmd = f'/bin/bash -i {remote_setup_file_name} 2>&1'
        unset_ray_env_vars = ' && '.join(
            [f'unset {var}' for var in task_codegen.UNSET_RAY_ENV_VARS])
        setup_cmd = f'{unset_ray_env_vars}; {setup_cmd}'
        runners = handle.get_command_runners(avoid_ssh_control=True)

        def _setup_node(node_id: int) -> None:
            setup_envs = task_lib.get_plaintext_envs_and_secrets(
                task.envs_and_secrets)
            setup_envs.update(self._skypilot_predefined_env_vars(handle))
            setup_envs['SKYPILOT_SETUP_NODE_IPS'] = '\n'.join(internal_ips)
            setup_envs['SKYPILOT_SETUP_NODE_RANK'] = str(node_id)
            setup_envs[constants.SKYPILOT_SETUP_NUM_GPUS_PER_NODE] = (str(
                self._get_num_gpus(task)))

            runner = runners[node_id]
            setup_script = log_lib.make_task_bash_script(setup,
                                                         env_vars=setup_envs)
            encoded_script = shlex.quote(setup_script)

            def _dump_final_script(
                    setup_script: str,
                    target_dir: str = remote_setup_file_name) -> None:
                with tempfile.NamedTemporaryFile('w', prefix='sky_setup_') as f:
                    f.write(setup_script)
                    f.flush()
                    setup_sh_path = f.name
                    runner.rsync(source=setup_sh_path,
                                 target=target_dir,
                                 up=True,
                                 stream_logs=False)

            # Always dump the full setup script to the persistent path first
            # In high availability mode, we need to dump the full setup script
            # to a persistent path BEFORE any other operations. This ensures
            # that if the pod restarts, it can find and execute the complete
            # setup script, rather than a reference to a temporary file that
            # would no longer exist after restart.
            if self._dump_final_script:
                _dump_final_script(setup_script,
                                   constants.PERSISTENT_SETUP_SCRIPT_PATH)

            if (detach_setup or
                    backend_utils.is_command_length_over_limit(encoded_script)):
                _dump_final_script(setup_script)
                create_script_code = 'true'
            else:
                create_script_code = (f'{{ echo {encoded_script} > '
                                      f'{remote_setup_file_name}; }}')

            if detach_setup:
                return

            setup_log_path = os.path.join(self.log_dir,
                                          f'setup-{runner.node_id}.log')

            def _run_setup(setup_cmd: str) -> int:
                returncode = runner.run(
                    setup_cmd,
                    log_path=setup_log_path,
                    process_stream=False,
                    # We do not source bashrc for setup, since bashrc is sourced
                    # in the script already.
                    # Skip an empty line and two lines due to the /bin/bash -i
                    # and source ~/.bashrc in the setup_cmd.
                    #   bash: cannot set terminal process group (7398): Inappropriate ioctl for device # pylint: disable=line-too-long
                    #   bash: no job control in this shell
                    skip_num_lines=3)
                return returncode

            returncode = _run_setup(f'{create_script_code} && {setup_cmd}',)

            if _is_message_too_long(returncode, file_path=setup_log_path):
                # If the setup script is too long, we need to retry it
                # with dumping the script to a file and running it the script
                # on remote cluster instead.
                logger.debug('Failed to run setup command inline due to '
                             'command length limit. Dumping setup script to '
                             'file and running it with SSH.')
                _dump_final_script(setup_script)
                returncode = _run_setup(setup_cmd)

            def error_message() -> str:
                # Use the function to avoid tailing the file in success case
                try:
                    last_10_lines = subprocess.run(
                        ['tail', '-n10',
                         os.path.expanduser(setup_log_path)],
                        stdout=subprocess.PIPE,
                        check=True).stdout.decode('utf-8')
                except subprocess.CalledProcessError:
                    last_10_lines = None

                err_msg = (f'Failed to setup with return code {returncode}. '
                           f'Check the details in log: {setup_log_path}')
                if last_10_lines:
                    err_msg += (f'\n\n{colorama.Fore.RED}'
                                '****** START Last lines of setup output ******'
                                f'{colorama.Style.RESET_ALL}\n'
                                f'{last_10_lines}'
                                f'{colorama.Fore.RED}'
                                '******* END Last lines of setup output *******'
                                f'{colorama.Style.RESET_ALL}')
                return err_msg

            subprocess_utils.handle_returncode(returncode=returncode,
                                               command=setup_cmd,
                                               error_msg=error_message)

        num_nodes = len(runners)
        plural = 's' if num_nodes > 1 else ''
        node_str = f'{num_nodes} VM{plural}'
        if isinstance(handle.launched_resources.cloud, clouds.Kubernetes):
            node_str = f'{num_nodes} pod{plural}'
        controller = controller_utils.Controllers.from_name(handle.cluster_name)
        if controller is not None:
            node_str = controller.value.name
        if not detach_setup:
            logger.info(
                ux_utils.starting_message(f'Running setup on {node_str}.'))
        # TODO(zhwu): run_in_parallel uses multi-thread to run the commands,
        # which can cause the program waiting for all the threads to finish,
        # even if some of them raise exceptions. We should replace it with
        # multi-process.
        rich_utils.stop_safe_status()
        subprocess_utils.run_in_parallel(_setup_node, list(range(num_nodes)))

        if detach_setup:
            # Only set this when setup needs to be run outside the self._setup()
            # as part of a job (detach_setup, default).
            self._setup_cmd = setup_cmd
            logger.info(ux_utils.finishing_message('Setup detached.'))
            return
        end = time.time()
        logger.debug(f'Setup took {end - start} seconds.')
        setup_log_path = os.path.join(self.log_dir, 'setup-*.log')
        logger.info(
            ux_utils.finishing_message('Setup completed.', setup_log_path))

    def _download_file(self, handle: CloudVmRayResourceHandle,
                       local_file_path: str, remote_file_path: str) -> None:
        """Syncs file from remote to local."""
        cloud_vm_ray_file_sync.download_file(handle, local_file_path,
                                             remote_file_path)

    def _exec_code_on_head(
        self,
        handle: CloudVmRayResourceHandle,
        codegen: str,
        job_id: int,
        managed_job_dag: Optional['dag.Dag'] = None,
        managed_job_user_id: str | None = None,
        remote_log_dir: str | None = None,
    ) -> None:
        """Executes generated code on the head node."""
        use_legacy = not handle.is_grpc_enabled_with_flag
        file_name = f'sky_job_{job_id}'
        script_path = os.path.join(SKY_REMOTE_APP_DIR, file_name)
        if remote_log_dir is None:
            remote_log_dir = self.log_dir
        remote_log_path = os.path.join(remote_log_dir, 'run.log')

        def _dump_code_to_file(codegen: str,
                               target_dir: str = SKY_REMOTE_APP_DIR) -> None:
            runners = handle.get_command_runners()
            head_runner = runners[0]
            with tempfile.NamedTemporaryFile('w', prefix='sky_app_') as fp:
                fp.write(codegen)
                fp.flush()
                script_path = os.path.join(target_dir, file_name)
                # We choose to sync code + exec, because the alternative of
                # 'ray submit' may not work as it may use system python
                # (python2) to execute the script. Happens for AWS.
                head_runner.rsync_driver(source=fp.name,
                                         target=script_path,
                                         up=True,
                                         stream_logs=False)

        mkdir_code = f'mkdir -p {remote_log_dir} && touch {remote_log_path}'
        encoded_script = shlex.quote(codegen)
        create_script_code = f'{{ echo {encoded_script} > {script_path}; }}'
        job_submit_cmd = (
            # JOB_CMD_IDENTIFIER is used for identifying the process
            # retrieved with pid is the same driver process.
            f'{job_lib.JOB_CMD_IDENTIFIER.format(job_id)} && '
            f'{constants.SKY_PYTHON_CMD} -u {script_path}'
            # Do not use &>, which is not POSIX and may not work.
            # Note that the order of ">filename 2>&1" matters.
            f'> {remote_log_path} 2>&1')
        code = job_lib.JobLibCodeGen.queue_job(job_id, job_submit_cmd)

        # For Slurm, we need to wait for the job to complete before exiting,
        # because Slurm's proctrack/cgroup kills all processes when the srun
        # job step ends, including child processes launched as a separate
        # process group.
        # So this keeps srun alive so the job driver process that was spawned
        # (and runs in the background) by job_lib.JobScheduler.schedule_step()
        # does not get killed.
        # Note: proctrack/cgroup is enabled by default on Nebius' Managed
        # Soperator.
        is_slurm = isinstance(handle.launched_resources.cloud, clouds.Slurm)
        if is_slurm:
            wait_code = job_lib.JobLibCodeGen.wait_for_job(job_id)
            code = code + ' && ' + wait_code

        job_submit_cmd = ' && '.join([mkdir_code, create_script_code, code])

        # Should also be ealier than is_command_length_over_limit
        # Same reason as in _setup
        if self._dump_final_script:
            _dump_code_to_file(job_submit_cmd,
                               constants.PERSISTENT_RUN_SCRIPT_DIR)

        if not use_legacy:
            try:
                managed_job_info: jobsv1_pb2.ManagedJobInfo | None = None
                if managed_job_dag is not None:
                    # `ManagedJobInfo.workspace` is currently not read by
                    # skylet (see `services.py::QueueJob`). Kept on the
                    # wire for future consumers.
                    workspace = skypilot_config.get_active_workspace(
                        force_user_workspace=True)
                    entrypoint = common_utils.get_current_command()

                    managed_job_tasks: list[jobsv1_pb2.ManagedJobTask] = []
                    for task_id, task in enumerate(managed_job_dag.tasks):
                        resources_str = backend_utils.get_task_resources_str(
                            task, is_managed_job=True)
                        managed_job_task = jobsv1_pb2.ManagedJobTask(
                            task_id=task_id,
                            name=task.name,
                            resources_str=resources_str,
                            metadata_json=task.metadata_json)
                        # Only set is_primary_in_job_group for job groups
                        if managed_job_dag.is_job_group():
                            # If primary_task_names is None, all tasks are
                            # primary
                            managed_job_task.is_primary_in_job_group = (
                                managed_job_dag.primary_tasks is None or
                                task.name in managed_job_dag.primary_tasks)
                        managed_job_tasks.append(managed_job_task)

                    # Execution mode: 'parallel' for job groups, 'serial' for
                    # pipelines and single jobs
                    execution = (managed_job_dag.execution.value
                                 if managed_job_dag.execution else
                                 DEFAULT_EXECUTION.value)
                    managed_job_info = jobsv1_pb2.ManagedJobInfo(
                        name=managed_job_dag.name,
                        pool=managed_job_dag.pool,
                        workspace=workspace,
                        entrypoint=entrypoint,
                        tasks=managed_job_tasks,
                        user_id=managed_job_user_id,
                        execution=execution)

                if backend_utils.is_command_length_over_limit(codegen):
                    _dump_code_to_file(codegen)
                    queue_job_request = jobsv1_pb2.QueueJobRequest(
                        job_id=job_id,
                        # codegen not set - server assumes script uploaded
                        remote_log_dir=remote_log_dir,
                        managed_job=managed_job_info,
                        script_path=script_path)
                else:
                    queue_job_request = jobsv1_pb2.QueueJobRequest(
                        job_id=job_id,
                        codegen=codegen,
                        remote_log_dir=remote_log_dir,
                        managed_job=managed_job_info,
                        script_path=script_path)

                backend_utils.invoke_skylet_with_retries(lambda: SkyletClient(
                    handle.get_grpc_channel()).queue_job(queue_job_request))
            except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                logger.debug(f'gRPC failed, falling back to SSH: {e}')
                use_legacy = True

        if use_legacy:
            if backend_utils.is_command_length_over_limit(job_submit_cmd):
                _dump_code_to_file(codegen)
                job_submit_cmd = f'{mkdir_code} && {code}'

            # For Slurm, run in background so that SSH returns immediately.
            # This is needed because we add the wait_for_job code above which
            # makes the command block until the job completes.
            returncode, stdout, stderr = self.run_on_head(
                handle,
                job_submit_cmd,
                stream_logs=False,
                require_outputs=True,
                run_in_background=is_slurm)
            # Happens when someone calls `sky exec` but remote is outdated for
            # running a job. Necessitating calling `sky launch`.
            backend_utils.check_stale_runtime_on_remote(returncode, stderr,
                                                        handle.cluster_name)
            output = stdout + stderr
            if _is_message_too_long(returncode, output=output):
                # If the job submit script is too long, we need to retry it
                # with dumping the script to a file and running it the script
                # on remote cluster instead.
                logger.debug(
                    'Failed to submit job due to command length limit. '
                    'Dumping job to file and running it with SSH. '
                    f'Output: {output}')
                _dump_code_to_file(codegen)
                job_submit_cmd = f'{mkdir_code} && {code}'
                # See comment above for why run_in_background=is_slurm.
                returncode, stdout, stderr = self.run_on_head(
                    handle,
                    job_submit_cmd,
                    stream_logs=False,
                    require_outputs=True,
                    run_in_background=is_slurm)

            subprocess_utils.handle_returncode(
                returncode,
                job_submit_cmd,
                f'Failed to submit job {job_id}.',
                stderr=stdout + stderr)

        controller = controller_utils.Controllers.from_name(handle.cluster_name)
        if controller == controller_utils.Controllers.SKY_SERVE_CONTROLLER:
            logger.info(ux_utils.starting_message('Service registered.'))
        else:
            logger.info(
                ux_utils.starting_message(f'Job submitted, ID: {job_id}'))
        rich_utils.stop_safe_status()

    def _run_job_id_command_with_ssm_retries(self,
                                             handle: CloudVmRayResourceHandle,
                                             code: str) -> tuple[int, str, str]:
        """Runs a job-ID command, retrying pre-session AWS SSM failures."""
        backoff = common_utils.Backoff(
            initial_backoff=_JOB_ID_SSM_RECONNECT_INITIAL_BACKOFF_SECONDS,
            max_backoff_factor=(_JOB_ID_SSM_RECONNECT_MAX_BACKOFF_SECONDS //
                                _JOB_ID_SSM_RECONNECT_INITIAL_BACKOFF_SECONDS),
            multiplier=2)
        for attempt in range(1, _JOB_ID_SSM_RECONNECT_MAX_ATTEMPTS + 1):
            returncode, result_str, stderr = self.run_on_head(
                handle,
                code,
                stream_logs=False,
                require_outputs=True,
                separate_stderr=True)
            target_not_connected = (returncode == 255 and
                                    'TargetNotConnected' in stderr)
            if not target_not_connected:
                return returncode, result_str, stderr
            if attempt == _JOB_ID_SSM_RECONNECT_MAX_ATTEMPTS:
                return returncode, result_str, stderr
            # TargetNotConnected is emitted by SSM StartSession before SSH
            # establishes a session, so the remote job mutation did not run.
            # Do not broaden this retry to ambiguous mid-command disconnects.
            sleep_seconds = backoff.current_backoff()
            logger.warning(
                'AWS SSM target is not connected while fetching a job ID; '
                f'retrying in {sleep_seconds:.1f} seconds '
                f'(attempt {attempt + 1}/'
                f'{_JOB_ID_SSM_RECONNECT_MAX_ATTEMPTS}).')
            time.sleep(sleep_seconds)
        raise AssertionError('SSM reconnect attempts must be positive.')

    def _add_job(self, handle: CloudVmRayResourceHandle, job_name: str | None,
                 resources_str: str, metadata: str) -> tuple[int, str]:
        if handle.is_grpc_enabled_with_flag:
            try:
                request = jobsv1_pb2.AddJobRequest(
                    job_name=job_name,
                    username=common_utils.get_user_hash(),
                    run_timestamp=self.run_timestamp,
                    resources_str=resources_str,
                    metadata=metadata)
                response = backend_utils.invoke_skylet_with_retries(
                    lambda: SkyletClient(handle.get_grpc_channel()).add_job(
                        request))
                job_id = response.job_id
                log_dir = response.log_dir
                return job_id, log_dir
            except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                logger.debug(f'gRPC failed, falling back to SSH: {e}')

        code = job_lib.JobLibCodeGen.add_job(
            job_name=job_name,
            username=common_utils.get_user_hash(),
            run_timestamp=self.run_timestamp,
            resources_str=resources_str,
            metadata=metadata)
        returncode, result_str, stderr = (
            self._run_job_id_command_with_ssm_retries(handle, code))
        # Happens when someone calls `sky exec` but remote is outdated for
        # adding a job. Necessitating calling `sky launch`.
        backend_utils.check_stale_runtime_on_remote(returncode, stderr,
                                                    handle.cluster_name)
        subprocess_utils.handle_returncode(returncode, code,
                                           'Failed to fetch job id.', stderr)
        try:
            job_id_match = _JOB_ID_PATTERN.search(result_str)
            if job_id_match is not None:
                job_id = int(job_id_match.group(1))
            else:
                # For backward compatibility.
                job_id = int(result_str)
            log_dir_match = _LOG_DIR_PATTERN.search(result_str)
            if log_dir_match is not None:
                log_dir = log_dir_match.group(1).strip()
            else:
                # For backward compatibility, use the same log dir as local.
                log_dir = self.log_dir
        except ValueError as e:
            logger.error(stderr)
            raise ValueError(f'Failed to parse job id: {result_str}; '
                             f'Returncode: {returncode}') from e
        return job_id, log_dir

    def set_job_info_without_job_id(
        self,
        handle: CloudVmRayResourceHandle,
        name: str,
        workspace: str,
        entrypoint: str,
        pool: str | None,
        pool_hash: str | None,
        user_hash: str | None,
        task_ids: list[int],
        task_names: list[str],
        resources_str: str,
        metadata_jsons: list[str],
        is_primary_in_job_groups: list[bool | None],
        num_jobs: int = 1,
        execution: str = DEFAULT_EXECUTION.value,
        is_batch: bool = False,
    ) -> list[int]:
        """Set job info without creating entries in the jobs table.

        This creates entries in job_info_table and spot_table without creating
        entries in the jobs table, which prevents autostop from being blocked
        by jobs stuck in INIT status.
        """
        use_legacy = not handle.is_grpc_enabled_with_flag

        if not use_legacy:
            try:
                request = jobsv1_pb2.SetJobInfoWithoutJobIdRequest(
                    name=name,
                    workspace=workspace,
                    entrypoint=entrypoint,
                    pool=pool,
                    pool_hash=pool_hash,
                    user_hash=user_hash,
                    task_ids=task_ids,
                    task_names=task_names,
                    resources_str=resources_str,
                    metadata_jsons=metadata_jsons,
                    num_jobs=num_jobs,
                    execution=execution,
                    # Field 13 cannot represent None. Keep populating it for
                    # compatibility with older jobs controllers.
                    is_primary_in_job_groups=[
                        value if value is not None else False
                        for value in is_primary_in_job_groups
                    ],
                    is_primary_in_job_groups_v2=[
                        jobsv1_pb2.OptionalBool(value=value)
                        if value is not None else jobsv1_pb2.OptionalBool()
                        for value in is_primary_in_job_groups
                    ])
                response = backend_utils.invoke_skylet_with_retries(
                    lambda: SkyletClient(handle.get_grpc_channel()
                                        ).set_job_info_without_job_id(request))
                return list(response.job_ids)
            except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                logger.debug(f'gRPC failed, falling back to SSH: {e}')
                use_legacy = True

        if use_legacy:
            code = job_lib.JobLibCodeGen.set_job_info_without_job_id(
                name=name,
                workspace=workspace,
                entrypoint=entrypoint,
                pool=pool,
                pool_hash=pool_hash,
                user_hash=user_hash,
                task_ids=task_ids,
                task_names=task_names,
                resources_str=resources_str,
                metadata_jsons=metadata_jsons,
                is_primary_in_job_groups=is_primary_in_job_groups,
                num_jobs=num_jobs,
                execution=execution,
                is_batch=is_batch)
            returncode, result_str, stderr = (
                self._run_job_id_command_with_ssm_retries(handle, code))
            backend_utils.check_stale_runtime_on_remote(returncode, stderr,
                                                        handle.cluster_name)
            subprocess_utils.handle_returncode(returncode, code,
                                               'Failed to fetch job id.',
                                               stderr)
            try:
                # Parse job IDs from output
                job_ids_match = _JOB_IDS_PATTERN.search(result_str)
                if job_ids_match:
                    job_ids = [
                        int(x.strip())
                        for x in job_ids_match.group(1).split(',')
                    ]
                    return job_ids
                else:
                    raise ValueError(
                        f'Failed to parse job ids from: {result_str}')
            except ValueError as e:
                logger.error(stderr)
                raise ValueError(f'Failed to parse job id: {result_str}; '
                                 f'Returncode: {returncode}') from e
        return []

    def _execute(
        self,
        handle: CloudVmRayResourceHandle,
        task: task_lib.Task,
        dryrun: bool = False,
    ) -> int | None:
        """Executes the task on the cluster.

        Returns:
            Job id if the task is submitted to the cluster, None otherwise.
        """
        if handle.provision_runtime_metadata.run_started:
            logger.info('Skipping run: provisioner reported run already '
                        'started.')
            return None

        if task.run is None and self._setup_cmd is None:
            # This message is fine without mentioning setup, as there are two
            # cases when run section is empty:
            # 1. setup specified: setup is executed in detached mode and this
            #    message will not be shown.
            # 2. no setup specified: this message is fine as a user is likely
            #    creating a cluster only, and ok with the empty run command.
            logger.info('Run commands not specified or empty.')
            return None
        if task.run is None:
            # If the task has no run command, we still need to execute the
            # generated ray driver program to run the setup command in detached
            # mode.
            # In this case, we reset the resources for the task, so that the
            # detached setup does not need to wait for the task resources to be
            # ready (which is not used for setup anyway).
            valid_resource = resources_lib.Resources()
        else:
            # Check the task resources vs the cluster resources. Since
            # `sky exec` will not run the provision and _check_existing_cluster
            # We need to check ports here since sky.exec shouldn't change
            # resources.
            valid_resource = self.check_resources_fit_cluster(handle,
                                                              task,
                                                              check_ports=True)
        task_copy = copy.copy(task)
        # Handle multiple resources exec case.
        task_copy.set_resources(valid_resource)
        if len(task.resources) > 1:
            logger.info('Multiple resources are specified '
                        f'for the task, using: {valid_resource}')
        task_copy.best_resources = None
        resources_str = backend_utils.get_task_resources_str(task_copy)

        if dryrun:
            logger.info(f'Dryrun complete. Would have run:\n{task}')
            return None

        job_id, log_dir = self._add_job(handle, task_copy.name, resources_str,
                                        task.metadata_json)

        num_actual_nodes = task.num_nodes * handle.num_ips_per_node
        # Case: task_lib.Task(run, num_nodes=N) or TPU VM Pods
        if num_actual_nodes > 1:
            self._execute_task_n_nodes(handle, task_copy, job_id, log_dir)
        else:
            # Case: task_lib.Task(run, num_nodes=1)
            self._execute_task_one_node(handle, task_copy, job_id, log_dir)

        return job_id

    def _post_execute(self, handle: CloudVmRayResourceHandle,
                      down: bool) -> None:
        """Post-execute cleanup."""
        del handle, down  # Unused.
        # All logic is handled in previous stages, no-op.

    def _teardown_ephemeral_storage(self, task: task_lib.Task) -> None:
        storage_mounts = task.storage_mounts
        if storage_mounts is not None:
            for _, storage in storage_mounts.items():
                if not storage.persistent:
                    storage.delete()

    def _teardown(self,
                  handle: CloudVmRayResourceHandle,
                  terminate: bool,
                  purge: bool = False):
        """Tear down or stop the cluster.

        Args:
            handle: The handle to the cluster.
            terminate: Terminate or stop the cluster.
            purge: Purge the cluster record from the cluster table, even if
                the teardown fails.
        Raises:
            exceptions.ClusterOwnerIdentityMismatchError: If the cluster is
                owned by another user.
            exceptions.CloudUserIdentityError: if we fail to get the current
                user identity.
            RuntimeError: If the cluster fails to be terminated/stopped.
        """
        cluster_name = handle.cluster_name
        # Check if the cluster is owned by the current user. Raise
        # exceptions.ClusterOwnerIdentityMismatchError
        yellow = colorama.Fore.YELLOW
        reset = colorama.Style.RESET_ALL
        is_identity_mismatch_and_purge = False
        try:
            backend_utils.check_owner_identity(cluster_name)
        except (exceptions.ClusterOwnerIdentityMismatchError,
                exceptions.CloudUserIdentityError) as e:
            if purge:
                logger.error(e)
                verbed = 'terminated' if terminate else 'stopped'
                logger.warning(
                    f'{yellow}Purge (-p/--purge) is set, ignoring the '
                    f'identity mismatch error and removing '
                    f'the cluster record from cluster table.{reset}\n{yellow}It'
                    ' is the user\'s responsibility to ensure that this '
                    f'cluster is actually {verbed} on the cloud.{reset}')
                is_identity_mismatch_and_purge = True
            else:
                raise
        lock_id = backend_utils.cluster_status_lock_id(cluster_name)
        lock = locks.get_lock(lock_id, timeout=1)
        resource_lock_id = backend_utils.cluster_resource_operation_lock_id(
            cluster_name)
        resource_lock = locks.get_lock(resource_lock_id, timeout=1)
        # Retry in case new cluster operation comes in and holds the lock
        # right after the lock is removed.
        n_attempts = 2
        while True:
            n_attempts -= 1
            # We have to kill the cluster requests, because `down` and `stop`
            # should be higher priority than the cluster requests, and we should
            # release the lock from other requests.
            exclude_request_to_kill = 'sky.down' if terminate else 'sky.stop'
            try:
                # TODO(zhwu): we should get rid of this when it is being called
                # internally without involving an API server, e.g., when a
                # controller is trying to terminate a cluster.
                requests_lib.kill_cluster_requests(handle.cluster_name,
                                                   exclude_request_to_kill)
            except Exception as e:  # pylint: disable=broad-except
                # We allow the failure to kill other launch requests, because
                # it is not critical to the cluster teardown.
                logger.warning(
                    'Failed to kill other launch requests for the '
                    f'cluster {handle.cluster_name}: '
                    f'{common_utils.format_exception(e, use_bracket=True)}')
            # In case other running cluster operations are still holding the
            # lock.
            lock.force_unlock()
            try:
                with lock:
                    with resource_lock:
                        self.teardown_no_lock(
                            handle,
                            terminate,
                            purge,
                            # When --purge is set and we already see an ID
                            # mismatch error, we skip the refresh codepath. This
                            # is because refresh checks current user identity
                            # can throw ClusterOwnerIdentityMismatchError. The
                            # argument/flag `purge` should bypass such ID
                            # mismatch errors.
                            refresh_cluster_status=(
                                not is_identity_mismatch_and_purge))
                if terminate:
                    lock.force_unlock()
                break
            except locks.LockTimeout as e:
                logger.debug(f'Failed to acquire lock for {cluster_name}, '
                             f'retrying...')
                if n_attempts <= 0:
                    raise RuntimeError(
                        f'Cluster {cluster_name!r} is locked by {lock_id} or '
                        f'{resource_lock_id}. Check to see if it is still '
                        'being launched or torn down') from e

    # --- CloudVMRayBackend Specific APIs ---

    def get_job_status(
            self,
            handle: CloudVmRayResourceHandle,
            job_ids: list[int] | None = None,
            stream_logs: bool = True
    ) -> dict[int | None, job_lib.JobStatus | None]:
        if handle.is_grpc_enabled_with_flag:
            try:
                request = jobsv1_pb2.GetJobStatusRequest(job_ids=job_ids)
                response = backend_utils.invoke_skylet_with_retries(
                    lambda: SkyletClient(handle.get_grpc_channel()
                                        ).get_job_status(request))
                statuses: dict[int | None, job_lib.JobStatus | None] = {
                    job_id: job_lib.JobStatus.from_protobuf(proto_status)
                    for job_id, proto_status in response.job_statuses.items()
                }
                return statuses
            except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                logger.debug(f'gRPC failed, falling back to SSH: {e}')

        code = job_lib.JobLibCodeGen.get_job_status(job_ids)
        returncode, stdout, stderr = self.run_on_head(handle,
                                                      code,
                                                      stream_logs=stream_logs,
                                                      require_outputs=True,
                                                      separate_stderr=True)
        subprocess_utils.handle_returncode(returncode, code,
                                           'Failed to get job status.', stderr)
        if not stdout:
            # We see some cases in the wild where a misbehaving cluster/k8s
            # apiserver (not sure which) can have a returncode of 0 but
            # incorrectly empty stdout. Treat this as a failure.
            raise exceptions.CommandFailureException(
                command=code,
                failure='produced no output',
                error_msg='Failed to get job status.',
                detailed_reason=f'stderr="{stderr}"',
            )
        statuses = job_lib.load_statuses_payload(stdout)
        return statuses

    def cancel_jobs(self,
                    handle: CloudVmRayResourceHandle,
                    jobs: list[int] | None,
                    cancel_all: bool = False,
                    user_hash: str | None = None) -> None:
        """Cancels jobs.

        See `skylet.job_lib.cancel_jobs_encoded_results` for more details.
        """
        cancelled_ids = None
        use_legacy = not handle.is_grpc_enabled_with_flag

        if not use_legacy:
            try:
                request = jobsv1_pb2.CancelJobsRequest(job_ids=jobs,
                                                       cancel_all=cancel_all,
                                                       user_hash=user_hash)
                response = backend_utils.invoke_skylet_with_retries(
                    lambda: SkyletClient(handle.get_grpc_channel()).cancel_jobs(
                        request))
                cancelled_ids = response.cancelled_job_ids
            except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                logger.debug(f'gRPC failed, falling back to SSH: {e}')
                use_legacy = True

        if use_legacy:
            code = job_lib.JobLibCodeGen.cancel_jobs(jobs, cancel_all,
                                                     user_hash)
            returncode, stdout, _ = self.run_on_head(handle,
                                                     code,
                                                     stream_logs=False,
                                                     require_outputs=True)
            subprocess_utils.handle_returncode(
                returncode, code,
                f'Failed to cancel jobs on cluster {handle.cluster_name}.',
                stdout)
            cancelled_ids = message_utils.decode_payload(stdout)
        if cancelled_ids is None:
            raise RuntimeError('Job cancellation produced no result.')
        if cancelled_ids:
            logger.info(
                f'Cancelled job ID(s): {", ".join(map(str, cancelled_ids))}')
        else:
            logger.info('No jobs cancelled. They may be in terminal states.')

    def sync_down_logs(
            self,
            handle: CloudVmRayResourceHandle,
            job_ids: list[str] | None,
            local_dir: str = constants.SKY_LOGS_DIRECTORY) -> dict[str, str]:
        """Sync down logs for the given job_ids.

        Returns:
            A dictionary mapping job_id to log path.
        """
        job_to_dir: dict[str, str] = {}
        use_legacy = not handle.is_grpc_enabled_with_flag

        if not use_legacy:
            try:
                int_job_ids = []
                if job_ids:
                    for str_job_id in job_ids:
                        if str_job_id.isdigit():
                            int_job_ids.append(int(str_job_id))
                request = jobsv1_pb2.GetLogDirsForJobsRequest(
                    job_ids=int_job_ids)
                response = backend_utils.invoke_skylet_with_retries(
                    lambda: SkyletClient(handle.get_grpc_channel()
                                        ).get_log_dirs_for_jobs(request))
                job_log_dirs = response.job_log_dirs
                if not job_log_dirs:
                    logger.info(f'{colorama.Fore.YELLOW}'
                                'No matching log directories found'
                                f'{colorama.Style.RESET_ALL}')
                    return {}
                for job_id, log_dir in job_log_dirs.items():
                    # Convert to string for backwards compatibility
                    job_to_dir[str(job_id)] = log_dir
            except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                logger.debug(f'gRPC failed, falling back to SSH: {e}')
                use_legacy = True

        if use_legacy:
            code = job_lib.JobLibCodeGen.get_log_dirs_for_jobs(job_ids)
            returncode, stdout, stderr = self.run_on_head(handle,
                                                          code,
                                                          stream_logs=False,
                                                          require_outputs=True,
                                                          separate_stderr=True)
            subprocess_utils.handle_returncode(returncode, code,
                                               'Failed to sync logs.', stderr)
            job_to_dir = message_utils.decode_payload(stdout)
            if not job_to_dir:
                logger.info(f'{colorama.Fore.YELLOW}'
                            'No matching log directories found'
                            f'{colorama.Style.RESET_ALL}')
                return {}

        job_ids = list(job_to_dir.keys())
        dirs = list(job_to_dir.values())
        remote_log_dirs = [
            # TODO(aylei): backward compatibility for legacy runtime that
            # returns run_timestamp only, remove after 0.12.0
            (dir if constants.SKY_LOGS_DIRECTORY in dir else os.path.join(
                constants.SKY_LOGS_DIRECTORY, dir)) for dir in dirs
        ]
        # Include cluster name in local log directory path to avoid conflicts
        # when the same job_id exists on different clusters
        cluster_name = handle.cluster_name
        local_log_dirs = []
        for remote_log_dir in dirs:
            if constants.SKY_LOGS_DIRECTORY in remote_log_dir:
                # Extract the job-specific directory name from the full path
                # e.g., ~/sky_logs/1-job_name -> 1-job_name
                job_dir = remote_log_dir.replace(constants.SKY_LOGS_DIRECTORY,
                                                 '').lstrip('/')
                local_log_dir = os.path.join(local_dir, cluster_name, job_dir)
            else:
                # remote_log_dir is already just the job directory name (e.g.,
                # "1-job_name")
                local_log_dir = os.path.join(local_dir, cluster_name,
                                             remote_log_dir)
            local_log_dirs.append(local_log_dir)

        runners = handle.get_command_runners()

        def _rsync_down(args) -> None:
            """Rsync down logs from remote nodes.

            Args:
                args: A tuple of (runner, local_log_dir, remote_log_dir)
            """
            (runner, local_log_dir, remote_log_dir) = args
            try:
                os.makedirs(os.path.expanduser(local_log_dir), exist_ok=True)
                runner.rsync_driver(
                    # Require a `/` at the end to make sure the parent dir
                    # are not created locally. We do not add additional '*' as
                    # kubernetes's rsync does not work with an ending '*'.
                    source=f'{remote_log_dir}/',
                    target=os.path.expanduser(local_log_dir),
                    up=False,
                    stream_logs=False,
                )
            except exceptions.CommandError as e:
                if e.returncode == exceptions.RSYNC_FILE_NOT_FOUND_CODE:
                    # Raised by rsync_down. Remote log dir may not exist, since
                    # the job can be run on some part of the nodes.
                    logger.debug(f'{runner.node_id} does not have the tasks/*.')
                else:
                    raise

        parallel_args = [
            [runner, *item]
            # Both lists are derived from the same `dirs` list.
            for item in zip(local_log_dirs, remote_log_dirs)  # noqa: B905
            for runner in runners
        ]
        subprocess_utils.run_in_parallel(_rsync_down, parallel_args)
        # Both lists are derived from the same `job_to_dir` dictionary.
        return dict(zip(job_ids, local_log_dirs))  # noqa: B905

    @context_utils.cancellation_guard
    def tail_logs(self,
                  handle: CloudVmRayResourceHandle,
                  job_id: int | None,
                  managed_job_id: int | None = None,
                  follow: bool = True,
                  tail: int = 0,
                  tail_offset: int | None = None,
                  require_outputs: bool = False,
                  stream_logs: bool = True,
                  process_stream: bool = False) -> int | tuple[int, str, str]:
        """Tail the logs of a job.

        Args:
            handle: The handle to the cluster.
            job_id: The job ID to tail the logs of.
            managed_job_id: The managed job ID for display purpose only.
            follow: Whether to follow the logs.
            tail: The number of lines to display from the end of the
                log file. If 0, print all lines.
            tail_offset: Skip this many lines from EOF before applying
                ``tail``. Used for paginated backfill (e.g. dashboard
                scroll-up). 0 / None means no offset.
            require_outputs: Whether to return the stdout/stderr of the command.
            stream_logs: Whether to stream the logs to stdout/stderr.
            process_stream: Whether to process the stream.

        Returns:
            The exit code of the tail command. Returns code 100 if the job has
            failed. See exceptions.JobExitCode for possible return codes.
        """
        offset = tail_offset if tail_offset is not None else 0
        if handle.is_grpc_enabled_with_flag:
            last_exit_code = 0
            try:
                request = jobsv1_pb2.TailLogsRequest(
                    job_id=job_id,
                    managed_job_id=managed_job_id,
                    follow=follow,
                    tail=tail,
                    tail_offset=offset)
                for resp in backend_utils.invoke_skylet_streaming_with_retries(
                        lambda: SkyletClient(handle.get_grpc_channel()
                                            ).tail_logs(request, timeout=None)):
                    if resp.log_line:
                        print(resp.log_line, end='', flush=True)
                    last_exit_code = resp.exit_code
                return last_exit_code
            except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                logger.debug(f'gRPC failed, falling back to SSH: {e}')
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.CANCELLED:
                    return last_exit_code
                raise e

        code = job_lib.JobLibCodeGen.tail_logs(
            job_id,
            managed_job_id=managed_job_id,
            follow=follow,
            tail=tail,
            tail_offset=offset if offset > 0 else None)
        if job_id is None and managed_job_id is None:
            logger.info(
                'Job ID not provided. Streaming the logs of the latest job.')

        # With the stdin=subprocess.DEVNULL, the ctrl-c will not directly
        # kill the process, so we need to handle it manually here.
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, backend_utils.interrupt_handler)
            signal.signal(signal.SIGTSTP, backend_utils.stop_handler)
        try:
            final = self.run_on_head(
                handle,
                code,
                stream_logs=stream_logs,
                process_stream=process_stream,
                require_outputs=require_outputs,
                # Allocate a pseudo-terminal to disable output buffering.
                # Otherwise, there may be 5 minutes delay in logging.
                ssh_mode=command_runner.SshMode.INTERACTIVE,
            )
        except SystemExit as e:
            final = e.code
        return final

    def tail_hook_logs(self,
                       handle: CloudVmRayResourceHandle,
                       event: str | None = None,
                       follow: bool = True,
                       tail: int = 0) -> int:
        """Tail per-event lifecycle-hook logs.

        Args:
            handle: The handle to the cluster.
            event: One of 'stop', 'preemption', 'down'. When ``None``,
                auto-selects whichever per-event log exists on the head.
            follow: Whether to follow the logs.
            tail: The number of lines to display from the end of the
                log file. If 0, print all lines.

        Returns:
            The exit code of the tail command.
        """
        legacy_log_path = f'~/{constants.AUTOSTOP_HOOK_LOG_FILE}'
        new_log_dir = f'~/{constants.HOOK_LOG_DIR}'
        tail_flags = []
        if tail > 0:
            tail_flags.extend(['-n', str(tail)])
        elif not follow:
            tail_flags.extend(['-n', '+1'])
        if follow:
            tail_flags.append('-f')
        tail_flag_str = ' '.join(tail_flags)

        if event is None:
            # Auto-select: pick whichever per-event log exists. Prefer
            # recency via -t sort (newest first). Fall back to legacy path.
            # TODO(zpoint): drop the legacy_log_path branch after v0.15.0
            # (aligned with the autostop.hook removal pinned at v0.15.0
            # in sky/utils/schemas.py:_AUTOSTOP_SCHEMA). By then, clusters
            # predating the new ~/.sky/hooks/ layout have aged out via
            # re-launches.
            cmd = (f'if ls {new_log_dir}/*.log >/dev/null 2>&1; then '
                   f'  latest=$(ls -t {new_log_dir}/*.log | head -n1); '
                   f'  echo "=== $(basename $latest .log) ==="; '
                   f'  tail {tail_flag_str} "$latest"; '
                   f'elif [ -f {legacy_log_path} ]; then '
                   f'  tail {tail_flag_str} {legacy_log_path}; '
                   f'else '
                   f'  echo "No hook has fired yet on this cluster."; exit 1; '
                   f'fi')
        else:
            log_path = f'{new_log_dir}/{event}.log'
            if event == 'stop':
                # Legacy-path fallback for clusters predating the hooks
                # framework — master's single autostop_hook.log corresponds
                # to the new ``stop`` event (idle-timer teardown without
                # autodown). Autodown clusters had their hook log under
                # the same legacy path; users who want it via ``--hook down``
                # should re-launch (the legacy cluster's skylet won't write
                # to ~/.sky/hooks/down.log anyway).
                # TODO(zpoint): drop the legacy_log_path branch after
                # v0.15.0 (aligned with the autostop.hook removal pinned
                # at v0.15.0 in sky/utils/schemas.py:_AUTOSTOP_SCHEMA).
                cmd = (
                    f'if [ -f {log_path} ]; then tail {tail_flag_str} '
                    f'{log_path}; '
                    f'elif [ -f {legacy_log_path} ]; then tail {tail_flag_str} '
                    f'{legacy_log_path}; '
                    f'else echo "No {event} hook log found."; exit 1; fi')
            else:
                cmd = (f'if [ -f {log_path} ]; then tail {tail_flag_str} '
                       f'{log_path}; '
                       f'else echo "No {event} hook log found."; exit 1; fi')

        # With the stdin=subprocess.DEVNULL, the ctrl-c will not directly
        # kill the process, so we need to handle it manually here.
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, backend_utils.interrupt_handler)
            signal.signal(signal.SIGTSTP, backend_utils.stop_handler)
        try:
            returncode = self.run_on_head(
                handle,
                cmd,
                stream_logs=True,
                # Allocate a pseudo-terminal to disable output buffering.
                ssh_mode=command_runner.SshMode.INTERACTIVE,
            )
        except SystemExit as e:
            returncode = e.code
        return returncode

    def tail_managed_job_logs(self,
                              handle: CloudVmRayResourceHandle,
                              job_id: int | None = None,
                              job_name: str | None = None,
                              controller: bool = False,
                              follow: bool = True,
                              tail: int | None = None,
                              tail_offset: int | None = None,
                              task: str | int | None = None) -> int:
        # if job_name is not None, job_id should be None
        assert job_name is None or job_id is None, (job_name, job_id)
        # TODO(kevin): Migrate stream_logs to gRPC
        code = managed_jobs.ManagedJobCodeGen.stream_logs(
            job_name, job_id, follow, controller, tail, tail_offset, task)

        # With the stdin=subprocess.DEVNULL, the ctrl-c will not directly
        # kill the process, so we need to handle it manually here.
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, backend_utils.interrupt_handler)
            signal.signal(signal.SIGTSTP, backend_utils.stop_handler)

        # Refer to the notes in tail_logs.
        try:
            returncode = self.run_on_head(
                handle,
                code,
                stream_logs=True,
                process_stream=False,
                ssh_mode=command_runner.SshMode.INTERACTIVE,
            )
        except SystemExit as e:
            returncode = e.code
        return returncode

    def sync_down_managed_job_logs(
            self,
            handle: CloudVmRayResourceHandle,
            job_id: int | None = None,
            job_name: str | None = None,
            controller: bool = False,
            local_dir: str = constants.SKY_LOGS_DIRECTORY) -> dict[str, str]:
        """Sync down logs for a managed job.

        Args:
            handle: The handle to the cluster.
            job_id: The job ID to sync down logs for.
            job_name: The job name to sync down logs for.
            controller: Whether to sync down logs for the controller.
            local_dir: The local directory to sync down logs to.

        Returns:
            A dictionary mapping job_id to log path.
        """
        # if job_name and job_id should not both be specified
        assert job_name is None or job_id is None, (job_name, job_id)

        if job_id is None:
            # get the job_id
            # if job_name is None, get all job_ids
            # TODO: Only get the latest job_id, since that's the only one we use

            job_ids = None
            use_legacy = not handle.is_grpc_enabled_with_flag
            logger.info(f'handle.is_grpc_enabled_with_flag: '
                        f'{handle.is_grpc_enabled_with_flag}')
            if not use_legacy:
                try:
                    request = managed_jobsv1_pb2.GetAllJobIdsByNameRequest(
                        job_name=job_name)
                    response = backend_utils.invoke_skylet_with_retries(
                        lambda: SkyletClient(handle.get_grpc_channel(
                        )).get_all_managed_job_ids_by_name(request))
                    job_ids = list(response.job_ids)
                except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                    logger.debug(f'gRPC failed, falling back to SSH: {e}')
                    use_legacy = True

            if use_legacy:
                code = managed_jobs.ManagedJobCodeGen.get_all_job_ids_by_name(
                    job_name=job_name)
                returncode, job_ids_payload, stderr = self.run_on_head(
                    handle,
                    code,
                    stream_logs=False,
                    require_outputs=True,
                    separate_stderr=True)
                subprocess_utils.handle_returncode(returncode, code,
                                                   'Failed to sync down logs.',
                                                   stderr)
                job_ids = message_utils.decode_payload(job_ids_payload)
            if job_ids is None:
                raise RuntimeError('Managed job lookup produced no result.')
            if not job_ids:
                logger.info(f'{colorama.Fore.YELLOW}'
                            'No matching job found'
                            f'{colorama.Style.RESET_ALL}')
                return {}
            elif len(job_ids) > 1:
                name_str = ''
                if job_name is not None:
                    name_str = ('Multiple jobs IDs found under the name '
                                f'{job_name}. ')
                controller_str = ' (controller)' if controller else ''
                logger.info(f'{colorama.Fore.YELLOW}'
                            f'{name_str}'
                            f'Downloading the latest job logs{controller_str}.'
                            f'{colorama.Style.RESET_ALL}')
            # list should aready be in descending order
            job_id = job_ids[0]

        if isinstance(handle, LocalResourcesHandle):
            # In consolidation mode, we don't submit a ray job, therefore no
            # run_timestamp is available. We use a dummy run_timestamp here.
            run_timestamps = {
                job_id: f'managed-jobs-consolidation-mode-{job_id}'
            }
        else:
            # get the run_timestamp
            # the function takes in [job_id]
            run_timestamps = None
            use_legacy = not handle.is_grpc_enabled_with_flag
            if not use_legacy:
                try:
                    log_dirs_request = jobsv1_pb2.GetLogDirsForJobsRequest(
                        job_ids=[job_id])
                    log_dirs_response = (
                        backend_utils.invoke_skylet_with_retries(
                            lambda: SkyletClient(handle.get_grpc_channel(
                            )).get_log_dirs_for_jobs(log_dirs_request)))
                    job_log_dirs = log_dirs_response.job_log_dirs
                    # Convert back to the expected format
                    # {job_id: run_timestamp}
                    run_timestamps = {}
                    for jid, log_dir in job_log_dirs.items():
                        run_timestamps[int(jid)] = log_dir
                except exceptions.SKYLET_GRPC_FALLBACK_ERRORS as e:
                    logger.debug(f'gRPC failed, falling back to SSH: {e}')
                    use_legacy = True

            if use_legacy:
                code = job_lib.JobLibCodeGen.get_log_dirs_for_jobs(
                    [str(job_id)])
                returncode, run_timestamps_payload, stderr = self.run_on_head(
                    handle,
                    code,
                    stream_logs=False,
                    require_outputs=True,
                    separate_stderr=True)
                subprocess_utils.handle_returncode(returncode, code,
                                                   'Failed to sync logs.',
                                                   stderr)
                # returns with a dict of {job_id: run_timestamp}
                run_timestamps = message_utils.decode_payload(
                    run_timestamps_payload)
            if run_timestamps is None:
                raise RuntimeError('Managed job log lookup produced no result.')
        if not run_timestamps:
            logger.info(f'{colorama.Fore.YELLOW}'
                        'No matching log directories found'
                        f'{colorama.Style.RESET_ALL}')
            return {}

        run_timestamp = list(run_timestamps.values())[0]
        job_id = list(run_timestamps.keys())[0]

        # If run_timestamp contains the full path with SKY_LOGS_DIRECTORY,
        # strip the prefix to get just the relative part to avoid duplication
        # when constructing local paths.
        if run_timestamp.startswith(constants.SKY_LOGS_DIRECTORY):
            run_timestamp = run_timestamp[len(constants.SKY_LOGS_DIRECTORY
                                             ):].lstrip('/')
        local_log_dir = ''
        if controller:  # download controller logs
            remote_log = os.path.join(managed_jobs.JOBS_CONTROLLER_LOGS_DIR,
                                      f'{job_id}.log')
            local_log_dir = os.path.join(local_dir, 'managed_jobs',
                                         run_timestamp)
            os.makedirs(os.path.dirname(os.path.expanduser(local_log_dir)),
                        exist_ok=True)

            logger.debug(f'{colorama.Fore.CYAN}'
                         f'Job {job_id} local logs: {local_log_dir}'
                         f'{colorama.Style.RESET_ALL}')

            runners = handle.get_command_runners()

            def _rsync_down(args) -> None:
                """Rsync down logs from remote nodes.

                Args:
                    args: A tuple of (runner, local_log_dir, remote_log_dir)
                """
                (runner, local_log_dir, remote_log) = args
                try:
                    os.makedirs(os.path.expanduser(local_log_dir),
                                exist_ok=True)
                    runner.rsync(
                        source=remote_log,
                        target=f'{local_log_dir}/controller.log',
                        up=False,
                        stream_logs=False,
                    )
                except exceptions.CommandError as e:
                    if e.returncode == exceptions.RSYNC_FILE_NOT_FOUND_CODE:
                        # Raised by rsync_down. Remote log dir may not exist
                        # since the job can be run on some part of the nodes.
                        logger.debug(
                            f'{runner.node_id} does not have the tasks/*.')
                    else:
                        raise

            parallel_args = [
                (runner, local_log_dir, remote_log) for runner in runners
            ]
            subprocess_utils.run_in_parallel(_rsync_down, parallel_args)
        else:  # download job logs
            local_log_dir = os.path.join(local_dir, 'managed_jobs',
                                         run_timestamp)
            os.makedirs(os.path.dirname(os.path.expanduser(local_log_dir)),
                        exist_ok=True)
            log_file = os.path.join(local_log_dir, 'run.log')

            # TODO(kevin): Migrate stream_logs to gRPC
            code = managed_jobs.ManagedJobCodeGen.stream_logs(
                job_name=None,
                job_id=int(job_id),
                follow=False,
                controller=False)
            # With the stdin=subprocess.DEVNULL, the ctrl-c will not
            # kill the process, so we need to handle it manually here.
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, backend_utils.interrupt_handler)
                signal.signal(signal.SIGTSTP, backend_utils.stop_handler)

            # We redirect the output to the log file
            # and disable the STDOUT and STDERR
            self.run_on_head(
                handle,
                code,
                log_path=os.path.expanduser(log_file),
                stream_logs=False,
                process_stream=False,
                ssh_mode=command_runner.SshMode.INTERACTIVE,
            )

        logger.debug(f'{colorama.Fore.CYAN}'
                     f'Job {job_id} logs: {local_log_dir}'
                     f'{colorama.Style.RESET_ALL}')
        return {str(job_id): local_log_dir}

    def teardown_no_lock(self,
                         handle: CloudVmRayResourceHandle,
                         terminate: bool,
                         purge: bool = False,
                         post_teardown_cleanup: bool = True,
                         refresh_cluster_status: bool = True,
                         remove_from_db: bool = True) -> None:
        """Teardown the cluster without acquiring the cluster status lock.

        NOTE: This method should not be called without holding the cluster
        status lock already.

        refresh_cluster_status is only used internally in the status refresh
        process, and should not be set to False in other cases.

        Raises:
            RuntimeError: If the cluster fails to be terminated/stopped.
        """
        try:
            handle.close_skylet_ssh_tunnel()
        except Exception as e:  # pylint: disable=broad-except
            # Not critical to the cluster teardown, just log a warning.
            logger.warning(
                'Failed to close Skylet SSH tunnel for cluster '
                f'{handle.cluster_name}: '
                f'{common_utils.format_exception(e, use_bracket=True)}')

        exclude_request_to_kill = 'sky.down' if terminate else 'sky.stop'
        # We have to kill the cluster requests again within the lock, because
        # any pending requests on the same cluster should be cancelled after
        # the cluster is terminated/stopped. Otherwise, it will be quite
        # confusing to see the cluster restarted immediately after it is
        # terminated/stopped, when there is a pending launch request.
        try:
            # TODO(zhwu): we should get rid of this when it is being called
            # internally without involving an API server, e.g., when a
            # controller is trying to terminate a cluster.
            requests_lib.kill_cluster_requests(handle.cluster_name,
                                               exclude_request_to_kill)
        except Exception as e:  # pylint: disable=broad-except
            # We allow the failure to kill other launch requests, because
            # it is not critical to the cluster teardown.
            logger.warning(
                'Failed to kill other launch requests for the '
                f'cluster {handle.cluster_name}: '
                f'{common_utils.format_exception(e, use_bracket=True)}')
        if refresh_cluster_status:
            try:
                prev_cluster_status, refreshed_handle = (
                    backend_utils.refresh_cluster_status_handle(
                        handle.cluster_name,
                        # There is a case where
                        # 1. The cluster was interrupted during provisioning.
                        # 2. The API request to create the cluster instances was
                        #    sent to the cloud, but hasn't been processed yet.
                        # In this case, the cluster will be INIT. We should do a
                        # hard status refresh to see if the instances are
                        # actually there or not. Otherwise, teardown may not
                        # find the instances, leading to a leak. This was
                        # observed in AWS. See also
                        # _LAUNCH_DOUBLE_CHECK_WINDOW in backend_utils.py.
                        force_refresh_statuses={status_lib.ClusterStatus.INIT},
                        cluster_lock_already_held=True,
                        cluster_resource_lock_already_held=True,
                        retry_if_missing=False))
                if refreshed_handle is not None:
                    # Use the latest handle from status refresh to avoid acting
                    # on stale runtime metadata persisted earlier in launch.
                    handle = refreshed_handle
            except exceptions.ClusterStatusFetchingError:
                logger.warning(
                    'Failed to fetch cluster status for '
                    f'{handle.cluster_name!r}. Assuming the cluster is still '
                    'up.')
                prev_cluster_status = (
                    global_user_state.get_status_from_cluster_name(
                        handle.cluster_name))
        else:
            prev_cluster_status = (
                global_user_state.get_status_from_cluster_name(
                    handle.cluster_name))
        if prev_cluster_status is None:
            # When the cluster is not in the cluster table, we guarantee that
            # all related resources / cache / config are cleaned up, i.e. it
            # is safe to skip and return True.
            ux_utils.console_newline()
            logger.warning(
                f'Cluster {handle.cluster_name!r} is already terminated. '
                'Skipped.')
            return

        if handle.cluster_yaml is None:
            logger.warning(f'Cluster {handle.cluster_name!r} has no '
                           f'provision yaml so it '
                           'has not been provisioned. Skipped.')
            global_user_state.remove_cluster(handle.cluster_name,
                                             terminate=terminate)
            return
        log_path = os.path.join(os.path.expanduser(self.log_dir),
                                'teardown.log')
        log_abs_path = os.path.abspath(log_path)
        launched_resources = handle.launched_resources.assert_launchable()
        cloud = launched_resources.cloud
        config = global_user_state.get_cluster_yaml_dict(handle.cluster_yaml)
        cluster_name = handle.cluster_name
        cluster_name_on_cloud = handle.cluster_name_on_cloud

        # Avoid possibly unbound warnings. Code below must overwrite these vars:
        returncode = 0
        stdout = ''
        stderr = ''

        if (cloud.PROVISIONER_VERSION >=
                clouds.ProvisionerVersion.RAY_PROVISIONER_SKYPILOT_TERMINATOR):
            logger.debug(f'Provisioner version: {cloud.PROVISIONER_VERSION} '
                         'using new provisioner for teardown.')
            # Stop the ray autoscaler first to avoid the head node trying to
            # re-launch the worker nodes, during the termination of the
            # cluster.
            if handle.provision_runtime_metadata.has_ray:
                try:
                    # We do not check the return code, since Ray returns
                    # non-zero return code when calling Ray stop,
                    # even when the command was executed successfully.
                    self.run_on_head(handle,
                                     f'{constants.SKY_RAY_CMD} stop --force')
                except exceptions.FetchClusterInfoError:
                    # This error is expected if the previous cluster IP is
                    # failed to be found,
                    # i.e., the cluster is already stopped/terminated.
                    if prev_cluster_status == status_lib.ClusterStatus.UP:
                        logger.warning(
                            'Failed to take down Ray autoscaler on the head '
                            'node. It might be because the cluster\'s head '
                            'node has already been terminated. It is fine to '
                            'skip this.')

            try:
                provisioner.teardown_cluster(repr(cloud),
                                             resources_utils.ClusterName(
                                                 cluster_name,
                                                 cluster_name_on_cloud),
                                             terminate=terminate,
                                             provider_config=config['provider'])
            except Exception as e:  # pylint: disable=broad-except
                if purge:
                    logger.warning(
                        _TEARDOWN_PURGE_WARNING.format(
                            reason='stopping/terminating cluster nodes',
                            details=common_utils.format_exception(
                                e, use_bracket=True)))
                else:
                    raise

            if post_teardown_cleanup:
                self.post_teardown_cleanup(handle, terminate, purge,
                                           remove_from_db)
            return

        if (isinstance(cloud, clouds.IBM) and terminate and
                prev_cluster_status == status_lib.ClusterStatus.STOPPED):
            # pylint: disable= W0622 W0703 C0415
            from sky.adaptors import ibm
            from sky.skylet.providers.ibm.vpc_provider import IBMVPCProvider

            config_provider = global_user_state.get_cluster_yaml_dict(
                handle.cluster_yaml)['provider']
            region = config_provider['region']
            search_client = ibm.search_client()
            vpc_found = False
            # pylint: disable=unsubscriptable-object
            vpcs_filtered_by_tags_and_region = search_client.search(
                query=(f'type:vpc AND tags:{cluster_name_on_cloud} '
                       f'AND region:{region}'),
                fields=['tags', 'region', 'type'],
                limit=1000).get_result()['items']
            vpc_id = None
            try:
                vpc_id = vpcs_filtered_by_tags_and_region[0]['crn'].rsplit(
                    ':', 1)[-1]
                vpc_found = True
            except Exception:
                logger.critical('failed to locate vpc for ibm cloud')
                returncode = -1

            if vpc_found:
                # Delete VPC and it's associated resources
                vpc_provider = IBMVPCProvider(
                    config_provider['resource_group_id'], region,
                    cluster_name_on_cloud)
                vpc_provider.delete_vpc(vpc_id, region)
                # successfully removed cluster as no exception was raised
                returncode = 0

        else:
            config['provider']['cache_stopped_nodes'] = not terminate
            with tempfile.NamedTemporaryFile('w',
                                             prefix='sky_',
                                             delete=False,
                                             suffix='.yml') as f:
                yaml_utils.dump_yaml(f.name, config)
                f.flush()

                teardown_verb = 'Terminating' if terminate else 'Stopping'
                with rich_utils.safe_status(
                        ux_utils.spinner_message(
                            f'{teardown_verb}: {cluster_name}', log_path)):
                    # FIXME(zongheng): support retries. This call can fail for
                    # example due to GCP returning list requests per limit
                    # exceeded.
                    returncode, stdout, stderr = log_lib.run_with_log(
                        ['ray', 'down', '-y', f.name],
                        log_abs_path,
                        stream_logs=False,
                        require_outputs=True,
                        # Disable stdin to avoid ray outputs mess up the
                        # terminal with misaligned output when multithreading/
                        # multiprocessing are used.
                        # Refer to: https://github.com/ray-project/ray/blob/d462172be7c5779abf37609aed08af112a533e1e/python/ray/autoscaler/_private/subprocess_output_util.py#L264 # pylint: disable=line-too-long
                        stdin=subprocess.DEVNULL)
        if returncode != 0:
            if purge:
                logger.warning(
                    _TEARDOWN_PURGE_WARNING.format(
                        reason='stopping/terminating cluster nodes',
                        details=stderr))
            # 'TPU must be specified.': This error returns when we call "gcloud
            #   delete" with an empty VM list where no instance exists. Safe to
            #   ignore it and do cleanup locally. TODO(wei-lin): refactor error
            #   handling mechanism.
            #
            # 'SKYPILOT_ERROR_NO_NODES_LAUNCHED': this indicates nodes are
            #   never launched and the errors are related to pre-launch
            #   configurations (such as VPC not found). So it's safe & good UX
            #   to not print a failure message.
            elif ('TPU must be specified.' not in stderr and
                  provision_constants.ERROR_NO_NODES_LAUNCHED not in stderr):
                raise RuntimeError(
                    _TEARDOWN_FAILURE_MESSAGE.format(
                        extra_reason='',
                        cluster_name=common_utils.cluster_name_in_hint(
                            cluster_name, cluster_name_on_cloud),
                        stdout=stdout,
                        stderr=stderr))

        # No need to clean up if the cluster is already terminated
        # (i.e., prev_status is None), as the cleanup has already been done
        # if the cluster is removed from the status table.
        if post_teardown_cleanup:
            self.post_teardown_cleanup(handle, terminate, purge)

    def post_teardown_cleanup(self,
                              handle: CloudVmRayResourceHandle,
                              terminate: bool,
                              purge: bool = False,
                              remove_from_db: bool = True,
                              failover: bool = False) -> None:
        """Cleanup local configs/caches and delete TPUs after teardown.

        This method will handle the following cleanup steps:
        * Deleting the TPUs;
        * Removing ssh configs for the cluster;
        * Deleting the open ports;
        * Deleting the custom multi network infrastructure based on the
          failover flag (e.g. delete firewalls, subnets, and VPCs for GPU
          Direct if failover is False, otherwise, only delete the subnets);
        * Updating the local state of the cluster;
        * Removing the terminated cluster's scripts and ray yaml files.
        """
        cluster_name_on_cloud = handle.cluster_name_on_cloud
        cloud = handle.launched_resources.cloud

        if terminate and handle.launched_resources.is_image_managed is True:
            # Delete the image when terminating a "cloned" cluster, i.e.,
            # whose image is created by SkyPilot (--clone-disk-from)
            logger.debug(f'Deleting image {handle.launched_resources.image_id}')
            cluster_resources = handle.launched_resources
            cluster_cloud = cluster_resources.cloud
            image_dict = cluster_resources.image_id
            assert cluster_cloud is not None, cluster_resources
            assert image_dict is not None and len(image_dict) == 1
            image_id = list(image_dict.values())[0]
            try:
                cluster_cloud.delete_image(image_id,
                                           handle.launched_resources.region)
            except exceptions.CommandError as e:
                logger.warning(
                    f'Failed to delete cloned image {image_id}. Please '
                    'remove it manually to avoid image leakage. Details: '
                    f'{common_utils.format_exception(e, use_bracket=True)}')
        if terminate:
            # This function could be directly called from status refresh,
            # where we need to cleanup the cluster profile.
            metadata_utils.remove_cluster_metadata(handle.cluster_name)
            # The cluster yaml does not exist when skypilot has not found
            # the right resource to provision the cluster.
            if handle.cluster_yaml is not None:
                launched_resources = (
                    handle.launched_resources.assert_launchable())
                cloud = launched_resources.cloud
                config = global_user_state.get_cluster_yaml_dict(
                    handle.cluster_yaml)
                ports_cleaned_up = False
                custom_multi_network_cleaned_up = False
                try:
                    cloud.check_features_are_supported(
                        launched_resources,
                        {clouds.CloudImplementationFeatures.OPEN_PORTS})
                    provision_lib.cleanup_ports(repr(cloud),
                                                cluster_name_on_cloud,
                                                handle.launched_resources.ports,
                                                config['provider'])
                    ports_cleaned_up = True
                except exceptions.NotSupportedError:
                    ports_cleaned_up = True
                except exceptions.PortDoesNotExistError:
                    logger.debug('Ports do not exist. Skipping cleanup.')
                    ports_cleaned_up = True
                except Exception as e:  # pylint: disable=broad-except
                    if purge:
                        msg = common_utils.format_exception(e, use_bracket=True)
                        logger.warning(
                            f'Failed to cleanup ports. Skipping since purge is '
                            f'set. Details: {msg}')
                    else:
                        raise

                # Clean up custom multi networks, e.g. the subnets, firewalls,
                # and VPCs created for GCP GPUDirect TCPX
                try:
                    cloud.check_features_are_supported(
                        handle.launched_resources, {
                            clouds.CloudImplementationFeatures.
                            CUSTOM_MULTI_NETWORK
                        })
                    provision_lib.cleanup_custom_multi_network(
                        repr(cloud), cluster_name_on_cloud, config['provider'],
                        failover)
                    custom_multi_network_cleaned_up = True
                except exceptions.NotSupportedError:
                    custom_multi_network_cleaned_up = True
                except Exception as e:  # pylint: disable=broad-except
                    if purge:
                        msg = common_utils.format_exception(e, use_bracket=True)
                        logger.warning(
                            f'Failed to cleanup custom multi network. Skipping '
                            f'since purge is set. Details: {msg}')
                    else:
                        raise

                # Clean up all cluster resources (e.g., Kubernetes services).
                # This is a no-op for most clouds, but Kubernetes needs it to
                # clean up orphaned services when pods are deleted externally.
                try:
                    provision_lib.cleanup_cluster_resources(
                        repr(cloud), cluster_name_on_cloud, config['provider'])
                except Exception as e:  # pylint: disable=broad-except
                    if purge:
                        msg = common_utils.format_exception(e, use_bracket=True)
                        logger.warning(
                            f'Failed to cleanup cluster resources. Skipping '
                            f'since purge is set. Details: {msg}')
                    else:
                        raise

                if ports_cleaned_up and custom_multi_network_cleaned_up:
                    try:
                        self.remove_cluster_config(handle)
                    except Exception as e:  # pylint: disable=broad-except
                        if purge:
                            msg = common_utils.format_exception(
                                e, use_bracket=True)
                            logger.warning(
                                f'Failed to remove cluster config. Skipping '
                                f'since purge is set. Details: {msg}')
                        else:
                            raise

        cluster_utils.SSHConfigHelper.remove_cluster(handle.cluster_name)

        def _detect_abnormal_non_terminated_nodes(
                handle: CloudVmRayResourceHandle) -> None:
            # Confirm that instances have actually transitioned state before
            # updating the state database. We do this immediately before
            # removing the state from the database, so that we can guarantee
            # that this is always called before the state is removed. We
            # considered running this check as part of
            # provisioner.teardown_cluster or provision.terminate_instances, but
            # it would open the door to code paths that successfully call this
            # function but do not first call teardown_cluster or
            # terminate_instances. See
            # https://github.com/skypilot-org/skypilot/pull/4443#discussion_r1872798032
            # The cluster YAML is immutable during teardown; fetch it once
            # instead of re-reading it from the database on every retry.
            config = global_user_state.get_cluster_yaml_dict(
                handle.cluster_yaml)
            attempts = 0
            while True:
                logger.debug(f'instance statuses attempt {attempts + 1}')
                node_status_dict = provision_lib.query_instances(
                    repr(cloud),
                    handle.cluster_name,
                    cluster_name_on_cloud,
                    config['provider'],
                    non_terminated_only=False)

                unexpected_nodes = []
                for node_id, node_status_tuple in node_status_dict.items():
                    node_status, reason = node_status_tuple
                    reason_str = '' if reason is None else f' ({reason})'
                    logger.debug(f'{node_id} status: {node_status}{reason_str}')
                    # FIXME(cooperc): Some clouds (e.g. GCP) do not distinguish
                    # between "stopping/stopped" and "terminating/terminated",
                    # so we allow for either status instead of casing on
                    # `terminate`.
                    if node_status not in [
                            None, status_lib.ClusterStatus.STOPPED
                    ]:
                        unexpected_nodes.append((node_id, node_status, reason))

                if not unexpected_nodes:
                    break

                attempts += 1
                if attempts < _TEARDOWN_WAIT_MAX_ATTEMPTS:
                    time.sleep(_TEARDOWN_WAIT_BETWEEN_ATTEMPS_SECONDS)
                else:
                    unexpected_nodes_str = '\n'.join([
                        f'  - {node_id}: {node_status}' +
                        (f' ({reason})' if reason else '')
                        for node_id, node_status, reason in unexpected_nodes
                    ])
                    raise RuntimeError(f'Instances in unexpected state:\n'
                                       f'{unexpected_nodes_str}')

        # If cluster_yaml is None, the cluster should ensured to be terminated,
        # so we don't need to do the double check.
        if handle.cluster_yaml is not None:
            try:
                _detect_abnormal_non_terminated_nodes(handle)
            except exceptions.ClusterStatusFetchingError as e:
                if purge:
                    msg = common_utils.format_exception(e, use_bracket=True)
                    logger.warning(
                        'Failed abnormal non-terminated nodes cleanup. '
                        'Skipping and cleaning up as purge is set. '
                        f'Details: {msg}')
                    logger.debug(f'Full exception details: {msg}',
                                 exc_info=True)
                else:
                    raise

        if not terminate or remove_from_db:
            global_user_state.remove_cluster(handle.cluster_name,
                                             terminate=terminate)

    def remove_cluster_config(self, handle: CloudVmRayResourceHandle) -> None:
        """Remove the YAML config of a cluster."""
        cluster_yaml_path = handle.cluster_yaml
        handle.cluster_yaml = None
        global_user_state.update_cluster_handle(handle.cluster_name, handle)
        # Removing the cluster YAML can cause some unexpected stability issues.
        # See #5011.
        # global_user_state.remove_cluster_yaml(handle.cluster_name)
        common_utils.remove_file_if_exists(cluster_yaml_path)

    def set_autostop(self,
                     handle: CloudVmRayResourceHandle,
                     idle_minutes_to_autostop: int | None,
                     wait_for: autostop_lib.AutostopWaitFor | None,
                     down: bool = False,
                     stream_logs: bool = True,
                     hook: str | None = None,
                     hook_timeout: int | None = None,
                     hooks: list[dict[str, Any]] | None = None) -> None:
        if not handle.provision_runtime_metadata.has_skylet:
            return
        # The core.autostop() function should have already checked that the
        # cloud and resources support requested autostop.
        if idle_minutes_to_autostop is not None:
            # Skip auto-stop for Kubernetes and RunPod clusters.
            if (isinstance(handle.launched_resources.cloud,
                           (clouds.Kubernetes, clouds.RunPod)) and not down and
                    idle_minutes_to_autostop >= 0):
                # We should hit this code path only for the controllers on
                # Kubernetes and RunPod clusters, because autostop() will
                # skip the supported feature check. Non-controller k8s/runpod
                # clusters will have already errored out.
                controller = controller_utils.Controllers.from_name(
                    handle.cluster_name)
                assert (controller is not None), handle.cluster_name
                if (controller
                        == controller_utils.Controllers.SKY_SERVE_CONTROLLER and
                        isinstance(handle.launched_resources.cloud,
                                   clouds.Kubernetes)):
                    # For SkyServe controllers on Kubernetes: override autostop
                    # behavior to force autodown (instead of no-op)
                    # to avoid dangling controllers.

                    # down = False is the default, but warn the user in case
                    # they have explicitly specified it.
                    # TODO(cooperc): Fix for new autostop stuff.
                    config_override_down = skypilot_config.get_nested(
                        (controller.value.controller_type, 'controller',
                         'autostop', 'down'), None)
                    if config_override_down is False:  # will not match None
                        logger.warning(
                            'SkyServe controller autodown is disabled in the '
                            '~/.sky/config.yaml configuration file '
                            '(serve.controller.autostop.down_when_idle), but '
                            'it is force enabled for Kubernetes clusters.')

                    down = True
                else:
                    logger.info('Auto-stop is not supported for Kubernetes '
                                'and RunPod clusters. Skipping.')
                    return

            # Check if we're stopping spot
            assert (handle.launched_resources is not None and
                    handle.launched_resources.cloud is not None), handle
            # On Kubernetes, cap any preemption-hook timeout to the pod's
            # terminationGracePeriodSeconds cap so the stored value
            # matches kubelet's actual SIGKILL boundary. Done before the
            # gRPC/SSH branch so the SSH codegen fallback path also sees
            # the capped values (otherwise pre-gRPC skylets on K8s would
            # store misleading timeouts).
            if isinstance(handle.launched_resources.cloud, clouds.Kubernetes):
                hooks = k8s_cloud.cap_preemption_hook_timeouts(hooks)
            if handle.is_grpc_enabled_with_flag:
                request = autostopv1_pb2.SetAutostopRequest(
                    idle_minutes=idle_minutes_to_autostop,
                    backend=self.NAME,
                    wait_for=wait_for.to_protobuf() if wait_for is not None else
                    autostopv1_pb2.AUTOSTOP_WAIT_FOR_UNSPECIFIED,
                    down=down,
                )
                if hook:
                    request.hook = hook
                if hook_timeout is not None:
                    request.hook_timeout = hook_timeout
                # v7+: send the full hooks list inline on the same RPC.
                # Three states for the `hooks` arg:
                #   None  → legacy/no-hook-aware caller; don't touch stored
                #   []    → caller explicitly clears stored hooks
                #   [...] → replace stored hooks with this list
                if hooks is None:
                    pass  # leave stored hooks alone
                elif not hooks:
                    request.clear_hooks = True
                else:
                    request.hooks.extend(autostop_lib.hooks_to_protobuf(hooks))
                backend_utils.invoke_skylet_with_retries(lambda: SkyletClient(
                    handle.get_grpc_channel()).set_autostop(request))
            else:
                code = autostop_lib.AutostopCodeGen.set_autostop(
                    idle_minutes_to_autostop, self.NAME, wait_for, down, hook,
                    hook_timeout, hooks)
                returncode, _, stderr = self.run_on_head(
                    handle, code, require_outputs=True, stream_logs=stream_logs)
                subprocess_utils.handle_returncode(returncode,
                                                   code,
                                                   'Failed to set autostop',
                                                   stderr=stderr,
                                                   stream_logs=stream_logs)
            global_user_state.set_cluster_autostop_value(
                handle.cluster_name, idle_minutes_to_autostop, down)

        # Add/Remove autodown annotations to/from Kubernetes pods.
        if isinstance(handle.launched_resources.cloud, clouds.Kubernetes):
            kubernetes_utils.set_autodown_annotations(
                handle=handle,
                idle_minutes_to_autostop=idle_minutes_to_autostop,
                down=down)

    def probe_autostopping(self,
                           handle: CloudVmRayResourceHandle,
                           stream_logs: bool = True) -> bool | None:
        """Ask the skylet whether the cluster is autostopping.

        Returns:
            True or False when the skylet answered, and None when the probe
            itself failed (transient network/gRPC issues, or a misbehaving
            cluster) so the caller can tell "not autostopping" apart from
            "unknown". Collapsing the two lets a single failed probe demote a
            live autodown to UP, which rewrites the cluster's AUTOSTOPPING
            transition event and re-anchors any deadline measured from it.
        """
        if not handle.provision_runtime_metadata.has_skylet:
            return False
        if handle.head_ip is None:
            # The head node of the cluster is not UP or in an abnormal state.
            # We cannot check if the cluster is autostopping.
            return False

        if handle.is_grpc_enabled_with_flag:
            try:
                request = autostopv1_pb2.IsAutostoppingRequest()
                response = backend_utils.invoke_skylet_with_retries(
                    lambda: SkyletClient(handle.get_grpc_channel()
                                        ).is_autostopping(request))
                return response.is_autostopping
            except Exception as e:  # pylint: disable=broad-except
                # The cluster may have been terminated, causing the gRPC call
                # to timeout and fail.
                logger.debug(f'Failed to check if cluster is autostopping: {e}')
                return None

        code = autostop_lib.AutostopCodeGen.is_autostopping()
        returncode, stdout, stderr = self.run_on_head(handle,
                                                      code,
                                                      require_outputs=True,
                                                      stream_logs=stream_logs)
        # We see some cases in the wild where a misbehaving cluster/k8s
        # apiserver (not sure which) can have a returncode of 0 but
        # incorrectly empty stdout. Don't try to decode this.
        if returncode == 0 and stdout:
            return message_utils.decode_payload(stdout)
        logger.debug('Failed to check if cluster is autostopping with '
                     f'{returncode}: {stdout+stderr}\n'
                     f'Command: {code}')
        return None

    def is_definitely_autostopping(self,
                                   handle: CloudVmRayResourceHandle,
                                   stream_logs: bool = True) -> bool:
        """Check if the cluster is autostopping.

        Returns:
            True if the cluster is definitely autostopping. It is possible
            that the cluster is still autostopping when False is returned,
            due to errors like transient network issues. Callers that must
            distinguish those two cases should use ``probe_autostopping``.
        """
        return self.probe_autostopping(handle, stream_logs=stream_logs) is True

    # TODO(zhwu): Refactor this to a CommandRunner class, so different backends
    # can support its own command runner.
    @timeline.event
    @context_utils.cancellation_guard
    def run_on_head(
        self,
        handle: CloudVmRayResourceHandle,
        cmd: str,
        *,
        port_forward: list[int] | None = None,
        log_path: str = '/dev/null',
        stream_logs: bool = False,
        ssh_mode: command_runner.SshMode = command_runner.SshMode.
        NON_INTERACTIVE,
        under_remote_workdir: bool = False,
        require_outputs: bool = False,
        separate_stderr: bool = False,
        process_stream: bool = True,
        source_bashrc: bool = False,
        **kwargs,
    ) -> int | tuple[int, str, str]:
        """Runs 'cmd' on the cluster's head node.

        It will try to fetch the head node IP if it is not cached.

        Args:
            handle: The ResourceHandle to the cluster.
            cmd: The command to run.

            Advanced options:

            port_forward: A list of ports to forward.
            log_path: The path to the log file.
            stream_logs: Whether to stream the logs to stdout/stderr.
            ssh_mode: The mode to use for ssh.
                See command_runner.SSHCommandRunner.SSHMode for more details.
            under_remote_workdir: Whether to run the command under the remote
                workdir ~/sky_workdir.
            require_outputs: Whether to return the stdout and stderr of the
                command.
            separate_stderr: Whether to separate stderr from stdout.
            process_stream: Whether to post-process the stdout/stderr of the
                command, such as replacing or skipping lines on the fly. If
                enabled, lines are printed only when '\r' or '\n' is found.
            source_bashrc: Whether to source bashrc when running on the command
                on the VM. If it is a user-related commands, it would always be
                good to source bashrc to make sure the env vars are set.

        Returns:
            returncode
            or
            A tuple of (returncode, stdout, stderr).

        Raises:
            exceptions.FetchClusterInfoError: If the cluster info cannot be
                fetched.
        """
        # This will try to fetch the head node IP if it is not cached.

        runners = handle.get_command_runners()
        head_runner = runners[0]
        if under_remote_workdir:
            cmd = f'cd {SKY_REMOTE_WORKDIR} && {cmd}'

        return head_runner.run_driver(
            cmd,
            port_forward=port_forward,
            log_path=log_path,
            process_stream=process_stream,
            stream_logs=stream_logs,
            ssh_mode=ssh_mode,
            require_outputs=require_outputs,
            separate_stderr=separate_stderr,
            source_bashrc=source_bashrc,
            **kwargs,
        )

    # --- Utilities ---

    @context_utils.cancellation_guard
    @timeline.event
    def _check_existing_cluster(
            self,
            task: task_lib.Task,
            to_provision: resources_lib.Resources | None,
            cluster_name: str,
            dryrun: bool = False) -> RetryingVmProvisioner.ToProvisionConfig:
        """Checks if the cluster exists and returns the provision config.

        Raises:
            exceptions.ResourcesMismatchError: If the resources in the task
                does not match the existing cluster.
            exceptions.InvalidClusterNameError: If the cluster name is invalid.
            # TODO(zhwu): complete the list of exceptions.
        """
        record = global_user_state.get_cluster_from_name(
            cluster_name, include_user_info=False, summary_response=True)
        if record is None:
            handle_before_refresh = None
            status_before_refresh = None
        else:
            handle_before_refresh = record['handle']
            status_before_refresh = record['status']

        handle: CloudVmRayResourceHandle | None
        prev_cluster_status, handle = (status_before_refresh,
                                       handle_before_refresh)

        if not dryrun:
            # We force refresh any cluster (1) with INIT status, or (2) has
            # autostop set. This is to determine the actual state of such a
            # cluster and to make the hint that uses prev_cluster_status more
            # accurate.
            record = backend_utils.refresh_cluster_record(
                cluster_name,
                force_refresh_statuses={status_lib.ClusterStatus.INIT},
                cluster_lock_already_held=True,
                cluster_resource_lock_already_held=True,
                include_user_info=False,
                summary_response=True,
            )
            if record is not None:
                prev_cluster_status = record['status']
                handle = record['handle']
            else:
                prev_cluster_status = None
                handle = None
        # We should check the cluster_ever_up after refresh, because if the
        # cluster is terminated (through console or auto-down), the record will
        # become None and the cluster_ever_up should be considered as False.
        cluster_ever_up = record is not None and record['cluster_ever_up']
        prev_cluster_hash = (record['cluster_hash']
                             if record is not None else None)
        prev_config_hash = record['config_hash'] if record is not None else None
        logger.debug(f'cluster_ever_up: {cluster_ever_up}')
        logger.debug(f'record: {record}')

        if prev_cluster_status is not None:
            assert handle is not None
            # Cluster already exists.
            self.check_resources_fit_cluster(handle, task)

            # Use the existing cluster.
            assert handle.launched_resources is not None, (cluster_name, handle)
            # Take a random resource in order to get resource info that applies
            # to all resources.
            one_task_resource = list(task.resources)[0]

            # Assume resources share the same ports.
            for resource in task.resources:
                assert resource.ports == one_task_resource.ports
            requested_ports_set = resources_utils.port_ranges_to_set(
                one_task_resource.ports)
            current_ports_set = resources_utils.port_ranges_to_set(
                handle.launched_resources.ports)
            all_ports = resources_utils.port_set_to_ranges(current_ports_set |
                                                           requested_ports_set)
            to_provision = handle.launched_resources
            assert to_provision is not None
            to_provision = to_provision.assert_launchable()
            if (to_provision.cloud.OPEN_PORTS_VERSION
                    <= clouds.OpenPortsVersion.LAUNCH_ONLY):
                if not requested_ports_set <= current_ports_set:
                    current_cloud = to_provision.cloud
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.NotSupportedError(
                            'Failed to open new ports on an existing cluster '
                            f'with the current cloud {current_cloud} as it only'
                            ' supports opening ports on launch of the cluster. '
                            'Please terminate the existing cluster and launch '
                            'a new cluster with the desired ports open.')
            if all_ports:
                to_provision = to_provision.copy(ports=all_ports)
            # Docker login should always be the same for all resources, since
            # it's set from envs.
            for resource in task.resources:
                assert (resource.docker_login_config ==
                        one_task_resource.docker_login_config), (
                            resource.docker_login_config,
                            one_task_resource.docker_login_config)
            # If we have docker login config in the new task, override the
            # existing resources to pick up new credentials. This allows the
            # user to specify new or fixed credentials if the existing
            # credentials are not working. If we don't do this, the credentials
            # from the existing resources will always be reused.
            if one_task_resource.docker_login_config is not None:
                to_provision = to_provision.copy(
                    _docker_login_config=one_task_resource.docker_login_config)

            # Re-launch with changed config.hooks: propagate the new list
            # onto to_provision so the next SetAutostop RPC carries the
            # up-to-date scripts / timeouts.
            if one_task_resource.hooks != to_provision.hooks:
                # On Kubernetes the pod's terminationGracePeriodSeconds is
                # fixed at pod-creation time. Warn if the new hooks would
                # need more grace than the existing pod has.
                grace_warning = (
                    k8s_cloud.warn_if_preemption_grace_change_requires_relaunch(
                        to_provision.cloud, to_provision.hooks,
                        one_task_resource.hooks))
                if grace_warning is not None:
                    logger.warning(grace_warning)
                to_provision = to_provision.copy(hooks=one_task_resource.hooks)

            # cluster_config_overrides should be the same for all resources.
            for resource in task.resources:
                assert (resource.cluster_config_overrides ==
                        one_task_resource.cluster_config_overrides)

            cluster_yaml_str = global_user_state.get_cluster_yaml_str(
                cluster_name)
            cluster_yaml_obj = (yaml_utils.safe_load(cluster_yaml_str)
                                if cluster_yaml_str is not None else None)

            def _get_pod_config(yaml_obj: dict[str, Any]) -> dict[str, Any]:
                return (yaml_obj.get('available_node_types',
                                     {}).get('ray_head_default',
                                             {}).get('node_config', {}))

            if isinstance(to_provision.cloud,
                          clouds.Kubernetes) and cluster_yaml_obj is not None:
                # Warn users if the Kubernetes pod config is different
                # from the existing cluster.
                desired_cluster_yaml_obj = (
                    kubernetes_utils.combine_pod_config_fields_and_metadata(
                        cluster_yaml_obj,
                        cluster_config_overrides=one_task_resource.
                        cluster_config_overrides,
                        cloud=to_provision.cloud,
                        context=to_provision.region))

                if _get_pod_config(desired_cluster_yaml_obj) != _get_pod_config(
                        cluster_yaml_obj):
                    # pylint: disable=line-too-long
                    logger.warning(
                        f'{colorama.Fore.YELLOW}WARNING: Kubernetes pod config mismatch detected. Task requires different '
                        f'pod config than the existing cluster. The existing '
                        f'cluster will be used with its current pod config.'
                        f'To apply use your task\'s new pod config:\n'
                        f'  • Use a new cluster'
                        f'  • Or restart this cluster: sky down {cluster_name}; sky launch -c {cluster_name} ...'
                        f'{colorama.Style.RESET_ALL}')

            # Check for volume mount warnings
            if task.volume_mounts:
                # Get existing cluster's volume mounts from cluster yaml
                existing_volume_names = set()
                try:
                    if cluster_yaml_obj is not None:
                        # Extract volume names from existing cluster
                        node_config = _get_pod_config(cluster_yaml_obj)

                        if isinstance(to_provision.cloud, clouds.Kubernetes):
                            # Check for K8s-style persistent volumes
                            # (spec.volumes)
                            # See sky/templates/kubernetes-ray.yml.j2.
                            volumes = node_config.get('spec',
                                                      {}).get('volumes', [])
                            for vol in volumes:
                                # Volume from PVC has structure:
                                # - name: <volume_name>
                                #   persistentVolumeClaim:
                                #     claimName: <volume_name_on_cloud>
                                if 'persistentVolumeClaim' in vol:
                                    pvc = vol.get('persistentVolumeClaim', {})
                                    # Use claimName (volume_name_on_cloud) to
                                    # be consistent with RunPod.
                                    vol_name_on_cloud = pvc.get('claimName')
                                    if vol_name_on_cloud:
                                        existing_volume_names.add(
                                            vol_name_on_cloud)

                            # Check for K8s ephemeral volumes
                            # See sky/templates/kubernetes-ray.yml.j2.
                            provider_config = cluster_yaml_obj.get(
                                'provider', {})
                            ephemeral_specs = provider_config.get(
                                'ephemeral_volume_specs', [])
                            for spec in ephemeral_specs:
                                # For ephemeral volumes, we check the mount
                                # path.
                                mount_path = spec.get('path')
                                if mount_path:
                                    existing_volume_names.add(mount_path)

                        elif isinstance(to_provision.cloud, clouds.RunPod):
                            # Check for custom VolumeMounts config
                            # (e.g. RunPod)
                            # See sky/templates/runpod-ray.yml.j2.
                            volume_mounts_config = node_config.get(
                                'VolumeMounts', [])
                            for vol_mount in volume_mounts_config:
                                vol_name = vol_mount.get('VolumeNameOnCloud')
                                if vol_name:
                                    existing_volume_names.add(vol_name)
                except Exception as e:  # pylint: disable=broad-except
                    # If we can't get the existing volume mounts, log debug
                    # and skip the warning check
                    logger.debug(f'Failed to check existing volume mounts: {e}',
                                 exc_info=True)

                # Check if task has new volumes not in existing cluster
                new_ephemeral_volumes = []
                new_persistent_volumes = []
                for volume_mount in task.volume_mounts:
                    # Compare using volume_name for user-facing name
                    if volume_mount.is_ephemeral:
                        if volume_mount.path not in existing_volume_names:
                            new_ephemeral_volumes.append(volume_mount.path)
                    elif (volume_mount.volume_name not in existing_volume_names
                          and volume_mount.volume_config.name_on_cloud
                          not in existing_volume_names):
                        new_persistent_volumes.append(volume_mount.volume_name)

                if new_ephemeral_volumes or new_persistent_volumes:
                    msg_parts = []
                    if new_ephemeral_volumes:
                        msg_parts.append(f'new ephemeral volume(s) with path '
                                         f'{", ".join(new_ephemeral_volumes)}')
                    if new_persistent_volumes:
                        msg_parts.append(
                            f'new volume(s) {", ".join(new_persistent_volumes)}'
                        )

                    volume_msg = ' and '.join(msg_parts)
                    # Capitalize the first letter of the message
                    volume_msg = volume_msg[0].upper() + volume_msg[1:]

                    logger.warning(
                        f'{colorama.Fore.YELLOW}WARNING: {volume_msg} '
                        f'specified in task but not '
                        f'mounted to existing cluster "{cluster_name}". '
                        f'These volumes will not be mounted to the cluster. '
                        f'To mount new volumes, either:\n'
                        f'  • Use a new cluster, or\n'
                        f'  • Terminate and recreate this cluster'
                        f'{colorama.Style.RESET_ALL}')

            return RetryingVmProvisioner.ToProvisionConfig(
                cluster_name,
                to_provision,
                handle.launched_nodes,
                prev_cluster_status=prev_cluster_status,
                prev_handle=handle,
                prev_cluster_ever_up=cluster_ever_up,
                prev_config_hash=prev_config_hash,
                prev_cluster_hash=prev_cluster_hash)
        usage_lib.messages.usage.set_new_cluster()
        # Use the task_cloud, because the cloud in `to_provision` can be changed
        # later during the retry.
        common_utils.check_cluster_name_is_valid(cluster_name)

        if to_provision is None:
            # Recently terminated after refresh. OPTIMIZE usually ran outside
            # the lock, so that decision may be stale by now. Under the lock,
            # ensure we always have a concrete plan via the following order:
            #   1) Reuse last placement snapshot (if available);
            #   2) Else, call injected planner for a fresh plan.
            # If we still have a pre-refresh handle snapshot with a concrete
            # placement, prefer reusing it.
            if (isinstance(handle_before_refresh, CloudVmRayResourceHandle) and
                    handle_before_refresh.launched_resources is not None):
                to_provision = handle_before_refresh.launched_resources
                # Ensure the requested task fits the previous placement.
                self.check_resources_fit_cluster(handle_before_refresh, task)
                # Mirror the original message for reuse path.
                status_before_refresh_str = None
                if status_before_refresh is not None:
                    status_before_refresh_str = status_before_refresh.value
                logger.info(
                    f'The cluster {cluster_name!r} (status: '
                    f'{status_before_refresh_str}) was not found on the cloud: '
                    'it may be autodowned, manually terminated, or its launch '
                    'never succeeded. Provisioning a new cluster by using the '
                    'same resources as its original launch.')
            elif self._planner is not None:
                to_provision = self._planner(task)
                logger.info(
                    'Previous placement snapshot missing; computing a fresh '
                    'plan for provisioning.')
            else:
                # Without a snapshot or planner, we cannot proceed safely.
                # Surface a user-friendly error without a long traceback.
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        'No concrete launch plan available after recent cloud '
                        f'termination of cluster {cluster_name!r}. Ensure the '
                        'OPTIMIZE stage runs or provide concrete resources.')

        return RetryingVmProvisioner.ToProvisionConfig(
            cluster_name,
            to_provision,
            task.num_nodes,
            prev_cluster_status=None,
            prev_handle=None,
            prev_cluster_ever_up=False,
            prev_config_hash=prev_config_hash,
            prev_cluster_hash=None)

    def _execute_storage_mounts(
            self, handle: CloudVmRayResourceHandle,
            storage_mounts: dict[Path, storage_lib.Storage] | None):
        """Executes storage mounts: installing mounting tools and mounting."""
        # Handle cases where `storage_mounts` is None. This occurs when users
        # initiate a 'sky start' command from a Skypilot version that predates
        # the introduction of the `storage_mounts_metadata` feature.
        if storage_mounts is None:
            return

        # Process only mount mode objects here. COPY mode objects have been
        # converted to regular copy file mounts and thus have been handled
        # in the '_execute_file_mounts' method.
        storage_mounts = {
            path: storage_mount
            for path, storage_mount in storage_mounts.items()
            if storage_mount.mode in storage_lib.MOUNTABLE_STORAGE_MODES
        }

        # Handle cases when there aren't any Storages with either MOUNT or
        # MOUNT_CACHED mode.
        if not storage_mounts:
            return
        start = time.time()
        runners = handle.get_command_runners()
        num_threads = subprocess_utils.get_parallel_threads(
            str(handle.launched_resources.cloud))
        log_path = os.path.join(self.log_dir, 'storage_mounts.log')

        plural = 's' if len(storage_mounts) > 1 else ''
        rich_utils.force_update_status(
            ux_utils.spinner_message(
                f'Mounting {len(storage_mounts)} storage{plural}', log_path))

        for dst, storage_obj in storage_mounts.items():
            storage_obj.construct()
            if not os.path.isabs(dst) and not dst.startswith('~/'):
                dst = f'{SKY_REMOTE_WORKDIR}/{dst}'
            # Raised when the bucket is externall removed before re-mounting
            # with sky start.
            if not storage_obj.stores:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageExternalDeletionError(
                        f'The bucket, {storage_obj.name!r}, could not be '
                        f'mounted on cluster {handle.cluster_name!r}. Please '
                        'verify that the bucket exists. The cluster started '
                        'successfully without mounting the bucket.')
            # Get the first store and use it to mount
            store = list(storage_obj.stores.values())[0]
            assert store is not None, storage_obj
            if storage_obj.mode == storage_lib.StorageMode.MOUNT:
                read_only = bool(storage_obj.mount_config and
                                 storage_obj.mount_config.read_only)
                if isinstance(store, storage_lib.HuggingFaceStore):
                    # getattr guards against MountConfig instances serialized
                    # before hf_mount_args existed (and against a None config).
                    hf_mount_args = getattr(storage_obj.mount_config,
                                            'hf_mount_args', None)
                    mount_cmd = store.mount_command(dst,
                                                    read_only=read_only,
                                                    hf_mount_args=hf_mount_args)
                else:
                    mount_cmd = store.mount_command(dst, read_only=read_only)
                action_message = 'Mounting'
            else:
                assert storage_obj.mode == storage_lib.StorageMode.MOUNT_CACHED
                mount_cmd = store.mount_cached_command(
                    dst, config=storage_obj.resolve_mount_cached_config())
                action_message = 'Mounting cached mode'
            src_print = (storage_obj.source
                         if storage_obj.source else storage_obj.name)
            if isinstance(src_print, list):
                src_print = ', '.join(src_print)
            try:
                backend_utils.parallel_data_transfer_to_nodes(
                    runners,
                    source=src_print,
                    target=dst,
                    cmd=mount_cmd,
                    run_rsync=False,
                    action_message=action_message,
                    log_path=log_path,
                    # Need to source bashrc, as the cloud specific CLI or SDK
                    # may require PATH in bashrc.
                    source_bashrc=True,
                    num_threads=num_threads,
                )
            except exceptions.CommandError as e:
                if e.returncode == exceptions.MOUNT_PATH_NON_EMPTY_CODE:
                    mount_path = (f'{colorama.Fore.RED}'
                                  f'{colorama.Style.BRIGHT}{dst}'
                                  f'{colorama.Style.RESET_ALL}')
                    error_msg = (f'Mount path {mount_path} is non-empty.'
                                 f' {mount_path} may be a standard unix '
                                 f'path or may contain files from a previous'
                                 f' task. To fix, change the mount path'
                                 f' to an empty or non-existent path.')
                    raise RuntimeError(error_msg) from None
                else:
                    # By default, raising an error caused from mounting_utils
                    # shows a big heredoc as part of it. Here, we want to
                    # conditionally show the heredoc only if SKYPILOT_DEBUG
                    # is set
                    if env_options.Options.SHOW_DEBUG_INFO.get():
                        raise exceptions.CommandError(
                            e.returncode,
                            command='to mount',
                            error_msg=e.error_msg,
                            detailed_reason=e.detailed_reason)
                    else:
                        # Strip the command (a big heredoc) from the exception
                        raise exceptions.CommandError(
                            e.returncode,
                            command='to mount',
                            error_msg=e.error_msg,
                            detailed_reason=e.detailed_reason) from None

        end = time.time()
        logger.debug(f'Storage mount sync took {end - start} seconds.')
        logger.info(ux_utils.finishing_message('Storage mounted.', log_path))

    def _set_storage_mounts_metadata(
            self, cluster_name: str,
            storage_mounts: dict[Path, storage_lib.Storage] | None) -> None:
        """Sets 'storage_mounts' object in cluster's storage_mounts_metadata.

        After converting Storage objects in 'storage_mounts' to metadata,
        it stores {PATH: StorageMetadata} into the table.
        """
        if not storage_mounts:
            return
        storage_mounts_metadata = {}
        for dst, storage_obj in storage_mounts.items():
            if storage_obj.mode not in storage_lib.MOUNTABLE_STORAGE_MODES:
                # Skip non-mount storage objects, as there is no need to
                # reconstruct them during cluster restart.
                continue
            storage_mounts_metadata[dst] = storage_obj.handle
        lock_id = backend_utils.cluster_file_mounts_lock_id(cluster_name)
        lock_timeout = backend_utils.CLUSTER_FILE_MOUNTS_LOCK_TIMEOUT_SECONDS
        try:
            with locks.get_lock(lock_id, lock_timeout):
                global_user_state.set_cluster_storage_mounts_metadata(
                    cluster_name, storage_mounts_metadata)
        except locks.LockTimeout as e:
            raise RuntimeError(
                f'Failed to store metadata for cluster {cluster_name!r} due to '
                'a timeout when trying to access local database. Please '
                f'try again or manually remove the lock at {lock_id}. '
                f'{common_utils.format_exception(e)}') from None

    def get_storage_mounts_metadata(
            self, cluster_name: str) -> dict[Path, storage_lib.Storage] | None:
        """Gets 'storage_mounts' object from cluster's storage_mounts_metadata.

        After retrieving storage_mounts_metadata, it converts back the
        StorageMetadata to Storage object and restores 'storage_mounts.'
        """
        lock_id = backend_utils.cluster_file_mounts_lock_id(cluster_name)
        lock_timeout = backend_utils.CLUSTER_FILE_MOUNTS_LOCK_TIMEOUT_SECONDS
        try:
            with locks.get_lock(lock_id, lock_timeout):
                storage_mounts_metadata = (
                    global_user_state.get_cluster_storage_mounts_metadata(
                        cluster_name))
        except locks.LockTimeout as e:
            raise RuntimeError(
                f'Failed to retrieve metadata for cluster {cluster_name!r} '
                'due to a timeout when trying to access local database. '
                f'Please try again or manually remove the lock at {lock_id}.'
                f' {common_utils.format_exception(e)}') from None

        if storage_mounts_metadata is None:
            return None
        storage_mounts = {}
        for dst, storage_metadata in storage_mounts_metadata.items():
            # Setting 'sync_on_reconstruction' to False prevents from Storage
            # object creation to sync local source syncing to the bucket. Local
            # source specified in Storage object is synced to the bucket only
            # when it is created with 'sky launch'.
            storage_mounts[dst] = storage_lib.Storage.from_metadata(
                storage_metadata, sync_on_reconstruction=False)
        return storage_mounts

    def _skypilot_predefined_env_vars(
            self, handle: CloudVmRayResourceHandle) -> dict[str, str]:
        """Returns the SkyPilot predefined environment variables.

        TODO(zhwu): Check if a single variable for all the cluster info is more
        desirable or separate variables for each piece of info.
        NOTE: In order to avoid complication in a potential future separation
        of the info into multiple env vars, we should not treat this json format
        as a sink for all the cluster info.
        """
        return {
            'SKYPILOT_CLUSTER_INFO': json.dumps({
                'cluster_name': handle.cluster_name,
                'cloud': str(handle.launched_resources.cloud),
                'region': handle.launched_resources.region,
                'zone': handle.launched_resources.zone,
            }),
            constants.USER_ENV_VAR: common_utils.get_current_user_name(),
        }

    def _get_task_env_vars(self, task: task_lib.Task, job_id: int,
                           handle: CloudVmRayResourceHandle) -> dict[str, str]:
        """Returns the environment variables for the task."""
        env_vars = task_lib.get_plaintext_envs_and_secrets(
            task.envs_and_secrets)
        # If it is a managed job, the TASK_ID_ENV_VAR will have been already set
        # by the controller.
        if constants.TASK_ID_ENV_VAR not in env_vars:
            env_vars[
                constants.TASK_ID_ENV_VAR] = common_utils.get_global_job_id(
                    self.run_timestamp,
                    cluster_name=handle.cluster_name,
                    job_id=str(job_id))
        env_vars.update(self._skypilot_predefined_env_vars(handle))
        return env_vars

    def _get_managed_job_user_id(self, task: task_lib.Task) -> str | None:
        """Returns the user id for the managed job."""
        if task.managed_job_dag is not None:
            return task.envs[constants.USER_ID_ENV_VAR]
        return None

    def _get_task_codegen_class(
            self, handle: CloudVmRayResourceHandle) -> task_codegen.TaskCodeGen:
        """Returns the appropriate TaskCodeGen for the given handle."""
        if isinstance(handle.launched_resources.cloud, clouds.Slurm):
            assert (handle.cached_cluster_info
                    is not None), ('cached_cluster_info must be set')
            head_instance = handle.cached_cluster_info.get_head_instance()
            assert (head_instance is not None), (
                'Head instance not found in cached cluster info')
            slurm_job_id = head_instance.tags.get('job_id')
            assert (slurm_job_id
                    is not None), ('job_id tag not found in head instance')
            container_image = handle.launched_resources.extract_docker_image()
            container_name = None
            if container_image is not None:
                container_name = slurm_utils.pyxis_container_name(
                    handle.cluster_name_on_cloud)

            return task_codegen.SlurmCodeGen(
                slurm_job_id,
                container_name,
            )
        else:
            return task_codegen.RayCodeGen()

    def _execute_task_one_node(self, handle: CloudVmRayResourceHandle,
                               task: task_lib.Task, job_id: int,
                               remote_log_dir: str) -> None:
        # Launch the command as a Ray task.
        log_dir = os.path.join(remote_log_dir, 'tasks')

        resources_dict = backend_utils.get_task_demands_dict(task)
        internal_ips = handle.internal_ips()
        assert internal_ips is not None, 'internal_ips is not cached in handle'

        task_env_vars = self._get_task_env_vars(task, job_id, handle)

        codegen = self._get_task_codegen_class(handle)

        codegen.add_prologue(job_id)
        codegen.add_setup(
            1,
            resources_dict,
            stable_cluster_internal_ips=internal_ips,
            env_vars=task_env_vars,
            log_dir=log_dir,
            setup_cmd=self._setup_cmd,
        )

        codegen.add_task(
            1,
            bash_script=task.run,
            env_vars=task_env_vars,
            task_name=task.name,
            resources_dict=backend_utils.get_task_demands_dict(task),
            log_dir=log_dir)

        codegen.add_epilogue()

        self._exec_code_on_head(
            handle,
            codegen.build(),
            job_id,
            managed_job_dag=task.managed_job_dag,
            managed_job_user_id=self._get_managed_job_user_id(task),
            remote_log_dir=remote_log_dir)

    def _execute_task_n_nodes(self, handle: CloudVmRayResourceHandle,
                              task: task_lib.Task, job_id: int,
                              remote_log_dir: str) -> None:
        # Strategy:
        #   ray.init(...)
        #   for node:
        #     submit _run_cmd(cmd) with resource {node_i: 1}
        log_dir = os.path.join(remote_log_dir, 'tasks')
        resources_dict = backend_utils.get_task_demands_dict(task)
        internal_ips = handle.internal_ips()
        assert internal_ips is not None, 'internal_ips is not cached in handle'

        # If TPU VM Pods is used, #num_nodes should be num_nodes * num_node_ips
        num_actual_nodes = task.num_nodes * handle.num_ips_per_node
        task_env_vars = self._get_task_env_vars(task, job_id, handle)

        codegen = self._get_task_codegen_class(handle)

        codegen.add_prologue(job_id)
        codegen.add_setup(
            num_actual_nodes,
            resources_dict,
            stable_cluster_internal_ips=internal_ips,
            env_vars=task_env_vars,
            log_dir=log_dir,
            setup_cmd=self._setup_cmd,
        )

        codegen.add_task(
            num_actual_nodes,
            bash_script=task.run,
            env_vars=task_env_vars,
            task_name=task.name,
            resources_dict=backend_utils.get_task_demands_dict(task),
            log_dir=log_dir)

        codegen.add_epilogue()
        # TODO(zhanghao): Add help info for downloading logs.
        self._exec_code_on_head(
            handle,
            codegen.build(),
            job_id,
            managed_job_dag=task.managed_job_dag,
            managed_job_user_id=self._get_managed_job_user_id(task),
            remote_log_dir=remote_log_dir)
