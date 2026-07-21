"""Restart-stable managed-image consumer identity and optimizer context."""

from __future__ import annotations

from collections.abc import Iterator
import contextlib
import contextvars
import dataclasses
import hashlib
import json
import re
from typing import Any

from sky import task as task_lib
from sky.container_images import models
from sky.serve import constants as serve_constants
from sky.skylet import constants
from sky.utils import common_utils


@dataclasses.dataclass(frozen=True)
class ImageConsumerContext:
    """One logical deployment owner shared by optimization and provisioning."""

    consumer_kind: str
    consumer_owner: str
    controller_epoch: str
    controller_sequence: int | None
    allow_epoch_advance: bool
    metadata: dict[str, Any]


CLUSTER_CONTROLLER_EPOCH_KEY = 'container_image_cluster_controller_epoch'
CLUSTER_ALLOW_EPOCH_ADVANCE_KEY = 'container_image_cluster_allow_epoch_advance'
MANAGED_JOB_RECOVERY_GENERATION_KEY = (
    'container_image_managed_job_recovery_generation')

_CURRENT: contextvars.ContextVar[ImageConsumerContext |
                                 None] = (contextvars.ContextVar(
                                     'managed_image_consumer', default=None))


def _workload_attribution(
        task: task_lib.Task, cluster_name: str, workload_type: str,
        launch_context: dict[str, Any] | None) -> tuple[str, int | None]:
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


def derive(task: task_lib.Task, cluster_name: str | None, workload_type: str,
           launch_context: dict[str, Any] | None) -> ImageConsumerContext:
    """Derives an identity that survives request and controller restarts."""
    request_id = common_utils.get_current_request_id()
    stable_cluster_name = cluster_name or f'unnamed:{request_id}'
    workload_id, workload_task_id = _workload_attribution(
        task, stable_cluster_name, workload_type, launch_context)
    if workload_type in ('service', 'pool'):
        service_hash = (launch_context or {}).get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
        if (isinstance(service_hash, str) and service_hash and
                workload_task_id is not None):
            return ImageConsumerContext(
                consumer_kind='service_version',
                consumer_owner=(f'{workload_id}:incarnation:{service_hash}:'
                                f'v{workload_task_id}'),
                controller_epoch=(
                    f'service:{service_hash}:v{workload_task_id}'),
                controller_sequence=workload_task_id,
                allow_epoch_advance=False,
                metadata={
                    'workload_type': workload_type,
                    'workload_id': workload_id,
                    'workload_task_id': workload_task_id,
                    'service_hash': service_hash,
                })
    elif workload_type == 'managed_job' and workload_task_id is not None:
        managed_job_id = (task.envs or {}).get(constants.MANAGED_JOB_ID_ENV_VAR)
        if managed_job_id is not None:
            owner = f'{managed_job_id}:task:{workload_task_id}'
            recovery_generation = (launch_context or
                                   {}).get(MANAGED_JOB_RECOVERY_GENERATION_KEY,
                                           0)
            if (type(recovery_generation) is not int or
                    recovery_generation < 0):
                raise ValueError(
                    'Managed image recovery generation must be nonnegative.')
            return ImageConsumerContext(
                consumer_kind='managed_job_task',
                consumer_owner=owner,
                controller_epoch=(
                    f'managed-job:{owner}:recovery:{recovery_generation}'),
                controller_sequence=recovery_generation,
                allow_epoch_advance=recovery_generation > 0,
                metadata={
                    'workload_type': workload_type,
                    'workload_id': str(managed_job_id),
                    'workload_task_id': workload_task_id,
                    'recovery_generation': recovery_generation,
                    'request_id': request_id,
                })
    controller_epoch = (launch_context or {}).get(CLUSTER_CONTROLLER_EPOCH_KEY)
    if controller_epoch is None:
        controller_epoch = f'cluster-request:{request_id}'
    if (not isinstance(controller_epoch, str) or not controller_epoch or
            len(controller_epoch) > 1024):
        raise ValueError('Cluster image controller epoch is invalid.')
    allow_epoch_advance = bool((launch_context or
                                {}).get(CLUSTER_ALLOW_EPOCH_ADVANCE_KEY, False))
    return ImageConsumerContext(consumer_kind='cluster',
                                consumer_owner=stable_cluster_name,
                                controller_epoch=controller_epoch,
                                controller_sequence=None,
                                allow_epoch_advance=allow_epoch_advance,
                                metadata={
                                    'workload_type': 'cluster',
                                    'workload_id': stable_cluster_name,
                                    'request_id': request_id,
                                })


def scope_for_placement(context: ImageConsumerContext,
                        placement: models.Placement) -> ImageConsumerContext:
    """Scopes one Serve version to a target without multiplying by replica."""
    if (context.consumer_kind != 'service_version' or
            'target_scope' in context.metadata):
        return context
    target = {
        'provider': placement.provider.lower(),
        'region': placement.region,
        'backend': placement.backend,
        'platform': placement.platform or 'linux/amd64',
    }
    encoded = json.dumps(target, sort_keys=True, separators=(',', ':'))
    target_scope = hashlib.sha256(encoded.encode()).hexdigest()
    metadata = dict(context.metadata)
    metadata['target_scope'] = target
    return ImageConsumerContext(
        consumer_kind=context.consumer_kind,
        consumer_owner=f'{context.consumer_owner}:target:{target_scope}',
        controller_epoch=context.controller_epoch,
        controller_sequence=context.controller_sequence,
        allow_epoch_advance=False,
        metadata=metadata)


def current() -> ImageConsumerContext | None:
    return _CURRENT.get()


@contextlib.contextmanager
def use(context: ImageConsumerContext) -> Iterator[None]:
    token = _CURRENT.set(context)
    try:
        yield
    finally:
        _CURRENT.reset(token)
