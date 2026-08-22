"""Client-side Python SDK for SkyPilot.

All functions will return a future that can be awaited on with the `get` method.

Usage example:

.. code-block:: python

    request_id = sky.status()
    statuses = sky.get(request_id)

"""
from collections.abc import Iterator
import contextlib
import dataclasses
import datetime
import json
import logging
import os
import platform
import subprocess
import sys
import typing
from typing import Any, Literal, Optional, TypeVar, Union
from urllib import parse as urlparse
import uuid

import click
import colorama
import filelock

from sky import admin_policy
from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.client import common as client_common
from sky.client import request_results
from sky.client.api_auth import api_login
from sky.client.api_auth import api_logout
from sky.events import api_models as event_api_models
from sky.schemas.api import responses
from sky.serve import constants as serve_constants
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import rest
from sky.server import versions
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.skylet import autostop_lib
from sky.skylet import constants
from sky.ssh_node_pools import utils as ssh_utils
from sky.usage import usage_lib
from sky.utils import admin_policy_utils
from sky.utils import annotations
from sky.utils import cluster_utils
from sky.utils import common
from sky.utils import common_utils
from sky.utils import context as sky_context
from sky.utils import dag_utils
from sky.utils import debug_dump_helpers
from sky.utils import env_options
from sky.utils import hooks_deprecation
from sky.utils import infra_utils
from sky.utils import rich_utils
from sky.utils import status_lib
from sky.utils import subprocess_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import io

    import psutil
    import requests

    import sky
    from sky import backends
    from sky import catalog
    from sky import models
    from sky.events import client as events_client
    from sky.provision.kubernetes import utils as kubernetes_utils
    from sky.serve import reserved_capacity
    from sky.skylet import job_lib
else:
    requests = adaptors_common.LazyImport('requests')
    events_client = adaptors_common.LazyImport('sky.events.client')
    reserved_capacity = adaptors_common.LazyImport(
        'sky.serve.reserved_capacity')
    # only used in api_stop()
    psutil = adaptors_common.LazyImport('psutil')

logger = sky_logging.init_logger(__name__)
logging.getLogger('httpx').setLevel(logging.CRITICAL)

_LINE_PROCESSED_KEY = 'line_processed'

T = TypeVar('T')


def reload_config() -> None:
    """Reloads the client-side config."""
    skypilot_config.safe_reload_config()


# The overloads are not comprehensive - e.g. get_result Literal[False] could be
# specified to return None. We can add more overloads if needed. To do that see
# https://github.com/python/mypy/issues/8634#issuecomment-609411104
@typing.overload
def stream_response(request_id: None,
                    response: 'requests.Response',
                    output_stream: Optional['io.TextIOBase'] = None,
                    resumable: bool = False,
                    get_result: bool = True,
                    relay_rich_status: bool = False) -> None:
    ...


@typing.overload
def stream_response(request_id: server_common.RequestId[T],
                    response: 'requests.Response',
                    output_stream: Optional['io.TextIOBase'] = None,
                    resumable: bool = False,
                    get_result: Literal[True] = True,
                    relay_rich_status: bool = False) -> T:
    ...


@typing.overload
def stream_response(request_id: server_common.RequestId[T],
                    response: 'requests.Response',
                    output_stream: Optional['io.TextIOBase'] = None,
                    resumable: bool = False,
                    get_result: bool = True,
                    relay_rich_status: bool = False) -> T | None:
    ...


def stream_response(request_id: server_common.RequestId[T] | None,
                    response: 'requests.Response',
                    output_stream: Optional['io.TextIOBase'] = None,
                    resumable: bool = False,
                    get_result: bool = True,
                    relay_rich_status: bool = False) -> T | None:
    """Streams the response to the console.

    Args:
        request_id: The request ID of the request to stream. May be a full
            request ID or a prefix.
            If None, the latest request submitted to the API server is streamed.
            Using None request_id is not recommended in multi-user environments.
        response: The HTTP response.
        output_stream: The output stream to write to. If None, print to the
            console.
        resumable: Whether the response is resumable on retry. If True, the
            streaming will start from the previous failure point on retry.
        get_result: Whether to get the result of the request. This will
            typically be set to False for `--no-follow` flags as requests may
            continue to run for long periods of time without further streaming.
        relay_rich_status: If True, forward encoded rich-status control payloads
            verbatim to the output instead of rendering a local spinner. See
            :func:`sky.utils.rich_utils.decode_rich_status`.
    """

    return request_results.stream_response(request_id,
                                           response,
                                           output_stream,
                                           resumable,
                                           get_result,
                                           relay_rich_status,
                                           get_request_result=get,
                                           logger=logger)


def _check_container_image_api_support(
    dag: 'sky.Dag',
    remote_api_version: int | None = None,
) -> None:
    """Fails clearly before serializing container_image to an old server."""
    if remote_api_version is None:
        remote_api_version = versions.get_remote_api_version()
    if (remote_api_version is None or remote_api_version
            >= server_constants.MIN_CONTAINER_IMAGES_API_VERSION):
        return
    if any(resource.container_image is not None and
           not getattr(resource, 'container_image_from_legacy_image_id', False)
           for task in dag.tasks
           for resource in task.resources):
        with ux_utils.print_exception_no_traceback():
            raise exceptions.APINotSupportedError(
                'resources.container_image requires API server version '
                f'{server_constants.MIN_CONTAINER_IMAGES_API_VERSION} or '
                'newer. Please upgrade the remote server.')


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def check(
    infra_list: tuple[str, ...] | None,
    verbose: bool,
    workspace: str | None = None
) -> server_common.RequestId[dict[str, dict[str, list[str]]]]:
    """Checks the credentials to enable clouds.

    Args:
        infra: The infra to check.
        verbose: Whether to show verbose output.
        workspace: The workspace to check. If None, all workspaces will be
        checked.

    Returns:
        The request ID of the check request.

    Request Returns:
        Dict mapping workspace name to a dict of cloud name to list of
        enabled capability strings (e.g. 'compute', 'storage').
    """
    if infra_list is None:
        clouds = None
    else:
        specified_clouds = []
        for infra_str in infra_list:
            infra = infra_utils.InfraInfo.from_str(infra_str)
            if infra.cloud is None:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(f'Invalid infra to check: {infra_str}')
            if infra.region is not None or infra.zone is not None:
                region_zone = infra_str.partition('/')[-1]
                logger.warning(f'Infra {infra_str} is specified, but `check` '
                               f'only supports checking {infra.cloud}, '
                               f'ignoring {region_zone}')
            specified_clouds.append(infra.cloud)
        clouds = tuple(specified_clouds)
    body = payloads.CheckBody(clouds=clouds,
                              verbose=verbose,
                              workspace=workspace)
    response = server_common.make_authenticated_request(
        'POST', '/check', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def enabled_clouds(workspace: str | None = None,
                   expand: bool = False) -> server_common.RequestId[list[str]]:
    """Gets the enabled clouds.

    Args:
        workspace: The workspace to get the enabled clouds for. If None, the
        active workspace will be used.
        expand: Whether to expand Kubernetes and SSH to list of resource pools.

    Returns:
        The request ID of the enabled clouds request.

    Request Returns:
        A list of enabled clouds in string format.
    """
    # Only stamp an explicit workspace into the request when the user
    # actually configured one (thread-local, project `.sky.yaml`, or
    # user `~/.sky/config.yaml`). Falling back to the literal 'default'
    # here would be sent on the wire as an explicit intent — the
    # server-side workspace resolver gate (c) respects explicit names
    # and refuses to substitute a workspace the user has access to.
    # When `workspace is None`, let the server resolver run and pick
    # based on the user's accessible workspaces / preferred default.
    if workspace is None and skypilot_config.is_active_workspace_set():
        workspace = skypilot_config.get_active_workspace()
    if workspace is None:
        url = f'/enabled_clouds?expand={expand}'
    else:
        url = f'/enabled_clouds?workspace={workspace}&expand={expand}'
    response = server_common.make_authenticated_request('GET', url)
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def list_accelerators(
    gpus_only: bool = True,
    name_filter: str | None = None,
    region_filter: str | None = None,
    quantity_filter: int | None = None,
    clouds: list[str] | str | None = None,
    all_regions: bool = False,
    require_price: bool = True,
    case_sensitive: bool = True
) -> server_common.RequestId[dict[str,
                                  list['catalog.common.InstanceTypeInfo']]]:
    """Lists the names of all accelerators offered by Sky.

    This will include all accelerators offered by Sky, including those
    that may not be available in the user's account.

    Args:
        gpus_only: Whether to only list GPU accelerators.
        name_filter: The name filter.
        region_filter: The region filter.
        quantity_filter: The quantity filter.
        clouds: The clouds to list.
        all_regions: Whether to list all regions.
        require_price: Whether to require price.
        case_sensitive: Whether to case sensitive.

    Returns:
        The request ID of the list accelerator counts request.

    Request Returns:
        acc_to_instance_type_dict (Dict[str, List[InstanceTypeInfo]]): A
            dictionary of canonical accelerator names mapped to a list of
            instance type offerings. See usage in cli.py.
    """
    body = payloads.ListAcceleratorsBody(
        gpus_only=gpus_only,
        name_filter=name_filter,
        region_filter=region_filter,
        quantity_filter=quantity_filter,
        clouds=clouds,
        all_regions=all_regions,
        require_price=require_price,
        case_sensitive=case_sensitive,
    )
    response = server_common.make_authenticated_request(
        'POST', '/list_accelerators', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def list_accelerator_counts(
    gpus_only: bool = True,
    name_filter: str | None = None,
    region_filter: str | None = None,
    quantity_filter: int | None = None,
    clouds: list[str] | str | None = None
) -> server_common.RequestId[dict[str, list[float]]]:
    """Lists all accelerators offered by Sky and available counts.

    Args:
        gpus_only: Whether to only list GPU accelerators.
        name_filter: The name filter.
        region_filter: The region filter.
        quantity_filter: The quantity filter.
        clouds: The clouds to list.

    Returns:
        The request ID of the list accelerator counts request.

    Request Returns:
        acc_to_acc_num_dict (Dict[str, List[int]]): A dictionary of canonical
            accelerator names mapped to a list of available counts. See usage
            in cli.py.
    """
    body = payloads.ListAcceleratorCountsBody(
        gpus_only=gpus_only,
        name_filter=name_filter,
        region_filter=region_filter,
        quantity_filter=quantity_filter,
        clouds=clouds,
    )
    response = server_common.make_authenticated_request(
        'POST',
        '/list_accelerator_counts',
        json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(34)
@annotations.client_api
def kubernetes_label_gpus(
    context: str | None = None,
    cleanup_only: bool = False,
    wait_for_completion: bool = True,
) -> server_common.RequestId[dict[str, Any]]:
    """Labels GPU nodes in a Kubernetes cluster for use with SkyPilot.

    Note: Currently only supports NVIDIA GPUs. AMD GPUs must be labeled
    manually.

    Args:
        context: Kubernetes context to use. If None, uses current context.
        cleanup_only: If True, only cleanup existing labeling resources.
        wait_for_completion: If True, wait for labeling jobs to complete.

    Returns:
        RequestId for the labeling operation.
    """
    body = payloads.KubernetesLabelGpusBody(
        context=context,
        cleanup_only=cleanup_only,
        wait_for_completion=wait_for_completion,
    )
    response = server_common.make_authenticated_request(
        'POST',
        '/kubernetes_label_gpus',
        json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def optimize(
    dag: 'sky.Dag',
    minimize: common.OptimizeTarget = common.OptimizeTarget.COST,
    admin_policy_request_options: admin_policy.RequestOptions | None = None
) -> server_common.RequestId['sky.Dag']:
    """Finds the best execution plan for the given DAG.

    Args:
        dag: the DAG to optimize.
        minimize: whether to minimize cost or time.
        admin_policy_request_options: Request options used for admin policy
            validation. This is only required when a admin policy is in use,
            see: https://docs.skypilot.co/en/latest/cloud-setup/policy.html

    Returns:
        The request ID of the optimize request.

    Request Returns:
        optimized_dag (str): The optimized DAG in YAML format.

    Request Raises:
        exceptions.ResourcesUnavailableError: if no resources are available
            for a task.
        exceptions.NoCloudAccessError: if no public clouds are enabled.
    """
    _check_container_image_api_support(dag)
    dag_str = dag_utils.dump_dag_to_yaml_str(dag)

    body = payloads.OptimizeBody(dag=dag_str,
                                 minimize=minimize,
                                 request_options=admin_policy_request_options)
    response = server_common.make_authenticated_request(
        'POST', '/optimize', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


def workspaces() -> server_common.RequestId[dict[str, Any]]:
    """Gets the workspaces."""
    response = server_common.make_authenticated_request('GET', '/workspaces')
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(
    server_constants.MIN_PREFERRED_WORKSPACE_API_VERSION)
@annotations.client_api
def set_preferred_workspace(preferred: str | None) -> dict[str, Any]:
    """Sets (or clears with None) the user's preferred workspace.

    Args:
        preferred: workspace name to set as default, or None to clear.

    Returns:
        ``{'preferred': <new value>}`` echoing what was set. Callers that
        need the resolved workspace + accessible list should follow up
        with :func:`get_user_workspace`. Raises if the server rejects
        the change (workspace does not exist, or user lacks permission
        to it).
    """
    response = server_common.make_authenticated_request(
        'POST', '/users/me/workspace', json={'preferred': preferred})
    response.raise_for_status()
    return response.json()


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(
    server_constants.MIN_PREFERRED_WORKSPACE_API_VERSION)
@annotations.client_api
def get_user_workspace(requested: str | None = None) -> dict[str, Any]:
    """Returns workspace state for the calling user.

    Mirrors the launch-path precedence — if the caller has an explicit
    ``active_workspace``, the server returns that with ``source='explicit'``;
    otherwise the resolver runs (preferred / default-fallback /
    single-membership).

    Args:
        requested: explicit active workspace to ask about. ``None`` (the
            default) — the SDK reads your locally-configured
            ``active_workspace`` (the value `skypilot_config` merges
            from ``~/.sky/config.yaml`` + ``./.sky.yaml`` + any
            ``--config active_workspace=X`` override) and forwards it
            on the wire as ``?requested=``. Pass a non-None value to
            query the resolver as if ``active_workspace`` were that
            value, without changing your local config — useful for
            previewing "what would land if I switched to X".

    Returns:
        ``{workspace, source, note, preferred, accessible}``.

        * ``workspace``: the workspace the launch path would pick. Can
          be ``None`` when the resolver couldn't pick (no access /
          ambiguous / explicit ``requested`` rejected by RBAC); the
          reason is then in ``note``.
        * ``source``: one of ``WORKSPACE_SOURCE_*`` on success, ``None``
          when ``workspace`` is ``None``.
        * ``note``: optional message — drift on success
          (``preferred 'team-x' not accessible``) or the resolver error
          when ``workspace`` is ``None``.
        * ``preferred``: the persisted preferred workspace (``None`` if
          unset).
        * ``accessible``: sorted list of workspaces the user can launch
          into.
    """
    # Same fallback the launch path uses: only stamp `requested` when
    # the user actually set `active_workspace` somewhere. Sending the
    # default 'default' literal as `requested` would change the
    # resolver's precedence and reject users without 'default' access.
    if requested is None and skypilot_config.is_active_workspace_set():
        requested = skypilot_config.get_active_workspace()
    url = '/users/me/workspace'
    if requested is not None:
        url += f'?requested={urlparse.quote(requested)}'
    response = server_common.make_authenticated_request('GET', url)
    response.raise_for_status()
    return response.json()


def _raise_exception_object_on_client(e: BaseException) -> None:
    """Raise the exception object on the client."""
    if env_options.Options.SHOW_DEBUG_INFO.get():
        stacktrace = getattr(e, 'stacktrace', str(e))
        logger.error('=== Traceback on SkyPilot API Server ===\n'
                     f'{stacktrace}')
    with ux_utils.print_exception_no_traceback():
        raise e


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def validate(
    dag: 'sky.Dag',
    workdir_only: bool = False,
    admin_policy_request_options: admin_policy.RequestOptions | None = None
) -> None:
    """Validates the tasks.

    The file paths (workdir and file_mounts) are validated on the client side
    while the rest (e.g. resource) are validated on server side.

    Raises exceptions if the DAG is invalid.

    Args:
        dag: the DAG to validate.
        workdir_only: whether to only validate the workdir. This is used for
            `exec` as it does not need other files/folders in file_mounts.
        admin_policy_request_options: Request options used for admin policy
            validation. This is only required when a admin policy is in use,
            see: https://docs.skypilot.co/en/latest/cloud-setup/policy.html
    """
    remote_api_version = versions.get_remote_api_version()
    _validate_dag_locally(dag,
                          workdir_only=workdir_only,
                          remote_api_version=remote_api_version)

    dag_str = dag_utils.dump_dag_to_yaml_str(dag)
    body = payloads.ValidateBody(dag=dag_str,
                                 request_options=admin_policy_request_options)
    response = server_common.make_authenticated_request(
        'POST', '/validate', json=json.loads(body.model_dump_json()))
    if response.status_code == 400:
        _raise_exception_object_on_client(
            exceptions.deserialize_exception(response.json().get('detail')))


def _validate_dag_locally(
    dag: 'sky.Dag',
    *,
    workdir_only: bool,
    remote_api_version: int | None,
) -> None:
    """Apply client-side validation/version projection without HTTP."""
    _check_container_image_api_support(dag,
                                       remote_api_version=remote_api_version)

    def _omit(version: int) -> bool:
        return remote_api_version is None or remote_api_version < version

    # TODO(kevin): remove this in v0.13.0
    omit_user_specified_yaml = _omit(15)
    # TODO (kyuds): remove these in v0.13.0
    omit_local_disk = _omit(35)
    omit_mount_cached_config = _omit(37)
    omit_file_mount_type = _omit(40)
    omit_priority_class = _omit(43)
    omit_max_hourly_cost = _omit(44)
    omit_mount_config = _omit(48)

    for task in dag.tasks:
        if omit_user_specified_yaml:
            # pylint: disable=protected-access
            task._user_specified_yaml = None
        task.expand_and_validate_workdir()
        if not workdir_only:
            task.expand_and_validate_file_mounts()
        if omit_local_disk:
            for resource in task.resources:
                # pylint: disable=protected-access
                resource._set_local_disk(None)
            logger.debug('`local_disk` is ignored because the server does '
                         'not support it yet.')
        if omit_mount_cached_config:
            for storage in task.storage_mounts.values():
                storage.mount_cached_config = None
            logger.debug('`mount_cached_config` is ignored because the server '
                         'does not support it yet.')
        if omit_file_mount_type:
            for storage in task.storage_mounts.values():
                storage.file_mount_type = None
            logger.debug('`type` is ignored because the server does not '
                         'support it yet.')
        if omit_mount_config:
            for storage in task.storage_mounts.values():
                storage.mount_config = None
            logger.debug('`mount_config` is ignored because the server '
                         'does not support it yet.')
        if omit_priority_class:
            for resource in task.resources:
                if resource.priority_class:
                    # pylint: disable=protected-access
                    resource._set_priority_class(None)
            logger.debug('`priority_class` is ignored because the server '
                         'does not support it yet.')
        if omit_max_hourly_cost:
            for resource in task.resources:
                # pylint: disable=protected-access
                resource._max_hourly_cost = None
            logger.debug('`max_hourly_cost` is ignored because the server '
                         'does not support it yet.')


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def dashboard(starting_page: str | None = None) -> None:
    """Starts the dashboard for SkyPilot."""
    api_server_url = server_common.get_server_url()
    url = server_common.get_dashboard_url(api_server_url,
                                          starting_page=starting_page)
    logger.info(f'Opening dashboard in browser: {url}')
    common_utils.open_browser(url)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@sky_context.contextual
def launch(
    task: Union['sky.Task', 'sky.Dag'],
    cluster_name: str | None = None,
    retry_until_up: bool = False,
    idle_minutes_to_autostop: int | None = None,
    wait_for: autostop_lib.AutostopWaitFor | None = None,
    dryrun: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    backend: Optional['backends.Backend'] = None,
    optimize_target: common.OptimizeTarget = common.OptimizeTarget.COST,
    no_setup: bool = False,
    clone_disk_from: str | None = None,
    fast: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False,
    _is_launched_by_jobs_controller: bool = False,
    _is_launched_by_sky_serve_controller: bool = False,
    _disable_controller_check: bool = False,
    _file_mounts_blob_id: str | None = None,
    _extra_launch_context: dict[str, Any] | None = None,
    _include_credentials: bool = False,
) -> server_common.RequestId[tuple[int | None,
                                   Optional['backends.ResourceHandle']]]:
    """Launches a cluster or task.

    The task's setup and run commands are executed under the task's workdir
    (when specified, it is synced to remote cluster).  The task undergoes job
    queue scheduling on the cluster.

    Currently, the first argument must be a sky.Task, or (EXPERIMENTAL advanced
    usage) a sky.Dag. In the latter case, currently it must contain a single
    task; support for pipelines/general DAGs are in experimental branches.

    Example:
        .. code-block:: python

            import sky
            task = sky.Task(run='echo hello SkyPilot')
            task.set_resources(
                sky.Resources(infra='aws', accelerators='V100:4'))
            sky.launch(task, cluster_name='my-cluster')


    Args:
        task: sky.Task, or sky.Dag (experimental; 1-task only) to launch.
        cluster_name: name of the cluster to create/reuse.  If None,
          auto-generate a name.
        retry_until_up: whether to retry launching the cluster until it is
          up.
        idle_minutes_to_autostop: automatically stop the cluster after this
            many minute of idleness, i.e., no running or pending jobs in the
            cluster's job queue. Idleness gets reset whenever setting-up/
            running/pending jobs are found in the job queue. Setting this
            flag is equivalent to running
            ``sky.launch(...)`` and then
            ``sky.autostop(idle_minutes=<minutes>)``. If set, the autostop
            config specified in the task' resources will be overridden by
            this parameter.
        wait_for: determines the condition for resetting the idleness timer.
            This option works in conjunction with ``idle_minutes_to_autostop``.
            Choices:

            1. "jobs_and_ssh" (default) - Wait for in-progress jobs and SSH
               connections to finish.
            2. "jobs" - Only wait for in-progress jobs.
            3. "none" - Wait for nothing; autostop right after
               ``idle_minutes_to_autostop``.
        dryrun: if True, do not actually launch the cluster.
        down: Tear down the cluster after all jobs finish (successfully or
            abnormally). If --idle-minutes-to-autostop is also set, the
            cluster will be torn down after the specified idle time.
            Note that if errors occur during provisioning/data syncing/setting
            up, the cluster will not be torn down for debugging purposes. If
            set, the autostop config specified in the task' resources will be
            overridden by this parameter.
        backend: backend to use.  If None, use the default backend
          (CloudVMRayBackend).
        optimize_target: target to optimize for. Choices: OptimizeTarget.COST,
          OptimizeTarget.TIME.
        no_setup: if True, do not re-run setup commands.
        clone_disk_from: [Experimental] if set, clone the disk from the
          specified cluster. This is useful to migrate the cluster to a
          different availability zone or region.
        fast: [Experimental] If the cluster is already up and available,
          skip provisioning and setup steps.
        _need_confirmation: (Internal only) If True, show the confirmation
            prompt.

    Returns:
        The request ID of the launch request.

    Request Returns:
        job_id (Optional[int]): the job ID of the submitted job. None if the
          backend is not ``CloudVmRayBackend``, or no job is submitted to the
          cluster.
        handle (Optional[backends.ResourceHandle]): the handle to the cluster.
          None if dryrun.

    Request Raises:
        exceptions.ClusterOwnerIdentityMismatchError: if the cluster is owned
          by another user.
        exceptions.InvalidClusterNameError: if the cluster name is invalid.
        exceptions.ResourcesMismatchError: if the requested resources
          do not match the existing cluster.
        exceptions.NotSupportedError: if required features are not supported
          by the backend/cloud/cluster.
        exceptions.ResourcesUnavailableError: if the requested resources
          cannot be satisfied. The failover_history of the exception will be set
          as:

          1. Empty: iff the first-ever sky.optimize() fails to find a feasible
             resource; no pre-check or actual launch is attempted.

          2. Non-empty: iff at least 1 exception from either our pre-checks
             (e.g., cluster name invalid) or a region/zone throwing resource
             unavailability.

        exceptions.CommandError: any ssh command error.
        exceptions.NoCloudAccessError: if all clouds are disabled.

    Other exceptions may be raised depending on the backend.
    """
    with _prepared_launch_request_in_current_context(
            task=task,
            cluster_name=cluster_name,
            retry_until_up=retry_until_up,
            idle_minutes_to_autostop=idle_minutes_to_autostop,
            wait_for=wait_for,
            dryrun=dryrun,
            down=down,
            backend=backend,
            optimize_target=optimize_target,
            no_setup=no_setup,
            clone_disk_from=clone_disk_from,
            fast=fast,
            _need_confirmation=_need_confirmation,
            _is_launched_by_jobs_controller=_is_launched_by_jobs_controller,
            _is_launched_by_sky_serve_controller=(
                _is_launched_by_sky_serve_controller),
            _disable_controller_check=_disable_controller_check,
            _file_mounts_blob_id=_file_mounts_blob_id,
            _extra_launch_context=_extra_launch_context,
            _include_credentials=_include_credentials) as prepared_request:
        return submit_prepared_launch_request(prepared_request)


@dataclasses.dataclass(frozen=True)
class PreparedLaunchRequest:
    """Internal, replay-stable snapshot of one launch request payload.

    The immutable canonical bytes are the sole authority.  ``body`` returns a
    fresh ``LaunchBody`` for typed inspection, so mutating that view or the
    source task cannot alter a later inspection or submission.
    """

    submitted_bytes: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.submitted_bytes, bytes):
            raise TypeError('Launch request canonical payload must be bytes.')
        try:
            submitted_json = self.submitted_bytes.decode('utf-8')
            submitted_payload = json.loads(submitted_json)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                'Launch request payload must be valid UTF-8 JSON.') from exc
        if not isinstance(submitted_payload, dict):
            raise ValueError('Launch request payload must be a JSON object.')
        canonical_bytes = _canonical_launch_json_bytes(submitted_payload)
        if canonical_bytes != self.submitted_bytes:
            raise ValueError('Launch request payload is not canonical JSON.')
        try:
            body = payloads.LaunchBody.model_validate_json(self.submitted_bytes)
        except ValueError as exc:
            raise ValueError(
                'Launch request payload is not a LaunchBody.') from exc
        if _canonical_launch_body_bytes(body) != self.submitted_bytes:
            raise ValueError('Launch request payload does not exactly match '
                             'its LaunchBody representation.')

    @property
    def body(self) -> payloads.LaunchBody:
        """Returns a fresh typed view of the committed request bytes."""
        return payloads.LaunchBody.model_validate_json(self.submitted_bytes)

    @property
    def submitted_json(self) -> str:
        """Returns the canonical UTF-8 JSON text committed for submission."""
        return self.submitted_bytes.decode('utf-8')


def _canonical_launch_json_bytes(value: Any) -> bytes:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def _canonical_launch_body_bytes(body: payloads.LaunchBody) -> bytes:
    return _canonical_launch_json_bytes(json.loads(body.model_dump_json()))


@contextlib.contextmanager
def _prepared_launch_request_in_current_context(
    task: Union['sky.Task', 'sky.Dag'],
    cluster_name: str | None = None,
    retry_until_up: bool = False,
    idle_minutes_to_autostop: int | None = None,
    wait_for: autostop_lib.AutostopWaitFor | None = None,
    dryrun: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    backend: Optional['backends.Backend'] = None,
    optimize_target: common.OptimizeTarget = common.OptimizeTarget.COST,
    no_setup: bool = False,
    clone_disk_from: str | None = None,
    fast: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False,
    _is_launched_by_jobs_controller: bool = False,
    _is_launched_by_sky_serve_controller: bool = False,
    _disable_controller_check: bool = False,
    _file_mounts_blob_id: str | None = None,
    _extra_launch_context: dict[str, Any] | None = None,
    _include_credentials: bool = False,
    _target_api_version: int | None = None,
    _server_side_only: bool = False,
) -> Iterator[PreparedLaunchRequest]:
    """Yields a frozen launch while its preparation context remains active.

    Public launch submission must occur inside this context because the client
    admin policy may temporarily override transport configuration. A standalone
    preparer may retain the yielded immutable request after the context exits.
    """
    if cluster_name is None:
        cluster_name = cluster_utils.generate_cluster_name()

    if clone_disk_from is not None:
        with ux_utils.print_exception_no_traceback():
            raise NotImplementedError('clone_disk_from is not implemented yet. '
                                      'Please contact the SkyPilot team if you '
                                      'need this feature at slack.skypilot.co.')

    remote_api_version = (_target_api_version if _target_api_version is not None
                          else versions.get_remote_api_version())
    if wait_for is not None and (remote_api_version is None or
                                 remote_api_version < 13):
        logger.warning('wait_for is not supported in your API server. '
                       'Please upgrade to a newer API server to use it.')

    dag = dag_utils.convert_entrypoint_to_dag(task)
    launch_context = _extra_launch_context or {}
    has_reserved_fill_context = any(
        isinstance(key, str) and
        key.startswith(serve_constants.RESERVED_FILL_LAUNCH_FENCE_PREFIX)
        for key in launch_context)
    if _server_side_only and has_reserved_fill_context:
        try:
            reserved_fill_fence = (
                reserved_capacity.parse_protocol_v2_launch_fence(launch_context)
            )
        except ValueError as error:
            raise exceptions.ReservedFillLaunchFenceError(
                'Server-controller launch has a malformed reserved-fill '
                'fence.') from error
        if reserved_fill_fence is not None:
            # The v2 execution capsule binds the controller and executor to
            # the policy-absent mode. This check happens before request bytes
            # are frozen and hashed.
            reserved_capacity.require_protocol_v2_admin_policy_absent()
    # Override the autostop config from command line flags to task YAML.
    for dag_task in dag.tasks:
        for resource in dag_task.resources:
            if remote_api_version is None or remote_api_version < 13:
                # An older server would not recognize the wait_for field
                # in the schema, so we need to omit it.
                resource.override_autostop_config(
                    down=down, idle_minutes=idle_minutes_to_autostop)
            else:
                resource.override_autostop_config(
                    down=down,
                    idle_minutes=idle_minutes_to_autostop,
                    wait_for=wait_for)
            if resource.autostop_config is not None:
                # For backward-compatibility, get the final autostop config for
                # admin policy.
                # TODO(aylei): remove this after 0.12.0
                down = resource.autostop_config.down
                idle_minutes_to_autostop = resource.autostop_config.idle_minutes

    request_options = admin_policy.RequestOptions(
        cluster_name=cluster_name,
        idle_minutes_to_autostop=idle_minutes_to_autostop,
        down=down,
        dryrun=dryrun)
    policy_context = (
        contextlib.nullcontext(dag) if _server_side_only else
        admin_policy_utils.apply_and_use_config_in_current_request(
            dag,
            request_name=(request_names.AdminPolicyRequestName.CLUSTER_LAUNCH),
            request_options=request_options,
            at_client_side=True))
    # Public submission keeps policy transport overrides active through yield.
    # pylint: disable-next=contextmanager-generator-missing-cleanup
    with policy_context as dag:
        prepared_request = _freeze_launch_request(
            dag,
            cluster_name,
            request_options,
            retry_until_up,
            idle_minutes_to_autostop,
            dryrun,
            down,
            backend,
            optimize_target,
            no_setup,
            clone_disk_from,
            fast,
            _need_confirmation,
            _is_launched_by_jobs_controller,
            _is_launched_by_sky_serve_controller,
            _disable_controller_check,
            _file_mounts_blob_id,
            _extra_launch_context,
            _include_credentials,
            _target_api_version,
            _server_side_only,
        )
        yield prepared_request


@usage_lib.entrypoint('sky.client.sdk.launch')
@server_common.check_server_healthy_or_start
@annotations.client_api
@sky_context.contextual
def prepare_launch_request(
    task: Union['sky.Task', 'sky.Dag'],
    cluster_name: str | None = None,
    retry_until_up: bool = False,
    idle_minutes_to_autostop: int | None = None,
    wait_for: autostop_lib.AutostopWaitFor | None = None,
    dryrun: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    backend: Optional['backends.Backend'] = None,
    optimize_target: common.OptimizeTarget = common.OptimizeTarget.COST,
    no_setup: bool = False,
    clone_disk_from: str | None = None,
    fast: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False,
    _is_launched_by_jobs_controller: bool = False,
    _is_launched_by_sky_serve_controller: bool = False,
    _disable_controller_check: bool = False,
    _file_mounts_blob_id: str | None = None,
    _extra_launch_context: dict[str, Any] | None = None,
    _include_credentials: bool = False,
) -> PreparedLaunchRequest:
    """Prepares a launch for inspection and later exact submission.

    This is the non-submitting counterpart of ``launch``. It preserves the
    same client health, usage, and context boundaries while returning the
    immutable request produced by the shared launch-preparation pipeline.
    """
    with _prepared_launch_request_in_current_context(
            task=task,
            cluster_name=cluster_name,
            retry_until_up=retry_until_up,
            idle_minutes_to_autostop=idle_minutes_to_autostop,
            wait_for=wait_for,
            dryrun=dryrun,
            down=down,
            backend=backend,
            optimize_target=optimize_target,
            no_setup=no_setup,
            clone_disk_from=clone_disk_from,
            fast=fast,
            _need_confirmation=_need_confirmation,
            _is_launched_by_jobs_controller=_is_launched_by_jobs_controller,
            _is_launched_by_sky_serve_controller=(
                _is_launched_by_sky_serve_controller),
            _disable_controller_check=_disable_controller_check,
            _file_mounts_blob_id=_file_mounts_blob_id,
            _extra_launch_context=_extra_launch_context,
            _include_credentials=_include_credentials) as prepared_request:
        return prepared_request


def prepare_launch_request_for_server_controller(
    task: Union['sky.Task', 'sky.Dag'],
    cluster_name: str,
    *,
    workspace: str,
    retry_until_up: bool = False,
    backend: Optional['backends.Backend'] = None,
    optimize_target: common.OptimizeTarget = common.OptimizeTarget.COST,
    no_setup: bool = False,
    extra_launch_context: dict[str, Any] | None = None,
) -> PreparedLaunchRequest:
    """Freeze one server-local controller launch without HTTP or uploads."""
    if not isinstance(workspace, str) or not workspace:
        raise ValueError('Server controller workspace must be non-empty.')
    with skypilot_config.local_active_workspace_ctx(
            workspace), _prepared_launch_request_in_current_context(
                task=task,
                cluster_name=cluster_name,
                retry_until_up=retry_until_up,
                backend=backend,
                optimize_target=optimize_target,
                no_setup=no_setup,
                _is_launched_by_sky_serve_controller=True,
                _extra_launch_context=extra_launch_context,
                _target_api_version=server_constants.API_VERSION,
                _server_side_only=True) as prepared_request:
        launch_body = prepared_request.body
        if launch_body.override_skypilot_config_path is not None:
            raise ValueError(
                'Server controller launches cannot use a mutable config path.')
        override_config = dict(launch_body.override_skypilot_config or {})
        configured_workspace = override_config.get('active_workspace')
        if (configured_workspace is not None and
                configured_workspace != workspace):
            raise ValueError('Server controller launch workspace conflicts '
                             'with its service workspace.')
        override_config['active_workspace'] = workspace
        launch_body.override_skypilot_config = override_config
        return PreparedLaunchRequest(
            submitted_bytes=_canonical_launch_body_bytes(launch_body))


def _freeze_launch_request(
    dag: 'sky.Dag',
    cluster_name: str,
    request_options: admin_policy.RequestOptions,
    retry_until_up: bool = False,
    idle_minutes_to_autostop: int | None = None,
    dryrun: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    backend: Optional['backends.Backend'] = None,
    optimize_target: common.OptimizeTarget = common.OptimizeTarget.COST,
    no_setup: bool = False,
    clone_disk_from: str | None = None,
    fast: bool = False,
    # Internal only:
    # pylint: disable=invalid-name
    _need_confirmation: bool = False,
    _is_launched_by_jobs_controller: bool = False,
    _is_launched_by_sky_serve_controller: bool = False,
    _disable_controller_check: bool = False,
    _file_mounts_blob_id: str | None = None,
    _extra_launch_context: dict[str, Any] | None = None,
    _include_credentials: bool = False,
    _target_api_version: int | None = None,
    _server_side_only: bool = False,
) -> PreparedLaunchRequest:
    """Freezes a launch DAG after high-level policy and option preparation."""

    if _server_side_only:
        if _target_api_version is None:
            raise ValueError(
                'Server-side launch preparation requires a target API version.')
        _validate_dag_locally(dag,
                              workdir_only=False,
                              remote_api_version=_target_api_version)
    else:
        validate(dag, admin_policy_request_options=request_options)
    # The flags have been applied to the task YAML and the backward
    # compatibility of admin policy has been handled. We should no longer use
    # these flags.
    del down, idle_minutes_to_autostop

    confirm_shown = False
    if _need_confirmation:
        cluster_status = None
        # TODO(SKY-998): we should reduce RTTs before launching the cluster.
        status_request_id = status([cluster_name], all_users=True)
        clusters = get(status_request_id)
        cluster_user_hash = common_utils.get_user_hash()
        cluster_user_hash_str = ''
        current_user = common_utils.get_local_user_name()
        cluster_user_name = current_user
        if not clusters:
            # Show the optimize log before the prompt if the cluster does not
            # exist.
            optimize_request_id = optimize(
                dag, admin_policy_request_options=request_options)
            stream_and_get(optimize_request_id)
        else:
            cluster_record = clusters[0]
            cluster_status = cluster_record['status']
            cluster_user_hash = cluster_record['user_hash']
            cluster_user_name = cluster_record['user_name']
            if cluster_user_name == current_user:
                # Only show the hash if the username is the same as the local
                # username, to avoid confusion.
                cluster_user_hash_str = f' (hash: {cluster_user_hash})'

        # Prompt if (1) --cluster is None, or (2) cluster doesn't exist, or (3)
        # it exists but is STOPPED.
        prompt = None
        if cluster_status is None:
            prompt = (
                f'Launching a new cluster {cluster_name!r}. '
                # '{clone_source_str}. '
                'Proceed?')
        elif cluster_status == status_lib.ClusterStatus.STOPPED:
            user_name_str = ''
            if cluster_user_hash != common_utils.get_user_hash():
                user_name_str = (' created by another user '
                                 f'{cluster_user_name!r}'
                                 f'{cluster_user_hash_str}')
            prompt = (f'Restarting the stopped cluster {cluster_name!r}'
                      f'{user_name_str}. Proceed?')
        elif cluster_user_hash != common_utils.get_user_hash():
            # Prompt if the cluster was created by a different user.
            prompt = (f'Cluster {cluster_name!r} was created by another user '
                      f'{cluster_user_name!r}{cluster_user_hash_str}. '
                      'Reusing the cluster. Proceed?')
        if prompt is not None:
            confirm_shown = True
            click.confirm(prompt, default=True, abort=True, show_default=True)

    if not confirm_shown:
        click.secho('Running on cluster: ', fg='cyan', nl=False)
        click.secho(cluster_name)

    file_mounts_blob_id: str | None = None
    if _file_mounts_blob_id is not None:
        # Caller (e.g. job controller) has a blob for this dag's file mounts,
        # skip the re-upload.
        file_mounts_blob_id = _file_mounts_blob_id
    elif _server_side_only:
        client_common.validate_no_local_inputs(dag)
        file_mounts_blob_id = None
    else:
        dag, file_mounts_blob_id = client_common.upload_mounts_to_api_server(
            dag)

    dag_str = dag_utils.dump_dag_to_yaml_str(dag)

    # Only request credential bundling when the remote server advertises
    # support for it. Old servers ignore the field via Pydantic
    # ``extra='ignore'`` so this is also safe to send unconditionally,
    # but checking up-front lets us skip the work on servers that would
    # discard it anyway and makes the gating intent explicit.
    include_credentials = _include_credentials
    if include_credentials:
        remote_api_version = (_target_api_version
                              if _target_api_version is not None else
                              versions.get_remote_api_version())
        if (remote_api_version is None or remote_api_version
                < server_constants.MIN_LAUNCH_CREDENTIALS_API_VERSION):
            include_credentials = False

    request_context = ({
        'env_vars': {},
        'entrypoint': '',
        'entrypoint_command': '',
        'using_remote_api_server': False,
        'override_skypilot_config': {},
        'override_skypilot_config_path': None,
        'client_api_version': server_constants.API_VERSION,
    } if _server_side_only else {})
    body = payloads.LaunchBody(
        task=dag_str,
        cluster_name=cluster_name,
        retry_until_up=retry_until_up,
        dryrun=dryrun,
        backend=backend.NAME if backend else None,
        optimize_target=optimize_target,
        no_setup=no_setup,
        clone_disk_from=clone_disk_from,
        fast=fast,
        # For internal use
        quiet_optimizer=_need_confirmation,
        is_launched_by_jobs_controller=_is_launched_by_jobs_controller,
        is_launched_by_sky_serve_controller=(
            _is_launched_by_sky_serve_controller),
        disable_controller_check=_disable_controller_check,
        file_mounts_blob_id=file_mounts_blob_id,
        extra_launch_context=_extra_launch_context or {},
        include_credentials=include_credentials,
        **request_context,
    )

    # Keep the submitted representation detached from both the source Dag and
    # mutable nested values accepted by LaunchBody.  Sorting keys and removing
    # insignificant whitespace gives callers stable bytes to journal/hash,
    # while decoding those bytes below preserves the existing ``json=`` HTTP
    # request behavior.
    submitted_bytes = _canonical_launch_body_bytes(body)
    return PreparedLaunchRequest(submitted_bytes=submitted_bytes)


def submit_prepared_launch_request(
    prepared_request: PreparedLaunchRequest,
) -> server_common.RequestId[tuple[int | None,
                                   Optional['backends.ResourceHandle']]]:
    """Submits exactly one HTTP request from a prepared launch snapshot."""
    response = server_common.make_authenticated_request(
        'POST',
        '/launch',
        json=json.loads(prepared_request.submitted_bytes),
        timeout=5)
    return server_common.get_request_id(response)


def submit_prepared_ordinary_launch_request(
    prepared_request: PreparedLaunchRequest,
    submission_uuid: str | uuid.UUID,
) -> server_common.RequestId[tuple[int | None,
                                   Optional['backends.ResourceHandle']]]:
    """Submit one frozen launch through the private durable binding seam.

    The caller must reuse ``submission_uuid`` for every transport retry.  An
    old API target has no such route and returns 404 without scheduling through
    the unsafe public launch fallback.
    """
    try:
        parsed_submission_uuid = uuid.UUID(str(submission_uuid))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError('submission_uuid must be a UUID.') from error
    canonical_submission_uuid = str(parsed_submission_uuid)
    if (isinstance(submission_uuid, str) and
            submission_uuid != canonical_submission_uuid):
        raise ValueError('submission_uuid must be a canonical UUID string.')
    response = server_common.make_authenticated_request(
        'POST',
        server_constants.ORDINARY_LAUNCH_BINDING_PATH,
        json={
            'submission_uuid': canonical_submission_uuid,
            'launch': json.loads(prepared_request.submitted_bytes),
        },
        timeout=5)
    server_common.handle_request_error(response)
    try:
        binding = responses.OrdinaryLaunchBindingResponse.model_validate(
            response.json())
    except (AttributeError, TypeError, ValueError) as error:
        raise RuntimeError(
            'Ordinary Serve launch binding returned a malformed response.') \
            from error
    if str(binding.submission_uuid) != canonical_submission_uuid:
        raise RuntimeError(
            'Ordinary Serve launch binding returned a different submission '
            'UUID.')
    return server_common.RequestId[tuple[int | None,
                                         Optional['backends.ResourceHandle']]](
                                             str(binding.request_id))


def submit_prepared_non_pool_launch_request(
    prepared_request: PreparedLaunchRequest,
    submission_uuid: str | uuid.UUID,
    profile_kind: str,
) -> server_common.RequestId[tuple[int | None,
                                   Optional['backends.ResourceHandle']]]:
    """Submit one frozen request through the generic durable binding seam."""
    try:
        parsed_submission_uuid = uuid.UUID(str(submission_uuid))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError('submission_uuid must be a UUID.') from error
    canonical_submission_uuid = str(parsed_submission_uuid)
    if (isinstance(submission_uuid, str) and
            submission_uuid != canonical_submission_uuid):
        raise ValueError('submission_uuid must be a canonical UUID string.')
    if not isinstance(profile_kind, str) or not profile_kind:
        raise ValueError('profile_kind must be non-empty text.')
    response = server_common.make_authenticated_request(
        'POST',
        server_constants.NON_POOL_LAUNCH_BINDING_PATH,
        json={
            'submission_uuid': canonical_submission_uuid,
            'profile_kind': profile_kind,
            'launch': json.loads(prepared_request.submitted_bytes),
        },
        timeout=5)
    server_common.handle_request_error(response)
    try:
        binding = responses.OrdinaryLaunchBindingResponse.model_validate(
            response.json())
    except (AttributeError, TypeError, ValueError) as error:
        raise RuntimeError(
            'Non-pool Serve launch binding returned a malformed response.') \
            from error
    if str(binding.submission_uuid) != canonical_submission_uuid:
        raise RuntimeError(
            'Non-pool Serve launch binding returned a different submission '
            'UUID.')
    return server_common.RequestId[tuple[int | None,
                                         Optional['backends.ResourceHandle']]](
                                             str(binding.request_id))


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def exec(  # pylint: disable=redefined-builtin
    task: Union['sky.Task', 'sky.Dag'],
    cluster_name: str | None = None,
    dryrun: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    backend: Optional['backends.Backend'] = None,
) -> server_common.RequestId[tuple[int | None,
                                   Optional['backends.ResourceHandle']]]:
    """Executes a task on an existing cluster.

    This function performs two actions:

    (1) workdir syncing, if the task has a workdir specified;
    (2) executing the task's ``run`` commands.

    All other steps (provisioning, setup commands, file mounts syncing) are
    skipped.  If any of those specifications changed in the task, this function
    will not reflect those changes.  To ensure a cluster's setup is up to date,
    use ``sky.launch()`` instead.

    Execution and scheduling behavior:

    - The task will undergo job queue scheduling, respecting any specified
      resource requirement. It can be executed on any node of the cluster with
      enough resources.
    - The task is run under the workdir (if specified).
    - The task is run non-interactively (without a pseudo-terminal or
      pty), so interactive commands such as ``htop`` do not work.
      Use ``ssh my_cluster`` instead.

    Args:
        task: sky.Task, or sky.Dag (experimental; 1-task only) containing the
          task to execute.
        cluster_name: name of an existing cluster to execute the task.
        dryrun: if True, do not actually execute the task.
        down: Tear down the cluster after all jobs finish (successfully or
          abnormally). If --idle-minutes-to-autostop is also set, the
          cluster will be torn down after the specified idle time.
          Note that if errors occur during provisioning/data syncing/setting
          up, the cluster will not be torn down for debugging purposes.
        backend: backend to use.  If None, use the default backend
          (CloudVMRayBackend).

    Returns:
        The request ID of the exec request.


    Request Returns:
        job_id (Optional[int]): the job ID of the submitted job. None if the
          backend is not CloudVmRayBackend, or no job is submitted to
          the cluster.
        handle (Optional[backends.ResourceHandle]): the handle to the cluster.
          None if dryrun.

    Request Raises:
        ValueError: if the specified cluster is not in UP status.
        sky.exceptions.ClusterDoesNotExist: if the specified cluster does not
          exist.
        sky.exceptions.NotSupportedError: if the specified cluster is a
          controller that does not support this operation.
    """
    dag = dag_utils.convert_entrypoint_to_dag(task)
    validate(dag, workdir_only=True)
    dag, file_mounts_blob_id = client_common.upload_mounts_to_api_server(
        dag, workdir_only=True)
    dag_str = dag_utils.dump_dag_to_yaml_str(dag)
    body = payloads.ExecBody(
        task=dag_str,
        cluster_name=cluster_name,
        dryrun=dryrun,
        down=down,
        backend=backend.NAME if backend else None,
        file_mounts_blob_id=file_mounts_blob_id,
    )

    response = server_common.make_authenticated_request(
        'POST', '/exec', json=json.loads(body.model_dump_json()), timeout=5)
    return server_common.get_request_id(response)


@typing.overload
def tail_logs(
        cluster_name: str,
        job_id: int | None,
        follow: bool,
        tail: int = 0,
        output_stream: Optional['io.TextIOBase'] = None,
        *,  # keyword only separator
        preload_content: Literal[True] = True) -> int:
    ...


@typing.overload
def tail_logs(cluster_name: str,
              job_id: int | None,
              follow: bool,
              tail: int = 0,
              output_stream: None = None,
              *,
              preload_content: Literal[False]) -> Iterator[str | None]:
    ...


# TODO(aylei): when retry logs request, there will be duplicated log entries.
# We should fix this.
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@rest.retry_transient_errors()
def tail_logs(
    cluster_name: str,
    job_id: int | None,
    follow: bool,
    tail: int = 0,
    output_stream: Optional['io.TextIOBase'] = None,
    *,  # keyword only separator
    preload_content: bool = True
) -> int | Iterator[str | None]:
    """Tails the logs of a job.

    Args:
        cluster_name: name of the cluster.
        job_id: job id.
        follow: if True, follow the logs. Otherwise, return the logs
            immediately.
        tail: if > 0, tail the last N lines of the logs.
        output_stream: the stream to write the logs to. If None, print to the
            console. Cannot be used with preload_content=False.
        preload_content: if False, returns an Iterator[str | None] containing
            the logs without the function blocking on the retrieval of entire
            log. Iterator returns None when the log has been completely
            streamed. Default True. Cannot be used with output_stream.

    Returns:
        If preload_content is True:
            Exit code based on success or failure of the job. 0 if success,
            100 if the job failed. See exceptions.JobExitCode for possible exit
            codes.
        If preload_content is False:
            Iterator[str | None] containing the logs without the function
            blocking on the retrieval of entire log. Iterator returns None
            when the log has been completely streamed.

    Request Raises:
        ValueError: if arguments are invalid or the cluster is not supported.
        sky.exceptions.ClusterDoesNotExist: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
          CloudVmRayBackend.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
    """
    if output_stream is not None and not preload_content:
        raise ValueError(
            'output_stream cannot be specified when preload_content is False')

    body = payloads.ClusterJobBody(
        cluster_name=cluster_name,
        job_id=job_id,
        follow=follow,
        tail=tail,
    )
    response = server_common.make_authenticated_request(
        'POST',
        '/logs',
        json=json.loads(body.model_dump_json()),
        stream=True,
        timeout=(client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
                 None))
    request_id: server_common.RequestId[int] = server_common.get_request_id(
        response)
    if preload_content:
        # Log request is idempotent when tail is 0, thus can resume previous
        # streaming point on retry.
        return stream_response(request_id=request_id,
                               response=response,
                               output_stream=output_stream,
                               resumable=(tail == 0))
    else:
        return rich_utils.decode_rich_status(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(17)
@annotations.client_api
@rest.retry_transient_errors()
def tail_provision_logs(cluster_name: str,
                        worker: int | None = None,
                        follow: bool = True,
                        tail: int = 0,
                        output_stream: Optional['io.TextIOBase'] = None) -> int:
    """Tails the provisioning logs (provision.log) for a cluster.

    Args:
        cluster_name: name of the cluster.
        worker: worker id in multi-node cluster.
             If None, stream the logs of the head node.
        follow: follow the logs.
        tail: lines from end to tail.
        output_stream: optional stream to write logs.
    Returns:
        Exit code 0 on streaming success; raises on HTTP error.
    """
    body = payloads.ProvisionLogsBody(cluster_name=cluster_name)

    if worker is not None:
        remote_api_version = versions.get_remote_api_version()
        if remote_api_version is not None and remote_api_version >= 21:
            if worker < 1:
                raise ValueError('Worker must be a positive integer.')
            body.worker = worker
        else:
            raise exceptions.APINotSupportedError(
                'Worker node provision logs are not supported in your API '
                'server. Please upgrade to a newer API server to use it.')
    params = {
        'follow': str(follow).lower(),
        'tail': tail,
    }

    response = server_common.make_authenticated_request(
        'POST',
        '/provision_logs',
        json=json.loads(body.model_dump_json()),
        params=params,
        stream=True,
        timeout=(client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
                 None))
    # Check for HTTP errors before streaming the response
    if response.status_code != 200:
        with ux_utils.print_exception_no_traceback():
            raise exceptions.CommandError(response.status_code,
                                          'tail_provision_logs',
                                          'Failed to stream provision logs',
                                          response.text)

    # Log request is idempotent when tail is 0, thus can resume previous
    # streaming point on retry.
    # request_id=None here because /provision_logs does not create an async
    # request. Instead, it streams a plain file from the server. This does NOT
    # violate the stream_response doc warning about None in multi-user
    # environments: we are not asking stream_response to select "the latest
    # request". We already have the HTTP response to stream; request_id=None
    # merely disables the follow-up GET. It is also necessary for --no-follow
    # to return cleanly after printing the tailed lines. If we provided a
    # non-None request_id here, the get(request_id) in stream_response(
    # would fail since /provision_logs does not create a request record.
    # By virtue of this, we set get_result to False to block get() from
    # running.
    stream_response(request_id=None,
                    response=response,
                    output_stream=output_stream,
                    resumable=(tail == 0),
                    get_result=False)
    return 0


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(52)
@annotations.client_api
def tail_hook_logs(cluster_name: str,
                   event: str | None = None,
                   follow: bool = True,
                   tail: int = 0) -> int:
    """Tails a per-event lifecycle-hook log on the cluster.

    Args:
        cluster_name: name of the cluster.
        event: one of ``stop``, ``preemption``, ``down``. When None,
            auto-selects whichever log exists on the cluster.
        follow: whether to follow the logs.
        tail: number of lines to display from the end of the log file.

    Returns:
        Exit code 0 on streaming success; non-zero on failure.
    """
    body = payloads.HookLogsBody(cluster_name=cluster_name,
                                 event=event,
                                 follow=follow,
                                 tail=tail)
    response = server_common.make_authenticated_request(
        'POST', '/hook_logs', json=json.loads(body.model_dump_json()))
    request_id: server_common.RequestId[int] = server_common.get_request_id(
        response)
    return stream_and_get(request_id)


# TODO(zpoint): drop the tail_autostop_logs deprecation alias after
# v0.15.0. Replacement: tail_hook_logs(cluster_name, event='stop', ...).
def tail_autostop_logs(cluster_name: str,
                       follow: bool = True,
                       tail: int = 0) -> int:
    """[DEPRECATED] Master-era alias for tail_hook_logs(event='stop').

    The autostop event was renamed to ``stop`` in the generalized
    lifecycle-hooks framework. This shim emits a one-line stderr
    deprecation warning and delegates to :func:`tail_hook_logs` so
    master-version code keeps working through the v0.15.0 grace window.
    """
    sys.stderr.write(hooks_deprecation.TAIL_AUTOSTOP_LOGS_SDK)
    return tail_hook_logs(cluster_name=cluster_name,
                          event='stop',
                          follow=follow,
                          tail=tail)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def download_logs(cluster_name: str,
                  job_ids: list[str] | None) -> dict[str, str]:
    """Downloads the logs of jobs.

    Args:
        cluster_name: (str) name of the cluster.
        job_ids: (List[str]) job ids.

    Returns:
        The request ID of the download_logs request.

    Request Returns:
        job_log_paths (Dict[str, str]): a mapping of job_id to local log path.

    Request Raises:
        sky.exceptions.ClusterDoesNotExist: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
          CloudVmRayBackend.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
          not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
          user identity.
    """
    body = payloads.ClusterJobsDownloadLogsBody(
        cluster_name=cluster_name,
        job_ids=job_ids,
    )
    response = server_common.make_authenticated_request(
        'POST', '/download_logs', json=json.loads(body.model_dump_json()))
    request_id: server_common.RequestId[dict[
        str, str]] = server_common.get_request_id(response)
    job_id_remote_path_dict = stream_and_get(request_id)
    remote2local_path_dict = client_common.download_logs_from_api_server(
        job_id_remote_path_dict.values())
    return {
        job_id: remote2local_path_dict[remote_path]
        for job_id, remote_path in job_id_remote_path_dict.items()
    }


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def start(
    cluster_name: str,
    idle_minutes_to_autostop: int | None = None,
    wait_for: autostop_lib.AutostopWaitFor | None = None,
    retry_until_up: bool = False,
    down: bool = False,  # pylint: disable=redefined-outer-name
    force: bool = False,
) -> server_common.RequestId['backends.CloudVmRayResourceHandle']:
    """Restart a cluster.

    If a cluster is previously stopped (status is STOPPED) or failed in
    provisioning/runtime installation (status is INIT), this function will
    attempt to start the cluster.  In the latter case, provisioning and runtime
    installation will be retried.

    Auto-failover provisioning is not used when restarting a stopped
    cluster. It will be started on the same cloud, region, and zone that were
    chosen before.

    If a cluster is already in the UP status, this function has no effect.

    Args:
        cluster_name: name of the cluster to start.
        idle_minutes_to_autostop: automatically stop the cluster after this
            many minute of idleness, i.e., no running or pending jobs in the
            cluster's job queue. Idleness gets reset whenever setting-up/
            running/pending jobs are found in the job queue. Setting this
            flag is equivalent to running ``sky.launch()`` and then
            ``sky.autostop(idle_minutes=<minutes>)``. If not set, the
            cluster will not be autostopped.
        wait_for: determines the condition for resetting the idleness timer.
            This option works in conjunction with ``idle_minutes_to_autostop``.
            Choices:

            1. "jobs_and_ssh" (default) - Wait for in-progress jobs and SSH
               connections to finish.
            2. "jobs" - Only wait for in-progress jobs.
            3. "none" - Wait for nothing; autostop right after
               ``idle_minutes_to_autostop``.
        retry_until_up: whether to retry launching the cluster until it is
            up.
        down: Autodown the cluster: tear down the cluster after specified
            minutes of idle time after all jobs finish (successfully or
            abnormally). Requires ``idle_minutes_to_autostop`` to be set.
        force: whether to force start the cluster even if it is already up.
            Useful for upgrading SkyPilot runtime.

    Returns:
        The request ID of the start request.

    Request Returns:
        None

    Request Raises:
        ValueError: argument values are invalid: (1) if ``down`` is set to True
            but ``idle_minutes_to_autostop`` is None; (2) if the specified
            cluster is the managed jobs controller, and either
            ``idle_minutes_to_autostop`` is not None or ``down`` is True (omit
            them to use the default autostop settings).
        sky.exceptions.ClusterDoesNotExist: the specified cluster does not
            exist.
        sky.exceptions.NotSupportedError: if the cluster to restart was
            launched using a non-default backend that does not support this
            operation.
        sky.exceptions.ClusterOwnerIdentitiesMismatchError: if the cluster to
            restart was launched by a different user.
    """
    remote_api_version = versions.get_remote_api_version()
    if wait_for is not None and (remote_api_version is None or
                                 remote_api_version < 13):
        logger.warning('wait_for is not supported in your API server. '
                       'Please upgrade to a newer API server to use it.')

    body = payloads.StartBody(
        cluster_name=cluster_name,
        idle_minutes_to_autostop=idle_minutes_to_autostop,
        wait_for=wait_for,
        retry_until_up=retry_until_up,
        down=down,
        force=force,
    )
    response = server_common.make_authenticated_request(
        'POST', '/start', json=json.loads(body.model_dump_json()), timeout=5)
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def down(
    cluster_name: str,
    purge: bool = False,
    graceful: bool = False,
    graceful_timeout: int | None = None,
    *,
    _expected_cluster_record_uuid: str | None = None,
) -> server_common.RequestId[None]:
    """Tears down a cluster.

    Tearing down a cluster will delete all associated resources (all billing
    stops), and any data on the attached disks will be lost.  Accelerators
    (e.g., TPUs) that are part of the cluster will be deleted too.

    Args:
        cluster_name: name of the cluster to down.
        purge: (Advanced) Forcefully remove the cluster from SkyPilot's cluster
            table, even if the actual cluster termination failed on the cloud.
            WARNING: This flag should only be set sparingly in certain manual
            troubleshooting scenarios; with it set, it is the user's
            responsibility to ensure there are no leaked instances and related
            resources.
        graceful: Cancel the user's task but block until MOUNT_CACHED data is
            fully uploaded. This helps with preserving user data integrity.
        graceful_timeout: If not None, sets a timeout for the graceful option
            above (in seconds).

    Returns:
        The request ID of the down request.

    Request Returns:
        None

    Request Raises:
        sky.exceptions.ClusterDoesNotExist: the specified cluster does not
            exist.
        RuntimeError: failed to tear down the cluster.
        sky.exceptions.NotSupportedError: the specified cluster is the managed
            jobs controller.

    """
    version = versions.get_remote_api_version()
    if graceful and version is not None and version < 32:
        logger.warning('`--graceful` is ignored because the server does '
                       'not support it yet.')
    if _expected_cluster_record_uuid is not None:
        try:
            parsed_record_uuid = uuid.UUID(_expected_cluster_record_uuid)
        except (AttributeError, TypeError, ValueError) as e:
            raise ValueError('Expected cluster-record UUID must be canonical '
                             'UUID text.') from e
        if str(parsed_record_uuid) != _expected_cluster_record_uuid:
            raise ValueError('Expected cluster-record UUID must be canonical '
                             'UUID text.')
        minimum_version = (
            server_constants.
            MIN_RESOURCE_ACTION_EXPECTED_CLUSTER_UUID_API_VERSION)
        if version is None or version < minimum_version:
            raise RuntimeError(
                'The API server cannot preserve the resource-action '
                'cluster-record teardown fence.')
    body = payloads.StopOrDownBody(
        cluster_name=cluster_name,
        purge=purge,
        graceful=graceful,
        graceful_timeout=graceful_timeout,
        resource_action_expected_cluster_record_uuid=(
            _expected_cluster_record_uuid),
    )
    response = server_common.make_authenticated_request(
        'POST', '/down', json=json.loads(body.model_dump_json()), timeout=5)
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def stop(
    cluster_name: str,
    purge: bool = False,
    graceful: bool = False,
    graceful_timeout: int | None = None,
) -> server_common.RequestId[None]:
    """Stops a cluster.

    Data on attached disks is not lost when a cluster is stopped.  Billing for
    the instances will stop, while the disks will still be charged.  Those
    disks will be reattached when restarting the cluster.

    Currently, spot instance clusters cannot be stopped (except for GCP, which
    does allow disk contents to be preserved when stopping spot VMs).

    Args:
        cluster_name: name of the cluster to stop.
        purge: (Advanced) Forcefully mark the cluster as stopped in SkyPilot's
            cluster table, even if the actual cluster stop operation failed on
            the cloud. WARNING: This flag should only be set sparingly in
            certain manual troubleshooting scenarios; with it set, it is the
            user's responsibility to ensure there are no leaked instances and
            related resources.

    Returns:
        The request ID of the stop request.

    Request Returns:
        None

    Request Raises:
        sky.exceptions.ClusterDoesNotExist: the specified cluster does not
            exist.
        RuntimeError: failed to stop the cluster.
        sky.exceptions.NotSupportedError: if the specified cluster is a spot
            cluster, or a TPU VM Pod cluster, or the managed jobs controller.

    """
    version = versions.get_remote_api_version()
    if graceful and version is not None and version < 32:
        logger.warning('`--graceful` is ignored because the server does '
                       'not support it yet.')
    body = payloads.StopOrDownBody(
        cluster_name=cluster_name,
        purge=purge,
        graceful=graceful,
        graceful_timeout=graceful_timeout,
    )
    response = server_common.make_authenticated_request(
        'POST', '/stop', json=json.loads(body.model_dump_json()), timeout=5)
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def autostop(
    cluster_name: str,
    idle_minutes: int,
    wait_for: autostop_lib.AutostopWaitFor | None = None,
    down: bool = False,  # pylint: disable=redefined-outer-name
    hook: str | None = None,
    hook_timeout: int | None = None,
) -> server_common.RequestId[None]:
    """Schedules an autostop/autodown for a cluster.

    Autostop/autodown will automatically stop or teardown a cluster when it
    becomes idle for a specified duration.  Idleness means there are no
    in-progress (pending/running) jobs in a cluster's job queue.

    Idleness time of a cluster is reset to zero, whenever:

    - A job is submitted (``sky.launch()`` or ``sky.exec()``).

    - The cluster has restarted.

    - An autostop is set when there is no active setting. (Namely, either
      there's never any autostop setting set, or the previous autostop setting
      was canceled.) This is useful for restarting the autostop timer.

    Example: say a cluster without any autostop set has been idle for 1 hour,
    then an autostop of 30 minutes is set. The cluster will not be immediately
    autostopped. Instead, the idleness timer only starts counting after the
    autostop setting was set.

    When multiple autostop settings are specified for the same cluster, the
    last setting takes precedence.

    Args:
        cluster_name: name of the cluster.
        idle_minutes: the number of minutes of idleness (no pending/running
            jobs) after which the cluster will be stopped automatically. Setting
            to a negative number cancels any autostop/autodown setting.
        wait_for: determines the condition for resetting the idleness timer.
            This option works in conjunction with ``idle_minutes``.
            Choices:

            1. "jobs_and_ssh" (default) - Wait for in-progress jobs and SSH
               connections to finish.
            2. "jobs" - Only wait for in-progress jobs.
            3. "none" - Wait for nothing; autostop right after ``idle_minutes``.
        down: if true, use autodown (tear down the cluster; non-restartable),
            rather than autostop (restartable).
        hook: optional script to execute on the remote cluster before autostop.
            The script runs before the cluster is stopped or torn down. If the
            hook fails, autostop will still proceed but a warning will be
            logged.
        hook_timeout: timeout in seconds for hook execution. If None, uses
            DEFAULT_HOOK_TIMEOUT_SECONDS (3600 = 1 hour). The hook will
            be terminated if it exceeds this timeout.

    Returns:
        The request ID of the autostop request.

    Request Returns:
        None

    Request Raises:
        ValueError: if arguments are invalid.
        sky.exceptions.ClusterDoesNotExist: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
            CloudVmRayBackend or the cluster is TPU VM Pod.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
            not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
            user identity.
    """
    if hook_timeout is not None and hook is None:
        raise ValueError('hook_timeout can only be set if hook is set.')

    remote_api_version = versions.get_remote_api_version()
    if wait_for is not None and (remote_api_version is None or
                                 remote_api_version < 13):
        logger.warning('wait_for is not supported in your API server. '
                       'Please upgrade to a newer API server to use it.')

    # Hook support requires API version 28 or higher
    if hook is not None and (remote_api_version is None or
                             remote_api_version < 28):
        logger.warning('Autostop hook is not supported in your API server. '
                       'Please upgrade to a newer API server to use it.')

    body = payloads.AutostopBody(
        cluster_name=cluster_name,
        idle_minutes=idle_minutes,
        wait_for=wait_for,
        down=down,
        hook=hook,
        hook_timeout=hook_timeout,
    )
    response = server_common.make_authenticated_request(
        'POST', '/autostop', json=json.loads(body.model_dump_json()), timeout=5)
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def queue(
    cluster_name: str,
    skip_finished: bool = False,
    all_users: bool = False
) -> server_common.RequestId[list[responses.ClusterJobRecord]]:
    """Gets the job queue of a cluster.

    Args:
        cluster_name: name of the cluster.
        skip_finished: if True, skip finished jobs.
        all_users: if True, return jobs from all users.


    Returns:
        The request ID of the queue request.

    Request Returns:
        job_records (List[responses.ClusterJobRecord]): A list of job records
            for each job in the queue.

            .. code-block:: python

                [
                    {
                        'job_id': (int) job id,
                        'job_name': (str) job name,
                        'username': (str) username,
                        'user_hash': (str) user hash,
                        'submitted_at': (int) timestamp of submitted,
                        'start_at': (int) timestamp of started,
                        'end_at': (int) timestamp of ended,
                        'resources': (str) resources,
                        'status': (job_lib.JobStatus) job status,
                        'log_path': (str) log path,
                    }
                ]

    Request Raises:
        sky.exceptions.ClusterDoesNotExist: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
            ``CloudVmRayBackend``.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
            not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
            user identity.
        sky.exceptions.CommandError: if failed to get the job queue with ssh.
    """
    body = payloads.QueueBody(
        cluster_name=cluster_name,
        skip_finished=skip_finished,
        all_users=all_users,
    )
    response = server_common.make_authenticated_request(
        'POST', '/queue', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def job_status(
    cluster_name: str,
    job_ids: list[int] | None = None
) -> server_common.RequestId[dict[int | None, Optional['job_lib.JobStatus']]]:
    """Gets the status of jobs on a cluster.

    Args:
        cluster_name: name of the cluster.
        job_ids: job ids. If None, get the status of the last job.

    Returns:
        The request ID of the job status request.

    Request Returns:
        job_statuses (Dict[Optional[int], Optional[job_lib.JobStatus]]): A
            mapping of job_id to job statuses. The status will be None if the
            job does not exist. If job_ids is None and there is no job on the
            cluster, it will return {None: None}.

    Request Raises:
        sky.exceptions.ClusterDoesNotExist: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the cluster is not based on
            ``CloudVmRayBackend``.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
            not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
            user identity.
    """
    # TODO: merge this into the queue endpoint, i.e., let the queue endpoint
    # take job_ids to filter the returned jobs.
    body = payloads.JobStatusBody(
        cluster_name=cluster_name,
        job_ids=job_ids,
    )
    response = server_common.make_authenticated_request(
        'POST', '/job_status', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def cancel(
    cluster_name: str,
    all: bool = False,  # pylint: disable=redefined-builtin
    all_users: bool = False,
    job_ids: list[int] | None = None,
    # pylint: disable=invalid-name
    _try_cancel_if_cluster_is_init: bool = False
) -> server_common.RequestId[None]:
    """Cancels jobs on a cluster.

    Args:
        cluster_name: name of the cluster.
        all: if True, cancel all jobs.
        all_users: if True, cancel all jobs from all users.
        job_ids: a list of job IDs to cancel.
        _try_cancel_if_cluster_is_init: (bool) whether to try cancelling the job
            even if the cluster is not UP, but the head node is still alive.
            This is used by the jobs controller to cancel the job when the
            worker node is preempted in the spot cluster.

    Returns:
        The request ID of the cancel request.

    Request Returns:
        None

    Request Raises:
        ValueError: if arguments are invalid.
        sky.exceptions.ClusterDoesNotExist: if the cluster does not exist.
        sky.exceptions.ClusterNotUpError: if the cluster is not UP.
        sky.exceptions.NotSupportedError: if the specified cluster is a
            controller that does not support this operation.
        sky.exceptions.ClusterOwnerIdentityMismatchError: if the current user is
            not the same as the user who created the cluster.
        sky.exceptions.CloudUserIdentityError: if we fail to get the current
            user identity.

    """
    body = payloads.CancelBody(
        cluster_name=cluster_name,
        all=all,
        all_users=all_users,
        job_ids=job_ids,
        try_cancel_if_cluster_is_init=_try_cancel_if_cluster_is_init,
    )
    response = server_common.make_authenticated_request(
        'POST', '/cancel', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def status(
    cluster_names: list[str] | None = None,
    refresh: common.StatusRefreshMode = common.StatusRefreshMode.NONE,
    all_users: bool = False,
    *,
    workspaces_filter: list[str] | None = None,
    _include_credentials: bool = False,
    _summary_response: bool = False,
) -> server_common.RequestId[list[responses.StatusResponse]]:
    """Gets cluster statuses.

    If cluster_names is given, return those clusters. Otherwise, return all
    clusters.

    Each cluster can have one of the following statuses:

    - ``INIT``: The cluster may be live or down. It can happen in the following
      cases:

      - Ongoing provisioning or runtime setup. (A ``sky.launch()`` has started
        but has not completed.)
      - Or, the cluster is in an abnormal state, e.g., some cluster nodes are
        down, or the SkyPilot runtime is unhealthy. (To recover the cluster,
        try ``sky launch`` again on it.)

    - ``UP``: Provisioning and runtime setup have succeeded and the cluster is
      live.  (The most recent ``sky.launch()`` has completed successfully.)

    - ``STOPPED``: The cluster is stopped and the storage is persisted. Use
      ``sky.start()`` to restart the cluster.

    Autostop column:

    - The autostop column indicates how long the cluster will be autostopped
      after minutes of idling (no jobs running). If ``to_down`` is True, the
      cluster will be autodowned, rather than autostopped.

    Getting up-to-date cluster statuses:

    - In normal cases where clusters are entirely managed by SkyPilot (i.e., no
      manual operations in cloud consoles) and no autostopping is used, the
      table returned by this command will accurately reflect the cluster
      statuses.

    - In cases where the clusters are changed outside of SkyPilot (e.g., manual
      operations in cloud consoles; unmanaged spot clusters getting preempted)
      or for autostop-enabled clusters, use ``refresh=True`` to query the
      latest cluster statuses from the cloud providers.

    Args:
        cluster_names: a list of cluster names to query. If not
            provided, all clusters will be queried.
        workspaces_filter: if provided, only clusters in these workspaces
            will be queried.
        refresh: whether to query the latest cluster statuses from the cloud
            provider(s).
        all_users: whether to include all users' clusters. By default, only
            the current user's clusters are included.
        _include_credentials: (internal) whether to include cluster ssh
            credentials in the response (default: False).

    Returns:
        The request ID of the status request.

    Request Returns:
        cluster_records (List[Dict[str, Any]]): A list of dicts, with each dict
          containing the information of a cluster. If a cluster is found to be
          terminated or not found, it will be omitted from the returned list.

          .. code-block:: python

            {
              'name': (str) cluster name,
              'launched_at': (int) timestamp of last launch on this cluster,
              'handle': (ResourceHandle) an internal handle to the cluster,
              'last_use': (str) the last command/entrypoint that affected this
              cluster,
              'status': (sky.ClusterStatus) cluster status,
              'autostop': (int) idle time before autostop,
              'to_down': (bool) whether autodown is used instead of autostop,
              'metadata': (dict) metadata of the cluster,
              'user_hash': (str) user hash of the cluster owner,
              'user_name': (str) user name of the cluster owner,
              'resources_str': (str) the resource string representation of the
                cluster,
            }

    """
    # TODO(zhwu): this does not stream the logs output by logger back to the
    # user, due to the rich progress implementation.
    remote_api_version = versions.get_remote_api_version()
    if (workspaces_filter is not None and remote_api_version is not None and
            remote_api_version
            < server_constants.MIN_STATUS_WORKSPACE_FILTER_API_VERSION):
        with ux_utils.print_exception_no_traceback():
            raise exceptions.APINotSupportedError(
                'Filtering cluster status by workspace requires API server '
                'version '
                f'{server_constants.MIN_STATUS_WORKSPACE_FILTER_API_VERSION} '
                'or newer. Please upgrade the remote server.')
    body = payloads.StatusBody(
        cluster_names=cluster_names,
        workspaces_filter=workspaces_filter,
        refresh=refresh,
        all_users=all_users,
        include_credentials=_include_credentials,
        summary_response=_summary_response,
    )
    response = server_common.make_authenticated_request(
        'POST', '/status', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def endpoints(
        cluster: str,
        port: int | str | None = None
) -> server_common.RequestId[dict[int, str]]:
    """Gets the endpoint for a given cluster and port number (endpoint).

    Example:
        .. code-block:: python

            import sky
            request_id = sky.endpoints('test-cluster')
            sky.get(request_id)


    Args:
        cluster: The name of the cluster.
        port: The port number to get the endpoint for. If None, endpoints
            for all ports are returned.

    Returns:
        The request ID of the endpoints request.

    Request Returns:
        A dictionary of port numbers to endpoints.
        If port is None, the dictionary contains all
            ports:endpoints exposed on the cluster.

    Request Raises:
        ValueError: if the cluster is not UP or the endpoint is not exposed.
        RuntimeError: if the cluster has no ports to be exposed or no endpoints
            are exposed yet.
    """
    body = payloads.EndpointsBody(
        cluster=cluster,
        port=port,
    )
    response = server_common.make_authenticated_request(
        'POST', '/endpoints', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def cost_report(
    days: int | None = None
) -> server_common.RequestId[list[dict[str, Any]]]:  # pylint: disable=redefined-builtin
    """Gets all cluster cost reports, including those that have been downed.

    The estimated cost column indicates price for the cluster based on the type
    of resources being used and the duration of use up until the call to
    status. This means if the cluster is UP, successive calls to report will
    show increasing price. The estimated cost is calculated based on the local
    cache of the cluster status, and may not be accurate for the cluster with
    autostop/use_spot set or terminated/stopped on the cloud console.

    Args:
        days: The number of days to get the cost report for. If not provided,
            the default is 30 days.

    Returns:
        The request ID of the cost report request.

    Request Returns:
        cluster_cost_records (List[Dict[str, Any]]): A list of dicts, with each
          dict containing the cost information of a cluster.

          .. code-block:: python

            {
              'name': (str) cluster name,
              'launched_at': (int) timestamp of last launch on this cluster,
              'duration': (int) total seconds that cluster was up and running,
              'last_use': (str) the last command/entrypoint that affected this
              'num_nodes': (int) number of nodes launched for cluster,
              'resources': (resources.Resources) type of resource launched,
              'cluster_hash': (str) unique hash identifying cluster,
              'usage_intervals': (List[Tuple[int, int]]) cluster usage times,
              'total_cost': (float) cost given resources and usage intervals,
            }
    """
    body = payloads.CostReportBody(days=days)
    response = server_common.make_authenticated_request(
        'POST', '/cost_report', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


def list_events(  # pylint: disable=redefined-outer-name
    *,
    cluster: str | None = None,
    workspaces: list[str] | tuple[str, ...] | None = None,
    kinds: list[event_api_models.EventKind | str] |
    tuple[event_api_models.EventKind | str, ...] | None = None,
    outcomes: list[event_api_models.EventOutcome | str] |
    tuple[event_api_models.EventOutcome | str, ...] | None = None,
    actor_ids: list[str] | tuple[str, ...] | None = None,
    actor_types: list[event_api_models.EventActorType | str] |
    tuple[event_api_models.EventActorType | str, ...] | None = None,
    target_type: event_api_models.EventTargetType | str | None = None,
    target_id: str | None = None,
    target_name: str | None = None,
    request_id: str | None = None,
    since: datetime.datetime | str | None = None,
    until: datetime.datetime | str | None = None,
    direction: event_api_models.TraversalDirection |
    str = (event_api_models.TraversalDirection.OLDER),
    limit: int = 50,
    cursor: str | None = None,
) -> event_api_models.EventsPage:
    """Returns one actor-aware operational event page."""
    return events_client.list_events(cluster=cluster,
                                     workspaces=workspaces,
                                     kinds=kinds,
                                     outcomes=outcomes,
                                     actor_ids=actor_ids,
                                     actor_types=actor_types,
                                     target_type=target_type,
                                     target_id=target_id,
                                     target_name=target_name,
                                     request_id=request_id,
                                     since=since,
                                     until=until,
                                     direction=direction,
                                     limit=limit,
                                     cursor=cursor)


# === Storage APIs ===
@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def storage_ls() -> server_common.RequestId[list[responses.StorageRecord]]:
    """Gets the storages.

    Returns:
        The request ID of the storage list request.

    Request Returns:
        storage_records (List[responses.StorageRecord]):
            A list of storage records.
    """
    response = server_common.make_authenticated_request('GET', '/storage/ls')
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def storage_delete(name: str) -> server_common.RequestId[None]:
    """Deletes a storage.

    Args:
        name: The name of the storage to delete.

    Returns:
        The request ID of the storage delete request.

    Request Returns:
        None

    Request Raises:
        ValueError: If the storage does not exist.
    """
    body = payloads.StorageBody(name=name)
    response = server_common.make_authenticated_request(
        'POST', '/storage/delete', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


# === Kubernetes ===


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def local_up(gpus: bool,
             name: str | None = None,
             port_start: int | None = None) -> server_common.RequestId[None]:
    """Launches a Kubernetes cluster on local machines.

    Returns:
        request_id: The request ID of the local up request.
    """
    # We do not allow local up when the API server is running remotely since it
    # will modify the kubeconfig.
    # TODO: move this check to server.
    if not server_common.is_api_server_local():
        with ux_utils.print_exception_no_traceback():
            raise ValueError('`sky local up` is only supported when '
                             'running SkyPilot locally.')

    body = payloads.LocalUpBody(gpus=gpus, name=name, port_start=port_start)
    response = server_common.make_authenticated_request(
        'POST', '/local_up', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def local_down(name: str | None) -> server_common.RequestId[None]:
    """Tears down the Kubernetes cluster started by local_up."""
    # We do not allow local up when the API server is running remotely since it
    # will modify the kubeconfig.
    # TODO: move this check to remote server.
    if not server_common.is_api_server_local():
        with ux_utils.print_exception_no_traceback():
            raise ValueError('`sky local down` is only supported when running '
                             'SkyPilot locally.')

    body = payloads.LocalDownBody(name=name)
    response = server_common.make_authenticated_request(
        'POST', '/local_down', json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


def _update_remote_ssh_node_pools(file: str, infra: str | None = None) -> None:
    """Update the SSH node pools on the remote server.

    This function will also upload the local SSH key to the remote server, and
    replace the file path to the remote SSH key file path.

    Args:
        file: The path to the local SSH node pools config file.
        infra: The name of the cluster configuration in the local SSH node
            pools config file. If None, all clusters in the file are updated.
    """
    file = os.path.expanduser(file)
    if not os.path.exists(file):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'SSH Node Pool config file {file} does not exist. '
                'Please check if the file exists and the path is correct.')
    config = ssh_utils.load_ssh_targets(file)
    config = ssh_utils.get_cluster_config(config, infra)
    pools_config = {}
    for name, pool_config in config.items():
        hosts_info = ssh_utils.prepare_hosts_info(
            name, pool_config, upload_ssh_key_func=_upload_ssh_key_and_wait)
        pools_config[name] = {'hosts': hosts_info}
    server_common.make_authenticated_request('POST',
                                             '/ssh_node_pools',
                                             json=pools_config)


def _upload_ssh_key_and_wait(key_name: str, key_file_path: str) -> str:
    """Upload the SSH key to the remote server and wait for the key to be
    uploaded.

    Args:
        key_name: The name of the SSH key.
        key_file_path: The path to the local SSH key file.

    Returns:
        The path for the remote SSH key file on the API server.
    """
    if not os.path.exists(os.path.expanduser(key_file_path)):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'SSH key file not found: {key_file_path}')

    with open(os.path.expanduser(key_file_path), 'rb') as key_file:
        response = server_common.make_authenticated_request(
            'POST',
            '/ssh_node_pools/keys',
            files={
                'key_file': (key_name, key_file, 'application/octet-stream')
            },
            data={'key_name': key_name},
            cookies=server_common.get_api_cookie_jar())

    return response.json()['key_path']


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def ssh_up(infra: str | None = None,
           file: str | None = None) -> server_common.RequestId[None]:
    """Deploys the SSH Node Pools defined in ~/.sky/ssh_targets.yaml.

    Args:
        infra: Name of the cluster configuration in ssh_targets.yaml.
            If None, the first cluster in the file is used.
        file: Name of the ssh node pool configuration file to use. If
            None, the default path, ~/.sky/ssh_node_pools.yaml is used.

    Returns:
        request_id: The request ID of the SSH cluster deployment request.
    """
    if file is not None:
        _update_remote_ssh_node_pools(file, infra)

    # Use SSH node pools router endpoint
    body = payloads.SSHUpBody(infra=infra, cleanup=False)
    if infra is not None:
        # Call the specific pool deployment endpoint
        response = server_common.make_authenticated_request(
            'POST', f'/ssh_node_pools/{infra}/deploy')
    else:
        # Call the general deployment endpoint
        response = server_common.make_authenticated_request(
            'POST',
            '/ssh_node_pools/deploy',
            json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def ssh_down(infra: str | None = None) -> server_common.RequestId[None]:
    """Tears down a Kubernetes cluster on SSH targets.

    Args:
        infra: Name of the cluster configuration in ssh_targets.yaml.
            If None, the first cluster in the file is used.

    Returns:
        request_id: The request ID of the SSH cluster teardown request.
    """
    # Use SSH node pools router endpoint
    body = payloads.SSHUpBody(infra=infra, cleanup=True)
    if infra is not None:
        # Call the specific pool down endpoint
        response = server_common.make_authenticated_request(
            'POST', f'/ssh_node_pools/{infra}/down')
    else:
        # Call the general down endpoint
        response = server_common.make_authenticated_request(
            'POST',
            '/ssh_node_pools/down',
            json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def realtime_kubernetes_gpu_availability(
    context: str | None = None,
    name_filter: str | None = None,
    quantity_filter: int | None = None,
    is_ssh: bool | None = None
) -> server_common.RequestId[list[tuple[
        str, list['models.RealtimeGpuAvailability']]]]:
    """Gets the real-time Kubernetes GPU availability.

    Returns:
        The request ID of the real-time Kubernetes GPU availability request.
    """
    body = payloads.RealtimeGpuAvailabilityRequestBody(
        context=context,
        name_filter=name_filter,
        quantity_filter=quantity_filter,
        is_ssh=is_ssh,
    )
    response = server_common.make_authenticated_request(
        'POST',
        '/realtime_kubernetes_gpu_availability',
        json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def kubernetes_node_info(
    context: str | None = None
) -> server_common.RequestId['models.KubernetesNodesInfo']:
    """Gets the resource information for all the nodes in the cluster.

    Currently only GPU resources are supported. The function returns the total
    number of GPUs available on the node and the number of free GPUs on the
    node.

    If the user does not have sufficient permissions to list pods in all
    namespaces, the function will return free GPUs as -1.

    Args:
        context: The Kubernetes context. If None, the default context is used.

    Returns:
        The request ID of the Kubernetes node info request.

    Request Returns:
        KubernetesNodesInfo: A model that contains the node info map and other
            information.
    """
    body = payloads.KubernetesNodeInfoRequestBody(context=context)
    response = server_common.make_authenticated_request(
        'POST',
        '/kubernetes_node_info',
        json=json.loads(body.model_dump_json()))
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
def status_kubernetes() -> server_common.RequestId[
    tuple[list['kubernetes_utils.KubernetesSkyPilotClusterInfoPayload'],
          list['kubernetes_utils.KubernetesSkyPilotClusterInfoPayload'],
          list[responses.ManagedJobRecord], str | None]]:
    """[Experimental] Gets all SkyPilot clusters and jobs
    in the Kubernetes cluster.

    Managed jobs and services are also included in the clusters returned.
    The caller must parse the controllers to identify which clusters are run
    as managed jobs or services.

    Returns:
        The request ID of the status request.

    Request Returns:
        A tuple containing:
        - all_clusters: List of KubernetesSkyPilotClusterInfoPayload with info
            for all clusters, including managed jobs, services and controllers.
        - unmanaged_clusters: List of KubernetesSkyPilotClusterInfoPayload with
            info for all clusters excluding managed jobs and services.
            Controllers are included.
        - all_jobs: List of managed jobs from all controllers. Each entry is a
            dictionary job info, see jobs.queue_from_kubernetes_pod for details.
        - context: Kubernetes context used to fetch the cluster information.
    """
    response = server_common.make_authenticated_request('GET',
                                                        '/status_kubernetes')
    return server_common.get_request_id(response)


# === API request APIs ===
@usage_lib.entrypoint
@annotations.client_api
def get(request_id: server_common.RequestId[T]) -> T:
    """Waits for and gets the result of a request.

    This function will not check the server health since /api/get is typically
    not the first API call in an SDK session and checking the server health
    may cause GET /api/get being sent to a restarted API server.

    Args:
        request_id: The request ID of the request to get. May be a full request
            ID or a prefix. Authenticated non-admin users can retrieve only
            requests they own.

    Returns:
        The ``Request Returns`` of the specified request. See the documentation
        of the specific requests above for more details.

    Raises:
        Exception: It raises the same exceptions as the specific requests,
            see ``Request Raises`` in the documentation of the specific requests
            above.
    """
    return request_results.get(
        request_id,
        raise_exception=_raise_exception_object_on_client,
        logger=logger)


@typing.overload
def stream_and_get(request_id: server_common.RequestId[T],
                   log_path: str | None = None,
                   tail: int | None = None,
                   follow: bool = True,
                   output_stream: Optional['io.TextIOBase'] = None,
                   relay_rich_status: bool = False) -> T:
    ...


@typing.overload
def stream_and_get(request_id: None = None,
                   log_path: str | None = None,
                   tail: int | None = None,
                   follow: bool = True,
                   output_stream: Optional['io.TextIOBase'] = None,
                   relay_rich_status: bool = False) -> None:
    ...


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@annotations.client_api
@rest.retry_transient_errors()
def stream_and_get(
    request_id: server_common.RequestId[T] | None = None,
    log_path: str | None = None,
    tail: int | None = None,
    follow: bool = True,
    output_stream: Optional['io.TextIOBase'] = None,
    relay_rich_status: bool = False,
) -> T | None:
    """Streams the logs of a request or a log file and gets the final result.

    This will block until the request is finished. The request id can be a
    prefix of the full request id.

    Args:
        request_id: The request ID of the request to stream. May be a full
            request ID or a prefix.
            If None, the latest request submitted to the API server is streamed.
            Using None request_id is not recommended in multi-user environments.
        log_path: The path to the log file to stream. On an authenticated
            multi-user server, arbitrary log paths require an admin role.
        tail: The number of lines to show from the end of the logs.
            If None, show all logs.
        follow: Whether to follow the logs.
        output_stream: The output stream to write to. If None, print to the
            console.
        relay_rich_status: If True, forward encoded rich-status control payloads
            verbatim to the output instead of rendering a local spinner. Used by
            the managed jobs controller to preserve provisioning spinner codes
            in its per-job log. See
            :func:`sky.utils.rich_utils.decode_rich_status`.

    Returns:
        The ``Request Returns`` of the specified request. See the documentation
        of the specific requests above for more details.
        If follow is False, will always return None. See note on
        stream_response.

    Raises:
        Exception: It raises the same exceptions as the specific requests,
            see ``Request Raises`` in the documentation of the specific requests
            above.
    """
    return request_results.stream_and_get(request_id,
                                          log_path,
                                          tail,
                                          follow,
                                          output_stream,
                                          relay_rich_status,
                                          get_request_result=get,
                                          stream_response_fn=stream_response)


@usage_lib.entrypoint
@annotations.client_api
def api_cancel(request_ids: server_common.RequestId[T] |
               list[server_common.RequestId[T]] | str | list[str] | None = None,
               all_users: bool = False,
               silent: bool = False) -> server_common.RequestId[list[str]]:
    """Aborts a request or all requests.

    Args:
        request_ids: The request ID(s) to abort. Can be a single string or a
            list of strings.
        all_users: Whether to abort all requests from all users. This requires
            an admin role on an authenticated server.
        silent: Whether to suppress the output.

    Returns:
        The request ID of the abort request itself.

    Request Returns:
        A list of request IDs that were cancelled.

    Raises:
        click.BadParameter: If no request ID is specified and not all or
            all_users is not set.
    """
    echo = logger.info if not silent else logger.debug
    user_id = None
    if not all_users:
        user_id = common_utils.get_user_hash()

    # Convert single request ID to list if needed
    if isinstance(request_ids, str):
        request_ids = [request_ids]

    body = payloads.RequestCancelBody(request_ids=request_ids, user_id=user_id)
    if all_users:
        echo('Cancelling all users\' requests...')
    elif request_ids is None:
        echo(f'Cancelling all requests for user {user_id!r}...')
    else:
        request_id_str = ', '.join(
            repr(request_id) for request_id in request_ids)
        plural = 's' if len(request_ids) > 1 else ''
        echo(f'Cancelling {len(request_ids)} request{plural}: '
             f'{request_id_str}...')

    response = server_common.make_authenticated_request(
        'POST',
        '/api/cancel',
        json=json.loads(body.model_dump_json()),
        timeout=5)
    return server_common.get_request_id(response)


def _local_api_server_running(kill: bool = False) -> bool:
    """Checks if the local api server is running."""
    for process in psutil.process_iter(attrs=['pid', 'cmdline']):
        cmdline = process.info['cmdline']
        if cmdline and server_common.API_SERVER_CMD in ' '.join(cmdline):
            if kill:
                subprocess_utils.kill_children_processes(
                    parent_pids=[process.pid], force=True)
            return True
    return False


@usage_lib.entrypoint
@annotations.client_api
def api_status(
    request_ids: list[server_common.RequestId[T] | str] | None = None,
    # pylint: disable=redefined-builtin
    all_status: bool = False,
    limit: int | None = None,
    fields: list[str] | None = None,
    cluster_name: str | None = None,
    cluster_names: list[str] | None = None,
    _include_request_names: list[str] | None = None,
    _execution_quiescence_candidates_only: bool = False,
    _exact_request_ids: bool = False,
    _use_body: bool = False,
    _request_timeout_seconds: float | None = None,
    _retry_on_server_unavailable: bool = True,
) -> list[payloads.RequestPayload]:
    """Lists all requests.

    Args:
        request_ids: The prefixes of the request IDs of the requests to query.
            If None, all requests are queried.
        all_status: Whether to list all finished requests as well. This argument
            is ignored if request_ids is not None.
        limit: The number of requests to show. If None, show all requests.
        fields: Safe metadata fields to get. Request bodies, callables, return
            values, errors, status messages, executor PIDs, and file-mount blob
            IDs are not available from the organization-wide status interface.
        cluster_name: Filter requests by cluster name.
            If None, show all requests.
        cluster_names: Filter requests by any of these cluster names in one
            server-side query. Mutually exclusive with ``cluster_name``.
        _include_request_names: Internal exact request-name allowlist.
        _execution_quiescence_candidates_only: Internal PostgreSQL filter for
            active or receipt-required request generations.
        _exact_request_ids: Treat ``request_ids`` as full primary keys and
            query them in one server-side batch instead of as prefixes.
        _use_body: Use the v70 body-backed endpoint for a potentially large
            filter set.
        _request_timeout_seconds: Internal bounded connect/read timeout. The
            default preserves the ordinary unbounded status-read contract.
        _retry_on_server_unavailable: Internal switch for callers that own a
            bounded, best-effort observation rather than an operator request.

    Returns:
        A list of request payloads.
    """
    if server_common.is_api_server_local() and not _local_api_server_running():
        logger.info('SkyPilot API server is not running.')
        return []

    if cluster_name is not None and cluster_names is not None:
        raise ValueError('cluster_name and cluster_names are mutually '
                         'exclusive.')
    if (_request_timeout_seconds is not None and
        (isinstance(_request_timeout_seconds, bool) or
         not isinstance(_request_timeout_seconds,
                        (int, float)) or _request_timeout_seconds <= 0)):
        raise ValueError('_request_timeout_seconds must be positive.')

    # Backward compatibility check for the new flag cluster_name
    version = versions.get_remote_api_version()
    if (cluster_name is not None) and (version is None or version < 38):
        logger.warning(
            'The flag is ignored because the server does not support it yet.')
    if (cluster_names is not None and version is not None and version
            < server_constants.MIN_REQUEST_EXECUTION_QUIESCENCE_API_VERSION):
        with ux_utils.print_exception_no_traceback():
            raise exceptions.APINotSupportedError(
                'Filtering API requests by multiple cluster names requires '
                'API server version '
                f'{server_constants.MIN_REQUEST_EXECUTION_QUIESCENCE_API_VERSION}'
                ' or newer. Please upgrade the remote server.')

    body = payloads.RequestStatusBody(
        request_ids=request_ids,
        all_status=all_status,
        limit=limit,
        fields=fields,
        cluster_name=cluster_name,
        cluster_names=cluster_names,
        include_request_names=_include_request_names,
        execution_quiescence_candidates_only=(
            _execution_quiescence_candidates_only),
        exact_request_ids=_exact_request_ids,
    )
    use_body = (_use_body or cluster_names is not None or
                _include_request_names is not None or
                _execution_quiescence_candidates_only or _exact_request_ids)
    request_kwargs: dict[str, Any]
    if use_body:
        request_kwargs = {'json': json.loads(body.model_dump_json())}
    else:
        request_kwargs = {
            'params': server_common.request_body_to_params(body),
        }
    request_timeout: float | tuple[int, None]
    if _request_timeout_seconds is None:
        request_timeout = (
            client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS, None)
    else:
        request_timeout = _request_timeout_seconds
    response = server_common.make_authenticated_request(
        'POST' if use_body else 'GET',
        '/api/status/query' if use_body else '/api/status',
        **request_kwargs,
        timeout=request_timeout,
        retry=_retry_on_server_unavailable,
        allow_non_get_without_retry=(use_body and
                                     not _retry_on_server_unavailable))
    server_common.handle_request_error(response)
    return [payloads.RequestPayload(**request) for request in response.json()]


# === API server management APIs ===
@usage_lib.entrypoint
@annotations.client_api
def api_info() -> responses.APIHealthResponse:
    """Gets the server's status, commit and version.

    Returns:
        A dictionary containing the server's status, commit and version.

        .. code-block:: python

            {
                'status': 'healthy',
                'api_version': '1',
                'commit': 'abc1234567890',
                'version': '1.0.0',
                'version_on_disk': '1.0.0',
                'user': {
                    'name': 'test@example.com',
                    'id': '12345abcd',
                },
            }

        Note that user may be None if we are not using an auth proxy.

    """
    response = server_common.make_authenticated_request('GET', '/api/health')
    response.raise_for_status()
    api_health_response = responses.APIHealthResponse(**response.json())

    return api_health_response


@usage_lib.entrypoint
@annotations.client_api
def api_start(
    *,
    deploy: bool = False,
    host: str = '127.0.0.1',
    foreground: bool = False,
    metrics: bool = False,
    metrics_port: int | None = None,
    enable_basic_auth: bool = False,
) -> None:
    """Starts the API server.

    It checks the existence of the API server and starts it if it does not
    exist.

    Args:
        deploy: Whether to deploy the API server, i.e. fully utilize the
            resources of the machine.
        host: The host to deploy the API server. It will be set to 0.0.0.0
            if deploy is True, to allow remote access.
        foreground: Whether to run the API server in the foreground (run in
            the current process).
        metrics: Whether to export metrics of the API server.
        metrics_port: The port to export metrics of the API server.
        enable_basic_auth: Whether to enable basic authentication
            in the API server.
    Returns:
        None
    """
    if deploy:
        host = '0.0.0.0'
    if host not in server_common.AVAILBLE_LOCAL_API_SERVER_HOSTS:
        raise ValueError(f'Invalid host: {host}. Should be one of: '
                         f'{server_common.AVAILBLE_LOCAL_API_SERVER_HOSTS}')
    is_local_api_server = server_common.is_api_server_local()
    if not is_local_api_server:
        server_url = server_common.get_server_url()
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Unable to start local API server: '
                             f'server endpoint is set to {server_url}. '
                             'To start a local API server, remove the endpoint '
                             'from the config file and/or unset the '
                             'SKYPILOT_API_SERVER_ENDPOINT environment '
                             'variable.')
    server_common.check_server_healthy_or_start_fn(deploy, host, foreground,
                                                   metrics, metrics_port,
                                                   enable_basic_auth)
    if foreground:
        # Explain why current process exited
        logger.info('API server is already running:')
    api_server_url = server_common.get_server_url(host)
    logger.info(f'{ux_utils.INDENT_SYMBOL}SkyPilot API server and dashboard: '
                f'{api_server_url}\n'
                f'{ux_utils.INDENT_LAST_SYMBOL}'
                f'View API server logs at: {constants.API_SERVER_LOGS}')


@usage_lib.entrypoint
@annotations.client_api
def api_stop() -> None:
    """Stops the API server.

    It will do nothing if the API server is remotely hosted.

    Returns:
        None
    """
    # Kill the uvicorn process by name: uvicorn sky.server.server:app
    server_url = server_common.get_server_url()
    if not server_common.is_api_server_local():
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(
                f'Cannot kill the API server at {server_url} because it is not '
                f'the default SkyPilot API server started locally.')

    # Acquire the api server creation lock to prevent multiple processes from
    # stopping and starting the API server at the same time.
    with filelock.FileLock(
            os.path.expanduser(constants.API_SERVER_CREATION_LOCK_PATH)):
        # The runtime owns and drains managed-job slot families before its
        # process exits.  The CLI has no cross-process PID authority and must
        # not race that supervisor with an independent process-tree scan.
        found = _local_api_server_running(kill=True)

    if found:
        logger.info(f'{colorama.Fore.GREEN}SkyPilot API server stopped.'
                    f'{colorama.Style.RESET_ALL}')
    else:
        logger.info('SkyPilot API server is not running.')


# Use the same args as `docker logs`
@usage_lib.entrypoint
@annotations.client_api
def api_server_logs(follow: bool = True, tail: int | None = None) -> None:
    """Streams the API server logs.

    Args:
        follow: Whether to follow the logs.
        tail: the number of lines to show from the end of the logs.
            If None, show all logs.

    Returns:
        None
    """
    if server_common.is_api_server_local():
        tail_args = ['-f'] if follow else []
        if tail is None:
            tail_args.extend(['-n', '+1'])
        else:
            tail_args.extend(['-n', f'{tail}'])
        log_path = os.path.expanduser(constants.API_SERVER_LOGS)
        subprocess.run(['tail', *tail_args, f'{log_path}'], check=False)
    else:
        stream_and_get(log_path=constants.API_SERVER_LOGS, tail=tail)


# These public functions historically lived in this module. Keep their
# introspection and pickle identity stable while delegating their implementation.
api_login.__module__ = __name__
api_logout.__module__ = __name__


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(24)
@annotations.client_api
def realtime_slurm_gpu_availability(
        name_filter: str | None = None,
        quantity_filter: int | None = None,
        slurm_cluster_name: str | None = None) -> server_common.RequestId:
    """Gets the real-time Slurm GPU availability.

    Args:
        name_filter: Optional name filter for GPUs.
        quantity_filter: Optional quantity filter for GPUs.
        slurm_cluster_name: Optional Slurm cluster name to filter by.

    Returns:
        The request ID of the Slurm GPU availability request.
    """
    remote_api_version = versions.get_remote_api_version()
    # TODO(kevin): remove this in v0.13.0
    if (slurm_cluster_name is not None and remote_api_version is not None and
            remote_api_version < 27):
        logger.warning(
            'The Slurm cluster filter is not supported in your API server; '
            'the server will ignore it and show all Slurm clusters. '
            'Please upgrade the API server to enable it.')

    body = payloads.SlurmGpuAvailabilityRequestBody(
        slurm_cluster_name=slurm_cluster_name,
        name_filter=name_filter,
        quantity_filter=quantity_filter,
    )
    response = server_common.make_authenticated_request(
        'POST',
        '/slurm_gpu_availability',
        json=json.loads(body.model_dump_json()),
    )
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(24)
@annotations.client_api
def slurm_node_info(
        slurm_cluster_name: str | None = None) -> server_common.RequestId:
    """Gets the resource information for all nodes in the Slurm cluster.

    Returns:
        The request ID of the Slurm node info request.

    Request Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing info
            for a single Slurm node (node_name, partition, node_state,
            gpu_type, total_gpus, free_gpus, vcpu_count, memory_gb).
    """
    body = payloads.SlurmNodeInfoRequestBody(
        slurm_cluster_name=slurm_cluster_name)
    response = server_common.make_authenticated_request(
        'GET',
        '/slurm_node_info',
        json=json.loads(body.model_dump_json()),
    )
    return server_common.get_request_id(response)


# =====================
# = Debug Dump =
# =====================


def _build_client_info() -> dict[str, Any]:
    """Build client-side info for debug dumps."""
    import sky  # pylint: disable=import-outside-toplevel

    # Get configs
    user_config: dict[str, Any] = {}
    merged_config: dict[str, Any] = {}
    try:
        user_config = debug_dump_helpers.redact_config(
            dict(skypilot_config.get_user_config()))
        merged_config = debug_dump_helpers.redact_config(
            dict(skypilot_config.to_dict()))
    except Exception:  # pylint: disable=broad-except
        pass  # Config may not be available

    return {
        'skypilot_version': sky.__version__,
        'skypilot_commit': sky.__commit__,
        'api_version': server_constants.API_VERSION,
        'python_version': platform.python_version(),
        'platform': platform.platform(),
        'user_hash': common_utils.get_user_hash(),
        'environment': {
            k: v
            for k, v in sorted(os.environ.items())
            if k.startswith(('SKYPILOT_', 'SKY_'))
        },
        'user_config': user_config,
        'merged_config': merged_config,
    }


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(46)
@annotations.client_api
def create_debug_dump(
    request_ids: list[str] | None = None,
    cluster_names: list[str] | None = None,
    managed_job_ids: list[int] | None = None,
    recent_minutes: float | None = None,
) -> server_common.RequestId[str]:
    """Create a debug dump for troubleshooting.

    At least one of ``request_ids``, ``cluster_names``, ``managed_job_ids``,
    or ``recent_minutes`` must be provided.

    Args:
        request_ids: List of request IDs or prefixes to include in the
            dump. Prefixes are resolved to all matching request IDs on
            the server.
        cluster_names: List of cluster names to include in the dump.
        managed_job_ids: List of managed job IDs to include in the dump.
        recent_minutes: If specified, include all resources active within
            this many minutes.

    Returns:
        The request ID of the debug dump creation request.

    Request Returns:
        Path to the created zip file on the server.
    """
    body = payloads.CreateDebugDumpBody(
        request_ids=request_ids,
        cluster_names=cluster_names,
        managed_job_ids=managed_job_ids,
        recent_minutes=recent_minutes,
        client_info=_build_client_info(),
    )
    response = server_common.make_authenticated_request(
        'POST',
        '/debug/dump_create',
        json=json.loads(body.model_dump_json()),
    )
    return server_common.get_request_id(response)


@usage_lib.entrypoint
@server_common.check_server_healthy_or_start
@versions.minimal_api_version(46)
@annotations.client_api
def download_debug_dump(dump_filename: str,
                        local_path: str | None = None) -> str:
    """Download a debug dump from the server.

    Args:
        dump_filename: The filename of the dump to download.
        local_path: Local path to save the dump. If None, saves to
            current directory with the original filename.

    Returns:
        Path to the downloaded file.
    """
    response = server_common.make_authenticated_request(
        'GET',
        f'/debug/dump_download/{dump_filename}',
        stream=True,
        timeout=(client_common.API_SERVER_REQUEST_CONNECTION_TIMEOUT_SECONDS,
                 None),
    )

    with response:
        if response.status_code != 200:
            try:
                detail = response.json().get('detail', 'Unknown error')
            except (json.JSONDecodeError, ValueError):
                detail = response.text or f'HTTP {response.status_code}'
            raise exceptions.ClientError(
                f'Failed to download debug dump: {detail}')

        if local_path is None:
            local_path = dump_filename

        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_path
