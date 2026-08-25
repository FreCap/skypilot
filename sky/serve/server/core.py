"""SkyServe core APIs."""
import typing
from typing import Any, Optional

from sky import backends
from sky import exceptions
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.provision import capacity_cache
from sky.serve import constants as serve_constants
from sky.serve import maintenance
from sky.serve import placement_history
from sky.serve import serve_rpc_utils
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve.server import impl
from sky.usage import usage_lib
from sky.utils import controller_utils
from sky.utils import subprocess_utils

if typing.TYPE_CHECKING:
    import grpc

    import sky
else:
    grpc = adaptors_common.LazyImport('grpc')

logger = sky_logging.init_logger(__name__)


def _unavailable_section(reason: str) -> dict[str, Any]:
    return {'available': False, 'reason': reason}


@usage_lib.entrypoint
def up(
    task: 'sky.Task',
    service_name: str | None = None,
    submitted_yaml_content: str | None = None,
) -> tuple[str, str]:
    """Spins up a service.

    Please refer to the sky.cli.serve_up for the document.

    Args:
        task: sky.Task to serve up.
        service_name: Name of the service.

    Returns:
        service_name: str; The name of the service.  Same if passed in as an
            argument.
        endpoint: str; The service endpoint.
    """
    return impl.up(task,
                   service_name,
                   pool=False,
                   submitted_yaml_content=submitted_yaml_content)


@usage_lib.entrypoint
def update(task: Optional['sky.Task'],
           service_name: str,
           mode: serve_utils.UpdateMode = serve_utils.DEFAULT_UPDATE_MODE,
           workers: int | None = None,
           submitted_yaml_content: str | None = None) -> None:
    """Updates an existing service.

    Please refer to the sky.cli.serve_update for the document.

    Args:
        task: sky.Task to update, or None if updating
            the number of workers/replicas.
        service_name: Name of the service.
        mode: Update mode.
        workers: Number of workers/replicas to set for the service when
            task is None.
    """
    return impl.update(task,
                       service_name,
                       mode,
                       pool=False,
                       workers=workers,
                       submitted_yaml_content=submitted_yaml_content)


@usage_lib.entrypoint
def elect_version(service_name: str, version: int, expected_service_hash: str,
                  expected_elected_version: int | None) -> None:
    """Roll out a new generation from an immutable stored version."""
    return impl.elect_version(service_name, version, expected_service_hash,
                              expected_elected_version)


@usage_lib.entrypoint
def set_load_balancer_high_availability(service_name: str, enabled: bool,
                                        expected_service_hash: str) -> None:
    """Change a service's external-LB topology without a model rollout."""
    return impl.set_load_balancer_high_availability(service_name, enabled,
                                                    expected_service_hash)


@usage_lib.entrypoint
# pylint: disable=redefined-builtin
def down(
    service_names: str | list[str] | None = None,
    all: bool = False,
    purge: bool = False,
) -> None:
    """Tears down a service.

    Please refer to the sky.cli.serve_down for the docs.

    Args:
        service_names: Name of the service(s).
        all: Whether to terminate all services.
        purge: Whether to terminate services in a failed status. These services
          may potentially lead to resource leaks.

    Raises:
        sky.exceptions.ClusterNotUpError: if the sky serve controller is not up.
        ValueError: if the arguments are invalid.
        RuntimeError: if failed to terminate the service.
    """
    return impl.down(service_names, all, purge, pool=False)


@usage_lib.entrypoint
def terminate_replica(service_name: str, replica_id: int, purge: bool) -> None:
    """Tears down a specific replica for the given service.

    Args:
        service_name: Name of the service.
        replica_id: ID of replica to terminate.
        purge: Whether to terminate replicas in a failed status. These replicas
          may lead to resource leaks, so we require the user to explicitly
          specify this flag to make sure they are aware of this potential
          resource leak.

    Raises:
        sky.exceptions.ClusterNotUpError: if the sky sere controller is not up.
        RuntimeError: if failed to terminate the replica.
    """
    if maintenance.is_controller_hold_active():
        identity = serve_state.get_service_mode_and_hash(service_name)
        if identity is None or not identity[0]:
            raise RuntimeError(
                'SkyServe replica termination is disabled while the server '
                'controller hold is active.')
    handle = backend_utils.is_controller_accessible(
        controller=controller_utils.Controllers.SKY_SERVE_CONTROLLER,
        stopped_message=
        'No service is running now. Please spin up a service first.',
        non_existent_message='No service is running now. '
        'Please spin up a service first.',
    )

    assert isinstance(handle, backends.CloudVmRayResourceHandle)
    use_legacy = not handle.is_grpc_enabled_with_flag
    stdout = None

    if not use_legacy:
        try:
            stdout = serve_rpc_utils.RpcRunner.terminate_replica(
                handle, service_name, replica_id, purge)
        except exceptions.SkyletMethodNotImplementedError:
            use_legacy = True

    if use_legacy:
        backend = backend_utils.get_backend_from_handle(handle)
        assert isinstance(backend, backends.CloudVmRayBackend)

        code = serve_utils.ServeCodeGen.terminate_replica(
            service_name, replica_id, purge)
        returncode, stdout, stderr = backend.run_on_head(handle,
                                                         code,
                                                         require_outputs=True,
                                                         stream_logs=False,
                                                         separate_stderr=True)

        try:
            subprocess_utils.handle_returncode(
                returncode,
                code,
                'Failed to terminate the replica',
                stderr,
                stream_logs=True)
        except exceptions.CommandError as e:
            raise RuntimeError(e.error_msg) from e

    if stdout is None:
        raise RuntimeError('Replica termination produced no output.')
    sky_logging.print(stdout)


@usage_lib.entrypoint
def status(
    service_names: str | list[str] | None = None,
    summary_only: bool = False,
    include_target_num_replicas: bool | None = None,
    history_hours: int | None = None,
    metadata_only: bool = False,
    include_endpoints: bool = False,
    authorized_owner_user_id: str | None = None,
) -> list[dict[str, Any]]:
    """Gets service statuses.

    If service_names is given, return those services. Otherwise, return all
    services.

    Each returned value has the following fields:

    .. code-block:: python

        {
            'name': (str) service name,
            'active_versions': (List[int]) a list of versions that are active,
            'elected_version': (int) the latest durably selected version; it
              can differ from active_versions while a rollout is converging,
            'controller_job_id': (int) the job id of the controller,
            'uptime': (int) uptime in seconds,
            'status': (sky.ServiceStatus) service status,
            'controller_port': (Optional[int]) controller port,
            'load_balancer_port': (Optional[int]) load balancer port,
            'endpoint': (Optional[str]) load balancer endpoint,
            'policy': (Optional[str]) autoscaling policy description,
            'requested_resources_str': (str) str representation of
              requested resources,
            'load_balancing_policy': (str) load balancing policy name,
            'tls_encrypted': (bool) whether the service is TLS encrypted,
            'recent_request_count': (Optional[int]) requests observed in the
              autoscaler's rolling window,
            'request_window_seconds': (Optional[int]) rolling window size,
            'requests_per_second': (Optional[float]) average request rate in
              that rolling window,
            'in_flight_requests': (Optional[int]) currently in-flight
              requests when concurrency metrics are available,
            'request_queue_depth': (Optional[int]) requests waiting for a
              replica,
            'rejected_requests': (Optional[int]) recent capacity rejections,
            'committed_version': (Optional[int]) latest durably accepted
              service version,
            'applied_version': (Optional[int]) latest version applied to the
              live controller runtime,
            'update_apply_pending': (Optional[bool]) whether the controller is
              still applying a committed version,
            'update_apply_lag_seconds': (Optional[int]) seconds since the
              pending version committed,
            'update_apply_error': (Optional[str]) most recent runtime apply
              error for the pending version,
            'update_apply_failures': (Optional[int]) consecutive apply
              failures for the pending version,
            'quarantined_version': (Optional[int]) latest deterministically
              rejected committed version,
            'quarantined_at': (Optional[float]) quarantine timestamp,
            'quarantine_reason': (Optional[str]) deterministic failure reason,
            'replica_info': (List[Dict[str, Any]]) replica information,
        }

    Each entry in replica_info has the following fields:

    .. code-block:: python

        {
            'replica_id': (int) replica id,
            'name': (str) replica name,
            'status': (sky.serve.ReplicaStatus) replica status,
            'version': (int) replica version,
            'launched_at': (int) timestamp of launched,
            'handle': (Optional[ResourceHandle]) handle of the replica
                cluster. New API servers (>=
                MIN_LAZY_REPLICA_HANDLE_API_VERSION) strip this to ``None``
                on the wire to keep payloads small; callers should read the
                pre-computed string fields below instead. Old servers still
                return a full handle.
            'cloud': (Optional[str]) selected or launched cloud name of the
                replica,
            'region': (Optional[str]) selected or launched region of the
                replica,
            'infra': (Optional[str]) human-readable infra string,
                e.g. ``'aws (us-east-1)'``,
            'resources_str': (Optional[str]) simplified resource string,
            'resources_str_full': (Optional[str]) full resource string with
                accelerator details,
            'hourly_cost': (Optional[float]) current-catalog hourly compute
                estimate for the launched replica resources,
            'hourly_cost_exclusion_reason': (Optional[str]) why the replica
                could not be priced,
            'endpoint': (str) endpoint of the replica,
        }

    For possible service statuses and replica statuses, please refer to
    sky.cli.serve_status.

    Args:
        service_names: a single or a list of service names to query. If None,
            query all services.

    Returns:
        A list of dicts, with each dict containing the information of a service.
        If a service is not found, it will be omitted from the returned list.

    Raises:
        RuntimeError: if failed to get the service status.
        exceptions.ClusterNotUpError: if the sky serve controller is not up.
    """
    kwargs: dict[str, Any] = {}
    if authorized_owner_user_id is not None:
        kwargs['authorized_owner_user_id'] = authorized_owner_user_id
    return impl.status(service_names,
                       pool=False,
                       summary_only=summary_only,
                       metadata_only=metadata_only,
                       include_target_num_replicas=include_target_num_replicas,
                       history_hours=history_hours,
                       include_endpoints=include_endpoints,
                       **kwargs)


def authorized_status(
    service_names: str | list[str] | None = None,
    summary_only: bool = False,
    include_target_num_replicas: bool | None = None,
    history_hours: int | None = None,
    metadata_only: bool = False,
    include_endpoints: bool = False,
    *,
    authorized_owner_user_id: str | None,
) -> list[dict[str, Any]]:
    """Execute status with the authorization scope derived by the API server.

    The distinct durable handler name is a worker-capability fence: an older
    controller cannot claim and accidentally execute this request unscoped.
    """
    return status(
        service_names=service_names,
        summary_only=summary_only,
        include_target_num_replicas=include_target_num_replicas,
        history_hours=history_hours,
        metadata_only=metadata_only,
        include_endpoints=include_endpoints,
        authorized_owner_user_id=authorized_owner_user_id,
    )


@usage_lib.entrypoint
def placement(
        service_name: str,
        hours: int = placement_history.RETENTION_HOURS,
        limit: int = placement_history.DEFAULT_PAGE_SIZE,
        cursor: str | None = None,
        location_limit: int = serve_constants.PLACEMENT_STATE_DEFAULT_PAGE_SIZE,
        location_offset: int = 0,
        location_order_generation: str | None = None,
        authorized_owner_user_id: str | None = None) -> dict[str, Any]:
    """Return bounded placement state for one exact service incarnation."""
    if authorized_owner_user_id is None:
        record = serve_state.get_service_from_name(service_name)
    else:
        record = serve_state.get_service_from_name(
            service_name, owner_user_id=authorized_owner_user_id)
    if record is None or record.get('pool'):
        raise ValueError(f'Service {service_name!r} not found.')
    service_hash = record.get('hash')
    if not isinstance(service_hash, str) or not service_hash:
        return {
            'service_name': service_name,
            'placer_state': _unavailable_section('legacy_service'),
            'capacity_hints': _unavailable_section('legacy_service'),
            'history': _unavailable_section('legacy_service'),
        }

    try:
        placer_state = serve_utils.get_service_placement_state(
            service_name,
            service_hash,
            limit=location_limit,
            offset=location_offset,
            expected_order_generation=location_order_generation)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('Placement-state read failed for %r: %s', service_name, e)
        placer_state = _unavailable_section('controller_unavailable')

    try:
        capacity_hints = capacity_cache.active_service_observations(
            service_name, service_hash)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('Capacity-observation read failed for %r: %s',
                     service_name, e)
        capacity_hints = _unavailable_section('cache_unavailable')

    try:
        history = placement_history.get_history(service_name,
                                                service_hash,
                                                hours=hours,
                                                limit=limit,
                                                cursor=cursor)
    except ValueError:
        raise
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('Placement-history read failed for %r: %s', service_name,
                     e)
        history = _unavailable_section('history_unavailable')

    return {
        'service_name': service_name,
        'placer_state': placer_state,
        'capacity_hints': capacity_hints,
        'history': history,
    }


def authorized_placement(
        service_name: str,
        hours: int = placement_history.RETENTION_HOURS,
        limit: int = placement_history.DEFAULT_PAGE_SIZE,
        cursor: str | None = None,
        location_limit: int = serve_constants.PLACEMENT_STATE_DEFAULT_PAGE_SIZE,
        location_offset: int = 0,
        location_order_generation: str | None = None,
        *,
        authorized_owner_user_id: str | None) -> dict[str, Any]:
    """Execute placement with the API-server-derived authorization scope.

    Its distinct durable handler name prevents older controllers from
    claiming a request whose owner field their payload model would discard.
    """
    return placement(
        service_name=service_name,
        hours=hours,
        limit=limit,
        cursor=cursor,
        location_limit=location_limit,
        location_offset=location_offset,
        location_order_generation=location_order_generation,
        authorized_owner_user_id=authorized_owner_user_id,
    )


ServiceComponentOrStr = str | serve_utils.ServiceComponent


@usage_lib.entrypoint
def tail_logs(
    service_name: str,
    *,
    target: ServiceComponentOrStr,
    replica_id: int | None = None,
    follow: bool = True,
    tail: int | None = None,
) -> None:
    """Tails logs for a service.

    Usage:
        sky.serve.tail_logs(
            service_name,
            target=<component>,
            follow=False, # Optionally, default to True
            # replica_id=3, # Must be specified when target is REPLICA.
        )

    `target` is a enum of sky.serve.ServiceComponent, which can be one of:
        - CONTROLLER
        - LOAD_BALANCER
        - REPLICA
    Pass target as a lower-case string is also supported, e.g.
    target='controller'.
    To use REPLICA, you must specify `replica_id`.

    To tail controller logs:
        # follow default to True
        sky.serve.tail_logs(
            service_name, target=sky.serve.ServiceComponent.CONTROLLER)

    To print replica 3 logs:
        # Pass target as a lower-case string is also supported.
        sky.serve.tail_logs(
            service_name, target='replica',
            follow=False, replica_id=3)

    Raises:
        sky.exceptions.ClusterNotUpError: the sky serve controller is not up.
        ValueError: arguments not valid, or failed to tail the logs.
    """
    return impl.tail_logs(service_name,
                          target=target,
                          replica_id=replica_id,
                          follow=follow,
                          tail=tail,
                          pool=False)


@usage_lib.entrypoint
def sync_down_logs(
    service_name: str,
    *,
    local_dir: str,
    targets: ServiceComponentOrStr | list[ServiceComponentOrStr] | None = None,
    replica_ids: list[int] | None = None,
    tail: int | None = None,
) -> str:
    """Sync down logs from the controller for the given service.

    This function is called by the server endpoint. It gathers logs from the
    controller, load balancer, and/or replicas and places them in a directory
    under the user's log space on the API server filesystem.

    Args:
        service_name: The name of the service to download logs from.
        local_dir: The local directory to save the logs to.
        targets: Which component(s) to download logs for. If None or empty,
            means download all logs (controller, load-balancer, all replicas).
            Can be a string (e.g. "controller"), or a `ServiceComponent` object,
            or a list of them for multiple components. Currently accepted
            values:
                - "controller"/ServiceComponent.CONTROLLER
                - "load_balancer"/ServiceComponent.LOAD_BALANCER
                - "replica"/ServiceComponent.REPLICA
        replica_ids: The list of replica IDs to download logs from, specified
            when target includes `ServiceComponent.REPLICA`. If target includes
            `ServiceComponent.REPLICA` but this is None/empty, logs for all
            replicas will be downloaded.

    Returns:
        A dict mapping component names to local paths where the logs were synced
        down to.

    Raises:
        RuntimeError: If fails to gather logs or fails to rsync from the
          controller.
        sky.exceptions.ClusterNotUpError: If the controller is not up.
        ValueError: Arguments not valid.
    """
    return impl.sync_down_logs(service_name,
                               local_dir=local_dir,
                               targets=targets,
                               replica_ids=replica_ids,
                               tail=tail,
                               pool=False)
