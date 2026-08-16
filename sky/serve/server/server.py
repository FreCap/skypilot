"""Rest APIs for SkyServe."""

import asyncio
import enum

import fastapi

from sky import sky_logging
from sky.serve import demand_state
from sky.serve import kubernetes_identity
from sky.serve import serve_dashboard
from sky.serve import serve_history
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve.server import core
from sky.server import common as server_common
from sky.server import stream_utils
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.server.requests import requests as api_requests
from sky.users import permission
from sky.users import rbac
from sky.utils import common
from sky.utils import debug_dump_helpers
from sky.utils import yaml_utils

logger = sky_logging.init_logger(__name__)
router = fastapi.APIRouter()


class StatusHistorySection(str, enum.Enum):
    REQUESTS = 'requests'
    REPLICAS = 'replicas'
    PREDICTION = 'prediction'
    AUTOSCALER = 'autoscaler'


class ReplicaScope(str, enum.Enum):
    CURRENT_OR_UNCERTAIN = serve_dashboard.CURRENT_OR_UNCERTAIN_SCOPE
    PAST_ATTEMPTS = serve_dashboard.PAST_ATTEMPTS_SCOPE


def _require_admin(request: fastapi.Request) -> None:
    """Reject non-admin callers while preserving local single-user mode."""
    auth_user = request.state.auth_user
    if auth_user is None:
        return
    roles = permission.permission_service.get_user_roles(auth_user.id)
    if rbac.RoleName.ADMIN.value not in roles:
        raise fastapi.HTTPException(
            status_code=403,
            detail='Only admins can manage service versions and load '
            'balancers.')


def _redact_version_yaml(yaml_content: str | None,
                         stable_order: bool = False) -> str | None:
    if yaml_content is None:
        return None
    if stable_order:
        try:
            documents = list(yaml_utils.safe_load_all(yaml_content))
            config = (documents[0] if len(documents) == 1 and
                      isinstance(documents[0], dict) else documents)
            yaml_content = yaml_utils.dump_yaml_str(config, sort_keys=True)
        except Exception:  # pylint: disable=broad-except
            pass
    return debug_dump_helpers.redact_task_yaml(yaml_content)


def _service_version_history(service_name: str) -> dict:
    """Return redacted immutable versions and current rollout state."""
    record = serve_state.get_service_from_name(service_name)
    if record is None or record.get('pool'):
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    elected_version = record.get('elected_version')
    active_versions = record.get('active_versions', [])
    versions = []
    for version_record in reversed(
            serve_state.get_version_records(service_name)):
        version = version_record['version']
        spec = version_record['spec']
        versions.append({
            'version': version,
            'submitted_yaml_content': _redact_version_yaml(
                version_record['submitted_yaml_content']),
            'compiled_yaml_content': _redact_version_yaml(
                version_record['yaml_content'], stable_order=True),
            'created_at': version_record['created_at'],
            'created_by': version_record['created_by'],
            'quarantined_at': version_record['quarantined_at'],
            'quarantine_reason': version_record['quarantine_reason'],
            'controller_job_identity':
                kubernetes_identity.validate_controller_job_projection(
                    version_record.get('controller_job_projection')),
            'controller_work_cache':
                kubernetes_identity.validate_controller_work_cache_projection(
                    version_record.get('controller_work_cache')),
            'worker_placement_identities':
                kubernetes_identity.validate_worker_placement_projections(
                    version_record.get('worker_placement_projections')),
            'policy':
                (spec.autoscaling_policy_str() if spec is not None else None),
            'elected': version == elected_version,
            'active': version in active_versions,
        })
    return {
        'service_name': service_name,
        'placement_projection_protocol_version':
            kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION,
        'elected_version': elected_version,
        'active_versions': active_versions,
        'versions': versions,
    }


@router.post('/up')
async def up(
    request: fastapi.Request,
    up_body: payloads.ServeUpBody,
) -> None:
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_UP,
        request_body=up_body,
        func=core.up,
        schedule_type=api_requests.ScheduleType.LONG,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )


@router.post('/update')
async def update(
    request: fastapi.Request,
    update_body: payloads.ServeUpdateBody,
) -> None:
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_UPDATE,
        request_body=update_body,
        func=core.update,
        schedule_type=api_requests.ScheduleType.SHORT,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )


@router.post('/_internal/{service_name}/ordinary-launch-binding',
             include_in_schema=False)
async def set_ordinary_launch_binding_mode(
        request: fastapi.Request,
        service_name: str,
        mode: str = fastapi.Body(...),
        expected_service_hash: str = fastapi.Body(..., min_length=1),
        expected_binding_epoch: int = fastapi.Body(..., ge=0),
) -> dict:
    """Run an explicit admin-only legacy/bound transition on one service."""
    _require_admin(request)
    if mode not in ('legacy', 'bound'):
        raise fastapi.HTTPException(status_code=422,
                                    detail='mode must be legacy or bound.')
    try:
        return await asyncio.to_thread(
            serve_utils.set_ordinary_launch_binding_mode_encoded,
            service_name,
            mode,
            expected_service_hash,
            expected_binding_epoch,
        )
    except ValueError as error:
        raise fastapi.HTTPException(status_code=404,
                                    detail=str(error)) from error
    except RuntimeError as error:
        raise fastapi.HTTPException(status_code=409,
                                    detail=str(error)) from error


@router.get('/{service_name}/versions')
def version_history(request: fastapi.Request, service_name: str) -> dict:
    """Return immutable version history to an administrator."""
    _require_admin(request)
    return _service_version_history(service_name)


@router.get('/{service_name}/history')
async def status_history(
    service_name: str,
    expected_service_hash: str = fastapi.Query(min_length=1),
    hours: int = fastapi.Query(default=1,
                               ge=1,
                               le=serve_history.RETENTION_HOURS),
    section: list[StatusHistorySection] = fastapi.Query(
        default=list(StatusHistorySection)),
) -> dict:
    """Read selected persisted history without contacting the controller."""
    requested_sections = {item.value for item in section}
    if not await asyncio.to_thread(serve_utils.is_consolidation_mode):
        return serve_history.unavailable_status_history('non_consolidated',
                                                        requested_sections)
    service = await asyncio.to_thread(serve_state.get_service_status_snapshot,
                                      service_name)
    if service is None or service.get('pool'):
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    if service.get('hash') != expected_service_hash:
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service incarnation changed. Refresh and retry.')
    history = await asyncio.to_thread(
        serve_history.get_status_history,
        service_name,
        hours=hours,
        expected_service_hash=expected_service_hash,
        sections=requested_sections,
    )
    reason = history.get('reason')
    if reason == 'service_not_found':
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    if reason == 'service_hash_mismatch':
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service incarnation changed. Refresh and retry.')
    return history


@router.get('/{service_name}/demand')
async def current_demand(
        service_name: str,
        expected_service_hash: str = fastapi.Query(min_length=1),
) -> dict:
    """Read current request telemetry without contacting the controller."""
    if not await asyncio.to_thread(serve_utils.is_consolidation_mode):
        summary = demand_state.unavailable_request_summary('non_consolidated')
        return {
            'service_name': service_name,
            'service_hash': expected_service_hash,
            **summary,
        }
    service = await asyncio.to_thread(serve_state.get_service_status_snapshot,
                                      service_name)
    if service is None or service.get('pool'):
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    if service.get('hash') != expected_service_hash:
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service incarnation changed. Refresh and retry.')
    summary = await asyncio.to_thread(demand_state.get_request_summary,
                                      service_name, expected_service_hash)
    if summary.get(
            'request_telemetry_reason') == 'service_incarnation_mismatch':
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service incarnation changed. Refresh and retry.')
    return {
        'service_name': service_name,
        'service_hash': expected_service_hash,
        **summary,
    }


@router.get('/replica-summaries')
async def replica_summaries(
        service_name: list[str] | None = fastapi.Query(default=None),) -> dict:
    """Read compact persisted counts without contacting a controller."""
    if not await asyncio.to_thread(serve_utils.is_consolidation_mode):
        return serve_dashboard.unavailable_replica_summaries('non_consolidated')
    return await asyncio.to_thread(serve_dashboard.get_replica_summaries,
                                   service_name)


@router.get('/{service_name}/replicas')
async def replica_page(
    service_name: str,
    expected_service_hash: str = fastapi.Query(min_length=1),
    scope: ReplicaScope = fastapi.Query(),
    limit: int = fastapi.Query(default=50, ge=1, le=100),
    cursor: str | None = fastapi.Query(default=None,
                                       min_length=1,
                                       max_length=4096),
) -> dict:
    """Read one bounded persisted replica page without controller work."""
    scope_value = scope.value
    if not await asyncio.to_thread(serve_utils.is_consolidation_mode):
        return serve_dashboard.unavailable_replica_page(service_name,
                                                        expected_service_hash,
                                                        scope_value,
                                                        'non_consolidated')
    try:
        return await asyncio.to_thread(
            serve_dashboard.get_replica_page,
            service_name,
            expected_service_hash,
            scope_value,
            limit,
            cursor,
        )
    except serve_dashboard.ServiceNotFoundError as exc:
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.') from exc
    except (serve_dashboard.ServiceHashMismatchError,
            serve_dashboard.ReplicaCursorMismatchError) as exc:
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service incarnation or replica cursor changed. Refresh '
            'and retry.') from exc
    except serve_dashboard.InvalidReplicaCursorError as exc:
        raise fastapi.HTTPException(status_code=422,
                                    detail='Invalid replica cursor.') from exc


@router.get('/{service_name}/pricing')
async def service_pricing(
        service_name: str,
        expected_service_hash: str = fastapi.Query(min_length=1),
        replica_id: list[int] | None = fastapi.Query(default=None),
) -> dict:
    """Read bounded persisted pricing without controller or executor work."""
    if replica_id is not None:
        # Validate raw cardinality before deduplication and before the
        # topology check so every deployment exposes the same wire contract.
        if len(replica_id) > serve_dashboard.MAX_PRICING_REPLICA_IDS:
            raise fastapi.HTTPException(
                status_code=422,
                detail='At most 100 replica IDs may be requested.')
        if any(item < 1 or item > serve_dashboard.MAX_PRICING_REPLICA_ID
               for item in replica_id):
            raise fastapi.HTTPException(
                status_code=422,
                detail='Replica IDs must be positive PostgreSQL INTEGER '
                'values.')
    if not await asyncio.to_thread(serve_utils.is_consolidation_mode):
        return serve_dashboard.unavailable_service_pricing(
            service_name, expected_service_hash, 'non_consolidated')
    try:
        return await asyncio.to_thread(
            serve_dashboard.get_service_pricing,
            service_name,
            expected_service_hash,
            replica_id,
        )
    except serve_dashboard.ServiceNotFoundError as exc:
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.') from exc
    except serve_dashboard.ServiceHashMismatchError as exc:
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service incarnation changed. Refresh and retry.') from exc
    except ValueError as exc:
        raise fastapi.HTTPException(status_code=422,
                                    detail='Invalid pricing query.') from exc


@router.post('/{service_name}/versions/elect')
async def elect_version(
    request: fastapi.Request,
    service_name: str,
    election_body: payloads.ServeVersionElectionBody,
) -> None:
    """Safely roll out a new generation from a stored version."""
    await asyncio.to_thread(_require_admin, request)
    record = await asyncio.to_thread(serve_state.get_service_from_name,
                                     service_name)
    if record is None or record.get('pool'):
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    if not record.get('hash'):
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service has no durable incarnation identity.')
    if await asyncio.to_thread(serve_state.get_yaml_content, service_name,
                               election_body.version) is None:
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service version not found.')
    if record.get('elected_version') == election_body.version:
        raise fastapi.HTTPException(status_code=409,
                                    detail='Version is already elected.')
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_UPDATE,
        request_body=payloads.ServeElectVersionBody(
            service_name=service_name,
            version=election_body.version,
            expected_service_hash=record['hash'],
            expected_elected_version=record.get('elected_version')),
        func=core.elect_version,
        schedule_type=api_requests.ScheduleType.SHORT,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )


@router.post('/{service_name}/load-balancer/high-availability')
async def set_load_balancer_high_availability(
    request: fastapi.Request,
    service_name: str,
    body: payloads.ServeLoadBalancerHighAvailabilityBody,
) -> None:
    """Change only the external-LB topology for an existing service."""
    await asyncio.to_thread(_require_admin, request)
    record = await asyncio.to_thread(serve_state.get_service_from_name,
                                     service_name)
    if record is None or record.get('pool'):
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    if not record.get('hash'):
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service has no durable incarnation identity.')
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_LB_HIGH_AVAILABILITY,
        request_body=payloads.ServeSetLoadBalancerHighAvailabilityBody(
            service_name=service_name,
            enabled=body.enabled,
            expected_service_hash=record['hash']),
        func=core.set_load_balancer_high_availability,
        schedule_type=api_requests.ScheduleType.SHORT,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )


@router.post('/down')
async def down(
    request: fastapi.Request,
    down_body: payloads.ServeDownBody,
) -> None:
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_DOWN,
        request_body=down_body,
        func=core.down,
        schedule_type=api_requests.ScheduleType.SHORT,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )


@router.post('/terminate-replica')
async def terminate_replica(
    request: fastapi.Request,
    terminate_replica_body: payloads.ServeTerminateReplicaBody,
) -> None:
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_TERMINATE_REPLICA,
        request_body=terminate_replica_body,
        func=core.terminate_replica,
        schedule_type=api_requests.ScheduleType.SHORT,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )


@router.post('/status')
async def status(
    request: fastapi.Request,
    status_body: payloads.ServeStatusBody,
) -> None:
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_STATUS,
        request_body=status_body,
        func=core.status,
        schedule_type=api_requests.ScheduleType.SHORT,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )


@router.post('/placement')
async def placement(
    request: fastapi.Request,
    placement_body: payloads.ServePlacementBody,
) -> None:
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_PLACEMENT,
        request_body=placement_body,
        func=core.placement,
        schedule_type=api_requests.ScheduleType.SHORT,
        auth_user=request.state.auth_user,
    )


@router.post('/logs')
async def tail_logs(
    request: fastapi.Request, log_body: payloads.ServeLogsBody,
    background_tasks: fastapi.BackgroundTasks
) -> fastapi.responses.StreamingResponse:
    stream_utils.ensure_request_log_storage_available()
    kill_request_on_disconnect = False
    if executor.api_process_execution_enabled():
        executor.check_request_thread_executor_available()
        request_task = await executor.prepare_request_async(
            request_id=request.state.request_id,
            request_name=request_names.RequestName.SERVE_LOGS,
            request_body=log_body,
            func=core.tail_logs,
            schedule_type=api_requests.ScheduleType.SHORT,
            request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
            auth_user=request.state.auth_user,
        )
        task = executor.execute_request_in_coroutine(request_task)
        # Cancel the coroutine after the request is done or client disconnects
        background_tasks.add_task(task.cancel)
    else:
        await executor.schedule_request_async(
            request_id=request.state.request_id,
            request_name=request_names.RequestName.SERVE_LOGS,
            request_body=log_body,
            func=core.tail_logs,
            schedule_type=api_requests.ScheduleType.SHORT,
            request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
            auth_user=request.state.auth_user,
        )
        request_task = await api_requests.get_request_async(
            request.state.request_id)
        assert request_task is not None
        kill_request_on_disconnect = True
    return stream_utils.stream_response_for_long_request(
        request_id=request_task.request_id,
        logs_path=request_task.log_path,
        background_tasks=background_tasks,
        kill_request_on_disconnect=kill_request_on_disconnect,
    )


@router.post('/sync-down-logs')
async def download_logs(
    request: fastapi.Request,
    download_logs_body: payloads.ServeDownloadLogsBody,
) -> None:
    user_hash = server_common.get_request_user_id(request, download_logs_body)
    timestamp = sky_logging.get_run_timestamp()
    download_tmp = await asyncio.to_thread(
        server_common.prepare_download_tmp_dir, user_hash)
    logs_dir_on_api_server = (download_tmp / 'service' /
                              f'{download_logs_body.service_name}_{timestamp}')
    await asyncio.to_thread(logs_dir_on_api_server.mkdir,
                            parents=True,
                            exist_ok=True)
    # We should reuse the original request body, so that the env vars, such as
    # user hash, are kept the same.
    download_logs_body.local_dir = str(logs_dir_on_api_server)
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_SYNC_DOWN_LOGS,
        request_body=download_logs_body,
        func=core.sync_down_logs,
        schedule_type=api_requests.ScheduleType.SHORT,
        request_cluster_name=common.SKY_SERVE_CONTROLLER_NAME,
        auth_user=request.state.auth_user,
    )
