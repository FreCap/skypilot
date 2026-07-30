"""Rest APIs for SkyServe."""

import asyncio

import fastapi

from sky import sky_logging
from sky.serve import serve_state
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
            'policy':
                (spec.autoscaling_policy_str() if spec is not None else None),
            'elected': version == elected_version,
            'active': version in active_versions,
        })
    return {
        'service_name': service_name,
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


@router.get('/{service_name}/versions')
def version_history(request: fastapi.Request, service_name: str) -> dict:
    """Return immutable version history to an administrator."""
    _require_admin(request)
    return _service_version_history(service_name)


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
