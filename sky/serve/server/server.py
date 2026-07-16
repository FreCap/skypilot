"""Rest APIs for SkyServe."""

import pathlib

import fastapi

from sky import sky_logging
from sky.serve import serve_state
from sky.serve.server import core
from sky.server import stream_utils
from sky.server.blob import blob_storage as bs
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.server.requests import requests as api_requests
from sky.skylet import constants
from sky.users import permission
from sky.users import rbac
from sky.utils import common
from sky.utils import debug_dump_helpers

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
            detail='Only admins can view or elect service versions.')


def _service_version_history(service_name: str,
                             record: dict | None = None) -> dict:
    """Return redacted immutable versions and current rollout state."""
    if record is None:
        record = serve_state.get_service_from_name(service_name)
    if record is None or record.get('pool'):
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    elected_version = record.get('elected_version')
    active_versions = record.get('active_versions', [])
    versions = [{
        'version': version,
        'yaml_content': debug_dump_helpers.redact_task_yaml(yaml_content),
        'elected': version == elected_version,
        'active': version in active_versions,
    } for version, yaml_content in reversed(
        serve_state.get_version_yaml_contents(service_name).items())]
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
    _require_admin(request)
    record = serve_state.get_service_from_name(service_name)
    if record is None or record.get('pool'):
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service not found.')
    if not record.get('hash'):
        raise fastapi.HTTPException(
            status_code=409,
            detail='Service has no durable incarnation identity.')
    history = _service_version_history(service_name, record)
    if election_body.version < 1:
        raise fastapi.HTTPException(status_code=422,
                                    detail='Version must be positive.')
    selected = next((version for version in history['versions']
                     if version['version'] == election_body.version), None)
    if selected is None:
        raise fastapi.HTTPException(status_code=404,
                                    detail='Service version not found.')
    if selected['elected']:
        raise fastapi.HTTPException(status_code=409,
                                    detail='Version is already elected.')
    await executor.schedule_request_async(
        request_id=request.state.request_id,
        request_name=request_names.RequestName.SERVE_UPDATE,
        request_body=payloads.ServeElectVersionBody(
            service_name=service_name,
            version=election_body.version,
            expected_service_hash=record['hash']),
        func=core.elect_version,
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


@router.post('/logs')
async def tail_logs(
    request: fastapi.Request, log_body: payloads.ServeLogsBody,
    background_tasks: fastapi.BackgroundTasks
) -> fastapi.responses.StreamingResponse:
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
    return stream_utils.stream_response_for_long_request(
        request_id=request_task.request_id,
        logs_path=request_task.log_path,
        background_tasks=background_tasks,
        kill_request_on_disconnect=False,
    )


@router.post('/sync-down-logs')
async def download_logs(
    request: fastapi.Request,
    download_logs_body: payloads.ServeDownloadLogsBody,
) -> None:
    user_hash = download_logs_body.env_vars[constants.USER_ID_ENV_VAR]
    timestamp = sky_logging.get_run_timestamp()
    logs_dir_on_api_server = (
        pathlib.Path(bs.get_blob_storage().download_tmp_dir(user_hash)) /
        'service' / f'{download_logs_body.service_name}_{timestamp}')
    logs_dir_on_api_server.expanduser().mkdir(parents=True, exist_ok=True)
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
