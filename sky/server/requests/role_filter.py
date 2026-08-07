"""Role-aware body filters for the SkyPilot API server.

This module provides per-endpoint shims that mutate incoming request
bodies when the caller has the strictly-read-only `viewer` role.  The
viewer endpoint allowlist (in `sky.users.rbac`) is enough to keep
viewers off write endpoints, but a handful of "ambiguous" endpoints
have body fields that swing the action between read and write:

  * `POST /status`: `include_credentials` returns SSH private keys;
    `refresh` queries clouds and mutates state.db.
  * `POST /jobs/queue`, `/jobs/queue/v2`, `/jobs/logs`,
    `/jobs/download_logs`: `refresh` restarts the jobs controller.
  * `GET /volumes`: `refresh` queries cloud volume state.

For viewers, these fields are forced to their read-only / no-side-
effect values *before* the handler runs.  Non-viewer callers see no
behaviour change.

The viewer shims are wired into FastAPI as `Depends()` dependencies so they
can mutate parsed pydantic bodies in-place; standard Starlette middlewares run
before body parsing.  The pod-config guard runs at the common request enqueue
boundary so it covers every queued endpoint and every task submission path.
"""

from typing import Any

import fastapi

from sky import models
from sky.server.requests import payloads
from sky.users import permission
from sky.users import rbac
from sky.utils import common as common_lib
from sky.utils import yaml_utils


def _config_has_pod_config(config: Any) -> bool:
    if isinstance(config, dict):
        if 'pod_config' in config:
            return True
        return any(_config_has_pod_config(child) for child in config.values())
    if isinstance(config, list):
        return any(_config_has_pod_config(child) for child in config)
    return False


def _task_yaml_has_pod_config(task_yaml: str) -> bool:
    try:
        documents = yaml_utils.read_yaml_all_str(task_yaml)
    except Exception:  # pylint: disable=broad-except
        # The normal task parser will report malformed YAML. This guard only
        # decides whether an otherwise valid request needs admin privileges.
        return False

    def _visit(value: Any) -> bool:
        if isinstance(value, dict):
            for key, child in value.items():
                # User-authored YAML carries overrides under `config`; SDK
                # serialization stores the same overrides on Resources.
                if (key in ('config', '_cluster_config_overrides') and
                        _config_has_pod_config(child)):
                    return True
                if _visit(child):
                    return True
        elif isinstance(value, list):
            return any(_visit(child) for child in value)
        return False

    return any(_visit(document) for document in documents)


def reject_non_admin_pod_config(
    auth_user: models.User | None,
    request_body: payloads.RequestBody,
) -> None:
    """Reject raw per-request pod specs from authenticated non-admins.

    A Kubernetes/SSH ``pod_config`` is an arbitrary pod-spec escape hatch: it
    can select service accounts, mount host paths, or request privileged pod
    settings. Server-owned config remains available to every request; only
    client/task overrides require an admin caller.
    """
    if auth_user is None:
        # Local and internal controller calls do not cross a multi-user auth
        # boundary. Their server-side configuration remains trusted.
        return

    task_yamls = [
        value for field in ('task', 'dag') if isinstance((
            value := getattr(request_body, field, None)), str) and value
    ]
    has_pod_config = _config_has_pod_config(
        request_body.override_skypilot_config) or any(
            _task_yaml_has_pod_config(task_yaml) for task_yaml in task_yamls)
    if not has_pod_config:
        return

    roles = permission.permission_service.get_user_roles(auth_user.id)
    if rbac.RoleName.ADMIN.value in roles:
        return
    raise fastapi.HTTPException(
        status_code=403,
        detail=('Only admins can set kubernetes.pod_config or ssh.pod_config '
                'in client or task configuration.'),
    )


def _is_viewer(request: fastapi.Request) -> bool:
    """Return True if the authenticated caller has the viewer role.

    Uses the in-memory Casbin enforcer state (no DB roundtrip),
    matching the perf pattern in
    `PermissionService.check_endpoint_permission`.
    """
    auth_user = request.state.auth_user
    if auth_user is None:
        return False
    # Trust the in-memory grouping policy; same source the middleware
    # already consulted to gate this request to here.
    enforcer = permission.permission_service._ensure_enforcer()  # pylint: disable=protected-access
    roles = enforcer.get_roles_for_user(auth_user.id)
    # Admin wins over viewer when both roles are present.
    return (rbac.RoleName.VIEWER.value in roles and
            rbac.RoleName.ADMIN.value not in roles)


def force_viewer_status_body(
    request: fastapi.Request,
    status_body: payloads.StatusBody = fastapi.Body(
        default_factory=payloads.StatusBody),
) -> payloads.StatusBody:
    """Strip side-effecting fields from `POST /status` for viewers.

    Forces:
      * `refresh = NONE` — viewers cannot trigger cloud refresh or DB
        mutations like cluster status updates.
      * `include_credentials = False` — viewers cannot retrieve SSH
        private keys (which would also write the keys to disk if
        missing, see backend_utils.create_ssh_key_files_from_db).
    """
    if _is_viewer(request):
        status_body.refresh = common_lib.StatusRefreshMode.NONE
        status_body.include_credentials = False
    return status_body


def force_viewer_jobs_queue_body(
    request: fastapi.Request,
    jobs_queue_body: payloads.JobsQueueBody,
) -> payloads.JobsQueueBody:
    """Strip `refresh` from `/jobs/queue` for viewers."""
    if _is_viewer(request):
        jobs_queue_body.refresh = False
    return jobs_queue_body


def force_viewer_jobs_queue_v2_body(
    request: fastapi.Request,
    jobs_queue_body_v2: payloads.JobsQueueV2Body,
) -> payloads.JobsQueueV2Body:
    """Strip `refresh` from `/jobs/queue/v2` for viewers."""
    if _is_viewer(request):
        jobs_queue_body_v2.refresh = False
    return jobs_queue_body_v2


def force_viewer_jobs_logs_body(
    request: fastapi.Request,
    jobs_logs_body: payloads.JobsLogsBody,
) -> payloads.JobsLogsBody:
    """Strip `refresh` from `/jobs/logs` for viewers."""
    if _is_viewer(request):
        jobs_logs_body.refresh = False
    return jobs_logs_body


def force_viewer_jobs_download_logs_body(
    request: fastapi.Request,
    jobs_download_logs_body: payloads.JobsDownloadLogsBody,
) -> payloads.JobsDownloadLogsBody:
    """Strip `refresh` from `/jobs/download_logs` for viewers."""
    if _is_viewer(request):
        jobs_download_logs_body.refresh = False
    return jobs_download_logs_body


def force_viewer_volume_refresh(
    request: fastapi.Request,
    refresh: bool = False,
) -> bool:
    """Strip `refresh` from `GET /volumes` (a query param, not a body)."""
    if _is_viewer(request):
        return False
    return refresh
