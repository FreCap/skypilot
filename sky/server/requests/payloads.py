"""Payloads for the Sky API requests.

All the payloads that will be used between the client and server communication
must be defined here to make sure it get covered by our API compatbility tests.

Compatibility note:
- Adding a new body for new API is compatible as long as the SDK method using
  the new API is properly decorated with `versions.minimal_api_version`.
- Adding a new field with default value to an existing body is compatible at
  API level, but the business logic must handle the case where the field is
  not proccessed by an old version of remote client/server. This can usually
  be done by checking `versions.get_remote_api_version()`.
- Other changes are not compatible at API level, so must be handled specially.
  A common pattern is to keep both the old and new version of the body and
  checking `versions.get_remote_api_version()` to decide which body to use. For
  example, say we refactor the `LaunchBody`, the original `LaunchBody` must be
  kept in the codebase and the new body should be added via `LaunchBodyV2`.
  Then if the remote runs in an old version, the local code should still send
  `LaunchBody` to keep the backward compatibility. `LaunchBody` can be removed
  later when constants.MIN_COMPATIBLE_API_VERSION is updated to a version that
  supports `LaunchBodyV2`

Also refer to sky.server.constants.MIN_COMPATIBLE_API_VERSION and the
sky.server.versions module for more details.
"""
import os
import typing
from typing import Any

from sky import admin_policy
from sky import serve
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.serve import constants as serve_constants
from sky.server import common
from sky.skylet import autostop_lib
from sky.skylet import constants
from sky.usage import constants as usage_constants
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import common as common_lib
from sky.utils import common_utils
from sky.utils import registry

if typing.TYPE_CHECKING:
    import pydantic
else:
    pydantic = adaptors_common.LazyImport('pydantic')

logger = sky_logging.init_logger(__name__)

# These non-skypilot environment variables will be updated from the local
# environment on each request when running a local API server.
# We should avoid adding variables here, but we should include credential-
# related variables.
EXTERNAL_LOCAL_ENV_VARS = [
    # Allow overriding the AWS authentication.
    'AWS_PROFILE',
    'AWS_DEFAULT_PROFILE',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_SESSION_TOKEN',
    # Allow overriding the Azure authentication.
    'AZURE_CLIENT_ID',
    'AZURE_CLIENT_SECRET',
    'AZURE_TENANT_ID',
    'AZURE_SUBSCRIPTION_ID',
    # Allow overriding the GCP authentication.
    'GOOGLE_APPLICATION_CREDENTIALS',
    # Allow overriding the kubeconfig.
    'KUBECONFIG',
]

# Platform capabilities must come from the server deployment, never a client
# process or a hand-crafted request body. The external-LB variable predates the
# SKYPILOT_SERVER_ naming convention, so classify it explicitly while keeping
# the existing name compatible across rolling chart/image upgrades.
_SERVER_OWNED_ENV_VARS = frozenset({
    serve_constants.EXTERNAL_LB_ENABLED_ENV_VAR,
})


def remove_server_owned_env_vars(env_vars: dict[str, str]) -> None:
    """Remove deployment-owned variables from a client environment in place."""
    for env_var in tuple(env_vars):
        if (env_var.startswith(constants.SKYPILOT_SERVER_ENV_VAR_PREFIX) or
                env_var in _SERVER_OWNED_ENV_VARS):
            env_vars.pop(env_var, None)


def request_body_env_vars() -> dict:
    env_vars = {}
    for env_var in os.environ:
        if (env_var.startswith(constants.SKYPILOT_ENV_VAR_PREFIX) and
                not env_var.startswith(
                    constants.SKYPILOT_SERVER_ENV_VAR_PREFIX)):
            env_vars[env_var] = os.environ[env_var]
        if common.is_api_server_local() and env_var in EXTERNAL_LOCAL_ENV_VARS:
            env_vars[env_var] = os.environ[env_var]
    env_vars[constants.USER_ID_ENV_VAR] = common_utils.get_user_hash()
    env_vars[constants.USER_ENV_VAR] = common_utils.get_local_user_name()
    env_vars[
        usage_constants.USAGE_RUN_ID_ENV_VAR] = usage_lib.messages.usage.run_id
    # Send client user hash for basic auth at API server case, so the server
    # can include it in its own usage report.
    if common.basic_auth_enabled and common.client_user_hash is not None:
        env_vars[constants.CLIENT_USER_HASH_ENV_VAR] = common.client_user_hash
    if not common.is_api_server_local():
        # Used in job controller, for local API server, keep the
        # SKYPILOT_CONFIG env var to use the config for the managed job.
        env_vars.pop(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
    # Remove the path to config file, as the config content is included in the
    # request body and will be merged with the config on the server side.
    env_vars.pop(skypilot_config.ENV_VAR_GLOBAL_CONFIG, None)
    env_vars.pop(skypilot_config.ENV_VAR_PROJECT_CONFIG, None)
    # Remove the config related env vars, as the client config override
    # should be passed in the request body.
    # Any new environment variables that are server-specific should
    # use SKYPILOT_SERVER_ENV_VAR_PREFIX.
    env_vars.pop(constants.ENV_VAR_DB_CONNECTION_URI, None)
    # Remove the in-cluster context name - this is only meaningful for the
    # local Kubernetes environment and should not be forwarded to the server,
    # which has its own cluster context configuration.
    env_vars.pop(kubernetes_adaptor.IN_CLUSTER_CONTEXT_NAME_ENV_VAR, None)
    remove_server_owned_env_vars(env_vars)
    return env_vars


def get_override_skypilot_config_from_client() -> dict[str, Any]:
    """Returns the override configs from the client."""
    if annotations.is_on_api_server:
        return {}
    config = skypilot_config.to_dict()
    # Remove the API server config, as we should not specify the SkyPilot
    # server endpoint on the server side. This avoids the warning at
    # server-side.
    config.pop_nested(('api_server',), default_value=None)
    # Remove the admin policy, as the policy has been applied on the client
    # side.
    config.pop_nested(('admin_policy',), default_value=None)
    return config


def get_override_skypilot_config_path_from_client() -> str | None:
    """Returns the override config path from the client."""
    if annotations.is_on_api_server:
        return None
    # Currently, we don't need to check if the client-side config
    # has been overridden because we only deal with cases where
    # client has a project-level config/changed config and the
    # api server has a different config.
    return skypilot_config.loaded_config_path_serialized()


class BasePayload(pydantic.BaseModel):
    """The base payload for the SkyPilot API."""
    # Ignore extra fields in the request body, which is useful for backward
    # compatibility. The difference with `allow` is that `ignore` will not
    # include the unknown fields when dump the model, i.e., we can add new
    # fields to the request body without breaking the existing old API server
    # where the handler function does not accept the new field in function
    # signature.
    model_config = pydantic.ConfigDict(extra='ignore')


class RequestBody(BasePayload):
    """The request body for the SkyPilot API."""
    env_vars: dict[str, str] = {}
    entrypoint: str = ''
    entrypoint_command: str = ''
    using_remote_api_server: bool = False
    override_skypilot_config: dict[str, Any] | None = {}
    override_skypilot_config_path: str | None = None
    # Blob ID for uploaded file mounts
    file_mounts_blob_id: str | None = None
    # The client's API_VERSION as captured server-side from the
    # `X-SkyPilot-API-Version` request header in `prepare_request_async`
    # (the FastAPI dispatch context, where the `_remote_api_version`
    # ContextVar set by APIVersionMiddleware is visible). The field
    # exists because the worker process that later runs the request
    # cannot see that ContextVar — it crosses a process boundary via
    # the persisted request body. Clients themselves do NOT populate
    # this field; the server fills it in from the wire header so any
    # client that already sets the header (Python SDK already does; the
    # dashboard apiClient also sets it) gets the right value without
    # client-specific code. `None` means the request arrived without
    # the header — i.e. an old client.
    client_api_version: int | None = None

    def __init__(self, **data):
        data['env_vars'] = data.get('env_vars', request_body_env_vars())
        usage_lib_entrypoint = usage_lib.messages.usage.entrypoint
        if usage_lib_entrypoint is None:
            usage_lib_entrypoint = ''
        data['entrypoint'] = data.get('entrypoint', usage_lib_entrypoint)
        data['entrypoint_command'] = data.get(
            'entrypoint_command', common_utils.get_pretty_entrypoint_cmd())
        data['using_remote_api_server'] = data.get(
            'using_remote_api_server', not common.is_api_server_local())
        data['override_skypilot_config'] = data.get(
            'override_skypilot_config',
            get_override_skypilot_config_from_client())
        data['override_skypilot_config_path'] = data.get(
            'override_skypilot_config_path',
            get_override_skypilot_config_path_from_client())
        super().__init__(**data)

    def to_kwargs(self) -> dict[str, Any]:
        """Convert the request body to a kwargs dictionary on API server.

        This converts the request body into kwargs for the underlying SkyPilot
        backend's function.
        """
        kwargs = self.model_dump()
        kwargs.pop('env_vars')
        kwargs.pop('entrypoint')
        kwargs.pop('entrypoint_command')
        kwargs.pop('using_remote_api_server')
        kwargs.pop('override_skypilot_config')
        kwargs.pop('override_skypilot_config_path')
        kwargs.pop('file_mounts_blob_id')
        kwargs.pop('client_api_version', None)
        return kwargs

    @property
    def user_hash(self) -> str | None:
        return self.env_vars.get(constants.USER_ID_ENV_VAR)


class CheckBody(RequestBody):
    """The request body for the check endpoint."""
    clouds: tuple[str, ...] | None = None
    verbose: bool = False
    workspace: str | None = None


class EnabledCloudsBody(RequestBody):
    """The request body for the enabled clouds endpoint."""
    workspace: str | None = None
    expand: bool = False


class EnabledCloudsBatchBody(RequestBody):
    """The request body for the batch enabled clouds endpoint."""
    workspaces: list[str]
    expand: bool = False


class KubernetesLabelGpusBody(RequestBody):
    """The request body for the GPU labeling endpoint."""
    context: str | None = None
    cleanup_only: bool = False
    wait_for_completion: bool = True


class DagRequestBody(RequestBody):
    """Request body base class for endpoints with a dag."""
    dag: str

    def to_kwargs(self) -> dict[str, Any]:
        # Import here to avoid requirement of the whole SkyPilot dependency on
        # local clients.
        # pylint: disable=import-outside-toplevel
        from sky.utils import dag_utils

        kwargs = super().to_kwargs()

        dag = dag_utils.load_dag_from_yaml_str(self.dag)
        # We should not validate the dag here, as the file mounts are not
        # processed yet, but we need to validate the resources during the
        # optimization to make sure the resources are available.
        kwargs['dag'] = dag
        return kwargs


class DagRequestBodyWithRequestOptions(DagRequestBody):
    """Request body base class for endpoints with a dag and request options."""
    request_options: admin_policy.RequestOptions | None

    def get_request_options(self) -> admin_policy.RequestOptions | None:
        """Get the request options."""
        if self.request_options is None:
            return None
        if isinstance(self.request_options, dict):
            return admin_policy.RequestOptions(**self.request_options)
        return self.request_options

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        kwargs['request_options'] = self.get_request_options()
        return kwargs


class ValidateBody(DagRequestBodyWithRequestOptions):
    """The request body for the validate endpoint."""
    dag: str


class OptimizeBody(DagRequestBodyWithRequestOptions):
    """The request body for the optimize endpoint."""
    dag: str
    minimize: common_lib.OptimizeTarget = common_lib.OptimizeTarget.COST


class LaunchBody(RequestBody):
    """The request body for the launch endpoint."""
    task: str
    cluster_name: str
    retry_until_up: bool = False
    # TODO(aylei): remove this field in v0.12.0
    idle_minutes_to_autostop: int | None = None
    dryrun: bool = False
    # TODO(aylei): remove this field in v0.12.0
    down: bool = False
    backend: str | None = None
    optimize_target: common_lib.OptimizeTarget = common_lib.OptimizeTarget.COST
    no_setup: bool = False
    clone_disk_from: str | None = None
    fast: bool = False
    # Internal only:
    # pylint: disable=invalid-name
    quiet_optimizer: bool = False
    is_launched_by_jobs_controller: bool = False
    is_launched_by_sky_serve_controller: bool = False
    disable_controller_check: bool = False
    extra_launch_context: dict[str, Any] = {}
    # When True and the server supports it (API_VERSION >=
    # MIN_LAUNCH_CREDENTIALS_API_VERSION), the launch result will be a
    # 3-tuple (job_id, handle, credentials) instead of (job_id, handle).
    # Old servers ignore this field via Pydantic ``extra='ignore'`` and
    # continue to return the 2-tuple, so it is safe for new clients to
    # set against any server.
    include_credentials: bool = False

    def to_kwargs(self) -> dict[str, Any]:

        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task_on_api_server(
            self.task,
            self.env_vars,
            workdir_only=False,
            file_mounts_blob_id=self.file_mounts_blob_id)

        backend_cls = registry.BACKEND_REGISTRY.from_str(self.backend)
        backend = backend_cls() if backend_cls is not None else None
        kwargs['task'] = dag
        kwargs['backend'] = backend
        kwargs['_quiet_optimizer'] = kwargs.pop('quiet_optimizer')
        kwargs['_is_launched_by_jobs_controller'] = kwargs.pop(
            'is_launched_by_jobs_controller')
        kwargs['_is_launched_by_sky_serve_controller'] = kwargs.pop(
            'is_launched_by_sky_serve_controller')
        kwargs['_disable_controller_check'] = kwargs.pop(
            'disable_controller_check')
        kwargs['_extra_launch_context'] = kwargs.pop('extra_launch_context')
        kwargs['_include_credentials'] = kwargs.pop('include_credentials')
        return kwargs


class ExecBody(RequestBody):
    """The request body for the exec endpoint."""
    task: str
    cluster_name: str
    dryrun: bool = False
    down: bool = False
    backend: str | None = None

    def to_kwargs(self) -> dict[str, Any]:

        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task_on_api_server(
            self.task,
            self.env_vars,
            workdir_only=True,
            file_mounts_blob_id=self.file_mounts_blob_id)
        backend_cls = registry.BACKEND_REGISTRY.from_str(self.backend)
        backend = backend_cls() if backend_cls is not None else None
        kwargs['task'] = dag
        kwargs['backend'] = backend
        return kwargs


class StopOrDownBody(RequestBody):
    cluster_name: str
    purge: bool = False
    graceful: bool = False
    graceful_timeout: int | None = None


class StatusBody(RequestBody):
    """The request body for the status endpoint."""
    cluster_names: list[str] | None = None
    refresh: common_lib.StatusRefreshMode = common_lib.StatusRefreshMode.NONE
    all_users: bool = True
    # TODO (kyuds): default to False post 0.12.0
    include_credentials: bool = True
    # Only return fields that are needed for the
    # dashboard / CLI summary response
    summary_response: bool = False
    # Include the cluster handle in the response
    include_handle: bool = True


class StartBody(RequestBody):
    """The request body for the start endpoint."""
    cluster_name: str
    idle_minutes_to_autostop: int | None = None
    wait_for: autostop_lib.AutostopWaitFor | None = None
    retry_until_up: bool = False
    down: bool = False
    force: bool = False


class AutostopBody(RequestBody):
    """The request body for the autostop endpoint."""
    cluster_name: str
    idle_minutes: int
    wait_for: autostop_lib.AutostopWaitFor | None = None
    down: bool = False
    hook: str | None = None
    hook_timeout: int | None = None


class QueueBody(RequestBody):
    """The request body for the queue endpoint."""
    cluster_name: str
    skip_finished: bool = False
    all_users: bool = False


class CancelBody(RequestBody):
    """The request body for the cancel endpoint."""
    cluster_name: str
    job_ids: list[int] | None
    all: bool = False
    all_users: bool = False
    # Internal only. We cannot use prefix `_` because pydantic will not
    # include it in the request body.
    try_cancel_if_cluster_is_init: bool = False

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        kwargs['_try_cancel_if_cluster_is_init'] = kwargs.pop(
            'try_cancel_if_cluster_is_init')
        return kwargs


class ProvisionLogsBody(RequestBody):
    """Cluster node."""
    cluster_name: str
    worker: int | None = None


class HookLogsBody(RequestBody):
    """Per-event lifecycle-hook logs request body.

    ``event`` is optional — when None, the server auto-selects
    whichever per-event log exists on the cluster.
    """
    cluster_name: str
    event: str | None = None
    follow: bool = True
    tail: int = 0


class ClusterJobBody(RequestBody):
    """The request body for the cluster job endpoint."""
    cluster_name: str
    job_id: int | None
    follow: bool = True
    tail: int = 0


class ClusterJobsBody(RequestBody):
    """The request body for the cluster jobs endpoint."""
    cluster_name: str
    job_ids: list[str] | None


class ClusterJobsDownloadLogsBody(RequestBody):
    """The request body for the cluster jobs download logs endpoint."""
    cluster_name: str
    job_ids: list[str] | None
    local_dir: str = constants.SKY_LOGS_DIRECTORY


class UserCreateBody(RequestBody):
    """The request body for the user create endpoint."""
    username: str
    password: str
    role: str | None = None


class UserDeleteBody(RequestBody):
    """The request body for the user delete endpoint."""
    user_id: str


class UserUpdateBody(RequestBody):
    """The request body for the user update endpoint."""
    user_id: str
    role: str | None = None
    password: str | None = None


class UserImportBody(RequestBody):
    """The request body for the user import endpoint."""
    csv_content: str


class UserBatchUpdateBody(RequestBody):
    """The request body for the user batch update endpoint."""
    user_ids: list[str]
    role: str


class ServiceAccountTokenCreateBody(RequestBody):
    """The request body for creating a service account token."""
    token_name: str
    expires_in_days: int | None = None


class ServiceAccountTokenDeleteBody(RequestBody):
    """The request body for deleting a service account token."""
    token_id: str


class UpdateRoleBody(RequestBody):
    """The request body for updating a user role."""
    role: str


class ServiceAccountTokenRoleBody(RequestBody):
    """The request body for getting a service account token role."""
    token_id: str


class ServiceAccountTokenUpdateRoleBody(RequestBody):
    """The request body for updating a service account token role."""
    token_id: str
    role: str


class ServiceAccountTokenRotateBody(RequestBody):
    """The request body for rotating a service account token."""
    token_id: str
    expires_in_days: int | None = None


class DownloadBody(RequestBody):
    """The request body for the download endpoint."""
    folder_paths: list[str]


class StorageBody(RequestBody):
    """The request body for the storage endpoint."""
    name: str


class VolumeApplyBody(RequestBody):
    """The request body for the volume apply endpoint."""
    name: str
    volume_type: str
    cloud: str
    region: str | None = None
    zone: str | None = None
    size: str | None = None
    config: dict[str, Any] | None = None
    labels: dict[str, str] | None = None
    use_existing: bool | None = None
    creation_yaml: str | None = None


class VolumeDeleteBody(RequestBody):
    """The request body for the volume delete endpoint."""
    names: list[str]
    purge: bool = False


class VolumeListBody(RequestBody):
    """The request body for the volume list endpoint."""
    refresh: bool = False


class VolumeValidateBody(RequestBody):
    """The request body for the volume validate endpoint."""
    name: str | None = None
    volume_type: str | None = None
    infra: str | None = None
    size: str | None = None
    labels: dict[str, str] | None = None
    config: dict[str, Any] | None = None
    use_existing: bool | None = None


class EndpointsBody(RequestBody):
    """The request body for the endpoint."""
    cluster: str
    port: int | str | None = None


class ServeEndpointBody(RequestBody):
    """The request body for the serve controller endpoint."""
    port: int | str | None = None


class JobStatusBody(RequestBody):
    """The request body for the job status endpoint."""
    cluster_name: str
    job_ids: list[int] | None


class JobsLaunchBody(RequestBody):
    """The request body for the jobs launch endpoint."""
    task: str
    name: str | None
    pool: str | None = None
    num_jobs: int | None = None

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        kwargs['task'] = common.process_mounts_in_task_on_api_server(
            self.task,
            self.env_vars,
            workdir_only=False,
            file_mounts_blob_id=self.file_mounts_blob_id)
        # Pass the blob id through so that consolidation-mode submissions can
        # record it on the job and keep the blob alive until the job is
        # terminal.
        kwargs['file_mounts_blob_id'] = self.file_mounts_blob_id
        return kwargs


class JobsQueueBody(RequestBody):
    """The request body for the jobs queue endpoint."""
    refresh: bool = False
    skip_finished: bool = False
    all_users: bool = False
    job_ids: list[int] | None = None


class JobsQueueV2Body(RequestBody):
    """The request body for the jobs queue endpoint."""
    refresh: bool = False
    skip_finished: bool = False
    all_users: bool = False
    job_ids: list[int] | None = None
    user_match: str | None = None
    workspace_match: str | None = None
    name_match: str | None = None
    pool_match: str | None = None
    page: int | None = None
    limit: int | None = None
    statuses: list[str] | None = None
    # The fields to return in the response.
    # Refer to the fields in the `class ManagedJobRecord` in `response.py`
    fields: list[str] | None = None
    # Sorting parameters, added in ManagedJobsService v14.
    sort_by: str | None = None  # Field to sort by (e.g., 'job_id', 'name')
    sort_order: str | None = None  # 'asc' or 'desc'
    # Time-range filter on submitted_at (epoch seconds).
    submitted_after: float | None = None
    submitted_before: float | None = None


class JobsCancelBody(RequestBody):
    """The request body for the jobs cancel endpoint."""
    name: str | None = None
    job_ids: list[int] | None = None
    all: bool = False
    all_users: bool = False
    pool: str | None = None
    graceful: bool = False
    graceful_timeout: int | None = None


class JobsLogsBody(RequestBody):
    """The request body for the jobs logs endpoint."""
    name: str | None = None
    job_id: int | None = None
    follow: bool = True
    controller: bool = False
    refresh: bool = False
    tail: int | None = None
    # Skip the last `tail_offset` lines from the end of the file before
    # taking `tail` lines. Used by the dashboard live-tail UI to fetch
    # progressively older windows without re-reading the whole file.
    tail_offset: int | None = None
    # Task identifier: int for task_id, str for task_name
    task: str | int | None = None


class JobsWaitBody(RequestBody):
    """The request body for the jobs wait endpoint."""
    name: str | None = None
    job_id: int | None = None
    # Timeout in seconds. None means wait forever.
    timeout: int | None = None
    # Polling interval in seconds. Minimum 5, default 15.
    poll_interval: int = 15
    # Task identifier for JobGroups: int for task_id, str for task_name.
    # If None, waits for all tasks.
    task: str | int | None = None


class RequestCancelBody(RequestBody):
    """The request body for the API request cancellation endpoint."""
    # Kill all requests if request_ids is None.
    request_ids: list[str] | None = None
    user_id: str | None = None


class RequestStatusBody(pydantic.BaseModel):
    """The request body for the API request status endpoint."""
    request_ids: list[str] | None = None
    all_status: bool = False
    limit: int | None = None
    fields: list[str] | None = None
    cluster_name: str | None = None


class OperatorNotificationReadBody(pydantic.BaseModel):
    """The request body for advancing an operator notification cursor."""
    through_sequence: int


class ServeUpBody(RequestBody):
    """The request body for the serve up endpoint."""
    task: str
    service_name: str

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task_on_api_server(
            self.task,
            self.env_vars,
            workdir_only=False,
            file_mounts_blob_id=self.file_mounts_blob_id)
        assert len(
            dag.tasks) == 1, ('Must only specify one task in the DAG for '
                              'a service.', dag)
        kwargs['task'] = dag.tasks[0]
        kwargs['submitted_yaml_content'] = self.task
        return kwargs


class ServeUpdateBody(RequestBody):
    """The request body for the serve update endpoint."""
    task: str
    service_name: str
    mode: serve.UpdateMode

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task_on_api_server(
            self.task,
            self.env_vars,
            workdir_only=False,
            file_mounts_blob_id=self.file_mounts_blob_id)
        assert len(
            dag.tasks) == 1, ('Must only specify one task in the DAG for '
                              'a service.', dag)
        kwargs['task'] = dag.tasks[0]
        kwargs['submitted_yaml_content'] = self.task
        return kwargs


class ServeVersionElectionBody(BasePayload):
    """The public admin request for electing a stored service version."""
    version: int = pydantic.Field(ge=1)


class ServeElectVersionBody(RequestBody):
    """Internal queued request for a fenced service version election."""
    service_name: str
    version: int
    expected_service_hash: str
    expected_elected_version: int | None


class ServeLoadBalancerHighAvailabilityBody(BasePayload):
    """The public admin request for changing a service's LB topology."""
    enabled: pydantic.StrictBool


class ServeSetLoadBalancerHighAvailabilityBody(RequestBody):
    """Internal queued request for a fenced LB topology transition."""
    service_name: str
    enabled: pydantic.StrictBool
    expected_service_hash: str


class ServeDownBody(RequestBody):
    """The request body for the serve down endpoint."""
    service_names: str | list[str] | None
    all: bool = False
    purge: bool = False


class ServeLogsBody(RequestBody):
    """The request body for the serve logs endpoint."""
    service_name: str
    target: str | serve.ServiceComponent
    replica_id: int | None = None
    follow: bool = True
    tail: int | None = None


class ServeDownloadLogsBody(RequestBody):
    """The request body for the serve download logs endpoint."""
    service_name: str
    local_dir: str
    targets: str | serve.ServiceComponent | list[str |
                                                 serve.ServiceComponent] | None
    replica_ids: list[int] | None = None
    tail: int | None = None


class ServeStatusBody(RequestBody):
    """The request body for the serve status endpoint."""
    service_names: str | list[str] | None
    # Skip per-replica info; return cheap replica_status_counts instead.
    # Used by the dashboard for fast list/header rendering at fleet scale.
    summary_only: bool = False
    # Optional override for target_num_replicas. If unset, the server keeps
    # full status behavior (include targets) but leaves summary-only requests
    # on the cheap DB-only path.
    include_target_num_replicas: bool | None = None
    # Include aggregate physical-machine history for one named service.
    # Central history is PostgreSQL-only and retained for up to 72 hours.
    history_hours: int | None = None


class RealtimeGpuAvailabilityRequestBody(RequestBody):
    """The request body for the realtime GPU availability endpoint."""
    context: str | None = None
    name_filter: str | None = None
    quantity_filter: int | None = None
    is_ssh: bool | None = None


class KubernetesNodeInfoRequestBody(RequestBody):
    """The request body for the kubernetes node info endpoint."""
    context: str | None = None


class SlurmNodeInfoRequestBody(RequestBody):
    """The request body for the slurm node info endpoint."""
    slurm_cluster_name: str | None = None


class ListAcceleratorsBody(RequestBody):
    """The request body for the list accelerators endpoint."""
    gpus_only: bool = True
    name_filter: str | None = None
    region_filter: str | None = None
    quantity_filter: int | None = None
    clouds: list[str] | str | None = None
    all_regions: bool = False
    require_price: bool = True
    case_sensitive: bool = True


class ListAcceleratorCountsBody(RequestBody):
    """The request body for the list accelerator counts endpoint."""
    gpus_only: bool = True
    name_filter: str | None = None
    region_filter: str | None = None
    quantity_filter: int | None = None
    clouds: list[str] | str | None = None


class LocalUpBody(RequestBody):
    """The request body for the local up endpoint."""
    gpus: bool = True
    name: str | None = None
    port_start: int | None = None


class LocalDownBody(RequestBody):
    """The request body for the local down endpoint."""
    name: str | None = None


class SSHUpBody(RequestBody):
    """The request body for the SSH up/down endpoints."""
    infra: str | None = None
    cleanup: bool = False


class ServeTerminateReplicaBody(RequestBody):
    """The request body for the serve terminate replica endpoint."""
    service_name: str
    replica_id: int
    purge: bool = False


class KillRequestProcessesBody(RequestBody):
    """The request body for the kill request processes endpoint."""
    request_ids: list[str]


class StreamBody(pydantic.BaseModel):
    """The request body for the stream endpoint."""
    request_id: str | None = None
    log_path: str | None = None
    tail: int | None = None
    plain_logs: bool = True


class JobsDownloadLogsBody(RequestBody):
    """The request body for the jobs download logs endpoint."""
    name: str | None
    job_id: int | None
    refresh: bool = False
    controller: bool = False
    local_dir: str = constants.SKY_LOGS_DIRECTORY


class JobsPoolApplyBody(RequestBody):
    """The request body for the jobs pool apply endpoint."""
    task: str | None = None
    workers: int | None = None
    pool_name: str
    mode: serve.UpdateMode

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        if self.task is not None:
            dag = common.process_mounts_in_task_on_api_server(
                self.task,
                self.env_vars,
                workdir_only=False,
                file_mounts_blob_id=self.file_mounts_blob_id)
            assert len(
                dag.tasks) == 1, ('Must only specify one task in the DAG for '
                                  'a pool.', dag)
            kwargs['task'] = dag.tasks[0]
        else:
            kwargs['task'] = None
        return kwargs


class JobsPoolDownBody(RequestBody):
    """The request body for the jobs pool down endpoint."""
    pool_names: str | list[str] | None
    all: bool = False
    purge: bool = False


class JobsPoolStatusBody(RequestBody):
    """The request body for the jobs pool status endpoint."""
    pool_names: str | list[str] | None


class JobsPoolLogsBody(RequestBody):
    """The request body for the jobs pool logs endpoint."""
    pool_name: str
    target: str | serve.ServiceComponent
    worker_id: int | None = None
    follow: bool = True
    tail: int | None = None


class JobsPoolDownloadLogsBody(RequestBody):
    """The request body for the jobs pool download logs endpoint."""
    pool_name: str
    local_dir: str
    targets: str | serve.ServiceComponent | list[str |
                                                 serve.ServiceComponent] | None
    worker_ids: list[int] | None = None
    tail: int | None = None


class UploadZipFileResponse(pydantic.BaseModel):
    """The response body for the upload zip file endpoint."""
    status: str
    missing_chunks: list[str] | None = None


class UpdateWorkspaceBody(RequestBody):
    """The request body for updating a specific workspace configuration."""
    workspace_name: str = ''  # Will be set from path parameter
    config: dict[str, Any]


class CreateWorkspaceBody(RequestBody):
    """The request body for creating a new workspace."""
    workspace_name: str = ''  # Will be set from path parameter
    config: dict[str, Any]


class DeleteWorkspaceBody(RequestBody):
    """The request body for deleting a workspace."""
    workspace_name: str


class WorkspaceBatchAddUsersBody(RequestBody):
    """The request body for adding users to multiple workspaces."""
    workspace_names: list[str]
    user_ids: list[str]


class WorkspaceBatchRemoveUsersBody(RequestBody):
    """The request body for removing users from multiple workspaces."""
    workspace_names: list[str]
    user_ids: list[str]


class UpdateConfigBody(RequestBody):
    """The request body for updating the entire SkyPilot configuration."""
    config: dict[str, Any]


class GetConfigBody(RequestBody):
    """The request body for getting the entire SkyPilot configuration."""
    pass


class UserPreferredWorkspaceBody(RequestBody):
    """Request body for POST /users/me/workspace.

    `preferred` is the workspace name to set as the user's default, or None
    to clear the preference. RBAC is validated server-side in
    sky/workspaces/core.set_user_preferred_workspace().
    """
    preferred: str | None = None


class CostReportBody(RequestBody):
    """The request body for the cost report endpoint."""
    days: int | None = 30
    # we use hashes instead of names to avoid the case where
    # the name is not unique
    cluster_hashes: list[str] | None = None
    # Filter by cluster name. Useful for the dashboard, which routes a
    # torn-down cluster's detail page by name (the URL param is the
    # cluster name, not the hash). When both cluster_hashes and
    # cluster_names are set, rows matching either are returned.
    cluster_names: list[str] | None = None
    # Only return fields that are needed for the dashboard
    # summary page
    dashboard_summary_response: bool = False
    # Exclude clusters launched by a controller (managed jobs and services).
    # Used by the dashboard so that clusters backing managed jobs do not show
    # up in the cluster history view.
    exclude_managed_clusters: bool = False


class CreateDebugDumpBody(RequestBody):
    """The request body for the debug dump init endpoint."""
    request_ids: list[str] | None = None
    cluster_names: list[str] | None = None
    managed_job_ids: list[int] | None = None
    recent_minutes: float | None = None
    # Client-side info for troubleshooting (version, config, environment)
    client_info: dict[str, Any] | None = None


class RequestPayload(BasePayload):
    """The payload for the requests."""

    request_id: str
    name: str
    entrypoint: str
    request_body: str
    status: str
    created_at: float
    user_id: str
    return_value: str
    error: str
    pid: int | None
    schedule_type: str
    user_name: str | None = None
    # Resources the request operates on.
    cluster_name: str | None = None
    status_msg: str | None = None
    should_retry: bool = False
    finished_at: float | None = None
    file_mounts_blob_id: str | None = None


class SlurmGpuAvailabilityRequestBody(RequestBody):
    """Request body for getting Slurm real-time GPU availability."""
    slurm_cluster_name: str | None = None
    name_filter: str | None = None
    quantity_filter: int | None = None


class ClusterEventsBody(RequestBody):
    """The request body for the cluster events endpoint."""
    cluster_name: str | None = None
    cluster_hash: str | None = None
    # Event type to retrieve (e.g. 'STATUS_CHANGE' or 'DEBUG'). Multiple types
    # may be requested as a comma-separated string (e.g.
    # 'STATUS_CHANGE,LAUNCH_PROGRESS'); results are merged by timestamp.
    # TODO: consider replacing this with a typed `event_types: List[str]`
    # field (mapping a single `event_type` to `[event_type]` for back-compat)
    # so callers don't have to encode the list as a comma-separated string.
    event_type: str
    include_timestamps: bool = False
    limit: int | None = None  # If specified, returns at most this many events


class GetJobEventsBody(RequestBody):
    """The request body for the get job task events endpoint."""
    job_id: int
    task_id: int | None = None
    limit: int | None = 10  # Default to 10 most recent task events
    # When True, merge in launch-progress events from the job's underlying
    # cluster (e.g. image pulling) so the timeline shows provisioning
    # milestones between STARTING and RUNNING. Defaults to False to keep the
    # response backward compatible for callers that only want status events.
    include_cluster_events: bool = False


# =============================================================================
# YAML Hub payloads
# =============================================================================


class RecipeListBody(RequestBody):
    """The request body for listing recipes."""
    pinned_only: bool = False
    my_recipes_only: bool = False
    recipe_type: str | None = None  # See RecipeType: 'cluster', 'job', 'pool', 'volume'

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        # Inject user_id from env_vars for filtering by user
        # Fallback to 'local' for unauthenticated local servers
        kwargs['user_id'] = self.env_vars.get(constants.USER_ID_ENV_VAR,
                                              'local')
        return kwargs


class RecipeGetBody(RequestBody):
    """The request body for getting a single recipe."""
    recipe_name: str


class RecipeCreateBody(RequestBody):
    """The request body for creating a new recipe."""
    name: str
    content: str
    recipe_type: str  # See RecipeType: 'cluster', 'job', 'pool', 'volume'
    description: str | None = None
    owner_name: str | None = None  # Override user_name for unauthenticated

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        # Inject user_id and user_name from env_vars
        # Fallback to 'local' for unauthenticated local servers
        kwargs['user_id'] = self.env_vars.get(constants.USER_ID_ENV_VAR,
                                              'local')
        # Use owner_name if provided (for unauthenticated users), else use env
        # var.
        if self.owner_name:
            kwargs['user_name'] = self.owner_name
        else:
            kwargs['user_name'] = self.env_vars.get(constants.USER_ENV_VAR,
                                                    'local')
        # Remove owner_name from kwargs - it's only used to set user_name above
        kwargs.pop('owner_name', None)
        return kwargs


class RecipeUpdateBody(RequestBody):
    """The request body for updating an existing recipe."""
    recipe_name: str
    description: str | None = None
    content: str | None = None

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        # Inject user_id and user_name from env_vars
        # Fallback to 'local' for unauthenticated local servers
        kwargs['user_id'] = self.env_vars.get(constants.USER_ID_ENV_VAR,
                                              'local')
        kwargs['user_name'] = self.env_vars.get(constants.USER_ENV_VAR, 'local')
        return kwargs


class RecipeDeleteBody(RequestBody):
    """The request body for deleting a recipe."""
    recipe_name: str

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        # Inject user_id from env_vars for ownership check
        # Fallback to 'local' for unauthenticated local servers
        kwargs['user_id'] = self.env_vars.get(constants.USER_ID_ENV_VAR,
                                              'local')
        return kwargs


class RecipePinBody(RequestBody):
    """The request body for toggling pin status."""
    recipe_name: str
    pinned: bool
