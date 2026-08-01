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
import copy
import os
import typing
from typing import Any

from sky import admin_policy
from sky import serve
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.container_images import models as container_image_models
from sky.serve import constants as serve_constants
from sky.server import common
from sky.skylet import autostop_lib
from sky.skylet import constants
from sky.usage import constants as usage_constants
from sky.usage import usage_lib
from sky.utils import annotations
from sky.utils import common as common_lib
from sky.utils import common_utils
from sky.utils import config_utils
from sky.utils import infra_utils
from sky.utils import registry
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:
    import pydantic
else:
    pydantic = adaptors_common.LazyImport('pydantic')

logger = sky_logging.init_logger(__name__)

_CONTAINER_IMAGE_TASK_ERROR_MESSAGE = (
    'Invalid managed container image task specification.')
_MAX_TASK_RESOURCE_CONFIGS = 4096
_RESOURCE_CANDIDATE_FIELDS = ('any_of', 'ordered')


class ContainerImageTaskValidationError(ValueError):
    """Closed marker for task-image validation failures at the REST edge."""


def _without_server_owned_override_config(
        override_configs: dict[str, Any] | None) -> dict[str, Any] | None:
    """Removes ignored server-owned config before durable persistence."""
    if override_configs is None:
        return None
    if not isinstance(override_configs, dict):
        raise ValueError('Invalid client SkyPilot configuration override.')
    sanitized = copy.deepcopy(override_configs)
    skipped_keys = config_utils.expand_nested_key_patterns(
        sanitized, constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    for key_path in skipped_keys:
        parent: Any = sanitized
        for key in key_path[:-1]:
            if not isinstance(parent, dict):
                break
            parent = parent.get(key)
        else:
            if isinstance(parent, dict):
                parent.pop(key_path[-1], None)
    return sanitized


def is_container_image_task_validation_error(error: Any) -> bool:
    """Returns whether a Pydantic/FastAPI error carries the closed marker."""
    try:
        details = error.errors()
    except (AttributeError, TypeError, ValueError):
        return False
    for detail in details:
        if not isinstance(detail, dict):
            continue
        context = detail.get('ctx')
        if (isinstance(context, dict) and isinstance(
                context.get('error'), ContainerImageTaskValidationError)):
            return True
    return False


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

# Platform identity and capabilities must come from the server deployment,
# never a client process, an older API pod's persisted environment, or a
# hand-crafted request body. Most of these variables predate the
# SKYPILOT_SERVER_ naming convention, so classify them explicitly while
# keeping their existing names compatible across rolling chart/image upgrades.
_SERVER_OWNED_ENV_VARS = frozenset({
    'POD_IP',
    'SKY_API_SERVER_METRICS_ENABLED',
    'SKYPILOT_APISERVER_UUID',
    'SKYPILOT_API_DEPLOYMENT_NAME',
    'SKYPILOT_API_REQUEST_BACKEND',
    'SKYPILOT_API_REQUEST_CUTOVER_GATE_PATH',
    'SKYPILOT_API_SERVER_INSTANCE_ID',
    'SKYPILOT_API_SERVER_ROLE',
    'SKYPILOT_API_SERVER_STORAGE_ENABLED',
    'SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS',
    'SKYPILOT_GRACE_PERIOD_SECONDS',
    'SKYPILOT_IN_CLUSTER_NAMESPACE',
    'SKYPILOT_POD_NAME',
    'SKYPILOT_POD_NAMESPACE',
    'SKYPILOT_POD_UID',
    'SKYPILOT_RELEASE_NAME',
    'SKYPILOT_ROLLING_UPDATE_ENABLED',
    'SKYPILOT_SERVE_API_SERVICE_URL',
    'SKYPILOT_SERVE_CONTROLLER_ADMIN_AUTH_TOKENS_FILE',
    'SKYPILOT_SERVE_LB_AUTH_TOKENS_FILE',
    'SKYPILOT_SERVE_LB_DATA_PLANE_AUTH_ENABLED',
    'SKYPILOT_SERVE_LB_HA_RBAC_READY',
    'SKYPILOT_SERVE_LB_RESOURCES_JSON',
    'SKYPILOT_SERVE_LB_SYNC_AUTH_TOKENS_FILE',
    'SKYPILOT_STATE_DB_MIGRATION_MODE',
    constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR,
    constants.SKY_API_SERVER_URL_ENV_VAR,
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


def _resource_config_cloud_constraint(
        resource_config: dict[str, Any]) -> str | None:
    """Parses one resource cloud constraint with normal wildcard semantics."""
    cloud = resource_config.get('cloud')
    infra = resource_config.get('infra')
    if cloud is not None and not isinstance(cloud, str):
        raise ValueError
    if infra is not None and not isinstance(infra, str):
        raise ValueError
    if (infra is not None and any(
            resource_config.get(field) is not None
            for field in ('cloud', 'region', 'zone'))):
        raise ValueError
    if isinstance(infra, str):
        return infra_utils.InfraInfo.from_str(infra).cloud
    if cloud is None:
        return None
    normalized = cloud.strip().lower()
    if not normalized or normalized == '*':
        return None
    if normalized == 'k8s':
        return 'kubernetes'
    return normalized


def _resource_config_targets_kubernetes(
        resource_config: dict[str, Any]) -> bool:
    """Returns whether one effective resource config targets Kubernetes."""
    cloud_name = _resource_config_cloud_constraint(resource_config)
    if cloud_name is None:
        return False
    return _cloud_constraint_targets_kubernetes(cloud_name)


def _resource_config_may_target_kubernetes(
        resource_config: dict[str, Any]) -> bool:
    """Returns whether optimization can reinterpret an image ID as Docker."""
    cloud_name = _resource_config_cloud_constraint(resource_config)
    if cloud_name is None:
        return True
    return _cloud_constraint_targets_kubernetes(cloud_name)


def _cloud_constraint_targets_kubernetes(cloud_name: str) -> bool:
    """Resolves a validated cloud constraint without optimization-only guards."""
    cloud = registry.CLOUD_REGISTRY.from_str(cloud_name)
    kubernetes = registry.CLOUD_REGISTRY.from_str('kubernetes')
    if cloud is None or kubernetes is None:
        raise ValueError
    return isinstance(cloud, type(kubernetes))


def _validate_legacy_image_id(image_id: Any,
                              *,
                              kubernetes_possible: bool,
                              scan_unclassified: bool = False) -> bool:
    """Validates legacy Docker ``image_id`` forms and returns if one exists."""
    if image_id is None:
        return False

    docker_references: list[str] = []
    has_reserved_docker_key = False
    has_docker_image_value = False
    if isinstance(image_id, str):
        image_value = image_id.strip()
        if image_value.startswith('docker:'):
            docker_references.append(image_value[len('docker:'):])
        elif kubernetes_possible:
            docker_references.append(image_value)
        elif scan_unclassified:
            _validate_unclassified_legacy_image_value(image_value)
    elif isinstance(image_id, dict):
        for region, raw_image_value in image_id.items():
            if region is not None and not isinstance(region, str):
                raise ValueError
            if not isinstance(raw_image_value, str):
                raise ValueError
            image_value = raw_image_value.strip()
            if region == 'docker':
                has_reserved_docker_key = True
                if image_value.startswith('docker:'):
                    image_value = image_value[len('docker:'):]
                docker_references.append(image_value)
            elif image_value.startswith('docker:'):
                has_docker_image_value = True
                docker_references.append(image_value[len('docker:'):])
            elif kubernetes_possible:
                has_docker_image_value = True
                docker_references.append(image_value)
            elif scan_unclassified:
                _validate_unclassified_legacy_image_value(image_value)
    else:
        raise ValueError

    if has_reserved_docker_key and has_docker_image_value:
        raise ValueError
    normalized_references = {
        container_image_models.ContainerImage.from_legacy_ref(reference).ref
        for reference in docker_references
    }
    if len(normalized_references) > 1:
        raise ValueError
    return bool(normalized_references)


def _validate_unclassified_legacy_image_value(image_value: str) -> None:
    """Rejects credential syntax without treating a cloud VM ID as OCI."""
    suspicious = (len(image_value) > 1024 or not image_value.isprintable() or
                  any(character.isspace() for character in image_value) or
                  '://' in image_value or
                  any(character in image_value
                      for character in ('@', '?', '#', '%', '\\', '=')))
    if suspicious:
        container_image_models.validate_operational_image_selector(image_value)


def _validate_container_image_docker_login_config(
        resource_config: dict[str, Any]) -> bool:
    """Validates a resource login config and reports inline credentials."""
    login_config = resource_config.get('_docker_login_config')
    if login_config is None:
        return False
    if not isinstance(login_config, dict):
        raise ValueError
    has_inline_credentials = False
    for field in ('username', 'password'):
        value = login_config.get(field)
        if value is not None and not isinstance(value, str):
            raise ValueError
        if value:
            has_inline_credentials = True
    return has_inline_credentials


def _validate_task_container_image_credentials(
        task_config: dict[str, Any]) -> None:
    """Rejects inline Docker credentials before a task can be persisted."""
    credential_keys = (constants.DOCKER_USERNAME_ENV_VAR,
                       constants.DOCKER_PASSWORD_ENV_VAR)
    for field in ('envs', 'secrets'):
        values = task_config.get(field)
        if values is None:
            continue
        if field == 'secrets' and isinstance(values, list):
            # Managed secret references contain no inline credential value.
            continue
        if not isinstance(values, dict):
            raise ValueError
        for key in credential_keys:
            value = values.get(key)
            if value is not None and not isinstance(value, str):
                raise ValueError
            if value:
                raise ValueError


def _validate_resource_container_images(
    resource_config: dict[str, Any],
    *,
    include_unconstrained_kubernetes: bool,
    scan_unclassified: bool,
) -> tuple[bool, bool]:
    """Validates image selectors in one explicit or effective config."""
    if '_resolved_container_image' in resource_config:
        # This is a server-created placement snapshot. Reject it before the
        # serialized task can enter the request database, even when the task
        # has no ordinary image selector that would otherwise classify it as
        # a managed-image request.
        raise ValueError
    if ('container_image' not in resource_config and
            'image_id' not in resource_config):
        return False, False
    has_container_image = False
    container_image = resource_config.get('container_image')
    if container_image is not None:
        container_image_models.ContainerImage.from_config(container_image)
        if _validate_container_image_docker_login_config(resource_config):
            raise ValueError
        has_container_image = True

    image_id = resource_config.get('image_id')
    kubernetes_possible = False
    if image_id is not None:
        kubernetes_possible = (
            _resource_config_may_target_kubernetes(resource_config)
            if include_unconstrained_kubernetes else
            _resource_config_targets_kubernetes(resource_config))
    has_legacy_container_image = _validate_legacy_image_id(
        image_id,
        kubernetes_possible=kubernetes_possible,
        scan_unclassified=scan_unclassified)
    if has_container_image and has_legacy_container_image:
        raise ValueError
    return (has_container_image or
            has_legacy_container_image, has_container_image)


def _serialized_task_uses_container_image(value: str) -> bool:
    """Validates and classifies image selectors in serialized task YAML."""
    yaml_utils.check_no_duplicate_keys(value)
    configs = yaml_utils.read_yaml_all_str(value)
    uses_container_image = False
    processed_resource_configs = 0
    for task_config in configs:
        if task_config is None:
            continue
        if isinstance(task_config, str):
            # Preserve the existing request-model contract for opaque task
            # placeholders used by internal and older clients.  A scalar
            # string cannot define a SkyPilot resource mapping, so there is no
            # managed-image selector to validate at this edge. Normal task
            # parsing remains responsible for rejecting it before execution.
            continue
        if not isinstance(task_config, dict):
            raise ValueError
        resources_config = task_config.get('resources')
        if resources_config is None:
            continue
        task_uses_explicit_container_image = False
        task_has_inline_resource_credentials = False
        if isinstance(resources_config, dict):
            root_resource_configs = [resources_config]
        elif isinstance(resources_config, list):
            root_resource_configs = list(resources_config)
        else:
            raise ValueError

        pending_resource_configs: list[tuple[dict[str, Any], dict[str, Any],
                                             frozenset[int]]] = []
        for root_resource_config in root_resource_configs:
            if not isinstance(root_resource_config, dict):
                raise ValueError
            pending_resource_configs.append(
                (root_resource_config, {}, frozenset()))

        while pending_resource_configs:
            resource_config, inherited_config, ancestors = (
                pending_resource_configs.pop())
            resource_config_id = id(resource_config)
            if resource_config_id in ancestors:
                raise ValueError
            processed_resource_configs += 1
            if processed_resource_configs > _MAX_TASK_RESOURCE_CONFIGS:
                raise ValueError
            task_has_inline_resource_credentials |= (
                _validate_container_image_docker_login_config(resource_config))

            # Validate explicit fields even when a child candidate overrides
            # them. Rejected task text must never persist credential-bearing
            # image values that happen not to survive resource inheritance.
            raw_uses_image, raw_uses_explicit_image = (
                _validate_resource_container_images(
                    resource_config,
                    include_unconstrained_kubernetes=False,
                    scan_unclassified=True))
            uses_container_image |= raw_uses_image
            task_uses_explicit_container_image |= raw_uses_explicit_image

            effective_config = inherited_config.copy()
            effective_config.update({
                key: item
                for key, item in resource_config.items()
                if key not in _RESOURCE_CANDIDATE_FIELDS
            })
            candidate_lists: list[list[Any]] = []
            candidate_fields_present = 0
            for candidate_field in _RESOURCE_CANDIDATE_FIELDS:
                candidates = resource_config.get(candidate_field)
                if candidates is None:
                    continue
                candidate_fields_present += 1
                if not isinstance(candidates, list):
                    raise ValueError
                candidate_lists.append(candidates)
            if candidate_fields_present > 1:
                raise ValueError

            candidates = candidate_lists[0] if candidate_lists else []
            # A known Kubernetes-family constraint is meaningful at every
            # intermediate node. An unconstrained node is classified only if
            # it is a concrete leaf; its children may inherit a non-container
            # cloud whose VM image syntax is intentionally not OCI.
            effective_uses_image, effective_uses_explicit_image = (
                _validate_resource_container_images(
                    effective_config,
                    include_unconstrained_kubernetes=not candidates,
                    scan_unclassified=False))
            uses_container_image |= effective_uses_image
            task_uses_explicit_container_image |= (
                effective_uses_explicit_image)
            if not candidates:
                continue
            next_ancestors = ancestors | {resource_config_id}
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    raise ValueError
                pending_resource_configs.append(
                    (candidate, effective_config, next_ancestors))
        if task_uses_explicit_container_image:
            if task_has_inline_resource_credentials:
                raise ValueError
            _validate_task_container_image_credentials(task_config)
    return uses_container_image


def validate_task_request_body_for_persistence(body: 'RequestBody') -> None:
    """Sanitizes server-owned config and revalidates final request-row data."""
    body.override_skypilot_config = _without_server_owned_override_config(
        body.override_skypilot_config)
    if isinstance(body, DagRequestBody):
        _validate_serialized_task_container_images(body.dag)
        return
    task_body_types = (LaunchBody, ExecBody, JobsLaunchBody, ServeUpBody,
                       ServeUpdateBody, JobsPoolApplyBody)
    if isinstance(body, task_body_types):
        task = body.task
        if task is not None:
            _validate_serialized_task_container_images(task)


def serialized_task_uses_container_image(value: str | None) -> bool:
    """Returns whether serialized task YAML crosses the image boundary.

    Persisted request bodies have already passed the validator below. For an
    older or corrupted row, fail closed so its terminal error cannot expose a
    provider value.
    """
    if value is None:
        return False
    try:
        return _serialized_task_uses_container_image(value)
    except Exception:  # pylint: disable=broad-except
        return True


def _validate_serialized_task_container_images(value: str | None) -> str | None:
    """Validates managed-image selectors before a task body is persisted.

    Task-bearing REST payloads intentionally keep YAML as a string until the
    request worker processes file mounts. Without this preflight, invalid
    ``container_image`` or effective legacy Docker ``image_id`` values could
    be stored in task.db before normal task parsing rejects them. Collapse
    every failure to a value-free error.
    """
    if value is None:
        return value
    try:
        _serialized_task_uses_container_image(value)
    except Exception:  # pylint: disable=broad-except
        raise ContainerImageTaskValidationError(
            _CONTAINER_IMAGE_TASK_ERROR_MESSAGE) from None
    return value


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
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

    dag: str

    _validate_container_images = pydantic.field_validator('dag')(
        _validate_serialized_task_container_images)

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
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

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

    _validate_container_images = pydantic.field_validator('task')(
        _validate_serialized_task_container_images)

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
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

    task: str
    cluster_name: str
    dryrun: bool = False
    down: bool = False
    backend: str | None = None

    _validate_container_images = pydantic.field_validator('task')(
        _validate_serialized_task_container_images)

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
    workspaces_filter: list[str] | None = None
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
    name: str | None = None


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
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

    task: str
    name: str | None
    pool: str | None = None
    num_jobs: int | None = None

    _validate_container_images = pydantic.field_validator('task')(
        _validate_serialized_task_container_images)

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
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

    task: str
    service_name: str

    _validate_container_images = pydantic.field_validator('task')(
        _validate_serialized_task_container_images)

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task_on_api_server(
            self.task,
            self.env_vars,
            workdir_only=False,
            file_mounts_blob_id=self.file_mounts_blob_id)
        if len(dag.tasks) != 1:
            raise ValueError('Must only specify one task in the DAG for '
                             f'a service. Found {len(dag.tasks)} tasks.')
        kwargs['task'] = dag.tasks[0]
        kwargs['submitted_yaml_content'] = self.task
        return kwargs


class ServeUpdateBody(RequestBody):
    """The request body for the serve update endpoint."""
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

    task: str
    service_name: str
    mode: serve.UpdateMode

    _validate_container_images = pydantic.field_validator('task')(
        _validate_serialized_task_container_images)

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        dag = common.process_mounts_in_task_on_api_server(
            self.task,
            self.env_vars,
            workdir_only=False,
            file_mounts_blob_id=self.file_mounts_blob_id)
        if len(dag.tasks) != 1:
            raise ValueError('Must only specify one task in the DAG for '
                             f'a service. Found {len(dag.tasks)} tasks.')
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
    # Return persisted service metadata without replica counts/details,
    # autoscaler data, history, YAML, endpoint resolution, or provider calls.
    metadata_only: bool = False
    # Optional override for target_num_replicas. If unset, the server keeps
    # full status behavior (include targets) but leaves summary-only requests
    # on the cheap DB-only path.
    include_target_num_replicas: bool | None = None
    # Include aggregate physical-machine history for one named service.
    # Central history is PostgreSQL-only and retained for up to 72 hours.
    history_hours: int | None = None
    # Summary responses skip endpoint resolution by default because it requires
    # Kubernetes reads. Dashboard list enrichment opts in after metadata lands.
    include_endpoints: bool = False


class ServePlacementBody(RequestBody):
    """The request body for one service's placement observability."""
    service_name: str
    hours: int = pydantic.Field(default=24, ge=1, le=24)
    limit: int = pydantic.Field(default=50, ge=1, le=100)
    cursor: str | None = pydantic.Field(default=None,
                                        min_length=1,
                                        max_length=512)


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
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

    task: str | None = None
    workers: int | None = None
    pool_name: str
    mode: serve.UpdateMode

    _validate_container_images = pydantic.field_validator('task')(
        _validate_serialized_task_container_images)

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = super().to_kwargs()
        if self.task is not None:
            dag = common.process_mounts_in_task_on_api_server(
                self.task,
                self.env_vars,
                workdir_only=False,
                file_mounts_blob_id=self.file_mounts_blob_id)
            if len(dag.tasks) != 1:
                raise ValueError('Must only specify one task in the DAG for '
                                 f'a pool. Found {len(dag.tasks)} tasks.')
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
