"""Pure construction of canonical paid SkyServe launch requests."""

from collections.abc import Mapping
import copy
import dataclasses
import json
import os
from typing import Any, TYPE_CHECKING

from sky import skypilot_config
from sky import task as task_lib
from sky.client import sdk
from sky.serve import constants
from sky.serve import serve_utils
from sky.utils import config_utils

if TYPE_CHECKING:
    from sky.serve import service_spec

_REPLICA_ID_TEMPLATE_TOKEN = '__SKYPILOT_PAID_REPLICA_ID_TEMPLATE__'
_CLUSTER_NAME_TEMPLATE = 'sky-paid-launch-template'


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidLaunchBodyTemplate:
    """Canonical identity-free launch body for one exact provider location."""

    submitted_bytes: bytes

    def __post_init__(self) -> None:
        prepared = sdk.PreparedLaunchRequest(self.submitted_bytes)
        body = prepared.body
        if (body.cluster_name != _CLUSTER_NAME_TEMPLATE or
                body.extra_launch_context or
                body.task.count(_REPLICA_ID_TEMPLATE_TOKEN) != 1):
            raise ValueError('Paid launch body template is malformed.')


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReplicaLaunchRuntime:
    """One explicit snapshot of process runtime inputs that affect a task."""

    tls_mode: str
    tls_certificate_pem: str | None
    tls_private_key_pem: str | None

    def __post_init__(self) -> None:
        pinned = self.tls_mode == constants.REPLICA_TLS_MODE_PINNED
        complete_material = (isinstance(self.tls_certificate_pem, str) and
                             bool(self.tls_certificate_pem) and
                             isinstance(self.tls_private_key_pem, str) and
                             bool(self.tls_private_key_pem))
        absent_material = (self.tls_certificate_pem is None and
                           self.tls_private_key_pem is None)
        if (not isinstance(self.tls_mode, str) or
                self.tls_mode not in constants.REPLICA_TLS_MODES or
                not (complete_material if pinned else absent_material)):
            raise ValueError('Replica launch TLS runtime is incomplete.')


def capture_replica_launch_runtime() -> ReplicaLaunchRuntime:
    """Capture ambient controller inputs once, before correctness locks."""
    mode = serve_utils.replica_tls_mode()
    if mode != constants.REPLICA_TLS_MODE_PINNED:
        return ReplicaLaunchRuntime(tls_mode=mode,
                                    tls_certificate_pem=None,
                                    tls_private_key_pem=None)
    certificate_pem = os.environ.get(constants.REPLICA_TLS_CERT_ENV_VAR, '')
    private_key_pem = os.environ.get(constants.REPLICA_TLS_KEY_SECRET_ENV_VAR,
                                     '')
    if not certificate_pem or not private_key_pem:
        raise RuntimeError(
            f'{constants.REPLICA_TLS_MODE_ENV_VAR}='
            f'{constants.REPLICA_TLS_MODE_PINNED} requires both '
            f'{constants.REPLICA_TLS_CERT_ENV_VAR} and '
            f'{constants.REPLICA_TLS_KEY_SECRET_ENV_VAR} in the controller '
            'environment.')
    return ReplicaLaunchRuntime(tls_mode=mode,
                                tls_certificate_pem=certificate_pem,
                                tls_private_key_pem=private_key_pem)


def scope_security_group_to_service(task: task_lib.Task,
                                    service_name: str | None) -> None:
    """Pin replicas to one operator-overridable security group per service."""
    if not service_name:
        return
    scoped = f'sky-sg-{service_name}'
    new_resources = []
    for resource in task.resources:
        existing = dict(resource.cluster_config_overrides or {})
        aws_overrides = dict(existing.get('aws', {}))
        if aws_overrides.get('security_group_name'):
            return
        aws_overrides['security_group_name'] = scoped
        existing['aws'] = aws_overrides
        new_resources.append(resource.copy(_cluster_config_overrides=existing))
    task.set_resources(type(task.resources)(new_resources))


def inject_replica_tls_material(task: task_lib.Task,
                                runtime: ReplicaLaunchRuntime) -> None:
    """Inject pinned TLS material from one explicit runtime snapshot."""
    if not isinstance(runtime, ReplicaLaunchRuntime):
        raise TypeError('runtime must be a ReplicaLaunchRuntime.')
    if runtime.tls_mode != constants.REPLICA_TLS_MODE_PINNED:
        return
    assert runtime.tls_certificate_pem is not None
    assert runtime.tls_private_key_pem is not None
    task.update_envs(
        {constants.REPLICA_TLS_CERT_ENV_VAR: runtime.tls_certificate_pem})
    task.update_secrets(
        {constants.REPLICA_TLS_KEY_SECRET_ENV_VAR: runtime.tls_private_key_pem})


def build_replica_launch_task(
    yaml_content: str,
    replica_id: int | str,
    resources_override: dict[str, Any] | None,
    *,
    exact_resources_override: bool,
    authoritative_service_spec: 'service_spec.SkyServiceSpec | None',
    service_name: str | None,
    runtime: ReplicaLaunchRuntime,
    task_template: task_lib.Task | None = None,
) -> task_lib.Task:
    """Build the one exact pre-policy task used by every replica launch."""
    task = (copy.deepcopy(task_template) if task_template is not None
            else serve_utils.load_task_with_service_spec(
                yaml_content, authoritative_service_spec))
    task._user_specified_yaml = None  # pylint: disable=protected-access
    if resources_override is not None:
        resources = task.resources
        if exact_resources_override:
            resource = next(iter(resources)).copy(**resources_override)
            task.set_resources(resource)
        else:
            overridden_resources = [
                resource.copy(**resources_override) for resource in resources
            ]
            task.set_resources(type(resources)(overridden_resources))
    task.update_envs({constants.REPLICA_ID_ENV_VAR: str(replica_id)})
    inject_replica_tls_material(task, runtime)
    scope_security_group_to_service(task, service_name)
    return task


def prepare_paid_launch_request(
    *,
    yaml_content: str,
    authoritative_service_spec: 'service_spec.SkyServiceSpec',
    frozen_controller_config: config_utils.Config,
    resources_override: dict[str, Any],
    replica_id: int | str,
    cluster_name: str,
    workspace: str,
    service_name: str,
    launch_fence: Mapping[str, Any],
    runtime: ReplicaLaunchRuntime,
    task_template: task_lib.Task | None = None,
) -> sdk.PreparedLaunchRequest:
    """Return canonical server-only bytes without database/provider/HTTP I/O."""
    task = build_replica_launch_task(
        yaml_content,
        replica_id,
        resources_override,
        exact_resources_override=True,
        authoritative_service_spec=authoritative_service_spec,
        service_name=service_name,
        runtime=runtime,
        task_template=task_template)
    with skypilot_config.replace_skypilot_config_in_memory(
            frozen_controller_config):
        return sdk.prepare_launch_request_for_server_controller(
            task,
            cluster_name,
            workspace=workspace,
            retry_until_up=False,
            extra_launch_context=dict(launch_fence))


def prepare_paid_launch_body_template(
    *,
    yaml_content: str,
    authoritative_service_spec: 'service_spec.SkyServiceSpec',
    frozen_controller_config: config_utils.Config,
    resources_override: dict[str, Any],
    workspace: str,
    service_name: str,
    runtime: ReplicaLaunchRuntime,
    task_template: task_lib.Task | None = None,
) -> PaidLaunchBodyTemplate:
    """Build one provider-specific request body without member identity."""
    prepared = prepare_paid_launch_request(
        yaml_content=yaml_content,
        authoritative_service_spec=authoritative_service_spec,
        frozen_controller_config=frozen_controller_config,
        resources_override=resources_override,
        replica_id=_REPLICA_ID_TEMPLATE_TOKEN,
        cluster_name=_CLUSTER_NAME_TEMPLATE,
        workspace=workspace,
        service_name=service_name,
        launch_fence={},
        runtime=runtime,
        task_template=task_template)
    return PaidLaunchBodyTemplate(submitted_bytes=prepared.submitted_bytes)


def materialize_paid_launch_request(
    template: PaidLaunchBodyTemplate,
    *,
    replica_id: int,
    cluster_name: str,
    launch_fence: Mapping[str, Any],
) -> sdk.PreparedLaunchRequest:
    """Bind one locked member identity into a canonical body template."""
    if (not isinstance(template, PaidLaunchBodyTemplate) or
            type(replica_id) is not int or replica_id < 1 or
            not isinstance(cluster_name, str) or not cluster_name or
            not isinstance(launch_fence, Mapping)):
        raise ValueError('Paid launch body materialization is malformed.')
    payload = json.loads(template.submitted_bytes)
    task = payload.get('task')
    if (not isinstance(task, str) or
            task.count(_REPLICA_ID_TEMPLATE_TOKEN) != 1 or
            payload.get('cluster_name') != _CLUSTER_NAME_TEMPLATE or
            payload.get('extra_launch_context') != {}):
        raise ValueError('Paid launch body template changed before binding.')
    payload['task'] = task.replace(_REPLICA_ID_TEMPLATE_TOKEN, str(replica_id))
    payload['cluster_name'] = cluster_name
    payload['extra_launch_context'] = dict(launch_fence)
    return sdk.PreparedLaunchRequest(
        submitted_bytes=_canonical_json_bytes(payload))
