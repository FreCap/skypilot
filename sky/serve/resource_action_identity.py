"""Pure, non-authorizing projection of immutable SkyServe YAML.

This module deliberately owns no database, pickle, provider, or request state.
Its only inputs are an immutable version-spec YAML and its locked relational
identity. The result is only a potential identity: arbitrary command and
environment values can contribute to its hashes, so no caller may persist it
without the separate locked source/secret/representability proof. The exact
YAML bytes are committed separately by the launch source.
"""

from __future__ import annotations

from typing import Any, NoReturn
import uuid

from sky import task as task_lib
from sky.serve import resource_actions
from sky.serve import service_spec as service_spec_lib
from sky.utils import yaml_utils


class ServeServiceVersionIdentityProjectionError(ValueError):
    """Immutable YAML cannot enter the narrow M4 identity domain."""


class ServeServiceVersionIdentityVerificationError(
        ServeServiceVersionIdentityProjectionError):
    """A committed identity differs from its immutable YAML projection."""


def _fail(reason: str) -> NoReturn:
    raise ServeServiceVersionIdentityProjectionError(reason)


def _contains_value(value: Any) -> bool:
    return value not in (None, {}, [], ())


def _reject_resource_internal_keys(resources: Any, *, subject: str) -> None:
    """Reject internal resource fields without inspecting labels or env data."""

    stack: list[Any] = [resources]
    visited: set[int] = set()
    while stack:
        value = stack.pop()
        if type(value) is list:
            stack.extend(value)
            continue
        if type(value) is not dict:
            continue
        value_id = id(value)
        if value_id in visited:
            continue
        visited.add(value_id)
        for key in value:
            if type(key) is not str:
                _fail(f'{subject} contains a non-text resource key.')
            if key.startswith('_'):
                _fail(f'{subject} contains internal or provenance state.')
        for group_key in ('any_of', 'ordered'):
            group = value.get(group_key)
            if type(group) is list:
                stack.extend(group)


def _reject_internal_config_keys(config: dict[str, Any], *,
                                 subject: str) -> None:
    """Reject internal root/resource fields, not arbitrary user map keys."""

    for key in config:
        if type(key) is not str:
            _fail(f'{subject} contains a non-text key.')
        if key.startswith('_'):
            _fail(f'{subject} contains internal or provenance state.')
    _reject_resource_internal_keys(config.get('resources'), subject=subject)


def _reject_raw_secrets_and_provenance(config: dict[str, Any]) -> None:
    """Reject declared secret carriers before constructing effective values."""

    _reject_internal_config_keys(config, subject='Immutable service YAML')

    for field in ('secrets', 'managed_secrets'):
        if _contains_value(config.get(field)):
            _fail('Secret-bearing task content is not M4-eligible.')
    service = config.get('service')
    if type(service) is dict and _contains_value(service.get('tls')):
        _fail('TLS-bearing service content is not M4-eligible.')


def _reject_effective_internal_keys(value: Any) -> None:
    """Reject every internal field after the one provenance-key removal."""

    if type(value) is not dict:
        _fail('Effective configuration must contain a mapping.')
    _reject_internal_config_keys(value, subject='Effective configuration')


def _validate_capacity_profile(
    task: task_lib.Task,
    service: service_spec_lib.SkyServiceSpec,
) -> resource_actions.ServeActionCapacityProfileV1:
    """Fail closed unless the parsed task is the exact initial M4 profile."""

    if service.pool:
        _fail('SkyServe pools are not M4-eligible.')
    if type(task.num_nodes) is not int or task.num_nodes != 1:
        _fail('M4 requires exactly one physical node per replica.')
    if service.spot_placer is not None:
        _fail('Spot placement is not M4-eligible.')
    if service.reserved_capacity_fill:
        _fail('Reserved-capacity fill is not M4-eligible.')
    if service.cost_rebalance:
        _fail('Cost rebalance is not M4-eligible.')
    if service.dynamic_ondemand_fallback not in (None, False):
        _fail('Dynamic on-demand fallback is not M4-eligible.')
    if service.base_ondemand_fallback_replicas not in (None, 0):
        _fail('Base on-demand fallback is not M4-eligible.')
    if service.uses_logical_replicas:
        _fail('Logical replicas are not M4-eligible.')
    if service.tls_credential is not None:
        _fail('TLS-bearing service content is not M4-eligible.')

    if (task.secrets or task.managed_secret_refs or task.file_mounts or
            task.storage_mounts or task.volume_mounts or task.volumes):
        _fail('Secrets, mounts, and volumes are not M4-eligible.')
    resources = tuple(task.resources)
    if len(resources) != 1:
        _fail('M4 requires exactly one resource alternative.')
    resource = resources[0]
    if type(resource.use_spot) is not bool or resource.use_spot:
        _fail('Spot resources are not M4-eligible.')
    if resource.accelerators is not None:
        _fail('Accelerator resources are not M4-eligible.')
    if resource.cloud is None or str(resource.cloud).lower() != 'kubernetes':
        _fail('M4 requires one explicit Kubernetes resource profile.')
    if resource.hooks:
        _fail('Custom resource lifecycle hooks are not M4-eligible.')

    return (resource_actions.ServeActionCapacityProfileV1.
            ordinary_ondemand_physical_width1())


def _parse_effective_configs(
    yaml_content: str,
) -> tuple[dict[str, Any], dict[str, Any],
           resource_actions.ServeActionCapacityProfileV1]:
    if type(yaml_content) is not str:
        raise TypeError('yaml_content must be immutable text.')
    try:
        raw = yaml_utils.read_yaml_str(yaml_content, reject_duplicate_keys=True)
    except ValueError as e:
        raise ServeServiceVersionIdentityProjectionError(str(e)) from None
    if type(raw) is not dict:
        _fail('Immutable service YAML must contain one mapping.')
    _reject_raw_secrets_and_provenance(raw)
    if type(raw.get('service')) is not dict or raw.get('pool') is not None:
        _fail('Immutable service YAML must contain one non-pool service.')

    try:
        # This is Serve's actual task entrypoint.  It invokes the actual
        # SkyServiceSpec.from_yaml_config() constructor after task-level env
        # interpolation, and sets the exact YAML provenance carrier.
        task = task_lib.Task.from_yaml_str(yaml_content)
    except (TypeError, ValueError):
        _fail('Immutable service YAML is not a valid SkyServe task.')
    service = task.service
    if type(service) is not service_spec_lib.SkyServiceSpec:
        _fail('Immutable service YAML did not construct a SkyServiceSpec.')
    capacity_profile = _validate_capacity_profile(task, service)

    try:
        service_config = service.to_yaml_config()
        task_config = task.to_yaml_config(use_user_specified_yaml=False)
    except (TypeError, ValueError):
        _fail('Effective service or task configuration is not projectable.')
    if type(service_config) is not dict or type(task_config) is not dict:
        _fail('Effective service and task configurations must be mappings.')
    if task_config.get('_user_specified_yaml') != yaml_content:
        _fail('Effective task YAML provenance does not match the immutable '
              'version input.')
    task_config = dict(task_config)
    del task_config['_user_specified_yaml']
    _reject_effective_internal_keys(service_config)
    _reject_effective_internal_keys(task_config)
    return service_config, task_config, capacity_profile


def project_potential_serve_service_version_spec_identity_v1(
    *,
    yaml_content: str,
    service_name: str,
    service_incarnation: uuid.UUID | str,
    service_version: int,
) -> resource_actions.ServeServiceVersionSpecIdentityV1:
    """Project a structural candidate that grants no persistence authority."""

    service_config, task_config, capacity_profile = _parse_effective_configs(
        yaml_content)
    try:
        effective_service_sha256 = resource_actions.canonical_sha256(
            service_config)
        effective_task_sha256 = resource_actions.canonical_sha256(task_config)
    except (TypeError, ValueError):
        _fail('Effective service or task configuration is outside the '
              'canonical M4 JSON domain.')
    return resource_actions.ServeServiceVersionSpecIdentityV1.from_value({
        'version': 1,
        'service_name': service_name,
        'service_incarnation':
            (str(service_incarnation) if type(service_incarnation) is uuid.UUID
             else service_incarnation),
        'service_version': service_version,
        'effective_service_config_sha256': effective_service_sha256,
        'effective_task_config_sha256': effective_task_sha256,
        'capacity_profile': capacity_profile.canonical_value(),
        'provider_profile':
            resource_actions.ProviderProfile.POD_CLUSTER_V1.value,
    })


def verify_locked_serve_service_version_spec_identity_v1(
    *,
    yaml_content: str,
    service_name: str,
    service_incarnation: uuid.UUID | str,
    service_version: int,
    committed_identity: resource_actions.ServeServiceVersionSpecIdentityV1,
    committed_identity_sha256: str,
) -> resource_actions.ServeServiceVersionSpecIdentityV1:
    """Reproject and verify one separately authorized committed identity.

    The caller must hold the immutable version row lock and must have decoded
    the stored identity/hash pair from that row.  This helper never makes the
    potential projector a persistence authority: it returns only after the
    independently committed typed value and hash match the projection exactly.
    """

    if type(committed_identity) is not (
            resource_actions.ServeServiceVersionSpecIdentityV1):
        raise TypeError('committed_identity has an invalid type.')
    if (type(committed_identity_sha256) is not str or
            len(committed_identity_sha256) != 64 or
            any(character not in '0123456789abcdef'
                for character in committed_identity_sha256)):
        raise ValueError(
            'committed_identity_sha256 must be lowercase SHA-256 text.')
    if committed_identity_sha256 != committed_identity.sha256:
        raise ServeServiceVersionIdentityVerificationError(
            'Committed identity hash differs from its canonical bytes.')
    projected = project_potential_serve_service_version_spec_identity_v1(
        yaml_content=yaml_content,
        service_name=service_name,
        service_incarnation=service_incarnation,
        service_version=service_version)
    if (projected.canonical_bytes != committed_identity.canonical_bytes or
            projected.sha256 != committed_identity_sha256):
        raise ServeServiceVersionIdentityVerificationError(
            'Committed identity differs from immutable YAML.')
    return projected


__all__ = [
    'ServeServiceVersionIdentityProjectionError',
    'ServeServiceVersionIdentityVerificationError',
    'project_potential_serve_service_version_spec_identity_v1',
    'verify_locked_serve_service_version_spec_identity_v1',
]
