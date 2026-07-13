"""YAML serialization helpers for :class:`sky.Task`."""

# This module is the designated serializer for Task's internal state.
# pylint: disable=protected-access

import typing
from typing import Any

from sky import resources as resources_lib

if typing.TYPE_CHECKING:
    from sky import task as task_lib


def to_yaml_config(task: 'task_lib.Task',
                   redact_secrets: bool = False) -> dict[str, Any]:
    """Projects a Task into its YAML-compatible dictionary form."""
    config = {}

    def add_if_not_none(key, value, no_empty: bool = False):
        if no_empty and not value:
            return
        if value is not None:
            config[key] = value

    add_if_not_none('name', task.name)

    tmp_resource_config = resources_to_config(task.resources,
                                              redact_secrets=redact_secrets)
    add_if_not_none('resources', tmp_resource_config)

    # Lifecycle hooks are stored on each Resources instance (the internal API
    # replicates them across all resources), but their canonical YAML placement
    # is the task-level ``config.hooks:``.
    task_hooks: list[dict[str, Any]] | None = None
    for resource in task.resources:
        if resource.hooks:
            task_hooks = [dict(hook) for hook in resource.hooks]
            break
    if task_hooks:
        config.setdefault('config', {})['hooks'] = task_hooks

    if task.service is not None:
        add_if_not_none('service', task.service.to_yaml_config())

    add_if_not_none('num_nodes', task.num_nodes)

    if task.inputs is not None:
        add_if_not_none('inputs',
                        {task.inputs: task.estimated_inputs_size_gigabytes})
    if task.outputs is not None:
        add_if_not_none('outputs',
                        {task.outputs: task.estimated_outputs_size_gigabytes})

    add_if_not_none('setup', task.setup)
    add_if_not_none('workdir', task.workdir)
    add_if_not_none('event_callback', task.event_callback)
    add_if_not_none('run', task.run)
    add_if_not_none('envs', task.envs, no_empty=True)

    secrets = task.secrets
    has_refs = any(key.startswith('secrets:') for key in (secrets or {}))
    has_refs = has_refs or bool(task._managed_secret_refs)

    if secrets and not has_refs:
        if not redact_secrets:
            secrets = {
                key: value.get_secret_value() for key, value in secrets.items()
            }
        else:
            secrets = {key: '<redacted>' for key in secrets}
        add_if_not_none('secrets', secrets, no_empty=True)
    elif secrets or has_refs:
        inline = {
            key: value
            for key, value in (secrets or {}).items()
            if not key.startswith('secrets:')
        }
        if inline:
            if not redact_secrets:
                inline = {
                    key: value.get_secret_value()
                    for key, value in inline.items()
                }
            else:
                inline = {key: '<redacted>' for key in inline}
            config['secrets'] = inline

        ref_list = sorted(
            key for key in (secrets or {}) if key.startswith('secrets:'))
        managed_secrets_field: list = []
        if task._managed_secret_refs:
            for ref in task._managed_secret_refs:
                prefix = (f'{ref.scope_override}.'
                          if ref.scope_override else '')
                if ref.mount_path is not None:
                    managed_secrets_field.append(
                        {f'{prefix}{ref.name}': {
                            'mount_path': ref.mount_path
                        }})
                else:
                    ref_list.append(f'secrets:{prefix}{ref.name}')

        if ref_list:
            existing = config.get('secrets')
            if isinstance(existing, dict):
                managed_secrets_field.extend(ref_list)
            else:
                config['secrets'] = ref_list

        if managed_secrets_field:
            config['managed_secrets'] = managed_secrets_field

    add_if_not_none('file_mounts', {})
    if task.file_mounts is not None:
        config['file_mounts'].update(task.file_mounts)
    if task.storage_mounts is not None:
        config['file_mounts'].update({
            mount_path: storage.to_yaml_config()
            for mount_path, storage in task.storage_mounts.items()
        })

    add_if_not_none('file_mounts_mapping', task.file_mounts_mapping)
    add_if_not_none('volumes', task.volumes)
    if task.volume_mounts is not None:
        config['volume_mounts'] = [
            volume_mount.to_yaml_config() for volume_mount in task.volume_mounts
        ]
    if not task._api_server_access:
        config['api_server_access'] = False
    add_if_not_none('_metadata', task._metadata if task._metadata else None)
    add_if_not_none('_user_specified_yaml', task._user_specified_yaml)
    return config


def resources_to_config(
    resources: list[resources_lib.Resources] | set[resources_lib.Resources],
    factor_out_common_fields: bool = False,
    redact_secrets: bool = False,
) -> dict[str, Any]:
    """Serializes one or more resource alternatives."""
    if len(resources) > 1:
        resource_list: list[dict[str, str | int]] = []
        for resource in resources:
            resource_list.append(
                resource.to_yaml_config(redact_secrets=redact_secrets))
        group_key = 'ordered' if isinstance(resources, list) else 'any_of'
        if factor_out_common_fields:
            return _factor_out_common_resource_fields(resource_list, group_key)
        return {group_key: resource_list}
    return list(resources)[0].to_yaml_config(redact_secrets=redact_secrets)


def _factor_out_common_resource_fields(
    configs: list[dict[str, str | int]],
    group_key: str,
) -> dict[str, Any]:
    """Factors out fields that are common to all resource alternatives."""
    return_config: dict[str, Any] = configs[0].copy()
    if len(configs) > 1:
        for config in configs[1:]:
            for key, value in config.items():
                if key in return_config and return_config[key] != value:
                    del return_config[key]
    num_empty_configs = 0
    for config in configs:
        keys_to_delete = []
        for key in config:
            if key in return_config:
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del config[key]
        if not config:
            num_empty_configs += 1

    if num_empty_configs == len(configs):
        return return_config
    if configs:
        return_config[group_key] = configs
    return return_config
