"""YAML ingress and serialization helpers for :class:`sky.Task`."""

# This module is the designated YAML gateway for Task's internal state.
# pylint: disable=protected-access

import json
import re
import typing
from typing import Any

from sky import exceptions
from sky import resources as resources_lib
from sky.data import storage as storage_lib
from sky.serve import service_spec
from sky.utils import common_utils
from sky.utils import schemas
from sky.utils import ux_utils
from sky.utils import volume as volume_lib
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:
    from sky import task as task_lib


def _fill_in_env_vars(
    yaml_field: dict[str, Any],
    task_envs: dict[str, str],
) -> dict[str, Any]:
    """Detects env vars in yaml field and fills them with task_envs.

    Use cases of env vars in file_mounts:
    - dst/src paths; e.g.,
        /model_path/llama-${SIZE}b: s3://llama-weights/llama-${SIZE}b
    - storage's name (bucket name)
    - storage's source (local path)

    Use cases of env vars in service:
    - model type; e.g.,
        service:
          readiness_probe:
            path: /v1/chat/completions
            post_data:
              model: $MODEL_NAME
              messages:
                - role: user
                  content: How to print hello world?
              max_tokens: 1

    We simply dump yaml_field into a json string, and replace env vars using
    regex. This should be safe as yaml config has been schema-validated.

    Env vars of the following forms are detected:
        - ${ENV}
        - $ENV
    where <ENV> must appear in task.envs.
    """
    # TODO(zongheng): support ${ENV:-default}?
    yaml_field_str = json.dumps(yaml_field)

    def replace_var(match):
        var_name = match.group(1)
        # If the variable isn't in the dictionary, return it unchanged
        return task_envs.get(var_name, match.group(0))

    # Pattern for valid env var names in bash.
    pattern = r'\$\{?\b([a-zA-Z_][a-zA-Z0-9_]*)\b\}?'
    yaml_field_str = re.sub(pattern, replace_var, yaml_field_str)
    return json.loads(yaml_field_str)


def _parse_secret_name(raw_name: str) -> tuple[str, str | None]:
    """Parse '[secrets:]scope.NAME' into (name, scope_override).

    Supports prefixes: personal., workspace., global.
    Returns (name, None) if no scope prefix found.

    Strips a leading ``secrets:`` if present so the parse is symmetric with
    the YAML emission in ``_to_yaml_config``: refs with no inline mount
    path are written as ``secrets:NAME`` (or ``secrets:scope.NAME``) into
    either the ``secrets:`` array or the ``managed_secrets:`` field. The
    ``secrets:`` array form strips the prefix at its call site; the
    ``managed_secrets:`` form (used when the task also carries inline
    secrets, e.g. an injected service-account token) routes through this
    function, so we strip here to avoid the prefix accumulating across
    YAML round-trips.
    """
    raw_name = common_utils.removeprefix(raw_name, 'secrets:')
    for prefix in ('personal.', 'workspace.', 'global.'):
        if raw_name.startswith(prefix):
            return raw_name[len(prefix):], prefix[:-1]
    return raw_name, None


def from_yaml_config(
    task_cls: type['task_lib.Task'],
    managed_secret_ref_cls: type['task_lib.ManagedSecretRef'],
    config: dict[str, Any],
    env_overrides: list[tuple[str, str]] | None = None,
    secrets_overrides: list[tuple[str, str]] | None = None,
) -> 'task_lib.Task':
    user_specified_yaml = config.pop('_user_specified_yaml',
                                     yaml_utils.dump_yaml_str(config))
    # More robust handling for 'envs': explicitly convert keys and values to
    # str, since users may pass '123' as keys/values which will get parsed
    # as int causing validate_schema() to fail.
    envs = config.get('envs')
    if envs is not None and isinstance(envs, dict):
        new_envs: dict[str, str | None] = {}
        for k, v in envs.items():
            if v is not None:
                new_envs[str(k)] = str(v)
            else:
                new_envs[str(k)] = None
        config['envs'] = new_envs

    # More robust handling for 'secrets': explicitly convert keys and values
    # to str, since users may pass '123' as keys/values which will get
    # parsed as int causing validate_schema() to fail.
    secrets = config.get('secrets')
    if secrets is not None and isinstance(secrets, dict):
        new_secrets: dict[str, str | None] = {}
        for k, v in secrets.items():
            if v is not None:
                new_secrets[str(k)] = str(v)
            else:
                new_secrets[str(k)] = None
        config['secrets'] = new_secrets
    elif secrets is not None and isinstance(secrets, list):
        config['secrets'] = [str(item) for item in secrets]

    common_utils.validate_schema(config, schemas.get_task_schema(),
                                 'Invalid task YAML: ')
    if env_overrides is not None:
        # We must override env vars before constructing the Task, because
        # the Storage object creation is eager and it (its name/source
        # fields) may depend on env vars.
        #
        # FIXME(zongheng): The eagerness / how we construct Task's from
        # entrypoint (YAML, CLI args) should be fixed.
        new_envs = config.get('envs', {})
        new_envs.update(env_overrides)
        config['envs'] = new_envs

    if secrets_overrides is not None:
        # Override secrets vars from CLI.
        existing = config.get('secrets')
        if isinstance(existing, list):
            # Convert list form to dict to merge CLI overrides
            merged: dict[str, str | None] = {}
            for item in existing:
                merged[str(item)] = None
            merged.update(secrets_overrides)
            config['secrets'] = merged
        else:
            new_secrets = existing or {}
            new_secrets.update(secrets_overrides)
            config['secrets'] = new_secrets

    for k, v in config.get('envs', {}).items():
        if v is None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    f'Environment variable {k!r} is None. Please set a '
                    'value for it in task YAML or with --env flag. '
                    f'To set it to be empty, use an empty string ({k}: "" '
                    f'in task YAML or --env {k}="" in CLI).')

    raw_secrets_check = config.get('secrets')
    if isinstance(raw_secrets_check, dict):
        for k, v in raw_secrets_check.items():
            if v is None and not k.startswith('secrets:'):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        f'Secret variable {k!r} is None. Please set a '
                        'value for it in task YAML or with --secret flag.'
                        f' To set it to be empty, use an empty string '
                        f'({k}: "" in task YAML or '
                        f'--secret {k}="" in CLI).')

    # Fill in any Task.envs into file_mounts (src/dst paths, storage
    # name/source).
    env_vars = config.get('envs', {})
    secrets_for_subst = config.get('secrets', {})
    if isinstance(secrets_for_subst, list):
        secrets_for_subst = {}  # Array form has no inline values
    env_and_secrets = env_vars.copy()
    env_and_secrets.update(secrets_for_subst)
    if config.get('file_mounts') is not None:
        config['file_mounts'] = _fill_in_env_vars(config['file_mounts'],
                                                  env_and_secrets)

    # Fill in any Task.envs into service (e.g. MODEL_NAME).
    if config.get('service') is not None:
        config['service'] = _fill_in_env_vars(config['service'],
                                              env_and_secrets)

    # Fill in any Task.envs into workdir
    if config.get('workdir') is not None:
        config['workdir'] = _fill_in_env_vars(config['workdir'],
                                              env_and_secrets)

    if config.get('volumes') is not None:
        config['volumes'] = _fill_in_env_vars(config['volumes'],
                                              env_and_secrets)

    # Split secrets: inline values vs managed references
    raw_secrets = config.pop('secrets', None)
    inline_secrets = {}
    managed_from_secrets = []
    if isinstance(raw_secrets, list):
        # Array form: all items are managed secret references
        for item in raw_secrets:
            if not isinstance(item, str) or not item.startswith('secrets:'):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        f'Invalid secret in array form: {item!r}. '
                        'Array items must use the secrets: prefix '
                        '(e.g., secrets:HF_TOKEN). For inline secrets '
                        'with values, use dict form: '
                        'secrets: {MY_SECRET: "value"}')
            ref_name = item[len('secrets:'):]
            name, scope = _parse_secret_name(ref_name)
            managed_from_secrets.append(
                managed_secret_ref_cls(name=name, scope_override=scope))
    elif isinstance(raw_secrets, dict):
        # Dict form: split inline values vs secrets: prefix refs
        for key, value in raw_secrets.items():
            if key.startswith('secrets:'):
                if value is not None:
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(
                            f'Invalid secret {key!r}: secret '
                            'references (secrets: prefix) must have '
                            'a null value in dict form. To provide '
                            'an inline secret value, remove the '
                            '\'secrets:\' prefix.')
                ref_name = key[len('secrets:'):]
                name, scope = _parse_secret_name(ref_name)
                managed_from_secrets.append(
                    managed_secret_ref_cls(name=name, scope_override=scope))
            else:
                inline_secrets[key] = value

    task = task_cls(
        config.pop('name', None),
        run=config.pop('run', None),
        workdir=config.pop('workdir', None),
        setup=config.pop('setup', None),
        num_nodes=config.pop('num_nodes', None),
        envs=config.pop('envs', None),
        secrets=inline_secrets or None,
        volumes=config.pop('volumes', None),
        event_callback=config.pop('event_callback', None),
        api_server_access=config.pop('api_server_access', True),
        _file_mounts_mapping=config.pop('file_mounts_mapping', None),
        _metadata=config.pop('_metadata', None),
        _user_specified_yaml=user_specified_yaml,
    )

    # Append managed refs from secrets: field (secrets:NAME entries)
    # pylint: disable=protected-access
    for ref in managed_from_secrets:
        task._managed_secret_refs.append(ref)

    # Parse managed_secrets references
    managed_secrets_raw = config.pop('managed_secrets', None)
    if managed_secrets_raw:
        # pylint: disable=protected-access
        for entry in managed_secrets_raw:
            if isinstance(entry, str):
                name, scope = _parse_secret_name(entry)
                task._managed_secret_refs.append(
                    managed_secret_ref_cls(name=name, scope_override=scope))
            elif isinstance(entry, dict):
                for raw_name, opts in entry.items():
                    name, scope = _parse_secret_name(raw_name)
                    task._managed_secret_refs.append(
                        managed_secret_ref_cls(
                            name=name,
                            mount_path=opts.get('mount_path'),
                            scope_override=scope,
                        ))

    # Create lists to store storage objects inlined in file_mounts.
    # These are retained in dicts in the YAML schema and later parsed to
    # storage objects with the storage/storage_mount objects.
    fm_storages = []
    file_mounts = config.pop('file_mounts', None)
    volumes = []
    if file_mounts is not None:
        copy_mounts = {}
        for dst_path, src in file_mounts.items():
            # Check if it is str path
            if isinstance(src, str):
                copy_mounts[dst_path] = src
            # If the src is not a str path, it is likely a dict. Try to
            # parse storage object.
            elif isinstance(src, dict):
                if (src.get('store') ==
                        storage_lib.StoreType.VOLUME.value.lower()):
                    # Build the volumes config for resources.
                    volume_config = {
                        'path': dst_path,
                    }
                    if src.get('name'):
                        volume_config['name'] = src.get('name')
                    persistent = src.get('persistent', False)
                    volume_config['auto_delete'] = not persistent
                    volume_config_detail = src.get('config', {})
                    volume_config.update(volume_config_detail)
                    volumes.append(volume_config)
                    source_path = src.get('source')
                    if source_path:
                        # For volume, copy the source path to the
                        # data directory of the volume mount point.
                        copy_mounts[
                            f'{dst_path.rstrip("/")}/data'] = source_path
                else:
                    fm_storages.append((dst_path, src))
            else:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(f'Unable to parse file_mount '
                                     f'{dst_path}:{src}')
        task.set_file_mounts(copy_mounts)

    task_storage_mounts: dict[str, storage_lib.Storage] = {}
    all_storages = fm_storages
    for storage in all_storages:
        mount_path = storage[0]
        assert mount_path, 'Storage mount path cannot be empty.'
        try:
            storage_obj = storage_lib.Storage.from_yaml_config(storage[1])
        except exceptions.StorageSourceError as e:
            # Patch the error message to include the mount path, if included
            e.args = (e.args[0].replace('<destination_path>',
                                        mount_path),) + e.args[1:]
            raise e
        task_storage_mounts[mount_path] = storage_obj
    task.set_storage_mounts(task_storage_mounts)

    if config.get('inputs') is not None:
        inputs_dict = config.pop('inputs')
        assert len(inputs_dict) == 1, 'Only one input is allowed.'
        inputs = list(inputs_dict.keys())[0]
        estimated_size_gigabytes = list(inputs_dict.values())[0]
        # TODO: allow option to say (or detect) no download/egress cost.
        task.set_inputs(inputs=inputs,
                        estimated_size_gigabytes=estimated_size_gigabytes)

    if config.get('outputs') is not None:
        outputs_dict = config.pop('outputs')
        assert len(outputs_dict) == 1, 'Only one output is allowed.'
        outputs = list(outputs_dict.keys())[0]
        estimated_size_gigabytes = list(outputs_dict.values())[0]
        task.set_outputs(outputs=outputs,
                         estimated_size_gigabytes=estimated_size_gigabytes)

    # Handle the top-level config field
    config_override = config.pop('config', None) or {}
    # Lifecycle hooks live under `config.hooks:` but they are not a
    # SkyPilot-config override — they are task lifecycle metadata.
    # Pull them out of the override block and forward to Resources
    # via the same path master's `resources.hooks:` used (kept for
    # backward compat — see below).
    config_hooks = config_override.pop('hooks', None)

    # Store the final config override for use in resource setup.
    # Restore None semantics if the override block was hooks-only.
    cluster_config_override = config_override or None

    # Parse resources field. Coerce `resources: null` / `resources:`
    # (empty value) to {} so the assignment below doesn't fail.
    resources_config = config.pop('resources', None) or {}
    # `resources.hooks:` was an in-flight rename during PR1 review;
    # it never landed in master. Reject the form explicitly so users
    # write `config.hooks:` (the canonical placement) instead of
    # discovering the rename via Resources-internal silent acceptance.
    if 'hooks' in resources_config:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Lifecycle hooks live under top-level `config.hooks:`, '
                'not `resources.hooks:`. Move the list to '
                '`config.hooks` at the top level of the task YAML.')
    # Forward task.config.hooks into the resources block so the
    # Resources constructor can pick it up via the same key the
    # internal API uses.
    if config_hooks is not None:
        resources_config['hooks'] = config_hooks
    if cluster_config_override is not None:
        assert resources_config.get('_cluster_config_overrides') is None, (
            'Cannot set _cluster_config_overrides in both resources and '
            'experimental.config_overrides')
        resources_config['_cluster_config_overrides'] = cluster_config_override
    if volumes:
        resources_config['volumes'] = volumes
    task.set_resources(
        resources_lib.Resources.from_yaml_config(resources_config))

    service = config.pop('service', None)
    pool = config.pop('pool', None)
    if service is not None and pool is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Cannot set both service and pool in the same task.')

    if service is not None:
        service = service_spec.SkyServiceSpec.from_yaml_config(service)
        task.set_service(service)
    elif pool is not None:
        # When pool is a dict (from top-level pool: in YAML), wrap it
        # properly The schema expects {'pool': {...}} structure, not
        # {'workers': 1, 'pool': True}
        if isinstance(pool, dict):
            # pool is a dict like {'workers': 1, 'max_workers': 3}
            # Wrap it as {'pool': {'workers': 1, 'max_workers': 3}}
            pool_config_dict = {'pool': pool}
        else:
            # pool is a boolean True (shouldn't happen, but handle it)
            pool_config_dict = {'pool': {}}
        pool_spec = service_spec.SkyServiceSpec.from_yaml_config(
            pool_config_dict)
        task.set_service(pool_spec)

    volume_mounts = config.pop('volume_mounts', None)
    if volume_mounts is not None:
        task.volume_mounts = []
        for vol in volume_mounts:
            common_utils.validate_schema(vol, schemas.get_volume_mount_schema(),
                                         'Invalid volume mount config: ')
            volume_mount = volume_lib.VolumeMount.from_yaml_config(vol)
            task.volume_mounts.append(volume_mount)

    assert not config, f'Invalid task args: {config.keys()}'
    return task


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
