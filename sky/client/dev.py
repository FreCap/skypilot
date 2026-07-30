"""Client-only development environment manifest support."""

from __future__ import annotations

import dataclasses
import enum
import os
import posixpath
from typing import Any
import unicodedata
import urllib.parse

from sky import exceptions
from sky import task as task_lib
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import yaml_utils

_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_REMOTE_PATH_BYTES = 4096
_DEV_KEYS = frozenset({'version', 'name', 'ide', 'remote_path'})
_UNSUPPORTED_TASK_KEYS = ('run', 'service', 'pool')


class DevEditor(enum.Enum):
    """Desktop editors supported by development environment links."""

    VSCODE = 'vscode'
    CURSOR = 'cursor'
    WINDSURF = 'windsurf'
    ZED = 'zed'
    NONE = 'none'


@dataclasses.dataclass(frozen=True)
class DevConfig:
    """The closed v1 client-only ``dev`` configuration."""

    version: int
    name: str
    ide: DevEditor = DevEditor.VSCODE
    remote_path: str = constants.SKY_REMOTE_WORKDIR


@dataclasses.dataclass(frozen=True)
class DevManifest:
    """A parsed client-only projection and its ordinary SkyPilot Task."""

    config: DevConfig
    task: task_lib.Task
    stripped_yaml: str


def _has_control_character(value: str) -> bool:
    return any(unicodedata.category(char).startswith('C') for char in value)


def _validate_remote_path(remote_path: str, *,
                          allow_home_relative: bool) -> None:
    if not remote_path:
        raise ValueError('dev.remote_path must not be empty.')
    try:
        encoded_path = remote_path.encode('utf-8')
    except UnicodeEncodeError:
        raise ValueError('dev.remote_path must be valid UTF-8.') from None
    if len(encoded_path) > _MAX_REMOTE_PATH_BYTES:
        raise ValueError('dev.remote_path is too long.')
    if _has_control_character(remote_path):
        raise ValueError('dev.remote_path must not contain control characters.')
    is_home_relative = remote_path == '~' or remote_path.startswith('~/')
    if not posixpath.isabs(remote_path) and not (allow_home_relative and
                                                 is_home_relative):
        expected = 'absolute or start with ~/'
        if not allow_home_relative:
            expected = 'absolute'
        raise ValueError(f'dev.remote_path must be {expected}.')


def validate_absolute_remote_path(remote_path: str) -> None:
    """Validates a resolved editor path."""
    _validate_remote_path(remote_path, allow_home_relative=False)


def validate_cluster_name(name: str) -> None:
    """Validates a dev cluster name without echoing the input on failure."""
    if _has_control_character(name):
        raise ValueError('dev.name must not contain control characters.')
    try:
        common_utils.check_cluster_name_is_valid(name)
    except exceptions.InvalidClusterNameError:
        raise ValueError(
            'dev.name must be a valid SkyPilot cluster name.') from None


def _parse_dev_config(raw_config: Any) -> DevConfig:
    if not isinstance(raw_config, dict):
        raise ValueError('The dev section must be a mapping.')
    if any(not isinstance(key, str) for key in raw_config):
        raise ValueError('All keys in the dev section must be strings.')
    if any(_has_control_character(key) for key in raw_config):
        raise ValueError(
            'Field names in the dev section must not contain control '
            'characters.')
    unknown_keys = set(raw_config) - _DEV_KEYS
    if unknown_keys:
        raise ValueError('The dev section contains an unknown field.')

    version = raw_config.get('version')
    if type(version) is not int or version != 1:  # pylint: disable=unidiomatic-typecheck
        raise ValueError('dev.version must be the integer 1.')

    name = raw_config.get('name')
    if not isinstance(name, str) or not name:
        raise ValueError('dev.name must be a non-empty cluster name.')
    validate_cluster_name(name)

    raw_ide = raw_config.get('ide', DevEditor.VSCODE.value)
    if not isinstance(raw_ide, str):
        raise ValueError('dev.ide must be a string.')
    if _has_control_character(raw_ide):
        raise ValueError('dev.ide must not contain control characters.')
    try:
        ide = DevEditor(raw_ide)
    except ValueError:
        choices = ', '.join(editor.value for editor in DevEditor)
        raise ValueError(f'dev.ide must be one of: {choices}.') from None

    remote_path = raw_config.get('remote_path', constants.SKY_REMOTE_WORKDIR)
    if not isinstance(remote_path, str):
        raise ValueError('dev.remote_path must be a string.')
    _validate_remote_path(remote_path, allow_home_relative=True)
    return DevConfig(version=version,
                     name=name,
                     ide=ide,
                     remote_path=remote_path)


def parse_manifest(yaml_str: str) -> DevManifest:
    """Parses one development manifest without contacting an API server."""
    try:
        manifest_size = len(yaml_str.encode('utf-8'))
    except UnicodeEncodeError:
        raise ValueError('Development manifest must be valid UTF-8.') from None
    if manifest_size > _MAX_MANIFEST_BYTES:
        raise ValueError('Development manifest exceeds the 1 MiB limit.')

    configs = yaml_utils.read_yaml_all_str(yaml_str, reject_duplicate_keys=True)
    if len(configs) != 1:
        raise ValueError(
            'Development manifest must contain exactly one YAML document.')
    raw_task_config = configs[0]
    if not isinstance(raw_task_config, dict) or not raw_task_config:
        raise ValueError('Development manifest must be a non-empty mapping.')

    task_config = dict(raw_task_config)
    if 'dev' not in task_config:
        raise ValueError('Development manifest must contain a dev section.')
    dev_config = _parse_dev_config(task_config.pop('dev'))

    for key in _UNSUPPORTED_TASK_KEYS:
        if key in task_config:
            raise ValueError(
                f'Development manifests do not support the {key} field.')
    task_config.setdefault('workdir', '.')

    stripped_yaml = yaml_utils.dump_yaml_str(task_config)
    task = task_lib.Task.from_yaml_str(stripped_yaml)
    if task.num_nodes != 1:
        raise ValueError(
            'Development environments support exactly one node in v1.')
    if task.service is not None or task.run is not None:
        raise ValueError(
            'Development manifests cannot define a run, service, or pool.')
    return DevManifest(config=dev_config,
                       task=task,
                       stripped_yaml=stripped_yaml)


def load_manifest(path: str) -> DevManifest:
    """Loads a bounded UTF-8 development manifest from ``path``."""
    expanded_path = os.path.expanduser(path)
    with open(expanded_path, 'rb') as manifest_file:
        manifest_bytes = manifest_file.read(_MAX_MANIFEST_BYTES + 1)
    if len(manifest_bytes) > _MAX_MANIFEST_BYTES:
        raise ValueError('Development manifest exceeds the 1 MiB limit.')
    try:
        yaml_str = manifest_bytes.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError('Development manifest must be valid UTF-8.') from None

    manifest = parse_manifest(yaml_str)
    yaml_dir = os.path.dirname(os.path.realpath(expanded_path))
    git_commit = common_utils.get_git_commit(yaml_dir)
    if git_commit is not None and 'git_commit' not in manifest.task.metadata:
        # Match the ordinary file-based Task loader.
        # pylint: disable=protected-access
        manifest.task._metadata['git_commit'] = git_commit
    return manifest


def all_resources_have_autostop(task: task_lib.Task) -> bool:
    """Returns whether every resource alternative enables autostop."""
    return bool(task.resources) and all(resource.autostop_config is not None and
                                        resource.autostop_config.enabled
                                        for resource in task.resources)


def resolve_remote_path(remote_path: str, remote_home: str) -> str:
    """Resolves a validated home-relative editor path."""
    _validate_remote_path(remote_path, allow_home_relative=True)
    validate_absolute_remote_path(remote_home)
    if remote_path == '~':
        return remote_home
    if remote_path.startswith('~/'):
        resolved_path = posixpath.join(remote_home, remote_path[2:])
        validate_absolute_remote_path(resolved_path)
        return resolved_path
    return remote_path


def build_editor_uri(editor: DevEditor, cluster_name: str,
                     remote_path: str) -> str | None:
    """Builds a credential-free editor URI for an absolute remote path."""
    if editor is DevEditor.NONE:
        return None
    validate_cluster_name(cluster_name)
    validate_absolute_remote_path(remote_path)
    authority = urllib.parse.quote(cluster_name, safe='')
    path = urllib.parse.quote(remote_path, safe='/')
    if editor is DevEditor.ZED:
        return f'zed://ssh/{authority}{path}'
    return (f'{editor.value}://vscode-remote/'
            f'ssh-remote+{authority}{path}')
