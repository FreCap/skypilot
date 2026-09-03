"""User interface with the SkyServe."""
import base64
import collections
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
import concurrent.futures
import contextlib
import contextvars
import dataclasses
import datetime
import enum
import errno
import hashlib
import json
import logging
import math
import os
import pathlib
import pickle
import re
import shlex
import shutil
import stat
import tempfile
import threading
import time
import traceback
import typing
from typing import Any, TextIO
import uuid

import colorama
import filelock

from sky import backends
from sky import exceptions
from sky import global_user_state
from sky import resources as resources_lib
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.client import sdk
from sky.container_images import task_utils as container_image_task_utils
from sky.jobs import state as managed_job_state
from sky.serve import auth_tokens
from sky.serve import constants
from sky.serve import controller_transport
from sky.serve import demand_state
from sky.serve import maintenance
from sky.serve import provider_phase
from sky.serve import request_aggregator
from sky.serve import serve_state
from sky.serve import serve_status_formatter
from sky.serve import spot_placer
from sky.server import constants as server_constants
from sky.server import runtime_profile
from sky.server.requests import request_names
from sky.skylet import constants as skylet_constants
from sky.skylet import job_lib
from sky.utils import annotations
from sky.utils import command_runner
from sky.utils import common_utils
from sky.utils import context as sky_context
from sky.utils import controller_utils
from sky.utils import debug_dump_helpers
from sky.utils import locks
from sky.utils import log_utils
from sky.utils import message_utils
from sky.utils import resources_utils
from sky.utils import status_lib
from sky.utils import thread_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.utils.serve_types import ServiceComponent
from sky.utils.serve_types import UpdateMode

if typing.TYPE_CHECKING:
    import fastapi
    import psutil
    import requests

    import sky
    from sky.backends.cloud_vm_ray_backend import CloudVmRayResourceHandle
    from sky.data import storage as storage_lib
    from sky.serve import replica_managers
    from sky.serve import service_spec as service_spec_lib
else:
    psutil = adaptors_common.LazyImport('psutil')
    requests = controller_transport.requests

kueue_lane_observer = adaptors_common.LazyImport(
    'sky.serve.kueue_lane_observer')
task_lib = adaptors_common.LazyImport('sky.task')

logger: logging.Logger = sky_logging.init_logger(__name__)
controller_transport.logger = logger

_LEGACY_HA_CONFIG_BLOCK_BEGIN = '# SKY_SERVE_CONFIG_SNAPSHOT_BEGIN'
_LEGACY_HA_CONFIG_BLOCK_END = '# SKY_SERVE_CONFIG_SNAPSHOT_END'
_VERSIONED_HA_CONFIG_MARKER = constants.VERSIONED_HA_CONFIG_RECOVERY_MARKER
_HA_RECOVERY_PYTHON_EXECUTABLE_RE = re.compile(r'python(?:3(?:[.][0-9]+)?)?\Z')
_HA_RECOVERY_SKY_PYTHON_CMD_TOKENS = tuple(
    shlex.split(skylet_constants.SKY_PYTHON_CMD))
# This grammar is owned by ``write_config_snapshot_receipt()``. Keep it
# deliberately exact: recovery and GC must never treat an arbitrary dotfile
# that merely shares the prefix as one of our receipts.
_CONFIG_RECEIPT_TEMP_FILE_PATTERN = re.compile(
    r'\.config-receipt-[0-9a-f]{32}\.tmp\Z')
_HA_CONFIG_SAFE_TOP_LEVEL_KEYS = frozenset({
    'active_workspace',
    'allowed_clouds',
    'aws',
    'azure',
    'container_registries',
    'data',
    'gcp',
    'jobs',
    'kubernetes',
    'nebius',
    'nvidia_gpus',
    'oci',
    'provision',
    'serve',
    'slurm',
    'ssh',
    'vast',
    'workspaces',
})
_HA_CONFIG_RECURSIVE_SENSITIVE_KEYS = frozenset({
    '_metadata',
    'additional_labels',
    'additional_tags',
    'annotations',
    'external_id',
    'create_instance_kwargs',
    'custom_metadata',
    'instance_tags',
    'labels',
    'pod_config',
    'post_provision_runcmd',
    'sbatch_options',
    'ssh_proxy_command',
})
# Plugin-registered Kubernetes properties are intentionally absent. They may
# have arbitrary schemas and credential semantics, so they cannot be placed in
# a durable DB script without an explicit safe-persistence contract.
_HA_CONFIG_SAFE_KUBERNETES_KEYS = frozenset({
    'allowed_contexts',
    'allowed_nodes',
    'apt_mirrors',
    'auto_mounts',
    'autoscaler',
    'context_configs',
    'dws',
    'disabled',
    'enable_docker',
    'high_availability',
    'kueue',
    'namespace',
    'networking',
    'ports',
    'pricing',
    'provision_timeout',
    'quota',
    'remote_identity',
    'serve_controller_priority_class_name',
    'set_pod_resource_limits',
})
_HA_CONFIG_SAFE_CONTROLLER_KEYS = frozenset({
    'autostop',
    'consolidation_mode',
    'controller_logs_gc_retention_hours',
    'high_availability',
    'task_logs_gc_retention_hours',
})
_HA_CONFIG_SAFE_JOBS_KEYS = frozenset({'controller'})


def sanitize_ha_recovery_config_bytes(config_bytes: bytes) -> bytes:
    """Project a controller config onto its safe durable-recovery subset."""
    if len(config_bytes) > 1024 * 1024:
        raise ValueError('Controller config snapshot exceeds the 1MiB '
                         'HA-recovery limit.')
    try:
        config = yaml_utils.safe_load_value_free(
            config_bytes.decode('utf-8')) or {}
    except (UnicodeDecodeError, ValueError) as e:
        raise ValueError('Controller config snapshot is not valid YAML.') from e

    if not isinstance(config, dict):
        raise ValueError('Controller config snapshot must be a YAML mapping.')
    for key in list(config):
        if key not in _HA_CONFIG_SAFE_TOP_LEVEL_KEYS:
            config.pop(key)

    visited: set[int] = set()
    visiting: set[int] = set()

    def _strip_sensitive(node: Any) -> None:
        if not isinstance(node, (dict, list)):
            return
        node_id = id(node)
        if node_id in visiting:
            raise ValueError('Controller config snapshot contains a cyclic '
                             'YAML alias.')
        if node_id in visited:
            return
        visiting.add(node_id)
        if isinstance(node, dict):
            for key, child in list(node.items()):
                if key in _HA_CONFIG_RECURSIVE_SENSITIVE_KEYS:
                    node.pop(key)
                else:
                    _strip_sensitive(child)
        else:
            for child in node:
                _strip_sensitive(child)
        visiting.remove(node_id)
        visited.add(node_id)

    def _project_kubernetes(block: Any) -> None:
        if not isinstance(block, dict):
            return
        for key in list(block):
            if key not in _HA_CONFIG_SAFE_KUBERNETES_KEYS:
                block.pop(key)
        context_configs = block.get('context_configs')
        if isinstance(context_configs, dict):
            for context_config in context_configs.values():
                _project_kubernetes(context_config)
        quota = block.get('quota')
        if isinstance(quota, dict):
            queue = quota.get('queue')
            quota.clear()
            if isinstance(queue, str):
                quota['queue'] = queue

    def _project_controller_block(block: Any) -> None:
        if not isinstance(block, dict):
            return
        for key in list(block):
            if key not in _HA_CONFIG_SAFE_JOBS_KEYS:
                block.pop(key)
        controller = block.get('controller')
        if isinstance(controller, dict):
            for key in list(controller):
                if key not in _HA_CONFIG_SAFE_CONTROLLER_KEYS:
                    controller.pop(key)

    _strip_sensitive(config)
    _project_kubernetes(config.get('kubernetes'))
    _project_controller_block(config.get('jobs'))
    _project_controller_block(config.get('serve'))
    # A controller is permanently bound to one durable workspace. Persisting
    # every workspace would unnecessarily copy other tenants' policy (and
    # potentially their provider identity metadata) into this service row.
    active_workspace = config.get('active_workspace')
    workspaces = config.get('workspaces')
    if isinstance(active_workspace, str) and isinstance(workspaces, dict):
        active_workspace_config = workspaces.get(active_workspace)
        if isinstance(active_workspace_config, dict):
            active_workspace_config.pop('allowed_users', None)
            active_workspace_config.pop('private', None)
            _project_kubernetes(active_workspace_config.get('kubernetes'))
            config['workspaces'] = {
                active_workspace: active_workspace_config,
            }
        else:
            config.pop('workspaces', None)
    else:
        config.pop('workspaces', None)
    return yaml_utils.dump_yaml_str(config).encode('utf-8')


def _find_ha_recovery_controller_launch_index(lines: list[str]) -> int:
    """Locate the generated Python command that starts a Serve controller."""
    candidates: list[int] = []
    line_index = 0
    while line_index < len(lines):
        launch_index = line_index
        logical_parts: list[str] = []
        unterminated_continuation = False
        while line_index < len(lines):
            physical_line = lines[line_index].rstrip()
            continued = physical_line.endswith('\\')
            logical_parts.append(
                physical_line[:-1] if continued else physical_line)
            line_index += 1
            if not continued:
                break
            if line_index == len(lines):
                unterminated_continuation = True
        if unterminated_continuation:
            continue
        try:
            tokens = shlex.split(' '.join(logical_parts), comments=True)
        except ValueError:
            # A malformed or non-generated shell block cannot authorize a
            # controller launch. The exact-one check below fails closed if it
            # was the only apparent launch.
            continue
        if not tokens:
            continue
        module_pair_count = sum(
            token == '-m' and index +
            1 < len(tokens) and tokens[index + 1] == 'sky.serve.service'
            for index, token in enumerate(tokens))
        if module_pair_count != 1:
            continue
        direct_python = (_HA_RECOVERY_PYTHON_EXECUTABLE_RE.fullmatch(
            os.path.basename(tokens[0])) is not None)
        direct_args = tokens[1:]
        if direct_args[:1] == ['-u']:
            direct_args = direct_args[1:]
        direct_invocation = direct_python and direct_args[:2] == [
            '-m', 'sky.serve.service'
        ]
        generated_prefix_size = len(_HA_RECOVERY_SKY_PYTHON_CMD_TOKENS)
        generated_invocation = (
            tuple(tokens[:generated_prefix_size])
            == _HA_RECOVERY_SKY_PYTHON_CMD_TOKENS and
            tokens[generated_prefix_size:generated_prefix_size + 3]
            == ['-u', '-m', 'sky.serve.service'])
        if direct_invocation or generated_invocation:
            candidates.append(launch_index)
    if len(candidates) != 1:
        raise ValueError('Cannot locate exactly one generated SkyServe '
                         'controller launch in the HA recovery script.')
    return candidates[0]


def strip_legacy_ha_recovery_config_payload(script: str,
                                            remote_path: str) -> str:
    """Remove historical config bytes and retain the controller launch.

    Consolidation-mode recovery scripts have only ever embedded the controller
    config. New binaries restore the per-version safe projection from
    PostgreSQL before executing this script, so retaining the old one-line
    base64 restore would both duplicate secrets and risk the operating system
    command-argument limit.
    """
    begin_count = script.count(_LEGACY_HA_CONFIG_BLOCK_BEGIN)
    end_count = script.count(_LEGACY_HA_CONFIG_BLOCK_END)
    if begin_count != end_count or begin_count > 1:
        raise ValueError('Malformed legacy Serve HA config markers.')
    if begin_count:
        marked = re.compile(
            rf'(?m)^{re.escape(_LEGACY_HA_CONFIG_BLOCK_BEGIN)}[^\n]*\n.*?^'
            rf'{re.escape(_LEGACY_HA_CONFIG_BLOCK_END)}\n?', re.DOTALL)
        script, count = marked.subn('', script, count=1)
        if count != 1:
            raise ValueError('Malformed legacy Serve HA config block.')

    # Match only the exact historical one-line restore primitive. Recovery
    # scripts may legitimately contain unrelated base64 decoding in an
    # entrypoint; deleting every such line would silently change user code.
    # The path is captured once and must be byte-for-byte identical in the
    # mkdir and redirect positions.
    legacy_restore = re.compile(
        r'^mkdir -p -- "\$\(dirname -- (?P<path>.+)\)" && '
        r'printf %s [A-Za-z0-9+/]+={0,2} \| base64 -d > (?P=path)$')
    original_lines = script.splitlines()
    export_prefix = f'export {skypilot_config.ENV_VAR_SKYPILOT_CONFIG}='
    lines = []
    for index, line in enumerate(original_lines):
        # Remove only our generated marker grammar: immediately followed by
        # the generated config export. An identical line inside arbitrary user
        # shell text is not a protocol signal and must be preserved.
        if (line == _VERSIONED_HA_CONFIG_MARKER and
                index + 1 < len(original_lines) and
                original_lines[index + 1].startswith(export_prefix)):
            continue
        if legacy_restore.fullmatch(line) is not None:
            continue
        lines.append(line)
    config_export = export_prefix + shlex.quote(remote_path)
    rewritten: list[str] = []
    wrote_export = False
    for line in lines:
        if line.startswith(export_prefix):
            if not wrote_export:
                rewritten.append(config_export)
                wrote_export = True
            continue
        rewritten.append(line)
    if not wrote_export:
        launch_index = _find_ha_recovery_controller_launch_index(rewritten)
        rewritten.insert(launch_index, config_export)
    export_index = rewritten.index(config_export)
    rewritten.insert(export_index, _VERSIONED_HA_CONFIG_MARKER)
    scrubbed = '\n'.join(rewritten).rstrip() + '\n'
    if (_LEGACY_HA_CONFIG_BLOCK_BEGIN in scrubbed or
            _LEGACY_HA_CONFIG_BLOCK_END in scrubbed or
            f'{_VERSIONED_HA_CONFIG_MARKER}\n{config_export}' not in scrubbed):
        raise ValueError('Legacy Serve HA config payload was not removed.')
    return scrubbed


def bind_ha_recovery_config_snapshot_receipt(script: str, *, config_path: str,
                                             config_bytes: bytes) -> str:
    """Bind one HA launch to its exact restored controller config bytes.

    Recovery scripts outlive API/controller pods.  Scripts retained before
    guarded child snapshots were introduced have no receipt exports, while a
    script retained for an older version can carry receipts for different
    bytes.  PostgreSQL recovery already selects and restores the exact elected
    version before launch; issue its invocation-local receipt at that boundary
    instead of trusting either form of retained environment.
    """
    config_export = (f'export {skypilot_config.ENV_VAR_SKYPILOT_CONFIG}='
                     f'{shlex.quote(config_path)}')

    def _find_config_marker(candidate_lines: list[str]) -> int | None:
        for index, line in enumerate(candidate_lines[:-1]):
            if (line == _VERSIONED_HA_CONFIG_MARKER and
                    candidate_lines[index + 1] == config_export):
                return index
        return None

    lines = script.splitlines()
    marker_index = _find_config_marker(lines)
    if marker_index is None:
        raise ValueError('HA recovery script is not bound to the restored '
                         'controller config path.')
    launch_index = _find_ha_recovery_controller_launch_index(lines)
    if marker_index >= launch_index:
        raise ValueError('Versioned controller config binding must precede '
                         'the SkyServe controller launch.')

    receipt = skypilot_config.internal_config_snapshot_environment(
        skypilot_config.INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE, config_path,
        config_bytes)
    receipt_prefixes = tuple(f'export {name}=' for name in receipt)
    # Retained scripts can contain the receipt issued when the service was
    # first created.  It is not authority for this recovery invocation, so
    # remove the generated assignments and emit exactly one canonical set.
    lines = [
        line for index, line in enumerate(lines)
        if index >= launch_index or not line.startswith(receipt_prefixes)
    ]
    marker_index = _find_config_marker(lines)
    if marker_index is None:
        raise ValueError('HA recovery script lost its restored controller '
                         'config binding while replacing its receipt.')
    launch_index = _find_ha_recovery_controller_launch_index(lines)
    if marker_index >= launch_index:
        raise ValueError('Versioned controller config binding must precede '
                         'the SkyServe controller launch.')
    receipt_exports = [
        f'export {name}={shlex.quote(value)}'
        for name, value in receipt.items()
    ]
    lines[launch_index:launch_index] = receipt_exports
    return '\n'.join(lines).rstrip() + '\n'


def bind_ha_recovery_owner_fence(
    script: str,
    *,
    service_hash: str,
    lifecycle_epoch: int | None,
    controller_pid: int | None,
    controller_ip: str | None,
    status: serve_state.ServiceStatus,
    recovery_version: int,
) -> str:
    """Bind one HA launch to the exact JIT owner and version snapshot."""
    payload = {
        'service_hash': service_hash,
        'lifecycle_epoch': lifecycle_epoch,
        'controller_pid': controller_pid,
        'controller_ip': controller_ip,
        'status': status.value,
        'recovery_version': recovery_version,
    }
    # Reuse the strict decoder as the single validation contract before the
    # payload is placed in a shell export.
    parse_ha_recovery_owner_fence(
        json.dumps(payload, separators=(',', ':'), sort_keys=True))
    export_prefix = f'export {constants.HA_RECOVERY_OWNER_FENCE_ENV_VAR}='
    lines = script.splitlines()
    if any(line.startswith(export_prefix) for line in lines):
        raise ValueError('HA recovery script already contains an owner fence.')
    launch_index = _find_ha_recovery_controller_launch_index(lines)
    encoded = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    lines.insert(launch_index, export_prefix + shlex.quote(encoded))
    return '\n'.join(lines).rstrip() + '\n'


def parse_ha_recovery_owner_fence(payload: str) -> dict[str, Any]:
    """Decode and strictly validate one invocation-local HA owner fence."""
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError('HA recovery owner fence is not valid JSON.') from e
    expected_keys = {
        'service_hash', 'lifecycle_epoch', 'controller_pid', 'controller_ip',
        'status', 'recovery_version'
    }
    if not isinstance(decoded, dict) or set(decoded) != expected_keys:
        raise ValueError('HA recovery owner fence has an invalid schema.')
    service_hash = decoded['service_hash']
    lifecycle_epoch = decoded['lifecycle_epoch']
    controller_pid = decoded['controller_pid']
    controller_ip = decoded['controller_ip']
    recovery_version = decoded['recovery_version']
    if not isinstance(service_hash, str) or not service_hash:
        raise ValueError('HA recovery owner fence has an invalid service hash.')
    if (lifecycle_epoch is not None and
        (type(lifecycle_epoch) is not int or lifecycle_epoch < 1)):
        raise ValueError(
            'HA recovery owner fence has an invalid lifecycle epoch.')
    if (controller_pid is not None and
        (type(controller_pid) is not int or controller_pid < 1)):
        raise ValueError('HA recovery owner fence has an invalid controller '
                         'PID.')
    if (controller_ip is not None and
        (not isinstance(controller_ip, str) or not controller_ip)):
        raise ValueError('HA recovery owner fence has an invalid controller '
                         'IP.')
    if type(recovery_version) is not int or recovery_version < 1:
        raise ValueError('HA recovery owner fence has an invalid version.')
    try:
        decoded['status'] = serve_state.ServiceStatus(decoded['status'])
    except (TypeError, ValueError) as e:
        raise ValueError(
            'HA recovery owner fence has an invalid status.') from e
    return decoded


# Keep the established serve_utils import and pickle identities while the
# presentation-only implementation lives in its own low-state module.
# pylint: disable=protected-access
_REPLICA_TRUNC_NUM = serve_status_formatter._REPLICA_TRUNC_NUM
_get_replicas = serve_status_formatter._get_replicas
format_service_table = serve_status_formatter.format_service_table
_format_replica_table = serve_status_formatter._format_replica_table
# pylint: enable=protected-access
for _status_formatter_symbol in (
        _get_replicas,
        format_service_table,
        _format_replica_table,
):
    _status_formatter_symbol.__module__ = __name__
del _status_formatter_symbol


class _ClusterYamlHandle(typing.Protocol):
    """Handle interface needed by the batched provider-config reader."""

    @property
    def cluster_yaml(self) -> str | None:
        ...


def get_provider_configs_for_handles(
    handles_by_key: 'typing.Mapping[Any, _ClusterYamlHandle | None]',
    *,
    failed_keys: set[Any] | None = None,
) -> dict[Any, dict[str, Any]]:
    """Fetch provider configs once per unique cluster YAML path.

    Multiple logical replicas can share the same physical cluster and thus the
    same ``cluster_yaml``.  Serve hot paths only need the parsed ``provider``
    block, so reuse one batched YAML read per unique path and fan the result
    back out to every caller key.
    """
    yaml_paths: list[str] = []
    keys_by_yaml: dict[str, list[Any]] = collections.defaultdict(list)
    for key, handle in handles_by_key.items():
        if handle is None:
            continue
        try:
            cluster_yaml = handle.cluster_yaml
        except Exception as error:  # pylint: disable=broad-except
            if failed_keys is not None:
                failed_keys.add(key)
            logger.warning(
                'Deferring Serve provider operations for handle %r: %s', key,
                common_utils.format_exception(error))
            continue
        if not isinstance(cluster_yaml, str):
            if failed_keys is not None:
                failed_keys.add(key)
            continue
        if cluster_yaml not in keys_by_yaml:
            yaml_paths.append(cluster_yaml)
        keys_by_yaml[cluster_yaml].append(key)

    if not yaml_paths:
        return {}

    try:
        yaml_strings = list(
            global_user_state.get_cluster_yaml_str_multiple(yaml_paths))
        if len(yaml_strings) != len(yaml_paths):
            raise ValueError('batched cluster YAML result length mismatch')
    except Exception as batch_error:  # pylint: disable=broad-except
        # A batch-level database failure provides no trustworthy per-handle
        # distinction. Fail this observation closed; falling back to N
        # singleton reads amplifies an unhealthy dependency exactly when the
        # controller needs to remain responsive.
        logger.warning(
            'Batched Serve provider-config read failed; deferring all '
            'matching handles: %s', common_utils.format_exception(batch_error))
        if failed_keys is not None:
            for keys in keys_by_yaml.values():
                failed_keys.update(keys)
        return {}

    provider_configs_by_yaml: dict[str, dict[str, Any]] = {}
    for yaml_path, yaml_string in zip(yaml_paths, yaml_strings, strict=True):
        try:
            if not isinstance(yaml_string, str):
                raise ValueError('cluster YAML is unavailable')
            config = yaml_utils.safe_load(yaml_string)
            if not isinstance(config, dict):
                raise ValueError('cluster YAML root is not a mapping')
            provider_config = config.get('provider')
            if not isinstance(provider_config, dict):
                raise ValueError('cluster YAML provider is not a mapping')
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Deferring Serve provider operations for cluster '
                'YAML %r: %s', yaml_path, common_utils.format_exception(error))
            continue
        provider_configs_by_yaml[yaml_path] = provider_config
    provider_configs: dict[Any, dict[str, Any]] = {}
    for yaml_path, keys in keys_by_yaml.items():
        provider_config = provider_configs_by_yaml.get(yaml_path)
        if provider_config is None:
            if failed_keys is not None:
                failed_keys.update(keys)
            continue
        for key in keys:
            provider_configs[key] = provider_config
    return provider_configs


# Keep the established serve_utils import and pickle identities while the
# security-sensitive implementation lives in its own low-state module.
AuthTokenConfigurationError = auth_tokens.AuthTokenConfigurationError
is_lb_data_plane_auth_enabled = auth_tokens.is_lb_data_plane_auth_enabled
validate_controller_auth_token_isolation = (
    auth_tokens.validate_controller_auth_token_isolation)
get_lb_sync_auth_tokens = auth_tokens.get_lb_sync_auth_tokens
get_controller_admin_auth_tokens = auth_tokens.get_controller_admin_auth_tokens
get_lb_auth_tokens = auth_tokens.get_lb_auth_tokens
for _auth_token_symbol in (
        AuthTokenConfigurationError,
        is_lb_data_plane_auth_enabled,
        validate_controller_auth_token_isolation,
        get_lb_sync_auth_tokens,
        get_controller_admin_auth_tokens,
        get_lb_auth_tokens,
):
    _auth_token_symbol.__module__ = __name__
del _auth_token_symbol

_LAUNCH_QUIESCE_MAX_CANCEL_ROUNDS = 3
_LAUNCH_QUIESCE_TIMEOUT_SECONDS = 60
_LAUNCH_QUIESCE_POLL_SECONDS = 0.5

# Bound on the per-call thread pool used by `get_service_status_pickled` to
# fan out across services/pools. The per-service work is dominated by I/O
# (controller HTTP + DB reads), so threads parallelize well. Capped low so a
# 100-pool deployment doesn't open 100 simultaneous DB connections or
# trigger memory pressure on big pools.
_STATUS_FANOUT_MAX_WORKERS = 8


@dataclasses.dataclass(frozen=True)
class _PurgeResult:
    """Outcome of an immediate purge attempt."""

    completed: bool
    message: str | None = None


# Keep the established serve_utils import and pickle identities while the
# controller HTTP implementation lives in its own low-state gateway.
# pylint: disable=protected-access
_CONTROLLER_HTTP_RETRY_ATTEMPTS: int = (
    controller_transport._CONTROLLER_HTTP_RETRY_ATTEMPTS)
_CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS: float = (
    controller_transport._CONTROLLER_HTTP_RETRY_BACKOFF_SECONDS)
_CONTROLLER_HTTP_TIMEOUT_SECONDS: tuple[float, float] = (
    controller_transport._CONTROLLER_HTTP_TIMEOUT_SECONDS)
ControllerOwnerError = controller_transport.ControllerOwnerError
_ControllerOwner = controller_transport._ControllerOwner
make_controller_owner_fingerprint = (
    controller_transport.make_controller_owner_fingerprint)
_get_controller_url = controller_transport._get_controller_url
_get_local_controller_url = controller_transport._get_local_controller_url
_request_to_controller_with_retry = (
    controller_transport._request_to_controller_with_retry)
_post_to_controller_with_retry = (
    controller_transport._post_to_controller_with_retry)
_get_to_controller_with_retry = (
    controller_transport._get_to_controller_with_retry)
get_service_placement_state = controller_transport.get_service_placement_state
_get_to_local_controller_with_retry = (
    controller_transport._get_to_local_controller_with_retry)
# pylint: enable=protected-access
for _controller_transport_symbol in (
        ControllerOwnerError,
        make_controller_owner_fingerprint,
        _get_controller_url,
        _get_local_controller_url,
        _request_to_controller_with_retry,
        _post_to_controller_with_retry,
        _get_to_controller_with_retry,
        get_service_placement_state,
        _get_to_local_controller_with_retry,
):
    _controller_transport_symbol.__module__ = __name__
del _controller_transport_symbol

# NOTE(dev): We assume log are print with the hint 'sky api logs -l'. Be careful
# when changing UX as this assumption is used to expand some log files while
# ignoring others.
_SKYPILOT_LOG_HINT = r'.*sky api logs -l'
_SKYPILOT_PROVISION_API_LOG_PATTERN = (
    fr'{_SKYPILOT_LOG_HINT} (.*/provision\.log)')
# New hint pattern for provision logs
_SKYPILOT_PROVISION_LOG_CMD_PATTERN = r'.*sky logs --provision\s+(\S+)'
_SKYPILOT_LOG_PATTERN = fr'{_SKYPILOT_LOG_HINT} (.*\.log)'

# TODO(tian): Find all existing replica id and print here.
_FAILED_TO_FIND_REPLICA_MSG = (
    f'{colorama.Fore.RED}Failed to find replica '
    '{replica_id}. Please use `sky serve status [SERVICE_NAME]`'
    f' to check all valid replica id.{colorama.Style.RESET_ALL}')


@dataclasses.dataclass
class ServiceComponentTarget:
    """Represents a target service component with an optional replica ID.
    """
    component: ServiceComponent
    replica_id: int | None = None

    def __init__(self,
                 component: str | ServiceComponent,
                 replica_id: int | None = None):
        if isinstance(component, str):
            component = ServiceComponent(component)
        self.component = component
        self.replica_id = replica_id

    def __post_init__(self):
        """Validate that replica_id is only provided for REPLICA component."""
        if (self.component == ServiceComponent.REPLICA) != (self.replica_id
                                                            is None):
            raise ValueError(
                'replica_id must be specified if and only if component is '
                'REPLICA.')

    def __hash__(self) -> int:
        return hash((self.component, self.replica_id))

    def __str__(self) -> str:
        if self.component == ServiceComponent.REPLICA:
            return f'{self.component.value}-{self.replica_id}'
        return self.component.value


class UserSignal(enum.Enum):
    """User signal to send to controller.

    User can send signal to controller by writing to a file. The controller
    will read the file and handle the signal.
    """
    # Stop the controller, load balancer and all replicas.
    TERMINATE = 'terminate'

    # TODO(tian): Add more signals, such as pause.

    def error_type(self) -> type[Exception]:
        """Get the error corresponding to the signal."""
        return _SIGNAL_TO_ERROR[self]


@dataclasses.dataclass
class TLSCredential:
    """TLS credential for the service."""
    keyfile: str
    certfile: str


DEFAULT_UPDATE_MODE = UpdateMode.ROLLING

_SIGNAL_TO_ERROR = {
    UserSignal.TERMINATE: exceptions.ServeUserTerminatedError,
}

RequestsAggregator = request_aggregator.RequestsAggregator
RequestTimestamp = request_aggregator.RequestTimestamp
for _request_aggregator_symbol in (RequestsAggregator, RequestTimestamp):
    _request_aggregator_symbol.__module__ = __name__
del _request_aggregator_symbol


def get_service_filelock_path(pool: str) -> str:
    # Request serialization must not use an inode inside the canonical service
    # directory. Incarnation teardown deletes that directory; a waiter creating
    # ``<service>/pool.lock`` afterward would bypass the old lock inode and
    # silently lose serialization with the operation already in flight.
    digest = hashlib.sha256(pool.encode('utf-8')).hexdigest()
    path = (pathlib.Path(locks.SKY_LOCKS_DIR) /
            f'.skyserve-request-{digest}.lock').expanduser().absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


class ServiceLifecycleLock:
    """Advisory/file lock paired with a durable monotonically increasing token.

    Mutual exclusion handles the normal case; the epoch handles silent
    PostgreSQL session loss.  Resource mutations additionally use
    incarnation-scoped identities, while authoritative DB commits validate
    this token under a row lock. ``advance_epoch=None`` defers the epoch choice
    until the caller has inspected durable state under the acquired name lock.
    """

    def __init__(self,
                 service_name: str,
                 lock: locks.DistributedLock,
                 *,
                 advance_epoch: bool | None = True) -> None:
        self.service_name = service_name
        self.lock = lock
        self.advance_epoch = advance_epoch
        self.epoch: int | None = None

    def acquire(self) -> 'ServiceLifecycleLock':
        self.lock.acquire()
        try:
            if self.advance_epoch is None:
                # Some operations decide whether they are a mutation of an
                # existing incarnation or a same-name creation only after
                # acquiring the name mutex. They initialize the epoch under
                # this still-held lock with retain_service_lifecycle_epoch()
                # or advance_service_lifecycle_epoch().
                pass
            elif (isinstance(self.lock, locks.PostgresLock) and
                  not self.advance_epoch):
                self.epoch = self.lock.run_in_lock_session(
                    lambda connection: serve_state.read_service_lifecycle_epoch(
                        self.service_name, connection))
            elif isinstance(self.lock, locks.PostgresLock):
                self.epoch = self.lock.run_in_lock_session(
                    lambda connection:
                    serve_state.claim_service_lifecycle_epoch(
                        self.service_name, connection))
            else:
                self.epoch = serve_state.claim_service_lifecycle_epoch(
                    self.service_name)
            if not self.session_is_valid():
                raise RuntimeError('Lifecycle lock session was lost while '
                                   f'acquiring {self.service_name!r}.')
        except BaseException:
            # Executor cancellation is delivered as KeyboardInterrupt.  If it
            # lands while claiming the fencing epoch, release the already-held
            # advisory lock before the worker is reused.
            self.lock.release()
            raise
        return self

    def release(self) -> None:
        self.lock.release()

    def session_is_valid(self) -> bool:
        if isinstance(self.lock, locks.PostgresLock):
            return self.lock.is_session_alive()
        return self.lock.is_locked()

    def __enter__(self) -> 'ServiceLifecycleLock':
        return self.acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def get_service_lifecycle_lock(
        service_name: str,
        *,
        advance_epoch: bool | None = True) -> ServiceLifecycleLock:
    """Return the cross-pod lock serializing destructive service lifecycles.

    The lock ID is outside the service working directory: deleting or
    quarantining that directory must never create a fresh lock inode. In
    PostgreSQL deployments this resolves to an advisory lock shared by every
    API pod; local/SQLite deployments use the runtime-global lock directory.
    """
    # The PostgreSQL epoch claim executes raw SQL on the advisory-lock session,
    # so ensure migration 008 has created its table before acquiring that
    # session and attempting the claim.
    serve_state.ensure_tables_initialized()
    # Generic lock auto-detection intentionally falls back to a local FileLock
    # when DB initialization raises. That is acceptable for best-effort
    # callers, but unsafe here: a transient PostgreSQL/config outage would let
    # multiple HA pods concurrently destroy the same name. Detect explicitly
    # and fail closed instead.
    engine = global_user_state.initialize_and_get_db()
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        lock_type = 'postgres'
    elif engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        lock_type = 'filelock'
    else:
        raise RuntimeError('Unsupported database dialect for service '
                           f'lifecycle lock: {engine.dialect.name!r}.')
    digest = hashlib.sha256(service_name.encode('utf-8')).hexdigest()
    lock = locks.get_lock(f'skyserve-lifecycle-{digest}', lock_type=lock_type)
    return ServiceLifecycleLock(service_name, lock, advance_epoch=advance_epoch)


def lifecycle_lock_is_valid(lock: ServiceLifecycleLock) -> bool:
    """Whether a lifecycle lease still owns both session and durable token."""
    if lock.epoch is None or not lock.session_is_valid():
        return False
    try:
        return serve_state.service_lifecycle_epoch_matches(
            lock.service_name, lock.epoch)
    except Exception:  # pylint: disable=broad-except
        # A DB outage makes the durable fence unverifiable.  Destructive work
        # must stop rather than degrading to a process-local assumption.
        return False


def get_service_lifecycle_epoch(lock: ServiceLifecycleLock) -> int:
    """Return an acquired lifecycle lease's durable token."""
    if lock.epoch is None:
        raise RuntimeError('Service lifecycle lock has not been acquired.')
    return lock.epoch


def retain_service_lifecycle_epoch(lock: ServiceLifecycleLock) -> int:
    """Bind a deferred name lock to an existing controller's epoch."""
    if lock.advance_epoch is not None or lock.epoch is not None:
        raise RuntimeError('Service lifecycle lock is not awaiting an epoch.')
    if not lock.session_is_valid():
        raise RuntimeError('Cannot retain a lost service lifecycle lock.')
    if isinstance(lock.lock, locks.PostgresLock):
        epoch = lock.lock.run_in_lock_session(
            lambda connection: serve_state.read_service_lifecycle_epoch(
                lock.service_name, connection))
    else:
        # Local/SQLite service state still uses the historical per-operation
        # epoch contract. The central PostgreSQL path is the controller-
        # preserving path guarded by the same lock-owning DB session.
        epoch = serve_state.claim_service_lifecycle_epoch(lock.service_name)
    lock.epoch = epoch
    if not lock.session_is_valid():
        raise RuntimeError('Lifecycle lock session was lost while retaining '
                           f'{lock.service_name!r}.')
    return epoch


def advance_service_lifecycle_epoch(lock: ServiceLifecycleLock) -> int:
    """Fence an in-flight lifecycle operation while retaining its name lock."""
    if ((lock.epoch is None and
         (lock.advance_epoch is not None or not lock.session_is_valid())) or
        (lock.epoch is not None and not lifecycle_lock_is_valid(lock))):
        raise RuntimeError('Cannot advance a lost service lifecycle lock.')
    if isinstance(lock.lock, locks.PostgresLock):
        epoch = lock.lock.run_in_lock_session(
            lambda connection: serve_state.claim_service_lifecycle_epoch(
                lock.service_name, connection))
    else:
        epoch = serve_state.claim_service_lifecycle_epoch(lock.service_name)
    lock.epoch = epoch
    if not lock.session_is_valid():
        raise RuntimeError('Lifecycle lock session was lost while fencing '
                           f'{lock.service_name!r}.')
    return epoch


def remove_service_directory(service_dir: str) -> None:
    """Remove one already-fenced incarnation directory.

    New service directories are derived from the durable resource scope, so
    they can be deleted after the service row is removed without any rename or
    canonical-path TOCTOU.  A legacy directory is also safe here: every
    successor created by this version uses a scoped path and therefore cannot
    occupy the old name-only location.
    """
    try:
        if os.path.islink(service_dir):
            os.unlink(service_dir)
        else:
            shutil.rmtree(service_dir)
    except FileNotFoundError:
        pass


def _validate_consolidation_mode_config(current_is_consolidation_mode: bool,
                                        pool: bool) -> None:
    """Validate the consolidation mode config."""
    # Check whether the consolidation mode config is changed.
    controller = controller_utils.get_controller_for_pool(pool).value
    if current_is_consolidation_mode:
        controller_cn = controller.cluster_name
        if global_user_state.cluster_with_name_exists(controller_cn):
            logger.warning(
                f'{colorama.Fore.RED}Consolidation mode for '
                f'{controller.controller_type} is enabled, but the controller '
                f'cluster {controller_cn} is still running. Please terminate '
                'the controller cluster first.'
                f'{colorama.Style.RESET_ALL}')
    else:
        noun = 'pool' if pool else 'service'
        num_services = serve_state.get_num_services(pool=pool)
        if num_services:
            logger.warning(
                f'{colorama.Fore.RED}Consolidation mode for '
                f'{controller.controller_type} is disabled, but there are '
                f'still {num_services} {noun}s running. Please terminate '
                f'those {noun}s first.{colorama.Style.RESET_ALL}')


def _pool_consolidation_extra_validator(arg: bool) -> None:
    """Warn about leftover pools when switching to non-consolidated mode.

    Passed as extra_validator to controller_utils.is_jobs_consolidation_mode
    from the pool branch of is_consolidation_mode. Skipped when consolidation
    is on because the jobs validator already warns about the shared
    controller cluster in that case.
    """
    if not arg:
        _validate_consolidation_mode_config(arg, pool=True)


@annotations.lru_cache(scope='request', maxsize=1)
def is_consolidation_mode(pool: bool = False) -> bool:
    if pool:
        # INVARIANT: pool consolidation state must match managed jobs —
        # pool operations run on the jobs controller. Route both readers
        # through controller_utils.is_jobs_consolidation_mode so they
        # cannot diverge. Pool adds one extra validator (leftover pools)
        # because the jobs validator only knows about leftover jobs.
        return controller_utils.is_jobs_consolidation_mode(
            extra_validator=_pool_consolidation_extra_validator)
    # The external-only Helm topology necessarily runs service controllers in
    # the API pod. Treat its explicit capability signal as authoritative so
    # enabling the chart cannot accidentally launch an obsolete, billable
    # dedicated controller VM because an old persisted config omitted the
    # consolidation flag.
    if (os.environ.get(constants.EXTERNAL_LB_ENABLED_ENV_VAR,
                       '').lower() == 'true'):
        return True
    # Serve (pool=False) otherwise runs on its own controller cluster,
    # independent of the jobs controller, and keeps a config-driven
    # consolidation flag for compatibility outside the Helm topology.
    if os.environ.get(skylet_constants.OVERRIDE_CONSOLIDATION_MODE) is not None:
        # if we are in the serve controller, we must always be in
        # consolidation mode.
        return True
    consolidation_mode = skypilot_config.get_nested(
        ('serve', 'controller', 'consolidation_mode'), default_value=False)
    # We should only do this check on API server, as the controller will not
    # have related config and will always seemingly disabled for consolidation
    # mode. Check #6611 for more details.
    if os.environ.get(skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER) is not None:
        _validate_consolidation_mode_config(consolidation_mode, pool)
    return consolidation_mode


def is_external_load_balancer_mode() -> bool:
    """Whether the external-LB platform capability is enabled.

    Helm injects the same explicit capability into the API pod and every
    generated LB pod. Consolidated controller children inherit the API pod's
    environment. No persisted or per-service config participates, so all
    processes in the topology necessarily agree. False/unset means service
    startup is unsupported (pools remain valid because they have no inference
    endpoint); it no longer selects an in-pod implementation.
    """
    return (os.environ.get(constants.EXTERNAL_LB_ENABLED_ENV_VAR,
                           '').lower() == 'true')


def replica_tls_mode() -> str:
    """Encryption mode for the load-balancer-to-replica hop.

    Read from the same Helm-injected environment as the external-LB capability
    flag, and for the same reason: the controller (which mints and injects the
    key material) and the load balancer (which pins it) must never disagree, or
    the LB would dial https at a plaintext replica, or verify against material
    the replica was never given.
    """
    mode = os.environ.get(constants.REPLICA_TLS_MODE_ENV_VAR,
                          '').strip().lower()
    if not mode:
        return constants.REPLICA_TLS_MODE_OFF
    if mode not in constants.REPLICA_TLS_MODES:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'{constants.REPLICA_TLS_MODE_ENV_VAR}={mode!r} is not one of '
                f'{", ".join(constants.REPLICA_TLS_MODES)}.')
    return mode


def ha_recovery_for_consolidation_mode(pool: bool,
                                       still_leader: Callable[[], bool] |
                                       None = None):
    """Recovery logic for HA mode.

    Args:
        pool: Whether to recover pools (True) or services (False).
        still_leader: Optional probe returning whether this pod still holds
            the consolidation leader lock. Re-checked before every recovery
            launch: the leader lock is session-scoped (a PG advisory lock),
            so it can be silently lost mid-sweep (RDS failover, idle
            timeout), at which point another pod may already be running its
            own recovery. Launching more controllers here without
            leadership would split-brain, so the sweep aborts instead. None
            means the caller has no revocable leadership concept (e.g. a
            single-pod filelock deployment).
    """
    if not pool and maintenance.is_controller_hold_active():
        logger.warning('Skipping SkyServe controller HA recovery while the '
                       'server deployment hold is active.')
        return

    # No setup recovery is needed in consolidation mode, as the API server
    # already has all runtime installed. Directly start jobs recovery here.
    # Refers to sky/templates/kubernetes-ray.yml.j2 for more details.
    runner = command_runner.LocalProcessCommandRunner()
    noun = 'pool' if pool else 'serve'
    capnoun = noun.capitalize()
    prefix = f'{noun}_'
    # Snapshot the set of in-flight _start service names once per iteration
    # so we don't walk /proc N times for N services. This also gives all
    # services a consistent view (no torn read where service A is checked
    # before service B's _start spawns, and B is checked after).
    in_flight_service_incarnations = (
        _snapshot_in_flight_start_service_incarnations())
    with open(skylet_constants.HA_PERSISTENT_RECOVERY_LOG_PATH.format(prefix),
              'w',
              encoding='utf-8') as f:
        start = time.time()
        f.write(f'Starting HA recovery at {datetime.datetime.now()}\n')
        # Snapshot only the mode this sweep can recover. Pools never own
        # external LBs, so the service-mode snapshot is also the complete set
        # of live LB owners used by reconciliation below.
        service_names = serve_state.get_glob_service_names(None, pool=pool)
        # One slim query for the whole sweep instead of a per-service joined
        # read (which deserializes the latest spec N times). The snapshot
        # carries every field this loop consumes, including the latest
        # version's raw yaml for placeholder detection.
        liveness_snapshots = {
            record['name']: record
            for record in serve_state.get_service_liveness_snapshots(pool=pool)
        }
        committed_version_candidates = [
            service_name for service_name in service_names
            if ((svc := liveness_snapshots.get(service_name)) is None or
                svc.get('yaml_content') is None)
        ]
        latest_committed_versions = serve_state.get_latest_committed_versions(
            committed_version_candidates)
        raw_identity_candidates = [
            service_name for service_name in committed_version_candidates
            if (service_name not in latest_committed_versions and
                liveness_snapshots.get(service_name) is None)
        ]
        raw_identities = {}
        if raw_identity_candidates:
            raw_identities = serve_state.get_service_mode_and_hashes(
                raw_identity_candidates)
        for service_name in service_names:
            svc = liveness_snapshots.get(service_name)
            # A row with no version_specs row is invisible to the joined
            # snapshot query.  A row whose latest version is a NULL-yaml
            # placeholder is visible, but is equally unbootable when it has
            # no earlier committed version.  Retire both shapes atomically;
            # mark_unrecoverable_service_for_cleanup rechecks the absence of
            # committed yaml in the same transaction as the terminal fence.
            needs_committed_version_check = (svc is None or
                                             ('yaml_content' in svc and
                                              svc['yaml_content'] is None))
            if needs_committed_version_check:
                committed_version = latest_committed_versions.get(service_name)
                if committed_version is None:
                    expected_service_hash = None
                    if svc is None:
                        raw_identity = raw_identities.get(service_name)
                        if (raw_identity is not None and
                                raw_identity[0] == pool and
                                isinstance(raw_identity[1], str) and
                                raw_identity[1]):
                            expected_service_hash = raw_identity[1]
                    elif isinstance(svc.get('hash'), str) and svc['hash']:
                        expected_service_hash = svc['hash']
                    if expected_service_hash is not None:
                        retired = (
                            serve_state.mark_unrecoverable_service_for_cleanup(
                                service_name, expected_service_hash, pool))
                        if retired:
                            f.write(
                                f'{capnoun} {service_name} has no committed '
                                'version; retired its unusable recovery '
                                'script and marked it for purge.\n')
                    continue
            if svc is None:
                # The row disappeared or changed mode between the name and
                # joined-status snapshots. It is no longer ours to recover in
                # this sweep.
                continue
            controller_pid = svc['controller_pid']
            controller_ip = svc.get('controller_ip')
            status_dbg = svc.get('status')
            f.write(f'ha_recovery candidate {service_name}: '
                    f'pid={controller_pid} ip={controller_ip} '
                    f'status={status_dbg}\n')
            if controller_pid is not None:
                try:
                    alive = _controller_process_alive(
                        controller_pid,
                        service_name,
                        svc.get('hash'),
                        allow_legacy=(svc.get('resource_scope') is None))
                except Exception as e:  # pylint: disable=broad-except
                    # _controller_process_alive may raise if psutil fails
                    # (transient AccessDenied / cmdline read race / etc).
                    # Treating "raised" as "dead" would replace a possibly-
                    # alive controller every iteration that hits the
                    # exception, churning pid/ip/port and disrupting
                    # in-flight requests. Be conservative: skip this round.
                    f.write(f'Error checking controller pid {controller_pid}'
                            f' for {noun} {service_name}: {e}\n')
                    continue
                if alive:
                    f.write(f'Controller pid {controller_pid} for '
                            f'{noun} {service_name} is still running. '
                            'Skipping recovery.\n')
                    continue

            # Defense in depth: even if DB controller_pid is stale (e.g. an
            # older _start hadn't yet pre-claimed it, or pre-claim is
            # disabled / lost), still skip the recovery launch if any
            # `python -m sky.serve.service --service-name <name>` is
            # already running on this pod (snapshot taken once at the
            # top of the iteration). Otherwise the daemon's ~20s
            # iteration repeatedly fires recovery during the 0-60s
            # controller boot window, piling up multiple _start instances.
            service_hash = svc.get('hash')
            resource_scope = svc.get('resource_scope')
            exact_start_running = ((service_name, service_hash)
                                   in in_flight_service_incarnations)
            legacy_start_running = (resource_scope is None and
                                    (service_name, None)
                                    in in_flight_service_incarnations)
            if exact_start_running or legacy_start_running:
                f.write(f'{capnoun} {service_name}: _start process already '
                        f'running on this pod; skipping recovery this '
                        f'round.\n')
                continue

            # Fence right before the launch: the leader-lock session may have
            # died since the caller's top-of-iteration probe (this sweep can
            # take a while with many services). Without leadership, another
            # pod may already be recovering the same services — abort the
            # sweep instead of racing it.
            if still_leader is not None and not still_leader():
                msg = ('Consolidation leader lock session lost mid-recovery; '
                       'aborting the rest of this recovery sweep.')
                f.write(msg + '\n')
                logger.error(msg)
                break
            # Production liveness records carry the protocol marker. Bind the
            # current service owner, recovery-version election, exact config
            # bytes, and recovery script in one just-in-time statement. This
            # prevents a long fleet sweep from launching a stale same-name
            # incarnation or pairing a pre-update script with post-update
            # config. Missing metadata is accepted only for legacy mocked/read
            # records and uses the historical script lookup.
            recovery_snapshot = svc
            recovery_version: int | None = None
            if 'config_protocol_active' in svc:
                if not isinstance(service_hash, str) or not service_hash:
                    f.write(f'{capnoun} {service_name} has no durable '
                            'incarnation identity. Skipping recovery.\n')
                    continue
                try:
                    current_snapshot = (
                        serve_state.get_service_ha_recovery_snapshot(
                            service_name, expected_service_hash=service_hash))
                except Exception as e:  # pylint: disable=broad-except
                    f.write(f'Failed to authorize recovery for '
                            f'{service_name}: {e}. Skipping recovery.\n')
                    continue
                if current_snapshot is None:
                    f.write(f'{capnoun} {service_name} changed incarnation '
                            'during the recovery sweep. Skipping recovery.\n')
                    continue
                owner_fields = ('hash', 'lifecycle_epoch', 'controller_pid',
                                'controller_ip', 'workspace', 'resource_scope',
                                'status')
                changed_fields = [
                    field for field in owner_fields
                    if field in svc and svc[field] != current_snapshot[field]
                ]
                if changed_fields:
                    f.write(f'{capnoun} {service_name} changed recovery owner '
                            f'metadata ({", ".join(changed_fields)}) during '
                            'the recovery sweep. Skipping recovery.\n')
                    continue
                recovery_snapshot = current_snapshot
                if (runtime_profile.guarded_ha_ephemeral_artifacts_enabled() and
                    (not current_snapshot.get('config_protocol_active') or
                     current_snapshot.get('controller_config_snapshot')
                     is None)):
                    f.write(f'{capnoun} {service_name} has no complete '
                            'PostgreSQL controller recovery snapshot; guarded '
                            'HA will not use a predecessor-local or embedded '
                            'configuration fallback. Skipping recovery.\n')
                    continue
                script = current_snapshot['ha_recovery_script']
                recovery_version = current_snapshot.get('recovery_version')
                if (isinstance(recovery_version, bool) or
                        not isinstance(recovery_version, int) or
                        recovery_version < 1):
                    f.write(f'{capnoun} {service_name} has no applicable '
                            'recovery version. Skipping recovery.\n')
                    continue
            else:
                if (runtime_profile.guarded_ha_ephemeral_artifacts_enabled()):
                    f.write(f'{capnoun} {service_name} has no PostgreSQL '
                            'recovery-protocol marker; guarded HA will not '
                            'read a legacy controller-local fallback. '
                            'Skipping recovery.\n')
                    continue
                script = serve_state.get_ha_recovery_script(service_name)
            if script is None:
                f.write(f'{capnoun} {service_name}\'s recovery script does '
                        'not exist. Skipping recovery.\n')
                continue
            # Recreate the service working directory before running the
            # recovery script. It lives on pod-local storage (emptyDir), so a
            # pod REPLACEMENT (rolling update, reschedule) wipes it while the
            # durable service row and recovery script survive in the DB. The
            # stored script redirects its output into this directory; without
            # the mkdir the redirect fails and the script dies instantly —
            # recovery then retries every ~20s forever and the service stays
            # headless (measured live: a 224-replica spot fleet sat
            # CONTROLLER_FAILED for hours while its replicas kept billing).
            # _start itself re-derives everything else from the DB on
            # recovery, so the empty directory is all that is needed.
            try:
                os.makedirs(os.path.expanduser(
                    generate_remote_service_dir_name(
                        service_name, recovery_snapshot.get('resource_scope'))),
                            exist_ok=True)
            except OSError as e:
                f.write(f'Failed to recreate the service dir for '
                        f'{service_name}: {e}\n')
                continue
            # The one-statement liveness snapshot carries both protocol
            # activation and the quarantine-aware elected generation. Avoid
            # recomputing the election in a later transaction, which could
            # pair one generation with another snapshot's controller owner.
            # Missing keys preserve compatibility with legacy mocked/read
            # records and therefore select the legacy HA script path.
            uses_versioned_config = bool(
                recovery_snapshot.get('config_protocol_active', False))
            if uses_versioned_config:
                recovery_version = recovery_snapshot.get('recovery_version')
                assert isinstance(recovery_version, int)
                config_snapshot = recovery_snapshot.get(
                    'controller_config_snapshot')
                if config_snapshot is None:
                    f.write(f'{capnoun} {service_name} recovery version '
                            f'{recovery_version} has no complete controller '
                            'config snapshot. Skipping recovery.\n')
                    continue
                live_config_path: str | None = None
                try:
                    live_config_path = generate_versioned_config_yaml_file_name(
                        service_name, recovery_version,
                        recovery_snapshot.get('resource_scope'))
                    staged_config_path = (generate_staged_config_yaml_file_name(
                        service_name,
                        recovery_version,
                        recovery_snapshot.get('resource_scope'),
                        snapshot_id=config_snapshot[2]))
                    restored_config_bytes = restore_controller_config_snapshot(
                        config_snapshot,
                        live_config_path,
                        staged_config_path,
                        expected_workspace=recovery_snapshot.get('workspace'))
                    # Never pass historical embedded config bytes through
                    # `/bin/sh -c`, argv, or debug logging. The exact selected
                    # version is already restored above.
                    script = strip_legacy_ha_recovery_config_payload(
                        script, live_config_path)
                    script = bind_ha_recovery_config_snapshot_receipt(
                        script,
                        config_path=live_config_path,
                        config_bytes=restored_config_bytes)
                except Exception as e:  # pylint: disable=broad-except
                    if live_config_path is not None:
                        try:
                            os.unlink(os.path.expanduser(live_config_path))
                        except FileNotFoundError:
                            pass
                    f.write('Failed to restore committed controller config for '
                            f'{service_name}: {e}. Skipping recovery.\n')
                    continue
            if 'config_protocol_active' in svc:
                assert isinstance(recovery_version, int)
                try:
                    script = bind_ha_recovery_owner_fence(
                        script,
                        service_hash=recovery_snapshot['hash'],
                        lifecycle_epoch=recovery_snapshot['lifecycle_epoch'],
                        controller_pid=recovery_snapshot['controller_pid'],
                        controller_ip=recovery_snapshot['controller_ip'],
                        status=recovery_snapshot['status'],
                        recovery_version=recovery_version)
                except (KeyError, TypeError, ValueError) as e:
                    f.write(f'Failed to bind recovery ownership for '
                            f'{service_name}: {e}. Skipping recovery.\n')
                    continue
            # Config restoration and local filesystem repair can take time.
            # Recheck the revocable leader session immediately before process
            # creation for both legacy and versioned recovery paths.
            if still_leader is not None and not still_leader():
                msg = ('Consolidation leader lock session lost before '
                       'recovery launch; aborting the rest of this recovery '
                       'sweep.')
                f.write(msg + '\n')
                logger.error(msg)
                break
            rc, out, err = runner.run(script, require_outputs=True)
            if rc:
                f.write(f'Recovery script returned {rc}. '
                        f'Output: {out}\nError: {err}\n')
            f.write(f'{capnoun} {service_name} completed recovery at '
                    f'{datetime.datetime.now()}\n')
        # Reap external LB objects whose owning service no longer exists. Only
        # for services (pools have no LB). No-op outside external-LB +
        # in-cluster mode. Lazy import: lb_k8s imports serve_utils at module
        # level, so a top-level import here would be circular.
        if not pool:
            from sky.serve import (  # pylint: disable=import-outside-toplevel  # noqa: E501
                lb_k8s)
            try:
                lb_k8s.reconcile_lb_objects(set(service_names))
            except Exception as e:  # pylint: disable=broad-except
                # Reconcile is best-effort cleanup; never let it abort recovery.
                f.write(f'Failed to reconcile external LB objects: {e}\n')
        f.write(f'HA recovery completed at {datetime.datetime.now()}\n')
        f.write(f'Total recovery time: {time.time() - start} seconds\n')


def _controller_process_alive(pid: int,
                              service_name: str,
                              service_incarnation: str | None = None,
                              allow_legacy: bool = True) -> bool:
    """Check exact local controller identity, not pod-local PID alone."""
    try:
        process = psutil.Process(pid)
        cmdline = process.cmdline()
        if not process.is_running():
            return False
        try:
            name_idx = cmdline.index('--service-name')
        except ValueError:
            return False
        if name_idx + 1 >= len(cmdline) or cmdline[name_idx +
                                                   1] != service_name:
            return False
        try:
            incarnation_idx = cmdline.index('--service-incarnation')
        except ValueError:
            return allow_legacy
        if incarnation_idx + 1 >= len(cmdline):
            return False
        return (service_incarnation is not None and
                cmdline[incarnation_idx + 1] == service_incarnation)
    except psutil.NoSuchProcess:
        return False


def _snapshot_in_flight_start_service_incarnations(
) -> set[tuple[str, str | None]]:
    """Return active ``(service name, requested incarnation)`` processes.

    Used by ha_recovery_for_consolidation_mode to deduplicate recovery
    launches: while a previously-spawned _start is still in its 0-60s
    boot window waiting for the controller subprocess to bind, DB
    controller_pid may still point at the dead previous instance.
    Re-firing the recovery script in that window causes pile-up
    (multiple _start instances racing on the same service).

    Snapshotting once per daemon iteration (rather than per-service)
    gives O(processes + N services) instead of O(processes * N), and
    also a consistent view (all services see the same set).

    Zombies are excluded — a `_start` process that died but hasn't been
    reaped (pods without a proper init process) would otherwise
    permanently block recovery for that service.

    Matching is on the argv LIST (not a joined string), so
    `--service-name pool-a` does not falsely match `--service-name pool-abc`.
    """
    in_flight: set[tuple[str, str | None]] = set()
    for proc in psutil.process_iter(['cmdline', 'status']):
        try:
            if proc.info.get('status') == psutil.STATUS_ZOMBIE:
                continue
            cmdline = proc.info.get('cmdline') or []
            if 'sky.serve.service' not in ' '.join(cmdline):
                continue
            try:
                idx = cmdline.index('--service-name')
            except ValueError:
                continue
            if idx + 1 < len(cmdline):
                service_name = cmdline[idx + 1]
                incarnation = None
                try:
                    incarnation_idx = cmdline.index('--service-incarnation')
                except ValueError:
                    pass
                else:
                    if incarnation_idx + 1 < len(cmdline):
                        incarnation = cmdline[incarnation_idx + 1]
                in_flight.add((service_name, incarnation))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return in_flight


def _start_in_flight(service_name: str) -> bool:
    """Thin wrapper around the process snapshot for
    one-off checks (e.g. tests, ad-hoc callers).

    The hot path in `ha_recovery_for_consolidation_mode` calls the
    snapshot helper directly and reuses the set across services.
    """
    return any(name == service_name
               for name, _ in _snapshot_in_flight_start_service_incarnations())


def validate_external_lb_service_spec(
        service_spec: 'service_spec_lib.SkyServiceSpec') -> None:
    """Validate service fields implemented by the external-only LB.

    Task-level TLS used to terminate inside the in-pod load balancer. The
    external LB intentionally serves HTTP behind the platform ingress, so
    accepting that field would persist ``tls_encrypted=True`` and advertise an
    HTTPS endpoint that does not exist.
    """
    if service_spec.tls_credential is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Task-level service.tls_credential is not supported by the '
                'external SkyServe load balancer. Terminate TLS at the '
                'platform ingress/load balancer and remove tls_credential '
                'from the service specification.')


def snapshot_service_container_images(
    task: 'sky.Task',
    workspace: str | None = None,
) -> str | None:
    """Pins every candidate in one service version to one artifact.

    Managed selectors may retain different distribution profiles for
    placement, but their content identity must converge. Explicit direct
    selectors must also converge. Legacy image_id candidates remain outside
    the catalog and retain their historical heterogeneous behavior. The
    rewritten task YAML is the durable per-version snapshot.
    """
    try:
        artifact_ids = container_image_task_utils.snapshot_task_container_images(
            [task], workspace)
    except ValueError:
        with ux_utils.print_exception_no_traceback():
            raise
    return next(iter(artifact_ids), None)


def resolve_service_workspace(
    service_name: str,
    service_record: dict[str, Any],
    requested_workspace: str | None = None,
    *,
    trusted_recovery_hint: bool = False,
) -> str:
    """Returns and, when necessary, safely backfills a service workspace.

    Existing service rows predate durable workspace storage. Replica cluster
    rows are authoritative evidence because every launch already persisted its
    active workspace. A controller's service-scoped recovery script is also a
    valid hint when no replica has ever been launched; an ordinary update
    request is not.
    """
    stored_workspace = service_record.get('workspace')
    if isinstance(stored_workspace, str) and stored_workspace:
        if (requested_workspace is not None and
                requested_workspace != stored_workspace):
            raise RuntimeError(
                f'Service {service_name!r} belongs to workspace '
                f'{stored_workspace!r}, not {requested_workspace!r}.')
        return stored_workspace

    expected_service_hash = service_record.get('hash')
    if (not isinstance(expected_service_hash, str) or
            not expected_service_hash):
        raise RuntimeError(
            f'Cannot safely recover legacy service {service_name!r} without '
            'a durable incarnation hash.')

    replica_infos = serve_state.get_replica_infos(service_name)
    cluster_names = list(
        dict.fromkeys(
            info.cluster_name
            for info in replica_infos
            if isinstance(info.cluster_name, str) and info.cluster_name))
    cluster_records = global_user_state.get_clusters_from_names(cluster_names)
    evidenced_workspaces = {
        record['workspace']
        for record in cluster_records.values()
        if record is not None and isinstance(record.get('workspace'), str) and
        record['workspace']
    }
    if len(evidenced_workspaces) > 1:
        raise RuntimeError(
            f'Cannot safely recover legacy service {service_name!r}: its '
            'replica clusters belong to multiple workspaces.')

    inferred_workspace = next(iter(evidenced_workspaces), None)
    if inferred_workspace is not None:
        if (requested_workspace is not None and
                requested_workspace != inferred_workspace):
            raise RuntimeError(
                f'Service {service_name!r} replica evidence belongs to '
                f'workspace {inferred_workspace!r}, not '
                f'{requested_workspace!r}.')
        workspace = inferred_workspace
    elif (trusted_recovery_hint and isinstance(requested_workspace, str) and
          requested_workspace):
        workspace = requested_workspace
    else:
        raise RuntimeError(
            f'Cannot safely recover legacy service {service_name!r} without '
            'a durable workspace or replica-cluster workspace evidence. '
            'Recreate it in the intended workspace.')

    if not serve_state.set_service_workspace_if_owner(service_name, workspace,
                                                      expected_service_hash):
        refreshed = serve_state.get_service_status_snapshot(service_name)
        if (refreshed is None or
                refreshed.get('hash') != expected_service_hash or
                refreshed.get('workspace') != workspace):
            raise RuntimeError(
                f'Lost ownership while backfilling workspace for service '
                f'{service_name!r}.')
    logger.warning(f'Backfilled legacy service {service_name!r} workspace '
                   f'to {workspace!r}.')
    return workspace


def validate_logical_replica_task(
        task: 'sky.Task',
        service_spec: 'service_spec_lib.SkyServiceSpec | None' = None) -> None:
    """Reject topologies without a defined logical-replica contract."""
    if service_spec is None:
        service_spec = task.service
    if (service_spec is not None and
            service_spec.uses_logical_replicas is True and task.num_nodes != 1):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'dynamic_fallback_per_gpu currently supports only single-node '
                'services. Multi-node replica routing does not yet define a '
                'safe logical capacity contract.')


def load_task_with_service_spec(
    yaml_content: str,
    authoritative_service_spec: 'service_spec_lib.SkyServiceSpec | None' = None,
) -> 'sky.Task':
    """Load task resources while preserving one committed service policy."""
    if authoritative_service_spec is None:
        return task_lib.Task.from_yaml_str(yaml_content)
    config = yaml_utils.safe_load(yaml_content)
    if not isinstance(config, dict):
        raise ValueError('Service task YAML must contain a mapping.')
    config.pop('service', None)
    config.pop('pool', None)
    task = task_lib.Task.from_yaml_config(config)
    task.set_service(authoritative_service_spec)
    return task


def resolve_replica_ingress_port(task: 'sky.Task', pool: bool) -> str:
    """Resolve the one ingress port accepted by Serve validation and launch."""
    if task.service is None:
        raise RuntimeError('Service or pool section not found.')
    if pool:
        if (task.service.ports is not None or any(
                resources.ports is not None for resources in task.resources)):
            raise ValueError('Cannot specify ports in a pool.')
        return '-'
    if task.service.ports is not None:
        return task.service.ports

    inferred_ports: set[int] = set()
    for resources in task.resources:
        ports = list(resources_utils.port_ranges_to_set(resources.ports))
        if len(ports) != 1:
            raise ValueError(
                'To open multiple ports on the replica, please set the '
                '`service.ports` field to specify a main service port. '
                'Must only specify one port in resources otherwise. '
                'Each replica will use the port specified as application '
                f'ingress port. Got {ports!r}.')
        inferred_ports.add(ports[0])
    if len(inferred_ports) != 1:
        raise ValueError('Got multiple ports in different resources: '
                         f'{sorted(inferred_ports)!r}. Please specify the '
                         'same port instead.')
    return str(next(iter(inferred_ports)))


def validate_service_task(task: 'sky.Task', pool: bool) -> None:
    """Validate the task for Sky Serve.

    Args:
        task: sky.Task to validate

    Raises:
        ValueError: if the arguments are invalid.
        RuntimeError: if the task.serve is not found.
    """
    spot_resources: list[sky.Resources] = [
        resource for resource in task.resources if resource.use_spot
    ]
    has_spot_placer = (task.service is not None and
                       task.service.placement_contract.enabled)
    # A spot placer may manage a heterogeneous set that mixes spot cloud
    # entries with non-spot reserved-capacity entries (e.g. a zero-cost
    # Kubernetes pool): each launch is pinned to its location's spot-ness.
    # Without a placer, mixing stays unsupported.
    if (len(spot_resources) not in [0, len(task.resources)] and
            not has_spot_placer):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Resources must either all use spot or none use spot. '
                'To use on-demand and spot instances together, '
                'use `dynamic_ondemand_fallback`, set '
                'base_ondemand_fallback_replicas, or configure a '
                '`spot_placer` to manage a mixed set.')

    field_name = 'service' if not pool else 'pool'
    if task.service is None:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'{field_name.capitalize()} section not found.')

    if pool != task.service.pool:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'{field_name.capitalize()} section in the YAML '
                             f'file does not match the pool argument. '
                             f'To fix, add a valid `{field_name}` field.')

    # Empty secrets are supported for ordinary tasks, but a long-lived service
    # must not provision replicas with credentials that are guaranteed to
    # fail.  This runs after YAML and CLI overrides have been merged.
    if not pool:
        empty_secret_names = sorted(
            name for name, value in task.secrets.items()
            if value.get_secret_value() == '')
        if empty_secret_names:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'SkyServe secret values must be non-empty. Provide a '
                    'value in the task YAML or with --secret for: '
                    f'{", ".join(empty_secret_names)}.')

    validate_logical_replica_task(task)

    # Validate that pools do not use ordered resources
    if pool and isinstance(task.resources, list):
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'Ordered resources are not supported for pools. '
                'Use `any_of` instead, or specify a single resource.')

    policy_description = ('on-demand'
                          if task.service.dynamic_ondemand_fallback else 'spot')
    for resource in list(task.resources):
        if resource.job_recovery is not None:
            sys_name = 'SkyServe' if not pool else 'Pool'
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'job_recovery is disabled for {sys_name}. '
                                 f'{sys_name} will replenish preempted spot '
                                 f'with {policy_description} instances.')

    # Every Kubernetes context is one reserved-fill pool edge.  Accelerator
    # names in that context share one brokered capacity group and therefore
    # must use the same physical GPU width.  Different physical contexts may
    # use different widths.  Zero-cost-ness is not fully knowable client-side,
    # so all Kubernetes entries are the safe conservative candidate set.
    if task.service.reserved_capacity_fill:
        pool_widths: dict[str | None, set[int]] = {}
        for requested_resources in task.resources:
            if str(requested_resources.cloud).lower() != 'kubernetes':
                continue
            accelerators = requested_resources.accelerators or {}
            if not accelerators:
                continue
            gpu_name, gpu_count = next(iter(accelerators.items()))
            is_numeric = (not isinstance(gpu_count, bool) and
                          isinstance(gpu_count, (int, float)))
            is_finite = is_numeric and math.isfinite(float(gpu_count))
            is_whole = is_finite and float(gpu_count).is_integer()
            if (task.service.placement_contract.
                    requires_single_gpu_reserved_fill and
                (not is_whole or float(gpu_count) != 1.0)):
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'The per-GPU placement contract with '
                        'reserved_capacity_fill requires one-GPU Kubernetes '
                        'fill shapes so broker slots equal placement slots. '
                        f'Got {gpu_name}:{gpu_count!r}.')
            if not is_whole or float(gpu_count) < 1:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'reserved_capacity_fill requires each Kubernetes GPU '
                        'count to be a positive whole number. '
                        f'Got {gpu_name}:{gpu_count!r}.')
            context = requested_resources.region
            exact_gpu_count = int(gpu_count)
            pool_widths.setdefault(context, set()).add(exact_gpu_count)
        inconsistent_contexts = {
            context: sorted(widths)
            for context, widths in pool_widths.items()
            if len(widths) > 1
        }
        if inconsistent_contexts:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'reserved_capacity_fill requires one GPU count within '
                    'each Kubernetes context; got context widths '
                    f'{inconsistent_contexts}.')
        if (task.service.placement_contract.requires_single_gpu_reserved_fill
                and pool_widths and
                any(widths != {1} for widths in pool_widths.values())):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'The per-GPU placement contract with '
                    'reserved_capacity_fill '
                    'requires one-GPU Kubernetes fill shapes so broker slots '
                    'equal placement slots.')

    # Validate the placer contract without enumerating providers. The final
    # policy-mutated task gets one complete catalog immediately before its
    # immutable service version is committed.
    spot_placer.SpotPlacer.validate_task(task.service, task)

    requested_resources_list = list(task.resources)

    def _is_non_spot_kubernetes_gpu_shape(resource: 'sky.Resources') -> bool:
        accelerators = resource.accelerators or {}
        if (resource.use_spot or str(resource.cloud).lower() != 'kubernetes' or
                len(accelerators) != 1):
            return False
        count = next(iter(accelerators.values()))
        return (not isinstance(count, bool) and isinstance(count,
                                                           (int, float)) and
                math.isfinite(float(count)) and float(count).is_integer() and
                float(count) >= 1)

    kubernetes_only_placement = (not pool and
                                 task.service.placement_contract.enabled and
                                 not spot_resources and
                                 bool(requested_resources_list) and all(
                                     _is_non_spot_kubernetes_gpu_shape(resource)
                                     for resource in requested_resources_list))

    replica_ingress_port = resolve_replica_ingress_port(task, pool)
    for requested_resources in requested_resources_list:
        if (task.service.use_ondemand_fallback and
                not requested_resources.use_spot):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    '`use_ondemand_fallback` is only supported '
                    'for spot resources. Please explicitly specify '
                    '`use_spot: true` in resources for on-demand fallback.')
        if (task.service.placement_contract.enabled and
                not requested_resources.use_spot and not spot_resources and
                not kubernetes_only_placement):
            # Non-spot entries are fine under a placer as the reserved
            # zero-cost tier of a mixed set.  A Kubernetes-only set is also a
            # valid zero-cost placement catalog.  Other all-on-demand sets do
            # not have a capacity-fallback contract and remain invalid.
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    '`spot_placer` requires at least one spot resource. '
                    'Please specify `use_spot: true` on the cloud entries, or '
                    'use only non-spot Kubernetes entries with one positive '
                    'whole-number accelerator shape each.')
    if not pool and task.service.ports is None:
        task.service.set_ports(replica_ingress_port)


def generate_service_name(pool: bool = False):
    noun = 'pool' if pool else 'service'
    return f'sky-{noun}-{uuid.uuid4().hex[:4]}'


def _resource_scope_tag(resource_scope: str, length: int = 20) -> str:
    """Filesystem/cloud-safe digest for an incarnation resource scope."""
    return hashlib.sha256(resource_scope.encode('utf-8')).hexdigest()[:length]


def generate_ephemeral_storage_scope_id(resource_scope: str,
                                        storage_generation: str) -> str:
    """Return one version generation's compact bucket/path namespace."""
    # Keep close to the historical 8-character file-mount run ID so generated
    # bucket names remain within provider limits. The prefix distinguishes a
    # Serve-owned namespace from an arbitrary user suffix.
    identity = json.dumps([resource_scope, storage_generation],
                          separators=(',', ':'))
    return f'sv{_resource_scope_tag(identity, length=10)}'


def ephemeral_storage_identity_matches_scope(storage: 'storage_lib.Storage',
                                             scope_id: str) -> bool:
    """Whether a storage object's bucket/subpath carries ``scope_id``."""
    suffix = f'-{scope_id}'
    name = storage.name
    if isinstance(name, str) and name.endswith(suffix):
        return True
    source = storage.source
    if isinstance(source, str):
        # Covers provider URI shapes (bucket in netloc for S3/GCS/R2, path
        # segment for Azure/COS/OCI) without treating a substring inside a
        # larger identifier as ownership.
        source_without_query = source.split('?', 1)[0].rstrip('/')
        if any(
                segment.endswith(suffix)
                for segment in source_without_query.split('/')):
            return True
    bucket_sub_path = storage._bucket_sub_path  # pylint: disable=protected-access
    if isinstance(bucket_sub_path, str):
        scoped_prefix = f'job-{scope_id}'
        normalized = bucket_sub_path.strip('/')
        if (normalized == scoped_prefix or
                normalized.startswith(f'{scoped_prefix}/') or
                f'/{scoped_prefix}/' in f'/{normalized}/'):
            return True
    return False


def generate_remote_service_dir_name(service_name: str,
                                     resource_scope: str | None = None) -> str:
    legacy_name = service_name.replace('-', '_')
    if resource_scope is None:
        # Compatibility only for rows created before resource_scope existed.
        # New incarnations never use this lossy name.
        return os.path.join(constants.SKYSERVE_METADATA_DIR, legacy_name)
    # The readable prefix is deliberately non-authoritative: validation has
    # historically admitted names whose normalized forms collide (`svc-a`,
    # `svc_a`, `Svc.A`).  Hash the exact original spelling as well as the
    # incarnation so the path identity remains injective across both service
    # names and same-name successors.
    readable_name = re.sub(r'[^A-Za-z0-9]+', '_', service_name).strip('_')
    if not readable_name:
        readable_name = 'service'
    name_tag = _resource_scope_tag(service_name, length=16)
    scope_tag = _resource_scope_tag(resource_scope)
    scoped_name = f'{readable_name}_name_{name_tag}_inc_{scope_tag}'
    return os.path.join(constants.SKYSERVE_METADATA_DIR, scoped_name)


def generate_remote_tmp_task_yaml_file_name(service_name: str,
                                            resource_scope: str | None = None
                                           ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'task.yaml.tmp')


def generate_remote_tmp_submitted_task_yaml_file_name(service_name: str,
                                                      resource_scope: str |
                                                      None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    return os.path.join(dir_name, 'submitted_task.yaml.tmp')


def generate_task_yaml_file_name(service_name: str,
                                 version: int,
                                 expand_user: bool = True,
                                 resource_scope: str | None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    if expand_user:
        dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'task_v{version}.yaml')


def generate_submitted_task_yaml_file_name(
        service_name: str,
        version: int,
        expand_user: bool = True,
        resource_scope: str | None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    if expand_user:
        dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'submitted_task_v{version}.yaml')


def generate_remote_config_yaml_file_name(service_name: str,
                                          resource_scope: str | None = None
                                         ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'config.yaml')


def generate_versioned_config_yaml_file_name(
        service_name: str,
        version: int,
        resource_scope: str | None = None) -> str:
    """Immutable config path inherited by one exact controller version."""
    if type(version) is not int or version <= 0:  # pylint: disable=unidiomatic-typecheck
        raise ValueError('Controller config version must be a positive int.')
    return (
        generate_remote_config_yaml_file_name(service_name, resource_scope) +
        f'.v{version}')


def generate_staged_config_yaml_file_name(
        service_name: str,
        version: int,
        resource_scope: str | None = None,
        snapshot_id: str | None = None) -> str:
    """Path for a complete config snapshot awaiting controller admission."""
    if snapshot_id is not None and re.fullmatch(r'[0-9a-f]{64}',
                                                snapshot_id) is None:
        raise ValueError('Controller config snapshot ID is malformed.')
    nonce_suffix = '' if snapshot_id is None else f'.{snapshot_id}'
    return (
        generate_remote_config_yaml_file_name(service_name, resource_scope) +
        f'.v{version}{nonce_suffix}.staged')


def secure_staged_controller_config(config_path: str,
                                    expected_digest: str) -> bytes:
    """Tighten and verify one raw stage without following a symlink."""
    if re.fullmatch(r'[0-9a-f]{64}', expected_digest) is None:
        raise ValueError('Expected controller config digest is malformed.')
    expanded_path = os.path.expanduser(config_path)
    no_follow_flag = getattr(os, 'O_NOFOLLOW', 0)
    pre_open_stat = None
    if no_follow_flag == 0:
        # O_NOFOLLOW is available on the Linux controller image. Keep the
        # helper fail-closed on other platforms too, while checking inode
        # identity below to detect a replacement between lstat() and open().
        pre_open_stat = os.lstat(expanded_path)
        if not stat.S_ISREG(pre_open_stat.st_mode):
            raise RuntimeError('Staged controller config snapshot is not a '
                               'regular file.')
    open_flags = (os.O_RDONLY | no_follow_flag | getattr(os, 'O_NONBLOCK', 0))
    try:
        config_fd = os.open(expanded_path, open_flags)
    except OSError as e:
        if e.errno == errno.ELOOP:
            raise RuntimeError('Staged controller config snapshot is not a '
                               'regular file.') from None
        raise
    try:
        staged_stat = os.fstat(config_fd)
        if not stat.S_ISREG(staged_stat.st_mode):
            raise RuntimeError('Staged controller config snapshot is not a '
                               'regular file.')
        if (pre_open_stat is not None and
            (pre_open_stat.st_dev, pre_open_stat.st_ino)
                != (staged_stat.st_dev, staged_stat.st_ino)):
            raise RuntimeError('Staged controller config snapshot changed '
                               'while it was being opened.')
        if staged_stat.st_size > 1024 * 1024:
            raise RuntimeError('Staged controller config snapshot exceeds '
                               'the 1MiB limit.')
        os.fchmod(config_fd, 0o600)
        with os.fdopen(config_fd, 'rb') as config_file:
            config_fd = -1
            config_bytes = config_file.read(1024 * 1024 + 1)
    finally:
        if config_fd >= 0:
            os.close(config_fd)
    if len(config_bytes) > 1024 * 1024:
        raise RuntimeError('Staged controller config snapshot exceeds the '
                           '1MiB limit.')
    if hashlib.sha256(config_bytes).hexdigest() != expected_digest:
        raise RuntimeError('Staged controller config snapshot digest does '
                           'not match the API-server submission.')
    return config_bytes


def generate_config_snapshot_receipt_file_name(config_path: str) -> str:
    return f'{config_path}.receipt'


def remove_staged_controller_config(staged_path: str) -> None:
    """Remove one exact uncommitted raw snapshot and its local receipt."""
    for path in (staged_path,
                 generate_config_snapshot_receipt_file_name(staged_path)):
        try:
            os.unlink(os.path.expanduser(path))
        except FileNotFoundError:
            pass


def scrub_obsolete_controller_config_files(
        service_name: str,
        elected_version: int,
        resource_scope: str | None = None) -> list[str]:
    """Remove raw live config generations after safe DB recovery.

    The elected version must already have been replaced with its sanitized,
    digest-verified PostgreSQL snapshot while holding the process-wide config
    lock.  Preserve that safe file and every ``.staged`` candidate, but remove
    the initial unversioned source, all non-elected live generations, and all
    live receipts and interrupted receipt-write temporaries (whose source
    digests are offline credential verifiers).
    """
    if type(elected_version) is not int or elected_version <= 0:  # pylint: disable=unidiomatic-typecheck
        raise ValueError('Elected controller config version must be positive.')
    base_path = os.path.expanduser(
        generate_remote_config_yaml_file_name(service_name, resource_scope))
    directory = os.path.dirname(base_path)
    base_name = os.path.basename(base_path)
    version_pattern = re.compile(
        rf'{re.escape(base_name)}\.v([1-9][0-9]*)(\.receipt)?\Z')
    try:
        entries = list(os.scandir(directory))
    except FileNotFoundError:
        return []
    removed: list[str] = []
    for entry in entries:
        should_remove = entry.name in (base_name, f'{base_name}.receipt')
        is_receipt_temporary = (_CONFIG_RECEIPT_TEMP_FILE_PATTERN.fullmatch(
            entry.name) is not None and entry.is_file(follow_symlinks=False))
        should_remove = should_remove or is_receipt_temporary
        match = version_pattern.fullmatch(entry.name)
        if match is not None:
            version = int(match.group(1))
            is_receipt = match.group(2) is not None
            should_remove = is_receipt or version != elected_version
        if not should_remove:
            continue
        try:
            os.unlink(entry.path)
        except FileNotFoundError:
            continue
        removed.append(entry.name)
    return sorted(removed)


def remove_uncommitted_staged_controller_config(
        service_name: str,
        version: int,
        resource_scope: str | None,
        snapshot_id: str | None = None) -> bool:
    """Delete one exact staged raw snapshot only while its version is NULL.

    A database read failure deliberately propagates so callers preserve the
    file. If a controller already wrote a receipt, its nonce must match the
    cleanup request; a missing receipt is the expected pre-delivery state.
    """
    if serve_state.get_yaml_content(service_name, version) is not None:
        return False
    staged_path = generate_staged_config_yaml_file_name(service_name,
                                                        version,
                                                        resource_scope,
                                                        snapshot_id=snapshot_id)
    if snapshot_id is not None:
        receipt = _read_config_snapshot_receipt(staged_path)
        if receipt is not None and receipt['snapshot_id'] != snapshot_id:
            return False
    remove_staged_controller_config(staged_path)
    return True


def gc_orphaned_staged_controller_configs(
        service_name: str,
        resource_scope: str | None,
        *,
        now: float | None = None) -> list[int]:
    """Delete expired raw stages for DB-confirmed uncommitted versions.

    The protocol's nonce-bearing path makes different API requests disjoint.
    The caller serializes this sweep with controller update handlers; the age
    gate additionally covers a request that synced bytes but has not POSTed to
    the controller yet. Missing rows, committed rows, fresh paths, malformed
    filenames, and every database error are preserved.
    """
    config_path = os.path.expanduser(
        generate_remote_config_yaml_file_name(service_name, resource_scope))
    config_dir = os.path.dirname(config_path)
    config_basename = os.path.basename(config_path)
    stage_pattern = re.compile(
        rf'^{re.escape(config_basename)}\.v([1-9][0-9]{{0,9}})'
        r'(?:\.([0-9a-f]{64}))?\.staged(?:\.receipt)?$')
    try:
        directory_entries = list(os.scandir(config_dir))
    except FileNotFoundError:
        return []

    # Key by both version and nonce. Legacy fixed-name stages are accepted for
    # cleanup after rollout, but every new writer uses a nonce-bearing path.
    candidates: dict[tuple[int, str | None], dict[str, tuple[int, int, int,
                                                             int]]] = {}
    receipt_temporaries: dict[str, tuple[int, int, int, int, int]] = {}
    for entry in directory_entries:
        if (_CONFIG_RECEIPT_TEMP_FILE_PATTERN.fullmatch(entry.name)
                is not None):
            try:
                entry_stat = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(entry_stat.st_mode):
                continue
            receipt_temporaries[entry.path] = (entry_stat.st_dev,
                                               entry_stat.st_ino,
                                               entry_stat.st_mtime_ns,
                                               entry_stat.st_size,
                                               entry_stat.st_mode)
            continue
        match = stage_pattern.fullmatch(entry.name)
        if match is None:
            continue
        try:
            entry_stat = entry.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        try:
            version = int(match.group(1))
        except ValueError:
            continue
        fingerprint = (entry_stat.st_dev, entry_stat.st_ino,
                       entry_stat.st_mtime_ns, entry_stat.st_size)
        candidates.setdefault((version, match.group(2)),
                              {})[entry.path] = (fingerprint)

    wall_time = time.time() if now is None else now

    # A hard-killed receipt writer cannot run its exception cleanup. These
    # temporaries are never durable protocol inputs, so unlike raw stages they
    # need no database lookup. The same generous age gate protects a paused
    # writer, and an identity recheck lets a concurrent refresh win.
    for temporary_path, receipt_observed in receipt_temporaries.items():
        age_seconds = wall_time - receipt_observed[2] / 1_000_000_000
        if age_seconds < constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS:
            continue
        try:
            current_stat = os.stat(temporary_path, follow_symlinks=False)
        except FileNotFoundError:
            continue
        receipt_current = (current_stat.st_dev, current_stat.st_ino,
                           current_stat.st_mtime_ns, current_stat.st_size,
                           current_stat.st_mode)
        if (receipt_current != receipt_observed or
                not stat.S_ISREG(current_stat.st_mode)):
            continue
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass

    expired: dict[tuple[int, str | None], dict[str, tuple[int, int, int,
                                                          int]]] = {}
    for identity, paths in candidates.items():
        newest_mtime_ns = max(fingerprint[2] for fingerprint in paths.values())
        age_seconds = wall_time - newest_mtime_ns / 1_000_000_000
        if age_seconds >= constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS:
            expired[identity] = paths
    if not expired:
        return []

    expired_versions = sorted({version for version, _ in expired})
    yaml_contents = serve_state.get_yaml_contents(service_name,
                                                  expired_versions)
    removed_versions: set[int] = set()
    for (version, snapshot_id), observed_paths in sorted(expired.items()):
        if version not in yaml_contents or yaml_contents[version] is not None:
            continue
        staged_path = os.path.expanduser(
            generate_staged_config_yaml_file_name(service_name,
                                                  version,
                                                  resource_scope,
                                                  snapshot_id=snapshot_id))
        candidate_paths = (
            staged_path,
            generate_config_snapshot_receipt_file_name(staged_path))
        # Re-prove the exact path identities immediately before unlinking. A
        # sync that refreshed either file after the directory scan wins and is
        # left for a later sweep.
        unchanged = True
        for candidate_path in candidate_paths:
            observed = observed_paths.get(candidate_path)
            try:
                current_stat = os.stat(candidate_path, follow_symlinks=False)
            except FileNotFoundError:
                if observed is not None:
                    unchanged = False
                continue
            current = (current_stat.st_dev, current_stat.st_ino,
                       current_stat.st_mtime_ns, current_stat.st_size)
            if observed != current:
                unchanged = False
        if not unchanged:
            continue
        for candidate_path in candidate_paths:
            try:
                os.unlink(candidate_path)
            except FileNotFoundError:
                pass
        removed_versions.add(version)
    return sorted(removed_versions)


def write_config_snapshot_receipt(config_path: str, version: int,
                                  snapshot_id: str, source_digest: str) -> None:
    """Atomically record a pod-local receipt next to raw config bytes."""
    if (re.fullmatch(r'[0-9a-f]{64}', snapshot_id) is None or
            re.fullmatch(r'[0-9a-f]{64}', source_digest) is None):
        raise ValueError('Config snapshot receipt is malformed.')
    receipt_path = os.path.expanduser(
        generate_config_snapshot_receipt_file_name(config_path))
    receipt_dir = os.path.dirname(receipt_path)
    os.makedirs(receipt_dir, exist_ok=True)
    # Own the basename grammar instead of depending on tempfile's private
    # random-name length. O_EXCL makes a pre-created path lose safely; retries
    # handle the vanishingly unlikely UUID collision without unlinking it.
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_CLOEXEC', 0)
    fd = -1
    temporary_path = ''
    for _ in range(3):
        temporary_path = os.path.join(
            receipt_dir, f'.config-receipt-{uuid.uuid4().hex}.tmp')
        try:
            fd = os.open(temporary_path, flags, 0o600)
            break
        except FileExistsError:
            continue
    else:
        raise RuntimeError('Failed to allocate a config receipt temporary.')
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as receipt_file:
            json.dump(
                {
                    'version': version,
                    'snapshot_id': snapshot_id,
                    'source_digest': source_digest,
                },
                receipt_file,
                separators=(',', ':'))
            receipt_file.flush()
            os.fsync(receipt_file.fileno())
        os.replace(temporary_path, receipt_path)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise


def _read_config_snapshot_receipt(config_path: str) -> dict[str, Any] | None:
    receipt_path = os.path.expanduser(
        generate_config_snapshot_receipt_file_name(config_path))
    try:
        with open(receipt_path, encoding='utf-8') as receipt_file:
            receipt = json.load(receipt_file)
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return None
    if (not isinstance(receipt, dict) or
            not isinstance(receipt.get('version'), int) or
            isinstance(receipt.get('version'), bool) or
            not isinstance(receipt.get('snapshot_id'), str) or
            re.fullmatch(r'[0-9a-f]{64}', receipt['snapshot_id']) is None or
            not isinstance(receipt.get('source_digest'), str) or
            re.fullmatch(r'[0-9a-f]{64}', receipt['source_digest']) is None):
        return None
    return receipt


def get_config_snapshot_receipt(config_path: str) -> dict[str, Any] | None:
    """Return one validated pod-local config receipt, if present."""
    return _read_config_snapshot_receipt(config_path)


def _read_verified_config_with_receipt(config_path: str, version: int,
                                       snapshot_id: str) -> bytes | None:
    receipt = _read_config_snapshot_receipt(config_path)
    if (receipt is None or receipt['version'] != version or
            receipt['snapshot_id'] != snapshot_id):
        return None
    try:
        with open(os.path.expanduser(config_path), 'rb') as config_file:
            config_bytes = config_file.read()
    except OSError:
        return None
    if hashlib.sha256(config_bytes).hexdigest() != receipt['source_digest']:
        return None
    return config_bytes


def read_verified_controller_config(config_path: str, version: int,
                                    snapshot_id: str,
                                    source_digest: str) -> bytes | None:
    """Read raw bytes only when receipt identity and exact digest agree."""
    receipt = _read_config_snapshot_receipt(config_path)
    if receipt is None or receipt['source_digest'] != source_digest:
        return None
    return _read_verified_config_with_receipt(config_path, version, snapshot_id)


def promote_staged_controller_config(live_path: str, staged_path: str,
                                     version: int, snapshot_id: str,
                                     source_digest: str) -> bytes:
    """Verify and atomically promote one raw staged config and receipt."""
    config_bytes = _read_verified_config_with_receipt(staged_path, version,
                                                      snapshot_id)
    if (config_bytes is None or
            hashlib.sha256(config_bytes).hexdigest() != source_digest):
        raise RuntimeError('Staged controller config snapshot or receipt is '
                           'missing or changed before installation.')
    expanded_live = os.path.expanduser(live_path)
    expanded_staged = os.path.expanduser(staged_path)
    expanded_staged_receipt = os.path.expanduser(
        generate_config_snapshot_receipt_file_name(staged_path))
    expanded_live_receipt = os.path.expanduser(
        generate_config_snapshot_receipt_file_name(live_path))
    os.makedirs(os.path.dirname(expanded_live), exist_ok=True)
    # Tighten permissions before either rename. A crash between publication
    # steps must never leave the raw policy-admitted config world-readable.
    os.chmod(expanded_staged, 0o600)
    os.chmod(expanded_staged_receipt, 0o600)
    os.replace(expanded_staged, expanded_live)
    os.chmod(expanded_live, 0o600)
    os.replace(expanded_staged_receipt, expanded_live_receipt)
    os.chmod(expanded_live_receipt, 0o600)
    with open(expanded_live, 'rb') as live_file:
        installed_bytes = live_file.read()
    if hashlib.sha256(installed_bytes).hexdigest() != source_digest:
        raise RuntimeError('Installed controller config changed during '
                           'atomic promotion.')
    return installed_bytes


def _atomic_write_controller_config(live_path: str,
                                    config_bytes: bytes) -> None:
    """Write exact DB-verified config bytes atomically with mode 0600."""
    expanded_live = os.path.expanduser(live_path)
    os.makedirs(os.path.dirname(expanded_live), exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix='.config-recovery-',
                                          dir=os.path.dirname(expanded_live))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb') as config_file:
            config_file.write(config_bytes)
            config_file.flush()
            os.fsync(config_file.fileno())
        os.replace(temporary_path, expanded_live)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise
    os.chmod(expanded_live, 0o600)


def restore_version_controller_config(
    service_name: str,
    version: int,
    live_path: str,
    staged_path: str | None = None,
    expected_workspace: str | None = None,
) -> bytes | None:
    """Restore the PostgreSQL-bound safe config for an exact version.

    None is the compatibility signal for a pre-protocol version whose legacy
    HA script still owns config restoration. Corruption raises and recovery
    fails closed before a controller is spawned.
    """
    snapshot = serve_state.get_version_controller_config(service_name, version)
    if snapshot is None:
        return None
    return restore_controller_config_snapshot(snapshot, live_path, staged_path,
                                              expected_workspace)


def restore_controller_config_snapshot(
    snapshot: tuple[bytes, str, str],
    live_path: str,
    staged_path: str | None = None,
    expected_workspace: str | None = None,
) -> bytes:
    """Restore one already-authorized durable controller-config snapshot.

    Callers that authorize a recovery generation and select its exact config
    in one database statement use this helper so installation cannot re-read
    and accidentally pair that decision with a later database generation.
    """
    config_bytes, durable_digest, _ = snapshot
    if hashlib.sha256(config_bytes).hexdigest() != durable_digest:
        raise RuntimeError('Committed controller config snapshot failed its '
                           'integrity check.')
    if expected_workspace is not None:
        parse_and_validate_version_controller_config(
            config_bytes, expected_workspace,
            'committed Serve controller recovery config')
    _atomic_write_controller_config(live_path, config_bytes)
    obsolete_paths = [
        generate_config_snapshot_receipt_file_name(live_path),
    ]
    if staged_path is not None:
        obsolete_paths.extend((
            staged_path,
            generate_config_snapshot_receipt_file_name(staged_path),
        ))
    for obsolete_path in obsolete_paths:
        try:
            os.unlink(os.path.expanduser(obsolete_path))
        except FileNotFoundError:
            pass
    return config_bytes


def parse_and_validate_version_controller_config(config_bytes: bytes,
                                                 expected_workspace: str,
                                                 source: str) -> Any:
    """Parse a version snapshot in isolation and enforce workspace identity."""
    if not isinstance(expected_workspace, str) or not expected_workspace:
        raise RuntimeError('Durable service workspace is unavailable.')

    def _parse() -> Any:
        sky_context.initialize()
        parsed = skypilot_config.parse_and_validate_config_bytes(
            config_bytes, source, log_config=False, apply_db_env=False)
        actual_workspace = parsed.get_nested(keys=('active_workspace',),
                                             default_value=None)
        if actual_workspace != expected_workspace:
            raise RuntimeError(
                f'Committed controller config belongs to workspace '
                f'{actual_workspace!r}, expected {expected_workspace!r}.')
        if expected_workspace != skylet_constants.SKYPILOT_DEFAULT_WORKSPACE:
            workspaces = parsed.get_nested(keys=('workspaces',),
                                           default_value=None)
            workspace_config = (workspaces.get(expected_workspace)
                                if isinstance(workspaces, dict) else None)
            if not isinstance(workspace_config, dict):
                raise RuntimeError(
                    f'Committed controller config does not define durable '
                    f'workspace {expected_workspace!r}.')
        return parsed

    return contextvars.Context().run(_parse)


def generate_remote_controller_log_file_name(service_name: str,
                                             resource_scope: str | None = None
                                            ) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'controller.log')


def generate_remote_batch_controller_log_file_name(service_name: str,
                                                   resource_scope: str |
                                                   None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    # Don't expand here since it is used for remote machine.
    return os.path.join(dir_name, 'batch_controller.log')


def generate_replica_launch_log_file_name(
        service_name: str,
        replica_id: int,
        resource_scope: str | None = None) -> str:
    dir_name = generate_remote_service_dir_name(service_name, resource_scope)
    dir_name = os.path.expanduser(dir_name)
    return os.path.join(dir_name, f'replica_{replica_id}_launch.log')


def generate_replica_cluster_name(service_name: str,
                                  replica_id: int,
                                  resource_scope: str | None = None) -> str:
    # NOTE(dev): This format is used in sky/serve/service.py::_cleanup, for
    # checking replica cluster existence. Be careful when changing it.
    if resource_scope is None:
        return f'{service_name}-{replica_id}'
    identity = json.dumps([service_name, resource_scope], separators=(',', ':'))
    scope_tag = _resource_scope_tag(identity, length=10)
    suffix = f'-{replica_id}-{scope_tag}'
    # Keep Kubernetes/cloud-derived names within the common 63-character
    # ceiling even when the user service name itself occupies that budget.
    prefix = service_name[:63 - len(suffix)].rstrip('-')
    if not prefix:
        prefix = 'skyserve'
    return f'{prefix}{suffix}'


_COMPLETED_REPLICA_FAILURE_STATUSES = frozenset({
    serve_state.ReplicaStatus.FAILED_PROVISION,
})


def _service_status_from_replica_infos(
    replica_infos: list['replica_managers.ReplicaInfo'],
    target_num_replicas: int | None,
) -> serve_state.ServiceStatus:
    replica_statuses = [info.status for info in replica_infos]
    status = serve_state.ServiceStatus.from_replica_statuses(replica_statuses)
    if (status == serve_state.ServiceStatus.FAILED and
            target_num_replicas == 0 and
            all(replica_status in _COMPLETED_REPLICA_FAILURE_STATUSES
                for replica_status in replica_statuses)):
        # Completed provisioning failures are retained for operator-visible
        # history. Once the autoscaler authoritatively wants no replicas, those
        # rows no longer describe the current fleet. App/readiness failures and
        # cleanup-uncertain FAILED_CLEANUP/UNKNOWN rows remain visible.
        return serve_state.ServiceStatus.NO_REPLICA
    return status


def set_service_status_and_active_versions_from_replica(
    service_name: str,
    replica_infos: list['replica_managers.ReplicaInfo'],
    update_mode: UpdateMode,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    target_num_replicas: int | None = None,
) -> None:
    record = serve_state.get_service_controller_owner(service_name,
                                                      require_version=True)
    if record is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'The service is up-ed in an old version and does not '
                'support update. Please `sky serve down` '
                'it first and relaunch the service.')
    record_hash = record.get('hash')
    if (expected_service_hash is not None and
            record_hash != expected_service_hash):
        logger.debug(f'Refusing replica-driven status write from stale '
                     f'incarnation {expected_service_hash!r} for '
                     f'{service_name!r}; current incarnation is '
                     f'{record_hash!r}.')
        return
    record_owner = (record.get('controller_pid'), record.get('controller_ip'))
    if (expected_controller_owner is not None and
            record_owner != expected_controller_owner):
        logger.debug(f'Refusing replica-driven status write from stale '
                     f'controller {expected_controller_owner!r} for '
                     f'{service_name!r}; current controller is '
                     f'{record_owner!r}.')
        return
    observed_status = record['status']
    if observed_status in serve_state.ServiceStatus.terminal_statuses():
        # A controller child can briefly keep probing after its parent has
        # durably entered teardown or marked it failed. Terminal state belongs
        # to the parent/finalizer and must never be healed by replica probes.
        return

    ready_replicas = list(filter(lambda info: info.is_ready, replica_infos))
    if update_mode == UpdateMode.ROLLING:
        active_versions = sorted(
            list(set(info.version for info in ready_replicas)))
    else:
        chosen_version = get_latest_version_with_min_replicas(
            service_name, replica_infos)
        active_versions = [chosen_version] if chosen_version is not None else []
    # Compute the service status from ALL replicas, not just the ready ones.
    # The authoritative autoscaler target lets the helper distinguish an idle
    # scale-to-zero service from an actively desired fleet whose replicas all
    # failed. `active_versions` above intentionally stays on ready replicas
    # (the versions actually serving traffic).
    service_hash = (expected_service_hash
                    if expected_service_hash is not None else record_hash)
    if not isinstance(service_hash, str) or not service_hash:
        logger.warning(f'Refusing replica-driven status write for '
                       f'{service_name!r} without a durable incarnation.')
        return
    updated = serve_state.set_service_status_and_active_versions_if_owner(
        service_name,
        service_hash, (expected_controller_owner[0] if expected_controller_owner
                       is not None else record.get('controller_pid')),
        (expected_controller_owner[1] if expected_controller_owner is not None
         else record.get('controller_ip')),
        _service_status_from_replica_infos(replica_infos, target_num_replicas),
        active_versions=active_versions,
        expected_status=observed_status)
    if not updated:
        logger.debug(f'Skipped stale replica-driven status write for '
                     f'{service_name!r}; owner or status changed.')


def update_service_status(pool: bool) -> None:
    noun = 'pool' if pool else 'serve'
    capnoun = noun.capitalize()
    records = serve_state.get_service_liveness_snapshots(pool=pool)
    terminal_statuses = set(serve_state.ServiceStatus.terminal_statuses())
    for record in records:
        service_name = record['name']
        service_status = record['status']
        if service_status in terminal_statuses:
            # Finalization and recovery own terminal states.  In particular,
            # never erase FAILED_CLEANUP by rewriting it CONTROLLER_FAILED.
            continue

        logger.info(f'Update {noun} status for {service_name!r} '
                    f'with status {service_status}')

        controller_pid = record['controller_pid']
        if controller_pid is None:
            logger.info(f'{capnoun} {service_name!r} controller pid is None. '
                        f'Unexpected status {service_status}. Set to failure.')
        elif controller_pid < 0:
            # Backwards compatibility: this service was submitted when ray was
            # still used for controller process management. We set the
            # value_to_replace_existing_entries to -1 to indicate historical
            # services.
            # TODO(tian): Remove before 0.13.0.
            controller_job_id = record['controller_job_id']
            assert controller_job_id is not None
            controller_status = job_lib.get_status(controller_job_id)
            if (controller_status is not None and
                    not controller_status.is_terminal()):
                continue
            logger.info(f'Updating {noun} {service_name!r} in old version. '
                        f'SkyPilot job status: {controller_status}. '
                        'Set to failure.')
        else:
            if _controller_process_alive(
                    controller_pid,
                    service_name,
                    record.get('hash'),
                    allow_legacy=record.get('resource_scope') is None):
                # The controller is still running.
                continue
            logger.info(f'{capnoun} {service_name!r} controller pid '
                        f'{controller_pid} is not alive. Set to failure.')

        # If controller job is not running, set it as controller failed.
        serve_state.set_service_status_and_active_versions_if_owner(
            service_name,
            record['hash'],
            record['controller_pid'],
            record['controller_ip'],
            serve_state.ServiceStatus.CONTROLLER_FAILED,
            expected_status=service_status)


def require_update_config_snapshot_capability(service_name: str,
                                              service_hash: str) -> None:
    """Fail before version allocation if a live controller is too old."""
    response = _get_to_controller_with_retry(
        service_name, service_hash,
        constants.CONTROLLER_UPDATE_CAPABILITIES_ENDPOINT_PATH)
    if response.status_code != 200:
        raise RuntimeError(
            f'Service {service_name!r} controller does not support atomic '
            'config refresh. Finish the API-server rollout so its controller '
            'is recovered on the new image, then retry the update.')
    try:
        version = response.json()['config_snapshot_protocol_version']
    except (KeyError, TypeError, ValueError) as e:
        raise RuntimeError(
            f'Service {service_name!r} controller returned an invalid config '
            'refresh capability response.') from e
    if (type(version) is not int or  # pylint: disable=unidiomatic-typecheck
            version != constants.SERVE_UPDATE_CONFIG_SNAPSHOT_PROTOCOL_VERSION):
        raise RuntimeError(
            f'Service {service_name!r} controller config refresh protocol '
            f'{version!r} is incompatible with this API server.')


def cleanup_staged_config_update_encoded(service_name: str, service_hash: str,
                                         version: int,
                                         expected_lifecycle_epoch: int,
                                         config_snapshot_id: str) -> bool:
    """Serialize cleanup behind any ambiguous controller update attempt."""
    response = _post_to_controller_with_retry(
        service_name,
        service_hash,
        constants.CONTROLLER_CONFIG_CLEANUP_ENDPOINT_PATH,
        json={
            'version': version,
            'expected_lifecycle_epoch': expected_lifecycle_epoch,
            'config_snapshot_id': config_snapshot_id,
        },
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.UPDATE_SERVICE_TIMEOUT_SECONDS))
    if response.status_code != 200:
        raise RuntimeError(
            f'Controller could not safely clean staged config for service '
            f'{service_name!r}: {response.text}')
    return bool(response.json().get('removed', False))


def set_ordinary_launch_binding_mode_encoded(
    service_name: str,
    mode: str,
    expected_service_hash: str,
    expected_binding_epoch: int,
) -> dict[str, Any]:
    """Ask the exact live controller to run a fenced binding transition."""
    if mode not in ('legacy', 'bound'):
        raise ValueError(
            'Ordinary launch binding mode must be legacy or bound.')
    if (isinstance(expected_binding_epoch, bool) or
            not isinstance(expected_binding_epoch, int) or
            expected_binding_epoch < 0):
        raise ValueError(
            'Expected ordinary launch binding epoch must be nonnegative.')
    service_status = _get_service_status(service_name,
                                         pool=False,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        raise ValueError(f'Service {service_name!r} does not exist.')
    service_hash = service_status['hash']
    if service_hash != expected_service_hash:
        raise RuntimeError(f'Service {service_name!r} was replaced before '
                           'the binding transition was submitted.')
    response = _post_to_controller_with_retry(
        service_name,
        service_hash,
        constants.CONTROLLER_ORDINARY_LAUNCH_BINDING_ENDPOINT_PATH,
        json={
            'mode': mode,
            'expected_service_hash': expected_service_hash,
            'expected_binding_epoch': expected_binding_epoch,
        },
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.UPDATE_SERVICE_TIMEOUT_SECONDS))
    if response.status_code == 404:
        raise RuntimeError(
            f'Service {service_name!r} controller does not support durable '
            'ordinary-launch binding transitions.')
    if response.status_code == 409:
        raise RuntimeError(
            f'Binding transition for service {service_name!r} was rejected: '
            f'{response.text}')
    if response.status_code != 200:
        raise RuntimeError(
            f'Binding transition for service {service_name!r} failed: '
            f'{response.text}')
    result = response.json()
    epoch = result.get('binding_epoch')
    if (result.get('binding_mode') != mode or isinstance(epoch, bool) or
            not isinstance(epoch, int) or epoch < 1):
        raise RuntimeError(
            'Controller returned an invalid ordinary-launch binding epoch.')
    return result


def update_service_encoded(service_name: str,
                           version: int,
                           mode: str,
                           pool: bool,
                           expected_service_hash: str | None = None,
                           expected_lifecycle_epoch: int | None = None,
                           has_submitted_yaml: bool = False,
                           has_config_snapshot: bool = False,
                           expected_config_snapshot_digest: str | None = None,
                           config_snapshot_id: str | None = None) -> str:
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    # Only existence and the incarnation hash are consumed here; skip the
    # YAML render and the fleet-sized replica serialization.
    service_status = _get_service_status(service_name,
                                         pool=pool,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'{capnoun} {service_name!r} does not exist.')
    service_hash = service_status['hash']
    if (expected_service_hash is not None and
            service_hash != expected_service_hash):
        raise RuntimeError(f'{capnoun} {service_name!r} was replaced before '
                           'the update was submitted.')
    request_body = {
        'version': version,
        'mode': mode,
    }
    if expected_service_hash is not None:
        request_body['service_hash'] = expected_service_hash
    if expected_lifecycle_epoch is not None:
        request_body['lifecycle_epoch'] = expected_lifecycle_epoch
    if has_submitted_yaml:
        request_body['has_submitted_yaml'] = True
    if has_config_snapshot:
        if (expected_config_snapshot_digest is None or re.fullmatch(
                r'[0-9a-f]{64}', expected_config_snapshot_digest) is None):
            raise ValueError('A valid expected config snapshot digest is '
                             'required for an atomic config refresh.')
        if (config_snapshot_id is None or
                re.fullmatch(r'[0-9a-f]{64}', config_snapshot_id) is None):
            raise ValueError('A valid config snapshot ID is required for an '
                             'atomic config refresh.')
        request_body['has_config_snapshot'] = True
        request_body['config_snapshot_digest'] = (
            expected_config_snapshot_digest)
        request_body['config_snapshot_id'] = config_snapshot_id
    resp = _post_to_controller_with_retry(
        service_name,
        service_hash,
        (constants.CONTROLLER_CONFIG_UPDATE_ENDPOINT_PATH
         if has_config_snapshot else '/controller/update_service'),
        json=request_body,
        # Keep the compatibility timeout for controllers predating the
        # commit-then-reconcile protocol, whose handler may still wait behind
        # a slow replica-manager probe round.
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.UPDATE_SERVICE_TIMEOUT_SECONDS))
    if resp.status_code == 404:
        with ux_utils.print_exception_no_traceback():
            if has_config_snapshot:
                raise RuntimeError(
                    f'{capnoun} {service_name!r} controller changed during '
                    'the update and no longer supports atomic config '
                    'refresh. Finish the API-server rollout and retry.')
            # This only happens for services since pool is added after the
            # update feature is introduced.
            raise ValueError(
                'The service is up-ed in an old version and does not '
                'support update. Please `sky serve down` '
                'it first and relaunch the service. ')
    elif resp.status_code == 400:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Client error during {noun} update: {resp.text}')
    elif resp.status_code == 409:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'Stale {noun} update rejected: {resp.text}')
    elif resp.status_code == 500:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(
                f'Server error during {noun} update: {resp.text}')
    elif resp.status_code != 200:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Failed to update {noun}: {resp.text}')

    response_body = resp.json()
    if has_config_snapshot:
        activated_snapshot_id = response_body.get('config_snapshot_id')
        if activated_snapshot_id != config_snapshot_id:
            raise RuntimeError(
                f'{capnoun} {service_name!r} controller acknowledged a '
                'different config snapshot. Inspect controller health before '
                'retrying.')
    service_msg = response_body['message']
    return message_utils.encode_payload(service_msg)


def set_load_balancer_high_availability_encoded(
        service_name: str, enabled: bool, expected_service_hash: str,
        expected_lifecycle_epoch: int) -> None:
    """Submit a lifecycle-fenced, LB-only topology transition."""
    service_status = _get_service_status(service_name,
                                         pool=False,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Service {service_name!r} does not exist.')
    service_hash = service_status['hash']
    if service_hash != expected_service_hash:
        raise RuntimeError(f'Service {service_name!r} was replaced before '
                           'the load balancer update was submitted.')
    resp = _post_to_controller_with_retry(
        service_name,
        service_hash,
        '/controller/set_load_balancer_high_availability',
        json={
            'enabled': enabled,
            'service_hash': expected_service_hash,
            'lifecycle_epoch': expected_lifecycle_epoch,
        },
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.UPDATE_SERVICE_TIMEOUT_SECONDS))
    if resp.status_code == 404:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                'The service controller does not support load balancer '
                'topology updates. Upgrade the API server and retry.')
    if resp.status_code == 400:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'Invalid load balancer topology update: {resp.text}')
    if resp.status_code == 409:
        raise RuntimeError(
            f'Stale load balancer topology update rejected: {resp.text}')
    if resp.status_code == 500:
        raise RuntimeError(f'Load balancer topology update failed: {resp.text}')
    if resp.status_code != 200:
        raise RuntimeError(
            f'Failed to update the load balancer topology: {resp.text}')


def terminate_replica(service_name: str, replica_id: int, purge: bool) -> str:
    # TODO(tian): Currently pool does not support terminating replica.
    # Only existence and the incarnation hash are consumed here; avoid the
    # full service-row read and the fleet-sized replica serialization.
    service_status = _get_service_status(service_name,
                                         pool=False,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Service {service_name!r} does not exist.')
    replica_info = serve_state.get_replica_info_from_id(service_name,
                                                        replica_id)
    if replica_info is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'Replica {replica_id} for service {service_name} does not '
                'exist.')

    resp = _post_to_controller_with_retry(
        service_name,
        service_status['hash'],
        '/controller/terminate_replica',
        json={
            'replica_id': replica_id,
            'purge': purge,
        },
        timeout=(_CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.TERMINATE_REPLICA_TIMEOUT_SECONDS))

    try:
        body = resp.json()
    except ValueError:
        body = {}
    # HTTPException responses (e.g. 400/404 validation errors) use FastAPI's
    # default {'detail': ...} shape; the controller's generic error handler
    # and success responses use {'message': ...}.
    message: str = str(body.get('message') or body.get('detail') or resp.text)
    if resp.status_code != 200:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(f'Failed to terminate replica {replica_id} '
                             f'in {service_name}. Reason:\n{message}')
    return message


def get_yaml_content(service_name: str,
                     version: int,
                     resource_scope: str | None = None) -> str:
    yaml_content = serve_state.get_yaml_content(service_name, version)
    if yaml_content is not None:
        return yaml_content
    if runtime_profile.guarded_ha_ephemeral_artifacts_enabled():
        raise RuntimeError(
            f'Guarded HA service {service_name!r} version {version} has no '
            'committed PostgreSQL task YAML; refusing predecessor-local '
            'fallback.')
    # Backward compatibility for old service records that
    # does not dump the yaml content to version database.
    # TODO(tian): Remove this after 2 minor releases, i.e. 0.13.0.
    if resource_scope is None:
        record = serve_state.get_service_status_snapshot(service_name)
        resource_scope = record.get('resource_scope') if record else None
    latest_yaml_path = generate_task_yaml_file_name(
        service_name, version, resource_scope=resource_scope)
    with open(latest_yaml_path, encoding='utf-8') as f:
        return f.read()


def _set_replica_status_aggregates(record: dict[str, Any],
                                   status_counts: dict[str, int],
                                   capacity_counts: dict[str, int]) -> None:
    """Attach public replica capacity and physical backend aggregates."""
    failed_statuses = {
        status.value for status in serve_state.ReplicaStatus.failed_statuses()
    }
    physical_failed = sum(count for status, count in status_counts.items()
                          if status in failed_statuses)
    capacity_failed = sum(count for status, count in capacity_counts.items()
                          if status in failed_statuses)
    logical = bool(record.get('logical_replica_semantics'))
    record.update({
        'replica_unit': ('logical_slot' if logical else 'physical_backend'),
        'ready_replicas': capacity_counts.get(
            serve_state.ReplicaStatus.READY.value, 0),
        'total_replicas': sum(capacity_counts.values()) - capacity_failed,
        'failed_replicas': capacity_failed,
        'physical_ready_replicas': status_counts.get(
            serve_state.ReplicaStatus.READY.value, 0),
        'physical_total_replicas': sum(status_counts.values()) -
                                   physical_failed,
        'physical_failed_replicas': physical_failed,
    })


_PROVIDER_STATUS_FIELDS = (
    'cloud',
    'region',
    'hourly_cost',
    'hourly_cost_exclusion_reason',
    'resources_str',
    'resources_str_full',
    'infra',
)


@dataclasses.dataclass
class _PreparedServiceStatus:
    """Provider-free inputs and mutable outputs for one status snapshot."""

    record: dict[str, Any]
    pool: bool
    include_replica_info: bool
    replica_infos: list[Any] = dataclasses.field(default_factory=list)
    cluster_records: dict[str, Any] = dataclasses.field(default_factory=dict)
    ordinary_infos: list[Any] = dataclasses.field(default_factory=list)
    fenced_groups: dict[tuple[str, str],
                        list[Any]] = dataclasses.field(default_factory=dict)
    validated_handles: dict[int, Any] = dataclasses.field(default_factory=dict)
    identity_uncertain_infos: list[Any] = dataclasses.field(
        default_factory=list)
    serialized_by_id: dict[int,
                           dict[str,
                                Any]] = dataclasses.field(default_factory=dict)
    rate_cache: dict[str, float] = dataclasses.field(default_factory=dict)
    rate_cache_lock: threading.Lock = dataclasses.field(
        default_factory=threading.Lock)
    job_status_counts: dict[str, int] | None = None
    jobs_by_cluster: dict[str | None, list[int]] | None = None


def _prepare_service_status(
        service_name: str,
        pool: bool,
        with_replica_info: bool = True,
        with_replica_counts: bool = False,
        with_yaml: bool = True,
        with_target_num_replicas: bool = False,
        status_snapshot_only: bool = False) -> _PreparedServiceStatus | None:
    """Build one complete status snapshot without contacting providers.

    Args:
        service_name: The name of the service.
        with_replica_info: Whether to include the information of all replicas.
        with_replica_counts: Whether to include a per-status replica count
            histogram (``replica_status_counts``). Cheaper than
            ``with_replica_info`` but not free (one pass over the replica
            rows), so internal callers that only need the service row
            should leave both off.
        with_yaml: Whether to include the rendered YAML (``pool_yaml`` for
            pools, secret-redacted ``service_yaml`` for services). Liveness
            callers can skip the parse/dump work when they only need
            controller metadata.
        with_target_num_replicas: Whether to fetch autoscaler info
            (including ``target_num_replicas``) from the controller and merge
            controller-independent request telemetry when available.
            This is an HTTP round-trip to the controller process, so it is
            opt-in: control and liveness paths (HA recovery, termination,
            registration polling) must never block on a possibly-dead
            controller's connect timeout for fields they do not read. Only
            user-facing status rendering should pass True.
        status_snapshot_only: Whether to read only lifecycle fields from the
            services table. Callers must opt in explicitly because some
            YAML-free lifecycle paths still inspect latest-version metadata.

    Returns:
        Provider-free state for the service if it exists, otherwise None.
    """
    if status_snapshot_only:
        if (with_replica_info or with_replica_counts or with_yaml or
                with_target_num_replicas):
            raise ValueError('A status-only snapshot cannot include service '
                             'enrichment.')
        record = serve_state.get_service_status_snapshot(service_name,
                                                         require_version=True)
    else:
        record = serve_state.get_service_from_name(service_name)
    if record is None:
        return None
    if record['pool'] != pool:
        return None

    if record['pool'] and with_yaml:
        record['pool_yaml'] = ''
        version = record['version']
        try:
            yaml_content = get_yaml_content(service_name, version,
                                            record.get('resource_scope'))
            raw_yaml_config = yaml_utils.read_yaml_str(yaml_content)
        except Exception as e:  # pylint: disable=broad-except
            # If this is a consolidation mode running without an PVC, the file
            # might lost after an API server update (restart). In such case, we
            # don't want it to crash the command. Fall back to an empty string.
            logger.error(f'Failed to read YAML for service {service_name} '
                         f'with version {version}: {e}')
            record['pool_yaml'] = ''
        else:
            original_config = raw_yaml_config.get('_user_specified_yaml')
            if original_config is None:
                # Fall back to old display format.
                original_config = raw_yaml_config
                original_config.pop('run', None)
                svc: dict[str, Any] = original_config.pop('service')
                if svc is not None:
                    svc.pop('pool', None)  # Remove pool from service config
                    original_config['pool'] = svc  # Add pool to root config
            else:
                original_config = yaml_utils.safe_load(original_config)
            record['pool_yaml'] = yaml_utils.dump_yaml_str(original_config)
    elif not record['pool'] and with_yaml:
        # Services get a display-only copy of their YAML (e.g. for the
        # dashboard service page). Unlike ``pool_yaml``, which the batch
        # coordinator parses back into a launchable task, this is for
        # humans, so secrets are redacted.
        record['service_yaml'] = ''
        version = record['version']
        try:
            # The latest-version join already fetched the YAML; only fall
            # back to a storage read for old rows that predate YAML-in-DB.
            stored_yaml = record.get('yaml_content')
            if stored_yaml is None:
                stored_yaml = get_yaml_content(service_name, version,
                                               record.get('resource_scope'))
            raw_yaml_config = yaml_utils.read_yaml_str(stored_yaml)
            original_yaml = raw_yaml_config.get('_user_specified_yaml')
            if original_yaml is None:
                # Old records without the embedded user YAML: show the
                # rendered task YAML instead.
                original_yaml = stored_yaml
            record['service_yaml'] = debug_dump_helpers.redact_task_yaml(
                str(original_yaml))
        except Exception as e:  # pylint: disable=broad-except
            # Same rationale as the pool branch: a lost or malformed YAML
            # must not fail the whole status query.
            logger.error(f'Failed to read YAML for service {service_name} '
                         f'with version {version}: {e}')
            record['service_yaml'] = ''

    if with_target_num_replicas:
        record['target_num_replicas'] = 0
        durable_request_summary = None
        if not pool:
            durable_request_summary = demand_state.get_request_summary(
                service_name, record['hash'])
        try:
            controller_kwargs = {}
            if (durable_request_summary is not None and
                    durable_request_summary.get('request_telemetry_state')
                    == 'fresh'):
                controller_kwargs['timeout'] = (
                    constants.DURABLE_DEMAND_CONTROLLER_STATUS_TIMEOUT_SECONDS)
            resp = _get_to_controller_with_retry(service_name, record['hash'],
                                                 '/autoscaler/info',
                                                 **controller_kwargs)
            autoscaler_info = resp.json()
            record['target_num_replicas'] = autoscaler_info[
                'target_num_replicas']
            request_field_map = {
                'min_replicas_by_accelerator': 'min_replicas_by_accelerator',
                'target_num_replicas_by_accelerator': 'target_num_replicas_by_accelerator',
                'demand_target_by_accelerator': 'demand_target_by_accelerator',
                'warm_retention_target_by_accelerator': 'warm_retention_target_by_accelerator',
                'cold_launch_authority_by_accelerator': 'cold_launch_authority_by_accelerator',
                'ready_replicas_by_accelerator': 'ready_replicas_by_accelerator',
                'provisioning_replicas_by_accelerator': 'provisioning_replicas_by_accelerator',
                'total_replicas_by_accelerator': 'total_replicas_by_accelerator',
                'zero_cost_ready_replicas_by_accelerator': 'zero_cost_ready_replicas_by_accelerator',
                'fill_target_by_accelerator': 'fill_target_by_accelerator',
                'free_reserved_slots_by_accelerator': 'free_reserved_slots_by_accelerator',
                'fill_target': 'fill_target',
                'fill_free_slots': 'fill_free_slots',
                'reserved_fill_reconciliation': 'reserved_fill_reconciliation',
                'recent_request_count': 'recent_request_count',
                'request_window_seconds': 'request_window_seconds',
                'requests_per_second': 'requests_per_second',
                'observed_ready_replicas': 'ready_replicas',
                'in_flight_requests': 'in_flight_total',
                'confirmed_in_flight_requests': 'confirmed_in_flight_requests',
                'unknown_in_flight_replica_count': 'unknown_in_flight_replica_count',
                'request_queue_depth': 'queue_depth',
                'rejected_requests': 'rejected_in_window',
                'recent_rejected_requests': 'rejected_in_recent_window',
                'rejected_concurrency': 'rejected_concurrency',
                'raw_target_num_replicas': 'raw_target_num_replicas',
                'committed_capacity': 'committed_capacity',
                'target_utilization_percentage': 'target_utilization_percentage',
                'latest_scale_up_wave_at': 'latest_scale_up_wave_at',
                'observed_ready_replicas_age_seconds': 'report_age_seconds',
                'request_stats_age_seconds': 'report_age_seconds',
                'committed_version': 'committed_version',
                'applied_version': 'applied_version',
                'update_apply_pending': 'update_apply_pending',
                'update_apply_lag_seconds': 'update_apply_lag_seconds',
                'update_apply_error': 'update_apply_error',
                'update_apply_failures': 'update_apply_failures',
                'quarantined_version': 'quarantined_version',
                'quarantined_at': 'quarantined_at',
                'quarantine_reason': 'quarantine_reason',
            }
            for record_field, autoscaler_field in request_field_map.items():
                if autoscaler_field in autoscaler_info:
                    record[record_field] = autoscaler_info[autoscaler_field]
        except requests.exceptions.RequestException:
            record['target_num_replicas'] = None
        except Exception as e:  # pylint: disable=broad-except
            record['target_num_replicas'] = None
            logger.error(f'Failed to get autoscaler info for {service_name}: '
                         f'{common_utils.format_exception(e)}\n'
                         f'Traceback: {traceback.format_exc()}')

        # Request telemetry has an independent data-plane-to-PostgreSQL path.
        # Prefer it whenever fresh, including when the controller request
        # above failed. During the dark-write rollout, preserve a usable
        # legacy controller value until the first durable report arrives, but
        # always expose the durable freshness state explicitly.
        if not pool:
            assert durable_request_summary is not None
            telemetry_fields = {
                'request_telemetry_state',
                'request_telemetry_reason',
                'request_telemetry_generation',
                'request_telemetry_compatibility_complete',
                'request_reporter_count',
                'request_telemetry_observed_at',
                'processing_requests',
                'confirmed_processing_requests',
                'http_in_flight_requests',
            }
            for field in telemetry_fields:
                record[field] = durable_request_summary.get(field)
            if (durable_request_summary.get('request_telemetry_state') ==
                    'fresh'):
                record.update(durable_request_summary)
            elif record.get('recent_request_count') is None:
                record.update(durable_request_summary)

    if with_replica_counts and not with_replica_info:
        # Summary mode: give callers (the dashboard header, list views)
        # enough to render without the expensive per-replica
        # `to_info_dict` serialization below. Physical services use one
        # indexed GROUP BY. Logical services scan only the compact JSON state
        # needed for persisted widths; neither path resolves cluster records,
        # endpoints, handles, or cloud/cluster APIs.
        logical = bool(record.get('logical_replica_semantics'))
        if logical:
            status_counts, capacity_counts = (
                serve_state.get_replica_status_and_capacity_counts(service_name)
            )
        else:
            status_counts = serve_state.get_replica_status_counts(service_name)
            capacity_counts = status_counts
        record['replica_status_counts'] = status_counts
        _set_replica_status_aggregates(record, status_counts, capacity_counts)

    prepared = _PreparedServiceStatus(record=record,
                                      pool=pool,
                                      include_replica_info=with_replica_info)
    if with_replica_info:
        prepared.replica_infos = serve_state.get_replica_infos(service_name)
        # Pre-fetch cluster records in one batched DB query instead of
        # letting each to_info_dict() do its own. With a long failure
        # history this was an N+1.
        cluster_names = [info.cluster_name for info in prepared.replica_infos]
        prepared.cluster_records = global_user_state.get_clusters_from_names(
            cluster_names)
        # Local import avoids the serve_utils -> reserved_capacity ->
        # serve_state cycle. Group protocol-v2 rows by physical target so a
        # later global provider phase can perform one UID proof per pool even
        # when several services share it. Merely constructing the context
        # manager below validates durable row/handle agreement; it performs no
        # provider I/O until entered by the phased serializer.
        # pylint: disable-next=import-outside-toplevel
        from sky.serve import reserved_capacity

        for info in prepared.replica_infos:
            cluster_record = prepared.cluster_records.get(info.cluster_name)
            try:
                cleanup_fence = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
                if cleanup_fence is None:
                    prepared.ordinary_infos.append(info)
                    continue
                handle = (cluster_record.get('handle') if isinstance(
                    cluster_record, dict) else None)
                reserved_capacity.protocol_v2_provider_fence(info, handle)
            except exceptions.KubernetesPhysicalClusterIdentityError:
                prepared.identity_uncertain_infos.append(info)
                continue
            prepared.validated_handles[info.replica_id] = handle
            group_key = (cleanup_fence.kubernetes_context,
                         cleanup_fence.physical_cluster_uid)
            prepared.fenced_groups.setdefault(group_key, []).append(info)

        if pool:
            prepared.job_status_counts = (
                managed_job_state.get_nonterminal_job_status_counts_by_pool(
                    service_name))
            # Fetch all nonterminal job ids in the pool in a single query,
            # grouped by current_cluster_name. Avoids the N+1 pattern of
            # (1 + len(replicas)) per-pool queries against a job_info table
            # that may contain tens of thousands of finished rows.
            # Pool-level jobs (e.g. batch coordinators) span every worker.
            # They have pool set but no cluster_name, so they live under the
            # None bucket of the grouped result. Note: the prior per-call
            # implementation passed cluster_name=None to a function that
            # treated None as "no filter" rather than "IS NULL", so it
            # accidentally returned every nonterminal job in the pool and
            # surfaced unrelated replicas' jobs as `used_by` on each READY
            # worker. The grouped query lets us implement the intended
            # semantic exactly.
            prepared.jobs_by_cluster = (
                managed_job_state.get_nonterminal_job_ids_by_pool_grouped(
                    service_name))
    return prepared


_PreparedReplicaStatus = tuple[_PreparedServiceStatus, Any]


def _sanitize_provider_uncertain_status(
        replica_record: dict[str, Any],
        *,
        strip_placement_metadata: bool = False) -> dict[str, Any]:
    """Fail a provider partition closed without dropping its durable row."""
    replica_record['status'] = serve_state.ReplicaStatus.UNKNOWN
    replica_record['endpoint'] = None
    replica_record['handle'] = None
    replica_record['launched_at'] = None
    replica_record['provider_identity_uncertain'] = True
    if strip_placement_metadata:
        for field in _PROVIDER_STATUS_FIELDS:
            replica_record.pop(field, None)
    return replica_record


def _provider_uncertain_replica_status(info: Any,
                                       *,
                                       strip_placement_metadata: bool = False
                                      ) -> dict[str, Any]:
    """Serialize durable fields only; no provider or replacement metadata."""
    try:
        replica_record = info.to_info_dict(with_handle=True,
                                           with_url=False,
                                           cluster_record=None,
                                           rate_cache=None)
    except Exception as error:  # pylint: disable=broad-except
        # Corrupt presentation data must not make the fail-closed fallback
        # call the same failing serializer again and black out every peer.
        # These are direct durable fields; provider-derived fields are
        # deliberately absent/unknown.
        logger.warning(
            'Using minimal durable status for replica %s after serialization '
            'failed: %s', getattr(info, 'replica_id', '<unknown>'),
            common_utils.format_exception(error))
        replica_record = {
            'replica_id': getattr(info, 'replica_id', None),
            'replica_record_id': getattr(info, 'replica_record_id', None),
            'name': getattr(info, 'cluster_name', '<unknown>'),
            'version': getattr(info, 'version', None),
            'is_spot': getattr(info, 'is_spot', None),
            'status': serve_state.ReplicaStatus.UNKNOWN,
        }
    return _sanitize_provider_uncertain_status(
        replica_record, strip_placement_metadata=strip_placement_metadata)


def _serialize_prepared_replica(prepared: _PreparedServiceStatus,
                                info: Any) -> dict[str, Any]:
    """Serialize one admitted replica from the prepared cluster snapshot."""
    # Separate physical groups of one service can run concurrently. Protect
    # its pricing memo while retaining provider fanout across services.
    with prepared.rate_cache_lock:
        replica_record = info.to_info_dict(
            with_handle=True,
            with_url=not prepared.pool,
            cluster_record=prepared.cluster_records.get(info.cluster_name),
            rate_cache=prepared.rate_cache,
        )
    if replica_record.get('provider_identity_uncertain'):
        return _sanitize_provider_uncertain_status(replica_record)
    return replica_record


def _store_serialized_results(
        results: list[tuple[_PreparedServiceStatus, int, dict[str,
                                                              Any]]]) -> None:
    for prepared, replica_id, replica_record in results:
        prepared.serialized_by_id[replica_id] = replica_record


def _uncertain_results(
    entries: list[_PreparedReplicaStatus],
    *,
    strip_placement_metadata: bool = False,
) -> list[tuple[_PreparedServiceStatus, int, dict[str, Any]]]:
    return [(prepared, info.replica_id,
             _provider_uncertain_replica_status(
                 info, strip_placement_metadata=strip_placement_metadata))
            for prepared, info in entries]


def _serialize_status_entries_with_row_isolation(
    entries: list[_PreparedReplicaStatus],
) -> list[tuple[_PreparedServiceStatus, int, dict[str, Any]]]:
    """Serialize rows independently while preserving phase/fence failures."""
    results = []
    for prepared, info in entries:
        try:
            replica_record = _serialize_prepared_replica(prepared, info)
        # pylint: disable-next=try-except-raise
        except (exceptions.KubernetesPhysicalClusterIdentityError,
                exceptions.ProviderPhaseError):
            # These are authority/phase facts shared by the surrounding
            # partition and must retain its coherent fail-closed handling.
            raise
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Service status could not serialize replica %s; withholding '
                'only that row: %s', info.replica_id,
                common_utils.format_exception(error))
            replica_record = _provider_uncertain_replica_status(
                info, strip_placement_metadata=True)
        results.append((prepared, info.replica_id, replica_record))
    return results


def _serialize_v2_status_group(
    entries: list[_PreparedReplicaStatus],
    admission: provider_phase.ProviderPhaseAdmission | None = None,
) -> list[tuple[_PreparedServiceStatus, int, dict[str, Any]]]:
    """Serialize one physical v2 pool under one UID proof."""
    assert entries
    join_context: contextlib.AbstractContextManager[Any] = (
        contextlib.nullcontext()
        if admission is None else provider_phase.join_provider_phase(admission))
    representative_prepared, representative = entries[0]
    # Local import avoids the serve_utils -> reserved_capacity -> serve_state
    # initialization cycle.
    # pylint: disable-next=import-outside-toplevel
    from sky.serve import reserved_capacity
    try:
        with join_context:
            with reserved_capacity.protocol_v2_provider_fence(
                    representative, representative_prepared.validated_handles[
                        representative.replica_id]):
                return _serialize_status_entries_with_row_isolation(entries)
    except exceptions.KubernetesPhysicalClusterIdentityError as error:
        logger.warning(
            'Service status fenced off a protocol-v2 provider partition: %s',
            common_utils.format_exception(error))
        return _uncertain_results(entries)
    except exceptions.ProviderPhaseTimeoutError as error:
        logger.warning(
            'Service status timed out joining a protocol-v2 provider phase: '
            '%s', common_utils.format_exception(error))
        return _uncertain_results(entries, strip_placement_metadata=True)
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(
            'Service status could not serialize one protocol-v2 provider '
            'partition; withholding only that partition: %s',
            common_utils.format_exception(error))
        return _uncertain_results(entries, strip_placement_metadata=True)


def _serialize_ordinary_status_partition(
    entries: list[_PreparedReplicaStatus],
    admission: provider_phase.ProviderPhaseAdmission | None = None,
) -> list[tuple[_PreparedServiceStatus, int, dict[str, Any]]]:
    """Serialize one service's ordinary replicas in the ambient phase."""
    assert entries
    join_context: contextlib.AbstractContextManager[Any] = (
        contextlib.nullcontext()
        if admission is None else provider_phase.join_provider_phase(admission))
    try:
        with join_context:
            return _serialize_status_entries_with_row_isolation(entries)
    except exceptions.ProviderPhaseTimeoutError as error:
        logger.warning(
            'Service status fenced off an ambient provider partition: %s',
            common_utils.format_exception(error))
        return _uncertain_results(entries, strip_placement_metadata=True)
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(
            'Service status could not serialize one ambient provider '
            'partition; withholding only that partition: %s',
            common_utils.format_exception(error))
        return _uncertain_results(entries, strip_placement_metadata=True)


def _seed_identity_uncertain_statuses(
        prepared_statuses: list[_PreparedServiceStatus]) -> None:
    for prepared in prepared_statuses:
        _store_serialized_results(
            _uncertain_results([
                (prepared, info) for info in prepared.identity_uncertain_infos
            ]))


def _global_v2_status_groups(
    prepared_statuses: list[_PreparedServiceStatus],
) -> dict[tuple[str, str], list[_PreparedReplicaStatus]]:
    groups: dict[tuple[str, str], list[_PreparedReplicaStatus]] = {}
    for prepared in prepared_statuses:
        for key, infos in prepared.fenced_groups.items():
            groups.setdefault(key, []).extend(
                (prepared, info) for info in infos)
    return groups


def _reject_conflicting_v2_status_groups(
    groups: dict[tuple[str, str], list[_PreparedReplicaStatus]],) -> None:
    """Fail every contradictory UID for one mutable context closed."""
    keys_by_context: dict[str, list[tuple[str, str]]] = {}
    for key in groups:
        keys_by_context.setdefault(key[0], []).append(key)
    conflicted_keys = [
        key for keys in keys_by_context.values()
        if len({candidate[1] for candidate in keys}) > 1 for key in keys
    ]
    for key in conflicted_keys:
        entries = groups.pop(key)
        logger.warning(
            'Service status rejected conflicting physical-cluster '
            'UIDs for Kubernetes context %r.', key[0])
        _store_serialized_results(_uncertain_results(entries))


def _ordinary_status_partitions(
    prepared_statuses: list[_PreparedServiceStatus],
) -> list[list[_PreparedReplicaStatus]]:
    return [[(prepared, info)
             for info in prepared.ordinary_infos]
            for prepared in prepared_statuses
            if prepared.ordinary_infos]


def _mark_phase_timeout(partitions: typing.Iterable[
    list[_PreparedReplicaStatus]], mode: provider_phase.ProviderPhaseMode,
                        error: exceptions.ProviderPhaseTimeoutError) -> None:
    logger.warning('Service status timed out waiting for provider phase %s: %s',
                   mode.value, common_utils.format_exception(error))
    for entries in partitions:
        _store_serialized_results(
            _uncertain_results(entries, strip_placement_metadata=True))


def _serialize_prepared_statuses_synchronously(
        prepared_statuses: list[_PreparedServiceStatus]) -> None:
    """Run v2 then ambient serialization without child-thread admission."""
    _seed_identity_uncertain_statuses(prepared_statuses)
    v2_groups = _global_v2_status_groups(prepared_statuses)
    _reject_conflicting_v2_status_groups(v2_groups)
    if v2_groups:
        try:
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED):
                for entries in v2_groups.values():
                    _store_serialized_results(
                        _serialize_v2_status_group(entries))
        except exceptions.ProviderPhaseTimeoutError as error:
            _mark_phase_timeout(v2_groups.values(),
                                provider_phase.ProviderPhaseMode.V2_FENCED,
                                error)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Service status could not enter the v2 provider '
                'phase: %s', common_utils.format_exception(error))
            for entries in v2_groups.values():
                _store_serialized_results(
                    _uncertain_results(entries, strip_placement_metadata=True))

    ordinary_partitions = _ordinary_status_partitions(prepared_statuses)
    if ordinary_partitions:
        try:
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
                for entries in ordinary_partitions:
                    _store_serialized_results(
                        _serialize_ordinary_status_partition(entries))
        except exceptions.ProviderPhaseTimeoutError as error:
            _mark_phase_timeout(ordinary_partitions,
                                provider_phase.ProviderPhaseMode.AMBIENT_LEGACY,
                                error)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Service status could not enter the ambient '
                'provider phase: %s', common_utils.format_exception(error))
            for entries in ordinary_partitions:
                _store_serialized_results(
                    _uncertain_results(entries, strip_placement_metadata=True))


def _run_status_phase_fanout(
    work: list[list[_PreparedReplicaStatus]],
    worker: Callable[
        [list[_PreparedReplicaStatus], provider_phase.ProviderPhaseAdmission],
        list[tuple[_PreparedServiceStatus, int, dict[str, Any]]],
    ],
    admission: provider_phase.ProviderPhaseAdmission,
    parent_ctx: contextvars.Context,
) -> None:
    """Run and fully join one admitted status-provider fanout."""
    max_workers = min(len(work), _STATUS_FANOUT_MAX_WORKERS)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [(ex.submit(parent_ctx.copy().run, worker, entries,
                              admission), entries) for entries in work]
        for future, entries in futures:
            try:
                results = future.result()
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(
                    'Service status provider fanout failed for one partition; '
                    'withholding only that partition: %s',
                    common_utils.format_exception(error))
                results = _uncertain_results(entries,
                                             strip_placement_metadata=True)
            _store_serialized_results(results)


def _serialize_prepared_statuses_with_fanout(
        prepared_statuses: list[_PreparedServiceStatus],
        parent_ctx: contextvars.Context) -> None:
    """Serialize a batch under one fully joined v2 and ambient root each."""
    _seed_identity_uncertain_statuses(prepared_statuses)
    v2_groups = _global_v2_status_groups(prepared_statuses)
    _reject_conflicting_v2_status_groups(v2_groups)
    if v2_groups:
        v2_work = list(v2_groups.values())
        try:
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED) as admission:
                _run_status_phase_fanout(v2_work, _serialize_v2_status_group,
                                         admission, parent_ctx)
        except exceptions.ProviderPhaseTimeoutError as error:
            _mark_phase_timeout(v2_work,
                                provider_phase.ProviderPhaseMode.V2_FENCED,
                                error)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Service status could not enter the v2 provider '
                'phase: %s', common_utils.format_exception(error))
            for entries in v2_work:
                _store_serialized_results(
                    _uncertain_results(entries, strip_placement_metadata=True))

    ordinary_work = _ordinary_status_partitions(prepared_statuses)
    if ordinary_work:
        try:
            with provider_phase.provider_phase(provider_phase.ProviderPhaseMode.
                                               AMBIENT_LEGACY) as admission:
                _run_status_phase_fanout(ordinary_work,
                                         _serialize_ordinary_status_partition,
                                         admission, parent_ctx)
        except exceptions.ProviderPhaseTimeoutError as error:
            _mark_phase_timeout(ordinary_work,
                                provider_phase.ProviderPhaseMode.AMBIENT_LEGACY,
                                error)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Service status could not enter the ambient '
                'provider phase: %s', common_utils.format_exception(error))
            for entries in ordinary_work:
                _store_serialized_results(
                    _uncertain_results(entries, strip_placement_metadata=True))


def _finalize_prepared_service_status(
        prepared: _PreparedServiceStatus) -> dict[str, Any]:
    """Attach provider results and aggregates without further I/O."""
    record = prepared.record
    if prepared.include_replica_info:
        replica_records = [
            prepared.serialized_by_id[info.replica_id]
            for info in prepared.replica_infos
        ]
        record['replica_info'] = replica_records
        full_status_counts: collections.defaultdict[str, int] = (
            collections.defaultdict(int))
        full_capacity_counts: collections.defaultdict[str, int] = (
            collections.defaultdict(int))
        logical = bool(record.get('logical_replica_semantics'))
        for info, replica_record in zip(prepared.replica_infos,
                                        replica_records):
            status = replica_record['status'].value
            full_status_counts[status] += 1
            full_capacity_counts[status] += (info.planned_capacity
                                             if logical else 1)
        _set_replica_status_aggregates(record, dict(full_status_counts),
                                       dict(full_capacity_counts))
        if prepared.pool:
            record['job_status_counts'] = prepared.job_status_counts
            jobs_by_cluster = prepared.jobs_by_cluster or {}
            pool_level_job_ids = list(jobs_by_cluster.get(None, []))
            for replica_record in replica_records:
                job_ids = list(jobs_by_cluster.get(replica_record['name'], []))
                if (replica_record.get('status') ==
                        serve_state.ReplicaStatus.READY):
                    job_ids = list(dict.fromkeys(pool_level_job_ids + job_ids))
                replica_record['used_by'] = job_ids
    observed_ready = record.get('observed_ready_replicas')
    # Demand telemetry and the controller's router-capacity observation have
    # independent writers and freshness clocks. A fresh demand report must not
    # revive stale ready capacity, and an unavailable demand report must not
    # invalidate a fresh controller capacity observation during transition.
    report_age = record.get('observed_ready_replicas_age_seconds',
                            record.get('request_stats_age_seconds'))
    observed_ready_is_valid = (isinstance(observed_ready, int) and
                               not isinstance(observed_ready, bool) and
                               observed_ready >= 0)
    observed_ready_is_fresh = (
        observed_ready_is_valid and isinstance(report_age, (int, float)) and
        not isinstance(report_age, bool) and report_age >= 0 and
        report_age <= 3 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
    if observed_ready_is_valid:
        record['observed_ready_replicas_fresh'] = observed_ready_is_fresh
    # The router observation is the best live logical-capacity count only
    # while the controller report carrying it is fresh. Once controller
    # reporting stops, replica reconciliation can remove backends while this
    # value remains frozen;
    # replacing the current snapshot would then produce impossible displays
    # such as 262 ready / 64 total.  Keep the stale observation as a diagnostic
    # but fall back to the provider/replica-state aggregate for readiness.
    if 'ready_replicas' in record and observed_ready_is_fresh:
        record['ready_replicas'] = observed_ready
    return record


def _get_service_status(
        service_name: str,
        pool: bool,
        with_replica_info: bool = True,
        with_replica_counts: bool = False,
        with_yaml: bool = True,
        with_target_num_replicas: bool = False,
        status_snapshot_only: bool = False) -> dict[str, Any] | None:
    """Get one service status using synchronous v2-before-ambient phases."""
    prepared = _prepare_service_status(
        service_name,
        pool,
        with_replica_info=with_replica_info,
        with_replica_counts=with_replica_counts,
        with_yaml=with_yaml,
        with_target_num_replicas=with_target_num_replicas,
        status_snapshot_only=status_snapshot_only)
    if prepared is None:
        return None
    _serialize_prepared_statuses_synchronously([prepared])
    return _finalize_prepared_service_status(prepared)


def resolve_target_qps_for_gpu_shape(
        gpu_type: str, gpu_count: int,
        target_qps_per_replica: dict[str, float]) -> float | None:
    """Per-REPLICA target QPS for a replica with `gpu_count` x `gpu_type`.

    Key semantics (backward compatible with the count-blind matcher):
      1. An exact shape key ('L4:4') is a per-replica value, used as-is.
      2. A bare type key ('L4') is a per-GPU value, multiplied by the
         replica's GPU count.
      3. A count-suffixed key of the same type but different count
         ('L4:1' for an L4:8 replica) is normalized to per-GPU
         (value / key count) and multiplied by the replica's count.
    Returns None when nothing matches (caller picks its fallback).

    NOTE: the per-GPU semantics of (2) and (3) assume ONE model instance
    per GPU (each server pinned to a distinct device). For a model that
    needs k GPUs per instance, per-GPU scaling would overcount capacity
    by k: declare an exact per-replica shape key instead, e.g. a 2-GPU
    model on L4:8 machines serving 4 instances at 0.1 qps each ->
    {'L4:8': 0.4}.
    """
    exact_key = f'{gpu_type}:{gpu_count}'
    if exact_key in target_qps_per_replica:
        return target_qps_per_replica[exact_key]
    if gpu_type in target_qps_per_replica:
        return target_qps_per_replica[gpu_type] * gpu_count
    for key, value in target_qps_per_replica.items():
        base, _, count_str = key.partition(':')
        if base == gpu_type and count_str.isdigit() and int(count_str) > 0:
            return value / int(count_str) * gpu_count
    return None


def get_service_status_pickled(
    service_names: list[str] | None,
    pool: bool,
    summary_only: bool = False,
    include_target_num_replicas: bool | None = None,
    metadata_only: bool = False,
) -> list[dict[str, str]]:
    if summary_only and metadata_only:
        raise ValueError(
            'summary_only and metadata_only are mutually exclusive.')
    if service_names is None:
        # Get all names for the requested mode only.
        service_names = serve_state.get_glob_service_names(None, pool=pool)
    if not service_names:
        return []
    if include_target_num_replicas is None:
        include_target_num_replicas = not summary_only and not metadata_only
    # Fan out the provider-free service/replica/cluster snapshots first. The
    # resulting immutable work set is then serialized under one process-wide
    # v2 phase followed by one ambient phase; no worker can independently
    # interleave the two authority modes.
    # Each task gets a fresh `Context.copy()` because the same Context
    # can't be entered from multiple threads (Context.run raises
    # RuntimeError otherwise) — but the values (request_id / user_id)
    # are inherited so log redirection still works inside workers.
    parent_ctx = contextvars.copy_context()

    def _run_in_context(name: str) -> _PreparedServiceStatus | None:
        kwargs = {
            'pool': pool,
            'with_replica_info': not summary_only and not metadata_only,
            'with_replica_counts': summary_only,
            'with_target_num_replicas': include_target_num_replicas,
        }
        if metadata_only:
            kwargs['status_snapshot_only'] = True
        # Service summaries are metadata-only dashboard snapshots. Avoid
        # parsing, redacting, and dumping one YAML document per service on
        # every poll. Pool summaries deliberately keep YAML because pool
        # lifecycle consumers parse it back into a launchable task.
        if (summary_only and not pool) or metadata_only:
            kwargs['with_yaml'] = False
        prepared = parent_ctx.copy().run(_prepare_service_status, name,
                                         **kwargs)
        if prepared is not None and metadata_only:
            prepared.record['metadata_only'] = True
        return prepared

    max_workers = min(len(service_names), _STATUS_FANOUT_MAX_WORKERS)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            (name, ex.submit(_run_in_context, name)) for name in service_names
        ]
        prepared_statuses = []
        for name, future in futures:
            try:
                prepared_statuses.append(future.result())
            except Exception as error:  # pylint: disable=broad-except
                # A malformed service/replica snapshot is not evidence about
                # any peer service. Keep the dashboard/status batch available
                # for healthy services; the failed service is retried on the
                # next poll.
                logger.warning(
                    'Service status preparation failed for %r; omitting only '
                    'that service from this snapshot: %s', name,
                    common_utils.format_exception(error))
    live_prepared = [
        prepared for prepared in prepared_statuses if prepared is not None
    ]
    _serialize_prepared_statuses_with_fanout(live_prepared, parent_ctx)
    live_statuses = sorted((_finalize_prepared_service_status(prepared)
                            for prepared in live_prepared),
                           key=lambda status: status['name'])
    for status in live_statuses:
        # The rendered YAML carries plaintext secrets (replicas need it
        # launchable), so it never leaves the controller for services:
        # clients get the redacted `service_yaml` instead. Pools keep it
        # because the batch coordinator and worker-count updates parse
        # `yaml_content`/`pool_yaml` back into a launchable task.
        if not status.get('pool'):
            status.pop('yaml_content', None)
    return [{
        k: base64.b64encode(pickle.dumps(v)).decode('utf-8')
        for k, v in s.items()
    }
            for s in live_statuses]


# TODO (kyuds): remove when serve codegen is removed
def get_service_status_encoded(service_names: list[str] | None,
                               pool: bool,
                               summary_only: bool = False,
                               include_target_num_replicas: bool | None = None,
                               metadata_only: bool = False) -> str:
    # We have to use payload_type here to avoid the issue of
    # message_utils.decode_payload() not being able to correctly decode the
    # message with <sky-payload> tags.
    service_statuses = get_service_status_pickled(
        service_names,
        pool,
        summary_only=summary_only,
        metadata_only=metadata_only,
        include_target_num_replicas=include_target_num_replicas)
    return message_utils.encode_payload(service_statuses,
                                        payload_type='service_status')


def unpickle_service_status(
        payload: list[dict[str, str]]) -> list[dict[str, Any]]:
    service_statuses: list[dict[str, Any]] = []
    for service_status in payload:
        if not isinstance(service_status, dict):
            raise ValueError(f'Invalid service status: {service_status}')
        service_statuses.append({
            k: pickle.loads(base64.b64decode(v))
            for k, v in service_status.items()
        })
    return service_statuses


# TODO (kyuds): remove when serve codegen is removed
def load_service_status(payload: str) -> list[dict[str, Any]]:
    try:
        service_statuses_encoded = message_utils.decode_payload(
            payload, payload_type='service_status')
    except ValueError as e:
        if 'Invalid payload string' in str(e):
            # Backward compatibility for serve controller started before #4660
            # where the payload type is not added.
            service_statuses_encoded = message_utils.decode_payload(payload)
        else:
            raise
    return unpickle_service_status(service_statuses_encoded)


# TODO (kyuds): remove when serve codegen is removed
def add_version_encoded(service_name: str) -> str:
    new_version = serve_state.add_version(service_name)
    return message_utils.encode_payload(new_version)


# TODO (kyuds): remove when serve codegen is removed
def load_version_string(payload: str) -> str:
    return message_utils.decode_payload(payload)


def get_ready_replicas(
    service_name: str,
    replicas: list['replica_managers.ReplicaInfo'] | None = None
) -> list['replica_managers.ReplicaInfo']:
    logger.info(f'Get number of replicas for pool {service_name!r}')
    if replicas is None:
        replicas = serve_state.get_replica_infos(service_name)
    return [
        info for info in replicas
        if info.status == serve_state.ReplicaStatus.READY
    ]


def _get_pool_cluster_records(
    replicas: list['replica_managers.ReplicaInfo']
) -> dict[str, dict[str, Any] | None]:
    return global_user_state.get_clusters_from_names(
        [replica_info.cluster_name for replica_info in replicas])


def _task_fits(task_resources: 'resources_lib.Resources',
               free_resources: 'resources_lib.Resources') -> bool:
    """Check if the task resources fit in the free resources."""
    if not task_resources.less_demanding_than(free_resources,
                                              check_cloud=False):
        return False
    if task_resources.cpus is not None:
        if (free_resources.cpus is None or
                task_resources.cpus > free_resources.cpus):
            return False
    if task_resources.memory is not None:
        if (free_resources.memory is None or
                task_resources.memory > free_resources.memory):
            return False
    return True


def _is_empty_resource(resource: 'resources_lib.Resources') -> bool:
    # Returns True if this resource object does not specify any resources.
    return (resource.cpus is None and resource.memory is None and
            resource.accelerators is None)


def get_free_worker_resources(
    pool: str,
    replicas: list['replica_managers.ReplicaInfo'] | None = None,
    cluster_records: dict[str, dict[str, Any] | None] | None = None,
    resolved_handles: 'dict[str, CloudVmRayResourceHandle | None] | None' = None,
) -> dict[str, resources_lib.Resources | None] | None:
    """Get free resources for each worker in a pool.

    Args:
        pool: Pool name (service name)
        replicas: Optional replica snapshot to reuse; fetched from the state
            store when not provided.
        cluster_records: Optional cluster-table snapshot keyed by worker name.
            When provided, the free-resource walk reuses it instead of issuing
            a second batched cluster read.
        resolved_handles: Optional output mapping keyed by worker name. When
            provided, each worker's resolved handle is stored for reuse by the
            caller's post-selection bookkeeping.

    Returns:
        Dictionary mapping cluster_name (worker) to free Resources object (or
        None if worker is not available or has no free resources).
    """

    free_resources: dict[str, resources_lib.Resources | None] = {}
    if replicas is None:
        replicas = serve_state.get_replica_infos(pool)
    used_resources_by_cluster = (
        managed_job_state.get_pool_worker_used_resources_by_cluster(pool))
    if used_resources_by_cluster is None:
        logger.warning('Failed to get used resources for pool '
                       f'{pool!r}; disabling resource-aware scheduling')
        return None

    # Snapshot every worker's cluster record in one batched read; the
    # per-replica ``handle()`` fallback would issue one cluster-table read
    # per worker on every scheduling attempt.
    if cluster_records is None:
        cluster_records = _get_pool_cluster_records(replicas)
    for replica_info in replicas:
        cluster_name = replica_info.cluster_name

        # Get cluster handle
        cluster_record = cluster_records.get(cluster_name)
        handle = (None if cluster_record is None else
                  replica_info.handle(cluster_record))
        if resolved_handles is not None:
            resolved_handles[cluster_name] = handle
        if handle is None or handle.launched_resources is None:
            free_resources[cluster_name] = None
            continue

        total_resources = handle.launched_resources

        used_resources = used_resources_by_cluster.get(cluster_name)
        if used_resources is None:
            free_resources[cluster_name] = total_resources
            continue

        if _is_empty_resource(used_resources):
            # At least one job on this worker has no explicit resource request.
            # Treat the worker as fully occupied for resource-aware placement.
            logger.debug('Some jobs on cluster '
                         f'{cluster_name!r} have no resources specified. '
                         'Skipping resource-aware scheduling for cluster '
                         f'{cluster_name!r}')
            free_resources[cluster_name] = resources_lib.Resources()
        else:
            # Calculate free resources using - operator
            free = total_resources - used_resources
            free_resources[cluster_name] = free

    return free_resources


def get_next_cluster_name(
    service_name: str,
    job_id: int,
    task_resources: typing.Union['resources_lib.Resources',
                                 set['resources_lib.Resources'],
                                 list['resources_lib.Resources']] | None = None
) -> str | None:
    """Get the next available cluster name from replicas with sufficient
    resources.

    Args:
        service_name: The name of the service.
        job_id: Job ID to associate with the acquired cluster.
        task_resources: Optional task resource requirements. If provided, will
                check if resources fit in free worker resources. Can be
                a single Resources object or a set/list of Resources objects.

    Returns:
        The cluster name if a suitable replica is found, None otherwise.
    """
    # Check if service exists
    service_status = _get_service_status(service_name,
                                         pool=True,
                                         with_replica_info=False,
                                         with_yaml=False,
                                         status_snapshot_only=True)
    if service_status is None:
        logger.error(f'Service {service_name!r} does not exist.')
        return None
    if not service_status['pool']:
        logger.error(f'Service {service_name!r} is not a pool.')
        return None

    with filelock.FileLock(get_service_filelock_path(service_name)):
        logger.debug(f'Get next cluster name for pool {service_name!r}')
        # Read the replica set once and share the snapshot between readiness
        # filtering and free-resource accounting so the scheduling decision
        # is made against a single consistent view of the fleet.
        replicas = serve_state.get_replica_infos(service_name)
        ready_replicas = get_ready_replicas(service_name, replicas=replicas)

        logger.debug(f'Ready replicas: {ready_replicas!r}')

        idle_replicas: list[replica_managers.ReplicaInfo] = []
        # cluster_name -> the task resource option that fit on that worker.
        chosen_resources: dict[str, resources_lib.Resources] = {}

        # If task_resources is provided, use resource-aware scheduling
        # Normalize task_resources to a list
        if isinstance(task_resources, resources_lib.Resources):
            task_resources_list = [task_resources]
        elif isinstance(task_resources, (set, list)):
            task_resources_list = list(task_resources)
        else:
            task_resources_list = []

        # We should do resource aware scheduling if:
        # 1. There are task resources.
        # 2. The first task resource has some resources listed.
        # 3. There are free resources.
        # 4. Any free resource has some resources listed.
        resource_aware = len(task_resources_list) > 0
        resource_aware = (resource_aware and
                          not _is_empty_resource(task_resources_list[0]))

        free_resources = None
        cluster_records = None
        resolved_handles: (dict[str, CloudVmRayResourceHandle | None] |
                           None) = None
        if resource_aware:
            cluster_records = _get_pool_cluster_records(replicas)
            resolved_handles = {}
            free_resources = get_free_worker_resources(
                service_name,
                replicas=replicas,
                cluster_records=cluster_records,
                resolved_handles=resolved_handles)
            logger.debug(f'Free resources: {free_resources!r}')
            resource_aware = free_resources is not None
        if resource_aware and free_resources is not None:
            for free_resource in free_resources.values():
                if free_resource is not None and not _is_empty_resource(
                        free_resource):
                    break
            else:
                resource_aware = False

        if resource_aware:
            logger.debug('Doing resource aware scheduling')
            for replica_info in ready_replicas:
                cluster_name = replica_info.cluster_name
                assert free_resources is not None
                free_resources_on_worker = free_resources.get(cluster_name)
                logger.debug(f'Free resources for cluster {cluster_name!r}: '
                             f'{free_resources_on_worker!r}')

                # Skip if worker has no free resources available
                if free_resources_on_worker is None:
                    logger.debug(f'Worker {cluster_name!r} has no free '
                                 'resources')
                    continue

                # Check if any of the task resource options fit, remembering
                # which option fit so the selection below does not have to
                # recompute it.
                for task_res in task_resources_list:
                    logger.debug(f'Task resources: {task_res!r}')
                    if _task_fits(task_res, free_resources_on_worker):
                        logger.debug(f'Task resources {task_res!r} fits'
                                     ' in free resources '
                                     f'{free_resources_on_worker!r}')
                        chosen_resources[cluster_name] = task_res
                        idle_replicas.append(replica_info)
                        break
                    else:
                        logger.debug(f'Task resources {task_res!r} does not fit'
                                     ' in free resources '
                                     f'{free_resources_on_worker!r}')
        # Also fall back to resource unaware scheduling if no idle replicas are
        # found. This might be because our launched resources were improperly
        # set. If that's the case then jobs will fail to schedule in a resource
        # aware way because one of the resources will be `None` so we can just
        # fallback to 1 job per replica. If we are truly resource bottlenecked
        # then we will see that there are jobs running on the replica and will
        # not schedule another.
        if len(idle_replicas) == 0:
            logger.debug('Falling back to resource unaware scheduling')
            jobs_per_replica = (
                managed_job_state.get_nonterminal_job_counts_by_pool(
                    service_name))
            # Fall back to resource unaware scheduling if no task resources
            # are provided.
            for replica_info in ready_replicas:
                if jobs_per_replica.get(replica_info.cluster_name, 0) == 0:
                    idle_replicas.append(replica_info)

        if not idle_replicas:
            logger.info(f'No idle replicas found for pool {service_name!r}')
            return None

        # Select the first idle replica.
        replica_info = idle_replicas[0]
        logger.info(f'Selected replica {replica_info.replica_id} with cluster '
                    f'{replica_info.cluster_name!r} for job {job_id!r} in pool '
                    f'{service_name!r}')

        # If job has heterogeneous resources (any_of/ordered), update
        # full_resources to the specific resource that was selected for this
        # worker. This must happen before releasing the filelock to ensure
        # atomicity with the scheduling decision.
        if resource_aware and len(task_resources_list) > 1:
            chosen_res = chosen_resources.get(replica_info.cluster_name)
            if chosen_res is not None:
                logger.debug(f'Updating full_resources for job {job_id!r} '
                             f'to selected resource: {chosen_res!r}')
                managed_job_state.update_job_full_resources(
                    job_id, chosen_res.to_yaml_config())

        managed_job_state.set_current_cluster_name(job_id,
                                                   replica_info.cluster_name)

        # Set infrastructure info for sorting/filtering
        if cluster_records is None:
            handle = replica_info.handle()
        else:
            handle = None
            if resolved_handles is not None:
                handle = resolved_handles.get(replica_info.cluster_name)
            if handle is None:
                cluster_record = cluster_records.get(replica_info.cluster_name)
                handle = (None if cluster_record is None else
                          replica_info.handle(cluster_record))
        if handle is not None and handle.launched_resources is not None:
            lr = handle.launched_resources
            managed_job_state.set_job_infra(
                job_id,
                cloud=str(lr.cloud) if lr.cloud is not None else None,
                region=lr.region,
                zone=lr.zone,
            )

        return replica_info.cluster_name


def _purge_ownership_failure(service_name: str, detail: str) -> str:
    return (f'{colorama.Fore.YELLOW}failed service {service_name!r} could not '
            'be purged because its service incarnation changed during '
            f'teardown ({detail}); retry against the current service state.'
            f'{colorama.Style.RESET_ALL}')


def run_bounded_serve_teardown_threads(
    work: list[Any],
    *,
    make_worker: Callable[[Any], thread_utils.SafeThread],
    pool: bool,
    reserve_running: Callable[[list[Any], int], Mapping[int, Any]],
    restore_never_started: Callable[[Any], Any | None],
    handle_success: Callable[[Any], None],
    handle_failure: Callable[[Any, str | None], None],
    continue_guard: Callable[[], bool],
    max_concurrent_per_service: int,
    poll_interval_seconds: float = 3,
    max_no_progress_polls: int = 20,
) -> None:
    """Run provider teardown with one durable cross-pod admission budget.

    Every candidate must already carry durable ``SCHEDULED`` or ``RUNNING``
    teardown intent. ``reserve_running`` atomically counts and changes an
    exact bounded SCHEDULED batch to RUNNING on the transaction owning mutation
    authority.  A RUNNING row reconstructed after process loss is already
    charged to D and is adopted without another reservation.  Only a row this
    invocation received from ``reserve_running`` may be restored after a
    provably identity-less ``Thread.start()`` failure.  Launch saturation is
    deliberately irrelevant to this cost-cleanup budget.
    """
    if type(max_no_progress_polls) is not int or max_no_progress_polls < 1:
        raise ValueError('Serve teardown no-progress bound must be positive.')

    def _replica_identity(info: Any) -> tuple[int, str]:
        replica_id = getattr(info, 'replica_id', None)
        if (isinstance(replica_id, bool) or not isinstance(replica_id, int) or
                replica_id < 0):
            raise ValueError('Serve teardown requires a nonnegative integer '
                             'replica ID.')
        replica_record_id = getattr(info, 'replica_record_id', None)
        if not isinstance(replica_record_id, str):
            raise ValueError('Serve teardown requires a canonical replica '
                             'record UUID.')
        try:
            parsed_record_id = uuid.UUID(replica_record_id)
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError('Serve teardown requires a canonical replica '
                             'record UUID.') from error
        if str(parsed_record_id) != replica_record_id:
            raise ValueError('Serve teardown requires a canonical replica '
                             'record UUID.')
        return replica_id, replica_record_id

    pending: dict[tuple[int, str], tuple[Any,
                                         thread_utils.SafeThread | None]] = {}
    for info in work:
        identity = _replica_identity(info)
        if identity in pending:
            raise ValueError('Serve teardown work contains duplicate replica '
                             f'identity {identity!r}.')
        pending[identity] = (info, None)
    effective_infos: dict[tuple[int, str], Any] = {}
    no_progress_polls = 0
    while pending:
        made_progress = False
        if not continue_guard():
            raise RuntimeError('Serve teardown ownership was lost.')
        scheduled: list[tuple[tuple[int, str], Any,
                              thread_utils.SafeThread | None]] = []
        running_to_adopt: list[tuple[tuple[int, str], Any,
                                     thread_utils.SafeThread | None]] = []
        concurrent_workers = 0
        for identity, (info, worker) in list(pending.items()):
            effective_info = effective_infos.get(identity)
            if effective_info is None:
                effective_info = info
            if worker is None:
                phase = effective_info.status_property.sky_down_status
                if phase == common_utils.ProcessStatus.SCHEDULED:
                    scheduled.append((identity, effective_info, worker))
                    continue
                if phase == common_utils.ProcessStatus.RUNNING:
                    running_to_adopt.append((identity, effective_info, worker))
                    continue
                raise RuntimeError(
                    'Serve teardown work has no runnable durable phase for '
                    f'replica {effective_info.replica_id}.')
            if worker.is_alive():
                concurrent_workers += 1
                continue
            if (effective_info.status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED):
                scheduled.append((identity, effective_info, worker))
                continue
            if (worker.ident is None and
                    effective_info.status_property.sky_down_status
                    == common_utils.ProcessStatus.RUNNING):
                # This is durable work inherited from a lost process or an
                # ambiguous commit acknowledgement.  Teardown is idempotent;
                # RUNNING already consumes D and must be adopted, never
                # rewritten to SCHEDULED merely because this new SafeThread
                # has no native identity yet.
                running_to_adopt.append((identity, effective_info, worker))
                continue
            worker.join()
            del pending[identity]
            effective_infos.pop(identity, None)
            made_progress = True
            if worker.format_exc is None:
                handle_success(effective_info)
            else:
                handle_failure(effective_info, worker.format_exc)
        available = max(0, max_concurrent_per_service - concurrent_workers)
        for identity, effective_info, worker in running_to_adopt[:available]:
            if not continue_guard():
                raise RuntimeError('Serve teardown ownership was lost '
                                   'during RUNNING adoption.')
            if worker is None:
                worker = make_worker(effective_info)
                pending[identity] = (pending[identity][0], worker)
            try:
                worker.start()
            except BaseException as error:  # pylint: disable=broad-except
                # Inherited RUNNING is not proof that this process created the
                # reservation.  Retain it on every identity-less failure; the
                # next retry adopts the same idempotent teardown again.
                if not isinstance(error, Exception):
                    raise
                logger.warning(
                    'Could not adopt durable RUNNING teardown for replica '
                    '%s; retaining its charged reservation: %s',
                    effective_info.replica_id,
                    common_utils.format_exception(error))
            else:
                concurrent_workers += 1
                made_progress = True
        if scheduled:
            available = max(0, max_concurrent_per_service - concurrent_workers)
            selected = scheduled[:available]
            if selected:
                if not continue_guard():
                    raise RuntimeError('Serve teardown ownership was lost '
                                       'during admission.')
                reserved = reserve_running(
                    [info for _, info, _ in selected],
                    controller_utils.get_serve_termination_limit(pool))
                for identity, info, worker in selected:
                    reserved_info = reserved.get(info.replica_id)
                    if reserved_info is None:
                        continue
                    if _replica_identity(reserved_info) != identity:
                        raise RuntimeError(
                            'Serve teardown reservation changed exact replica '
                            f'identity {identity!r}.')
                    effective_infos[identity] = reserved_info
                    if worker is None:
                        worker = make_worker(reserved_info)
                        pending[identity] = (pending[identity][0], worker)
                    try:
                        worker.start()
                    except BaseException as error:  # pylint: disable=broad-except
                        # An asynchronous BaseException can land after the OS
                        # thread starts but before ``ident`` is observable.
                        # It is never proof that provider work did not begin.
                        if not isinstance(error, Exception):
                            raise
                        # ``reserved_info`` is the exact commit receipt from
                        # this invocation.  It is the only proof permitting a
                        # RUNNING -> SCHEDULED rollback when start never
                        # obtained a native identity.
                        if worker.ident is None:
                            restored = restore_never_started(reserved_info)
                            if restored is not None:
                                if _replica_identity(restored) != identity:
                                    raise RuntimeError(
                                        'Serve teardown restoration changed '
                                        'exact replica identity '
                                        f'{identity!r}.') from error
                                effective_infos[identity] = restored
                                pending[identity] = (pending[identity][0], None)
                        if worker.ident is not None:
                            logger.warning(
                                'Serve teardown worker returned a start '
                                'error after obtaining a native identity; '
                                'retaining durable RUNNING evidence: %s',
                                common_utils.format_exception(error))
                    else:
                        concurrent_workers += 1
                        made_progress = True
        if pending:
            # A global/service guard may be transiently busy, but an empty
            # admission result (or an inherited RUNNING worker that cannot be
            # adopted) must not hold a purge/controller caller forever.  Live
            # local workers are observable progress and may legitimately run
            # longer than this retry horizon.  Otherwise fail closed and leave
            # every durable row for the caller's normal cleanup retry.
            if concurrent_workers == 0 and not made_progress:
                no_progress_polls += 1
                if no_progress_polls >= max_no_progress_polls:
                    raise RuntimeError(
                        'Serve teardown admission made no progress; durable '
                        'cleanup rows were retained for retry.')
            else:
                no_progress_polls = 0
            time.sleep(poll_interval_seconds)


def _begin_service_teardown_if_owner(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> tuple[bool, Any | None]:
    """Publish SHUTTING_DOWN without blocking bound request cancellation.

    Bound ordinary launches can hold the shared launch-authority advisory
    guard for an entire provider retry.  Their cancellation must therefore be
    made reachable by the canonical row-lock transition before any exclusive
    writer is attempted.  Serve042 legacy mode is classified and marked in the
    same transaction, closing promotion races; unsupported stores keep the
    established advisory-locked hash CAS.

    Returns whether teardown was published and, for bound mode, the exact
    controller authority that may cancel and reduce existing associations.
    Binding conflicts are intentionally raised so cleanup fails closed.
    """
    # Local to avoid loading the PostgreSQL-only binding state machine on
    # legacy CLI paths during serve_utils import.
    # pylint: disable=import-outside-toplevel
    from sky.serve import ordinary_launch_binding

    # pylint: enable=import-outside-toplevel
    result = ordinary_launch_binding.begin_service_teardown_if_owner(
        service_name, expected_service_hash, expected_controller_owner)
    if result.disposition != (
            ordinary_launch_binding.ServiceTeardownDisposition.UNSUPPORTED):
        return True, result.authority
    marked = serve_state.set_service_status_and_active_versions_if_hash(
        service_name,
        expected_service_hash,
        serve_state.ServiceStatus.SHUTTING_DOWN,
        expected_lifecycle_epoch=expected_lifecycle_epoch)
    return marked, None


def quiesce_service_replica_launch_requests(
    service_name: str,
    replica_infos: list['replica_managers.ReplicaInfo'],
    continue_guard: Callable[[], bool] | None = None,
    *,
    include_terminal_history: bool = False,
) -> bool:
    """Cancel and execution-quiesce launches backed by replica inventory.

    Cancellation publishes ``CANCELLED`` before a remote executor has
    necessarily stopped its handler. Teardown may remove replica/service rows
    only after every retained target request proves that its exact execution
    generation is quiescent. Backend-guarded central controllers use the
    authoritative in-process request backend directly; public API
    authentication and response encoding are not part of this safety barrier.
    The caller must first stop the controller child (or receive its teardown
    acknowledgement), so no producer can enqueue a new launch after this
    barrier begins.

    Every backend-guarded central call uses one server-side cluster-name batch
    to discover active and already-terminal unproven requests. The
    ``include_terminal_history`` bit additionally requires that central path;
    protocol-v2 interrupted fill recovery uses it so it can never fall back to
    a remote compatibility query.

    Returns False on any transport/status/identity/ownership uncertainty.
    Callers then retain all durable service and replica rows for a later retry.
    """
    # Local to avoid requests -> decoders -> Serve state/spec -> serve_utils
    # during ``import sky``. The quiescence barrier runs only after startup.
    # pylint: disable=import-outside-toplevel
    from sky.server.requests import postgres as request_postgres
    from sky.server.requests import requests as api_requests

    # pylint: enable=import-outside-toplevel

    def _guard_allows() -> bool:
        if continue_guard is None:
            return True
        try:
            return continue_guard()
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to verify service ownership while '
                           'quiescing replica launches: '
                           f'{common_utils.format_exception(e)}')
            return False

    launch_request_name = (server_constants.REQUEST_NAME_PREFIX +
                           request_names.RequestName.CLUSTER_LAUNCH.value)
    cluster_names = {info.cluster_name for info in replica_infos}
    # The server-owned backend guard is the topology/capability witness. A
    # controller-child override is deliberately not: every Serve controller
    # child sets it, including legacy remote controllers. Protocol-v2 is
    # central-PostgreSQL-only and forces this path so it cannot silently
    # downgrade to a public API query.
    use_authoritative_request_store = (
        include_terminal_history or
        request_postgres.execution_quiescence_backend_guard_enabled())

    def _discover_launch_requests() -> tuple[dict[str, int], dict[str, int]]:
        if not cluster_names:
            return {}, {}
        # The caller has stopped the launch producer (and generic teardown has
        # already durably terminalized the service). A launch request racing
        # this snapshot is caught by the next cancellation round. The
        # guarded central path asks PostgreSQL for active and terminal-unproved
        # rows across the whole incarnation-scoped cluster-name set in one
        # bounded query; only remote compatibility teardown scans active rows.
        fields = ['request_id', 'name', 'cluster_name']
        if use_authoritative_request_store:
            fields.extend([
                'execution_generation', 'status',
                'execution_quiescence_required',
                'execution_quiesced_generation', 'execution_quiesced_at'
            ])
        if use_authoritative_request_store:
            request_filter = api_requests.RequestTaskFilter(
                cluster_names=sorted(cluster_names),
                include_request_names=[launch_request_name],
                execution_quiescence_candidates_only=True,
                fields=fields,
                sort=True)
            requests = api_requests.get_request_tasks(request_filter)
        else:
            assert not include_terminal_history
            requests = sdk.api_status(all_status=False, fields=fields)

        active: dict[str, int] = {}
        terminal_unproven: dict[str, int] = {}
        for request in requests:
            if (request.name != launch_request_name or
                    request.cluster_name not in cluster_names):
                continue
            if not use_authoritative_request_store:
                active[request.request_id] = 0
                continue
            execution_generation = request.execution_generation
            if (not isinstance(execution_generation, int) or
                    execution_generation < 0):
                raise RuntimeError(
                    'API server did not return an execution generation for '
                    f'launch request {request.request_id}.')
            status = request.status
            if isinstance(status, str):
                status = api_requests.RequestStatus(status)
            if status in api_requests.RequestStatus.finished_status():
                if request.execution_quiescence_required is not True:
                    # Pre-v70 terminal history has no completion-receipt
                    # contract. Protocol v2 cannot create those rows, and
                    # replica creation time excludes reused cluster names.
                    continue
                if (request.execution_quiesced_generation
                        != execution_generation or
                        request.execution_quiesced_at is None):
                    terminal_unproven[request.request_id] = (
                        execution_generation)
                continue
            active[request.request_id] = execution_generation
        return active, terminal_unproven

    def _await_execution_quiescence(
            request_generations: dict[str, int]) -> bool:
        """Wait for exact target generations, not just terminal status."""
        request_ids = set(request_generations)
        deadline = time.monotonic() + _LAUNCH_QUIESCE_TIMEOUT_SECONDS
        while True:
            if not _guard_allows():
                return False
            fields = [
                'request_id', 'name', 'cluster_name', 'status',
                'execution_generation', 'execution_quiescence_required',
                'execution_quiesced_generation', 'execution_quiesced_at'
            ]
            if use_authoritative_request_store:
                requests = api_requests.get_request_tasks(
                    api_requests.RequestTaskFilter(
                        request_ids=sorted(request_ids),
                        fields=fields,
                        sort=True))
            else:
                requests = sdk.api_status(request_ids=sorted(request_ids),
                                          fields=fields,
                                          _exact_request_ids=True,
                                          _use_body=True)
            exact_requests = {
                request.request_id: request
                for request in requests
                if request.request_id in request_ids
            }
            missing = request_ids - exact_requests.keys()
            if missing:
                logger.error('Launch requests disappeared before execution '
                             f'quiescence was proven for {service_name!r}: '
                             f'{sorted(missing)}')
                return False

            waiting: list[str] = []
            for request_id, request in exact_requests.items():
                if (request.name != launch_request_name or
                        request.cluster_name not in cluster_names):
                    logger.error(
                        'Launch request identity changed while quiescing '
                        f'{service_name!r}: {request_id}')
                    return False
                expected_generation = request_generations[request_id]
                if request.execution_generation != expected_generation:
                    logger.error(
                        'Launch request execution generation changed while '
                        f'quiescing {service_name!r}: {request_id} '
                        f'({request.execution_generation} != '
                        f'{expected_generation})')
                    return False
                status = request.status
                if isinstance(status, str):
                    status = api_requests.RequestStatus(status)
                if status not in api_requests.RequestStatus.finished_status():
                    logger.error(
                        'Launch request did not reach a terminal state while '
                        f'quiescing {service_name!r}: {request_id} '
                        f'({request.status})')
                    return False
                if (request.execution_quiescence_required is not True or
                        request.execution_quiesced_generation
                        != expected_generation or
                        request.execution_quiesced_at is None):
                    waiting.append(request_id)

            if not waiting:
                return True
            if time.monotonic() >= deadline:
                logger.error(
                    'Timed out waiting for cancelled launch handlers to '
                    f'quiesce for {service_name!r}: {sorted(waiting)}')
                return False
            time.sleep(_LAUNCH_QUIESCE_POLL_SECONDS)

    try:
        if use_authoritative_request_store:
            # The direct barrier is safe only when the configured store and
            # queue resolve to the exact built-in durable implementations.
            request_postgres.require_builtin_execution_quiescence_backends(
                required=True)
        # The caller has already stopped the producer (and generic service
        # teardown has published SHUTTING_DOWN). Both the scheduler
        # precondition and persisted execution entrypoint reject a launch row
        # that appears after the final empty scan.
        cancel_rounds = 0
        while True:
            if not _guard_allows():
                return False
            active_requests, terminal_unproven = (_discover_launch_requests())

            if active_requests:
                if cancel_rounds >= _LAUNCH_QUIESCE_MAX_CANCEL_ROUNDS:
                    logger.error(
                        'Replica launch requests remained active after '
                        f'cancellation for {service_name!r}: '
                        f'{sorted(active_requests)}')
                    return False
                if use_authoritative_request_store:
                    api_requests.kill_requests_exact(sorted(active_requests),
                                                     user_id=None)
                else:
                    cancel_request_id = sdk.api_cancel(sorted(active_requests),
                                                       all_users=True,
                                                       silent=True)
                    sdk.stream_and_get(cancel_request_id)
                cancel_rounds += 1
                if not use_authoritative_request_store:
                    # Transitional active-only compatibility is restricted to
                    # legacy remote controllers. Every guarded central caller
                    # waits for the exact generation receipt below.
                    continue

            targets = dict(terminal_unproven)
            targets.update(active_requests)
            if not targets:
                return True
            if not _await_execution_quiescence(targets):
                return False
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Failed to quiesce replica launch requests for '
                     f'{service_name!r}: '
                     f'{common_utils.format_exception(e)}')
        return False


def get_existing_replica_cluster_names(
    replica_infos: list['replica_managers.ReplicaInfo'],) -> set[str]:
    """Return one batched snapshot of replica names in the cluster table."""
    cluster_names = list(
        dict.fromkeys(info.cluster_name for info in replica_infos))
    if not cluster_names:
        return set()
    return set(
        global_user_state.get_cluster_status_fields(cluster_names).keys())


def get_orphaned_service_cluster_status_fields(
) -> dict[str, global_user_state.ManagedClusterStatusFields]:
    """Returns managed service clusters without an exact replica owner.

    Only consolidated SkyServe has both inventories in the API server's
    central database. Non-consolidated services keep replica authority on
    their remote controller, so an API-server-side absence is not evidence of
    orphaned ownership there.
    """
    if not is_consolidation_mode():
        return {}
    candidates = global_user_state.get_managed_cluster_status_fields('service')
    if not candidates:
        return {}
    owned_cluster_names = serve_state.get_replica_cluster_names()
    return {
        cluster_name: status_fields
        for cluster_name, status_fields in candidates.items()
        if cluster_name not in owned_cluster_names
    }


def replica_cleanup_requires_terminal_history(
        replica_infos: list['replica_managers.ReplicaInfo']) -> bool:
    """Whether cleanup must quiesce terminal launch-request history.

    Protocol-v2 launch handlers can remain unproved after their request row is
    terminal. Partial v2 authority is treated identically: cleanup cannot
    safely downgrade malformed durable state to the legacy active-only scan.
    """
    # Local to avoid the Serve/request payload import cycle documented below.
    # pylint: disable=import-outside-toplevel
    from sky.serve import reserved_capacity

    # pylint: enable=import-outside-toplevel

    for info in replica_infos:
        try:
            if (reserved_capacity.parse_protocol_v2_cleanup_fence(info)
                    is not None):
                return True
        except exceptions.KubernetesPhysicalClusterIdentityError:
            return True
    return False


def _partition_replica_cleanup_targets(
    replica_infos: list['replica_managers.ReplicaInfo'],
    existing_cluster_names: set[str],
    *,
    live_service_name: str | None = None,
    live_service_hash: str | None = None,
    live_lifecycle_epoch: int | None = None,
    live_controller_owner: tuple[int | None, str | None] | None = None,
) -> tuple[list[tuple['replica_managers.ReplicaInfo', Any]], list[str]]:
    """Separate cleanup targets from rows whose provider absence is unknown.

    Legacy rows retain the historical cluster-table absence behavior. A
    protocol-v2 row whose local cluster record is absent is removable only
    after an exact context/UID-fenced provider read proves that it owns no
    Pods. A live, unreadable, or malformed v2 target remains retained for
    operator repair.
    """
    # Local to avoid payloads -> task -> service_spec -> serve_utils ->
    # reserved_capacity_broker -> request_wire -> payloads during import.
    # pylint: disable=import-outside-toplevel
    from sky.serve import reserved_capacity

    # pylint: enable=import-outside-toplevel

    to_terminate: list[tuple[replica_managers.ReplicaInfo, Any]] = []
    unresolved_cluster_names: list[str] = []
    for info in replica_infos:
        try:
            cleanup_fence = (
                reserved_capacity.parse_protocol_v2_cleanup_fence(info))
        except exceptions.KubernetesPhysicalClusterIdentityError as error:
            logger.error('Refusing name-only cleanup for replica cluster '
                         f'{info.cluster_name!r}: '
                         f'{common_utils.format_exception(error)}')
            unresolved_cluster_names.append(info.cluster_name)
            continue
        if info.cluster_name in existing_cluster_names:
            to_terminate.append((info, cleanup_fence))
        elif cleanup_fence is not None:
            if live_service_name is not None:
                if (live_service_hash is None or live_lifecycle_epoch is None or
                        live_controller_owner is None):
                    raise ValueError(
                        'Live exact cleanup requires the complete service '
                        'incarnation and controller owner.')
                # Persist teardown intent before any unlocked provider read.
                # A crash after the subsequent 404 can then safely retry this
                # exact stored Pod identity without recreating a cluster row.
                if info.status_property.sky_down_status is None:
                    info.status_property.sky_down_status = (
                        common_utils.ProcessStatus.SCHEDULED)
                persisted = serve_state.add_or_update_replica(
                    live_service_name,
                    info.replica_id,
                    info,
                    expected_service_hash=live_service_hash,
                    expected_lifecycle_epoch=live_lifecycle_epoch,
                    expected_controller_owner=live_controller_owner,
                    expected_replica_exists=True)
                if not persisted:
                    logger.error(
                        'Retaining protocol-v2 replica cluster %r because its '
                        'live teardown identity changed before exact Pod probe.',
                        info.cluster_name)
                    unresolved_cluster_names.append(info.cluster_name)
                    continue
                try:
                    exact_absence = (kueue_lane_observer.
                                     project_exact_pod_absence_after_teardown(
                                         live_service_name, info.replica_id,
                                         info.replica_record_id))
                except Exception as error:  # pylint: disable=broad-except
                    logger.error(
                        'Retaining protocol-v2 replica cluster %r because its '
                        'exact admitted Pod absence is unproved: %s.',
                        info.cluster_name, common_utils.format_exception(error))
                    unresolved_cluster_names.append(info.cluster_name)
                    continue
                if exact_absence:
                    logger.info(
                        'Protocol-v2 replica cluster %r exact admitted Pod is '
                        'absent; provider cleanup is complete.',
                        info.cluster_name)
                    continue
            presence = reserved_capacity.probe_physical_replica_presence(
                cleanup_fence, info.cluster_name)
            if presence is reserved_capacity.PhysicalReplicaPresence.ABSENT:
                logger.info('Protocol-v2 replica cluster '
                            f'{info.cluster_name!r} owns no Pod on its fenced '
                            'physical cluster; provider cleanup is complete.')
                continue
            logger.error(
                'Retaining protocol-v2 replica cluster '
                f'{info.cluster_name!r}: its SkyPilot cluster record is '
                'absent and fenced provider presence is '
                f'{presence.value.lower()}.')
            unresolved_cluster_names.append(info.cluster_name)
    return to_terminate, unresolved_cluster_names


def _terminate_failed_services(service_name: str,
                               expected_service_hash: str | None,
                               service_status: serve_state.ServiceStatus | None,
                               pool: bool = False) -> _PurgeResult:
    """Terminate service in failed status.

    Failed-status services may still have a parent or recovering controller,
    so a file signal alone is not authoritative. Claim durable SHUTTING_DOWN,
    wait for an exact-owner child-teardown acknowledgement (or atomically claim
    a proven orphan), then terminate any remaining replicas and conditionally
    remove the exact service incarnation. Clusters that fail to terminate are
    reported as a potential resource leak.

    Returns:
        A structured completion result and optional failure message.
    """
    if not expected_service_hash:
        return _PurgeResult(
            False,
            _purge_ownership_failure(service_name,
                                     'missing durable service hash'))
    # An unresolved bound association keeps its admission-time lifecycle
    # epoch as immutable provenance.  Advancing the service epoch before the
    # teardown transaction would therefore trip the deferred consistency
    # guard before the reducer can publish SHUTTING_DOWN and settle the
    # association.  Retain the current epoch while the same name-scoped
    # advisory lock is held.  SHUTTING_DOWN is the explicit launch fence, and
    # the existing hash/owner/epoch CASes plus the final conditional delete
    # continue to fence same-name replacement.
    lifecycle_lock = get_service_lifecycle_lock(service_name,
                                                advance_epoch=False)
    # Kept in the outer helper's compatibility signature for existing callers;
    # cleanup behavior is now fully determined by durable DB state.
    del service_status
    with lifecycle_lock:
        message = _terminate_failed_services_locked(service_name,
                                                    expected_service_hash, pool,
                                                    lifecycle_lock)
    return _PurgeResult(message is None, message)


def _terminate_failed_services_locked(
        service_name: str, expected_service_hash: str, pool: bool,
        lifecycle_lock: ServiceLifecycleLock) -> str | None:
    """Locked implementation of failed-service purge."""

    def _still_owns() -> bool:
        return (lifecycle_lock_is_valid(lifecycle_lock) and
                serve_state.service_owner_matches(service_name,
                                                  expected_service_hash))

    lifecycle_epoch = get_service_lifecycle_epoch(lifecycle_lock)

    if not _still_owns():
        return _purge_ownership_failure(service_name,
                                        'ownership lost before cleanup')
    owner = serve_state.get_service_controller_owner(service_name,
                                                     include_lb_state=True)
    if owner is None or owner.get('hash') != expected_service_hash:
        return _purge_ownership_failure(service_name,
                                        'owner disappeared before teardown')
    expected_controller_owner = (owner.get('controller_pid'),
                                 owner.get('controller_ip'))
    try:
        marked_for_teardown, bound_authority = (
            _begin_service_teardown_if_owner(service_name,
                                             expected_service_hash,
                                             expected_controller_owner,
                                             lifecycle_epoch))
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            f'Failed to establish ordinary-launch teardown authority for '
            f'{service_name!r}; retaining purge state for retry: '
            f'{common_utils.format_exception(e)}')
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because its ordinary-launch teardown '
                'authority could not be established; durable cleanup '
                f'inventory was retained for retry.{colorama.Style.RESET_ALL}')
    if not marked_for_teardown:
        return _purge_ownership_failure(
            service_name, 'could not claim durable teardown state')

    # Generic API cancellation intentionally excludes requests associated with
    # the closed ordinary-launch state machine.  Resolve those requests under
    # the pre-teardown controller authority before any orphan claim can rotate
    # that authority, and before generic quiescence or provider deletion.
    if bound_authority is not None:
        try:
            bound_replica_infos = serve_state.get_replica_infos(service_name)
            # Local to break service -> serve_utils at module import time.
            # pylint: disable=import-outside-toplevel,protected-access
            from sky.serve import service as service_lib

            service_lib._settle_bound_ordinary_launches_for_teardown(
                bound_authority, bound_replica_infos)
            # pylint: enable=import-outside-toplevel,protected-access
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to settle exact bound ordinary launches for failed '
                f'service {service_name!r}; retaining purge state for retry: '
                f'{common_utils.format_exception(e)}')
            return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                    'could not be purged because its exact bound replica '
                    'launches could not be cancelled and settled; durable '
                    'cleanup inventory was retained for retry.'
                    f'{colorama.Style.RESET_ALL}')

    # A CONTROLLER_FAILED row may still have a live parent/child (for example,
    # a transient LB failure). The parent polls durable SHUTTING_DOWN at the
    # top of every tick, kills and joins its child, then clears controller_port
    # before waiting for this same lifecycle lock. Do not down name-reused
    # replica clusters until that acknowledgement arrives.
    resource_scope = owner.get('resource_scope')
    high_availability = bool(owner.get('lb_ha_enabled'))
    if owner.get('controller_port') != constants.CONTROLLER_TEARDOWN_ACK_PORT:
        recovery_script = serve_state.get_ha_recovery_script(service_name)
        unrecoverable = (recovery_script is not None and
                         serve_state.get_latest_committed_version(service_name)
                         is None)
        if (bound_authority is not None and
            (recovery_script is None or unrecoverable)):
            # The old PID/IP cannot be rewritten in place for a bound service:
            # owner rotation must also rotate the controller incarnation and
            # transfer every unsettled association.  Exact settlement above
            # makes that transfer immediately available; try rather than wait
            # so any unexpected authority holder leaves cleanup retryable.
            try:
                # Local to keep the PostgreSQL-only protocol off legacy CLI
                # import paths.
                # pylint: disable=import-outside-toplevel
                from sky.serve import ordinary_launch_binding

                # pylint: enable=import-outside-toplevel
                purge_owner = (os.getpid(), os.environ.get('POD_IP'))
                claimed_authority = (
                    ordinary_launch_binding.claim_controller_incarnation(
                        service_name,
                        expected_service_hash,
                        expected_controller_owner,
                        uuid.uuid4(),
                        new_parent_owner=purge_owner,
                        expected_lifecycle_epoch=lifecycle_epoch,
                        expected_status=(
                            serve_state.ServiceStatus.SHUTTING_DOWN),
                        wait_for_authority=False))
                if claimed_authority is None:
                    raise RuntimeError(
                        'Bound orphan claim returned no controller authority.')
                bound_authority = claimed_authority
                owner = dict(owner)
                owner['controller_pid'], owner['controller_ip'] = purge_owner
            except Exception as e:  # pylint: disable=broad-except
                logger.error(
                    f'Failed to claim exact bound orphan teardown for '
                    f'{service_name!r}; retaining cleanup state for retry: '
                    f'{common_utils.format_exception(e)}')
                return (f'{colorama.Fore.YELLOW}failed service '
                        f'{service_name!r} could not be purged because its '
                        'bound orphan authority could not be claimed; durable '
                        'cleanup inventory was retained for retry.'
                        f'{colorama.Style.RESET_ALL}')
        if recovery_script is None:
            # Legacy orphan/FAILED_CLEANUP rows may have no parent left to
            # write the new acknowledgement. Absence of the recovery script
            # is durable proof that no controller can be (re)spawned.
            claimed = serve_state.claim_orphaned_service_teardown(
                service_name,
                expected_service_hash,
                owner.get('controller_pid'),
                owner.get('controller_ip'),
                os.getpid(),
                os.environ.get('POD_IP'),
                expected_lifecycle_epoch=lifecycle_epoch)
        elif unrecoverable:
            # Old partial-registration rows can retain a recovery script but
            # no committed yaml. Such a script can never boot a controller;
            # atomically consume it while claiming teardown so purge does not
            # wait forever for an impossible acknowledgement.
            claimed = serve_state.claim_unrecoverable_service_teardown(
                service_name,
                expected_service_hash,
                owner.get('controller_pid'),
                owner.get('controller_ip'),
                os.getpid(),
                os.environ.get('POD_IP'),
                expected_lifecycle_epoch=lifecycle_epoch)
        else:
            claimed = None
        if claimed is False:
            return _purge_ownership_failure(
                service_name, 'orphan teardown claim lost ownership')

    owner_ack_deadline = time.monotonic() + 10
    while True:
        owner = serve_state.get_service_controller_owner(service_name)
        if owner is None or owner.get('hash') != expected_service_hash:
            return _purge_ownership_failure(
                service_name, 'ownership changed while awaiting controller')
        if (owner.get('controller_port') ==
                constants.CONTROLLER_TEARDOWN_ACK_PORT):
            break
        remaining = owner_ack_deadline - time.monotonic()
        if remaining <= 0:
            return (f'{colorama.Fore.YELLOW}failed service '
                    f'{service_name!r} could not be purged because its '
                    'controller has not yet acknowledged durable teardown; '
                    'cleanup remains scheduled and can be retried.'
                    f'{colorama.Style.RESET_ALL}')
        time.sleep(min(0.2, remaining))

    if not _still_owns():
        return _purge_ownership_failure(
            service_name, 'lifecycle lock or ownership lost after controller '
            'acknowledgement')

    replica_infos = serve_state.get_replica_infos(service_name)
    provider_present_cleanup_contexts: dict[tuple[int, str], Any] = {}
    provider_reconciliation_failures: dict[tuple[int, str], str] = {}
    if bound_authority is not None:
        try:
            # A live parent may rotate the controller incarnation between the
            # first teardown transition and its acknowledgement. Re-read the
            # exact current authority under the same lifecycle/name lock, then
            # revalidate every retained provider-present marker against it.
            marked_for_teardown, current_authority = (
                _begin_service_teardown_if_owner(
                    service_name, expected_service_hash,
                    (owner.get('controller_pid'), owner.get('controller_ip')),
                    lifecycle_epoch))
            if not marked_for_teardown or current_authority is None:
                raise RuntimeError(
                    'Bound teardown authority disappeared after controller '
                    'acknowledgement.')
            bound_authority = current_authority
            # Local to break service -> serve_utils at module import time.
            # pylint: disable=import-outside-toplevel,protected-access
            from sky.serve import service as service_lib

            settlement = (
                service_lib._settle_bound_ordinary_launches_for_teardown(
                    bound_authority, replica_infos))
            provider_present_cleanup_contexts = (
                settlement.provider_present_cleanup_contexts)
            provider_reconciliation_failures = (
                settlement.provider_reconciliation_failures)
            # pylint: enable=import-outside-toplevel,protected-access
            # Settlement may atomically persist the provider-present cleanup
            # marker on a separately locked ReplicaInfo instance.  Refresh
            # before matching that marker to the returned exact context.
            replica_infos = serve_state.get_replica_infos(service_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to refresh exact bound cleanup authority for '
                f'service {service_name!r}; retaining purge state for retry: '
                f'{common_utils.format_exception(e)}')
            return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                    'could not be purged because its exact provider cleanup '
                    'authority could not be refreshed; durable cleanup '
                    'inventory was retained for retry.'
                    f'{colorama.Style.RESET_ALL}')
    if not quiesce_service_replica_launch_requests(
            service_name,
            replica_infos,
            continue_guard=_still_owns,
            include_terminal_history=(
                replica_cleanup_requires_terminal_history(replica_infos))):
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because its replica launch requests '
                'could not be quiesced; durable cleanup inventory was '
                f'retained for retry.{colorama.Style.RESET_ALL}')

    # Fence the public data plane before *any* replica teardown.  The LB keeps
    # its last coherent routing view when controller sync stops, so reversing
    # this order accepts requests for clusters already being destroyed.  A
    # failed delete retains the exact row/name and aborts all cloud teardown.
    from sky.serve import lb_k8s  # pylint: disable=import-outside-toplevel
    if not pool:
        try:
            api_deployment_uid = lb_k8s.get_api_deployment_owner_uid(
                require_runtime=True)
            if resource_scope is None:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=expected_service_hash,
                    require_runtime=True,
                    expected_api_deployment_uid=api_deployment_uid,
                    high_availability=high_availability)
            else:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=expected_service_hash,
                    resource_scope=resource_scope,
                    require_runtime=True,
                    expected_api_deployment_uid=api_deployment_uid,
                    high_availability=high_availability)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to delete external LB objects for failed service '
                f'{service_name!r}; retaining purge state for retry: '
                f'{common_utils.format_exception(e)}')
            return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                    'could not be purged because its external load balancer '
                    'could not be deleted; retry purge after fixing '
                    f'Kubernetes access.{colorama.Style.RESET_ALL}')

    if not _still_owns():
        return _purge_ownership_failure(
            service_name, 'ownership lost after load balancer cleanup')

    remaining_replica_clusters: list[str] = []
    # The controller is dead (CONTROLLER_FAILED / FAILED_CLEANUP / zombie
    # SHUTTING_DOWN), so no down thread will ever run for these replicas:
    # terminate their clusters here, BEFORE dropping the DB rows. Deleting
    # the rows first (the old behavior) permanently orphaned any cluster
    # that still existed -- nothing referenced it anymore, so it kept
    # billing until manually downed.
    try:
        existing_cluster_names = get_existing_replica_cluster_names(
            replica_infos)
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f'Failed to prove replica cluster inventory for failed '
                     f'service {service_name!r}: '
                     f'{common_utils.format_exception(e)}')
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because its replica cluster inventory '
                'could not be verified; durable cleanup inventory was '
                f'retained for retry.{colorama.Style.RESET_ALL}')
    if not _still_owns():
        return _purge_ownership_failure(
            service_name, 'ownership lost after cluster inventory snapshot')
    # Local to break service -> serve_utils at module import time.
    # pylint: disable=import-outside-toplevel,protected-access
    from sky.serve import service as service_lib

    try:
        preparation = service_lib._prepare_provider_present_cleanup(
            service_name, bound_authority, replica_infos,
            existing_cluster_names, provider_present_cleanup_contexts)
    except Exception as error:  # pylint: disable=broad-except
        logger.error(
            'Retaining failed service %r because its exact provider cleanup '
            'inventory could not be prepared (%s).', service_name,
            common_utils.format_exception(error))
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because its exact provider cleanup '
                'inventory could not be prepared; durable cleanup inventory '
                f'was retained for retry.{colorama.Style.RESET_ALL}')
    # pylint: enable=import-outside-toplevel,protected-access
    provider_present_cleanup_contexts = preparation.contexts
    replica_keys = {
        (info.replica_id, info.replica_record_id) for info in replica_infos
    }
    extra_failure_keys = provider_reconciliation_failures.keys() - replica_keys
    overlapping_failure_keys = (provider_reconciliation_failures.keys() &
                                provider_present_cleanup_contexts.keys())
    if extra_failure_keys or overlapping_failure_keys:
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because exact provider reconciliation '
                'lost its replica inventory; durable cleanup inventory was '
                f'retained for retry.{colorama.Style.RESET_ALL}')
    cleanup_failures = dict(preparation.failures)
    cleanup_failures.update(provider_reconciliation_failures)
    for cleanup_key, reason in cleanup_failures.items():
        logger.warning(
            'Retaining exact failed-service cleanup target %r for %r: %s.',
            cleanup_key, service_name, reason)
    remaining_replica_clusters.extend(
        info.cluster_name
        for info in replica_infos
        if (info.replica_id, info.replica_record_id) in cleanup_failures)
    skipped_cleanup_keys = (set(cleanup_failures) |
                            set(preparation.projected_absence_keys))
    partition_infos = [
        info for info in replica_infos
        if (info.replica_id, info.replica_record_id) not in skipped_cleanup_keys
    ]
    # This remains the failed-service purge submission owner.  The retired
    # action-authority proposal does not replace it.
    to_terminate, unresolved_cluster_names = (
        _partition_replica_cleanup_targets(
            partition_infos,
            existing_cluster_names,
            live_service_name=service_name,
            live_service_hash=(expected_service_hash),
            live_lifecycle_epoch=lifecycle_epoch,
            live_controller_owner=(owner.get('controller_pid'),
                                   owner.get('controller_ip'))))
    cleanup_targets: list[tuple[Any, Any, Any | None]] = []
    for info, cleanup_fence in to_terminate:
        try:
            # pylint: disable=protected-access
            cleanup_context = service_lib._provider_present_cleanup_context(
                info, bound_authority, provider_present_cleanup_contexts)
            # pylint: enable=protected-access
        except Exception as error:  # pylint: disable=broad-except
            logger.error(
                'Retaining replica cluster %r because its provider-present '
                'cleanup marker lost exact bound association authority (%s).',
                info.cluster_name, common_utils.format_exception(error))
            unresolved_cluster_names.append(info.cluster_name)
            continue
        cleanup_targets.append((info, cleanup_fence, cleanup_context))
    remaining_replica_clusters.extend(unresolved_cluster_names)
    if cleanup_targets:
        try:
            teardown_identities = (
                serve_state.get_replica_resource_action_identities(
                    service_name,
                    [info.replica_id for info, _, _ in cleanup_targets]))
            if set(teardown_identities) != {
                    info.replica_id for info, _, _ in cleanup_targets
            }:
                raise RuntimeError(
                    'Replica inventory changed while snapshotting teardown '
                    'identities.')
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to prove replica teardown identities for service '
                f'{service_name!r}: {common_utils.format_exception(e)}')
            return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                    'could not be purged because its durable replica teardown '
                    'identities could not be verified; cleanup inventory was '
                    f'retained for retry.{colorama.Style.RESET_ALL}')
        # Imported here to break the circular dependency: replica_managers
        # imports serve_utils at module load.
        # pylint: disable=import-outside-toplevel
        from sky.serve import replica_managers

        # DistributedLock's PostgreSQL liveness probe uses the lock-owning
        # connection.  Multiple replica workers must not use that connection
        # concurrently: psycopg connections/cursors are not a thread-safe
        # ownership oracle.  Serialize guard probes while keeping the actual
        # cluster termination requests parallel.
        ownership_guard_lock = threading.Lock()

        def _worker_still_owns() -> bool:
            with ownership_guard_lock:
                return _still_owns()

        cleanup_target_by_identity = {
            (info.replica_id, info.replica_record_id):
                (cleanup_fence, cleanup_context)
            for info, cleanup_fence, cleanup_context in cleanup_targets
        }

        def _terminate_replica_cluster(
                info: 'replica_managers.ReplicaInfo') -> None:
            # Reuse the canonical direct core.down path with retries.
            cleanup_fence, cleanup_context = cleanup_target_by_identity[(
                info.replica_id, info.replica_record_id)]
            identity = teardown_identities[info.replica_id]
            terminate_kwargs: dict[str, Any] = {
                'continue_guard': _worker_still_owns,
                'expected_cluster_record_uuid':
                    (str(identity.sky_cluster_record_uuid)
                     if identity is not None else None),
            }
            if cleanup_fence is not None:
                terminate_kwargs['cleanup_fence'] = cleanup_fence
            # pylint: disable=protected-access
            service_lib._terminate_replica_cluster_for_service_cleanup(
                service_name, info, cleanup_context, bound_authority,
                info.cluster_name, **terminate_kwargs)
            # pylint: enable=protected-access

        cleanup_owner = (owner.get('controller_pid'),
                         owner.get('controller_ip'))

        def _persist_cleanup(info: 'replica_managers.ReplicaInfo') -> None:
            persisted = serve_state.add_or_update_replica(
                service_name,
                info.replica_id,
                info,
                expected_service_hash=expected_service_hash,
                expected_lifecycle_epoch=lifecycle_epoch,
                expected_controller_owner=cleanup_owner,
                expected_replica_exists=True,
                guard_launch_exclusion=(
                    serve_state.replica_info_has_binding_excluded_profile(info)
                ))
            if not persisted:
                raise RuntimeError('Failed service cleanup lost exact replica '
                                   f'{info.replica_id} ownership.')

        def _cleanup_succeeded(info: 'replica_managers.ReplicaInfo') -> None:
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            _persist_cleanup(info)

        def _cleanup_failed(info: 'replica_managers.ReplicaInfo',
                            reason: str | None) -> None:
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.FAILED)
            _persist_cleanup(info)
            remaining_replica_clusters.append(info.cluster_name)
            suffix = '' if reason is None else f': {reason}'
            logger.error(
                'Failed to terminate replica cluster %s of failed '
                'service %r%s', info.cluster_name, service_name, suffix)

        def _reserve_failed_cleanup(
            infos: list['replica_managers.ReplicaInfo'],
            termination_limit: int,
        ) -> Mapping[int, 'replica_managers.ReplicaInfo']:
            return serve_state.reserve_replica_teardowns_running_if_capacity(
                service_name,
                [(info.replica_id, info.replica_record_id) for info in infos],
                termination_limit=termination_limit,
                expected_service_hash=expected_service_hash,
                expected_lifecycle_epoch=lifecycle_epoch,
                expected_controller_owner=cleanup_owner)

        def _restore_failed_cleanup(
            info: 'replica_managers.ReplicaInfo',
        ) -> 'replica_managers.ReplicaInfo | None':
            return (
                serve_state.restore_never_started_replica_teardown_to_scheduled(
                    service_name,
                    info.replica_id,
                    info.replica_record_id,
                    expected_service_hash=expected_service_hash,
                    expected_lifecycle_epoch=lifecycle_epoch,
                    expected_controller_owner=cleanup_owner))

        cleanup_work: list[Any] = []
        for target in cleanup_targets:
            info = target[0]
            if (info.status_property.sky_down_status
                    != common_utils.ProcessStatus.RUNNING):
                info.status_property.sky_down_status = (
                    common_utils.ProcessStatus.SCHEDULED)
            _persist_cleanup(info)
            cleanup_work.append(info)
        try:
            run_bounded_serve_teardown_threads(
                cleanup_work,
                make_worker=lambda info: thread_utils.SafeThread(
                    target=_terminate_replica_cluster, args=(info,)),
                pool=pool,
                reserve_running=_reserve_failed_cleanup,
                restore_never_started=_restore_failed_cleanup,
                handle_success=_cleanup_succeeded,
                handle_failure=_cleanup_failed,
                continue_guard=_still_owns,
                max_concurrent_per_service=(
                    replica_managers.MAX_CONCURRENT_DOWNS_PER_SERVICE))
        except Exception as error:  # pylint: disable=broad-except
            logger.error(
                'Failed-service replica teardown admission failed '
                'closed for %r: %s', service_name,
                common_utils.format_exception(error))
            return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                    'could not be purged because bounded provider cleanup '
                    'admission failed; durable cleanup inventory was retained '
                    f'for retry.{colorama.Style.RESET_ALL}')

    if not _still_owns():
        return _purge_ownership_failure(service_name,
                                        'ownership lost after replica cleanup')

    if remaining_replica_clusters:
        # Keep every durable row/file and the name even though new replica
        # names are incarnation-scoped.  This preserves an authoritative,
        # retryable inventory for any billable cluster that survived down.
        if not serve_state.set_service_status_and_active_versions_if_owner(
                service_name,
                expected_service_hash,
                owner.get('controller_pid'),
                owner.get('controller_ip'),
                serve_state.ServiceStatus.FAILED_CLEANUP,
                expected_status=serve_state.ServiceStatus.SHUTTING_DOWN,
                expected_lifecycle_epoch=lifecycle_epoch):
            return _purge_ownership_failure(
                service_name, 'ownership lost while retaining failed cleanup')
        remaining_identity = ', '.join(
            repr(name) for name in remaining_replica_clusters)
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because some replica clusters could not '
                'be terminated. The service name and cleanup metadata remain '
                'reserved; retry purge after checking: '
                f'{remaining_identity}{colorama.Style.RESET_ALL}')

    # Version rows may already have been retired while this service was live;
    # consume the separate durable generation manifests only after every
    # replica is confirmed gone and before the final DB removal.
    if not service_lib.cleanup_storage_intents(service_name, resource_scope,
                                               _still_owns):
        return (f'{colorama.Fore.YELLOW}failed service {service_name!r} '
                'could not be purged because scoped storage cleanup failed; '
                'durable cleanup inventory was retained for retry.'
                f'{colorama.Style.RESET_ALL}')

    service_dir = os.path.expanduser(
        generate_remote_service_dir_name(service_name, resource_scope))
    # A legacy name-only directory has no injective owner identity. Keep it;
    # new scoped directories are safe to delete after the final DB CAS.
    remove_directory = resource_scope is not None
    # Claim the exact incarnation + lifecycle epoch first inside one
    # transaction, then remove every name-keyed child row.  All later
    # filesystem work targets the old incarnation's disjoint path.
    removed = serve_state.remove_service_completely(
        service_name,
        expected_service_hash,
        expected_lifecycle_epoch=lifecycle_epoch)
    if not removed:
        return _purge_ownership_failure(
            service_name, 'final database compare-and-delete '
            'lost ownership')
    if remove_directory:
        remove_service_directory(service_dir)
    return None


def _terminate_orphaned_service_children(service_name: str,
                                         expected_pool: bool) -> _PurgeResult:
    """Purge child-only replica/storage inventory under the name fence."""
    message = _terminate_orphaned_service_children_impl(service_name,
                                                        expected_pool)
    return _PurgeResult(message is None, message)


def _terminate_orphaned_service_children_impl(
        service_name: str, expected_pool: bool) -> str | None:
    """Implementation returning a diagnostic for an incomplete purge."""
    lifecycle_lock = get_service_lifecycle_lock(service_name)
    with lifecycle_lock:
        lifecycle_epoch = get_service_lifecycle_epoch(lifecycle_lock)

        child_pool = serve_state.get_orphaned_service_child_mode(service_name)
        if child_pool is None:
            return (f'{colorama.Fore.YELLOW}orphaned name {service_name!r} '
                    'could not be purged because its service/pool mode is '
                    'ambiguous; durable child inventory was retained for '
                    f'manual inspection.{colorama.Style.RESET_ALL}')
        if child_pool != expected_pool:
            expected_noun = 'pool' if expected_pool else 'service'
            actual_noun = 'pool' if child_pool else 'service'
            return (f'{colorama.Fore.YELLOW}orphaned name {service_name!r} '
                    f'belongs to a {actual_noun}, not a {expected_noun}; no '
                    f'children were changed.{colorama.Style.RESET_ALL}')

        def _still_orphaned() -> bool:
            return (lifecycle_lock_is_valid(lifecycle_lock) and
                    serve_state.get_service_mode_and_hash(service_name) is None
                    and
                    serve_state.get_orphaned_service_child_mode(service_name)
                    == expected_pool)

        if not _still_orphaned():
            return _purge_ownership_failure(
                service_name, 'a service row appeared before orphan cleanup')

        replica_infos = serve_state.get_replica_infos(service_name)
        if not quiesce_service_replica_launch_requests(
                service_name,
                replica_infos,
                continue_guard=_still_orphaned,
                include_terminal_history=(
                    replica_cleanup_requires_terminal_history(replica_infos))):
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because its replica '
                    'launch requests could not be quiesced; durable child '
                    f'inventory was retained.{colorama.Style.RESET_ALL}')

        # Imported here to break replica_managers -> serve_utils and
        # service -> replica_managers dependency cycles.
        # pylint: disable=import-outside-toplevel
        from sky.serve import lb_k8s
        from sky.serve import replica_managers
        from sky.serve import service as service_lib

        intents = serve_state.get_ephemeral_storage_cleanup_intents(
            service_name)
        resource_scopes = sorted({
            intent['resource_scope']
            for intent in intents
            if isinstance(intent.get('resource_scope'), str)
        })
        api_deployment_uid: str | None = None
        if resource_scopes and not expected_pool:
            try:
                api_deployment_uid = lb_k8s.get_api_deployment_owner_uid(
                    require_runtime=True)
            except Exception as e:  # pylint: disable=broad-except
                return (f'{colorama.Fore.YELLOW}orphaned service '
                        f'{service_name!r} could not be purged because the '
                        'current API Deployment owner could not be verified: '
                        f'{common_utils.format_exception(e)}.'
                        f'{colorama.Style.RESET_ALL}')
        for resource_scope in resource_scopes:
            if expected_pool:
                break
            if not _still_orphaned():
                return _purge_ownership_failure(
                    service_name, 'ownership lost before orphan LB cleanup')
            try:
                lb_k8s.delete_lb_objects(
                    service_name,
                    expected_service_hash=resource_scope,
                    resource_scope=resource_scope,
                    require_runtime=True,
                    expected_api_deployment_uid=api_deployment_uid,
                    high_availability=True)
            except Exception as e:  # pylint: disable=broad-except
                return (f'{colorama.Fore.YELLOW}orphaned service '
                        f'{service_name!r} could not be purged because scoped '
                        f'load balancer cleanup failed: '
                        f'{common_utils.format_exception(e)}.'
                        f'{colorama.Style.RESET_ALL}')

        try:
            existing_cluster_names = get_existing_replica_cluster_names(
                replica_infos)
        except Exception as e:  # pylint: disable=broad-except
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because its '
                    'replica cluster inventory could not be verified: '
                    f'{common_utils.format_exception(e)}.'
                    f'{colorama.Style.RESET_ALL}')
        if not _still_orphaned():
            return _purge_ownership_failure(
                service_name,
                'ownership lost after orphan cluster inventory snapshot')
        # This remains the orphan purge submission owner.  The retired
        # action-authority proposal does not replace it.
        to_terminate, unresolved_cluster_names = (
            _partition_replica_cleanup_targets(replica_infos,
                                               existing_cluster_names))
        try:
            teardown_identities = (
                serve_state.get_replica_resource_action_identities(
                    service_name,
                    [info.replica_id for info, _ in to_terminate]))
            if set(teardown_identities) != {
                    info.replica_id for info, _ in to_terminate
            }:
                raise RuntimeError(
                    'Replica inventory changed while snapshotting teardown '
                    'identities.')
        except Exception as e:  # pylint: disable=broad-except
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because durable '
                    'replica teardown identities could not be verified: '
                    f'{common_utils.format_exception(e)}.'
                    f'{colorama.Style.RESET_ALL}')
        termination_failures = list(unresolved_cluster_names)

        def _persist_orphan_cleanup(
                info: 'replica_managers.ReplicaInfo') -> None:
            persisted = serve_state.add_or_update_replica(
                service_name,
                info.replica_id,
                info,
                expected_lifecycle_epoch=lifecycle_epoch,
                expected_replica_exists=True,
                guard_launch_exclusion=(
                    serve_state.replica_info_has_binding_excluded_profile(info)
                ))
            if not persisted:
                raise RuntimeError('Orphan cleanup lost exact replica '
                                   f'{info.replica_id} ownership.')

        def _terminate_orphan(info: 'replica_managers.ReplicaInfo',
                              cleanup_fence: Any) -> None:
            identity = teardown_identities[info.replica_id]
            terminate_kwargs: dict[str, Any] = {
                'continue_guard': _still_orphaned,
                'expected_cluster_record_uuid':
                    (str(identity.sky_cluster_record_uuid)
                     if identity is not None else None),
            }
            if cleanup_fence is not None:
                terminate_kwargs['cleanup_fence'] = cleanup_fence
            replica_managers.terminate_cluster(info.cluster_name,
                                               **terminate_kwargs)

        def _orphan_cleanup_succeeded(
                info: 'replica_managers.ReplicaInfo') -> None:
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            _persist_orphan_cleanup(info)

        def _orphan_cleanup_failed(info: 'replica_managers.ReplicaInfo',
                                   reason: str | None) -> None:
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.FAILED)
            _persist_orphan_cleanup(info)
            termination_failures.append(info.cluster_name)
            suffix = '' if reason is None else f': {reason}'
            logger.error('Failed to terminate orphan replica cluster %r%s',
                         info.cluster_name, suffix)

        def _reserve_orphan_cleanup(
            infos: list['replica_managers.ReplicaInfo'],
            termination_limit: int,
        ) -> Mapping[int, 'replica_managers.ReplicaInfo']:
            return serve_state.reserve_replica_teardowns_running_if_capacity(
                service_name,
                [(info.replica_id, info.replica_record_id) for info in infos],
                termination_limit=termination_limit,
                expected_lifecycle_epoch=lifecycle_epoch)

        def _restore_orphan_cleanup(
            info: 'replica_managers.ReplicaInfo',
        ) -> 'replica_managers.ReplicaInfo | None':
            return (
                serve_state.restore_never_started_replica_teardown_to_scheduled(
                    service_name,
                    info.replica_id,
                    info.replica_record_id,
                    expected_lifecycle_epoch=lifecycle_epoch))

        cleanup_fence_by_identity: dict[tuple[int, str], Any] = {}
        orphan_cleanup_work: list[Any] = []
        for info, cleanup_fence in to_terminate:
            if (info.status_property.sky_down_status
                    != common_utils.ProcessStatus.RUNNING):
                info.status_property.sky_down_status = (
                    common_utils.ProcessStatus.SCHEDULED)
            _persist_orphan_cleanup(info)
            cleanup_fence_by_identity[(info.replica_id,
                                       info.replica_record_id)] = cleanup_fence
            orphan_cleanup_work.append(info)
        try:
            run_bounded_serve_teardown_threads(
                orphan_cleanup_work,
                make_worker=lambda info: thread_utils.SafeThread(
                    target=_terminate_orphan,
                    args=(info, cleanup_fence_by_identity[
                        (info.replica_id, info.replica_record_id)])),
                pool=expected_pool,
                reserve_running=_reserve_orphan_cleanup,
                restore_never_started=_restore_orphan_cleanup,
                handle_success=_orphan_cleanup_succeeded,
                handle_failure=_orphan_cleanup_failed,
                continue_guard=_still_orphaned,
                max_concurrent_per_service=(
                    replica_managers.MAX_CONCURRENT_DOWNS_PER_SERVICE))
        except Exception as error:  # pylint: disable=broad-except
            logger.error(
                'Orphan replica teardown admission failed closed for '
                '%r: %s', service_name, common_utils.format_exception(error))
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because bounded '
                    'provider cleanup admission failed; durable child '
                    f'inventory was retained.{colorama.Style.RESET_ALL}')
        if termination_failures:
            return (f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because replica '
                    'cluster termination failed; retry after checking: '
                    f'{", ".join(sorted(termination_failures))}.'
                    f'{colorama.Style.RESET_ALL}')

        for resource_scope in resource_scopes:
            if not service_lib.cleanup_storage_intents(
                    service_name, resource_scope, _still_orphaned):
                return (
                    f'{colorama.Fore.YELLOW}orphaned service '
                    f'{service_name!r} could not be purged because scoped '
                    'storage cleanup failed; durable inventory was retained.'
                    f'{colorama.Style.RESET_ALL}')

        if not serve_state.remove_orphaned_service_children(
                service_name, lifecycle_epoch):
            return _purge_ownership_failure(
                service_name, 'ownership lost during orphan metadata removal')
    return None


def terminate_services(service_names: list[str] | None, purge: bool,
                       pool: bool) -> str:
    if not pool and maintenance.is_controller_hold_active():
        raise RuntimeError(
            'SkyServe termination and purge are disabled while the server '
            'controller hold is active.')
    noun = 'pool' if pool else 'service'
    capnoun = noun.capitalize()
    requested_service_names = service_names
    service_names = serve_state.get_glob_service_names(service_names, pool=pool)
    if purge:
        service_names = sorted(
            set(service_names) | set(
                serve_state.get_orphaned_service_child_names(
                    requested_service_names)))
    terminated_service_names: list[str] = []
    messages: list[str] = []

    for service_name in service_names:
        service_status = _get_service_status(service_name,
                                             pool=pool,
                                             with_replica_info=False,
                                             with_yaml=False,
                                             status_snapshot_only=True)
        if service_status is None:
            # `_get_service_status` returns None for two distinct cases: a
            # healthy service of the *other* mode (its `pool` flag != the
            # requested `pool`), and a `services` row with no `version_specs`
            # row -- an orphan stranded by an interrupted first-run
            # registration, invisible to the latest-version inner join.
            # `add_service` now writes both rows atomically, but a row
            # stranded before that fix can still exist, and no normal path
            # can recover or remove it (HA recovery and plain `down` both
            # skip a None status). With --purge, clean such an orphan up
            # directly -- but only when the raw row belongs to the requested
            # mode, so a serve `down --purge` never removes a jobs-pool's row
            # (or vice versa).
            if purge:
                raw_identity = serve_state.get_service_mode_and_hash(
                    service_name)
                if raw_identity is not None and raw_identity[0] == pool:
                    result = _terminate_failed_services(service_name,
                                                        raw_identity[1],
                                                        None,
                                                        pool=pool)
                    if result.message is not None:
                        messages.append(result.message)
                    if result.completed:
                        terminated_service_names.append(f'{service_name!r}')
                elif raw_identity is None:
                    result = _terminate_orphaned_service_children(
                        service_name, pool)
                    if result.message is not None:
                        messages.append(result.message)
                    if result.completed:
                        terminated_service_names.append(f'{service_name!r}')
            continue
        if (service_status is not None and service_status['status']
                == serve_state.ServiceStatus.SHUTTING_DOWN):
            if purge:
                # Resume exact-owner cleanup for a zombie or a prior
                # fail-closed purge attempt. The first purge durably CASes the
                # row to SHUTTING_DOWN before touching replicas/LB/files; any
                # later failure deliberately keeps that row retryable here.
                result = _terminate_failed_services(
                    service_name,
                    service_status.get('hash'),
                    serve_state.ServiceStatus.SHUTTING_DOWN,
                    pool=pool)
                if result.message is not None:
                    messages.append(result.message)
                if result.completed:
                    terminated_service_names.append(service_name)
            # Without --purge, treat as already scheduled to terminate.
            continue
        if pool:
            nonterminal_job_ids = (
                managed_job_state.get_nonterminal_job_ids_by_pool(service_name))
            if nonterminal_job_ids:
                nonterminal_job_ids_str = ','.join(
                    str(job_id) for job_id in nonterminal_job_ids)
                num_nonterminal_jobs = len(nonterminal_job_ids)
                messages.append(
                    f'{colorama.Fore.YELLOW}{capnoun} {service_name!r} has '
                    f'{num_nonterminal_jobs} nonterminal jobs: '
                    f'{nonterminal_job_ids_str}. To terminate the {noun}, '
                    f'please run `sky jobs cancel --pool {service_name}` to '
                    'cancel all jobs in the pool first.'
                    f'{colorama.Style.RESET_ALL}')
                continue
        purge_cmd = (f'sky jobs pool down {service_name} --purge'
                     if pool else f'sky serve down {service_name} --purge')
        if (service_status['status']
                in serve_state.ServiceStatus.failed_statuses()):
            failed_status = service_status['status']
            if purge:
                result = _terminate_failed_services(service_name,
                                                    service_status.get('hash'),
                                                    failed_status,
                                                    pool=pool)
                if result.message is not None:
                    messages.append(result.message)
                if result.completed:
                    terminated_service_names.append(f'{service_name!r}')
            else:
                messages.append(
                    f'{colorama.Fore.YELLOW}{capnoun} {service_name!r} is in '
                    f'failed status ({failed_status}). Skipping '
                    'its termination as it could lead to a resource leak. '
                    f'(Use `{purge_cmd}` to forcefully terminate the {noun}.)'
                    f'{colorama.Style.RESET_ALL}')
                # Don't add to terminated_service_names since it's not
                # actually terminated.
                continue
        else:
            # Send the terminate signal to controller.
            expected_service_hash = service_status.get('hash')
            # Unresolved bound associations retain their admission-time
            # lifecycle epoch as immutable provenance.  Teardown is fenced by
            # the same name-scoped advisory lock plus the atomic
            # SHUTTING_DOWN transition, so retain the live epoch here just as
            # failed-service purge does.  Advancing it before the transition
            # would make the service and its unresolved associations
            # inconsistent at the deferred Serve042 commit guard.
            lifecycle_lock = get_service_lifecycle_lock(service_name,
                                                        advance_epoch=False)
            with lifecycle_lock:
                # Re-read under the same distributed lifecycle fence used by
                # update/apply. This runs on the controller for named and
                # ``--all`` calls alike, so no client-side lock topology can
                # re-open the race.
                current = serve_state.get_service_controller_owner(service_name)
                marked_for_teardown = False
                if (lifecycle_lock_is_valid(lifecycle_lock) and
                        isinstance(expected_service_hash, str) and
                        bool(expected_service_hash) and current is not None and
                        current.get('hash') == expected_service_hash and
                        current['status']
                        not in serve_state.ServiceStatus.terminal_statuses()):
                    try:
                        marked_for_teardown, _ = (
                            _begin_service_teardown_if_owner(
                                service_name, expected_service_hash,
                                (current.get('controller_pid'),
                                 current.get('controller_ip')),
                                get_service_lifecycle_epoch(lifecycle_lock)))
                    except Exception as e:  # pylint: disable=broad-except
                        logger.warning(
                            f'Could not establish ordinary-launch teardown '
                            f'authority for {service_name!r}: '
                            f'{common_utils.format_exception(e)}')
            if not marked_for_teardown:
                messages.append(
                    f'{colorama.Fore.YELLOW}{capnoun} {service_name!r} '
                    'changed incarnation before termination could be '
                    f'signaled; retry the command.{colorama.Style.RESET_ALL}')
                continue
            signal_file = pathlib.Path(
                constants.SIGNAL_FILE_PATH.format(service_name)).expanduser()
            # Make sure parent directory exists.
            signal_file.parent.mkdir(parents=True, exist_ok=True)
            # Filelock is needed to prevent race condition between signal
            # check/removal and signal writing.
            with filelock.FileLock(str(signal_file) + '.lock'):
                with signal_file.open(mode='w', encoding='utf-8') as f:
                    json.dump(
                        {
                            'signal': UserSignal.TERMINATE.value,
                            'service_hash': expected_service_hash,
                        },
                        f,
                        separators=(',', ':'))
                    f.flush()
        # Failed-service purge branches append only after confirming the row
        # was removed; normal signal-based down always reaches this point.
        if (service_status is None or
            (service_status['status']
             not in serve_state.ServiceStatus.failed_statuses() and
             service_status['status']
             != serve_state.ServiceStatus.SHUTTING_DOWN) or not purge):
            terminated_service_names.append(f'{service_name!r}')
    if not terminated_service_names:
        messages.append(f'No {noun} to terminate.')
    else:
        identity_str = f'{capnoun} {terminated_service_names[0]} is'
        if len(terminated_service_names) > 1:
            terminated_service_names_str = ', '.join(terminated_service_names)
            identity_str = f'{capnoun}s {terminated_service_names_str} are'
        messages.append(f'{identity_str} scheduled to be terminated.')
    return '\n'.join(messages)


def wait_service_registration(
        service_name: str,
        job_id: int,
        pool: bool,
        expected_resource_scope: str | None = None) -> str:
    """Util function to call at the end of `sky.serve.up()`.

    This function will:
        (1) Check the name duplication by job id of the controller. If
            the job id is not the same as the database record, this
            means another service is already taken that name. See
            sky/serve/api.py::up for more details.
        (2) Wait for the load balancer port to be assigned and return.

    Returns:
        Encoded load balancer port assigned to the service.
    """

    # TODO (kyuds): when codegen is fully deprecated, return the lb port
    # as an int directly instead of encoding it.
    def _controller_log_path(record: dict[str, Any] | None = None) -> str:
        resource_scope = (record.get('resource_scope')
                          if record is not None else expected_resource_scope)
        return os.path.expanduser(
            generate_remote_controller_log_file_name(service_name,
                                                     resource_scope))

    deadline = (time.monotonic() + constants.CONTROLLER_SETUP_TIMEOUT_SECONDS)
    setup_completed = False
    noun = 'pool' if pool else 'service'
    while True:
        # Only do this check for non-consolidation mode as consolidation mode
        # has no setup process.
        if not is_consolidation_mode(pool):
            job_status = job_lib.get_status(job_id)
            if job_status is not None and job_status.is_terminal():
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'The controller job for the {noun} {service_name!r} '
                        f'reached terminal status {job_status.value} before '
                        'registration completed.')
            if job_status != job_lib.JobStatus.RUNNING:
                # Wait for the controller process to finish setting up. It
                # can be slow if a lot cloud dependencies are being installed.
                if time.monotonic() > deadline:
                    with ux_utils.print_exception_no_traceback():
                        raise RuntimeError(
                            f'Failed to start the controller process for '
                            f'the {noun} {service_name!r} within '
                            f'{constants.CONTROLLER_SETUP_TIMEOUT_SECONDS}'
                            f' seconds.')
                # No need to check the service status as the controller process
                # is still setting up.
                time.sleep(1)
                continue

        if not setup_completed:
            setup_completed = True
            # Give service registration its own full timeout budget.
            deadline = (time.monotonic() +
                        constants.SERVICE_REGISTER_TIMEOUT_SECONDS)

        record = _get_service_status(service_name,
                                     pool=pool,
                                     with_replica_info=False,
                                     with_yaml=False,
                                     status_snapshot_only=True)
        if record is not None:
            if (expected_resource_scope is not None and
                    record.get('resource_scope') != expected_resource_scope):
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'The {noun} {service_name!r} changed incarnation '
                        'during registration; refusing to accept a same-name '
                        'replacement that reused the controller job id.')
            if job_id != record['controller_job_id']:
                if pool:
                    command_to_run = 'sky jobs pool apply --pool'
                else:
                    command_to_run = 'sky serve update'
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        f'The {noun} {service_name!r} is already running. '
                        f'Please specify a different name for your {noun}. '
                        f'To update an existing {noun}, run: {command_to_run}'
                        f' {service_name} <new-{noun}-yaml>')
            status = record['status']
            if status in serve_state.ServiceStatus.terminal_statuses():
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'The {noun} {service_name!r} entered terminal status '
                        f'{status.value} during registration; cleanup is in '
                        'progress or required.')
            lb_port = record['load_balancer_port']
            if lb_port is not None:
                return message_utils.encode_payload(lb_port)
        else:
            controller_log_path = _controller_log_path()
            if os.path.exists(controller_log_path):
                with open(controller_log_path, encoding='utf-8') as f:
                    log_content = f.read()
                if (constants.MAX_NUMBER_OF_SERVICES_REACHED_ERROR
                        in log_content):
                    with ux_utils.print_exception_no_traceback():
                        raise RuntimeError(
                            controller_utils.get_max_services_error_message(
                                pool))
        if time.monotonic() > deadline:
            # Print the controller log to help user debug.
            controller_log_path = _controller_log_path(record)
            try:
                with open(controller_log_path, encoding='utf-8') as f:
                    log_content = f.read()
            except FileNotFoundError:
                log_content = (f'Controller log {controller_log_path!r} '
                               'not found.')
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Failed to register service {service_name!r} '
                                 'on the SkyServe controller. '
                                 f'Reason:\n{log_content}')
        time.sleep(1)


def load_service_initialization_result(payload: str) -> int:
    return message_utils.decode_payload(payload)


def _get_service_log_owner_record(service_name: str,
                                  pool: bool) -> dict[str, Any] | None:
    """Read the slim service row used by log/liveness helpers.

    These paths only need status and resource-scope metadata, so they must
    stay off the joined latest-spec read and never parse YAML.
    """
    record = serve_state.get_service_controller_owner(service_name,
                                                      require_version=True)
    if record is None or record.get('pool') != pool:
        return None
    return record


def _get_healthy_service_log_owner_record(
        service_name: str,
        pool: bool) -> tuple[dict[str, Any] | None, str | None]:
    """Return one slim owner snapshot or the user-facing health error."""
    service_record = _get_service_log_owner_record(service_name, pool)
    capnoun = 'Service' if not pool else 'Pool'
    if service_record is None:
        return None, f'{capnoun} {service_name!r} does not exist.'
    if service_record['status'] == serve_state.ServiceStatus.CONTROLLER_INIT:
        return (None, f'{capnoun} {service_name!r} is still initializing its '
                'controller. Please try again later.')
    return service_record, None


def _check_service_status_healthy(service_name: str, pool: bool) -> str | None:
    _, msg = _get_healthy_service_log_owner_record(service_name, pool)
    return msg


def get_latest_version_with_min_replicas(
        service_name: str,
        replica_infos: list['replica_managers.ReplicaInfo']) -> int | None:
    # Find the latest version with at least min_replicas replicas.
    version2count: collections.defaultdict[int,
                                           int] = collections.defaultdict(int)
    for info in replica_infos:
        if info.is_ready:
            version2count[info.version] += 1

    active_versions = sorted(version2count.keys(), reverse=True)
    specs_by_version = serve_state.get_specs(service_name, active_versions)
    for version in active_versions:
        spec = specs_by_version.get(version)
        if (spec is not None and version2count[version] >= spec.min_replicas):
            return version
    # Use the oldest version if no version has enough replicas.
    return active_versions[-1] if active_versions else None


def _process_line(
        line: str,
        cluster_name: str,
        stop_on_eof: bool = False,
        streamed_provision_log_paths: set | None = None) -> Iterator[str]:
    # The line might be directing users to view logs, like
    # `✓ Cluster launched: new-http.  View logs at: *.log`
    # We should tail the detailed logs for user.
    def cluster_is_up() -> bool:
        status = global_user_state.get_status_from_cluster_name(cluster_name)
        return status in (status_lib.ClusterStatus.UP,
                          status_lib.ClusterStatus.AUTOSTOPPING)

    provision_api_log_prompt = re.match(_SKYPILOT_PROVISION_API_LOG_PATTERN,
                                        line)
    provision_log_cmd_prompt = re.match(_SKYPILOT_PROVISION_LOG_CMD_PATTERN,
                                        line)
    log_prompt = re.match(_SKYPILOT_LOG_PATTERN, line)

    def _stream_provision_path(p: pathlib.Path) -> Iterator[str]:
        # Check if this provision log has already been streamed to avoid
        # duplicate expansion. When a Kubernetes cluster needs to pull a Docker
        # image, rich spinner updates can produce hundreds of lines matching
        # _SKYPILOT_PROVISION_LOG_CMD_PATTERN (e.g., "Launching (1 pod(s)
        # pending due to Pulling)... View logs: sky logs --provision ...").
        # Without this check, the same provision log would be expanded hundreds
        # of times, creating huge log files (30M+) and making users think the
        # system is stuck in an infinite loop.
        if streamed_provision_log_paths is not None:
            resolved_path = str(p.resolve())
            if resolved_path in streamed_provision_log_paths:
                return
            streamed_provision_log_paths.add(resolved_path)

        try:
            with open(p, newline='', encoding='utf-8') as f:
                # Exit if >10s without new content to avoid hanging when INIT
                yield from log_utils.follow_logs(f,
                                                 should_stop=cluster_is_up,
                                                 stop_on_eof=stop_on_eof,
                                                 idle_timeout_seconds=10)
        except FileNotFoundError:
            # Fall back cleanly if the hinted path doesn't exist
            yield line
            yield (f'{colorama.Fore.YELLOW}{colorama.Style.BRIGHT}'
                   f'Try to expand log file {p} but not found. Skipping...'
                   f'{colorama.Style.RESET_ALL}')
        return

    if provision_api_log_prompt is not None:
        rel_path = provision_api_log_prompt.group(1)
        nested_log_path = pathlib.Path(
            skylet_constants.SKY_LOGS_DIRECTORY).expanduser().joinpath(
                rel_path).resolve()
        yield from _stream_provision_path(nested_log_path)
        return

    if provision_log_cmd_prompt is not None:
        # Resolve provision log via cluster table first, then history.
        log_path_str = global_user_state.get_cluster_provision_log_path(
            cluster_name)
        if not log_path_str:
            log_path_str = (
                global_user_state.get_cluster_history_provision_log_path(
                    cluster_name))
        if not log_path_str:
            yield line
            return
        yield from _stream_provision_path(
            pathlib.Path(log_path_str).expanduser().resolve())
        return

    if log_prompt is not None:
        # Now we skip other logs (file sync logs) since we lack
        # utility to determine when these log files are finished
        # writing.
        # TODO(tian): We should not skip these logs since there are
        # small chance that error will happen in file sync. Need to
        # find a better way to do this.
        return

    yield line


def _follow_logs_with_provision_expanding(
    file: TextIO,
    cluster_name: str,
    *,
    should_stop: Callable[[], bool],
    stop_on_eof: bool = False,
    idle_timeout_seconds: int | None = None,
) -> Iterator[str]:
    """Follows logs and expands any provision.log references found.

    Args:
        file: Log file to read from.
        cluster_name: Name of the cluster being launched.
        should_stop: Callback that returns True when streaming should stop.
        stop_on_eof: If True, stop when reaching end of file.
        idle_timeout_seconds: If set, stop after these many seconds without
            new content.

    Yields:
        Log lines, including expanded content from referenced provision logs.
    """
    streamed_provision_log_paths: set = set()

    def process_line(line: str) -> Iterator[str]:
        yield from _process_line(
            line,
            cluster_name,
            stop_on_eof=stop_on_eof,
            streamed_provision_log_paths=streamed_provision_log_paths)

    return log_utils.follow_logs(file,
                                 should_stop=should_stop,
                                 stop_on_eof=stop_on_eof,
                                 process_line=process_line,
                                 idle_timeout_seconds=idle_timeout_seconds)


def _capped_follow_logs_with_provision_expanding(
    log_list: list[str],
    cluster_name: str,
    *,
    line_cap: int = 100,
) -> Iterator[str]:
    """Follows logs and expands any provision.log references found.

    Args:
        log_list: List of Log Lines to read from.
        cluster_name: Name of the cluster being launched.
        line_cap: Number of last lines to return

    Yields:
        Log lines, including expanded content from referenced provision logs.
    """
    all_lines: collections.deque[str] = collections.deque(maxlen=line_cap)
    streamed_provision_log_paths: set = set()

    for line in log_list:
        for processed in _process_line(
                line=line,
                cluster_name=cluster_name,
                stop_on_eof=False,
                streamed_provision_log_paths=streamed_provision_log_paths):
            all_lines.append(processed)

    yield from all_lines


def stream_replica_logs(service_name: str, replica_id: int, follow: bool,
                        tail: int | None, pool: bool) -> str:
    record, msg = _get_healthy_service_log_owner_record(service_name, pool=pool)
    if msg is not None:
        return msg
    assert record is not None
    repnoun = 'worker' if pool else 'replica'
    caprepnoun = repnoun.capitalize()
    print(f'{colorama.Fore.YELLOW}Start streaming logs for launching process '
          f'of {repnoun} {replica_id}.{colorama.Style.RESET_ALL}')
    resource_scope = record.get('resource_scope')
    launch_log_file_name = generate_replica_launch_log_file_name(
        service_name, replica_id, resource_scope)
    if not os.path.exists(launch_log_file_name):
        return (f'{colorama.Fore.RED}{caprepnoun} {replica_id} doesn\'t exist.'
                f'{colorama.Style.RESET_ALL}')

    matching_info = serve_state.get_replica_info_from_id(
        service_name, replica_id)
    recorded_cluster_name = (matching_info.cluster_name
                             if matching_info is not None else None)
    replica_cluster_name = (recorded_cluster_name if isinstance(
        recorded_cluster_name, str) else generate_replica_cluster_name(
            service_name, replica_id, resource_scope))

    def _get_replica_status() -> serve_state.ReplicaStatus:
        # Single-row lookup: this runs on every poll of the follow loop
        # below, so scanning (and unpickling) every replica of the service
        # per poll is O(replicas) wasted work at fleet scale.
        info = serve_state.get_replica_info_from_id(service_name, replica_id)
        if info is not None:
            return info.status
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                _FAILED_TO_FIND_REPLICA_MSG.format(replica_id=replica_id))

    replica_provisioned = (
        lambda: _get_replica_status() != serve_state.ReplicaStatus.PROVISIONING)

    # Handle launch logs based on number parameter
    final_lines_to_print = []
    if tail is not None:
        static_lines = common_utils.read_last_n_lines(launch_log_file_name,
                                                      tail)
        lines = list(
            _capped_follow_logs_with_provision_expanding(
                log_list=static_lines,
                cluster_name=replica_cluster_name,
                line_cap=tail,
            ))
        final_lines_to_print += lines
    else:
        with open(launch_log_file_name, newline='', encoding='utf-8') as f:
            for line in _follow_logs_with_provision_expanding(
                    f,
                    replica_cluster_name,
                    should_stop=replica_provisioned,
                    stop_on_eof=not follow,
            ):
                print(line, end='', flush=True)

    if (not follow and
            _get_replica_status() == serve_state.ReplicaStatus.PROVISIONING):
        # Early exit if not following the logs.
        if tail is not None:
            for line in final_lines_to_print:
                if not line.endswith('\n'):
                    line += '\n'
                print(line, end='', flush=True)
        return ''

    backend = backends.CloudVmRayBackend()
    handle = global_user_state.get_handle_from_cluster_name(
        replica_cluster_name)
    provider_fence: contextlib.AbstractContextManager[None] = (
        contextlib.nullcontext())
    provider_mode = provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
    if matching_info is not None:
        # Imported lazily to avoid the serve_utils -> reserved_capacity ->
        # serve_state import cycle during module initialization.  Log tailing
        # is a remote command just like status and cleanup: a protocol-v2
        # reserved-fill row must prove that its durable handle still targets
        # the same physical Kubernetes cluster before any output is read.
        # pylint: disable-next=import-outside-toplevel
        from sky.serve import reserved_capacity
        cleanup_fence = reserved_capacity.parse_protocol_v2_cleanup_fence(
            matching_info)
        provider_fence = reserved_capacity.protocol_v2_provider_fence(
            matching_info, handle, include_provider_phase=False)
        if cleanup_fence is not None:
            provider_mode = provider_phase.ProviderPhaseMode.V2_FENCED
    if handle is None:
        if tail is not None:
            for line in final_lines_to_print:
                if not line.endswith('\n'):
                    line += '\n'
                print(line, end='', flush=True)
        return _FAILED_TO_FIND_REPLICA_MSG.format(replica_id=replica_id)
    assert isinstance(handle, backends.CloudVmRayResourceHandle), handle

    # Notify user here to make sure user won't think the log is finished.
    print(f'{colorama.Fore.YELLOW}Start streaming logs for task job '
          f'of {repnoun} {replica_id}...{colorama.Style.RESET_ALL}')

    # Always tail the latest logs, which represent user setup & run. A
    # bounded read participates in the normal provider phases. Interactive
    # follow deliberately holds only its immutable physical fence: keeping a
    # process-wide phase open for an unbounded stream would starve every
    # opposite-mode operation in the API process.
    phase_context: contextlib.AbstractContextManager = (
        contextlib.nullcontext()
        if follow else provider_phase.provider_phase(provider_mode))
    if tail is None:
        with phase_context, provider_fence:
            returncode = backend.tail_logs(handle, job_id=None, follow=follow)
        if returncode != 0:
            return (f'{colorama.Fore.RED}Failed to stream logs for {repnoun} '
                    f'{replica_id}.{colorama.Style.RESET_ALL}')
    elif not follow and tail > 0:
        with phase_context, provider_fence:
            final = backend.tail_logs(handle,
                                      job_id=None,
                                      follow=follow,
                                      tail=tail,
                                      stream_logs=False,
                                      require_outputs=True,
                                      process_stream=True)
        if isinstance(final, int) or (final[0] != 0 and final[0] != 101):
            if tail is not None:
                for line in final_lines_to_print:
                    if not line.endswith('\n'):
                        line += '\n'
                    print(line, end='', flush=True)
            return (f'{colorama.Fore.RED}Failed to stream logs for replica '
                    f'{replica_id}.{colorama.Style.RESET_ALL}')
        final_lines_to_print += final[1].splitlines()
        for line in final_lines_to_print[-tail:]:
            if not line.endswith('\n'):
                line += '\n'
            print(line, end='', flush=True)
    return ''


def stream_serve_process_logs(service_name: str, stream_controller: bool,
                              follow: bool, tail: int | None,
                              pool: bool) -> str:
    record, msg = _get_healthy_service_log_owner_record(service_name, pool)
    if msg is not None:
        return msg
    assert record is not None
    if not stream_controller:
        if pool:
            return 'Pools do not have a load balancer.'
        # Lazy import avoids the lb_k8s -> serve_utils module cycle. External-
        # only SkyServe writes LB output to the Kubernetes Pod log, never the
        # legacy controller-local load_balancer.log file.
        from sky.serve import lb_k8s  # pylint: disable=import-outside-toplevel
        return lb_k8s.stream_lb_logs(service_name, follow, tail)
    resource_scope = record.get('resource_scope')
    log_file = generate_remote_controller_log_file_name(service_name,
                                                        resource_scope)

    def _service_is_terminal() -> bool:
        record = _get_service_log_owner_record(service_name, pool)
        if record is None:
            return True
        return record['status'] in serve_state.ServiceStatus.failed_statuses()

    if tail is not None:
        lines = common_utils.read_last_n_lines(os.path.expanduser(log_file),
                                               tail)
        for line in lines:
            if not line.endswith('\n'):
                line += '\n'
            print(line, end='', flush=True)
    else:
        with open(os.path.expanduser(log_file), newline='',
                  encoding='utf-8') as f:
            for line in log_utils.follow_logs(
                    f,
                    should_stop=_service_is_terminal,
                    stop_on_eof=not follow,
            ):
                print(line, end='', flush=True)
    return ''


# =========================== CodeGen for Sky Serve ===========================
# TODO (kyuds): deprecate and remove serve codegen entirely.


# TODO(tian): Use REST API instead of SSH in the future. This codegen pattern
# is to reuse the authentication of ssh. If we want to use REST API, we need
# to implement some authentication mechanism.
class ServeCodeGen:
    """Code generator for SkyServe.

    Usage:
      >> code = ServeCodeGen.get_service_status(service_name)
    """

    # TODO(zhwu): When any API is changed, we should update the
    # constants.SERVE_VERSION.
    _PREFIX = [
        'from sky.serve import serve_state',
        'from sky.serve import serve_utils',
        'from sky.serve import constants',
        'serve_version = constants.SERVE_VERSION',
    ]

    @classmethod
    def get_service_status(cls,
                           service_names: list[str] | None,
                           pool: bool,
                           summary_only: bool = False,
                           include_target_num_replicas: bool | None = None,
                           metadata_only: bool = False) -> str:
        if metadata_only:
            # Serve v9 controllers already expose the slim lifecycle snapshot
            # used by control paths, but do not understand the metadata_only
            # RPC field. Build the projection from that primitive so existing
            # services benefit immediately after an API-server rollout instead
            # of silently materializing the full historical replica inventory.
            metadata_code = [
                f'names = {service_names!r}',
                ('names = serve_state.get_glob_service_names('
                 f'None, pool={pool}) if names is None else names'),
                ('statuses = [serve_utils._get_service_status('
                 f'name, pool={pool}, with_replica_info=False, '
                 'with_yaml=False, with_target_num_replicas=False, '
                 'status_snapshot_only=True) for name in names]'),
                ('statuses = [status for status in statuses '
                 'if status is not None]'),
                ('_ = [status.update({"metadata_only": True}) '
                 'for status in statuses]'),
                'statuses = sorted(statuses, key=lambda status: status["name"])',
                ('pickled = [{key: serve_utils.base64.b64encode('
                 'serve_utils.pickle.dumps(value)).decode("utf-8") '
                 'for key, value in status.items()} for status in statuses]'),
                ('msg = serve_utils.message_utils.encode_payload('
                 'pickled, payload_type="service_status")'),
                'print(msg, end="", flush=True)',
            ]
            return cls._build(metadata_code)
        # summary_only is only forwarded to controllers whose lib version
        # understands it (v6+); older controllers just return the full
        # payload — a graceful degradation, never an error.
        code: list[str | None] = [
            f'kwargs={{}} if serve_version < 3 else {{"pool": {pool}}}',
            ('kwargs.update({"summary_only": '
             f'{summary_only}}}) if serve_version >= 6 else None'),
            ('kwargs.update({"include_target_num_replicas": '
             f'{include_target_num_replicas}}}) if serve_version >= 7 else '
             'None') if include_target_num_replicas is not None else None,
            f'msg = serve_utils.get_service_status_encoded({service_names!r}, '
            '**kwargs)', 'print(msg, end="", flush=True)'
        ]
        return cls._build([line for line in code if line is not None])

    @classmethod
    def add_version(cls, service_name: str) -> str:
        code = [
            f'msg = serve_utils.add_version_encoded({service_name!r})',
            'print(msg, end="", flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def remove_uncommitted_staged_controller_config(
            cls,
            service_name: str,
            version: int,
            resource_scope: str | None,
            snapshot_id: str | None = None) -> str:
        code = [
            ('removed = serve_utils.'
             'remove_uncommitted_staged_controller_config('
             f'{service_name!r}, {version!r}, {resource_scope!r}, '
             f'{snapshot_id!r})'),
            'print(str(int(removed)), end="", flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def terminate_services(cls, service_names: list[str] | None, purge: bool,
                           pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 3 else {{"pool": {pool}}}',
            f'msg = serve_utils.terminate_services({service_names!r}, '
            f'purge={purge}, **kwargs)', 'print(msg, end="", flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def terminate_replica(cls, service_name: str, replica_id: int,
                          purge: bool) -> str:
        code = [
            f'(lambda: print(serve_utils.terminate_replica({service_name!r}, '
            f'{replica_id}, {purge}), end="", flush=True) '
            'if getattr(constants, "SERVE_VERSION", 0) >= 2 else '
            f'exec("raise RuntimeError('
            f'{constants.TERMINATE_REPLICA_VERSION_MISMATCH_ERROR!r})"))()'
        ]
        return cls._build(code)

    @classmethod
    def wait_service_registration(cls, service_name: str, job_id: int,
                                  pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 4 else {{"pool": {pool}}}',
            'msg = serve_utils.wait_service_registration('
            f'{service_name!r}, {job_id}, **kwargs)',
            'print(msg, end="", flush=True)'
        ]
        cmd = cls._build(code)
        # When running in consolidation mode, the codegen subprocess inherits
        # SKYPILOT_GLOBAL_CONFIG pointing to the client override config, which
        # lacks serve.controller.consolidation_mode=true. The subprocess would
        # then read the server config from the client override path and
        # incorrectly conclude it is NOT in consolidation mode, causing a
        # 300-second CONTROLLER_SETUP_TIMEOUT. Bake OVERRIDE_CONSOLIDATION_MODE
        # into the shell command so the subprocess always sees the correct mode.
        if is_consolidation_mode(pool):
            cmd = (f'export {skylet_constants.OVERRIDE_CONSOLIDATION_MODE}'
                   f'=true; {cmd}')
        return cmd

    @classmethod
    def stream_replica_logs(cls, service_name: str, replica_id: int,
                            follow: bool, tail: int | None, pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 5 else {{"pool": {pool}}}',
            'msg = serve_utils.stream_replica_logs('
            f'{service_name!r}, {replica_id!r}, follow={follow}, tail={tail}, '
            '**kwargs)', 'print(msg, flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def stream_serve_process_logs(cls, service_name: str,
                                  stream_controller: bool, follow: bool,
                                  tail: int | None, pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 5 else {{"pool": {pool}}}',
            f'msg = serve_utils.stream_serve_process_logs({service_name!r}, '
            f'{stream_controller}, follow={follow}, tail={tail}, **kwargs)',
            'print(msg, flush=True)'
        ]
        return cls._build(code)

    @classmethod
    def update_service(cls, service_name: str, version: int, mode: str,
                       pool: bool) -> str:
        code = [
            f'kwargs={{}} if serve_version < 3 else {{"pool": {pool}}}',
            f'msg = serve_utils.update_service_encoded({service_name!r}, '
            f'{version}, mode={mode!r}, **kwargs)',
            'print(msg, end="", flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def _build(cls, code: list[str]) -> str:
        code = cls._PREFIX + code
        generated_code = '; '.join(code)
        # Use the local user id to make sure the operation goes to the correct
        # user.
        return (f'export {skylet_constants.USER_ID_ENV_VAR}='
                f'"{common_utils.get_user_hash()}"; '
                f'{skylet_constants.SKY_PYTHON_CMD} '
                f'-u -c {shlex.quote(generated_code)}')
