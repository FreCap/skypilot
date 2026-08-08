"""Typed cleanup contract for SkyServe-owned ephemeral storage.

This module deliberately depends on neither Task construction nor Serve state.
It is shared by durable-state writers and offline migration tooling, which must
agree on the exact internal Task-YAML spelling and cleanup target projection.
"""

import dataclasses
import hashlib
import json
from typing import Any

from sky.serve import constants
from sky.utils import yaml_utils


class EphemeralStorageContractError(ValueError):
    """Task YAML does not contain a safe ephemeral-storage contract."""


@dataclasses.dataclass(frozen=True)
class EphemeralStorageScope:
    """Exact identity and owned mount paths for one storage generation."""

    resource_scope: str
    scope_id: str
    storage_generation: str
    storage_mounts: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class ZeroDeletionTargetProjection:
    """Secret-free proof that one cleanup contract has no deletion targets."""

    scope: EphemeralStorageScope

    def to_dict(self) -> dict[str, object]:
        """Return the canonical, path-free projection used by proof ledgers."""
        return {
            'resource_scope': self.scope.resource_scope,
            'scope_id': self.scope.scope_id,
            'storage_generation': self.scope.storage_generation,
            'file_mount_target_count': 0,
            'storage_mount_target_count': 0,
            'volume_target_count': 0,
            'volume_mount_target_count': 0,
            'workdir_target_count': 0,
            'scoped_storage_mount_target_count': 0,
        }


def canonical_ephemeral_storage_scope_id(resource_scope: str,
                                         storage_generation: str) -> str:
    """Return the exact compact namespace used by the SkyServe writer."""
    if (type(resource_scope) is not str or not resource_scope or
            type(storage_generation) is not str or not storage_generation):
        raise EphemeralStorageContractError(
            'Storage scope identities must be non-empty strings.')
    identity = json.dumps([resource_scope, storage_generation],
                          separators=(',', ':'))
    digest = hashlib.sha256(identity.encode('utf-8')).hexdigest()
    return f'sv{digest[:10]}'


def _load_task_yaml(yaml_content: str) -> dict[str, Any]:
    if type(yaml_content) is not str:
        raise EphemeralStorageContractError(
            'Ephemeral-storage cleanup YAML must be a string.')
    try:
        yaml_utils.check_no_duplicate_keys(yaml_content)
        config = yaml_utils.safe_load_value_free(yaml_content)
    except (TypeError, ValueError):
        raise EphemeralStorageContractError(
            'Ephemeral-storage cleanup YAML is malformed.') from None
    if type(config) is not dict:
        raise EphemeralStorageContractError(
            'Ephemeral-storage cleanup YAML must be a mapping.')
    return config


def _parse_scope_from_config(
        config: dict[str, Any]) -> EphemeralStorageScope | None:
    scope_key = constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY
    legacy_metadata = config.get('metadata')
    if type(legacy_metadata) is dict and scope_key in legacy_metadata:
        raise EphemeralStorageContractError(
            'Ephemeral-storage scope uses the wrong Task metadata field.')
    if '_metadata' not in config:
        return None
    metadata = config['_metadata']
    if type(metadata) is not dict:
        raise EphemeralStorageContractError(
            'Task `_metadata` must be a mapping.')
    if scope_key not in metadata:
        return None
    scope = metadata[scope_key]
    if type(scope) is not dict:
        raise EphemeralStorageContractError(
            'Ephemeral-storage scope metadata must be a mapping.')

    required_fields = {
        'resource_scope',
        'scope_id',
        'storage_generation',
        'storage_mounts',
    }
    if set(scope) != required_fields:
        raise EphemeralStorageContractError(
            'Ephemeral-storage scope metadata has partial or unknown fields.')
    resource_scope = scope['resource_scope']
    scope_id = scope['scope_id']
    storage_generation = scope['storage_generation']
    storage_mounts = scope['storage_mounts']
    if (type(resource_scope) is not str or not resource_scope or
            type(scope_id) is not str or not scope_id or
            type(storage_generation) is not str or not storage_generation):
        raise EphemeralStorageContractError(
            'Ephemeral-storage scope identities must be non-empty strings.')
    if (type(storage_mounts) is not list or
            any(type(mount) is not str for mount in storage_mounts)):
        raise EphemeralStorageContractError(
            'Ephemeral-storage scope mounts must be a list of strings.')
    expected_scope_id = canonical_ephemeral_storage_scope_id(
        resource_scope, storage_generation)
    if scope_id != expected_scope_id:
        raise EphemeralStorageContractError(
            'Ephemeral-storage scope ID is not canonical.')
    return EphemeralStorageScope(
        resource_scope=resource_scope,
        scope_id=scope_id,
        storage_generation=storage_generation,
        storage_mounts=tuple(storage_mounts),
    )


def parse_ephemeral_storage_scope(
        yaml_content: str | None) -> EphemeralStorageScope | None:
    """Parse the exact internal Task-YAML storage scope, if one is present.

    Only ``_metadata.sky_serve_ephemeral_storage_scope`` is authoritative.
    The public-looking ``metadata`` key is intentionally not an alias.  An
    attempted scope under that wrong spelling raises because treating it as
    ordinary absence would recreate the missed-handoff failure class.  A
    missing authoritative key returns ``None``; a present but malformed scope
    raises so cleanup and handoff callers fail closed.
    """
    if yaml_content is None:
        return None
    return _parse_scope_from_config(_load_task_yaml(yaml_content))


def _require_empty_mapping(config: dict[str, Any], field: str) -> None:
    value = config.get(field)
    if value is not None and (type(value) is not dict or value):
        raise EphemeralStorageContractError(
            'Ephemeral-storage cleanup YAML has a nonzero deletion target.')


def require_zero_deletion_target_projection(
        yaml_content: str) -> ZeroDeletionTargetProjection:
    """Return a typed projection only when cleanup would target nothing.

    The accepted empty spellings mirror Task YAML without constructing a Task:
    mappings may be absent, null, or exactly empty; ``volume_mounts`` may be
    absent, null, or an exact empty list; and ``workdir`` may only be absent or
    null.  A scoped cleanup contract is mandatory and its owned mount list must
    be exactly empty.
    """
    config = _load_task_yaml(yaml_content)
    scope = _parse_scope_from_config(config)
    if scope is None:
        raise EphemeralStorageContractError(
            'Zero-target cleanup proof requires a storage scope.')
    if scope.storage_mounts:
        raise EphemeralStorageContractError(
            'Ephemeral-storage cleanup YAML has a nonzero deletion target.')

    for field in ('file_mounts', 'storage_mounts', 'volumes'):
        _require_empty_mapping(config, field)
    volume_mounts = config.get('volume_mounts')
    if (volume_mounts is not None and
        (type(volume_mounts) is not list or volume_mounts)):
        raise EphemeralStorageContractError(
            'Ephemeral-storage cleanup YAML has a nonzero deletion target.')
    if config.get('workdir') is not None:
        raise EphemeralStorageContractError(
            'Ephemeral-storage cleanup YAML has a nonzero deletion target.')
    return ZeroDeletionTargetProjection(scope=scope)
