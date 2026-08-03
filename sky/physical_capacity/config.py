"""Strict server-owned configuration for physical-capacity staging."""

from collections.abc import Callable
import dataclasses
import enum
import json
import os
import re
from typing import Any, TypeVar
import uuid

from sky.physical_capacity import canonical
from sky.physical_capacity import models
from sky.skylet import constants as skylet_constants

PHYSICAL_CAPACITY_MODE_ENV_VAR = 'SKYPILOT_PHYSICAL_CAPACITY_MODE'
PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR = (
    'SKYPILOT_PHYSICAL_CAPACITY_ALLOWLIST_JSON')
MODE_ENV_VAR = PHYSICAL_CAPACITY_MODE_ENV_VAR
ALLOWLIST_ENV_VAR = PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR

MAX_ALLOWLIST_JSON_BYTES = 65536
MAX_ALLOWLIST_ENTRIES_PER_FIELD = 1000
MAX_ALLOWLIST_ENTRY_BYTES = 512

_ALLOWLIST_FIELDS = frozenset(
    {'providers', 'workspaces', 'owner_kinds', 'groups', 'verbs'})
_T = TypeVar('_T')


class CapacityMode(str, enum.Enum):
    DISABLED = 'disabled'
    SHADOW = 'shadow'
    OBSERVE = 'observe'
    TEARDOWN = 'teardown'
    SERVE = 'serve'
    JOBS = 'jobs'


class CapacityProvider(str, enum.Enum):
    AWS = 'aws'
    GCP = 'gcp'
    KUBERNETES = 'kubernetes'


class CapacityVerb(str, enum.Enum):
    OBSERVE = 'observe'
    LAUNCH = 'launch'
    START = 'start'
    STOP = 'stop'
    DOWN = 'down'
    OCCUPY = 'occupy'


@dataclasses.dataclass(frozen=True)
class CapacityAllowlist:
    providers: tuple[CapacityProvider, ...] = ()
    workspaces: tuple[str, ...] = ()
    owner_kinds: tuple[models.OwnerKind, ...] = ()
    groups: tuple[str, ...] = ()
    verbs: tuple[CapacityVerb, ...] = ()


@dataclasses.dataclass(frozen=True)
class CapacityConfig:
    mode: CapacityMode = CapacityMode.DISABLED
    allowlist: CapacityAllowlist | None = None


def _reject_json_constant(value: str) -> None:
    raise ValueError(f'Non-standard JSON constant {value!r} is not allowed.')


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON field {key!r} is not allowed.')
        result[key] = value
    return result


def _validate_entry_string(value: object, field: str) -> str:
    validated = canonical.validate_bounded_string(
        value,
        max_bytes=MAX_ALLOWLIST_ENTRY_BYTES,
        field=f'Capacity allowlist {field} entry')
    if validated != validated.strip():
        raise ValueError(
            f'Capacity allowlist {field} entries must not have surrounding '
            'whitespace.')
    return validated


def _parse_enum(value: str, field: str, enum_type: Callable[[str], _T]) -> _T:
    try:
        return enum_type(value)
    except ValueError as e:
        raise ValueError(
            f'Unknown capacity allowlist {field} value {value!r}.') from e


def _parse_workspace(value: str) -> str:
    if (len(value.encode('utf-8')) > canonical.MAX_WORKSPACE_IDENTIFIER_BYTES or
            len(value) > skylet_constants.WORKSPACE_NAME_MAX_LENGTH or
            re.fullmatch(skylet_constants.WORKSPACE_NAME_VALID_REGEX,
                         value) is None):
        raise ValueError(
            f'Invalid capacity allowlist workspace value {value!r}.')
    return value


def _parse_group(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as e:
        raise ValueError(
            f'Invalid capacity allowlist group UUID {value!r}.') from e
    canonical_value = str(parsed)
    if value != canonical_value:
        raise ValueError('Capacity allowlist group UUIDs must use canonical '
                         'lowercase hyphenated form.')
    return canonical_value


def _parse_entries(raw: object, field: str,
                   parser: Callable[[str], _T]) -> tuple[_T, ...]:
    if not isinstance(raw, list):
        raise ValueError(
            f'Capacity allowlist field {field!r} must be a JSON array.')
    if len(raw) > MAX_ALLOWLIST_ENTRIES_PER_FIELD:
        raise ValueError(
            f'Capacity allowlist field {field!r} must contain at most '
            f'{MAX_ALLOWLIST_ENTRIES_PER_FIELD} entries.')
    parsed: list[_T] = []
    for value in raw:
        parsed.append(parser(_validate_entry_string(value, field)))
    if len(set(parsed)) != len(parsed):
        raise ValueError(
            f'Capacity allowlist field {field!r} must not contain duplicates.')
    return tuple(parsed)


def _parse_allowlist(raw: str | None) -> CapacityAllowlist | None:
    if raw is None:
        return None
    try:
        raw_bytes = raw.encode('utf-8')
    except UnicodeEncodeError as e:
        raise ValueError('Capacity allowlist must be valid UTF-8.') from e
    if len(raw_bytes) > MAX_ALLOWLIST_JSON_BYTES:
        raise ValueError(f'Capacity allowlist JSON must be at most '
                         f'{MAX_ALLOWLIST_JSON_BYTES} UTF-8 bytes.')
    try:
        parsed = json.loads(raw,
                            object_pairs_hook=_strict_json_object,
                            parse_constant=_reject_json_constant)
    except json.JSONDecodeError as e:
        raise ValueError('Capacity allowlist must be valid JSON.') from e
    if not isinstance(parsed, dict):
        raise ValueError('Capacity allowlist must be a JSON object.')
    unknown_fields = set(parsed) - _ALLOWLIST_FIELDS
    if unknown_fields:
        raise ValueError('Unknown capacity allowlist field(s): '
                         f'{", ".join(sorted(unknown_fields))}.')

    return CapacityAllowlist(
        providers=_parse_entries(
            parsed.get('providers', []), 'providers',
            lambda value: _parse_enum(value, 'providers', CapacityProvider)),
        workspaces=_parse_entries(parsed.get('workspaces', []), 'workspaces',
                                  _parse_workspace),
        owner_kinds=_parse_entries(
            parsed.get('owner_kinds', []), 'owner_kinds',
            lambda value: _parse_enum(value, 'owner_kinds', models.OwnerKind)),
        groups=_parse_entries(parsed.get('groups', []), 'groups', _parse_group),
        verbs=_parse_entries(
            parsed.get('verbs', []), 'verbs',
            lambda value: _parse_enum(value, 'verbs', CapacityVerb)),
    )


def load_config() -> CapacityConfig:
    """Load and validate capacity configuration without caching it."""
    raw_mode = os.environ.get(PHYSICAL_CAPACITY_MODE_ENV_VAR,
                              CapacityMode.DISABLED.value)
    try:
        mode = CapacityMode(raw_mode)
    except ValueError as e:
        raise ValueError(f'Unknown physical-capacity mode {raw_mode!r}.') from e
    allowlist = _parse_allowlist(
        os.environ.get(PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR))
    return CapacityConfig(mode=mode, allowlist=allowlist)


def validate_runtime_capability(config: CapacityConfig,
                                revision: str = '001') -> None:
    """Fail closed when configuration requests unavailable C1 behavior."""
    if not isinstance(config, CapacityConfig):
        raise TypeError('config must be a CapacityConfig.')
    if revision != '001':
        raise ValueError(
            f'Unsupported physical-capacity schema revision {revision!r}.')
    if config.mode is not CapacityMode.DISABLED:
        raise RuntimeError(
            f'Physical-capacity mode {config.mode.value!r} is unavailable '
            f'with schema revision {revision}.')
