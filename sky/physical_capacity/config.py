"""Strict server-owned configuration for physical-capacity staging."""

from collections.abc import Callable
import dataclasses
import datetime
import enum
import json
import os
import re
from typing import Any, TypeVar
import uuid

from sky.physical_capacity import canonical
from sky.physical_capacity import contracts
from sky.physical_capacity import models
from sky.skylet import constants as skylet_constants

PHYSICAL_CAPACITY_MODE_ENV_VAR = 'SKYPILOT_PHYSICAL_CAPACITY_MODE'
PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR = (
    'SKYPILOT_PHYSICAL_CAPACITY_ALLOWLIST_JSON')
PHYSICAL_CAPACITY_SOURCES_ENV_VAR = ('SKYPILOT_PHYSICAL_CAPACITY_SOURCES_JSON')
PHYSICAL_CAPACITY_PILOT_END_ENV_VAR = (
    'SKYPILOT_PHYSICAL_CAPACITY_PILOT_END_UTC')
MODE_ENV_VAR = PHYSICAL_CAPACITY_MODE_ENV_VAR
ALLOWLIST_ENV_VAR = PHYSICAL_CAPACITY_ALLOWLIST_ENV_VAR
SOURCES_ENV_VAR = PHYSICAL_CAPACITY_SOURCES_ENV_VAR
PILOT_END_ENV_VAR = PHYSICAL_CAPACITY_PILOT_END_ENV_VAR

API_SERVER_ROLE_ENV_VAR = 'SKYPILOT_API_SERVER_ROLE'
API_REQUEST_BACKEND_ENV_VAR = 'SKYPILOT_API_REQUEST_BACKEND'
CONTROLLER_SERVER_ROLE = 'controller'
POSTGRES_REQUEST_BACKEND = 'postgres'

MAX_ALLOWLIST_JSON_BYTES = 65536
MAX_ALLOWLIST_ENTRIES_PER_FIELD = 1000
MAX_ALLOWLIST_ENTRY_BYTES = 512
MAX_SOURCES_JSON_BYTES = 65536
MAX_SOURCE_SELECTORS = 1000
MAX_SOURCE_PARTITIONS = 16

_ALLOWLIST_FIELDS = frozenset(
    {'providers', 'workspaces', 'owner_kinds', 'groups', 'verbs'})
_SERVE_SELECTOR_FIELDS = frozenset({'workspace', 'source_kind', 'service_name'})
_MANAGED_SELECTOR_FIELDS = frozenset(
    {'workspace', 'source_kind', 'spot_job_id', 'task_id'})
_PILOT_END_PATTERN = re.compile(
    r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
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
    sources: tuple[contracts.SourceSelector, ...] = ()
    pilot_end_utc: str | None = None

    @property
    def partitions(self) -> tuple[contracts.SourcePartition, ...]:
        return tuple(
            sorted(
                {
                    contracts.selector_partition(selector)
                    for selector in self.sources
                },
                key=lambda partition:
                (partition.workspace, partition.source_kind.value)))


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


def _selector_sort_key(selector: contracts.SourceSelector) -> bytes:
    return canonical.canonical_json_bytes(
        {
            'mapping_version': contracts.MAPPING_VERSION,
            'component': 'source_selectors',
            'value': contracts.selector_payload(selector),
        },
        domain=canonical.CanonicalDomain.SCOPE_ENTRY)


def _parse_selector(raw: object) -> contracts.SourceSelector:
    if not isinstance(raw, dict):
        raise ValueError('Each physical-capacity source selector must be a '
                         'JSON object.')
    raw_source_kind = raw.get('source_kind')
    if not isinstance(raw_source_kind, str):
        raise ValueError('Selector source_kind must be a string.')
    try:
        source_kind = models.ProjectionSourceKind(raw_source_kind)
    except ValueError as e:
        raise ValueError(
            f'Unknown selector source_kind {raw_source_kind!r}.') from e
    expected_fields = (_MANAGED_SELECTOR_FIELDS if source_kind
                       is models.ProjectionSourceKind.MANAGED_JOB_TASK else
                       _SERVE_SELECTOR_FIELDS)
    if set(raw) != expected_fields:
        missing = sorted(expected_fields - set(raw))
        unknown = sorted(set(raw) - expected_fields)
        details = []
        if missing:
            details.append(f'missing {missing}')
        if unknown:
            details.append(f'unknown {unknown}')
        raise ValueError('Selector fields must match its source kind exactly '
                         f'({"; ".join(details)}).')
    if source_kind is models.ProjectionSourceKind.MANAGED_JOB_TASK:
        return contracts.ManagedJobTaskSelector(workspace=raw['workspace'],
                                                spot_job_id=raw['spot_job_id'],
                                                task_id=raw['task_id'])
    return contracts.ServeSourceSelector(workspace=raw['workspace'],
                                         source_kind=source_kind,
                                         service_name=raw['service_name'])


def _parse_sources(raw: str | None) -> tuple[contracts.SourceSelector, ...]:
    if raw is None:
        return ()
    try:
        raw_bytes = raw.encode('utf-8')
    except UnicodeEncodeError as e:
        raise ValueError(
            'Physical-capacity sources must be valid UTF-8.') from e
    if len(raw_bytes) > MAX_SOURCES_JSON_BYTES:
        raise ValueError('Physical-capacity sources JSON must be at most '
                         f'{MAX_SOURCES_JSON_BYTES} UTF-8 bytes.')
    try:
        parsed = json.loads(raw,
                            object_pairs_hook=_strict_json_object,
                            parse_constant=_reject_json_constant)
    except json.JSONDecodeError as e:
        raise ValueError('Physical-capacity sources must be valid JSON.') from e
    if not isinstance(parsed, list):
        raise ValueError('Physical-capacity sources must be one top-level '
                         'JSON array.')
    if len(parsed) > MAX_SOURCE_SELECTORS:
        raise ValueError('Physical-capacity sources must contain at most '
                         f'{MAX_SOURCE_SELECTORS} selectors.')
    selectors = tuple(_parse_selector(value) for value in parsed)
    if len(set(selectors)) != len(selectors):
        raise ValueError('Physical-capacity source selectors must not contain '
                         'duplicates.')
    partitions = {
        contracts.selector_partition(selector) for selector in selectors
    }
    if len(partitions) > MAX_SOURCE_PARTITIONS:
        raise ValueError('Physical-capacity sources must contain at most '
                         f'{MAX_SOURCE_PARTITIONS} partitions.')
    return tuple(sorted(selectors, key=_selector_sort_key))


def parse_pilot_end_utc(raw: object) -> str:
    """Validate and return the one accepted pilot-end representation."""
    try:
        raw_bytes = raw.encode('utf-8') if isinstance(raw, str) else b''
    except UnicodeEncodeError as e:
        raise ValueError(
            'Physical-capacity pilot end must be valid UTF-8.') from e
    if (not isinstance(raw, str) or len(raw_bytes) != 20 or
            _PILOT_END_PATTERN.fullmatch(raw) is None):
        raise ValueError('Physical-capacity pilot end must use '
                         'YYYY-MM-DDTHH:MM:SSZ.')
    try:
        datetime.datetime.strptime(raw, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError as e:
        raise ValueError('Physical-capacity pilot end is not a valid UTC '
                         'timestamp.') from e
    return raw


def pilot_end_datetime(value: str) -> datetime.datetime:
    """Decode a validated pilot end without applying activation-time policy."""
    normalized = parse_pilot_end_utc(value)
    return datetime.datetime.strptime(
        normalized, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)


def _validate_selector_allowlists(sources: tuple[contracts.SourceSelector, ...],
                                  allowlist: CapacityAllowlist | None) -> None:
    if allowlist is None:
        return
    admitted_workspaces = set(allowlist.workspaces)
    admitted_owner_kinds = set(allowlist.owner_kinds)
    for selector in sources:
        if (admitted_workspaces and
                selector.workspace not in admitted_workspaces):
            raise ValueError(
                f'Selector workspace {selector.workspace!r} is not admitted '
                'by the physical-capacity workspace allowlist.')
        owner_kind = contracts.owner_kind_for_selector(selector)
        if admitted_owner_kinds and owner_kind not in admitted_owner_kinds:
            raise ValueError(
                f'Selector owner kind {owner_kind.value!r} is not admitted '
                'by the physical-capacity owner-kind allowlist.')


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
    sources = _parse_sources(os.environ.get(PHYSICAL_CAPACITY_SOURCES_ENV_VAR))
    raw_pilot_end = os.environ.get(PHYSICAL_CAPACITY_PILOT_END_ENV_VAR)
    pilot_end_utc = (None if raw_pilot_end is None else
                     parse_pilot_end_utc(raw_pilot_end))
    _validate_selector_allowlists(sources, allowlist)
    return CapacityConfig(mode=mode,
                          allowlist=allowlist,
                          sources=sources,
                          pilot_end_utc=pilot_end_utc)


def validate_runtime_capability(config: CapacityConfig,
                                revision: str = '001') -> None:
    """Fail closed when configuration requests unavailable C1 behavior."""
    if not isinstance(config, CapacityConfig):
        raise TypeError('config must be a CapacityConfig.')
    if revision != '001':
        raise ValueError(
            f'Unsupported physical-capacity schema revision {revision!r}.')
    if config.mode not in (CapacityMode.DISABLED, CapacityMode.SHADOW):
        raise RuntimeError(
            f'Physical-capacity mode {config.mode.value!r} is unavailable '
            f'with schema revision {revision}.')


def validate_common_runtime_environment(
    config: CapacityConfig,
    revision: str = '001',
    *,
    server_role: str | None = None,
    request_backend: str | None = None,
) -> None:
    """Validate shadow gates that need no consolidation or database state.

    Consolidation, co-location, catalog, durable pilot-anchor, and leadership
    checks intentionally remain in post-initialization activation code.
    """
    validate_runtime_capability(config, revision=revision)
    if config.mode is CapacityMode.DISABLED:
        return
    if not config.sources:
        raise RuntimeError(
            f'{PHYSICAL_CAPACITY_SOURCES_ENV_VAR} must contain at least one '
            'selector in shadow mode.')
    if config.pilot_end_utc is None:
        raise RuntimeError(
            f'{PHYSICAL_CAPACITY_PILOT_END_ENV_VAR} is required in shadow '
            'mode.')
    parse_pilot_end_utc(config.pilot_end_utc)
    _validate_selector_allowlists(config.sources, config.allowlist)
    if config.allowlist is not None and config.allowlist.groups:
        raise RuntimeError('A non-empty physical-capacity group allowlist is '
                           'not permitted in shadow mode.')
    resolved_role = (os.environ.get(API_SERVER_ROLE_ENV_VAR, 'all')
                     if server_role is None else server_role)
    if resolved_role != CONTROLLER_SERVER_ROLE:
        raise RuntimeError('Physical-capacity shadow mode requires the split '
                           'controller server role.')
    resolved_backend = (os.environ.get(API_REQUEST_BACKEND_ENV_VAR, 'sqlite')
                        if request_backend is None else request_backend)
    if resolved_backend != POSTGRES_REQUEST_BACKEND:
        raise RuntimeError('Physical-capacity shadow mode requires the '
                           'PostgreSQL request backend.')
