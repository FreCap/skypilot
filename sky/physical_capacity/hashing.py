"""Streaming hash contracts for the C2 physical-capacity evidence scan."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
import datetime
import enum
import hashlib
import re
import struct

from sky.physical_capacity import canonical
from sky.physical_capacity import contracts
from sky.physical_capacity import models

_SCOPE_COMPONENT_PREFIX = b'skypilot-capacity-scope-component-v1\x00'
_EVIDENCE_PREFIX = b'skypilot-capacity-evidence-v1\x00'
_SLOT_PREFIX = b'skypilot-capacity-slot-v1\x00'
_WORKSPACE_METRIC_PREFIX = b'skypilot-capacity-workspace-metric-v1\x00'
_LOWERCASE_SHA256 = re.compile(r'^[0-9a-f]{64}$')
_PILOT_END = re.compile(
    r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
_COMPONENT_NAMES = ('source_selectors', 'owner_kinds', 'providers', 'groups',
                    'verbs')


def _unsigned_64(value: int) -> bytes:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError('Length/count must be a non-negative integer.')
    try:
        return struct.pack('>Q', value)
    except struct.error as e:
        raise ValueError('Length/count exceeds unsigned 64-bit.') from e


def _validate_sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _LOWERCASE_SHA256.fullmatch(value) is None:
        raise ValueError(f'{field} must be a lowercase SHA-256 digest.')
    return value


def _normalize_pilot_end(value: object) -> str:
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None or value.utcoffset() != datetime.timedelta(0):
            raise ValueError('pilot_end_utc datetime must be UTC-aware.')
        if value.microsecond:
            raise ValueError('pilot_end_utc must not contain fractional time.')
        value = value.strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        value_bytes = value.encode('utf-8') if isinstance(value, str) else b''
    except UnicodeEncodeError as e:
        raise ValueError('pilot_end_utc must be valid UTF-8.') from e
    if (not isinstance(value, str) or len(value_bytes) != 20 or
            _PILOT_END.fullmatch(value) is None):
        raise ValueError('pilot_end_utc must use YYYY-MM-DDTHH:MM:SSZ.')
    try:
        datetime.datetime.strptime(
            value, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)
    except ValueError as e:
        raise ValueError('pilot_end_utc is not a valid UTC timestamp.') from e
    return value


def _scope_entry(component: str, value: object) -> bytes:
    if component not in _COMPONENT_NAMES:
        raise ValueError(f'Unknown scope component {component!r}.')
    return canonical.canonical_json_bytes(
        {
            'mapping_version': contracts.MAPPING_VERSION,
            'component': component,
            'value': value,
        },
        domain=canonical.CanonicalDomain.SCOPE_ENTRY)


def _component_digest(component: str,
                      values: Iterable[object]) -> tuple[int, str]:
    records = sorted(_scope_entry(component, value) for value in values)
    digest = hashlib.sha256()
    component_bytes = component.encode('utf-8')
    digest.update(_SCOPE_COMPONENT_PREFIX)
    digest.update(_unsigned_64(len(component_bytes)))
    digest.update(component_bytes)
    digest.update(_unsigned_64(len(records)))
    for record in records:
        digest.update(_unsigned_64(len(record)))
        digest.update(record)
    return len(records), digest.hexdigest()


def _closed_string(value: object, *, component: str) -> str:
    if isinstance(value, enum.Enum):
        value = value.value
    return canonical.validate_bounded_string(
        value,
        max_bytes=canonical.MAX_CANONICAL_STRING_BYTES,
        field=f'{component} scope value')


def _normalize_component_values(values: Iterable[object], *,
                                component: str) -> tuple[str, ...]:
    normalized = tuple(
        _closed_string(value, component=component) for value in values)
    if len(set(normalized)) != len(normalized):
        raise ValueError(f'{component} scope values must be unique.')
    return normalized


def source_partition_hash(partition: contracts.SourcePartition) -> str:
    """Hash the mapping-version-1 workspace/source-kind partition."""
    if not isinstance(partition, contracts.SourcePartition):
        raise TypeError('partition must be a SourcePartition.')
    return canonical.canonical_hash(
        partition.to_payload(),
        domain=canonical.CanonicalDomain.SOURCE_PARTITION)


def dependency_selectors_for_partition(
    selectors: Sequence[contracts.SourceSelector],
    partition: contracts.SourcePartition,
) -> tuple[contracts.SourceSelector, ...]:
    """Return the exact selector dependency set for one partition."""
    if not isinstance(partition, contracts.SourcePartition):
        raise TypeError('partition must be a SourcePartition.')
    normalized: list[contracts.SourceSelector] = []
    primary_count = 0
    for selector in selectors:
        selector_partition = contracts.selector_partition(selector)
        is_primary = selector_partition == partition
        is_pool_dependency = (
            partition.source_kind
            is models.ProjectionSourceKind.MANAGED_JOB_TASK and
            selector.workspace == partition.workspace and
            selector.source_kind is models.ProjectionSourceKind.SERVE_POOL)
        if is_primary:
            primary_count += 1
        if is_primary or is_pool_dependency:
            normalized.append(selector)
    if primary_count == 0:
        raise ValueError('Partition has no configured primary selector.')
    if len(set(normalized)) != len(normalized):
        raise ValueError('Source selectors must be unique.')
    return tuple(
        sorted(normalized,
               key=lambda selector: _scope_entry(
                   'source_selectors', contracts.selector_payload(selector))))


def projection_scope_payload(
        partition: contracts.SourcePartition,
        selectors: Sequence[contracts.SourceSelector],
        pilot_end_utc: str | datetime.datetime,
        *,
        owner_kinds: Iterable[object] = (),
        providers: Iterable[object] = (),
        groups: Iterable[object] = (),
        verbs: Iterable[object] = (),
) -> dict[str, object]:
    """Build the compact, bounded scope payload for one partition."""
    dependencies = dependency_selectors_for_partition(selectors, partition)
    component_values: dict[str, tuple[object, ...]] = {
        'source_selectors': tuple(
            contracts.selector_payload(selector) for selector in dependencies),
        'owner_kinds': _normalize_component_values(owner_kinds,
                                                   component='owner_kinds'),
        'providers': _normalize_component_values(providers,
                                                 component='providers'),
        'groups': _normalize_component_values(groups, component='groups'),
        'verbs': _normalize_component_values(verbs, component='verbs'),
    }
    components: dict[str, object] = {}
    for component in _COMPONENT_NAMES:
        count, digest = _component_digest(component,
                                          component_values[component])
        components[component] = {'count': count, 'hash': digest}
    return {
        'mapping_version': contracts.MAPPING_VERSION,
        'workspace': partition.workspace,
        'source_kind': partition.source_kind.value,
        'pilot_end_utc': _normalize_pilot_end(pilot_end_utc),
        **components,
    }


def projection_scope_hash(
        partition: contracts.SourcePartition,
        selectors: Sequence[contracts.SourceSelector],
        pilot_end_utc: str | datetime.datetime,
        *,
        owner_kinds: Iterable[object] = (),
        providers: Iterable[object] = (),
        groups: Iterable[object] = (),
        verbs: Iterable[object] = (),
) -> str:
    """Hash one partition's exact selector/allowlist/pilot dependency set."""
    payload = projection_scope_payload(partition,
                                       selectors,
                                       pilot_end_utc,
                                       owner_kinds=owner_kinds,
                                       providers=providers,
                                       groups=groups,
                                       verbs=verbs)
    return canonical.canonical_hash(
        payload, domain=canonical.CanonicalDomain.SOURCE_PARTITION)


def slot_jitter_seconds(partition_hash: str) -> int:
    """Return the deterministic inclusive 0..60 second partition jitter."""
    normalized_hash = _validate_sha256(partition_hash,
                                       field='source_partition_hash')
    digest = hashlib.sha256(_SLOT_PREFIX + normalized_hash.encode('ascii'))
    return int.from_bytes(digest.digest()[:8], byteorder='big') % 61


def evidence_inventory_digest(
        records: Iterable[contracts.EvidenceRecord]) -> str:
    """Compute the order-independent, duplicate-preserving evidence digest."""
    encoded_records: list[tuple[bytes, bytes, bytes]] = []
    for record in records:
        if not isinstance(record,
                          (contracts.GroupEvidenceRecord,
                           contracts.AllocationCandidateEvidenceRecord)):
            raise TypeError('records must contain typed evidence DTOs.')
        encoded = canonical.canonical_json_bytes(
            record.to_payload(),
            domain=canonical.CanonicalDomain.EVIDENCE_RECORD)
        encoded_records.append(
            (record.record_type.value.encode('utf-8'),
             record.source_incarnation_hash.encode('ascii'), encoded))
    encoded_records.sort()
    digest = hashlib.sha256()
    digest.update(_EVIDENCE_PREFIX)
    digest.update(_unsigned_64(len(encoded_records)))
    for _, _, encoded in encoded_records:
        digest.update(_unsigned_64(len(encoded)))
        digest.update(encoded)
    return digest.hexdigest()


def workspace_metric_hash(workspace: str) -> str:
    """Hash a workspace into the sole permitted workspace metric label."""
    normalized = contracts.validate_workspace(workspace)
    return hashlib.sha256(_WORKSPACE_METRIC_PREFIX +
                          normalized.encode('utf-8')).hexdigest()
