"""Short-lived, process-shared hints for AWS capacity and quota storms."""
from collections.abc import Iterable
import hashlib
import json
import time
from typing import NamedTuple

from sky.utils.db import kv_cache

_CACHE_KEY_PREFIX = 'aws:capacity_exhausted:v1:'
_QUOTA_COOLDOWN_KEY_PREFIX = 'aws:quota_cooldown:v1:'
_CAPACITY_OBSERVATION_KEY_PREFIX = 'aws:capacity_observation:v1:'
_QUOTA_OBSERVATION_KEY_PREFIX = 'aws:quota_observation:v1:'
_CAPACITY_TTL_SECONDS = 120
# Quota may recover as soon as sibling instances terminate. Keep this fixed and
# deliberately brief: after the last recorded failure, it trades at most 15
# seconds of delayed re-probing for cross-worker suppression of an otherwise
# immediate external retry storm.
_QUOTA_COOLDOWN_TTL_SECONDS = 15


class ResourceKey(NamedTuple):
    """The exact AWS spot shape covered by an exhaustion hint."""

    account: str
    region: str
    zone: str
    instance_type: str
    num_nodes: int


class QuotaCooldownKey(NamedTuple):
    """An exact AWS Spot demand covered by a brief quota cooldown."""

    account: str
    region: str
    instance_type: str
    num_nodes: int


class ServiceObservation(NamedTuple):
    """The exact Serve incarnation that observed a provider failure."""

    service_name: str
    service_hash: str


def _cache_key(key: ResourceKey) -> str:
    payload = json.dumps(key, separators=(',', ':'))
    return f'{_CACHE_KEY_PREFIX}{payload}'


def _quota_cooldown_cache_key(key: QuotaCooldownKey) -> str:
    payload = json.dumps(key, separators=(',', ':'))
    return f'{_QUOTA_COOLDOWN_KEY_PREFIX}{payload}'


def _service_observation_prefix(prefix: str, service_name: str) -> str:
    # Key by the stable service name so recreating a service does not create a
    # permanently new prefix. The value still carries and validates the exact
    # incarnation hash, so stale observations cannot cross incarnations.
    digest = hashlib.sha256(service_name.encode('utf-8')).hexdigest()
    return f'{prefix}{digest}:'


def _service_observation_entry(
    prefix: str,
    observation: ServiceObservation,
    kind: str,
    canonical_key: str,
    resource: tuple,
    expires_at: float,
) -> tuple[str, str, float]:
    observation_prefix = _service_observation_prefix(prefix,
                                                     observation.service_name)
    canonical_digest = hashlib.sha256(canonical_key.encode('utf-8')).hexdigest()
    value = json.dumps(
        {
            'version': 1,
            'kind': kind,
            'service_name': observation.service_name,
            'service_hash': observation.service_hash,
            'canonical_key': canonical_key,
            'resource': list(resource),
            'observed_at': time.time(),
        },
        separators=(',', ':'))
    return (f'{observation_prefix}{canonical_digest}', value, expires_at)


def mark_exhausted(key: ResourceKey,
                   observation: ServiceObservation | None = None) -> None:
    """Marks ``key`` exhausted for a short, bounded period."""
    expires_at = time.time() + _CAPACITY_TTL_SECONDS
    canonical_key = _cache_key(key)
    entries = [(canonical_key, '1', expires_at)]
    if observation is not None:
        entries.append(
            _service_observation_entry(_CAPACITY_OBSERVATION_KEY_PREFIX,
                                       observation, 'capacity', canonical_key,
                                       key, expires_at))
    kv_cache.add_or_extend_cache_entries(entries)


def active_exhausted_keys(
        candidates: Iterable[ResourceKey]) -> set[ResourceKey]:
    """Returns candidates with an unexpired exhaustion hint."""
    return {
        key for key in candidates
        if kv_cache.get_cache_entry(_cache_key(key)) is not None
    }


def clear(key: ResourceKey) -> None:
    """Clears the exact capacity hint after a successful provision."""
    kv_cache.delete_cache_entry(_cache_key(key))


def mark_quota_failure(key: QuotaCooldownKey,
                       observation: ServiceObservation | None = None) -> None:
    """Starts or extends a brief cooldown after an exact quota failure."""
    expires_at = time.time() + _QUOTA_COOLDOWN_TTL_SECONDS
    canonical_key = _quota_cooldown_cache_key(key)
    entries = [(canonical_key, '1', expires_at)]
    if observation is not None:
        entries.append(
            _service_observation_entry(_QUOTA_OBSERVATION_KEY_PREFIX,
                                       observation, 'quota', canonical_key, key,
                                       expires_at))
    kv_cache.add_or_extend_cache_entries(entries)


def is_quota_cooldown_active(key: QuotaCooldownKey) -> bool:
    """Returns whether ``key`` is still in its brief quota cooldown."""
    return kv_cache.get_cache_entry(_quota_cooldown_cache_key(key)) is not None


def clear_quota_cooldown(key: QuotaCooldownKey) -> None:
    """Clears the exact quota cooldown after a successful provision."""
    kv_cache.delete_cache_entry(_quota_cooldown_cache_key(key))


def active_service_observations(service_name: str,
                                service_hash: str,
                                limit: int = 100) -> dict:
    """Return redacted, active AWS hints observed by one Serve incarnation."""
    if (not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 or
            limit > 200):
        raise ValueError('limit must be an integer from 1 to 200.')
    capacity_prefix = _service_observation_prefix(
        _CAPACITY_OBSERVATION_KEY_PREFIX, service_name)
    quota_prefix = _service_observation_prefix(_QUOTA_OBSERVATION_KEY_PREFIX,
                                               service_name)
    rows = kv_cache.list_active_cache_entries_by_prefix(capacity_prefix,
                                                        limit + 1)
    rows.extend(
        kv_cache.list_active_cache_entries_by_prefix(quota_prefix, limit + 1))

    parsed = []
    canonical_keys = []
    for _, value, shadow_expiry in rows:
        try:
            payload = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            continue
        if (not isinstance(payload, dict) or payload.get('version') != 1 or
                payload.get('service_name') != service_name or
                payload.get('service_hash') != service_hash or
                payload.get('kind') not in ('capacity', 'quota') or
                not isinstance(payload.get('canonical_key'), str) or
                not isinstance(payload.get('resource'), list)):
            continue
        parsed.append((payload, shadow_expiry))
        canonical_keys.append(payload['canonical_key'])

    canonical_entries = kv_cache.get_active_cache_entries(canonical_keys)
    hints = []
    for payload, shadow_expiry in parsed:
        canonical = canonical_entries.get(payload['canonical_key'])
        if canonical is None:
            continue
        _, canonical_expiry = canonical
        resource = payload['resource']
        kind = payload['kind']
        expected_length = 5 if kind == 'capacity' else 4
        if len(resource) != expected_length:
            continue
        # resource[0] is the AWS account. It is intentionally omitted.
        if kind == 'capacity':
            _, region, zone, instance_type, num_nodes = resource
        else:
            _, region, instance_type, num_nodes = resource
            zone = None
        hints.append({
            'kind': kind,
            'region': region,
            'zone': zone,
            'instance_type': instance_type,
            'num_nodes': num_nodes,
            'observed_at': payload.get('observed_at'),
            'expires_at': min(shadow_expiry, canonical_expiry),
        })

    hints.sort(key=lambda hint: (hint['expires_at'], hint['kind']))
    truncated = len(hints) > limit
    return {
        'available': True,
        'hints': hints[:limit],
        'truncated': truncated,
    }
