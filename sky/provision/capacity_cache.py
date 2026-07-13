"""Short-lived, process-shared hints for AWS capacity and quota storms."""
from collections.abc import Iterable
import json
import time
from typing import NamedTuple

from sky.utils.db import kv_cache

_CACHE_KEY_PREFIX = 'aws:capacity_exhausted:v1:'
_QUOTA_COOLDOWN_KEY_PREFIX = 'aws:quota_cooldown:v1:'
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


def _cache_key(key: ResourceKey) -> str:
    payload = json.dumps(key, separators=(',', ':'))
    return f'{_CACHE_KEY_PREFIX}{payload}'


def _quota_cooldown_cache_key(key: QuotaCooldownKey) -> str:
    payload = json.dumps(key, separators=(',', ':'))
    return f'{_QUOTA_COOLDOWN_KEY_PREFIX}{payload}'


def mark_exhausted(key: ResourceKey) -> None:
    """Marks ``key`` exhausted for a short, bounded period."""
    kv_cache.add_or_extend_cache_entry(_cache_key(key), '1',
                                       time.time() + _CAPACITY_TTL_SECONDS)


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


def mark_quota_failure(key: QuotaCooldownKey) -> None:
    """Starts or extends a brief cooldown after an exact quota failure."""
    kv_cache.add_or_extend_cache_entry(
        _quota_cooldown_cache_key(key), '1',
        time.time() + _QUOTA_COOLDOWN_TTL_SECONDS)


def is_quota_cooldown_active(key: QuotaCooldownKey) -> bool:
    """Returns whether ``key`` is still in its brief quota cooldown."""
    return kv_cache.get_cache_entry(_quota_cooldown_cache_key(key)) is not None


def clear_quota_cooldown(key: QuotaCooldownKey) -> None:
    """Clears the exact quota cooldown after a successful provision."""
    kv_cache.delete_cache_entry(_quota_cooldown_cache_key(key))
