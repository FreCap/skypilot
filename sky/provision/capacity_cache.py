"""Short-lived, process-shared hints for exhausted AWS spot capacity."""
import json
import time
from typing import Iterable, NamedTuple, Set

from sky.utils.db import kv_cache

_CACHE_KEY_PREFIX = 'aws:capacity_exhausted:v1:'
_CAPACITY_TTL_SECONDS = 120


class ResourceKey(NamedTuple):
    """The exact AWS spot shape covered by an exhaustion hint."""

    account: str
    region: str
    zone: str
    instance_type: str
    num_nodes: int


def _cache_key(key: ResourceKey) -> str:
    payload = json.dumps(key, separators=(',', ':'))
    return f'{_CACHE_KEY_PREFIX}{payload}'


def mark_exhausted(key: ResourceKey) -> None:
    """Marks ``key`` exhausted for a short, bounded period."""
    kv_cache.add_or_update_cache_entry(_cache_key(key), '1',
                                       time.time() + _CAPACITY_TTL_SECONDS)


def active_exhausted_keys(
        candidates: Iterable[ResourceKey]) -> Set[ResourceKey]:
    """Returns candidates with an unexpired exhaustion hint."""
    return {
        key for key in candidates
        if kv_cache.get_cache_entry(_cache_key(key)) is not None
    }
