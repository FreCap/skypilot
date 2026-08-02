"""Low-cardinality telemetry for the SkyServe system-OOM rollout."""

import typing

import prometheus_client as prom

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers

EVENTS = frozenset({
    'authorization_v3_candidate',
    'authorization_v3_ordinary',
    'authorization_v3_capable',
    'recovery_started',
    'recovery_succeeded',
    'recovery_exhausted',
    'evidence_lost',
    'preemption_observed',
})
PROVIDERS = frozenset({'aws', 'gcp', 'kubernetes', 'other', 'unknown'})
MARKETS = frozenset({'on_demand', 'spot', 'other', 'unknown'})

SYSTEM_OOM_RECOVERY_EVENTS = prom.Counter(
    'sky_serve_system_oom_recovery_events_total',
    'Bounded SkyServe system-OOM recovery rollout events.',
    ('event', 'provider', 'market'))


def record(event: str,
           *,
           provider: str = 'unknown',
           market: str = 'unknown') -> None:
    """Increment one closed, nonsecret label tuple."""
    if event not in EVENTS:
        raise ValueError(f'Unknown system-OOM recovery event: {event!r}')
    if provider not in PROVIDERS:
        provider = 'other'
    if market not in MARKETS:
        market = 'other'
    SYSTEM_OOM_RECOVERY_EVENTS.labels(event=event,
                                      provider=provider,
                                      market=market).inc()


def labels_for_replica(info: 'replica_managers.ReplicaInfo') -> tuple[str, str]:
    """Derive bounded provider/market labels from persisted placement."""
    location = info.location
    cloud = location.get('cloud') if isinstance(location, dict) else None
    if cloud is None:
        resources_override = info.resources_override or {}
        cloud = resources_override.get('cloud')
    provider = str(cloud).lower() if cloud is not None else 'unknown'
    if provider not in PROVIDERS:
        provider = 'other'
    market = 'spot' if info.is_spot else 'on_demand'
    return provider, market


def record_for_replica(event: str,
                       info: 'replica_managers.ReplicaInfo') -> None:
    provider, market = labels_for_replica(info)
    record(event, provider=provider, market=market)
