"""Low-cardinality metrics for the temporary capacity evidence pilot."""

from typing import Mapping

import prometheus_client as prom

from sky.physical_capacity import hashing
from sky.physical_capacity import models

_SOURCE_KINDS = frozenset(kind.value for kind in models.ProjectionSourceKind)
_FINDING_KEYS = frozenset({
    'source_rows',
    'selectors_present',
    'selectors_missing',
    'groups_exact',
    'groups_legacy',
    'groups_unknown',
    'allocation_candidates',
    'allocations_exact',
    'allocations_legacy',
    'allocations_unknown',
    'identity_gap',
    'no_cluster_yet',
    'scalar_placement_known',
    'selected_spec_gap',
    'desired_present',
    'desired_absent',
    'desired_unknown',
    'source_conflict',
    'pool_assignment_unfenced',
    'pool_assignment_ambiguous',
})

SCAN_DURATION_SECONDS = prom.Histogram(
    'skypilot_physical_capacity_scan_duration_seconds',
    'Physical-capacity evidence scan duration.',
    ('source_kind', 'workspace_hash'))
SCAN_LAG_SECONDS = prom.Gauge(
    'skypilot_physical_capacity_scan_lag_seconds',
    'Lag from a scheduled evidence-scan slot to completion.',
    ('source_kind', 'workspace_hash'),
    multiprocess_mode='livemax')
SCAN_ROWS = prom.Gauge('skypilot_physical_capacity_scan_rows',
                       'Rows observed by the latest committed evidence scan.',
                       ('source_kind', 'workspace_hash'),
                       multiprocess_mode='livemax')
SCAN_SUCCESSES_TOTAL = prom.Counter(
    'skypilot_physical_capacity_scan_successes_total',
    'Successful physical-capacity evidence scans.',
    ('source_kind', 'workspace_hash'))
SCAN_FAILURES_TOTAL = prom.Counter(
    'skypilot_physical_capacity_scan_failures_total',
    'Failed physical-capacity evidence scans.',
    ('source_kind', 'workspace_hash'))
DIGEST_CHANGES_TOTAL = prom.Counter(
    'skypilot_physical_capacity_digest_changes_total',
    'Committed evidence-inventory digest changes.',
    ('source_kind', 'workspace_hash'))
FINDINGS = prom.Gauge('skypilot_physical_capacity_findings',
                      'Finding counts from the latest committed evidence scan.',
                      ('source_kind', 'workspace_hash', 'finding'),
                      multiprocess_mode='livemax')
PROJECTOR_HEALTH = prom.Gauge(
    'skypilot_physical_capacity_projector_healthy',
    'Whether the controller-owned evidence projector is healthy.',
    multiprocess_mode='livemax')
PILOT_EXPIRED = prom.Gauge(
    'skypilot_physical_capacity_pilot_expired',
    'Whether the immutable evidence pilot end has passed.',
    multiprocess_mode='livemax')


def workspace_hash(workspace: str) -> str:
    """Return the only workspace representation permitted in metric labels."""
    return hashing.workspace_metric_hash(workspace)


def _validate_source_kind(source_kind: str) -> None:
    if source_kind not in _SOURCE_KINDS:
        raise ValueError(f'Unknown physical-capacity source kind: '
                         f'{source_kind!r}.')


def record_scan(*,
                workspace: str,
                source_kind: str,
                succeeded: bool,
                duration_seconds: float,
                lag_seconds: float,
                rows_seen: int = 0,
                findings: Mapping[str, int] | None = None,
                digest_changed: bool = False) -> None:
    """Record one terminal scan using only closed, bounded labels."""
    _validate_source_kind(source_kind)
    label_hash = workspace_hash(workspace)
    labels = (source_kind, label_hash)
    SCAN_DURATION_SECONDS.labels(*labels).observe(max(0.0, duration_seconds))
    if succeeded:
        SCAN_SUCCESSES_TOTAL.labels(*labels).inc()
    else:
        SCAN_FAILURES_TOTAL.labels(*labels).inc()
    SCAN_LAG_SECONDS.labels(*labels).set(max(0.0, lag_seconds))
    SCAN_ROWS.labels(*labels).set(max(0, rows_seen))
    if digest_changed:
        DIGEST_CHANGES_TOTAL.labels(*labels).inc()
    provided = {} if findings is None else findings
    unknown = set(provided) - _FINDING_KEYS
    if unknown:
        raise ValueError('Unknown physical-capacity finding(s): '
                         f'{", ".join(sorted(unknown))}.')
    for finding in _FINDING_KEYS:
        FINDINGS.labels(*labels, finding).set(max(0, provided.get(finding, 0)))


def set_projector_health(*, healthy: bool, expired: bool) -> None:
    """Publish process-local projector health and immutable-pilot expiry."""
    PROJECTOR_HEALTH.set(1 if healthy else 0)
    PILOT_EXPIRED.set(1 if expired else 0)
