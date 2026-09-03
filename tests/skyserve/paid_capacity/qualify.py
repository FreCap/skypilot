"""Read-only paid-Spot qualification shared by small and scale profiles.

This module never creates or deletes infrastructure.  Billable runs must use
``lifecycle.py``, whose finalizer owns normal ``sky serve down`` and exact
cleanup evidence.

Examples::

    python tests/skyserve/paid_capacity/lifecycle.py \
      --profile small --service-name paid-e2e \
      --artifacts-dir /tmp/paid-e2e-artifacts

The data-plane bearer is read from ``SKYPILOT_SERVE_E2E_AUTH_TOKEN`` when an
external runner explicitly supplies it.  In an API-server pod, the normal
projected data-plane token ring is used instead.  ``SKYPILOT_DB_CONNECTION_URI``
must name the same PostgreSQL database as the API server.  No credential is
emitted into the receipt.  Provider census uses the Google Compute v1 and AWS
EC2 APIs with the committed service version's credentials; no cloud CLI is
required in the API-server image.
"""

import argparse
import asyncio
import collections.abc
import concurrent.futures
import copy
import dataclasses
import datetime
import enum
import hashlib
import json
import math
import os
import pathlib
import re
import time
import typing
from typing import Any
import uuid

import aiohttp
import rfc8785
import sqlalchemy
import yaml

from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.adaptors import gcp as gcp_adaptor
from sky.provision import constants as provision_constants
from sky.provision.gcp import instance_utils
from sky.serve import async_request_ledger
from sky.serve import auth_tokens
from sky.serve import constants as serve_constants
from sky.serve import demand_state
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.server.requests import non_pool_launch
from sky.server.requests import postgres as request_postgres

aws_adaptor = adaptors_common.LazyImport('sky.adaptors.aws')


class QualificationError(RuntimeError):
    """A qualification invariant failed."""


class GuardViolation(QualificationError):
    """An authoritative market, card, cap, or identity guard failed."""


_PROVIDER_SCOPE_SCHEMA_VERSION = 6
_QUALIFICATION_RECEIPT_SCHEMA_VERSION = 11
_CLEANUP_RECEIPT_SCHEMA_VERSION = 2
_AGGREGATE_RECEIPT_SCHEMA_VERSION = 3
_CAMPAIGN_LOAD_WINDOW_SECONDS = (
    serve_constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS)
_AWS_CENSUS_MAX_WORKERS = 8
_AWS_FILTER_MAX_VALUES = 200


@dataclasses.dataclass(frozen=True, kw_only=True)
class Profile:
    """Bounded parameters for one qualification scale."""

    name: str
    max_replicas: int
    max_units: int
    minimum_running: int
    exact_requests: int
    request_concurrency: int
    request_queue_timeout_seconds: int
    scale_timeout_seconds: float
    scale_slo_seconds: float
    drain_timeout_seconds: float
    zero_hold_seconds: float
    poll_seconds: float
    scale_up_min_replicas: int
    scale_up_period_seconds: int


PROFILES = {
    'small': Profile(name='small',
                     max_replicas=2,
                     max_units=2,
                     minimum_running=2,
                     exact_requests=16,
                     request_concurrency=4,
                     request_queue_timeout_seconds=600,
                     scale_timeout_seconds=15 * 60,
                     scale_slo_seconds=5 * 60,
                     drain_timeout_seconds=20 * 60,
                     zero_hold_seconds=6 * 60,
                     poll_seconds=5,
                     scale_up_min_replicas=2,
                     scale_up_period_seconds=10),
    'scale': Profile(name='scale',
                     max_replicas=800,
                     max_units=800,
                     minimum_running=100,
                     exact_requests=10_000,
                     request_concurrency=128,
                     request_queue_timeout_seconds=600,
                     scale_timeout_seconds=15 * 60,
                     scale_slo_seconds=5 * 60,
                     drain_timeout_seconds=30 * 60,
                     zero_hold_seconds=6 * 60,
                     poll_seconds=10,
                     scale_up_min_replicas=800,
                     scale_up_period_seconds=10),
    # The provider canary uses the same exact one-L4 task shape as the economic
    # run and permits one physical backend without naming an instance type.
    'provider-canary': Profile(name='provider-canary',
                               max_replicas=1,
                               max_units=1,
                               minimum_running=1,
                               exact_requests=1,
                               request_concurrency=1,
                               request_queue_timeout_seconds=600,
                               scale_timeout_seconds=15 * 60,
                               scale_slo_seconds=5 * 60,
                               drain_timeout_seconds=20 * 60,
                               zero_hold_seconds=6 * 60,
                               poll_seconds=5,
                               scale_up_min_replicas=1,
                               scale_up_period_seconds=10),
}


def request_processing_seconds(profile: Profile) -> float:
    """Keep synthetic work observable across at least two polling ticks."""
    return max(10.0, 2 * profile.poll_seconds)


def request_queue_max_concurrency(profile: Profile) -> int:
    """Return the fixture's bounded HTTP admission concurrency."""
    return max(8, min(128, profile.request_concurrency))


def scale_stimulus_count(profile: Profile) -> int:
    """Return the bounded cohort sufficient to request the configured cap."""
    return min(profile.max_units, profile.exact_requests)


@dataclasses.dataclass(frozen=True, kw_only=True)
class _ScaleArrivalAttributionState:
    """Last complete rolling-arrival projection bound to the scale cohort."""

    unique_job_arrivals_60s: int
    unique_job_arrivals_300s: int
    campaign_offered: int
    campaign_succeeded: int


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExactRequestCampaignCounters:
    """One lock-consistent projection of immutable campaign progress."""

    offered: int
    succeeded: int


@dataclasses.dataclass(kw_only=True)
class ExactRequestCampaignProgress:
    """Serialize the sliding window independently of proof observers."""

    total_count: int
    window_size: int
    _offered: int = dataclasses.field(default=0, init=False, repr=False)
    _succeeded: int = dataclasses.field(default=0, init=False, repr=False)
    _lock: asyncio.Lock = dataclasses.field(default_factory=asyncio.Lock,
                                            init=False,
                                            repr=False)

    def __post_init__(self) -> None:
        if (type(self.total_count) is not int or self.total_count < 1 or
                type(self.window_size) is not int or self.window_size < 1 or
                self.window_size > self.total_count):
            raise ValueError('Exact request campaign progress is invalid.')

    async def mark_offered(self) -> None:
        """Record a never-before-offered ID immediately before its first POST."""
        async with self._lock:
            if self._offered >= self.total_count:
                raise QualificationError(
                    'Exact request campaign offered too many identities.')
            if self._offered - self._succeeded >= self.window_size:
                raise QualificationError(
                    'Exact request campaign exceeded its active window.')
            self._offered += 1

    async def mark_succeeded(self) -> None:
        """Record one exact terminal receipt before the worker takes another ID."""
        async with self._lock:
            if self._succeeded >= self._offered:
                raise QualificationError(
                    'Exact request campaign terminal order is invalid.')
            self._succeeded += 1

    async def snapshot(self) -> ExactRequestCampaignCounters:
        """Return a read-only atomic view for an evidence observer."""
        async with self._lock:
            return ExactRequestCampaignCounters(offered=self._offered,
                                                succeeded=self._succeeded)


def _next_scale_arrival_attribution_state(
    *,
    previous: _ScaleArrivalAttributionState | None,
    unique_job_arrivals_60s: object,
    unique_job_arrivals_300s: object,
    headerless_arrivals_60s: object,
    headerless_arrivals_300s: object,
    offered_arrival_tracking_saturated: object,
    initial_arrivals: int,
    maximum_arrivals: int,
    campaign_offered: object,
    campaign_succeeded: object,
) -> _ScaleArrivalAttributionState | None:
    """Bound rolling arrivals by the campaign's exact terminal frontier."""
    if (previous is not None and
            not isinstance(previous, _ScaleArrivalAttributionState)):
        return None
    counters = (unique_job_arrivals_60s, unique_job_arrivals_300s,
                headerless_arrivals_60s, headerless_arrivals_300s)
    if (type(initial_arrivals) is not int or initial_arrivals <= 0 or
            type(maximum_arrivals) is not int or
            maximum_arrivals < initial_arrivals or
            type(campaign_offered) is not int or
            type(campaign_succeeded) is not int or
            any(type(value) is not int for value in counters) or
            offered_arrival_tracking_saturated is not False):
        return None
    assert isinstance(unique_job_arrivals_60s, int)
    assert isinstance(unique_job_arrivals_300s, int)
    assert isinstance(headerless_arrivals_60s, int)
    assert isinstance(headerless_arrivals_300s, int)
    assert isinstance(campaign_offered, int)
    assert isinstance(campaign_succeeded, int)
    terminal_frontier = min(maximum_arrivals,
                            initial_arrivals + campaign_succeeded)
    if (not 0 <= campaign_succeeded <= campaign_offered <= terminal_frontier or
            campaign_offered < initial_arrivals or
            not 0 <= unique_job_arrivals_60s <= unique_job_arrivals_300s <=
            campaign_offered or headerless_arrivals_60s != 0 or
            headerless_arrivals_300s != 0):
        return None
    if (previous is not None and
        (campaign_offered < previous.campaign_offered or
         campaign_succeeded < previous.campaign_succeeded)):
        return None
    # Later rolling counters may rise when a natural terminal success exposes
    # the next never-before-offered identity, or fall as old arrivals age out.
    # The driver-owned terminal frontier—not campaign cardinality alone—is the
    # hard upper bound. Exact final ledger evidence independently requires all
    # immutable identities and no others to reach SUCCEEDED.
    return _ScaleArrivalAttributionState(
        unique_job_arrivals_60s=unique_job_arrivals_60s,
        unique_job_arrivals_300s=unique_job_arrivals_300s,
        campaign_offered=campaign_offered,
        campaign_succeeded=campaign_succeeded)


def positive_telemetry_window_seconds(profile: Profile) -> float:
    """Return the profile-specific first-dispatch observation window."""
    if profile.name != 'scale':
        return max(2 * 60, 4 * profile.poll_seconds)
    observation_margin = max(1.0, profile.poll_seconds)
    queue_attempt_window = (profile.request_queue_timeout_seconds -
                            observation_margin)
    if not math.isfinite(queue_attempt_window) or queue_attempt_window <= 0:
        raise ValueError(
            'Request queue timeout has no positive telemetry polling margin.')
    # Provider convergence and application readiness are independent clocks.
    # The resident stimulus retries only after the load balancer returns its
    # typed REJECTED_PRE_DISPATCH receipt, so allow the provider its complete
    # qualification window followed by one complete queue attempt.  This does
    # not extend any individual request's configured queue deadline.
    window = profile.scale_timeout_seconds + queue_attempt_window
    if not math.isfinite(window) or window <= 0:
        raise ValueError(
            'Request queue timeout has no positive telemetry polling margin.')
    return window


def positive_telemetry_deadline_monotonic(
        profile: Profile, *, scale_started_monotonic: float) -> float:
    """Return an absolute first-dispatch deadline for one scale campaign."""
    if (not math.isfinite(scale_started_monotonic) or
            scale_started_monotonic < 0):
        raise ValueError('Scale start must be a finite monotonic timestamp.')
    return (scale_started_monotonic +
            positive_telemetry_window_seconds(profile))


class ExpectationKind(str, enum.Enum):
    """Placement fact one qualification run is allowed to prove."""

    ECONOMIC = 'economic'
    PROVIDER_CANARY = 'provider-canary'


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderExpectation:
    """Typed evidence gate that never participates in runtime placement."""

    kind: ExpectationKind
    providers: tuple[str, ...]
    minimum_physical_running: int
    exact_request_count: int

    def __post_init__(self) -> None:
        if (self.providers not in (('aws',), ('gcp',), ('aws', 'gcp')) or
                type(self.minimum_physical_running) is not int or
                self.minimum_physical_running < 1 or
                type(self.exact_request_count) is not int or
                self.exact_request_count < 1 or
            (self.kind is ExpectationKind.ECONOMIC and
             self.providers != ('aws', 'gcp')) or
            (self.kind is ExpectationKind.PROVIDER_CANARY and
             len(self.providers) != 1)):
            raise ValueError('Paid-provider expectation is malformed.')

    @property
    def requires_full_request_telemetry(self) -> bool:
        return self.kind is ExpectationKind.ECONOMIC


def provider_expectation(profile: Profile,
                         provider: str | None) -> ProviderExpectation:
    """Build the only two supported qualification evidence contracts."""
    if profile.name == 'provider-canary':
        if provider not in ('aws', 'gcp'):
            raise QualificationError(
                'Provider-canary profile requires --provider={aws,gcp}.')
        kind = ExpectationKind.PROVIDER_CANARY
        providers = (provider,)
    else:
        if provider is not None:
            raise QualificationError(
                'Economic profiles do not accept a provider override.')
        kind = ExpectationKind.ECONOMIC
        providers = ('aws', 'gcp')
    return ProviderExpectation(kind=kind,
                               providers=providers,
                               minimum_physical_running=profile.minimum_running,
                               exact_request_count=profile.exact_requests)


_AUTH_HEADER = 'X-SkyPilot-Serve-Authorization'
_JOB_ID_HEADER = 'X-SkyServe-Job-Id'
_PRIORITY_HEADER = 'X-SkyServe-Priority'
_ACCELERATORS_HEADER = 'X-SkyServe-Compatible-Accelerators'
_REQUEST_PRIORITY = 50
_GCP_LIST_PAGE_SIZE = 500
_GCP_API_RETRIES = 3
_RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
_ENDPOINT_AUTHENTICATION_TIMEOUT_SECONDS = 5 * 60
_ENDPOINT_AUTHENTICATION_POLL_SECONDS = 2
_ENDPOINT_AUTHENTICATION_REQUEST_TIMEOUT_SECONDS = 15
_ASYNC_ACTIVE_STATES = frozenset({
    'DISPATCH_MAY_HAVE_OCCURRED',
    'ACCEPTED',
    'AMBIGUOUS',
})
_ACTIVE_DEMAND_NAMES = frozenset({
    'async_occupancy',
    'busy_replicas',
    'http_in_flight',
    'in_flight',
    'in_flight_by_accelerator',
    'local_in_flight',
    'offered_arrivals',
    'queue_depth',
    'queue_depth_by_priority',
    'queued_request_deadline_buckets',
    'queued_requests_by_compatibility',
    'rejected_in_recent_window',
    'rejected_in_recent_window_by_priority',
    'rejected_in_window',
    'rejected_in_window_by_priority',
    'rejected_requests_by_compatibility',
    'request_queue_depth',
    'retry_handler_depth',
    'running_count',
    'running_slots',
})


def _basename(value: object) -> str:
    return str(value or '').rstrip('/').rsplit('/', 1)[-1]


def _normalized_operation_group_id(
        operation: collections.abc.Mapping[str, Any]) -> str | None:
    """Return one provider-issued GCP operation lineage identifier."""
    raw_group_id = operation.get('operationGroupId')
    if not isinstance(raw_group_id, str):
        return None
    try:
        return str(uuid.UUID(raw_group_id))
    except ValueError:
        return None


@dataclasses.dataclass(frozen=True, kw_only=True)
class _GcpInsertLineage:
    """Exact child target that proves one service-owned insert lineage."""

    target_link: str
    target_name: str
    operation_group_id: str | None


def _scoped_inflight_gcp_insert_lineages(
    service_name: str,
    operations: collections.abc.Sequence[object],
) -> tuple[_GcpInsertLineage, ...]:
    """Reduce child inserts and their project-scoped bulk parents once."""
    scoped_children: list[_GcpInsertLineage] = []
    active_lineages: list[_GcpInsertLineage] = []
    active_child_groups: set[str] = set()
    for operation in operations:
        if (not isinstance(operation, dict) or
                not str(operation.get('operationType',
                                      '')).casefold().endswith('insert') or
                not _generated_name_in_service_scope(
                    operation.get('targetLink'), service_name)):
            continue
        target_link = str(operation.get('targetLink', ''))
        lineage = _GcpInsertLineage(
            target_link=target_link,
            target_name=_basename(target_link),
            operation_group_id=_normalized_operation_group_id(operation))
        scoped_children.append(lineage)
        if str(operation.get('status', '')).upper() == 'DONE':
            continue
        active_lineages.append(lineage)
        if lineage.operation_group_id is not None:
            active_child_groups.add(lineage.operation_group_id)

    service_operation_group_ids = {
        child.operation_group_id
        for child in scoped_children
        if child.operation_group_id is not None
    }
    active_parent_groups: set[str] = set()
    for operation in operations:
        if (not isinstance(operation, dict) or
                str(operation.get('operationType',
                                  '')).casefold() != 'bulkinsert' or
                str(operation.get('status', '')).upper() == 'DONE'):
            continue
        operation_group_id = _normalized_operation_group_id(operation)
        if (operation_group_id in service_operation_group_ids and
                operation_group_id not in active_child_groups):
            active_parent_groups.add(operation_group_id)

    # A terminal child remains immutable lineage evidence while its parent is
    # active.  If a child is itself active, it is already present above and the
    # parent must not add a second effect.
    active_lineages.extend(child for child in scoped_children
                           if child.operation_group_id in active_parent_groups)
    return tuple(active_lineages)


def _numeric_total(value: object) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, (int, float)):
        if value < 0:
            raise QualificationError(
                'Demand evidence contains a negative value.')
        return int(value)
    if isinstance(value, dict):
        return sum(_numeric_total(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return sum(_numeric_total(item) for item in value)
    return 0


def demand_units(payload: object) -> int:
    """Return a conservative positive/zero projection of one LB report."""
    if not isinstance(payload, dict):
        raise QualificationError('Demand report payload is not an object.')
    total = 0
    for key, value in payload.items():
        normalized = key.casefold()
        if (normalized in _ACTIVE_DEMAND_NAMES or 'in_flight' in normalized or
                'queue_depth' in normalized or
                normalized.startswith('rejected_in_') or
                normalized.startswith('offered_arrival') or
                'arrivals_' in normalized):
            total += _numeric_total(value)
    return total


@dataclasses.dataclass(frozen=True, kw_only=True)
class RequestTelemetry:
    """Production-reduced request gauges plus exact-ledger state counts."""

    observed_at: float
    state: str
    reason: str
    compatibility_complete: bool
    queue_depth: int | None
    in_flight_requests: int | None
    processing_requests: int | None
    confirmed_in_flight_requests: int | None
    confirmed_processing_requests: int | None
    ledger_state_counts: tuple[tuple[str, int], ...]

    def ledger_count(self, state: str) -> int:
        return dict(self.ledger_state_counts).get(state, 0)

    @property
    def ledger_total(self) -> int:
        return sum(count for _, count in self.ledger_state_counts)

    @property
    def ledger_active(self) -> int:
        counts = dict(self.ledger_state_counts)
        return sum(counts.get(state, 0) for state in _ASYNC_ACTIVE_STATES)

    @property
    def ledger_succeeded(self) -> int:
        return self.ledger_count('SUCCEEDED')

    def is_fresh_complete(self) -> bool:
        return (math.isfinite(self.observed_at) and self.observed_at > 0 and
                self.state == 'fresh' and self.reason == 'complete' and
                self.compatibility_complete and self.queue_depth is not None and
                self.in_flight_requests is not None and
                self.processing_requests is not None)

    def is_exact_zero(self) -> bool:
        return (self.is_fresh_complete() and self.queue_depth == 0 and
                self.in_flight_requests == 0 and
                self.processing_requests == 0 and self.ledger_active == 0)


def request_telemetry_from_summary(
    summary: collections.abc.Mapping[str, Any],
    ledger_state_counts: collections.abc.Mapping[str, int],
) -> RequestTelemetry:
    """Validate one production demand summary and exact-ledger projection."""
    states = tuple(
        state.value for state in async_request_ledger.AsyncRequestState)
    normalized_counts: list[tuple[str, int]] = []
    for state in states:
        count = ledger_state_counts.get(state, 0)
        if type(count) is not int or count < 0:
            raise QualificationError('Async-ledger state count is invalid.')
        normalized_counts.append((state, count))

    def _nullable_count(field: str) -> int | None:
        value = summary.get(field)
        if value is None:
            return None
        if type(value) is not int or value < 0:
            raise QualificationError(
                f'Request telemetry field {field} is invalid.')
        return value

    observed_at = summary.get('request_telemetry_observed_at')
    if observed_at is None:
        # Unavailable/stale summaries intentionally have no source timestamp.
        observed_at = 0.0
    elif (not isinstance(observed_at,
                         (int, float)) or isinstance(observed_at, bool) or
          not math.isfinite(observed_at) or observed_at <= 0):
        raise QualificationError('Request telemetry timestamp is invalid.')
    state = summary.get('request_telemetry_state')
    reason = summary.get('request_telemetry_reason')
    complete = summary.get('request_telemetry_compatibility_complete')
    if (not isinstance(state, str) or not isinstance(reason, str) or
            type(complete) is not bool):
        raise QualificationError('Request telemetry summary is malformed.')
    return RequestTelemetry(
        observed_at=float(observed_at),
        state=state,
        reason=reason,
        compatibility_complete=complete,
        queue_depth=_nullable_count('request_queue_depth'),
        in_flight_requests=_nullable_count('in_flight_requests'),
        processing_requests=_nullable_count('processing_requests'),
        confirmed_in_flight_requests=_nullable_count(
            'confirmed_in_flight_requests'),
        confirmed_processing_requests=_nullable_count(
            'confirmed_processing_requests'),
        ledger_state_counts=tuple(normalized_counts),
    )


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderShapeState:
    """Physical machine and logical GPU counts for one exact shape."""

    gpu_units_per_instance: int
    instance_count: int
    instance_type: str
    running_count: int
    running_gpu_units: int


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderCloudState:
    """Exact provider-native counts for one cloud."""

    cloud: str
    instance_count: int
    running_count: int
    gpu_units: int
    running_gpu_units: int
    disk_count: int
    inflight_operation_count: int
    shapes: tuple[ProviderShapeState, ...] = ()


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderState:
    """One exact provider-native reduction, optionally across clouds."""

    instance_count: int
    running_count: int
    gpu_units: int
    running_gpu_units: int
    disk_count: int
    inflight_operation_count: int
    cluster_names: frozenset[str]
    clouds: tuple[ProviderCloudState, ...] = ()

    def cloud(self, name: str) -> ProviderCloudState:
        matches = [state for state in self.clouds if state.cloud == name]
        if len(matches) != 1:
            raise QualificationError(
                f'Provider census has no unique {name} reduction.')
        return matches[0]


def combine_provider_states(*states: ProviderState) -> ProviderState:
    """Combine disjoint provider reductions without losing provenance."""
    clouds = tuple(cloud for state in states for cloud in state.clouds)
    names = [cloud.cloud for cloud in clouds]
    if len(names) != len(set(names)):
        raise QualificationError(
            'Provider reductions contain duplicate clouds.')
    return ProviderState(
        instance_count=sum(state.instance_count for state in states),
        running_count=sum(state.running_count for state in states),
        gpu_units=sum(state.gpu_units for state in states),
        running_gpu_units=sum(state.running_gpu_units for state in states),
        disk_count=sum(state.disk_count for state in states),
        inflight_operation_count=sum(
            state.inflight_operation_count for state in states),
        cluster_names=frozenset().union(
            *(state.cluster_names for state in states)),
        clouds=clouds)


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderCensus:
    """Raw provider read captured before its PostgreSQL authorization."""

    instances: object
    disks: object
    operations: object


@dataclasses.dataclass(frozen=True, kw_only=True)
class AwsProviderCensus:
    """One frozen-region census of every service-tagged AWS effect."""

    service_instances: object
    service_volumes: object


@dataclasses.dataclass(frozen=True, kw_only=True)
class _AwsRegionCensus:
    """One side-effect-isolated AWS region read."""

    region: str
    service_instances: tuple[dict[str, Any], ...]
    service_volumes: tuple[dict[str, Any], ...]
    retained_volume_ids: tuple[str, ...]
    instance_type_widths: tuple[tuple[str, int], ...]


@dataclasses.dataclass(frozen=True, kw_only=True)
class GcpProviderIdentity:
    """One GCP allocation recovered from its retained request and pool."""

    cluster_name_on_cloud: str
    gpu_units_per_instance: int
    instance_type: str
    project_id: str
    region: str
    workspace: str
    zone: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class AwsProviderIdentity:
    """One AWS allocation recovered from its retained request and pool."""

    aws_account_id: str
    client_token: str
    cluster_name_on_cloud: str
    gpu_units_per_instance: int
    instance_type: str
    num_nodes: int
    region: str
    use_spot: bool
    workspace: str
    zone: str


class GcpLocationScope(str, enum.Enum):
    """Provider census boundary persisted by the qualification harness."""

    PROJECT_WIDE = 'project-wide'


class AwsLocationScope(str, enum.Enum):
    """AWS census boundary retained by every paid launch request."""

    FROZEN_CATALOG_REGIONS = 'frozen-catalog-regions'


@dataclasses.dataclass(frozen=True, kw_only=True)
class AwsRegionScope:
    """One committed-catalog AWS account/credential/region boundary."""

    aws_account_id: str
    credential_profile: str | None
    region: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class CatalogShape:
    """One exact whole-L4 launch shape frozen with the service version."""

    cloud: str
    region: str
    zone: str
    instance_type: str
    gpu_units_per_instance: int


def _catalog_shape_key(shape: CatalogShape) -> tuple[str, str, str, str, int]:
    return (shape.cloud, shape.region, shape.zone or
            '', shape.instance_type, shape.gpu_units_per_instance)


def _scope_has_catalog_shape(scope: 'ProviderScope', *, cloud: str, region: str,
                             zone: str, instance_type: str, width: int) -> bool:
    return CatalogShape(cloud=cloud,
                        region=region,
                        zone=zone,
                        instance_type=instance_type,
                        gpu_units_per_instance=width) in scope.catalog_shapes


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderScope:
    """Exact cross-cloud provider scope frozen by the committed version."""

    service_hash: str
    resource_scope: str
    lifecycle_epoch: int
    service_version: int
    max_live_paid_gpu_units: int
    providers: tuple[str, ...]
    project_id: str | None
    workspace: str
    location_scope: GcpLocationScope | None
    aws_location_scope: AwsLocationScope | None
    aws_regions: tuple[AwsRegionScope, ...]
    catalog_shapes: tuple[CatalogShape, ...]
    placement_catalog_sha256: str
    service_yaml_sha256: str
    qualification_profile: str
    qualification_source_sha256: str
    qualification_projection_sha256: str
    controller_config_digest: str
    controller_config_snapshot_id: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class QualificationServiceContract:
    """Canonical provider-neutral contract persisted with one test service."""

    providers: tuple[str, ...]
    min_replicas: int
    max_replicas: int
    max_live_paid_gpu_units: int
    scale_up_rate_min_replicas: int
    scale_up_rate_period_seconds: int
    request_queue_min_size: int
    request_queue_max_size: int
    request_queue_max_concurrency: int
    request_queue_timeout_seconds: float
    max_concurrency_per_replica: int


def _fixture_duration_limit(config: collections.abc.Mapping[str, Any]) -> float:
    """Read the worker's single bounded synthetic-duration contract."""
    run = config.get('run')
    if not isinstance(run, str):
        raise ValueError('service YAML has no worker program')
    matches = re.findall(
        r'(?m)^_MAX_SYNTHETIC_DURATION_SECONDS = ([0-9]+(?:\.[0-9]+)?)$', run)
    if len(matches) != 1:
        raise ValueError('worker duration limit is not singular')
    limit = float(matches[0])
    if limit <= 0:
        raise ValueError('worker duration limit is invalid')
    return limit


def _profile_projection(profile: Profile) -> dict[str, int]:
    """Return every field the canonical renderer may vary by profile."""
    if profile.name == 'scale':
        request_queue_min_size = scale_stimulus_count(profile)
        request_queue_max_size = request_queue_min_size
    else:
        request_queue_min_size = profile.request_concurrency
        request_queue_max_size = max(32, profile.request_concurrency)
    return {
        'max_replicas': profile.max_replicas,
        'max_live_paid_gpu_units': profile.max_units,
        'scale_up_rate_min_replicas': profile.scale_up_min_replicas,
        'scale_up_rate_period_seconds': profile.scale_up_period_seconds,
        'request_queue_min_size': request_queue_min_size,
        'request_queue_max_size': request_queue_max_size,
        'request_queue_max_concurrency': request_queue_max_concurrency(profile),
        'request_queue_timeout_seconds': profile.request_queue_timeout_seconds,
    }


def _profile_matches_contract(profile: Profile,
                              contract: QualificationServiceContract) -> bool:
    projection = _profile_projection(profile)
    return all(
        getattr(contract, field) == value
        for field, value in projection.items())


def _qualification_profile(contract: QualificationServiceContract) -> Profile:
    """Resolve one exact typed projection from the persisted task."""
    candidates = [
        profile for profile in PROFILES.values()
        if ((profile.name == 'provider-canary') == (
            len(contract.providers) == 1)) and
        _profile_matches_contract(profile, contract)
    ]
    if len(candidates) != 1:
        raise ValueError('service YAML is not one exact qualification profile')
    return candidates[0]


_USER_SPECIFIED_YAML_KEY = '_user_specified_yaml'


def _contains_reserved_user_yaml_key(value: object) -> bool:
    if isinstance(value, collections.abc.Mapping):
        return (_USER_SPECIFIED_YAML_KEY in value or any(
            _contains_reserved_user_yaml_key(item) for item in value.values()))
    if isinstance(value, (list, tuple)):
        return any(_contains_reserved_user_yaml_key(item) for item in value)
    return False


def _persisted_qualification_user_config(
        persisted_config: collections.abc.Mapping[str, Any]) -> dict[str, Any]:
    """Extract exactly one server-owned provenance envelope."""
    user_yaml = persisted_config.get(_USER_SPECIFIED_YAML_KEY)
    if not isinstance(user_yaml, str):
        raise ValueError('service has no immutable user task identity')
    try:
        user_config = yaml.safe_load(user_yaml)
    except yaml.YAMLError as error:
        raise ValueError('persisted user service YAML is invalid') from error
    if (not isinstance(user_config, dict) or
            _contains_reserved_user_yaml_key(user_config)):
        raise ValueError(
            'persisted user service YAML contains reserved provenance')
    return user_config


def _qualification_source_sha256(config: dict[str, Any]) -> str:
    """Hash one user task after removing only the allowed projection."""
    if _contains_reserved_user_yaml_key(config):
        raise ValueError('user service YAML contains reserved provenance')
    normalized = copy.deepcopy(config)
    normalized['resources'] = _canonical_qualification_resources(
        normalized['resources'])
    normalized['resources']['any_of'] = [{
        'infra': cloud,
        'accelerators': {
            'L4': 1,
        },
        'use_spot': True,
    } for cloud in ('aws', 'gcp')]
    policy = normalized['service']['replica_policy']
    queue = normalized['service']['load_balancer']['request_queue']
    for field in ('max_replicas', 'max_live_paid_gpu_units',
                  'scale_up_rate_min_replicas', 'scale_up_rate_period_seconds'):
        policy.pop(field, None)
    for field in ('min_size', 'max_size', 'max_concurrency', 'timeout_seconds'):
        queue.pop(field, None)
    return hashlib.sha256(rfc8785.dumps(normalized)).hexdigest()


def _qualification_projection_sha256(*, source_sha256: str, profile: Profile,
                                     providers: tuple[str, ...]) -> str:
    """Bind one allowed profile/provider projection to its source task."""
    return hashlib.sha256(
        rfc8785.dumps({
            'source_sha256': source_sha256,
            'profile': profile.name,
            'providers': list(providers),
            **_profile_projection(profile),
        })).hexdigest()


def _whole_l4_width(accelerators: object) -> int:
    """Return one exact positive whole-L4 width."""
    if not isinstance(accelerators,
                      collections.abc.Mapping) or len(accelerators) != 1:
        raise ValueError('accelerator shape is not exact L4')
    accelerator, width = next(iter(accelerators.items()))
    if (not isinstance(accelerator, str) or accelerator.casefold() != 'l4' or
            type(width) is not int or width < 1):
        raise ValueError('accelerator shape is not exact whole L4')
    return width


def _pool_l4_width(pool_identity: collections.abc.Mapping[str, Any]) -> int:
    """Return the exact width in a canonical paid-capacity pool key."""
    accelerators = pool_identity.get('accelerators')
    if (not isinstance(accelerators, list) or len(accelerators) != 1 or
            not isinstance(accelerators[0], list) or len(accelerators[0]) != 2):
        raise ValueError('paid pool has no exact accelerator shape')
    accelerator, width = accelerators[0]
    if (not isinstance(accelerator, str) or accelerator.casefold() != 'l4' or
            type(width) is not int or width < 1):
        raise ValueError('paid pool has no exact whole-L4 shape')
    return width


def _canonical_qualification_resources(
        resources: dict[str, Any]) -> dict[str, Any]:
    """Expand the user shorthand into the API's persisted branch shape."""
    branches = resources.get('any_of')
    if (resources.get('accelerators') != 'L4:1' or
            resources.get('use_spot') is not True or
            not isinstance(branches, list) or len(branches) not in (1, 2) or
            not all(
                isinstance(branch, dict) and set(branch) == {'infra'}
                for branch in branches)):
        return resources
    result = {
        key: value
        for key, value in resources.items()
        if key not in ('accelerators', 'use_spot')
    }
    result['any_of'] = [{
        **branch,
        'accelerators': {
            'L4': 1,
        },
        'use_spot': True,
    } for branch in branches]
    return result


def _validate_qualification_service_config(
        config: object) -> QualificationServiceContract:
    """Require the canonical provider-neutral logical-L4 test contract."""
    if not isinstance(config, dict):
        raise ValueError('service YAML is not a mapping')
    service = config.get('service')
    resources = config.get('resources')
    if (not isinstance(service, dict) or not isinstance(resources, dict)):
        raise ValueError('service YAML is incomplete')
    resources = _canonical_qualification_resources(resources)
    policy = service.get('replica_policy')
    load_balancer = service.get('load_balancer')
    queue = (load_balancer.get('request_queue') if isinstance(
        load_balancer, dict) else None)
    max_paid_units = (policy.get('max_live_paid_gpu_units') if isinstance(
        policy, dict) else None)
    per_replica_concurrency = (queue.get('max_concurrency_per_replica')
                               if isinstance(queue, dict) else None)
    max_replicas = (policy.get('max_replicas')
                    if isinstance(policy, dict) else None)
    min_replicas = (policy.get('min_replicas')
                    if isinstance(policy, dict) else None)
    scale_up_min = (policy.get('scale_up_rate_min_replicas') if isinstance(
        policy, dict) else None)
    scale_up_period = (policy.get('scale_up_rate_period_seconds') if isinstance(
        policy, dict) else None)
    queue_min_size = queue.get('min_size') if isinstance(queue, dict) else None
    queue_max_size = queue.get('max_size') if isinstance(queue, dict) else None
    queue_max_concurrency = (queue.get('max_concurrency') if isinstance(
        queue, dict) else None)
    queue_timeout = queue.get('timeout_seconds') if isinstance(queue,
                                                               dict) else None
    resource_branches = resources.get('any_of')
    canonical_clouds: set[str] = set()
    canonical_resources = (isinstance(resource_branches, list) and
                           len(resource_branches) in (1, 2) and
                           all(key not in resources
                               for key in ('infra', 'instance_type', 'region',
                                           'zone', 'accelerators', 'use_spot')))
    if canonical_resources:
        for branch in resource_branches:
            if not isinstance(branch, dict):
                canonical_resources = False
                break
            cloud = branch.get('infra')
            try:
                width = _whole_l4_width(branch.get('accelerators'))
            except ValueError:
                canonical_resources = False
                break
            if (cloud not in ('aws', 'gcp') or cloud in canonical_clouds or
                    width != 1 or branch.get('use_spot') is not True or
                    any(key in branch
                        for key in ('instance_type', 'region', 'zone'))):
                canonical_resources = False
                break
            canonical_clouds.add(cloud)
        canonical_resources = (canonical_resources and canonical_clouds
                               in ({'aws'}, {'gcp'}, {'aws', 'gcp'}))
    if (service.get('load_balancing_policy') != 'instance_aware_least_load' or
            not isinstance(policy, dict) or
            policy.get('spot_placer') != 'dynamic_fallback_per_gpu' or
            type(min_replicas) is not int or min_replicas != 0 or
            type(max_replicas) is not int or max_replicas < 1 or
            type(max_paid_units) is not int or max_paid_units < 1 or
            type(scale_up_min) is not int or scale_up_min < 1 or
            type(scale_up_period) is not int or scale_up_period < 1 or
            not isinstance(queue, dict) or type(queue_min_size) is not int or
            queue_min_size < 1 or type(queue_max_size) is not int or
            queue_max_size < queue_min_size or
            type(queue_max_concurrency) is not int or
            queue_max_concurrency < 1 or not isinstance(queue_timeout,
                                                        (int, float)) or
            isinstance(queue_timeout, bool) or queue_timeout <= 0 or
            type(per_replica_concurrency) is not int or
            per_replica_concurrency < 1 or not canonical_resources):
        raise ValueError('service YAML is not generic whole-L4 Spot')
    return QualificationServiceContract(
        providers=tuple(sorted(canonical_clouds)),
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        max_live_paid_gpu_units=max_paid_units,
        scale_up_rate_min_replicas=scale_up_min,
        scale_up_rate_period_seconds=scale_up_period,
        request_queue_min_size=queue_min_size,
        request_queue_max_size=queue_max_size,
        request_queue_max_concurrency=queue_max_concurrency,
        request_queue_timeout_seconds=float(queue_timeout),
        max_concurrency_per_replica=per_replica_concurrency)


def provider_scope_from_controller_config(
        authority: collections.abc.Mapping[str, Any]) -> ProviderScope:
    """Resolve provider scope from immutable version state, never ambient config."""
    config_bytes = authority.get('controller_config')
    if isinstance(config_bytes, memoryview):
        config_bytes = config_bytes.tobytes()
    digest = authority.get('controller_config_digest')
    snapshot_id = authority.get('controller_config_snapshot_id')
    workspace = authority.get('workspace')
    if (not isinstance(config_bytes, bytes) or not isinstance(digest, str) or
            re.fullmatch(r'[0-9a-f]{64}', digest) is None or
            hashlib.sha256(config_bytes).hexdigest() != digest or
            not isinstance(snapshot_id, str) or
            re.fullmatch(r'[0-9a-f]{64}', snapshot_id) is None or
            not isinstance(workspace, str) or not workspace):
        raise GuardViolation(
            'Current service version has no valid controller-config authority.')
    try:
        config_snapshot = serve_utils.parse_and_validate_version_controller_config(
            config_bytes, workspace, 'paid-provider E2E service version')
    except Exception as error:  # pylint: disable=broad-except
        raise GuardViolation(
            'Current service version controller config is invalid.') from error
    service_hash = authority.get('service_hash')
    resource_scope = authority.get('resource_scope')
    lifecycle_epoch = authority.get('service_lifecycle_epoch')
    service_version = authority.get('current_version')
    placement_catalog = authority.get('placement_catalog')
    yaml_content = authority.get('yaml_content')
    if (not isinstance(config_snapshot, collections.abc.Mapping) or
            not isinstance(service_hash, str) or not service_hash or
            not isinstance(resource_scope, str) or not resource_scope or
            type(lifecycle_epoch) is not int or lifecycle_epoch < 1 or
            type(service_version) is not int or service_version < 1 or
            not isinstance(placement_catalog, dict) or
            not isinstance(yaml_content, str)):
        raise GuardViolation(
            'Current service version has no exact workspace authority.')
    try:
        service_config = yaml.safe_load(yaml_content)
        user_config = _persisted_qualification_user_config(service_config)
        contract = _validate_qualification_service_config(service_config)
        profile = _qualification_profile(contract)
        source_sha256 = _qualification_source_sha256(user_config)
        projection_sha256 = _qualification_projection_sha256(
            source_sha256=source_sha256,
            profile=profile,
            providers=contract.providers)
        duration_limit = _fixture_duration_limit(service_config)
    except (TypeError, ValueError, yaml.YAMLError) as error:
        raise GuardViolation(
            'Current service version is not generic whole-L4 Spot.') \
            from error
    if (request_processing_seconds(profile) > duration_limit or
            duration_limit >= contract.request_queue_timeout_seconds):
        raise GuardViolation(
            'Current service duration contract cannot preserve bounded demand.')
    try:
        catalog = spot_placer.PlacementCatalog.from_dict(placement_catalog)
    except (KeyError, TypeError, ValueError) as error:
        raise GuardViolation(
            'Current service version has no exact placement catalog.'
        ) from error
    providers = tuple(
        sorted({
            str(location.cloud).casefold() for location, _ in catalog.entries
        }))
    try:
        catalog_is_exact = bool(
            providers == contract.providers and catalog.num_nodes == 1 and
            all(location.use_spot is True and isinstance(location.region, str)
                and bool(location.region) and isinstance(location.zone, str) and
                bool(location.zone) and isinstance(location.instance_type, str)
                and bool(location.instance_type) and
                _whole_l4_width(location.accelerators) >= 1
                for location, _ in catalog.entries))
    except ValueError:
        catalog_is_exact = False
    if not catalog_is_exact:
        raise GuardViolation(
            'Current service version has no exact whole-L4 Spot catalog.')
    if contract.max_concurrency_per_replica < max(
            _whole_l4_width(location.accelerators)
            for location, _ in catalog.entries):
        raise GuardViolation(
            'Current service queue clips a catalog L4 machine width.')
    catalog_shapes = tuple(
        sorted(
            {
                CatalogShape(cloud=str(location.cloud).casefold(),
                             region=location.region,
                             zone=location.zone,
                             instance_type=str(location.instance_type),
                             gpu_units_per_instance=_whole_l4_width(
                                 location.accelerators))
                for location, _ in catalog.entries
            },
            key=_catalog_shape_key))
    project_id: str | None = None
    gcp_location_scope: GcpLocationScope | None = None
    if 'gcp' in providers:
        project_id = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'gcp', ('project_id',),
                region=None,
                workspace=workspace))
        if (not isinstance(project_id, str) or re.fullmatch(
                r'[a-z][a-z0-9-]{4,28}[a-z0-9]', project_id) is None):
            raise GuardViolation(
                'Current service version has no exact GCP project authority.')
        gcp_location_scope = GcpLocationScope.PROJECT_WIDE
    aws_location_scope = (AwsLocationScope.FROZEN_CATALOG_REGIONS
                          if 'aws' in providers else None)
    aws_regions: list[AwsRegionScope] = []
    try:
        for region in sorted({
                location.region
                for location, _ in catalog.entries
                if str(location.cloud).casefold() == 'aws'
        }):
            credential_profile = (
                skypilot_config.
                get_effective_workspace_region_config_from_snapshot(
                    config_snapshot,
                    'aws', ('profile',),
                    region=region,
                    workspace=workspace))
            if (credential_profile is not None and
                (not isinstance(credential_profile, str) or
                 not credential_profile)):
                raise ValueError('invalid AWS credential profile')
            session = aws_adaptor.session(profile=credential_profile)
            caller = session.client('sts',
                                    region_name=region).get_caller_identity()
            account_id = caller.get('Account')
            if (not isinstance(account_id, str) or
                    re.fullmatch(r'[0-9]{12}', account_id) is None):
                raise ValueError('invalid AWS account identity')
            aws_regions.append(
                AwsRegionScope(aws_account_id=account_id,
                               credential_profile=credential_profile,
                               region=region))
    except Exception as error:  # pylint: disable=broad-except
        raise GuardViolation(
            'Current service version has no exact AWS catalog authority.') \
            from error
    if ('aws' in providers) != bool(aws_regions):
        raise GuardViolation(
            'Current service version has inconsistent AWS catalog regions.')
    return ProviderScope(
        service_hash=service_hash,
        resource_scope=resource_scope,
        lifecycle_epoch=lifecycle_epoch,
        service_version=service_version,
        max_live_paid_gpu_units=(contract.max_live_paid_gpu_units),
        providers=providers,
        project_id=project_id,
        workspace=workspace,
        location_scope=gcp_location_scope,
        aws_location_scope=aws_location_scope,
        aws_regions=tuple(aws_regions),
        catalog_shapes=catalog_shapes,
        placement_catalog_sha256=hashlib.sha256(
            rfc8785.dumps(placement_catalog)).hexdigest(),
        service_yaml_sha256=hashlib.sha256(
            yaml_content.encode('utf-8')).hexdigest(),
        qualification_profile=profile.name,
        qualification_source_sha256=source_sha256,
        qualification_projection_sha256=projection_sha256,
        controller_config_digest=digest,
        controller_config_snapshot_id=snapshot_id)


def write_provider_scope(path: pathlib.Path, service_name: str,
                         scope: ProviderScope) -> None:
    """Persist credential-free teardown authority before traffic is offered."""
    payload = {
        'schema_version': _PROVIDER_SCOPE_SCHEMA_VERSION,
        'service_name': service_name,
        'provider': 'serve-paid-spot',
        **dataclasses.asdict(scope),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + '\n',
                    encoding='utf-8')
    path.chmod(0o600)


def read_provider_scope(path: pathlib.Path, service_name: str) -> ProviderScope:
    """Read and validate the frozen teardown scope without ambient fallback."""
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise QualificationError('Provider-scope receipt is unavailable.') \
            from error
    field_names = {field.name for field in dataclasses.fields(ProviderScope)}
    if (not isinstance(payload, dict) or
            payload.get('schema_version') != _PROVIDER_SCOPE_SCHEMA_VERSION or
            payload.get('service_name') != service_name or
            payload.get('provider') != 'serve-paid-spot' or set(payload)
            != field_names | {'schema_version', 'service_name', 'provider'}):
        raise QualificationError('Provider-scope receipt is malformed.')
    try:
        values = {field: payload[field] for field in field_names}
        values['providers'] = tuple(values['providers'])
        values['aws_regions'] = tuple(
            AwsRegionScope(**region) for region in values['aws_regions'])
        values['catalog_shapes'] = tuple(
            CatalogShape(**shape) for shape in values['catalog_shapes'])
        if values['location_scope'] is not None:
            values['location_scope'] = GcpLocationScope(
                values['location_scope'])
        if values['aws_location_scope'] is not None:
            values['aws_location_scope'] = AwsLocationScope(
                values['aws_location_scope'])
        scope = ProviderScope(**values)
    except (TypeError, ValueError) as error:
        raise QualificationError('Provider-scope receipt is malformed.') \
            from error
    if (not isinstance(scope.service_hash, str) or not scope.service_hash or
            not isinstance(scope.resource_scope, str) or
            not scope.resource_scope or
            type(scope.lifecycle_epoch) is not int or
            scope.lifecycle_epoch < 1 or
            type(scope.service_version) is not int or
            scope.service_version < 1 or
            type(scope.max_live_paid_gpu_units) is not int or
            scope.max_live_paid_gpu_units < 1 or
            scope.providers not in (('aws',), ('gcp',), ('aws', 'gcp')) or
        (('gcp' in scope.providers)
         != (isinstance(scope.project_id, str) and re.fullmatch(
             r'[a-z][a-z0-9-]{4,28}[a-z0-9]', scope.project_id) is not None)) or
            not isinstance(scope.workspace, str) or not scope.workspace or
        (('gcp' in scope.providers) != (scope.location_scope
                                        is GcpLocationScope.PROJECT_WIDE)) or
        (('aws' in scope.providers)
         != (scope.aws_location_scope
             is AwsLocationScope.FROZEN_CATALOG_REGIONS)) or
        (('aws' in scope.providers) != bool(scope.aws_regions)) or
            tuple(sorted(scope.aws_regions, key=lambda region: region.region))
            != scope.aws_regions or
            len({region.region for region in scope.aws_regions}) != len(
                scope.aws_regions) or
            any(not isinstance(region.region, str) or not region.region or
                (region.credential_profile is not None and
                 (not isinstance(region.credential_profile, str) or
                  not region.credential_profile)) or
                re.fullmatch(r'[0-9]{12}', region.aws_account_id) is None
                for region in scope.aws_regions) or not scope.catalog_shapes or
            tuple(sorted(scope.catalog_shapes,
                         key=_catalog_shape_key)) != scope.catalog_shapes or
            len(set(scope.catalog_shapes)) != len(scope.catalog_shapes) or
            any(shape.cloud not in scope.providers or
                not isinstance(shape.region, str) or not shape.region or
                not isinstance(shape.zone, str) or not shape.zone or
                not isinstance(shape.instance_type, str) or
                not shape.instance_type or type(shape.gpu_units_per_instance)
                is not int or shape.gpu_units_per_instance < 1
                for shape in scope.catalog_shapes) or
        {shape.cloud for shape in scope.catalog_shapes} != set(
            scope.providers) or
            not isinstance(scope.placement_catalog_sha256, str) or re.fullmatch(
                r'[0-9a-f]{64}', scope.placement_catalog_sha256) is None or
            not isinstance(scope.service_yaml_sha256, str) or
            re.fullmatch(r'[0-9a-f]{64}', scope.service_yaml_sha256) is None or
            scope.qualification_profile not in PROFILES or
            scope.max_live_paid_gpu_units != PROFILES.get(
                scope.qualification_profile, PROFILES['small']).max_units or
        ((scope.qualification_profile == 'provider-canary') != (len(
            scope.providers) == 1)) or
            not isinstance(scope.qualification_source_sha256, str) or
            re.fullmatch(r'[0-9a-f]{64}',
                         scope.qualification_source_sha256) is None or
            not isinstance(scope.qualification_projection_sha256, str) or
            re.fullmatch(r'[0-9a-f]{64}', scope.qualification_projection_sha256)
            is None or scope.qualification_projection_sha256
            != _qualification_projection_sha256(
                source_sha256=scope.qualification_source_sha256,
                profile=PROFILES[scope.qualification_profile],
                providers=scope.providers) or
            not isinstance(scope.controller_config_digest, str) or re.fullmatch(
                r'[0-9a-f]{64}', scope.controller_config_digest) is None or
            not isinstance(scope.controller_config_snapshot_id, str) or
            re.fullmatch(r'[0-9a-f]{64}',
                         scope.controller_config_snapshot_id) is None):
        raise QualificationError('Provider-scope receipt is malformed.')
    return scope


def _managed_compute_resource_pattern(cluster_name: str) -> re.Pattern[str]:
    return re.compile(
        rf'{re.escape(cluster_name)}-(?:head|worker)-'
        rf'[a-z0-9]{{{instance_utils.INSTANCE_NAME_UUID_LEN}}}-compute')


def _gcp_region_from_zone(zone: object) -> str | None:
    """Return the exact parent region for one well-formed GCP zone."""
    if not isinstance(zone, str):
        return None
    match = re.fullmatch(r'([a-z]+-[a-z0-9]+[0-9])-[a-z]', zone)
    return None if match is None else match.group(1)


def _gcp_region_matches_zone(region: object, zone: object) -> bool:
    return isinstance(region, str) and _gcp_region_from_zone(zone) == region


def _scoped_compute_resource_pattern(service_name: str) -> re.Pattern[str]:
    """Match generated compute names within one dedicated service prefix."""
    return re.compile(
        rf'{re.escape(service_name)}(?:-[a-z0-9-]+)?-(?:head|worker)-'
        rf'[a-z0-9]{{{instance_utils.INSTANCE_NAME_UUID_LEN}}}-compute')


def _cluster_label(resource: collections.abc.Mapping[str, Any]) -> str | None:
    labels = resource.get('labels')
    if not isinstance(labels, collections.abc.Mapping):
        return None
    cluster_name = labels.get('ray-cluster-name')
    return cluster_name if isinstance(cluster_name,
                                      str) and cluster_name else None


def _resource_in_service_scope(resource: collections.abc.Mapping[str, Any],
                               service_name: str) -> bool:
    cluster_name = _cluster_label(resource)
    label_matches = (cluster_name == service_name or
                     (cluster_name is not None and
                      cluster_name.startswith(f'{service_name}-')))
    return label_matches or _generated_name_in_service_scope(
        resource.get('name'), service_name)


def _generated_name_in_service_scope(value: object, service_name: str) -> bool:
    return (_scoped_compute_resource_pattern(service_name).fullmatch(
        _basename(value)) is not None)


def _unique_scoped_resources(
    resources: collections.abc.Iterable[object],
    *,
    service_name: str,
    kind: str,
) -> list[dict[str, Any]]:
    """Return exact service-scoped resources, rejecting duplicate identities."""
    result = []
    identities: set[tuple[str, str]] = set()
    for resource in resources:
        if (not isinstance(resource, dict) or
                not _resource_in_service_scope(resource, service_name)):
            continue
        name = _basename(resource.get('name'))
        zone = _basename(resource.get('zone'))
        if not name or not zone:
            raise GuardViolation(
                f'Scoped GCP {kind} lacks an exact provider identity.')
        identity = (zone, name)
        if identity in identities:
            raise GuardViolation(
                f'GCP {kind} census contains a duplicate provider identity.')
        identities.add(identity)
        result.append(resource)
    return result


def _gcp_instance_l4_width(instance: collections.abc.Mapping[str, Any]) -> int:
    """Read and validate one exact whole-L4 provider shape."""
    accelerators = instance.get('guestAccelerators')
    if (not isinstance(accelerators, list) or len(accelerators) != 1 or
            not isinstance(accelerators[0], dict) or
            _basename(accelerators[0].get('acceleratorType')).casefold()
            != 'nvidia-l4'):
        raise GuardViolation(
            f'GCP instance {instance.get("name")!r} is not exact L4.')
    width = accelerators[0].get('acceleratorCount')
    if type(width) is not int or width < 1:
        raise GuardViolation(
            f'GCP instance {instance.get("name")!r} has invalid L4 width.')
    return width


def parse_gcp_state(
    *,
    service_name: str,
    profile: Profile,
    instances: object,
    disks: object,
    operations: object = (),
    expected_identities: collections.abc.Mapping[str, GcpProviderIdentity] |
    None = None,
    expected_cluster_zones: collections.abc.Mapping[str, str] | None = None
) -> ProviderState:
    """Validate and reduce one provider-native GCP census."""
    if (not isinstance(instances, list) or not isinstance(disks, list) or
            not isinstance(operations, (list, tuple))):
        raise QualificationError('GCP provider census is not a resource list.')
    if not service_name:
        raise QualificationError('Provider census requires a service scope.')
    if expected_identities is None:
        if expected_cluster_zones is None:
            raise GuardViolation('Durable launch binding has no GCP identity.')
        expected_identities = {
            cluster_name: GcpProviderIdentity(
                cluster_name_on_cloud=cluster_name,
                gpu_units_per_instance=1,
                instance_type='g2-standard-4',
                project_id='unit-test-project',
                region=_gcp_region_from_zone(zone) or '',
                workspace='unit-test',
                zone=zone)
            for cluster_name, zone in expected_cluster_zones.items()
        }
    elif expected_cluster_zones is not None:
        raise GuardViolation('GCP provider identity scope is ambiguous.')
    if (not isinstance(expected_identities, collections.abc.Mapping) or any(
            not isinstance(cluster_name, str) or not cluster_name or
            not isinstance(identity, GcpProviderIdentity) or
            identity.cluster_name_on_cloud != cluster_name or
            _gcp_region_from_zone(identity.zone) != identity.region or
            not identity.instance_type or identity.gpu_units_per_instance < 1
            for cluster_name, identity in expected_identities.items())):
        raise GuardViolation(
            'Durable launch binding has an invalid GCP identity.')
    expected_cluster_names = frozenset(expected_identities)
    patterns = {
        name: _managed_compute_resource_pattern(name)
        for name in expected_cluster_names
    }

    def bound_cluster_for_generated_name(value: object) -> str | None:
        name = _basename(value)
        matches = [
            cluster for cluster, pattern in patterns.items()
            if pattern.fullmatch(name) is not None
        ]
        if len(matches) > 1:
            raise GuardViolation(
                'Provider resource matches multiple durable launch bindings.')
        return matches[0] if matches else None

    owned_instances = _unique_scoped_resources(instances,
                                               service_name=service_name,
                                               kind='instance')
    owned_disks = _unique_scoped_resources(disks,
                                           service_name=service_name,
                                           kind='disk')
    owned_inflight_operation_targets: dict[str, str] = {}
    for lineage in _scoped_inflight_gcp_insert_lineages(service_name,
                                                        operations):
        target_link = lineage.target_link
        zone_match = re.search(r'/zones/([^/]+)/', target_link)
        if (zone_match is None or
                _gcp_region_from_zone(zone_match.group(1)) is None):
            raise GuardViolation(
                'GCP create operation has no exact provider zone.')
        previous_zone = owned_inflight_operation_targets.setdefault(
            lineage.target_name, zone_match.group(1))
        if previous_zone != zone_match.group(1):
            raise GuardViolation(
                'GCP create operation has contradictory zone evidence.')
    cluster_names: set[str] = set()
    instance_identity_by_cluster: dict[str, tuple[str, str]] = {}
    for instance in owned_instances:
        cluster_name = bound_cluster_for_generated_name(instance.get('name'))
        if cluster_name is None:
            raise GuardViolation(
                'Provider instance exists without a durable launch binding.')
        if _cluster_label(instance) != cluster_name:
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} has cluster metadata '
                'that disagrees with its durable launch binding.')
        identity = (_basename(instance.get('zone')),
                    _basename(instance.get('name')))
        if cluster_name in instance_identity_by_cluster:
            raise GuardViolation(
                'A one-node paid binding has multiple GCP instance effects.')
        instance_identity_by_cluster[cluster_name] = identity
        cluster_names.add(cluster_name)
        scheduling = instance.get('scheduling') or {}
        if (not isinstance(scheduling, dict) or
                str(scheduling.get('provisioningModel', '')).upper() != 'SPOT'):
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} is not Spot.')
        expected_identity = expected_identities[cluster_name]
        if (_basename(instance.get('machineType'))
                != expected_identity.instance_type):
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} has the wrong shape.')
        instance_zone = _basename(instance.get('zone'))
        if instance_zone != expected_identity.zone:
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} is in the wrong '
                'binding zone.')
        if (_gcp_instance_l4_width(instance)
                != expected_identity.gpu_units_per_instance):
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} has the wrong L4 '
                'width.')
    disk_identity_by_cluster: dict[str, tuple[str, str]] = {}
    for disk in owned_disks:
        cluster_name = bound_cluster_for_generated_name(disk.get('name'))
        if cluster_name is None:
            raise GuardViolation(
                'Provider disk exists without a durable launch binding.')
        labels = disk.get('labels')
        if (not isinstance(labels, collections.abc.Mapping) or
                labels.get('skypilot-managed') != 'true' or
                _cluster_label(disk) != cluster_name):
            raise GuardViolation(
                f'GCP disk {disk.get("name")!r} has metadata that disagrees '
                'with its durable launch binding.')
        identity = (_basename(disk.get('zone')), _basename(disk.get('name')))
        if cluster_name in disk_identity_by_cluster:
            raise GuardViolation(
                'A one-node paid binding has multiple GCP disk effects.')
        disk_identity_by_cluster[cluster_name] = identity
        if (_basename(disk.get('zone'))
                != expected_identities[cluster_name].zone):
            raise GuardViolation(
                f'GCP disk {disk.get("name")!r} is in the wrong binding zone.')
    for cluster_name in instance_identity_by_cluster.keys(
    ) & disk_identity_by_cluster.keys():
        if (instance_identity_by_cluster[cluster_name]
                != disk_identity_by_cluster[cluster_name]):
            raise GuardViolation(
                'A paid binding has different GCP instance and disk identities.'
            )
    operation_target_by_cluster: dict[str, str] = {}
    for target, operation_zone in owned_inflight_operation_targets.items():
        cluster_name = bound_cluster_for_generated_name(target)
        if cluster_name is None:
            raise GuardViolation(
                'Provider operation exists without a durable launch binding.')
        previous_target = operation_target_by_cluster.setdefault(
            cluster_name, target)
        if previous_target != target:
            raise GuardViolation(
                'A one-node paid binding has multiple GCP create operations.')
        if operation_zone != expected_identities[cluster_name].zone:
            raise GuardViolation(
                'GCP create operation is outside its binding zone.')
        operation_identity = (operation_zone, target)
        for existing_identity in (
                instance_identity_by_cluster.get(cluster_name),
                disk_identity_by_cluster.get(cluster_name)):
            if (existing_identity is not None and
                    existing_identity != operation_identity):
                raise GuardViolation(
                    'A paid binding has contradictory GCP create identities.')
    gpu_units = sum(
        _gcp_instance_l4_width(instance) for instance in owned_instances)
    running_gpu_units = sum(
        _gcp_instance_l4_width(instance)
        for instance in owned_instances
        if str(instance.get('status', '')).upper() == 'RUNNING')
    if gpu_units > profile.max_units:
        raise GuardViolation('Provider GPU units exceeded the armed cap.')
    shape_counts: dict[tuple[str, int], list[int]] = {}
    for instance in owned_instances:
        cluster_name = bound_cluster_for_generated_name(instance.get('name'))
        assert cluster_name is not None
        shape_identity = expected_identities[cluster_name]
        counts = shape_counts.setdefault(
            (shape_identity.instance_type,
             shape_identity.gpu_units_per_instance), [0, 0])
        counts[0] += 1
        if str(instance.get('status', '')).upper() == 'RUNNING':
            counts[1] += 1
    shapes = tuple(
        ProviderShapeState(instance_type=instance_type,
                           gpu_units_per_instance=width,
                           instance_count=counts[0],
                           running_count=counts[1],
                           running_gpu_units=counts[1] * width)
        for (instance_type, width), counts in sorted(shape_counts.items()))
    cloud_state = ProviderCloudState(
        cloud='gcp',
        instance_count=len(owned_instances),
        running_count=sum(
            str(item.get('status', '')).upper() == 'RUNNING'
            for item in owned_instances),
        gpu_units=gpu_units,
        running_gpu_units=running_gpu_units,
        disk_count=len(owned_disks),
        inflight_operation_count=len(owned_inflight_operation_targets),
        shapes=shapes)
    return ProviderState(
        instance_count=cloud_state.instance_count,
        running_count=cloud_state.running_count,
        gpu_units=cloud_state.gpu_units,
        running_gpu_units=cloud_state.running_gpu_units,
        disk_count=cloud_state.disk_count,
        inflight_operation_count=(cloud_state.inflight_operation_count),
        cluster_names=frozenset(cluster_names),
        clouds=(cloud_state,))


def parse_gcp_cleanup_state(*, service_name: str, instances: object,
                            disks: object, operations: object) -> ProviderState:
    """Count every remaining provider effect in the dedicated test scope."""
    if (not isinstance(instances, list) or not isinstance(disks, list) or
            not isinstance(operations, (list, tuple))):
        raise QualificationError('GCP cleanup census is not a resource list.')
    owned_instances = _unique_scoped_resources(instances,
                                               service_name=service_name,
                                               kind='instance')
    # Cleanup counts exact generated names even when provider metadata was lost.
    # A missing marker must keep an orphan billable disk visible, not exempt it.
    owned_disks = _unique_scoped_resources(disks,
                                           service_name=service_name,
                                           kind='disk')
    # Project-scoped bulkInsert parents have no service name in targetLink.
    # The shared reducer attributes them only through the provider's durable
    # operationGroupId/child edge; operation-name prefixes are never trusted.
    operation_targets = {
        lineage.target_name for lineage in _scoped_inflight_gcp_insert_lineages(
            service_name, operations)
    }
    cluster_names = frozenset(
        cluster_name for item in owned_instances
        if (cluster_name := _cluster_label(item)) is not None)
    gpu_units = sum(
        _gcp_instance_l4_width(instance) for instance in owned_instances)
    running_gpu_units = sum(
        _gcp_instance_l4_width(instance)
        for instance in owned_instances
        if str(instance.get('status', '')).upper() == 'RUNNING')
    shape_counts: dict[tuple[str, int], list[int]] = {}
    for instance in owned_instances:
        shape = (_basename(instance.get('machineType')),
                 _gcp_instance_l4_width(instance))
        if not shape[0]:
            raise GuardViolation('GCP cleanup instance has no exact shape.')
        counts = shape_counts.setdefault(shape, [0, 0])
        counts[0] += 1
        if str(instance.get('status', '')).upper() == 'RUNNING':
            counts[1] += 1
    shapes = tuple(
        ProviderShapeState(instance_type=instance_type,
                           gpu_units_per_instance=width,
                           instance_count=counts[0],
                           running_count=counts[1],
                           running_gpu_units=counts[1] * width)
        for (instance_type, width), counts in sorted(shape_counts.items()))
    cloud_state = ProviderCloudState(
        cloud='gcp',
        instance_count=len(owned_instances),
        running_count=sum(
            str(item.get('status', '')).upper() == 'RUNNING'
            for item in owned_instances),
        gpu_units=gpu_units,
        running_gpu_units=running_gpu_units,
        disk_count=len(owned_disks),
        inflight_operation_count=len(operation_targets),
        shapes=shapes)
    return ProviderState(
        instance_count=cloud_state.instance_count,
        running_count=cloud_state.running_count,
        gpu_units=cloud_state.gpu_units,
        running_gpu_units=cloud_state.running_gpu_units,
        disk_count=cloud_state.disk_count,
        inflight_operation_count=(cloud_state.inflight_operation_count),
        cluster_names=cluster_names,
        clouds=(cloud_state,))


class GcpObserver:
    """Read-only provider census reduced by durable cloud identities."""

    def __init__(self,
                 *,
                 service_name: str,
                 scope: ProviderScope,
                 profile: Profile,
                 compute: object | None = None) -> None:
        self._service_name = service_name
        self._scope = scope
        self._profile = profile
        if ('gcp' not in scope.providers or
                not isinstance(scope.project_id, str)):
            raise QualificationError('Provider scope does not include GCP.')
        try:
            self._compute = (gcp_adaptor.build(
                'compute', 'v1', credentials=None, cache_discovery=False)
                             if compute is None else compute)
        except Exception as error:  # pylint: disable=broad-except
            raise QualificationError(
                'GCP Compute API client initialization failed.') from error

    def _aggregated_list(self, collection_name: str,
                         item_name: str) -> list[dict[str, Any]]:
        """Read every project resource with ADC and explicit pagination."""
        try:
            collection_factory = getattr(self._compute, collection_name)
            collection = collection_factory()
        except (AttributeError, TypeError) as error:
            raise QualificationError(
                f'GCP Compute API {item_name} census is unavailable.') \
                from error

        resources: list[dict[str, Any]] = []
        page_token: str | None = None
        seen_tokens: set[str] = set()
        while True:
            request_kwargs: dict[str, Any] = {
                'project': self._scope.project_id,
                'maxResults': _GCP_LIST_PAGE_SIZE,
                'returnPartialSuccess': True,
            }
            if page_token is not None:
                request_kwargs['pageToken'] = page_token
            try:
                response = collection.aggregatedList(**request_kwargs).execute(
                    num_retries=_GCP_API_RETRIES)
            except Exception as error:  # pylint: disable=broad-except
                # Never include provider exception text in a qualification
                # error: it may contain credential or request metadata.
                raise QualificationError(
                    f'GCP Compute API {item_name} census failed.') from error
            if not isinstance(response, collections.abc.Mapping):
                raise QualificationError(
                    f'GCP Compute API {item_name} census is malformed.')
            scoped_items = response.get('items', {})
            if not isinstance(scoped_items, collections.abc.Mapping):
                raise QualificationError(
                    f'GCP Compute API {item_name} census is malformed.')
            for scope_name in sorted(scoped_items):
                scope_payload = scoped_items[scope_name]
                if not isinstance(scope_payload, collections.abc.Mapping):
                    raise QualificationError(
                        f'GCP Compute API {item_name} census is malformed.')
                page_resources = scope_payload.get(item_name, ())
                if not isinstance(page_resources, (list, tuple)):
                    raise QualificationError(
                        f'GCP Compute API {item_name} census is malformed.')
                if not all(isinstance(item, dict) for item in page_resources):
                    raise QualificationError(
                        f'GCP Compute API {item_name} census is malformed.')
                resources.extend(page_resources)
            next_token = response.get('nextPageToken')
            if next_token is None:
                return resources
            if (not isinstance(next_token, str) or not next_token or
                    next_token in seen_tokens):
                raise QualificationError(
                    f'GCP Compute API {item_name} pagination is malformed.')
            seen_tokens.add(next_token)
            page_token = next_token

    def census(self) -> ProviderCensus:
        return ProviderCensus(
            instances=self._aggregated_list('instances', 'instances'),
            disks=self._aggregated_list('disks', 'disks'),
            operations=self._aggregated_list('globalOperations', 'operations'))

    def reduce(
        self, census: ProviderCensus,
        expected_identities: collections.abc.Mapping[str, GcpProviderIdentity]
    ) -> ProviderState:
        return parse_gcp_state(service_name=self._service_name,
                               expected_identities=expected_identities,
                               profile=self._profile,
                               instances=census.instances,
                               disks=census.disks,
                               operations=census.operations)


def empty_provider_state(cloud: str) -> ProviderState:
    cloud_state = ProviderCloudState(cloud=cloud,
                                     instance_count=0,
                                     running_count=0,
                                     gpu_units=0,
                                     running_gpu_units=0,
                                     disk_count=0,
                                     inflight_operation_count=0)
    return ProviderState(instance_count=0,
                         running_count=0,
                         gpu_units=0,
                         running_gpu_units=0,
                         disk_count=0,
                         inflight_operation_count=0,
                         cluster_names=frozenset(),
                         clouds=(cloud_state,))


def _aws_shape_states(
    instances: collections.abc.Sequence[collections.abc.Mapping[str, Any]],
) -> tuple[ProviderShapeState, ...]:
    shape_counts: dict[tuple[str, int], list[int]] = {}
    for instance in instances:
        instance_type = instance.get('instance_type')
        width = instance.get('provider_gpu_units')
        state = instance.get('state')
        if (not isinstance(instance_type, str) or not instance_type or
                type(width) is not int or width < 1 or
                not isinstance(state, str) or not state):
            raise GuardViolation('AWS service instance has no exact L4 shape.')
        counts = shape_counts.setdefault((instance_type, width), [0, 0])
        counts[0] += 1
        if state == 'running':
            counts[1] += 1
    return tuple(
        ProviderShapeState(instance_type=instance_type,
                           gpu_units_per_instance=width,
                           instance_count=counts[0],
                           running_count=counts[1],
                           running_gpu_units=counts[1] * width)
        for (instance_type, width), counts in sorted(shape_counts.items()))


def _canonical_aws_resources(
    *, instances: object, volumes: object
) -> tuple[tuple[collections.abc.Mapping[str, Any], ...], tuple[
        collections.abc.Mapping[str, Any], ...]]:
    if not isinstance(instances, tuple) or not isinstance(volumes, tuple):
        raise QualificationError('AWS service census is not canonical.')
    if not all(
            isinstance(item, collections.abc.Mapping)
            for item in (*instances, *volumes)):
        raise QualificationError('AWS service census is not canonical.')
    return instances, volumes


_AWS_INSTANCE_IDENTITY_FIELDS = (
    'availability_zone',
    'client_token',
    'cluster_name_on_cloud',
    'instance_id',
    'instance_type',
    'market',
    'region',
)
_AWS_VOLUME_IDENTITY_FIELDS = (
    'cluster_name_on_cloud',
    'region',
    'volume_id',
)


def _merge_aws_instance_observations(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> dict[str, Any]:
    """Merge two sequential tag-query observations of one EC2 instance."""
    if any(
            previous.get(field) != current.get(field)
            for field in _AWS_INSTANCE_IDENTITY_FIELDS):
        raise GuardViolation('AWS service instance identity is contradictory.')

    previous_volume_ids = previous['volume_ids']
    current_volume_ids = current['volume_ids']
    if (len(previous_volume_ids) != len(set(previous_volume_ids)) or
            len(current_volume_ids) != len(set(current_volume_ids))):
        raise GuardViolation(
            'AWS service instance repeats one EBS volume identity.')

    merged = dict(previous)
    previous_state = previous['state']
    current_state = current['state']
    if previous_state != current_state:
        # The two service-tag queries are separate EC2 snapshots.  Preserve an
        # observed non-running state on disagreement so this census cannot
        # overstate physical RUNNING capacity.  The reducer distinguishes only
        # running from non-running, so the first non-running observation
        # suffices.
        merged['state'] = (current_state
                           if previous_state == 'running' else previous_state)
    merged['volume_ids'] = tuple(
        sorted(set(previous_volume_ids) | set(current_volume_ids)))
    return merged


def _merge_aws_volume_observations(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> dict[str, Any]:
    """Merge two sequential tag-query observations of one EBS volume."""
    if any(
            previous.get(field) != current.get(field)
            for field in _AWS_VOLUME_IDENTITY_FIELDS):
        raise GuardViolation('AWS service volume identity is contradictory.')
    merged = dict(previous)
    # Presence, rather than the lifecycle spelling, is the cleanup evidence.
    # Keep one deterministic state that was actually observed.
    merged['state'] = min(previous['state'], current['state'])
    return merged


def parse_aws_state(*,
                    identities: collections.abc.Sequence[AwsProviderIdentity],
                    profile: Profile, service_instances: object,
                    service_volumes: object) -> ProviderState:
    """Correlate every service-scoped AWS effect with durable launch state."""
    instances, volumes = _canonical_aws_resources(instances=service_instances,
                                                  volumes=service_volumes)
    identity_by_token = {
        identity.client_token: identity for identity in identities
    }
    if len(identity_by_token) != len(identities):
        raise GuardViolation('AWS paid bindings reuse one ClientToken.')
    allowed_regions_by_cluster: dict[str,
                                     set[str]] = collections.defaultdict(set)
    for binding_identity in identities:
        allowed_regions_by_cluster[binding_identity.cluster_name_on_cloud].add(
            binding_identity.region)

    allocation_counts: dict[str, int] = collections.defaultdict(int)
    seen_instance_ids: set[str] = set()
    attached_volume_ids: set[str] = set()
    live_clusters: list[str] = []
    attachment_incomplete: str | None = None
    for instance in instances:
        token = instance.get('client_token')
        observed_identity = (identity_by_token.get(token) if isinstance(
            token, str) else None)
        instance_id = instance.get('instance_id')
        state = instance.get('state')
        raw_volume_ids = instance.get('volume_ids')
        if (observed_identity is None or not isinstance(instance_id, str) or
                not instance_id or instance_id in seen_instance_ids or
                instance.get('cluster_name_on_cloud')
                != observed_identity.cluster_name_on_cloud or
                instance.get('availability_zone') != observed_identity.zone or
                instance.get('region') != observed_identity.region or
                instance.get('instance_type') != observed_identity.instance_type
                or instance.get('provider_gpu_units')
                != observed_identity.gpu_units_per_instance or
                instance.get('market') != 'spot' or state not in {
                    'pending', 'running', 'shutting-down', 'stopping', 'stopped'
                } or not isinstance(raw_volume_ids, tuple) or
                any(not isinstance(volume_id, str) or not volume_id
                    for volume_id in raw_volume_ids)):
            raise GuardViolation(
                'AWS service effect escaped its retained launch binding.')
        if len(raw_volume_ids) != len(set(raw_volume_ids)):
            raise GuardViolation(
                'AWS service instance repeats one EBS volume identity.')
        if not raw_volume_ids:
            # EC2 may expose a correctly bound instance before its root EBS
            # mapping.  Defer the retryable result until every other effect in
            # this census has passed the fatal guards below.
            attachment_incomplete = (
                'AWS instance EBS attachment is not yet visible.')
        else:
            overlap = attached_volume_ids.intersection(raw_volume_ids)
            if overlap:
                raise GuardViolation(
                    'AWS service instances share an EBS volume.')
            attached_volume_ids.update(raw_volume_ids)
        seen_instance_ids.add(instance_id)
        allocation_counts[observed_identity.client_token] += 1
        if (allocation_counts[observed_identity.client_token]
                > observed_identity.num_nodes):
            raise GuardViolation(
                'AWS provider allocation exceeded its retained node count.')
        live_clusters.append(observed_identity.cluster_name_on_cloud)

    existing_volume_ids: set[str] = set()
    volume_clusters: set[str] = set()
    for volume in volumes:
        volume_id = volume.get('volume_id')
        state = volume.get('state')
        region = volume.get('region')
        cluster_name = volume.get('cluster_name_on_cloud')
        if (not isinstance(volume_id, str) or not volume_id or
                volume_id in existing_volume_ids or
                not isinstance(state, str) or not state or
                not isinstance(region, str) or not region or
                not isinstance(cluster_name, str) or not cluster_name):
            raise GuardViolation('AWS service EBS census is not canonical.')
        if region not in allowed_regions_by_cluster.get(cluster_name, set()):
            raise GuardViolation(
                'AWS service EBS effect has no retained launch binding.')
        existing_volume_ids.add(volume_id)
        volume_clusters.add(cluster_name)
    if (not attached_volume_ids.issubset(existing_volume_ids) and
            attachment_incomplete is None):
        attachment_incomplete = (
            'AWS attached EBS volume is not yet visible in the service census.')

    if len(live_clusters) != len(set(live_clusters)):
        raise GuardViolation(
            'AWS retry history contains multiple live provider effects.')
    gpu_units = sum(
        int(instance['provider_gpu_units']) for instance in instances)
    running_gpu_units = sum(
        int(instance['provider_gpu_units'])
        for instance in instances
        if instance['state'] == 'running')
    if gpu_units > profile.max_units:
        raise GuardViolation('Provider GPU units exceeded the armed cap.')
    shapes = _aws_shape_states(instances)
    if attachment_incomplete is not None:
        raise QualificationError(attachment_incomplete)
    cloud_state = ProviderCloudState(
        cloud='aws',
        instance_count=len(instances),
        running_count=sum(
            instance['state'] == 'running' for instance in instances),
        gpu_units=gpu_units,
        running_gpu_units=running_gpu_units,
        disk_count=len(existing_volume_ids),
        inflight_operation_count=0,
        shapes=shapes)
    return ProviderState(instance_count=cloud_state.instance_count,
                         running_count=cloud_state.running_count,
                         gpu_units=cloud_state.gpu_units,
                         running_gpu_units=cloud_state.running_gpu_units,
                         disk_count=cloud_state.disk_count,
                         inflight_operation_count=0,
                         cluster_names=frozenset(live_clusters) |
                         frozenset(volume_clusters),
                         clouds=(cloud_state,))


def parse_aws_cleanup_state(*, service_instances: object,
                            service_volumes: object) -> ProviderState:
    """Count AWS effects from frozen regions after database state is gone."""
    instances, volumes = _canonical_aws_resources(instances=service_instances,
                                                  volumes=service_volumes)
    instance_ids: set[str] = set()
    cluster_names: set[str] = set()
    for instance in instances:
        instance_id = instance.get('instance_id')
        cluster_name = instance.get('cluster_name_on_cloud')
        if (not isinstance(instance_id, str) or not instance_id or
                instance_id in instance_ids or
                not isinstance(cluster_name, str) or not cluster_name):
            raise GuardViolation(
                'AWS cleanup instance census is not canonical.')
        instance_ids.add(instance_id)
        cluster_names.add(cluster_name)
    volume_ids: set[str] = set()
    for volume in volumes:
        volume_id = volume.get('volume_id')
        cluster_name = volume.get('cluster_name_on_cloud')
        if (not isinstance(volume_id, str) or not volume_id or
                volume_id in volume_ids or
            (cluster_name is not None and
             (not isinstance(cluster_name, str) or not cluster_name))):
            raise GuardViolation('AWS cleanup EBS census is not canonical.')
        volume_ids.add(volume_id)
        if isinstance(cluster_name, str):
            cluster_names.add(cluster_name)
    shapes = _aws_shape_states(instances)
    gpu_units = sum(
        int(instance['provider_gpu_units']) for instance in instances)
    running_gpu_units = sum(
        int(instance['provider_gpu_units'])
        for instance in instances
        if instance['state'] == 'running')
    cloud_state = ProviderCloudState(
        cloud='aws',
        instance_count=len(instances),
        running_count=sum(
            instance['state'] == 'running' for instance in instances),
        gpu_units=gpu_units,
        running_gpu_units=running_gpu_units,
        disk_count=len(volumes),
        inflight_operation_count=0,
        shapes=shapes)
    return ProviderState(instance_count=cloud_state.instance_count,
                         running_count=cloud_state.running_count,
                         gpu_units=cloud_state.gpu_units,
                         running_gpu_units=cloud_state.running_gpu_units,
                         disk_count=cloud_state.disk_count,
                         inflight_operation_count=0,
                         cluster_names=frozenset(cluster_names),
                         clouds=(cloud_state,))


class AwsObserver:
    """Census every service-tagged effect in every frozen AWS region."""

    def __init__(
        self,
        *,
        profile: Profile,
        service_name: str,
        scope: ProviderScope,
        retained_volume_ids_by_region: collections.abc.Mapping[
            str, collections.abc.Sequence[str]] | None = None,
    ) -> None:
        self._profile = profile
        self._service_name = service_name
        self._scope = scope
        configured_regions = {region.region for region in scope.aws_regions}
        retained = retained_volume_ids_by_region or {}
        if not set(retained).issubset(configured_regions):
            raise QualificationError(
                'Retained AWS volumes are outside frozen catalog regions.')
        self._cleanup_mode = retained_volume_ids_by_region is not None
        self._retained_volume_ids_by_region = {
            region: set(retained.get(region, ()))
            for region in configured_regions
        }
        self._instance_type_widths: dict[tuple[str, str], int] = {}

    @staticmethod
    def _tags(resource: collections.abc.Mapping[str, Any]) -> dict[str, str]:
        raw_tags = resource.get('Tags', [])
        if not isinstance(raw_tags, list):
            raise GuardViolation('AWS service effect has invalid tags.')
        tags: dict[str, str] = {}
        for tag in raw_tags:
            if (not isinstance(tag, collections.abc.Mapping) or
                    not isinstance(tag.get('Key'), str) or
                    not isinstance(tag.get('Value'), str) or
                    tag['Key'] in tags):
                raise GuardViolation(
                    'AWS service effect has non-canonical tags.')
            tags[tag['Key']] = tag['Value']
        return tags

    def _exact_service_cluster(
            self, tags: collections.abc.Mapping[str, str]) -> str | None:
        ray_name = tags.get(provision_constants.TAG_RAY_CLUSTER_NAME)
        sky_name = tags.get(provision_constants.TAG_SKYPILOT_CLUSTER_NAME)
        if (not isinstance(ray_name, str) or
                not ray_name.startswith(f'{self._service_name}-') or
                sky_name != ray_name or
                tags.get(provision_constants.TAG_SKYPILOT_MANAGED)
                != provision_constants.SKYPILOT_MANAGED_TAG_VALUE):
            return None
        return ray_name

    @staticmethod
    def _provider_l4_width(instance_type: object) -> tuple[str, int]:
        if not isinstance(instance_type, collections.abc.Mapping):
            raise GuardViolation('AWS instance-type census is malformed.')
        name = instance_type.get('InstanceType')
        gpu_info = instance_type.get('GpuInfo')
        gpus = (gpu_info.get('Gpus')
                if isinstance(gpu_info, collections.abc.Mapping) else None)
        if (not isinstance(name, str) or not name or
                not isinstance(gpus, list) or not gpus):
            raise GuardViolation(
                'AWS instance type has no exact NVIDIA L4 inventory.')
        width = 0
        for gpu in gpus:
            if (not isinstance(gpu, collections.abc.Mapping) or
                    str(gpu.get('Manufacturer', '')).casefold() != 'nvidia' or
                    str(gpu.get('Name', '')).casefold() != 'l4' or
                    type(gpu.get('Count')) is not int or gpu['Count'] < 1):
                raise GuardViolation(
                    'AWS instance type has non-L4 GPU inventory.')
            width += gpu['Count']
        return name, width

    def _catalog_width(self, region: str, instance_type: str) -> int:
        widths = {
            shape.gpu_units_per_instance
            for shape in self._scope.catalog_shapes
            if shape.cloud == 'aws' and shape.region == region and
            shape.instance_type == instance_type
        }
        if len(widths) != 1:
            raise GuardViolation(
                'AWS service effect is absent from the frozen catalog.')
        return next(iter(widths))

    def _attest_instance_types(self, client: Any, region: str,
                               instance_types: set[str]) -> None:
        missing = sorted(instance_type for instance_type in instance_types
                         if (region,
                             instance_type) not in self._instance_type_widths)
        for start in range(0, len(missing), 100):
            batch = missing[start:start + 100]
            response = client.describe_instance_types(InstanceTypes=batch)
            values = (response.get('InstanceTypes') if isinstance(
                response, collections.abc.Mapping) else None)
            if not isinstance(values, list):
                raise QualificationError(
                    'AWS instance-type census is malformed.')
            observed: dict[str, int] = {}
            for value in values:
                name, width = self._provider_l4_width(value)
                if name in observed:
                    raise GuardViolation(
                        'AWS instance-type census repeats one shape.')
                observed[name] = width
            if set(observed) != set(batch):
                raise GuardViolation('AWS instance-type census is incomplete.')
            for name, width in observed.items():
                if width != self._catalog_width(region, name):
                    raise GuardViolation(
                        'AWS provider GPU width disagrees with frozen catalog.')
                self._instance_type_widths[(region, name)] = width

    def _service_census_serial(
        self, region_scopes: collections.abc.Sequence[AwsRegionScope]
    ) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
        instances_by_id: dict[str, dict[str, Any]] = {}
        volumes_by_id: dict[str, dict[str, Any]] = {}
        newly_retained_by_region: dict[str,
                                       set[str]] = collections.defaultdict(set)
        tag_keys = (provision_constants.TAG_RAY_CLUSTER_NAME,
                    provision_constants.TAG_SKYPILOT_CLUSTER_NAME)
        for region_scope in region_scopes:
            session = aws_adaptor.session(
                profile=region_scope.credential_profile)
            caller = session.client(
                'sts', region_name=region_scope.region).get_caller_identity()
            if caller.get('Account') != region_scope.aws_account_id:
                raise GuardViolation(
                    'AWS credential profile resolved to another account.')
            client = session.client('ec2', region_name=region_scope.region)
            regional_instances: dict[str, dict[str, Any]] = {}
            for tag_key in tag_keys:
                pages = client.get_paginator('describe_instances').paginate(
                    Filters=[{
                        'Name': 'instance-state-name',
                        'Values': [
                            'pending', 'running', 'stopping', 'stopped',
                            'shutting-down'
                        ],
                    }, {
                        'Name': f'tag:{tag_key}',
                        'Values': [f'{self._service_name}-*'],
                    }])
                for page in pages:
                    reservations = page.get('Reservations')
                    if not isinstance(reservations, list):
                        raise QualificationError(
                            'AWS service instance census is malformed.')
                    for reservation in reservations:
                        raw_instances = (
                            reservation.get('Instances') if isinstance(
                                reservation, collections.abc.Mapping) else None)
                        if not isinstance(raw_instances, list):
                            raise QualificationError(
                                'AWS service instance census is malformed.')
                        for instance in raw_instances:
                            if not isinstance(instance,
                                              collections.abc.Mapping):
                                raise QualificationError(
                                    'AWS service instance census is malformed.')
                            tags = self._tags(instance)
                            cluster_name = self._exact_service_cluster(tags)
                            block_devices = instance.get('BlockDeviceMappings')
                            if (cluster_name is None or
                                    not isinstance(block_devices, list)):
                                raise GuardViolation(
                                    'AWS service instance escaped exact tags.')
                            volume_ids: list[str] = []
                            for block_device in block_devices:
                                ebs = (block_device.get('Ebs') if isinstance(
                                    block_device, collections.abc.Mapping) else
                                       None)
                                if (not isinstance(ebs, collections.abc.Mapping)
                                        or ebs.get('DeleteOnTermination')
                                        is not True or
                                        not isinstance(ebs.get('VolumeId'), str)
                                        or not ebs['VolumeId']):
                                    raise GuardViolation(
                                        'AWS service instance has unsafe EBS.')
                                volume_ids.append(ebs['VolumeId'])
                            placement = instance.get('Placement')
                            state = instance.get('State')
                            lifecycle = instance.get('InstanceLifecycle')
                            canonical = {
                                'availability_zone':
                                    (placement.get('AvailabilityZone')
                                     if isinstance(placement,
                                                   collections.abc.Mapping) else
                                     None),
                                'client_token': instance.get('ClientToken'),
                                'cluster_name_on_cloud': cluster_name,
                                'instance_id': instance.get('InstanceId'),
                                'instance_type': instance.get('InstanceType'),
                                'market': ('spot' if lifecycle == 'spot' else
                                           'on_demand'
                                           if lifecycle is None else lifecycle),
                                'region': region_scope.region,
                                'state': (state.get('Name') if isinstance(
                                    state, collections.abc.Mapping) else None),
                                'volume_ids': tuple(sorted(volume_ids)),
                            }
                            if (any(value is None or value == ''
                                    for value in canonical.values()) or
                                    not isinstance(canonical['state'], str)):
                                raise GuardViolation(
                                    'AWS service instance identity is '
                                    'incomplete.')
                            instance_id = str(canonical['instance_id'])
                            previous = regional_instances.get(instance_id)
                            if previous is None:
                                regional_instances[instance_id] = canonical
                            else:
                                regional_instances[instance_id] = (
                                    _merge_aws_instance_observations(
                                        previous, canonical))
            instance_types = {
                str(instance['instance_type'])
                for instance in regional_instances.values()
            }
            self._attest_instance_types(client, region_scope.region,
                                        instance_types)
            for instance_id, canonical in regional_instances.items():
                canonical['provider_gpu_units'] = self._instance_type_widths[(
                    region_scope.region, str(canonical['instance_type']))]
                previous = instances_by_id.setdefault(instance_id, canonical)
                if previous != canonical:
                    raise GuardViolation(
                        'AWS service instance identity is duplicated.')
                newly_retained_by_region[region_scope.region].update(
                    canonical['volume_ids'])

            prior_retained = set(
                self._retained_volume_ids_by_region[region_scope.region])
            exact_volume_ids = (prior_retained |
                                newly_retained_by_region[region_scope.region])
            volume_queries: list[tuple[bool, list[dict[str, Any]]]] = []
            for tag_key in tag_keys:
                volume_queries.append((False, [{
                    'Name': f'tag:{tag_key}',
                    'Values': [f'{self._service_name}-*'],
                }]))
            sorted_volume_ids = sorted(exact_volume_ids)
            volume_queries.extend((True, [{
                'Name': 'volume-id',
                'Values': sorted_volume_ids[start:start +
                                            _AWS_FILTER_MAX_VALUES],
            }]) for start in range(0, len(sorted_volume_ids),
                                   _AWS_FILTER_MAX_VALUES))
            for exact_lookup, filters in volume_queries:
                pages = client.get_paginator('describe_volumes').paginate(
                    Filters=filters)
                for page in pages:
                    raw_volumes = page.get('Volumes')
                    if not isinstance(raw_volumes, list):
                        raise QualificationError(
                            'AWS service volume census is malformed.')
                    for volume in raw_volumes:
                        if not isinstance(volume, collections.abc.Mapping):
                            raise QualificationError(
                                'AWS service volume census is malformed.')
                        volume_id = volume.get('VolumeId')
                        state = volume.get('State')
                        tags = self._tags(volume)
                        cluster_name = self._exact_service_cluster(tags)
                        has_service_identity_tag = any(
                            tags.get(key) is not None for key in tag_keys)
                        may_be_legacy_receipt = (exact_lookup and
                                                 self._cleanup_mode and
                                                 volume_id in prior_retained and
                                                 not has_service_identity_tag)
                        if (not isinstance(volume_id, str) or not volume_id or
                                not isinstance(state, str) or not state or
                            (cluster_name is None and
                             not may_be_legacy_receipt)):
                            raise GuardViolation(
                                'AWS service volume escaped exact scope.')
                        canonical_volume = {
                            'cluster_name_on_cloud': cluster_name,
                            'region': region_scope.region,
                            'state': state,
                            'volume_id': volume_id,
                        }
                        previous = volumes_by_id.get(volume_id)
                        if previous is None:
                            volumes_by_id[volume_id] = canonical_volume
                        else:
                            volumes_by_id[volume_id] = (
                                _merge_aws_volume_observations(
                                    previous, canonical_volume))
                        newly_retained_by_region[region_scope.region].add(
                            volume_id)

        for region, retained_ids in newly_retained_by_region.items():
            self._retained_volume_ids_by_region[region].update(retained_ids)
        return (tuple(instances_by_id[key] for key in sorted(instances_by_id)),
                tuple(volumes_by_id[key] for key in sorted(volumes_by_id)))

    def _read_region(self, region_scope: AwsRegionScope) -> _AwsRegionCensus:
        """Read one region with state isolated from concurrent siblings."""
        # Each worker owns this child observer, so its otherwise-private state
        # cannot race another region or the parent observer.
        # pylint: disable=protected-access
        retained = self._retained_volume_ids_by_region[region_scope.region]
        child = AwsObserver(profile=self._profile,
                            service_name=self._service_name,
                            scope=self._scope,
                            retained_volume_ids_by_region={
                                region_scope.region: sorted(retained)
                            } if self._cleanup_mode else None)
        child._retained_volume_ids_by_region[region_scope.region].update(
            retained)
        child._instance_type_widths.update({
            key: width
            for key, width in self._instance_type_widths.items()
            if key[0] == region_scope.region
        })
        instances, volumes = child._service_census_serial((region_scope,))
        return _AwsRegionCensus(
            region=region_scope.region,
            service_instances=instances,
            service_volumes=volumes,
            retained_volume_ids=tuple(
                sorted(
                    child._retained_volume_ids_by_region[region_scope.region])),
            instance_type_widths=tuple(
                sorted((instance_type, width)
                       for (region, instance_type
                           ), width in child._instance_type_widths.items()
                       if region == region_scope.region)))

    def _service_census(
            self
    ) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
        """Read frozen regions concurrently and aggregate deterministically."""
        region_scopes = tuple(
            sorted(self._scope.aws_regions, key=lambda value: value.region))
        if not region_scopes:
            return (), ()
        if len(region_scopes) == 1:
            return self._service_census_serial(region_scopes)
        workers = min(_AWS_CENSUS_MAX_WORKERS, len(region_scopes))
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix='paid-aws-census') as executor:
            futures = [
                executor.submit(self._read_region, region_scope)
                for region_scope in region_scopes
            ]
            regions = [future.result() for future in futures]
        instances_by_id: dict[str, dict[str, Any]] = {}
        volumes_by_id: dict[str, dict[str, Any]] = {}
        for expected_scope, result in zip(region_scopes, regions):
            if result.region != expected_scope.region:
                raise GuardViolation('AWS region census order changed.')
            for instance in result.service_instances:
                instance_id = str(instance['instance_id'])
                previous = instances_by_id.setdefault(instance_id, instance)
                if previous != instance:
                    raise GuardViolation(
                        'AWS service instance identity is duplicated.')
            for volume in result.service_volumes:
                volume_id = str(volume['volume_id'])
                previous = volumes_by_id.setdefault(volume_id, volume)
                if previous != volume:
                    raise GuardViolation(
                        'AWS service volume identity is duplicated.')
            self._retained_volume_ids_by_region[result.region].update(
                result.retained_volume_ids)
            for instance_type, width in result.instance_type_widths:
                key = (result.region, instance_type)
                prior_width = self._instance_type_widths.setdefault(key, width)
                if prior_width != width:
                    raise GuardViolation(
                        'AWS provider GPU width changed within one census.')
        return (tuple(instances_by_id[key] for key in sorted(instances_by_id)),
                tuple(volumes_by_id[key] for key in sorted(volumes_by_id)))

    def census(self) -> AwsProviderCensus:
        try:
            service_instances, service_volumes = self._service_census()
        except GuardViolation:
            raise
        except QualificationError:
            raise
        except Exception as error:  # pylint: disable=broad-except
            raise QualificationError('AWS EC2 census failed.') from error
        return AwsProviderCensus(service_instances=service_instances,
                                 service_volumes=service_volumes)

    def retained_volume_ids(self) -> dict[str, list[str]]:
        """Return credential-free disk identities for durable receipts."""
        return {
            region: sorted(volume_ids) for region, volume_ids in sorted(
                self._retained_volume_ids_by_region.items())
        }

    def reduce(
        self, census: AwsProviderCensus,
        identities: collections.abc.Sequence[AwsProviderIdentity]
    ) -> ProviderState:
        return parse_aws_state(identities=identities,
                               profile=self._profile,
                               service_instances=census.service_instances,
                               service_volumes=census.service_volumes)


@dataclasses.dataclass(frozen=True, kw_only=True)
class ControllerIdentity:
    """Exact durable owner of the observed service-controller generation."""

    pid: int
    ip: str
    owner_epoch: int
    incarnation: str


def controller_identity_from_authority(
        authority: collections.abc.Mapping[str, Any]) -> ControllerIdentity:
    """Validate and normalize one PostgreSQL controller-owner fence."""
    pid = authority.get('controller_pid')
    ip = authority.get('controller_ip')
    owner_epoch = authority.get('controller_owner_epoch')
    incarnation = authority.get('controller_incarnation')
    try:
        normalized_incarnation = str(uuid.UUID(str(incarnation)))
    except (AttributeError, TypeError, ValueError) as error:
        raise GuardViolation(
            'Service has no exact controller incarnation.') from error
    if (type(pid) is not int or pid < 1 or not isinstance(ip, str) or not ip or
            type(owner_epoch) is not int or owner_epoch < 1):
        raise GuardViolation('Service has no exact controller owner fence.')
    return ControllerIdentity(pid=pid,
                              ip=ip,
                              owner_epoch=owner_epoch,
                              incarnation=normalized_incarnation)


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidClaimPriorityUnits:
    """Logical GPU units held by live claims at one exact priority."""

    priority: int
    gpu_units: int


@dataclasses.dataclass(frozen=True, kw_only=True)
class DatabaseState:
    """One consistent PostgreSQL authority and telemetry snapshot."""

    service_hash: str
    controller: ControllerIdentity
    paid_debit_units: int
    claimed_units: int
    claim_priority_units: tuple[PaidClaimPriorityUnits, ...]
    waiter_count: int
    demand_units: int
    gcp_provider_identities: tuple[GcpProviderIdentity, ...]
    aws_provider_identities: tuple[AwsProviderIdentity, ...]
    provider_free_unbound_replica_ids: tuple[int, ...] = ()


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidDebitCensus:
    """Rows and physical GPU units still debited by production admission."""

    replicas: tuple[collections.abc.Mapping[str, Any], ...]
    gpu_units: int


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidClaimCensus:
    """Exact priority and immutable-plan attribution of live paid claims."""

    gpu_units: int
    priority_units: tuple[PaidClaimPriorityUnits, ...]


def paid_debit_census(
    replicas: collections.abc.Sequence[collections.abc.Mapping[str, Any]],
) -> PaidDebitCensus:
    """Apply the production paid-capacity cleanup/debit predicates exactly."""
    debit_replicas: list[collections.abc.Mapping[str, Any]] = []
    gpu_units = 0
    try:
        for replica in replicas:
            replica_state = replica['replica_state']
            pool_key = replica['paid_capacity_pool_key']
            if paid_capacity.paid_replica_cleanup_proven(
                    replica_state,
                    sky_down_status_value=replica['sky_down_status']):
                continue
            if not paid_capacity.validate_paid_replica_relational_copies(
                    replica_state, pool_key_value=pool_key):
                continue
            gpu_units += paid_capacity.paid_replica_gpu_units(
                replica_state, pool_key_value=pool_key)
            debit_replicas.append(replica)
    except (KeyError, TypeError,
            paid_capacity.PaidGPUAttributionError) as error:
        raise GuardViolation(
            'Replica rows have no exact paid cleanup/debit attribution.') \
            from error
    return PaidDebitCensus(replicas=tuple(debit_replicas), gpu_units=gpu_units)


def paid_claim_census(
    claims: collections.abc.Sequence[collections.abc.Mapping[str, Any]],
) -> PaidClaimCensus:
    """Require every live claim to retain the offered priority and plan."""
    claimed_units = 0
    units_by_priority: dict[int, int] = {}
    try:
        for claim in claims:
            priority = claim['priority']
            if (type(priority) is not int or priority != _REQUEST_PRIORITY or
                    type(claim['capacity_plan_generation']) is not int or
                    claim['capacity_plan_generation'] <= 0 or
                    not isinstance(claim['capacity_plan_sha256'], str) or
                    claim['capacity_plan_sha256']
                    != claim['persisted_plan_sha256'] or
                    str(claim['capacity_plan_accelerator']).casefold() != 'l4'
                    or type(claim['capacity_plan_units']) is not int or
                    claim['capacity_plan_units'] < 1):
                raise GuardViolation(
                    f'Paid claim is not priority {_REQUEST_PRIORITY} and '
                    'linked to one immutable whole-L4 plan.')
            claimed_units += claim['capacity_plan_units']
            units_by_priority[priority] = (units_by_priority.get(priority, 0) +
                                           claim['capacity_plan_units'])
    except (KeyError, TypeError) as error:
        raise GuardViolation(
            'Paid claim has incomplete priority or plan attribution.') \
            from error
    return PaidClaimCensus(
        gpu_units=claimed_units,
        priority_units=tuple(
            PaidClaimPriorityUnits(priority=priority, gpu_units=gpu_units)
            for priority, gpu_units in sorted(units_by_priority.items())))


_BOUND_REQUEST_PROFILE_FIELDS = (
    'binding_protocol_version',
    'profile_kind',
    'profile_version',
    'profile_digest',
    'capability_cohort_epoch',
    'capability_profile_set_digest',
    'receipt_protocol_version',
)


def _retained_launch_request(
    binding: collections.abc.Mapping[str, Any],
    request_row: sqlalchemy.engine.RowMapping,
) -> tuple[collections.abc.Mapping[str, Any], collections.abc.Mapping[str, Any],
           str]:
    """Correlate a retained API request with its immutable association."""
    if (str(request_row['ordinary_launch_association_id']) != str(
            binding['association_id']) or
            request_row['request_id'] != binding['request_id'] or
            request_row['handler_name']
            != non_pool_launch.NON_POOL_LAUNCH_HANDLER_NAME or
            request_row['user_id'] != binding['tenant_scope'] or
            request_row['cluster_name'] != binding['cluster_name'] or
            any(request_row[field] != binding[field]
                for field in _BOUND_REQUEST_PROFILE_FIELDS)):
        raise ValueError('request correlation mismatch')
    request = request_postgres.request_from_mapping(request_row)
    body = request.request_body
    context = ordinary_launch_binding.bound_context_from_association(binding)
    parsed_context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
        body.extra_launch_context)
    if parsed_context != context:
        raise ValueError('request binding context mismatch')
    config_snapshot = body.override_skypilot_config
    pool_identity = paid_capacity.pool_key_payload(
        str(binding['paid_capacity_pool_key']))
    workspace = binding['service_workspace']
    if (not isinstance(config_snapshot, collections.abc.Mapping) or
            not isinstance(pool_identity, collections.abc.Mapping) or
            not isinstance(workspace, str) or not workspace):
        raise ValueError('request provider scope mismatch')
    return config_snapshot, pool_identity, workspace


def gcp_identity_from_retained_request(
    binding: collections.abc.Mapping[str, Any],
    request_row: sqlalchemy.engine.RowMapping,
    scope: ProviderScope,
) -> GcpProviderIdentity:
    """Recover one exact GCP allocation from its immutable API request."""
    try:
        config_snapshot, pool_identity, workspace = _retained_launch_request(
            binding, request_row)
        pool_region = pool_identity.get('region')
        pool_zone = pool_identity.get('zone')
        gpu_units = _pool_l4_width(pool_identity)
        if (pool_identity.get('cloud') != 'gcp' or
                pool_identity.get('workspace') != workspace or
                not isinstance(pool_region, str) or not pool_region or
                not isinstance(pool_zone, str) or not pool_zone or
                not _gcp_region_matches_zone(pool_region, pool_zone) or
                pool_identity.get('num_nodes') != 1 or
                pool_identity.get('use_spot') is not True or
                not isinstance(pool_identity.get('instance_type'), str) or
                not pool_identity['instance_type'] or
                config_snapshot.get('active_workspace') != workspace or
                workspace != scope.workspace or not _scope_has_catalog_shape(
                    scope,
                    cloud='gcp',
                    region=pool_region,
                    zone=pool_zone,
                    instance_type=pool_identity['instance_type'],
                    width=gpu_units)):
            raise ValueError('request provider scope mismatch')
        managed_instance_group = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'gcp', ('managed_instance_group',),
                region=pool_region,
                workspace=workspace))
        if managed_instance_group is not None:
            raise ValueError('managed instance groups are outside this test')
        request_project = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'gcp', ('project_id',),
                region=pool_region,
                workspace=workspace))
        identity = ordinary_launch_binding.ordinary_paid_gcp_provider_identity(
            binding, project_id=request_project)
        if (identity['project_id'] != scope.project_id or
                identity['workspace'] != scope.workspace or
                identity['region'] != pool_region or
                identity['zone'] != pool_zone or
                identity['instance_type'] != pool_identity['instance_type'] or
                identity['num_nodes'] != 1 or identity['use_spot'] is not True):
            raise ValueError('request-derived provider identity mismatch')
        return GcpProviderIdentity(
            cluster_name_on_cloud=identity['cluster_name_on_cloud'],
            gpu_units_per_instance=gpu_units,
            instance_type=identity['instance_type'],
            project_id=identity['project_id'],
            region=identity['region'],
            workspace=identity['workspace'],
            zone=identity['zone'])
    except (AttributeError, KeyError, TypeError, ValueError,
            ordinary_launch_binding.OrdinaryLaunchBindingConflict) as error:
        raise GuardViolation(
            'Paid replica has no exact retained-request GCP identity.') \
            from error


def aws_identity_from_retained_request(
    binding: collections.abc.Mapping[str, Any],
    request_row: sqlalchemy.engine.RowMapping,
    scope: ProviderScope,
) -> AwsProviderIdentity:
    """Recover one exact AWS allocation from its immutable API request."""
    try:
        config_snapshot, pool_identity, workspace = _retained_launch_request(
            binding, request_row)
        region = pool_identity.get('region')
        zone = pool_identity.get('zone')
        gpu_units = _pool_l4_width(pool_identity)
        if (pool_identity.get('cloud') != 'aws' or
                pool_identity.get('workspace') != workspace or
                not isinstance(region, str) or not region or
                not isinstance(zone, str) or not zone or
                not zone.startswith(region) or
                pool_identity.get('num_nodes') != 1 or
                pool_identity.get('use_spot') is not True or
                not isinstance(pool_identity.get('instance_type'), str) or
                not pool_identity['instance_type'] or
                config_snapshot.get('active_workspace') != workspace or
                workspace != scope.workspace or not _scope_has_catalog_shape(
                    scope,
                    cloud='aws',
                    region=region,
                    zone=zone,
                    instance_type=pool_identity['instance_type'],
                    width=gpu_units)):
            raise ValueError('request provider scope mismatch')
        # Server-controller launch requests intentionally retain only the
        # active workspace.  AwsRegionScope.credential_profile selects the
        # qualifier observer's credentials; it is not durable launch identity.
        # Bind the retained launch to the frozen scope by stable AWS account
        # and placement identity instead.
        identity = ordinary_launch_binding.ordinary_paid_aws_provider_identity(
            binding, credential_profile=None)
        matching_region_scopes = [
            region_scope for region_scope in scope.aws_regions
            if region_scope.region == region
        ]
        if (identity['workspace'] != scope.workspace or
                identity['region'] != region or identity['zone'] != zone or
                identity['instance_type'] != pool_identity['instance_type'] or
                identity['num_nodes'] != 1 or
                identity['use_spot'] is not True or
                len(matching_region_scopes) != 1 or
                matching_region_scopes[0].aws_account_id
                != identity['aws_account_id']):
            raise ValueError('request-derived provider identity mismatch')
        return AwsProviderIdentity(
            aws_account_id=identity['aws_account_id'],
            client_token=identity['client_token'],
            cluster_name_on_cloud=identity['cluster_name_on_cloud'],
            gpu_units_per_instance=gpu_units,
            instance_type=identity['instance_type'],
            num_nodes=identity['num_nodes'],
            region=identity['region'],
            use_spot=identity['use_spot'],
            workspace=identity['workspace'],
            zone=identity['zone'])
    except (AttributeError, KeyError, TypeError, ValueError,
            ordinary_launch_binding.OrdinaryLaunchBindingConflict) as error:
        raise GuardViolation(
            'Paid replica has no exact retained-request AWS identity.') \
            from error


def validate_route_authority(authority: object) -> tuple[str, int]:
    """Validate only live route/lifecycle authority, never a plan head."""
    if not isinstance(authority, collections.abc.Mapping):
        raise QualificationError('Service has no durable incarnation.')
    service_hash = authority.get('service_hash')
    lifecycle_epoch = authority.get('service_lifecycle_epoch')
    route_generation = authority.get('route_generation')
    if not isinstance(service_hash, str) or not service_hash:
        raise QualificationError('Service has no durable incarnation.')
    if (type(lifecycle_epoch) is not int or lifecycle_epoch < 1 or
            type(route_generation) is not int or route_generation < 1 or
            authority.get('route_fresh') is not True or
            authority.get('route_service_hash') != service_hash or
            authority.get('route_lifecycle_epoch') != lifecycle_epoch):
        raise QualificationError(
            'Service lacks fresh route/lifecycle authority.')
    return service_hash, lifecycle_epoch


def _is_projected_paid_success(
        binding: collections.abc.Mapping[str, Any]) -> bool:
    return (binding.get('resolution')
            == ordinary_launch_binding.Resolution.PROJECTED.value and
            binding.get('reconciliation_outcome')
            == ordinary_launch_binding.ReconciliationOutcome.PROJECTED.value and
            binding.get('projected_at') is not None and
            binding.get('service_job_id') is not None)


def _is_selectable_settled_paid_binding(
        binding: collections.abc.Mapping[str, Any]) -> bool:
    """Whether pointerless retained history identifies one paid attempt."""
    if _is_projected_paid_success(binding):
        return True
    # An exact provider-absence projection deliberately clears the replica's
    # current association pointer without recording a service job.  Delegate
    # this exceptional shape to the production read-side validator instead of
    # duplicating its quiescence and provider-evidence contract in the harness.
    return bool(
        binding.get('resolution')
        == ordinary_launch_binding.Resolution.PROJECTED.value and
        binding.get('reconciliation_outcome')
        == ordinary_launch_binding.ReconciliationOutcome.PROJECTED.value and
        binding.get('provider_evidence')
        == ordinary_launch_binding.ProviderEvidence.ABSENT.value and
        binding.get('service_job_id') is None and
        ordinary_launch_binding.settled_association_proves_execution_quiescence(
            binding))


def _is_exact_provider_free_unbound_paid_debit(
    replica: collections.abc.Mapping[str, Any],
    claim: collections.abc.Mapping[str, Any] | None,
    bindings: collections.abc.Sequence[collections.abc.Mapping[str, Any]],
) -> bool:
    """Whether one unbound paid debit permits no possible provider I/O.

    This includes the initial atomic claim+replica Phase-A pair and a settled
    pre-effect attempt while its logical replica awaits retry or cleanup.  A
    pre-effect settlement atomically releases its claim before asynchronous
    retirement removes the provider-free row.  The two shapes are mutually
    exclusive: no history requires a claim, while exact pre-effect history
    requires claim absence.  Provider-present or ambiguous history never
    enters this path.
    """
    record_id = replica.get('replica_record_id')
    try:
        canonical_record_id = str(uuid.UUID(str(record_id)))
        replica_state_version = replica.get('replica_state_version')
        replica_state = replica.get('replica_state')
        if (type(replica_state_version) is not int or  # pylint: disable=unidiomatic-typecheck
                not isinstance(replica_state, dict)):
            return False
        info = serve_state.decode_replica_state_for_authority(
            replica_state_version, replica_state)
        relationally_paid = (
            paid_capacity.validate_paid_replica_relational_copies(
                replica_state,
                pool_key_value=replica.get('paid_capacity_pool_key')))
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError,
            paid_capacity.PaidGPUAttributionError):
        return False
    exact_history = [
        binding for binding in bindings
        if binding.get('replica_id') == replica.get('replica_id') and
        str(binding.get('replica_record_id')) == canonical_record_id
    ]
    pool_key = replica.get('paid_capacity_pool_key')
    claim_matches = bool(
        isinstance(claim, collections.abc.Mapping) and
        claim.get('replica_id') == replica.get('replica_id') and
        claim.get('pool_key') == pool_key)
    history_is_provider_free = False
    if len(exact_history) == 1:
        predecessor = exact_history[0]
        history_is_provider_free = bool(
            predecessor.get('paid_capacity_pool_key') == pool_key and
            type(predecessor.get('launch_generation')) is int and
            predecessor.get('launch_generation') == 1 and
            predecessor.get('cancel_reason') is None and
            predecessor.get('cancel_requested_at') is None and
            predecessor.get('resolution')
            == ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL.value and
            predecessor.get('reconciliation_outcome') == ordinary_launch_binding
            .ReconciliationOutcome.PRE_EFFECT_TERMINAL.value and
            ordinary_launch_binding.
            settled_association_proves_execution_quiescence(predecessor))
    funding_is_exact = (claim_matches if not exact_history else
                        history_is_provider_free and claim is None)
    return bool(
        canonical_record_id == str(record_id) and
        replica.get('ordinary_launch_association_id') is None and
        (not exact_history or history_is_provider_free) and funding_is_exact and
        replica.get('status') in ('PENDING', 'PROVISIONING', 'SHUTTING_DOWN',
                                  'FAILED_CLEANUP') and
        replica.get('is_spot') is True and relationally_paid and
        info.replica_id == replica.get('replica_id') and
        info.replica_record_id == canonical_record_id and
        info.version == replica.get('version') and
        info.cluster_name == replica.get('cluster_name') and
        info.status.value == replica.get('status') and
        info.is_spot is replica.get('is_spot') and
        info.paid_capacity_pool_key == pool_key and
        ordinary_launch_binding.classify_non_pool_launch_profile(info)
        is ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID)


def select_replica_binding(
    replica: collections.abc.Mapping[str, Any],
    bindings: collections.abc.Sequence[collections.abc.Mapping[str, Any]],
) -> collections.abc.Mapping[str, Any]:
    """Select one current binding while permitting legitimate retry history."""
    record_id = replica.get('replica_record_id')
    candidates = [
        binding for binding in bindings
        if binding.get('replica_id') == replica.get('replica_id') and
        str(binding.get('replica_record_id')) == str(record_id)
    ]
    pointer = replica.get('ordinary_launch_association_id')
    if pointer is not None:
        selected = [
            binding for binding in candidates
            if str(binding.get('association_id')) == str(pointer)
        ]
    else:
        settled = [
            binding for binding in candidates
            if _is_selectable_settled_paid_binding(binding)
        ]
        if not settled:
            selected = []
        else:
            latest_generation = max(
                binding.get('launch_generation', -1) for binding in settled)
            selected = [
                binding for binding in settled
                if binding.get('launch_generation') == latest_generation
            ]
    if len(selected) != 1:
        raise GuardViolation(
            'Paid replica has no unique current or latest settled binding.')
    return selected[0]


class PostgresObserver:
    """Read durable provider authority without re-gating committed claims.

    A paid claim points to its immutable plan.  The mutable plan head is not
    consulted after that commit; only current route/lifecycle authority must
    remain fresh while traffic is being qualified.
    """

    def __init__(self, database_url: str, service_name: str) -> None:
        url = sqlalchemy.engine.make_url(database_url)
        if not url.drivername.startswith('postgresql'):
            raise QualificationError('Paid qualification requires PostgreSQL.')
        self._engine = sqlalchemy.create_engine(database_url,
                                                pool_pre_ping=True)
        self._service_name = service_name
        self._provider_scope: ProviderScope | None = None

    def close(self) -> None:
        self._engine.dispose()

    def bind_provider_scope(self, scope: ProviderScope) -> None:
        """Retain a frozen scope for post-service cleanup observation."""
        if (self._provider_scope is not None and self._provider_scope != scope):
            raise GuardViolation('Provider scope changed after it was frozen.')
        self._provider_scope = scope

    def provider_scope(self) -> ProviderScope:
        """Freeze provider scope from the current committed service version."""
        with self._engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.text('''
                    SELECT s.hash AS service_hash,
                           s.resource_scope,
                           s.lifecycle_epoch AS service_lifecycle_epoch,
                           s.current_version,
                           s.workspace,
                           v.controller_config,
                           v.controller_config_digest,
                           v.controller_config_snapshot_id,
                           v.placement_catalog,
                           v.yaml_content
                    FROM services AS s
                    JOIN version_specs AS v
                      ON v.service_name = s.name
                     AND v.version = s.current_version
                    WHERE s.name = :name
                      AND v.yaml_content IS NOT NULL
                      AND v.quarantined_at IS NULL
                      AND v.retired_at IS NULL
                '''), {
                    'name': self._service_name
                }).mappings().one_or_none()
        if row is None:
            raise GuardViolation(
                'Service has no current committed provider-scope authority.')
        scope = provider_scope_from_controller_config(row)
        self._provider_scope = scope
        return scope

    def _retained_launch_rows(
        self,
        connection: sqlalchemy.engine.Connection,
        scope: ProviderScope,
    ) -> tuple[list[sqlalchemy.engine.RowMapping],
               list[sqlalchemy.engine.RowMapping]]:
        requests = connection.execute(
            sqlalchemy.text('''
                SELECT request.*
                FROM api_requests AS request
                JOIN serve_ordinary_launch_associations AS binding
                  ON binding.association_id =
                     request.ordinary_launch_association_id
                 AND binding.request_id = request.request_id
                WHERE binding.service_name = :name
                  AND binding.service_hash = :service_hash
                  AND binding.profile_kind = :profile_kind
                  AND binding.binding_protocol_version = :protocol
            '''), {
                'name': self._service_name,
                'service_hash': scope.service_hash,
                'profile_kind': (ordinary_launch_binding.
                                 NonPoolLaunchProfileKind.ORDINARY_PAID.value),
                'protocol':
                    (ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
            }).mappings().all()
        bindings = connection.execute(
            sqlalchemy.text('''
                SELECT binding.*
                FROM serve_ordinary_launch_associations AS binding
                WHERE binding.service_name = :name
                  AND binding.service_hash = :service_hash
                  AND binding.profile_kind = :profile_kind
                  AND binding.binding_protocol_version = :protocol
                ORDER BY binding.replica_id, binding.launch_generation
            '''), {
                'name': self._service_name,
                'service_hash': scope.service_hash,
                'profile_kind': (ordinary_launch_binding.
                                 NonPoolLaunchProfileKind.ORDINARY_PAID.value),
                'protocol':
                    (ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
            }).mappings().all()
        return list(requests), list(bindings)

    @staticmethod
    def _request_by_association(
        requests: collections.abc.Sequence[sqlalchemy.engine.RowMapping],
    ) -> dict[str, sqlalchemy.engine.RowMapping]:
        result: dict[str, sqlalchemy.engine.RowMapping] = {}
        for request in requests:
            association_id = str(request['ordinary_launch_association_id'])
            if association_id in result:
                raise GuardViolation(
                    'A paid binding has multiple retained API requests.')
            result[association_id] = request
        return result

    def _aws_identities_from_rows(
        self,
        bindings: collections.abc.Sequence[sqlalchemy.engine.RowMapping],
        request_by_association: collections.abc.Mapping[
            str, sqlalchemy.engine.RowMapping],
        scope: ProviderScope,
    ) -> tuple[AwsProviderIdentity, ...]:
        identities: list[AwsProviderIdentity] = []
        for binding in bindings:
            pool = paid_capacity.pool_key_payload(
                str(binding['paid_capacity_pool_key']))
            if not isinstance(pool, collections.abc.Mapping):
                raise GuardViolation('Paid binding has no exact provider pool.')
            if pool.get('cloud') != 'aws':
                continue
            request = request_by_association.get(str(binding['association_id']))
            if request is None:
                raise GuardViolation(
                    'AWS paid binding lost its retained API request.')
            identities.append(
                aws_identity_from_retained_request(binding, request, scope))
        tokens = [identity.client_token for identity in identities]
        if len(tokens) != len(set(tokens)):
            raise GuardViolation('AWS paid bindings reuse one ClientToken.')
        return tuple(
            sorted(identities, key=lambda identity: identity.client_token))

    def cleanup_debits(self) -> tuple[int, int, int]:
        """Return production-equivalent debits, claims, and live waiters."""
        with self._engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(
                    'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
                ))
            replicas = connection.execute(
                sqlalchemy.text('''
                    SELECT replica.replica_state,
                           replica.paid_capacity_pool_key,
                           replica.sky_down_status
                    FROM replicas AS replica
                    WHERE replica.service_name = :name
                '''), {
                    'name': self._service_name
                }).mappings().all()
            claim_count = connection.execute(
                sqlalchemy.text('''
                    SELECT count(*) FROM paid_capacity_claims
                    WHERE service_name = :name
                '''), {
                    'name': self._service_name
                }).scalar_one()
            waiter_count = connection.execute(
                sqlalchemy.text('''
                    SELECT count(*) FROM paid_capacity_waiters
                    WHERE service_name = :name
                      AND heartbeat_at >= EXTRACT(EPOCH FROM clock_timestamp())
                                             - 45
                '''), {
                    'name': self._service_name
                }).scalar_one()
        return (paid_debit_census(replicas).gpu_units, int(claim_count),
                int(waiter_count))

    def request_telemetry(self) -> RequestTelemetry:
        """Read the same durable request reduction shown by the service UI."""
        scope = self._provider_scope
        if scope is None:
            raise QualificationError('Provider scope was not frozen.')
        summary = demand_state.get_request_summary(self._service_name,
                                                   scope.service_hash,
                                                   engine=self._engine)
        ledger = async_request_ledger.get_summary(self._service_name,
                                                  scope.service_hash,
                                                  engine=self._engine)
        if (ledger.get('available') is not True or
                ledger.get('service_hash') != scope.service_hash or
                not isinstance(ledger.get('state_counts'), dict)):
            raise QualificationError(
                'Exact async-ledger request summary is unavailable.')
        return request_telemetry_from_summary(summary, ledger['state_counts'])

    def snapshot(
        self,
        load_balancer: 'LoadBalancerState',
        *,
        require_complete_demand_report: bool = True,
    ) -> DatabaseState:
        scope = self._provider_scope
        if scope is None:
            raise QualificationError('Provider scope was not frozen.')
        with self._engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(
                    'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
                ))
            authority = connection.execute(
                sqlalchemy.text('''
                    SELECT s.hash AS service_hash,
                           s.lifecycle_epoch AS service_lifecycle_epoch,
                           s.current_version,
                           s.workspace,
                           s.controller_pid,
                           s.controller_ip,
                           s.controller_incarnation,
                           s.controller_owner_epoch,
                           s.lb_ha_enabled,
                           s.lb_active_slot,
                           s.lb_cutover_generation,
                           s.lb_cutover_phase,
                           v.controller_config_digest,
                           v.controller_config_snapshot_id,
                           v.placement_catalog,
                           v.yaml_content,
                           h.generation AS route_generation,
                           h.valid_until > clock_timestamp() AS route_fresh,
                           r.service_hash AS route_service_hash,
                           r.service_lifecycle_epoch AS route_lifecycle_epoch
                    FROM services AS s
                    JOIN version_specs AS v
                     ON v.service_name = s.name
                     AND v.version = s.current_version
                     AND v.yaml_content IS NOT NULL
                     AND v.quarantined_at IS NULL
                     AND v.retired_at IS NULL
                    LEFT JOIN serve_route_heads AS h
                      ON h.service_name = s.name
                    LEFT JOIN serve_route_snapshots AS r
                      ON r.service_name = h.service_name
                     AND r.generation = h.generation
                    WHERE s.name = :name
                '''), {
                    'name': self._service_name
                }).mappings().one_or_none()
            service_hash, lifecycle_epoch = validate_route_authority(authority)
            controller_identity = controller_identity_from_authority(authority)
            if (service_hash != scope.service_hash or
                    lifecycle_epoch != scope.lifecycle_epoch or
                    authority['current_version'] != scope.service_version or
                    authority['workspace'] != scope.workspace or
                    authority['controller_config_digest']
                    != scope.controller_config_digest or
                    authority['controller_config_snapshot_id']
                    != scope.controller_config_snapshot_id or
                    not isinstance(authority['placement_catalog'], dict) or
                    hashlib.sha256(rfc8785.dumps(
                        authority['placement_catalog'])).hexdigest()
                    != scope.placement_catalog_sha256 or
                    not isinstance(authority['yaml_content'], str) or
                    hashlib.sha256(
                        authority['yaml_content'].encode('utf-8')).hexdigest()
                    != scope.service_yaml_sha256):
                raise GuardViolation(
                    'Service provider scope changed during qualification.')
            replicas = connection.execute(
                sqlalchemy.text('''
                    SELECT replica.replica_id, replica.cluster_name,
                           replica.status, replica.is_spot,
                           replica.replica_state_version, replica.version,
                           replica.paid_capacity_pool_key,
                           replica.ordinary_launch_association_id,
                           replica.replica_state ->> 'replica_record_id'
                               AS replica_record_id,
                           replica.replica_state,
                           replica.sky_down_status,
                           replica.created_at
                    FROM replicas AS replica
                    WHERE replica.service_name = :name
                    ORDER BY replica.replica_id
                '''), {
                    'name': self._service_name
                }).mappings().all()
            requests, bindings = self._retained_launch_rows(connection, scope)
            claims = connection.execute(
                sqlalchemy.text('''
                    SELECT c.replica_id, c.pool_key, c.claimed_at, c.priority,
                           c.capacity_plan_generation,
                           c.capacity_plan_sha256,
                           c.capacity_plan_accelerator,
                           c.capacity_plan_units,
                           p.content_sha256 AS persisted_plan_sha256
                    FROM paid_capacity_claims AS c
                    LEFT JOIN serve_capacity_plans AS p
                      ON p.service_name = c.service_name
                     AND p.service_hash = c.service_hash
                     AND p.generation = c.capacity_plan_generation
                    WHERE c.service_name = :name
                      AND c.service_hash = :service_hash
                    ORDER BY c.replica_id
                '''), {
                    'name': self._service_name,
                    'service_hash': service_hash,
                }).mappings().all()
            waiter_count = connection.execute(
                sqlalchemy.text('''
                    SELECT count(*)
                    FROM paid_capacity_waiters
                    WHERE service_name = :name AND service_hash = :service_hash
                      AND heartbeat_at >= EXTRACT(EPOCH FROM clock_timestamp())
                                             - 45
                '''), {
                    'name': self._service_name,
                    'service_hash': service_hash,
                }).scalar_one()
            demand_reports = connection.execute(
                sqlalchemy.text('''
                    SELECT reporter_session_id, lb_session_id, lb_slot,
                           received_at, complete, payload
                    FROM serve_lb_demand_reports
                    WHERE service_name = :name
                      AND service_hash = :service_hash
                      AND valid_until > clock_timestamp()
                '''), {
                    'name': self._service_name,
                    'service_hash': service_hash,
                }).mappings().all()
        debits = paid_debit_census(replicas)
        replica_ids = {row['replica_id'] for row in debits.replicas}
        claim_ids = {row['replica_id'] for row in claims}
        if not claim_ids.issubset(replica_ids):
            raise GuardViolation(
                'An unresolved paid claim has no debit-bearing replica.')
        claim_census = paid_claim_census(claims)
        claim_by_replica_id = {claim['replica_id']: claim for claim in claims}
        if len(claim_by_replica_id) != len(claims):
            raise GuardViolation('Paid claims have duplicate replica IDs.')
        request_by_association = self._request_by_association(requests)
        aws_identities = self._aws_identities_from_rows(bindings,
                                                        request_by_association,
                                                        scope)
        aws_identities_by_token = {
            identity.client_token: identity for identity in aws_identities
        }
        gcp_identities: dict[str, GcpProviderIdentity] = {}
        provider_free_unbound_replica_ids: list[int] = []
        for replica in debits.replicas:
            try:
                binding = select_replica_binding(replica, bindings)
            except GuardViolation:
                claim = claim_by_replica_id.get(replica['replica_id'])
                if not _is_exact_provider_free_unbound_paid_debit(
                        replica, claim, bindings):
                    raise
                provider_free_unbound_replica_ids.append(replica['replica_id'])
                continue
            try:
                context = ordinary_launch_binding.bound_context_from_association(
                    binding)
                if (not isinstance(
                        context,
                        ordinary_launch_binding.BoundNonPoolLaunchContext) or
                        context.profile.kind is not ordinary_launch_binding.
                        NonPoolLaunchProfileKind.ORDINARY_PAID or
                        context.profile.authorization_kind
                        is not ordinary_launch_binding.
                        NonPoolLaunchAuthorizationKind.PAID_CAPACITY_CLAIM or
                        binding['service_lifecycle_epoch'] != lifecycle_epoch or
                        context.profile.authorization_reference
                        != f'paid-capacity:{service_hash}:'
                        f'{context.replica_record_id}:'
                        f'{binding["paid_capacity_pool_key"]}'):
                    raise ValueError('binding identity mismatch')
                request = request_by_association.get(
                    str(binding['association_id']))
                if request is None:
                    raise ValueError('retained request is unavailable')
                pool = paid_capacity.pool_key_payload(
                    str(binding['paid_capacity_pool_key']))
                if not isinstance(pool, collections.abc.Mapping):
                    raise ValueError('paid pool identity is unavailable')
                cloud = pool.get('cloud')
                if cloud == 'gcp':
                    identity: GcpProviderIdentity | AwsProviderIdentity = (
                        gcp_identity_from_retained_request(
                            binding, request, scope))
                elif cloud == 'aws':
                    identity = aws_identity_from_retained_request(
                        binding, request, scope)
                    if aws_identities_by_token.get(
                            identity.client_token) != identity:
                        raise ValueError('AWS retained identity is unstable')
                else:
                    raise ValueError('paid pool uses an unqualified provider')
            except (KeyError, TypeError, ValueError,
                    ordinary_launch_binding.OrdinaryLaunchBindingConflict
                   ) as error:
                raise GuardViolation(
                    'Paid replica has no exact durable provider launch binding.') \
                    from error
            if (replica['is_spot'] is not True or
                    not replica['replica_record_id'] or
                    binding['paid_capacity_pool_key']
                    != replica['paid_capacity_pool_key']):
                raise GuardViolation(
                    'Paid replica has no exact retained Spot launch binding.')
            claim = claim_by_replica_id.get(replica['replica_id'])
            if (claim is not None and claim['capacity_plan_units']
                    != identity.gpu_units_per_instance):
                raise GuardViolation(
                    'Paid claim units disagree with its provider shape.')
            if isinstance(identity, GcpProviderIdentity):
                cloud_name = identity.cluster_name_on_cloud
                if cloud_name in gcp_identities:
                    raise GuardViolation(
                        'Paid replicas share one durable GCP provider identity.'
                    )
                gcp_identities[cloud_name] = identity
        demand_report = select_route_authoritative_report(
            authority,
            demand_reports,
            load_balancer,
            require_complete=require_complete_demand_report)
        return DatabaseState(
            service_hash=service_hash,
            controller=controller_identity,
            paid_debit_units=debits.gpu_units,
            claimed_units=claim_census.gpu_units,
            claim_priority_units=claim_census.priority_units,
            waiter_count=int(waiter_count),
            demand_units=demand_units(demand_report['payload']),
            gcp_provider_identities=tuple(
                sorted(gcp_identities.values(),
                       key=lambda identity: identity.cluster_name_on_cloud)),
            aws_provider_identities=aws_identities,
            provider_free_unbound_replica_ids=tuple(
                sorted(provider_free_unbound_replica_ids)),
        )


@dataclasses.dataclass(frozen=True, kw_only=True)
class LoadBalancerState:
    """Authenticated data-plane demand and readiness snapshot."""

    service_hash: str
    demand_units: int
    ready_replicas: int
    pod_uid: str
    slot: str
    role_generation: int
    unique_job_arrivals_60s: int = 0
    unique_job_arrivals_300s: int = 0
    headerless_arrivals_60s: int = 0
    headerless_arrivals_300s: int = 0
    offered_arrival_tracking_saturated: bool = False


def select_route_authoritative_report(
    authority: collections.abc.Mapping[str, Any],
    reports: collections.abc.Sequence[collections.abc.Mapping[str, Any]],
    load_balancer: LoadBalancerState,
    *,
    require_complete: bool = True,
) -> collections.abc.Mapping[str, Any]:
    """Select only the report belonging to the LB that served this probe.

    Scale-out measures provider startup while traffic is intentionally
    saturating replicas.  A routed report may therefore be incomplete until
    every new replica has produced its first in-flight sample.  That must not
    hide already-running provider capacity.  Baseline and drain observations
    retain the complete-report requirement because they prove exact zero.
    """
    if (authority.get('lb_ha_enabled') != 1 or
            authority.get('lb_cutover_phase') != 'STABLE' or
            authority.get('lb_active_slot') != load_balancer.slot or
            authority.get('lb_cutover_generation')
            != load_balancer.role_generation):
        raise QualificationError(
            'HTTP probe does not match stable PostgreSQL LB authority.')
    candidates = []
    for report in reports:
        payload = report.get('payload')
        if (report.get('lb_session_id') == load_balancer.pod_uid and
                report.get('lb_slot') == load_balancer.slot and
                isinstance(payload, collections.abc.Mapping) and
                payload.get('applied_role') == 'ACTIVE' and
                payload.get('applied_generation')
                == load_balancer.role_generation):
            candidates.append(report)
    if not candidates:
        raise QualificationError(
            'Routed ACTIVE load balancer has no fresh PostgreSQL report.')
    received_at_values: list[datetime.datetime] = []
    for report in candidates:
        value = report.get('received_at')
        if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
                value.utcoffset() is None):
            raise QualificationError(
                'Routed load-balancer report lacks an exact receipt time.')
        received_at_values.append(value)
    newest_at = max(received_at_values)
    newest = [
        report for report in candidates
        if report.get('received_at') == newest_at
    ]
    if len(newest) != 1:
        raise QualificationError(
            'Routed load-balancer report is ambiguous or incomplete.')
    if require_complete and newest[0].get('complete') is not True:
        raise QualificationError(
            'Routed load-balancer report is ambiguous or incomplete.')
    return newest[0]


class HttpObserver:
    """Read the authenticated load-balancer capacity contract."""

    def __init__(self, endpoint: str, token: str) -> None:
        self._capacity_url = endpoint.rstrip('/') + '/_lb/capacity'
        self._token = token

    async def _authentication_ready(self,
                                    session: aiohttp.ClientSession) -> bool:
        """Return whether one reachable endpoint probe is authenticated."""
        async with session.get(self._capacity_url) as response:
            await response.read()
            if response.status in _RETRYABLE_STATUSES:
                return False
            if response.status not in (401, 403):
                raise QualificationError(
                    'Data-plane authentication is not enforced.')
        async with session.get(self._capacity_url,
                               headers={_AUTH_HEADER: f'Bearer {self._token}'
                                       }) as response:
            await response.read()
            if response.status in _RETRYABLE_STATUSES:
                return False
            if response.status != 200:
                raise QualificationError(
                    f'Authenticated capacity probe returned {response.status}.')
        return True

    async def prove_authentication(self) -> None:
        # A newly published AWS NLB hostname can precede DNS propagation and
        # listener readiness.  Keep this startup boundary distinct from the
        # authentication verdict: only transport/readiness failures retry.
        deadline = (time.monotonic() + _ENDPOINT_AUTHENTICATION_TIMEOUT_SECONDS)
        last_transient: BaseException | None = None
        timeout = aiohttp.ClientTimeout(
            total=_ENDPOINT_AUTHENTICATION_REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                started = time.monotonic()
                try:
                    if await self._authentication_ready(session):
                        return
                    last_transient = None
                except (aiohttp.ClientConnectionError,
                        asyncio.TimeoutError) as error:
                    last_transient = error
                now = time.monotonic()
                if now >= deadline:
                    raise QualificationError(
                        'Capacity endpoint did not become reachable and ready '
                        'before the authentication deadline.'
                    ) from last_transient
                await asyncio.sleep(
                    max(
                        0,
                        min(deadline, started +
                            _ENDPOINT_AUTHENTICATION_POLL_SECONDS) - now))
                if time.monotonic() >= deadline:
                    raise QualificationError(
                        'Capacity endpoint did not become reachable and ready '
                        'before the authentication deadline.'
                    ) from last_transient

    async def snapshot(self) -> LoadBalancerState:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    self._capacity_url,
                    headers={_AUTH_HEADER: f'Bearer {self._token}'},
                    timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status != 200:
                    raise QualificationError(
                        f'Capacity probe returned HTTP {response.status}.')
                payload = await response.json()
        service_hash = payload.get('service_incarnation')
        if not isinstance(service_hash, str) or not service_hash:
            raise QualificationError('Capacity endpoint lacks an incarnation.')
        ready = payload.get('ready_replicas')
        if type(ready) is not int or ready < 0:
            raise QualificationError('Capacity endpoint has invalid readiness.')
        pod_uid = payload.get('lb_pod_uid')
        slot = payload.get('lb_slot')
        role_generation = payload.get('lb_role_generation')
        offered_count_fields = (
            'unique_job_arrivals_60s',
            'unique_job_arrivals_300s',
            'headerless_arrivals_60s',
            'headerless_arrivals_300s',
        )
        offered_counts = {
            field: payload.get(field) for field in offered_count_fields
        }
        if (not isinstance(pod_uid, str) or not pod_uid or
                slot not in ('a', 'b') or payload.get('lb_role') != 'ACTIVE' or
                type(role_generation) is not int or role_generation < 1 or
                payload.get('synced') is not True or
                payload.get('draining') is not False or any(
                    type(value) is not int or value < 0
                    for value in offered_counts.values()) or
                type(payload.get('offered_arrival_tracking_saturated'))
                is not bool):
            raise QualificationError(
                'Capacity endpoint lacks routed ACTIVE LB authority.')
        return LoadBalancerState(service_hash=service_hash,
                                 demand_units=demand_units(payload),
                                 ready_replicas=ready,
                                 pod_uid=pod_uid,
                                 slot=slot,
                                 role_generation=role_generation,
                                 **offered_counts,
                                 offered_arrival_tracking_saturated=payload[
                                     'offered_arrival_tracking_saturated'])


@dataclasses.dataclass(frozen=True, kw_only=True)
class Observation:
    """One composed database, provider, and data-plane observation."""

    observed_started_at: float
    observed_started_monotonic: float
    observed_at: float
    observed_monotonic: float
    database: DatabaseState
    provider: ProviderState
    load_balancer: LoadBalancerState

    def is_exact_zero(self) -> bool:
        return (self.database.paid_debit_units == 0 and
                self.database.claimed_units == 0 and
                self.database.waiter_count == 0 and
                not self.database.provider_free_unbound_replica_ids and
                self.database.demand_units == 0 and
                self.provider.instance_count == 0 and
                self.provider.disk_count == 0 and
                self.provider.inflight_operation_count == 0 and
                self.load_balancer.demand_units == 0 and
                self.load_balancer.ready_replicas == 0)


async def _sleep_after_observation(observation: Observation,
                                   poll_seconds: float) -> None:
    """Subtract census latency instead of serially adding it to polling."""
    await asyncio.sleep(
        max(
            0, poll_seconds - (observation.observed_monotonic -
                               observation.observed_started_monotonic)))


def validate_observation(
        observation: Observation,
        profile: Profile,
        expectation: ProviderExpectation | None = None) -> None:
    if expectation is None:
        expectation = provider_expectation(profile, None)
    if (not math.isfinite(observation.observed_started_at) or
            not math.isfinite(observation.observed_at) or
            observation.observed_started_at <= 0 or
            not math.isfinite(observation.observed_started_monotonic) or
            not math.isfinite(observation.observed_monotonic) or
            observation.observed_started_monotonic < 0 or
            observation.observed_monotonic
            < observation.observed_started_monotonic):
        raise QualificationError(
            'Provider observation has an invalid sample interval.')
    database = observation.database
    provider = observation.provider
    gcp_identities = {
        identity.cluster_name_on_cloud: identity
        for identity in database.gcp_provider_identities
    }
    aws_identities = database.aws_provider_identities
    if (('gcp' not in expectation.providers and gcp_identities) or
        ('aws' not in expectation.providers and aws_identities)):
        raise GuardViolation(
            'Durable launch binding exists outside the qualification scope.')
    bound_cluster_names = frozenset(gcp_identities) | frozenset(
        identity.cluster_name_on_cloud for identity in aws_identities)
    if database.service_hash != observation.load_balancer.service_hash:
        raise QualificationError(
            'PostgreSQL and load balancer incarnations differ.')
    if (database.claimed_units > profile.max_units or
            database.paid_debit_units > profile.max_units):
        raise GuardViolation('PostgreSQL capacity exceeded the armed cap.')
    if provider.gpu_units > profile.max_units:
        raise GuardViolation('Provider capacity exceeded the armed GPU cap.')
    if provider.instance_count > profile.max_replicas:
        raise GuardViolation(
            'Provider capacity exceeded the armed physical-instance cap.')
    if tuple(cloud.cloud for cloud in provider.clouds) != ('gcp', 'aws'):
        raise QualificationError(
            'Provider census lacks the canonical AWS/GCP projection.')
    for cloud in provider.clouds:
        if cloud.cloud not in expectation.providers and (
                cloud.instance_count != 0 or cloud.gpu_units != 0 or
                cloud.disk_count != 0 or cloud.inflight_operation_count != 0):
            raise GuardViolation(
                'Provider effect exists outside the qualification scope.')
    gcp = provider.cloud('gcp')
    aws = provider.cloud('aws')
    if (gcp.instance_count > len(gcp_identities) or
            gcp.disk_count > len(gcp_identities) or
            gcp.inflight_operation_count > len(gcp_identities) or
            gcp.gpu_units > sum(identity.gpu_units_per_instance
                                for identity in gcp_identities.values()) or
            aws.instance_count > len(aws_identities) or
            aws.gpu_units > sum(identity.gpu_units_per_instance
                                for identity in aws_identities)):
        raise GuardViolation('Provider effects exceed durable launch bindings.')
    unbound_clusters = provider.cluster_names - bound_cluster_names
    if unbound_clusters:
        raise GuardViolation(
            'Provider effect exists without a durable launch binding.')


class Observer:
    """Compose raw provider state with newer durable/data-plane evidence."""

    def __init__(self, *, postgres: PostgresObserver, gcp: GcpObserver | None,
                 aws: AwsObserver | None, http: HttpObserver) -> None:
        self._postgres = postgres
        self._gcp = gcp
        self._aws = aws
        self._http = http

    async def request_telemetry(self) -> RequestTelemetry:
        return await asyncio.to_thread(self._postgres.request_telemetry)

    async def snapshot(
        self,
        *,
        require_complete_demand_report: bool = True,
    ) -> Observation:
        observed_started_at = time.time()
        observed_started_monotonic = time.monotonic()
        # Capture raw provider state first, then the durable authorization used
        # to classify it.  Since a binding commit precedes its provider effect,
        # this avoids both logical-name prefix guesses and an old-DB/new-VM
        # ordering manufactured by the observer itself.
        provider_reads = []
        if self._gcp is not None:
            provider_reads.append(asyncio.to_thread(self._gcp.census))
        if self._aws is not None:
            provider_reads.append(asyncio.to_thread(self._aws.census))
        raw_censuses = await asyncio.gather(*provider_reads)
        census_index = 0
        gcp_census = None
        aws_census = None
        if self._gcp is not None:
            gcp_census = raw_censuses[census_index]
            census_index += 1
        if self._aws is not None:
            aws_census = raw_censuses[census_index]
        load_balancer = await self._http.snapshot()
        database = await asyncio.to_thread(
            self._postgres.snapshot,
            load_balancer,
            require_complete_demand_report=require_complete_demand_report)
        gcp_identities = {
            identity.cluster_name_on_cloud: identity
            for identity in database.gcp_provider_identities
        }
        gcp_state = (empty_provider_state('gcp') if self._gcp is None else
                     self._gcp.reduce(gcp_census, gcp_identities))
        aws_state = (empty_provider_state('aws')
                     if self._aws is None else self._aws.reduce(
                         aws_census, database.aws_provider_identities))
        provider = combine_provider_states(gcp_state, aws_state)
        return Observation(
            observed_started_at=observed_started_at,
            observed_started_monotonic=(observed_started_monotonic),
            observed_at=time.time(),
            observed_monotonic=time.monotonic(),
            database=database,
            provider=provider,
            load_balancer=load_balancer)


@dataclasses.dataclass
class Progress:
    """Strict scale and drain gates accumulated from valid samples."""

    peak_running: int = 0
    peak_running_gpu_units: int = 0
    peak_running_by_cloud: dict[str,
                                int] = dataclasses.field(default_factory=dict)
    peak_running_gpu_units_by_cloud: dict[str, int] = dataclasses.field(
        default_factory=dict)
    baseline_qualified_iteration_id: int | None = None
    baseline_qualified_observed_at: float | None = None
    scale_started_monotonic: float | None = None
    scale_started_at: float | None = None
    scale_reached_monotonic: float | None = None
    scale_qualified_observed_at: float | None = None
    scale_qualified_iteration_id: int | None = None
    scale_slo_met: bool | None = None
    zero_since_monotonic: float | None = None
    zero_samples: int = 0

    def start_scale(self) -> None:
        if (self.scale_started_monotonic is not None or
                self.scale_started_at is not None):
            raise QualificationError('Scale timer was already started.')
        self.scale_started_monotonic = time.monotonic()
        self.scale_started_at = time.time()

    def observe(self,
                observation: Observation,
                profile: Profile,
                expectation: ProviderExpectation | None = None,
                *,
                qualify_scale: bool = True) -> None:
        if expectation is None:
            expectation = provider_expectation(profile, None)
        self.peak_running = max(self.peak_running,
                                observation.provider.running_count)
        self.peak_running_gpu_units = max(
            self.peak_running_gpu_units, observation.provider.running_gpu_units)
        for cloud in observation.provider.clouds:
            self.peak_running_by_cloud[cloud.cloud] = max(
                self.peak_running_by_cloud.get(cloud.cloud, 0),
                cloud.running_count)
            self.peak_running_gpu_units_by_cloud[cloud.cloud] = max(
                self.peak_running_gpu_units_by_cloud.get(cloud.cloud, 0),
                cloud.running_gpu_units)
        if (qualify_scale and self.scale_reached_monotonic is None and
                observation.provider.running_count
                >= expectation.minimum_physical_running):
            if self.scale_started_monotonic is None:
                raise QualificationError(
                    'Provider reached the physical RUNNING target '
                    'before the scale timer.')
            elapsed = (observation.observed_monotonic -
                       self.scale_started_monotonic)
            if elapsed > profile.scale_timeout_seconds:
                raise QualificationError(
                    f'Scale-out to {expectation.minimum_physical_running} '
                    f'physical RUNNING L4 Spot VMs took {elapsed:.1f}s; '
                    'qualification timeout is '
                    f'{profile.scale_timeout_seconds:.1f}s.')
            self.scale_reached_monotonic = observation.observed_monotonic
            self.scale_slo_met = elapsed <= profile.scale_slo_seconds
            if (not math.isfinite(observation.observed_at) or
                    observation.observed_at < 0):
                raise QualificationError(
                    'Qualified provider sample has no finite wall timestamp.')
            self.scale_qualified_observed_at = observation.observed_at

    def observe_zero(self, observation: Observation) -> None:
        if observation.is_exact_zero():
            self.zero_samples += 1
            if self.zero_since_monotonic is None:
                self.zero_since_monotonic = observation.observed_monotonic
        else:
            self.zero_samples = 0
            self.zero_since_monotonic = None

    def drain_complete(self, observation: Observation,
                       profile: Profile) -> bool:
        return (self.zero_samples >= 3 and
                self.zero_since_monotonic is not None and
                observation.observed_monotonic - self.zero_since_monotonic
                >= profile.zero_hold_seconds)


def observation_summary(observation: Observation) -> dict[str, Any]:
    return {
        'observation_started_at': observation.observed_started_at,
        'observation_finished_at': observation.observed_at,
        'observation_duration_seconds':
            (observation.observed_monotonic -
             observation.observed_started_monotonic),
        'observed_at': observation.observed_at,
        'controller_pid': observation.database.controller.pid,
        'controller_ip': observation.database.controller.ip,
        'controller_owner_epoch': observation.database.controller.owner_epoch,
        'controller_incarnation': observation.database.controller.incarnation,
        'paid_debit_units': observation.database.paid_debit_units,
        'claimed_units': observation.database.claimed_units,
        'paid_claim_priority_units': [
            dataclasses.asdict(item)
            for item in observation.database.claim_priority_units
        ],
        'waiters': observation.database.waiter_count,
        # Phase-A launch intents are expected while a wave is being bound.
        # Keep them visible in the receipt without mistaking them for an
        # observer blackout; exact-zero still requires that they disappear.
        'provider_free_unbound_replicas': len(
            observation.database.provider_free_unbound_replica_ids),
        'postgres_demand_units': observation.database.demand_units,
        'provider_instances': observation.provider.instance_count,
        'provider_running': observation.provider.running_count,
        'provider_gpu_units': observation.provider.gpu_units,
        'provider_running_gpu_units': observation.provider.running_gpu_units,
        'provider_by_cloud': {
            cloud.cloud: {
                'instances': cloud.instance_count,
                'running': cloud.running_count,
                'gpu_units': cloud.gpu_units,
                'running_gpu_units': cloud.running_gpu_units,
                'disks': cloud.disk_count,
                'inflight_operations': cloud.inflight_operation_count,
                'shapes': [dataclasses.asdict(shape) for shape in cloud.shapes],
            } for cloud in observation.provider.clouds
        },
        'provider_disks': observation.provider.disk_count,
        'provider_inflight_operations':
            (observation.provider.inflight_operation_count),
        'lb_demand_units': observation.load_balancer.demand_units,
        'lb_ready_replicas': observation.load_balancer.ready_replicas,
        'lb_unique_job_arrivals_60s':
            observation.load_balancer.unique_job_arrivals_60s,
        'lb_unique_job_arrivals_300s':
            observation.load_balancer.unique_job_arrivals_300s,
        'lb_headerless_arrivals_60s':
            observation.load_balancer.headerless_arrivals_60s,
        'lb_headerless_arrivals_300s':
            observation.load_balancer.headerless_arrivals_300s,
        'lb_offered_arrival_tracking_saturated':
            observation.load_balancer.offered_arrival_tracking_saturated,
    }


class Receipt:
    """Credential-free evidence emitted even when qualification fails."""

    def __init__(self,
                 *,
                 path: pathlib.Path,
                 service_name: str,
                 profile: Profile,
                 expectation: ProviderExpectation | None = None,
                 scope: ProviderScope | None = None,
                 authorized_economic_receipt_sha256: str | None = None) -> None:
        if expectation is None:
            expectation = provider_expectation(profile, None)
        self._path = path
        self._payload: dict[str, Any] = {
            'schema_version': _QUALIFICATION_RECEIPT_SCHEMA_VERSION,
            'service_name': service_name,
            'profile': profile.name,
            'expectation_kind': expectation.kind.value,
            'expected_providers': list(expectation.providers),
            'request_priority': _REQUEST_PRIORITY,
            'max_units': profile.max_units,
            'minimum_running': expectation.minimum_physical_running,
            'exact_request_count': expectation.exact_request_count,
            'scale_slo_seconds': profile.scale_slo_seconds,
            'scale_timeout_seconds': profile.scale_timeout_seconds,
            'started_at': time.time(),
            'samples': [],
            'request_telemetry_samples': [],
        }
        if scope is not None:
            self._payload.update({
                'service_hash': scope.service_hash,
                'lifecycle_epoch': scope.lifecycle_epoch,
                'service_version': scope.service_version,
                'controller_config_digest': scope.controller_config_digest,
                'controller_config_snapshot_id':
                    scope.controller_config_snapshot_id,
                'service_yaml_sha256': scope.service_yaml_sha256,
                'qualification_profile': scope.qualification_profile,
                'qualification_source_sha256':
                    scope.qualification_source_sha256,
                'qualification_projection_sha256':
                    scope.qualification_projection_sha256,
                'placement_catalog_sha256': scope.placement_catalog_sha256,
            })
        if authorized_economic_receipt_sha256 is not None:
            self._payload['authorized_economic_receipt_sha256'] = (
                authorized_economic_receipt_sha256)

    def sample(self,
               phase: str,
               observation: Observation,
               *,
               scale_iteration_id: int | None = None,
               baseline_iteration_id: int | None = None,
               baseline_pair_observed_at: float | None = None) -> None:
        sample = {
            'phase': phase,
            'exact_zero': observation.is_exact_zero(),
            **observation_summary(observation),
        }
        if scale_iteration_id is not None:
            sample['scale_iteration_id'] = scale_iteration_id
        if baseline_iteration_id is not None:
            sample['baseline_iteration_id'] = baseline_iteration_id
            sample['baseline_pair_observed_at'] = baseline_pair_observed_at
        self._payload['samples'].append(sample)

    def miss(self,
             phase: str,
             error: Exception,
             *,
             scale_iteration_id: int | None = None) -> None:
        sample = {
            'phase': phase,
            'observed_at': time.time(),
            'observation_error_type': type(error).__name__,
        }
        if scale_iteration_id is not None:
            sample['scale_iteration_id'] = scale_iteration_id
        self._payload['samples'].append(sample)

    def bind_scale_campaign_counters(
            self, *, scale_iteration_id: int,
            counters: ExactRequestCampaignCounters) -> None:
        """Bind one atomic driver frontier to its just-recorded scale sample."""
        if not self._payload['samples']:
            raise QualificationError(
                'Scale sample is unavailable for campaign attribution.')
        sample = self._payload['samples'][-1]
        if (sample.get('phase') != 'scale' or
                sample.get('scale_iteration_id') != scale_iteration_id or
                'campaign_offered' in sample or 'campaign_succeeded' in sample):
            raise QualificationError(
                'Scale sample has conflicting campaign attribution.')
        sample['campaign_offered'] = counters.offered
        sample['campaign_succeeded'] = counters.succeeded

    def request_telemetry(
            self,
            phase: str,
            telemetry: RequestTelemetry,
            *,
            scale_iteration_id: int | None = None,
            baseline_iteration_id: int | None = None,
            baseline_pair_observed_at: float | None = None) -> None:
        sample = {
            'phase': phase,
            **dataclasses.asdict(telemetry),
            'ledger_active': telemetry.ledger_active,
            'ledger_succeeded': telemetry.ledger_succeeded,
            'ledger_total': telemetry.ledger_total,
        }
        if scale_iteration_id is not None:
            sample['scale_iteration_id'] = scale_iteration_id
        if baseline_iteration_id is not None:
            sample['baseline_iteration_id'] = baseline_iteration_id
            sample['baseline_pair_observed_at'] = baseline_pair_observed_at
        self._payload['request_telemetry_samples'].append(sample)

    def finish(self,
               *,
               progress: Progress,
               exact_request_successes: int,
               aws_volume_ids: collections.abc.Mapping[str, list[str]] |
               None = None,
               ledger_baseline: RequestTelemetry | None = None,
               ledger_final: RequestTelemetry | None = None,
               error: BaseException | None = None) -> None:
        self._payload.update({
            'finished_at': time.time(),
            'outcome': 'passed' if error is None else 'failed',
            'peak_running': progress.peak_running,
            'peak_running_gpu_units': progress.peak_running_gpu_units,
            'peak_running_by_cloud': progress.peak_running_by_cloud,
            'peak_running_gpu_units_by_cloud':
                progress.peak_running_gpu_units_by_cloud,
            'baseline_qualified_iteration_id':
                progress.baseline_qualified_iteration_id,
            'baseline_qualified_observed_at':
                progress.baseline_qualified_observed_at,
            # Preserve the wall start and bind the exact provider observation
            # that qualified.  The aggregate gate derives elapsed time from
            # these two receipt facts; a free-standing elapsed scalar is not
            # evidence.
            'scale_started_at': progress.scale_started_at,
            'scale_qualified_observed_at': progress.scale_qualified_observed_at,
            'scale_qualified_iteration_id':
                progress.scale_qualified_iteration_id,
            'scale_slo_met': progress.scale_slo_met,
            'exact_request_successes': exact_request_successes,
            'aws_retained_volume_ids': dict(aws_volume_ids or {}),
        })
        if ledger_baseline is not None:
            self._payload['ledger_baseline_total'] = (
                ledger_baseline.ledger_total)
            self._payload['ledger_baseline_succeeded'] = (
                ledger_baseline.ledger_succeeded)
        if ledger_final is not None:
            self._payload['ledger_final_total'] = ledger_final.ledger_total
            self._payload['ledger_final_succeeded'] = (
                ledger_final.ledger_succeeded)
        if ledger_baseline is not None and ledger_final is not None:
            self._payload['ledger_request_delta'] = (
                ledger_final.ledger_total - ledger_baseline.ledger_total)
            self._payload['ledger_succeeded_delta'] = (
                ledger_final.ledger_succeeded -
                ledger_baseline.ledger_succeeded)
        if error is not None:
            # Never serialize exception text: transport/database errors may
            # contain an endpoint or credential-bearing connection string.
            self._payload['error_type'] = type(error).__name__
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._payload, indent=2, sort_keys=True) + '\n',
            encoding='utf-8')


def read_aws_volume_ids_receipt(
    path: pathlib.Path,
    service_name: str,
) -> dict[str, list[str]]:
    """Read exact EBS identities retained by the qualification run."""
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise QualificationError('Qualification receipt is unavailable.') \
            from error
    raw = (payload.get('aws_retained_volume_ids')
           if isinstance(payload, dict) else None)
    if (not isinstance(payload, dict) or payload.get('schema_version')
            != _QUALIFICATION_RECEIPT_SCHEMA_VERSION or
            payload.get('service_name') != service_name or
            not isinstance(raw, dict)):
        raise QualificationError('Qualification receipt is malformed.')
    result: dict[str, list[str]] = {}
    for region, volume_ids in raw.items():
        if (not isinstance(region, str) or not region or
                not isinstance(volume_ids, list) or
                volume_ids != sorted(set(volume_ids)) or
                any(not isinstance(volume_id, str) or not volume_id
                    for volume_id in volume_ids)):
            raise QualificationError('Qualification receipt is malformed.')
        result[region] = volume_ids
    return result


def read_optional_aws_volume_ids_receipt(
    path: pathlib.Path,
    service_name: str,
) -> dict[str, list[str]]:
    """Best-effort teardown aid; frozen service-tag scans remain authoritative."""
    try:
        return read_aws_volume_ids_receipt(path, service_name)
    except QualificationError:
        # The process can be killed before its non-authoritative receipt is
        # written.  New EBS resources carry both service identity tags, so a
        # missing or partial receipt must not prevent scoped cleanup polling.
        return {}


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExactAsyncReceipt:
    """Receipt fields required to complete one synthetic exact request."""

    attempt_id: str
    attempt_no: int
    state: str
    revision: int


class _ExactAdmissionRecoveryAction(enum.Enum):
    RETURN = enum.auto()
    RETRY_SUBMISSION = enum.auto()
    POLL_RECEIPT = enum.auto()


def _single_response_header(response: aiohttp.ClientResponse, name: str) -> str:
    get_all = getattr(response.headers, 'getall', None)
    if callable(get_all):
        values = list(get_all(name, []))
    else:
        value = response.headers.get(name)
        values = [] if value is None else [value]
    if len(values) != 1 or not isinstance(values[0], str) or not values[0]:
        raise QualificationError(
            f'Exact async response lacks one {name} header.')
    return values[0]


def _receipt_from_headers(
    response: aiohttp.ClientResponse,
    *,
    service_hash: str,
) -> ExactAsyncReceipt:
    _validate_exact_response_fence(response, service_hash=service_hash)
    attempt_id = _single_response_header(
        response, serve_constants.LB_ASYNC_ATTEMPT_ID_HEADER)
    try:
        if str(uuid.UUID(attempt_id)) != attempt_id:
            raise ValueError
        attempt_no = int(
            _single_response_header(response,
                                    serve_constants.LB_ASYNC_ATTEMPT_NO_HEADER))
        revision = int(
            _single_response_header(
                response, serve_constants.LB_ASYNC_LEDGER_REVISION_HEADER))
    except (TypeError, ValueError) as error:
        raise QualificationError(
            'Exact async response has malformed receipt identity.') from error
    state = _single_response_header(
        response, serve_constants.LB_ASYNC_LEDGER_STATE_HEADER)
    if attempt_no < 1 or revision < 1:
        raise QualificationError(
            'Exact async response has a conflicting receipt transition.')
    return ExactAsyncReceipt(attempt_id=attempt_id,
                             attempt_no=attempt_no,
                             state=state,
                             revision=revision)


def _validate_exact_response_fence(response: aiohttp.ClientResponse, *,
                                   service_hash: str) -> None:
    """Require proof that a receipt response came from this exact service."""
    if (_single_response_header(
            response, serve_constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER) != str(
                serve_constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION) or
            _single_response_header(
                response, serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER)
            != service_hash):
        raise QualificationError(
            'Exact async response changed protocol or service incarnation.')


def _validate_submission_receipt(
    current: ExactAsyncReceipt,
    *,
    previous_rejection: ExactAsyncReceipt | None,
    accepted_response: bool,
) -> None:
    """Validate one public response against reachable ledger transitions."""
    if accepted_response:
        valid_state_revision = (
            (current.state == 'ACCEPTED' and current.revision == 2) or
            (current.state == 'SUCCEEDED' and current.revision in (2, 3)))
    else:
        valid_state_revision = (
            current.state == 'REJECTED_PRE_DISPATCH' and
            (current.revision == 2 or
             (current.attempt_no == 1 and current.revision == 1)))
    valid_attempt = True
    if previous_rejection is not None:
        same_rejection = (
            not accepted_response and
            current.attempt_id == previous_rejection.attempt_id and
            current.attempt_no == previous_rejection.attempt_no and
            current.revision == previous_rejection.revision)
        later_attempt = (current.attempt_id != previous_rejection.attempt_id and
                         current.attempt_no > previous_rejection.attempt_no)
        valid_attempt = later_attempt if accepted_response else (
            same_rejection or later_attempt)
    if not valid_state_revision or not valid_attempt:
        raise QualificationError(
            'Exact async response has a conflicting receipt transition.')


def _validate_recovered_submission_receipt(
    current: ExactAsyncReceipt,
    *,
    request_id: str,
    previous_rejection: ExactAsyncReceipt | None,
    pending_dispatch: ExactAsyncReceipt | None,
) -> _ExactAdmissionRecoveryAction:
    """Validate one read-only or duplicate-POST admission observation."""
    if current.state in {'AMBIGUOUS', 'FAILED', 'CANCELLED', 'EXPIRED'}:
        raise QualificationError(
            f'{request_id} has durable exact admission state {current.state}.')
    if current.state == 'DISPATCH_MAY_HAVE_OCCURRED':
        if current.revision != 1:
            raise QualificationError(
                'Exact async response has a conflicting receipt transition.')
        if pending_dispatch is not None:
            valid_attempt = (
                current == pending_dispatch or
                (current.attempt_id != pending_dispatch.attempt_id and
                 current.attempt_no > pending_dispatch.attempt_no))
        elif previous_rejection is not None:
            valid_attempt = (current.attempt_id != previous_rejection.attempt_id
                             and
                             current.attempt_no > previous_rejection.attempt_no)
        else:
            valid_attempt = True
        if not valid_attempt:
            raise QualificationError(
                'Exact async response has a conflicting receipt transition.')
        return _ExactAdmissionRecoveryAction.POLL_RECEIPT
    if current.state not in {'REJECTED_PRE_DISPATCH', 'ACCEPTED', 'SUCCEEDED'}:
        raise QualificationError(
            f'{request_id} has unsupported durable exact admission state '
            f'{current.state}.')
    accepted = current.state in {'ACCEPTED', 'SUCCEEDED'}
    if pending_dispatch is None:
        _validate_submission_receipt(current,
                                     previous_rejection=previous_rejection,
                                     accepted_response=accepted)
        return (_ExactAdmissionRecoveryAction.RETURN
                if accepted else _ExactAdmissionRecoveryAction.RETRY_SUBMISSION)
    same_attempt = (current.attempt_id == pending_dispatch.attempt_id and
                    current.attempt_no == pending_dispatch.attempt_no)
    later_attempt = (current.attempt_id != pending_dispatch.attempt_id and
                     current.attempt_no > pending_dispatch.attempt_no)
    if not (same_attempt or later_attempt):
        raise QualificationError(
            'Exact async response has a conflicting receipt transition.')
    if (same_attempt and current.state == 'REJECTED_PRE_DISPATCH' and
            current.revision != 2):
        # Revision 1 is reserved for a no-route initial rejection. A bound
        # DISPATCH/r1 attempt can reject only through its r2 transition.
        raise QualificationError(
            'Exact async response has a conflicting receipt transition.')
    _validate_submission_receipt(current,
                                 previous_rejection=None,
                                 accepted_response=accepted)
    return (_ExactAdmissionRecoveryAction.RETURN
            if accepted else _ExactAdmissionRecoveryAction.RETRY_SUBMISSION)


def _validate_completion_receipt(accepted: ExactAsyncReceipt,
                                 current: ExactAsyncReceipt) -> None:
    """Require the exact accepted attempt to advance to terminal success."""
    if (accepted.state != 'ACCEPTED' or accepted.revision != 2 or
            current.state != 'SUCCEEDED' or
            current.attempt_id != accepted.attempt_id or
            current.attempt_no != accepted.attempt_no or
            current.revision != accepted.revision + 1):
        raise QualificationError(
            'Exact async response has a conflicting receipt transition.')


def _canonical_exact_request(request_id: str,
                             duration_seconds: float) -> tuple[bytes, str]:
    body = rfc8785.dumps({
        'action': 'async_predict',
        'payload': {
            'duration_seconds': duration_seconds,
        },
        'request_id': request_id,
    })
    return body, hashlib.sha256(body).hexdigest()


def _exact_request_headers(*, token: str, service_hash: str, request_id: str,
                           stable_job_id: str,
                           intent_sha256: str) -> dict[str, str]:
    return {
        _AUTH_HEADER: f'Bearer {token}',
        _JOB_ID_HEADER: stable_job_id,
        _PRIORITY_HEADER: str(_REQUEST_PRIORITY),
        _ACCELERATORS_HEADER: 'L4',
        'Content-Type': 'application/json',
        serve_constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER: str(
            serve_constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION),
        serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: service_hash,
        serve_constants.LB_ASYNC_INTENT_SHA256_HEADER: intent_sha256,
        serve_constants.LB_ASYNC_EXECUTION_REQUEST_ID_HEADER: request_id,
    }


def _bounded_retry_delay(retry_after: object, *, attempt: int,
                         request_id: str) -> float:
    """Return bounded exponential delay with stable per-request jitter."""
    try:
        base = max(0.1, float(retry_after))
    except (TypeError, ValueError):
        base = 1.0
    exponential = base * 2**min(max(attempt, 0), 4)
    digest = hashlib.sha256(f'{request_id}:{attempt}'.encode()).digest()
    fraction = int.from_bytes(digest[:8], 'big') / float(2**64 - 1)
    jittered = exponential * (0.75 + 0.5 * fraction)
    return min(10.0, max(0.1, jittered))


async def _submit_exact_async_request(
    session: aiohttp.ClientSession,
    *,
    endpoint: str,
    token: str,
    service_hash: str,
    request_id: str,
    stable_job_id: str,
    duration_seconds: float,
    deadline: float,
) -> tuple[ExactAsyncReceipt, str]:
    """Submit or read-only recover one immutable exact async request."""
    url = endpoint.rstrip('/') + '/v1/models/model:predict'
    receipt_url = (endpoint.rstrip('/') +
                   serve_constants.LB_ASYNC_REQUEST_RECEIPT_ENDPOINT_PATH)
    body, intent_sha256 = _canonical_exact_request(request_id, duration_seconds)
    headers = _exact_request_headers(token=token,
                                     service_hash=service_hash,
                                     request_id=request_id,
                                     stable_job_id=stable_job_id,
                                     intent_sha256=intent_sha256)
    receipt_headers = {
        _AUTH_HEADER: f'Bearer {token}',
        'Content-Type': 'application/json',
        serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: service_hash,
    }
    receipt_payload = {
        'ledger_protocol_version':
            serve_constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION,
        'request_id': request_id,
        'intent_sha256': intent_sha256,
    }
    retry_attempt = 0
    previous_rejection: ExactAsyncReceipt | None = None
    pending_dispatch: ExactAsyncReceipt | None = None
    durable_receipt_observed = False
    recovering = False
    while time.monotonic() < deadline:
        retry_after = '0.5'
        observed_receipt: ExactAsyncReceipt | None = None
        read_only = recovering
        try:
            if read_only:
                request = session.post(receipt_url,
                                       headers=receipt_headers,
                                       json=receipt_payload)
            else:
                request = session.post(url, headers=headers, data=body)
            async with request as response:
                if read_only:
                    if response.status == 404:
                        _validate_exact_response_fence(
                            response, service_hash=service_hash)
                        if durable_receipt_observed:
                            raise QualificationError(
                                f'{request_id} lost a previously durable exact '
                                'admission receipt.')
                        recovering = False
                    elif response.status == 200:
                        observed_receipt = _receipt_from_headers(
                            response, service_hash=service_hash)
                        durable_receipt_observed = True
                        retry_after = response.headers.get('Retry-After', '0.5')
                    elif response.status not in _RETRYABLE_STATUSES:
                        raise QualificationError(
                            f'{request_id} receipt lookup returned '
                            f'HTTP {response.status}.')
                    else:
                        retry_after = response.headers.get('Retry-After', '0.5')
                elif response.status == 202:
                    receipt = _receipt_from_headers(response,
                                                    service_hash=service_hash)
                    durable_receipt_observed = True
                    _validate_submission_receipt(
                        receipt,
                        previous_rejection=previous_rejection,
                        accepted_response=True)
                    try:
                        response_body = await response.read()
                        result = json.loads(response_body)
                    except (UnicodeDecodeError, ValueError) as error:
                        raise QualificationError(
                            f'{request_id} returned invalid JSON.') from error
                    if result != {
                            'request_id': request_id,
                            'status': 'accepted',
                    }:
                        raise QualificationError(
                            f'{request_id} returned an invalid acceptance.')
                    return receipt, intent_sha256
                elif (response.status == 409 or
                      response.status in _RETRYABLE_STATUSES):
                    try:
                        observed_receipt = _receipt_from_headers(
                            response, service_hash=service_hash)
                        durable_receipt_observed = True
                    except QualificationError:
                        if response.status == 409:
                            raise
                        recovering = True
                    retry_after = response.headers.get('Retry-After', '1')
                else:
                    raise QualificationError(
                        f'{request_id} returned HTTP {response.status}.')
        except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError,
                asyncio.TimeoutError):
            # Once a submission response is lost, only the read-only durable
            # receipt endpoint can authorize an identical replay. A lookup
            # transport error remains read-only and is retried to the same
            # shared deadline.
            recovering = True
        if observed_receipt is not None:
            action = _validate_recovered_submission_receipt(
                observed_receipt,
                request_id=request_id,
                previous_rejection=previous_rejection,
                pending_dispatch=pending_dispatch if read_only else None)
            if action is _ExactAdmissionRecoveryAction.RETURN:
                return observed_receipt, intent_sha256
            if action is _ExactAdmissionRecoveryAction.RETRY_SUBMISSION:
                previous_rejection = observed_receipt
                pending_dispatch = None
                recovering = False
            else:
                pending_dispatch = observed_receipt
                recovering = True
        delay = _bounded_retry_delay(retry_after,
                                     attempt=retry_attempt,
                                     request_id=request_id)
        retry_attempt += 1
        await asyncio.sleep(delay)
    raise QualificationError(
        f'{request_id} exhausted its exact admission deadline.')


async def _complete_exact_async_request(
    session: aiohttp.ClientSession,
    *,
    endpoint: str,
    token: str,
    service_hash: str,
    request_id: str,
    intent_sha256: str,
    accepted: ExactAsyncReceipt,
    processing_time_us: int,
    deadline: float,
) -> None:
    """Retry only the idempotent terminal callback for an accepted attempt."""
    url = (endpoint.rstrip('/') +
           serve_constants.LB_PREDICTION_COMPLETION_ENDPOINT_PATH)
    headers = {
        _AUTH_HEADER: f'Bearer {token}',
        serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER: service_hash,
    }
    payload = {
        'ledger_protocol_version':
            serve_constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION,
        'request_id': request_id,
        'intent_sha256': intent_sha256,
        'attempt_id': accepted.attempt_id,
        'attempt_no': accepted.attempt_no,
        'expected_revision': accepted.revision,
        'status': 'SUCCEEDED',
        'processing_time_us': processing_time_us,
    }
    retry_attempt = 0
    while time.monotonic() < deadline:
        retry_after = '0.5'
        try:
            async with session.post(url, headers=headers,
                                    json=payload) as response:
                await response.read()
                if response.status == 204:
                    completion = _receipt_from_headers(
                        response, service_hash=service_hash)
                    _validate_completion_receipt(accepted, completion)
                    return
                if (response.status not in _RETRYABLE_STATUSES and
                        response.status != 409):
                    raise QualificationError(
                        f'{request_id} completion returned '
                        f'HTTP {response.status}.')
                retry_after = response.headers.get('Retry-After', '0.5')
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
            pass
        delay = _bounded_retry_delay(retry_after,
                                     attempt=retry_attempt,
                                     request_id=request_id)
        retry_attempt += 1
        await asyncio.sleep(delay)
    raise QualificationError(f'{request_id} exhausted its completion deadline.')


async def _one_exact_async_request(
    session: aiohttp.ClientSession,
    *,
    endpoint: str,
    token: str,
    service_hash: str,
    request_id: str,
    stable_job_id: str,
    duration_seconds: float,
    deadline: float,
) -> None:
    receipt, intent_sha256 = await _submit_exact_async_request(
        session,
        endpoint=endpoint,
        token=token,
        service_hash=service_hash,
        request_id=request_id,
        stable_job_id=stable_job_id,
        duration_seconds=duration_seconds,
        deadline=deadline)
    if receipt.state == 'SUCCEEDED':
        return
    await asyncio.sleep(duration_seconds)
    await _complete_exact_async_request(session,
                                        endpoint=endpoint,
                                        token=token,
                                        service_hash=service_hash,
                                        request_id=request_id,
                                        intent_sha256=intent_sha256,
                                        accepted=receipt,
                                        processing_time_us=int(
                                            duration_seconds * 1_000_000),
                                        deadline=deadline)


async def send_exact_async_requests(
    *,
    endpoint: str,
    token: str,
    service_hash: str,
    prefix: str,
    count: int,
    concurrency: int,
    hold_requests: int,
    hold_seconds: float,
    timeout_seconds: float,
    campaign_progress: ExactRequestCampaignProgress | None = None,
) -> int:
    """Submit and durably complete exact synthetic async requests."""
    worker_count = min(count, concurrency)
    if campaign_progress is None:
        campaign_progress = ExactRequestCampaignProgress(
            total_count=count, window_size=worker_count)
    elif (campaign_progress.total_count != count or
          campaign_progress.window_size != worker_count):
        raise ValueError('Exact request campaign progress does not match.')
    if await campaign_progress.snapshot() != ExactRequestCampaignCounters(
            offered=0, succeeded=0):
        raise ValueError('Exact request campaign progress is not fresh.')
    queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()
    for index in range(count):
        queue.put_nowait((index, f'{prefix}-execution-{index:05d}'))
    deadline = time.monotonic() + timeout_seconds

    async def worker() -> None:
        while True:
            try:
                index, request_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            await campaign_progress.mark_offered()
            await _one_exact_async_request(
                session,
                endpoint=endpoint,
                token=token,
                service_hash=service_hash,
                request_id=request_id,
                stable_job_id=f'{prefix}-job-{index:05d}',
                duration_seconds=(hold_seconds if index < hold_requests else 0),
                deadline=deadline)
            await campaign_progress.mark_succeeded()
            queue.task_done()

    timeout = aiohttp.ClientTimeout(total=11 * 60)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout,
                                     connector=connector) as session:
        await asyncio.gather(*(worker() for _ in range(worker_count)))
    final = await campaign_progress.snapshot()
    if final != ExactRequestCampaignCounters(offered=count, succeeded=count):
        raise QualificationError('Exact request campaign is incomplete.')
    return final.succeeded


async def _wait_for_joined_baseline(
        *,
        observer: Observer,
        profile: Profile,
        progress: Progress,
        receipt: Receipt,
        expectation: ProviderExpectation | None = None) -> RequestTelemetry:
    """Prove paired request/provider zero immediately before traffic."""
    deadline = time.monotonic() + 5 * 60
    baseline_iteration_id = 0
    while time.monotonic() < deadline:
        try:
            telemetry = await observer.request_telemetry()
        except Exception:  # pylint: disable=broad-except
            await asyncio.sleep(profile.poll_seconds)
            continue
        if telemetry.is_fresh_complete() and not telemetry.is_exact_zero():
            raise QualificationError(
                'First valid pre-demand request telemetry is nonzero.')
        if not telemetry.is_exact_zero():
            await asyncio.sleep(profile.poll_seconds)
            continue
        try:
            observation = await observer.snapshot(
                require_complete_demand_report=True)
            validate_observation(observation, profile, expectation)
        except GuardViolation:
            raise
        except Exception as error:  # pylint: disable=broad-except
            receipt.miss('baseline', error)
            await asyncio.sleep(profile.poll_seconds)
            continue
        if not observation.is_exact_zero():
            raise QualificationError(
                'First valid pre-demand provider observation is nonzero.')
        baseline_iteration_id += 1
        paired_at = observation.observed_at
        if not math.isfinite(paired_at) or paired_at <= 0:
            raise QualificationError(
                'Joined pre-demand baseline has no finite timestamp.')
        receipt.request_telemetry('baseline',
                                  telemetry,
                                  baseline_iteration_id=baseline_iteration_id,
                                  baseline_pair_observed_at=paired_at)
        progress.observe(observation, profile, expectation)
        receipt.sample('baseline',
                       observation,
                       baseline_iteration_id=baseline_iteration_id,
                       baseline_pair_observed_at=paired_at)
        if baseline_iteration_id >= 3:
            progress.baseline_qualified_iteration_id = baseline_iteration_id
            progress.baseline_qualified_observed_at = paired_at
            return telemetry
        await _sleep_after_observation(observation, profile.poll_seconds)
    raise QualificationError(
        'Service did not establish a joined exact-zero pre-demand baseline.')


def _has_exact_campaign_demand(telemetry: RequestTelemetry,
                               baseline: RequestTelemetry) -> bool:
    """Return whether dispatched work is exactly ledger-attributed."""
    if (not baseline.is_exact_zero() or not telemetry.is_fresh_complete() or
            telemetry.queue_depth is None or
            telemetry.in_flight_requests is None or
            telemetry.processing_requests is None or
            telemetry.confirmed_in_flight_requests is None or
            telemetry.confirmed_processing_requests is None):
        return False
    if (telemetry.confirmed_in_flight_requests != telemetry.in_flight_requests
            or telemetry.confirmed_processing_requests
            != telemetry.processing_requests or
            telemetry.processing_requests > telemetry.in_flight_requests):
        return False
    active_delta = telemetry.ledger_active - baseline.ledger_active
    # A queued request has not selected a READY replica and therefore has no
    # ledger row yet.  Exact async rows bind at dispatch, so ledger-active must
    # equal in-flight—not queued + in-flight—while the immutable terminal
    # ledger delta later proves that every queued identity was processed.
    return (active_delta >= 0 and
            active_delta == telemetry.in_flight_requests and
            telemetry.queue_depth + telemetry.in_flight_requests > 0)


def _resident_campaign_size(telemetry: RequestTelemetry) -> int:
    if (telemetry.queue_depth is None or telemetry.in_flight_requests is None):
        return -1
    return telemetry.queue_depth + telemetry.in_flight_requests


async def _wait_for_scale_stimulus(
    *,
    observer: Observer,
    profile: Profile,
    receipt: Receipt,
    traffic: asyncio.Task[int],
    baseline: RequestTelemetry,
    expected_resident: int,
    deadline_monotonic: float,
) -> RequestTelemetry:
    """Prove the bounded cohort is resident before observing scale-out."""
    while time.monotonic() < deadline_monotonic:
        if traffic.done():
            try:
                successes = traffic.result()
            except BaseException as error:
                raise QualificationError(
                    'Held campaign prefix failed before queue admission.') \
                    from error
            raise QualificationError(
                'Held campaign prefix ended before queue admission after '
                f'{successes} successes.')
        try:
            telemetry = await observer.request_telemetry()
        except Exception:  # pylint: disable=broad-except
            await asyncio.sleep(profile.poll_seconds)
            continue
        if (telemetry.is_fresh_complete() and
                _resident_campaign_size(telemetry) > expected_resident):
            raise QualificationError(
                'Held campaign prefix contains unattributed demand.')
        if (_has_exact_campaign_demand(telemetry, baseline) and
                _resident_campaign_size(telemetry) == expected_resident):
            receipt.request_telemetry('scale-stimulus', telemetry)
            return telemetry
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        'Scale stimulus did not enter the request queue in time.')


async def _wait_for_positive_request_telemetry(
    *,
    observer: Observer,
    profile: Profile,
    receipt: Receipt,
    traffic: asyncio.Task[int],
    baseline: RequestTelemetry,
    deadline_monotonic: float,
) -> RequestTelemetry:
    while time.monotonic() < deadline_monotonic:
        if traffic.done():
            try:
                successes = traffic.result()
            except BaseException as error:
                raise QualificationError(
                    'Exact traffic failed before positive telemetry.') \
                    from error
            raise QualificationError(
                'Exact traffic finished before fresh positive processing, '
                f'queue, and in-flight telemetry after {successes} successes.')
        try:
            telemetry = await observer.request_telemetry()
        except Exception:  # pylint: disable=broad-except
            await asyncio.sleep(profile.poll_seconds)
            continue
        if (_has_exact_campaign_demand(telemetry, baseline) and
                telemetry.queue_depth is not None and
            (profile.name == 'scale' or telemetry.queue_depth > 0) and
                telemetry.in_flight_requests is not None and
                telemetry.in_flight_requests > 0 and
                telemetry.processing_requests is not None and
                telemetry.processing_requests > 0 and
                telemetry.confirmed_in_flight_requests is not None and
                telemetry.confirmed_in_flight_requests > 0 and
                telemetry.confirmed_processing_requests is not None and
                telemetry.confirmed_processing_requests > 0 and
                telemetry.ledger_total > baseline.ledger_total and
                telemetry.ledger_count('ACCEPTED') > 0 and
            (profile.name != 'scale' or _resident_campaign_size(telemetry)
             == scale_stimulus_count(profile))):
            receipt.request_telemetry('positive', telemetry)
            return telemetry
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        'No fresh positive processing and in-flight sample was observed for '
        'exact traffic.')


async def _wait_for_final_request_telemetry(
    *,
    observer: Observer,
    profile: Profile,
    receipt: Receipt,
    baseline: RequestTelemetry,
    expected_succeeded_delta: int,
) -> RequestTelemetry:
    deadline = time.monotonic() + 5 * 60
    while time.monotonic() < deadline:
        try:
            telemetry = await observer.request_telemetry()
        except Exception:  # pylint: disable=broad-except
            await asyncio.sleep(profile.poll_seconds)
            continue
        if (telemetry.is_exact_zero() and
                telemetry.ledger_total - baseline.ledger_total
                == expected_succeeded_delta and
                telemetry.ledger_succeeded - baseline.ledger_succeeded
                == expected_succeeded_delta):
            receipt.request_telemetry('final', telemetry)
            return telemetry
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        'Exact request telemetry did not reach the required SUCCEEDED delta '
        'and current-work zero.')


async def _validated_sample(
        *,
        observer: Observer,
        profile: Profile,
        progress: Progress,
        receipt: Receipt,
        phase: str,
        expectation: ProviderExpectation | None = None,
        scale_iteration_id: int | None = None) -> Observation | None:
    """Collect one sample without turning observer loss into launch control."""
    try:
        observation = await observer.snapshot(
            require_complete_demand_report=phase != 'scale')
        validate_observation(observation, profile, expectation)
    except GuardViolation:
        # Market, card, cap, and durable provider-identity guards are the only
        # evidence failures authoritative enough to stop offered traffic.
        raise
    except Exception as error:  # pylint: disable=broad-except
        receipt.miss(phase, error, scale_iteration_id=scale_iteration_id)
        return None
    progress.observe(observation, profile, expectation)
    receipt.sample(phase, observation, scale_iteration_id=scale_iteration_id)
    return observation


async def _wait_for_scale(
        *,
        observer: Observer,
        profile: Profile,
        progress: Progress,
        receipt: Receipt,
        traffic: asyncio.Task[int],
        baseline: RequestTelemetry,
        campaign_progress: ExactRequestCampaignProgress | None = None,
        expectation: ProviderExpectation | None = None) -> None:
    if expectation is None:
        expectation = provider_expectation(profile, None)
    if profile.name == 'scale':
        if progress.scale_started_monotonic is None:
            raise QualificationError(
                'Provider scale has no absolute start time.')
        # The five-minute SLO is diagnostic. Keep the exact pre-demand clock,
        # but permit correctness qualification until the broader timeout.
        # Canary/small profiles retain their relative timeout.
        deadline = (progress.scale_started_monotonic +
                    profile.scale_timeout_seconds)
    else:
        deadline = time.monotonic() + profile.scale_timeout_seconds
    scale_iteration_id = 0
    arrival_attribution: _ScaleArrivalAttributionState | None = None
    while time.monotonic() < deadline:
        if traffic.done():
            try:
                successes = traffic.result()
            except BaseException as error:
                raise QualificationError(
                    'Exact traffic failed before scale convergence.') \
                    from error
            raise QualificationError(
                'Exact traffic ended before scale convergence after '
                f'{successes} successes.')
        try:
            telemetry = await observer.request_telemetry()
        except Exception:  # pylint: disable=broad-except
            await asyncio.sleep(profile.poll_seconds)
            continue
        active_delta = telemetry.ledger_active - baseline.ledger_active
        if active_delta < 0 or active_delta > expectation.exact_request_count:
            raise QualificationError(
                'Scale demand has contradictory exact dispatched identities.')
        if not _has_exact_campaign_demand(telemetry, baseline):
            # The LB demand report and request ledger are independent durable
            # publications.  A queue-to-dispatch handoff can therefore be
            # visible in either projection first.  Such a sample proves
            # nothing, but it is not evidence of corruption; only eventual
            # exact samples are paired with provider observations below.
            await asyncio.sleep(profile.poll_seconds)
            continue
        if profile.name == 'scale':
            resident = _resident_campaign_size(telemetry)
            stimulus = scale_stimulus_count(profile)
            if resident > stimulus:
                raise QualificationError(
                    'Scale demand exceeds the bounded sliding window.')
            if resident < stimulus:
                # A truthful terminal callback precedes offering the next
                # immutable identity.  The small refill gap is not a provider
                # sample; wait for the exact window without delaying either
                # transition.
                await asyncio.sleep(profile.poll_seconds)
                continue
        scale_iteration_id += 1
        receipt.request_telemetry('scale',
                                  telemetry,
                                  scale_iteration_id=scale_iteration_id)
        observation = await _validated_sample(
            observer=observer,
            profile=profile,
            progress=progress,
            receipt=receipt,
            phase='scale',
            expectation=expectation,
            scale_iteration_id=(scale_iteration_id))
        if observation is None:
            await asyncio.sleep(profile.poll_seconds)
            continue
        if (observation.database.demand_units <= 0 or
                observation.load_balancer.demand_units <= 0):
            raise QualificationError(
                'Provider scale sample has no same-observation demand.')
        if profile.name == 'scale':
            if campaign_progress is None:
                raise QualificationError(
                    'Scale campaign has no exact progress projection.')
            campaign_counters = await campaign_progress.snapshot()
            receipt.bind_scale_campaign_counters(
                scale_iteration_id=scale_iteration_id,
                counters=campaign_counters)
            arrivals = observation.load_balancer
            next_arrival_attribution = _next_scale_arrival_attribution_state(
                previous=arrival_attribution,
                unique_job_arrivals_60s=(arrivals.unique_job_arrivals_60s),
                unique_job_arrivals_300s=(arrivals.unique_job_arrivals_300s),
                headerless_arrivals_60s=arrivals.headerless_arrivals_60s,
                headerless_arrivals_300s=arrivals.headerless_arrivals_300s,
                offered_arrival_tracking_saturated=(
                    arrivals.offered_arrival_tracking_saturated),
                initial_arrivals=scale_stimulus_count(profile),
                maximum_arrivals=expectation.exact_request_count,
                campaign_offered=campaign_counters.offered,
                campaign_succeeded=campaign_counters.succeeded)
            if next_arrival_attribution is None:
                raise QualificationError(
                    'Scale stimulus contains unattributed offered arrivals.')
            # Rolling counters may rise with terminal-gated replacements or
            # fall as prior arrivals age out; the atomic driver frontier binds
            # either projection to this immutable campaign.
            arrival_attribution = next_arrival_attribution
        if progress.scale_reached_monotonic is not None:
            if progress.scale_qualified_iteration_id is not None:
                raise QualificationError(
                    'Provider scale qualification was recorded twice.')
            progress.scale_qualified_iteration_id = scale_iteration_id
            return
        await _sleep_after_observation(observation, profile.poll_seconds)
    raise QualificationError(
        f'Provider did not reach {expectation.minimum_physical_running} '
        f'physical RUNNING L4 Spot VMs for {expectation.kind.value} '
        f'providers {expectation.providers}.')


async def _join_independent_proofs(
    scale_proof: collections.abc.Awaitable[Any],
    positive_proof: collections.abc.Awaitable[Any],
) -> None:
    """Await two read-only proof consumers without controlling request work."""
    proof_tasks = (asyncio.ensure_future(scale_proof),
                   asyncio.ensure_future(positive_proof))
    try:
        await asyncio.gather(*proof_tasks)
    except BaseException:
        for task in proof_tasks:
            task.cancel()
        await asyncio.gather(*proof_tasks, return_exceptions=True)
        raise


async def _wait_for_scale_and_positive_request_telemetry(
    *,
    observer: Observer,
    profile: Profile,
    progress: Progress,
    receipt: Receipt,
    traffic: asyncio.Task[int],
    baseline: RequestTelemetry,
    campaign_progress: ExactRequestCampaignProgress,
    expectation: ProviderExpectation,
    positive_deadline_monotonic: float,
) -> None:
    """Join independent physical and request proofs for one sliding cohort."""
    await _join_independent_proofs(
        _wait_for_scale(observer=observer,
                        profile=profile,
                        progress=progress,
                        receipt=receipt,
                        traffic=traffic,
                        baseline=baseline,
                        campaign_progress=campaign_progress,
                        expectation=expectation),
        _wait_for_positive_request_telemetry(
            observer=observer,
            profile=profile,
            receipt=receipt,
            traffic=traffic,
            baseline=baseline,
            deadline_monotonic=positive_deadline_monotonic))


async def _wait_for_drain(
        *,
        observer: Observer,
        profile: Profile,
        progress: Progress,
        receipt: Receipt,
        expectation: ProviderExpectation | None = None) -> None:
    deadline = time.monotonic() + profile.drain_timeout_seconds
    while time.monotonic() < deadline:
        observation = await _validated_sample(observer=observer,
                                              profile=profile,
                                              progress=progress,
                                              receipt=receipt,
                                              phase='drain',
                                              expectation=expectation)
        if observation is None:
            await asyncio.sleep(profile.poll_seconds)
            continue
        progress.observe_zero(observation)
        if progress.drain_complete(observation, profile):
            return
        await _sleep_after_observation(observation, profile.poll_seconds)
    raise QualificationError(
        'Demand-led drain did not reach sustained exact zero.')


def resolve_data_plane_token(explicit_env: str) -> str:
    """Read an explicit runner token or the normal projected LB token ring."""
    explicit = os.environ.get(explicit_env)
    if explicit:
        return explicit
    return auth_tokens.get_lb_auth_tokens(required=True)[0]


def _provider_observers(
    *,
    service_name: str,
    scope: ProviderScope,
    profile: Profile,
    retained_volume_ids_by_region: collections.abc.Mapping[str, list[str]] |
    None = None,
) -> tuple[GcpObserver | None, AwsObserver | None]:
    """Construct only the provider clients frozen into this service version."""
    gcp = (GcpObserver(service_name=service_name, scope=scope, profile=profile)
           if 'gcp' in scope.providers else None)
    aws = (AwsObserver(
        profile=profile,
        service_name=service_name,
        scope=scope,
        retained_volume_ids_by_region=retained_volume_ids_by_region,
    ) if 'aws' in scope.providers else None)
    return gcp, aws


def freeze_provider_scope(args: argparse.Namespace) -> None:
    """Persist provider identity before any billable demand is submitted."""
    database_url = os.environ.get(args.postgres_url_env)
    if not database_url:
        raise QualificationError(
            f'{args.postgres_url_env} must contain the PostgreSQL URL.')
    postgres = PostgresObserver(database_url, args.service_name)
    deadline = time.monotonic() + args.timeout_seconds
    scope: ProviderScope | None = None
    try:
        while True:
            try:
                scope = postgres.provider_scope()
                write_provider_scope(pathlib.Path(args.output),
                                     args.service_name, scope)
                break
            except Exception as error:  # pylint: disable=broad-except
                if time.monotonic() >= deadline:
                    raise QualificationError(
                        'Service did not publish durable provider scope.') \
                        from error
                time.sleep(args.poll_seconds)
    finally:
        postgres.close()
    if scope is None:
        raise QualificationError('Provider scope was not frozen.')
    print(
        json.dumps(
            {
                'outcome': 'scope-frozen',
                'providers': list(scope.providers),
                'gcp_location_scope': (None if scope.location_scope is None else
                                       scope.location_scope.value),
                'aws_location_scope': (None if scope.aws_location_scope is None
                                       else scope.aws_location_scope.value),
                'receipt': str(pathlib.Path(args.output)),
            },
            sort_keys=True))


async def wait_for_cleanup(args: argparse.Namespace) -> None:
    """Wait until teardown has no scoped provider effect or paid DB debit."""
    database_url = os.environ.get(args.postgres_url_env)
    if not database_url:
        raise QualificationError(
            f'{args.postgres_url_env} must contain the PostgreSQL URL.')
    scope = read_provider_scope(pathlib.Path(args.scope), args.service_name)
    retained_volume_ids = read_optional_aws_volume_ids_receipt(
        pathlib.Path(args.receipt), args.service_name)
    postgres = PostgresObserver(database_url, args.service_name)
    postgres.bind_provider_scope(scope)
    cleanup_profile = dataclasses.replace(
        PROFILES['scale'], max_units=scope.max_live_paid_gpu_units)
    gcp, aws = _provider_observers(
        service_name=args.service_name,
        scope=scope,
        profile=cleanup_profile,
        retained_volume_ids_by_region=retained_volume_ids)
    deadline = time.monotonic() + args.timeout_seconds
    consecutive_zero = 0
    started_at = time.time()
    samples: list[dict[str, Any]] = []
    failure: BaseException | None = None
    try:
        while time.monotonic() < deadline:
            observation_started_at = time.time()
            observation_started_monotonic = time.monotonic()
            try:
                reads = []
                if gcp is not None:
                    reads.append(asyncio.to_thread(gcp.census))
                if aws is not None:
                    reads.append(asyncio.to_thread(aws.census))
                census_results = await asyncio.gather(*reads,
                                                      return_exceptions=True)
                for result in census_results:
                    if isinstance(result, BaseException):
                        raise result
            except GuardViolation:
                raise
            except Exception as error:  # pylint: disable=broad-except
                # An unavailable observer proves neither zero nor nonzero. Keep
                # only its safe type, break any partial zero streak, and retry
                # until the original cleanup deadline. Persistent loss still
                # reaches the fail-closed timeout below.
                consecutive_zero = 0
                observation_finished_at = time.time()
                observation_finished_monotonic = time.monotonic()
                sample = {
                    'observation_started_at': observation_started_at,
                    'observation_finished_at': observation_finished_at,
                    'observation_duration_seconds':
                        (observation_finished_monotonic -
                         observation_started_monotonic),
                    'observed_at': observation_finished_at,
                    'observation_error_type': type(error).__name__,
                    'exact_zero': False,
                    'zero_samples': consecutive_zero,
                }
                samples.append(sample)
                print(json.dumps(sample, sort_keys=True), flush=True)
                await asyncio.sleep(
                    max(
                        0, observation_started_monotonic + args.poll_seconds -
                        time.monotonic()))
                continue
            censuses = typing.cast(list[ProviderCensus | AwsProviderCensus],
                                   census_results)
            census_index = 0
            if gcp is None:
                gcp_state = empty_provider_state('gcp')
            else:
                gcp_census = typing.cast(ProviderCensus, censuses[census_index])
                census_index += 1
                gcp_state = parse_gcp_cleanup_state(
                    service_name=args.service_name,
                    instances=gcp_census.instances,
                    disks=gcp_census.disks,
                    operations=gcp_census.operations)
            if aws is None:
                aws_state = empty_provider_state('aws')
            else:
                aws_census = typing.cast(AwsProviderCensus,
                                         censuses[census_index])
                aws_state = parse_aws_cleanup_state(
                    service_instances=aws_census.service_instances,
                    service_volumes=aws_census.service_volumes)
            provider = combine_provider_states(gcp_state, aws_state)
            debits, claims, waiters = await asyncio.to_thread(
                postgres.cleanup_debits)
            exact_zero = (provider.instance_count == 0 and
                          provider.disk_count == 0 and
                          provider.inflight_operation_count == 0 and
                          debits == 0 and claims == 0 and waiters == 0)
            consecutive_zero = consecutive_zero + 1 if exact_zero else 0
            observation_finished_at = time.time()
            observation_finished_monotonic = time.monotonic()
            sample = {
                'observation_started_at': observation_started_at,
                'observation_finished_at': observation_finished_at,
                'observation_duration_seconds':
                    (observation_finished_monotonic -
                     observation_started_monotonic),
                'observed_at': observation_finished_at,
                'cleanup_claims': claims,
                'cleanup_debit_units': debits,
                'cleanup_provider_disks': provider.disk_count,
                'cleanup_provider_instances': provider.instance_count,
                'cleanup_provider_operations':
                    provider.inflight_operation_count,
                'cleanup_provider_by_cloud': {
                    cloud.cloud: dataclasses.asdict(cloud)
                    for cloud in provider.clouds
                },
                'cleanup_waiters': waiters,
                'exact_zero': exact_zero,
                'zero_samples': consecutive_zero,
            }
            samples.append(sample)
            print(json.dumps(sample, sort_keys=True), flush=True)
            if consecutive_zero >= 3:
                return
            await asyncio.sleep(
                max(
                    0, observation_started_monotonic + args.poll_seconds -
                    time.monotonic()))
        raise QualificationError(
            'Teardown left paid database debits or scoped provider resources.')
    except BaseException as error:
        failure = error
        raise
    finally:
        postgres.close()
        qualification_receipt = pathlib.Path(args.receipt)
        try:
            qualification_receipt_sha256 = hashlib.sha256(
                qualification_receipt.read_bytes()).hexdigest()
        except OSError:
            qualification_receipt_sha256 = None
        payload = {
            'schema_version': _CLEANUP_RECEIPT_SCHEMA_VERSION,
            'service_name': args.service_name,
            'service_hash': scope.service_hash,
            'lifecycle_epoch': scope.lifecycle_epoch,
            'service_version': scope.service_version,
            'controller_config_digest': scope.controller_config_digest,
            'controller_config_snapshot_id':
                scope.controller_config_snapshot_id,
            'expected_providers': list(scope.providers),
            'service_yaml_sha256': scope.service_yaml_sha256,
            'qualification_profile': scope.qualification_profile,
            'qualification_source_sha256': scope.qualification_source_sha256,
            'qualification_projection_sha256':
                scope.qualification_projection_sha256,
            'qualification_receipt_sha256': qualification_receipt_sha256,
            'started_at': started_at,
            'finished_at': time.time(),
            'outcome': 'passed' if failure is None else 'failed',
            'zero_samples': consecutive_zero,
            'samples': samples,
        }
        if failure is not None:
            payload['error_type'] = type(failure).__name__
        output = pathlib.Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n',
                          encoding='utf-8')


@dataclasses.dataclass(frozen=True, kw_only=True)
class QualificationEvidence:
    """Strict, credential-free projection consumed by the aggregate gate."""

    path: pathlib.Path
    sha256: str
    service_name: str
    service_hash: str
    lifecycle_epoch: int
    service_version: int
    controller_config_digest: str
    controller_config_snapshot_id: str
    service_yaml_sha256: str
    qualification_source_sha256: str
    qualification_projection_sha256: str
    authorized_economic_receipt_sha256: str | None
    expectation_kind: ExpectationKind
    expected_providers: tuple[str, ...]
    positive_providers: frozenset[str]
    peak_running: int
    scale_elapsed_seconds: float
    scale_slo_met: bool
    exact_request_count: int


def _read_json_object(path: pathlib.Path,
                      description: str) -> tuple[dict[str, Any], str]:
    try:
        contents = path.read_bytes()
        payload = json.loads(contents)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise QualificationError(f'{description} is unavailable.') from error
    if not isinstance(payload, dict):
        raise QualificationError(f'{description} is malformed.')
    return payload, hashlib.sha256(contents).hexdigest()


_ZERO_OBSERVATION_FIELDS = (
    'paid_debit_units',
    'claimed_units',
    'waiters',
    'provider_free_unbound_replicas',
    'postgres_demand_units',
    'provider_instances',
    'provider_running',
    'provider_gpu_units',
    'provider_running_gpu_units',
    'provider_disks',
    'provider_inflight_operations',
    'lb_demand_units',
    'lb_ready_replicas',
    'lb_unique_job_arrivals_60s',
    'lb_unique_job_arrivals_300s',
    'lb_headerless_arrivals_60s',
    'lb_headerless_arrivals_300s',
)
_ZERO_PROVIDER_CLOUD_FIELDS = (
    'instances',
    'running',
    'gpu_units',
    'running_gpu_units',
    'disks',
    'inflight_operations',
)
_PROVIDER_CLOUD_FIELDS = frozenset((*_ZERO_PROVIDER_CLOUD_FIELDS, 'shapes'))
_PROVIDER_SHAPE_FIELDS = frozenset({
    'gpu_units_per_instance',
    'instance_count',
    'instance_type',
    'running_count',
    'running_gpu_units',
})


def _strict_timestamp(sample: collections.abc.Mapping[str, Any]) -> float:
    value = sample.get('observed_at')
    if (not isinstance(value, (int, float)) or isinstance(value, bool) or
            not math.isfinite(value) or value <= 0):
        raise QualificationError('Evidence sample has no strict timestamp.')
    return float(value)


def _canonical_zero_provider_projection(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {'aws', 'gcp'}:
        return False
    return all(
        isinstance(value[cloud], dict) and set(value[cloud]) ==
        _PROVIDER_CLOUD_FIELDS and value[cloud].get('shapes') == [] and all(
            value[cloud].get(field) == 0
            for field in _ZERO_PROVIDER_CLOUD_FIELDS)
        for cloud in ('aws', 'gcp'))


def _qualification_sample_is_exact_zero(sample: object,
                                        *,
                                        phase: str = 'drain') -> bool:
    return (
        isinstance(sample, dict) and sample.get('phase') == phase and
        sample.get('exact_zero') is True and
        sample.get('lb_offered_arrival_tracking_saturated') is False and
        all(sample.get(field) == 0 for field in _ZERO_OBSERVATION_FIELDS) and
        _canonical_zero_provider_projection(sample.get('provider_by_cloud')))


def _validate_natural_drain_samples(samples: object,
                                    profile: Profile) -> tuple[float, float]:
    if not isinstance(samples, list) or len(samples) < 3:
        raise QualificationError(
            'Qualification receipt lacks natural-drain evidence.')
    trailing: list[dict[str, Any]] = []
    for sample in reversed(samples):
        if not _qualification_sample_is_exact_zero(sample):
            break
        assert isinstance(sample, dict)
        trailing.append(sample)
    trailing.reverse()
    if len(trailing) < 3:
        raise QualificationError(
            'Qualification receipt lacks natural-drain evidence.')
    timestamps = [_strict_timestamp(sample) for sample in trailing]
    if (any(current <= prior
            for prior, current in zip(timestamps, timestamps[1:])) or
            timestamps[-1] - timestamps[0] < profile.zero_hold_seconds):
        raise QualificationError(
            'Qualification receipt lacks sustained natural-drain evidence.')
    return timestamps[0], timestamps[-1]


def _ledger_counts(sample: collections.abc.Mapping[str, Any]) -> dict[str, int]:
    raw = sample.get('ledger_state_counts')
    expected_states = [
        state.value for state in async_request_ledger.AsyncRequestState
    ]
    if not isinstance(raw, list) or len(raw) != len(expected_states):
        raise QualificationError('Request telemetry evidence is malformed.')
    counts: dict[str, int] = {}
    for expected_state, item in zip(expected_states, raw):
        if (not isinstance(item, list) or len(item) != 2 or
                item[0] != expected_state or type(item[1]) is not int or
                item[1] < 0):
            raise QualificationError('Request telemetry evidence is malformed.')
        counts[item[0]] = item[1]
    return counts


def _telemetry_is_fresh_complete(
        sample: collections.abc.Mapping[str, Any]) -> bool:
    return (sample.get('state') == 'fresh' and
            sample.get('reason') == 'complete' and
            sample.get('compatibility_complete') is True and all(
                type(sample.get(field)) is int and sample[field] >= 0
                for field in ('queue_depth', 'in_flight_requests',
                              'processing_requests',
                              'confirmed_in_flight_requests',
                              'confirmed_processing_requests')))


def _telemetry_is_exactly_attributed(sample: collections.abc.Mapping[str, Any],
                                     baseline_active: int) -> bool:
    if not _telemetry_is_fresh_complete(sample):
        return False
    counts = _ledger_counts(sample)
    ledger_active = sum(counts.get(state, 0) for state in _ASYNC_ACTIVE_STATES)
    ledger_total = sum(counts.values())
    return (sample.get('ledger_active') == ledger_active and
            sample.get('ledger_total') == ledger_total and
            sample.get('ledger_succeeded') == counts.get('SUCCEEDED', 0) and
            sample['confirmed_in_flight_requests']
            == sample['in_flight_requests'] and
            sample['confirmed_processing_requests']
            == sample['processing_requests'] and
            sample['processing_requests'] <= sample['in_flight_requests'] and
            ledger_active - baseline_active == sample['in_flight_requests'])


@dataclasses.dataclass(frozen=True, kw_only=True)
class _RequestEvidence:
    """Validated request-side timing and provider-pair authority."""

    baseline_pairs: tuple[tuple[int, float], ...]
    scale_stimulus_observed_at: float | None
    scale_iterations: frozenset[int]
    scale_observed_at_by_iteration: tuple[tuple[int, float], ...]
    positive_observed_at: float | None
    final_observed_at: float


@dataclasses.dataclass(frozen=True, kw_only=True)
class _ProviderEvidence:
    """Validated provider-side timing derived from physical samples."""

    scale_elapsed_seconds: float
    scale_slo_met: bool
    scale_started_at: float
    first_scale_observed_at: float
    last_scale_observed_at: float
    qualified_observed_at: float


def _validate_request_evidence(payload: collections.abc.Mapping[str, Any], *,
                               profile: Profile,
                               exact_count: int) -> _RequestEvidence:
    samples = payload.get('request_telemetry_samples')
    if (not isinstance(samples, list) or not samples or
            any(not isinstance(sample, dict) for sample in samples)):
        raise QualificationError(
            'Qualification receipt lacks request telemetry evidence.')
    typed_samples = typing.cast(list[dict[str, Any]], samples)
    allowed_phases = {
        'baseline', 'scale-stimulus', 'positive', 'scale', 'final'
    }
    if any(
            sample.get('phase') not in allowed_phases
            for sample in typed_samples):
        raise QualificationError(
            'Qualification receipt has malformed request telemetry evidence.')
    phase_ranks = {
        'baseline': 0,
        'scale-stimulus': 1,
        'positive': 2,
        'scale': 2,
        'final': 3,
    }
    ranks = [phase_ranks[sample['phase']] for sample in typed_samples]
    if any(current < previous for previous, current in zip(ranks, ranks[1:])):
        raise QualificationError(
            'Qualification receipt has no canonical request phase order.')
    for sample in typed_samples:
        _strict_timestamp(sample)
    baselines = [
        sample for sample in typed_samples if sample.get('phase') == 'baseline'
    ]
    finals = [
        sample for sample in typed_samples if sample.get('phase') == 'final'
    ]
    scale = [
        sample for sample in typed_samples if sample.get('phase') == 'scale'
    ]
    if len(baselines) != 3 or len(finals) != 1 or not scale:
        raise QualificationError(
            'Qualification receipt lacks request telemetry evidence.')

    baseline_pairs: list[tuple[int, float]] = []
    for expected_id, sample in enumerate(baselines, start=1):
        pair_id = sample.get('baseline_iteration_id')
        pair_at = _strict_timestamp(
            {'observed_at': sample.get('baseline_pair_observed_at')})
        if (pair_id != expected_id or
                not _telemetry_is_exactly_attributed(sample, 0) or
                sample.get('queue_depth') != 0 or
                sample.get('in_flight_requests') != 0 or
                sample.get('processing_requests') != 0):
            raise QualificationError(
                'Qualification receipt has a nonzero or unpaired request '
                'baseline.')
        baseline_pairs.append((expected_id, pair_at))
    if any(current[1] <= prior[1]
           for prior, current in zip(baseline_pairs, baseline_pairs[1:])):
        raise QualificationError(
            'Qualification receipt has replayed request baseline evidence.')
    baseline = baselines[-1]
    final = finals[0]
    baseline_counts = _ledger_counts(baseline)
    baseline_active = sum(
        baseline_counts.get(state, 0) for state in _ASYNC_ACTIVE_STATES)

    scale_stimulus_observed_at: float | None = None
    scale_stimulus = [
        sample for sample in typed_samples
        if sample.get('phase') == 'scale-stimulus'
    ]
    if profile.name == 'scale':
        if len(scale_stimulus) != 1:
            raise QualificationError(
                'Qualification receipt lacks exact scale-stimulus evidence.')
        stimulus = scale_stimulus[0]
        if (not _telemetry_is_exactly_attributed(stimulus, baseline_active) or
                stimulus['queue_depth'] + stimulus['in_flight_requests']
                != scale_stimulus_count(profile)):
            raise QualificationError(
                'Qualification receipt has an incomplete scale stimulus.')
        scale_stimulus_observed_at = _strict_timestamp(stimulus)
        if baseline_pairs[-1][1] >= scale_stimulus_observed_at:
            raise QualificationError(
                'Qualification receipt has reordered scale-stimulus evidence.')
    elif scale_stimulus:
        raise QualificationError(
            'Provider canary contains economic scale-stimulus evidence.')

    scale_iteration_ids: set[int] = set()
    scale_observed_at_by_iteration: list[tuple[int, float]] = []
    scale_timestamps: list[float] = []
    for expected_iteration_id, sample in enumerate(scale, start=1):
        iteration_id = sample.get('scale_iteration_id')
        if (type(iteration_id) is not int or
                iteration_id != expected_iteration_id):
            raise QualificationError(
                'Qualification receipt scale iterations are not contiguous '
                'in request evidence order.')
        if not _telemetry_is_exactly_attributed(sample, baseline_active):
            raise QualificationError(
                'Qualification receipt contains unattributed scale demand.')
        resident = sample['queue_depth'] + sample['in_flight_requests']
        if resident != (scale_stimulus_count(profile)
                        if profile.name == 'scale' else 1):
            raise QualificationError(
                'Qualification receipt contains unattributed scale demand.')
        scale_iteration_ids.add(iteration_id)
        scale_observed_at = _strict_timestamp(sample)
        if (scale_timestamps and scale_observed_at <= scale_timestamps[-1]):
            raise QualificationError(
                'Qualification receipt scale timestamps are not strictly '
                'increasing in request iteration order.')
        scale_observed_at_by_iteration.append((iteration_id, scale_observed_at))
        scale_timestamps.append(scale_observed_at)
    if not scale_iteration_ids:
        raise QualificationError(
            'Qualification receipt contains unattributed scale demand.')
    positive_observed_at: float | None = None
    if profile.name == 'scale':
        positive = [
            sample for sample in typed_samples
            if sample.get('phase') == 'positive'
        ]
        if (len(positive) != 1 or not _telemetry_is_exactly_attributed(
                positive[0], baseline_active) or
                positive[0]['in_flight_requests'] <= 0 or
                positive[0]['processing_requests'] <= 0 or
                positive[0]['queue_depth'] + positive[0]['in_flight_requests']
                != scale_stimulus_count(profile)):
            raise QualificationError(
                'Qualification receipt lacks exact positive request telemetry.')
        positive_observed_at = _strict_timestamp(positive[0])
    elif any(sample.get('phase') == 'positive' for sample in typed_samples):
        raise QualificationError(
            'Provider canary contains economic request telemetry.')

    final_counts = _ledger_counts(final)
    final_active = sum(
        final_counts.get(state, 0) for state in _ASYNC_ACTIVE_STATES)
    if (not _telemetry_is_exactly_attributed(final, baseline_active) or
            final_active != 0 or final.get('queue_depth') != 0 or
            final.get('in_flight_requests') != 0 or
            final.get('processing_requests') != 0 or
            sum(final_counts.values()) - sum(baseline_counts.values())
            != exact_count or final_counts.get('SUCCEEDED', 0) -
            baseline_counts.get('SUCCEEDED', 0) != exact_count):
        raise QualificationError(
            'Qualification receipt lacks exact terminal ledger evidence.')
    final_observed_at = _strict_timestamp(final)
    if (not scale_timestamps or final_observed_at <= max(scale_timestamps) or
            baseline_pairs[-1][1] >= min(scale_timestamps) or
        (scale_stimulus_observed_at is not None and
         scale_stimulus_observed_at > min(scale_timestamps)) or
        (positive_observed_at is not None and
         (scale_stimulus_observed_at is None or
          scale_stimulus_observed_at >= positive_observed_at or
          final_observed_at <= positive_observed_at))):
        raise QualificationError(
            'Terminal request evidence does not follow scale demand.')
    return _RequestEvidence(
        baseline_pairs=tuple(baseline_pairs),
        scale_stimulus_observed_at=scale_stimulus_observed_at,
        scale_iterations=frozenset(scale_iteration_ids),
        scale_observed_at_by_iteration=tuple(scale_observed_at_by_iteration),
        positive_observed_at=positive_observed_at,
        final_observed_at=final_observed_at)


def _validate_paid_claim_priority_units_evidence(
        sample: collections.abc.Mapping[str, Any], *, max_units: int) -> None:
    """Bind compact live-claim logical GPU units to the offered priority."""
    claimed_units = sample.get('claimed_units')
    priority_units = sample.get('paid_claim_priority_units')
    if (type(claimed_units) is not int or not 0 <= claimed_units <= max_units or
            not isinstance(priority_units, list) or
            'paid_claim_priorities' in sample):
        raise QualificationError(
            'Qualification receipt has invalid paid claim priority-unit '
            'evidence.')
    entries: list[tuple[int, int]] = []
    for entry in priority_units:
        if (not isinstance(entry, dict) or
                set(entry) != {'priority', 'gpu_units'} or
                type(entry['priority']) is not int or
                type(entry['gpu_units']) is not int or entry['gpu_units'] <= 0):
            raise QualificationError(
                'Qualification receipt has invalid paid claim priority-unit '
                'evidence.')
        entries.append((entry['priority'], entry['gpu_units']))
    priorities = [priority for priority, _ in entries]
    if (priorities != sorted(set(priorities)) or
            sum(gpu_units for _, gpu_units in entries) != claimed_units or
            any(priority != _REQUEST_PRIORITY for priority in priorities)):
        raise QualificationError(
            'Qualification receipt has invalid paid claim priority-unit '
            'evidence.')


def _complete_provider_sample(
    sample: collections.abc.Mapping[str, Any],
    *,
    max_units: int,
    max_instances: int,
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Validate and reduce one canonical physical-provider observation."""
    by_cloud = sample.get('provider_by_cloud')
    if not isinstance(by_cloud, dict) or set(by_cloud) != {'aws', 'gcp'}:
        raise QualificationError(
            'Qualification receipt has an incomplete provider sample.')
    cloud_totals: dict[str, dict[str, int]] = {}
    for cloud in ('aws', 'gcp'):
        projection = by_cloud[cloud]
        if (not isinstance(projection, dict) or
                set(projection) != _PROVIDER_CLOUD_FIELDS):
            raise QualificationError(
                'Qualification receipt has an incomplete provider sample.')
        fields = {
            field: projection[field] for field in _ZERO_PROVIDER_CLOUD_FIELDS
        }
        if any(
                type(value) is not int or value < 0
                for value in fields.values()):
            raise QualificationError(
                'Qualification receipt has an invalid provider sample.')
        if (fields['running'] > fields['instances'] or
                fields['running_gpu_units'] > fields['gpu_units']):
            raise QualificationError(
                'Qualification receipt has an invalid provider sample.')
        shapes = projection['shapes']
        if not isinstance(shapes, list):
            raise QualificationError(
                'Qualification receipt has an incomplete provider sample.')
        shape_keys: list[tuple[str, int]] = []
        shape_instances = 0
        shape_running = 0
        shape_gpu_units = 0
        shape_running_gpu_units = 0
        for shape in shapes:
            if (not isinstance(shape, dict) or
                    set(shape) != _PROVIDER_SHAPE_FIELDS):
                raise QualificationError(
                    'Qualification receipt has an incomplete provider shape.')
            instance_type = shape['instance_type']
            width = shape['gpu_units_per_instance']
            instances = shape['instance_count']
            running = shape['running_count']
            running_gpu_units = shape['running_gpu_units']
            if (not isinstance(instance_type, str) or not instance_type or
                    type(width) is not int or width < 1 or
                    type(instances) is not int or instances < 1 or
                    type(running) is not int or not 0 <= running <= instances or
                    type(running_gpu_units) is not int or
                    running_gpu_units != running * width):
                raise QualificationError(
                    'Qualification receipt has an invalid provider shape.')
            shape_keys.append((instance_type, width))
            shape_instances += instances
            shape_running += running
            shape_gpu_units += instances * width
            shape_running_gpu_units += running_gpu_units
        if (shape_keys != sorted(set(shape_keys)) or
                shape_instances != fields['instances'] or
                shape_running != fields['running'] or
                shape_gpu_units != fields['gpu_units'] or
                shape_running_gpu_units != fields['running_gpu_units']):
            raise QualificationError(
                'Qualification receipt has a contradictory provider shape.')
        cloud_totals[cloud] = fields

    top_level_names = {
        'instances': 'provider_instances',
        'running': 'provider_running',
        'gpu_units': 'provider_gpu_units',
        'running_gpu_units': 'provider_running_gpu_units',
        'disks': 'provider_disks',
        'inflight_operations': 'provider_inflight_operations',
    }
    totals = {
        field: sample.get(top_level)
        for field, top_level in top_level_names.items()
    }
    if (any(type(value) is not int or value < 0 for value in totals.values()) or
            any(totals[field] != sum(cloud_totals[cloud][field]
                                     for cloud in ('aws', 'gcp'))
                for field in top_level_names) or
            totals['running'] > totals['instances'] or
            totals['running_gpu_units'] > totals['gpu_units'] or
            totals['gpu_units'] > max_units or
            totals['instances'] > max_instances):
        raise QualificationError(
            'Qualification receipt has contradictory provider totals or cap.')
    return totals, cloud_totals


def _validate_provider_scale_samples(
        payload: collections.abc.Mapping[str, Any], *,
        providers: tuple[str, ...], profile: Profile,
        request_evidence: _RequestEvidence) -> _ProviderEvidence:
    """Derive physical peaks and scale elapsed from complete bound samples."""
    samples = payload.get('samples')
    if not isinstance(samples, list):
        raise QualificationError(
            'Qualification receipt lacks provider scale evidence.')
    phase_ranks = {'baseline': 0, 'scale': 1, 'drain': 2}
    scale_started_at = _strict_timestamp(
        {'observed_at': payload.get('scale_started_at')})
    scale_slo_seconds = payload.get('scale_slo_seconds')
    scale_timeout_seconds = payload.get('scale_timeout_seconds')
    scale_slo_met = payload.get('scale_slo_met')
    if (isinstance(scale_slo_seconds, bool) or
            not isinstance(scale_slo_seconds, (int, float)) or
            isinstance(scale_timeout_seconds, bool) or
            not isinstance(scale_timeout_seconds,
                           (int, float)) or type(scale_slo_met) is not bool):
        raise QualificationError(
            'Qualification receipt has invalid scale timing policy.')
    scale_slo_seconds = float(scale_slo_seconds)
    scale_timeout_seconds = float(scale_timeout_seconds)
    if (not math.isfinite(scale_slo_seconds) or
            scale_slo_seconds != profile.scale_slo_seconds or
            not math.isfinite(scale_timeout_seconds) or
            scale_timeout_seconds != profile.scale_timeout_seconds):
        raise QualificationError(
            'Qualification receipt has invalid scale timing policy.')
    bound_qualified_at = _strict_timestamp(
        {'observed_at': payload.get('scale_qualified_observed_at')})
    qualified_at: float | None = None
    qualified_iteration_id: int | None = None
    baseline_pairs: list[tuple[int, float]] = []
    provider_scale_iterations: set[int] = set()
    provider_scale_entry_ids: list[int] = []
    request_scale_times = dict(request_evidence.scale_observed_at_by_iteration)
    first_scale_observed_at: float | None = None
    last_scale_observed_at: float | None = None
    previous_phase_rank = 0
    previous_observed_at: float | None = None
    calculated_peak = 0
    calculated_gpu_peak = 0
    calculated_by_cloud = {'aws': 0, 'gcp': 0}
    calculated_gpu_by_cloud = {'aws': 0, 'gcp': 0}
    arrival_attribution: _ScaleArrivalAttributionState | None = None
    for sample in samples:
        if not isinstance(sample, dict):
            raise QualificationError(
                'Qualification receipt has a malformed provider sample.')
        phase = sample.get('phase')
        if not isinstance(phase, str) or phase not in phase_ranks:
            raise QualificationError(
                'Qualification receipt has no canonical provider phase '
                'order.')
        phase_rank = phase_ranks[phase]
        if phase_rank < previous_phase_rank:
            raise QualificationError(
                'Qualification receipt has no canonical provider phase '
                'order.')
        previous_phase_rank = phase_rank
        observed_at = _strict_timestamp(sample)
        if (previous_observed_at is not None and
                observed_at <= previous_observed_at):
            raise QualificationError(
                'Qualification receipt provider timestamps are not strictly '
                'increasing in canonical phase order.')
        previous_observed_at = observed_at
        iteration_id: int | None = None
        if phase == 'scale':
            raw_iteration_id = sample.get('scale_iteration_id')
            expected_iteration_id = len(provider_scale_entry_ids) + 1
            if (type(raw_iteration_id) is not int or
                    raw_iteration_id != expected_iteration_id or
                    raw_iteration_id not in request_evidence.scale_iterations):
                raise QualificationError(
                    'Provider scale samples have no canonical contiguous '
                    'request pairing.')
            iteration_id = raw_iteration_id
            provider_scale_entry_ids.append(iteration_id)
            last_scale_observed_at = observed_at
        if 'observation_error_type' in sample:
            if (not isinstance(sample['observation_error_type'], str) or
                    not sample['observation_error_type']):
                raise QualificationError(
                    'Qualification receipt has a malformed provider sample.')
            continue
        _validate_paid_claim_priority_units_evidence(
            sample, max_units=profile.max_units)
        totals, by_cloud = _complete_provider_sample(
            sample,
            max_units=profile.max_units,
            max_instances=profile.max_replicas)
        if phase == 'baseline':
            pair_id = sample.get('baseline_iteration_id')
            pair_at = _strict_timestamp(
                {'observed_at': sample.get('baseline_pair_observed_at')})
            if not _qualification_sample_is_exact_zero(sample,
                                                       phase='baseline'):
                raise QualificationError(
                    'Qualification receipt has a nonzero provider baseline.')
            if (type(pair_id) is not int or pair_id < 1 or
                    pair_at != observed_at):
                raise QualificationError(
                    'Qualification receipt has an unpaired provider baseline.')
            baseline_pairs.append((pair_id, pair_at))
        calculated_peak = max(calculated_peak, totals['running'])
        calculated_gpu_peak = max(calculated_gpu_peak,
                                  totals['running_gpu_units'])
        for cloud in ('aws', 'gcp'):
            calculated_by_cloud[cloud] = max(calculated_by_cloud[cloud],
                                             by_cloud[cloud]['running'])
            calculated_gpu_by_cloud[cloud] = max(
                calculated_gpu_by_cloud[cloud],
                by_cloud[cloud]['running_gpu_units'])
        if phase != 'scale':
            continue
        if (sample.get('postgres_demand_units', 0) <= 0 or
                sample.get('lb_demand_units', 0) <= 0):
            raise QualificationError(
                'Provider scale sample has no same-observation demand.')
        if profile.name == 'scale':
            next_arrival_attribution = _next_scale_arrival_attribution_state(
                previous=arrival_attribution,
                unique_job_arrivals_60s=sample.get(
                    'lb_unique_job_arrivals_60s'),
                unique_job_arrivals_300s=sample.get(
                    'lb_unique_job_arrivals_300s'),
                headerless_arrivals_60s=sample.get(
                    'lb_headerless_arrivals_60s'),
                headerless_arrivals_300s=sample.get(
                    'lb_headerless_arrivals_300s'),
                offered_arrival_tracking_saturated=sample.get(
                    'lb_offered_arrival_tracking_saturated'),
                initial_arrivals=scale_stimulus_count(profile),
                maximum_arrivals=profile.exact_requests,
                campaign_offered=sample.get('campaign_offered'),
                campaign_succeeded=sample.get('campaign_succeeded'))
            if next_arrival_attribution is None:
                raise QualificationError(
                    'Provider scale sample has unattributed offered arrivals.')
            arrival_attribution = next_arrival_attribution
        assert iteration_id is not None
        if (iteration_id in provider_scale_iterations or
                observed_at < request_scale_times[iteration_id]):
            raise QualificationError(
                'Provider scale sample has no paired exact demand evidence.')
        provider_scale_iterations.add(iteration_id)
        if first_scale_observed_at is None:
            first_scale_observed_at = observed_at
        if any(
                any(by_cloud[cloud][field] != 0
                    for field in _ZERO_PROVIDER_CLOUD_FIELDS)
                for cloud in {'aws', 'gcp'} - set(providers)):
            raise QualificationError(
                'Qualification receipt contains out-of-scope scale evidence.')
        if (qualified_at is None and
                totals['running'] >= profile.minimum_running):
            qualified_at = observed_at
            qualified_iteration_id = iteration_id
    elapsed = (None if qualified_at is None else qualified_at -
               scale_started_at)
    final_baseline_at = request_evidence.baseline_pairs[-1][1]
    baseline_is_immediate = (0 <= scale_started_at - final_baseline_at <= max(
        1.0, profile.poll_seconds))
    if (provider_scale_entry_ids != list(
            range(1,
                  len(request_evidence.scale_iterations) + 1)) or
            tuple(baseline_pairs) != request_evidence.baseline_pairs or
            payload.get('baseline_qualified_iteration_id')
            != request_evidence.baseline_pairs[-1][0] or
            payload.get('baseline_qualified_observed_at') != final_baseline_at
            or not baseline_is_immediate or first_scale_observed_at is None or
            last_scale_observed_at is None or
            last_scale_observed_at >= request_evidence.final_observed_at or
            scale_started_at > first_scale_observed_at or
            scale_started_at > min(request_scale_times.values()) or
            qualified_at is None or
            qualified_at >= request_evidence.final_observed_at or
            bound_qualified_at != qualified_at or
            payload.get('scale_qualified_iteration_id')
            != qualified_iteration_id or elapsed is None or
            not math.isfinite(elapsed) or elapsed < 0 or
            elapsed > profile.scale_timeout_seconds or
            scale_slo_met != (elapsed <= profile.scale_slo_seconds) or
            payload.get('peak_running') != calculated_peak or
            payload.get('peak_running_gpu_units') != calculated_gpu_peak or
            payload.get('peak_running_by_cloud') != calculated_by_cloud or
            payload.get('peak_running_gpu_units_by_cloud')
            != calculated_gpu_by_cloud):
        raise QualificationError(
            'Qualification receipt lacks provider scale evidence.')
    if profile.name == 'scale':
        stimulus_at = request_evidence.scale_stimulus_observed_at
        positive_at = request_evidence.positive_observed_at
        if (stimulus_at is None or not scale_started_at <= stimulus_at <=
                scale_started_at + _CAMPAIGN_LOAD_WINDOW_SECONDS or
                not stimulus_at <= first_scale_observed_at <= qualified_at or
                positive_at is None or not stimulus_at < positive_at <=
                scale_started_at + positive_telemetry_window_seconds(profile)):
            raise QualificationError(
                'Qualification receipt lacks joined scale-stimulus evidence.')
    elif (request_evidence.scale_stimulus_observed_at is not None or
          request_evidence.positive_observed_at is not None):
        raise QualificationError(
            'Provider canary contains economic scale-stimulus evidence.')
    return _ProviderEvidence(scale_elapsed_seconds=elapsed,
                             scale_slo_met=scale_slo_met,
                             scale_started_at=scale_started_at,
                             first_scale_observed_at=first_scale_observed_at,
                             last_scale_observed_at=last_scale_observed_at,
                             qualified_observed_at=qualified_at)


def _read_qualification_evidence(
        path: pathlib.Path,
        expected_kind: ExpectationKind) -> QualificationEvidence:
    payload, sha256 = _read_json_object(path, 'Qualification receipt')
    providers_raw = payload.get('expected_providers')
    peaks = payload.get('peak_running_by_cloud')
    exact_count = payload.get('exact_request_count')
    exact_successes = payload.get('exact_request_successes')
    authorized_economic_receipt_sha256 = payload.get(
        'authorized_economic_receipt_sha256')
    try:
        kind = ExpectationKind(payload.get('expectation_kind'))
    except (TypeError, ValueError) as error:
        raise QualificationError('Qualification receipt is malformed.') \
            from error
    identity_fields = ('service_name', 'service_hash',
                       'controller_config_digest',
                       'controller_config_snapshot_id', 'service_yaml_sha256',
                       'qualification_source_sha256',
                       'qualification_projection_sha256')
    if (payload.get('schema_version') != _QUALIFICATION_RECEIPT_SCHEMA_VERSION
            or payload.get('outcome') != 'passed' or
            type(payload.get('request_priority')) is not int or
            payload['request_priority'] != _REQUEST_PRIORITY or
            kind is not expected_kind or not all(
                isinstance(payload.get(field), str) and payload[field]
                for field in identity_fields) or any(
                    re.fullmatch(r'[0-9a-f]{64}', payload[field]) is None
                    for field in identity_fields
                    if field not in ('service_name', 'service_hash')) or
            type(payload.get('lifecycle_epoch')) is not int or
            payload['lifecycle_epoch'] < 1 or
            type(payload.get('service_version')) is not int or
            payload['service_version'] < 1 or
            not isinstance(providers_raw, list) or
            tuple(providers_raw) not in (('aws',), ('gcp',), ('aws', 'gcp')) or
            not isinstance(peaks, dict) or
            any(cloud not in ('aws',
                              'gcp') or type(count) is not int or count < 0
                for cloud, count in peaks.items()) or
            type(payload.get('peak_running')) is not int or
            payload['peak_running'] < 1 or type(exact_count) is not int or
            exact_count < 1 or exact_successes != exact_count or
            payload.get('ledger_request_delta') != exact_count or
            payload.get('ledger_succeeded_delta') != exact_count or
        (kind is ExpectationKind.ECONOMIC and
         'authorized_economic_receipt_sha256' in payload) or
        (kind is ExpectationKind.PROVIDER_CANARY and
         (not isinstance(authorized_economic_receipt_sha256, str) or
          re.fullmatch(r'[0-9a-f]{64}',
                       authorized_economic_receipt_sha256) is None))):
        raise QualificationError('Qualification receipt is malformed.')
    providers = tuple(providers_raw)
    profile = (PROFILES['scale'] if kind is ExpectationKind.ECONOMIC else
               PROFILES['provider-canary'])
    request_evidence = _validate_request_evidence(payload,
                                                  profile=profile,
                                                  exact_count=exact_count)
    provider_evidence = _validate_provider_scale_samples(
        payload,
        providers=providers,
        profile=profile,
        request_evidence=request_evidence)
    scale_elapsed = provider_evidence.scale_elapsed_seconds
    if ((kind is ExpectationKind.ECONOMIC and
         (providers != ('aws', 'gcp') or payload.get('profile') != 'scale' or
          payload.get('minimum_running') != 100 or payload['peak_running'] < 100
          or payload.get('max_units') != profile.max_units or
          exact_count != 10_000)) or
        (kind is ExpectationKind.PROVIDER_CANARY and
         (len(providers) != 1 or payload.get('profile') != 'provider-canary' or
          payload.get('minimum_running') != 1 or
          payload.get('max_units') != profile.max_units or exact_count != 1 or
          peaks.get(providers[0], 0) < 1))):
        raise QualificationError(
            'Qualification receipt does not meet its typed evidence gate.')
    if (payload.get('qualification_profile') != profile.name or
            payload['qualification_projection_sha256']
            != _qualification_projection_sha256(
                source_sha256=payload['qualification_source_sha256'],
                profile=profile,
                providers=providers)):
        raise QualificationError(
            'Qualification receipt has a conflicting task projection.')
    positive_providers = frozenset(
        cloud for cloud, count in peaks.items() if count > 0)
    if not positive_providers.issubset(providers):
        raise QualificationError(
            'Qualification receipt contains an out-of-scope provider effect.')
    drain_first_at, drain_last_at = _validate_natural_drain_samples(
        payload.get('samples'), profile)
    finished_at = _strict_timestamp({'observed_at': payload.get('finished_at')})
    preterminal_request_times = [
        observed_at
        for _, observed_at in request_evidence.scale_observed_at_by_iteration
    ]
    if request_evidence.positive_observed_at is not None:
        preterminal_request_times.append(request_evidence.positive_observed_at)
    if not (max(preterminal_request_times) < request_evidence.final_observed_at
            and provider_evidence.last_scale_observed_at < request_evidence.
            final_observed_at < drain_first_at < drain_last_at < finished_at):
        raise QualificationError(
            'Qualification receipt has reordered lifecycle evidence.')
    return QualificationEvidence(
        path=path,
        sha256=sha256,
        service_name=payload['service_name'],
        service_hash=payload['service_hash'],
        lifecycle_epoch=payload['lifecycle_epoch'],
        service_version=payload['service_version'],
        controller_config_digest=payload['controller_config_digest'],
        controller_config_snapshot_id=payload['controller_config_snapshot_id'],
        service_yaml_sha256=payload['service_yaml_sha256'],
        qualification_source_sha256=payload['qualification_source_sha256'],
        qualification_projection_sha256=payload[
            'qualification_projection_sha256'],
        authorized_economic_receipt_sha256=(authorized_economic_receipt_sha256),
        expectation_kind=kind,
        expected_providers=providers,
        positive_providers=positive_providers,
        peak_running=payload['peak_running'],
        scale_elapsed_seconds=scale_elapsed,
        scale_slo_met=provider_evidence.scale_slo_met,
        exact_request_count=exact_count)


def _authorize_provider_canary(economic_receipt: object, *, provider: str,
                               source_sha256: str) -> QualificationEvidence:
    """Authorize one missing-provider canary before any billable launch."""
    if not isinstance(economic_receipt, str) or not economic_receipt:
        raise QualificationError(
            'Provider canary requires the completed economic receipt.')
    economic = _read_qualification_evidence(pathlib.Path(economic_receipt),
                                            ExpectationKind.ECONOMIC)
    missing = {'aws', 'gcp'} - economic.positive_providers
    if missing != {provider}:
        raise QualificationError(
            'Provider canary is not the one provider missing from the '
            'economic receipt.')
    if economic.qualification_source_sha256 != source_sha256:
        raise QualificationError(
            'Provider canary source differs from the economic task identity.')
    return economic


def _validate_cleanup_evidence(path: pathlib.Path,
                               qualification: QualificationEvidence) -> str:
    payload, sha256 = _read_json_object(path, 'Cleanup receipt')
    matching_identity = (
        payload.get('service_name') == qualification.service_name and
        payload.get('service_hash') == qualification.service_hash and
        payload.get('lifecycle_epoch') == qualification.lifecycle_epoch and
        payload.get('service_version') == qualification.service_version and
        payload.get('controller_config_digest')
        == qualification.controller_config_digest and
        payload.get('controller_config_snapshot_id')
        == qualification.controller_config_snapshot_id and
        payload.get('service_yaml_sha256') == qualification.service_yaml_sha256
        and payload.get('qualification_profile')
        == ('scale' if qualification.expectation_kind
            is ExpectationKind.ECONOMIC else 'provider-canary') and
        payload.get('qualification_source_sha256')
        == qualification.qualification_source_sha256 and
        payload.get('qualification_projection_sha256')
        == qualification.qualification_projection_sha256 and
        tuple(payload.get('expected_providers',
                          ())) == qualification.expected_providers and
        payload.get('qualification_receipt_sha256') == qualification.sha256)
    samples = payload.get('samples')
    if (payload.get('schema_version') != _CLEANUP_RECEIPT_SCHEMA_VERSION or
            payload.get('outcome') != 'passed' or not matching_identity or
            type(payload.get('zero_samples')) is not int or
            payload['zero_samples'] < 3 or not isinstance(samples, list) or
            len(samples) < 3):
        raise QualificationError('Cleanup receipt is malformed.')
    final_samples = samples[-3:]
    timestamps: list[float] = []
    counters: list[int] = []
    cleanup_cloud_fields = {
        'cloud', 'instance_count', 'running_count', 'gpu_units',
        'running_gpu_units', 'disk_count', 'inflight_operation_count', 'shapes'
    }
    for sample in final_samples:
        by_cloud = (sample.get('cleanup_provider_by_cloud') if isinstance(
            sample, dict) else None)
        if (not isinstance(sample, dict) or
                sample.get('exact_zero') is not True or any(
                    sample.get(field) != 0
                    for field in ('cleanup_claims', 'cleanup_debit_units',
                                  'cleanup_provider_disks',
                                  'cleanup_provider_instances',
                                  'cleanup_provider_operations',
                                  'cleanup_waiters')) or
                not isinstance(by_cloud, dict) or
                set(by_cloud) != {'aws', 'gcp'} or
                any(not isinstance(by_cloud[cloud], dict) or
                    set(by_cloud[cloud]) != cleanup_cloud_fields or
                    by_cloud[cloud].get('cloud') != cloud or
                    by_cloud[cloud].get('shapes') != [] or any(
                        by_cloud[cloud].get(field) != 0
                        for field in ('instance_count', 'running_count',
                                      'gpu_units', 'running_gpu_units',
                                      'disk_count', 'inflight_operation_count'))
                    for cloud in ('aws', 'gcp')) or
                type(sample.get('zero_samples')) is not int):
            raise QualificationError(
                'Cleanup receipt does not prove sustained exact zero.')
        timestamps.append(_strict_timestamp(sample))
        counters.append(sample['zero_samples'])
    if (any(current <= prior
            for prior, current in zip(timestamps, timestamps[1:])) or
            any(current != prior + 1
                for prior, current in zip(counters, counters[1:])) or
            counters[-1] != payload['zero_samples']):
        raise QualificationError(
            'Cleanup receipt does not prove sustained exact zero.')
    return sha256


def aggregate_evidence(args: argparse.Namespace) -> None:
    """Join economic, provider-union, request, and cleanup proof gates."""
    economic = _read_qualification_evidence(pathlib.Path(args.economic_receipt),
                                            ExpectationKind.ECONOMIC)
    canaries = [
        _read_qualification_evidence(pathlib.Path(path),
                                     ExpectationKind.PROVIDER_CANARY)
        for path in args.canary_receipt
    ]
    missing_providers = {'aws', 'gcp'} - economic.positive_providers
    canary_providers = [item.expected_providers[0] for item in canaries]
    if (len(canary_providers) != len(set(canary_providers)) or
            set(canary_providers) != missing_providers):
        raise QualificationError(
            'Provider canaries must exactly cover providers absent from the '
            'economic result.')
    if any(canary.qualification_source_sha256 !=
           economic.qualification_source_sha256 for canary in canaries):
        raise QualificationError(
            'Provider canaries are not projections of the economic task.')
    if any(canary.authorized_economic_receipt_sha256 != economic.sha256
           for canary in canaries):
        raise QualificationError(
            'Provider canary was not authorized by the exact economic '
            'receipt.')
    qualifications = [economic, *canaries]
    if len({item.service_name for item in qualifications
           }) != len(qualifications):
        raise QualificationError(
            'Qualification services must have unique immutable identities.')
    canary_cleanup_paths = [
        pathlib.Path(path) for path in args.canary_cleanup_receipt
    ]
    if len(canary_cleanup_paths) != len(canaries):
        raise QualificationError(
            'Every qualification receipt requires one cleanup receipt.')
    canary_cleanup_by_service: dict[str, pathlib.Path] = {}
    for path in canary_cleanup_paths:
        payload, _ = _read_json_object(path, 'Cleanup receipt')
        service_name = payload.get('service_name')
        if (not isinstance(service_name, str) or not service_name or
                service_name in canary_cleanup_by_service):
            raise QualificationError('Cleanup receipt is malformed.')
        canary_cleanup_by_service[service_name] = path
    if set(canary_cleanup_by_service) != {
            canary.service_name for canary in canaries
    }:
        raise QualificationError(
            'Every qualification receipt requires one cleanup receipt.')
    cleanup_sha256 = [
        _validate_cleanup_evidence(pathlib.Path(args.economic_cleanup_receipt),
                                   economic),
        *(_validate_cleanup_evidence(
            canary_cleanup_by_service[canary.service_name], canary)
          for canary in canaries),
    ]
    provider_union = frozenset().union(
        *(item.positive_providers for item in qualifications))
    if provider_union != {'aws', 'gcp'}:
        raise QualificationError(
            'Aggregate evidence lacks a positive AWS/GCP provider union.')
    payload = {
        'schema_version': _AGGREGATE_RECEIPT_SCHEMA_VERSION,
        'outcome': 'passed',
        'economic_service_name': economic.service_name,
        'economic_peak_running': economic.peak_running,
        'economic_scale_elapsed_seconds': economic.scale_elapsed_seconds,
        'economic_scale_slo_met': economic.scale_slo_met,
        'economic_exact_request_count': economic.exact_request_count,
        'qualification_source_sha256': economic.qualification_source_sha256,
        'positive_provider_union': sorted(provider_union),
        'qualification_receipts': [{
            'service_name': item.service_name,
            'expectation_kind': item.expectation_kind.value,
            'expected_providers': list(item.expected_providers),
            'service_yaml_sha256': item.service_yaml_sha256,
            'qualification_projection_sha256':
                item.qualification_projection_sha256,
            'sha256': item.sha256,
        } for item in qualifications],
        'cleanup_receipt_sha256': cleanup_sha256,
        'finished_at': time.time(),
    }
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n',
                      encoding='utf-8')
    print(json.dumps(payload, sort_keys=True))


async def qualify(args: argparse.Namespace) -> None:
    profile = PROFILES[args.profile]
    expectation = provider_expectation(profile, getattr(args, 'provider', None))
    token = resolve_data_plane_token(args.auth_token_env)
    database_url = os.environ.get(args.postgres_url_env)
    if not database_url:
        raise QualificationError(
            f'{args.postgres_url_env} must contain the PostgreSQL URL.')
    if (len(args.service_name) > 20 or not args.service_name or
            not args.service_name[0].islower() or
            any(character not in 'abcdefghijklmnopqrstuvwxyz0123456789-'
                for character in args.service_name)):
        raise QualificationError(
            'Use a unique lowercase service name of at most 20 characters.')

    frozen_scope = read_provider_scope(pathlib.Path(args.scope),
                                       args.service_name)
    economic_receipt = getattr(args, 'economic_receipt', None)
    authorized_economic: QualificationEvidence | None = None
    if expectation.kind is ExpectationKind.PROVIDER_CANARY:
        authorized_economic = _authorize_provider_canary(
            economic_receipt,
            provider=expectation.providers[0],
            source_sha256=frozen_scope.qualification_source_sha256)
    elif economic_receipt is not None:
        raise QualificationError(
            'Economic qualification does not accept an economic receipt.')
    postgres = PostgresObserver(database_url, args.service_name)
    provider_scope = postgres.provider_scope()
    if provider_scope != frozen_scope:
        postgres.close()
        raise GuardViolation(
            'Current service provider scope differs from the frozen receipt.')
    if (profile.max_units != provider_scope.max_live_paid_gpu_units or
            profile.name != provider_scope.qualification_profile):
        postgres.close()
        raise GuardViolation(
            'Run profile differs from the committed paid GPU cap.')
    if provider_scope.providers != expectation.providers:
        postgres.close()
        raise GuardViolation(
            'Run expectation differs from the committed provider scope.')
    http = HttpObserver(args.endpoint, token)
    gcp, aws = _provider_observers(service_name=args.service_name,
                                   scope=provider_scope,
                                   profile=profile)
    observer = Observer(postgres=postgres, gcp=gcp, aws=aws, http=http)
    progress = Progress()
    receipt = Receipt(
        path=pathlib.Path(args.receipt),
        service_name=args.service_name,
        profile=profile,
        expectation=expectation,
        scope=provider_scope,
        authorized_economic_receipt_sha256=(None if authorized_economic is None
                                            else authorized_economic.sha256))
    exact_request_successes = 0
    ledger_baseline: RequestTelemetry | None = None
    ledger_final: RequestTelemetry | None = None
    failure: BaseException | None = None
    campaign_tasks: list[asyncio.Task[int]] = []
    run_id = f'{args.service_name}-{int(time.time())}'
    try:
        await http.prove_authentication()
        ledger_baseline = await _wait_for_joined_baseline(
            observer=observer,
            profile=profile,
            progress=progress,
            receipt=receipt,
            expectation=expectation)
        progress.start_scale()
        assert progress.scale_started_monotonic is not None
        positive_deadline = positive_telemetry_deadline_monotonic(
            profile, scale_started_monotonic=progress.scale_started_monotonic)
        if profile.name == 'scale':
            stimulus_count = scale_stimulus_count(profile)
            campaign_progress = ExactRequestCampaignProgress(
                total_count=expectation.exact_request_count,
                window_size=stimulus_count)
            traffic = asyncio.create_task(
                send_exact_async_requests(
                    endpoint=args.endpoint,
                    token=token,
                    service_hash=provider_scope.service_hash,
                    prefix=f'{run_id}-campaign',
                    count=expectation.exact_request_count,
                    concurrency=stimulus_count,
                    hold_requests=expectation.exact_request_count,
                    hold_seconds=request_processing_seconds(profile),
                    timeout_seconds=(profile.scale_timeout_seconds +
                                     profile.drain_timeout_seconds),
                    campaign_progress=campaign_progress))
            campaign_tasks.append(traffic)
            stimulus_deadline = (progress.scale_started_monotonic +
                                 _CAMPAIGN_LOAD_WINDOW_SECONDS)
            await _wait_for_scale_stimulus(observer=observer,
                                           profile=profile,
                                           receipt=receipt,
                                           traffic=traffic,
                                           baseline=ledger_baseline,
                                           expected_resident=stimulus_count,
                                           deadline_monotonic=stimulus_deadline)
        else:
            traffic = asyncio.create_task(
                send_exact_async_requests(
                    endpoint=args.endpoint,
                    token=token,
                    service_hash=provider_scope.service_hash,
                    prefix=f'{run_id}-exact',
                    count=expectation.exact_request_count,
                    concurrency=profile.request_concurrency,
                    hold_requests=scale_stimulus_count(profile),
                    hold_seconds=request_processing_seconds(profile),
                    timeout_seconds=(profile.scale_timeout_seconds +
                                     profile.drain_timeout_seconds)))
            campaign_tasks.append(traffic)
        try:
            assert ledger_baseline is not None
            if profile.name == 'scale':
                await _wait_for_scale_and_positive_request_telemetry(
                    observer=observer,
                    profile=profile,
                    progress=progress,
                    receipt=receipt,
                    traffic=traffic,
                    baseline=ledger_baseline,
                    campaign_progress=campaign_progress,
                    expectation=expectation,
                    positive_deadline_monotonic=positive_deadline)
            else:
                if expectation.requires_full_request_telemetry:
                    await _wait_for_positive_request_telemetry(
                        observer=observer,
                        profile=profile,
                        receipt=receipt,
                        traffic=traffic,
                        baseline=ledger_baseline,
                        deadline_monotonic=positive_deadline)
                await _wait_for_scale(observer=observer,
                                      profile=profile,
                                      progress=progress,
                                      receipt=receipt,
                                      traffic=traffic,
                                      baseline=ledger_baseline,
                                      expectation=expectation)
            exact_request_successes = await traffic
        except BaseException:
            traffic.cancel()
            await asyncio.gather(traffic, return_exceptions=True)
            raise
        if exact_request_successes != expectation.exact_request_count:
            raise QualificationError('Exact request count is incomplete.')
        assert ledger_baseline is not None
        ledger_final = await _wait_for_final_request_telemetry(
            observer=observer,
            profile=profile,
            receipt=receipt,
            baseline=ledger_baseline,
            expected_succeeded_delta=expectation.exact_request_count)
        await _wait_for_drain(observer=observer,
                              profile=profile,
                              progress=progress,
                              receipt=receipt,
                              expectation=expectation)
    except BaseException as error:
        for task in campaign_tasks:
            task.cancel()
        await asyncio.gather(*campaign_tasks, return_exceptions=True)
        failure = error
        raise
    finally:
        receipt.finish(
            progress=progress,
            exact_request_successes=exact_request_successes,
            aws_volume_ids=(None if aws is None else aws.retained_volume_ids()),
            ledger_baseline=ledger_baseline,
            ledger_final=ledger_final,
            error=failure)
        postgres.close()
    print(
        json.dumps(
            {
                'outcome': 'passed',
                'profile': profile.name,
                'expectation_kind': expectation.kind.value,
                'expected_providers': list(expectation.providers),
                'peak_running': progress.peak_running,
                'peak_running_gpu_units': progress.peak_running_gpu_units,
                'peak_running_by_cloud': progress.peak_running_by_cloud,
                'peak_running_gpu_units_by_cloud':
                    progress.peak_running_gpu_units_by_cloud,
                'exact_request_successes': exact_request_successes,
                'receipt': str(pathlib.Path(args.receipt)),
            },
            sort_keys=True))


def render_service(args: argparse.Namespace) -> None:
    profile = PROFILES[args.profile]
    expectation = provider_expectation(profile, getattr(args, 'provider', None))
    source = pathlib.Path(args.source)
    config = yaml.safe_load(source.read_text(encoding='utf-8'))
    try:
        if not isinstance(config, dict):
            raise ValueError('source service YAML is not an object')
        source_sha256 = _qualification_source_sha256(config)
    except ValueError as error:
        raise QualificationError(
            'Source service YAML contains invalid reserved provenance.') \
            from error
    duration_limit = _fixture_duration_limit(config)
    queue_timeout = config['service']['load_balancer']['request_queue'].get(
        'timeout_seconds')
    if (not isinstance(queue_timeout, (int, float)) or
            isinstance(queue_timeout, bool) or any(
                request_processing_seconds(candidate) > duration_limit
                for candidate in PROFILES.values()) or
            duration_limit >= queue_timeout):
        raise QualificationError(
            'Fixture must bound every held request below its queue timeout.')
    economic_receipt = getattr(args, 'economic_receipt', None)
    if expectation.kind is ExpectationKind.PROVIDER_CANARY:
        _authorize_provider_canary(economic_receipt,
                                   provider=expectation.providers[0],
                                   source_sha256=source_sha256)
    elif economic_receipt is not None:
        raise QualificationError(
            'Economic rendering does not accept an economic receipt.')
    if config['service'].get(
            'load_balancing_policy') != 'instance_aware_least_load':
        raise QualificationError(
            'Paid qualification requires exact accelerator routing.')
    policy = config['service']['replica_policy']
    policy.update({
        'max_replicas': profile.max_replicas,
        'max_live_paid_gpu_units': profile.max_units,
        'scale_up_rate_min_replicas': profile.scale_up_min_replicas,
        'scale_up_rate_period_seconds': profile.scale_up_period_seconds,
    })
    queue = config['service']['load_balancer']['request_queue']
    projection = _profile_projection(profile)
    queue.update({
        'min_size': projection['request_queue_min_size'],
        'max_size': projection['request_queue_max_size'],
        'max_concurrency': projection['request_queue_max_concurrency'],
        'timeout_seconds': projection['request_queue_timeout_seconds'],
    })
    resources = config['resources']
    expected_locations = [
        {
            'infra': 'aws',
        },
        {
            'infra': 'gcp',
        },
    ]
    if (resources.get('use_spot') is not True or
            resources.get('accelerators') != 'L4:1' or
            resources.get('any_of') != expected_locations or
            'infra' in resources or 'instance_type' in resources):
        raise QualificationError(
            'Service fixture is not cross-cloud whole-L4 Spot.')
    # A provider canary is a projection of this same source fixture.  It does
    # not add a second service template or any runtime placement hook.
    resources['any_of'] = [
        branch for branch in resources['any_of']
        if branch['infra'] in expectation.providers
    ]
    try:
        contract = _validate_qualification_service_config(config)
    except ValueError as error:
        raise QualificationError(
            'Rendered service lost the whole-L4 Spot contract.') from error
    if (contract.providers != expectation.providers or
            contract.max_live_paid_gpu_units != profile.max_units or
            _qualification_profile(contract) != profile or
            _qualification_source_sha256(config) != source_sha256):
        raise QualificationError(
            'Rendered service differs from its typed provider expectation.')
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml.safe_dump(config, sort_keys=False), encoding='utf-8')


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest='command', required=True)
    render = subparsers.add_parser('render')
    render.add_argument('--profile', choices=PROFILES, required=True)
    render.add_argument('--provider', choices=('aws', 'gcp'))
    render.add_argument('--source',
                        default=str(
                            pathlib.Path(__file__).with_name('service.yaml')))
    render.add_argument('--economic-receipt')
    render.add_argument('--output', required=True)

    freeze = subparsers.add_parser('freeze-scope')
    freeze.add_argument('--service-name', required=True)
    freeze.add_argument('--output', required=True)
    freeze.add_argument('--timeout-seconds', type=float, default=5 * 60)
    freeze.add_argument('--poll-seconds', type=float, default=5)
    freeze.add_argument('--postgres-url-env',
                        default='SKYPILOT_DB_CONNECTION_URI')

    run = subparsers.add_parser('run')
    run.add_argument('--profile', choices=PROFILES, required=True)
    run.add_argument('--provider', choices=('aws', 'gcp'))
    run.add_argument('--service-name', required=True)
    run.add_argument('--endpoint', required=True)
    run.add_argument('--receipt', required=True)
    run.add_argument('--scope', required=True)
    run.add_argument('--economic-receipt')
    run.add_argument('--auth-token-env',
                     default='SKYPILOT_SERVE_E2E_AUTH_TOKEN')
    run.add_argument('--postgres-url-env', default='SKYPILOT_DB_CONNECTION_URI')

    cleanup = subparsers.add_parser('wait-cleanup')
    cleanup.add_argument('--service-name', required=True)
    cleanup.add_argument('--scope', required=True)
    cleanup.add_argument('--receipt', required=True)
    cleanup.add_argument('--output', required=True)
    cleanup.add_argument('--timeout-seconds', type=float, default=10 * 60)
    cleanup.add_argument('--poll-seconds', type=float, default=10)
    cleanup.add_argument('--postgres-url-env',
                         default='SKYPILOT_DB_CONNECTION_URI')

    aggregate = subparsers.add_parser('aggregate')
    aggregate.add_argument('--economic-receipt', required=True)
    aggregate.add_argument('--economic-cleanup-receipt', required=True)
    aggregate.add_argument('--canary-receipt', action='append', default=[])
    aggregate.add_argument('--canary-cleanup-receipt',
                           action='append',
                           default=[])
    aggregate.add_argument('--output', required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == 'render':
        render_service(args)
    elif args.command == 'freeze-scope':
        freeze_provider_scope(args)
    elif args.command == 'wait-cleanup':
        asyncio.run(wait_for_cleanup(args))
    elif args.command == 'run':
        asyncio.run(qualify(args))
    elif args.command == 'aggregate':
        aggregate_evidence(args)
    else:
        raise AssertionError(args.command)


if __name__ == '__main__':
    main()
