"""Executable paid-Spot qualification shared by small and scale profiles.

The driver never creates or deletes infrastructure.  A caller renders and
starts the service, passes its authenticated endpoint here, and performs the
normal ``sky serve down`` only after this program proves demand-led scale-down.

Examples::

    python tests/skyserve/paid_capacity/qualify.py render \
      --profile small --output /tmp/paid-e2e.yaml
    sky serve up -n paid-e2e -y /tmp/paid-e2e.yaml
    python tests/skyserve/paid_capacity/qualify.py freeze-scope \
      --service-name paid-e2e --output /tmp/paid-e2e-scope.json
    python tests/skyserve/paid_capacity/qualify.py run \
      --profile small --service-name paid-e2e \
      --endpoint https://example.test --receipt /tmp/paid-e2e.json \
      --scope /tmp/paid-e2e-scope.json

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
import dataclasses
import datetime
import enum
import hashlib
import json
import os
import pathlib
import re
import time
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


_PROVIDER_SCOPE_SCHEMA_VERSION = 3


@dataclasses.dataclass(frozen=True, kw_only=True)
class Profile:
    """Bounded parameters for one qualification scale."""

    name: str
    max_units: int
    minimum_running: int
    pressure_concurrency: int
    pressure_duration_seconds: float
    warm_requests: int
    warm_concurrency: int
    scale_timeout_seconds: float
    scale_slo_seconds: float
    drain_timeout_seconds: float
    zero_hold_seconds: float
    poll_seconds: float
    scale_up_min_replicas: int
    scale_up_period_seconds: int


PROFILES = {
    'small': Profile(name='small',
                     max_units=2,
                     minimum_running=2,
                     pressure_concurrency=4,
                     pressure_duration_seconds=30,
                     warm_requests=16,
                     warm_concurrency=4,
                     scale_timeout_seconds=15 * 60,
                     scale_slo_seconds=10 * 60,
                     drain_timeout_seconds=20 * 60,
                     zero_hold_seconds=6 * 60,
                     poll_seconds=5,
                     scale_up_min_replicas=2,
                     scale_up_period_seconds=10),
    'scale': Profile(name='scale',
                     max_units=420,
                     minimum_running=100,
                     pressure_concurrency=512,
                     pressure_duration_seconds=30,
                     warm_requests=10_000,
                     warm_concurrency=256,
                     scale_timeout_seconds=15 * 60,
                     scale_slo_seconds=5 * 60,
                     drain_timeout_seconds=30 * 60,
                     zero_hold_seconds=6 * 60,
                     poll_seconds=10,
                     scale_up_min_replicas=420,
                     scale_up_period_seconds=10),
}

_AUTH_HEADER = 'X-SkyPilot-Serve-Authorization'
_JOB_ID_HEADER = 'X-SkyServe-Job-Id'
_PRIORITY_HEADER = 'X-SkyServe-Priority'
_ACCELERATORS_HEADER = 'X-SkyServe-Compatible-Accelerators'
_REQUEST_PRIORITY = 50
_GCP_LIST_PAGE_SIZE = 500
_GCP_API_RETRIES = 3
_RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
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
        return (self.state == 'fresh' and self.reason == 'complete' and
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
    if (not isinstance(observed_at, (int, float)) or
            isinstance(observed_at, bool) or observed_at < 0):
        # Unavailable/stale summaries intentionally have no source timestamp.
        observed_at = 0.0
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
    credential_profile: str | None
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
    controller_config_digest: str
    controller_config_snapshot_id: str


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
            not isinstance(branches, list) or len(branches) != 2 or not all(
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


def _validate_cross_cloud_service_config(config: object) -> tuple[int, int]:
    """Require the canonical persisted logical-L4 qualification contract."""
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
    resource_branches = resources.get('any_of')
    canonical_clouds: set[str] = set()
    canonical_resources = (isinstance(resource_branches, list) and
                           len(resource_branches) == 2 and
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
        canonical_resources = (canonical_resources and
                               canonical_clouds == {'aws', 'gcp'})
    if (service.get('load_balancing_policy') != 'instance_aware_least_load' or
            not isinstance(policy, dict) or
            policy.get('spot_placer') != 'dynamic_fallback_per_gpu' or
            type(max_paid_units) is not int or max_paid_units < 1 or
            not isinstance(queue, dict) or
            type(per_replica_concurrency) is not int or
            per_replica_concurrency < 1 or
            type(queue.get('max_concurrency')) is not int or
            queue['max_concurrency'] < 1 or not canonical_resources):
        raise ValueError('service YAML is not generic cross-cloud L4')
    return max_paid_units, per_replica_concurrency


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
    lifecycle_epoch = authority.get('service_lifecycle_epoch')
    service_version = authority.get('current_version')
    placement_catalog = authority.get('placement_catalog')
    yaml_content = authority.get('yaml_content')
    if (not isinstance(config_snapshot, collections.abc.Mapping) or
            not isinstance(service_hash, str) or not service_hash or
            type(lifecycle_epoch) is not int or lifecycle_epoch < 1 or
            type(service_version) is not int or service_version < 1 or
            not isinstance(placement_catalog, dict) or
            not isinstance(yaml_content, str)):
        raise GuardViolation(
            'Current service version has no exact workspace authority.')
    try:
        (max_live_paid_gpu_units,
         max_concurrency_per_replica) = _validate_cross_cloud_service_config(
             yaml.safe_load(yaml_content))
    except (TypeError, ValueError, yaml.YAMLError) as error:
        raise GuardViolation(
            'Current service version is not generic cross-cloud L4.') \
            from error
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
            providers == ('aws', 'gcp') and catalog.num_nodes == 1 and
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
    if max_concurrency_per_replica < max(
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
    if not aws_regions:
        raise GuardViolation(
            'Current service version has no AWS catalog regions.')
    return ProviderScope(service_hash=service_hash,
                         lifecycle_epoch=lifecycle_epoch,
                         service_version=service_version,
                         max_live_paid_gpu_units=max_live_paid_gpu_units,
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
            type(scope.lifecycle_epoch) is not int or
            scope.lifecycle_epoch < 1 or
            type(scope.service_version) is not int or
            scope.service_version < 1 or
            type(scope.max_live_paid_gpu_units) is not int or
            scope.max_live_paid_gpu_units < 1 or
            scope.providers != ('aws', 'gcp') or
        (('gcp' in scope.providers)
         != (isinstance(scope.project_id, str) and re.fullmatch(
             r'[a-z][a-z0-9-]{4,28}[a-z0-9]', scope.project_id) is not None)) or
            not isinstance(scope.workspace, str) or not scope.workspace or
        (('gcp' in scope.providers) != (scope.location_scope
                                        is GcpLocationScope.PROJECT_WIDE)) or
        (('aws' in scope.providers)
         != (scope.aws_location_scope
             is AwsLocationScope.FROZEN_CATALOG_REGIONS)) or
            not scope.aws_regions or
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
                not raw_volume_ids or
                any(not isinstance(volume_id, str) or not volume_id
                    for volume_id in raw_volume_ids)):
            raise GuardViolation(
                'AWS service effect escaped its retained launch binding.')
        if len(raw_volume_ids) != len(set(raw_volume_ids)):
            raise GuardViolation(
                'AWS service instance repeats one EBS volume identity.')
        overlap = attached_volume_ids.intersection(raw_volume_ids)
        if overlap:
            raise GuardViolation('AWS service instances share an EBS volume.')
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
    if not attached_volume_ids.issubset(existing_volume_ids):
        raise QualificationError(
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

    def _service_census(
            self
    ) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]:
        instances_by_id: dict[str, dict[str, Any]] = {}
        volumes_by_id: dict[str, dict[str, Any]] = {}
        newly_retained_by_region: dict[str,
                                       set[str]] = collections.defaultdict(set)
        tag_keys = (provision_constants.TAG_RAY_CLUSTER_NAME,
                    provision_constants.TAG_SKYPILOT_CLUSTER_NAME)
        for region_scope in self._scope.aws_regions:
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
            volume_queries.extend((True, [{
                'Name': 'volume-id',
                'Values': sorted(exact_volume_ids)[start:start + 500],
            }]) for start in range(0, len(exact_volume_ids), 500))
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
class DatabaseState:
    """One consistent PostgreSQL authority and telemetry snapshot."""

    service_hash: str
    controller: ControllerIdentity
    paid_debit_units: int
    claimed_units: int
    claim_priorities: tuple[int, ...]
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
    priorities: tuple[int, ...]


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
    priorities: list[int] = []
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
            priorities.append(priority)
    except (KeyError, TypeError) as error:
        raise GuardViolation(
            'Paid claim has incomplete priority or plan attribution.') \
            from error
    return PaidClaimCensus(gpu_units=claimed_units,
                           priorities=tuple(priorities))


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
        credential_profile = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'aws', ('profile',),
                region=region,
                workspace=workspace))
        identity = ordinary_launch_binding.ordinary_paid_aws_provider_identity(
            binding, credential_profile=credential_profile)
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
                matching_region_scopes[0].credential_profile
                != identity['credential_profile'] or
                matching_region_scopes[0].aws_account_id
                != identity['aws_account_id']):
            raise ValueError('request-derived provider identity mismatch')
        return AwsProviderIdentity(
            aws_account_id=identity['aws_account_id'],
            client_token=identity['client_token'],
            cluster_name_on_cloud=identity['cluster_name_on_cloud'],
            credential_profile=identity['credential_profile'],
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
            claim_priorities=claim_census.priorities,
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

    async def prove_authentication(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.get(self._capacity_url) as response:
                await response.read()
                if response.status not in (401, 403):
                    raise QualificationError(
                        'Data-plane authentication is not enforced.')
            async with session.get(self._capacity_url,
                                   headers={
                                       _AUTH_HEADER: f'Bearer {self._token}'
                                   }) as response:
                if response.status != 200:
                    raise QualificationError(
                        f'Authenticated capacity probe returned {response.status}.'
                    )
                await response.read()

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
        if (not isinstance(pod_uid, str) or not pod_uid or
                slot not in ('a', 'b') or payload.get('lb_role') != 'ACTIVE' or
                type(role_generation) is not int or role_generation < 1 or
                payload.get('synced') is not True or
                payload.get('draining') is not False):
            raise QualificationError(
                'Capacity endpoint lacks routed ACTIVE LB authority.')
        return LoadBalancerState(service_hash=service_hash,
                                 demand_units=demand_units(payload),
                                 ready_replicas=ready,
                                 pod_uid=pod_uid,
                                 slot=slot,
                                 role_generation=role_generation)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Observation:
    """One composed database, provider, and data-plane observation."""

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


def validate_observation(observation: Observation, profile: Profile) -> None:
    database = observation.database
    provider = observation.provider
    gcp_identities = {
        identity.cluster_name_on_cloud: identity
        for identity in database.gcp_provider_identities
    }
    aws_identities = database.aws_provider_identities
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

    def __init__(self, *, postgres: PostgresObserver, gcp: GcpObserver,
                 aws: AwsObserver, http: HttpObserver) -> None:
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
        # Capture raw provider state first, then the durable authorization used
        # to classify it.  Since a binding commit precedes its provider effect,
        # this avoids both logical-name prefix guesses and an old-DB/new-VM
        # ordering manufactured by the observer itself.
        gcp_census, aws_census = await asyncio.gather(
            asyncio.to_thread(self._gcp.census),
            asyncio.to_thread(self._aws.census))
        load_balancer = await self._http.snapshot()
        database = await asyncio.to_thread(
            self._postgres.snapshot,
            load_balancer,
            require_complete_demand_report=require_complete_demand_report)
        gcp_identities = {
            identity.cluster_name_on_cloud: identity
            for identity in database.gcp_provider_identities
        }
        provider = combine_provider_states(
            self._gcp.reduce(gcp_census, gcp_identities),
            self._aws.reduce(aws_census, database.aws_provider_identities))
        return Observation(observed_at=time.time(),
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
    scale_started_monotonic: float | None = None
    scale_reached_monotonic: float | None = None
    zero_since_monotonic: float | None = None
    zero_samples: int = 0

    def start_scale(self) -> None:
        if self.scale_started_monotonic is not None:
            raise QualificationError('Scale timer was already started.')
        self.scale_started_monotonic = time.monotonic()

    def observe(self, observation: Observation, profile: Profile) -> None:
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
        aws_running = observation.provider.cloud('aws').running_count
        gcp_running = observation.provider.cloud('gcp').running_count
        if (self.scale_reached_monotonic is None and
                observation.provider.running_count >= profile.minimum_running
                and aws_running > 0 and gcp_running > 0):
            if self.scale_started_monotonic is None:
                raise QualificationError(
                    'Provider reached the physical cross-cloud RUNNING target '
                    'before the scale timer.')
            elapsed = (observation.observed_monotonic -
                       self.scale_started_monotonic)
            if elapsed > profile.scale_slo_seconds:
                raise QualificationError(
                    f'Scale-out to {profile.minimum_running} physical RUNNING '
                    f'L4 Spot VMs across AWS and GCP took {elapsed:.1f}s; '
                    'limit is '
                    f'{profile.scale_slo_seconds:.1f}s.')
            self.scale_reached_monotonic = observation.observed_monotonic

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
        'observed_at': observation.observed_at,
        'controller_pid': observation.database.controller.pid,
        'controller_ip': observation.database.controller.ip,
        'controller_owner_epoch': observation.database.controller.owner_epoch,
        'controller_incarnation': observation.database.controller.incarnation,
        'paid_debit_units': observation.database.paid_debit_units,
        'claimed_units': observation.database.claimed_units,
        'paid_claim_priorities': list(observation.database.claim_priorities),
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
    }


class Receipt:
    """Credential-free evidence emitted even when qualification fails."""

    def __init__(self, *, path: pathlib.Path, service_name: str,
                 profile: Profile) -> None:
        self._path = path
        self._payload: dict[str, Any] = {
            'schema_version': 3,
            'service_name': service_name,
            'profile': profile.name,
            'request_priority': _REQUEST_PRIORITY,
            'max_units': profile.max_units,
            'minimum_running': profile.minimum_running,
            'started_at': time.time(),
            'samples': [],
            'request_telemetry_samples': [],
        }

    def sample(self, phase: str, observation: Observation) -> None:
        self._payload['samples'].append({
            'phase': phase,
            **observation_summary(observation),
        })

    def miss(self, phase: str, error: Exception) -> None:
        self._payload['samples'].append({
            'phase': phase,
            'observed_at': time.time(),
            'observation_error_type': type(error).__name__,
        })

    def request_telemetry(self, phase: str,
                          telemetry: RequestTelemetry) -> None:
        self._payload['request_telemetry_samples'].append({
            'phase': phase,
            **dataclasses.asdict(telemetry),
            'ledger_active': telemetry.ledger_active,
            'ledger_succeeded': telemetry.ledger_succeeded,
            'ledger_total': telemetry.ledger_total,
        })

    def finish(self,
               *,
               progress: Progress,
               pressure_successes: int,
               warm_successes: int,
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
            'scale_elapsed_seconds':
                (None if progress.scale_started_monotonic is None or
                 progress.scale_reached_monotonic is None else
                 progress.scale_reached_monotonic -
                 progress.scale_started_monotonic),
            'pressure_successes': pressure_successes,
            'warm_successes': warm_successes,
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
    if (not isinstance(payload, dict) or payload.get('schema_version') != 3 or
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
    expected_state: str,
    prior: ExactAsyncReceipt | None = None,
) -> ExactAsyncReceipt:
    if (_single_response_header(
            response, serve_constants.LB_ASYNC_LEDGER_PROTOCOL_HEADER) != str(
                serve_constants.LB_ASYNC_LEDGER_PROTOCOL_VERSION) or
            _single_response_header(
                response, serve_constants.LB_ASYNC_SERVICE_INCARNATION_HEADER)
            != service_hash):
        raise QualificationError(
            'Exact async response changed protocol or service incarnation.')
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
    if (attempt_no < 1 or revision < 1 or state != expected_state or
        (prior is not None and
         (attempt_id != prior.attempt_id or attempt_no != prior.attempt_no or
          revision <= prior.revision))):
        raise QualificationError(
            'Exact async response has a conflicting receipt transition.')
    return ExactAsyncReceipt(attempt_id=attempt_id,
                             attempt_no=attempt_no,
                             state=state,
                             revision=revision)


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
    """Submit only after an exact pre-dispatch rejection authorizes retry."""
    url = endpoint.rstrip('/') + '/v1/models/model:predict'
    body, intent_sha256 = _canonical_exact_request(request_id, duration_seconds)
    headers = _exact_request_headers(token=token,
                                     service_hash=service_hash,
                                     request_id=request_id,
                                     stable_job_id=stable_job_id,
                                     intent_sha256=intent_sha256)
    while time.monotonic() < deadline:
        try:
            async with session.post(url, headers=headers,
                                    data=body) as response:
                response_body = await response.read()
                if response.status == 202:
                    receipt = _receipt_from_headers(response,
                                                    service_hash=service_hash,
                                                    expected_state='ACCEPTED')
                    try:
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
                if response.status not in (429, 503):
                    raise QualificationError(
                        f'{request_id} returned HTTP {response.status}.')
                _receipt_from_headers(response,
                                      service_hash=service_hash,
                                      expected_state='REJECTED_PRE_DISPATCH')
                retry_after = response.headers.get('Retry-After', '1')
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as error:
            # A lost submission response is dispatch-ambiguous. This
            # qualification driver is not a durable campaign controller, so it
            # fails closed instead of replaying a non-idempotent request.
            raise QualificationError(
                f'{request_id} lost its exact admission response.') from error
        try:
            delay = min(10.0, max(0.1, float(retry_after)))
        except ValueError:
            delay = 1.0
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
    while time.monotonic() < deadline:
        retry_after = '0.5'
        try:
            async with session.post(url, headers=headers,
                                    json=payload) as response:
                await response.read()
                if response.status == 204:
                    _receipt_from_headers(response,
                                          service_hash=service_hash,
                                          expected_state='SUCCEEDED',
                                          prior=accepted)
                    return
                if (response.status not in _RETRYABLE_STATUSES and
                        response.status != 409):
                    raise QualificationError(
                        f'{request_id} completion returned '
                        f'HTTP {response.status}.')
                retry_after = response.headers.get('Retry-After', '0.5')
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
            pass
        try:
            delay = min(5.0, max(0.1, float(retry_after)))
        except ValueError:
            delay = 0.5
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
) -> int:
    """Submit and durably complete exact synthetic async requests."""
    queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()
    for index in range(count):
        queue.put_nowait((index, f'{prefix}-execution-{index:05d}'))
    deadline = time.monotonic() + timeout_seconds
    successes = 0
    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal successes
        while True:
            try:
                index, request_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            await _one_exact_async_request(
                session,
                endpoint=endpoint,
                token=token,
                service_hash=service_hash,
                request_id=request_id,
                stable_job_id=f'{prefix}-job-{index:05d}',
                duration_seconds=(hold_seconds if index < hold_requests else 0),
                deadline=deadline)
            async with lock:
                successes += 1
            queue.task_done()

    timeout = aiohttp.ClientTimeout(total=11 * 60)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout,
                                     connector=connector) as session:
        await asyncio.gather(*(worker() for _ in range(min(count, concurrency)))
                            )
    return successes


async def _one_request(session: aiohttp.ClientSession, *, url: str, token: str,
                       request_id: str, duration_seconds: float,
                       deadline: float) -> None:
    headers = {
        _AUTH_HEADER: f'Bearer {token}',
        _JOB_ID_HEADER: request_id,
        _PRIORITY_HEADER: str(_REQUEST_PRIORITY),
        _ACCELERATORS_HEADER: 'L4',
    }
    body = {
        'request_id': request_id,
        'duration_seconds': duration_seconds,
    }
    while time.monotonic() < deadline:
        try:
            async with session.post(url, headers=headers,
                                    json=body) as response:
                response_body = await response.read()
                if response.status == 200:
                    try:
                        result = json.loads(response_body)
                    except json.JSONDecodeError as error:
                        raise QualificationError(
                            f'{request_id} returned invalid JSON.') from error
                    if result.get('request_id') != request_id:
                        raise QualificationError(
                            f'{request_id} returned a different identity.')
                    if result.get('status') != 'ok':
                        raise QualificationError(
                            f'{request_id} did not report completed processing.'
                        )
                    return
                if response.status not in _RETRYABLE_STATUSES:
                    raise QualificationError(
                        f'{request_id} returned HTTP {response.status}: '
                        f'{response_body[:200]!r}')
                retry_after = response.headers.get('Retry-After', '1')
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
            retry_after = '1'
        try:
            delay = min(10.0, max(0.1, float(retry_after)))
        except ValueError:
            delay = 1.0
        await asyncio.sleep(delay)
    raise QualificationError(f'{request_id} exhausted its retry deadline.')


async def send_requests(*, endpoint: str, token: str, prefix: str, count: int,
                        concurrency: int, duration_seconds: float,
                        timeout_seconds: float) -> int:
    url = endpoint.rstrip('/') + '/v1/models/model:predict'
    queue: asyncio.Queue[str] = asyncio.Queue()
    for index in range(count):
        queue.put_nowait(f'{prefix}-{index:05d}')
    deadline = time.monotonic() + timeout_seconds
    successes = 0
    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal successes
        while True:
            try:
                request_id = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            await _one_request(session,
                               url=url,
                               token=token,
                               request_id=request_id,
                               duration_seconds=duration_seconds,
                               deadline=deadline)
            async with lock:
                successes += 1
            queue.task_done()

    timeout = aiohttp.ClientTimeout(total=11 * 60)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout,
                                     connector=connector) as session:
        await asyncio.gather(*(worker() for _ in range(min(count, concurrency)))
                            )
    return successes


async def send_continuous_pressure(*, endpoint: str, token: str, prefix: str,
                                   concurrency: int, duration_seconds: float,
                                   timeout_seconds: float,
                                   stop: asyncio.Event) -> int:
    """Keep stable, retryable requests offered until scale-out converges."""
    url = endpoint.rstrip('/') + '/v1/models/model:predict'
    deadline = time.monotonic() + timeout_seconds
    successes = 0

    async def worker(worker_index: int) -> None:
        nonlocal successes
        sequence = 0
        while not stop.is_set():
            request_id = f'{prefix}-{worker_index:03d}-{sequence:06d}'
            await _one_request(session,
                               url=url,
                               token=token,
                               request_id=request_id,
                               duration_seconds=duration_seconds,
                               deadline=deadline)
            successes += 1
            sequence += 1

    timeout = aiohttp.ClientTimeout(total=11 * 60)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout,
                                     connector=connector) as session:
        await asyncio.gather(*(worker(index) for index in range(concurrency)))
    return successes


async def _wait_for_telemetry_baseline(*, observer: Observer, profile: Profile,
                                       receipt: Receipt) -> RequestTelemetry:
    deadline = time.monotonic() + 5 * 60
    while time.monotonic() < deadline:
        try:
            telemetry = await observer.request_telemetry()
        except Exception:  # pylint: disable=broad-except
            await asyncio.sleep(profile.poll_seconds)
            continue
        receipt.request_telemetry('baseline', telemetry)
        if telemetry.is_exact_zero():
            return telemetry
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        'Service did not establish a fresh exact request-telemetry baseline.')


async def _wait_for_positive_request_telemetry(
    *,
    observer: Observer,
    profile: Profile,
    receipt: Receipt,
    traffic: asyncio.Task[int],
    baseline: RequestTelemetry,
) -> RequestTelemetry:
    deadline = time.monotonic() + max(2 * 60, 4 * profile.poll_seconds)
    while time.monotonic() < deadline:
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
        receipt.request_telemetry('positive', telemetry)
        if (telemetry.is_fresh_complete() and
                telemetry.queue_depth is not None and
                telemetry.queue_depth > 0 and
                telemetry.in_flight_requests is not None and
                telemetry.in_flight_requests > 0 and
                telemetry.processing_requests is not None and
                telemetry.processing_requests > 0 and
                telemetry.confirmed_in_flight_requests is not None and
                telemetry.confirmed_in_flight_requests > 0 and
                telemetry.confirmed_processing_requests is not None and
                telemetry.confirmed_processing_requests > 0 and
                telemetry.ledger_total > baseline.ledger_total and
                telemetry.ledger_count('ACCEPTED') > 0):
            return telemetry
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        'No fresh positive processing, queued, and in-flight sample was '
        'observed for exact traffic.')


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
        receipt.request_telemetry('final', telemetry)
        if (telemetry.is_exact_zero() and
                telemetry.ledger_total - baseline.ledger_total
                == expected_succeeded_delta and
                telemetry.ledger_succeeded - baseline.ledger_succeeded
                == expected_succeeded_delta):
            return telemetry
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        'Exact request telemetry did not reach the required SUCCEEDED delta '
        'and current-work zero.')


async def _validated_sample(*, observer: Observer, profile: Profile,
                            progress: Progress, receipt: Receipt,
                            phase: str) -> Observation | None:
    """Collect one sample without turning observer loss into launch control."""
    try:
        observation = await observer.snapshot(
            require_complete_demand_report=phase != 'scale')
        validate_observation(observation, profile)
    except GuardViolation:
        # Market, card, cap, and durable provider-identity guards are the only
        # evidence failures authoritative enough to stop offered traffic.
        raise
    except Exception as error:  # pylint: disable=broad-except
        receipt.miss(phase, error)
        return None
    progress.observe(observation, profile)
    receipt.sample(phase, observation)
    return observation


async def _wait_for_baseline(*, observer: Observer, profile: Profile,
                             progress: Progress, receipt: Receipt) -> None:
    zero_samples = 0
    deadline = time.monotonic() + 5 * 60
    while time.monotonic() < deadline:
        observation = await _validated_sample(observer=observer,
                                              profile=profile,
                                              progress=progress,
                                              receipt=receipt,
                                              phase='baseline')
        if observation is None:
            await asyncio.sleep(profile.poll_seconds)
            continue
        zero_samples = zero_samples + 1 if observation.is_exact_zero() else 0
        if zero_samples >= 3:
            return
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError('Service did not establish a fresh zero baseline.')


async def _wait_for_scale(*, observer: Observer, profile: Profile,
                          progress: Progress, receipt: Receipt,
                          pressure: asyncio.Task[int]) -> None:
    deadline = time.monotonic() + profile.scale_timeout_seconds
    while time.monotonic() < deadline:
        if pressure.done():
            try:
                successes = pressure.result()
            except BaseException as error:
                raise QualificationError(
                    'Continuous pressure failed before scale convergence.') \
                    from error
            raise QualificationError(
                'Continuous pressure ended before scale convergence after '
                f'{successes} successes.')
        observation = await _validated_sample(observer=observer,
                                              profile=profile,
                                              progress=progress,
                                              receipt=receipt,
                                              phase='scale')
        if observation is None:
            await asyncio.sleep(profile.poll_seconds)
            continue
        if progress.scale_reached_monotonic is not None:
            return
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        f'Provider did not reach {profile.minimum_running} physical RUNNING '
        'L4 Spot VMs with nonzero AWS and GCP cohorts.')


async def _wait_for_drain(*, observer: Observer, profile: Profile,
                          progress: Progress, receipt: Receipt) -> None:
    deadline = time.monotonic() + profile.drain_timeout_seconds
    while time.monotonic() < deadline:
        observation = await _validated_sample(observer=observer,
                                              profile=profile,
                                              progress=progress,
                                              receipt=receipt,
                                              phase='drain')
        if observation is None:
            await asyncio.sleep(profile.poll_seconds)
            continue
        progress.observe_zero(observation)
        if progress.drain_complete(observation, profile):
            return
        await asyncio.sleep(profile.poll_seconds)
    raise QualificationError(
        'Demand-led drain did not reach sustained exact zero.')


def resolve_data_plane_token(explicit_env: str) -> str:
    """Read an explicit runner token or the normal projected LB token ring."""
    explicit = os.environ.get(explicit_env)
    if explicit:
        return explicit
    return auth_tokens.get_lb_auth_tokens(required=True)[0]


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
    if (scope.location_scope is None or scope.aws_location_scope is None):
        raise QualificationError('Provider scope lacks one cloud boundary.')
    print(
        json.dumps(
            {
                'outcome': 'scope-frozen',
                'providers': list(scope.providers),
                'gcp_location_scope': scope.location_scope.value,
                'aws_location_scope': scope.aws_location_scope.value,
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
    gcp = GcpObserver(service_name=args.service_name,
                      scope=scope,
                      profile=cleanup_profile)
    aws = AwsObserver(profile=cleanup_profile,
                      service_name=args.service_name,
                      scope=scope,
                      retained_volume_ids_by_region=retained_volume_ids)
    deadline = time.monotonic() + args.timeout_seconds
    consecutive_zero = 0
    try:
        while time.monotonic() < deadline:
            gcp_census, aws_census = await asyncio.gather(
                asyncio.to_thread(gcp.census), asyncio.to_thread(aws.census))
            provider = combine_provider_states(
                parse_gcp_cleanup_state(service_name=args.service_name,
                                        instances=gcp_census.instances,
                                        disks=gcp_census.disks,
                                        operations=gcp_census.operations),
                parse_aws_cleanup_state(
                    service_instances=aws_census.service_instances,
                    service_volumes=aws_census.service_volumes))
            debits, claims, waiters = await asyncio.to_thread(
                postgres.cleanup_debits)
            exact_zero = (provider.instance_count == 0 and
                          provider.disk_count == 0 and
                          provider.inflight_operation_count == 0 and
                          debits == 0 and claims == 0 and waiters == 0)
            consecutive_zero = consecutive_zero + 1 if exact_zero else 0
            print(json.dumps(
                {
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
                    'zero_samples': consecutive_zero,
                },
                sort_keys=True),
                  flush=True)
            if consecutive_zero >= 3:
                return
            await asyncio.sleep(args.poll_seconds)
    finally:
        postgres.close()
    raise QualificationError(
        'Teardown left paid database debits or scoped provider resources.')


async def qualify(args: argparse.Namespace) -> None:
    profile = PROFILES[args.profile]
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
    postgres = PostgresObserver(database_url, args.service_name)
    provider_scope = postgres.provider_scope()
    if provider_scope != frozen_scope:
        postgres.close()
        raise GuardViolation(
            'Current service provider scope differs from the frozen receipt.')
    if profile.max_units != provider_scope.max_live_paid_gpu_units:
        postgres.close()
        raise GuardViolation(
            'Run profile differs from the committed paid GPU cap.')
    http = HttpObserver(args.endpoint, token)
    aws = AwsObserver(profile=profile,
                      service_name=args.service_name,
                      scope=provider_scope)
    observer = Observer(postgres=postgres,
                        gcp=GcpObserver(service_name=args.service_name,
                                        scope=provider_scope,
                                        profile=profile),
                        aws=aws,
                        http=http)
    progress = Progress()
    receipt = Receipt(path=pathlib.Path(args.receipt),
                      service_name=args.service_name,
                      profile=profile)
    warm_successes = 0
    pressure_successes = 0
    ledger_baseline: RequestTelemetry | None = None
    ledger_final: RequestTelemetry | None = None
    failure: BaseException | None = None
    run_id = f'{args.service_name}-{int(time.time())}'
    try:
        await http.prove_authentication()
        await _wait_for_baseline(observer=observer,
                                 profile=profile,
                                 progress=progress,
                                 receipt=receipt)
        ledger_baseline = await _wait_for_telemetry_baseline(observer=observer,
                                                             profile=profile,
                                                             receipt=receipt)
        progress.start_scale()
        pressure_stop = asyncio.Event()
        pressure = asyncio.create_task(
            send_continuous_pressure(
                endpoint=args.endpoint,
                token=token,
                prefix=f'{run_id}-pressure',
                concurrency=profile.pressure_concurrency,
                duration_seconds=profile.pressure_duration_seconds,
                timeout_seconds=profile.scale_timeout_seconds + 660,
                stop=pressure_stop))
        warm: asyncio.Task[int] | None = None
        try:
            await _wait_for_scale(observer=observer,
                                  profile=profile,
                                  progress=progress,
                                  receipt=receipt,
                                  pressure=pressure)
            pressure_stop.set()
            pressure_successes = await pressure
            if pressure_successes < 1:
                raise QualificationError('No pressure request completed.')
            warm = asyncio.create_task(
                send_exact_async_requests(
                    endpoint=args.endpoint,
                    token=token,
                    service_hash=provider_scope.service_hash,
                    prefix=f'{run_id}-warm',
                    count=profile.warm_requests,
                    concurrency=profile.warm_concurrency,
                    hold_requests=min(profile.minimum_running,
                                      profile.warm_requests),
                    hold_seconds=max(30, 3 * profile.poll_seconds + 10),
                    timeout_seconds=profile.drain_timeout_seconds))
            assert ledger_baseline is not None
            await _wait_for_positive_request_telemetry(observer=observer,
                                                       profile=profile,
                                                       receipt=receipt,
                                                       traffic=warm,
                                                       baseline=ledger_baseline)
            warm_successes = await warm
        except BaseException:
            pressure_stop.set()
            pressure.cancel()
            if warm is not None:
                warm.cancel()
            await asyncio.gather(pressure,
                                 *([] if warm is None else [warm]),
                                 return_exceptions=True)
            raise
        if warm_successes != profile.warm_requests:
            raise QualificationError('Warm request count is incomplete.')
        await _wait_for_drain(observer=observer,
                              profile=profile,
                              progress=progress,
                              receipt=receipt)
        assert ledger_baseline is not None
        ledger_final = await _wait_for_final_request_telemetry(
            observer=observer,
            profile=profile,
            receipt=receipt,
            baseline=ledger_baseline,
            expected_succeeded_delta=profile.warm_requests)
    except BaseException as error:
        failure = error
        raise
    finally:
        receipt.finish(progress=progress,
                       pressure_successes=pressure_successes,
                       warm_successes=warm_successes,
                       aws_volume_ids=aws.retained_volume_ids(),
                       ledger_baseline=ledger_baseline,
                       ledger_final=ledger_final,
                       error=failure)
        postgres.close()
    print(
        json.dumps(
            {
                'outcome': 'passed',
                'profile': profile.name,
                'peak_running': progress.peak_running,
                'peak_running_gpu_units': progress.peak_running_gpu_units,
                'peak_running_by_cloud': progress.peak_running_by_cloud,
                'peak_running_gpu_units_by_cloud':
                    progress.peak_running_gpu_units_by_cloud,
                'warm_successes': warm_successes,
                'receipt': str(pathlib.Path(args.receipt)),
            },
            sort_keys=True))


def render_service(args: argparse.Namespace) -> None:
    profile = PROFILES[args.profile]
    source = pathlib.Path(args.source)
    config = yaml.safe_load(source.read_text(encoding='utf-8'))
    if config['service'].get(
            'load_balancing_policy') != 'instance_aware_least_load':
        raise QualificationError(
            'Paid qualification requires exact accelerator routing.')
    policy = config['service']['replica_policy']
    policy.update({
        'max_replicas': profile.max_units,
        'max_live_paid_gpu_units': profile.max_units,
        'scale_up_rate_min_replicas': profile.scale_up_min_replicas,
        'scale_up_rate_period_seconds': profile.scale_up_period_seconds,
    })
    queue = config['service']['load_balancer']['request_queue']
    queue.update({
        'min_size': profile.pressure_concurrency,
        'max_size': max(32, profile.pressure_concurrency * 2),
        'max_concurrency': max(8, min(128, profile.pressure_concurrency)),
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
    try:
        _validate_cross_cloud_service_config(config)
    except ValueError as error:
        raise QualificationError(
            'Rendered service lost the cross-cloud L4 contract.') from error
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml.safe_dump(config, sort_keys=False), encoding='utf-8')


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest='command', required=True)
    render = subparsers.add_parser('render')
    render.add_argument('--profile', choices=PROFILES, required=True)
    render.add_argument('--source',
                        default=str(
                            pathlib.Path(__file__).with_name('service.yaml')))
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
    run.add_argument('--service-name', required=True)
    run.add_argument('--endpoint', required=True)
    run.add_argument('--receipt', required=True)
    run.add_argument('--scope', required=True)
    run.add_argument('--auth-token-env',
                     default='SKYPILOT_SERVE_E2E_AUTH_TOKEN')
    run.add_argument('--postgres-url-env', default='SKYPILOT_DB_CONNECTION_URI')

    cleanup = subparsers.add_parser('wait-cleanup')
    cleanup.add_argument('--service-name', required=True)
    cleanup.add_argument('--scope', required=True)
    cleanup.add_argument('--receipt', required=True)
    cleanup.add_argument('--timeout-seconds', type=float, default=10 * 60)
    cleanup.add_argument('--poll-seconds', type=float, default=10)
    cleanup.add_argument('--postgres-url-env',
                         default='SKYPILOT_DB_CONNECTION_URI')
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
    else:
        asyncio.run(qualify(args))


if __name__ == '__main__':
    main()
