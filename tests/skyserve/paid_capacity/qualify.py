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
emitted into the receipt.
"""

import argparse
import asyncio
import collections.abc
import dataclasses
import datetime
import hashlib
import json
import os
import pathlib
import re
import subprocess
import time
from typing import Any

import aiohttp
import sqlalchemy
import yaml

from sky import skypilot_config
from sky.provision.gcp import instance_utils
from sky.serve import auth_tokens
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import serve_utils
from sky.server.requests import non_pool_launch
from sky.server.requests import postgres as request_postgres


class QualificationError(RuntimeError):
    """A qualification invariant failed."""


class GuardViolation(QualificationError):
    """An authoritative market, card, cap, or identity guard failed."""


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
                     max_units=120,
                     minimum_running=100,
                     pressure_concurrency=256,
                     pressure_duration_seconds=30,
                     warm_requests=10_000,
                     warm_concurrency=256,
                     scale_timeout_seconds=15 * 60,
                     scale_slo_seconds=5 * 60,
                     drain_timeout_seconds=30 * 60,
                     zero_hold_seconds=6 * 60,
                     poll_seconds=10,
                     scale_up_min_replicas=100,
                     scale_up_period_seconds=60),
}

_AUTH_HEADER = 'X-SkyPilot-Serve-Authorization'
_JOB_ID_HEADER = 'X-SkyServe-Job-Id'
_PRIORITY_HEADER = 'X-SkyServe-Priority'
_ACCELERATORS_HEADER = 'X-SkyServe-Compatible-Accelerators'
_RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
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
class ProviderState:
    """One exact provider-native reduction."""

    instance_count: int
    running_count: int
    disk_count: int
    inflight_operation_count: int
    cluster_names: frozenset[str]


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderCensus:
    """Raw provider read captured before its PostgreSQL authorization."""

    instances: object
    disks: object
    operations: object


@dataclasses.dataclass(frozen=True, kw_only=True)
class ProviderScope:
    """Exact GCP scope frozen by the service's committed version."""

    service_hash: str
    lifecycle_epoch: int
    service_version: int
    project_id: str
    workspace: str
    region: str
    controller_config_digest: str
    controller_config_snapshot_id: str


def provider_scope_from_controller_config(
        authority: collections.abc.Mapping[str, Any], *,
        expected_region: str) -> ProviderScope:
    """Resolve GCP scope from immutable version state, never ambient config."""
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
    if (not isinstance(config_snapshot, collections.abc.Mapping) or
            not isinstance(service_hash, str) or not service_hash or
            type(lifecycle_epoch) is not int or lifecycle_epoch < 1 or
            type(service_version) is not int or service_version < 1 or
            not isinstance(expected_region, str) or
            re.fullmatch(r'[a-z]+-[a-z0-9]+[0-9]', expected_region) is None):
        raise GuardViolation(
            'Current service version has no exact workspace/region authority.')
    project_id = (
        skypilot_config.get_effective_workspace_region_config_from_snapshot(
            config_snapshot,
            'gcp', ('project_id',),
            region=expected_region,
            workspace=workspace))
    if (not isinstance(project_id, str) or
            re.fullmatch(r'[a-z][a-z0-9-]{4,28}[a-z0-9]', project_id) is None):
        raise GuardViolation(
            'Current service version has no exact GCP project authority.')
    return ProviderScope(service_hash=service_hash,
                         lifecycle_epoch=lifecycle_epoch,
                         service_version=service_version,
                         project_id=project_id,
                         workspace=workspace,
                         region=expected_region,
                         controller_config_digest=digest,
                         controller_config_snapshot_id=snapshot_id)


def write_provider_scope(path: pathlib.Path, service_name: str,
                         scope: ProviderScope) -> None:
    """Persist credential-free teardown authority before traffic is offered."""
    payload = {
        'schema_version': 1,
        'service_name': service_name,
        'provider': 'gcp',
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
    if (not isinstance(payload, dict) or payload.get('schema_version') != 1 or
            payload.get('service_name') != service_name or
            payload.get('provider') != 'gcp' or set(payload)
            != field_names | {'schema_version', 'service_name', 'provider'}):
        raise QualificationError('Provider-scope receipt is malformed.')
    try:
        scope = ProviderScope(**{field: payload[field] for field in field_names})
    except TypeError as error:
        raise QualificationError('Provider-scope receipt is malformed.') \
            from error
    if (not isinstance(scope.service_hash, str) or not scope.service_hash or
            type(scope.lifecycle_epoch) is not int or
            scope.lifecycle_epoch < 1 or
            type(scope.service_version) is not int or
            scope.service_version < 1 or
            not isinstance(scope.project_id, str) or re.fullmatch(
                r'[a-z][a-z0-9-]{4,28}[a-z0-9]', scope.project_id) is None or
            not isinstance(scope.workspace, str) or not scope.workspace or
            not isinstance(scope.region, str) or
            re.fullmatch(r'[a-z]+-[a-z0-9]+[0-9]', scope.region) is None or
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


def parse_gcp_state(
    *,
    service_name: str,
    expected_cluster_zones: collections.abc.Mapping[str, str],
    profile: Profile,
    instances: object,
    disks: object,
    expected_region: str,
    operations: object = ()) -> ProviderState:
    """Validate and reduce one provider-native GCP census."""
    if (not isinstance(instances, list) or not isinstance(disks, list) or
            not isinstance(operations, (list, tuple))):
        raise QualificationError('gcloud census is not a JSON list.')
    if not service_name:
        raise QualificationError('Provider census requires a service scope.')
    expected_cluster_names = frozenset(expected_cluster_zones)
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
    for operation in operations:
        if (not isinstance(operation, dict) or
                not str(operation.get('operationType',
                                      '')).casefold().endswith('insert') or
                str(operation.get('status', '')).upper() == 'DONE' or
                not _generated_name_in_service_scope(
                    operation.get('targetLink'), service_name)):
            continue
        target_link = str(operation.get('targetLink', ''))
        zone_match = re.search(r'/zones/([^/]+)/', target_link)
        if (zone_match is None or
                not zone_match.group(1).startswith(f'{expected_region}-')):
            raise GuardViolation(
                'GCP create operation is outside the bound region.')
        target_name = _basename(target_link)
        previous_zone = owned_inflight_operation_targets.setdefault(
            target_name, zone_match.group(1))
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
        if _basename(instance.get('machineType')) != 'g2-standard-4':
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} has the wrong shape.')
        instance_zone = _basename(instance.get('zone'))
        if (not instance_zone.startswith(f'{expected_region}-') or
                instance_zone != expected_cluster_zones[cluster_name]):
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} is in the wrong '
                'binding zone.')
        accelerators = instance.get('guestAccelerators')
        if (not isinstance(accelerators, list) or len(accelerators) != 1 or
                not isinstance(accelerators[0], dict) or
                accelerators[0].get('acceleratorCount') != 1 or
                _basename(accelerators[0].get('acceleratorType')).casefold()
                != 'nvidia-l4'):
            raise GuardViolation(
                f'GCP instance {instance.get("name")!r} is not one L4.')
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
        if _basename(disk.get('zone')) != expected_cluster_zones[cluster_name]:
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
        if operation_zone != expected_cluster_zones[cluster_name]:
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
    if len(owned_instances) > profile.max_units:
        raise GuardViolation('Provider instance count exceeded the armed cap.')
    return ProviderState(
        instance_count=len(owned_instances),
        running_count=sum(
            str(item.get('status', '')).upper() == 'RUNNING'
            for item in owned_instances),
        disk_count=len(owned_disks),
        inflight_operation_count=len(owned_inflight_operation_targets),
        cluster_names=frozenset(cluster_names),
    )


def parse_gcp_cleanup_state(*, service_name: str, instances: object,
                            disks: object, operations: object) -> ProviderState:
    """Count every remaining provider effect in the dedicated test scope."""
    if (not isinstance(instances, list) or not isinstance(disks, list) or
            not isinstance(operations, (list, tuple))):
        raise QualificationError('gcloud cleanup census is not a JSON list.')
    owned_instances = _unique_scoped_resources(instances,
                                               service_name=service_name,
                                               kind='instance')
    # Cleanup counts exact generated names even when provider metadata was lost.
    # A missing marker must keep an orphan billable disk visible, not exempt it.
    owned_disks = _unique_scoped_resources(disks,
                                           service_name=service_name,
                                           kind='disk')
    operation_targets = {
        _basename(item.get('targetLink'))
        for item in operations
        if isinstance(item, dict) and
        str(item.get('operationType', '')).casefold().endswith('insert') and
        str(item.get('status', '')).upper() != 'DONE' and
        _generated_name_in_service_scope(item.get('targetLink'), service_name)
    }
    cluster_names = frozenset(
        cluster_name for item in owned_instances
        if (cluster_name := _cluster_label(item)) is not None)
    return ProviderState(
        instance_count=len(owned_instances),
        running_count=sum(
            str(item.get('status', '')).upper() == 'RUNNING'
            for item in owned_instances),
        disk_count=len(owned_disks),
        inflight_operation_count=len(operation_targets),
        cluster_names=cluster_names,
    )


class GcpObserver:
    """Read-only provider census reduced by durable cloud identities."""

    def __init__(self, *, service_name: str, scope: ProviderScope,
                 profile: Profile) -> None:
        self._service_name = service_name
        self._scope = scope
        self._profile = profile

    def _list(self, kind: str) -> object:
        result = subprocess.run([
            'gcloud', 'compute', kind, 'list', '--project',
            self._scope.project_id, '--format=json'
        ],
                                check=False,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                timeout=45)
        if result.returncode != 0:
            raise QualificationError(
                f'gcloud {kind} census failed: {result.stderr[:300]}')
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise QualificationError(
                f'gcloud {kind} returned invalid JSON.') from error

    def census(self) -> ProviderCensus:
        return ProviderCensus(instances=self._list('instances'),
                              disks=self._list('disks'),
                              operations=self._list('operations'))

    def reduce(
        self, census: ProviderCensus,
        expected_cluster_zones: collections.abc.Mapping[str,
                                                        str]) -> ProviderState:
        return parse_gcp_state(service_name=self._service_name,
                               expected_cluster_zones=expected_cluster_zones,
                               profile=self._profile,
                               instances=census.instances,
                               disks=census.disks,
                               expected_region=self._scope.region,
                               operations=census.operations)


@dataclasses.dataclass(frozen=True, kw_only=True)
class DatabaseState:
    """One consistent PostgreSQL authority and telemetry snapshot."""

    service_hash: str
    paid_debit_units: int
    claimed_units: int
    waiter_count: int
    demand_units: int
    bound_cluster_zones: tuple[tuple[str, str], ...]


@dataclasses.dataclass(frozen=True, kw_only=True)
class PaidDebitCensus:
    """Rows and physical GPU units still debited by production admission."""

    replicas: tuple[collections.abc.Mapping[str, Any], ...]
    gpu_units: int


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


_BOUND_REQUEST_PROFILE_FIELDS = (
    'binding_protocol_version',
    'profile_kind',
    'profile_version',
    'profile_digest',
    'capability_cohort_epoch',
    'capability_profile_set_digest',
    'receipt_protocol_version',
)


def gcp_identity_from_retained_request(
    binding: collections.abc.Mapping[str, Any],
    request_row: sqlalchemy.engine.RowMapping,
    scope: ProviderScope,
) -> dict[str, Any]:
    """Recover one exact GCP allocation from its immutable API request."""
    try:
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
        context = ordinary_launch_binding.bound_context_from_association(
            binding)
        parsed_context = (
            ordinary_launch_binding.parse_bound_non_pool_launch_context(
                body.extra_launch_context))
        if parsed_context != context:
            raise ValueError('request binding context mismatch')
        config_snapshot = body.override_skypilot_config
        pool_identity = paid_capacity.pool_key_payload(
            str(binding['paid_capacity_pool_key']))
        workspace = binding['service_workspace']
        if (not isinstance(config_snapshot, collections.abc.Mapping) or
                not isinstance(pool_identity, collections.abc.Mapping) or
                pool_identity.get('cloud') != 'gcp' or
                pool_identity.get('workspace') != workspace or
                pool_identity.get('region') != scope.region or
                not isinstance(pool_identity.get('zone'), str) or
                not pool_identity['zone'].startswith(f'{scope.region}-') or
                config_snapshot.get('active_workspace') != workspace or
                workspace != scope.workspace):
            raise ValueError('request provider scope mismatch')
        managed_instance_group = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'gcp', ('managed_instance_group',),
                region=pool_identity['region'],
                workspace=workspace))
        if managed_instance_group is not None:
            raise ValueError('managed instance groups are outside this test')
        request_project = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'gcp', ('project_id',),
                region=pool_identity['region'],
                workspace=workspace))
        identity = ordinary_launch_binding.ordinary_paid_gcp_provider_identity(
            binding, project_id=request_project)
        if (identity['project_id'] != scope.project_id or
                identity['workspace'] != scope.workspace or
                identity['region'] != scope.region or
                identity['zone'] != pool_identity['zone'] or
                identity['instance_type'] != 'g2-standard-4' or
                identity['num_nodes'] != 1 or identity['use_spot'] is not True):
            raise ValueError('request-derived provider identity mismatch')
        return identity
    except (AttributeError, KeyError, TypeError, ValueError,
            ordinary_launch_binding.OrdinaryLaunchBindingConflict) as error:
        raise GuardViolation(
            'Paid replica has no exact retained-request GCP identity.') \
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
            if _is_projected_paid_success(binding)
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
            'Paid replica has no unique current or latest successful binding.')
    return selected[0]


class PostgresObserver:
    """Read durable provider authority without re-gating committed claims.

    A paid claim points to its immutable plan.  The mutable plan head is not
    consulted after that commit; only current route/lifecycle authority must
    remain fresh while traffic is being qualified.
    """

    def __init__(self, database_url: str, service_name: str,
                 region: str) -> None:
        url = sqlalchemy.engine.make_url(database_url)
        if not url.drivername.startswith('postgresql'):
            raise QualificationError('Paid qualification requires PostgreSQL.')
        self._engine = sqlalchemy.create_engine(database_url,
                                                pool_pre_ping=True)
        self._service_name = service_name
        self._region = region
        self._provider_scope: ProviderScope | None = None

    def close(self) -> None:
        self._engine.dispose()

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
                           v.controller_config_snapshot_id
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
        scope = provider_scope_from_controller_config(
            row, expected_region=self._region)
        self._provider_scope = scope
        return scope

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

    def snapshot(self, load_balancer: 'LoadBalancerState') -> DatabaseState:
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
                           s.lb_ha_enabled,
                           s.lb_active_slot,
                           s.lb_cutover_generation,
                           s.lb_cutover_phase,
                           v.controller_config_digest,
                           v.controller_config_snapshot_id,
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
            if (service_hash != scope.service_hash or
                    lifecycle_epoch != scope.lifecycle_epoch or
                    authority['current_version'] != scope.service_version or
                    authority['workspace'] != scope.workspace or
                    authority['controller_config_digest']
                    != scope.controller_config_digest or
                    authority['controller_config_snapshot_id']
                    != scope.controller_config_snapshot_id):
                raise GuardViolation(
                    'Service provider scope changed during qualification.')
            replicas = connection.execute(
                sqlalchemy.text('''
                    SELECT replica.replica_id, replica.cluster_name,
                           replica.status, replica.is_spot,
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
                    'service_hash': service_hash,
                    'profile_kind':
                        (ordinary_launch_binding.NonPoolLaunchProfileKind.
                         ORDINARY_PAID.value),
                    'protocol': (ordinary_launch_binding.
                                 NON_POOL_BINDING_PROTOCOL_VERSION),
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
                    'service_hash': service_hash,
                    'profile_kind':
                        (ordinary_launch_binding.NonPoolLaunchProfileKind.
                         ORDINARY_PAID.value),
                    'protocol': (ordinary_launch_binding.
                                 NON_POOL_BINDING_PROTOCOL_VERSION),
                }).mappings().all()
            claims = connection.execute(
                sqlalchemy.text('''
                    SELECT c.replica_id, c.claimed_at,
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
        request_by_association: dict[str, sqlalchemy.engine.RowMapping] = {}
        for request in requests:
            association_id = str(request['ordinary_launch_association_id'])
            if association_id in request_by_association:
                raise GuardViolation(
                    'A paid binding has multiple retained API requests.')
            request_by_association[association_id] = request
        bound_cluster_zones: dict[str, str] = {}
        for replica in debits.replicas:
            binding = select_replica_binding(replica, bindings)
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
                identity = gcp_identity_from_retained_request(
                    binding, request, scope)
            except (KeyError, TypeError, ValueError,
                    ordinary_launch_binding.OrdinaryLaunchBindingConflict
                   ) as error:
                raise GuardViolation(
                    'Paid replica has no exact durable GCP launch binding.') \
                    from error
            if (replica['is_spot'] is not True or
                    not replica['replica_record_id'] or
                    binding['paid_capacity_pool_key']
                    != replica['paid_capacity_pool_key']):
                raise GuardViolation(
                    'Paid replica has no exact retained Spot launch binding.')
            cloud_name = identity['cluster_name_on_cloud']
            if cloud_name in bound_cluster_zones:
                raise GuardViolation(
                    'Paid replicas share one durable GCP provider identity.')
            bound_cluster_zones[cloud_name] = identity['zone']
        claimed_units = 0
        for claim in claims:
            if (type(claim['capacity_plan_generation']) is not int or
                    claim['capacity_plan_generation'] <= 0 or
                    not isinstance(claim['capacity_plan_sha256'], str) or
                    claim['capacity_plan_sha256']
                    != claim['persisted_plan_sha256'] or
                    str(claim['capacity_plan_accelerator']).casefold() != 'l4'
                    or claim['capacity_plan_units'] != 1):
                raise GuardViolation(
                    'Paid claim is not linked to one immutable L4 plan.')
            claimed_units += claim['capacity_plan_units']
        demand_report = select_route_authoritative_report(
            authority, demand_reports, load_balancer)
        return DatabaseState(
            service_hash=service_hash,
            paid_debit_units=debits.gpu_units,
            claimed_units=claimed_units,
            waiter_count=int(waiter_count),
            demand_units=demand_units(demand_report['payload']),
            bound_cluster_zones=tuple(sorted(bound_cluster_zones.items())),
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
) -> collections.abc.Mapping[str, Any]:
    """Select only the report belonging to the LB that served this probe."""
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
    if len(newest) != 1 or newest[0].get('complete') is not True:
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
                self.database.demand_units == 0 and
                self.provider.instance_count == 0 and
                self.provider.disk_count == 0 and
                self.provider.inflight_operation_count == 0 and
                self.load_balancer.demand_units == 0 and
                self.load_balancer.ready_replicas == 0)


def validate_observation(observation: Observation, profile: Profile) -> None:
    database = observation.database
    provider = observation.provider
    bound_cluster_names = frozenset(
        cluster_name for cluster_name, _ in database.bound_cluster_zones)
    if database.service_hash != observation.load_balancer.service_hash:
        raise QualificationError(
            'PostgreSQL and load balancer incarnations differ.')
    if (database.claimed_units > profile.max_units or
            database.paid_debit_units > profile.max_units):
        raise GuardViolation('PostgreSQL capacity exceeded the armed cap.')
    if (provider.instance_count > len(bound_cluster_names) or
            provider.disk_count > len(bound_cluster_names) or
            provider.inflight_operation_count > len(bound_cluster_names)):
        raise GuardViolation('Provider effects exceed durable launch bindings.')
    unbound_clusters = provider.cluster_names - bound_cluster_names
    if unbound_clusters:
        raise GuardViolation(
            'Provider effect exists without a durable launch binding.')


class Observer:
    """Compose raw provider state with newer durable/data-plane evidence."""

    def __init__(self, *, postgres: PostgresObserver, gcp: GcpObserver,
                 http: HttpObserver) -> None:
        self._postgres = postgres
        self._gcp = gcp
        self._http = http

    async def snapshot(self) -> Observation:
        # Capture raw provider state first, then the durable authorization used
        # to classify it.  Since a binding commit precedes its provider effect,
        # this avoids both logical-name prefix guesses and an old-DB/new-VM
        # ordering manufactured by the observer itself.
        census = await asyncio.to_thread(self._gcp.census)
        load_balancer = await self._http.snapshot()
        database = await asyncio.to_thread(self._postgres.snapshot,
                                           load_balancer)
        provider = self._gcp.reduce(census, dict(database.bound_cluster_zones))
        return Observation(observed_at=time.time(),
                           observed_monotonic=time.monotonic(),
                           database=database,
                           provider=provider,
                           load_balancer=load_balancer)


@dataclasses.dataclass
class Progress:
    """Strict scale and drain gates accumulated from valid samples."""

    peak_running: int = 0
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
        if (self.scale_reached_monotonic is None and
                observation.provider.running_count >= profile.minimum_running):
            if self.scale_started_monotonic is None:
                raise QualificationError(
                    'Provider reached RUNNING before the scale timer.')
            elapsed = (observation.observed_monotonic -
                       self.scale_started_monotonic)
            if elapsed > profile.scale_slo_seconds:
                raise QualificationError(
                    f'Scale-out took {elapsed:.1f}s; limit is '
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
        'paid_debit_units': observation.database.paid_debit_units,
        'claimed_units': observation.database.claimed_units,
        'waiters': observation.database.waiter_count,
        'postgres_demand_units': observation.database.demand_units,
        'provider_instances': observation.provider.instance_count,
        'provider_running': observation.provider.running_count,
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
            'schema_version': 1,
            'service_name': service_name,
            'profile': profile.name,
            'max_units': profile.max_units,
            'minimum_running': profile.minimum_running,
            'started_at': time.time(),
            'samples': [],
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

    def finish(self,
               *,
               progress: Progress,
               pressure_successes: int,
               warm_successes: int,
               error: BaseException | None = None) -> None:
        self._payload.update({
            'finished_at': time.time(),
            'outcome': 'passed' if error is None else 'failed',
            'peak_running': progress.peak_running,
            'scale_elapsed_seconds':
                (None if progress.scale_started_monotonic is None or
                 progress.scale_reached_monotonic is None else
                 progress.scale_reached_monotonic -
                 progress.scale_started_monotonic),
            'pressure_successes': pressure_successes,
            'warm_successes': warm_successes,
        })
        if error is not None:
            # Never serialize exception text: transport/database errors may
            # contain an endpoint or credential-bearing connection string.
            self._payload['error_type'] = type(error).__name__
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._payload, indent=2, sort_keys=True) + '\n',
            encoding='utf-8')


async def _one_request(session: aiohttp.ClientSession, *, url: str, token: str,
                       request_id: str, duration_seconds: float,
                       deadline: float) -> None:
    headers = {
        _AUTH_HEADER: f'Bearer {token}',
        _JOB_ID_HEADER: request_id,
        _PRIORITY_HEADER: '50',
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


async def _validated_sample(*, observer: Observer, profile: Profile,
                            progress: Progress, receipt: Receipt,
                            phase: str) -> Observation | None:
    """Collect one sample without turning observer loss into launch control."""
    try:
        observation = await observer.snapshot()
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
        f'Provider did not reach {profile.minimum_running} RUNNING instances.')


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
    postgres = PostgresObserver(database_url, args.service_name, args.region)
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
                'provider': 'gcp',
                'region': scope.region,
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
    postgres = PostgresObserver(database_url, args.service_name, scope.region)
    gcp = GcpObserver(service_name=args.service_name,
                      scope=scope,
                      profile=PROFILES['scale'])
    deadline = time.monotonic() + args.timeout_seconds
    consecutive_zero = 0
    try:
        while time.monotonic() < deadline:
            census = await asyncio.to_thread(gcp.census)
            provider = parse_gcp_cleanup_state(service_name=args.service_name,
                                               instances=census.instances,
                                               disks=census.disks,
                                               operations=census.operations)
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
        'Teardown left paid database debits or scoped GCP resources.')


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
    postgres = PostgresObserver(database_url, args.service_name, args.region)
    provider_scope = postgres.provider_scope()
    if provider_scope != frozen_scope:
        postgres.close()
        raise GuardViolation(
            'Current service provider scope differs from the frozen receipt.')
    http = HttpObserver(args.endpoint, token)
    observer = Observer(postgres=postgres,
                        gcp=GcpObserver(service_name=args.service_name,
                                        scope=provider_scope,
                                        profile=profile),
                        http=http)
    progress = Progress()
    receipt = Receipt(path=pathlib.Path(args.receipt),
                      service_name=args.service_name,
                      profile=profile)
    warm_successes = 0
    pressure_successes = 0
    failure: BaseException | None = None
    run_id = f'{args.service_name}-{int(time.time())}'
    try:
        await http.prove_authentication()
        await _wait_for_baseline(observer=observer,
                                 profile=profile,
                                 progress=progress,
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
            warm = asyncio.create_task(
                send_requests(endpoint=args.endpoint,
                              token=token,
                              prefix=f'{run_id}-warm',
                              count=profile.warm_requests,
                              concurrency=profile.warm_concurrency,
                              duration_seconds=0,
                              timeout_seconds=profile.drain_timeout_seconds))
            pressure_successes, warm_successes = await asyncio.gather(
                pressure, warm)
            if pressure_successes < 1:
                raise QualificationError('No pressure request completed.')
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
    except BaseException as error:
        failure = error
        raise
    finally:
        receipt.finish(progress=progress,
                       pressure_successes=pressure_successes,
                       warm_successes=warm_successes,
                       error=failure)
        postgres.close()
    print(
        json.dumps(
            {
                'outcome': 'passed',
                'profile': profile.name,
                'peak_running': progress.peak_running,
                'warm_successes': warm_successes,
                'receipt': str(pathlib.Path(args.receipt)),
            },
            sort_keys=True))


def render_service(args: argparse.Namespace) -> None:
    profile = PROFILES[args.profile]
    source = pathlib.Path(args.source)
    config = yaml.safe_load(source.read_text(encoding='utf-8'))
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
        'max_concurrency': min(128, profile.pressure_concurrency),
    })
    resources = config['resources']
    if (resources.get('use_spot') is not True or
            resources.get('infra') != 'gcp/us-central1' or
            resources.get('instance_type') != 'g2-standard-4' or
            resources.get('accelerators') != 'L4:1'):
        raise QualificationError('Service fixture is not exact one-L4 Spot.')
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
    freeze.add_argument('--region', default='us-central1')
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
    run.add_argument('--region', default='us-central1')
    run.add_argument('--auth-token-env',
                     default='SKYPILOT_SERVE_E2E_AUTH_TOKEN')
    run.add_argument('--postgres-url-env', default='SKYPILOT_DB_CONNECTION_URI')

    cleanup = subparsers.add_parser('wait-cleanup')
    cleanup.add_argument('--service-name', required=True)
    cleanup.add_argument('--scope', required=True)
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
