"""Provider-free PostgreSQL projection of SkyServe load-balancer routes.

The readiness owner supplies endpoints and accelerator material already
resolved by its bounded probe round.  Publication performs no provider read;
the stable API performs only PostgreSQL reads and returns the existing full LB
sync response shape.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
import copy
import dataclasses
import datetime
import enum
import hashlib
import json
import math
import re
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.serve import constants
from sky.serve import demand_state_schema
from sky.serve import lb_ha
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.serve import system_recovery_route_lease
from sky.serve import system_recovery_state
from sky.utils.db import db_utils

# The load-balancer snapshot wire document remains protocol 1.  Serve051
# changes only its producer: protocol 2 persists and probes exact per-replica
# material before composing the same immutable full snapshot.
PROTOCOL_VERSION = 1
INCREMENTAL_PRODUCER_PROTOCOL_VERSION = 2
SNAPSHOT_HISTORY_LIMIT = 96
LEASE_HISTORY_PER_REPLICA_LIMIT = 4
ALIAS_RETENTION_SECONDS = 430
MAX_ALIASES_PER_RECORD = 8
MAX_ROUTE_IDENTITIES = 100_000
MAX_ROUTE_TTL_SECONDS = 24 * 60 * 60
# A promotion read runs after the bounded Kubernetes role snapshot and inside
# the external eight-second role-heartbeat budget.  Waiting briefly joins the
# PostgreSQL row-lock queue instead of repeatedly losing to a stream of short
# capacity-admission writers, while the database timeout keeps the role channel
# fail closed and bounded if a writer is genuinely stuck.
PROMOTION_OWNER_LOCK_TIMEOUT_MS = 1_000

_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_SESSION_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$')

_SERVICES = serve_state_schema.services_table
_SNAPSHOTS = route_projection_schema.serve_route_snapshots_table
_HEADS = route_projection_schema.serve_route_heads_table
_LEASES = route_projection_schema.serve_route_replica_leases_table
_DEMAND_REPORTS = demand_state_schema.serve_lb_demand_reports_table
_REPLICAS = serve_state_schema.replicas_table
_LEASE_TABLE_SESSION_CACHE_KEY = 'serve_route_lease_table_available'


class RouteProjectionError(RuntimeError):
    """Base class for route publication/read failures."""


class RouteProjectionValidationError(RouteProjectionError, ValueError):
    """A route document violates the closed projection contract."""


class RouteProjectionConflict(RouteProjectionError):
    """The requested service incarnation or owner does not match."""


class RouteProjectionUnavailable(RouteProjectionError):
    """The selected projected route source has no usable fresh snapshot."""


class RouteProjectionCorruption(RouteProjectionUnavailable):
    """Persisted route state fails its digest or shape checks."""


class RouteSourceMode(str, enum.Enum):
    LEGACY_PROXY = 'LEGACY_PROXY'
    DURABLE_PROJECTED = 'DURABLE_PROJECTED'


def use_incremental_producer(service_row: Mapping[str, Any]) -> bool:
    """Select one writer while preserving an already-projected v1 cohort."""
    try:
        mode = RouteSourceMode(service_row.get('route_source_mode'))
    except (TypeError, ValueError):
        return False
    protocol = service_row.get('route_projection_protocol_version')
    if mode == RouteSourceMode.LEGACY_PROXY:
        return True
    return protocol == INCREMENTAL_PRODUCER_PROTOCOL_VERSION


@dataclasses.dataclass(frozen=True)
class ResolvedRouteMaterial:
    """Endpoint and immutable accelerator material from one probe round."""

    url: str
    gpu_type: str
    gpu_count: int

    def __post_init__(self) -> None:
        normalized = _normalize_url(self.url)
        if normalized != self.url:
            raise RouteProjectionValidationError(
                'Resolved route URL must already be normalized.')
        if not isinstance(self.gpu_type, str) or not self.gpu_type:
            raise RouteProjectionValidationError('gpu_type must be nonempty.')
        if (type(self.gpu_count) is not int or  # pylint: disable=unidiomatic-typecheck
                self.gpu_count < 1):
            raise RouteProjectionValidationError(
                'gpu_count must be a positive integer.')


@dataclasses.dataclass(frozen=True)
class RoutePublisherIdentity:
    """Exact controller owner permitted to publish one service snapshot."""

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int
    controller_pid: int
    controller_ip: str

    def __post_init__(self) -> None:
        _nonempty(self.service_name, 'service_name')
        _nonempty(self.service_hash, 'service_hash')
        _positive_int(self.service_lifecycle_epoch, 'service_lifecycle_epoch')
        if not isinstance(self.controller_incarnation, uuid.UUID):
            raise RouteProjectionValidationError(
                'controller_incarnation must be a UUID.')
        _positive_int(self.controller_owner_epoch, 'controller_owner_epoch')
        _positive_int(self.controller_pid, 'controller_pid')
        _nonempty(self.controller_ip, 'controller_ip')


@dataclasses.dataclass(frozen=True)
class RouteLeaseMaterial:
    """Provider-resolved material consumed by the provider-free worker."""

    route: ResolvedRouteMaterial
    readiness_path: str
    probe_timeout_seconds: int
    post_data: dict[str, Any] | None
    headers: dict[str, str] | None
    async_occupancy: bool | None
    uses_logical_replicas: bool
    is_zero_cost: bool
    planned_capacity: int
    route_allowed: bool
    requires_route_marker: bool
    route_marker: system_recovery_route_lease.RouteMarker | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.route, ResolvedRouteMaterial):
            raise RouteProjectionValidationError(
                'route must be resolved route material.')
        try:
            system_recovery_route_lease.normalize_probe_url(
                self.route.url, self.readiness_path)
        except system_recovery_route_lease.RouteLeaseError as error:
            raise RouteProjectionValidationError(
                'Readiness probe path is invalid.') from error
        _positive_int(self.probe_timeout_seconds, 'probe_timeout_seconds')
        if self.probe_timeout_seconds > MAX_ROUTE_TTL_SECONDS:
            raise RouteProjectionValidationError(
                'probe_timeout_seconds exceeds the bounded maximum.')
        post_data = _validate_optional_json_object(self.post_data, 'post_data')
        headers = _validate_headers(self.headers)
        if self.async_occupancy is not None and type(  # pylint: disable=unidiomatic-typecheck
                self.async_occupancy) is not bool:
            raise RouteProjectionValidationError(
                'async_occupancy must be boolean or null.')
        for field, value in (
            ('uses_logical_replicas', self.uses_logical_replicas),
            ('is_zero_cost', self.is_zero_cost),
            ('route_allowed', self.route_allowed),
            ('requires_route_marker', self.requires_route_marker),
        ):
            if type(value) is not bool:  # pylint: disable=unidiomatic-typecheck
                raise RouteProjectionValidationError(
                    f'{field} must be boolean.')
        _positive_int(self.planned_capacity, 'planned_capacity')
        marker = _validate_route_marker(self.route_marker)
        object.__setattr__(self, 'post_data', post_data)
        object.__setattr__(self, 'headers', headers)
        object.__setattr__(self, 'route_marker', marker)


@dataclasses.dataclass(frozen=True)
class RouteLeaseProbeTarget:
    """One exact lease generation safe to probe without provider state."""

    identity: RoutePublisherIdentity
    replica_id: int
    replica_record_id: str
    service_version: int
    route_url: str
    readiness_path: str
    timeout_seconds: int
    method: str
    post_data: dict[str, Any] | None
    headers: dict[str, str] | None
    material_sha256: str
    material_generation: int
    revocation_generation: int

    def __post_init__(self) -> None:
        if not isinstance(self.identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route lease target identity is invalid.')
        _positive_int(self.replica_id, 'replica_id')
        _canonical_record_id(self.replica_record_id)
        _positive_int(self.service_version, 'service_version')
        normalized = _normalize_url(self.route_url)
        if normalized != self.route_url:
            raise RouteProjectionValidationError(
                'Route lease target URL must already be normalized.')
        try:
            system_recovery_route_lease.normalize_probe_url(
                self.route_url, self.readiness_path)
        except system_recovery_route_lease.RouteLeaseError as error:
            raise RouteProjectionValidationError(
                'Route lease target probe path is invalid.') from error
        _positive_int(self.timeout_seconds, 'timeout_seconds')
        if self.timeout_seconds > MAX_ROUTE_TTL_SECONDS:
            raise RouteProjectionValidationError(
                'Route lease target timeout exceeds the bounded maximum.')
        post_data = _validate_optional_json_object(self.post_data, 'post_data')
        headers = _validate_headers(self.headers)
        expected_method = 'POST' if post_data is not None else 'GET'
        if self.method != expected_method:
            raise RouteProjectionValidationError(
                'Route lease target method disagrees with post_data.')
        if (not isinstance(self.material_sha256, str) or
                _SHA256_RE.fullmatch(self.material_sha256) is None):
            raise RouteProjectionValidationError(
                'Route lease material digest is invalid.')
        _positive_int(self.material_generation, 'material_generation')
        _nonnegative_int(self.revocation_generation, 'revocation_generation')
        object.__setattr__(self, 'post_data', post_data)
        object.__setattr__(self, 'headers', headers)

    @property
    def probe_url(self) -> str:
        return system_recovery_route_lease.normalize_probe_url(
            self.route_url, self.readiness_path)


@dataclasses.dataclass(frozen=True)
class RouteLeaseMaterialReceipt:
    material_generation: int
    material_sha256: str
    duplicate: bool


@dataclasses.dataclass(frozen=True)
class _PreparedRouteLeaseWrite:
    replica_id: int
    replica_record_id: str
    service_version: int
    payload: dict[str, Any]
    material_sha256: str
    allow_reactivation: bool


@dataclasses.dataclass(frozen=True)
class RouteLeaseProbeReceipt:
    accepted: bool
    readiness_generation: int | None = None
    valid_until: datetime.datetime | None = None


@dataclasses.dataclass(frozen=True)
class RouteLeaseProbeResult:
    """HTTP outcome for one exact route-lease generation."""

    target: RouteLeaseProbeTarget
    succeeded: bool

    def __post_init__(self) -> None:
        if not isinstance(self.target, RouteLeaseProbeTarget):
            raise RouteProjectionValidationError(
                'Route probe result target is invalid.')
        if type(self.succeeded) is not bool:  # pylint: disable=unidiomatic-typecheck
            raise RouteProjectionValidationError(
                'Route probe result must be boolean.')


@dataclasses.dataclass(frozen=True)
class RouteBuildResult:
    """One full public response and its private exact URL identities."""

    response: dict[str, Any]
    identities: dict[str, dict[str, Any]]
    live_record_ids: set[str]
    translation_cache: dict[int, tuple[str, str, int]]


@dataclasses.dataclass(frozen=True)
class IncrementalRouteReplica:
    """Scalar current-row identity used by provider-free composition."""

    replica_id: int
    replica_record_id: str
    service_version: int
    status: str

    def __post_init__(self) -> None:
        _positive_int(self.replica_id, 'replica_id')
        _canonical_record_id(self.replica_record_id)
        _positive_int(self.service_version, 'service_version')
        try:
            serve_statuses.ReplicaStatus(self.status)
        except (TypeError, ValueError) as error:
            raise RouteProjectionValidationError(
                'Replica status is invalid.') from error

    @property
    def is_terminal(self) -> bool:
        terminal = {
            status.value
            for status in serve_statuses.ReplicaStatus.terminal_statuses()
        }
        return self.status in terminal


@dataclasses.dataclass(frozen=True)
class RoutePublicationReceipt:
    generation: int
    content_sha256: str
    duplicate: bool
    valid_until: datetime.datetime


@dataclasses.dataclass(frozen=True)
class RouteSyncDecision:
    """Exactly one response owner selected from durable service state."""

    mode: RouteSourceMode
    response: dict[str, Any] | None = None


@dataclasses.dataclass(frozen=True)
class RoutePromotionContract:
    """Exact immutable route view one load balancer may promote with."""

    service_version: int
    route_source_epoch: int
    generation: int
    content_sha256: str
    routing_urls: frozenset[str]
    async_occupancy_urls: frozenset[str]

    def __post_init__(self) -> None:
        _positive_int(self.service_version, 'service_version')
        _positive_int(self.route_source_epoch, 'route_source_epoch')
        _positive_int(self.generation, 'generation')
        if (not isinstance(self.content_sha256, str) or
                _SHA256_RE.fullmatch(self.content_sha256) is None):
            raise RouteProjectionValidationError(
                'Route promotion digest is invalid.')
        for field, urls in (('routing_urls', self.routing_urls),
                            ('async_occupancy_urls',
                             self.async_occupancy_urls)):
            if (not isinstance(urls, frozenset) or
                    len(urls) > MAX_ROUTE_IDENTITIES):
                raise RouteProjectionValidationError(
                    f'{field} must be a bounded frozen URL set.')
            if any(_normalize_url(url) != url for url in urls):
                raise RouteProjectionValidationError(
                    f'{field} contains an invalid URL.')
        if not self.async_occupancy_urls.issubset(self.routing_urls):
            raise RouteProjectionValidationError(
                'Async promotion URLs must be routable.')


@dataclasses.dataclass(frozen=True)
class RoutePromotionDecision:
    """Current durable route owner and its optional promotion contract."""

    mode: RouteSourceMode
    contract: RoutePromotionContract | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, RouteSourceMode):
            raise RouteProjectionValidationError(
                'Route promotion mode is invalid.')
        if ((self.mode == RouteSourceMode.DURABLE_PROJECTED)
                != isinstance(self.contract, RoutePromotionContract)):
            raise RouteProjectionValidationError(
                'Durable route promotion requires exactly one contract.')


def publisher_identity_from_authority(authority: Any) -> RoutePublisherIdentity:
    """Copy the generic controller authority into the narrow route fence."""
    return RoutePublisherIdentity(
        service_name=authority.service_name,
        service_hash=authority.service_hash,
        service_lifecycle_epoch=authority.service_lifecycle_epoch,
        controller_incarnation=authority.controller_incarnation,
        controller_owner_epoch=authority.controller_owner_epoch,
        controller_pid=authority.controller_pid,
        controller_ip=authority.controller_ip,
    )


def _nonempty(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise RouteProjectionValidationError(f'{field} must be nonempty.')
    return value


def _positive_int(value: object, field: str) -> int:
    if type(value) is not int or value < 1:  # pylint: disable=unidiomatic-typecheck
        raise RouteProjectionValidationError(
            f'{field} must be a positive integer.')
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if type(value) is not int or value < 0:  # pylint: disable=unidiomatic-typecheck
        raise RouteProjectionValidationError(
            f'{field} must be a nonnegative integer.')
    return value


def _normalize_url(value: object) -> str:
    if not isinstance(value, str):
        raise RouteProjectionValidationError('Route URL must be a string.')
    try:
        return system_recovery_route_lease.normalize_route_url(value)
    except system_recovery_route_lease.RouteLeaseError as error:
        raise RouteProjectionValidationError('Route URL is invalid.') from error


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(value,
                          sort_keys=True,
                          separators=(',', ':'),
                          allow_nan=False).encode('utf-8')
    except (TypeError, ValueError) as error:
        raise RouteProjectionValidationError(
            'Route projection must contain canonical JSON values.') from error


def _validate_optional_json_object(value: object,
                                   field: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RouteProjectionValidationError(f'{field} must be an object.')
    _canonical_json(value)
    return copy.deepcopy(value)


def _validate_headers(value: object) -> dict[str, str] | None:
    headers = _validate_optional_json_object(value, 'headers')
    if headers is not None and any(not isinstance(key, str) or not key or
                                   not isinstance(header_value, str)
                                   for key, header_value in headers.items()):
        raise RouteProjectionValidationError(
            'headers must map nonempty strings to strings.')
    return headers


def _validate_route_marker(
    marker: object,) -> system_recovery_route_lease.RouteMarker | None:
    if marker is None:
        return None
    if not isinstance(marker, system_recovery_route_lease.RouteMarker):
        raise RouteProjectionValidationError(
            'route_marker must use the closed recovery marker type.')
    try:
        replica_id = system_recovery_route_lease.canonical_replica_id(
            marker.replica_id)
        route_token = system_recovery_route_lease.canonical_route_token(
            marker.route_token)
    except system_recovery_route_lease.RouteLeaseError as error:
        raise RouteProjectionValidationError(
            'route_marker is invalid.') from error
    return system_recovery_route_lease.RouteMarker(replica_id, route_token)


def _route_marker_payload(
    marker: system_recovery_route_lease.RouteMarker | None,
) -> dict[str, str] | None:
    if marker is None:
        return None
    return {
        'replica_id': marker.replica_id,
        'route_token': marker.route_token,
    }


def _route_marker_from_payload(
    payload: object,) -> system_recovery_route_lease.RouteMarker | None:
    if payload is None:
        return None
    if not isinstance(payload, dict) or set(payload) != {
            'replica_id', 'route_token'
    }:
        raise RouteProjectionValidationError(
            'Persisted route marker has an invalid shape.')
    return _validate_route_marker(
        system_recovery_route_lease.RouteMarker(
            replica_id=payload['replica_id'],
            route_token=payload['route_token']))


def _lease_material_payload(material: RouteLeaseMaterial) -> dict[str, Any]:
    return {
        'route_url': material.route.url,
        'gpu_type': material.route.gpu_type,
        'gpu_count': material.route.gpu_count,
        'probe_method': 'POST' if material.post_data is not None else 'GET',
        'readiness_path': material.readiness_path,
        'probe_timeout_seconds': material.probe_timeout_seconds,
        'probe_post_data': material.post_data,
        'probe_headers': material.headers,
        'async_occupancy': material.async_occupancy,
        'uses_logical_replicas': material.uses_logical_replicas,
        'is_zero_cost': material.is_zero_cost,
        'planned_capacity': material.planned_capacity,
        'route_allowed': material.route_allowed,
        'requires_route_marker': material.requires_route_marker,
        'route_marker_payload': _route_marker_payload(material.route_marker),
    }


def _validate_lease_material_row(row: Mapping[str, Any]) -> None:
    """Validate one persisted content-addressed provider result."""
    try:
        material = RouteLeaseMaterial(
            route=ResolvedRouteMaterial(row['route_url'], row['gpu_type'],
                                        row['gpu_count']),
            readiness_path=row['readiness_path'],
            probe_timeout_seconds=row['probe_timeout_seconds'],
            post_data=row['probe_post_data'],
            headers=row['probe_headers'],
            async_occupancy=row['async_occupancy'],
            uses_logical_replicas=row['uses_logical_replicas'],
            is_zero_cost=row['is_zero_cost'],
            planned_capacity=row['planned_capacity'],
            route_allowed=row['route_allowed'],
            requires_route_marker=row['requires_route_marker'],
            route_marker=_route_marker_from_payload(
                row['route_marker_payload']))
        payload = _lease_material_payload(material)
        digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
        if (not isinstance(row['material_sha256'], str) or
                _SHA256_RE.fullmatch(row['material_sha256']) is None or
                digest != row['material_sha256']):
            raise RouteProjectionValidationError(
                'Persisted route material digest does not match.')
        expected_method = 'POST' if material.post_data is not None else 'GET'
        if row['probe_method'] != expected_method:
            raise RouteProjectionValidationError(
                'Persisted route probe method is inconsistent.')
    except (KeyError, TypeError, ValueError) as error:
        raise RouteProjectionValidationError(
            'Persisted route material is corrupt.') from error


def _prepare_route_lease_write(
        replica_info: Any,
        material: RouteLeaseMaterial) -> _PreparedRouteLeaseWrite:
    if not isinstance(material, RouteLeaseMaterial):
        raise RouteProjectionValidationError('Route lease material is invalid.')
    replica_id = _positive_int(getattr(replica_info, 'replica_id', None),
                               'replica_id')
    record_id = _canonical_record_id(
        getattr(replica_info, 'replica_record_id', None))
    service_version = _positive_int(getattr(replica_info, 'version', None),
                                    'service_version')
    payload = _lease_material_payload(material)
    digest = hashlib.sha256(_canonical_json(payload)).hexdigest()
    status = getattr(replica_info, 'status', None)
    allow_reactivation = bool(
        getattr(status, 'name', None) == serve_statuses.ReplicaStatus.READY.name
        and material.route_allowed)
    return _PreparedRouteLeaseWrite(replica_id=replica_id,
                                    replica_record_id=record_id,
                                    service_version=service_version,
                                    payload=payload,
                                    material_sha256=digest,
                                    allow_reactivation=allow_reactivation)


def _content_sha256(response: object, identities: object) -> str:
    return hashlib.sha256(
        _canonical_json({
            'response': response,
            'identities': identities,
        })).hexdigest()


def _occupancy_context_sha256(response: Mapping[str, Any],
                              identities: Mapping[str, Any]) -> str:
    """Digest only route state that can invalidate occupancy evidence.

    The full projection digest also covers volatile capacity hints.  Those
    hints can change without changing a backend or its async-occupancy
    contract, so using the full digest as the load balancer's occupancy fence
    creates a fleet-wide unknown window on every capacity-only publication.
    Persisted route identities are included here so a successor replica that
    reuses the same URL still advances this narrower fence.
    """
    return hashlib.sha256(
        _canonical_json({
            'service_version': response.get('service_version'),
            'routing_spec': response.get('routing_spec'),
            'replica_info': response.get('replica_info'),
            'identities': identities,
        })).hexdigest()


def selected_route_context_sha256(response: Mapping[str, Any],
                                  identities: Mapping[str, Any],
                                  selected_url: str) -> str:
    """Digest the immutable contract needed to bind one selected route.

    Capacity telemetry and unrelated route churn cannot change the validity of
    an otherwise identical selected worker.  The complete routing spec remains
    in the fence because it owns compatibility, queueing, and admission policy.
    """
    replica_info = response.get('replica_info')
    selected_wire = (replica_info.get(selected_url) if isinstance(
        replica_info, Mapping) else None)
    return hashlib.sha256(
        _canonical_json({
            'service_version': response.get('service_version'),
            'routing_spec': response.get('routing_spec'),
            'selected_route_url': selected_url,
            'selected_route_wire': selected_wire,
            'selected_route_identity': identities.get(selected_url),
        })).hexdigest()


def _canonical_record_id(value: object) -> str:
    if not isinstance(value, str):
        raise RouteProjectionValidationError(
            'replica_record_id must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise RouteProjectionValidationError(
            'replica_record_id must be a canonical UUID string.') from error
    if str(parsed) != value:
        raise RouteProjectionValidationError(
            'replica_record_id must be a canonical UUID string.')
    return value


def _identity_entry(info: Any, material: ResolvedRouteMaterial, *,
                    advertised: bool) -> dict[str, Any]:
    return {
        'replica_id': _positive_int(info.replica_id, 'replica_id'),
        'replica_record_id': _canonical_record_id(info.replica_record_id),
        'service_version': _positive_int(info.version, 'service_version'),
        'gpu_type': material.gpu_type,
        'gpu_count': material.gpu_count,
        'advertised': bool(advertised),
        'alias_expires_at': None,
    }


def _is_recovery_capable(info: Any) -> bool:
    return (info.system_recovery_disposition ==
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)


def build_route_view(
    replica_infos: list[Any],
    resolved_routes: Mapping[int, ResolvedRouteMaterial],
    identity_verified_replica_ids: set[int],
    active_versions: set[int],
    async_occupancy_by_version: Mapping[int, bool | None],
    *,
    service_version: int,
    routing_spec: dict[str, Any],
    capacity_hint: dict[str, Any],
    route_allowed: Callable[[Any], bool],
    marker_for_route: Callable[[Any, str],
                               system_recovery_route_lease.RouteMarker | None],
    retire_route: Callable[[Any], None],
) -> RouteBuildResult:
    """Build the existing full LB wire response from one probe result."""
    _positive_int(service_version, 'service_version')
    if not isinstance(routing_spec, dict) or not routing_spec:
        raise RouteProjectionValidationError('routing_spec must be complete.')
    if not isinstance(capacity_hint, dict):
        raise RouteProjectionValidationError('capacity_hint must be an object.')
    infos_by_id = {info.replica_id: info for info in replica_infos}
    translation_cache: dict[int, tuple[str, str, int]] = {}
    current_identities: dict[str, dict[str, Any]] = {}
    identity_sources: dict[str, list[Any]] = {}
    live_record_ids: set[str] = set()

    for info in replica_infos:
        if info.is_terminal:
            continue
        try:
            live_record_ids.add(_canonical_record_id(info.replica_record_id))
        except RouteProjectionValidationError:
            # A malformed retained row is exact positive ambiguity. Keep it
            # out of the projected identity domain without suppressing a
            # complete generation for every healthy sibling.
            continue

    for replica_id, raw_material in resolved_routes.items():
        info = infos_by_id.get(replica_id)
        if info is None or info.is_terminal:
            continue
        if not isinstance(raw_material, ResolvedRouteMaterial):
            raise RouteProjectionValidationError(
                'Resolved route material has an invalid type.')
        url = _normalize_url(raw_material.url)
        material = (raw_material
                    if url == raw_material.url else ResolvedRouteMaterial(
                        url, raw_material.gpu_type, raw_material.gpu_count))
        try:
            identity = _identity_entry(info, material, advertised=False)
        except RouteProjectionValidationError:
            continue
        translation_cache[replica_id] = (url, material.gpu_type,
                                         material.gpu_count)
        identity_sources.setdefault(url, []).append(info)
        current_identities[url] = identity

    collision_urls = {
        url for url, sources in identity_sources.items()
        if len({source.replica_record_id for source in sources}) > 1
    }
    for url in collision_urls:
        current_identities.pop(url, None)

    ready_infos = [
        info for info in replica_infos
        if (info.replica_id in identity_verified_replica_ids and
            info.replica_record_id in live_record_ids and info.status.name ==
            'READY' and info.version in active_versions and route_allowed(info))
    ]
    num_ready = len(ready_infos)
    replica_info: dict[str, dict[str, str]] = {}
    advertised_sources: dict[str, list[Any]] = {}
    for info in ready_infos:
        ready_material = resolved_routes.get(info.replica_id)
        if ready_material is None:
            continue
        url = _normalize_url(ready_material.url)
        if url not in collision_urls:
            ready_identity = current_identities.get(url)
            if (ready_identity is None or ready_identity['replica_record_id']
                    != info.replica_record_id):
                continue
        advertised_sources.setdefault(url, []).append(info)

    for url, sources in advertised_sources.items():
        if (url in collision_urls or
                len({source.replica_record_id for source in sources}) > 1):
            for source in sources:
                if _is_recovery_capable(source):
                    retire_route(source)
            replica_info[url] = {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
            current_identities.pop(url, None)
            continue
        info = sources[0]
        advertised_material = resolved_routes[info.replica_id]
        marker = marker_for_route(info,
                                  url) if _is_recovery_capable(info) else None
        if _is_recovery_capable(info) and marker is None:
            replica_info[url] = {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
            continue
        wire_info = {
            'gpu_type': advertised_material.gpu_type,
            'gpu_count': str(advertised_material.gpu_count),
        }
        if marker is not None:
            wire_info.update({
                constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION,
                constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY:
                    marker.replica_id,
                constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY: marker.route_token,
            })
        if type(info.is_zero_cost) is bool:
            wire_info['is_zero_cost'] = ('true'
                                         if info.is_zero_cost else 'false')
        async_occupancy = async_occupancy_by_version.get(info.version)
        if async_occupancy is not None:
            wire_info['async_occupancy'] = ('true'
                                            if async_occupancy else 'false')
        replica_info[url] = wire_info
        advertised_identity = current_identities.get(url)
        if advertised_identity is not None:
            advertised_identity['advertised'] = True

    response = {
        'replica_info': replica_info,
        'num_ready_replicas': num_ready,
        'routing_spec': copy.deepcopy(routing_spec),
        'capacity_hint': copy.deepcopy(capacity_hint),
        # History is acknowledged by the separate durable-demand endpoint.
        'request_history_accepted': False,
        'request_classification_history_accepted': False,
        'response_time_history_accepted': False,
        'prediction_time_history_accepted': False,
        'queued_compatibility_demand_supported': True,
        'service_version': service_version,
    }
    return RouteBuildResult(response=response,
                            identities=current_identities,
                            live_record_ids=live_record_ids,
                            translation_cache=translation_cache)


def build_incremental_route_view(
    replicas: list[IncrementalRouteReplica],
    lease_rows: list[Mapping[str, Any]],
    active_versions: set[int],
    *,
    now: datetime.datetime,
    service_version: int,
    routing_spec: dict[str, Any],
    capacity_hint: dict[str, Any],
) -> RouteBuildResult:
    """Compose the unchanged LB response from independently renewed leases."""
    _positive_int(service_version, 'service_version')
    if not isinstance(now, datetime.datetime) or now.tzinfo is None:
        raise RouteProjectionValidationError(
            'Route composition requires an aware database timestamp.')
    if (not isinstance(active_versions, set) or any(
            type(version) is not int or version < 1
            for version in active_versions)):
        raise RouteProjectionValidationError('Active versions are invalid.')
    if not isinstance(routing_spec, dict) or not routing_spec:
        raise RouteProjectionValidationError('routing_spec must be complete.')
    if not isinstance(capacity_hint, dict):
        raise RouteProjectionValidationError('capacity_hint must be an object.')

    replicas_by_key = {
        (replica.replica_id, replica.replica_record_id): replica
        for replica in replicas
    }
    if len(replicas_by_key) != len(replicas):
        raise RouteProjectionValidationError(
            'Current replica rows contain duplicate identities.')
    live_record_ids = {
        replica.replica_record_id
        for replica in replicas
        if not replica.is_terminal
    }
    current_identities: dict[str, dict[str, Any]] = {}
    identity_sources: dict[str, list[tuple[IncrementalRouteReplica,
                                           Mapping[str, Any]]]] = {}
    translation_cache: dict[int, tuple[str, str, int]] = {}

    for row in lease_rows:
        try:
            _validate_lease_material_row(row)
            record_id = _canonical_record_id(str(row['replica_record_id']))
            replica = replicas_by_key.get((int(row['replica_id']), record_id))
            if replica is None or replica.is_terminal:
                continue
            if replica.service_version != int(row['service_version']):
                continue
            material = ResolvedRouteMaterial(row['route_url'], row['gpu_type'],
                                             int(row['gpu_count']))
            url = material.url
            identity = {
                'replica_id': replica.replica_id,
                'replica_record_id': replica.replica_record_id,
                'service_version': replica.service_version,
                'gpu_type': material.gpu_type,
                'gpu_count': material.gpu_count,
                'advertised': False,
                'alias_expires_at': None,
            }
        except (KeyError, TypeError, ValueError,
                RouteProjectionValidationError):
            # One corrupt material row withholds only that exact route. Its
            # readiness writer cannot make it current because the digest
            # validator rejects the same row.
            continue
        translation_cache[replica.replica_id] = (url, material.gpu_type,
                                                 material.gpu_count)
        identity_sources.setdefault(url, []).append((replica, row))
        current_identities[url] = identity

    collision_urls = {
        url for url, sources in identity_sources.items()
        if len({source[0].replica_record_id for source in sources}) > 1
    }
    for url in collision_urls:
        current_identities.pop(url, None)

    replica_info: dict[str, dict[str, str]] = {}
    advertised_count = 0
    for url, sources in identity_sources.items():
        eligible = []
        for replica, row in sources:
            valid_until = row.get('valid_until')
            if (replica.status != serve_statuses.ReplicaStatus.READY.value or
                    replica.service_version not in active_versions or
                    row.get('route_allowed') is not True or
                    row.get('ready') is not True or
                    not isinstance(valid_until, datetime.datetime) or
                    valid_until.tzinfo is None or valid_until <= now or
                    row.get('revoked_at') is not None):
                continue
            eligible.append((replica, row))
        if not eligible:
            continue
        if url in collision_urls or len(eligible) > 1:
            replica_info[url] = {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
            continue
        replica, row = eligible[0]
        marker = _route_marker_from_payload(row['route_marker_payload'])
        if row['requires_route_marker'] is True and marker is None:
            replica_info[url] = {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
            continue
        wire_info = {
            'gpu_type': row['gpu_type'],
            'gpu_count': str(row['gpu_count']),
            'is_zero_cost': ('true' if row['is_zero_cost'] else 'false'),
        }
        if row['async_occupancy'] is not None:
            wire_info['async_occupancy'] = ('true' if row['async_occupancy']
                                            else 'false')
        if marker is not None:
            wire_info.update({
                constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION,
                constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY:
                    marker.replica_id,
                constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY: marker.route_token,
            })
        replica_info[url] = wire_info
        advertised_count += 1
        current_identity = current_identities.get(url)
        if (current_identity is not None and
                current_identity['replica_record_id']
                == replica.replica_record_id):
            current_identity['advertised'] = True

    response = {
        'replica_info': replica_info,
        'num_ready_replicas': advertised_count,
        'routing_spec': copy.deepcopy(routing_spec),
        'capacity_hint': copy.deepcopy(capacity_hint),
        'request_history_accepted': False,
        'request_classification_history_accepted': False,
        'response_time_history_accepted': False,
        'prediction_time_history_accepted': False,
        'queued_compatibility_demand_supported': True,
        'service_version': service_version,
    }
    return RouteBuildResult(response=response,
                            identities=current_identities,
                            live_record_ids=live_record_ids,
                            translation_cache=translation_cache)


def _validate_response(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise RouteProjectionValidationError(
            'Route response payload must be an object.')
    required = {
        'replica_info', 'num_ready_replicas', 'routing_spec', 'capacity_hint',
        'request_history_accepted', 'request_classification_history_accepted',
        'response_time_history_accepted', 'prediction_time_history_accepted',
        'queued_compatibility_demand_supported', 'service_version'
    }
    if set(raw) != required:
        raise RouteProjectionValidationError(
            'Route response payload has an invalid shape.')
    replica_info = raw['replica_info']
    if (not isinstance(replica_info, dict) or
            len(replica_info) > MAX_ROUTE_IDENTITIES):
        raise RouteProjectionValidationError('replica_info is invalid.')
    for url, info in replica_info.items():
        if _normalize_url(url) != url or not isinstance(info, dict):
            raise RouteProjectionValidationError('replica_info is invalid.')
    _nonnegative_int(raw['num_ready_replicas'], 'num_ready_replicas')
    if (not isinstance(raw['routing_spec'], dict) or not raw['routing_spec'] or
            not isinstance(raw['capacity_hint'], dict)):
        raise RouteProjectionValidationError(
            'Routing spec or capacity hint is invalid.')
    for field in (
            'request_history_accepted',
            'request_classification_history_accepted',
            'response_time_history_accepted',
            'prediction_time_history_accepted',
            'queued_compatibility_demand_supported',
    ):
        if type(raw[field]) is not bool:
            raise RouteProjectionValidationError(f'{field} must be boolean.')
    _positive_int(raw['service_version'], 'service_version')
    _canonical_json(raw)
    return copy.deepcopy(raw)


def _validate_identities(
    raw: object,
    *,
    require_service_version: bool = False,
) -> dict[str, dict[str, Any]]:
    """Validate private route identities.

    Snapshots written before exact async dispatch did not carry a selected
    worker version.  They remain readable for one rolling control-plane upgrade,
    but cannot be published as a new current identity set or authorize an exact
    bind.  Every newly composed identity is versioned.
    """
    if not isinstance(raw, dict) or len(raw) > MAX_ROUTE_IDENTITIES:
        raise RouteProjectionValidationError(
            'Route identity payload must be a bounded object.')
    result: dict[str, dict[str, Any]] = {}
    current_records: set[str] = set()
    for raw_url, raw_entry in raw.items():
        url = _normalize_url(raw_url)
        if url != raw_url or not isinstance(raw_entry, dict):
            raise RouteProjectionValidationError(
                'Route identity entry is invalid.')
        legacy_expected = {
            'replica_id', 'replica_record_id', 'gpu_type', 'gpu_count',
            'advertised', 'alias_expires_at'
        }
        versioned_expected = legacy_expected | {'service_version'}
        if (set(raw_entry) != versioned_expected and
            (require_service_version or set(raw_entry) != legacy_expected)):
            raise RouteProjectionValidationError(
                'Route identity entry has an invalid shape.')
        entry = dict(raw_entry)
        _positive_int(entry['replica_id'], 'replica_id')
        record_id = _canonical_record_id(entry['replica_record_id'])
        if 'service_version' in entry:
            _positive_int(entry['service_version'], 'service_version')
        _nonempty(entry['gpu_type'], 'gpu_type')
        _positive_int(entry['gpu_count'], 'gpu_count')
        if type(entry['advertised']) is not bool:
            raise RouteProjectionValidationError(
                'Route advertised marker must be boolean.')
        expires = entry['alias_expires_at']
        if expires is None:
            if record_id in current_records:
                raise RouteProjectionValidationError(
                    'A replica record has multiple current route identities.')
            current_records.add(record_id)
        elif (not isinstance(expires, (int, float)) or
              isinstance(expires, bool) or not math.isfinite(expires) or
              expires <= 0 or entry['advertised']):
            raise RouteProjectionValidationError(
                'Route alias expiry is invalid.')
        result[url] = entry
    return dict(sorted(result.items()))


def _owner_columns() -> tuple[Any, ...]:
    return (
        _SERVICES.c.name,
        _SERVICES.c.hash,
        _SERVICES.c.status,
        _SERVICES.c.pool,
        _SERVICES.c.lifecycle_epoch,
        _SERVICES.c.controller_incarnation,
        _SERVICES.c.controller_owner_epoch,
        _SERVICES.c.controller_pid,
        _SERVICES.c.controller_ip,
        _SERVICES.c.current_version,
        _SERVICES.c.active_versions,
        _SERVICES.c.route_source_mode,
        _SERVICES.c.route_source_epoch,
        _SERVICES.c.route_projection_capable,
        _SERVICES.c.route_projection_controller_incarnation,
        _SERVICES.c.route_projection_protocol_version,
        _SERVICES.c.lb_ha_enabled,
        _SERVICES.c.lb_cutover_phase,
    )


def _owner_matches(identity: RoutePublisherIdentity,
                   owner: Mapping[str, Any]) -> bool:
    return (owner.get('name') == identity.service_name and
            owner.get('hash') == identity.service_hash and
            owner.get('pool') in (0, False) and
            owner.get('lifecycle_epoch') == identity.service_lifecycle_epoch and
            owner.get('controller_incarnation')
            == identity.controller_incarnation and
            owner.get('controller_owner_epoch')
            == identity.controller_owner_epoch and
            owner.get('controller_pid') == identity.controller_pid and
            owner.get('controller_ip') == identity.controller_ip)


_ACTIONABLE_REPLICA_STATUSES = tuple(status.value for status in (
    serve_statuses.ReplicaStatus.PENDING,
    serve_statuses.ReplicaStatus.PROVISIONING,
    serve_statuses.ReplicaStatus.STARTING,
    serve_statuses.ReplicaStatus.READY,
    serve_statuses.ReplicaStatus.NOT_READY,
))
_REPLICA_STATE_SHA256 = '_replica_state_sha256'
_LEASE_SAFETY_FIELDS = (
    'service_name',
    'service_hash',
    'replica_id',
    'replica_record_id',
    'service_lifecycle_epoch',
    'controller_incarnation',
    'controller_owner_epoch',
    'controller_pid',
    'controller_ip',
    'service_version',
    'route_url',
    'gpu_type',
    'gpu_count',
    'probe_method',
    'readiness_path',
    'probe_timeout_seconds',
    'probe_post_data',
    'probe_headers',
    'async_occupancy',
    'uses_logical_replicas',
    'is_zero_cost',
    'planned_capacity',
    'route_allowed',
    'requires_route_marker',
    'route_marker_payload',
    'material_sha256',
    'material_generation',
    'ready',
    'revocation_generation',
    'revoked_at',
    'revocation_reason',
)


def _replica_state_sha256_expression() -> Any:
    """Return a compact exact PostgreSQL JSONB content fingerprint."""
    state_text = sqlalchemy.cast(_REPLICAS.c.replica_state, sqlalchemy.Text)
    state_bytes = sqlalchemy.func.convert_to(state_text, 'UTF8')
    return sqlalchemy.func.encode(sqlalchemy.func.sha256(state_bytes),
                                  'hex').label(_REPLICA_STATE_SHA256)


def _incremental_replica_query(service_name: str, *, include_state: bool,
                               for_update: bool) -> sqlalchemy.Select:
    columns = [
        _REPLICAS.c.replica_id,
        _REPLICAS.c.replica_state_version,
        _REPLICAS.c.status,
        _REPLICAS.c.version,
        _replica_state_sha256_expression(),
    ]
    if include_state:
        columns.insert(2, _REPLICAS.c.replica_state)
    query = sqlalchemy.select(*columns).where(
        _REPLICAS.c.service_name == service_name,
        _REPLICAS.c.status.in_(_ACTIONABLE_REPLICA_STATUSES),
    ).order_by(_REPLICAS.c.replica_id)
    return query.with_for_update() if for_update else query


def _incremental_lease_query(identity: RoutePublisherIdentity, *,
                             for_update: bool) -> sqlalchemy.Select:
    query = sqlalchemy.select(_LEASES).where(
        _LEASES.c.service_name == identity.service_name,
        _LEASES.c.service_hash == identity.service_hash,
        _LEASES.c.service_lifecycle_epoch == identity.service_lifecycle_epoch,
        _LEASES.c.controller_incarnation == identity.controller_incarnation,
        _LEASES.c.controller_owner_epoch == identity.controller_owner_epoch,
        _LEASES.c.controller_pid == identity.controller_pid,
        _LEASES.c.controller_ip == identity.controller_ip,
        _LEASES.c.revoked_at.is_(None),
    ).order_by(_LEASES.c.replica_id, _LEASES.c.replica_record_id)
    return query.with_for_update() if for_update else query


def _replica_fingerprints(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[tuple[int, int, str, str, int], ...]:
    fingerprints = []
    for row in rows:
        try:
            digest = row[_REPLICA_STATE_SHA256]
            if (not isinstance(digest, str) or
                    _SHA256_RE.fullmatch(digest) is None):
                raise ValueError('invalid replica state digest')
            fingerprints.append(
                (int(row['replica_id']), int(row['replica_state_version']),
                 digest, str(row['status']), int(row['version'])))
        except (KeyError, TypeError, ValueError) as error:
            raise RouteProjectionCorruption(
                'A current replica fingerprint is corrupt.') from error
    if len({fingerprint[0] for fingerprint in fingerprints
           }) != len(fingerprints):
        raise RouteProjectionCorruption(
            'Current replica fingerprints contain duplicate IDs.')
    return tuple(fingerprints)


def _lease_key(row: Mapping[str, Any]) -> tuple[int, str]:
    try:
        return int(row['replica_id']), str(row['replica_record_id'])
    except (KeyError, TypeError, ValueError) as error:
        raise RouteProjectionCorruption(
            'A current route lease identity is corrupt.') from error


def _lease_safety_snapshot(
    rows: Sequence[Mapping[str,
                           Any]],) -> dict[tuple[int, str], tuple[Any, ...]]:
    snapshot: dict[tuple[int, str], tuple[Any, ...]] = {}
    for row in rows:
        key = _lease_key(row)
        if key in snapshot:
            raise RouteProjectionCorruption(
                'Current route leases contain duplicate identities.')
        try:
            snapshot[key] = tuple(row[field] for field in _LEASE_SAFETY_FIELDS)
        except KeyError as error:
            raise RouteProjectionCorruption(
                'A current route lease is missing safety state.') from error
    return snapshot


def _validate_revalidated_leases(
    prepared_rows: Sequence[Mapping[str, Any]],
    current_rows: Sequence[Mapping[str, Any]],
) -> None:
    """Accept only identical safety state and monotonic lease extension."""
    if _lease_safety_snapshot(prepared_rows) != _lease_safety_snapshot(
            current_rows):
        raise RouteProjectionConflict(
            'Route lease safety state changed during composition.')
    prepared_by_key = {_lease_key(row): row for row in prepared_rows}
    for current in current_rows:
        prepared = prepared_by_key[_lease_key(current)]
        prepared_until = prepared.get('valid_until')
        current_until = current.get('valid_until')
        if prepared_until is None:
            continue
        if (not isinstance(prepared_until, datetime.datetime) or
                prepared_until.tzinfo is None or
                not isinstance(current_until, datetime.datetime) or
                current_until.tzinfo is None or current_until < prepared_until):
            raise RouteProjectionConflict(
                'Route lease validity regressed during composition.')


def _lease_freshness_snapshot(
    rows: Sequence[Mapping[str, Any]],
    now: datetime.datetime,
) -> dict[tuple[int, str], bool]:
    """Return the only time-dependent input to route eligibility."""
    return {
        _lease_key(row): (
            isinstance(row.get('valid_until'), datetime.datetime) and
            row['valid_until'].tzinfo is not None and
            row['valid_until'] > now) for row in rows
    }


def _active_versions_from_owner(owner: Mapping[str, Any]) -> set[int]:
    try:
        raw_active_versions = owner['active_versions']
        active_versions = set(
            json.loads(raw_active_versions) if raw_active_versions else [])
        if any(
                type(version) is not int or version < 1
                for version in active_versions):
            raise ValueError('invalid active version')
        return active_versions
    except (KeyError, TypeError, ValueError) as error:
        raise RouteProjectionCorruption(
            'Service active versions are corrupt.') from error


def _route_probe_target_from_row(
        identity: RoutePublisherIdentity,
        row: Mapping[str, Any]) -> RouteLeaseProbeTarget:
    _validate_lease_material_row(row)
    return RouteLeaseProbeTarget(
        identity=identity,
        replica_id=int(row['replica_id']),
        replica_record_id=str(row['replica_record_id']),
        service_version=int(row['service_version']),
        route_url=row['route_url'],
        readiness_path=row['readiness_path'],
        timeout_seconds=int(row['probe_timeout_seconds']),
        method=row['probe_method'],
        post_data=row['probe_post_data'],
        headers=row['probe_headers'],
        material_sha256=row['material_sha256'],
        material_generation=int(row['material_generation']),
        revocation_generation=int(row['revocation_generation']),
    )


def revoke_replica_lease_in_session(
    session: orm.Session | sqlalchemy.engine.Connection,
    service_name: str,
    replica_id: int,
    replica_record_id: str,
    reason: str,
) -> int:
    """Revoke an exact replica's route material in its state transaction.

    This narrow write boundary deliberately performs no provider read and no
    commit.  Callers already mutating the replica row own the transaction.
    Missing pre-Serve051 material is the idempotent zero-row result.
    """
    _nonempty(service_name, 'service_name')
    # Historical rows may use replica ID 0 as a non-routable sentinel.  No
    # Serve051 lease can represent it because the lease schema requires a
    # positive ID, so revocation is already complete.
    if type(replica_id) is int and replica_id <= 0:  # pylint: disable=unidiomatic-typecheck
        return 0
    _positive_int(replica_id, 'replica_id')
    record_id = uuid.UUID(_canonical_record_id(replica_record_id))
    _nonempty(reason, 'reason')
    if not _route_lease_table_available(session):
        return 0
    now = sqlalchemy.func.clock_timestamp()
    result = session.execute(
        sqlalchemy.update(_LEASES).where(
            _LEASES.c.service_name == service_name,
            _LEASES.c.replica_id == replica_id,
            _LEASES.c.replica_record_id == record_id,
            _LEASES.c.revoked_at.is_(None),
        ).values(
            ready=False,
            observed_at=None,
            valid_until=None,
            revocation_generation=_LEASES.c.revocation_generation + 1,
            revoked_at=now,
            revocation_reason=reason,
        ))
    return int(result.rowcount or 0)


def revoke_service_leases_in_session(
    session: orm.Session | sqlalchemy.engine.Connection,
    service_name: str,
    reason: str,
    *,
    active_versions: set[int] | None = None,
) -> int:
    """Revoke all or retired-version leases in a service-row transaction."""
    _nonempty(service_name, 'service_name')
    _nonempty(reason, 'reason')
    if not _route_lease_table_available(session):
        return 0
    predicates = [
        _LEASES.c.service_name == service_name,
        _LEASES.c.revoked_at.is_(None),
    ]
    if active_versions is not None:
        if any(
                type(version) is not int or version < 1
                for version in active_versions):
            raise RouteProjectionValidationError(
                'Active route versions are invalid.')
        if active_versions:
            predicates.append(
                _LEASES.c.service_version.not_in(sorted(active_versions)))
    result = session.execute(
        sqlalchemy.update(_LEASES).where(*predicates).values(
            ready=False,
            observed_at=None,
            valid_until=None,
            revocation_generation=_LEASES.c.revocation_generation + 1,
            revoked_at=sqlalchemy.func.clock_timestamp(),
            revocation_reason=reason,
        ))
    return int(result.rowcount or 0)


def _route_lease_table_available(
    session: orm.Session | sqlalchemy.engine.Connection,) -> bool:
    """Return whether the additive Serve051 lease schema is installed.

    Replica and service state mutations are also exercised against historical
    migration revisions.  Before Serve051 there is no route lease that could
    survive a state transition, so the revocation hook is already satisfied.
    Checking before issuing DML is required because PostgreSQL marks the whole
    transaction failed after an undefined-table error.
    """
    if isinstance(session, orm.Session):
        cached = session.info.get(_LEASE_TABLE_SESSION_CACHE_KEY)
        if cached is not None:
            return bool(cached)
        connection = session.connection()
    else:
        connection = session
    available = sqlalchemy.inspect(connection).has_table(_LEASES.name)
    if isinstance(session, orm.Session):
        # Cache only for this short-lived state transaction.  An engine-level
        # cache would become stale while migration tests replace the schema.
        session.info[_LEASE_TABLE_SESSION_CACHE_KEY] = available
    return available


def snapshot_owner_matches(snapshot: Mapping[str, Any],
                           owner: Mapping[str, Any]) -> bool:
    """Return whether one durable snapshot belongs to the current owner."""
    return (snapshot.get('service_hash') == owner.get('hash') and
            snapshot.get('service_lifecycle_epoch')
            == owner.get('lifecycle_epoch') and
            snapshot.get('controller_incarnation')
            == owner.get('controller_incarnation') and
            snapshot.get('controller_owner_epoch')
            == owner.get('controller_owner_epoch') and
            snapshot.get('controller_pid') == owner.get('controller_pid') and
            snapshot.get('controller_ip') == owner.get('controller_ip') and
            snapshot.get('service_version') == owner.get('current_version'))


def current_promotion_contract_matches_in_session(
    session: orm.Session,
    owner: Mapping[str, Any],
    contract: RoutePromotionContract | None,
) -> bool:
    """Revalidate one already-decoded contract after locking its owner row.

    Snapshot generations are immutable and content-addressed.  The expensive
    payload validation therefore happens in ``resolve_promotion_contract``;
    the cutover CAS only needs to prove that the same fresh immutable row is
    still the current head under the service-row lock.
    """
    if not isinstance(contract, RoutePromotionContract):
        return False
    try:
        mode = RouteSourceMode(owner.get('route_source_mode'))
    except (TypeError, ValueError):
        return False
    if (mode != RouteSourceMode.DURABLE_PROJECTED or
            owner.get('pool') not in (0, False) or
            owner.get('current_version') != contract.service_version or
            owner.get('route_source_epoch') != contract.route_source_epoch or
            owner.get('route_projection_capable') is not True or
            owner.get('route_projection_controller_incarnation')
            != owner.get('controller_incarnation') or
            owner.get('route_projection_protocol_version')
            not in (PROTOCOL_VERSION, INCREMENTAL_PRODUCER_PROTOCOL_VERSION)):
        return False
    now = session.execute(sqlalchemy.select(
        sqlalchemy.func.clock_timestamp())).scalar_one()
    head = session.execute(
        sqlalchemy.select(_HEADS).where(_HEADS.c.service_name == owner.get(
            'name'))).mappings().one_or_none()
    if (head is None or head['valid_until'] <= now or
            head['generation'] != contract.generation):
        return False
    snapshot = session.execute(
        sqlalchemy.select(_SNAPSHOTS).where(
            _SNAPSHOTS.c.service_name == owner.get('name'),
            _SNAPSHOTS.c.generation ==
            contract.generation)).mappings().one_or_none()
    return bool(snapshot is not None and
                snapshot['content_sha256'] == contract.content_sha256 and
                snapshot_owner_matches(snapshot, owner) and
                snapshot['producer_protocol_version']
                == owner.get('route_projection_protocol_version'))


def _merge_aliases(current: dict[str, dict[str, Any]],
                   previous: dict[str, dict[str, Any]] | None,
                   current_record_ids: set[str],
                   now: datetime.datetime) -> dict[str, dict[str, Any]]:
    """Retain fixed-lifetime translations for exact still-live records."""
    result = copy.deepcopy(current)
    if previous is None:
        return _validate_identities(result)
    current_url_by_record = {
        entry['replica_record_id']: url
        for url, entry in current.items()
        if entry['alias_expires_at'] is None
    }
    aliases_by_record: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    now_timestamp = now.timestamp()
    for url, prior in previous.items():
        record_id = prior['replica_record_id']
        if (record_id not in current_record_ids or
                current_url_by_record.get(record_id) == url):
            continue
        alias = copy.deepcopy(prior)
        alias['advertised'] = False
        expires = alias.get('alias_expires_at')
        if expires is None:
            expires = now_timestamp + ALIAS_RETENTION_SECONDS
            alias['alias_expires_at'] = expires
        if expires <= now_timestamp:
            continue
        aliases_by_record.setdefault(record_id, []).append((url, alias))
    for aliases in aliases_by_record.values():
        aliases.sort(key=lambda item: (item[1]['alias_expires_at'], item[0]),
                     reverse=True)
        for url, alias in aliases[:MAX_ALIASES_PER_RECORD]:
            existing = result.get(url)
            if (existing is not None and existing['replica_record_id']
                    != alias['replica_record_id']):
                continue
            result.setdefault(url, alias)
    return _validate_identities(result)


class RouteProjectionRepository:
    """Owner-fenced storage and provider-free route reads."""

    def __init__(self, engine: sqlalchemy.engine.Engine | None = None) -> None:
        self._engine = engine

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        engine = self._engine or serve_state_schema.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RouteProjectionUnavailable(
                'Route projection requires consolidated PostgreSQL state.')
        return engine

    @staticmethod
    def _owner_query(service_name: str,
                     *,
                     for_update: bool = False) -> sqlalchemy.Select:
        query = sqlalchemy.select(*_owner_columns()).where(
            _SERVICES.c.name == service_name)
        return query.with_for_update() if for_update else query

    def current_owner_uses_incremental_producer(
            self, identity: RoutePublisherIdentity) -> bool:
        """Select the one route writer from exact current-schema authority."""
        if not isinstance(identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route publisher identity is invalid.')
        try:
            with orm.Session(self.engine) as session, session.begin():
                owner = session.execute(self._owner_query(
                    identity.service_name)).mappings().one_or_none()
                if owner is None or not _owner_matches(identity, owner):
                    raise RouteProjectionConflict(
                        'Route producer no longer owns this service.')
                return use_incremental_producer(owner)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route producer selection failed.') from error

    @staticmethod
    def validate_snapshot_row(
        row: Mapping[str,
                     Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        """Validate and decode one persisted content-addressed snapshot."""
        try:
            if row['protocol_version'] != PROTOCOL_VERSION:
                raise RouteProjectionValidationError(
                    'Route snapshot protocol is unsupported.')
            if row['producer_protocol_version'] not in (
                    PROTOCOL_VERSION, INCREMENTAL_PRODUCER_PROTOCOL_VERSION):
                raise RouteProjectionValidationError(
                    'Route snapshot producer protocol is unsupported.')
            response = _validate_response(row['response_payload'])
            identities = _validate_identities(row['identity_payload'])
            digest = _content_sha256(response, identities)
            if (not isinstance(row['content_sha256'], str) or
                    _SHA256_RE.fullmatch(row['content_sha256']) is None or
                    digest != row['content_sha256']):
                raise RouteProjectionValidationError(
                    'Route snapshot digest does not match its payload.')
            if response['service_version'] != row['service_version']:
                raise RouteProjectionValidationError(
                    'Route snapshot version disagrees with its response.')
            advertised = {
                url for url, entry in identities.items() if entry['advertised']
            }
            route_urls = {
                url for url, info in response['replica_info'].items()
                if constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY not in info
            }
            if not route_urls.issubset(advertised):
                raise RouteProjectionValidationError(
                    'Advertised routes have no exact private identity.')
            return response, identities
        except (KeyError, TypeError, ValueError,
                RouteProjectionValidationError) as error:
            raise RouteProjectionCorruption(
                'Persisted route snapshot is corrupt.') from error

    def upsert_replica_material(
        self,
        identity: RoutePublisherIdentity,
        replica_info: Any,
        material: RouteLeaseMaterial,
    ) -> RouteLeaseMaterialReceipt:
        """Persist one provider-resolved target without publishing routes."""
        receipts = self.upsert_replica_materials(identity,
                                                 [(replica_info, material)])
        if not receipts:
            raise RouteProjectionConflict(
                'Route material replica identity is no longer current or its '
                'lease was revoked and cannot be implicitly revived.')
        return receipts[0]

    def upsert_replica_materials(
        self,
        identity: RoutePublisherIdentity,
        entries: list[tuple[Any, RouteLeaseMaterial]],
    ) -> list[RouteLeaseMaterialReceipt]:
        """Persist one bounded provider result in one owner-fenced commit."""
        if not isinstance(identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route publisher identity is invalid.')
        if (not isinstance(entries, list) or
                len(entries) > constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS):
            raise RouteProjectionValidationError(
                'Route material batch is invalid or unbounded.')
        prepared = sorted((_prepare_route_lease_write(info, material)
                           for info, material in entries),
                          key=lambda item: item.replica_id)
        replica_ids = [item.replica_id for item in prepared]
        if len(replica_ids) != len(set(replica_ids)):
            raise RouteProjectionValidationError(
                'Route material batch has duplicate replica IDs.')
        if not prepared:
            return []

        try:
            with orm.Session(self.engine) as session, session.begin():
                owner = session.execute(
                    self._owner_query(identity.service_name, for_update=True).
                    add_columns(sqlalchemy.func.clock_timestamp().label(
                        'route_material_now'))).mappings().one_or_none()
                if owner is None or not _owner_matches(identity, owner):
                    raise RouteProjectionConflict(
                        'Route material writer no longer owns this service.')
                now = owner['route_material_now']
                replica_rows = session.execute(
                    sqlalchemy.select(
                        _REPLICAS.c.replica_id,
                        _REPLICAS.c.version,
                        _REPLICAS.c.status,
                        _REPLICAS.c.replica_state['replica_record_id'].
                        as_string().label('replica_record_id'),
                    ).where(_REPLICAS.c.service_name == identity.service_name,
                            _REPLICAS.c.replica_id.in_(replica_ids)).order_by(
                                _REPLICAS.c.replica_id).with_for_update()
                ).mappings().all()
                replica_rows_by_id = {
                    int(row['replica_id']): row for row in replica_rows
                }
                current = []
                for item in prepared:
                    row = replica_rows_by_id.get(item.replica_id)
                    if (row is not None and
                            row['version'] == item.service_version and
                            row['status']
                            == serve_statuses.ReplicaStatus.READY.value and
                            row['replica_record_id'] == item.replica_record_id):
                        current.append(item)
                if not current:
                    return []

                current_replica_ids = [item.replica_id for item in current]
                history_rows = session.execute(
                    sqlalchemy.select(_LEASES).where(
                        _LEASES.c.service_name == identity.service_name,
                        _LEASES.c.replica_id.in_(current_replica_ids)).order_by(
                            _LEASES.c.replica_id,
                            _LEASES.c.material_generation.desc(),
                            _LEASES.c.created_at.desc()).with_for_update()
                ).mappings().all()
                history_by_replica: dict[int, list[Mapping[str, Any]]] = {}
                existing_by_key: dict[tuple[int, str], Mapping[str, Any]] = {}
                for row in history_rows:
                    row_replica_id = int(row['replica_id'])
                    history_by_replica.setdefault(row_replica_id,
                                                  []).append(row)
                    if row['service_hash'] == identity.service_hash:
                        existing_by_key[(row_replica_id,
                                         str(row['replica_record_id']))] = row

                current_identities = sqlalchemy.values(
                    sqlalchemy.column('replica_id', sqlalchemy.Integer),
                    sqlalchemy.column('replica_record_id',
                                      sqlalchemy.Uuid(as_uuid=True)),
                    name='current_route_material').data([
                        (item.replica_id, uuid.UUID(item.replica_record_id))
                        for item in current
                    ])
                session.execute(
                    sqlalchemy.update(_LEASES).where(
                        _LEASES.c.service_name == identity.service_name,
                        _LEASES.c.service_hash == identity.service_hash,
                        _LEASES.c.replica_id == current_identities.c.replica_id,
                        _LEASES.c.replica_record_id
                        != current_identities.c.replica_record_id,
                        _LEASES.c.revoked_at.is_(None),
                    ).values(
                        ready=False,
                        observed_at=None,
                        valid_until=None,
                        revocation_generation=_LEASES.c.revocation_generation +
                        1,
                        revoked_at=now,
                        revocation_reason='replica_record_replaced',
                    ))

                upsert_values: list[dict[str, Any]] = []
                receipts: list[RouteLeaseMaterialReceipt] = []
                for item in current:
                    key = (item.replica_id, item.replica_record_id)
                    existing = existing_by_key.get(key)
                    if (existing is not None and
                            existing['revoked_at'] is not None and
                            not item.allow_reactivation):
                        # A revoked/stale sibling is isolated from this one
                        # complete provider-resolution batch.
                        continue
                    duplicate = bool(
                        existing is not None and
                        existing['revoked_at'] is None and
                        existing['material_sha256'] == item.material_sha256)
                    owner_changed = bool(
                        duplicate and existing is not None and
                        not (existing['service_lifecycle_epoch']
                             == identity.service_lifecycle_epoch and
                             existing['controller_incarnation']
                             == identity.controller_incarnation and
                             existing['controller_owner_epoch']
                             == identity.controller_owner_epoch and
                             existing['controller_pid']
                             == identity.controller_pid and
                             existing['controller_ip']
                             == identity.controller_ip))
                    maximum = max((
                        int(row['material_generation'])
                        for row in history_by_replica.get(item.replica_id, [])),
                                  default=0)
                    generation = (int(existing['material_generation'])
                                  if duplicate and existing is not None else
                                  maximum + 1)
                    preserve_readiness = bool(duplicate and not owner_changed)
                    upsert_values.append(
                        dict(
                            service_name=identity.service_name,
                            service_hash=identity.service_hash,
                            replica_id=item.replica_id,
                            replica_record_id=uuid.UUID(item.replica_record_id),
                            service_lifecycle_epoch=identity.
                            service_lifecycle_epoch,
                            controller_incarnation=identity.
                            controller_incarnation,
                            controller_owner_epoch=identity.
                            controller_owner_epoch,
                            controller_pid=identity.controller_pid,
                            controller_ip=identity.controller_ip,
                            service_version=item.service_version,
                            material_sha256=item.material_sha256,
                            material_generation=generation,
                            readiness_generation=(
                                int(existing['readiness_generation'])
                                if duplicate and existing is not None else 0),
                            ready=(bool(existing['ready'])
                                   if preserve_readiness and
                                   existing is not None else False),
                            created_at=(existing['created_at']
                                        if existing is not None else now),
                            resolved_at=now,
                            observed_at=(existing['observed_at']
                                         if preserve_readiness and
                                         existing is not None else None),
                            valid_until=(existing['valid_until']
                                         if preserve_readiness and
                                         existing is not None else None),
                            revocation_generation=(int(
                                existing['revocation_generation']) if existing
                                                   is not None else 0),
                            revoked_at=None,
                            revocation_reason=None,
                            **item.payload,
                        ))
                    receipts.append(
                        RouteLeaseMaterialReceipt(
                            material_generation=generation,
                            material_sha256=item.material_sha256,
                            duplicate=duplicate))

                if upsert_values:
                    insert = postgresql.insert(_LEASES).values(upsert_values)
                    primary_key_columns = {
                        'service_name', 'service_hash', 'replica_id',
                        'replica_record_id'
                    }
                    session.execute(
                        insert.on_conflict_do_update(
                            index_elements=[
                                _LEASES.c.service_name,
                                _LEASES.c.service_hash,
                                _LEASES.c.replica_id,
                                _LEASES.c.replica_record_id,
                            ],
                            set_={
                                column.name: insert.excluded[column.name]
                                for column in _LEASES.c
                                if column.name not in primary_key_columns |
                                {'created_at'}
                            }))

                    ranked_history = sqlalchemy.select(
                        _LEASES.c.service_hash, _LEASES.c.replica_id,
                        _LEASES.c.replica_record_id,
                        sqlalchemy.func.row_number().over(
                            partition_by=(_LEASES.c.service_name,
                                          _LEASES.c.replica_id),
                            order_by=(_LEASES.c.material_generation.desc(),
                                      _LEASES.c.created_at.desc())).
                        label('history_rank')).where(
                            _LEASES.c.service_name == identity.service_name,
                            _LEASES.c.replica_id.in_([
                                value['replica_id'] for value in upsert_values
                            ])).subquery()
                    stale_history = sqlalchemy.select(
                        ranked_history.c.service_hash,
                        ranked_history.c.replica_id,
                        ranked_history.c.replica_record_id).where(
                            ranked_history.c.history_rank >
                            LEASE_HISTORY_PER_REPLICA_LIMIT)
                    session.execute(
                        sqlalchemy.delete(_LEASES).where(
                            _LEASES.c.service_name == identity.service_name,
                            sqlalchemy.tuple_(_LEASES.c.service_hash,
                                              _LEASES.c.replica_id,
                                              _LEASES.c.replica_record_id).in_(
                                                  stale_history)))
                return receipts
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route material persistence failed.') from error

    def list_probe_targets(
        self,
        identity: RoutePublisherIdentity,
    ) -> list[RouteLeaseProbeTarget]:
        """Read current unrevoked targets for provider-free HTTP probes."""
        if not isinstance(identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route publisher identity is invalid.')
        try:
            with orm.Session(self.engine) as session, session.begin():
                owner = session.execute(self._owner_query(
                    identity.service_name)).mappings().one_or_none()
                if owner is None or not _owner_matches(identity, owner):
                    raise RouteProjectionConflict(
                        'Route probe worker no longer owns this service.')
                current_replica = _LEASES.join(
                    _REPLICAS,
                    sqlalchemy.and_(
                        _REPLICAS.c.service_name == _LEASES.c.service_name,
                        _REPLICAS.c.replica_id == _LEASES.c.replica_id,
                        _REPLICAS.c.version == _LEASES.c.service_version,
                        _REPLICAS.c.status ==
                        serve_statuses.ReplicaStatus.READY.value,
                        _REPLICAS.c.replica_state['replica_record_id'].
                        as_string() == sqlalchemy.cast(
                            _LEASES.c.replica_record_id, sqlalchemy.Text)))
                rows = session.execute(
                    sqlalchemy.select(_LEASES).select_from(
                        current_replica).where(
                            _LEASES.c.service_name == identity.service_name,
                            _LEASES.c.service_hash == identity.service_hash,
                            _LEASES.c.service_lifecycle_epoch ==
                            identity.service_lifecycle_epoch,
                            _LEASES.c.controller_incarnation ==
                            identity.controller_incarnation,
                            _LEASES.c.controller_owner_epoch ==
                            identity.controller_owner_epoch,
                            _LEASES.c.controller_pid == identity.controller_pid,
                            _LEASES.c.controller_ip == identity.controller_ip,
                            _LEASES.c.route_allowed.is_(True),
                            _LEASES.c.revoked_at.is_(None),
                        ).order_by(
                            _LEASES.c.replica_id,
                            _LEASES.c.replica_record_id)).mappings().all()
                targets: list[RouteLeaseProbeTarget] = []
                for row in rows:
                    try:
                        targets.append(
                            _route_probe_target_from_row(identity, row))
                    except RouteProjectionValidationError:
                        # One poisoned row must not stall healthy siblings.
                        continue
                return targets
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route probe target read failed.') from error

    def record_probe_result(
        self,
        target: RouteLeaseProbeTarget,
        succeeded: bool,
        *,
        ttl_seconds: int,
    ) -> RouteLeaseProbeReceipt:
        """Apply an HTTP result only to its exact unrevoked generation."""
        result = RouteLeaseProbeResult(target=target, succeeded=succeeded)
        return self.record_probe_results([result], ttl_seconds=ttl_seconds)[0]

    def record_probe_results(
        self,
        results: Sequence[RouteLeaseProbeResult],
        *,
        ttl_seconds: int,
    ) -> list[RouteLeaseProbeReceipt]:
        """Persist one bounded exact-generation result batch atomically."""
        if (type(ttl_seconds) is not int or  # pylint: disable=unidiomatic-typecheck
                not 1 <= ttl_seconds <= MAX_ROUTE_TTL_SECONDS):
            raise RouteProjectionValidationError('Route lease TTL is invalid.')
        if not isinstance(results, Sequence):
            raise RouteProjectionValidationError(
                'Route probe results must be a sequence.')
        if len(results) > constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS:
            raise RouteProjectionValidationError(
                'Route probe result batch exceeds the bounded maximum.')
        if not results:
            return []
        validated_results: list[RouteLeaseProbeResult] = []
        for result in results:
            if not isinstance(result, RouteLeaseProbeResult):
                raise RouteProjectionValidationError(
                    'Route probe result is invalid.')
            validated_results.append(result)
        identity = validated_results[0].target.identity
        if any(result.target.identity != identity
               for result in validated_results[1:]):
            raise RouteProjectionValidationError(
                'Route probe result batch mixes publisher identities.')
        target_keys = [(result.target.replica_id,
                        result.target.replica_record_id)
                       for result in validated_results]
        if len(set(target_keys)) != len(target_keys):
            raise RouteProjectionValidationError(
                'Route probe result batch contains a duplicate target.')
        try:
            with orm.Session(self.engine) as session, session.begin():
                now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                valid_until = now + datetime.timedelta(seconds=ttl_seconds)
                input_rows = sqlalchemy.values(
                    sqlalchemy.column('replica_id', sqlalchemy.Integer),
                    sqlalchemy.column('replica_record_id',
                                      sqlalchemy.Uuid(as_uuid=True)),
                    sqlalchemy.column('service_version', sqlalchemy.Integer),
                    sqlalchemy.column('material_sha256', sqlalchemy.Text),
                    sqlalchemy.column('material_generation',
                                      sqlalchemy.BigInteger),
                    sqlalchemy.column('revocation_generation',
                                      sqlalchemy.BigInteger),
                    sqlalchemy.column('succeeded', sqlalchemy.Boolean),
                    name='probe_results').data([
                        (result.target.replica_id,
                         uuid.UUID(result.target.replica_record_id),
                         result.target.service_version,
                         result.target.material_sha256,
                         result.target.material_generation,
                         result.target.revocation_generation, result.succeeded)
                        for result in validated_results
                    ])
                owner_is_current = sqlalchemy.exists(
                    sqlalchemy.select(1).where(
                        _SERVICES.c.name == identity.service_name,
                        _SERVICES.c.hash == identity.service_hash,
                        _SERVICES.c.pool == 0, _SERVICES.c.lifecycle_epoch ==
                        identity.service_lifecycle_epoch,
                        _SERVICES.c.controller_incarnation ==
                        identity.controller_incarnation,
                        _SERVICES.c.controller_owner_epoch ==
                        identity.controller_owner_epoch,
                        _SERVICES.c.controller_pid == identity.controller_pid,
                        _SERVICES.c.controller_ip == identity.controller_ip))
                replica_is_current = sqlalchemy.exists(
                    sqlalchemy.select(1).where(
                        _REPLICAS.c.service_name == identity.service_name,
                        _REPLICAS.c.replica_id == input_rows.c.replica_id,
                        _REPLICAS.c.version == input_rows.c.service_version,
                        _REPLICAS.c.status ==
                        serve_statuses.ReplicaStatus.READY.value,
                        _REPLICAS.c.replica_state['replica_record_id'].
                        as_string() == sqlalchemy.cast(
                            input_rows.c.replica_record_id, sqlalchemy.Text)))
                updated = session.execute(
                    sqlalchemy.update(_LEASES).where(
                        _LEASES.c.service_name == identity.service_name,
                        _LEASES.c.service_hash == identity.service_hash,
                        _LEASES.c.replica_id == input_rows.c.replica_id,
                        _LEASES.c.replica_record_id ==
                        input_rows.c.replica_record_id,
                        _LEASES.c.service_lifecycle_epoch ==
                        identity.service_lifecycle_epoch,
                        _LEASES.c.controller_incarnation ==
                        identity.controller_incarnation,
                        _LEASES.c.controller_owner_epoch ==
                        identity.controller_owner_epoch,
                        _LEASES.c.controller_pid == identity.controller_pid,
                        _LEASES.c.controller_ip == identity.controller_ip,
                        _LEASES.c.service_version ==
                        input_rows.c.service_version,
                        _LEASES.c.route_allowed.is_(True), _LEASES.c.
                        material_sha256 == input_rows.c.material_sha256,
                        _LEASES.c.material_generation == input_rows.c.
                        material_generation, _LEASES.c.revocation_generation ==
                        input_rows.c.revocation_generation,
                        _LEASES.c.revoked_at.is_(None), owner_is_current,
                        replica_is_current).values(
                            readiness_generation=_LEASES.c.readiness_generation
                            + 1,
                            ready=input_rows.c.succeeded,
                            observed_at=now,
                            valid_until=valid_until).returning(
                                _LEASES.c.replica_id,
                                _LEASES.c.replica_record_id,
                                _LEASES.c.readiness_generation,
                                _LEASES.c.valid_until)).mappings().all()
                accepted = {
                    (int(row['replica_id']), str(row['replica_record_id'])): row
                    for row in updated
                }
                receipts: list[RouteLeaseProbeReceipt] = []
                for result in validated_results:
                    row = accepted.get((result.target.replica_id,
                                        result.target.replica_record_id))
                    receipts.append(
                        RouteLeaseProbeReceipt(
                            accepted=row is not None,
                            readiness_generation=(int(
                                row['readiness_generation'])
                                                  if row is not None else None),
                            valid_until=(row['valid_until']
                                         if row is not None else None)))
                return receipts
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route probe result batch persistence failed.'
            ) from error

    def revoke_replica(self, identity: RoutePublisherIdentity, replica_id: int,
                       replica_record_id: str, reason: str) -> int | None:
        """Durably revoke one exact route generation under owner fencing."""
        if not isinstance(identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route publisher identity is invalid.')
        _positive_int(replica_id, 'replica_id')
        record_id = uuid.UUID(_canonical_record_id(replica_record_id))
        _nonempty(reason, 'reason')
        try:
            with orm.Session(self.engine) as session, session.begin():
                owner = session.execute(
                    self._owner_query(
                        identity.service_name,
                        for_update=True)).mappings().one_or_none()
                if owner is None or not _owner_matches(identity, owner):
                    raise RouteProjectionConflict(
                        'Route revoker no longer owns this service.')
                lease = session.execute(
                    sqlalchemy.select(_LEASES).where(
                        _LEASES.c.service_name == identity.service_name,
                        _LEASES.c.service_hash == identity.service_hash,
                        _LEASES.c.replica_id == replica_id,
                        _LEASES.c.replica_record_id == record_id,
                    ).with_for_update()).mappings().one_or_none()
                if lease is None:
                    return None
                if lease['revoked_at'] is not None:
                    return int(lease['revocation_generation'])
                now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                generation = int(lease['revocation_generation']) + 1
                session.execute(
                    sqlalchemy.update(_LEASES).where(
                        _LEASES.c.service_name == identity.service_name,
                        _LEASES.c.service_hash == identity.service_hash,
                        _LEASES.c.replica_id == replica_id,
                        _LEASES.c.replica_record_id == record_id,
                    ).values(
                        ready=False,
                        observed_at=None,
                        valid_until=None,
                        revocation_generation=generation,
                        revoked_at=now,
                        revocation_reason=reason,
                    ))
                return generation
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route revocation failed.') from error

    def _publish_in_session(
        self,
        session: orm.Session,
        owner: Mapping[str, Any],
        identity: RoutePublisherIdentity,
        service_version: int,
        response: dict[str, Any],
        current_identities: dict[str, dict[str, Any]],
        current_record_ids: set[str],
        now: datetime.datetime,
        *,
        ttl_seconds: int,
        producer_protocol_version: int,
    ) -> RoutePublicationReceipt:
        """Write one already-validated snapshot in the caller transaction."""
        head = session.execute(
            sqlalchemy.select(_HEADS).where(
                _HEADS.c.service_name ==
                identity.service_name)).mappings().one_or_none()
        previous_row = None
        previous_identities = None
        if head is not None:
            previous_row = session.execute(
                sqlalchemy.select(_SNAPSHOTS).where(
                    _SNAPSHOTS.c.service_name == identity.service_name,
                    _SNAPSHOTS.c.generation ==
                    head['generation'])).mappings().one_or_none()
            if previous_row is None:
                raise RouteProjectionCorruption(
                    'Route head references no snapshot.')
            _, previous_identities = self.validate_snapshot_row(previous_row)
        valid_until = now + datetime.timedelta(seconds=ttl_seconds)
        identities = _merge_aliases(current_identities, previous_identities,
                                    current_record_ids, now)
        digest = _content_sha256(response, identities)
        duplicate = bool(previous_row is not None and
                         snapshot_owner_matches(previous_row, owner) and
                         previous_row['producer_protocol_version']
                         == producer_protocol_version and
                         previous_row['content_sha256'] == digest)
        if duplicate:
            assert previous_row is not None
            generation = int(previous_row['generation'])
        else:
            maximum = session.execute(
                sqlalchemy.select(sqlalchemy.func.max(
                    _SNAPSHOTS.c.generation)).where(
                        _SNAPSHOTS.c.service_name ==
                        identity.service_name)).scalar_one()
            generation = 1 if maximum is None else int(maximum) + 1
            session.execute(
                sqlalchemy.insert(_SNAPSHOTS).values(
                    service_name=identity.service_name,
                    generation=generation,
                    service_hash=identity.service_hash,
                    service_lifecycle_epoch=identity.service_lifecycle_epoch,
                    controller_incarnation=identity.controller_incarnation,
                    controller_owner_epoch=identity.controller_owner_epoch,
                    controller_pid=identity.controller_pid,
                    controller_ip=identity.controller_ip,
                    service_version=service_version,
                    protocol_version=PROTOCOL_VERSION,
                    producer_protocol_version=producer_protocol_version,
                    content_sha256=digest,
                    response_payload=response,
                    identity_payload=identities,
                    created_at=now))
        head_insert = postgresql.insert(_HEADS).values(
            service_name=identity.service_name,
            generation=generation,
            refreshed_at=now,
            valid_until=valid_until)
        session.execute(
            head_insert.on_conflict_do_update(
                index_elements=[_HEADS.c.service_name],
                set_={
                    'generation': generation,
                    'refreshed_at': now,
                    'valid_until': valid_until,
                }))
        session.execute(
            sqlalchemy.update(_SERVICES).where(
                _SERVICES.c.name == identity.service_name).values(
                    route_projection_capable=True,
                    route_projection_controller_incarnation=(
                        identity.controller_incarnation),
                    route_projection_protocol_version=(
                        producer_protocol_version)))
        oldest = generation - SNAPSHOT_HISTORY_LIMIT + 1
        if oldest > 1:
            session.execute(
                sqlalchemy.delete(_SNAPSHOTS).where(
                    _SNAPSHOTS.c.service_name == identity.service_name,
                    _SNAPSHOTS.c.generation < oldest))
        return RoutePublicationReceipt(generation=generation,
                                       content_sha256=digest,
                                       duplicate=duplicate,
                                       valid_until=valid_until)

    def publish(
        self,
        identity: RoutePublisherIdentity,
        service_version: int,
        response: dict[str, Any],
        current_identities: dict[str, dict[str, Any]],
        current_record_ids: set[str],
        *,
        ttl_seconds: int,
    ) -> RoutePublicationReceipt:
        """Publish or refresh one complete probe result transactionally."""
        if not isinstance(identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route publisher identity is invalid.')
        _positive_int(service_version, 'service_version')
        if (type(ttl_seconds) is not int or  # pylint: disable=unidiomatic-typecheck
                not 1 <= ttl_seconds <= MAX_ROUTE_TTL_SECONDS):
            raise RouteProjectionValidationError('Route TTL is invalid.')
        validated_response = _validate_response(response)
        validated_current = _validate_identities(current_identities,
                                                 require_service_version=True)
        canonical_records = {
            _canonical_record_id(record_id) for record_id in current_record_ids
        }
        if any(entry['alias_expires_at'] is not None
               for entry in validated_current.values()):
            raise RouteProjectionValidationError(
                'Publisher identities cannot contain aliases.')

        try:
            with orm.Session(self.engine) as session, session.begin():
                owner = session.execute(
                    self._owner_query(
                        identity.service_name,
                        for_update=True)).mappings().one_or_none()
                if owner is None or not _owner_matches(identity, owner):
                    raise RouteProjectionConflict(
                        'Route publisher no longer owns this service.')
                if owner['current_version'] != service_version:
                    raise RouteProjectionConflict(
                        'Route publisher version is no longer elected.')
                now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                return self._publish_in_session(
                    session,
                    owner,
                    identity,
                    service_version,
                    validated_response,
                    validated_current,
                    canonical_records,
                    now,
                    ttl_seconds=ttl_seconds,
                    producer_protocol_version=PROTOCOL_VERSION)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route publication failed.') from error

    def compose_incremental_snapshot(
        self,
        identity: RoutePublisherIdentity,
        service_version: int,
        routing_spec: dict[str, Any],
        decode_replica_state: Callable[[int, dict[str, Any]], Any],
        capacity_hint_builder: Callable[
            [list[Any], dict[int, tuple[str, str, int]], set[int]], dict[str,
                                                                         Any]],
        *,
        ttl_seconds: int,
    ) -> RoutePublicationReceipt:
        """Prepare without locks, then revalidate and publish atomically."""
        if not isinstance(identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route publisher identity is invalid.')
        _positive_int(service_version, 'service_version')
        if not isinstance(routing_spec, dict) or not routing_spec:
            raise RouteProjectionValidationError(
                'routing_spec must be complete.')
        if not callable(decode_replica_state) or not callable(
                capacity_hint_builder):
            raise RouteProjectionValidationError(
                'Route composition callbacks are invalid.')
        if (type(ttl_seconds) is not int or  # pylint: disable=unidiomatic-typecheck
                not 1 <= ttl_seconds <= MAX_ROUTE_TTL_SECONDS):
            raise RouteProjectionValidationError('Route TTL is invalid.')

        try:
            # The prepare transaction deliberately takes no row locks and ends
            # before any user-supplied decoder or capacity callback runs.
            with orm.Session(self.engine) as session, session.begin():
                prepared_owner = session.execute(
                    self._owner_query(
                        identity.service_name)).mappings().one_or_none()
                if (prepared_owner is None or
                        not _owner_matches(identity, prepared_owner)):
                    raise RouteProjectionConflict(
                        'Incremental route composer no longer owns this '
                        'service.')
                if prepared_owner['current_version'] != service_version:
                    raise RouteProjectionConflict(
                        'Incremental route version is no longer elected.')
                prepared_active_versions = _active_versions_from_owner(
                    prepared_owner)
                replica_rows = session.execute(
                    _incremental_replica_query(
                        identity.service_name,
                        include_state=True,
                        for_update=False)).mappings().all()
                prepared_replica_fingerprints = _replica_fingerprints(
                    replica_rows)
                lease_rows = session.execute(
                    _incremental_lease_query(
                        identity, for_update=False)).mappings().all()
                prepared_now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()

            replicas: list[IncrementalRouteReplica] = []
            replica_infos = []
            for row in replica_rows:
                try:
                    state = row['replica_state']
                    if not isinstance(state, dict):
                        raise ValueError('replica state is not an object')
                    replica = IncrementalRouteReplica(
                        replica_id=int(row['replica_id']),
                        replica_record_id=state['replica_record_id'],
                        service_version=int(row['version']),
                        status=row['status'])
                    info = decode_replica_state(
                        int(row['replica_state_version']), state)
                    if (getattr(info, 'replica_id', None) != replica.replica_id
                            or getattr(info, 'replica_record_id',
                                       None) != replica.replica_record_id or
                            getattr(info, 'version',
                                    None) != replica.service_version):
                        raise ValueError(
                            'replica state disagrees with scalar identity')
                except (KeyError, TypeError, ValueError,
                        RouteProjectionValidationError) as error:
                    raise RouteProjectionCorruption(
                        'A current replica row is corrupt.') from error
                replicas.append(replica)
                replica_infos.append(info)

            preliminary = build_incremental_route_view(
                replicas,
                lease_rows,
                prepared_active_versions,
                now=prepared_now,
                service_version=service_version,
                routing_spec=routing_spec,
                capacity_hint={})
            logical_versions = {
                int(row['service_version'])
                for row in lease_rows
                if row.get('uses_logical_replicas') is True
            }
            capacity_hint = capacity_hint_builder(replica_infos,
                                                  preliminary.translation_cache,
                                                  logical_versions)
            prepared_route_view = build_incremental_route_view(
                replicas,
                lease_rows,
                prepared_active_versions,
                now=prepared_now,
                service_version=service_version,
                routing_spec=routing_spec,
                capacity_hint=capacity_hint)
            response = _validate_response(prepared_route_view.response)
            current_identities = _validate_identities(
                prepared_route_view.identities, require_service_version=True)

            # The publish transaction has one fixed lock order and performs no
            # decoding, import, provider call, capacity callback, or route JSON
            # construction. A changed input rejects this tick; the worker
            # retries from a new snapshot.
            with orm.Session(self.engine) as session, session.begin():
                owner = session.execute(
                    self._owner_query(
                        identity.service_name,
                        for_update=True)).mappings().one_or_none()
                if owner is None or not _owner_matches(identity, owner):
                    raise RouteProjectionConflict(
                        'Incremental route composer no longer owns this '
                        'service.')
                if owner['current_version'] != service_version:
                    raise RouteProjectionConflict(
                        'Incremental route version is no longer elected.')
                active_versions = _active_versions_from_owner(owner)
                if active_versions != prepared_active_versions:
                    raise RouteProjectionConflict(
                        'Service active versions changed during composition.')
                current_replica_rows = session.execute(
                    _incremental_replica_query(
                        identity.service_name,
                        include_state=False,
                        for_update=True)).mappings().all()
                if (_replica_fingerprints(current_replica_rows)
                        != prepared_replica_fingerprints):
                    raise RouteProjectionConflict(
                        'Replica state changed during route composition.')
                current_lease_rows = session.execute(
                    _incremental_lease_query(identity,
                                             for_update=True)).mappings().all()
                _validate_revalidated_leases(lease_rows, current_lease_rows)
                now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                if (_lease_freshness_snapshot(lease_rows, prepared_now)
                        != _lease_freshness_snapshot(current_lease_rows, now)):
                    raise RouteProjectionConflict(
                        'Route lease freshness changed during composition.')
                return self._publish_in_session(
                    session,
                    owner,
                    identity,
                    service_version,
                    response,
                    current_identities,
                    prepared_route_view.live_record_ids,
                    now,
                    ttl_seconds=ttl_seconds,
                    producer_protocol_version=(
                        INCREMENTAL_PRODUCER_PROTOCOL_VERSION))
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL incremental route composition failed.') from error

    def resolve_sync(self, service_name: str, service_hash: str,
                     lb_session_id: object) -> RouteSyncDecision:
        """Select exactly one response owner and read a fresh projection."""
        _nonempty(service_name, 'service_name')
        _nonempty(service_hash, 'service_hash')
        engine = self._engine or serve_state_schema.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            return RouteSyncDecision(mode=RouteSourceMode.LEGACY_PROXY)
        try:
            with orm.Session(engine) as session, session.begin():
                owner = session.execute(
                    self._owner_query(service_name)).mappings().one_or_none()
                if owner is None or owner['hash'] != service_hash:
                    raise RouteProjectionConflict(
                        'Service incarnation mismatch.')
                if owner['pool'] not in (0, False):
                    raise RouteProjectionConflict(
                        'Pools have no load-balancer route projection.')
                try:
                    mode = RouteSourceMode(owner['route_source_mode'])
                except (TypeError, ValueError) as error:
                    raise RouteProjectionCorruption(
                        'Service route source mode is invalid.') from error
                if mode == RouteSourceMode.LEGACY_PROXY:
                    return RouteSyncDecision(mode=mode)
                if (not isinstance(lb_session_id, str) or
                        _SESSION_RE.fullmatch(lb_session_id) is None):
                    raise RouteProjectionConflict(
                        'A durable LB session identity is required.')
                if (owner['route_projection_capable'] is not True or
                        owner['route_projection_controller_incarnation']
                        != owner['controller_incarnation'] or
                        owner['route_projection_protocol_version']
                        not in (PROTOCOL_VERSION,
                                INCREMENTAL_PRODUCER_PROTOCOL_VERSION) or
                        owner['route_source_epoch'] < 1):
                    raise RouteProjectionUnavailable(
                        'Current controller route capability is unavailable.')
                now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                reporter_exists = session.execute(
                    sqlalchemy.select(sqlalchemy.exists().where(
                        _DEMAND_REPORTS.c.service_name == service_name,
                        _DEMAND_REPORTS.c.service_hash == service_hash,
                        _DEMAND_REPORTS.c.lb_session_id == lb_session_id,
                        _DEMAND_REPORTS.c.valid_until > now))).scalar_one()
                if not reporter_exists:
                    raise RouteProjectionUnavailable(
                        'LB session has no fresh durable membership report.')
                head = session.execute(
                    sqlalchemy.select(_HEADS).where(
                        _HEADS.c.service_name ==
                        service_name)).mappings().one_or_none()
                if head is None or head['valid_until'] <= now:
                    raise RouteProjectionUnavailable(
                        'Route projection is missing or stale.')
                snapshot = session.execute(
                    sqlalchemy.select(_SNAPSHOTS).where(
                        _SNAPSHOTS.c.service_name == service_name,
                        _SNAPSHOTS.c.generation ==
                        head['generation'])).mappings().one_or_none()
                if snapshot is None:
                    raise RouteProjectionCorruption(
                        'Route head references no snapshot.')
                if (not snapshot_owner_matches(snapshot, owner) or
                        snapshot['producer_protocol_version']
                        != owner['route_projection_protocol_version']):
                    raise RouteProjectionUnavailable(
                        'Route projection owner, version, or producer is '
                        'stale.')
                response, identities = self.validate_snapshot_row(snapshot)
                response.update({
                    'route_projection_generation': int(head['generation']),
                    'route_projection_sha256': snapshot['content_sha256'],
                    'route_occupancy_context_sha256': _occupancy_context_sha256(
                        response, identities),
                    'route_source_epoch': int(owner['route_source_epoch']),
                })
                return RouteSyncDecision(mode=mode, response=response)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route read failed.') from error

    def resolve_promotion_contract(
        self,
        identity: RoutePublisherIdentity,
        service_version: object,
        route_source_epoch: object,
        generation: object,
        content_sha256: object,
    ) -> RoutePromotionDecision:
        """Read the exact current route source and promotion fence once.

        Legacy route ownership has no durable projection contract.  Durable
        ownership exact-matches the load balancer's atomically reported
        version, epoch, generation, and digest against one fresh immutable
        snapshot copied from the same PostgreSQL transaction.  The controller
        chooses the legacy or durable promotion rule only from this result, so
        a concurrent one-way route-source promotion cannot race a stale legacy
        decision.
        """
        if not isinstance(identity, RoutePublisherIdentity):
            raise RouteProjectionValidationError(
                'Route publisher identity is invalid.')
        reported_version = _positive_int(service_version, 'service_version')
        try:
            with orm.Session(self.engine) as session, session.begin():
                # Every route publisher locks this row FOR UPDATE before it
                # advances the head.  A shared lock therefore gives the owner
                # and indexed head/snapshot join one coherent view while still
                # allowing both LB slots to report concurrently.  Join the
                # PostgreSQL lock queue for one short bounded interval so a
                # continuous stream of short writers cannot starve every role
                # heartbeat.  Timeout remains advisory and fail closed; the
                # next role heartbeat retries without changing the selected
                # slot.
                session.execute(
                    sqlalchemy.text('SET LOCAL lock_timeout = '
                                    f"'{PROMOTION_OWNER_LOCK_TIMEOUT_MS}ms'"))
                owner = session.execute(
                    self._owner_query(identity.service_name).with_for_update(
                        read=True)).mappings().one_or_none()
                if owner is None or not _owner_matches(identity, owner):
                    raise RouteProjectionConflict(
                        'Route promotion reader no longer owns this service.')
                try:
                    mode = RouteSourceMode(owner['route_source_mode'])
                except (TypeError, ValueError) as error:
                    raise RouteProjectionCorruption(
                        'Service route source mode is invalid.') from error
                if mode == RouteSourceMode.LEGACY_PROXY:
                    return RoutePromotionDecision(mode=mode)
                if owner['current_version'] != reported_version:
                    raise RouteProjectionConflict(
                        'Load-balancer service version is stale.')

                reported_epoch = _positive_int(route_source_epoch,
                                               'route_source_epoch')
                reported_generation = _positive_int(generation, 'generation')
                if (not isinstance(content_sha256, str) or
                        _SHA256_RE.fullmatch(content_sha256) is None):
                    raise RouteProjectionValidationError(
                        'Route projection digest is invalid.')
                if (owner['route_projection_capable'] is not True or
                        owner['route_projection_controller_incarnation']
                        != owner['controller_incarnation'] or
                        owner['route_projection_protocol_version']
                        not in (PROTOCOL_VERSION,
                                INCREMENTAL_PRODUCER_PROTOCOL_VERSION)):
                    raise RouteProjectionUnavailable(
                        'Current controller route capability is unavailable.')
                if owner['route_source_epoch'] != reported_epoch:
                    raise RouteProjectionConflict(
                        'Load-balancer route source epoch is stale.')

                head_snapshot = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp().label(
                            '_database_now'),
                        _HEADS.c.generation.label('_head_generation'),
                        _HEADS.c.valid_until.label('_head_valid_until'),
                        *_SNAPSHOTS.c,
                    ).select_from(
                        _HEADS.outerjoin(
                            _SNAPSHOTS,
                            sqlalchemy.and_(
                                _SNAPSHOTS.c.service_name ==
                                _HEADS.c.service_name,
                                _SNAPSHOTS.c.generation == _HEADS.c.generation))
                    ).where(_HEADS.c.service_name ==
                            identity.service_name)).mappings().one_or_none()
                if (head_snapshot is None or head_snapshot['_head_valid_until']
                        <= head_snapshot['_database_now']):
                    raise RouteProjectionUnavailable(
                        'Route projection is missing or stale.')
                if head_snapshot['_head_generation'] != reported_generation:
                    raise RouteProjectionConflict(
                        'Load-balancer route projection generation is stale.')
                if head_snapshot['service_name'] is None:
                    raise RouteProjectionCorruption(
                        'Route head references no snapshot.')
                snapshot = {
                    column.name: head_snapshot[column.name]
                    for column in _SNAPSHOTS.c
                }
                if snapshot['content_sha256'] != content_sha256:
                    raise RouteProjectionConflict(
                        'Load-balancer route projection digest is stale.')
                if (not snapshot_owner_matches(snapshot, owner) or
                        snapshot['producer_protocol_version']
                        != owner['route_projection_protocol_version']):
                    raise RouteProjectionUnavailable(
                        'Route projection owner, version, or producer is stale.'
                    )

                # `snapshot` is now a plain immutable-value copy.  Canonical
                # decoding is bounded but need not delay publishers; the
                # STABLE-to-PREPARING CAS revalidates this exact head.
            response, _ = self.validate_snapshot_row(snapshot)
            routing_urls = frozenset(
                url for url, info in response['replica_info'].items()
                if constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY not in info)
            async_urls: set[str] = set()
            for url in routing_urls:
                marker = response['replica_info'][url].get('async_occupancy')
                if marker not in (None, 'true', 'false'):
                    raise RouteProjectionCorruption(
                        'Route snapshot async-occupancy marker is invalid.')
                if marker == 'true':
                    async_urls.add(url)
            contract = RoutePromotionContract(
                service_version=reported_version,
                route_source_epoch=reported_epoch,
                generation=reported_generation,
                content_sha256=content_sha256,
                routing_urls=routing_urls,
                async_occupancy_urls=frozenset(async_urls))
            return RoutePromotionDecision(mode=mode, contract=contract)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route promotion-contract read failed.') from error

    def promote(self, service_name: str, service_hash: str) -> int:
        """Switch one fresh capable service to projected response ownership."""
        _nonempty(service_name, 'service_name')
        _nonempty(service_hash, 'service_hash')
        try:
            with orm.Session(self.engine) as session, session.begin():
                owner = session.execute(
                    self._owner_query(
                        service_name,
                        for_update=True)).mappings().one_or_none()
                if owner is None or owner['hash'] != service_hash:
                    raise RouteProjectionConflict(
                        'Service incarnation mismatch.')
                mode = RouteSourceMode(owner['route_source_mode'])
                if mode == RouteSourceMode.DURABLE_PROJECTED:
                    return int(owner['route_source_epoch'])
                ha_enabled = owner['lb_ha_enabled']
                if type(ha_enabled) is not int or ha_enabled not in (0, 1):
                    raise RouteProjectionCorruption(
                        'HA enablement state is invalid.')
                if ha_enabled == 1:
                    try:
                        cutover_phase = lb_ha.LbCutoverPhase(
                            owner['lb_cutover_phase'])
                    except (TypeError, ValueError) as error:
                        raise RouteProjectionCorruption(
                            'HA cutover phase is invalid.') from error
                    if cutover_phase is not lb_ha.LbCutoverPhase.STABLE:
                        raise RouteProjectionUnavailable(
                            'Route promotion requires a stable HA cutover.')
                if (owner['pool'] not in (0, False) or
                        owner['route_projection_capable'] is not True or
                        owner['route_projection_controller_incarnation']
                        != owner['controller_incarnation'] or
                        owner['route_projection_protocol_version']
                        != INCREMENTAL_PRODUCER_PROTOCOL_VERSION):
                    raise RouteProjectionUnavailable(
                        'Current controller has not published route capability.'
                    )
                now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                head = session.execute(
                    sqlalchemy.select(_HEADS).where(
                        _HEADS.c.service_name ==
                        service_name)).mappings().one_or_none()
                if head is None or head['valid_until'] <= now:
                    raise RouteProjectionUnavailable(
                        'A fresh route head is required for promotion.')
                snapshot = session.execute(
                    sqlalchemy.select(_SNAPSHOTS).where(
                        _SNAPSHOTS.c.service_name == service_name,
                        _SNAPSHOTS.c.generation ==
                        head['generation'])).mappings().one_or_none()
                if (snapshot is None or
                        not snapshot_owner_matches(snapshot, owner) or
                        snapshot['producer_protocol_version']
                        != INCREMENTAL_PRODUCER_PROTOCOL_VERSION):
                    raise RouteProjectionUnavailable(
                        'A current-owner route snapshot is required.')
                _, identities = self.validate_snapshot_row(snapshot)
                try:
                    raw_active_versions = owner['active_versions']
                    active_versions = set(
                        json.loads(raw_active_versions
                                  ) if raw_active_versions else [])
                except (TypeError, ValueError) as error:
                    raise RouteProjectionCorruption(
                        'Service active versions are corrupt.') from error
                ready_rows = session.execute(
                    sqlalchemy.select(
                        _REPLICAS.c.replica_state['replica_record_id'].
                        as_string().label('replica_record_id')).where(
                            _REPLICAS.c.service_name == service_name,
                            _REPLICAS.c.status ==
                            serve_statuses.ReplicaStatus.READY.value,
                            _REPLICAS.c.version.in_(sorted(
                                active_versions))).with_for_update()).all()
                expected_ready_records = {
                    _canonical_record_id(row.replica_record_id)
                    for row in ready_rows
                }
                advertised_records = {
                    entry['replica_record_id']
                    for entry in identities.values()
                    if entry['advertised']
                }
                if not expected_ready_records.issubset(advertised_records):
                    raise RouteProjectionUnavailable(
                        'Incremental route coverage is incomplete for current '
                        'ready replicas.')
                next_epoch = int(owner['route_source_epoch']) + 1
                session.execute(
                    sqlalchemy.update(_SERVICES).where(
                        _SERVICES.c.name == service_name,
                        _SERVICES.c.hash == service_hash,
                        _SERVICES.c.route_source_mode ==
                        RouteSourceMode.LEGACY_PROXY.value).values(
                            route_source_mode=(
                                RouteSourceMode.DURABLE_PROJECTED.value),
                            route_source_epoch=next_epoch))
                return next_epoch
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route promotion failed.') from error
