"""Provider-free PostgreSQL projection of SkyServe load-balancer routes.

The readiness owner supplies endpoints and accelerator material already
resolved by its bounded probe round.  Publication performs no provider read;
the stable API performs only PostgreSQL reads and returns the existing full LB
sync response shape.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
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
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.serve import system_recovery_route_lease
from sky.serve import system_recovery_state
from sky.utils.db import db_utils

PROTOCOL_VERSION = 1
SNAPSHOT_HISTORY_LIMIT = 96
ALIAS_RETENTION_SECONDS = 430
MAX_ALIASES_PER_RECORD = 8
MAX_ROUTE_IDENTITIES = 100_000
MAX_ROUTE_TTL_SECONDS = 24 * 60 * 60

_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_SESSION_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$')

_SERVICES = serve_state_schema.services_table
_SNAPSHOTS = route_projection_schema.serve_route_snapshots_table
_HEADS = route_projection_schema.serve_route_heads_table
_DEMAND_REPORTS = demand_state_schema.serve_lb_demand_reports_table


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
class RouteBuildResult:
    """One full public response and its private exact URL identities."""

    response: dict[str, Any]
    identities: dict[str, dict[str, Any]]
    live_record_ids: set[str]
    translation_cache: dict[int, tuple[str, str, int]]


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


def _content_sha256(response: object, identities: object) -> str:
    return hashlib.sha256(
        _canonical_json({
            'response': response,
            'identities': identities,
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


def _validate_identities(raw: object) -> dict[str, dict[str, Any]]:
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
        expected = {
            'replica_id', 'replica_record_id', 'gpu_type', 'gpu_count',
            'advertised', 'alias_expires_at'
        }
        if set(raw_entry) != expected:
            raise RouteProjectionValidationError(
                'Route identity entry has an invalid shape.')
        entry = dict(raw_entry)
        _positive_int(entry['replica_id'], 'replica_id')
        record_id = _canonical_record_id(entry['replica_record_id'])
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
        _SERVICES.c.route_source_mode,
        _SERVICES.c.route_source_epoch,
        _SERVICES.c.route_projection_capable,
        _SERVICES.c.route_projection_controller_incarnation,
        _SERVICES.c.route_projection_protocol_version,
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


def _snapshot_owner_matches(snapshot: Mapping[str, Any],
                            owner: Mapping[str, Any]) -> bool:
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

    @staticmethod
    def _snapshot_from_row(
        row: Mapping[str,
                     Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        try:
            if row['protocol_version'] != PROTOCOL_VERSION:
                raise RouteProjectionValidationError(
                    'Route snapshot protocol is unsupported.')
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
        validated_current = _validate_identities(current_identities)
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
                    _, previous_identities = self._snapshot_from_row(
                        previous_row)
                now = session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.clock_timestamp())).scalar_one()
                valid_until = now + datetime.timedelta(seconds=ttl_seconds)
                identities = _merge_aliases(validated_current,
                                            previous_identities,
                                            canonical_records, now)
                digest = _content_sha256(validated_response, identities)
                duplicate = bool(
                    previous_row is not None and
                    _snapshot_owner_matches(previous_row, owner) and
                    previous_row['content_sha256'] == digest)
                if duplicate:
                    assert previous_row is not None
                    generation = int(previous_row['generation'])
                else:
                    maximum = session.execute(
                        sqlalchemy.select(
                            sqlalchemy.func.max(_SNAPSHOTS.c.generation)).where(
                                _SNAPSHOTS.c.service_name ==
                                identity.service_name)).scalar_one()
                    generation = 1 if maximum is None else int(maximum) + 1
                    session.execute(
                        sqlalchemy.insert(_SNAPSHOTS).values(
                            service_name=identity.service_name,
                            generation=generation,
                            service_hash=identity.service_hash,
                            service_lifecycle_epoch=identity.
                            service_lifecycle_epoch,
                            controller_incarnation=identity.
                            controller_incarnation,
                            controller_owner_epoch=identity.
                            controller_owner_epoch,
                            controller_pid=identity.controller_pid,
                            controller_ip=identity.controller_ip,
                            service_version=service_version,
                            protocol_version=PROTOCOL_VERSION,
                            content_sha256=digest,
                            response_payload=validated_response,
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
                            route_projection_protocol_version=PROTOCOL_VERSION))
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
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route publication failed.') from error

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
                        != PROTOCOL_VERSION or owner['route_source_epoch'] < 1):
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
                if not _snapshot_owner_matches(snapshot, owner):
                    raise RouteProjectionUnavailable(
                        'Route projection owner or version is stale.')
                response, _ = self._snapshot_from_row(snapshot)
                response.update({
                    'route_projection_generation': int(head['generation']),
                    'route_projection_sha256': snapshot['content_sha256'],
                    'route_source_epoch': int(owner['route_source_epoch']),
                })
                return RouteSyncDecision(mode=mode, response=response)
        except sqlalchemy.exc.SQLAlchemyError as error:
            raise RouteProjectionUnavailable(
                'PostgreSQL route read failed.') from error

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
                if (owner['pool'] not in (0, False) or
                        owner['route_projection_capable'] is not True or
                        owner['route_projection_controller_incarnation']
                        != owner['controller_incarnation'] or
                        owner['route_projection_protocol_version']
                        != PROTOCOL_VERSION):
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
                        not _snapshot_owner_matches(snapshot, owner)):
                    raise RouteProjectionUnavailable(
                        'A current-owner route snapshot is required.')
                self._snapshot_from_row(snapshot)
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
