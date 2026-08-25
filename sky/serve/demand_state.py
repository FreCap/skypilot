"""Durable, controller-independent SkyServe request telemetry.

The load balancer writes bounded cumulative snapshots to the consolidated
PostgreSQL Serve database through the stable API server.  PostgreSQL receipt
time, not a reporter clock, owns freshness.  This module deliberately has no
provider or controller-process dependency so status remains useful while a
controller is wedged or restarting.
"""
from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import hashlib
import json
import math
import re
import time
from typing import Any

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.serve import constants
from sky.serve import demand_state_schema
from sky.serve import lb_ha
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_history
from sky.serve import serve_state_schema
from sky.utils.db import db_utils

_IDENTITY_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$')
_MAX_URLS = 4096
_MAX_PRIORITY_BUCKETS = 101
_MAX_COUNTER = (1 << 63) - 1


class DemandReportError(ValueError):
    """Base class for a rejected demand report."""


class DemandReportConflict(DemandReportError):
    """The report conflicts with durable service or sequence state."""


class DemandReportUnavailable(RuntimeError):
    """The durable feed is unavailable on this database backend."""


@dataclasses.dataclass(frozen=True)
class DemandReportReceipt:
    """Durable acknowledgement returned to one reporter."""

    generation: int
    duplicate: bool
    request_history_accepted: bool
    request_classification_history_accepted: bool
    prediction_time_history_accepted: bool


@dataclasses.dataclass(frozen=True)
class DurableReconcileAuthority:
    """Exact PostgreSQL evidence that can authorize logical reconciliation.

    ``deadline_monotonic`` is derived once from PostgreSQL's remaining
    validity at the start of the read.  Consumers must carry it unchanged;
    receiving or publishing the object locally can never grant a fresh TTL.
    ``valid_until`` is the matching database-clock boundary rechecked by the
    destructive commit transaction.
    """

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    service_version: int
    controller_incarnation: str
    controller_owner_epoch: int
    controller_pid: int
    controller_ip: str
    demand_source_epoch: int
    demand_feed_generation: int
    receipt_watermark: tuple[tuple[str, int, str], ...]
    route_generation: int
    route_sha256: str
    route_source_epoch: int
    lb_ha_enabled: bool
    lb_active_slot: str | None
    lb_cutover_generation: int
    fresh_aggregate_zero: bool
    occupancy_sampled_urls: tuple[str, ...]
    valid_until: datetime.datetime
    read_started_monotonic: float
    deadline_monotonic: float


@dataclasses.dataclass(frozen=True)
class DurableAutoscalingSnapshot:
    """One fresh, route-translated demand snapshot for the autoscaler."""

    service_name: str
    service_hash: str
    demand_source_epoch: int
    demand_feed_generation: int
    route_generation: int
    route_sha256: str
    route_source_epoch: int
    receipt_watermark: list[dict[str, Any]]
    request_information: dict[str, Any]
    normalized_demand: dict[str, Any]
    fresh_aggregate_zero: bool
    reconcile_authority: DurableReconcileAuthority


def _postgres_engine() -> sqlalchemy.engine.Engine:
    engine = serve_state_schema.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise DemandReportUnavailable(
            'The durable Serve demand feed requires PostgreSQL.')
    return engine


def _identity(value: Any, field: str) -> str:
    if not isinstance(value, str) or _IDENTITY_PATTERN.fullmatch(value) is None:
        raise DemandReportError(
            f'{field} must be a bounded process identity string.')
    return value


def _positive_int(value: Any, field: str) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value <= 0 or
            value > _MAX_COUNTER):
        raise DemandReportError(f'{field} must be a bounded positive integer.')
    return value


def _nonnegative_int(value: Any, field: str) -> int:
    if (not isinstance(value, int) or isinstance(value, bool) or value < 0 or
            value > _MAX_COUNTER):
        raise DemandReportError(
            f'{field} must be a bounded nonnegative integer.')
    return value


def _finite_timestamp(value: Any, field: str) -> float:
    if (not isinstance(value, (int, float)) or isinstance(value, bool) or
            not math.isfinite(value) or value < 0):
        raise DemandReportError(f'{field} must be a finite epoch timestamp.')
    return float(value)


def _string_list(value: Any, field: str, *, max_items: int) -> list[str]:
    if (not isinstance(value, list) or len(value) > max_items or
            not all(isinstance(item, str) and item for item in value) or
            len(set(value)) != len(value)):
        raise DemandReportError(
            f'{field} must be a bounded list of distinct strings.')
    return list(value)


def _nonnegative_map(value: Any, field: str) -> dict[str, int]:
    if not isinstance(value, dict) or len(value) > _MAX_URLS:
        raise DemandReportError(f'{field} must be a bounded object.')
    result: dict[str, int] = {}
    for key, count in value.items():
        if not isinstance(key, str) or not key:
            raise DemandReportError(f'{field} keys must be non-empty strings.')
        result[key] = _nonnegative_int(count, f'{field}[{key!r}]')
    return result


def _nonnegative_float_map(value: Any, field: str) -> dict[str, float]:
    if not isinstance(value, dict) or len(value) > _MAX_URLS:
        raise DemandReportError(f'{field} must be a bounded object.')
    result: dict[str, float] = {}
    for key, raw in value.items():
        if not isinstance(key, str) or not key:
            raise DemandReportError(f'{field} keys must be non-empty strings.')
        if (not isinstance(raw, (int, float)) or isinstance(raw, bool) or
                not math.isfinite(raw) or raw < 0 or raw > _MAX_COUNTER):
            raise DemandReportError(
                f'{field}[{key!r}] must be a bounded nonnegative number.')
        result[key] = float(raw)
    return result


def _priority_map(value: Any, field: str) -> dict[str, int]:
    parsed = _nonnegative_map(value, field)
    if len(parsed) > _MAX_PRIORITY_BUCKETS:
        raise DemandReportError(f'{field} has too many priority buckets.')
    for priority in parsed:
        try:
            number = int(priority)
        except ValueError:
            raise DemandReportError(
                f'{field} keys must be integer priorities.') from None
        if str(number) != priority or not 0 <= number <= 100:
            raise DemandReportError(
                f'{field} keys must be canonical priorities from 0 to 100.')
    return parsed


def _validate_compatibility_profiles(
        value: Any, field: str, *, require_timestamp: bool
) -> tuple[set[str], dict[str, int], dict[str, int]]:
    if not isinstance(value,
                      list) or len(value) > constants.LB_REQUEST_TIMESTAMP_CAP:
        raise DemandReportError(f'{field} must be a bounded list.')
    accelerators: set[str] = set()
    counts_by_priority: dict[str, int] = {}
    recent_counts_by_priority: dict[str, int] = {}
    for profile in value:
        parsed = lb_ha.CompatibilityDemand.from_dict(
            profile, require_timestamp=require_timestamp)
        if parsed is None:
            raise DemandReportError(f'{field} contains an invalid profile.')
        if not 0 <= parsed.priority <= 100 or parsed.count > _MAX_COUNTER:
            raise DemandReportError(f'{field} contains an invalid profile.')
        if (parsed.recent_count is not None and
                parsed.recent_count > _MAX_COUNTER):
            raise DemandReportError(f'{field} contains an invalid profile.')
        if (len(parsed.compatible_accelerators)
                > constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS or
                len(set(parsed.compatible_accelerators)) != len(
                    parsed.compatible_accelerators)):
            raise DemandReportError(
                f'{field} contains an invalid accelerator set.')
        accelerators.update(parsed.compatible_accelerators)
        priority = str(parsed.priority)
        counts_by_priority[priority] = (counts_by_priority.get(priority, 0) +
                                        parsed.count)
        if parsed.recent_count:
            recent_counts_by_priority[priority] = (
                recent_counts_by_priority.get(priority, 0) +
                parsed.recent_count)
    return accelerators, counts_by_priority, recent_counts_by_priority


def _validate_demand_window(
        value: Any,
        observed_at: float) -> tuple[dict[str, Any], bool, set[str]]:
    if not isinstance(value, dict):
        raise DemandReportError('demand_window must be an object.')
    bucket_seconds = value.get('bucket_seconds')
    window_seconds = value.get('window_seconds')
    if bucket_seconds != constants.LB_DEMAND_WINDOW_BUCKET_SECONDS:
        raise DemandReportError('demand_window bucket_seconds is unsupported.')
    if window_seconds != constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS:
        raise DemandReportError('demand_window window_seconds is unsupported.')
    buckets = value.get('buckets')
    max_buckets = window_seconds // bucket_seconds + 2
    if not isinstance(buckets, list) or len(buckets) > max_buckets:
        raise DemandReportError('demand_window buckets must be bounded.')
    oldest = observed_at - window_seconds - bucket_seconds
    newest = observed_at
    compatibility_complete = value.get('compatibility_complete')
    saturated = value.get('saturated')
    coverage_started_at = value.get('coverage_started_at')
    if not isinstance(compatibility_complete, bool):
        raise DemandReportError(
            'demand_window compatibility_complete must be boolean.')
    if not isinstance(saturated, bool):
        raise DemandReportError('demand_window saturated must be boolean.')
    if coverage_started_at is not None:
        coverage_started_at = _finite_timestamp(
            coverage_started_at, 'demand_window coverage_started_at')
        if coverage_started_at > observed_at:
            raise DemandReportError(
                'demand_window coverage_started_at is in the future.')
    # A saturated recorder has dropped events.  It may report the retained
    # counts for observability, but it cannot grant exact-card authority.
    compatibility_complete = compatibility_complete and not saturated
    normalized_buckets = []
    compatibility_accelerators: set[str] = set()
    seen: set[int] = set()
    for bucket in buckets:
        if not isinstance(bucket, dict):
            raise DemandReportError('demand_window bucket must be an object.')
        bucket_start = bucket.get('bucket_start')
        if (not isinstance(bucket_start, int) or
                isinstance(bucket_start, bool) or
                bucket_start % bucket_seconds != 0 or bucket_start in seen):
            raise DemandReportError(
                'demand_window bucket_start must be distinct and aligned.')
        seen.add(bucket_start)
        if not oldest <= bucket_start <= newest:
            raise DemandReportError(
                'demand_window bucket_start is outside the accepted window.')
        request_count = _nonnegative_int(bucket.get('request_count'),
                                         'demand_window request_count')
        profiles = bucket.get('compatibility_profiles')
        profile_accelerators, _, _ = _validate_compatibility_profiles(
            profiles,
            'demand_window compatibility_profiles',
            require_timestamp=False)
        compatibility_accelerators.update(profile_accelerators)
        assert isinstance(profiles, list)
        profile_count = 0
        for profile in profiles:
            assert isinstance(profile, dict)
            profile_count += int(profile.get('count', 1))
        if profile_count > request_count:
            raise DemandReportError(
                'demand_window compatibility counts exceed request_count.')
        if compatibility_complete and profile_count != request_count:
            raise DemandReportError(
                'Complete demand_window compatibility counts must equal '
                'request_count.')
        normalized_buckets.append({
            'bucket_start': bucket_start,
            'request_count': request_count,
            'compatibility_profiles': profiles,
        })
    if [bucket['bucket_start'] for bucket in normalized_buckets
       ] != sorted(seen):
        raise DemandReportError('demand_window buckets must be sorted.')
    return {
        'bucket_seconds': bucket_seconds,
        'window_seconds': window_seconds,
        'coverage_started_at': coverage_started_at,
        'buckets': normalized_buckets,
        'compatibility_complete': compatibility_complete,
        'saturated': saturated,
    }, compatibility_complete, compatibility_accelerators


def _validate_report(raw: Any) -> tuple[dict[str, Any], str, bool]:
    if not isinstance(raw, dict):
        raise DemandReportError('Demand report must be a JSON object.')
    try:
        encoded = json.dumps(raw,
                             sort_keys=True,
                             separators=(',', ':'),
                             allow_nan=False).encode('utf-8')
    except (TypeError, ValueError) as e:
        raise DemandReportError(
            'Demand report must contain canonical JSON values.') from e
    if len(encoded) > constants.LB_DEMAND_REPORT_MAX_BYTES:
        raise DemandReportError('Demand report exceeds the size limit.')
    protocol = raw.get('protocol_version')
    if (not isinstance(protocol, int) or isinstance(protocol, bool) or
            not constants.LB_DEMAND_REPORT_MIN_PROTOCOL_VERSION <= protocol <=
            constants.LB_DEMAND_REPORT_PROTOCOL_VERSION):
        raise DemandReportError('Demand report protocol is unsupported.')
    sequence = _positive_int(raw.get('sequence'), 'sequence')
    reporter_session_id = _identity(raw.get('reporter_session_id'),
                                    'reporter_session_id')
    lb_session_id = _identity(raw.get('lb_session_id'), 'lb_session_id')
    observed_at = _finite_timestamp(raw.get('reporter_observed_at'),
                                    'reporter_observed_at')
    lb_slot = raw.get('lb_slot')
    if lb_slot is not None and lb_ha.parse_slot(lb_slot) is None:
        raise DemandReportError('lb_slot is invalid.')
    routing_version = raw.get('routing_version')
    if (routing_version is not None and
        (not isinstance(routing_version, int) or
         isinstance(routing_version, bool) or routing_version < 1)):
        raise DemandReportError(
            'routing_version must be a positive integer or null.')
    route_projection_generation = raw.get('route_projection_generation')
    route_projection_sha256 = raw.get('route_projection_sha256')
    route_source_epoch = raw.get('route_source_epoch')
    route_fence = (route_projection_generation, route_projection_sha256,
                   route_source_epoch)
    if any(value is not None for value in route_fence):
        _positive_int(route_projection_generation,
                      'route_projection_generation')
        if (not isinstance(route_projection_sha256, str) or
                re.fullmatch(r'[0-9a-f]{64}', route_projection_sha256) is None):
            raise DemandReportError(
                'route_projection_sha256 must be SHA-256 or null.')
        _positive_int(route_source_epoch, 'route_source_epoch')
    try:
        role = lb_ha.LbRole(raw.get('applied_role'))
    except (TypeError, ValueError):
        raise DemandReportError('applied_role is invalid.') from None
    applied_generation = _nonnegative_int(raw.get('applied_generation'),
                                          'applied_generation')
    armed_generation = raw.get('armed_generation')
    if armed_generation is not None:
        _nonnegative_int(armed_generation, 'armed_generation')

    _nonnegative_int(raw.get('local_in_flight'), 'local_in_flight')
    _nonnegative_map(raw.get('http_in_flight'), 'http_in_flight')
    _nonnegative_map(raw.get('async_occupancy'), 'async_occupancy')
    _nonnegative_map(raw.get('occupancy_sample_generation'),
                     'occupancy_sample_generation')
    _nonnegative_float_map(raw.get('occupancy_sample_age_seconds'),
                           'occupancy_sample_age_seconds')
    for field in ('routing_urls', 'unknown_in_flight_urls', 'draining_urls'):
        _string_list(raw.get(field), field, max_items=_MAX_URLS)
    if protocol >= 2:
        total_slots = _nonnegative_map(raw.get('total_slots_by_url'),
                                       'total_slots_by_url')
        sampled_urls = _string_list(raw.get('occupancy_sampled_urls'),
                                    'occupancy_sampled_urls',
                                    max_items=_MAX_URLS)
        sampled_set = set(sampled_urls)
        occupancy_sets = (
            set(raw['async_occupancy']),
            set(raw['occupancy_sample_generation']),
            set(raw['occupancy_sample_age_seconds']),
        )
        if (set(total_slots) != sampled_set or
                any(urls != sampled_set for urls in occupancy_sets)):
            raise DemandReportError(
                'Protocol 2 slot and occupancy URLs must exactly match.')
        if not set(raw['routing_urls']).issubset(
                sampled_set | set(raw['unknown_in_flight_urls'])):
            raise DemandReportError(
                'Every routed URL must be sampled or explicitly unknown.')

    # Reuse the production HA parser for the three-way HTTP/async/unknown
    # accounting contract instead of maintaining a second interpretation.
    ledger = lb_ha.LbSessionLedger(constants.LB_DEMAND_REPORT_TTL_SECONDS,
                                   constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS)
    slot = lb_ha.parse_slot(lb_slot) or lb_ha.LbSlot.A
    if not ledger.update(
            lb_session_id, slot, role, applied_generation, raw,
            now=observed_at):
        raise DemandReportError('Demand report role/occupancy data is invalid.')

    demand_window, compatibility_complete, demand_accelerators = (
        _validate_demand_window(raw.get('demand_window'), observed_at))
    configured_accelerators = _string_list(
        raw.get('configured_accelerators'),
        'configured_accelerators',
        max_items=constants.LB_REQUEST_ACCELERATORS_MAX_ITEMS)
    compatibility_version = raw.get('request_accelerator_compatibility_version')
    if (compatibility_version is not None and
        (not isinstance(compatibility_version, int) or
         isinstance(compatibility_version, bool) or compatibility_version < 1)):
        raise DemandReportError(
            'request_accelerator_compatibility_version is invalid.')
    counts = {
        field: _nonnegative_int(raw.get(field), field)
        for field in ('queue_depth', 'rejected_in_window',
                      'rejected_in_recent_window', 'unique_job_arrivals_60s',
                      'unique_job_arrivals_300s', 'headerless_arrivals_60s',
                      'headerless_arrivals_300s')
    }
    queue_by_priority = _priority_map(raw.get('queue_depth_by_priority'),
                                      'queue_depth_by_priority')
    rejected_by_priority = _priority_map(
        raw.get('rejected_in_window_by_priority'),
        'rejected_in_window_by_priority')
    recent_rejected_by_priority = _priority_map(
        raw.get('rejected_in_recent_window_by_priority'),
        'rejected_in_recent_window_by_priority')
    (queued_accelerators, queued_profiles_by_priority,
     _) = _validate_compatibility_profiles(
         raw.get('queued_requests_by_compatibility'),
         'queued_requests_by_compatibility',
         require_timestamp=False)
    (rejected_accelerators, rejected_profiles_by_priority,
     recent_rejected_profiles_by_priority) = _validate_compatibility_profiles(
         raw.get('rejected_requests_by_compatibility'),
         'rejected_requests_by_compatibility',
         require_timestamp=False)
    queued_profiles = raw['queued_requests_by_compatibility']
    rejected_profiles = raw['rejected_requests_by_compatibility']
    queued_profile_count = sum(
        profile.get('count', 1) for profile in queued_profiles)
    rejected_profile_count = sum(
        profile.get('count', 1) for profile in rejected_profiles)
    recent_rejected_profile_count = sum(
        profile.get('recent_count', 0) for profile in rejected_profiles)
    if (queued_profile_count > counts['queue_depth'] or
            rejected_profile_count > counts['rejected_in_window'] or
            recent_rejected_profile_count
            > counts['rejected_in_recent_window']):
        raise DemandReportError(
            'Compatibility profile counts exceed their aggregate gauges.')
    complete_priority_totals = (
        queued_profile_count == counts['queue_depth'] and
        sum(queue_by_priority.values()) == counts['queue_depth'] and
        rejected_profile_count == counts['rejected_in_window'] and
        sum(rejected_by_priority.values()) == counts['rejected_in_window'] and
        recent_rejected_profile_count == counts['rejected_in_recent_window'] and
        sum(recent_rejected_by_priority.values())
        == counts['rejected_in_recent_window'])
    if complete_priority_totals and (
            queued_profiles_by_priority != queue_by_priority or
            rejected_profiles_by_priority != rejected_by_priority or
            recent_rejected_profiles_by_priority
            != recent_rejected_by_priority):
        raise DemandReportError(
            'Compatibility profile priorities conflict with aggregate '
            'priority gauges.')
    unknown_accelerators = (
        demand_accelerators | queued_accelerators |
        rejected_accelerators) - set(configured_accelerators)
    if unknown_accelerators:
        raise DemandReportError(
            'Compatibility profiles contain accelerators outside the '
            'configured catalog.')
    offered_saturated = raw.get('offered_arrival_tracking_saturated')
    if not isinstance(offered_saturated, bool):
        raise DemandReportError(
            'offered_arrival_tracking_saturated must be boolean.')
    compatibility_complete = bool(
        compatibility_complete and
        queued_profile_count == counts['queue_depth'] and
        rejected_profile_count == counts['rejected_in_window'] and
        recent_rejected_profile_count == counts['rejected_in_recent_window'] and
        complete_priority_totals and not offered_saturated)

    request_history = raw.get('request_history')
    if request_history is not None:
        serve_history.validate_request_activity_history(request_history,
                                                        observed_at)
    classification = raw.get('request_classification_history')
    if classification is not None:
        serve_history.validate_request_classification_history(
            classification, observed_at)
    prediction = raw.get('prediction_time_history')
    if prediction is not None:
        serve_history.validate_prediction_time_history(prediction, observed_at)

    normalized = dict(raw)
    normalized.update(
        protocol_version=protocol,
        sequence=sequence,
        reporter_session_id=reporter_session_id,
        lb_session_id=lb_session_id,
        reporter_observed_at=observed_at,
        demand_window=demand_window,
        configured_accelerators=configured_accelerators,
    )
    complete = bool(protocol >= 2 and routing_version is not None and
                    configured_accelerators and
                    compatibility_version is not None and
                    compatibility_complete)
    digest = hashlib.sha256(encoded).hexdigest()
    return normalized, digest, complete


def _record_history(service_name: str, service_hash: str,
                    payload: dict[str, Any]) -> tuple[bool, bool, bool]:
    reporter = (f"{payload['lb_session_id']}:"
                f"{payload['reporter_session_id']}")
    observed_at = payload['reporter_observed_at']
    request_history = payload.get('request_history')
    classification = payload.get('request_classification_history')
    prediction = payload.get('prediction_time_history')
    serve_history.record_request_activity(service_name, service_hash, reporter,
                                          request_history, observed_at)
    serve_history.record_request_classification(service_name,
                                                service_hash,
                                                reporter,
                                                classification,
                                                observed_at,
                                                request_history=request_history)
    serve_history.record_prediction_times(service_name, service_hash, reporter,
                                          prediction, observed_at)
    return request_history is not None, classification is not None, prediction is not None


def ingest_report(service_name: str, service_hash: str,
                  raw: Any) -> DemandReportReceipt:
    """Validate and durably replace one reporter's monotonic snapshot."""
    _identity(service_name, 'service_name')
    _identity(service_hash, 'service_hash')
    payload, digest, complete = _validate_report(raw)
    engine = _postgres_engine()
    reports = demand_state_schema.serve_lb_demand_reports_table
    generations = demand_state_schema.serve_demand_feed_generations_table
    services = serve_state_schema.services_table
    duplicate = False
    with engine.begin() as connection:
        service = connection.execute(
            sqlalchemy.select(services.c.hash, services.c.pool).where(
                services.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
        if service is None:
            raise DemandReportConflict('Service does not exist.')
        if service['hash'] != service_hash:
            raise DemandReportConflict('Service incarnation mismatch.')
        if bool(service['pool']):
            raise DemandReportConflict('Pools do not have a demand feed.')
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.execute(
            sqlalchemy.delete(reports).where(
                reports.c.service_name == service_name, reports.c.valid_until
                < now - datetime.timedelta(
                    seconds=constants.LB_DEMAND_REPORT_RETENTION_SECONDS)))
        try:
            reporter_time = datetime.datetime.fromtimestamp(
                payload['reporter_observed_at'], datetime.timezone.utc)
        except (OverflowError, OSError, ValueError) as e:
            raise DemandReportError(
                'reporter_observed_at is outside the supported range.') from e
        if (abs((reporter_time - now).total_seconds())
                > constants.LB_DEMAND_REPORT_MAX_CLOCK_SKEW_SECONDS):
            raise DemandReportError(
                'reporter_observed_at differs too far from the database clock.')
        existing = connection.execute(
            sqlalchemy.select(reports.c.sequence, reports.c.payload_sha256,
                              reports.c.lb_session_id, reports.c.lb_slot).where(
                                  reports.c.service_name == service_name,
                                  reports.c.service_hash == service_hash,
                                  reports.c.reporter_session_id ==
                                  payload['reporter_session_id']).
            with_for_update()).mappings().one_or_none()
        if existing is not None:
            if (existing['lb_session_id'] != payload['lb_session_id'] or
                    existing['lb_slot'] != payload.get('lb_slot')):
                raise DemandReportConflict(
                    'Demand reporter identity changed within one session.')
            existing_sequence = int(existing['sequence'])
            if payload['sequence'] < existing_sequence:
                raise DemandReportConflict(
                    'Demand report sequence moved backwards.')
            if payload['sequence'] == existing_sequence:
                if existing['payload_sha256'] != digest:
                    raise DemandReportConflict(
                        'Demand report sequence conflicts with prior payload.')
                duplicate = True
        else:
            # Live rows are operational gauges, not history. Once a new
            # reporter is present, expired rows add no state and must not let a
            # compromised shared sync credential grow this table without
            # bound by minting process identities.
            connection.execute(
                sqlalchemy.delete(reports).where(
                    reports.c.service_name == service_name,
                    reports.c.service_hash == service_hash,
                    reports.c.valid_until <= now))
            reporter_count = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(reports).where(
                    reports.c.service_name == service_name,
                    reports.c.service_hash == service_hash)).scalar_one()
            if reporter_count >= constants.LB_DEMAND_REPORT_MAX_REPORTERS:
                raise DemandReportConflict(
                    'Demand reporter limit reached for this service.')
        if not duplicate:
            values = {
                'service_name': service_name,
                'service_hash': service_hash,
                'reporter_session_id': payload['reporter_session_id'],
                'lb_session_id': payload['lb_session_id'],
                'lb_slot': payload.get('lb_slot'),
                'protocol_version': payload['protocol_version'],
                'sequence': payload['sequence'],
                'routing_version': payload.get('routing_version'),
                'reporter_observed_at': reporter_time,
                'received_at': now,
                'valid_until': now + datetime.timedelta(
                    seconds=constants.LB_DEMAND_REPORT_TTL_SECONDS),
                'payload_sha256': digest,
                'complete': complete,
                'payload': payload,
            }
            insert = postgresql.insert(reports).values(**values)
            connection.execute(
                insert.on_conflict_do_update(
                    index_elements=[
                        reports.c.service_name, reports.c.service_hash,
                        reports.c.reporter_session_id
                    ],
                    set_={
                        key: value
                        for key, value in values.items()
                        if key not in ('service_name', 'service_hash',
                                       'reporter_session_id')
                    }))
            generation_row = connection.execute(
                sqlalchemy.select(generations.c.generation).where(
                    generations.c.service_name ==
                    service_name).with_for_update()).scalar_one_or_none()
            generation = int(generation_row or 0) + 1
            generation_insert = postgresql.insert(generations).values(
                service_name=service_name,
                service_hash=service_hash,
                generation=generation,
                updated_at=now)
            connection.execute(
                generation_insert.on_conflict_do_update(
                    index_elements=[generations.c.service_name],
                    set_={
                        'service_hash': service_hash,
                        'generation': generation,
                        'updated_at': now,
                    }))
        else:
            generation = connection.execute(
                sqlalchemy.select(generations.c.generation).where(
                    generations.c.service_name == service_name,
                    generations.c.service_hash == service_hash)).scalar_one()

    # History uses idempotent greatest-value upserts.  It intentionally runs
    # after the report transaction: a transient history failure returns no
    # acknowledgement, and retrying the exact same sequence/digest completes
    # history without extending the report's DB-owned validity window.
    history_accepted = _record_history(service_name, service_hash, payload)
    return DemandReportReceipt(generation, duplicate, *history_accepted)


def _empty_summary(state: str,
                   reason: str,
                   *,
                   generation: int | None = None) -> dict[str, Any]:
    return {
        'request_telemetry_source': 'postgresql_lb_demand_reports',
        'request_telemetry_state': state,
        'request_telemetry_reason': reason,
        'request_telemetry_generation': generation,
        'request_telemetry_compatibility_complete': False,
        'request_reporter_count': 0,
        'recent_request_count': None,
        'request_window_seconds': constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS,
        'requests_per_second': None,
        'in_flight_requests': None,
        'confirmed_in_flight_requests': None,
        'unknown_in_flight_replica_count': None,
        'request_queue_depth': None,
        'rejected_requests': None,
        'recent_rejected_requests': None,
        'request_stats_age_seconds': None,
    }


def unavailable_request_summary(reason: str) -> dict[str, Any]:
    """Return the stable public shape when direct demand reads are disabled."""
    return _empty_summary('unavailable', reason)


def reports_match_current_lb_authority(
    rows: list[Mapping[str, Any]],
    service: Mapping[str, Any],
) -> bool:
    """Prove that fresh demand includes the currently selected ACTIVE LB.

    HA cutover advances the service generation before a new role becomes
    authoritative.  A still-fresh report from the previous generation must
    therefore be display-only: it cannot promote durable demand, publish a
    capacity plan, or keep a paid claim alive.
    """
    ha_enabled = service.get('lb_ha_enabled') == 1
    active_slot = service.get('lb_active_slot')
    generation = service.get('lb_cutover_generation')
    current_active_present = False
    for row in rows:
        payload = row.get('payload')
        if not isinstance(payload, Mapping):
            return False
        try:
            role = lb_ha.LbRole(payload.get('applied_role'))
        except (TypeError, ValueError):
            return False
        if role not in (lb_ha.LbRole.ACTIVE, lb_ha.LbRole.DRAINING):
            continue
        if ha_enabled and payload.get('applied_generation') != generation:
            return False
        if role is lb_ha.LbRole.ACTIVE:
            if (not ha_enabled or
                (row.get('lb_slot') == active_slot and
                 payload.get('applied_generation') == generation)):
                current_active_present = True
    return current_active_present


def reports_prove_fresh_aggregate_zero(rows: list[Mapping[str, Any]]) -> bool:
    """Whether fresh protocol-2 reporters cover an entirely quiet window.

    Callers own PostgreSQL freshness and current-LB authority checks. This
    pure predicate intentionally ignores exact-card compatibility and
    occupancy: it can revoke paid launch authority, while destructive
    teardown remains separately gated by each replica's exact idle proof.
    """
    if not rows:
        return False
    for row in rows:
        payload = row.get('payload')
        if (not isinstance(payload, Mapping) or
                payload.get('protocol_version') != 2):
            return False
        window = payload.get('demand_window')
        observed_at = payload.get('reporter_observed_at')
        if (not isinstance(window, Mapping) or
                not isinstance(observed_at,
                               (int, float)) or isinstance(observed_at, bool)):
            return False
        coverage_started_at = window.get('coverage_started_at')
        window_seconds = window.get('window_seconds')
        buckets = window.get('buckets')
        if (not isinstance(coverage_started_at, (int, float)) or
                isinstance(coverage_started_at, bool) or
                not isinstance(window_seconds, int) or
                isinstance(window_seconds, bool) or window_seconds < 1 or
                float(coverage_started_at) > float(observed_at) - window_seconds
                or window.get('saturated') is not False or
                payload.get('offered_arrival_tracking_saturated') is not False
                or not isinstance(buckets, list) or
                any(not isinstance(bucket, Mapping) or
                    bucket.get('request_count') != 0 for bucket in buckets)):
            return False
        for field in ('queue_depth', 'rejected_in_window',
                      'rejected_in_recent_window'):
            if payload.get(field) != 0:
                return False
    return True


def _aggregate_fresh_reports(rows: list[Any], generation: int | None,
                             now: datetime.datetime) -> dict[str, Any]:
    """Aggregate already-selected fresh rows, rejecting corrupt state."""
    ledger = lb_ha.LbSessionLedger(constants.LB_DEMAND_REPORT_TTL_SECONDS,
                                   constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS)
    sessions: set[str] = set()
    demand_buckets: list[tuple[float, int]] = []
    queue_depth = 0
    rejected = 0
    recent_rejected = 0
    newest_received_at = min(row['received_at'] for row in rows)
    complete = True
    active_report_present = False
    now_epoch = now.timestamp()
    for row in rows:
        payload = row['payload']
        if not isinstance(payload, dict):
            raise ValueError('Demand report payload must be an object.')
        session = str(row['reporter_session_id'])
        slot = lb_ha.parse_slot(row['lb_slot']) or lb_ha.LbSlot.A
        role = lb_ha.LbRole(payload.get('applied_role'))
        active_report_present = (active_report_present or
                                 role is lb_ha.LbRole.ACTIVE)
        received_epoch = row['received_at'].timestamp()
        elapsed_since_receipt = max(0.0, now_epoch - received_epoch)
        ledger_payload = dict(payload)
        raw_sample_ages = payload.get('occupancy_sample_age_seconds')
        if not isinstance(raw_sample_ages, dict):
            raise ValueError('Demand report sample ages must be an object.')
        ledger_payload['occupancy_sample_age_seconds'] = {
            url: float(age) + elapsed_since_receipt
            for url, age in raw_sample_ages.items()
        }
        if not ledger.update(session,
                             slot,
                             role,
                             int(payload.get('applied_generation', 0)),
                             ledger_payload,
                             now=now_epoch):
            raise ValueError('Demand report occupancy payload is invalid.')
        sessions.add(session)
        window = payload.get('demand_window', {})
        bucket_seconds = int(window.get('bucket_seconds', 0))
        reporter_epoch = row['reporter_observed_at'].timestamp()
        # Rebase reporter-relative bucket ages onto PostgreSQL receipt time.
        # Even a tolerated host-clock offset therefore cannot make arrivals
        # remain current beyond the database-owned rolling window.
        demand_buckets.extend(
            (received_epoch +
             (int(bucket['bucket_start']) + bucket_seconds - reporter_epoch),
             int(bucket['request_count']))
            for bucket in window.get('buckets', []))
        queue_depth += int(payload.get('queue_depth', 0))
        rejected += int(payload.get('rejected_in_window', 0))
        recent_rejected += int(payload.get('rejected_in_recent_window', 0))
        newest_received_at = max(newest_received_at, row['received_at'])
        complete = complete and bool(row['complete'])
    if not active_report_present:
        return _empty_summary('stale',
                              'active_report_missing',
                              generation=generation)
    aggregate = ledger.aggregate(sessions, now=now_epoch)
    if not aggregate.complete:
        return _empty_summary('stale',
                              'report_set_incomplete',
                              generation=generation)
    window_seconds = constants.AUTOSCALER_QPS_WINDOW_SIZE_SECONDS
    cutoff = now_epoch - window_seconds
    recent_count = sum(count for effective_end, count in demand_buckets
                       if effective_end > cutoff)
    # Per-URL HTTP envelopes are disjoint across LB processes and async
    # occupancy is replica-global/max-selected by LbSessionLedger. The local
    # admission count overlaps HTTP dispatch and therefore remains a drain
    # proof, not another additive UI demand unit.
    confirmed_in_flight = sum(aggregate.in_flight.values())
    unknown_in_flight_replica_count = len(aggregate.unknown_urls)
    in_flight = (None
                 if unknown_in_flight_replica_count else confirmed_in_flight)
    reason = ('compatibility_incomplete' if not complete else
              'in_flight_incomplete' if aggregate.unknown_urls else 'complete')
    return {
        'request_telemetry_source': 'postgresql_lb_demand_reports',
        'request_telemetry_state': 'fresh',
        'request_telemetry_reason': reason,
        'request_telemetry_generation': int(generation or 0),
        'request_telemetry_compatibility_complete': complete,
        'request_reporter_count': len(rows),
        'recent_request_count': recent_count,
        'request_window_seconds': window_seconds,
        'requests_per_second': recent_count / window_seconds,
        'in_flight_requests': in_flight,
        # Keep the exact gauge nullable when any routed replica lacks a
        # current occupancy proof, but retain the independently proven lower
        # bound and its coverage gap for honest operator observability.
        'confirmed_in_flight_requests': confirmed_in_flight,
        'unknown_in_flight_replica_count': unknown_in_flight_replica_count,
        'request_queue_depth': queue_depth,
        'rejected_requests': rejected,
        'recent_rejected_requests': recent_rejected,
        'request_stats_age_seconds': max(
            0.0, (now - newest_received_at).total_seconds()),
    }


def get_request_summary(service_name: str, service_hash: str) -> dict[str, Any]:
    """Return a provider- and controller-free current request projection."""
    try:
        engine = _postgres_engine()
    except DemandReportUnavailable:
        return _empty_summary('unavailable', 'postgresql_required')
    reports = demand_state_schema.serve_lb_demand_reports_table
    generations = demand_state_schema.serve_demand_feed_generations_table
    services = serve_state_schema.services_table
    try:
        with engine.connect() as connection:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            service = connection.execute(
                sqlalchemy.select(services.c.hash).where(
                    services.c.name == service_name)).scalar_one_or_none()
            if service != service_hash:
                return _empty_summary('unavailable',
                                      'service_incarnation_mismatch')
            generation = connection.execute(
                sqlalchemy.select(generations.c.generation).where(
                    generations.c.service_name == service_name,
                    generations.c.service_hash ==
                    service_hash)).scalar_one_or_none()
            rows = connection.execute(
                sqlalchemy.select(reports.c.reporter_session_id,
                                  reports.c.lb_session_id, reports.c.lb_slot,
                                  reports.c.reporter_observed_at,
                                  reports.c.received_at, reports.c.valid_until,
                                  reports.c.complete, reports.c.payload).where(
                                      reports.c.service_name == service_name,
                                      reports.c.service_hash ==
                                      service_hash)).mappings().all()
    except sqlalchemy.exc.SQLAlchemyError:
        return _empty_summary('unavailable', 'database_read_failed')
    if not rows:
        return _empty_summary('unavailable',
                              'no_report_received',
                              generation=generation)
    fresh = [row for row in rows if row['valid_until'] > now]
    if not fresh:
        return _empty_summary('stale',
                              'all_reports_expired',
                              generation=generation)
    try:
        return _aggregate_fresh_reports(fresh, generation, now)
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
        return _empty_summary('unavailable',
                              'invalid_durable_payload',
                              generation=generation)


def get_autoscaling_snapshot(
    service_name: str,
    service_hash: str,
    *,
    connection: sqlalchemy.engine.Connection | None = None,
    _read_started_monotonic: float | None = None,
) -> DurableAutoscalingSnapshot | None:
    """Read the sole promoted demand source and translate its exact routes.

    Unlike the public summary, this read fails closed on every incomplete
    reporter, mixed route generation, unknown URL identity, or source-mode
    mismatch. It never calls a provider or controller.  A caller that already
    owns the canonical PostgreSQL locks may provide its transaction so this
    same normalizer can prove demand semantics at an effect boundary.
    """
    read_started_monotonic = (_read_started_monotonic if _read_started_monotonic
                              is not None else time.monotonic())
    if connection is None:
        engine = _postgres_engine()
        with engine.connect().execution_options(
                isolation_level='REPEATABLE READ') as owned_connection:
            with owned_connection.begin():
                owned_connection.exec_driver_sql('SET TRANSACTION READ ONLY')
                return get_autoscaling_snapshot(
                    service_name,
                    service_hash,
                    connection=owned_connection,
                    _read_started_monotonic=read_started_monotonic)

    reports_table = demand_state_schema.serve_lb_demand_reports_table
    generations = demand_state_schema.serve_demand_feed_generations_table
    routes = route_projection_schema.serve_route_snapshots_table
    route_heads = route_projection_schema.serve_route_heads_table
    services = serve_state_schema.services_table
    now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    service = connection.execute(
        sqlalchemy.select(services).where(
            services.c.name == service_name)).mappings().one_or_none()
    if (service is None or service['hash'] != service_hash or
            service['pool'] != 0 or
            service['demand_source_mode'] != 'DURABLE_FEED' or
            service['demand_authority_capable'] is not True or
            service['demand_authority_controller_incarnation']
            != service['controller_incarnation'] or
            service['demand_authority_protocol_version'] != 1 or
            service['route_source_mode'] != 'DURABLE_PROJECTED' or
            service['route_projection_capable'] is not True or
            service['route_projection_controller_incarnation']
            != service['controller_incarnation'] or
            service['route_projection_protocol_version'] not in (1, 2)):
        return None
    generation = connection.execute(
        sqlalchemy.select(generations.c.generation).where(
            generations.c.service_name == service_name,
            generations.c.service_hash == service_hash)).scalar_one_or_none()
    head = connection.execute(
        sqlalchemy.select(route_heads).where(
            route_heads.c.service_name ==
            service_name)).mappings().one_or_none()
    if generation is None or head is None or head['valid_until'] <= now:
        return None
    route = connection.execute(
        sqlalchemy.select(routes).where(
            routes.c.service_name == service_name, routes.c.generation ==
            head['generation'])).mappings().one_or_none()
    rows = connection.execute(
        sqlalchemy.select(reports_table).where(
            reports_table.c.service_name == service_name,
            reports_table.c.service_hash == service_hash,
            reports_table.c.valid_until > now).order_by(
                reports_table.c.reporter_session_id)).mappings().all()
    if (route is None or route['service_hash'] != service_hash or
            route['service_lifecycle_epoch'] != service['lifecycle_epoch'] or
            route['controller_incarnation'] != service['controller_incarnation']
            or route['service_version'] != service['current_version'] or
            route['protocol_version'] != 1 or not rows or
            route['producer_protocol_version']
            != service['route_projection_protocol_version']):
        return None
    try:
        _, identities = (route_projection.RouteProjectionRepository.
                         validate_snapshot_row(route))
    except route_projection.RouteProjectionError:
        return None
    if not route_projection.snapshot_owner_matches(route, service):
        return None
    route_generation = int(head['generation'])
    route_sha256 = route['content_sha256']
    route_epoch = int(service['route_source_epoch'])
    ledger = lb_ha.LbSessionLedger(constants.LB_DEMAND_REPORT_TTL_SECONDS,
                                   constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS)
    if not reports_match_current_lb_authority(rows, service):
        return None
    stream_owners: set[str] = set()
    watermark: list[dict[str, Any]] = []
    timestamps: list[float] = []
    compatibility_profiles: list[dict[str, Any]] = []
    queued_profiles: list[dict[str, Any]] = []
    rejected_profiles: list[dict[str, Any]] = []
    queue_depth = 0
    rejected = 0
    recent_rejected = 0
    queue_by_priority: dict[str, int] = {}
    rejected_by_priority: dict[str, int] = {}
    recent_rejected_by_priority: dict[str, int] = {}
    observed_slots_by_url: dict[str, int] = {}
    optional_counts = {
        key: 0
        for key in ('unique_job_arrivals_60s', 'unique_job_arrivals_300s',
                    'headerless_arrivals_60s', 'headerless_arrivals_300s')
    }
    configured_accelerators: tuple[str, ...] | None = None
    compatibility_complete = True
    aggregate_window_covered = True
    offered_arrival_tracking_saturated = False
    now_epoch = now.timestamp()

    def _add_counts(target: dict[str, int], raw: Any) -> bool:
        if not isinstance(raw, dict):
            return False
        for key, count in raw.items():
            if not isinstance(count, int) or isinstance(count,
                                                        bool) or count < 0:
                return False
            target[str(key)] = target.get(str(key), 0) + count
        return True

    for row in rows:
        payload = row['payload']
        if (not isinstance(payload, dict) or
                payload.get('protocol_version') != 2 or
                payload.get('routing_version') != service['current_version'] or
                payload.get('route_projection_generation') != route_generation
                or payload.get('route_projection_sha256') != route_sha256 or
                payload.get('route_source_epoch') != route_epoch):
            return None
        compatibility_complete = (compatibility_complete and
                                  row['complete'] is True)
        configured = tuple(payload.get('configured_accelerators', ()))
        if configured_accelerators is None:
            configured_accelerators = configured
        elif configured != configured_accelerators:
            return None
        session = str(row['reporter_session_id'])
        role = lb_ha.LbRole(payload['applied_role'])
        if role in (lb_ha.LbRole.ACTIVE, lb_ha.LbRole.DRAINING):
            stream_owners.add(session)
        elapsed = max(0.0, now_epoch - row['received_at'].timestamp())
        ledger_payload = dict(payload)
        ledger_payload['occupancy_sample_age_seconds'] = {
            url: float(age) + elapsed
            for url, age in payload['occupancy_sample_age_seconds'].items()
        }
        slot = lb_ha.parse_slot(row['lb_slot']) or lb_ha.LbSlot.A
        if not ledger.update(session,
                             slot,
                             role,
                             int(payload['applied_generation']),
                             ledger_payload,
                             now=now_epoch):
            return None
        watermark.append({
            'reporter_session_id': session,
            'sequence': int(row['sequence']),
            'payload_sha256': row['payload_sha256'],
        })
        reporter_epoch = row['reporter_observed_at'].timestamp()
        window = payload['demand_window']
        bucket_seconds = int(window['bucket_seconds'])
        coverage_started_at = window.get('coverage_started_at')
        aggregate_window_covered = bool(
            aggregate_window_covered and isinstance(coverage_started_at,
                                                    (int, float)) and
            not isinstance(coverage_started_at, bool) and
            float(coverage_started_at)
            <= float(payload['reporter_observed_at']) -
            int(window['window_seconds']) and
            not window.get('saturated', True) and
            not payload.get('offered_arrival_tracking_saturated', True))
        offered_arrival_tracking_saturated = bool(
            offered_arrival_tracking_saturated or window.get('saturated') or
            payload.get('offered_arrival_tracking_saturated'))
        for bucket in window['buckets']:
            effective_end = (row['received_at'].timestamp() +
                             int(bucket['bucket_start']) + bucket_seconds -
                             reporter_epoch)
            if effective_end <= now_epoch - int(window['window_seconds']):
                continue
            count = int(bucket['request_count'])
            if len(timestamps) + count > constants.LB_REQUEST_TIMESTAMP_CAP:
                return None
            timestamps.extend([effective_end] * count)
            for raw_profile in bucket['compatibility_profiles']:
                profile = dict(raw_profile)
                profile['timestamp'] = effective_end
                compatibility_profiles.append(profile)
        queue_depth += int(payload['queue_depth'])
        rejected += int(payload['rejected_in_window'])
        recent_rejected += int(payload['rejected_in_recent_window'])
        queued_profiles.extend(payload['queued_requests_by_compatibility'])
        rejected_profiles.extend(payload['rejected_requests_by_compatibility'])
        if (not _add_counts(queue_by_priority,
                            payload['queue_depth_by_priority']) or
                not _add_counts(rejected_by_priority,
                                payload['rejected_in_window_by_priority']) or
                not _add_counts(
                    recent_rejected_by_priority,
                    payload['rejected_in_recent_window_by_priority'])):
            return None
        for key in optional_counts:
            optional_counts[key] += int(payload[key])
        for url, slots in payload['total_slots_by_url'].items():
            observed_slots_by_url[url] = max(observed_slots_by_url.get(url, 0),
                                             int(slots))
    if not stream_owners:
        return None
    aggregate = ledger.aggregate(stream_owners, now=now_epoch)
    if not aggregate.complete:
        return None
    fresh_aggregate_zero = bool(
        service['route_projection_protocol_version'] == 2 and
        aggregate_window_covered and
        reports_prove_fresh_aggregate_zero(rows) and not timestamps and
        queue_depth == 0 and rejected == 0 and recent_rejected == 0)
    if not compatibility_complete and not fresh_aggregate_zero:
        return None
    remaining_validity_seconds = min(
        [(head['valid_until'] - now).total_seconds()] +
        [(row['valid_until'] - now).total_seconds() for row in rows])
    for age_seconds in aggregate.occupancy_sample_ages_seconds.values():
        remaining_validity_seconds = min(
            remaining_validity_seconds,
            constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS - age_seconds)
    authority_deadline = (read_started_monotonic +
                          max(0.0, remaining_validity_seconds))
    # Query and translation time consumes the original PostgreSQL allowance.
    # It must never be replaced with a fresh process-local receive timestamp.
    if remaining_validity_seconds <= 0 or time.monotonic(
    ) >= authority_deadline:
        return None
    controller_pid = service['controller_pid']
    controller_ip = service['controller_ip']
    controller_owner_epoch = service['controller_owner_epoch']
    if (not isinstance(controller_pid, int) or
            isinstance(controller_pid, bool) or controller_pid < 1 or
            not isinstance(controller_ip, str) or not controller_ip or
            not isinstance(controller_owner_epoch, int) or
            isinstance(controller_owner_epoch, bool) or
            controller_owner_epoch < 1):
        return None
    authority_valid_until = now + datetime.timedelta(
        seconds=remaining_validity_seconds)

    def _replica_id(url: str) -> int | None:
        identity = identities.get(url)
        if (not isinstance(identity, dict) or
                not isinstance(identity.get('replica_id'), int) or
                not isinstance(identity.get('replica_record_id'), str)):
            return None
        return int(identity['replica_id'])

    translated_in_flight: dict[int, int] = {}
    for url, count in aggregate.in_flight.items():
        replica_id = _replica_id(url)
        if replica_id is None:
            return None
        translated_in_flight[replica_id] = (
            translated_in_flight.get(replica_id, 0) + int(count))
    unknown_ids: set[int] = set()
    for url in aggregate.unknown_urls:
        replica_id = _replica_id(url)
        if replica_id is None:
            return None
        unknown_ids.add(replica_id)
    observed_slots: dict[int, int] = {}
    for url, slots in observed_slots_by_url.items():
        replica_id = _replica_id(url)
        if replica_id is None:
            return None
        observed_slots[replica_id] = max(observed_slots.get(replica_id, 0),
                                         slots)
    request_information = {
        'timestamps': sorted(timestamps),
        'compatibility_profiles': compatibility_profiles,
        'queued_requests_by_compatibility': queued_profiles,
        'rejected_requests_by_compatibility': rejected_profiles,
        'compatibility_demand_complete': compatibility_complete,
        'fresh_aggregate_zero': fresh_aggregate_zero,
        'in_flight_by_replica_id': translated_in_flight,
        'unknown_in_flight_replica_ids': sorted(unknown_ids),
        'observed_slots_by_replica_id': observed_slots,
        'unknown_capacity_replica_ids': sorted(unknown_ids),
        'reconcile_generation': int(generation),
        'queue_depth': queue_depth,
        'queue_depth_by_priority': queue_by_priority,
        'rejected_in_window': rejected,
        'rejected_in_recent_window': recent_rejected,
        'rejected_in_window_by_priority': rejected_by_priority,
        'rejected_in_recent_window_by_priority': recent_rejected_by_priority,
        'offered_arrival_tracking_saturated': offered_arrival_tracking_saturated,
        **optional_counts,
    }

    def _compatibility_totals(
        profiles: list[dict[str, Any]],) -> list[dict[str, Any]]:
        totals: dict[tuple[int, tuple[str, ...]], tuple[int, int]] = {}
        for profile in profiles:
            key = (
                int(profile['priority']),
                tuple(str(card) for card in profile['compatible_accelerators']))
            count, recent_count = totals.get(key, (0, 0))
            totals[key] = (count + int(profile.get('count', 1)),
                           recent_count + int(profile.get('recent_count', 0)))
        return [{
            'priority': priority,
            'compatible_accelerators': list(accelerators),
            'count': count,
            'recent_count': recent_count,
        } for (priority,
               accelerators), (count, recent_count) in sorted(totals.items())]

    # ``normalized_demand`` crosses a JSONB boundary before paid admission
    # compares it with a freshly reconstructed snapshot.  JSON object keys are
    # strings, while ``request_information`` intentionally keeps integer
    # replica IDs for autoscaler/runtime use.  Canonicalize only the durable
    # semantic projection here.  Otherwise IDs such as 42 and 116 sort
    # numerically before persistence but lexicographically afterwards, making
    # identical demand look different and permanently fencing paid launches.
    normalized_in_flight = {
        str(replica_id): translated_in_flight[replica_id]
        for replica_id in sorted(translated_in_flight)
    }
    normalized_demand = {
        'configured_accelerators': list(configured_accelerators or ()),
        'recent_request_count': len(timestamps),
        'in_flight_by_replica_id': normalized_in_flight,
        'unknown_in_flight_replica_ids': sorted(unknown_ids),
        'queue_depth': queue_depth,
        'rejected_in_window': rejected,
        'recent_rejected_in_window': recent_rejected,
        'fresh_aggregate_zero': fresh_aggregate_zero,
        # Exclude translated event timestamps: receipt time corrects reporter
        # clock skew, so those floats may move slightly on a heartbeat even
        # while the bounded demand classes and target are unchanged.
        'compatibility_demand': {
            'arrivals': _compatibility_totals(compatibility_profiles),
            'queued': _compatibility_totals(queued_profiles),
            'rejected': _compatibility_totals(rejected_profiles),
        },
    }
    reconcile_authority = DurableReconcileAuthority(
        service_name=service_name,
        service_hash=service_hash,
        service_lifecycle_epoch=int(service['lifecycle_epoch']),
        service_version=int(service['current_version']),
        controller_incarnation=str(service['controller_incarnation']),
        controller_owner_epoch=controller_owner_epoch,
        controller_pid=controller_pid,
        controller_ip=controller_ip,
        demand_source_epoch=int(service['demand_source_epoch']),
        demand_feed_generation=int(generation),
        receipt_watermark=tuple(
            (str(item['reporter_session_id']), int(item['sequence']),
             str(item['payload_sha256'])) for item in watermark),
        route_generation=route_generation,
        route_sha256=str(route_sha256),
        route_source_epoch=route_epoch,
        lb_ha_enabled=service['lb_ha_enabled'] == 1,
        lb_active_slot=service['lb_active_slot'],
        lb_cutover_generation=int(service['lb_cutover_generation']),
        fresh_aggregate_zero=fresh_aggregate_zero,
        occupancy_sampled_urls=tuple(aggregate.occupancy_sampled_urls),
        valid_until=authority_valid_until,
        read_started_monotonic=read_started_monotonic,
        deadline_monotonic=authority_deadline)
    return DurableAutoscalingSnapshot(service_name=service_name,
                                      service_hash=service_hash,
                                      demand_source_epoch=int(
                                          service['demand_source_epoch']),
                                      demand_feed_generation=int(generation),
                                      route_generation=route_generation,
                                      route_sha256=str(route_sha256),
                                      route_source_epoch=route_epoch,
                                      receipt_watermark=watermark,
                                      request_information=request_information,
                                      normalized_demand=normalized_demand,
                                      fresh_aggregate_zero=fresh_aggregate_zero,
                                      reconcile_authority=reconcile_authority)
