"""Authority and accounting primitives for SkyServe LB warm standby.

This module is deliberately free of Kubernetes and database clients.  The
controller and lifecycle reconciler provide fenced observations, while these
types keep role, promotion, drain, and demand-handoff rules deterministic and
unit-testable.
"""
from __future__ import annotations

import dataclasses
import enum
import math
import time
from typing import Any

from sky.serve import constants


class LbSlot(str, enum.Enum):
    A = 'a'
    B = 'b'

    @property
    def other(self) -> LbSlot:
        return LbSlot.B if self is LbSlot.A else LbSlot.A


class LbRole(str, enum.Enum):
    STANDBY = 'STANDBY'
    ARMED = 'ARMED'
    ACTIVE = 'ACTIVE'
    DRAINING = 'DRAINING'


class LbCutoverPhase(str, enum.Enum):
    MIGRATING = 'MIGRATING'
    STABLE = 'STABLE'
    PREPARING = 'PREPARING'
    DRAINING = 'DRAINING'
    ROLLING_BACK = 'ROLLING_BACK'


@dataclasses.dataclass(frozen=True)
class LbCutoverState:
    """Durable cutover state copied from the service row."""

    enabled: bool
    active_slot: LbSlot | None
    generation: int
    pending_slot: LbSlot | None
    phase: LbCutoverPhase
    lifecycle_epoch: int | None
    drain_started_at: float | None = None

    def role_for(self, slot: LbSlot) -> LbRole:
        if not self.enabled or self.active_slot is None:
            return LbRole.ACTIVE
        if self.phase is LbCutoverPhase.PREPARING and slot == self.pending_slot:
            return LbRole.ARMED
        if self.phase is LbCutoverPhase.DRAINING and slot == self.pending_slot:
            return LbRole.DRAINING
        if slot == self.active_slot:
            return LbRole.ACTIVE
        return LbRole.STANDBY


def parse_slot(value: Any) -> LbSlot | None:
    try:
        return LbSlot(value)
    except (TypeError, ValueError):
        return None


def parse_phase(value: Any) -> LbCutoverPhase | None:
    try:
        return LbCutoverPhase(value)
    except (TypeError, ValueError):
        return None


def occupancy_samples_are_promotable(
    required_urls: set[str],
    sample_generations: dict[str, Any],
    sample_ages_seconds: dict[str, Any],
    max_age_seconds: float,
) -> bool:
    """Whether every async replica has a current, generation-valid sample."""
    if not math.isfinite(max_age_seconds) or max_age_seconds < 0:
        return False
    for url in required_urls:
        generation = sample_generations.get(url)
        age = sample_ages_seconds.get(url)
        if (not isinstance(generation, int) or isinstance(generation, bool) or
                generation < 0 or not isinstance(age, (int, float)) or
                isinstance(age, bool) or not math.isfinite(age) or age < 0 or
                age > max_age_seconds):
            return False
    return True


@dataclasses.dataclass(frozen=True)
class LbSessionReport:
    """One role-fenced Pod/process report retained during handoff."""

    session_id: str
    slot: LbSlot
    role: LbRole
    generation: int
    applied_role: LbRole | None
    applied_generation: int | None
    received_at: float
    local_in_flight: int
    http_in_flight: dict[str, int]
    async_occupancy: dict[str, int]
    occupancy_sample_generations: dict[str, int]
    occupancy_sample_ages_seconds: dict[str, float]
    routing_urls: frozenset[str]
    unknown_urls: frozenset[str]
    draining_urls: frozenset[str]


@dataclasses.dataclass(frozen=True)
class AggregatedDrainReport:
    """Service-wide drain view across ACTIVE and DRAINING sessions."""

    complete: bool
    local_in_flight: int
    http_in_flight: dict[str, int]
    in_flight: dict[str, int]
    routing_urls: list[str] | None
    unknown_urls: list[str]
    draining_urls: list[str]
    occupancy_sampled_urls: list[str]
    occupancy_sample_ages_seconds: dict[str, float]


class LbSessionLedger:
    """Bounded multi-session ledger for promotion-safe drain accounting."""

    def __init__(self, max_session_age_seconds: float,
                 max_occupancy_age_seconds: float) -> None:
        if max_session_age_seconds <= 0 or max_occupancy_age_seconds <= 0:
            raise ValueError('LB session and occupancy ages must be positive.')
        self._max_session_age_seconds = max_session_age_seconds
        self._max_occupancy_age_seconds = max_occupancy_age_seconds
        self._reports: dict[str, LbSessionReport] = {}

    @staticmethod
    def _nonnegative_map(raw: Any) -> dict[str, int] | None:
        if not isinstance(raw, dict):
            return None
        parsed: dict[str, int] = {}
        for url, value in raw.items():
            if (not isinstance(url, str) or not isinstance(value, int) or
                    isinstance(value, bool) or value < 0):
                return None
            parsed[url] = value
        return parsed

    @staticmethod
    def _generation_map(raw: Any) -> dict[str, int] | None:
        return LbSessionLedger._nonnegative_map(raw)

    @staticmethod
    def _age_map(raw: Any) -> dict[str, float] | None:
        if not isinstance(raw, dict):
            return None
        parsed: dict[str, float] = {}
        for url, value in raw.items():
            if (not isinstance(url, str) or not isinstance(value,
                                                           (int, float)) or
                    isinstance(value, bool) or not math.isfinite(value) or
                    value < 0):
                return None
            parsed[url] = float(value)
        return parsed

    def update(self,
               session_id: str,
               slot: LbSlot,
               role: LbRole,
               generation: int,
               request_data: dict[str, Any],
               now: float | None = None) -> bool:
        """Validate and retain one report. Invalid payloads fail closed."""
        if (not session_id or not isinstance(generation, int) or
                isinstance(generation, bool) or generation < 0):
            return False
        http_in_flight = self._nonnegative_map(
            request_data.get('http_in_flight'))
        local_in_flight = request_data.get('local_in_flight')
        async_occupancy = self._nonnegative_map(
            request_data.get('async_occupancy'))
        sample_generations = self._generation_map(
            request_data.get('occupancy_sample_generation'))
        sample_ages = self._age_map(
            request_data.get('occupancy_sample_age_seconds'))
        raw_applied_role = request_data.get('applied_role')
        raw_applied_generation = request_data.get('applied_generation')
        applied_role: LbRole | None = None
        applied_generation: int | None = None
        if raw_applied_role is not None or raw_applied_generation is not None:
            try:
                applied_role = LbRole(raw_applied_role)
            except (TypeError, ValueError):
                return False
            if (not isinstance(raw_applied_generation, int) or
                    isinstance(raw_applied_generation, bool) or
                    raw_applied_generation < 0):
                return False
            applied_generation = raw_applied_generation
        routing_urls = request_data.get('routing_urls')
        unknown_urls = request_data.get('unknown_in_flight_urls', [])
        draining_urls = request_data.get('draining_urls', [])
        if (http_in_flight is None or not isinstance(local_in_flight, int) or
                isinstance(local_in_flight, bool) or local_in_flight < 0 or
                async_occupancy is None or sample_generations is None or
                sample_ages is None or not isinstance(routing_urls, list) or
                not all(isinstance(url, str) for url in routing_urls) or
                not isinstance(unknown_urls, list) or
                not all(isinstance(url, str) for url in unknown_urls) or
                not isinstance(draining_urls, list) or
                not all(isinstance(url, str) for url in draining_urls)):
            return False
        # A numeric async value is authoritative only with matching freshness
        # metadata. Keeping the maps identical prevents a stale zero from
        # silently becoming a clean drain proof.
        if (set(async_occupancy) != set(sample_generations) or
                set(async_occupancy) != set(sample_ages)):
            return False
        received_at = time.monotonic() if now is None else now
        self._reports[session_id] = LbSessionReport(
            session_id=session_id,
            slot=slot,
            role=role,
            generation=generation,
            applied_role=applied_role,
            applied_generation=applied_generation,
            received_at=received_at,
            local_in_flight=local_in_flight,
            http_in_flight=http_in_flight,
            async_occupancy=async_occupancy,
            occupancy_sample_generations=sample_generations,
            occupancy_sample_ages_seconds=sample_ages,
            routing_urls=frozenset(routing_urls),
            unknown_urls=frozenset(unknown_urls),
            draining_urls=frozenset(draining_urls))
        return True

    def discard_dead(self, live_session_ids: set[str]) -> None:
        self._reports = {
            session_id: report
            for session_id, report in self._reports.items()
            if session_id in live_session_ids
        }

    def aggregate(
        self,
        stream_owner_session_ids: set[str],
        now: float | None = None,
        required_applied_role: LbRole | None = None,
        required_applied_generation: int | None = None
    ) -> AggregatedDrainReport:
        """Aggregate disjoint HTTP work and replica-global async occupancy."""
        if ((required_applied_role is None) != (required_applied_generation
                                                is None)):
            raise ValueError('Applied role and generation must be required '
                             'together.')
        current_time = time.monotonic() if now is None else now
        reports: list[LbSessionReport] = []
        for session_id in stream_owner_session_ids:
            report = self._reports.get(session_id)
            if (report is None or current_time - report.received_at
                    > self._max_session_age_seconds or
                (required_applied_role is not None and
                 (report.applied_role is not required_applied_role or
                  report.applied_generation != required_applied_generation))):
                return AggregatedDrainReport(complete=False,
                                             local_in_flight=0,
                                             http_in_flight={},
                                             in_flight={},
                                             routing_urls=None,
                                             unknown_urls=[],
                                             draining_urls=[],
                                             occupancy_sampled_urls=[],
                                             occupancy_sample_ages_seconds={})
            reports.append(report)

        local_in_flight = 0
        http_in_flight: dict[str, int] = {}
        routing_urls: set[str] = set()
        unknown_urls: set[str] = set()
        draining_urls: set[str] = set()
        # url -> (sample observed time, generation, count). Received-at minus
        # age makes samples from different slots comparable at the controller.
        freshest_async: dict[str, tuple[float, int, int]] = {}
        for report in reports:
            local_in_flight += report.local_in_flight
            routing_urls.update(report.routing_urls)
            unknown_urls.update(report.unknown_urls)
            draining_urls.update(report.draining_urls)
            for url, count in report.http_in_flight.items():
                http_in_flight[url] = http_in_flight.get(url, 0) + count
            for url, count in report.async_occupancy.items():
                age = report.occupancy_sample_ages_seconds[url]
                if age > self._max_occupancy_age_seconds:
                    unknown_urls.add(url)
                    continue
                observed_at = report.received_at - age
                candidate = (observed_at,
                             report.occupancy_sample_generations[url], count)
                existing = freshest_async.get(url)
                if existing is None or candidate[:2] > existing[:2]:
                    freshest_async[url] = candidate
                elif candidate[:2] == existing[:2] and count > existing[2]:
                    # Equal observations from two slots are the same backend
                    # work, never two additive jobs. Choose the conservative max.
                    freshest_async[url] = candidate

        in_flight = dict(http_in_flight)
        for url, (_, _, count) in freshest_async.items():
            in_flight[url] = in_flight.get(url, 0) + count
        return AggregatedDrainReport(
            complete=True,
            local_in_flight=local_in_flight,
            http_in_flight=http_in_flight,
            in_flight=in_flight,
            routing_urls=sorted(routing_urls),
            unknown_urls=sorted(unknown_urls),
            draining_urls=sorted(draining_urls),
            occupancy_sampled_urls=sorted(freshest_async),
            occupancy_sample_ages_seconds={
                url: max(0.0, current_time - observed_at)
                for url, (observed_at, _, _) in freshest_async.items()
            },
        )


@dataclasses.dataclass(frozen=True)
class CompatibilityDemand:
    """One validated, JSON-safe accelerator compatibility demand profile."""

    priority: int
    compatible_accelerators: tuple[str, ...]
    count: int
    timestamp: float | None = None
    recent_count: int | None = None

    @classmethod
    def from_dict(cls, value: Any, *,
                  require_timestamp: bool) -> CompatibilityDemand | None:
        if not isinstance(value, dict):
            return None
        priority = value.get('priority')
        accelerators = value.get('compatible_accelerators')
        count = value.get('count', 1)
        recent_count = value.get('recent_count')
        timestamp = value.get('timestamp')
        if (not isinstance(priority, int) or isinstance(priority, bool) or
                not isinstance(accelerators, list) or not accelerators or
                not all(
                    isinstance(card, str) and card for card in accelerators) or
                not isinstance(count, int) or isinstance(count, bool) or
                count < 1):
            return None
        if (recent_count is not None and
            (not isinstance(recent_count, int) or
             isinstance(recent_count, bool) or recent_count < 0 or
             recent_count > count)):
            return None
        if require_timestamp:
            if (not isinstance(timestamp,
                               (int, float)) or isinstance(timestamp, bool) or
                    not math.isfinite(timestamp)):
                return None
            normalized_timestamp: float | None = float(timestamp)
        else:
            normalized_timestamp = None
        return cls(priority, tuple(accelerators), count, normalized_timestamp,
                   recent_count)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            'priority': self.priority,
            'compatible_accelerators': list(self.compatible_accelerators),
            'count': self.count,
        }
        if self.timestamp is not None:
            value['timestamp'] = self.timestamp
        if self.recent_count is not None:
            value['recent_count'] = self.recent_count
        return value


@dataclasses.dataclass(frozen=True)
class QueueDeadlineDemand:
    """One bounded queue class with a remaining dispatch deadline."""

    priority: int
    compatible_accelerators: tuple[str, ...]
    remaining_seconds: int
    count: int

    @classmethod
    def from_dict(cls, value: Any) -> QueueDeadlineDemand | None:
        if not isinstance(value, dict):
            return None
        compatible = CompatibilityDemand.from_dict(
            {
                'priority': value.get('priority'),
                'compatible_accelerators': value.get('compatible_accelerators'),
                'count': value.get('count'),
            },
            require_timestamp=False)
        remaining = value.get('remaining_seconds')
        if (compatible is None or not isinstance(remaining, int) or
                isinstance(remaining, bool) or remaining < 0 or
                remaining > constants.LB_REQUEST_DEADLINE_MAX_SECONDS or
                remaining % constants.LB_REQUEST_DEADLINE_BUCKET_SECONDS != 0):
            return None
        return cls(compatible.priority, compatible.compatible_accelerators,
                   remaining, compatible.count)

    def to_dict(self) -> dict[str, Any]:
        return {
            'priority': self.priority,
            'compatible_accelerators': list(self.compatible_accelerators),
            'remaining_seconds': self.remaining_seconds,
            'count': self.count,
        }


@dataclasses.dataclass(frozen=True)
class DemandSnapshot:
    """Durable scale-down-safe evidence retained across one promotion."""

    timestamps: tuple[float, ...]
    queue_depth: int
    rejected_in_window: int
    in_flight: dict[str, int] = dataclasses.field(default_factory=dict)
    unknown_in_flight_urls: tuple[str, ...] = ()
    compatibility_profiles: tuple[CompatibilityDemand, ...] = ()
    queued_compatibility_profiles: tuple[CompatibilityDemand, ...] = ()
    queued_deadline_profiles: tuple[QueueDeadlineDemand, ...] | None = None
    rejected_compatibility_profiles: tuple[CompatibilityDemand, ...] = ()
    rejected_in_recent_window: int = 0
    # Compatibility profiles are meaningful only under the exact routing
    # catalog that admitted them.  None represents legacy/unfenced snapshots
    # and is deliberately not compatible with any handoff report.
    routing_version: int | None = None
    queue_depth_by_priority: dict[str,
                                  int] = dataclasses.field(default_factory=dict)
    rejected_in_window_by_priority: dict[str, int] = dataclasses.field(
        default_factory=dict)
    rejected_in_recent_window_by_priority: dict[str, int] = dataclasses.field(
        default_factory=dict)
    unique_job_arrivals_60s: int = 0
    unique_job_arrivals_300s: int = 0
    headerless_arrivals_60s: int = 0
    headerless_arrivals_300s: int = 0
    offered_arrival_tracking_saturated: bool = False

    @classmethod
    def from_request(cls, request_data: dict[str, Any]) -> DemandSnapshot:
        aggregator = request_data.get('request_aggregator', {})
        timestamps = aggregator.get('timestamps', []) if isinstance(
            aggregator, dict) else []
        valid_timestamps = tuple(
            value for value in timestamps if isinstance(value, (int, float)) and
            not isinstance(value, bool) and math.isfinite(value) and value >= 0)

        def _nonnegative(value: Any) -> int:
            return (value if isinstance(value, int) and
                    not isinstance(value, bool) and value >= 0 else 0)

        def _priority_map(value: Any) -> dict[str, int]:
            if not isinstance(value, dict):
                return {}
            result = {}
            for priority, count in value.items():
                try:
                    normalized_priority = int(priority)
                except (TypeError, ValueError):
                    continue
                if (not 0 <= normalized_priority <= 100 or
                        not isinstance(count, int) or isinstance(count, bool) or
                        count < 0):
                    continue
                result[str(normalized_priority)] = count
            return result

        raw_in_flight = request_data.get('in_flight')
        in_flight = ({
            url: value
            for url, value in raw_in_flight.items()
            if isinstance(url, str) and isinstance(value, int) and
            not isinstance(value, bool) and value >= 0
        } if isinstance(raw_in_flight, dict) else {})
        unknown = request_data.get('unknown_in_flight_urls', [])
        if not isinstance(unknown, list):
            unknown = []
        raw_profiles = (aggregator.get('compatibility_profiles', [])
                        if isinstance(aggregator, dict) else [])
        if not isinstance(raw_profiles, list):
            raw_profiles = []
        compatibility_profiles = tuple(
            profile for value in raw_profiles
            if (profile := CompatibilityDemand.from_dict(
                value, require_timestamp=True)) is not None)
        raw_queued_profiles = request_data.get(
            'queued_requests_by_compatibility', [])
        if not isinstance(raw_queued_profiles, list):
            raw_queued_profiles = []
        queued_compatibility_profiles = tuple(
            profile for value in raw_queued_profiles
            if (profile := CompatibilityDemand.from_dict(
                value, require_timestamp=False)) is not None)
        raw_deadline_profiles = request_data.get(
            'queued_request_deadline_buckets')
        queued_deadline_profiles = None
        if isinstance(raw_deadline_profiles, list):
            parsed_deadlines = tuple(
                deadline_profile for value in raw_deadline_profiles
                if (deadline_profile := QueueDeadlineDemand.from_dict(value)
                   ) is not None)
            if len(parsed_deadlines) == len(raw_deadline_profiles):
                queued_deadline_profiles = parsed_deadlines
        raw_rejected_profiles = request_data.get(
            'rejected_requests_by_compatibility', [])
        if not isinstance(raw_rejected_profiles, list):
            raw_rejected_profiles = []
        rejected_compatibility_profiles = tuple(
            profile for value in raw_rejected_profiles
            if (profile := CompatibilityDemand.from_dict(
                value, require_timestamp=False)) is not None)
        raw_routing_version = request_data.get('routing_version')
        routing_version = (raw_routing_version
                           if isinstance(raw_routing_version, int) and
                           not isinstance(raw_routing_version, bool) and
                           raw_routing_version >= 0 else None)
        return cls(
            timestamps=valid_timestamps,
            queue_depth=_nonnegative(request_data.get('queue_depth')),
            rejected_in_window=_nonnegative(
                request_data.get('rejected_in_window')),
            in_flight=in_flight,
            unknown_in_flight_urls=tuple(
                sorted(value for value in unknown if isinstance(value, str))),
            compatibility_profiles=compatibility_profiles,
            queued_compatibility_profiles=queued_compatibility_profiles,
            queued_deadline_profiles=queued_deadline_profiles,
            rejected_compatibility_profiles=rejected_compatibility_profiles,
            rejected_in_recent_window=_nonnegative(
                request_data.get('rejected_in_recent_window')),
            routing_version=routing_version,
            queue_depth_by_priority=_priority_map(
                request_data.get('queue_depth_by_priority')),
            rejected_in_window_by_priority=_priority_map(
                request_data.get('rejected_in_window_by_priority')),
            rejected_in_recent_window_by_priority=_priority_map(
                request_data.get('rejected_in_recent_window_by_priority')),
            unique_job_arrivals_60s=_nonnegative(
                request_data.get('unique_job_arrivals_60s')),
            unique_job_arrivals_300s=_nonnegative(
                request_data.get('unique_job_arrivals_300s')),
            headerless_arrivals_60s=_nonnegative(
                request_data.get('headerless_arrivals_60s')),
            headerless_arrivals_300s=_nonnegative(
                request_data.get('headerless_arrivals_300s')),
            offered_arrival_tracking_saturated=request_data.get(
                'offered_arrival_tracking_saturated') is True,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            'timestamps': list(self.timestamps),
            'queue_depth': self.queue_depth,
            'rejected_in_window': self.rejected_in_window,
            'rejected_in_recent_window': self.rejected_in_recent_window,
            'in_flight': self.in_flight,
            'unknown_in_flight_urls': list(self.unknown_in_flight_urls),
            'compatibility_profiles': [
                profile.to_dict() for profile in self.compatibility_profiles
            ],
            'queued_requests_by_compatibility': [
                profile.to_dict()
                for profile in self.queued_compatibility_profiles
            ],
            'queued_request_deadline_buckets': ([
                profile.to_dict() for profile in self.queued_deadline_profiles
            ] if self.queued_deadline_profiles is not None else None),
            'rejected_requests_by_compatibility': [
                profile.to_dict()
                for profile in self.rejected_compatibility_profiles
            ],
            'routing_version': self.routing_version,
            'queue_depth_by_priority': self.queue_depth_by_priority,
            'rejected_in_window_by_priority':
                self.rejected_in_window_by_priority,
            'rejected_in_recent_window_by_priority':
                self.rejected_in_recent_window_by_priority,
            'unique_job_arrivals_60s': self.unique_job_arrivals_60s,
            'unique_job_arrivals_300s': self.unique_job_arrivals_300s,
            'headerless_arrivals_60s': self.headerless_arrivals_60s,
            'headerless_arrivals_300s': self.headerless_arrivals_300s,
            'offered_arrival_tracking_saturated':
                self.offered_arrival_tracking_saturated,
        }

    @classmethod
    def from_dict(cls, value: Any) -> DemandSnapshot | None:
        if not isinstance(value, dict):
            return None
        snapshot = cls.from_request({
            'request_aggregator': {
                'timestamps': value.get('timestamps', []),
                'compatibility_profiles': value.get('compatibility_profiles',
                                                    []),
            },
            'queue_depth': value.get('queue_depth'),
            'rejected_in_window': value.get('rejected_in_window'),
            'rejected_in_recent_window': value.get('rejected_in_recent_window'),
            'in_flight': value.get('in_flight'),
            'unknown_in_flight_urls': value.get('unknown_in_flight_urls'),
            'queued_requests_by_compatibility':
                value.get('queued_requests_by_compatibility'),
            'queued_request_deadline_buckets':
                value.get('queued_request_deadline_buckets'),
            'rejected_requests_by_compatibility':
                value.get('rejected_requests_by_compatibility'),
            'routing_version': value.get('routing_version'),
            'queue_depth_by_priority': value.get('queue_depth_by_priority'),
            'rejected_in_window_by_priority':
                value.get('rejected_in_window_by_priority'),
            'rejected_in_recent_window_by_priority':
                value.get('rejected_in_recent_window_by_priority'),
            'unique_job_arrivals_60s': value.get('unique_job_arrivals_60s'),
            'unique_job_arrivals_300s': value.get('unique_job_arrivals_300s'),
            'headerless_arrivals_60s': value.get('headerless_arrivals_60s'),
            'headerless_arrivals_300s': value.get('headerless_arrivals_300s'),
            'offered_arrival_tracking_saturated':
                value.get('offered_arrival_tracking_saturated'),
        })
        return snapshot

    def floor(self,
              request_data: dict[str, Any],
              *,
              include_arrivals: bool = True) -> dict[str, Any]:
        """Return a copy with scale-up-safe demand floors applied."""
        merged = dict(request_data)
        current = DemandSnapshot.from_request(request_data)
        same_compatibility_epoch = (self.routing_version is not None and
                                    self.routing_version
                                    == current.routing_version)
        if include_arrivals:
            # Arrival samples are events, not gauges. Transfer the old-active
            # batch exactly once; DemandHandoff keeps the remaining gauges
            # floored without replaying these events on every heartbeat.
            compatibility_profiles = (list(self.compatibility_profiles)
                                      if same_compatibility_epoch else [])
            known_profiles = set(compatibility_profiles)
            for profile in current.compatibility_profiles:
                if profile not in known_profiles:
                    compatibility_profiles.append(profile)
                    known_profiles.add(profile)
            merged['request_aggregator'] = {
                'timestamps':
                    sorted(set(self.timestamps) | set(current.timestamps)),
                'compatibility_profiles': [
                    profile.to_dict() for profile in compatibility_profiles
                ],
            }
        queued_profiles = ({
            (profile.priority, profile.compatible_accelerators): profile
            for profile in self.queued_compatibility_profiles
        } if same_compatibility_epoch else {})
        for profile in current.queued_compatibility_profiles:
            key = (profile.priority, profile.compatible_accelerators)
            previous = queued_profiles.get(key)
            if previous is None or profile.count > previous.count:
                queued_profiles[key] = profile
        merged['queued_requests_by_compatibility'] = [
            profile.to_dict() for profile in queued_profiles.values()
        ]
        rejected_profiles = ({
            (profile.priority, profile.compatible_accelerators): profile
            for profile in self.rejected_compatibility_profiles
        } if same_compatibility_epoch else {})
        for profile in current.rejected_compatibility_profiles:
            key = (profile.priority, profile.compatible_accelerators)
            previous = rejected_profiles.get(key)
            if previous is None:
                rejected_profiles[key] = profile
            else:
                rejected_profiles[key] = CompatibilityDemand(
                    priority=profile.priority,
                    compatible_accelerators=profile.compatible_accelerators,
                    count=max(previous.count, profile.count),
                    recent_count=max(previous.recent_count or 0,
                                     profile.recent_count or 0))
        merged['rejected_requests_by_compatibility'] = [
            profile.to_dict() for profile in rejected_profiles.values()
        ]
        merged['queue_depth'] = max(self.queue_depth, current.queue_depth)
        if (merged['queue_depth'] == current.queue_depth and
                current.queued_deadline_profiles is not None and
                sum(profile.count
                    for profile in current.queued_deadline_profiles)
                == current.queue_depth):
            merged['queued_request_deadline_buckets'] = [
                profile.to_dict()
                for profile in current.queued_deadline_profiles
            ]
        else:
            merged['queued_request_deadline_buckets'] = None
        merged['rejected_in_window'] = max(self.rejected_in_window,
                                           current.rejected_in_window)
        merged['rejected_in_recent_window'] = max(
            self.rejected_in_recent_window, current.rejected_in_recent_window)

        def _merge_map(old: dict[str, int], new: dict[str,
                                                      int]) -> dict[str, int]:
            keys = set(old) | set(new)
            return {key: max(old.get(key, 0), new.get(key, 0)) for key in keys}

        merged['queue_depth_by_priority'] = _merge_map(
            self.queue_depth_by_priority, current.queue_depth_by_priority)
        merged['rejected_in_window_by_priority'] = _merge_map(
            self.rejected_in_window_by_priority,
            current.rejected_in_window_by_priority)
        merged['rejected_in_recent_window_by_priority'] = _merge_map(
            self.rejected_in_recent_window_by_priority,
            current.rejected_in_recent_window_by_priority)
        for field in ('unique_job_arrivals_60s', 'unique_job_arrivals_300s',
                      'headerless_arrivals_60s', 'headerless_arrivals_300s'):
            merged[field] = max(getattr(self, field), getattr(current, field))
        merged['offered_arrival_tracking_saturated'] = (
            self.offered_arrival_tracking_saturated or
            current.offered_arrival_tracking_saturated)
        # Controller-internal marker. This never crosses the LB wire.
        merged['pressure_report_is_floored'] = True
        merged['in_flight'] = {
            url: max(count, current.in_flight.get(url, 0))
            for url, count in self.in_flight.items()
        }
        for url, count in current.in_flight.items():
            merged['in_flight'].setdefault(url, count)
        merged['unknown_in_flight_urls'] = sorted(
            set(self.unknown_in_flight_urls) |
            set(current.unknown_in_flight_urls))
        return merged


class DemandHandoff:
    """Temporary old-active demand floor across one promotion generation."""

    def __init__(self, handoff_seconds: float) -> None:
        if handoff_seconds < 0 or not math.isfinite(handoff_seconds):
            raise ValueError('Demand handoff duration must be finite and >= 0.')
        self._handoff_seconds = handoff_seconds
        self._generation: int | None = None
        self._snapshot: DemandSnapshot | None = None
        self._complete_report_at: float | None = None
        self._arrivals_applied = False

    def begin(self, generation: int, snapshot: DemandSnapshot | None) -> None:
        self._generation = generation
        self._snapshot = snapshot
        self._complete_report_at = None
        self._arrivals_applied = False

    def restore(self, generation: int | None, snapshot: DemandSnapshot | None,
                complete_report_at: float | None) -> None:
        if generation != self._generation or snapshot != self._snapshot:
            self._arrivals_applied = False
        self._generation = generation
        self._snapshot = snapshot
        self._complete_report_at = complete_report_at

    @property
    def generation(self) -> int | None:
        return self._generation

    @property
    def complete_report_at(self) -> float | None:
        return self._complete_report_at

    @property
    def snapshot(self) -> DemandSnapshot | None:
        return self._snapshot

    def apply(self,
              generation: int,
              request_data: dict[str, Any],
              complete_authoritative_report: bool,
              now: float | None = None) -> dict[str, Any]:
        current_time = time.time() if now is None else now
        if generation != self._generation or self._snapshot is None:
            return request_data
        if complete_authoritative_report and self._complete_report_at is None:
            self._complete_report_at = current_time
        if (self._complete_report_at is not None and
                current_time - self._complete_report_at
                >= self._handoff_seconds):
            self._generation = None
            self._snapshot = None
            self._complete_report_at = None
            self._arrivals_applied = False
            return request_data
        floored = self._snapshot.floor(
            request_data, include_arrivals=not self._arrivals_applied)
        self._arrivals_applied = True
        return floored
