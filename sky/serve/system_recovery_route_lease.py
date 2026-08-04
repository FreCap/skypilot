"""Closed route-lease protocol for same-VM SkyServe recovery.

The controller manager owns :class:`ManagerRouteLeaseRegistry`; external load
balancers only receive its bounded marker and heartbeat projections.  A token
is freshness correlation, not application or recovery authority.
"""

from collections.abc import Callable
from collections.abc import Mapping
import copy
import dataclasses
import ipaddress
import math
import re
import threading
import time
from typing import Any
import urllib.parse
import uuid

from sky.serve import constants

_DECIMAL_REPLICA_ID = re.compile(r'[1-9][0-9]*\Z')
_ROUTE_TOKEN = re.compile(r'[0-9a-f]{32}\Z')
_DNS_LABEL = re.compile(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z')
_LEGACY_NUMERIC_HOST = re.compile(
    r'(?:0x[0-9a-f]+|[0-9]+)(?:\.(?:0x[0-9a-f]+|[0-9]+))*\Z', re.IGNORECASE)


class RouteLeaseError(ValueError):
    """A route-lease value violates the closed protocol."""


def _canonical_uuid(value: object,
                    name: str,
                    *,
                    optional: bool = False) -> str | None:
    if optional and value is None:
        return None
    if not isinstance(value, str):
        raise RouteLeaseError(f'{name} must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as e:
        raise RouteLeaseError(f'{name} must be a canonical UUID string.') from e
    if str(parsed) != value:
        raise RouteLeaseError(f'{name} must be a canonical UUID string.')
    return value


def canonical_replica_id(value: object) -> str:
    """Return the protocol's positive canonical decimal replica ID."""
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        value = str(value)
    if not isinstance(value,
                      str) or _DECIMAL_REPLICA_ID.fullmatch(value) is None:
        raise RouteLeaseError(
            'replica_id must be a positive canonical decimal string.')
    return value


def canonical_route_token(value: object) -> str:
    if not isinstance(value, str) or _ROUTE_TOKEN.fullmatch(value) is None:
        raise RouteLeaseError(
            'route_token must be 32 lowercase hexadecimal characters.')
    return value


def new_route_token() -> str:
    return uuid.uuid4().hex


def normalize_route_url(value: object) -> str:
    """Validate and transport-canonicalize a replica HTTP(S) base URL."""
    if not isinstance(value, str) or not value:
        raise RouteLeaseError('route_url must be a nonempty HTTP(S) base URL.')
    if (value != value.strip() or '?' in value or '#' in value or any(
            ord(character) < 0x20 or ord(character) == 0x7f
            for character in value)):
        raise RouteLeaseError(
            f'route_url must be an HTTP(S) base URL without credentials, path, query, or fragment: {value!r}'
        )
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError as e:
        raise RouteLeaseError(
            f'route_url has a malformed authority: {value!r}') from e
    scheme = parsed.scheme.lower()
    authority = parsed.netloc
    if (scheme not in ('http', 'https') or not authority or
            parsed.path not in ('', '/') or parsed.query or parsed.fragment or
            '@' in authority or '%' in authority or '\\' in authority):
        raise RouteLeaseError(
            f'route_url must be an HTTP(S) base URL without credentials, path, query, or fragment: {value!r}'
        )

    # urlsplit() deliberately accepts several ambiguous authorities, including
    # an empty port and an unbracketed IPv6 literal.  Reject those before using
    # its hostname/port conveniences.
    if authority.startswith('['):
        authority_pattern = r'\[[^\[\]]+\](?::[0-9]+)?\Z'
    else:
        authority_pattern = r'[^:]+(?::[0-9]+)?\Z'
    if re.fullmatch(authority_pattern, authority) is None:
        raise RouteLeaseError(f'route_url has a malformed authority: {value!r}')
    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as e:
        raise RouteLeaseError(
            f'route_url has a malformed authority: {value!r}') from e
    if hostname is None or port == 0:
        raise RouteLeaseError(f'route_url has a malformed authority: {value!r}')

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        try:
            canonical_host = hostname.encode('idna').decode('ascii').lower()
        except UnicodeError as e:
            raise RouteLeaseError(
                f'route_url has an invalid DNS hostname: {value!r}') from e
        labels = canonical_host.split('.')
        if (len(canonical_host) > 253 or not labels or
                any(_DNS_LABEL.fullmatch(label) is None for label in labels) or
                _LEGACY_NUMERIC_HOST.fullmatch(canonical_host) is not None):
            raise RouteLeaseError(
                f'route_url has an invalid DNS hostname: {value!r}') from None
    else:
        canonical_host = address.compressed.lower()
        if address.version == 6:
            canonical_host = f'[{canonical_host}]'

    default_port = 80 if scheme == 'http' else 443
    canonical_port = '' if port is None or port == default_port else f':{port}'
    return f'{scheme}://{canonical_host}{canonical_port}'


def normalize_probe_url(route_url: object, readiness_path: object) -> str:
    route = normalize_route_url(route_url)
    if (not isinstance(readiness_path, str) or
            not readiness_path.startswith('/') or '#' in readiness_path):
        raise RouteLeaseError(
            'readiness_path must be an absolute path without a fragment.')
    return f'{route}{readiness_path}'


@dataclasses.dataclass(frozen=True)
class RouteGeneration:
    """Exact controller/row/remote-attempt identity allowed to route."""

    controller_epoch: str
    replica_record_id: str
    event_id: str | None
    attempt_id: str
    recovery_state: str

    def __post_init__(self) -> None:
        _canonical_uuid(self.controller_epoch, 'controller_epoch')
        _canonical_uuid(self.replica_record_id, 'replica_record_id')
        _canonical_uuid(self.event_id, 'event_id', optional=True)
        _canonical_uuid(self.attempt_id, 'attempt_id')
        if self.recovery_state not in ('ARMED', 'RECOVERED'):
            raise RouteLeaseError('recovery_state must be ARMED or RECOVERED.')
        if self.recovery_state == 'ARMED' and self.event_id is not None:
            raise RouteLeaseError(
                'ARMED route generation cannot have an event_id.')
        if self.recovery_state == 'RECOVERED' and self.event_id is None:
            raise RouteLeaseError(
                'RECOVERED route generation requires an event_id.')


@dataclasses.dataclass(frozen=True)
class RouteProbeTarget:
    """Immutable request snapshot correlated to one probe epoch."""

    replica_id: int
    generation: RouteGeneration
    route_url: str
    probe_url: str
    method: str
    post_data: dict[str, Any] | None
    headers: dict[str, str] | None
    route_token: str
    # Defaults preserve the small direct-construction test/API surface while
    # registry-issued targets always carry the exact current epoch.
    probe_epoch: int = 0


@dataclasses.dataclass
class _TargetState:
    """Mutable lease state for one exact process-local route target."""

    target: RouteProbeTarget
    deadline: float
    active: bool = True
    activated: bool = False
    consecutive_failures: int = 0
    suspension_count: int = 0


@dataclasses.dataclass(frozen=True)
class RouteMarker:
    replica_id: str
    route_token: str


@dataclasses.dataclass(frozen=True)
class RouteSuspension:
    """Exact reversible omission held around one fenced durable mutation."""

    replica_id: int
    generation: RouteGeneration
    route_token: str
    probe_epoch: int = 0


class ManagerRouteLeaseRegistry:
    """Thread-safe, process-local owner of recovery route generations.

    Successful row admissions and fleet snapshots must be supplied in their
    authoritative manager-lock order.  Random UUID row identities have no
    intrinsic chronological order; bounded tombstones additionally reject
    delayed replays of row identities this process has already superseded.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._targets: dict[int, _TargetState] = {}
        # Each per-replica collection has at most one entry per admitted
        # numeric ID.  Old physical-row identities are additionally bounded
        # by one global cap, so exact stale-row rejection cannot grow without
        # limit across repeated row recreation.
        self._live_record_ids: dict[int, str] = {}
        self._retired: dict[int, RouteGeneration] = {}
        self._retired_record_ids: dict[int, set[str]] = {}
        # Exhausting the bounded exact old-row history fails closed only for
        # the numeric ID whose next recreation could no longer be ordered.
        self._blocked_record_ids: set[int] = set()

    @staticmethod
    def _replica_key(replica_id: object) -> int:
        return int(canonical_replica_id(replica_id))

    @staticmethod
    def _can_advance_generation(previous: RouteGeneration,
                                candidate: RouteGeneration) -> bool:
        """Return whether candidate is the sole valid same-row advance."""
        return (previous.controller_epoch == candidate.controller_epoch and
                previous.replica_record_id == candidate.replica_record_id and
                previous.recovery_state == 'ARMED' and
                candidate.recovery_state == 'RECOVERED' and
                previous.attempt_id != candidate.attempt_id)

    def _remember_record_locked(self, replica_id: int,
                                replica_record_id: str) -> bool:
        if (replica_id in self._blocked_record_ids or
                replica_record_id in self._retired_record_ids.get(
                    replica_id, set())):
            return False
        current = self._live_record_ids.get(replica_id)
        if current is not None:
            return current == replica_record_id
        if (len(self._live_record_ids)
                >= constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS):
            return False
        self._live_record_ids[replica_id] = replica_record_id
        return True

    def _record_tombstone_count_locked(self) -> int:
        return sum(
            len(record_ids) for record_ids in self._retired_record_ids.values())

    def _advance_record_identity_locked(self, replica_id: int,
                                        replica_record_id: str) -> bool:
        """Apply an ordered identity, rejecting any known historical rewind."""
        if replica_id in self._blocked_record_ids:
            return False
        current = self._live_record_ids.get(replica_id)
        if current is None:
            return self._remember_record_locked(replica_id, replica_record_id)
        if current == replica_record_id:
            return True
        retired_record_ids = self._retired_record_ids.get(replica_id, set())
        if replica_record_id in retired_record_ids:
            # A delayed insertion callback or fleet snapshot observed an older
            # physical row.  It cannot revoke or rewind the newer live row.
            return False
        if (self._record_tombstone_count_locked()
                >= constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS):
            self._blocked_record_ids.add(replica_id)
            state = self._targets.get(replica_id)
            if state is not None:
                self._retire_locked(replica_id, state)
            return False
        self._retired_record_ids.setdefault(replica_id, set()).add(current)
        self._live_record_ids[replica_id] = replica_record_id
        # The old row identity now supplies its bounded tombstone.  Drop its
        # target and generation tombstone so neither can accumulate per row.
        self._targets.pop(replica_id, None)
        self._retired.pop(replica_id, None)
        return True

    def _drop_record_locked(self, replica_id: int) -> None:
        self._targets.pop(replica_id, None)
        self._retired.pop(replica_id, None)
        self._live_record_ids.pop(replica_id, None)
        self._retired_record_ids.pop(replica_id, None)
        self._blocked_record_ids.discard(replica_id)

    def _retire_locked(self, replica_id: int, state: _TargetState) -> None:
        self._retired[replica_id] = state.target.generation
        state.active = False
        state.activated = False
        state.suspension_count = 0

    def _expire_locked(self, replica_id: int, state: _TargetState,
                       now: float) -> bool:
        if now < state.deadline:
            return False
        self._retire_locked(replica_id, state)
        return True

    def _is_retired_locked(self, replica_id: int,
                           generation: RouteGeneration) -> bool:
        if (replica_id in self._blocked_record_ids or
                generation.replica_record_id in self._retired_record_ids.get(
                    replica_id, set())):
            return True
        live_record_id = self._live_record_ids.get(replica_id)
        if (live_record_id is not None and
                generation.replica_record_id != live_record_id):
            return True
        state = self._targets.get(replica_id)
        if state is not None:
            current = state.target.generation
            if current == generation:
                return (not state.active or
                        self._retired.get(replica_id) == generation)
            # All same-record transitions other than ARMED -> RECOVERED are
            # stale or malformed.  Classify them as retired without touching
            # the exact current target.
            return not self._can_advance_generation(current, generation)
        retired = self._retired.get(replica_id)
        if retired is None:
            return False
        if retired == generation:
            return True
        return not self._can_advance_generation(retired, generation)

    def needs_issuance(self, replica_id: int, generation: RouteGeneration,
                       route_url: str) -> bool:
        replica_id = self._replica_key(replica_id)
        route_url = normalize_route_url(route_url)
        now = self._clock()
        with self._lock:
            if replica_id in self._blocked_record_ids:
                return True
            live_record_id = self._live_record_ids.get(replica_id)
            if (live_record_id is not None and
                    live_record_id != generation.replica_record_id):
                # An observation of a deleted/recreated row must not revoke a
                # token owned by the exact live row.
                return True
            state = self._targets.get(replica_id)
            if state is None:
                return True
            if self._expire_locked(replica_id, state, now):
                return True
            if (state.target.generation == generation and
                    state.target.route_url == route_url and state.active and
                    state.suspension_count == 0):
                return False
            if state.suspension_count:
                # A concurrent fenced mutation owns this omission.  Do not
                # turn its reversible suspension into a permanent tombstone.
                return True
            if state.target.generation == generation:
                # A URL transition within one exact generation can never
                # rotate or re-enter.  Stop its renewal before the potentially
                # blocking ordered remote read.
                self._retire_locked(replica_id, state)
                return True
            if self._can_advance_generation(state.target.generation,
                                            generation):
                # Exact RETRY_SUBMITTED adoption is the only valid advance.
                self._retire_locked(replica_id, state)
            # Invalid/backward generations remain off-route, but cannot revoke
            # the newer exact target already owned by this numeric ID.
            return True

    def issue(self, replica_id: int, generation: RouteGeneration,
              route_url: str, readiness_path: str,
              post_data: dict[str, Any] | None, headers: dict[str, str] | None,
              normal_probe_started_at: float) -> bool:
        """Issue one token, initially inactive, for an exact generation."""
        replica_id = self._replica_key(replica_id)
        route_url = normalize_route_url(route_url)
        probe_url = normalize_probe_url(route_url, readiness_path)
        if (not isinstance(normal_probe_started_at, (int, float)) or
                isinstance(normal_probe_started_at, bool) or
                not math.isfinite(normal_probe_started_at)):
            raise RouteLeaseError('normal_probe_started_at must be finite.')
        method = 'POST' if post_data is not None else 'GET'
        target = RouteProbeTarget(replica_id=replica_id,
                                  generation=generation,
                                  route_url=route_url,
                                  probe_url=probe_url,
                                  method=method,
                                  post_data=copy.deepcopy(post_data),
                                  headers=copy.deepcopy(headers),
                                  route_token=new_route_token())
        deadline = (float(normal_probe_started_at) +
                    constants.SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS)
        now = self._clock()
        with self._lock:
            if replica_id in self._blocked_record_ids:
                return False
            if not self._remember_record_locked(replica_id,
                                                generation.replica_record_id):
                return False
            existing = self._targets.get(replica_id)
            if existing is not None:
                self._expire_locked(replica_id, existing, now)
                if existing.suspension_count:
                    return False
                if existing.target.generation == generation:
                    if (not existing.active or
                            existing.target.route_url != route_url or
                            self._retired.get(replica_id) == generation):
                        self._retire_locked(replica_id, existing)
                        return False
                    # Ordinary readiness never renews or replaces an already
                    # issued token.  Only dedicated successful probes do.
                    return True
                if not self._can_advance_generation(existing.target.generation,
                                                    generation):
                    return False
                self._retire_locked(replica_id, existing)
            else:
                retired = self._retired.get(replica_id)
                if (retired is not None and retired != generation and
                        not self._can_advance_generation(retired, generation)):
                    return False
                if retired == generation:
                    return False
            state = _TargetState(target=target, deadline=deadline)
            self._targets[replica_id] = state
            # Older same-record tombstones are implied by the single forward
            # transition and need not accumulate alongside the new target.
            self._retired.pop(replica_id, None)
            if deadline <= now:
                self._retire_locked(replica_id, state)
                return False
            return True

    def deactivate(self,
                   replica_id: int,
                   generation: RouteGeneration | None = None) -> None:
        replica_id = self._replica_key(replica_id)
        with self._lock:
            state = self._targets.get(replica_id)
            if state is None:
                return
            if generation is not None and state.target.generation != generation:
                return
            self._retire_locked(replica_id, state)

    def deactivate_record(self, replica_id: int,
                          replica_record_id: str) -> None:
        """Retire only the target owned by one exact durable row."""
        replica_id = self._replica_key(replica_id)
        record_id = _canonical_uuid(replica_record_id, 'replica_record_id')
        assert record_id is not None
        with self._lock:
            state = self._targets.get(replica_id)
            if (state is None or
                    state.target.generation.replica_record_id != record_id):
                return
            self._retire_locked(replica_id, state)

    def suspend_record(self, replica_id: int,
                       replica_record_id: str) -> RouteSuspension | None:
        """Immediately omit one exact row while a fenced write is pending."""
        replica_id = self._replica_key(replica_id)
        record_id = _canonical_uuid(replica_record_id, 'replica_record_id')
        assert record_id is not None
        now = self._clock()
        with self._lock:
            state = self._targets.get(replica_id)
            if (state is None or not state.active or
                    state.target.generation.replica_record_id != record_id or
                    self._expire_locked(replica_id, state, now)):
                return None
            if state.suspension_count == 0:
                # A target snapshot already handed to the prober must remain
                # stale even if the durable write aborts and this exact token
                # is restored.  Nested holders share one suspension epoch.
                state.target = dataclasses.replace(
                    state.target, probe_epoch=state.target.probe_epoch + 1)
            state.suspension_count += 1
            return RouteSuspension(replica_id, state.target.generation,
                                   state.target.route_token,
                                   state.target.probe_epoch)

    def commit_suspension(self, suspension: RouteSuspension) -> None:
        """Permanently retire a suspended exact token after durable commit."""
        with self._lock:
            state = self._targets.get(suspension.replica_id)
            if (state is None or not state.active or
                    state.target.generation != suspension.generation or
                    state.target.route_token != suspension.route_token or
                    state.target.probe_epoch != suspension.probe_epoch or
                    state.suspension_count == 0):
                return
            self._retire_locked(suspension.replica_id, state)

    def rollback_suspension(self, suspension: RouteSuspension) -> None:
        """Restore only the unchanged still-live token after durable abort."""
        now = self._clock()
        with self._lock:
            state = self._targets.get(suspension.replica_id)
            if (state is None or not state.active or
                    state.target.generation != suspension.generation or
                    state.target.route_token != suspension.route_token or
                    state.target.probe_epoch != suspension.probe_epoch or
                    state.suspension_count == 0):
                return
            state.suspension_count -= 1
            if state.suspension_count == 0:
                self._expire_locked(suspension.replica_id, state, now)

    def observe_record_identity(self, replica_id: int,
                                replica_record_id: str) -> None:
        """Observe one row admission in authoritative manager-lock order."""
        replica_id = self._replica_key(replica_id)
        record_id = _canonical_uuid(replica_record_id, 'replica_record_id')
        assert record_id is not None
        with self._lock:
            self._advance_record_identity_locked(replica_id, record_id)

    def prune(self, live_record_ids: Mapping[int, str]) -> None:
        """Reconcile an authoritative ordered snapshot of live capable rows.

        Omitting a numeric ID releases its process-local identity history; a
        later admission for that ID therefore starts a new ordered history.
        """
        bounded_live_record_ids: dict[int, str] = {}
        for raw_replica_id, raw_record_id in live_record_ids.items():
            if (len(bounded_live_record_ids)
                    >= constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS):
                break
            replica_id = self._replica_key(raw_replica_id)
            record_id = _canonical_uuid(raw_record_id, 'replica_record_id')
            assert record_id is not None
            previous = bounded_live_record_ids.get(replica_id)
            if previous is not None and previous != record_id:
                raise RouteLeaseError(
                    'live_record_ids contains conflicting replica identities')
            bounded_live_record_ids[replica_id] = record_id
        with self._lock:
            tracked_ids = (set(self._live_record_ids) | set(self._targets) |
                           set(self._retired) | set(self._retired_record_ids) |
                           self._blocked_record_ids)
            for replica_id in tracked_ids - set(bounded_live_record_ids):
                self._drop_record_locked(replica_id)
            for replica_id, record_id in bounded_live_record_ids.items():
                self._advance_record_identity_locked(replica_id, record_id)
            self._targets = {
                replica_id: state
                for replica_id, state in self._targets.items()
                if self._live_record_ids.get(replica_id) ==
                state.target.generation.replica_record_id
            }
            self._retired = {
                replica_id: generation
                for replica_id, generation in self._retired.items()
                if self._live_record_ids.get(replica_id) ==
                generation.replica_record_id
            }

    def is_retired(self, replica_id: int, generation: RouteGeneration) -> bool:
        replica_id = self._replica_key(replica_id)
        now = self._clock()
        with self._lock:
            state = self._targets.get(replica_id)
            if state is not None and state.target.generation == generation:
                self._expire_locked(replica_id, state, now)
            return self._is_retired_locked(replica_id, generation)

    def marker(self, replica_id: int, generation: RouteGeneration,
               route_url: str) -> RouteMarker | None:
        replica_id = self._replica_key(replica_id)
        route_url = normalize_route_url(route_url)
        now = self._clock()
        with self._lock:
            if (replica_id in self._blocked_record_ids or
                    self._live_record_ids.get(replica_id)
                    != generation.replica_record_id):
                return None
            state = self._targets.get(replica_id)
            if state is None or state.target.generation != generation:
                return None
            if (state.active and not state.suspension_count and
                    state.target.route_url != route_url):
                # Endpoint resolution can observe a URL transition before the
                # next normal probe.  Revoke the same generation immediately;
                # it may never rotate or re-enter at the new transport.
                self._retire_locked(replica_id, state)
                return None
            if (state.target.route_url != route_url or not state.active or
                    state.suspension_count or
                    self._expire_locked(replica_id, state, now)):
                return None
            return RouteMarker(replica_id=canonical_replica_id(replica_id),
                               route_token=state.target.route_token)

    def probe_targets(self) -> list[RouteProbeTarget]:
        now = self._clock()
        with self._lock:
            targets = []
            for replica_id, state in self._targets.items():
                if (state.active and not state.suspension_count and
                        not self._expire_locked(replica_id, state, now)):
                    targets.append(copy.deepcopy(state.target))
            return targets

    def record_probe_result(self, target: RouteProbeTarget, *,
                            request_started_at: float, succeeded: bool) -> None:
        now = self._clock()
        with self._lock:
            state = self._targets.get(target.replica_id)
            if (state is None or
                    state.target.route_token != target.route_token or
                    state.target.generation != target.generation or
                    state.target.probe_epoch != target.probe_epoch or
                    not state.active or state.suspension_count):
                return
            # Expiry is checked before success publication, including before a
            # token's first activation.
            if self._expire_locked(target.replica_id, state, now):
                return
            if not succeeded:
                state.consecutive_failures += 1
                if state.consecutive_failures >= 2:
                    self._retire_locked(target.replica_id, state)
                return
            proposed_deadline = (float(request_started_at) +
                                 constants.SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS)
            if proposed_deadline <= now:
                self._retire_locked(target.replica_id, state)
                return
            state.activated = True
            state.consecutive_failures = 0
            state.deadline = proposed_deadline

    def heartbeat_payload(self) -> dict[str, Any]:
        now = self._clock()
        entries = []
        with self._lock:
            for replica_id, state in sorted(self._targets.items()):
                if (not state.active or state.suspension_count or
                        self._expire_locked(replica_id, state, now) or
                        not state.activated):
                    continue
                remaining = state.deadline - now
                if not 0 < remaining <= constants.SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS:
                    self._retire_locked(replica_id, state)
                    continue
                entries.append({
                    'replica_id': canonical_replica_id(replica_id),
                    'route_token': state.target.route_token,
                    'remaining_seconds': remaining,
                })
        return {
            'version': constants.SYSTEM_RECOVERY_ROUTE_LEASE_PROTOCOL_VERSION,
            'entries': entries,
        }


def parse_route_marker(info: object) -> tuple[bool, RouteMarker | None]:
    """Return (marker fields present, validated marker or None)."""
    if not isinstance(info, Mapping):
        return False, None
    marker_keys = {
        constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY,
        constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY,
        constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY,
        constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY,
    }
    if not marker_keys.intersection(info):
        return False, None
    if constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY in info:
        # Any explicit fence is deliberately unroutable, including malformed
        # or mixed fence/marker projections.
        return True, None
    if (info.get(constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY)
            != constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION):
        return True, None
    try:
        return True, RouteMarker(
            replica_id=canonical_replica_id(
                info.get(constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY)),
            route_token=canonical_route_token(
                info.get(constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY)))
    except RouteLeaseError:
        return True, None


def validate_heartbeat_payload(payload: object) -> dict[RouteMarker, float]:
    """Validate the closed v1 heartbeat response atomically."""
    if (not isinstance(payload, Mapping) or
            set(payload) != {'version', 'entries'} or payload.get('version')
            != constants.SYSTEM_RECOVERY_ROUTE_LEASE_PROTOCOL_VERSION):
        raise RouteLeaseError('invalid route-lease heartbeat envelope')
    raw_entries = payload.get('entries')
    if not isinstance(raw_entries, list):
        raise RouteLeaseError('heartbeat entries must be a list')
    if len(raw_entries) > constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS:
        raise RouteLeaseError('heartbeat contains too many entries')
    result: dict[RouteMarker, float] = {}
    replica_ids: set[str] = set()
    for raw in raw_entries:
        if (not isinstance(raw, Mapping) or
                set(raw) != {'replica_id', 'route_token', 'remaining_seconds'}):
            raise RouteLeaseError('invalid heartbeat entry')
        marker = RouteMarker(canonical_replica_id(raw.get('replica_id')),
                             canonical_route_token(raw.get('route_token')))
        remaining = raw.get('remaining_seconds')
        if (not isinstance(remaining,
                           (int, float)) or isinstance(remaining, bool) or
                not math.isfinite(remaining) or not 0 < remaining <=
                constants.SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS):
            raise RouteLeaseError(
                'remaining_seconds is outside the closed lease bound')
        if marker in result or marker.replica_id in replica_ids:
            raise RouteLeaseError('heartbeat contains duplicate route identity')
        result[marker] = float(remaining)
        replica_ids.add(marker.replica_id)
    return result
