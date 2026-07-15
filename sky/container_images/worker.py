"""Isolated data-plane operations for container image distribution."""

from collections.abc import Callable
import dataclasses
import hashlib
import threading
import time

from sky.container_images import config
from sky.container_images import models
from sky.container_images import providers
from sky.container_images import references
from sky.container_images import state


@dataclasses.dataclass(frozen=True)
class EvictionSweepResult:
    candidates: int
    evicted: int
    failed: int


MaterializationResult = models.MaterializationResult


@dataclasses.dataclass(frozen=True)
class ReconciliationSweepResult:
    """Bounded result from one independently deployable worker sweep."""

    candidates: int
    materialized: int
    revalidated: int
    failed: int


class LeaseLostError(RuntimeError):
    """Raised when a data-plane callback no longer owns its catalog lease."""


_RetryDeadline = int | Callable[[], int] | None


def _retry_deadline(value: _RetryDeadline) -> int | None:
    return value() if callable(value) else value


class _LeaseHeartbeat:
    """Keeps a lease alive and exposes cooperative cancellation to I/O."""

    def __init__(self, location_id: str, lease_token: str,
                 lease_seconds: int) -> None:
        self.cancel_event = threading.Event()
        self._stop_event = threading.Event()
        self._location_id = location_id
        self._lease_token = lease_token
        self._lease_seconds = lease_seconds
        self._interval = max(1.0, min(30.0, lease_seconds / 3))
        self._lost = False
        self._thread = threading.Thread(
            target=self._run,
            name=f'container-image-lease-{location_id}',
            daemon=True,
        )

    def __enter__(self) -> '_LeaseHeartbeat':
        # Extend immediately. A newly claimed item must not wait one heartbeat
        # interval before proving that its lease is still live.
        if not state.heartbeat_location(self._location_id, self._lease_token,
                                        self._lease_seconds):
            self._mark_lost()
            raise LeaseLostError('Container image operation lost its lease.')
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(1.0, self._interval + 1))

    def _mark_lost(self) -> None:
        self._lost = True
        self.cancel_event.set()

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval):
            try:
                owned = state.heartbeat_location(self._location_id,
                                                 self._lease_token,
                                                 self._lease_seconds)
            except Exception:  # pylint: disable=broad-except
                owned = False
            if not owned:
                self._mark_lost()
                return

    def assert_owned(self) -> None:
        """Extends and verifies ownership immediately before publication."""
        if self._lost:
            raise LeaseLostError('Container image operation lost its lease.')
        if not state.heartbeat_location(self._location_id, self._lease_token,
                                        self._lease_seconds):
            self._mark_lost()
            raise LeaseLostError('Container image operation lost its lease.')


def _retry_time(location: state.LocationRecord, now: int,
                base_seconds: int) -> int:
    """Returns bounded exponential backoff with per-location jitter."""
    exponent = min(max(location.attempt_count - 1, 0), 6)
    delay = base_seconds * (2**exponent)
    jitter_window = max(1, base_seconds // 2)
    jitter_seed = hashlib.sha256(
        f'{location.id}:{location.attempt_count}'.encode()).digest()
    jitter = int.from_bytes(jitter_seed[:4], 'big') % jitter_window
    return now + min(delay + jitter, 24 * 60 * 60)


def _retry_callback(location: state.LocationRecord, current_time: Callable[[],
                                                                           int],
                    base_seconds: int) -> Callable[[], int]:

    def retry_deadline() -> int:
        return _retry_time(location, current_time(), base_seconds)

    return retry_deadline


def _eviction_delete_callback(
    delete_manifest: Callable[[str, str, str, threading.Event], None],
    location: state.LocationRecord,
) -> Callable[[str, threading.Event], None]:

    def delete(reference: str, cancel_event: threading.Event) -> None:
        delete_manifest(location.image_id, location.target_id, reference,
                        cancel_event)

    return delete


def _materialization_inputs(
    location: state.LocationRecord,
) -> tuple[state.ImageRecord, models.RegistryProfile, models.RegistryTarget,
           str, str]:
    image = state.get_image(location.image_id)
    if image is None:
        raise ValueError(f'Image artifact {location.image_id!r} is missing.')
    profile, _ = config.resolve_profile(location.profile, image.workspace)
    if profile is None:
        raise ValueError(
            f'Distribution {location.profile!r} is no longer configured.')
    target = profile.target(location.target_id)
    if (location.profile_revision != profile.revision or
            not state.profile_revision_matches(image.workspace, profile.name,
                                               profile.revision,
                                               profile.revision_fingerprint)):
        raise ValueError('Materialization belongs to a stale registry profile '
                         'revision and cannot be operated on.')
    if profile.physical_fingerprint(target) != location.target_fingerprint:
        raise ValueError('Materialization belongs to a different physical '
                         'registry destination and cannot be reinterpreted.')
    if (profile.policy_fingerprint(target, location.canonical)
            != location.policy_fingerprint):
        raise ValueError('Materialization policy or authority has changed; '
                         'apply the current profile before operating on it.')
    canonical: state.LocationRecord | None
    if location.canonical:
        canonical = location
    else:
        canonical = (state.get_location_by_id(location.canonical_location_id)
                     if location.canonical_location_id is not None else None)
        if (canonical is None or not canonical.canonical or
                canonical.image_id != image.id or
                canonical.profile_revision != profile.revision or
                canonical.state != models.ImageLocationState.READY or
                canonical.target_ref is None):
            raise ValueError('Regional materialization requires a verified '
                             'current canonical source revision.')
        assert canonical.target_ref is not None
    assert canonical is not None
    if canonical.source_id is None:
        raise ValueError('Canonical materialization has no immutable source '
                         'binding.')
    source_record = state.get_source_by_id(canonical.source_id)
    if (source_record is None or source_record.image_id != image.id or
            source_record.workspace != image.workspace):
        raise ValueError('Canonical materialization source binding does not '
                         'belong to the artifact.')
    _, source_digest = models.split_digest(source_record.resolved_source_ref)
    if source_digest != image.source_digest:
        raise ValueError('Canonical materialization source binding does not '
                         'match the artifact digest.')
    destination = references.managed_reference(
        profile, target, image.workspace, source_record.resolved_source_ref,
        image.source_digest)
    source = (source_record.resolved_source_ref
              if location.canonical else canonical.target_ref)
    assert source is not None
    return image, profile, target, source, destination


def _materialize_claim(
    claim: state.LocationRecord,
    copy_and_verify: Callable[[str, str, str, threading.Event],
                              MaterializationResult],
    *,
    lease_seconds: int,
    retry_at: _RetryDeadline,
) -> bool:
    lease_token = claim.lease_owner
    assert lease_token is not None
    try:
        with _LeaseHeartbeat(claim.id, lease_token, lease_seconds) as lease:
            image, profile, target, source, destination = (
                _materialization_inputs(claim))
            # Credentials remain inside the adapter and callback process.
            repository, destination_digest = models.split_digest(destination)
            assert destination_digest == claim.expected_digest
            providers.get_adapter(target.provider).ensure_target_repository(
                target, profile, image.workspace, repository)
            copy_result = copy_and_verify(source, destination,
                                          claim.expected_digest,
                                          lease.cancel_event)
            if not isinstance(copy_result, MaterializationResult):
                raise TypeError('Container image copy callbacks must return '
                                'verified materialization metadata.')
            lease.assert_owned()
        return state.complete_location(claim.id, lease_token, destination,
                                       copy_result.digest,
                                       copy_result.platforms,
                                       copy_result.compressed_size_bytes)
    except Exception:  # pylint: disable=broad-except
        state.fail_location(
            claim.id, lease_token,
            models.ImageLocationErrorCode.MATERIALIZATION_FAILED,
            _retry_deadline(retry_at))
        return False


def materialize_location(
    location_id: str,
    owner: str,
    copy_and_verify: Callable[[str, str, str, threading.Event],
                              MaterializationResult],
    *,
    lease_seconds: int = 3600,
    retry_at: int | None = None,
) -> bool:
    """Copies one OCI index and publishes READY after metadata verification.

    The callback receives digest-pinned references, the expected digest, and
    a cancellation event. It returns the verified digest and nonempty platform
    set, and must abort promptly when cancellation is set.
    """
    claim = state.claim_location(location_id, owner, lease_seconds)
    if claim is None:
        return False
    return _materialize_claim(claim,
                              copy_and_verify,
                              lease_seconds=lease_seconds,
                              retry_at=retry_at)


def _adopt_external_claim(
    claim: state.LocationRecord,
    inspect_metadata: Callable[[str, threading.Event], MaterializationResult],
    *,
    lease_seconds: int,
    retry_at: _RetryDeadline,
) -> bool:
    lease_token = claim.lease_owner
    assert lease_token is not None
    try:
        with _LeaseHeartbeat(claim.id, lease_token, lease_seconds) as lease:
            _, profile, _, _, destination = _materialization_inputs(claim)
            if profile.ownership != models.RegistryOwnership.EXTERNAL:
                raise ValueError('Adoption is allowed only for externally '
                                 'owned registry targets.')
            result = inspect_metadata(destination, lease.cancel_event)
            if not isinstance(result, MaterializationResult):
                raise TypeError('Container image inspection callbacks must '
                                'return verified materialization metadata.')
            lease.assert_owned()
        return state.complete_location(claim.id, lease_token, destination,
                                       result.digest, result.platforms,
                                       result.compressed_size_bytes)
    except Exception:  # pylint: disable=broad-except
        state.fail_location(
            claim.id, lease_token,
            models.ImageLocationErrorCode.EXTERNAL_ADOPTION_FAILED,
            _retry_deadline(retry_at))
        return False


def adopt_external_location(
    location_id: str,
    owner: str,
    inspect_metadata: Callable[[str, threading.Event], MaterializationResult],
    *,
    lease_seconds: int = 300,
    retry_at: int | None = None,
) -> bool:
    """Adopts an external target after digest and platform inspection."""
    claim = state.claim_location(location_id, owner, lease_seconds)
    if claim is None:
        return False
    return _adopt_external_claim(claim,
                                 inspect_metadata,
                                 lease_seconds=lease_seconds,
                                 retry_at=retry_at)


def _revalidate_claim(
    claim: state.LocationRecord,
    inspect_digest: Callable[[str, threading.Event], str],
    *,
    lease_seconds: int,
    retry_at: _RetryDeadline,
) -> bool:
    lease_token = claim.lease_owner
    assert lease_token is not None
    assert claim.target_ref is not None
    try:
        with _LeaseHeartbeat(claim.id, lease_token, lease_seconds) as lease:
            _, _, _, _, destination = _materialization_inputs(claim)
            if destination != claim.target_ref:
                raise ValueError('READY materialization does not match the '
                                 'current physical destination.')
            verified_digest = inspect_digest(claim.target_ref,
                                             lease.cancel_event)
            lease.assert_owned()
        return state.complete_location_verification(claim.id, lease_token,
                                                    verified_digest,
                                                    _retry_deadline(retry_at))
    except Exception:  # pylint: disable=broad-except
        state.fail_location_verification(
            claim.id, lease_token,
            models.ImageLocationErrorCode.REVALIDATION_FAILED,
            _retry_deadline(retry_at))
        return False


def revalidate_location(
    location_id: str,
    owner: str,
    inspect_digest: Callable[[str, threading.Event], str],
    *,
    lease_seconds: int = 300,
    retry_at: int | None = None,
) -> bool:
    """Revalidates READY in place; only confirmed drift makes it unavailable."""
    claim = state.claim_location_verification(location_id, owner, lease_seconds)
    if claim is None:
        return False
    return _revalidate_claim(claim,
                             inspect_digest,
                             lease_seconds=lease_seconds,
                             retry_at=retry_at)


def reconcile_once(
    workspace: str,
    owner: str,
    copy_and_verify: Callable[
        [state.LocationRecord, str, str, str, threading.Event],
        MaterializationResult],
    inspect_metadata: Callable[[state.LocationRecord, str, threading.Event],
                               MaterializationResult],
    *,
    now: int | None = None,
    limit: int = 100,
    lease_seconds: int = 3600,
    retry_seconds: int = 60,
) -> ReconciliationSweepResult:
    """Claims and reconciles a bounded batch outside API request workers.

    The callbacks are the deployment-specific credential boundary. They can
    mint short-lived provider credentials in the worker process and must not
    return or persist those credentials. Canonical work is ordered before
    regional work. PostgreSQL replicas claim one row at a time with
    ``SKIP LOCKED``, so a slow transfer cannot make one worker hoard a page.
    """
    if retry_seconds <= 0:
        raise ValueError('retry_seconds must be positive.')
    if limit <= 0:
        raise ValueError('limit must be positive.')
    fixed_now = now

    def current_time() -> int:
        return fixed_now if fixed_now is not None else int(time.time())

    candidates = 0
    materialized = 0
    revalidated = 0
    failed = 0
    verification_lease_seconds = min(lease_seconds, 300)
    for _ in range(limit):
        claim_now = current_time()
        claimed = state.claim_next_reconciliation_candidate(
            workspace,
            owner,
            lease_seconds,
            verification_lease_seconds,
            now=claim_now)
        if claimed is None:
            break
        candidate = claimed
        candidates += 1
        retry_at = lambda candidate=candidate: _retry_time(
            candidate, current_time(), retry_seconds)

        def inspect_candidate_metadata(
            reference: str,
            cancel_event: threading.Event,
            candidate_record: state.LocationRecord = candidate,
        ) -> MaterializationResult:
            return inspect_metadata(candidate_record, reference, cancel_event)

        def inspect_candidate_digest(
            reference: str,
            cancel_event: threading.Event,
            candidate_record: state.LocationRecord = candidate,
        ) -> str:
            return inspect_metadata(candidate_record, reference,
                                    cancel_event).digest

        def copy_candidate(
            source: str,
            destination: str,
            digest: str,
            cancel_event: threading.Event,
            candidate_record: state.LocationRecord = candidate,
        ) -> MaterializationResult:
            return copy_and_verify(candidate_record, source, destination,
                                   digest, cancel_event)

        if candidate.state == models.ImageLocationState.READY:
            succeeded = _revalidate_claim(
                candidate,
                inspect_candidate_digest,
                lease_seconds=verification_lease_seconds,
                retry_at=retry_at)
            if succeeded:
                revalidated += 1
            else:
                failed += 1
            continue

        image = state.get_image(candidate.image_id)
        profile = None
        if image is not None:
            try:
                profile, _ = config.resolve_profile(candidate.profile,
                                                    image.workspace)
            except Exception:  # pylint: disable=broad-except
                # The claimed operation below records the configuration error
                # through the normal lease-fenced failure transition.
                profile = None
        if (candidate.canonical and profile is not None and
                profile.ownership == models.RegistryOwnership.EXTERNAL):
            succeeded = _adopt_external_claim(
                candidate,
                inspect_candidate_metadata,
                lease_seconds=verification_lease_seconds,
                retry_at=retry_at)
        else:
            succeeded = _materialize_claim(candidate,
                                           copy_candidate,
                                           lease_seconds=lease_seconds,
                                           retry_at=retry_at)
        if succeeded:
            materialized += 1
        else:
            failed += 1
    return ReconciliationSweepResult(candidates=candidates,
                                     materialized=materialized,
                                     revalidated=revalidated,
                                     failed=failed)


def _evict_claim(
    claim: state.LocationRecord,
    delete_manifest: Callable[[str, threading.Event], None],
    *,
    lease_seconds: int,
    retry_at: _RetryDeadline,
) -> bool:
    """Deletes one already-claimed regional manifest."""
    lease_token = claim.lease_owner
    assert lease_token is not None
    if claim.target_ref is None:
        state.fail_location_eviction(
            claim.id, lease_token,
            models.ImageLocationErrorCode.EVICTION_REFERENCE_INVALID,
            _retry_deadline(retry_at))
        return False
    try:
        _, target_digest = models.split_digest(claim.target_ref)
    except ValueError:
        target_digest = None
    if target_digest != claim.expected_digest:
        state.fail_location_eviction(
            claim.id, lease_token,
            models.ImageLocationErrorCode.EVICTION_REFERENCE_INVALID,
            _retry_deadline(retry_at))
        return False
    delete_started = False
    try:
        with _LeaseHeartbeat(claim.id, lease_token, lease_seconds) as lease:
            image, profile, target, _, destination = _materialization_inputs(
                claim)
            if profile.ownership != models.RegistryOwnership.MANAGED:
                raise ValueError('Current registry policy does not grant '
                                 'SkyPilot deletion authority.')
            if destination != claim.target_ref:
                raise ValueError('Eviction target does not match the current '
                                 'physical registry destination.')
            providers.get_adapter(target.provider).authorize_manifest_deletion(
                target, profile, image.workspace, claim.target_ref)
            delete_started = True
            delete_manifest(claim.target_ref, lease.cancel_event)
            lease.assert_owned()
    except Exception:  # pylint: disable=broad-except
        state.fail_location_eviction(
            claim.id,
            lease_token,
            models.ImageLocationErrorCode.EVICTION_FAILED,
            _retry_deadline(retry_at),
            manifest_may_be_missing=delete_started)
        return False
    if state.complete_location_eviction(claim.id, lease_token):
        return True
    # The manifest delete returned successfully, but a completion fence changed
    # while it ran (for example the exact canonical source became unavailable).
    # Never restore a destination whose continued existence is now unknown.
    state.fail_location_eviction(
        claim.id,
        lease_token,
        models.ImageLocationErrorCode.EVICTION_COMPLETION_FENCE_CHANGED,
        _retry_deadline(retry_at),
        manifest_may_be_missing=True)
    return False


def evict_location(
    location_id: str,
    owner: str,
    unused_before: int,
    delete_manifest: Callable[[str, threading.Event], None],
    *,
    lease_seconds: int = 300,
    retry_at: int | None = None,
) -> bool:
    """Claims and deletes one regional manifest with lease fencing."""
    claim = state.claim_location_eviction(location_id, owner, lease_seconds,
                                          unused_before)
    if claim is None:
        return False
    return _evict_claim(claim,
                        delete_manifest,
                        lease_seconds=lease_seconds,
                        retry_at=retry_at)


def sweep_evictions(
    workspace: str,
    owner: str,
    delete_manifest: Callable[[str, str, str, threading.Event], None],
    *,
    now: int | None = None,
    limit: int = 100,
    lease_seconds: int = 300,
    retry_seconds: int = 3600,
) -> EvictionSweepResult:
    if retry_seconds <= 0:
        raise ValueError('retry_seconds must be positive.')
    fixed_now = now

    def current_time() -> int:
        return fixed_now if fixed_now is not None else int(time.time())

    evaluation_now = current_time()
    retention_weeks = config.get_workspace_policy(
        workspace).regional_cache_retention_weeks
    if retention_weeks is None:
        return EvictionSweepResult(candidates=0, evicted=0, failed=0)
    unused_before = evaluation_now - retention_weeks * 7 * 24 * 60 * 60
    claimed = 0
    evicted = 0
    for _ in range(limit):
        candidate = state.claim_next_eviction_candidate(workspace,
                                                        owner,
                                                        lease_seconds,
                                                        unused_before,
                                                        now=current_time())
        if candidate is None:
            break
        delete = _eviction_delete_callback(delete_manifest, candidate)
        retry_deadline = _retry_callback(candidate, current_time, retry_seconds)

        claimed += 1
        if _evict_claim(candidate,
                        delete,
                        lease_seconds=lease_seconds,
                        retry_at=retry_deadline):
            evicted += 1
    return EvictionSweepResult(candidates=claimed,
                               evicted=evicted,
                               failed=claimed - evicted)
