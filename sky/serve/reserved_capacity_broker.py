"""Reserved-fill broker: multi-service arbitration of zero-cost pools.

[boltz fork] With multiple fill-enabled services (#108) on one reserved
pool, independent pollers race for the same free GPUs: every autoscaler
targets them, the k8s scheduler picks winners, losers bench their zero-cost
tier for the retry TTL, and the cluster-wide realtime query runs N times per
interval. This module arbitrates: each service's poller upserts a CLAIM
(weight / floor / holdings / heartbeat), one poller per interval drives a
ROUND (single cluster query under a cross-process lock), and the round
publishes per-service GRANTS (entitlement ceilings) and FEEDS (launchable-
now free slots, sum <= observed free by construction).

Design invariants (see the 2026-07-08 design doc):
- Entitlements are floors-first (largest-remainder proportional scale-down
  if oversubscribed) then weighted water-filling of the remainder with
  headroom caps and redistribution; all arithmetic in integer replica slots
  (v1 requires uniform gpus_per_replica per pool).
- Feeds are a separate water-fill of OBSERVED FREE among under-holders: a
  peer's slow graceful drain must not make Sum(feeds) exceed physical free
  capacity (entitlement-as-feed overshoots; feed-split cannot).
- Grants only ever gate NEW launches, so stale readers are safe; the pool's
  ROUND epoch is the fencing token that keeps a respawned/stalled controller
  from ACTING on a superseded allocation (per-pool, so one pool's grant
  churn never fences another's launches); the global lease epoch exists
  only for the publish CAS.
- Under protocol v1, exactly one live claim uses the fast path: grant None
  (no ceiling), feed = raw observed free -- byte-identical #108 behavior,
  pinned by the existing test suite. Protocol v2 always publishes an integer
  grant capped by the partitioned edge cap.

This module is the stable broker facade and owns the stateful round driver;
deterministic allocation policy lives in reserved_capacity_allocation. All SQL
lives in serve_state (the shared serve DB every controller in the api-server
pod already uses).
"""
import base64
import binascii
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
import hashlib
import json
import math
import os
import re
import stat
import threading
import time
import typing
from typing import Any, TypeGuard

from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.adaptors import kubernetes
from sky.serve import constants
from sky.serve import pool_capacity_observation
from sky.serve import reserved_capacity_allocation
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import serve_state
from sky.server.requests import postgres as request_postgres
from sky.utils import common_utils
from sky.utils import locks
from sky.utils.db import migration_utils

if typing.TYPE_CHECKING:
    from sky.serve import replica_managers
    from sky.serve import zero_cost_actuation
else:
    zero_cost_actuation = adaptors_common.LazyImport(
        'sky.serve.zero_cost_actuation')

logger = sky_logging.init_logger(__name__)

# Round age below which a poller reads the published round instead of
# driving a new one, as a fraction of the poll interval. Slightly below 1 so
# scheduling jitter cannot leave a pool permanently one-poller short of
# driving (with N pollers on the same interval, roughly one round is driven
# per interval; the rest read).
_ROUND_FRESH_FRACTION = 0.9

# Durable broker protocol versions.  Protocol v1 is the historical
# context-keyed, one-claim-per-service path.  Protocol v2 is activated only
# through the durable state gate and uses physical-cluster pool keys plus
# generation-fenced composite claims.
PROTOCOL_V1 = 1
PROTOCOL_V2 = 2
_SUPPORTED_PROTOCOLS = frozenset((PROTOCOL_V1, PROTOCOL_V2))
# Additive metadata inside the existing per-service exact-card JSON.  `$`
# cannot begin a valid SkyServe service name. The slot-width key is emitted
# only by committed-observation rounds after the fleet convergence gate, so a
# legacy mixed-binary writer never observes a key its epoch canonicalizer does
# not understand.
OBSERVED_FREE_BY_ACCELERATOR_KEY = '$skypilot-observed-free-v1'
_OBSERVED_FREE_BY_ACCELERATOR_KEY = OBSERVED_FREE_BY_ACCELERATOR_KEY
BROKER_SLOT_WIDTH_KEY = '$skypilot-slot-width-v1'
_BROKER_SLOT_WIDTH_KEY = BROKER_SLOT_WIDTH_KEY

# This is the fixed container name emitted by the SkyPilot Helm chart.  The
# activation gate deliberately does not accept an operator-selected container:
# otherwise a stable sidecar could be presented as proof while the API server
# itself is still rolling out.
_API_SERVER_CONTAINER_NAME = 'skypilot-api'
_CONTROLLER_CONTAINER_NAME = 'skypilot-controller'
_EXECUTOR_CONTAINER_NAME = 'skypilot-executor'
_SERVER_ROLE_ENV_VAR = 'SKYPILOT_API_SERVER_ROLE'
_RELEASE_NAME_ENV_VAR = 'SKYPILOT_RELEASE_NAME'
_REQUEST_BACKEND_ENV_VAR = 'SKYPILOT_API_REQUEST_BACKEND'
_QUIESCENCE_BACKEND_GUARD_ENV_VAR = (
    'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS')
_IMAGE_ID_DIGEST_PATTERN = re.compile(r'(?:@|//)(sha256:[0-9a-fA-F]{64})$')
_PROTOCOL_V2_SCHEMA_REVISIONS = frozenset({'035', '036', '037'})
_PROTOCOL_V2_API_REQUEST_SCHEMA_REVISION = '011'
_MAX_SERVICE_ACCOUNT_TOKEN_BYTES = 64 * 1024
# Keep this equal to the API request server-instance lease's stale horizon.
# Recently draining/unready rows remain relevant: their controller children may
# still be alive until the full lease ages out.
_WRITER_INSTANCE_STALE_AFTER_SECONDS = 20
_HELM_INSTANCE_LABEL = 'app.kubernetes.io/instance'
_MIGRATION_COMPONENT_LABEL = 'app.kubernetes.io/component'
_MIGRATION_COMPONENT = 'database-migration'


class ProtocolV2ActivationError(RuntimeError):
    """A Kubernetes rollout cannot safely authorize broker protocol v2."""


class ProtocolV1DemotionError(RuntimeError):
    """The live rollout or durable claims cannot safely return to v1."""


@dataclasses.dataclass(frozen=True)
class _TokenBoundPodIdentity:
    """Pod identity authenticated by the mounted service-account token."""

    namespace: str
    name: str
    uid: str


@dataclasses.dataclass(frozen=True)
class _DeploymentOwnerIdentity:
    """Deployment owner reached from a token-bound Pod owner chain."""

    name: str
    uid: str


@dataclasses.dataclass(frozen=True)
class _WriterDeploymentTarget:
    """One mechanically discovered writer Deployment."""

    role: str
    name: str
    container_name: str
    server_role: str


@dataclasses.dataclass(frozen=True)
class _WriterDeploymentSnapshot:
    """One fully validated API, controller, or executor rollout."""

    role: str
    deployment_name: str
    deployment_generation: str
    deployment_resource_version: str
    deployment_uid: str
    container_name: str
    image_digest: str
    # Name, UID, and resourceVersion make both membership and each member's
    # observed state part of the stable double-read fence.
    pod_cohort: tuple[tuple[str, str, str], ...]


@dataclasses.dataclass(frozen=True)
class _WriterProcessInstance:
    """One recent database lease held by a request-serving process."""

    role: str
    instance_id: str
    pod_name: str
    pod_uid: str
    version: str
    ready: bool
    draining: bool
    request_storage_backend: str
    request_queue_backend: str
    execution_quiescence_capable: bool


@dataclasses.dataclass(frozen=True)
class _WriterRolloutSnapshot:
    """One stable view of every release process that can write fill state."""

    release_name: str
    deployments: tuple[_WriterDeploymentSnapshot, ...]
    writer_instances: tuple[_WriterProcessInstance, ...]

    @property
    def image_digest(self) -> str:
        digests = {deployment.image_digest for deployment in self.deployments}
        if len(digests) != 1:
            raise ProtocolV2ActivationError(
                'The API/controller/executor writer fleet has mixed immutable '
                'image digests.')
        return digests.pop()

    @property
    def deployment_generation(self) -> str:
        inventory = [(deployment.role, deployment.deployment_name,
                      deployment.deployment_generation)
                     for deployment in self.deployments]
        return json.dumps(inventory, separators=(',', ':'), ensure_ascii=True)

    @property
    def deployment_uid(self) -> str:
        inventory = [(deployment.role, deployment.deployment_name,
                      deployment.deployment_uid)
                     for deployment in self.deployments]
        return json.dumps(inventory, separators=(',', ':'), ensure_ascii=True)

    @property
    def pod_inventory(self) -> tuple[tuple[str, str, str, str, str, str], ...]:
        inventory: list[tuple[str, str, str, str, str, str]] = []
        for deployment in self.deployments:
            inventory.extend(
                (deployment.role, deployment.deployment_name,
                 deployment.container_name, pod_name, pod_uid, resource_version)
                for pod_name, pod_uid, resource_version in
                deployment.pod_cohort)
        return tuple(sorted(inventory))

    @property
    def pod_inventory_count(self) -> int:
        return len(self.pod_inventory)

    @property
    def pod_inventory_sha256(self) -> str:
        proof = {
            'pods': self.pod_inventory,
            'writer_instances': [
                dataclasses.astuple(instance)
                for instance in self.writer_instances
            ],
        }
        serialized = json.dumps(proof, separators=(',', ':'), ensure_ascii=True)
        return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def claim_ttl_seconds() -> float:
    override = os.environ.get(constants.RESERVED_FILL_CLAIM_TTL_ENV_VAR)
    if override is not None:
        try:
            return max(1.0, float(override))
        except ValueError:
            logger.warning(
                f'Invalid {constants.RESERVED_FILL_CLAIM_TTL_ENV_VAR} value '
                f'{override!r}, using default '
                f'{constants.RESERVED_FILL_CLAIM_TTL_SECONDS}s.')
    return float(constants.RESERVED_FILL_CLAIM_TTL_SECONDS)


def _canonical_gpu_names(
    gpu_names: str | list[str] | tuple[str, ...],) -> tuple[str, ...]:
    names: tuple[str, ...]
    if isinstance(gpu_names, str):
        names = (gpu_names.lower(),)
    else:
        names = tuple(sorted({name.lower() for name in gpu_names}))
    if not names:
        raise ValueError('A reserved-capacity pool needs an accelerator.')
    return names


def _encoded_gpu_names(names: tuple[str, ...]) -> str | list[str]:
    return names[0] if len(names) == 1 else list(names)


def make_pool_key(
    context: str,
    gpu_names: str | list[str] | tuple[str, ...],
    *,
    protocol_version: int = PROTOCOL_V1,
    physical_cluster_uid: str | None = None,
) -> str:
    """Canonical pool identity for one broker protocol.

    Protocol v1 remains byte-for-byte ``[context, accelerators]``. Protocol v2
    deliberately drops the access-context alias from physical identity and
    encodes ``["v2", cluster_uid, accelerators]``.  The context remains on the
    v2 claim for querying and launch actuation.
    """
    if protocol_version not in _SUPPORTED_PROTOCOLS:
        raise ValueError(f'Unsupported reserved-fill protocol version: '
                         f'{protocol_version!r}.')
    names = _canonical_gpu_names(gpu_names)
    encoded_names = _encoded_gpu_names(names)
    if protocol_version == PROTOCOL_V1:
        return json.dumps([context, encoded_names])
    if not isinstance(physical_cluster_uid, str) or not physical_cluster_uid:
        raise ValueError('Protocol-v2 pool keys require a physical cluster '
                         'UID.')
    return json.dumps(['v2', physical_cluster_uid, encoded_names])


@dataclasses.dataclass(frozen=True)
class PoolIdentity:
    protocol_version: int
    access_context: str | None
    physical_cluster_uid: str | None
    gpu_names: tuple[str, ...]


def parse_pool_identity(pool_key: str) -> PoolIdentity:
    """Parse either pool-key protocol without guessing malformed keys."""
    decoded = json.loads(pool_key)
    if not isinstance(decoded, list):
        raise ValueError(f'Invalid reserved-fill pool key: {pool_key!r}.')
    if len(decoded) == 2:
        context, encoded_names = decoded
        if not isinstance(context, str) or not context:
            raise ValueError(f'Invalid reserved-fill pool key: {pool_key!r}.')
        protocol_version = PROTOCOL_V1
        physical_cluster_uid = None
        access_context: str | None = context
    elif len(decoded) == 3 and decoded[0] == 'v2':
        _, physical_cluster_uid, encoded_names = decoded
        if (not isinstance(physical_cluster_uid, str) or
                not physical_cluster_uid):
            raise ValueError(f'Invalid reserved-fill pool key: {pool_key!r}.')
        protocol_version = PROTOCOL_V2
        access_context = None
    else:
        raise ValueError(f'Invalid reserved-fill pool key: {pool_key!r}.')
    if isinstance(encoded_names, str):
        raw_names = (encoded_names,)
    elif isinstance(encoded_names, list) and all(
            isinstance(name, str) for name in encoded_names):
        raw_names = tuple(encoded_names)
    else:
        raise ValueError(f'Invalid reserved-fill pool key: {pool_key!r}.')
    names = _canonical_gpu_names(raw_names)
    return PoolIdentity(protocol_version=protocol_version,
                        access_context=access_context,
                        physical_cluster_uid=physical_cluster_uid,
                        gpu_names=names)


def parse_pool_key(pool_key: str) -> tuple[str, tuple[str, ...]]:
    """Parse a protocol-v1 pool key.

    Kept for existing callers that require an access context. Protocol-v2
    callers must use :func:`parse_pool_identity` and the claim's separate
    access-context field.
    """
    identity = parse_pool_identity(pool_key)
    if identity.protocol_version != PROTOCOL_V1:
        raise ValueError('Protocol-v2 pool keys do not contain an access '
                         'context.')
    assert identity.access_context is not None
    return identity.access_context, identity.gpu_names


def _pool_keys_overlap(left: str, right: str) -> bool:
    left_identity = parse_pool_identity(left)
    right_identity = parse_pool_identity(right)
    if left_identity.protocol_version != right_identity.protocol_version:
        # Protocols never share a round. The durable activation gate prevents
        # them from acting concurrently; treating them as non-overlapping here
        # keeps the v1 compatibility shadow inert under protocol v2.
        return False
    if left_identity.protocol_version == PROTOCOL_V1:
        same_pool = (
            left_identity.access_context == right_identity.access_context)
    else:
        same_pool = (left_identity.physical_cluster_uid ==
                     right_identity.physical_cluster_uid)
    return same_pool and bool(
        set(left_identity.gpu_names).intersection(right_identity.gpu_names))


@dataclasses.dataclass(frozen=True)
class PoolObservation:
    """One realtime free-capacity measurement of a pool.

    free_slots None = the query FAILED (measurement blackout) -- distinct
    from 0 free, which is a successful measurement of a full pool.
    gpu_names are the canonical accelerator names the realtime query
    reported for the pool's context; empty on a successful query means the
    claimed GPU resolves to no labeled nodes (phantom pool).

    free_slots_by_accelerator is the optional exact-card decomposition of
    free_slots, in deterministic task-resource order. ``None`` means the
    provider/writer did not publish exact-card telemetry; an empty tuple is an
    authoritative zero-card split.  Tuple storage keeps this frozen value
    immutable and JSON-independent while it crosses the round driver.
    """
    free_slots: int | None
    gpu_names: tuple[str, ...] = ()
    free_slots_by_accelerator: tuple[tuple[str, int], ...] | None = None


@dataclasses.dataclass(frozen=True)
class Allocation:
    """One service's slice of the latest round."""
    # None = protocol-v1 single-claimant fast path: no ceiling (#108 identity).
    grant: int | None
    feed: int
    round_id: int
    epoch: int
    snapshot_time: float
    # What the DEMAND-placement gate reads, as opposed to the fill ceiling.
    # The two consumers need opposite biases. The ceiling must be
    # conservative on the way up: do not launch fill you are about to cull.
    # The demand gate must be permissive on the way up: a burst that has
    # just reclaimed its entitlement must not have its demand replicas
    # steered onto paid capacity for the two rounds damping takes to walk
    # the ceiling back, which is both the opposite of the intent and the
    # slowest possible reacquisition path on a saturated pool. Since a rise
    # is instantaneous in the raw entitlement, max(damped, raw) reopens the
    # gate in the same round the burst is observed.
    demand_gate_grant: int | None = None
    # Allocation authority. Protocol v1 uses generation zero. Protocol v2
    # decisions must carry every field through final replica persistence.
    protocol_version: int = PROTOCOL_V1
    service_generation: int = 0
    physical_cluster_uid: str | None = None
    edge_cap: int | None = None
    pool_key: str | None = None
    # Exact-card portion of this service's feed. None means the round had no
    # exact-card telemetry; an empty mapping means exact telemetry authorized
    # no shaped launch.  The aggregate feed is always clamped to this mapping
    # when present.
    feed_by_accelerator: dict[str, int] | None = None
    # Provider observation converted to replica slots by the successfully
    # published round. These fields are absent for old, blackout, rejected, or
    # corrupt rounds. They are placement evidence only; feed/grant remain
    # launch authority. Raw GPU evidence stays in the observation ledger.
    observed_free_slots: int | None = None
    observed_free_slots_by_accelerator: dict[str, int] | None = None
    observed_at: float | None = None
    broker_slot_width: int = 1


@dataclasses.dataclass(frozen=True)
class RoundObservationProvenance:
    """Immutable observation authority attached to one round publication.

    A sequenced round publisher must persist this in the same transaction as
    the allocation.  Keeping the complete physical identity here lets that
    boundary reject a confused-deputy publication instead of trusting only a
    generation number and digest.  ``access_context`` identifies the alias
    used to acquire the observation; placement access is attested separately
    by each service claim edge and may use another alias of the same UID.
    """

    pool_key: str
    physical_cluster_uid: str
    accelerator_names: tuple[str, ...]
    access_context: str
    observation_generation: int
    observation_sequence: int
    materialization_sequence: int
    payload_sha256: str
    observed_at: float
    valid_until: float


@dataclasses.dataclass(frozen=True)
class ReservedFillRoundPublication:
    """Typed input to the durable round-publication boundary."""

    pool_key: str
    round_id: int
    snapshot_time: float
    epoch: int
    grants: str
    feeds: str
    feed_by_accelerator: str | None
    raw_grants: str
    feed_state: str
    sum_holdings: int
    last_observed_free: int | None
    last_observed_free_ts: float | None
    phantom_streak: int
    shrink_baseline: int | None
    lease_token: int
    lease_expires_at: float
    protocol_version: int
    claim_generations: str
    utilization_state: str | None
    observation_provenance: RoundObservationProvenance | None = None


class RoundPublisher(typing.Protocol):
    """Persists one allocation publication, including its provenance."""

    def __call__(self, publication: ReservedFillRoundPublication, /) -> bool:
        ...


@dataclasses.dataclass(frozen=True)
class _CommittedRoundObservation:
    """Validated provider-free input consumed while the broker lock is held."""

    payload: pool_capacity_observation.PoolCapacitySuccess
    provenance: RoundObservationProvenance

    def is_authoritative_at(self, now: float) -> bool:
        return self.provenance.observed_at <= now <= self.provenance.valid_until

    def to_slot_observation(self, gpus_per_replica: int) -> PoolObservation:
        """Convert raw physical evidence under one authenticated claim width."""
        slots_by_accelerator = self.payload.slot_counts(gpus_per_replica)
        return PoolObservation(free_slots=sum(
            count for _, count in slots_by_accelerator),
                               gpu_names=self.payload.present_accelerator_names,
                               free_slots_by_accelerator=slots_by_accelerator)


# Keep the historical broker import and pickle identities as a direct facade.
ClaimInput = reserved_capacity_allocation.ClaimInput
# pylint: disable-next=protected-access
_largest_remainder_round = reserved_capacity_allocation._largest_remainder_round
scale_floors = reserved_capacity_allocation.scale_floors
water_fill = reserved_capacity_allocation.water_fill
compute_entitlements = reserved_capacity_allocation.compute_entitlements
damp_grants = reserved_capacity_allocation.damp_grants
compute_feeds = reserved_capacity_allocation.compute_feeds
advance_release_target = reserved_capacity_allocation.advance_release_target
for _allocation_symbol in (ClaimInput, _largest_remainder_round, scale_floors,
                           water_fill, compute_entitlements, damp_grants,
                           compute_feeds, advance_release_target):
    _allocation_symbol.__module__ = __name__
del _allocation_symbol


@dataclasses.dataclass(frozen=True)
class CachedPoolGrant:
    """Fresh v2 demand-gate authority for one physical pool edge."""
    grant: int
    access_context: str
    accelerator_names: tuple[str, ...]
    physical_cluster_uid: str
    service_generation: int


@dataclasses.dataclass(frozen=True)
class _GrantCacheEntry:
    grant: int | None
    cached_at: float
    access_context: str | None = None
    accelerator_names: tuple[str, ...] = ()
    physical_cluster_uid: str | None = None
    service_generation: int = 0


# In-process cache of the last GRANT each service/pool edge observed, refreshed
# by its poller every poll interval. Protocol v1 stores its historical
# service-only entry under a None pool key; protocol v2 entries are composite.
# The demand-placement gate reads ONLY this cache (never the DB).
_GRANT_CACHE: dict[tuple[str, str | None], _GrantCacheEntry] = {}
# Pollers refresh/reconcile this map while replica reconciliation reads it.
# The entries are immutable, so readers can release the lock after taking one
# complete snapshot. RLock is required because cache reconciliation calls the
# service-clear helper while it may already own the cache lock.
_GRANT_CACHE_LOCK = threading.RLock()


def clear_caches() -> None:
    """Test hook: drop in-process state."""
    with _GRANT_CACHE_LOCK:
        _GRANT_CACHE.clear()


def _cache_key(service_name: str,
               pool_key: str | None) -> tuple[str, str | None]:
    return service_name, pool_key


def _clear_service_cache(service_name: str,
                         pool_key: str | None = None) -> None:
    with _GRANT_CACHE_LOCK:
        if pool_key is not None:
            _GRANT_CACHE.pop(_cache_key(service_name, pool_key), None)
            return
        for key in list(_GRANT_CACHE):
            if key[0] == service_name:
                _GRANT_CACHE.pop(key, None)


def _grant_cache_snapshot(
) -> tuple[tuple[tuple[str, str | None], _GrantCacheEntry], ...]:
    """Return one coherent immutable-entry snapshot for lock-free filtering."""
    with _GRANT_CACHE_LOCK:
        return tuple(_GRANT_CACHE.items())


def get_cached_grant(service_name: str,
                     max_age_seconds: float,
                     *,
                     pool_key: str | None = None) -> int | None:
    """Read one fresh advisory grant.

    Omitting ``pool_key`` preserves the protocol-v1 service-only API.
    Protocol-v2 callers must name the exact pool; grants are not transferable
    across contexts.
    """
    with _GRANT_CACHE_LOCK:
        entry = _GRANT_CACHE.get(_cache_key(service_name, pool_key))
    if entry is None:
        return None
    if time.time() - entry.cached_at > max_age_seconds:
        return None
    return entry.grant


def get_cached_grants(service_name: str,
                      max_age_seconds: float) -> dict[str, int]:
    """Return every fresh protocol-v2 pool grant for one service."""
    now = time.time()
    return {
        pool_key: entry.grant
        for (cached_service, pool_key), entry in _grant_cache_snapshot()
        if (cached_service == service_name and pool_key is not None and
            entry.grant is not None and now -
            entry.cached_at <= max_age_seconds)
    }


def get_cached_pool_grants(
        service_name: str,
        max_age_seconds: float) -> dict[str, CachedPoolGrant]:
    """Return fresh v2 grants with the access metadata needed to place."""
    now = time.time()
    result: dict[str, CachedPoolGrant] = {}
    for (cached_service, pool_key), entry in _grant_cache_snapshot():
        if (cached_service != service_name or pool_key is None or
                entry.grant is None or
                now - entry.cached_at > max_age_seconds or
                entry.access_context is None or
                entry.physical_cluster_uid is None or
                not entry.accelerator_names):
            continue
        result[pool_key] = CachedPoolGrant(
            grant=entry.grant,
            access_context=entry.access_context,
            accelerator_names=entry.accelerator_names,
            physical_cluster_uid=entry.physical_cluster_uid,
            service_generation=entry.service_generation)
    return result


def get_protocol_version() -> int:
    """Read the durable broker protocol, failing closed to protocol v1."""
    row = serve_state.get_reserved_fill_protocol_state()
    if row is None:
        return PROTOCOL_V1
    try:
        version = int(row['protocol_version'])
    except (KeyError, TypeError, ValueError):
        logger.error('Reserved-fill protocol state is malformed; retaining '
                     'protocol v1.')
        return PROTOCOL_V1
    if version not in _SUPPORTED_PROTOCOLS:
        logger.error(f'Unsupported durable reserved-fill protocol {version}; '
                     'retaining protocol v1.')
        return PROTOCOL_V1
    return version


def _decode_token_bound_pod_identity(token: str) -> _TokenBoundPodIdentity:
    """Decode pod-bound claims whose trust comes from using this exact token."""
    if (not isinstance(token, str) or not token or token != token.strip() or
            len(token.encode('utf-8')) > _MAX_SERVICE_ACCOUNT_TOKEN_BYTES):
        raise ProtocolV2ActivationError(
            'The mounted service-account token is malformed.')
    segments = token.split('.')
    if len(segments) != 3 or any(not segment for segment in segments):
        raise ProtocolV2ActivationError(
            'The mounted service-account token is not a JWT.')
    payload_segment = segments[1]
    padded_payload = payload_segment + '=' * (-len(payload_segment) % 4)
    try:
        payload_bytes = base64.b64decode(padded_payload.encode('ascii'),
                                         altchars=b'-_',
                                         validate=True)
        if len(payload_bytes) > _MAX_SERVICE_ACCOUNT_TOKEN_BYTES:
            raise ValueError('JWT payload is too large.')
        payload = json.loads(payload_bytes.decode('utf-8'))
    except (binascii.Error, UnicodeError, ValueError):
        raise ProtocolV2ActivationError(
            'The mounted service-account token payload is malformed.') from None
    if not isinstance(payload, Mapping):
        raise ProtocolV2ActivationError(
            'The mounted service-account token payload is malformed.')
    kubernetes_claims = payload.get('kubernetes.io')
    if not isinstance(kubernetes_claims, Mapping):
        raise ProtocolV2ActivationError(
            'The mounted service-account token is not pod-bound.')
    pod_claim = kubernetes_claims.get('pod')
    namespace = kubernetes_claims.get('namespace')
    pod_name = pod_claim.get('name') if isinstance(pod_claim, Mapping) else None
    pod_uid = pod_claim.get('uid') if isinstance(pod_claim, Mapping) else None
    if any(not isinstance(value, str) or not value
           for value in (namespace, pod_name, pod_uid)):
        raise ProtocolV2ActivationError(
            'The mounted service-account token has no complete pod binding.')
    assert isinstance(namespace, str)
    assert isinstance(pod_name, str)
    assert isinstance(pod_uid, str)
    return _TokenBoundPodIdentity(namespace=namespace,
                                  name=pod_name,
                                  uid=pod_uid)


def _read_token_bound_pod_identity() -> tuple[str, _TokenBoundPodIdentity]:
    """Read one bounded mounted token and its required pod binding."""
    try:
        with open(kubernetes.IN_CLUSTER_TOKEN_PATH, 'rb') as token_file:
            if not stat.S_ISREG(os.fstat(token_file.fileno()).st_mode):
                raise ProtocolV2ActivationError(
                    'The mounted service-account token is not a regular file.')
            token_bytes = token_file.read(_MAX_SERVICE_ACCOUNT_TOKEN_BYTES + 1)
    except OSError as error:
        raise ProtocolV2ActivationError(
            'The mounted service-account token could not be read.') from error
    if not token_bytes or len(token_bytes) > _MAX_SERVICE_ACCOUNT_TOKEN_BYTES:
        raise ProtocolV2ActivationError(
            'The mounted service-account token has an invalid size.')
    try:
        token = token_bytes.decode('ascii')
    except UnicodeError:
        raise ProtocolV2ActivationError(
            'The mounted service-account token is malformed.') from None
    return token, _decode_token_bound_pod_identity(token)


def _required_object_string(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProtocolV2ActivationError(
            f'The rollout {description} is missing.')
    return value


def _required_positive_int(value: Any, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProtocolV2ActivationError(
            f'The rollout {description} is not a positive integer.')
    return value


def _replica_count(value: Any, description: str, *, none_is_zero: bool) -> int:
    if value is None and none_is_zero:
        return 0
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtocolV2ActivationError(
            f'The rollout {description} is not a replica count.')
    return value


def _deployment_exact_selector_labels(deployment: Any) -> dict[str, str]:
    selector = getattr(getattr(deployment, 'spec', None), 'selector', None)
    labels = getattr(selector, 'match_labels', None)
    expressions = getattr(selector, 'match_expressions', None)
    if expressions:
        # The shipped chart uses exact matchLabels.  Refusing expressions keeps
        # the proof cohort mechanically tied to a finite set of exact labels.
        raise ProtocolV2ActivationError(
            'A writer Deployment selector must use exact matchLabels only.')
    if not isinstance(labels, Mapping) or not labels:
        raise ProtocolV2ActivationError(
            'A writer Deployment has no exact selector labels.')
    normalized: dict[str, str] = {}
    for key, value in labels.items():
        if (not isinstance(key, str) or not key or not isinstance(value, str)):
            raise ProtocolV2ActivationError(
                'A writer Deployment has an invalid selector label.')
        normalized[key] = value
    return normalized


def _is_object_sequence(value: Any) -> TypeGuard[Sequence[Any]]:
    return (isinstance(value, Sequence) and
            not isinstance(value, (str, bytes, bytearray)))


def _named_container(containers: Any, container_name: str, description: str, *,
                     required: bool) -> Any | None:
    if not _is_object_sequence(containers):
        if required:
            raise ProtocolV2ActivationError(
                f'The {description} has no container inventory.')
        return None
    matches = [
        container for container in containers
        if getattr(container, 'name', None) == container_name
    ]
    if len(matches) > 1:
        raise ProtocolV2ActivationError(
            f'The {description} has duplicate {container_name!r} containers.')
    if not matches:
        if required:
            raise ProtocolV2ActivationError(
                f'The {description} has no {container_name!r} container.')
        return None
    return matches[0]


def _deployment_container(deployment: Any, container_name: str, *,
                          required: bool) -> Any | None:
    spec = getattr(deployment, 'spec', None)
    template_spec = getattr(getattr(spec, 'template', None), 'spec', None)
    return _named_container(getattr(template_spec, 'containers', None),
                            container_name,
                            'writer Deployment template',
                            required=required)


def _pod_container(pod: Any, container_name: str, *, required: bool) -> Any:
    spec = getattr(pod, 'spec', None)
    return _named_container(getattr(spec, 'containers', None),
                            container_name,
                            'writer Pod spec',
                            required=required)


def _literal_env_value(container: Any, name: str, description: str) -> str:
    env = getattr(container, 'env', None)
    if not _is_object_sequence(env):
        raise ProtocolV2ActivationError(
            f'The {description} has no literal {name} identity.')
    matches = [entry for entry in env if getattr(entry, 'name', None) == name]
    if len(matches) != 1:
        raise ProtocolV2ActivationError(
            f'The {description} does not have exactly one {name} identity.')
    entry = matches[0]
    value = getattr(entry, 'value', None)
    if (getattr(entry, 'value_from', None) is not None or
            not isinstance(value, str) or not value):
        raise ProtocolV2ActivationError(
            f'The {description} {name} identity is not a nonempty literal.')
    return value


def _labels_match(labels: Any, selector_labels: Mapping[str, str]) -> bool:
    return (isinstance(labels, Mapping) and all(
        labels.get(key) == value for key, value in selector_labels.items()))


def _required_label(resource: Any, label: str, description: str) -> str:
    labels = getattr(getattr(resource, 'metadata', None), 'labels', None)
    value = labels.get(label) if isinstance(labels, Mapping) else None
    if not isinstance(value, str) or not value:
        raise ProtocolV2ActivationError(
            f'The {description} has no {label!r} label.')
    return value


def _deployment_rollout_identity(
        deployment: Any, target: _WriterDeploymentTarget, *, namespace: str,
        release_name: str,
        helm_instance: str) -> tuple[str, str, str, int, dict[str, str]]:
    metadata = getattr(deployment, 'metadata', None)
    if getattr(metadata, 'deletion_timestamp', None) is not None:
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Deployment is terminating.')
    observed_name = _required_object_string(getattr(metadata, 'name', None),
                                            'Deployment name')
    observed_namespace = _required_object_string(
        getattr(metadata, 'namespace', None), 'Deployment namespace')
    if observed_name != target.name or observed_namespace != namespace:
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Deployment identity changed.')
    generation = _required_positive_int(getattr(metadata, 'generation', None),
                                        'Deployment generation')
    resource_version = _required_object_string(
        getattr(metadata, 'resource_version', None),
        'Deployment resourceVersion')
    deployment_uid = _required_object_string(getattr(metadata, 'uid', None),
                                             'Deployment UID')
    spec = getattr(deployment, 'spec', None)
    desired = _required_positive_int(getattr(spec, 'replicas', None),
                                     'desired replica count')
    status = getattr(deployment, 'status', None)
    observed_generation = _required_positive_int(
        getattr(status, 'observed_generation', None),
        'observed Deployment generation')
    if observed_generation != generation:
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Deployment controller has not observed '
            'its generation.')
    replica_counts = {
        'replicas': _replica_count(getattr(status, 'replicas', None),
                                   'current replica count',
                                   none_is_zero=False),
        'updated replicas': _replica_count(getattr(status, 'updated_replicas',
                                                   None),
                                           'updated replica count',
                                           none_is_zero=False),
        'ready replicas': _replica_count(getattr(status, 'ready_replicas',
                                                 None),
                                         'ready replica count',
                                         none_is_zero=False),
        'available replicas': _replica_count(getattr(status,
                                                     'available_replicas',
                                                     None),
                                             'available replica count',
                                             none_is_zero=False),
    }
    if any(count != desired for count in replica_counts.values()):
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Deployment rollout is not fully '
            'available: '
            f'desired={desired}, {replica_counts}.')
    unavailable = _replica_count(getattr(status, 'unavailable_replicas', None),
                                 'unavailable replica count',
                                 none_is_zero=True)
    if unavailable != 0:
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Deployment still has unavailable '
            'replicas.')
    container = _deployment_container(deployment,
                                      target.container_name,
                                      required=True)
    if (_literal_env_value(container, _RELEASE_NAME_ENV_VAR,
                           f'{target.role} writer Deployment')
            != release_name or _literal_env_value(
                container, _SERVER_ROLE_ENV_VAR,
                f'{target.role} writer Deployment') != target.server_role):
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Deployment runtime identity changed.')
    if (_required_label(deployment, _HELM_INSTANCE_LABEL,
                        f'{target.role} writer Deployment') != helm_instance or
            _required_label(
                getattr(getattr(deployment, 'spec', None), 'template',
                        None), _HELM_INSTANCE_LABEL,
                f'{target.role} writer Pod template') != helm_instance):
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Deployment release scope changed.')
    return (str(generation), resource_version, deployment_uid, desired,
            _deployment_exact_selector_labels(deployment))


def _pod_image_digest(pod: Any, container_name: str) -> str:
    status = getattr(pod, 'status', None)
    if getattr(status, 'phase', None) != 'Running':
        raise ProtocolV2ActivationError('A writer Pod is not Running.')
    conditions = getattr(status, 'conditions', None)
    if not isinstance(conditions, Sequence) or not any(
            getattr(condition, 'type', None) == 'Ready' and
            getattr(condition, 'status', None) in (True, 'True')
            for condition in conditions):
        raise ProtocolV2ActivationError('A writer Pod is not Ready.')
    container_statuses = getattr(status, 'container_statuses', None)
    if not isinstance(container_statuses, Sequence):
        raise ProtocolV2ActivationError(
            'A writer Pod has no container status cohort.')
    target_statuses = [
        container_status for container_status in container_statuses
        if getattr(container_status, 'name', None) == container_name
    ]
    if len(target_statuses) != 1:
        raise ProtocolV2ActivationError(
            f'A writer Pod does not have exactly one {container_name!r} '
            'container status.')
    target_status = target_statuses[0]
    if getattr(target_status, 'ready', None) is not True:
        raise ProtocolV2ActivationError(
            'The target writer container is not Ready.')
    image_id = getattr(target_status, 'image_id', None)
    if not isinstance(image_id, str):
        raise ProtocolV2ActivationError(
            'The target writer container has no immutable imageID.')
    match = _IMAGE_ID_DIGEST_PATTERN.search(image_id)
    if match is None:
        raise ProtocolV2ActivationError(
            'The target writer container imageID has no sha256 digest.')
    return match.group(1).lower()


def _controller_owner(resource: Any, kind: str,
                      description: str) -> tuple[str, str]:
    owner_references = getattr(getattr(resource, 'metadata', None),
                               'owner_references', None)
    if not _is_object_sequence(owner_references):
        raise ProtocolV2ActivationError(
            f'The {description} has no controller owner reference.')
    owners = [
        owner for owner in owner_references
        if getattr(owner, 'controller', None) is True and
        getattr(owner, 'kind', None) == kind and
        getattr(owner, 'api_version', None) == 'apps/v1'
    ]
    if len(owners) != 1:
        raise ProtocolV2ActivationError(
            f'The {description} is not controlled by exactly one {kind}.')
    return (_required_object_string(getattr(owners[0], 'name', None),
                                    f'{kind} owner name'),
            _required_object_string(getattr(owners[0], 'uid', None),
                                    f'{kind} owner UID'))


def _read_token_bound_api_owner(
        apps_api: Any, core_api: Any, identity: _TokenBoundPodIdentity
) -> tuple[Any, _DeploymentOwnerIdentity]:
    """Bind the authenticated Pod to its Deployment through immutable UIDs."""
    bound_pod = core_api.read_namespaced_pod(
        name=identity.name,
        namespace=identity.namespace,
        _request_timeout=kubernetes.API_TIMEOUT)
    metadata = getattr(bound_pod, 'metadata', None)
    observed_name = _required_object_string(getattr(metadata, 'name', None),
                                            'bound pod name')
    observed_namespace = _required_object_string(
        getattr(metadata, 'namespace', None), 'bound pod namespace')
    observed_uid = _required_object_string(getattr(metadata, 'uid', None),
                                           'bound pod UID')
    if (observed_name != identity.name or
            observed_namespace != identity.namespace or
            observed_uid != identity.uid):
        raise ProtocolV2ActivationError(
            'The authenticated pod binding does not match the live Pod.')
    _required_label(bound_pod, _HELM_INSTANCE_LABEL, 'authenticated API Pod')
    replica_set_name, replica_set_uid = _controller_owner(
        bound_pod, 'ReplicaSet', 'authenticated API Pod')
    replica_set = apps_api.read_namespaced_replica_set(
        name=replica_set_name,
        namespace=identity.namespace,
        _request_timeout=kubernetes.API_TIMEOUT)
    replica_set_metadata = getattr(replica_set, 'metadata', None)
    if (_required_object_string(getattr(replica_set_metadata, 'name', None),
                                'ReplicaSet name') != replica_set_name or
            _required_object_string(
                getattr(replica_set_metadata, 'namespace', None),
                'ReplicaSet namespace') != identity.namespace or
            _required_object_string(getattr(replica_set_metadata, 'uid', None),
                                    'ReplicaSet UID') != replica_set_uid):
        raise ProtocolV2ActivationError(
            'The authenticated API Pod owner ReplicaSet identity changed.')
    deployment_name, deployment_uid = _controller_owner(
        replica_set, 'Deployment', 'authenticated API Pod ReplicaSet')
    return bound_pod, _DeploymentOwnerIdentity(name=deployment_name,
                                               uid=deployment_uid)


def _deployment_inventory(deployment_list: Any,
                          namespace: str) -> dict[str, Any]:
    deployments = getattr(deployment_list, 'items', None)
    if not _is_object_sequence(deployments):
        raise ProtocolV2ActivationError(
            'The writer Deployment inventory is malformed.')
    inventory: dict[str, Any] = {}
    for deployment in deployments:
        metadata = getattr(deployment, 'metadata', None)
        name = _required_object_string(getattr(metadata, 'name', None),
                                       'Deployment inventory name')
        observed_namespace = _required_object_string(
            getattr(metadata, 'namespace', None),
            'Deployment inventory namespace')
        if observed_namespace != namespace or name in inventory:
            raise ProtocolV2ActivationError(
                'The writer Deployment inventory has an invalid identity.')
        inventory[name] = deployment
    return inventory


def _discover_writer_targets(
    deployments: dict[str,
                      Any], bound_pod: Any, api_owner: _DeploymentOwnerIdentity
) -> tuple[str, str, tuple[_WriterDeploymentTarget, ...]]:
    """Derive the complete chart writer topology from authenticated ownership."""
    api_deployment = deployments.get(api_owner.name)
    if api_deployment is None:
        raise ProtocolV2ActivationError(
            'The authenticated API Deployment is absent from inventory.')
    api_metadata = getattr(api_deployment, 'metadata', None)
    if _required_object_string(getattr(api_metadata, 'uid', None),
                               'API Deployment UID') != api_owner.uid:
        raise ProtocolV2ActivationError(
            'The authenticated API Deployment UID changed.')
    api_container = _deployment_container(api_deployment,
                                          _API_SERVER_CONTAINER_NAME,
                                          required=True)
    selector_labels = _deployment_exact_selector_labels(api_deployment)
    if not _labels_match(
            getattr(getattr(bound_pod, 'metadata', None), 'labels', None),
            selector_labels):
        raise ProtocolV2ActivationError(
            'The authenticated API Deployment does not select its bound Pod.')
    release_name = _literal_env_value(api_container, _RELEASE_NAME_ENV_VAR,
                                      'authenticated API Deployment')
    api_server_role = _literal_env_value(api_container, _SERVER_ROLE_ENV_VAR,
                                         'authenticated API Deployment')
    if api_server_role not in ('all', 'api'):
        raise ProtocolV2ActivationError(
            'The authenticated API Deployment has an invalid server role.')
    if api_owner.name != f'{release_name}-api-server':
        raise ProtocolV2ActivationError(
            'The authenticated API Deployment name does not match its '
            'chart-owned release identity.')
    helm_instance = _required_label(bound_pod, _HELM_INSTANCE_LABEL,
                                    'authenticated API Pod')

    matching: dict[str, list[str]] = {
        'api': [],
        'controller': [],
        'executor': [],
    }
    container_names = {
        'api': _API_SERVER_CONTAINER_NAME,
        'controller': _CONTROLLER_CONTAINER_NAME,
        'executor': _EXECUTOR_CONTAINER_NAME,
    }
    expected_controller_name = f'{release_name}-controller'
    expected_executor_name = f'{release_name}-executor'
    for name, deployment in deployments.items():
        for role, container_name in container_names.items():
            container = _deployment_container(deployment,
                                              container_name,
                                              required=False)
            if container is None:
                continue
            try:
                observed_release = _literal_env_value(
                    container, _RELEASE_NAME_ENV_VAR,
                    f'{role} writer Deployment candidate')
            except ProtocolV2ActivationError:
                if name in (api_owner.name, expected_controller_name,
                            expected_executor_name):
                    raise
                continue
            if observed_release != release_name:
                continue
            observed_server_role = _literal_env_value(
                container, _SERVER_ROLE_ENV_VAR,
                f'{role} writer Deployment candidate')
            if ((role == 'api' and observed_server_role not in ('all', 'api'))
                    or (role == 'controller' and
                        observed_server_role != 'controller') or
                (role == 'executor' and observed_server_role != 'executor')):
                raise ProtocolV2ActivationError(
                    f'The {role} writer Deployment candidate has an invalid '
                    'server role.')
            request_backend = _literal_env_value(
                container, _REQUEST_BACKEND_ENV_VAR,
                f'{role} writer Deployment candidate')
            if request_backend != 'postgres':
                raise ProtocolV2ActivationError(
                    f'The {role} writer Deployment candidate does not use '
                    'the PostgreSQL API request backend.')
            if _literal_env_value(
                    container, _QUIESCENCE_BACKEND_GUARD_ENV_VAR,
                    f'{role} writer Deployment candidate') != 'true':
                raise ProtocolV2ActivationError(
                    f'The {role} writer Deployment candidate does not '
                    'enforce built-in execution-quiescence backends.')
            observed_helm_instance = _required_label(
                deployment, _HELM_INSTANCE_LABEL,
                f'{role} writer Deployment candidate')
            if observed_helm_instance != helm_instance:
                raise ProtocolV2ActivationError(
                    f'The {role} writer Deployment crosses Helm release '
                    'scope.')
            matching[role].append(name)

    if matching['api'] != [api_owner.name]:
        raise ProtocolV2ActivationError(
            'The Helm release does not have exactly its authenticated API '
            'writer Deployment.')
    api_target = _WriterDeploymentTarget(
        role='api',
        name=api_owner.name,
        container_name=_API_SERVER_CONTAINER_NAME,
        server_role=api_server_role)
    if api_server_role == 'all':
        if (matching['controller'] or matching['executor'] or
                expected_controller_name in deployments or
                expected_executor_name in deployments):
            raise ProtocolV2ActivationError(
                'A compatibility API release has a separate controller or '
                'executor Deployment.')
        return release_name, helm_instance, (api_target,)
    if matching['controller'] != [expected_controller_name]:
        raise ProtocolV2ActivationError(
            'The HA API release does not have exactly its controller writer '
            'Deployment.')
    if matching['executor'] != [expected_executor_name]:
        raise ProtocolV2ActivationError(
            'The HA API release does not have exactly its executor writer '
            'Deployment.')
    return release_name, helm_instance, (
        api_target,
        _WriterDeploymentTarget(role='controller',
                                name=expected_controller_name,
                                container_name=_CONTROLLER_CONTAINER_NAME,
                                server_role='controller'),
        _WriterDeploymentTarget(role='executor',
                                name=expected_executor_name,
                                container_name=_EXECUTOR_CONTAINER_NAME,
                                server_role='executor'))


def _pod_identity(pod: Any, namespace: str) -> tuple[str, str, str]:
    metadata = getattr(pod, 'metadata', None)
    observed_namespace = _required_object_string(
        getattr(metadata, 'namespace', None), 'Pod namespace')
    if observed_namespace != namespace:
        raise ProtocolV2ActivationError(
            'The Pod inventory crosses namespace scope.')
    return (_required_object_string(getattr(metadata, 'name', None),
                                    'Pod name'),
            _required_object_string(getattr(metadata, 'uid', None), 'Pod UID'),
            _required_object_string(getattr(metadata, 'resource_version', None),
                                    'Pod resourceVersion'))


def _is_terminal_pod(pod: Any) -> bool:
    return getattr(getattr(pod, 'status', None), 'phase',
                   None) in ('Succeeded', 'Failed')


def _read_recent_writer_instances() -> tuple[_WriterProcessInstance, ...]:
    """Read every recent all/api/controller/executor lease from PostgreSQL."""
    try:
        rows = serve_state.get_recent_reserved_fill_writer_instances(
            _WRITER_INSTANCE_STALE_AFTER_SECONDS)
    except RuntimeError as error:
        raise ProtocolV2ActivationError(
            'The live writer-process inventory could not be read.') from error
    result: list[_WriterProcessInstance] = []
    for row in rows:
        role = row.role
        pod_name = row.pod_name
        pod_uid = row.pod_uid
        version = row.version
        request_storage_backend = row.request_storage_backend
        request_queue_backend = row.request_queue_backend
        execution_quiescence_capable = row.execution_quiescence_capable
        if (role not in ('all', 'api', 'controller', 'executor') or
                any(not isinstance(value, str) or not value
                    for value in (pod_name, pod_uid, version,
                                  request_storage_backend,
                                  request_queue_backend)) or
                not isinstance(execution_quiescence_capable, bool)):
            raise ProtocolV2ActivationError(
                'A recent writer-process lease has malformed Pod identity.')
        assert isinstance(role, str)
        assert isinstance(pod_name, str)
        assert isinstance(pod_uid, str)
        assert isinstance(version, str)
        assert isinstance(request_storage_backend, str)
        assert isinstance(request_queue_backend, str)
        result.append(
            _WriterProcessInstance(
                role=role,
                instance_id=row.instance_id,
                pod_name=pod_name,
                pod_uid=pod_uid,
                version=version,
                ready=row.ready,
                draining=row.draining,
                request_storage_backend=(request_storage_backend),
                request_queue_backend=request_queue_backend,
                execution_quiescence_capable=(execution_quiescence_capable)))
    return tuple(
        sorted(result,
               key=lambda item: (item.role, item.pod_uid, item.instance_id)))


def _validate_pod_runtime_identity(pod: Any, target: _WriterDeploymentTarget,
                                   release_name: str,
                                   helm_instance: str) -> None:
    container = _pod_container(pod, target.container_name, required=True)
    if (_literal_env_value(container, _RELEASE_NAME_ENV_VAR,
                           f'{target.role} writer Pod') != release_name or
            _literal_env_value(
                container, _SERVER_ROLE_ENV_VAR,
                f'{target.role} writer Pod') != target.server_role or
            _required_label(pod, _HELM_INSTANCE_LABEL,
                            f'{target.role} writer Pod') != helm_instance):
        raise ProtocolV2ActivationError(
            f'The {target.role} writer Pod runtime identity is malformed.')


def _validate_live_writer_pod_inventory(
        pods: Sequence[Any], deployments: Sequence[_WriterDeploymentSnapshot],
        *, namespace: str, release_name: str, helm_instance: str) -> None:
    attested: dict[str, tuple[str, str]] = {}
    for deployment in deployments:
        server_role = ('all' if deployment.role == 'api' and
                       len(deployments) == 1 else deployment.role)
        for pod_name, pod_uid, _ in deployment.pod_cohort:
            if pod_uid in attested:
                raise ProtocolV2ActivationError(
                    'A writer Pod is selected by multiple Deployments.')
            attested[pod_uid] = (server_role, pod_name)

    observed: dict[str, tuple[str, str]] = {}
    for pod in pods:
        pod_name, pod_uid, _ = _pod_identity(pod, namespace)
        labels = getattr(getattr(pod, 'metadata', None), 'labels', None)
        if (isinstance(labels, Mapping) and
                labels.get(_HELM_INSTANCE_LABEL) == helm_instance and
                labels.get(_MIGRATION_COMPONENT_LABEL) == _MIGRATION_COMPONENT
                and not _is_terminal_pod(pod)):
            raise ProtocolV2ActivationError(
                'A same-release database migration Pod is still active.')
        if _is_terminal_pod(pod):
            continue
        matched: list[tuple[str, str]] = []
        for role, container_name in (('api', _API_SERVER_CONTAINER_NAME),
                                     ('controller', _CONTROLLER_CONTAINER_NAME),
                                     ('executor', _EXECUTOR_CONTAINER_NAME)):
            container = _pod_container(pod, container_name, required=False)
            if container is None:
                continue
            pod_release = _literal_env_value(container, _RELEASE_NAME_ENV_VAR,
                                             f'{role} writer Pod candidate')
            if pod_release != release_name:
                continue
            if (not isinstance(labels, Mapping) or
                    labels.get(_HELM_INSTANCE_LABEL) != helm_instance):
                raise ProtocolV2ActivationError(
                    'A release writer Pod crosses Helm release scope.')
            server_role = _literal_env_value(container, _SERVER_ROLE_ENV_VAR,
                                             f'{role} writer Pod candidate')
            if ((role == 'api' and server_role not in ('all', 'api')) or
                (role == 'controller' and server_role != 'controller') or
                (role == 'executor' and server_role != 'executor')):
                raise ProtocolV2ActivationError(
                    'A Helm-scoped writer Pod has an invalid server role.')
            request_backend = _literal_env_value(
                container, _REQUEST_BACKEND_ENV_VAR,
                f'{role} writer Pod candidate')
            if request_backend != 'postgres':
                raise ProtocolV2ActivationError(
                    'A Helm-scoped writer Pod does not use the PostgreSQL API '
                    'request backend.')
            if _literal_env_value(container, _QUIESCENCE_BACKEND_GUARD_ENV_VAR,
                                  f'{role} writer Pod candidate') != 'true':
                raise ProtocolV2ActivationError(
                    'A Helm-scoped writer Pod does not enforce built-in '
                    'execution-quiescence backends.')
            matched.append((server_role, role))
        if not matched:
            continue
        if len(matched) != 1:
            raise ProtocolV2ActivationError(
                'A Pod has multiple writer-capable server containers.')
        if pod_uid in observed:
            raise ProtocolV2ActivationError(
                'The live writer Pod inventory has duplicate UIDs.')
        observed[pod_uid] = (matched[0][0], pod_name)
    if observed != attested:
        raise ProtocolV2ActivationError(
            'The live writer Pod inventory is not exactly the attested '
            'Deployment cohort.')


def _validate_writer_process_instances(
        instances: Sequence[_WriterProcessInstance],
        deployments: Sequence[_WriterDeploymentSnapshot]) -> None:
    expected: dict[str, tuple[str, str]] = {}
    for deployment in deployments:
        role = ('all' if deployment.role == 'api' and len(deployments) == 1 else
                deployment.role)
        for pod_name, pod_uid, _ in deployment.pod_cohort:
            expected[pod_uid] = (role, pod_name)
    observed: dict[str, tuple[str, str]] = {}
    for instance in instances:
        if (not instance.ready or instance.draining or
                instance.instance_id != instance.pod_uid or
                instance.pod_uid in observed):
            raise ProtocolV2ActivationError(
                'A recent writer-process lease is not one healthy '
                'Pod-bound instance.')
        if (instance.request_storage_backend
                != request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE or
                instance.request_queue_backend
                != request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE or
                not instance.execution_quiescence_capable):
            raise ProtocolV2ActivationError(
                'A recent writer-process lease does not attest the built-in '
                'PostgreSQL request storage and queue with execution '
                'quiescence support.')
        observed[instance.pod_uid] = (instance.role, instance.pod_name)
    if observed != expected:
        raise ProtocolV2ActivationError(
            'The shared database writer-process inventory is not exactly the '
            'attested Kubernetes writer cohort.')


def _read_writer_rollout_snapshot(
        apps_api: Any, core_api: Any, *, namespace: str, bound_pod: Any,
        api_owner: _DeploymentOwnerIdentity) -> _WriterRolloutSnapshot:
    deployment_list = apps_api.list_namespaced_deployment(
        namespace=namespace, _request_timeout=kubernetes.API_TIMEOUT)
    deployment_inventory = _deployment_inventory(deployment_list, namespace)
    release_name, helm_instance, targets = _discover_writer_targets(
        deployment_inventory, bound_pod, api_owner)
    deployment_rollouts = []
    for target in targets:
        deployment = deployment_inventory[target.name]
        rollout_identity = _deployment_rollout_identity(
            deployment,
            target,
            namespace=namespace,
            release_name=release_name,
            helm_instance=helm_instance)
        deployment_rollouts.append((target, rollout_identity))
    pod_list = core_api.list_namespaced_pod(
        namespace=namespace, _request_timeout=kubernetes.API_TIMEOUT)
    pods = getattr(pod_list, 'items', None)
    if not _is_object_sequence(pods):
        raise ProtocolV2ActivationError(
            'The writer Pod inventory is malformed.')
    snapshots: list[_WriterDeploymentSnapshot] = []
    for target, rollout_identity in deployment_rollouts:
        (generation, resource_version, deployment_uid, desired,
         selector_labels) = rollout_identity
        selected_pods = [
            pod for pod in pods if _labels_match(
                getattr(getattr(pod, 'metadata', None), 'labels', None),
                selector_labels)
        ]
        if len(selected_pods) != desired:
            raise ProtocolV2ActivationError(
                f'The {target.role} writer Deployment selector does not '
                'resolve to exactly its desired Pod cohort: '
                f'desired={desired}, observed={len(selected_pods)}.')
        digests: set[str] = set()
        cohort: list[tuple[str, str, str]] = []
        for pod in selected_pods:
            metadata = getattr(pod, 'metadata', None)
            if getattr(metadata, 'deletion_timestamp', None) is not None:
                raise ProtocolV2ActivationError('A writer Pod is terminating.')
            _validate_pod_runtime_identity(pod, target, release_name,
                                           helm_instance)
            cohort.append(_pod_identity(pod, namespace))
            digests.add(_pod_image_digest(pod, target.container_name))
        if len(digests) != 1:
            raise ProtocolV2ActivationError(
                f'The {target.role} writer Pod cohort has mixed immutable '
                'image digests.')
        snapshots.append(
            _WriterDeploymentSnapshot(
                role=target.role,
                deployment_name=target.name,
                deployment_generation=generation,
                deployment_resource_version=resource_version,
                deployment_uid=deployment_uid,
                container_name=target.container_name,
                image_digest=digests.pop(),
                pod_cohort=tuple(sorted(cohort))))
    _validate_live_writer_pod_inventory(pods,
                                        snapshots,
                                        namespace=namespace,
                                        release_name=release_name,
                                        helm_instance=helm_instance)
    writer_instances = _read_recent_writer_instances()
    _validate_writer_process_instances(writer_instances, snapshots)
    snapshot = _WriterRolloutSnapshot(release_name=release_name,
                                      deployments=tuple(snapshots),
                                      writer_instances=writer_instances)
    # Require a single image containing both this activation action and every
    # process that can mutate broker state.  Independent image overrides are
    # allowed only when they resolve to that same immutable digest.
    if not snapshot.deployments:
        raise ProtocolV2ActivationError(
            'The writer Deployment inventory is empty.')
    _ = snapshot.image_digest
    return snapshot


def _read_stable_writer_rollout() -> _WriterRolloutSnapshot:
    """Return a double-read proof of all writer processes for this database.

    The caller holds the global broker lock.  No identity is accepted from the
    caller: the mounted pod-bound token anchors an owner-UID chain, and its
    exact bytes authenticate the one bounded, no-refresh Kubernetes client.
    """
    token, identity = _read_token_bound_pod_identity()
    try:
        with kubernetes.in_cluster_core_and_apps_apis_for_token(token) as (
                core_api, apps_api):
            bound_pod, api_owner = _read_token_bound_api_owner(
                apps_api, core_api, identity)
            first = _read_writer_rollout_snapshot(apps_api,
                                                  core_api,
                                                  namespace=identity.namespace,
                                                  bound_pod=bound_pod,
                                                  api_owner=api_owner)
            second = _read_writer_rollout_snapshot(apps_api,
                                                   core_api,
                                                   namespace=identity.namespace,
                                                   bound_pod=bound_pod,
                                                   api_owner=api_owner)
            if second != first:
                raise ProtocolV2ActivationError(
                    'The API/controller/executor writer topology changed '
                    'between rollout proof reads.')
            token_pod = (identity.name, identity.uid)
            for snapshot in (first, second):
                api_deployments = [
                    deployment for deployment in snapshot.deployments
                    if deployment.role == 'api'
                ]
                if (len(api_deployments) != 1 or
                        not any((pod_name, pod_uid) == token_pod for pod_name,
                                pod_uid, _ in api_deployments[0].pod_cohort)):
                    raise ProtocolV2ActivationError(
                        'The token-bound pod UID is not in both verified API '
                        'Pod cohorts.')
            return second
    finally:
        del token


def activate_protocol_v2() -> bool:
    """Mechanically activate v2 from a stable complete writer rollout.

    No rollout identity or proof is accepted from the operator.  The mounted
    pod-bound service-account token supplies namespace, pod name, and pod UID;
    its exact bytes authenticate every Kubernetes read.  Both observations and
    the durable CAS occur under the global broker lock, excluding broker rounds
    and fill persists throughout activation.
    """
    lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
    with lock.acquire(blocking=True):
        engine = serve_state.get_database_engine()
        schema_revision = migration_utils.get_current_alembic_revision(
            engine, migration_utils.SERVE_DB_NAME)
        if schema_revision not in _PROTOCOL_V2_SCHEMA_REVISIONS:
            observed_revision = schema_revision or 'uninitialized'
            raise ProtocolV2ActivationError(
                'Reserved-fill protocol v2 requires exact Serve schema '
                'revision 035, 036, or 037; observed '
                f'{observed_revision}.')
        api_request_schema_revision = (
            migration_utils.get_current_alembic_revision(
                engine, migration_utils.API_REQUESTS_DB_NAME))
        if (api_request_schema_revision
                != _PROTOCOL_V2_API_REQUEST_SCHEMA_REVISION):
            observed_revision = api_request_schema_revision or 'uninitialized'
            raise ProtocolV2ActivationError(
                'Reserved-fill protocol v2 requires exact API-request schema '
                f'revision {_PROTOCOL_V2_API_REQUEST_SCHEMA_REVISION}; '
                f'observed {observed_revision}.')
        protocol_state = serve_state.get_reserved_fill_protocol_state()
        try:
            current_protocol = int(protocol_state['protocol_version'])
        except (KeyError, TypeError, ValueError) as error:
            raise ProtocolV2ActivationError(
                'The durable reserved-fill protocol state is malformed.'
            ) from error
        if current_protocol != PROTOCOL_V1:
            if current_protocol == PROTOCOL_V2:
                raise ProtocolV2ActivationError(
                    'Reserved-fill protocol v2 is already active.')
            raise ProtocolV2ActivationError(
                f'Unsupported durable reserved-fill protocol '
                f'{current_protocol}.')
        rollout = _read_stable_writer_rollout()
        changed = serve_state.set_reserved_fill_protocol_version(
            PROTOCOL_V2,
            expected_protocol_version=PROTOCOL_V1,
            image_digest=rollout.image_digest,
            deployment_generation=rollout.deployment_generation,
            deployment_uid=rollout.deployment_uid,
            pod_inventory_count=rollout.pod_inventory_count,
            pod_inventory_sha256=rollout.pod_inventory_sha256,
            changed_at=time.time())
        if changed:
            clear_caches()
        return changed


def demote_protocol_v1() -> bool:
    """Mechanically attest the live writers, rebuild v1 state, and demote."""
    lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
    with lock.acquire(blocking=True):
        engine = serve_state.get_database_engine()
        schema_revision = migration_utils.get_current_alembic_revision(
            engine, migration_utils.SERVE_DB_NAME)
        if schema_revision not in _PROTOCOL_V2_SCHEMA_REVISIONS:
            observed_revision = schema_revision or 'uninitialized'
            raise ProtocolV1DemotionError(
                'Reserved-fill protocol v1 demotion requires exact Serve '
                'schema revision 035, 036, or 037; observed '
                f'{observed_revision}.')
        api_request_schema_revision = (
            migration_utils.get_current_alembic_revision(
                engine, migration_utils.API_REQUESTS_DB_NAME))
        if (api_request_schema_revision
                != _PROTOCOL_V2_API_REQUEST_SCHEMA_REVISION):
            observed_revision = api_request_schema_revision or 'uninitialized'
            raise ProtocolV1DemotionError(
                'Reserved-fill protocol v1 demotion requires exact '
                'API-request schema revision '
                f'{_PROTOCOL_V2_API_REQUEST_SCHEMA_REVISION}; observed '
                f'{observed_revision}.')
        protocol_state = serve_state.get_reserved_fill_protocol_state()
        try:
            current_protocol = int(protocol_state['protocol_version'])
        except (KeyError, TypeError, ValueError) as error:
            raise ProtocolV1DemotionError(
                'The durable reserved-fill protocol state is malformed.'
            ) from error
        if current_protocol != PROTOCOL_V2:
            if current_protocol == PROTOCOL_V1:
                raise ProtocolV1DemotionError(
                    'Reserved-fill protocol v1 is already active.')
            raise ProtocolV1DemotionError(
                f'Unsupported durable reserved-fill protocol '
                f'{current_protocol}.')
        # The proof is deliberately observed, not supplied by an operator.  It
        # ensures no old/mixed writer can recreate a legacy-only row across the
        # projection inventory and gate transaction below.
        _read_stable_writer_rollout()
        changed = serve_state.set_reserved_fill_protocol_version(
            PROTOCOL_V1,
            expected_protocol_version=PROTOCOL_V2,
            changed_at=time.time())
        if not changed:
            raise ProtocolV1DemotionError(
                'The atomic legacy projection rebuild was rejected; remove '
                'multi-edge, malformed, or legacy-only reserved-fill claims '
                'before retrying demotion.')
        clear_caches()
        return True


# Sentinel returned by current_epoch while a pool's fence_pending marker
# is set: published epochs start at 1, so no launch ever carries it and
# the launch-path comparison fails closed (skip) without a special case.
_FENCE_PENDING_EPOCH = -1


def current_epoch(pool_key: str) -> int | None:
    """The POOL's current fencing epoch (cheap single-row DB read).

    Per-pool by design: rounds and grants are per-pool, so the launch
    fence must compare a carried epoch against ITS pool's round epoch.
    Fencing on the global lease epoch would let pool A's grant churn
    fence pool B's unrelated fill launches for up to two poll intervals.
    None (no round published yet) fails open at the fence: there is no
    newer allocation to defer to.

    A set fence_pending marker fails CLOSED: every grant issued before a
    lease-dead gap is suspect until an epoch-bumping publish clears the
    marker, so the sentinel returned here mismatches any carried epoch
    and the launch skips -- even for a pool that will never publish again
    (claims gone). add_replica_if_round_epoch enforces the same predicate
    atomically at persist time.
    """
    round_row = serve_state.get_reserved_fill_round(pool_key)
    if round_row is None:
        return None
    if bool(round_row['fence_pending']):
        return _FENCE_PENDING_EPOCH
    return int(round_row['epoch'])


def persist_fill_replica(
    service_name: str,
    replica_id: int,
    replica_info: Any,
    *,
    pool_key: str,
    expected_epoch: int,
    expected_protocol_version: int = PROTOCOL_V1,
    expected_service_generation: int = 0,
    expected_physical_cluster_uid: str | None = None,
    expected_ordinary_zero_cost_admission_sequence: int | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    expected_actuation_mode: str | None = None,
    actuation_lease: 'zero_cost_actuation.IntentLease | None' = None,
) -> bool:
    """Atomically persists a fill replica row, excluded from broker rounds.

    Ordering invariant (the other half lives on run_round_if_stale): a
    fill row must never become durable INSIDE a round's scan->publish
    window. The round's debit scan cannot see a row persisted after it
    ran, and the epoch fence cannot see a round that has not published
    yet -- a persist landing between the two is counted by neither, and
    the round re-feeds the just-taken slot to a peer. The round holds the
    cross-process broker lock for its whole body (scan through publish),
    so taking the same lock here leaves exactly two outcomes: the persist
    lands BEFORE the round's scan (the row is counted by the debit) or
    AFTER its publish (a superseded decision is fenced by the bumped
    epoch / fence_pending inside add_replica_if_round_epoch).

    PostgreSQL advisory locks can disappear server-side while this process
    still believes it owns one.  Before using an ordinary ORM connection, the
    persist advances the round lease epoch on the exact lock-owning session and
    carries that token into the insert transaction.  A replacement round
    advances the same epoch before its scan: either this transaction locks the
    token first and commits before that scan, or the replacement advances first
    and this persist fails closed.

    Non-blocking on purpose: a round in flight holds the lock across its
    whole cluster query, and blocking a scale-up batch that long is worse
    than skipping -- contention degrades into a fence-skip (False) and
    the autoscaler re-emits the launch on its next tick. The persist
    itself is one quick DB write, so a round waiting behind it is never
    delayed noticeably.
    """
    try:
        lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
        with lock.acquire(blocking=False):
            lease_token = None
            if isinstance(lock, locks.PostgresLock):
                try:
                    lease_token = lock.run_in_lock_session(
                        serve_state.advance_reserved_fill_persist_token)
                except Exception as error:  # pylint: disable=broad-except
                    # The dedicated advisory-lock session may have died before
                    # or during the token transaction.  This launch is not
                    # authorized to fall back to an unrelated ORM connection.
                    logger.error('Reserved-fill broker: persist fencing token '
                                 f'advance failed; skipping fill launch: '
                                 f'{error}')
                    return False
                if lease_token is None:
                    logger.error('Reserved-fill broker: could not advance the '
                                 'persist fencing token; skipping fill launch.')
                    return False
            return serve_state.add_replica_if_round_epoch(
                service_name,
                replica_id,
                replica_info,
                pool_key=pool_key,
                expected_epoch=expected_epoch,
                expected_protocol_version=expected_protocol_version,
                expected_service_generation=expected_service_generation,
                expected_physical_cluster_uid=(expected_physical_cluster_uid),
                expected_ordinary_zero_cost_admission_sequence=(
                    expected_ordinary_zero_cost_admission_sequence),
                expected_lease_token=lease_token,
                expected_service_hash=expected_service_hash,
                expected_controller_owner=expected_controller_owner,
                expected_actuation_mode=expected_actuation_mode,
                actuation_lease=actuation_lease)
    except locks.LockTimeout:
        return False


# ============================== Round driver ================================


def _claim_rows(protocol_version: int,
                pool_key: str | None = None) -> list[dict[str, Any]]:
    if protocol_version == PROTOCOL_V2:
        return serve_state.get_authoritative_reserved_fill_claims(
            pool_key=pool_key)
    return serve_state.get_reserved_fill_claims(pool_key=pool_key)


def _claim_generation(row: dict[str, Any], protocol_version: int) -> int:
    if protocol_version == PROTOCOL_V1:
        return 0
    try:
        generation = int(row['service_generation'])
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError('Protocol-v2 claim is missing a valid service '
                         f'generation: {row!r}.') from e
    if generation < 1:
        raise ValueError('Protocol-v2 service generations must be positive; '
                         f'got {generation!r}.')
    return generation


def _claim_round_metadata(
    pool_key: str,
    rows: dict[str, dict[str, Any]],
    protocol_version: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, int], tuple[str, ...], str |
           None]:
    """Validate and normalize the authority carried by one round."""
    identity = parse_pool_identity(pool_key)
    if identity.protocol_version != protocol_version:
        raise ValueError('Pool-key protocol does not match the durable '
                         f'protocol: pool={pool_key!r}, durable='
                         f'{protocol_version}.')
    normalized: dict[str, dict[str, Any]] = {}
    generations: dict[str, int] = {}
    access_contexts: set[str] = set()
    for name, raw_row in rows.items():
        row = dict(raw_row)
        generations[name] = _claim_generation(row, protocol_version)
        if protocol_version == PROTOCOL_V2:
            row_uid = row.get('physical_cluster_uid')
            if (not isinstance(row_uid, str) or not row_uid or
                    row_uid != identity.physical_cluster_uid):
                raise ValueError(
                    'Protocol-v2 claim physical UID does not match its pool '
                    f'key for {name!r}/{pool_key}.')
            access_context = row.get('access_context')
            if not isinstance(access_context, str) or not access_context:
                raise ValueError('Protocol-v2 claim is missing its access '
                                 f'context for {name!r}/{pool_key}.')
            access_contexts.add(access_context)
            try:
                raw_cap = row.get('effective_cap')
                if raw_cap is None:
                    raise ValueError('missing')
                row['effective_cap'] = max(0, int(raw_cap))
            except (TypeError, ValueError):
                # A v2 edge is always partitioned and therefore always has a
                # finite cap. Corrupt/migration-shadow input cannot revive the
                # v1 unbounded semantics; clamp that edge to zero.
                logger.error('Protocol-v2 claim has no valid edge cap; '
                             f'clamping {name!r}/{pool_key} to zero.')
                row['effective_cap'] = 0
        normalized[name] = row
    return (normalized, generations, tuple(sorted(access_contexts)),
            identity.physical_cluster_uid)


def _remove_claim_for_protocol(
    protocol_version: int,
    service_name: str,
    *,
    pool_key: str | None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
) -> bool:
    if protocol_version == PROTOCOL_V2:
        if pool_key is None:
            if expected_service_hash is None:
                logger.error('Removing a protocol-v2 complete claim set '
                             'requires an exact service owner hash.')
                return False
            return serve_state.remove_reserved_fill_claim_set(
                service_name,
                expected_service_hash=expected_service_hash,
                expected_controller_owner=expected_controller_owner)
        return serve_state.remove_authoritative_reserved_fill_claim(
            service_name,
            pool_key=pool_key,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
    return serve_state.remove_reserved_fill_claim(
        service_name,
        expected_service_hash=expected_service_hash,
        expected_controller_owner=expected_controller_owner)


def _remove_legacy_claims_for_pool(pool_key: str) -> None:
    serve_state.remove_reserved_fill_claims_for_pool(pool_key)


def _prune_claims(protocol_version: int, expired_before: float) -> list[Any]:
    if protocol_version == PROTOCOL_V2:
        pruned = serve_state.prune_authoritative_reserved_fill_claim_sets(
            expired_before)
        for service_name in pruned:
            _clear_service_cache(str(service_name))
        return pruned
    return serve_state.prune_reserved_fill_claims(expired_before)


def replace_claim_set(
    service_name: str,
    *,
    semantic_hash: str,
    global_headroom: int,
    utilization_ceiling: int,
    utilization_state: Any,
    edges: Sequence[dict[str, Any]],
    expected_service_hash: str | None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
) -> int | None:
    """Atomically heartbeat one complete authoritative protocol-v2 set.

    The owner fence is mandatory at this facade. A successful replacement
    returns the authoritative service generation and invalidates every cached
    edge absent from that set or produced by an earlier generation.
    """
    if expected_service_hash is None:
        _clear_service_cache(service_name)
        logger.error('Protocol-v2 claim-set replacement requires an exact '
                     f'service owner hash for {service_name!r}.')
        return None
    if not semantic_hash:
        raise ValueError('Protocol-v2 claim sets require a semantic hash.')
    if (isinstance(global_headroom, bool) or
            not isinstance(global_headroom, int) or global_headroom < 0 or
            isinstance(utilization_ceiling, bool) or
            not isinstance(utilization_ceiling, int) or
            utilization_ceiling < 0):
        raise ValueError('Protocol-v2 global budgets must be nonnegative.')
    normalized_edges: list[dict[str, Any]] = []
    pool_keys: set[str] = set()
    physical_uid_by_access_context: dict[str, str] = {}
    identities: list[tuple[str, PoolIdentity]] = []
    for raw_edge in edges:
        edge = dict(raw_edge)
        pool_key = edge.get('pool_key')
        if not isinstance(pool_key, str) or not pool_key:
            raise ValueError('Every protocol-v2 edge requires a pool key.')
        identity = parse_pool_identity(pool_key)
        if identity.protocol_version != PROTOCOL_V2:
            raise ValueError('An authoritative protocol-v2 claim set cannot '
                             f'contain a v1 pool key: {pool_key!r}.')
        if pool_key in pool_keys:
            raise ValueError('A protocol-v2 complete set cannot contain '
                             f'duplicate pool edge {pool_key!r}.')
        pool_keys.add(pool_key)
        physical_uid = edge.get('physical_cluster_uid')
        if (not isinstance(physical_uid, str) or not physical_uid or
                physical_uid != identity.physical_cluster_uid):
            raise ValueError('Protocol-v2 edge physical UID does not match '
                             f'its pool key: {pool_key!r}.')
        access_context = edge.get('access_context')
        if not isinstance(access_context, str) or not access_context:
            raise ValueError('Every protocol-v2 edge requires an access '
                             f'context: {pool_key!r}.')
        context_uid = physical_uid_by_access_context.setdefault(
            access_context, physical_uid)
        if context_uid != physical_uid:
            raise ValueError('One protocol-v2 access context cannot identify '
                             'multiple physical clusters: '
                             f'{access_context!r}.')
        for prior_key, prior_identity in identities:
            if (prior_identity.physical_cluster_uid
                    == identity.physical_cluster_uid and
                    set(prior_identity.gpu_names).intersection(
                        identity.gpu_names)):
                raise ValueError('A protocol-v2 complete set contains '
                                 'overlapping accelerator groups on one '
                                 f'physical cluster: {prior_key!r} and '
                                 f'{pool_key!r}.')
        identities.append((pool_key, identity))
        effective_cap = edge.get('effective_cap')
        if (isinstance(effective_cap, bool) or
                not isinstance(effective_cap, int)):
            raise ValueError('Every protocol-v2 edge requires an integer '
                             f'effective cap: {pool_key!r}.')
        if effective_cap < 0:
            raise ValueError('Protocol-v2 edge caps must be nonnegative: '
                             f'{pool_key!r}.')
        edge['effective_cap'] = effective_cap
        normalized_edges.append(edge)

    claim_scope = None
    claim_authorization = None
    service_version = None
    try:
        gate = (pool_capacity_observation.PoolCapacityObservationRepository().
                read_reconciliation_gate())
        if gate.sequenced_active:
            service = serve_state.get_service_status_snapshot(service_name)
            if (service is None or
                    service.get('hash') != expected_service_hash or
                (service.get('controller_pid'), service.get('controller_ip'))
                    != expected_controller_owner):
                raise reserved_fill_reclaim_attestation.ReclaimAttestationError(
                    'Sequenced claim owner or incarnation is not current.')
            raw_version = service.get('version')
            if type(raw_version) is not int or raw_version < 1:
                raise reserved_fill_reclaim_attestation.ReclaimAttestationError(
                    'Sequenced claim has no current committed service version.')
            service_version = raw_version
            (found, _, _,
             worker_projections) = (serve_state.get_placement_projection_record(
                 service_name, service_version))
            if not found:
                raise reserved_fill_reclaim_attestation.ReclaimAttestationError(
                    'Sequenced claim service version is not committed.')
            claim_edges = []
            for edge in normalized_edges:
                identity = parse_pool_identity(str(edge['pool_key']))
                projected_admissions = (
                    serve_state.reserved_fill_reclaim_projected_admissions(
                        worker_projections,
                        access_context=str(edge['access_context']),
                        accelerator_names=identity.gpu_names,
                        accelerator_count=int(edge['gpus_per_replica'])))
                edge['worker_projection_sha256_by_accelerator'] = {
                    admission.accelerator: admission.worker_projection_sha256
                    for admission in projected_admissions
                }
                claim_edges.append(
                    reserved_fill_reclaim_attestation.ReclaimClaimEdge(
                        pool_key=str(edge['pool_key']),
                        access_context=str(edge['access_context']),
                        physical_cluster_uid=str(edge['physical_cluster_uid']),
                        accelerator_names=tuple(sorted(identity.gpu_names)),
                        projected_admissions=projected_admissions))
            # The broker is the sole point that combines caller-owned pool
            # semantics with the exact immutable version/projection authority.
            # Hash that completed payload so either a version or one candidate
            # projection advances the durable claim generation.
            semantic_hash = hashlib.sha256(
                json.dumps(
                    {
                        'base_semantic_hash': semantic_hash,
                        'service_version': service_version,
                        'worker_projection_sha256_by_pool': {
                            str(edge['pool_key']):
                                edge['worker_projection_sha256_by_accelerator']
                            for edge in sorted(
                                normalized_edges,
                                key=lambda item: str(item['pool_key']))
                        },
                    },
                    sort_keys=True,
                    separators=(',', ':'),
                    allow_nan=False).encode('utf-8')).hexdigest()
            claim_scope = (
                reserved_fill_reclaim_attestation.ReclaimClaimSetScope(
                    service_name=service_name,
                    service_incarnation=expected_service_hash,
                    service_version=service_version,
                    semantic_hash=semantic_hash,
                    edges=tuple(sorted(claim_edges))))
            reclaim_identity = gate.reclaim_policy_identity
            if reclaim_identity is None:
                raise reserved_fill_reclaim_attestation.ReclaimAttestationError(
                    'Sequenced reconciliation has no reclaim-policy identity.')
            policy = reserved_fill_reclaim_attestation.require_unique_policy()
            (reserved_fill_reclaim_attestation.require_exact_policy_identity)(
                policy, reclaim_identity)
            policy_deadline = (reserved_fill_reclaim_attestation.
                               new_policy_operation_deadline())
            claim_authorization = policy.authorize_claim_set(
                claim_scope,
                expected_identity=reclaim_identity,
                expected_gate_generation=gate.generation,
                deadline_monotonic=policy_deadline)
            (reserved_fill_reclaim_attestation.
             require_policy_operation_completed)(policy_deadline)
            (reserved_fill_reclaim_attestation.require_exact_claim_authorization
            )(claim_authorization,
              expected_identity=reclaim_identity,
              expected_gate_generation=gate.generation,
              expected_scope=claim_scope)
    except Exception as error:  # pylint: disable=broad-except
        _clear_service_cache(service_name)
        logger.error(
            'Reserved-fill broker: reclaim policy refused the '
            'complete claim set for %r: %s', service_name,
            common_utils.format_exception(error))
        return None

    lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
    with lock.acquire(blocking=True):
        if get_protocol_version() != PROTOCOL_V2:
            _clear_service_cache(service_name)
            logger.error('Reserved-fill protocol v2 is not active; refusing '
                         f'the complete claim set of {service_name!r}.')
            return None
        heartbeat_ts = time.time()
        _prune_claims(PROTOCOL_V2, heartbeat_ts - claim_ttl_seconds())
        for existing in _claim_rows(PROTOCOL_V2):
            if existing['service_name'] == service_name:
                continue
            existing_pool = str(existing['pool_key'])
            for pool_key in pool_keys:
                if (existing_pool != pool_key and
                        _pool_keys_overlap(existing_pool, pool_key)):
                    logger.error(
                        'Reserved-fill broker: rejecting the complete claim '
                        f'set of {service_name!r}; pool {pool_key} overlaps '
                        f'{existing_pool} claimed by '
                        f'{existing["service_name"]!r}.')
                    serve_state.remove_reserved_fill_claim_set(
                        service_name,
                        expected_service_hash=expected_service_hash,
                        expected_controller_owner=expected_controller_owner)
                    _clear_service_cache(service_name)
                    return None
        generation = serve_state.replace_reserved_fill_claim_set(
            service_name,
            semantic_hash=semantic_hash,
            global_headroom=global_headroom,
            utilization_ceiling=utilization_ceiling,
            utilization_state=utilization_state,
            edges=normalized_edges,
            heartbeat_ts=heartbeat_ts,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner,
            service_version=service_version,
            reclaim_claim_scope=claim_scope,
            reclaim_claim_authorization=claim_authorization)
        if generation is None:
            _clear_service_cache(service_name)
            return None
        generation = int(generation)
        edge_by_pool = {
            str(edge['pool_key']): edge for edge in normalized_edges
        }
        with _GRANT_CACHE_LOCK:
            for cache_key, entry in list(_GRANT_CACHE.items()):
                cached_service, cached_pool = cache_key
                if cached_service != service_name:
                    continue
                current_edge = edge_by_pool.get(str(cached_pool))
                if (current_edge is None or
                        entry.service_generation != generation or
                        entry.access_context != current_edge['access_context']
                        or entry.physical_cluster_uid
                        != current_edge['physical_cluster_uid']):
                    _GRANT_CACHE.pop(cache_key, None)
        return generation


def upsert_claim(
    service_name: str,
    *,
    pool_key: str,
    weight: float,
    floor_replicas: int,
    gpus_per_replica: int,
    holdings_fill: int,
    launchable: bool,
    effective_cap: int | None = None,
    activity: dict[str, Any] | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Upsert one heartbeat without allowing overlapping pool groups."""
    lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
    with lock.acquire(blocking=True):
        if get_protocol_version() != PROTOCOL_V1:
            logger.error('Reserved-fill protocol v2 requires an atomic '
                         'complete claim-set heartbeat; refusing the legacy '
                         f'one-edge upsert for {service_name!r}.')
            return False
        now = time.time()
        for row in serve_state.get_reserved_fill_claims():
            if row['service_name'] == service_name:
                continue
            if now - float(row['heartbeat_ts'] or 0) > claim_ttl_seconds():
                continue
            other_pool_key = row['pool_key']
            if (other_pool_key != pool_key and
                    _pool_keys_overlap(pool_key, other_pool_key)):
                logger.error(
                    'Reserved-fill broker: rejecting claim of '
                    f'{service_name!r} for overlapping pool group '
                    f'{pool_key}; {row["service_name"]!r} already claims '
                    f'{other_pool_key}. Use the same accelerator group for '
                    'shared arbitration.')
                serve_state.remove_reserved_fill_claim(
                    service_name,
                    expected_service_hash=expected_service_hash,
                    expected_controller_owner=expected_controller_owner)
                _clear_service_cache(service_name)
                return False
        return serve_state.upsert_reserved_fill_claim(
            service_name,
            pool_key=pool_key,
            weight=weight,
            floor_replicas=floor_replicas,
            gpus_per_replica=gpus_per_replica,
            holdings_fill=holdings_fill,
            effective_cap=effective_cap,
            launchable=launchable,
            heartbeat_ts=now,
            demonstrated_need=(None if activity is None or
                               activity.get('demonstrated_need') is None else
                               int(activity['demonstrated_need'])),
            boot_hold=(None
                       if activity is None else bool(activity['boot_hold'])),
            # Paired with heartbeat_ts from the SAME `now`, in the same
            # statement, so the freshness comparison downstream is exact
            # and epsilon-free. A writer that predates the gate advances
            # heartbeat_ts without touching activity_ts, which is precisely
            # what the lag check downstream detects.
            activity_ts=(None if activity is None else now),
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)


def remove_claim(
    service_name: str,
    pool_key: str | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Remove one v2 edge, or every claim of a service when pool is None."""
    lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
    with lock.acquire(blocking=True):
        protocol_version = get_protocol_version()
        removed = _remove_claim_for_protocol(
            protocol_version,
            service_name,
            pool_key=pool_key,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
        if removed or expected_service_hash is None:
            # A v2 edge removal advances the complete service-set generation,
            # invalidating cached allocations for every sibling edge too.
            _clear_service_cache(service_name)
        return removed


def utilization_gate_enabled() -> bool:
    """Process-wide kill switch for the utilization gate."""
    override = os.environ.get(constants.RESERVED_FILL_UTILIZATION_GATE_ENV_VAR)
    if override is None:
        return True
    return override.strip().lower() not in ('0', 'false', 'no', 'off', '')


@dataclasses.dataclass(frozen=True)
class ActivityInput:
    """One claimant's utilization signal, or the absence of one.

    `armed` distinguishes an explicit/static opt-out from a default-gated
    claimant whose utilization is temporarily unobservable. `blind` means an
    armed gate cannot tell idle from active and must follow its bounded blind
    grace instead of treating the missing sample as confirmed zero.
    """
    armed: bool
    demonstrated_need: int
    boot_hold: bool
    blind: bool


def _activity_input(row: dict[str, Any]) -> ActivityInput:
    """Reads a claim's utilization signal, rejecting stale or absent ones."""
    ungated = ActivityInput(armed=False,
                            demonstrated_need=0,
                            boot_hold=False,
                            blind=True)
    if not utilization_gate_enabled():
        return ungated
    activity_ts = row.get('activity_ts')
    if activity_ts is None:
        # All activity columns NULL is the durable explicit opt-out (and the
        # shape of a pre-gate writer). It must clear any prior governor state,
        # not freeze a cap left behind before a service update disabled the
        # gate.
        return ungated
    blind = ActivityInput(armed=True,
                          demonstrated_need=0,
                          boot_hold=False,
                          blind=True)
    try:
        lag = float(row['heartbeat_ts'] or 0.0) - float(activity_ts)
    except (TypeError, ValueError):
        return blind
    if not 0 <= lag <= constants.RESERVED_FILL_ACTIVITY_MAX_LAG_SECONDS:
        # VERSION-SKEW GUARD. upsert builds its values dict from the columns
        # its own binary knows, and the ON CONFLICT set_ iterates that dict,
        # so a pre-gate binary heartbeating a migrated row advances
        # heartbeat_ts while freezing this signal. Trusting a frozen
        # demonstrated_need of 0 would walk a fully busy service down to its
        # floor. Failing to blind here is the whole reason activity_ts
        # exists; a negative lag is equally untrustworthy (clock surgery or
        # a hand-edited row).
        return blind
    need = row.get('demonstrated_need')
    if need is None:
        # A current gated writer deliberately pairs a fresh activity_ts with
        # NULL need when no detailed utilization sample is available. This is
        # armed-but-blind, not confirmed idle and not an opt-out.
        return blind
    return ActivityInput(armed=True,
                         demonstrated_need=max(0, int(need)),
                         boot_hold=bool(row.get('boot_hold')),
                         blind=False)


def _apply_utilization_gate(
    claims: dict[str, reserved_capacity_allocation.ClaimInput],
    activity: dict[str, 'ActivityInput'],
    prev_state: dict[str, dict[str, Any]],
    now: float,
) -> tuple[dict[str, reserved_capacity_allocation.ClaimInput], dict[str, dict[
        str, Any]]]:
    """Advance every claimant's release target and attach it as a cap.

    Returns the claims with utilization_cap set and the new state to persist
    on the round row. A gated claimant decays to zero while idle and recovers
    a utilization-proportional cap while active; the cap can remain below its
    declared floor. A claimant with no entry in the returned state is
    explicitly ungated, which preserves static-reservation behavior.
    """
    if not utilization_gate_enabled():
        # PROCESS-WIDE KILL SWITCH. The behavior contract (requirement 2)
        # is that a disabled gate leaves every service ungated at exactly
        # today's entitlement and "never fails toward release", and the env
        # var "disables it for every service in the process" (requirement
        # 10). Relying on _activity_input returning `blind` is not enough:
        # an already-gated claimant (one carrying prev release state) would
        # take the blind FREEZE path below instead, holding its decayed cap
        # and, past RESERVED_FILL_BLIND_GRACE_SECONDS, resuming the decay
        # toward its floor on a service the operator just told the gate to
        # stop touching. Force-ungate here and drop all release state; the
        # empty state clears utilization_state on the round row (the writer
        # publishes NULL for a falsy state), exactly as "disarming must
        # clear the state" requires, so re-enabling re-arms from current
        # holdings rather than resuming a half-finished decay.
        return claims, {}
    gated: dict[str, reserved_capacity_allocation.ClaimInput] = {}
    state: dict[str, dict[str, Any]] = {}
    for name, claim in claims.items():
        signal = activity[name]
        prev = prev_state.get(name)
        if not signal.armed:
            # Explicit utilization_gate:false (or a pre-gate all-NULL row).
            # Omit it from the rebuilt state even when `prev` exists so an
            # update that opts out restores the static reservation now rather
            # than freezing the last decayed cap.
            gated[name] = claim
            continue
        entry = advance_release_target(
            prev,
            # A gated reservation is retained only while utilization is
            # demonstrated. Idle releases all fill capacity and active
            # entitlement is proportional to demonstrated need.
            floor=0,
            holdings=claim.holdings_fill,
            need=signal.demonstrated_need,
            boot_hold=signal.boot_hold,
            blind=signal.blind,
            now=now,
            dwell=constants.RESERVED_FILL_IDLE_DWELL_SECONDS,
            step_seconds=constants.RESERVED_FILL_RELEASE_STEP_SECONDS,
            step_fraction=constants.RESERVED_FILL_RELEASE_STEP_FRACTION,
            min_step=constants.RESERVED_FILL_RELEASE_MIN_STEP,
            headroom=constants.RESERVED_FILL_UTILIZATION_HEADROOM,
            blind_grace=constants.RESERVED_FILL_BLIND_GRACE_SECONDS,
        )
        state[name] = entry
        # An already-gated claimant keeps its cap applied even while blind.
        # Dropping the cap on a blind round would be a RISE, restoring full
        # weighted entitlement the moment telemetry blips; since every serve
        # controller is a process in the api-server pod, one deploy would
        # un-decay the whole pool at once. advance_release_target froze the
        # value rather than moving it, so what binds here is the level the
        # claimant had already earned.
        gated[name] = dataclasses.replace(claim,
                                          utilization_cap=int(entry['cap']))
    return gated, state


def _claim_input(
        row: dict[str, Any]) -> reserved_capacity_allocation.ClaimInput:
    effective_cap = row.get('effective_cap')
    weight = float(row['weight'] or 1.0)
    if not math.isfinite(weight):
        # Defensive: SkyServiceSpec rejects non-finite weights, but a
        # poisoned DB row (older writer, manual surgery) must not crash
        # water-filling (inf/inf -> NaN in rounding) EVERY round for the
        # pool while the claim stays live. Clamp loudly to the default.
        logger.warning(
            f'Reserved-fill broker: claim of {row["service_name"]!r} '
            f'carries a non-finite weight {weight!r}; clamping to 1.0.')
        weight = 1.0
    elif weight > constants.RESERVED_FILL_MAX_WEIGHT:
        # Same defense for finite-but-out-of-bound weights (the spec
        # rejects them at construction): clamp to the documented bound so
        # a poisoned row degrades to an extreme-but-finite share instead
        # of crashing rounds (the water-fill normalization above is the
        # second layer).
        logger.warning(
            f'Reserved-fill broker: claim of {row["service_name"]!r} '
            f'carries weight {weight!r} above the supported maximum; '
            f'clamping to {constants.RESERVED_FILL_MAX_WEIGHT}.')
        weight = float(constants.RESERVED_FILL_MAX_WEIGHT)
    return ClaimInput(floor=int(row['floor_replicas'] or 0),
                      weight=weight,
                      holdings_fill=int(row['holdings_fill'] or 0),
                      launchable=bool(row['launchable']),
                      effective_cap=(int(effective_cap)
                                     if effective_cap is not None else None))


def _clamp_v2_grants(
    grants: dict[str, int],
    claims: dict[str, reserved_capacity_allocation.ClaimInput],
    protocol_version: int,
) -> dict[str, int]:
    """Enforce partitioned edge caps even across damping/blackout carries."""
    if protocol_version != PROTOCOL_V2:
        return grants
    return {
        name: min(max(0, int(grants.get(name, 0))),
                  max(0, int(claim.effective_cap or
                             0))) for name, claim in claims.items()
    }


def _normalize_exact_card_observation(
    observation: PoolObservation,
    pool_key: str,
) -> dict[str, int] | None:
    """Validate the optional exact-card decomposition of one measurement.

    The aggregate remains the broker's allocation unit, but a present split is
    authoritative launch metadata.  A malformed split therefore invalidates
    the measurement instead of silently degrading to an unshaped launch.
    """
    raw = observation.free_slots_by_accelerator
    if raw is None:
        return None
    identity = parse_pool_identity(pool_key)
    allowed_cards = set(identity.gpu_names)
    normalized: dict[str, int] = {}
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError('exact-card observation entries must be pairs')
        raw_card, raw_count = item
        if not isinstance(raw_card, str) or not raw_card:
            raise ValueError('exact-card observation has an invalid card name')
        card = raw_card.casefold()
        if card not in allowed_cards:
            raise ValueError(
                f'exact-card observation {card!r} is outside pool {pool_key!r}')
        if card in normalized:
            raise ValueError(
                f'exact-card observation repeats accelerator {card!r}')
        if (isinstance(raw_count, bool) or not isinstance(raw_count, int) or
                raw_count < 0):
            raise ValueError(
                f'exact-card observation has invalid count {raw_count!r}')
        normalized[card] = raw_count
    measured = max(0, int(observation.free_slots or 0))
    if sum(normalized.values()) != measured:
        raise ValueError(
            'exact-card observation does not sum to its aggregate '
            f'free slots ({sum(normalized.values())} != {measured})')
    return normalized


def _allocate_feed_by_accelerator(
    feeds: dict[str, int],
    measured_by_accelerator: dict[str, int] | None,
    observed_free: int,
) -> dict[str, dict[str, int]] | None:
    """Partition aggregate service feeds over the measured exact cards.

    Aggregate debits can reduce ``observed_free`` below the raw measurement
    without an exact-card identity (legacy rows are the important case).  Clip
    the card budget to that conserved aggregate in measured-card order, then
    assign each service's already-arbitrated feed in stable service order.
    This may conservatively withhold a usable card, but can never invent one.
    """
    if measured_by_accelerator is None:
        return None
    remaining_total = max(0, int(observed_free))
    remaining_by_card: dict[str, int] = {}
    for card, measured in measured_by_accelerator.items():
        available = min(max(0, int(measured)), remaining_total)
        remaining_by_card[card] = available
        remaining_total -= available

    result: dict[str, dict[str, int]] = {}
    for service_name in sorted(feeds):
        remaining_feed = max(0, int(feeds[service_name]))
        service_cards: dict[str, int] = {}
        for card, card_remaining in remaining_by_card.items():
            if remaining_feed <= 0:
                break
            assigned = min(remaining_feed, card_remaining)
            if assigned <= 0:
                continue
            service_cards[card] = assigned
            remaining_by_card[card] -= assigned
            remaining_feed -= assigned
        result[service_name] = service_cards
    return result


def _apply_occupancy_to_exact_card_observation(
    measured_by_accelerator: dict[str, int] | None,
    aggregate_after_debit: int,
    debit_by_accelerator: Mapping[str, int],
) -> tuple[int, dict[str, int] | None]:
    """Apply row occupancy without moving a debit to a different card."""
    aggregate = max(0, int(aggregate_after_debit))
    if measured_by_accelerator is None:
        return aggregate, None
    spendable = {
        card: max(0,
                  int(card_free) - int(debit_by_accelerator.get(card, 0)))
        for card, card_free in measured_by_accelerator.items()
    }
    # The shaped measurement is authoritative when available.  Clamping its
    # sum by the unshaped debit would move any unsatisfied debit onto a
    # different card (for example, an A100 debit with zero measured A100 free
    # would incorrectly suppress H200).  Ambiguous rows instead carry a debit
    # for every plausible card, so their conservative underfill stays local to
    # those cards too.
    return sum(spendable.values()), spendable


def _normalize_persisted_accelerator_counts(
    raw_counts: Any,
    pool_key: str,
    *,
    expected_total: int | None = None,
) -> dict[str, int]:
    """Validate one exact-card mapping read from a published round."""
    if not isinstance(raw_counts, dict):
        raise TypeError('exact-card entry must be an object')
    allowed_cards = set(parse_pool_identity(pool_key).gpu_names)
    normalized: dict[str, int] = {}
    for raw_card, raw_count in raw_counts.items():
        if (not isinstance(raw_card, str) or not raw_card or
                isinstance(raw_count, bool) or not isinstance(raw_count, int) or
                raw_count < 0):
            raise ValueError('invalid exact-card entry')
        card = raw_card.casefold()
        if card not in allowed_cards:
            raise ValueError(
                f'exact-card entry {card!r} is outside pool {pool_key!r}')
        if card in normalized:
            raise ValueError('duplicate exact-card entry')
        if raw_count > 0:
            normalized[card] = raw_count
    if expected_total is not None and sum(
            normalized.values()) != expected_total:
        raise ValueError('exact-card observation does not sum to its '
                         f'aggregate ({sum(normalized.values())} != '
                         f'{expected_total})')
    return normalized


def _service_feed_payload_for_epoch(raw_payload: str | None) -> str | None:
    """Canonicalize shaped service authority without observation metadata."""
    if raw_payload is None:
        return None
    try:
        decoded = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        # Preserve malformed payload identity so replacing it with valid state
        # still advances the epoch.
        return raw_payload
    if not isinstance(decoded, dict):
        return raw_payload
    decoded.pop(_OBSERVED_FREE_BY_ACCELERATOR_KEY, None)
    decoded.pop(_BROKER_SLOT_WIDTH_KEY, None)
    return json.dumps(decoded, sort_keys=True)


def _reject_mixed_gpus_per_replica(
    pool_key: str,
    rows: dict[str, dict[str, Any]],
    protocol_version: int = PROTOCOL_V1
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    """Isolates claims disagreeing on gpus_per_replica.

    Integer replica-slot bookkeeping is only sound when every claimant of a
    pool converts GPUs to slots the same way. Deterministic survivor rule:
    the gpus_per_replica value shared by the most claimants wins, ties by the
    smaller value.

    Protocol v1 preserves its historical behavior and deletes losing claims.
    A protocol-v2 edge is one member of an authoritative complete service
    set, so deleting it here would advance the service-wide generation during
    a pool round, fence healthy sibling pools, and let the next heartbeat add
    it back forever.  V2 therefore retains the durable edge and generation,
    but replaces its round-local policy with explicit zero authority.  The
    returned loser set also prevents a losing poller's differently-scaled
    query callback from driving the shared round.
    """
    sizes = sorted({int(row['gpus_per_replica'] or 1) for row in rows.values()})
    if len(sizes) <= 1:
        return rows, set()
    counts = {
        size: sum(1
                  for row in rows.values()
                  if int(row['gpus_per_replica'] or 1) == size) for size in sizes
    }
    winner = max(sizes, key=lambda size: (counts[size], -size))
    losers = [
        name for name, row in rows.items()
        if int(row['gpus_per_replica'] or 1) != winner
    ]
    logger.error(
        f'Reserved-fill broker: pool {pool_key} has claims with mixed '
        f'gpus_per_replica {sizes}; the broker requires a uniform pool. '
        f'Blackouting claims of {losers} '
        f'(keeping gpus_per_replica={winner}).')
    loser_set = set(losers)
    for name in losers:
        if protocol_version == PROTOCOL_V1:
            _remove_claim_for_protocol(protocol_version,
                                       name,
                                       pool_key=pool_key)
            _clear_service_cache(name)
            rows.pop(name)
            continue
        # This is a round-local view only; the normalized edge remains
        # untouched in PostgreSQL.  A zero cap makes every grant/feed path
        # fail closed while retaining the complete claim-generation map in
        # the published round.
        row = dict(rows[name])
        row.update(floor_replicas=0,
                   holdings_fill=0,
                   effective_cap=0,
                   launchable=0)
        rows[name] = row
        _clear_service_cache(name, pool_key=pool_key)
    return rows, loser_set


def _zero_v2_mixed_width_allocation(
    service_name: str,
    pool_key: str,
    service_generation: int,
    claim_row: dict[str, Any],
    round_row: dict[str, Any] | None,
    now: float,
) -> Allocation:
    """Return non-durable zero authority for a mismatched-width poller.

    Only a poller whose width matches the deterministic pool width may query
    and publish the shared slot count.  A loser still needs an explicit zero
    result so its autoscaler withdraws both launch and shelter authority while
    a winning peer drives the next durable round.
    """
    if round_row is not None:
        durable = _allocation_from_round(service_name,
                                         pool_key,
                                         round_row,
                                         protocol_version=PROTOCOL_V2,
                                         service_generation=service_generation,
                                         claim_row=claim_row)
        if durable is not None:
            return durable
    identity = parse_pool_identity(pool_key)
    allocation = Allocation(
        grant=0,
        feed=0,
        round_id=(0 if round_row is None else int(round_row['round_id'])),
        epoch=(0 if round_row is None else int(round_row['epoch'])),
        snapshot_time=now,
        demand_gate_grant=0,
        protocol_version=PROTOCOL_V2,
        service_generation=service_generation,
        physical_cluster_uid=identity.physical_cluster_uid,
        edge_cap=0,
        pool_key=pool_key,
        feed_by_accelerator={},
        broker_slot_width=int(claim_row.get('gpus_per_replica') or 1),
    )
    _cache_allocation(service_name, allocation, claim_row)
    return allocation


def _replica_row_on_pool(
    info: 'replica_managers.ReplicaInfo',
    context: str | tuple[str, ...],
    gpu_names: tuple[str, ...],
    *,
    pool_key: str | None = None,
    physical_cluster_uid: str | None = None,
) -> bool:
    """Whether a replica row's persisted location sits on the pool.

    Complete, internally consistent protocol-v2 pool-key and physical-UID
    provenance dominates mutable aliases and widths. Otherwise exact current
    Kubernetes placement is physical occupancy regardless of economic
    classification. Partial, malformed, contradictory, or unattributed
    zero-cost provenance falls back to every plausible pool indicated by a
    trustworthy key, UID, or accelerator hint. Ambiguity may underfill
    multiple pools, but must never authorize the same physical slot twice.
    """
    identity: PoolIdentity | None = None
    if pool_key is not None:
        try:
            identity = parse_pool_identity(pool_key)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
    contexts = (context,) if isinstance(context, str) else context
    if identity is None or identity.protocol_version == PROTOCOL_V1:
        # Preserve protocol-v1 attribution exactly during the rollout window.
        # Its historical rows may carry only the v1 pool key, and pre-upgrade
        # shape-less rows remain physical occupants of this context.
        persisted_pool_key = info.reserved_fill_pool_key
        if isinstance(persisted_pool_key, str) and persisted_pool_key:
            return persisted_pool_key == pool_key
        persisted_uid = info.reserved_fill_physical_cluster_uid
        if (isinstance(persisted_uid, str) and persisted_uid and
                physical_cluster_uid is not None):
            if persisted_uid != physical_cluster_uid:
                return False
            location = info.location
            accelerators = (location or {}).get('accelerators') or {}
            return (not accelerators or any(
                isinstance(name, str) and name.lower() in gpu_names
                for name in accelerators))
        location = info.location
        if not location:
            return False
        if str(location.get('cloud', '')).lower() != 'kubernetes':
            return False
        if location.get('region') not in contexts:
            return False
        accelerators = location.get('accelerators') or {}
        accelerator_matches = any(
            isinstance(name, str) and name.lower() in gpu_names
            for name in accelerators)
        return not accelerators or accelerator_matches

    persisted_pool_key = info.reserved_fill_pool_key
    persisted_generation = info.reserved_fill_service_generation
    persisted_uid = info.reserved_fill_physical_cluster_uid
    complete_provenance = (isinstance(persisted_pool_key, str) and
                           bool(persisted_pool_key) and
                           type(persisted_generation) is int and
                           persisted_generation > 0 and
                           isinstance(persisted_uid, str) and
                           bool(persisted_uid))
    if complete_provenance:
        assert isinstance(persisted_pool_key, str)
        try:
            persisted_identity = parse_pool_identity(persisted_pool_key)
        except (TypeError, ValueError, json.JSONDecodeError):
            persisted_identity = None
        if (persisted_identity is not None and
                persisted_identity.protocol_version == PROTOCOL_V2 and
                persisted_identity.physical_cluster_uid == persisted_uid):
            # Only this self-consistent immutable tuple proves exact membership
            # or exact absence. Alias, generation, and width changes do not
            # move an existing pod.
            return (persisted_pool_key == pool_key and
                    persisted_uid == identity.physical_cluster_uid and
                    (physical_cluster_uid is None or
                     persisted_uid == physical_cluster_uid))

    target_hint = (persisted_pool_key == pool_key or
                   persisted_uid == identity.physical_cluster_uid)
    location = info.location
    if isinstance(location, Mapping) and location:
        cloud = location.get('cloud')
        if (isinstance(cloud, str) and cloud and
                cloud.casefold() != 'kubernetes'):
            return target_hint
        accelerators = location.get('accelerators')
        accelerator_matches = False
        if isinstance(accelerators, Mapping) and accelerators:
            accelerator_names = {
                name.casefold()
                for name in accelerators
                if isinstance(name, str) and name
            }
            if accelerator_names:
                accelerator_matches = bool(
                    accelerator_names.intersection(identity.gpu_names))

        # A row located in a current access context physically occupies that
        # Kubernetes cluster.  Cost provenance cannot override placement:
        # old rows are rewritten to the latest record version without gaining
        # historical is_zero_cost truth, and skipping such a row can feed its
        # slot to a peer while its launch binds.  Exact context matching makes
        # this rule durable without pessimistically coupling unrelated
        # clusters that happen to use the same accelerator.
        region = location.get('region')
        if region in contexts:
            return not accelerators or accelerator_matches or target_hint

        # Rows can retain a retired alias.  Without complete immutable
        # provenance, same-card Kubernetes placement remains plausible on
        # every compatible v2 pool.  This includes old ordinary rows whose
        # false is_zero_cost value is only a compatibility default and remains
        # false after a record-version rewrite.  A non-Kubernetes row was
        # already excluded above.
        if (accelerator_matches or info.reserved_fill is True or
                info.is_zero_cost is True):
            return accelerator_matches or target_hint
        return target_hint
    # A shapeless malformed/fill row remains plausible on any v2 pool until
    # its lifecycle row disappears. This is conservative underfill, never
    # oversubscription.
    return bool(target_hint or info.reserved_fill is True or
                info.is_zero_cost is True)


@dataclasses.dataclass(frozen=True)
class _ReplicaPoolOccupancy:
    """Current-width slot debit and every plausible exact-card debit."""

    slots: int
    by_accelerator: tuple[tuple[str, int], ...]


def _replica_pool_occupancy(
    info: 'replica_managers.ReplicaInfo',
    contexts: tuple[str, ...],
    identity: PoolIdentity,
    *,
    pool_key: str,
    physical_cluster_uid: str | None,
    current_service_generation: int | None,
    pool_gpus_per_replica: int | None,
) -> _ReplicaPoolOccupancy | None:
    """Return conservative pool/card occupancy in current replica slots."""
    # Service generations fence new authority, but cannot relocate an already
    # persisted physical occupant.
    del current_service_generation
    if not _replica_row_on_pool(info,
                                contexts,
                                identity.gpu_names,
                                pool_key=pool_key,
                                physical_cluster_uid=physical_cluster_uid):
        return None
    width = (pool_gpus_per_replica if type(pool_gpus_per_replica) is int and
             pool_gpus_per_replica > 0 else 1)
    location = info.location
    accelerators = (location.get('accelerators') if isinstance(
        location, Mapping) else None)
    shaped: dict[str, int] = {}
    if isinstance(accelerators, Mapping) and accelerators:
        for raw_name, raw_count in accelerators.items():
            if (not isinstance(raw_name, str) or
                    raw_name.casefold() not in identity.gpu_names):
                continue
            card = raw_name.casefold()
            if (isinstance(raw_count, bool) or
                    not isinstance(raw_count, (int, float)) or
                    not float(raw_count).is_integer() or raw_count <= 0):
                slots = 1
            else:
                slots = max(1, math.ceil(int(raw_count) / width))
            shaped[card] = max(shaped.get(card, 0), slots)
    if shaped:
        return _ReplicaPoolOccupancy(slots=sum(shaped.values()),
                                     by_accelerator=tuple(sorted(
                                         shaped.items())))
    # Pool provenance or a shapeless/contradictory legacy row proves no exact
    # card. One row can consume at least one slot on any compatible card, so
    # withhold one from each card while subtracting one from aggregate free.
    return _ReplicaPoolOccupancy(slots=1,
                                 by_accelerator=tuple(
                                     (card, 1) for card in identity.gpu_names))


def _row_was_launched(info: 'replica_managers.ReplicaInfo') -> bool:
    """Whether the row's current launch status proves materialization.

    SHUTTING_DOWN is broader than "bound graceful drainer": a
    launch-cancelled row (sky.launch INTERRUPTED mid-run) maps to
    SHUTTING_DOWN too, yet may never have bound a pod -- the measured
    free still counts its slot. Only sky_launch_status == SUCCEEDED
    means a pod was actually provisioned.  Sequenced callers additionally use
    the immutable materialization marker because teardown may replace the
    current process status after a successful launch.  This helper gates only
    entitlement conservation; a sequenced cleanup-unproven row lacking both
    signals still debits a possibly stale provider query until deletion proves
    cleanup.
    """
    return (info.status_property.sky_launch_status ==
            common_utils.ProcessStatus.SUCCEEDED)


class IncompleteReplicaOccupancySnapshotError(RuntimeError):
    """A sequenced broker round could not prove complete row occupancy."""


def _sequenced_row_occupies_observed_free(
    info: 'replica_managers.ReplicaInfo',
    observation_admission_sequence: int,
    observation_materialization_sequence: int,
) -> bool:
    """Whether event order cannot prove the observation saw this row.

    A row is safe to leave to the provider measurement only when both durable
    markers are well-formed and no newer than their observation high-waters.
    Missing/malformed legacy attribution is conservatively debited until that
    row naturally churns; the legacy callback path continues using its
    historical readiness/clock heuristic.
    """
    admission = info.zero_cost_admission_sequence
    materialization = info.zero_cost_materialization_sequence
    if (type(admission) is not int or admission < 1 or
            type(materialization) is not int or materialization < 1):
        return True
    return (admission > observation_admission_sequence or
            materialization > observation_materialization_sequence)


def _occupying_debit(
    claim_names: list[str],
    pool_key: str,
    snapshot_time: float,
    *,
    access_contexts: tuple[str, ...] | None = None,
    physical_cluster_uid: str | None = None,
    claim_generations: Mapping[str, int] | None = None,
    pool_gpus_per_replica: int | None = None,
    observation_admission_sequence: int | None = None,
    observation_materialization_sequence: int | None = None,
) -> tuple[int, int, dict[str, int], dict[str, int], int]:
    """Row-consistent scan of every service's replica rows on the pool.

    Mirrors the #108 occupied-slot subtraction at broker level. The scan
    covers ALL services with replica rows, not just current claimants: a
    FORMER claimant (disabled, pruned, or moved to another pool) can
    leave nonterminal fill rows behind -- a queued launch not yet bound
    (invisible to the cluster query: its slot still reads free) or a live
    pod riding out its lifetime. Scanning only claimants would feed those
    slots to a peer while the orphaned launch can still start. Rows are a
    local DB read, so the wider scan costs no cluster traffic. Returns
    (feed_debit, entitlement_debit, feed_debit_by_accelerator, live_fill,
    unclaimed_fill):

    - feed_debit: applied to observed free before the FEED split. With a
      sequenced observation, a compatible zero-cost row is debited when its
      admission or first-success materialization is newer than the captured
      high-water, or either marker is missing/malformed. Legacy callback
      rounds preserve the historical not-READY-or-post-snapshot heuristic.
    - entitlement_debit: applied to the ENTITLEMENT total. With a sequenced
      observation it uses the same event-order rule as feed_debit, closing
      both admission-after-query-start and materialization-during-query races.
      A fully marked row materialized before query start is left to the
      provider measurement, avoiding a persistent double debit. Legacy
      callback rounds debit only rows whose created_at is post-snapshot.
    - live_fill (per-CLAIMANT CURRENT count of nonterminal pool-matched
      rows with reserved_fill=True; an entry for EVERY claimant whose
      rows were readable, 0 included): the row-consistent replacement for
      the owner's claimed holdings_fill. A claim's holdings are only as
      fresh as its owner's last heartbeat, while unclaimed_fill below is
      a live row scan; mixing the two views double-counts every replica
      that turned SHUTTING_DOWN after its owner's last poll. This is the
      same quantity the owner itself reports (nonterminal fill rows on
      its zero-cost location), just read from the rows NOW. It inherently
      includes post-snapshot fill binds, so a mid-query FILL bind stays
      attributed to its owner (the entitlement debit subtracts it from
      free; counting it here keeps the whole-pool total conserved and
      the owner's grant covering the replica the previous round's feed
      just launched). Post-snapshot DEMAND rows keep the plain debit:
      they are an external mid-query race, not arbitrated capacity.
    - unclaimed_fill (pool-wide count of fill rows occupying the pool
      that belong to NO current claimant's holdings): added to the
      ENTITLEMENT total. Two populations, same conservation reasoning:

      * Graceful drainers -- SHUTTING_DOWN rows with reserved_fill=True
        (any service) whose sky.launch SUCCEEDED (see _row_was_launched).
        A culled fill replica leaves its owner's holdings the moment it
        turns terminal, but its pod stays bound for the whole graceful
        drain (multiple broker rounds), so the measured free does not
        see the slot either; without this term the round total
        undercounts by every drainer and the shrunken Sum(holdings)
        reads as "pods physically gone", triggering immediate down-moves
        that cull warm replicas below the allocation fixpoint.
      * FORMER claimants' nonterminal pool-matched fill rows. Their
        service holds no claim, so live_fill cannot attribute them, yet
        the rows occupy (or are about to occupy) the pool exactly like a
        drainer mid-drain: conserved in the total, granted to nobody's
        holdings, and their unbound window feed-debited below so the
        slot is never fed to a peer while the orphaned launch can still
        start (the atomic persist additionally refuses new orphan rows
        -- see add_replica_if_round_epoch's live-claim predicate).

      Counted pool-wide (never re-attributed to any claimant's
      holdings): these slots back no live claim, so they must not lower
      anyone's feed need or raise a blind-round holdings floor. Draining
      DEMAND rows are deliberately NOT counted: a demand row was never
      in holdings and its bound pod was already excluded from the
      measured free while it was LIVE, so the total's view of it is
      unchanged by the drain -- demand capacity is not fill-arbitrable,
      before or during its drain (the pre-existing steady-state
      undercount by live demand pods is by design; non-claimants'
      nonterminal DEMAND rows stay invisible for the same reason).
      In sequenced rounds, FAILED_CLEANUP fill rows with materialization proof
      are also conserved until cleanup is proven.  A successful ``sky.down``
      status is durable cleanup proof even when the retained diagnostic row
      still renders as SHUTTING_DOWN because its launch was interrupted.  Such
      a row must not continue withholding capacity after provider absence was
      proved.  Other retained rows may persist after the pod eventually dies,
      but this can only withhold fill; treating unresolved cleanup as free
      could oversubscribe a still-bound slot.

    A sequenced round requires the grouped replica scan to succeed completely;
    partial enumeration or decode is not spendable authority. It includes
    ordinary zero-cost rows owned by nonclaimants. Until ordinary placement
    persists a physical UID, such a row conservatively matches every v2 pool
    with the same card and width, which can delay fill but cannot over-grant.
    """
    identity = parse_pool_identity(pool_key)
    has_sequence_boundary = (observation_admission_sequence is not None or
                             observation_materialization_sequence is not None)
    if has_sequence_boundary:
        if (type(observation_admission_sequence) is not int or
                observation_admission_sequence < 0 or
                type(observation_materialization_sequence) is not int or
                observation_materialization_sequence < 0):
            raise ValueError('Sequenced occupancy debit requires complete '
                             'nonnegative observation high-waters.')
    contexts: tuple[str, ...]
    if identity.protocol_version == PROTOCOL_V1:
        assert identity.access_context is not None
        contexts = (identity.access_context,)
    else:
        contexts = tuple(access_contexts or ())
        if physical_cluster_uid is None:
            physical_cluster_uid = identity.physical_cluster_uid
        if not contexts:
            # Every authoritative v2 edge carries an access context. Missing
            # identity is corrupt state; matching nothing is fail-closed for
            # ownership (the measured pool still excludes bound pods).
            logger.error('Reserved-fill protocol-v2 debit has no access '
                         f'context for pool {pool_key}.')
    feed_debit = 0
    entitlement_debit = 0
    feed_debit_by_accelerator: dict[str, int] = {}
    live_fill: dict[str, int] = {}
    unclaimed_fill = 0
    claimants = set(claim_names)

    def debit_occupancy(occupancy: _ReplicaPoolOccupancy) -> None:
        nonlocal feed_debit, entitlement_debit
        feed_debit += occupancy.slots
        entitlement_debit += occupancy.slots
        for card, slots in occupancy.by_accelerator:
            feed_debit_by_accelerator[card] = (
                feed_debit_by_accelerator.get(card, 0) + slots)

    try:
        replica_infos_by_service = serve_state.get_replica_infos_grouped()
        # Claimants with no replica rows are still a successful zero-row read;
        # recording zero replaces their possibly-stale claimed holdings.
        for name in claimants:
            replica_infos_by_service.setdefault(name, [])
    except Exception as snapshot_error:  # pylint: disable=broad-except
        if has_sequence_boundary:
            raise IncompleteReplicaOccupancySnapshotError(
                'Sequenced reserved-fill occupancy snapshot is incomplete: '
                f'{common_utils.format_exception(snapshot_error)}') from (
                    snapshot_error)
        logger.warning(
            'Reserved-fill broker: could not snapshot replica rows for the '
            'round debit; falling back to isolated service reads: '
            f'{common_utils.format_exception(snapshot_error)}')
        # Preserve the old failure isolation on corrupt rows or a transient
        # query failure. Enumeration failure degrades to current claimants,
        # matching the previous behavior.
        scan_names = set(claim_names)
        try:
            scan_names.update(serve_state.get_replica_service_names())
        except Exception as enumeration_error:  # pylint: disable=broad-except
            logger.warning(
                'Reserved-fill broker: could not enumerate replica-owning '
                'services for the round debit (scanning claimants only): '
                f'{common_utils.format_exception(enumeration_error)}')
        replica_infos_by_service = {}
        for name in scan_names:
            try:
                replica_infos_by_service[name] = (
                    serve_state.get_replica_infos(name))
            except Exception as service_error:  # pylint: disable=broad-except
                # Failing to read one service's rows must not sink the round;
                # skipping its debit (and, for a claimant, falling back to
                # its possibly-stale claim holdings: no live_fill entry) is
                # optimistic but bounded to one service and one round.
                logger.warning(
                    f'Reserved-fill broker: could not read replicas of '
                    f'{name!r} for the round debit: '
                    f'{common_utils.format_exception(service_error)}')

    for name, infos in sorted(replica_infos_by_service.items()):
        is_claimant = name in claimants
        if is_claimant:
            live_fill[name] = 0
        for info in infos:
            occupancy = _replica_pool_occupancy(
                info,
                contexts,
                identity,
                pool_key=pool_key,
                physical_cluster_uid=physical_cluster_uid,
                current_service_generation=(claim_generations or {}).get(name),
                pool_gpus_per_replica=pool_gpus_per_replica)
            if info.is_terminal:
                cleanup_succeeded = (info.status_property.sky_down_status ==
                                     common_utils.ProcessStatus.SUCCEEDED)
                cleanup_not_proven = (
                    (info.status == serve_state.ReplicaStatus.SHUTTING_DOWN and
                     not cleanup_succeeded) or
                    (has_sequence_boundary and
                     info.status == serve_state.ReplicaStatus.FAILED_CLEANUP))
                if occupancy is not None and cleanup_not_proven:
                    materialization = (info.zero_cost_materialization_sequence)
                    launch_was_materialized = (_row_was_launched(info) or
                                               (type(materialization) is int and
                                                materialization > 0))
                    if launch_was_materialized and info.reserved_fill:
                        unclaimed_fill += occupancy.slots
                    if has_sequence_boundary:
                        assert observation_admission_sequence is not None
                        assert observation_materialization_sequence is not None
                        # Cleanup-unproven terminal rows are not safe to infer
                        # absent from an in-flight provider query.  A launch
                        # may bind immediately before cancellation while its
                        # success reducer loses the race, leaving INTERRUPTED
                        # and no materialization marker.  Missing M is itself
                        # conservative debit evidence until cleanup completes.
                        if _sequenced_row_occupies_observed_free(
                                info, observation_admission_sequence,
                                observation_materialization_sequence):
                            debit_occupancy(occupancy)
                continue
            if occupancy is None:
                continue
            is_fill = info.reserved_fill
            if not is_claimant and not is_fill:
                if not has_sequence_boundary:
                    # Preserve protocol-v1/legacy callback behavior exactly.
                    # Sequenced observations, however, must include every row
                    # physically matched to the pool, including ordinary rows
                    # whose legacy cost provenance was lost during rewrite.
                    continue
            if is_fill:
                if is_claimant:
                    live_fill[name] += occupancy.slots
                else:
                    # Former claimant's fill row: unclaimed occupancy,
                    # conserved like a drainer (see docstring).
                    unclaimed_fill += occupancy.slots
            if has_sequence_boundary:
                assert observation_admission_sequence is not None
                assert observation_materialization_sequence is not None
                occupies_observed_free = _sequenced_row_occupies_observed_free(
                    info, observation_admission_sequence,
                    observation_materialization_sequence)
                if occupies_observed_free:
                    debit_occupancy(occupancy)
            else:
                created_at = info.created_at
                post_snapshot = (created_at is not None and
                                 created_at > snapshot_time)
                if (not info.is_ready) or post_snapshot:
                    feed_debit += occupancy.slots
                    for card, slots in occupancy.by_accelerator:
                        feed_debit_by_accelerator[card] = (
                            feed_debit_by_accelerator.get(card, 0) + slots)
                if post_snapshot:
                    entitlement_debit += occupancy.slots

    if has_sequence_boundary:
        try:
            pending_debits = zero_cost_actuation.pending_pool_debits(pool_key)
        except Exception as intent_error:  # pylint: disable=broad-except
            raise IncompleteReplicaOccupancySnapshotError(
                'Sequenced reserved-fill intent snapshot is incomplete: '
                f'{common_utils.format_exception(intent_error)}'
            ) from intent_error
        for debit in pending_debits:
            if (debit.pool_key != pool_key or debit.replica_slots < 1 or
                    debit.accelerator not in identity.gpu_names):
                raise IncompleteReplicaOccupancySnapshotError(
                    'Sequenced reserved-fill intent has inconsistent physical '
                    'pool attribution.')
            occupancy = _ReplicaPoolOccupancy(
                slots=debit.replica_slots,
                by_accelerator=((debit.accelerator, debit.replica_slots),))
            debit_occupancy(occupancy)
            if debit.service_name in claimants:
                live_fill[debit.service_name] = (
                    live_fill.get(debit.service_name, 0) + debit.replica_slots)
            else:
                unclaimed_fill += debit.replica_slots
    return (feed_debit, entitlement_debit,
            dict(sorted(feed_debit_by_accelerator.items())), live_fill,
            unclaimed_fill)


def _demand_gate_grant(damped: int | None, raw: Any) -> int | None:
    """The permissive grant the demand-placement gate reads.

    None (no ceiling) stays None: the gate is inert there by design.
    """
    if damped is None:
        return None
    try:
        raw_int = int(raw)
    except (TypeError, ValueError):
        return damped
    return max(damped, raw_int)


def _round_protocol_version(round_row: dict[str, Any]) -> int:
    raw = round_row.get('protocol_version')
    if raw is None:
        return PROTOCOL_V1
    try:
        version = int(raw)
    except (TypeError, ValueError):
        return -1
    return version


def _round_claim_generations(round_row: dict[str, Any]) -> dict[str, int]:
    raw = round_row.get('claim_generations')
    if not raw:
        return {}
    try:
        decoded = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(decoded, dict):
            return {}
        return {str(name): int(value) for name, value in decoded.items()}
    except (TypeError, ValueError):
        return {}


def _round_matches_claim(round_row: dict[str, Any], service_name: str,
                         protocol_version: int,
                         service_generation: int) -> bool:
    if _round_protocol_version(round_row) != protocol_version:
        return False
    if protocol_version == PROTOCOL_V1:
        return True
    return (_round_claim_generations(round_row).get(service_name) ==
            service_generation)


def _round_matches_claim_set(round_row: dict[str, Any], protocol_version: int,
                             claim_generations: dict[str, int]) -> bool:
    if _round_protocol_version(round_row) != protocol_version:
        return False
    if protocol_version == PROTOCOL_V1:
        return True
    return _round_claim_generations(round_row) == claim_generations


def _cache_allocation(service_name: str, allocation: Allocation,
                      claim_row: dict[str, Any] | None) -> None:
    """Cache an allocation without losing v2 access-context identity."""
    if allocation.protocol_version == PROTOCOL_V1:
        entry = _GrantCacheEntry(grant=allocation.demand_gate_grant,
                                 cached_at=time.time())
        with _GRANT_CACHE_LOCK:
            _GRANT_CACHE[_cache_key(service_name, None)] = entry
        return
    if allocation.pool_key is None or claim_row is None:
        return
    try:
        identity = parse_pool_identity(allocation.pool_key)
    except (TypeError, ValueError, json.JSONDecodeError):
        return
    access_context = claim_row.get('access_context')
    physical_cluster_uid = (allocation.physical_cluster_uid or
                            identity.physical_cluster_uid)
    if (not isinstance(access_context, str) or not access_context or
            not isinstance(physical_cluster_uid, str) or
            not physical_cluster_uid):
        # A v2 grant that cannot be mapped back to an access context must not
        # influence demand placement. The allocation itself remains useful to
        # its owning poller, whose query callback already carries the context.
        return
    entry = _GrantCacheEntry(grant=allocation.demand_gate_grant,
                             cached_at=time.time(),
                             access_context=access_context,
                             accelerator_names=identity.gpu_names,
                             physical_cluster_uid=physical_cluster_uid,
                             service_generation=allocation.service_generation)
    with _GRANT_CACHE_LOCK:
        _GRANT_CACHE[_cache_key(service_name, allocation.pool_key)] = entry


def _allocation_from_round(
    service_name: str,
    pool_key: str,
    round_row: dict[str, Any],
    *,
    protocol_version: int,
    service_generation: int,
    claim_row: dict[str, Any] | None = None,
) -> Allocation | None:
    if not _round_matches_claim(round_row, service_name, protocol_version,
                                service_generation):
        return None
    grants = json.loads(round_row['grants'] or '{}')
    if service_name not in grants:
        # Claimed after this round was published: no allocation until the
        # next round (at most one poll interval away).
        return None
    feeds = json.loads(round_row['feeds'] or '{}')
    raw_grants = json.loads(round_row['raw_grants'] or '{}')
    raw_feed_by_accelerator = round_row.get('feed_by_accelerator')
    feed_by_accelerator: dict[str, int] | None = None
    observed_free_slots: int | None = None
    observed_free_slots_by_accelerator: dict[str, int] | None = None
    observed_at: float | None = None
    broker_slot_width = 1
    all_feed_by_accelerator: dict[str, Any] | None = None
    if raw_feed_by_accelerator is not None:
        try:
            decoded = json.loads(raw_feed_by_accelerator)
            if not isinstance(decoded, dict):
                raise TypeError('exact-card feed envelope must be an object')
            all_feed_by_accelerator = decoded
        except (TypeError, json.JSONDecodeError) as error:
            # A present exact-card allocation is authoritative.  Corruption
            # cannot degrade into an aggregate launch that may select another
            # card from the same physical pool.
            logger.error(
                'Reserved-fill round has malformed exact-card feed for '
                f'{service_name!r}/{pool_key}: {error}')
            feed_by_accelerator = {}
        if all_feed_by_accelerator is not None:
            try:
                feed_by_accelerator = _normalize_persisted_accelerator_counts(
                    all_feed_by_accelerator[service_name], pool_key)
            except (KeyError, TypeError, ValueError,
                    json.JSONDecodeError) as error:
                # Parse service launch authority independently from the raw
                # observation metadata below.  Either side can fail closed
                # without poisoning the other.
                logger.error(
                    'Reserved-fill round has malformed exact-card feed for '
                    f'{service_name!r}/{pool_key}: {error}')
                feed_by_accelerator = {}
            raw_observation = all_feed_by_accelerator.get(
                _OBSERVED_FREE_BY_ACCELERATOR_KEY)
            if raw_observation is not None:
                try:
                    raw_slot_width = all_feed_by_accelerator.get(
                        _BROKER_SLOT_WIDTH_KEY, (claim_row or
                                                 {}).get('gpus_per_replica', 1))
                    if (isinstance(raw_slot_width, bool) or
                            not isinstance(raw_slot_width, int) or
                            raw_slot_width <= 0):
                        raise ValueError('invalid broker slot width')
                    broker_slot_width = raw_slot_width
                    raw_observed_free = round_row.get('last_observed_free')
                    if (isinstance(raw_observed_free, bool) or
                            not isinstance(raw_observed_free, int) or
                            raw_observed_free < 0):
                        raise ValueError('invalid aggregate observation')
                    raw_observed_at = round_row.get('last_observed_free_ts')
                    if (isinstance(raw_observed_at, bool) or
                            not isinstance(raw_observed_at, (int, float)) or
                            not math.isfinite(raw_observed_at)):
                        raise ValueError('invalid observation timestamp')
                    snapshot_time = float(round_row['snapshot_time'])
                    if float(raw_observed_at) != snapshot_time:
                        raise ValueError('observation timestamp does not match '
                                         'the round snapshot')
                    observed_free_slots_by_accelerator = (
                        _normalize_persisted_accelerator_counts(
                            raw_observation,
                            pool_key,
                            expected_total=raw_observed_free))
                    observed_free_slots = raw_observed_free
                    observed_at = float(raw_observed_at)
                except (KeyError, TypeError, ValueError,
                        json.JSONDecodeError) as error:
                    logger.error(
                        'Reserved-fill round has malformed measured capacity '
                        f'for {pool_key}: {error}')
                    observed_free_slots = None
                    observed_free_slots_by_accelerator = None
                    observed_at = None
    edge_cap = None
    physical_cluster_uid = None
    if claim_row is not None:
        raw_cap = claim_row.get('effective_cap')
        try:
            edge_cap = None if raw_cap is None else max(0, int(raw_cap))
        except (TypeError, ValueError):
            edge_cap = None
        physical_cluster_uid = claim_row.get('physical_cluster_uid')
    grant = grants[service_name]
    feed = max(0, int(feeds.get(service_name, 0)))
    raw_for_gate = raw_grants.get(service_name)
    if protocol_version == PROTOCOL_V2:
        # Authoritative v2 claims always carry a finite partitioned cap. A
        # missing cap is corrupt and fails closed to zero rather than reviving
        # the v1 unbounded-None meaning.
        if edge_cap is None:
            logger.error('Protocol-v2 claim has no edge cap; clamping grant '
                         f'to zero for {service_name!r}/{pool_key}.')
            edge_cap = 0
        grant = min(max(0, int(grant or 0)), edge_cap)
        feed = min(feed, grant)
        if feed_by_accelerator is not None:
            feed = min(feed, sum(feed_by_accelerator.values()))
        try:
            raw_for_gate = min(max(0, int(raw_for_gate)), edge_cap)
        except (TypeError, ValueError):
            raw_for_gate = grant
        if not isinstance(physical_cluster_uid,
                          str) or not physical_cluster_uid:
            try:
                physical_cluster_uid = parse_pool_identity(
                    pool_key).physical_cluster_uid
            except (TypeError, ValueError, json.JSONDecodeError):
                physical_cluster_uid = None
    allocation = Allocation(
        grant=grant,
        feed=feed,
        round_id=int(round_row['round_id']),
        epoch=int(round_row['epoch']),
        snapshot_time=float(round_row['snapshot_time']),
        demand_gate_grant=_demand_gate_grant(grant, raw_for_gate),
        protocol_version=protocol_version,
        service_generation=service_generation,
        physical_cluster_uid=physical_cluster_uid,
        edge_cap=edge_cap,
        pool_key=pool_key,
        feed_by_accelerator=feed_by_accelerator,
        observed_free_slots=observed_free_slots,
        observed_free_slots_by_accelerator=(observed_free_slots_by_accelerator),
        observed_at=observed_at,
        broker_slot_width=broker_slot_width)
    _cache_allocation(service_name, allocation, claim_row)
    return allocation


def get_my_allocation(service_name: str,
                      pool_key: str | None = None) -> Allocation | None:
    """This service's slice of the latest published round, or None.

    None when the service has no live claim (expired/rejected) or the
    latest round predates its claim.
    """
    protocol_version = get_protocol_version()
    claims = _claim_rows(protocol_version, pool_key=pool_key)
    matches = [
        row for row in claims if row['service_name'] == service_name and
        (pool_key is None or row['pool_key'] == pool_key)
    ]
    if protocol_version == PROTOCOL_V2 and pool_key is None and len(
            matches) > 1:
        logger.error('Protocol-v2 allocation lookup requires a pool key for '
                     f'multi-pool service {service_name!r}.')
        return None
    row = matches[0] if len(matches) == 1 else None
    if row is None:
        return None
    if time.time() - float(row['heartbeat_ts'] or 0) > claim_ttl_seconds():
        return None
    resolved_pool_key = row['pool_key']
    round_row = serve_state.get_reserved_fill_round(resolved_pool_key)
    if round_row is None:
        return None
    try:
        generation = _claim_generation(row, protocol_version)
        round_claims = [
            claim for claim in claims if claim['pool_key'] == resolved_pool_key
        ]
        claim_generations = {
            str(claim['service_name']): _claim_generation(
                claim, protocol_version) for claim in round_claims
        }
    except ValueError as e:
        logger.error(str(e))
        return None
    if not _round_matches_claim_set(round_row, protocol_version,
                                    claim_generations):
        return None
    return _allocation_from_round(service_name,
                                  resolved_pool_key,
                                  round_row,
                                  protocol_version=protocol_version,
                                  service_generation=generation,
                                  claim_row=row)


def _prepare_committed_round_observation(
    pool_key: str,
    observation: pool_capacity_observation.PoolCapacityObservation,
    now: float,
) -> _CommittedRoundObservation:
    """Validate immutable observation authority before broker-lock admission."""
    if not isinstance(observation,
                      pool_capacity_observation.PoolCapacityObservation):
        raise ValueError('A committed pool-capacity observation is required.')
    if observation.pool_key != pool_key:
        raise ValueError('Committed observation does not match the requested '
                         'pool key.')
    try:
        identity = parse_pool_identity(pool_key)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(
            'Committed observation has an invalid pool key.') from error
    if identity.protocol_version != PROTOCOL_V2:
        raise ValueError('Committed observations require a protocol-v2 pool.')
    assert identity.physical_cluster_uid is not None
    canonical_pool_key = make_pool_key(
        '',
        identity.gpu_names,
        protocol_version=PROTOCOL_V2,
        physical_cluster_uid=identity.physical_cluster_uid)
    if canonical_pool_key != pool_key:
        raise ValueError('Committed observation pool key is not canonical.')
    if (observation.physical_cluster_uid != identity.physical_cluster_uid or
            observation.accelerator_names != identity.gpu_names):
        raise ValueError('Committed observation physical identity does not '
                         'match its pool key.')
    if not observation.access_context:
        raise ValueError('Committed observation has no access context.')
    payload = observation.payload
    if not isinstance(payload, pool_capacity_observation.PoolCapacitySuccess):
        raise ValueError('Only a successful committed observation can drive '
                         'a reserved-fill round.')
    payload_names = tuple(name for name, _ in payload.free_gpus_by_accelerator)
    if payload_names != identity.gpu_names:
        raise ValueError('Committed observation exact-card split does not '
                         'match its pool key.')
    for value, field_name in (
        (observation.observed_at, 'observed_at'),
        (observation.completed_at, 'completed_at'),
        (observation.published_at, 'published_at'),
        (observation.valid_until, 'valid_until'),
    ):
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or
                not math.isfinite(float(value))):
            raise ValueError(f'Committed observation {field_name} is not '
                             'finite.')
    if not (observation.observed_at <= observation.completed_at <=
            observation.published_at <= observation.valid_until):
        raise ValueError('Committed observation timestamps are inconsistent.')
    if (isinstance(observation.observation_generation, bool) or
            not isinstance(observation.observation_generation, int) or
            observation.observation_generation <= 0):
        raise ValueError(
            'Committed observation observation_generation is invalid.')
    if (isinstance(observation.observation_sequence, bool) or
            not isinstance(observation.observation_sequence, int) or
            observation.observation_sequence < 0):
        raise ValueError(
            'Committed observation observation_sequence is invalid.')
    if (isinstance(observation.materialization_sequence, bool) or
            not isinstance(observation.materialization_sequence, int) or
            observation.materialization_sequence < 0):
        raise ValueError('Committed observation materialization_sequence is '
                         'invalid.')
    if (not isinstance(observation.payload_sha256, str) or
            re.fullmatch(r'[0-9a-f]{64}', observation.payload_sha256) is None):
        raise ValueError('Committed observation payload digest is invalid.')
    if not observation.is_authoritative_at(now):
        raise ValueError(
            'Committed observation is not currently authoritative.')
    provenance = RoundObservationProvenance(
        pool_key=pool_key,
        physical_cluster_uid=identity.physical_cluster_uid,
        accelerator_names=identity.gpu_names,
        access_context=observation.access_context,
        observation_generation=observation.observation_generation,
        observation_sequence=observation.observation_sequence,
        materialization_sequence=observation.materialization_sequence,
        payload_sha256=observation.payload_sha256,
        observed_at=float(observation.observed_at),
        valid_until=float(observation.valid_until))
    return _CommittedRoundObservation(payload=payload, provenance=provenance)


def _publish_legacy_round(publication: ReservedFillRoundPublication) -> bool:
    """Preserve the historical state writer for legacy callback rounds."""
    if publication.observation_provenance is not None:
        raise ValueError('The legacy publisher cannot discard observation '
                         'provenance.')
    return serve_state.publish_reserved_fill_round(
        publication.pool_key,
        round_id=publication.round_id,
        snapshot_time=publication.snapshot_time,
        epoch=publication.epoch,
        grants=publication.grants,
        feeds=publication.feeds,
        feed_by_accelerator=publication.feed_by_accelerator,
        raw_grants=publication.raw_grants,
        feed_state=publication.feed_state,
        sum_holdings=publication.sum_holdings,
        last_observed_free=publication.last_observed_free,
        last_observed_free_ts=publication.last_observed_free_ts,
        phantom_streak=publication.phantom_streak,
        shrink_baseline=publication.shrink_baseline,
        lease_token=publication.lease_token,
        lease_expires_at=publication.lease_expires_at,
        protocol_version=publication.protocol_version,
        claim_generations=publication.claim_generations,
        utilization_state=publication.utilization_state)


def publish_committed_round(publication: ReservedFillRoundPublication) -> bool:
    """Atomically persist one sequenced round and its exact provenance."""
    provenance = publication.observation_provenance
    if provenance is None:
        raise ValueError('A committed round requires observation provenance.')
    if (publication.protocol_version != PROTOCOL_V2 or
            publication.pool_key != provenance.pool_key or
            publication.snapshot_time != provenance.observed_at):
        raise ValueError('Committed round publication does not match its '
                         'observation authority.')
    return serve_state.publish_reserved_fill_round(
        publication.pool_key,
        round_id=publication.round_id,
        snapshot_time=publication.snapshot_time,
        epoch=publication.epoch,
        grants=publication.grants,
        feeds=publication.feeds,
        feed_by_accelerator=publication.feed_by_accelerator,
        raw_grants=publication.raw_grants,
        feed_state=publication.feed_state,
        sum_holdings=publication.sum_holdings,
        last_observed_free=publication.last_observed_free,
        last_observed_free_ts=publication.last_observed_free_ts,
        phantom_streak=publication.phantom_streak,
        shrink_baseline=publication.shrink_baseline,
        lease_token=publication.lease_token,
        lease_expires_at=publication.lease_expires_at,
        protocol_version=publication.protocol_version,
        claim_generations=publication.claim_generations,
        utilization_state=publication.utilization_state,
        observation_generation=provenance.observation_generation,
        observation_sequence=provenance.observation_sequence,
        observation_materialization_sequence=(
            provenance.materialization_sequence),
        observation_payload_sha256=provenance.payload_sha256)


def run_round_from_committed_observation(
    service_name: str,
    pool_key: str,
    observation: pool_capacity_observation.PoolCapacityObservation,
    poll_interval_seconds: float,
    *,
    expected_service_generation: int,
    publish_round: RoundPublisher,
    lock_timeout_seconds: float = (
        constants.RESERVED_FILL_BROKER_LOCK_TIMEOUT_SECONDS),
) -> Allocation | None:
    """Drive a protocol-v2 round from already committed capacity evidence.

    Validation and conversion happen before lock admission.  The broker lock
    therefore contains no provider query or callback capable of performing
    one.  ``publish_round`` is required (there is deliberately no fallback to
    the legacy writer): its implementation must atomically persist the round
    and ``publication.observation_provenance``, and may reject an observation
    that expires before its database transaction commits.
    """
    try:
        committed = _prepare_committed_round_observation(
            pool_key, observation, time.time())
    except (TypeError, ValueError) as error:
        logger.warning('Reserved-fill broker: rejecting committed observation '
                       f'for {service_name!r}/{pool_key}: {error}')
        return None
    try:
        with locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID,
                            timeout=lock_timeout_seconds):
            if not committed.is_authoritative_at(time.time()):
                logger.warning(
                    'Reserved-fill broker: committed observation expired '
                    f'before lock admission for {service_name!r}/{pool_key}.')
                return None
            return _run_round_locked(service_name,
                                     pool_key,
                                     None,
                                     poll_interval_seconds,
                                     PROTOCOL_V2,
                                     expected_service_generation,
                                     committed_observation=committed,
                                     publish_round=publish_round)
    except locks.LockTimeout:
        logger.warning(
            'Reserved-fill broker: timed out waiting for the round lock '
            f'(service {service_name!r}, pool {pool_key}); skipping this '
            'committed observation.')
        return None


def run_round_if_stale(
    service_name: str,
    pool_key: str,
    query_fn: Callable[[], PoolObservation | None],
    poll_interval_seconds: float,
    *,
    expected_protocol_version: int = PROTOCOL_V1,
    expected_service_generation: int = 0,
    lock_timeout_seconds: float = (
        constants.RESERVED_FILL_BROKER_LOCK_TIMEOUT_SECONDS)
) -> Allocation | None:
    """Reads the pool's round, driving a fresh one if it went stale.

    The caller (a service's capacity poller) must have upserted its claim
    first. Under the cross-process broker lock: if the published round is
    younger than ~one poll interval, return the caller's slice of it (no
    cluster query -- this is what collapses N per-interval queries to one);
    otherwise drive a new round: CAS-advance the global lease FIRST to
    take an ownership token (the round's entry point), then read all live
    claims and the previous round (reads-after-token), snapshot time
    BEFORE the slow query, validate, debit, allocate, publish atomically
    conditional on the lease still holding that exact token.

    The broker lock also excludes fill-row persists (see
    persist_fill_replica): the round holds it from its debit scan through
    its publish, so a launch's row lands either before the scan (counted)
    or after the publish (fenced by the bumped epoch) -- never inside the
    scan->publish window where it would be counted by neither.

    Returns None when the caller holds no live claim (expired, or rejected
    by a validation) or the round could not be driven; the caller then
    feeds its autoscaler zero free slots (existing holdings keep their
    shelter via zero_cost_count, no new fill). Callers that already own a
    bounded provider phase should pass a zero timeout: broker-lock contention
    must retire that phase immediately instead of waiting behind another
    controller's slow observation round.
    """
    try:
        with locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID,
                            timeout=lock_timeout_seconds):
            return _run_round_locked(service_name, pool_key, query_fn,
                                     poll_interval_seconds,
                                     expected_protocol_version,
                                     expected_service_generation)
    except locks.LockTimeout:
        logger.warning(
            'Reserved-fill broker: timed out waiting for the round lock '
            f'(service {service_name!r}, pool {pool_key}); skipping this '
            'cycle.')
        return None


def _run_round_locked(
        service_name: str,
        pool_key: str,
        query_fn: Callable[[], PoolObservation | None] | None,
        poll_interval_seconds: float,
        expected_protocol_version: int = PROTOCOL_V1,
        expected_service_generation: int = 0,
        *,
        committed_observation: _CommittedRoundObservation | None = None,
        publish_round: RoundPublisher | None = None) -> Allocation | None:
    if committed_observation is None:
        if query_fn is None:
            raise ValueError('Legacy rounds require an observation callback.')
        if publish_round is not None:
            raise ValueError('Legacy rounds cannot replace their state writer.')
        round_publisher: RoundPublisher = _publish_legacy_round
    else:
        if query_fn is not None:
            raise ValueError('Committed rounds cannot invoke an observation '
                             'callback.')
        if publish_round is None:
            raise ValueError('Committed rounds require a provenance-aware '
                             'publisher.')
        round_publisher = publish_round
    now = time.time()
    if (committed_observation is not None and
            not committed_observation.is_authoritative_at(now)):
        logger.warning('Reserved-fill broker: committed observation expired '
                       f'before round admission for {service_name!r}/'
                       f'{pool_key}.')
        return None
    protocol_version = get_protocol_version()
    if protocol_version != expected_protocol_version:
        logger.info('Reserved-fill broker protocol changed before round '
                    f'actuation for {service_name!r}/{pool_key}: expected '
                    f'{expected_protocol_version}, current '
                    f'{protocol_version}.')
        return None
    pruned = _prune_claims(protocol_version, now - claim_ttl_seconds())
    if pruned:
        logger.warning('Reserved-fill broker: pruned expired claim(s) of '
                       f'{pruned}.')
    claim_rows = {
        row['service_name']: row
        for row in _claim_rows(protocol_version, pool_key=pool_key)
    }
    claim_rows, mixed_width_losers = _reject_mixed_gpus_per_replica(
        pool_key, claim_rows, protocol_version)
    try:
        (claim_rows, claim_generations, access_contexts,
         physical_cluster_uid) = _claim_round_metadata(pool_key, claim_rows,
                                                       protocol_version)
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        logger.error(f'Reserved-fill broker: invalid round authority: {e}')
        return None
    if service_name not in claim_rows:
        # Our own claim was pruned or rejected; the poller will re-upsert
        # (and re-trip any validation, loudly) next interval.
        return None
    try:
        service_generation = _claim_generation(claim_rows[service_name],
                                               protocol_version)
    except ValueError as e:
        logger.error(str(e))
        return None
    if service_generation != expected_service_generation:
        logger.info('Reserved-fill service generation changed before round '
                    f'actuation for {service_name!r}/{pool_key}: expected '
                    f'{expected_service_generation}, current '
                    f'{service_generation}.')
        return None
    round_row = serve_state.get_reserved_fill_round(pool_key)
    if service_name in mixed_width_losers:
        return _zero_v2_mixed_width_allocation(service_name, pool_key,
                                               service_generation,
                                               claim_rows[service_name],
                                               round_row, now)
    if (committed_observation is None and round_row is not None and
            now - float(round_row['snapshot_time'])
            < _ROUND_FRESH_FRACTION * poll_interval_seconds and
            _round_matches_claim_set(round_row, protocol_version,
                                     claim_generations)):
        return _allocation_from_round(service_name,
                                      pool_key,
                                      round_row,
                                      protocol_version=protocol_version,
                                      service_generation=service_generation,
                                      claim_row=claim_rows[service_name])

    # ---- Drive a new round: ownership token FIRST. ----
    # TOKEN-FIRST ordering invariant (the other half lives in
    # serve_state.acquire_reserved_fill_lease_token): the token is the
    # round's entry point, CAS-advanced and committed before ANY state
    # that feeds the publish is read -- the claims, the previous round row
    # and the slow cluster query all come after it. The advisory round
    # lock can die mid-round (e.g. a PostgreSQL advisory-lock session
    # drop), letting a replacement writer drive and publish a newer round
    # while this writer is suspended anywhere below; because the publish
    # CASes on this exact token and the replacement's own advance
    # invalidates it, a writer resuming with pre-replacement state can
    # never publish it (rowcount 0 -> rollback -> observation discarded)
    # -- no per-pool epoch regress, no clearing of a peer's fence_pending
    # marker. The claims/round reads ABOVE this line serve only the read
    # path (freshness gate) and are re-read below.
    lease_ttl_seconds = (constants.RESERVED_FILL_LEASE_TTL_INTERVALS *
                         poll_interval_seconds)
    # A post-expiry acquisition (dead gap: no rounds at all for a lease
    # TTL) also stamps the persistent per-pool fence_pending marker in the
    # same transaction; see acquire_reserved_fill_lease_token for the
    # crash-window reasoning and the epoch computation below for the bump
    # it forces.
    if (committed_observation is not None and
            not committed_observation.is_authoritative_at(time.time())):
        logger.warning('Reserved-fill broker: committed observation expired '
                       f'before lease admission for {service_name!r}/'
                       f'{pool_key}.')
        return None
    acquired = serve_state.acquire_reserved_fill_lease_token(now=now,
                                                             expires_at=now +
                                                             lease_ttl_seconds)
    if acquired is None:
        logger.error(
            'Reserved-fill broker: lost the lease-token race before the '
            f'round query (pool {pool_key}); a writer bypassed the round '
            'lock. Skipping this cycle.')
        return None
    lease_token, lease_expired = acquired
    # Reads-after-token: the claim set and the previous round feeding the
    # publish below.
    claim_rows = {
        row['service_name']: row
        for row in _claim_rows(protocol_version, pool_key=pool_key)
    }
    claim_rows, mixed_width_losers = _reject_mixed_gpus_per_replica(
        pool_key, claim_rows, protocol_version)
    try:
        (claim_rows, claim_generations, access_contexts,
         physical_cluster_uid) = _claim_round_metadata(pool_key, claim_rows,
                                                       protocol_version)
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        logger.error(f'Reserved-fill broker: invalid round authority: {e}')
        return None
    if service_name not in claim_rows:
        # Our claim vanished between the pre-token check and here (only
        # possible when the round lock was bypassed); same reaction as
        # the pre-token miss.
        return None
    try:
        service_generation = _claim_generation(claim_rows[service_name],
                                               protocol_version)
    except ValueError as e:
        logger.error(str(e))
        return None
    if service_generation != expected_service_generation:
        return None
    round_row = serve_state.get_reserved_fill_round(pool_key)
    if service_name in mixed_width_losers:
        return _zero_v2_mixed_width_allocation(service_name, pool_key,
                                               service_generation,
                                               claim_rows[service_name],
                                               round_row, now)
    winning_widths = {
        int(row['gpus_per_replica'] or 1)
        for name, row in claim_rows.items()
        if name not in mixed_width_losers
    }
    pool_gpus_per_replica = (next(iter(winning_widths))
                             if len(winning_widths) == 1 else None)
    if pool_gpus_per_replica is None:
        logger.error('Reserved-fill broker could not establish one winning '
                     f'replica width for physical pool {pool_key!r}.')
        return None
    # Snapshot time BEFORE the slow cluster query: a zero-cost row created
    # while the query runs already occupies a slot the query may still have
    # counted free, and the created_at > snapshot_time debit only catches it
    # if the snapshot predates the row.
    observation_provenance: RoundObservationProvenance | None = None
    snapshot_time = time.time()
    observation: PoolObservation | None = None
    if committed_observation is not None:
        snapshot_time = committed_observation.provenance.observed_at
        observation = committed_observation.to_slot_observation(
            pool_gpus_per_replica)
        observation_provenance = committed_observation.provenance
    else:
        assert query_fn is not None
        try:
            observation = query_fn()
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Reserved-fill broker: pool query failed for '
                           f'{pool_key}: {common_utils.format_exception(e)}')
    query_ok = observation is not None and observation.free_slots is not None
    confirmed_phantom = False
    measured_by_accelerator: dict[str, int] | None = None
    if query_ok:
        assert observation is not None
        try:
            measured_by_accelerator = _normalize_exact_card_observation(
                observation, pool_key)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            # The card split is launch authority, not optional decoration once
            # present. Treat an internally inconsistent split as a blackout so
            # no aggregate feed can silently select the wrong card.
            logger.error('Reserved-fill broker: invalid exact-card pool '
                         f'observation for {pool_key}: {error}')
            query_ok = False
    prev_phantom_streak = (int(round_row['phantom_streak'] or 0)
                           if round_row is not None else 0)
    # Carried unchanged through a measurement blackout: a failed query is
    # not an observation, so it neither confirms nor clears a phantom
    # suspicion.
    phantom_streak = prev_phantom_streak
    if query_ok:
        assert observation is not None
        if observation.gpu_names:
            phantom_streak = 0
        else:
            # Phantom pool: the claimed GPU resolves to no labeled nodes.
            # kubernetes_catalog reports empty dicts WITHOUT raising on
            # credential/cache/label-formatter failures, so one phantom
            # reading can be a transient kube-apiserver blip disguised as
            # a successful observation. Require N consecutive phantom
            # observations before rejecting every claim on the pool
            # (their pollers re-log per interval); until confirmed, treat
            # the round as a measurement blackout: feed 0 (conservative),
            # release nothing, keep the claims.
            phantom_streak = prev_phantom_streak + 1
            if (phantom_streak
                    >= constants.RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS):
                if protocol_version == PROTOCOL_V2:
                    logger.error(
                        f'Reserved-fill broker: pool {pool_key} is phantom '
                        '(the realtime query reports no such accelerator in '
                        f'the context, {phantom_streak} consecutive rounds). '
                        'Blackouting this pool while retaining its complete '
                        'service claim sets.')
                    confirmed_phantom = True
                else:
                    logger.error(
                        f'Reserved-fill broker: pool {pool_key} is phantom '
                        '(the realtime query reports no such accelerator in '
                        f'the context, {phantom_streak} consecutive rounds). '
                        'Rejecting all claims on it.')
                    _remove_legacy_claims_for_pool(pool_key)
                # Fall through and PUBLISH an empty (blackout) round
                # instead of returning here: without a published round
                # the freshness gate never engages, so every claimant's
                # poller re-drives the full cluster query each interval
                # forever (N x duplication). Protocol v1 retains its legacy
                # claim-rejection behavior. Protocol v2 must retain the
                # complete normalized edge set: deleting this one edge would
                # advance the whole service generation mid-poll and fence
                # healthy sibling rounds. Instead the branch below publishes
                # zero grants/feeds under the unchanged generation. A later
                # healthy observation resets the streak and resumes normally.
                if protocol_version == PROTOCOL_V1:
                    claim_rows = {}
                    claim_generations = {}
                    access_contexts = ()
            else:
                logger.warning(
                    f'Reserved-fill broker: pool {pool_key} looks phantom '
                    f'({phantom_streak} consecutive observation(s), need '
                    f'{constants.RESERVED_FILL_PHANTOM_CONFIRM_ROUNDS} to '
                    'reject claims); treating the round as a measurement '
                    'blackout.')
            query_ok = False

    claims = {name: _claim_input(row) for name, row in claim_rows.items()}
    activity = {name: _activity_input(row) for name, row in claim_rows.items()}
    names = sorted(claims)
    prev_grants_json: dict[str, Any] = (json.loads(round_row['grants'] or '{}')
                                        if round_row is not None else {})
    prev_raw: dict[str, int] = (json.loads(round_row['raw_grants'] or '{}')
                                if round_row is not None else {})
    sticky: dict[str, dict[str, Any]] = (json.loads(
        round_row['feed_state'] or '{}') if round_row is not None else {})
    prev_utilization: dict[str, dict[str, Any]] = (
        json.loads(round_row['utilization_state'] or '{}')
        if round_row is not None and 'utilization_state' in round_row.keys()
        else {})
    # Disarming is immediate even on a measurement blackout, where the
    # governor itself is intentionally not advanced. Otherwise the blackout
    # carry path would preserve a decayed cap after an update explicitly set
    # utilization_gate:false. Current armed-but-blind claimants retain their
    # state and follow the normal blind grace.
    prev_utilization = {
        name: entry
        for name, entry in prev_utilization.items()
        if name in activity and activity[name].armed
    }
    # Rebuilt from the current claimants every round, mirroring the sticky
    # feed state, so entries for departed services cannot accumulate.
    utilization_state: dict[str, dict[str, Any]] = {}
    last_free: int | None = (round_row['last_observed_free']
                             if round_row is not None else None)
    last_free_ts: float | None = (round_row['last_observed_free_ts']
                                  if round_row is not None else None)
    sum_holdings = sum(claim.holdings_fill for claim in claims.values())

    grants: dict[str, int | None]
    observed_free = 0
    spendable_by_accelerator = measured_by_accelerator
    if len(claims) == 1 and protocol_version == PROTOCOL_V1:
        # SINGLE-CLAIMANT FAST PATH: #108 identity. No ceiling, feed = raw
        # measured free (a failed query reads 0 free, exactly like the
        # pre-broker poller), no debit (the local overlay already debits its
        # own rows), no damping (the local two-poll damping is untouched),
        # no stickiness.
        assert names == [service_name], (names, service_name)
        free = 0
        if query_ok:
            assert (observation is not None and
                    observation.free_slots is not None)
            free = max(0, int(observation.free_slots))
            observed_free = free
            last_free, last_free_ts = free, snapshot_time
        # The gate must survive the fast path. Left alone, a lone claimant
        # publishes a None grant, the autoscaler applies no ceiling at all,
        # and the release target would be computed and thrown away every
        # round. That configuration is not exotic: it is exactly the case
        # where the pool's other users declare no reserved_capacity_fill
        # (so they never appear as claimants), and it also arrives by
        # accident whenever a peer's claim expires.
        gated_claims, utilization_state = _apply_utilization_gate(
            claims, activity, prev_utilization, now)
        claims = gated_claims
        lone_cap = claims[service_name].utilization_cap
        grants = {service_name: lone_cap}
        feeds = {service_name: free}
        # Raw measured free, unchanged: the launch side is separately
        # clamped by the autoscaler's launch-side ceiling, so preserving
        # the pre-broker feed identity here is safe.
        # raw_grants must carry the cap too. Left empty, the first
        # multi-claimant round after this transition finds a published
        # integer grant with no raw baseline, and damp_grants stalls the
        # move for a round.
        raw_grants: dict[str, int] = ({} if lone_cap is None else {
            service_name: lone_cap
        })
        new_sticky: dict[str, dict[str, Any]] = {}
        # No debit scan on the fast path (#108 identity), so no draining
        # term either; harmless -- a single-claimant round's stored sum is
        # never a damping baseline (its None grant carries no integer
        # baseline into the next multi-claimant round). Any pending shrink
        # candidate is dropped for the same reason: with the peers gone
        # there is no damping bypass left to confirm.
        published_sum_holdings = sum_holdings
        new_shrink_baseline: int | None = None
    else:
        # The debit scan runs on blind rounds too (replica rows are DB
        # reads, not cluster queries): draining rows keep occupying the
        # pool regardless of whether this round's measurement succeeded,
        # the live-holdings correction below must apply while blind, and
        # the conservation bookkeeping must not flip on a blackout.
        try:
            (feed_debit, entitlement_debit, feed_debit_by_accelerator,
             live_fill, unclaimed_fill) = _occupying_debit(
                 names,
                 pool_key,
                 snapshot_time,
                 access_contexts=access_contexts,
                 physical_cluster_uid=physical_cluster_uid,
                 claim_generations=claim_generations,
                 pool_gpus_per_replica=pool_gpus_per_replica,
                 observation_admission_sequence=(
                     None if observation_provenance is None else
                     observation_provenance.observation_sequence),
                 observation_materialization_sequence=(
                     None if observation_provenance is None else
                     observation_provenance.materialization_sequence))
        except IncompleteReplicaOccupancySnapshotError as error:
            # A successful provider measurement without a complete row scan is
            # not spendable authority: an unread row could materialize into a
            # slot the query counted free. Leave the prior round untouched;
            # its own freshness bounds remain the recovery ceiling.
            logger.error('Reserved-fill broker: rejecting sequenced '
                         f'observation because occupancy is incomplete: '
                         f'{error}')
            return None
        # One row-consistent view: a claim's holdings_fill is only as
        # fresh as its owner's last heartbeat, while unclaimed_fill comes
        # from the live row scan above -- summing the two double-counts
        # every replica that turned SHUTTING_DOWN after its owner's last
        # poll (the stale claim still holds it AND the scan counts it
        # draining), inflating the pool total (over-grants, too-permissive
        # demand gate) until the owner re-heartbeats. For every claimant
        # whose rows were readable the scan-derived CURRENT nonterminal
        # fill count REPLACES the claim's holdings for all round math
        # (grants, feeds, the holdings-shrank bypass, the blind-round
        # floor); the claim value is only the fallback when the scan
        # could not cover that service (see _occupying_debit).
        if live_fill:
            claims = {
                name: (dataclasses.replace(claim, holdings_fill=live_fill[name])
                       if name in live_fill else claim
                      ) for name, claim in claims.items()
            }
            sum_holdings = sum(claim.holdings_fill for claim in claims.values())
        # Conservation invariant: the whole-pool total is observed free +
        # live fill holdings + unclaimed fill rows (drainers and former
        # claimants' orphaned rows). A drainer has left its owner's
        # holdings but its pod still occupies the pool (excluded from the
        # measured free), so without the unclaimed term every in-flight
        # cull shrinks the total below the pool's real capacity and the
        # round reclaims slots that are not actually gone; an orphaned
        # fill row occupies its slot the same way, just with no claim
        # left to ever re-adopt it.
        conserved_holdings = sum_holdings + unclaimed_fill
        # Previous single-claimant None grants carry no integer baseline:
        # drop them so damping treats the service as newly-baselined.
        prev_published: dict[str, int] | None = None
        if round_row is not None:
            prev_published = {
                name: value
                for name, value in prev_grants_json.items()
                if isinstance(value, int)
            }
        prev_sum_holdings = (round_row['sum_holdings']
                             if round_row is not None else None)
        prev_shrink_baseline = (round_row['shrink_baseline']
                                if round_row is not None else None)
        if confirmed_phantom:
            # A confirmed v2 phantom is authoritative zero capacity for this
            # physical pool, not a transient measurement blackout. Withdraw
            # both launch and shelter authority, clear sticky feed state, and
            # fence the transition while leaving the complete service claim
            # generation untouched for healthy sibling pools.
            assert protocol_version == PROTOCOL_V2
            last_free, last_free_ts = 0, snapshot_time
            raw_grants = {name: 0 for name in names}
            damped = {name: 0 for name in names}
            feeds = {name: 0 for name in names}
            new_sticky = {}
            utilization_state = {
                name: dict(entry)
                for name, entry in prev_utilization.items()
                if name in claims
            }
            published_sum_holdings = conserved_holdings
            new_shrink_baseline = None
        elif query_ok:
            assert (observation is not None and
                    observation.free_slots is not None)
            measured = max(0, int(observation.free_slots))
            last_free, last_free_ts = measured, snapshot_time
            observed_free = max(0, measured - feed_debit)
            (observed_free, spendable_by_accelerator) = (
                _apply_occupancy_to_exact_card_observation(
                    measured_by_accelerator, observed_free,
                    feed_debit_by_accelerator))
            # The entitlement total only debits the mid-query bind race:
            # bound not-READY pods are already excluded from the measured
            # free AND counted in their owner's fill holdings, so the full
            # feed debit here would double-subtract them for the whole
            # bind->READY window and cull the booting pods (see
            # _occupying_debit). A mid-query FILL bind stays attributed to
            # its owner through the live_fill holdings above, keeping the
            # total conserved.
            entitlement_free = max(0, measured - entitlement_debit)
            total = entitlement_free + conserved_holdings
            # Advance the release governor HERE, inside the measured
            # branch, after the live-holdings correction above (so the
            # actuation gate compares against row-consistent holdings) and
            # immediately before entitlements are computed. Advancing
            # before the query_ok split would let the cap walk down across
            # a run of measurement blackouts in which grants are never
            # recomputed, and then apply the whole accumulated drop in one
            # step once the query recovered.
            claims, utilization_state = _apply_utilization_gate(
                claims, activity, prev_utilization, now)
            raw_grants = compute_entitlements(total, claims)
            # The immediate-down bypass keys on (holdings + draining): a
            # holdings drop whose slots merely moved into a graceful drain
            # is NOT capacity that physically vanished -- the drainers'
            # pods are still bound. And a one-round conserved shrink can
            # be a pure observation artifact: a drain completing between
            # the cluster query and the row scan leaves the slot counted
            # occupied by the query (not free) yet already deleted from
            # the rows (not held, not draining), so BOTH terms omit it for
            # exactly this round; firing the bypass on that phantom culls
            # a warm replica the next query would have vindicated. The
            # bypass therefore requires CONFIRMATION: a shrink below the
            # previous round's conserved sum only records that sum as a
            # pending baseline (this round takes the normal two-round
            # damped path), and only a next round still below the baseline
            # treats the capacity as physically gone. A legitimate fast
            # reclaim (pods really deleted) loses at most one round of
            # down-speed to this -- acceptable, and the ordinary two-round
            # damped down usually lands the same round anyway.
            new_shrink_baseline = None
            if (prev_shrink_baseline is not None and
                    conserved_holdings < int(prev_shrink_baseline)):
                # Confirmed: the shrink persisted across two consecutive
                # row-consistent scans -- pods are physically gone.
                holdings_shrank = True
            elif (prev_sum_holdings is not None and
                  conserved_holdings < int(prev_sum_holdings)):
                # First observation of this shrink: could be the
                # query-then-scan gap; damp normally and remember the
                # pre-shrink baseline for next round's confirmation.
                holdings_shrank = False
                new_shrink_baseline = int(prev_sum_holdings)
            else:
                holdings_shrank = False
            damped = damp_grants(raw_grants, prev_published, prev_raw,
                                 holdings_shrank)
            damped = _clamp_v2_grants(damped, claims, protocol_version)
            # raw_grants clamps each feed need to min(damped, raw): a
            # service inside a down-move's damping window must not be fed
            # above its raw entitlement -- the damped grant catches down
            # next round and the just-launched replica would be culled.
            feeds, new_sticky = compute_feeds(
                observed_free,
                damped,
                claims,
                sticky,
                now,
                constants.RESERVED_FILL_STICKY_FEED_INTERVALS *
                poll_interval_seconds,
                raw_grants=raw_grants)
            published_sum_holdings = conserved_holdings
        else:
            # Measurement blackout: a failed query is not an observation,
            # so it must not CHANGE the allocation -- the previous round's
            # grants are carried forward as-is (floored at each claimant's
            # CURRENT holdings so a blackout never strips a live replica's
            # shelter), never recomputed. Recomputing from a synthesized
            # total (stale last-known free + current holdings) double-
            # counts every slot consumed since the last good measurement:
            # 10 free observed -> 10 launched -> blackout would read
            # 10 + 10 = 20 and the inflated grants would reopen the
            # demand-placement gate on a ten-slot pool. Feeds are 0 (never
            # launch blind), sticky state is carried unchanged (its window
            # is wall-clock, so a short blackout does not break an
            # in-progress streak), and the raw-grant damping baselines,
            # sum_holdings and any pending shrink baseline are carried too
            # -- the blackout is fully transparent to the shrink
            # confirmation, which then compares the last measured round
            # directly against the next one (no bypass evaluation happens
            # on a carried round: grants are not recomputed at all). A
            # claimant with no previous grant (joined during the blackout)
            # gets its holdings floor: nothing new, nothing stripped.
            raw_grants = {name: int(value) for name, value in prev_raw.items()}
            raw_grants = _clamp_v2_grants(raw_grants, claims, protocol_version)
            damped = {}
            for name, claim in claims.items():
                base = (prev_published.get(name)
                        if prev_published is not None else None)
                floor_holdings = claim.holdings_fill
                carried = prev_utilization.get(name)
                if carried is not None:
                    # The holdings floor exists so a blackout never strips a
                    # live replica's shelter, but for a claimant mid-release
                    # it would also UN-DECAY the grant back up to holdings
                    # and make Sum(grants) exceed the round total. Cap the
                    # floor at the release target the claimant had already
                    # walked down to.
                    floor_holdings = min(floor_holdings, int(carried['cap']))
                damped[name] = max(base if base is not None else 0,
                                   floor_holdings)
            damped = _clamp_v2_grants(damped, claims, protocol_version)
            # Carry the release state through the blackout with its clocks
            # pushed forward, so a long outage cannot bank steps and then
            # apply several at once when measurement recovers.
            for name in claims:
                carried = prev_utilization.get(name)
                if carried is None:
                    continue
                utilization_state[name] = {
                    'cap': int(carried['cap']),
                    'hot_until': max(
                        float(carried['hot_until']),
                        now + constants.RESERVED_FILL_IDLE_DWELL_SECONDS),
                    'stepped_at': now,
                    'blind_since': carried.get('blind_since'),
                }
            feeds = {name: 0 for name in claims}
            new_sticky = dict(sticky)
            published_sum_holdings = (int(prev_sum_holdings)
                                      if prev_sum_holdings is not None else
                                      conserved_holdings)
            new_shrink_baseline = (int(prev_shrink_baseline) if
                                   prev_shrink_baseline is not None else None)
        grants = dict(damped)

    if not query_ok:
        spendable_by_accelerator = None
    feed_by_accelerator = (_allocate_feed_by_accelerator(
        feeds, spendable_by_accelerator, observed_free) if query_ok else None)
    service_feed_by_accelerator = (json.dumps(feed_by_accelerator,
                                              sort_keys=True)
                                   if feed_by_accelerator is not None else None)
    feed_envelope: dict[str, Any] | None = feed_by_accelerator
    if feed_envelope is not None:
        assert measured_by_accelerator is not None
        feed_envelope[_OBSERVED_FREE_BY_ACCELERATOR_KEY] = dict(
            measured_by_accelerator)
        if committed_observation is not None:
            # This key first appears after SEQUENCED_ACTIVE's exact writer
            # convergence proof. Keeping legacy rounds byte-compatible avoids
            # mixed-binary epoch churn during the feature-image rollout.
            assert pool_gpus_per_replica is not None
            feed_envelope[_BROKER_SLOT_WIDTH_KEY] = pool_gpus_per_replica
    serialized_feed_by_accelerator = (json.dumps(feed_envelope, sort_keys=True)
                                      if feed_envelope is not None else None)

    grants_changed = round_row is None or prev_grants_json != grants
    # Feeds are part of the allocation the fence protects: a feed-only
    # redistribution (grants damped in place while the launchable-now
    # split moved to a peer) or a positive-feed round giving way to a
    # blackout must fence launch batches queued under the previous round
    # -- their slots may now be fed to someone else, or unmeasurable.
    # V1 multi-claimant rounds only: the v1 single-claimant fast-path feed is
    # raw measured free and redistributes to nobody. Protocol v2 deliberately
    # has no unbounded single-claimant fast path, so feed movement remains
    # fenced even when the partition currently contains one edge.
    feeds_changed = ((protocol_version == PROTOCOL_V2 or len(claims) != 1) and
                     round_row is not None and
                     json.loads(round_row['feeds'] or '{}') != feeds)
    previous_feed_by_accelerator = (_service_feed_payload_for_epoch(
        round_row.get('feed_by_accelerator'))
                                    if round_row is not None else None)
    exact_feed_changed = (protocol_version == PROTOCOL_V2 and
                          round_row is not None and previous_feed_by_accelerator
                          != service_feed_by_accelerator)
    published_claim_generations = (claim_generations
                                   if protocol_version == PROTOCOL_V2 else {})
    metadata_changed = (
        round_row is None or
        _round_protocol_version(round_row) != protocol_version or
        (protocol_version == PROTOCOL_V2 and
         _round_claim_generations(round_row) != published_claim_generations))
    # The ROUND epoch is per-pool (the fencing token the launch path
    # compares against -- pool A's grant churn must not fence pool B's
    # launches). It bumps only when THIS pool's allocation (grants OR
    # feeds) changes, or after a lease-dead gap where every outstanding
    # grant is suspect -- not on every round: per-round bumps would fence
    # out nearly every fill launch in steady state (each service's carried
    # epoch is refreshed only on its own poll), while the fencing intent
    # is precisely "never actuate a superseded allocation". The LEASE
    # epoch is a separate global stream advanced unconditionally per
    # driven round (the pre-query ownership token above).
    prev_round_epoch = (int(round_row['epoch'])
                        if round_row is not None else None)
    new_epoch = prev_round_epoch if prev_round_epoch is not None else 0
    # lease_expired covers the writer that OBSERVED the dead gap;
    # fence_pending covers its crash window: a post-expiry writer's token
    # acquisition already refreshed expires_at, so if it died before
    # publishing, the next writer reads an unexpired lease and only the
    # persisted per-pool marker still demands the bump. The successful
    # publish below clears the marker in the same transaction (safe: any
    # concurrent marker-setter advanced the lease, so this publish would
    # CAS-fail instead of clearing).
    fence_pending = (bool(round_row['fence_pending'])
                     if round_row is not None else False)
    if (grants_changed or feeds_changed or exact_feed_changed or
            metadata_changed or lease_expired or fence_pending):
        new_epoch += 1
    round_id = int(round_row['round_id']) + 1 if round_row is not None else 1
    publication = ReservedFillRoundPublication(
        pool_key=pool_key,
        round_id=round_id,
        snapshot_time=snapshot_time,
        epoch=new_epoch,
        grants=json.dumps(grants, sort_keys=True),
        feeds=json.dumps(feeds, sort_keys=True),
        feed_by_accelerator=serialized_feed_by_accelerator,
        raw_grants=json.dumps(raw_grants, sort_keys=True),
        feed_state=json.dumps(new_sticky, sort_keys=True),
        sum_holdings=published_sum_holdings,
        last_observed_free=last_free,
        last_observed_free_ts=last_free_ts,
        phantom_streak=phantom_streak,
        shrink_baseline=new_shrink_baseline,
        lease_token=lease_token,
        lease_expires_at=now + lease_ttl_seconds,
        protocol_version=protocol_version,
        claim_generations=json.dumps(published_claim_generations,
                                     sort_keys=True),
        utilization_state=(json.dumps(utilization_state, sort_keys=True)
                           if utilization_state else None),
        observation_provenance=observation_provenance)
    published = round_publisher(publication)
    if not published:
        logger.error(
            'Reserved-fill broker: lease token superseded while publishing '
            f'round {round_id} for pool {pool_key} (token {lease_token}); a '
            'replacement writer took over mid-query. Discarding this '
            'observation.')
        return None
    logger.info(
        f'Reserved-fill broker: round {round_id} (epoch {new_epoch}) for '
        f'pool {pool_key}: grants={grants} feeds={feeds} '
        f'feed_by_accelerator={feed_by_accelerator} '
        f'claimants={names}'
        f'{f" utilization={utilization_state}" if utilization_state else ""}.')
    if service_name not in grants:
        # Protocol-v1 confirmed-phantom blackout round: our claim was just
        # rejected along with everyone else's, so there is no allocation to
        # hand back. Protocol v2 retains its claim and returns an explicit
        # zero allocation under the unchanged service generation.
        return None
    # Build through the same generation-aware reader used by fresh-round
    # lookups so the immediate writer path cannot omit authority metadata.
    return _allocation_from_round(
        service_name,
        pool_key, {
            'protocol_version': protocol_version,
            'claim_generations': json.dumps(published_claim_generations,
                                            sort_keys=True),
            'grants': json.dumps(grants, sort_keys=True),
            'feeds': json.dumps(feeds, sort_keys=True),
            'feed_by_accelerator': serialized_feed_by_accelerator,
            'raw_grants': json.dumps(raw_grants, sort_keys=True),
            'round_id': round_id,
            'epoch': new_epoch,
            'snapshot_time': snapshot_time,
            'last_observed_free': last_free,
            'last_observed_free_ts': last_free_ts,
        },
        protocol_version=protocol_version,
        service_generation=service_generation,
        claim_row=claim_rows[service_name])
