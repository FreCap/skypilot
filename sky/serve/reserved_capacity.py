"""Reserved-capacity fill poller.

[boltz fork] Opt-in (replica_policy.reserved_capacity_fill): a controller
background thread that measures FREE capacity on the service's zero-cost
locations (reserved/already-paid Kubernetes pools) and feeds the autoscaler
a snapshot via `collect_reserved_capacity`, so the fleet opportunistically
fills idle reserved GPUs. This module owns only the measurement side; the
target composition lives in `Autoscaler._apply_reserved_capacity_fill` and
the zero-cost-only launch pinning in `ReplicaManager._launch_replica`.

With a service_name the poller participates in the reserved-fill BROKER
(multi-service arbitration, sky/serve/reserved_capacity_broker.py): it
upserts a claim each cycle, lets the broker drive/read the shared per-pool
round (one cluster query per interval across ALL services), and feeds the
autoscaler the broker's feed + grant instead of a privately measured free
level. With a single live claim the broker's fast path reproduces the
standalone behavior exactly.
"""
import asyncio
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
import contextlib
import dataclasses
import enum
import hashlib
import json
import math
import os
import re
import threading
import time
import typing
from typing import Any, Optional

from sky import backends
from sky import clouds
from sky import exceptions
from sky import resources as resources_lib
from sky import sky_logging
from sky.adaptors import kubernetes
from sky.catalog import kubernetes_catalog
from sky.serve import constants
from sky.serve import provider_phase
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import spot_placer as spot_placer_lib
from sky.utils import common_utils
from sky.utils import locks

if typing.TYPE_CHECKING:
    from sky.serve import autoscalers
    from sky.serve import replica_info as replica_info_lib

logger = sky_logging.init_logger(__name__)

ReservedFillLaunchFenceError = exceptions.ReservedFillLaunchFenceError


@dataclasses.dataclass(frozen=True)
class FreeGpuObservation:
    """One cached raw free-GPU value and the query's start time."""

    free_gpus: int | None
    snapshot_time: float | None


@dataclasses.dataclass(frozen=True)
class FillPoolCandidate:
    """One ordered Kubernetes-context group before physical resolution."""

    position: int
    context: str
    shapes: tuple[tuple[str, int], ...]
    locations: tuple['spot_placer_lib.Location', ...]

    @property
    def accelerator_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.shapes)

    @property
    def gpus_per_replica(self) -> int:
        widths = {count for _, count in self.shapes}
        if len(widths) != 1:
            raise ValueError('A fill pool requires one GPU count per replica; '
                             f'got {self.shapes!r}.')
        return next(iter(widths))


@dataclasses.dataclass(frozen=True)
class FillPoolSpec:
    """One resolved protocol-v2 pool edge in stable task-resource order."""

    position: int
    context: str
    shapes: tuple[tuple[str, int], ...]
    locations: tuple['spot_placer_lib.Location', ...]
    physical_cluster_uid: str
    pool_key: str
    legacy_pool_key: str

    @property
    def accelerator_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.shapes)

    @property
    def gpus_per_replica(self) -> int:
        widths = {count for _, count in self.shapes}
        if len(widths) != 1:
            raise ValueError('A fill pool requires one GPU count per replica; '
                             f'got {self.shapes!r}.')
        return next(iter(widths))


@dataclasses.dataclass(frozen=True)
class FillPoolBudgetInput:
    """One pool's inputs to the service-global budget partition."""

    holdings: int
    capacity_hint: int


@dataclasses.dataclass(frozen=True)
class FillPoolBudget:
    """One pool's partitioned broker cap and floor."""

    edge_cap: int
    edge_floor: int


@dataclasses.dataclass(frozen=True)
class ProtocolV2LaunchFence:
    """Immutable reserved-fill authority persisted with one API request."""

    protocol_version: int
    pool_key: str
    service_generation: int
    physical_cluster_uid: str
    kubernetes_context: str
    accelerator: str
    accelerator_count: int


@dataclasses.dataclass(frozen=True)
class ProtocolV2CleanupFence:
    """Physical authority reconstructed from one durable replica row."""

    kubernetes_context: str
    physical_cluster_uid: str


class PhysicalReplicaPresence(enum.Enum):
    """Whether a replica still owns Pods on its fenced physical cluster."""

    # No Pod claims this cluster name, and every observed Pod carried the
    # ownership annotation, so the absence is authoritative.
    ABSENT = 'ABSENT'
    # At least one Pod still claims this cluster name.
    PRESENT = 'PRESENT'
    # The provider could not be read, or a Pod predating the ownership
    # annotation makes a negative answer unsafe to trust.
    UNPROVEN = 'UNPROVEN'


# One provider read serves every replica torn down in the same sweep: a
# service draining hundreds of rows would otherwise issue hundreds of
# identical all-namespace Pod lists against the same physical cluster.
_PHYSICAL_PRESENCE_SNAPSHOT_TTL_SECONDS = 30.0
_physical_presence_snapshots: dict[tuple[str, str],
                                   tuple[float, frozenset[str], frozenset[str],
                                         bool]] = {}
_physical_presence_lock = threading.Lock()


def _read_physical_replica_names(
    fence: ProtocolV2CleanupFence
) -> tuple[frozenset[str], frozenset[str], bool]:
    """List Pod ownership on the fenced physical cluster.

    Returns the annotated logical cluster names, the on-cloud names taken
    from the SkyPilot cluster label, and whether every observed Pod carried
    the annotation (only then can absence be proven).
    """
    # Imported lazily: `sky.provision.__init__` pulls in every cloud
    # provisioner, which the serve control path must not pay for at import.
    # pylint: disable=import-outside-toplevel
    from sky.provision import constants as provision_constants

    with kubernetes.physical_cluster_uid_fence(fence.kubernetes_context,
                                               fence.physical_cluster_uid,
                                               wait_for_initializer=False):
        pods = kubernetes.core_api(
            fence.kubernetes_context).list_pod_for_all_namespaces(
                label_selector=provision_constants.TAG_SKYPILOT_CLUSTER_NAME,
                _request_timeout=kubernetes.API_TIMEOUT).items
    annotated_names: set[str] = set()
    on_cloud_names: set[str] = set()
    fully_annotated = True
    for pod in pods:
        metadata = getattr(pod, 'metadata', None)
        annotations = getattr(metadata, 'annotations', None) or {}
        labels = getattr(metadata, 'labels', None) or {}
        annotated_name = annotations.get(
            provision_constants.TAG_SKYPILOT_CLUSTER_NAME)
        on_cloud_name = labels.get(
            provision_constants.TAG_SKYPILOT_CLUSTER_NAME)
        if isinstance(on_cloud_name, str) and on_cloud_name:
            on_cloud_names.add(on_cloud_name)
        if isinstance(annotated_name, str) and annotated_name:
            annotated_names.add(annotated_name)
        else:
            # A Pod predating the ownership annotation cannot be attributed
            # to a full cluster name, so no absence claim may rely on it.
            fully_annotated = False
    return frozenset(annotated_names), frozenset(
        on_cloud_names), fully_annotated


def probe_physical_replica_presence(
        fence: ProtocolV2CleanupFence,
        cluster_name: str,
        now: float | None = None) -> PhysicalReplicaPresence:
    """Prove whether `cluster_name` still owns Pods on the fenced cluster.

    A cleanup whose durable record vanished is not evidence that provider
    resources leaked: the replica may have been retired before provisioning
    ever created one. Reading the provider converts that ambiguity into a
    fact, so only genuinely unresolved rows are retained for retry.
    """
    if now is None:
        now = time.monotonic()
    key = (fence.kubernetes_context, fence.physical_cluster_uid)
    snapshot: tuple[frozenset[str], frozenset[str], bool] | None = None
    with _physical_presence_lock:
        cached = _physical_presence_snapshots.get(key)
        if (cached is not None and
                now - cached[0] <= _PHYSICAL_PRESENCE_SNAPSHOT_TTL_SECONDS):
            snapshot = (cached[1], cached[2], cached[3])
    if snapshot is None:
        try:
            snapshot = _read_physical_replica_names(fence)
        except Exception as error:  # pylint: disable=broad-except
            # Contention, a retargeted kubeconfig, or an API failure all mean
            # the same thing here: nothing was proven.
            logger.debug(f'Could not read physical Pod ownership on '
                         f'{fence.kubernetes_context!r}: '
                         f'{common_utils.format_exception(error)}')
            return PhysicalReplicaPresence.UNPROVEN
        with _physical_presence_lock:
            _physical_presence_snapshots[key] = (now, snapshot[0], snapshot[1],
                                                 snapshot[2])
    annotated_names, on_cloud_names, fully_annotated = snapshot
    if cluster_name in annotated_names:
        return PhysicalReplicaPresence.PRESENT
    # The on-cloud name is the (possibly shortened) cluster name plus a hash
    # suffix. A hit is positive evidence; a miss alone proves nothing because
    # shortening can drop the prefix.
    prefix = f'{cluster_name}-'
    if any(name == cluster_name or name.startswith(prefix)
           for name in on_cloud_names):
        return PhysicalReplicaPresence.PRESENT
    if not fully_annotated:
        return PhysicalReplicaPresence.UNPROVEN
    return PhysicalReplicaPresence.ABSENT


def ordinary_provider_phase_mode(
    handle: Any,
    cluster_name: str,
) -> provider_phase.ProviderPhaseMode | None:
    """Classify ordinary provider work for Kubernetes phase admission.

    Only an exact durable CloudVm handle with a finalized non-Kubernetes cloud
    may bypass the process gate. Unknown or malformed handles remain ambient
    because a later provider read could still consult mutable kubeconfig.
    """
    if not isinstance(handle, backends.CloudVmRayResourceHandle):
        return provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
    launched_resources = handle.launched_resources
    if launched_resources is None:
        return provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
    cloud = launched_resources.cloud
    if (handle.cluster_name == cluster_name and
            isinstance(cloud, clouds.Cloud) and
            not isinstance(cloud, clouds.Kubernetes)):
        return None
    return provider_phase.ProviderPhaseMode.AMBIENT_LEGACY


@contextlib.contextmanager
def _provider_phase_scope(
    mode: provider_phase.ProviderPhaseMode,
    admission: provider_phase.ProviderPhaseAdmission | None,
) -> typing.Iterator[None]:
    phase_context = (provider_phase.provider_phase(mode) if admission is None
                     else provider_phase.join_provider_phase(admission))
    with phase_context:
        yield


@contextlib.contextmanager
def _protocol_v2_provider_fence_scope(
    cleanup_fence: ProtocolV2CleanupFence,
    *,
    phase_admission: provider_phase.ProviderPhaseAdmission | None,
    include_provider_phase: bool,
    wait_for_initializer: bool,
) -> typing.Iterator[None]:
    phase_context: contextlib.AbstractContextManager[Any]
    if include_provider_phase:
        phase_context = _provider_phase_scope(
            provider_phase.ProviderPhaseMode.V2_FENCED, phase_admission)
    else:
        if phase_admission is not None:
            raise exceptions.ProviderPhaseMisuseError(
                'A physical-only provider fence cannot consume a phase '
                'admission.')
        phase_context = contextlib.nullcontext()

    with phase_context:
        if wait_for_initializer:
            physical_context = kubernetes.physical_cluster_uid_fence(
                cleanup_fence.kubernetes_context,
                cleanup_fence.physical_cluster_uid)
        else:
            physical_context = kubernetes.physical_cluster_uid_fence(
                cleanup_fence.kubernetes_context,
                cleanup_fence.physical_cluster_uid,
                wait_for_initializer=False)
        with physical_context:
            yield


def protocol_v2_provider_fence(
    replica_info: 'replica_info_lib.ReplicaInfo',
    handle: 'backends.CloudVmRayResourceHandle | None' = None,
    *,
    phase_admission: provider_phase.ProviderPhaseAdmission | None = None,
    include_provider_phase: bool = True,
    wait_for_initializer: bool = True,
) -> contextlib.AbstractContextManager[None]:
    """Return the exact provider fence for one durable replica row.

    Genuine ordinary and protocol-v1 rows enter the ambient provider phase.
    A protocol-v2 row must also prove that the supplied durable handle still
    names the same Kubernetes cluster/context before any provider operation is
    allowed.  Child workers must receive their root's explicit admission.
    Malformed rows and missing/replaced handles fail closed.

    ``include_provider_phase=False`` is reserved for interactive log follow:
    it keeps a v2 physical target immutable without monopolizing the bounded
    process phase. Ordinary rows are ungated in that mode.
    """
    cleanup_fence = parse_protocol_v2_cleanup_fence(replica_info)
    if cleanup_fence is None:
        if not include_provider_phase:
            if phase_admission is not None:
                raise exceptions.ProviderPhaseMisuseError(
                    'A physical-only provider fence cannot consume a phase '
                    'admission.')
            return contextlib.nullcontext()
        if (phase_admission is not None and phase_admission.mode
                != provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
            raise exceptions.ProviderPhaseMisuseError(
                'An ordinary provider operation requires ambient admission.')
        if phase_admission is None:
            return _provider_phase_scope(
                provider_phase.ProviderPhaseMode.AMBIENT_LEGACY, None)
        return _provider_phase_scope(
            provider_phase.ProviderPhaseMode.AMBIENT_LEGACY, phase_admission)
    if (phase_admission is not None and
            phase_admission.mode != provider_phase.ProviderPhaseMode.V2_FENCED):
        raise exceptions.ProviderPhaseMisuseError(
            'A protocol-v2 provider operation requires fenced admission.')
    try:
        cluster_name = replica_info.cluster_name
    except AttributeError as error:
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'The durable replica record has no cluster identity.') from error
    if not isinstance(handle, backends.CloudVmRayResourceHandle):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'The durable replica handle does not match its fenced Kubernetes '
            'context.')
    launched_resources = handle.launched_resources
    if (not isinstance(cluster_name, str) or not cluster_name or
            handle.cluster_name != cluster_name or launched_resources is None or
            not isinstance(launched_resources.cloud, clouds.Kubernetes) or
            launched_resources.region != cleanup_fence.kubernetes_context):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'The durable replica handle does not match its fenced Kubernetes '
            'context.')
    return _protocol_v2_provider_fence_scope(
        cleanup_fence,
        phase_admission=phase_admission,
        include_provider_phase=include_provider_phase,
        wait_for_initializer=wait_for_initializer)


@dataclasses.dataclass
class _ProtocolV2BatchFenceHolder:
    """Process-local owner that keeps one physical target pinned."""

    ready: threading.Event
    release: threading.Event
    thread: threading.Thread | None = None
    error: BaseException | None = None


@contextlib.contextmanager
def protocol_v2_provider_batch_fences(
    representatives: Mapping[tuple[str, str], tuple[Any, Any]],
    *,
    phase_admission: provider_phase.ProviderPhaseAdmission | None = None,
    wait_for_initializer: bool = True,
) -> typing.Iterator[dict[tuple[str, str], BaseException]]:
    """Pin each physical target once for a complete provider batch.

    A single caller context cannot hold two Kubernetes contexts at once. One
    short-lived owner thread per physical target therefore keeps the
    process-wide capture alive while batch workers enter their normal central
    per-operation fence. Those nested entries reuse the capture and UID proof
    instead of racing to initialize one proof per fast worker.

    Entry failures are returned by group so callers can isolate only the rows
    whose provider identity is uncertain. Unexpected failures remain typed in
    the result and must be re-raised by callers.
    """
    if phase_admission is None:
        phase_context = provider_phase.provider_phase(
            provider_phase.ProviderPhaseMode.V2_FENCED)
    else:
        if phase_admission.mode != provider_phase.ProviderPhaseMode.V2_FENCED:
            raise exceptions.ProviderPhaseMisuseError(
                'A protocol-v2 provider batch requires fenced admission.')
        phase_context = provider_phase.join_provider_phase(phase_admission)

    with phase_context as active_admission:
        with _protocol_v2_provider_batch_fences(
                representatives,
                active_admission,
                wait_for_initializer=wait_for_initializer) as failures:
            yield failures


@contextlib.contextmanager
def _protocol_v2_provider_batch_fences(
    representatives: Mapping[tuple[str, str], tuple[Any, Any]],
    phase_admission: provider_phase.ProviderPhaseAdmission,
    *,
    wait_for_initializer: bool,
) -> typing.Iterator[dict[tuple[str, str], BaseException]]:
    """Hold one physical owner per target inside an admitted v2 phase."""
    holders: dict[tuple[str, str], _ProtocolV2BatchFenceHolder] = {}
    failures: dict[tuple[str, str], BaseException] = {}

    # Two durable UIDs for one mutable context cannot both be authoritative in
    # a batch. Reject every conflicting group before choosing a winner based
    # on thread scheduling.
    keys_by_context: dict[str, list[tuple[str, str]]] = {}
    for key in representatives:
        keys_by_context.setdefault(key[0], []).append(key)
    conflicted_keys = {
        key for keys in keys_by_context.values()
        if len({candidate[1] for candidate in keys}) > 1 for key in keys
    }
    for key in conflicted_keys:
        failures[key] = exceptions.KubernetesPhysicalClusterIdentityError(
            'One Kubernetes context has conflicting physical-cluster UIDs in '
            'the same provider batch.')

    def _hold(replica_info: Any, handle: Any,
              holder: _ProtocolV2BatchFenceHolder) -> None:
        try:
            with protocol_v2_provider_fence(
                    replica_info,
                    handle,
                    phase_admission=phase_admission,
                    wait_for_initializer=wait_for_initializer):
                holder.ready.set()
                holder.release.wait()
        except asyncio.CancelledError as error:
            holder.error = error
            holder.ready.set()
            raise
        except BaseException as error:  # pylint: disable=broad-exception-caught
            holder.error = error
            holder.ready.set()

    try:
        for key, (replica_info, handle) in representatives.items():
            if key in conflicted_keys:
                continue
            holder = _ProtocolV2BatchFenceHolder(threading.Event(),
                                                 threading.Event())
            thread = threading.Thread(target=_hold,
                                      args=(replica_info, handle, holder),
                                      daemon=True,
                                      name='reserved-fill-provider-fence')
            holder.thread = thread
            holders[key] = holder
            thread.start()
        for key, holder in holders.items():
            holder.ready.wait()
            if holder.error is not None:
                failures[key] = holder.error
        yield failures
    finally:
        for holder in holders.values():
            holder.release.set()
        for holder in holders.values():
            assert holder.thread is not None
            holder.thread.join()


def _normalize_positive_whole_count(value: Any) -> int | None:
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or
            not math.isfinite(value) or value < 1 or
            not float(value).is_integer()):
        return None
    return int(value)


def parse_protocol_v2_cleanup_fence(
        replica_info: Any) -> ProtocolV2CleanupFence | None:
    """Return a complete v2 cleanup fence, legacy None, or fail closed.

    Protocol-v2 rows deployed before the explicit context field retain the
    selected context in their immutable placement ``location``.  New rows
    persist both and require equality, so a partial/corrupt row can never turn
    cleanup into a name-only operation.
    """
    persisted = vars(replica_info)
    pool_key = persisted.get('reserved_fill_pool_key')
    generation = persisted.get('reserved_fill_service_generation')
    physical_uid = persisted.get('reserved_fill_physical_cluster_uid')
    explicit_context = persisted.get('reserved_fill_kubernetes_context')
    has_identity_fields = any(value is not None
                              for value in (pool_key, generation, physical_uid,
                                            explicit_context))
    reserved_fill = persisted.get('reserved_fill', False)
    if reserved_fill is not True:
        if reserved_fill is not False:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Reserved-fill cleanup marker is malformed.')
        if has_identity_fields:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'A non-fill replica carries reserved-fill cleanup authority.')
        return None
    identity = None
    if pool_key is not None:
        if not isinstance(pool_key, str) or not pool_key:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Reserved-fill cleanup identity is malformed.')
        try:
            identity = reserved_capacity_broker.parse_pool_identity(pool_key)
        except (TypeError, ValueError) as error:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Reserved-fill cleanup identity is malformed.') from error

    legacy_generation = (generation is None or
                         type(generation) is int and generation == 0)
    if (identity is None or
            identity.protocol_version == reserved_capacity_broker.PROTOCOL_V1):
        if (legacy_generation and physical_uid is None and
                explicit_context is None):
            return None
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup identity is incomplete.')

    if (identity.protocol_version != reserved_capacity_broker.PROTOCOL_V2 or
            type(generation) is not int or generation < 1 or
            not isinstance(physical_uid, str) or not physical_uid or
            identity.physical_cluster_uid != physical_uid):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup identity is incomplete.')
    raw_location = persisted.get('location')
    if not isinstance(raw_location, Mapping):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup location is missing.')
    location_context = raw_location.get('region')
    location_cloud = raw_location.get('cloud')
    if (not isinstance(location_context, str) or not location_context or
            not isinstance(location_cloud, str) or
            location_cloud.casefold() != 'kubernetes'):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup location is malformed.')
    if explicit_context is None:
        context = location_context
    elif (not isinstance(explicit_context, str) or not explicit_context or
          explicit_context != location_context):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup contexts are contradictory.')
    else:
        context = explicit_context
    resources_override = persisted.get('resources_override')
    if not isinstance(resources_override, Mapping):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup resource pin is missing.')
    resource_cloud = resources_override.get('cloud')
    is_kubernetes_cloud = (isinstance(resource_cloud, clouds.Kubernetes) or
                           isinstance(resource_cloud, str) and
                           resource_cloud.casefold() == 'kubernetes')
    resource_context = resources_override.get('region')
    resource_accelerators = resources_override.get('accelerators')
    location_accelerators = raw_location.get('accelerators')
    if (not is_kubernetes_cloud or resource_context != context or
            not isinstance(resource_accelerators, Mapping) or
            len(resource_accelerators) != 1 or
            not isinstance(location_accelerators, Mapping) or
            len(location_accelerators) != 1):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup resource pin is malformed.')
    resource_card, resource_count = next(iter(resource_accelerators.items()))
    location_card, location_count = next(iter(location_accelerators.items()))
    normalized_resource_count = _normalize_positive_whole_count(resource_count)
    normalized_location_count = _normalize_positive_whole_count(location_count)
    if (not isinstance(resource_card, str) or not resource_card or
            not isinstance(location_card, str) or
            resource_card.casefold() != location_card.casefold() or
            resource_card.casefold() not in identity.gpu_names or
            normalized_resource_count is None or
            normalized_location_count is None or
            normalized_location_count != normalized_resource_count):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill cleanup accelerator pin is contradictory.')
    return ProtocolV2CleanupFence(kubernetes_context=context,
                                  physical_cluster_uid=physical_uid)


def parse_protocol_v2_launch_fence(
    launch_context: Mapping[str, Any],) -> ProtocolV2LaunchFence | None:
    """Parse a complete durable protocol-v2 launch tuple, or no tuple.

    Any claimed reserved-fill launch field makes the whole tuple mandatory.
    This keeps mixed-version or attacker-supplied partial contexts from
    silently degrading into an ordinary Serve launch.
    """
    if not isinstance(launch_context, Mapping):
        raise ValueError('Reserved-fill launch context must be a mapping.')
    claimed_keys = {
        key for key in launch_context if isinstance(key, str) and
        key.startswith(constants.RESERVED_FILL_LAUNCH_FENCE_PREFIX)
    }
    if not claimed_keys:
        return None
    expected_keys = set(constants.RESERVED_FILL_LAUNCH_FENCE_KEYS)
    if claimed_keys != expected_keys:
        raise ValueError('Reserved-fill launch context is incomplete.')

    protocol_version = launch_context[
        constants.RESERVED_FILL_LAUNCH_PROTOCOL_VERSION_KEY]
    pool_key = launch_context[constants.RESERVED_FILL_LAUNCH_POOL_KEY]
    service_generation = launch_context[
        constants.RESERVED_FILL_LAUNCH_SERVICE_GENERATION_KEY]
    physical_cluster_uid = launch_context[
        constants.RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY]
    kubernetes_context = launch_context[
        constants.RESERVED_FILL_LAUNCH_KUBERNETES_CONTEXT_KEY]
    accelerator = launch_context[constants.RESERVED_FILL_LAUNCH_ACCELERATOR_KEY]
    accelerator_count = launch_context[
        constants.RESERVED_FILL_LAUNCH_ACCELERATOR_COUNT_KEY]

    if (type(protocol_version) is not int or  # pylint: disable=unidiomatic-typecheck
            protocol_version != reserved_capacity_broker.PROTOCOL_V2):
        raise ValueError('Reserved-fill launch protocol must be v2.')
    if not isinstance(pool_key, str) or not pool_key:
        raise ValueError('Reserved-fill launch pool key is invalid.')
    if (type(service_generation) is not int or  # pylint: disable=unidiomatic-typecheck
            service_generation < 1):
        raise ValueError('Reserved-fill service generation is invalid.')
    for value, field_name in ((physical_cluster_uid, 'physical cluster UID'),
                              (kubernetes_context, 'Kubernetes context'),
                              (accelerator, 'accelerator')):
        if not isinstance(value, str) or not value:
            raise ValueError(f'Reserved-fill {field_name} is invalid.')
    if (type(accelerator_count) is not int or  # pylint: disable=unidiomatic-typecheck
            accelerator_count < 1):
        raise ValueError('Reserved-fill accelerator count is invalid.')

    try:
        identity = reserved_capacity_broker.parse_pool_identity(pool_key)
    except (TypeError, ValueError) as error:
        raise ValueError('Reserved-fill launch pool key is invalid.') from error
    if (identity.protocol_version != reserved_capacity_broker.PROTOCOL_V2 or
            identity.physical_cluster_uid != physical_cluster_uid):
        raise ValueError('Reserved-fill launch pool identity is contradictory.')
    canonical_accelerator = accelerator.casefold()
    if canonical_accelerator not in identity.gpu_names:
        raise ValueError('Reserved-fill accelerator is outside its pool.')

    return ProtocolV2LaunchFence(protocol_version=protocol_version,
                                 pool_key=pool_key,
                                 service_generation=service_generation,
                                 physical_cluster_uid=physical_cluster_uid,
                                 kubernetes_context=kubernetes_context,
                                 accelerator=canonical_accelerator,
                                 accelerator_count=accelerator_count)


def make_protocol_v2_launch_fence(
    *,
    pool_key: str,
    service_generation: int,
    physical_cluster_uid: str,
    kubernetes_context: str,
    accelerator: str,
    accelerator_count: int,
) -> dict[str, Any]:
    """Build the canonical durable tuple for one selected fill location."""
    launch_context = {
        constants.RESERVED_FILL_LAUNCH_PROTOCOL_VERSION_KEY:
            reserved_capacity_broker.PROTOCOL_V2,
        constants.RESERVED_FILL_LAUNCH_POOL_KEY: pool_key,
        constants.RESERVED_FILL_LAUNCH_SERVICE_GENERATION_KEY: service_generation,
        constants.RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY: physical_cluster_uid,
        constants.RESERVED_FILL_LAUNCH_KUBERNETES_CONTEXT_KEY: kubernetes_context,
        constants.RESERVED_FILL_LAUNCH_ACCELERATOR_KEY: accelerator.casefold(),
        constants.RESERVED_FILL_LAUNCH_ACCELERATOR_COUNT_KEY: accelerator_count,
    }
    # Keep producer and consumer validation identical.
    fence = parse_protocol_v2_launch_fence(launch_context)
    assert fence is not None
    return launch_context


def validate_protocol_v2_launch_resources(
    fence: ProtocolV2LaunchFence,
    resources: resources_lib.Resources,
) -> None:
    """Require one final provider candidate to retain the durable exact pin."""
    if (resources is None or
            not isinstance(resources.cloud, clouds.Kubernetes) or
            resources.region != fence.kubernetes_context):
        raise ValueError('Reserved-fill Kubernetes context changed.')
    accelerators = resources.accelerators
    if not isinstance(accelerators, Mapping) or len(accelerators) != 1:
        raise ValueError('Reserved-fill accelerator shape changed.')
    accelerator, count = next(iter(accelerators.items()))
    if (not isinstance(accelerator, str) or
            accelerator.casefold() != fence.accelerator or
            isinstance(count, bool) or not isinstance(count, (int, float)) or
            count < 1 or not float(count).is_integer() or
            int(count) != fence.accelerator_count):
        raise ValueError('Reserved-fill accelerator shape changed.')


def allocate_fill_pool_budgets(
    global_budget: int,
    service_floor: int,
    pools: tuple[FillPoolBudgetInput, ...],
) -> tuple[FillPoolBudget, ...]:
    """Partition one service budget over ordered physical pool edges.

    Existing holdings are retained first, clipped by the hard global budget.
    Residual budget is equal-weight water-filled up to each pool's capacity
    hint; integer remainder follows stable input order.  The service floor is
    then assigned in that same order without exceeding an edge cap.
    """
    if (isinstance(global_budget, bool) or not isinstance(global_budget, int) or
            global_budget < 0):
        raise ValueError('global_budget must be a nonnegative integer.')
    if (isinstance(service_floor, bool) or not isinstance(service_floor, int) or
            service_floor < 0):
        raise ValueError('service_floor must be a nonnegative integer.')
    for pool in pools:
        if (isinstance(pool.holdings, bool) or
                not isinstance(pool.holdings, int) or pool.holdings < 0 or
                isinstance(pool.capacity_hint, bool) or
                not isinstance(pool.capacity_hint, int) or
                pool.capacity_hint < 0):
            raise ValueError('Pool holdings and capacity hints must be '
                             'nonnegative integers.')

    caps: list[int] = []
    remaining = global_budget
    for pool in pools:
        retained = min(pool.holdings, remaining)
        caps.append(retained)
        remaining -= retained

    while remaining > 0:
        eligible = [
            index for index, pool in enumerate(pools)
            if caps[index] < max(pool.holdings, pool.capacity_hint)
        ]
        if not eligible:
            break
        share, remainder = divmod(remaining, len(eligible))
        allocated = 0
        for position, index in enumerate(eligible):
            requested = share + int(position < remainder)
            if requested == 0:
                continue
            limit = max(pools[index].holdings, pools[index].capacity_hint)
            give = min(requested, limit - caps[index])
            caps[index] += give
            allocated += give
        if allocated == 0:
            break
        remaining -= allocated

    floor_remaining = min(service_floor, global_budget)
    floors: list[int] = []
    for cap in caps:
        assigned = min(cap, floor_remaining)
        floors.append(assigned)
        floor_remaining -= assigned
    return tuple(
        FillPoolBudget(edge_cap=cap, edge_floor=floor)
        for cap, floor in zip(caps, floors))


_DEMAND_REFRESH_STATE_LOCK = threading.Lock()
_DEMAND_REFRESH_PENDING_CONTEXTS: set[str] = set()
_DEMAND_REFRESH_RUNNING = False

_PHYSICAL_CLUSTER_UID_CACHE_LOCK = threading.Lock()
# context -> (physical uid, expiry on the monotonic clock, lookup generation)
_PHYSICAL_CLUSTER_UID_CACHE: dict[str, tuple[str, float, int]] = {}
# A slow older request must not overwrite the cache result of a newer request.
_PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS: dict[str, int] = {}
# Independent UID discovery may briefly collide with an exact physical owner.
# One absolute deadline bounds every owner/initializer replacement race.
_PHYSICAL_CLUSTER_UID_FENCE_RETIREMENT_TIMEOUT_SECONDS = 30.0


def poll_interval_seconds() -> float:
    override = os.environ.get(constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR)
    if override is not None:
        try:
            return max(1.0, float(override))
        except ValueError:
            logger.warning(
                f'Invalid {constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR} '
                f'value {override!r}, using default '
                f'{constants.RESERVED_CAPACITY_POLL_INTERVAL_SECONDS}s.')
    return float(constants.RESERVED_CAPACITY_POLL_INTERVAL_SECONDS)


def get_kubernetes_physical_cluster_uid(
    context: str,
    *,
    force_refresh: bool = False,
) -> str | None:
    """Resolve a context to the physical cluster's kube-system UID.

    Successful reads are cached for at most one poll interval.  A forced read
    is used at launch time to fence a context that was retargeted after the
    broker observation.  Failures never fall back to an expired identity.
    """
    now = time.monotonic()
    with _PHYSICAL_CLUSTER_UID_CACHE_LOCK:
        cached = _PHYSICAL_CLUSTER_UID_CACHE.get(context)
        if (not force_refresh and cached is not None and now < cached[1]):
            return cached[0]
        if cached is not None and now >= cached[1]:
            _PHYSICAL_CLUSTER_UID_CACHE.pop(context, None)
        lookup_generation = (
            _PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.get(context, 0) + 1)
        _PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS[context] = lookup_generation

    lookup_error: Exception | None = None
    retirement_deadline: float | None = None
    uid = ''
    while True:
        try:
            namespace = kubernetes.core_api(context).read_namespace(
                'kube-system', _request_timeout=kubernetes.API_TIMEOUT)
            metadata = getattr(namespace, 'metadata', None)
            raw_uid = getattr(metadata, 'uid', None)
            uid = raw_uid.strip() if isinstance(raw_uid, str) else ''
            if not uid:
                raise ValueError('kube-system namespace has no UID')
            break
        except exceptions.KubernetesPhysicalClusterFenceBusyError as busy_error:
            # This typed tokenless collision alone is retryable. Never borrow
            # an unrelated capture; wait for retirement and read again from
            # fresh ambient credentials.
            if retirement_deadline is None:
                retirement_deadline = (
                    time.monotonic() +
                    _PHYSICAL_CLUSTER_UID_FENCE_RETIREMENT_TIMEOUT_SECONDS)
            if busy_error.context != context:
                lookup_error = busy_error
                break
            try:
                retired = (
                    kubernetes.wait_for_physical_cluster_uid_fence_retirement(
                        context, retirement_deadline,
                        busy_error.failure_generation))
            except exceptions.KubernetesPhysicalClusterIdentityError as wait_error:
                lookup_error = wait_error
                break
            if not retired:
                lookup_error = busy_error
                break
        except Exception as error:  # pylint: disable=broad-except
            lookup_error = error
            break

    if lookup_error is not None:
        with _PHYSICAL_CLUSTER_UID_CACHE_LOCK:
            if (_PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.get(context) ==
                    lookup_generation):
                _PHYSICAL_CLUSTER_UID_CACHE.pop(context, None)
        logger.warning('Reserved-capacity physical-cluster identity lookup '
                       f'failed for context {context!r}: '
                       f'{common_utils.format_exception(lookup_error)}')
        return None

    expires_at = time.monotonic() + poll_interval_seconds()
    with _PHYSICAL_CLUSTER_UID_CACHE_LOCK:
        if (_PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.get(context) ==
                lookup_generation):
            _PHYSICAL_CLUSTER_UID_CACHE[context] = (uid, expires_at,
                                                    lookup_generation)
            return uid

        # A newer lookup completed (or failed) while this request was in
        # flight.  Returning this request's now-stale UID would let a forced
        # launch-time check accept the identity that was current before a
        # context retarget, even though the newer observation already fenced
        # it.  Prefer the newer generation's still-live cache entry.
        current = _PHYSICAL_CLUSTER_UID_CACHE.get(context)
        if current is not None:
            current_uid, current_expires_at, current_generation = current
            if (current_generation > lookup_generation and
                    time.monotonic() < current_expires_at):
                return current_uid
            if current_generation > lookup_generation:
                _PHYSICAL_CLUSTER_UID_CACHE.pop(context, None)
        # No newer entry to defer to, because the newer lookup has not
        # finished yet. This caller did complete its own successful read of
        # this very context, and that read happened after its own request, so
        # it satisfies what both consumers need:
        #
        #  - the observation path, where discarding it drops the pool edge for
        #    the cycle (`resolve_fill_pool_specs` skips a candidate that
        #    resolves to None) and switches fill off fleet-wide;
        #  - the launch fence, where `_authorize_reserved_fill_launch`
        #    compares this value to the pinned pool UID and reads None as
        #    `fill-physical-cluster-uid-mismatch`, refusing every fill launch.
        #
        # Returning None here was never a stronger fence: the generation stamp
        # orders lookup STARTS, not the reads themselves, so a losing reader's
        # value is not demonstrably older than the winner's. If the newer
        # lookup does land a different identity it owns the cache, and the
        # next check sees it. Report the read without publishing it.
        return uid


def group_zero_cost_fill_pools(
    zero_cost_locations: list['spot_placer_lib.Location'],
) -> tuple[FillPoolCandidate, ...]:
    """Group zero-cost Kubernetes locations by context in input order.

    Pool order is the first matching task-resource position. Accelerator names
    are canonicalized case-insensitively, while locations retain their input
    order for deterministic launch selection.  One physical context must use
    one positive whole GPU width; separate contexts may use different widths.
    """
    grouped: dict[str, dict[str, Any]] = {}
    for position, location in enumerate(zero_cost_locations):
        if str(location.cloud).lower() != 'kubernetes':
            continue
        if not location.accelerators:
            continue
        gpu_name, raw_count = next(iter(location.accelerators.items()))
        is_numeric = (not isinstance(raw_count, bool) and
                      isinstance(raw_count, (int, float)))
        is_finite = is_numeric and math.isfinite(float(raw_count))
        if (not is_finite or not float(raw_count).is_integer() or
                float(raw_count) < 1):
            raise ValueError('Reserved-fill capacity requires each '
                             'Kubernetes GPU count to be a positive whole '
                             f'number; got {gpu_name}:{raw_count!r}.')
        context = location.region
        if not isinstance(context, str) or not context:
            raise ValueError('Reserved-fill Kubernetes locations require a '
                             f'nonempty context; got {context!r}.')
        normalized_name = str(gpu_name).lower()
        if not normalized_name:
            raise ValueError('Reserved-fill Kubernetes locations require a '
                             'nonempty accelerator name.')
        exact_count = int(raw_count)
        group = grouped.setdefault(context, {
            'position': position,
            'shapes': {},
            'widths': set(),
            'locations': [],
        })
        group['shapes'][normalized_name] = exact_count
        group['widths'].add(exact_count)
        group['locations'].append(location)

    candidates: list[FillPoolCandidate] = []
    for context, group in grouped.items():
        widths = group['widths']
        if len(widths) != 1:
            raise ValueError('Reserved-fill capacity requires one GPU count '
                             'within each Kubernetes context; context '
                             f'{context!r} has widths {sorted(widths)}.')
        candidates.append(
            FillPoolCandidate(position=group['position'],
                              context=context,
                              shapes=tuple(sorted(group['shapes'].items())),
                              locations=tuple(group['locations'])))
    return tuple(candidates)


def resolve_fill_pool_specs(
    candidates: tuple[FillPoolCandidate, ...],) -> tuple[FillPoolSpec, ...]:
    """Resolve physical identities and reject later alias/overlap edges.

    A failed identity lookup removes only that candidate.  For aliases that
    resolve to the same physical cluster and overlap in accelerator names, the
    first task-resource position survives deterministically.
    """
    resolved: list[FillPoolSpec] = []
    physical_accelerators: dict[str, set[str]] = {}
    for candidate in candidates:
        physical_uid = get_kubernetes_physical_cluster_uid(candidate.context)
        if physical_uid is None:
            logger.error('Reserved-fill pool edge for context '
                         f'{candidate.context!r} is inactive because its '
                         'physical cluster identity could not be resolved.')
            continue
        accelerator_names = candidate.accelerator_names
        prior_names = physical_accelerators.setdefault(physical_uid, set())
        overlap = prior_names.intersection(accelerator_names)
        if overlap:
            logger.error(
                'Reserved-fill pool edge for context '
                f'{candidate.context!r} overlaps an earlier context alias '
                f'on physical cluster {physical_uid!r} for accelerators '
                f'{sorted(overlap)}; keeping the first task-resource edge.')
            continue
        prior_names.update(accelerator_names)
        pool_key = reserved_capacity_broker.make_pool_key(
            candidate.context,
            accelerator_names,
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid=physical_uid)
        legacy_pool_key = reserved_capacity_broker.make_pool_key(
            candidate.context, accelerator_names)
        resolved.append(
            FillPoolSpec(position=candidate.position,
                         context=candidate.context,
                         shapes=candidate.shapes,
                         locations=candidate.locations,
                         physical_cluster_uid=physical_uid,
                         pool_key=pool_key,
                         legacy_pool_key=legacy_pool_key))
    return tuple(resolved)


def discover_fill_pool_specs(
    zero_cost_locations: list['spot_placer_lib.Location'],
) -> tuple[FillPoolSpec, ...]:
    """Build the ordered, physically resolved protocol-v2 pool set."""
    return resolve_fill_pool_specs(
        group_zero_cost_fill_pools(zero_cost_locations))


def zero_cost_pool_shapes(
    zero_cost_locations: list['spot_placer_lib.Location']
) -> dict[tuple[str, str], int]:
    """Per-(context, gpu) pool shapes of the zero-cost location set.

    Pure spec parsing (no cluster query). Rules:
    - Only Kubernetes locations are queryable in v0/v1; other zero-cost
      locations contribute no pool.
    - Same (context, gpu) shape enumerated with different per-replica
      counts (e.g. A100:1 and A100:8 entries over one pool) draws from
      the same free GPUs: count the key once with the LARGEST
      per-replica size -- deterministic and conservative (fewest fill
      launches). A first-seen-wins dedupe would let any_of entry ORDER
      change the fill level.
    - Lowercased gpu name: the realtime query matches
      case-insensitively, so 'A100' and 'a100' entries hit the same
      pool and must dedupe to one key.
    """
    per_key_replica_size: dict[tuple[str, str], int] = {}
    for location in zero_cost_locations:
        if str(location.cloud).lower() != 'kubernetes':
            continue
        if not location.accelerators:
            continue
        gpu_name, per_replica = next(iter(location.accelerators.items()))
        is_numeric = (not isinstance(per_replica, bool) and
                      isinstance(per_replica, (int, float)))
        is_finite = is_numeric and math.isfinite(float(per_replica))
        if (not is_finite or not float(per_replica).is_integer() or
                float(per_replica) < 1):
            logger.error('Reserved-fill capacity has an invalid Kubernetes '
                         f'GPU shape {gpu_name}:{per_replica!r}; each count '
                         'must be a positive whole number. Fill is inactive '
                         'for this service.')
            return {}
        exact_per_replica = int(per_replica)
        key = (location.region, gpu_name.lower())
        per_key_replica_size[key] = max(per_key_replica_size.get(key, 1),
                                        exact_per_replica)
    return per_key_replica_size


def query_pool_observation(
        context: str, gpu_name: str,
        per_replica: int) -> reserved_capacity_broker.PoolObservation:
    """Realtime free-slot measurement of one (context, gpu) pool.

    EXPENSIVE: the realtime availability query lists every pod in the
    cluster and is deliberately uncached -- call it ONLY from the poller
    thread (or the broker round it drives), never from the autoscaler
    decision tick.

    Unknown availability (any negative count, e.g. a swallowed pod-list
    403 surfacing as {'A100': -1}) is a MEASUREMENT BLACKOUT
    (free_slots=None), exactly like a raised query error: converting it
    to an authoritative 0 would let a new claimant or weight change
    redistribute grants and drain existing holdings while availability is
    unknown -- precisely what the broker's blackout semantics prohibit.
    (Single-claimant observable behavior is unchanged: a blackout feeds 0,
    same as a 0 measurement.) A FAILED/unknown query is distinct from a
    successful 0 (full pool). gpu_names carries the canonical accelerator
    names the query saw, the broker's phantom-pool signal (empty = the
    claimed GPU resolves to no labeled nodes).
    """
    try:
        _, _, available = kubernetes_catalog.list_accelerators_realtime(
            gpus_only=True,
            name_filter=f'^{re.escape(gpu_name)}$',
            region_filter=context,
            quantity_filter=None,
            case_sensitive=False,
            require_price=False)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Reserved-capacity poll failed for context '
                       f'{context!r} gpu {gpu_name!r}: '
                       f'{common_utils.format_exception(e)}')
        return reserved_capacity_broker.PoolObservation(free_slots=None)
    if any(count < 0 for count in available.values()):
        logger.warning('Reserved-capacity poll: availability unknown for '
                       f'context {context!r} gpu {gpu_name!r} '
                       f'({available}); treating as a measurement blackout.')
        return reserved_capacity_broker.PoolObservation(free_slots=None,
                                                        gpu_names=tuple(
                                                            available.keys()))
    free_gpus = sum(count for count in available.values() if count > 0)
    free_slots = free_gpus // max(1, per_replica)
    return reserved_capacity_broker.PoolObservation(
        free_slots=free_slots,
        gpu_names=tuple(available.keys()),
        free_slots_by_accelerator=((gpu_name.lower(), free_slots),))


def query_pool_group_observation(
    context: str,
    shapes: dict[str, int],
    *,
    expected_physical_cluster_uid: str | None = None,
) -> reserved_capacity_broker.PoolObservation:
    """Measure several accelerator names in one Kubernetes context query."""
    try:
        provider_fence = (contextlib.nullcontext()
                          if expected_physical_cluster_uid is None else
                          kubernetes.physical_cluster_uid_fence(
                              context, expected_physical_cluster_uid))
        with provider_fence:
            _, _, available = kubernetes_catalog.list_accelerators_realtime(
                gpus_only=True,
                name_filter=None,
                region_filter=context,
                quantity_filter=None,
                case_sensitive=False,
                require_price=False)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Reserved-capacity group poll failed for context '
                       f'{context!r}: {common_utils.format_exception(e)}')
        return reserved_capacity_broker.PoolObservation(free_slots=None)
    available_lower = {
        str(gpu_name).lower(): count for gpu_name, count in available.items()
    }
    requested_counts = [available_lower.get(name, 0) for name in shapes]
    if any(count < 0 for count in requested_counts):
        logger.warning('Reserved-capacity group availability is unknown for '
                       f'context {context!r} ({available}).')
        return reserved_capacity_broker.PoolObservation(free_slots=None,
                                                        gpu_names=tuple(
                                                            available.keys()))
    free_slots_by_accelerator = tuple(
        (name, max(0, available_lower.get(name, 0)) // per_replica)
        for name, per_replica in shapes.items())
    free_slots = sum(count for _, count in free_slots_by_accelerator)
    matched_names = tuple(
        name for name in available if str(name).lower() in shapes)
    return reserved_capacity_broker.PoolObservation(
        free_slots=free_slots,
        gpu_names=matched_names,
        free_slots_by_accelerator=(free_slots_by_accelerator))


def query_free_slots(
        zero_cost_locations: list['spot_placer_lib.Location']) -> int:
    """Free replica slots across the zero-cost locations, summed per shape.

    Standalone (non-broker) measurement: shapes are assumed to map to
    disjoint node pools (v0; overlapping pools would double-count and are
    explicitly out of scope). A failed context contributes 0 this cycle;
    the autoscaler's staleness decay handles a persistently failing
    poller.
    """
    total = 0
    for (context, gpu_name
        ), per_replica in zero_cost_pool_shapes(zero_cost_locations).items():
        observation = query_pool_observation(context, gpu_name, per_replica)
        if observation.free_slots is not None:
            total += observation.free_slots
    return total


def query_free_slots_by_context(
    zero_cost_locations: list['spot_placer_lib.Location']
) -> dict[str, int | None]:
    """Measure free replica slots with one cluster query per context.

    Demand placement can contain several Kubernetes accelerator shapes in the
    same context.  Calling :func:`query_pool_observation` once per shape would
    repeat the expensive cluster-wide pod listing for every shape.  Fetch all
    accelerator availability in a context once, then project that snapshot
    onto the shapes the placer can actually use.

    ``None`` means the context could not be measured.  A missing accelerator
    key is a successful zero-capacity observation, while a negative value is
    the catalog's explicit unknown-availability sentinel.
    """
    shapes_by_context: dict[str, dict[str, int]] = {}
    for (context, gpu_name
        ), per_replica in zero_cost_pool_shapes(zero_cost_locations).items():
        shapes_by_context.setdefault(context, {})[gpu_name] = per_replica

    result: dict[str, int | None] = {}
    for context, shapes in shapes_by_context.items():
        try:
            _, _, available = kubernetes_catalog.list_accelerators_realtime(
                gpus_only=True,
                name_filter=None,
                region_filter=context,
                quantity_filter=None,
                case_sensitive=False,
                require_price=False)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Zero-cost demand capacity query failed for '
                           f'context {context!r}: '
                           f'{common_utils.format_exception(e)}')
            result[context] = None
            continue

        available_lower = {
            str(gpu_name).lower(): count
            for gpu_name, count in available.items()
        }
        requested_counts = [
            available_lower.get(gpu_name, 0) for gpu_name in shapes
        ]
        if any(count < 0 for count in requested_counts):
            logger.warning('Zero-cost demand capacity is unknown for '
                           f'context {context!r} ({available}).')
            result[context] = None
            continue
        result[context] = sum(
            max(0, available_lower.get(gpu_name, 0)) // per_replica
            for gpu_name, per_replica in shapes.items())
    return result


def _observation_is_fresh(row: dict[str, Any] | None, now: float) -> bool:
    return (row is not None and
            now - float(row['completed_at']) <= poll_interval_seconds())


def _refresh_demand_capacity_contexts(contexts: set[str]) -> None:
    """Refresh stale context rows under one cross-controller query lock."""
    try:
        # Provider admission precedes the distributed query lock. The callback
        # below must never discover that a v2 round started after it already
        # owns a lower-level broker resource.
        with provider_phase.provider_phase(
                provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
            lock = locks.get_lock(constants.DEMAND_CAPACITY_REFRESH_LOCK_ID)
            with lock.acquire(blocking=False):
                now = time.time()
                rows = serve_state.get_demand_capacity_observations(contexts)
                for context in sorted(contexts):
                    if _observation_is_fresh(rows.get(context), now):
                        continue
                    # Capture before the expensive query. A replica row created
                    # during it is debited from the cached result by the planner.
                    snapshot_time = time.time()
                    availability: dict[str, int] | None
                    try:
                        _, _, available = (
                            kubernetes_catalog.list_accelerators_realtime(
                                gpus_only=True,
                                name_filter=None,
                                region_filter=context,
                                quantity_filter=None,
                                case_sensitive=False,
                                require_price=False))
                        availability = {
                            str(gpu_name).lower(): int(count)
                            for gpu_name, count in available.items()
                        }
                    except Exception as e:  # pylint: disable=broad-except
                        logger.warning(
                            'Shared demand-capacity query failed for context '
                            f'{context!r}: {common_utils.format_exception(e)}')
                        availability = None
                    serve_state.upsert_demand_capacity_observation(
                        context, snapshot_time, time.time(), availability)
    except locks.LockTimeout:
        # Another controller is already producing the shared observation.
        # The next reconciliation tick will consume its durable result.
        return


def _demand_capacity_refresh_worker() -> None:
    global _DEMAND_REFRESH_RUNNING
    while True:
        with _DEMAND_REFRESH_STATE_LOCK:
            contexts = set(_DEMAND_REFRESH_PENDING_CONTEXTS)
            _DEMAND_REFRESH_PENDING_CONTEXTS.clear()
            if not contexts:
                _DEMAND_REFRESH_RUNNING = False
                return
        try:
            _refresh_demand_capacity_contexts(contexts)
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Shared demand-capacity refresh failed: '
                         f'{common_utils.format_exception(e)}')


def _schedule_demand_capacity_refresh(contexts: set[str]) -> None:
    """Coalesce refresh work without issuing provider calls on the caller."""
    global _DEMAND_REFRESH_RUNNING
    if not contexts:
        return
    with _DEMAND_REFRESH_STATE_LOCK:
        _DEMAND_REFRESH_PENDING_CONTEXTS.update(contexts)
        if _DEMAND_REFRESH_RUNNING:
            return
        _DEMAND_REFRESH_RUNNING = True
    worker = threading.Thread(target=_demand_capacity_refresh_worker,
                              name='serve-demand-capacity-refresh',
                              daemon=True)
    try:
        worker.start()
    except RuntimeError as e:
        # Thread.start() can fail under transient process-wide thread
        # exhaustion. No worker exists to release this reservation, so make
        # the pending contexts retryable by the next reconciliation tick.
        with _DEMAND_REFRESH_STATE_LOCK:
            _DEMAND_REFRESH_RUNNING = False
        logger.error('Failed to start shared demand-capacity refresh worker: '
                     f'{common_utils.format_exception(e)}')


def get_cached_free_gpus_by_pool(
    zero_cost_locations: list['spot_placer_lib.Location']
) -> dict[tuple[str, str], FreeGpuObservation]:
    """Read shared raw free GPUs and asynchronously refresh stale contexts.

    This function performs only one batched database read on the reconciliation
    path. Kubernetes/provider calls run in a coalesced daemon worker and are
    serialized across controller processes by a distributed lock.
    """
    pool_keys = set(zero_cost_pool_shapes(zero_cost_locations))
    contexts = {context for context, _ in pool_keys}
    rows = serve_state.get_demand_capacity_observations(contexts)
    now = time.time()
    stale_contexts = {
        context for context in contexts
        if not _observation_is_fresh(rows.get(context), now)
    }
    _schedule_demand_capacity_refresh(stale_contexts)

    observations: dict[tuple[str, str], FreeGpuObservation] = {}
    for context, gpu_name in pool_keys:
        row = rows.get(context)
        if context in stale_contexts or row is None:
            observations[(context, gpu_name)] = FreeGpuObservation(None, None)
            continue
        availability_json = row['availability']
        if availability_json is None:
            free_gpus = None
        else:
            availability = json.loads(availability_json)
            count = int(availability.get(gpu_name, 0))
            free_gpus = None if count < 0 else max(0, count)
        observations[(context, gpu_name)] = FreeGpuObservation(
            free_gpus, float(row['snapshot_time']))
    return observations


def _standalone_cycle(autoscaler: 'autoscalers.Autoscaler',
                      zero_cost: list['spot_placer_lib.Location'],
                      keys: list[dict[str, Any]]) -> None:
    """Pre-broker measurement cycle: private query, no arbitration."""
    # Snapshot time is captured BEFORE the (slow, cluster-wide)
    # availability query: a zero-cost replica row created while the query
    # runs already occupies a slot the query may still have counted free,
    # and the post-snapshot debit (created_at > snapshot_time) only
    # catches it if the snapshot predates the row.
    snapshot_time = time.time()
    with provider_phase.provider_phase(
            provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
        free_slots = query_free_slots(zero_cost)
    autoscaler.collect_reserved_capacity(free_slots, keys, snapshot_time)
    logger.info(f'Reserved-capacity poll: {free_slots} free '
                f'slot(s) across {len(keys)} zero-cost '
                'location(s).')


def _placer_can_launch_zero_cost(placer: 'spot_placer_lib.SpotPlacer') -> bool:
    """Whether any zero-cost location is effectively ACTIVE (not benched)."""
    active = placer.active_locations()
    return any(location in active for location in placer.zero_cost_locations())


def _record_pool_observation(
    placer: 'spot_placer_lib.SpotPlacer',
    locations: Sequence['spot_placer_lib.Location'],
    observation: 'reserved_capacity_broker.PoolObservation | None',
    observed_at: float,
) -> None:
    """Hand a round's measured free slots to the placer.

    A reserved Kubernetes pool is counted every round, so a bench on it is
    not standing in for missing information the way a spot region's is. Once
    the placer holds the count it can keep the pool selectable instead of
    rationing one probe per TTL window, which is what bounds refill after a
    full-cluster preemption.

    Prefers the exact per-accelerator split when the provider published one,
    so a pool whose A100 shape is full and whose A100-80GB shape is free does
    not advertise the full shape as available.
    """
    if observation is None or observation.free_slots is None:
        # A failed query is a blackout, not a measurement of zero: leave the
        # existing reading (and its own freshness clock) alone.
        return
    by_accelerator = observation.free_slots_by_accelerator
    free_by_name: dict[str, int] | None = None
    if by_accelerator:
        free_by_name = {
            str(name).lower(): int(count) for name, count in by_accelerator
        }
    free_by_location: dict[spot_placer_lib.Location, int] = {}
    for location in locations:
        if free_by_name is None:
            free_by_location[location] = int(observation.free_slots)
            continue
        accelerators = location.accelerators or {}
        free_by_location[location] = sum(
            free_by_name.get(str(name).lower(), 0) for name in accelerators)
    placer.observe_zero_cost_capacity(free_by_location, observed_at)


def _record_allocation_observation(
    placer: 'spot_placer_lib.SpotPlacer',
    locations: Sequence['spot_placer_lib.Location'],
    allocation: 'reserved_capacity_broker.Allocation',
) -> None:
    """Record only capacity carried by a successfully published round."""
    if (allocation.observed_free is None or
            allocation.observed_free_by_accelerator is None or
            allocation.observed_at is None):
        return
    observation = reserved_capacity_broker.PoolObservation(
        free_slots=allocation.observed_free,
        gpu_names=tuple(allocation.observed_free_by_accelerator),
        free_slots_by_accelerator=tuple(
            allocation.observed_free_by_accelerator.items()))
    _record_pool_observation(placer, locations, observation,
                             allocation.observed_at)


def _fresh_round_observation(
    round_row: dict[str, Any] | None,
    now: float,
) -> tuple[int, float] | None:
    """Return a fresh durable pool observation, if one exists.

    The broker's round row is already authoritative enough for
    `_pool_capacity_hint()`. Reuse the same freshness contract when a
    protocol-v2 controller rebuilds launchability from durable state after a
    local placer bench or controller restart.
    """
    if round_row is None or round_row.get('last_observed_free') is None:
        return None
    observed_at = round_row.get('last_observed_free_ts')
    if (not isinstance(observed_at, (int, float)) or
            isinstance(observed_at, bool)):
        return None
    observed_at = float(observed_at)
    if (now - observed_at > poll_interval_seconds() *
            constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS):
        return None
    try:
        free_slots = max(0, int(round_row['last_observed_free']))
    except (TypeError, ValueError):
        return None
    return free_slots, observed_at


def _seed_pool_launchability_from_round(
    placer: 'spot_placer_lib.SpotPlacer',
    locations: Sequence['spot_placer_lib.Location'],
    round_row: dict[str, Any] | None,
    now: float,
) -> None:
    """Apply a fresh committed round to the local placer before v2 claiming.

    Protocol-v2 computes per-pool launchability and capacity hints before it
    drives or reads the current round. If the local placer is still benched
    but the durable broker row already measured free capacity, prime the
    placer from that committed observation first so the claim heartbeat and
    its budget partition do not ignore fresh broker state for one cycle.
    """
    observed = _fresh_round_observation(round_row, now)
    if observed is None:
        return
    free_slots, observed_at = observed
    _record_pool_observation(
        placer, locations,
        reserved_capacity_broker.PoolObservation(
            free_slots=free_slots, gpu_names=(),
            free_slots_by_accelerator=None), observed_at)


def _broker_cycle(
    autoscaler: 'autoscalers.Autoscaler',
    placer: 'spot_placer_lib.SpotPlacer',
    service_name: str,
    zero_cost: list['spot_placer_lib.Location'],
    keys: list[dict[str, Any]],
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> None:
    """Broker-arbitrated cycle: claim heartbeat -> round -> feed+grant."""
    fence_kwargs: dict[str, Any] = {}
    if expected_service_hash is not None:
        fence_kwargs['expected_service_hash'] = expected_service_hash
    if expected_controller_owner is not None:
        fence_kwargs['expected_controller_owner'] = expected_controller_owner
    shapes = zero_cost_pool_shapes(zero_cost)
    contexts = {context for context, _ in shapes}
    per_replica_counts = set(shapes.values())
    logical_slot_mismatch = (
        placer.placement_contract.requires_single_gpu_reserved_fill and
        per_replica_counts != {1})
    if (len(contexts) != 1 or len(per_replica_counts) != 1 or
            logical_slot_mismatch):
        logger.error(
            'Reserved-fill broker: zero-cost shapes must share one context '
            'and GPU count per backend, and logical services require exact '
            f'one-GPU shapes; got {sorted(shapes.items())}. Fill is inactive '
            'for this service.')
        reserved_capacity_broker.remove_claim(service_name, **fence_kwargs)
        autoscaler.collect_reserved_capacity(0, keys, time.time())
        return
    context = next(iter(contexts))
    per_replica = next(iter(per_replica_counts))
    grouped_shapes = {
        gpu_name: count
        for (shape_context, gpu_name), count in shapes.items()
        if shape_context == context
    }
    pool_key = reserved_capacity_broker.make_pool_key(context,
                                                      tuple(grouped_shapes))
    replica_infos = serve_state.get_replica_infos(service_name)
    # Seed before counting (idempotent no-op when already seeded): after a
    # respawn whose best-effort boot seed failed, an unseeded autoscaler
    # counts zero holdings, and that under-report reaches the broker as a
    # holdings SHRINK -- bypassing the two-round down-damping and cutting
    # peers' grants on a pure reporting artifact.
    autoscaler.seed_zero_cost_locations(keys)
    # Only the FILL count reaches the claim: demand-placed rows are exempt
    # from the ceiling and the broker never reads them.
    holdings_fill, _ = autoscaler.count_zero_cost_holdings(replica_infos)
    floor = autoscaler.reserved_fill_floor_replicas
    # Real capacity cap this claimant can materialize right now: fill rides
    # ABOVE the demand target, so anything past max_replicas - demand_target
    # is phantom capacity. The broker clamps the effective floor, the
    # headroom (share above the floor, derived at allocation time) and the
    # feed need by it -- otherwise an unattainable floor permanently
    # absorbs entitlement and feed the service never launches.
    effective_cap = max(
        0, autoscaler.max_replicas - autoscaler.get_final_target_num_replicas())
    # Utilization signal for the release governor. Sampled HERE rather than
    # inside the decision tick's request-information path, which early
    # returns without a report, does not run when the controller is not
    # demand-authoritative, and stamps the freshness timestamp itself (so a
    # freshness check evaluated beside it would be vacuously true). This
    # cycle runs unconditionally every poll interval and holds a live
    # reference to the very autoscaler the decision tick uses.
    activity: dict[str, Any] | None = None
    if autoscaler.reserved_fill_utilization_gate:
        sample = autoscaler.fill_demand_sample(replica_infos)
        # A current gated writer publishes activity_ts every round. A missing
        # detailed sample carries NULL need so the broker can distinguish
        # armed-but-blind (freeze, then bounded blind-grace decay) from the
        # all-NULL explicit utilization_gate:false opt-out.
        activity = {
            'demonstrated_need':
                (None if sample is None else sample.demonstrated_need()),
            'boot_hold': False if sample is None else sample.boot_hold(),
        }
    claim_persisted = reserved_capacity_broker.upsert_claim(
        service_name,
        pool_key=pool_key,
        weight=autoscaler.reserved_fill_weight,
        floor_replicas=floor,
        gpus_per_replica=per_replica,
        holdings_fill=holdings_fill,
        effective_cap=effective_cap,
        launchable=_placer_can_launch_zero_cost(placer),
        activity=activity,
        **fence_kwargs)
    if claim_persisted is False:
        autoscaler.collect_reserved_capacity(0, keys, time.time())
        logger.info('Reserved-fill broker: claim rejected or controller '
                    f'stale for {service_name!r}; feeding 0 slots.')
        return

    # Admission is deliberately outside the broker. The round callback may
    # issue provider work while holding its cross-controller lock.
    with provider_phase.provider_phase(
            provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
        allocation = reserved_capacity_broker.run_round_if_stale(
            service_name,
            pool_key,
            lambda: query_pool_group_observation(context, grouped_shapes),
            poll_interval_seconds(),
            lock_timeout_seconds=0)
    if allocation is None:
        # No allocation this cycle (claim rejected/expired, round lock
        # timeout, or the fresh round predates our claim): feed zero free
        # slots. Existing holdings stay sheltered via zero_cost_count; no
        # new fill until the broker admits us.
        autoscaler.collect_reserved_capacity(0, keys, time.time())
        logger.info('Reserved-fill broker: no allocation for '
                    f'{service_name!r} this cycle; feeding 0 free slots.')
        return
    _record_allocation_observation(placer, zero_cost, allocation)
    autoscaler.collect_reserved_capacity(allocation.feed,
                                         keys,
                                         allocation.snapshot_time,
                                         grant=allocation.grant,
                                         grant_epoch=allocation.epoch,
                                         grant_pool_key=pool_key)
    logger.info(f'Reserved-fill broker: {service_name!r} feed='
                f'{allocation.feed} grant={allocation.grant} '
                f'(round {allocation.round_id}, epoch {allocation.epoch}).')


def _pool_round_sum_holdings(round_row: dict[str, Any] | None) -> int:
    """Fill slots every claimant of this pool holds, per the last round.

    Absent on rows written before the broker published it, and on a pool that
    has not completed a round yet. Zero is the safe read: the caller only ever
    takes it as a lower bound on the pool's size.
    """
    if round_row is None:
        return 0
    raw = round_row.get('sum_holdings')
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        return 0
    return raw


def _pool_capacity_hint(spec: FillPoolSpec,
                        holdings: int,
                        launchable: bool,
                        previous_cap: int,
                        now: float,
                        round_row: dict[str, Any] | None = None) -> int:
    """Return the bounded discovery/blackout hint for one pool edge."""
    if not launchable:
        return holdings
    if round_row is None:
        round_row = serve_state.get_reserved_fill_round(spec.pool_key)
    if round_row is None or round_row.get('last_observed_free') is None:
        return holdings + 1
    observed = _fresh_round_observation(round_row, now)
    if observed is not None:
        # Size the hint to the WHOLE pool, not to this claimant's own corner
        # of it. Slots held by peers are reclaimable: that is precisely what
        # the weighted arbitration in compute_entitlements exists to do, and
        # its `total` is already "observed free + Sum of fill holdings".
        #
        # Reporting `holdings + free` instead made a full pool self-locking.
        # With free at zero every claimant's cap collapsed to exactly what it
        # already held, so the allocator could not move a single slot and
        # weights and floors both stopped applying. Measured in production:
        # a weight-0.1 claimant sat on 63 of 65 A100s while a weight-100 peer
        # with floor_replicas=10 was pinned at 2, unchanged across seven
        # consecutive rounds, until the incumbent's Pods were deleted by hand.
        #
        # max() against the local count keeps this monotonic. sum_holdings is
        # one round old and this claimant's own holdings may already have
        # grown past it, so the hint can only widen, never narrow: no claimant
        # loses ground to a stale total.
        #
        # This authorizes reclaim, it does not launch anything. Feeds stay
        # bounded by observed free, and the service ceiling still binds --
        # allocate_fill_pool_budgets never hands out more than global_budget.
        return max(holdings, _pool_round_sum_holdings(round_row)) + observed[0]
    return max(holdings, previous_cap)


def _broker_cycle_v2(
    autoscaler: 'autoscalers.Autoscaler',
    placer: 'spot_placer_lib.SpotPlacer',
    service_name: str,
    zero_cost: list['spot_placer_lib.Location'],
    expected_service_hash: str | None,
    expected_controller_owner: tuple[int | None, str | None] | None,
) -> None:
    """Publish and consume one atomic protocol-v2 multi-pool heartbeat."""
    try:
        specs = discover_fill_pool_specs(zero_cost)
    except ValueError as error:
        logger.error('Reserved-fill protocol-v2 pool discovery rejected the '
                     f'service configuration for {service_name!r}: {error}')
        reserved_capacity_broker.remove_claim(
            service_name,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
        autoscaler.collect_reserved_capacity_pools({})
        return
    logical_slot_mismatch = (
        placer.placement_contract.requires_single_gpu_reserved_fill and
        any(spec.gpus_per_replica != 1 for spec in specs))
    if not specs or logical_slot_mismatch:
        logger.error('Reserved-fill protocol v2 found no valid physical pool '
                     'set (logical services also require one-GPU shapes); '
                     f'fill is inactive for {service_name!r}.')
        reserved_capacity_broker.remove_claim(
            service_name,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
        autoscaler.collect_reserved_capacity_pools({})
        return

    location_keys = {
        spec.pool_key: [
            location.to_pickleable() for location in spec.locations
        ] for spec in specs
    }
    autoscaler.seed_zero_cost_pools(location_keys)
    previous_set = serve_state.get_reserved_fill_service_claim_set(service_name)
    if previous_set is None or not previous_set.get('integrity_valid'):
        previous_set = None
    previous_edges = ({
        str(edge['pool_key']): edge for edge in previous_set.get('edges', [])
    } if previous_set is not None else {})
    previous_generation = (int(previous_set['generation'])
                           if previous_set is not None else 0)
    pool_authority = {
        pool_key: (str(edge['physical_cluster_uid']), previous_generation)
        for pool_key, edge in previous_edges.items()
        if isinstance(edge.get('physical_cluster_uid'), str) and
        edge['physical_cluster_uid']
    }
    replica_infos = serve_state.get_replica_infos(service_name)
    holdings_by_pool = autoscaler.count_zero_cost_holdings_by_pool(
        replica_infos, location_keys, pool_authority)

    now = time.time()
    global_headroom = max(
        0, autoscaler.max_replicas - autoscaler.get_final_target_num_replicas())
    total_fill_holdings = sum(
        holdings_by_pool.get(spec.pool_key, (0, 0))[0] for spec in specs)
    if (autoscaler.reserved_fill_utilization_gate and
            reserved_capacity_broker.utilization_gate_enabled()):
        sample = autoscaler.fill_demand_sample(replica_infos)
        prior_state = (previous_set.get('utilization_state')
                       if previous_set is not None else None)
        utilization_state = reserved_capacity_broker.advance_release_target(
            prior_state if isinstance(prior_state, dict) else None,
            floor=0,
            holdings=total_fill_holdings,
            need=0 if sample is None else sample.demonstrated_need(),
            boot_hold=False if sample is None else sample.boot_hold(),
            blind=sample is None,
            now=now,
            dwell=constants.RESERVED_FILL_IDLE_DWELL_SECONDS,
            step_seconds=constants.RESERVED_FILL_RELEASE_STEP_SECONDS,
            step_fraction=constants.RESERVED_FILL_RELEASE_STEP_FRACTION,
            min_step=constants.RESERVED_FILL_RELEASE_MIN_STEP,
            headroom=constants.RESERVED_FILL_UTILIZATION_HEADROOM,
            blind_grace=constants.RESERVED_FILL_BLIND_GRACE_SECONDS)
        utilization_ceiling = min(global_headroom,
                                  max(0, int(utilization_state['cap'])))
    else:
        utilization_state = None
        utilization_ceiling = global_headroom
    global_budget = min(global_headroom, utilization_ceiling)

    round_rows = {
        spec.pool_key: serve_state.get_reserved_fill_round(spec.pool_key)
        for spec in specs
    }
    for spec in specs:
        _seed_pool_launchability_from_round(placer, spec.locations,
                                            round_rows[spec.pool_key], now)
    active_locations = placer.active_locations()
    launchable: dict[str, bool] = {
        spec.pool_key: any(
            any(
                spot_placer_lib.locations_match_placement(location, active)
                for active in active_locations)
            for location in spec.locations) for spec in specs
    }
    budget_inputs: list[FillPoolBudgetInput] = []
    for spec in specs:
        holdings_fill = holdings_by_pool.get(spec.pool_key, (0, 0))[0]
        previous_cap = max(
            0,
            int(
                previous_edges.get(spec.pool_key, {}).get('effective_cap') or
                0))
        budget_inputs.append(
            FillPoolBudgetInput(holdings=holdings_fill,
                                capacity_hint=_pool_capacity_hint(
                                    spec,
                                    holdings_fill,
                                    launchable[spec.pool_key],
                                    previous_cap,
                                    now,
                                    round_row=round_rows[spec.pool_key])))
    budgets = allocate_fill_pool_budgets(
        global_budget, autoscaler.reserved_fill_floor_replicas,
        tuple(budget_inputs))

    edges: list[dict[str, Any]] = []
    semantic_edges: list[dict[str, Any]] = []
    for spec, budget in zip(specs, budgets):
        holdings_fill = holdings_by_pool.get(spec.pool_key, (0, 0))[0]
        edge = {
            'pool_key': spec.pool_key,
            'legacy_pool_key': spec.legacy_pool_key,
            'pool_position': spec.position,
            'access_context': spec.context,
            'physical_cluster_uid': spec.physical_cluster_uid,
            'accelerator_names': list(spec.accelerator_names),
            'weight': autoscaler.reserved_fill_weight,
            'floor_replicas': budget.edge_floor,
            'gpus_per_replica': spec.gpus_per_replica,
            'holdings_fill': holdings_fill,
            'effective_cap': budget.edge_cap,
            'launchable': launchable[spec.pool_key],
        }
        edges.append(edge)
        semantic_edges.append({
            key: value
            for key, value in edge.items()
            if key not in ('holdings_fill', 'launchable')
        })
    semantic_payload = {
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'global_headroom': global_headroom,
        'utilization_ceiling': utilization_ceiling,
        'utilization_gate': autoscaler.reserved_fill_utilization_gate,
        'service_floor': autoscaler.reserved_fill_floor_replicas,
        'service_weight': autoscaler.reserved_fill_weight,
        'edges': semantic_edges,
    }
    semantic_hash = hashlib.sha256(
        json.dumps(semantic_payload, sort_keys=True,
                   separators=(',', ':')).encode('utf-8')).hexdigest()
    generation = reserved_capacity_broker.replace_claim_set(
        service_name,
        semantic_hash=semantic_hash,
        global_headroom=global_headroom,
        utilization_ceiling=utilization_ceiling,
        utilization_state=utilization_state,
        edges=edges,
        expected_service_hash=expected_service_hash,
        expected_controller_owner=expected_controller_owner)
    if generation is None:
        autoscaler.collect_reserved_capacity_pools({})
        logger.info('Reserved-fill broker: complete claim-set heartbeat was '
                    f'rejected for {service_name!r}; feeding every pool 0.')
        return

    snapshots: dict[str, dict[str, Any]] = {}
    for spec, budget in zip(specs, budgets):

        def _query_pool(
            spec: FillPoolSpec = spec
        ) -> 'reserved_capacity_broker.PoolObservation':
            return query_pool_group_observation(
                spec.context,
                dict(spec.shapes),
                expected_physical_cluster_uid=spec.physical_cluster_uid)

        try:
            # The phase precedes the broker lock and remains active through the
            # callback's exact physical proof and allocation materialization.
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED):
                allocation = reserved_capacity_broker.run_round_if_stale(
                    service_name,
                    spec.pool_key,
                    _query_pool,
                    poll_interval_seconds(),
                    expected_protocol_version=(
                        reserved_capacity_broker.PROTOCOL_V2),
                    expected_service_generation=generation,
                    lock_timeout_seconds=0)
        except Exception as error:  # pylint: disable=broad-except
            # One pool's transient database/lock path must not suppress a
            # healthy peer edge in this same complete-map publication.
            logger.warning('Reserved-fill broker round failed for '
                           f'{service_name!r}/{spec.pool_key}: '
                           f'{common_utils.format_exception(error)}')
            allocation = None
        if allocation is not None:
            # Every controller consumes the committed round, including peers
            # whose fresh-round path deliberately skipped `_query_pool`.
            _record_allocation_observation(placer, spec.locations, allocation)
        # A lock timeout or other transient round miss must not cull existing
        # fill from this one pool. Carry only the last real grant from the
        # same physical pool as scale-down shelter, including across a forward
        # service-generation transition. Live launch authority still fails
        # closed: feed and grant are zero and no epoch is replayed. The pool
        # key/UID lookup prevents a replacement physical cluster from
        # inheriting the shelter, and a removed edge is absent from the
        # autoscaler's atomically replaced pool map.
        shelter_grant = (autoscaler.get_reserved_capacity_pool_shelter_grant(
            spec.pool_key,
            service_generation=generation,
            physical_cluster_uid=spec.physical_cluster_uid,
            edge_cap=budget.edge_cap)
                         if allocation is None else allocation.grant)
        snapshots[spec.pool_key] = {
            'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
            'pool_key': spec.pool_key,
            'physical_cluster_uid': spec.physical_cluster_uid,
            'service_generation': generation,
            'edge_cap': budget.edge_cap,
            'zero_cost_location_keys': location_keys[spec.pool_key],
            'free_slots': 0 if allocation is None else allocation.feed,
            'free_slots_by_accelerator':
                (None if allocation is None else allocation.feed_by_accelerator
                ),
            'grant': 0 if allocation is None else allocation.grant,
            'shelter_grant': shelter_grant,
            'grant_epoch': None if allocation is None else allocation.epoch,
            'timestamp': now
                         if allocation is None else allocation.snapshot_time,
        }
    autoscaler.collect_reserved_capacity_pools(snapshots)
    logger.info('Reserved-fill broker: published service generation '
                f'{generation} with {len(snapshots)} physical pool(s) for '
                f'{service_name!r}.')


def poller_loop(
    get_autoscaler: Callable[[], 'autoscalers.Autoscaler'],
    get_spot_placer: Callable[[], Optional['spot_placer_lib.SpotPlacer']],
    service_name: str | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    stop_event: threading.Event | None = None,
    actuation_epoch_lock: contextlib.AbstractContextManager[Any] | None = None,
) -> None:
    """Poll free zero-cost capacity forever, feeding the autoscaler.

    Runs as a supervised thread started by the controller (only when the
    service opted in AND a spot placer exists -- the placer defines the
    zero-cost location set). Takes getters, not the live objects: an
    update_service can replace the controller's autoscaler, and the
    snapshot must reach the current one.

    service_name enables broker arbitration (the controller always passes
    it); None preserves the standalone pre-broker cycle for direct callers
    and tests.
    """
    # Whether a broker claim of ours may exist. Starts True: a previous
    # incarnation of this controller may have left one behind (respawn),
    # so the first disabled observation still clears it. Reset to True
    # BEFORE every broker cycle (which upserts the claim).
    claim_may_exist = service_name is not None
    fence_kwargs: dict[str, Any] = {}
    if expected_service_hash is not None:
        fence_kwargs['expected_service_hash'] = expected_service_hash
    if expected_controller_owner is not None:
        fence_kwargs['expected_controller_owner'] = expected_controller_owner
    while stop_event is None or not stop_event.is_set():
        epoch_context = (actuation_epoch_lock if actuation_epoch_lock
                         is not None else contextlib.nullcontext())
        with epoch_context:
            # An update may have set the irreversible fence while this poller
            # waited behind an autoscaler/update epoch. Never begin provider or
            # durable broker work after that transition.
            if stop_event is not None and stop_event.is_set():
                return
            try:
                if (service_name is not None and
                        expected_service_hash is not None):
                    owner = serve_state.get_service_controller_owner(
                        service_name)
                    current_owner = ((owner.get('controller_pid'),
                                      owner.get('controller_ip'))
                                     if owner else None)
                    if (owner is None or
                            owner.get('hash') != expected_service_hash or
                        (expected_controller_owner is not None and
                         current_owner != expected_controller_owner)):
                        logger.info(
                            f'Reserved-capacity poller for stale service owner '
                            f'{service_name!r}/{expected_service_hash!r}/'
                            f'{expected_controller_owner!r} is exiting.')
                        return
                placer = get_spot_placer()
                # An update can turn the flag off on the live autoscaler; the
                # thread stays alive (a later update can re-enable it) but
                # must not keep issuing the expensive cluster-wide pod-listing
                # query for a snapshot nobody consumes.
                autoscaler = get_autoscaler()
                fill_enabled = autoscaler.reserved_capacity_fill
                if placer is not None and fill_enabled:
                    zero_cost = placer.zero_cost_locations()
                    keys: list[dict[str, Any]] = [
                        location.to_pickleable() for location in zero_cost
                    ]
                    if service_name is None:
                        _standalone_cycle(autoscaler, zero_cost, keys)
                    else:
                        # Set BEFORE the cycle: it upserts the claim partway
                        # through, and an exception after that upsert (e.g.
                        # the round query) must still leave the flag true --
                        # otherwise a subsequent disable would skip
                        # remove_claim and leave a ghost claim absorbing
                        # entitlement for the whole claim TTL.
                        claim_may_exist = True
                        protocol_version = (
                            reserved_capacity_broker.get_protocol_version())
                        if (protocol_version ==
                                reserved_capacity_broker.PROTOCOL_V2):
                            _broker_cycle_v2(autoscaler, placer, service_name,
                                             zero_cost, expected_service_hash,
                                             expected_controller_owner)
                        else:
                            _broker_cycle(autoscaler, placer, service_name,
                                          zero_cost, keys,
                                          expected_service_hash,
                                          expected_controller_owner)
                elif service_name is not None and claim_may_exist:
                    # Fill turned off (or the placer is gone): withdraw the
                    # claim NOW instead of leaving peers arbitrating around a
                    # ghost for the whole claim TTL. Once per disable
                    # transition (idempotent; also drops our cached
                    # allocation), not re-spammed every cycle.
                    try:
                        reserved_capacity_broker.remove_claim(
                            service_name, **fence_kwargs)
                    finally:
                        # Claim removal is a hard lifecycle boundary for local
                        # shelter too. If the durable write fails, the next
                        # poll retries it, but a later re-enable must never
                        # inherit shelter from the deliberately withdrawn edge.
                        autoscaler.collect_reserved_capacity_pools({})
                    claim_may_exist = False
            except Exception as e:  # pylint: disable=broad-except
                logger.error('Error in reserved-capacity poller: '
                             f'{common_utils.format_exception(e)}')
        interval = poll_interval_seconds()
        if stop_event is None:
            time.sleep(interval)
        elif stop_event.wait(interval):
            return
