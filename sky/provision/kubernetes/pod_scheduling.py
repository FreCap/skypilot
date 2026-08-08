"""Kubernetes pod scheduling and capacity diagnostics."""

import collections
import dataclasses
import datetime
import hashlib
import json
import math
import os
import sys
import tempfile
import threading
import time
from typing import Any

import filelock

from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import kubernetes
from sky.provision import constants
from sky.provision.kubernetes import config as config_lib
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.utils import common_utils
from sky.utils import kubernetes_enums
from sky.utils import rich_utils
from sky.utils import timeline
from sky.utils import ux_utils

# Once a definitive TriggeredScaleUp event is observed, extend the pod
# scheduling deadline from the detection moment. Only TriggeredScaleUp is used
# because the FailedScheduling heuristic can also indicate genuine resource
# mismatches, taints, or PVC issues.
_AUTOSCALE_DETECTED_TIMEOUT_SECONDS = 900  # 15 minutes
# Give an enabled Cluster Autoscaler enough time to scan and emit its first
# event even when the user leaves provision_timeout at its short default.
_AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS = 60

# Karpenter reports deterministic NodePool incompatibilities through
# FailedScheduling Events. Cache the normalized signals per Kubernetes
# context/namespace so concurrent launches share one Events API read.
_FAILED_SCHEDULING_EVENT_CACHE_TTL_SECONDS = 2
_FAILED_SCHEDULING_EVENT_CACHE_MAX_ENTRIES = 64
_FAILED_SCHEDULING_EVENT_CACHE_MAX_UID_MATCHES = 1024
_FAILED_SCHEDULING_EVENT_SHARED_CACHE_VERSION = 1
_FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKETS = 64
_FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKET_MAX_ENTRIES = 8
_FAILED_SCHEDULING_EVENT_SHARED_CACHE_DIR = os.path.join(
    tempfile.gettempdir(), 'skypilot-failed-scheduling-event-cache')


@dataclasses.dataclass
class _FailedSchedulingEventSnapshot:
    latest_occurrence: datetime.datetime | None = None
    gpu_incompatibilities: dict[str, tuple[datetime.datetime,
                                           str]] = dataclasses.field(
                                               default_factory=dict)


@dataclasses.dataclass
class _FailedSchedulingEventCacheEntry:
    refresh_lock: Any = dataclasses.field(default_factory=threading.Lock)
    cached_at: float | None = None
    snapshot: _FailedSchedulingEventSnapshot = dataclasses.field(
        default_factory=_FailedSchedulingEventSnapshot)
    pins: int = 0


_FAILED_SCHEDULING_EVENT_CACHE_LOCK = threading.Lock()
_FAILED_SCHEDULING_EVENT_CACHE: collections.OrderedDict[
    tuple[str | None,
          str], _FailedSchedulingEventCacheEntry] = (collections.OrderedDict())

# Use the historical logger so facade imports and logging behavior stay stable.
logger = sky_logging.init_logger('sky.provision.kubernetes.instance')


def _clear_failed_scheduling_event_cache_for_testing() -> None:
    with _FAILED_SCHEDULING_EVENT_CACHE_LOCK:
        _FAILED_SCHEDULING_EVENT_CACHE.clear()


def _pin_failed_scheduling_event_cache_entry(
        context: str | None,
        namespace: str) -> _FailedSchedulingEventCacheEntry | None:
    key = (context, namespace)
    with _FAILED_SCHEDULING_EVENT_CACHE_LOCK:
        entry = _FAILED_SCHEDULING_EVENT_CACHE.get(key)
        if entry is not None:
            entry.pins += 1
            _FAILED_SCHEDULING_EVENT_CACHE.move_to_end(key)
            return entry

        if (len(_FAILED_SCHEDULING_EVENT_CACHE)
                >= _FAILED_SCHEDULING_EVENT_CACHE_MAX_ENTRIES):
            eviction_key = next(
                (candidate_key for candidate_key, candidate_entry in
                 _FAILED_SCHEDULING_EVENT_CACHE.items()
                 if candidate_entry.pins == 0), None)
            if eviction_key is None:
                return None
            del _FAILED_SCHEDULING_EVENT_CACHE[eviction_key]

        entry = _FailedSchedulingEventCacheEntry(pins=1)
        _FAILED_SCHEDULING_EVENT_CACHE[key] = entry
        return entry


def _release_failed_scheduling_event_cache_entry(
        entry: _FailedSchedulingEventCacheEntry) -> None:
    with _FAILED_SCHEDULING_EVENT_CACHE_LOCK:
        entry.pins -= 1


def _as_aware_utc(timestamp: Any) -> datetime.datetime | None:
    if not isinstance(timestamp, datetime.datetime):
        return None
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=datetime.timezone.utc)
    return timestamp.astimezone(datetime.timezone.utc)


def _failed_scheduling_event_occurrence(event: Any) -> datetime.datetime | None:
    series = getattr(event, 'series', None)
    timestamps = [
        getattr(series, 'last_observed_time', None) if series else None,
        getattr(event, 'event_time', None),
        getattr(event, 'last_timestamp', None),
        getattr(getattr(event, 'metadata', None), 'creation_timestamp', None),
    ]
    for timestamp in timestamps:
        normalized = _as_aware_utc(timestamp)
        if normalized is not None:
            return normalized
    return None


def _karpenter_gpu_incompatibility(
        event: Any) -> tuple[str, datetime.datetime, str] | None:
    if (getattr(event, 'reason', None) != 'FailedScheduling' or
            getattr(event, 'type', None) != 'Warning'):
        return None

    reporting_component = getattr(event, 'reporting_component', None)
    source = getattr(event, 'source', None)
    source_component = getattr(source, 'component', None) if source else None
    components = (reporting_component, source_component)
    if not any(
            isinstance(component, str) and component.lower() == 'karpenter'
            for component in components):
        return None

    message = getattr(event, 'message', None)
    if not isinstance(message, str) or ';' in message:
        return None
    lower_message = message.lower()
    required_fragments = (
        'incompatible requirements',
        'nvidia.com/gpu.product',
        'does not have known values',
    )
    if not all(fragment in lower_message for fragment in required_fragments):
        return None

    involved_object = getattr(event, 'involved_object', None)
    pod_uid = getattr(involved_object, 'uid', None)
    occurrence = _failed_scheduling_event_occurrence(event)
    if not isinstance(pod_uid, str) or occurrence is None:
        return None
    return pod_uid, occurrence, message


def _refresh_failed_scheduling_event_snapshot(
        namespace: str, context: str | None) -> _FailedSchedulingEventSnapshot:
    try:
        events = kubernetes.core_api(context).list_namespaced_event(
            namespace=namespace,
            field_selector='reason=FailedScheduling',
            _request_timeout=kubernetes.API_TIMEOUT)
        matches: dict[str, tuple[datetime.datetime, str]] = {}
        latest_occurrence = None
        for event in events.items:
            occurrence = _failed_scheduling_event_occurrence(event)
            if (occurrence is not None and
                (latest_occurrence is None or occurrence > latest_occurrence)):
                latest_occurrence = occurrence
            match = _karpenter_gpu_incompatibility(event)
            if match is None:
                continue
            pod_uid, occurrence, message = match
            previous = matches.get(pod_uid)
            if previous is None or occurrence > previous[0]:
                matches[pod_uid] = (occurrence, message)
        newest_matches = sorted(matches.items(),
                                key=lambda item: item[1][0],
                                reverse=True)
        return _FailedSchedulingEventSnapshot(
            latest_occurrence=latest_occurrence,
            gpu_incompatibilities=dict(
                newest_matches[:_FAILED_SCHEDULING_EVENT_CACHE_MAX_UID_MATCHES])
        )
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(
            f'Failed to inspect Karpenter FailedScheduling events: {e}')
        return _FailedSchedulingEventSnapshot()


def _failed_scheduling_event_shared_cache_identity(context: str | None,
                                                   namespace: str) -> str:
    return json.dumps([context, namespace],
                      ensure_ascii=False,
                      separators=(',', ':'))


def _failed_scheduling_event_shared_cache_paths(
        context: str | None, namespace: str) -> tuple[str, str, str]:
    identity = _failed_scheduling_event_shared_cache_identity(
        context, namespace)
    digest = hashlib.sha256(identity.encode('utf-8')).digest()
    bucket = int.from_bytes(
        digest[:8], 'big') % (_FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKETS)
    prefix = os.path.join(_FAILED_SCHEDULING_EVENT_SHARED_CACHE_DIR,
                          f'bucket-{bucket}')
    return f'{prefix}.json', f'{prefix}.lock', f'{prefix}.json.tmp'


def _serialize_failed_scheduling_event_snapshot(
        snapshot: _FailedSchedulingEventSnapshot) -> dict[str, Any]:
    latest_occurrence = (snapshot.latest_occurrence.isoformat()
                         if snapshot.latest_occurrence is not None else None)
    matches = [[uid, occurrence.isoformat(), message]
               for uid, (occurrence,
                         message) in snapshot.gpu_incompatibilities.items()]
    return {
        'latest_occurrence': latest_occurrence,
        'gpu_incompatibilities': matches,
    }


def _deserialize_failed_scheduling_event_snapshot(
        value: Any) -> _FailedSchedulingEventSnapshot | None:
    if not isinstance(value, dict):
        return None
    latest_occurrence_value = value.get('latest_occurrence')
    if latest_occurrence_value is None:
        latest_occurrence = None
    elif isinstance(latest_occurrence_value, str):
        try:
            latest_occurrence = _as_aware_utc(
                datetime.datetime.fromisoformat(latest_occurrence_value))
        except ValueError:
            return None
        if latest_occurrence is None:
            return None
    else:
        return None

    match_values = value.get('gpu_incompatibilities')
    if (not isinstance(match_values, list) or
            len(match_values) > _FAILED_SCHEDULING_EVENT_CACHE_MAX_UID_MATCHES):
        return None
    matches: dict[str, tuple[datetime.datetime, str]] = {}
    for match_value in match_values:
        if (not isinstance(match_value, list) or len(match_value) != 3 or
                not isinstance(match_value[0], str) or
                not isinstance(match_value[1], str) or
                not isinstance(match_value[2], str)):
            return None
        try:
            occurrence = _as_aware_utc(
                datetime.datetime.fromisoformat(match_value[1]))
        except ValueError:
            return None
        if occurrence is None or match_value[0] in matches:
            return None
        matches[match_value[0]] = (occurrence, match_value[2])
    if set(value) != {'latest_occurrence', 'gpu_incompatibilities'}:
        return None
    return _FailedSchedulingEventSnapshot(latest_occurrence=latest_occurrence,
                                          gpu_incompatibilities=matches)


def _read_failed_scheduling_event_shared_bucket(
        bucket_path: str, now: float) -> list[dict[str, Any]] | None:
    try:
        with open(bucket_path, encoding='utf-8') as bucket_file:
            value = json.load(bucket_file)
    except FileNotFoundError:
        return []
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if (not isinstance(value, dict) or value.get('version')
            != _FAILED_SCHEDULING_EVENT_SHARED_CACHE_VERSION or
            set(value) != {'version', 'entries'}):
        return None
    entries = value.get('entries')
    if (not isinstance(entries, list) or len(entries)
            > _FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKET_MAX_ENTRIES):
        return None
    identities = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
                'identity', 'refreshed_at', 'last_accessed_at', 'snapshot'
        }:
            return None
        identity = entry['identity']
        refreshed_at = entry['refreshed_at']
        last_accessed_at = entry['last_accessed_at']
        if (not isinstance(identity, str) or identity in identities or
                not isinstance(refreshed_at, (int, float)) or
                isinstance(refreshed_at, bool) or
                not math.isfinite(refreshed_at) or refreshed_at < 0 or
                refreshed_at > now or not isinstance(last_accessed_at,
                                                     (int, float)) or
                isinstance(last_accessed_at, bool) or last_accessed_at < 0 or
                not math.isfinite(last_accessed_at) or last_accessed_at > now or
                _deserialize_failed_scheduling_event_snapshot(
                    entry['snapshot']) is None):
            return None
        identities.add(identity)
    return entries


def _write_failed_scheduling_event_shared_bucket(
        bucket_path: str, staging_path: str, entries: list[dict[str,
                                                                Any]]) -> None:
    value = {
        'version': _FAILED_SCHEDULING_EVENT_SHARED_CACHE_VERSION,
        'entries': entries,
    }
    with open(staging_path, 'w', encoding='utf-8') as staging_file:
        json.dump(value, staging_file, separators=(',', ':'), sort_keys=True)
        staging_file.flush()
        os.fsync(staging_file.fileno())
    os.replace(staging_path, bucket_path)


def _get_failed_scheduling_event_shared_snapshot(
        namespace: str,
        context: str | None) -> _FailedSchedulingEventSnapshot | None:
    identity = _failed_scheduling_event_shared_cache_identity(
        context, namespace)
    bucket_path, lock_path, staging_path = (
        _failed_scheduling_event_shared_cache_paths(context, namespace))
    try:
        os.makedirs(_FAILED_SCHEDULING_EVENT_SHARED_CACHE_DIR, exist_ok=True)
        with filelock.FileLock(lock_path, timeout=0):
            now = time.monotonic()
            entries = _read_failed_scheduling_event_shared_bucket(
                bucket_path, now)
            if entries is None:
                entries = []

            matching_index = next((index for index, entry in enumerate(entries)
                                   if entry['identity'] == identity), None)
            if matching_index is not None:
                matching_entry = entries[matching_index]
                age = now - matching_entry['refreshed_at']
                if age < _FAILED_SCHEDULING_EVENT_CACHE_TTL_SECONDS:
                    snapshot = _deserialize_failed_scheduling_event_snapshot(
                        matching_entry['snapshot'])
                    assert snapshot is not None
                    matching_entry['last_accessed_at'] = now
                    _write_failed_scheduling_event_shared_bucket(
                        bucket_path, staging_path, entries)
                    return snapshot
                replacement_index = matching_index
            elif len(entries) < (
                    _FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKET_MAX_ENTRIES):
                replacement_index = len(entries)
            else:
                expired_indexes = [
                    index for index, entry in enumerate(entries)
                    if now - entry['refreshed_at'] >=
                    _FAILED_SCHEDULING_EVENT_CACHE_TTL_SECONDS
                ]
                if not expired_indexes:
                    return None
                replacement_index = min(
                    expired_indexes,
                    key=lambda index: entries[index]['last_accessed_at'])

            snapshot = _refresh_failed_scheduling_event_snapshot(
                namespace, context)
            refreshed_at = time.monotonic()
            new_entry = {
                'identity': identity,
                'refreshed_at': refreshed_at,
                'last_accessed_at': refreshed_at,
                'snapshot':
                    _serialize_failed_scheduling_event_snapshot(snapshot),
            }
            if replacement_index == len(entries):
                entries.append(new_entry)
            else:
                entries[replacement_index] = new_entry
            _write_failed_scheduling_event_shared_bucket(
                bucket_path, staging_path, entries)
            return snapshot
    except filelock.Timeout:
        return None
    except OSError as e:
        logger.debug(f'Failed to use shared FailedScheduling Event cache: {e}')
        return None


def _get_failed_scheduling_event_snapshot(
        namespace: str, context: str | None) -> _FailedSchedulingEventSnapshot:
    entry = _pin_failed_scheduling_event_cache_entry(context, namespace)
    if entry is None:
        return _FailedSchedulingEventSnapshot()
    try:
        with entry.refresh_lock:
            now = time.monotonic()
            if (entry.cached_at is None or now - entry.cached_at
                    >= _FAILED_SCHEDULING_EVENT_CACHE_TTL_SECONDS):
                snapshot = _get_failed_scheduling_event_shared_snapshot(
                    namespace, context)
                if snapshot is None:
                    return _FailedSchedulingEventSnapshot()
                entry.snapshot = snapshot
                entry.cached_at = time.monotonic()
            return _FailedSchedulingEventSnapshot(
                latest_occurrence=entry.snapshot.latest_occurrence,
                gpu_incompatibilities=dict(
                    entry.snapshot.gpu_incompatibilities))
    finally:
        _release_failed_scheduling_event_cache_entry(entry)


def _get_failed_scheduling_event_matches(
        namespace: str,
        context: str | None) -> dict[str, tuple[datetime.datetime, str]]:
    return _get_failed_scheduling_event_snapshot(namespace,
                                                 context).gpu_incompatibilities


def _raise_for_karpenter_gpu_incompatibility(
        namespace: str, context: str | None, pending_pod_uids: set[str],
        create_pods_start: datetime.datetime) -> None:
    if not pending_pod_uids:
        return
    cutoff = _as_aware_utc(create_pods_start)
    if cutoff is None:
        return
    matches = _get_failed_scheduling_event_matches(namespace, context)
    for pod_uid in pending_pod_uids:
        match = matches.get(pod_uid)
        if match is None:
            continue
        occurrence, message = match
        if occurrence < cutoff:
            continue
        raise config_lib.KubernetesError(
            'Karpenter cannot provision a node matching the pod GPU product '
            f'requirement. Details: {message!r}',
            insufficent_resources=['GPUs'])


def _pod_is_scheduled(pod) -> bool:
    """Whether the kube-scheduler has bound this pod to a node.

    The scheduler sets ``spec.nodeName`` (and the ``PodScheduled`` status
    condition to ``True``) the moment it places a pod -- i.e. capacity has
    been found. The kubelet on the target node only later populates
    ``status.container_statuses`` / ``host_ip`` once it picks the pod up and
    starts the sandbox. That kubelet pickup can occasionally lag past
    ``provision_timeout`` when the control plane is slow to propagate the
    binding to the kubelet, even though the pod is already bound to a node.

    We treat a bound pod as scheduled so that provisioning hands off to
    ``_wait_for_pods_to_run`` (which waits for containers without the short
    ``provision_timeout``) instead of failing over as if the cluster were out
    of resources. A genuinely unschedulable pod keeps ``PodScheduled`` False
    and no ``nodeName``, so it stays in the scheduling wait loop.
    """
    # Running/Succeeded/Failed pods are clearly past scheduling; Failed pods
    # are surfaced as errors later in _wait_for_pods_to_run.
    if pod.status.phase != 'Pending':
        return True
    # spec.nodeName is set atomically when the scheduler binds the pod.
    if pod.spec.node_name:
        return True
    # Fall back to the PodScheduled status condition.
    for condition in (pod.status.conditions or []):
        if condition.type == 'PodScheduled' and condition.status == 'True':
            return True
    return False


def _formatted_resource_requirements(pod_or_spec: Any | dict) -> str:
    # Returns a formatted string of resource requirements for a pod.
    resource_requirements = {}

    if isinstance(pod_or_spec, dict):
        containers = pod_or_spec.get('spec', {}).get('containers', [])
    else:
        containers = pod_or_spec.spec.containers

    for container in containers:
        if isinstance(container, dict):
            resources = container.get('resources', {})
            requests = resources.get('requests', {})
        else:
            resources = container.resources
            requests = resources.requests or {}

        for resource, value in requests.items():
            if resource not in resource_requirements:
                resource_requirements[resource] = 0
            if resource == 'memory':
                int_value = kubernetes_utils.parse_memory_resource(value)
            else:
                int_value = kubernetes_utils.parse_cpu_or_gpu_resource(value)
            resource_requirements[resource] += int(int_value)
    return ', '.join(f'{resource}={value}'
                     for resource, value in resource_requirements.items())


def _formatted_node_selector(pod_or_spec: Any | dict) -> str | None:
    # Returns a formatted string of node selectors for a pod.
    node_selectors = []

    if isinstance(pod_or_spec, dict):
        selectors = pod_or_spec.get('spec', {}).get('nodeSelector', {})
    else:
        selectors = pod_or_spec.spec.node_selector

    if not selectors:
        return None

    for label_key, label_value in selectors.items():
        node_selectors.append(f'{label_key}={label_value}')
    return ', '.join(node_selectors)


def _lack_resource_msg(resource: str,
                       pod_or_spec: Any | dict,
                       extra_msg: str | None = None,
                       details: str | None = None) -> str:
    resource_requirements = _formatted_resource_requirements(pod_or_spec)
    node_selectors = _formatted_node_selector(pod_or_spec)
    node_selector_str = f' and labels ({node_selectors})' if (
        node_selectors) else ''
    msg = (f'Insufficient {resource} capacity on the cluster. '
           f'Required resources ({resource_requirements}){node_selector_str} '
           'were not found in a single node. Other SkyPilot tasks or pods may '
           'be using resources. Check resource usage by running '
           '`kubectl describe nodes`.')
    if extra_msg:
        msg += f' {extra_msg}'
    if details:
        msg += f'\nFull error: {details}'
    return msg


def _format_pvc_binding_error(pvc_details: str | None, pvc_names: list[str],
                              namespace: str) -> str:
    """Format a PVC binding error message.

    Args:
        pvc_details: Optional details about the PVC issue (e.g., event messages)
            If None, a generic message is used.
        pvc_names: List of PVC names that have binding issues.
        namespace: Kubernetes namespace.

    Returns:
        Formatted error message with debug instructions.
    """
    if pvc_details:
        header = f'PVC binding issue detected: {pvc_details}.'
    else:
        header = 'PVC binding issue detected.'
    debug_lines = ['To debug, run:', '  sky volumes ls']
    if pvc_names:
        # kubectl describe pvc can take multiple PVC names as args
        pvc_names_str = ' '.join(pvc_names)
        debug_lines.append(
            f'  kubectl describe pvc {pvc_names_str} -n {namespace}')
    return (f'{header}\n'
            'Check if the storage class supports the requested access '
            'mode and if there is sufficient storage capacity.\n' +
            '\n'.join(debug_lines))


def _get_pvc_binding_status(namespace: str, context: str | None,
                            pod: Any) -> str | None:
    """Check if any PVCs used by a pod are pending/unbound.

    Returns an error message if any PVC is pending, None otherwise.
    """
    if pod.spec.volumes is None:
        return None

    pending_pvcs = []  # List of (pvc_name, details_string)
    for vol in pod.spec.volumes:
        pvc_claim = vol.persistent_volume_claim
        if pvc_claim is None:
            continue
        pvc_name = pvc_claim.claim_name
        try:
            pvc = kubernetes.core_api(
                context).read_namespaced_persistent_volume_claim(
                    name=pvc_name,
                    namespace=namespace,
                    _request_timeout=kubernetes.API_TIMEOUT)
            if pvc.status.phase == 'Pending':
                # Get events for the PVC to understand why it's pending
                sorted_events = kubernetes_utils.get_pvc_events(context,
                                                                namespace,
                                                                pvc_name,
                                                                reverse=False)
                event_messages = []
                for event in sorted_events:
                    if event.type == 'Warning' or event.reason in (
                            'ProvisioningFailed', 'WaitForFirstConsumer'):
                        msg = event.message or ''
                        if msg:
                            event_messages.append(f'{event.reason}: {msg}')
                pending_info = f'{pvc_name} (phase: Pending)'
                if event_messages:
                    # Take the most recent event message
                    pending_info += f' - {event_messages[-1]}'
                pending_pvcs.append((pvc_name, pending_info))
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Failed to get PVC {pvc_name} status: {e}')
            continue

    if pending_pvcs:
        pvc_names = [pvc[0] for pvc in pending_pvcs]
        pvc_details = ', '.join(pvc[1] for pvc in pending_pvcs)
        return _format_pvc_binding_error(pvc_details, pvc_names, namespace)
    return None


def _raise_pod_scheduling_errors(namespace, context, new_nodes):
    """Raise pod scheduling failure reason.

    When a pod fails to schedule in Kubernetes, the reasons for the failure
    are recorded as events. This function retrieves those events and raises
    descriptive errors for better debugging and user feedback.
    """
    timeout_err_msg = ('Timed out while waiting for nodes to start. '
                       'Cluster may be out of resources or '
                       'may be too slow to autoscale.')
    for new_node in new_nodes:
        pod = kubernetes.core_api(context).read_namespaced_pod(
            new_node.metadata.name, namespace)
        pod_status = pod.status.phase
        # When there are multiple pods involved while launching instance,
        # there may be a single pod causing issue while others are
        # successfully scheduled. In this case, we make sure to not surface
        # the error message from the pod that is already scheduled.
        if pod_status != 'Pending':
            continue
        pod_name = pod._metadata._name  # pylint: disable=protected-access
        events = kubernetes.core_api(context).list_namespaced_event(
            namespace,
            field_selector=(f'involvedObject.name={pod_name},'
                            'involvedObject.kind=Pod'))
        # Events created in the past hours are kept by
        # Kubernetes python client and we want to surface
        # the latest event message
        events_desc_by_time = sorted(
            events.items,
            key=lambda e: e.metadata.creation_timestamp,
            reverse=True)

        event_message = None
        for event in events_desc_by_time:
            if event.reason == 'FailedScheduling':
                event_message = event.message
                break
        if event_message is not None:
            out_of = {}
            if pod_status == 'Pending':
                # key: resource name, value: (extra message, nice name)
                if 'Insufficient cpu' in event_message:
                    out_of['CPU'] = (': Run \'kubectl get nodes -o '
                                     'custom-columns=NAME:.metadata.name,'
                                     'CPU:.status.allocatable.cpu\' to check '
                                     'the available CPUs on the node.', 'CPUs')
                if 'Insufficient memory' in event_message:
                    out_of['memory'] = (': Run \'kubectl get nodes -o '
                                        'custom-columns=NAME:.metadata.name,'
                                        'MEMORY:.status.allocatable.memory\' '
                                        'to check the available memory on the '
                                        'node.', 'Memory')

                # TODO(aylei): after switching from smarter-device-manager to
                # fusermount-server, we need a new way to check whether the
                # fusermount-server daemonset is ready.
                gpu_lf_keys = [
                    key for lf in kubernetes_utils.LABEL_FORMATTER_REGISTRY
                    for key in lf.get_label_keys()
                ]
                for label_key in gpu_lf_keys:
                    # TODO(romilb): We may have additional node
                    #  affinity selectors in the future - in that
                    #  case we will need to update this logic.
                    # TODO(Doyoung): Update the error message raised
                    # with the multi-host TPU support.
                    gpu_resource_key = kubernetes_utils.get_gpu_resource_key(
                        context)  # pylint: disable=line-too-long
                    if ((f'Insufficient {gpu_resource_key}' in event_message) or
                        ('didn\'t match Pod\'s node affinity/selector'
                         in event_message) and pod.spec.node_selector):
                        if 'gpu' in gpu_resource_key.lower():
                            info_msg = (
                                ': Run \'sky gpus list --infra kubernetes\' to '
                                'see the available GPUs.')
                        else:
                            info_msg = ': '
                        if (pod.spec.node_selector and
                                label_key in pod.spec.node_selector):
                            extra_msg = (
                                f'Verify if any node matching label '
                                f'{pod.spec.node_selector[label_key]} and '
                                f'sufficient resource {gpu_resource_key} '
                                f'is available in the cluster.')
                            extra_msg = info_msg + ' ' + extra_msg
                        else:
                            extra_msg = info_msg
                        if gpu_resource_key not in out_of or len(
                                out_of[gpu_resource_key][0]) < len(extra_msg):
                            out_of[f'{gpu_resource_key}'] = (extra_msg, 'GPUs')

            if len(out_of) > 0:
                # We are out of some resources. We should raise an error.
                rsrc_err_msg = 'Insufficient resource capacity on the '
                rsrc_err_msg += 'cluster:\n'
                out_of_keys = list(out_of.keys())
                for i in range(len(out_of_keys)):
                    rsrc = out_of_keys[i]
                    (extra_msg, nice_name) = out_of[rsrc]
                    extra_msg = extra_msg if extra_msg else ''
                    if i == len(out_of_keys) - 1:
                        indent = '└──'
                    else:
                        indent = '├──'
                    rsrc_err_msg += (f'{indent} Cluster does not have '
                                     f'sufficient {nice_name} for your request'
                                     f'{extra_msg}')
                    if i != len(out_of_keys) - 1:
                        rsrc_err_msg += '\n'

                # Emit the error message without logging prefixes for better UX.
                tmp_handler = sky_logging.EnvAwareHandler(sys.stdout)
                tmp_handler.flush = sys.stdout.flush  # type: ignore
                tmp_handler.setFormatter(sky_logging.NO_PREFIX_FORMATTER)
                tmp_handler.setLevel(sky_logging.ERROR)
                prev_propagate = logger.propagate
                try:
                    logger.addHandler(tmp_handler)
                    logger.propagate = False
                    logger.error(ux_utils.error_message(f'{rsrc_err_msg}'))
                finally:
                    logger.removeHandler(tmp_handler)
                    logger.propagate = prev_propagate
                nice_names = [out_of[rsrc][1] for rsrc in out_of_keys]
                raise config_lib.KubernetesError(
                    f'{timeout_err_msg} '
                    f'Pod status: {pod_status} '
                    f'Details: \'{event_message}\' ',
                    insufficent_resources=nice_names,
                )

        # Check for PVC binding issues
        pvc_error = _get_pvc_binding_status(namespace, context, pod)
        has_pvc_issue = (event_message is not None and
                         'unbound immediate PersistentVolumeClaims'
                         in event_message)
        if pvc_error is not None or has_pvc_issue:
            pvc_msg = pvc_error if pvc_error else (_format_pvc_binding_error(
                pvc_details=None, pvc_names=[], namespace=namespace))
            err_msg = f'{pvc_msg}\nPod status: {pod_status}'
            if event_message:
                err_msg += f' Details: \'{event_message}\''
            raise config_lib.KubernetesError(err_msg)

        err_msg = f'{timeout_err_msg} Pod status: {pod_status}'
        if event_message:
            err_msg += f' Details: \'{event_message}\''
        raise config_lib.KubernetesError(err_msg)

    raise config_lib.KubernetesError(f'{timeout_err_msg}')


def _detect_cluster_event_reason_occurred(namespace, context, search_start,
                                          reason) -> bool:

    def _convert_to_utc(timestamp):
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=datetime.timezone.utc)
        return timestamp.astimezone(datetime.timezone.utc)

    def _get_event_timestamp(event):
        if event.last_timestamp:
            return event.last_timestamp
        elif event.metadata.creation_timestamp:
            return event.metadata.creation_timestamp
        return None

    events = kubernetes.core_api(context).list_namespaced_event(
        namespace=namespace, field_selector=f'reason={reason}')
    for event in events.items:
        ts = _get_event_timestamp(event)
        if ts and _convert_to_utc(ts) > search_start:
            return True
    return False


def _cluster_had_autoscale_event(namespace, context, search_start) -> bool:
    """Detects whether the cluster had a autoscaling event after a
    specified datetime. This only works when using cluster-autoscaler.

    Args:
        namespace: kubernetes namespace
        context: kubernetes context
        search_start (datetime.datetime): filter for events that occurred
            after search_start

    Returns:
        A boolean whether the cluster has an autoscaling event or not.
    """
    assert namespace is not None

    try:
        return _detect_cluster_event_reason_occurred(namespace, context,
                                                     search_start,
                                                     'TriggeredScaleUp')
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Error occurred while detecting cluster autoscaler: {e}')
        return False


def _cluster_maybe_autoscaling(namespace, context, search_start) -> bool:
    """Detects whether a kubernetes cluster may have an autoscaling event.

    This is not a definitive detection. FailedScheduling, which is an
    event that can occur when not enough resources are present in the cluster,
    which is a trigger for cluster autoscaling. However, FailedScheduling may
    have occurred due to other reasons (cluster itself is abnormal).

    Hence, this should only be used for autoscalers that don't emit the
    TriggeredScaleUp event, e.g.: Karpenter.

    Args:
        namespace: kubernetes namespace
        context: kubernetes context
        search_start (datetime.datetime): filter for events that occurred
            after search_start

    Returns:
        A boolean whether the cluster has an autoscaling event or not.
    """
    assert namespace is not None
    search_start = _as_aware_utc(search_start)
    if search_start is None:
        return False
    snapshot = _get_failed_scheduling_event_snapshot(namespace, context)
    return (snapshot.latest_occurrence is not None and
            snapshot.latest_occurrence > search_start)


def _update_spinner_message(*, iteration: int, pods: list[Any],
                            context: str | None, namespace: str,
                            cluster_name_on_cloud: str,
                            cluster_name: str) -> None:
    del iteration, pods, context, namespace  #unused
    del cluster_name_on_cloud, cluster_name  #unused
    pass


@timeline.event
def _wait_for_pods_to_schedule(namespace, context, new_nodes, timeout: int,
                               cluster_name: str,
                               create_pods_start: datetime.datetime):
    """Wait for all pods to be scheduled.

    Wait for all pods including jump pod to be scheduled, and if it
    exceeds the timeout, raise an exception. If pod's container
    is ContainerCreating, then we can assume that resources have been
    allocated and we can exit.

    If timeout is set to a negative value, this method will wait indefinitely.

    Will update the spinner message to indicate autoscaling if autoscaling
    is happening.
    """
    # Create a set of pod names we're waiting for
    if not new_nodes:
        return
    expected_pod_names = {node.metadata.name for node in new_nodes}
    start_time = time.time()

    # Variables for autoscaler detection
    is_ssh_node_pool = context.startswith('ssh-') if context else False
    autoscaler_type = skypilot_config.get_effective_region_config(
        cloud='ssh' if is_ssh_node_pool else 'kubernetes',
        region=context,
        keys=('autoscaler',),
        default_value=None)
    autoscaler_is_set = autoscaler_type is not None
    configured_autoscaler = (
        kubernetes_enums.KubernetesAutoscalerType(autoscaler_type)
        if autoscaler_is_set else None)
    use_heuristic_detection = (
        configured_autoscaler is not None and
        not configured_autoscaler.emits_autoscale_event())
    is_autoscaling = False
    # When a definitive TriggeredScaleUp event is observed, this records the
    # detection moment so that we can extend the deadline — node scale-up is
    # unpredictable and the user-configured provision_timeout is usually
    # tuned for normal scheduling latency rather than for waiting on
    # autoscaler nodes. Heuristic FailedScheduling detection (Karpenter) does
    # NOT set this — extending a deadline by 15 min based on FailedScheduling
    # alone would mask real failures (oversized requests, taints, etc.).
    autoscale_detected_time: float | None = None

    # If the user configured an autoscaler but left provision_timeout too
    # short, bump the initial timeout up to the minimum so the Cluster
    # Autoscaler has time to scan and emit its first event. Without this
    # floor the loop would exit before autoscale_detected_time could ever
    # be set. Negative timeout (indefinite wait) is left alone.
    if (autoscaler_is_set and
            0 <= timeout < _AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS):
        logger.warning(
            f'Autoscaler is configured but provision_timeout ({timeout}s) '
            f'is too short; bumping initial timeout to '
            f'{_AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS}s.')
        timeout = _AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS

    original_deadline = start_time + timeout

    def _evaluate_timeout() -> bool:
        # If timeout is negative, retry indefinitely.
        if timeout < 0:
            return True
        # If autoscaling has been detected, extend the deadline from the
        # detection moment. Use max(...) so an explicitly long user timeout
        # is never shortened by this extension.
        if autoscale_detected_time is not None:
            extended_deadline = (autoscale_detected_time +
                                 _AUTOSCALE_DETECTED_TIMEOUT_SECONDS)
            deadline = max(original_deadline, extended_deadline)
        else:
            deadline = original_deadline
        return time.time() < deadline

    iteration = 0
    while _evaluate_timeout():
        # Get all pods in a single API call using the cluster name label
        # which all pods in new_nodes should share
        cluster_name_on_cloud = new_nodes[0].metadata.labels[
            constants.TAG_SKYPILOT_CLUSTER_NAME]
        pods = kubernetes.core_api(context).list_namespaced_pod(
            namespace,
            label_selector=
            f'{constants.TAG_SKYPILOT_CLUSTER_NAME}={cluster_name_on_cloud}'
        ).items

        # Get the set of found pod names and check if we have all expected pods
        found_pod_names = {pod.metadata.name for pod in pods}
        missing_pods = expected_pod_names - found_pod_names
        if missing_pods:
            logger.info('Retrying waiting for pods: '
                        f'Missing pods: {missing_pods}')
            time.sleep(0.5)
            continue

        # A pod is considered scheduled once the kube-scheduler has bound it
        # to a node (capacity found). We deliberately do not wait for the
        # kubelet to populate container_statuses here -- that can lag and is
        # handled by _wait_for_pods_to_run, which has no provision_timeout.
        all_scheduled = all(
            _pod_is_scheduled(pod)
            for pod in pods
            if pod.metadata.name in expected_pod_names)

        if all_scheduled:
            return

        # The Event source is authoritative. Karpenter may manage a cluster
        # even when SkyPilot's optional autoscaler setting is absent. A
        # negative timeout is the explicit exception: the caller asked to
        # retain an admitted pod indefinitely, including on a fixed GPU pool
        # that Karpenter itself cannot expand.
        if timeout >= 0:
            pending_pod_uids = {
                pod.metadata.uid
                for pod in pods
                if (pod.metadata.name in expected_pod_names and pod.status.phase
                    == 'Pending' and not _pod_is_scheduled(pod) and
                    isinstance(pod.metadata.uid, str))
            }
            _raise_for_karpenter_gpu_incompatibility(namespace, context,
                                                     pending_pod_uids,
                                                     create_pods_start)

        # Check if cluster is autoscaling and update spinner message.
        # Minor optimization to not query k8s api after autoscaling
        # event was detected. This is useful because there isn't any
        # autoscaling complete event.
        if autoscaler_is_set and not is_autoscaling:
            if use_heuristic_detection:
                is_autoscaling = _cluster_maybe_autoscaling(
                    namespace, context, create_pods_start)
                msg = 'Kubernetes cluster may be scaling up'
            else:
                is_autoscaling = _cluster_had_autoscale_event(
                    namespace, context, create_pods_start)
                msg = 'Kubernetes cluster is autoscaling'
                if is_autoscaling:
                    # Definitive TriggeredScaleUp observed — extend the
                    # deadline from this moment in _evaluate_timeout().
                    autoscale_detected_time = time.time()

            if is_autoscaling:
                rich_utils.force_update_status(
                    ux_utils.spinner_message(f'Launching ({msg})',
                                             cluster_name=cluster_name))
                # The cluster row is written by add_or_update_cluster
                # earlier in the launch flow, so the hash lookup inside
                # add_cluster_event is guaranteed to succeed here.
                # TODO(kev): mirror this emit on AWS / GCP / Slurm autoscaler
                # paths.
                global_user_state.add_cluster_event(
                    cluster_name,
                    new_status=None,
                    reason=f'Launching ({msg})',
                    event_type=global_user_state.ClusterEventType.
                    LAUNCH_PROGRESS,
                    nop_if_duplicate=True,
                )
        if not is_autoscaling:
            _update_spinner_message(iteration=iteration,
                                    pods=pods,
                                    context=context,
                                    namespace=namespace,
                                    cluster_name_on_cloud=cluster_name_on_cloud,
                                    cluster_name=cluster_name)

        iteration += 1
        time.sleep(1)

    # Handle pod scheduling errors
    try:
        _raise_pod_scheduling_errors(namespace, context, new_nodes)
    except config_lib.KubernetesError:
        raise
    except Exception as e:
        raise config_lib.KubernetesError(
            'An error occurred while trying to fetch the reason '
            'for pod scheduling failure. '
            f'Error: {common_utils.format_exception(e)}') from None
