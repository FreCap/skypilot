"""Kubernetes Event gateway for autostop attribution."""

import datetime
from typing import Any

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision.kubernetes import utils as kubernetes_utils

# Use the historical logger so facade imports and logging behavior stay stable.
logger = sky_logging.init_logger('sky.provision.kubernetes.instance')

# Custom Kubernetes Event reason emitted by skylet when a cluster autodowns
# itself after reaching its idle timeout. The server's status refresh reads this
# back (get_cluster_autostop_event) as a durable breadcrumb to attribute the
# termination to autostop -- even when the refresh never observed the cluster in
# the AUTOSTOPPING state, which happens when the pod completes the autodown
# between two refreshes. On Kubernetes a cluster only ever autodowns (stop is
# not supported), so a single reason suffices.
AUTOSTOP_EVENT_REASON = 'SkyPilotAutodown'


def emit_autostop_event_best_effort(provider_config: dict[str, Any],
                                    cluster_name_on_cloud: str) -> None:
    """Emit a Kubernetes Event marking that the cluster is autodowning.

    Best-effort breadcrumb written by skylet from the autostop code path, just
    before the pods are terminated, so it survives the pod deletion (Kubernetes
    keeps events for the namespace's event TTL, ~1h by default). Read back by
    get_cluster_autostop_event on the server. Never raises -- failing to emit
    the event must not block the actual autodown.
    """
    try:
        namespace = kubernetes_utils.get_namespace_from_config(provider_config)
        context = kubernetes_utils.get_context_from_config(provider_config)
        k8s_client = kubernetes.kubernetes.client
        now = datetime.datetime.now(datetime.timezone.utc)
        # The event references the head pod, whose name is exactly
        # f'{cluster_name_on_cloud}-head' -- the reader matches on it. Event
        # names must be unique within the namespace.
        head_pod_name = f'{cluster_name_on_cloud}-head'
        suffix = f'{int(now.timestamp() * 1e6):x}'
        event_name = f'{head_pod_name}.skyautodown.{suffix}'
        event = k8s_client.CoreV1Event(
            metadata=k8s_client.V1ObjectMeta(name=event_name,
                                             namespace=namespace),
            involved_object=k8s_client.V1ObjectReference(kind='Pod',
                                                         name=head_pod_name,
                                                         namespace=namespace),
            reason=AUTOSTOP_EVENT_REASON,
            message='Cluster is autodowning after reaching its idle timeout.',
            type='Normal',
            source=k8s_client.V1EventSource(component='skypilot-skylet'),
            first_timestamp=now,
            last_timestamp=now)
        kubernetes.core_api(context).create_namespaced_event(
            namespace, event, _request_timeout=kubernetes.API_TIMEOUT)
        logger.debug(f'Emitted {AUTOSTOP_EVENT_REASON} event for '
                     f'{cluster_name_on_cloud}.')
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Failed to emit autodown event for '
                     f'{cluster_name_on_cloud}: {e}')


def get_cluster_autostop_event(
        provider_config: dict[str, Any],
        cluster_name_on_cloud: str,
        since: float | None = None) -> dict[str, Any] | None:
    """Most recent autodown breadcrumb for the cluster, or None.

    Reads the Kubernetes Event emitted by skylet (see
    emit_autostop_event_best_effort) when the cluster autodowned itself. Lets
    the server attribute a terminated k8s cluster to autostop when the status
    refresh never observed the AUTOSTOPPING state. Matches the head pod's name
    exactly so it still resolves after the head pod (the event's involvedObject)
    has been deleted. Returns a dict with ``reason``, ``message`` and
    ``transitioned_at`` (unix seconds, or None if the event carries no
    timestamp). Best-effort -- never raises.

    Args:
        since: If given (unix seconds), ignore events older than this. Pass the
            current cluster's launch time so a stale breadcrumb left by a prior
            incarnation of a same-named cluster (k8s keeps events for the
            namespace TTL, ~1h) is not mis-attributed to this teardown.
    """
    try:
        namespace = kubernetes_utils.get_namespace_from_config(provider_config)
        context = kubernetes_utils.get_context_from_config(provider_config)
        events = kubernetes.core_api(context).list_namespaced_event(
            namespace,
            field_selector=f'reason={AUTOSTOP_EVENT_REASON}',
            _request_timeout=kubernetes.API_TIMEOUT).items
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Failed to read autodown event for '
                     f'{cluster_name_on_cloud}: {e}')
        return None

    def _event_unix_time(event: Any) -> int | None:
        ts = (event.last_timestamp or event.event_time or
              event.metadata.creation_timestamp)
        if ts is None:
            return None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return int(ts.timestamp())

    # The writer always references the head pod, whose name is exactly
    # f'{cluster_name_on_cloud}-head'. Match it exactly (not by prefix) so a
    # sibling cluster whose name shares this prefix cannot contaminate the
    # result, and bound by `since` so a stale breadcrumb from a previous
    # incarnation of a same-named cluster is ignored.
    head_pod_name = f'{cluster_name_on_cloud}-head'
    matching = []
    for event in events:
        if (event.involved_object is None or
                event.involved_object.name != head_pod_name):
            continue
        event_time = _event_unix_time(event)
        if since is not None and (event_time is None or event_time < since):
            continue
        matching.append((event_time, event))
    if not matching:
        return None
    event_time, latest = max(matching, key=lambda item: item[0] or 0)
    return {
        'reason': latest.reason,
        'message': latest.message,
        'transitioned_at': event_time,
    }
