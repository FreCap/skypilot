"""Observe exact Kubernetes executor termination without inferring quiescence."""

from __future__ import annotations

import datetime
import os
import threading
import typing
from typing import Any

from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.server.requests import postgres as request_postgres

if typing.TYPE_CHECKING:
    from sky.server.requests.postgres import ServerPodIdentity

logger = sky_logging.init_logger(__name__)
kubernetes = adaptors_common.LazyImport('sky.adaptors.kubernetes')

OBSERVER_ENABLED_ENV_VAR = (
    'SKYPILOT_API_EXECUTOR_TERMINATION_EVIDENCE_OBSERVER_ENABLED')
_WATCH_TIMEOUT_SECONDS = 5
_RECONNECT_SECONDS = 1
_STOP_TIMEOUT_SECONDS = _WATCH_TIMEOUT_SECONDS + 10


def enabled() -> bool:
    """Whether the transition-only Kubernetes observer is enabled."""
    return os.environ.get(OBSERVER_ENABLED_ENV_VAR, '').lower() == 'true'


def _required_text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def observation_from_pod(
    pod: Any,
    *,
    kubernetes_cluster_uid: str,
) -> request_postgres.ExecutorTerminationObservation | None:
    """Extract only a current role-container termination after Pod deletion."""
    metadata = getattr(pod, 'metadata', None)
    status = getattr(pod, 'status', None)
    pod_uid = _required_text(getattr(metadata, 'uid', None))
    namespace = _required_text(getattr(metadata, 'namespace', None))
    pod_name = _required_text(getattr(metadata, 'name', None))
    resource_version = _required_text(
        getattr(metadata, 'resource_version', None))
    deletion_timestamp = getattr(metadata, 'deletion_timestamp', None)
    if (pod_uid is None or namespace is None or pod_name is None or
            resource_version is None or
            not isinstance(deletion_timestamp, datetime.datetime) or
            deletion_timestamp.tzinfo is None):
        return None
    container_statuses = getattr(status, 'container_statuses', None)
    if not isinstance(container_statuses, list):
        return None
    expected_names = frozenset(
        {'skypilot-api', 'skypilot-executor', 'skypilot-controller'})
    matching = [
        container_status for container_status in container_statuses
        if getattr(container_status, 'name', None) in expected_names
    ]
    if len(matching) != 1:
        return None
    container_status = matching[0]
    # Deliberately ignore last_state.terminated: a liveness restart can run
    # another executor in the same Pod UID after that historical exit.
    state = getattr(container_status, 'state', None)
    terminated = getattr(state, 'terminated', None)
    finished_at = getattr(terminated, 'finished_at', None)
    exit_code = getattr(terminated, 'exit_code', None)
    if (not isinstance(finished_at, datetime.datetime) or
            finished_at.tzinfo is None or finished_at < deletion_timestamp or
            isinstance(exit_code, bool) or not isinstance(exit_code, int) or
            exit_code < 0):
        return None
    cluster_uid = _required_text(kubernetes_cluster_uid)
    container_name = _required_text(getattr(container_status, 'name', None))
    if cluster_uid is None or container_name is None:
        return None
    reason = getattr(terminated, 'reason', None)
    if reason is not None and not isinstance(reason, str):
        reason = str(reason)
    return request_postgres.ExecutorTerminationObservation(
        kubernetes_cluster_uid=cluster_uid,
        pod_namespace=namespace,
        pod_name=pod_name,
        pod_uid=pod_uid,
        container_name=container_name,
        pod_resource_version=resource_version,
        pod_deletion_timestamp=deletion_timestamp,
        container_finished_at=finished_at,
        container_exit_code=exit_code,
        container_reason=reason)


class ExecutorTerminationEvidenceObserver:
    """One controller-generation-owned, synchronously stoppable Pod watch."""

    def __init__(self, controller_owner: tuple[str, int],
                 namespace: str) -> None:
        self._controller_owner = controller_owner
        self._namespace = namespace
        self._stop_event = threading.Event()
        self._watch_lock = threading.Lock()
        self._watch: Any | None = None
        self._thread = threading.Thread(
            target=self._run,
            name='executor-termination-evidence-observer',
            daemon=True)

    def start(self) -> None:
        if not self._namespace:
            raise RuntimeError(
                'Executor termination observer requires a Pod namespace.')
        self._thread.start()

    def _observe_once(self) -> None:
        context = kubernetes.in_cluster_context_name()
        core_api = kubernetes.core_api(context)
        cluster = core_api.read_namespace('kube-system', _request_timeout=10)
        cluster_uid = _required_text(
            getattr(getattr(cluster, 'metadata', None), 'uid', None))
        if cluster_uid is None:
            raise RuntimeError('kube-system Namespace has no immutable UID.')
        watcher = kubernetes.watch(context)
        with self._watch_lock:
            if self._stop_event.is_set():
                return
            self._watch = watcher
        try:
            stream = watcher.stream(core_api.list_namespaced_pod,
                                    namespace=self._namespace,
                                    timeout_seconds=_WATCH_TIMEOUT_SECONDS)
            for event in stream:
                if self._stop_event.is_set():
                    return
                pod = event.get('object') if isinstance(event, dict) else None
                observation = observation_from_pod(
                    pod, kubernetes_cluster_uid=cluster_uid)
                if observation is None:
                    continue
                recorded: tuple[str, ...] = ()
                try:
                    recorded = (
                        request_postgres.record_executor_termination_evidence(
                            observation, observer_owner=self._controller_owner))
                except request_postgres.ExecutorTerminationEvidenceRejected as e:
                    logger.debug('Rejected executor termination observation: '
                                 f'{e}')
                    continue
                except request_postgres.ExecutorTerminationEvidenceConflict as e:
                    # A deleting Pod can emit another resource version after
                    # its first valid certificate was persisted. Preserve the
                    # immutable first certificate, surface the disagreement,
                    # and keep observing unrelated Pods on this same watch.
                    logger.error('Conflicting executor termination evidence '
                                 f'was rejected: {e}')
                    continue
                if recorded:
                    logger.info('Recorded executor termination evidence for '
                                f'{len(recorded)} request execution(s) owned '
                                f'by Pod {observation.pod_namespace}/'
                                f'{observation.pod_name}.')
        finally:
            with self._watch_lock:
                if self._watch is watcher:
                    self._watch = None
            watcher.stop()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                if not request_postgres.controller_leadership_is_current(
                        *self._controller_owner):
                    logger.info('Executor termination observer lost controller '
                                'ownership; stopping.')
                    return
                self._observe_once()
            except Exception as e:  # pylint: disable=broad-except
                if not self._stop_event.is_set():
                    logger.warning('Executor termination observer reconnecting '
                                   f'after error: {e}')
            self._stop_event.wait(_RECONNECT_SECONDS)

    def stop(self) -> None:
        """Stop the watch and prove its thread cannot outlive leadership."""
        self._stop_event.set()
        with self._watch_lock:
            watcher = self._watch
        if watcher is not None:
            watcher.stop()
        self._thread.join(timeout=_STOP_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            raise RuntimeError(
                'Executor termination observer did not stop before leadership '
                'release.')


def start(
    controller_owner: tuple[str, int],
    pod_identity: ServerPodIdentity,
) -> ExecutorTerminationEvidenceObserver | None:
    """Start the transition observer only for an enabled exact controller."""
    if not enabled():
        return None
    if controller_owner[0] != pod_identity.uid:
        raise RuntimeError(
            'Executor termination observer owner must be this Pod UID.')
    observer = ExecutorTerminationEvidenceObserver(controller_owner,
                                                   pod_identity.namespace)
    observer.start()
    return observer
