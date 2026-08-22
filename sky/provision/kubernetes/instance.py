"""Kubernetes instance provisioning."""
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
import contextlib
import copy
import datetime
import json
import re
import time
from typing import Any, Literal, NoReturn, Optional, TYPE_CHECKING

from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision import common
from sky.provision import constants
from sky.provision import docker_utils
from sky.provision.kubernetes import autostop_events
from sky.provision.kubernetes import config as config_lib
from sky.provision.kubernetes import constants as k8s_constants
from sky.provision.kubernetes import host_network_probe
from sky.provision.kubernetes import kueue_admission
from sky.provision.kubernetes import pod_diagnostics
from sky.provision.kubernetes import pod_scheduling
from sky.provision.kubernetes import pod_spec as pod_spec_lib
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.provision.kubernetes import volume
from sky.utils import command_runner
from sky.utils import common_utils
from sky.utils import plugin_extensions
from sky.utils import rich_utils
from sky.utils import status_lib
from sky.utils import subprocess_utils
from sky.utils import timeline
from sky.utils import ux_utils
from sky.utils.db import db_utils

if TYPE_CHECKING:
    from kubernetes.client import V1Pod

POLL_INTERVAL = 2
_TIMEOUT_FOR_POD_TERMINATION = 60  # 1 minutes
_MAX_RETRIES = 3
_MAX_MISSING_PODS_RETRIES = 5
_MAX_QUERY_INSTANCES_RETRIES = 5
_QUERY_INSTANCES_RETRY_INTERVAL = .5
_NUM_THREADS = subprocess_utils.get_parallel_threads('kubernetes')

# Normal-type pod events that represent slow, legitimately-in-flight steps
# whose state.waiting.reason is the uninformative 'ContainerCreating'.
# Consulted only as a fallback when no Warning-type event is present.
_PENDING_REASON_NORMAL_EVENT_ALLOWLIST = {
    'Pulling',  # kubelet pulling image (can be minutes for large images)
    'Provisioning',  # external CSI provisioner creating a PV
    'WaitForFirstConsumer',  # late-binding storage class
}

# Pattern to extract SSH user from command output, handling MOTD contamination
_SSH_USER_PATTERN = re.compile(r'SKYPILOT_SSH_USER: ([^\s\n]+)')

# Kueue's AssignQueueLabelsForPods feature publishes the ClusterQueue name in
# a Kubernetes label value.  It only emits that label when the name is also a
# DNS-1123 label, even though the ClusterQueue API accepts DNS subdomains.
_DNS_1123_LABEL_PATTERN = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')

logger = sky_logging.init_logger(__name__)

_RequiredKueuePodLifecycle = Literal['create_response', 'adoption', 'admitted']


@contextlib.contextmanager
def _provider_mutation_guard(
    factory: common.ProviderEffectGuardFactory | None,) -> Iterator[None]:
    """Enters one fresh runtime-only provider-effect authorization."""
    if factory is None:
        yield
        return
    with factory():
        yield


# These direct aliases preserve the historical instance import surface while
# the cross-process Kubernetes Event protocol lives in its own gateway module.
AUTOSTOP_EVENT_REASON = autostop_events.AUTOSTOP_EVENT_REASON
emit_autostop_event_best_effort = (
    autostop_events.emit_autostop_event_best_effort)
get_cluster_autostop_event = autostop_events.get_cluster_autostop_event

# Preserve module and pickle identities for historical imports.
emit_autostop_event_best_effort.__module__ = __name__
get_cluster_autostop_event.__module__ = __name__

# These aliases preserve the historical instance import surface while pod
# scheduling and capacity diagnosis live in their own low-state module.
# pylint: disable=protected-access
_AUTOSCALE_DETECTED_TIMEOUT_SECONDS = (
    pod_scheduling._AUTOSCALE_DETECTED_TIMEOUT_SECONDS)
_AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS = (
    pod_scheduling._AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS)
_pod_is_scheduled = pod_scheduling._pod_is_scheduled
_formatted_resource_requirements = (
    pod_scheduling._formatted_resource_requirements)
_formatted_node_selector = pod_scheduling._formatted_node_selector
_lack_resource_msg = pod_scheduling._lack_resource_msg
_format_pvc_binding_error = pod_scheduling._format_pvc_binding_error
_get_pvc_binding_status = pod_scheduling._get_pvc_binding_status
_raise_pod_scheduling_errors = pod_scheduling._raise_pod_scheduling_errors
_detect_cluster_event_reason_occurred = (
    pod_scheduling._detect_cluster_event_reason_occurred)
_cluster_had_autoscale_event = pod_scheduling._cluster_had_autoscale_event
_cluster_maybe_autoscaling = pod_scheduling._cluster_maybe_autoscaling
_update_spinner_message = pod_scheduling._update_spinner_message
_wait_for_pods_to_schedule = pod_scheduling._wait_for_pods_to_schedule
# pylint: enable=protected-access

# These aliases preserve the historical instance import surface while pod
# status interpretation lives with the existing Kubernetes diagnostics.
# pylint: disable=protected-access
NodeHealthInfo = pod_diagnostics.NodeHealthInfo
_get_pod_health_issues = pod_diagnostics._get_pod_health_issues
_reason_lacks_specific_cause = (pod_diagnostics._reason_lacks_specific_cause)
_unmask_crashloopbackoff_reason = (
    pod_diagnostics._unmask_crashloopbackoff_reason)
_get_pod_pending_reason_from_container_status = (
    pod_diagnostics._get_pod_pending_reason_from_container_status)

# Preserve module and pickle identities for historical imports.
for _pod_diagnostics_symbol in (
        NodeHealthInfo,
        _get_pod_health_issues,
        _reason_lacks_specific_cause,
        _unmask_crashloopbackoff_reason,
        _get_pod_pending_reason_from_container_status,
):
    _pod_diagnostics_symbol.__module__ = __name__
# pylint: enable=protected-access


def ray_tag_filter(cluster_name: str) -> dict[str, str]:
    return {k8s_constants.TAG_RAY_CLUSTER_NAME: cluster_name}


def _is_head(pod) -> bool:
    return pod.metadata.labels.get(constants.TAG_RAY_NODE_KIND) == 'head'


def _get_head_pod_name(pods: dict[str, Any]) -> str | None:
    return next((pod_name for pod_name, pod in pods.items() if _is_head(pod)),
                None)


def _get_pvc_name(cluster_name: str, volume_name: str) -> str:
    return f'{cluster_name}-{volume_name}'


def _get_deployment_name(cluster_name: str) -> str:
    return f'{cluster_name}-deployment'


def is_high_availability_cluster_by_kubectl(
        cluster_name: str,
        context: str | None = None,
        namespace: str | None = None) -> bool:
    """Check if a cluster is a high availability controller by calling
    `kubectl get deployment`.

    The deployment must have the label `skypilot-cluster-name` set to
    `cluster_name`.
    """
    try:
        deployment_list = kubernetes.apps_api(
            context).list_namespaced_deployment(
                namespace,
                label_selector=
                f'{constants.TAG_SKYPILOT_CLUSTER_NAME}={cluster_name}')
    except kubernetes.api_exception():
        return False
    # It is a high availability cluster if there is at least one deployment
    # matching the label selector.
    return bool(deployment_list.items)


def _raise_command_running_error(message: str, command: str, pod_name: str,
                                 rc: int, stdout: str) -> None:
    if rc == 0:
        return
    raise config_lib.KubernetesError(
        f'Failed to {message} for pod {pod_name} with return '
        f'code {rc}: {command!r}\nOutput: {stdout}.')


@timeline.event
def _wait_for_pods_to_run(namespace, context, cluster_name,
                          new_pods) -> dict[str, object]:
    """Wait for pods and their containers to be ready.

    Pods may be pulling images or may be in the process of container
    creation.
    """
    if not new_pods:
        return {}

    # Create a set of pod names we're waiting for
    expected_pod_names = {pod.metadata.name for pod in new_pods}

    def _check_init_containers(pod) -> tuple[str, int, int] | None:
        """Check init containers for errors and return running container info.

        Returns (name, 1-based index, total) of the currently running init
        container, or None if none is running.
        Raises KubernetesError if any init container failed.
        """
        init_statuses = pod.status.init_container_statuses
        total = len(init_statuses)
        running_info: tuple[str, int, int] | None = None
        for idx, init_status in enumerate(init_statuses):
            init_terminated = init_status.state.terminated
            if init_terminated:
                if init_terminated.exit_code != 0:
                    msg = init_terminated.message if (
                        init_terminated.message) else str(init_terminated)
                    raise config_lib.KubernetesError(
                        'Failed to run init container for pod '
                        f'{pod.metadata.name}. Error details: {msg}.')
                continue
            if (init_status.state.running is not None and running_info is None):
                running_info = (init_status.name, idx + 1, total)
            init_waiting = init_status.state.waiting
            if (init_waiting is not None and init_waiting.reason
                    not in ['ContainerCreating', 'PodInitializing']):
                # TODO(romilb): There may be more states to check for. Add
                #  them as needed.
                msg = init_waiting.message if (
                    init_waiting.message) else str(init_waiting)
                unmasked = _unmask_crashloopbackoff_reason(init_status)
                reason_text = (unmasked if unmasked is not None else
                               (init_waiting.reason or 'Unknown'))
                raise config_lib.KubernetesError(
                    f'Failed to create init container for pod '
                    f'{pod.metadata.name}. Error details: '
                    f'{reason_text}: {msg}.')
        return running_info

    def _inspect_pod_status(pod):
        # Check if pod is terminated/preempted/failed (unchanged).
        if (pod.metadata.deletion_timestamp is not None or
                pod.status.phase == 'Failed'):
            # Get the reason and write to cluster events before
            # the pod gets completely deleted from the API.
            termination_reason = _get_pod_termination_reason(pod, cluster_name)
            logger.warning(
                f'Pod {pod.metadata.name} terminated: {termination_reason}')
            condensed = _condensed_pod_reason(pod)
            raise config_lib.KubernetesError(
                f'Pod {pod.metadata.name} failed: {condensed}')

        container_statuses = pod.status.container_statuses
        # Happy path: pod Running and every container Running (unchanged).
        if (pod.status.phase == 'Running' and container_statuses is not None and
                all(container.state.running
                    for container in container_statuses)):
            return True, None

        # Tier 1: container-status sweep. Computed once, consumed in both
        # branches below.
        container_reason = _get_pod_pending_reason_from_container_status(pod)

        if pod.status.phase == 'Pending':
            # Today's raise block -- control flow preserved, message enriched
            # via _unmask_crashloopbackoff_reason when the waiting state is
            # CrashLoopBackOff. msg body (waiting.message) is always preserved.
            init_reason: str | None = None
            if container_statuses is not None:
                for container_status in container_statuses:
                    if not container_status.state:
                        continue
                    waiting = container_status.state.waiting
                    if waiting is not None:
                        if waiting.reason == 'PodInitializing':
                            running_init = _check_init_containers(pod)
                            if running_init is not None:
                                name, idx, total = running_init
                                init_reason = (f'init container {name!r} '
                                               f'running ({idx}/{total})')
                            else:
                                init_reason = 'init container running'
                        elif waiting.reason != 'ContainerCreating':
                            msg = waiting.message if (
                                waiting.message) else str(waiting)
                            unmasked = _unmask_crashloopbackoff_reason(
                                container_status)
                            reason_text = (unmasked if unmasked is not None else
                                           (waiting.reason or 'Unknown'))
                            raise config_lib.KubernetesError(
                                f'{reason_text}: {msg}')
                    terminated = container_status.state.terminated
                    if terminated is not None and terminated.exit_code != 0:
                        reason_str = (terminated.reason if terminated.reason
                                      else f'exit({terminated.exit_code})')
                        raise config_lib.KubernetesError(
                            f'Container in pod {pod.metadata.name} '
                            f'terminated with error while pod is still '
                            f'pending: {reason_str}. Run '
                            f'`sky logs --provision {cluster_name}` '
                            'for more details.')

            # Init container reason wins over all event-based reasons,
            # since events can retain stale "Pulling image" entries long
            # after the pull completed.  Otherwise, Tier 1 (container
            # status) wins; fall back to Tier 2/3 events.
            reason: str | None = init_reason or container_reason
            event_message: str | None = None
            if reason is None:
                pending_reason = _get_pod_pending_reason(
                    context, namespace, pod.metadata.name)
                if pending_reason is not None:
                    reason, event_message = pending_reason
            if reason is None and _pod_is_scheduled(pod):
                # A freshly-bound pod that the kubelet has not picked up yet
                # (and the uninformative 'ContainerCreating' state) has no
                # container-status reason and no event yet. Default to
                # 'container creation' so the launch spinner shows useful
                # detail (e.g. 'Launching (1 pod(s) pending due to container
                # creation)') instead of a bare 'Launching'. Gate on
                # _pod_is_scheduled so an unbound pod still waiting for
                # capacity is not mislabeled as creating a container.
                reason = 'container creation'
            if reason is not None:
                log_msg = f'Pod {pod.metadata.name} is pending: {reason}'
                if event_message:
                    log_msg += f': {event_message}'
                logger.debug(log_msg)
            return False, reason

        # phase == 'Running' but not all containers running (e.g. one is in
        # CrashLoopBackOff). Surface tier-1's pending reason -- previously this
        # returned (False, None) silently, masking OOMKilled etc.
        return False, container_reason

    missing_pods_retry = 0
    last_status_msg: str | None = None
    while True:
        # Get all pods in a single API call
        cluster_name_on_cloud = new_pods[0].metadata.labels[
            constants.TAG_SKYPILOT_CLUSTER_NAME]
        all_pods = kubernetes.core_api(context).list_namespaced_pod(
            namespace,
            label_selector=
            f'{constants.TAG_SKYPILOT_CLUSTER_NAME}={cluster_name_on_cloud}'
        ).items

        # Get the set of found pod names and check if we have all expected pods
        found_pod_names = {pod.metadata.name for pod in all_pods}
        missing_pod_names = expected_pod_names - found_pod_names
        if missing_pod_names:
            # In _wait_for_pods_to_schedule, we already wait for all pods to go
            # from pending to scheduled. So if a pod is missing here, it means
            # something unusual must have happened, and so should be treated as
            # an exception.
            # It is also only in _wait_for_pods_to_schedule that
            # provision_timeout is used.
            # TODO(kevin): Should we take provision_timeout into account here,
            # instead of hardcoding the number of retries?
            if missing_pods_retry >= _MAX_MISSING_PODS_RETRIES:
                first_pod = True
                for pod_name in missing_pod_names:
                    reason = _get_pod_missing_reason(context, namespace,
                                                     cluster_name, pod_name,
                                                     first_pod)
                    logger.warning(f'Pod {pod_name} missing: {reason}')
                    first_pod = False
                raise config_lib.KubernetesError(
                    f'Failed to get all pods after {missing_pods_retry} '
                    f'retries. Some pods may have been terminated or failed '
                    f'unexpectedly. Run `sky logs --provision {cluster_name}` '
                    'for more details.')
            logger.info('Retrying running pods check: '
                        f'Missing pods: {missing_pod_names}')
            time.sleep(0.5)
            missing_pods_retry += 1
            continue

        pods_to_check = [
            pod for pod in all_pods if pod.metadata.name in expected_pod_names
        ]
        pod_statuses = subprocess_utils.run_in_parallel(_inspect_pod_status,
                                                        pods_to_check,
                                                        _NUM_THREADS)

        all_pods_running = True
        pending_reasons_count: dict[str, int] = {}
        for is_running, pending_reason in pod_statuses:
            if not is_running:
                all_pods_running = False
            if pending_reason is not None:
                pending_reasons_count[pending_reason] = (
                    pending_reasons_count.get(pending_reason, 0) + 1)

        if all_pods_running:
            # Bind the exact object incarnation whose Running state ended the
            # wait. A same-name replacement cannot inherit that proof at the
            # post-admission attestation boundary.
            return {
                pod.metadata.name: getattr(pod.metadata, 'uid', None)
                for pod in pods_to_check
            }

        if pending_reasons_count:
            msg = ', '.join([
                f'{count} pod(s) pending due to {reason}'
                for reason, count in sorted(pending_reasons_count.items())
            ])
            status_text = f'Launching ({msg})'
        else:
            status_text = 'Launching'
        new_status_msg = ux_utils.spinner_message(status_text,
                                                  cluster_name=cluster_name)
        if new_status_msg != last_status_msg:
            rich_utils.force_update_status(new_status_msg)
            if pending_reasons_count:
                # Skip the bare 'Launching' status_text — it duplicates
                # the badge label and would produce a useless tooltip.
                # The cluster row is written by add_or_update_cluster
                # earlier in the launch flow, so the hash lookup inside
                # add_cluster_event is guaranteed to succeed here.
                # TODO(kev): mirror this emit on AWS / GCP / Slurm
                # wait-for-instance loops.
                global_user_state.add_cluster_event(
                    cluster_name,
                    new_status=None,
                    reason=status_text,
                    event_type=global_user_state.ClusterEventType.
                    LAUNCH_PROGRESS,
                    nop_if_duplicate=True,
                )
            last_status_msg = new_status_msg
        time.sleep(1)


def _projected_serve_worker_pod_is_runtime_ready(pod: object) -> bool:
    """Whether one exact Pod reports marker-gated ray-node readiness."""
    status = getattr(pod, 'status', None)
    if getattr(status, 'phase', None) != 'Running':
        return False
    conditions = getattr(status, 'conditions', None)
    if not isinstance(conditions, (list, tuple)):
        return False
    ready_conditions = [
        condition for condition in conditions
        if getattr(condition, 'type', None) == 'Ready'
    ]
    if (len(ready_conditions) != 1 or
            getattr(ready_conditions[0], 'status', None) != 'True'):
        return False
    container_statuses = getattr(status, 'container_statuses', None)
    if not isinstance(container_statuses, (list, tuple)):
        return False
    runtime_statuses = [
        container_status for container_status in container_statuses
        if getattr(container_status, 'name', None) == 'ray-node'
    ]
    return (len(runtime_statuses) == 1 and
            getattr(runtime_statuses[0], 'ready', None) is True and
            getattr(getattr(runtime_statuses[0], 'state', None), 'running',
                    None) is not None)


def _raise_projected_runtime_readiness_failure(
    message: str,
    namespace: str,
    expected_pod_uids: Mapping[str, str],
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory | None),
) -> NoReturn:
    if provider_effect_guard_factory is not None:
        provider_resource_ids = tuple(
            f'{namespace}/{pod_name}@{pod_uid}'
            for pod_name, pod_uid in sorted(expected_pod_uids.items()))
        raise exceptions.ReservedFillProviderPresentError(
            message + ' Protocol-v2 reconciliation retains exact cleanup '
            'authority.', provider_resource_ids)
    raise config_lib.KubernetesError(message)


@timeline.event
def _wait_for_projected_serve_worker_runtime_ready(
    namespace: str,
    context: str | None,
    cluster_name: str,
    cluster_name_on_cloud: str,
    expected_pod_uids: Mapping[str, str],
    *,
    timeout: int = (pod_spec_lib.SERVE_WORKER_RUNTIME_STARTUP_TIMEOUT_SECONDS),
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
) -> None:
    """Wait for marker-gated Ready on the exact already-Running Pod UIDs."""
    if not expected_pod_uids:
        return
    deadline = time.monotonic() + timeout
    expected_names = set(expected_pod_uids)
    while True:
        try:
            pod_list = kubernetes.core_api(context).list_namespaced_pod(
                namespace,
                label_selector=(f'{constants.TAG_SKYPILOT_CLUSTER_NAME}='
                                f'{cluster_name_on_cloud}'),
                _request_timeout=kubernetes.API_TIMEOUT)
        except kubernetes.api_exception() as error:
            _raise_projected_runtime_readiness_failure(
                'Kubernetes failed to observe the exact projected worker '
                'Pods while SkyPilot waited for runtime readiness: '
                f'{common_utils.format_exception(error)}.', namespace,
                expected_pod_uids, provider_effect_guard_factory)
        observed = getattr(pod_list, 'items', None)
        if not isinstance(observed, list):
            _raise_projected_runtime_readiness_failure(
                'Kubernetes returned an invalid Pod list while SkyPilot '
                'waited for projected worker runtime readiness.', namespace,
                expected_pod_uids, provider_effect_guard_factory)
        observed_by_name = {
            getattr(getattr(pod, 'metadata', None), 'name', None): pod
            for pod in observed
            if getattr(getattr(pod, 'metadata', None), 'name', None) in
            expected_names
        }
        missing = expected_names - set(observed_by_name)
        if missing:
            _raise_projected_runtime_readiness_failure(
                'Projected SkyServe worker runtime readiness lost the exact '
                f'Pod objects {sorted(missing)!r}; SkyPilot refused to adopt '
                'a deletion or same-name replacement.', namespace,
                expected_pod_uids, provider_effect_guard_factory)
        waiting = []
        for pod_name, expected_uid in expected_pod_uids.items():
            pod = observed_by_name[pod_name]
            metadata = getattr(pod, 'metadata', None)
            actual_uid = getattr(metadata, 'uid', None)
            if actual_uid != expected_uid:
                _raise_projected_runtime_readiness_failure(
                    'Projected SkyServe worker runtime readiness observed a '
                    f'same-name replacement for {pod_name!r}: UID '
                    f'{actual_uid!r}; expected {expected_uid!r}.', namespace,
                    expected_pod_uids, provider_effect_guard_factory)
            if getattr(metadata, 'deletion_timestamp', None) is not None:
                _raise_projected_runtime_readiness_failure(
                    f'Projected SkyServe worker Pod {pod_name!r} entered '
                    'deletion before runtime readiness.', namespace,
                    expected_pod_uids, provider_effect_guard_factory)
            phase = getattr(getattr(pod, 'status', None), 'phase', None)
            if phase != 'Running':
                condensed = _condensed_pod_reason(pod)
                _raise_projected_runtime_readiness_failure(
                    f'Projected SkyServe worker Pod {pod_name!r} left '
                    f'Running before runtime readiness: {phase!r}; '
                    f'{condensed}.', namespace, expected_pod_uids,
                    provider_effect_guard_factory)
            if not _projected_serve_worker_pod_is_runtime_ready(pod):
                waiting.append(pod_name)
        if not waiting:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            _raise_projected_runtime_readiness_failure(
                f'Timed out after {timeout}s waiting for projected SkyServe '
                'worker runtime readiness on exact Pods '
                f'{sorted(waiting)!r}. Inspect the ray-node startup probe and '
                'bootstrap logs.', namespace, expected_pod_uids,
                provider_effect_guard_factory)
        rich_utils.force_update_status(
            ux_utils.spinner_message(
                'Launching (waiting for projected worker runtime bootstrap)',
                cluster_name=cluster_name))
        time.sleep(min(POLL_INTERVAL, remaining))


@timeline.event
def pre_init(namespace: str, context: str | None, new_nodes: list) -> None:
    """Pre-initialization step for SkyPilot pods.
    This step is run in the pod right after it is created and before the
    SkyPilot runtime is setup.
    This step includes three key steps:
    1. Privilege check: Checks if the default user has sufficient privilege
    to set up the kubernetes instance pod.
    2. SSH setup: Sets up SSH for the pod instance.
    3. Environment variable setup to populate k8s env vars in the pod.
    Make sure commands used in these methods are generic and work
    on most base images. E.g., do not use Python, since that may not
    be installed by default.
    If you run any apt commands, be sure to check if the lock is available.
    It is possible the `apt update` run in the pod container args may still
    be running.
    Args:
        namespace (str): Kubernetes namespace.
        context (Optional[str]): Kubernetes context.
        new_nodes (List): List of new pod instances.
    Raises:
        config_lib.KubernetesError: If user privileges are insufficient or
          setup fails.
    """

    check_k8s_user_sudo_cmd = (
        'if [ $(id -u) -eq 0 ]; then'
        # If user is root, create an alias for sudo used in skypilot setup
        '  echo \'alias sudo=""\' >> ~/.bashrc; echo succeed;'
        'else '
        '  if command -v sudo >/dev/null 2>&1; then '
        '    timeout 2 sudo -l >/dev/null 2>&1 && echo succeed || '
        f'    ( echo {exceptions.INSUFFICIENT_PRIVILEGES_CODE!r}; '
        f'      exit {exceptions.INSUFFICIENT_PRIVILEGES_CODE}; ); '
        '  else '
        f'    ( echo {exceptions.INSUFFICIENT_PRIVILEGES_CODE!r}; '
        f'      exit {exceptions.INSUFFICIENT_PRIVILEGES_CODE}; ); '
        '  fi; '
        'fi;')

    # Kubernetes automatically populates containers with critical
    # environment variables, such as those for discovering services running
    # in the cluster and CUDA/nvidia environment variables. We need to
    # make sure these env vars are available in every task and ssh session.
    # This is needed for GPU support and service discovery.
    # See https://github.com/skypilot-org/skypilot/issues/2287 for more details.
    # To do so, we capture env vars from the pod's runtime and write them to
    # /etc/profile.d/, making them available for all users in future
    # shell sessions.
    set_k8s_env_var_cmd = docker_utils.SETUP_ENV_VARS_CMD

    check_apt_update_complete_cmd = (
        'echo "Checking if apt update from container init is complete..."; '
        'timeout_secs=600; '
        'start_time=$(date +%s); '
        'while ! grep -q "Fetched" /tmp/apt-update.log 2>/dev/null; do '
        '  echo "apt update still running. Logs:"; '
        '  cat /tmp/apt-update.log || true; '
        '  current_time=$(date +%s); '
        '  elapsed=$((current_time - start_time)); '
        '  if [ $elapsed -ge $timeout_secs ]; then '
        '    echo "Timed out waiting for apt update"; '
        '    exit 1; '
        '  fi; '
        '  sleep 5; '
        'done; '
        'echo "apt update complete."; ')

    install_ssh_k8s_cmd = (
        'prefix_cmd() '
        '{ if [ $(id -u) -ne 0 ]; then echo "sudo"; else echo ""; fi; }; '
        'export DEBIAN_FRONTEND=noninteractive;'
        'echo "Installing missing packages..."; '
        'for i in {1..5}; do '
        '  output=$($(prefix_cmd) apt install openssh-server rsync -y 2>&1); '
        '  rc=$?; '
        '  if [ $rc -eq 0 ]; then '
        '    break; '
        '  fi; '
        '  echo "$output" | grep -qi "could not get lock" || '
        '  grep -qi "Unable to acquire the dpkg frontend lock"; '
        '  if [ $? -eq 0 ]; then '
        '    echo "apt install failed due to lock, retrying. (Attempt $i/5)"; '
        '    sleep 5; '
        '  else '
        '    echo "apt install failed for a non-lock reason: $output"; '
        '    exit $rc; '
        '  fi; '
        'done; '
        'if [ $rc -ne 0 ]; then '
        '    echo "apt install failed after 5 attempts due to lock errors."; '
        '    exit $rc; '
        'fi; '
        '$(prefix_cmd) mkdir -p /var/run/sshd; '
        '$(prefix_cmd) '
        'sed -i "s/PermitRootLogin prohibit-password/PermitRootLogin yes/" '
        '/etc/ssh/sshd_config; '
        '$(prefix_cmd) sed '
        '"s@session\\s*required\\s*pam_loginuid.so@session optional '
        'pam_loginuid.so@g" -i /etc/pam.d/sshd; '
        'cd /etc/ssh/ && $(prefix_cmd) ssh-keygen -A; '
        '$(prefix_cmd) mkdir -p ~/.ssh; '
        '$(prefix_cmd) chown -R $(whoami) ~/.ssh;'
        '$(prefix_cmd) chmod 700 ~/.ssh; '
        '$(prefix_cmd) cat /etc/secret-volume/ssh-publickey* > '
        '~/.ssh/authorized_keys; '
        '$(prefix_cmd) chmod 644 ~/.ssh/authorized_keys; '
        '$(prefix_cmd) service ssh restart; '
        # Eliminate the error
        # `mesg: ttyname failed: inappropriate ioctl for device`.
        # See https://www.educative.io/answers/error-mesg-ttyname-failed-inappropriate-ioctl-for-device  # pylint: disable=line-too-long
        '$(prefix_cmd) sed -i "s/mesg n/tty -s \\&\\& mesg n/" ~/.profile;')

    pre_init_cmd = ('set -ex; ' + check_k8s_user_sudo_cmd +
                    set_k8s_env_var_cmd + check_apt_update_complete_cmd +
                    install_ssh_k8s_cmd)

    def _pre_init_thread(new_node):
        pod_name = new_node.metadata.name
        logger.info(f'{"-"*20}Start: Pre-init in pod {pod_name!r} {"-"*20}')
        runner = command_runner.KubernetesCommandRunner(
            ((namespace, context), pod_name),
            container=k8s_constants.RAY_NODE_CONTAINER_NAME)

        # Run the combined pre-init command
        rc, stdout, _ = runner.run(pre_init_cmd,
                                   require_outputs=True,
                                   stream_logs=False)
        if rc == exceptions.INSUFFICIENT_PRIVILEGES_CODE:
            raise config_lib.KubernetesError(
                'Insufficient system privileges detected. '
                'Ensure the default user has root access or '
                '"sudo" is installed and the user is added to the sudoers '
                'from the image.')

        op_name = 'pre-init'
        _raise_command_running_error(op_name, pre_init_cmd, pod_name, rc,
                                     stdout)

        logger.info(f'{"-"*20}End: Pre-init in pod {pod_name!r} {"-"*20}')

    # Run pre_init in parallel across all new_nodes
    subprocess_utils.run_in_parallel(_pre_init_thread, new_nodes, _NUM_THREADS)


def _label_pod(namespace: str, context: str | None, pod_name: str,
               label: dict[str, str]) -> None:
    """Label a pod."""
    kubernetes.core_api(context).patch_namespaced_pod(
        pod_name,
        namespace, {'metadata': {
            'labels': label
        }},
        _request_timeout=kubernetes.API_TIMEOUT)


def _force_remove_terminating_pod(pod_name: str, namespace: str,
                                  context: str | None) -> None:
    """Force-removes a stuck-terminating pod so a same-named pod can be created.

    A terminating pod can block recreation with 409 ``object is being deleted``
    for two reasons, both of which this handles:
    1. Kueue keeps its ``kueue.x-k8s.io/managed`` finalizer on a pod-group pod
       until it observes a replacement; the finalizer blocks garbage-collection.
       Removing it is safe -- Kueue does not re-add it and admits the recreated
       pod as the replacement.
    2. Even with no finalizer, the object survives its
       ``terminationGracePeriodSeconds`` (for Ray pods this is the
       preemption-hook timeout, which can be minutes).

    A force-delete with grace period 0 removes the object from the API server
    before the call returns, so the caller can recreate the same name at once.
    """
    finalizers: list[str] = []
    try:
        pod = kubernetes.core_api(context).read_namespaced_pod(
            pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
        # The create response can race with a later read.  Fail closed if the
        # pod is no longer terminating: deleting it with grace period 0 would
        # otherwise remove a healthy pod.
        if pod.metadata.deletion_timestamp is None:
            raise config_lib.KubernetesError(
                f'Refusing to force-remove non-terminating pod {pod_name}.')
        finalizers = pod.metadata.finalizers or []
    except kubernetes.api_exception() as e:
        if e.status == 404:
            # Pod already gone (the goal).
            return
        # Best-effort: log and still attempt the force-delete below.
        logger.warning(f'Failed to read terminating pod {pod_name}: {e}')
    if k8s_constants.KUEUE_MANAGED_FINALIZER in finalizers:
        remaining = [
            f for f in finalizers if f != k8s_constants.KUEUE_MANAGED_FINALIZER
        ]
        # Use a JSON patch (list body), not the default strategic-merge patch:
        # a strategic-merge patch with an empty/replacement finalizers list is a
        # no-op for this field, so it would not actually remove the finalizer.
        try:
            kubernetes.core_api(context).patch_namespaced_pod(
                pod_name,
                namespace, [{
                    'op': 'replace',
                    'path': '/metadata/finalizers',
                    'value': remaining
                }],
                _request_timeout=kubernetes.API_TIMEOUT)
            logger.info(
                f'Removed Kueue finalizer from terminating pod {pod_name}.')
        except kubernetes.api_exception() as e:
            if e.status == 404:
                # Pod already gone (the goal); skip the redundant force-delete.
                return
            # Best-effort: log and still attempt the force-delete below.
            logger.warning(f'Failed to strip finalizer from terminating pod '
                           f'{pod_name}: {e}')
    # grace=0 is required: otherwise the finalizer-free object lingers for its
    # (possibly minutes-long) terminationGracePeriodSeconds.
    try:
        kubernetes.core_api(context).delete_namespaced_pod(
            pod_name,
            namespace,
            grace_period_seconds=0,
            _request_timeout=kubernetes.API_TIMEOUT)
    except kubernetes.api_exception() as e:
        if e.status != 404:
            logger.warning(
                f'Force delete of terminating pod {pod_name} failed: {e}')


def _prepare_pod_for_required_kueue(pod_spec: dict,
                                    expected_queue: str,
                                    pod_group_name: str,
                                    pod_group_total_count: int,
                                    workload_priority_class_name: str | None,
                                    *,
                                    strict_projection: bool = False) -> None:
    """Reasserts the server-owned Kueue contract on a final Pod spec."""
    metadata = pod_spec.setdefault('metadata', {})
    labels = metadata.setdefault('labels', {})
    if strict_projection:
        for key in list(labels):
            if key.startswith(k8s_constants.KUEUE_METADATA_PREFIX):
                labels.pop(key)
    labels[k8s_constants.KUEUE_QUEUE_LABEL] = expected_queue
    labels[k8s_constants.KUEUE_POD_GROUP_LABEL] = pod_group_name
    # The managed label is admission attestation.  It must never arrive in the
    # request, otherwise a custom pod_config could forge a successful check.
    labels.pop(k8s_constants.KUEUE_MANAGED_KEY, None)
    if workload_priority_class_name is None:
        labels.pop(k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL, None)
    else:
        labels[k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL] = (
            workload_priority_class_name)

    annotations = metadata.setdefault('annotations', {})
    if strict_projection:
        for key in list(annotations):
            if key.startswith(k8s_constants.KUEUE_METADATA_PREFIX):
                annotations.pop(key)
    # Kueue's WorkloadIdentifierAnnotations feature gives the annotation form
    # precedence over this canonical label identity. Keep one unambiguous
    # label-based pod group across all supported Kueue configurations.
    annotations.pop(k8s_constants.KUEUE_POD_GROUP_LABEL, None)
    annotations[k8s_constants.KUEUE_RETRIABLE_IN_GROUP_ANNOTATION] = 'false'
    annotations[k8s_constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION] = str(
        pod_group_total_count)

    # A client-supplied Kueue finalizer can leak a Pod if admission never runs.
    finalizers = metadata.get('finalizers')
    if finalizers is not None:
        metadata['finalizers'] = [
            finalizer for finalizer in finalizers
            if finalizer != k8s_constants.KUEUE_MANAGED_FINALIZER
        ]

    # Kueue adds this gate itself, but adding it before admission reverses the
    # failure mode: if the webhook is missing or excludes this namespace, the
    # unverified Pod cannot reach the default scheduler or consume a GPU.
    pod_body = pod_spec.setdefault('spec', {})
    scheduling_gates = pod_body.setdefault('schedulingGates', [])
    if strict_projection:
        scheduling_gates = []
        pod_body['schedulingGates'] = scheduling_gates
    if not any(
            gate.get('name') == k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
            for gate in scheduling_gates):
        scheduling_gates.append(
            {'name': k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE})


def _kueue_api_field(obj: Any, field: str) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(field)
    return getattr(obj, field, None)


def _get_required_kueue_api_version(context: str | None) -> str:
    """Discovers the first supported Kueue API version served by the cluster."""
    try:
        api_groups = kubernetes.apis_api(context).get_api_versions(
            _request_timeout=kubernetes.API_TIMEOUT)
    except kubernetes.api_exception() as e:
        raise config_lib.KubernetesError(
            'Failed to discover Kueue API versions: '
            f'{common_utils.format_exception(e)}. SkyPilot refused to create '
            'Pods because it cannot prove that Kueue is available.') from None

    groups = _kueue_api_field(api_groups, 'groups')
    if not isinstance(groups, list):
        raise config_lib.KubernetesError(
            'Kubernetes returned an invalid API discovery response. SkyPilot '
            'refused to create Pods because it cannot prove that Kueue is '
            'available.')
    group = next(
        (candidate for candidate in groups
         if _kueue_api_field(candidate, 'name') == k8s_constants.KUEUE_API_GROUP
        ), None)
    versions = _kueue_api_field(group, 'versions')
    if not isinstance(versions, list):
        versions = []
    served_versions = {
        version for item in versions
        if isinstance((version := _kueue_api_field(item, 'version')), str)
    }
    for version in k8s_constants.KUEUE_API_VERSIONS:
        if version in served_versions:
            return version
    raise config_lib.KubernetesError(
        f'Kubernetes does not serve a supported Kueue API version '
        f'({", ".join(k8s_constants.KUEUE_API_VERSIONS)}). SkyPilot refused '
        'to create Pods.')


def _get_required_kueue_object(*,
                               context: str | None,
                               api_version: str,
                               kind: str,
                               plural: str,
                               name: str,
                               namespace: str | None = None) -> Mapping:
    """Gets a required Kueue object from the discovered API version."""
    api = kubernetes.custom_objects_api(context)
    object_ref = f'{namespace}/{name}' if namespace is not None else name
    try:
        if namespace is None:
            obj = api.get_cluster_custom_object(
                group=k8s_constants.KUEUE_API_GROUP,
                version=api_version,
                plural=plural,
                name=name,
                _request_timeout=kubernetes.API_TIMEOUT)
        else:
            obj = api.get_namespaced_custom_object(
                group=k8s_constants.KUEUE_API_GROUP,
                version=api_version,
                namespace=namespace,
                plural=plural,
                name=name,
                _request_timeout=kubernetes.API_TIMEOUT)
    except kubernetes.api_exception() as e:
        if e.status == 404:
            raise config_lib.KubernetesError(
                f'Required Kueue {kind} {object_ref!r} does not exist in '
                f'{api_version}. Create it and wait for its current-generation '
                'Active condition to become True before launching this '
                'workload.') from None
        raise config_lib.KubernetesError(
            f'Failed to verify required Kueue {kind} {object_ref!r}: '
            f'{common_utils.format_exception(e)}. SkyPilot refused to create '
            'Pods because it cannot prove that the queue is usable.') from None

    if not isinstance(obj, Mapping):
        raise config_lib.KubernetesError(
            f'Kubernetes returned an invalid response for required Kueue '
            f'{kind} {object_ref!r}. SkyPilot refused to create Pods.')
    metadata = obj.get('metadata')
    if (not isinstance(metadata, Mapping) or metadata.get('name') != name or
        (namespace is not None and metadata.get('namespace') != namespace)):
        raise config_lib.KubernetesError(
            f'Kubernetes returned the wrong object for required Kueue '
            f'{kind} {object_ref!r}. SkyPilot refused to create Pods.')
    return obj


def _require_current_kueue_active(obj: Mapping, *, kind: str,
                                  object_ref: str) -> None:
    """Requires a reconciled, non-deleting Kueue queue object."""
    metadata = obj.get('metadata')
    if not isinstance(metadata, Mapping):
        raise config_lib.KubernetesError(
            f'Required Kueue {kind} {object_ref!r} has invalid metadata. '
            'SkyPilot refused to create Pods.')
    if metadata.get('deletionTimestamp') is not None:
        raise config_lib.KubernetesError(
            f'Required Kueue {kind} {object_ref!r} is being deleted. '
            'SkyPilot refused to create Pods.')

    status = obj.get('status')
    conditions = status.get('conditions') if isinstance(status, Mapping) else []
    active_conditions = []
    if isinstance(conditions, list):
        active_conditions = [
            condition for condition in conditions
            if isinstance(condition, Mapping) and
            condition.get('type') == k8s_constants.KUEUE_ACTIVE_CONDITION
        ]
    if not active_conditions:
        raise config_lib.KubernetesError(
            f'Required Kueue {kind} {object_ref!r} has not reported '
            'Active=True. Wait for Kueue to reconcile it before launching '
            'this workload.')
    if len(active_conditions) != 1:
        raise config_lib.KubernetesError(
            f'Required Kueue {kind} {object_ref!r} reported multiple Active '
            'conditions. SkyPilot refused to create Pods.')
    active_condition = active_conditions[0]
    if active_condition.get('status') != 'True':
        reason = active_condition.get('reason')
        message = active_condition.get('message')
        raise config_lib.KubernetesError(
            f'Required Kueue {kind} {object_ref!r} is not active '
            f'(reason={reason!r}, message={message!r}). Wait for its Active '
            'condition to become True before launching this workload.')

    generation = metadata.get('generation')
    observed_generation = active_condition.get('observedGeneration')
    if generation is None or observed_generation != generation:
        raise config_lib.KubernetesError(
            f'Required Kueue {kind} {object_ref!r} has stale Active status '
            f'(generation={generation!r}, '
            f'observedGeneration={observed_generation!r}). Wait for Kueue to '
            'reconcile the current generation before launching this workload.')


def _namespace_matches_kueue_selector(selector: Any,
                                      labels: Mapping[str, str]) -> bool:
    """Evaluates a Kubernetes metav1.LabelSelector against Namespace labels."""
    # Kueue defines a nil ClusterQueue selector as matching no namespaces. An
    # explicit empty object is the match-all selector.
    if not isinstance(selector, Mapping):
        return False
    if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in labels.items()):
        return False
    if set(selector) - {'matchLabels', 'matchExpressions'}:
        return False

    match_labels = selector.get('matchLabels', {})
    if match_labels is None:
        match_labels = {}
    if not isinstance(match_labels, Mapping):
        return False
    for key, value in match_labels.items():
        if not isinstance(key, str) or not isinstance(value, str):
            return False
        if labels.get(key) != value:
            return False

    match_expressions = selector.get('matchExpressions', [])
    if match_expressions is None:
        match_expressions = []
    if not isinstance(match_expressions, list):
        return False
    for expression in match_expressions:
        if not isinstance(expression, Mapping):
            return False
        if set(expression) - {'key', 'operator', 'values'}:
            return False
        key = expression.get('key')
        operator = expression.get('operator')
        values = expression.get('values', [])
        if not isinstance(key, str) or not isinstance(operator, str):
            return False
        if values is None:
            values = []
        if not isinstance(values, list) or not all(
                isinstance(value, str) for value in values):
            return False

        if operator == 'In':
            if not values or key not in labels or labels[key] not in values:
                return False
        elif operator == 'NotIn':
            if not values or (key in labels and labels[key] in values):
                return False
        elif operator == 'Exists':
            if values or key not in labels:
                return False
        elif operator == 'DoesNotExist':
            if values or key in labels:
                return False
        else:
            return False
    return True


def _get_required_namespace_labels(namespace: str,
                                   context: str | None) -> Mapping[str, str]:
    """Reads the exact Namespace whose labels Kueue evaluates."""
    try:
        namespace_obj = kubernetes.core_api(context).read_namespace(
            namespace, _request_timeout=kubernetes.API_TIMEOUT)
    except kubernetes.api_exception() as e:
        raise config_lib.KubernetesError(
            f'Failed to verify Namespace {namespace!r} for required Kueue '
            f'admission: {common_utils.format_exception(e)}. SkyPilot refused '
            'to create Pods because it cannot prove that the queue is '
            'usable.') from None
    metadata = getattr(namespace_obj, 'metadata', None)
    labels = getattr(metadata, 'labels', None)
    if labels is None:
        labels = {}
    if getattr(metadata, 'name',
               None) != namespace or not isinstance(labels, Mapping):
        raise config_lib.KubernetesError(
            f'Kubernetes returned an invalid response for Namespace '
            f'{namespace!r}. SkyPilot refused to create Pods.')
    return labels


def _preflight_required_kueue_local_queue(namespace: str, context: str | None,
                                          expected_queue: str) -> str:
    """Return the active ClusterQueue whose current policy admits Namespace."""
    api_version = _get_required_kueue_api_version(context)
    queue_ref = f'{namespace}/{expected_queue}'
    local_queue = _get_required_kueue_object(
        context=context,
        api_version=api_version,
        kind='LocalQueue',
        plural=k8s_constants.KUEUE_LOCAL_QUEUE_PLURAL,
        name=expected_queue,
        namespace=namespace)
    _require_current_kueue_active(local_queue,
                                  kind='LocalQueue',
                                  object_ref=queue_ref)

    local_queue_spec = local_queue.get('spec')
    cluster_queue_name = (local_queue_spec.get('clusterQueue') if isinstance(
        local_queue_spec, Mapping) else None)
    if not isinstance(cluster_queue_name,
                      str) or not cluster_queue_name.strip():
        raise config_lib.KubernetesError(
            f'Required Kueue LocalQueue {queue_ref!r} has no valid '
            'spec.clusterQueue. SkyPilot refused to create Pods.')
    if _DNS_1123_LABEL_PATTERN.fullmatch(cluster_queue_name) is None:
        raise config_lib.KubernetesError(
            f'Required Kueue LocalQueue {queue_ref!r} targets ClusterQueue '
            f'{cluster_queue_name!r}, whose name cannot be published in '
            "Kueue's AssignQueueLabelsForPods cluster-queue-name Pod label. "
            'Rename it to a DNS-1123 label (1-63 lowercase alphanumeric or '
            "'-' characters, starting and ending with an alphanumeric "
            'character). SkyPilot refused to create Pods.')

    cluster_queue = _get_required_kueue_object(
        context=context,
        api_version=api_version,
        kind='ClusterQueue',
        plural=k8s_constants.KUEUE_CLUSTER_QUEUE_PLURAL,
        name=cluster_queue_name)
    _require_current_kueue_active(cluster_queue,
                                  kind='ClusterQueue',
                                  object_ref=cluster_queue_name)
    cluster_queue_spec = cluster_queue.get('spec')
    selector = (cluster_queue_spec.get('namespaceSelector') if isinstance(
        cluster_queue_spec, Mapping) else None)
    namespace_labels = _get_required_namespace_labels(namespace, context)
    if not _namespace_matches_kueue_selector(selector, namespace_labels):
        raise config_lib.KubernetesError(
            f'Required Kueue ClusterQueue {cluster_queue_name!r} does not '
            f'admit Namespace {namespace!r} under its current '
            'spec.namespaceSelector. SkyPilot refused to create Pods.')
    return cluster_queue_name


def _attest_required_kueue_pod(
        pod: Any,
        namespace: str,
        context: str | None,
        expected_queue: str,
        expected_pod_group_name: str,
        expected_pod_group_total_count: int,
        expected_workload_priority_class_name: str | None = None,
        *,
        expected_cluster_queue: str,
        strict_projection: bool = False,
        expected_lifecycle: _RequiredKueuePodLifecycle = 'create_response',
        provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                        None) = None,
        defer_cleanup: bool = False) -> None:
    """Verifies Kueue admission mutation, deleting a Pod that bypassed it."""
    labels = pod.metadata.labels
    if not isinstance(labels, Mapping):
        labels = {}
    managed_value = labels.get(k8s_constants.KUEUE_MANAGED_KEY)
    actual_queue = labels.get(k8s_constants.KUEUE_QUEUE_LABEL)
    actual_workload_priority = labels.get(
        k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL)
    actual_pod_group_name = labels.get(k8s_constants.KUEUE_POD_GROUP_LABEL)
    annotations = pod.metadata.annotations
    if not isinstance(annotations, Mapping):
        annotations = {}
    actual_pod_group_total_count = annotations.get(
        k8s_constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION)
    actual_retriable_in_group = annotations.get(
        k8s_constants.KUEUE_RETRIABLE_IN_GROUP_ANNOTATION)
    actual_role_hash = annotations.get(k8s_constants.KUEUE_ROLE_HASH_ANNOTATION)
    role_hash_is_valid = bool(
        isinstance(actual_role_hash, str) and
        re.fullmatch(r'[0-9a-f]{8}', actual_role_hash))
    actual_podset = labels.get(k8s_constants.KUEUE_PODSET_LABEL)
    actual_workload = annotations.get(k8s_constants.KUEUE_WORKLOAD_ANNOTATION)
    actual_unconstrained_topology = annotations.get(
        k8s_constants.KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION)
    actual_local_queue = labels.get(k8s_constants.KUEUE_LOCAL_QUEUE_LABEL)
    actual_cluster_queue = labels.get(k8s_constants.KUEUE_CLUSTER_QUEUE_LABEL)
    # The webhook always stamps role-hash. PodSet/workload/queue outputs are
    # added only after Kueue admits and ungates the Workload, so they must be
    # absent from the create response or form one exact admitted set later.
    # The queue pair is mandatory after admission: without it, a LocalQueue
    # retarget between preflight and admission would be indistinguishable from
    # admission through the ClusterQueue that was actually reviewed.
    admitted_metadata_absent = all(value is None
                                   for value in (actual_podset, actual_workload,
                                                 actual_local_queue,
                                                 actual_cluster_queue))
    queue_outputs_exact = bool(actual_local_queue == expected_queue and
                               actual_cluster_queue == expected_cluster_queue)
    admitted_metadata_exact = bool(
        actual_podset == actual_role_hash and
        (actual_workload is None or
         actual_workload == expected_pod_group_name) and queue_outputs_exact)
    competing_pod_group_annotation = annotations.get(
        k8s_constants.KUEUE_POD_GROUP_LABEL)
    pod_spec = _kueue_api_field(pod, 'spec')
    scheduling_gates = _kueue_api_field(pod_spec, 'scheduling_gates')
    if scheduling_gates is None and isinstance(pod_spec, Mapping):
        scheduling_gates = pod_spec.get('schedulingGates')
    gate_names = ([] if not isinstance(scheduling_gates, (list, tuple)) else
                  [_kueue_api_field(gate, 'name') for gate in scheduling_gates])
    has_admission_gate = (k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
                          in gate_names)
    # Kueue v0.19's implicit-TAS admission path injects this annotation while
    # applying the admitted PodSet assignment.  Permit only that exact
    # server-produced value and only once the rest of the admitted identity is
    # present and the admission gate is gone.  Keeping the lifecycle check
    # here means a caller cannot smuggle the annotation into the submitted or
    # still-gated projection, and unknown future Kueue metadata remains closed.
    unconstrained_topology_output_exact = bool(
        not strict_projection or actual_unconstrained_topology is None or
        (actual_unconstrained_topology == 'true' and not has_admission_gate and
         admitted_metadata_exact))
    allowed_gate_names = {
        k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE,
        k8s_constants.KUEUE_TOPOLOGY_SCHEDULING_GATE,
    }
    unexpected_scheduling_gates = ([] if not strict_projection else [
        name for name in gate_names if name not in allowed_gate_names
    ])
    allowed_kueue_labels = {
        k8s_constants.KUEUE_MANAGED_KEY,
        k8s_constants.KUEUE_QUEUE_LABEL,
        k8s_constants.KUEUE_POD_GROUP_LABEL,
        k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL,
        k8s_constants.KUEUE_CLUSTER_QUEUE_LABEL,
        k8s_constants.KUEUE_LOCAL_QUEUE_LABEL,
        k8s_constants.KUEUE_PODSET_LABEL,
    }
    unexpected_kueue_labels = ([] if not strict_projection else sorted(
        key for key in labels
        if key.startswith(k8s_constants.KUEUE_METADATA_PREFIX) and
        key not in allowed_kueue_labels))
    allowed_kueue_annotations = {
        k8s_constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION,
        k8s_constants.KUEUE_RETRIABLE_IN_GROUP_ANNOTATION,
        k8s_constants.KUEUE_ROLE_HASH_ANNOTATION,
        k8s_constants.KUEUE_WORKLOAD_ANNOTATION,
        k8s_constants.KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION,
    }
    unexpected_kueue_annotations = ([] if not strict_projection else sorted(
        key for key in annotations
        if key.startswith(k8s_constants.KUEUE_METADATA_PREFIX) and
        key not in allowed_kueue_annotations))
    finalizers = getattr(pod.metadata, 'finalizers', None)
    has_managed_finalizer = (isinstance(finalizers, (list, tuple)) and
                             k8s_constants.KUEUE_MANAGED_FINALIZER
                             in finalizers)
    # Kueue installs its managed finalizer before quota admission, so the
    # finalizer cannot independently prove that an ungated Pod was admitted.
    # Couple lifecycle proof to the metadata phase: gated Pods remain strictly
    # pre-admission; ungated Pods need Kueue's exact PodSet admission binding.
    if expected_lifecycle == 'create_response':
        lifecycle_contract_matches = (has_admission_gate and
                                      admitted_metadata_absent)
    elif expected_lifecycle == 'adoption':
        lifecycle_contract_matches = (
            (has_admission_gate and admitted_metadata_absent) or
            (not has_admission_gate and has_managed_finalizer and
             admitted_metadata_exact))
    else:
        assert expected_lifecycle == 'admitted'
        lifecycle_contract_matches = (not has_admission_gate and
                                      has_managed_finalizer and
                                      admitted_metadata_exact)
    if (managed_value == k8s_constants.KUEUE_MANAGED_VALUE and
            actual_queue == expected_queue and actual_workload_priority
            == expected_workload_priority_class_name and
            actual_pod_group_name == expected_pod_group_name and
            actual_pod_group_total_count == str(expected_pod_group_total_count)
            and actual_retriable_in_group == 'false' and role_hash_is_valid and
            lifecycle_contract_matches and
            competing_pod_group_annotation is None and
            unconstrained_topology_output_exact and
            not unexpected_scheduling_gates and not unexpected_kueue_labels and
            not unexpected_kueue_annotations):
        return

    actual = {
        'managed_label': managed_value,
        'queue_label': actual_queue,
        'workload_priority_class_label': actual_workload_priority,
        'pod_group_label': actual_pod_group_name,
        'pod_group_total_count_annotation': actual_pod_group_total_count,
        'retriable_in_group_annotation': actual_retriable_in_group,
        'role_hash_annotation': actual_role_hash,
        'role_hash_is_valid': role_hash_is_valid,
        'podset_label': actual_podset,
        'workload_annotation': actual_workload,
        'podset_unconstrained_topology_annotation': actual_unconstrained_topology,
        'local_queue_label': actual_local_queue,
        'cluster_queue_label': actual_cluster_queue,
        'admitted_metadata_absent': admitted_metadata_absent,
        'admitted_metadata_exact': admitted_metadata_exact,
        'queue_outputs_exact': queue_outputs_exact,
        'unconstrained_topology_output_exact': unconstrained_topology_output_exact,
        'competing_pod_group_annotation': competing_pod_group_annotation,
        'unexpected_scheduling_gates': unexpected_scheduling_gates,
        'unexpected_kueue_labels': unexpected_kueue_labels,
        'unexpected_kueue_annotations': unexpected_kueue_annotations,
        'has_admission_scheduling_gate': has_admission_gate,
        'has_kueue_managed_finalizer': has_managed_finalizer,
        'lifecycle_contract_matches': lifecycle_contract_matches,
    }
    expected = {
        'managed_label': k8s_constants.KUEUE_MANAGED_VALUE,
        'queue_label': expected_queue,
        'cluster_queue_label': expected_cluster_queue,
        'workload_priority_class_label': expected_workload_priority_class_name,
        'pod_group_label': expected_pod_group_name,
        'pod_group_total_count_annotation': str(expected_pod_group_total_count),
        'retriable_in_group_annotation': 'false',
        'podset_unconstrained_topology_annotation':
            ('absent before admission; absent or the literal string "true" '
             'with exact admitted identity after admission'),
        'role_hash_annotation': '8 lowercase hexadecimal characters',
        'admitted_metadata': ('absent from create response; on adoption, '
                              'podset=role-hash, optional workload='
                              'pod-group-name, and exact local-queue/cluster-'
                              'queue outputs'),
        'competing_pod_group_annotation': None,
        'allowed_scheduling_gates': sorted(allowed_gate_names),
        'unexpected_kueue_metadata': [],
        'has_admission_scheduling_gate': True,
        'lifecycle_contract': {
            'create_response': 'pre-admission metadata plus admission scheduling gate',
            'adoption':
                ('gated pre-admission metadata, or ungated exact PodSet and '
                 'queue admission binding plus managed finalizer'),
            'admitted':
                ('ungated exact PodSet and queue admission binding plus '
                 'managed finalizer'),
        }[expected_lifecycle],
    }
    _reject_admitted_serve_worker_identity(
        pod,
        namespace,
        context,
        'Kueue admission contract',
        actual,
        expected,
        provider_effect_guard_factory=provider_effect_guard_factory,
        defer_cleanup=defer_cleanup)


@timeline.event
def _create_namespaced_pod_with_retries(
    namespace: str,
    pod_spec: dict,
    context: str | None,
    post_create_attestation: Callable[[Any], None] | None = None,
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
    persisted_pod_identity: (common.KueuePersistedPodIdentity | None) = None,
) -> Any:
    """Attempts to create a Kubernetes Pod and handle any errors.

    Currently, we handle errors due to the AppArmor annotation and retry if
    it fails due to the `FieldValueForbidden` error.
    See https://github.com/skypilot-org/skypilot/issues/4174 for details.

    Returns: The created Pod object.
    """
    if persisted_pod_identity is not None:
        _raise_persisted_kueue_pod_requires_reconciliation(
            persisted_pod_identity,
            'the Pod-create helper was reached after PostgreSQL had already '
            'bound an exact Pod UID')

    def attest_if_required(pod: Any) -> Any:
        # Run every server-owned admission check on the exact API response
        # before this create thread can return.  In particular, do not defer
        # attestation until a parallel create batch joins: an earlier Pod may
        # already be schedulable while a sibling admission request is blocked.
        if post_create_attestation is not None:
            post_create_attestation(pod)
        return pod

    def create_and_attest() -> Any:
        """Creates once and attests the response in the same authority epoch."""
        try:
            with _provider_mutation_guard(provider_effect_guard_factory):
                pod = kubernetes.core_api(context).create_namespaced_pod(
                    namespace,
                    pod_spec,
                    _request_timeout=kubernetes.API_TIMEOUT)
                return attest_if_required(pod)
        except _ServeWorkerIdentityRejection as rejection:
            # Detection happened before the create epoch ended. Cleanup gets a
            # new authorization per bounded attempt so its retry sleeps cannot
            # monopolize service/fleet authority.
            _raise_rejected_serve_worker_after_cleanup(
                rejection,
                provider_effect_guard_factory,
                persisted_pod_identity=persisted_pod_identity)

    try:
        # Attempt to create the Pod with the AppArmor annotation
        return create_and_attest()
    except kubernetes.api_exception() as e:
        try:
            error_body = json.loads(e.body)
            error_message = error_body.get('message', '')
        except json.JSONDecodeError:
            error_message = str(e.body)
        # Check if the error is due to the AppArmor annotation and retry.
        # We add an AppArmor annotation to set it as unconfined in our
        # base template in kubernetes-ray.yml.j2. This is required for
        # FUSE to work in the pod on most Kubernetes distributions.
        # However, some distributions do not support the AppArmor annotation
        # and will fail to create the pod. In this case, we retry without
        # the annotation.
        if (e.status == 422 and 'FieldValueForbidden' in error_message and
                'AppArmorProfile: nil' in error_message):
            logger.warning('AppArmor annotation caused pod creation to fail. '
                           'Retrying without the annotation. '
                           'Note: this may cause bucket mounting to fail.')

            # Remove the AppArmor annotation
            annotations = pod_spec.get('metadata', {}).get('annotations', {})
            apparmor_key = ('container.apparmor.security.beta.kubernetes.io/'
                            f'{k8s_constants.RAY_NODE_CONTAINER_NAME}')
            if apparmor_key in annotations:
                del annotations[apparmor_key]
                pod_spec['metadata']['annotations'] = annotations
                logger.info('AppArmor annotation removed from Pod spec.')
            else:
                logger.warning('AppArmor annotation not found in pod spec, '
                               'retrying will not help. '
                               f'Current annotations: {annotations}')
                raise e

            # Retry Pod creation without the AppArmor annotation
            try:
                pod = create_and_attest()
                logger.info(f'Pod {pod.metadata.name} created successfully '
                            'without AppArmor annotation.')
                return pod
            except kubernetes.api_exception() as retry_exception:
                logger.info('Failed to create Pod without AppArmor annotation: '
                            f'{retry_exception}')
                raise retry_exception
        # Unlike other error from resource lackage on CPU/GPU/Memory, TPU
        # lackage error is raised when pod is attemtped to be created.
        # TODO(Doyoung): Update the error message raised with the multi-host
        # TPU support.
        elif 'Invalid resource requests for google.com/tpu.' in error_message:
            extra_message = ('Verify if the cluster has a TPU slice node with '
                             'a topology matching the number of TPU(s) '
                             'requested. Note that multi-host TPU podslices '
                             'are currently not unsupported.')
            raise config_lib.KubernetesError(
                _lack_resource_msg('TPU',
                                   pod_spec,
                                   details=error_message,
                                   extra_msg=extra_message))
        elif (e.status == 409 and
              re.match(r'^object is being deleted: pods \".+\" already exists$',
                       error_message)):
            # Pod from a previous cluster with the same name is
            # still being deleted.
            # Extract pod name from the error message.
            # The error message is expected to match:
            # object is being deleted: pods "<podname>" already exists
            match = re.search(r'pods "([^"]+)"', error_message)
            assert match, f'Could not extract pod name from: {error_message}'
            pod_name = match.group(1)
            logger.info(
                f'Pod {pod_name} from previous cluster is still terminating. '
                'Force-removing it and retrying pod creation.')
            # Both the Kueue finalizer and the termination grace period can keep
            # the old object around; _force_remove_terminating_pod clears both.
            with _provider_mutation_guard(provider_effect_guard_factory):
                _force_remove_terminating_pod(pod_name, namespace, context)
            try:
                pod = create_and_attest()
                logger.info(f'Pod {pod.metadata.name} created successfully '
                            'after force-removing the terminating pod.')
                return pod
            except kubernetes.api_exception() as retry_exception:
                logger.warning(f'Failed to create pod {pod_name} on retry: '
                               f'{retry_exception}')
                raise retry_exception
        else:
            # Re-raise the exception if it's a different error
            raise e


@timeline.event
def _wait_for_deployment_pod(context,
                             namespace,
                             deployment,
                             timeout=300) -> list:
    label_selector = ','.join([
        f'{key}={value}'
        for key, value in deployment.spec.selector.match_labels.items()
    ])
    target_replicas = deployment.spec.replicas
    deployment_name = deployment.metadata.name
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Refresh the deployment status
        deployment = kubernetes.apps_api(
            context).read_namespaced_deployment_status(deployment_name,
                                                       namespace)
        if (deployment.status and
                deployment.status.ready_replicas is not None and
                deployment.status.ready_replicas >= target_replicas):
            pods = kubernetes.core_api(context).list_namespaced_pod(
                namespace, label_selector=label_selector).items
            return pods

        ready_replicas = (deployment.status.ready_replicas
                          if deployment.status is not None else 0)
        logger.debug(f'Waiting for deployment {deployment_name!r} to be ready. '
                     f'Ready replicas: {ready_replicas}/{target_replicas}')
        time.sleep(2)

    raise TimeoutError(
        f'Timeout: Deployment {deployment_name!r} did not become '
        'ready.')


# Preserve the historical private import seam for focused callers while the
# production mutation owner lives in pod_spec.py.
# pylint: disable=protected-access
_configure_runtime_class = pod_spec_lib._configure_runtime_class
_head_service_selector = pod_spec_lib._head_service_selector
# pylint: enable=protected-access
for _pod_spec_symbol in (_configure_runtime_class, _head_service_selector):
    _pod_spec_symbol.__module__ = __name__
del _pod_spec_symbol


def _validate_cluster_name_annotations(pods: dict[str, Any], cluster_name: str,
                                       cluster_name_on_cloud: str) -> None:
    """Refuse to adopt Pods owned by a different full cluster name."""
    for pod in pods.values():
        annotations = pod.metadata.annotations or {}
        annotated_name = annotations.get('skypilot-cluster-name')
        # Older Pods may predate this annotation. New SkyPilot Pods always set
        # it below, so a present mismatch is authoritative collision evidence.
        if isinstance(annotated_name, str) and annotated_name != cluster_name:
            raise config_lib.KubernetesError(
                'Kubernetes cluster name collision: shortened name '
                f'{cluster_name_on_cloud!r} is already owned by full cluster '
                f'{annotated_name!r}, not requested cluster {cluster_name!r}.')


_NO_SERVE_WORKER_IDENTITY_ATTESTATION = object()
_SERVE_WORKER_IDENTITY_CLEANUP_ATTEMPTS = 3


class _ServeWorkerIdentityRejection(Exception):
    """An admitted worker response failed immutable identity attestation."""

    def __init__(self, pod_name: str, namespace: str, context: str | None,
                 identity_name: str, actual: object, expected: object) -> None:
        super().__init__(identity_name)
        self.pod_name = pod_name
        self.namespace = namespace
        self.context = context
        self.identity_name = identity_name
        self.actual = actual
        self.expected = expected


def _delete_admitted_serve_worker_and_confirm_absent(
    pod_name: str,
    namespace: str,
    context: str | None,
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
) -> None:
    """Force-delete an unsafe admitted Pod and prove it left the API."""
    core_api = kubernetes.core_api(context)
    last_observation = 'the Pod remained visible'
    for attempt in range(_SERVE_WORKER_IDENTITY_CLEANUP_ATTEMPTS):
        confirmed_absent = False
        with _provider_mutation_guard(provider_effect_guard_factory):
            delete_error = None
            try:
                core_api.delete_namespaced_pod(
                    pod_name,
                    namespace,
                    grace_period_seconds=0,
                    _request_timeout=config_lib.DELETION_TIMEOUT)
            except exceptions.RequestCancelled:
                raise
            except Exception as e:  # pylint: disable=broad-except
                delete_error = common_utils.format_exception(e)
            try:
                core_api.read_namespaced_pod(
                    pod_name,
                    namespace,
                    _request_timeout=kubernetes.API_TIMEOUT)
            except exceptions.RequestCancelled:
                raise
            except kubernetes.api_exception() as e:
                if e.status == 404:
                    confirmed_absent = True
                else:
                    last_observation = common_utils.format_exception(e)
            except Exception as e:  # pylint: disable=broad-except
                last_observation = common_utils.format_exception(e)
            else:
                last_observation = ('the Pod remained visible'
                                    if delete_error is None else
                                    f'delete failed ({delete_error}) and the '
                                    'Pod remained visible')
        if confirmed_absent:
            return
        if attempt + 1 < _SERVE_WORKER_IDENTITY_CLEANUP_ATTEMPTS:
            # This is deliberately outside the provider-effect guard. A later
            # delete is a new mutation and must obtain fresh authority.
            time.sleep(POLL_INTERVAL)
    raise config_lib.KubernetesError(
        'Failed to confirm deletion of unsafe admitted SkyServe worker Pod '
        f'{namespace}/{pod_name} after '
        f'{_SERVE_WORKER_IDENTITY_CLEANUP_ATTEMPTS} attempts; last cleanup '
        f'observation: {last_observation}.')


def _raise_rejected_serve_worker_after_cleanup(
    rejection: _ServeWorkerIdentityRejection,
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
    persisted_pod_identity: (common.KueuePersistedPodIdentity | None) = None,
) -> NoReturn:
    """Cleans one rejected worker under fresh authority, then rejects it."""
    if persisted_pod_identity is not None:
        _raise_persisted_kueue_pod_requires_reconciliation(
            persisted_pod_identity,
            f'observed {rejection.identity_name} {rejection.actual!r}; '
            f'expected {rejection.expected!r}')
    cleanup_error = None
    try:
        _delete_admitted_serve_worker_and_confirm_absent(
            rejection.pod_name, rejection.namespace, rejection.context,
            provider_effect_guard_factory)
    except exceptions.RequestCancelled:
        # Losing durable authority is terminal: the stale executor must not
        # turn this into an ordinary Kubernetes failure and fail over.
        raise
    except Exception as e:  # pylint: disable=broad-except
        cleanup_error = common_utils.format_exception(e)
        logger.error(
            'Failed to confirm absence of a SkyServe worker Pod with an '
            'unexpected %s: %s', rejection.identity_name, cleanup_error)
    cleanup_detail = (' Its absence was confirmed.'
                      if cleanup_error is None else
                      ' Cleanup could not confirm Pod absence and provisioning '
                      f'will fail closed: {cleanup_error}')
    raise config_lib.KubernetesError(
        f'Admitted SkyServe worker Pod {rejection.namespace}/'
        f'{rejection.pod_name} has {rejection.identity_name} '
        f'{rejection.actual!r}; expected {rejection.expected!r}. The Pod was '
        'rejected to enforce the immutable platform placement contract.'
        f'{cleanup_detail}') from rejection


def _raise_persisted_kueue_pod_requires_reconciliation(
    persisted_pod_identity: common.KueuePersistedPodIdentity,
    detail: str,
) -> NoReturn:
    """Reject replacement of a Pod whose exact UID is durable Serve state."""
    provider_resource_id = (f'{persisted_pod_identity.namespace}/'
                            f'{persisted_pod_identity.pod_name}@'
                            f'{persisted_pod_identity.pod_uid}')
    raise exceptions.ReservedFillProviderPresentError(
        'Persisted Kueue Pod identity requires adoption-only provisioning; '
        f'{detail}. SkyPilot refused replacement mutation. Canonical '
        'protocol-v2 teardown and reconciliation retain cleanup authority.',
        (provider_resource_id,))


def _reject_admitted_serve_worker_identity(
    pod: Any,
    namespace: str,
    context: str | None,
    identity_name: str,
    actual: object,
    expected: object,
    *,
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
    defer_cleanup: bool = False,
) -> NoReturn:
    """Delete, prove absence, and reject a Pod with changed identity."""
    rejection = _ServeWorkerIdentityRejection(pod.metadata.name, namespace,
                                              context, identity_name, actual,
                                              expected)
    if defer_cleanup:
        # The caller is inside the Pod-create authorization. Raising first
        # lets that epoch close before cleanup attempts obtain fresh guards.
        raise rejection
    _raise_rejected_serve_worker_after_cleanup(rejection,
                                               provider_effect_guard_factory)


def _attest_serve_worker_priority_class(
    pod: Any,
    namespace: str,
    context: str | None,
    expected_priority_class_name: object,
    expected_priority_value: object = _NO_SERVE_WORKER_IDENTITY_ATTESTATION,
    expected_preemption_policy: object = _NO_SERVE_WORKER_IDENTITY_ATTESTATION,
    *,
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
    defer_cleanup: bool = False,
) -> None:
    """Reject a worker whose admitted priority contract is unexpected."""
    if (expected_priority_class_name is _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
        return
    actual_priority_class_name = getattr(pod.spec, 'priority_class_name', None)
    checks = [('priority class', actual_priority_class_name,
               expected_priority_class_name)]
    # Kubernetes materializes a no-class Pod as numeric priority zero and may
    # default preemptionPolicy. A null class contract attests only the absence
    # of a named class; non-null projected classes freeze all three semantics.
    if (expected_priority_class_name is not None and expected_priority_value
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
        checks.append(
            ('numeric priority', getattr(pod.spec, 'priority',
                                         None), expected_priority_value))
    if (expected_priority_class_name is not None and expected_preemption_policy
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
        checks.append(
            ('preemption policy', getattr(pod.spec, 'preemption_policy',
                                          None), expected_preemption_policy))
    for identity_name, actual, expected in checks:
        if actual != expected:
            _reject_admitted_serve_worker_identity(
                pod,
                namespace,
                context,
                identity_name,
                actual,
                expected,
                provider_effect_guard_factory=(provider_effect_guard_factory),
                defer_cleanup=defer_cleanup)


def _attest_serve_worker_service_account(pod: Any,
                                         namespace: str,
                                         context: str | None,
                                         expected_service_account_name: object,
                                         *,
                                         provider_effect_guard_factory: (
                                             common.ProviderEffectGuardFactory |
                                             None) = None,
                                         defer_cleanup: bool = False) -> None:
    """Reject a worker whose admitted namespace or account is unexpected."""
    if (expected_service_account_name is _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
        return
    actual_namespace = getattr(pod.metadata, 'namespace', None)
    if actual_namespace != namespace:
        _reject_admitted_serve_worker_identity(
            pod,
            namespace,
            context,
            'namespace',
            actual_namespace,
            namespace,
            provider_effect_guard_factory=(provider_effect_guard_factory),
            defer_cleanup=defer_cleanup)
    actual_service_account_name = getattr(pod.spec, 'service_account_name',
                                          None)
    if actual_service_account_name == expected_service_account_name:
        return
    _reject_admitted_serve_worker_identity(
        pod,
        namespace,
        context,
        'service account',
        actual_service_account_name,
        expected_service_account_name,
        provider_effect_guard_factory=(provider_effect_guard_factory),
        defer_cleanup=defer_cleanup)


def _attest_serve_worker_accelerator_scheduling(
    pod: Any,
    namespace: str,
    context: str | None,
    expected_label_key: object,
    expected_label_values: object,
    expected_resource_key: object,
    expected_accelerator_count: object,
    *,
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
    defer_cleanup: bool = False,
) -> None:
    """Reject admission mutations that weaken frozen accelerator placement."""
    if expected_label_key is _NO_SERVE_WORKER_IDENTITY_ATTESTATION:
        return
    accelerator_contract_error: str | None
    try:
        accelerator_contract = (
            pod_spec_lib.enforce_projected_accelerator_contract(
                pod.spec,
                str(expected_resource_key),
                expected_accelerator_count,
                rewrite=False))
    except pod_spec_lib.ProjectedAcceleratorContractError as error:
        accelerator_contract = None
        accelerator_contract_error = str(error)
    else:
        accelerator_contract_error = None

    affinity = getattr(pod.spec, 'affinity', None)
    node_affinity = getattr(affinity, 'node_affinity', None)
    required = getattr(node_affinity,
                       'required_during_scheduling_ignored_during_execution',
                       None)
    terms = getattr(required, 'node_selector_terms', None)
    affinity_contract_matches = False
    if isinstance(terms, list) and terms:
        affinity_contract_matches = True
        for term in terms:
            expressions = getattr(term, 'match_expressions', None)
            matching = ([] if not isinstance(expressions, list) else [
                expression for expression in expressions
                if getattr(expression, 'key', None) == expected_label_key
            ])
            if (len(matching) != 1 or
                    getattr(matching[0], 'operator', None) != 'In' or getattr(
                        matching[0], 'values', None) != expected_label_values):
                affinity_contract_matches = False
                break
    if (accelerator_contract is not None and accelerator_contract.matches and
            affinity_contract_matches):
        return
    actual = {
        'accelerator_contract_error': accelerator_contract_error,
        'ray_node_container_count':
            (None if accelerator_contract is None else
             accelerator_contract.ray_node_container_count),
        'resource_contract_matches':
            (False if accelerator_contract is None else
             accelerator_contract.ray_node_resource_contract_matches),
        'unexpected_accelerator_resources':
            ({} if accelerator_contract is None else
             accelerator_contract.unexpected_accelerator_resources),
        'dynamic_resource_claims':
            ({} if accelerator_contract is None else
             accelerator_contract.dynamic_resource_claims),
        'affinity_contract_matches': affinity_contract_matches,
    }
    expected = {
        'label_key': expected_label_key,
        'label_values': expected_label_values,
        'resource_key': expected_resource_key,
        'accelerator_count': expected_accelerator_count,
    }
    _reject_admitted_serve_worker_identity(
        pod,
        namespace,
        context,
        'accelerator scheduling contract',
        actual,
        expected,
        provider_effect_guard_factory=(provider_effect_guard_factory),
        defer_cleanup=defer_cleanup)


def _pod_scheduling_gate_names(pod: Any) -> list[object]:
    """Return the API-shape-independent scheduling-gate names for one Pod."""
    pod_spec = _kueue_api_field(pod, 'spec')
    scheduling_gates = _kueue_api_field(pod_spec, 'scheduling_gates')
    if scheduling_gates is None and isinstance(pod_spec, Mapping):
        scheduling_gates = pod_spec.get('schedulingGates')
    if not isinstance(scheduling_gates, (list, tuple)):
        return []
    return [_kueue_api_field(gate, 'name') for gate in scheduling_gates]


def _attest_serve_worker_scheduler_and_binding(
    pod: Any,
    namespace: str,
    context: str | None,
    expected_scheduler_name: object,
    expected_label_key: object,
    expected_label_values: object,
    expected_lifecycle: _RequiredKueuePodLifecycle,
    *,
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory |
                                    None) = None,
    defer_cleanup: bool = False,
) -> None:
    """Prove the projected scheduler and, when bound, the exact Node class."""
    if expected_scheduler_name is _NO_SERVE_WORKER_IDENTITY_ATTESTATION:
        return
    pod_spec = getattr(pod, 'spec', None)
    actual_scheduler_name = getattr(pod_spec, 'scheduler_name', None)
    if actual_scheduler_name != expected_scheduler_name:
        _reject_admitted_serve_worker_identity(
            pod,
            namespace,
            context,
            'scheduler',
            actual_scheduler_name,
            expected_scheduler_name,
            provider_effect_guard_factory=provider_effect_guard_factory,
            defer_cleanup=defer_cleanup)

    node_name = getattr(pod_spec, 'node_name', None)
    has_kueue_admission_gate = (k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
                                in _pod_scheduling_gate_names(pod))
    must_be_unbound = (expected_lifecycle == 'create_response' or
                       (expected_lifecycle == 'adoption' and
                        has_kueue_admission_gate))
    if must_be_unbound:
        if node_name not in (None, ''):
            _reject_admitted_serve_worker_identity(
                pod,
                namespace,
                context,
                'pre-admission node binding',
                node_name,
                None,
                provider_effect_guard_factory=provider_effect_guard_factory,
                defer_cleanup=defer_cleanup)
        return

    # An admitted Pod can remain Pending and unbound during adoption.  The
    # post-wait admitted phase, however, is the proof published as successful
    # provisioning and must be bound.
    if node_name in (None, ''):
        if expected_lifecycle == 'adoption':
            return
        _reject_admitted_serve_worker_identity(
            pod,
            namespace,
            context,
            'post-admission node binding',
            node_name,
            'a non-empty nodeName',
            provider_effect_guard_factory=provider_effect_guard_factory,
            defer_cleanup=defer_cleanup)
    if not isinstance(node_name, str):
        _reject_admitted_serve_worker_identity(
            pod,
            namespace,
            context,
            'post-admission node binding',
            node_name,
            'a non-empty nodeName',
            provider_effect_guard_factory=provider_effect_guard_factory,
            defer_cleanup=defer_cleanup)

    assert isinstance(node_name, str) and node_name
    try:
        node = kubernetes.core_api(context).read_node(
            node_name, _request_timeout=kubernetes.API_TIMEOUT)
    except exceptions.RequestCancelled:
        # Authority loss is already a typed terminal fence.  It must not be
        # converted into an identity cleanup attempted by a stale request.
        raise
    except Exception as error:  # pylint: disable=broad-except
        # Kubernetes clients surface HTTP failures as ApiException and network
        # failures through urllib3/transport exception types.  Neither is
        # placement proof; converge all ordinary read uncertainty through the
        # exact rejected-Pod cleanup seam.
        _reject_admitted_serve_worker_identity(
            pod,
            namespace,
            context,
            'bound Node accelerator identity', {
                'node_name': node_name,
                'read_error': common_utils.format_exception(error),
            }, {
                'node_name': node_name,
                'label_key': expected_label_key,
                'label_values': expected_label_values,
            },
            provider_effect_guard_factory=provider_effect_guard_factory,
            defer_cleanup=defer_cleanup)
    node_metadata = getattr(node, 'metadata', None)
    actual_node_name = getattr(node_metadata, 'name', None)
    node_labels = getattr(node_metadata, 'labels', None)
    if not isinstance(node_labels, Mapping):
        node_labels = {}
    actual_label_value = node_labels.get(expected_label_key)
    if (actual_node_name == node_name and
            isinstance(expected_label_values, list) and
            actual_label_value in expected_label_values):
        return
    _reject_admitted_serve_worker_identity(
        pod,
        namespace,
        context,
        'bound Node accelerator identity', {
            'node_name': actual_node_name,
            'label_key': expected_label_key,
            'label_value': actual_label_value,
        }, {
            'node_name': node_name,
            'label_key': expected_label_key,
            'label_values': expected_label_values,
        },
        provider_effect_guard_factory=provider_effect_guard_factory,
        defer_cleanup=defer_cleanup)


def _validate_serve_worker_scratch_contract(value: object) -> dict[str, object]:
    """Validate the persisted provider-side protocol-v3 scratch contract."""
    try:
        return pod_spec_lib.validate_projected_worker_scratch(value)
    except pod_spec_lib.ProjectedScratchContractError as error:
        raise config_lib.KubernetesError(str(error)) from error


def _attest_serve_worker_scratch(
    pod: object,
    namespace: str,
    context: str | None,
    expected_scratch: object,
    *,
    defer_cleanup: bool = False,
) -> None:
    """Reject an admitted worker whose exact v3 /tmp contract changed."""
    if expected_scratch is _NO_SERVE_WORKER_IDENTITY_ATTESTATION:
        return
    expected = _validate_serve_worker_scratch_contract(expected_scratch)
    pod_spec = (pod.get('spec') if isinstance(pod, Mapping) else getattr(
        pod, 'spec', None))
    contract = pod_spec_lib.enforce_projected_worker_scratch_contract(
        pod_spec, expected, rewrite=False)
    if contract.matches:
        return
    _reject_admitted_serve_worker_identity(pod,
                                           namespace,
                                           context,
                                           'worker scratch contract',
                                           contract.actual,
                                           contract.expected,
                                           defer_cleanup=defer_cleanup)


def _attest_serve_worker_runtime_readiness(
    pod: object,
    namespace: str,
    context: str | None,
    required: bool,
    expected_bootstrap_sha256: object,
    *,
    defer_cleanup: bool = False,
) -> None:
    """Reject a projected worker whose producer or UID probes changed."""
    if not required:
        return
    pod_spec = (pod.get('spec') if isinstance(pod, Mapping) else getattr(
        pod, 'spec', None))
    contract = (
        pod_spec_lib.enforce_projected_worker_runtime_readiness_contract(
            pod_spec,
            rewrite=False,
            expected_bootstrap_sha256=expected_bootstrap_sha256))
    if contract.matches:
        return
    _reject_admitted_serve_worker_identity(pod,
                                           namespace,
                                           context,
                                           'worker runtime-readiness contract',
                                           contract.actual,
                                           contract.expected,
                                           defer_cleanup=defer_cleanup)


def _attest_created_serve_worker_pod(
    pod: Any,
    namespace: str,
    context: str | None,
    *,
    expected_kueue_queue: str | None,
    expected_kueue_cluster_queue: str | None,
    expected_kueue_pod_group_name: str,
    expected_kueue_pod_group_total_count: int,
    expected_kueue_workload_priority_class_name: str | None,
    strict_kueue_projection: bool,
    expected_priority_class_name: object,
    expected_priority_value: object,
    expected_preemption_policy: object,
    expected_service_account_name: object,
    expected_scheduler_name: object,
    expected_accelerator_label_key: object,
    expected_accelerator_label_values: object,
    expected_accelerator_resource_key: object,
    expected_accelerator_count: object,
    expected_scratch: object,
    require_runtime_readiness: bool,
    expected_runtime_bootstrap_sha256: object,
    expected_kueue_lifecycle: _RequiredKueuePodLifecycle = 'create_response',
) -> None:
    """Attest one admitted Pod before its create thread returns."""
    if expected_kueue_queue is not None:
        assert expected_kueue_cluster_queue is not None
        _attest_required_kueue_pod(
            pod,
            namespace,
            context,
            expected_kueue_queue,
            expected_kueue_pod_group_name,
            expected_kueue_pod_group_total_count,
            expected_kueue_workload_priority_class_name,
            expected_cluster_queue=(expected_kueue_cluster_queue),
            strict_projection=strict_kueue_projection,
            expected_lifecycle=expected_kueue_lifecycle,
            defer_cleanup=True)
    _attest_serve_worker_priority_class(pod,
                                        namespace,
                                        context,
                                        expected_priority_class_name,
                                        expected_priority_value,
                                        expected_preemption_policy,
                                        defer_cleanup=True)
    _attest_serve_worker_service_account(pod,
                                         namespace,
                                         context,
                                         expected_service_account_name,
                                         defer_cleanup=True)
    _attest_serve_worker_accelerator_scheduling(
        pod,
        namespace,
        context,
        expected_accelerator_label_key,
        expected_accelerator_label_values,
        expected_accelerator_resource_key,
        expected_accelerator_count,
        defer_cleanup=True)
    _attest_serve_worker_scheduler_and_binding(
        pod,
        namespace,
        context,
        expected_scheduler_name,
        expected_accelerator_label_key,
        expected_accelerator_label_values,
        expected_kueue_lifecycle,
        defer_cleanup=True)
    _attest_serve_worker_scratch(pod,
                                 namespace,
                                 context,
                                 expected_scratch,
                                 defer_cleanup=True)
    _attest_serve_worker_runtime_readiness(pod,
                                           namespace,
                                           context,
                                           require_runtime_readiness,
                                           expected_runtime_bootstrap_sha256,
                                           defer_cleanup=True)


def _attest_pod_with_provider_guard(
    pod: Any,
    post_create_attestation: Callable[[Any], None],
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory | None),
    persisted_pod_identity: (common.KueuePersistedPodIdentity | None) = None,
) -> None:
    """Attests an existing Pod, then separately cleans any rejection."""
    try:
        with _provider_mutation_guard(provider_effect_guard_factory):
            post_create_attestation(pod)
    except _ServeWorkerIdentityRejection as rejection:
        _raise_rejected_serve_worker_after_cleanup(
            rejection,
            provider_effect_guard_factory,
            persisted_pod_identity=persisted_pod_identity)


def _wait_for_required_kueue_admission(
    namespace: str,
    context: str | None,
    pods: list[Any],
    admission_attestation: Callable[[Any], None],
    provider_effect_guard_factory: common.ProviderEffectGuardFactory | None,
    *,
    timeout: int = k8s_constants.KUEUE_ADMISSION_TIMEOUT_SECONDS,
    lane_expectation: (kueue_admission.KueuePodAdmissionExpectation |
                       None) = None,
    lane_observer: common.KueuePodAdmissionObserver | None = None,
    persisted_pod_identity: (common.KueuePersistedPodIdentity | None) = None,
) -> dict[str, str]:
    """Wait for exact required-Kueue Pods to become admitted.

    Kueue quota waiting is intentionally not charged to the ordinary Pod
    scheduling timeout. Every poll reattests the existing/adoption lifecycle;
    once the admission gate is absent that contract also proves the exact
    admitted PodSet and queue outputs. The returned UIDs bind the following
    scheduling and Running proofs to these same objects.
    """
    if not pods:
        return {}
    if (lane_expectation is None) != (lane_observer is None):
        raise config_lib.KubernetesError(
            'Kueue lane expectation and observer must be supplied together.')
    if (lane_observer is not None and
        (not callable(lane_observer) or
         not callable(getattr(lane_observer, 'begin_observation', None)))):
        raise config_lib.KubernetesError(
            'Kueue lane observer must expose callable clock-begin and commit '
            'boundaries.')

    expected_pod_uids: dict[str, str] = {}
    cluster_name_on_cloud: str | None = None
    for pod in pods:
        metadata = getattr(pod, 'metadata', None)
        initial_pod_name = getattr(metadata, 'name', None)
        pod_uid = getattr(metadata, 'uid', None)
        labels = getattr(metadata, 'labels', None)
        if (not isinstance(initial_pod_name, str) or not initial_pod_name or
                not isinstance(pod_uid, str) or not pod_uid):
            raise config_lib.KubernetesError(
                'Required Kueue admission cannot bind a Pod without an exact '
                f'non-empty name and UID (name={initial_pod_name!r}, '
                f'uid={pod_uid!r}).')
        if initial_pod_name in expected_pod_uids:
            raise config_lib.KubernetesError(
                f'Required Kueue admission received duplicate Pod name '
                f'{initial_pod_name!r}.')
        if not isinstance(labels, Mapping):
            raise config_lib.KubernetesError(
                f'Required Kueue Pod {namespace}/{initial_pod_name} has invalid '
                'metadata labels.')
        pod_cluster_name = labels.get(constants.TAG_SKYPILOT_CLUSTER_NAME)
        if (not isinstance(pod_cluster_name, str) or not pod_cluster_name or
            (cluster_name_on_cloud is not None and
             pod_cluster_name != cluster_name_on_cloud)):
            raise config_lib.KubernetesError(
                f'Required Kueue Pod {namespace}/{initial_pod_name} has an '
                'invalid SkyPilot cluster identity.')
        cluster_name_on_cloud = pod_cluster_name
        expected_pod_uids[initial_pod_name] = pod_uid

    assert cluster_name_on_cloud is not None
    expected_pod_names = set(expected_pod_uids)
    deadline = time.time() + timeout

    if lane_expectation is not None:
        assert lane_observer is not None
        if (lane_expectation.namespace != namespace or
                lane_expectation.cluster_name_on_cloud
                != cluster_name_on_cloud):
            raise config_lib.KubernetesError(
                'Kueue lane expectation does not match the exact Pod lookup '
                'scope.')

        def observe_lane_pods() -> list[tuple[
            common.KueuePodAdmissionObservation, datetime.datetime]]:
            """Clock-anchor, read, and classify exact Pods without locks."""
            observations: list[tuple[common.KueuePodAdmissionObservation,
                                     datetime.datetime]] = []
            for pod_name, expected_uid in expected_pod_uids.items():
                try:
                    # This is one unlocked database-clock read.  It must
                    # precede provider I/O so Kubernetes latency and later CAS
                    # lock contention consume (rather than mint) freshness.
                    provider_read_started_at = (
                        lane_observer.begin_observation())
                    observed_pod = kubernetes.core_api(
                        context).read_namespaced_pod(
                            pod_name,
                            namespace,
                            _request_timeout=kubernetes.API_TIMEOUT)
                except kubernetes.api_exception() as error:
                    if error.status == 404:
                        raise config_lib.KubernetesError(
                            'Required Kueue admission lost exact Pod '
                            f'{namespace}/{pod_name}@{expected_uid}; SkyPilot '
                            'refused to adopt a deletion or same-name '
                            'replacement.') from None
                    raise
                try:
                    observation = kueue_admission.classify_pod(
                        observed_pod,
                        lane_expectation,
                        expected_pod_name=pod_name,
                        expected_pod_uid=expected_uid)
                except (kueue_admission.KueuePodAdmissionClassificationError
                       ) as error:
                    # Cleanup must target the exact requested name, even if an
                    # invalid API response claimed a different metadata.name.
                    raise _ServeWorkerIdentityRejection(
                        pod_name, namespace, context, error.identity_name,
                        error.actual, error.expected) from error
                observations.append((observation, provider_read_started_at))
            return observations

        def publish_lane_observations(
            observations: list[tuple[common.KueuePodAdmissionObservation,
                                     datetime.datetime]],
        ) -> None:
            # The callback owns the PostgreSQL CAS. Invoke it only after all
            # provider reads have exited, so no SQL/advisory lock can wrap
            # Kubernetes I/O.
            for observation, provider_read_started_at in observations:
                lane_observer(observation, provider_read_started_at)

        def raise_lane_observation_failure(error: Exception,
                                           operation: str) -> NoReturn:
            if isinstance(error, exceptions.RequestCancelled):
                raise error
            if provider_effect_guard_factory is not None:
                provider_resource_ids = tuple(
                    f'{namespace}/{pod_name}@{pod_uid}'
                    for pod_name, pod_uid in sorted(expected_pod_uids.items()))
                raise exceptions.ReservedFillProviderPresentError(
                    f'Reserved-fill Kueue {operation} failed after exact Pod '
                    'materialization. Protocol-v2 reconciliation retains '
                    'cleanup authority.', provider_resource_ids) from error
            raise error

        try:
            lane_observations = observe_lane_pods()
        except _ServeWorkerIdentityRejection as rejection:
            _raise_rejected_serve_worker_after_cleanup(
                rejection,
                provider_effect_guard_factory,
                persisted_pod_identity=persisted_pod_identity)
        except Exception as error:  # pylint: disable=broad-except
            raise_lane_observation_failure(error, 'Pod observation')
        waiting_pod_names = [
            observation.pod_name
            for observation, _ in lane_observations
            if observation.state is common.KueuePodAdmissionState.POD_WAITING
        ]
        if not waiting_pod_names:
            # One final fresh read narrows an admission-to-bootstrap race. It
            # is read-only and remains outside both the provider-effect
            # advisory guard and the callback's SQL transaction; the later
            # lifecycle/identity CAS rejects a concurrent teardown or update.
            try:
                lane_observations = observe_lane_pods()
            except _ServeWorkerIdentityRejection as rejection:
                _raise_rejected_serve_worker_after_cleanup(
                    rejection,
                    provider_effect_guard_factory,
                    persisted_pod_identity=persisted_pod_identity)
            except Exception as error:  # pylint: disable=broad-except
                raise_lane_observation_failure(error, 'Pod observation')
            waiting_pod_names = [
                observation.pod_name for observation, _ in lane_observations if
                observation.state is common.KueuePodAdmissionState.POD_WAITING
            ]
        try:
            publish_lane_observations(lane_observations)
        except Exception as error:  # pylint: disable=broad-except
            raise_lane_observation_failure(error, 'receipt commit')
        if not waiting_pod_names:
            return expected_pod_uids

        # A policy-gated Pod is healthy durable provider state, not a reason to
        # retain one execution process and worker slot for hours.  The callback
        # has just committed a PostgreSQL-clock receipt for the exact Pod UID.
        # Pause this invocation so the request executor can atomically release
        # its claim and redeliver the same launch after a short delay.  Provider
        # teardown explicitly preserves ExecutionPausedError, and the replay
        # adopts and reattests this same immutable Pod before making progress.
        raise exceptions.ExecutionPausedError(
            'Required Kueue admission is still policy-gated for exact Pods '
            f'{sorted(waiting_pod_names)!r}.',
            'SkyPilot retained the Pods and will retry their exact admission '
            'observation without occupying an executor slot.',
            retry_wait_seconds=5)

    def observe_exact_pods() -> list[str]:
        """Return gated Pod names from one fresh, exact batch observation."""
        pod_list = kubernetes.core_api(context).list_namespaced_pod(
            namespace,
            label_selector=(f'{constants.TAG_SKYPILOT_CLUSTER_NAME}='
                            f'{cluster_name_on_cloud}'),
            _request_timeout=kubernetes.API_TIMEOUT)
        observed_pods = getattr(pod_list, 'items', None)
        if not isinstance(observed_pods, list):
            raise config_lib.KubernetesError(
                'Kubernetes returned an invalid Pod list while SkyPilot '
                'waited for required Kueue admission.')
        observed_by_name = {
            getattr(getattr(pod, 'metadata', None), 'name', None): pod
            for pod in observed_pods
            if getattr(getattr(pod, 'metadata', None), 'name', None) in
            expected_pod_names
        }
        missing_pod_names = expected_pod_names - set(observed_by_name)
        if missing_pod_names:
            raise config_lib.KubernetesError(
                'Required Kueue admission lost the exact Pod objects '
                f'{sorted(missing_pod_names)!r}; SkyPilot refused to adopt '
                'a deletion or same-name replacement.')

        waiting_pod_names: list[str] = []
        for pod_name, expected_pod_uid in expected_pod_uids.items():
            observed_pod = observed_by_name[pod_name]

            def attest_exact_pod(candidate: Any,
                                 *,
                                 expected_name: str = pod_name,
                                 expected_uid: str = expected_pod_uid) -> None:
                metadata = getattr(candidate, 'metadata', None)
                actual_name = getattr(metadata, 'name', None)
                actual_uid = getattr(metadata, 'uid', None)
                deletion_timestamp = getattr(metadata, 'deletion_timestamp',
                                             None)
                finalizers = getattr(metadata, 'finalizers', None)
                has_managed_finalizer = (isinstance(finalizers,
                                                    (list, tuple)) and
                                         k8s_constants.KUEUE_MANAGED_FINALIZER
                                         in finalizers)
                phase = getattr(getattr(candidate, 'status', None), 'phase',
                                None)
                identity = {
                    'name': actual_name,
                    'uid': actual_uid,
                    'deletion_timestamp': deletion_timestamp,
                    'has_kueue_managed_finalizer': has_managed_finalizer,
                    'phase': phase,
                }
                expected_identity = {
                    'name': expected_name,
                    'uid': expected_uid,
                    'deletion_timestamp': None,
                    'has_kueue_managed_finalizer': True,
                    'phase': 'Pending or Running',
                }
                if (actual_name != expected_name or
                        actual_uid != expected_uid or
                        deletion_timestamp is not None or
                        not has_managed_finalizer or
                        phase not in ('Pending', 'Running')):
                    _reject_admitted_serve_worker_identity(
                        candidate,
                        namespace,
                        context,
                        'Kueue admission Pod identity',
                        identity,
                        expected_identity,
                        defer_cleanup=True)
                admission_attestation(candidate)

            # Passive quota observation is not a provider effect. In
            # particular, do not retain or repeatedly reacquire service/fleet
            # authority while Kueue keeps this Pod scheduling-gated. A
            # rejected identity reacquires authority only for its exact
            # cleanup below, and the admitted transition is re-read under one
            # fresh authority epoch before this helper returns.
            attest_exact_pod(observed_pod)
            if (k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
                    in _pod_scheduling_gate_names(observed_pod)):
                waiting_pod_names.append(pod_name)
        return waiting_pod_names

    while True:
        try:
            waiting_pod_names = observe_exact_pods()
        except _ServeWorkerIdentityRejection as rejection:
            _raise_rejected_serve_worker_after_cleanup(
                rejection,
                provider_effect_guard_factory,
                persisted_pod_identity=persisted_pod_identity)

        if not waiting_pod_names:
            # The passive read above can race a service update or association
            # transfer. Reacquire exact request/service/fleet authority, then
            # repeat the full Pod UID and Kueue queue proof in that same
            # bounded epoch. Scheduling and post-wait publication may proceed
            # only from this admitted handoff.
            try:
                with _provider_mutation_guard(provider_effect_guard_factory):
                    waiting_pod_names = observe_exact_pods()
            except _ServeWorkerIdentityRejection as rejection:
                _raise_rejected_serve_worker_after_cleanup(
                    rejection,
                    provider_effect_guard_factory,
                    persisted_pod_identity=persisted_pod_identity)
            if not waiting_pod_names:
                return expected_pod_uids
        if time.time() >= deadline:
            timeout_message = (
                f'Timed out after {timeout}s waiting for required Kueue '
                f'admission of Pods {sorted(waiting_pod_names)!r}. The Pods '
                'remained safely scheduling-gated; this is not proof that '
                'the Kubernetes cluster lacks capacity.')
            if provider_effect_guard_factory is not None:
                # The injected guard is exclusive to protocol-v2 Kubernetes
                # reserved fill. Exact Pod creation/adoption already advanced
                # the durable association to provider I/O. Do not enter a
                # second request-owned deletion authority at this timeout: an
                # admission or owner transition can race the passive read.
                # Preserve the exact observed identities for diagnosis and
                # leave the pinned row to the canonical durable
                # PRESENT -> UID-fenced down -> ABSENT reconciliation path.
                provider_resource_ids = tuple(
                    f'{namespace}/{pod_name}@{pod_uid}'
                    for pod_name, pod_uid in sorted(expected_pod_uids.items()))
                raise exceptions.ReservedFillProviderPresentError(
                    timeout_message +
                    ' Protocol-v2 reconciliation retains cleanup authority.',
                    provider_resource_ids)
            raise config_lib.KubernetesError(timeout_message)
        time.sleep(min(POLL_INTERVAL, max(0, deadline - time.time())))


def _read_and_attest_pod_with_provider_guard(
    pod_name: str,
    expected_pod_uid: object,
    namespace: str,
    context: str | None,
    post_create_attestation: Callable[[Any], None],
    provider_effect_guard_factory: (common.ProviderEffectGuardFactory | None),
    *,
    require_runtime_readiness: bool = False,
    persisted_pod_identity: (common.KueuePersistedPodIdentity | None) = None,
) -> None:
    """Fresh-read and attest one exact publishable Pod in one authority epoch."""
    try:
        with _provider_mutation_guard(provider_effect_guard_factory):
            pod = kubernetes.core_api(context).read_namespaced_pod(
                pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
            actual_pod_name = getattr(pod.metadata, 'name', None)
            if actual_pod_name != pod_name:
                _reject_admitted_serve_worker_identity(pod,
                                                       namespace,
                                                       context,
                                                       'post-wait Pod name',
                                                       actual_pod_name,
                                                       pod_name,
                                                       defer_cleanup=True)
            actual_pod_uid = getattr(pod.metadata, 'uid', None)
            if (not isinstance(expected_pod_uid, str) or not expected_pod_uid or
                    actual_pod_uid != expected_pod_uid):
                _reject_admitted_serve_worker_identity(pod,
                                                       namespace,
                                                       context,
                                                       'post-wait Pod UID',
                                                       actual_pod_uid,
                                                       expected_pod_uid,
                                                       defer_cleanup=True)
            actual_phase = getattr(pod.status, 'phase', None)
            if actual_phase != 'Running':
                _reject_admitted_serve_worker_identity(pod,
                                                       namespace,
                                                       context,
                                                       'post-wait Pod phase',
                                                       actual_phase,
                                                       'Running',
                                                       defer_cleanup=True)
            if (require_runtime_readiness and
                    not _projected_serve_worker_pod_is_runtime_ready(pod)):
                status = getattr(pod, 'status', None)
                ready_conditions = [
                    getattr(condition, 'status', None)
                    for condition in (getattr(status, 'conditions', None) or [])
                    if getattr(condition, 'type', None) == 'Ready'
                ]
                runtime_ready = [
                    getattr(container_status, 'ready', None)
                    for container_status in (
                        getattr(status, 'container_statuses', None) or [])
                    if getattr(container_status, 'name', None) == 'ray-node'
                ]
                readiness_actual = {
                    'pod_ready_conditions': ready_conditions,
                    'ray_node_ready': runtime_ready,
                }
                readiness_expected = {
                    'pod_ready_conditions': ['True'],
                    'ray_node_ready': [True],
                }
                if provider_effect_guard_factory is not None:
                    _raise_projected_runtime_readiness_failure(
                        'Projected SkyServe worker runtime readiness '
                        'regressed during the final guarded read: '
                        f'{readiness_actual!r}; expected '
                        f'{readiness_expected!r}.', namespace,
                        {pod_name: expected_pod_uid},
                        provider_effect_guard_factory)
                _reject_admitted_serve_worker_identity(
                    pod,
                    namespace,
                    context,
                    'post-wait Pod runtime readiness',
                    readiness_actual,
                    readiness_expected,
                    defer_cleanup=True)
            post_create_attestation(pod)
    except _ServeWorkerIdentityRejection as rejection:
        _raise_rejected_serve_worker_after_cleanup(
            rejection,
            provider_effect_guard_factory,
            persisted_pod_identity=persisted_pod_identity)


@timeline.event
def _create_pods(region: str, cluster_name: str, cluster_name_on_cloud: str,
                 config: common.ProvisionConfig) -> common.ProvisionRecord:
    """Create pods based on the config."""
    provider_config = config.provider_config
    namespace = kubernetes_utils.get_namespace_from_config(provider_config)
    context = kubernetes_utils.get_context_from_config(provider_config)
    pod_spec = copy.deepcopy(config.node_config)
    create_pods_start = datetime.datetime.now(datetime.timezone.utc)

    deployment_spec: dict[str, Any] | None = None
    pvc_spec: dict[str, Any] | None = None
    to_create_deployment = 'deployment_spec' in pod_spec
    if to_create_deployment:
        deployment_spec = pod_spec.pop('deployment_spec')
        pvc_spec = pod_spec.pop('pvc_spec')

    kueue_local_queue_name = provider_config.get('kueue_local_queue_name')
    # Provider configs are persisted and may outlive the renderer that created
    # them.  Derive strict mode from the queue again at the final provisioning
    # boundary so a missing or false flag can never downgrade a queued Pod.
    kueue_require_managed = bool(
        provider_config.get('kueue_require_managed', False) or
        kueue_local_queue_name)
    kueue_workload_priority_class_name = provider_config.get(
        'kueue_workload_priority_class_name')
    lane_runtime = config.kueue_admission_runtime
    if lane_runtime is not None and not isinstance(
            lane_runtime, common.KueuePodAdmissionRuntime):
        raise exceptions.ReservedFillLaunchFenceError(
            'Kueue lane observation requires one complete typed runtime.')
    lane_identity = None if lane_runtime is None else lane_runtime.identity
    lane_accelerator = (None
                        if lane_runtime is None else lane_runtime.accelerator)
    lane_observer = None if lane_runtime is None else lane_runtime.observer
    persisted_pod_identity = (None if lane_runtime is None else
                              lane_runtime.persisted_pod_identity)
    if (lane_runtime is not None and
            not isinstance(lane_identity, common.KueuePodAdmissionIdentity)):
        raise config_lib.KubernetesError(
            'Kueue lane runtime has an invalid admission identity.')
    if (persisted_pod_identity is not None and not isinstance(
            persisted_pod_identity, common.KueuePersistedPodIdentity)):
        raise exceptions.ReservedFillLaunchFenceError(
            'Persisted Kueue Pod identity must use the typed runtime contract.')
    if (lane_runtime is not None and
        (not isinstance(lane_accelerator, str) or not lane_accelerator)):
        raise config_lib.KubernetesError(
            'Kueue lane accelerator must be a non-empty string.')
    if (lane_runtime is not None and
        (not callable(lane_observer) or
         not callable(getattr(lane_observer, 'begin_observation', None)))):
        raise config_lib.KubernetesError(
            'Kueue lane observer must expose callable clock-begin and commit '
            'boundaries.')
    if lane_runtime is not None and config.count != 1:
        raise config_lib.KubernetesError(
            'Kueue lane observation requires exactly one Pod.')
    if persisted_pod_identity is not None:
        expected_pod_name = f'{cluster_name_on_cloud}-head'
        if (persisted_pod_identity.namespace != namespace or
                persisted_pod_identity.pod_name != expected_pod_name):
            _raise_persisted_kueue_pod_requires_reconciliation(
                persisted_pod_identity,
                'the rendered single-Pod cluster namespace or name changed')
    serve_worker_projection_protocol_version = provider_config.get(
        'serve_worker_projection_protocol_version')
    try:
        pod_spec_lib.validate_serve_worker_projection_protocol_version(
            serve_worker_projection_protocol_version, allow_none=True)
    except ValueError as error:
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker projection protocol version must be '
            '1, 2, 3, 4, or absent.') from error
    strict_kueue_projection = (
        pod_spec_lib.serve_worker_projection_protocol_has_strict_admission(
            serve_worker_projection_protocol_version))
    if (strict_kueue_projection and kueue_require_managed and
            config.provider_effect_guard_factory is not None and
            lane_runtime is None):
        raise config_lib.KubernetesError(
            'Protocol-v2 reserved-fill Kueue provisioning requires the exact '
            'lane identity, accelerator, and durable observation callback.')
    require_runtime_readiness = (
        pod_spec_lib.serve_worker_projection_protocol_has_runtime_readiness(
            serve_worker_projection_protocol_version))
    if (kueue_workload_priority_class_name is not None and
        (not isinstance(kueue_workload_priority_class_name, str) or
         not kueue_workload_priority_class_name)):
        raise config_lib.KubernetesError(
            'The rendered Kueue WorkloadPriorityClass must be a non-empty '
            'string or null.')
    serve_worker_expected_priority_class_name = provider_config.get(
        'serve_worker_expected_priority_class_name',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_priority_value = provider_config.get(
        'serve_worker_expected_priority_value',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_preemption_policy = provider_config.get(
        'serve_worker_expected_preemption_policy',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_service_account_name = provider_config.get(
        'serve_worker_expected_service_account_name',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_scheduler_name = provider_config.get(
        'serve_worker_expected_scheduler_name',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_accelerator_label_key = provider_config.get(
        'serve_worker_expected_accelerator_label_key',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_accelerator_label_values = provider_config.get(
        'serve_worker_expected_accelerator_label_values',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_accelerator_resource_key = provider_config.get(
        'serve_worker_expected_accelerator_resource_key',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_accelerator_count = provider_config.get(
        'serve_worker_expected_accelerator_count',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_scratch = provider_config.get(
        'serve_worker_expected_scratch', _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    serve_worker_expected_runtime_bootstrap_sha256 = provider_config.get(
        'serve_worker_expected_runtime_bootstrap_sha256',
        _NO_SERVE_WORKER_IDENTITY_ATTESTATION)
    if pod_spec_lib.serve_worker_projection_protocol_has_scratch(
            serve_worker_projection_protocol_version):
        if (serve_worker_expected_scratch
                is _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
            raise config_lib.KubernetesError(
                'Projection protocol v3/v4 requires the complete worker '
                'scratch attestation contract.')
        serve_worker_expected_scratch = (
            _validate_serve_worker_scratch_contract(
                serve_worker_expected_scratch))
    elif (serve_worker_expected_scratch
          is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
        raise config_lib.KubernetesError(
            'Only projection protocol v3/v4 may carry a worker scratch '
            'attestation contract.')
    if require_runtime_readiness:
        if (serve_worker_expected_runtime_bootstrap_sha256
                is _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
            raise config_lib.KubernetesError(
                'Projection protocol v4 requires the complete worker runtime '
                'bootstrap SHA256 contract.')
        try:
            serve_worker_expected_runtime_bootstrap_sha256 = (
                pod_spec_lib.validate_projected_worker_runtime_bootstrap_sha256(
                    serve_worker_expected_runtime_bootstrap_sha256))
        except pod_spec_lib.ProjectedRuntimeReadinessContractError as error:
            raise config_lib.KubernetesError(str(error)) from error
    elif (serve_worker_expected_runtime_bootstrap_sha256
          is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
        raise config_lib.KubernetesError(
            'Only projection protocol v4 may carry a worker runtime '
            'bootstrap SHA256 contract.')
    priority_attestation_presence = tuple(
        value is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION
        for value in (serve_worker_expected_priority_class_name,
                      serve_worker_expected_priority_value,
                      serve_worker_expected_preemption_policy))
    if any(priority_attestation_presence
          ) and not all(priority_attestation_presence):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker priority attestation must include '
            'class name, numeric value, and preemption policy together.')
    accelerator_attestation_values = (
        serve_worker_expected_accelerator_label_key,
        serve_worker_expected_accelerator_label_values,
        serve_worker_expected_accelerator_resource_key,
        serve_worker_expected_accelerator_count)
    accelerator_attestation_presence = tuple(
        value is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION
        for value in accelerator_attestation_values)
    if any(accelerator_attestation_presence
          ) and not all(accelerator_attestation_presence):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker accelerator attestation must '
            'include label key, label values, resource key, and count '
            'together.')
    if strict_kueue_projection and (not all(priority_attestation_presence) or
                                    serve_worker_expected_service_account_name
                                    is _NO_SERVE_WORKER_IDENTITY_ATTESTATION or
                                    serve_worker_expected_scheduler_name
                                    is _NO_SERVE_WORKER_IDENTITY_ATTESTATION or
                                    not all(accelerator_attestation_presence)):
        raise config_lib.KubernetesError(
            'A strict projection protocol requires the complete priority, '
            'service account, scheduler, and accelerator attestation contract.')
    kueue_cluster_queue_name: str | None = None
    if kueue_require_managed:
        if not kueue_local_queue_name:
            raise config_lib.KubernetesError(
                'Required Kueue management is enabled, but the rendered '
                'provider config has no LocalQueue name.')
        if to_create_deployment:
            raise config_lib.KubernetesError(
                'Required Kueue management currently supports direct Pods, '
                'not high-availability Deployment-owned Pods.')
        kueue_cluster_queue_name = _preflight_required_kueue_local_queue(
            namespace, context, kueue_local_queue_name)
    if (serve_worker_expected_priority_class_name
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION and
            serve_worker_expected_priority_class_name is not None and
        (not isinstance(serve_worker_expected_priority_class_name, str) or
         not serve_worker_expected_priority_class_name)):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker expected priority class must be a '
            'non-empty string or null.')
    if (serve_worker_expected_priority_value
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION and
        (serve_worker_expected_priority_value is not None and
         (type(serve_worker_expected_priority_value) is not int or
          serve_worker_expected_priority_value < -2147483648 or
          serve_worker_expected_priority_value > 1000000000))):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker expected numeric priority must be '
            'a Kubernetes priority integer or null.')
    if (serve_worker_expected_preemption_policy
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION and
            serve_worker_expected_preemption_policy
            not in (None, 'Never', 'PreemptLowerPriority')):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker expected preemption policy must be '
            'Never, PreemptLowerPriority, or null.')
    if (serve_worker_expected_priority_class_name
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION and
        ((serve_worker_expected_priority_class_name
          is None) != (serve_worker_expected_priority_value is None) or
         (serve_worker_expected_priority_class_name
          is None) != (serve_worker_expected_preemption_policy is None))):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker priority class, numeric value, and '
            'preemption policy must be configured together.')
    if (serve_worker_expected_service_account_name
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION and
        (not isinstance(serve_worker_expected_service_account_name, str) or
         not serve_worker_expected_service_account_name)):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker expected service account must be a '
            'non-empty string.')
    if (serve_worker_expected_scheduler_name
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION and
        (not isinstance(serve_worker_expected_scheduler_name, str) or
         not serve_worker_expected_scheduler_name)):
        raise config_lib.KubernetesError(
            'The rendered SkyServe worker expected scheduler must be a '
            'non-empty string.')
    if all(accelerator_attestation_presence):
        if (not isinstance(serve_worker_expected_accelerator_label_key, str) or
                not serve_worker_expected_accelerator_label_key or
                not isinstance(serve_worker_expected_accelerator_resource_key,
                               str) or
                not serve_worker_expected_accelerator_resource_key or
                not isinstance(serve_worker_expected_accelerator_label_values,
                               list) or
                not serve_worker_expected_accelerator_label_values or
                len(serve_worker_expected_accelerator_label_values) > 16 or
                any(not isinstance(value, str) or not value
                    for value in serve_worker_expected_accelerator_label_values)
                or len(set(serve_worker_expected_accelerator_label_values))
                != len(serve_worker_expected_accelerator_label_values) or
                type(serve_worker_expected_accelerator_count) is not int or
                serve_worker_expected_accelerator_count < 1):
            raise config_lib.KubernetesError(
                'The rendered SkyServe worker accelerator attestation is '
                'invalid.')

    lane_expectation: (kueue_admission.KueuePodAdmissionExpectation |
                       None) = None
    if lane_runtime is not None:
        if (not kueue_require_managed or not strict_kueue_projection or
                kueue_cluster_queue_name is None):
            raise config_lib.KubernetesError(
                'Kueue lane observation requires one strict projected worker '
                'with non-null Kueue admission.')
        assert isinstance(lane_identity, common.KueuePodAdmissionIdentity)
        assert isinstance(lane_accelerator, str)
        assert lane_observer is not None
        assert isinstance(kueue_local_queue_name, str)
        assert isinstance(serve_worker_expected_service_account_name, str)
        assert isinstance(serve_worker_expected_scheduler_name, str)
        assert isinstance(serve_worker_expected_accelerator_label_key, str)
        assert isinstance(serve_worker_expected_accelerator_label_values, list)
        assert isinstance(serve_worker_expected_accelerator_resource_key, str)
        assert isinstance(serve_worker_expected_accelerator_count, int)
        assert (serve_worker_expected_priority_class_name is None or
                isinstance(serve_worker_expected_priority_class_name, str))
        assert (serve_worker_expected_priority_value is None or
                isinstance(serve_worker_expected_priority_value, int))
        assert (serve_worker_expected_preemption_policy is None or
                isinstance(serve_worker_expected_preemption_policy, str))
        lane_expectation = kueue_admission.KueuePodAdmissionExpectation(
            namespace=namespace,
            cluster_name_on_cloud=cluster_name_on_cloud,
            local_queue_name=kueue_local_queue_name,
            cluster_queue_name=kueue_cluster_queue_name,
            workload_priority_class_name=(kueue_workload_priority_class_name),
            pod_group_total_count=config.count,
            priority_class_name=serve_worker_expected_priority_class_name,
            priority_value=serve_worker_expected_priority_value,
            preemption_policy=serve_worker_expected_preemption_policy,
            service_account_name=(serve_worker_expected_service_account_name),
            scheduler_name=serve_worker_expected_scheduler_name,
            accelerator=lane_accelerator,
            accelerator_label_key=(serve_worker_expected_accelerator_label_key),
            accelerator_label_values=tuple(
                serve_worker_expected_accelerator_label_values),
            accelerator_resource_key=(
                serve_worker_expected_accelerator_resource_key),
            accelerator_count=serve_worker_expected_accelerator_count,
            identity=lane_identity)

    post_create_attestation: Callable[[Any], None] | None = None
    existing_pod_attestation: Callable[[Any], None] | None = None
    post_wait_pod_attestation: Callable[[Any], None] | None = None
    if (kueue_require_managed or serve_worker_expected_priority_class_name
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION or
            serve_worker_expected_service_account_name
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION or
            serve_worker_expected_scheduler_name
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION or
            serve_worker_expected_accelerator_label_key
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION or
            serve_worker_expected_scratch
            is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION or
            require_runtime_readiness):

        def attest_pod(
            pod: Any,
            *,
            expected_kueue_lifecycle: _RequiredKueuePodLifecycle,
        ) -> None:
            _attest_created_serve_worker_pod(
                pod,
                namespace,
                context,
                expected_kueue_queue=(kueue_local_queue_name
                                      if kueue_require_managed else None),
                expected_kueue_cluster_queue=(kueue_cluster_queue_name if
                                              kueue_require_managed else None),
                expected_kueue_pod_group_name=cluster_name_on_cloud,
                expected_kueue_pod_group_total_count=config.count,
                expected_kueue_workload_priority_class_name=(
                    kueue_workload_priority_class_name),
                strict_kueue_projection=strict_kueue_projection,
                expected_priority_class_name=(
                    serve_worker_expected_priority_class_name),
                expected_priority_value=serve_worker_expected_priority_value,
                expected_preemption_policy=(
                    serve_worker_expected_preemption_policy),
                expected_service_account_name=(
                    serve_worker_expected_service_account_name),
                expected_scheduler_name=serve_worker_expected_scheduler_name,
                expected_accelerator_label_key=(
                    serve_worker_expected_accelerator_label_key),
                expected_accelerator_label_values=(
                    serve_worker_expected_accelerator_label_values),
                expected_accelerator_resource_key=(
                    serve_worker_expected_accelerator_resource_key),
                expected_accelerator_count=(
                    serve_worker_expected_accelerator_count),
                expected_scratch=serve_worker_expected_scratch,
                require_runtime_readiness=require_runtime_readiness,
                expected_runtime_bootstrap_sha256=(
                    serve_worker_expected_runtime_bootstrap_sha256),
                expected_kueue_lifecycle=expected_kueue_lifecycle)

        def attest_created_pod(pod: Any) -> None:
            attest_pod(pod, expected_kueue_lifecycle='create_response')

        def attest_existing_pod(pod: Any) -> None:
            attest_pod(pod, expected_kueue_lifecycle='adoption')

        def attest_admitted_pod(pod: Any) -> None:
            attest_pod(pod, expected_kueue_lifecycle='admitted')

        post_create_attestation = attest_created_pod
        existing_pod_attestation = attest_existing_pod
        post_wait_pod_attestation = attest_admitted_pod

    tags = ray_tag_filter(cluster_name_on_cloud)

    pod_spec['metadata']['namespace'] = namespace
    if 'labels' in pod_spec['metadata']:
        pod_spec['metadata']['labels'].update(tags)
    else:
        pod_spec['metadata']['labels'] = tags
    pod_spec['metadata']['labels'].update(
        {constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud})
    # Add the cluster name as an annotation to the pod spec.
    # We cannot use a label because label values have both
    # a length limit and charset limit (i.e no special chars).
    # Annotations are not subject to these limits.
    # This annotation is used to identify the cluster name from the pod
    pod_spec['metadata'].setdefault('annotations', {}).update({
        'skypilot-cluster-name': cluster_name,
    })

    ephemeral_volumes = provider_config.get('ephemeral_volume_infos')
    if ephemeral_volumes:
        for ephemeral_volume in ephemeral_volumes:
            # Update the volumes and volume mounts in the pod spec
            if 'volumes' not in pod_spec['spec']:
                pod_spec['spec']['volumes'] = []
            pod_spec['spec']['volumes'].append({
                'name': ephemeral_volume.name,
                'persistentVolumeClaim': {
                    'claimName': ephemeral_volume.volume_name_on_cloud,
                },
            })
            if 'volumeMounts' not in pod_spec['spec']['containers'][0]:
                pod_spec['spec']['containers'][0]['volumeMounts'] = []
            pod_spec['spec']['containers'][0]['volumeMounts'].append({
                'name': ephemeral_volume.name,
                'mountPath': ephemeral_volume.path,
            })

    # Docker sidecar cache volume injection: if a SkyPilot volume was
    # specified for the enable_docker cache, look up the PVC name. The actual
    # volume + volumeMount are added per-pod inside _create_resource_thread (so
    # that each pod can have its own subPath).
    raw_docker_config = provider_config.get('docker_config')
    docker_config: kubernetes_utils.DockerConfig | None = None
    if raw_docker_config:
        docker_config = kubernetes_utils.DockerConfig.from_dict(
            raw_docker_config)
    docker_pvc_name: str | None = None
    if docker_config and docker_config.cache_volume:
        cache_vol_name = docker_config.cache_volume
        vol_record = global_user_state.get_volume_by_name(cache_vol_name)
        if vol_record is None:
            raise exceptions.VolumeNotFoundError(
                f'Docker cache volume {cache_vol_name!r} not found.')
        docker_pvc_name = vol_record['handle'].name_on_cloud

    terminating_pods = kubernetes_utils.filter_pods(namespace, context, tags,
                                                    ['Terminating'])
    _validate_cluster_name_annotations(terminating_pods, cluster_name,
                                       cluster_name_on_cloud)
    if terminating_pods and persisted_pod_identity is not None:
        _raise_persisted_kueue_pod_requires_reconciliation(
            persisted_pod_identity, 'a tagged Pod is terminating '
            f'({sorted(terminating_pods)!r})')
    start_time = time.time()
    while (terminating_pods and
           time.time() - start_time < _TIMEOUT_FOR_POD_TERMINATION):
        logger.debug(f'run_instances: Found {len(terminating_pods)} '
                     'terminating pods. Waiting them to finish: '
                     f'{list(terminating_pods.keys())}')
        time.sleep(POLL_INTERVAL)
        terminating_pods = kubernetes_utils.filter_pods(namespace, context,
                                                        tags, ['Terminating'])
        _validate_cluster_name_annotations(terminating_pods, cluster_name,
                                           cluster_name_on_cloud)

    if terminating_pods:
        # If there are still terminating pods, we force delete them.
        logger.debug(f'run_instances: Found {len(terminating_pods)} '
                     'terminating pods still in terminating state after '
                     f'timeout {_TIMEOUT_FOR_POD_TERMINATION}s. '
                     'Force deleting them.')
        for pod_name in terminating_pods.keys():
            # grace_period_seconds=0 means force delete the pod.
            # https://github.com/kubernetes-client/python/issues/508#issuecomment-1695759777
            with common.provider_effect_guard(config):
                kubernetes.core_api(context).delete_namespaced_pod(
                    pod_name,
                    namespace,
                    _request_timeout=config_lib.DELETION_TIMEOUT,
                    grace_period_seconds=0)

    # Clean up pods in Failed/Succeeded phase from previous runs.
    # These are invisible to the Pending/Running filter below but still
    # block pod creation with the same name (409 AlreadyExists).
    stale_pods = kubernetes_utils.filter_pods(namespace, context, tags,
                                              ['Failed', 'Succeeded'])
    _validate_cluster_name_annotations(stale_pods, cluster_name,
                                       cluster_name_on_cloud)
    if stale_pods and persisted_pod_identity is not None:
        _raise_persisted_kueue_pod_requires_reconciliation(
            persisted_pod_identity, 'a tagged Pod is terminal '
            f'({sorted(stale_pods)!r})')
    if stale_pods:
        logger.info(f'Found {len(stale_pods)} pods in Failed/Succeeded '
                    f'phase: {list(stale_pods.keys())}. Deleting them.')
        for pod_name in stale_pods:

            def delete_stale_pod(name: str = pod_name) -> None:
                # Enter per attempt so delete retry backoff never monopolizes
                # launch authority.
                with common.provider_effect_guard(config):
                    kubernetes.core_api(context).delete_namespaced_pod(
                        name,
                        namespace,
                        _request_timeout=config_lib.DELETION_TIMEOUT,
                        grace_period_seconds=0)

            kubernetes_utils.delete_k8s_resource_with_retry(
                delete_func=delete_stale_pod,
                resource_type='pod',
                resource_name=pod_name)

    if persisted_pod_identity is not None:
        try:
            with _provider_mutation_guard(config.provider_effect_guard_factory):
                persisted_pod = kubernetes.core_api(
                    context).read_namespaced_pod(
                        persisted_pod_identity.pod_name,
                        namespace,
                        _request_timeout=kubernetes.API_TIMEOUT)
        except exceptions.RequestCancelled:
            raise
        except Exception as error:  # pylint: disable=broad-except
            _raise_persisted_kueue_pod_requires_reconciliation(
                persisted_pod_identity,
                'the exact persisted Pod could not be re-read '
                f'({common_utils.format_exception(error)})')
        metadata = getattr(persisted_pod, 'metadata', None)
        actual_name = getattr(metadata, 'name', None)
        actual_uid = getattr(metadata, 'uid', None)
        deletion_timestamp = getattr(metadata, 'deletion_timestamp', None)
        actual_phase = getattr(getattr(persisted_pod, 'status', None), 'phase',
                               None)
        if (actual_name != persisted_pod_identity.pod_name or
                actual_uid != persisted_pod_identity.pod_uid or
                deletion_timestamp is not None or
                actual_phase not in ('Pending', 'Running')):
            _raise_persisted_kueue_pod_requires_reconciliation(
                persisted_pod_identity,
                'the exact persisted Pod is absent, replaced, terminating, '
                'or terminal; observed '
                f'name={actual_name!r}, uid={actual_uid!r}, '
                f'deletion_timestamp={deletion_timestamp!r}, '
                f'phase={actual_phase!r}')
        running_pods = {persisted_pod_identity.pod_name: persisted_pod}
    else:
        running_pods = kubernetes_utils.filter_pods(namespace, context, tags,
                                                    ['Pending', 'Running'])
    _validate_cluster_name_annotations(running_pods, cluster_name,
                                       cluster_name_on_cloud)
    if persisted_pod_identity is not None:
        assert len(running_pods) == 1
    if existing_pod_attestation is not None:
        for existing_pod in running_pods.values():
            # Pre-receipt reattestation may delete an unsafe Pod. Once its UID
            # is durable, rejection remains observation-only and canonical
            # reconciliation owns cleanup.
            _attest_pod_with_provider_guard(
                existing_pod,
                existing_pod_attestation,
                config.provider_effect_guard_factory,
                persisted_pod_identity=persisted_pod_identity)
    head_pod_name = _get_head_pod_name(running_pods)
    running_pod_statuses = [{
        pod.metadata.name: pod.status.phase
    } for pod in running_pods.values()]
    logger.debug(f'Found {len(running_pods)} existing pods: '
                 f'{running_pod_statuses}')

    to_start_count = config.count - len(running_pods)
    if to_start_count < 0:
        raise RuntimeError(
            'The number of running+pending pods '
            f'({config.count - to_start_count}) in cluster '
            f'"{cluster_name_on_cloud}" is greater than the number '
            f'requested by the user ({config.count}). '
            'This is likely a resource leak. '
            'Use "sky down" to terminate the cluster.')

    # Add nvidia runtime class if it exists
    nvidia_runtime_exists = False
    try:
        nvidia_runtime_exists = kubernetes_utils.check_nvidia_runtime_class(
            context=context)
    except kubernetes.kubernetes.client.ApiException as e:
        logger.warning('run_instances: Error occurred while checking for '
                       f'nvidia RuntimeClass - '
                       f'{common_utils.format_exception(e)}'
                       'Continuing without using nvidia RuntimeClass.\n'
                       'If you are on a K3s cluster, manually '
                       'override runtimeClassName in ~/.sky/config.yaml. '
                       'For more details, refer to https://docs.skypilot.co/en/latest/reference/config.html')  # pylint: disable=line-too-long

    needs_gpus = False
    needs_gpus_nvidia = False
    gpu_resource_key = kubernetes_utils.SUPPORTED_GPU_RESOURCE_KEYS['nvidia']
    if all(accelerator_attestation_presence):
        # Projection v2 owns the whole Pod and may place the sole ray-node
        # container after one or more sidecars. Its authenticated resource is
        # therefore the source for runtime/toleration selection; container[0]
        # is neither an identity nor necessarily accelerator-bearing.
        assert isinstance(serve_worker_expected_accelerator_resource_key, str)
        assert isinstance(serve_worker_expected_accelerator_count, int)
        gpu_resource_key = serve_worker_expected_accelerator_resource_key
        needs_gpus = serve_worker_expected_accelerator_count > 0
        needs_gpus_nvidia = (gpu_resource_key == kubernetes_utils.
                             SUPPORTED_GPU_RESOURCE_KEYS['nvidia'])
    else:
        limits = pod_spec['spec']['containers'][0].get('resources',
                                                       {}).get('limits')
        if limits is not None:
            gpu_resource_key = kubernetes_utils.get_gpu_resource_key(context)
            needs_gpus = limits.get(gpu_resource_key, 0) > 0
            needs_gpus_nvidia = limits.get(
                kubernetes_utils.SUPPORTED_GPU_RESOURCE_KEYS['nvidia'], 0) > 0

    tpu_label = kubernetes_utils.GKELabelFormatter.TPU_LABEL_KEY
    needs_tpu = tpu_label in config.node_config.get('spec',
                                                    {}).get('nodeSelector', {})

    # Resolve allowed_nodes once before parallel Pod construction.  This is the
    # only part of the policy that may list provider nodes; the pure finalizer
    # receives the resulting full base affinity.
    resolved_base_affinity = None
    if to_start_count > 0:
        resolved_spec = copy.deepcopy(pod_spec['spec'])
        allowed_nodes_config = kubernetes_utils.get_allowed_nodes_config(
            context)
        kubernetes_utils.inject_allowed_nodes_affinity(resolved_spec,
                                                       allowed_nodes_config,
                                                       context=context)
        resolved_base_affinity = resolved_spec.get('affinity')

    logger.debug(f'run_instances: calling create_namespaced_pod '
                 f'(count={to_start_count}).')

    def _create_resource_thread(i: int):
        # 0 is for head pod, while 1+ is for worker pods.
        if i == 0:
            if head_pod_name is None:
                # First pod should be head if no head exists
                role: pod_spec_lib.PodRole = 'head'
                pod_name = f'{cluster_name_on_cloud}-head'
            else:
                # If head pod already exists, we skip creating it.
                return
        else:
            # Worker pods
            role = 'worker'
            pod_name = f'{cluster_name_on_cloud}-worker{i}'
            if pod_name in running_pods:
                # If the pod is already running, we skip creating it.
                return

        deployment_name = None
        if to_create_deployment:
            assert deployment_spec is not None
            deployment_name = deployment_spec['metadata']['name']
        pod_spec_copy = pod_spec_lib.finalize_pod_spec(
            pod_spec,
            role=role,
            pod_name=pod_name,
            cluster_name_on_cloud=cluster_name_on_cloud,
            node_count=config.count,
            nvidia_runtime_exists=nvidia_runtime_exists,
            needs_gpus=needs_gpus,
            needs_gpus_nvidia=needs_gpus_nvidia,
            gpu_resource_key=gpu_resource_key,
            needs_tpu=needs_tpu,
            resolved_base_affinity=resolved_base_affinity,
            docker_config=docker_config,
            docker_pvc_name=docker_pvc_name,
            context=context,
            namespace=namespace,
            deployment_name=deployment_name,
        )

        if (serve_worker_expected_scratch
                is not _NO_SERVE_WORKER_IDENTITY_ATTESTATION):
            assert isinstance(serve_worker_expected_scratch, dict)
            scratch_contract = (
                pod_spec_lib.enforce_projected_worker_scratch_contract(
                    pod_spec_copy['spec'],
                    serve_worker_expected_scratch,
                    rewrite=False))
            if not scratch_contract.matches:
                raise config_lib.KubernetesError(
                    'The finalized SkyServe worker Pod changed the immutable '
                    'scratch contract: '
                    f'{scratch_contract.actual!r}; expected '
                    f'{scratch_contract.expected!r}.')
        if require_runtime_readiness:
            runtime_readiness_contract = (
                pod_spec_lib.
                enforce_projected_worker_runtime_readiness_contract(
                    pod_spec_copy['spec'],
                    rewrite=False,
                    expected_bootstrap_sha256=(
                        serve_worker_expected_runtime_bootstrap_sha256)))
            if not runtime_readiness_contract.matches:
                raise config_lib.KubernetesError(
                    'The finalized SkyServe worker Pod changed the immutable '
                    'runtime-readiness contract: '
                    f'{runtime_readiness_contract.actual!r}; expected '
                    f'{runtime_readiness_contract.expected!r}.')

        if kueue_require_managed:
            assert kueue_local_queue_name is not None
            _prepare_pod_for_required_kueue(
                pod_spec_copy,
                expected_queue=kueue_local_queue_name,
                pod_group_name=cluster_name_on_cloud,
                pod_group_total_count=config.count,
                workload_priority_class_name=(
                    kueue_workload_priority_class_name),
                strict_projection=strict_kueue_projection,
            )
        if lane_identity is not None:
            # Dynamic intent identity is installed after every caller/workspace
            # merge and after static projection rendering.  Any collision is a
            # caller attempt to occupy a server-owned field and fails closed.
            try:
                kueue_admission.install_dynamic_identity_annotations(
                    pod_spec_copy, lane_identity)
            except (TypeError, ValueError) as error:
                raise config_lib.KubernetesError(str(error)) from error

        if to_create_deployment:
            assert deployment_spec is not None
            assert pvc_spec is not None
            with common.provider_effect_guard(config):
                volume.create_persistent_volume_claim(namespace, context,
                                                      pvc_spec)

            # It's safe to directly modify the template spec in the deployment spec
            # because controller pod is singleton, i in [0].
            template_pod_spec = deployment_spec['spec']['template']
            template_pod_spec['metadata'] = pod_spec_copy['metadata']
            template_pod_spec['spec'].update(pod_spec_copy['spec'])
            # Propagate the labels to the deployment for identification.
            deployment_spec['metadata']['labels'] = pod_spec_copy['metadata'][
                'labels']
            try:
                with common.provider_effect_guard(config):
                    return kubernetes.apps_api(
                        context).create_namespaced_deployment(
                            namespace, deployment_spec)
            except Exception as e:
                print('Deployment failed', e)
                raise e

        # Check if any PVCs with access mode ReadWriteOnce or ReadWriteOncePod
        # is used by any pod in the namespace.
        volume.check_pvc_usage_for_pod(context, namespace, pod_spec_copy)

        # Keep every create/retry response and its immediate admission
        # attestation in one fresh authority epoch. Internal retries reacquire;
        # passive scheduling/readiness waits below hold no authority guard.
        create_kwargs: dict[str, Any] = {
            'post_create_attestation': post_create_attestation,
            'provider_effect_guard_factory':
                (config.provider_effect_guard_factory),
        }
        if persisted_pod_identity is not None:
            create_kwargs['persisted_pod_identity'] = persisted_pod_identity
        return _create_namespaced_pod_with_retries(namespace, pod_spec_copy,
                                                   context, **create_kwargs)

    if not to_start_count:
        is_provisioned_cluster_ha = is_high_availability_cluster_by_kubectl(
            cluster_name_on_cloud, context, namespace)
        if is_provisioned_cluster_ha != to_create_deployment:
            ha_str = lambda x: 'high availability' if x else 'non-high availability'

            message = (
                f'The cluster "{cluster_name_on_cloud}" is configured to be '
                f'{ha_str(to_create_deployment)} but the cluster has already been '
                f'provisioned as {ha_str(is_provisioned_cluster_ha)}. '
                'If you want to make the provisioned cluster '
                f'{ha_str(to_create_deployment)}, please first down the cluster '
                'and then up the cluster again.')
            raise exceptions.InconsistentHighAvailabilityError(message)

    created_resources = []
    if to_start_count > 0:
        # Create pods in parallel.
        # Use `config.count` instead of `to_start_count` to keep the index of
        # the Pods consistent especially for the case where some Pods are down
        # due to node failure or manual termination, etc. and then launch
        # again to create the Pods back.
        # The existing Pods will be skipped in _create_resource_thread.
        created_resources = subprocess_utils.run_in_parallel(
            _create_resource_thread, list(range(config.count)), _NUM_THREADS)

    if to_create_deployment:
        deployments = copy.deepcopy(created_resources)
        pods = [
            pod for deployment in deployments
            for pod in _wait_for_deployment_pod(context, namespace, deployment)
        ]
    else:
        # If not creating deployments, 'created_resources' already holds Pod objects
        pods = created_resources

    created_pods = {}
    valid_pods = []
    for pod in pods:
        # In case Pod is not created
        if pod is None:
            continue
        valid_pods.append(pod)
        created_pods[pod.metadata.name] = pod
        if head_pod_name is None and _is_head(pod):
            head_pod_name = pod.metadata.name
    pods = valid_pods
    if to_create_deployment and existing_pod_attestation is not None:
        # Deployment-owned Pods do not pass through the direct Pod-create seam.
        # Required Kueue already rejects this topology above; preserve the
        # existing identity checks for other Deployment-based launches.
        for admitted_pod in pods:
            _attest_pod_with_provider_guard(
                admitted_pod, existing_pod_attestation,
                config.provider_effect_guard_factory)

    # The running_pods may include Pending Pods, so we add them to the pods
    # list to wait for scheduling and running
    if running_pods:
        pods = pods + list(running_pods.values())

    provision_timeout = provider_config['timeout']

    wait_str = ('indefinitely'
                if provision_timeout < 0 else f'for {provision_timeout}s')
    logger.debug(f'run_instances: waiting {wait_str} for pods to schedule and '
                 f'run: {[pod.metadata.name for pod in pods]}')

    admitted_kueue_pod_uids: dict[str, str] | None = None
    scheduling_wait_start = create_pods_start
    if kueue_require_managed:
        assert existing_pod_attestation is not None
        logger.debug(
            'run_instances: waiting up to %ss for required Kueue admission '
            'before starting the configured scheduling timeout (%s): %s',
            k8s_constants.KUEUE_ADMISSION_TIMEOUT_SECONDS, wait_str,
            [pod.metadata.name for pod in pods])
        admitted_kueue_pod_uids = _wait_for_required_kueue_admission(
            namespace,
            context,
            pods,
            existing_pod_attestation,
            config.provider_effect_guard_factory,
            lane_expectation=lane_expectation,
            lane_observer=lane_observer,
            persisted_pod_identity=persisted_pod_identity)
        # Autoscaler and scheduling-error evidence from while Kueue retained
        # the admission gate cannot have been caused by these Pods. Start that
        # observation window together with the fresh scheduling deadline.
        scheduling_wait_start = datetime.datetime.now(datetime.timezone.utc)

    # Wait until the pods are scheduled and surface cause for error
    # if there is one. Required-Kueue quota waiting completed above, so this
    # call starts a fresh configured scheduling deadline.
    _wait_for_pods_to_schedule(namespace, context, pods, provision_timeout,
                               cluster_name, scheduling_wait_start)
    # Reset spinner message here because it might have hinted autoscaling
    # while waiting for pods to schedule.
    rich_utils.force_update_status(
        ux_utils.spinner_message('Launching', cluster_name=cluster_name))
    # Wait until the pods and their containers are up and running, and
    # fail early if there is an error
    logger.debug(f'run_instances: waiting for pods to be running: '
                 f'{[pod.metadata.name for pod in pods]}')
    projected_expected_pod_uids: dict[str, str] | None = None
    if require_runtime_readiness:
        if admitted_kueue_pod_uids is not None:
            projected_expected_pod_uids = dict(admitted_kueue_pod_uids)
        else:
            projected_expected_pod_uids = {}
            for pod in pods:
                pod_name = getattr(getattr(pod, 'metadata', None), 'name', None)
                pod_uid = getattr(getattr(pod, 'metadata', None), 'uid', None)
                if (not isinstance(pod_name, str) or not pod_name or
                        not isinstance(pod_uid, str) or not pod_uid or
                        pod_name in projected_expected_pod_uids):
                    _raise_projected_runtime_readiness_failure(
                        'Projected SkyServe worker runtime readiness requires '
                        'one exact non-empty Pod name and UID per worker.',
                        namespace, projected_expected_pod_uids,
                        config.provider_effect_guard_factory)
                projected_expected_pod_uids[pod_name] = pod_uid
    running_pod_uids = _wait_for_pods_to_run(namespace, context, cluster_name,
                                             pods)
    # Reset spinner message here because it might have hinted the reason
    # pods were pending.
    rich_utils.force_update_status(
        ux_utils.spinner_message('Launching', cluster_name=cluster_name))
    logger.debug(f'run_instances: all pods are scheduled and running: '
                 f'{[pod.metadata.name for pod in pods]}')

    if projected_expected_pod_uids is not None:
        if running_pod_uids != projected_expected_pod_uids:
            _raise_projected_runtime_readiness_failure(
                'Projected SkyServe worker Running observation changed the '
                'exact Pod UID set; SkyPilot refused to publish provisioning '
                'success.', namespace, projected_expected_pod_uids,
                config.provider_effect_guard_factory)
        _wait_for_projected_serve_worker_runtime_ready(
            namespace,
            context,
            cluster_name,
            cluster_name_on_cloud,
            projected_expected_pod_uids,
            provider_effect_guard_factory=(
                config.provider_effect_guard_factory))

    if post_wait_pod_attestation is not None:
        # The create response proves the webhook installed the closed,
        # pre-admission contract. Scheduling can take arbitrarily long, during
        # which a LocalQueue could be retargeted. Re-read every exact Pod only
        # after it is Running and require the admitted identity (including the
        # preflight ClusterQueue) before publishing provisioning success.
        expected_pod_uids = (projected_expected_pod_uids
                             if projected_expected_pod_uids is not None else
                             (running_pod_uids if admitted_kueue_pod_uids
                              is None else admitted_kueue_pod_uids))
        expected_pod_names = set(expected_pod_uids)
        if set(running_pod_uids) != expected_pod_names:
            raise config_lib.KubernetesError(
                'The Kubernetes Running wait did not return exact identity '
                'proof for every expected Pod. SkyPilot refused to publish '
                'provisioning success.')
        for pod_name, pod_uid in expected_pod_uids.items():
            _read_and_attest_pod_with_provider_guard(
                pod_name,
                pod_uid,
                namespace,
                context,
                post_wait_pod_attestation,
                config.provider_effect_guard_factory,
                require_runtime_readiness=require_runtime_readiness,
                persisted_pod_identity=persisted_pod_identity)

    assert head_pod_name is not None, 'head_instance_id should not be None'
    return common.ProvisionRecord(
        provider_name='kubernetes',
        region=region,
        zone=None,
        cluster_name=cluster_name_on_cloud,
        head_instance_id=head_pod_name,
        resumed_instance_ids=[],
        created_instance_ids=list(created_pods.keys()),
    )


def run_instances(region: str, cluster_name: str, cluster_name_on_cloud: str,
                  config: common.ProvisionConfig) -> common.ProvisionRecord:
    """Runs instances for the given cluster."""
    try:
        return _create_pods(region, cluster_name, cluster_name_on_cloud, config)
    except (kubernetes.api_exception(), config_lib.KubernetesError) as e:
        e_msg = common_utils.format_exception(e)
        logger.warning('run_instances: Error occurred when creating pods:\n'
                       f'{e_msg}')
        raise


def wait_instances(region: str, cluster_name_on_cloud: str,
                   state: status_lib.ClusterStatus | None) -> None:
    del region, cluster_name_on_cloud, state


def stop_instances(
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any] | None = None,
    worker_only: bool = False,
) -> None:
    raise NotImplementedError()


def _delete_services(name_prefix: str,
                     namespace: str,
                     context: str | None,
                     skip_ssh_service: bool = False) -> None:
    """Delete services with the given name prefix.

    Args:
        name_prefix: Prefix of the service names to delete
        namespace: Kubernetes namespace
        context: Kubernetes context
    """
    # TODO(andy): We should use tag for the service filter.
    services = ([name_prefix, f'{name_prefix}-ssh']
                if not skip_ssh_service else [name_prefix])
    for service_name in services:
        # Since we are not saving this lambda, it's a false positive.
        # TODO(andyl): Wait for
        # https://github.com/pylint-dev/pylint/issues/5263.
        # pylint: disable=cell-var-from-loop
        kubernetes_utils.delete_k8s_resource_with_retry(
            delete_func=lambda: kubernetes.core_api(context).
            delete_namespaced_service(
                name=service_name,  # noqa: B023
                namespace=namespace,
                _request_timeout=config_lib.DELETION_TIMEOUT),
            resource_type='service',
            resource_name=service_name)


def _delete_cluster_services(cluster_name: str, namespace: str,
                             context: str | None) -> None:
    """Delete all services associated with a cluster using label selector.

    This is a fallback cleanup mechanism that works even when pods have been
    deleted externally. Services are identified by the skypilot-cluster-name
    label.

    Args:
        cluster_name: The cluster name used in the skypilot-cluster-name label
        namespace: Kubernetes namespace
        context: Kubernetes context
    """
    label_selector = f'{constants.TAG_SKYPILOT_CLUSTER_NAME}={cluster_name}'
    try:
        kubernetes.core_api(context).delete_collection_namespaced_service(
            namespace,
            label_selector=label_selector,
            _request_timeout=config_lib.DELETION_TIMEOUT)
    except kubernetes.api_exception() as e:
        logger.warning(f'Failed to cleanup services for cluster '
                       f'{cluster_name}: {e}')


def _terminate_node(namespace: str,
                    context: str | None,
                    pod_name: str,
                    is_head: bool = False) -> None:
    """Terminate a pod and its associated services."""
    logger.debug(f'terminate_instances: namespace: {namespace}, context: '
                 f'{context}, pod_name: {pod_name}, is_head: {is_head}')

    if is_head:
        # Delete services for the head pod
        # services are specified in sky/templates/kubernetes-ray.yml.j2
        _delete_services(pod_name, namespace, context)
    else:
        # No ssh service is created for worker pods
        _delete_services(pod_name, namespace, context, skip_ssh_service=True)

    # Note - delete pod after all other resources are deleted.
    # This is to ensure there are no leftover resources if this down is run
    # from within the pod, e.g., for autodown.
    # Note - some misbehaving pods may not terminate gracefully if they have
    # open file descriptors. We force delete pods to avoid this.
    kubernetes_utils.delete_k8s_resource_with_retry(
        delete_func=lambda: kubernetes.core_api(context).delete_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            _request_timeout=config_lib.DELETION_TIMEOUT,
            grace_period_seconds=0),
        resource_type='pod',
        resource_name=pod_name)


def _terminate_deployment(cluster_name: str, namespace: str,
                          context: str | None) -> None:
    """Terminate a deployment."""
    # Delete services first
    _delete_services(f'{cluster_name}-head', namespace, context)

    # Delete deployment
    deployment_name = _get_deployment_name(cluster_name)
    kubernetes_utils.delete_k8s_resource_with_retry(
        delete_func=lambda: kubernetes.apps_api(
            context).delete_namespaced_deployment(name=deployment_name,
                                                  namespace=namespace,
                                                  _request_timeout=config_lib.
                                                  DELETION_TIMEOUT),
        resource_type='deployment',
        resource_name=deployment_name)

    # Delete PVCs
    pvc_name = _get_pvc_name(
        cluster_name,
        kubernetes_utils.HIGH_AVAILABILITY_DEPLOYMENT_VOLUME_MOUNT_NAME)
    # pylint: disable=cell-var-from-loop
    kubernetes_utils.delete_k8s_resource_with_retry(
        delete_func=lambda: kubernetes.core_api(
            context).delete_namespaced_persistent_volume_claim(
                name=pvc_name,
                namespace=namespace,
                _request_timeout=config_lib.DELETION_TIMEOUT),
        resource_type='pvc',
        resource_name=pvc_name)


def terminate_instances(
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any],
    worker_only: bool = False,
) -> None:
    """See sky/provision/__init__.py"""
    namespace = kubernetes_utils.get_namespace_from_config(provider_config)
    context = kubernetes_utils.get_context_from_config(provider_config)
    pods = kubernetes_utils.filter_pods(namespace, context,
                                        ray_tag_filter(cluster_name_on_cloud),
                                        None)

    if is_high_availability_cluster_by_kubectl(cluster_name_on_cloud, context,
                                               namespace):
        # For high availability controllers, terminate the deployment
        logger.debug(f'Terminating deployment {cluster_name_on_cloud}')
        _terminate_deployment(cluster_name_on_cloud, namespace, context)
        return

    def _terminate_pod_thread(pod_info):
        pod_name, pod = pod_info
        if _is_head(pod) and worker_only:
            return
        logger.debug(f'Terminating instance {pod_name}: {pod}')
        _terminate_node(namespace, context, pod_name, _is_head(pod))

    # Run pod termination in parallel
    subprocess_utils.run_in_parallel(_terminate_pod_thread, list(pods.items()),
                                     _NUM_THREADS)

    if not worker_only:
        # Cleanup all services by label selector as a fallback.
        # This handles the case where pods were deleted externally.
        # Only do this when terminating the entire cluster, not when
        # terminating workers only (head services should remain).
        _delete_cluster_services(cluster_name_on_cloud, namespace, context)


def cleanup_cluster_resources(
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any],
) -> None:
    """Cleanup Kubernetes resources for a cluster.

    This function is called during post-teardown cleanup to ensure all cluster
    resources are deleted even when pods were deleted externally. It uses label
    selectors to find and delete resources, making it resilient to external
    deletions.

    Args:
        cluster_name_on_cloud: The cluster name on cloud
        provider_config: Provider configuration dictionary
    """
    namespace = kubernetes_utils.get_namespace_from_config(provider_config)
    context = kubernetes_utils.get_context_from_config(provider_config)
    _delete_cluster_services(cluster_name_on_cloud, namespace, context)


# The probe runs as soon as ray-installation finishes in step 2 of the
# pod bootstrap, typically within tens of seconds of the pod going
# Running. 60s gives the common case plenty of slack without pinning
# every status refresh during a pod restart to a multi-minute hang.
_HOST_NETWORK_SSHD_WAIT_TIMEOUT_S = 60
_HOST_NETWORK_SSHD_WAIT_INTERVAL_S = 2


def _read_host_network_sshd_ports(cluster_name_on_cloud: str, namespace: str,
                                  context: str | None,
                                  expected_pods: list[str]) -> dict[str, int]:
    """Read each pod's probed sshd port from the hostNetwork ConfigMap.

    Polls until every entry in ``expected_pods`` is present (or the
    timeout elapses); returning partial state would freeze every
    subsequent SSH at port 22 until the next refresh.
    """
    if not expected_pods:
        return {}
    name = host_network_probe.ray_ports_configmap_name(cluster_name_on_cloud)
    expected = set(expected_pods)
    deadline = time.monotonic() + _HOST_NETWORK_SSHD_WAIT_TIMEOUT_S
    while True:
        out: dict[str, int] = {}
        try:
            cm = kubernetes.core_api(context).read_namespaced_config_map(
                name=name, namespace=namespace)
        except kubernetes.api_exception() as e:
            if e.status != 404:
                raise
            cm = None
        data = (cm.data or {}) if cm is not None else {}
        for key, value in data.items():
            if not key.startswith(host_network_probe.SSHD_KEY_PREFIX):
                continue
            podname = common_utils.removeprefix(
                key, host_network_probe.SSHD_KEY_PREFIX)
            try:
                out[podname] = int(value)
            except ValueError:
                logger.warning(
                    f'ConfigMap {namespace}/{name} has non-integer value '
                    f'for {key!r}: {value!r}. SSH to {podname!r} will '
                    f'fall back to port 22 and hit the K8s node\'s sshd.')
        if expected.issubset(out.keys()):
            return out
        if time.monotonic() >= deadline:
            missing = sorted(expected - out.keys())
            logger.warning(
                f'hostNetwork sshd ports for {missing} did not appear '
                f'in ConfigMap {namespace}/{name} within '
                f'{_HOST_NETWORK_SSHD_WAIT_TIMEOUT_S}s — `ssh <cluster>` '
                f'to those pods will fail until the next '
                f'`sky status -r`.')
            return out
        time.sleep(_HOST_NETWORK_SSHD_WAIT_INTERVAL_S)


def get_cluster_info(
        region: str,
        cluster_name_on_cloud: str,
        provider_config: dict[str, Any] | None = None) -> common.ClusterInfo:
    del region  # unused
    assert provider_config is not None
    namespace = kubernetes_utils.get_namespace_from_config(provider_config)
    context = kubernetes_utils.get_context_from_config(provider_config)

    running_pods = kubernetes_utils.filter_pods(
        namespace, context, ray_tag_filter(cluster_name_on_cloud), ['Running'])
    logger.debug(f'Running pods: {list(running_pods.keys())}')

    pods: dict[str, list[common.InstanceInfo]] = {}
    head_pod_name = None

    port = 22
    if not provider_config.get('use_internal_ips', False):
        port = kubernetes_utils.get_head_ssh_port(cluster_name_on_cloud,
                                                  namespace, context)

    # Each hostNetwork pod's sshd binds a probed port (host:22 is the
    # K8s node's own sshd). The SSH config writer needs that port per
    # pod, so wait for every hostNetwork pod's entry to land in the
    # ConfigMap before caching the result.
    host_network_pods = [
        name for name, pod in running_pods.items() if pod.spec.host_network
    ]
    pod_sshd_ports = _read_host_network_sshd_ports(cluster_name_on_cloud,
                                                   namespace, context,
                                                   host_network_pods)

    head_pod_name = None
    cpu_request = None
    for pod_name, pod in running_pods.items():
        # Under hostNetwork the pod's network namespace is the host's, so
        # pod_ip is the K8s node's host IP. SkyPilot injects a required
        # per-cluster podAntiAffinity for hostNetwork clusters, so every
        # pod of a cluster is on its own node and thus has a distinct,
        # routable host IP — no per-pod loopback disambiguation needed.
        internal_ip = pod.status.pod_ip
        # Get the k8s node name the pod is running on (for dashboard display)
        k8s_node_name = getattr(pod.spec, 'node_name', None)
        pods[pod_name] = [
            common.InstanceInfo(
                instance_id=pod_name,
                internal_ip=internal_ip,
                external_ip=None,
                ssh_port=pod_sshd_ports.get(pod_name, port),
                tags=pod.metadata.labels,
                # TODO(hailong): `cluster.local` may need to be configurable
                # Service name is same as the pod name for now.
                internal_svc=f'{pod_name}.{namespace}.svc.cluster.local',
                node_name=k8s_node_name,
            )
        ]
        if _is_head(pod):
            head_pod_name = pod_name
            head_spec = pod.spec
            assert head_spec is not None, pod
            primary_container = kubernetes_utils.get_pod_primary_container(pod)
            resources = getattr(primary_container, 'resources', None)
            requests = (getattr(resources, 'requests', None)
                        if resources else None)
            limits = (getattr(resources, 'limits', None) if resources else None)
            cpu_request = ((requests or {}).get('cpu') or
                           (limits or {}).get('cpu'))

    if cpu_request is None:
        raise RuntimeError(f'Pod {cluster_name_on_cloud}-head not found'
                           ' or not Running, check the Pod status')

    ssh_user = 'sky'
    # Use pattern matching to extract SSH user, handling MOTD contamination.
    # Some container images (like CUDA-Q) print MOTD when login shells start,
    # which can contaminate command output. We use a unique pattern to extract
    # the actual username reliably.
    get_k8s_ssh_user_cmd = 'echo "SKYPILOT_SSH_USER: $(whoami)"'
    assert head_pod_name is not None
    runner = command_runner.KubernetesCommandRunner(
        ((namespace, context), head_pod_name),
        container=k8s_constants.RAY_NODE_CONTAINER_NAME)
    rc, stdout, stderr = runner.run(get_k8s_ssh_user_cmd,
                                    require_outputs=True,
                                    separate_stderr=True,
                                    stream_logs=False)
    _raise_command_running_error('get ssh user', get_k8s_ssh_user_cmd,
                                 head_pod_name, rc, stdout + stderr)

    # Extract SSH user using pattern matching
    ssh_user_match = _SSH_USER_PATTERN.search(stdout)
    if ssh_user_match:
        ssh_user = ssh_user_match.group(1)
    else:
        raise ValueError('Failed to find SSH user identifier: '
                         f'{stdout + stderr}')
    logger.debug(
        f'Using ssh user {ssh_user} for cluster {cluster_name_on_cloud}')

    # cpu_request may be a string like `100m`, need to parse and convert
    num_cpus = kubernetes_utils.parse_cpu_or_gpu_resource_to_float(cpu_request)
    # 'num-cpus' for ray must be an integer, but we should not set it to 0 if
    # cpus is <1.
    # Keep consistent with the logic in clouds/kubernetes.py
    str_cpus = str(max(int(num_cpus), 1))

    return common.ClusterInfo(
        instances=pods,
        head_instance_id=head_pod_name,
        ssh_user=ssh_user,
        # We manually set object-store-memory=500000000 to avoid ray from
        # allocating a very large object store in each pod that may cause
        # problems for other pods.
        custom_ray_options={
            'object-store-memory': 500000000,
            'num-cpus': str_cpus,
        },
        provider_name='kubernetes',
        provider_config=provider_config)


def _check_nodes_health(
    context: str | None,
    node_names: set[str],
) -> dict[str, str]:
    """Check health of specific Kubernetes nodes.

    Tries the NodeInfoSource plugin first (fast, cached), then falls back
    to direct Kubernetes API calls.

    Args:
        context: Kubernetes context name.
        node_names: Set of node names to check.

    Returns:
        Dict mapping node_name -> issue description for unhealthy nodes.
        Healthy nodes are omitted.
    """
    if not node_names:
        return {}

    issues: dict[str, str] = {}

    # Try NodeInfoSource plugin first (node-info-service sidecar).
    # get() safely returns None when no provider is registered.
    # Note: if a node is in node_names but not in the cache, it's silently
    # skipped (we don't fall back to the k8s API for missing entries). This
    # is acceptable since this is diagnostic-only and doesn't affect the
    # cluster status transition.
    node_info = plugin_extensions.NodeInfoSource.get(
        context) if context is not None else None
    if node_info is not None:
        for name in node_names:
            info = node_info.node_info_dict.get(name)
            if info is None:
                continue
            if not info.is_ready:
                issues[name] = 'NotReady'
            elif info.is_cordoned:
                issues[name] = 'cordoned'
        return issues

    # Fallback: direct Kubernetes API (parallelized)
    def _check_single_node(name: str) -> tuple[str, str] | None:
        try:
            node = kubernetes.core_api(context).read_node(
                name, _request_timeout=kubernetes.API_TIMEOUT)
            # Check NotReady first (more severe than cordoned)
            node_status = getattr(node, 'status', None)
            for condition in (getattr(node_status, 'conditions', None) or []):
                if condition.type == 'Ready' and condition.status != 'True':
                    return (name, 'NotReady')
            # Check if node is cordoned (unschedulable)
            node_spec = getattr(node, 'spec', None)
            if getattr(node_spec, 'unschedulable', False):
                return (name, 'cordoned')
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Failed to read node {name}: {e}')
        return None

    results = subprocess_utils.run_in_parallel(_check_single_node,
                                               sorted(node_names))
    for result in results:
        if result is not None:
            issues[result[0]] = result[1]

    return issues


def get_node_health_for_cluster(
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any],
    unhealthy_pod_names: list[str],
) -> dict[str, pod_diagnostics.NodeHealthInfo]:
    """Check node health for specific unhealthy pods in a cluster.

    Fetches pods to determine which nodes they run on, then checks
    those nodes' health via NodeInfoSource or the Kubernetes API.

    Args:
        cluster_name_on_cloud: The cluster name as known to the cloud.
        provider_config: The provider config from the cluster YAML.
        unhealthy_pod_names: Pod names that have health issues.

    Returns:
        Dict mapping node_name -> NodeHealthInfo for unhealthy nodes.
    """
    namespace = kubernetes_utils.get_namespace_from_config(provider_config)
    context = kubernetes_utils.get_context_from_config(provider_config)
    is_ssh = context.startswith('ssh-') if context else False
    identity = 'SSH Node Pool' if is_ssh else 'Kubernetes cluster'
    label_selector = (f'{constants.TAG_SKYPILOT_CLUSTER_NAME}='
                      f'{cluster_name_on_cloud}')

    pods = list_namespaced_pod(context, namespace, cluster_name_on_cloud,
                               is_ssh, identity, label_selector)

    # Build pod -> node mapping for unhealthy pods
    unhealthy_set = set(unhealthy_pod_names)
    pod_node_map: dict[str, str | None] = {}
    for pod in pods:
        name = pod.metadata.name
        if name in unhealthy_set:
            pod_node_map[name] = getattr(pod.spec, 'node_name', None)

    unique_nodes = {n for n in pod_node_map.values() if n}
    if not unique_nodes:
        return {}

    node_issues = _check_nodes_health(context, unique_nodes)
    if not node_issues:
        return {}

    # Build structured result: node -> NodeHealthInfo
    result: dict[str, pod_diagnostics.NodeHealthInfo] = {}
    for pod_name, node_name in pod_node_map.items():
        if node_name and node_name in node_issues:
            if node_name not in result:
                result[node_name] = NodeHealthInfo(issue=node_issues[node_name],
                                                   pods=[])
            result[node_name].pods.append(pod_name)

    return result


def _get_pod_termination_reason(pod: Any, cluster_name: str) -> str:
    """Get pod termination reason and write to cluster events.

    Checks both pod conditions (for preemption/disruption) and
    container statuses (for exit codes/errors).
    """
    utc_min_time = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    latest_timestamp = (pod.status.start_time or utc_min_time)
    ready_state = 'Unknown'
    termination_reason = 'Terminated unexpectedly'
    container_reasons = []

    # Check pod status conditions for high level overview.
    # No need to sort, as each condition.type will only appear once.
    for condition in pod.status.conditions:
        reason = condition.reason or 'Unknown reason'
        message = condition.message or ''

        # Get last known readiness state.
        if condition.type == 'Ready':
            ready_state = f'{reason} ({message})' if message else reason
        # Kueue preemption, as defined in:
        # https://pkg.go.dev/sigs.k8s.io/kueue/pkg/controller/jobs/pod#pkg-constants
        elif condition.type == 'TerminationTarget':
            termination_reason = f'Preempted by Kueue: {reason}'
            if message:
                termination_reason += f' ({message})'
        # Generic disruption.
        elif condition.type == 'DisruptionTarget':
            termination_reason = f'Disrupted: {reason}'
            if message:
                termination_reason += f' ({message})'

        if condition.last_transition_time is not None:
            latest_timestamp = max(latest_timestamp,
                                   condition.last_transition_time)

    # Fall back to the pod-level kubelet reason (e.g. 'Evicted' for
    # ephemeral-storage / disk / memory pressure) when no preemption/disruption
    # condition explained the failure. This is often the only place an eviction
    # cause is recorded (container statuses may be uninformative).
    pod_status_reason = getattr(pod.status, 'reason', None)
    if termination_reason == 'Terminated unexpectedly' and pod_status_reason:
        termination_reason = pod_status_reason
        pod_status_message = (getattr(pod.status, 'message', None) or
                              '').strip()
        if pod_status_message:
            termination_reason += f' ({pod_status_message})'

    pod_reason = (f'{termination_reason}.\n'
                  f'Last known state: {ready_state}.')

    # Check container statuses for exit codes/errors
    if pod.status and pod.status.container_statuses:
        for container_status in pod.status.container_statuses:
            terminated = container_status.state.terminated
            if terminated:
                exit_code = terminated.exit_code
                reason = terminated.reason
                if exit_code == 0:
                    # skip exit 0 (non-failed) just for sanity
                    logger.debug(f'{pod.metadata.name}/{container_status.name} '
                                 'had exit code 0. Skipping.')
                    continue
                if reason is None:
                    # just in-case reason is None, have default for debugging
                    reason = f'exit({exit_code})'
                container_reasons.append(reason)
                if terminated.finished_at is not None:
                    latest_timestamp = max(latest_timestamp,
                                           terminated.finished_at)

            # TODO (kyuds): later, if needed, query `last_state` too.

    # Normally we will have a single container per pod for skypilot
    # but doing this just in-case there are multiple containers.
    if container_reasons:
        pod_reason += f'\nContainer errors: {" | ".join(container_reasons)}'

    global_user_state.add_cluster_event(
        cluster_name,
        None,
        f'[kubernetes pod {pod.metadata.name} terminated] {pod_reason}',
        global_user_state.ClusterEventType.DEBUG,
        transitioned_at=int(latest_timestamp.timestamp()),
    )
    return pod_reason


def _condensed_pod_reason(pod: 'V1Pod') -> str:
    """Condense pod failure into a single-line user-facing summary.

    Thin wrapper around ``kubernetes_utils.get_condensed_pod_reason`` (the
    canonical implementation, shared with the command-runner OOM diagnosis
    path).
    """
    return kubernetes_utils.get_condensed_pod_reason(pod)


def _get_pod_events(context: str | None, namespace: str,
                    pod_name: str) -> list[Any]:
    """Get the events for a pod, sorted by timestamp, most recent first."""
    pod_field_selector = (
        f'involvedObject.kind=Pod,involvedObject.name={pod_name}')
    pod_events = kubernetes.core_api(context).list_namespaced_event(
        namespace,
        field_selector=pod_field_selector,
        _request_timeout=kubernetes.API_TIMEOUT).items
    return sorted(
        pod_events,
        key=lambda event: event.metadata.creation_timestamp,
        # latest event appears first
        reverse=True)


# kubelet pod-event reasons that carry a terminal failure cause which is not
# always reflected in pod.status in time -- notably an eviction for
# ephemeral-storage / disk / memory pressure, where the kubelet emits the event
# while pod.status.phase is still 'Running' and status.reason/message lag.
_FAILURE_EVENT_REASONS = ('Evicted',)


def _get_pod_failure_reason_from_events(context: str | None, namespace: str,
                                        pod_name: str) -> str | None:
    """Best-effort failure reason from the pod's most recent kubelet event.

    Some failures (notably evictions for ephemeral-storage / disk / memory
    pressure) are recorded in pod events before they propagate to
    pod.status.reason / phase. Returns '<reason>: <message>' for the most
    recent event whose reason is in ``_FAILURE_EVENT_REASONS``, else None.
    Never raises -- this is additive diagnostics.
    """
    try:
        events = _get_pod_events(context, namespace, pod_name)
    except Exception:  # pylint: disable=broad-except
        return None
    for event in events:  # most recent first
        if event.reason in _FAILURE_EVENT_REASONS:
            message = (event.message or '').strip()
            return f'{event.reason}: {message}'.rstrip(': ')
    return None


def _get_pod_failure_reason_from_status(context: str | None, namespace: str,
                                        pod_name: str) -> str | None:
    """Best-effort durable failure reason from the pod's terminated states.

    A run-phase OOMKilled is recorded in the container's
    ``last_state.terminated`` and survives the restart, but the live ``Ready``
    condition flips back to True once the container is running again -- so a
    snapshot taken outside that window (the read raced the restart) misses it.
    Re-reads the pod and derives the reason from current *and* previous
    terminated states, so the OOM is recovered regardless of where the read
    landed in the restart cycle. Returns '<pod> is not ready (<reason>)' (the
    framing mirrors the single-pod output of
    backend_utils._summarize_pod_reasons so the message reads the same whether
    the live status or this fallback caught it), else None. Never raises.
    """
    try:
        pod = kubernetes.core_api(context).read_namespaced_pod(
            pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
    except Exception:  # pylint: disable=broad-except
        return None
    if not kubernetes_utils.pod_terminated_abnormally(pod):
        return None
    return (f'{pod_name} is not ready '
            f'({kubernetes_utils.get_condensed_pod_reason(pod)})')


def _first_pod_failure_reason(
        provider_config: dict[str, Any], pod_names: list[str],
        per_pod_fn: Callable[[str | None, str, str], str | None]) -> str | None:
    """Return the first non-None ``per_pod_fn(context, namespace, pod)``.

    Resolves namespace/context from the provider config and probes each pod in
    order. Used when a cluster is abnormal but the live per-pod status did not
    name a cause. Best-effort -- per_pod_fn is expected to never raise.
    """
    namespace = kubernetes_utils.get_namespace_from_config(provider_config)
    context = kubernetes_utils.get_context_from_config(provider_config)
    for pod_name in pod_names:
        reason = per_pod_fn(context, namespace, pod_name)
        if reason is not None:
            return reason
    return None


def get_cluster_failure_reason_from_events(provider_config: dict[str, Any],
                                           pod_names: list[str]) -> str | None:
    """First pod eviction reason from kubelet events (status lags), or None.

    An eviction (ephemeral-storage / disk / memory pressure) is emitted as a
    pod event while the pod can still report Running/Ready and status.reason
    has not caught up. See _get_pod_failure_reason_from_events.
    """
    return _first_pod_failure_reason(provider_config, pod_names,
                                     _get_pod_failure_reason_from_events)


def get_cluster_failure_reason_from_pods(provider_config: dict[str, Any],
                                         pod_names: list[str]) -> str | None:
    """First pod's durable terminated-state reason (e.g. a restarted OOM).

    See _get_pod_failure_reason_from_status. Complements the events lookup:
    catches an OOMKilled recovered from last_state when no kubelet event names
    the cause.
    """
    return _first_pod_failure_reason(provider_config, pod_names,
                                     _get_pod_failure_reason_from_status)


def _get_pod_pending_reason(context: str | None, namespace: str,
                            pod_name: str) -> tuple[str, str] | None:
    """Get the reason why a pod is pending from its events.

    Two-pass scan over the event list (sorted newest-first by _get_pod_events):
      1. Tier 2 -- return the newest event with event.type == 'Warning'.
      2. Tier 3 -- return the newest event whose reason is in
         _PENDING_REASON_NORMAL_EVENT_ALLOWLIST.
    Warnings always beat allow-listed Normals, regardless of timestamp ordering
    in the event window -- a FailedScheduling Warning is a more truthful pending
    reason than a Pulling Normal from a doomed retry.

    Returns a (reason, message) tuple, or None if neither pass matches.
    """
    try:
        pod_events = _get_pod_events(context, namespace, pod_name)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Failed to get events for pod {pod_name}: {e}')
        return None

    if not pod_events:
        return None

    # Tier 2: Warning events.
    for event in pod_events:
        if event.type == 'Warning':
            return event.reason or 'Unknown', event.message or ''

    # Tier 3: allow-listed Normal events.
    for event in pod_events:
        if event.reason in _PENDING_REASON_NORMAL_EVENT_ALLOWLIST:
            return event.reason, event.message or ''

    return None


def _format_pod_missing_reason(
        *, context: str | None, pod_name: str, event: Any, cluster_name: str,
        transitioned_at: int,
        first_pod: bool) -> tuple[str, global_user_state.ClusterEventType]:
    """Format pod missing reason.

    Args:
        context: The context of the Kubernetes cluster.
        pod_name: The name of the pod.
        event: The event object.
        cluster_name: The name of the cluster.
        transitioned_at: The timestamp of the event.
        first_pod: Whether this is the first pod.
                   Used in cases where some logic only needs to be run
                   for one pod in the cluster.

    Returns:
        A tuple of the formatted event string and the event type.
    """
    del first_pod, context, cluster_name, transitioned_at  #unused
    event_str = (f'[kubernetes pod {pod_name}] '
                 f'{event.reason} {event.message}')
    event_type = global_user_state.ClusterEventType.DEBUG
    return event_str, event_type


def _get_pod_missing_reason(context: str | None, namespace: str,
                            cluster_name: str, pod_name: str,
                            first_pod: bool) -> str | None:
    """Get events for missing pod and write to cluster events."""
    logger.debug(f'Analyzing events for pod {pod_name}')
    pod_events = _get_pod_events(context, namespace, pod_name)
    last_scheduled_node = None
    insert_new_pod_event = True
    new_event_inserted = False
    inserted_pod_events = 0

    for event in pod_events:
        if event.reason == 'Scheduled':
            pattern = r'Successfully assigned (\S+) to (\S+)'
            match = re.search(pattern, event.message)
            if match:
                scheduled_node = match.group(2)
                last_scheduled_node = scheduled_node
        if insert_new_pod_event:
            # Try inserting the latest events first. If the event is a
            # duplicate, it means the event (and any previous events) have
            # already been inserted - so do not insert further events.
            transitioned_at = int(event.metadata.creation_timestamp.timestamp())
            event_str, event_type = _format_pod_missing_reason(
                context=context,
                pod_name=pod_name,
                event=event,
                first_pod=first_pod,
                cluster_name=cluster_name,
                transitioned_at=transitioned_at)
            try:
                global_user_state.add_cluster_event(
                    cluster_name,
                    None,
                    event_str,
                    event_type,
                    transitioned_at=transitioned_at,
                    expose_duplicate_error=True)
                logger.debug(f'[pod {pod_name}] encountered new pod event: '
                             f'{event.metadata.creation_timestamp} '
                             f'{event.reason} {event.message}')
            except db_utils.UniqueConstraintViolationError:
                insert_new_pod_event = False
            else:
                new_event_inserted = True
                inserted_pod_events += 1

    logger.debug(f'[pod {pod_name}] processed {len(pod_events)} pod events and '
                 f'inserted {inserted_pod_events} new pod events '
                 'previously unseen')

    if last_scheduled_node is not None:
        node_field_selector = ('involvedObject.kind=Node,'
                               f'involvedObject.name={last_scheduled_node}')
        node_events = kubernetes.core_api(context).list_namespaced_event(
            namespace,
            field_selector=node_field_selector,
            _request_timeout=kubernetes.API_TIMEOUT).items
        node_events = sorted(
            node_events,
            key=lambda event: event.metadata.creation_timestamp,
            # latest event appears first
            reverse=True)
        insert_new_node_event = True
        inserted_node_events = 0
        for event in node_events:
            if insert_new_node_event:
                # Try inserting the latest events first. If the event is a
                # duplicate, it means the event (and any previous events) have
                # already been inserted - so do not insert further events.
                try:
                    global_user_state.add_cluster_event(
                        cluster_name,
                        None, f'[kubernetes node {last_scheduled_node}] '
                        f'{event.reason} {event.message}',
                        global_user_state.ClusterEventType.DEBUG,
                        transitioned_at=int(
                            event.metadata.creation_timestamp.timestamp()),
                        expose_duplicate_error=True)
                    logger.debug(
                        f'[pod {pod_name}] encountered new node event: '
                        f'{event.metadata.creation_timestamp} '
                        f'{event.reason} {event.message}')
                except db_utils.UniqueConstraintViolationError:
                    insert_new_node_event = False
                else:
                    new_event_inserted = True
                    inserted_node_events += 1

        logger.debug(f'[pod {pod_name}: node {last_scheduled_node}] '
                     f'processed {len(node_events)} node events and '
                     f'inserted {inserted_node_events} new node events '
                     'previously unseen')
    else:
        logger.debug(f'[pod {pod_name}] could not determine the node '
                     'the pod was scheduled to')

    if not new_event_inserted:
        # If new event is not inserted, there is no useful information to
        # return. Return None.
        return None

    # Analyze the events for failure
    failure_reason = None
    failure_decisiveness = 0

    def _record_failure_reason(reason: str, decisiveness: int):
        nonlocal failure_reason, failure_decisiveness
        if decisiveness > failure_decisiveness:
            failure_reason = reason
            failure_decisiveness = decisiveness

    cluster_events = global_user_state.get_cluster_events(
        cluster_name, None, global_user_state.ClusterEventType.DEBUG)
    for event in cluster_events:
        if event.startswith('[kubernetes pod'):
            event = event.split(']')[1].strip()
        elif event.startswith('[kubernetes node'):
            event = event.split(']')[1].strip()

        if event.startswith('NodeNotReady '):
            _record_failure_reason(event[len('NodeNotReady '):], 1)
        elif event.startswith('TaintManagerEviction '):
            # usually the event message for TaintManagerEviction is not useful
            # so we record a more generic message.
            _record_failure_reason('pod was evicted by taint manager', 2)
        elif event.startswith('DeletingNode '):
            _record_failure_reason(event[len('DeletingNode '):], 3)
    return failure_reason


def list_namespaced_pod(context: str | None, namespace: str,
                        cluster_name_on_cloud: str, is_ssh: bool, identity: str,
                        label_selector: str) -> list[Any]:
    # Get all the pods with the label skypilot-cluster-name: <cluster_name>
    try:
        # log the query parameters we pass to the k8s api
        logger.debug(f'Querying k8s api for pods:\n'
                     f'context: {context}\n'
                     f'namespace: {namespace}\n'
                     f'label selector:`{label_selector}`.')

        response = kubernetes.core_api(context).list_namespaced_pod(
            namespace,
            label_selector=label_selector,
            _request_timeout=kubernetes.API_TIMEOUT)

        # log PodList response info
        if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
            logger.debug(f'k8s api response for `{label_selector}`:\n'
                         f'apiVersion={response.api_version}, '
                         f'kind={response.kind},\n'
                         f'metadata={response.metadata}')

        pods = response.items

        # log detailed Pod info
        if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
            logger.debug(f'k8s api response for `{label_selector}`: '
                         f'len(pods)={len(pods)}')
            for pod in pods:
                logger.debug(f'k8s pod info for `{label_selector}`: '
                             f'pod.apiVersion={pod.api_version}, '
                             f'pod.kind={pod.kind}, \n'
                             f'pod.name={pod.metadata.name}, '
                             f'pod.namespace={pod.metadata.namespace}, \n'
                             f'pod.labels={pod.metadata.labels}, \n'
                             f'pod.annotations={pod.metadata.annotations}, \n'
                             'pod.creationTimestamp='
                             f'{pod.metadata.creation_timestamp}, '
                             'pod.deletionTimestamp='
                             f'{pod.metadata.deletion_timestamp}, \n'
                             f'pod.status={pod.status}')
        return pods

    except kubernetes.max_retry_error():
        with ux_utils.print_exception_no_traceback():
            if is_ssh:
                node_pool = common_utils.removeprefix(context,
                                                      'ssh-') if context else ''
                msg = (
                    f'Cannot connect to SSH Node Pool {node_pool}. '
                    'Please check if the SSH Node Pool is up and accessible. '
                    'To debug, run `sky check ssh` to check the status of '
                    'the SSH Node Pool.')
            else:
                ctx = kubernetes_utils.get_current_kube_config_context_name()
                msg = (f'Network error - check if the {identity} in '
                       f'context {ctx} is up and accessible.')
            raise exceptions.ClusterStatusFetchingError(
                f'Failed to query cluster {cluster_name_on_cloud!r} status. ' +
                msg) from None
    except Exception as e:  # pylint: disable=broad-except
        with ux_utils.print_exception_no_traceback():
            raise exceptions.ClusterStatusFetchingError(
                f'Failed to query {identity} {cluster_name_on_cloud!r} '
                f'status: {common_utils.format_exception(e)}')


def query_instances(
    cluster_name: str,
    cluster_name_on_cloud: str,
    provider_config: dict[str, Any] | None = None,
    non_terminated_only: bool = True,
    retry_if_missing: bool = False,
    status_map_overrides: Mapping[str, Optional['status_lib.ClusterStatus']] |
    None = None,
) -> dict[str, tuple[Optional['status_lib.ClusterStatus'], str | None]]:
    # Mapping from pod phase to skypilot status. These are the only valid pod
    # phases.
    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
    # ``status_map_overrides`` lets callers (e.g. plugin provisioners whose
    # pods don't follow the ray-cluster lifecycle) selectively remap a
    # subset of phases without duplicating this whole function.
    status_map = {
        'Pending': status_lib.ClusterStatus.INIT,
        'Running': status_lib.ClusterStatus.UP,
        'Failed': status_lib.ClusterStatus.INIT,
        'Unknown': None,
        'Succeeded': None,
    }
    if status_map_overrides:
        status_map = {**status_map, **status_map_overrides}

    assert provider_config is not None
    namespace = kubernetes_utils.get_namespace_from_config(provider_config)
    context = kubernetes_utils.get_context_from_config(provider_config)
    is_ssh = context.startswith('ssh-') if context else False
    identity = 'SSH Node Pool' if is_ssh else 'Kubernetes cluster'
    label_selector = (f'{constants.TAG_SKYPILOT_CLUSTER_NAME}='
                      f'{cluster_name_on_cloud}')

    attempts = 0
    pods = list_namespaced_pod(context, namespace, cluster_name_on_cloud,
                               is_ssh, identity, label_selector)
    # When we see no pods returned from the k8s api, we assume the pods have
    # been terminated by the user directly and mark the cluster as terminated
    # in the global user state.
    # We add retry logic here as an attempt to mitigate a leak caused by the
    # kubernetes api returning no pods despite the pods actually existing.
    while (retry_if_missing and not pods and
           attempts < _MAX_QUERY_INSTANCES_RETRIES):
        logger.debug(f'Retrying to query k8s api for {cluster_name_on_cloud} '
                     f'{attempts}/{_MAX_QUERY_INSTANCES_RETRIES} times.'
                     f'after {_QUERY_INSTANCES_RETRY_INTERVAL} seconds.')
        time.sleep(_QUERY_INSTANCES_RETRY_INTERVAL)
        attempts += 1
        pods = list_namespaced_pod(context, namespace, cluster_name_on_cloud,
                                   is_ssh, identity, label_selector)
        if len(pods) > 0:
            logger.info(f'Found {len(pods)} pods for {label_selector} after'
                        f'{attempts} retries.')

    # Check if the pods are running or pending
    cluster_status: dict[str, tuple[status_lib.ClusterStatus | None,
                                    str | None]] = {}
    for pod in pods:
        phase = pod.status.phase
        is_terminating = pod.metadata.deletion_timestamp is not None
        pod_status = status_map[phase]
        reason = None
        if phase in ('Failed', 'Unknown') or is_terminating:
            reason = _get_pod_termination_reason(pod, cluster_name)
            logger.debug(f'Pod Status ({phase}) Reason(s): {reason}')
        elif phase == 'Running':
            reason = _get_pod_health_issues(pod)
        # An eviction (ephemeral-storage / disk / memory pressure) is recorded
        # in the pod's kubelet events before it reaches pod.status -- often
        # while the pod still reports 'Running'. When a flagged pod's
        # status-derived reason is missing the specific cause, recover it from
        # events. Scoped to already-flagged pods (reason set) with a generic
        # reason, so healthy pods incur no extra events API call.
        if reason is not None and _reason_lacks_specific_cause(reason):
            event_reason = _get_pod_failure_reason_from_events(
                context, namespace, pod.metadata.name)
            if event_reason is not None:
                reason = f'{reason}; {event_reason}'
        if non_terminated_only and pod_status is None:
            logger.debug(f'Pod {pod.metadata.name} is terminated, but '
                         'query_instances is called with '
                         f'non_terminated_only=True. Phase: {phase}')
            continue
        pod_name = pod.metadata.name
        cluster_status[pod_name] = (pod_status, reason)

    # Find the list of pod names that should be there
    # from k8s services. Filter duplicates as -ssh service
    # creates a duplicate entry.
    target_pod_names = list(
        set([
            service['spec']['selector']['component']
            for service in provider_config.get('services', [])
        ]))

    first_pod = True
    for target_pod_name in target_pod_names:
        if target_pod_name not in cluster_status:
            # If the pod is not in the cluster_status, it means it's not
            # running.
            # Analyze what happened to the pod based on events.
            reason = _get_pod_missing_reason(context, namespace, cluster_name,
                                             target_pod_name, first_pod)
            first_pod = False
            if not non_terminated_only:
                cluster_status[target_pod_name] = (None, reason)

    return cluster_status


def get_command_runners(
    cluster_info: common.ClusterInfo,
    **credentials: dict[str, Any],
) -> list[command_runner.CommandRunner]:
    """Get a command runner for the given cluster."""
    assert cluster_info.provider_config is not None, cluster_info
    instances = cluster_info.instances
    namespace = kubernetes_utils.get_namespace_from_config(
        cluster_info.provider_config)
    context = kubernetes_utils.get_context_from_config(
        cluster_info.provider_config)

    runners: list[command_runner.CommandRunner] = []
    if cluster_info.head_instance_id is not None:
        pod_name = cluster_info.head_instance_id

        # Try to get deployment name from label first
        head_instance_info = instances[pod_name][0]
        deployment = head_instance_info.tags.get(
            k8s_constants.TAG_SKYPILOT_DEPLOYMENT_NAME)

        node_list = [((namespace, context), pod_name)]
        head_runner = command_runner.KubernetesCommandRunner(
            node_list[0],
            deployment=deployment,
            container=k8s_constants.RAY_NODE_CONTAINER_NAME,
            **credentials)
        runners.append(head_runner)

    node_list = [((namespace, context), pod_name)
                 for pod_name in instances.keys()
                 if pod_name != cluster_info.head_instance_id]
    runners.extend(
        command_runner.KubernetesCommandRunner.make_runner_list(
            node_list,
            container=k8s_constants.RAY_NODE_CONTAINER_NAME,
            **credentials))

    return runners
