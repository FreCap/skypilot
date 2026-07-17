"""Kubernetes pod scheduling and capacity diagnostics."""

import datetime
import sys
import time
from typing import Any

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

# Use the historical logger so facade imports and logging behavior stay stable.
logger = sky_logging.init_logger('sky.provision.kubernetes.instance')


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

    try:
        return _detect_cluster_event_reason_occurred(namespace, context,
                                                     search_start,
                                                     'FailedScheduling')
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'Error occurred while detecting cluster autoscaler: {e}')
        return False


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
    use_heuristic_detection = (autoscaler_is_set and
                               not kubernetes_enums.KubernetesAutoscalerType(
                                   autoscaler_type).emits_autoscale_event())
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
