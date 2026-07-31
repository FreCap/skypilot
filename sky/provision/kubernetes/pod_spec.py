"""Pure construction of concrete Kubernetes Pod specifications.

Provider discovery and configuration resolution happen before this module is
called.  :func:`finalize_pod_spec` owns the deterministic mutations that turn a
prepared cluster-level Pod template into the exact head or worker Pod submitted
to Kubernetes.
"""

from collections.abc import Mapping
import copy
from typing import Any, Literal

from sky.provision import constants
from sky.provision.kubernetes import constants as k8s_constants
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.utils import config_utils

PodRole = Literal['head', 'worker']


def _head_service_selector(cluster_name: str) -> dict[str, str]:
    """Returns the canonical selector shared by head Pods and Services."""
    return {'component': f'{cluster_name}-head'}


def _configure_runtime_class(pod_spec: dict[str,
                                            Any], nvidia_runtime_exists: bool,
                             needs_gpus_nvidia: bool) -> None:
    """Sets or strips runtimeClassName on ``pod_spec`` in place.

    A falsy runtimeClassName means the user explicitly disabled the runtime
    class.  Kubernetes rejects an empty runtimeClassName, and the explicit
    override must also suppress the automatic ``nvidia`` assignment.
    """
    spec = pod_spec['spec']
    if 'runtimeClassName' in spec and not spec['runtimeClassName']:
        del spec['runtimeClassName']
        return
    if (nvidia_runtime_exists and needs_gpus_nvidia and
            'runtimeClassName' not in spec):
        spec['runtimeClassName'] = 'nvidia'


def finalize_pod_spec(
    base_pod_spec: Mapping[str, Any],
    *,
    role: PodRole,
    pod_name: str,
    cluster_name_on_cloud: str,
    node_count: int,
    nvidia_runtime_exists: bool,
    needs_gpus: bool,
    needs_gpus_nvidia: bool,
    gpu_resource_key: str,
    needs_tpu: bool,
    resolved_base_affinity: Mapping[str, Any] | None,
    docker_config: kubernetes_utils.DockerConfig | None,
    docker_pvc_name: str | None,
    context: str | None,
    namespace: str,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    """Builds one concrete Pod spec without mutating any input.

    All provider-dependent values are explicit arguments.  In particular,
    ``nvidia_runtime_exists`` and ``gpu_resource_key`` are discovered before
    this boundary, ``docker_pvc_name`` is resolved from user state, and
    ``resolved_base_affinity`` is the template affinity after the
    ``allowed_nodes`` policy has resolved any node names or IPs.
    """
    pod_spec: dict[str, Any] = copy.deepcopy(dict(base_pod_spec))
    spec = pod_spec['spec']

    if resolved_base_affinity is not None:
        spec['affinity'] = copy.deepcopy(resolved_base_affinity)

    _configure_runtime_class(pod_spec, nvidia_runtime_exists, needs_gpus_nvidia)

    metadata = pod_spec['metadata']
    labels = metadata['labels']
    metadata['name'] = pod_name
    if role == 'head':
        labels.update(constants.HEAD_NODE_TAGS)
        labels.update(_head_service_selector(cluster_name_on_cloud))
    else:
        labels.update(constants.WORKER_NODE_TAGS)
        labels['component'] = pod_name

    if deployment_name is not None:
        labels[k8s_constants.TAG_SKYPILOT_DEPLOYMENT_NAME] = deployment_name

    if docker_config is not None:
        kubernetes_utils.inject_docker_cache_volume(
            pod_spec=pod_spec,
            docker_config=docker_config,
            pvc_name=docker_pvc_name,
            context=context,
            namespace=namespace,
        )

    # Keep placement fields identical for head and workers so Kueue can merge
    # them into one PodSet for queued provisioning.  Only role metadata differs.
    if node_count > 1:
        # Prefer distinct physical nodes while allowing co-location when the
        # cluster has no other schedulable capacity.
        pod_spec_config = config_utils.Config(spec.get('affinity', {}))
        existing_rules = pod_spec_config.get_nested(
            ('podAntiAffinity',
             'preferredDuringSchedulingIgnoredDuringExecution'), [])
        existing_rules.append({
            'weight': 100,
            'podAffinityTerm': {
                'labelSelector': {
                    'matchExpressions': [{
                        'key': constants.TAG_SKYPILOT_CLUSTER_NAME,
                        'operator': 'In',
                        'values': [cluster_name_on_cloud],
                    }],
                },
                'topologyKey': 'kubernetes.io/hostname',
            },
        })
        pod_spec_config.set_nested(
            ('podAntiAffinity',
             'preferredDuringSchedulingIgnoredDuringExecution'), existing_rules)
        spec['affinity'] = pod_spec_config

    # GKE TPU slice nodes carry google.com/tpu=present:NoSchedule.
    if needs_tpu:
        existing_tolerations = spec.get('tolerations', [])
        spec['tolerations'] = existing_tolerations + [{
            'key': kubernetes_utils.TPU_RESOURCE_KEY,
            'operator': 'Equal',
            'value': 'present',
            'effect': 'NoSchedule',
        }]

    # DWS-created GPU nodes may carry a resource-key NoSchedule taint.  This is
    # harmless for non-DWS clusters and preserves the existing scheduling path.
    if needs_gpus:
        existing_tolerations = spec.get('tolerations', [])
        spec['tolerations'] = existing_tolerations + [{
            'key': gpu_resource_key,
            'operator': 'Exists',
            'effect': 'NoSchedule',
        }]

    return pod_spec
