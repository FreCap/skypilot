"""Pure construction of concrete Kubernetes Pod specifications.

Provider discovery and configuration resolution happen before this module is
called.  :func:`finalize_pod_spec` owns the deterministic mutations that turn a
prepared cluster-level Pod template into the exact head or worker Pod submitted
to Kubernetes.
"""

from collections.abc import Mapping
import copy
import dataclasses
from typing import Any, Literal

from sky.provision import constants
from sky.provision.kubernetes import constants as k8s_constants
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.utils import config_utils

PodRole = Literal['head', 'worker']


class ProjectedAcceleratorContractError(ValueError):
    """A projected worker Pod has an ambiguous resource-request shape."""


@dataclasses.dataclass(frozen=True)
class ProjectedAcceleratorContract:
    """Whole-Pod accelerator ownership observed at one contract boundary."""

    matches: bool
    ray_node_container_count: int
    ray_node_resource_contract_matches: bool
    unexpected_accelerator_resources: dict[str, object]
    dynamic_resource_claims: dict[str, object]


def _pod_api_field(owner: object, yaml_name: str, api_name: str) -> Any:
    """Reads one YAML or Kubernetes-client-model field without coercion."""
    if isinstance(owner, Mapping):
        return owner.get(yaml_name)
    try:
        state = vars(owner)
    except TypeError:
        return None
    if api_name in state:
        return state[api_name]
    # kubernetes.client models expose public properties backed by private
    # fields (for example ``containers`` -> ``_containers``). Read the stored
    # value directly so generic mocks cannot synthesize a truthy missing field.
    return state.get(f'_{api_name}')


def _resource_mapping(owner: object, section: str, location: str,
                      rewrite: bool) -> dict[str, Any] | Mapping[str, Any]:
    resources = _pod_api_field(owner, 'resources', 'resources')
    if resources is None and rewrite:
        if not isinstance(owner, dict):
            raise ProjectedAcceleratorContractError(
                f'{location} must be a mapping.')
        resources = {}
        owner['resources'] = resources
    if resources is None:
        return {}
    if rewrite and not isinstance(resources, dict):
        raise ProjectedAcceleratorContractError(
            f'{location} resources must be a mapping.')
    values = _pod_api_field(resources, section, section)
    if values is None and rewrite:
        assert isinstance(resources, dict)
        values = {}
        resources[section] = values
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise ProjectedAcceleratorContractError(
            f'{location} resource requests and limits must be mappings.')
    if rewrite and not isinstance(values, dict):
        raise ProjectedAcceleratorContractError(
            f'{location} resource requests and limits must be mutable '
            'mappings.')
    return values


def _nonempty_resource_claims(owner: object, location: str) -> object | None:
    resources = _pod_api_field(owner, 'resources', 'resources')
    if resources is None:
        return None
    claims = _pod_api_field(resources, 'claims', 'claims')
    if claims is None:
        return None
    if not isinstance(claims, (list, tuple)):
        raise ProjectedAcceleratorContractError(
            f'{location} resource claims must be a list.')
    return claims or None


def enforce_projected_accelerator_contract(
    pod_spec: object,
    expected_resource_key: str,
    expected_accelerator_count: object,
    *,
    rewrite: bool,
) -> ProjectedAcceleratorContract:
    """Owns every accelerator-request surface for a projected worker Pod.

    ``rewrite=True`` canonicalizes a mutable YAML Pod spec: all supported
    accelerator resources are removed from every Pod surface and the exact
    projected request is installed on the sole ``ray-node`` container.
    Dynamic Resource Allocation is rejected because an opaque claim can select
    a device outside this resource-key contract.

    ``rewrite=False`` performs the same traversal on an admitted Kubernetes
    object without mutation and returns the complete attestation result.
    """
    if rewrite and not isinstance(pod_spec, dict):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes Pod spec must be a mapping.')
    accelerator_resource_keys = {
        *kubernetes_utils.SUPPORTED_GPU_RESOURCE_KEYS.values(),
        kubernetes_utils.TPU_RESOURCE_KEY,
        expected_resource_key,
    }
    expected_quantity = str(expected_accelerator_count)
    dynamic_resource_claims: dict[str, object] = {}
    pod_claims = _pod_api_field(pod_spec, 'resourceClaims', 'resource_claims')
    if pod_claims is not None:
        if not isinstance(pod_claims, (list, tuple)):
            raise ProjectedAcceleratorContractError(
                'Pod resourceClaims must be a list.')
        if pod_claims:
            dynamic_resource_claims['pod'] = pod_claims

    raw_containers = _pod_api_field(pod_spec, 'containers', 'containers')
    if not isinstance(raw_containers, list):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes containers must be a list.')
    raw_init_containers = _pod_api_field(pod_spec, 'initContainers',
                                         'init_containers')
    if raw_init_containers is None:
        raw_init_containers = []
    if not isinstance(raw_init_containers, list):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes initContainers must be a list.')
    if rewrite and (any(not isinstance(container, dict)
                        for container in raw_containers) or
                    any(not isinstance(container, dict)
                        for container in raw_init_containers)):
        raise ProjectedAcceleratorContractError(
            'Projected SkyServe Kubernetes containers and initContainers '
            'must contain mappings.')

    unexpected: dict[str, object] = {}
    ray_node_container_count = 0
    ray_node_resource_contract_matches = False

    def _inspect_container(container: object, location: str,
                           is_runtime_container: bool) -> None:
        nonlocal ray_node_container_count
        nonlocal ray_node_resource_contract_matches
        claims = _nonempty_resource_claims(container, location)
        if claims is not None:
            dynamic_resource_claims[location] = claims
        requests = _resource_mapping(container, 'requests', location, rewrite)
        limits = _resource_mapping(container, 'limits', location, rewrite)
        if rewrite:
            assert isinstance(requests, dict)
            assert isinstance(limits, dict)
            for resource_key in accelerator_resource_keys:
                requests.pop(resource_key, None)
                limits.pop(resource_key, None)
            if is_runtime_container:
                requests[expected_resource_key] = expected_accelerator_count
                limits[expected_resource_key] = expected_accelerator_count
                ray_node_resource_contract_matches = True
            return
        entries: dict[str, dict[str, str]] = {}
        for section, values in (('requests', requests), ('limits', limits)):
            selected = {
                str(key): str(value)
                for key, value in values.items()
                if key in accelerator_resource_keys
            }
            if selected:
                entries[section] = selected
        if is_runtime_container:
            ray_node_resource_contract_matches = entries == {
                'requests': {
                    expected_resource_key: expected_quantity,
                },
                'limits': {
                    expected_resource_key: expected_quantity,
                },
            }
        elif entries:
            unexpected[location] = entries

    for index, container in enumerate(raw_containers):
        name = _pod_api_field(container, 'name', 'name')
        is_runtime_container = name == 'ray-node'
        if is_runtime_container:
            ray_node_container_count += 1
        _inspect_container(container, f'container[{index}]',
                           is_runtime_container)
    for index, container in enumerate(raw_init_containers):
        _inspect_container(container, f'init_container[{index}]', False)

    pod_resources = _pod_api_field(pod_spec, 'resources', 'resources')
    if pod_resources is not None:
        if rewrite and not isinstance(pod_resources, dict):
            raise ProjectedAcceleratorContractError(
                'Projected SkyServe Kubernetes Pod resources must be a '
                'mapping.')
        for section in ('requests', 'limits'):
            values = _pod_api_field(pod_resources, section, section)
            if values is None:
                continue
            if not isinstance(values, Mapping):
                raise ProjectedAcceleratorContractError(
                    'Projected SkyServe Kubernetes Pod resource requests and '
                    'limits must be mappings.')
            selected = {
                str(key): str(value)
                for key, value in values.items()
                if key in accelerator_resource_keys
            }
            if rewrite:
                if not isinstance(values, dict):
                    raise ProjectedAcceleratorContractError(
                        'Projected SkyServe Kubernetes Pod resource requests '
                        'and limits must be mutable mappings.')
                for resource_key in accelerator_resource_keys:
                    values.pop(resource_key, None)
            elif selected:
                unexpected[f'pod_resources.{section}'] = selected

    overhead = _pod_api_field(pod_spec, 'overhead', 'overhead')
    if overhead is not None:
        if not isinstance(overhead, Mapping):
            raise ProjectedAcceleratorContractError(
                'Projected SkyServe Kubernetes Pod overhead must be a '
                'mapping.')
        selected = {
            str(key): str(value)
            for key, value in overhead.items()
            if key in accelerator_resource_keys
        }
        if rewrite:
            if not isinstance(overhead, dict):
                raise ProjectedAcceleratorContractError(
                    'Projected SkyServe Kubernetes Pod overhead must be a '
                    'mutable mapping.')
            for resource_key in accelerator_resource_keys:
                overhead.pop(resource_key, None)
        elif selected:
            unexpected['overhead'] = selected

    matches = (ray_node_container_count == 1 and
               ray_node_resource_contract_matches and not unexpected and
               not dynamic_resource_claims)
    return ProjectedAcceleratorContract(
        matches=matches,
        ray_node_container_count=ray_node_container_count,
        ray_node_resource_contract_matches=(ray_node_resource_contract_matches),
        unexpected_accelerator_resources=unexpected,
        dynamic_resource_claims=dynamic_resource_claims,
    )


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
