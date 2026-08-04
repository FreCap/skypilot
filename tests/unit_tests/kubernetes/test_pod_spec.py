"""Characterization tests for final Kubernetes Pod construction."""

import copy
import hashlib
import pickle
import types
from typing import Any

import pytest

from sky.provision import constants
from sky.provision.kubernetes import constants as k8s_constants
from sky.provision.kubernetes import instance
from sky.provision.kubernetes import pod_spec as pod_spec_lib
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.utils import config_utils

_CLUSTER = 'characterization-cluster'
_CONTEXT = 'test-context'
_NAMESPACE = 'test-namespace'
_GPU_KEY = kubernetes_utils.SUPPORTED_GPU_RESOURCE_KEYS['nvidia']


def _base_pod_spec(*,
                   resources: dict[str, Any] | None = None,
                   node_selector: dict[str, str] | None = None,
                   affinity: dict[str, Any] | None = None,
                   tolerations: list[dict[str, Any]] | None = None,
                   docker_container: str | None = None) -> dict[str, Any]:
    containers = [{
        'name': k8s_constants.RAY_NODE_CONTAINER_NAME,
        'resources': resources or {},
    }]
    if docker_container is not None:
        containers.append({'name': docker_container})
    spec: dict[str, Any] = {'containers': containers}
    if node_selector is not None:
        spec['nodeSelector'] = node_selector
    if affinity is not None:
        spec['affinity'] = affinity
    if tolerations is not None:
        spec['tolerations'] = tolerations
    return {
        'metadata': {
            'labels': {
                'ray-cluster-name': _CLUSTER,
                constants.TAG_SKYPILOT_CLUSTER_NAME: _CLUSTER,
            },
            'annotations': {
                'skypilot-cluster-name': 'full-cluster-name',
            },
        },
        'spec': spec,
    }


def _resolved_base_affinity(
    base_pod_spec: dict[str, Any],
    allowed_nodes_config: dict[str, Any] | None,
) -> dict[str, Any] | None:
    spec = copy.deepcopy(base_pod_spec['spec'])
    kubernetes_utils.inject_allowed_nodes_affinity(spec,
                                                   allowed_nodes_config,
                                                   context=_CONTEXT)
    return spec.get('affinity')


def _legacy_finalize_pod_spec(
    base: dict[str, Any],
    *,
    role: pod_spec_lib.PodRole,
    pod_name: str,
    node_count: int,
    nvidia_runtime_exists: bool,
    needs_gpus: bool,
    needs_gpus_nvidia: bool,
    needs_tpu: bool,
    allowed_nodes_config: dict[str, Any] | None,
    docker_config: kubernetes_utils.DockerConfig | None,
    docker_pvc_name: str | None,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    """The pre-extraction mutation order from ``instance._create_pods``."""
    pod_spec = copy.deepcopy(base)
    instance._configure_runtime_class(  # pylint: disable=protected-access
        pod_spec, nvidia_runtime_exists, needs_gpus_nvidia)

    labels = pod_spec['metadata']['labels']
    pod_spec['metadata']['name'] = pod_name
    if role == 'head':
        labels.update(constants.HEAD_NODE_TAGS)
        labels['component'] = f'{_CLUSTER}-head'
    else:
        labels.update(constants.WORKER_NODE_TAGS)
        labels['component'] = pod_name

    if docker_config is not None:
        kubernetes_utils.inject_docker_cache_volume(
            pod_spec=pod_spec,
            docker_config=docker_config,
            pvc_name=docker_pvc_name,
            context=_CONTEXT,
            namespace=_NAMESPACE,
        )

    if node_count > 1:
        affinity = config_utils.Config(pod_spec['spec'].get('affinity', {}))
        existing_rules = affinity.get_nested(
            ('podAntiAffinity',
             'preferredDuringSchedulingIgnoredDuringExecution'), [])
        existing_rules.append({
            'weight': 100,
            'podAffinityTerm': {
                'labelSelector': {
                    'matchExpressions': [{
                        'key': constants.TAG_SKYPILOT_CLUSTER_NAME,
                        'operator': 'In',
                        'values': [_CLUSTER],
                    }],
                },
                'topologyKey': 'kubernetes.io/hostname',
            },
        })
        affinity.set_nested(('podAntiAffinity',
                             'preferredDuringSchedulingIgnoredDuringExecution'),
                            existing_rules)
        pod_spec['spec']['affinity'] = affinity

    if needs_tpu:
        existing = pod_spec['spec'].get('tolerations', [])
        pod_spec['spec']['tolerations'] = existing + [{
            'key': kubernetes_utils.TPU_RESOURCE_KEY,
            'operator': 'Equal',
            'value': 'present',
            'effect': 'NoSchedule',
        }]
    if needs_gpus:
        existing = pod_spec['spec'].get('tolerations', [])
        pod_spec['spec']['tolerations'] = existing + [{
            'key': _GPU_KEY,
            'operator': 'Exists',
            'effect': 'NoSchedule',
        }]

    kubernetes_utils.inject_allowed_nodes_affinity(pod_spec['spec'],
                                                   allowed_nodes_config,
                                                   context=_CONTEXT)
    if deployment_name is not None:
        labels[k8s_constants.TAG_SKYPILOT_DEPLOYMENT_NAME] = deployment_name
    return pod_spec


@pytest.fixture
def fake_allowed_nodes(monkeypatch):
    nodes = [
        types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                name='node-a',
                labels={'kubernetes.io/hostname': 'hostname-a'},
            ),
            status=types.SimpleNamespace(addresses=[
                types.SimpleNamespace(address='10.0.0.1'),
            ]),
        ),
        types.SimpleNamespace(
            metadata=types.SimpleNamespace(
                name='node-b',
                labels={'kubernetes.io/hostname': 'hostname-b'},
            ),
            status=types.SimpleNamespace(addresses=[
                types.SimpleNamespace(address='10.0.0.2'),
            ]),
        ),
    ]
    monkeypatch.setattr(kubernetes_utils, 'get_kubernetes_nodes',
                        lambda **_: nodes)


# pylint: disable=protected-access
@pytest.mark.parametrize(
    'case',
    [
        pytest.param(
            {
                'base': _base_pod_spec(),
                'role': 'head',
                'pod_name': f'{_CLUSTER}-head',
                'node_count': 1,
                'nvidia_runtime_exists': False,
                'needs_gpus': False,
                'needs_gpus_nvidia': False,
                'needs_tpu': False,
                'allowed_nodes_config': None,
                'docker_config': None,
                'docker_pvc_name': None,
                'deployment_name': 'controller-deployment',
            },
            id='single-node-cpu-head',
        ),
        pytest.param(
            {
                'base': _base_pod_spec(
                    affinity={
                        'podAntiAffinity': {
                            'preferredDuringSchedulingIgnoredDuringExecution': [{
                                'weight': 25,
                                'podAffinityTerm': {
                                    'topologyKey': 'topology.kubernetes.io/zone'
                                },
                            }],
                        },
                    },
                    tolerations=[{
                        'key': 'existing',
                        'operator': 'Exists',
                    }],
                ),
                'role': 'worker',
                'pod_name': f'{_CLUSTER}-worker2',
                'node_count': 3,
                'nvidia_runtime_exists': False,
                'needs_gpus': False,
                'needs_gpus_nvidia': False,
                'needs_tpu': False,
                'allowed_nodes_config': None,
                'docker_config': None,
                'docker_pvc_name': None,
            },
            id='multi-node-cpu-worker',
        ),
        pytest.param(
            {
                'base': _base_pod_spec(resources={'limits': {
                    _GPU_KEY: 1
                }}),
                'role': 'head',
                'pod_name': f'{_CLUSTER}-head',
                'node_count': 1,
                'nvidia_runtime_exists': True,
                'needs_gpus': True,
                'needs_gpus_nvidia': True,
                'needs_tpu': False,
                'allowed_nodes_config': None,
                'docker_config': None,
                'docker_pvc_name': None,
            },
            id='single-node-gpu-head',
        ),
        pytest.param(
            {
                'base': _base_pod_spec(
                    node_selector={
                        kubernetes_utils.GKELabelFormatter.TPU_LABEL_KEY: 'tpu-v5-lite-podslice'
                    }),
                'role': 'worker',
                'pod_name': f'{_CLUSTER}-worker1',
                'node_count': 2,
                'nvidia_runtime_exists': False,
                'needs_gpus': False,
                'needs_gpus_nvidia': False,
                'needs_tpu': True,
                'allowed_nodes_config': None,
                'docker_config': None,
                'docker_pvc_name': None,
            },
            id='multi-node-tpu-worker',
        ),
        pytest.param(
            {
                'base': _base_pod_spec(
                    affinity={
                        'nodeAffinity': {
                            'requiredDuringSchedulingIgnoredDuringExecution': {
                                'nodeSelectorTerms': [{
                                    'matchExpressions': [{
                                        'key': 'cloud.google.com/gke-accelerator',
                                        'operator': 'In',
                                        'values': ['nvidia-l4'],
                                    }],
                                }],
                            },
                        },
                    }),
                'role': 'worker',
                'pod_name': f'{_CLUSTER}-worker1',
                'node_count': 1,
                'nvidia_runtime_exists': False,
                'needs_gpus': False,
                'needs_gpus_nvidia': False,
                'needs_tpu': False,
                'allowed_nodes_config': {
                    'label_selector': {
                        'pool': 'research'
                    },
                    'names': ['node-a'],
                    'ips': ['10.0.0.2'],
                },
                'docker_config': None,
                'docker_pvc_name': None,
            },
            id='allowed-nodes-worker',
        ),
        pytest.param(
            {
                'base': _base_pod_spec(docker_container='dind'),
                'role': 'head',
                'pod_name': f'{_CLUSTER}-head',
                'node_count': 1,
                'nvidia_runtime_exists': False,
                'needs_gpus': False,
                'needs_gpus_nvidia': False,
                'needs_tpu': False,
                'allowed_nodes_config': None,
                'docker_config': kubernetes_utils.DockerConfig(
                    mode=kubernetes_utils.DockerMode.ALL,
                    cache_volume='docker-cache',
                ),
                'docker_pvc_name': 'docker-cache-pvc',
            },
            id='docker-pvc-cache-head',
        ),
    ],
)
@pytest.mark.usefixtures('fake_allowed_nodes')
def test_finalize_pod_spec_matches_legacy_mutations(case):
    base = case['base']
    allowed_nodes_config = case['allowed_nodes_config']
    expected = _legacy_finalize_pod_spec(**case)

    actual = pod_spec_lib.finalize_pod_spec(
        base,
        role=case['role'],
        pod_name=case['pod_name'],
        cluster_name_on_cloud=_CLUSTER,
        node_count=case['node_count'],
        nvidia_runtime_exists=case['nvidia_runtime_exists'],
        needs_gpus=case['needs_gpus'],
        needs_gpus_nvidia=case['needs_gpus_nvidia'],
        gpu_resource_key=_GPU_KEY,
        needs_tpu=case['needs_tpu'],
        resolved_base_affinity=_resolved_base_affinity(base,
                                                       allowed_nodes_config),
        docker_config=case['docker_config'],
        docker_pvc_name=case['docker_pvc_name'],
        context=_CONTEXT,
        namespace=_NAMESPACE,
        deployment_name=case.get('deployment_name'),
    )

    assert actual == expected


def test_finalize_pod_spec_docker_cache_subpath_is_per_pod():
    base = _base_pod_spec(docker_container='dind')
    docker_config = kubernetes_utils.DockerConfig(
        mode=kubernetes_utils.DockerMode.ALL,
        cache_volume='docker-cache',
    )

    def finalize(role: pod_spec_lib.PodRole, pod_name: str) -> dict[str, Any]:
        return pod_spec_lib.finalize_pod_spec(
            base,
            role=role,
            pod_name=pod_name,
            cluster_name_on_cloud=_CLUSTER,
            node_count=2,
            nvidia_runtime_exists=False,
            needs_gpus=False,
            needs_gpus_nvidia=False,
            gpu_resource_key=_GPU_KEY,
            needs_tpu=False,
            resolved_base_affinity=None,
            docker_config=docker_config,
            docker_pvc_name='docker-cache-pvc',
            context=_CONTEXT,
            namespace=_NAMESPACE,
        )

    head_name = f'{_CLUSTER}-head'
    worker_name = f'{_CLUSTER}-worker1'
    head = finalize('head', head_name)
    worker = finalize('worker', worker_name)
    head_mount = head['spec']['containers'][1]['volumeMounts'][0]
    worker_mount = worker['spec']['containers'][1]['volumeMounts'][0]

    def expected_subpath(pod_name: str) -> str:
        digest = hashlib.sha256(
            f'{_CONTEXT}:{_NAMESPACE}:{pod_name}'.encode()).hexdigest()[:12]
        return f'var_lib_docker_{digest}'

    assert head_mount['subPath'] == expected_subpath(head_name)
    assert worker_mount['subPath'] == expected_subpath(worker_name)
    assert head_mount['subPath'] != worker_mount['subPath']


def test_finalize_pod_spec_does_not_mutate_inputs():
    base = _base_pod_spec(
        affinity={
            'nodeAffinity': {
                'preferredDuringSchedulingIgnoredDuringExecution': []
            }
        },
        tolerations=[{
            'key': 'existing'
        }],
        docker_container='buildkitd',
    )
    resolved_affinity = {
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'pool',
                        'operator': 'In',
                        'values': ['research'],
                    }],
                }],
            },
        },
    }
    base_before = copy.deepcopy(base)
    affinity_before = copy.deepcopy(resolved_affinity)

    result = pod_spec_lib.finalize_pod_spec(
        base,
        role='worker',
        pod_name=f'{_CLUSTER}-worker1',
        cluster_name_on_cloud=_CLUSTER,
        node_count=2,
        nvidia_runtime_exists=True,
        needs_gpus=True,
        needs_gpus_nvidia=True,
        gpu_resource_key=_GPU_KEY,
        needs_tpu=True,
        resolved_base_affinity=resolved_affinity,
        docker_config=kubernetes_utils.DockerConfig(
            mode=kubernetes_utils.DockerMode.BUILD,
            cache_volume='docker-cache',
        ),
        docker_pvc_name='docker-cache-pvc',
        context=_CONTEXT,
        namespace=_NAMESPACE,
        deployment_name='controller-deployment',
    )

    assert result is not base
    assert base == base_before
    assert resolved_affinity == affinity_before


def test_finalize_pod_spec_has_no_ambient_provider_reads(monkeypatch):

    def forbidden(*_args, **_kwargs):
        raise AssertionError(
            'ambient provider/config read crossed pure boundary')

    monkeypatch.setattr(kubernetes_utils, 'get_allowed_nodes_config', forbidden)
    monkeypatch.setattr(kubernetes_utils, 'get_kubernetes_nodes', forbidden)
    monkeypatch.setattr(kubernetes_utils, 'get_gpu_resource_key', forbidden)
    monkeypatch.setattr(kubernetes_utils, 'check_nvidia_runtime_class',
                        forbidden)

    result = pod_spec_lib.finalize_pod_spec(
        _base_pod_spec(docker_container='dind'),
        role='head',
        pod_name=f'{_CLUSTER}-head',
        cluster_name_on_cloud=_CLUSTER,
        node_count=1,
        nvidia_runtime_exists=True,
        needs_gpus=True,
        needs_gpus_nvidia=True,
        gpu_resource_key=_GPU_KEY,
        needs_tpu=False,
        resolved_base_affinity={'nodeAffinity': {}},
        docker_config=kubernetes_utils.DockerConfig(
            mode=kubernetes_utils.DockerMode.ALL),
        docker_pvc_name=None,
        context=_CONTEXT,
        namespace=_NAMESPACE,
    )

    assert result['spec']['runtimeClassName'] == 'nvidia'


@pytest.mark.parametrize(
    ('instance_symbol', 'owner_symbol'),
    (
        (instance._configure_runtime_class,
         pod_spec_lib._configure_runtime_class),
        (instance._head_service_selector, pod_spec_lib._head_service_selector),
    ),
)
def test_historical_instance_seams_are_direct_pickle_safe_aliases(
        instance_symbol, owner_symbol):
    assert instance_symbol is owner_symbol
    assert instance_symbol.__module__ == instance.__name__
    assert pickle.loads(pickle.dumps(instance_symbol)) is instance_symbol


# pylint: enable=protected-access
