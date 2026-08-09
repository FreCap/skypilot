"""Tests for Kubernetes provision."""

import datetime
import json
import multiprocessing
import os
import re
import threading
import time
import types
from unittest import mock

import filelock
import pytest

from sky import clouds
from sky import exceptions as sky_exceptions
from sky import resources
from sky.adaptors import kubernetes
from sky.backends import cloud_vm_ray_backend
from sky.provision import common as provision_common
from sky.provision.kubernetes import config as config_lib
from sky.provision.kubernetes import constants as k8s_constants
from sky.provision.kubernetes import instance
from sky.provision.kubernetes import pod_scheduling
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.provision.kubernetes.instance import logger
from sky.utils import subprocess_utils


def _remove_colorama_escape_codes(error_output):
    return [re.sub(r'\x1b\[[0-9;]*m', '', line) for line in error_output]


def _make_provision_config(count):
    """A minimal ProvisionConfig sufficient to drive _create_pods."""
    return provision_common.ProvisionConfig(
        provider_config={'timeout': 10},
        authentication_config={},
        docker_config={},
        node_config={
            'metadata': {
                'labels': {}
            },
            'spec': {
                'containers': [{}]
            },
        },
        count=count,
        tags={},
        resume_stopped_nodes=False,
        ports_to_open_on_launch=None,
    )


def _fake_pod(name):
    pod = mock.MagicMock()
    pod.metadata.name = name
    pod.status.phase = 'Pending'
    return pod


def test_cluster_name_annotation_collision_fails_closed():
    pod = mock.MagicMock()
    pod.metadata.name = 'short-name-head'
    pod.metadata.annotations = {
        'skypilot-cluster-name': 'service-v1-replica-68'
    }

    with pytest.raises(config_lib.KubernetesError,
                       match='already owned by full cluster'):
        instance._validate_cluster_name_annotations(
            {pod.metadata.name: pod},
            'service-v2-replica-8',
            'short-name',
        )


def _patch_create_pods_k8s_boundary(monkeypatch, existing_pods, head_name):
    """Stub the Kubernetes-touching calls _create_pods makes.

    Leaves the to_start_count arithmetic and per-pod skip logic to run for
    real, so the idempotency contract is what's actually exercised.
    """

    def fake_filter_pods(namespace, context, tags, phases):
        # Only the Pending/Running query returns existing pods; the
        # Terminating and Failed/Succeeded cleanup queries return nothing.
        if 'Pending' in phases:
            return dict(existing_pods)
        return {}

    monkeypatch.setattr(kubernetes_utils, 'get_namespace_from_config',
                        lambda *a, **k: 'ns')
    monkeypatch.setattr(kubernetes_utils, 'get_context_from_config',
                        lambda *a, **k: 'ctx')
    monkeypatch.setattr(kubernetes_utils, 'filter_pods', fake_filter_pods)
    monkeypatch.setattr(instance, '_get_head_pod_name', lambda pods: head_name)
    monkeypatch.setattr(kubernetes_utils, 'check_nvidia_runtime_class',
                        lambda *a, **k: False)
    monkeypatch.setattr(instance, '_wait_for_pods_to_schedule',
                        lambda *a, **k: None)
    monkeypatch.setattr(instance, '_wait_for_pods_to_run', lambda *a, **k: None)
    monkeypatch.setattr(instance, 'is_high_availability_cluster_by_kubectl',
                        lambda *a, **k: False)


def test_create_pods_is_idempotent_when_all_pods_exist(monkeypatch):
    """Relaunching with all pods already present must create nothing.

    This pins the reattach-safety contract that lets a paused launch resume
    onto the pods it already created: _create_pods computes to_start_count == 0
    and issues no create_namespaced_pod calls, returning the existing head.
    """
    cluster_on_cloud = 'test-cluster-abc'
    head_name = f'{cluster_on_cloud}-head'
    existing = {
        head_name: _fake_pod(head_name),
        f'{cluster_on_cloud}-worker1': _fake_pod(f'{cluster_on_cloud}-worker1'),
    }
    _patch_create_pods_k8s_boundary(monkeypatch, existing, head_name)

    core_api = mock.MagicMock()
    monkeypatch.setattr(kubernetes, 'core_api', lambda *a, **k: core_api)

    # run_in_parallel drives the per-index create thread; run it inline so the
    # head/worker skip logic actually executes.
    monkeypatch.setattr(subprocess_utils, 'run_in_parallel',
                        lambda fn, items, *a, **k: [fn(i) for i in items])

    config = _make_provision_config(count=2)
    record = instance._create_pods('us', cluster_on_cloud, cluster_on_cloud,
                                   config)

    core_api.create_namespaced_pod.assert_not_called()
    assert record.created_instance_ids == []
    assert record.head_instance_id == head_name


def test_create_pods_resolves_placement_once_before_finalizing_each_pod(
        monkeypatch):
    """Provider/config reads are outside the per-Pod pure owner."""
    cluster_on_cloud = 'test-cluster-placement-owner'
    _patch_create_pods_k8s_boundary(monkeypatch, {}, None)

    resolution_calls = []
    apply_calls = []
    finalizer_calls = []
    created_specs = []
    allowed_nodes_config = {'label_selector': {'pool': 'research'}}
    original_inject = kubernetes_utils.inject_allowed_nodes_affinity
    original_finalize = instance.pod_spec_lib.finalize_pod_spec

    def get_allowed_nodes_config(_context):
        resolution_calls.append(_context)
        return allowed_nodes_config

    def inject_allowed_nodes_affinity(spec, config, *, context):
        apply_calls.append((config, context))
        return original_inject(spec, config, context=context)

    def finalize_pod_spec(*args, **kwargs):
        finalizer_calls.append(kwargs['pod_name'])
        return original_finalize(*args, **kwargs)

    def create_pod(_namespace, pod_spec, _context):
        created_specs.append(pod_spec)
        pod = types.SimpleNamespace()
        pod.metadata = types.SimpleNamespace(
            name=pod_spec['metadata']['name'],
            labels=pod_spec['metadata']['labels'],
        )
        pod.status = types.SimpleNamespace(phase='Pending')
        return pod

    monkeypatch.setattr(kubernetes_utils, 'get_allowed_nodes_config',
                        get_allowed_nodes_config)
    monkeypatch.setattr(kubernetes_utils, 'inject_allowed_nodes_affinity',
                        inject_allowed_nodes_affinity)
    monkeypatch.setattr(instance.pod_spec_lib, 'finalize_pod_spec',
                        finalize_pod_spec)
    monkeypatch.setattr(instance.volume, 'check_pvc_usage_for_pod',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(instance, '_create_namespaced_pod_with_retries',
                        create_pod)
    monkeypatch.setattr(subprocess_utils, 'run_in_parallel',
                        lambda fn, items, *_args: [fn(i) for i in items])

    config = _make_provision_config(count=2)
    record = instance._create_pods('us', cluster_on_cloud, cluster_on_cloud,
                                   config)

    assert resolution_calls == ['ctx']
    assert apply_calls == [(allowed_nodes_config, 'ctx')]
    assert finalizer_calls == [
        f'{cluster_on_cloud}-head',
        f'{cluster_on_cloud}-worker1',
    ]
    assert [spec['metadata']['name'] for spec in created_specs
           ] == finalizer_calls
    assert record.created_instance_ids == finalizer_calls


def test_create_pods_enforces_required_kueue_after_finalization(monkeypatch):
    """Custom node metadata is overwritten before the Pod API boundary."""
    cluster_on_cloud = 'test-cluster-strict-kueue'
    _patch_create_pods_k8s_boundary(monkeypatch, {}, None)
    monkeypatch.setattr(kubernetes_utils, 'get_allowed_nodes_config',
                        lambda _context: None)
    monkeypatch.setattr(kubernetes_utils, 'inject_allowed_nodes_affinity',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(instance.volume, 'check_pvc_usage_for_pod',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess_utils, 'run_in_parallel',
                        lambda fn, items, *_args: [fn(i) for i in items])
    created_specs = []

    def create_pod(_namespace, pod_spec, _context, expected_kueue_queue=None):
        created_specs.append((pod_spec, expected_kueue_queue))
        pod = types.SimpleNamespace()
        pod.metadata = types.SimpleNamespace(
            name=pod_spec['metadata']['name'],
            labels={
                **pod_spec['metadata']['labels'],
                k8s_constants.KUEUE_MANAGED_KEY: 'true',
            },
        )
        pod.status = types.SimpleNamespace(phase='Pending')
        return pod

    monkeypatch.setattr(instance, '_create_namespaced_pod_with_retries',
                        create_pod)

    config = _make_provision_config(count=1)
    config.provider_config.update({
        # A stale/hand-built provider config may carry false even though a
        # queue is present.  The provisioner must still fail closed.
        'kueue_require_managed': False,
        'kueue_local_queue_name': 'inference',
        'kueue_workload_priority_class_name': 'inference-low',
    })
    config.node_config['metadata'].update({
        'labels': {
            k8s_constants.KUEUE_QUEUE_LABEL: 'forged',
            k8s_constants.KUEUE_MANAGED_KEY: 'true',
        }
    })

    record = instance._create_pods('us', cluster_on_cloud, cluster_on_cloud,
                                   config)

    assert record.created_instance_ids == [f'{cluster_on_cloud}-head']
    assert len(created_specs) == 1
    pod_spec, expected_queue = created_specs[0]
    assert expected_queue == 'inference'
    labels = pod_spec['metadata']['labels']
    assert labels[k8s_constants.KUEUE_QUEUE_LABEL] == 'inference'
    assert labels[k8s_constants.KUEUE_POD_GROUP_LABEL] == cluster_on_cloud
    assert labels[k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL] == (
        'inference-low')
    assert k8s_constants.KUEUE_MANAGED_KEY not in labels
    assert pod_spec['spec']['schedulingGates'] == [{
        'name': k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
    }]


def test_create_pods_raises_on_more_pods_than_requested(monkeypatch):
    """More running+pending pods than requested trips the leak guard."""
    cluster_on_cloud = 'test-cluster-xyz'
    head_name = f'{cluster_on_cloud}-head'
    existing = {
        head_name: _fake_pod(head_name),
        f'{cluster_on_cloud}-worker1': _fake_pod(f'{cluster_on_cloud}-worker1'),
        f'{cluster_on_cloud}-worker2': _fake_pod(f'{cluster_on_cloud}-worker2'),
    }
    _patch_create_pods_k8s_boundary(monkeypatch, existing, head_name)

    config = _make_provision_config(count=2)
    with pytest.raises(RuntimeError, match='resource leak'):
        instance._create_pods('us', cluster_on_cloud, cluster_on_cloud, config)


def test_required_kueue_rejects_existing_unmanaged_pod(monkeypatch):
    """Strict mode never adopts a Pod that bypassed Kueue admission."""
    cluster_on_cloud = 'test-cluster-required-kueue'
    head_name = f'{cluster_on_cloud}-head'
    existing_pod = _fake_pod(head_name)
    existing_pod.metadata.labels = {
        k8s_constants.KUEUE_QUEUE_LABEL: 'inference'
    }
    existing = {head_name: existing_pod}
    _patch_create_pods_k8s_boundary(monkeypatch, existing, head_name)

    core_api = mock.MagicMock()
    monkeypatch.setattr(kubernetes, 'core_api', lambda *a, **k: core_api)

    config = _make_provision_config(count=1)
    config.provider_config.update({
        'kueue_local_queue_name': 'inference',
    })
    with pytest.raises(config_lib.KubernetesError, match='was not admitted'):
        instance._create_pods('us', cluster_on_cloud, cluster_on_cloud, config)

    core_api.delete_namespaced_pod.assert_called_once_with(
        head_name,
        'ns',
        grace_period_seconds=0,
        _request_timeout=config_lib.DELETION_TIMEOUT)


def test_required_kueue_rejects_deployment_owned_pods(monkeypatch):
    """Create-response attestation is intentionally limited to direct Pods."""
    monkeypatch.setattr(kubernetes_utils, 'get_namespace_from_config',
                        lambda *a, **k: 'ns')
    monkeypatch.setattr(kubernetes_utils, 'get_context_from_config',
                        lambda *a, **k: 'ctx')
    config = _make_provision_config(count=1)
    config.provider_config.update({
        'kueue_local_queue_name': 'inference',
    })
    config.node_config.update({
        'deployment_spec': {},
        'pvc_spec': {},
    })

    with pytest.raises(config_lib.KubernetesError,
                       match='not high-availability Deployment-owned Pods'):
        instance._create_pods('us', 'cluster', 'cluster', config)


def test_required_kueue_rejects_missing_local_queue(monkeypatch):
    monkeypatch.setattr(kubernetes_utils, 'get_namespace_from_config',
                        lambda *a, **k: 'ns')
    monkeypatch.setattr(kubernetes_utils, 'get_context_from_config',
                        lambda *a, **k: 'ctx')
    config = _make_provision_config(count=1)
    config.provider_config['kueue_require_managed'] = True

    with pytest.raises(config_lib.KubernetesError,
                       match='has no LocalQueue name'):
        instance._create_pods('us', 'cluster', 'cluster', config)


def test_out_of_cpus(monkeypatch):
    """Test to check if the error message is correct when there is only CPU resource shortage."""

    error_message = '0/7 nodes are available: 3 Insufficient cpu, 4 node(s) didn\'t match Pod\'s node affinity/selector. preemption: 0/7 nodes are available: 3 No preemption victims found for incoming pod, 4 Preemption is not helpful for scheduling.'

    namespace = 'test-namespace'
    context = 'test-context'

    new_node = mock.MagicMock()
    new_node.metadata = mock.MagicMock()
    new_node.metadata.name = 'test-node'
    new_node.status = mock.MagicMock()
    new_node.status.phase = 'Pending'
    new_node.status.conditions = [mock.MagicMock()]
    new_node.status.conditions[0].type = 'Ready'
    new_node.status.conditions[0].status = 'False'
    new_node.status.conditions[0].reason = 'InsufficientCPU'
    new_node.status.conditions[0].message = error_message

    read_namespaced_pod_mock = mock.MagicMock()
    read_namespaced_pod_mock.status.phase = 'Pending'
    read_namespaced_pod_mock.spec.node_selector = None

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_pod.return_value = read_namespaced_pod_mock

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    test_event = mock.MagicMock()
    test_event.metadata = mock.MagicMock()
    test_event.metadata.creation_timestamp = '2021-01-01T00:00:00Z'
    test_event.reason = 'FailedScheduling'
    test_event.message = error_message

    events_mock = mock.MagicMock()
    events_mock.items = [test_event]

    core_api_mock.list_namespaced_event.return_value = events_mock

    error_output = []

    def mock_warning(msg, *args, **kwargs):
        if args:
            msg = msg % args
        error_output.append(msg)

    monkeypatch.setattr(logger, 'error', mock_warning)

    with pytest.raises(config_lib.KubernetesError) as e:
        instance._raise_pod_scheduling_errors(namespace, context, [new_node])

    error_output = error_output[0].split('\n')
    error_output = _remove_colorama_escape_codes(error_output)

    assert error_output[0] == '⨯ Insufficient resource capacity on the cluster:'
    assert error_output[
        1] == '└── Cluster does not have sufficient CPUs for your request: Run \'kubectl get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.allocatable.cpu\' to check the available CPUs on the node.'

    assert len(error_output) == 2


def test_out_of_gpus(monkeypatch):
    """Test to check if the error message is correct when there is only GPU resource shortage."""

    error_message = '0/7 nodes are available: 3 Insufficient nvidia.com/gpu, 4 node(s) didn\'t match Pod\'s node affinity/selector. preemption: 0/7 nodes are available: 3 No preemption victims found for incoming pod, 4 Preemption is not helpful for scheduling.'

    namespace = 'test-namespace'
    context = 'test-context'

    new_node = mock.MagicMock()
    new_node.metadata = mock.MagicMock()
    new_node.metadata.name = 'test-node'
    new_node.status = mock.MagicMock()
    new_node.status.phase = 'Pending'
    new_node.status.conditions = [mock.MagicMock()]
    new_node.status.conditions[0].type = 'Ready'
    new_node.status.conditions[0].status = 'False'
    new_node.status.conditions[0].reason = 'InsufficientCPU'
    new_node.status.conditions[0].message = error_message

    read_namespaced_pod_mock = mock.MagicMock()
    read_namespaced_pod_mock.status.phase = 'Pending'
    read_namespaced_pod_mock.spec.node_selector = None

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_pod.return_value = read_namespaced_pod_mock

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    test_event = mock.MagicMock()
    test_event.metadata = mock.MagicMock()
    test_event.metadata.creation_timestamp = '2021-01-01T00:00:00Z'
    test_event.reason = 'FailedScheduling'
    test_event.message = error_message

    events_mock = mock.MagicMock()
    events_mock.items = [test_event]

    core_api_mock.list_namespaced_event.return_value = events_mock

    error_output = []

    def mock_warning(msg, *args, **kwargs):
        if args:
            msg = msg % args
        error_output.append(msg)

    monkeypatch.setattr(logger, 'error', mock_warning)

    with pytest.raises(config_lib.KubernetesError) as e:
        instance._raise_pod_scheduling_errors(namespace, context, [new_node])

    error_output = error_output[0].split('\n')
    # Remove any colorama escape codes
    error_output = _remove_colorama_escape_codes(error_output)

    assert error_output[0] == '⨯ Insufficient resource capacity on the cluster:'
    assert error_output[
        1] == '└── Cluster does not have sufficient GPUs for your request: Run \'sky gpus list --infra kubernetes\' to see the available GPUs.'

    assert len(error_output) == 2


def test_out_of_both_cpus_and_gpus(monkeypatch):
    error_message = '0/7 nodes are available: 3 Insufficient cpu, 3 Insufficient nvidia.com/gpu, 4 node(s) didn\'t match Pod\'s node affinity/selector. preemption: 0/7 nodes are available: 3 No preemption victims found for incoming pod, 4 Preemption is not helpful for scheduling.'

    namespace = 'test-namespace'
    context = 'test-context'

    new_node = mock.MagicMock()
    new_node.metadata = mock.MagicMock()
    new_node.metadata.name = 'test-node'
    new_node.status = mock.MagicMock()
    new_node.status.phase = 'Pending'
    new_node.status.conditions = [mock.MagicMock()]
    new_node.status.conditions[0].type = 'Ready'
    new_node.status.conditions[0].status = 'False'
    new_node.status.conditions[0].reason = 'InsufficientCPU'
    new_node.status.conditions[0].message = error_message

    read_namespaced_pod_mock = mock.MagicMock()
    read_namespaced_pod_mock.status.phase = 'Pending'
    read_namespaced_pod_mock.spec.node_selector = None

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_pod.return_value = read_namespaced_pod_mock

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    test_event = mock.MagicMock()
    test_event.metadata = mock.MagicMock()
    test_event.metadata.creation_timestamp = '2021-01-01T00:00:00Z'
    test_event.reason = 'FailedScheduling'
    test_event.message = error_message

    events_mock = mock.MagicMock()
    events_mock.items = [test_event]

    core_api_mock.list_namespaced_event.return_value = events_mock

    error_output = []

    def mock_warning(msg, *args, **kwargs):
        if args:
            msg = msg % args
        error_output.append(msg)

    monkeypatch.setattr(logger, 'error', mock_warning)

    with pytest.raises(config_lib.KubernetesError) as e:
        instance._raise_pod_scheduling_errors(namespace, context, [new_node])

    error_output = error_output[0].split('\n')
    # Remove any colorama escape codes
    error_output = _remove_colorama_escape_codes(error_output)

    assert error_output[0] == '⨯ Insufficient resource capacity on the cluster:'
    assert error_output[
        1] == '├── Cluster does not have sufficient CPUs for your request: Run \'kubectl get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.allocatable.cpu\' to check the available CPUs on the node.'
    assert error_output[
        2] == '└── Cluster does not have sufficient GPUs for your request: Run \'sky gpus list --infra kubernetes\' to see the available GPUs.'

    assert len(error_output) == 3


def test_out_of_gpus_and_node_selector_failed(monkeypatch):
    """Test to check if the error message is correct when there is GPU resource

    shortage and node selector failed to match.
    """

    error_message = '0/7 nodes are available: 3 Insufficient nvidia.com/gpu, 4 node(s) didn\'t match Pod\'s node affinity/selector. preemption: 0/7 nodes are available: 3 No preemption victims found for incoming pod, 4 Preemption is not helpful for scheduling.'

    namespace = 'test-namespace'
    context = 'test-context'

    new_node = mock.MagicMock()
    new_node.metadata = mock.MagicMock()
    new_node.metadata.name = 'test-node'
    new_node.status = mock.MagicMock()
    new_node.status.phase = 'Pending'
    new_node.status.conditions = [mock.MagicMock()]
    new_node.status.conditions[0].type = 'Ready'
    new_node.status.conditions[0].status = 'False'
    new_node.status.conditions[0].reason = 'InsufficientCPU'
    new_node.status.conditions[0].message = error_message

    read_namespaced_pod_mock = mock.MagicMock()
    read_namespaced_pod_mock.status.phase = 'Pending'
    read_namespaced_pod_mock.spec.node_selector = {
        'cloud.google.com/gke-accelerator': 'nvidia-tesla-a100'
    }

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_pod.return_value = read_namespaced_pod_mock

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    test_event = mock.MagicMock()
    test_event.metadata = mock.MagicMock()
    test_event.metadata.creation_timestamp = '2021-01-01T00:00:00Z'
    test_event.reason = 'FailedScheduling'
    test_event.message = error_message

    events_mock = mock.MagicMock()
    events_mock.items = [test_event]

    core_api_mock.list_namespaced_event.return_value = events_mock

    error_output = []

    def mock_warning(msg, *args, **kwargs):
        if args:
            msg = msg % args
        error_output.append(msg)

    monkeypatch.setattr(logger, 'error', mock_warning)

    with pytest.raises(config_lib.KubernetesError) as e:
        instance._raise_pod_scheduling_errors(namespace, context, [new_node])

    error_output = error_output[0].split('\n')
    # Remove any colorama escape codes
    error_output = _remove_colorama_escape_codes(error_output)

    assert error_output[0] == '⨯ Insufficient resource capacity on the cluster:'
    assert error_output[
        1] == '└── Cluster does not have sufficient GPUs for your request: Run \'sky gpus list --infra kubernetes\' to see the available GPUs. Verify if any node matching label nvidia-tesla-a100 and sufficient resource nvidia.com/gpu is available in the cluster.'

    assert len(error_output) == 2


def test_out_of_memory(monkeypatch):
    """Test to check if the error message is correct when there is only CPU resource shortage."""

    error_message = '0/7 nodes are available: 3 Insufficient memory, 4 node(s) didn\'t match Pod\'s node affinity/selector. preemption: 0/7 nodes are available: 3 No preemption victims found for incoming pod, 4 Preemption is not helpful for scheduling.'

    namespace = 'test-namespace'
    context = 'test-context'

    new_node = mock.MagicMock()
    new_node.metadata = mock.MagicMock()
    new_node.metadata.name = 'test-node'
    new_node.status = mock.MagicMock()
    new_node.status.phase = 'Pending'
    new_node.status.conditions = [mock.MagicMock()]
    new_node.status.conditions[0].type = 'Ready'
    new_node.status.conditions[0].status = 'False'
    new_node.status.conditions[0].reason = 'InsufficientCPU'
    new_node.status.conditions[0].message = error_message

    read_namespaced_pod_mock = mock.MagicMock()
    read_namespaced_pod_mock.status.phase = 'Pending'
    read_namespaced_pod_mock.spec.node_selector = None

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_pod.return_value = read_namespaced_pod_mock

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    test_event = mock.MagicMock()
    test_event.metadata = mock.MagicMock()
    test_event.metadata.creation_timestamp = '2021-01-01T00:00:00Z'
    test_event.reason = 'FailedScheduling'
    test_event.message = error_message

    events_mock = mock.MagicMock()
    events_mock.items = [test_event]

    core_api_mock.list_namespaced_event.return_value = events_mock

    error_output = []

    def mock_warning(msg, *args, **kwargs):
        if args:
            msg = msg % args
        error_output.append(msg)

    monkeypatch.setattr(logger, 'error', mock_warning)

    with pytest.raises(config_lib.KubernetesError) as e:
        instance._raise_pod_scheduling_errors(namespace, context, [new_node])

    error_output = error_output[0].split('\n')
    error_output = _remove_colorama_escape_codes(error_output)

    assert error_output[0] == '⨯ Insufficient resource capacity on the cluster:'
    assert error_output[
        1] == '└── Cluster does not have sufficient Memory for your request: Run \'kubectl get nodes -o custom-columns=NAME:.metadata.name,MEMORY:.status.allocatable.memory\' to check the available memory on the node.'

    assert len(error_output) == 2


def test_insufficient_resources_msg(monkeypatch):
    """Test to check if the error message is correct when there is only CPU resource shortage."""

    monkeypatch.setattr(
        cloud_vm_ray_backend.RetryingVmProvisioner,
        '__init__',
        lambda *args, **kwargs: None,
    )

    retrying_vm_prosioner = cloud_vm_ray_backend.RetryingVmProvisioner(
        log_dir=None,
        dag=None,
        optimize_target=None,
        requested_features=None,
        local_wheel_path=None,
        wheel_hash=None,
        extra_launch_context={},
    )

    zone = "Test Zone"
    region = "Test Region"
    cloud = "Test Cloud"
    requested_resources = mock.MagicMock()
    insufficient_resources = mock.MagicMock()

    to_provision = mock.MagicMock()
    to_provision.zone = zone
    to_provision.region = region
    to_provision.cloud = cloud

    num_cpus = 10
    num_memory = 10
    requested_resources = resources.Resources(cpus=num_cpus, memory=num_memory)
    insufficient_resources = ["CPUs", "Memory"]

    ssh_cloud = mock.MagicMock()
    ssh_cloud.is_same_cloud = mock.MagicMock()
    ssh_cloud.is_same_cloud.return_value = False

    monkeypatch.setattr(clouds, 'SSH', lambda *args, **kwargs: ssh_cloud)

    kubernetes_cloud = mock.MagicMock()
    kubernetes_cloud.is_same_cloud = mock.MagicMock()
    kubernetes_cloud.is_same_cloud.return_value = True

    monkeypatch.setattr(clouds, 'Kubernetes',
                        lambda *args, **kwargs: kubernetes_cloud)

    requested_resources_str = f'{requested_resources}'
    assert (
        retrying_vm_prosioner._insufficient_resources_msg(
            to_provision, requested_resources, insufficient_resources) ==
        f'Failed to acquire resources (CPUs, Memory) in {zone} for {requested_resources_str}. '
    )

    assert (
        retrying_vm_prosioner._insufficient_resources_msg(
            to_provision, requested_resources, None) ==
        f'Failed to acquire resources in {zone} for {requested_resources_str}. '
    )

    to_provision.zone = None
    assert (
        retrying_vm_prosioner._insufficient_resources_msg(
            to_provision, requested_resources, insufficient_resources) ==
        f'Failed to acquire resources (CPUs, Memory) in context {region} for {requested_resources_str}. '
    )


def test_pod_termination_reason_start_error(monkeypatch):
    """Test _get_pod_termination_reason with StartError (like busybox).

    Pod is in Failed state with container terminated due to StartError.
    """

    now = datetime.datetime(2025, 1, 1, 0, 0, 0)

    pod = mock.MagicMock()
    pod.metadata.name = 'test-pod'
    pod.status.start_time = now
    pod.status.reason = None
    pod.status.message = None

    # Ready condition showing PodFailed
    ready_condition = mock.MagicMock()
    ready_condition.type = 'Ready'
    ready_condition.reason = 'PodFailed'
    ready_condition.message = ''
    ready_condition.last_transition_time = now

    pod.status.conditions = [ready_condition]

    # Container with StartError
    container_status = mock.MagicMock()
    container_status.name = 'ray-node'
    container_status.state.terminated = mock.MagicMock()
    container_status.state.terminated.exit_code = 128
    container_status.state.terminated.reason = 'StartError'
    container_status.state.terminated.finished_at = now

    pod.status.container_statuses = [container_status]

    monkeypatch.setattr('sky.provision.kubernetes.instance.global_user_state',
                        mock.MagicMock())

    reason = instance._get_pod_termination_reason(pod, 'test-cluster')

    expected = ('Terminated unexpectedly.\n'
                'Last known state: PodFailed.\n'
                'Container errors: StartError')
    assert reason == expected


def test_pod_termination_reason_kueue_preemption(monkeypatch):
    """Test _get_pod_termination_reason with Kueue preemption.

    Pod is being terminated by Kueue due to PodsReady timeout.
    Includes both the TerminationTarget condition (preemption) and
    Ready condition (container status), as seen in real API responses.
    """

    now = datetime.datetime(2025, 1, 1, 0, 0, 0)

    pod = mock.MagicMock()
    pod.metadata.name = 'test-pod'
    pod.status.start_time = now

    ready_condition = mock.MagicMock()
    ready_condition.type = 'Ready'
    ready_condition.reason = 'ContainersNotReady'
    ready_condition.message = 'containers with unready status: [ray-node]'
    ready_condition.last_transition_time = now

    # Taken from an actual Pod that got preempted by Kueue.
    termination_condition = mock.MagicMock()
    termination_condition.type = 'TerminationTarget'
    termination_condition.reason = 'WorkloadEvictedDueToPodsReadyTimeout'
    termination_condition.message = 'Exceeded the PodsReady timeout default/test-pod'
    termination_condition.last_transition_time = now

    pod.status.conditions = [ready_condition, termination_condition]

    # Container still creating (not terminated)
    container_status = mock.MagicMock()
    container_status.state.terminated = None
    pod.status.container_statuses = [container_status]

    monkeypatch.setattr('sky.provision.kubernetes.instance.global_user_state',
                        mock.MagicMock())

    reason = instance._get_pod_termination_reason(pod, 'test-cluster')

    expected = (
        'Preempted by Kueue: WorkloadEvictedDueToPodsReadyTimeout '
        '(Exceeded the PodsReady timeout default/test-pod).\n'
        'Last known state: ContainersNotReady (containers with unready status: [ray-node]).'
    )
    assert reason == expected


def test_pod_termination_reason_null_finished_at(monkeypatch):
    """Test _get_pod_termination_reason with null finished_at timestamp.

    When pods are in certain failed states (e.g., Unknown status due to
    ephemeral storage issues), terminated.finished_at can be None.
    This should not cause a TypeError.

    Regression test for SKY-4423.
    """

    now = datetime.datetime(2025, 1, 1, 0, 0, 0)

    pod = mock.MagicMock()
    pod.metadata.name = 'test-pod'
    pod.status.start_time = now
    pod.status.reason = None
    pod.status.message = None

    # Ready condition
    ready_condition = mock.MagicMock()
    ready_condition.type = 'Ready'
    ready_condition.reason = 'PodFailed'
    ready_condition.message = ''
    ready_condition.last_transition_time = now

    pod.status.conditions = [ready_condition]

    # Container with terminated state but null finished_at
    container_status = mock.MagicMock()
    container_status.name = 'ray-node'
    container_status.state.terminated = mock.MagicMock()
    container_status.state.terminated.exit_code = 137
    container_status.state.terminated.reason = 'Unknown'
    container_status.state.terminated.finished_at = None

    pod.status.container_statuses = [container_status]

    monkeypatch.setattr('sky.provision.kubernetes.instance.global_user_state',
                        mock.MagicMock())

    # Should not raise TypeError
    reason = instance._get_pod_termination_reason(pod, 'test-cluster')

    expected = ('Terminated unexpectedly.\n'
                'Last known state: PodFailed.\n'
                'Container errors: Unknown')
    assert reason == expected


def test_list_namespaced_pod_success(monkeypatch):
    """Test that list_namespaced_pod returns pods from the API response."""
    mock_pod1 = mock.MagicMock()
    mock_pod1.metadata.name = 'test-pod-1'
    mock_pod1.status.phase = 'Running'

    mock_pod2 = mock.MagicMock()
    mock_pod2.metadata.name = 'test-pod-2'
    mock_pod2.status.phase = 'Pending'

    mock_response = mock.MagicMock()
    mock_response.items = [mock_pod1, mock_pod2]
    mock_response.api_version = 'v1'
    mock_response.kind = 'PodList'
    mock_response.metadata = mock.MagicMock()

    core_api_mock = mock.MagicMock()
    core_api_mock.list_namespaced_pod.return_value = mock_response

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    pods = instance.list_namespaced_pod(
        context='test-context',
        namespace='test-namespace',
        cluster_name_on_cloud='test-cluster',
        is_ssh=False,
        identity='Kubernetes cluster',
        label_selector='skypilot-cluster-name=test-cluster')

    assert len(pods) == 2
    assert pods[0].metadata.name == 'test-pod-1'
    assert pods[1].metadata.name == 'test-pod-2'


def test_query_instances_uses_correct_label_selector(monkeypatch):
    """Test that query_instances uses constants.TAG_SKYPILOT_CLUSTER_NAME."""
    from sky.provision import constants

    captured_label_selector = []

    def mock_list_namespaced_pod(context, namespace, cluster_name_on_cloud,
                                 is_ssh, identity, label_selector):
        captured_label_selector.append(label_selector)
        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = 'test-pod'
        mock_pod.status.phase = 'Running'

        mock_response = mock.MagicMock()
        mock_response.items = [mock_pod]
        mock_response.api_version = 'v1'
        mock_response.kind = 'PodList'
        mock_response.metadata = mock.MagicMock()
        return [mock_pod]

    monkeypatch.setattr('sky.provision.kubernetes.instance.list_namespaced_pod',
                        mock_list_namespaced_pod)
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.get_namespace_from_config',
        lambda *args: 'default')
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.get_context_from_config',
        lambda *args: 'test-context')

    instance.query_instances(cluster_name='test-cluster',
                             cluster_name_on_cloud='test-cluster-on-cloud',
                             provider_config={'namespace': 'default'},
                             retry_if_missing=False)

    # Verify the label selector was constructed correctly
    expected_label = f'{constants.TAG_SKYPILOT_CLUSTER_NAME}=test-cluster-on-cloud'
    assert captured_label_selector[0] == expected_label


def test_query_instances_retry_if_missing(monkeypatch):
    """Test that query_instances retries when retry_if_missing=True and pods are empty."""
    call_count = [0]

    def mock_list_namespaced_pod(*args, **kwargs):
        call_count[0] += 1
        # Return empty on first call, non-empty on second
        if call_count[0] == 1:
            return []
        else:
            mock_pod = mock.MagicMock()
            mock_pod.metadata.name = 'test-pod'
            mock_pod.status.phase = 'Running'
            return [mock_pod]

    monkeypatch.setattr('sky.provision.kubernetes.instance.list_namespaced_pod',
                        mock_list_namespaced_pod)
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.get_namespace_from_config',
        lambda *args: 'default')
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.get_context_from_config',
        lambda *args: 'test-context')
    # Mock time.sleep to speed up test
    monkeypatch.setattr('time.sleep', lambda *args: None)

    result = instance.query_instances(
        cluster_name='test-cluster',
        cluster_name_on_cloud='test-cluster-on-cloud',
        provider_config={'namespace': 'default'},
        retry_if_missing=True)

    # Should have retried once
    assert call_count[0] == 2
    assert len(result) == 1


def test_query_instances_retry_exhausted(monkeypatch):
    """Test that query_instances stops after max retries and returns empty dict."""
    call_count = [0]

    def mock_list_namespaced_pod(*args, **kwargs):
        call_count[0] += 1
        # Always return empty to exhaust retries
        return []

    monkeypatch.setattr('sky.provision.kubernetes.instance.list_namespaced_pod',
                        mock_list_namespaced_pod)
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.get_namespace_from_config',
        lambda *args: 'default')
    monkeypatch.setattr(
        'sky.provision.kubernetes.utils.get_context_from_config',
        lambda *args: 'test-context')
    # Mock time.sleep to speed up test
    monkeypatch.setattr('time.sleep', lambda *args: None)

    result = instance.query_instances(
        cluster_name='test-cluster',
        cluster_name_on_cloud='test-cluster-on-cloud',
        provider_config={'namespace': 'default'},
        retry_if_missing=True)

    # Should have called 1 (initial) + _MAX_QUERY_INSTANCES_RETRIES times
    assert call_count[0] == 1 + instance._MAX_QUERY_INSTANCES_RETRIES
    # Should return empty dict when no pods found
    assert result == {}


def test_get_pvc_binding_status_no_volumes(monkeypatch):
    """Test _get_pvc_binding_status with no volumes."""
    pod = mock.MagicMock()
    pod.spec.volumes = None

    result = instance._get_pvc_binding_status('test-namespace', 'test-context',
                                              pod)
    assert result is None


def test_get_pvc_binding_status_no_pvc(monkeypatch):
    """Test _get_pvc_binding_status with volumes but no PVC."""
    pod = mock.MagicMock()
    volume = mock.MagicMock()
    volume.persistent_volume_claim = None
    pod.spec.volumes = [volume]

    result = instance._get_pvc_binding_status('test-namespace', 'test-context',
                                              pod)
    assert result is None


def test_get_pvc_binding_status_bound_pvc(monkeypatch):
    """Test _get_pvc_binding_status with a bound PVC."""
    pod = mock.MagicMock()
    pvc_claim = mock.MagicMock()
    pvc_claim.claim_name = 'test-pvc'
    volume = mock.MagicMock()
    volume.persistent_volume_claim = pvc_claim
    pod.spec.volumes = [volume]

    # Mock the PVC as bound
    pvc = mock.MagicMock()
    pvc.status.phase = 'Bound'

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_persistent_volume_claim.return_value = pvc

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    result = instance._get_pvc_binding_status('test-namespace', 'test-context',
                                              pod)
    assert result is None


def test_get_pvc_binding_status_pending_pvc(monkeypatch):
    """Test _get_pvc_binding_status with a pending PVC."""
    pod = mock.MagicMock()
    pvc_claim = mock.MagicMock()
    pvc_claim.claim_name = 'test-pvc'
    volume = mock.MagicMock()
    volume.persistent_volume_claim = pvc_claim
    pod.spec.volumes = [volume]

    # Mock the PVC as pending
    pvc = mock.MagicMock()
    pvc.status.phase = 'Pending'

    # Mock the events for the PVC
    pvc_event = mock.MagicMock()
    pvc_event.type = 'Warning'
    pvc_event.reason = 'ProvisioningFailed'
    pvc_event.message = 'storageclass does not support ReadWriteMany'
    pvc_events = mock.MagicMock()
    pvc_events.items = [pvc_event]

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_persistent_volume_claim.return_value = pvc
    core_api_mock.list_namespaced_event.return_value = pvc_events

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    result = instance._get_pvc_binding_status('test-namespace', 'test-context',
                                              pod)
    assert result is not None
    assert 'test-pvc' in result
    assert 'Pending' in result
    assert 'ProvisioningFailed' in result
    assert 'storageclass does not support ReadWriteMany' in result
    assert 'kubectl describe pvc' in result
    assert 'test-namespace' in result


def test_raise_pod_scheduling_errors_pvc_unbound(monkeypatch):
    """Test that _raise_pod_scheduling_errors surfaces PVC binding issues."""
    error_message = '0/3 nodes are available: 3 pod has unbound immediate PersistentVolumeClaims.'

    namespace = 'test-namespace'
    context = 'test-context'

    new_node = mock.MagicMock()
    new_node.metadata = mock.MagicMock()
    new_node.metadata.name = 'test-node'
    new_node.status = mock.MagicMock()
    new_node.status.phase = 'Pending'

    # Mock the pod with a PVC
    pvc_claim = mock.MagicMock()
    pvc_claim.claim_name = 'test-pvc'
    volume = mock.MagicMock()
    volume.persistent_volume_claim = pvc_claim

    read_namespaced_pod_mock = mock.MagicMock()
    read_namespaced_pod_mock.status.phase = 'Pending'
    read_namespaced_pod_mock.spec.node_selector = None
    read_namespaced_pod_mock.spec.volumes = [volume]

    # Mock the PVC as pending
    pvc = mock.MagicMock()
    pvc.status.phase = 'Pending'

    # Mock the events for the PVC
    pvc_event = mock.MagicMock()
    pvc_event.type = 'Warning'
    pvc_event.reason = 'ProvisioningFailed'
    pvc_event.message = 'storageclass does not support ReadWriteMany'
    pvc_events = mock.MagicMock()
    pvc_events.items = [pvc_event]

    # Mock the pod scheduling event
    test_event = mock.MagicMock()
    test_event.metadata = mock.MagicMock()
    test_event.metadata.creation_timestamp = '2021-01-01T00:00:00Z'
    test_event.reason = 'FailedScheduling'
    test_event.message = error_message

    events_mock = mock.MagicMock()
    events_mock.items = [test_event]

    core_api_mock = mock.MagicMock()
    core_api_mock.read_namespaced_pod.return_value = read_namespaced_pod_mock
    core_api_mock.read_namespaced_persistent_volume_claim.return_value = pvc
    core_api_mock.list_namespaced_event.side_effect = [events_mock, pvc_events]

    monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                        lambda *args, **kwargs: core_api_mock)

    with pytest.raises(config_lib.KubernetesError) as exc_info:
        instance._raise_pod_scheduling_errors(namespace, context, [new_node])

    error_str = str(exc_info.value)
    # Verify that PVC binding issue is mentioned in the error
    assert 'PVC binding issue' in error_str or 'unbound' in error_str
    assert 'test-pvc' in error_str or 'PersistentVolumeClaims' in error_str


# ---------- RBAC 409 Conflict Handling Tests ----------


class FakeApiException(Exception):
    """A real exception that mimics kubernetes.client.rest.ApiException."""

    def __init__(self, status, reason='', body=''):
        super().__init__(status, reason, body)
        self.status = status
        self.reason = reason
        self.body = body


def _make_api_exception(status, reason='', body=''):
    """Create a fake Kubernetes ApiException with the given status code."""
    return FakeApiException(status, reason, body)


def _make_provider_config_for_rbac():
    """Return a minimal provider_config with all RBAC fields populated."""
    return {
        'autoscaler_service_account': {
            'metadata': {
                'name': 'skypilot-service-account',
                'namespace': 'default',
            },
        },
        'autoscaler_role': {
            'metadata': {
                'name': 'skypilot-service-account-role',
                'namespace': 'default',
            },
            'rules': [{
                'apiGroups': [''],
                'resources': ['pods'],
                'verbs': ['get', 'list'],
            }],
        },
        'autoscaler_role_binding': {
            'metadata': {
                'name': 'skypilot-service-account-role-binding',
                'namespace': 'default',
            },
            'roleRef': {
                'apiGroup': 'rbac.authorization.k8s.io',
                'kind': 'Role',
                'name': 'skypilot-service-account-role',
            },
            'subjects': [{
                'kind': 'ServiceAccount',
                'name': 'skypilot-service-account',
                'namespace': 'default',
            }],
        },
        'autoscaler_cluster_role': {
            'metadata': {
                'name': 'skypilot-service-account-cluster-role',
                'namespace': 'default',
            },
            'rules': [{
                'apiGroups': [''],
                'resources': ['nodes'],
                'verbs': ['get', 'list'],
            }],
        },
        'autoscaler_cluster_role_binding': {
            'metadata': {
                'name': 'skypilot-service-account-cluster-role-binding',
                'namespace': 'default',
            },
            'roleRef': {
                'apiGroup': 'rbac.authorization.k8s.io',
                'kind': 'ClusterRole',
                'name': 'skypilot-service-account-cluster-role',
            },
            'subjects': [{
                'kind': 'ServiceAccount',
                'name': 'skypilot-service-account',
                'namespace': 'default',
            }],
        },
    }


class TestRbac409ConflictHandling:
    """Tests that RBAC resource creation handles 409 Conflict gracefully.

    When two concurrent cluster launches both find RBAC resources missing
    and try to create them, the second one gets a 409 Conflict. The fix
    catches this, re-reads the resource, and falls through to compare/patch
    (upsert semantics).
    """

    @pytest.fixture(autouse=True)
    def mock_api_client(self, monkeypatch):
        """Mock api_client so dict_to_k8s_object works without kubeconfig."""
        import kubernetes as k8s_lib
        bare_client = k8s_lib.client.ApiClient(k8s_lib.client.Configuration())
        monkeypatch.setattr('sky.adaptors.kubernetes.api_client',
                            lambda *args, **kwargs: bare_client)

    @staticmethod
    def _make_existing_role(rules):
        """Create a mock existing role with the given rules."""
        existing = mock.MagicMock()
        existing.rules = rules
        return existing

    @staticmethod
    def _make_existing_binding(role_ref, subjects):
        """Create a mock existing binding with the given role_ref/subjects."""
        existing = mock.MagicMock()
        existing.role_ref = role_ref
        existing.subjects = subjects
        return existing

    def test_service_account_409_handled(self, monkeypatch):
        """Test 409 on create_namespaced_service_account re-reads and succeeds.
        """
        api_exc = _make_api_exception(409, 'Conflict')
        existing_sa = mock.MagicMock()

        core_api_mock = mock.MagicMock()
        # First list returns empty -> "not found", second list (after 409)
        # returns the concurrently-created resource.
        core_api_mock.list_namespaced_service_account.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_sa]),
        ]
        core_api_mock.create_namespaced_service_account.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *args, **kwargs: core_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        provider_config = _make_provider_config_for_rbac()
        # Should not raise
        config_lib._configure_autoscaler_service_account(
            'default', None, provider_config)

    def test_service_account_other_error_raised(self, monkeypatch):
        """Test that non-409 errors are still raised."""
        api_exc = _make_api_exception(500, 'Internal Server Error')

        core_api_mock = mock.MagicMock()
        core_api_mock.list_namespaced_service_account.return_value = (
            mock.MagicMock(items=[]))
        core_api_mock.create_namespaced_service_account.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *args, **kwargs: core_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: FakeApiException)

        provider_config = _make_provider_config_for_rbac()
        with pytest.raises(FakeApiException):
            config_lib._configure_autoscaler_service_account(
                'default', None, provider_config)

    def test_role_409_handled(self, monkeypatch):
        """Test 409 on create_namespaced_role re-reads and succeeds."""
        from sky.provision.kubernetes import utils as kubernetes_utils

        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        # Build the expected k8s role object so we can match its rules.
        new_role = kubernetes_utils.dict_to_k8s_object(
            provider_config['autoscaler_role'], 'V1Role')
        existing_role = self._make_existing_role(new_role.rules)

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_namespaced_role.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_role]),
        ]
        auth_api_mock.create_namespaced_role.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_role('default', None, provider_config,
                                              'autoscaler_role')
        # Rules match, so patch should NOT be called.
        auth_api_mock.patch_namespaced_role.assert_not_called()

    def test_role_409_then_patch(self, monkeypatch):
        """Test 409 on create_namespaced_role re-reads and patches when rules
        differ."""
        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        # Existing role has different rules than what we want.
        existing_role = self._make_existing_role(rules=['stale-rules'])

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_namespaced_role.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_role]),
        ]
        auth_api_mock.create_namespaced_role.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_role('default', None, provider_config,
                                              'autoscaler_role')
        # Rules differ, so patch SHOULD be called.
        auth_api_mock.patch_namespaced_role.assert_called_once()

    def test_role_binding_409_handled(self, monkeypatch):
        """Test 409 on create_namespaced_role_binding re-reads and succeeds."""
        from sky.provision.kubernetes import utils as kubernetes_utils

        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        new_rb = kubernetes_utils.dict_to_k8s_object(
            provider_config['autoscaler_role_binding'], 'V1RoleBinding')
        existing_rb = self._make_existing_binding(new_rb.role_ref,
                                                  new_rb.subjects)

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_namespaced_role_binding.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_rb]),
        ]
        auth_api_mock.create_namespaced_role_binding.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_role_binding(
            'default', None, provider_config, 'autoscaler_role_binding')
        auth_api_mock.patch_namespaced_role_binding.assert_not_called()

    def test_role_binding_409_then_patch(self, monkeypatch):
        """Test 409 on create_namespaced_role_binding re-reads and patches when
        binding differs."""
        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        # Existing binding has different subjects.
        existing_rb = self._make_existing_binding(role_ref='stale-role-ref',
                                                  subjects=['stale-subject'])

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_namespaced_role_binding.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_rb]),
        ]
        auth_api_mock.create_namespaced_role_binding.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_role_binding(
            'default', None, provider_config, 'autoscaler_role_binding')
        auth_api_mock.patch_namespaced_role_binding.assert_called_once()

    def test_cluster_role_409_handled(self, monkeypatch):
        """Test 409 on create_cluster_role re-reads and succeeds."""
        from sky.provision.kubernetes import utils as kubernetes_utils

        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        new_cr = kubernetes_utils.dict_to_k8s_object(
            provider_config['autoscaler_cluster_role'], 'V1ClusterRole')
        existing_cr = self._make_existing_role(new_cr.rules)

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_cluster_role.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_cr]),
        ]
        auth_api_mock.create_cluster_role.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_cluster_role('default', None,
                                                      provider_config)
        auth_api_mock.patch_cluster_role.assert_not_called()

    def test_cluster_role_409_then_patch(self, monkeypatch):
        """Test 409 on create_cluster_role re-reads and patches when rules
        differ."""
        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        existing_cr = self._make_existing_role(rules=['stale-rules'])

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_cluster_role.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_cr]),
        ]
        auth_api_mock.create_cluster_role.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_cluster_role('default', None,
                                                      provider_config)
        auth_api_mock.patch_cluster_role.assert_called_once()

    def test_cluster_role_binding_409_handled(self, monkeypatch):
        """Test 409 on create_cluster_role_binding re-reads and succeeds."""
        from sky.provision.kubernetes import utils as kubernetes_utils

        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        new_binding = kubernetes_utils.dict_to_k8s_object(
            provider_config['autoscaler_cluster_role_binding'],
            'V1ClusterRoleBinding')
        existing_binding = self._make_existing_binding(new_binding.role_ref,
                                                       new_binding.subjects)

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_cluster_role_binding.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_binding]),
        ]
        auth_api_mock.create_cluster_role_binding.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_cluster_role_binding(
            'default', None, provider_config)
        auth_api_mock.patch_cluster_role_binding.assert_not_called()

    def test_cluster_role_binding_409_then_patch(self, monkeypatch):
        """Test 409 on create_cluster_role_binding re-reads and patches when
        binding differs."""
        api_exc = _make_api_exception(409, 'Conflict')
        provider_config = _make_provider_config_for_rbac()

        existing_binding = self._make_existing_binding(
            role_ref='stale-role-ref', subjects=['stale-subject'])

        auth_api_mock = mock.MagicMock()
        auth_api_mock.list_cluster_role_binding.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(items=[existing_binding]),
        ]
        auth_api_mock.create_cluster_role_binding.side_effect = api_exc

        monkeypatch.setattr('sky.adaptors.kubernetes.auth_api',
                            lambda *args, **kwargs: auth_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *args, **kwargs: type(api_exc))

        config_lib._configure_autoscaler_cluster_role_binding(
            'default', None, provider_config)
        auth_api_mock.patch_cluster_role_binding.assert_called_once()


class TestRuntimeClassOverride:
    """Tests for runtimeClassName override behavior in GPU pod specs.

    When nvidia runtime exists and GPU pods are requested, SkyPilot
    auto-sets runtimeClassName to 'nvidia'. pod_config should be able
    to override or remove this setting.
    """

    @staticmethod
    def _apply_runtime_class_logic(pod_spec, nvidia_runtime_exists,
                                   needs_gpus_nvidia):
        """Replicates the runtimeClassName logic from _create_pods."""
        if nvidia_runtime_exists and needs_gpus_nvidia:
            if 'runtimeClassName' not in pod_spec['spec']:
                pod_spec['spec']['runtimeClassName'] = 'nvidia'
            elif not pod_spec['spec']['runtimeClassName']:
                del pod_spec['spec']['runtimeClassName']

    def test_default_sets_nvidia_runtime(self):
        """No runtimeClassName in pod_config -> auto-set to 'nvidia'."""
        pod_spec = {'spec': {'containers': [{}]}}
        self._apply_runtime_class_logic(pod_spec,
                                        nvidia_runtime_exists=True,
                                        needs_gpus_nvidia=True)
        assert pod_spec['spec']['runtimeClassName'] == 'nvidia'

    def test_pod_config_overrides_runtime(self):
        """pod_config sets a custom runtimeClassName -> respected."""
        pod_spec = {'spec': {'containers': [{}], 'runtimeClassName': 'custom'}}
        self._apply_runtime_class_logic(pod_spec,
                                        nvidia_runtime_exists=True,
                                        needs_gpus_nvidia=True)
        assert pod_spec['spec']['runtimeClassName'] == 'custom'

    def test_pod_config_null_removes_runtime(self):
        """pod_config sets runtimeClassName to None -> field removed."""
        pod_spec = {'spec': {'containers': [{}], 'runtimeClassName': None}}
        self._apply_runtime_class_logic(pod_spec,
                                        nvidia_runtime_exists=True,
                                        needs_gpus_nvidia=True)
        assert 'runtimeClassName' not in pod_spec['spec']

    def test_pod_config_empty_string_removes_runtime(self):
        """pod_config sets runtimeClassName to '' -> field removed."""
        pod_spec = {'spec': {'containers': [{}], 'runtimeClassName': ''}}
        self._apply_runtime_class_logic(pod_spec,
                                        nvidia_runtime_exists=True,
                                        needs_gpus_nvidia=True)
        assert 'runtimeClassName' not in pod_spec['spec']

    def test_no_nvidia_runtime_no_change(self):
        """nvidia runtime doesn't exist -> no runtimeClassName set."""
        pod_spec = {'spec': {'containers': [{}]}}
        self._apply_runtime_class_logic(pod_spec,
                                        nvidia_runtime_exists=False,
                                        needs_gpus_nvidia=True)
        assert 'runtimeClassName' not in pod_spec['spec']

    def test_no_gpu_no_change(self):
        """No GPU requested -> no runtimeClassName set."""
        pod_spec = {'spec': {'containers': [{}]}}
        self._apply_runtime_class_logic(pod_spec,
                                        nvidia_runtime_exists=True,
                                        needs_gpus_nvidia=False)
        assert 'runtimeClassName' not in pod_spec['spec']


class TestWaitForPodsToScheduleAutoscaleTimeout:
    """Tests for the autoscaler-aware timeout extension in
    _wait_for_pods_to_schedule.

    The production bug: when an autoscaler is configured, node scale-up can
    take 10+ minutes, but the default provision_timeout (10s) is tuned for
    normal scheduling latency. Tests verify that once autoscaling is
    detected, the deadline is extended from the detection moment.
    """

    def test_empty_pod_set_is_a_noop(self, monkeypatch):
        """An empty creation result must not touch config or Kubernetes."""
        config_lookup = mock.MagicMock()
        core_api = mock.MagicMock()
        monkeypatch.setattr('sky.skypilot_config.get_effective_region_config',
                            config_lookup)
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api', core_api)

        instance._wait_for_pods_to_schedule(
            namespace='ns',
            context='test-context',
            new_nodes=[],
            timeout=5,
            cluster_name='cn',
            create_pods_start=mock.sentinel.create_pods_start)

        config_lookup.assert_not_called()
        core_api.assert_not_called()

    class _FakeClock:
        """Deterministic clock that advances only when sleep() is called.

        Replaces time.time()/time.sleep() in the instance module so the
        while loop in _wait_for_pods_to_schedule is driven by simulated
        time rather than wall-clock time.
        """

        def __init__(self):
            self.now = 0.0

        def time(self):
            return self.now

        def sleep(self, secs):
            self.now += secs

    @staticmethod
    def _make_node(name: str, cluster_name_on_cloud: str):
        """Build a mock new_node (used to derive expected pod names)."""
        from sky.provision import constants as prov_constants
        node = mock.MagicMock()
        node.metadata.name = name
        node.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud
        }
        return node

    @staticmethod
    def _make_pending_pod(name: str, cluster_name_on_cloud: str):
        """Build a pod that is Pending and not yet bound by the scheduler.

        This represents a pod that has not yet been scheduled — no
        spec.node_name and no PodScheduled=True condition — so the loop
        should keep waiting for it (and eventually time out).
        """
        from sky.provision import constants as prov_constants
        pod = mock.MagicMock()
        pod.metadata.name = name
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud
        }
        pod.status.phase = 'Pending'
        pod.status.container_statuses = None
        # The scheduler has not bound this pod: no node assignment and no
        # PodScheduled=True condition. Set explicitly so the MagicMock does
        # not auto-create truthy attributes that _pod_is_scheduled would
        # misread as "bound".
        pod.spec.node_name = None
        pod.status.conditions = []
        return pod

    def _setup(self, monkeypatch, autoscaler_type, autoscale_detected):
        """Wire up all mocks. Returns (clock, raise_errors_mock)."""

        # 1. Config lookup — return the autoscaler type when asked.
        def mock_config(cloud, region, keys, default_value=None, **kwargs):
            if keys == ('autoscaler',):
                return autoscaler_type
            return default_value

        monkeypatch.setattr('sky.skypilot_config.get_effective_region_config',
                            mock_config)

        # 2. k8s core API — always return the same pending pod.
        cluster_name_on_cloud = 'my-cluster'
        pod = self._make_pending_pod('pod-0', cluster_name_on_cloud)
        pods_list = mock.MagicMock()
        pods_list.items = [pod]
        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.return_value = pods_list
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)

        # 3. Autoscale detection — return caller-supplied flag.
        monkeypatch.setattr(pod_scheduling, '_cluster_had_autoscale_event',
                            lambda *a, **kw: autoscale_detected)
        monkeypatch.setattr(pod_scheduling, '_cluster_maybe_autoscaling',
                            lambda *a, **kw: autoscale_detected)

        # 4. Replace the slow error-surfacing path with a simple marker
        #    so we can cheaply detect that the timeout path fired.
        raise_errors = mock.MagicMock(
            side_effect=config_lib.KubernetesError('simulated-timeout'))
        monkeypatch.setattr(pod_scheduling, '_raise_pod_scheduling_errors',
                            raise_errors)

        # 5. Deterministic clock — advances only via sleep().
        clock = self._FakeClock()
        monkeypatch.setattr(pod_scheduling.time, 'time', clock.time)
        monkeypatch.setattr(pod_scheduling.time, 'sleep', clock.sleep)

        # 6. No-op spinner update to avoid rich_utils side effects.
        monkeypatch.setattr('sky.utils.rich_utils.force_update_status',
                            lambda *a, **kw: None)

        return clock, raise_errors, cluster_name_on_cloud

    def test_timeout_fires_without_autoscaler(self, monkeypatch):
        """Without any autoscaler configured, the original timeout is
        enforced — the function should exit the loop and raise once the
        user-specified timeout elapses."""
        _, raise_errors, cluster_name_on_cloud = self._setup(
            monkeypatch, autoscaler_type=None, autoscale_detected=False)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        with pytest.raises(config_lib.KubernetesError,
                           match='simulated-timeout'):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=5,
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        assert raise_errors.called, (
            'Without autoscaler, timeout=5s must trigger the error path.')

    def test_indefinite_wait_skips_karpenter_gpu_fast_fail(self, monkeypatch):
        """A negative timeout retains a fixed-pool pod until it can schedule."""
        _, raise_errors, cluster_name_on_cloud = self._setup(
            monkeypatch, autoscaler_type=None, autoscale_detected=False)
        pending = self._make_pending_pod('pod-0', cluster_name_on_cloud)
        pending.metadata.uid = 'pod-uid'
        scheduled = self._make_pending_pod('pod-0', cluster_name_on_cloud)
        scheduled.metadata.uid = 'pod-uid'
        scheduled.spec.node_name = 'gpu-node'

        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.side_effect = [
            types.SimpleNamespace(items=[pending]),
            types.SimpleNamespace(items=[scheduled]),
        ]
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)
        fast_fail = mock.MagicMock(
            side_effect=config_lib.KubernetesError('must-not-fast-fail'))
        monkeypatch.setattr(pod_scheduling,
                            '_raise_for_karpenter_gpu_incompatibility',
                            fast_fail)

        node = self._make_node('pod-0', cluster_name_on_cloud)
        instance._wait_for_pods_to_schedule(
            namespace='ns',
            context='test-context',
            new_nodes=[node],
            timeout=-1,
            cluster_name='cn',
            create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        fast_fail.assert_not_called()
        raise_errors.assert_not_called()
        assert core_api.list_namespaced_pod.call_count == 2

    def test_finite_wait_preserves_karpenter_gpu_fast_fail(self, monkeypatch):
        """A finite timeout retains the existing fast-fallback behavior."""
        _, raise_errors, cluster_name_on_cloud = self._setup(
            monkeypatch, autoscaler_type=None, autoscale_detected=False)
        pending = self._make_pending_pod('pod-0', cluster_name_on_cloud)
        pending.metadata.uid = 'pod-uid'

        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.return_value = types.SimpleNamespace(
            items=[pending])
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)
        fast_fail = mock.MagicMock(
            side_effect=config_lib.KubernetesError('expected-fast-fail'))
        monkeypatch.setattr(pod_scheduling,
                            '_raise_for_karpenter_gpu_incompatibility',
                            fast_fail)

        node = self._make_node('pod-0', cluster_name_on_cloud)
        with pytest.raises(config_lib.KubernetesError,
                           match='expected-fast-fail'):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=5,
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        fast_fail.assert_called_once()
        raise_errors.assert_not_called()

    def test_autoscale_detection_extends_deadline(self, monkeypatch):
        """When autoscaling is detected, the deadline is extended from the
        detection moment by _AUTOSCALE_DETECTED_TIMEOUT_SECONDS. A short
        user timeout alone would exit in seconds, but the extension keeps
        the loop alive for much longer."""
        clock, raise_errors, cluster_name_on_cloud = self._setup(
            monkeypatch, autoscaler_type='gke', autoscale_detected=True)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        with pytest.raises(config_lib.KubernetesError):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=5,  # far shorter than the 900s extension
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        # The loop sleeps 1s per iteration via the fake clock. If the
        # extension did NOT apply we would exit after ~5s of simulated
        # time. It must run for at least the extension window instead.
        assert clock.now >= instance._AUTOSCALE_DETECTED_TIMEOUT_SECONDS, (
            f'Expected simulated time >= '
            f'{instance._AUTOSCALE_DETECTED_TIMEOUT_SECONDS}s after '
            f'autoscale detection, but got {clock.now}s — the extension '
            f'did not take effect.')
        assert raise_errors.called

    def test_autoscale_extension_does_not_shorten_user_timeout(
            self, monkeypatch):
        """If the user set a provision_timeout longer than the extension
        window, their value must still be honored (max of the two)."""
        clock, _, cluster_name_on_cloud = self._setup(monkeypatch,
                                                      autoscaler_type='gke',
                                                      autoscale_detected=True)

        node = self._make_node('pod-0', cluster_name_on_cloud)
        long_timeout = instance._AUTOSCALE_DETECTED_TIMEOUT_SECONDS + 600

        with pytest.raises(config_lib.KubernetesError):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=long_timeout,
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        # The function should run at least until the longer user timeout
        # elapses, even though the extension window expired earlier.
        assert clock.now >= long_timeout, (
            f'User-specified timeout of {long_timeout}s must not be '
            f'shortened by the autoscale extension; loop ran for only '
            f'{clock.now}s.')

    def test_karpenter_heuristic_does_not_extend_deadline(self, monkeypatch):
        """Karpenter does not emit TriggeredScaleUp; the code falls back
        to heuristic FailedScheduling detection. That signal is NOT
        reliable enough (same event fires for oversized requests,
        taints, PVC binding errors, etc.) to extend the deadline by
        15 min, so the heuristic path must only update the spinner
        message and leave the deadline alone.

        The autoscaler-configured initial minimum timeout still applies,
        but nothing beyond that.
        """
        clock, _, cluster_name_on_cloud = self._setup(
            monkeypatch, autoscaler_type='karpenter', autoscale_detected=True)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        with pytest.raises(config_lib.KubernetesError):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=5,
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        # Initial timeout is bumped to the autoscaler minimum (60s), but
        # the 15 min extension must NOT apply under the heuristic path.
        assert clock.now >= instance._AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS, (
            f'Expected at least the autoscaler initial minimum '
            f'({instance._AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS}s) of '
            f'waiting, got {clock.now}s.')
        assert clock.now < instance._AUTOSCALE_DETECTED_TIMEOUT_SECONDS, (
            f'Heuristic FailedScheduling detection must NOT extend the '
            f'deadline by the full 15 min window, but loop ran for '
            f'{clock.now}s.')

    def test_autoscaler_configured_bumps_short_timeout_to_minimum(
            self, monkeypatch):
        """The default provision_timeout (10s) is shorter than the
        Cluster Autoscaler scan interval (~10s), so with a vanilla
        config the loop would exit before any TriggeredScaleUp could
        be emitted. When an autoscaler is configured, the initial
        timeout must be bumped to at least
        _AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS so detection has a
        chance to run."""
        clock, _, cluster_name_on_cloud = self._setup(monkeypatch,
                                                      autoscaler_type='gke',
                                                      autoscale_detected=False)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        with pytest.raises(config_lib.KubernetesError):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=10,  # default; shorter than CA scan interval
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        # No detection → no 15 min extension. But the initial timeout
        # must have been bumped to the autoscaler minimum, so the loop
        # should run for at least that long.
        assert clock.now >= instance._AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS, (
            f'Autoscaler-configured timeout should be bumped to >= '
            f'{instance._AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS}s, but '
            f'loop ran only {clock.now}s.')
        assert clock.now < instance._AUTOSCALE_DETECTED_TIMEOUT_SECONDS, (
            f'No TriggeredScaleUp detected → 15 min extension must not '
            f'apply, but loop ran for {clock.now}s.')

    def test_no_autoscaler_does_not_bump_timeout(self, monkeypatch):
        """Without an autoscaler configured, the initial-minimum bump
        must NOT apply — a user who explicitly sets a short timeout on
        a non-autoscaling cluster expects it to be honored."""
        clock, _, cluster_name_on_cloud = self._setup(monkeypatch,
                                                      autoscaler_type=None,
                                                      autoscale_detected=False)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        with pytest.raises(config_lib.KubernetesError):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=5,
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        # Without autoscaler, the 5s timeout must be honored (not bumped
        # to the 60s autoscaler minimum). Loop should exit shortly after
        # 5s — generously below the autoscaler minimum.
        assert clock.now < instance._AUTOSCALE_INITIAL_MIN_TIMEOUT_SECONDS, (
            f'No autoscaler configured → short user timeout must not be '
            f'bumped, but loop ran for {clock.now}s.')

    def test_emits_launch_progress_on_autoscale_detection(self, monkeypatch):
        """When the autoscaler is detected, exactly one LAUNCH_PROGRESS event
        must be emitted with the spinner's status text."""
        _, raise_errors, cluster_name_on_cloud = self._setup(
            monkeypatch,
            autoscaler_type='gke',
            autoscale_detected=True,
        )

        add_event = mock.MagicMock()
        monkeypatch.setattr(instance.global_user_state, 'add_cluster_event',
                            add_event)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        with pytest.raises(config_lib.KubernetesError,
                           match='simulated-timeout'):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=5,
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        # The autoscaler branch latches once — exactly one LAUNCH_PROGRESS emit.
        launch_progress_calls = [
            call for call in add_event.call_args_list
            if call.kwargs.get('event_type') is
            instance.global_user_state.ClusterEventType.LAUNCH_PROGRESS
        ]
        assert len(launch_progress_calls) == 1
        kwargs = launch_progress_calls[0].kwargs
        assert kwargs['reason'].startswith('Launching (')
        assert kwargs['nop_if_duplicate'] is True


class TestKarpenterGpuSchedulingFastFail:
    """Tests the bounded Karpenter FailedScheduling Event diagnosis."""

    _MESSAGE = ('Failed to schedule pod, incompatible requirements, label '
                '"nvidia.com/gpu.product" does not have known values')

    @pytest.fixture(autouse=True)
    def _clear_cache(self, monkeypatch, tmp_path):
        monkeypatch.setattr(pod_scheduling,
                            '_FAILED_SCHEDULING_EVENT_SHARED_CACHE_DIR',
                            str(tmp_path / 'failed-scheduling-event-cache'))
        pod_scheduling._clear_failed_scheduling_event_cache_for_testing()
        yield
        pod_scheduling._clear_failed_scheduling_event_cache_for_testing()

    @staticmethod
    def _event(*,
               uid='pod-uid',
               message=None,
               occurrence=None,
               creation_timestamp=None,
               event_time=None,
               last_timestamp=None,
               series_timestamp=None,
               reason='FailedScheduling',
               event_type='Warning',
               reporting_component='karpenter',
               source_component=None):
        if message is None:
            message = TestKarpenterGpuSchedulingFastFail._MESSAGE
        if occurrence is not None:
            creation_timestamp = occurrence
        return types.SimpleNamespace(
            reason=reason,
            type=event_type,
            message=message,
            reporting_component=reporting_component,
            source=types.SimpleNamespace(component=source_component),
            involved_object=types.SimpleNamespace(uid=uid),
            series=(types.SimpleNamespace(last_observed_time=series_timestamp)
                    if series_timestamp is not None else None),
            event_time=event_time,
            last_timestamp=last_timestamp,
            metadata=types.SimpleNamespace(
                creation_timestamp=creation_timestamp),
        )

    @staticmethod
    def _patch_events(monkeypatch, events):
        core_api = mock.MagicMock()
        core_api.list_namespaced_event.return_value = types.SimpleNamespace(
            items=events)
        monkeypatch.setattr(kubernetes, 'core_api', lambda *a, **k: core_api)
        return core_api

    def test_exact_current_event_fast_fails_with_gpu_classification(
            self, monkeypatch):
        cutoff = datetime.datetime(2026,
                                   7,
                                   20,
                                   17,
                                   tzinfo=datetime.timezone.utc)
        events = [
            self._event(uid='pod-uid',
                        occurrence=cutoff,
                        reporting_component='default-scheduler'),
            self._event(uid='pod-uid', occurrence=cutoff),
        ]
        core_api = self._patch_events(monkeypatch, events)

        with pytest.raises(config_lib.KubernetesError) as exc_info:
            pod_scheduling._raise_for_karpenter_gpu_incompatibility(
                'ns', 'ctx', {'pod-uid'}, cutoff)

        assert exc_info.value.insufficent_resources == ['GPUs']
        core_api.list_namespaced_event.assert_called_once_with(
            namespace='ns',
            field_selector='reason=FailedScheduling',
            _request_timeout=kubernetes.API_TIMEOUT)

    def test_source_component_can_identify_karpenter(self):
        event = self._event(occurrence=datetime.datetime(2026, 7, 20, 17),
                            reporting_component='another-reporter',
                            source_component='karpenter')
        assert pod_scheduling._karpenter_gpu_incompatibility(event) is not None

    @pytest.mark.parametrize('event_kwargs', [
        {
            'message': _MESSAGE + '; another NodePool is temporarily full'
        },
        {
            'message': ('incompatible requirements, label '
                        '"karpenter.k8s.aws/instance-family" does not '
                        'have known values')
        },
        {
            'event_type': 'Normal'
        },
    ])
    def test_ambiguous_or_non_gpu_event_does_not_match(self, event_kwargs):
        event = self._event(occurrence=datetime.datetime(2026, 7, 20, 17),
                            **event_kwargs)
        assert pod_scheduling._karpenter_gpu_incompatibility(event) is None

    def test_old_or_wrong_uid_event_does_not_fast_fail(self, monkeypatch):
        cutoff = datetime.datetime(2026,
                                   7,
                                   20,
                                   17,
                                   tzinfo=datetime.timezone.utc)
        old = cutoff - datetime.timedelta(seconds=1)
        self._patch_events(monkeypatch, [
            self._event(uid='pod-uid', occurrence=old),
            self._event(uid='other-uid', occurrence=cutoff),
        ])

        pod_scheduling._raise_for_karpenter_gpu_incompatibility(
            'ns', 'ctx', {'pod-uid'}, cutoff)

    def test_latest_coalesced_occurrence_and_timestamp_precedence(self):
        creation = datetime.datetime(2026, 7, 20, 15)
        last = datetime.datetime(2026, 7, 20, 16, tzinfo=datetime.timezone.utc)
        event_time = datetime.datetime(2026,
                                       7,
                                       20,
                                       17,
                                       tzinfo=datetime.timezone.utc)
        series = datetime.datetime(2026,
                                   7,
                                   20,
                                   18,
                                   tzinfo=datetime.timezone.utc)
        event = self._event(creation_timestamp=creation,
                            last_timestamp=last,
                            event_time=event_time,
                            series_timestamp=series)

        assert pod_scheduling._failed_scheduling_event_occurrence(
            event) == series
        event.series = None
        assert pod_scheduling._failed_scheduling_event_occurrence(
            event) == event_time
        event.event_time = None
        assert pod_scheduling._failed_scheduling_event_occurrence(event) == last
        event.last_timestamp = None
        assert pod_scheduling._failed_scheduling_event_occurrence(
            event) == creation.replace(tzinfo=datetime.timezone.utc)

    def test_old_creation_with_fresh_series_occurrence_fast_fails(
            self, monkeypatch):
        cutoff = datetime.datetime(2026,
                                   7,
                                   20,
                                   17,
                                   tzinfo=datetime.timezone.utc)
        self._patch_events(monkeypatch, [
            self._event(creation_timestamp=cutoff - datetime.timedelta(hours=1),
                        series_timestamp=cutoff)
        ])

        with pytest.raises(config_lib.KubernetesError):
            pod_scheduling._raise_for_karpenter_gpu_incompatibility(
                'ns', 'ctx', {'pod-uid'}, cutoff)

    def test_event_api_failure_is_negatively_cached(self, monkeypatch):
        core_api = mock.MagicMock()
        core_api.list_namespaced_event.side_effect = RuntimeError('api down')
        monkeypatch.setattr(kubernetes, 'core_api', lambda *a, **k: core_api)

        assert pod_scheduling._get_failed_scheduling_event_matches('ns',
                                                                   'ctx') == {}
        assert pod_scheduling._get_failed_scheduling_event_matches('ns',
                                                                   'ctx') == {}
        assert core_api.list_namespaced_event.call_count == 1

    def test_shared_snapshot_survives_process_local_cache_clear(
            self, monkeypatch):
        occurrence = datetime.datetime.now(datetime.timezone.utc)
        core_api = self._patch_events(monkeypatch,
                                      [self._event(occurrence=occurrence)])

        first = pod_scheduling._get_failed_scheduling_event_matches('ns', 'ctx')
        pod_scheduling._clear_failed_scheduling_event_cache_for_testing()
        second = pod_scheduling._get_failed_scheduling_event_matches(
            'ns', 'ctx')

        assert first == second
        assert core_api.list_namespaced_event.call_count == 1

    def test_classifier_and_heuristic_share_one_concurrent_refresh(
            self, monkeypatch):
        occurrence = datetime.datetime.now(datetime.timezone.utc)
        core_api = self._patch_events(monkeypatch,
                                      [self._event(occurrence=occurrence)])
        barrier = threading.Barrier(8)
        results = []

        def classify():
            barrier.wait()
            results.append(
                bool(
                    pod_scheduling._get_failed_scheduling_event_matches(
                        'ns', 'ctx')))

        def detect_heuristic():
            barrier.wait()
            results.append(
                pod_scheduling._cluster_maybe_autoscaling(
                    'ns', 'ctx', occurrence - datetime.timedelta(seconds=1)))

        threads = [threading.Thread(target=classify) for _ in range(4)]
        threads += [threading.Thread(target=detect_heuristic) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()

        assert all(results)
        assert core_api.list_namespaced_event.call_count == 1

    def test_concurrent_callers_share_one_refresh(self, monkeypatch):
        entered = threading.Event()
        release = threading.Event()
        call_count = 0
        call_count_lock = threading.Lock()
        occurrence = datetime.datetime.now(datetime.timezone.utc)

        def list_events(**kwargs):
            del kwargs
            nonlocal call_count
            with call_count_lock:
                call_count += 1
            entered.set()
            assert release.wait(timeout=5)
            return types.SimpleNamespace(
                items=[self._event(occurrence=occurrence)])

        core_api = mock.MagicMock()
        core_api.list_namespaced_event.side_effect = list_events
        monkeypatch.setattr(kubernetes, 'core_api', lambda *a, **k: core_api)
        results = []

        def get_matches():
            results.append(
                pod_scheduling._get_failed_scheduling_event_matches(
                    'ns', 'ctx'))

        threads = [threading.Thread(target=get_matches) for _ in range(8)]
        for thread in threads:
            thread.start()
        assert entered.wait(timeout=5)
        release.set()
        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()

        assert call_count == 1
        assert all(result['pod-uid'][0] == occurrence for result in results)

    @pytest.mark.skipif('fork' not in multiprocessing.get_all_start_methods(),
                        reason='requires process-level file-lock contention')
    def test_processes_share_one_refresh_and_retry_from_snapshot(
            self, monkeypatch):
        del monkeypatch
        process_context = multiprocessing.get_context('fork')
        process_count = 4
        barrier = process_context.Barrier(process_count)
        refresh_entered = process_context.Event()
        release_refresh = process_context.Event()
        snapshot_ready = process_context.Event()
        refresh_count = process_context.Value('i', 0)
        results = process_context.Queue()
        occurrence = datetime.datetime.now(datetime.timezone.utc)
        event = self._event(occurrence=occurrence)
        cache_dir = pod_scheduling._FAILED_SCHEDULING_EVENT_SHARED_CACHE_DIR

        def worker():
            pod_scheduling._FAILED_SCHEDULING_EVENT_SHARED_CACHE_DIR = cache_dir
            pod_scheduling._clear_failed_scheduling_event_cache_for_testing()

            class CoreApi:

                def list_namespaced_event(self, **kwargs):
                    del kwargs
                    with refresh_count.get_lock():
                        refresh_count.value += 1
                    refresh_entered.set()
                    assert release_refresh.wait(timeout=10)
                    return types.SimpleNamespace(items=[event])

            kubernetes.core_api = lambda *args, **kwargs: CoreApi()
            barrier.wait(timeout=10)
            first = pod_scheduling._get_failed_scheduling_event_matches(
                'ns', 'ctx')
            if first:
                snapshot_ready.set()
            else:
                assert snapshot_ready.wait(timeout=10)
            second = first
            for _ in range(20):
                if second:
                    break
                time.sleep(0.05)
                second = pod_scheduling._get_failed_scheduling_event_matches(
                    'ns', 'ctx')
            results.put((bool(first), bool(second)))

        processes = [
            process_context.Process(target=worker) for _ in range(process_count)
        ]
        for process in processes:
            process.start()
        assert refresh_entered.wait(timeout=10)
        # Keep the winner in the Kubernetes read long enough for every other
        # process to take the nonblocking lock-contention path.
        time.sleep(0.5)
        release_refresh.set()
        for process in processes:
            process.join(timeout=15)
            assert process.exitcode == 0

        process_results = [results.get(timeout=5) for _ in range(process_count)]
        assert refresh_count.value == 1
        assert sum(first for first, _ in process_results) == 1
        assert all(second for _, second in process_results)

    def test_cache_does_not_evict_pinned_entries(self, monkeypatch):
        monkeypatch.setattr(pod_scheduling,
                            '_FAILED_SCHEDULING_EVENT_CACHE_MAX_ENTRIES', 2)
        first = pod_scheduling._pin_failed_scheduling_event_cache_entry(
            'ctx', 'first')
        second = pod_scheduling._pin_failed_scheduling_event_cache_entry(
            'ctx', 'second')
        assert first is not None
        assert second is not None
        assert pod_scheduling._pin_failed_scheduling_event_cache_entry(
            'ctx', 'third') is None

        pod_scheduling._release_failed_scheduling_event_cache_entry(second)
        third = pod_scheduling._pin_failed_scheduling_event_cache_entry(
            'ctx', 'third')
        assert third is not None
        assert ('ctx', 'first') in pod_scheduling._FAILED_SCHEDULING_EVENT_CACHE
        assert ('ctx',
                'second') not in pod_scheduling._FAILED_SCHEDULING_EVENT_CACHE
        pod_scheduling._release_failed_scheduling_event_cache_entry(third)
        pod_scheduling._release_failed_scheduling_event_cache_entry(first)

    def test_cache_entry_and_uid_match_bounds(self, monkeypatch):
        monkeypatch.setattr(pod_scheduling,
                            '_FAILED_SCHEDULING_EVENT_CACHE_MAX_ENTRIES', 2)
        monkeypatch.setattr(pod_scheduling,
                            '_FAILED_SCHEDULING_EVENT_CACHE_MAX_UID_MATCHES', 3)
        cutoff = datetime.datetime(2026,
                                   7,
                                   20,
                                   17,
                                   tzinfo=datetime.timezone.utc)
        events = [
            self._event(uid=f'uid-{index}',
                        occurrence=cutoff + datetime.timedelta(seconds=index))
            for index in range(5)
        ]
        self._patch_events(monkeypatch, events)

        matches = pod_scheduling._get_failed_scheduling_event_matches(
            'ns-1', 'ctx')
        assert set(matches) == {'uid-2', 'uid-3', 'uid-4'}
        pod_scheduling._get_failed_scheduling_event_matches('ns-2', 'ctx')
        pod_scheduling._get_failed_scheduling_event_matches('ns-3', 'ctx')
        assert len(pod_scheduling._FAILED_SCHEDULING_EVENT_CACHE) == 2

    def test_shared_bucket_selection_is_stable(self):
        first = pod_scheduling._failed_scheduling_event_shared_cache_paths(
            'ctx', 'ns')
        second = pod_scheduling._failed_scheduling_event_shared_cache_paths(
            'ctx', 'ns')
        assert first == second
        assert first[0].endswith('.json')
        assert first[1].endswith('.lock')
        assert first[2].endswith('.json.tmp')

    def test_full_fresh_shared_bucket_skips_colliding_identity(
            self, monkeypatch):
        monkeypatch.setattr(pod_scheduling,
                            '_FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKETS', 1)
        monkeypatch.setattr(
            pod_scheduling,
            '_FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKET_MAX_ENTRIES', 2)
        occurrence = datetime.datetime.now(datetime.timezone.utc)
        core_api = self._patch_events(monkeypatch,
                                      [self._event(occurrence=occurrence)])

        assert pod_scheduling._get_failed_scheduling_event_matches(
            'ns-1', 'ctx')
        assert pod_scheduling._get_failed_scheduling_event_matches(
            'ns-2', 'ctx')
        assert pod_scheduling._get_failed_scheduling_event_matches(
            'ns-3', 'ctx') == {}
        assert core_api.list_namespaced_event.call_count == 2

    def test_expired_shared_bucket_entry_is_replaced(self, monkeypatch):
        monkeypatch.setattr(pod_scheduling,
                            '_FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKETS', 1)
        monkeypatch.setattr(
            pod_scheduling,
            '_FAILED_SCHEDULING_EVENT_SHARED_CACHE_BUCKET_MAX_ENTRIES', 1)
        monkeypatch.setattr(pod_scheduling,
                            '_FAILED_SCHEDULING_EVENT_CACHE_TTL_SECONDS', 0)
        occurrence = datetime.datetime.now(datetime.timezone.utc)
        core_api = self._patch_events(monkeypatch,
                                      [self._event(occurrence=occurrence)])

        assert pod_scheduling._get_failed_scheduling_event_matches(
            'ns-1', 'ctx')
        assert pod_scheduling._get_failed_scheduling_event_matches(
            'ns-2', 'ctx')
        bucket_path, _, _ = (
            pod_scheduling._failed_scheduling_event_shared_cache_paths(
                'ctx', 'ns-2'))
        with open(bucket_path, encoding='utf-8') as bucket_file:
            bucket = json.load(bucket_file)
        assert bucket['entries'][0]['identity'] == (
            pod_scheduling._failed_scheduling_event_shared_cache_identity(
                'ctx', 'ns-2'))
        assert core_api.list_namespaced_event.call_count == 2

    @pytest.mark.parametrize('invalid_bucket', [
        '{not-json',
        json.dumps({
            'version':
                pod_scheduling._FAILED_SCHEDULING_EVENT_SHARED_CACHE_VERSION,
            'entries': [{
                'identity': '["ctx","ns"]',
                'refreshed_at': 1e30,
                'last_accessed_at': 1e30,
                'snapshot': {
                    'latest_occurrence': None,
                    'gpu_incompatibilities': [],
                },
            }],
        }),
        json.dumps({
            'version':
                pod_scheduling._FAILED_SCHEDULING_EVENT_SHARED_CACHE_VERSION,
            'entries': [{
                'identity': '["ctx","ns"]',
                'refreshed_at': float('nan'),
                'last_accessed_at': 0,
                'snapshot': {
                    'latest_occurrence': None,
                    'gpu_incompatibilities': [],
                },
            }],
        }),
    ])
    def test_malformed_or_future_shared_bucket_is_repaired(
            self, monkeypatch, invalid_bucket):
        occurrence = datetime.datetime.now(datetime.timezone.utc)
        core_api = self._patch_events(monkeypatch,
                                      [self._event(occurrence=occurrence)])
        bucket_path, _, staging_path = (
            pod_scheduling._failed_scheduling_event_shared_cache_paths(
                'ctx', 'ns'))
        os.makedirs(os.path.dirname(bucket_path), exist_ok=True)
        with open(bucket_path, 'w', encoding='utf-8') as bucket_file:
            bucket_file.write(invalid_bucket)

        assert pod_scheduling._get_failed_scheduling_event_matches('ns', 'ctx')
        with open(bucket_path, encoding='utf-8') as bucket_file:
            repaired = json.load(bucket_file)
        assert repaired['version'] == (
            pod_scheduling._FAILED_SCHEDULING_EVENT_SHARED_CACHE_VERSION)
        assert len(repaired['entries']) == 1
        assert not os.path.exists(staging_path)
        assert core_api.list_namespaced_event.call_count == 1

    def test_invalid_utf8_shared_bucket_is_repaired(self, monkeypatch):
        occurrence = datetime.datetime.now(datetime.timezone.utc)
        core_api = self._patch_events(monkeypatch,
                                      [self._event(occurrence=occurrence)])
        bucket_path, _, _ = (
            pod_scheduling._failed_scheduling_event_shared_cache_paths(
                'ctx', 'ns'))
        os.makedirs(os.path.dirname(bucket_path), exist_ok=True)
        with open(bucket_path, 'wb') as bucket_file:
            bucket_file.write(b'\xff\xfe')

        assert pod_scheduling._get_failed_scheduling_event_matches('ns', 'ctx')
        with open(bucket_path, encoding='utf-8') as bucket_file:
            repaired = json.load(bucket_file)
        assert repaired['version'] == (
            pod_scheduling._FAILED_SCHEDULING_EVENT_SHARED_CACHE_VERSION)
        assert core_api.list_namespaced_event.call_count == 1

    def test_shared_lock_contention_skips_refresh(self, monkeypatch):
        core_api = self._patch_events(monkeypatch, [
            self._event(occurrence=datetime.datetime.now(datetime.timezone.utc))
        ])
        _, lock_path, _ = (
            pod_scheduling._failed_scheduling_event_shared_cache_paths(
                'ctx', 'ns'))
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        with filelock.FileLock(lock_path):
            assert pod_scheduling._get_failed_scheduling_event_matches(
                'ns', 'ctx') == {}
        assert core_api.list_namespaced_event.call_count == 0

    def test_shared_filesystem_error_skips_refresh(self, monkeypatch):
        core_api = self._patch_events(monkeypatch, [
            self._event(occurrence=datetime.datetime.now(datetime.timezone.utc))
        ])
        bucket_path, _, _ = (
            pod_scheduling._failed_scheduling_event_shared_cache_paths(
                'ctx', 'ns'))
        real_open = open

        def fail_bucket_read(path, *args, **kwargs):
            if path == bucket_path:
                raise PermissionError('cache unavailable')
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr('builtins.open', fail_bucket_read)
        assert pod_scheduling._get_failed_scheduling_event_matches('ns',
                                                                   'ctx') == {}
        assert core_api.list_namespaced_event.call_count == 0

    def test_wait_loop_exits_without_autoscaler_config_for_exact_event(
            self, monkeypatch):
        cutoff = datetime.datetime(2026,
                                   7,
                                   20,
                                   17,
                                   tzinfo=datetime.timezone.utc)
        cluster_name_on_cloud = 'my-cluster'
        node = TestWaitForPodsToScheduleAutoscaleTimeout._make_node(
            'pod-0', cluster_name_on_cloud)
        pod = TestWaitForPodsToScheduleAutoscaleTimeout._make_pending_pod(
            'pod-0', cluster_name_on_cloud)
        pod.metadata.uid = 'pod-uid'

        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.return_value = types.SimpleNamespace(
            items=[pod])
        core_api.list_namespaced_event.return_value = types.SimpleNamespace(
            items=[self._event(occurrence=cutoff)])
        monkeypatch.setattr(kubernetes, 'core_api', lambda *a, **k: core_api)
        monkeypatch.setattr('sky.skypilot_config.get_effective_region_config',
                            lambda *a, **k: None)
        clock = TestWaitForPodsToScheduleAutoscaleTimeout._FakeClock()
        monkeypatch.setattr(pod_scheduling.time, 'time', clock.time)
        monkeypatch.setattr(pod_scheduling.time, 'sleep', clock.sleep)

        with pytest.raises(config_lib.KubernetesError) as exc_info:
            pod_scheduling._wait_for_pods_to_schedule(namespace='ns',
                                                      context='ctx',
                                                      new_nodes=[node],
                                                      timeout=60,
                                                      cluster_name='cn',
                                                      create_pods_start=cutoff)

        assert exc_info.value.insufficent_resources == ['GPUs']
        assert clock.now == 0


class TestWaitForPodsToScheduleBoundPod:
    """Tests that _wait_for_pods_to_schedule treats a pod as scheduled once
    the kube-scheduler has bound it to a node, even before the kubelet has
    populated status.container_statuses.

    Regression: the previous implementation decided a pod was scheduled by
    checking ``container_statuses is not None``. container_statuses is
    populated by the kubelet only after it picks the pod up and starts the
    sandbox, which can lag the scheduler binding when the control plane is
    slow to propagate the binding to the kubelet. At the provision_timeout
    deadline a fully bound pod (PodScheduled True / spec.node_name set) could
    still have container_statuses == None and be wrongly reported as out of
    resources. The function must instead hand off to _wait_for_pods_to_run.
    """

    @staticmethod
    def _make_node(name: str, cluster_name_on_cloud: str):
        from sky.provision import constants as prov_constants
        node = mock.MagicMock()
        node.metadata.name = name
        node.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud
        }
        return node

    @staticmethod
    def _make_bound_pending_pod(name: str,
                                cluster_name_on_cloud: str,
                                node_name=None,
                                pod_scheduled_condition: bool = False):
        """A Pending pod that the scheduler has bound but the kubelet has not
        yet picked up: container_statuses / host_ip / start_time are still
        None. Binding is expressed via spec.node_name and/or a PodScheduled
        condition.
        """
        from sky.provision import constants as prov_constants
        pod = mock.MagicMock()
        pod.metadata.name = name
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud
        }
        pod.status.phase = 'Pending'
        # The kubelet has not populated any of these yet.
        pod.status.container_statuses = None
        pod.status.host_ip = None
        pod.status.start_time = None
        # The scheduler has bound the pod.
        pod.spec.node_name = node_name
        if pod_scheduled_condition:
            cond = mock.MagicMock()
            cond.type = 'PodScheduled'
            cond.status = 'True'
            pod.status.conditions = [cond]
        else:
            pod.status.conditions = []
        return pod

    @staticmethod
    def _make_unbound_pending_pod(name: str, cluster_name_on_cloud: str):
        """A Pending pod the scheduler has NOT bound: no node_name and no
        PodScheduled=True condition. This pod is genuinely unschedulable and
        must keep the loop waiting until the timeout.
        """
        from sky.provision import constants as prov_constants
        pod = mock.MagicMock()
        pod.metadata.name = name
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud
        }
        pod.status.phase = 'Pending'
        pod.status.container_statuses = None
        pod.status.host_ip = None
        pod.status.start_time = None
        pod.spec.node_name = None
        pod.status.conditions = []
        return pod

    @staticmethod
    def _wire_common_mocks(monkeypatch, pod, autoscaler_type=None):
        """Mock config lookup, core_api, autoscale detection and the
        error-surfacing path. Returns the raise_errors marker mock."""

        def mock_config(cloud, region, keys, default_value=None, **kwargs):
            if keys == ('autoscaler',):
                return autoscaler_type
            return default_value

        monkeypatch.setattr('sky.skypilot_config.get_effective_region_config',
                            mock_config)

        pods_list = mock.MagicMock()
        pods_list.items = [pod]
        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.return_value = pods_list
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)

        monkeypatch.setattr(pod_scheduling, '_cluster_had_autoscale_event',
                            lambda *a, **kw: False)
        monkeypatch.setattr(pod_scheduling, '_cluster_maybe_autoscaling',
                            lambda *a, **kw: False)

        raise_errors = mock.MagicMock(
            side_effect=config_lib.KubernetesError('simulated-timeout'))
        monkeypatch.setattr(pod_scheduling, '_raise_pod_scheduling_errors',
                            raise_errors)
        monkeypatch.setattr('sky.utils.rich_utils.force_update_status',
                            lambda *a, **kw: None)
        return raise_errors, core_api

    @pytest.mark.parametrize('node_name, pod_scheduled_condition', [
        ('node-1', False),
        (None, True),
        ('node-1', True),
    ])
    def test_bound_pending_pod_without_container_statuses_returns(
            self, monkeypatch, node_name, pod_scheduled_condition):
        """The regression case: a bound but not-yet-picked-up pod (no
        container_statuses / host_ip) must be treated as scheduled, so the
        function returns promptly without raising — handing off to
        _wait_for_pods_to_run."""
        cluster_name_on_cloud = 'my-cluster'
        pod = self._make_bound_pending_pod(
            'pod-0',
            cluster_name_on_cloud,
            node_name=node_name,
            pod_scheduled_condition=pod_scheduled_condition)
        raise_errors, core_api = self._wire_common_mocks(monkeypatch, pod)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        # A positive timeout; the function must return well before it without
        # raising because the pod is bound.
        instance._wait_for_pods_to_schedule(
            namespace='ns',
            context='test-context',
            new_nodes=[node],
            timeout=30,
            cluster_name='cn',
            create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        assert not raise_errors.called, (
            'A bound pod (scheduler placed it) must not trigger the '
            'out-of-resources error path even though the kubelet has not '
            'populated container_statuses yet.')
        core_api.list_namespaced_pod.assert_called_once()

    def test_unbound_pending_pod_times_out_and_raises(self, monkeypatch):
        """A genuinely unschedulable pod (no node_name, no PodScheduled=True)
        must keep the loop waiting and eventually raise once the timeout
        elapses."""
        cluster_name_on_cloud = 'my-cluster'
        pod = self._make_unbound_pending_pod('pod-0', cluster_name_on_cloud)
        # No autoscaler configured, so the timeout is not bumped to the
        # autoscaler minimum and stays at the tiny value below.
        raise_errors, _ = self._wire_common_mocks(monkeypatch,
                                                  pod,
                                                  autoscaler_type=None)

        node = self._make_node('pod-0', cluster_name_on_cloud)

        with pytest.raises(config_lib.KubernetesError,
                           match='simulated-timeout'):
            instance._wait_for_pods_to_schedule(
                namespace='ns',
                context='test-context',
                new_nodes=[node],
                timeout=1,
                cluster_name='cn',
                create_pods_start=datetime.datetime.now(datetime.timezone.utc))

        assert raise_errors.called, (
            'An unbound (unschedulable) pod must drive the timeout/error '
            'path.')


# ---------------------------------------------------------------------------
# Helpers and tests for _condensed_pod_reason()
# ---------------------------------------------------------------------------


def _make_mock_pod(phase='Failed',
                   deletion_timestamp=None,
                   conditions=None,
                   container_statuses=None,
                   init_container_statuses=None):
    """Helper to build a mock pod with the given status fields."""
    pod = mock.MagicMock()
    pod.metadata.name = 'test-pod'
    pod.metadata.deletion_timestamp = deletion_timestamp
    pod.status.phase = phase
    pod.status.start_time = None
    pod.status.conditions = conditions or []
    pod.status.container_statuses = container_statuses or []
    pod.status.init_container_statuses = init_container_statuses or []
    # A real pod has no pod-level kubelet reason unless evicted/preempted;
    # leave it unset so condition/container-derived reasons are exercised.
    pod.status.reason = None
    pod.status.message = None
    return pod


def _make_condition(type_,
                    reason,
                    message='',
                    status='True',
                    last_transition_time=None):
    cond = mock.MagicMock()
    cond.type = type_
    cond.reason = reason
    cond.message = message
    cond.status = status
    cond.last_transition_time = last_transition_time
    return cond


def _make_container_status(name='main',
                           terminated_reason=None,
                           terminated_exit_code=None,
                           terminated_finished_at=None,
                           waiting_reason=None,
                           waiting_message=None):
    cs = mock.MagicMock()
    cs.name = name
    cs.state.terminated = None
    cs.state.waiting = None
    if terminated_reason is not None:
        cs.state.terminated = mock.MagicMock()
        cs.state.terminated.reason = terminated_reason
        cs.state.terminated.exit_code = terminated_exit_code or 1
        cs.state.terminated.finished_at = terminated_finished_at
        cs.state.terminated.message = None
    if waiting_reason is not None:
        cs.state.waiting = mock.MagicMock()
        cs.state.waiting.reason = waiting_reason
        cs.state.waiting.message = waiting_message
    return cs


class TestCondensedPodReason:
    """Tests for _condensed_pod_reason()."""

    def test_oom_killed(self):
        container = _make_container_status(terminated_reason='OOMKilled',
                                           terminated_exit_code=137)
        pod = _make_mock_pod(conditions=[_make_condition('Ready', 'False')],
                             container_statuses=[container])
        result = instance._condensed_pod_reason(pod)
        assert 'OOMKilled' in result
        assert '137' in result

    def test_kueue_preemption(self):
        conditions = [
            _make_condition('Ready', 'False'),
            _make_condition('TerminationTarget', 'PreemptedByWorkloadPriority',
                            'Higher priority workload scheduled'),
        ]
        pod = _make_mock_pod(conditions=conditions)
        result = instance._condensed_pod_reason(pod)
        assert 'Preempted by Kueue' in result
        assert 'PreemptedByWorkloadPriority' in result

    def test_disruption(self):
        conditions = [
            _make_condition('Ready', 'False'),
            _make_condition('DisruptionTarget', 'EvictionByKueue',
                            'Preempted to accommodate a higher priority'),
        ]
        pod = _make_mock_pod(conditions=conditions)
        result = instance._condensed_pod_reason(pod)
        assert 'Disrupted' in result

    def test_image_pull_backoff_from_waiting(self):
        container = _make_container_status(
            waiting_reason='ImagePullBackOff',
            waiting_message='Back-off pulling image "nvcr.io/foo:bad"')
        pod = _make_mock_pod(phase='Failed',
                             conditions=[_make_condition('Ready', 'False')],
                             container_statuses=[container])
        result = instance._condensed_pod_reason(pod)
        assert 'ImagePullBackOff' in result
        assert 'nvcr.io/foo:bad' in result

    def test_crash_loop_from_terminated(self):
        container = _make_container_status(terminated_reason='Error',
                                           terminated_exit_code=1)
        pod = _make_mock_pod(conditions=[_make_condition('Ready', 'False')],
                             container_statuses=[container])
        result = instance._condensed_pod_reason(pod)
        assert 'Error' in result
        assert 'exit code 1' in result

    def test_terminated_no_reason(self):
        """When terminated.reason is None, should show exit code cleanly."""
        container = mock.MagicMock()
        container.state.waiting = None
        container.state.terminated = mock.MagicMock()
        container.state.terminated.reason = None
        container.state.terminated.exit_code = 137
        pod = _make_mock_pod(conditions=[_make_condition('Ready', 'False')],
                             container_statuses=[container])
        result = instance._condensed_pod_reason(pod)
        assert result == 'Terminated with exit code 137'

    def test_unknown_fallback(self):
        pod = _make_mock_pod(conditions=[_make_condition('Ready', 'False')],
                             container_statuses=[])
        result = instance._condensed_pod_reason(pod)
        assert 'Terminated unexpectedly' in result


class TestInsufficientResourcesMsg:
    """Tests for _insufficient_resources_msg with last_error_reason."""

    def _make_provisioner(self):
        provisioner = cloud_vm_ray_backend.RetryingVmProvisioner.__new__(
            cloud_vm_ray_backend.RetryingVmProvisioner)
        return provisioner

    def test_includes_error_reason_for_k8s(self):
        provisioner = self._make_provisioner()
        k8s_resource = mock.MagicMock()
        k8s_resource.zone = None
        k8s_resource.region = 'my-context'
        k8s_resource.cloud = clouds.Kubernetes()
        requested = {k8s_resource}

        msg = provisioner._insufficient_resources_msg(
            k8s_resource,
            requested,
            None,
            last_error_reason='OOMKilled (exit code 137)')
        assert 'OOMKilled (exit code 137)' in msg
        assert 'my-context' in msg

    def test_no_error_reason_falls_back(self):
        provisioner = self._make_provisioner()
        k8s_resource = mock.MagicMock()
        k8s_resource.zone = None
        k8s_resource.region = 'my-context'
        k8s_resource.cloud = clouds.Kubernetes()
        requested = {k8s_resource}

        msg = provisioner._insufficient_resources_msg(k8s_resource,
                                                      requested,
                                                      None,
                                                      last_error_reason=None)
        assert 'Failed to acquire resources' in msg
        assert 'my-context' in msg
        assert 'OOMKilled' not in msg


@pytest.fixture()
def mock_format_resource(monkeypatch):
    """Mock format_resource to avoid needing real Resources objects."""
    monkeypatch.setattr(
        'sky.backends.cloud_vm_ray_backend.resources_utils.format_resource',
        lambda resource, simplified_only=False:
        ('H100:1, cpus=4, mem=16', None))


class TestProvisionFailureBlocks:
    """Tests for _format_provision_failure_blocks."""

    def _make_k8s_resource(self,
                           infra_str='Kubernetes (in-cluster)',
                           region='in-cluster'):
        resource = mock.MagicMock()
        resource.infra.formatted_str.return_value = infra_str
        resource.cloud = clouds.Kubernetes()
        resource.region = region
        return resource

    def _make_aws_resource(self, infra_str='AWS (us-east-1)'):
        resource = mock.MagicMock()
        resource.infra.formatted_str.return_value = infra_str
        resource.cloud = clouds.AWS()
        return resource

    def test_single_failure_block(self, mock_format_resource):
        resource = self._make_k8s_resource()
        exc = sky_exceptions.ResourcesUnavailableError(
            'Failed to acquire resources in context in-cluster. '
            'Reason: OOMKilled (exit code 137)')
        result = cloud_vm_ray_backend._format_provision_failure_blocks(
            {resource: exc})
        assert '\u2717 Kubernetes (in-cluster)' in result
        assert 'OOMKilled' in result

    def test_hint_for_image_pull(self, mock_format_resource):
        resource = self._make_k8s_resource()
        exc = sky_exceptions.ResourcesUnavailableError(
            'Reason: ImagePullBackOff: nvcr.io/foo:bad - manifest unknown')
        result = cloud_vm_ray_backend._format_provision_failure_blocks(
            {resource: exc})
        assert 'Hint:' in result
        assert 'image' in result.lower() or 'registry' in result.lower()

    def test_hint_for_oom(self, mock_format_resource):
        resource = self._make_k8s_resource()
        exc = sky_exceptions.ResourcesUnavailableError(
            'Reason: OOMKilled (exit code 137)')
        result = cloud_vm_ray_backend._format_provision_failure_blocks(
            {resource: exc})
        assert 'Hint:' in result
        assert 'memory' in result.lower()

    def test_hint_for_insufficient_includes_url_and_kubectl(
            self, mock_format_resource, monkeypatch):
        """Insufficient hint links the dashboard infra page scoped to the
        failing context and also mentions `kubectl describe nodes`."""
        monkeypatch.setattr(
            'sky.backends.cloud_vm_ray_backend.server_common.get_server_url',
            lambda: 'http://api.example.com')
        resource = self._make_k8s_resource(region='my-cluster-context')
        exc = sky_exceptions.ResourcesUnavailableError(
            'Reason: Insufficient nvidia.com/gpu')
        result = cloud_vm_ray_backend._format_provision_failure_blocks(
            {resource: exc})
        assert 'Hint:' in result
        assert 'kubectl describe nodes' in result
        assert ('http://api.example.com/dashboard/infra/my-cluster-context'
                in result)

    def test_hint_falls_back_when_url_resolution_fails(self,
                                                       mock_format_resource,
                                                       monkeypatch):
        """If get_server_url raises, the hint should still render (with a
        generic fallback) rather than crash the failure-rendering path."""

        def _boom():
            raise RuntimeError('no api server endpoint configured')

        monkeypatch.setattr(
            'sky.backends.cloud_vm_ray_backend.server_common.get_server_url',
            _boom)
        resource = self._make_k8s_resource(region='my-cluster-context')
        exc = sky_exceptions.ResourcesUnavailableError(
            'Reason: Insufficient nvidia.com/gpu')
        result = cloud_vm_ray_backend._format_provision_failure_blocks(
            {resource: exc})
        assert 'Hint:' in result
        assert 'kubectl describe nodes' in result
        # Generic fallback text used when URL resolution fails.
        assert 'SkyPilot dashboard infra page' in result
        # Placeholder must not leak through.
        assert '{dashboard_url}' not in result

    def test_no_hint_for_unknown_k8s_failure(self, mock_format_resource):
        """K8s block with no recognized failure substring gets no hint."""
        resource = self._make_k8s_resource()
        exc = sky_exceptions.ResourcesUnavailableError(
            'Some unrecognized cluster failure.')
        result = cloud_vm_ray_backend._format_provision_failure_blocks(
            {resource: exc})
        assert 'Hint:' not in result

    def test_no_hint_for_non_k8s_cloud(self, mock_format_resource):
        """Hints don't fire for non-k8s clouds even if substring matches.

        Prevents AWS messages like 'InsufficientInstanceCapacity' from
        triggering the k8s insufficient-resources hint.
        """
        resource = self._make_aws_resource()
        exc = sky_exceptions.ResourcesUnavailableError(
            'InsufficientInstanceCapacity: no A100 capacity in us-east-1.')
        result = cloud_vm_ray_backend._format_provision_failure_blocks(
            {resource: exc})
        assert 'Hint:' not in result
        assert 'dashboard' not in result

    def test_multiple_failures_mixed_clouds(self, mock_format_resource):
        r1 = self._make_k8s_resource()
        r2 = self._make_aws_resource()
        exc1 = sky_exceptions.ResourcesUnavailableError(
            'Reason: OOMKilled (exit code 137)')
        exc2 = sky_exceptions.ResourcesUnavailableError(
            'No capacity in us-east-1.')
        result = cloud_vm_ray_backend._format_provision_failure_blocks({
            r1: exc1,
            r2: exc2
        })
        assert '\u2717 Kubernetes (in-cluster)' in result
        assert '\u2717 AWS (us-east-1)' in result
        # K8s block has the OOM hint
        assert 'memory' in result.lower()


class TestFullPipeline:
    """Integration test: KubernetesError message → retry loop → block output."""

    def test_oom_reason_reaches_block_output(self, mock_format_resource):
        """Verify OOMKilled flows from exception through to block rendering."""
        k8s_error = config_lib.KubernetesError(
            'Pod test-pod failed: OOMKilled (exit code 137). '
            'Run `sky logs --provision test-cluster` for more details.')
        last_error_reason = str(k8s_error)

        backend = cloud_vm_ray_backend.RetryingVmProvisioner.__new__(
            cloud_vm_ray_backend.RetryingVmProvisioner)
        k8s_resource = mock.MagicMock()
        k8s_resource.zone = None
        k8s_resource.region = 'my-context'
        k8s_resource.cloud = clouds.Kubernetes()
        msg = backend._insufficient_resources_msg(
            k8s_resource, {k8s_resource},
            None,
            last_error_reason=last_error_reason)

        exc = sky_exceptions.ResourcesUnavailableError(msg)
        blocks = cloud_vm_ray_backend._format_provision_failure_blocks(
            {k8s_resource: exc})
        assert 'OOMKilled' in blocks
        assert 'exit code 137' in blocks
        assert 'Hint:' in blocks
        assert 'memory' in blocks.lower()

    def test_image_pull_reason_reaches_block_output(self, mock_format_resource):
        """Verify ImagePullBackOff flows end-to-end."""
        k8s_error = config_lib.KubernetesError(
            'Pod test-pod failed: ImagePullBackOff: '
            'Back-off pulling image "nvcr.io/nvidia/pytorch:bad-tag". '
            'Run `sky logs --provision test-cluster` for more details.')
        last_error_reason = str(k8s_error)

        backend = cloud_vm_ray_backend.RetryingVmProvisioner.__new__(
            cloud_vm_ray_backend.RetryingVmProvisioner)
        k8s_resource = mock.MagicMock()
        k8s_resource.zone = None
        k8s_resource.region = 'my-context'
        k8s_resource.cloud = clouds.Kubernetes()
        msg = backend._insufficient_resources_msg(
            k8s_resource, {k8s_resource},
            None,
            last_error_reason=last_error_reason)

        exc = sky_exceptions.ResourcesUnavailableError(msg)
        blocks = cloud_vm_ray_backend._format_provision_failure_blocks(
            {k8s_resource: exc})
        assert 'ImagePullBackOff' in blocks
        assert 'nvcr.io/nvidia/pytorch:bad-tag' in blocks
        assert 'Hint:' in blocks
        assert 'image' in blocks.lower() or 'registry' in blocks.lower()


class TestWaitForPodsToRunLaunchProgressEmit:
    """Tests for the LAUNCH_PROGRESS emit added to _wait_for_pods_to_run."""

    @staticmethod
    def _make_pod(name: str, cluster_name_on_cloud: str):
        from sky.provision import constants as prov_constants
        pod = mock.MagicMock()
        pod.metadata.name = name
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud,
        }
        # status_text branch in the production code only checks
        # phase / container_statuses via _inspect_pod_status, which we
        # mock below — so attribute values here can be loose.
        pod.status.phase = 'Pending'
        pod.status.container_statuses = None
        return pod

    def _setup(self, monkeypatch, inspect_results_per_iter):
        """Drive the loop with a scripted sequence of _inspect_pod_status
        return values. Each entry of inspect_results_per_iter is the list
        the parallel-map returns for that iteration (one tuple per pod)."""
        cluster_name_on_cloud = 'my-cluster'
        pod = self._make_pod('pod-0', cluster_name_on_cloud)
        pods_list = mock.MagicMock()
        pods_list.items = [pod]
        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.return_value = pods_list
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)

        results_iter = iter(inspect_results_per_iter)
        monkeypatch.setattr(
            'sky.utils.subprocess_utils.run_in_parallel',
            lambda fn, items, n: next(results_iter),
        )

        monkeypatch.setattr('sky.utils.rich_utils.force_update_status',
                            lambda *a, **kw: None)
        monkeypatch.setattr(instance.time, 'sleep', lambda *a, **kw: None)

        add_event = mock.MagicMock()
        monkeypatch.setattr(instance.global_user_state, 'add_cluster_event',
                            add_event)
        return pod, add_event

    def test_emit_on_stage_change_dedup_on_no_change(self, monkeypatch):
        """First iteration: pulling → emit. Second iteration: still pulling
        → no emit (same status_text). Third iteration: all running → loop
        exits."""
        pod, add_event = self._setup(monkeypatch, [
            [(False, 'Pulling')],
            [(False, 'Pulling')],
            [(True, None)],
        ])

        instance._wait_for_pods_to_run(
            namespace='ns',
            context='ctx',
            cluster_name='cn',
            new_pods=[pod],
        )

        lp_calls = [
            c for c in add_event.call_args_list if c.kwargs.get('event_type') is
            instance.global_user_state.ClusterEventType.LAUNCH_PROGRESS
        ]
        assert len(lp_calls) == 1
        assert lp_calls[0].kwargs['reason'] == (
            'Launching (1 pod(s) pending due to Pulling)')
        assert lp_calls[0].kwargs['nop_if_duplicate'] is True

    def test_no_emit_when_pending_reasons_empty(self, monkeypatch):
        """When _inspect_pod_status returns no pending reason but is_running
        is False, status_text is the bare 'Launching' — useless tooltip.
        No emit must happen for that iteration."""
        pod, add_event = self._setup(monkeypatch, [
            [(False, None)],
            [(True, None)],
        ])

        instance._wait_for_pods_to_run(
            namespace='ns',
            context='ctx',
            cluster_name='cn',
            new_pods=[pod],
        )

        lp_calls = [
            c for c in add_event.call_args_list if c.kwargs.get('event_type') is
            instance.global_user_state.ClusterEventType.LAUNCH_PROGRESS
        ]
        assert lp_calls == []


class TestUnmaskCrashloopbackoffReason:
    """Tests for _unmask_crashloopbackoff_reason: surfaces last_state.terminated.reason
    when a container is in CrashLoopBackOff, else returns None."""

    @staticmethod
    def _cs(*, waiting=None, last_terminated=None):
        """Build a V1ContainerStatus-shaped mock."""
        cs = mock.MagicMock()
        cs.state = mock.MagicMock()
        cs.state.waiting = waiting
        cs.last_state = mock.MagicMock()
        cs.last_state.terminated = last_terminated
        return cs

    def test_returns_none_when_state_waiting_is_none(self):
        cs = self._cs(waiting=None)
        assert instance._unmask_crashloopbackoff_reason(cs) is None

    def test_returns_none_when_waiting_reason_is_not_crashloop(self):
        cs = self._cs(waiting=mock.MagicMock(reason='ImagePullBackOff'))
        assert instance._unmask_crashloopbackoff_reason(cs) is None

    def test_returns_none_when_last_state_terminated_is_none(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='CrashLoopBackOff'),
            last_terminated=None,
        )
        assert instance._unmask_crashloopbackoff_reason(cs) is None

    def test_returns_none_when_last_terminated_reason_is_empty(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='CrashLoopBackOff'),
            last_terminated=mock.MagicMock(reason='', exit_code=137),
        )
        assert instance._unmask_crashloopbackoff_reason(cs) is None

    def test_returns_last_terminated_reason_when_crashloop_and_present(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='CrashLoopBackOff'),
            last_terminated=mock.MagicMock(reason='OOMKilled', exit_code=137),
        )
        assert instance._unmask_crashloopbackoff_reason(cs) == 'OOMKilled'

    def test_returns_error_for_non_oom_crashloop(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='CrashLoopBackOff'),
            last_terminated=mock.MagicMock(reason='Error', exit_code=1),
        )
        assert instance._unmask_crashloopbackoff_reason(cs) == 'Error'


class TestGetPodPendingReasonFromContainerStatus:
    """Tier-1 sweep over pod.status.container_statuses. Per-container first-match
    wins; iterates state.waiting (skipping ContainerCreating/PodInitializing),
    then state.terminated, then last_state.terminated."""

    @staticmethod
    def _cs(*,
            waiting=None,
            terminated=None,
            last_terminated=None,
            running=False,
            ready=False):
        """Build a V1ContainerStatus-shaped mock."""
        cs = mock.MagicMock()
        cs.ready = ready
        cs.state = mock.MagicMock()
        cs.state.waiting = waiting
        cs.state.terminated = terminated
        cs.state.running = mock.MagicMock() if running else None
        cs.last_state = mock.MagicMock()
        cs.last_state.terminated = last_terminated
        return cs

    @staticmethod
    def _pod(container_statuses):
        pod = mock.MagicMock()
        pod.status = mock.MagicMock()
        pod.status.container_statuses = container_statuses
        return pod

    def test_healthy_returns_none(self):
        cs = self._cs(running=True, ready=True)
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) is None

    def test_waiting_image_pull_back_off(self):
        cs = self._cs(waiting=mock.MagicMock(reason='ImagePullBackOff',
                                             message='Back-off pulling image'))
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) == 'ImagePullBackOff'

    def test_waiting_container_creating_returns_none(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='ContainerCreating', message=''))
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) is None

    def test_waiting_pod_initializing_returns_none(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='PodInitializing', message=''))
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) is None

    def test_crashloopbackoff_unmasks_oomkilled(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='CrashLoopBackOff',
                                   message='back-off 5m0s'),
            last_terminated=mock.MagicMock(reason='OOMKilled', exit_code=137),
        )
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) == 'OOMKilled'

    def test_crashloopbackoff_without_last_state_falls_back(self):
        cs = self._cs(
            waiting=mock.MagicMock(reason='CrashLoopBackOff',
                                   message='back-off 5m0s'),
            last_terminated=None,
        )
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) == 'CrashLoopBackOff'

    def test_terminated_non_zero_exit(self):
        cs = self._cs(terminated=mock.MagicMock(reason='Error', exit_code=1))
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) == 'Error'

    def test_terminated_non_zero_exit_no_reason(self):
        cs = self._cs(terminated=mock.MagicMock(reason=None, exit_code=139))
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) == 'Terminated'

    def test_last_state_terminated_with_running_current(self):
        # Race-window case: container restarted, current state Running, but
        # last_state.terminated carries the OOM signal.
        cs = self._cs(
            running=True,
            last_terminated=mock.MagicMock(reason='OOMKilled', exit_code=137),
        )
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) == 'OOMKilled'

    def test_last_state_terminated_completed_clean_exit_returns_none(self):
        # Negative test: a cleanly-completed previous exit must NOT be
        # surfaced as a pending reason.
        cs = self._cs(
            running=True,
            last_terminated=mock.MagicMock(reason='Completed', exit_code=0),
        )
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([cs])) is None

    def test_multi_container_returns_earliest_status_array_match(self):
        # Walks pod.status.container_statuses in native array order
        # (= pod-manifest spec order per k8s API).
        healthy = self._cs(running=True, ready=True)
        bad = self._cs(
            waiting=mock.MagicMock(reason='ImagePullBackOff', message=''))
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([healthy, bad])) == 'ImagePullBackOff'

    def test_transient_waiting_with_prior_oom_beats_later_container(self):
        # Container A is in transient ContainerCreating but has a prior
        # OOMKilled in last_state -- checks 2/3 still run on A and surface the
        # OOM, before B's ImagePullBackOff is ever consulted. Pins the choice
        # documented in _get_pod_pending_reason_from_container_status's
        # docstring.
        a = self._cs(
            waiting=mock.MagicMock(reason='ContainerCreating', message=''),
            last_terminated=mock.MagicMock(reason='OOMKilled', exit_code=137),
        )
        b = self._cs(
            waiting=mock.MagicMock(reason='ImagePullBackOff', message=''))
        assert instance._get_pod_pending_reason_from_container_status(
            self._pod([a, b])) == 'OOMKilled'

    def test_no_container_statuses_returns_none(self):
        pod = self._pod(None)
        assert instance._get_pod_pending_reason_from_container_status(
            pod) is None

    def test_empty_container_statuses_returns_none(self):
        pod = self._pod([])
        assert instance._get_pod_pending_reason_from_container_status(
            pod) is None


class TestGetPodPendingReasonTieredEventFilter:
    """Two-pass scan: Warning events first (regardless of timestamp),
    then a small allow-list of slow Normal events."""

    @staticmethod
    def _event(reason: str, event_type: str = 'Normal', message: str = ''):
        ev = mock.MagicMock()
        ev.reason = reason
        ev.type = event_type
        ev.message = message
        return ev

    def _patch_events(self, monkeypatch, events):
        monkeypatch.setattr(instance, '_get_pod_events',
                            lambda *a, **kw: events)

    def test_no_events_returns_none(self, monkeypatch):
        self._patch_events(monkeypatch, [])
        assert instance._get_pod_pending_reason('ctx', 'ns', 'pod-0') is None

    def test_warning_wins_over_normal_regardless_of_age(self, monkeypatch):
        # Newest event (index 0) is a Normal Pulling, older event is a
        # Warning FailedScheduling. Warning must win.
        events = [
            self._event('Pulling', 'Normal', 'Pulling image "foo:bar"'),
            self._event('FailedScheduling', 'Warning',
                        '0/3 nodes are available: insufficient cpu.'),
        ]
        self._patch_events(monkeypatch, events)
        assert instance._get_pod_pending_reason('ctx', 'ns', 'p') == (
            'FailedScheduling',
            '0/3 nodes are available: insufficient cpu.',
        )

    def test_allow_listed_normal_returned_when_no_warning(self, monkeypatch):
        events = [self._event('Pulling', 'Normal', 'Pulling image "foo:bar"')]
        self._patch_events(monkeypatch, events)
        assert instance._get_pod_pending_reason(
            'ctx', 'ns', 'p') == ('Pulling', 'Pulling image "foo:bar"')

    def test_non_allow_listed_normal_returns_none(self, monkeypatch):
        # SuccessfulAttachVolume is the canonical false-positive we're killing.
        events = [
            self._event('SuccessfulAttachVolume', 'Normal',
                        'AttachVolume.Attach succeeded for volume "x"'),
            self._event('Pulled', 'Normal', 'Successfully pulled image'),
            self._event('Created', 'Normal', 'Created container ray-head'),
        ]
        self._patch_events(monkeypatch, events)
        assert instance._get_pod_pending_reason('ctx', 'ns', 'p') is None

    def test_provisioning_normal_returned(self, monkeypatch):
        events = [
            self._event('Provisioning', 'Normal',
                        'External provisioner is provisioning volume')
        ]
        self._patch_events(monkeypatch, events)
        assert instance._get_pod_pending_reason('ctx', 'ns', 'p') == (
            'Provisioning',
            'External provisioner is provisioning volume',
        )

    def test_wait_for_first_consumer_normal_returned(self, monkeypatch):
        events = [
            self._event('WaitForFirstConsumer', 'Normal',
                        'waiting for first consumer to be created'),
        ]
        self._patch_events(monkeypatch, events)
        assert instance._get_pod_pending_reason('ctx', 'ns', 'p') == (
            'WaitForFirstConsumer',
            'waiting for first consumer to be created',
        )

    def test_warning_returns_first_in_newest_first_order(self, monkeypatch):
        # When multiple Warnings, return newest (index 0 in our list).
        events = [
            self._event('FailedScheduling', 'Warning', 'newer'),
            self._event('FailedMount', 'Warning', 'older'),
        ]
        self._patch_events(monkeypatch, events)
        assert instance._get_pod_pending_reason('ctx', 'ns',
                                                'p') == ('FailedScheduling',
                                                         'newer')

    def test_empty_message_returns_empty_string_not_none(self, monkeypatch):
        # Preserve the today-behavior of event.message or '' for the second
        # tuple element.
        events = [self._event('FailedScheduling', 'Warning', '')]
        self._patch_events(monkeypatch, events)
        result = instance._get_pod_pending_reason('ctx', 'ns', 'p')
        assert result == ('FailedScheduling', '')

    def test_events_fetch_failure_returns_none(self, monkeypatch):

        def raise_for_events(*a, **kw):
            raise Exception('kube API down')

        monkeypatch.setattr(instance, '_get_pod_events', raise_for_events)
        assert instance._get_pod_pending_reason('ctx', 'ns', 'p') is None


class TestInspectPodStatusTierIntegration:
    """End-to-end behavior of _inspect_pod_status with the new tier-1 helper.

    These tests exercise the closure indirectly by driving _wait_for_pods_to_run
    with scripted pod objects. They lock in:
    - Running-but-not-all-running pods now surface a pending reason instead of
      returning (False, None).
    - The Pending-raise path enriches the message via _unmask_crashloopbackoff_reason.
    """

    @staticmethod
    def _make_pod(*,
                  name='pod-0',
                  phase,
                  container_statuses,
                  cluster_name_on_cloud='cn-on-cloud'):
        from sky.provision import constants as prov_constants
        pod = mock.MagicMock()
        pod.metadata.name = name
        pod.metadata.deletion_timestamp = None
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud,
        }
        pod.status.phase = phase
        pod.status.container_statuses = container_statuses
        return pod

    @staticmethod
    def _cs(*,
            waiting=None,
            terminated=None,
            last_terminated=None,
            running=False,
            ready=False):
        cs = mock.MagicMock()
        cs.ready = ready
        cs.state.waiting = waiting
        cs.state.terminated = terminated
        cs.state.running = mock.MagicMock() if running else None
        cs.last_state.terminated = last_terminated
        return cs

    def _drive_one_iteration(self, monkeypatch, pod, then_running=True):
        """Drive _wait_for_pods_to_run one iteration. Patches the API
        list-pods call and the parallel-map. Returns the captured
        add_cluster_event mock so tests can assert on emits.

        If `then_running` is True, the second iteration returns an all-Running
        pod so the loop exits cleanly.
        """
        # Second iteration: all containers running, all pods running → exit.
        healthy_pod = self._make_pod(
            phase='Running',
            container_statuses=[self._cs(running=True, ready=True)],
            name=pod.metadata.name,
        )
        core_api = mock.MagicMock()
        call_count = {'n': 0}

        def _list_pods(*a, **kw):
            call_count['n'] += 1
            return mock.MagicMock(
                items=[pod if call_count['n'] == 1 else healthy_pod])

        core_api.list_namespaced_pod.side_effect = _list_pods
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)

        # _inspect_pod_status is the function passed to run_in_parallel.
        # We let the real function run by invoking fn(pod) inside our patch.
        def _run_in_parallel(fn, items, n):
            return [fn(p) for p in items]

        monkeypatch.setattr('sky.utils.subprocess_utils.run_in_parallel',
                            _run_in_parallel)

        monkeypatch.setattr('sky.utils.rich_utils.force_update_status',
                            lambda *a, **kw: None)
        monkeypatch.setattr(instance.time, 'sleep', lambda *a, **kw: None)

        add_event = mock.MagicMock()
        monkeypatch.setattr(instance.global_user_state, 'add_cluster_event',
                            add_event)

        instance._wait_for_pods_to_run(
            namespace='ns',
            context='ctx',
            cluster_name='cn',
            new_pods=[pod],
        )
        return add_event

    def test_running_pod_with_crashloopbackoff_emits_oomkilled(
            self, monkeypatch):
        """phase=Running, container in CrashLoopBackOff + last_state OOMKilled.
        Today this returns (False, None) and no LAUNCH_PROGRESS row is emitted.
        After: emits a LAUNCH_PROGRESS row whose reason contains 'OOMKilled'."""
        pod = self._make_pod(
            phase='Running',
            container_statuses=[
                self._cs(
                    waiting=mock.MagicMock(reason='CrashLoopBackOff',
                                           message='back-off 5m0s'),
                    last_terminated=mock.MagicMock(reason='OOMKilled',
                                                   exit_code=137),
                )
            ],
        )
        add_event = self._drive_one_iteration(monkeypatch, pod)
        lp_calls = [
            c for c in add_event.call_args_list if c.kwargs.get('event_type') is
            instance.global_user_state.ClusterEventType.LAUNCH_PROGRESS
        ]
        assert len(lp_calls) == 1
        assert 'OOMKilled' in lp_calls[0].kwargs['reason']
        assert 'CrashLoopBackOff' not in lp_calls[0].kwargs['reason']

    def test_pending_pod_with_crashloopbackoff_raises_enriched(
            self, monkeypatch):
        """phase=Pending, container in CrashLoopBackOff. Today raises
        'CrashLoopBackOff: <msg>'. After: raises 'OOMKilled: <msg>'
        (msg preserved). The bare 'CrashLoopBackOff' substring must NOT
        appear because kubelet's waiting.message text is lowercased
        'back-off Xs restarting failed container=...'."""
        pod = self._make_pod(
            phase='Pending',
            container_statuses=[
                self._cs(
                    waiting=mock.MagicMock(
                        reason='CrashLoopBackOff',
                        message='back-off 5m0s restarting failed '
                        'container=ray pod=foo',
                    ),
                    last_terminated=mock.MagicMock(reason='OOMKilled',
                                                   exit_code=137),
                )
            ],
        )
        with pytest.raises(config_lib.KubernetesError) as excinfo:
            self._drive_one_iteration(monkeypatch, pod, then_running=False)
        err = str(excinfo.value)
        assert 'OOMKilled' in err
        assert 'back-off 5m0s' in err
        assert 'CrashLoopBackOff' not in err

    def test_pending_pod_with_image_pull_back_off_raises_preserves_message(
            self, monkeypatch):
        """phase=Pending, ImagePullBackOff. Today raises 'ImagePullBackOff: <msg>'
        and the message body (e.g. registry URL) is the critical debug info.
        Verify it's preserved unchanged after the refactor."""
        pod = self._make_pod(
            phase='Pending',
            container_statuses=[
                self._cs(waiting=mock.MagicMock(
                    reason='ImagePullBackOff',
                    message='Back-off pulling image "registry.example/foo:bar": '
                    'connection refused',
                ))
            ],
        )
        with pytest.raises(config_lib.KubernetesError) as excinfo:
            self._drive_one_iteration(monkeypatch, pod, then_running=False)
        err = str(excinfo.value)
        assert err.startswith('ImagePullBackOff:')
        assert 'connection refused' in err
        assert 'registry.example/foo:bar' in err

    def test_pending_pod_container_creating_does_not_raise(self, monkeypatch):
        """phase=Pending, ContainerCreating: tier-1 returns None, tier-2/3
        consulted, no raise. Smoke check that the happy in-flight case still
        loops. With no container-status reason and no event, the pending reason
        defaults to 'container creation' so the spinner shows useful detail."""
        pod = self._make_pod(
            phase='Pending',
            container_statuses=[
                self._cs(waiting=mock.MagicMock(reason='ContainerCreating',
                                                message=''))
            ],
        )
        # No events → tier-2 and tier-3 also return None.
        monkeypatch.setattr(instance, '_get_pod_events', lambda *a, **kw: [])
        add_event = self._drive_one_iteration(monkeypatch, pod)
        lp_calls = [
            c for c in add_event.call_args_list if c.kwargs.get('event_type') is
            instance.global_user_state.ClusterEventType.LAUNCH_PROGRESS
        ]
        # The reason defaults to 'container creation', so a LAUNCH_PROGRESS row
        # is emitted with the enriched 'Launching (...)' status text.
        assert len(lp_calls) == 1
        assert ('Launching (1 pod(s) pending due to container creation)'
                in lp_calls[0].kwargs['reason'])

    def test_pending_pod_bound_no_container_statuses_defaults_reason(
            self, monkeypatch):
        """A freshly-bound pod the kubelet has not picked up yet sits Pending
        with container_statuses == None, host_ip == None, and no events. The
        pending reason must default to 'container creation' so the spinner shows
        'Launching (1 pod(s) pending due to container creation)' instead of a
        bare 'Launching' during the kubelet-pickup window. We assert on the
        LAUNCH_PROGRESS reason, whose value is the spinner status text."""
        pod = self._make_pod(
            phase='Pending',
            container_statuses=None,
        )
        pod.status.host_ip = None
        # No events → tier-2 and tier-3 return None, so the default kicks in.
        monkeypatch.setattr(instance, '_get_pod_events', lambda *a, **kw: [])
        add_event = self._drive_one_iteration(monkeypatch, pod)
        lp_calls = [
            c for c in add_event.call_args_list if c.kwargs.get('event_type') is
            instance.global_user_state.ClusterEventType.LAUNCH_PROGRESS
        ]
        assert len(lp_calls) == 1
        assert ('Launching (1 pod(s) pending due to container creation)'
                in lp_calls[0].kwargs['reason'])

    def test_pending_pod_not_scheduled_does_not_default_reason(
            self, monkeypatch):
        """A Pending pod the scheduler has NOT bound yet (no spec.node_name,
        no PodScheduled=True) with no determinable reason must NOT be labeled
        'container creation' -- it is still waiting for capacity, not creating
        a container. _inspect_pod_status returns (False, None), so no
        LAUNCH_PROGRESS row is emitted and the spinner stays a bare
        'Launching'."""
        pod = self._make_pod(
            phase='Pending',
            container_statuses=None,
        )
        pod.status.host_ip = None
        # Not bound: no node assignment and no PodScheduled=True condition.
        pod.spec.node_name = None
        pod.status.conditions = []
        # No events → tier-2 and tier-3 return None.
        monkeypatch.setattr(instance, '_get_pod_events', lambda *a, **kw: [])
        add_event = self._drive_one_iteration(monkeypatch, pod)
        lp_calls = [
            c for c in add_event.call_args_list if c.kwargs.get('event_type') is
            instance.global_user_state.ClusterEventType.LAUNCH_PROGRESS
        ]
        assert not lp_calls


class TestCheckInitContainersEnrichedRaise:
    """Tests the enriched raise message in _check_init_containers when an
    init container is in CrashLoopBackOff."""

    @staticmethod
    def _make_init_status(*,
                          waiting=None,
                          terminated=None,
                          last_terminated=None):
        s = mock.MagicMock()
        s.state.waiting = waiting
        s.state.terminated = terminated
        s.last_state.terminated = last_terminated
        return s

    @staticmethod
    def _make_pod(init_container_statuses, name='pod-0'):
        pod = mock.MagicMock()
        pod.metadata.name = name
        pod.status.init_container_statuses = init_container_statuses
        return pod

    def test_init_crashloopbackoff_unmasks_oomkilled(self, monkeypatch):
        # We drive _wait_for_pods_to_run with a pod whose main container is
        # in waiting.reason='PodInitializing', which causes _inspect_pod_status
        # to call _check_init_containers, which then raises the enriched error.
        init_cs = self._make_init_status(
            waiting=mock.MagicMock(
                reason='CrashLoopBackOff',
                message='back-off 5m0s restarting failed container=init pod=foo',
            ),
            last_terminated=mock.MagicMock(reason='OOMKilled', exit_code=137),
        )
        pod = self._make_pod([init_cs])
        pod.status.phase = 'Pending'
        pod.metadata.deletion_timestamp = None
        from sky.provision import constants as prov_constants
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: 'cn-on-cloud',
        }
        # Main container in PodInitializing so we dispatch to _check_init_containers.
        main_cs = mock.MagicMock()
        main_cs.state.waiting = mock.MagicMock(reason='PodInitializing',
                                               message='')
        main_cs.state.terminated = None
        main_cs.state.running = None
        main_cs.last_state.terminated = None
        pod.status.container_statuses = [main_cs]

        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.return_value = mock.MagicMock(items=[pod])
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)
        monkeypatch.setattr('sky.utils.subprocess_utils.run_in_parallel',
                            lambda fn, items, n: [fn(p) for p in items])
        monkeypatch.setattr('sky.utils.rich_utils.force_update_status',
                            lambda *a, **kw: None)
        monkeypatch.setattr(instance.time, 'sleep', lambda *a, **kw: None)
        monkeypatch.setattr(instance.global_user_state, 'add_cluster_event',
                            mock.MagicMock())

        with pytest.raises(config_lib.KubernetesError) as excinfo:
            instance._wait_for_pods_to_run(
                namespace='ns',
                context='ctx',
                cluster_name='cn',
                new_pods=[pod],
            )

        err = str(excinfo.value)
        assert 'Failed to create init container' in err
        assert 'OOMKilled' in err
        assert 'CrashLoopBackOff' not in err
        assert 'back-off 5m0s' in err  # waiting.message body preserved

    def test_init_other_waiting_reason_unchanged(self, monkeypatch):
        """Non-CrashLoopBackOff init failure: raise message format matches
        today (no unmask)."""
        init_cs = self._make_init_status(waiting=mock.MagicMock(
            reason='ImagePullBackOff',
            message='Back-off pulling image "init-img:bad"',
        ),)
        pod = self._make_pod([init_cs])
        pod.status.phase = 'Pending'
        pod.metadata.deletion_timestamp = None
        from sky.provision import constants as prov_constants
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: 'cn-on-cloud',
        }
        main_cs = mock.MagicMock()
        main_cs.state.waiting = mock.MagicMock(reason='PodInitializing',
                                               message='')
        main_cs.state.terminated = None
        main_cs.state.running = None
        main_cs.last_state.terminated = None
        pod.status.container_statuses = [main_cs]

        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.return_value = mock.MagicMock(items=[pod])
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)
        monkeypatch.setattr('sky.utils.subprocess_utils.run_in_parallel',
                            lambda fn, items, n: [fn(p) for p in items])
        monkeypatch.setattr('sky.utils.rich_utils.force_update_status',
                            lambda *a, **kw: None)
        monkeypatch.setattr(instance.time, 'sleep', lambda *a, **kw: None)
        monkeypatch.setattr(instance.global_user_state, 'add_cluster_event',
                            mock.MagicMock())

        with pytest.raises(config_lib.KubernetesError) as excinfo:
            instance._wait_for_pods_to_run(
                namespace='ns',
                context='ctx',
                cluster_name='cn',
                new_pods=[pod],
            )

        err = str(excinfo.value)
        assert 'Failed to create init container' in err
        assert 'ImagePullBackOff' in err
        assert 'init-img:bad' in err


# ---------------------------------------------------------------------------
# Tests for _inspect_pod_status init container pending reason reporting
# ---------------------------------------------------------------------------


def _make_init_status_with_name(name='init-copy-home',
                                running=False,
                                terminated_exit_code=None,
                                waiting_reason=None,
                                waiting_message=None):
    """Build a mock init container status with an explicit name."""
    cs = mock.MagicMock()
    cs.name = name
    cs.state.running = mock.MagicMock() if running else None
    cs.state.terminated = None
    cs.state.waiting = None
    if terminated_exit_code is not None:
        cs.state.terminated = mock.MagicMock()
        cs.state.terminated.exit_code = terminated_exit_code
        cs.state.terminated.message = None
    if waiting_reason is not None:
        cs.state.waiting = mock.MagicMock()
        cs.state.waiting.reason = waiting_reason
        cs.state.waiting.message = waiting_message
    return cs


class TestInspectPodStatusInitContainerReason:
    """Tests that _inspect_pod_status reports init container running
    instead of the stale 'Pulling' event reason."""

    @staticmethod
    def _make_pod(name='test-pod', cluster_name_on_cloud='test-cluster'):
        from sky.provision import constants as prov_constants
        pod = mock.MagicMock()
        pod.metadata.name = name
        pod.metadata.deletion_timestamp = None
        pod.metadata.labels = {
            prov_constants.TAG_SKYPILOT_CLUSTER_NAME: cluster_name_on_cloud,
        }
        return pod

    def _run_wait(self, monkeypatch, pod_sequence):
        """Run _wait_for_pods_to_run with a sequence of pod states.

        Each entry in pod_sequence is a callable that configures the pod
        for that iteration. The last entry must make the pod Running.
        Returns the list of (is_running, reason) tuples from each
        iteration and captured log messages.
        """
        cluster_name_on_cloud = 'test-cluster'
        pod = self._make_pod(cluster_name_on_cloud=cluster_name_on_cloud)

        iteration = [0]
        results = []

        def configure_and_list(*_args, **_kwargs):
            if iteration[0] < len(pod_sequence):
                pod_sequence[iteration[0]](pod)
            pods_list = mock.MagicMock()
            pods_list.items = [pod]
            return pods_list

        core_api = mock.MagicMock()
        core_api.list_namespaced_pod.side_effect = configure_and_list
        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **kw: core_api)

        def passthrough_parallel(fn, items, n):
            r = [fn(item) for item in items]
            results.append(r)
            iteration[0] += 1
            return r

        monkeypatch.setattr('sky.utils.subprocess_utils.run_in_parallel',
                            passthrough_parallel)
        monkeypatch.setattr('sky.utils.rich_utils.force_update_status',
                            lambda *a, **kw: None)
        monkeypatch.setattr(instance.time, 'sleep', lambda *a, **kw: None)
        monkeypatch.setattr(instance.global_user_state, 'add_cluster_event',
                            mock.MagicMock())

        log_messages = []

        def capture_debug(msg, *args, **kwargs):
            log_messages.append(msg % args if args else msg)

        monkeypatch.setattr(logger, 'debug', capture_debug)

        instance._wait_for_pods_to_run(
            namespace='ns',
            context='ctx',
            cluster_name='cn',
            new_pods=[pod],
        )
        return results, log_messages

    @staticmethod
    def _make_running_container_status():
        cs = mock.MagicMock()
        cs.state.running = True
        cs.state.waiting = None
        cs.state.terminated = None
        cs.last_state.terminated = None
        return cs

    def test_pulling_image_reason_during_actual_pull(self, monkeypatch):
        """While containers are in ContainerCreating, the event-based
        'Pulling' reason (with image name) should be reported."""
        monkeypatch.setattr(
            instance, '_get_pod_pending_reason', lambda *a, **kw:
            ('Pulling', 'Pulling image "us-docker.pkg.dev/foo:v1"'))

        def pending_creating(pod):
            pod.status.phase = 'Pending'
            cs = _make_container_status(waiting_reason='ContainerCreating')
            cs.state = mock.MagicMock()
            cs.state.waiting = mock.MagicMock()
            cs.state.waiting.reason = 'ContainerCreating'
            cs.state.terminated = None
            cs.last_state.terminated = None
            pod.status.container_statuses = [cs]
            pod.status.init_container_statuses = []

        def running(pod):
            pod.status.phase = 'Running'
            pod.status.container_statuses = [
                self._make_running_container_status()
            ]

        results, log_msgs = self._run_wait(monkeypatch,
                                           [pending_creating, running])

        assert results[0] == [(False, 'Pulling')]
        pull_logs = [m for m in log_msgs if 'Pulling' in m]
        assert any('us-docker.pkg.dev/foo:v1' in m for m in pull_logs)

    def test_pod_initializing_overrides_pulling_reason(self, monkeypatch):
        """When containers show PodInitializing with a running init
        container, the reason must reflect the init container name."""
        monkeypatch.setattr(
            instance, '_get_pod_pending_reason', lambda *a, **kw:
            ('Pulling', 'Pulling image "us-docker.pkg.dev/foo:v1"'))

        def pending_init(pod):
            pod.status.phase = 'Pending'
            cs = mock.MagicMock()
            cs.state = mock.MagicMock()
            cs.state.waiting = mock.MagicMock()
            cs.state.waiting.reason = 'PodInitializing'
            cs.state.terminated = None
            cs.last_state.terminated = None
            pod.status.container_statuses = [cs]
            pod.status.init_container_statuses = [
                _make_init_status_with_name(name='init-copy-home',
                                            running=True),
            ]

        def running(pod):
            pod.status.phase = 'Running'
            pod.status.container_statuses = [
                self._make_running_container_status()
            ]

        results, log_msgs = self._run_wait(monkeypatch, [pending_init, running])

        assert results[0] == [(False,
                               "init container 'init-copy-home' running (1/1)")]
        init_logs = [m for m in log_msgs if 'init container' in m]
        assert len(init_logs) >= 1
        assert "'init-copy-home'" in init_logs[0]
        assert '(1/1)' in init_logs[0]
        assert 'Pulling' not in init_logs[0]

    def test_pod_initializing_no_running_init_container(self, monkeypatch):
        """When PodInitializing but no init container is in running state,
        fall back to generic message."""
        monkeypatch.setattr(instance, '_get_pod_pending_reason',
                            lambda *a, **kw: None)

        def pending_init(pod):
            pod.status.phase = 'Pending'
            cs = mock.MagicMock()
            cs.state = mock.MagicMock()
            cs.state.waiting = mock.MagicMock()
            cs.state.waiting.reason = 'PodInitializing'
            cs.state.terminated = None
            cs.last_state.terminated = None
            pod.status.container_statuses = [cs]
            pod.status.init_container_statuses = [
                _make_init_status_with_name(name='init-copy-home',
                                            terminated_exit_code=0),
            ]

        def running(pod):
            pod.status.phase = 'Running'
            pod.status.container_statuses = [
                self._make_running_container_status()
            ]

        results, _ = self._run_wait(monkeypatch, [pending_init, running])
        assert results[0] == [(False, 'init container running')]

    def test_pod_initializing_multiple_init_containers(self, monkeypatch):
        """With multiple init containers, report the currently running one."""
        monkeypatch.setattr(instance, '_get_pod_pending_reason',
                            lambda *a, **kw: None)

        def pending_init(pod):
            pod.status.phase = 'Pending'
            cs = mock.MagicMock()
            cs.state = mock.MagicMock()
            cs.state.waiting = mock.MagicMock()
            cs.state.waiting.reason = 'PodInitializing'
            cs.state.terminated = None
            cs.last_state.terminated = None
            pod.status.container_statuses = [cs]
            pod.status.init_container_statuses = [
                _make_init_status_with_name(name='init-setup-ssh',
                                            terminated_exit_code=0),
                _make_init_status_with_name(name='init-copy-home',
                                            running=True),
            ]

        def running(pod):
            pod.status.phase = 'Running'
            pod.status.container_statuses = [
                self._make_running_container_status()
            ]

        results, _ = self._run_wait(monkeypatch, [pending_init, running])
        assert results[0] == [(False,
                               "init container 'init-copy-home' running (2/2)")]

    def test_log_message_includes_image_during_pull(self, monkeypatch):
        """The provision log must include the image name during actual pull."""
        monkeypatch.setattr(
            instance, '_get_pod_pending_reason', lambda *a, **kw:
            ('Pulling', 'Pulling image "registry.io/myimg:latest"'))

        def pending_creating(pod):
            pod.status.phase = 'Pending'
            cs = mock.MagicMock()
            cs.state = mock.MagicMock()
            cs.state.waiting = mock.MagicMock()
            cs.state.waiting.reason = 'ContainerCreating'
            cs.state.terminated = None
            cs.last_state.terminated = None
            pod.status.container_statuses = [cs]
            pod.status.init_container_statuses = []

        def running(pod):
            pod.status.phase = 'Running'
            pod.status.container_statuses = [
                self._make_running_container_status()
            ]

        _, log_msgs = self._run_wait(monkeypatch, [pending_creating, running])
        pending_logs = [m for m in log_msgs if 'is pending' in m]
        assert len(pending_logs) == 1
        assert 'Pulling' in pending_logs[0]
        assert 'registry.io/myimg:latest' in pending_logs[0]

    def test_log_message_no_image_during_init(self, monkeypatch):
        """The provision log must NOT include stale image pull info
        when the pod is actually waiting for init containers."""
        monkeypatch.setattr(
            instance, '_get_pod_pending_reason', lambda *a, **kw:
            ('Pulling', 'Pulling image "us-docker.pkg.dev/foo:v1"'))

        def pending_init(pod):
            pod.status.phase = 'Pending'
            cs = mock.MagicMock()
            cs.state = mock.MagicMock()
            cs.state.waiting = mock.MagicMock()
            cs.state.waiting.reason = 'PodInitializing'
            cs.state.terminated = None
            cs.last_state.terminated = None
            pod.status.container_statuses = [cs]
            pod.status.init_container_statuses = [
                _make_init_status_with_name(name='init-copy-home',
                                            running=True),
            ]

        def running(pod):
            pod.status.phase = 'Running'
            pod.status.container_statuses = [
                self._make_running_container_status()
            ]

        _, log_msgs = self._run_wait(monkeypatch, [pending_init, running])
        pending_logs = [m for m in log_msgs if 'is pending' in m]
        assert len(pending_logs) == 1
        assert 'init container' in pending_logs[0]
        assert 'Pulling image' not in pending_logs[0]


class TestConfigureRuntimeClass:
    """Tests for _configure_runtime_class.

    A falsy runtimeClassName (e.g. '' or None from a
    kubernetes.pod_config override) means the user explicitly disabled
    the runtime class. It must be stripped from ALL pods: the
    Kubernetes API rejects an empty string with 'resource name may not
    be empty', so leaving it on CPU-only pods breaks their creation.
    """

    def _pod_spec(self, runtime_class_name=...):
        spec = {'containers': [{'name': 'c'}]}
        if runtime_class_name is not ...:
            spec['runtimeClassName'] = runtime_class_name
        return {'spec': spec}

    def test_cpu_pod_empty_string_override_is_stripped(self):
        pod_spec = self._pod_spec(runtime_class_name='')
        instance._configure_runtime_class(pod_spec,
                                          nvidia_runtime_exists=True,
                                          needs_gpus_nvidia=False)
        assert 'runtimeClassName' not in pod_spec['spec']

    def test_cpu_pod_none_override_is_stripped(self):
        pod_spec = self._pod_spec(runtime_class_name=None)
        instance._configure_runtime_class(pod_spec,
                                          nvidia_runtime_exists=False,
                                          needs_gpus_nvidia=False)
        assert 'runtimeClassName' not in pod_spec['spec']

    def test_gpu_pod_falsy_override_not_replaced_with_nvidia(self):
        pod_spec = self._pod_spec(runtime_class_name='')
        instance._configure_runtime_class(pod_spec,
                                          nvidia_runtime_exists=True,
                                          needs_gpus_nvidia=True)
        assert 'runtimeClassName' not in pod_spec['spec']

    def test_gpu_pod_gets_nvidia_when_runtime_exists(self):
        pod_spec = self._pod_spec()
        instance._configure_runtime_class(pod_spec,
                                          nvidia_runtime_exists=True,
                                          needs_gpus_nvidia=True)
        assert pod_spec['spec']['runtimeClassName'] == 'nvidia'

    def test_gpu_pod_unchanged_when_runtime_missing(self):
        pod_spec = self._pod_spec()
        instance._configure_runtime_class(pod_spec,
                                          nvidia_runtime_exists=False,
                                          needs_gpus_nvidia=True)
        assert 'runtimeClassName' not in pod_spec['spec']

    def test_user_custom_runtime_class_is_preserved(self):
        pod_spec = self._pod_spec(runtime_class_name='sysbox-runc')
        instance._configure_runtime_class(pod_spec,
                                          nvidia_runtime_exists=True,
                                          needs_gpus_nvidia=True)
        assert pod_spec['spec']['runtimeClassName'] == 'sysbox-runc'

    def test_cpu_pod_custom_runtime_class_is_preserved(self):
        pod_spec = self._pod_spec(runtime_class_name='sysbox-runc')
        instance._configure_runtime_class(pod_spec,
                                          nvidia_runtime_exists=True,
                                          needs_gpus_nvidia=False)
        assert pod_spec['spec']['runtimeClassName'] == 'sysbox-runc'


# ---------- Pod creation 409 "object is being deleted" handling ----------


class TestRequiredKueueAdmission:
    """Fail-closed preparation and create-response attestation."""

    def test_prepare_reasserts_server_owned_contract(self):
        pod_spec = {
            'metadata': {
                'labels': {
                    k8s_constants.KUEUE_QUEUE_LABEL: 'forged-queue',
                    k8s_constants.KUEUE_POD_GROUP_LABEL: 'forged-group',
                    k8s_constants.KUEUE_MANAGED_KEY: 'true',
                    k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL: 'admin',
                },
                'annotations': {
                    k8s_constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION: '99',
                },
                'finalizers': [
                    k8s_constants.KUEUE_MANAGED_FINALIZER,
                    'example.com/keep',
                ],
            },
            'spec': {
                'schedulingGates': [{
                    'name': 'example.com/keep'
                }],
            },
        }

        instance._prepare_pod_for_required_kueue(  # pylint: disable=protected-access
            pod_spec,
            expected_queue='inference',
            pod_group_name='service-replica',
            pod_group_total_count=4,
            workload_priority_class_name='inference-low',
        )

        metadata = pod_spec['metadata']
        assert metadata['labels'] == {
            k8s_constants.KUEUE_QUEUE_LABEL: 'inference',
            k8s_constants.KUEUE_POD_GROUP_LABEL: 'service-replica',
            k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL: 'inference-low',
        }
        assert metadata['annotations'][
            k8s_constants.KUEUE_POD_GROUP_TOTAL_COUNT_ANNOTATION] == '4'
        assert metadata['annotations'][
            k8s_constants.KUEUE_RETRIABLE_IN_GROUP_ANNOTATION] == 'false'
        assert metadata['finalizers'] == ['example.com/keep']
        assert pod_spec['spec']['schedulingGates'] == [
            {
                'name': 'example.com/keep'
            },
            {
                'name': k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
            },
        ]

    def test_prepare_deduplicates_gate_and_removes_unconfigured_priority(self):
        pod_spec = {
            'metadata': {
                'labels': {
                    k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL: 'admin',
                }
            },
            'spec': {
                'schedulingGates': [{
                    'name': k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
                }]
            },
        }

        instance._prepare_pod_for_required_kueue(  # pylint: disable=protected-access
            pod_spec,
            expected_queue='inference',
            pod_group_name='replica',
            pod_group_total_count=1,
            workload_priority_class_name=None,
        )

        assert k8s_constants.KUEUE_WORKLOAD_PRIORITY_CLASS_LABEL not in (
            pod_spec['metadata']['labels'])
        assert pod_spec['spec']['schedulingGates'] == [{
            'name': k8s_constants.KUEUE_ADMISSION_SCHEDULING_GATE
        }]

    def test_create_accepts_kueue_mutated_response(self, monkeypatch):
        created_pod = mock.MagicMock()
        created_pod.metadata.name = 'replica-head'
        created_pod.metadata.labels = {
            k8s_constants.KUEUE_MANAGED_KEY: 'true',
            k8s_constants.KUEUE_QUEUE_LABEL: 'inference',
        }
        core_api_mock = mock.MagicMock()
        core_api_mock.create_namespaced_pod.return_value = created_pod
        monkeypatch.setattr(kubernetes, 'core_api',
                            lambda *a, **k: core_api_mock)

        result = instance._create_namespaced_pod_with_retries(
            'inference-ns', {'metadata': {
                'name': 'replica-head'
            }},
            None,
            expected_kueue_queue='inference')

        assert result is created_pod
        core_api_mock.delete_namespaced_pod.assert_not_called()

    @pytest.mark.parametrize('labels', [
        {
            k8s_constants.KUEUE_QUEUE_LABEL: 'inference',
        },
        {
            k8s_constants.KUEUE_MANAGED_KEY: 'true',
            k8s_constants.KUEUE_QUEUE_LABEL: 'wrong-queue',
        },
    ])
    def test_create_deletes_and_rejects_unattested_response(
            self, monkeypatch, labels):
        created_pod = mock.MagicMock()
        created_pod.metadata.name = 'replica-head'
        created_pod.metadata.labels = labels
        core_api_mock = mock.MagicMock()
        core_api_mock.create_namespaced_pod.return_value = created_pod
        monkeypatch.setattr(kubernetes, 'core_api',
                            lambda *a, **k: core_api_mock)

        with pytest.raises(config_lib.KubernetesError,
                           match='to prevent it from bypassing Kueue'):
            instance._create_namespaced_pod_with_retries(
                'inference-ns', {'metadata': {
                    'name': 'replica-head'
                }},
                None,
                expected_kueue_queue='inference')

        core_api_mock.delete_namespaced_pod.assert_called_once_with(
            'replica-head',
            'inference-ns',
            grace_period_seconds=0,
            _request_timeout=config_lib.DELETION_TIMEOUT)

    def test_apparmor_retry_cannot_bypass_attestation(self, monkeypatch):
        forbidden = _make_api_exception(
            422,
            'Unprocessable Entity',
            body=json.dumps(
                {'message': 'FieldValueForbidden AppArmorProfile: nil'}))
        created_pod = mock.MagicMock()
        created_pod.metadata.name = 'replica-head'
        created_pod.metadata.labels = {
            k8s_constants.KUEUE_QUEUE_LABEL: 'inference'
        }
        core_api_mock = mock.MagicMock()
        core_api_mock.create_namespaced_pod.side_effect = [
            forbidden, created_pod
        ]
        monkeypatch.setattr(kubernetes, 'core_api',
                            lambda *a, **k: core_api_mock)
        monkeypatch.setattr(kubernetes, 'api_exception',
                            lambda *a, **k: FakeApiException)
        apparmor_key = ('container.apparmor.security.beta.kubernetes.io/'
                        f'{k8s_constants.RAY_NODE_CONTAINER_NAME}')
        pod_spec = {
            'metadata': {
                'name': 'replica-head',
                'annotations': {
                    apparmor_key: 'unconfined'
                },
            },
        }

        with pytest.raises(config_lib.KubernetesError,
                           match='was not admitted'):
            instance._create_namespaced_pod_with_retries(
                'inference-ns',
                pod_spec,
                None,
                expected_kueue_queue='inference')

        assert core_api_mock.create_namespaced_pod.call_count == 2
        assert apparmor_key not in pod_spec['metadata']['annotations']
        core_api_mock.delete_namespaced_pod.assert_called_once()

    def test_terminating_pod_retry_still_attests_success(self, monkeypatch):
        conflict = _make_api_exception(
            409,
            'Conflict',
            body=json.dumps({
                'message': ('object is being deleted: pods "replica-head" '
                            'already exists')
            }))
        created_pod = mock.MagicMock()
        created_pod.metadata.name = 'replica-head'
        created_pod.metadata.labels = {
            k8s_constants.KUEUE_MANAGED_KEY: 'true',
            k8s_constants.KUEUE_QUEUE_LABEL: 'inference',
        }
        stuck_pod = mock.MagicMock()
        stuck_pod.metadata.deletion_timestamp = object()
        stuck_pod.metadata.finalizers = []
        core_api_mock = mock.MagicMock()
        core_api_mock.create_namespaced_pod.side_effect = [
            conflict, created_pod
        ]
        core_api_mock.read_namespaced_pod.return_value = stuck_pod
        monkeypatch.setattr(kubernetes, 'core_api',
                            lambda *a, **k: core_api_mock)
        monkeypatch.setattr(kubernetes, 'api_exception',
                            lambda *a, **k: FakeApiException)

        result = instance._create_namespaced_pod_with_retries(
            'inference-ns', {'metadata': {
                'name': 'replica-head'
            }},
            None,
            expected_kueue_queue='inference')

        assert result is created_pod
        assert core_api_mock.create_namespaced_pod.call_count == 2


class TestCreatePodFinalizerHandling:
    """Recovery path for the 409 ``object is being deleted`` error:

    ``_create_namespaced_pod_with_retries`` force-removes the terminating pod
    (strip Kueue finalizer + force-delete grace=0), then recreates it.
    """

    _DELETING_MSG = ('object is being deleted: pods "t-reco-head" already '
                     'exists')

    def _conflict_exc(self):
        return _make_api_exception(409,
                                   'Conflict',
                                   body=json.dumps(
                                       {'message': self._DELETING_MSG}))

    def test_strips_kueue_finalizer_then_recreates(self, monkeypatch):
        """Kueue case: strip the finalizer, force-delete (grace 0), recreate."""
        conflict = self._conflict_exc()
        created_pod = mock.MagicMock()
        created_pod.metadata.name = 't-reco-head'

        stuck_pod = mock.MagicMock()
        stuck_pod.metadata.finalizers = [
            'kueue.x-k8s.io/managed', 'example.com/other'
        ]

        core_api_mock = mock.MagicMock()
        # 1: initial create 409s; 2: create after force-removing the old pod
        # succeeds.
        core_api_mock.create_namespaced_pod.side_effect = [
            conflict, created_pod
        ]
        core_api_mock.read_namespaced_pod.return_value = stuck_pod

        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **k: core_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *a, **k: FakeApiException)

        pod_spec = {'metadata': {'name': 't-reco-head'}, 'spec': {}}
        result = instance._create_namespaced_pod_with_retries(
            'default', pod_spec, None)

        assert result is created_pod
        # JSON patch (list body), not strategic-merge (which no-ops on the
        # finalizers list); keeps the unrelated finalizer, drops only Kueue's.
        patch_call = core_api_mock.patch_namespaced_pod.call_args
        assert patch_call.args[2] == [{
            'op': 'replace',
            'path': '/metadata/finalizers',
            'value': ['example.com/other']
        }]
        # Removing the finalizer alone leaves the pod lingering for its grace
        # period, so it must also be force-deleted (grace 0) before recreating.
        assert any(
            c.kwargs.get('grace_period_seconds') == 0
            for c in core_api_mock.delete_namespaced_pod.call_args_list)
        assert core_api_mock.create_namespaced_pod.call_count == 2

    def test_force_delete_without_finalizer_then_recreates(self, monkeypatch):
        """Non-Kueue case: no finalizer to strip; force-delete (grace 0) then

        recreate. The pod is read (to check finalizers) but never patched.
        """
        conflict = self._conflict_exc()
        created_pod = mock.MagicMock()
        created_pod.metadata.name = 't-reco-head'

        plain_pod = mock.MagicMock()
        plain_pod.metadata.finalizers = None

        core_api_mock = mock.MagicMock()
        core_api_mock.create_namespaced_pod.side_effect = [
            conflict, created_pod
        ]
        core_api_mock.read_namespaced_pod.return_value = plain_pod

        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **k: core_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *a, **k: FakeApiException)

        pod_spec = {'metadata': {'name': 't-reco-head'}, 'spec': {}}
        result = instance._create_namespaced_pod_with_retries(
            'default', pod_spec, None)

        assert result is created_pod
        # No Kueue finalizer, so no patch; still force-deleted (grace 0).
        core_api_mock.patch_namespaced_pod.assert_not_called()
        assert any(
            c.kwargs.get('grace_period_seconds') == 0
            for c in core_api_mock.delete_namespaced_pod.call_args_list)
        assert core_api_mock.create_namespaced_pod.call_count == 2

    def test_pod_gc_d_between_read_and_patch(self, monkeypatch):
        """If the pod is GC'd between read and finalizer-patch, the 404 on the

        patch is the success state (pod gone) — return (skipping the redundant
        force-delete) and let the caller recreate, rather than raising.
        """
        conflict = self._conflict_exc()
        not_found = _make_api_exception(404, 'Not Found')
        created_pod = mock.MagicMock()
        created_pod.metadata.name = 't-reco-head'

        stuck_pod = mock.MagicMock()
        stuck_pod.metadata.finalizers = ['kueue.x-k8s.io/managed']

        core_api_mock = mock.MagicMock()
        core_api_mock.create_namespaced_pod.side_effect = [
            conflict, created_pod
        ]
        core_api_mock.read_namespaced_pod.return_value = stuck_pod
        core_api_mock.patch_namespaced_pod.side_effect = not_found

        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **k: core_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *a, **k: FakeApiException)

        pod_spec = {'metadata': {'name': 't-reco-head'}, 'spec': {}}
        result = instance._create_namespaced_pod_with_retries(
            'default', pod_spec, None)

        assert result is created_pod
        # 404 on patch => pod already gone => skip force-delete, just recreate.
        core_api_mock.delete_namespaced_pod.assert_not_called()
        assert core_api_mock.create_namespaced_pod.call_count == 2

    def test_rejects_non_terminating_pod(self, monkeypatch):
        """The helper is only valid for a terminating pod; if the read returns a

        live pod (no deletionTimestamp), fail rather than force-deleting it.
        """
        conflict = self._conflict_exc()
        live_pod = mock.MagicMock()
        live_pod.metadata.deletion_timestamp = None  # not terminating

        core_api_mock = mock.MagicMock()
        core_api_mock.create_namespaced_pod.side_effect = conflict
        core_api_mock.read_namespaced_pod.return_value = live_pod

        monkeypatch.setattr('sky.adaptors.kubernetes.core_api',
                            lambda *a, **k: core_api_mock)
        monkeypatch.setattr('sky.adaptors.kubernetes.api_exception',
                            lambda *a, **k: FakeApiException)

        pod_spec = {'metadata': {'name': 't-reco-head'}, 'spec': {}}
        with pytest.raises(config_lib.KubernetesError,
                           match='Refusing to force-remove'):
            instance._create_namespaced_pod_with_retries(
                'default', pod_spec, None)
        # Must not have touched the live pod.
        core_api_mock.patch_namespaced_pod.assert_not_called()
        core_api_mock.delete_namespaced_pod.assert_not_called()
