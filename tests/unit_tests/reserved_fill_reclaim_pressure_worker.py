"""Narrow spawned worker for the Serve054 process-pressure test.

The pressure case exercises 90 clean ``spawn`` interpreters. Importing
SkyPilot's CLI/API re-export packages in each process does not add coverage for
the provider-proof repository, but consumes most of a 4x16 runner. A clean
worker therefore installs only package search paths before importing the real
policy, repository, schemas, and database utilities from this checkout. The
ordinary pytest process has already imported ``sky`` and never takes this path.

This is strictly a test-only import topology for qualifying the real database
and concurrency contract. It is not a production worker bootstrap and its RSS
is not evidence about production handler RSS. Separate ``DisposableExecutor``
tests own the production process boundary.
"""

# This isolated test target deliberately installs package search paths before
# importing real implementation modules and wraps private pressure-test seams.
# pylint: disable=protected-access,wrong-import-position

import importlib.machinery
import json
import math
import multiprocessing
import os
import pathlib
import sys
import time
import types
from typing import Any
from unittest import mock

import sqlalchemy

_REPO_ROOT = pathlib.Path(__file__).parents[2]


def _install_minimal_package(name: str, path: pathlib.Path) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    module.__package__ = name
    module.__spec__ = importlib.machinery.ModuleSpec(name,
                                                     loader=None,
                                                     is_package=True)
    module.__spec__.submodule_search_locations = module.__path__
    sys.modules[name] = module
    if '.' in name:
        parent_name, child_name = name.rsplit('.', 1)
        setattr(sys.modules[parent_name], child_name, module)
    return module


MINIMAL_SKY_PACKAGE_BOOTSTRAP = (multiprocessing.parent_process() is not None
                                 and 'sky' not in sys.modules)
if MINIMAL_SKY_PACKAGE_BOOTSTRAP:
    sky_package = _install_minimal_package('sky', _REPO_ROOT / 'sky')
    _install_minimal_package('sky.serve', _REPO_ROOT / 'sky' / 'serve')

from sky.serve import reserved_fill_reclaim_attestation as reclaim
from sky.serve import reserved_fill_reclaim_proofs as proofs

_POLICY_PROJECT = _REPO_ROOT / 'boltz' / 'reserved_fill_reclaim_policy'
sys.path.insert(0, str(_POLICY_PROJECT / 'src'))

from boltz_reserved_fill_reclaim_policy import aws_attestation  # noqa: E402
from boltz_reserved_fill_reclaim_policy import (  # noqa: E402
    kubernetes_attestation)
from boltz_reserved_fill_reclaim_policy import policy as policy_lib

_CONTEXT = 'phx_research_cluster_eks'
_GATE_GENERATION = 17


def _admission(
    policy: policy_lib.BoltzReservedFillReclaimPolicy,
) -> reclaim.ReclaimProjectedAdmission:
    context = policy._bundle.fleet_context(_CONTEXT)
    accelerator = context['accelerators']['h200']
    admission = context['kueue_admission']
    assert admission is not None
    priority = context['priority_class']
    return reclaim.ReclaimProjectedAdmission(
        worker_projection_sha256='c' * 64,
        kubernetes_context=_CONTEXT,
        namespace=context['namespace'],
        service_account_name=context['service_account_name'],
        pod_identity_role_arn=context['pod_identity_role_arn'],
        scheduler_name=context['scheduler_name'],
        priority_class_name=priority['name'],
        priority_value=priority['value'],
        preemption_policy=priority['preemption_policy'],
        admission_mode=reclaim.ReclaimAdmissionMode.KUEUE,
        local_queue_name=admission['local_queue_name'],
        workload_priority_class_name=(
            admission['workload_priority_class_name']),
        accelerator='h200',
        accelerator_count=accelerator['count'],
        accelerator_scheduling=reclaim.ReclaimAcceleratorScheduling(
            label_key=accelerator['product_label_key'],
            label_values=tuple(sorted(accelerator['product_label_values'])),
            resource_key=accelerator['resource_name']))


def _launch_scope(
    policy: policy_lib.BoltzReservedFillReclaimPolicy,
) -> reclaim.ReclaimLaunchScope:
    context = policy._bundle.fleet_context(_CONTEXT)
    return reclaim.ReclaimLaunchScope(
        service_name='boltz-l4-fleet',
        service_version=64,
        pool_key=json.dumps(['v2', context['physical_cluster_uid'], 'h200']),
        service_generation=21,
        physical_cluster_uid=context['physical_cluster_uid'],
        kubernetes_context=_CONTEXT,
        accelerator='h200',
        accelerator_count=1,
        projected_admission=_admission(policy))


def _provider_proof(
    policy: policy_lib.BoltzReservedFillReclaimPolicy,
    domain: str,
) -> object:
    context = policy._bundle.fleet_context(_CONTEXT)
    provider = policy._bundle.provider_context(_CONTEXT)
    if domain == 'aws':
        return aws_attestation.PodIdentityProof(
            kubernetes_context=_CONTEXT,
            cluster_arn=provider['eks']['cluster_arn'],
            namespace=context['namespace'],
            service_account_name=context['service_account_name'],
            expected_role_arn=context['pod_identity_role_arn'],
            association_count=0,
            identity_absence_proven=True)
    admission = context['kueue_admission']
    assert admission is not None
    return kubernetes_attestation.KubernetesContextProof(
        kubernetes_context=_CONTEXT,
        physical_cluster_uid=context['physical_cluster_uid'],
        namespace_uid=provider['namespace_uid'],
        kueue_managed=True,
        local_queue_name=admission['local_queue_name'],
        cluster_queue_name=admission['queues']['inference_cluster_queue'],
        pod_identity_irsa_annotation_absent=True,
        assign_queue_labels_for_pods=True,
        topology_aware_scheduling=True,
        custom_scheduler_deployment_proven=False,
        resource_flavor_topology_names=tuple(
            sorted((flavor['name'], flavor['topology_name'])
                   for flavor in provider['resource_flavors'])),
        node_flavors=tuple(
            kubernetes_attestation.NodeFlavorProof(
                flavor=node['flavor'],
                non_deleting_node_count=1,
                product_label_value=node['product_label_value'],
                resource_name=node['resource_name'],
                capacity_per_node=node['capacity_per_node'])
            for node in provider['node_inventory']))


def multiprocess_launch_authorization(
    database_url: str,
    ready_queue: Any,
    start_event: Any,
    deadline_value: Any,
    provider_started_queue: Any,
    provider_release_event: Any,
    loser_parked_queue: Any,
    loser_release_event: Any,
) -> tuple[str, bool, float, float, bool]:
    """Create one independent policy/engine, like a disposable handler."""
    engine = sqlalchemy.create_engine(
        database_url,
        pool_size=2,
        max_overflow=0,
        connect_args={'application_name': 'skypilot-reclaim-proof-worker'})
    counter_engine = sqlalchemy.create_engine(
        database_url,
        poolclass=sqlalchemy.NullPool,
        connect_args={'application_name': 'skypilot-reclaim-proof-counter'})
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = proofs.ReclaimProviderProofRepository(engine)

    def _provider_job(context_name, domain, _deadline, _cancellation):
        assert context_name == _CONTEXT
        with counter_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO serve054_test_provider_calls
                        (domain, call_count)
                    VALUES (:domain, 1)
                    ON CONFLICT (domain) DO UPDATE
                    SET call_count =
                        serve054_test_provider_calls.call_count + 1
                """), {'domain': domain})
        provider_started_queue.put((os.getpid(), domain))
        if not provider_release_event.wait(timeout=120):
            raise TimeoutError('The parent did not release provider proof.')
        return _provider_proof(policy, domain)

    original_wait_for_receipt = repository._wait_for_published_receipt

    def _wait_after_parent_observation(*args: Any, **kwargs: Any) -> Any:
        # This method is entered only after the losing election transaction's
        # NullPool connection has been physically closed. Parking here gives
        # the parent a deterministic no-retained-loser observation point.
        loser_parked_queue.put(os.getpid())
        if not loser_release_event.wait(timeout=120):
            raise TimeoutError('The parent did not release receipt polling.')
        return original_wait_for_receipt(*args, **kwargs)

    policy._provider_job = _provider_job
    policy._emit_proof = lambda _payload: None
    repository._wait_for_published_receipt = _wait_after_parent_observation
    identity = policy.policy_identity()
    ready_queue.put(os.getpid())
    if not start_event.wait(timeout=120):
        raise TimeoutError('Cold-wave start barrier was not released.')
    deadline = float(deadline_value.value)
    if not math.isfinite(deadline) or deadline <= 0:
        raise AssertionError('The parent did not publish a wave deadline.')
    wave_start = deadline - reclaim.POLICY_OPERATION_TIMEOUT_SECONDS
    started = time.monotonic()
    with mock.patch.object(policy_lib.reserved_fill_reclaim_proofs,
                           'ReclaimProviderProofRepository',
                           return_value=repository):
        authorization = policy.authorize_launch(
            _launch_scope(policy),
            expected_identity=identity,
            expected_gate_generation=_GATE_GENERATION,
            deadline_monotonic=deadline)
    with engine.begin() as connection:
        guard_holds = proofs.provider_proof_reference_holds_in_connection(
            connection,
            authorization.provider_proof_reference,
            expected_physical_cluster_uid=(
                authorization.scope.physical_cluster_uid))
    nonce = authorization.provider_proof_reference.receipt_nonce
    repository._proof_engine.dispose()
    engine.dispose()
    counter_engine.dispose()
    return (nonce, guard_holds, time.monotonic() - wave_start,
            started - wave_start, MINIMAL_SKY_PACKAGE_BOOTSTRAP)
