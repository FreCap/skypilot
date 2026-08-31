"""Deployment-policy tests for Boltz reserved-capacity reclaim."""

# pylint: disable=protected-access,wrong-import-position
import base64
import contextlib
import copy
import dataclasses
import datetime
import hashlib
import json
import pathlib
import sys
import threading
import time
import types
from typing import Any
from unittest import mock

import pytest

_REPO_ROOT = pathlib.Path(__file__).parents[2]
_POLICY_PROJECT = _REPO_ROOT / 'boltz' / 'reserved_fill_reclaim_policy'
sys.path.insert(0, str(_POLICY_PROJECT / 'src'))

from boltz_reserved_fill_reclaim_policy import aws_attestation  # noqa: E402
from boltz_reserved_fill_reclaim_policy import bundle as bundle_lib
from boltz_reserved_fill_reclaim_policy import (  # noqa: E402
    kubernetes_attestation)
from boltz_reserved_fill_reclaim_policy import policy as policy_lib
from boltz_reserved_fill_reclaim_policy import POLICY_REVISION  # noqa: E402
from boltz_reserved_fill_reclaim_policy import preflight  # noqa: E402

from sky.serve import reserved_fill_reclaim_attestation as reclaim
from sky.serve import reserved_fill_reclaim_proofs as reclaim_proofs


def _wait_for_no_thread(prefix: str, timeout: float = 1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not any(
                thread.name.startswith(prefix)
                for thread in threading.enumerate()):
            return True
        time.sleep(0.01)
    return False


def _bundle_document() -> dict:
    return json.loads(
        (_POLICY_PROJECT / 'src' / 'boltz_reserved_fill_reclaim_policy' /
         'fleet_bundle.json').read_text(encoding='utf-8'))


def _encoded_bundle(document: dict) -> bytes:
    return json.dumps(document, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')


def _managed_document_context(document: dict) -> dict:
    return next(context for context in document['fleet']['contexts']
                if context['kueue_admission'] is not None)


def _managed_provider_context(document: dict) -> dict:
    return next(
        context for context in document['provider_inventory']['contexts']
        if context['kueue_enforcement'] is not None)


def test_embedded_bundle_binds_observed_gpu_products_and_canonical_names():
    bundle = bundle_lib.load_embedded_bundle()
    east = bundle.fleet_context('prod_research_cluster_eks')
    phx = bundle.fleet_context('phx_research_cluster_eks')

    assert east['kueue_admission'] is None
    assert east['priority_class'][
        'name'] == 'rescluster-k8s-prod-east1-preemptible-inference-low'
    assert east['service_account_name'] == 'skypilot-pool-sa'
    assert phx['service_account_name'] == 'skypilot-pool-sa'
    assert phx['namespace'] == 'boltz-research'
    assert phx['kueue_admission'] == {
        'cluster_queue_name': 'research-be',
        'local_queue_name': 'be',
        'workload_priority_class_name': 'be-lt',
        'workload_priority_value': 11,
    }
    assert phx['priority_class'] == {
        'name': 'rescluster-k8s-prod-east1-preemptible-inference-low',
        'preemption_policy': 'Never',
        'value': -1000,
    }
    assert east['scheduler_name'] == 'gpu-binpack-scheduler'
    assert phx['scheduler_name'] == 'default-scheduler'
    assert east['accelerators']['a100-80gb'] == {
        'count': 1,
        'flavors': ['ml.p4de.24xlarge'],
        'product_label_key': 'nvidia.com/gpu.product',
        'product_label_values': ['NVIDIA-A100-SXM4-80GB'],
        'resource_name': 'nvidia.com/gpu',
    }
    assert east['accelerators']['a100']['product_label_values'] == [
        'NVIDIA-A100-SXM4-40GB'
    ]
    assert phx['accelerators']['h200']['product_label_values'] == [
        'NVIDIA-H200'
    ]
    assert bundle.provider_context('phx_research_cluster_eks')[
        'namespace_uid'] == '44f8d097-6591-46cf-9b8e-59deea8777e7'
    assert bundle.policy_revision == (
        f'boltz-reserved-fill-reclaim-policy/{POLICY_REVISION}')
    assert POLICY_REVISION == '1.1.1578'


def test_policy_exposes_provider_free_physical_pool_inventory():
    policy = policy_lib.BoltzReservedFillReclaimPolicy()

    assert policy.provider_free_pool_inventory() == (
        reclaim.ReclaimPoolInventoryEntry(
            access_context='phx_research_cluster_eks',
            physical_cluster_uid='ba2dcdca-2a0d-447f-ad8a-31849a63c1d5',
            accelerator_shapes=(('h200', 1),)),
        reclaim.ReclaimPoolInventoryEntry(
            access_context='prod_research_cluster_eks',
            physical_cluster_uid='14de98b4-cb7b-4f82-beb7-6f754a96f1dd',
            accelerator_shapes=(('a100', 1), ('a100-80gb', 1))),
    )


def test_altered_inventory_under_same_identity_cannot_authorize_claim(
        monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    identity = policy.policy_identity()
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    forged_uid = '00000000-0000-4000-8000-000000000001'
    forged_inventory = (reclaim.ReclaimPoolInventoryEntry(
        access_context=context['kubernetes_context'],
        physical_cluster_uid=forged_uid,
        accelerator_shapes=(('h200', 1),)),)
    monkeypatch.setattr(policy, 'provider_free_pool_inventory',
                        lambda: forged_inventory)

    # The generic boundary verifies the policy's exact gate identity. The
    # independent claim callback must still validate every returned atom from
    # the embedded bundle so an implementation bug cannot turn identity-only
    # bootstrap data into capacity authority.
    assert reclaim.require_provider_free_pool_inventory(
        policy, identity) == forged_inventory
    edge = dataclasses.replace(_edge(context),
                               pool_key=json.dumps(['v2', forged_uid, 'h200']),
                               physical_cluster_uid=forged_uid)
    scope = reclaim.ReclaimClaimSetScope(service_name='service',
                                         service_incarnation='incarnation',
                                         service_version=1,
                                         semantic_hash='semantic',
                                         edges=(edge,))
    read_provider_proof = mock.Mock()
    monkeypatch.setattr(policy, '_read_launch_context', read_provider_proof)

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='physical-cluster identity is not allowlisted'):
        policy.authorize_claim_set(scope,
                                   expected_identity=identity,
                                   expected_gate_generation=7,
                                   deadline_monotonic=time.monotonic() + 5)

    read_provider_proof.assert_not_called()


def test_bundle_hashes_are_domain_separated_and_order_independent():
    document = _bundle_document()
    first = bundle_lib.parse_bundle_bytes(_encoded_bundle(document))
    reordered = copy.deepcopy(document)
    reordered['fleet']['contexts'].reverse()
    reordered['provider_inventory']['contexts'].reverse()
    for context in reordered['fleet']['contexts']:
        for accelerator in context['accelerators'].values():
            accelerator['flavors'].reverse()
            accelerator['product_label_values'].reverse()
    for context in reordered['provider_inventory']['contexts']:
        context['resource_flavors'].reverse()
        context['node_inventory'].reverse()
    second = bundle_lib.parse_bundle_bytes(_encoded_bundle(reordered))

    assert first.fleet_bundle_sha256 == second.fleet_bundle_sha256
    assert (first.provider_inventory_sha256 == second.provider_inventory_sha256)
    assert first.fleet_bundle_sha256 != first.provider_inventory_sha256
    provider_bytes = bundle_lib._canonical_bytes(
        bundle_lib._normalized_section(document['provider_inventory']))
    assert first.provider_inventory_sha256 == hashlib.sha256(
        b'boltz-reserved-fill/provider/v4\x00' + provider_bytes).hexdigest()
    assert first.provider_inventory_sha256 != hashlib.sha256(
        b'boltz-reserved-fill/provider/v3\x00' + provider_bytes).hexdigest()
    fleet_bytes = bundle_lib._canonical_bytes(
        bundle_lib._normalized_section(document['fleet']))
    assert first.fleet_bundle_sha256 == hashlib.sha256(
        b'boltz-reserved-fill/fleet/v6\x00' + fleet_bytes).hexdigest()
    assert first.fleet_bundle_sha256 != hashlib.sha256(
        b'boltz-reserved-fill/fleet/v5\x00' + fleet_bytes).hexdigest()


def test_bundle_rejects_duplicate_keys_unknown_keys_and_unsafe_provider():
    with pytest.raises(bundle_lib.BundleValidationError, match='Duplicate'):
        bundle_lib.parse_bundle_bytes(
            b'{"schema_version":7,"schema_version":7}')

    document = _bundle_document()
    document['unknown'] = True
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='unexpected schema'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    _managed_provider_context(
        document)['node_inventory'][0]['product_label_value'] = 'NVIDIA-A100'
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='reviewed flavor, Node selector, and product'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))


@pytest.mark.parametrize('mutation,match', [
    (lambda context: context.__setitem__('namespace', 'another-namespace'),
     'external-lane contract'),
    (lambda context: context['kueue_admission'].__setitem__(
        'local_queue_name', 'skypilot-be'), 'external-lane contract'),
    (lambda context: context['kueue_admission'].__setitem__(
        'cluster_queue_name', 'skypilot-be'), 'external-lane contract'),
    (lambda context: context['kueue_admission'].__setitem__(
        'workload_priority_class_name', 'skypilot-reserved-fill'),
     'external-lane contract'),
    (lambda context: context['kueue_admission'].__setitem__(
        'workload_priority_value', -1000), 'external-lane contract'),
    (lambda context: context['priority_class'].__setitem__('value', 12),
     'exact server-owned'),
])
def test_bundle_requires_exact_external_kueue_lane(mutation, match):
    document = _bundle_document()
    context = _managed_document_context(document)
    mutation(context)

    with pytest.raises(bundle_lib.BundleValidationError, match=match):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))


def test_bundle_requires_one_node_contract_per_provider_flavor():
    document = _bundle_document()
    document['provider_inventory']['contexts'][0]['node_inventory'].pop()

    with pytest.raises(bundle_lib.BundleValidationError,
                       match='cover each ResourceFlavor once'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    document['provider_inventory']['contexts'][0]['node_inventory'][0][
        'selector_label_value'] = 'ml.wrong'
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='selector is not owned'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    _managed_provider_context(document)['kueue_enforcement']['webhooks'][
        'validating']['operations'] = ['CREATE']
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='exact reviewed Pod operations'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))


def test_bundle_requires_matching_kueue_admission_and_enforcement():
    document = _bundle_document()
    _managed_provider_context(document)['kueue_enforcement'] = None

    with pytest.raises(bundle_lib.BundleValidationError,
                       match='both null or both configured'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))


def test_bundle_requires_one_scheduler_or_tas_topology_authority():
    document = _bundle_document()
    _managed_document_context(document)['scheduler_name'] = (
        'gpu-binpack-scheduler')

    with pytest.raises(bundle_lib.BundleValidationError,
                       match='Kueue TAS must be the sole topology authority'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    _managed_provider_context(document)['scheduler'] = copy.deepcopy(
        document['provider_inventory']['contexts'][0]['scheduler'])
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='Kueue TAS must be the sole topology authority'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    _managed_provider_context(
        document)['resource_flavors'][0]['topology_name'] = None
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='exact topology names'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    del _managed_provider_context(document)['kueue_enforcement']['controller'][
        'required_feature_gates']['TopologyAwareScheduling']
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='exact reviewed Kueue TAS gate set'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))


def _admission(context: dict,
               accelerator: str) -> reclaim.ReclaimProjectedAdmission:
    kueue_admission = context['kueue_admission']
    admission_mode = (reclaim.ReclaimAdmissionMode.KUBERNETES_SCHEDULER
                      if kueue_admission is None else
                      reclaim.ReclaimAdmissionMode.KUEUE)
    return reclaim.ReclaimProjectedAdmission(
        worker_projection_sha256='a' * 64,
        kubernetes_context=context['kubernetes_context'],
        namespace=context['namespace'],
        service_account_name=context['service_account_name'],
        pod_identity_role_arn=context['pod_identity_role_arn'],
        scheduler_name=context['scheduler_name'],
        priority_class_name=context['priority_class']['name'],
        priority_value=context['priority_class']['value'],
        preemption_policy=context['priority_class']['preemption_policy'],
        admission_mode=admission_mode,
        local_queue_name=(None if kueue_admission is None else
                          kueue_admission['local_queue_name']),
        workload_priority_class_name=(
            None if kueue_admission is None else
            kueue_admission['workload_priority_class_name']),
        accelerator=accelerator,
        accelerator_count=context['accelerators'][accelerator]['count'],
        accelerator_scheduling=reclaim.ReclaimAcceleratorScheduling(
            label_key=(
                context['accelerators'][accelerator]['product_label_key']),
            label_values=tuple(
                sorted(context['accelerators'][accelerator]
                       ['product_label_values'])),
            resource_key=context['accelerators'][accelerator]['resource_name']))


def _edge(context: dict, accelerator: str = 'h200') -> reclaim.ReclaimClaimEdge:
    admission = _admission(context, accelerator)
    pool_key = json.dumps(['v2', context['physical_cluster_uid'], accelerator])
    return reclaim.ReclaimClaimEdge(
        pool_key=pool_key,
        access_context=context['kubernetes_context'],
        physical_cluster_uid=context['physical_cluster_uid'],
        accelerator_names=(accelerator,),
        projected_admissions=(admission,))


def _launch_scope(
    policy: policy_lib.BoltzReservedFillReclaimPolicy,
    context_name: str = 'phx_research_cluster_eks',
    accelerator: str = 'h200',
) -> reclaim.ReclaimLaunchScope:
    context = policy._bundle.fleet_context(context_name)
    return reclaim.ReclaimLaunchScope(
        service_name='service',
        service_version=1,
        pool_key=json.dumps(
            ['v2', context['physical_cluster_uid'], accelerator]),
        service_generation=1,
        physical_cluster_uid=context['physical_cluster_uid'],
        kubernetes_context=context['kubernetes_context'],
        accelerator=accelerator,
        accelerator_count=context['accelerators'][accelerator]['count'],
        projected_admission=_admission(context, accelerator))


def _claim(context: dict,
           accelerator: str,
           *,
           projection_sha256: str = 'a' * 64) -> reclaim.ReservedContextClaim:
    edge = _edge(context, accelerator)
    admission = dataclasses.replace(edge.projected_admissions[0],
                                    worker_projection_sha256=projection_sha256)
    return reclaim.ReservedContextClaim(
        service_name='service',
        service_version=1,
        service_generation=1,
        pool_key=edge.pool_key,
        access_context=edge.access_context,
        physical_cluster_uid=edge.physical_cluster_uid,
        accelerator_names=edge.accelerator_names,
        projected_admissions=(admission,))


def _context_proof(context: dict, provider: dict) -> policy_lib._ContextProof:
    admission = context['kueue_admission']
    return policy_lib._ContextProof(
        aws=aws_attestation.PodIdentityProof(
            kubernetes_context=context['kubernetes_context'],
            cluster_arn=provider['eks']['cluster_arn'],
            namespace=context['namespace'],
            service_account_name=context['service_account_name'],
            expected_role_arn=context['pod_identity_role_arn'],
            association_count=0,
            identity_absence_proven=True),
        kubernetes=kubernetes_attestation.KubernetesContextProof(
            kubernetes_context=context['kubernetes_context'],
            physical_cluster_uid=context['physical_cluster_uid'],
            namespace_uid=provider['namespace_uid'],
            kueue_managed=admission is not None,
            local_queue_name=(admission['local_queue_name']
                              if admission is not None else None),
            cluster_queue_name=(admission['cluster_queue_name']
                                if admission is not None else None),
            pod_identity_irsa_annotation_absent=True,
            assign_queue_labels_for_pods=(True
                                          if admission is not None else None),
            topology_aware_scheduling=(True if admission is not None else None),
            custom_scheduler_deployment_proven=(provider['scheduler']
                                                is not None),
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
                for node in provider['node_inventory'])))


def _context_proof_with_node_counts(
        context: dict, provider: dict,
        counts: dict[str, int]) -> policy_lib._ContextProof:
    proof = _context_proof(context, provider)
    return dataclasses.replace(
        proof,
        kubernetes=dataclasses.replace(
            proof.kubernetes,
            node_flavors=tuple(
                dataclasses.replace(node,
                                    non_deleting_node_count=counts.get(
                                        node.flavor,
                                        node.non_deleting_node_count))
                for node in proof.kubernetes.node_flavors)))


def _set_summary_path(summary: dict, path: tuple[str | int, ...],
                      value: object) -> None:
    target: Any = summary
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value


@pytest.mark.parametrize(
    ('domain', 'path', 'value'),
    (
        ('aws', ('cluster_arn',), 'arn:aws:eks:us-west-2:1:cluster/wrong'),
        ('aws', ('expected_role_arn',), 'arn:aws:iam::123456789012:role/wrong'),
        ('aws', ('identity_absence_proven',), 'yes'),
        ('kubernetes', ('physical_cluster_uid',), 'wrong-cluster'),
        ('kubernetes', ('namespace_uid',), 'wrong-namespace'),
        ('kubernetes', ('local_queue_name',), 'wrong-queue'),
        ('kubernetes', ('custom_scheduler_deployment_proven',), 'yes'),
        ('kubernetes',
         ('resource_flavor_topology_names', 0, 1), 'wrong-topology'),
        ('kubernetes', ('node_flavors', 0, 'non_deleting_node_count'), -1),
        ('kubernetes',
         ('node_flavors', 0, 'product_label_value'), 'wrong-product'),
    ),
)
def test_cached_provider_summary_is_bound_to_exact_reviewed_context(
        domain, path, value):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    context_name = 'phx_research_cluster_eks'
    proof = _context_proof(policy._bundle.fleet_context(context_name),
                           policy._bundle.provider_context(context_name))
    typed = proof.aws if domain == 'aws' else proof.kubernetes
    summary, _ = reclaim_proofs.canonical_proof_payload(
        dataclasses.asdict(typed))

    if domain == 'aws':
        assert policy._decode_aws_proof_summary(
            summary, context_name=context_name) == typed
    else:
        assert policy._decode_kubernetes_proof_summary(
            summary, context_name=context_name) == typed

    _set_summary_path(summary, path, value)
    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='cached .* provider proof'):
        if domain == 'aws':
            policy._decode_aws_proof_summary(summary, context_name=context_name)
        else:
            policy._decode_kubernetes_proof_summary(summary,
                                                    context_name=context_name)


def test_cached_provider_summary_accepts_exact_zero_node_count():
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    context_name = 'phx_research_cluster_eks'
    context = policy._bundle.fleet_context(context_name)
    provider = policy._bundle.provider_context(context_name)
    proof = _context_proof_with_node_counts(context, provider,
                                            {'ml.p5e.48xlarge': 0})
    summary, _ = reclaim_proofs.canonical_proof_payload(
        dataclasses.asdict(proof.kubernetes))

    decoded = policy._decode_kubernetes_proof_summary(summary,
                                                      context_name=context_name)

    assert decoded.node_flavors[0].non_deleting_node_count == 0


def _fake_attest(policy: policy_lib.BoltzReservedFillReclaimPolicy):

    def attest(names, _deadline):
        context_names = tuple(names)
        context_proofs = {
            name: _context_proof(policy._bundle.fleet_context(name),
                                 policy._bundle.provider_context(name))
            for name in context_names
        }
        completed = time.monotonic()
        return context_proofs, {name: completed for name in context_names}

    return attest


def _fake_receipt_read(policy: policy_lib.BoltzReservedFillReclaimPolicy):

    def read(context_name, identity, gate_generation, _deadline):
        completed = time.monotonic()
        return (_context_proof(policy._bundle.fleet_context(context_name),
                               policy._bundle.provider_context(context_name)),
                reclaim.ReclaimProviderProofReference(
                    receipt_nonce='c' * 64,
                    proof_sha256='d' * 64,
                    identity=identity,
                    gate_generation=gate_generation,
                    kubernetes_context=context_name,
                    completed_monotonic=completed))

    return read


def test_two_arbitrary_services_share_one_canonical_claim_path(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    monkeypatch.setattr(policy, '_read_launch_context',
                        _fake_receipt_read(policy))
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    identity = policy.policy_identity()

    for service_name in ('first-service', 'second-service-with-weight-1000'):
        scope = reclaim.ReclaimClaimSetScope(
            service_name=service_name,
            service_incarnation=f'incarnation-{service_name}',
            service_version=1,
            semantic_hash=f'semantic-{service_name}',
            edges=(_edge(context),))
        authorization = policy.authorize_claim_set(
            scope,
            expected_identity=identity,
            expected_gate_generation=7,
            deadline_monotonic=time.monotonic() + 5)
        assert authorization.scope == scope
        assert authorization.identity == identity


def test_zero_capacity_context_does_not_block_positive_claim_peer(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    identity = policy.policy_identity()
    east_name = 'prod_research_cluster_eks'
    phx_name = 'phx_research_cluster_eks'
    east = policy._bundle.fleet_context(east_name)
    phx = policy._bundle.fleet_context(phx_name)
    proofs = {
        east_name: _context_proof_with_node_counts(
            east, policy._bundle.provider_context(east_name), {
                'ml.p4d.24xlarge': 0,
                'ml.p4de.24xlarge': 0,
            }),
        phx_name: _context_proof(phx,
                                 policy._bundle.provider_context(phx_name)),
    }

    def read(context_name, expected_identity, gate_generation, _deadline):
        return proofs[context_name], reclaim.ReclaimProviderProofReference(
            receipt_nonce=hashlib.sha256(context_name.encode()).hexdigest(),
            proof_sha256='d' * 64,
            identity=expected_identity,
            gate_generation=gate_generation,
            kubernetes_context=context_name,
            completed_monotonic=time.monotonic())

    receipt_read = mock.Mock(side_effect=read)
    monkeypatch.setattr(policy, '_read_launch_context', receipt_read)
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    scope = reclaim.ReclaimClaimSetScope(
        service_name='mixed-capacity-service',
        service_incarnation='mixed-capacity-incarnation',
        service_version=1,
        semantic_hash='mixed-capacity-semantic',
        edges=tuple(sorted((_edge(east, 'a100'), _edge(phx, 'h200')))))

    authorization = policy.authorize_claim_set(
        scope,
        expected_identity=identity,
        expected_gate_generation=7,
        deadline_monotonic=time.monotonic() + 5)

    assert authorization.scope == scope
    assert {call.args[0] for call in receipt_read.call_args_list
           } == {east_name, phx_name}


def test_claim_ticket_is_minted_after_proof_logging(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    clock = [100.0]
    monkeypatch.setattr(policy_lib.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(policy, '_read_launch_context',
                        _fake_receipt_read(policy))

    def _slow_log(_payload):
        clock[0] += reclaim.AUTHORIZATION_MAX_AGE_SECONDS + 1

    monkeypatch.setattr(policy, '_emit_proof', _slow_log)
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    scope = reclaim.ReclaimClaimSetScope(service_name='svc',
                                         service_incarnation='incarnation',
                                         service_version=1,
                                         semantic_hash='semantic',
                                         edges=(_edge(context),))

    authorization = policy.authorize_claim_set(
        scope,
        expected_identity=policy.policy_identity(),
        expected_gate_generation=7,
        deadline_monotonic=clock[0] + 10)

    assert authorization.completed_monotonic == clock[0] == 106.0


def test_unchanged_policy_authorizes_current_service_version_refresh(
        monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    monkeypatch.setattr(policy, '_read_launch_context',
                        _fake_receipt_read(policy))
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    identity = policy.policy_identity()
    assert identity.policy_revision == (
        'boltz-reserved-fill-reclaim-policy/1.1.1578')

    authorizations = []
    for service_version in (63, 64):
        scope = reclaim.ReclaimClaimSetScope(
            service_name='boltz-l4-fleet',
            service_incarnation='current-incarnation',
            service_version=service_version,
            semantic_hash=f'semantic-v{service_version}',
            edges=(_edge(context),))
        authorizations.append(
            policy.authorize_claim_set(scope,
                                       expected_identity=identity,
                                       expected_gate_generation=1,
                                       deadline_monotonic=time.monotonic() + 5))

    assert [
        authorization.scope.service_version for authorization in authorizations
    ] == [63, 64]
    assert all(
        authorization.identity == identity for authorization in authorizations)
    assert all(
        authorization.gate_generation == 1 for authorization in authorizations)


def test_unmanaged_context_cannot_claim_with_forged_admission(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
    context = policy._bundle.fleet_context('prod_research_cluster_eks')
    accelerator = context['accelerators']['a100']
    admission = reclaim.ReclaimProjectedAdmission(
        worker_projection_sha256='a' * 64,
        kubernetes_context=context['kubernetes_context'],
        namespace=context['namespace'],
        service_account_name=context['service_account_name'],
        pod_identity_role_arn=context['pod_identity_role_arn'],
        scheduler_name=context['scheduler_name'],
        priority_class_name=context['priority_class']['name'],
        priority_value=context['priority_class']['value'],
        preemption_policy=context['priority_class']['preemption_policy'],
        admission_mode=reclaim.ReclaimAdmissionMode.KUEUE,
        local_queue_name='forged',
        workload_priority_class_name='forged',
        accelerator='a100',
        accelerator_count=accelerator['count'],
        accelerator_scheduling=reclaim.ReclaimAcceleratorScheduling(
            label_key=accelerator['product_label_key'],
            label_values=tuple(accelerator['product_label_values']),
            resource_key=accelerator['resource_name']))
    pool_key = json.dumps(['v2', context['physical_cluster_uid'], 'a100'])
    edge = reclaim.ReclaimClaimEdge(
        pool_key=pool_key,
        access_context=context['kubernetes_context'],
        physical_cluster_uid=context['physical_cluster_uid'],
        accelerator_names=('a100',),
        projected_admissions=(admission,))
    scope = reclaim.ReclaimClaimSetScope(
        service_name='east-unmanaged',
        service_incarnation='incarnation-east-unmanaged',
        service_version=1,
        semantic_hash='semantic-east-unmanaged',
        edges=(edge,))

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='reviewed fleet bundle'):
        policy.authorize_claim_set(scope,
                                   expected_identity=policy.policy_identity(),
                                   expected_gate_generation=7,
                                   deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


def test_unmanaged_context_accepts_exact_scheduler_admission(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    monkeypatch.setattr(policy, '_read_launch_context',
                        _fake_receipt_read(policy))
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    context = policy._bundle.fleet_context('prod_research_cluster_eks')
    edge = _edge(context, 'a100')
    scope = reclaim.ReclaimClaimSetScope(
        service_name='east-scheduler',
        service_incarnation='incarnation-east-scheduler',
        service_version=1,
        semantic_hash='semantic-east-scheduler',
        edges=(edge,))

    authorization = policy.authorize_claim_set(
        scope,
        expected_identity=policy.policy_identity(),
        expected_gate_generation=7,
        deadline_monotonic=time.monotonic() + 5)

    assert authorization.scope == scope
    assert edge.projected_admissions[0].admission_mode is (
        reclaim.ReclaimAdmissionMode.KUBERNETES_SCHEDULER)


def test_claim_set_rejects_duplicate_physical_card_atom(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    first = _edge(context, 'h200')
    second_admission = dataclasses.replace(first.projected_admissions[0],
                                           worker_projection_sha256='b' * 64)
    second = dataclasses.replace(first,
                                 projected_admissions=(second_admission,))
    scope = reclaim.ReclaimClaimSetScope(
        service_name='duplicate-phx-card',
        service_incarnation='incarnation-duplicate-phx-card',
        service_version=1,
        semantic_hash='semantic-duplicate-east-card',
        edges=tuple(sorted((first, second))))

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='same physical accelerator pool twice'):
        policy.authorize_claim_set(scope,
                                   expected_identity=policy.policy_identity(),
                                   expected_gate_generation=7,
                                   deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


def test_static_admission_mismatch_makes_no_provider_calls(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    admission = dataclasses.replace(_admission(context, 'h200'),
                                    local_queue_name='wrong')
    edge = dataclasses.replace(_edge(context),
                               projected_admissions=(admission,))
    scope = reclaim.ReclaimClaimSetScope(service_name='service',
                                         service_incarnation='incarnation',
                                         service_version=1,
                                         semantic_hash='semantic',
                                         edges=(edge,))

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='reviewed fleet bundle'):
        policy.authorize_claim_set(scope,
                                   expected_identity=policy.policy_identity(),
                                   expected_gate_generation=1,
                                   deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


@pytest.mark.parametrize(('field', 'value'), ((
    'label_key',
    'node.kubernetes.io/instance-type',
), ('label_values',
    ('NVIDIA-A100-SXM4-80GB',)), ('resource_key', 'example.com/gpu')))
def test_accelerator_scheduling_mismatch_makes_no_provider_calls(
        monkeypatch, field, value):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    admission = _admission(context, 'h200')
    scheduling = dataclasses.replace(admission.accelerator_scheduling,
                                     **{field: value})
    admission = dataclasses.replace(admission,
                                    accelerator_scheduling=scheduling)
    edge = dataclasses.replace(_edge(context),
                               projected_admissions=(admission,))
    scope = reclaim.ReclaimClaimSetScope(service_name='service',
                                         service_incarnation='incarnation',
                                         service_version=1,
                                         semantic_hash='semantic',
                                         edges=(edge,))

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='reviewed fleet bundle'):
        policy.authorize_claim_set(scope,
                                   expected_identity=policy.policy_identity(),
                                   expected_gate_generation=1,
                                   deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


def test_activation_rejects_accelerator_scheduling_mismatch(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    claim = _claim(context, 'h200')
    admission = dataclasses.replace(
        claim.projected_admissions[0],
        accelerator_scheduling=reclaim.ReclaimAcceleratorScheduling(
            label_key='nvidia.com/gpu.product',
            label_values=('NVIDIA-H100',),
            resource_key='nvidia.com/gpu'))
    claim = dataclasses.replace(claim, projected_admissions=(admission,))

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='reviewed fleet bundle'):
        policy.attest_activation((claim,),
                                 writer_image_digest='sha256:' + 'b' * 64,
                                 deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


def test_launch_rejects_accelerator_scheduling_mismatch(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_launch_context', provider_calls)
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    admission = dataclasses.replace(
        _admission(context, 'h200'),
        accelerator_scheduling=reclaim.ReclaimAcceleratorScheduling(
            label_key='nvidia.com/gpu.product',
            label_values=('NVIDIA-H100',),
            resource_key='nvidia.com/gpu'))
    scope = reclaim.ReclaimLaunchScope(
        service_name='service',
        service_version=1,
        pool_key=json.dumps(['v2', context['physical_cluster_uid'], 'h200']),
        service_generation=1,
        physical_cluster_uid=context['physical_cluster_uid'],
        kubernetes_context=context['kubernetes_context'],
        accelerator='h200',
        accelerator_count=1,
        projected_admission=admission)

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='reviewed fleet bundle'):
        policy.authorize_launch(scope,
                                expected_identity=policy.policy_identity(),
                                expected_gate_generation=1,
                                deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


def test_launch_requires_positive_capacity_for_target_flavor(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    context_name = 'prod_research_cluster_eks'
    context = policy._bundle.fleet_context(context_name)
    provider = policy._bundle.provider_context(context_name)
    proof = _context_proof_with_node_counts(context, provider, {
        'ml.p4d.24xlarge': 0,
        'ml.p4de.24xlarge': 1,
    })
    identity = policy.policy_identity()
    reference = reclaim.ReclaimProviderProofReference(
        receipt_nonce='c' * 64,
        proof_sha256='d' * 64,
        identity=identity,
        gate_generation=1,
        kubernetes_context=context_name,
        completed_monotonic=time.monotonic())
    receipt_read = mock.Mock(return_value=(proof, reference))
    emit_proof = mock.Mock()
    monkeypatch.setattr(policy, '_attest_launch_context', receipt_read)
    monkeypatch.setattr(policy, '_emit_proof', emit_proof)

    with pytest.raises(reclaim.ReclaimProviderProofUnavailableError,
                       match="accelerator 'a100'"):
        policy.authorize_launch(_launch_scope(policy, context_name, 'a100'),
                                expected_identity=identity,
                                expected_gate_generation=1,
                                deadline_monotonic=time.monotonic() + 5)

    emit_proof.assert_not_called()
    authorization = policy.authorize_launch(
        _launch_scope(policy, context_name, 'a100-80gb'),
        expected_identity=identity,
        expected_gate_generation=1,
        deadline_monotonic=time.monotonic() + 5)

    assert authorization.scope.accelerator == 'a100-80gb'
    assert authorization.provider_proof_reference == reference
    emit_proof.assert_called_once()


@pytest.mark.parametrize('deadline', (True, float('nan'), float('inf')))
def test_launch_malformed_deadline_is_permanent(monkeypatch, deadline):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    receipt_read = mock.Mock()
    monkeypatch.setattr(policy, '_attest_launch_context', receipt_read)

    with pytest.raises(reclaim.ReclaimAttestationError) as error:
        policy.authorize_launch(_launch_scope(policy),
                                expected_identity=policy.policy_identity(),
                                expected_gate_generation=1,
                                deadline_monotonic=deadline)

    assert not isinstance(error.value,
                          reclaim.ReclaimProviderProofUnavailableError)
    receipt_read.assert_not_called()


def test_launch_expired_finite_deadline_is_transient(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    receipt_read = mock.Mock()
    monkeypatch.setattr(policy, '_attest_launch_context', receipt_read)

    with pytest.raises(reclaim.ReclaimProviderProofUnavailableError):
        policy.authorize_launch(_launch_scope(policy),
                                expected_identity=policy.policy_identity(),
                                expected_gate_generation=1,
                                deadline_monotonic=time.monotonic() - 1)

    receipt_read.assert_not_called()


@pytest.mark.parametrize(
    ('repository_error', 'transient'),
    ((reclaim_proofs.ReclaimProviderProofUnavailableError('temporarily down'),
      True),
     (reclaim_proofs.ReclaimProviderProofError('malformed row'), False)))
def test_launch_translates_typed_repository_errors(monkeypatch,
                                                   repository_error, transient):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = mock.Mock()
    repository.get_fresh.side_effect = repository_error
    monkeypatch.setattr(reclaim_proofs, 'ReclaimProviderProofRepository',
                        mock.Mock(return_value=repository))

    with pytest.raises(reclaim.ReclaimAttestationError) as error:
        policy._read_launch_context('phx_research_cluster_eks',
                                    policy.policy_identity(), 1,
                                    time.monotonic() + 5)

    assert isinstance(error.value,
                      reclaim.ReclaimProviderProofUnavailableError) is transient
    assert error.value.__cause__ is repository_error


def test_launch_receipt_read_deadline_exhaustion_is_transient(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    repository = mock.Mock()
    repository.get_fresh.return_value = object()
    monkeypatch.setattr(reclaim_proofs, 'ReclaimProviderProofRepository',
                        mock.Mock(return_value=repository))
    monotonic = mock.Mock(side_effect=(100.0, 102.0))
    monkeypatch.setattr(policy_lib.time, 'monotonic', monotonic)

    with pytest.raises(reclaim.ReclaimProviderProofUnavailableError):
        policy._read_launch_context('phx_research_cluster_eks',
                                    policy.policy_identity(), 1, 101.0)

    repository.get_fresh.assert_called_once()


def test_launch_post_decode_deadline_exhaustion_is_transient(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    scope = _launch_scope(policy)
    context = policy._bundle.fleet_context(scope.kubernetes_context)
    provider = policy._bundle.provider_context(scope.kubernetes_context)
    reference = reclaim.ReclaimProviderProofReference(
        receipt_nonce='c' * 64,
        proof_sha256='d' * 64,
        identity=policy.policy_identity(),
        gate_generation=1,
        kubernetes_context=scope.kubernetes_context,
        completed_monotonic=99.0)
    receipt_read = mock.Mock(return_value=(_context_proof(context, provider),
                                           reference))
    monkeypatch.setattr(policy, '_attest_launch_context', receipt_read)
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    monkeypatch.setattr(policy_lib.time, 'monotonic',
                        mock.Mock(side_effect=(100.0, 100.5, 102.0)))

    with pytest.raises(reclaim.ReclaimProviderProofUnavailableError):
        policy.authorize_launch(scope,
                                expected_identity=policy.policy_identity(),
                                expected_gate_generation=1,
                                deadline_monotonic=101.0)

    receipt_read.assert_called_once()


def test_bundle_rejects_overlapping_exact_card_contracts():
    document = _bundle_document()
    accelerators = document['fleet']['contexts'][0]['accelerators']
    accelerators['a100']['flavors'] = ['ml.p4de.24xlarge']
    accelerators['a100']['product_label_values'] = ['NVIDIA-A100-SXM4-80GB']

    with pytest.raises(bundle_lib.BundleValidationError,
                       match='overlaps the exact card contract'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))


def test_activation_attests_whole_fleet_with_zero_current_claims(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    attest = mock.Mock(side_effect=_fake_attest(policy))
    monkeypatch.setattr(policy, '_attest_contexts', attest)
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())

    evidence = policy.attest_activation(
        (),
        writer_image_digest='sha256:' + 'b' * 64,
        deadline_monotonic=time.monotonic() + 5)

    assert not evidence.claimed_contexts
    assert evidence.identity == policy.policy_identity()
    assert attest.call_args.args[0] == policy._bundle.contexts


def test_activation_accepts_zero_capacity_context_and_positive_peer(
        monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    east_name = 'prod_research_cluster_eks'
    phx_name = 'phx_research_cluster_eks'
    east = policy._bundle.fleet_context(east_name)
    phx = policy._bundle.fleet_context(phx_name)
    proofs = {
        east_name: _context_proof_with_node_counts(
            east, policy._bundle.provider_context(east_name), {
                'ml.p4d.24xlarge': 0,
                'ml.p4de.24xlarge': 0,
            }),
        phx_name: _context_proof(phx,
                                 policy._bundle.provider_context(phx_name)),
    }
    attest = mock.Mock(return_value=(
        proofs, {context_name: time.monotonic() for context_name in proofs}))
    emit_proof = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', attest)
    monkeypatch.setattr(policy, '_emit_proof', emit_proof)
    claims = (_claim(phx, 'h200'),)

    evidence = policy.attest_activation(claims,
                                        writer_image_digest='sha256:' +
                                        'b' * 64,
                                        deadline_monotonic=time.monotonic() + 5)

    assert evidence.claimed_contexts == claims
    assert attest.call_args.args[0] == policy._bundle.contexts
    payload_by_context = {
        context['kubernetes_context']: context
        for context in emit_proof.call_args.args[0]['contexts']
    }
    assert {
        node['non_deleting_node_count']
        for node in payload_by_context[east_name]['kubernetes']['node_flavors']
    } == {0}
    assert all(
        node['non_deleting_node_count'] > 0
        for node in payload_by_context[phx_name]['kubernetes']['node_flavors'])


def test_activation_accepts_managed_phx_claim(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    attest = mock.Mock(side_effect=_fake_attest(policy))
    monkeypatch.setattr(policy, '_attest_contexts', attest)
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    claims = (_claim(context, 'h200'),)

    evidence = policy.attest_activation(claims,
                                        writer_image_digest='sha256:' +
                                        'b' * 64,
                                        deadline_monotonic=time.monotonic() + 5)

    assert evidence.claimed_contexts == claims
    assert attest.call_args.args[0] == policy._bundle.contexts


def test_activation_rejects_duplicate_physical_card_atom(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    claims = tuple(
        sorted((_claim(context, 'h200'),
                _claim(context, 'h200', projection_sha256='b' * 64))))

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='same physical accelerator pool twice'):
        policy.attest_activation(claims,
                                 writer_image_digest='sha256:' + 'b' * 64,
                                 deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


def test_activation_accepts_shared_physical_card_across_services(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    attest = mock.Mock(side_effect=_fake_attest(policy))
    monkeypatch.setattr(policy, '_attest_contexts', attest)
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    context = policy._bundle.fleet_context('phx_research_cluster_eks')
    first = dataclasses.replace(_claim(context, 'h200'),
                                service_name='service-a')
    second = dataclasses.replace(_claim(context, 'h200'),
                                 service_name='service-b')
    claims = tuple(sorted((first, second)))

    evidence = policy.attest_activation(claims,
                                        writer_image_digest='sha256:' +
                                        'b' * 64,
                                        deadline_monotonic=time.monotonic() + 5)

    assert evidence.claimed_contexts == claims
    assert attest.call_args.args[0] == policy._bundle.contexts


def test_provider_domains_and_contexts_start_concurrently(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    barrier = threading.Barrier(4)

    def provider_job(context_name, domain, _deadline, _cancellation):
        barrier.wait(timeout=1)
        context = policy._bundle.fleet_context(context_name)
        provider = policy._bundle.provider_context(context_name)
        proof = _context_proof(context, provider)
        return proof.aws if domain == 'aws' else proof.kubernetes

    monkeypatch.setattr(policy, '_provider_job', provider_job)
    proofs, oldest_completions = policy._attest_contexts(
        policy._bundle.contexts,
        time.monotonic() + 2)

    assert set(proofs) == set(policy._bundle.contexts)
    assert set(oldest_completions) == set(policy._bundle.contexts)


def test_completed_negative_wins_over_indeterminate_peer(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    context_name = 'phx_research_cluster_eks'
    peer_started = threading.Event()
    release_peer = threading.Event()

    def provider_job(_context_name, domain, _deadline, _cancellation):
        if domain == 'aws':
            peer_started.set()
            # Model synchronous DNS/libpq-style code that ignores both its
            # deadline and cooperative cancellation. The enclosing disposable
            # renewal boundary owns termination after durable invalidation.
            assert release_peer.wait(timeout=5)
            raise RuntimeError('late indeterminate AWS transport')
        assert peer_started.wait(timeout=1)
        raise kubernetes_attestation.KubernetesAttestationNonconformanceError(
            'complete Kubernetes mismatch')

    monkeypatch.setattr(policy, '_provider_job', provider_job)
    started = time.monotonic()
    try:
        with pytest.raises(reclaim.ReclaimProviderNonconformanceError,
                           match='nonconforming'):
            policy._attest_contexts((context_name,), time.monotonic() + 5)
        assert time.monotonic() - started < 1
    finally:
        release_peer.set()
    assert _wait_for_no_thread('boltz-reclaim-attest')


def test_renewal_context_negative_wins_over_never_returning_peer(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    contexts = policy._bundle.contexts
    assert len(contexts) == 2
    peer_started = threading.Event()
    release_peer = threading.Event()

    class _Repository:

        def renew(self, *, kubernetes_context, **_kwargs):
            if kubernetes_context == contexts[0]:
                peer_started.set()
                assert release_peer.wait(timeout=5)
                raise RuntimeError('late indeterminate context transport')
            assert peer_started.wait(timeout=1)
            raise reclaim.ReclaimProviderNonconformanceError(
                'committed exact context invalidation')

    monkeypatch.setattr(reclaim_proofs, 'ReclaimProviderProofRepository',
                        _Repository)
    started = time.monotonic()
    try:
        with pytest.raises(reclaim.ReclaimProviderNonconformanceError,
                           match='committed exact context invalidation'):
            policy.renew_provider_proofs(
                expected_identity=policy.policy_identity(),
                expected_gate_generation=7,
                deadline_monotonic=time.monotonic() + 5)
        assert time.monotonic() - started < 1
    finally:
        release_peer.set()
    assert _wait_for_no_thread('boltz-reclaim-renew')


def test_renewal_accepts_zero_capacity_context_and_positive_peer(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    east_name = 'prod_research_cluster_eks'
    published = {}

    def attest(names, _deadline):
        context_name, = tuple(names)
        context = policy._bundle.fleet_context(context_name)
        provider = policy._bundle.provider_context(context_name)
        if context_name == east_name:
            proof = _context_proof_with_node_counts(context, provider, {
                'ml.p4d.24xlarge': 0,
                'ml.p4de.24xlarge': 0,
            })
        else:
            proof = _context_proof(context, provider)
        completed = time.monotonic()
        return {context_name: proof}, {context_name: completed}

    class _Repository:
        """Exercise the production proof and validation callbacks."""

        @staticmethod
        def renew(**kwargs):
            candidate = kwargs['prove']()
            payload, _ = reclaim_proofs.canonical_proof_payload(
                candidate.proof_payload)
            assert kwargs['validate'](payload)
            published[kwargs['kubernetes_context']] = payload
            return types.SimpleNamespace(proof_payload=payload,
                                         publication_observed=False)

    monkeypatch.setattr(policy, '_attest_contexts', attest)
    monkeypatch.setattr(reclaim_proofs, 'ReclaimProviderProofRepository',
                        _Repository)

    assert not policy.renew_provider_proofs(
        expected_identity=policy.policy_identity(),
        expected_gate_generation=7,
        deadline_monotonic=time.monotonic() + 5)

    assert set(published) == set(policy._bundle.contexts)
    east_nodes = published[east_name]['kubernetes']['node_flavors']
    assert {node['non_deleting_node_count'] for node in east_nodes} == {0}


def test_renewal_uses_distinct_refresh_and_handoff_reserves(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    calls = []

    class _Repository:
        """Capture one proof renewal call per physical context."""

        def renew(self, **kwargs):
            calls.append(kwargs)
            context_name = kwargs['kubernetes_context']
            context = policy._bundle.fleet_context(context_name)
            provider = policy._bundle.provider_context(context_name)
            proof = _context_proof(context, provider)
            payload, _ = reclaim_proofs.canonical_proof_payload({
                'aws': dataclasses.asdict(proof.aws),
                'kubernetes': dataclasses.asdict(proof.kubernetes),
            })
            return types.SimpleNamespace(proof_payload=payload,
                                         publication_observed=False)

    monkeypatch.setattr(reclaim_proofs, 'ReclaimProviderProofRepository',
                        _Repository)

    assert not policy.renew_provider_proofs(
        expected_identity=policy.policy_identity(),
        expected_gate_generation=7,
        deadline_monotonic=time.monotonic() + 5)

    assert {call['kubernetes_context'] for call in calls
           } == set(policy._bundle.contexts)
    assert all(call['minimum_remaining_seconds'] ==
               reclaim.PROVIDER_PROOF_RENEW_MIN_REMAINING_SECONDS
               for call in calls)


def test_kubernetes_provider_uses_the_exact_assumed_audit_session(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    context_name = 'phx_research_cluster_eks'
    provider = policy._bundle.provider_context(context_name)
    audit_session = object()
    session_call = mock.Mock(return_value=audit_session)
    monkeypatch.setattr(policy._aws_sessions, 'session', session_call)
    expected_proof = object()
    attest = mock.Mock(return_value=expected_proof)
    monkeypatch.setattr(kubernetes_attestation, 'attest_context', attest)
    deadline = time.monotonic() + 5
    cancellation = threading.Event()

    result = policy._provider_job(context_name, 'kubernetes', deadline,
                                  cancellation)

    assert result is expected_proof
    session_call.assert_called_once_with(provider['eks']['audit_role_arn'],
                                         provider['eks']['region'], deadline,
                                         cancellation)
    attest.assert_called_once_with(policy._bundle.fleet_context(context_name),
                                   provider,
                                   deadline_monotonic=deadline,
                                   cancellation=cancellation,
                                   audit_session=audit_session)


@pytest.mark.parametrize('already_frozen', [False, True])
def test_eks_bearer_token_is_signed_by_the_supplied_audit_session(
        monkeypatch, already_frozen):
    frozen_credentials = types.SimpleNamespace(access_key='access',
                                               secret_key='secret',
                                               token='token')
    wrapper_call = {}

    class WrappedCredentials:
        """Captures reconstruction of an immutable signing provider."""

        def __init__(self, **kwargs):
            wrapper_call['kwargs'] = kwargs
            wrapper_call['instance'] = self

        @staticmethod
        def get_frozen_credentials():
            return frozen_credentials

    class Credentials:
        """Minimal frozen-credential provider for the signer test."""

        @staticmethod
        def get_frozen_credentials():
            return frozen_credentials

    sts = types.SimpleNamespace(meta=types.SimpleNamespace(
        service_model=types.SimpleNamespace(service_id='STS'),
        events=object(),
        endpoint_url='https://sts.us-west-2.amazonaws.com',
    ))

    class AuditSession:
        """Minimal exact-session seam for the signer test."""

        @staticmethod
        def client(service, **kwargs):
            assert service == 'sts'
            assert kwargs['region_name'] == 'us-west-2'
            return sts

        @staticmethod
        def get_credentials():
            return frozen_credentials if already_frozen else Credentials()

    signer_call = {}

    class Signer:
        """Captures the token-signing request without provider access."""

        def __init__(self, *args):
            signer_call['args'] = args

        @staticmethod
        def generate_presigned_url(request, **kwargs):
            signer_call['request'] = request
            signer_call['kwargs'] = kwargs
            return 'https://signed.example/token'

    monkeypatch.setattr(kubernetes_attestation.botocore_signers,
                        'RequestSigner', Signer)
    monkeypatch.setattr(kubernetes_attestation, 'botocore_credentials',
                        types.SimpleNamespace(Credentials=WrappedCredentials))

    token = kubernetes_attestation._eks_bearer_token(
        AuditSession(),
        region='us-west-2',
        cluster_name='exact-cluster',
        deadline_monotonic=time.monotonic() + 5,
        cancellation=threading.Event())

    if already_frozen:
        wrapped = signer_call['args'][4]
        assert wrapped is wrapper_call['instance']
        assert wrapped.get_frozen_credentials() is frozen_credentials
        assert wrapper_call['kwargs'] == {
            'access_key': 'access',
            'secret_key': 'secret',
            'token': 'token',
        }
    else:
        assert isinstance(signer_call['args'][4], Credentials)
        assert not wrapper_call
    assert signer_call['request']['headers'] == {
        'x-k8s-aws-id': 'exact-cluster'
    }
    assert signer_call['kwargs'] == {
        'region_name': 'us-west-2',
        'expires_in': 60,
        'operation_name': '',
    }
    encoded = token.removeprefix('k8s-aws-v1.')
    encoded += '=' * (-len(encoded) % 4)
    assert base64.urlsafe_b64decode(encoded).decode() == (
        'https://signed.example/token')


def test_audit_api_client_uses_exact_eks_connection_and_scrubs_credentials(
        monkeypatch):
    bundle = bundle_lib.load_embedded_bundle()
    provider = bundle.provider_context('phx_research_cluster_eks')
    contract = provider['eks']

    class Eks:
        """Returns the exact reviewed cluster connection."""

        @staticmethod
        def describe_cluster(name):
            assert name == contract['cluster_name']
            return {
                'cluster': {
                    'name': name,
                    'arn': contract['cluster_arn'],
                    'status': 'ACTIVE',
                    'endpoint': 'https://exact.eks.example',
                    'certificateAuthority': {
                        'data': 'Y2E=',
                    },
                }
            }

    audit_session = mock.Mock()
    audit_session.client.return_value = Eks()
    token_call = mock.Mock(return_value='k8s-aws-v1.exact')
    monkeypatch.setattr(kubernetes_attestation, '_eks_bearer_token', token_call)

    captured = {}

    class Configuration:
        """Minimal isolated Kubernetes client configuration."""

        def __init__(self):
            self.api_key = {}
            self.api_key_prefix = {}
            self.refresh_api_key_hook = object()

    class Loader:
        """Captures and materializes the in-memory kubeconfig."""

        def __init__(self, *, config_dict, active_context):
            captured['document'] = config_dict
            captured['active_context'] = active_context

        @staticmethod
        def load_and_set(configuration):
            configuration.api_key['authorization'] = 'secret-token'
            configuration.api_key_prefix['authorization'] = 'Bearer'

    class ApiClient:
        """Records deterministic client retirement."""

        def __init__(self, *, configuration):
            self.configuration = configuration
            self.closed = False

        def close(self):
            self.closed = True

    fake_kubernetes = types.SimpleNamespace(
        client=types.SimpleNamespace(Configuration=Configuration,
                                     ApiClient=ApiClient),
        config=types.SimpleNamespace(kube_config=types.SimpleNamespace(
            KubeConfigLoader=Loader)),
    )
    monkeypatch.setattr(kubernetes_attestation.kubernetes_adaptor, 'kubernetes',
                        fake_kubernetes)

    with kubernetes_attestation._audit_api_client(
            provider,
            audit_session,
            deadline_monotonic=time.monotonic() + 5,
            cancellation=threading.Event()) as client:
        assert not client.closed
        assert captured['document']['clusters'][0]['cluster'] == {
            'server': 'https://exact.eks.example',
            'certificate-authority-data': 'Y2E=',
        }
        assert captured['document']['users'][0]['user'] == {}
        assert client.configuration.api_key == {'authorization': 'secret-token'}
        assert client.configuration.retries == 0

    assert client.closed
    assert client.configuration.api_key == {}
    assert client.configuration.api_key_prefix == {}
    token_call.assert_called_once_with(audit_session,
                                       region=contract['region'],
                                       cluster_name=contract['cluster_name'],
                                       deadline_monotonic=mock.ANY,
                                       cancellation=mock.ANY)


def test_positive_identity_absence_proof_is_explicit():
    aws_attestation.validate_pod_identity_inventory([],
                                                    described_association=None,
                                                    cluster_name='cluster',
                                                    namespace='inference',
                                                    service_account='worker',
                                                    expected_role_arn=None)
    with pytest.raises(aws_attestation.AwsAttestationError,
                       match='identity-free'):
        aws_attestation.validate_pod_identity_inventory(
            [{
                'associationId': 'unexpected'
            }],
            described_association=None,
            cluster_name='cluster',
            namespace='inference',
            service_account='worker',
            expected_role_arn=None)


def test_positive_identity_requires_exact_summary_description_and_owner():
    role = 'arn:aws:iam::123456789012:role/worker'
    summary = {
        'associationId': 'a-123',
        'associationArn': 'arn:association',
        'clusterName': 'cluster',
        'namespace': 'inference',
        'serviceAccount': 'worker',
        'ownerArn': None,
    }
    described = {
        **summary,
        'roleArn': role,
        'targetRoleArn': None,
    }
    aws_attestation.validate_pod_identity_inventory(
        [summary],
        described_association=described,
        cluster_name='cluster',
        namespace='inference',
        service_account='worker',
        expected_role_arn=role)
    described['roleArn'] = 'arn:aws:iam::123456789012:role/wrong'
    with pytest.raises(aws_attestation.AwsAttestationError, match='unexpected'):
        aws_attestation.validate_pod_identity_inventory(
            [summary],
            described_association=described,
            cluster_name='cluster',
            namespace='inference',
            service_account='worker',
            expected_role_arn=role)


def test_aws_pagination_rejects_token_cycles():

    class Eks:

        def list_pod_identity_associations(self, **kwargs):
            del kwargs
            return {'associations': [], 'nextToken': 'cycle'}

    class Session:

        @staticmethod
        def client(*_args, **_kwargs):
            return Eks()

    with pytest.raises(aws_attestation.AwsAttestationError, match='pagination'):
        aws_attestation._list_associations(Session(),
                                           region='us-east-1',
                                           cluster_name='cluster',
                                           namespace='inference',
                                           service_account='worker',
                                           deadline_monotonic=time.monotonic() +
                                           5,
                                           cancellation=threading.Event())


def test_aws_pagination_recomputes_client_timeout_per_page(monkeypatch):
    pages = iter(({
        'associations': [],
        'nextToken': 'next',
    }, {
        'associations': [],
    }))
    configured_timeouts = []
    timeouts = iter((0.9, 0.4))
    monkeypatch.setattr(aws_attestation, '_client_timeout',
                        lambda *_args: next(timeouts))

    class Eks:

        @staticmethod
        def list_pod_identity_associations(**_kwargs):
            return next(pages)

    class Session:

        @staticmethod
        def client(_service, *, region_name, config):
            assert region_name == 'us-east-1'
            configured_timeouts.append(
                (config.connect_timeout, config.read_timeout))
            return Eks()

    associations = aws_attestation._list_associations(
        Session(),
        region='us-east-1',
        cluster_name='cluster',
        namespace='inference',
        service_account='worker',
        deadline_monotonic=time.monotonic() + 5,
        cancellation=threading.Event())

    assert not associations
    assert configured_timeouts == [(0.9, 0.9), (0.4, 0.4)]


def test_audit_session_cache_coalesces_concurrent_assume_role():
    expiration = datetime.datetime.now(
        datetime.timezone.utc) + datetime.timedelta(minutes=15)
    assume_calls = 0
    assume_lock = threading.Lock()

    class Sts:

        def assume_role(self, **kwargs):
            nonlocal assume_calls
            del kwargs
            with assume_lock:
                assume_calls += 1
            return {
                'Credentials': {
                    'AccessKeyId': 'key',
                    'SecretAccessKey': 'secret',
                    'SessionToken': 'token',
                    'Expiration': expiration,
                }
            }

    class Ambient:

        def client(self, *_args, **_kwargs):
            return Sts()

    cache = aws_attestation.AuditSessionCache(
        ambient_session_factory=lambda **_kwargs: Ambient(),
        assumed_session_factory=lambda **kwargs: kwargs)
    barrier = threading.Barrier(4)
    sessions = []

    def worker():
        barrier.wait(timeout=1)
        sessions.append(
            cache.session('arn:aws:iam::123456789012:role/audit', 'us-east-1',
                          time.monotonic() + 5, threading.Event()))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert assume_calls == 1
    assert len(sessions) == 4


def _active_object(name: str, spec: dict, namespace: str | None = None) -> dict:
    metadata = {'name': name, 'generation': 3}
    if namespace is not None:
        metadata['namespace'] = namespace
    return {
        'metadata': metadata,
        'spec': spec,
        'status': {
            'conditions': [{
                'type': 'Active',
                'status': 'True',
                'observedGeneration': 3,
            }]
        },
    }


def _queue_object(context: dict) -> dict:
    admission = context['kueue_admission']
    assert admission is not None
    spec = {
        'namespaceSelector': {
            'matchLabels': {
                'kubernetes.io/metadata.name': context['namespace'],
            }
        },
        'stopPolicy': 'None',
        # SkyPilot deliberately ignores platform-owned scheduling policy.
        'cohortName': 'platform-owned',
        'preemption': {
            'platform': 'owned'
        },
        'resourceGroups': [{
            'platform': 'owned'
        }],
    }
    return _active_object(admission['cluster_queue_name'], spec)


def _deployment(contract: dict, image_key: str) -> dict:
    images = contract[image_key]
    replicas = contract['replicas']
    return {
        'metadata': {
            'name': contract['deployment'],
            'namespace': contract['namespace'],
            'generation': 4,
        },
        'spec': {
            'replicas': replicas,
            'template': {
                'spec': {
                    'containers': [{
                        'name': name,
                        'image': image,
                    } for name, image in images.items()]
                }
            },
        },
        'status': {
            'observedGeneration': 4,
            'readyReplicas': replicas,
            'availableReplicas': replicas,
            'updatedReplicas': replicas,
        },
    }


def _pod_webhook_configuration(contract: dict, controller: dict, *,
                               mutating: bool) -> dict:
    pod_contract = contract['mutating' if mutating else 'validating']
    webhook = {
        'admissionReviewVersions': ['v1'],
        'clientConfig': {
            'caBundle': 'reviewed-ca',
            'service': {
                'name': contract['service_name'],
                'namespace': controller['namespace'],
                'path': pod_contract['path'],
                'port': contract['service_port'],
            },
        },
        'failurePolicy': 'Fail',
        'matchPolicy': 'Equivalent',
        'name': pod_contract['webhook_name'],
        'namespaceSelector': {
            'matchExpressions': [{
                'key': 'kubernetes.io/metadata.name',
                'operator': 'NotIn',
                'values': ['kube-system', controller['namespace']],
            }],
        },
        'objectSelector': {},
        'rules': [{
            'apiGroups': [''],
            'apiVersions': ['v1'],
            'operations': copy.deepcopy(pod_contract['operations']),
            'resources': ['pods'],
            'scope': '*',
        }],
        'sideEffects': 'None',
        'timeoutSeconds': 10,
    }
    if mutating:
        webhook['reinvocationPolicy'] = 'Never'
    return {
        'metadata': {
            'name': pod_contract['configuration_name'],
        },
        'webhooks': [webhook],
    }


def _snapshot_node_labels(provider: dict, node: dict) -> dict:
    flavor = next(flavor for flavor in provider['resource_flavors']
                  if flavor['name'] == node['flavor'])
    labels = copy.deepcopy(flavor['node_labels'])
    labels[node['product_label_key']] = node['product_label_value']
    return labels


def _kubernetes_snapshot(context: dict, provider: dict) -> dict:
    namespace = context['namespace']
    kueue_admission = context['kueue_admission']
    enforcement = provider['kueue_enforcement']
    assert kueue_admission is not None
    assert enforcement is not None
    controller = enforcement['controller']
    webhooks = enforcement['webhooks']
    snapshot = {
        'namespace': {
            'metadata': {
                'name': namespace,
                'uid': provider['namespace_uid'],
                'labels': {
                    'kubernetes.io/metadata.name': namespace,
                },
            },
            'status': {
                'phase': 'Active'
            },
        },
        'service_account': {
            'metadata': {
                'name': context['service_account_name'],
                'namespace': namespace,
                'annotations': {},
            }
        },
        'priority_class': {
            'metadata': {
                'name': context['priority_class']['name']
            },
            'value': context['priority_class']['value'],
            'globalDefault': False,
            'preemptionPolicy': context['priority_class']['preemption_policy'],
        },
        'workload_priority_class': {
            'metadata': {
                'name': kueue_admission['workload_priority_class_name']
            },
            'value': kueue_admission['workload_priority_value'],
        },
        'local_queue': _active_object(
            kueue_admission['local_queue_name'], {
                'clusterQueue': kueue_admission['cluster_queue_name'],
                'stopPolicy': 'None',
            }, namespace),
        'cluster_queue': _queue_object(context),
        'resource_flavors': {
            flavor['name']: {
                'metadata': {
                    'name': flavor['name']
                },
                'spec': {
                    'nodeLabels': copy.deepcopy(flavor['node_labels']),
                    'topologyName': flavor['topology_name'],
                },
            } for flavor in provider['resource_flavors']
        },
        'nodes': {
            node['flavor']: {
                'items': [{
                    'metadata': {
                        'name': f"initializing-{node['flavor']}",
                        'labels': _snapshot_node_labels(provider, node),
                    },
                    'status': {
                        'capacity': {
                            node['resource_name']: str(node['capacity_per_node']
                                                      )
                        },
                        'conditions': [{
                            'type': 'Ready',
                            'status': 'False',
                        }],
                    },
                }]
            } for node in provider['node_inventory']
        },
        'kueue_controller': _deployment(controller, 'images'),
        'kueue_config': {
            'metadata': {
                'name': controller['config_map'],
                'namespace': controller['namespace'],
            },
            'data': {
                'controller_manager_config.yaml':
                    ('integrations:\n  frameworks:\n  - pod\n'
                     'featureGates:\n' +
                     ''.join(f'  {name}: {str(enabled).lower()}\n'
                             for name, enabled in
                             controller['required_feature_gates'].items()))
            },
        },
        'validating_webhook': _pod_webhook_configuration(webhooks,
                                                         controller,
                                                         mutating=False),
        'mutating_webhook': _pod_webhook_configuration(webhooks,
                                                       controller,
                                                       mutating=True),
    }
    if provider['scheduler'] is not None:
        snapshot['scheduler'] = _deployment(provider['scheduler'], 'containers')
    return snapshot


def _unmanaged_kubernetes_snapshot(context: dict, provider: dict) -> dict:
    namespace = context['namespace']
    return {
        'namespace': {
            'metadata': {
                'name': namespace,
                'uid': provider['namespace_uid'],
                'labels': {},
            },
            'status': {
                'phase': 'Active'
            },
        },
        'service_account': {
            'metadata': {
                'name': context['service_account_name'],
                'namespace': namespace,
                'annotations': {},
            }
        },
        'priority_class': {
            'metadata': {
                'name': context['priority_class']['name']
            },
            'value': context['priority_class']['value'],
            'globalDefault': False,
            'preemptionPolicy': context['priority_class']['preemption_policy'],
        },
        'resource_flavors': {
            flavor['name']: {
                'metadata': {
                    'name': flavor['name']
                },
                'spec': {
                    'nodeLabels': copy.deepcopy(flavor['node_labels']),
                    'topologyName': flavor['topology_name'],
                },
            } for flavor in provider['resource_flavors']
        },
        'nodes': {
            node['flavor']: {
                'items': [{
                    'metadata': {
                        'name': f"initializing-{node['flavor']}",
                        'labels': _snapshot_node_labels(provider, node),
                    },
                    'status': {
                        'capacity': {
                            node['resource_name']: str(node['capacity_per_node']
                                                      )
                        },
                    },
                }]
            } for node in provider['node_inventory']
        },
        'scheduler': _deployment(provider['scheduler'], 'containers'),
    }


def test_kubernetes_snapshot_proves_unmanaged_context_without_kueue_reads():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('prod_research_cluster_eks')
    provider = bundle.provider_context('prod_research_cluster_eks')
    snapshot = _unmanaged_kubernetes_snapshot(context, provider)

    proof = kubernetes_attestation.validate_snapshot(context, provider,
                                                     snapshot)

    assert not proof.kueue_managed
    assert proof.local_queue_name is None
    assert proof.cluster_queue_name is None
    assert proof.assign_queue_labels_for_pods is None
    assert proof.topology_aware_scheduling is None
    assert proof.custom_scheduler_deployment_proven
    assert proof.resource_flavor_topology_names == (
        ('ml.p4d.24xlarge', 'hyperpod'),
        ('ml.p4de.24xlarge', 'hyperpod'),
    )

    snapshot = _unmanaged_kubernetes_snapshot(context, provider)
    snapshot['resource_flavors']['ml.p4d.24xlarge']['spec'][
        'topologyName'] = 'wrong'
    with pytest.raises(
            kubernetes_attestation.KubernetesAttestationNonconformanceError,
            match='topology'):
        kubernetes_attestation.validate_snapshot(context, provider, snapshot)


def test_kubernetes_snapshot_proves_exact_external_lane():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')

    snapshot = _kubernetes_snapshot(context, provider)
    assert 'workload_priority_pod_class' not in snapshot
    proof = kubernetes_attestation.validate_snapshot(context, provider,
                                                     snapshot)

    assert proof.kueue_managed
    assert proof.local_queue_name == 'be'
    assert proof.cluster_queue_name == 'research-be'
    assert proof.pod_identity_irsa_annotation_absent
    assert proof.assign_queue_labels_for_pods
    assert proof.topology_aware_scheduling
    assert not proof.custom_scheduler_deployment_proven
    assert proof.resource_flavor_topology_names == (('ml.p5e.48xlarge',
                                                     'hyperpod'),)
    assert proof.node_flavors == (kubernetes_attestation.NodeFlavorProof(
        flavor='ml.p5e.48xlarge',
        non_deleting_node_count=1,
        product_label_value='NVIDIA-H200',
        resource_name='nvidia.com/gpu',
        capacity_per_node=8),)


def test_kubernetes_node_list_selector_uses_all_resource_flavor_labels():
    bundle = bundle_lib.load_embedded_bundle()
    provider = bundle.provider_context('phx_research_cluster_eks')

    selector = kubernetes_attestation._resource_flavor_node_selector(
        provider, 'ml.p5e.48xlarge')

    assert selector == ('beta.kubernetes.io/instance-type=ml.p5e.48xlarge,'
                        'node.kubernetes.io/instance-type=ml.p5e.48xlarge,'
                        'sagemaker.amazonaws.com/compute-type=hyperpod')


def test_kubernetes_snapshot_ignores_platform_owned_queue_policy():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    snapshot['cluster_queue']['spec'].update({
        'namespaceSelector': {},
        'cohortName': 'changed-by-platform',
        'fairSharing': {
            'weight': '17'
        },
        'preemption': {
            'reclaimWithinCohort': 'Any'
        },
        'queueingStrategy': 'StrictFIFO',
        'resourceGroups': [{
            'changed': 'by-platform'
        }],
    })

    proof = kubernetes_attestation.validate_snapshot(context, provider,
                                                     snapshot)

    assert proof.kueue_managed
    assert proof.cluster_queue_name == 'research-be'


@pytest.mark.parametrize('selector_mutation', [
    lambda spec: spec.pop('namespaceSelector'),
    lambda spec: spec.__setitem__('namespaceSelector', None),
])
def test_kubernetes_snapshot_rejects_nil_cluster_queue_selector(
        selector_mutation):
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    selector_mutation(snapshot['cluster_queue']['spec'])

    with pytest.raises(
            kubernetes_attestation.KubernetesAttestationNonconformanceError,
            match='external namespace'):
        kubernetes_attestation.validate_snapshot(context, provider, snapshot)


def test_attest_context_gets_only_the_named_external_lane(monkeypatch):
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    client = mock.Mock()
    client.sanitize_for_serialization.side_effect = lambda value: value
    core = mock.Mock()
    custom = mock.Mock()
    apps = mock.Mock()
    scheduling = mock.Mock()
    admission = mock.Mock()
    for api in (core, apps, scheduling, admission):
        for method_name in ('read_namespace', 'read_namespaced_service_account',
                            'list_node', 'read_namespaced_config_map',
                            'read_namespaced_deployment', 'read_priority_class',
                            'read_validating_webhook_configuration',
                            'read_mutating_webhook_configuration'):
            getattr(api, method_name).return_value = {}
    custom.get_cluster_custom_object.side_effect = (lambda **kwargs: {
        'metadata': {
            'name': kwargs['name']
        }
    })
    custom.get_namespaced_custom_object.side_effect = (lambda **kwargs: {
        'metadata': {
            'name': kwargs['name'],
            'namespace': kwargs['namespace'],
        }
    })
    client_module = kubernetes_attestation.kubernetes_adaptor.kubernetes.client
    monkeypatch.setattr(client_module, 'CoreV1Api', lambda **_kwargs: core)
    monkeypatch.setattr(client_module, 'CustomObjectsApi',
                        lambda **_kwargs: custom)
    monkeypatch.setattr(client_module, 'AppsV1Api', lambda **_kwargs: apps)
    monkeypatch.setattr(client_module, 'SchedulingV1Api',
                        lambda **_kwargs: scheduling)
    monkeypatch.setattr(client_module, 'AdmissionregistrationV1Api',
                        lambda **_kwargs: admission)
    monkeypatch.setattr(
        kubernetes_attestation, '_audit_api_client',
        lambda *_args, **_kwargs: contextlib.nullcontext(client))
    monkeypatch.setattr(kubernetes_attestation, '_require_physical_cluster_uid',
                        lambda *_args, **_kwargs: None)
    expected_proof = object()
    monkeypatch.setattr(kubernetes_attestation, 'validate_snapshot',
                        lambda *_args: expected_proof)

    proof = kubernetes_attestation.attest_context(
        context,
        provider,
        deadline_monotonic=time.monotonic() + 5,
        cancellation=threading.Event(),
        audit_session=object())

    assert proof is expected_proof
    cluster_queue_calls = [
        call.kwargs
        for call in custom.get_cluster_custom_object.call_args_list
        if call.kwargs['plural'] == 'clusterqueues'
    ]
    assert len(cluster_queue_calls) == 1
    assert cluster_queue_calls[0]['name'] == 'research-be'
    custom.list_cluster_custom_object.assert_not_called()
    assert not any(
        call.kwargs.get('plural') == 'cohorts'
        for call in custom.get_cluster_custom_object.call_args_list)


def test_kueue_version_fallback_recomputes_remaining_timeout(monkeypatch):
    calls = []
    timeouts = iter(((0.8, 0.8), (0.3, 0.3)))
    monkeypatch.setattr(kubernetes_attestation, '_request_timeout',
                        lambda *_args: next(timeouts))
    not_found = kubernetes_attestation.kubernetes_adaptor.api_exception()(
        status=404)

    class CustomObjects:

        @staticmethod
        def get_cluster_custom_object(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise not_found
            return {'metadata': {'name': kwargs['name']}}

    result = kubernetes_attestation._get_kueue_object(
        CustomObjects(),
        plural='resourceflavors',
        name='exact-flavor',
        deadline_monotonic=time.monotonic() + 5,
        cancellation=threading.Event())

    assert result == {'metadata': {'name': 'exact-flavor'}}
    assert [call['version'] for call in calls] == ['v1beta2', 'v1beta1']
    assert [call['_request_timeout'] for call in calls] == [(0.8, 0.8),
                                                            (0.3, 0.3)]


def test_generated_kubernetes_client_receives_real_bounded_timeout():
    """Exercise the generated REST layer, which ignores float timeouts."""
    client_lib = kubernetes_attestation.kubernetes_adaptor.kubernetes.client
    configuration = client_lib.Configuration()
    configuration.retries = 0
    rest_client = client_lib.rest.RESTClientObject(configuration)
    response = types.SimpleNamespace(status=200)
    with mock.patch.object(rest_client.pool_manager,
                           'request',
                           return_value=response) as request:
        result = rest_client.request('GET',
                                     'https://exact.eks.example/api/v1/nodes',
                                     _preload_content=False,
                                     _request_timeout=(0.7, 0.4))

    assert result is response
    timeout = request.call_args.kwargs['timeout']
    assert timeout.connect_timeout == 0.7
    assert timeout.read_timeout == 0.4
    retries = rest_client.pool_manager.connection_pool_kw['retries']
    assert retries.total == 0


def test_kubernetes_snapshot_ignores_platform_admission_policy_objects():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    expected = kubernetes_attestation.validate_snapshot(context, provider,
                                                        snapshot)

    assert 'admission_policy' not in snapshot
    assert 'admission_policy_binding' not in snapshot
    snapshot['admission_policy'] = {
        'metadata': 'not-a-mapping',
        'spec': {
            'failurePolicy': 'Ignore',
            'validations': [],
        },
    }
    snapshot['admission_policy_binding'] = {
        'spec': {
            'validationActions': ['Warn'],
            'matchResources': 'not-a-mapping',
        },
    }

    assert kubernetes_attestation.validate_snapshot(context, provider,
                                                    snapshot) == expected


@pytest.mark.parametrize('mutation,match', [
    (lambda snapshot: snapshot['service_account']['metadata']['annotations'].
     update({'eks.amazonaws.com/role-arn': 'arn:unreviewed'}), 'IRSA'),
    (lambda snapshot: snapshot['resource_flavors']['ml.p5e.48xlarge']['spec']
     ['nodeLabels'].__setitem__('beta.kubernetes.io/instance-type', 'ml.wrong'),
     'instance selector'),
    (lambda snapshot: snapshot['resource_flavors']['ml.p5e.48xlarge']['spec'][
        'nodeLabels'].__setitem__('example.com/unreviewed', 'true'),
     'exact reviewed'),
    (lambda snapshot: snapshot['resource_flavors']['ml.p5e.48xlarge']['spec'].
     __setitem__('nodeTaints', []), 'exact reviewed'),
    (lambda snapshot: snapshot['resource_flavors']['ml.p5e.48xlarge']['spec'].
     __setitem__('topologyName', 'wrong'), 'topology'),
    (lambda snapshot: snapshot['nodes'][
        'ml.p5e.48xlarge']['items'][0]['metadata']['labels'].__setitem__(
            'nvidia.com/gpu.product', 'NVIDIA-A100'), 'GPU product'),
    (lambda snapshot: snapshot['nodes']['ml.p5e.48xlarge']['items'][0]['status']
     ['capacity'].__setitem__('nvidia.com/gpu', '4'), 'GPU product'),
    (lambda snapshot: snapshot['local_queue']['spec'].__setitem__(
        'clusterQueue', 'wrong'), 'LocalQueue target'),
    (lambda snapshot: snapshot['local_queue']['spec'].__setitem__(
        'stopPolicy', 'Hold'), 'LocalQueue target'),
    (lambda snapshot: snapshot['workload_priority_class'].__setitem__(
        'value', 12), 'WorkloadPriorityClass reclaim contract'),
    (lambda snapshot: snapshot['cluster_queue']['metadata'].__setitem__(
        'name', 'wrong'), 'exact current object'),
    (lambda snapshot: snapshot['cluster_queue']['spec'].__setitem__(
        'namespaceSelector',
        {'matchLabels': {
            'kubernetes.io/metadata.name': 'another-namespace'
        }}), 'external namespace'),
    (lambda snapshot: snapshot['cluster_queue']['spec'].__setitem__(
        'stopPolicy', 'Hold'), 'external namespace'),
    (lambda snapshot: snapshot['kueue_config']['data'].__setitem__(
        'controller_manager_config.yaml',
        'integrations:\n  frameworks:\n  - pod\nfeatureGates: {}\n'),
     'required feature gate'),
    (lambda snapshot: snapshot['validating_webhook']['webhooks'][0].__setitem__(
        'rules', []), 'Pod admission contract'),
    (lambda snapshot: snapshot['validating_webhook']['webhooks'][0]['rules'][0].
     __setitem__('operations', ['CREATE']), 'Pod admission contract'),
    (lambda snapshot: snapshot['validating_webhook']['webhooks'][0][
        'clientConfig']['service'].__setitem__('name', 'unrelated-webhook'),
     'Pod admission contract'),
    (lambda snapshot: snapshot['mutating_webhook']['webhooks'][0][
        'clientConfig']['service'].__setitem__('path', '/unrelated'),
     'Pod admission contract'),
    (lambda snapshot: snapshot['mutating_webhook']['webhooks'][0][
        'clientConfig'].__setitem__('caBundle', ''), 'Pod admission contract'),
    (lambda snapshot: snapshot['mutating_webhook']['webhooks'][0].__setitem__(
        'namespaceSelector', {}), 'Pod admission contract'),
])
def test_kubernetes_snapshot_fails_closed_on_drift(mutation, match):
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    mutation(snapshot)

    with pytest.raises(
            kubernetes_attestation.KubernetesAttestationNonconformanceError,
            match=match):
        kubernetes_attestation.validate_snapshot(context, provider, snapshot)


@pytest.mark.parametrize('nodes', [
    [],
    [{
        'metadata': {
            'name': 'terminating',
            'deletionTimestamp': '2026-08-13T00:00:00Z',
            'labels': {
                'beta.kubernetes.io/instance-type': 'ml.p5e.48xlarge'
            },
        },
    }],
])
def test_kubernetes_snapshot_records_exact_zero_non_deleting_nodes(nodes):
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    snapshot['nodes']['ml.p5e.48xlarge']['items'] = nodes

    proof = kubernetes_attestation.validate_snapshot(context, provider,
                                                     snapshot)

    assert proof.node_flavors == (kubernetes_attestation.NodeFlavorProof(
        flavor='ml.p5e.48xlarge',
        non_deleting_node_count=0,
        product_label_value='NVIDIA-H200',
        resource_name='nvidia.com/gpu',
        capacity_per_node=8),)


def test_kubernetes_snapshot_requires_complete_node_selector_match():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    del snapshot['nodes']['ml.p5e.48xlarge']['items'][0]['metadata']['labels'][
        'sagemaker.amazonaws.com/compute-type']

    with pytest.raises(
            kubernetes_attestation.KubernetesAttestationNonconformanceError,
            match='complete reviewed ResourceFlavor instance selector'):
        kubernetes_attestation.validate_snapshot(context, provider, snapshot)


def test_kubernetes_snapshot_keeps_malformed_response_indeterminate():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    snapshot['cluster_queue']['spec'] = None

    with pytest.raises(
            kubernetes_attestation.KubernetesAttestationIndeterminateError,
            match='invalid research-be spec data'):
        kubernetes_attestation.validate_snapshot(context, provider, snapshot)


def test_machine_readable_proof_exposes_identity_absence(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    monkeypatch.setattr(policy, '_attest_contexts', _fake_attest(policy))
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())

    payload = policy.preflight(deadline_monotonic=time.monotonic() + 5)

    assert payload['schema_version'] == 2
    assert payload['operation'] == 'preflight'
    assert payload['success'] is True
    assert len(payload['contexts']) == 2
    assert all(item['aws']['identity_absence_proven'] is True
               for item in payload['contexts'])


def test_preflight_cli_prints_exactly_one_json_object(monkeypatch, capsys):

    class FakePolicy:

        def __init__(self):
            print('provider startup noise')

        def preflight(self, *, deadline_monotonic, emit_log):
            del deadline_monotonic
            assert emit_log is False
            print('provider operation noise')
            return {
                'schema_version': 2,
                'operation': 'preflight',
                'success': True,
            }

    monkeypatch.setattr(preflight, 'BoltzReservedFillReclaimPolicy', FakePolicy)

    assert preflight.main() == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        'schema_version': 2,
        'operation': 'preflight',
        'success': True,
    }
    assert captured.out.count('\n') == 1
    assert 'provider startup noise' in captured.err
    assert 'provider operation noise' in captured.err


def test_preflight_cli_failure_uses_current_proof_schema(monkeypatch, capsys):

    class FailingPolicy:

        def __init__(self):
            raise RuntimeError('attestation failed')

    monkeypatch.setattr(preflight, 'BoltzReservedFillReclaimPolicy',
                        FailingPolicy)

    assert preflight.main() == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        'schema_version': 2,
        'operation': 'preflight',
        'success': False,
        'error_code': 'ATTESTATION_FAILED',
    }
    assert captured.out.count('\n') == 1
