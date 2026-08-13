"""Deployment-policy tests for Boltz reserved-capacity reclaim."""

# pylint: disable=protected-access,wrong-import-position
import copy
import dataclasses
import datetime
import json
import pathlib
import sys
import threading
import time
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
from boltz_reserved_fill_reclaim_policy import preflight  # noqa: E402

from sky.serve import reserved_fill_reclaim_attestation as reclaim


def _bundle_document() -> dict:
    return json.loads(
        (_POLICY_PROJECT / 'src' / 'boltz_reserved_fill_reclaim_policy' /
         'fleet_bundle.json').read_text(encoding='utf-8'))


def _encoded_bundle(document: dict) -> bytes:
    return json.dumps(document, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')


def test_embedded_bundle_binds_observed_gpu_products_and_canonical_names():
    bundle = bundle_lib.load_embedded_bundle()
    east = bundle.fleet_context('prod_research_cluster_eks')
    phx = bundle.fleet_context('phx_research_cluster_eks')

    assert east['local_queue_name'] == 'default'
    assert east['queues'][
        'inference_cluster_queue'] == 'skyserve-inference-borrowed'
    assert east['workload_priority_class_name'] == 'skyserve-inference-low'
    assert east['priority_class'][
        'name'] == 'rescluster-k8s-prod-east1-preemptible-inference-low'
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
    assert [
        quota['borrowing_limit']
        for quota in east['queues']['research_gpu_quotas']
    ] == ['0', '0']
    assert all(quota['resource_name'] == 'nvidia.com/gpu'
               for context in (east, phx)
               for quota in context['queues']['inference_gpu_quotas'])


def test_bundle_hashes_are_domain_separated_and_order_independent():
    document = _bundle_document()
    first = bundle_lib.parse_bundle_bytes(_encoded_bundle(document))
    reordered = copy.deepcopy(document)
    reordered['fleet']['contexts'].reverse()
    reordered['provider_inventory']['contexts'].reverse()
    for context in reordered['fleet']['contexts']:
        context['queues']['inference_gpu_quotas'].reverse()
        context['queues']['research_gpu_quotas'].reverse()
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


def test_bundle_rejects_duplicate_keys_unknown_keys_and_unsafe_quota():
    with pytest.raises(bundle_lib.BundleValidationError, match='Duplicate'):
        bundle_lib.parse_bundle_bytes(
            b'{"schema_version":1,"schema_version":1}')

    document = _bundle_document()
    document['unknown'] = True
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='unexpected schema'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    document['fleet']['contexts'][0]['queues']['inference_gpu_quotas'][0][
        'nominal_quota'] = '1'
    with pytest.raises(bundle_lib.BundleValidationError, match='zero-nominal'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))

    document = _bundle_document()
    document['provider_inventory']['contexts'][0]['node_inventory'][0][
        'product_label_value'] = 'NVIDIA-H200'
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='reviewed flavor, Node selector, and product'):
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
    document['provider_inventory']['contexts'][0]['kueue_webhooks'][
        'validating']['operations'] = ['CREATE']
    with pytest.raises(bundle_lib.BundleValidationError,
                       match='exact reviewed Pod operations'):
        bundle_lib.parse_bundle_bytes(_encoded_bundle(document))


def _admission(context: dict,
               accelerator: str) -> reclaim.ReclaimProjectedAdmission:
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
        local_queue_name=context['local_queue_name'],
        workload_priority_class_name=context['workload_priority_class_name'],
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
            local_queue_name=context['local_queue_name'],
            cluster_queue_name=context['queues']['inference_cluster_queue'],
            pod_identity_irsa_annotation_absent=True,
            assign_queue_labels_for_pods=True,
            node_flavors=tuple(
                kubernetes_attestation.NodeFlavorProof(
                    flavor=node['flavor'],
                    non_deleting_node_count=1,
                    product_label_value=node['product_label_value'],
                    resource_name=node['resource_name'],
                    capacity_per_node=node['capacity_per_node'])
                for node in provider['node_inventory'])))


def _fake_attest(policy: policy_lib.BoltzReservedFillReclaimPolicy):

    def attest(names, _deadline):
        return {
            name: _context_proof(
                policy._bundle.fleet_context(name),
                policy._bundle.provider_context(name)) for name in names
        }

    return attest


def test_two_arbitrary_services_share_one_canonical_claim_path(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    monkeypatch.setattr(policy, '_attest_contexts', _fake_attest(policy))
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


def test_one_context_can_claim_multiple_exact_card_edges(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    attest = mock.Mock(side_effect=_fake_attest(policy))
    monkeypatch.setattr(policy, '_attest_contexts', attest)
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    context = policy._bundle.fleet_context('prod_research_cluster_eks')
    edges = tuple(sorted((_edge(context, 'a100'), _edge(context, 'a100-80gb'))))
    scope = reclaim.ReclaimClaimSetScope(
        service_name='east-all-cards',
        service_incarnation='incarnation-east-all-cards',
        service_version=1,
        semantic_hash='semantic-east-all-cards',
        edges=edges)

    authorization = policy.authorize_claim_set(
        scope,
        expected_identity=policy.policy_identity(),
        expected_gate_generation=7,
        deadline_monotonic=time.monotonic() + 5)

    assert authorization.scope == scope
    assert attest.call_args.args[0] == ('prod_research_cluster_eks',)


def test_claim_set_rejects_duplicate_physical_card_atom(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    provider_calls = mock.Mock()
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
    context = policy._bundle.fleet_context('prod_research_cluster_eks')
    first = _edge(context, 'a100')
    second_admission = dataclasses.replace(first.projected_admissions[0],
                                           worker_projection_sha256='b' * 64)
    second = dataclasses.replace(first,
                                 projected_admissions=(second_admission,))
    scope = reclaim.ReclaimClaimSetScope(
        service_name='duplicate-east-card',
        service_incarnation='incarnation-duplicate-east-card',
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
    context = policy._bundle.fleet_context('prod_research_cluster_eks')
    claim = _claim(context, 'a100')
    admission = dataclasses.replace(
        claim.projected_admissions[0],
        accelerator_scheduling=reclaim.ReclaimAcceleratorScheduling(
            label_key='nvidia.com/gpu.product',
            label_values=('NVIDIA-A100-SXM4-80GB',),
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
    monkeypatch.setattr(policy, '_attest_contexts', provider_calls)
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


def test_activation_accepts_multiple_cards_in_one_context(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    attest = mock.Mock(side_effect=_fake_attest(policy))
    monkeypatch.setattr(policy, '_attest_contexts', attest)
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())
    context = policy._bundle.fleet_context('prod_research_cluster_eks')
    claims = tuple(
        sorted((_claim(context, 'a100'), _claim(context, 'a100-80gb'))))

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
    context = policy._bundle.fleet_context('prod_research_cluster_eks')
    claims = tuple(
        sorted((_claim(context, 'a100'),
                _claim(context, 'a100', projection_sha256='b' * 64))))

    with pytest.raises(reclaim.ReclaimAttestationError,
                       match='same physical accelerator pool twice'):
        policy.attest_activation(claims,
                                 writer_image_digest='sha256:' + 'b' * 64,
                                 deadline_monotonic=time.monotonic() + 5)
    provider_calls.assert_not_called()


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
    proofs = policy._attest_contexts(policy._bundle.contexts,
                                     time.monotonic() + 2)

    assert set(proofs) == set(policy._bundle.contexts)


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

    with pytest.raises(aws_attestation.AwsAttestationError, match='pagination'):
        aws_attestation._list_associations(Eks(),
                                           cluster_name='cluster',
                                           namespace='inference',
                                           service_account='worker',
                                           deadline_monotonic=time.monotonic() +
                                           5,
                                           cancellation=threading.Event())


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


def _queue_object(context: dict, *, inference: bool) -> dict:
    queues = context['queues']
    prefix = 'inference' if inference else 'research'
    quotas = queues[f'{prefix}_gpu_quotas']
    spec = {
        'cohortName': queues['cohort'],
        'namespaceSelector': {
            'matchLabels': {
                'kubernetes.io/metadata.name':
                    (context['namespace']
                     if inference else queues['research_namespace'])
            }
        },
        'preemption': {
            'borrowWithinCohort': {
                'policy': queues[f'{prefix}_preemption']['borrow_within_cohort']
            },
            'reclaimWithinCohort': queues[f'{prefix}_preemption']
                                   ['reclaim_within_cohort'],
            'withinClusterQueue': queues[f'{prefix}_preemption']
                                  ['within_cluster_queue'],
        },
        'stopPolicy': 'None',
        'resourceGroups': [{
            'coveredResources': ['nvidia.com/gpu'],
            'flavors': [{
                'name': quota['flavor'],
                'resources': [{
                    'name': 'nvidia.com/gpu',
                    'nominalQuota': quota['nominal_quota'],
                    'borrowingLimit': quota['borrowing_limit'],
                }],
            } for quota in quotas],
        }],
    }
    name = queues[f'{prefix}_cluster_queue']
    return _active_object(name, spec)


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


def _kubernetes_snapshot(context: dict, provider: dict) -> dict:
    namespace = context['namespace']
    admission = provider['admission_policy']
    controller = provider['kueue_controller']
    webhooks = provider['kueue_webhooks']
    return {
        'namespace': {
            'metadata': {
                'name': namespace,
                'uid': provider['namespace_uid'],
                'labels': {
                    admission['namespace_label_key']:
                        admission['namespace_label_value']
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
                'name': context['workload_priority_class_name']
            },
            'value': context['priority_class']['value'],
        },
        'local_queue': _active_object(
            context['local_queue_name'], {
                'clusterQueue': context['queues']['inference_cluster_queue'],
                'stopPolicy': 'None',
            }, namespace),
        'inference_cluster_queue': _queue_object(context, inference=True),
        'research_cluster_queue': _queue_object(context, inference=False),
        'resource_flavors': {
            flavor['name']: {
                'metadata': {
                    'name': flavor['name']
                },
                'spec': {
                    'nodeLabels': copy.deepcopy(flavor['node_labels'])
                },
            } for flavor in provider['resource_flavors']
        },
        'nodes': {
            node['flavor']: {
                'items': [{
                    'metadata': {
                        'name': f"initializing-{node['flavor']}",
                        'labels': {
                            node['selector_label_key']:
                                node['selector_label_value'],
                            node['product_label_key']:
                                node['product_label_value'],
                        },
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
        'scheduler': _deployment(provider['scheduler'], 'containers'),
        'kueue_controller': _deployment(controller, 'images'),
        'kueue_config': {
            'metadata': {
                'name': controller['config_map'],
                'namespace': controller['namespace'],
            },
            'data': {
                'controller_manager_config.yaml':
                    ('integrations:\n  frameworks:\n  - pod\n'
                     'featureGates:\n  AssignQueueLabelsForPods: true\n')
            },
        },
        'admission_policy': {
            'metadata': {
                'name': admission['name']
            },
            'spec': {
                'failurePolicy': 'Fail',
                'matchConditions': [{
                    'name': 'exclude-hpto-owned',
                    'expression':
                        ('!(object.kind == "StatefulSet" && '
                         'has(object.metadata.ownerReferences) && '
                         'object.metadata.ownerReferences.exists(ref, '
                         'ref.kind == "HyperPodPyTorchJob" && '
                         'ref.apiVersion == '
                         '"sagemaker.amazonaws.com/v1")) && '
                         '!(object.kind == "Pod" && '
                         'has(object.metadata.ownerReferences) && '
                         'object.metadata.ownerReferences.exists(ref, '
                         'ref.kind == "StatefulSet" && '
                         'ref.apiVersion == "apps/v1"))'),
                }],
                'matchConstraints': {
                    'matchPolicy': 'Equivalent',
                    'namespaceSelector': {},
                    'objectSelector': {},
                    'resourceRules': [{
                        'apiGroups': [''],
                        'apiVersions': ['v1'],
                        'resources': ['pods'],
                        'operations': ['CREATE', 'UPDATE'],
                        'scope': '*',
                    }]
                },
                'validations': [{
                    'expression':
                        ("has(object.metadata.labels) && "
                         "'kueue.x-k8s.io/queue-name' in "
                         'object.metadata.labels && '
                         "object.metadata.labels['kueue.x-k8s.io/queue-name'] "
                         "!= ''")
                }],
            },
        },
        'admission_policy_binding': {
            'metadata': {
                'name': admission['binding_name']
            },
            'spec': {
                'policyName': admission['name'],
                'validationActions': ['Deny'],
                'matchResources': {
                    'matchPolicy': 'Equivalent',
                    'namespaceSelector': {
                        'matchLabels': {
                            admission['namespace_label_key']:
                                admission['namespace_label_value']
                        }
                    },
                    'objectSelector': {},
                },
            },
        },
        'validating_webhook': _pod_webhook_configuration(webhooks,
                                                         controller,
                                                         mutating=False),
        'mutating_webhook': _pod_webhook_configuration(webhooks,
                                                       controller,
                                                       mutating=True),
    }


def test_kubernetes_snapshot_proves_exact_reclaim_topology():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')

    proof = kubernetes_attestation.validate_snapshot(
        context, provider, _kubernetes_snapshot(context, provider))

    assert proof.cluster_queue_name == 'skyserve-inference-borrowed'
    assert proof.pod_identity_irsa_annotation_absent
    assert proof.assign_queue_labels_for_pods
    assert proof.node_flavors == (kubernetes_attestation.NodeFlavorProof(
        flavor='ml.p5e.48xlarge',
        non_deleting_node_count=1,
        product_label_value='NVIDIA-H200',
        resource_name='nvidia.com/gpu',
        capacity_per_node=8),)


@pytest.mark.parametrize('mutation,match', [
    (lambda snapshot: snapshot['service_account']['metadata']['annotations'].
     update({'eks.amazonaws.com/role-arn': 'arn:unreviewed'}), 'IRSA'),
    (lambda snapshot: snapshot['resource_flavors']['ml.p5e.48xlarge']['spec']
     ['nodeLabels'].__setitem__('beta.kubernetes.io/instance-type', 'ml.wrong'),
     'instance selector'),
    (lambda snapshot: snapshot['nodes'][
        'ml.p5e.48xlarge']['items'][0]['metadata']['labels'].__setitem__(
            'nvidia.com/gpu.product', 'NVIDIA-A100'), 'GPU product'),
    (lambda snapshot: snapshot['nodes']['ml.p5e.48xlarge']['items'][0]['status']
     ['capacity'].__setitem__('nvidia.com/gpu', '4'), 'GPU product'),
    (lambda snapshot: snapshot['local_queue']['spec'].__setitem__(
        'clusterQueue', 'wrong'), 'LocalQueue target'),
    (lambda snapshot: snapshot['kueue_config']['data'].__setitem__(
        'controller_manager_config.yaml',
        'integrations:\n  frameworks:\n  - pod\nfeatureGates: {}\n'),
     'AssignQueueLabelsForPods'),
    (lambda snapshot: snapshot['admission_policy_binding']['spec'].__setitem__(
        'validationActions', ['Warn']), 'binding'),
    (lambda snapshot: snapshot['admission_policy']['spec']['validations'][0].
     __setitem__('expression', 'true'), 'not fail closed'),
    (lambda snapshot: snapshot['admission_policy']['spec']['matchConditions'][0]
     .__setitem__('expression', 'false'), 'owner exclusion'),
    (lambda snapshot: snapshot['admission_policy']['spec']['matchConstraints'][
        'resourceRules'][0].__setitem__('operations', None), 'invalid'),
    (lambda snapshot: snapshot['research_cluster_queue']['spec']['preemption'].
     __setitem__('reclaimWithinCohort', 'Never'), 'reclaim contract'),
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

    with pytest.raises(kubernetes_attestation.KubernetesAttestationError,
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
def test_kubernetes_snapshot_requires_a_non_deleting_node(nodes):
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    snapshot['nodes']['ml.p5e.48xlarge']['items'] = nodes

    with pytest.raises(kubernetes_attestation.KubernetesAttestationError,
                       match='no non-deleting Node'):
        kubernetes_attestation.validate_snapshot(context, provider, snapshot)


def test_kubernetes_snapshot_rejects_node_selector_mismatch():
    bundle = bundle_lib.load_embedded_bundle()
    context = bundle.fleet_context('phx_research_cluster_eks')
    provider = bundle.provider_context('phx_research_cluster_eks')
    snapshot = _kubernetes_snapshot(context, provider)
    snapshot['nodes']['ml.p5e.48xlarge']['items'][0]['metadata']['labels'][
        'beta.kubernetes.io/instance-type'] = 'ml.wrong'

    with pytest.raises(kubernetes_attestation.KubernetesAttestationError,
                       match='instance selector'):
        kubernetes_attestation.validate_snapshot(context, provider, snapshot)


def test_machine_readable_proof_exposes_identity_absence(monkeypatch):
    policy = policy_lib.BoltzReservedFillReclaimPolicy()
    monkeypatch.setattr(policy, '_attest_contexts', _fake_attest(policy))
    monkeypatch.setattr(policy, '_emit_proof', mock.Mock())

    payload = policy.preflight(deadline_monotonic=time.monotonic() + 5)

    assert payload['schema_version'] == 1
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
                'schema_version': 1,
                'operation': 'preflight',
                'success': True,
            }

    monkeypatch.setattr(preflight, 'BoltzReservedFillReclaimPolicy', FakePolicy)

    assert preflight.main() == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        'schema_version': 1,
        'operation': 'preflight',
        'success': True,
    }
    assert captured.out.count('\n') == 1
    assert 'provider startup noise' in captured.err
    assert 'provider operation noise' in captured.err
