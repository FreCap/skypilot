"""Boltz implementation of SkyPilot's reserved-fill reclaim policy."""

from collections.abc import Mapping
from collections.abc import Sequence
import concurrent.futures
import dataclasses
import json
import logging
import math
import re
import threading
import time
from typing import Any

from boltz_reserved_fill_reclaim_policy import aws_attestation
from boltz_reserved_fill_reclaim_policy import bundle as bundle_lib
from boltz_reserved_fill_reclaim_policy import kubernetes_attestation

from sky.serve import reserved_fill_reclaim_attestation as reclaim
from sky.serve import reserved_fill_reclaim_proofs

logger = logging.getLogger(__name__)

_IMAGE_DIGEST_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
PROOF_SCHEMA_VERSION = 2


@dataclasses.dataclass(frozen=True)
class _ContextProof:
    aws: aws_attestation.PodIdentityProof
    kubernetes: kubernetes_attestation.KubernetesContextProof


class BoltzReservedFillReclaimPolicy(reclaim.ReservedFillReclaimPolicy):
    """One code-owned path for activation, claims, and launches."""

    def __init__(self) -> None:
        self._bundle = bundle_lib.load_embedded_bundle()
        self._aws_sessions = aws_attestation.AuditSessionCache()

    def enforcement_contract(self) -> reclaim.ReclaimEnforcementContract:
        return (reclaim.ReclaimEnforcementContract.
                GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2)

    def policy_identity(self) -> reclaim.ReclaimPolicyIdentity:
        return reclaim.ReclaimPolicyIdentity(
            fleet_bundle_sha256=self._bundle.fleet_bundle_sha256,
            policy_revision=self._bundle.policy_revision,
            provider_inventory_sha256=(self._bundle.provider_inventory_sha256))

    @staticmethod
    def _require_deadline(deadline_monotonic: float) -> None:
        if (isinstance(deadline_monotonic, bool) or
                not isinstance(deadline_monotonic, (int, float)) or
                not math.isfinite(float(deadline_monotonic)) or
                time.monotonic() >= deadline_monotonic):
            raise reclaim.ReclaimAttestationError(
                'The Boltz reclaim-policy deadline is invalid or expired.')

    def _require_identity(
        self,
        expected_identity: reclaim.ReclaimPolicyIdentity,
        expected_gate_generation: int,
    ) -> None:
        if (not isinstance(expected_identity, reclaim.ReclaimPolicyIdentity) or
                expected_identity != self.policy_identity()):
            raise reclaim.ReclaimAttestationError(
                'The expected reclaim-policy identity is not this bundle.')
        if (type(expected_gate_generation) is not int or
                expected_gate_generation <= 0):
            raise reclaim.ReclaimAttestationError(
                'The reclaim gate generation must be positive.')

    def _require_admission(
        self,
        admission: reclaim.ReclaimProjectedAdmission,
    ) -> Mapping[str, Any]:
        if not isinstance(admission, reclaim.ReclaimProjectedAdmission):
            raise reclaim.ReclaimAttestationError(
                'The projected admission is not typed.')
        try:
            context = self._bundle.fleet_context(admission.kubernetes_context)
        except bundle_lib.BundleValidationError as error:
            raise reclaim.ReclaimAttestationError(
                'The projected Kubernetes context is not allowlisted.'
            ) from error
        accelerator = context['accelerators'].get(admission.accelerator)
        if not isinstance(accelerator, Mapping):
            raise reclaim.ReclaimAttestationError(
                'The projected accelerator is not allowlisted in this context.')
        kueue_admission = context['kueue_admission']
        if kueue_admission is None:
            admission_mode = reclaim.ReclaimAdmissionMode.KUBERNETES_SCHEDULER
            local_queue_name = None
            workload_priority_class_name = None
        elif isinstance(kueue_admission, Mapping):
            admission_mode = reclaim.ReclaimAdmissionMode.KUEUE
            local_queue_name = kueue_admission['local_queue_name']
            workload_priority_class_name = (
                kueue_admission['workload_priority_class_name'])
        else:
            raise reclaim.ReclaimAttestationError(
                'The reviewed fleet admission contract is malformed.')
        expected_scheduling = reclaim.ReclaimAcceleratorScheduling(
            label_key=accelerator['product_label_key'],
            label_values=tuple(sorted(accelerator['product_label_values'])),
            resource_key=accelerator['resource_name'])
        priority = context['priority_class']
        if (admission.namespace != context['namespace'] or
                admission.service_account_name
                != context['service_account_name'] or
                admission.pod_identity_role_arn
                != context['pod_identity_role_arn'] or
                admission.scheduler_name != context['scheduler_name'] or
                admission.priority_class_name != priority['name'] or
                admission.priority_value != priority['value'] or
                admission.preemption_policy != priority['preemption_policy'] or
                admission.admission_mode is not admission_mode or
                admission.local_queue_name != local_queue_name or
                admission.workload_priority_class_name
                != workload_priority_class_name or
                admission.accelerator_count != accelerator['count'] or
                admission.accelerator_scheduling != expected_scheduling):
            raise reclaim.ReclaimAttestationError(
                'The projected admission does not match the reviewed fleet '
                'bundle.')
        return context

    @staticmethod
    def _require_pool_key(pool_key: str, physical_cluster_uid: str,
                          accelerator_names: tuple[str, ...]) -> None:
        encoded_names: str | list[str] = (accelerator_names[0]
                                          if len(accelerator_names) == 1 else
                                          list(accelerator_names))
        expected = json.dumps(['v2', physical_cluster_uid, encoded_names])
        if pool_key != expected:
            raise reclaim.ReclaimAttestationError(
                'The claim does not use the canonical physical-pool key.')

    @staticmethod
    def _require_launch_pool_key(pool_key: str, physical_cluster_uid: str,
                                 accelerator: str,
                                 allowed_accelerators: set[str]) -> None:
        try:
            decoded = json.loads(pool_key)
        except json.JSONDecodeError as error:
            raise reclaim.ReclaimAttestationError(
                'The launch pool key is invalid.') from error
        if (not isinstance(decoded, list) or len(decoded) != 3 or
                decoded[0] != 'v2' or decoded[1] != physical_cluster_uid or
                json.dumps(decoded) != pool_key):
            raise reclaim.ReclaimAttestationError(
                'The launch does not use a canonical physical-pool key.')
        encoded_names = decoded[2]
        if isinstance(encoded_names, str):
            names = (encoded_names,)
        elif (isinstance(encoded_names, list) and encoded_names and
              all(isinstance(name, str) for name in encoded_names)):
            names = tuple(encoded_names)
        else:
            raise reclaim.ReclaimAttestationError(
                'The launch pool accelerator set is invalid.')
        if (tuple(sorted(set(names))) != names or
                any(name != name.casefold() for name in names) or
                accelerator not in names or
                not set(names).issubset(allowed_accelerators)):
            raise reclaim.ReclaimAttestationError(
                'The launch accelerator is outside its physical-pool key.')

    def _require_edge(
        self,
        edge: reclaim.ReclaimClaimEdge | reclaim.ReservedContextClaim,
    ) -> str:
        if not isinstance(
                edge, (reclaim.ReclaimClaimEdge, reclaim.ReservedContextClaim)):
            raise reclaim.ReclaimAttestationError(
                'The reclaim claim edge is not typed.')
        try:
            context = self._bundle.fleet_context(edge.access_context)
        except bundle_lib.BundleValidationError as error:
            raise reclaim.ReclaimAttestationError(
                'The claim context is not allowlisted.') from error
        if edge.physical_cluster_uid != context['physical_cluster_uid']:
            raise reclaim.ReclaimAttestationError(
                'The claim physical-cluster identity is not allowlisted.')
        accelerator_names = tuple(
            name.casefold() for name in edge.accelerator_names)
        if (accelerator_names != edge.accelerator_names or
                not set(accelerator_names).issubset(context['accelerators'])):
            raise reclaim.ReclaimAttestationError(
                'The claim accelerator set is not allowlisted.')
        self._require_pool_key(edge.pool_key, edge.physical_cluster_uid,
                               edge.accelerator_names)
        if ({item.accelerator for item in edge.projected_admissions}
                != set(accelerator_names)):
            raise reclaim.ReclaimAttestationError(
                'The claim admission set does not cover its accelerators.')
        for admission in edge.projected_admissions:
            admission_context = self._require_admission(admission)
            if admission_context['kubernetes_context'] != edge.access_context:
                raise reclaim.ReclaimAttestationError(
                    'The claim admission and access context disagree.')
        return edge.access_context

    def _require_claim_edges(
        self,
        edges: Sequence[reclaim.ReclaimClaimEdge |
                        reclaim.ReservedContextClaim],
    ) -> tuple[str, ...]:
        context_names: list[str] = []
        physical_card_atoms: set[tuple[str, str]] = set()
        for edge in edges:
            context_names.append(self._require_edge(edge))
            edge_atoms = {(edge.physical_cluster_uid, accelerator)
                          for accelerator in edge.accelerator_names}
            if physical_card_atoms.intersection(edge_atoms):
                raise reclaim.ReclaimAttestationError(
                    'One service cannot claim the same physical accelerator '
                    'pool twice.')
            physical_card_atoms.update(edge_atoms)
        return tuple(sorted(set(context_names)))

    def _require_activation_claims(
        self,
        claims: Sequence[reclaim.ReservedContextClaim],
    ) -> None:
        """Validate duplicate pools within, rather than across, services."""
        claims_by_service: dict[str, list[reclaim.ReservedContextClaim]] = {}
        for claim in claims:
            claims_by_service.setdefault(claim.service_name, []).append(claim)
        for service_claims in claims_by_service.values():
            self._require_claim_edges(service_claims)

    def _provider_job(self, context_name: str, domain: str,
                      deadline_monotonic: float,
                      cancellation: threading.Event) -> object:
        fleet_context = self._bundle.fleet_context(context_name)
        provider_context = self._bundle.provider_context(context_name)
        if domain == 'aws':
            return aws_attestation.attest_pod_identity(
                fleet_context,
                provider_context,
                deadline_monotonic=deadline_monotonic,
                cancellation=cancellation,
                session_cache=self._aws_sessions)
        if domain == 'kubernetes':
            eks_contract = provider_context['eks']
            audit_session = self._aws_sessions.session(
                eks_contract['audit_role_arn'], eks_contract['region'],
                deadline_monotonic, cancellation)
            return kubernetes_attestation.attest_context(
                fleet_context,
                provider_context,
                deadline_monotonic=deadline_monotonic,
                cancellation=cancellation,
                audit_session=audit_session)
        raise AssertionError(f'Unknown provider domain: {domain}')

    def _attest_contexts(
        self,
        context_names: Sequence[str],
        deadline_monotonic: float,
    ) -> tuple[dict[str, _ContextProof], dict[str, float]]:
        self._require_deadline(deadline_monotonic)
        canonical_contexts = tuple(sorted(set(context_names)))
        if not canonical_contexts:
            raise reclaim.ReclaimAttestationError(
                'At least one context must be attested.')
        for context_name in canonical_contexts:
            try:
                self._bundle.fleet_context(context_name)
            except bundle_lib.BundleValidationError as error:
                raise reclaim.ReclaimAttestationError(
                    'An attested context is not allowlisted.') from error
        cancellation = threading.Event()
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(canonical_contexts) * 2,
            thread_name_prefix='boltz-reclaim-attest')
        futures: dict[tuple[str, str],
                      concurrent.futures.Future[tuple[object, float]]] = {}

        def _timed_provider(context_name: str,
                            domain: str) -> tuple[object, float]:
            value = self._provider_job(context_name, domain, deadline_monotonic,
                                       cancellation)
            return value, time.monotonic()

        try:
            for context_name in canonical_contexts:
                for domain in ('aws', 'kubernetes'):
                    futures[(context_name,
                             domain)] = executor.submit(_timed_provider,
                                                        context_name, domain)
            values = {}
            failures = []
            pending = set(futures.values())
            keys_by_future = {future: key for key, future in futures.items()}
            while pending:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError
                completed, pending = concurrent.futures.wait(
                    pending,
                    timeout=remaining,
                    return_when=concurrent.futures.FIRST_COMPLETED)
                if not completed:
                    raise TimeoutError
                nonconforming = False
                for future in completed:
                    key = keys_by_future[future]
                    try:
                        values[key] = future.result()
                    except (aws_attestation.AwsAttestationNonconformanceError,
                            kubernetes_attestation.
                            KubernetesAttestationNonconformanceError):
                        # One complete exact domain mismatch disproves the
                        # conjunction immediately. In particular, do not wait
                        # for a peer SDK call that may ignore its deadline: the
                        # caller must be able to commit durable invalidation
                        # while the enclosing disposable boundary still owns
                        # that peer.
                        nonconforming = True
                    except Exception as error:  # pylint: disable=broad-except
                        # An indeterminate result cannot mask a later completed
                        # negative, so retain it until all peers finish or the
                        # common deadline expires.
                        failures.append(error)
                if nonconforming:
                    raise reclaim.ReclaimProviderNonconformanceError(
                        'The exact reclaim provider inventory is nonconforming.'
                    )
            if failures:
                raise failures[0]
            self._require_deadline(deadline_monotonic)
        except reclaim.ReclaimProviderNonconformanceError:
            cancellation.set()
            for future in futures.values():
                future.cancel()
            raise
        except Exception:
            cancellation.set()
            for future in futures.values():
                future.cancel()
            raise reclaim.ReclaimAttestationError(
                'The Boltz deployment could not prove the exact reclaim '
                'enforcement inventory before its deadline.') from None
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        proofs: dict[str, _ContextProof] = {}
        oldest_completions: dict[str, float] = {}
        for context_name in canonical_contexts:
            aws_proof, aws_completed = values[(context_name, 'aws')]
            kubernetes_proof, kubernetes_completed = values[(context_name,
                                                             'kubernetes')]
            if (not isinstance(aws_proof, aws_attestation.PodIdentityProof) or
                    not isinstance(
                        kubernetes_proof,
                        kubernetes_attestation.KubernetesContextProof)):
                raise reclaim.ReclaimAttestationError(
                    'A deployment attestation returned an untyped proof.')
            proofs[context_name] = _ContextProof(aws=aws_proof,
                                                 kubernetes=kubernetes_proof)
            oldest_completions[context_name] = min(aws_completed,
                                                   kubernetes_completed)
        return proofs, oldest_completions

    def _decode_aws_proof_summary(
        self,
        summary: Mapping[str, Any],
        *,
        context_name: str,
    ) -> aws_attestation.PodIdentityProof:
        fleet_context = self._bundle.fleet_context(context_name)
        provider_context = self._bundle.provider_context(context_name)
        try:
            proof = aws_attestation.PodIdentityProof(**summary)
            normalized, _ = (
                reserved_fill_reclaim_proofs.canonical_proof_payload)(
                    dataclasses.asdict(proof))
            expected, _ = (
                reserved_fill_reclaim_proofs.canonical_proof_payload)(summary)
        except (TypeError, ValueError,
                reserved_fill_reclaim_proofs.ReclaimProviderProofError
               ) as error:
            raise reclaim.ReclaimAttestationError(
                'The cached AWS provider proof is malformed.') from error
        if normalized != expected:
            raise reclaim.ReclaimAttestationError(
                'The cached AWS provider proof is not exact.')
        expected_role = fleet_context['pod_identity_role_arn']
        expected_count = 0 if expected_role is None else 1
        expected_absence = expected_role is None
        if (type(proof.kubernetes_context) is not str or
                proof.kubernetes_context != context_name or
                type(proof.cluster_arn) is not str or
                proof.cluster_arn != provider_context['eks']['cluster_arn'] or
                type(proof.namespace) is not str or
                proof.namespace != fleet_context['namespace'] or
                type(proof.service_account_name) is not str or
                proof.service_account_name
                != fleet_context['service_account_name'] or
                proof.expected_role_arn != expected_role or
                type(proof.association_count) is not int or
                proof.association_count != expected_count or
                type(proof.identity_absence_proven) is not bool or
                proof.identity_absence_proven is not expected_absence):
            raise reclaim.ReclaimAttestationError(
                'The cached AWS provider proof does not match the exact '
                'reviewed context.')
        return proof

    def _decode_kubernetes_proof_summary(
        self,
        summary: Mapping[str, Any],
        *,
        context_name: str,
    ) -> kubernetes_attestation.KubernetesContextProof:
        fleet_context = self._bundle.fleet_context(context_name)
        provider_context = self._bundle.provider_context(context_name)
        try:
            values = dict(summary)
            raw_topologies = values['resource_flavor_topology_names']
            if type(raw_topologies) is not list:
                raise TypeError
            values['resource_flavor_topology_names'] = tuple(
                tuple(item) for item in raw_topologies)
            raw_nodes = values['node_flavors']
            if type(raw_nodes) is not list:
                raise TypeError
            values['node_flavors'] = tuple(
                kubernetes_attestation.NodeFlavorProof(**item)
                for item in raw_nodes)
            proof = kubernetes_attestation.KubernetesContextProof(**values)
            normalized, _ = (
                reserved_fill_reclaim_proofs.canonical_proof_payload)(
                    dataclasses.asdict(proof))
            expected, _ = (
                reserved_fill_reclaim_proofs.canonical_proof_payload)(summary)
        except (KeyError, TypeError, ValueError,
                reserved_fill_reclaim_proofs.ReclaimProviderProofError
               ) as error:
            raise reclaim.ReclaimAttestationError(
                'The cached Kubernetes provider proof is malformed.') from error
        if normalized != expected:
            raise reclaim.ReclaimAttestationError(
                'The cached Kubernetes provider proof is not exact.')
        admission = fleet_context['kueue_admission']
        managed = admission is not None
        expected_local_queue = (None if admission is None else
                                admission['local_queue_name'])
        expected_cluster_queue = (
            None if admission is None else
            admission['queues']['inference_cluster_queue'])
        expected_topologies = tuple(
            sorted((flavor['name'], flavor['topology_name'])
                   for flavor in provider_context['resource_flavors']))
        expected_nodes = {
            node['flavor']: node for node in provider_context['node_inventory']
        }
        if (type(proof.kubernetes_context) is not str or
                proof.kubernetes_context != context_name or
                type(proof.physical_cluster_uid) is not str or
                proof.physical_cluster_uid
                != fleet_context['physical_cluster_uid'] or
                type(proof.namespace_uid) is not str or
                proof.namespace_uid != provider_context['namespace_uid'] or
                type(proof.kueue_managed) is not bool or
                proof.kueue_managed is not managed or
                proof.local_queue_name != expected_local_queue or
                proof.cluster_queue_name != expected_cluster_queue or
                type(proof.pod_identity_irsa_annotation_absent) is not bool or
                not proof.pod_identity_irsa_annotation_absent or
                proof.assign_queue_labels_for_pods
                != (True if managed else None) or
            (proof.assign_queue_labels_for_pods is not None and
             type(proof.assign_queue_labels_for_pods) is not bool) or
                proof.topology_aware_scheduling != (True if managed else None)
                or (proof.topology_aware_scheduling is not None and
                    type(proof.topology_aware_scheduling) is not bool) or
                type(proof.custom_scheduler_deployment_proven) is not bool or
                proof.custom_scheduler_deployment_proven
                is not (provider_context['scheduler'] is not None) or
                proof.resource_flavor_topology_names != expected_topologies or
                tuple(node.flavor for node in proof.node_flavors) != tuple(
                    sorted(expected_nodes))):
            raise reclaim.ReclaimAttestationError(
                'The cached Kubernetes provider proof does not match the '
                'exact reviewed context.')
        for node in proof.node_flavors:
            expected_node = expected_nodes[node.flavor]
            if (type(node.flavor) is not str or
                    type(node.non_deleting_node_count) is not int or
                    node.non_deleting_node_count <= 0 or
                    type(node.product_label_value) is not str or
                    node.product_label_value
                    != expected_node['product_label_value'] or
                    type(node.resource_name) is not str or
                    node.resource_name != expected_node['resource_name'] or
                    type(node.capacity_per_node) is not int or
                    node.capacity_per_node
                    != expected_node['capacity_per_node']):
                raise reclaim.ReclaimAttestationError(
                    'The cached Kubernetes provider proof has an invalid '
                    'reviewed Node flavor.')
        return proof

    def _decode_context_proof_summary(
        self,
        summary: Mapping[str, Any],
        *,
        context_name: str,
    ) -> _ContextProof:
        if (type(summary) is not dict or
                set(summary) != {'aws', 'kubernetes'} or
                type(summary['aws']) is not dict or
                type(summary['kubernetes']) is not dict):
            raise reclaim.ReclaimAttestationError(
                'The cached context-wide provider proof is malformed.')
        return _ContextProof(aws=self._decode_aws_proof_summary(
            summary['aws'], context_name=context_name),
                             kubernetes=self._decode_kubernetes_proof_summary(
                                 summary['kubernetes'],
                                 context_name=context_name))

    def _validate_context_summary(self, context_name: str,
                                  summary: Mapping[str, Any]) -> bool:
        try:
            self._decode_context_proof_summary(summary,
                                               context_name=context_name)
        except reclaim.ReclaimAttestationError:
            return False
        return True

    def _prove_context(
        self,
        context_name: str,
        deadline_monotonic: float,
    ) -> reserved_fill_reclaim_proofs.ReclaimProviderProofCandidate:
        proofs, oldest_completions = self._attest_contexts((context_name,),
                                                           deadline_monotonic)
        proof = proofs[context_name]
        return reserved_fill_reclaim_proofs.ReclaimProviderProofCandidate(
            proof_payload={
                'aws': dataclasses.asdict(proof.aws),
                'kubernetes': dataclasses.asdict(proof.kubernetes),
            },
            oldest_completed_monotonic=oldest_completions[context_name])

    def _read_launch_context(
        self,
        context_name: str,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        deadline_monotonic: float,
    ) -> tuple[_ContextProof, reclaim.ReclaimProviderProofReference]:
        """Read one receipt inside the launch handler's disposable boundary."""
        self._require_deadline(deadline_monotonic)
        try:
            repository = (
                reserved_fill_reclaim_proofs.ReclaimProviderProofRepository)()
            receipt = repository.get_fresh(
                identity=identity,
                gate_generation=gate_generation,
                kubernetes_context=context_name,
                deadline_monotonic=deadline_monotonic,
                validate=lambda summary: self._validate_context_summary(
                    context_name, summary),
                minimum_remaining_seconds=(
                    reclaim.PROVIDER_PROOF_CONSUMER_MIN_REMAINING_SECONDS))
            self._require_deadline(deadline_monotonic)
        except Exception:
            raise reclaim.ReclaimAttestationError(
                'The Boltz deployment has no fresh exact reclaim-provider '
                'receipt for this launch.') from None
        proof = self._decode_context_proof_summary(receipt.proof_payload,
                                                   context_name=context_name)
        return proof, receipt.reference

    # Kept as the policy's narrow test/extension seam. Despite the historical
    # name it now attests only a PostgreSQL receipt and performs no provider
    # reads from the launch handler.
    def _attest_launch_context(
        self,
        context_name: str,
        identity: reclaim.ReclaimPolicyIdentity,
        gate_generation: int,
        deadline_monotonic: float,
    ) -> tuple[_ContextProof, reclaim.ReclaimProviderProofReference]:
        return self._read_launch_context(context_name, identity,
                                         gate_generation, deadline_monotonic)

    def _proof_payload(self, operation: str, proofs: Mapping[str,
                                                             _ContextProof],
                       completed_monotonic: float,
                       **fields: object) -> dict[str, Any]:
        identity = self.policy_identity()
        context_proofs = []
        for context_name in sorted(proofs):
            proof = proofs[context_name]
            context_proofs.append({
                'kubernetes_context': context_name,
                'aws': dataclasses.asdict(proof.aws),
                'kubernetes': dataclasses.asdict(proof.kubernetes),
            })
        return {
            'schema_version': PROOF_SCHEMA_VERSION,
            'operation': operation,
            'success': True,
            'contract': self.enforcement_contract().value,
            'identity': dataclasses.asdict(identity),
            'completed_monotonic': completed_monotonic,
            'contexts': context_proofs,
            **fields,
        }

    @staticmethod
    def _emit_proof(payload: Mapping[str, Any]) -> None:
        logger.info(
            'reserved_fill_reclaim_proof=%s',
            json.dumps(payload,
                       sort_keys=True,
                       separators=(',', ':'),
                       allow_nan=False))

    def preflight(self,
                  *,
                  deadline_monotonic: float,
                  emit_log: bool = True) -> dict[str, Any]:
        """Run the full-fleet proof and return its stable JSON-ready record."""
        proofs, _ = self._attest_contexts(self._bundle.contexts,
                                          deadline_monotonic)
        completed = time.monotonic()
        self._require_deadline(deadline_monotonic)
        payload = self._proof_payload('preflight', proofs, completed)
        if emit_log:
            self._emit_proof(payload)
        return payload

    def attest_activation(
        self,
        claimed_contexts: tuple[reclaim.ReservedContextClaim, ...],
        *,
        writer_image_digest: str,
        deadline_monotonic: float,
    ) -> reclaim.ReclaimEnforcementEvidence:
        if (type(claimed_contexts) is not tuple or
                tuple(sorted(set(claimed_contexts))) != claimed_contexts):
            raise reclaim.ReclaimAttestationError(
                'Activation claims must be a unique sorted tuple.')
        if (_IMAGE_DIGEST_RE.fullmatch(writer_image_digest) is None):
            raise reclaim.ReclaimAttestationError(
                'Activation requires an immutable writer image digest.')
        self._require_activation_claims(claimed_contexts)
        # Activation always proves the whole static fleet, including contexts
        # with no current claim, so the one-way gate authorizes future claims.
        proofs, _ = self._attest_contexts(self._bundle.contexts,
                                          deadline_monotonic)
        completed = time.monotonic()
        self._require_deadline(deadline_monotonic)
        evidence = reclaim.ReclaimEnforcementEvidence(
            contract=self.enforcement_contract(),
            fleet_bundle_sha256=self._bundle.fleet_bundle_sha256,
            policy_revision=self._bundle.policy_revision,
            provider_inventory_sha256=(self._bundle.provider_inventory_sha256),
            claimed_contexts=claimed_contexts,
            completed_monotonic=completed)
        payload = self._proof_payload('activation',
                                      proofs,
                                      completed,
                                      writer_image_digest=writer_image_digest,
                                      current_claim_count=len(claimed_contexts))
        self._emit_proof(payload)
        return evidence

    def authorize_claim_set(
        self,
        scope: reclaim.ReclaimClaimSetScope,
        *,
        expected_identity: reclaim.ReclaimPolicyIdentity,
        expected_gate_generation: int,
        deadline_monotonic: float,
    ) -> reclaim.ReclaimClaimAuthorization:
        self._require_deadline(deadline_monotonic)
        self._require_identity(expected_identity, expected_gate_generation)
        if not isinstance(scope, reclaim.ReclaimClaimSetScope):
            raise reclaim.ReclaimAttestationError(
                'The claim authorization scope is not typed.')
        context_names = self._require_claim_edges(scope.edges)
        proofs = {}
        for context_name in context_names:
            proof, _ = self._read_launch_context(context_name,
                                                 expected_identity,
                                                 expected_gate_generation,
                                                 deadline_monotonic)
            proofs[context_name] = proof
        completed = time.monotonic()
        self._require_deadline(deadline_monotonic)
        authorization = reclaim.ReclaimClaimAuthorization(
            identity=self.policy_identity(),
            gate_generation=expected_gate_generation,
            scope=scope,
            completed_monotonic=completed)
        payload = self._proof_payload('claim',
                                      proofs,
                                      completed,
                                      gate_generation=expected_gate_generation,
                                      service_name=scope.service_name,
                                      service_version=scope.service_version,
                                      semantic_hash=scope.semantic_hash)
        self._emit_proof(payload)
        return authorization

    def renew_provider_proofs(
        self,
        *,
        expected_identity: reclaim.ReclaimPolicyIdentity,
        expected_gate_generation: int,
        deadline_monotonic: float,
    ) -> bool:
        """Proactively refresh every bundle context outside launch handlers."""
        self._require_deadline(deadline_monotonic)
        self._require_identity(expected_identity, expected_gate_generation)
        repository = (
            reserved_fill_reclaim_proofs.ReclaimProviderProofRepository)()
        provider_deadline = (reserved_fill_reclaim_proofs.
                             provider_proof_deadline(deadline_monotonic))

        def _renew_context(
            context_name: str,
        ) -> reserved_fill_reclaim_proofs.ReclaimProviderProofReceipt:
            return repository.renew(
                identity=expected_identity,
                gate_generation=expected_gate_generation,
                kubernetes_context=context_name,
                deadline_monotonic=deadline_monotonic,
                prove=lambda: self._prove_context(context_name,
                                                  provider_deadline),
                validate=lambda summary: self._validate_context_summary(
                    context_name, summary),
                minimum_remaining_seconds=(
                    reclaim.PROVIDER_PROOF_RENEW_MIN_REMAINING_SECONDS))

        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self._bundle.contexts),
            thread_name_prefix='boltz-reclaim-renew')
        futures = {
            context_name: executor.submit(_renew_context, context_name)
            for context_name in self._bundle.contexts
        }
        try:
            receipts = {}
            failures = []
            pending = set(futures.values())
            contexts_by_future = {
                future: context_name
                for context_name, future in futures.items()
            }
            while pending:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError
                completed, pending = concurrent.futures.wait(
                    pending,
                    timeout=remaining,
                    return_when=concurrent.futures.FIRST_COMPLETED)
                if not completed:
                    raise TimeoutError
                nonconformance = None
                for future in completed:
                    context_name = contexts_by_future[future]
                    try:
                        receipts[context_name] = future.result()
                    except reclaim.ReclaimProviderNonconformanceError as error:
                        # Each context renews and commits invalidation inside
                        # its own PostgreSQL transaction. One completed exact
                        # negative therefore dominates immediately, even when
                        # another context's provider call ignores its deadline.
                        nonconformance = error
                    except Exception as error:  # pylint: disable=broad-except
                        # Retain indeterminate failures until every context
                        # finishes or the common deadline expires: a later
                        # committed negative must still win.
                        failures.append(error)
                if nonconformance is not None:
                    for future in pending:
                        future.cancel()
                    raise nonconformance
            if failures:
                raise failures[0]
            self._require_deadline(deadline_monotonic)
        except reclaim.ReclaimProviderNonconformanceError:
            raise
        except Exception:
            raise reclaim.ReclaimAttestationError(
                'The Boltz deployment could not renew the exact reclaim '
                'provider inventory before its deadline.') from None
        finally:
            for future in futures.values():
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
        proofs = {
            context_name: self._decode_context_proof_summary(
                receipt.proof_payload, context_name=context_name)
            for context_name, receipt in receipts.items()
        }
        observed_fresh_publication = any(
            receipt.publication_observed for receipt in receipts.values())
        if observed_fresh_publication:
            self._emit_proof(
                self._proof_payload('renewal',
                                    proofs,
                                    time.monotonic(),
                                    gate_generation=expected_gate_generation))
        return observed_fresh_publication

    def authorize_launch(
        self,
        scope: reclaim.ReclaimLaunchScope,
        *,
        expected_identity: reclaim.ReclaimPolicyIdentity,
        expected_gate_generation: int,
        deadline_monotonic: float,
    ) -> reclaim.ReclaimLaunchAuthorization:
        self._require_deadline(deadline_monotonic)
        self._require_identity(expected_identity, expected_gate_generation)
        if not isinstance(scope, reclaim.ReclaimLaunchScope):
            raise reclaim.ReclaimAttestationError(
                'The launch authorization scope is not typed.')
        context = self._require_admission(scope.projected_admission)
        if (scope.kubernetes_context != context['kubernetes_context'] or
                scope.physical_cluster_uid != context['physical_cluster_uid']):
            raise reclaim.ReclaimAttestationError(
                'The launch does not target the reviewed physical context.')
        self._require_launch_pool_key(scope.pool_key,
                                      scope.physical_cluster_uid,
                                      scope.accelerator,
                                      set(context['accelerators']))
        proof, reference = self._attest_launch_context(
            scope.kubernetes_context, expected_identity,
            expected_gate_generation, deadline_monotonic)
        completed = time.monotonic()
        self._require_deadline(deadline_monotonic)
        authorization = reclaim.ReclaimLaunchAuthorization(
            identity=self.policy_identity(),
            gate_generation=expected_gate_generation,
            scope=scope,
            provider_proof_reference=reference,
            completed_monotonic=completed)
        payload = self._proof_payload('launch',
                                      {scope.kubernetes_context: proof},
                                      completed,
                                      gate_generation=expected_gate_generation,
                                      service_name=scope.service_name,
                                      service_version=scope.service_version,
                                      accelerator=scope.accelerator,
                                      accelerator_count=scope.accelerator_count)
        self._emit_proof(payload)
        return authorization
