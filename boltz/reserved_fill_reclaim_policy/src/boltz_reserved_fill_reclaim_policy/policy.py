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

logger = logging.getLogger(__name__)

_IMAGE_DIGEST_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_PROOF_SCHEMA_VERSION = 1


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
                admission.local_queue_name != context['local_queue_name'] or
                admission.workload_priority_class_name
                != context['workload_priority_class_name'] or
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
            return kubernetes_attestation.attest_context(
                fleet_context,
                provider_context,
                deadline_monotonic=deadline_monotonic,
                cancellation=cancellation)
        raise AssertionError(f'Unknown provider domain: {domain}')

    def _attest_contexts(
        self,
        context_names: Sequence[str],
        deadline_monotonic: float,
    ) -> dict[str, _ContextProof]:
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
        futures: dict[tuple[str, str], concurrent.futures.Future[object]] = {}
        try:
            for context_name in canonical_contexts:
                for domain in ('aws', 'kubernetes'):
                    futures[(context_name, domain)] = executor.submit(
                        self._provider_job, context_name, domain,
                        deadline_monotonic, cancellation)
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            done, pending = concurrent.futures.wait(
                futures.values(),
                timeout=remaining,
                return_when=concurrent.futures.ALL_COMPLETED)
            if pending or len(done) != len(futures):
                raise TimeoutError
            values = {key: future.result() for key, future in futures.items()}
            self._require_deadline(deadline_monotonic)
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
        for context_name in canonical_contexts:
            aws_proof = values[(context_name, 'aws')]
            kubernetes_proof = values[(context_name, 'kubernetes')]
            if (not isinstance(aws_proof, aws_attestation.PodIdentityProof) or
                    not isinstance(
                        kubernetes_proof,
                        kubernetes_attestation.KubernetesContextProof)):
                raise reclaim.ReclaimAttestationError(
                    'A deployment attestation returned an untyped proof.')
            proofs[context_name] = _ContextProof(aws=aws_proof,
                                                 kubernetes=kubernetes_proof)
        return proofs

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
            'schema_version': _PROOF_SCHEMA_VERSION,
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
        proofs = self._attest_contexts(self._bundle.contexts,
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
        self._require_claim_edges(claimed_contexts)
        # Activation always proves the whole static fleet, including contexts
        # with no current claim, so the one-way gate authorizes future claims.
        proofs = self._attest_contexts(self._bundle.contexts,
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
        proofs = self._attest_contexts(context_names, deadline_monotonic)
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
        proofs = self._attest_contexts((scope.kubernetes_context,),
                                       deadline_monotonic)
        completed = time.monotonic()
        self._require_deadline(deadline_monotonic)
        authorization = reclaim.ReclaimLaunchAuthorization(
            identity=self.policy_identity(),
            gate_generation=expected_gate_generation,
            scope=scope,
            completed_monotonic=completed)
        payload = self._proof_payload('launch',
                                      proofs,
                                      completed,
                                      gate_generation=expected_gate_generation,
                                      service_name=scope.service_name,
                                      service_version=scope.service_version,
                                      accelerator=scope.accelerator,
                                      accelerator_count=scope.accelerator_count)
        self._emit_proof(payload)
        return authorization
