"""Closed Serve039 authority-preflight wire values.

This module is intentionally additive.  It imports the frozen provider leaf
contracts and the Serve039 authority identity contracts, but neither of those
modules imports this one.  In particular, no V1 preflight envelope, seed,
capsule, cohort, worker identity, or response is accepted or converted here.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import re
from typing import Any, ClassVar, TypeVar
import unicodedata
import uuid

from sky.serve import resource_action_authority
from sky.serve import resource_action_cleanup_v2
from sky.serve import resource_actions
from sky.server.requests import resource_actions as kernel_actions

_MAX_CANONICAL_BYTES = 65_536
_MAX_TEXT_BYTES = 1_024
_MAX_SHORT_TEXT_BYTES = 253
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_CONTRACT = 'provider_kubernetes_preflight_v2'

JsonObject = dict[str, Any]
_EnumT = TypeVar('_EnumT', bound=enum.Enum)


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> JsonObject:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if any(type(key) is not str for key in value):
        raise TypeError(f'{name} keys must be text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    encoded = resource_actions.canonical_json_bytes(value)
    if len(encoded) > _MAX_CANONICAL_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_CANONICAL_BYTES} bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if normalized != value:
        raise ValueError(f'{name} is not canonical.')
    return normalized


def _version_two(value: Any, *, name: str) -> int:
    if type(value) is not int or value != 2:
        raise ValueError(f'{name} must be integer 2.')
    return value


def _text(value: Any,
          *,
          name: str,
          maximum_bytes: int = _MAX_TEXT_BYTES) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        size = len(value.encode('utf-8'))
    except UnicodeEncodeError as e:
        raise ValueError(f'{name} must be valid UTF-8 text.') from e
    if (size == 0 or size > maximum_bytes or '\x00' in value or
            unicodedata.normalize('NFC', value) != value):
        raise ValueError(
            f'{name} must be 1..{maximum_bytes} canonical UTF-8 bytes.')
    return value


def _sha256(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be a UUID or canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as e:
        raise ValueError(f'{name} must be a UUID.') from e
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


def _boolean(value: Any, *, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f'{name} must be a Boolean.')
    return value


def _nonnegative_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value < 0 or value > 2**63 - 1:
        raise ValueError(f'{name} must be a nonnegative signed-int64 integer.')
    return value


def _enum_value(enum_type: type[_EnumT], value: Any, *, name: str) -> _EnumT:
    if type(value) is enum_type:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        parsed = enum_type(value)
    except ValueError as e:
        raise ValueError(f'{name} is unsupported.') from e
    if parsed.value != value:
        raise ValueError(f'{name} is not canonical.')
    return parsed


def _action_kind(value: Any, *, name: str) -> kernel_actions.ActionKind:
    if type(value) is kernel_actions.ActionKind:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        return kernel_actions.ActionKind(value)
    except ValueError as e:
        raise ValueError(f'{name} is unsupported.') from e


@dataclasses.dataclass(frozen=True)
class ProviderLaunchPreflightSeedV2(resource_action_authority.CanonicalContract
                                   ):
    """Complete controller-owned launch input for live M4 preflight."""

    version: int
    resource_identity: resource_actions.ProviderResourceIdentityV1
    workspace: str
    source: resource_actions.ProviderLaunchSourceV1
    requested_target: resource_actions.ProviderLocatorV1
    requested_cloud: str
    context_mode: str
    target_namespace: str
    resources: resource_actions.ProviderPodResourceSnapshotV1
    topology: resource_actions.ProviderPodTopologyV1
    replica_id: int
    retry_until_up: bool
    request_identity: resource_actions.ProviderKubernetesRequestIdentityV1
    config_projection: resource_actions.ProviderKubernetesConfigProjectionV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'resource_identity', 'workspace', 'source',
        'requested_target', 'requested_cloud', 'context_mode',
        'target_namespace', 'resources', 'topology', 'replica_id',
        'retry_until_up', 'request_identity', 'config_projection'
    })

    def __post_init__(self) -> None:
        _version_two(self.version, name='launch preflight seed V2 version')
        for field, expected_type in (
            ('resource_identity', resource_actions.ProviderResourceIdentityV1),
            ('source', resource_actions.ProviderLaunchSourceV1),
            ('requested_target', resource_actions.ProviderLocatorV1),
            ('resources', resource_actions.ProviderPodResourceSnapshotV1),
            ('topology', resource_actions.ProviderPodTopologyV1),
            ('request_identity',
             resource_actions.ProviderKubernetesRequestIdentityV1),
            ('config_projection',
             resource_actions.ProviderKubernetesConfigProjectionV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'launch preflight seed V2 {field} has an '
                                'invalid type.')
        workspace = _text(self.workspace,
                          name='launch_preflight_seed_v2.workspace')
        namespace = _text(self.target_namespace,
                          name='launch_preflight_seed_v2.target_namespace',
                          maximum_bytes=_MAX_SHORT_TEXT_BYTES)
        object.__setattr__(self, 'workspace', workspace)
        object.__setattr__(self, 'target_namespace', namespace)
        if self.requested_cloud != 'kubernetes':
            raise ValueError('launch preflight seed V2 cloud must be '
                             'kubernetes.')
        if self.context_mode != 'in_cluster':
            raise ValueError('launch preflight seed V2 context must be '
                             'in_cluster.')
        replica_id = _nonnegative_integer(
            self.replica_id, name='launch_preflight_seed_v2.replica_id')
        object.__setattr__(self, 'replica_id', replica_id)
        _boolean(self.retry_until_up,
                 name='launch_preflight_seed_v2.retry_until_up')
        if not self.requested_target.is_authoritative_pod_locator:
            raise ValueError('launch preflight seed V2 requires an '
                             'authoritative Kubernetes Pod target.')
        kubernetes = self.requested_target.kubernetes
        assert kubernetes is not None
        proof = self.source.identity_canonicalization
        if proof.context.input.resource_identity.canonical_bytes != (
                self.resource_identity.canonical_bytes):
            raise ValueError('launch preflight seed V2 identity differs from '
                             'its retained source proof.')
        projected_identity = (
            resource_actions.project_provider_kubernetes_request_identity_v1(
                proof.effective_original_user, kubernetes.name_basis))
        if (self.source.content.workspace != workspace or
                self.source.content.service_incarnation
                != self.resource_identity.service_incarnation or
                proof.effective_user_hash
                != kubernetes.name_basis.frozen_user_hash or
                projected_identity.canonical_bytes
                != self.request_identity.canonical_bytes):
            raise ValueError('launch preflight seed V2 source or request '
                             'identity projection does not match.')
        if (replica_id != self.resource_identity.replica_id or
                kubernetes.replica_incarnation_label != str(
                    self.resource_identity.replica_incarnation)):
            raise ValueError('launch preflight seed V2 replica identity does '
                             'not match its target.')
        if (namespace != kubernetes.namespace or
                namespace != self.resources.namespace or
                namespace != self.config_projection.target_namespace or
                self.resources.cluster_fingerprint_sha256
                != kubernetes.cluster_fingerprint_sha256 or
                self.topology.canonical_bytes
                != kubernetes.topology.canonical_bytes or
                self.resources.ports != self.topology.resources_ports):
            raise ValueError('launch preflight seed V2 target, resources, '
                             'topology, or namespace does not match.')
        if (self.config_projection.workspace != workspace or
                self.config_projection.context_mode != self.context_mode):
            raise ValueError('launch preflight seed V2 configuration '
                             'projection does not match.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchPreflightSeedV2:
        raw = _closed_object(value,
                             name='launch preflight seed V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            resource_identity=resource_actions.ProviderResourceIdentityV1.
            from_value(raw['resource_identity']),
            workspace=raw['workspace'],
            source=resource_actions.ProviderLaunchSourceV1.from_value(
                raw['source']),
            requested_target=resource_actions.ProviderLocatorV1.from_value(
                raw['requested_target']),
            requested_cloud=raw['requested_cloud'],
            context_mode=raw['context_mode'],
            target_namespace=raw['target_namespace'],
            resources=resource_actions.ProviderPodResourceSnapshotV1.from_value(
                raw['resources']),
            topology=resource_actions.ProviderPodTopologyV1.from_value(
                raw['topology']),
            replica_id=raw['replica_id'],
            retry_until_up=raw['retry_until_up'],
            request_identity=(
                resource_actions.ProviderKubernetesRequestIdentityV1.from_value(
                    raw['request_identity'])),
            config_projection=(resource_actions.
                               ProviderKubernetesConfigProjectionV1.from_value(
                                   raw['config_projection'])))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'resource_identity': self.resource_identity.canonical_value(),
            'workspace': self.workspace,
            'source': self.source.canonical_value(),
            'requested_target': self.requested_target.canonical_value(),
            'requested_cloud': 'kubernetes',
            'context_mode': 'in_cluster',
            'target_namespace': self.target_namespace,
            'resources': self.resources.canonical_value(),
            'topology': self.topology.canonical_value(),
            'replica_id': self.replica_id,
            'retry_until_up': self.retry_until_up,
            'request_identity': self.request_identity.canonical_value(),
            'config_projection': self.config_projection.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderDownPreflightSeedV2(resource_action_authority.CanonicalContract):
    """Complete controller-owned down input for live M4 preflight."""

    version: int
    resource_identity: resource_actions.ProviderResourceIdentityV1
    workspace: str
    requested_target: resource_actions.ProviderLocatorV1
    prior_launch_basis: resource_actions.PriorLaunchBasisV1
    prior_launch_basis_sha256: str
    cleanup_target: resource_actions.ProviderKubernetesCleanupTargetV1
    cleanup_target_sha256: str
    context_mode: str
    config_projection: resource_actions.ProviderKubernetesConfigProjectionV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'resource_identity', 'workspace', 'requested_target',
        'prior_launch_basis', 'prior_launch_basis_sha256', 'cleanup_target',
        'cleanup_target_sha256', 'context_mode', 'config_projection'
    })

    def __post_init__(self) -> None:
        _version_two(self.version, name='down preflight seed V2 version')
        for field, expected_types in (
            ('resource_identity',
             (resource_actions.ProviderResourceIdentityV1,)),
            ('requested_target', (resource_actions.ProviderLocatorV1,)),
            ('prior_launch_basis',
             (resource_actions.CompletedLaunchBasisV1,
              resource_actions.PartialLaunchCleanupBasisV1)),
            ('cleanup_target',
             (resource_actions.ProviderKubernetesCleanupTargetV1,)),
            ('config_projection',
             (resource_actions.ProviderKubernetesConfigProjectionV1,)),
        ):
            if type(getattr(self, field)) not in expected_types:
                raise TypeError(f'down preflight seed V2 {field} has an '
                                'invalid type.')
        workspace = _text(self.workspace,
                          name='down_preflight_seed_v2.workspace')
        object.__setattr__(self, 'workspace', workspace)
        if self.context_mode != 'in_cluster':
            raise ValueError('down preflight seed V2 context must be '
                             'in_cluster.')
        basis_hash = _sha256(
            self.prior_launch_basis_sha256,
            name='down_preflight_seed_v2.prior_launch_basis_sha256')
        cleanup_hash = _sha256(
            self.cleanup_target_sha256,
            name='down_preflight_seed_v2.cleanup_target_sha256')
        object.__setattr__(self, 'prior_launch_basis_sha256', basis_hash)
        object.__setattr__(self, 'cleanup_target_sha256', cleanup_hash)
        if basis_hash != self.prior_launch_basis.sha256:
            raise ValueError('down preflight seed V2 prior basis hash does '
                             'not match its complete preimage.')
        if cleanup_hash != self.cleanup_target.sha256:
            raise ValueError('down preflight seed V2 cleanup hash does not '
                             'match its complete preimage.')
        resource_action_cleanup_v2.validate_provider_kubernetes_cleanup_target_binding_v2(
            self.prior_launch_basis, self.cleanup_target)
        if not self.requested_target.is_authoritative_pod_locator:
            raise ValueError('down preflight seed V2 requires an '
                             'authoritative Kubernetes Pod target.')
        if self.requested_target.canonical_bytes != (
                self.prior_launch_basis.launch_requested_target.canonical_bytes
        ):
            raise ValueError('down preflight seed V2 target differs from its '
                             'prior launch basis.')
        prior_identity = self.prior_launch_basis.launch_resource_identity
        stable_fields = ('service_hash', 'service_incarnation', 'replica_id',
                         'replica_incarnation')
        if any(
                getattr(self.resource_identity, field) != getattr(
                    prior_identity, field) for field in stable_fields):
            raise ValueError('down preflight seed V2 identity differs from '
                             'its prior launch identity.')
        if self.resource_identity.desired_generation != (
                prior_identity.desired_generation + 1):
            raise ValueError('down preflight seed V2 generation must '
                             'immediately follow launch.')
        kubernetes = self.requested_target.kubernetes
        assert kubernetes is not None
        if (workspace
                != self.prior_launch_basis.launch_workspace_identity.workspace
                or self.config_projection.workspace != workspace or
                self.config_projection.context_mode != self.context_mode or
                self.config_projection.target_namespace
                != kubernetes.namespace):
            raise ValueError('down preflight seed V2 workspace, target, or '
                             'configuration projection does not match.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderDownPreflightSeedV2:
        raw = _closed_object(value,
                             name='down preflight seed V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            resource_identity=resource_actions.ProviderResourceIdentityV1.
            from_value(raw['resource_identity']),
            workspace=raw['workspace'],
            requested_target=resource_actions.ProviderLocatorV1.from_value(
                raw['requested_target']),
            prior_launch_basis=resource_actions.
            prior_launch_basis_from_value_v1(raw['prior_launch_basis']),
            prior_launch_basis_sha256=raw['prior_launch_basis_sha256'],
            cleanup_target=(
                resource_actions.ProviderKubernetesCleanupTargetV1.from_value(
                    raw['cleanup_target'])),
            cleanup_target_sha256=raw['cleanup_target_sha256'],
            context_mode=raw['context_mode'],
            config_projection=(resource_actions.
                               ProviderKubernetesConfigProjectionV1.from_value(
                                   raw['config_projection'])))

    def canonical_value(self) -> JsonObject:
        return {
            'version': 2,
            'resource_identity': self.resource_identity.canonical_value(),
            'workspace': self.workspace,
            'requested_target': self.requested_target.canonical_value(),
            'prior_launch_basis': self.prior_launch_basis.canonical_value(),
            'prior_launch_basis_sha256': self.prior_launch_basis_sha256,
            'cleanup_target': self.cleanup_target.canonical_value(),
            'cleanup_target_sha256': self.cleanup_target_sha256,
            'context_mode': 'in_cluster',
            'config_projection': self.config_projection.canonical_value(),
        }


ProviderLifecyclePreflightSeedV2 = (ProviderLaunchPreflightSeedV2 |
                                    ProviderDownPreflightSeedV2)


def provider_lifecycle_preflight_seed_from_value_v2(
    value: Any,
    action_kind: kernel_actions.ActionKind | str,
) -> ProviderLifecyclePreflightSeedV2:
    """Decode only the V2 seed selected by the outer action kind."""

    kind = _action_kind(action_kind, name='preflight request V2 action_kind')
    if kind is kernel_actions.ActionKind.LAUNCH:
        return ProviderLaunchPreflightSeedV2.from_value(value)
    return ProviderDownPreflightSeedV2.from_value(value)


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityPreflightRequestV2(
        resource_action_authority.CanonicalContract):
    """Nonce-bound live M4 preflight request."""

    version: int
    contract: str
    action_kind: kernel_actions.ActionKind
    nonce: uuid.UUID
    seed: ProviderLifecyclePreflightSeedV2
    expected_cohort_manifest: resource_action_authority.ProviderAuthorityWorkerCohortManifestV2
    request_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'contract', 'action_kind', 'nonce', 'seed',
        'expected_cohort_manifest', 'request_sha256'
    })

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='authority preflight request V2 version')
        if self.contract != _CONTRACT:
            raise ValueError('authority preflight request V2 contract is '
                             'unsupported.')
        kind = _action_kind(self.action_kind,
                            name='authority preflight request V2 action_kind')
        object.__setattr__(self, 'action_kind', kind)
        object.__setattr__(
            self, 'nonce',
            _uuid(self.nonce, name='authority preflight request V2 nonce'))
        seed_type = (ProviderLaunchPreflightSeedV2
                     if kind is kernel_actions.ActionKind.LAUNCH else
                     ProviderDownPreflightSeedV2)
        if type(self.seed) is not seed_type:
            raise TypeError('authority preflight request V2 seed has the '
                            'wrong action-kind type.')
        if type(self.expected_cohort_manifest) is not (
                resource_action_authority.
                ProviderAuthorityWorkerCohortManifestV2):
            raise TypeError('authority preflight request V2 manifest has an '
                            'invalid type.')
        if kind is kernel_actions.ActionKind.LAUNCH:
            assert type(self.seed) is ProviderLaunchPreflightSeedV2
            if (self.seed.source.identity_canonicalization.context.cohort_id
                    != self.expected_cohort_manifest.cohort_id):
                raise ValueError('launch preflight request V2 source proof '
                                 'does not name its expected cohort.')
        request_hash = _sha256(
            self.request_sha256,
            name='authority preflight request V2 request_sha256')
        object.__setattr__(self, 'request_sha256', request_hash)
        if request_hash != resource_actions.canonical_sha256(
                self.preimage_value()):
            raise ValueError('authority preflight request V2 hash does not '
                             'match its complete preimage.')
        _ = self.canonical_bytes

    @classmethod
    def create(
        cls,
        *,
        action_kind: kernel_actions.ActionKind | str,
        nonce: uuid.UUID | str,
        seed: ProviderLifecyclePreflightSeedV2,
        expected_cohort_manifest: resource_action_authority.
        ProviderAuthorityWorkerCohortManifestV2,
    ) -> ProviderAuthorityPreflightRequestV2:
        kind = _action_kind(action_kind,
                            name='authority preflight request V2 action_kind')
        parsed_nonce = _uuid(nonce, name='authority preflight request V2 nonce')
        preimage: JsonObject = {
            'version': 2,
            'contract': _CONTRACT,
            'action_kind': kind.value,
            'nonce': str(parsed_nonce),
            'seed': seed.canonical_value(),
            'expected_cohort_manifest':
                expected_cohort_manifest.canonical_value(),
        }
        return cls(version=2,
                   contract=_CONTRACT,
                   action_kind=kind,
                   nonce=parsed_nonce,
                   seed=seed,
                   expected_cohort_manifest=expected_cohort_manifest,
                   request_sha256=resource_actions.canonical_sha256(preimage))

    @classmethod
    def from_value(cls, value: Any) -> ProviderAuthorityPreflightRequestV2:
        raw = _closed_object(value,
                             name='authority preflight request V2',
                             keys=cls._KEYS)
        kind = _action_kind(raw['action_kind'],
                            name='authority preflight request V2.action_kind')
        return cls(version=raw['version'],
                   contract=raw['contract'],
                   action_kind=kind,
                   nonce=raw['nonce'],
                   seed=provider_lifecycle_preflight_seed_from_value_v2(
                       raw['seed'], kind),
                   expected_cohort_manifest=(
                       resource_action_authority.
                       ProviderAuthorityWorkerCohortManifestV2.from_value(
                           raw['expected_cohort_manifest'])),
                   request_sha256=raw['request_sha256'])

    def preimage_value(self) -> JsonObject:
        return {
            'version': 2,
            'contract': _CONTRACT,
            'action_kind': self.action_kind.value,
            'nonce': str(self.nonce),
            'seed': self.seed.canonical_value(),
            'expected_cohort_manifest':
                self.expected_cohort_manifest.canonical_value(),
        }

    def canonical_value(self) -> JsonObject:
        value = self.preimage_value()
        value['request_sha256'] = self.request_sha256
        return value


class ProviderAuthorityPreflightDispositionV2(str, enum.Enum):
    COMPLETE = 'complete'
    NOT_REPRESENTABLE = 'not_representable'


_RESPONSE_KEYS = frozenset({
    'version', 'contract', 'action_kind', 'nonce', 'request_sha256',
    'disposition', 'reason', 'resolved_cohort', 'execution_capsule',
    'executor_policy_proof', 'worker_identity'
})


def _validate_complete_against_request(
    request: ProviderAuthorityPreflightRequestV2,
    cohort: resource_action_authority.ProviderAuthorityWorkerCohortV2,
    capsule: resource_actions.ProviderKubernetesExecutionCapsuleV2 |
    resource_actions.ProviderKubernetesDownExecutionCapsuleV2,
    proof: resource_actions.ProviderPolicyBoundaryProofV1,
) -> None:
    if cohort.manifest.canonical_bytes != (
            request.expected_cohort_manifest.canonical_bytes):
        raise ValueError('preflight response V2 cohort manifest differs from '
                         'the request.')
    resource_action_authority.validate_locked_action_spec_cohort_v2(
        capsule.executor_cohort, cohort)
    seed = request.seed
    subject: (resource_actions.ProviderLaunchPolicySubjectV1 |
              resource_actions.ProviderDownPolicySubjectV1)
    if request.action_kind is kernel_actions.ActionKind.LAUNCH:
        if (type(seed) is not ProviderLaunchPreflightSeedV2 or type(capsule)
                is not resource_actions.ProviderKubernetesExecutionCapsuleV2):
            raise TypeError('launch preflight response V2 has crossed seed or '
                            'capsule types.')
        if (capsule.config_projection.canonical_bytes
                != seed.config_projection.canonical_bytes or
                capsule.request_identity.canonical_bytes
                != seed.request_identity.canonical_bytes):
            raise ValueError('launch preflight response V2 capsule differs '
                             'from its seed.')
        subject = resource_actions.project_provider_launch_policy_subject_v2(
            seed.resource_identity, seed.source, seed.requested_target,
            seed.resources, seed.topology, seed.replica_id, seed.retry_until_up,
            capsule)
    else:
        if (type(seed) is not ProviderDownPreflightSeedV2 or type(capsule)
                is not resource_actions.ProviderKubernetesDownExecutionCapsuleV2
           ):
            raise TypeError('down preflight response V2 has crossed seed or '
                            'capsule types.')
        if (capsule.config_projection.canonical_bytes
                != seed.config_projection.canonical_bytes or
                capsule.cleanup_target.canonical_bytes
                != seed.cleanup_target.canonical_bytes):
            raise ValueError('down preflight response V2 capsule differs from '
                             'its seed.')
        subject = resource_actions.project_provider_down_policy_subject_v2(
            seed.requested_target, seed.workspace, seed.prior_launch_basis,
            capsule)
    subject_hash = subject.sha256
    if (proof.boundary != 'api_executor_pre_io' or
            proof.config_projection_sha256 != capsule.config_projection_sha256
            or proof.policy_subject_sha256 != subject_hash or
            proof.projection_before_sha256 != subject_hash or
            proof.projection_after_sha256 != subject_hash):
        raise ValueError('preflight response V2 proof does not bind the '
                         'selected capsule and policy subject.')


def _validate_response(
    *,
    expected_kind: kernel_actions.ActionKind,
    action_kind: kernel_actions.ActionKind | str,
    disposition: ProviderAuthorityPreflightDispositionV2 | str,
    reason: resource_actions.ProviderLaunchNotRepresentableReasonV1 |
    resource_actions.ProviderDownNotRepresentableReasonV1 | None,
    resolved_cohort: resource_action_authority.ProviderAuthorityWorkerCohortV2 |
    None,
    execution_capsule: resource_actions.ProviderKubernetesExecutionCapsuleV2 |
    resource_actions.ProviderKubernetesDownExecutionCapsuleV2 | None,
    executor_policy_proof: resource_actions.ProviderPolicyBoundaryProofV1 |
    None,
    worker_identity: resource_action_authority.ProviderAuthorityWorkerIdentityV2
    | None,
) -> ProviderAuthorityPreflightDispositionV2:
    kind = _action_kind(action_kind,
                        name='authority preflight response V2 action_kind')
    if kind is not expected_kind:
        raise ValueError('authority preflight response V2 action kind is '
                         'wrong.')
    parsed = _enum_value(ProviderAuthorityPreflightDispositionV2,
                         disposition,
                         name='authority preflight response V2 disposition')
    reason_type = (resource_actions.ProviderLaunchNotRepresentableReasonV1
                   if kind is kernel_actions.ActionKind.LAUNCH else
                   resource_actions.ProviderDownNotRepresentableReasonV1)
    capsule_type = (resource_actions.ProviderKubernetesExecutionCapsuleV2
                    if kind is kernel_actions.ActionKind.LAUNCH else
                    resource_actions.ProviderKubernetesDownExecutionCapsuleV2)
    evidence = (resolved_cohort, execution_capsule, executor_policy_proof,
                worker_identity)
    if parsed is ProviderAuthorityPreflightDispositionV2.COMPLETE:
        if reason is not None:
            raise ValueError('complete preflight response V2 reason must be '
                             'null.')
        expected_types = (
            resource_action_authority.ProviderAuthorityWorkerCohortV2,
            capsule_type, resource_actions.ProviderPolicyBoundaryProofV1,
            resource_action_authority.ProviderAuthorityWorkerIdentityV2)
        if any(
                type(item) is not expected_type
                for item, expected_type in zip(evidence, expected_types)):
            raise TypeError('complete preflight response V2 requires all four '
                            'kind-matched evidence values.')
        assert resolved_cohort is not None
        assert execution_capsule is not None
        assert executor_policy_proof is not None
        assert worker_identity is not None
        resource_action_authority.validate_locked_action_spec_cohort_v2(
            execution_capsule.executor_cohort, resolved_cohort)
        worker_identity.validate_for_cohort(resolved_cohort)
        if (executor_policy_proof.boundary != 'api_executor_pre_io' or
                executor_policy_proof.config_projection_sha256
                != execution_capsule.config_projection_sha256):
            raise ValueError('complete preflight response V2 proof does not '
                             'bind its selected capsule.')
    else:
        if type(reason) is not reason_type:
            raise TypeError('not-representable preflight response V2 has a '
                            'wrong-kind or absent reason.')
        if any(item is not None for item in evidence):
            raise ValueError('not-representable preflight response V2 '
                             'evidence must be entirely null.')
    return parsed


@dataclasses.dataclass(frozen=True)
class ProviderLaunchAuthorityPreflightResponseV2(
        resource_action_authority.CanonicalContract):
    """Closed live-M4 authority preflight response for launch."""

    version: int
    contract: str
    action_kind: kernel_actions.ActionKind
    nonce: uuid.UUID
    request_sha256: str
    disposition: ProviderAuthorityPreflightDispositionV2
    reason: resource_actions.ProviderLaunchNotRepresentableReasonV1 | None
    resolved_cohort: (resource_action_authority.ProviderAuthorityWorkerCohortV2
                      | None)
    execution_capsule: (resource_actions.ProviderKubernetesExecutionCapsuleV2 |
                        None)
    executor_policy_proof: resource_actions.ProviderPolicyBoundaryProofV1 | None
    worker_identity: (
        resource_action_authority.ProviderAuthorityWorkerIdentityV2 | None)

    def __post_init__(self) -> None:
        _version_two(self.version, name='launch preflight response V2 version')
        if self.contract != _CONTRACT:
            raise ValueError('launch preflight response V2 contract is '
                             'unsupported.')
        object.__setattr__(
            self, 'nonce',
            _uuid(self.nonce, name='launch preflight response V2 nonce'))
        object.__setattr__(
            self, 'request_sha256',
            _sha256(self.request_sha256,
                    name='launch preflight response V2 request_sha256'))
        parsed = _validate_response(
            expected_kind=kernel_actions.ActionKind.LAUNCH,
            action_kind=self.action_kind,
            disposition=self.disposition,
            reason=self.reason,
            resolved_cohort=self.resolved_cohort,
            execution_capsule=self.execution_capsule,
            executor_policy_proof=self.executor_policy_proof,
            worker_identity=self.worker_identity)
        object.__setattr__(self, 'action_kind',
                           kernel_actions.ActionKind.LAUNCH)
        object.__setattr__(self, 'disposition', parsed)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderLaunchAuthorityPreflightResponseV2:
        raw = _closed_object(value,
                             name='launch preflight response V2',
                             keys=_RESPONSE_KEYS)
        reason = (None if raw['reason'] is None else _enum_value(
            resource_actions.ProviderLaunchNotRepresentableReasonV1,
            raw['reason'],
            name='launch preflight response V2.reason'))
        return cls(
            version=raw['version'],
            contract=raw['contract'],
            action_kind=raw['action_kind'],
            nonce=raw['nonce'],
            request_sha256=raw['request_sha256'],
            disposition=raw['disposition'],
            reason=reason,
            resolved_cohort=(None if raw['resolved_cohort'] is None else
                             resource_action_authority.
                             ProviderAuthorityWorkerCohortV2.from_value(
                                 raw['resolved_cohort'])),
            execution_capsule=(None if raw['execution_capsule'] is None else
                               resource_actions.
                               ProviderKubernetesExecutionCapsuleV2.from_value(
                                   raw['execution_capsule'])),
            executor_policy_proof=(
                None if raw['executor_policy_proof'] is None else
                resource_actions.ProviderPolicyBoundaryProofV1.from_value(
                    raw['executor_policy_proof'])),
            worker_identity=(None if raw['worker_identity'] is None else
                             resource_action_authority.
                             ProviderAuthorityWorkerIdentityV2.from_value(
                                 raw['worker_identity'])))

    @classmethod
    def unavailable(
        cls, request: ProviderAuthorityPreflightRequestV2
    ) -> ProviderLaunchAuthorityPreflightResponseV2:
        if (type(request) is not ProviderAuthorityPreflightRequestV2 or
                request.action_kind is not kernel_actions.ActionKind.LAUNCH):
            raise TypeError('launch unavailable V2 response requires a '
                            'launch V2 request.')
        return cls(
            version=2,
            contract=_CONTRACT,
            action_kind=kernel_actions.ActionKind.LAUNCH,
            nonce=request.nonce,
            request_sha256=request.request_sha256,
            disposition=ProviderAuthorityPreflightDispositionV2.
            NOT_REPRESENTABLE,
            reason=resource_actions.ProviderLaunchNotRepresentableReasonV1.
            PREFLIGHT_UNAVAILABLE_OR_INVALID,
            resolved_cohort=None,
            execution_capsule=None,
            executor_policy_proof=None,
            worker_identity=None)

    def validate_request(self,
                         request: ProviderAuthorityPreflightRequestV2) -> None:
        if (type(request) is not ProviderAuthorityPreflightRequestV2 or
                request.action_kind is not kernel_actions.ActionKind.LAUNCH or
                self.nonce != request.nonce or
                self.request_sha256 != request.request_sha256):
            raise ValueError('launch preflight response V2 does not match its '
                             'request envelope.')
        if self.disposition is ProviderAuthorityPreflightDispositionV2.COMPLETE:
            assert self.resolved_cohort is not None
            assert self.execution_capsule is not None
            assert self.executor_policy_proof is not None
            _validate_complete_against_request(request, self.resolved_cohort,
                                               self.execution_capsule,
                                               self.executor_policy_proof)

    def canonical_value(self) -> JsonObject:
        return _response_value(self)


@dataclasses.dataclass(frozen=True)
class ProviderDownAuthorityPreflightResponseV2(
        resource_action_authority.CanonicalContract):
    """Closed live-M4 authority preflight response for down."""

    version: int
    contract: str
    action_kind: kernel_actions.ActionKind
    nonce: uuid.UUID
    request_sha256: str
    disposition: ProviderAuthorityPreflightDispositionV2
    reason: resource_actions.ProviderDownNotRepresentableReasonV1 | None
    resolved_cohort: (resource_action_authority.ProviderAuthorityWorkerCohortV2
                      | None)
    execution_capsule: (
        resource_actions.ProviderKubernetesDownExecutionCapsuleV2 | None)
    executor_policy_proof: resource_actions.ProviderPolicyBoundaryProofV1 | None
    worker_identity: (
        resource_action_authority.ProviderAuthorityWorkerIdentityV2 | None)

    def __post_init__(self) -> None:
        _version_two(self.version, name='down preflight response V2 version')
        if self.contract != _CONTRACT:
            raise ValueError('down preflight response V2 contract is '
                             'unsupported.')
        object.__setattr__(
            self, 'nonce',
            _uuid(self.nonce, name='down preflight response V2 nonce'))
        object.__setattr__(
            self, 'request_sha256',
            _sha256(self.request_sha256,
                    name='down preflight response V2 request_sha256'))
        parsed = _validate_response(
            expected_kind=kernel_actions.ActionKind.DOWN,
            action_kind=self.action_kind,
            disposition=self.disposition,
            reason=self.reason,
            resolved_cohort=self.resolved_cohort,
            execution_capsule=self.execution_capsule,
            executor_policy_proof=self.executor_policy_proof,
            worker_identity=self.worker_identity)
        object.__setattr__(self, 'action_kind', kernel_actions.ActionKind.DOWN)
        object.__setattr__(self, 'disposition', parsed)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderDownAuthorityPreflightResponseV2:
        raw = _closed_object(value,
                             name='down preflight response V2',
                             keys=_RESPONSE_KEYS)
        reason = (None if raw['reason'] is None else _enum_value(
            resource_actions.ProviderDownNotRepresentableReasonV1,
            raw['reason'],
            name='down preflight response V2.reason'))
        return cls(
            version=raw['version'],
            contract=raw['contract'],
            action_kind=raw['action_kind'],
            nonce=raw['nonce'],
            request_sha256=raw['request_sha256'],
            disposition=raw['disposition'],
            reason=reason,
            resolved_cohort=(None if raw['resolved_cohort'] is None else
                             resource_action_authority.
                             ProviderAuthorityWorkerCohortV2.from_value(
                                 raw['resolved_cohort'])),
            execution_capsule=(
                None if raw['execution_capsule'] is None else resource_actions.
                ProviderKubernetesDownExecutionCapsuleV2.from_value(
                    raw['execution_capsule'])),
            executor_policy_proof=(
                None if raw['executor_policy_proof'] is None else
                resource_actions.ProviderPolicyBoundaryProofV1.from_value(
                    raw['executor_policy_proof'])),
            worker_identity=(None if raw['worker_identity'] is None else
                             resource_action_authority.
                             ProviderAuthorityWorkerIdentityV2.from_value(
                                 raw['worker_identity'])))

    @classmethod
    def unavailable(
        cls, request: ProviderAuthorityPreflightRequestV2
    ) -> ProviderDownAuthorityPreflightResponseV2:
        if (type(request) is not ProviderAuthorityPreflightRequestV2 or
                request.action_kind is not kernel_actions.ActionKind.DOWN):
            raise TypeError('down unavailable V2 response requires a down V2 '
                            'request.')
        return cls(version=2,
                   contract=_CONTRACT,
                   action_kind=kernel_actions.ActionKind.DOWN,
                   nonce=request.nonce,
                   request_sha256=request.request_sha256,
                   disposition=ProviderAuthorityPreflightDispositionV2.
                   NOT_REPRESENTABLE,
                   reason=resource_actions.ProviderDownNotRepresentableReasonV1.
                   PREFLIGHT_UNAVAILABLE_OR_INVALID,
                   resolved_cohort=None,
                   execution_capsule=None,
                   executor_policy_proof=None,
                   worker_identity=None)

    def validate_request(self,
                         request: ProviderAuthorityPreflightRequestV2) -> None:
        if (type(request) is not ProviderAuthorityPreflightRequestV2 or
                request.action_kind is not kernel_actions.ActionKind.DOWN or
                self.nonce != request.nonce or
                self.request_sha256 != request.request_sha256):
            raise ValueError('down preflight response V2 does not match its '
                             'request envelope.')
        if self.disposition is ProviderAuthorityPreflightDispositionV2.COMPLETE:
            assert self.resolved_cohort is not None
            assert self.execution_capsule is not None
            assert self.executor_policy_proof is not None
            _validate_complete_against_request(request, self.resolved_cohort,
                                               self.execution_capsule,
                                               self.executor_policy_proof)

    def canonical_value(self) -> JsonObject:
        return _response_value(self)


ProviderAuthorityPreflightResponseV2 = (
    ProviderLaunchAuthorityPreflightResponseV2 |
    ProviderDownAuthorityPreflightResponseV2)


def _response_value(
        response: ProviderAuthorityPreflightResponseV2) -> JsonObject:
    reason = response.reason
    return {
        'version': 2,
        'contract': _CONTRACT,
        'action_kind': response.action_kind.value,
        'nonce': str(response.nonce),
        'request_sha256': response.request_sha256,
        'disposition': response.disposition.value,
        'reason': None if reason is None else reason.value,
        'resolved_cohort': (None if response.resolved_cohort is None else
                            response.resolved_cohort.canonical_value()),
        'execution_capsule': (None if response.execution_capsule is None else
                              response.execution_capsule.canonical_value()),
        'executor_policy_proof':
            (None if response.executor_policy_proof is None else
             response.executor_policy_proof.canonical_value()),
        'worker_identity': (None if response.worker_identity is None else
                            response.worker_identity.canonical_value()),
    }


def provider_authority_preflight_response_from_value_v2(
        value: Any) -> ProviderAuthorityPreflightResponseV2:
    """Decode only the V2 response selected by its action discriminator."""

    raw = _closed_object(value,
                         name='authority preflight response V2',
                         keys=_RESPONSE_KEYS)
    kind = _action_kind(raw['action_kind'],
                        name='authority preflight response V2.action_kind')
    if kind is kernel_actions.ActionKind.LAUNCH:
        return ProviderLaunchAuthorityPreflightResponseV2.from_value(value)
    return ProviderDownAuthorityPreflightResponseV2.from_value(value)


__all__ = [
    'ProviderAuthorityPreflightDispositionV2',
    'ProviderAuthorityPreflightRequestV2',
    'ProviderAuthorityPreflightResponseV2',
    'ProviderDownAuthorityPreflightResponseV2',
    'ProviderDownPreflightSeedV2',
    'ProviderLaunchAuthorityPreflightResponseV2',
    'ProviderLaunchPreflightSeedV2',
    'ProviderLifecyclePreflightSeedV2',
    'provider_authority_preflight_response_from_value_v2',
    'provider_lifecycle_preflight_seed_from_value_v2',
]
