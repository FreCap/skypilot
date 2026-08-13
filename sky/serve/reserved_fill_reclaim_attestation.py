"""Deployment-owned reclaim policy for sequenced reserved-capacity fill.

The generic distribution deliberately installs no implementation.  A
deployment that activates sequenced fill exposes exactly one zero-argument
policy class through the ``skypilot.reserved_fill_reclaim_policy`` Python
entry-point group.  Every process resolves that same interface directly; the
Serve controller does not depend on API-server plugin initialization.
"""

import abc
from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
import enum
import hashlib
import importlib.metadata
import json
import math
import os
import re
import threading
import time
import typing
from typing import Final

_SHA256_RE: Final = re.compile(r'^[0-9a-f]{64}$')
POLICY_ENTRY_POINT_GROUP: Final = 'skypilot.reserved_fill_reclaim_policy'
POLICY_REVISION_MAX_BYTES: Final = 1024
AUTHORIZATION_MAX_AGE_SECONDS: Final = 5.0
POLICY_OPERATION_TIMEOUT_SECONDS: Final = 5.0


class ReclaimAttestationError(RuntimeError):
    """The deployment could not prove or enforce its reclaim contract."""


class ReclaimEnforcementContract(str, enum.Enum):
    """Closed contracts strong enough for one-way fleet activation."""

    GLOBAL_FLEET_CLAIM_AND_LAUNCH_FENCES_V1 = (
        'GLOBAL_FLEET_CLAIM_AND_LAUNCH_FENCES_V1')
    GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2 = (
        'GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2')


def _require_nonempty_text(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f'{name} must be nonempty text.')
    return value


def _require_bounded_text(value: object, name: str, maximum_bytes: int) -> str:
    text = _require_nonempty_text(value, name)
    if len(text.encode('utf-8')) > maximum_bytes:
        raise ValueError(f'{name} must be at most {maximum_bytes} UTF-8 bytes.')
    return text


def _require_sha256(value: object, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be a lowercase SHA-256 digest.')
    return value


def _require_positive_int(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f'{name} must be a positive integer.')
    return value


@dataclasses.dataclass(frozen=True)
class ReclaimPolicyIdentity:
    """Immutable identity bound to the sequenced reconciliation gate."""

    fleet_bundle_sha256: str
    policy_revision: str
    provider_inventory_sha256: str

    def __post_init__(self) -> None:
        _require_sha256(self.fleet_bundle_sha256, 'fleet_bundle_sha256')
        _require_bounded_text(self.policy_revision, 'policy_revision',
                              POLICY_REVISION_MAX_BYTES)
        _require_sha256(self.provider_inventory_sha256,
                        'provider_inventory_sha256')


@dataclasses.dataclass(frozen=True, order=True)
class ReclaimProjectedAdmission:
    """Exact immutable Kubernetes/Kueue admission for one worker candidate."""

    worker_projection_sha256: str
    kubernetes_context: str
    namespace: str
    service_account_name: str
    scheduler_name: str
    priority_class_name: str
    priority_value: int
    preemption_policy: str
    local_queue_name: str
    workload_priority_class_name: str
    accelerator: str
    accelerator_count: int

    def __post_init__(self) -> None:
        _require_sha256(self.worker_projection_sha256,
                        'worker_projection_sha256')
        for name, value in (
            ('kubernetes_context', self.kubernetes_context),
            ('namespace', self.namespace),
            ('service_account_name', self.service_account_name),
            ('scheduler_name', self.scheduler_name),
            ('priority_class_name', self.priority_class_name),
            ('preemption_policy', self.preemption_policy),
            ('local_queue_name', self.local_queue_name),
            ('workload_priority_class_name', self.workload_priority_class_name),
            ('accelerator', self.accelerator),
        ):
            _require_nonempty_text(value, name)
        if (type(self.priority_value) is not int or
                self.priority_value < -2147483648 or
                self.priority_value > 1000000000):
            raise ValueError('priority_value must be a Kubernetes priority '
                             'integer.')
        if self.preemption_policy not in ('Never', 'PreemptLowerPriority'):
            raise ValueError('preemption_policy must be Never or '
                             'PreemptLowerPriority.')
        _require_positive_int(self.accelerator_count, 'accelerator_count')
        object.__setattr__(self, 'accelerator', self.accelerator.casefold())


def projected_admission_from_worker_projection(
    projection: Mapping[str, object],
    *,
    worker_projection_sha256: str,
) -> ReclaimProjectedAdmission:
    """Build the policy view from one already validated v2 source record."""
    if not isinstance(projection, Mapping):
        raise ValueError('Worker projection must be a mapping.')
    kueue_admission = projection.get('kueue_admission')
    if not isinstance(kueue_admission, Mapping):
        raise ValueError('Sequenced reclaim requires projected Kueue '
                         'admission.')
    priority_class_name = projection.get('priority_class_name')
    priority_value = projection.get('priority_value')
    preemption_policy = projection.get('preemption_policy')
    if (not isinstance(priority_class_name, str) or
            type(priority_value) is not int or
            not isinstance(preemption_policy, str)):
        raise ValueError('Sequenced reclaim requires projected Pod priority.')
    return ReclaimProjectedAdmission(
        worker_projection_sha256=worker_projection_sha256,
        kubernetes_context=typing.cast(str,
                                       projection.get('kubernetes_context')),
        namespace=typing.cast(str, projection.get('namespace')),
        service_account_name=typing.cast(
            str, projection.get('service_account_name')),
        scheduler_name=typing.cast(str, projection.get('scheduler_name')),
        priority_class_name=priority_class_name,
        priority_value=priority_value,
        preemption_policy=preemption_policy,
        local_queue_name=typing.cast(str,
                                     kueue_admission.get('local_queue_name')),
        workload_priority_class_name=typing.cast(
            str, kueue_admission.get('workload_priority_class_name')),
        accelerator=typing.cast(str, projection.get('accelerator_name')),
        accelerator_count=typing.cast(int, projection.get('accelerator_count')),
    )


def _require_projected_admissions(
    value: object,
    accelerator_names: tuple[str, ...],
) -> tuple[ReclaimProjectedAdmission, ...]:
    if (type(value) is not tuple or not value or any(
            not isinstance(item, ReclaimProjectedAdmission) for item in value)
            or tuple(sorted(set(value))) != value):
        raise ValueError('projected_admissions must be a unique sorted tuple.')
    admissions = value
    if ({item.accelerator for item in admissions}
            != {name.casefold() for name in accelerator_names}):
        raise ValueError('projected_admissions must exactly cover every '
                         'accelerator name.')
    if len({item.accelerator for item in admissions}) != len(admissions):
        raise ValueError('projected_admissions must contain one candidate per '
                         'accelerator.')
    return admissions


@dataclasses.dataclass(frozen=True, order=True)
class ReservedContextClaim:
    """One durable, currently selectable context/physical-pool claim edge."""

    service_name: str
    service_version: int
    service_generation: int
    pool_key: str
    access_context: str
    physical_cluster_uid: str
    accelerator_names: tuple[str, ...]
    projected_admissions: tuple[ReclaimProjectedAdmission, ...]

    def __post_init__(self) -> None:
        for name, value in (
            ('service_name', self.service_name),
            ('pool_key', self.pool_key),
            ('access_context', self.access_context),
            ('physical_cluster_uid', self.physical_cluster_uid),
        ):
            _require_nonempty_text(value, name)
        _require_positive_int(self.service_version, 'service_version')
        _require_positive_int(self.service_generation, 'service_generation')
        if (type(self.accelerator_names) is not tuple or
                not self.accelerator_names or any(
                    type(name) is not str or not name
                    for name in self.accelerator_names) or
                tuple(sorted(set(
                    self.accelerator_names))) != self.accelerator_names):
            raise ValueError('accelerator_names must be unique sorted text.')
        _require_projected_admissions(self.projected_admissions,
                                      self.accelerator_names)


@dataclasses.dataclass(frozen=True, order=True)
class ReclaimClaimEdge:
    """One normalized edge requested by a complete claim-set replacement."""

    pool_key: str
    access_context: str
    physical_cluster_uid: str
    accelerator_names: tuple[str, ...]
    projected_admissions: tuple[ReclaimProjectedAdmission, ...]

    def __post_init__(self) -> None:
        for name, value in (
            ('pool_key', self.pool_key),
            ('access_context', self.access_context),
            ('physical_cluster_uid', self.physical_cluster_uid),
        ):
            _require_nonempty_text(value, name)
        if (type(self.accelerator_names) is not tuple or
                not self.accelerator_names or any(
                    type(name) is not str or not name
                    for name in self.accelerator_names) or
                tuple(sorted(set(
                    self.accelerator_names))) != self.accelerator_names):
            raise ValueError('accelerator_names must be unique sorted text.')
        _require_projected_admissions(self.projected_admissions,
                                      self.accelerator_names)


@dataclasses.dataclass(frozen=True)
class ReclaimClaimSetScope:
    """Exact normalized complete claim-set request authorized by policy."""

    service_name: str
    service_incarnation: str
    service_version: int
    semantic_hash: str
    edges: tuple[ReclaimClaimEdge, ...]

    def __post_init__(self) -> None:
        _require_nonempty_text(self.service_name, 'service_name')
        _require_nonempty_text(self.service_incarnation, 'service_incarnation')
        _require_positive_int(self.service_version, 'service_version')
        _require_nonempty_text(self.semantic_hash, 'semantic_hash')
        if (type(self.edges) is not tuple or not self.edges or
                tuple(sorted(set(self.edges))) != self.edges):
            raise ValueError('Claim edges must be a unique sorted tuple.')


@dataclasses.dataclass(frozen=True)
class ReclaimLaunchScope:
    """Exact terminal provider effect requested by one durable launch."""

    service_name: str
    service_version: int
    pool_key: str
    service_generation: int
    physical_cluster_uid: str
    kubernetes_context: str
    accelerator: str
    accelerator_count: int
    projected_admission: ReclaimProjectedAdmission

    def __post_init__(self) -> None:
        for name, value in (
            ('service_name', self.service_name),
            ('pool_key', self.pool_key),
            ('physical_cluster_uid', self.physical_cluster_uid),
            ('kubernetes_context', self.kubernetes_context),
            ('accelerator', self.accelerator),
        ):
            _require_nonempty_text(value, name)
        _require_positive_int(self.service_version, 'service_version')
        _require_positive_int(self.service_generation, 'service_generation')
        _require_positive_int(self.accelerator_count, 'accelerator_count')
        object.__setattr__(self, 'accelerator', self.accelerator.casefold())
        if not isinstance(self.projected_admission, ReclaimProjectedAdmission):
            raise ValueError('projected_admission must be typed.')
        if (self.projected_admission.kubernetes_context
                != self.kubernetes_context or
                self.projected_admission.accelerator != self.accelerator or
                self.projected_admission.accelerator_count
                != self.accelerator_count):
            raise ValueError('projected_admission does not match the launch '
                             'location and accelerator shape.')


@dataclasses.dataclass(frozen=True)
class ReclaimEnforcementEvidence:
    """Typed result of one complete external platform-policy proof."""

    contract: ReclaimEnforcementContract
    fleet_bundle_sha256: str
    policy_revision: str
    provider_inventory_sha256: str
    claimed_contexts: tuple[ReservedContextClaim, ...]
    completed_monotonic: float

    def __post_init__(self) -> None:
        if not isinstance(self.contract, ReclaimEnforcementContract):
            raise ValueError('contract must be a ReclaimEnforcementContract.')
        ReclaimPolicyIdentity(self.fleet_bundle_sha256, self.policy_revision,
                              self.provider_inventory_sha256)
        if (type(self.claimed_contexts) is not tuple or tuple(
                sorted(set(self.claimed_contexts))) != self.claimed_contexts):
            raise ValueError('claimed_contexts must be unique and sorted.')
        _require_monotonic(self.completed_monotonic, 'completed_monotonic')

    @property
    def identity(self) -> ReclaimPolicyIdentity:
        return ReclaimPolicyIdentity(self.fleet_bundle_sha256,
                                     self.policy_revision,
                                     self.provider_inventory_sha256)


@dataclasses.dataclass(frozen=True)
class ReclaimClaimAuthorization:
    """Fresh exact-scope authorization for one claim-set transaction."""

    identity: ReclaimPolicyIdentity
    gate_generation: int
    scope: ReclaimClaimSetScope
    completed_monotonic: float

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ReclaimPolicyIdentity):
            raise ValueError('identity must be ReclaimPolicyIdentity.')
        _require_positive_int(self.gate_generation, 'gate_generation')
        if not isinstance(self.scope, ReclaimClaimSetScope):
            raise ValueError('scope must be ReclaimClaimSetScope.')
        _require_monotonic(self.completed_monotonic, 'completed_monotonic')


@dataclasses.dataclass(frozen=True)
class ReclaimLaunchAuthorization:
    """Fresh exact-scope authorization for one terminal provider effect."""

    identity: ReclaimPolicyIdentity
    gate_generation: int
    scope: ReclaimLaunchScope
    completed_monotonic: float

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ReclaimPolicyIdentity):
            raise ValueError('identity must be ReclaimPolicyIdentity.')
        _require_positive_int(self.gate_generation, 'gate_generation')
        if not isinstance(self.scope, ReclaimLaunchScope):
            raise ValueError('scope must be ReclaimLaunchScope.')
        _require_monotonic(self.completed_monotonic, 'completed_monotonic')


@dataclasses.dataclass(frozen=True)
class ReclaimActivationReceipt:
    """Canonical durable projection of one writer and reclaim proof."""

    identity: ReclaimPolicyIdentity
    claim_scope_count: int
    claim_scope_sha256: str
    evidence_sha256: str
    writer_image_digest: str
    writer_deployment_generation: str
    writer_deployment_uid: str
    writer_pod_inventory_count: int
    writer_pod_inventory_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ReclaimPolicyIdentity):
            raise ValueError('identity must be ReclaimPolicyIdentity.')
        if (type(self.claim_scope_count) is not int or
                self.claim_scope_count < 0):
            raise ValueError('claim_scope_count must be nonnegative.')
        _require_sha256(self.claim_scope_sha256, 'claim_scope_sha256')
        _require_sha256(self.evidence_sha256, 'evidence_sha256')
        if (type(self.writer_image_digest) is not str or re.fullmatch(
                r'sha256:[0-9a-f]{64}', self.writer_image_digest) is None):
            raise ValueError('writer_image_digest must be a sha256 digest.')
        _require_bounded_text(self.writer_deployment_generation,
                              'writer_deployment_generation', 1024)
        _require_bounded_text(self.writer_deployment_uid,
                              'writer_deployment_uid', 1024)
        _require_positive_int(self.writer_pod_inventory_count,
                              'writer_pod_inventory_count')
        _require_sha256(self.writer_pod_inventory_sha256,
                        'writer_pod_inventory_sha256')


def claim_scope_projection(
    claimed_contexts: tuple[ReservedContextClaim, ...],) -> tuple[int, str]:
    """Return the canonical count and digest for an exact durable scope."""
    if (type(claimed_contexts) is not tuple or
            tuple(sorted(set(claimed_contexts))) != claimed_contexts):
        raise ValueError('claimed_contexts must be a unique sorted tuple.')
    encoded = json.dumps(
        [dataclasses.asdict(claim) for claim in claimed_contexts],
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
        allow_nan=False).encode('utf-8')
    return len(claimed_contexts), hashlib.sha256(encoded).hexdigest()


def activation_receipt(
    evidence: ReclaimEnforcementEvidence,
    *,
    writer_image_digest: str,
    writer_deployment_generation: str,
    writer_deployment_uid: str,
    writer_pod_inventory_count: int,
    writer_pod_inventory_sha256: str,
) -> ReclaimActivationReceipt:
    """Hash the exact policy, claim scope, and writer cohort attested."""
    if not isinstance(evidence, ReclaimEnforcementEvidence):
        raise ValueError('evidence must be ReclaimEnforcementEvidence.')
    scope_count, scope_sha256 = claim_scope_projection(
        evidence.claimed_contexts)
    identity = evidence.identity
    material = {
        'schema_version': 2,
        'contract': evidence.contract.value,
        'fleet_bundle_sha256': identity.fleet_bundle_sha256,
        'policy_revision': identity.policy_revision,
        'provider_inventory_sha256': identity.provider_inventory_sha256,
        'claim_scope_count': scope_count,
        'claim_scope_sha256': scope_sha256,
        'writer_image_digest': writer_image_digest,
        'writer_deployment_generation': writer_deployment_generation,
        'writer_deployment_uid': writer_deployment_uid,
        'writer_pod_inventory_count': writer_pod_inventory_count,
        'writer_pod_inventory_sha256': writer_pod_inventory_sha256,
    }
    evidence_sha256 = hashlib.sha256(
        json.dumps(material,
                   sort_keys=True,
                   separators=(',', ':'),
                   ensure_ascii=False,
                   allow_nan=False).encode('utf-8')).hexdigest()
    return ReclaimActivationReceipt(
        identity=identity,
        claim_scope_count=scope_count,
        claim_scope_sha256=scope_sha256,
        evidence_sha256=evidence_sha256,
        writer_image_digest=writer_image_digest,
        writer_deployment_generation=writer_deployment_generation,
        writer_deployment_uid=writer_deployment_uid,
        writer_pod_inventory_count=writer_pod_inventory_count,
        writer_pod_inventory_sha256=writer_pod_inventory_sha256)


def _require_monotonic(value: object, name: str) -> float:
    if (isinstance(value, bool) or not isinstance(value, (int, float)) or
            not math.isfinite(float(value))):
        raise ValueError(f'{name} must be finite numeric monotonic time.')
    return float(value)


class ReservedFillReclaimPolicy(abc.ABC):
    """Deployment implementation of activation, claim, and launch policy."""

    @abc.abstractmethod
    def enforcement_contract(self) -> ReclaimEnforcementContract:
        """Return the exact claim/admission/launch contract implemented."""
        raise NotImplementedError

    @abc.abstractmethod
    def policy_identity(self) -> ReclaimPolicyIdentity:
        """Return the provider-free immutable identity implemented locally."""
        raise NotImplementedError

    @abc.abstractmethod
    def attest_activation(
        self,
        claimed_contexts: tuple[ReservedContextClaim, ...],
        *,
        writer_image_digest: str,
        deadline_monotonic: float,
    ) -> ReclaimEnforcementEvidence:
        """Prove the fleet contract before the absolute deadline."""
        raise NotImplementedError

    @abc.abstractmethod
    def authorize_claim_set(
        self,
        scope: ReclaimClaimSetScope,
        *,
        expected_identity: ReclaimPolicyIdentity,
        expected_gate_generation: int,
        deadline_monotonic: float,
    ) -> ReclaimClaimAuthorization:
        """Authorize one exact claim set before the absolute deadline."""
        raise NotImplementedError

    @abc.abstractmethod
    def authorize_launch(
        self,
        scope: ReclaimLaunchScope,
        *,
        expected_identity: ReclaimPolicyIdentity,
        expected_gate_generation: int,
        deadline_monotonic: float,
    ) -> ReclaimLaunchAuthorization:
        """Authorize one exact effect before the absolute deadline."""
        raise NotImplementedError


_POLICY_CACHE_LOCK = threading.Lock()
_POLICY_CACHE_PID: int | None = None
_POLICY_CACHE: ReservedFillReclaimPolicy | None = None


def _reset_policy_cache_after_fork() -> None:
    """Discard inherited policy state and any parent-owned mutex."""
    global _POLICY_CACHE_LOCK, _POLICY_CACHE_PID, _POLICY_CACHE
    _POLICY_CACHE_LOCK = threading.Lock()
    _POLICY_CACHE_PID = None
    _POLICY_CACHE = None


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_reset_policy_cache_after_fork)


def require_unique_policy() -> ReservedFillReclaimPolicy:
    """Load exactly one deployment policy class once in this process."""
    global _POLICY_CACHE_PID, _POLICY_CACHE
    process_id = os.getpid()
    with _POLICY_CACHE_LOCK:
        if _POLICY_CACHE_PID == process_id and _POLICY_CACHE is not None:
            return _POLICY_CACHE
        policy = _load_unique_policy()
        _POLICY_CACHE_PID = process_id
        _POLICY_CACHE = policy
        return policy


def _load_unique_policy() -> ReservedFillReclaimPolicy:
    """Discover and instantiate the sole policy while the cache mutex is held."""
    try:
        discovered = importlib.metadata.entry_points()
        entries = tuple(discovered.select(group=POLICY_ENTRY_POINT_GROUP))
    except Exception as error:
        raise ReclaimAttestationError(
            'The deployment reclaim-policy entry points could not be read.') \
            from error
    if len(entries) != 1:
        raise ReclaimAttestationError(
            'Sequenced reserved fill requires exactly one deployment reclaim '
            f'policy; discovered {len(entries)}.')
    try:
        policy_class = entries[0].load()
        if (not isinstance(policy_class, type) or
                not issubclass(policy_class, ReservedFillReclaimPolicy)):
            raise TypeError('entry point must load a policy class')
        policy = policy_class()
    except Exception as error:
        raise ReclaimAttestationError(
            'The deployment reclaim-policy entry point could not be loaded.') \
            from error
    return policy


def require_policy_identity(
    policy: ReservedFillReclaimPolicy,) -> ReclaimPolicyIdentity:
    """Read and validate the provider-free local policy identity."""
    if not isinstance(policy, ReservedFillReclaimPolicy):
        raise ReclaimAttestationError(
            'The deployment reclaim policy is not a typed policy plugin.')
    try:
        identity = policy.policy_identity()
    except Exception as error:  # pylint: disable=broad-except
        raise ReclaimAttestationError(
            'The deployment reclaim policy identity could not be read.') \
            from error
    if not isinstance(identity, ReclaimPolicyIdentity):
        raise ReclaimAttestationError(
            'The deployment reclaim policy returned an untyped identity.')
    return identity


def require_policy_contract(
    policy: ReservedFillReclaimPolicy,) -> ReclaimEnforcementContract:
    """Require the current immutable-admission enforcement contract."""
    if not isinstance(policy, ReservedFillReclaimPolicy):
        raise ReclaimAttestationError(
            'The deployment reclaim policy is not a typed policy plugin.')
    try:
        contract = policy.enforcement_contract()
    except Exception as error:  # pylint: disable=broad-except
        raise ReclaimAttestationError(
            'The deployment reclaim-policy contract could not be read.') \
            from error
    required = (ReclaimEnforcementContract.
                GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2)
    if contract != required:
        raise ReclaimAttestationError(
            'The deployment reclaim policy does not own immutable worker '
            'admission plus future claim and launch fences.')
    return contract


def require_exact_policy_identity(
    policy: ReservedFillReclaimPolicy,
    expected_identity: ReclaimPolicyIdentity,
) -> ReclaimPolicyIdentity:
    """Require the local plugin to implement the exact expected identity."""
    if not isinstance(expected_identity, ReclaimPolicyIdentity):
        raise ReclaimAttestationError(
            'Expected reclaim policy identity is not typed.')
    require_policy_contract(policy)
    identity = require_policy_identity(policy)
    if identity != expected_identity:
        raise ReclaimAttestationError(
            'The local deployment reclaim policy identity does not match '
            'the expected authority.')
    return identity


def require_exact_evidence(
    evidence: ReclaimEnforcementEvidence,
    claimed_contexts: Sequence[ReservedContextClaim],
) -> ReclaimEnforcementEvidence:
    """Validate exact current scope plus the future-context fleet contract."""
    if not isinstance(evidence, ReclaimEnforcementEvidence):
        raise ReclaimAttestationError(
            'The deployment reclaim policy returned untyped evidence.')
    expected = tuple(sorted(claimed_contexts))
    if evidence.claimed_contexts != expected:
        raise ReclaimAttestationError(
            'The reclaim proof does not exactly cover current durable claims.')
    if evidence.contract != (ReclaimEnforcementContract.
                             GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2):
        raise ReclaimAttestationError(
            'The reclaim proof does not own immutable worker admission plus '
            'future claim and launch fences.')
    return evidence


def new_policy_operation_deadline() -> float:
    """Return the absolute deadline every deployment policy must honor."""
    return time.monotonic() + POLICY_OPERATION_TIMEOUT_SECONDS


def require_policy_operation_completed(deadline_monotonic: float) -> None:
    """Reject a policy result returned after its caller-owned deadline."""
    deadline = _require_monotonic(deadline_monotonic, 'deadline_monotonic')
    now = time.monotonic()
    if not math.isfinite(now) or now > deadline:
        raise ReclaimAttestationError(
            'The deployment reclaim-policy operation exceeded its deadline.')


def _require_fresh(completed_monotonic: float, *, now_monotonic: float | None,
                   subject: str) -> None:
    completed = _require_monotonic(completed_monotonic,
                                   f'{subject} completed_monotonic')
    now = time.monotonic() if now_monotonic is None else _require_monotonic(
        now_monotonic, 'now_monotonic')
    age = now - completed
    if age < 0 or age > AUTHORIZATION_MAX_AGE_SECONDS:
        raise ReclaimAttestationError(f'The reclaim {subject} is stale.')


def require_exact_claim_authorization(
    authorization: ReclaimClaimAuthorization,
    *,
    expected_identity: ReclaimPolicyIdentity,
    expected_gate_generation: int,
    expected_scope: ReclaimClaimSetScope,
    now_monotonic: float | None = None,
) -> ReclaimClaimAuthorization:
    """Validate one provider-produced ticket at locked claim persistence."""
    if not isinstance(authorization, ReclaimClaimAuthorization):
        raise ReclaimAttestationError(
            'The deployment reclaim policy returned an untyped claim ticket.')
    if (authorization.identity != expected_identity or
            authorization.gate_generation != expected_gate_generation or
            authorization.scope != expected_scope):
        raise ReclaimAttestationError(
            'The reclaim claim authorization does not match locked authority.')
    _require_fresh(authorization.completed_monotonic,
                   now_monotonic=now_monotonic,
                   subject='claim authorization')
    return authorization


def require_exact_launch_authorization(
    authorization: ReclaimLaunchAuthorization,
    *,
    expected_identity: ReclaimPolicyIdentity,
    expected_gate_generation: int,
    expected_scope: ReclaimLaunchScope,
    now_monotonic: float | None = None,
) -> ReclaimLaunchAuthorization:
    """Validate one provider-produced ticket at the provider boundary."""
    if not isinstance(authorization, ReclaimLaunchAuthorization):
        raise ReclaimAttestationError(
            'The deployment reclaim policy returned an untyped launch ticket.')
    if (authorization.identity != expected_identity or
            authorization.gate_generation != expected_gate_generation or
            authorization.scope != expected_scope):
        raise ReclaimAttestationError(
            'The reclaim launch authorization does not match durable authority.'
        )
    _require_fresh(authorization.completed_monotonic,
                   now_monotonic=now_monotonic,
                   subject='launch authorization')
    return authorization
