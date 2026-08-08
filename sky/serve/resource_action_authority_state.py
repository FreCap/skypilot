"""Transactional PostgreSQL primitives for the Serve035 authority plane.

This first closed slice owns only operations whose complete lock/evidence
contract is available without Kubernetes or provider I/O:

* one-member ``REGISTERING`` anchor plus generation-one lease;
* second-member append plus generation-one lease;
* current-member or handoff-candidate own-lease renewal;
* initial ``REGISTERING -> ACCEPTING`` activation;
* authoritative-only V2 ``PREPARING`` reference creation/adoption and its
  mutation-free dark-preflight trust fence; and
* one-way ``OPEN -> DRAINING`` policy admission shutdown.

The store never performs Kubernetes I/O, calls a provider, or opens an
execution lease.  Handoff mutation, full-set cold recovery, cohort rollback,
and lifecycle retirement are intentionally absent until their complete joined
request/API-instance evidence transactions land.  Policy promotion, close,
reopen, and successor activation are likewise absent until each reconstructs
and validates its complete inventory under the same locks.  There are no
permissive placeholder implementations for those transitions.
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import enum
import time
from typing import Any, cast, TypeAlias
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_action_state_schema as state_schema
from sky.serve import resource_actions
from sky.serve import serve_state_schema
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.server.requests import requests as requests_lib
from sky.server.requests import resource_actions as kernel_actions

_UTC = datetime.timezone.utc
_REGISTRATION_MAX_AGE = datetime.timedelta(minutes=5)
_LEASE_TTL = datetime.timedelta(
    seconds=authority.RESOURCE_ACTION_WORKER_REGISTRATION_LEASE_TTL_SECONDS_V1)
_API_INSTANCE_LEASE_TTL = datetime.timedelta(
    seconds=authority.RESOURCE_ACTION_WORKER_API_INSTANCE_LEASE_TTL_SECONDS_V1)
_MAX_AUTHORITY_RELEASE_COHORTS = 256
# Bootstrap workers have a 60-second termination grace.  These are the
# transaction-local graceful budgets; the coordinator separately fail-stops at
# its 30-second whole-mutation fence if pool/connect or DBAPI/network behavior
# defeats them.  They apply only to the four initial V2 membership mutations,
# not to ambient connections or unrelated state paths.
_BOOTSTRAP_MUTATION_STATEMENT_TIMEOUT_MILLISECONDS = 5_000
_BOOTSTRAP_MUTATION_LOCK_TIMEOUT_MILLISECONDS = 3_000
# The transport owns the hard five-second publication deadline.  These shorter
# database limits are graceful containment for the one isolated, mutation-free
# trust read that may finish after a driver/network blackhole.  The monotonic
# transaction guard includes checkout on the store-owned path and prevents a
# new statement from starting after the cumulative budget expires.
_PREFLIGHT_TRUST_TRANSACTION_TIMEOUT_SECONDS = 4.0
_PREFLIGHT_TRUST_STATEMENT_TIMEOUT_MILLISECONDS = 3_500
_PREFLIGHT_TRUST_LOCK_TIMEOUT_MILLISECONDS = 750
_PREFLIGHT_TRUST_IDLE_TRANSACTION_TIMEOUT_MILLISECONDS = 4_000


class AuthorityStateError(RuntimeError):
    """Base error for fail-closed authority state transitions."""


class AuthorityStateConflict(AuthorityStateError):
    """The requested transition conflicts with locked durable state."""


class AuthorityStateCorruption(AuthorityStateError):
    """A retained row violates its typed/hash/cross-row contract."""


class AuthorityStateSuperseded(AuthorityStateConflict):
    """A later legal revision won; the requested acknowledgement is not it."""


@dataclasses.dataclass(frozen=True)
class WorkerCohortV2Record:
    """Fully revalidated V2 cohort and registration-set projection."""

    cohort: authority.ProviderAuthorityWorkerCohortV2
    registration_set: authority.ProviderAuthorityWorkerRegistrationSetV2
    lifecycle_state: resource_actions.WorkerCohortLifecycleState
    revision: int
    created_at: datetime.datetime
    state_changed_at: datetime.datetime
    removal_authorized_at: datetime.datetime | None
    retired_at: datetime.datetime | None

    def __post_init__(self) -> None:
        if self.revision != self.registration_set.revision:
            raise AuthorityStateCorruption(
                'V2 cohort and registration-set revisions differ.')
        if self.lifecycle_state is resource_actions.WorkerCohortLifecycleState.REGISTERING:
            self.registration_set.validate_registering()
        elif self.lifecycle_state in (
                resource_actions.WorkerCohortLifecycleState.ACCEPTING,
                resource_actions.WorkerCohortLifecycleState.DRAINING):
            self.registration_set.validate_accepted()

    @property
    def cohort_id(self) -> str:
        return self.cohort.cohort_id


@dataclasses.dataclass(frozen=True)
class WorkerRegistrationMutation:
    cohort: WorkerCohortV2Record
    lease: authority.ProviderAuthorityWorkerLeaseV1
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class WorkerCohortActivationMutation:
    """Committed or exactly adopted initial V2 cohort activation."""

    cohort: WorkerCohortV2Record
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class WorkerBootstrapState:
    """One typed read of a V2 cohort and the caller's own lease."""

    cohort: WorkerCohortV2Record
    own_lease: authority.ProviderAuthorityWorkerLeaseV1 | None


@dataclasses.dataclass(frozen=True)
class WorkerCohortReferenceV2Record:
    """Strict Serve035 projection of one retained cohort reference."""

    reference: resource_actions.WorkerCohortReferenceInputV1
    reference_state: resource_actions.WorkerCohortReferenceState
    revision: int
    created_at: datetime.datetime
    bound_at: datetime.datetime | None
    released_at: datetime.datetime | None
    authority_policy_epoch: uuid.UUID | None
    authority_policy_sha256: str | None
    authority_binding_sha256: str | None

    @property
    def decision_id(self) -> uuid.UUID:
        return self.reference.decision_id

    @property
    def cohort_id(self) -> str:
        return self.reference.cohort_id


AuthorityReleaseManifest: TypeAlias = (
    resource_actions.ProviderAuthorityWorkerCohortManifestV1 |
    authority.ProviderAuthorityWorkerCohortManifestV2)


@dataclasses.dataclass(frozen=True)
class AuthorityReleaseLedgerRecord:
    """Strict version-aware decode of one retained Serve034 release row."""

    namespace: str
    helm_release_name: str
    installation_id: uuid.UUID
    helm_full_name: str
    enabled: bool
    live_manifests: tuple[AuthorityReleaseManifest, ...]
    tombstone_suffixes: tuple[str, ...]
    revision: int
    created_at: datetime.datetime
    updated_at: datetime.datetime


class AuthorityPolicyState(str, enum.Enum):
    ACTIVE = 'ACTIVE'
    SUPERSEDED = 'SUPERSEDED'


class AuthorityPolicyAdmissionState(str, enum.Enum):
    OPEN = 'OPEN'
    DRAINING = 'DRAINING'
    CLOSED = 'CLOSED'


class AuthorityPolicyOperation(str, enum.Enum):
    ACTIVATE = 'ACTIVATE'
    DRAIN = 'DRAIN'
    CLOSE = 'CLOSE'
    REOPEN = 'REOPEN'
    SUPERSEDE = 'SUPERSEDE'


@dataclasses.dataclass(frozen=True)
class AuthorityPolicyEpochRecord:
    """Fully revalidated immutable policy payload and mutable admission state."""

    service_hash: str
    policy_epoch: uuid.UUID
    predecessor_policy_epoch: uuid.UUID | None
    policy: authority.ResourceActionQualificationPolicyV1
    policy_sha256: str
    authority_binding_sha256: str
    rotation_proof: (authority.AuthoritativePromotionProofV1 |
                     authority.ServeAuthorityPolicyRotationProofV1)
    rotation_proof_sha256: str
    nonterminal_inventory: authority.AuthorityNonterminalInventoryV1
    nonterminal_inventory_sha256: str
    reason: str
    policy_state: AuthorityPolicyState
    admission_state: AuthorityPolicyAdmissionState
    admission_revision: int
    last_operation_id: uuid.UUID
    last_operation_kind: AuthorityPolicyOperation
    created_at: datetime.datetime
    admission_changed_at: datetime.datetime
    activated_at: datetime.datetime
    superseded_at: datetime.datetime | None


@dataclasses.dataclass(frozen=True)
class AuthorityPolicyMutation:
    record: AuthorityPolicyEpochRecord
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class WorkerCohortReferencePreparationV2:
    """Locked evidence returned by one V2 PREPARING transaction."""

    record: WorkerCohortReferenceV2Record
    cohort: WorkerCohortV2Record
    accepted_memberships: tuple[
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
    ]
    initial_candidate_binding: resource_actions.ShadowCandidateActionBindingV1
    current_authority_binding: authority.ResourceActionCandidateBindingV1
    authority_policy: AuthorityPolicyEpochRecord
    adopted: bool = False

    def __post_init__(self) -> None:
        if type(self.record) is not WorkerCohortReferenceV2Record:
            raise TypeError('record must be a V2 cohort reference.')
        if type(self.cohort) is not WorkerCohortV2Record:
            raise TypeError('cohort must be a locked V2 cohort record.')
        memberships = self.accepted_memberships
        if (type(memberships) is not tuple or len(memberships) != 2 or any(
                type(item)
                is not authority.ProviderAuthorityWorkerAcceptedMembershipV2
                for item in memberships)):
            raise TypeError('accepted_memberships must be the exact V2 pair.')
        if type(self.initial_candidate_binding) is not (
                resource_actions.ShadowCandidateActionBindingV1):
            raise TypeError('initial_candidate_binding has invalid type.')
        if type(self.current_authority_binding) is not (
                authority.ResourceActionCandidateBindingV1):
            raise TypeError('current_authority_binding has invalid type.')
        if type(self.authority_policy) is not AuthorityPolicyEpochRecord:
            raise TypeError('authority_policy has invalid type.')
        if type(self.adopted) is not bool:
            raise TypeError('adopted must be Boolean.')


def _canonical_datetime(value: Any, *, name: str) -> datetime.datetime:
    if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
            value.utcoffset() is None):
        raise AuthorityStateCorruption(f'{name} is not timezone aware.')
    return value.astimezone(_UTC)


def _canonical_hash(value: Any, *, name: str) -> str:
    if (type(value) is not str or len(value) != 64 or
            any(character not in '0123456789abcdef' for character in value)):
        raise AuthorityStateCorruption(f'{name} is not lowercase SHA-256.')
    return value


def _same_canonical(left: Any, right: Any) -> bool:
    return authority.canonical_json_bytes(
        left) == authority.canonical_json_bytes(right)


def _row_uuid(value: Any, *, name: str) -> uuid.UUID:
    try:
        parsed = value if type(value) is uuid.UUID else uuid.UUID(str(value))
    except (TypeError, ValueError) as e:
        raise AuthorityStateCorruption(f'{name} is not a UUID.') from e
    return parsed


def _cohort_id_installation_id(cohort_id: str) -> uuid.UUID:
    parts = cohort_id.split(':')
    if len(parts) != 4 or parts[0] != 'ra':
        raise AuthorityStateConflict('V2 cohort ID has invalid derivation.')
    try:
        installation_id = uuid.UUID(parts[1])
    except ValueError as e:
        raise AuthorityStateConflict(
            'V2 cohort ID has invalid installation UUID.') from e
    if str(installation_id) != parts[1]:
        raise AuthorityStateConflict(
            'V2 cohort installation UUID is not canonical.')
    return installation_id


def _release_label(value: Any, *, name: str, maximum_bytes: int = 63) -> str:
    if (type(value) is not str or not value or
            len(value.encode('utf-8')) > maximum_bytes or not value.isascii() or
            value != value.lower() or value.startswith('-') or
            value.endswith('-') or
            any(not (character.islower() or character.isdecimal() or
                     character == '-') for character in value)):
        raise ValueError(f'{name} is not one lowercase DNS label.')
    return value


def _release_manifest_from_value(value: Any) -> AuthorityReleaseManifest:
    if type(value) is not dict:
        raise TypeError('release manifest is not an object.')
    version = value.get('version')
    if type(version) is not int:
        raise TypeError('release manifest version is not an integer.')
    if version == 1:
        return resource_actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
            value)
    if version == 2:
        return authority.ProviderAuthorityWorkerCohortManifestV2.from_value(
            value)
    raise ValueError('release manifest version is unsupported.')


def _release_manifest_suffix(manifest: AuthorityReleaseManifest) -> str:
    return manifest.pod_template_binding.release_inputs.cohort_suffix


def _release_manifest_installation_id(
        manifest: AuthorityReleaseManifest) -> uuid.UUID:
    parts = manifest.cohort_id.split(':')
    if len(parts) != 4 or parts[0] != 'ra':
        raise ValueError('release manifest cohort ID is malformed.')
    installation_id = uuid.UUID(parts[1])
    if str(installation_id) != parts[1]:
        raise ValueError('release manifest installation ID is noncanonical.')
    return installation_id


def decode_authority_release_ledger_row(
        row: Mapping[str, Any]) -> AuthorityReleaseLedgerRecord:
    """Fully validate one release row before it can authorize V2 membership."""

    try:
        namespace = _release_label(row['namespace'], name='release.namespace')
        release_name = _release_label(row['helm_release_name'],
                                      name='release.helm_release_name')
        installation_id = _row_uuid(row['installation_id'],
                                    name='release.installation_id')
        full_name = _release_label(row['helm_full_name'],
                                   name='release.helm_full_name')
        enabled = row['enabled']
        if type(enabled) is not bool:
            raise TypeError('release.enabled is not Boolean.')

        live_values = row['live_manifests']
        if type(live_values) is not list:
            raise TypeError('release.live_manifests is not an array.')
        tombstone_values = row['tombstone_suffixes']
        if type(tombstone_values) is not list:
            raise TypeError('release tombstone inventory is not an array.')
        if len(live_values) > _MAX_AUTHORITY_RELEASE_COHORTS:
            raise ValueError('release live inventory exceeds 256 entries.')
        if len(tombstone_values) > _MAX_AUTHORITY_RELEASE_COHORTS:
            raise ValueError('release tombstone inventory exceeds 256 entries.')
        if (len(live_values) + len(tombstone_values)
                > _MAX_AUTHORITY_RELEASE_COHORTS):
            raise ValueError('release inventories exceed 256 entries.')
        live_hash = _canonical_hash(row['live_inventory_sha256'],
                                    name='release.live_inventory_sha256')
        if live_hash != authority.canonical_sha256(live_values):
            raise ValueError('release live inventory hash does not match.')
        live_manifests = tuple(
            _release_manifest_from_value(value) for value in live_values)
        if any(not _same_canonical(value, manifest.canonical_value())
               for value, manifest in zip(live_values, live_manifests)):
            raise ValueError('release live inventory is not canonical.')
        versions = tuple(manifest.version for manifest in live_manifests)
        if versions and len(set(versions)) != 1:
            raise ValueError('release live inventory mixes manifest versions.')
        live_suffixes = tuple(
            _release_manifest_suffix(manifest) for manifest in live_manifests)
        if live_suffixes != tuple(sorted(set(live_suffixes))):
            raise ValueError('release live inventory is not sorted and unique.')

        if any(type(value) is not str for value in tombstone_values):
            raise TypeError('release tombstone inventory is not a text array.')
        tombstone_hash = _canonical_hash(
            row['tombstone_inventory_sha256'],
            name='release.tombstone_inventory_sha256')
        if tombstone_hash != authority.canonical_sha256(tombstone_values):
            raise ValueError('release tombstone inventory hash does not match.')
        tombstones = tuple(
            _release_label(
                value, name='release.tombstone_suffix', maximum_bytes=42)
            for value in tombstone_values)
        if tombstones != tuple(sorted(set(tombstones))):
            raise ValueError('release tombstones are not sorted and unique.')
        if (not set(live_suffixes).isdisjoint(tombstones) or
                len(live_suffixes) + len(tombstones)
                > _MAX_AUTHORITY_RELEASE_COHORTS):
            raise ValueError('release inventories overlap or exceed bounds.')
        if enabled and not live_manifests and not tombstones:
            raise ValueError('enabled release has an empty inventory.')
        if not enabled and (live_manifests or tombstones):
            raise ValueError('disabled release has a nonempty inventory.')
        for manifest in live_manifests:
            release_inputs = manifest.pod_template_binding.release_inputs
            if (manifest.namespace != namespace or
                    release_inputs.helm_full_name != full_name or
                    _release_manifest_installation_id(manifest)
                    != installation_id):
                raise ValueError('release manifest has another identity.')

        revision = row['revision']
        if type(revision) is not int or revision <= 0:
            raise TypeError('release.revision is not a positive integer.')
        created_at = _canonical_datetime(row['created_at'],
                                         name='release.created_at')
        updated_at = _canonical_datetime(row['updated_at'],
                                         name='release.updated_at')
        if updated_at < created_at:
            raise ValueError('release update precedes creation.')
        return AuthorityReleaseLedgerRecord(namespace, release_name,
                                            installation_id, full_name, enabled,
                                            live_manifests, tombstones,
                                            revision, created_at, updated_at)
    except (KeyError, TypeError, ValueError) as e:
        raise AuthorityStateCorruption(
            'Authority release row failed closed validation.') from e


def _validate_authority_release_cohort_binding(
    row: Mapping[str, Any],
    release: AuthorityReleaseLedgerRecord,
    manifest: AuthorityReleaseManifest,
) -> None:
    """Validate every typed/hash/identity field of a permanent binding row."""

    try:
        suffix = _release_label(row['cohort_suffix'],
                                name='binding.cohort_suffix',
                                maximum_bytes=42)
        bound = _release_manifest_from_value(row['manifest'])
        digest = _canonical_hash(row['manifest_sha256'],
                                 name='binding.manifest_sha256')
        if (row['namespace'] != release.namespace or
                row['helm_release_name'] != release.helm_release_name or
                suffix != _release_manifest_suffix(manifest) or
                row['cohort_id'] != manifest.cohort_id or
                digest != bound.sha256 or digest != manifest.sha256 or
                not _same_canonical(bound.canonical_value(),
                                    manifest.canonical_value())):
            raise ValueError('release cohort binding differs.')
        _canonical_datetime(row['bound_at'], name='binding.bound_at')
    except (KeyError, TypeError, ValueError) as e:
        raise AuthorityStateCorruption(
            'Authority release cohort binding failed closed validation.') from e


def decode_worker_cohort_reference_v2_row(
        row: Mapping[str, Any]) -> WorkerCohortReferenceV2Record:
    """Decode every Serve035 reference field, including its policy triple."""

    try:
        reference = resource_actions.WorkerCohortReferenceInputV1.from_value({
            'version': 1,
            'decision_id': str(
                _row_uuid(row['decision_id'], name='reference.decision_id')),
            'cohort_id': row['cohort_id'],
            'service_hash': row['service_hash'],
            'replica_incarnation': str(
                _row_uuid(row['replica_incarnation'],
                          name='reference.replica_incarnation')),
            'desired_generation': row['desired_generation'],
            'action_type': row['action_type'],
            'controller_owner_fence': row['controller_owner_fence'],
            'lifecycle_epoch': row['lifecycle_epoch'],
            'preparation_capability_sha256':
                row['preparation_capability_sha256'],
        })
        reference_state = resource_actions.WorkerCohortReferenceState(
            row['reference_state'])
        revision = row['revision']
        if type(revision) is not int or revision <= 0:
            raise TypeError('reference.revision is not a positive integer.')
        created_at = _canonical_datetime(row['created_at'],
                                         name='reference.created_at')
        bound_at = (None if row['bound_at'] is None else _canonical_datetime(
            row['bound_at'], name='reference.bound_at'))
        released_at = (None if row['released_at'] is None else
                       _canonical_datetime(row['released_at'],
                                           name='reference.released_at'))

        raw_policy = (row['authority_policy_epoch'],
                      row['authority_policy_sha256'],
                      row['authority_binding_sha256'])
        policy_present = tuple(value is not None for value in raw_policy)
        if any(policy_present) and not all(policy_present):
            raise ValueError('reference policy triple is partially null.')
        if all(policy_present):
            policy_epoch = _row_uuid(raw_policy[0],
                                     name='reference.authority_policy_epoch')
            policy_sha256 = _canonical_hash(
                raw_policy[1], name='reference.authority_policy_sha256')
            binding_sha256 = _canonical_hash(
                raw_policy[2], name='reference.authority_binding_sha256')
        else:
            policy_epoch = None
            policy_sha256 = None
            binding_sha256 = None

        if bound_at is not None and bound_at < created_at:
            raise ValueError('reference binding predates creation.')
        release_floor = bound_at if bound_at is not None else created_at
        if released_at is not None and released_at < release_floor:
            raise ValueError('reference release predates retained evidence.')
        if reference_state is resource_actions.WorkerCohortReferenceState.PREPARING:
            if (revision != 1 or bound_at is not None or
                    released_at is not None or policy_epoch is not None):
                raise ValueError(
                    'PREPARING reference is not revision-one/unbound/null-policy.'
                )
        elif reference_state is (
                resource_actions.WorkerCohortReferenceState.SHADOW_ACTIVE):
            if (bound_at is None or released_at is not None or
                    policy_epoch is not None):
                raise ValueError('SHADOW_ACTIVE reference shape is invalid.')
        elif reference_state is (
                resource_actions.WorkerCohortReferenceState.ACTION_ACTIVE):
            if (bound_at is None or released_at is not None or
                    policy_epoch is None):
                raise ValueError('ACTION_ACTIVE reference shape is invalid.')
        elif released_at is None:
            raise ValueError('RELEASED reference has no release timestamp.')
        return WorkerCohortReferenceV2Record(
            reference=reference,
            reference_state=reference_state,
            revision=revision,
            created_at=created_at,
            bound_at=bound_at,
            released_at=released_at,
            authority_policy_epoch=policy_epoch,
            authority_policy_sha256=policy_sha256,
            authority_binding_sha256=binding_sha256)
    except (KeyError, TypeError, ValueError) as e:
        raise AuthorityStateCorruption(
            'Worker cohort reference row is malformed.') from e


def _approved_cohort_artifact_from_manifest_v2(
    manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
) -> authority.ApprovedAuthorityCohortArtifactV1:
    """Project the sole policy-facing artifact from one exact V2 manifest."""

    if type(manifest) is not authority.ProviderAuthorityWorkerCohortManifestV2:
        raise TypeError('manifest must be an exact V2 authority manifest.')
    return authority.ApprovedAuthorityCohortArtifactV1(
        cohort_id=manifest.cohort_id,
        oci_manifest_digest=manifest.image.oci_manifest_digest,
        oci_config_digest=manifest.image.oci_config_digest,
        manifest_sha256=manifest.sha256,
        qualification_artifact_sha256=(
            manifest.image.qualification_artifact.sha256),
        pod_template_contract_sha256=manifest.pod_template_contract.sha256,
        pod_template_binding_sha256=manifest.pod_template_binding.sha256,
        artifact_inventory_sha256=manifest.artifact_inventory.sha256,
        callable_inventory_sha256=manifest.callable_inventory.sha256,
        handler_allowlist_sha256=resource_actions.canonical_sha256(
            list(manifest.handler_allowlist)),
        claim_contract='frozen_action_cohort_join_v2')


class ServeResourceActionAuthorityStore:
    """Closed PostgreSQL state store for the initial Serve035 primitives."""

    def __init__(self, database: sqlalchemy.engine.Engine):
        if not isinstance(database, sqlalchemy.engine.Engine):
            raise TypeError('database must be a SQLAlchemy Engine.')
        if database.dialect.name != 'postgresql':
            raise RuntimeError('Serve035 authority state requires PostgreSQL.')
        self._database = database

    def _require_transaction(self,
                             connection: sqlalchemy.engine.Connection) -> None:
        """Require one live PostgreSQL transaction owned by this store."""

        if not isinstance(connection, sqlalchemy.engine.Connection):
            raise TypeError('connection must be a SQLAlchemy Connection.')
        if connection.engine is not self._database:
            raise RuntimeError('connection does not belong to this store.')
        if connection.dialect.name != 'postgresql':
            raise RuntimeError('Serve035 authority state requires PostgreSQL.')
        if not connection.in_transaction():
            raise RuntimeError(
                'authority operations require a caller-owned transaction.')

    @staticmethod
    def _database_now(
            connection: sqlalchemy.engine.Connection) -> datetime.datetime:
        value = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        return _canonical_datetime(value, name='PostgreSQL clock')

    @staticmethod
    def _bound_bootstrap_mutation(
            connection: sqlalchemy.engine.Connection) -> None:
        """Install transaction-local PostgreSQL stop budgets before locking."""

        connection.exec_driver_sql(
            'SET LOCAL statement_timeout = '
            f"'{_BOOTSTRAP_MUTATION_STATEMENT_TIMEOUT_MILLISECONDS}ms'")
        connection.exec_driver_sql(
            'SET LOCAL lock_timeout = '
            f"'{_BOOTSTRAP_MUTATION_LOCK_TIMEOUT_MILLISECONDS}ms'")

    @staticmethod
    def _bound_preflight_trust_read(
            connection: sqlalchemy.engine.Connection) -> None:
        """Bound PostgreSQL work inside the synchronous preflight deadline."""

        connection.exec_driver_sql(
            'SET LOCAL statement_timeout = '
            f"'{_PREFLIGHT_TRUST_STATEMENT_TIMEOUT_MILLISECONDS}ms'")
        connection.exec_driver_sql(
            'SET LOCAL lock_timeout = '
            f"'{_PREFLIGHT_TRUST_LOCK_TIMEOUT_MILLISECONDS}ms'")
        connection.exec_driver_sql(
            'SET LOCAL idle_in_transaction_session_timeout = '
            f"'{_PREFLIGHT_TRUST_IDLE_TRANSACTION_TIMEOUT_MILLISECONDS}ms'")

    @staticmethod
    def _lock_release(
        connection: sqlalchemy.engine.Connection,
        *,
        cohort: authority.ProviderAuthorityWorkerCohortV2,
        helm_release_name: str,
    ) -> None:
        """Lock and exact-validate the retained release and cohort binding."""

        release_selector = authority._text(  # pylint: disable=protected-access
            helm_release_name,
            name='helm_release_name',
            maximum_bytes=63)
        manifest = cohort.manifest
        release_inputs = manifest.pod_template_binding.release_inputs
        installation_id = _cohort_id_installation_id(cohort.cohort_id)
        # The worker projection deliberately carries the rendered Helm full
        # name, not Helm's raw release name.  Select the globally unique
        # installation anchor from the immutable cohort ID, then use the
        # retained raw name for its child binding.  This remains release-first
        # lock order and works when nameOverride/fullnameOverride differ.
        release = connection.execute(
            sqlalchemy.select(state_schema.AUTHORITY_RELEASES).where(
                state_schema.AUTHORITY_RELEASES.c.namespace ==
                manifest.namespace,
                state_schema.AUTHORITY_RELEASES.c.installation_id ==
                installation_id).with_for_update()).mappings().one_or_none()
        if release is None:
            raise AuthorityStateConflict(
                'Authority release anchor does not exist.')
        release_record = decode_authority_release_ledger_row(release)
        if (not release_record.enabled or
                release_record.installation_id != installation_id or
                release_record.helm_full_name != release_inputs.helm_full_name
                or release_selector not in (release_record.helm_release_name,
                                            release_record.helm_full_name)):
            raise AuthorityStateConflict(
                'Authority release anchor does not select this cohort.')
        live_manifest = next(
            (item for item in release_record.live_manifests
             if _release_manifest_suffix(item) == release_inputs.cohort_suffix),
            None)
        if (type(live_manifest)
                is not authority.ProviderAuthorityWorkerCohortManifestV2 or
                live_manifest.canonical_bytes != manifest.canonical_bytes):
            raise AuthorityStateConflict(
                'Authority release live inventory omits the V2 manifest.')
        bound = connection.execute(
            sqlalchemy.select(state_schema.AUTHORITY_RELEASE_COHORTS).where(
                state_schema.AUTHORITY_RELEASE_COHORTS.c.namespace ==
                manifest.namespace,
                state_schema.AUTHORITY_RELEASE_COHORTS.c.helm_release_name ==
                release_record.helm_release_name,
                state_schema.AUTHORITY_RELEASE_COHORTS.c.cohort_suffix ==
                release_inputs.cohort_suffix).with_for_update()).mappings(
                ).one_or_none()
        if bound is None:
            raise AuthorityStateCorruption(
                'Authority release live inventory lacks its binding.')
        _validate_authority_release_cohort_binding(bound, release_record,
                                                   manifest)

    @staticmethod
    def _cohort_record(row: Mapping[str, Any]) -> WorkerCohortV2Record:
        try:
            cohort = authority.ProviderAuthorityWorkerCohortV2.from_value(
                row['cohort_identity'])
            cohort_hash = _canonical_hash(row['cohort_identity_sha256'],
                                          name='cohort identity hash')
            if cohort_hash != cohort.sha256:
                raise AuthorityStateCorruption(
                    'Cohort identity hash does not match.')
            if (row['cohort_id'] != cohort.cohort_id or
                    row['deployment_uid'] != cohort.deployment_uid):
                raise AuthorityStateCorruption(
                    'Cohort row identity columns do not match its value.')
            registrations = (
                authority.ProviderAuthorityWorkerRegistrationSetV2.from_value(
                    row['registration_attestations']))
            registration_hash = _canonical_hash(
                row['registration_attestations_sha256'],
                name='registration-set hash')
            if (registration_hash != registrations.sha256 or
                    registrations.cohort_identity_sha256 != cohort_hash):
                raise AuthorityStateCorruption(
                    'Registration set hash or cohort binding does not match.')
            registrations.validate_for_cohort(cohort)
            state = resource_actions.WorkerCohortLifecycleState(
                row['lifecycle_state'])
            revision = int(row['revision'])
            return WorkerCohortV2Record(
                cohort=cohort,
                registration_set=registrations,
                lifecycle_state=state,
                revision=revision,
                created_at=_canonical_datetime(row['created_at'],
                                               name='cohort.created_at'),
                state_changed_at=_canonical_datetime(
                    row['state_changed_at'], name='cohort.state_changed_at'),
                removal_authorized_at=(
                    None if row['removal_authorized_at'] is None else
                    _canonical_datetime(row['removal_authorized_at'],
                                        name='cohort.removal_authorized_at')),
                retired_at=(None if row['retired_at'] is None else
                            _canonical_datetime(row['retired_at'],
                                                name='cohort.retired_at')))
        except (KeyError, TypeError, ValueError) as e:
            raise AuthorityStateCorruption(
                'Worker cohort row is malformed.') from e

    @staticmethod
    def _lease_record(
            row: Mapping[str, Any]) -> authority.ProviderAuthorityWorkerLeaseV1:
        try:
            worker_instance_id = _row_uuid(row['worker_instance_id'],
                                           name='lease.worker_instance_id')
            pod_uid = _row_uuid(row['pod_uid'], name='lease.pod_uid')
            if worker_instance_id != pod_uid:
                raise AuthorityStateCorruption(
                    'Lease worker instance and Pod UID differ.')
            return authority.ProviderAuthorityWorkerLeaseV1(
                version=1,
                worker_instance_id=worker_instance_id,
                generation=int(row['generation']),
                state=row['state'],
                renewal_registration=(
                    authority.ProviderAuthorityWorkerRegistrationV2.from_value(
                        row['renewal_registration'])),
                renewal_registration_sha256=row['renewal_registration_sha256'],
                renewed_at=authority.datetime_to_timestamp(
                    row['renewed_at'], name='lease.renewed_at'),
                expires_at=authority.datetime_to_timestamp(
                    row['expires_at'], name='lease.expires_at'),
                revoked_at=(None if row['revoked_at'] is None else
                            authority.datetime_to_timestamp(
                                row['revoked_at'], name='lease.revoked_at')),
                revocation_reason=row['revocation_reason'],
                revocation_owner_id=row['revocation_owner_id'],
                last_operation_id=row['last_operation_id'],
                last_operation_kind=row['last_operation_kind'],
                revision=int(row['revision']))
        except (KeyError, TypeError, ValueError) as e:
            raise AuthorityStateCorruption(
                'Worker lease row is malformed.') from e

    @staticmethod
    def _lock_cohort(
        connection: sqlalchemy.engine.Connection,
        cohort_id: str,
    ) -> Mapping[str, Any] | None:
        return cast(
            Mapping[str, Any] | None,
            connection.execute(
                sqlalchemy.select(m4_schema.WORKER_COHORTS_V2).where(
                    m4_schema.WORKER_COHORTS_V2.c.cohort_id ==
                    cohort_id).with_for_update()).mappings().one_or_none())

    @staticmethod
    def _lock_lease(
        connection: sqlalchemy.engine.Connection,
        cohort_id: str,
        worker_instance_id: uuid.UUID,
    ) -> Mapping[str, Any] | None:
        return cast(
            Mapping[str, Any] | None,
            connection.execute(
                sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                    cohort_id,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                    worker_instance_id).with_for_update()).mappings().
            one_or_none())

    @staticmethod
    def _lock_reference(
        connection: sqlalchemy.engine.Connection,
        decision_id: uuid.UUID,
    ) -> Mapping[str, Any] | None:
        return cast(
            Mapping[str, Any] | None,
            connection.execute(
                sqlalchemy.select(m4_schema.WORKER_COHORT_REFS_V2).where(
                    m4_schema.WORKER_COHORT_REFS_V2.c.decision_id ==
                    decision_id).with_for_update()).mappings().one_or_none())

    def preflight_authority_release_v2(
        self,
        namespace: str,
        helm_release_name: str,
        helm_full_name: str,
        installation_id: str,
        enabled: bool,
        live_manifests: tuple[authority.ProviderAuthorityWorkerCohortManifestV2,
                              ...],
        tombstone_suffixes: tuple[str, ...],
    ) -> AuthorityReleaseLedgerRecord:
        """Atomically install an additive exact-V2 release inventory.

        V1 disable/tombstone semantics stay owned by the frozen Serve034 store.
        This additive path therefore accepts only the first post-035 V2 enable
        or an idempotent/additive V2 inventory.  Removing a V2 suffix remains
        closed until the V2 retirement protocol exists.
        """

        namespace = _release_label(namespace, name='namespace')
        helm_release_name = _release_label(helm_release_name,
                                           name='helm_release_name')
        helm_full_name = _release_label(helm_full_name, name='helm_full_name')
        if type(enabled) is not bool or not enabled:
            raise ValueError('V2 release preflight requires enabled=true.')
        try:
            proposed_installation_id = uuid.UUID(installation_id)
        except (AttributeError, TypeError, ValueError) as e:
            raise ValueError('V2 release installation ID is not a UUID.') from e
        if str(proposed_installation_id) != installation_id:
            raise ValueError(
                'V2 release installation ID is not canonical UUID text.')
        if (type(live_manifests) is not tuple or not live_manifests or any(
                type(manifest)
                is not authority.ProviderAuthorityWorkerCohortManifestV2
                for manifest in live_manifests)):
            raise TypeError(
                'V2 live_manifests must be a nonempty exact typed tuple.')
        if type(tombstone_suffixes) is not tuple or tombstone_suffixes:
            raise ValueError(
                'V2 release preflight does not yet admit tombstones.')
        suffixes = tuple(
            _release_manifest_suffix(manifest) for manifest in live_manifests)
        if suffixes != tuple(sorted(set(suffixes))):
            raise ValueError('V2 live manifests are not sorted and unique.')
        if len(suffixes) > _MAX_AUTHORITY_RELEASE_COHORTS:
            raise ValueError('V2 release inventory exceeds 256 cohorts.')
        for manifest in live_manifests:
            inputs = manifest.pod_template_binding.release_inputs
            if (manifest.namespace != namespace or
                    inputs.helm_full_name != helm_full_name or
                    _release_manifest_installation_id(manifest)
                    != proposed_installation_id):
                raise ValueError(
                    'V2 manifest differs from its proposed release identity.')

        live_values = [
            manifest.canonical_value() for manifest in live_manifests
        ]
        release_table = state_schema.AUTHORITY_RELEASES
        binding_table = state_schema.AUTHORITY_RELEASE_COHORTS
        with self._database.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(release_table).where(
                    release_table.c.namespace == namespace,
                    release_table.c.helm_release_name == helm_release_name).
                with_for_update()).mappings().one_or_none()
            if row is None:
                connection.execute(
                    postgresql.insert(release_table).values(
                        namespace=namespace,
                        helm_release_name=helm_release_name,
                        installation_id=proposed_installation_id,
                        helm_full_name=helm_full_name,
                        enabled=True,
                        live_manifests=live_values,
                        live_inventory_sha256=authority.canonical_sha256(
                            live_values),
                        tombstone_suffixes=[],
                        tombstone_inventory_sha256=authority.canonical_sha256(
                            []),
                        revision=1,
                        created_at=sqlalchemy.func.clock_timestamp(),
                        updated_at=sqlalchemy.func.clock_timestamp()).
                    on_conflict_do_nothing())
                row = connection.execute(
                    sqlalchemy.select(release_table).where(
                        release_table.c.namespace == namespace,
                        release_table.c.helm_release_name == helm_release_name).
                    with_for_update()).mappings().one_or_none()
                if row is None:
                    collision = connection.execute(
                        sqlalchemy.select(release_table.c.namespace).where(
                            release_table.c.installation_id ==
                            proposed_installation_id).with_for_update()
                    ).scalar_one_or_none()
                    if collision is not None:
                        raise AuthorityStateConflict(
                            'V2 installation ID belongs to another release.')
                    raise AuthorityStateConflict(
                        'V2 release identity could not be bound.')

            current = decode_authority_release_ledger_row(row)
            if (current.installation_id != proposed_installation_id or
                    current.helm_full_name != helm_full_name):
                raise AuthorityStateConflict(
                    'V2 release immutable identity changed.')
            if current.tombstone_suffixes:
                raise AuthorityStateConflict(
                    'V2 activation requires the V1 tombstone inventory cleared.'
                )
            current_by_suffix: dict[
                str, authority.ProviderAuthorityWorkerCohortManifestV2] = {}
            for installed in current.live_manifests:
                if type(installed) is not (
                        authority.ProviderAuthorityWorkerCohortManifestV2):
                    raise AuthorityStateConflict(
                        'V2 activation requires the V1 live inventory cleared.')
                current_by_suffix[_release_manifest_suffix(
                    installed)] = installed
            proposed_by_suffix = {
                _release_manifest_suffix(manifest): manifest
                for manifest in live_manifests
            }
            for suffix, installed in current_by_suffix.items():
                proposed = proposed_by_suffix.get(suffix)
                if (proposed is None or
                        proposed.canonical_bytes != installed.canonical_bytes):
                    raise AuthorityStateConflict(
                        'V2 release preflight cannot remove or replace a live '
                        'cohort.')

            for manifest in live_manifests:
                suffix = _release_manifest_suffix(manifest)
                connection.execute(
                    postgresql.insert(binding_table).values(
                        namespace=namespace,
                        helm_release_name=helm_release_name,
                        cohort_suffix=suffix,
                        cohort_id=manifest.cohort_id,
                        manifest=manifest.canonical_value(),
                        manifest_sha256=manifest.sha256,
                        bound_at=sqlalchemy.func.clock_timestamp()).
                    on_conflict_do_nothing())
                binding = connection.execute(
                    sqlalchemy.select(binding_table).where(
                        binding_table.c.namespace == namespace,
                        binding_table.c.helm_release_name == helm_release_name,
                        binding_table.c.cohort_suffix ==
                        suffix).with_for_update()).mappings().one_or_none()
                if binding is None:
                    raise AuthorityStateConflict(
                        'V2 cohort ID is permanently bound outside this release.'
                    )
                _validate_authority_release_cohort_binding(
                    binding, current, manifest)

            desired_hash = authority.canonical_sha256(live_values)
            if ([
                    manifest.canonical_value()
                    for manifest in current.live_manifests
            ] != live_values or not current.enabled):
                expected_revision = current.revision + 1
                updated = connection.execute(
                    sqlalchemy.update(release_table).where(
                        release_table.c.namespace == namespace,
                        release_table.c.helm_release_name == helm_release_name,
                        release_table.c.revision == current.revision).values(
                            enabled=True,
                            live_manifests=live_values,
                            live_inventory_sha256=desired_hash,
                            tombstone_suffixes=[],
                            tombstone_inventory_sha256=(
                                authority.canonical_sha256([])),
                            revision=expected_revision,
                            updated_at=sqlalchemy.func.clock_timestamp()))
                if updated.rowcount != 1:
                    raise AuthorityStateConflict(
                        'V2 release update lost its revision CAS.')
                row = connection.execute(
                    sqlalchemy.select(release_table).where(
                        release_table.c.namespace == namespace,
                        release_table.c.helm_release_name ==
                        helm_release_name).with_for_update()).mappings().one()
                current = decode_authority_release_ledger_row(row)
            if (not current.enabled or
                    current.installation_id != proposed_installation_id or [
                        manifest.canonical_value()
                        for manifest in current.live_manifests
                    ] != live_values or current.tombstone_suffixes):
                raise AuthorityStateCorruption(
                    'V2 release readback differs from the proposed fence.')
            return current

    def read_database_clock(self) -> datetime.datetime:
        """Read the PostgreSQL clock used to timestamp live observations."""

        with self._database.connect() as connection:
            return self._database_now(connection)

    def read_worker_bootstrap_state(
        self,
        cohort_id: str,
        worker_instance_id: uuid.UUID,
    ) -> WorkerBootstrapState | None:
        """Read and fully type-check one cohort plus the caller's lease.

        Mutating successors still acquire their documented locks and CAS the
        complete predecessor.  This read exists only so a restarted worker or
        a lost acknowledgement can choose the exact successor operation
        instead of blindly replaying an insert or append.
        """

        cohort_key = authority._text(  # pylint: disable=protected-access
            cohort_id, name='cohort_id')
        worker_id = authority._uuid(  # pylint: disable=protected-access
            worker_instance_id,
            name='worker_instance_id')
        with self._database.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(m4_schema.WORKER_COHORTS_V2).where(
                    m4_schema.WORKER_COHORTS_V2.c.cohort_id ==
                    cohort_key)).mappings().one_or_none()
            if row is None:
                return None
            record = self._cohort_record(row)
            lease_row = connection.execute(
                sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                    cohort_key,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                    worker_id)).mappings().one_or_none()
            lease = (None
                     if lease_row is None else self._lease_record(lease_row))
            registration = record.registration_set.registration_for(worker_id)
            if registration is not None:
                if lease is None:
                    raise AuthorityStateCorruption(
                        'Bootstrap member has no registration lease.')
                self._validate_anchor_lease(lease, registration)
            return WorkerBootstrapState(record, lease)

    @staticmethod
    def _registration_at(
        worker: authority.ProviderAuthorityWorkerIdentityV2,
        database_now: datetime.datetime,
    ) -> authority.ProviderAuthorityWorkerRegistrationV2:
        observed = authority.timestamp_to_datetime(worker.observed_at,
                                                   name='worker.observed_at')
        if observed > database_now or observed < database_now - _REGISTRATION_MAX_AGE:
            raise AuthorityStateConflict(
                'Worker observation is future or older than five minutes.')
        return authority.ProviderAuthorityWorkerRegistrationV2(
            version=2,
            worker_instance_id=worker.pod_uid,
            worker=worker,
            pod_ready=True,
            registered_at=authority.datetime_to_timestamp(database_now,
                                                          name='database_now'))

    @staticmethod
    def _lease_values(
        cohort_id: str,
        registration: authority.ProviderAuthorityWorkerRegistrationV2,
        operation_id: uuid.UUID,
        database_now: datetime.datetime,
    ) -> dict[str, Any]:
        return {
            'cohort_id': cohort_id,
            'worker_instance_id': registration.worker_instance_id,
            'pod_uid': registration.worker_instance_id,
            'generation': 1,
            'state': authority.WorkerRegistrationLeaseState.ACTIVE.value,
            'renewal_registration': registration.canonical_value(),
            'renewal_registration_sha256': registration.sha256,
            'renewed_at': database_now,
            'expires_at': database_now + _LEASE_TTL,
            'revoked_at': None,
            'revocation_reason': None,
            'revocation_owner_id': None,
            'last_operation_id': operation_id,
            'last_operation_kind':
                authority.WorkerRegistrationLeaseOperation.INSERT.value,
            'revision': 1,
        }

    @staticmethod
    def _validate_anchor_lease(
        lease: authority.ProviderAuthorityWorkerLeaseV1,
        anchor: authority.ProviderAuthorityWorkerRegistrationV2,
        *,
        operation_id: uuid.UUID | None = None,
    ) -> None:
        if lease.state is not authority.WorkerRegistrationLeaseState.ACTIVE:
            raise AuthorityStateConflict(
                'Registration anchor lease is revoked.')
        if (lease.worker_instance_id != anchor.worker_instance_id or
                authority.project_stable_worker_identity_v1(
                    lease.renewal_registration.worker).canonical_bytes
                != authority.project_stable_worker_identity_v1(
                    anchor.worker).canonical_bytes):
            raise AuthorityStateCorruption(
                'Registration anchor and lease lineage differ.')
        # Once the immutable anchor's lease has been renewed, the insert
        # operation is no longer the lease row's last operation.  The active
        # lease shape and stable-identity projection above still prove the
        # same lineage, so a replay of the original registration/append may
        # adopt it.  Generation one has no intervening renewal and therefore
        # must retain the exact insert acknowledgement.
        if operation_id is not None and lease.generation == 1 and (
                lease.revision != 1 or lease.last_operation_id != operation_id
                or lease.last_operation_kind
                is not authority.WorkerRegistrationLeaseOperation.INSERT or
                lease.renewal_registration.canonical_bytes
                != anchor.canonical_bytes):
            raise AuthorityStateConflict(
                'Lost registration insert does not exactly adopt its lease.')

    @staticmethod
    def _activation_api_heartbeat(
        row: Mapping[str, Any],
        registration: authority.ProviderAuthorityWorkerRegistrationV2,
    ) -> datetime.datetime:
        """Validate one locked bootstrap-only authority server instance."""

        try:
            instance_id = _row_uuid(row['instance_id'],
                                    name='api_instance.instance_id')
            heartbeat = _canonical_datetime(row['heartbeat_at'],
                                            name='api_instance.heartbeat_at')
            if (instance_id != registration.worker_instance_id or
                    row['pod_uid'] != str(registration.worker.pod_uid) or
                    row['role'] != 'authority-worker' or
                    type(row['ready']) is not bool or row['ready'] or
                    row['draining_at'] is not None):
                raise AuthorityStateConflict(
                    'API server instance is not the exact bootstrap-only '
                    'authority worker.')
            return heartbeat
        except KeyError as e:
            raise AuthorityStateCorruption(
                'API server instance row is malformed.') from e

    def register_initial_member(
        self,
        *,
        helm_release_name: str,
        cohort: authority.ProviderAuthorityWorkerCohortV2,
        worker: authority.ProviderAuthorityWorkerIdentityV2,
        operation_id: uuid.UUID,
    ) -> WorkerRegistrationMutation:
        """Atomically create/adopt one ``REGISTERING`` anchor and lease."""

        if type(cohort) is not authority.ProviderAuthorityWorkerCohortV2:
            raise TypeError('cohort must be a V2 worker cohort.')
        if type(worker) is not authority.ProviderAuthorityWorkerIdentityV2:
            raise TypeError('worker must be a V2 worker identity.')
        worker.validate_for_cohort(cohort)
        operation = authority._uuid(  # pylint: disable=protected-access
            operation_id,
            name='operation_id')
        with self._database.begin() as connection:
            self._bound_bootstrap_mutation(connection)
            self._lock_release(connection,
                               cohort=cohort,
                               helm_release_name=helm_release_name)
            existing = self._lock_cohort(connection, cohort.cohort_id)
            if existing is not None:
                record = self._cohort_record(existing)
                if (record.lifecycle_state is not resource_actions.
                        WorkerCohortLifecycleState.REGISTERING or
                        record.revision != 1 or record.cohort.canonical_bytes
                        != cohort.canonical_bytes or
                        len(record.registration_set.workers) != 1 or
                        record.registration_set.workers[0].worker.
                        canonical_bytes != worker.canonical_bytes):
                    raise AuthorityStateConflict(
                        'Initial registration was superseded or conflicts.')
                lease_row = self._lock_lease(connection, cohort.cohort_id,
                                             worker.pod_uid)
                if lease_row is None:
                    raise AuthorityStateCorruption(
                        'Initial registration anchor has no lease.')
                lease = self._lease_record(lease_row)
                self._validate_anchor_lease(lease,
                                            record.registration_set.workers[0],
                                            operation_id=operation)
                return WorkerRegistrationMutation(record, lease, adopted=True)

            database_now = self._database_now(connection)
            registration = self._registration_at(worker, database_now)
            registration_set = authority.ProviderAuthorityWorkerRegistrationSetV2(
                version=2,
                cohort_identity_sha256=cohort.sha256,
                revision=1,
                deployment_snapshot=None,
                workers=(registration,))
            registration_set.validate_for_cohort(cohort)
            inserted = connection.execute(
                postgresql.insert(m4_schema.WORKER_COHORTS_V2).values(
                    cohort_id=cohort.cohort_id,
                    deployment_uid=cohort.deployment_uid,
                    cohort_identity=cohort.canonical_value(),
                    cohort_identity_sha256=cohort.sha256,
                    registration_attestations=registration_set.canonical_value(
                    ),
                    registration_attestations_sha256=registration_set.sha256,
                    lifecycle_state=(resource_actions.WorkerCohortLifecycleState
                                     .REGISTERING.value),
                    revision=1,
                    created_at=database_now,
                    state_changed_at=database_now,
                    removal_authorized_at=None,
                    retired_at=None).
                on_conflict_do_nothing(
                    index_elements=[m4_schema.WORKER_COHORTS_V2.c.cohort_id]))
            if inserted.rowcount != 1:
                raise AuthorityStateConflict(
                    'Concurrent initial registration won; retry for exact '
                    'adoption.')
            lease_inserted = connection.execute(
                postgresql.insert(m4_schema.WORKER_REGISTRATION_LEASES).values(
                    **self._lease_values(cohort.cohort_id, registration,
                                         operation, database_now)).
                on_conflict_do_nothing(index_elements=[
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id
                ]))
            if lease_inserted.rowcount != 1:
                raise AuthorityStateCorruption(
                    'Initial cohort insert encountered an orphan lease.')
            row = self._lock_cohort(connection, cohort.cohort_id)
            lease_row = self._lock_lease(connection, cohort.cohort_id,
                                         worker.pod_uid)
            assert row is not None and lease_row is not None
            return WorkerRegistrationMutation(self._cohort_record(row),
                                              self._lease_record(lease_row))

    def append_registering_member(
        self,
        *,
        helm_release_name: str,
        cohort: authority.ProviderAuthorityWorkerCohortV2,
        worker: authority.ProviderAuthorityWorkerIdentityV2,
        expected_cohort_revision: int,
        operation_id: uuid.UUID,
    ) -> WorkerRegistrationMutation:
        """Append/adopt the second immutable anchor and its first lease."""

        if expected_cohort_revision != 1:
            raise ValueError('second-member append requires revision one.')
        if type(cohort) is not authority.ProviderAuthorityWorkerCohortV2:
            raise TypeError('cohort must be a V2 worker cohort.')
        if type(worker) is not authority.ProviderAuthorityWorkerIdentityV2:
            raise TypeError('worker must be a V2 worker identity.')
        worker.validate_for_cohort(cohort)
        operation = authority._uuid(  # pylint: disable=protected-access
            operation_id,
            name='operation_id')
        with self._database.begin() as connection:
            self._bound_bootstrap_mutation(connection)
            self._lock_release(connection,
                               cohort=cohort,
                               helm_release_name=helm_release_name)
            row = self._lock_cohort(connection, cohort.cohort_id)
            if row is None:
                raise AuthorityStateConflict('Registering cohort is absent.')
            record = self._cohort_record(row)
            if record.cohort.canonical_bytes != cohort.canonical_bytes:
                raise AuthorityStateConflict('Cohort identity changed.')
            if (record.lifecycle_state is not resource_actions.
                    WorkerCohortLifecycleState.REGISTERING):
                raise AuthorityStateConflict('Cohort is no longer REGISTERING.')
            if record.revision == expected_cohort_revision + 1:
                installed = record.registration_set.registration_for(
                    worker.pod_uid)
                if (installed is None or installed.worker.canonical_bytes
                        != worker.canonical_bytes):
                    raise AuthorityStateSuperseded(
                        'Second-member append was superseded.')
                lease_row = self._lock_lease(connection, cohort.cohort_id,
                                             worker.pod_uid)
                if lease_row is None:
                    raise AuthorityStateCorruption(
                        'Appended anchor has no registration lease.')
                lease = self._lease_record(lease_row)
                self._validate_anchor_lease(lease,
                                            installed,
                                            operation_id=operation)
                return WorkerRegistrationMutation(record, lease, adopted=True)
            if record.revision != expected_cohort_revision:
                raise AuthorityStateSuperseded(
                    'Second-member append lost its cohort revision.')
            if len(record.registration_set.workers) != 1:
                raise AuthorityStateCorruption(
                    'Revision-one REGISTERING cohort is not one-member.')
            anchor = record.registration_set.workers[0]
            if anchor.worker_instance_id == worker.pod_uid:
                raise AuthorityStateConflict(
                    'Second registration must have a distinct Pod UID.')
            # Lock all extant lease keys in canonical UUID order before the
            # absent candidate key is inserted.
            lease_rows = connection.execute(
                sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                    cohort.cohort_id,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id.
                    in_([anchor.worker_instance_id, worker.pod_uid])).order_by(
                        m4_schema.WORKER_REGISTRATION_LEASES.c.
                        worker_instance_id).with_for_update()).mappings().all()
            if len(lease_rows) != 1:
                raise AuthorityStateConflict(
                    'Candidate lease already exists or anchor lease is absent.')
            anchor_lease = self._lease_record(lease_rows[0])
            self._validate_anchor_lease(anchor_lease, anchor)
            database_now = self._database_now(connection)
            registration = self._registration_at(worker, database_now)
            workers = tuple(
                sorted((anchor, registration),
                       key=lambda item: item.worker_instance_id.bytes))
            successor = authority.ProviderAuthorityWorkerRegistrationSetV2(
                version=2,
                cohort_identity_sha256=cohort.sha256,
                revision=expected_cohort_revision + 1,
                deployment_snapshot=None,
                workers=workers)
            successor.validate_for_cohort(cohort)
            lease_inserted = connection.execute(
                postgresql.insert(m4_schema.WORKER_REGISTRATION_LEASES).values(
                    **self._lease_values(cohort.cohort_id, registration,
                                         operation, database_now)).
                on_conflict_do_nothing(index_elements=[
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id
                ]))
            if lease_inserted.rowcount != 1:
                raise AuthorityStateConflict(
                    'Second-member lease insert lost a concurrent race.')
            updated = connection.execute(
                sqlalchemy.update(m4_schema.WORKER_COHORTS_V2).where(
                    m4_schema.WORKER_COHORTS_V2.c.cohort_id == cohort.cohort_id,
                    m4_schema.WORKER_COHORTS_V2.c.lifecycle_state ==
                    resource_actions.WorkerCohortLifecycleState.REGISTERING.
                    value, m4_schema.WORKER_COHORTS_V2.c.revision ==
                    expected_cohort_revision).values(
                        registration_attestations=successor.canonical_value(),
                        registration_attestations_sha256=successor.sha256,
                        revision=expected_cohort_revision + 1,
                        state_changed_at=sqlalchemy.func.greatest(
                            sqlalchemy.func.clock_timestamp(),
                            m4_schema.WORKER_COHORTS_V2.c.state_changed_at)))
            if updated.rowcount != 1:
                raise AuthorityStateConflict(
                    'Second-member append lost its cohort CAS.')
            final_row = self._lock_cohort(connection, cohort.cohort_id)
            final_lease = self._lock_lease(connection, cohort.cohort_id,
                                           worker.pod_uid)
            assert final_row is not None and final_lease is not None
            return WorkerRegistrationMutation(self._cohort_record(final_row),
                                              self._lease_record(final_lease))

    def activate_initial_cohort(
        self,
        *,
        cohort_id: str,
        expected_cohort_revision: int,
        deployment_snapshot: authority.
        ProviderAuthorityWorkerDeploymentSnapshotV2,
    ) -> WorkerCohortActivationMutation:
        """Atomically install current renewals and accept a V2 cohort.

        The caller supplies only the exact CAS identity and the typed final
        Kubernetes snapshot.  Registration and API-instance evidence is read
        from, locked in, and revalidated against server-owned durable rows.
        """

        cohort_key = authority._text(  # pylint: disable=protected-access
            cohort_id, name='cohort_id')
        expected_revision = authority._positive_integer(  # pylint: disable=protected-access
            expected_cohort_revision,
            name='expected_cohort_revision')
        if expected_revision != 2:
            raise ValueError('initial activation requires cohort revision two.')
        if type(deployment_snapshot
               ) is not authority.ProviderAuthorityWorkerDeploymentSnapshotV2:
            raise TypeError('deployment_snapshot must be a typed V2 snapshot.')

        with self._database.begin() as connection:
            self._bound_bootstrap_mutation(connection)
            # Global order classes 3 -> 5 -> 14: cohort, both registration
            # leases, then both API server-instance rows.  There is no
            # Kubernetes I/O or caller-projected durable evidence under lock.
            row = self._lock_cohort(connection, cohort_key)
            if row is None:
                raise AuthorityStateConflict('Registering cohort is absent.')
            record = self._cohort_record(row)

            if (record.lifecycle_state
                    is resource_actions.WorkerCohortLifecycleState.ACCEPTING):
                installed_snapshot = record.registration_set.deployment_snapshot
                if (record.revision != expected_revision + 1 or
                        installed_snapshot is None or
                        installed_snapshot.canonical_bytes
                        != deployment_snapshot.canonical_bytes):
                    raise AuthorityStateSuperseded(
                        'Initial activation acknowledgement was superseded.')
                return WorkerCohortActivationMutation(record, adopted=True)

            if (record.lifecycle_state is not resource_actions.
                    WorkerCohortLifecycleState.REGISTERING or
                    record.revision != expected_revision):
                raise AuthorityStateSuperseded(
                    'Initial activation lost its cohort state/revision CAS.')
            anchors = record.registration_set.workers
            if len(anchors) != 2:
                raise AuthorityStateConflict(
                    'Initial activation requires exactly two V2 anchors.')
            worker_ids = tuple(anchor.worker_instance_id for anchor in anchors)
            if worker_ids != tuple(
                    sorted(set(worker_ids), key=lambda item: item.bytes)):
                raise AuthorityStateCorruption(
                    'Registering anchors are not distinct and canonical.')

            lease_rows = connection.execute(
                sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                    cohort_key,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id.
                    in_(worker_ids)).order_by(
                        m4_schema.WORKER_REGISTRATION_LEASES.c.
                        worker_instance_id).with_for_update()).mappings().all()
            if (len(lease_rows) != 2 or tuple(
                    _row_uuid(lease_row['worker_instance_id'],
                              name='lease.worker_instance_id')
                    for lease_row in lease_rows) != worker_ids):
                raise AuthorityStateConflict(
                    'Both canonical registration leases must exist.')

            leases: list[authority.ProviderAuthorityWorkerLeaseV1] = []
            installed_registrations: list[
                authority.ProviderAuthorityWorkerRegistrationV2] = []
            for anchor, lease_row in zip(anchors, lease_rows):
                lease = self._lease_record(lease_row)
                self._validate_anchor_lease(lease, anchor)
                try:
                    lease.renewal_registration.worker.validate_for_cohort(
                        record.cohort)
                except (TypeError, ValueError) as e:
                    raise AuthorityStateCorruption(
                        'Lease renewal registration does not match its cohort.'
                    ) from e
                leases.append(lease)
                installed_registrations.append(lease.renewal_registration)

            try:
                successor = authority.ProviderAuthorityWorkerRegistrationSetV2(
                    version=2,
                    cohort_identity_sha256=record.cohort.sha256,
                    revision=expected_revision + 1,
                    deployment_snapshot=deployment_snapshot,
                    workers=tuple(installed_registrations))
                successor.validate_for_cohort(record.cohort)
                successor.validate_accepted()
            except (TypeError, ValueError) as e:
                raise AuthorityStateConflict(
                    'Final Deployment snapshot or worker identity drifted.'
                ) from e

            api_rows = connection.execute(
                sqlalchemy.select(
                    request_postgres_schema.SERVER_INSTANCES).where(
                        request_postgres_schema.SERVER_INSTANCES.c.instance_id.
                        in_(worker_ids)).order_by(
                            request_postgres_schema.SERVER_INSTANCES.c.
                            instance_id).with_for_update()).mappings().all()
            if (len(api_rows) != 2 or tuple(
                    _row_uuid(api_row['instance_id'],
                              name='api_instance.instance_id')
                    for api_row in api_rows) != worker_ids):
                raise AuthorityStateConflict(
                    'Both canonical API server instances must exist.')
            api_heartbeats = tuple(
                self._activation_api_heartbeat(api_row, registration) for
                api_row, registration in zip(api_rows, installed_registrations))

            transition_time = max(
                self._database_now(connection), record.state_changed_at,
                *(authority.timestamp_to_datetime(lease.renewed_at,
                                                  name='lease.renewed_at')
                  for lease in leases))
            updated = connection.execute(
                sqlalchemy.update(m4_schema.WORKER_COHORTS_V2).where(
                    m4_schema.WORKER_COHORTS_V2.c.cohort_id == cohort_key,
                    m4_schema.WORKER_COHORTS_V2.c.deployment_uid ==
                    record.cohort.deployment_uid,
                    m4_schema.WORKER_COHORTS_V2.c.cohort_identity ==
                    record.cohort.canonical_value(),
                    m4_schema.WORKER_COHORTS_V2.c.cohort_identity_sha256 ==
                    record.cohort.sha256,
                    m4_schema.WORKER_COHORTS_V2.c.registration_attestations ==
                    record.registration_set.canonical_value(), m4_schema.
                    WORKER_COHORTS_V2.c.registration_attestations_sha256 ==
                    record.registration_set.sha256,
                    m4_schema.WORKER_COHORTS_V2.c.lifecycle_state ==
                    resource_actions.WorkerCohortLifecycleState.REGISTERING.
                    value,
                    m4_schema.WORKER_COHORTS_V2.c.revision == expected_revision,
                    m4_schema.WORKER_COHORTS_V2.c.state_changed_at ==
                    record.state_changed_at,
                    m4_schema.WORKER_COHORTS_V2.c.removal_authorized_at.is_(None
                                                                           ),
                    m4_schema.WORKER_COHORTS_V2.c.retired_at.is_(None)).values(
                        registration_attestations=successor.canonical_value(),
                        registration_attestations_sha256=successor.sha256,
                        lifecycle_state=(
                            resource_actions.WorkerCohortLifecycleState.
                            ACCEPTING.value),
                        revision=expected_revision + 1,
                        state_changed_at=transition_time))
            if updated.rowcount != 1:
                raise AuthorityStateConflict(
                    'Initial activation lost its exact cohort CAS.')

            # This is deliberately after the optimistic write.  Any lock wait
            # that consumes a registration or API bootstrap lease, or ages the
            # snapshot/registration evidence out, raises and rolls back that
            # write before the transaction can commit.
            precommit_now = self._database_now(connection)
            try:
                successor.validate_freshness(precommit_now)
            except (TypeError, ValueError) as e:
                raise AuthorityStateConflict(
                    'Activation evidence is not fresh at precommit.') from e
            if any(not lease.is_fresh(precommit_now) for lease in leases):
                raise AuthorityStateConflict(
                    'Registration lease expired before activation commit.')
            if any(heartbeat > precommit_now or heartbeat +
                   _API_INSTANCE_LEASE_TTL <= precommit_now
                   for heartbeat in api_heartbeats):
                raise AuthorityStateConflict(
                    'API bootstrap lease expired before activation commit.')

            accepted = WorkerCohortV2Record(
                cohort=record.cohort,
                registration_set=successor,
                lifecycle_state=(
                    resource_actions.WorkerCohortLifecycleState.ACCEPTING),
                revision=expected_revision + 1,
                created_at=record.created_at,
                state_changed_at=transition_time,
                removal_authorized_at=None,
                retired_at=None)
            return WorkerCohortActivationMutation(accepted)

    def renew_own_lease(
        self,
        *,
        cohort_id: str,
        worker: authority.ProviderAuthorityWorkerIdentityV2,
        expected_generation: int,
        operation_id: uuid.UUID,
    ) -> WorkerRegistrationMutation:
        """Renew one authorized member/candidate lease by exact CAS."""

        cohort_key = authority._text(  # pylint: disable=protected-access
            cohort_id, name='cohort_id')
        if type(worker) is not authority.ProviderAuthorityWorkerIdentityV2:
            raise TypeError('worker must be a V2 worker identity.')
        generation = authority._positive_integer(  # pylint: disable=protected-access
            expected_generation,
            name='expected_generation')
        operation = authority._uuid(  # pylint: disable=protected-access
            operation_id,
            name='operation_id')
        with self._database.begin() as connection:
            self._bound_bootstrap_mutation(connection)
            row = self._lock_cohort(connection, cohort_key)
            if row is None:
                raise AuthorityStateConflict('Lease cohort is absent.')
            record = self._cohort_record(row)
            if record.lifecycle_state not in (
                    resource_actions.WorkerCohortLifecycleState.REGISTERING,
                    resource_actions.WorkerCohortLifecycleState.ACCEPTING,
                    resource_actions.WorkerCohortLifecycleState.DRAINING):
                raise AuthorityStateConflict(
                    'Cohort lifecycle does not authorize lease renewal.')
            handoff = connection.execute(
                sqlalchemy.select(m4_schema.WORKER_REGISTRATION_HANDOFFS).where(
                    m4_schema.WORKER_REGISTRATION_HANDOFFS.c.cohort_id ==
                    cohort_key,
                    m4_schema.WORKER_REGISTRATION_HANDOFFS.c.handoff_state.in_(
                        ('OPEN', 'READY'))).order_by(
                            m4_schema.WORKER_REGISTRATION_HANDOFFS.c.handoff_id
                        ).with_for_update()).mappings().one_or_none()
            authorizer = record.registration_set.registration_for(
                worker.pod_uid)
            if authorizer is None and handoff is not None:
                try:
                    candidate = (authority.ProviderAuthorityWorkerRegistrationV2
                                 .from_value(handoff['candidate_registration']))
                except (TypeError, ValueError) as e:
                    raise AuthorityStateCorruption(
                        'Nonterminal handoff candidate is malformed.') from e
                candidate_hash = _canonical_hash(
                    handoff['candidate_registration_sha256'],
                    name='handoff.candidate_registration_sha256')
                if candidate_hash != candidate.sha256:
                    raise AuthorityStateCorruption(
                        'Nonterminal handoff candidate hash does not match.')
                if (candidate.worker_instance_id == worker.pod_uid and
                        handoff['candidate_worker_instance_id']
                        == worker.pod_uid and
                        handoff['candidate_pod_uid'] == worker.pod_uid):
                    authorizer = candidate
            if authorizer is None:
                raise AuthorityStateConflict(
                    'Worker is not an authorized member or handoff candidate.')
            if (authority.project_stable_worker_identity_v1(
                    authorizer.worker).canonical_bytes
                    != authority.project_stable_worker_identity_v1(
                        worker).canonical_bytes):
                raise AuthorityStateConflict(
                    'Renewal worker stable identity drifted.')
            lease_row = self._lock_lease(connection, cohort_key, worker.pod_uid)
            if lease_row is None:
                raise AuthorityStateConflict('Registration lease is absent.')
            lease = self._lease_record(lease_row)
            if (lease.last_operation_id == operation and
                    lease.last_operation_kind
                    is authority.WorkerRegistrationLeaseOperation.RENEW and
                    lease.generation == generation + 1 and
                    lease.revision == generation + 1 and
                    lease.renewal_registration.worker.canonical_bytes
                    == worker.canonical_bytes):
                return WorkerRegistrationMutation(record, lease, adopted=True)
            if (lease.state is not authority.WorkerRegistrationLeaseState.ACTIVE
                    or lease.generation != generation or
                    lease.revision != generation):
                raise AuthorityStateSuperseded(
                    'Lease renewal lost its generation/revision CAS.')
            database_now = self._database_now(connection)
            registration = self._registration_at(worker, database_now)
            updated = connection.execute(
                sqlalchemy.update(m4_schema.WORKER_REGISTRATION_LEASES).where(
                    m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                    cohort_key,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                    worker.pod_uid, m4_schema.WORKER_REGISTRATION_LEASES.c.state
                    == authority.WorkerRegistrationLeaseState.ACTIVE.value,
                    m4_schema.WORKER_REGISTRATION_LEASES.c.generation ==
                    generation, m4_schema.WORKER_REGISTRATION_LEASES.c.revision
                    == generation).values(
                        generation=generation + 1,
                        revision=generation + 1,
                        renewal_registration=registration.canonical_value(),
                        renewal_registration_sha256=registration.sha256,
                        renewed_at=database_now,
                        expires_at=database_now + _LEASE_TTL,
                        last_operation_id=operation,
                        last_operation_kind=(
                            authority.WorkerRegistrationLeaseOperation.RENEW.
                            value)))
            if updated.rowcount != 1:
                raise AuthorityStateConflict('Lease renewal lost its CAS.')
            final = self._lock_lease(connection, cohort_key, worker.pod_uid)
            assert final is not None
            return WorkerRegistrationMutation(record, self._lease_record(final))

    @staticmethod
    def _service_owner_fence(row: Mapping[str, Any]) -> str:
        return f"{row['controller_pid']}:{row['controller_ip']}"

    @staticmethod
    def _lock_service(
        connection: sqlalchemy.engine.Connection,
        service_name: str,
    ) -> Mapping[str, Any] | None:
        return cast(
            Mapping[str, Any] | None,
            connection.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    service_name).with_for_update()).mappings().one_or_none())

    @staticmethod
    def _validate_service_fence(
        row: Mapping[str, Any],
        fence: authority.AuthorityServiceFenceV1,
    ) -> None:
        if (row['name'] != fence.service_name or
                row['hash'] != fence.service_hash or
                row['lifecycle_epoch'] != fence.lifecycle_epoch or
                ServeResourceActionAuthorityStore._service_owner_fence(row)
                != fence.controller_owner_fence):
            raise AuthorityStateConflict(
                'Service owner/incarnation/lifecycle fence changed.')

    @staticmethod
    def _policy_record(row: Mapping[str, Any]) -> AuthorityPolicyEpochRecord:
        try:
            service_hash = str(
                authority._schema_uuid(  # pylint: disable=protected-access
                    row['service_hash'],
                    name='policy.service_hash'))
            policy = authority.ResourceActionQualificationPolicyV1.from_value(
                row['policy'])
            policy_hash = _canonical_hash(row['policy_sha256'],
                                          name='policy.policy_sha256')
            if policy_hash != policy.sha256:
                raise AuthorityStateCorruption('Policy hash does not match.')
            reason = row['reason']
            proof: (authority.AuthoritativePromotionProofV1 |
                    authority.ServeAuthorityPolicyRotationProofV1)
            if reason == 'INITIAL_PROMOTION':
                proof = authority.AuthoritativePromotionProofV1.from_value(
                    row['rotation_proof'])
            elif reason == 'COMPATIBLE_IMAGE_ROTATION':
                proof = authority.ServeAuthorityPolicyRotationProofV1.from_value(
                    row['rotation_proof'])
            else:
                raise AuthorityStateCorruption(
                    'Policy reason is not in the closed set.')
            proof_hash = _canonical_hash(row['rotation_proof_sha256'],
                                         name='policy.rotation_proof_sha256')
            if proof_hash != proof.sha256:
                raise AuthorityStateCorruption(
                    'Policy rotation-proof hash does not match.')
            inventory = authority.AuthorityNonterminalInventoryV1.from_value(
                row['nonterminal_inventory'])
            inventory_hash = _canonical_hash(
                row['nonterminal_inventory_sha256'],
                name='policy.nonterminal_inventory_sha256')
            if inventory_hash != inventory.sha256:
                raise AuthorityStateCorruption(
                    'Policy nonterminal-inventory hash does not match.')
            activated = _canonical_datetime(row['activated_at'],
                                            name='policy.activated_at')
            created = _canonical_datetime(row['created_at'],
                                          name='policy.created_at')
            admission_changed = _canonical_datetime(
                row['admission_changed_at'], name='policy.admission_changed_at')
            superseded = (None if row['superseded_at'] is None else
                          _canonical_datetime(row['superseded_at'],
                                              name='policy.superseded_at'))
            policy_epoch = _row_uuid(row['policy_epoch'],
                                     name='policy.policy_epoch')
            predecessor_epoch = (None if row['predecessor_policy_epoch'] is None
                                 else _row_uuid(
                                     row['predecessor_policy_epoch'],
                                     name='policy.predecessor_policy_epoch'))
            binding_hash = _canonical_hash(
                row['authority_binding_sha256'],
                name='policy.authority_binding_sha256')
            policy_state = AuthorityPolicyState(row['policy_state'])
            admission_state = AuthorityPolicyAdmissionState(
                row['admission_state'])
            admission_revision = row['admission_revision']
            if type(admission_revision) is not int or admission_revision <= 0:
                raise AuthorityStateCorruption(
                    'Policy admission revision is not a positive integer.')
            operation = AuthorityPolicyOperation(row['last_operation_kind'])
            if policy_state is AuthorityPolicyState.ACTIVE:
                if admission_state is AuthorityPolicyAdmissionState.OPEN:
                    operation_shape = (
                        (admission_revision == 1 and
                         operation is AuthorityPolicyOperation.ACTIVATE) or
                        (admission_revision >= 4 and
                         admission_revision % 3 == 1 and
                         operation is AuthorityPolicyOperation.REOPEN))
                elif admission_state is (
                        AuthorityPolicyAdmissionState.DRAINING):
                    operation_shape = (admission_revision >= 2 and
                                       admission_revision % 3 == 2 and operation
                                       is AuthorityPolicyOperation.DRAIN)
                else:
                    operation_shape = (
                        admission_state is AuthorityPolicyAdmissionState.CLOSED
                        and admission_revision >= 3 and
                        admission_revision % 3 == 0 and
                        operation is AuthorityPolicyOperation.CLOSE)
                timestamp_shape = (superseded is None and
                                   created == activated and
                                   admission_changed >= activated)
            else:
                operation_shape = (
                    admission_state is AuthorityPolicyAdmissionState.CLOSED and
                    admission_revision >= 4 and admission_revision % 3 == 1 and
                    operation is AuthorityPolicyOperation.SUPERSEDE)
                timestamp_shape = (superseded is not None and
                                   created == activated and
                                   admission_changed >= activated and
                                   superseded >= admission_changed)
            if not operation_shape or not timestamp_shape:
                raise AuthorityStateCorruption(
                    'Policy admission state/revision/operation/timestamps have '
                    'an invalid closed shape.')
            if admission_revision == 1 and admission_changed != activated:
                raise AuthorityStateCorruption(
                    'Initial policy activation timestamps differ.')
            if (proof.service_fence.service_hash != service_hash or
                (reason == 'INITIAL_PROMOTION' and
                 (not isinstance(proof, authority.AuthoritativePromotionProofV1)
                  or predecessor_epoch is not None or
                  proof.candidate_epoch != policy_epoch or
                  proof.qualification_policy_sha256 != policy_hash or
                  proof.qualification_binding_sha256 != binding_hash or
                  activated < authority.timestamp_to_datetime(
                      proof.verified_at, name='promotion.verified_at'))) or
                (reason == 'COMPATIBLE_IMAGE_ROTATION' and
                 (not isinstance(
                     proof, authority.ServeAuthorityPolicyRotationProofV1) or
                  proof.predecessor_policy_epoch != predecessor_epoch or
                  proof.successor_policy.canonical_bytes
                  != policy.canonical_bytes or proof.successor_policy_sha256
                  != policy_hash or proof.successor_authority_binding_sha256
                  != binding_hash or proof.nonterminal_inventory.canonical_bytes
                  != inventory.canonical_bytes or
                  activated < authority.timestamp_to_datetime(
                      proof.completed_at, name='rotation.completed_at')))):
                raise AuthorityStateCorruption(
                    'Policy row and its typed promotion/rotation proof differ.')
            return AuthorityPolicyEpochRecord(
                service_hash=service_hash,
                policy_epoch=policy_epoch,
                predecessor_policy_epoch=predecessor_epoch,
                policy=policy,
                policy_sha256=policy_hash,
                authority_binding_sha256=binding_hash,
                rotation_proof=proof,
                rotation_proof_sha256=proof_hash,
                nonterminal_inventory=inventory,
                nonterminal_inventory_sha256=inventory_hash,
                reason=reason,
                policy_state=policy_state,
                admission_state=admission_state,
                admission_revision=admission_revision,
                last_operation_id=_row_uuid(row['last_operation_id'],
                                            name='policy.last_operation_id'),
                last_operation_kind=operation,
                created_at=created,
                admission_changed_at=admission_changed,
                activated_at=activated,
                superseded_at=superseded)
        except (KeyError, TypeError, ValueError) as e:
            raise AuthorityStateCorruption(
                'Authority policy row is malformed.') from e

    @staticmethod
    def _lock_policy(
        connection: sqlalchemy.engine.Connection,
        service_hash: str,
        policy_epoch: uuid.UUID,
    ) -> Mapping[str, Any] | None:
        return cast(
            Mapping[str, Any] | None,
            connection.execute(
                sqlalchemy.select(m4_schema.AUTHORITY_POLICY_EPOCHS).where(
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.service_hash ==
                    service_hash,
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch ==
                    policy_epoch).with_for_update()).mappings().one_or_none())

    @staticmethod
    def _lock_lifecycle_fence(
        connection: sqlalchemy.engine.Connection,
        *,
        service_name: str,
        lifecycle_epoch: int,
    ) -> None:
        """Lock class-1 name ownership before any service-owned row."""

        table = serve_state_schema.service_lifecycle_fences_table
        row = connection.execute(
            sqlalchemy.select(table.c.epoch).where(
                table.c.name == service_name).with_for_update()).one_or_none()
        if row is None:
            raise AuthorityStateConflict(
                'Service lifecycle fence no longer exists.')
        epoch = row.epoch
        if type(epoch) is not int or epoch <= 0:
            raise AuthorityStateCorruption(
                'Service lifecycle fence epoch is invalid.')
        if epoch != lifecycle_epoch:
            raise AuthorityStateConflict(
                'Service lifecycle claimant changed before preparation.')

    @staticmethod
    def _initial_candidate_binding(
        row: Mapping[str, Any],
        fence: authority.AuthorityServiceFenceV1,
    ) -> resource_actions.ShadowCandidateActionBindingV1:
        """Validate one authoritative service and return its promotion root."""

        try:
            ServeResourceActionAuthorityStore._validate_service_fence(
                row, fence)
            if row['pool'] is None or row['pool'] != 0:
                raise AuthorityStateConflict(
                    'Pool services cannot prepare V2 resource actions.')
            if row['resource_action_mode'] != 'authoritative':
                raise AuthorityStateConflict(
                    'V2 PREPARING currently requires authoritative mode.')
            if row['resource_action_mode_changed_at'] is None:
                raise AuthorityStateCorruption(
                    'Authoritative service has no mode-change timestamp.')
            _canonical_datetime(row['resource_action_mode_changed_at'],
                                name='service.resource_action_mode_changed_at')
            return resource_actions.ShadowCandidateActionBindingV1(
                version=1,
                binding_kind=(resource_actions.ShadowCandidateActionBindingV1.
                              BINDING_KIND),
                candidate_epoch=_row_uuid(
                    row['resource_action_candidate_epoch'],
                    name='service.resource_action_candidate_epoch'),
                qualification_policy_sha256=_canonical_hash(
                    row['resource_action_candidate_policy_sha256'],
                    name=('service.'
                          'resource_action_candidate_policy_sha256')),
                qualification_binding_sha256=_canonical_hash(
                    row['resource_action_candidate_binding_sha256'],
                    name=('service.'
                          'resource_action_candidate_binding_sha256')))
        except KeyError as e:
            raise AuthorityStateCorruption(
                'Authoritative service row is malformed.') from e

    def _lock_current_open_policy(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        fence: authority.AuthorityServiceFenceV1,
        initial_candidate: resource_actions.ShadowCandidateActionBindingV1,
    ) -> AuthorityPolicyEpochRecord:
        """Lock the complete policy lineage and require a rooted OPEN tip."""

        rows = connection.execute(
            sqlalchemy.select(m4_schema.AUTHORITY_POLICY_EPOCHS).where(
                m4_schema.AUTHORITY_POLICY_EPOCHS.c.service_hash ==
                fence.service_hash).order_by(
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch).
            with_for_update()).mappings().all()
        records = tuple(self._policy_record(row) for row in rows)
        roots = tuple(record for record in records
                      if record.predecessor_policy_epoch is None)
        active = tuple(record for record in records
                       if record.policy_state is AuthorityPolicyState.ACTIVE)
        if not active:
            raise AuthorityStateConflict(
                'Current ACTIVE authority policy is absent.')
        if len(roots) != 1 or len(active) != 1:
            raise AuthorityStateCorruption(
                'Authority policy root/current lineage is not singular.')
        root = roots[0]
        record = active[0]
        records_by_epoch = {item.policy_epoch: item for item in records}
        if len(records_by_epoch) != len(records):
            raise AuthorityStateCorruption(
                'Authority policy lineage contains a duplicate epoch.')

        visited: set[uuid.UUID] = set()
        cursor = record
        while True:
            if cursor.policy_epoch in visited:
                raise AuthorityStateCorruption(
                    'Authority policy lineage contains a cycle.')
            visited.add(cursor.policy_epoch)
            predecessor_epoch = cursor.predecessor_policy_epoch
            if predecessor_epoch is None:
                if cursor.policy_epoch != root.policy_epoch:
                    raise AuthorityStateCorruption(
                        'Current authority policy is disconnected from its '
                        'initial root.')
                break
            predecessor = records_by_epoch.get(predecessor_epoch)
            if predecessor is None:
                raise AuthorityStateCorruption(
                    'Authority policy lineage has a missing predecessor.')
            proof = cursor.rotation_proof
            if (type(proof) is not authority.ServeAuthorityPolicyRotationProofV1
                    or proof.predecessor_policy_epoch
                    != predecessor.policy_epoch or
                    proof.predecessor_policy_sha256
                    != predecessor.policy_sha256):
                raise AuthorityStateCorruption(
                    'Authority policy rotation differs from its locked '
                    'predecessor.')
            if (predecessor.policy_state is not AuthorityPolicyState.SUPERSEDED
                    or predecessor.admission_state
                    is not AuthorityPolicyAdmissionState.CLOSED or
                    predecessor.superseded_at != cursor.activated_at or
                    cursor.activated_at < predecessor.created_at or
                    cursor.activated_at < predecessor.admission_changed_at):
                raise AuthorityStateCorruption(
                    'Authority policy rotation has invalid predecessor state '
                    'or activation chronology.')
            cursor = predecessor
        if len(visited) != len(records):
            raise AuthorityStateCorruption(
                'Authority policy lineage contains disconnected rows.')
        endpoint_epochs = {root.policy_epoch, record.policy_epoch}
        if any(item.policy_epoch not in endpoint_epochs and item.rotation_proof.
               service_fence.canonical_bytes != fence.canonical_bytes
               for item in records):
            raise AuthorityStateCorruption(
                'Intermediate authority policy proof crosses the locked '
                'service fence.')
        if (root.reason != 'INITIAL_PROMOTION' or
                root.policy_epoch != initial_candidate.candidate_epoch or
                root.policy_sha256
                != initial_candidate.qualification_policy_sha256 or
                root.authority_binding_sha256
                != initial_candidate.qualification_binding_sha256 or
                root.rotation_proof.service_fence.canonical_bytes
                != fence.canonical_bytes):
            raise AuthorityStateConflict(
                'Initial authority policy differs from the locked service.')
        if (record.policy_state is not AuthorityPolicyState.ACTIVE or
                record.admission_state
                is not AuthorityPolicyAdmissionState.OPEN):
            raise AuthorityStateConflict(
                'Current authority policy admission is not ACTIVE/OPEN.')
        if (record.service_hash != fence.service_hash or
                record.rotation_proof.service_fence.canonical_bytes
                != fence.canonical_bytes):
            raise AuthorityStateConflict(
                'Current authority policy differs from the locked service.')
        return record

    @staticmethod
    def _validate_policy_approves_manifest(
        policy: AuthorityPolicyEpochRecord,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
    ) -> authority.ApprovedAuthorityCohortArtifactV1:
        approved = _approved_cohort_artifact_from_manifest_v2(expected_manifest)
        if not any(approved.canonical_bytes == item.canonical_bytes
                   for item in policy.policy.approved_cohorts):
            raise AuthorityStateConflict(
                'Current authority policy does not approve the V2 cohort.')
        return approved

    @staticmethod
    def _validate_current_candidate_binding(
        *,
        candidate_binding: authority.ResourceActionCandidateBindingV1,
        policy: AuthorityPolicyEpochRecord,
        service_fence: authority.AuthorityServiceFenceV1,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
    ) -> None:
        if type(candidate_binding
               ) is not authority.ResourceActionCandidateBindingV1:
            raise TypeError('candidate_binding has invalid type.')
        approved = ServeResourceActionAuthorityStore._validate_policy_approves_manifest(
            policy, expected_manifest)
        try:
            candidate_binding.validate_for_policy(policy.policy)
        except (TypeError, ValueError) as e:
            raise AuthorityStateConflict(
                'Candidate binding differs from the current policy.') from e
        elected = candidate_binding.elected_version_identity
        if (candidate_binding.sha256 != policy.authority_binding_sha256 or
                candidate_binding.qualification_policy_sha256
                != policy.policy_sha256 or
                candidate_binding.selected_cohort.canonical_bytes
                != approved.canonical_bytes or
                elected.service_name != service_fence.service_name or
                elected.service_incarnation != uuid.UUID(
                    service_fence.service_hash)):
            raise AuthorityStateConflict(
                'Candidate binding is not the locked service selection.')

    @staticmethod
    def _reference_for_preparation(
        *,
        service_fence: authority.AuthorityServiceFenceV1,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        preparation_capability_sha256: str,
    ) -> resource_actions.WorkerCohortReferenceInputV1:
        if type(service_fence) is not authority.AuthorityServiceFenceV1:
            raise TypeError('service_fence has invalid type.')
        if type(resource_identity
               ) is not resource_actions.ProviderResourceIdentityV1:
            raise TypeError('resource_identity has invalid type.')
        if type(expected_manifest) is not (
                authority.ProviderAuthorityWorkerCohortManifestV2):
            raise TypeError('expected_manifest must be an exact V2 manifest.')
        if resource_identity.service_hash != service_fence.service_hash:
            raise AuthorityStateConflict(
                'Resource identity differs from the service fence.')
        action_identity = resource_identity.action_identity(action_kind)
        return resource_actions.WorkerCohortReferenceInputV1(
            version=1,
            decision_id=action_identity.action_id,
            cohort_id=expected_manifest.cohort_id,
            service_hash=resource_identity.service_hash,
            replica_incarnation=resource_identity.replica_incarnation,
            desired_generation=resource_identity.desired_generation,
            action_type=action_identity.action_kind,
            controller_owner_fence=service_fence.controller_owner_fence,
            lifecycle_epoch=service_fence.lifecycle_epoch,
            preparation_capability_sha256=preparation_capability_sha256)

    def _lock_accepting_memberships(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
    ) -> tuple[WorkerCohortV2Record,
               tuple[authority.ProviderAuthorityWorkerAcceptedMembershipV2,
                     authority.ProviderAuthorityWorkerAcceptedMembershipV2]]:
        """Lock cohort, empty handoff slot, and its exact accepted lease pair."""

        row = self._lock_cohort(connection, expected_manifest.cohort_id)
        if row is None:
            raise AuthorityStateConflict('Selected V2 cohort is absent.')
        cohort = self._cohort_record(row)
        if (cohort.cohort.manifest.canonical_bytes
                != expected_manifest.canonical_bytes or cohort.lifecycle_state
                is not resource_actions.WorkerCohortLifecycleState.ACCEPTING):
            raise AuthorityStateConflict(
                'Selected V2 cohort is not the exact ACCEPTING manifest.')

        handoffs = connection.execute(
            sqlalchemy.select(
                m4_schema.WORKER_REGISTRATION_HANDOFFS.c.handoff_id).where(
                    m4_schema.WORKER_REGISTRATION_HANDOFFS.c.cohort_id ==
                    cohort.cohort_id,
                    m4_schema.WORKER_REGISTRATION_HANDOFFS.c.handoff_state.in_(
                        ('OPEN', 'READY'))).order_by(
                            m4_schema.WORKER_REGISTRATION_HANDOFFS.c.handoff_id
                        ).with_for_update()).all()
        if handoffs:
            raise AuthorityStateConflict(
                'Selected V2 cohort has a nonterminal worker handoff.')

        registrations = cohort.registration_set.workers
        worker_ids = tuple(item.worker_instance_id for item in registrations)
        if len(worker_ids) != 2:
            raise AuthorityStateCorruption(
                'ACCEPTING V2 cohort does not have exactly two members.')
        lease_rows = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                cohort.cohort_id,
                m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id.in_(
                    worker_ids)).order_by(
                        m4_schema.WORKER_REGISTRATION_LEASES.c.
                        worker_instance_id).with_for_update()).mappings().all()
        if (len(lease_rows) != 2 or tuple(
                _row_uuid(item['worker_instance_id'],
                          name='lease.worker_instance_id')
                for item in lease_rows) != worker_ids):
            raise AuthorityStateConflict(
                'Accepted V2 membership lease pair is incomplete.')

        memberships: list[
            authority.ProviderAuthorityWorkerAcceptedMembershipV2] = []
        for registration, lease_row in zip(registrations, lease_rows):
            lease = self._lease_record(lease_row)
            self._validate_anchor_lease(lease, registration)
            try:
                lease.renewal_registration.worker.validate_for_cohort(
                    cohort.cohort)
                membership = (
                    authority.ProviderAuthorityWorkerAcceptedMembershipV2(
                        version=2,
                        registration=registration,
                        registration_set_revision=cohort.registration_set.
                        revision,
                        registration_set_sha256=cohort.registration_set.sha256,
                        lease=lease))
            except (TypeError, ValueError) as e:
                raise AuthorityStateCorruption(
                    'Accepted V2 lease differs from its cohort.') from e
            memberships.append(membership)
        pair = (memberships[0], memberships[1])
        self._require_fresh_memberships(pair, self._database_now(connection))
        return cohort, pair

    @staticmethod
    def _require_fresh_memberships(
        memberships: tuple[
            authority.ProviderAuthorityWorkerAcceptedMembershipV2,
            authority.ProviderAuthorityWorkerAcceptedMembershipV2,
        ],
        database_now: datetime.datetime,
    ) -> None:
        normalized_now = database_now.astimezone(_UTC)
        for membership in memberships:
            lease = membership.lease
            renewed_at = authority.timestamp_to_datetime(
                lease.renewed_at, name='lease.renewed_at')
            observed_at = authority.timestamp_to_datetime(
                lease.renewal_registration.worker.observed_at,
                name='lease.worker.observed_at')
            if (renewed_at > normalized_now or
                    observed_at < renewed_at - _REGISTRATION_MAX_AGE):
                raise AuthorityStateCorruption(
                    'Accepted V2 lease has impossible renewal evidence.')
            if not lease.is_fresh(normalized_now):
                raise AuthorityStateConflict(
                    'Accepted V2 registration lease is not fresh.')

    @staticmethod
    def _require_reference_not_in_future(
        record: WorkerCohortReferenceV2Record,
        database_now: datetime.datetime,
    ) -> None:
        if record.created_at > database_now:
            raise AuthorityStateCorruption(
                'PREPARING reference creation is in the database future.')

    def read_worker_cohort_reference_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        decision_id: uuid.UUID | str,
    ) -> WorkerCohortReferenceV2Record | None:
        """Strictly read one V2 reference in a caller-owned transaction."""

        self._require_transaction(connection)
        decision = authority._uuid(  # pylint: disable=protected-access
            decision_id,
            name='decision_id')
        row = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_COHORT_REFS_V2).where(
                m4_schema.WORKER_COHORT_REFS_V2.c.decision_id ==
                decision)).mappings().one_or_none()
        return (None
                if row is None else decode_worker_cohort_reference_v2_row(row))

    def validate_preparing_worker_cohort_reference_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        expected_reference: resource_actions.WorkerCohortReferenceInputV1,
    ) -> WorkerCohortReferenceV2Record:
        """Lock and exactly validate one revision-one PREPARING reference."""

        self._require_transaction(connection)
        if type(expected_reference) is not (
                resource_actions.WorkerCohortReferenceInputV1):
            raise TypeError('expected_reference has invalid type.')
        row = self._lock_reference(connection, expected_reference.decision_id)
        if row is None:
            raise AuthorityStateConflict('PREPARING reference is absent.')
        record = decode_worker_cohort_reference_v2_row(row)
        if (record.reference.canonical_bytes
                != expected_reference.canonical_bytes or record.reference_state
                is not resource_actions.WorkerCohortReferenceState.PREPARING or
                record.revision != 1 or
                record.authority_policy_epoch is not None or
                record.authority_policy_sha256 is not None or
                record.authority_binding_sha256 is not None):
            raise AuthorityStateConflict(
                'Decision has another reference identity or lifecycle.')
        return record

    def prepare_worker_cohort_reference_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        service_fence: authority.AuthorityServiceFenceV1,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        preparation_capability_sha256: str,
        candidate_binding: authority.ResourceActionCandidateBindingV1,
    ) -> WorkerCohortReferencePreparationV2:
        """Insert/adopt one authoritative, nonexecuting V2 reference."""

        self._require_transaction(connection)
        expected_reference = self._reference_for_preparation(
            service_fence=service_fence,
            resource_identity=resource_identity,
            action_kind=action_kind,
            expected_manifest=expected_manifest,
            preparation_capability_sha256=preparation_capability_sha256)
        self._lock_lifecycle_fence(
            connection,
            service_name=service_fence.service_name,
            lifecycle_epoch=service_fence.lifecycle_epoch)
        service = self._lock_service(connection, service_fence.service_name)
        if service is None:
            raise AuthorityStateConflict('Authority service is absent.')
        initial_candidate = self._initial_candidate_binding(
            service, service_fence)
        policy = self._lock_current_open_policy(
            connection,
            fence=service_fence,
            initial_candidate=initial_candidate)
        self._validate_current_candidate_binding(
            candidate_binding=candidate_binding,
            policy=policy,
            service_fence=service_fence,
            expected_manifest=expected_manifest)
        cohort, memberships = self._lock_accepting_memberships(
            connection, expected_manifest=expected_manifest)

        table = m4_schema.WORKER_COHORT_REFS_V2
        inserted = connection.execute(
            postgresql.insert(table).values(
                decision_id=expected_reference.decision_id,
                cohort_id=expected_reference.cohort_id,
                service_hash=expected_reference.service_hash,
                replica_incarnation=expected_reference.replica_incarnation,
                desired_generation=expected_reference.desired_generation,
                action_type=expected_reference.action_type.value,
                controller_owner_fence=(
                    expected_reference.controller_owner_fence),
                lifecycle_epoch=expected_reference.lifecycle_epoch,
                preparation_capability_sha256=(
                    expected_reference.preparation_capability_sha256),
                reference_state=(resource_actions.WorkerCohortReferenceState.
                                 PREPARING.value),
                revision=1,
                created_at=sqlalchemy.func.clock_timestamp(),
                bound_at=None,
                released_at=None,
                authority_policy_epoch=None,
                authority_policy_sha256=None,
                authority_binding_sha256=None).on_conflict_do_nothing(
                    index_elements=[table.c.decision_id]).returning(
                        table.c.decision_id)).scalar_one_or_none()
        record = self.validate_preparing_worker_cohort_reference_in_transaction(
            connection, expected_reference)
        # A conflicting insert/reference lock can consume most of a 60-second
        # lease.  Recheck both leases against one fresh PostgreSQL clock before
        # allowing the caller to commit even an exact adoption.
        final_now = self._database_now(connection)
        self._require_fresh_memberships(memberships, final_now)
        self._require_reference_not_in_future(record, final_now)
        return WorkerCohortReferencePreparationV2(
            record=record,
            cohort=cohort,
            accepted_memberships=memberships,
            initial_candidate_binding=initial_candidate,
            current_authority_binding=candidate_binding,
            authority_policy=policy,
            adopted=inserted is None)

    def prepare_worker_cohort_reference(
        self,
        *,
        service_fence: authority.AuthorityServiceFenceV1,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        preparation_capability_sha256: str,
        candidate_binding: authority.ResourceActionCandidateBindingV1,
    ) -> WorkerCohortReferencePreparationV2:
        """Run the complete short PREPARING transaction."""

        with self._database.begin() as connection:
            return self.prepare_worker_cohort_reference_in_transaction(
                connection,
                service_fence=service_fence,
                resource_identity=resource_identity,
                action_kind=action_kind,
                expected_manifest=expected_manifest,
                preparation_capability_sha256=(preparation_capability_sha256),
                candidate_binding=candidate_binding)

    @staticmethod
    def _validate_reference_resource_identity(
        record: WorkerCohortReferenceV2Record,
        *,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        launch_identity_context: resource_actions.
        ProviderLaunchIdentityCanonicalizationContextV1 | None,
    ) -> None:
        expected_action = resource_identity.action_identity(action_kind)
        if expected_action.action_kind is kernel_actions.ActionKind.LAUNCH:
            if type(launch_identity_context) is not (
                    resource_actions.
                    ProviderLaunchIdentityCanonicalizationContextV1):
                raise TypeError(
                    'Launch preflight requires its exact canonicalization '
                    'context.')
        elif launch_identity_context is not None:
            raise TypeError(
                'Down preflight must not carry a launch canonicalization '
                'context.')
        reference = record.reference
        if (reference.decision_id != expected_action.action_id or
                reference.cohort_id != expected_manifest.cohort_id or
                reference.service_hash != resource_identity.service_hash or
                reference.replica_incarnation
                != resource_identity.replica_incarnation or
                reference.desired_generation
                != resource_identity.desired_generation or
                reference.action_type is not expected_action.action_kind):
            raise AuthorityStateConflict(
                'PREPARING reference differs from the preflight identity.')

    @staticmethod
    def _validate_locked_preflight_context(
        record: WorkerCohortReferenceV2Record,
        *,
        service_name: str,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        launch_identity_context: resource_actions.
        ProviderLaunchIdentityCanonicalizationContextV1 | None,
    ) -> None:
        """Bind the launch proof context to the exact locked reference."""

        action_identity = resource_identity.action_identity(action_kind)
        if action_identity.action_kind is kernel_actions.ActionKind.DOWN:
            if launch_identity_context is not None:
                raise TypeError(
                    'Down preflight must keep its capability reference-owned.')
            return
        if type(launch_identity_context) is not (
                resource_actions.ProviderLaunchIdentityCanonicalizationContextV1
        ):
            raise TypeError(
                'Launch preflight requires its exact canonicalization '
                'context.')
        context = launch_identity_context
        context_input = context.input
        expected_reference = resource_actions.WorkerCohortReferenceInputV1(
            version=1,
            decision_id=context.decision_id,
            cohort_id=context.cohort_id,
            service_hash=context_input.resource_identity.service_hash,
            replica_incarnation=(
                context_input.resource_identity.replica_incarnation),
            desired_generation=(
                context_input.resource_identity.desired_generation),
            action_type=context.action_type,
            controller_owner_fence=context.controller_owner_fence,
            lifecycle_epoch=context.lifecycle_epoch,
            preparation_capability_sha256=(
                context.preparation_capability_sha256))
        if (context_input.service_name != service_name or
                context_input.resource_identity.canonical_bytes
                != resource_identity.canonical_bytes or
                context.preparation_reference_revision != record.revision or
                context.reference_state is not record.reference_state or
                expected_reference.canonical_bytes
                != record.reference.canonical_bytes):
            raise AuthorityStateConflict(
                'Launch canonicalization context differs from the locked '
                'PREPARING reference or service.')

    @staticmethod
    def _discover_service_name(
        connection: sqlalchemy.engine.Connection,
        service_hash: str,
    ) -> str:
        """Non-authoritatively discover a name; later locks revalidate it."""

        table = serve_state_schema.services_table
        names = connection.execute(
            sqlalchemy.select(
                table.c.name).where(table.c.hash == service_hash).order_by(
                    table.c.name).limit(2)).scalars().all()
        if len(names) != 1:
            raise AuthorityStateConflict(
                'Preflight service incarnation is absent or ambiguous.')
        name = names[0]
        if type(name) is not str or not name:
            raise AuthorityStateCorruption(
                'Discovered preflight service name is invalid.')
        return name

    @staticmethod
    def _lock_and_validate_preflight_api_instance(
        connection: sqlalchemy.engine.Connection,
        *,
        worker_instance_id: uuid.UUID,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        memberships: tuple[
            authority.ProviderAuthorityWorkerAcceptedMembershipV2,
            authority.ProviderAuthorityWorkerAcceptedMembershipV2,
        ],
    ) -> tuple[Mapping[str, Any],
               authority.ProviderAuthorityWorkerAcceptedMembershipV2]:
        membership = next(
            (item for item in memberships
             if item.registration.worker_instance_id == worker_instance_id),
            None)
        if membership is None:
            raise AuthorityStateConflict(
                'Preflight worker is not an accepted cohort member.')
        row = connection.execute(
            sqlalchemy.select(request_postgres_schema.SERVER_INSTANCES).where(
                request_postgres_schema.SERVER_INSTANCES.c.instance_id ==
                worker_instance_id).with_for_update()).mappings().one_or_none()
        if row is None:
            raise AuthorityStateConflict(
                'Preflight authority-worker API instance is absent.')
        try:
            instance_id = _row_uuid(row['instance_id'],
                                    name='api_instance.instance_id')
            started_at = _canonical_datetime(row['started_at'],
                                             name='api_instance.started_at')
            heartbeat_at = _canonical_datetime(row['heartbeat_at'],
                                               name='api_instance.heartbeat_at')
            worker = membership.lease.renewal_registration.worker
            expected_handlers = sorted(expected_manifest.handler_allowlist)
            expected_payload_versions = {
                requests_lib.DURABLE_PAYLOAD_FORMAT: {
                    'minimum': requests_lib.DURABLE_PAYLOAD_VERSION,
                    'maximum': requests_lib.DURABLE_PAYLOAD_VERSION,
                }
            }
            if (instance_id != worker_instance_id or
                    row['role'] != 'authority-worker' or
                    row['pod_name'] != worker.pod_name or
                    row['pod_uid'] != str(worker.pod_uid) or
                    type(row['ready']) is not bool or row['ready'] or
                    row['draining_at'] is not None or row['health_detail'] != {
                        'phase': 'preflight-only'
                    } or not _same_canonical(row['supported_handlers'],
                                             expected_handlers) or
                    not _same_canonical(row['supported_payload_versions'],
                                        expected_payload_versions) or
                    heartbeat_at < started_at):
                raise AuthorityStateConflict(
                    'API instance is not the exact preflight-only worker.')
            return cast(Mapping[str, Any], row), membership
        except KeyError as e:
            raise AuthorityStateCorruption(
                'Preflight API instance row is malformed.') from e

    @staticmethod
    def _require_fresh_preflight_api_instance(
        row: Mapping[str, Any],
        database_now: datetime.datetime,
    ) -> None:
        heartbeat_at = _canonical_datetime(row['heartbeat_at'],
                                           name='api_instance.heartbeat_at')
        if (heartbeat_at > database_now or
                heartbeat_at <= database_now - _API_INSTANCE_LEASE_TTL):
            raise AuthorityStateConflict(
                'Preflight authority-worker API lease is not fresh.')

    def validate_preparing_reference_for_preflight_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        worker_instance_id: uuid.UUID | str,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        launch_identity_context: resource_actions.
        ProviderLaunchIdentityCanonicalizationContextV1 | None,
    ) -> WorkerCohortReferenceV2Record:
        """Run the dark trust fence in one bounded caller-owned transaction."""

        self._require_transaction(connection)
        deadline = (time.monotonic() +
                    _PREFLIGHT_TRUST_TRANSACTION_TIMEOUT_SECONDS)
        return self._validate_preparing_reference_with_deadline(
            connection,
            deadline_monotonic=deadline,
            worker_instance_id=worker_instance_id,
            expected_manifest=expected_manifest,
            resource_identity=resource_identity,
            action_kind=action_kind,
            launch_identity_context=launch_identity_context)

    def _validate_preparing_reference_with_deadline(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        deadline_monotonic: float,
        worker_instance_id: uuid.UUID | str,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        launch_identity_context: resource_actions.
        ProviderLaunchIdentityCanonicalizationContextV1 | None,
    ) -> WorkerCohortReferenceV2Record:
        """Apply one cumulative deadline to an already-open transaction."""

        self._require_transaction(connection)
        if type(deadline_monotonic) is not float:
            raise TypeError('deadline_monotonic must be a float.')

        def reject_statement_after_deadline(
            _connection: sqlalchemy.engine.Connection,
            _cursor: Any,
            _statement: str,
            _parameters: Any,
            _context: Any,
            _executemany: bool,
        ) -> None:
            if time.monotonic() >= deadline_monotonic:
                raise AuthorityStateConflict(
                    'Preflight trust transaction exceeded its cumulative '
                    'budget.')

        sqlalchemy.event.listen(connection, 'before_cursor_execute',
                                reject_statement_after_deadline)
        try:
            self._bound_preflight_trust_read(connection)
            return self._validate_preparing_reference_for_preflight_after_budget(
                connection,
                worker_instance_id=worker_instance_id,
                expected_manifest=expected_manifest,
                resource_identity=resource_identity,
                action_kind=action_kind,
                launch_identity_context=launch_identity_context)
        except sqlalchemy.exc.DBAPIError as e:
            raise AuthorityStateConflict(
                'Preflight trust database transaction failed or exceeded its '
                'budget.') from e
        finally:
            sqlalchemy.event.remove(connection, 'before_cursor_execute',
                                    reject_statement_after_deadline)

    def _validate_preparing_reference_for_preflight_after_budget(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        worker_instance_id: uuid.UUID | str,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        launch_identity_context: resource_actions.
        ProviderLaunchIdentityCanonicalizationContextV1 | None,
    ) -> WorkerCohortReferenceV2Record:
        """Lock the complete dark trust fence without changing durable state."""

        worker_id = authority._uuid(  # pylint: disable=protected-access
            worker_instance_id,
            name='worker_instance_id')
        if type(expected_manifest) is not (
                authority.ProviderAuthorityWorkerCohortManifestV2):
            raise TypeError('expected_manifest must be an exact V2 manifest.')
        if type(resource_identity
               ) is not resource_actions.ProviderResourceIdentityV1:
            raise TypeError('resource_identity has invalid type.')
        decision_id = resource_identity.action_identity(action_kind).action_id
        discovered = self.read_worker_cohort_reference_in_transaction(
            connection, decision_id)
        if discovered is None:
            raise AuthorityStateConflict('PREPARING reference is absent.')
        self._validate_reference_resource_identity(
            discovered,
            expected_manifest=expected_manifest,
            resource_identity=resource_identity,
            action_kind=action_kind,
            launch_identity_context=launch_identity_context)
        service_name = self._discover_service_name(
            connection, resource_identity.service_hash)
        reference = discovered.reference
        fence = authority.AuthorityServiceFenceV1(
            service_name=service_name,
            service_hash=reference.service_hash,
            controller_owner_fence=reference.controller_owner_fence,
            lifecycle_epoch=reference.lifecycle_epoch)

        self._lock_lifecycle_fence(connection,
                                   service_name=service_name,
                                   lifecycle_epoch=fence.lifecycle_epoch)
        service = self._lock_service(connection, service_name)
        if service is None:
            raise AuthorityStateConflict('Authority service is absent.')
        initial_candidate = self._initial_candidate_binding(service, fence)
        policy = self._lock_current_open_policy(
            connection, fence=fence, initial_candidate=initial_candidate)
        self._validate_policy_approves_manifest(policy, expected_manifest)
        _, memberships = self._lock_accepting_memberships(
            connection, expected_manifest=expected_manifest)
        locked = self.validate_preparing_worker_cohort_reference_in_transaction(
            connection, discovered.reference)
        self._validate_reference_resource_identity(
            locked,
            expected_manifest=expected_manifest,
            resource_identity=resource_identity,
            action_kind=action_kind,
            launch_identity_context=launch_identity_context)
        self._validate_locked_preflight_context(
            locked,
            service_name=service_name,
            resource_identity=resource_identity,
            action_kind=action_kind,
            launch_identity_context=launch_identity_context)
        api_row, _ = self._lock_and_validate_preflight_api_instance(
            connection,
            worker_instance_id=worker_id,
            expected_manifest=expected_manifest,
            memberships=memberships)
        final_now = self._database_now(connection)
        self._require_fresh_memberships(memberships, final_now)
        self._require_fresh_preflight_api_instance(api_row, final_now)
        self._require_reference_not_in_future(locked, final_now)
        return locked

    def validate_preparing_reference_for_preflight(
        self,
        *,
        worker_instance_id: uuid.UUID | str,
        expected_manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        resource_identity: resource_actions.ProviderResourceIdentityV1,
        action_kind: kernel_actions.ActionKind | str,
        launch_identity_context: resource_actions.
        ProviderLaunchIdentityCanonicalizationContextV1 | None,
    ) -> WorkerCohortReferenceV2Record:
        """Run the complete dark preflight trust-fence transaction."""

        deadline = (time.monotonic() +
                    _PREFLIGHT_TRUST_TRANSACTION_TIMEOUT_SECONDS)
        with self._database.begin() as connection:
            return self._validate_preparing_reference_with_deadline(
                connection,
                deadline_monotonic=deadline,
                worker_instance_id=worker_instance_id,
                expected_manifest=expected_manifest,
                resource_identity=resource_identity,
                action_kind=action_kind,
                launch_identity_context=launch_identity_context)

    def drain_policy(
        self,
        *,
        service_fence: authority.AuthorityServiceFenceV1,
        policy_epoch: uuid.UUID,
        expected_revision: int,
        operation_id: uuid.UUID,
    ) -> AuthorityPolicyMutation:
        """Atomically stop new admission for one active policy.

        This one-way edge only removes authority: it does not declare existing
        work drained, close claims, reopen admission, or activate a policy.
        Consequently it needs no positive work-inventory evidence.
        """

        if type(service_fence) is not authority.AuthorityServiceFenceV1:
            raise TypeError('service_fence has invalid type.')
        epoch = authority._uuid(  # pylint: disable=protected-access
            policy_epoch,
            name='policy_epoch')
        revision = authority._positive_integer(  # pylint: disable=protected-access
            expected_revision,
            name='expected_revision')
        operation = authority._uuid(  # pylint: disable=protected-access
            operation_id,
            name='operation_id')
        with self._database.begin() as connection:
            service = self._lock_service(connection, service_fence.service_name)
            if service is None:
                raise AuthorityStateConflict('Authority service is absent.')
            self._validate_service_fence(service, service_fence)
            if service['resource_action_mode'] != 'authoritative':
                raise AuthorityStateConflict(
                    'Policy admission requires authoritative mode.')
            row = self._lock_policy(connection, service_fence.service_hash,
                                    epoch)
            if row is None:
                raise AuthorityStateConflict(
                    'Authority policy epoch is absent.')
            record = self._policy_record(row)
            if (record.admission_revision == revision + 1 and
                    record.policy_state is AuthorityPolicyState.ACTIVE and
                    record.admission_state
                    is AuthorityPolicyAdmissionState.DRAINING and
                    record.last_operation_id == operation and
                    record.last_operation_kind
                    is AuthorityPolicyOperation.DRAIN):
                return AuthorityPolicyMutation(record, adopted=True)
            if (record.policy_state is not AuthorityPolicyState.ACTIVE or
                    record.admission_state
                    is not AuthorityPolicyAdmissionState.OPEN or
                    record.admission_revision != revision):
                raise AuthorityStateSuperseded(
                    'Policy admission drain was superseded.')
            logical_time = max(self._database_now(connection),
                               record.admission_changed_at, record.activated_at)
            updated = connection.execute(
                sqlalchemy.update(m4_schema.AUTHORITY_POLICY_EPOCHS).where(
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.service_hash ==
                    service_fence.service_hash,
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_epoch == epoch,
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.policy_state ==
                    AuthorityPolicyState.ACTIVE.value,
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.admission_state ==
                    AuthorityPolicyAdmissionState.OPEN.value,
                    m4_schema.AUTHORITY_POLICY_EPOCHS.c.admission_revision ==
                    revision).
                values(admission_state=(
                    AuthorityPolicyAdmissionState.DRAINING.value),
                       admission_revision=revision + 1,
                       last_operation_id=operation,
                       last_operation_kind=AuthorityPolicyOperation.DRAIN.value,
                       admission_changed_at=logical_time))
            if updated.rowcount != 1:
                raise AuthorityStateConflict(
                    'Policy admission drain lost its CAS.')
            final = self._lock_policy(connection, service_fence.service_hash,
                                      epoch)
            assert final is not None
            return AuthorityPolicyMutation(self._policy_record(final))
