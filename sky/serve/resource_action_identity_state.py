"""Locked PostgreSQL identity boundary for live Serve V2 actions.

This module is deliberately narrower than action admission.  It owns only a
provisional class-2 service/policy/version/replica identity projection and an
optimistic immutable prior-launch snapshot required to prepare a down action.
It never commits a transaction, acquires a class-11 action-row lock, creates an
action, decodes ``version_specs.spec`` or ``replicas.replica_info``, or performs
provider work.

Launch callers invoke
:meth:`stage_authoritative_launch_class2_replica_link_in_transaction` only
after a version-commit/private-activation boundary has initialized the exact
immutable version identity.  This slice may provisionally stage only the
replica action link/hash.  The same still-open transaction must subsequently
validate the complete source, secret, cohort, reference, action, attempt,
cleanup-target, and represented-action graph.  A down caller similarly locks
the complete class-2 version set and reads a non-authorizing prior-launch
snapshot through
:meth:`project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction`.
The combined boundary must later lock the sorted class-11 action set and
revalidate that snapshot byte-exactly before commit.
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import hashlib
from typing import Any, cast
import uuid

import sqlalchemy

from sky.serve import resource_action_authority_state as authority_state
from sky.serve import resource_action_identity as identity_projector
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_actions
from sky.serve import serve_state_schema
from sky.serve.serve_statuses import ServiceStatus
from sky.server.requests import postgres_schema as request_schema
from sky.server.requests import resource_actions as kernel_actions


class ServeServiceVersionIdentityStateError(RuntimeError):
    """Base failure for the locked service-version identity boundary."""


class ServeServiceVersionIdentityConflict(ServeServiceVersionIdentityStateError
                                         ):
    """Caller identity or ownership no longer matches locked SQL state."""


class ServeServiceVersionIdentityCorruption(
        ServeServiceVersionIdentityStateError):
    """Durable SQL state violates an immutable identity contract."""


def _positive_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f'{name} must be a positive integer.')
    return value


def _nonnegative_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer.')
    return value


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is not uuid.UUID:
        raise TypeError(f'{name} must be a UUID.')
    return value


def _text(value: Any, *, name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f'{name} must be nonempty text.')
    return value


def _sha256(value: Any, *, name: str) -> str:
    if (type(value) is not str or len(value) != 64 or
            any(character not in '0123456789abcdef' for character in value)):
        raise ValueError(f'{name} must be lowercase SHA-256 text.')
    return value


@dataclasses.dataclass(frozen=True)
class ServeControllerOwnerFenceV1:
    """Exact current service incarnation and controller ownership fence."""

    service_name: str
    service_incarnation: uuid.UUID
    lifecycle_epoch: int
    controller_pid: int
    controller_ip: str

    def __post_init__(self) -> None:
        object.__setattr__(self, 'service_name',
                           _text(self.service_name, name='service_name'))
        object.__setattr__(
            self, 'service_incarnation',
            _uuid(self.service_incarnation, name='service_incarnation'))
        object.__setattr__(
            self, 'lifecycle_epoch',
            _positive_integer(self.lifecycle_epoch, name='lifecycle_epoch'))
        object.__setattr__(
            self, 'controller_pid',
            _positive_integer(self.controller_pid, name='controller_pid'))
        object.__setattr__(self, 'controller_ip',
                           _text(self.controller_ip, name='controller_ip'))

    @property
    def controller_owner(self) -> tuple[int, str]:
        return self.controller_pid, self.controller_ip


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionRecordFenceV1:
    """Exact relational replica row expected by one V2 admission."""

    replica_id: int
    replica_incarnation: uuid.UUID
    desired_generation: int
    creating_service_version: int
    replica_state_version: int
    is_spot: bool
    cluster_name: str
    sky_cluster_record_uuid: uuid.UUID
    launch_action_id: uuid.UUID
    down_action_id: uuid.UUID | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'replica_id',
            _nonnegative_integer(self.replica_id, name='replica_id'))
        object.__setattr__(
            self, 'replica_incarnation',
            _uuid(self.replica_incarnation, name='replica_incarnation'))
        object.__setattr__(
            self, 'desired_generation',
            _positive_integer(self.desired_generation,
                              name='desired_generation'))
        object.__setattr__(
            self, 'creating_service_version',
            _positive_integer(self.creating_service_version,
                              name='creating_service_version'))
        object.__setattr__(
            self, 'replica_state_version',
            _positive_integer(self.replica_state_version,
                              name='replica_state_version'))
        if type(self.is_spot) is not bool or self.is_spot:
            raise ValueError('is_spot must be exact Boolean false.')
        object.__setattr__(self, 'cluster_name',
                           _text(self.cluster_name, name='cluster_name'))
        object.__setattr__(
            self, 'sky_cluster_record_uuid',
            _uuid(self.sky_cluster_record_uuid, name='sky_cluster_record_uuid'))
        object.__setattr__(
            self, 'launch_action_id',
            _uuid(self.launch_action_id, name='launch_action_id'))
        if self.down_action_id is not None:
            object.__setattr__(
                self, 'down_action_id',
                _uuid(self.down_action_id, name='down_action_id'))


@dataclasses.dataclass(frozen=True)
class LockedServeServiceVersionIdentityV1:
    """Reprojected immutable SQL version identity and exact source digest."""

    identity: resource_actions.ServeServiceVersionSpecIdentityV1
    identity_sha256: str
    yaml_content_sha256: str

    def __post_init__(self) -> None:
        if type(self.identity) is not (
                resource_actions.ServeServiceVersionSpecIdentityV1):
            raise TypeError('identity has an invalid type.')
        if self.identity_sha256 != self.identity.sha256:
            raise ValueError('identity_sha256 does not match identity bytes.')
        _sha256(self.yaml_content_sha256, name='yaml_content_sha256')


@dataclasses.dataclass(frozen=True)
class LockedServeAuthoritativePolicyV1:
    """Typed ACTIVE/OPEN policy evidence held under its class-2 row lock."""

    binding: resource_actions.AuthoritativeActionPolicyBindingV1
    record: authority_state.AuthorityPolicyEpochRecord

    def __post_init__(self) -> None:
        if type(self.binding) is not (
                resource_actions.AuthoritativeActionPolicyBindingV1):
            raise TypeError('binding has an invalid type.')
        if type(self.record) is not authority_state.AuthorityPolicyEpochRecord:
            raise TypeError('record has an invalid type.')
        if (self.binding.policy_epoch != self.record.policy_epoch or
                self.binding.policy_sha256 != self.record.policy_sha256 or
                self.binding.authority_binding_sha256
                != self.record.authority_binding_sha256):
            raise ValueError('binding differs from the typed policy record.')
        if (self.record.policy_state
                is not authority_state.AuthorityPolicyState.ACTIVE or
                self.record.admission_state
                is not authority_state.AuthorityPolicyAdmissionState.OPEN):
            raise ValueError('typed policy record is not ACTIVE/OPEN.')


@dataclasses.dataclass(frozen=True)
class ServeProvisionalClass2IdentityEvidenceV1:
    """Class-2 locked evidence valid only inside the caller transaction."""

    owner_fence: ServeControllerOwnerFenceV1
    elected_service_version: int
    action_service_version: int
    locked_versions: tuple[LockedServeServiceVersionIdentityV1, ...]
    authoritative_policy: LockedServeAuthoritativePolicyV1

    def __post_init__(self) -> None:
        if type(self.owner_fence) is not ServeControllerOwnerFenceV1:
            raise TypeError('owner_fence has an invalid type.')
        elected = _positive_integer(self.elected_service_version,
                                    name='elected_service_version')
        action = _positive_integer(self.action_service_version,
                                   name='action_service_version')
        if type(self.locked_versions) is not tuple or not self.locked_versions:
            raise TypeError('locked_versions must be a nonempty tuple.')
        if any(
                type(item) is not LockedServeServiceVersionIdentityV1
                for item in self.locked_versions):
            raise TypeError('locked_versions contains an invalid value.')
        versions = tuple(
            item.identity.service_version for item in self.locked_versions)
        if versions != tuple(sorted({elected, action})):
            raise ValueError('locked_versions is not the sorted version union.')
        if type(self.authoritative_policy) is not (
                LockedServeAuthoritativePolicyV1):
            raise TypeError('authoritative_policy has an invalid type.')

    def identity_for_version(
            self, service_version: int) -> LockedServeServiceVersionIdentityV1:
        """Return one already-locked version projection without SQL lookup."""

        version = _positive_integer(service_version, name='service_version')
        for locked in self.locked_versions:
            if locked.identity.service_version == version:
                return locked
        raise KeyError(f'Service version {version} was not locked.')

    @property
    def action_identity(self) -> LockedServeServiceVersionIdentityV1:
        return self.identity_for_version(self.action_service_version)

    @property
    def elected_identity(self) -> LockedServeServiceVersionIdentityV1:
        return self.identity_for_version(self.elected_service_version)


@dataclasses.dataclass(frozen=True)
class ServeAuthoritativeLaunchClass2ReplicaLinkStageV1:
    """Authoritative class-2 evidence plus a provisional replica-link write."""

    class2_evidence: ServeProvisionalClass2IdentityEvidenceV1
    replica_link_initialized: bool

    def __post_init__(self) -> None:
        if type(self.class2_evidence) is not (
                ServeProvisionalClass2IdentityEvidenceV1):
            raise TypeError('class2_evidence has an invalid type.')
        if type(self.replica_link_initialized) is not bool:
            raise TypeError('replica_link_initialized must be an exact '
                            'Boolean.')


@dataclasses.dataclass(frozen=True)
class ServeAuthoritativeDownClass2PriorLaunchOptimisticSnapshotV1:
    """Class-2 evidence plus a non-authorizing immutable MVCC snapshot."""

    class2_evidence: ServeProvisionalClass2IdentityEvidenceV1
    prior_launch_spec: resource_actions.ServeReplicaActionSpecV2
    prior_launch_spec_sha256: str

    def __post_init__(self) -> None:
        if type(self.class2_evidence) is not (
                ServeProvisionalClass2IdentityEvidenceV1):
            raise TypeError('class2_evidence has an invalid type.')
        if type(self.prior_launch_spec) is not (
                resource_actions.ServeReplicaActionSpecV2):
            raise TypeError('prior_launch_spec has an invalid type.')
        if self.prior_launch_spec_sha256 != self.prior_launch_spec.sha256:
            raise ValueError('prior launch spec hash does not match its bytes.')

    def revalidate_locked_prior_launch_spec(
            self,
            locked_spec: resource_actions.ServeReplicaActionSpecV2) -> None:
        """Require a later canonically locked class-11 row to match exactly.

        This comparison does not prove that ``locked_spec`` came from a row
        lock.  The combined admission boundary owns that lock and must call
        this method only after acquiring the sorted action-row set.
        """

        if type(locked_spec) is not resource_actions.ServeReplicaActionSpecV2:
            raise TypeError('locked_spec must be a live V2 action spec.')
        if locked_spec.canonical_bytes != self.prior_launch_spec.canonical_bytes:
            raise ServeServiceVersionIdentityConflict(
                'Canonically locked prior launch differs from its optimistic '
                'snapshot.')


class ServeServiceVersionIdentityStore:
    """PostgreSQL-only provisional class-2 boundary for Serve V2 preparation."""

    def __init__(self, database: sqlalchemy.engine.Engine):
        if not isinstance(database, sqlalchemy.engine.Engine):
            raise TypeError('database must be a SQLAlchemy Engine.')
        if database.dialect.name != 'postgresql':
            raise RuntimeError(
                'Serve service-version identity state requires PostgreSQL.')
        self._database = database

    def _require_transaction(self,
                             connection: sqlalchemy.engine.Connection) -> None:
        if not isinstance(connection, sqlalchemy.engine.Connection):
            raise TypeError('connection must be a SQLAlchemy Connection.')
        if connection.engine is not self._database:
            raise RuntimeError('connection does not belong to this store.')
        if connection.dialect.name != 'postgresql':
            raise RuntimeError(
                'Serve service-version identity state requires PostgreSQL.')
        if not connection.in_transaction():
            raise RuntimeError(
                'identity operations require a caller-owned transaction.')

    @staticmethod
    def _require_authoritative_spec(
            action_spec: resource_actions.ServeReplicaActionSpecV2) -> None:
        if type(action_spec.admission_binding) is not (
                resource_actions.AuthoritativeActionPolicyBindingV1):
            raise ServeServiceVersionIdentityConflict(
                'This action-link slice accepts only authoritative-bound V2 '
                'actions.')

    @staticmethod
    def _lock_lifecycle_fence(
        connection: sqlalchemy.engine.Connection,
        owner: ServeControllerOwnerFenceV1,
    ) -> None:
        """Acquire the durable class-1 name fence before every class-2 row."""

        table = serve_state_schema.service_lifecycle_fences_table
        row = connection.execute(
            sqlalchemy.select(table.c.epoch).where(
                table.c.name ==
                owner.service_name).with_for_update()).one_or_none()
        if row is None:
            raise ServeServiceVersionIdentityConflict(
                'Service lifecycle fence no longer exists.')
        try:
            epoch = _positive_integer(row.epoch,
                                      name='service_lifecycle_fence.epoch')
        except (TypeError, ValueError) as e:
            raise ServeServiceVersionIdentityCorruption(str(e)) from e
        if epoch != owner.lifecycle_epoch:
            raise ServeServiceVersionIdentityConflict(
                'Service lifecycle claimant changed before action admission.')

    @staticmethod
    def _lock_service(
        connection: sqlalchemy.engine.Connection,
        owner: ServeControllerOwnerFenceV1,
        action_spec: resource_actions.ServeReplicaActionSpecV2,
        *,
        require_elected_version: bool,
    ) -> Mapping[str, Any]:
        if type(owner) is not ServeControllerOwnerFenceV1:
            raise TypeError('owner has an invalid type.')
        table = serve_state_schema.services_table
        row = connection.execute(
            sqlalchemy.select(table).where(table.c.name == owner.service_name).
            with_for_update()).mappings().one_or_none()
        if row is None:
            raise ServeServiceVersionIdentityConflict(
                f'Service {owner.service_name!r} no longer exists.')

        stored_hash = row['hash']
        try:
            stored_incarnation = uuid.UUID(stored_hash)
        except (AttributeError, TypeError, ValueError) as e:
            raise ServeServiceVersionIdentityCorruption(
                'Locked service hash is not a canonical UUID.') from e
        if str(stored_incarnation) != stored_hash:
            raise ServeServiceVersionIdentityCorruption(
                'Locked service hash is not canonical UUID text.')
        if (stored_incarnation != owner.service_incarnation or
                row['lifecycle_epoch'] != owner.lifecycle_epoch or
            (row['controller_pid'], row['controller_ip'])
                != owner.controller_owner):
            raise ServeServiceVersionIdentityConflict(
                'Service incarnation, lifecycle epoch, or controller owner '
                'changed.')
        if row['pool'] is None or bool(row['pool']):
            raise ServeServiceVersionIdentityConflict(
                'Pool services cannot enter Serve V2 action admission.')
        if row['resource_action_mode_changed_at'] is None:
            raise ServeServiceVersionIdentityCorruption(
                'Action-aware service has no mode transition timestamp.')

        if (row['resource_action_mode']
                != resource_actions.ResourceActionMode.AUTHORITATIVE.value):
            raise ServeServiceVersionIdentityConflict(
                'Locked service is not authoritative for this action.')

        try:
            service_status = ServiceStatus(row['status'])
        except (TypeError, ValueError) as e:
            raise ServeServiceVersionIdentityCorruption(
                'Locked service has an invalid canonical status.') from e
        if (action_spec.invocation.action_kind
                is kernel_actions.ActionKind.LAUNCH and service_status
                in ServiceStatus.replica_launch_blocking_statuses()):
            raise ServeServiceVersionIdentityConflict(
                'Locked service status blocks replica launch admission.')

        identity = action_spec.service_version_spec_identity
        if (identity.service_name != owner.service_name or
                identity.service_incarnation != owner.service_incarnation):
            raise ServeServiceVersionIdentityConflict(
                'Action service-version identity differs from the locked '
                'service fence.')
        current_version = row['current_version']
        if type(current_version) is not int or current_version <= 0:
            raise ServeServiceVersionIdentityCorruption(
                'Locked service current version is invalid.')
        if (require_elected_version and
                current_version != identity.service_version):
            raise ServeServiceVersionIdentityConflict(
                'Launch action no longer names the elected service version.')
        return cast(Mapping[str, Any], row)

    @staticmethod
    def _lock_authoritative_policy(
        connection: sqlalchemy.engine.Connection,
        *,
        owner: ServeControllerOwnerFenceV1,
        action_spec: resource_actions.ServeReplicaActionSpecV2,
    ) -> LockedServeAuthoritativePolicyV1:
        """Lock the action's exact policy after service and before versions."""

        binding = action_spec.admission_binding
        if type(binding) is not (
                resource_actions.AuthoritativeActionPolicyBindingV1):
            raise ServeServiceVersionIdentityConflict(
                'Action spec is not authoritative-bound.')
        table = m4_schema.AUTHORITY_POLICY_EPOCHS
        service_hash = str(owner.service_incarnation)
        row = connection.execute(
            sqlalchemy.select(table).where(
                table.c.service_hash == service_hash,
                table.c.policy_epoch == binding.policy_epoch).with_for_update()
        ).mappings().one_or_none()
        if row is None:
            raise ServeServiceVersionIdentityConflict(
                'Authoritative action policy no longer exists.')
        if (row['service_hash'] != service_hash or
                row['policy_epoch'] != binding.policy_epoch or
                row['policy_sha256'] != binding.policy_sha256 or
                row['authority_binding_sha256']
                != binding.authority_binding_sha256):
            raise ServeServiceVersionIdentityConflict(
                'Locked authoritative policy binding differs from the action.')
        try:
            # Reuse the authority store's one strict decoder.  It validates
            # the canonical policy, rotation proof, inventory, reason/
            # predecessor relation, operation enums, and all timestamps.
            # pylint: disable=protected-access
            record = (authority_state.ServeResourceActionAuthorityStore.
                      _policy_record(row))
            # pylint: enable=protected-access
        except authority_state.AuthorityStateCorruption as e:
            raise ServeServiceVersionIdentityCorruption(
                f'Locked authoritative policy is invalid: {e}') from e
        if (record.service_hash != service_hash or
                record.policy_epoch != binding.policy_epoch or
                record.policy_sha256 != binding.policy_sha256 or
                record.authority_binding_sha256
                != binding.authority_binding_sha256):
            raise ServeServiceVersionIdentityConflict(
                'Typed authoritative policy differs from the action binding.')
        if (record.policy_state
                is not authority_state.AuthorityPolicyState.ACTIVE or
                record.admission_state
                is not authority_state.AuthorityPolicyAdmissionState.OPEN):
            raise ServeServiceVersionIdentityConflict(
                'Authoritative policy is not ACTIVE with OPEN admission.')
        return LockedServeAuthoritativePolicyV1(binding=binding, record=record)

    @staticmethod
    def _lock_and_project_version(
        connection: sqlalchemy.engine.Connection,
        *,
        owner: ServeControllerOwnerFenceV1,
        service_version: int,
        expected_action_identity: resource_actions.
        ServeServiceVersionSpecIdentityV1 | None,
        expected_action_identity_sha256: str | None,
    ) -> LockedServeServiceVersionIdentityV1:
        version = _positive_integer(service_version, name='service_version')
        if (expected_action_identity
                is None) != (expected_action_identity_sha256 is None):
            raise TypeError('Expected action identity/hash must be paired.')
        expected_hash: str | None = None
        if expected_action_identity is not None:
            if type(expected_action_identity) is not (
                    resource_actions.ServeServiceVersionSpecIdentityV1):
                raise TypeError('expected_action_identity has invalid type.')
            expected_hash = _sha256(expected_action_identity_sha256,
                                    name='service_version_spec_identity_sha256')
            if expected_hash != expected_action_identity.sha256:
                raise ServeServiceVersionIdentityConflict(
                    'Caller identity hash differs from its canonical bytes.')
            if (expected_action_identity.service_name != owner.service_name or
                    expected_action_identity.service_incarnation
                    != owner.service_incarnation or
                    expected_action_identity.service_version != version):
                raise ServeServiceVersionIdentityConflict(
                    'Caller identity differs from its class-2 version key.')

        table = serve_state_schema.version_specs_table
        row = connection.execute(
            sqlalchemy.select(table).where(
                table.c.service_name == owner.service_name, table.c.version ==
                version).with_for_update()).mappings().one_or_none()
        if row is None:
            raise ServeServiceVersionIdentityConflict(
                'Creating immutable service version no longer exists.')
        yaml_content = row['yaml_content']
        if type(yaml_content) is not str:
            raise ServeServiceVersionIdentityCorruption(
                'Immutable service version has no text YAML source.')
        if (row['quarantined_at'] is None) != (row['quarantine_reason']
                                               is None):
            raise ServeServiceVersionIdentityCorruption(
                'Immutable service version has partial quarantine state.')
        if row['quarantined_at'] is not None:
            raise ServeServiceVersionIdentityConflict(
                'Quarantined service version cannot enter action admission.')

        stored_value = row['resource_action_spec_identity']
        stored_hash = row['resource_action_spec_identity_sha256']
        if (stored_value is None) != (stored_hash is None):
            raise ServeServiceVersionIdentityCorruption(
                'Version identity JSON/hash pair is partially null.')
        if stored_value is None:
            raise ServeServiceVersionIdentityConflict(
                'Authoritative action requires every locked service version '
                'to have an initialized immutable identity.')
        try:
            stored_identity = (
                resource_actions.ServeServiceVersionSpecIdentityV1.from_value(
                    stored_value))
            canonical_stored_hash = _sha256(
                stored_hash, name='stored resource_action_spec_identity_sha256')
        except (KeyError, TypeError, ValueError) as e:
            raise ServeServiceVersionIdentityCorruption(
                f'Stored version identity is invalid: {e}') from e

        # Deliberately use only relational keys, immutable YAML, and the
        # separately committed typed identity/hash pair.  The historical
        # pickled ``spec`` is neither read nor decoded, and the potential
        # projector never directly grants persistence authority.
        try:
            projected = (identity_projector.
                         verify_locked_serve_service_version_spec_identity_v1(
                             yaml_content=yaml_content,
                             service_name=owner.service_name,
                             service_incarnation=owner.service_incarnation,
                             service_version=version,
                             committed_identity=stored_identity,
                             committed_identity_sha256=canonical_stored_hash))
        except identity_projector.ServeServiceVersionIdentityVerificationError as e:
            raise ServeServiceVersionIdentityCorruption(
                f'Stored version identity differs from immutable YAML: {e}'
            ) from e
        except (TypeError, ValueError) as e:
            raise ServeServiceVersionIdentityConflict(
                f'Locked immutable YAML is not M4-projectable: {e}') from e
        if (expected_action_identity is not None and
            (projected.canonical_bytes
             != expected_action_identity.canonical_bytes or
             projected.sha256 != expected_hash)):
            raise ServeServiceVersionIdentityConflict(
                'Caller identity is not byte-equal to the locked YAML '
                'projection.')

        yaml_sha256 = hashlib.sha256(yaml_content.encode('utf-8')).hexdigest()
        return LockedServeServiceVersionIdentityV1(
            identity=projected,
            identity_sha256=projected.sha256,
            yaml_content_sha256=yaml_sha256)

    @staticmethod
    def _lock_class2_versions(
        connection: sqlalchemy.engine.Connection,
        *,
        owner: ServeControllerOwnerFenceV1,
        service_row: Mapping[str, Any],
        action_spec: resource_actions.ServeReplicaActionSpecV2,
        authoritative_policy: LockedServeAuthoritativePolicyV1,
    ) -> ServeProvisionalClass2IdentityEvidenceV1:
        """Lock and project elected/action versions in ascending order."""

        elected_version = service_row['current_version']
        action_identity = action_spec.service_version_spec_identity
        action_version = action_identity.service_version
        versions = tuple(sorted({elected_version, action_version}))
        locked_versions: list[LockedServeServiceVersionIdentityV1] = []
        for version in versions:
            is_action_version = version == action_version
            locked = ServeServiceVersionIdentityStore._lock_and_project_version(
                connection,
                owner=owner,
                service_version=version,
                expected_action_identity=(action_identity
                                          if is_action_version else None),
                expected_action_identity_sha256=(
                    action_spec.service_version_spec_identity_sha256
                    if is_action_version else None))
            locked_versions.append(locked)

        supported = (resource_actions.ServeActionCapacityProfileV1.
                     ordinary_ondemand_physical_width1())
        profiles = {(locked.identity.capacity_profile.canonical_bytes,
                     locked.identity.provider_profile)
                    for locked in locked_versions}
        if any(locked.identity.capacity_profile.canonical_bytes !=
               supported.canonical_bytes for locked in locked_versions):
            raise ServeServiceVersionIdentityConflict(
                'Locked service version uses an unsupported capacity profile.')
        if len(profiles) != 1:
            raise ServeServiceVersionIdentityConflict(
                'Elected and replica-creating versions have different '
                'capacity/provider profiles.')
        return ServeProvisionalClass2IdentityEvidenceV1(
            owner_fence=owner,
            elected_service_version=elected_version,
            action_service_version=action_version,
            locked_versions=tuple(locked_versions),
            authoritative_policy=authoritative_policy)

    @staticmethod
    def _lock_replica(
        connection: sqlalchemy.engine.Connection,
        *,
        owner: ServeControllerOwnerFenceV1,
        replica: ServeReplicaActionRecordFenceV1,
        action_spec: resource_actions.ServeReplicaActionSpecV2,
        allow_unbound_launch: bool = False,
    ) -> Mapping[str, Any]:
        if type(replica) is not ServeReplicaActionRecordFenceV1:
            raise TypeError('replica has an invalid type.')
        identity = action_spec.invocation.resource_identity
        target = action_spec.invocation.requested_target
        if (identity.service_incarnation != owner.service_incarnation or
                identity.replica_id != replica.replica_id or
                identity.replica_incarnation != replica.replica_incarnation or
                identity.desired_generation != replica.desired_generation or
                target.sky_cluster_name != replica.cluster_name or
                target.sky_cluster_record_uuid
                != replica.sky_cluster_record_uuid or
                replica.creating_service_version
                != action_spec.service_version_spec_identity.service_version):
            raise ServeServiceVersionIdentityConflict(
                'Replica fence differs from the V2 action identity.')

        table = serve_state_schema.replicas_table
        row = connection.execute(
            sqlalchemy.select(table).where(
                table.c.service_name == owner.service_name,
                table.c.replica_id ==
                replica.replica_id).with_for_update()).mappings().one_or_none()
        if row is None:
            raise ServeServiceVersionIdentityConflict(
                'Exact replica record no longer exists.')
        expected = {
            'replica_incarnation': replica.replica_incarnation,
            'desired_generation': replica.desired_generation,
            'version': replica.creating_service_version,
            'replica_state_version': replica.replica_state_version,
            'is_spot': replica.is_spot,
            'cluster_name': replica.cluster_name,
            'sky_cluster_record_uuid': replica.sky_cluster_record_uuid,
        }
        if any(row[name] != value for name, value in expected.items()):
            raise ServeServiceVersionIdentityConflict(
                'Replica record, creating version, or action linkage changed.')
        if row['paid_capacity_pool_key'] is not None:
            raise ServeServiceVersionIdentityConflict(
                'Paid-capacity replicas cannot enter this M4 profile.')
        competing_shadow_fields = (
            'launch_shadow_coverage_id',
            'down_shadow_coverage_id',
            'launch_shadow_sample_id',
            'down_shadow_sample_id',
        )
        # This boundary supports only API-action launch provenance, so any
        # shadow link is an impossible mixed graph.  This is intentionally not
        # a global Serve invariant: the future shadow-source authoritative-down
        # boundary may retain launch-shadow linkage beside a down action after
        # locking coverage and parent rows in their canonical classes.
        if any(row[name] is not None for name in competing_shadow_fields):
            raise ServeServiceVersionIdentityConflict(
                'Action linkage is exclusive with shadow coverage/sample '
                'linkage.')
        if allow_unbound_launch and row['launch_action_id'] is None:
            unbound_fields = ('down_action_id',
                              'resource_action_spec_identity_sha256')
            if any(row[name] is not None for name in unbound_fields):
                raise ServeServiceVersionIdentityCorruption(
                    'Unbound launch replica has a partial action identity.')
        elif (row['launch_action_id'] != replica.launch_action_id or
              row['down_action_id'] != replica.down_action_id):
            raise ServeServiceVersionIdentityConflict(
                'Replica launch/down action linkage changed.')
        return cast(Mapping[str, Any], row)

    @staticmethod
    def _stage_replica_identity(
        connection: sqlalchemy.engine.Connection,
        *,
        owner: ServeControllerOwnerFenceV1,
        replica: ServeReplicaActionRecordFenceV1,
        row: Mapping[str, Any],
        identity_sha256: str,
    ) -> bool:
        stored_hash = row['resource_action_spec_identity_sha256']
        if stored_hash is not None:
            try:
                canonical_stored_hash = _sha256(
                    stored_hash,
                    name='replica resource_action_spec_identity_sha256')
            except ValueError as e:
                raise ServeServiceVersionIdentityCorruption(str(e)) from e
            if canonical_stored_hash != identity_sha256:
                raise ServeServiceVersionIdentityConflict(
                    'Replica is already bound to another creating-version '
                    'identity.')
            return False

        if row['launch_action_id'] is not None:
            raise ServeServiceVersionIdentityCorruption(
                'Replica launch linkage exists without its version identity.')

        table = serve_state_schema.replicas_table
        result = connection.execute(
            sqlalchemy.update(table).where(
                table.c.service_name == owner.service_name,
                table.c.replica_id == replica.replica_id,
                table.c.replica_incarnation == replica.replica_incarnation,
                table.c.desired_generation == replica.desired_generation,
                table.c.version == replica.creating_service_version,
                table.c.replica_state_version == replica.replica_state_version,
                table.c.is_spot.is_(False),
                table.c.paid_capacity_pool_key.is_(None),
                table.c.cluster_name == replica.cluster_name,
                table.c.sky_cluster_record_uuid ==
                replica.sky_cluster_record_uuid,
                table.c.launch_action_id.is_(None),
                table.c.down_action_id.is_(None),
                table.c.launch_shadow_coverage_id.is_(None),
                table.c.down_shadow_coverage_id.is_(None),
                table.c.launch_shadow_sample_id.is_(None),
                table.c.down_shadow_sample_id.is_(None),
                table.c.resource_action_spec_identity_sha256.is_(None)).values(
                    launch_action_id=replica.launch_action_id,
                    resource_action_spec_identity_sha256=identity_sha256))
        if result.rowcount != 1:
            raise ServeServiceVersionIdentityConflict(
                'Replica identity changed while its row was locked.')
        return True

    @staticmethod
    def _validate_launch_source(
        service_row: Mapping[str, Any],
        locked: LockedServeServiceVersionIdentityV1,
        action_spec: resource_actions.ServeReplicaActionSpecV2,
    ) -> None:
        launch = action_spec.invocation.require_launch()
        source = launch.source.content
        identity = locked.identity
        if (source.service_name != identity.service_name or
                source.service_incarnation != identity.service_incarnation or
                source.service_version != identity.service_version or
                source.yaml_content_sha256 != locked.yaml_content_sha256 or
                source.workspace != service_row['workspace']):
            raise ServeServiceVersionIdentityConflict(
                'Launch retained source is not byte-equal to the locked '
                'version YAML/workspace identity.')

    def stage_authoritative_launch_class2_replica_link_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        owner: ServeControllerOwnerFenceV1,
        replica: ServeReplicaActionRecordFenceV1,
        action_spec: resource_actions.ServeReplicaActionSpecV2,
    ) -> ServeAuthoritativeLaunchClass2ReplicaLinkStageV1:
        """Project authoritative launch class-2 state and stage replica link.

        The immutable version pair must already have been initialized by the
        version-commit/private-activation boundary with its complete source,
        secret, and representability proof.  This internal method never writes
        version identity and is not admission or lost-ack recovery.  Its
        provisional replica link must roll back unless the caller completes
        the full cohort/reference/action graph in this same transaction.
        """

        self._require_transaction(connection)
        if type(action_spec) is not resource_actions.ServeReplicaActionSpecV2:
            raise TypeError('action_spec must be a live V2 action spec.')
        self._require_authoritative_spec(action_spec)
        if action_spec.invocation.action_kind is not kernel_actions.ActionKind.LAUNCH:
            raise ValueError('launch identity staging requires a launch spec.')
        if replica.down_action_id is not None:
            raise ServeServiceVersionIdentityConflict(
                'Launch identity staging requires no down-action linkage.')
        if replica.launch_action_id != action_spec.action_id:
            raise ServeServiceVersionIdentityConflict(
                'Replica launch action ID differs from the V2 action.')
        self._lock_lifecycle_fence(connection, owner)
        service_row = self._lock_service(connection,
                                         owner,
                                         action_spec,
                                         require_elected_version=True)
        policy = self._lock_authoritative_policy(connection,
                                                 owner=owner,
                                                 action_spec=action_spec)
        evidence = self._lock_class2_versions(connection,
                                              service_row=service_row,
                                              owner=owner,
                                              action_spec=action_spec,
                                              authoritative_policy=policy)
        locked = evidence.action_identity
        self._validate_launch_source(service_row, locked, action_spec)
        replica_row = self._lock_replica(connection,
                                         owner=owner,
                                         replica=replica,
                                         action_spec=action_spec,
                                         allow_unbound_launch=True)
        replica_initialized = self._stage_replica_identity(
            connection,
            owner=owner,
            replica=replica,
            row=replica_row,
            identity_sha256=locked.identity_sha256)
        return ServeAuthoritativeLaunchClass2ReplicaLinkStageV1(
            class2_evidence=evidence,
            replica_link_initialized=replica_initialized)

    @staticmethod
    def _read_prior_launch_spec(
        connection: sqlalchemy.engine.Connection,
        *,
        action_id: uuid.UUID,
    ) -> resource_actions.ServeReplicaActionSpecV2:
        """Read immutable launch bytes without taking a class-11 row lock.

        API resource-action immutable identity/spec columns have no legal
        update or deletion path.  A nonlocking MVCC read is intentional here:
        taking only the launch row before the caller admits the down row could
        violate canonical UUID order for class-11 action locks.  The later
        combined admission boundary remains responsible for locking/admitting
        the down row in its canonical class-11 position.
        """

        table = request_schema.RESOURCE_ACTIONS
        row = connection.execute(
            sqlalchemy.select(table).where(
                table.c.action_id == action_id)).mappings().one_or_none()
        if row is None:
            raise ServeServiceVersionIdentityConflict(
                'Down action has no retained prior launch action.')
        try:
            prior = resource_actions.serve_replica_action_spec_from_value_v2(
                row['immutable_spec'])
            prior.require_authoritative_action_binding()
            stored_hash = _sha256(row['immutable_spec_sha256'],
                                  name='prior launch immutable spec hash')
        except (KeyError, TypeError, ValueError) as e:
            raise ServeServiceVersionIdentityCorruption(
                f'Retained prior launch action is not valid V2: {e}') from e
        kernel_identity = prior.invocation.resource_identity.action_identity(
            kernel_actions.ActionKind.LAUNCH)
        if (row['action_id'] != prior.action_id or row['domain'] != 'serve' or
                row['resource_type'] != 'replica' or
                row['resource_identity'] != kernel_identity.resource_identity or
                row['desired_generation'] != kernel_identity.desired_generation
                or
                row['action_type'] != kernel_actions.ActionKind.LAUNCH.value or
                stored_hash != prior.sha256):
            raise ServeServiceVersionIdentityCorruption(
                'Retained prior launch action row differs from its immutable '
                'V2 spec.')
        return prior

    def project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        owner: ServeControllerOwnerFenceV1,
        replica: ServeReplicaActionRecordFenceV1,
        down_action_spec: resource_actions.ServeReplicaActionSpecV2,
    ) -> ServeAuthoritativeDownClass2PriorLaunchOptimisticSnapshotV1:
        """Lock class-2 evidence and read an optimistic prior-launch snapshot.

        This is a provisional internal boundary, not down admission.  It locks
        the class-1 lifecycle fence, service, exact authoritative policy, the
        sorted union of elected and replica-creating versions, and the replica.
        It does not lock any class-11 action row.  Full admission must subsequently
        lock the sorted action set, revalidate the snapshot byte-exactly, and
        validate source action/attempt/progress, cleanup target, cohort,
        reference, and represented-action evidence before the same transaction
        may commit.
        """

        self._require_transaction(connection)
        if type(down_action_spec) is not (
                resource_actions.ServeReplicaActionSpecV2):
            raise TypeError('down_action_spec must be a live V2 action spec.')
        self._require_authoritative_spec(down_action_spec)
        if down_action_spec.invocation.action_kind is not (
                kernel_actions.ActionKind.DOWN):
            raise ValueError('prior launch resolution requires a down spec.')
        if replica.down_action_id != down_action_spec.action_id:
            raise ServeServiceVersionIdentityConflict(
                'Replica down action ID differs from the V2 action.')

        self._lock_lifecycle_fence(connection, owner)
        service_row = self._lock_service(connection,
                                         owner,
                                         down_action_spec,
                                         require_elected_version=False)
        policy = self._lock_authoritative_policy(connection,
                                                 owner=owner,
                                                 action_spec=down_action_spec)
        evidence = self._lock_class2_versions(connection,
                                              owner=owner,
                                              service_row=service_row,
                                              action_spec=down_action_spec,
                                              authoritative_policy=policy)
        locked = evidence.action_identity
        replica_row = self._lock_replica(connection,
                                         owner=owner,
                                         replica=replica,
                                         action_spec=down_action_spec)
        try:
            replica_identity_hash = _sha256(
                replica_row['resource_action_spec_identity_sha256'],
                name='replica resource_action_spec_identity_sha256')
        except ValueError as e:
            raise ServeServiceVersionIdentityCorruption(str(e)) from e
        if replica_identity_hash != locked.identity_sha256:
            raise ServeServiceVersionIdentityConflict(
                'Replica identity does not name its locked creating version.')

        basis = down_action_spec.invocation.require_down().prior_launch_basis
        if basis.launch_action_id != replica.launch_action_id:
            raise ServeServiceVersionIdentityConflict(
                'Down basis does not name the replica row prior launch.')
        if basis.source_store is not (
                resource_actions.ProviderPriorLaunchSourceStoreV1.
                API_RESOURCE_ACTIONS):
            raise ServeServiceVersionIdentityConflict(
                'Shadow-sample prior-launch provenance is unsupported by this '
                'class-2 slice; full admission must resolve it under '
                'coverage/parent lock ordering.')
        prior = self._read_prior_launch_spec(connection,
                                             action_id=replica.launch_action_id)
        try:
            down_action_spec.validate_down_prior_launch_spec(prior)
        except (TypeError, ValueError) as e:
            raise ServeServiceVersionIdentityConflict(
                f'Down action is not linked to its retained launch: {e}') from e
        if (prior.service_version_spec_identity.canonical_bytes
                != locked.identity.canonical_bytes or
                prior.service_version_spec_identity_sha256
                != locked.identity_sha256):
            raise ServeServiceVersionIdentityCorruption(
                'Retained launch identity differs from its locked immutable '
                'version.')
        return ServeAuthoritativeDownClass2PriorLaunchOptimisticSnapshotV1(
            class2_evidence=evidence,
            prior_launch_spec=prior,
            prior_launch_spec_sha256=prior.sha256)


__all__ = [
    'LockedServeAuthoritativePolicyV1',
    'LockedServeServiceVersionIdentityV1',
    'ServeAuthoritativeDownClass2PriorLaunchOptimisticSnapshotV1',
    'ServeAuthoritativeLaunchClass2ReplicaLinkStageV1',
    'ServeControllerOwnerFenceV1',
    'ServeProvisionalClass2IdentityEvidenceV1',
    'ServeReplicaActionRecordFenceV1',
    'ServeServiceVersionIdentityConflict',
    'ServeServiceVersionIdentityCorruption',
    'ServeServiceVersionIdentityStateError',
    'ServeServiceVersionIdentityStore',
]
