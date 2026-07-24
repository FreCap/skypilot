"""Profile, physical shard, location, budget, and worker persistence."""
# pylint: disable=missing-class-docstring

from __future__ import annotations

from collections.abc import Callable
import dataclasses
import hashlib
import json
import time
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.container_images import catalog_state
from sky.container_images import models
from sky.container_images import schema


class StaleProfileRevisionError(ValueError):
    """A worker or activation attempted to use a superseded revision."""


class QualificationMutationInProgressError(ValueError):
    """A global qualification delete or restoration is still active."""


class CanonicalCustodyChangeError(ValueError):
    """V0 cannot move canonical physical custody after a release is READY."""


class RegistryCapacityExhaustedError(ValueError):
    """Every qualified fixed shard is full for the requested reservation."""


class RegistryShardUnavailableError(ValueError):
    """The selected shard cannot admit or retry the requested location."""


class RegistryLocationQuarantinedError(ValueError):
    """The exact physical location cannot be safely reused after a delete."""


class LocationLeaseLostError(RuntimeError):
    """A location completion no longer owns the random database fence."""


class _InventoryLeaseFenceLost(RuntimeError):
    """An inventory page expired while committing its bounded result."""


@dataclasses.dataclass(frozen=True)
class ProfileRevisionRecord:
    id: str
    workspace: str
    profile: str
    revision: int
    desired_generation: int
    state: models.ImageProfileState
    config_hash: str
    config_snapshot: dict[str, Any]
    terraform_hash: str | None
    physical_manifest_hash: str
    attestations: dict[str, Any]
    attestations_hash: str | None
    qualified_at: int | None
    failed_code: str | None
    canary_window_day: str | None
    canary_reserved_microusd: int
    max_daily_canary_microusd: int
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class QualificationRepositoryQuarantineRecord:
    repository_arn: str
    owner_profile_revision_id: str
    owner_target: str
    owner_target_fingerprint: str
    runtime_digest: str
    lifecycle_proof_id: str
    quarantine_reason: str
    quarantined_at: int


@dataclasses.dataclass(frozen=True)
class ShardRecord:
    id: str
    workspace: str
    profile: str
    profile_revision_id: str | None
    target_id: str
    provider: str
    partition: str
    account: str
    region: str
    shard_generation: int
    shard_index: int
    target_fingerprint: str
    physical_fingerprint: str
    eviction_enabled: bool
    registry: str
    repository_name: str
    repository_arn: str
    max_manifests: int
    max_declared_bytes: int
    reserved_manifests: int
    reserved_declared_bytes: int
    observed_manifests: int
    max_in_flight: int
    in_flight: int
    state: models.ImageShardState
    qualified_at: int | None
    last_dispatch_at: int | None
    inventory_epoch: int
    inventory_cursor: str | None
    inventory_started_at: int | None
    inventory_completed_at: int | None
    inventory_finalizing: bool
    inventory_lease_token: str | None
    inventory_lease_expires_at: int | None
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class LocationRecord:
    id: str
    workspace: str
    image_id: str
    shard_id: str
    target_fingerprint: str
    physical_fingerprint: str
    runtime_digest: str
    canonical: bool
    canonical_location_id: str | None
    target_ref: str
    state: models.ImageLocationState
    lease_kind: str | None
    lease_token: str | None
    lease_expires_at: int | None
    attempt_count: int
    next_retry_at: int | None
    error_code: str | None
    last_verified_at: int | None
    last_used_at: int | None
    inventory_epoch_seen: int | None
    reserved_declared_bytes: int
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class WorkerRecord:
    id: str
    kind: models.ImageWorkerKind
    version: str
    started_at: int
    heartbeat_at: int
    last_success_at: int | None
    in_flight: int
    max_in_flight: int
    grant_budget_id: str | None
    grant_tokens_milli: int
    grant_expires_at: int | None


@dataclasses.dataclass(frozen=True)
class ProviderGrant:
    budget_id: str
    tokens: int
    valid_for_seconds: int


@dataclasses.dataclass(frozen=True)
class ProviderBudgetRecord:
    id: str
    provider: str
    partition: str
    account: str
    region: str
    api_family: str
    applied_rate_milli: int
    burst_milli: int
    tokens_milli: int
    refilled_at: int
    blocked_until: int | None
    throttle_count: int
    updated_at: int


def _profile(row: sqlalchemy.engine.RowMapping) -> ProfileRevisionRecord:
    return ProfileRevisionRecord(
        id=str(row['id']),
        workspace=str(row['workspace']),
        profile=str(row['profile']),
        revision=int(row['revision']),
        desired_generation=int(row['desired_generation']),
        state=models.ImageProfileState(str(row['state'])),
        config_hash=str(row['config_hash']),
        config_snapshot=json.loads(str(row['config_json'])),
        terraform_hash=row['terraform_hash'],
        physical_manifest_hash=str(row['physical_manifest_hash']),
        attestations=json.loads(str(row['attestations_json'])),
        attestations_hash=row['attestations_hash'],
        qualified_at=row['qualified_at'],
        failed_code=row['failed_code'],
        canary_window_day=row['canary_window_day'],
        canary_reserved_microusd=int(row['canary_reserved_microusd']),
        max_daily_canary_microusd=int(row['max_daily_canary_microusd']),
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def _qualification_repository_quarantine(
    row: sqlalchemy.engine.RowMapping,
) -> QualificationRepositoryQuarantineRecord:
    return QualificationRepositoryQuarantineRecord(
        repository_arn=str(row['repository_arn']),
        owner_profile_revision_id=str(row['owner_profile_revision_id']),
        owner_target=str(row['owner_target']),
        owner_target_fingerprint=str(row['owner_target_fingerprint']),
        runtime_digest=str(row['runtime_digest']),
        lifecycle_proof_id=str(row['lifecycle_proof_id']),
        quarantine_reason=str(row['quarantine_reason']),
        quarantined_at=int(row['quarantined_at']))


def _shard(row: sqlalchemy.engine.RowMapping) -> ShardRecord:
    return ShardRecord(
        id=str(row['id']),
        workspace=str(row['workspace']),
        profile=str(row['profile']),
        profile_revision_id=row['profile_revision_id'],
        target_id=str(row['target_id']),
        provider=str(row['provider']),
        partition=str(row['partition']),
        account=str(row['account']),
        region=str(row['region']),
        shard_generation=int(row['shard_generation']),
        shard_index=int(row['shard_index']),
        target_fingerprint=str(row['target_fingerprint']),
        physical_fingerprint=str(row['physical_fingerprint']),
        eviction_enabled=bool(row['eviction_enabled']),
        registry=str(row['registry']),
        repository_name=str(row['repository_name']),
        repository_arn=str(row['repository_arn']),
        max_manifests=int(row['max_manifests']),
        max_declared_bytes=int(row['max_declared_bytes']),
        reserved_manifests=int(row['reserved_manifests']),
        reserved_declared_bytes=int(row['reserved_declared_bytes']),
        observed_manifests=int(row['observed_manifests']),
        max_in_flight=int(row['max_in_flight']),
        in_flight=int(row['in_flight']),
        state=models.ImageShardState(str(row['state'])),
        qualified_at=row['qualified_at'],
        last_dispatch_at=row['last_dispatch_at'],
        inventory_epoch=int(row['inventory_epoch']),
        inventory_cursor=row['inventory_cursor'],
        inventory_started_at=row['inventory_started_at'],
        inventory_completed_at=row['inventory_completed_at'],
        inventory_finalizing=bool(row['inventory_finalizing']),
        inventory_lease_token=row['inventory_lease_token'],
        inventory_lease_expires_at=row['inventory_lease_expires_at'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def _inventory_active(row: sqlalchemy.engine.RowMapping) -> bool:
    """Returns whether one shard is listing or finalizing an inventory epoch."""
    return (row['inventory_started_at'] is not None and
            (row['inventory_completed_at'] is None or
             bool(row['inventory_finalizing'])))


def _inventory_active_condition(table: sqlalchemy.Table) -> Any:
    """Builds the SQL equivalent of `_inventory_active`."""
    return sqlalchemy.and_(
        table.c.inventory_started_at.is_not(None),
        sqlalchemy.or_(table.c.inventory_completed_at.is_(None),
                       table.c.inventory_finalizing.is_(True)))


def _location(row: sqlalchemy.engine.RowMapping) -> LocationRecord:
    return LocationRecord(
        id=str(row['id']),
        workspace=str(row['workspace']),
        image_id=str(row['image_id']),
        shard_id=str(row['shard_id']),
        target_fingerprint=str(row['target_fingerprint']),
        physical_fingerprint=str(row['physical_fingerprint']),
        runtime_digest=str(row['runtime_digest']),
        canonical=bool(row['canonical']),
        canonical_location_id=row['canonical_location_id'],
        target_ref=str(row['target_ref']),
        state=models.ImageLocationState(str(row['state'])),
        lease_kind=row['lease_kind'],
        lease_token=row['lease_token'],
        lease_expires_at=row['lease_expires_at'],
        attempt_count=int(row['attempt_count']),
        next_retry_at=row['next_retry_at'],
        error_code=row['error_code'],
        last_verified_at=row['last_verified_at'],
        last_used_at=row['last_used_at'],
        inventory_epoch_seen=row['inventory_epoch_seen'],
        reserved_declared_bytes=int(row['reserved_declared_bytes']),
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def _worker(row: sqlalchemy.engine.RowMapping) -> WorkerRecord:
    return WorkerRecord(
        id=str(row['id']),
        kind=models.ImageWorkerKind(str(row['kind'])),
        version=str(row['version']),
        started_at=int(row['started_at']),
        heartbeat_at=int(row['heartbeat_at']),
        last_success_at=row['last_success_at'],
        in_flight=int(row['in_flight']),
        max_in_flight=int(row['max_in_flight']),
        grant_budget_id=row['grant_budget_id'],
        grant_tokens_milli=int(row['grant_tokens_milli']),
        grant_expires_at=row['grant_expires_at'],
    )


def _provider_budget(row: sqlalchemy.engine.RowMapping) -> ProviderBudgetRecord:
    return ProviderBudgetRecord(
        id=str(row['id']),
        provider=str(row['provider']),
        partition=str(row['partition']),
        account=str(row['account']),
        region=str(row['region']),
        api_family=str(row['api_family']),
        applied_rate_milli=int(row['applied_rate_milli']),
        burst_milli=int(row['burst_milli']),
        tokens_milli=int(row['tokens_milli']),
        refilled_at=int(row['refilled_at']),
        blocked_until=row['blocked_until'],
        throttle_count=int(row['throttle_count']),
        updated_at=int(row['updated_at']),
    )


def lock_profile_mutation_in_session(session: orm.Session, *, workspace: str,
                                     profile: str) -> None:
    """Serializes one profile's bounded staging and activation mutations."""
    lock_key = json.dumps([workspace, profile], separators=(',', ':'))
    session.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
        {'key': f'skypilot:container-image-profile:{lock_key}'})


def lock_qualification_mutation_in_session(session: orm.Session, *,
                                           exclusive: bool) -> None:
    """Locks the catalog-wide destructive qualification barrier."""
    function = ('pg_advisory_xact_lock'
                if exclusive else 'pg_advisory_xact_lock_shared')
    session.execute(
        sqlalchemy.text(f'SELECT {function}(hashtextextended(:key, 0))'),
        {'key': 'skypilot:container-image-qualification-mutation'})


def get_qualification_mutation_in_session(
    session: orm.Session,
    *,
    exclusive: bool,
) -> sqlalchemy.engine.RowMapping | None:
    """Locks and returns the singleton destructive qualification barrier."""
    lock_qualification_mutation_in_session(session, exclusive=exclusive)
    return session.execute(
        sqlalchemy.select(schema.qualification_mutation).where(
            schema.qualification_mutation.c.id == 'global')).mappings().first()


def assert_qualification_mutation_idle_in_session(session: orm.Session) -> None:
    """Acquires a shared barrier lock and rejects active mutation windows."""
    if get_qualification_mutation_in_session(session,
                                             exclusive=False) is not None:
        raise QualificationMutationInProgressError(
            'Qualification delete or restoration is in progress.')


def qualification_repository_quarantined_in_session(
        session: orm.Session, repository_arn: str) -> bool:
    """Returns whether a physical qualification repository is tombstoned."""
    if not isinstance(repository_arn, str) or not repository_arn:
        raise ValueError('Qualification repository ARN is invalid.')
    return bool(
        session.execute(
            sqlalchemy.select(sqlalchemy.exists().where(
                schema.qualification_repository_quarantines.c.repository_arn ==
                repository_arn))).scalar_one())


def qualification_repository_quarantined(repository_arn: str) -> bool:
    """Returns the durable catalog-wide quarantine for one repository."""
    with orm.Session(catalog_state.engine()) as session:
        return qualification_repository_quarantined_in_session(
            session, repository_arn)


def list_qualification_repository_quarantines(
    *,
    limit: int = 1001,
) -> list[QualificationRepositoryQuarantineRecord]:
    """Returns a bounded oldest-first physical quarantine projection."""
    if not 1 <= limit <= 1001:
        raise ValueError(
            'Qualification repository quarantine page size is invalid.')
    table = schema.qualification_repository_quarantines
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(table).order_by(
                table.c.quarantined_at,
                table.c.repository_arn).limit(limit)).mappings().all()
    return [_qualification_repository_quarantine(row) for row in rows]


def qualification_repository_quarantines_for_arns(
    repository_arns: set[str],
) -> list[QualificationRepositoryQuarantineRecord]:
    """Returns exact tombstones for a caller-bounded repository identity set."""
    if (len(repository_arns) > 257_000 or
            any(not isinstance(repository_arn, str) or not repository_arn
                for repository_arn in repository_arns)):
        raise ValueError('Qualification repository ARN lookup is invalid.')
    if not repository_arns:
        return []
    table = schema.qualification_repository_quarantines
    repository_array = sqlalchemy.bindparam('qualification_repository_arns',
                                            value=sorted(repository_arns),
                                            type_=postgresql.ARRAY(
                                                sqlalchemy.Text))
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.repository_arn == sqlalchemy.any_(
                    repository_array)).order_by(
                        table.c.repository_arn)).mappings().all()
    return [_qualification_repository_quarantine(row) for row in rows]


def qualification_revision_owns_work_in_session(
    session: orm.Session,
    *,
    profile_revision_id: str,
    workspace: str,
    profile: str,
    state: models.ImageProfileState,
) -> bool:
    """Checks that a revision still owns provider qualification work."""
    if state == models.ImageProfileState.QUALIFYING:
        return True
    if state != models.ImageProfileState.ACTIVE:
        return False
    candidate = schema.profile_revisions.alias('qualification_successor')
    return not bool(
        session.execute(
            sqlalchemy.select(sqlalchemy.exists().where(
                candidate.c.workspace == workspace, candidate.c.profile
                == profile, candidate.c.id != profile_revision_id,
                candidate.c.state
                == models.ImageProfileState.QUALIFYING.value))).scalar_one())


def lock_profile_revision_mutation_in_session(
        session: orm.Session,
        profile_revision_id: str) -> sqlalchemy.engine.RowMapping:
    """Locks one revision behind its immutable profile advisory lock."""
    table = schema.profile_revisions
    identity = session.execute(
        sqlalchemy.select(table.c.workspace, table.c.profile).where(
            table.c.id == profile_revision_id)).mappings().first()
    if identity is None:
        raise StaleProfileRevisionError(
            'The image profile revision no longer exists.')
    workspace = str(identity['workspace'])
    profile = str(identity['profile'])
    lock_profile_mutation_in_session(session,
                                     workspace=workspace,
                                     profile=profile)
    row = session.execute(
        sqlalchemy.select(table).where(table.c.id == profile_revision_id).
        with_for_update()).mappings().first()
    if (row is None or str(row['workspace']) != workspace or
            str(row['profile']) != profile):
        raise StaleProfileRevisionError(
            'The image profile revision no longer exists.')
    return row


def lock_profile_custody_for_revision_in_session(
        session: orm.Session,
        profile_revision_id: str) -> sqlalchemy.engine.RowMapping:
    """Locks the immutable custody key before any shard or location row."""
    revision = session.execute(
        sqlalchemy.select(schema.profile_revisions).where(
            schema.profile_revisions.c.id ==
            profile_revision_id)).mappings().first()
    if revision is None:
        raise StaleProfileRevisionError(
            'The image profile revision no longer exists.')
    lock_profile_mutation_in_session(session,
                                     workspace=str(revision['workspace']),
                                     profile=str(revision['profile']))
    return revision


def record_profile_custody_in_session(session: orm.Session,
                                      revision: sqlalchemy.engine.RowMapping, *,
                                      expected_workspace: str,
                                      expected_profile: str, now: int) -> None:
    """Records or revalidates immutable physical custody under its lock."""
    workspace = str(revision['workspace'])
    profile = str(revision['profile'])
    physical_manifest_hash = str(revision['physical_manifest_hash'])
    if workspace != expected_workspace or profile != expected_profile:
        raise CanonicalCustodyChangeError(
            'A canonical location cannot project releases across profiles.')
    table = schema.profile_custody
    session.execute(
        postgresql.insert(table).values(
            workspace=workspace,
            profile=profile,
            physical_manifest_hash=physical_manifest_hash,
            first_profile_revision_id=str(revision['id']),
            acquired_at=now).on_conflict_do_nothing(
                index_elements=[table.c.workspace, table.c.profile]))
    custody = session.execute(
        sqlalchemy.select(table).where(
            table.c.workspace == workspace,
            table.c.profile == profile)).mappings().one()
    if str(custody['physical_manifest_hash']) != physical_manifest_hash:
        raise CanonicalCustodyChangeError(
            'V0 cannot change the canonical physical manifest after a '
            'release is published.')


def _is_fresh_qualification_repository_successor(
    session: orm.Session,
    mutation: sqlalchemy.engine.RowMapping,
    *,
    workspace: str,
    profile: str,
    revision: int,
    config_snapshot: dict[str, Any],
    physical_manifest_hash: str,
) -> bool:
    """Validates the only revision allowed through a quarantine barrier."""
    if str(mutation['state']) != 'QUARANTINED':
        return False
    owner = session.execute(
        sqlalchemy.select(schema.profile_revisions).where(
            schema.profile_revisions.c.id ==
            mutation['owner_profile_revision_id'])).mappings().first()
    if (owner is None or str(owner['workspace']) != workspace or
            str(owner['profile']) != profile or
            revision <= int(owner['revision']) or
            str(owner['physical_manifest_hash']) != physical_manifest_hash):
        return False
    try:
        owner_profile = models.ManagedRegistryProfile.from_snapshot(
            json.loads(str(owner['config_json'])))
        successor_profile = models.ManagedRegistryProfile.from_snapshot(
            config_snapshot)
        owner_target = owner_profile.target(str(mutation['owner_target']))
        successor_target = successor_profile.target(
            str(mutation['owner_target']))
    except (KeyError, TypeError, ValueError):
        return False
    return bool(successor_profile.name == profile and
                successor_profile.revision == revision and
                owner_target.target_fingerprint == str(
                    mutation['owner_target_fingerprint']) and
                successor_target.target_fingerprint
                == owner_target.target_fingerprint and
                successor_target.qualification_repository_generation
                > owner_target.qualification_repository_generation)


def _qualification_repository_generations_are_monotonic(
    session: orm.Session,
    *,
    workspace: str,
    profile: str,
    config_snapshot: dict[str, Any],
) -> bool:
    """Rejects an immediate generation decrease for an unchanged target."""
    previous = session.execute(
        sqlalchemy.select(schema.profile_revisions).where(
            schema.profile_revisions.c.workspace == workspace,
            schema.profile_revisions.c.profile == profile).order_by(
                schema.profile_revisions.c.desired_generation.desc()).limit(
                    1)).mappings().first()
    if previous is None:
        return True
    try:
        previous_profile = models.ManagedRegistryProfile.from_snapshot(
            json.loads(str(previous['config_json'])))
        candidate_profile = models.ManagedRegistryProfile.from_snapshot(
            config_snapshot)
        for previous_target in ((previous_profile.canonical,) +
                                previous_profile.targets):
            try:
                candidate_target = candidate_profile.target(
                    previous_target.name)
            except ValueError:
                continue
            if (candidate_target.target_fingerprint
                    == previous_target.target_fingerprint and
                    candidate_target.qualification_repository_generation
                    < previous_target.qualification_repository_generation):
                return False
    except (TypeError, ValueError):
        return False
    return True


def stage_profile_revision(*,
                           workspace: str,
                           profile: str,
                           revision: int,
                           config_hash: str,
                           config_snapshot: dict[str, Any],
                           physical_manifest_hash: str,
                           max_daily_canary_microusd: int,
                           now: int | None = None) -> ProfileRevisionRecord:
    """Stages a nonblocking desired revision and supersedes older desired work."""
    encoded_config = json.dumps(config_snapshot,
                                sort_keys=True,
                                separators=(',', ':'))
    if len(encoded_config.encode()) > 256 * 1024:
        raise ValueError('Registry profile snapshot exceeds 256 KiB.')
    if hashlib.sha256(encoded_config.encode()).hexdigest() != config_hash:
        raise ValueError('Registry profile snapshot does not match its hash.')
    table = schema.profile_revisions
    with orm.Session(catalog_state.engine()) as session, session.begin():
        lock_profile_mutation_in_session(session,
                                         workspace=workspace,
                                         profile=profile)
        candidate = session.execute(
            sqlalchemy.select(table).where(
                table.c.workspace == workspace, table.c.profile == profile,
                table.c.revision ==
                revision).with_for_update()).mappings().first()
        if candidate is not None:
            immutable_payload_matches = (
                str(candidate['config_hash']) == config_hash and
                str(candidate['config_json']) == encoded_config and
                str(candidate['physical_manifest_hash'])
                == physical_manifest_hash)
            if not immutable_payload_matches:
                raise ValueError(
                    'Registry profile revision immutable payload mismatch.')
            if str(candidate['state']) in (
                    models.ImageProfileState.QUALIFYING.value,
                    models.ImageProfileState.ACTIVE.value):
                return _profile(candidate)
            raise ValueError(
                'Registry profile revision is no longer operational.')

        current_qualifying = session.execute(
            sqlalchemy.select(table.c.id).where(
                table.c.workspace == workspace, table.c.profile == profile,
                table.c.state == models.ImageProfileState.QUALIFYING.value).
            with_for_update()).mappings().first()

        # A quarantine may only be superseded by a later revision that selects
        # a fresh physical qualification repository generation. The barrier
        # remains present until Terraform evidence for that exact repository is
        # recorded, so staging alone cannot re-admit provider work.
        mutation = get_qualification_mutation_in_session(session,
                                                         exclusive=False)
        if (mutation is not None and
                not _is_fresh_qualification_repository_successor(
                    session,
                    mutation,
                    workspace=workspace,
                    profile=profile,
                    revision=revision,
                    config_snapshot=config_snapshot,
                    physical_manifest_hash=physical_manifest_hash)):
            raise QualificationMutationInProgressError(
                'Qualification delete or restoration is in progress.')
        if not _qualification_repository_generations_are_monotonic(
                session,
                workspace=workspace,
                profile=profile,
                config_snapshot=config_snapshot):
            raise ValueError(
                'Qualification repository generations cannot decrease.')
        custody = session.execute(
            sqlalchemy.select(schema.profile_custody).where(
                schema.profile_custody.c.workspace == workspace,
                schema.profile_custody.c.profile ==
                profile)).mappings().first()
        if (custody is not None and str(custody['physical_manifest_hash'])
                != physical_manifest_hash):
            raise CanonicalCustodyChangeError(
                'V0 cannot change the canonical physical manifest after a '
                'release is published.')
        current = catalog_state.database_epoch(session, now=now)
        if current_qualifying is not None:
            session.execute(table.update().where(
                table.c.id == current_qualifying['id']).values(
                    state=models.ImageProfileState.SUPERSEDED.value,
                    updated_at=current))
        generation = int(
            session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.coalesce(
                        sqlalchemy.func.max(table.c.desired_generation),
                        0)).where(table.c.workspace == workspace,
                                  table.c.profile == profile)).scalar_one()) + 1
        row = session.execute(table.insert().values(
            id=str(uuid.uuid4()),
            workspace=workspace,
            profile=profile,
            revision=revision,
            desired_generation=generation,
            state=models.ImageProfileState.QUALIFYING.value,
            config_hash=config_hash,
            config_json=encoded_config,
            physical_manifest_hash=physical_manifest_hash,
            attestations_json='{}',
            canary_reserved_microusd=0,
            max_daily_canary_microusd=max_daily_canary_microusd,
            created_at=current,
            updated_at=current).returning(table)).mappings().one()
        return _profile(row)


def complete_qualification_quarantine_cutover(
    *,
    profile_revision_id: str,
    now: int | None = None,
) -> ProfileRevisionRecord:
    """Clears quarantine only after a fresh repository is Terraform-attested."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        candidate = lock_profile_revision_mutation_in_session(
            session, profile_revision_id)
        mutation = get_qualification_mutation_in_session(session,
                                                         exclusive=True)
        current = _profile(candidate)
        if mutation is None:
            return current
        if str(mutation['state']) != 'QUARANTINED':
            raise QualificationMutationInProgressError(
                'Qualification delete or restoration is in progress.')
        if (current.state != models.ImageProfileState.QUALIFYING or
                not _is_fresh_qualification_repository_successor(
                    session,
                    mutation,
                    workspace=current.workspace,
                    profile=current.profile,
                    revision=current.revision,
                    config_snapshot=current.config_snapshot,
                    physical_manifest_hash=current.physical_manifest_hash)):
            raise QualificationMutationInProgressError(
                'Qualification quarantine requires a fresh repository '
                'generation.')
        if not qualification_repository_quarantined_in_session(
                session, str(mutation['repository_arn'])):
            raise RuntimeError(
                'Qualification quarantine has no durable repository '
                'tombstone.')
        if session.execute(
                sqlalchemy.select(sqlalchemy.exists().where(
                    schema.operations.c.kind == 'PROFILE_CANARY',
                    schema.operations.c.state ==
                    models.ImageOperationState.RUNNING.value))).scalar_one():
            raise QualificationMutationInProgressError(
                'Qualification quarantine cannot cut over while a canary is '
                'running.')

        owner = session.execute(
            sqlalchemy.select(schema.profile_revisions).where(
                schema.profile_revisions.c.id ==
                mutation['owner_profile_revision_id'])).mappings().one()
        owner_profile = models.ManagedRegistryProfile.from_snapshot(
            json.loads(str(owner['config_json'])))
        successor_profile = models.ManagedRegistryProfile.from_snapshot(
            current.config_snapshot)
        target_name = str(mutation['owner_target'])
        owner_target = owner_profile.target(target_name)
        successor_target = successor_profile.target(target_name)
        target_evidence = current.attestations.get(
            models.profile_attestation_key('terraform_target', target_name))
        if (current.terraform_hash is None or
                not isinstance(target_evidence, dict) or
                target_evidence.get('status') != 'READY' or
                target_evidence.get('target_fingerprint')
                != successor_target.target_fingerprint or
                target_evidence.get('registry') != successor_target.registry or
                target_evidence.get('qualification_repository_generation')
                != successor_target.qualification_repository_generation or
                not isinstance(target_evidence.get('repository_name'), str) or
                not isinstance(target_evidence.get('repository_arn'), str) or
                target_evidence['repository_arn'] == mutation['repository_arn']
                or qualification_repository_quarantined_in_session(
                    session, str(target_evidence['repository_arn']))):
            raise QualificationMutationInProgressError(
                'Qualification quarantine successor has no fresh Terraform '
                'repository evidence.')
        quarantine_reason = mutation['quarantine_reason']
        if not isinstance(quarantine_reason, str) or not quarantine_reason:
            raise RuntimeError('Qualification quarantine reason is missing.')
        updated = record_profile_attestation_in_session(
            session,
            profile_revision_id=current.id,
            kind=models.profile_attestation_key('quarantine_cutover',
                                                target_name),
            evidence={
                'status': 'READY',
                'owner_profile_revision_id': str(owner['id']),
                'owner_target': target_name,
                'owner_target_fingerprint': str(
                    mutation['owner_target_fingerprint']),
                'old_repository_arn': str(mutation['repository_arn']),
                'new_repository_arn': str(target_evidence['repository_arn']),
                'old_qualification_repository_generation':
                    owner_target.qualification_repository_generation,
                'new_qualification_repository_generation':
                    successor_target.qualification_repository_generation,
                'lifecycle_proof_id': str(mutation['lifecycle_proof_id']),
                'quarantine_reason': quarantine_reason,
            },
            expected_generation=current.desired_generation,
            expected_config_hash=current.config_hash,
            now=now)
        cleared = session.execute(schema.qualification_mutation.delete().where(
            schema.qualification_mutation.c.id == 'global',
            schema.qualification_mutation.c.state == 'QUARANTINED',
            schema.qualification_mutation.c.owner_profile_revision_id ==
            owner['id'],
            schema.qualification_mutation.c.owner_target == target_name,
            schema.qualification_mutation.c.repository_arn ==
            mutation['repository_arn'],
            schema.qualification_mutation.c.lifecycle_proof_id ==
            mutation['lifecycle_proof_id'])).rowcount
        if cleared != 1:
            raise RuntimeError(
                'Qualification quarantine cutover barrier CAS drifted.')
        return updated


def get_profile_revision(
        profile_revision_id: str) -> ProfileRevisionRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.profile_revisions).where(
                schema.profile_revisions.c.id ==
                profile_revision_id)).mappings().first()
    return _profile(row) if row is not None else None


def get_active_profile(workspace: str,
                       profile: str) -> ProfileRevisionRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.profile_revisions).where(
                schema.profile_revisions.c.workspace == workspace,
                schema.profile_revisions.c.profile == profile,
                schema.profile_revisions.c.state ==
                models.ImageProfileState.ACTIVE.value)).mappings().first()
    return _profile(row) if row is not None else None


def get_desired_profile(workspace: str,
                        profile: str) -> ProfileRevisionRecord | None:
    """Returns the single still-qualifying desired revision, if present."""
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.profile_revisions).where(
                schema.profile_revisions.c.workspace == workspace,
                schema.profile_revisions.c.profile == profile,
                schema.profile_revisions.c.state ==
                models.ImageProfileState.QUALIFYING.value)).mappings().first()
    return _profile(row) if row is not None else None


def list_active_profile_revisions(
    workspace: str,
    profile_names: tuple[str, ...],
) -> list[ProfileRevisionRecord]:
    """Returns ACTIVE revisions for one bounded configured profile set."""
    if (len(profile_names) > 128 or any(
            not isinstance(name, str) or not name for name in profile_names) or
            len(set(profile_names)) != len(profile_names)):
        raise ValueError('Active profile lookup is invalid.')
    if not profile_names:
        return []
    table = schema.profile_revisions
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.workspace == workspace,
                table.c.profile.in_(profile_names), table.c.state ==
                models.ImageProfileState.ACTIVE.value).order_by(
                    table.c.profile,
                    table.c.id).limit(len(profile_names))).mappings().all()
    return [_profile(row) for row in rows]


def list_qualifying_profiles(*,
                             include_active: bool = False,
                             limit: int = 100) -> list[ProfileRevisionRecord]:
    """Returns a bounded fair qualification work page.

    A desired revision exclusively owns qualification for its logical profile.
    The preceding ACTIVE revision becomes eligible for maintenance again only
    after that candidate leaves QUALIFYING.  This prevents workers from
    mutating a superseded qualification repository while its replacement is
    being proven. QUALIFYING work is queried first and returned without filling
    from ACTIVE maintenance, so a large tombstoned ACTIVE population cannot
    delay a fresh candidate.
    """
    if not 1 <= limit <= 1000:
        raise ValueError('Qualifying profile page size is invalid.')
    with orm.Session(catalog_state.engine()) as session:
        table = schema.profile_revisions

        def _tombstoned_repository() -> sqlalchemy.ColumnElement[bool]:
            attestation = sqlalchemy.func.jsonb_each(
                sqlalchemy.cast(table.c.attestations_json,
                                postgresql.JSONB)).table_valued(
                                    'key',
                                    'value').alias('qualification_attestation')
            return sqlalchemy.exists(
                sqlalchemy.select(sqlalchemy.literal(1)).select_from(
                    attestation.join(
                        schema.qualification_repository_quarantines, schema.
                        qualification_repository_quarantines.c.repository_arn ==
                        attestation.c.value.op('->>')('repository_arn'))).where(
                            attestation.c.key.like('terraform_target:%')))

        qualifying_rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.state == models.ImageProfileState.QUALIFYING.value,
                ~_tombstoned_repository()).order_by(
                    table.c.updated_at,
                    table.c.id).limit(limit)).mappings().all()
        if qualifying_rows or not include_active:
            return [_profile(row) for row in qualifying_rows]

        candidate = table.alias('qualification_candidate')
        candidate_exists = sqlalchemy.exists(
            sqlalchemy.select(sqlalchemy.literal(1)).where(
                candidate.c.workspace == table.c.workspace,
                candidate.c.profile == table.c.profile,
                candidate.c.state == models.ImageProfileState.QUALIFYING.value))
        active_rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.state == models.ImageProfileState.ACTIVE.value,
                ~candidate_exists, ~_tombstoned_repository()).order_by(
                    table.c.updated_at,
                    table.c.id).limit(limit)).mappings().all()
        return [_profile(row) for row in active_rows]


def list_profile_revisions(
        workspace: str,
        *,
        profile: str | None = None,
        limit: int | None = None) -> list[ProfileRevisionRecord]:
    statement = sqlalchemy.select(schema.profile_revisions).where(
        schema.profile_revisions.c.workspace == workspace).order_by(
            schema.profile_revisions.c.profile,
            schema.profile_revisions.c.desired_generation.desc())
    if profile is not None:
        statement = statement.where(
            schema.profile_revisions.c.profile == profile)
    if limit is not None:
        if not 1 <= limit <= 1001:
            raise ValueError('Profile revision page size is invalid.')
        statement = statement.limit(limit)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_profile(row) for row in rows]


def list_operational_profile_revisions(
    workspace: str,
    *,
    limit: int = 1001,
) -> list[ProfileRevisionRecord]:
    """Lists only current ACTIVE and QUALIFYING revisions for readiness.

    Separate UNION branches let PostgreSQL use the partial unique index for
    each operational state. Historical revisions cannot consume this bounded
    readiness window.
    """
    if not 1 <= limit <= 1001:
        raise ValueError('Operational profile revision page size is invalid.')
    table = schema.profile_revisions
    active = sqlalchemy.select(table).where(
        table.c.workspace == workspace,
        table.c.state == models.ImageProfileState.ACTIVE.value)
    qualifying = sqlalchemy.select(table).where(
        table.c.workspace == workspace,
        table.c.state == models.ImageProfileState.QUALIFYING.value)
    operational = active.union_all(qualifying).subquery()
    statement = sqlalchemy.select(operational).order_by(
        operational.c.profile, operational.c.state,
        operational.c.id).limit(limit)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_profile(row) for row in rows]


def list_profile_revision_history(
    workspace: str,
    *,
    limit: int = 50,
    after: tuple[int, str] | None = None,
) -> list[ProfileRevisionRecord]:
    """Returns one newest-first keyset page of durable profile history."""
    if not 1 <= limit <= 101:
        raise ValueError('Internal profile history page size is invalid.')
    table = schema.profile_revisions
    statement = sqlalchemy.select(table).where(table.c.workspace == workspace)
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(table.c.created_at, table.c.id) < after)
    statement = statement.order_by(table.c.created_at.desc(),
                                   table.c.id.desc()).limit(limit)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_profile(row) for row in rows]


def record_profile_attestation(*,
                               profile_revision_id: str,
                               kind: str,
                               evidence: dict[str, Any],
                               expected_generation: int,
                               expected_config_hash: str,
                               terraform_hash: str | None = None,
                               now: int | None = None) -> ProfileRevisionRecord:
    """Merges one independently authenticated, secret-free attestation."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        return record_profile_attestation_in_session(
            session,
            profile_revision_id=profile_revision_id,
            kind=kind,
            evidence=evidence,
            expected_generation=expected_generation,
            expected_config_hash=expected_config_hash,
            terraform_hash=terraform_hash,
            now=now)


def record_profile_attestation_in_session(
        session: orm.Session,
        *,
        profile_revision_id: str,
        kind: str,
        evidence: dict[str, Any],
        expected_generation: int,
        expected_config_hash: str,
        terraform_hash: str | None = None,
        now: int | None = None) -> ProfileRevisionRecord:
    """Transaction-scoped variant for operation and attestation atomicity."""
    if not isinstance(kind, str) or not kind or len(kind) > 128:
        raise ValueError('Attestation kind must be a bounded identifier.')
    authoritative_evidence = dict(evidence)
    encoded_evidence = json.dumps(authoritative_evidence,
                                  sort_keys=True,
                                  separators=(',', ':'))
    if len(encoded_evidence.encode()) > 16 * 1024:
        raise ValueError('Profile attestation exceeds 16 KiB.')
    table = schema.profile_revisions
    row = lock_profile_revision_mutation_in_session(session,
                                                    profile_revision_id)
    current = catalog_state.database_epoch(session, now=now)
    if (str(row['state']) not in (models.ImageProfileState.QUALIFYING.value,
                                  models.ImageProfileState.ACTIVE.value) or
            int(row['desired_generation']) != expected_generation or
            str(row['config_hash']) != expected_config_hash):
        raise StaleProfileRevisionError(
            'Attestation no longer matches the desired profile revision.')
    authoritative_evidence['observed_at'] = current
    encoded_evidence = json.dumps(authoritative_evidence,
                                  sort_keys=True,
                                  separators=(',', ':'))
    if len(encoded_evidence.encode()) > 16 * 1024:
        raise ValueError('Profile attestation exceeds 16 KiB.')
    attestations = json.loads(str(row['attestations_json']))
    attestations[kind] = authoritative_evidence
    encoded = json.dumps(attestations, sort_keys=True, separators=(',', ':'))
    if len(encoded.encode()) > 256 * 1024:
        raise ValueError('Profile attestation set exceeds 256 KiB.')
    attestation_hash = hashlib.sha256(encoded.encode()).hexdigest()
    values: dict[str, Any] = {
        'attestations_json': encoded,
        'attestations_hash': attestation_hash,
        'updated_at': current,
    }
    if terraform_hash is not None:
        if row['terraform_hash'] not in (None, terraform_hash):
            raise ValueError('Terraform qualification hash is immutable.')
        values['terraform_hash'] = terraform_hash
    updated = session.execute(
        table.update().where(table.c.id == profile_revision_id).values(
            **values).returning(table)).mappings().one()
    return _profile(updated)


def record_candidate_shard_attestation(
        *,
        profile_revision_id: str,
        expected_generation: int,
        expected_config_hash: str,
        shard_id: str,
        expected_operational_revision_id: str,
        expected_target_fingerprint: str,
        expected_physical_fingerprint: str,
        expected_inventory_epoch: int,
        expected_inventory_completed_at: int,
        kind: str,
        evidence: dict[str, Any],
        now: int | None = None) -> ProfileRevisionRecord | None:
    """Records candidate proof only against one unchanged operational epoch."""
    profiles = schema.profile_revisions
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        candidate = lock_profile_revision_mutation_in_session(
            session, profile_revision_id)
        if (str(candidate['state']) != models.ImageProfileState.QUALIFYING.value
                or
                int(candidate['desired_generation']) != expected_generation or
                str(candidate['config_hash']) != expected_config_hash):
            raise StaleProfileRevisionError(
                'Attestation no longer matches the desired profile revision.')
        shard = session.execute(
            sqlalchemy.select(shards).where(
                shards.c.id == shard_id).with_for_update()).mappings().first()
        if (shard is None or shard['workspace'] != candidate['workspace'] or
                shard['profile'] != candidate['profile'] or
                shard['profile_revision_id'] != expected_operational_revision_id
                or shard['target_fingerprint'] != expected_target_fingerprint or
                shard['physical_fingerprint'] != expected_physical_fingerprint
                or str(shard['state'])
                not in (models.ImageShardState.READY.value,
                        models.ImageShardState.FULL.value) or
                int(shard['inventory_epoch']) != expected_inventory_epoch or
                shard['inventory_completed_at']
                != expected_inventory_completed_at or
                bool(shard['inventory_finalizing']) or
                shard['inventory_lease_token'] is not None):
            return None
        operational_state = session.execute(
            sqlalchemy.select(profiles.c.state).where(
                profiles.c.id == expected_operational_revision_id)).scalar()
        if operational_state != models.ImageProfileState.ACTIVE.value:
            return None
        # The profile advisory lock and candidate row are already held. The
        # shared helper's repeated lock calls are reentrant in this transaction
        # and preserve profile-before-shard ordering for every competitor.
        return record_profile_attestation_in_session(
            session,
            profile_revision_id=profile_revision_id,
            kind=kind,
            evidence=evidence,
            expected_generation=expected_generation,
            expected_config_hash=expected_config_hash,
            now=now)


def reserve_canary_cost(
    session: orm.Session,
    *,
    profile_revision_id: str,
    expected_generation: int,
    worst_case_microusd: int,
    admission_check: Callable[[ProfileRevisionRecord], bool] | None = None,
    now: int | None = None,
) -> tuple[ProfileRevisionRecord, int]:
    """Hard-reserves one canary's worst-case daily cost before launch."""
    table = schema.profile_revisions
    row = lock_profile_revision_mutation_in_session(session,
                                                    profile_revision_id)
    current = catalog_state.database_epoch(session, now=now)
    utc_day = time.strftime('%Y-%m-%d', time.gmtime(current))
    if (str(row['state']) not in (models.ImageProfileState.QUALIFYING.value,
                                  models.ImageProfileState.ACTIVE.value) or
            int(row['desired_generation']) != expected_generation):
        raise StaleProfileRevisionError(
            'Canary no longer matches the desired profile revision.')
    revision = _profile(row)
    if admission_check is not None and not admission_check(revision):
        raise StaleProfileRevisionError(
            'Canary qualification copy is not available.')
    reserved = (0 if row['canary_window_day'] != utc_day else int(
        row['canary_reserved_microusd']))
    if (worst_case_microusd < 0 or reserved + worst_case_microusd > int(
            row['max_daily_canary_microusd'])):
        raise ValueError('CANARY_DAILY_COST_LIMIT')
    updated = session.execute(
        table.update().where(table.c.id == profile_revision_id).values(
            canary_window_day=utc_day,
            canary_reserved_microusd=reserved + worst_case_microusd,
            updated_at=current).returning(table)).mappings().one()
    return _profile(updated), current


def upsert_qualified_shard(session: orm.Session, *, workspace: str,
                           profile: str, target_id: str, provider: str,
                           partition: str, account: str, region: str,
                           shard_generation: int, shard_index: int,
                           target_fingerprint: str, physical_fingerprint: str,
                           registry: str, repository_name: str,
                           repository_arn: str, max_manifests: int,
                           max_declared_bytes: int, max_in_flight: int,
                           now: int) -> ShardRecord:
    """Creates one bootstrap shard or validates an operational physical slot."""
    table = schema.registry_shards
    identity = (table.c.workspace == workspace, table.c.profile == profile,
                table.c.target_id == target_id,
                table.c.shard_generation == shard_generation,
                table.c.shard_index == shard_index)
    row = session.execute(
        sqlalchemy.select(table).where(
            *identity).with_for_update()).mappings().first()
    if row is None:
        row = session.execute(table.insert().values(
            id=str(uuid.uuid4()),
            workspace=workspace,
            profile=profile,
            target_id=target_id,
            provider=provider,
            partition=partition,
            account=account,
            region=region,
            shard_generation=shard_generation,
            shard_index=shard_index,
            target_fingerprint=target_fingerprint,
            physical_fingerprint=physical_fingerprint,
            eviction_enabled=False,
            registry=registry,
            repository_name=repository_name,
            repository_arn=repository_arn,
            max_manifests=max_manifests,
            max_declared_bytes=max_declared_bytes,
            max_in_flight=max_in_flight,
            state=models.ImageShardState.PENDING.value,
            created_at=now,
            updated_at=now).returning(table)).mappings().one()
        return _shard(row)
    immutable = {
        'provider': provider,
        'partition': partition,
        'account': account,
        'region': region,
        'target_fingerprint': target_fingerprint,
        'physical_fingerprint': physical_fingerprint,
        'registry': registry,
        'repository_name': repository_name,
        'repository_arn': repository_arn,
    }
    if any(str(row[key]) != str(value) for key, value in immutable.items()):
        raise ValueError('Qualified registry shard physical identity drifted.')
    if (max_manifests < int(row['reserved_manifests']) or
            max_declared_bytes < int(row['reserved_declared_bytes']) or
            max_in_flight < int(row['in_flight'])):
        raise ValueError(
            'Registry shard ceilings cannot fall below reservations.')
    # Once activation owns a physical slot, a later Terraform handoff is only
    # candidate evidence. Its operational ceilings and inventory epoch change
    # in the fenced activation transaction, never during qualification.
    if row['profile_revision_id'] is not None:
        return _shard(row)
    capacities_changed = (int(row['max_manifests']) != max_manifests or int(
        row['max_declared_bytes']) != max_declared_bytes or
                          int(row['max_in_flight']) != max_in_flight)
    if not capacities_changed:
        return _shard(row)
    if row['inventory_lease_token'] is not None or _inventory_active(row):
        raise ValueError(
            'Bootstrap shard inventory must finish before limits change.')
    values: dict[str, Any] = {
        'max_manifests': max_manifests,
        'max_declared_bytes': max_declared_bytes,
        'max_in_flight': max_in_flight,
        'updated_at': now,
        'inventory_cursor': None,
        'inventory_started_at': None,
        'inventory_completed_at': None,
        'inventory_finalizing': False,
        'inventory_next_at': now,
        'observed_manifests': 0,
    }
    if str(row['state']) in (models.ImageShardState.READY.value,
                             models.ImageShardState.FULL.value):
        values.update(state=models.ImageShardState.PENDING.value,
                      qualified_at=None)
    updated = session.execute(
        table.update().where(table.c.id == row['id']).values(
            **values).returning(table)).mappings().one()
    return _shard(updated)


def lock_profile_shards(session: orm.Session, *, workspace: str,
                        profile: str) -> None:
    """Locks an existing physical ring in the global shard-ID order."""
    table = schema.registry_shards
    session.execute(
        sqlalchemy.select(
            table.c.id).where(table.c.workspace == workspace,
                              table.c.profile == profile).order_by(
                                  table.c.id).with_for_update()).all()


def list_shards(workspace: str,
                profile: str | None = None,
                *,
                limit: int | None = None) -> list[ShardRecord]:
    statement = sqlalchemy.select(schema.registry_shards).where(
        schema.registry_shards.c.workspace == workspace)
    if profile is not None:
        statement = statement.where(schema.registry_shards.c.profile == profile)
    statement = statement.order_by(schema.registry_shards.c.profile,
                                   schema.registry_shards.c.target_id,
                                   schema.registry_shards.c.shard_index)
    if limit is not None:
        if not 1 <= limit <= 1001:
            raise ValueError('Registry shard page size is invalid.')
        statement = statement.limit(limit)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_shard(row) for row in rows]


def get_target_shard(workspace: str, profile: str,
                     target_id: str) -> ShardRecord | None:
    """Returns one deterministic shard for target-scoped provider budgeting."""
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.registry_shards).where(
                schema.registry_shards.c.workspace == workspace,
                schema.registry_shards.c.profile == profile,
                schema.registry_shards.c.target_id == target_id).order_by(
                    schema.registry_shards.c.shard_generation,
                    schema.registry_shards.c.shard_index).limit(
                        1)).mappings().first()
    return _shard(row) if row is not None else None


def list_target_shards(workspace: str, profile: str,
                       target_id: str) -> list[ShardRecord]:
    """Returns one bounded physical target ring in deterministic order."""
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(schema.registry_shards).where(
                schema.registry_shards.c.workspace == workspace,
                schema.registry_shards.c.profile == profile,
                schema.registry_shards.c.target_id == target_id).order_by(
                    schema.registry_shards.c.shard_generation,
                    schema.registry_shards.c.shard_index).limit(
                        257)).mappings().all()
    if len(rows) > 256:
        raise ValueError('Registry target contains too many physical shards.')
    return [_shard(row) for row in rows]


def claim_inventory_shard(*,
                          worker_id: str,
                          lease_seconds: int,
                          interval_seconds: int = 10 * 60,
                          now: int | None = None) -> ShardRecord | None:
    """Claims one resumable repository inventory epoch without provider I/O."""
    if lease_seconds <= 0 or interval_seconds <= 0:
        raise ValueError('Inventory lease and interval must be positive.')
    table = schema.registry_shards
    token = f'{worker_id}:{uuid.uuid4()}'
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
        clock = catalog_state.database_epoch_expression(now=now)
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.state.in_([
                    models.ImageShardState.PENDING.value,
                    models.ImageShardState.READY.value,
                    models.ImageShardState.FULL.value,
                    models.ImageShardState.DRIFTED.value,
                ]), table.c.inventory_next_at
                <= clock).order_by(table.c.inventory_finalizing.desc(),
                                   table.c.inventory_next_at,
                                   table.c.id).limit(1).with_for_update(
                                       skip_locked=True)).mappings().first()
        if row is None:
            return None
        in_progress = (row['inventory_started_at'] is not None and
                       (row['inventory_completed_at'] is None or
                        bool(row['inventory_finalizing'])))
        values: dict[str, Any] = {
            'inventory_lease_token': token,
            'inventory_lease_expires_at': current + lease_seconds,
            'inventory_interval_seconds': interval_seconds,
            'inventory_next_at': current + lease_seconds,
            'updated_at': current,
        }
        if not in_progress:
            values.update(inventory_epoch=int(row['inventory_epoch']) + 1,
                          inventory_cursor=None,
                          inventory_started_at=current,
                          inventory_completed_at=None,
                          inventory_finalizing=False,
                          observed_manifests=0)
        updated = session.execute(
            table.update().where(table.c.id == row['id']).values(
                **values).returning(table)).mappings().one()
        return _shard(updated)


def heartbeat_inventory_shard(shard_id: str,
                              lease_token: str,
                              lease_seconds: int,
                              *,
                              now: int | None = None) -> bool:
    """Renews one exact inventory authority, including its final readback."""
    if lease_seconds <= 0:
        return False
    table = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(table.update().where(
            table.c.id == shard_id,
            table.c.inventory_lease_token == lease_token,
            table.c.inventory_lease_expires_at.is_not(None),
            table.c.inventory_lease_expires_at
            > clock).values(inventory_lease_expires_at=clock + lease_seconds,
                            inventory_next_at=clock + lease_seconds,
                            updated_at=clock)).rowcount
    return changed == 1


def record_inventory_page(shard_id: str,
                          lease_token: str,
                          digests: tuple[str, ...],
                          next_cursor: str | None,
                          *,
                          now: int | None = None) -> ShardRecord | None:
    """Commits one bounded provider page and its durable continuation cursor."""
    if len(digests) > 1000 or len(digests) != len(set(digests)):
        raise ValueError('Inventory page must contain at most 1000 digests.')
    normalized = tuple(
        models.validate_sha256_digest(digest, 'Inventory digest')
        for digest in digests)
    if next_cursor is not None and (not isinstance(next_cursor, str) or
                                    not next_cursor or
                                    len(next_cursor.encode()) > 8192):
        raise ValueError('Inventory continuation cursor is invalid.')
    shards = schema.registry_shards
    locations = schema.locations
    try:
        with orm.Session(catalog_state.engine()) as session, session.begin():
            shard = session.execute(
                sqlalchemy.select(shards).where(shards.c.id == shard_id).
                with_for_update()).mappings().first()
            if (shard is None or
                    shard['inventory_lease_token'] != lease_token or
                    shard['inventory_lease_expires_at'] is None or
                    shard['inventory_started_at'] is None or
                    shard['inventory_completed_at'] is not None or
                    bool(shard['inventory_finalizing'])):
                return None
            epoch = int(shard['inventory_epoch'])
            known = 0
            if normalized:
                known = session.execute(
                    sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                                     ).select_from(locations).where(
                                         locations.c.shard_id == shard_id,
                                         locations.c.runtime_digest.in_(
                                             normalized))).scalar_one()
            observed = int(shard['observed_manifests']) + len(normalized)
            if known != len(normalized):
                # Persist epoch-local unexplained content across continuation
                # pages without adding another ledger field. A clean new epoch
                # resets the counter and can recover the shard.
                observed = max(observed, int(shard['reserved_manifests']) + 1)
            drifted = observed > int(shard['reserved_manifests'])
            clock = catalog_state.database_epoch_expression(now=now)
            values: dict[str, Any] = {
                'observed_manifests': observed,
                'inventory_cursor': next_cursor,
                'updated_at': clock,
            }
            if next_cursor is None:
                if drifted:
                    state = models.ImageShardState.DRIFTED.value
                elif (int(shard['reserved_manifests']) >= int(
                        shard['max_manifests']) or
                      int(shard['reserved_declared_bytes']) >= int(
                          shard['max_declared_bytes'])):
                    state = models.ImageShardState.FULL.value
                else:
                    state = models.ImageShardState.READY.value
                values.update(
                    inventory_completed_at=clock,
                    inventory_finalizing=(
                        state in (models.ImageShardState.READY.value,
                                  models.ImageShardState.FULL.value)),
                    state=state,
                    qualified_at=(clock if state
                                  in (models.ImageShardState.READY.value,
                                      models.ImageShardState.FULL.value) else
                                  shard['qualified_at']))
            live_inventory = sqlalchemy.exists().where(
                shards.c.id == shard_id, shards.c.inventory_epoch == epoch,
                shards.c.inventory_lease_token == lease_token,
                shards.c.inventory_lease_expires_at.is_not(None),
                shards.c.inventory_lease_expires_at > clock,
                shards.c.inventory_started_at.is_not(None),
                shards.c.inventory_completed_at.is_(None),
                shards.c.inventory_finalizing.is_(False))
            if normalized:
                session.execute(locations.update().where(
                    locations.c.shard_id == shard_id,
                    locations.c.runtime_digest.in_(normalized),
                    live_inventory).values(inventory_epoch_seen=epoch,
                                           updated_at=clock))
            updated = session.execute(shards.update().where(
                shards.c.id == shard_id, shards.c.inventory_epoch == epoch,
                shards.c.inventory_lease_token == lease_token,
                shards.c.inventory_lease_expires_at.is_not(None),
                shards.c.inventory_lease_expires_at > clock,
                shards.c.inventory_started_at.is_not(None),
                shards.c.inventory_completed_at.is_(None),
                shards.c.inventory_finalizing.is_(False)).values(
                    **values).returning(shards)).mappings().first()
            if updated is None:
                raise _InventoryLeaseFenceLost
            refresh_time = catalog_state.database_epoch(session, now=now)
            _refresh_shard_copy_queue_in_session(session,
                                                 shard_id,
                                                 now=refresh_time)
            completion_matches = [
                shards.c.inventory_completed_at.is_(None)
                if updated['inventory_completed_at'] is None else
                shards.c.inventory_completed_at
                == updated['inventory_completed_at']
            ]
            final_fence = session.execute(shards.update().where(
                shards.c.id == shard_id, shards.c.inventory_epoch == epoch,
                shards.c.inventory_lease_token == lease_token,
                shards.c.inventory_lease_expires_at.is_not(None),
                shards.c.inventory_lease_expires_at
                > catalog_state.database_epoch_expression(now=now),
                shards.c.inventory_started_at ==
                updated['inventory_started_at'],
                shards.c.inventory_finalizing == bool(
                    updated['inventory_finalizing']),
                *completion_matches).values(
                    updated_at=shards.c.updated_at)).rowcount
            if final_fence != 1:
                raise _InventoryLeaseFenceLost
            return _shard(updated)
    except _InventoryLeaseFenceLost:
        return None


def release_inventory_claim(shard_id: str,
                            lease_token: str,
                            expected_inventory_epoch: int,
                            *,
                            now: int | None = None) -> bool:
    """Releases one successfully persisted continuation or drift result."""
    table = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.id == shard_id).with_for_update()).mappings().first()
        if (row is None or
                int(row['inventory_epoch']) != expected_inventory_epoch or
                row['inventory_lease_token'] != lease_token or
                row['inventory_lease_expires_at'] is None):
            return False
        current = catalog_state.database_epoch(session, now=now)
        if (row['inventory_completed_at'] is not None and
                not bool(row['inventory_finalizing'])):
            next_at = (int(row['inventory_completed_at']) +
                       int(row['inventory_interval_seconds']))
        else:
            next_at = current
        changed = session.execute(table.update().where(
            table.c.id == shard_id,
            table.c.inventory_epoch == expected_inventory_epoch,
            table.c.inventory_lease_token == lease_token,
            table.c.inventory_lease_expires_at.is_not(None),
            table.c.inventory_lease_expires_at
            > catalog_state.database_epoch_expression(now=now)).values(
                inventory_lease_token=None,
                inventory_lease_expires_at=None,
                inventory_next_at=next_at,
                updated_at=current)).rowcount
    return changed == 1


def abandon_inventory_claim(shard_id: str,
                            lease_token: str,
                            expected_inventory_epoch: int,
                            *,
                            invalid_cursor: bool = False,
                            now: int | None = None) -> bool:
    """Releases a failed claim, restarting only an invalid provider cursor."""
    table = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.id == shard_id).with_for_update()).mappings().first()
        if (row is None or
                int(row['inventory_epoch']) != expected_inventory_epoch or
                row['inventory_lease_token'] != lease_token or
                row['inventory_lease_expires_at'] is None or
                row['inventory_started_at'] is None):
            return False
        current = catalog_state.database_epoch(session, now=now)
        if int(row['inventory_lease_expires_at']) <= current:
            return False
        values: dict[str, Any] = {
            'inventory_lease_token': None,
            'inventory_lease_expires_at': None,
            'inventory_next_at': current,
            'updated_at': current,
        }
        if invalid_cursor:
            values.update(inventory_cursor=None,
                          inventory_started_at=None,
                          inventory_completed_at=None,
                          inventory_finalizing=False,
                          observed_manifests=0)
        changed = session.execute(table.update().where(
            table.c.id == shard_id,
            table.c.inventory_epoch == expected_inventory_epoch,
            table.c.inventory_lease_token == lease_token,
            table.c.inventory_lease_expires_at.is_not(None),
            table.c.inventory_lease_expires_at
            > catalog_state.database_epoch_expression(now=now),
            table.c.inventory_started_at.is_not(None)).values(
                **values)).rowcount
    return changed == 1


def mark_shard_drifted(shard_id: str,
                       lease_token: str,
                       *,
                       now: int | None = None) -> bool:
    """Fails closed on live infrastructure mismatch while releasing its lease."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard_id,
            schema.registry_shards.c.inventory_lease_token == lease_token,
            schema.registry_shards.c.inventory_lease_expires_at.is_not(None),
            schema.registry_shards.c.inventory_lease_expires_at
            > clock).values(state=models.ImageShardState.DRIFTED.value,
                            inventory_lease_token=None,
                            inventory_lease_expires_at=None,
                            inventory_cursor=None,
                            inventory_started_at=None,
                            inventory_completed_at=None,
                            inventory_finalizing=False,
                            inventory_next_at=clock,
                            observed_manifests=0,
                            updated_at=clock)).rowcount
        if changed == 1:
            current = catalog_state.database_epoch(session, now=now)
            _refresh_shard_copy_queue_in_session(session, shard_id, now=current)
    return changed == 1


def _inventory_missing_candidate_conditions(
    shard_id: Any,
    inventory_epoch: Any,
    inventory_started_at: Any,
) -> tuple[Any, ...]:
    locations = schema.locations
    return (
        locations.c.shard_id == shard_id,
        locations.c.state == models.ImageLocationState.READY.value,
        locations.c.last_verified_at.is_not(None),
        locations.c.last_verified_at < inventory_started_at,
        sqlalchemy.or_(locations.c.inventory_epoch_seen.is_(None),
                       locations.c.inventory_epoch_seen < inventory_epoch),
    )


def list_inventory_missing_candidates(shard_id: str,
                                      inventory_epoch: int,
                                      *,
                                      limit: int = 100) -> list[LocationRecord]:
    """Returns old READY rows absent from one completed inventory epoch."""
    if not 1 <= limit <= 1000:
        raise ValueError('Inventory confirmation page size is invalid.')
    shards = schema.registry_shards
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(locations).join(
                shards, shards.c.id == locations.c.shard_id).where(
                    shards.c.id == shard_id,
                    shards.c.inventory_epoch == inventory_epoch,
                    shards.c.inventory_completed_at.is_not(None),
                    shards.c.inventory_finalizing.is_(True),
                    *_inventory_missing_candidate_conditions(
                        shards.c.id, shards.c.inventory_epoch,
                        shards.c.inventory_started_at)).order_by(
                            locations.c.last_verified_at,
                            locations.c.id).limit(limit)).mappings().all()
    return [_location(row) for row in rows]


def complete_inventory_confirmation(
        location_id: str,
        shard_id: str,
        inventory_epoch: int,
        inventory_lease_token: str,
        *,
        present: bool,
        now: int | None = None) -> LocationRecord | None:
    """Records an exact digest read under the completed epoch's live lease."""
    shards = schema.registry_shards
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        shard = session.execute(
            sqlalchemy.select(shards).where(
                shards.c.id == shard_id).with_for_update()).mappings().first()
        if (shard is None or int(shard['inventory_epoch']) != inventory_epoch or
                shard['inventory_completed_at'] is None or
                not bool(shard['inventory_finalizing']) or
                shard['inventory_started_at'] is None or
                shard['inventory_lease_token'] != inventory_lease_token or
                shard['inventory_lease_expires_at'] is None):
            return None
        row = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.id == location_id, locations.c.shard_id ==
                shard_id).with_for_update()).mappings().first()
        if (row is None or
                str(row['state']) != models.ImageLocationState.READY.value or
                row['last_verified_at'] is None or int(row['last_verified_at'])
                >= int(shard['inventory_started_at']) or
            (row['inventory_epoch_seen'] is not None and
             int(row['inventory_epoch_seen']) >= inventory_epoch)):
            return _location(row) if row is not None else None
        current = catalog_state.database_epoch(session, now=now)
        clock = catalog_state.database_epoch_expression(now=now)
        live_inventory = sqlalchemy.exists().where(
            shards.c.id == shard_id,
            shards.c.inventory_epoch == inventory_epoch,
            shards.c.inventory_completed_at.is_not(None),
            shards.c.inventory_finalizing.is_(True),
            shards.c.inventory_started_at.is_not(None),
            shards.c.inventory_lease_token == inventory_lease_token,
            shards.c.inventory_lease_expires_at.is_not(None),
            shards.c.inventory_lease_expires_at > clock)
        updated = session.execute(locations.update().where(
            locations.c.id == location_id, locations.c.shard_id == shard_id,
            locations.c.state == models.ImageLocationState.READY.value,
            locations.c.last_verified_at.is_not(None),
            locations.c.last_verified_at < int(shard['inventory_started_at']),
            sqlalchemy.or_(locations.c.inventory_epoch_seen.is_(None),
                           locations.c.inventory_epoch_seen < inventory_epoch),
            live_inventory).values(
                state=(models.ImageLocationState.READY.value
                       if present else models.ImageLocationState.MISSING.value),
                inventory_epoch_seen=(inventory_epoch if present else
                                      row['inventory_epoch_seen']),
                last_verified_at=(current
                                  if present else row['last_verified_at']),
                error_code=(None if present else
                            models.ImageLocationErrorCode.MANIFEST_MISSING.value
                           ),
                updated_at=current).returning(locations)).mappings().first()
        return _location(updated) if updated is not None else None


def record_inventory_attestation_and_release(
    *,
    profile_revision_id: str,
    expected_generation: int,
    expected_config_hash: str,
    shard_id: str,
    inventory_lease_token: str,
    expected_profile_revision_id: str | None,
    expected_target_fingerprint: str,
    expected_physical_fingerprint: str,
    expected_inventory_epoch: int,
    expected_inventory_completed_at: int,
    kind: str,
    evidence: dict[str, Any],
    now: int | None = None,
) -> ProfileRevisionRecord | None:
    """Publishes evidence only after finalization, then releases atomically."""
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        try:
            revision = lock_profile_revision_mutation_in_session(
                session, profile_revision_id)
        except StaleProfileRevisionError:
            return None
        shard = session.execute(
            sqlalchemy.select(shards).where(
                shards.c.id == shard_id).with_for_update()).mappings().first()
        if (shard is None or shard['workspace'] != revision['workspace'] or
                shard['profile'] != revision['profile'] or
                shard['profile_revision_id'] != expected_profile_revision_id or
                shard['target_fingerprint'] != expected_target_fingerprint or
                shard['physical_fingerprint'] != expected_physical_fingerprint
                or str(shard['state'])
                not in (models.ImageShardState.READY.value,
                        models.ImageShardState.FULL.value) or
                int(shard['inventory_epoch']) != expected_inventory_epoch or
                shard['inventory_completed_at']
                != expected_inventory_completed_at or
                not bool(shard['inventory_finalizing']) or
                shard['inventory_started_at'] is None or
                shard['inventory_lease_token'] != inventory_lease_token or
                shard['inventory_lease_expires_at'] is None):
            return None
        has_candidates = session.execute(
            sqlalchemy.select(sqlalchemy.exists().where(
                *_inventory_missing_candidate_conditions(
                    shard_id, expected_inventory_epoch,
                    int(shard['inventory_started_at']))))).scalar_one()
        current = catalog_state.database_epoch(session, now=now)
        fence = (
            shards.c.id == shard_id,
            shards.c.workspace == revision['workspace'],
            shards.c.profile == revision['profile'],
            shards.c.profile_revision_id == expected_profile_revision_id,
            shards.c.target_fingerprint == expected_target_fingerprint,
            shards.c.physical_fingerprint == expected_physical_fingerprint,
            shards.c.state.in_([
                models.ImageShardState.READY.value,
                models.ImageShardState.FULL.value,
            ]),
            shards.c.inventory_epoch == expected_inventory_epoch,
            shards.c.inventory_completed_at == expected_inventory_completed_at,
            shards.c.inventory_finalizing.is_(True),
            shards.c.inventory_started_at.is_not(None),
            shards.c.inventory_lease_token == inventory_lease_token,
            shards.c.inventory_lease_expires_at.is_not(None),
            shards.c.inventory_lease_expires_at
            > catalog_state.database_epoch_expression(now=now),
        )
        if has_candidates:
            # Exact confirmation is intentionally paged. Releasing the token
            # while preserving inventory_finalizing makes the same completed
            # epoch immediately successor-claimable without relisting ECR.
            changed = session.execute(shards.update().where(*fence).values(
                inventory_lease_token=None,
                inventory_lease_expires_at=None,
                inventory_next_at=current,
                updated_at=current)).rowcount
            if changed != 1:
                return None
            return None
        changed = session.execute(shards.update().where(*fence).values(
            inventory_finalizing=False,
            inventory_lease_token=None,
            inventory_lease_expires_at=None,
            inventory_next_at=(expected_inventory_completed_at +
                               int(shard['inventory_interval_seconds'])),
            updated_at=current)).rowcount
        if changed != 1:
            return None
        recorded = record_profile_attestation_in_session(
            session,
            profile_revision_id=profile_revision_id,
            kind=kind,
            evidence=evidence,
            expected_generation=expected_generation,
            expected_config_hash=expected_config_hash,
            now=current)
        return recorded


def readiness_queue_stats(
    shard_records: list[ShardRecord],
    *,
    group_limit: int = 100,
) -> tuple[list[dict[str, Any]], bool]:
    """Returns one capped, fixed-query projection for selected shard groups."""
    if not 1 <= group_limit <= 100:
        raise ValueError('Readiness queue group limit must be 1 through 100.')
    result_cap = 10_000
    pending_states = (
        models.ImageLocationState.PENDING.value,
        models.ImageLocationState.COPYING.value,
        models.ImageLocationState.VERIFYING.value,
        models.ImageLocationState.MISSING.value,
        models.ImageLocationState.EVICTED.value,
    )
    grouped: dict[tuple[str, str, str, str], list[ShardRecord]] = {}
    for shard in shard_records:
        key = (shard.profile, shard.target_id, shard.account, shard.region)
        grouped.setdefault(key, []).append(shard)
    ordered_groups = sorted(grouped.items())
    truncated = len(ordered_groups) > group_limit
    selected_groups = ordered_groups[:group_limit]
    for _, items in selected_groups:
        if len(items) > 256:
            raise ValueError(
                'Registry target contains too many physical shards.')
    if not selected_groups:
        return [], truncated
    group_payload = [{
        'group_index': index,
        'shard_ids': [shard.id for shard in items],
    } for index, (_, items) in enumerate(selected_groups)]
    statement = sqlalchemy.text("""
        WITH selected_groups AS (
            SELECT group_index, shard_ids
            FROM jsonb_to_recordset(CAST(:groups AS jsonb))
                 AS selected(group_index integer, shard_ids text[])
        )
        SELECT selected.group_index,
               queued.result_count AS queued_count,
               failed.result_count AS failed_count,
               quarantined.result_count AS quarantined_count,
               quarantined.reserved_bytes AS quarantined_bytes,
               oldest.updated_at AS oldest_queued_at
        FROM selected_groups AS selected
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS result_count
            FROM (
                SELECT location.id
                FROM container_image_locations AS location
                WHERE location.shard_id = ANY(selected.shard_ids)
                  AND location.state = ANY(CAST(:pending_states AS text[]))
                LIMIT :result_limit
            ) AS bounded
        ) AS queued
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS result_count
            FROM (
                SELECT location.id
                FROM container_image_locations AS location
                WHERE location.shard_id = ANY(selected.shard_ids)
                  AND location.state = ANY(CAST(:failed_states AS text[]))
                LIMIT :result_limit
            ) AS bounded
        ) AS failed
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS result_count,
                   COALESCE(SUM(bounded.reserved_declared_bytes), 0)
                       AS reserved_bytes
            FROM (
                SELECT location.reserved_declared_bytes
                FROM container_image_locations AS location
                WHERE location.shard_id = ANY(selected.shard_ids)
                  AND location.state = 'QUARANTINED'
                LIMIT :result_limit
            ) AS bounded
        ) AS quarantined
        CROSS JOIN LATERAL (
            SELECT MIN(candidate.updated_at) AS updated_at
            FROM unnest(selected.shard_ids) AS shard(id)
            CROSS JOIN unnest(CAST(:pending_states AS text[]))
                AS state(value)
            CROSS JOIN LATERAL (
                SELECT location.updated_at
                FROM container_image_locations AS location
                WHERE location.shard_id = shard.id
                  AND location.state = state.value
                ORDER BY location.updated_at, location.id
                LIMIT 1
            ) AS candidate
        ) AS oldest
        ORDER BY selected.group_index
    """)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            statement, {
                'groups': json.dumps(group_payload),
                'pending_states': list(pending_states),
                'failed_states': [
                    models.ImageLocationState.FAILED.value,
                    models.ImageLocationState.QUARANTINED.value,
                ],
                'result_limit': result_cap + 1,
            }).mappings().all()
    stats = {int(row['group_index']): row for row in rows}
    results: list[dict[str, Any]] = []
    for index, (key, items) in enumerate(selected_groups):
        row = stats[index]
        queued_count = int(row['queued_count'])
        failed_count = int(row['failed_count'])
        quarantined_count = int(row['quarantined_count'])
        profile, target, account, region = key
        results.append({
            'profile': profile,
            'target': target,
            'account': account,
            'region': region,
            'reserved_manifests': sum(item.reserved_manifests for item in items
                                     ),
            'max_manifests': sum(item.max_manifests for item in items),
            'reserved_declared_bytes': sum(
                item.reserved_declared_bytes for item in items),
            'max_declared_bytes': sum(item.max_declared_bytes for item in items
                                     ),
            'in_flight': sum(item.in_flight for item in items),
            'max_in_flight': sum(item.max_in_flight for item in items),
            'queue_depth': min(queued_count, result_cap),
            'queue_depth_at_least': queued_count > result_cap,
            'failed_count': min(failed_count, result_cap),
            'failed_count_at_least': failed_count > result_cap,
            'quarantined_count': min(quarantined_count, result_cap),
            'quarantined_count_at_least': quarantined_count > result_cap,
            'quarantined_reserved_declared_bytes': int(row['quarantined_bytes']
                                                      ),
            'quarantined_reserved_declared_bytes_at_least': quarantined_count >
                                                            result_cap,
            'oldest_queued_at': (int(row['oldest_queued_at']) if
                                 row['oldest_queued_at'] is not None else None),
        })
    return results, truncated


def get_shard(shard_id: str) -> ShardRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.registry_shards).where(
                schema.registry_shards.c.id == shard_id)).mappings().first()
    return _shard(row) if row is not None else None


def get_shards(shard_ids: set[str]) -> dict[str, ShardRecord]:
    """Returns one bounded shard map without per-location database queries."""
    if not shard_ids:
        return {}
    if len(shard_ids) > 101:
        raise ValueError('At most 101 shard records may be loaded per page.')
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(schema.registry_shards).where(
                schema.registry_shards.c.id.in_(shard_ids))).mappings().all()
    return {str(row['id']): _shard(row) for row in rows}


def get_location(location_id: str) -> LocationRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.locations).where(
                schema.locations.c.id == location_id)).mappings().first()
    return _location(row) if row is not None else None


def get_location_for_target(*, image_id: str, workspace: str,
                            target_fingerprint: str,
                            runtime_digest: str) -> LocationRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.locations).where(
                schema.locations.c.image_id == image_id,
                schema.locations.c.workspace == workspace,
                schema.locations.c.target_fingerprint == target_fingerprint,
                schema.locations.c.runtime_digest ==
                runtime_digest)).mappings().first()
    return _location(row) if row is not None else None


def _refresh_shard_copy_queue_in_session(
        session: orm.Session,
        shard_id: str,
        *,
        now: int,
        rotate_due: bool = False) -> int | None:
    """Refreshes one exact shard-level projection under its row lock."""
    shards = schema.registry_shards
    locations = schema.locations
    shard = session.execute(
        sqlalchemy.select(shards).where(
            shards.c.id == shard_id).with_for_update()).mappings().one()
    candidates: list[int] = []
    recovery_at = session.execute(
        sqlalchemy.select(locations.c.copy_claimable_at).where(
            locations.c.shard_id == shard_id,
            locations.c.state.in_([
                models.ImageLocationState.COPYING.value,
                models.ImageLocationState.VERIFYING.value,
            ])).order_by(locations.c.copy_claimable_at,
                         locations.c.id).limit(1)).scalar_one_or_none()
    if recovery_at is not None:
        candidates.append(int(recovery_at))
    if (str(shard['state']) in (models.ImageShardState.READY.value,
                                models.ImageShardState.FULL.value) and
            int(shard['in_flight']) < int(shard['max_in_flight'])):
        fresh_at = session.execute(
            sqlalchemy.select(locations.c.copy_claimable_at).where(
                locations.c.shard_id == shard_id, locations.c.state ==
                models.ImageLocationState.PENDING.value).order_by(
                    locations.c.copy_claimable_at,
                    locations.c.id).limit(1)).scalar_one_or_none()
        if fresh_at is not None:
            candidates.append(int(fresh_at))
    next_at = min(candidates) if candidates else None
    if next_at is not None:
        dispatch_floor = int(shard['last_dispatch_at'] or 0)
        if rotate_due and next_at <= now:
            dispatch_floor = max(dispatch_floor, now)
        next_at = max(next_at, dispatch_floor)
    session.execute(shards.update().where(shards.c.id == shard_id).values(
        copy_next_at=next_at))
    return next_at


def list_locations(
        image_id: str,
        workspace: str,
        *,
        limit: int = 50,
        after: tuple[int, str] | None = None) -> list[LocationRecord]:
    statement = sqlalchemy.select(schema.locations).where(
        schema.locations.c.image_id == image_id,
        schema.locations.c.workspace == workspace)
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(schema.locations.c.created_at,
                              schema.locations.c.id) > after)
    statement = statement.order_by(schema.locations.c.created_at,
                                   schema.locations.c.id).limit(limit)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_location(row) for row in rows]


def claim_next_location(*,
                        worker_id: str,
                        lease_seconds: int,
                        workspace: str | None = None,
                        now: int | None = None) -> LocationRecord | None:
    """Claims one indexed due shard, then one indexed local location."""
    shards = schema.registry_shards
    locations = schema.locations
    clock = catalog_state.database_epoch_expression(now=now)
    shard_statement = sqlalchemy.select(shards).where(
        shards.c.copy_next_at.is_not(None), shards.c.copy_next_at <= clock)
    if workspace is not None:
        shard_statement = shard_statement.where(shards.c.workspace == workspace)
    shard_statement = shard_statement.order_by(
        shards.c.copy_next_at,
        shards.c.id).limit(1).with_for_update(skip_locked=True)
    token = f'{worker_id}:{uuid.uuid4()}'
    with orm.Session(catalog_state.engine()) as session, session.begin():
        shard_row = session.execute(shard_statement).mappings().first()
        if shard_row is None:
            return None
        location_row = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.shard_id == shard_row['id'],
                locations.c.state.in_([
                    models.ImageLocationState.COPYING.value,
                    models.ImageLocationState.VERIFYING.value,
                ]), locations.c.copy_claimable_at
                <= clock).order_by(locations.c.copy_claimable_at,
                                   locations.c.id).limit(1).with_for_update(
                                       skip_locked=True)).mappings().first()
        if (location_row is None and str(
                shard_row['state']) in (models.ImageShardState.READY.value,
                                        models.ImageShardState.FULL.value) and
                int(shard_row['in_flight']) < int(shard_row['max_in_flight'])):
            location_row = session.execute(
                sqlalchemy.select(locations).where(
                    locations.c.shard_id == shard_row['id'], locations.c.state
                    == models.ImageLocationState.PENDING.value,
                    locations.c.copy_claimable_at <= clock).order_by(
                        locations.c.copy_claimable_at,
                        locations.c.id).limit(1).with_for_update(
                            skip_locked=True)).mappings().first()
        current = catalog_state.database_epoch(session, now=now)
        if location_row is None:
            _refresh_shard_copy_queue_in_session(session,
                                                 str(shard_row['id']),
                                                 now=current)
            return None
        if (location_row['copy_claimable_at'] is None or
                int(location_row['copy_claimable_at']) > current):
            _refresh_shard_copy_queue_in_session(session,
                                                 str(shard_row['id']),
                                                 now=current)
            return None
        reclaimed = str(location_row['state']) in (
            models.ImageLocationState.COPYING.value,
            models.ImageLocationState.VERIFYING.value)
        if not reclaimed:
            if str(shard_row['state']) not in (
                    models.ImageShardState.READY.value,
                    models.ImageShardState.FULL.value):
                return None
            if int(shard_row['in_flight']) >= int(shard_row['max_in_flight']):
                return None
        row = session.execute(locations.update().where(
            locations.c.id == location_row['id']).values(
                state=(models.ImageLocationState.VERIFYING.value if reclaimed
                       else models.ImageLocationState.COPYING.value),
                lease_kind='VERIFY' if reclaimed else 'COPY',
                lease_token=token,
                lease_expires_at=current + lease_seconds,
                attempt_count=locations.c.attempt_count + 1,
                next_retry_at=None,
                error_code=None,
                updated_at=current).returning(locations)).mappings().one()
        session.execute(
            shards.update().where(shards.c.id == shard_row['id']).values(
                in_flight=(shards.c.in_flight
                           if reclaimed else shards.c.in_flight + 1),
                last_dispatch_at=current,
                updated_at=current))
        _refresh_shard_copy_queue_in_session(session,
                                             str(shard_row['id']),
                                             now=current,
                                             rotate_due=True)
        return _location(row)


def heartbeat_location(location_id: str,
                       lease_token: str,
                       lease_seconds: int,
                       *,
                       now: int | None = None) -> bool:
    with orm.Session(catalog_state.engine()) as session, session.begin():
        snapshot = session.execute(
            sqlalchemy.select(
                schema.locations.c.shard_id, schema.locations.c.state).where(
                    schema.locations.c.id == location_id,
                    schema.locations.c.lease_token == lease_token)).first()
        if snapshot is None:
            return False
        copy_lease = str(
            snapshot.state) in (models.ImageLocationState.COPYING.value,
                                models.ImageLocationState.VERIFYING.value)
        if copy_lease:
            session.execute(
                sqlalchemy.select(schema.registry_shards.c.id).where(
                    schema.registry_shards.c.id ==
                    snapshot.shard_id).with_for_update()).one()
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(schema.locations.update().where(
            schema.locations.c.id == location_id,
            schema.locations.c.lease_token == lease_token,
            schema.locations.c.lease_expires_at.is_not(None),
            schema.locations.c.lease_expires_at > clock,
            schema.locations.c.state.in_([
                models.ImageLocationState.COPYING.value,
                models.ImageLocationState.VERIFYING.value,
                models.ImageLocationState.EVICTING.value,
            ])).values(lease_expires_at=clock + lease_seconds,
                       updated_at=clock)).rowcount
        if changed == 1 and copy_lease:
            current = catalog_state.database_epoch(session, now=now)
            _refresh_shard_copy_queue_in_session(session,
                                                 str(snapshot.shard_id),
                                                 now=current)
    return changed == 1


def begin_eviction_delete(location_id: str,
                          lease_token: str,
                          *,
                          now: int | None = None) -> bool:
    """Durably records that the fenced ECR delete may now begin."""
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(locations.update().where(
            locations.c.id == location_id,
            locations.c.state == models.ImageLocationState.EVICTING.value,
            locations.c.lease_kind == 'EVICT',
            locations.c.lease_token == lease_token,
            locations.c.lease_expires_at.is_not(None),
            locations.c.lease_expires_at
            > clock).values(lease_kind='DELETE', updated_at=clock)).rowcount
    return changed == 1


def cancel_eviction_delete(location_id: str,
                           lease_token: str,
                           *,
                           now: int | None = None) -> bool:
    """Restores pre-delete intent only when the SDK proved no call began."""
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(locations.update().where(
            locations.c.id == location_id,
            locations.c.state == models.ImageLocationState.EVICTING.value,
            locations.c.lease_kind == 'DELETE',
            locations.c.lease_token == lease_token,
            locations.c.lease_expires_at.is_not(None),
            locations.c.lease_expires_at
            > clock).values(lease_kind='EVICT', updated_at=clock)).rowcount
    return changed == 1


def mark_eviction_readback(location_id: str,
                           lease_token: str,
                           *,
                           now: int | None = None) -> bool:
    """Records a conclusive delete response before any exact readback."""
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(locations.update().where(
            locations.c.id == location_id,
            locations.c.state == models.ImageLocationState.EVICTING.value,
            locations.c.lease_kind == 'DELETE',
            locations.c.lease_token == lease_token,
            locations.c.lease_expires_at.is_not(None),
            locations.c.lease_expires_at
            > clock).values(lease_kind='READBACK', updated_at=clock)).rowcount
    return changed == 1


def transition_location_to_verifying(location_id: str,
                                     lease_token: str,
                                     *,
                                     ambiguous: bool = False,
                                     now: int | None = None) -> bool:
    with orm.Session(catalog_state.engine()) as session, session.begin():
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(schema.locations.update().where(
            schema.locations.c.id == location_id,
            schema.locations.c.state == models.ImageLocationState.COPYING.value,
            schema.locations.c.lease_token == lease_token,
            schema.locations.c.lease_expires_at.is_not(None),
            schema.locations.c.lease_expires_at
            > clock).values(state=models.ImageLocationState.VERIFYING.value,
                            lease_kind='VERIFY',
                            error_code=(models.ImageLocationErrorCode.
                                        PROVIDER_OUTCOME_AMBIGUOUS.value
                                        if ambiguous else None),
                            updated_at=clock)).rowcount
    return changed == 1


def _finish_location(session: orm.Session, *, location_id: str,
                     lease_token: str, state: models.ImageLocationState,
                     error_code: str | None, next_retry_at: int | None,
                     now: int | None) -> sqlalchemy.engine.RowMapping | None:
    locations = schema.locations
    optimistic = session.execute(
        sqlalchemy.select(
            locations.c.shard_id).where(locations.c.id == location_id)).first()
    if optimistic is None:
        return None
    shard_id = optimistic[0]
    session.execute(
        sqlalchemy.select(schema.registry_shards.c.id).where(
            schema.registry_shards.c.id == shard_id).with_for_update()).one()
    row = session.execute(
        sqlalchemy.select(locations).where(locations.c.id == location_id).
        with_for_update()).mappings().first()
    if (row is None or row['lease_token'] != lease_token or
            row['lease_expires_at'] is None or str(row['state'])
            not in (models.ImageLocationState.COPYING.value,
                    models.ImageLocationState.VERIFYING.value)):
        return None
    current = catalog_state.database_epoch(session, now=now)
    updated = session.execute(locations.update().where(
        locations.c.id == location_id, locations.c.lease_token == lease_token,
        locations.c.lease_expires_at.is_not(None), locations.c.lease_expires_at
        > catalog_state.database_epoch_expression(now=now),
        locations.c.state.in_([
            models.ImageLocationState.COPYING.value,
            models.ImageLocationState.VERIFYING.value,
        ])).values(state=state.value,
                   lease_kind=None,
                   lease_token=None,
                   lease_expires_at=None,
                   error_code=error_code,
                   next_retry_at=next_retry_at,
                   last_verified_at=(current
                                     if state == models.ImageLocationState.READY
                                     else row['last_verified_at']),
                   updated_at=current).returning(locations)).mappings().first()
    if updated is None:
        return None
    session.execute(schema.registry_shards.update().where(
        schema.registry_shards.c.id == row['shard_id'],
        schema.registry_shards.c.in_flight
        > 0).values(in_flight=schema.registry_shards.c.in_flight - 1,
                    updated_at=current))
    _refresh_shard_copy_queue_in_session(session,
                                         str(row['shard_id']),
                                         now=current)
    return updated


def complete_location_ready(location_id: str,
                            lease_token: str,
                            *,
                            now: int | None = None) -> LocationRecord | None:
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = _finish_location(session,
                               location_id=location_id,
                               lease_token=lease_token,
                               state=models.ImageLocationState.READY,
                               error_code=None,
                               next_retry_at=None,
                               now=now)
        return _location(row) if row is not None else None


def fail_location(location_id: str,
                  lease_token: str,
                  *,
                  error_code: str,
                  terminal: bool,
                  retry_at: int | None,
                  now: int | None = None) -> LocationRecord | None:
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = _finish_location(
            session,
            location_id=location_id,
            lease_token=lease_token,
            state=(models.ImageLocationState.FAILED
                   if terminal else models.ImageLocationState.PENDING),
            error_code=error_code,
            next_retry_at=None if terminal else retry_at,
            now=now)
        return _location(row) if row is not None else None


def retry_location(location_id: str,
                   workspace: str,
                   *,
                   now: int | None = None) -> LocationRecord | None:
    locations = schema.locations
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(locations.c.shard_id, locations.c.image_id).where(
                locations.c.id == location_id,
                locations.c.workspace == workspace)).first()
        if optimistic is None:
            return None
        shard = session.execute(
            sqlalchemy.select(shards).where(shards.c.id == optimistic[0]).
            with_for_update()).mappings().one()
        artifact = session.execute(
            sqlalchemy.select(schema.images).where(
                schema.images.c.id == optimistic[1], schema.images.c.workspace
                == workspace).with_for_update()).mappings().one()
        row = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.id == location_id, locations.c.workspace ==
                workspace).with_for_update()).mappings().one()
        if str(row['state']) not in (models.ImageLocationState.FAILED.value,
                                     models.ImageLocationState.MISSING.value,
                                     models.ImageLocationState.EVICTED.value):
            return None
        if str(shard['state']) not in (models.ImageShardState.READY.value,
                                       models.ImageShardState.FULL.value):
            return None
        current = catalog_state.database_epoch(session, now=now)
        values: dict[str, Any] = {
            'state': models.ImageLocationState.PENDING.value,
            'next_retry_at': None,
            'error_code': None,
            'updated_at': current,
        }
        if str(row['state']) == models.ImageLocationState.EVICTED.value:
            if int(row['reserved_declared_bytes']) != 0:
                raise RuntimeError(
                    'Evicted location retained a capacity charge.')
            charge = int(artifact['declared_size_bytes'])
            changed = session.execute(shards.update().where(
                shards.c.id == shard['id'], shards.c.reserved_manifests
                < shards.c.max_manifests,
                shards.c.reserved_declared_bytes + charge
                <= shards.c.max_declared_bytes).values(
                    reserved_manifests=shards.c.reserved_manifests + 1,
                    reserved_declared_bytes=(shards.c.reserved_declared_bytes +
                                             charge),
                    updated_at=current)).rowcount
            if changed != 1:
                raise RegistryCapacityExhaustedError(
                    'REGISTRY_CAPACITY_EXHAUSTED')
            values['reserved_declared_bytes'] = charge
        updated = session.execute(
            locations.update().where(locations.c.id == location_id).values(
                **values).returning(locations)).mappings().one()
        _refresh_shard_copy_queue_in_session(session,
                                             str(shard['id']),
                                             now=current)
        return _location(updated)


def register_worker(worker_id: str,
                    kind: models.ImageWorkerKind,
                    version: str,
                    max_in_flight: int,
                    *,
                    now: int | None = None) -> WorkerRecord:
    table = schema.workers
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
        row = session.execute(
            postgresql.insert(table).values(
                id=worker_id,
                kind=kind.value,
                version=version,
                started_at=current,
                heartbeat_at=current,
                max_in_flight=max_in_flight).on_conflict_do_update(
                    index_elements=[table.c.id],
                    set_={
                        'kind': kind.value,
                        'version': version,
                        'started_at': current,
                        'heartbeat_at': current,
                        'last_success_at': None,
                        'in_flight': 0,
                        'max_in_flight': max_in_flight,
                        'grant_budget_id': None,
                        'grant_tokens_milli': 0,
                        'grant_expires_at': None,
                    }).returning(table)).mappings().one()
        return _worker(row)


def heartbeat_worker(worker_id: str,
                     *,
                     in_flight: int,
                     success: bool = False,
                     now: int | None = None) -> bool:
    if (not isinstance(in_flight, int) or isinstance(in_flight, bool) or
            in_flight < 0):
        return False
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
        values: dict[str, Any] = {
            'heartbeat_at': current,
            'in_flight': in_flight,
        }
        if success:
            values['last_success_at'] = current
        changed = session.execute(schema.workers.update().where(
            schema.workers.c.id == worker_id, in_flight
            <= schema.workers.c.max_in_flight).values(**values)).rowcount
    return changed == 1


def _quarantine_eviction(
    session: orm.Session,
    row: sqlalchemy.engine.RowMapping,
    *,
    now: int,
    lease_token: str | None = None,
    lease_now: int | None = None,
) -> LocationRecord | None:
    """Fails one possibly in-flight delete closed without releasing capacity."""
    locations = schema.locations
    shards = schema.registry_shards
    predicates = [locations.c.id == row['id']]
    if lease_token is not None:
        predicates.extend((
            locations.c.state == models.ImageLocationState.EVICTING.value,
            locations.c.lease_kind == row['lease_kind'],
            locations.c.lease_token == lease_token,
            locations.c.lease_expires_at.is_not(None),
            locations.c.lease_expires_at
            > catalog_state.database_epoch_expression(now=lease_now),
        ))
    updated = session.execute(locations.update().where(*predicates).values(
        state=models.ImageLocationState.QUARANTINED.value,
        lease_kind=None,
        lease_token=None,
        lease_expires_at=None,
        next_retry_at=None,
        error_code=(
            models.ImageLocationErrorCode.PROVIDER_OUTCOME_AMBIGUOUS.value),
        updated_at=now).returning(locations)).mappings().first()
    if updated is None:
        return None
    changed = session.execute(shards.update().where(
        shards.c.id == row['shard_id'], shards.c.in_flight
        > 0).values(in_flight=shards.c.in_flight - 1, updated_at=now)).rowcount
    if changed != 1:
        raise RuntimeError('Eviction quarantine accounting drifted.')
    _refresh_shard_copy_queue_in_session(session, str(row['shard_id']), now=now)
    return _location(updated)


def claim_next_eviction(*,
                        worker_id: str,
                        lease_seconds: int,
                        retention_seconds: int | None = None,
                        workspace_retention_seconds: dict[str, int | None] |
                        None = None,
                        unused_before: int | None = None,
                        workspace_unused_before: dict[str, int | None] |
                        None = None,
                        now: int | None = None) -> LocationRecord | None:
    """Claims one demand-free regional digest after its retention window."""
    locations = schema.locations
    demands = schema.demands
    age_anchor = sqlalchemy.func.coalesce(locations.c.last_used_at,
                                          locations.c.last_verified_at,
                                          locations.c.created_at)
    duration_mode = retention_seconds is not None
    workspace_retentions: dict[str, int | None] = {}
    workspace_cutoffs: dict[str, int | None] = {}
    if duration_mode:
        if unused_before is not None or workspace_unused_before is not None:
            raise ValueError(
                'Eviction retention durations cannot mix with cutoffs.')
        if (not isinstance(retention_seconds, int) or
                isinstance(retention_seconds, bool) or retention_seconds < 0):
            raise ValueError('Default eviction retention is invalid.')
        workspace_retentions = workspace_retention_seconds or {}
        for workspace, seconds in workspace_retentions.items():
            if (not isinstance(workspace, str) or not workspace or
                (seconds is not None and
                 (not isinstance(seconds, int) or isinstance(seconds, bool) or
                  seconds < 0))):
                raise ValueError('Workspace eviction retention is invalid.')
    else:
        if retention_seconds is None and unused_before is None:
            raise ValueError('An eviction retention policy is required.')
        if workspace_retention_seconds is not None:
            raise ValueError(
                'Workspace retention durations require a default duration.')
        if now is None:
            raise ValueError(
                'Absolute eviction cutoffs require an explicit test clock.')
        if (not isinstance(unused_before, int) or
                isinstance(unused_before, bool)):
            raise ValueError('Default eviction cutoff is invalid.')
        workspace_cutoffs = workspace_unused_before or {}
        for workspace, cutoff in workspace_cutoffs.items():
            if (not isinstance(workspace, str) or not workspace or
                (cutoff is not None and
                 (not isinstance(cutoff, int) or isinstance(cutoff, bool)))):
                raise ValueError('Workspace eviction cutoff is invalid.')
    token = f'{worker_id}:{uuid.uuid4()}'
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        selection_current = catalog_state.database_epoch(session, now=now)
        if duration_mode:
            assert retention_seconds is not None
            default_cutoff = selection_current - retention_seconds
            workspace_cutoffs = {
                workspace:
                    (None if seconds is None else selection_current - seconds)
                for workspace, seconds in workspace_retentions.items()
            }
        else:
            assert unused_before is not None
            default_cutoff = unused_before
        if workspace_cutoffs:
            configured = tuple(workspace_cutoffs)
            due_conditions = [
                sqlalchemy.and_(locations.c.workspace == workspace, age_anchor
                                < cutoff)
                for workspace, cutoff in workspace_cutoffs.items()
                if cutoff is not None
            ]
            retention_due = sqlalchemy.or_(
                sqlalchemy.and_(locations.c.workspace.not_in(configured),
                                age_anchor < default_cutoff), *due_conditions)
        else:
            retention_due = age_anchor < default_cutoff
        ready_due = sqlalchemy.and_(
            locations.c.state == models.ImageLocationState.READY.value,
            locations.c.canonical.is_(False), retention_due,
            ~sqlalchemy.exists().where(
                demands.c.location_id == locations.c.id,
                demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                ])))
        expired = sqlalchemy.and_(
            locations.c.state == models.ImageLocationState.EVICTING.value,
            locations.c.lease_expires_at <= selection_current)
        claimable_for_shard = sqlalchemy.and_(
            sqlalchemy.not_(_inventory_active_condition(shards)),
            sqlalchemy.or_(
                sqlalchemy.and_(ready_due, shards.c.eviction_enabled.is_(True),
                                shards.c.in_flight < shards.c.max_in_flight),
                expired))
        shard_locations = sqlalchemy.and_(locations.c.shard_id == shards.c.id,
                                          claimable_for_shard)
        oldest_anchor = sqlalchemy.select(age_anchor).where(
            shard_locations).order_by(
                age_anchor,
                locations.c.id).limit(1).correlate(shards).scalar_subquery()
        oldest_id = sqlalchemy.select(
            locations.c.id).where(shard_locations).order_by(
                age_anchor,
                locations.c.id).limit(1).correlate(shards).scalar_subquery()
        shard = session.execute(
            sqlalchemy.select(shards).where(
                sqlalchemy.exists().where(shard_locations)).order_by(
                    oldest_anchor, oldest_id,
                    shards.c.id).limit(1).with_for_update(
                        of=shards, skip_locked=True)).mappings().first()
        if shard is None:
            return None
        if _inventory_active(shard):
            return None
        ready_claimable = sqlalchemy.and_(
            ready_due, bool(shard['eviction_enabled']),
            int(shard['in_flight']) < int(shard['max_in_flight']))
        row = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.shard_id == shard['id'],
                sqlalchemy.or_(ready_claimable, expired)).order_by(
                    age_anchor, locations.c.id).limit(1).with_for_update(
                        skip_locked=True)).mappings().first()
        if row is None:
            return None
        current = catalog_state.database_epoch(session, now=now)
        reclaimed = (str(
            row['state']) == models.ImageLocationState.EVICTING.value)
        if (reclaimed and (row['lease_expires_at'] is None or
                           int(row['lease_expires_at']) > current)):
            return None
        if not reclaimed:
            if duration_mode:
                assert retention_seconds is not None
                default_cutoff = current - retention_seconds
                workspace_cutoffs = {
                    workspace: (None if seconds is None else current - seconds)
                    for workspace, seconds in workspace_retentions.items()
                }
            row_cutoff = workspace_cutoffs.get(str(row['workspace']),
                                               default_cutoff)
            row_anchor = row['last_used_at']
            if row_anchor is None:
                row_anchor = row['last_verified_at']
            if row_anchor is None:
                row_anchor = row['created_at']
            if row_cutoff is None or int(row_anchor) >= row_cutoff:
                return None
        if (not reclaimed and
                int(shard['in_flight']) >= int(shard['max_in_flight'])):
            return None
        # Recheck the demand fence while the location remains locked.
        live_demand = session.execute(
            sqlalchemy.select(schema.demands.c.id).where(
                schema.demands.c.location_id == row['id'],
                schema.demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                ])).limit(1)).first()
        if live_demand is not None and not reclaimed:
            return None
        if reclaimed and row['lease_kind'] == 'DELETE':
            # An unconcluded DELETE may still resume or complete after any
            # later read. Never restore or recopy this physical reference.
            return _quarantine_eviction(session, row, now=current)
        if reclaimed and row['lease_kind'] == 'READBACK':
            # The provider conclusion is durable, so a successor repeats only
            # exact presence resolution and never submits another delete.
            updated = session.execute(
                locations.update().where(locations.c.id == row['id']).values(
                    lease_token=token,
                    lease_expires_at=current + lease_seconds,
                    attempt_count=locations.c.attempt_count + 1,
                    error_code=None,
                    updated_at=current).returning(locations)).mappings().one()
            session.execute(shards.update().where(
                shards.c.id == row['shard_id']).values(last_dispatch_at=current,
                                                       updated_at=current))
            return _location(updated)
        if reclaimed and row['lease_kind'] != 'EVICT':
            return _quarantine_eviction(session, row, now=current)
        if reclaimed and live_demand is not None:
            # No DELETE intent means the old worker could not pass its provider
            # hook. Restore the wanted bytes without performing provider I/O.
            updated = session.execute(
                locations.update().where(locations.c.id == row['id']).values(
                    state=models.ImageLocationState.READY.value,
                    lease_kind=None,
                    lease_token=None,
                    lease_expires_at=None,
                    next_retry_at=None,
                    error_code=None,
                    updated_at=current).returning(locations)).mappings().one()
            changed = session.execute(shards.update().where(
                shards.c.id == row['shard_id'], shards.c.in_flight
                > 0).values(in_flight=shards.c.in_flight - 1,
                            updated_at=current)).rowcount
            if changed != 1:
                raise RuntimeError('Eviction recovery accounting drifted.')
            _refresh_shard_copy_queue_in_session(session,
                                                 str(row['shard_id']),
                                                 now=current)
            return _location(updated)
        updated = session.execute(
            locations.update().where(locations.c.id == row['id']).values(
                state=models.ImageLocationState.EVICTING.value,
                lease_kind='EVICT',
                lease_token=token,
                lease_expires_at=current + lease_seconds,
                attempt_count=locations.c.attempt_count + 1,
                error_code=None,
                updated_at=current).returning(locations)).mappings().one()
        session.execute(
            shards.update().where(shards.c.id == row['shard_id']).values(
                in_flight=(shards.c.in_flight
                           if reclaimed else shards.c.in_flight + 1),
                last_dispatch_at=current,
                updated_at=current))
        _refresh_shard_copy_queue_in_session(session,
                                             str(row['shard_id']),
                                             now=current)
        return _location(updated)


def complete_eviction(location_id: str,
                      lease_token: str,
                      *,
                      present: bool | None,
                      provider_not_called: bool = False,
                      now: int | None = None) -> LocationRecord | None:
    """Commits only exact presence/absence or a proven no-I/O denial."""
    if provider_not_called and present is not None:
        raise ValueError('No-I/O eviction completion cannot assert presence.')
    locations = schema.locations
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(locations.c.shard_id).where(
                locations.c.id == location_id)).first()
        if optimistic is None:
            return None
        shard = session.execute(
            sqlalchemy.select(shards).where(shards.c.id == optimistic[0]).
            with_for_update()).mappings().one()
        row = session.execute(
            sqlalchemy.select(locations).where(locations.c.id == location_id).
            with_for_update()).mappings().first()
        if (row is None or
                str(row['state']) != models.ImageLocationState.EVICTING.value or
                row['lease_token'] != lease_token or
                row['lease_expires_at'] is None):
            return None
        current = catalog_state.database_epoch(session, now=now)
        if provider_not_called and row['lease_kind'] != 'EVICT':
            return None
        inventory_active = _inventory_active(shard)
        if provider_not_called and inventory_active:
            # A completed-list nominee cannot race to READY without the exact
            # provider proof required by inventory finalization.
            return None
        if (not provider_not_called and present is None and
                row['lease_kind'] != 'DELETE'):
            # Ambiguity is admissible only for the still-unconcluded intent.
            return None
        if (not provider_not_called and present is not None and
                row['lease_kind'] != 'READBACK'):
            # Presence is admissible only after a provider conclusion was
            # durably separated from an in-flight or ambiguous delete.
            return None
        live_demand = session.execute(
            sqlalchemy.select(schema.demands.c.id).where(
                schema.demands.c.location_id == location_id,
                schema.demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                ])).limit(1)).first()
        if present is None and not provider_not_called:
            return _quarantine_eviction(session,
                                        row,
                                        now=current,
                                        lease_token=lease_token,
                                        lease_now=now)
        release_reservation = False
        if provider_not_called:
            new_state = models.ImageLocationState.READY
            error_code = models.ImageLocationErrorCode.EVICTION_FAILED.value
        elif present:
            new_state = models.ImageLocationState.READY
            error_code = None
        elif live_demand is not None:
            new_state = models.ImageLocationState.PENDING
            error_code = None
        else:
            new_state = models.ImageLocationState.EVICTED
            error_code = None
            release_reservation = True
        if release_reservation and inventory_active:
            # Keep provider-proven absence in READBACK until inventory stops
            # comparing earlier observations with capacity accounting.
            return None
        location_values: dict[str, Any] = {
            'state': new_state.value,
            'lease_kind': None,
            'lease_token': None,
            'lease_expires_at': None,
            'next_retry_at': None,
            'error_code': error_code,
            'updated_at': current,
        }
        if release_reservation:
            location_values['reserved_declared_bytes'] = 0
        updated = session.execute(locations.update().where(
            locations.c.id == location_id,
            locations.c.state == models.ImageLocationState.EVICTING.value,
            locations.c.lease_kind == row['lease_kind'],
            locations.c.lease_token == lease_token,
            locations.c.lease_expires_at.is_not(None),
            locations.c.lease_expires_at
            > catalog_state.database_epoch_expression(now=now)).values(
                **location_values).returning(locations)).mappings().first()
        if updated is None:
            return None
        shard_values: dict[str, Any] = {
            'in_flight': sqlalchemy.case(
                (shards.c.in_flight > 0, shards.c.in_flight - 1), else_=0),
            'updated_at': current,
        }
        if release_reservation:
            shard_values.update(
                reserved_manifests=shards.c.reserved_manifests - 1,
                reserved_declared_bytes=(shards.c.reserved_declared_bytes -
                                         row['reserved_declared_bytes']),
                state=sqlalchemy.case((sqlalchemy.and_(
                    shards.c.state == models.ImageShardState.FULL.value,
                    shards.c.reserved_manifests - 1 < shards.c.max_manifests,
                    shards.c.reserved_declared_bytes -
                    row['reserved_declared_bytes']
                    < shards.c.max_declared_bytes),
                                       models.ImageShardState.READY.value),
                                      else_=shards.c.state))
        shard_predicates = [shards.c.id == row['shard_id']]
        if release_reservation:
            shard_predicates.extend((
                shards.c.reserved_manifests > 0,
                shards.c.reserved_declared_bytes
                >= row['reserved_declared_bytes'],
            ))
        changed = session.execute(shards.update().where(
            *shard_predicates).values(**shard_values)).rowcount
        if changed != 1:
            raise RuntimeError('Eviction reservation accounting drifted.')
        _refresh_shard_copy_queue_in_session(session,
                                             str(row['shard_id']),
                                             now=current)
        return _location(updated)


def list_failed_canonical_reap_candidates(*,
                                          limit: int = 100
                                         ) -> list[LocationRecord]:
    """Returns failed canonical reservations with no remaining owner."""
    if not 1 <= limit <= 1000:
        raise ValueError('Canonical reaping page size is invalid.')
    locations = schema.locations
    publications = schema.publications
    demands = schema.demands
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.canonical.is_(True),
                locations.c.state == models.ImageLocationState.FAILED.value,
                locations.c.lease_token.is_(None), ~sqlalchemy.exists().where(
                    publications.c.canonical_location_id == locations.c.id,
                    publications.c.reservation_active.is_(True)),
                ~sqlalchemy.exists().where(
                    demands.c.location_id == locations.c.id)).order_by(
                        locations.c.updated_at,
                        locations.c.id).limit(limit)).mappings().all()
    return [_location(row) for row in rows]


def reap_failed_canonical_reservation(location_id: str,
                                      *,
                                      expected_updated_at: int,
                                      exact_absence: bool,
                                      now: int | None = None) -> bool:
    """Deletes one empty failed reservation after rechecking every fence."""
    if not exact_absence:
        return False
    locations = schema.locations
    shards = schema.registry_shards
    publications = schema.publications
    demands = schema.demands
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(locations.c.shard_id).where(
                locations.c.id == location_id)).first()
        if optimistic is None:
            return False
        shard = session.execute(
            sqlalchemy.select(shards).where(shards.c.id == optimistic[0]).
            with_for_update()).mappings().one()
        row = session.execute(
            sqlalchemy.select(locations).where(locations.c.id == location_id).
            with_for_update()).mappings().first()
        if (row is None or not bool(row['canonical']) or
                str(row['state']) != models.ImageLocationState.FAILED.value or
                row['lease_token'] is not None or
                int(row['updated_at']) != expected_updated_at):
            return False
        retained_publication = session.execute(
            sqlalchemy.select(publications.c.id).where(
                publications.c.canonical_location_id == location_id,
                publications.c.reservation_active.is_(True)).limit(1)).first()
        any_demand = session.execute(
            sqlalchemy.select(demands.c.id).where(
                demands.c.location_id == location_id).limit(1)).first()
        if retained_publication is not None or any_demand is not None:
            return False
        current = catalog_state.database_epoch(session, now=now)
        # Failed history remains queryable, but its expired reservation no
        # longer owns the physical location being removed.
        session.execute(publications.update().where(
            publications.c.canonical_location_id == location_id,
            publications.c.reservation_active.is_(False)).values(
                canonical_location_id=None,
                image_id=None,
                source_id=None,
                updated_at=current))
        deleted = session.execute(
            locations.delete().where(locations.c.id == location_id)).rowcount
        if deleted != 1:
            return False
        reserved_manifests = int(shard['reserved_manifests']) - 1
        reserved_bytes = (int(shard['reserved_declared_bytes']) -
                          int(row['reserved_declared_bytes']))
        if reserved_manifests < 0 or reserved_bytes < 0:
            raise ValueError('Registry shard reservation counters are corrupt.')
        shard_state = str(shard['state'])
        if (shard_state == models.ImageShardState.FULL.value and
                reserved_manifests < int(shard['max_manifests']) and
                reserved_bytes < int(shard['max_declared_bytes'])):
            shard_state = models.ImageShardState.READY.value
        session.execute(
            shards.update().where(shards.c.id == shard['id']).values(
                reserved_manifests=reserved_manifests,
                reserved_declared_bytes=reserved_bytes,
                state=shard_state,
                updated_at=current))
        return True


def list_workers(*,
                 limit: int = 50,
                 after: tuple[int, str] | None = None) -> list[WorkerRecord]:
    statement = sqlalchemy.select(schema.workers)
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(schema.workers.c.heartbeat_at,
                              schema.workers.c.id) < after)
    statement = statement.order_by(schema.workers.c.heartbeat_at.desc(),
                                   schema.workers.c.id.desc()).limit(limit)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_worker(row) for row in rows]


def get_worker(worker_id: str) -> WorkerRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.workers).where(
                schema.workers.c.id == worker_id)).mappings().first()
    return _worker(row) if row is not None else None


def _provider_budget_values(*, provider: str, partition: str, account: str,
                            region: str, api_family: str,
                            applied_rate_per_second: int,
                            burst: int) -> tuple[str, dict[str, Any], int, int]:
    if (applied_rate_per_second <= 0 or burst <= 0 or
            applied_rate_per_second > 1_000_000 or burst > 1_000_000):
        raise ValueError('Provider budget limits are invalid.')
    rate_milli = applied_rate_per_second * 1000
    burst_milli = burst * 1000
    values = {
        'provider': provider,
        'partition': partition,
        'account': account,
        'region': region,
        'api_family': api_family,
    }
    budget_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL, 'skypilot-image-budget:' + ':'.join(
                (provider, partition, account, region, api_family))))
    return budget_id, values, rate_milli, burst_milli


def upsert_provider_budget_in_session(session: orm.Session, *, provider: str,
                                      partition: str, account: str, region: str,
                                      api_family: str,
                                      applied_rate_per_second: int, burst: int,
                                      now: int) -> ProviderBudgetRecord:
    """Applies one qualified budget inside its profile activation transaction."""
    budget_id, values, rate_milli, burst_milli = _provider_budget_values(
        provider=provider,
        partition=partition,
        account=account,
        region=region,
        api_family=api_family,
        applied_rate_per_second=applied_rate_per_second,
        burst=burst)
    table = schema.provider_budgets
    row = session.execute(
        postgresql.insert(table).values(
            id=budget_id,
            **values,
            applied_rate_milli=rate_milli,
            burst_milli=burst_milli,
            tokens_milli=burst_milli,
            refilled_at=now,
            updated_at=now).on_conflict_do_update(
                constraint='uq_container_image_provider_budget',
                set_={
                    'applied_rate_milli': rate_milli,
                    'burst_milli': burst_milli,
                    'tokens_milli': sqlalchemy.func.least(
                        table.c.tokens_milli, burst_milli),
                    'refilled_at': sqlalchemy.func.greatest(
                        table.c.refilled_at, now),
                    'updated_at': now,
                }).returning(table)).mappings().one()
    return _provider_budget(row)


def ensure_provider_budget(*,
                           provider: str,
                           partition: str,
                           account: str,
                           region: str,
                           api_family: str,
                           applied_rate_per_second: int,
                           burst: int,
                           now: int | None = None) -> ProviderBudgetRecord:
    """Creates a missing qualification budget without changing a live one."""
    budget_id, values, rate_milli, burst_milli = _provider_budget_values(
        provider=provider,
        partition=partition,
        account=account,
        region=region,
        api_family=api_family,
        applied_rate_per_second=applied_rate_per_second,
        burst=burst)
    table = schema.provider_budgets
    with orm.Session(catalog_state.engine()) as session, session.begin():
        existing = session.execute(
            sqlalchemy.select(table).where(
                table.c.id == budget_id).with_for_update()).mappings().first()
        if existing is not None:
            return _provider_budget(existing)
        current = catalog_state.database_epoch(session, now=now)
        row = session.execute(
            postgresql.insert(table).values(
                id=budget_id,
                **values,
                applied_rate_milli=rate_milli,
                burst_milli=burst_milli,
                tokens_milli=burst_milli,
                refilled_at=current,
                updated_at=current).on_conflict_do_nothing(
                    constraint='uq_container_image_provider_budget').returning(
                        table)).mappings().first()
        if row is None:
            row = session.execute(
                sqlalchemy.select(table).where(table.c.id == budget_id).
                with_for_update()).mappings().one()
    return _provider_budget(row)


def upsert_provider_budget(*,
                           provider: str,
                           partition: str,
                           account: str,
                           region: str,
                           api_family: str,
                           applied_rate_per_second: int,
                           burst: int,
                           now: int | None = None) -> ProviderBudgetRecord:
    """Installs one qualification-backed, account-region API budget."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        budget_id, _, _, _ = _provider_budget_values(
            provider=provider,
            partition=partition,
            account=account,
            region=region,
            api_family=api_family,
            applied_rate_per_second=applied_rate_per_second,
            burst=burst)
        session.execute(
            sqlalchemy.select(schema.provider_budgets.c.id).where(
                schema.provider_budgets.c.id ==
                budget_id).with_for_update()).first()
        current = catalog_state.database_epoch(session, now=now)
        return upsert_provider_budget_in_session(
            session,
            provider=provider,
            partition=partition,
            account=account,
            region=region,
            api_family=api_family,
            applied_rate_per_second=applied_rate_per_second,
            burst=burst,
            now=current)


def get_provider_budget(*, provider: str, partition: str, account: str,
                        region: str,
                        api_family: str) -> ProviderBudgetRecord | None:
    table = schema.provider_budgets
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.provider == provider, table.c.partition == partition,
                table.c.account == account, table.c.region == region,
                table.c.api_family == api_family)).mappings().first()
    return _provider_budget(row) if row is not None else None


def list_provider_budgets(*, limit: int = 1000) -> list[ProviderBudgetRecord]:
    if not 1 <= limit <= 1001:
        raise ValueError('Provider budget page size is invalid.')
    table = schema.provider_budgets
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(table).order_by(
                table.c.provider, table.c.partition, table.c.account,
                table.c.region,
                table.c.api_family).limit(limit)).mappings().all()
    return [_provider_budget(row) for row in rows]


def acquire_provider_grant(worker_id: str,
                           budget_id: str,
                           requested_calls: int,
                           *,
                           grant_seconds: int = 1,
                           now: int | None = None) -> ProviderGrant | None:
    """Moves a bounded token batch from one provider budget to one worker."""
    if (not isinstance(grant_seconds, int) or isinstance(grant_seconds, bool) or
            not 1 <= grant_seconds <= 60):
        raise ValueError('Provider grant duration is invalid.')
    budgets = schema.provider_budgets
    workers = schema.workers
    with orm.Session(catalog_state.engine()) as session, session.begin():
        budget = session.execute(
            sqlalchemy.select(budgets).where(budgets.c.id == budget_id).
            with_for_update()).mappings().first()
        worker = session.execute(
            sqlalchemy.select(workers).where(workers.c.id == worker_id).
            with_for_update()).mappings().first()
        if budget is None or worker is None:
            return None
        current = catalog_state.database_epoch(session, now=now)
        if (budget['blocked_until'] is not None and
                int(budget['blocked_until']) > current):
            return None
        if (worker['grant_expires_at'] is not None and
                int(worker['grant_expires_at']) > current and
                int(worker['grant_tokens_milli']) > 0):
            if str(worker['grant_budget_id']) != budget_id:
                return None
            return ProviderGrant(
                budget_id=str(worker['grant_budget_id']),
                tokens=int(worker['grant_tokens_milli']) // 1000,
                valid_for_seconds=(int(worker['grant_expires_at']) - current))
        refill_current = max(current, int(budget['refilled_at']))
        elapsed = max(0, refill_current - int(budget['refilled_at']))
        available = min(
            int(budget['burst_milli']),
            int(budget['tokens_milli']) +
            elapsed * int(budget['applied_rate_milli']))
        request_milli = min(requested_calls, 64) * 1000
        one_second = int(budget['applied_rate_milli'])
        granted = min(available, request_milli, max(1000, one_second))
        granted -= granted % 1000
        if granted <= 0:
            session.execute(budgets.update().where(
                budgets.c.id == budget_id).values(tokens_milli=available,
                                                  refilled_at=refill_current,
                                                  updated_at=current))
            return None
        expires_at = current + grant_seconds
        session.execute(budgets.update().where(
            budgets.c.id == budget_id).values(tokens_milli=available - granted,
                                              refilled_at=refill_current,
                                              updated_at=current))
        session.execute(workers.update().where(
            workers.c.id == worker_id).values(grant_budget_id=budget_id,
                                              grant_tokens_milli=granted,
                                              grant_expires_at=expires_at,
                                              heartbeat_at=current))
        return ProviderGrant(budget_id=budget_id,
                             tokens=granted // 1000,
                             valid_for_seconds=grant_seconds)


def record_provider_throttle(budget_id: str,
                             *,
                             now: int | None = None) -> int | None:
    budgets = schema.provider_budgets
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(budgets).where(budgets.c.id == budget_id).
            with_for_update()).mappings().first()
        if row is None:
            return None
        current = catalog_state.database_epoch(session, now=now)
        count = int(row['throttle_count']) + 1
        delay = min(2**min(count, 8), 300)
        blocked_until = current + delay
        session.execute(budgets.update().where(
            budgets.c.id == budget_id).values(throttle_count=count,
                                              blocked_until=blocked_until,
                                              updated_at=current))
        return delay


def compact_stale_workers(*,
                          max_age_seconds: int,
                          limit: int = 500,
                          now: int | None = None) -> int:
    if (not isinstance(max_age_seconds, int) or
            isinstance(max_age_seconds, bool) or max_age_seconds < 1):
        raise ValueError('Worker retention duration is invalid.')
    table = schema.workers
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
        worker_ids = session.execute(
            sqlalchemy.select(table.c.id).where(
                table.c.heartbeat_at < current - max_age_seconds).order_by(
                    table.c.heartbeat_at,
                    table.c.id).limit(limit).with_for_update(
                        skip_locked=True)).scalars().all()
        if not worker_ids:
            return 0
        return session.execute(table.delete().where(
            table.c.id.in_(worker_ids))).rowcount
