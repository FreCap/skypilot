"""Profile, physical shard, location, budget, and worker persistence."""
# pylint: disable=missing-class-docstring

from __future__ import annotations

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
    expires_at: int


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
        inventory_lease_token=row['inventory_lease_token'],
        inventory_lease_expires_at=row['inventory_lease_expires_at'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


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
    current = int(time.time()) if now is None else now
    encoded_config = json.dumps(config_snapshot,
                                sort_keys=True,
                                separators=(',', ':'))
    if len(encoded_config.encode()) > 256 * 1024:
        raise ValueError('Registry profile snapshot exceeds 256 KiB.')
    if hashlib.sha256(encoded_config.encode()).hexdigest() != config_hash:
        raise ValueError('Registry profile snapshot does not match its hash.')
    table = schema.profile_revisions
    with orm.Session(catalog_state.engine()) as session, session.begin():
        existing_rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.workspace == workspace,
                table.c.profile == profile).order_by(
                    table.c.id).with_for_update()).mappings().all()
        for row in existing_rows:
            if (int(row['revision']) == revision and
                    str(row['config_hash']) == config_hash and
                    str(row['config_json']) == encoded_config and
                    str(row['physical_manifest_hash']) == physical_manifest_hash
                    and str(row['state'])
                    in (models.ImageProfileState.QUALIFYING.value,
                        models.ImageProfileState.ACTIVE.value)):
                return _profile(row)

        ready_exists = session.execute(
            sqlalchemy.select(sqlalchemy.literal(True)).select_from(
                schema.publications.join(
                    table,
                    schema.publications.c.profile_revision_id == table.c.id)).
            where(
                table.c.workspace == workspace, table.c.profile == profile,
                schema.publications.c.state ==
                models.ImagePublicationState.READY.value).limit(1)).first()
        active = next(
            (row for row in existing_rows
             if str(row['state']) == models.ImageProfileState.ACTIVE.value),
            None)
        if (ready_exists is not None and active is not None and str(
                active['physical_manifest_hash']) != physical_manifest_hash):
            raise CanonicalCustodyChangeError(
                'V0 cannot change the canonical physical manifest after a '
                'release is published.')
        session.execute(table.update().where(
            table.c.workspace == workspace, table.c.profile == profile,
            table.c.state == models.ImageProfileState.QUALIFYING.value).values(
                state=models.ImageProfileState.SUPERSEDED.value,
                updated_at=current))
        generation = max(
            (int(row['desired_generation']) for row in existing_rows),
            default=0) + 1
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


def list_qualifying_profiles(*,
                             include_active: bool = False,
                             limit: int = 100) -> list[ProfileRevisionRecord]:
    """Returns a bounded fair qualification work page."""
    if not 1 <= limit <= 1000:
        raise ValueError('Qualifying profile page size is invalid.')
    with orm.Session(catalog_state.engine()) as session:
        states = [models.ImageProfileState.QUALIFYING.value]
        if include_active:
            states.append(models.ImageProfileState.ACTIVE.value)
        rows = session.execute(
            sqlalchemy.select(schema.profile_revisions).where(
                schema.profile_revisions.c.state.in_(states)).order_by(
                    schema.profile_revisions.c.updated_at,
                    schema.profile_revisions.c.id).limit(
                        limit)).mappings().all()
    return [_profile(row) for row in rows]


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


def record_profile_attestation(*,
                               profile_revision_id: str,
                               kind: str,
                               evidence: dict[str, Any],
                               expected_generation: int,
                               expected_config_hash: str,
                               terraform_hash: str | None = None,
                               now: int | None = None) -> ProfileRevisionRecord:
    """Merges one independently authenticated, secret-free attestation."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        return record_profile_attestation_in_session(
            session,
            profile_revision_id=profile_revision_id,
            kind=kind,
            evidence=evidence,
            expected_generation=expected_generation,
            expected_config_hash=expected_config_hash,
            terraform_hash=terraform_hash,
            now=current)


def record_profile_attestation_in_session(session: orm.Session,
                                          *,
                                          profile_revision_id: str,
                                          kind: str,
                                          evidence: dict[str, Any],
                                          expected_generation: int,
                                          expected_config_hash: str,
                                          terraform_hash: str | None = None,
                                          now: int) -> ProfileRevisionRecord:
    """Transaction-scoped variant for operation and attestation atomicity."""
    if not isinstance(kind, str) or not kind or len(kind) > 128:
        raise ValueError('Attestation kind must be a bounded identifier.')
    encoded_evidence = json.dumps(evidence,
                                  sort_keys=True,
                                  separators=(',', ':'))
    if len(encoded_evidence.encode()) > 16 * 1024:
        raise ValueError('Profile attestation exceeds 16 KiB.')
    table = schema.profile_revisions
    row = session.execute(
        sqlalchemy.select(table).where(table.c.id == profile_revision_id).
        with_for_update()).mappings().one()
    if (str(row['state']) not in (models.ImageProfileState.QUALIFYING.value,
                                  models.ImageProfileState.ACTIVE.value) or
            int(row['desired_generation']) != expected_generation or
            str(row['config_hash']) != expected_config_hash):
        raise StaleProfileRevisionError(
            'Attestation no longer matches the desired profile revision.')
    attestations = json.loads(str(row['attestations_json']))
    attestations[kind] = evidence
    encoded = json.dumps(attestations, sort_keys=True, separators=(',', ':'))
    if len(encoded.encode()) > 256 * 1024:
        raise ValueError('Profile attestation set exceeds 256 KiB.')
    attestation_hash = hashlib.sha256(encoded.encode()).hexdigest()
    values: dict[str, Any] = {
        'attestations_json': encoded,
        'attestations_hash': attestation_hash,
        'updated_at': now,
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
        *, profile_revision_id: str, expected_generation: int,
        expected_config_hash: str, shard_id: str,
        expected_operational_revision_id: str, expected_target_fingerprint: str,
        expected_physical_fingerprint: str, expected_inventory_epoch: int,
        expected_inventory_completed_at: int, kind: str,
        evidence: dict[str, Any], now: int) -> ProfileRevisionRecord | None:
    """Records candidate proof only against one unchanged operational epoch."""
    profiles = schema.profile_revisions
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        candidate = session.execute(
            sqlalchemy.select(profiles).where(
                profiles.c.id ==
                profile_revision_id).with_for_update()).mappings().one()
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
                shard['inventory_lease_token'] is not None):
            return None
        operational_state = session.execute(
            sqlalchemy.select(profiles.c.state).where(
                profiles.c.id == expected_operational_revision_id)).scalar()
        if operational_state != models.ImageProfileState.ACTIVE.value:
            return None
        # The candidate row is already locked. The shared helper's repeated
        # SELECT FOR UPDATE is reentrant and preserves profile-before-shard
        # ordering for every competing transaction.
        return record_profile_attestation_in_session(
            session,
            profile_revision_id=profile_revision_id,
            kind=kind,
            evidence=evidence,
            expected_generation=expected_generation,
            expected_config_hash=expected_config_hash,
            now=now)


def reserve_canary_cost(session: orm.Session, *, profile_revision_id: str,
                        expected_generation: int, utc_day: str,
                        worst_case_microusd: int,
                        now: int) -> ProfileRevisionRecord:
    """Hard-reserves one canary's worst-case daily cost before launch."""
    table = schema.profile_revisions
    row = session.execute(
        sqlalchemy.select(table).where(table.c.id == profile_revision_id).
        with_for_update()).mappings().one()
    if (str(row['state']) not in (models.ImageProfileState.QUALIFYING.value,
                                  models.ImageProfileState.ACTIVE.value) or
            int(row['desired_generation']) != expected_generation):
        raise StaleProfileRevisionError(
            'Canary no longer matches the desired profile revision.')
    reserved = (0 if row['canary_window_day'] != utc_day else int(
        row['canary_reserved_microusd']))
    if (worst_case_microusd < 0 or reserved + worst_case_microusd > int(
            row['max_daily_canary_microusd'])):
        raise ValueError('CANARY_DAILY_COST_LIMIT')
    updated = session.execute(
        table.update().where(table.c.id == profile_revision_id).values(
            canary_window_day=utc_day,
            canary_reserved_microusd=reserved + worst_case_microusd,
            updated_at=now).returning(table)).mappings().one()
    return _profile(updated)


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
    if row['inventory_lease_token'] is not None:
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
    current = int(time.time()) if now is None else now
    table = schema.registry_shards
    available = sqlalchemy.or_(table.c.inventory_lease_token.is_(None),
                               table.c.inventory_lease_expires_at <= current)
    due = sqlalchemy.or_(
        table.c.inventory_completed_at.is_(None), table.c.inventory_completed_at
        <= current - interval_seconds)
    token = f'{worker_id}:{uuid.uuid4()}'
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.state.in_([
                    models.ImageShardState.PENDING.value,
                    models.ImageShardState.READY.value,
                    models.ImageShardState.FULL.value,
                    models.ImageShardState.DRIFTED.value,
                ]), available,
                due).order_by(table.c.inventory_completed_at.asc().nullsfirst(),
                              table.c.id).limit(1).with_for_update(
                                  skip_locked=True)).mappings().first()
        if row is None:
            return None
        in_progress = (row['inventory_started_at'] is not None and
                       row['inventory_completed_at'] is None)
        values: dict[str, Any] = {
            'inventory_lease_token': token,
            'inventory_lease_expires_at': current + lease_seconds,
            'updated_at': current,
        }
        if not in_progress:
            values.update(inventory_epoch=int(row['inventory_epoch']) + 1,
                          inventory_cursor=None,
                          inventory_started_at=current,
                          inventory_completed_at=None,
                          observed_manifests=0)
        updated = session.execute(
            table.update().where(table.c.id == row['id']).values(
                **values).returning(table)).mappings().one()
        return _shard(updated)


def record_inventory_page(shard_id: str,
                          lease_token: str,
                          digests: tuple[str, ...],
                          next_cursor: str | None,
                          *,
                          now: int | None = None) -> ShardRecord | None:
    """Commits one bounded provider page and its durable continuation cursor."""
    current = int(time.time()) if now is None else now
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
    with orm.Session(catalog_state.engine()) as session, session.begin():
        shard = session.execute(
            sqlalchemy.select(shards).where(
                shards.c.id == shard_id).with_for_update()).mappings().first()
        if (shard is None or shard['inventory_lease_token'] != lease_token or
                shard['inventory_lease_expires_at'] is None or
                int(shard['inventory_lease_expires_at']) <= current or
                shard['inventory_started_at'] is None or
                shard['inventory_completed_at'] is not None):
            return None
        epoch = int(shard['inventory_epoch'])
        known = 0
        if normalized:
            known = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(locations).where(
                    locations.c.shard_id == shard_id,
                    locations.c.runtime_digest.in_(normalized))).scalar_one()
            session.execute(locations.update().where(
                locations.c.shard_id == shard_id,
                locations.c.runtime_digest.in_(normalized)).values(
                    inventory_epoch_seen=epoch, updated_at=current))
        observed = int(shard['observed_manifests']) + len(normalized)
        if known != len(normalized):
            # Persist epoch-local unexplained content across continuation
            # pages without adding another ledger field. A clean new epoch
            # resets the counter and can recover the shard.
            observed = max(observed, int(shard['reserved_manifests']) + 1)
        drifted = observed > int(shard['reserved_manifests'])
        values: dict[str, Any] = {
            'observed_manifests': observed,
            'inventory_cursor': next_cursor,
            'inventory_lease_token': None,
            'inventory_lease_expires_at': None,
            'updated_at': current,
        }
        if next_cursor is None:
            missing = session.execute(
                sqlalchemy.select(locations.c.id).where(
                    locations.c.shard_id == shard_id,
                    locations.c.state == models.ImageLocationState.READY.value,
                    locations.c.last_verified_at.is_not(None),
                    locations.c.last_verified_at
                    < int(shard['inventory_started_at']),
                    sqlalchemy.or_(locations.c.inventory_epoch_seen.is_(None),
                                   locations.c.inventory_epoch_seen
                                   < epoch)).limit(1)).first()
            drifted = drifted or missing is not None
            if drifted:
                state = models.ImageShardState.DRIFTED.value
            elif (int(shard['reserved_manifests']) >= int(
                    shard['max_manifests']) or
                  int(shard['reserved_declared_bytes']) >= int(
                      shard['max_declared_bytes'])):
                state = models.ImageShardState.FULL.value
            else:
                state = models.ImageShardState.READY.value
            values.update(inventory_completed_at=current,
                          state=state,
                          qualified_at=(current if state
                                        in (models.ImageShardState.READY.value,
                                            models.ImageShardState.FULL.value)
                                        else shard['qualified_at']))
        updated = session.execute(
            shards.update().where(shards.c.id == shard_id).values(
                **values).returning(shards)).mappings().one()
        return _shard(updated)


def abandon_inventory_claim(shard_id: str,
                            lease_token: str,
                            *,
                            invalid_cursor: bool = False,
                            now: int | None = None) -> bool:
    """Releases a failed claim, restarting only an invalid provider cursor."""
    current = int(time.time()) if now is None else now
    values: dict[str, Any] = {
        'inventory_lease_token': None,
        'inventory_lease_expires_at': None,
        'updated_at': current,
    }
    if invalid_cursor:
        values.update(inventory_cursor=None,
                      inventory_started_at=None,
                      inventory_completed_at=None,
                      observed_manifests=0)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard_id,
            schema.registry_shards.c.inventory_lease_token ==
            lease_token).values(**values)).rowcount
    return changed == 1


def mark_shard_drifted(shard_id: str,
                       lease_token: str,
                       *,
                       now: int | None = None) -> bool:
    """Fails closed on live infrastructure mismatch while releasing its lease."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard_id,
            schema.registry_shards.c.inventory_lease_token ==
            lease_token).values(state=models.ImageShardState.DRIFTED.value,
                                inventory_lease_token=None,
                                inventory_lease_expires_at=None,
                                inventory_cursor=None,
                                inventory_started_at=None,
                                inventory_completed_at=None,
                                observed_manifests=0,
                                updated_at=current)).rowcount
    return changed == 1


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
                    locations.c.state == models.ImageLocationState.READY.value,
                    locations.c.last_verified_at.is_not(None),
                    locations.c.last_verified_at
                    < shards.c.inventory_started_at,
                    sqlalchemy.or_(
                        locations.c.inventory_epoch_seen.is_(None),
                        locations.c.inventory_epoch_seen
                        < inventory_epoch)).order_by(
                            locations.c.last_verified_at,
                            locations.c.id).limit(limit)).mappings().all()
    return [_location(row) for row in rows]


def complete_inventory_confirmation(
        location_id: str,
        shard_id: str,
        inventory_epoch: int,
        *,
        present: bool,
        now: int | None = None) -> LocationRecord | None:
    """Records an exact digest read after rechecking the completed epoch."""
    current = int(time.time()) if now is None else now
    shards = schema.registry_shards
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        shard = session.execute(
            sqlalchemy.select(shards).where(
                shards.c.id == shard_id).with_for_update()).mappings().first()
        if (shard is None or int(shard['inventory_epoch']) != inventory_epoch or
                shard['inventory_completed_at'] is None):
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
        updated = session.execute(
            locations.update().where(locations.c.id == location_id).values(
                state=(models.ImageLocationState.READY.value
                       if present else models.ImageLocationState.MISSING.value),
                inventory_epoch_seen=(inventory_epoch if present else
                                      row['inventory_epoch_seen']),
                error_code=(
                    None if present else
                    models.ImageLocationErrorCode.MANIFEST_MISSING.value),
                updated_at=current).returning(locations)).mappings().one()
        return _location(updated)


def _bounded_location_count(session: orm.Session, shard_ids: list[str],
                            states: tuple[str, ...], result_cap: int) -> int:
    bounded = sqlalchemy.select(schema.locations.c.id).where(
        schema.locations.c.shard_id.in_(shard_ids),
        schema.locations.c.state.in_(states)).limit(result_cap + 1).subquery()
    statement = sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                                 ).select_from(bounded)
    return int(session.execute(statement).scalar_one())


def _bounded_location_reservation_stats(session: orm.Session,
                                        shard_ids: list[str], state: str,
                                        result_cap: int) -> tuple[int, int]:
    bounded = sqlalchemy.select(
        schema.locations.c.id,
        schema.locations.c.reserved_declared_bytes).where(
            schema.locations.c.shard_id.in_(shard_ids),
            schema.locations.c.state == state).limit(result_cap + 1).subquery()
    statement = sqlalchemy.select(
        sqlalchemy.func.count(),  # pylint: disable=not-callable
        sqlalchemy.func.coalesce(
            sqlalchemy.func.sum(bounded.c.reserved_declared_bytes), 0),
    ).select_from(bounded)
    count, reserved_bytes = session.execute(statement).one()
    return int(count), int(reserved_bytes)


def readiness_queue_stats(workspace: str) -> list[dict[str, Any]]:
    """Returns bounded queue and capacity aggregates without provider I/O."""
    result_cap = 10_000
    shards = schema.registry_shards
    pending_states = (
        models.ImageLocationState.PENDING.value,
        models.ImageLocationState.COPYING.value,
        models.ImageLocationState.VERIFYING.value,
        models.ImageLocationState.MISSING.value,
        models.ImageLocationState.EVICTED.value,
    )
    capacity_statement = sqlalchemy.select(
        shards.c.profile,
        shards.c.target_id,
        shards.c.account,
        shards.c.region,
        sqlalchemy.func.sum(
            shards.c.reserved_manifests).label('reserved_manifests'),
        sqlalchemy.func.sum(shards.c.max_manifests).label('max_manifests'),
        sqlalchemy.func.sum(
            shards.c.reserved_declared_bytes).label('reserved_declared_bytes'),
        sqlalchemy.func.sum(
            shards.c.max_declared_bytes).label('max_declared_bytes'),
        sqlalchemy.func.sum(shards.c.in_flight).label('in_flight'),
        sqlalchemy.func.sum(shards.c.max_in_flight).label('max_in_flight'),
        sqlalchemy.func.array_agg(shards.c.id).label('shard_ids'),
    ).where(shards.c.workspace == workspace).group_by(
        shards.c.profile, shards.c.target_id, shards.c.account,
        shards.c.region).order_by(shards.c.profile, shards.c.target_id)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(capacity_statement).mappings().all()
        results: list[dict[str, Any]] = []
        for row in rows:
            shard_ids = [str(item) for item in row['shard_ids']]
            queued_count = _bounded_location_count(session, shard_ids,
                                                   pending_states, result_cap)
            failed_count = _bounded_location_count(
                session, shard_ids,
                (models.ImageLocationState.FAILED.value,
                 models.ImageLocationState.QUARANTINED.value), result_cap)
            quarantined_count, quarantined_bytes = (
                _bounded_location_reservation_stats(
                    session, shard_ids,
                    models.ImageLocationState.QUARANTINED.value, result_cap))
            # The minimum of each shard/state index head is the exact global
            # oldest row, without scanning or sorting the complete queue.
            oldest_statement = sqlalchemy.text("""
                SELECT MIN(candidate.updated_at)
                FROM unnest(CAST(:shard_ids AS text[])) AS shard(id)
                CROSS JOIN unnest(CAST(:states AS text[])) AS selected(state)
                CROSS JOIN LATERAL (
                    SELECT location.updated_at
                    FROM container_image_locations AS location
                    WHERE location.shard_id = shard.id
                      AND location.state = selected.state
                    ORDER BY location.updated_at, location.id
                    LIMIT 1
                ) AS candidate
            """).bindparams(
                sqlalchemy.bindparam('shard_ids',
                                     type_=postgresql.ARRAY(sqlalchemy.Text())),
                sqlalchemy.bindparam('states',
                                     type_=postgresql.ARRAY(sqlalchemy.Text())))
            oldest_queued_at = session.execute(oldest_statement, {
                'shard_ids': shard_ids,
                'states': list(pending_states),
            }).scalar_one()
            results.append({
                'profile': str(row['profile']),
                'target': str(row['target_id']),
                'account': str(row['account']),
                'region': str(row['region']),
                'reserved_manifests': int(row['reserved_manifests'] or 0),
                'max_manifests': int(row['max_manifests'] or 0),
                'reserved_declared_bytes': int(row['reserved_declared_bytes'] or
                                               0),
                'max_declared_bytes': int(row['max_declared_bytes'] or 0),
                'in_flight': int(row['in_flight'] or 0),
                'max_in_flight': int(row['max_in_flight'] or 0),
                'queue_depth': min(queued_count, result_cap),
                'queue_depth_at_least': queued_count > result_cap,
                'failed_count': min(failed_count, result_cap),
                'failed_count_at_least': failed_count > result_cap,
                'quarantined_count': min(quarantined_count, result_cap),
                'quarantined_count_at_least': quarantined_count > result_cap,
                'quarantined_reserved_declared_bytes': quarantined_bytes,
                'quarantined_reserved_declared_bytes_at_least':
                    quarantined_count > result_cap,
                'oldest_queued_at': oldest_queued_at,
            })
    return results


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
    """Claims via oldest eligible target shard, then its oldest location."""
    current = int(time.time()) if now is None else now
    shards = schema.registry_shards
    locations = schema.locations
    fresh = sqlalchemy.and_(
        locations.c.state == models.ImageLocationState.PENDING.value,
        sqlalchemy.or_(locations.c.next_retry_at.is_(None),
                       locations.c.next_retry_at <= current))
    expired = sqlalchemy.and_(
        locations.c.state.in_([
            models.ImageLocationState.COPYING.value,
            models.ImageLocationState.VERIFYING.value,
        ]), locations.c.lease_expires_at <= current)
    eligible_location = sqlalchemy.or_(fresh, expired)
    accepts_reserved_work = shards.c.state.in_([
        models.ImageShardState.READY.value,
        models.ImageShardState.FULL.value,
    ])
    recovers_expired_work = shards.c.state.in_([
        models.ImageShardState.READY.value,
        models.ImageShardState.FULL.value,
        models.ImageShardState.DRIFTED.value,
        models.ImageShardState.DISABLED.value,
    ])
    shard_statement = sqlalchemy.select(shards).where(
        sqlalchemy.or_(
            sqlalchemy.and_(
                accepts_reserved_work, shards.c.in_flight
                < shards.c.max_in_flight,
                sqlalchemy.exists().where(locations.c.shard_id == shards.c.id,
                                          fresh)),
            sqlalchemy.and_(
                recovers_expired_work,
                sqlalchemy.exists().where(locations.c.shard_id == shards.c.id,
                                          expired))))
    if workspace is not None:
        shard_statement = shard_statement.where(shards.c.workspace == workspace)
    shard_statement = shard_statement.order_by(
        shards.c.last_dispatch_at.asc().nullsfirst(),
        shards.c.id).limit(1).with_for_update(skip_locked=True)
    token = f'{worker_id}:{uuid.uuid4()}'
    with orm.Session(catalog_state.engine()) as session, session.begin():
        shard_row = session.execute(shard_statement).mappings().first()
        if shard_row is None:
            return None
        location_row = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.shard_id == shard_row['id'],
                eligible_location).order_by(
                    sqlalchemy.case((expired, 0),
                                    else_=1), locations.c.updated_at,
                    locations.c.id).limit(1).with_for_update(
                        skip_locked=True)).mappings().first()
        if location_row is None:
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
        return _location(row)


def heartbeat_location(location_id: str,
                       lease_token: str,
                       lease_seconds: int,
                       *,
                       now: int | None = None) -> bool:
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.locations.update().where(
            schema.locations.c.id == location_id,
            schema.locations.c.lease_token == lease_token,
            schema.locations.c.lease_expires_at > current,
            schema.locations.c.state.in_([
                models.ImageLocationState.COPYING.value,
                models.ImageLocationState.VERIFYING.value,
                models.ImageLocationState.EVICTING.value,
            ])).values(lease_expires_at=current + lease_seconds,
                       updated_at=current)).rowcount
    return changed == 1


def begin_eviction_delete(location_id: str,
                          lease_token: str,
                          *,
                          now: int | None = None) -> bool:
    """Durably records that the fenced ECR delete may now begin."""
    current = int(time.time()) if now is None else now
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(locations.update().where(
            locations.c.id == location_id,
            locations.c.state == models.ImageLocationState.EVICTING.value,
            locations.c.lease_kind == 'EVICT',
            locations.c.lease_token == lease_token, locations.c.lease_expires_at
            > current).values(lease_kind='DELETE', updated_at=current)).rowcount
    return changed == 1


def transition_location_to_verifying(location_id: str,
                                     lease_token: str,
                                     *,
                                     ambiguous: bool = False,
                                     now: int | None = None) -> bool:
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.locations.update().where(
            schema.locations.c.id == location_id,
            schema.locations.c.state == models.ImageLocationState.COPYING.value,
            schema.locations.c.lease_token == lease_token,
            schema.locations.c.lease_expires_at
            > current).values(state=models.ImageLocationState.VERIFYING.value,
                              lease_kind='VERIFY',
                              error_code=(models.ImageLocationErrorCode.
                                          PROVIDER_OUTCOME_AMBIGUOUS.value
                                          if ambiguous else None),
                              updated_at=current)).rowcount
    return changed == 1


def _finish_location(session: orm.Session, *, location_id: str,
                     lease_token: str, state: models.ImageLocationState,
                     error_code: str | None, next_retry_at: int | None,
                     now: int) -> sqlalchemy.engine.RowMapping | None:
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
            row['lease_expires_at'] is None or
            int(row['lease_expires_at']) <= now or str(row['state'])
            not in (models.ImageLocationState.COPYING.value,
                    models.ImageLocationState.VERIFYING.value)):
        return None
    updated = session.execute(
        locations.update().where(locations.c.id == location_id).values(
            state=state.value,
            lease_kind=None,
            lease_token=None,
            lease_expires_at=None,
            error_code=error_code,
            next_retry_at=next_retry_at,
            last_verified_at=(now if state == models.ImageLocationState.READY
                              else row['last_verified_at']),
            updated_at=now).returning(locations)).mappings().one()
    session.execute(schema.registry_shards.update().where(
        schema.registry_shards.c.id == row['shard_id'],
        schema.registry_shards.c.in_flight
        > 0).values(in_flight=schema.registry_shards.c.in_flight - 1,
                    updated_at=now))
    return updated


def complete_location_ready(location_id: str,
                            lease_token: str,
                            *,
                            now: int | None = None) -> LocationRecord | None:
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = _finish_location(session,
                               location_id=location_id,
                               lease_token=lease_token,
                               state=models.ImageLocationState.READY,
                               error_code=None,
                               next_retry_at=None,
                               now=current)
        return _location(row) if row is not None else None


def fail_location(location_id: str,
                  lease_token: str,
                  *,
                  error_code: str,
                  terminal: bool,
                  retry_at: int | None,
                  now: int | None = None) -> LocationRecord | None:
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = _finish_location(
            session,
            location_id=location_id,
            lease_token=lease_token,
            state=(models.ImageLocationState.FAILED
                   if terminal else models.ImageLocationState.PENDING),
            error_code=error_code,
            next_retry_at=None if terminal else retry_at,
            now=current)
        return _location(row) if row is not None else None


def retry_location(location_id: str,
                   workspace: str,
                   *,
                   now: int | None = None) -> LocationRecord | None:
    current = int(time.time()) if now is None else now
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
        return _location(updated)


def register_worker(worker_id: str,
                    kind: models.ImageWorkerKind,
                    version: str,
                    max_in_flight: int,
                    *,
                    now: int | None = None) -> WorkerRecord:
    current = int(time.time()) if now is None else now
    table = schema.workers
    with orm.Session(catalog_state.engine()) as session, session.begin():
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
    current = int(time.time()) if now is None else now
    if (not isinstance(in_flight, int) or isinstance(in_flight, bool) or
            in_flight < 0):
        return False
    values: dict[str, Any] = {
        'heartbeat_at': current,
        'in_flight': in_flight,
    }
    if success:
        values['last_success_at'] = current
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.workers.update().where(
            schema.workers.c.id == worker_id, in_flight
            <= schema.workers.c.max_in_flight).values(**values)).rowcount
    return changed == 1


def _quarantine_eviction(
    session: orm.Session,
    row: sqlalchemy.engine.RowMapping,
    *,
    now: int,
) -> LocationRecord:
    """Fails one possibly in-flight delete closed without releasing capacity."""
    locations = schema.locations
    shards = schema.registry_shards
    updated = session.execute(
        locations.update().where(locations.c.id == row['id']).values(
            state=models.ImageLocationState.QUARANTINED.value,
            lease_kind=None,
            lease_token=None,
            lease_expires_at=None,
            next_retry_at=None,
            error_code=(
                models.ImageLocationErrorCode.PROVIDER_OUTCOME_AMBIGUOUS.value),
            updated_at=now).returning(locations)).mappings().one()
    changed = session.execute(shards.update().where(
        shards.c.id == row['shard_id'], shards.c.in_flight
        > 0).values(in_flight=shards.c.in_flight - 1, updated_at=now)).rowcount
    if changed != 1:
        raise RuntimeError('Eviction quarantine accounting drifted.')
    return _location(updated)


def claim_next_eviction(*,
                        worker_id: str,
                        unused_before: int,
                        workspace_unused_before: dict[str, int | None] |
                        None = None,
                        lease_seconds: int,
                        now: int | None = None) -> LocationRecord | None:
    """Claims one demand-free regional digest after its retention window."""
    current = int(time.time()) if now is None else now
    locations = schema.locations
    demands = schema.demands
    age_anchor = sqlalchemy.func.coalesce(locations.c.last_used_at,
                                          locations.c.last_verified_at,
                                          locations.c.created_at)
    workspace_cutoffs = workspace_unused_before or {}
    for workspace, cutoff in workspace_cutoffs.items():
        if (not isinstance(workspace, str) or not workspace or
            (cutoff is not None and
             (not isinstance(cutoff, int) or isinstance(cutoff, bool)))):
            raise ValueError('Workspace eviction cutoff is invalid.')
    if workspace_cutoffs:
        configured = tuple(workspace_cutoffs)
        due_conditions = [
            sqlalchemy.and_(locations.c.workspace == workspace, age_anchor
                            < cutoff)
            for workspace, cutoff in workspace_cutoffs.items()
            if cutoff is not None
        ]
        retention_due = sqlalchemy.or_(
            sqlalchemy.and_(locations.c.workspace.not_in(configured), age_anchor
                            < unused_before), *due_conditions)
    else:
        retention_due = age_anchor < unused_before
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
        locations.c.lease_expires_at <= current)
    token = f'{worker_id}:{uuid.uuid4()}'
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        claimable_for_shard = sqlalchemy.or_(
            sqlalchemy.and_(ready_due, shards.c.eviction_enabled.is_(True),
                            shards.c.in_flight < shards.c.max_in_flight),
            expired)
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
        reclaimed = (str(
            row['state']) == models.ImageLocationState.EVICTING.value)
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
        if reclaimed and row['lease_kind'] != 'EVICT':
            # DELETE means an old request may still resume or complete after
            # any later read. Never restore or recopy this physical reference.
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
    current = int(time.time()) if now is None else now
    locations = schema.locations
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(locations.c.shard_id).where(
                locations.c.id == location_id)).first()
        if optimistic is None:
            return None
        session.execute(
            sqlalchemy.select(shards.c.id).where(
                shards.c.id == optimistic[0]).with_for_update()).one()
        row = session.execute(
            sqlalchemy.select(locations).where(locations.c.id == location_id).
            with_for_update()).mappings().first()
        if (row is None or
                str(row['state']) != models.ImageLocationState.EVICTING.value or
                row['lease_token'] != lease_token or
                row['lease_expires_at'] is None or
                int(row['lease_expires_at']) <= current):
            return None
        if provider_not_called and row['lease_kind'] != 'EVICT':
            return None
        if not provider_not_called and row['lease_kind'] != 'DELETE':
            # Provider results are inadmissible unless the same lease first
            # committed its durable destructive intent.
            return None
        live_demand = session.execute(
            sqlalchemy.select(schema.demands.c.id).where(
                schema.demands.c.location_id == location_id,
                schema.demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                ])).limit(1)).first()
        if present is None and not provider_not_called:
            return _quarantine_eviction(session, row, now=current)
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
        updated = session.execute(
            locations.update().where(locations.c.id == location_id).values(
                **location_values).returning(locations)).mappings().one()
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
    current = int(time.time()) if now is None else now
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
                                   schema.workers.c.id).limit(limit)
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
                    'refilled_at': now,
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
    current = int(time.time()) if now is None else now
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
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
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
    current = int(time.time()) if now is None else now
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
        if (budget['blocked_until'] is not None and
                int(budget['blocked_until']) > current):
            return None
        if (worker['grant_expires_at'] is not None and
                int(worker['grant_expires_at']) > current and
                int(worker['grant_tokens_milli']) > 0):
            if str(worker['grant_budget_id']) != budget_id:
                return None
            return ProviderGrant(budget_id=str(worker['grant_budget_id']),
                                 tokens=int(worker['grant_tokens_milli']) //
                                 1000,
                                 expires_at=int(worker['grant_expires_at']))
        elapsed = max(0, current - int(budget['refilled_at']))
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
                                                  refilled_at=current,
                                                  updated_at=current))
            return None
        expires_at = current + grant_seconds
        session.execute(budgets.update().where(
            budgets.c.id == budget_id).values(tokens_milli=available - granted,
                                              refilled_at=current,
                                              updated_at=current))
        session.execute(workers.update().where(
            workers.c.id == worker_id).values(grant_budget_id=budget_id,
                                              grant_tokens_milli=granted,
                                              grant_expires_at=expires_at,
                                              heartbeat_at=current))
        return ProviderGrant(budget_id=budget_id,
                             tokens=granted // 1000,
                             expires_at=expires_at)


def record_provider_throttle(budget_id: str,
                             *,
                             now: int | None = None) -> int | None:
    current = int(time.time()) if now is None else now
    budgets = schema.provider_budgets
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(budgets).where(budgets.c.id == budget_id).
            with_for_update()).mappings().first()
        if row is None:
            return None
        count = int(row['throttle_count']) + 1
        delay = min(2**min(count, 8), 300)
        blocked_until = current + delay
        session.execute(budgets.update().where(
            budgets.c.id == budget_id).values(throttle_count=count,
                                              blocked_until=blocked_until,
                                              updated_at=current))
        return blocked_until


def compact_stale_workers(*, older_than: int, limit: int = 500) -> int:
    table = schema.workers
    with orm.Session(catalog_state.engine()) as session, session.begin():
        worker_ids = session.execute(
            sqlalchemy.select(
                table.c.id).where(table.c.heartbeat_at < older_than).order_by(
                    table.c.heartbeat_at,
                    table.c.id).limit(limit).with_for_update(
                        skip_locked=True)).scalars().all()
        if not worker_ids:
            return 0
        return session.execute(table.delete().where(
            table.c.id.in_(worker_ids))).rowcount
