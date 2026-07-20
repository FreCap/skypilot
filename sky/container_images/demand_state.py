"""Durable image demand fences and consumer-generation watermarks."""
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

_TERMINAL_CONFIRMATION_SECONDS = 60 * 60
_TERMINAL_RETENTION_SECONDS = 30 * 24 * 60 * 60
_MAX_POSTGRES_BIGINT = (1 << 63) - 1


class StaleConsumerGenerationError(ValueError):
    """A replay attempted to resurrect a terminal or older generation."""


@dataclasses.dataclass(frozen=True)
class DemandRecord:
    id: str
    authority_id: str
    workspace: str
    consumer_kind: str
    consumer_owner: str
    request_id: str | None
    consumer_generation: int
    target_key: str
    owner_epoch: int
    retry_epoch: int
    image_id: str
    runtime_digest: str
    profile_revision_id: str
    target_fingerprint: str
    location_id: str
    placement: dict[str, Any]
    pull_plan: dict[str, Any] | None
    state: models.ImageDemandState
    error_code: str | None
    consumer_attached: bool
    first_terminal_observed_at: int | None
    last_terminal_observed_at: int | None
    terminal_observation_count: int
    terminal_at: int | None
    expires_at: int | None
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class ConsumerWatermark:
    workspace: str
    consumer_kind: str
    consumer_owner: str
    max_seen_generation: int
    max_terminal_generation: int
    owner_deleted_at: int | None
    credential_expires_at: int | None
    created_at: int
    updated_at: int


def owner_epoch_from_token(token: str) -> int:
    """Maps a stable request/controller token to a non-secret bigint fence."""
    if not isinstance(token, str) or not token or len(token) > 1024:
        raise ValueError('Demand owner epoch token is invalid.')
    digest = hashlib.sha256(token.encode()).digest()
    return int.from_bytes(digest[:8], byteorder='big') & _MAX_POSTGRES_BIGINT


def _demand(row: sqlalchemy.engine.RowMapping) -> DemandRecord:
    pull_plan = row['pull_plan_json']
    return DemandRecord(
        id=str(row['id']),
        authority_id=str(row['authority_id']),
        workspace=str(row['workspace']),
        consumer_kind=str(row['consumer_kind']),
        consumer_owner=str(row['consumer_owner']),
        request_id=row['request_id'],
        consumer_generation=int(row['consumer_generation']),
        target_key=str(row['target_key']),
        owner_epoch=int(row['owner_epoch']),
        retry_epoch=int(row['retry_epoch']),
        image_id=str(row['image_id']),
        runtime_digest=str(row['runtime_digest']),
        profile_revision_id=str(row['profile_revision_id']),
        target_fingerprint=str(row['target_fingerprint']),
        location_id=str(row['location_id']),
        placement=json.loads(str(row['placement_json'])),
        pull_plan=json.loads(str(pull_plan)) if pull_plan is not None else None,
        state=models.ImageDemandState(str(row['state'])),
        error_code=row['error_code'],
        consumer_attached=bool(row['consumer_attached']),
        first_terminal_observed_at=row['first_terminal_observed_at'],
        last_terminal_observed_at=row['last_terminal_observed_at'],
        terminal_observation_count=int(row['terminal_observation_count']),
        terminal_at=row['terminal_at'],
        expires_at=row['expires_at'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def _watermark(row: sqlalchemy.engine.RowMapping) -> ConsumerWatermark:
    return ConsumerWatermark(
        workspace=str(row['workspace']),
        consumer_kind=str(row['consumer_kind']),
        consumer_owner=str(row['consumer_owner']),
        max_seen_generation=int(row['max_seen_generation']),
        max_terminal_generation=int(row['max_terminal_generation']),
        owner_deleted_at=row['owner_deleted_at'],
        credential_expires_at=row['credential_expires_at'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def _lock_watermark(session: orm.Session, *, workspace: str, consumer_kind: str,
                    consumer_owner: str, generation: int,
                    now: int) -> sqlalchemy.engine.RowMapping:
    table = schema.consumer_watermarks
    session.execute(
        postgresql.insert(table).values(
            workspace=workspace,
            consumer_kind=consumer_kind,
            consumer_owner=consumer_owner,
            max_seen_generation=generation,
            max_terminal_generation=-1,
            created_at=now,
            updated_at=now).on_conflict_do_nothing(index_elements=[
                table.c.workspace, table.c.consumer_kind, table.c.consumer_owner
            ]))
    return session.execute(
        sqlalchemy.select(table).where(
            table.c.workspace == workspace,
            table.c.consumer_kind == consumer_kind, table.c.consumer_owner ==
            consumer_owner).with_for_update()).mappings().one()


def create_demand_in_session(session: orm.Session, *, authority_id: str,
                             workspace: str, consumer_kind: str,
                             consumer_owner: str, consumer_generation: int,
                             target_key: str, owner_epoch: int, image_id: str,
                             runtime_digest: str, profile_revision_id: str,
                             target_fingerprint: str, location_id: str,
                             placement: dict[str,
                                             Any], now: int) -> DemandRecord:
    """Creates one WARMING demand while enforcing the owner high watermark."""
    watermark = _lock_watermark(session,
                                workspace=workspace,
                                consumer_kind=consumer_kind,
                                consumer_owner=consumer_owner,
                                generation=consumer_generation,
                                now=now)
    max_seen = int(watermark['max_seen_generation'])
    max_terminal = int(watermark['max_terminal_generation'])
    if consumer_generation < max_seen or consumer_generation <= max_terminal:
        raise StaleConsumerGenerationError(
            'Consumer generation is older than its durable watermark.')
    table = schema.demands
    existing = session.execute(
        sqlalchemy.select(table).where(
            table.c.workspace == workspace,
            table.c.consumer_kind == consumer_kind,
            table.c.consumer_owner == consumer_owner,
            table.c.consumer_generation == consumer_generation,
            table.c.target_key ==
            target_key).with_for_update()).mappings().first()
    if existing is not None:
        immutable = {
            'authority_id': authority_id,
            'owner_epoch': owner_epoch,
            'image_id': image_id,
            'runtime_digest': runtime_digest,
            'profile_revision_id': profile_revision_id,
            'target_fingerprint': target_fingerprint,
            'location_id': location_id,
        }
        if any(
                str(existing[key]) != str(value)
                for key, value in immutable.items()):
            raise ValueError(
                'A consumer generation cannot change image target.')
        return _demand(existing)
    if consumer_generation > max_seen:
        session.execute(schema.consumer_watermarks.update().where(
            schema.consumer_watermarks.c.workspace == workspace,
            schema.consumer_watermarks.c.consumer_kind == consumer_kind,
            schema.consumer_watermarks.c.consumer_owner ==
            consumer_owner).values(max_seen_generation=consumer_generation,
                                   updated_at=now))
    placement_json = json.dumps(placement,
                                sort_keys=True,
                                separators=(',', ':'))
    if len(placement_json.encode()) > 8192:
        raise ValueError('Demand placement constraints exceed 8 KiB.')
    request_id = None
    consumer_metadata = placement.get('consumer')
    if consumer_kind == 'cluster' and isinstance(consumer_metadata, dict):
        candidate_request_id = consumer_metadata.get('request_id')
        if candidate_request_id is not None:
            if (not isinstance(candidate_request_id, str) or
                    not candidate_request_id or
                    len(candidate_request_id) > 1024):
                raise ValueError('Cluster demand request ID is invalid.')
            request_id = candidate_request_id
    row = session.execute(table.insert().values(
        id=str(uuid.uuid4()),
        authority_id=authority_id,
        workspace=workspace,
        consumer_kind=consumer_kind,
        consumer_owner=consumer_owner,
        request_id=request_id,
        consumer_generation=consumer_generation,
        target_key=target_key,
        owner_epoch=owner_epoch,
        retry_epoch=0,
        image_id=image_id,
        runtime_digest=runtime_digest,
        profile_revision_id=profile_revision_id,
        target_fingerprint=target_fingerprint,
        location_id=location_id,
        placement_json=placement_json,
        state=models.ImageDemandState.WARMING.value,
        consumer_attached=False,
        created_at=now,
        updated_at=now).returning(table)).mappings().one()
    return _demand(row)


def create_demand(**kwargs: Any) -> DemandRecord:
    current = int(kwargs.pop('now', time.time()))
    with orm.Session(catalog_state.engine()) as session, session.begin():
        return create_demand_in_session(session, now=current, **kwargs)


def create_demand_for_owner_epoch_in_session(
        session: orm.Session,
        *,
        authority_id: str,
        workspace: str,
        consumer_kind: str,
        consumer_owner: str,
        target_key: str,
        owner_epoch: int,
        image_id: str,
        runtime_digest: str,
        profile_revision_id: str,
        target_fingerprint: str,
        location_id: str,
        placement: dict[str, Any],
        now: int,
        require_existing: bool = False) -> DemandRecord:
    """Converges request replay or allocates the next durable generation."""
    watermarks = schema.consumer_watermarks
    inserted = session.execute(
        postgresql.insert(watermarks).values(
            workspace=workspace,
            consumer_kind=consumer_kind,
            consumer_owner=consumer_owner,
            max_seen_generation=0,
            max_terminal_generation=-1,
            created_at=now,
            updated_at=now).on_conflict_do_nothing(index_elements=[
                watermarks.c.workspace, watermarks.c.consumer_kind,
                watermarks.c.consumer_owner
            ]).returning(watermarks.c.consumer_owner)).first()
    watermark = session.execute(
        sqlalchemy.select(watermarks).where(
            watermarks.c.workspace == workspace,
            watermarks.c.consumer_kind == consumer_kind,
            watermarks.c.consumer_owner ==
            consumer_owner).with_for_update()).mappings().one()
    existing_rows = session.execute(
        sqlalchemy.select(schema.demands).where(
            schema.demands.c.workspace == workspace,
            schema.demands.c.consumer_kind == consumer_kind,
            schema.demands.c.consumer_owner == consumer_owner,
            schema.demands.c.owner_epoch == owner_epoch,
            schema.demands.c.state.in_([
                models.ImageDemandState.WARMING.value,
                models.ImageDemandState.READY.value,
                models.ImageDemandState.FAILED.value,
            ])).order_by(schema.demands.c.consumer_generation.desc()).limit(
                2).with_for_update()).mappings().all()
    if len(existing_rows) > 1:
        raise RuntimeError('A consumer epoch has multiple live image demands.')
    existing = existing_rows[0] if existing_rows else None
    if existing is not None:
        immutable = {
            'authority_id': authority_id,
            'target_key': target_key,
            'image_id': image_id,
            'runtime_digest': runtime_digest,
            'profile_revision_id': profile_revision_id,
            'target_fingerprint': target_fingerprint,
            'location_id': location_id,
        }
        if any(
                str(existing[key]) != str(value)
                for key, value in immutable.items()):
            raise ValueError('A demand owner epoch cannot change image target.')
        return _demand(existing)
    if require_existing:
        raise StaleConsumerGenerationError(
            'A retired profile revision accepts only an exact live replay.')
    generation = (0 if inserted is not None else
                  int(watermark['max_seen_generation']) + 1)
    if generation > _MAX_POSTGRES_BIGINT:
        raise ValueError('Consumer generation is exhausted.')
    return create_demand_in_session(session,
                                    authority_id=authority_id,
                                    workspace=workspace,
                                    consumer_kind=consumer_kind,
                                    consumer_owner=consumer_owner,
                                    consumer_generation=generation,
                                    target_key=target_key,
                                    owner_epoch=owner_epoch,
                                    image_id=image_id,
                                    runtime_digest=runtime_digest,
                                    profile_revision_id=profile_revision_id,
                                    target_fingerprint=target_fingerprint,
                                    location_id=location_id,
                                    placement=placement,
                                    now=now)


def get_demand(demand_id: str, workspace: str) -> DemandRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id,
                schema.demands.c.workspace == workspace)).mappings().first()
    return _demand(row) if row is not None else None


def get_live_demand(*, workspace: str, consumer_kind: str, consumer_owner: str,
                    consumer_generation: int,
                    target_key: str) -> DemandRecord | None:
    with orm.Session(catalog_state.engine()) as session:
        row = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.workspace == workspace,
                schema.demands.c.consumer_kind == consumer_kind,
                schema.demands.c.consumer_owner == consumer_owner,
                schema.demands.c.consumer_generation == consumer_generation,
                schema.demands.c.target_key == target_key,
                schema.demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                    models.ImageDemandState.FAILED.value,
                ]))).mappings().first()
    return _demand(row) if row is not None else None


def get_current_demand_for_owner_epoch(*, workspace: str, consumer_kind: str,
                                       consumer_owner: str,
                                       owner_epoch: int) -> DemandRecord | None:
    """Returns the sole live target fence for a restart-stable owner epoch."""
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.workspace == workspace,
                schema.demands.c.consumer_kind == consumer_kind,
                schema.demands.c.consumer_owner == consumer_owner,
                schema.demands.c.owner_epoch == owner_epoch,
                schema.demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                    models.ImageDemandState.FAILED.value,
                ])).order_by(schema.demands.c.consumer_generation.desc()).limit(
                    2)).mappings().all()
    if len(rows) > 1:
        raise RuntimeError('A consumer epoch has multiple live image demands.')
    return _demand(rows[0]) if rows else None


def list_demands(image_id: str,
                 workspace: str,
                 *,
                 limit: int = 50,
                 after: tuple[int, str] | None = None) -> list[DemandRecord]:
    statement = sqlalchemy.select(schema.demands).where(
        schema.demands.c.image_id == image_id,
        schema.demands.c.workspace == workspace)
    if after is not None:
        statement = statement.where(
            sqlalchemy.tuple_(schema.demands.c.created_at, schema.demands.c.id)
            < after)
    statement = statement.order_by(schema.demands.c.created_at.desc(),
                                   schema.demands.c.id.desc()).limit(limit)
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(statement).mappings().all()
    return [_demand(row) for row in rows]


def attach_consumer(demand_id: str,
                    workspace: str,
                    *,
                    now: int | None = None) -> bool:
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.demands.update().where(
            schema.demands.c.id == demand_id,
            schema.demands.c.workspace == workspace,
            schema.demands.c.state.in_([
                models.ImageDemandState.WARMING.value,
                models.ImageDemandState.READY.value,
            ])).values(consumer_attached=True, updated_at=current)).rowcount
    return changed == 1


def mark_demand_failed(demand_id: str,
                       error_code: str,
                       *,
                       now: int | None = None) -> bool:
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.demands.update().where(
            schema.demands.c.id == demand_id, schema.demands.c.state ==
            models.ImageDemandState.WARMING.value).values(
                state=models.ImageDemandState.FAILED.value,
                error_code=error_code,
                updated_at=current)).rowcount
    return changed == 1


def mark_cluster_request_terminal(request_id: str,
                                  *,
                                  now: int | None = None) -> int:
    """Records exact terminal request proof for unattached cluster demands."""
    if (not isinstance(request_id, str) or not request_id or
            len(request_id) > 1024):
        raise ValueError('Cluster demand request ID is invalid.')
    current = int(time.time()) if now is None else now
    demands = schema.demands
    with orm.Session(catalog_state.engine()) as session, session.begin():
        rows = session.execute(
            sqlalchemy.select(demands).where(
                demands.c.consumer_kind == 'cluster',
                demands.c.consumer_attached.is_(False),
                demands.c.request_id == request_id,
                demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                    models.ImageDemandState.FAILED.value,
                ])).with_for_update()).mappings().all()
        changed = 0
        for row in rows:
            values: dict[str, Any] = {
                'last_terminal_observed_at': current,
                'terminal_observation_count': max(
                    int(row['terminal_observation_count']), 1),
                'updated_at': current,
            }
            if row['first_terminal_observed_at'] is None:
                values['first_terminal_observed_at'] = current
            changed += session.execute(demands.update().where(
                demands.c.id == row['id']).values(**values)).rowcount
    return changed


def supersede_demand(demand_id: str,
                     workspace: str,
                     *,
                     now: int | None = None) -> bool:
    """Ends a demand only after the owning controller chose real failover."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id,
                schema.demands.c.workspace == workspace)).mappings().first()
        if optimistic is None:
            return False
        _lock_watermark(session,
                        workspace=str(optimistic['workspace']),
                        consumer_kind=str(optimistic['consumer_kind']),
                        consumer_owner=str(optimistic['consumer_owner']),
                        generation=int(optimistic['consumer_generation']),
                        now=current)
        row = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id, schema.demands.c.workspace ==
                workspace).with_for_update()).mappings().first()
        if row is None:
            return False
        if str(row['state']) in (models.ImageDemandState.SUPERSEDED.value,
                                 models.ImageDemandState.RELEASED.value):
            return True
        _terminalize(session,
                     row,
                     models.ImageDemandState.SUPERSEDED,
                     now=current)
        return True


def fail_and_supersede_demand(demand_id: str,
                              error_code: str,
                              *,
                              now: int | None = None) -> bool:
    """Atomically records terminal materialization failure and opens failover."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id)).mappings().first()
        if optimistic is None:
            return False
        _lock_watermark(session,
                        workspace=str(optimistic['workspace']),
                        consumer_kind=str(optimistic['consumer_kind']),
                        consumer_owner=str(optimistic['consumer_owner']),
                        generation=int(optimistic['consumer_generation']),
                        now=current)
        row = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id ==
                demand_id).with_for_update()).mappings().one()
        if str(row['state']) in (models.ImageDemandState.SUPERSEDED.value,
                                 models.ImageDemandState.RELEASED.value):
            return True
        session.execute(schema.demands.update().where(
            schema.demands.c.id == demand_id).values(error_code=error_code,
                                                     updated_at=current))
        _terminalize(session,
                     row,
                     models.ImageDemandState.SUPERSEDED,
                     now=current)
        return True


def list_consumer_reconciliation_candidates(*,
                                            older_than: int,
                                            limit: int = 500
                                           ) -> list[DemandRecord]:
    """Returns a bounded, fair page of live consumer fences to re-observe."""
    if not 1 <= limit <= 1000:
        raise ValueError('Demand reconciliation page size is invalid.')
    table = schema.demands
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                    models.ImageDemandState.FAILED.value,
                ]), table.c.updated_at <= older_than).order_by(
                    table.c.updated_at,
                    table.c.id).limit(limit)).mappings().all()
    return [_demand(row) for row in rows]


def defer_consumer_reconciliation(demand_id: str,
                                  *,
                                  now: int | None = None) -> bool:
    """Rotates a still-live consumer behind other bounded candidates."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.demands.update().where(
            schema.demands.c.id == demand_id,
            schema.demands.c.state.in_([
                models.ImageDemandState.WARMING.value,
                models.ImageDemandState.READY.value,
                models.ImageDemandState.FAILED.value,
            ])).values(updated_at=current)).rowcount
    return changed == 1


def _terminalize(session: orm.Session, row: sqlalchemy.engine.RowMapping,
                 state: models.ImageDemandState, *, now: int) -> None:
    demands = schema.demands
    watermarks = schema.consumer_watermarks
    session.execute(demands.update().where(demands.c.id == row['id']).values(
        state=state.value,
        consumer_attached=False,
        terminal_at=now,
        expires_at=now + _TERMINAL_RETENTION_SECONDS,
        updated_at=now))
    session.execute(watermarks.update().where(
        watermarks.c.workspace == row['workspace'],
        watermarks.c.consumer_kind == row['consumer_kind'],
        watermarks.c.consumer_owner == row['consumer_owner']).values(
            max_terminal_generation=sqlalchemy.func.greatest(
                watermarks.c.max_terminal_generation,
                row['consumer_generation']),
            updated_at=now))


def observe_consumer_terminal(demand_id: str,
                              workspace: str,
                              *,
                              authoritative: bool,
                              now: int | None = None) -> bool:
    """Requires two authoritative observations an hour apart before release."""
    if not authoritative:
        return False
    current = int(time.time()) if now is None else now
    demands = schema.demands
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(demands).where(
                demands.c.id == demand_id,
                demands.c.workspace == workspace)).mappings().first()
        if optimistic is None:
            return False
        _lock_watermark(session,
                        workspace=str(optimistic['workspace']),
                        consumer_kind=str(optimistic['consumer_kind']),
                        consumer_owner=str(optimistic['consumer_owner']),
                        generation=int(optimistic['consumer_generation']),
                        now=current)
        row = session.execute(
            sqlalchemy.select(demands).where(
                demands.c.id == demand_id, demands.c.workspace ==
                workspace).with_for_update()).mappings().first()
        if row is None:
            return False
        if str(row['state']) == models.ImageDemandState.RELEASED.value:
            return True
        if str(row['state']) == models.ImageDemandState.SUPERSEDED.value:
            return True
        first = row['first_terminal_observed_at']
        count = int(row['terminal_observation_count'])
        if first is None:
            session.execute(
                demands.update().where(demands.c.id == demand_id).values(
                    first_terminal_observed_at=current,
                    last_terminal_observed_at=current,
                    terminal_observation_count=1,
                    updated_at=current))
            return False
        if current - int(first) < _TERMINAL_CONFIRMATION_SECONDS:
            session.execute(
                demands.update().where(demands.c.id == demand_id).values(
                    last_terminal_observed_at=current,
                    terminal_observation_count=max(count, 1),
                    updated_at=current))
            return False
        session.execute(
            demands.update().where(demands.c.id == demand_id).values(
                last_terminal_observed_at=current,
                terminal_observation_count=max(count + 1, 2)))
        _terminalize(session,
                     row,
                     models.ImageDemandState.RELEASED,
                     now=current)
        return True


def mark_owner_deleted(*,
                       workspace: str,
                       consumer_kind: str,
                       consumer_owner: str,
                       credential_expires_at: int,
                       now: int | None = None) -> bool:
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        changed = session.execute(schema.consumer_watermarks.update().where(
            schema.consumer_watermarks.c.workspace == workspace,
            schema.consumer_watermarks.c.consumer_kind == consumer_kind,
            schema.consumer_watermarks.c.consumer_owner ==
            consumer_owner).values(owner_deleted_at=current,
                                   credential_expires_at=credential_expires_at,
                                   updated_at=current)).rowcount
    return changed == 1


def compact_terminal_demands(*,
                             now: int | None = None,
                             limit: int = 500) -> tuple[int, int]:
    """Compacts tombstones only behind a nonresurrection watermark proof."""
    current = int(time.time()) if now is None else now
    demands = schema.demands
    watermarks = schema.consumer_watermarks
    with orm.Session(catalog_state.engine()) as session, session.begin():
        candidate_rows = session.execute(
            sqlalchemy.select(demands).where(
                demands.c.state.in_([
                    models.ImageDemandState.SUPERSEDED.value,
                    models.ImageDemandState.RELEASED.value,
                ]), demands.c.expires_at
                <= current).order_by(demands.c.expires_at,
                                     demands.c.id).limit(limit).with_for_update(
                                         skip_locked=True)).mappings().all()
        demand_ids: list[str] = []
        for row in candidate_rows:
            watermark = session.execute(
                sqlalchemy.select(watermarks).where(
                    watermarks.c.workspace == row['workspace'],
                    watermarks.c.consumer_kind == row['consumer_kind'],
                    watermarks.c.consumer_owner ==
                    row['consumer_owner']).with_for_update()).mappings().one()
            credential_expiry = watermark['credential_expires_at']
            if (watermark['owner_deleted_at'] is not None and
                    credential_expiry is not None and
                    int(credential_expiry) <= current and
                    int(watermark['max_terminal_generation']) >= int(
                        row['consumer_generation'])):
                demand_ids.append(str(row['id']))
        deleted_demands = 0
        if demand_ids:
            deleted_demands = session.execute(demands.delete().where(
                demands.c.id.in_(demand_ids))).rowcount
        watermark_keys = session.execute(
            sqlalchemy.select(watermarks).where(
                watermarks.c.owner_deleted_at.is_not(None),
                watermarks.c.credential_expires_at <= current,
                ~sqlalchemy.exists().where(
                    demands.c.workspace == watermarks.c.workspace,
                    demands.c.consumer_kind == watermarks.c.consumer_kind,
                    demands.c.consumer_owner == watermarks.c.consumer_owner)).
            limit(limit).with_for_update(skip_locked=True)).mappings().all()
        deleted_watermarks = 0
        for row in watermark_keys:
            deleted_watermarks += session.execute(watermarks.delete().where(
                watermarks.c.workspace == row['workspace'],
                watermarks.c.consumer_kind == row['consumer_kind'],
                watermarks.c.consumer_owner == row['consumer_owner'])).rowcount
    return deleted_demands, deleted_watermarks
