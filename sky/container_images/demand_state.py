"""Durable image demand fences and consumer-generation watermarks."""
# pylint: disable=missing-class-docstring

from __future__ import annotations

from collections.abc import Callable
import dataclasses
import hashlib
import json
import re
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.container_images import catalog_state
from sky.container_images import models
from sky.container_images import schema

_TERMINAL_CONFIRMATION_SECONDS = 60 * 60
_UNATTACHED_REQUEST_RETENTION_SECONDS = 24 * 60 * 60
_TERMINAL_RETENTION_SECONDS = 30 * 24 * 60 * 60
_MAX_POSTGRES_BIGINT = (1 << 63) - 1
_MAX_SERVICE_VERSION_DEMAND_EVIDENCE_ROWS = 10000


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
    controller_epoch: str
    controller_sequence: int | None
    owner_epoch: int
    max_seen_generation: int
    max_terminal_generation: int
    owner_deleted_at: int | None
    created_at: int
    updated_at: int


@dataclasses.dataclass(frozen=True)
class LiveServiceVersionDemandEvidence:
    """Bounded exact-incarnation image-demand proof for Serve retirement."""

    count: int
    digest: str


def validate_controller_epoch(value: str) -> str:
    """Validates one durable, non-secret controller incarnation ID."""
    if (not isinstance(value, str) or not value or len(value) > 1024 or
            any(character.isspace() for character in value)):
        raise ValueError('Demand controller epoch is invalid.')
    return value


def validate_controller_sequence(value: int | None) -> int | None:
    """Validates an optional monotonic sequence owned by the controller."""
    if value is not None and (not isinstance(value, int) or isinstance(
            value, bool) or value < 0 or value > _MAX_POSTGRES_BIGINT):
        raise ValueError('Demand controller sequence is invalid.')
    return value


def _encode_placement(placement: dict[str, Any]) -> str:
    """Validates and encodes the immutable v0 runtime placement fence."""
    required = {'provider', 'region', 'backend', 'platform'}
    identity_fields = {
        'runtime_binding_fingerprint',
        'host_image_id',
        'runtime_principal',
        'instance_profile',
        'kubernetes_cluster_arn',
        'kubernetes_node_role',
        'kubernetes_node_selector',
    }
    allowed = required | identity_fields | {'consumer'}
    if (not isinstance(placement, dict) or
            not required <= set(placement) <= allowed or
            placement['provider'] != 'aws' or
            placement['backend'] not in ('aws_vm', 'aws_eks')):
        raise ValueError('Demand placement constraints are invalid.')
    models.validate_control_plane_identifier(placement['region'],
                                             'Demand placement region')
    models.validate_oci_platform(placement['platform'],
                                 'Demand placement platform')
    fingerprint = placement.get('runtime_binding_fingerprint')
    if fingerprint is not None:
        models.validate_fingerprint(fingerprint,
                                    'Demand runtime binding fingerprint')
    for field in identity_fields - {
            'runtime_binding_fingerprint', 'kubernetes_node_selector'
    }:
        value = placement.get(field)
        if (value is not None and
            (not isinstance(value, str) or not value or len(value) > 2048 or
             any(character.isspace() for character in value))):
            raise ValueError('Demand runtime identity is invalid.')
    node_selector = placement.get('kubernetes_node_selector')
    if (node_selector is not None and
        (not isinstance(node_selector, list) or not node_selector or
         len(node_selector) > 16 or
         any(not isinstance(item, (list, tuple)) or len(item) != 2 or not all(
             isinstance(value, str)
             for value in item)
             for item in node_selector))):
        raise ValueError('Demand Kubernetes node selector is invalid.')
    consumer = placement.get('consumer')
    if consumer is not None and not isinstance(consumer, dict):
        raise ValueError('Demand consumer metadata must be an object.')
    try:
        encoded = json.dumps(placement, sort_keys=True, separators=(',', ':'))
    except (TypeError, ValueError) as error:
        raise ValueError('Demand placement constraints are invalid.') from error
    if len(encoded.encode()) > 8192:
        raise ValueError('Demand placement constraints exceed 8 KiB.')
    return encoded


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
        controller_epoch=str(row['controller_epoch']),
        controller_sequence=row['controller_sequence'],
        owner_epoch=int(row['owner_epoch']),
        max_seen_generation=int(row['max_seen_generation']),
        max_terminal_generation=int(row['max_terminal_generation']),
        owner_deleted_at=row['owner_deleted_at'],
        created_at=int(row['created_at']),
        updated_at=int(row['updated_at']),
    )


def _raise_if_owner_deleted(watermark: sqlalchemy.engine.RowMapping) -> None:
    if watermark['owner_deleted_at'] is not None:
        raise StaleConsumerGenerationError(
            'Consumer owner was authoritatively deleted.')


def _lock_watermark(session: orm.Session, *, workspace: str, consumer_kind: str,
                    consumer_owner: str, controller_epoch: str,
                    controller_sequence: int | None, owner_epoch: int,
                    generation: int, now: int) -> sqlalchemy.engine.RowMapping:
    controller_epoch = validate_controller_epoch(controller_epoch)
    controller_sequence = validate_controller_sequence(controller_sequence)
    table = schema.consumer_watermarks
    session.execute(
        postgresql.insert(table).values(
            workspace=workspace,
            consumer_kind=consumer_kind,
            consumer_owner=consumer_owner,
            controller_epoch=controller_epoch,
            controller_sequence=controller_sequence,
            owner_epoch=owner_epoch,
            max_seen_generation=generation,
            max_terminal_generation=-1,
            created_at=now,
            updated_at=now).on_conflict_do_nothing(index_elements=[
                table.c.workspace, table.c.consumer_kind, table.c.consumer_owner
            ]))
    row = session.execute(
        sqlalchemy.select(table).where(
            table.c.workspace == workspace,
            table.c.consumer_kind == consumer_kind, table.c.consumer_owner ==
            consumer_owner).with_for_update()).mappings().one()
    _raise_if_owner_deleted(row)
    if (str(row['controller_epoch']) != controller_epoch or
            row['controller_sequence'] != controller_sequence or
            int(row['owner_epoch']) != owner_epoch):
        raise StaleConsumerGenerationError(
            'Controller epoch no longer owns this consumer watermark.')
    return row


def _lock_existing_watermark(
        session: orm.Session, *, workspace: str, consumer_kind: str,
        consumer_owner: str) -> sqlalchemy.engine.RowMapping:
    return session.execute(
        sqlalchemy.select(schema.consumer_watermarks).where(
            schema.consumer_watermarks.c.workspace == workspace,
            schema.consumer_watermarks.c.consumer_kind == consumer_kind,
            schema.consumer_watermarks.c.consumer_owner ==
            consumer_owner).with_for_update()).mappings().one()


def create_demand_in_session(
        session: orm.Session,
        *,
        authority_id: str,
        workspace: str,
        consumer_kind: str,
        consumer_owner: str,
        consumer_generation: int,
        target_key: str,
        owner_epoch: int,
        image_id: str,
        runtime_digest: str,
        profile_revision_id: str,
        target_fingerprint: str,
        location_id: str,
        placement: dict[str, Any],
        now: int | None = None,
        controller_epoch: str | None = None,
        controller_sequence: int | None = None) -> DemandRecord:
    """Creates one WARMING demand while enforcing the owner high watermark."""
    if controller_epoch is None:
        controller_epoch = f'legacy-owner-epoch:{owner_epoch}'
        controller_sequence = owner_epoch
    placement_json = _encode_placement(placement)
    initial_current = catalog_state.database_epoch(session, now=now)
    watermark = _lock_watermark(session,
                                workspace=workspace,
                                consumer_kind=consumer_kind,
                                consumer_owner=consumer_owner,
                                controller_epoch=controller_epoch,
                                controller_sequence=controller_sequence,
                                owner_epoch=owner_epoch,
                                generation=consumer_generation,
                                now=initial_current)
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
            'placement_json': placement_json,
        }
        if any(
                str(existing[key]) != str(value)
                for key, value in immutable.items()):
            raise ValueError(
                'A consumer generation cannot change image target.')
        return _demand(existing)
    current = catalog_state.database_epoch(session, now=now)
    if consumer_generation > max_seen:
        session.execute(schema.consumer_watermarks.update().where(
            schema.consumer_watermarks.c.workspace == workspace,
            schema.consumer_watermarks.c.consumer_kind == consumer_kind,
            schema.consumer_watermarks.c.consumer_owner ==
            consumer_owner).values(max_seen_generation=consumer_generation,
                                   updated_at=current))
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
        created_at=current,
        updated_at=current).returning(table)).mappings().one()
    return _demand(row)


def create_demand(**kwargs: Any) -> DemandRecord:
    now = kwargs.pop('now', None)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        return create_demand_in_session(session, now=now, **kwargs)


def create_demand_for_controller_epoch_in_session(
        session: orm.Session,
        *,
        authority_id: str,
        workspace: str,
        consumer_kind: str,
        consumer_owner: str,
        controller_epoch: str,
        controller_sequence: int | None,
        allow_epoch_advance: bool,
        target_key: str,
        image_id: str,
        runtime_digest: str,
        profile_revision_id: str,
        target_fingerprint: str,
        location_id: str,
        placement: dict[str, Any],
        now: int | None = None,
        require_existing: bool = False,
        before_create: Callable[[int], None] | None = None) -> DemandRecord:
    """Maps a controller epoch and converges its durable target fence."""
    controller_epoch = validate_controller_epoch(controller_epoch)
    controller_sequence = validate_controller_sequence(controller_sequence)
    placement_json = _encode_placement(placement)
    watermarks = schema.consumer_watermarks
    initial_current = catalog_state.database_epoch(session, now=now)
    inserted = session.execute(
        postgresql.insert(watermarks).values(
            workspace=workspace,
            consumer_kind=consumer_kind,
            consumer_owner=consumer_owner,
            controller_epoch=controller_epoch,
            controller_sequence=controller_sequence,
            owner_epoch=0,
            max_seen_generation=0,
            max_terminal_generation=-1,
            created_at=initial_current,
            updated_at=initial_current).on_conflict_do_nothing(index_elements=[
                watermarks.c.workspace, watermarks.c.consumer_kind,
                watermarks.c.consumer_owner
            ]).returning(watermarks.c.consumer_owner)).first()
    watermark = session.execute(
        sqlalchemy.select(watermarks).where(
            watermarks.c.workspace == workspace,
            watermarks.c.consumer_kind == consumer_kind,
            watermarks.c.consumer_owner ==
            consumer_owner).with_for_update()).mappings().one()
    _raise_if_owner_deleted(watermark)
    current_controller_epoch = str(watermark['controller_epoch'])
    current_controller_sequence = watermark['controller_sequence']
    owner_epoch = int(watermark['owner_epoch'])
    if current_controller_epoch == controller_epoch:
        if current_controller_sequence != controller_sequence:
            raise StaleConsumerGenerationError(
                'A controller epoch cannot change its sequence.')
    if current_controller_epoch != controller_epoch:
        if require_existing:
            raise StaleConsumerGenerationError(
                'A retired profile revision accepts only an exact live replay.')
        if not allow_epoch_advance:
            raise StaleConsumerGenerationError(
                'A different controller epoch already owns this consumer.')
        if (current_controller_sequence is not None and
            (controller_sequence is None or
             controller_sequence <= int(current_controller_sequence))):
            raise StaleConsumerGenerationError(
                'Controller sequence cannot move backward.')
        previous = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.workspace == workspace,
                schema.demands.c.consumer_kind == consumer_kind,
                schema.demands.c.consumer_owner == consumer_owner,
                schema.demands.c.owner_epoch == owner_epoch,
                schema.demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                    models.ImageDemandState.FAILED.value,
                ])).order_by(
                    schema.demands.c.id).with_for_update()).mappings().all()
        current = catalog_state.database_epoch(session, now=now)
        for row in previous:
            _terminalize(session,
                         row,
                         models.ImageDemandState.SUPERSEDED,
                         now=current)
        if owner_epoch >= _MAX_POSTGRES_BIGINT:
            raise ValueError('Demand owner epoch is exhausted.')
        owner_epoch += 1
        watermark = session.execute(watermarks.update().where(
            watermarks.c.workspace == workspace,
            watermarks.c.consumer_kind == consumer_kind,
            watermarks.c.consumer_owner == consumer_owner).values(
                controller_epoch=controller_epoch,
                controller_sequence=controller_sequence,
                owner_epoch=owner_epoch,
                updated_at=current).returning(watermarks)).mappings().one()
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
            'placement_json': placement_json,
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
    current = catalog_state.database_epoch(session, now=now)
    if before_create is not None:
        before_create(current)
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
                                    controller_epoch=controller_epoch,
                                    controller_sequence=controller_sequence,
                                    now=current)


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


def get_live_service_version_demand_evidence(
        service_name: str, version: int,
        service_hash: str) -> LiveServiceVersionDemandEvidence:
    """Returns exact live image-demand evidence for one Serve version.

    One version may have an unscoped owner and target-scoped derivatives.  The
    explicit delimiter prevents a same-prefix service, hash, or version from
    being included in the retirement proof.
    """
    if (not isinstance(service_name, str) or not service_name or
            any(character.isspace() for character in service_name)):
        raise ValueError('Service name for image-demand evidence is invalid.')
    if type(version) is not int or version < 1:
        raise ValueError(
            'Service version for image-demand evidence is invalid.')
    if (not isinstance(service_hash, str) or not service_hash or
            any(character.isspace() for character in service_hash)):
        raise ValueError('Service hash for image-demand evidence is invalid.')
    owner = f'{service_name}:incarnation:{service_hash}:v{version}'
    target_prefix = f'{owner}:target:'
    demands = schema.demands
    with orm.Session(catalog_state.engine()) as session:
        rows = session.execute(
            sqlalchemy.select(
                demands.c.id, demands.c.consumer_owner,
                demands.c.consumer_generation, demands.c.target_key,
                demands.c.state).where(
                    demands.c.consumer_kind == 'service_version',
                    sqlalchemy.or_(
                        demands.c.consumer_owner == owner,
                        demands.c.consumer_owner.startswith(target_prefix,
                                                            autoescape=True)),
                    demands.c.state.in_([
                        models.ImageDemandState.WARMING.value,
                        models.ImageDemandState.READY.value,
                        models.ImageDemandState.FAILED.value,
                    ])).order_by(demands.c.id).limit(
                        _MAX_SERVICE_VERSION_DEMAND_EVIDENCE_ROWS + 1)).all()
    if len(rows) > _MAX_SERVICE_VERSION_DEMAND_EVIDENCE_ROWS:
        raise RuntimeError(
            'Live Serve version image-demand evidence exceeds its explicit '
            'row bound.')
    canonical = json.dumps(
        [(str(row.id), str(row.consumer_owner), int(
            row.consumer_generation), str(row.target_key), str(row.state))
         for row in rows],
        separators=(',', ':')).encode()
    return LiveServiceVersionDemandEvidence(
        count=len(rows), digest=hashlib.sha256(canonical).hexdigest())


def get_live_service_version_demand_evidence_any_incarnation(
        service_name: str, version: int) -> LiveServiceVersionDemandEvidence:
    """Returns live demand for a Serve version across every service hash.

    Historical ``version_specs`` rows predate durable per-version incarnation
    identity.  Retirement must therefore prove absence for every hash, not
    merely the hash on the current same-name service row.
    """
    if (not isinstance(service_name, str) or not service_name or
            any(character.isspace() for character in service_name)):
        raise ValueError('Service name for image-demand evidence is invalid.')
    if type(version) is not int or version < 1:
        raise ValueError(
            'Service version for image-demand evidence is invalid.')
    owner_prefix = f'{service_name}:incarnation:'
    version_suffix = f':v{version}'
    target_marker = f'{version_suffix}:target:'
    # Incarnation hashes are opaque legacy values and may themselves contain
    # colons.  Parse the exact version/optional-target suffix from the right;
    # a single-component hash grammar would silently drop live old owners.
    owner_pattern = re.compile(rf'^{re.escape(owner_prefix)}.+:v{version}'
                               r'(?::target:[^:]+)?$')
    demands = schema.demands
    with orm.Session(catalog_state.engine()) as session:
        candidates = session.execute(
            sqlalchemy.select(
                demands.c.id, demands.c.consumer_owner,
                demands.c.consumer_generation, demands.c.target_key,
                demands.c.state).where(
                    demands.c.consumer_kind == 'service_version',
                    demands.c.consumer_owner.startswith(owner_prefix,
                                                        autoescape=True),
                    sqlalchemy.or_(
                        demands.c.consumer_owner.endswith(version_suffix,
                                                          autoescape=True),
                        demands.c.consumer_owner.contains(target_marker,
                                                          autoescape=True)),
                    demands.c.state.in_([
                        models.ImageDemandState.WARMING.value,
                        models.ImageDemandState.READY.value,
                        models.ImageDemandState.FAILED.value,
                    ])).order_by(demands.c.id).limit(
                        _MAX_SERVICE_VERSION_DEMAND_EVIDENCE_ROWS + 1)).all()
    if len(candidates) > _MAX_SERVICE_VERSION_DEMAND_EVIDENCE_ROWS:
        raise RuntimeError(
            'Live Serve version image-demand evidence exceeds its explicit '
            'row bound.')
    malformed = [
        row for row in candidates
        if owner_pattern.fullmatch(str(row.consumer_owner)) is None
    ]
    if malformed:
        raise RuntimeError(
            'Live Serve version image-demand evidence contains a malformed '
            'possibly matching owner.')
    rows = candidates
    canonical = json.dumps(
        [(str(row.id), str(row.consumer_owner), int(
            row.consumer_generation), str(row.target_key), str(row.state))
         for row in rows],
        separators=(',', ':')).encode()
    return LiveServiceVersionDemandEvidence(
        count=len(rows), digest=hashlib.sha256(canonical).hexdigest())


def get_current_demand_for_controller_epoch(
        *, workspace: str, consumer_kind: str, consumer_owner: str,
        controller_epoch: str) -> DemandRecord | None:
    """Returns the live fence only for the authoritative controller epoch."""
    controller_epoch = validate_controller_epoch(controller_epoch)
    with orm.Session(catalog_state.engine()) as session:
        watermark = session.execute(
            sqlalchemy.select(schema.consumer_watermarks.c.owner_epoch).where(
                schema.consumer_watermarks.c.workspace == workspace,
                schema.consumer_watermarks.c.consumer_kind == consumer_kind,
                schema.consumer_watermarks.c.consumer_owner == consumer_owner,
                schema.consumer_watermarks.c.controller_epoch ==
                controller_epoch)).first()
        if watermark is None:
            return None
        rows = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.workspace == workspace,
                schema.demands.c.consumer_kind == consumer_kind,
                schema.demands.c.consumer_owner == consumer_owner,
                schema.demands.c.owner_epoch == int(watermark[0]),
                schema.demands.c.state.in_([
                    models.ImageDemandState.WARMING.value,
                    models.ImageDemandState.READY.value,
                    models.ImageDemandState.FAILED.value,
                ])).order_by(schema.demands.c.consumer_generation.desc()).limit(
                    2)).mappings().all()
    if len(rows) > 1:
        raise RuntimeError(
            'A controller epoch has multiple live image demands.')
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
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
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
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
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
                ])).order_by(demands.c.id).with_for_update()).mappings().all()
        current = catalog_state.database_epoch(session, now=now)
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
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id,
                schema.demands.c.workspace == workspace)).mappings().first()
        if optimistic is None:
            return False
        _lock_existing_watermark(session,
                                 workspace=str(optimistic['workspace']),
                                 consumer_kind=str(optimistic['consumer_kind']),
                                 consumer_owner=str(
                                     optimistic['consumer_owner']))
        row = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id, schema.demands.c.workspace ==
                workspace).with_for_update()).mappings().first()
        if row is None:
            return False
        if str(row['state']) in (models.ImageDemandState.SUPERSEDED.value,
                                 models.ImageDemandState.RELEASED.value):
            return True
        current = catalog_state.database_epoch(session, now=now)
        _terminalize(session,
                     row,
                     models.ImageDemandState.SUPERSEDED,
                     now=current)
        return True


def release_demand_authoritatively_in_session(session: orm.Session,
                                              demand_id: str,
                                              workspace: str,
                                              *,
                                              expected_consumer_kind: str |
                                              None = None,
                                              now: int | None = None) -> bool:
    """Releases and retires an owner inside its deletion transaction."""
    optimistic = session.execute(
        sqlalchemy.select(schema.demands).where(
            schema.demands.c.id == demand_id,
            schema.demands.c.workspace == workspace)).mappings().first()
    if optimistic is None:
        return False
    if (expected_consumer_kind is not None and
            str(optimistic['consumer_kind']) != expected_consumer_kind):
        return False
    owner_workspace = str(optimistic['workspace'])
    owner_kind = str(optimistic['consumer_kind'])
    owner = str(optimistic['consumer_owner'])
    _lock_existing_watermark(session,
                             workspace=owner_workspace,
                             consumer_kind=owner_kind,
                             consumer_owner=owner)
    row = session.execute(
        sqlalchemy.select(schema.demands).where(
            schema.demands.c.id == demand_id, schema.demands.c.workspace ==
            workspace).with_for_update()).mappings().first()
    if row is None:
        return False
    current = catalog_state.database_epoch(session, now=now)
    if str(row['state']) not in (models.ImageDemandState.RELEASED.value,
                                 models.ImageDemandState.SUPERSEDED.value):
        _terminalize(session,
                     row,
                     models.ImageDemandState.RELEASED,
                     now=current)
    return _mark_owner_deleted_in_session(session,
                                          workspace=owner_workspace,
                                          consumer_kind=owner_kind,
                                          consumer_owner=owner,
                                          now=current)


def release_owner_authoritatively_in_session(session: orm.Session,
                                             workspace: str,
                                             consumer_kind: str,
                                             consumer_owner: str,
                                             *,
                                             now: int | None = None) -> bool:
    """Releases every live demand for one durably bound deleted owner."""
    watermarks = schema.consumer_watermarks
    demands = schema.demands
    watermark = session.execute(
        sqlalchemy.select(watermarks).where(
            watermarks.c.workspace == workspace,
            watermarks.c.consumer_kind == consumer_kind,
            watermarks.c.consumer_owner ==
            consumer_owner).with_for_update()).mappings().first()
    if watermark is None:
        return False
    rows = session.execute(
        sqlalchemy.select(demands).where(
            demands.c.workspace == workspace,
            demands.c.consumer_kind == consumer_kind,
            demands.c.consumer_owner == consumer_owner,
            demands.c.state.in_([
                models.ImageDemandState.WARMING.value,
                models.ImageDemandState.READY.value,
                models.ImageDemandState.FAILED.value,
            ])).order_by(demands.c.id).with_for_update()).mappings().all()
    current = catalog_state.database_epoch(session, now=now)
    for row in rows:
        _terminalize(session,
                     row,
                     models.ImageDemandState.RELEASED,
                     now=current)
    return _mark_owner_deleted_in_session(session,
                                          workspace=workspace,
                                          consumer_kind=consumer_kind,
                                          consumer_owner=consumer_owner,
                                          now=current)


def release_demand_authoritatively(demand_id: str,
                                   workspace: str,
                                   *,
                                   now: int | None = None) -> bool:
    """Releases and retires an owner after first-party deletion proof."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        return release_demand_authoritatively_in_session(session,
                                                         demand_id,
                                                         workspace,
                                                         now=now)


def fail_and_supersede_demand(demand_id: str,
                              error_code: str,
                              *,
                              now: int | None = None) -> bool:
    """Atomically records terminal materialization failure and opens failover."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id)).mappings().first()
        if optimistic is None:
            return False
        _lock_existing_watermark(session,
                                 workspace=str(optimistic['workspace']),
                                 consumer_kind=str(optimistic['consumer_kind']),
                                 consumer_owner=str(
                                     optimistic['consumer_owner']))
        row = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id ==
                demand_id).with_for_update()).mappings().one()
        if str(row['state']) in (models.ImageDemandState.SUPERSEDED.value,
                                 models.ImageDemandState.RELEASED.value):
            return True
        current = catalog_state.database_epoch(session, now=now)
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
    """Invalidates partial terminal proof and rotates a live candidate."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        return defer_consumer_reconciliation_in_session(session,
                                                        demand_id,
                                                        now=now)


def defer_consumer_reconciliation_in_session(session: orm.Session,
                                             demand_id: str,
                                             *,
                                             now: int | None = None) -> bool:
    """Clears partial terminal proof inside a caller-owned transaction."""
    current = catalog_state.database_epoch(session, now=now)
    changed = session.execute(schema.demands.update().where(
        schema.demands.c.id == demand_id,
        schema.demands.c.state.in_([
            models.ImageDemandState.WARMING.value,
            models.ImageDemandState.READY.value,
            models.ImageDemandState.FAILED.value,
        ])).values(first_terminal_observed_at=None,
                   last_terminal_observed_at=None,
                   terminal_observation_count=0,
                   updated_at=current)).rowcount
    return changed == 1


def defer_terminal_confirmation(demand_id: str,
                                *,
                                now: int | None = None) -> bool:
    """Rotates a candidate without erasing its pending terminal proof."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
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


def _mark_owner_deleted_in_session(session: orm.Session, *, workspace: str,
                                   consumer_kind: str, consumer_owner: str,
                                   now: int) -> bool:
    watermarks = schema.consumer_watermarks
    demands = schema.demands
    watermark = session.execute(
        sqlalchemy.select(watermarks).where(
            watermarks.c.workspace == workspace,
            watermarks.c.consumer_kind == consumer_kind,
            watermarks.c.consumer_owner ==
            consumer_owner).with_for_update()).mappings().first()
    if watermark is None:
        return False
    if watermark['owner_deleted_at'] is not None:
        return True
    live_demand = session.execute(
        sqlalchemy.select(demands.c.id).where(
            demands.c.workspace == workspace,
            demands.c.consumer_kind == consumer_kind,
            demands.c.consumer_owner == consumer_owner,
            demands.c.state.in_([
                models.ImageDemandState.WARMING.value,
                models.ImageDemandState.READY.value,
                models.ImageDemandState.FAILED.value,
            ])).limit(1)).first()
    if live_demand is not None:
        return False
    session.execute(watermarks.update().where(
        watermarks.c.workspace == workspace,
        watermarks.c.consumer_kind == consumer_kind,
        watermarks.c.consumer_owner == consumer_owner).values(
            owner_deleted_at=now, updated_at=now))
    return True


def observe_consumer_terminal(demand_id: str,
                              workspace: str,
                              *,
                              authoritative: bool,
                              now: int | None = None) -> bool:
    """Requires two authoritative observations an hour apart before release."""
    if not authoritative:
        return False
    with orm.Session(catalog_state.engine()) as session, session.begin():
        return observe_consumer_terminal_in_session(session,
                                                    demand_id,
                                                    workspace,
                                                    authoritative=authoritative,
                                                    now=now)


def observe_consumer_terminal_in_session(session: orm.Session,
                                         demand_id: str,
                                         workspace: str,
                                         *,
                                         authoritative: bool,
                                         now: int | None = None) -> bool:
    """Observes terminal state inside a caller-owned transaction."""
    if not authoritative:
        return False
    demands = schema.demands
    optimistic = session.execute(
        sqlalchemy.select(demands).where(
            demands.c.id == demand_id,
            demands.c.workspace == workspace)).mappings().first()
    if optimistic is None:
        return False
    owner_workspace = str(optimistic['workspace'])
    owner_kind = str(optimistic['consumer_kind'])
    owner = str(optimistic['consumer_owner'])
    _lock_existing_watermark(session,
                             workspace=owner_workspace,
                             consumer_kind=owner_kind,
                             consumer_owner=owner)
    row = session.execute(
        sqlalchemy.select(demands).where(
            demands.c.id == demand_id, demands.c.workspace ==
            workspace).with_for_update()).mappings().first()
    if row is None:
        return False
    current = catalog_state.database_epoch(session, now=now)
    if str(row['state']) in (models.ImageDemandState.RELEASED.value,
                             models.ImageDemandState.SUPERSEDED.value):
        return _mark_owner_deleted_in_session(session,
                                              workspace=owner_workspace,
                                              consumer_kind=owner_kind,
                                              consumer_owner=owner,
                                              now=current)
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
    if (str(row['consumer_kind']) == 'cluster' and
            not bool(row['consumer_attached']) and
            current - int(row['created_at'])
            < _UNATTACHED_REQUEST_RETENTION_SECONDS):
        session.execute(
            demands.update().where(demands.c.id == demand_id).values(
                last_terminal_observed_at=current,
                terminal_observation_count=max(count, 1),
                updated_at=current))
        return False
    if current - int(first) < _TERMINAL_CONFIRMATION_SECONDS:
        session.execute(
            demands.update().where(demands.c.id == demand_id).values(
                last_terminal_observed_at=current,
                terminal_observation_count=max(count, 1),
                updated_at=current))
        return False
    session.execute(demands.update().where(demands.c.id == demand_id).values(
        last_terminal_observed_at=current,
        terminal_observation_count=max(count + 1, 2)))
    _terminalize(session, row, models.ImageDemandState.RELEASED, now=current)
    return _mark_owner_deleted_in_session(session,
                                          workspace=owner_workspace,
                                          consumer_kind=owner_kind,
                                          consumer_owner=owner,
                                          now=current)


def compact_terminal_demands(*,
                             now: int | None = None,
                             limit: int = 500) -> int:
    """Compacts tombstones while retaining their nonresurrection fences."""
    if not 1 <= limit <= 1000:
        raise ValueError('Demand compaction page size is invalid.')
    demands = schema.demands
    watermarks = schema.consumer_watermarks
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
        candidate_rows = session.execute(
            sqlalchemy.select(demands.c.id, demands.c.workspace,
                              demands.c.consumer_kind,
                              demands.c.consumer_owner).
            select_from(
                demands.join(
                    watermarks,
                    sqlalchemy.and_(
                        watermarks.c.workspace == demands.c.workspace,
                        watermarks.c.consumer_kind == demands.c.consumer_kind,
                        watermarks.c.consumer_owner ==
                        demands.c.consumer_owner))).where(
                            demands.c.state.in_([
                                models.ImageDemandState.SUPERSEDED.value,
                                models.ImageDemandState.RELEASED.value,
                            ]), demands.c.expires_at <= current,
                            watermarks.c.owner_deleted_at.is_not(None),
                            demands.c.consumer_generation
                            <= watermarks.c.max_terminal_generation).order_by(
                                demands.c.expires_at,
                                demands.c.id).limit(limit)).mappings().all()
        candidate_ids_by_owner: dict[tuple[str, str, str], list[str]] = {}
        for candidate in candidate_rows:
            owner_key = (str(candidate['workspace']),
                         str(candidate['consumer_kind']),
                         str(candidate['consumer_owner']))
            candidate_ids_by_owner.setdefault(owner_key,
                                              []).append(str(candidate['id']))
        owner_keys = set(candidate_ids_by_owner)
        deleted_demands = 0
        for workspace, consumer_kind, consumer_owner in sorted(owner_keys):
            watermark = session.execute(
                sqlalchemy.select(watermarks).where(
                    watermarks.c.workspace == workspace,
                    watermarks.c.consumer_kind == consumer_kind,
                    watermarks.c.consumer_owner == consumer_owner).
                with_for_update(skip_locked=True)).mappings().first()
            if watermark is None:
                continue
            if watermark['owner_deleted_at'] is None:
                continue
            candidate_ids = candidate_ids_by_owner.get(
                (workspace, consumer_kind, consumer_owner), [])
            if candidate_ids:
                locked_demands = session.execute(
                    sqlalchemy.select(demands.c.id).where(
                        demands.c.id.in_(candidate_ids),
                        demands.c.workspace == workspace,
                        demands.c.consumer_kind == consumer_kind,
                        demands.c.consumer_owner == consumer_owner,
                        demands.c.state.in_([
                            models.ImageDemandState.SUPERSEDED.value,
                            models.ImageDemandState.RELEASED.value,
                        ]), demands.c.expires_at <= current,
                        demands.c.consumer_generation
                        <= int(watermark['max_terminal_generation'])).order_by(
                            demands.c.id).with_for_update(
                                skip_locked=True)).scalars().all()
                if locked_demands:
                    deleted_demands += session.execute(demands.delete().where(
                        demands.c.id.in_(locked_demands))).rowcount
    return deleted_demands
