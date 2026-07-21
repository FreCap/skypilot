"""Cross-component transactions for managed image distribution.

Lock ordering in this module follows the canonical design: profile, budget,
shard and worker; artifact and source; canonical then regional location;
publication and operation; watermark and demand.
"""

from __future__ import annotations

import json
import time
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.container_images import catalog_state
from sky.container_images import demand_state
from sky.container_images import models
from sky.container_images import schema
from sky.container_images import topology_state

_PUBLICATION_FANOUT_BATCH = 100
_OPERATION_RETENTION_SECONDS = 30 * 24 * 60 * 60
_FAILED_RESERVATION_SECONDS = 30 * 24 * 60 * 60
_FAILED_PUBLICATION_RETENTION_SECONDS = 90 * 24 * 60 * 60


class DemandLocationNotReadyError(ValueError):
    """The demand's locked location left READY before plan commit."""

    def __init__(self, state: models.ImageLocationState,
                 error_code: str | None) -> None:
        self.state = state
        self.error_code = error_code
        super().__init__('Demand location is not READY.')


class ImageLimitExceededError(ValueError):
    """A bounded per-artifact release or location ceiling was reached."""


def _target_reference(shard: sqlalchemy.engine.RowMapping,
                      runtime_digest: str) -> str:
    return (f"{str(shard['registry']).rstrip('/')}/"
            f"{str(shard['repository_name']).strip('/')}@{runtime_digest}")


def _target_fingerprint(profile: sqlalchemy.engine.RowMapping,
                        target_id: str) -> str:
    snapshot = json.loads(str(profile['config_json']))
    configured = models.ManagedRegistryProfile.from_snapshot(snapshot)
    return configured.target(target_id).target_fingerprint


def _lock_profile(
    session: orm.Session, profile_revision_id: str, *,
    states: tuple[models.ImageProfileState,
                  ...]) -> sqlalchemy.engine.RowMapping:
    row = session.execute(
        sqlalchemy.select(schema.profile_revisions).where(
            schema.profile_revisions.c.id ==
            profile_revision_id).with_for_update()).mappings().first()
    if row is None or str(row['state']) not in tuple(
            state.value for state in states):
        raise topology_state.StaleProfileRevisionError(
            'The image profile revision is not eligible for this operation.')
    return row


def _select_and_lock_shard(
        session: orm.Session, *, workspace: str, profile: str, target_id: str,
        runtime_digest: str,
        declared_size_bytes: int) -> sqlalchemy.engine.RowMapping:
    """Locks one capacity-bearing shard using a stable digest-derived order."""
    table = schema.registry_shards
    # PostgreSQL md5 gives a stable ring without first locking rejected shards.
    score = sqlalchemy.func.md5(table.c.id + runtime_digest)
    eligible = (table.c.workspace == workspace, table.c.profile == profile,
                table.c.target_id == target_id,
                table.c.state == models.ImageShardState.READY.value,
                table.c.reserved_manifests < table.c.max_manifests,
                table.c.reserved_declared_bytes + declared_size_bytes
                <= table.c.max_declared_bytes)
    statement = sqlalchemy.select(table).where(*eligible).order_by(
        score, table.c.id).limit(1).with_for_update()
    # Under READ COMMITTED, a candidate can fill while this SELECT waits for
    # its row lock. PostgreSQL's EvalPlanQual then removes that row without
    # continuing past LIMIT 1. Retry from a fresh statement snapshot until an
    # unlocked candidate wins or a separate fresh snapshot proves that no
    # eligible candidate remains. Reservations only increase within this path,
    # so the loop converges even under a train of concurrent fills.
    while True:
        row = session.execute(statement).mappings().first()
        if row is not None:
            return row
        candidate_exists = session.execute(
            sqlalchemy.select(table.c.id).where(*eligible).limit(1)).first()
        if candidate_exists is None:
            raise topology_state.RegistryCapacityExhaustedError(
                'REGISTRY_CAPACITY_EXHAUSTED')


def _expected_pull_plan(*, profile_row: sqlalchemy.engine.RowMapping,
                        location: sqlalchemy.engine.RowMapping,
                        artifact: sqlalchemy.engine.RowMapping,
                        demand: sqlalchemy.engine.RowMapping) -> dict[str, Any]:
    """Reconstructs the only runtime plan allowed by durable catalog state."""
    profile = models.ManagedRegistryProfile.from_snapshot(
        json.loads(str(profile_row['config_json'])))
    if (profile.name != str(profile_row['profile']) or
            profile.revision != int(profile_row['revision']) or
            profile.config_hash != str(profile_row['config_hash'])):
        raise ValueError('Runtime pull plan profile snapshot is inconsistent.')
    matching_targets = [
        target for target in (profile.canonical,) + profile.targets
        if target.target_fingerprint == str(demand['target_fingerprint'])
    ]
    if len(matching_targets) != 1:
        raise ValueError('Runtime pull plan target is not uniquely configured.')
    target = matching_targets[0]
    placement = json.loads(str(demand['placement_json']))
    backend = placement.get('backend')
    region = placement.get('region')
    binding_id = target.runtime_binding(backend)
    if binding_id is None:
        raise ValueError('Runtime pull plan binding is not configured.')
    binding = profile.bindings[binding_id]
    runtime_principal: str | None = None
    instance_profile: str | None = None
    node_selector: list[tuple[str, str]] = []
    credential_helper: str | None = None
    if backend == 'aws_vm':
        runtime_principal = binding.principals[0]
        instance_profile = binding.instance_profile
        credential_helper = 'ecr-login'
    elif backend == 'aws_eks':
        qualified = next((cluster for cluster in binding.qualified_clusters
                          if cluster.context == region), None)
        if qualified is None:
            raise ValueError('Runtime pull plan EKS target is not qualified.')
        node_selector = list(qualified.node_selector)
    else:
        raise ValueError('Runtime pull plan backend is invalid.')
    return {
        'version': 1,
        'reference': str(location['target_ref']),
        'runtime_digest': str(demand['runtime_digest']),
        'platform': str(artifact['platform']),
        'distribution': profile.name,
        'profile_revision_id': str(demand['profile_revision_id']),
        'target_id': target.name,
        'target_fingerprint': str(demand['target_fingerprint']),
        'auth_strategy': 'ecr_runtime_identity',
        'credential_helper': credential_helper,
        'runtime_principal': runtime_principal,
        'instance_profile': instance_profile,
        'kubernetes_node_selector': node_selector,
    }


def _converge_artifact(session: orm.Session, *, workspace: str,
                       runtime_digest: str, platform: str, config_digest: str,
                       manifest_media_type: str, manifest_size_bytes: int,
                       declared_size_bytes: int, creator_user_hash: str,
                       now: int) -> sqlalchemy.engine.RowMapping:
    table = schema.images
    session.execute(
        postgresql.insert(table).values(
            id=str(uuid.uuid4()),
            workspace=workspace,
            runtime_digest=runtime_digest,
            platform=platform,
            config_digest=config_digest,
            manifest_media_type=manifest_media_type,
            manifest_size_bytes=manifest_size_bytes,
            declared_size_bytes=declared_size_bytes,
            creator_user_hash=creator_user_hash,
            producer_kind=models.ImageProducerKind.EXTERNAL_OCI.value,
            created_at=now,
            updated_at=now).on_conflict_do_nothing(index_elements=[
                table.c.workspace, table.c.runtime_digest, table.c.platform
            ]))
    row = session.execute(
        sqlalchemy.select(table).where(
            table.c.workspace == workspace,
            table.c.runtime_digest == runtime_digest,
            table.c.platform == platform).with_for_update()).mappings().one()
    immutable = {
        'config_digest': config_digest,
        'manifest_media_type': manifest_media_type,
        'manifest_size_bytes': manifest_size_bytes,
        'declared_size_bytes': declared_size_bytes,
    }
    if any(str(row[key]) != str(value) for key, value in immutable.items()):
        raise ValueError('Inspected OCI evidence conflicts with the artifact.')
    return row


def _converge_source(session: orm.Session, *, workspace: str, image_id: str,
                     source_ref: str, source_root_digest: str,
                     source_root_media_type: str, requested_platform: str,
                     selected_child_digest: str,
                     source_auth_binding_id: str | None,
                     source_auth_fingerprint: str | None,
                     now: int) -> sqlalchemy.engine.RowMapping:
    table = schema.sources
    session.execute(
        postgresql.insert(table).values(
            id=str(uuid.uuid4()),
            workspace=workspace,
            image_id=image_id,
            source_ref=source_ref,
            source_root_digest=source_root_digest,
            source_root_media_type=source_root_media_type,
            requested_platform=requested_platform,
            selected_child_digest=selected_child_digest,
            source_auth_binding_id=source_auth_binding_id,
            source_auth_fingerprint=source_auth_fingerprint,
            created_at=now).on_conflict_do_nothing(index_elements=[
                table.c.workspace, table.c.source_ref,
                table.c.requested_platform
            ]))
    row = session.execute(
        sqlalchemy.select(table).where(
            table.c.workspace == workspace, table.c.source_ref == source_ref,
            table.c.requested_platform ==
            requested_platform).with_for_update()).mappings().one()
    immutable = {
        'image_id': image_id,
        'source_root_digest': source_root_digest,
        'source_root_media_type': source_root_media_type,
        'selected_child_digest': selected_child_digest,
        'source_auth_binding_id': source_auth_binding_id,
        'source_auth_fingerprint': source_auth_fingerprint,
    }
    if any(row[key] != value for key, value in immutable.items()):
        raise ValueError('A source selection cannot change immutable evidence.')
    return row


def bind_inspected_publication(
    *,
    publication_id: str,
    inspection_lease_token: str,
    creator_user_hash: str,
    runtime_digest: str,
    platform: str,
    config_digest: str,
    source_root_media_type: str,
    selected_manifest_media_type: str,
    selected_manifest_size_bytes: int,
    declared_size_bytes: int,
    canonical_target_id: str,
    max_releases_per_artifact: int,
    now: int | None = None
) -> tuple[catalog_state.ArtifactRecord, catalog_state.SourceRecord,
           topology_state.LocationRecord, catalog_state.PublicationRecord]:
    """Binds inspected source evidence and reserves one canonical shard."""
    current = int(time.time()) if now is None else now
    runtime_digest = models.validate_sha256_digest(runtime_digest,
                                                   'Runtime digest')
    platform = models.validate_oci_platform(platform, 'Runtime platform')
    config_digest = models.validate_sha256_digest(config_digest,
                                                  'Config digest')
    publications = schema.publications
    locations = schema.locations
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(publications).where(
                publications.c.id == publication_id)).mappings().first()
        if optimistic is None:
            raise ValueError('Publication does not exist.')
        profile_row = _lock_profile(session,
                                    str(optimistic['profile_revision_id']),
                                    states=(models.ImageProfileState.ACTIVE,
                                            models.ImageProfileState.RETIRED))
        target_fingerprint = _target_fingerprint(profile_row,
                                                 canonical_target_id)
        existing_artifact = session.execute(
            sqlalchemy.select(schema.images).where(
                schema.images.c.workspace == optimistic['workspace'],
                schema.images.c.runtime_digest == runtime_digest,
                schema.images.c.platform == platform)).mappings().first()
        existing_location = None
        shard_row = None
        if existing_artifact is not None:
            existing_location = session.execute(
                sqlalchemy.select(locations).where(
                    locations.c.image_id == existing_artifact['id'],
                    locations.c.canonical.is_(True),
                    locations.c.target_fingerprint == target_fingerprint,
                    locations.c.runtime_digest == runtime_digest).order_by(
                        locations.c.id).limit(1)).mappings().first()
        if existing_location is not None:
            shard_row = session.execute(
                sqlalchemy.select(shards).where(
                    shards.c.id == existing_location['shard_id']).
                with_for_update()).mappings().one()
        else:
            shard_row = _select_and_lock_shard(
                session,
                workspace=str(optimistic['workspace']),
                profile=str(profile_row['profile']),
                target_id=canonical_target_id,
                runtime_digest=runtime_digest,
                declared_size_bytes=declared_size_bytes)

        artifact = _converge_artifact(
            session,
            workspace=str(optimistic['workspace']),
            runtime_digest=runtime_digest,
            platform=platform,
            config_digest=config_digest,
            manifest_media_type=selected_manifest_media_type,
            manifest_size_bytes=selected_manifest_size_bytes,
            declared_size_bytes=declared_size_bytes,
            creator_user_hash=creator_user_hash,
            now=current)
        source = _converge_source(
            session,
            workspace=str(optimistic['workspace']),
            image_id=str(artifact['id']),
            source_ref=str(optimistic['source_ref']),
            source_root_digest=str(optimistic['source_root_digest']),
            source_root_media_type=source_root_media_type,
            requested_platform=str(optimistic['requested_platform']),
            selected_child_digest=runtime_digest,
            source_auth_binding_id=optimistic['source_auth_binding_id'],
            source_auth_fingerprint=optimistic['source_auth_fingerprint'],
            now=current)

        target_ref = _target_reference(shard_row, runtime_digest)
        inserted = session.execute(
            postgresql.insert(locations).values(
                id=str(uuid.uuid4()),
                workspace=optimistic['workspace'],
                image_id=artifact['id'],
                shard_id=shard_row['id'],
                target_fingerprint=target_fingerprint,
                physical_fingerprint=shard_row['physical_fingerprint'],
                runtime_digest=runtime_digest,
                canonical=True,
                canonical_location_id=None,
                target_ref=target_ref,
                state=models.ImageLocationState.PENDING.value,
                attempt_count=0,
                reserved_declared_bytes=declared_size_bytes,
                created_at=current,
                updated_at=current).on_conflict_do_nothing(index_elements=[
                    locations.c.image_id, locations.c.target_fingerprint,
                    locations.c.runtime_digest
                ]).returning(locations.c.id)).first()
        location = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.image_id == artifact['id'],
                locations.c.target_fingerprint == target_fingerprint,
                locations.c.runtime_digest ==
                runtime_digest).with_for_update()).mappings().one()
        if inserted is not None:
            changed = session.execute(shards.update().where(
                shards.c.id == shard_row['id'], shards.c.reserved_manifests
                < shards.c.max_manifests,
                shards.c.reserved_declared_bytes + declared_size_bytes
                <= shards.c.max_declared_bytes).values(
                    reserved_manifests=shards.c.reserved_manifests + 1,
                    reserved_declared_bytes=(shards.c.reserved_declared_bytes +
                                             declared_size_bytes),
                    updated_at=current)).rowcount
            if changed != 1:
                raise topology_state.RegistryCapacityExhaustedError(
                    'REGISTRY_CAPACITY_EXHAUSTED')

        publication = session.execute(
            sqlalchemy.select(publications).where(
                publications.c.id ==
                publication_id).with_for_update()).mappings().one()
        if (str(publication['state'])
                != models.ImagePublicationState.INSPECTING.value or
                publication['inspection_lease_token'] != inspection_lease_token
                or publication['inspection_lease_expires_at'] is None or
                int(publication['inspection_lease_expires_at']) <= current):
            raise topology_state.LocationLeaseLostError(
                'Publication inspection lease was lost.')
        release_count = session.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(publications).where(
                                 publications.c.image_id == artifact['id'],
                                 publications.c.reservation_active.is_(True),
                                 publications.c.id
                                 != publication_id)).scalar_one()
        if int(release_count) >= max_releases_per_artifact:
            raise ImageLimitExceededError('IMAGE_LIMIT_EXCEEDED')
        publication = session.execute(publications.update().where(
            publications.c.id == publication_id).values(
                state=models.ImagePublicationState.PENDING.value,
                inspection_lease_token=None,
                inspection_lease_expires_at=None,
                image_id=artifact['id'],
                source_id=source['id'],
                canonical_location_id=location['id'],
                next_retry_at=None,
                error_code=None,
                updated_at=current).returning(publications)).mappings().one()
        return (
            catalog_state._artifact(artifact),  # pylint: disable=protected-access
            catalog_state._source(source),  # pylint: disable=protected-access
            topology_state._location(location),  # pylint: disable=protected-access
            catalog_state._publication(  # pylint: disable=protected-access
                publication))


def reserve_regional_location(
        *,
        image_id: str,
        workspace: str,
        profile_revision_id: str,
        target_id: str,
        canonical_location_id: str,
        max_regional_locations: int,
        now: int | None = None) -> topology_state.LocationRecord:
    """Reserves one selected target, never a target fanout."""
    current = int(time.time()) if now is None else now
    locations = schema.locations
    shards = schema.registry_shards
    with orm.Session(catalog_state.engine()) as session, session.begin():
        profile_row = _lock_profile(session,
                                    profile_revision_id,
                                    states=(models.ImageProfileState.ACTIVE,))
        target_fingerprint = _target_fingerprint(profile_row, target_id)
        artifact_snapshot = session.execute(
            sqlalchemy.select(schema.images).where(
                schema.images.c.id == image_id,
                schema.images.c.workspace == workspace)).mappings().one()
        existing = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.image_id == image_id,
                locations.c.canonical.is_(False),
                locations.c.target_fingerprint == target_fingerprint,
                locations.c.runtime_digest == artifact_snapshot[
                    'runtime_digest']).limit(1)).mappings().first()
        if existing is not None:
            shard = session.execute(
                sqlalchemy.select(shards).where(
                    shards.c.id ==
                    existing['shard_id']).with_for_update()).mappings().one()
        else:
            shard = _select_and_lock_shard(
                session,
                workspace=workspace,
                profile=str(profile_row['profile']),
                target_id=target_id,
                runtime_digest=str(artifact_snapshot['runtime_digest']),
                declared_size_bytes=int(
                    artifact_snapshot['declared_size_bytes']))
        artifact = session.execute(
            sqlalchemy.select(schema.images).where(
                schema.images.c.id == image_id, schema.images.c.workspace ==
                workspace).with_for_update()).mappings().one()
        canonical = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.id == canonical_location_id,
                locations.c.image_id == image_id,
                locations.c.canonical.is_(True),
                locations.c.state == models.ImageLocationState.READY.value).
            with_for_update()).mappings().one()
        if existing is not None:
            winner = session.execute(
                sqlalchemy.select(locations).where(
                    locations.c.id ==
                    existing['id']).with_for_update()).mappings().one()
            if str(winner['state']) in (
                    models.ImageLocationState.FAILED.value,
                    models.ImageLocationState.MISSING.value,
                    models.ImageLocationState.EVICTED.value):
                if str(shard['state']) not in (
                        models.ImageShardState.READY.value,
                        models.ImageShardState.FULL.value):
                    raise topology_state.RegistryShardUnavailableError(
                        'REGISTRY_SHARD_UNAVAILABLE')
                location_values: dict[str, Any] = {
                    'state': models.ImageLocationState.PENDING.value,
                    'next_retry_at': None,
                    'error_code': None,
                    'updated_at': current,
                }
                if (str(winner['state']) ==
                        models.ImageLocationState.EVICTED.value):
                    charge = int(artifact['declared_size_bytes'])
                    if int(winner['reserved_declared_bytes']) != 0:
                        raise RuntimeError(
                            'Evicted location retained a capacity charge.')
                    changed = session.execute(shards.update().where(
                        shards.c.id == shard['id'], shards.c.reserved_manifests
                        < shards.c.max_manifests,
                        shards.c.reserved_declared_bytes + charge
                        <= shards.c.max_declared_bytes).values(
                            reserved_manifests=(shards.c.reserved_manifests +
                                                1),
                            reserved_declared_bytes=(
                                shards.c.reserved_declared_bytes + charge),
                            updated_at=current)).rowcount
                    if changed != 1:
                        raise topology_state.RegistryCapacityExhaustedError(
                            'REGISTRY_CAPACITY_EXHAUSTED')
                    location_values['reserved_declared_bytes'] = charge
                winner = session.execute(locations.update().where(
                    locations.c.id == winner['id']).values(**location_values).
                                         returning(locations)).mappings().one()
            return topology_state._location(  # pylint: disable=protected-access
                winner)
        regional_count = session.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(locations).where(
                locations.c.image_id == image_id,
                locations.c.canonical.is_(False))).scalar_one()
        if int(regional_count) >= max_regional_locations:
            raise ImageLimitExceededError('IMAGE_LIMIT_EXCEEDED')
        target_ref = _target_reference(shard, str(artifact['runtime_digest']))
        inserted = session.execute(
            postgresql.insert(locations).values(
                id=str(uuid.uuid4()),
                workspace=workspace,
                image_id=image_id,
                shard_id=shard['id'],
                target_fingerprint=target_fingerprint,
                physical_fingerprint=shard['physical_fingerprint'],
                runtime_digest=artifact['runtime_digest'],
                canonical=False,
                canonical_location_id=canonical['id'],
                target_ref=target_ref,
                state=models.ImageLocationState.PENDING.value,
                attempt_count=0,
                reserved_declared_bytes=artifact['declared_size_bytes'],
                created_at=current,
                updated_at=current).on_conflict_do_nothing(index_elements=[
                    locations.c.image_id, locations.c.target_fingerprint,
                    locations.c.runtime_digest
                ]).returning(locations.c.id)).first()
        row = session.execute(
            sqlalchemy.select(locations).where(
                locations.c.image_id == image_id,
                locations.c.target_fingerprint == target_fingerprint,
                locations.c.runtime_digest ==
                artifact['runtime_digest']).with_for_update()).mappings().one()
        if inserted is None:
            return topology_state._location(  # pylint: disable=protected-access
                row)
        changed = session.execute(shards.update().where(
            shards.c.id == shard['id'], shards.c.reserved_manifests
            < shards.c.max_manifests,
            shards.c.reserved_declared_bytes + artifact['declared_size_bytes']
            <= shards.c.max_declared_bytes).values(
                reserved_manifests=shards.c.reserved_manifests + 1,
                reserved_declared_bytes=(shards.c.reserved_declared_bytes +
                                         artifact['declared_size_bytes']),
                updated_at=current)).rowcount
        if changed != 1:
            raise topology_state.RegistryCapacityExhaustedError(
                'REGISTRY_CAPACITY_EXHAUSTED')
        return topology_state._location(  # pylint: disable=protected-access
            row)


def _finish_location(session: orm.Session, *, location_id: str,
                     lease_token: str, ready: bool, error_code: str | None,
                     retry_at: int | None, terminal: bool,
                     now: int) -> sqlalchemy.engine.RowMapping:
    locations = schema.locations
    optimistic = session.execute(
        sqlalchemy.select(
            locations.c.shard_id).where(locations.c.id == location_id)).first()
    if optimistic is None:
        raise topology_state.LocationLeaseLostError(
            'Location completion lost its fenced lease.')
    session.execute(
        sqlalchemy.select(schema.registry_shards.c.id).where(
            schema.registry_shards.c.id ==
            optimistic[0]).with_for_update()).one()
    row = session.execute(
        sqlalchemy.select(locations).where(
            locations.c.id == location_id).with_for_update()).mappings().one()
    if (row['lease_token'] != lease_token or row['lease_expires_at'] is None or
            int(row['lease_expires_at']) <= now or str(row['state'])
            not in (models.ImageLocationState.COPYING.value,
                    models.ImageLocationState.VERIFYING.value)):
        raise topology_state.LocationLeaseLostError(
            'Location completion lost its fenced lease.')
    if ready:
        state = models.ImageLocationState.READY.value
    elif terminal:
        state = models.ImageLocationState.FAILED.value
    else:
        state = models.ImageLocationState.PENDING.value
    updated = session.execute(
        locations.update().where(locations.c.id == location_id).values(
            state=state,
            lease_kind=None,
            lease_token=None,
            lease_expires_at=None,
            next_retry_at=None if ready or terminal else retry_at,
            error_code=error_code,
            last_verified_at=now if ready else row['last_verified_at'],
            updated_at=now).returning(locations)).mappings().one()
    session.execute(schema.registry_shards.update().where(
        schema.registry_shards.c.id == row['shard_id'],
        schema.registry_shards.c.in_flight
        > 0).values(in_flight=schema.registry_shards.c.in_flight - 1,
                    updated_at=now))
    operation_values: dict[str, Any] = {
        'result_json': json.dumps({
            'location_id': location_id,
            'state': state,
        },
                                  sort_keys=True),
        'updated_at': now,
    }
    if ready:
        operation_values.update(
            state=models.ImageOperationState.SUCCEEDED.value,
            error_code=None,
            terminal_expires_at=now + _OPERATION_RETENTION_SECONDS)
    elif terminal:
        operation_values.update(state=models.ImageOperationState.FAILED.value,
                                error_code=error_code,
                                terminal_expires_at=now +
                                _OPERATION_RETENTION_SECONDS)
    session.execute(schema.operations.update().where(
        schema.operations.c.result_kind == 'location',
        schema.operations.c.result_id == location_id,
        schema.operations.c.state.in_([
            models.ImageOperationState.PENDING.value,
            models.ImageOperationState.RUNNING.value,
        ])).values(**operation_values))
    return updated


def _project_dependent_publications(session: orm.Session,
                                    location: sqlalchemy.engine.RowMapping, *,
                                    ready: bool, error_code: str | None,
                                    now: int) -> int:
    publications = schema.publications
    operations = schema.operations
    rows = session.execute(
        sqlalchemy.select(publications).where(
            publications.c.canonical_location_id == location['id'],
            publications.c.state == models.ImagePublicationState.PENDING.value).
        order_by(
            publications.c.id).limit(_PUBLICATION_FANOUT_BATCH).with_for_update(
                skip_locked=True)).mappings().all()
    for row in rows:
        if ready:
            publication_state = models.ImagePublicationState.READY.value
            publication_values = {
                'state': publication_state,
                'error_code': None,
                'next_retry_at': None,
                'reservation_expires_at': None,
                'record_expires_at': None,
                'updated_at': now,
            }
            operation_state = models.ImageOperationState.SUCCEEDED.value
            operation_error = None
        else:
            publication_state = models.ImagePublicationState.FAILED.value
            publication_values = {
                'state': publication_state,
                'error_code': error_code,
                'next_retry_at': None,
                'reservation_expires_at': now + _FAILED_RESERVATION_SECONDS,
                'record_expires_at':
                    (now + _FAILED_PUBLICATION_RETENTION_SECONDS),
                'updated_at': now,
            }
            operation_state = models.ImageOperationState.FAILED.value
            operation_error = error_code
        session.execute(publications.update().where(
            publications.c.id == row['id']).values(**publication_values))
        session.execute(operations.update().where(
            sqlalchemy.or_(operations.c.id == row['operation_id'],
                           operations.c.result_id == row['id']),
            operations.c.state.in_([
                models.ImageOperationState.PENDING.value,
                models.ImageOperationState.RUNNING.value,
            ])).values(state=operation_state,
                       result_kind='publication',
                       result_id=row['id'],
                       result_json=json.dumps(
                           {
                               'publication_id': row['id'],
                               'release': row['requested_release'],
                               'state': publication_state,
                               'image_id': row['image_id'],
                           },
                           sort_keys=True),
                       error_code=operation_error,
                       updated_at=now,
                       terminal_expires_at=(now +
                                            _OPERATION_RETENTION_SECONDS)))
    return len(rows)


def converge_canonical(*,
                       location_id: str,
                       lease_token: str,
                       ready: bool,
                       error_code: str | None = None,
                       retry_at: int | None = None,
                       terminal: bool = True,
                       now: int | None = None) -> topology_state.LocationRecord:
    """Commits canonical result and release projections in one transaction."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        location = _finish_location(session,
                                    location_id=location_id,
                                    lease_token=lease_token,
                                    ready=ready,
                                    error_code=error_code,
                                    retry_at=retry_at,
                                    terminal=terminal,
                                    now=current)
        if bool(location['canonical']) and (ready or terminal):
            _project_dependent_publications(session,
                                            location,
                                            ready=ready,
                                            error_code=error_code,
                                            now=current)
        return topology_state._location(  # pylint: disable=protected-access
            location)


def reconcile_canonical_publications(location_id: str,
                                     *,
                                     now: int | None = None) -> int:
    """Resumes bounded publication fanout without provider I/O."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        location = session.execute(
            sqlalchemy.select(schema.locations).where(
                schema.locations.c.id == location_id,
                schema.locations.c.canonical.is_(True),
                schema.locations.c.state.in_([
                    models.ImageLocationState.READY.value,
                    models.ImageLocationState.FAILED.value,
                ])).with_for_update()).mappings().one()
        return _project_dependent_publications(
            session,
            location,
            ready=str(
                location['state']) == models.ImageLocationState.READY.value,
            error_code=location['error_code'],
            now=current)


def reconcile_pending_canonical_publications(limit: int = 100) -> int:
    """Reconciles a bounded fanout batch from either operational worker."""
    if not 1 <= limit <= 1000:
        raise ValueError('Publication fanout reconciliation limit is invalid.')
    locations = schema.locations
    publications = schema.publications
    with orm.Session(catalog_state.engine()) as session:
        location_ids = session.execute(
            sqlalchemy.select(locations.c.id).where(
                locations.c.canonical.is_(True),
                locations.c.state.in_([
                    models.ImageLocationState.READY.value,
                    models.ImageLocationState.FAILED.value,
                ]),
                sqlalchemy.exists().where(
                    publications.c.canonical_location_id == locations.c.id,
                    publications.c.state ==
                    models.ImagePublicationState.PENDING.value)).order_by(
                        locations.c.updated_at,
                        locations.c.id).limit(limit)).scalars().all()
    return sum(
        reconcile_canonical_publications(location_id)
        for location_id in location_ids)


def retry_publication(
        *,
        publication_id: str,
        workspace: str,
        operation_id: str,
        now: int | None = None) -> catalog_state.PublicationRecord:
    """Requeues one retained failed publication behind its shared location."""
    current = int(time.time()) if now is None else now
    publications = schema.publications
    locations = schema.locations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(publications).where(
                publications.c.id == publication_id,
                publications.c.workspace == workspace)).mappings().first()
        if optimistic is None:
            raise ValueError('IMAGE_PUBLICATION_NOT_FOUND')
        location = None
        shard = None
        if optimistic['canonical_location_id'] is not None:
            location_snapshot = session.execute(
                sqlalchemy.select(locations.c.shard_id).where(
                    locations.c.id ==
                    optimistic['canonical_location_id'])).first()
            if location_snapshot is None:
                raise ValueError('Canonical location is unavailable.')
            shard = session.execute(
                sqlalchemy.select(schema.registry_shards).where(
                    schema.registry_shards.c.id ==
                    location_snapshot[0]).with_for_update()).mappings().one()
            location = session.execute(
                sqlalchemy.select(locations).where(
                    locations.c.id == optimistic['canonical_location_id']).
                with_for_update()).mappings().one()
        publication = session.execute(
            sqlalchemy.select(publications).where(
                publications.c.id ==
                publication_id).with_for_update()).mappings().one()
        if (str(publication['state'])
                != models.ImagePublicationState.FAILED.value or
                not bool(publication['reservation_active'])):
            raise ValueError('Only a retained FAILED publication can retry.')
        if location is not None and str(
                location['state']) in (models.ImageLocationState.FAILED.value,
                                       models.ImageLocationState.MISSING.value):
            if shard is None:
                raise RuntimeError('Canonical location shard is unavailable.')
            if str(shard['state']) not in (models.ImageShardState.READY.value,
                                           models.ImageShardState.FULL.value):
                raise topology_state.RegistryShardUnavailableError(
                    'REGISTRY_SHARD_UNAVAILABLE')
            session.execute(locations.update().where(
                locations.c.id == location['id']).values(
                    state=models.ImageLocationState.PENDING.value,
                    next_retry_at=None,
                    error_code=None,
                    updated_at=current))
        ready = (location is not None and str(location['state'])
                 == models.ImageLocationState.READY.value)
        publication_state = (models.ImagePublicationState.READY.value if ready
                             else models.ImagePublicationState.PENDING.value)
        updated = session.execute(publications.update().where(
            publications.c.id == publication_id).values(
                state=publication_state,
                inspection_lease_token=None,
                inspection_lease_expires_at=None,
                next_retry_at=None,
                error_code=None,
                reservation_expires_at=None,
                record_expires_at=None,
                updated_at=current).returning(publications)).mappings().one()
        operation_values: dict[str, Any] = {
            'result_kind': 'publication',
            'result_id': publication_id,
            'result_json': json.dumps(
                {
                    'publication_id': publication_id,
                    'state': publication_state,
                },
                sort_keys=True),
            'updated_at': current,
        }
        if ready:
            operation_values.update(
                state=models.ImageOperationState.SUCCEEDED.value,
                terminal_expires_at=current + _OPERATION_RETENTION_SECONDS)
        session.execute(schema.operations.update().where(
            schema.operations.c.id == operation_id, schema.operations.c.state ==
            models.ImageOperationState.PENDING.value).values(
                **operation_values))
        return catalog_state._publication(  # pylint: disable=protected-access
            updated)


def create_warming_demand(*,
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
                          now: int | None = None) -> demand_state.DemandRecord:
    """Commits a demand only after exact location and revision revalidation."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        _lock_profile(session,
                      profile_revision_id,
                      states=(models.ImageProfileState.ACTIVE,))
        location = session.execute(
            sqlalchemy.select(schema.locations).where(
                schema.locations.c.id == location_id,
                schema.locations.c.workspace == workspace,
                schema.locations.c.image_id == image_id,
                schema.locations.c.runtime_digest ==
                runtime_digest).with_for_update()).mappings().one()
        if str(location['target_fingerprint']) != target_fingerprint:
            raise ValueError(
                'Demand target fingerprint does not match location.')
        return demand_state.create_demand_in_session(
            session,
            authority_id=authority_id,
            workspace=workspace,
            consumer_kind=consumer_kind,
            consumer_owner=consumer_owner,
            consumer_generation=consumer_generation,
            target_key=target_key,
            owner_epoch=owner_epoch,
            image_id=image_id,
            runtime_digest=runtime_digest,
            profile_revision_id=profile_revision_id,
            target_fingerprint=target_fingerprint,
            location_id=location_id,
            placement=placement,
            now=current)


def create_warming_demand_for_controller_epoch(
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
        now: int | None = None) -> demand_state.DemandRecord:
    """Maps one controller epoch and converges its monotonic demand."""
    current = int(time.time()) if now is None else now
    with orm.Session(catalog_state.engine()) as session, session.begin():
        profile_row = _lock_profile(session,
                                    profile_revision_id,
                                    states=(models.ImageProfileState.ACTIVE,
                                            models.ImageProfileState.RETIRED))
        location = session.execute(
            sqlalchemy.select(schema.locations).where(
                schema.locations.c.id == location_id,
                schema.locations.c.workspace == workspace,
                schema.locations.c.image_id == image_id,
                schema.locations.c.runtime_digest ==
                runtime_digest).with_for_update()).mappings().one()
        if str(location['target_fingerprint']) != target_fingerprint:
            raise ValueError(
                'Demand target fingerprint does not match location.')
        return demand_state.create_demand_for_controller_epoch_in_session(
            session,
            authority_id=authority_id,
            workspace=workspace,
            consumer_kind=consumer_kind,
            consumer_owner=consumer_owner,
            controller_epoch=controller_epoch,
            controller_sequence=controller_sequence,
            allow_epoch_advance=allow_epoch_advance,
            target_key=target_key,
            image_id=image_id,
            runtime_digest=runtime_digest,
            profile_revision_id=profile_revision_id,
            target_fingerprint=target_fingerprint,
            location_id=location_id,
            placement=placement,
            now=current,
            require_existing=(str(
                profile_row['state']) == models.ImageProfileState.RETIRED.value
                             ))


def commit_ready_demand(*,
                        demand_id: str,
                        consumer_generation: int,
                        pull_plan: dict[str, Any],
                        now: int | None = None) -> demand_state.DemandRecord:
    """Atomically fences a READY location and its secret-free runtime plan."""
    current = int(time.time()) if now is None else now
    encoded_plan = json.dumps(pull_plan, sort_keys=True, separators=(',', ':'))
    if len(encoded_plan.encode()) > 16 * 1024:
        raise ValueError('Runtime pull plan exceeds 16 KiB.')
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id == demand_id)).mappings().first()
        if optimistic is None:
            raise demand_state.StaleConsumerGenerationError(
                'Demand no longer exists.')
        profile_row = session.execute(
            sqlalchemy.select(schema.profile_revisions).where(
                schema.profile_revisions.c.id ==
                optimistic['profile_revision_id'],
                schema.profile_revisions.c.workspace ==
                optimistic['workspace'])).mappings().one()
        artifact = session.execute(
            sqlalchemy.select(schema.images).where(
                schema.images.c.id == optimistic['image_id'],
                schema.images.c.workspace ==
                optimistic['workspace']).with_for_update()).mappings().one()
        location = session.execute(
            sqlalchemy.select(schema.locations).where(
                schema.locations.c.id ==
                optimistic['location_id']).with_for_update()).mappings().one()
        location_state = models.ImageLocationState(str(location['state']))
        if location_state != models.ImageLocationState.READY:
            raise DemandLocationNotReadyError(location_state,
                                              location['error_code'])
        if (location['lease_token'] is not None or
                location['target_ref'] is None or
                str(location['runtime_digest']) != str(
                    optimistic['runtime_digest']) or
                str(location['target_fingerprint']) != str(
                    optimistic['target_fingerprint'])):
            raise ValueError(
                'Location is not a lease-free READY demand target.')
        expected_plan = _expected_pull_plan(profile_row=profile_row,
                                            location=location,
                                            artifact=artifact,
                                            demand=optimistic)
        if pull_plan != expected_plan:
            raise ValueError('Runtime pull plan does not match its demand.')
        watermark = session.execute(
            sqlalchemy.select(schema.consumer_watermarks).where(
                schema.consumer_watermarks.c.workspace ==
                optimistic['workspace'],
                schema.consumer_watermarks.c.consumer_kind ==
                optimistic['consumer_kind'],
                schema.consumer_watermarks.c.consumer_owner == optimistic[
                    'consumer_owner']).with_for_update()).mappings().one()
        if (int(watermark['max_seen_generation']) != consumer_generation or int(
                watermark['max_terminal_generation']) >= consumer_generation):
            raise demand_state.StaleConsumerGenerationError(
                'Demand is no longer the current consumer generation.')
        demand = session.execute(
            sqlalchemy.select(schema.demands).where(
                schema.demands.c.id ==
                demand_id).with_for_update()).mappings().one()
        if int(demand['consumer_generation']) != consumer_generation:
            raise demand_state.StaleConsumerGenerationError(
                'Demand consumer generation changed.')
        if str(demand['state']) == models.ImageDemandState.READY.value:
            if str(demand['pull_plan_json']) != encoded_plan:
                raise ValueError('A READY demand pull plan is immutable.')
            return demand_state._demand(  # pylint: disable=protected-access
                demand)
        if str(demand['state']) != models.ImageDemandState.WARMING.value:
            raise ValueError('Only a WARMING demand can become READY.')
        updated = session.execute(schema.demands.update().where(
            schema.demands.c.id == demand_id).values(
                state=models.ImageDemandState.READY.value,
                pull_plan_json=encoded_plan,
                error_code=None,
                updated_at=current).returning(schema.demands)).mappings().one()
        session.execute(schema.locations.update().where(
            schema.locations.c.id == location['id']).values(
                last_used_at=current, updated_at=current))
        return demand_state._demand(  # pylint: disable=protected-access
            updated)


def _activation_budget_facts(
    profile: models.ManagedRegistryProfile,
    attestations: dict[str, Any],
    *,
    now: int,
) -> list[dict[str, Any]]:
    """Returns exact candidate budget facts in a global lock order."""
    facts: list[dict[str, Any]] = []
    expected_shape = {
        'status', 'observed_at', 'provider', 'partition', 'account', 'region',
        'api_family', 'applied_rate_per_second', 'burst'
    }
    for target in (profile.canonical,) + profile.targets:
        key = models.profile_attestation_key('terraform_budget', 'aws',
                                             profile.partition,
                                             profile.registry_account,
                                             target.region, 'ecr')
        evidence = attestations.get(key)
        if (not isinstance(evidence, dict) or set(evidence) != expected_shape or
                evidence.get('status') != 'READY' or
                type(evidence.get('observed_at')) is not int or
                not 0 <= now - evidence['observed_at'] or
                evidence.get('provider') != 'aws' or
                evidence.get('partition') != profile.partition or
                evidence.get('account') != profile.registry_account or
                evidence.get('region') != target.region or
                evidence.get('api_family') != 'ecr' or
                type(evidence.get('applied_rate_per_second')) is not int or
                evidence['applied_rate_per_second'] <= 0 or
                type(evidence.get('burst')) is not int or
                not 1 <= evidence['burst'] <= 64):
            raise ValueError('QUALIFICATION_FAILED')
        facts.append(evidence)
    return sorted(facts,
                  key=lambda item: (item['provider'], item['partition'], item[
                      'account'], item['region'], item['api_family']))


def _activation_shard_capacities(
        shard: sqlalchemy.engine.RowMapping,
        target: models.ManagedRegistryTarget,
        attestations: dict[str, Any]) -> tuple[int, int, int]:
    """Validates revision-scoped limits and live proof for one locked shard."""
    physical_fingerprint = str(shard['physical_fingerprint'])
    terraform_key = models.profile_attestation_key('terraform_shard',
                                                   physical_fingerprint)
    live_key = models.profile_attestation_key('infrastructure_shard',
                                              physical_fingerprint)
    expected = attestations.get(terraform_key)
    live = attestations.get(live_key)
    if (not isinstance(expected, dict) or expected.get('status') != 'READY' or
            expected.get('physical_fingerprint') != physical_fingerprint or
            expected.get('target_fingerprint') != target.target_fingerprint or
            expected.get('target') != target.name or
            expected.get('live_attestation_key') != live_key or
            not isinstance(live, dict) or live.get('status') != 'READY' or
            type(live.get('observed_at')) is not int or
            live.get('physical_fingerprint') != physical_fingerprint or
            live.get('target_fingerprint') != target.target_fingerprint or
            type(live.get('inventory_epoch')) is not int or
            live['inventory_epoch'] != int(shard['inventory_epoch']) or
            type(live.get('inventory_completed_at')) is not int or
            live['inventory_completed_at'] != shard['inventory_completed_at'] or
            shard['inventory_lease_token'] is not None):
        raise ValueError('QUALIFICATION_FAILED')
    max_manifests = expected.get('max_manifests')
    max_declared_bytes = expected.get('max_declared_bytes')
    max_in_flight = expected.get('max_in_flight')
    terraform_quota = expected.get('terraform_applied_quota')
    reserved_headroom = expected.get('reserved_headroom')
    applied_quota = live.get('applied_images_per_repository_quota')
    live_headroom = live.get('reserved_headroom')
    if (type(max_manifests) is not int or max_manifests <= 0 or
            max_manifests > target.max_manifests_per_shard or
            type(max_declared_bytes) is not int or max_declared_bytes <= 0 or
            max_declared_bytes > target.max_declared_bytes_per_shard or
            type(max_in_flight) is not int or max_in_flight <= 0 or
            max_in_flight > target.max_in_flight or
            type(terraform_quota) is not int or terraform_quota <= 0 or
            type(reserved_headroom) is not int or reserved_headroom < 0 or
            type(applied_quota) is not int or applied_quota < terraform_quota or
            live_headroom != reserved_headroom or
            max_manifests + reserved_headroom > applied_quota or
            max_manifests < int(shard['reserved_manifests']) or
            max_declared_bytes < int(shard['reserved_declared_bytes']) or
            max_in_flight < int(shard['in_flight'])):
        raise ValueError('QUALIFICATION_FAILED')
    return max_manifests, max_declared_bytes, max_in_flight


def activate_profile(
        *,
        profile_revision_id: str,
        expected_generation: int,
        expected_config_hash: str,
        expected_terraform_hash: str,
        expected_attestations_hash: str,
        required_attestations: dict[str, int | None],
        now: int | None = None) -> topology_state.ProfileRevisionRecord:
    """Promotes only the still-desired fully attested immutable revision."""
    current = int(time.time()) if now is None else now
    table = schema.profile_revisions
    with orm.Session(catalog_state.engine()) as session, session.begin():
        optimistic = session.execute(
            sqlalchemy.select(table.c.workspace, table.c.profile).where(
                table.c.id == profile_revision_id)).first()
        if optimistic is None:
            raise topology_state.StaleProfileRevisionError(
                'Qualification result no longer exists.')
        profile_rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.workspace == optimistic.workspace,
                table.c.profile == optimistic.profile).order_by(
                    table.c.id).with_for_update()).mappings().all()
        desired = next((row for row in profile_rows
                        if str(row['id']) == profile_revision_id), None)
        if desired is None:
            raise topology_state.StaleProfileRevisionError(
                'Qualification result no longer exists.')
        if (str(desired['state']) != models.ImageProfileState.QUALIFYING.value
                or int(desired['desired_generation']) != expected_generation or
                str(desired['config_hash']) != expected_config_hash or
                str(desired['terraform_hash']) != expected_terraform_hash or
                str(desired['attestations_hash'])
                != expected_attestations_hash):
            raise topology_state.StaleProfileRevisionError(
                'Qualification result no longer matches the desired revision.')
        attestations = json.loads(str(desired['attestations_json']))
        for attestation, max_age_seconds in required_attestations.items():
            evidence = attestations.get(attestation)
            if (not isinstance(evidence, dict) or
                    evidence.get('status') != 'READY' or
                    not isinstance(evidence.get('observed_at'), int) or
                    current < evidence['observed_at'] or
                (max_age_seconds is not None and
                 current - evidence['observed_at'] > max_age_seconds)):
                raise ValueError('QUALIFICATION_FAILED')
        configured = models.ManagedRegistryProfile.from_snapshot(
            json.loads(str(desired['config_json'])))
        budget_facts = _activation_budget_facts(configured,
                                                attestations,
                                                now=current)
        for fact in budget_facts:
            topology_state.upsert_provider_budget_in_session(
                session,
                provider=fact['provider'],
                partition=fact['partition'],
                account=fact['account'],
                region=fact['region'],
                api_family=fact['api_family'],
                applied_rate_per_second=fact['applied_rate_per_second'],
                burst=fact['burst'],
                now=current)
        shards = schema.registry_shards
        targets = (configured.canonical,) + configured.targets
        target_by_name = {target.name: target for target in targets}
        shard_rows = session.execute(
            sqlalchemy.select(shards).where(
                shards.c.workspace == desired['workspace'],
                shards.c.profile == desired['profile']).order_by(
                    shards.c.id).with_for_update()).mappings().all()
        if len(shard_rows) != sum(target.shard_count for target in targets):
            raise ValueError('QUALIFICATION_FAILED')
        target_counts = {target.name: 0 for target in targets}
        active_ids = {
            str(row['id'])
            for row in profile_rows
            if str(row['state']) == models.ImageProfileState.ACTIVE.value
        }
        for shard in shard_rows:
            target = target_by_name.get(str(shard['target_id']))
            if (target is None or
                    shard['target_fingerprint'] != target.target_fingerprint or
                    str(shard['state'])
                    not in (models.ImageShardState.READY.value,
                            models.ImageShardState.FULL.value) or
                (shard['profile_revision_id'] is not None and
                 str(shard['profile_revision_id']) not in active_ids)):
                raise ValueError('QUALIFICATION_FAILED')
            target_counts[target.name] += 1
            max_manifests, max_declared_bytes, max_in_flight = (
                _activation_shard_capacities(shard, target, attestations))
            full = (int(shard['reserved_manifests']) >= max_manifests or
                    int(shard['reserved_declared_bytes']) >= max_declared_bytes)
            session.execute(
                shards.update().where(shards.c.id == shard['id']).values(
                    profile_revision_id=profile_revision_id,
                    eviction_enabled=(target.delete_authority is not None),
                    max_manifests=max_manifests,
                    max_declared_bytes=max_declared_bytes,
                    max_in_flight=max_in_flight,
                    state=(models.ImageShardState.FULL.value
                           if full else models.ImageShardState.READY.value),
                    updated_at=current))
        if any(target_counts[target.name] != target.shard_count
               for target in targets):
            raise ValueError('QUALIFICATION_FAILED')
        session.execute(table.update().where(
            table.c.workspace == desired['workspace'],
            table.c.profile == desired['profile'],
            table.c.state == models.ImageProfileState.ACTIVE.value).values(
                state=models.ImageProfileState.RETIRED.value,
                updated_at=current))
        updated = session.execute(
            table.update().where(table.c.id == profile_revision_id).values(
                state=models.ImageProfileState.ACTIVE.value,
                qualified_at=current,
                failed_code=None,
                updated_at=current).returning(table)).mappings().one()
        return topology_state._profile(  # pylint: disable=protected-access
            updated)
