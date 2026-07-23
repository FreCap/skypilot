"""Idempotent, provider-free regional preparation and retry services."""

from __future__ import annotations

import dataclasses
import hashlib
import json

from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import models
from sky.container_images import topology_state
from sky.container_images import transactions


@dataclasses.dataclass(frozen=True)
class LocationMutation:
    operation: catalog_state.OperationRecord
    location: topology_state.LocationRecord


def _hash(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True,
                   separators=(',', ':')).encode()).hexdigest()


def _terminal(
    location: topology_state.LocationRecord
) -> tuple[models.ImageOperationState | None, str | None]:
    if location.state == models.ImageLocationState.READY:
        return models.ImageOperationState.SUCCEEDED, None
    if location.state in (models.ImageLocationState.FAILED,
                          models.ImageLocationState.QUARANTINED):
        return models.ImageOperationState.FAILED, location.error_code
    return None, None


def _raise_retry_conflict(error_code: str | None) -> None:
    if error_code == 'REGISTRY_LOCATION_QUARANTINED':
        raise topology_state.RegistryLocationQuarantinedError(error_code)
    if error_code == 'REGISTRY_SHARD_UNAVAILABLE':
        raise topology_state.RegistryShardUnavailableError(error_code)


def prepare(*, image_id: str, distribution: str, target_id: str, workspace: str,
            actor_hash: str, idempotency_key: str) -> LocationMutation:
    artifact = catalog_state.get_published_artifact(image_id, workspace)
    if artifact is None:
        raise ValueError('IMAGE_NOT_PUBLISHED')
    configured_profile, _ = config.resolve_profile(distribution, workspace)
    if configured_profile is None:
        raise ValueError('PROFILE_NOT_ACTIVE')
    active = topology_state.get_active_profile(workspace,
                                               configured_profile.name)
    if active is None:
        raise ValueError('PROFILE_NOT_ACTIVE')
    profile = models.ManagedRegistryProfile.from_snapshot(
        active.config_snapshot)
    if profile.name != configured_profile.name:
        raise ValueError('PROFILE_NOT_ACTIVE')
    target = profile.target(target_id)
    publication = catalog_state.get_ready_publication_for_artifact(
        artifact.id, workspace)
    if publication is None or publication.canonical_location_id is None:
        raise ValueError('ARTIFACT_NOT_READY')
    publication_revision = topology_state.get_profile_revision(
        publication.profile_revision_id)
    if (publication_revision is None or
            publication_revision.profile != profile.name):
        raise ValueError('ARTIFACT_NOT_READY')
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    request_hash = _hash({
        'authority_id': authority,
        'workspace': workspace,
        'image_id': artifact.id,
        'runtime_digest': artifact.runtime_digest,
        'profile_revision_id': active.id,
        'target_id': target.name,
        'target_fingerprint': target.target_fingerprint,
    })
    operation, _ = catalog_state.create_or_get_operation(
        authority_id=authority,
        scope=workspace,
        actor_hash=actor_hash,
        kind='PREPARE',
        idempotency_key=idempotency_key,
        request_hash=request_hash)
    if operation.result_id is not None:
        location = topology_state.get_location(operation.result_id)
        if location is None or operation.result_kind != 'location':
            raise ValueError('Operation location result is unavailable.')
        return LocationMutation(operation=operation, location=location)
    location = topology_state.get_location_for_target(
        image_id=artifact.id,
        workspace=workspace,
        target_fingerprint=target.target_fingerprint,
        runtime_digest=artifact.runtime_digest)
    if location is None:
        if target is profile.canonical:
            raise ValueError('ARTIFACT_NOT_READY')
        location = transactions.reserve_regional_location(
            image_id=artifact.id,
            workspace=workspace,
            profile_revision_id=active.id,
            target_id=target.name,
            canonical_location_id=publication.canonical_location_id,
            max_regional_locations=(
                profile.limits.max_regional_locations_per_artifact))
    elif location.state in (models.ImageLocationState.FAILED,
                            models.ImageLocationState.MISSING,
                            models.ImageLocationState.EVICTED):
        readmitted = topology_state.retry_location(location.id, workspace)
        if readmitted is not None:
            location = readmitted
        else:
            location = topology_state.get_location(location.id)
            if location is None or location.workspace != workspace:
                raise ValueError('ARTIFACT_NOT_READY')
    terminal_state, error_code = _terminal(location)
    operation = catalog_state.bind_operation_result(
        operation.id,
        result_kind='location',
        result_id=location.id,
        result={
            'location_id': location.id,
            'state': location.state.value,
        },
        terminal_state=terminal_state,
        error_code=error_code)
    return LocationMutation(operation=operation, location=location)


def retry_location(*, location_id: str, workspace: str, actor_hash: str,
                   idempotency_key: str) -> LocationMutation:
    existing = topology_state.get_location(location_id)
    if existing is None or existing.workspace != workspace:
        raise ValueError('IMAGE_LOCATION_NOT_FOUND')
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    request_hash = _hash({
        'authority_id': authority,
        'workspace': workspace,
        'location_id': location_id,
    })
    operation, _ = catalog_state.create_or_get_operation(
        authority_id=authority,
        scope=workspace,
        actor_hash=actor_hash,
        kind='RETRY_LOCATION',
        idempotency_key=idempotency_key,
        request_hash=request_hash)
    if operation.result_id is not None:
        location = topology_state.get_location(operation.result_id)
        if location is None:
            raise ValueError('Operation location result is unavailable.')
        _raise_retry_conflict(operation.error_code)
        return LocationMutation(operation=operation, location=location)
    location = topology_state.retry_location(location_id, workspace)
    if location is None:
        location = topology_state.get_location(location_id)
    if location is None or location.workspace != workspace:
        raise ValueError('IMAGE_LOCATION_NOT_FOUND')
    conflict_code: str | None = None
    if location.state == models.ImageLocationState.QUARANTINED:
        conflict_code = 'REGISTRY_LOCATION_QUARANTINED'
    if location.state in (models.ImageLocationState.FAILED,
                          models.ImageLocationState.MISSING,
                          models.ImageLocationState.EVICTED):
        conflict_code = 'REGISTRY_SHARD_UNAVAILABLE'
    if conflict_code is not None:
        operation = catalog_state.bind_operation_result(
            operation.id,
            result_kind='location',
            result_id=location.id,
            result={
                'location_id': location.id,
                'state': location.state.value,
            },
            terminal_state=models.ImageOperationState.FAILED,
            error_code=conflict_code)
        _raise_retry_conflict(operation.error_code)
        return LocationMutation(operation=operation, location=location)
    terminal_state, error_code = _terminal(location)
    operation = catalog_state.bind_operation_result(
        operation.id,
        result_kind='location',
        result_id=location.id,
        result={
            'location_id': location.id,
            'state': location.state.value
        },
        terminal_state=terminal_state,
        error_code=error_code)
    return LocationMutation(operation=operation, location=location)
