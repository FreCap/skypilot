"""Explicit, provider-free image publication service."""

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
class PublicationMutation:
    operation: catalog_state.OperationRecord
    publication: catalog_state.PublicationRecord


def _request_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True,
                         separators=(',', ':')).encode()
    return hashlib.sha256(encoded).hexdigest()


def publish(*,
            source_ref: str,
            release: str,
            distribution: str,
            workspace: str,
            actor_hash: str,
            idempotency_key: str,
            requested_platform: str = 'linux/amd64',
            source_auth_binding_id: str | None = None) -> PublicationMutation:
    """Reserves one release and queues source inspection without provider I/O."""
    source_ref = models.validate_oci_reference(source_ref, 'Publication source')
    _, source_digest = models.split_digest(source_ref)
    if source_digest is None:
        raise ValueError('Publication source must be digest-pinned.')
    release = models.validate_release_label(release, 'Publication release')
    requested_platform = models.validate_oci_platform(requested_platform,
                                                      'Publication platform')
    distribution = models.validate_control_plane_identifier(
        distribution, 'Publication distribution')
    if distribution == config.DIRECT_PROFILE:
        raise ValueError('Publication requires a managed distribution.')
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
    source_binding = config.get_source_binding(source_auth_binding_id)
    source_fingerprint = (source_binding.fingerprint
                          if source_binding is not None else None)
    authority_id = catalog_state.get_catalog_authority_id()
    assert authority_id is not None
    request_hash = _request_hash({
        'authority_id': authority_id,
        'workspace': workspace,
        'source_ref': source_ref,
        'release': release,
        'distribution': profile.name,
        'profile_revision_id': active.id,
        'requested_platform': requested_platform,
        'source_auth_binding_id': source_auth_binding_id,
        'source_auth_fingerprint': source_fingerprint,
    })
    operation, publication = catalog_state.create_publication(
        authority_id=authority_id,
        workspace=workspace,
        actor_hash=actor_hash,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        profile_revision_id=active.id,
        release=release,
        source_ref=source_ref,
        source_root_digest=source_digest,
        requested_platform=requested_platform,
        source_auth_binding_id=source_auth_binding_id,
        source_auth_fingerprint=source_fingerprint)
    return PublicationMutation(operation=operation, publication=publication)


def retry(*, publication_id: str, workspace: str, actor_hash: str,
          idempotency_key: str) -> PublicationMutation:
    """Requeues one retained failed release reservation idempotently."""
    publication_id = models.validate_catalog_id(publication_id,
                                                'Publication ID')
    existing = catalog_state.get_publication(publication_id, workspace)
    if existing is None:
        raise ValueError('IMAGE_PUBLICATION_NOT_FOUND')
    authority_id = catalog_state.get_catalog_authority_id()
    assert authority_id is not None
    request_hash = _request_hash({
        'authority_id': authority_id,
        'workspace': workspace,
        'publication_id': publication_id,
    })
    operation, _ = catalog_state.create_or_get_operation(
        authority_id=authority_id,
        scope=workspace,
        actor_hash=actor_hash,
        kind='RETRY_PUBLICATION',
        idempotency_key=idempotency_key,
        request_hash=request_hash)
    if operation.result_id is not None:
        publication = catalog_state.get_publication(operation.result_id,
                                                    workspace)
        if publication is None or operation.result_kind != 'publication':
            raise ValueError('Operation publication result is unavailable.')
        return PublicationMutation(operation=operation, publication=publication)
    publication = transactions.retry_publication(publication_id=publication_id,
                                                 workspace=workspace,
                                                 operation_id=operation.id)
    refreshed = catalog_state.get_operation(operation.id, workspace)
    if refreshed is None:
        raise RuntimeError('Publication retry operation disappeared.')
    return PublicationMutation(operation=refreshed, publication=publication)
