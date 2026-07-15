"""API-server operations for immutable image distribution."""

import dataclasses
import os
import time
import typing

from sky import skypilot_config
from sky.container_images import config as image_config
from sky.container_images import models
from sky.container_images import providers
from sky.container_images import references
from sky.container_images import resolver
from sky.container_images import state
from sky.provision import docker_utils
from sky.schemas.api import responses
from sky.skylet import constants
from sky.utils import common_utils

if typing.TYPE_CHECKING:
    from sky import resources as resources_lib

_MAX_UNPAGINATED_STATUS_RECORDS = 1000
_MAX_UNPAGINATED_STATUS_ASSOCIATIONS = 10_000
_MAX_PREPARE_TARGETS = 128


@dataclasses.dataclass(frozen=True)
class _DeploymentSnapshotPlan:
    """Read-only identity plan committed only after candidate convergence."""

    image_spec: models.ContainerImage
    workspace: str
    profile: models.RegistryProfile | None
    digest: str | None
    existing_record: state.ImageRecord | None = None
    resolved_source_ref: str | None = None
    policy: models.WorkspaceImagePolicy | None = None


def _workspace(workspace: str | None) -> str:
    resolved = workspace
    if resolved is None:
        resolved = skypilot_config.get_active_workspace()
    if resolved is None:
        resolved = constants.SKYPILOT_DEFAULT_WORKSPACE
    return models.validate_workspace_name(resolved, 'Container image workspace')


def _response(
    record: state.ImageRecord,
    *,
    sources: list[state.SourceRecord] | None = None,
    releases: list[state.ReleaseRecord] | None = None,
    location_records: list[state.LocationRecord] | None = None,
) -> responses.ContainerImageRecord:
    if sources is None:
        sources = state.list_sources(
            record.id,
            record.workspace,
            limit=_MAX_UNPAGINATED_STATUS_ASSOCIATIONS + 1)
    if releases is None:
        releases = state.list_releases(
            record.id,
            record.workspace,
            limit=_MAX_UNPAGINATED_STATUS_ASSOCIATIONS + 1)
    if location_records is None:
        location_records = state.list_locations(
            record.id, limit=_MAX_UNPAGINATED_STATUS_ASSOCIATIONS + 1)
    if any(
            len(records) > _MAX_UNPAGINATED_STATUS_ASSOCIATIONS
            for records in (sources, releases, location_records)):
        raise ValueError(
            'Container image status has too many associations to return '
            'without pagination. Filter by one artifact or reduce aliases.')
    location_responses = [
        responses.ContainerImageLocationRecord(
            id=location.id,
            image_id=location.image_id,
            distribution=location.profile,
            target_id=location.target_id,
            target_fingerprint=location.target_fingerprint,
            policy_fingerprint=location.policy_fingerprint,
            profile_revision=location.profile_revision,
            canonical=location.canonical,
            canonical_location_id=location.canonical_location_id,
            target_ref=location.target_ref,
            expected_digest=location.expected_digest,
            state=location.state.value,
            attempt_count=location.attempt_count,
            next_retry_at=location.next_retry_at,
            last_verified_at=location.last_verified_at,
            verification_requested_at=location.verification_requested_at,
            last_used_at=location.last_used_at,
            auto_evict=location.auto_evict,
            last_error=location.last_error,
            updated_at=location.updated_at,
        ) for location in location_records
    ]
    return responses.ContainerImageRecord(
        id=record.id,
        workspace=record.workspace,
        source_ref=record.source_ref,
        resolved_source_ref=record.resolved_source_ref,
        sources=[source.source_ref for source in sources],
        source_digest=record.source_digest,
        releases=[release.name for release in releases],
        producer_kind=record.producer_kind,
        producer_spec_hash=record.producer_spec_hash,
        builder_version=record.builder_version,
        platforms=list(record.platforms),
        compressed_size_bytes=record.compressed_size_bytes,
        created_at=record.created_at,
        updated_at=record.updated_at,
        locations=location_responses,
    )


def _responses(
    records: list[state.ImageRecord],
    workspace: str,
) -> list[responses.ContainerImageRecord]:
    image_ids = [record.id for record in records]
    sources, releases, locations = state.list_image_associations(
        image_ids,
        workspace,
        max_rows_per_kind=_MAX_UNPAGINATED_STATUS_ASSOCIATIONS)
    return [
        _response(record,
                  sources=sources[record.id],
                  releases=releases[record.id],
                  location_records=locations[record.id]) for record in records
    ]


def _pin_reference(reference: str, digest: str) -> str:
    repository, existing_digest = models.split_digest(reference)
    if existing_digest is not None:
        if existing_digest != digest.lower():
            raise ValueError('Source reference digest changed while it was '
                             'being resolved.')
        return f'{repository}@{existing_digest}'
    last_slash = repository.rfind('/')
    last_colon = repository.rfind(':')
    if last_colon > last_slash:
        repository = repository[:last_colon]
    return f'{repository}@{digest.lower()}'


def _resolve_source(reference: str) -> tuple[str, str]:
    _, digest = models.split_digest(reference)
    if digest is None:
        raise ValueError(
            'Managed container image sources must be digest-pinned. Resolving '
            'a mutable tag requires an isolated, credential-aware metadata '
            'worker and is not performed inside the API request process.')
    return _pin_reference(reference, digest), digest


def _record_for_selector(image_spec: models.ContainerImage,
                         workspace: str) -> state.ImageRecord | None:
    """Resolves a selector without mutating catalog state."""
    candidates: list[state.ImageRecord] = []
    if image_spec.artifact_id is not None:
        record = state.get_image(image_spec.artifact_id, workspace)
        return record
    if image_spec.release is not None:
        release_record = state.get_image_by_release(image_spec.release,
                                                    workspace)
        if release_record is not None:
            candidates.append(release_record)
    if image_spec.ref is not None:
        source_record = state.get_image_by_source_ref(image_spec.ref, workspace)
        if source_record is not None:
            candidates.append(source_record)
    # A digest validates consistency between supplied selector fields, but it
    # is not itself proof that an explicit ref= source alias is registered.
    # Only add the content candidate after an exact source or release selector
    # has established identity.
    if not candidates:
        return None
    if image_spec.digest is not None:
        digest_record = state.get_image_by_digest(image_spec.digest, workspace)
        if digest_record is not None:
            candidates.append(digest_record)
    first = candidates[0]
    if any(candidate.id != first.id for candidate in candidates[1:]):
        raise ValueError('Container image selector fields resolve to different '
                         'immutable artifacts.')
    if (image_spec.digest is not None and
            first.source_digest != image_spec.digest):
        raise ValueError('Container image selector fields resolve to different '
                         'immutable artifacts.')
    return first


def _ensure_record(image_spec: models.ContainerImage, workspace: str,
                   profile: models.RegistryProfile,
                   policy: models.WorkspaceImagePolicy) -> state.ImageRecord:
    """Resolves or atomically publishes a source-backed artifact."""
    if image_spec.artifact_id is not None:
        record = state.get_image(image_spec.artifact_id, workspace)
        if record is None:
            raise ValueError('Container image artifact was not found in the '
                             'requested workspace.')
        return record
    if image_spec.ref is None:
        assert image_spec.release is not None
        record = state.get_image_by_release(image_spec.release, workspace)
        if record is None:
            raise ValueError('Container image release was not found in the '
                             'requested workspace.')
        return record

    resolved_ref, digest = _resolve_source(image_spec.ref)
    canonical = profile.canonical
    record = state.publish_image(
        source_ref=image_spec.ref,
        resolved_source_ref=resolved_ref,
        source_digest=digest,
        workspace=workspace,
        creator_user_hash=common_utils.get_current_user().id,
        release=image_spec.release,
        profile=profile.name,
        target_id=canonical.name,
        target_fingerprint=profile.physical_fingerprint(canonical),
        policy_fingerprint=profile.policy_fingerprint(canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        max_artifacts=policy.max_artifacts,
        max_sources_per_artifact=policy.max_sources_per_artifact,
        max_releases_per_artifact=policy.max_releases_per_artifact,
    )
    return record


def _profile_for_spec(
    image_spec: models.ContainerImage,
    workspace: str,
) -> tuple[models.RegistryProfile | None, models.WorkspaceImagePolicy]:
    profile, policy = image_config.resolve_profile(image_spec.distribution,
                                                   workspace)
    image_config.validate_managed_source_policy(image_spec, profile, policy)
    if profile is None and image_spec.ref is None:
        raise ValueError('A release or artifact selector requires a managed '
                         'distribution so SkyPilot can resolve a physical '
                         'runtime reference.')
    return profile, policy


def _plan_snapshot_for_deployment(
    image: models.ContainerImage | str | dict[str, str],
    workspace: str,
) -> _DeploymentSnapshotPlan:
    """Resolves deployment identity without mutating the image catalog."""
    image_spec = models.ContainerImage.from_config(image)
    profile, policy = _profile_for_spec(image_spec, workspace)
    if profile is None:
        return _DeploymentSnapshotPlan(image_spec,
                                       workspace,
                                       None,
                                       None,
                                       policy=policy)
    _validate_catalog_authority()
    if image_spec.artifact_id is not None:
        record = state.get_image(image_spec.artifact_id, workspace)
        if record is None:
            raise ValueError('Container image artifact was not found in the '
                             'requested workspace.')
        return _DeploymentSnapshotPlan(image_spec,
                                       workspace,
                                       profile,
                                       record.source_digest,
                                       record,
                                       policy=policy)
    if image_spec.ref is None:
        assert image_spec.release is not None
        record = state.get_image_by_release(image_spec.release, workspace)
        if record is None:
            raise ValueError('Container image release was not found in the '
                             'requested workspace.')
        return _DeploymentSnapshotPlan(image_spec,
                                       workspace,
                                       profile,
                                       record.source_digest,
                                       record,
                                       policy=policy)

    resolved_ref, digest = _resolve_source(image_spec.ref)
    record = _record_for_selector(image_spec, workspace)
    if record is None:
        # Content identity is safe to discover by digest during planning, but
        # the exact source alias is not registered until the atomic commit.
        record = state.get_image_by_digest(digest, workspace)
    if record is not None and record.source_digest != digest:
        raise ValueError('Container image selector fields resolve to different '
                         'immutable artifacts.')
    return _DeploymentSnapshotPlan(image_spec, workspace, profile, digest,
                                   record, resolved_ref, policy)


def _commit_deployment_snapshot_plans(
    plans: list[_DeploymentSnapshotPlan],
) -> list[tuple[models.ContainerImage, str | None]]:
    """Commits all source-backed plans together and returns pinned selectors."""
    publications: list[state.ImagePublication] = []
    publication_plan_indices: list[int] = []
    creator_user_hash: str | None = None
    for index, plan in enumerate(plans):
        if plan.profile is None or plan.image_spec.ref is None:
            continue
        assert plan.digest is not None
        assert plan.resolved_source_ref is not None
        assert plan.policy is not None
        if creator_user_hash is None:
            creator_user_hash = common_utils.get_current_user().id
        canonical = plan.profile.canonical
        publications.append(
            state.ImagePublication(
                source_ref=plan.image_spec.ref,
                resolved_source_ref=plan.resolved_source_ref,
                source_digest=plan.digest,
                workspace=plan.workspace,
                creator_user_hash=creator_user_hash,
                release=plan.image_spec.release,
                profile=plan.profile.name,
                target_id=canonical.name,
                target_fingerprint=plan.profile.physical_fingerprint(canonical),
                policy_fingerprint=plan.profile.policy_fingerprint(
                    canonical, True),
                profile_revision=plan.profile.revision,
                profile_revision_fingerprint=(
                    plan.profile.revision_fingerprint),
                max_artifacts=plan.policy.max_artifacts,
                max_sources_per_artifact=(plan.policy.max_sources_per_artifact),
                max_releases_per_artifact=(
                    plan.policy.max_releases_per_artifact),
            ))
        publication_plan_indices.append(index)

    published_by_plan: dict[int, state.ImageRecord] = {}
    for plan_index, record in zip(
            publication_plan_indices,
            state.publish_images_atomically(publications),
    ):
        published_by_plan[plan_index] = record

    snapshots: list[tuple[models.ContainerImage, str | None]] = []
    for index, plan in enumerate(plans):
        if plan.profile is None:
            snapshots.append((plan.image_spec, None))
            continue
        snapshot_record = published_by_plan.get(index, plan.existing_record)
        if (snapshot_record is None or
                snapshot_record.source_digest != plan.digest):
            raise RuntimeError('Container image deployment snapshot committed '
                               'without its planned immutable artifact.')
        snapshots.append((
            models.ContainerImage(artifact_id=snapshot_record.id,
                                  distribution=plan.profile.name),
            snapshot_record.id,
        ))
    return snapshots


def snapshot_for_deployment_batch(
    images: typing.Sequence[models.ContainerImage | str | dict[str, str]],
    workspace: str | None = None,
) -> list[tuple[models.ContainerImage, str | None]]:
    """Pins a convergent candidate set after one read-only identity phase.

    A rejected set performs no artifact, alias, profile, or location writes.
    Source-backed candidates that pass convergence publish together in one
    transaction, so a later alias or policy conflict rolls the whole set back.
    """
    return snapshot_for_deployment_groups([images], workspace)[0]


def snapshot_for_deployment_groups(
    image_groups: typing.Sequence[typing.Sequence[models.ContainerImage | str |
                                                  dict[str, str]]],
    workspace: str | None = None,
) -> list[list[tuple[models.ContainerImage, str | None]]]:
    """Pins independent candidate groups in one all-or-nothing transaction.

    Candidate identity must converge within each task or service version, but
    different tasks in a managed-job DAG may intentionally use different
    artifacts. Every group is planned before any catalog write and every
    source-backed plan is then committed in one publication transaction.
    """
    workspace = _workspace(workspace)
    plans_by_group = [[
        _plan_snapshot_for_deployment(image, workspace) for image in images
    ] for images in image_groups]
    for plans in plans_by_group:
        if not plans:
            continue
        managed_plans = [plan for plan in plans if plan.profile is not None]
        if managed_plans:
            valid = (len(managed_plans) == len(plans) and
                     len({plan.digest for plan in managed_plans}) == 1)
        else:
            valid = len({plan.image_spec for plan in plans}) == 1
        if not valid:
            raise ValueError(
                'All resource candidates in one workload version must '
                'resolve to the same immutable container artifact. Use one '
                'managed artifact across placements or one identical direct '
                'image.')

    flattened = [plan for plans in plans_by_group for plan in plans]
    committed = _commit_deployment_snapshot_plans(flattened)
    results: list[list[tuple[models.ContainerImage, str | None]]] = []
    offset = 0
    for plans in plans_by_group:
        next_offset = offset + len(plans)
        results.append(committed[offset:next_offset])
        offset = next_offset
    return results


def snapshot_for_deployment(
    image: models.ContainerImage | str | dict[str, str],
    workspace: str | None = None,
) -> tuple[models.ContainerImage, str | None]:
    """Resolves one managed selector to an immutable deployment artifact.

    Direct images have no catalog artifact and are returned unchanged. Managed
    sources are atomically published before the snapshot is returned, while
    releases and artifact IDs must already resolve in the workspace.
    """
    return snapshot_for_deployment_batch([image], workspace)[0]


def _validate_record_platform(record: state.ImageRecord,
                              placement: models.Placement) -> None:
    if not models.platforms_support_runtime(record.platforms,
                                            placement.platform):
        raise resolver.ImageRouteUnavailableError(
            'The immutable container artifact does not support the selected '
            f'runtime platform {placement.platform!r}.')


def _ensure_target(
    record: state.ImageRecord,
    profile: models.RegistryProfile,
    target: models.RegistryTarget,
    *,
    canonical: bool,
    canonical_location_id: str | None = None,
) -> state.LocationRecord:
    return state.ensure_location(
        record.id,
        profile.name,
        target.name,
        profile.physical_fingerprint(target),
        record.source_digest,
        policy_fingerprint=profile.policy_fingerprint(target, canonical),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical=canonical,
        canonical_location_id=canonical_location_id,
        auto_evict=(not canonical and
                    profile.ownership == models.RegistryOwnership.MANAGED),
    )


def _runtime_local_targets(
    profile: models.RegistryProfile,
    placement: models.Placement,
) -> list[models.RegistryTarget]:
    """Returns local targets whose runtime pull authority is provable."""
    candidates = [profile.canonical, *profile.targets]
    exact_matches: list[models.RegistryTarget] = []
    locality_matches: list[models.RegistryTarget] = []
    for target in candidates:
        auth = providers.get_adapter(target.provider).resolve_runtime_pull_auth(
            target, placement)
        if auth is None:
            continue
        exact = (placement.registry_provider == target.provider and
                 placement.registry_region == target.region and
                 placement.registry_prefix == target.registry_prefix)
        if exact:
            exact_matches.append(target)
        elif target.is_local_to(placement.provider, placement.region):
            locality_matches.append(target)
    # An exact Kubernetes context binding is authoritative.  A broadly local
    # anonymous mirror must not win merely because its target name sorts first.
    matching = exact_matches or locality_matches
    if placement.backend == 'vm' and len(matching) > 1:
        target_names = sorted(target.name for target in matching)
        raise ValueError(
            'Managed container image locality is ambiguous for this VM '
            f'placement: targets {target_names!r} all match '
            f'{placement.provider}/{placement.region}, but a VM placement '
            'does not identify an '
            'exact registry account or project. Configure at most one '
            'runtime-accessible target per provider and region in a VM '
            'distribution, or split the endpoints into separate '
            'distributions.')
    return matching


def _local_target(profile: models.RegistryProfile,
                  placement: models.Placement) -> models.RegistryTarget | None:
    matching = _runtime_local_targets(profile, placement)
    if not matching:
        return None
    return sorted(matching,
                  key=lambda target:
                  (target is profile.canonical, target.name))[0]


def route_satisfies_locality(profile: models.RegistryProfile,
                             target: models.RegistryTarget,
                             placement: models.Placement,
                             locality: models.Locality) -> bool:
    """Checks one route against the current workspace locality policy."""
    if locality == models.Locality.CANONICAL:
        return target is profile.canonical
    local_targets = _runtime_local_targets(profile, placement)
    if locality == models.Locality.REQUIRE:
        return target in local_targets
    assert locality == models.Locality.PREFER, locality
    return target is profile.canonical or target in local_targets


def _ensure_for_placement(record: state.ImageRecord,
                          profile: models.RegistryProfile,
                          placement: models.Placement) -> None:
    """Creates canonical and selected-region intents after real placement."""
    # Resolve locality before creating any intent so an ambiguous VM profile
    # fails without leaving a partial canonical preparation behind.
    local_target = _local_target(profile, placement)
    canonical = _ensure_target(record,
                               profile,
                               profile.canonical,
                               canonical=True)
    if local_target is not None and local_target is not profile.canonical:
        _ensure_target(record,
                       profile,
                       local_target,
                       canonical=False,
                       canonical_location_id=canonical.id)


def publish(
    image: str | dict[str, str],
    workspace: str | None = None,
) -> responses.ContainerImageRecord:
    """Publishes content identity and an optional immutable release alias."""
    workspace = _workspace(workspace)
    image_spec = models.ContainerImage.from_config(image)
    profile, policy = _profile_for_spec(image_spec, workspace)
    if profile is None:
        raise ValueError('Direct unmanaged images are not catalog artifacts.')
    if image_spec.ref is None:
        raise ValueError(
            'Publishing requires a digest-pinned OCI source reference. '
            'Existing releases and artifacts are consumed or prepared, not '
            'republished.')
    resolved_ref, digest = _resolve_source(image_spec.ref)
    canonical = profile.canonical
    record = state.publish_image(
        source_ref=image_spec.ref,
        resolved_source_ref=resolved_ref,
        source_digest=digest,
        workspace=workspace,
        creator_user_hash=common_utils.get_current_user().id,
        release=image_spec.release,
        profile=profile.name,
        target_id=canonical.name,
        target_fingerprint=profile.physical_fingerprint(canonical),
        policy_fingerprint=profile.policy_fingerprint(canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        max_artifacts=policy.max_artifacts,
        max_sources_per_artifact=policy.max_sources_per_artifact,
        max_releases_per_artifact=policy.max_releases_per_artifact,
    )
    return _response(record)


def register(
    image: str | dict[str, str],
    workspace: str | None = None,
) -> responses.ContainerImageRecord:
    """Compatibility alias for :func:`publish`."""
    return publish(image, workspace)


def _find_image(identifier: str, workspace: str) -> state.ImageRecord | None:
    identifier = models.validate_operational_image_selector(identifier)
    explicit = models.parse_explicit_image_selector(identifier)
    if explicit is not None:
        return _record_for_selector(explicit, workspace)

    candidates = []
    try:
        artifact_id = models.validate_catalog_id(identifier,
                                                 'Image artifact selector')
    except ValueError:
        artifact_id = None
    artifact = (state.get_image(artifact_id, workspace)
                if artifact_id is not None else None)
    if artifact is not None:
        candidates.append(artifact)
    release = state.get_image_by_release(identifier, workspace)
    if release is not None:
        candidates.append(release)
    try:
        image_spec = models.ContainerImage.from_config(identifier)
    except ValueError:
        image_spec = None
    if image_spec is not None:
        source = _record_for_selector(image_spec, workspace)
        if source is not None:
            candidates.append(source)
    unique = {candidate.id: candidate for candidate in candidates}
    if len(unique) > 1:
        raise ValueError(
            'Container image selector is ambiguous across artifact, release, '
            'and source namespaces. Use ref=<reference>, release=<name>, or '
            'artifact_id=<id>.')
    return next(iter(unique.values()), None)


def status(
    image: str | None = None,
    workspace: str | None = None,
) -> list[responses.ContainerImageRecord]:
    workspace = _workspace(workspace)
    if image is None:
        records = state.list_images(workspace,
                                    limit=_MAX_UNPAGINATED_STATUS_RECORDS + 1)
        if len(records) > _MAX_UNPAGINATED_STATUS_RECORDS:
            raise ValueError(
                f'Workspace {workspace!r} has more than '
                f'{_MAX_UNPAGINATED_STATUS_RECORDS} image artifacts. Filter '
                'by artifact ID, release, or source reference.')
    else:
        record = _find_image(image, workspace)
        if record is None:
            raise ValueError('Container image was not found in the requested '
                             'workspace.')
        records = [record]
    return _responses(records, workspace)


def prepare(
    image: str | dict[str, str],
    targets: list[str],
    workspace: str | None = None,
    distribution: str | None = None,
) -> responses.ContainerImageRecord:
    """Eagerly creates materialization intents for selected targets."""
    if not targets:
        raise ValueError('At least one image target must be specified.')
    if len(targets) > _MAX_PREPARE_TARGETS:
        raise ValueError('Too many image preparation targets were specified.')
    targets = [
        models.validate_control_plane_identifier(target,
                                                 'Container image target')
        for target in targets
    ]
    if len(targets) != len(set(targets)):
        raise ValueError('Image preparation targets must be unique.')
    if distribution is not None:
        distribution = models.validate_control_plane_identifier(
            distribution, 'Container image distribution')
    workspace = _workspace(workspace)
    if isinstance(image, str):
        explicit = models.parse_explicit_image_selector(image)
        if explicit is not None:
            image_spec = explicit
            record = _record_for_selector(image_spec, workspace)
        else:
            record = _find_image(image, workspace)
            parsed_source: models.ContainerImage | None
            try:
                parsed_source = models.ContainerImage.from_config(image)
            except ValueError:
                parsed_source = None
            # A bare digest-pinned OCI reference is still a source
            # registration request even when its digest already exists. Bare
            # release names and artifact IDs remain lookup-only selectors.
            if (record is not None and parsed_source is not None and
                    parsed_source.digest is not None):
                image_spec = parsed_source
            else:
                image_spec = (models.ContainerImage(
                    artifact_id=record.id) if record is not None else
                              models.ContainerImage.from_config(image))
    else:
        image_spec = models.ContainerImage.from_config(image)
        record = _record_for_selector(image_spec, workspace)
    if distribution is not None:
        if (image_spec.distribution is not None and
                image_spec.distribution != distribution):
            raise ValueError('Image preparation specifies conflicting '
                             'distribution overrides.')
        image_spec = dataclasses.replace(image_spec, distribution=distribution)
    profile, policy = _profile_for_spec(image_spec, workspace)
    if profile is None:
        raise ValueError('Direct unmanaged images cannot be prepared.')
    # Resolve every target before publishing a source or creating any
    # materialization intents.  A typo in a later target must not leave a
    # partially registered image or only the earlier targets behind.
    target_configs = [profile.target(target_id) for target_id in targets]
    publication = None
    expected_digest: str
    existing_image_id: str | None
    # A source-bearing selector is also a registration request. Bind its source
    # and optional release in the same transaction as every target intent,
    # even when its digest already selects an existing artifact.
    if image_spec.ref is not None:
        resolved_ref, expected_digest = _resolve_source(image_spec.ref)
        canonical = profile.canonical
        publication = state.ImagePublication(
            source_ref=image_spec.ref,
            resolved_source_ref=resolved_ref,
            source_digest=expected_digest,
            workspace=workspace,
            creator_user_hash=common_utils.get_current_user().id,
            release=image_spec.release,
            profile=profile.name,
            target_id=canonical.name,
            target_fingerprint=profile.physical_fingerprint(canonical),
            policy_fingerprint=profile.policy_fingerprint(canonical, True),
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
            max_artifacts=policy.max_artifacts,
            max_sources_per_artifact=policy.max_sources_per_artifact,
            max_releases_per_artifact=policy.max_releases_per_artifact,
        )
        existing_image_id = None
    else:
        if record is None:
            record = _ensure_record(image_spec, workspace, profile, policy)
        expected_digest = record.source_digest
        existing_image_id = record.id
    targets_by_name = {
        target.name: target for target in (profile.canonical, *target_configs)
    }
    intents = [
        state.ImageLocationIntent(
            target_id=target.name,
            target_fingerprint=profile.physical_fingerprint(target),
            policy_fingerprint=profile.policy_fingerprint(
                target, target is profile.canonical),
            canonical=target is profile.canonical,
            auto_evict=(target is not profile.canonical and
                        profile.ownership == models.RegistryOwnership.MANAGED),
        ) for target in targets_by_name.values()
    ]
    record = state.prepare_image_atomically(
        existing_image_id=existing_image_id,
        publication=publication,
        workspace=workspace,
        profile=profile.name,
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        expected_digest=expected_digest,
        intents=intents,
    )
    return _response(record)


def retry(
    image: str,
    target: str,
    workspace: str | None = None,
    distribution: str | None = None,
) -> responses.ContainerImageRecord:
    """Forces a current materialization through exact-digest revalidation."""
    target = models.validate_control_plane_identifier(target,
                                                      'Container image target')
    if distribution is not None:
        distribution = models.validate_control_plane_identifier(
            distribution, 'Container image distribution')
    workspace = _workspace(workspace)
    record = _find_image(image, workspace)
    if record is None:
        raise ValueError('Container image was not found in the requested '
                         'workspace.')
    profile, _ = image_config.resolve_profile(distribution, workspace)
    if profile is None:
        raise ValueError('No default registry distribution is configured.')
    target_config = profile.target(target)
    canonical_location = _ensure_target(record,
                                        profile,
                                        profile.canonical,
                                        canonical=True)
    canonical = target_config is profile.canonical
    location = _ensure_target(
        record,
        profile,
        target_config,
        canonical=canonical,
        canonical_location_id=(None if canonical else canonical_location.id))
    if location.state == models.ImageLocationState.PENDING:
        return _response(record)
    if not state.retry_location(location.id):
        raise ValueError(
            f'Image target {target!r} is {location.state.value}; only READY, '
            'FAILED, MISSING, or EVICTED routes can be revalidated.')
    refreshed = state.get_image(record.id, workspace)
    assert refreshed is not None
    return _response(refreshed)


def routes_for_image(record: state.ImageRecord, profile: models.RegistryProfile,
                     placement: models.Placement) -> list[models.ImageRoute]:
    """Builds a read-only snapshot for the current distribution revision."""
    local_targets = _runtime_local_targets(profile, placement)
    if not state.profile_revision_matches(record.workspace, profile.name,
                                          profile.revision,
                                          profile.revision_fingerprint):
        return []
    canonical_location = state.get_location(
        record.id, profile.name, profile.canonical.name,
        profile.physical_fingerprint(profile.canonical))
    if canonical_location is None or canonical_location.source_id is None:
        return []
    canonical_source = state.get_source_by_id(canonical_location.source_id)
    if (canonical_source is None or canonical_source.image_id != record.id or
            canonical_source.workspace != record.workspace):
        return []
    routes: list[models.ImageRoute] = []
    for target in (profile.canonical, *profile.targets):
        location = state.get_location(record.id, profile.name, target.name,
                                      profile.physical_fingerprint(target))
        canonical = target is profile.canonical
        if (location is None or location.target_ref is None or
                location.profile_revision != profile.revision or
                location.policy_fingerprint != profile.policy_fingerprint(
                    target, canonical)):
            continue
        expected_reference = references.managed_reference(
            profile, target, record.workspace,
            canonical_source.resolved_source_ref, record.source_digest)
        if location.target_ref != expected_reference:
            continue
        if (not canonical and
            (canonical_location is None or
             location.canonical_location_id != canonical_location.id or
             canonical_location.profile_revision != profile.revision or
             canonical_location.state != models.ImageLocationState.READY)):
            continue
        auth_strategy = providers.get_adapter(
            target.provider).resolve_runtime_pull_auth(target, placement)
        # ImageRoute is a placement-specific resolver snapshot. Keep registry
        # adapter identity on RegistryTarget, but project an explicitly
        # declared compute locality onto this route so generic/R2 targets can
        # satisfy locality for AWS, GCP, Nebius, or Kubernetes placements.
        route_provider = target.provider
        route_region = target.region
        if target in local_targets:
            route_provider = placement.locality_provider
            route_region = placement.locality_region
        routes.append(
            models.ImageRoute(
                image_id=record.id,
                location_id=location.id,
                target_id=target.name,
                distribution=profile.name,
                profile_revision=location.profile_revision,
                policy_fingerprint=location.policy_fingerprint,
                provider=route_provider,
                region=route_region,
                reference=location.target_ref,
                digest=location.expected_digest,
                auth_strategy=auth_strategy,
                state=location.state,
                platforms=record.platforms,
                canonical=location.canonical,
            ))
    return routes


def _source_fallback(
    record: state.ImageRecord, image_spec: models.ContainerImage, reason: str,
    placement: models.Placement,
    configured_login: docker_utils.DockerLoginConfig | None
) -> tuple[models.ResolvedContainerImage, docker_utils.DockerLoginConfig |
           None]:
    if image_spec.ref is None:
        raise resolver.ImageRouteUnavailableError(
            f'{reason} Release and artifact selectors do not authorize an '
            'external source fallback; supply a digest-pinned ref or wait for '
            'a managed route.')
    resolved_source_ref, digest = _resolve_source(image_spec.ref)
    if digest != record.source_digest:
        raise resolver.ImageRouteUnavailableError(
            f'{reason} The explicitly supplied source does not match the '
            'selected artifact digest.')
    auth_strategy, runtime_login = providers.resolve_source_runtime_pull_auth(
        resolved_source_ref, placement, configured_login)
    return (models.ResolvedContainerImage(
        image_id=record.id,
        location_id=None,
        reference=resolved_source_ref,
        target_id='source',
        digest=record.source_digest,
        auth_strategy=auth_strategy,
        status='WARMING',
        fallback_reason=(
            models.ImageFallbackReason.MANAGED_ROUTE_WARMING.value),
    ), runtime_login)


def _validate_catalog_authority() -> None:
    """Fails managed controller work closed against the exact catalog UUID."""
    expected_authority = os.environ.get(
        constants.CONTAINER_IMAGE_CATALOG_AUTHORITY_ENV_VAR)
    if (expected_authority is not None and
            not state.catalog_authority_matches(expected_authority)):
        raise ValueError(
            'This controller is not connected to the managed container image '
            'catalog authority selected by the API server. Configure the '
            'controller and API server to use the same PostgreSQL database, '
            'or use controller consolidation mode.')
    is_dedicated_controller = (os.environ.get(
        constants.IS_SKYPILOT_SERVE_CONTROLLER,
        '').lower() == 'true' or os.environ.get(
            constants.OVERRIDE_CONSOLIDATION_MODE, '').lower() == 'true')
    if expected_authority is None and is_dedicated_controller:
        raise ValueError(
            'A dedicated controller is missing its managed container image '
            'catalog authority. Recreate it from the current API server or '
            'enable controller consolidation mode.')


def resolve_for_placement(
    resources: 'resources_lib.Resources',
    placement: models.Placement,
    workspace: str | None = None,
    *,
    ensure: bool = True,
) -> 'resources_lib.Resources':
    """Ensures and pins the best route after placement has been selected."""
    image_spec = resources.container_image
    if image_spec is None:
        return resources
    workspace = _workspace(workspace)
    if resources.resolved_container_image is not None:
        pinned = resources.resolved_container_image
        unpinned = resources.copy(_resolved_container_image=None,
                                  _docker_login_config=None)
        if pinned.location_id is None:
            # A source fallback is a per-attempt escape hatch, not a durable
            # policy decision. Re-resolve it so a restart adopts a READY
            # managed route as soon as one exists.
            return resolve_for_placement(unpinned,
                                         placement,
                                         workspace,
                                         ensure=ensure)

        _validate_catalog_authority()
        profile, policy = _profile_for_spec(image_spec, workspace)
        policy_current = (profile is not None and
                          pinned.distribution == profile.name and
                          pinned.profile_revision == profile.revision and
                          state.profile_revision_matches(
                              workspace, profile.name, profile.revision,
                              profile.revision_fingerprint))
        target = None
        if policy_current:
            assert profile is not None
            try:
                target = profile.target(pinned.target_id)
            except ValueError:
                policy_current = False
            else:
                policy_current = (
                    pinned.policy_fingerprint == profile.policy_fingerprint(
                        target, target is profile.canonical))
        if policy_current:
            assert profile is not None
            assert target is not None
            expected_auth = providers.get_adapter(
                target.provider).resolve_runtime_pull_auth(target, placement)
            policy_current = (expected_auth == pinned.auth_strategy and
                              route_satisfies_locality(
                                  profile, target, placement, policy.locality))
        if not policy_current:
            # A policy-only revision can reuse the same READY bytes while
            # changing the target alias or runtime pull authority. Drop the
            # stale launch snapshot and resolve against the complete current
            # profile before a restart is committed.
            return resolve_for_placement(unpinned,
                                         placement,
                                         workspace,
                                         ensure=ensure)

        assert profile is not None
        assert target is not None
        record = state.get_image(pinned.image_id, workspace)
        if record is None:
            return resolve_for_placement(unpinned,
                                         placement,
                                         workspace,
                                         ensure=ensure)
        _validate_record_platform(record, placement)
        location = state.get_location_by_id(pinned.location_id)
        location_current = (
            location is not None and
            location.state == models.ImageLocationState.READY and
            location.target_ref == pinned.reference and
            location.image_id == pinned.image_id and
            location.profile == pinned.distribution and
            location.target_id == pinned.target_id and
            location.profile_revision == pinned.profile_revision and
            location.policy_fingerprint == pinned.policy_fingerprint and
            location.expected_digest == pinned.digest)
        if not location_current:
            # A restart may move to another verified route. The cluster-row
            # transaction replaces the durable location reference only after
            # that new route and policy snapshot have been locked together.
            return resolve_for_placement(unpinned,
                                         placement,
                                         workspace,
                                         ensure=ensure)
        runtime_login = providers.get_adapter(
            target.provider).runtime_login_config(target, pinned.auth_strategy,
                                                  placement)
        return resources.copy(_docker_login_config=runtime_login)
    if (resources.container_image_from_legacy_image_id and
            image_spec.distribution is None):
        legacy_policy = image_config.get_workspace_policy(workspace)
        if legacy_policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED:
            raise ValueError(
                'This workspace requires managed container images. Migrate '
                'the legacy image_id: docker value to container_image and '
                'select an allowed distribution explicitly.')
        # A server default must not silently reinterpret an existing mutable
        # docker image task as a managed immutable import.
        return resources
    profile, policy = _profile_for_spec(image_spec, workspace)
    if profile is None:
        return resources
    _validate_catalog_authority()

    record = _record_for_selector(image_spec, workspace)
    if ensure:
        record = _ensure_record(image_spec, workspace, profile, policy)
    elif record is None:
        # A dry run remains mutation-free. Only the policy that authorizes
        # source fallback may report the existing direct-image path.
        if (image_spec.ref is not None and
                policy.mode == models.WorkspaceImageMode.MANAGED_PREFERRED and
                policy.locality == models.Locality.PREFER):
            return resources
        raise resolver.ImageRouteUnavailableError(
            'Dry-run resolution cannot create the managed image artifact or '
            'route required by this workspace policy.')
    assert record is not None
    # Before the first verified materialization, platform metadata is
    # intentionally unknown.  A managed route can never become READY without
    # nonempty proof, but managed-preferred policy may still authorize pulling
    # the caller's exact digest-pinned source while that proof is warming.
    if record.platforms:
        _validate_record_platform(record, placement)
    if ensure:
        try:
            _ensure_for_placement(record, profile, placement)
        except state.ProfileRevisionBusyError as e:
            raise resolver.ImageRouteUnavailableError(str(e)) from e

    source_runtime_login: docker_utils.DockerLoginConfig | None = None
    try:
        resolved = resolver.resolve(
            placement=placement,
            routes=routes_for_image(record, profile, placement),
            locality=policy.locality,
        )
    except resolver.ImageRouteUnavailableError as route_error:
        if (policy.mode != models.WorkspaceImageMode.MANAGED_PREFERRED or
                policy.locality != models.Locality.PREFER):
            raise
        resolved, source_runtime_login = _source_fallback(
            record, image_spec, str(route_error), placement,
            resources.docker_login_config)

    if resolved.target_id == 'source':
        runtime_login = source_runtime_login
    else:
        target = profile.target(resolved.target_id)
        runtime_login = providers.get_adapter(
            target.provider).runtime_login_config(target,
                                                  resolved.auth_strategy,
                                                  placement)
    return resources.copy(_resolved_container_image=resolved,
                          _docker_login_config=runtime_login)


def eviction_candidates(
    workspace: str | None = None,
    *,
    now: int | None = None,
    limit: int = 100,
) -> list[state.LocationRecord]:
    workspace = _workspace(workspace)
    policy = image_config.get_workspace_policy(workspace)
    retention_weeks = policy.regional_cache_retention_weeks
    if retention_weeks is None:
        return []
    if now is None:
        now = int(time.time())
    unused_before = now - retention_weeks * 7 * 24 * 60 * 60
    return state.list_eviction_candidates(workspace, unused_before, limit)


# Copy workers remain separate from API request executors. They consume the
# materialization leases above, resolve short-lived provider credentials, copy
# complete OCI indexes, and publish READY only after exact-digest verification.
