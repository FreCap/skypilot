"""Provider-free workload resolution and durable warming demand creation."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import typing

from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import consumers
from sky.container_images import demand_state
from sky.container_images import errors
from sky.container_images import models
from sky.container_images import topology_state
from sky.container_images import transactions
from sky.provision import docker_utils

if typing.TYPE_CHECKING:
    from sky import resources as resources_lib


class ContainerImageWarmingError(errors.ContainerImageError):
    """The selected placement is fenced while its exact digest materializes."""

    def __init__(self, demand: demand_state.DemandRecord) -> None:
        self.demand_id = demand.id
        self.consumer_generation = demand.consumer_generation
        super().__init__('IMAGE_WARMING')


class ContainerImagePreparationFailedError(errors.ContainerImageError):
    """The selected target reached a closed terminal preparation failure."""

    def __init__(self, demand_id: str) -> None:
        self.demand_id = demand_id
        super().__init__('IMAGE_PREPARATION_FAILED')


@dataclasses.dataclass(frozen=True)
class _MetadataResolution:
    """Provider-free eligibility result cached during optimization."""
    resources: resources_lib.Resources
    direct: bool
    managed_resources: resources_lib.Resources | None = None
    direct_fallback_resources: resources_lib.Resources | None = None
    profile: models.ManagedRegistryProfile | None = None
    policy: models.WorkspaceImagePolicy | None = None
    active: topology_state.ProfileRevisionRecord | None = None
    artifact: catalog_state.ArtifactRecord | None = None
    publication: catalog_state.PublicationRecord | None = None
    location: topology_state.LocationRecord | None = None
    target: models.ManagedRegistryTarget | None = None
    binding: models.RegistryAccessBinding | None = None
    runtime_principal: str | None = None
    instance_profile: str | None = None
    kubernetes_cluster_arn: str | None = None
    kubernetes_node_role: str | None = None
    kubernetes_node_selector: tuple[tuple[str, str], ...] = ()
    locality_rank: int = 0
    current_demand: demand_state.DemandRecord | None = None


def _policy_fingerprint(active: topology_state.ProfileRevisionRecord,
                        target: models.ManagedRegistryTarget,
                        binding: models.RegistryAccessBinding,
                        backend: str) -> str:
    payload = {
        'profile_revision_id': active.id,
        'config_hash': active.config_hash,
        'target_fingerprint': target.target_fingerprint,
        'runtime_binding_fingerprint': binding.fingerprint,
        'backend': backend,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True,
                   separators=(',', ':')).encode()).hexdigest()


def _published_identity(
    image: models.ContainerImage, workspace: str, platform: str
) -> tuple[catalog_state.ArtifactRecord,
           catalog_state.PublicationRecord] | None:
    artifact: catalog_state.ArtifactRecord | None = None
    publication: catalog_state.PublicationRecord | None = None
    if image.artifact_id is not None:
        artifact = catalog_state.get_published_artifact(image.artifact_id,
                                                        workspace)
    if image.release is not None:
        release = catalog_state.get_ready_release(image.release, workspace)
        if release is None or release.image_id is None:
            return None
        release_artifact = catalog_state.get_published_artifact(
            release.image_id, workspace)
        if release_artifact is None:
            return None
        if artifact is not None and artifact.id != release_artifact.id:
            raise ValueError('Container image selectors identify different '
                             'published artifacts.')
        artifact = release_artifact
        publication = release
    if image.ref is not None:
        source_artifact = catalog_state.get_published_artifact_by_source(
            workspace, image.ref, platform)
        if source_artifact is None:
            return None
        if artifact is not None and artifact.id != source_artifact.id:
            raise ValueError('Container image selectors identify different '
                             'published artifacts.')
        artifact = source_artifact
    if artifact is None or artifact.platform != platform:
        return None
    if publication is None:
        publication = catalog_state.get_ready_publication_for_artifact(
            artifact.id, workspace)
    if publication is None:
        return None
    return artifact, publication


def _target_for_placement(
        profile: models.ManagedRegistryProfile,
        policy: models.WorkspaceImagePolicy,
        placement: models.Placement) -> models.ManagedRegistryTarget:
    if placement.provider.lower() != 'aws' or placement.backend not in (
            'aws_vm', 'aws_eks'):
        raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
    if placement.backend == 'aws_eks':
        matching_targets: list[models.ManagedRegistryTarget] = []
        for candidate in (profile.canonical,) + profile.targets:
            binding_id = candidate.runtime_binding('aws_eks')
            if binding_id is None:
                continue
            binding = profile.bindings[binding_id]
            try:
                models.qualified_eks_cluster_for_target(
                    candidate,
                    binding,
                    placement.region,
                    cluster_arn=placement.kubernetes_cluster_arn,
                    node_role=placement.kubernetes_node_role)
            except ValueError:
                continue
            matching_targets.append(candidate)
        if len(matching_targets) != 1:
            raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
        target = matching_targets[0]
        if (policy.locality == models.Locality.CANONICAL and
                target is not profile.canonical):
            raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
        return target
    if policy.locality == models.Locality.CANONICAL:
        return profile.canonical
    local = next((target for target in (profile.canonical,) + profile.targets
                  if target.region == placement.region), None)
    if local is not None:
        return local
    if policy.locality == models.Locality.REQUIRE:
        raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
    return profile.canonical


def _runtime_binding(
    profile: models.ManagedRegistryProfile,
    target: models.ManagedRegistryTarget, placement: models.Placement
) -> tuple[models.RegistryAccessBinding, str | None, str | None, str | None,
           str | None, str | None, tuple[tuple[str, str], ...]]:
    binding_id = target.runtime_binding(placement.backend)
    if binding_id is None:
        raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
    binding = profile.bindings[binding_id]
    expected_host_image: str | None = None
    runtime_principal: str | None = None
    instance_profile: str | None = None
    kubernetes_cluster_arn: str | None = None
    kubernetes_node_role: str | None = None
    kubernetes_node_selector: tuple[tuple[str, str], ...] = ()
    if placement.backend == 'aws_vm':
        if (binding.kind
                != models.RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY):
            raise ValueError('QUALIFICATION_FAILED')
        expected_host_image = dict(binding.qualified_node_images).get(
            placement.region)
        if expected_host_image is None:
            raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
        runtime_principal = binding.principals[0]
        instance_profile = binding.instance_profile
        if (placement.host_image_id is not None and
                placement.host_image_id != expected_host_image):
            raise ValueError('QUALIFIED_HOST_IMAGE_REQUIRED')
        if (placement.runtime_principal is not None and
                placement.runtime_principal not in binding.principals):
            raise ValueError('QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED')
    else:
        if (binding.kind
                != models.RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY):
            raise ValueError('QUALIFICATION_FAILED')
        try:
            qualified = models.qualified_eks_cluster_for_target(
                target,
                binding,
                placement.region,
                cluster_arn=placement.kubernetes_cluster_arn,
                node_role=placement.kubernetes_node_role)
        except ValueError as error:
            raise ValueError(
                'QUALIFIED_KUBERNETES_NODE_ROLE_REQUIRED') from error
        kubernetes_cluster_arn = qualified.cluster_arn
        kubernetes_node_role = qualified.node_role
        kubernetes_node_selector = qualified.node_selector
    return (binding, expected_host_image, runtime_principal, instance_profile,
            kubernetes_cluster_arn, kubernetes_node_role,
            kubernetes_node_selector)


def _pin_host_image(resources: resources_lib.Resources,
                    placement: models.Placement,
                    expected: str | None) -> resources_lib.Resources:
    if expected is None:
        return resources
    configured = dict(resources.image_id or {})
    existing = configured.get(placement.region, configured.get(None))
    if existing is not None and existing != expected:
        raise ValueError('QUALIFIED_HOST_IMAGE_REQUIRED')
    configured.pop(None, None)
    configured[placement.region] = expected
    return resources.copy(image_id=configured)


def _direct_fallback_allowed(policy: models.WorkspaceImagePolicy,
                             image: models.ContainerImage) -> bool:
    return (policy.mode == models.WorkspaceImageMode.MANAGED_PREFERRED and
            policy.locality == models.Locality.PREFER and image.ref is not None)


def _managed_runtime_platform(placement: models.Placement) -> str:
    """Returns the only v0 qualified platform or fails closed."""
    expected = models.V0_MANAGED_RUNTIME_PLATFORM
    if placement.backend == 'aws_vm':
        if placement.platform != expected:
            raise ValueError('IMAGE_RUNTIME_PLATFORM_UNSUPPORTED')
    elif placement.backend == 'aws_eks':
        if placement.platform not in (None, expected):
            raise ValueError('IMAGE_RUNTIME_PLATFORM_UNSUPPORTED')
    else:
        raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
    return expected


def _unsupported_direct_locality_rank(image: models.ContainerImage,
                                      workspace: str) -> int:
    """Keeps direct-mode multicloud candidates in the same locality class."""
    if image.distribution == config.DIRECT_PROFILE:
        return 0
    if image.distribution is not None:
        return 1
    policy = config.get_workspace_policy(workspace)
    return int(policy.mode != models.WorkspaceImageMode.DIRECT)


def _current_consumer_demand(
    workspace: str,
    placement: models.Placement,
    cache: dict[tuple[typing.Any, ...], typing.Any],
) -> demand_state.DemandRecord | None:
    consumer = consumers.current()
    if consumer is None:
        return None
    consumer = consumers.scope_for_placement(consumer, placement)
    key = ('consumer_demand', workspace, consumer.consumer_kind,
           consumer.consumer_owner, consumer.controller_epoch)
    if key not in cache:
        cache[key] = demand_state.get_current_demand_for_controller_epoch(
            workspace=workspace,
            consumer_kind=consumer.consumer_kind,
            consumer_owner=consumer.consumer_owner,
            controller_epoch=consumer.controller_epoch)
    return cache[key]


def _matches_current_demand(
        demand: demand_state.DemandRecord, *, placement: models.Placement,
        active: topology_state.ProfileRevisionRecord,
        artifact: catalog_state.ArtifactRecord,
        target: models.ManagedRegistryTarget,
        location: topology_state.LocationRecord | None) -> bool:
    if location is None:
        return False
    expected_placement = {
        'provider': placement.provider,
        'region': placement.region,
        'backend': placement.backend,
        'platform': placement.platform or 'linux/amd64',
    }
    return (demand.image_id == artifact.id and
            demand.runtime_digest == artifact.runtime_digest and
            demand.profile_revision_id == active.id and
            demand.target_fingerprint == target.target_fingerprint and
            demand.location_id == location.id and demand.target_key
            == f'{artifact.id}:{target.target_fingerprint}' and all(
                demand.placement.get(key) == value
                for key, value in expected_placement.items()))


def _runtime_binding_matches(active: topology_state.ProfileRevisionRecord,
                             profile: models.ManagedRegistryProfile,
                             target: models.ManagedRegistryTarget,
                             placement: models.Placement,
                             binding: models.RegistryAccessBinding, *,
                             as_of: int | None) -> bool:
    """Checks runtime identity and optional new-demand proof freshness.

    An existing demand is the durable admission record, so its replay passes
    ``as_of=None``. A new demand passes one request-cached PostgreSQL epoch;
    locked admission samples the database again and remains authoritative.
    """
    runtime_id = placement.region
    key = models.profile_attestation_key('runtime', target.name,
                                         placement.backend, binding.fingerprint,
                                         runtime_id)
    evidence = active.attestations.get(key)
    qualified_cluster: models.QualifiedKubernetesCluster | None = None
    if placement.backend == 'aws_eks':
        try:
            qualified_cluster = models.qualified_eks_cluster_for_target(
                target,
                binding,
                runtime_id,
                cluster_arn=placement.kubernetes_cluster_arn,
                node_role=placement.kubernetes_node_role)
        except ValueError:
            return False
    return models.runtime_attestation_matches(
        profile,
        target,
        binding,
        placement.backend,
        runtime_id,
        evidence,
        as_of=as_of,
        qualified_cluster=qualified_cluster)


def _metadata_database_epoch(
    cache: dict[tuple[typing.Any, ...], typing.Any],) -> int:
    key = ('database_epoch',)
    if key not in cache:
        cache[key] = catalog_state.read_database_epoch()
    return int(cache[key])


def _resolve_metadata(
    resources: resources_lib.Resources,
    placement: models.Placement,
    workspace: str,
    cache: dict[tuple[typing.Any, ...], typing.Any] | None = None
) -> _MetadataResolution:
    if cache is None:
        cache = {}
    image = resources.container_image
    if (image is None or resources.container_image_from_legacy_image_id or
            resources.resolved_container_image is not None):
        return _MetadataResolution(resources=resources, direct=True)
    if (placement.provider.lower() != 'aws' or
            placement.backend not in ('aws_vm', 'aws_eks')):
        if (image.ref is not None and image.release is None and
                image.artifact_id is None):
            return _MetadataResolution(
                resources=resources,
                direct=True,
                locality_rank=(_unsupported_direct_locality_rank(
                    image, workspace)))
        raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
    try:
        platform = _managed_runtime_platform(placement)
    except ValueError:
        if (image.ref is not None and image.release is None and
                image.artifact_id is None):
            return _MetadataResolution(
                resources=resources,
                direct=True,
                locality_rank=(_unsupported_direct_locality_rank(
                    image, workspace)))
        raise
    try:
        current_demand = _current_consumer_demand(workspace, placement, cache)
    except catalog_state.ManagedImageDatabaseRequiredError:
        # A persisted consumer demand takes precedence when PostgreSQL exists.
        # Without managed state, only a currently direct exact ref is runnable.
        profile_key = ('profile', workspace, image.distribution)
        if profile_key not in cache:
            cache[profile_key] = config.resolve_profile(image.distribution,
                                                        workspace)
        configured_profile, policy = cache[profile_key]
        if configured_profile is not None:
            raise
        if image.ref is None:
            raise ValueError('PROFILE_NOT_ACTIVE') from None
        return _MetadataResolution(resources=resources,
                                   direct=True,
                                   policy=policy)
    if current_demand is not None:
        policy_key = ('workspace_policy', workspace)
        if policy_key not in cache:
            cache[policy_key] = config.get_workspace_policy(workspace)
        policy = cache[policy_key]
        revision_key = ('revision', current_demand.profile_revision_id)
        if revision_key not in cache:
            cache[revision_key] = topology_state.get_profile_revision(
                current_demand.profile_revision_id)
        active = cache[revision_key]
        if (active is None or active.workspace != workspace or
                active.state not in (models.ImageProfileState.ACTIVE,
                                     models.ImageProfileState.RETIRED)):
            raise ValueError('PROFILE_NOT_ACTIVE')
        profile = models.ManagedRegistryProfile.from_snapshot(
            active.config_snapshot)
        if (profile.name != active.profile or
            (image.distribution is not None and
             image.distribution != profile.name)):
            raise ValueError('IMAGE_DEMAND_TARGET_MISMATCH')
    else:
        profile_key = ('profile', workspace, image.distribution)
        if profile_key not in cache:
            cache[profile_key] = config.resolve_profile(image.distribution,
                                                        workspace)
        configured_profile, policy = cache[profile_key]
        if configured_profile is None:
            if image.ref is None:
                raise ValueError('PROFILE_NOT_ACTIVE')
            return _MetadataResolution(resources=resources,
                                       direct=True,
                                       policy=policy)
        active_key = ('active', workspace, configured_profile.name)
        if active_key not in cache:
            cache[active_key] = topology_state.get_active_profile(
                workspace, configured_profile.name)
        active = cache[active_key]
        if active is None:
            raise ValueError('PROFILE_NOT_ACTIVE')
        profile = models.ManagedRegistryProfile.from_snapshot(
            active.config_snapshot)
        if profile.name != configured_profile.name:
            raise ValueError('PROFILE_NOT_ACTIVE')
    identity_key = ('identity', workspace, image.ref, image.release,
                    image.artifact_id, platform)
    if identity_key not in cache:
        cache[identity_key] = _published_identity(image, workspace, platform)
    identity = cache[identity_key]
    if identity is None:
        if (_direct_fallback_allowed(policy, image) and current_demand is None):
            return _MetadataResolution(resources=resources,
                                       direct=True,
                                       profile=profile,
                                       policy=policy,
                                       active=active,
                                       locality_rank=1)
        raise ValueError('IMAGE_NOT_PUBLISHED: run sky image publish first.')
    artifact, publication = identity
    publication_key = ('revision', publication.profile_revision_id)
    if publication_key not in cache:
        cache[publication_key] = topology_state.get_profile_revision(
            publication.profile_revision_id)
    publication_revision = cache[publication_key]
    if (publication_revision is None or
            publication_revision.profile != profile.name):
        raise ValueError('ARTIFACT_NOT_READY')
    try:
        if current_demand is None:
            target = _target_for_placement(profile, policy, placement)
        else:
            matching_targets = [
                item for item in (profile.canonical,) + profile.targets
                if item.target_fingerprint == current_demand.target_fingerprint
            ]
            if len(matching_targets) != 1:
                raise ValueError('IMAGE_DEMAND_TARGET_MISMATCH')
            target = matching_targets[0]
        (binding, expected_host_image, runtime_principal, instance_profile,
         kubernetes_cluster_arn, kubernetes_node_role,
         kubernetes_node_selector) = _runtime_binding(profile, target,
                                                      placement)
        prepared = _pin_host_image(resources, placement, expected_host_image)
        qualification_epoch = (None if current_demand is not None else
                               _metadata_database_epoch(cache))
        if not _runtime_binding_matches(active,
                                        profile,
                                        target,
                                        placement,
                                        binding,
                                        as_of=qualification_epoch):
            raise ValueError('QUALIFICATION_STALE')
    except ValueError:
        if (_direct_fallback_allowed(policy, image) and current_demand is None):
            return _MetadataResolution(resources=resources,
                                       direct=True,
                                       profile=profile,
                                       policy=policy,
                                       active=active,
                                       artifact=artifact,
                                       publication=publication,
                                       locality_rank=1)
        raise
    location_key = (('location_id', current_demand.location_id)
                    if current_demand is not None else
                    ('location', workspace, artifact.id,
                     target.target_fingerprint, artifact.runtime_digest))
    if location_key not in cache:
        if current_demand is not None:
            cache[location_key] = topology_state.get_location(
                current_demand.location_id)
        else:
            cache[location_key] = topology_state.get_location_for_target(
                image_id=artifact.id,
                workspace=workspace,
                target_fingerprint=target.target_fingerprint,
                runtime_digest=artifact.runtime_digest)
    location = cache[location_key]
    if (current_demand is not None and
            not _matches_current_demand(current_demand,
                                        placement=placement,
                                        active=active,
                                        artifact=artifact,
                                        target=target,
                                        location=location)):
        raise ValueError('IMAGE_DEMAND_TARGET_MISMATCH')
    managed_ready = (location is not None and
                     location.state == models.ImageLocationState.READY)
    direct_fallback = (_direct_fallback_allowed(policy, image) and
                       current_demand is None)
    eligible_resources = prepared
    locality_rank = 0
    if direct_fallback and not managed_ready:
        # Keep the executable direct candidate byte-for-byte identical. The
        # qualified AMI belongs only to a READY managed route.
        eligible_resources = resources
        locality_rank = 1
    elif (policy.locality == models.Locality.PREFER and
          current_demand is None and not managed_ready):
        locality_rank = 2
    return _MetadataResolution(
        resources=eligible_resources,
        direct=False,
        managed_resources=prepared,
        direct_fallback_resources=(resources if direct_fallback else None),
        profile=profile,
        policy=policy,
        active=active,
        artifact=artifact,
        publication=publication,
        location=location,
        target=target,
        binding=binding,
        runtime_principal=runtime_principal,
        instance_profile=instance_profile,
        kubernetes_cluster_arn=kubernetes_cluster_arn,
        kubernetes_node_role=kubernetes_node_role,
        kubernetes_node_selector=(kubernetes_node_selector),
        locality_rank=locality_rank,
        current_demand=current_demand)


def prepare_metadata_only(
    resources: resources_lib.Resources,
    placement: models.Placement,
    workspace: str,
    cache: dict[tuple[typing.Any, ...], typing.Any] | None = None
) -> resources_lib.Resources | None:
    """Filters and pins one candidate without provider calls or mutations."""
    try:
        return _resolve_metadata(resources, placement, workspace,
                                 cache).resources
    except ValueError:
        return None


def prepare_metadata_only_with_rank(
    resources: resources_lib.Resources,
    placement: models.Placement,
    workspace: str,
    cache: dict[tuple[typing.Any, ...], typing.Any] | None = None,
) -> tuple[resources_lib.Resources, int] | None:
    """Returns one eligible candidate and its locality preference class."""
    try:
        resolution = _resolve_metadata(resources, placement, workspace, cache)
        return resolution.resources, resolution.locality_rank
    except ValueError:
        return None


def _managed_login(
        target: models.ManagedRegistryTarget,
        placement: models.Placement) -> docker_utils.DockerLoginConfig | None:
    if placement.backend != 'aws_vm':
        return None
    return docker_utils.DockerLoginConfig(username='',
                                          password='',
                                          server=target.registry,
                                          credential_helper='ecr-login')


def _readmit_location_for_demand(
    location: topology_state.LocationRecord,
    workspace: str,
    *,
    retry_failed: bool,
) -> topology_state.LocationRecord:
    states = {
        models.ImageLocationState.MISSING,
        models.ImageLocationState.EVICTED,
    }
    if retry_failed:
        states.add(models.ImageLocationState.FAILED)
    if location.state not in states:
        return location
    readmitted = topology_state.retry_location(location.id, workspace)
    if readmitted is not None:
        return readmitted
    # A concurrent retry may have moved the row before our locked mutation.
    # Reload only on that uncommon path so READY uses the current pull plan.
    refreshed = topology_state.get_location(location.id)
    if refreshed is None or refreshed.workspace != workspace:
        raise ValueError('ARTIFACT_NOT_READY')
    return refreshed


def resolve_for_placement(resources: resources_lib.Resources,
                          placement: models.Placement,
                          *,
                          workspace: str,
                          consumer_kind: str,
                          consumer_owner: str,
                          controller_epoch: str,
                          controller_sequence: int | None,
                          allow_epoch_advance: bool,
                          consumer_metadata: dict[str, typing.Any] |
                          None = None,
                          ensure: bool = True) -> resources_lib.Resources:
    """Pins one qualified AWS target or preserves the exact direct path."""
    image = resources.container_image
    consumer = consumers.ImageConsumerContext(
        consumer_kind=consumer_kind,
        consumer_owner=consumer_owner,
        controller_epoch=controller_epoch,
        controller_sequence=controller_sequence,
        allow_epoch_advance=allow_epoch_advance,
        metadata=consumer_metadata or {})
    consumer = consumers.scope_for_placement(consumer, placement)
    with consumers.use(consumer):
        metadata = _resolve_metadata(resources, placement, workspace)
    resources = metadata.resources
    managed_resources = metadata.managed_resources or resources
    direct_fallback_resources = metadata.direct_fallback_resources
    if metadata.direct:
        return resources
    assert image is not None
    assert metadata.profile is not None
    assert metadata.policy is not None
    assert metadata.active is not None
    assert metadata.artifact is not None
    assert metadata.publication is not None
    assert metadata.target is not None
    assert metadata.binding is not None
    profile = metadata.profile
    active = metadata.active
    artifact = metadata.artifact
    publication = metadata.publication
    target = metadata.target
    binding = metadata.binding
    runtime_principal = metadata.runtime_principal
    instance_profile = metadata.instance_profile
    kubernetes_cluster_arn = metadata.kubernetes_cluster_arn
    kubernetes_node_role = metadata.kubernetes_node_role
    kubernetes_node_selector = metadata.kubernetes_node_selector
    platform = placement.platform or 'linux/amd64'
    location = metadata.location
    if location is None:
        if not ensure:
            return resources
        if target is profile.canonical:
            if direct_fallback_resources is not None:
                return direct_fallback_resources
            raise ValueError('ARTIFACT_NOT_READY')
        if publication.canonical_location_id is None:
            if direct_fallback_resources is not None:
                return direct_fallback_resources
            raise ValueError('ARTIFACT_NOT_READY')
        location = transactions.reserve_regional_location(
            image_id=artifact.id,
            workspace=workspace,
            profile_revision_id=active.id,
            target_id=target.name,
            canonical_location_id=publication.canonical_location_id,
            max_regional_locations=(
                profile.limits.max_regional_locations_per_artifact))
    if not ensure:
        return resources
    is_new_demand = metadata.current_demand is None
    location = _readmit_location_for_demand(location,
                                            workspace,
                                            retry_failed=is_new_demand)
    if (direct_fallback_resources is not None and
            location.state != models.ImageLocationState.READY):
        return direct_fallback_resources
    authority_id = catalog_state.get_catalog_authority_id()
    placement_payload: dict[str, typing.Any] = {
        'provider': placement.provider,
        'region': placement.region,
        'backend': placement.backend,
        'platform': platform,
        'runtime_binding_fingerprint': binding.fingerprint,
    }
    if expected_host_image := dict(binding.qualified_node_images).get(
            placement.region):
        placement_payload['host_image_id'] = expected_host_image
    if runtime_principal is not None:
        placement_payload['runtime_principal'] = runtime_principal
    if instance_profile is not None:
        placement_payload['instance_profile'] = instance_profile
    if kubernetes_cluster_arn is not None:
        placement_payload['kubernetes_cluster_arn'] = kubernetes_cluster_arn
    if kubernetes_node_role is not None:
        placement_payload['kubernetes_node_role'] = kubernetes_node_role
    if kubernetes_node_selector:
        placement_payload['kubernetes_node_selector'] = list(
            kubernetes_node_selector)
    if consumer.metadata:
        placement_payload['consumer'] = consumer.metadata
    try:
        demand = transactions.create_warming_demand_for_controller_epoch(
            authority_id=authority_id,
            workspace=workspace,
            consumer_kind=consumer.consumer_kind,
            consumer_owner=consumer.consumer_owner,
            controller_epoch=consumer.controller_epoch,
            controller_sequence=consumer.controller_sequence,
            allow_epoch_advance=consumer.allow_epoch_advance,
            target_key=f'{artifact.id}:{target.target_fingerprint}',
            image_id=artifact.id,
            runtime_digest=artifact.runtime_digest,
            profile_revision_id=active.id,
            target_fingerprint=target.target_fingerprint,
            location_id=location.id,
            placement=placement_payload)
    except transactions.DemandQualificationStaleError:
        if direct_fallback_resources is not None:
            return direct_fallback_resources
        raise
    if location.state in (models.ImageLocationState.FAILED,
                          models.ImageLocationState.QUARANTINED):
        demand_state.fail_and_supersede_demand(
            demand.id, location.error_code or 'IMAGE_PREPARATION_FAILED')
        raise ContainerImagePreparationFailedError(demand.id)
    if location.state != models.ImageLocationState.READY:
        raise ContainerImageWarmingError(demand)
    policy_fingerprint = _policy_fingerprint(active, target, binding,
                                             placement.backend)
    pull_plan = {
        'version': 1,
        'reference': location.target_ref,
        'runtime_digest': artifact.runtime_digest,
        'platform': artifact.platform,
        'distribution': profile.name,
        'profile_revision_id': active.id,
        'target_id': target.name,
        'target_fingerprint': target.target_fingerprint,
        'auth_strategy': 'ecr_runtime_identity',
        'credential_helper':
            ('ecr-login' if placement.backend == 'aws_vm' else None),
        'runtime_principal': runtime_principal,
        'instance_profile': instance_profile,
        'kubernetes_node_selector': list(kubernetes_node_selector),
    }
    try:
        demand = transactions.commit_ready_demand(
            demand_id=demand.id,
            consumer_generation=demand.consumer_generation,
            pull_plan=pull_plan)
    except transactions.DemandLocationNotReadyError as e:
        # The metadata snapshot predates demand creation. An eviction can win
        # the location lock in between. Managed-preferred terminalizes the new
        # demand before using its original direct path; strict managed demand
        # remains warming so the durable worker can requeue the location.
        if direct_fallback_resources is not None:
            if e.state in (models.ImageLocationState.FAILED,
                           models.ImageLocationState.QUARANTINED):
                demand_state.fail_and_supersede_demand(
                    demand.id, e.error_code or 'IMAGE_PREPARATION_FAILED')
            else:
                demand_state.supersede_demand(demand.id, workspace)
            return direct_fallback_resources
        if e.state in (models.ImageLocationState.FAILED,
                       models.ImageLocationState.QUARANTINED):
            demand_state.fail_and_supersede_demand(
                demand.id, e.error_code or 'IMAGE_PREPARATION_FAILED')
            raise ContainerImagePreparationFailedError(demand.id) from e
        raise ContainerImageWarmingError(demand) from e
    resolved = models.ResolvedContainerImage(
        image_id=artifact.id,
        reference=location.target_ref,
        target_id=target.name,
        digest=artifact.runtime_digest,
        auth_strategy='ecr_runtime_identity',
        location_id=location.id,
        distribution=profile.name,
        profile_revision=active.revision,
        policy_fingerprint=policy_fingerprint,
        profile_revision_id=active.id,
        target_fingerprint=target.target_fingerprint,
        demand_id=demand.id,
        demand_generation=demand.consumer_generation,
        controller_epoch=consumer.controller_epoch,
        owner_epoch=demand.owner_epoch,
        credential_helper=('ecr-login'
                           if placement.backend == 'aws_vm' else None),
        runtime_principal=runtime_principal,
        instance_profile=instance_profile,
        kubernetes_node_selector=kubernetes_node_selector)
    return managed_resources.copy(_resolved_container_image=resolved,
                                  _docker_login_config=_managed_login(
                                      target, placement))
