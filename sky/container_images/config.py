"""Strict v0 registry-profile and workspace-policy resolution."""

from typing import Any

from sky import skypilot_config
from sky.container_images import models
from sky.skylet import constants

DIRECT_PROFILE = 'direct'
_MAX_WORKSPACE_PROFILES = 128
_MAX_WORKSPACE_PUBLISHERS = 256
_MISSING_WORKSPACE_POLICY = object()


def _workspace_policy_config(workspace: str) -> Any:
    workspaces = skypilot_config.get_nested(
        ('workspaces',), default_value=_MISSING_WORKSPACE_POLICY)
    if workspaces is _MISSING_WORKSPACE_POLICY:
        return {}
    if not isinstance(workspaces, dict):
        raise ValueError('SkyPilot workspaces configuration must be an object.')
    workspace_config = workspaces.get(workspace, _MISSING_WORKSPACE_POLICY)
    if workspace_config is _MISSING_WORKSPACE_POLICY:
        return {}
    if not isinstance(workspace_config, dict):
        raise ValueError('SkyPilot workspace configuration is invalid.')
    raw_policy = workspace_config.get('container_images',
                                      _MISSING_WORKSPACE_POLICY)
    if raw_policy is _MISSING_WORKSPACE_POLICY:
        return {}
    return raw_policy


def _parse_string_collection(value: Any, subject: str,
                             max_items: int) -> tuple[str, ...]:
    """Normalizes one bounded config list without leaking shape errors."""
    if not isinstance(value, (list, tuple)) or len(value) > max_items:
        raise ValueError(
            f'{subject} must be a list of at most {max_items} strings.')
    if not all(isinstance(item, str) for item in value):
        raise ValueError(
            f'{subject} must be a list of at most {max_items} strings.')
    normalized = tuple(value)
    if len(set(normalized)) != len(normalized):
        raise ValueError(f'{subject} must not contain duplicate values.')
    return normalized


def parse_workspace_policy(value: Any) -> models.WorkspaceImagePolicy:
    """Parses one strict workspace image policy from a config snapshot."""
    if not isinstance(value, dict):
        raise ValueError('Workspace container_images must be an object.')
    unknown = set(value) - {
        'mode', 'default_profile', 'allowed_profiles', 'publishers', 'locality',
        'regional_cache_retention_weeks'
    }
    if unknown:
        raise ValueError(
            'Workspace container_images contains unsupported keys.')
    retention_weeks = value.get('regional_cache_retention_weeks', 8)
    if (retention_weeks is not None and
        (not isinstance(retention_weeks, int) or
         isinstance(retention_weeks, bool) or retention_weeks <= 0)):
        raise ValueError('regional_cache_retention_weeks must be a positive '
                         'integer or null to disable automatic eviction.')
    allowed_profiles = _parse_string_collection(
        value.get('allowed_profiles', ()), 'Workspace allowed_profiles',
        _MAX_WORKSPACE_PROFILES)
    publishers = _parse_string_collection(value.get('publishers',
                                                    ()), 'Workspace publishers',
                                          _MAX_WORKSPACE_PUBLISHERS)
    return models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode(
            value.get('mode', models.WorkspaceImageMode.DIRECT.value)),
        default_profile=value.get('default_profile'),
        allowed_profiles=allowed_profiles,
        publishers=publishers,
        locality=models.Locality(
            value.get('locality', models.Locality.PREFER.value)),
        regional_cache_retention_weeks=retention_weeks,
    )


def get_workspace_policy(
        workspace: str | None = None) -> models.WorkspaceImagePolicy:
    """Returns explicit workspace opt-in, defaulting to unchanged direct pulls."""
    if workspace is None:
        workspace = (skypilot_config.get_active_workspace() or
                     constants.SKYPILOT_DEFAULT_WORKSPACE)
    return parse_workspace_policy(_workspace_policy_config(workspace))


def list_workspace_policies() -> dict[str, models.WorkspaceImagePolicy]:
    """Returns every explicitly configured workspace image policy."""
    workspaces = skypilot_config.get_nested(
        ('workspaces',), default_value=_MISSING_WORKSPACE_POLICY)
    if workspaces is _MISSING_WORKSPACE_POLICY:
        workspaces = {}
    if not isinstance(workspaces, dict):
        raise ValueError('SkyPilot workspaces configuration must be an object.')
    policies: dict[str, models.WorkspaceImagePolicy] = {}
    for workspace, workspace_config in workspaces.items():
        if not isinstance(workspace, str) or not isinstance(
                workspace_config, dict):
            raise ValueError('SkyPilot workspace configuration is invalid.')
        raw_policy = workspace_config.get('container_images',
                                          _MISSING_WORKSPACE_POLICY)
        if raw_policy is _MISSING_WORKSPACE_POLICY:
            raw_policy = {}
        policies[workspace] = parse_workspace_policy(raw_policy)
    return policies


def _binding_from_config(name: str,
                         value: dict[str, Any]) -> models.RegistryAccessBinding:
    if not isinstance(value, dict):
        raise ValueError(f'Registry access binding {name!r} must be an object.')
    try:
        kind = models.RegistryAccessBindingKind(value['kind'])
    except (KeyError, ValueError):
        raise ValueError(
            f'Registry access binding {name!r} has an unsupported kind.'
        ) from None
    purposes = tuple(value.get('purposes', ()))
    common = {'kind', 'purposes'}
    kwargs: dict[str, Any] = {}
    if kind == models.RegistryAccessBindingKind.AWS_ASSUME_ROLE:
        allowed = common | {'authority', 'external_id'}
        kwargs['authority'] = value.get('authority')
        kwargs['external_id'] = value.get('external_id')
    elif kind == models.RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY:
        allowed = common | {
            'principals', 'instance_profile', 'credential_helper',
            'qualified_node_images', 'canary_authority', 'canary_instance_type',
            'canary_use_spot', 'canary_subnets', 'canary_security_groups'
        }
        node_images = value.get('qualified_node_images', {})
        if not isinstance(node_images, dict):
            raise ValueError('qualified_node_images must map regions to AMIs.')
        canary_subnets = value.get('canary_subnets', {})
        canary_security_groups = value.get('canary_security_groups', {})
        if (not isinstance(canary_subnets, dict) or
                not isinstance(canary_security_groups, dict)):
            raise ValueError('EC2 canary networks must map regions to lists.')
        if (any(not isinstance(items, list)
                for items in canary_subnets.values()) or
                any(not isinstance(items, list)
                    for items in canary_security_groups.values())):
            raise ValueError('EC2 canary network values must be lists.')
        canary_use_spot = value.get('canary_use_spot', True)
        if not isinstance(canary_use_spot, bool):
            raise ValueError('EC2 canary Spot preference must be a boolean.')
        kwargs.update(
            principals=tuple(value.get('principals', ())),
            instance_profile=value.get('instance_profile'),
            credential_helper=value.get('credential_helper'),
            qualified_node_images=tuple(
                sorted((str(region), str(ami))
                       for region, ami in node_images.items())),
            canary_authority=value.get('canary_authority'),
            canary_instance_type=value.get('canary_instance_type'),
            canary_use_spot=canary_use_spot,
            canary_subnets=tuple(
                sorted((str(region), tuple(str(item)
                                           for item in items))
                       for region, items in canary_subnets.items())),
            canary_security_groups=tuple(
                sorted((str(region), tuple(str(item)
                                           for item in items))
                       for region, items in canary_security_groups.items())),
        )
    elif kind == models.RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY:
        allowed = common | {'qualified_clusters', 'canary_authority'}
        clusters = value.get('qualified_clusters', ())
        if not isinstance(clusters, list):
            raise ValueError('qualified_clusters must be a list.')
        qualified_clusters: list[models.QualifiedKubernetesCluster] = []
        for cluster in clusters:
            if not isinstance(cluster, dict) or set(cluster) != {
                    'context', 'cluster_arn', 'node_role', 'namespace',
                    'node_selector'
            }:
                raise ValueError(
                    'Each qualified EKS cluster requires only '
                    'context, cluster_arn, node_role, namespace, and '
                    'node_selector.')
            selector = cluster['node_selector']
            if not isinstance(selector, dict):
                raise ValueError('Qualified EKS node_selector must be a map.')
            qualified_clusters.append(
                models.QualifiedKubernetesCluster(
                    context=str(cluster['context']),
                    cluster_arn=str(cluster['cluster_arn']),
                    node_role=str(cluster['node_role']),
                    namespace=str(cluster['namespace']),
                    node_selector=tuple((str(key), str(item))
                                        for key, item in selector.items())))
        kwargs['qualified_clusters'] = tuple(qualified_clusters)
        kwargs['canary_authority'] = value.get('canary_authority')
    else:
        allowed = common | {'reference'}
        reference = value.get('reference')
        if not isinstance(reference, dict):
            raise ValueError('Docker config binding requires a reference.')
        kwargs['reference'] = {
            str(key): str(item) for key, item in reference.items()
        }
    if set(value) - allowed:
        raise ValueError(
            f'Registry access binding {name!r} contains unsupported keys.')
    return models.RegistryAccessBinding(id=name,
                                        kind=kind,
                                        purposes=purposes,
                                        **kwargs)


def parse_access_bindings(
        values: Any) -> dict[str, models.RegistryAccessBinding]:
    """Parses bounded access bindings without consulting global config."""
    if values is None:
        values = {}
    if not isinstance(values, dict) or len(values) > 256:
        raise ValueError('container_registries.access_bindings must contain at '
                         'most 256 named bindings.')
    return {
        str(name): _binding_from_config(str(name), value)
        for name, value in values.items()
    }


def access_bindings() -> dict[str, models.RegistryAccessBinding]:
    values = skypilot_config.get_nested(
        ('container_registries', 'access_bindings'), default_value={})
    return parse_access_bindings(values)


def get_source_binding(name: str | None) -> models.RegistryAccessBinding | None:
    if name is None:
        return None
    binding = access_bindings().get(name)
    if binding is None or 'source_read' not in binding.purposes:
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    return binding


def _target_from_config(name: str, value: dict[str, Any], *,
                        canonical: bool) -> models.ManagedRegistryTarget:
    if not isinstance(value, dict):
        raise ValueError(f'Registry target {name!r} must be an object.')
    required = {
        'region', 'registry', 'repository_prefix', 'shard_count',
        'max_manifests_per_shard', 'max_declared_bytes_per_shard',
        'max_in_flight', 'write_authority', 'delete_authority',
        'qualification_delete_authority', 'runtime_pull'
    }
    allowed = required | {'qualification_repository_generation'}
    if not canonical:
        required.add('name')
        allowed.add('name')
    if not required <= set(value) or not set(value) <= allowed:
        raise ValueError(
            f'Registry target {name!r} must define the complete v0 contract.')
    delete_authority = value['delete_authority']
    if delete_authority == 'disabled':
        delete_authority = None
    if canonical and delete_authority is not None:
        raise ValueError('Canonical registry target deletion must be disabled.')
    runtime_pull = value['runtime_pull']
    if not isinstance(runtime_pull, dict):
        raise ValueError('Registry target runtime_pull must be an object.')
    return models.ManagedRegistryTarget(
        name=name,
        region=value['region'],
        registry=value['registry'],
        repository_prefix=value['repository_prefix'],
        shard_count=value['shard_count'],
        max_manifests_per_shard=value['max_manifests_per_shard'],
        max_declared_bytes_per_shard=value['max_declared_bytes_per_shard'],
        max_in_flight=value['max_in_flight'],
        write_authority=value['write_authority'],
        delete_authority=delete_authority,
        qualification_delete_authority=value['qualification_delete_authority'],
        runtime_pull=tuple(
            sorted((str(backend), str(binding))
                   for backend, binding in runtime_pull.items())),
        qualification_repository_generation=value.get(
            'qualification_repository_generation', 0),
    )


def _profile_from_config(
    name: str,
    value: dict[str, Any],
    all_bindings: dict[str, models.RegistryAccessBinding],
) -> models.ManagedRegistryProfile:
    if not isinstance(value, dict):
        raise ValueError(f'Registry profile {name!r} must be an object.')
    expected = {
        'revision', 'ownership', 'provider', 'partition', 'registry_account',
        'realm', 'limits', 'qualification', 'canonical', 'targets'
    }
    if set(value) != expected:
        raise ValueError(
            f'Registry profile {name!r} must define the complete v0 contract.')
    if value['ownership'] != 'managed' or value['provider'] != 'aws':
        raise ValueError(
            'Managed distribution v0 supports only managed AWS ECR '
            'profiles. Other clouds retain direct OCI pulls.')
    limits = value['limits']
    if not isinstance(limits, dict) or set(limits) != {
            'max_artifact_bytes', 'max_releases_per_artifact',
            'max_regional_locations_per_artifact'
    }:
        raise ValueError('Registry profile limits are incomplete.')
    qualification = value['qualification']
    if not isinstance(qualification, dict) or set(qualification) != {
            'runtime_attestation_max_age_seconds', 'automatic_canaries',
            'max_daily_canary_cost_usd', 'canary_worst_case_cost_usd',
            'canary_timeout_seconds', 'canary_ref', 'canary_platform'
    }:
        raise ValueError('Registry profile qualification policy is incomplete.')
    canonical = _target_from_config('canonical',
                                    value['canonical'],
                                    canonical=True)
    target_values = value['targets']
    if not isinstance(target_values, list) or len(target_values) > 255:
        raise ValueError('Registry profile targets must be a bounded list.')
    targets_list: list[models.ManagedRegistryTarget] = []
    for target in target_values:
        if not isinstance(target, dict) or not isinstance(
                target.get('name'), str):
            raise ValueError('Each registry target requires a string name.')
        targets_list.append(
            _target_from_config(target['name'], target, canonical=False))
    targets = tuple(targets_list)
    binding_ids = {
        canonical.write_authority,
        canonical.qualification_delete_authority,
        *(binding for _, binding in canonical.runtime_pull),
    }
    for target in targets:
        binding_ids.add(target.write_authority)
        if target.delete_authority is not None:
            binding_ids.add(target.delete_authority)
        binding_ids.add(target.qualification_delete_authority)
        binding_ids.update(binding for _, binding in target.runtime_pull)
    runtime_binding_ids = {
        binding for target in (canonical,) + targets
        for _, binding in target.runtime_pull
    }
    for binding_id in runtime_binding_ids:
        runtime_binding = all_bindings.get(binding_id)
        if (runtime_binding is not None and
                runtime_binding.canary_authority is not None):
            binding_ids.add(runtime_binding.canary_authority)
    try:
        referenced_bindings = tuple(
            all_bindings[binding] for binding in sorted(binding_ids))
    except KeyError:
        raise ValueError('Registry profile references an unknown access binding.') \
            from None
    return models.ManagedRegistryProfile(
        name=name,
        revision=value['revision'],
        partition=value['partition'],
        registry_account=value['registry_account'],
        realm=value['realm'],
        limits=models.ManagedRegistryLimits(**limits),
        qualification=models.RegistryQualificationPolicy(**qualification),
        canonical=canonical,
        targets=targets,
        access_bindings=referenced_bindings,
    )


def parse_profiles(
    values: Any,
    bindings: dict[str, models.RegistryAccessBinding],
) -> dict[str, models.ManagedRegistryProfile]:
    """Parses complete managed profiles from one immutable config value."""
    if values is None:
        values = {}
    if not isinstance(values, dict) or len(values) > 128:
        raise ValueError('container_registries.profiles must contain at most '
                         '128 profiles.')
    return {
        str(name): _profile_from_config(str(name), value, bindings)
        for name, value in values.items()
    }


def resolve_profile_name(
    task_profile: str | None,
    workspace: str | None = None,
) -> tuple[str | None, models.WorkspaceImagePolicy]:
    """Resolves explicit selection before opted-in workspace defaults."""
    if workspace is None:
        workspace = (skypilot_config.get_active_workspace() or
                     constants.SKYPILOT_DEFAULT_WORKSPACE)
    policy = get_workspace_policy(workspace)
    if task_profile is not None:
        task_profile = models.validate_control_plane_identifier(
            task_profile, 'Container image distribution')
    if task_profile == DIRECT_PROFILE:
        if policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED:
            raise ValueError(
                'This workspace requires managed container images.')
        return None, policy
    if task_profile is not None:
        selected = task_profile
    elif policy.mode in (models.WorkspaceImageMode.MANAGED_PREFERRED,
                         models.WorkspaceImageMode.MANAGED_REQUIRED):
        selected = policy.default_profile or skypilot_config.get_nested(
            ('container_registries', 'default_profile'), default_value=None)
    else:
        selected = None
    if selected == DIRECT_PROFILE:
        raise ValueError('direct cannot be configured as a default profile.')
    if selected is not None:
        selected = models.validate_control_plane_identifier(
            selected, 'Container image distribution')
    if (selected is not None and policy.allowed_profiles and
            selected not in policy.allowed_profiles):
        raise ValueError(
            f'Registry profile {selected!r} is not allowed in this workspace.')
    if selected is None and policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED:
        raise ValueError('This workspace requires a managed registry profile.')
    return selected, policy


def resolve_profile(
    task_profile: str | None,
    workspace: str | None = None,
) -> tuple[models.ManagedRegistryProfile | None, models.WorkspaceImagePolicy]:
    name, policy = resolve_profile_name(task_profile, workspace)
    if name is None:
        return None, policy
    value = skypilot_config.get_nested(
        ('container_registries', 'profiles', name), default_value=None)
    if value is None:
        raise ValueError(f'Registry profile {name!r} is not configured.')
    return _profile_from_config(name, value, access_bindings()), policy


def is_declared_managed_eks_context(image: models.ContainerImage, context: str,
                                    workspace: str) -> bool:
    """Returns whether the selected profile explicitly binds this EKS context.

    This is configuration-only candidate classification. Runtime resolution
    still requires the exact active revision and fresh qualification evidence.
    """
    policy = get_workspace_policy(workspace)
    selected = image.distribution
    if selected == DIRECT_PROFILE:
        return False
    if selected is None and policy.mode in (
            models.WorkspaceImageMode.MANAGED_PREFERRED,
            models.WorkspaceImageMode.MANAGED_REQUIRED):
        selected = policy.default_profile or skypilot_config.get_nested(
            ('container_registries', 'default_profile'), default_value=None)
    if selected is None:
        return False
    selected = models.validate_control_plane_identifier(
        selected, 'Container image distribution')
    if policy.allowed_profiles and selected not in policy.allowed_profiles:
        raise ValueError(
            f'Registry profile {selected!r} is not allowed in this workspace.')
    value = skypilot_config.get_nested(
        ('container_registries', 'profiles', selected), default_value=None)
    if value is None:
        raise ValueError(f'Registry profile {selected!r} is not configured.')
    profile = _profile_from_config(selected, value, access_bindings())
    for target in (profile.canonical,) + profile.targets:
        binding_id = target.runtime_binding('aws_eks')
        if binding_id is None:
            continue
        if any(cluster.context == context
               for cluster in profile.bindings[binding_id].qualified_clusters):
            return True
    return False


def configured_profiles() -> tuple[models.ManagedRegistryProfile, ...]:
    values = skypilot_config.get_nested(('container_registries', 'profiles'),
                                        default_value={})
    profiles = parse_profiles(values, access_bindings())
    return tuple(profiles[name] for name in sorted(profiles))


def validate_managed_source_policy(image: models.ContainerImage,
                                   profile: models.ManagedRegistryProfile |
                                   None,
                                   policy: models.WorkspaceImagePolicy) -> None:
    if image.distribution == DIRECT_PROFILE:
        if policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED:
            raise ValueError(
                'This workspace requires managed container images.')
        return
    if (policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED and
            profile is None):
        raise ValueError('This workspace requires managed container images.')
