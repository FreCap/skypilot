"""Registry-profile and workspace-policy resolution."""

from typing import Any

from sky import skypilot_config
from sky.container_images import models
from sky.container_images import providers
from sky.skylet import constants

DIRECT_PROFILE = 'direct'


def get_kubernetes_registry_binding(
        context: str) -> tuple[str, str, str, str] | None:
    """Returns the explicit registry locality/auth binding for a context."""
    binding = skypilot_config.get_nested(
        ('container_registries', 'kubernetes_contexts', context),
        default_value=None)
    if binding is None:
        return None
    registry_prefix = models.normalize_registry_prefix(str(binding['registry']),
                                                       context)
    return (str(binding['registry_provider']).lower(),
            models.normalize_registry_region(
                binding['registry_region'],
                'Kubernetes registry binding region',
                binding['registry_provider']), registry_prefix,
            str(binding['auth_strategy']))


def _workspace_policy_config(workspace: str) -> dict[str, Any]:
    return skypilot_config.get_nested(
        ('workspaces', workspace, 'container_images'), default_value={}) or {}


def get_workspace_policy(
        workspace: str | None = None) -> models.WorkspaceImagePolicy:
    """Returns the effective image policy for a workspace."""
    if workspace is None:
        workspace = (skypilot_config.get_active_workspace() or
                     constants.SKYPILOT_DEFAULT_WORKSPACE)
    config = _workspace_policy_config(workspace)
    retention_weeks = config.get('regional_cache_retention_weeks', 8)
    if (retention_weeks is not None and
        (not isinstance(retention_weeks, int) or
         isinstance(retention_weeks, bool) or retention_weeks <= 0)):
        raise ValueError('regional_cache_retention_weeks must be a positive '
                         'integer or null to disable automatic eviction.')
    return models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode(
            config.get('mode',
                       models.WorkspaceImageMode.MANAGED_PREFERRED.value)),
        default_profile=config.get('default_profile'),
        allowed_profiles=tuple(config.get('allowed_profiles', ())),
        locality=models.Locality(
            config.get('locality', models.Locality.PREFER.value)),
        regional_cache_retention_weeks=retention_weeks,
        max_artifacts=config.get('max_artifacts', 1_000_000),
        max_sources_per_artifact=config.get('max_sources_per_artifact', 128),
        max_releases_per_artifact=config.get('max_releases_per_artifact', 128),
    )


def _profile_from_config(name: str,
                         config: dict[str, Any]) -> models.RegistryProfile:
    if 'revision' not in config:
        raise ValueError(
            f'Registry profile {name!r} must declare a positive monotonic '
            'revision.')
    require_digest_at_runtime = config.get('require_digest_at_runtime', True)
    if not require_digest_at_runtime:
        raise ValueError(
            f'Registry profile {name!r} must require digest-pinned runtime '
            'references. Mutable runtime pulls are not supported.')
    namespace = config['namespace']
    if (config['ownership'] == models.RegistryOwnership.MANAGED.value and
            '{workspace}' not in namespace):
        raise ValueError(
            f'Managed registry profile {name!r} must include the '
            '{workspace} placeholder in namespace. The current catalog is '
            'workspace-scoped and cannot safely evict content from a shared '
            'cross-workspace repository.')
    canonical_config = config['canonical']
    canonical = models.RegistryTarget.from_config('canonical', canonical_config)
    targets = tuple(
        models.RegistryTarget.from_config(target['name'], target)
        for target in config.get('targets', ()))
    target_names = [target.name for target in targets]
    if len(target_names) != len(set(target_names)):
        raise ValueError(
            f'Registry profile {name!r} contains duplicate target names.')
    if canonical.name in target_names:
        raise ValueError(
            f'Registry profile {name!r} reserves {canonical.name!r} for its '
            'canonical target.')
    endpoint_identities = [
        target.endpoint_identity for target in (canonical, *targets)
    ]
    if len(endpoint_identities) != len(set(endpoint_identities)):
        raise ValueError(
            f'Registry profile {name!r} assigns multiple target names to the '
            'same physical registry endpoint. Canonical and regional targets '
            'must be physically distinct so cache eviction cannot delete '
            'canonical content.')
    profile = models.RegistryProfile(
        name=name,
        ownership=models.RegistryOwnership(config['ownership']),
        realm=config['realm'],
        organization=config.get('organization'),
        namespace=namespace,
        require_digest_at_runtime=require_digest_at_runtime,
        canonical=canonical,
        targets=targets,
        revision=config['revision'],
    )
    for target in (profile.canonical, *profile.targets):
        providers.get_adapter(target.provider).validate_target(target)
    return profile


def resolve_profile_name(
    task_profile: str | None,
    workspace: str | None = None,
) -> tuple[str | None, models.WorkspaceImagePolicy]:
    """Resolves profile selection with task, workspace, server precedence."""
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
                f'Workspace {workspace!r} requires managed container images '
                f'and does not allow profile: {DIRECT_PROFILE}.')
        return None, policy
    server_default = skypilot_config.get_nested(
        ('container_registries', 'default_profile'), default_value=None)
    if server_default is not None:
        server_default = models.validate_control_plane_identifier(
            server_default, 'Default container image distribution')
    selected = task_profile or policy.default_profile or server_default
    if selected == DIRECT_PROFILE:
        raise ValueError(
            f'{DIRECT_PROFILE!r} is reserved as an explicit task-level '
            'managed-image bypass and cannot be a default registry profile.')
    if (selected is not None and policy.allowed_profiles and
            selected not in policy.allowed_profiles):
        raise ValueError(
            f'Registry profile {selected!r} is not allowed in workspace '
            f'{workspace!r}. Allowed profiles: '
            f'{list(policy.allowed_profiles)!r}.')
    if (selected is None and
            policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED):
        raise ValueError(
            f'Workspace {workspace!r} requires managed container images but '
            'does not select a registry profile.')
    return selected, policy


def resolve_profile(
    task_profile: str | None,
    workspace: str | None = None,
) -> tuple[models.RegistryProfile | None, models.WorkspaceImagePolicy]:
    """Returns the selected complete profile and effective workspace policy.

    Profile definitions are intentionally selected atomically.  They never
    merge with workspace or task dictionaries, which prevents half-overridden
    identities, namespaces, and registry endpoints.
    """
    name, policy = resolve_profile_name(task_profile, workspace)
    if name is None:
        return None, policy
    config = skypilot_config.get_nested(
        ('container_registries', 'profiles', name), default_value=None)
    if config is None:
        raise ValueError(f'Registry profile {name!r} is not configured.')
    return _profile_from_config(name, config), policy


def validate_managed_source_policy(image: models.ContainerImage,
                                   profile: models.RegistryProfile | None,
                                   policy: models.WorkspaceImagePolicy) -> None:
    """Validates whether an image source may bypass the managed catalog."""
    del image  # A fully qualified ref is still an import source, not a bypass.
    if (policy.mode == models.WorkspaceImageMode.MANAGED_REQUIRED and
            profile is None):
        raise ValueError('This workspace requires container images to use a '
                         'managed registry profile.')


# TODO(fcapponi): Add organization-aware namespace expansion after the API
# server has a first-class organization identifier.  Workspace scoping is
# enforceable today; inventing an organization key here would create a false
# isolation boundary.
