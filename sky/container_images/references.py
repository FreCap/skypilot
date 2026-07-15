"""Pure physical-reference construction for managed registry targets."""

from sky.container_images import models

# ECR limits images per repository, while repositories themselves are also a
# quota-managed resource. Two digest hex characters provide a fixed 256-way
# workspace shard set: repositories are created lazily, but artifact count can
# never make the number of repositories grow beyond this bound.
_MANAGED_REPOSITORY_SHARD_HEX_CHARS = 2


def registry_endpoint(target: models.RegistryTarget) -> str:
    """Returns the configured or derivable registry endpoint."""
    registry_prefix = target.registry_prefix
    if registry_prefix is not None:
        return registry_prefix
    raise ValueError(
        f'Registry target {target.name!r} needs an explicit registry field; '
        f'its {target.provider!r} endpoint cannot be derived safely.')


def managed_reference(profile: models.RegistryProfile,
                      target: models.RegistryTarget, workspace: str,
                      source_ref: str, digest: str) -> str:
    """Constructs a workspace-scoped digest-pinned destination reference."""
    namespace = profile.namespace
    replacements = {
        'realm': profile.realm,
        'workspace': workspace,
    }
    if '{organization}' in namespace:
        if profile.organization is None:
            raise ValueError(
                f'Registry profile {profile.name!r} uses the organization '
                'namespace placeholder but does not configure organization.')
        replacements['organization'] = profile.organization
    for key, value in replacements.items():
        namespace = namespace.replace(f'{{{key}}}', value)
    if '{' in namespace or '}' in namespace:
        raise ValueError(
            f'Registry profile {profile.name!r} has an unknown namespace '
            f'placeholder: {namespace!r}.')
    namespace = namespace.strip('/')
    try:
        namespace = models.validate_registry_repository_path(
            namespace, f'Managed registry namespace {namespace!r}')
    except ValueError as e:
        raise ValueError(
            f'Invalid managed registry namespace: {namespace!r}.') from e
    _, normalized_digest = models.split_digest(f'placeholder@{digest}')
    assert normalized_digest is not None
    normalized_source = models.validate_oci_reference(
        source_ref, 'Managed image source reference')
    _, source_digest = models.split_digest(normalized_source)
    if source_digest is None:
        raise ValueError(
            'Managed image source reference must be digest-pinned.')
    if source_digest != normalized_digest:
        raise ValueError('Managed destination digest must match the immutable '
                         'source reference digest.')
    # Source aliases select immutable import provenance, but never contribute
    # to destination identity. A bounded digest-prefix shard avoids both an
    # unbounded repository per artifact and a single repository's image quota.
    digest_hex = normalized_digest.split(':', 1)[1]
    repository_shard = digest_hex[:_MANAGED_REPOSITORY_SHARD_HEX_CHARS]
    reference = (f'{registry_endpoint(target)}/{namespace}/'
                 f'artifacts-{repository_shard}@{normalized_digest}')
    return models.validate_oci_reference(reference,
                                         'Managed destination reference')


# TODO(fcapponi): Add a deterministic readable shortening scheme if production
# profiles need namespaces that do not fit Docker's repository-name limit.
