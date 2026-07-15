"""Provider adapter boundary for registry namespace and pull authentication."""

import abc
import dataclasses
import re

from sky.container_images import models
from sky.container_images import references
from sky.container_images import resolver
from sky.provision import docker_utils

_ECR_AUTHORITY_PATTERN = re.compile(
    r'^[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com(?:\.cn)?$')
_GAR_AUTHORITY_PATTERN = re.compile(r'^[a-z0-9-]+-docker\.pkg\.dev$')


@dataclasses.dataclass(frozen=True)
class TransientRegistryCredentials:
    """Short-lived credentials confined to an isolated copy worker."""

    username: str = dataclasses.field(repr=False)
    password: str = dataclasses.field(repr=False)
    server: str
    expires_at: int

    def __getstate__(self):
        raise TypeError('Transient registry credentials must not be pickled.')


def _source_matches_registry_prefix(reference: str, prefix: str) -> bool:
    normalized = models.validate_oci_reference(reference,
                                               'Source fallback reference')
    repository, _ = models.split_digest(normalized)
    last_slash = repository.rfind('/')
    last_colon = repository.rfind(':')
    if last_colon > last_slash:
        repository = repository[:last_colon]
    components = repository.split('/')
    first = components[0]
    has_explicit_authority = (len(components) > 1 and
                              ('.' in first or ':' in first or
                               first == 'localhost' or first.startswith('[')))
    if not has_explicit_authority:
        # Docker-compatible shorthand is policy-equivalent to an explicit
        # docker.io reference even though the user-facing reference remains
        # unchanged.
        repository = f'docker.io/{repository}'
    return repository == prefix or repository.startswith(f'{prefix}/')


def resolve_source_runtime_pull_auth(
    reference: str,
    placement: models.Placement,
    configured_login: docker_utils.DockerLoginConfig | None,
) -> tuple[str, docker_utils.DockerLoginConfig | None]:
    """Pins a source fallback's exact, value-free runtime pull authority."""
    reference = models.validate_oci_reference(reference,
                                              'Source fallback reference')
    authority = models.reference_registry_authority(
        reference, 'Source fallback reference')
    if configured_login is not None:
        if configured_login.username or configured_login.password:
            raise ValueError(
                'Managed source fallback cannot persist inline registry '
                'credentials in a cluster handle. Use a public or '
                'workload-identity source, or wait for a managed READY route.')
        try:
            login_authority = models.normalize_registry_authority(
                configured_login.server, 'Source fallback login')
        except ValueError:
            raise ValueError(
                'A source fallback login instruction must name only its exact '
                'source registry authority.') from None
        if configured_login.server != login_authority or login_authority != authority:
            raise ValueError('A source fallback login instruction must name '
                             'its exact source registry authority.')

    if (placement.backend == 'kubernetes' and
            placement.registry_prefix is not None and
            placement.registry_auth_strategy is not None and
            _source_matches_registry_prefix(reference,
                                            placement.registry_prefix)):
        return (f'kubernetes_context:'
                f'{placement.registry_auth_strategy}', None)

    if _ECR_AUTHORITY_PATTERN.fullmatch(authority):
        if (placement.backend != 'vm' or placement.provider.lower() != 'aws'):
            raise resolver.ImageRouteUnavailableError(
                'A private ECR source fallback requires an AWS VM or an '
                'exact Kubernetes registry binding for that source.')
        return ('ecr_runtime_identity',
                docker_utils.DockerLoginConfig(username='',
                                               password='',
                                               server=authority))

    if _GAR_AUTHORITY_PATTERN.fullmatch(authority):
        if (placement.backend != 'vm' or placement.provider.lower() != 'gcp'):
            raise resolver.ImageRouteUnavailableError(
                'A private GAR source fallback requires a GCP VM or an exact '
                'Kubernetes registry binding for that source.')
        return ('gar_runtime_identity',
                docker_utils.DockerLoginConfig(username='',
                                               password='',
                                               server=authority))

    # For an arbitrary OCI source, SkyPilot cannot infer privacy from its
    # authority. Preserve an explicit value-free login instruction when one
    # exists; otherwise the source retains the direct-image authentication
    # contract and may be publicly pullable.
    return 'source_config', configured_login


class RegistryProviderAdapter(abc.ABC):
    """Narrow provider-specific boundary used by the distribution worker."""

    runtime_pull_auth_strategies = frozenset({'anonymous'})

    def validate_target(self, target: models.RegistryTarget) -> None:
        """Validates provider-specific static target configuration."""
        references.registry_endpoint(target)
        if target.pull_auth not in self.runtime_pull_auth_strategies:
            raise ValueError(
                'Registry target uses an unsupported pull-auth strategy for '
                'its provider.')

    @abc.abstractmethod
    def ensure_target_repository(self, target: models.RegistryTarget,
                                 profile: models.RegistryProfile,
                                 workspace: str, repository: str) -> None:
        """Ensures the exact rendered repository under managed ownership."""

    @abc.abstractmethod
    def mint_copy_credentials(
        self,
        target: models.RegistryTarget,
        repository: str,
        ttl_seconds: int,
    ) -> TransientRegistryCredentials:
        """Mints destination-scoped, short-lived copy credentials."""

    @abc.abstractmethod
    def authorize_manifest_deletion(
        self,
        target: models.RegistryTarget,
        profile: models.RegistryProfile,
        workspace: str,
        reference: str,
    ) -> None:
        """Proves SkyPilot ownership of one exact repository before delete."""

    def resolve_runtime_pull_auth(
        self,
        target: models.RegistryTarget,
        placement: models.Placement,
    ) -> str | None:
        """Returns a safe auth adapter name for a concrete placement."""
        if (placement.backend == 'kubernetes' and
                placement.registry_provider == target.provider and
                placement.registry_region == target.region and
                placement.registry_prefix == target.registry_prefix and
                placement.registry_auth_strategy is not None):
            return f'kubernetes_context:{placement.registry_auth_strategy}'
        if target.pull_auth == 'anonymous':
            return target.pull_auth
        return None

    def runtime_login_config(
        self,
        target: models.RegistryTarget,
        auth_strategy: str,
        placement: models.Placement,
    ) -> docker_utils.DockerLoginConfig | None:
        """Builds a non-secret runtime login instruction for provisioning."""
        del target
        if auth_strategy == 'anonymous':
            return None
        if (placement.backend == 'kubernetes' and
                auth_strategy.startswith('kubernetes_context:')):
            # The named context binding asserts that kubelet/node identity is
            # already configured on the cluster. No credential is serialized
            # here.
            return None
        raise ValueError(
            f'Runtime pull-auth strategy {auth_strategy!r} has no provider '
            'implementation.')


class _ProvisioningDeferredAdapter(RegistryProviderAdapter):
    """Shared safe behavior until provider control-plane writes are enabled."""

    def ensure_target_repository(self, target: models.RegistryTarget,
                                 profile: models.RegistryProfile,
                                 workspace: str, repository: str) -> None:
        del workspace, repository
        references.registry_endpoint(target)
        if profile.ownership == models.RegistryOwnership.MANAGED:
            raise NotImplementedError(
                f'Managed {target.provider} repository provisioning is not '
                'enabled. Bootstrap the repository externally and use '
                'ownership: external for the v0 validation path.')

    def mint_copy_credentials(
        self,
        target: models.RegistryTarget,
        repository: str,
        ttl_seconds: int,
    ) -> TransientRegistryCredentials:
        del repository, ttl_seconds
        raise NotImplementedError(
            f'{target.provider} short-lived copy credentials are not enabled.')

    def authorize_manifest_deletion(
        self,
        target: models.RegistryTarget,
        profile: models.RegistryProfile,
        workspace: str,
        reference: str,
    ) -> None:
        del profile, workspace, reference
        raise NotImplementedError(
            f'Managed {target.provider} registry deletion authorization is '
            'not enabled. Repository ownership must be proved by the provider '
            'adapter before automatic eviction.')


class AwsRegistryAdapter(_ProvisioningDeferredAdapter):
    """AWS ECR adapter boundary."""

    runtime_pull_auth_strategies = frozenset({'ecr_runtime_identity'})

    def validate_target(self, target: models.RegistryTarget) -> None:
        super().validate_target(target)
        if target.account is None:
            raise ValueError('AWS registry target requires an explicit '
                             'account.')
        authority = references.registry_endpoint(target).split('/', 1)[0]
        expected = models.aws_ecr_registry_authority(target.account,
                                                     target.region)
        if authority != expected:
            raise ValueError(
                'AWS registry target must use its exact ECR authority derived '
                'from the configured account and region; use provider '
                'generic for a non-ECR registry.')

    def resolve_runtime_pull_auth(
        self,
        target: models.RegistryTarget,
        placement: models.Placement,
    ) -> str | None:
        common = super().resolve_runtime_pull_auth(target, placement)
        if common is not None:
            return common
        if (target.pull_auth == 'ecr_runtime_identity' and
                placement.provider.lower() == 'aws' and
                placement.backend == 'vm'):
            return target.pull_auth
        return None

    def runtime_login_config(
        self,
        target: models.RegistryTarget,
        auth_strategy: str,
        placement: models.Placement,
    ) -> docker_utils.DockerLoginConfig | None:
        if (auth_strategy != 'ecr_runtime_identity' or
                placement.provider.lower() != 'aws' or
                placement.backend != 'vm'):
            return super().runtime_login_config(target, auth_strategy,
                                                placement)
        return docker_utils.DockerLoginConfig(
            username='',
            password='',
            server=references.registry_endpoint(target).split('/', 1)[0],
        )

    # TODO(fcapponi): Create ECR repositories only under the bootstrapped
    # namespace, require SkyPilot ownership tags, and refuse colliding unowned
    # repositories. Do not call DeleteRepository or SetRepositoryPolicy.
    #
    # TODO(fcapponi): Mint destination-scoped ECR authorization inside the copy
    # worker and implement a separate short-lived cross-cloud pull adapter.


class GcpRegistryAdapter(_ProvisioningDeferredAdapter):
    """Google Artifact Registry adapter boundary."""

    runtime_pull_auth_strategies = frozenset(
        {'anonymous', 'gar_runtime_identity'})

    def validate_target(self, target: models.RegistryTarget) -> None:
        super().validate_target(target)
        if target.project is None:
            raise ValueError('GCP registry target requires an explicit '
                             'project.')
        registry_prefix = references.registry_endpoint(target)
        expected = f'{target.region}-docker.pkg.dev/{target.project}'
        if (registry_prefix != expected and
                not registry_prefix.startswith(f'{expected}/')):
            raise ValueError(
                'GCP registry target must use its exact GAR project prefix '
                'derived from the configured project and region; use '
                'provider generic for a non-GAR registry.')

    def resolve_runtime_pull_auth(
        self,
        target: models.RegistryTarget,
        placement: models.Placement,
    ) -> str | None:
        common = super().resolve_runtime_pull_auth(target, placement)
        if common is not None:
            return common
        if (target.pull_auth == 'gar_runtime_identity' and
                placement.provider.lower() == 'gcp' and
                placement.backend == 'vm'):
            return target.pull_auth
        return None

    def runtime_login_config(
        self,
        target: models.RegistryTarget,
        auth_strategy: str,
        placement: models.Placement,
    ) -> docker_utils.DockerLoginConfig | None:
        if auth_strategy == 'anonymous':
            return None
        if (auth_strategy != 'gar_runtime_identity' or
                placement.provider.lower() != 'gcp' or
                placement.backend != 'vm'):
            return super().runtime_login_config(target, auth_strategy,
                                                placement)
        return docker_utils.DockerLoginConfig(
            username='',
            password='',
            server=references.registry_endpoint(target).split('/', 1)[0],
        )

    # TODO(fcapponi): Create standard GAR repositories in explicit profile
    # regions and mint OAuth access tokens for the isolated copy worker. GAR
    # remote repositories are not VERIFIED routes and need a future PROXIED
    # capability model.


class NebiusRegistryAdapter(_ProvisioningDeferredAdapter):
    """Nebius Container Registry adapter boundary."""

    # TODO(fcapponi): Add Nebius registry creation and a native service-account
    # token or credential-helper adapter through DockerLoginConfig. Generic
    # username/password plumbing alone is not a production identity design.


class GenericRegistryAdapter(_ProvisioningDeferredAdapter):
    """Validation-only adapter for externally provisioned OCI registries."""


_ADAPTERS: dict[str, RegistryProviderAdapter] = {
    'aws': AwsRegistryAdapter(),
    'gcp': GcpRegistryAdapter(),
    'nebius': NebiusRegistryAdapter(),
    'generic': GenericRegistryAdapter(),
}


def get_adapter(provider: str) -> RegistryProviderAdapter:
    """Returns the registered adapter for a profile target provider."""
    try:
        return _ADAPTERS[provider.lower()]
    except KeyError:
        raise ValueError('Unsupported container registry provider.') from None


# TODO(fcapponi): Add a secret-reference-based runtime strategy for externally
# managed private OCI registries. Registry profiles and resolved Resources must
# keep carrying only the reference, never the credential value.
