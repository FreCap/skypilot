"""Secret-free models for managed container image distribution.

This module deliberately contains no cloud SDK calls.  The models are shared
by task parsing, the pure placement resolver, durable catalog state, and
provider adapters.  Keeping credentials out of these values makes them safe to
pickle into launch state or return through the API.
"""

import dataclasses
import enum
import hashlib
import ipaddress
import json
import re
from typing import Any
import urllib.parse
import uuid

from sky.skylet import constants as skylet_constants

_DIGEST_PATTERN = re.compile(r'^sha256:[0-9a-fA-F]{64}$')
_FINGERPRINT_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_REGISTRY_COMPONENT = r'[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*'
_REGISTRY_PATH_PATTERN = re.compile(
    rf'^{_REGISTRY_COMPONENT}(?:/{_REGISTRY_COMPONENT})*$')
_TAG_PATTERN = re.compile(r'^[\w][\w.-]{0,127}$', re.ASCII)
_OCI_PLATFORM_COMPONENT_PATTERN = re.compile(r'^[a-z0-9]+(?:[._-][a-z0-9]+)*$')
_CONTROL_PLANE_IDENTIFIER_PATTERN = re.compile(
    r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')
_CATALOG_UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
_DNS_NAME_PATTERN = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?'
                               r'(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$')
_MAX_REFERENCE_LENGTH = 1024
_MAX_REPOSITORY_NAME_LENGTH = 255
_MAX_RELEASE_LENGTH = 128
_MAX_OCI_PLATFORM_LENGTH = 128
_MAX_OCI_PLATFORMS = 128
_MAX_REGISTRY_PROFILE_TARGETS = 128
_MAX_WORKSPACE_ARTIFACTS = 100_000_000
_MAX_ARTIFACT_ALIASES = 4096
_MAX_SIGNED_64_BIT_INTEGER = (1 << 63) - 1
_RUNTIME_AUTH_STRATEGY_PATTERN = re.compile(
    r'^(?:anonymous|source_config|ecr_runtime_identity|gar_runtime_identity|'
    r'kubernetes_context:node_identity)$')
_TARGET_PULL_AUTH_STRATEGIES = frozenset(
    {'anonymous', 'ecr_runtime_identity', 'gar_runtime_identity'})
_OCI_RUNTIME_ARCHITECTURES = {
    'amd64': 'amd64',
    'x86_64': 'amd64',
    'arm64': 'arm64',
    'aarch64': 'arm64',
}
_PORTABLE_UNKNOWN_RUNTIME_PLATFORMS = frozenset({'linux/amd64', 'linux/arm64'})
_REGISTRY_LOCALITY_REGION_PATTERN = re.compile(
    r'^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,511}$')
_AWS_REGISTRY_REGION_PATTERN = re.compile(
    r'^[a-z]{2}(?:-[a-z0-9]+){1,3}-[0-9]+$')
_GCP_STYLE_REGISTRY_REGION_PATTERN = re.compile(
    r'^(?:us|europe|asia|[a-z][a-z0-9]*(?:-[a-z0-9]+)*[0-9])$')
_REGISTRY_NAMESPACE_PLACEHOLDERS = frozenset(
    {'organization', 'realm', 'workspace'})


def validate_workspace_name(value: Any, subject: str) -> str:
    """Validates one established SkyPilot workspace name without reflection."""
    if (not isinstance(value, str) or
            len(value) > skylet_constants.WORKSPACE_NAME_MAX_LENGTH or
            re.fullmatch(skylet_constants.WORKSPACE_NAME_VALID_REGEX,
                         value) is None):
        raise ValueError(
            f'{subject} must be a valid bounded SkyPilot workspace name.')
    return value


def validate_oci_platform(value: str, subject: str) -> str:
    """Validates one bounded ``os/architecture[/variant]`` value."""
    if (not isinstance(value, str) or not value or
            len(value) > _MAX_OCI_PLATFORM_LENGTH):
        raise ValueError(
            f'{subject} must be a bounded OCI os/architecture value.')
    components = value.split('/')
    if (len(components) not in (2, 3) or
            any(not _OCI_PLATFORM_COMPONENT_PATTERN.fullmatch(component)
                for component in components)):
        raise ValueError(
            f'{subject} must use lowercase OCI os/architecture[/variant] '
            'components.')
    return value


def validate_oci_platforms(platforms: Any, subject: str) -> tuple[str, ...]:
    """Validates bounded, unique OCI platform metadata."""
    if not isinstance(platforms, (list, tuple)):
        raise ValueError(f'{subject} must be a list of OCI platform values.')
    if len(platforms) > _MAX_OCI_PLATFORMS:
        raise ValueError(
            f'{subject} must contain at most {_MAX_OCI_PLATFORMS} values.')
    validated = tuple(
        validate_oci_platform(platform, subject) for platform in platforms)
    if len(set(validated)) != len(validated):
        raise ValueError(f'{subject} must not contain duplicate values.')
    return validated


def validate_materialization_platforms(platforms: Any,
                                       subject: str) -> tuple[str, ...]:
    """Validates the nonempty platform proof required for READY content."""
    validated = validate_oci_platforms(platforms, subject)
    if not validated:
        raise ValueError(
            f'{subject} must contain at least one OCI platform value.')
    return validated


def runtime_platform_from_architecture(architecture: str | None) -> str | None:
    """Maps a known machine architecture to its Linux OCI platform."""
    if not isinstance(architecture, str):
        return None
    normalized = _OCI_RUNTIME_ARCHITECTURES.get(architecture.lower())
    if normalized is None:
        return None
    return f'linux/{normalized}'


def _is_safe_generic_platform(platform: str) -> bool:
    """Returns whether an image platform is safe for an arch-only runtime."""
    components = platform.split('/')
    if len(components) == 2:
        return True
    # OCI's baseline variants are compatible with the generic architectures
    # SkyPilot receives from cloud catalogs. Newer variants require node-level
    # CPU feature proof that an architecture string does not provide.
    return ((components[:2] == ['linux', 'amd64'] and components[2] == 'v1') or
            (components[:2] == ['linux', 'arm64'] and components[2] == 'v8'))


def platforms_support_runtime(platforms: tuple[str, ...] | list[str],
                              runtime_platform: str | None) -> bool:
    """Returns whether bounded OCI metadata supports a concrete runtime."""
    validated = validate_oci_platforms(platforms, 'Artifact platforms')
    if runtime_platform is None:
        # An unknown node architecture is not proof that a single-platform
        # image can run.  It is safe only when the verified image covers every
        # Linux architecture SkyPilot currently knows how to provision.
        platform_families = {
            '/'.join(platform.split('/')[:2])
            for platform in validated
            if _is_safe_generic_platform(platform)
        }
        return _PORTABLE_UNKNOWN_RUNTIME_PLATFORMS.issubset(platform_families)
    if not validated:
        return False
    runtime = validate_oci_platform(runtime_platform, 'Runtime platform')
    runtime_components = runtime.split('/')
    for platform in validated:
        components = platform.split('/')
        if components[:2] != runtime_components[:2]:
            continue
        if len(runtime_components) == 2:
            if _is_safe_generic_platform(platform):
                return True
            continue
        if len(components) == 2 or components[2] == runtime_components[2]:
            return True
    return False


def validate_compressed_size_bytes(value: int | None,
                                   subject: str) -> int | None:
    """Validates optional artifact size metadata for durable persistence."""
    if value is None:
        return None
    if (not isinstance(value, int) or isinstance(value, bool) or value < 0 or
            value > _MAX_SIGNED_64_BIT_INTEGER):
        raise ValueError(
            f'{subject} must be a nonnegative signed 64-bit integer.')
    return value


@dataclasses.dataclass(frozen=True)
class MaterializationResult:
    """Exact digest and nonempty OCI platform proof from registry I/O."""

    digest: str
    platforms: tuple[str, ...]
    compressed_size_bytes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'digest',
            validate_sha256_digest(self.digest, 'Materialization digest'))
        object.__setattr__(
            self, 'platforms',
            validate_materialization_platforms(self.platforms,
                                               'Materialization platforms'))
        object.__setattr__(
            self, 'compressed_size_bytes',
            validate_compressed_size_bytes(self.compressed_size_bytes,
                                           'Materialization compressed size'))


def normalize_registry_authority(authority: str, target_name: str) -> str:
    """Returns one canonical OCI registry host and optional nondefault port."""
    if authority.endswith(':'):
        raise ValueError(
            f'Registry target {target_name!r} has an invalid registry port.')
    try:
        parsed = urllib.parse.urlsplit(f'//{authority}')
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        raise ValueError(
            f'Registry target {target_name!r} has an invalid registry host '
            f'or port.') from None
    if (hostname is None or parsed.path or parsed.query or parsed.fragment or
            parsed.username is not None or parsed.password is not None):
        raise ValueError(
            f'Registry target {target_name!r} has an invalid registry host.')
    hostname = hostname.lower()
    if hostname.endswith('..') or '%' in hostname:
        raise ValueError(
            f'Registry target {target_name!r} has an invalid registry host.')
    if hostname.endswith('.'):
        hostname = hostname[:-1]
    if not hostname or len(hostname) > 253:
        raise ValueError(
            f'Registry target {target_name!r} has an invalid registry host.')
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        if not _DNS_NAME_PATTERN.fullmatch(hostname):
            raise ValueError(
                f'Registry target {target_name!r} has an invalid registry '
                'DNS name or IP address.') from None
        normalized_host = hostname
    else:
        normalized_host = address.compressed
        if address.version == 6:
            normalized_host = f'[{normalized_host}]'
    if port is not None and port <= 0:
        raise ValueError(
            f'Registry target {target_name!r} has an invalid registry port.')
    if port not in (None, 443):
        normalized_host = f'{normalized_host}:{port}'
    return normalized_host


def validate_registry_repository_path(path: str, subject: str) -> str:
    """Validates and returns one lowercase OCI repository path."""
    if (not isinstance(path, str) or not path or
            len(path) > _MAX_REPOSITORY_NAME_LENGTH or
            not _REGISTRY_PATH_PATTERN.fullmatch(path)):
        raise ValueError(
            f'{subject} must use lowercase OCI name components separated by '
            'single slashes.')
    return path


def normalize_registry_namespace_template(namespace: str, realm: str,
                                          organization: str | None,
                                          profile_name: str) -> str:
    """Returns one canonical, renderable managed repository template."""
    if not isinstance(namespace, str) or not namespace:
        raise ValueError(
            f'Registry profile {profile_name!r} needs a namespace template.')
    normalized = namespace.strip('/')
    if not normalized:
        raise ValueError(
            f'Registry profile {profile_name!r} needs a namespace template.')
    placeholders = set(re.findall(r'\{([^{}]+)\}', normalized))
    if ('{' in re.sub(r'\{[^{}]+\}', '', normalized) or
            '}' in re.sub(r'\{[^{}]+\}', '', normalized) or
            not placeholders.issubset(_REGISTRY_NAMESPACE_PLACEHOLDERS)):
        raise ValueError(
            f'Registry profile {profile_name!r} has an unknown namespace '
            'placeholder.')
    realm = validate_registry_repository_path(
        realm, f'Registry profile {profile_name!r} realm')
    if organization is not None:
        organization = validate_registry_repository_path(
            organization, f'Registry profile {profile_name!r} organization')
    if 'organization' in placeholders and organization is None:
        raise ValueError(
            f'Registry profile {profile_name!r} uses the organization '
            'namespace placeholder but does not configure organization.')
    rendered = normalized.replace('{realm}',
                                  realm).replace('{workspace}', 'workspace')
    if organization is not None:
        rendered = rendered.replace('{organization}', organization)
    validate_registry_repository_path(
        rendered, f'Registry profile {profile_name!r} namespace')
    return normalized


def normalize_registry_prefix(registry: str, target_name: str) -> str:
    """Canonicalizes an OCI authority plus optional repository prefix."""
    registry = registry.rstrip('/')
    if ('://' in registry or '@' in registry or
            any(char.isspace() for char in registry) or
            registry.startswith('/') or not registry):
        raise ValueError(
            f'Registry target {target_name!r} must contain only an OCI '
            'registry host and optional path, without a URL scheme, '
            'userinfo, or whitespace.')
    host, separator, path = registry.partition('/')
    authority = normalize_registry_authority(host, target_name)
    if not separator:
        return authority
    path = validate_registry_repository_path(
        path, f'Registry target {target_name!r} repository path')
    return f'{authority}/{path}'


def aws_ecr_registry_authority(account: str, region: str) -> str:
    """Returns the partition-correct private ECR registry authority."""
    if (not isinstance(account, str) or
            re.fullmatch(r'[0-9]{12}', account) is None):
        raise ValueError('AWS ECR account must be a 12-digit account ID.')
    region = validate_control_plane_identifier(region, 'AWS ECR region')
    dns_suffix = ('amazonaws.com.cn'
                  if region.startswith('cn-') else 'amazonaws.com')
    return f'{account}.dkr.ecr.{region}.{dns_suffix}'


def normalize_registry_region(value: Any,
                              subject: str,
                              provider: Any | None = None) -> str:
    """Normalizes a registry locality region without reflecting its value."""
    normalized = (value.strip().lower() if isinstance(value, str) else value)
    normalized = validate_control_plane_identifier(normalized, subject)
    if provider is None:
        return normalized
    provider = (provider.strip().lower()
                if isinstance(provider, str) else provider)
    provider = validate_control_plane_identifier(provider,
                                                 'Registry region provider')
    if (provider == 'aws' and
            _AWS_REGISTRY_REGION_PATTERN.fullmatch(normalized) is None):
        raise ValueError(f'{subject} must be a bounded AWS region name.')
    if (provider in ('gcp', 'nebius') and
            _GCP_STYLE_REGISTRY_REGION_PATTERN.fullmatch(normalized) is None):
        raise ValueError(f'{subject} must be a bounded cloud region name.')
    return normalized


class RegistryOwnership(enum.Enum):
    """Whether SkyPilot owns content lifecycle in a registry namespace.

    Managed ownership permits creation and regional-cache manifest eviction.
    Canonical content and catalog records are retained independently.
    """

    MANAGED = 'managed'
    EXTERNAL = 'external'


class WorkspaceImageMode(enum.Enum):
    """How strongly a workspace requires the managed image catalog."""

    MANAGED_REQUIRED = 'managed_required'
    MANAGED_PREFERRED = 'managed_preferred'


class Locality(enum.Enum):
    """Container image locality policy for a concrete placement."""

    PREFER = 'prefer'
    REQUIRE = 'require'
    CANONICAL = 'canonical'


class ImageLocationState(enum.Enum):
    """Durable state of one logical image at one registry target."""

    PENDING = 'PENDING'
    COPYING = 'COPYING'
    READY = 'READY'
    FAILED = 'FAILED'
    MISSING = 'MISSING'
    EVICTING = 'EVICTING'
    EVICTED = 'EVICTED'


class ImageLocationErrorCode(enum.Enum):
    """Closed, secret-free diagnostics persisted for image locations."""

    COPY_LEASE_EXPIRED = 'copy_lease_expired'
    EVICTION_LEASE_EXPIRED = 'eviction_lease_expired'
    DESTINATION_REFERENCE_INVALID = 'destination_reference_invalid'
    DESTINATION_DIGEST_MISMATCH = 'destination_digest_mismatch'
    REGIONAL_IDENTITY_MISMATCH = 'regional_identity_mismatch'
    DESTINATION_ALIAS_CONFLICT = 'destination_alias_conflict'
    MANIFEST_DIGEST_MISMATCH = 'manifest_digest_mismatch'
    MANIFEST_MISSING = 'manifest_missing'
    MATERIALIZATION_FAILED = 'materialization_failed'
    EXTERNAL_ADOPTION_FAILED = 'external_adoption_failed'
    REVALIDATION_FAILED = 'revalidation_failed'
    EVICTION_REFERENCE_INVALID = 'eviction_reference_invalid'
    EVICTION_FAILED = 'eviction_failed'
    EVICTION_COMPLETION_FENCE_CHANGED = ('eviction_completion_fence_changed')


class ImageFallbackReason(enum.Enum):
    """Closed reasons for using an explicitly authorized source route."""

    MANAGED_ROUTE_WARMING = 'managed_route_warming'


class ImageProducerKind(enum.Enum):
    """Closed producer kinds safe to persist and return through the API."""

    EXTERNAL_OCI = 'external_oci'


def validate_image_producer_kind(value: Any, subject: str) -> str:
    """Validates one supported, secret-free artifact producer kind."""
    try:
        return ImageProducerKind(value).value
    except (TypeError, ValueError):
        raise ValueError(
            f'{subject} must be a supported container image producer kind.'
        ) from None


def validate_producer_spec_hash(value: Any, subject: str) -> str | None:
    """Validates an optional SHA-256 producer specification fingerprint."""
    if value is None:
        return None
    if (not isinstance(value, str) or
            not re.fullmatch(r'[0-9a-fA-F]{64}', value)):
        raise ValueError(f'{subject} must be a 64-character hexadecimal hash.')
    return value.lower()


def validate_builder_version(value: Any, subject: str) -> str | None:
    """Validates optional bounded producer version metadata."""
    if value is None:
        return None
    return validate_control_plane_identifier(value, subject)


def split_digest(reference: str) -> tuple[str, str | None]:
    """Returns an OCI reference without its digest and the digest, if any."""
    if '@' not in reference:
        return reference, None
    repository, digest = reference.rsplit('@', 1)
    if not repository or not _DIGEST_PATTERN.fullmatch(digest):
        raise ValueError(
            'Container image digests must use sha256 followed by 64 hex '
            'characters.')
    return repository, digest.lower()


def validate_oci_reference(reference: str, subject: str) -> str:
    """Validates and canonicalizes one secret-free OCI image reference."""
    if len(reference) > _MAX_REFERENCE_LENGTH:
        raise ValueError(
            f'{subject} must be at most {_MAX_REFERENCE_LENGTH} characters.')
    if (not reference or any(char.isspace() for char in reference) or
            '://' in reference):
        raise ValueError(
            f'{subject} must be an OCI image reference without a URL scheme '
            'or whitespace.')
    repository, digest = split_digest(reference)
    if '@' in repository:
        raise ValueError(
            f'{subject} must not contain inline userinfo. Use a server-side '
            'credential reference instead.')
    if any(character in repository for character in ('?', '#', '%', '\\')):
        raise ValueError(
            f'{subject} must not contain a URL query, fragment, backslash, or '
            'percent-encoded material. Credentials must never be embedded in '
            'an image reference.')

    last_slash = repository.rfind('/')
    last_colon = repository.rfind(':')
    name = repository
    tag = None
    if last_colon > last_slash:
        name, tag = repository[:last_colon], repository[last_colon + 1:]
        if not name or not _TAG_PATTERN.fullmatch(tag):
            raise ValueError(f'{subject} has an invalid OCI image tag.')
    if not name or name.startswith('/') or name.endswith('/') or '//' in name:
        raise ValueError(f'{subject} has an invalid OCI repository name.')

    components = name.split('/')
    first = components[0]
    authority = None
    has_registry = (len(components) > 1 and
                    ('.' in first or ':' in first or first == 'localhost' or
                     first.startswith('[')))
    if has_registry:
        authority = normalize_registry_authority(first, subject)
        components = components[1:]
    if not components:
        raise ValueError(f'{subject} must include an OCI repository path.')
    repository_path = validate_registry_repository_path('/'.join(components),
                                                        subject)
    if authority is not None:
        normalized_name = f'{authority}/{repository_path}'
    else:
        normalized_name = repository_path
    if len(normalized_name) > _MAX_REPOSITORY_NAME_LENGTH:
        raise ValueError(f'{subject} repository name must be at most '
                         f'{_MAX_REPOSITORY_NAME_LENGTH} characters.')
    normalized_repository = normalized_name
    if tag is not None:
        normalized_repository = f'{normalized_repository}:{tag}'
    if digest is not None:
        return f'{normalized_repository}@{digest}'
    return normalized_repository


def validate_release_label(release: str, subject: str) -> str:
    """Validates a safe immutable release label using OCI tag grammar."""
    if (not isinstance(release, str) or len(release) > _MAX_RELEASE_LENGTH or
            not _TAG_PATTERN.fullmatch(release)):
        raise ValueError(
            f'{subject} must start with an ASCII letter, digit, or underscore '
            'and contain only ASCII letters, digits, underscores, periods, '
            f'or hyphens, up to {_MAX_RELEASE_LENGTH} characters.')
    return release


def validate_sha256_digest(value: str, subject: str) -> str:
    """Validates and canonicalizes one SHA-256 OCI digest."""
    if not isinstance(value, str) or not _DIGEST_PATTERN.fullmatch(value):
        raise ValueError(f'{subject} must be a SHA-256 OCI digest.')
    return value.lower()


def validate_fingerprint(value: Any, subject: str) -> str:
    """Validates one lowercase 64-hex control-plane fingerprint."""
    if not isinstance(value, str) or not _FINGERPRINT_PATTERN.fullmatch(value):
        raise ValueError(
            f'{subject} must be a lowercase 64-character hexadecimal hash.')
    return value


def validate_control_plane_identifier(value: str, subject: str) -> str:
    """Validates a bounded identifier that is safe to persist and render."""
    if (not isinstance(value, str) or
            not _CONTROL_PLANE_IDENTIFIER_PATTERN.fullmatch(value)):
        raise ValueError(
            f'{subject} must start with an ASCII letter or digit and contain '
            'only ASCII letters, digits, underscores, periods, or hyphens, '
            'up to 128 characters.')
    return value


def validate_catalog_id(value: str, subject: str) -> str:
    """Validates and canonicalizes one SkyPilot-generated UUID."""
    if (not isinstance(value, str) or
            not _CATALOG_UUID_PATTERN.fullmatch(value)):
        raise ValueError(f'{subject} must be a canonical UUID.')
    parsed = uuid.UUID(value)
    if parsed.version is None or parsed.variant != uuid.RFC_4122:
        raise ValueError(f'{subject} must be a canonical UUID.')
    return str(parsed)


def reference_registry_authority(reference: str, subject: str) -> str:
    """Returns the canonical runtime registry authority for a reference.

    Docker-compatible runtimes resolve references without an explicit
    authority through Docker Hub. Keep the user-facing shorthand intact, but
    make the implicit authority explicit whenever policy or authentication is
    derived from it.
    """
    normalized = validate_oci_reference(reference, subject)
    repository, _ = split_digest(normalized)
    last_slash = repository.rfind('/')
    last_colon = repository.rfind(':')
    if last_colon > last_slash:
        repository = repository[:last_colon]
    components = repository.split('/')
    first = components[0]
    if (len(components) > 1 and
        ('.' in first or ':' in first or first == 'localhost' or
         first.startswith('['))):
        return first
    return 'docker.io'


@dataclasses.dataclass(frozen=True, init=False)
class ContainerImage:
    """A user-facing immutable image selector and distribution override.

    A source reference may be combined with ``release`` to create an immutable
    human-readable binding on first use. Subsequent tasks can select only the
    release. ``artifact_id`` is the machine-oriented selector and is exclusive
    with the other identity fields.

    ``distribution='direct'`` is the explicit escape hatch for a workspace
    using ``managed_preferred``. It is rejected by ``managed_required`` policy.
    """

    ref: str | None = None
    release: str | None = None
    artifact_id: str | None = None
    distribution: str | None = None

    def __init__(
        self,
        ref: str | None = None,
        release: str | None = None,
        artifact_id: str | None = None,
        distribution: str | None = None,
        *,
        profile: str | None = None,
        version: str | None = None,
    ) -> None:
        """Constructs a selector, accepting the pre-release keyword aliases."""
        if (distribution is not None and profile is not None and
                distribution != profile):
            raise ValueError('container_image cannot specify conflicting '
                             'distribution and profile values.')
        if release is not None and version is not None and release != version:
            raise ValueError('container_image cannot specify conflicting '
                             'release and version values.')
        object.__setattr__(self, 'ref', ref)
        object.__setattr__(self, 'release',
                           release if release is not None else version)
        object.__setattr__(self, 'artifact_id', artifact_id)
        object.__setattr__(
            self, 'distribution',
            distribution if distribution is not None else profile)
        self.__post_init__()

    def __post_init__(self) -> None:
        if self.artifact_id is not None and (self.ref is not None or
                                             self.release is not None):
            raise ValueError('container_image.artifact_id cannot be combined '
                             'with ref or release.')
        if (self.ref is None and self.release is None and
                self.artifact_id is None):
            raise ValueError('container_image requires one of ref, release, '
                             'or artifact_id.')

        if self.ref is not None:
            ref = self.ref
            if not ref or any(char.isspace() for char in ref):
                raise ValueError(
                    'container_image.ref must be a non-empty OCI reference '
                    'with no whitespace.')
            if len(ref) > _MAX_REFERENCE_LENGTH:
                raise ValueError('container_image.ref must be at most '
                                 f'{_MAX_REFERENCE_LENGTH} characters.')
            ref = validate_oci_reference(ref, 'container_image.ref')
            object.__setattr__(self, 'ref', ref)

        if self.release is not None:
            release = validate_release_label(self.release,
                                             'container_image.release')
            object.__setattr__(self, 'release', release)

        if self.artifact_id is not None:
            artifact_id = validate_catalog_id(self.artifact_id,
                                              'container_image.artifact_id')
            object.__setattr__(self, 'artifact_id', artifact_id)

        if self.distribution is not None:
            distribution = validate_control_plane_identifier(
                self.distribution, 'container_image.distribution')
            object.__setattr__(self, 'distribution', distribution)

    @property
    def digest(self) -> str | None:
        """Returns the pinned digest, or None for a mutable source tag."""
        if self.ref is None:
            return None
        return split_digest(self.ref)[1]

    @property
    def profile(self) -> str | None:
        """Compatibility alias for pre-release callers."""
        return self.distribution

    @property
    def version(self) -> str | None:
        """Compatibility alias for pre-release callers."""
        return self.release

    @classmethod
    def from_config(
        cls,
        value: 'ContainerImage | str | dict[str, Any]',
    ) -> 'ContainerImage':
        """Parses the scalar or object task-YAML representation."""
        if isinstance(value, cls):
            # Reconstruct even frozen instances. A restored object, or one
            # altered with object.__setattr__, must cross the current trust
            # boundary again instead of bypassing validation.
            return cls(ref=value.ref,
                       release=value.release,
                       artifact_id=value.artifact_id,
                       distribution=value.distribution)
        if isinstance(value, str):
            return cls(ref=value)
        if not isinstance(value, dict):
            raise ValueError('container_image must be a string or an object '
                             'with ref, release, or artifact_id.')
        unknown = set(value) - {
            'ref', 'release', 'artifact_id', 'distribution', 'profile',
            'version'
        }
        if unknown:
            raise ValueError('container_image contains unsupported fields.')
        ref = value.get('ref')
        if ref is not None and not isinstance(ref, str):
            raise ValueError('container_image.ref must be a string.')
        artifact_id = value.get('artifact_id')
        if artifact_id is not None and not isinstance(artifact_id, str):
            raise ValueError('container_image.artifact_id must be a string.')
        distribution = value.get('distribution', value.get('profile'))
        if ('distribution' in value and 'profile' in value and
                value['distribution'] != value['profile']):
            raise ValueError('container_image cannot specify conflicting '
                             'distribution and profile values.')
        if distribution is not None and not isinstance(distribution, str):
            raise ValueError('container_image.distribution must be a string.')
        release = value.get('release', value.get('version'))
        if ('release' in value and 'version' in value and
                value['release'] != value['version']):
            raise ValueError('container_image cannot specify conflicting '
                             'release and version values.')
        if release is not None and not isinstance(release, str):
            raise ValueError('container_image.release must be a string.')
        return cls(ref=ref,
                   release=release,
                   artifact_id=artifact_id,
                   distribution=distribution)

    def to_yaml_config(self) -> str | dict[str, str]:
        """Returns the compact task-YAML representation."""
        validated = type(self).from_config(self)
        if (validated.ref is not None and validated.release is None and
                validated.artifact_id is None and
                validated.distribution is None):
            return validated.ref
        config = {}
        if validated.ref is not None:
            config['ref'] = validated.ref
        if validated.release is not None:
            config['release'] = validated.release
        if validated.artifact_id is not None:
            config['artifact_id'] = validated.artifact_id
        if validated.distribution is not None:
            config['distribution'] = validated.distribution
        return config


def parse_explicit_image_selector(value: str) -> ContainerImage | None:
    """Parses ``ref=``, ``release=``, or ``artifact_id=`` selectors."""
    for field in ('ref', 'release', 'artifact_id'):
        prefix = f'{field}='
        if value.startswith(prefix):
            selector_value = value[len(prefix):]
            return ContainerImage(**{field: selector_value})
    return None


def format_explicit_image_selector(selector: ContainerImage) -> str:
    """Formats a scalar selector with an explicit identity namespace."""
    if selector.distribution is not None:
        raise ValueError('An explicit scalar selector cannot carry a '
                         'distribution override.')
    populated = [(field, getattr(selector, field))
                 for field in ('ref', 'release', 'artifact_id')
                 if getattr(selector, field) is not None]
    if len(populated) != 1:
        raise ValueError('An explicit scalar selector requires exactly one '
                         'identity field.')
    field, selector_value = populated[0]
    return f'{field}={selector_value}'


def validate_operational_image_selector(value: str) -> str:
    """Validates a secret-free selector without guessing its namespace."""
    explicit = parse_explicit_image_selector(value)
    if explicit is not None:
        return format_explicit_image_selector(explicit)
    if (not value or len(value) > _MAX_REFERENCE_LENGTH or
            any(character.isspace() for character in value)):
        raise ValueError('Image selectors must be non-empty, contain no '
                         'whitespace, and be at most 1024 characters.')
    if (any(character in value for character in ('?', '#', '%', '\\')) or
            '://' in value):
        raise ValueError('Image selectors must not contain URL syntax, inline '
                         'userinfo, query parameters, fragments, or encoded '
                         'secrets.')
    if '@' in value:
        try:
            # Release labels and artifact IDs cannot contain '@'. Therefore an
            # operational selector containing it must be a complete OCI
            # digest reference, including the repository/userinfo checks.
            return validate_oci_reference(value, 'Image selector')
        except ValueError:
            raise ValueError(
                'Image selectors must not contain URL syntax, inline '
                'userinfo, query parameters, fragments, or encoded secrets.'
            ) from None
    valid_namespace = False
    for validator, subject in (
        (validate_catalog_id, 'Image artifact selector'),
        (validate_release_label, 'Image release selector'),
        (validate_oci_reference, 'Image source selector'),
    ):
        try:
            validator(value, subject)
            valid_namespace = True
        except ValueError:
            pass
    if not valid_namespace:
        raise ValueError('Image selectors must be a valid artifact UUID, '
                         'release label, or OCI image reference.')
    # Preserve the identity namespace. The catalog resolver checks bare
    # artifact, release, and source candidates together and rejects ambiguity.
    return value


@dataclasses.dataclass(frozen=True)
class RegistryLocality:
    """One compute placement for which a registry target is local."""

    provider: str
    region: str

    def __post_init__(self) -> None:
        provider = (self.provider.lower()
                    if isinstance(self.provider, str) else self.provider)
        object.__setattr__(
            self, 'provider',
            validate_control_plane_identifier(provider,
                                              'Registry locality provider'))
        if (not isinstance(self.region, str) or
                not _REGISTRY_LOCALITY_REGION_PATTERN.fullmatch(self.region)):
            raise ValueError('Registry locality region must be a bounded, '
                             'secret-free cloud region or context name.')

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> 'RegistryLocality':
        if not isinstance(config, dict) or set(config) != {
                'provider', 'region'
        }:
            raise ValueError('Registry locality requires only provider and '
                             'region.')
        return cls(provider=config['provider'], region=config['region'])


@dataclasses.dataclass(frozen=True)
class RegistryTarget:
    """An administrator-configured registry endpoint.

    Manager identity and pull-auth values are strategy names or identity
    references.  They must never contain tokens or passwords.
    """

    name: str
    provider: str
    region: str
    account: str | None = None
    project: str | None = None
    registry: str | None = None
    manager_identity: str | None = None
    pull_auth: str | None = None
    localities: tuple[RegistryLocality, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'name',
            validate_control_plane_identifier(self.name,
                                              'Registry target name'))
        provider = (self.provider.strip().lower() if isinstance(
            self.provider, str) else self.provider)
        provider = validate_control_plane_identifier(
            provider, 'Registry target provider')
        region = normalize_registry_region(self.region,
                                           'Registry target region', provider)
        object.__setattr__(self, 'provider', provider)
        object.__setattr__(self, 'region', region)
        for field, subject in (
            ('account', 'Registry target account'),
            ('project', 'Registry target project'),
            ('manager_identity', 'Registry target manager identity'),
        ):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(
                    self, field,
                    validate_control_plane_identifier(value, subject))
        localities = tuple(self.localities)
        if any(not isinstance(locality, RegistryLocality)
               for locality in localities):
            raise ValueError('Registry target localities must contain '
                             'RegistryLocality values.')
        locality_keys = [(item.provider, item.region) for item in localities]
        if len(locality_keys) != len(set(locality_keys)):
            raise ValueError('Registry target localities must be unique.')
        object.__setattr__(self, 'localities', localities)
        if (self.pull_auth is not None and
                self.pull_auth not in _TARGET_PULL_AUTH_STRATEGIES):
            raise ValueError(
                'Registry target uses an unsupported pull-auth strategy.')
        if self.registry is not None:
            object.__setattr__(
                self, 'registry',
                normalize_registry_prefix(self.registry, self.name))

    @property
    def registry_prefix(self) -> str | None:
        """Returns one canonical physical registry and repository prefix."""
        if self.registry is not None:
            return self.registry
        if self.provider == 'aws' and self.account:
            return normalize_registry_prefix(
                aws_ecr_registry_authority(self.account, self.region),
                self.name)
        if self.provider == 'gcp' and self.project:
            return normalize_registry_prefix(
                f'{self.region}-docker.pkg.dev/{self.project}', self.name)
        return None

    @property
    def endpoint_identity(self) -> tuple[str, ...]:
        """Returns the physical registry endpoint identity, excluding aliases."""
        registry_prefix = self.registry_prefix
        if registry_prefix is not None:
            return ('registry', registry_prefix)
        return ('provider', self.provider, self.region, self.account or
                '', self.project or '')

    @property
    def adapter_identity(self) -> tuple[str, ...]:
        """Returns the complete provider and authority interpretation."""
        return (self.provider, self.region, self.account or '', self.project or
                '', self.registry or '', self.manager_identity or
                '', self.pull_auth or
                '', *(f'{item.provider}:{item.region}'
                      for item in sorted(self.localities,
                                         key=lambda item:
                                         (item.provider, item.region))))

    def is_local_to(self, provider: str, region: str) -> bool:
        """Returns whether this target is declared local to a placement."""
        if self.localities:
            return any(
                item.provider == provider.lower() and item.region == region
                for item in self.localities)
        return self.provider == provider.lower() and self.region == region

    @property
    def fingerprint(self) -> str:
        """Returns physical identity, excluding mutable auth configuration."""
        serialized = json.dumps(self.endpoint_identity,
                                sort_keys=True,
                                separators=(',', ':')).encode()
        return hashlib.sha256(serialized).hexdigest()

    @classmethod
    def from_config(cls, name: str, config: dict[str, Any]) -> 'RegistryTarget':
        return cls(name=name,
                   provider=str(config['provider']).lower(),
                   region=str(config['region']),
                   account=config.get('account'),
                   project=config.get('project'),
                   registry=config.get('registry'),
                   manager_identity=config.get('manager_identity'),
                   pull_auth=config.get('pull_auth'),
                   localities=tuple(
                       RegistryLocality.from_config(item)
                       for item in config.get('localities', ())))


@dataclasses.dataclass(frozen=True)
class RegistryProfile:
    """A complete, non-mergeable registry distribution profile."""

    name: str
    ownership: RegistryOwnership
    realm: str
    namespace: str
    require_digest_at_runtime: bool
    canonical: RegistryTarget
    revision: int
    organization: str | None = None
    targets: tuple[RegistryTarget, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'name',
            validate_control_plane_identifier(self.name,
                                              'Registry profile name'))
        if (not isinstance(self.revision, int) or
                isinstance(self.revision, bool) or self.revision <= 0):
            raise ValueError('Registry profile revision must be a positive '
                             'integer.')
        normalized_namespace = normalize_registry_namespace_template(
            self.namespace, self.realm, self.organization, self.name)
        if (self.ownership == RegistryOwnership.MANAGED and
                '{workspace}' not in normalized_namespace):
            raise ValueError(
                f'Managed registry profile {self.name!r} must include the '
                '{workspace} placeholder in namespace.')
        object.__setattr__(self, 'namespace', normalized_namespace)
        if len(self.targets) >= _MAX_REGISTRY_PROFILE_TARGETS:
            raise ValueError(
                f'Registry profile {self.name!r} may configure at most '
                f'{_MAX_REGISTRY_PROFILE_TARGETS - 1} regional targets.')

    @property
    def fingerprint(self) -> str:
        """Returns distribution identity without mutable auth configuration."""
        payload = {
            'ownership': self.ownership.value,
            'realm': self.realm,
            'namespace': self.namespace,
            'organization': self.organization,
            'canonical_endpoint': self.canonical.endpoint_identity,
        }
        serialized = json.dumps(payload, sort_keys=True,
                                separators=(',', ':')).encode()
        return hashlib.sha256(serialized).hexdigest()

    def physical_fingerprint(self, target: RegistryTarget) -> str:
        """Returns stable identity for content in one physical namespace.

        Only values that can change the rendered destination repository belong
        here.  Policy and authority changes are versioned separately so they
        can reuse already-verified bytes without creating a second row for the
        same OCI manifest reference.
        """
        payload: dict[str, Any] = {
            'namespace': self.namespace,
            'endpoint': target.endpoint_identity,
        }
        if '{realm}' in self.namespace:
            payload['realm'] = self.realm
        if '{organization}' in self.namespace:
            payload['organization'] = self.organization
        serialized = json.dumps(payload, sort_keys=True,
                                separators=(',', ':')).encode()
        return hashlib.sha256(serialized).hexdigest()

    def materialization_fingerprint(self, target: RegistryTarget) -> str:
        """Compatibility alias for the physical destination fingerprint."""
        return self.physical_fingerprint(target)

    def policy_fingerprint(self, target: RegistryTarget,
                           canonical: bool) -> str:
        """Returns the current authority and runtime-policy revision."""
        payload = {
            'profile': self.name,
            'target': target.name,
            'physical': self.physical_fingerprint(target),
            'ownership': self.ownership.value,
            'canonical': canonical,
            'realm': self.realm,
            'namespace': self.namespace,
            'organization': self.organization,
            'manager_identity': target.manager_identity,
            'pull_auth': target.pull_auth,
            'adapter_identity': target.adapter_identity,
            'require_digest_at_runtime': self.require_digest_at_runtime,
        }
        serialized = json.dumps(payload, sort_keys=True,
                                separators=(',', ':')).encode()
        return hashlib.sha256(serialized).hexdigest()

    @property
    def revision_fingerprint(self) -> str:
        """Returns the complete config bound to one monotonic revision."""
        targets: list[dict[str, Any]] = []
        configured_targets = ((True, self.canonical),) + tuple(
            (False, target) for target in self.targets)
        for canonical, target in configured_targets:
            targets.append({
                'canonical': canonical,
                'name': target.name,
                'physical': self.physical_fingerprint(target),
                'policy': self.policy_fingerprint(target, canonical),
                'adapter': target.adapter_identity,
            })
        payload = {
            'name': self.name,
            'ownership': self.ownership.value,
            'realm': self.realm,
            'namespace': self.namespace,
            'organization': self.organization,
            'require_digest_at_runtime': self.require_digest_at_runtime,
            'targets': sorted(targets, key=lambda item: str(item['name'])),
        }
        serialized = json.dumps(payload, sort_keys=True,
                                separators=(',', ':')).encode()
        return hashlib.sha256(serialized).hexdigest()

    def target(self, target_id: str) -> RegistryTarget:
        """Returns a configured target by stable target ID."""
        target_id = validate_control_plane_identifier(target_id,
                                                      'Registry target name')
        if target_id == self.canonical.name:
            return self.canonical
        for target in self.targets:
            if target.name == target_id:
                return target
        raise ValueError(
            f'Unknown target {target_id!r} in registry profile {self.name!r}.')


@dataclasses.dataclass(frozen=True)
class WorkspaceImagePolicy:
    """Effective workspace policy for container images."""

    mode: WorkspaceImageMode = WorkspaceImageMode.MANAGED_PREFERRED
    default_profile: str | None = None
    allowed_profiles: tuple[str, ...] = ()
    locality: Locality = Locality.PREFER
    regional_cache_retention_weeks: int | None = 8
    max_artifacts: int = 1_000_000
    max_sources_per_artifact: int = 128
    max_releases_per_artifact: int = 128

    def __post_init__(self) -> None:
        if self.default_profile is not None:
            validate_control_plane_identifier(self.default_profile,
                                              'Workspace default distribution')
        for profile in self.allowed_profiles:
            validate_control_plane_identifier(profile,
                                              'Workspace allowed distribution')
        for value, subject, maximum in (
            (self.max_artifacts, 'Workspace container image artifact quota',
             _MAX_WORKSPACE_ARTIFACTS),
            (self.max_sources_per_artifact,
             'Workspace container image source quota', _MAX_ARTIFACT_ALIASES),
            (self.max_releases_per_artifact,
             'Workspace container image release quota', _MAX_ARTIFACT_ALIASES),
        ):
            if (not isinstance(value, int) or isinstance(value, bool) or
                    value <= 0 or value > maximum):
                raise ValueError(
                    f'{subject} must be a positive integer no greater than '
                    f'{maximum}.')


@dataclasses.dataclass(frozen=True)
class Placement:
    """Concrete runtime placement consumed by the pure resolver."""

    provider: str
    region: str
    backend: str
    registry_provider: str | None = None
    registry_region: str | None = None
    registry_prefix: str | None = None
    registry_auth_strategy: str | None = None
    platform: str | None = None

    def __post_init__(self) -> None:
        registry_provider = self.registry_provider
        if registry_provider is not None:
            registry_provider = (registry_provider.strip().lower()
                                 if isinstance(registry_provider, str) else
                                 registry_provider)
            registry_provider = validate_control_plane_identifier(
                registry_provider, 'Placement registry provider')
            object.__setattr__(self, 'registry_provider', registry_provider)
        if self.registry_region is not None:
            object.__setattr__(
                self, 'registry_region',
                normalize_registry_region(self.registry_region,
                                          'Placement registry region',
                                          registry_provider))
        if self.platform is not None:
            object.__setattr__(
                self, 'platform',
                validate_oci_platform(self.platform, 'Runtime platform'))

    @property
    def locality_provider(self) -> str:
        return self.registry_provider or self.provider

    @property
    def locality_region(self) -> str:
        return self.registry_region or self.region


@dataclasses.dataclass(frozen=True)
class ImageRoute:
    """Read-only readiness snapshot for one digest-pinned pull route."""

    image_id: str
    location_id: str
    target_id: str
    distribution: str
    profile_revision: int
    policy_fingerprint: str
    provider: str
    region: str
    reference: str
    digest: str
    auth_strategy: str | None
    state: ImageLocationState
    platforms: tuple[str, ...] = ()
    canonical: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'image_id',
            validate_catalog_id(self.image_id, 'Image route artifact ID'))
        object.__setattr__(
            self, 'location_id',
            validate_catalog_id(self.location_id, 'Image route location ID'))
        object.__setattr__(
            self, 'target_id',
            validate_control_plane_identifier(self.target_id,
                                              'Image route target'))
        object.__setattr__(
            self, 'distribution',
            validate_control_plane_identifier(self.distribution,
                                              'Image route distribution'))
        if (not isinstance(self.profile_revision, int) or
                isinstance(self.profile_revision, bool) or
                self.profile_revision <= 0):
            raise ValueError('Image route profile_revision must be a positive '
                             'integer.')
        if not _FINGERPRINT_PATTERN.fullmatch(self.policy_fingerprint):
            raise ValueError('Image route policy_fingerprint must be a '
                             'lowercase SHA-256 hex digest.')
        reference = validate_oci_reference(self.reference,
                                           'Image route reference')
        object.__setattr__(self, 'reference', reference)
        digest = validate_sha256_digest(self.digest, 'Image route digest')
        object.__setattr__(self, 'digest', digest)
        _, reference_digest = split_digest(reference)
        if reference_digest != digest:
            raise ValueError('Image route reference digest does not match its '
                             'expected digest.')
        if (self.auth_strategy is not None and
                not _RUNTIME_AUTH_STRATEGY_PATTERN.fullmatch(
                    self.auth_strategy)):
            raise ValueError('Image route has an invalid runtime pull-auth '
                             'strategy name.')
        if self.auth_strategy == 'source_config':
            raise ValueError('A managed image route cannot use source_config '
                             'runtime pull authority.')
        object.__setattr__(
            self, 'platforms',
            validate_oci_platforms(self.platforms, 'Image route platforms'))


@dataclasses.dataclass(frozen=True)
class ResolvedContainerImage:
    """Secret-free pull plan pinned into an in-flight launch."""

    image_id: str
    reference: str
    target_id: str
    digest: str
    auth_strategy: str
    location_id: str | None = None
    distribution: str | None = None
    profile_revision: int | None = None
    policy_fingerprint: str | None = None
    status: str = 'READY'
    fallback_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'image_id',
            validate_catalog_id(self.image_id,
                                'Resolved container image artifact ID'))
        object.__setattr__(
            self, 'target_id',
            validate_control_plane_identifier(
                self.target_id, 'Resolved container image target'))
        reference = validate_oci_reference(
            self.reference, 'Resolved container image reference')
        object.__setattr__(self, 'reference', reference)
        digest = validate_sha256_digest(self.digest,
                                        'Resolved container image digest')
        object.__setattr__(self, 'digest', digest)
        _, reference_digest = split_digest(reference)
        if reference_digest != digest:
            raise ValueError('Resolved container image reference must be '
                             'pinned to its expected digest.')
        if not _RUNTIME_AUTH_STRATEGY_PATTERN.fullmatch(self.auth_strategy):
            raise ValueError('Resolved container image requires a valid '
                             'runtime pull-auth strategy name.')
        policy_snapshot = (self.distribution, self.profile_revision,
                           self.policy_fingerprint)
        if self.location_id is None:
            if self.target_id != 'source' or self.status != 'WARMING':
                raise ValueError('A resolved image without a managed location '
                                 'must be a WARMING source fallback.')
            if any(value is not None for value in policy_snapshot):
                raise ValueError('A source fallback must not carry a managed '
                                 'distribution policy snapshot.')
            if self.auth_strategy not in {
                    'source_config', 'ecr_runtime_identity',
                    'gar_runtime_identity'
            } and not self.auth_strategy.startswith('kubernetes_context:'):
                raise ValueError('A source fallback must use a supported '
                                 'source runtime pull authority.')
        else:
            object.__setattr__(
                self, 'location_id',
                validate_catalog_id(self.location_id,
                                    'Resolved container image location ID'))
            if any(value is None for value in policy_snapshot):
                raise ValueError('A managed resolved container image requires '
                                 'distribution, profile_revision, and '
                                 'policy_fingerprint.')
            assert self.distribution is not None
            assert self.profile_revision is not None
            assert self.policy_fingerprint is not None
            object.__setattr__(
                self, 'distribution',
                validate_control_plane_identifier(
                    self.distribution, 'Resolved container image distribution'))
            if (not isinstance(self.profile_revision, int) or
                    isinstance(self.profile_revision, bool) or
                    self.profile_revision <= 0):
                raise ValueError('Resolved container image profile_revision '
                                 'must be a positive integer.')
            if not _FINGERPRINT_PATTERN.fullmatch(self.policy_fingerprint):
                raise ValueError('Resolved container image '
                                 'policy_fingerprint must be a lowercase '
                                 'SHA-256 hex digest.')
            if self.auth_strategy == 'source_config':
                raise ValueError('A managed resolved container image cannot '
                                 'use source_config runtime pull authority.')
        if self.status not in {'READY', 'WARMING'}:
            raise ValueError('Resolved container image status must be READY '
                             'or WARMING.')
        if (self.fallback_reason is not None and self.fallback_reason
                not in {reason.value for reason in ImageFallbackReason}):
            raise ValueError('Resolved container image fallback_reason must '
                             'be a supported secret-free reason code.')
        if ((self.status == 'WARMING') != (self.fallback_reason is not None)):
            raise ValueError('Resolved container image WARMING status '
                             'requires a fallback reason, and READY status '
                             'must not include one.')

    def to_dict(self) -> dict[str, str | int | None]:
        """Returns a secret-free API/serialization representation."""
        return dataclasses.asdict(type(self).from_dict(self))

    @classmethod
    def from_dict(
        cls,
        value: 'ResolvedContainerImage | dict[str, Any]',
    ) -> 'ResolvedContainerImage':
        """Parses the strict internal launch-state representation."""
        if isinstance(value, cls):
            value = {
                field.name: getattr(value, field.name)
                for field in dataclasses.fields(cls)
            }
        if not isinstance(value, dict):
            raise ValueError('_resolved_container_image must be an object.')
        required = {
            'image_id', 'reference', 'target_id', 'digest', 'auth_strategy'
        }
        optional = {
            'location_id', 'distribution', 'profile_revision',
            'policy_fingerprint', 'status', 'fallback_reason'
        }
        unknown = set(value) - required - optional
        missing = required - set(value)
        if unknown or missing:
            raise ValueError(
                '_resolved_container_image requires exactly image_id, '
                'reference, target_id, digest, and auth_strategy, with '
                'optional managed policy snapshot, status, and '
                'fallback_reason. The provided field set is invalid.')
        if not all(isinstance(value[key], str) for key in required):
            raise ValueError(
                '_resolved_container_image required fields must be strings.')
        string_optional = {
            'location_id', 'distribution', 'policy_fingerprint', 'status',
            'fallback_reason'
        }
        for key in string_optional:
            if key in value and value[key] is not None and not isinstance(
                    value[key], str):
                raise ValueError(f'_resolved_container_image.{key} must be '
                                 'a string or null.')
        profile_revision = value.get('profile_revision')
        if (profile_revision is not None and
            (not isinstance(profile_revision, int) or
             isinstance(profile_revision, bool))):
            raise ValueError('_resolved_container_image.profile_revision must '
                             'be an integer or null.')
        return cls(**value)
