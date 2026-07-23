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
_MAX_SIGNED_64_BIT_INTEGER = (1 << 63) - 1
_RUNTIME_AUTH_STRATEGY_PATTERN = re.compile(
    r'^(?:anonymous|source_config|ecr_runtime_identity|gar_runtime_identity|'
    r'kubernetes_context:node_identity)$')
_OCI_RUNTIME_ARCHITECTURES = {
    'amd64': 'amd64',
    'x86_64': 'amd64',
    'arm64': 'arm64',
    'aarch64': 'arm64',
}
_KNOWN_LINUX_RUNTIME_PLATFORMS = frozenset({'linux/amd64', 'linux/arm64'})
V0_MANAGED_RUNTIME_PLATFORM = 'linux/amd64'
KUBERNETES_ARCH_LABEL = 'kubernetes.io/arch'
_AWS_REGISTRY_REGION_PATTERN = re.compile(
    r'^[a-z]{2}(?:-[a-z0-9]+){1,3}-[0-9]+$')
_GCP_STYLE_REGISTRY_REGION_PATTERN = re.compile(
    r'^(?:us|europe|asia|[a-z][a-z0-9]*(?:-[a-z0-9]+)*[0-9])$')
_KUBERNETES_LABEL_NAME_PATTERN = re.compile(
    r'^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$')
_KUBERNETES_LABEL_PREFIX_PATTERN = re.compile(
    r'^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$')


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
    """Returns whether OCI metadata is not known incompatible with a runtime."""
    validated = validate_oci_platforms(platforms, 'Artifact platforms')
    if runtime_platform is None:
        # Unknown placement architecture is not proof of a mismatch. Requiring
        # every architecture here would force users to publish unused variants
        # merely because a cloud catalog or Kubernetes context cannot report
        # the eventual node architecture. Accept one generic Linux platform;
        # exact known architectures are still fenced below. CPU-feature-specific
        # variants remain incompatible without exact runtime proof.
        return any(
            '/'.join(platform.split('/')[:2]) in _KNOWN_LINUX_RUNTIME_PLATFORMS
            and _is_safe_generic_platform(platform) for platform in validated)
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


class RegistryAccessBindingKind(enum.Enum):
    """V0 credential resolvers with independently qualified purposes."""

    AWS_ASSUME_ROLE = 'aws_assume_role'
    AWS_EC2_INSTANCE_IDENTITY = 'aws_ec2_instance_identity'
    AWS_EKS_KUBELET_IDENTITY = 'aws_eks_kubelet_identity'
    KUBERNETES_DOCKERCONFIG_SECRET = 'kubernetes_dockerconfig_secret'


@dataclasses.dataclass(frozen=True)
class QualifiedKubernetesCluster:
    """One immutable EKS cluster and node-pool qualification boundary."""

    context: str
    cluster_arn: str
    node_role: str
    namespace: str
    node_selector: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        validate_control_plane_identifier(self.context,
                                          'Qualified Kubernetes context')
        if (not self.cluster_arn.startswith('arn:') or
                not self.node_role.startswith('arn:') or
                not isinstance(self.namespace, str) or not self.namespace or
                len(self.namespace) > 253 or
                any(character.isspace() for character in self.namespace)):
            raise ValueError('Qualified EKS cluster identity is invalid.')
        selector = tuple(self.node_selector)
        if (not selector or len(selector) > 16 or
                len({key for key, _ in selector}) != len(selector)):
            raise ValueError('Qualified EKS node selector must contain 1 to '
                             '16 unique labels.')
        normalized: list[tuple[str, str]] = []
        for key, value in selector:
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError('Qualified EKS node selector is invalid.')
            prefix, separator, name = key.rpartition('/')
            if not separator:
                name = key
                prefix = ''
            if (not _KUBERNETES_LABEL_NAME_PATTERN.fullmatch(name) or
                (prefix and
                 (len(prefix) > 253 or
                  not _KUBERNETES_LABEL_PREFIX_PATTERN.fullmatch(prefix))) or
                    not _KUBERNETES_LABEL_NAME_PATTERN.fullmatch(value)):
                raise ValueError('Qualified EKS node selector is invalid.')
            normalized.append((key, value))
        if dict(normalized).get(KUBERNETES_ARCH_LABEL) != 'amd64':
            raise ValueError('Qualified EKS node selector must require '
                             'kubernetes.io/arch=amd64.')
        object.__setattr__(self, 'node_selector', tuple(sorted(normalized)))


def eks_cluster_region(cluster_arn: str) -> str | None:
    """Returns the exact AWS region from a structurally valid EKS ARN."""
    if not isinstance(cluster_arn, str):
        return None
    arn = cluster_arn.split(':', 5)
    if (len(arn) != 6 or arn[0] != 'arn' or arn[2] != 'eks' or not arn[3] or
            not arn[4] or not arn[5].startswith('cluster/') or
            len(arn[5]) == len('cluster/')):
        return None
    return arn[3]


_ACCESS_PURPOSES = frozenset({
    'source_read',
    'destination_write',
    'verify',
    'runtime_pull',
    'lifecycle_delete',
    'canary_launch',
})


@dataclasses.dataclass(frozen=True)
class RegistryAccessBinding:
    """Secret-free authority or resolver reference used for one fixed role."""

    id: str
    kind: RegistryAccessBindingKind
    purposes: tuple[str, ...]
    authority: str | None = None
    external_id: str | None = None
    reference: dict[str, str] | None = None
    principals: tuple[str, ...] = ()
    credential_helper: str | None = None
    qualified_node_images: tuple[tuple[str, str], ...] = ()
    instance_profile: str | None = None
    canary_authority: str | None = None
    canary_instance_type: str | None = None
    canary_subnets: tuple[tuple[str, tuple[str, ...]], ...] = ()
    canary_security_groups: tuple[tuple[str, tuple[str, ...]], ...] = ()
    qualified_clusters: tuple[QualifiedKubernetesCluster, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'id',
            validate_control_plane_identifier(self.id,
                                              'Registry access binding'))
        purposes = tuple(self.purposes)
        if (not purposes or len(purposes) != len(set(purposes)) or
                not set(purposes) <= _ACCESS_PURPOSES):
            raise ValueError('Registry access binding purposes are invalid.')
        object.__setattr__(self, 'purposes', purposes)
        if self.kind == RegistryAccessBindingKind.AWS_ASSUME_ROLE:
            if (not isinstance(self.authority, str) or
                    not self.authority.startswith('arn:')):
                raise ValueError('AWS assume-role binding requires a role ARN.')
        elif self.authority is not None or self.external_id is not None:
            raise ValueError('Only AWS assume-role bindings accept authority '
                             'or external_id.')
        if self.kind == RegistryAccessBindingKind.KUBERNETES_DOCKERCONFIG_SECRET:
            if (self.reference is None or
                    set(self.reference) != {'namespace', 'name', 'key'}):
                raise ValueError('Docker config binding requires namespace, '
                                 'name, and key references.')
            if set(purposes) != {'source_read'}:
                raise ValueError('Docker config bindings are source-read only.')
        elif self.reference is not None:
            raise ValueError('Only Docker config bindings accept a reference.')
        if self.kind == RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY:
            if (set(purposes) != {'runtime_pull'} or
                    len(self.principals) != 1 or
                    self.credential_helper != 'amazon-ecr-credential-helper' or
                    not self.qualified_node_images or
                    self.instance_profile is None or
                    self.canary_authority is None or
                    self.canary_instance_type is None):
                raise ValueError(
                    'EC2 runtime binding requires one principal, '
                    'an instance profile, the ECR helper, regional '
                    'AMIs, and a canary launch authority.')
            principal_arn = self.principals[0].split(':', 5)
            if (len(principal_arn) != 6 or principal_arn[0] != 'arn' or
                    principal_arn[2] != 'iam' or principal_arn[3] or
                    re.fullmatch(r'[0-9]{12}', principal_arn[4]) is None or
                    not principal_arn[5].startswith('role/') or
                    len(principal_arn[5]) == len('role/')):
                raise ValueError('Qualified EC2 principal ARN is invalid.')
            validate_control_plane_identifier(self.instance_profile,
                                              'Qualified EC2 instance profile')
            validate_control_plane_identifier(self.canary_authority,
                                              'EC2 canary authority')
            validate_control_plane_identifier(self.canary_instance_type,
                                              'EC2 canary instance type')
            node_regions = {region for region, _ in self.qualified_node_images}
            subnet_regions = {region for region, _ in self.canary_subnets}
            security_group_regions = {
                region for region, _ in self.canary_security_groups
            }
            if (len(node_regions) != len(self.qualified_node_images) or
                    len(subnet_regions) != len(self.canary_subnets) or
                    len(security_group_regions) != len(
                        self.canary_security_groups) or
                    subnet_regions != node_regions or
                    security_group_regions != node_regions):
                raise ValueError('EC2 canary network regions must match the '
                                 'qualified regional AMIs.')
            for region, ami in self.qualified_node_images:
                normalize_registry_region(region, 'Qualified EC2 region', 'aws')
                if (not isinstance(ami, str) or not ami.startswith('ami-') or
                        len(ami) > 128):
                    raise ValueError('Qualified EC2 AMI is invalid.')
            for _, subnets in self.canary_subnets:
                if (not subnets or len(subnets) > 32 or
                        len(subnets) != len(set(subnets)) or
                        any(not subnet.startswith('subnet-') or
                            len(subnet) > 128 for subnet in subnets)):
                    raise ValueError('EC2 canary subnets are invalid.')
            for _, security_groups in self.canary_security_groups:
                if (not security_groups or len(security_groups) > 32 or
                        len(security_groups) != len(set(security_groups)) or
                        any(not group.startswith('sg-') or len(group) > 128
                            for group in security_groups)):
                    raise ValueError('EC2 canary security groups are invalid.')
        if self.kind == RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY:
            if (set(purposes) != {'runtime_pull'} or
                    not self.qualified_clusters or
                    self.canary_authority is None):
                raise ValueError('EKS runtime binding requires qualified '
                                 'cluster tuples and a canary authority.')
            validate_control_plane_identifier(self.canary_authority,
                                              'EKS canary authority')
            contexts: set[str] = set()
            for cluster in self.qualified_clusters:
                if (not isinstance(cluster, QualifiedKubernetesCluster) or
                        cluster.context in contexts):
                    raise ValueError(
                        'Qualified EKS cluster tuples are invalid.')
                contexts.add(cluster.context)

    @property
    def fingerprint(self) -> str:
        payload = dataclasses.asdict(self)
        payload['kind'] = self.kind.value
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True,
                       separators=(',', ':')).encode()).hexdigest()


def ec2_instance_profile_arn(binding: RegistryAccessBinding) -> str:
    """Returns the exact instance-profile ARN for a qualified EC2 binding."""
    if (binding.kind != RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY or
            binding.instance_profile is None or len(binding.principals) != 1):
        raise ValueError('Registry runtime binding is not an EC2 binding.')
    principal_arn = binding.principals[0].split(':', 5)
    if len(principal_arn) != 6:
        raise ValueError('Qualified EC2 principal ARN is invalid.')
    return (':'.join(principal_arn[:5]) +
            f':instance-profile/{binding.instance_profile}')


@dataclasses.dataclass(frozen=True)
class ManagedRegistryLimits:
    """Per-artifact hard limits enforced before provider I/O."""
    max_artifact_bytes: int
    max_releases_per_artifact: int
    max_regional_locations_per_artifact: int

    def __post_init__(self) -> None:
        for value, subject in (
            (self.max_artifact_bytes, 'max_artifact_bytes'),
            (self.max_releases_per_artifact, 'max_releases_per_artifact'),
            (self.max_regional_locations_per_artifact,
             'max_regional_locations_per_artifact'),
        ):
            if not isinstance(value, int) or isinstance(value,
                                                        bool) or value <= 0:
                raise ValueError(
                    f'Registry profile {subject} must be positive.')


@dataclasses.dataclass(frozen=True)
class RegistryQualificationPolicy:
    """Freshness, cost, and fixed canary contract for one profile."""
    runtime_attestation_max_age_seconds: int
    automatic_canaries: bool
    max_daily_canary_cost_usd: float
    canary_worst_case_cost_usd: float
    canary_timeout_seconds: int
    canary_ref: str
    canary_platform: str

    def __post_init__(self) -> None:
        if (not isinstance(self.runtime_attestation_max_age_seconds, int) or
                self.runtime_attestation_max_age_seconds <= 0 or
                not isinstance(self.automatic_canaries, bool) or
                not isinstance(self.max_daily_canary_cost_usd, (int, float)) or
                isinstance(self.max_daily_canary_cost_usd, bool) or
                self.max_daily_canary_cost_usd < 0 or
                not isinstance(self.canary_worst_case_cost_usd, (int, float)) or
                isinstance(self.canary_worst_case_cost_usd, bool) or
                self.canary_worst_case_cost_usd <= 0 or
                self.canary_worst_case_cost_usd > self.max_daily_canary_cost_usd
                or not isinstance(self.canary_timeout_seconds, int) or
                isinstance(self.canary_timeout_seconds, bool) or
                not 60 <= self.canary_timeout_seconds <= 3600):
            raise ValueError('Registry qualification policy is invalid.')
        reference = validate_oci_reference(self.canary_ref,
                                           'Qualification canary reference')
        if split_digest(reference)[1] is None:
            raise ValueError('Qualification canary reference must be '
                             'digest-pinned.')
        object.__setattr__(self, 'canary_ref', reference)
        platform = validate_oci_platform(self.canary_platform,
                                         'Qualification canary platform')
        if platform != V0_MANAGED_RUNTIME_PLATFORM:
            raise ValueError('Managed image canaries support linux/amd64 only.')
        object.__setattr__(self, 'canary_platform', platform)

    @property
    def max_daily_canary_microusd(self) -> int:
        return int(self.max_daily_canary_cost_usd * 1_000_000)

    @property
    def canary_worst_case_microusd(self) -> int:
        return int(self.canary_worst_case_cost_usd * 1_000_000)


@dataclasses.dataclass(frozen=True)
class ManagedRegistryTarget:
    """One pre-created fixed ECR shard ring and runtime-pull binding."""

    name: str
    region: str
    registry: str
    repository_prefix: str
    shard_count: int
    max_manifests_per_shard: int
    max_declared_bytes_per_shard: int
    max_in_flight: int
    write_authority: str
    delete_authority: str | None
    qualification_delete_authority: str
    runtime_pull: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'name',
            validate_control_plane_identifier(self.name,
                                              'Registry target name'))
        object.__setattr__(
            self, 'region',
            normalize_registry_region(self.region, 'Registry target region',
                                      'aws'))
        object.__setattr__(
            self, 'registry',
            normalize_registry_authority(self.registry, self.name))
        object.__setattr__(
            self, 'repository_prefix',
            validate_registry_repository_path(self.repository_prefix,
                                              'Registry repository prefix'))
        if not 1 <= self.shard_count <= 256:
            raise ValueError(
                'Registry target shard_count must be 1 through 256.')
        for value, subject in (
            (self.max_manifests_per_shard, 'max_manifests_per_shard'),
            (self.max_declared_bytes_per_shard, 'max_declared_bytes_per_shard'),
            (self.max_in_flight, 'max_in_flight'),
        ):
            if not isinstance(value, int) or isinstance(value,
                                                        bool) or value <= 0:
                raise ValueError(f'Registry target {subject} must be positive.')
        runtime_pull = tuple(self.runtime_pull)
        if (not runtime_pull or len(runtime_pull) != len(set(runtime_pull)) or
                any(backend not in ('aws_vm', 'aws_eks')
                    for backend, _ in runtime_pull)):
            raise ValueError('Registry target runtime_pull is invalid.')
        object.__setattr__(self, 'runtime_pull', runtime_pull)
        object.__setattr__(
            self, 'qualification_delete_authority',
            validate_control_plane_identifier(
                self.qualification_delete_authority,
                'Qualification delete authority'))

    def runtime_binding(self, backend: str) -> str | None:
        return dict(self.runtime_pull).get(backend)

    @property
    def target_fingerprint(self) -> str:
        """Identifies the immutable v0 repository ring, not one shard."""
        payload = {
            'provider': 'aws',
            'registry': self.registry,
            'repository_prefix': self.repository_prefix,
            'shard_generation': 0,
            'shard_count': self.shard_count,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True,
                       separators=(',', ':')).encode()).hexdigest()


@dataclasses.dataclass(frozen=True)
class ManagedRegistryProfile:
    """Complete immutable AWS managed distribution revision."""

    name: str
    revision: int
    partition: str
    registry_account: str
    realm: str
    limits: ManagedRegistryLimits
    qualification: RegistryQualificationPolicy
    canonical: ManagedRegistryTarget
    targets: tuple[ManagedRegistryTarget, ...]
    access_bindings: tuple[RegistryAccessBinding, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'name',
            validate_control_plane_identifier(self.name,
                                              'Registry profile name'))
        if not isinstance(self.revision, int) or self.revision <= 0:
            raise ValueError('Registry profile revision must be positive.')
        if self.partition not in ('aws', 'aws-us-gov', 'aws-cn'):
            raise ValueError(
                'Registry profile uses an unsupported AWS partition.')
        if not re.fullmatch(r'[0-9]{12}', self.registry_account):
            raise ValueError('Registry profile account must be 12 digits.')
        validate_control_plane_identifier(self.realm, 'Registry profile realm')
        targets = (self.canonical,) + tuple(self.targets)
        names = [target.name for target in targets]
        regions = [target.region for target in targets]
        if len(names) != len(set(names)) or len(regions) != len(set(regions)):
            raise ValueError(
                'Registry target names and regions must be unique.')
        bindings = {binding.id: binding for binding in self.access_bindings}
        if len(bindings) != len(self.access_bindings):
            raise ValueError('Registry access binding IDs must be unique.')
        for target in targets:
            expected = aws_ecr_registry_authority(self.registry_account,
                                                  target.region)
            if target.registry != expected:
                raise ValueError('Registry target authority must match the '
                                 'dedicated account and region.')
            write = bindings.get(target.write_authority)
            if (write is None or 'destination_write' not in write.purposes or
                    'verify' not in write.purposes):
                raise ValueError('Registry target write binding is incomplete.')
            if (target is self.canonical and
                    'source_read' not in write.purposes):
                raise ValueError('Canonical registry write binding must also '
                                 'permit regional source reads.')
            if target.delete_authority is not None:
                delete = bindings.get(target.delete_authority)
                if (delete is None or
                        'lifecycle_delete' not in delete.purposes or
                        'verify' not in delete.purposes):
                    raise ValueError(
                        'Registry target delete binding is invalid.')
            qualification_delete = bindings.get(
                target.qualification_delete_authority)
            if (qualification_delete is None or
                    'lifecycle_delete' not in qualification_delete.purposes or
                    'verify' not in qualification_delete.purposes):
                raise ValueError('Registry target qualification delete binding '
                                 'is invalid.')
            for backend, binding_id in target.runtime_pull:
                runtime = bindings.get(binding_id)
                expected_kind = (
                    RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY
                    if backend == 'aws_vm' else
                    RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY)
                if runtime is None or runtime.kind != expected_kind:
                    raise ValueError(
                        'Registry runtime binding kind is invalid.')
                if (backend == 'aws_vm' and target.region not in dict(
                        runtime.qualified_node_images)):
                    raise ValueError('Registry EC2 runtime binding has no '
                                     'qualified tuple for the target region.')
                if (backend == 'aws_eks' and not any(
                        eks_cluster_region(cluster.cluster_arn) == target.region
                        for cluster in runtime.qualified_clusters)):
                    raise ValueError('Registry EKS runtime binding has no '
                                     'qualified cluster in the target region.')
                canary = (bindings.get(runtime.canary_authority)
                          if runtime.canary_authority is not None else None)
                if (canary is None or canary.kind
                        != RegistryAccessBindingKind.AWS_ASSUME_ROLE or
                        'canary_launch' not in canary.purposes):
                    raise ValueError('Registry runtime canary authority is '
                                     'invalid.')

    @property
    def bindings(self) -> dict[str, RegistryAccessBinding]:
        return {binding.id: binding for binding in self.access_bindings}

    def target(self, name: str) -> ManagedRegistryTarget:
        for target in (self.canonical,) + self.targets:
            if target.name == name:
                return target
        raise ValueError(f'Unknown registry target {name!r}.')

    def to_snapshot(self) -> dict[str, Any]:
        """Returns the complete bounded, secret-free immutable worker config."""

        def normalize(value: Any) -> Any:
            if isinstance(value, enum.Enum):
                return value.value
            if dataclasses.is_dataclass(value):
                return {
                    field.name: normalize(getattr(value, field.name))
                    for field in dataclasses.fields(value)
                }
            if isinstance(value, tuple):
                return [normalize(item) for item in value]
            if isinstance(value, dict):
                return {str(key): normalize(item) for key, item in value.items()}
            return value

        snapshot = normalize(self)
        assert isinstance(snapshot, dict)
        return snapshot

    @classmethod
    def from_snapshot(cls, value: dict[str, Any]) -> 'ManagedRegistryProfile':
        """Revalidates a persisted profile snapshot at every worker boundary."""
        expected = {
            'name', 'revision', 'partition', 'registry_account', 'realm',
            'limits', 'qualification', 'canonical', 'targets', 'access_bindings'
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError('Managed registry profile snapshot is invalid.')

        def target(raw: dict[str, Any]) -> ManagedRegistryTarget:
            payload = dict(raw)
            payload['runtime_pull'] = tuple(
                tuple(item) for item in payload['runtime_pull'])
            return ManagedRegistryTarget(**payload)

        def binding(raw: dict[str, Any]) -> RegistryAccessBinding:
            payload = dict(raw)
            payload['kind'] = RegistryAccessBindingKind(payload['kind'])
            for field in ('purposes', 'principals', 'qualified_node_images'):
                payload[field] = tuple(
                    tuple(item) if isinstance(item, list) else item
                    for item in payload[field])
            for field in ('canary_subnets', 'canary_security_groups'):
                payload[field] = tuple(
                    (item[0], tuple(item[1])) for item in payload[field])
            payload['qualified_clusters'] = tuple(
                QualifiedKubernetesCluster(context=item['context'],
                                           cluster_arn=item['cluster_arn'],
                                           node_role=item['node_role'],
                                           namespace=item['namespace'],
                                           node_selector=tuple(
                                               tuple(pair)
                                               for pair in
                                               item['node_selector']))
                for item in payload['qualified_clusters'])
            return RegistryAccessBinding(**payload)

        return cls(
            name=value['name'],
            revision=value['revision'],
            partition=value['partition'],
            registry_account=value['registry_account'],
            realm=value['realm'],
            limits=ManagedRegistryLimits(**value['limits']),
            qualification=RegistryQualificationPolicy(**value['qualification']),
            canonical=target(value['canonical']),
            targets=tuple(target(item) for item in value['targets']),
            access_bindings=tuple(
                binding(item) for item in value['access_bindings']),
        )

    @property
    def config_hash(self) -> str:
        payload = self.to_snapshot()
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True,
                       separators=(',', ':')).encode()).hexdigest()

    @property
    def physical_manifest_hash(self) -> str:
        payload = [{
            'name': target.name,
            'region': target.region,
            'registry': target.registry,
            'repository_prefix': target.repository_prefix,
            'shard_count': target.shard_count,
        } for target in (self.canonical,) + self.targets]
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True,
                       separators=(',', ':')).encode()).hexdigest()


def qualified_eks_cluster_for_target(
    target: ManagedRegistryTarget,
    binding: RegistryAccessBinding,
    context: str,
    *,
    cluster_arn: str | None = None,
    node_role: str | None = None,
) -> QualifiedKubernetesCluster:
    """Resolves one EKS runtime tuple by context and target AWS region."""
    if binding.kind != RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY:
        raise ValueError('Registry runtime binding is not an EKS binding.')
    matches: list[QualifiedKubernetesCluster] = []
    for candidate in binding.qualified_clusters:
        if (eks_cluster_region(candidate.cluster_arn) != target.region or
                candidate.context != context or
                cluster_arn not in (None, candidate.cluster_arn) or
                node_role not in (None, candidate.node_role)):
            continue
        matches.append(candidate)
    if len(matches) != 1:
        raise ValueError(
            'EKS runtime context does not identify one target-region tuple.')
    return matches[0]


def runtime_attestation_matches(
    profile: ManagedRegistryProfile,
    target: ManagedRegistryTarget,
    binding: RegistryAccessBinding,
    backend: str,
    runtime_id: str,
    evidence: Any,
    *,
    as_of: int | None,
    qualified_cluster: QualifiedKubernetesCluster | None = None,
) -> bool:
    """Checks one exact runtime proof, optionally at an authoritative time."""
    if not isinstance(evidence, dict):
        return False
    observed_at = evidence.get('observed_at')
    if (not isinstance(observed_at, int) or isinstance(observed_at, bool) or
            evidence.get('status') != 'READY' or
            evidence.get('platform') != V0_MANAGED_RUNTIME_PLATFORM or
            evidence.get('target_fingerprint') != target.target_fingerprint or
            evidence.get('binding_fingerprint') != binding.fingerprint or
            evidence.get('backend') != backend or
            evidence.get('runtime_id') != runtime_id):
        return False
    if (as_of is not None and not 0 <= as_of - observed_at <=
            profile.qualification.runtime_attestation_max_age_seconds):
        return False
    if backend == 'aws_vm':
        if (binding.kind != RegistryAccessBindingKind.AWS_EC2_INSTANCE_IDENTITY
                or binding.instance_profile is None or
                len(binding.principals) != 1):
            return False
        try:
            expected_profile_arn = ec2_instance_profile_arn(binding)
        except ValueError:
            return False
        return (evidence.get('host_image_id') == dict(
            binding.qualified_node_images).get(target.region) and
                evidence.get('instance_architecture') == 'x86_64' and
                evidence.get('instance_profile_arn') == expected_profile_arn and
                evidence.get('actual_principal') == binding.principals[0])
    if backend != 'aws_eks' or binding.kind != (
            RegistryAccessBindingKind.AWS_EKS_KUBELET_IDENTITY):
        return False
    if qualified_cluster is None:
        try:
            qualified_cluster = qualified_eks_cluster_for_target(
                target, binding, runtime_id)
        except ValueError:
            return False
    return (evidence.get('context') == qualified_cluster.context and
            evidence.get('cluster_arn') == qualified_cluster.cluster_arn and
            evidence.get('node_role') == qualified_cluster.node_role and
            evidence.get('node_selector') == dict(
                qualified_cluster.node_selector) and
            isinstance(evidence.get('qualified_node_count'), int) and
            not isinstance(evidence.get('qualified_node_count'), bool) and
            evidence['qualified_node_count'] > 0 and
            isinstance(evidence.get('qualified_node_set_hash'), str) and
            bool(evidence['qualified_node_set_hash']))


class WorkspaceImageMode(enum.Enum):
    """How strongly a workspace requires the managed image catalog."""

    DIRECT = 'direct'
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
    VERIFYING = 'VERIFYING'
    READY = 'READY'
    FAILED = 'FAILED'
    MISSING = 'MISSING'
    EVICTING = 'EVICTING'
    EVICTED = 'EVICTED'
    QUARANTINED = 'QUARANTINED'


class ImageLocationErrorCode(enum.Enum):
    """Closed, secret-free diagnostics persisted for image locations."""

    COPY_LEASE_EXPIRED = 'copy_lease_expired'
    VERIFY_LEASE_EXPIRED = 'verify_lease_expired'
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
    PROVIDER_THROTTLED = 'provider_throttled'
    PROVIDER_OUTCOME_AMBIGUOUS = 'provider_outcome_ambiguous'
    REGISTRY_CAPACITY_EXHAUSTED = 'registry_capacity_exhausted'
    SOURCE_CONTENT_UNSUPPORTED = 'source_content_unsupported'
    SOURCE_PLATFORM_AMBIGUOUS = 'source_platform_ambiguous'
    SOURCE_PLATFORM_MISSING = 'source_platform_missing'


class ImagePublicationState(enum.Enum):
    """Closed source inspection and canonical publication states."""

    PENDING = 'PENDING'
    INSPECTING = 'INSPECTING'
    READY = 'READY'
    FAILED = 'FAILED'


class ImageOperationState(enum.Enum):
    """Closed public mutation states."""

    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'


class ImageProfileState(enum.Enum):
    """Closed immutable profile revision states."""

    QUALIFYING = 'QUALIFYING'
    ACTIVE = 'ACTIVE'
    FAILED = 'FAILED'
    SUPERSEDED = 'SUPERSEDED'
    RETIRED = 'RETIRED'


class ImageShardState(enum.Enum):
    """Admission state of a pre-provisioned physical repository shard."""

    PENDING = 'PENDING'
    READY = 'READY'
    FULL = 'FULL'
    DRIFTED = 'DRIFTED'
    DISABLED = 'DISABLED'


class ImageDemandState(enum.Enum):
    """Durable image readiness state for one logical consumer generation."""

    WARMING = 'WARMING'
    READY = 'READY'
    FAILED = 'FAILED'
    SUPERSEDED = 'SUPERSEDED'
    RELEASED = 'RELEASED'


class ImageWorkerKind(enum.Enum):
    """Independently permissioned worker services."""

    COPY = 'COPY'
    LIFECYCLE = 'LIFECYCLE'
    CANARY = 'CANARY'


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


def profile_attestation_key(capability: str, *identity: str) -> str:
    """Returns a stable bounded key for one independently attested tuple."""
    capability = validate_control_plane_identifier(
        capability, 'Profile attestation capability')
    if not identity:
        return capability
    if any(not isinstance(item, str) or not item for item in identity):
        raise ValueError('Profile attestation identity is invalid.')
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=False,
                   separators=(',', ':')).encode()).hexdigest()
    return f'{capability}:{digest}'


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


@dataclasses.dataclass(frozen=True)
class ContainerImage:
    """A user-facing immutable image selector and distribution override.

    A task may prove a published release with its digest-pinned source. It never
    creates a release. ``artifact_id`` is the machine-oriented selector and is
    exclusive with the other identity fields.

    ``distribution='direct'`` is the explicit escape hatch for a workspace
    using ``managed_preferred``. It is rejected by ``managed_required`` policy.
    """

    ref: str | None = None
    release: str | None = None
    artifact_id: str | None = None
    distribution: str | None = None
    _legacy_direct: bool = dataclasses.field(default=False,
                                             init=False,
                                             repr=False,
                                             compare=False)

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
            if split_digest(ref)[1] is None and not self._legacy_direct:
                raise ValueError(
                    'container_image.ref must be pinned by SHA-256 digest.')
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
        if self.distribution == 'direct' and (self.ref is None or
                                              self.release is not None or
                                              self.artifact_id is not None):
            raise ValueError(
                'container_image distribution direct requires only a '
                'digest-pinned ref.')

    @property
    def digest(self) -> str | None:
        """Returns the pinned digest, or None for a catalog-only selector."""
        if self.ref is None:
            return None
        return split_digest(self.ref)[1]

    @classmethod
    def from_legacy_ref(cls, ref: str) -> 'ContainerImage':
        """Builds only the unchanged private ``image_id: docker:`` path."""
        instance = cls.__new__(cls)
        object.__setattr__(instance, 'ref', ref)
        object.__setattr__(instance, 'release', None)
        object.__setattr__(instance, 'artifact_id', None)
        object.__setattr__(instance, 'distribution', None)
        object.__setattr__(instance, '_legacy_direct', True)
        instance.__post_init__()
        return instance

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
            if value._legacy_direct:  # pylint: disable=protected-access
                raise ValueError('Legacy Docker image identity is not valid '
                                 'container_image input.')
            return cls(ref=value.ref,
                       release=value.release,
                       artifact_id=value.artifact_id,
                       distribution=value.distribution)
        if isinstance(value, str):
            return cls(ref=value)
        if not isinstance(value, dict):
            raise ValueError('container_image must be a string or an object '
                             'with ref, release, or artifact_id.')
        unknown = set(value) - {'ref', 'release', 'artifact_id', 'distribution'}
        if unknown:
            raise ValueError('container_image contains unsupported fields.')
        ref = value.get('ref')
        if ref is not None and not isinstance(ref, str):
            raise ValueError('container_image.ref must be a string.')
        artifact_id = value.get('artifact_id')
        if artifact_id is not None and not isinstance(artifact_id, str):
            raise ValueError('container_image.artifact_id must be a string.')
        distribution = value.get('distribution')
        if distribution is not None and not isinstance(distribution, str):
            raise ValueError('container_image.distribution must be a string.')
        release = value.get('release')
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
class WorkspaceImagePolicy:
    """Effective workspace policy for container images."""

    mode: WorkspaceImageMode = WorkspaceImageMode.DIRECT
    default_profile: str | None = None
    allowed_profiles: tuple[str, ...] = ()
    publishers: tuple[str, ...] = ()
    locality: Locality = Locality.PREFER
    regional_cache_retention_weeks: int | None = 8

    def __post_init__(self) -> None:
        if not isinstance(self.mode, WorkspaceImageMode):
            raise ValueError('Workspace image mode must be a supported mode.')
        if (self.default_profile is not None and
                not isinstance(self.default_profile, str)):
            raise ValueError(
                'Workspace default distribution must be a string or null.')
        if self.default_profile is not None:
            validate_control_plane_identifier(self.default_profile,
                                              'Workspace default distribution')
        allowed_profiles = self.allowed_profiles
        if (not isinstance(allowed_profiles, (list, tuple)) or
                len(allowed_profiles) > 128 or not all(
                    isinstance(profile, str) for profile in allowed_profiles)):
            raise ValueError(
                'Workspace allowed distributions must be a list of at most '
                '128 strings.')
        normalized_profiles = tuple(allowed_profiles)
        if len(set(normalized_profiles)) != len(normalized_profiles):
            raise ValueError('Workspace allowed distributions must be unique.')
        for profile in normalized_profiles:
            validate_control_plane_identifier(profile,
                                              'Workspace allowed distribution')
        publishers = self.publishers
        if (not isinstance(publishers,
                           (list, tuple)) or len(publishers) > 256 or
                not all(isinstance(publisher, str)
                        for publisher in publishers)):
            raise ValueError('Workspace image publishers must be a list of at '
                             'most 256 strings.')
        normalized_publishers = tuple(publishers)
        if len(set(normalized_publishers)) != len(normalized_publishers):
            raise ValueError(
                'Workspace image publishers must be unique and at most 256.')
        for publisher in normalized_publishers:
            if (not publisher or len(publisher) > 256 or
                    any(character.isspace() for character in publisher)):
                raise ValueError('Workspace image publishers must be bounded '
                                 'stable user IDs without whitespace.')
        if not isinstance(self.locality, Locality):
            raise ValueError('Workspace image locality must be supported.')
        retention_weeks = self.regional_cache_retention_weeks
        if (retention_weeks is not None and
            (not isinstance(retention_weeks, int) or
             isinstance(retention_weeks, bool) or retention_weeks <= 0)):
            raise ValueError('Workspace regional cache retention must be a '
                             'positive integer or null.')
        object.__setattr__(self, 'allowed_profiles', normalized_profiles)
        object.__setattr__(self, 'publishers', normalized_publishers)


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
    host_image_id: str | None = None
    runtime_principal: str | None = None
    kubernetes_cluster_arn: str | None = None
    kubernetes_node_role: str | None = None

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
        for value, subject in (
            (self.host_image_id, 'Placement host image'),
            (self.runtime_principal, 'Placement runtime principal'),
            (self.kubernetes_cluster_arn, 'Placement Kubernetes cluster'),
            (self.kubernetes_node_role, 'Placement Kubernetes node role'),
        ):
            if (value is not None and
                (not isinstance(value, str) or not value or len(value) > 2048 or
                 any(char.isspace() for char in value))):
                raise ValueError(f'{subject} must be a bounded identity.')

    @property
    def locality_provider(self) -> str:
        return self.registry_provider or self.provider

    @property
    def locality_region(self) -> str:
        return self.registry_region or self.region


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
    profile_revision_id: str | None = None
    target_fingerprint: str | None = None
    demand_id: str | None = None
    demand_generation: int | None = None
    controller_epoch: str | None = None
    owner_epoch: int | None = None
    credential_helper: str | None = None
    runtime_principal: str | None = None
    instance_profile: str | None = None
    kubernetes_node_selector: tuple[tuple[str, str], ...] = ()
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
                           self.policy_fingerprint, self.profile_revision_id,
                           self.target_fingerprint)
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
            assert self.profile_revision_id is not None
            assert self.target_fingerprint is not None
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
            object.__setattr__(
                self, 'profile_revision_id',
                validate_catalog_id(self.profile_revision_id,
                                    'Resolved image profile revision ID'))
            object.__setattr__(
                self, 'target_fingerprint',
                validate_fingerprint(self.target_fingerprint,
                                     'Resolved image target fingerprint'))
            if self.auth_strategy == 'source_config':
                raise ValueError('A managed resolved container image cannot '
                                 'use source_config runtime pull authority.')
        demand_values = (self.demand_id, self.demand_generation,
                         self.controller_epoch, self.owner_epoch)
        if any(value is not None for value in demand_values):
            if any(value is None for value in demand_values):
                raise ValueError('Resolved image demand fields are atomic.')
            assert self.demand_id is not None
            assert self.demand_generation is not None
            assert self.controller_epoch is not None
            assert self.owner_epoch is not None
            object.__setattr__(
                self, 'demand_id',
                validate_catalog_id(self.demand_id, 'Resolved image demand ID'))
            if (not isinstance(self.demand_generation, int) or
                    isinstance(self.demand_generation, bool) or
                    self.demand_generation < 0):
                raise ValueError('Resolved image demand generation is invalid.')
            if (not isinstance(self.controller_epoch, str) or
                    not self.controller_epoch or
                    len(self.controller_epoch) > 1024 or
                    any(character.isspace()
                        for character in self.controller_epoch)):
                raise ValueError('Resolved image controller epoch is invalid.')
            if (not isinstance(self.owner_epoch, int) or
                    isinstance(self.owner_epoch, bool) or self.owner_epoch < 0):
                raise ValueError('Resolved image owner epoch is invalid.')
        if self.credential_helper not in (None, 'ecr-login'):
            raise ValueError('Resolved image credential helper is invalid.')
        for value, subject in ((self.runtime_principal,
                                'Resolved runtime principal'),
                               (self.instance_profile,
                                'Resolved instance profile')):
            if (value is not None and
                (not isinstance(value, str) or not value or len(value) > 2048 or
                 any(character.isspace() for character in value))):
                raise ValueError(f'{subject} is invalid.')
        if ((self.instance_profile is not None) != (self.runtime_principal
                                                    is not None)):
            raise ValueError('Resolved EC2 runtime identity fields are atomic.')
        if self.kubernetes_node_selector:
            if self.location_id is None or self.instance_profile is not None:
                raise ValueError('Resolved Kubernetes node selector is valid '
                                 'only for a managed Kubernetes pull plan.')
            qualified = QualifiedKubernetesCluster(
                context='validation',
                cluster_arn='arn:validation',
                node_role='arn:validation',
                namespace='validation',
                node_selector=self.kubernetes_node_selector)
            object.__setattr__(self, 'kubernetes_node_selector',
                               qualified.node_selector)
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

    def to_dict(self) -> dict[str, Any]:
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
            'policy_fingerprint', 'profile_revision_id', 'target_fingerprint',
            'demand_id', 'demand_generation', 'controller_epoch', 'owner_epoch',
            'credential_helper', 'runtime_principal', 'instance_profile',
            'kubernetes_node_selector', 'status', 'fallback_reason'
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
            'fallback_reason', 'profile_revision_id', 'target_fingerprint',
            'demand_id', 'controller_epoch', 'credential_helper'
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
        demand_generation = value.get('demand_generation')
        if (demand_generation is not None and
            (not isinstance(demand_generation, int) or
             isinstance(demand_generation, bool))):
            raise ValueError('_resolved_container_image.demand_generation '
                             'must be an integer or null.')
        owner_epoch = value.get('owner_epoch')
        if (owner_epoch is not None and (not isinstance(owner_epoch, int) or
                                         isinstance(owner_epoch, bool))):
            raise ValueError('_resolved_container_image.owner_epoch must be an '
                             'integer or null.')
        node_selector = value.get('kubernetes_node_selector', ())
        if (not isinstance(node_selector, (list, tuple)) or
                any(not isinstance(item, (list, tuple)) or len(item) != 2
                    for item in node_selector)):
            raise ValueError('_resolved_container_image.'
                             'kubernetes_node_selector must be a label-pair '
                             'list.')
        value = dict(value)
        value['kubernetes_node_selector'] = tuple(
            (str(item[0]), str(item[1])) for item in node_selector)
        return cls(**value)
