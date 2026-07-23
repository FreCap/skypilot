"""AWS ECR data-plane adapter for the managed v0 slice.

This module is imported by copy and lifecycle workers, never by placement or
API request handlers. Credentials stay inside the worker process and are not
accepted or returned by any public model.
"""

from __future__ import annotations

import base64
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
import dataclasses
import enum
import functools
import hashlib
import json
import threading
import time
from typing import Any, cast

from sqlalchemy import orm

from sky.adaptors import aws as aws_adaptor
from sky.container_images import catalog_state
from sky.container_images import config as image_config
from sky.container_images import models
from sky.container_images import oci
from sky.container_images import providers
from sky.container_images import topology_state

_UPLOAD_PART_BYTES = 20 * 1024 * 1024
_AWS_CONNECT_TIMEOUT_SECONDS = 10
_AWS_READ_TIMEOUT_SECONDS = 60
_AWS_TOTAL_MAX_ATTEMPTS = 1
_ECR_ACCEPTED_MANIFEST_TYPES = [
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json',
]
_AMBIGUOUS_ERROR_CODES = frozenset({
    'InternalServerError',
    'InternalFailure',
    'RequestTimeout',
    'RequestTimeoutException',
    'ServiceUnavailable',
})
_THROTTLE_ERROR_CODES = frozenset({
    'LimitExceededException',
    'Throttling',
    'ThrottlingException',
    'TooManyRequestsException',
})
_INVALID_INVENTORY_CURSOR_CODES = frozenset({
    'InvalidParameterException',
    'ValidationException',
})
_DELETE_NO_MUTATION_ERROR_CODES = _THROTTLE_ERROR_CODES | frozenset({
    'AccessDenied',
    'AccessDeniedException',
    'ExpiredToken',
    'ExpiredTokenException',
    'IncompleteSignature',
    'InvalidClientTokenId',
    'InvalidParameterException',
    'InvalidSignatureException',
    'MissingAuthenticationToken',
    'RepositoryNotFoundException',
    'UnauthorizedException',
    'UnrecognizedClientException',
    'ValidationException',
})


class ProviderThrottledError(RuntimeError):
    """AWS asked every worker sharing this budget to back off."""


class AmbiguousProviderOutcomeError(RuntimeError):
    """A write may have committed and requires exact destination readback."""


class DestinationContentMismatchError(RuntimeError):
    """The destination digest resolves to unexpected bytes or descriptors."""


class CopyOutcome(enum.Enum):
    PRESENT = 'PRESENT'
    WRITTEN = 'WRITTEN'
    AMBIGUOUS = 'AMBIGUOUS'


class DeleteOutcome(enum.Enum):
    """Exact deletion result, including proof that provider I/O never began."""

    ABSENT = 'ABSENT'
    PRESENT = 'PRESENT'
    AMBIGUOUS = 'AMBIGUOUS'
    NOT_STARTED = 'NOT_STARTED'
    READBACK_RETRY = 'READBACK_RETRY'


class DeleteRequestOutcome(enum.Enum):
    """Provider conclusion for the destructive request, before readback."""

    CONCLUDED = 'CONCLUDED'
    AMBIGUOUS = 'AMBIGUOUS'
    NOT_STARTED = 'NOT_STARTED'


@dataclasses.dataclass(frozen=True)
class EcrCallHooks:
    before_call: Callable[[], None]
    on_throttle: Callable[[], None]


@dataclasses.dataclass(frozen=True)
class QualifiedShard:
    """One Terraform-qualified physical ECR repository shard."""
    workspace: str
    target: str
    partition: str
    account: str
    region: str
    shard_generation: int
    shard_index: int
    registry: str
    repository_name: str
    repository_arn: str
    encryption_type: str
    kms_key_arn: str | None
    tag_immutability: str
    scanning_mode: str
    policy_hash: str
    ownership_tags_hash: str
    max_manifests: int
    max_declared_bytes: int
    max_in_flight: int
    physical_fingerprint: str

    def calculated_fingerprint(self) -> str:
        payload = dataclasses.asdict(self)
        payload.pop('physical_fingerprint')
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True,
                       separators=(',', ':')).encode()).hexdigest()

    @classmethod
    def from_dict(cls, value: Any) -> QualifiedShard:
        """Parses one closed Terraform shard fact without trusting JSON types."""
        fields = {field.name for field in dataclasses.fields(cls)}
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError('Qualification shard has an invalid shape.')

        def bounded_text(name: str, *, nullable: bool = False) -> str | None:
            item = value[name]
            if nullable and item is None:
                return None
            if (not isinstance(item, str) or not item or len(item) > 2048 or
                    any(character.isspace() for character in item)):
                raise ValueError('Qualification shard contains invalid text.')
            return item

        def integer(name: str, *, minimum: int) -> int:
            item = value[name]
            if (type(item) is not int or item < minimum or
                    item > (1 << 63) - 1):
                raise ValueError('Qualification shard contains invalid limits.')
            return item

        workspace = models.validate_workspace_name(value['workspace'],
                                                   'Qualification workspace')
        target = models.validate_control_plane_identifier(
            value['target'], 'Qualification target')
        partition = bounded_text('partition')
        if partition not in ('aws', 'aws-us-gov', 'aws-cn'):
            raise ValueError(
                'Qualification shard has an invalid AWS partition.')
        account = bounded_text('account')
        assert account is not None
        region = models.normalize_registry_region(value['region'],
                                                  'Qualification region', 'aws')
        registry = models.normalize_registry_authority(value['registry'],
                                                       'qualification')
        if registry != models.aws_ecr_registry_authority(account, region):
            raise ValueError('Qualification shard registry is invalid.')
        repository_name = models.validate_registry_repository_path(
            value['repository_name'], 'Qualification repository')
        repository_arn = bounded_text('repository_arn')
        assert repository_arn is not None
        encryption_type = bounded_text('encryption_type')
        kms_key_arn = bounded_text('kms_key_arn', nullable=True)
        if (encryption_type not in ('AES256', 'KMS') or
            (encryption_type == 'KMS') != (kms_key_arn is not None)):
            raise ValueError('Qualification shard encryption is invalid.')
        tag_immutability = bounded_text('tag_immutability')
        scanning_mode = bounded_text('scanning_mode')
        if tag_immutability != 'IMMUTABLE' or scanning_mode not in (
                'SCAN_ON_PUSH', 'MANUAL'):
            raise ValueError('Qualification shard safety settings are invalid.')
        shard = cls(workspace=workspace,
                    target=target,
                    partition=partition,
                    account=account,
                    region=region,
                    shard_generation=integer('shard_generation', minimum=0),
                    shard_index=integer('shard_index', minimum=0),
                    registry=registry,
                    repository_name=repository_name,
                    repository_arn=repository_arn,
                    encryption_type=encryption_type,
                    kms_key_arn=kms_key_arn,
                    tag_immutability=tag_immutability,
                    scanning_mode=scanning_mode,
                    policy_hash=models.validate_fingerprint(
                        value['policy_hash'], 'Shard policy hash'),
                    ownership_tags_hash=models.validate_fingerprint(
                        value['ownership_tags_hash'],
                        'Shard ownership tags hash'),
                    max_manifests=integer('max_manifests', minimum=1),
                    max_declared_bytes=integer('max_declared_bytes', minimum=1),
                    max_in_flight=integer('max_in_flight', minimum=1),
                    physical_fingerprint=models.validate_fingerprint(
                        value['physical_fingerprint'],
                        'Shard physical fingerprint'))
        if shard.calculated_fingerprint() != shard.physical_fingerprint:
            raise ValueError('Qualification shard fingerprint is invalid.')
        return shard


@dataclasses.dataclass(frozen=True)
class TerraformQualificationManifest:
    """Secret-free Terraform handoff for one profile revision."""
    schema_version: int
    catalog_authority: str
    workspace: str
    profile: str
    profile_revision: int
    config_hash: str
    physical_manifest_hash: str
    generated_at: int
    shards: tuple[QualifiedShard, ...]
    role_fingerprints: dict[str, str]
    quota_facts: dict[str, int]

    @classmethod
    def from_json(cls, payload: bytes) -> TerraformQualificationManifest:
        if len(payload) > 4 * 1024 * 1024:
            raise ValueError('Qualification manifest exceeds 4 MiB.')
        try:
            value = json.loads(payload)
        except (TypeError, ValueError):
            raise ValueError(
                'Qualification manifest is not valid JSON.') from None
        required = {
            'schema_version', 'catalog_authority', 'workspace', 'profile',
            'profile_revision', 'config_hash', 'physical_manifest_hash',
            'generated_at', 'shards', 'role_fingerprints', 'quota_facts'
        }
        if not isinstance(value, dict) or set(value) != required:
            raise ValueError('Qualification manifest has an invalid shape.')
        raw_shards = value['shards']
        if not isinstance(raw_shards, list) or not 1 <= len(raw_shards) <= 256:
            raise ValueError('Qualification manifest shards are invalid.')
        shards = [
            QualifiedShard.from_dict(raw_shard) for raw_shard in raw_shards
        ]
        roles = value['role_fingerprints']
        quotas = value['quota_facts']
        if (not isinstance(roles, dict) or not isinstance(quotas, dict) or
                any(not isinstance(key, str) or not key or len(key) > 512 or
                    not isinstance(item, str) or not item or len(item) > 2048 or
                    any(character.isspace()
                        for character in item)
                    for key, item in roles.items()) or
                any(not isinstance(key, str) or not key or len(key) > 512 or
                    type(item) is not int or item < 0 or item > (1 << 63) - 1
                    for key, item in quotas.items())):
            raise ValueError('Qualification role or quota facts are invalid.')
        schema_version = value['schema_version']
        profile_revision = value['profile_revision']
        generated_at = value['generated_at']
        if (type(schema_version) is not int or schema_version != 1 or
                type(profile_revision) is not int or profile_revision <= 0 or
                type(generated_at) is not int or generated_at < 0 or
                generated_at > (1 << 63) - 1):
            raise ValueError('Qualification manifest integers are invalid.')
        catalog_authority = models.validate_catalog_id(
            value['catalog_authority'], 'Qualification catalog authority')
        workspace = models.validate_workspace_name(value['workspace'],
                                                   'Qualification workspace')
        profile = models.validate_control_plane_identifier(
            value['profile'], 'Qualification profile')
        config_hash = models.validate_fingerprint(value['config_hash'],
                                                  'Profile config hash')
        physical_manifest_hash = models.validate_fingerprint(
            value['physical_manifest_hash'], 'Profile physical manifest hash')
        slots = [(shard.target, shard.shard_index) for shard in shards]
        repository_arns = [shard.repository_arn for shard in shards]
        if (len(set(slots)) != len(slots) or
                len(set(repository_arns)) != len(repository_arns)):
            raise ValueError(
                'Qualification manifest contains duplicate shards.')
        return cls(schema_version=schema_version,
                   catalog_authority=catalog_authority,
                   workspace=workspace,
                   profile=profile,
                   profile_revision=profile_revision,
                   config_hash=config_hash,
                   physical_manifest_hash=physical_manifest_hash,
                   generated_at=generated_at,
                   shards=tuple(shards),
                   role_fingerprints={
                       str(key): str(item) for key, item in roles.items()
                   },
                   quota_facts={
                       str(key): int(item) for key, item in quotas.items()
                   })

    @property
    def manifest_hash(self) -> str:
        payload = dataclasses.asdict(self)
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True,
                       separators=(',', ':')).encode()).hexdigest()


def ingest_terraform_qualification(
    payload: bytes,
    *,
    now: int | None = None,
) -> topology_state.ProfileRevisionRecord:
    """Stages exact Terraform facts without claiming live runtime readiness."""
    observed_at = int(time.time()) if now is None else now
    manifest = TerraformQualificationManifest.from_json(payload)
    if manifest.schema_version != 1:
        raise ValueError(
            'Qualification manifest schema version is unsupported.')
    authority = catalog_state.get_catalog_authority_id()
    if manifest.catalog_authority != authority:
        raise ValueError('Qualification manifest belongs to another catalog.')
    profile, policy = image_config.resolve_profile(manifest.profile,
                                                   manifest.workspace)
    if profile is None or policy.mode == models.WorkspaceImageMode.DIRECT:
        raise ValueError(
            'Qualification workspace is not opted into the profile.')
    if (manifest.profile_revision != profile.revision or
            manifest.config_hash != profile.config_hash or
            manifest.physical_manifest_hash != profile.physical_manifest_hash):
        raise ValueError('Qualification manifest does not match configuration.')
    expected_slots = {(target.name, index)
                      for target in (profile.canonical,) + profile.targets
                      for index in range(target.shard_count)}
    actual_slots = {
        (shard.target, shard.shard_index) for shard in manifest.shards
    }
    if actual_slots != expected_slots:
        raise ValueError('Qualification manifest does not cover every shard.')
    budget_scopes = {(shard.partition, shard.account, shard.region)
                     for shard in manifest.shards}
    provider_budgets: list[tuple[str, str, str, int, int]] = []
    regional_quotas: dict[str, tuple[int, int]] = {}
    for partition, account, region in sorted(budget_scopes):
        rate = manifest.quota_facts.get(f'{region}:ecr_api_rate_per_second')
        burst = manifest.quota_facts.get(f'{region}:ecr_api_burst')
        if (not isinstance(rate, int) or rate <= 0 or
                not isinstance(burst, int) or not 1 <= burst <= 64):
            raise ValueError(
                'Qualification manifest has no verified ECR API budget.')
        provider_budgets.append((partition, account, region, rate, burst))
        images = manifest.quota_facts.get(f'{region}:images_per_repository')
        headroom = manifest.quota_facts.get(f'{region}:reserved_headroom')
        if (not isinstance(images, int) or images <= 0 or
                not isinstance(headroom, int) or headroom < 0 or
                headroom >= images):
            raise ValueError(
                'Qualification manifest has no applied repository quota.')
        regional_quotas[region] = (images, headroom)
    for shard in manifest.shards:
        target = profile.target(shard.target)
        expected_prefix = f'{target.repository_prefix}/'
        applied_quota, reserved_headroom = regional_quotas[shard.region]
        if (shard.workspace != manifest.workspace or
                shard.partition != profile.partition or
                shard.account != profile.registry_account or
                shard.region != target.region or
                shard.registry != target.registry or
                shard.shard_generation != 0 or
                not shard.repository_name.startswith(expected_prefix) or
                shard.max_manifests > target.max_manifests_per_shard or
                shard.max_declared_bytes > target.max_declared_bytes_per_shard
                or shard.max_in_flight > target.max_in_flight or
                shard.max_manifests + reserved_headroom > applied_quota):
            raise ValueError('Qualification shard contradicts profile.')
    target_evidence: list[tuple[str, dict[str, Any]]] = []
    for target in (profile.canonical,) + profile.targets:
        prefix = f'{target.region}:'
        facts = {
            key.removeprefix(prefix): value
            for key, value in manifest.role_fingerprints.items()
            if key.startswith(prefix)
        }
        required_facts = {
            'copy_role_arn', 'copy_policy_hash', 'lifecycle_role_arn',
            'lifecycle_policy_hash', 'copy_boundary_policy_hash',
            'lifecycle_boundary_policy_hash', 'qualification_repo_arn'
        }
        if set(facts) != required_facts:
            raise ValueError(
                'Qualification manifest role facts are incomplete.')
        copy_binding = profile.bindings[target.write_authority]
        lifecycle_binding = profile.bindings[
            target.qualification_delete_authority]
        if (facts['copy_role_arn'] != copy_binding.authority or
                facts['lifecycle_role_arn'] != lifecycle_binding.authority):
            raise ValueError('Qualification role facts contradict the profile.')
        repository_arn = facts['qualification_repo_arn']
        arn_parts = repository_arn.split(':', 5)
        if (len(arn_parts) != 6 or arn_parts[0] != 'arn' or
                arn_parts[1] != profile.partition or arn_parts[2] != 'ecr' or
                arn_parts[3] != target.region or
                arn_parts[4] != profile.registry_account or
                not arn_parts[5].startswith('repository/')):
            raise ValueError(
                'Qualification repository ARN contradicts profile.')
        repository_name = arn_parts[5].removeprefix('repository/')
        expected_prefix = f'{target.repository_prefix}/r'
        expected_suffix = f'/qualification/{target.region}'
        if (not repository_name.startswith(expected_prefix) or
                not repository_name.endswith(expected_suffix)):
            raise ValueError('Qualification repository name is invalid.')
        target_evidence.append((target.name, {
            'status': 'READY',
            'observed_at': observed_at,
            'target_fingerprint': target.target_fingerprint,
            'registry': target.registry,
            'repository_name': repository_name,
            'repository_arn': repository_arn,
            'copy_role_arn': facts['copy_role_arn'],
            'copy_policy_hash': facts['copy_policy_hash'],
            'lifecycle_role_arn': facts['lifecycle_role_arn'],
            'lifecycle_policy_hash': facts['lifecycle_policy_hash'],
            'copy_boundary_policy_hash': facts['copy_boundary_policy_hash'],
            'lifecycle_boundary_policy_hash':
                facts['lifecycle_boundary_policy_hash'],
        }))
    desired = topology_state.stage_profile_revision(
        workspace=manifest.workspace,
        profile=profile.name,
        revision=profile.revision,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        physical_manifest_hash=profile.physical_manifest_hash,
        max_daily_canary_microusd=(
            profile.qualification.max_daily_canary_microusd),
        now=now)
    if desired.terraform_hash not in (None, manifest.manifest_hash):
        raise ValueError('Terraform qualification hash is immutable.')
    with orm.Session(catalog_state.engine()) as session, session.begin():
        topology_state.lock_profile_shards(session,
                                           workspace=manifest.workspace,
                                           profile=profile.name)
        state_current = catalog_state.database_epoch(session, now=now)
        for shard in sorted(manifest.shards,
                            key=lambda item: (item.target, item.shard_index)):
            target = profile.target(shard.target)
            topology_state.upsert_qualified_shard(
                session,
                workspace=manifest.workspace,
                profile=profile.name,
                target_id=target.name,
                provider='aws',
                partition=shard.partition,
                account=shard.account,
                region=shard.region,
                shard_generation=shard.shard_generation,
                shard_index=shard.shard_index,
                target_fingerprint=target.target_fingerprint,
                physical_fingerprint=shard.physical_fingerprint,
                registry=shard.registry,
                repository_name=shard.repository_name,
                repository_arn=shard.repository_arn,
                max_manifests=shard.max_manifests,
                max_declared_bytes=shard.max_declared_bytes,
                max_in_flight=shard.max_in_flight,
                now=state_current)
    for partition, account, region, rate, burst in provider_budgets:
        # A missing budget is required to run first-time qualification. An
        # existing operational budget remains unchanged until this revision
        # atomically activates.
        topology_state.ensure_provider_budget(provider='aws',
                                              partition=partition,
                                              account=account,
                                              region=region,
                                              api_family='ecr',
                                              applied_rate_per_second=rate,
                                              burst=burst,
                                              now=now)
    desired = topology_state.record_profile_attestation(
        profile_revision_id=desired.id,
        kind='terraform',
        evidence={
            'status': 'READY',
            'observed_at': observed_at,
            'manifest_hash': manifest.manifest_hash,
            'generated_at': manifest.generated_at,
            'shard_count': len(manifest.shards),
        },
        expected_generation=desired.desired_generation,
        expected_config_hash=profile.config_hash,
        terraform_hash=manifest.manifest_hash,
        now=now)
    for shard in manifest.shards:
        target = profile.target(shard.target)
        applied_quota, reserved_headroom = regional_quotas[shard.region]
        live_key = models.profile_attestation_key('infrastructure_shard',
                                                  shard.physical_fingerprint)
        desired = topology_state.record_profile_attestation(
            profile_revision_id=desired.id,
            kind=models.profile_attestation_key('terraform_shard',
                                                shard.physical_fingerprint),
            evidence={
                'status': 'READY',
                'observed_at': observed_at,
                'physical_fingerprint': shard.physical_fingerprint,
                'target_fingerprint': target.target_fingerprint,
                'target': shard.target,
                'repository_arn': shard.repository_arn,
                'repository_uri': f'{shard.registry}/{shard.repository_name}',
                'tag_mutability': shard.tag_immutability,
                'encryption_type': shard.encryption_type,
                'kms_key': shard.kms_key_arn,
                'scanning_mode': shard.scanning_mode,
                'policy_hash': shard.policy_hash,
                'ownership_tags_hash': shard.ownership_tags_hash,
                'max_manifests': shard.max_manifests,
                'max_declared_bytes': shard.max_declared_bytes,
                'max_in_flight': shard.max_in_flight,
                'terraform_applied_quota': applied_quota,
                'reserved_headroom': reserved_headroom,
                'live_attestation_key': live_key,
            },
            expected_generation=desired.desired_generation,
            expected_config_hash=profile.config_hash,
            now=now)
    for partition, account, region, rate, burst in provider_budgets:
        desired = topology_state.record_profile_attestation(
            profile_revision_id=desired.id,
            kind=models.profile_attestation_key('terraform_budget', 'aws',
                                                partition, account, region,
                                                'ecr'),
            evidence={
                'status': 'READY',
                'observed_at': observed_at,
                'provider': 'aws',
                'partition': partition,
                'account': account,
                'region': region,
                'api_family': 'ecr',
                'applied_rate_per_second': rate,
                'burst': burst,
            },
            expected_generation=desired.desired_generation,
            expected_config_hash=profile.config_hash,
            now=now)
    for target_name, evidence in target_evidence:
        desired = topology_state.record_profile_attestation(
            profile_revision_id=desired.id,
            kind=models.profile_attestation_key('terraform_target',
                                                target_name),
            evidence=evidence,
            expected_generation=desired.desired_generation,
            expected_config_hash=profile.config_hash,
            now=now)
    return desired


@dataclasses.dataclass(frozen=True)
class AwsRoleBinding:
    role_arn: str
    external_id: str | None
    session_name: str
    catalog_tag: str
    profile_tag: str


def _error_code(error: BaseException) -> str | None:
    response = getattr(error, 'response', None)
    if not isinstance(response, dict):
        return None
    payload = response.get('Error')
    if not isinstance(payload, dict):
        return None
    code = payload.get('Code')
    return str(code) if code is not None else None


def _error_code_in_chain(error: BaseException) -> str | None:
    """Finds the provider response hidden by a typed adapter exception."""
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        code = _error_code(current)
        if code is not None:
            return code
        current = current.__cause__
    return None


def _canonical_json_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True,
                   separators=(',', ':')).encode()).hexdigest()


def applied_ecr_images_per_repository_quota(
    binding: AwsRoleBinding,
    region: str,
    *,
    provider_fence: Callable[[], None] | None = None,
) -> int:
    """Reads the customized quota, falling back to the AWS default."""
    client = assumed_client(binding,
                            'service-quotas',
                            region,
                            provider_fence=provider_fence)
    kwargs = {
        'ServiceCode': 'ecr',
        'QuotaCode': 'L-03A36CE1',
    }
    try:
        if provider_fence is not None:
            provider_fence()
        response = client.get_service_quota(**kwargs)
        if provider_fence is not None:
            provider_fence()
    except Exception as error:  # pylint: disable=broad-except
        if _error_code(error) != 'NoSuchResourceException':
            raise
        if provider_fence is not None:
            provider_fence()
        response = client.get_aws_default_service_quota(**kwargs)
        if provider_fence is not None:
            provider_fence()
    value = (response.get('Quota') or {}).get('Value')
    if (not isinstance(value, (int, float)) or isinstance(value, bool) or
            value < 1 or int(value) != value):
        raise ValueError('ECR images-per-repository quota is invalid.')
    return int(value)


def _classify(error: BaseException) -> None:
    code = _error_code(error)
    if code in _THROTTLE_ERROR_CODES:
        raise ProviderThrottledError('AWS ECR provider budget was throttled.') \
            from error
    if code in _AMBIGUOUS_ERROR_CODES or isinstance(
            error, (TimeoutError, ConnectionError)):
        raise AmbiguousProviderOutcomeError(
            'AWS ECR write outcome requires exact readback.') from error
    raise error


def is_invalid_inventory_cursor(error: BaseException) -> bool:
    """Returns whether ECR explicitly rejected a saved pagination token."""
    return _error_code(error) in _INVALID_INVENTORY_CURSOR_CODES


class _HookedEcrClient:
    """Runs one qualified budget hook around each ECR SDK operation."""

    def __init__(self, client: Any, hooks: EcrCallHooks) -> None:
        self._client = client
        self._hooks = hooks
        self.started_calls = 0

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._client, name)
        if not callable(value):
            return value

        def call(*args: Any, **kwargs: Any) -> Any:
            self._hooks.before_call()
            self.started_calls += 1
            try:
                return value(*args, **kwargs)
            except Exception as error:  # pylint: disable=broad-except
                if _error_code(error) in _THROTTLE_ERROR_CODES:
                    self._hooks.on_throttle()
                _classify(error)
                raise AssertionError('unreachable') from error

        return call


class _ProviderFencedEcrClient:
    """Re-proves a durable lease immediately around every ECR SDK call."""

    def __init__(self, client: Any, provider_fence: Callable[[], None]) -> None:
        self._client = client
        self._provider_fence = provider_fence

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._client, name)
        if not callable(value):
            return value

        def call(*args: Any, **kwargs: Any) -> Any:
            self._provider_fence()
            try:
                result = value(*args, **kwargs)
            except Exception:
                self._provider_fence()
                raise
            self._provider_fence()
            return result

        return call


def assumed_client(
    binding: AwsRoleBinding,
    service: str,
    region: str,
    *,
    provider_fence: Callable[[], None] | None = None,
) -> Any:
    """Mints one short-lived role session for a bounded worker adapter."""

    def fenced_provider_call(call: Callable[[], Any]) -> Any:
        if provider_fence is not None:
            provider_fence()
        try:
            result = call()
        except Exception:
            if provider_fence is not None:
                provider_fence()
            raise
        if provider_fence is not None:
            provider_fence()
        return result

    assume_kwargs: dict[str, Any] = {
        'RoleArn': binding.role_arn,
        'RoleSessionName': binding.session_name,
        'DurationSeconds': 3600,
        'Tags': [{
            'Key': 'SkyPilotCatalog',
            'Value': binding.catalog_tag,
        }, {
            'Key': 'SkyPilotProfile',
            'Value': binding.profile_tag,
        }],
    }
    if binding.external_id is not None:
        assume_kwargs['ExternalId'] = binding.external_id
    ambient_session = aws_adaptor.session_with_client_defaults(
        connect_timeout=_AWS_CONNECT_TIMEOUT_SECONDS,
        read_timeout=_AWS_READ_TIMEOUT_SECONDS,
        total_max_attempts=_AWS_TOTAL_MAX_ATTEMPTS,
        profile=aws_adaptor.get_workspace_profile())
    credentials = fenced_provider_call(ambient_session.get_credentials)
    if credentials is None:
        raise aws_adaptor.botocore_exceptions().NoCredentialsError()
    # Force deferred IRSA/profile-role refresh to finish under the bounded
    # botocore-session defaults, then fence again before the explicit STS call.
    frozen = fenced_provider_call(credentials.get_frozen_credentials)
    sts_session = aws_adaptor.boto3.Session(
        aws_access_key_id=frozen.access_key,
        aws_secret_access_key=frozen.secret_key,
        aws_session_token=frozen.token,
        region_name=region)
    sts = cast(Any, sts_session).client(
        'sts',
        region_name=region,
        config=aws_adaptor.botocore.config.Config(
            connect_timeout=_AWS_CONNECT_TIMEOUT_SECONDS,
            read_timeout=_AWS_READ_TIMEOUT_SECONDS,
            retries={'total_max_attempts': _AWS_TOTAL_MAX_ATTEMPTS}))
    response = fenced_provider_call(lambda: sts.assume_role(**assume_kwargs))
    credentials = response['Credentials']
    session = aws_adaptor.boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=region)
    return cast(Any, session).client(
        service,
        region_name=region,
        config=aws_adaptor.botocore.config.Config(
            connect_timeout=_AWS_CONNECT_TIMEOUT_SECONDS,
            read_timeout=_AWS_READ_TIMEOUT_SECONDS,
            retries={'total_max_attempts': _AWS_TOTAL_MAX_ATTEMPTS}))


def _assumed_ecr_client(
    binding: AwsRoleBinding,
    region: str,
    *,
    provider_fence: Callable[[], None] | None = None,
) -> Any:
    return assumed_client(binding, 'ecr', region, provider_fence=provider_fence)


def mint_ecr_source_credentials(
    binding: AwsRoleBinding,
    *,
    region: str,
    account: str,
    expected_authority: str,
    provider_fence: Callable[[], None] | None = None,
) -> providers.SourceCredentials:
    """Mints one in-memory Docker bearer credential for an exact ECR source."""
    client = _assumed_ecr_client(binding, region, provider_fence=provider_fence)
    if provider_fence is not None:
        provider_fence()
    response = client.get_authorization_token(registryIds=[account])
    if provider_fence is not None:
        provider_fence()
    entries = response.get('authorizationData', [])
    if len(entries) != 1:
        raise ValueError('ECR authorization returned an unexpected registry.')
    entry = entries[0]
    endpoint = entry.get('proxyEndpoint')
    token = entry.get('authorizationToken')
    if (not isinstance(endpoint, str) or
            endpoint.removeprefix('https://') != expected_authority or
            not isinstance(token, str)):
        raise ValueError('ECR authorization authority does not match source.')
    try:
        decoded = base64.b64decode(token).decode()
        username, password = decoded.split(':', 1)
    except (ValueError, UnicodeDecodeError):
        raise ValueError('ECR authorization token is malformed.') from None
    return providers.SourceCredentials(username=username, password=password)


class EcrRepository:
    """Exact-manifest ECR operations scoped to one pre-created repository."""

    def __init__(
        self,
        client: Any,
        repository_name: str,
        *,
        provider_fence: Callable[[], None] | None = None,
    ) -> None:
        self._client = client
        self.repository_name = repository_name
        self._provider_fence = provider_fence
        self._download_session: Any | None = None

    def _fence(self) -> None:
        if self._provider_fence is not None:
            self._provider_fence()

    def _fence_download(self, response: Any) -> None:
        try:
            self._fence()
        except BaseException:
            response.close()
            raise

    def _get_download_session(self) -> Any:
        if self._download_session is None:
            self._download_session = providers.guarded_https_session()
        return self._download_session

    @classmethod
    def from_role(
            cls,
            binding: AwsRoleBinding,
            region: str,
            repository_name: str,
            *,
            hooks: EcrCallHooks | None = None,
            provider_fence: Callable[[], None] | None = None) -> EcrRepository:
        client = _assumed_ecr_client(binding,
                                     region,
                                     provider_fence=provider_fence)
        if provider_fence is not None:
            client = _ProviderFencedEcrClient(client, provider_fence)
        if hooks is not None:
            client = _HookedEcrClient(client, hooks)
        return cls(client, repository_name, provider_fence=provider_fence)

    def _batch_get_manifest(self, digest: str) -> tuple[bytes, str] | None:
        digest = models.validate_sha256_digest(digest, 'ECR image digest')
        response = self._client.batch_get_image(
            repositoryName=self.repository_name,
            imageIds=[{
                'imageDigest': digest
            }],
            acceptedMediaTypes=_ECR_ACCEPTED_MANIFEST_TYPES)
        images = response.get('images', [])
        if not images:
            failures = response.get('failures', [])
            if all(
                    failure.get('failureCode') in (
                        'ImageNotFound', 'ImageReferencedByManifestList')
                    for failure in failures):
                return None
            if failures:
                raise RuntimeError('ECR exact image lookup failed.')
            return None
        if len(images) != 1:
            raise DestinationContentMismatchError(
                'ECR exact digest lookup returned multiple manifests.')
        image = images[0]
        manifest = image.get('imageManifest')
        media_type = image.get('imageManifestMediaType')
        returned_digest = image.get('imageId', {}).get('imageDigest')
        if (not isinstance(manifest, str) or
                media_type not in _ECR_ACCEPTED_MANIFEST_TYPES or
                returned_digest != digest):
            raise DestinationContentMismatchError(
                'ECR exact digest response has inconsistent identity.')
        raw = manifest.encode()
        if f'sha256:{hashlib.sha256(raw).hexdigest()}' != digest:
            raise DestinationContentMismatchError(
                'ECR returned manifest bytes with a different digest.')
        return raw, str(media_type)

    def repository_metadata(self) -> dict[str, Any]:
        """Returns bounded live identity and immutable repository settings."""
        response = self._client.describe_repositories(
            repositoryNames=[self.repository_name])
        repositories = response.get('repositories', [])
        if len(repositories) != 1:
            raise ValueError('ECR qualification repository is unavailable.')
        repository = repositories[0]
        if repository.get('repositoryName') != self.repository_name:
            raise ValueError('ECR qualification repository identity changed.')
        encryption = repository.get('encryptionConfiguration') or {}
        scanning = repository.get('imageScanningConfiguration') or {}
        repository_arn = repository.get('repositoryArn')
        if not isinstance(repository_arn, str):
            raise ValueError('ECR repository ARN is unavailable.')
        policy_text = self._client.get_repository_policy(
            repositoryName=self.repository_name).get('policyText')
        if not isinstance(policy_text, str) or len(policy_text) > 1024 * 1024:
            raise ValueError('ECR repository policy is unavailable.')
        try:
            policy = json.loads(policy_text)
        except ValueError:
            raise ValueError('ECR repository policy is invalid.') from None
        tags_response = self._client.list_tags_for_resource(
            resourceArn=repository_arn)
        raw_tags = tags_response.get('tags', [])
        if not isinstance(raw_tags, list) or len(raw_tags) > 256:
            raise ValueError('ECR repository ownership tags are invalid.')
        tags: dict[str, str] = {}
        for item in raw_tags:
            if (not isinstance(item, dict) or
                    not isinstance(item.get('Key'), str) or
                    not isinstance(item.get('Value'), str) or
                    item['Key'] in tags):
                raise ValueError('ECR repository ownership tags are invalid.')
            tags[item['Key']] = item['Value']
        return {
            'repository_arn': repository_arn,
            'repository_uri': repository.get('repositoryUri'),
            'tag_mutability': repository.get('imageTagMutability'),
            'encryption_type': encryption.get('encryptionType'),
            'kms_key': encryption.get('kmsKey'),
            'scanning_mode': ('SCAN_ON_PUSH' if scanning.get('scanOnPush')
                              is True else 'MANUAL'),
            'policy_hash': _canonical_json_hash(policy),
            'ownership_tags_hash': _canonical_json_hash(tags),
        }

    def read_manifest(self, digest: str) -> bytes:
        found = self._batch_get_manifest(digest)
        if found is None:
            raise ValueError('ECR source manifest is missing.')
        return found[0]

    def _download_response(self, digest: str) -> Any:
        self._fence()
        response = self._client.get_download_url_for_layer(
            repositoryName=self.repository_name, layerDigest=digest)
        self._fence()
        url = response.get('downloadUrl')
        if not isinstance(url, str):
            raise ValueError('ECR layer response has no HTTPS download URL.')
        providers.validate_public_https_destination(url,
                                                    'ECR layer download URL')
        self._fence()
        download = self._get_download_session().get(url,
                                                    timeout=60,
                                                    stream=True,
                                                    allow_redirects=False)
        self._fence_download(download)
        if download.status_code in (301, 302, 303, 307, 308):
            download.close()
            raise ValueError('ECR layer download redirects are not allowed.')
        try:
            download.raise_for_status()
        except Exception:
            download.close()
            raise
        self._fence_download(download)
        return download

    def read_blob(self, descriptor: oci.OciDescriptor) -> Iterable[bytes]:

        def chunks() -> Iterator[bytes]:
            download = self._download_response(descriptor.digest)
            try:
                yield from providers.iter_fenced_response_chunks(
                    download,
                    chunk_size=1024 * 1024,
                    provider_fence=self._provider_fence)
            finally:
                download.close()

        return chunks()

    def read_blob_bytes(self, digest: str, *, max_bytes: int) -> bytes:
        descriptor = oci.OciDescriptor(
            media_type='application/vnd.oci.image.config.v1+json',
            digest=models.validate_sha256_digest(digest, 'ECR blob digest'),
            size=max_bytes)
        download = self._download_response(descriptor.digest)
        payload = bytearray()
        try:
            for chunk in providers.iter_fenced_response_chunks(
                    download,
                    chunk_size=1024 * 1024,
                    provider_fence=self._provider_fence):
                payload.extend(chunk)
                if len(payload) > max_bytes:
                    raise ValueError(
                        'ECR source blob exceeds inspection limit.')
        finally:
            download.close()
        return bytes(payload)

    def _layers_present(self, digests: Iterable[str]) -> dict[str, bool]:
        result: dict[str, bool] = {}
        values = list(digests)
        for start in range(0, len(values), 100):
            batch = values[start:start + 100]
            response = self._client.batch_check_layer_availability(
                repositoryName=self.repository_name, layerDigests=batch)
            for layer in response.get('layers', []):
                result[str(layer['layerDigest'])] = (
                    layer.get('layerAvailability') == 'AVAILABLE')
            for failure in response.get('failures', []):
                result[str(failure['layerDigest'])] = False
        return {digest: result.get(digest, False) for digest in values}

    def verify_graph(self, graph: oci.OciContentGraph) -> bool:
        """Proves exact raw manifest, config, platform, and layer presence."""
        found = self._batch_get_manifest(graph.runtime_digest)
        if found is None:
            return False
        raw, media_type = found
        if raw != graph.raw_runtime_manifest or media_type != graph.runtime_media_type:
            raise DestinationContentMismatchError(
                'ECR destination manifest differs from inspected source bytes.')
        payload = json.loads(raw)
        config = payload.get('config')
        layers = payload.get('layers')
        if (not isinstance(config, dict) or
                config.get('digest') != graph.config.digest or
                not isinstance(layers, list) or
            [layer.get('digest') for layer in layers
            ] != [layer.digest for layer in graph.layers]):
            raise DestinationContentMismatchError(
                'ECR destination manifest descriptors changed.')
        presence = self._layers_present(
            [graph.config.digest] + [layer.digest for layer in graph.layers])
        return all(presence.values())

    @staticmethod
    def _verified_chunks(chunks: Iterable[bytes], descriptor: oci.OciDescriptor,
                         cancel_event: threading.Event) -> Iterator[bytes]:
        digest = hashlib.sha256()
        size = 0
        for chunk in chunks:
            if cancel_event.is_set():
                raise RuntimeError('ECR upload cancelled after lease loss.')
            if not isinstance(chunk, bytes):
                raise TypeError('OCI source blob readers must yield bytes.')
            digest.update(chunk)
            size += len(chunk)
            if size > descriptor.size:
                raise ValueError(
                    'OCI source blob exceeds its declared descriptor size.')
            yield chunk
        actual_digest = f'sha256:{digest.hexdigest()}'
        if size != descriptor.size or actual_digest != descriptor.digest:
            raise ValueError('OCI source blob bytes do not match descriptor.')

    def _upload_layer(self, descriptor: oci.OciDescriptor,
                      read_chunks: Callable[[], Iterable[bytes]],
                      cancel_event: threading.Event) -> None:
        if self._layers_present([descriptor.digest])[descriptor.digest]:
            return
        chunks = read_chunks()
        try:
            initiated: dict[str, Any] = {}
            try:
                initiated = self._client.initiate_layer_upload(
                    repositoryName=self.repository_name)
            except Exception as error:  # pylint: disable=broad-except
                _classify(error)
                raise AssertionError('unreachable') from error
            upload_id = initiated['uploadId']
            part = bytearray()
            offset = 0

            def upload(payload: bytes) -> None:
                nonlocal offset
                if not payload:
                    return
                try:
                    self._client.upload_layer_part(
                        repositoryName=self.repository_name,
                        uploadId=upload_id,
                        partFirstByte=offset,
                        partLastByte=offset + len(payload) - 1,
                        layerPartBlob=payload)
                except Exception as error:  # pylint: disable=broad-except
                    try:
                        _classify(error)
                    except AmbiguousProviderOutcomeError:
                        if self._layers_present([descriptor.digest
                                                ])[descriptor.digest]:
                            return
                        raise
                offset += len(payload)

            try:
                for chunk in self._verified_chunks(chunks, descriptor,
                                                   cancel_event):
                    part.extend(chunk)
                    while len(part) >= _UPLOAD_PART_BYTES:
                        payload = bytes(part[:_UPLOAD_PART_BYTES])
                        del part[:_UPLOAD_PART_BYTES]
                        upload(payload)
                upload(bytes(part))
                self._client.complete_layer_upload(
                    repositoryName=self.repository_name,
                    uploadId=upload_id,
                    layerDigests=[descriptor.digest])
            except Exception as error:  # pylint: disable=broad-except
                if self._layers_present([descriptor.digest])[descriptor.digest]:
                    return
                _classify(error)
            if not self._layers_present([descriptor.digest])[descriptor.digest]:
                raise DestinationContentMismatchError(
                    'ECR layer upload completed without exact layer presence.')
        finally:
            close = getattr(chunks, 'close', None)
            if callable(close):
                close()

    def copy_graph(
        self,
        graph: oci.OciContentGraph,
        read_blob: Callable[[oci.OciDescriptor], Iterable[bytes]],
        cancel_event: threading.Event,
    ) -> CopyOutcome:
        """Copies missing exact blobs and submits the unchanged manifest."""
        if self.verify_graph(graph):
            return CopyOutcome.PRESENT
        descriptors = (graph.config,) + graph.layers
        present = self._layers_present(item.digest for item in descriptors)
        for descriptor in descriptors:
            if not present[descriptor.digest]:
                self._upload_layer(descriptor,
                                   functools.partial(read_blob, descriptor),
                                   cancel_event)
        try:
            self._client.put_image(
                repositoryName=self.repository_name,
                imageManifest=graph.raw_runtime_manifest.decode(),
                imageManifestMediaType=graph.runtime_media_type,
                imageDigest=graph.runtime_digest)
            return CopyOutcome.WRITTEN
        except Exception as error:  # pylint: disable=broad-except
            code = _error_code(error)
            if code == 'ImageAlreadyExistsException':
                if self.verify_graph(graph):
                    return CopyOutcome.PRESENT
                raise DestinationContentMismatchError(
                    'ECR reports an existing digest that does not verify.'
                ) from error
            try:
                _classify(error)
            except AmbiguousProviderOutcomeError:
                return CopyOutcome.AMBIGUOUS
            raise AssertionError('unreachable') from error

    def delete_request_outcome(self, digest: str) -> DeleteRequestOutcome:
        """Submits one delete without collapsing later readback failures."""
        digest = models.validate_sha256_digest(digest, 'ECR delete digest')
        calls_before = getattr(self._client, 'started_calls', None)
        try:
            self._client.batch_delete_image(repositoryName=self.repository_name,
                                            imageIds=[{
                                                'imageDigest': digest
                                            }])
        except Exception as error:  # pylint: disable=broad-except
            calls_after = getattr(self._client, 'started_calls', None)
            if (calls_before is not None and calls_after == calls_before):
                return DeleteRequestOutcome.NOT_STARTED
            # A read after a transport failure cannot prove that the timed-out
            # delete will not arrive later. Only an explicit provider rejection
            # known not to mutate may safely proceed to exact readback.
            if (_error_code_in_chain(error)
                    not in _DELETE_NO_MUTATION_ERROR_CODES):
                return DeleteRequestOutcome.AMBIGUOUS
        return DeleteRequestOutcome.CONCLUDED

    def delete_outcome(self, digest: str) -> DeleteOutcome:
        """Deletes one digest and reports request and readback outcomes."""
        request = self.delete_request_outcome(digest)
        if request == DeleteRequestOutcome.NOT_STARTED:
            return DeleteOutcome.NOT_STARTED
        if request == DeleteRequestOutcome.AMBIGUOUS:
            return DeleteOutcome.AMBIGUOUS
        try:
            return (DeleteOutcome.ABSENT if self._batch_get_manifest(digest)
                    is None else DeleteOutcome.PRESENT)
        except Exception:  # pylint: disable=broad-except
            return DeleteOutcome.READBACK_RETRY

    def exact_delete(self, digest: str) -> bool:
        """Deletes one regional digest and proves exact absence."""
        outcome = self.delete_outcome(digest)
        if outcome == DeleteOutcome.ABSENT:
            return True
        if outcome == DeleteOutcome.PRESENT:
            return False
        raise AmbiguousProviderOutcomeError(
            'ECR deletion has no exact final presence proof.')

    def exact_manifest_exists(self, digest: str) -> bool:
        """Returns exact manifest presence without accepting a tag alias."""
        return self._batch_get_manifest(digest) is not None

    def inventory_page(
        self,
        *,
        next_token: str | None = None,
        max_results: int = 1000,
    ) -> tuple[tuple[str, ...], str | None]:
        """Reads one digest-unique, resumable ECR repository inventory page."""
        if not 1 <= max_results <= 1000:
            raise ValueError('ECR inventory page size must be 1 through 1000.')
        kwargs: dict[str, Any] = {
            'repositoryName': self.repository_name,
            'maxResults': max_results,
        }
        if next_token is not None:
            if not isinstance(next_token, str) or not next_token:
                raise ValueError('ECR inventory cursor is invalid.')
            kwargs['nextToken'] = next_token
        response = self._client.describe_images(**kwargs)
        details = response.get('imageDetails', [])
        if not isinstance(details, list) or len(details) > max_results:
            raise RuntimeError('ECR inventory returned an invalid page.')
        digests: set[str] = set()
        for detail in details:
            digest = detail.get('imageDigest') if isinstance(detail,
                                                             dict) else None
            if not isinstance(digest, str):
                raise RuntimeError('ECR inventory omitted an image digest.')
            digests.add(
                models.validate_sha256_digest(digest, 'ECR inventory digest'))
        cursor = response.get('nextToken')
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise RuntimeError('ECR inventory returned an invalid cursor.')
        return tuple(sorted(digests)), cursor
