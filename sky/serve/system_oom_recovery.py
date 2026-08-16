"""Server-owned authorization for SkyServe system-OOM recovery."""

import copy
import dataclasses
import hashlib
import json
import os
import re
import secrets
from typing import Any

from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky import task_yaml as task_yaml_lib
from sky.serve import constants
from sky.skylet import system_oom_recovery as runtime_recovery

logger = sky_logging.init_logger(__name__)

_SHA256_DIGEST_PATTERN = re.compile(r'^sha256:[0-9a-f]{64}$')
_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_AWS_ACCOUNT_ID_PATTERN = re.compile(r'^[0-9]{12}$')
_LAUNCH_NONCE_PATTERN = re.compile(r'^[0-9a-f]{64}$')

AUTHORIZATION_VERSION_V3 = 3
RUNTIME_PROFILE_VERSION_V3 = runtime_recovery.PROFILE_VERSION_OWNED_CONTAINER
RUNTIME_CAPABILITY_V3 = runtime_recovery.CAPABILITY_V2
MAX_HOST_MEMORY_GIB_V3 = 16
_REQUIRED_AWS_IDENTITIES = ('aws_account_id', 'region', 'availability_zone',
                            'ec2_instance_id')
_ALLOWED_AWS_MARKETS = frozenset({'on_demand', 'spot'})

_RUNTIME_RESOURCE_KEYS = frozenset({
    'image_id', 'container_image', 'volumes', '_resolved_container_image',
    '_cluster_config_overrides', '_docker_login_config'
})


@dataclasses.dataclass(frozen=True)
class AWSAuthorizationLocation:
    """One exact region-to-availability-zone authorization entry."""

    region: str
    availability_zones: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.region, str) or not self.region:
            raise ValueError('region must be a nonempty string')
        zones = _canonical_nonempty_strings(self.availability_zones,
                                            'availability_zones')
        object.__setattr__(self, 'availability_zones', zones)

    def to_dict(self) -> dict[str, object]:
        return {
            'region': self.region,
            'availability_zones': list(self.availability_zones),
        }


@dataclasses.dataclass(frozen=True)
class AWSRecoveryResourceEnvelope:
    """Closed actual-result allowance for authorization document v3."""

    allowed_aws_account_ids: tuple[str, ...]
    allowed_locations: tuple[AWSAuthorizationLocation, ...]
    allowed_market_types: tuple[str, ...]
    allowed_instance_types: tuple[str, ...]
    provider: str = 'aws'
    max_host_memory_gib: int = MAX_HOST_MEMORY_GIB_V3
    num_nodes: int = 1
    dedicated: bool = True
    require_new_create: bool = True
    required_identity: tuple[str, ...] = _REQUIRED_AWS_IDENTITIES

    def __post_init__(self) -> None:
        if self.provider != 'aws':
            raise ValueError('authorization-v3 provider must be aws')
        account_ids = _canonical_nonempty_strings(self.allowed_aws_account_ids,
                                                  'allowed_aws_account_ids')
        if any(
                _AWS_ACCOUNT_ID_PATTERN.fullmatch(value) is None
                for value in account_ids):
            raise ValueError('AWS account IDs must contain exactly 12 digits')
        if (not isinstance(self.allowed_locations, (list, tuple)) or
                not self.allowed_locations):
            raise ValueError('allowed_locations must be nonempty')
        locations = tuple(self.allowed_locations)
        if any(not isinstance(item, AWSAuthorizationLocation)
               for item in locations):
            raise ValueError('allowed_locations contains an invalid entry')
        if tuple(sorted(locations, key=lambda item: item.region)) != locations:
            raise ValueError('allowed_locations must be canonical-sorted')
        if len({item.region for item in locations}) != len(locations):
            raise ValueError('allowed location regions must be unique')
        markets = _canonical_nonempty_strings(self.allowed_market_types,
                                              'allowed_market_types')
        if not set(markets).issubset(_ALLOWED_AWS_MARKETS):
            raise ValueError('allowed_market_types contains an invalid market')
        instance_types = _canonical_nonempty_strings(
            self.allowed_instance_types, 'allowed_instance_types')
        if (type(self.max_host_memory_gib) is not int or  # pylint: disable=unidiomatic-typecheck
                self.max_host_memory_gib != MAX_HOST_MEMORY_GIB_V3):
            raise ValueError('authorization-v3 host-memory cap must be 16 GiB')
        if (type(self.num_nodes) is not int or  # pylint: disable=unidiomatic-typecheck
                self.num_nodes != 1 or self.dedicated is not True
                or self.require_new_create is not True):
            raise ValueError('authorization-v3 requires one dedicated create')
        if tuple(self.required_identity) != _REQUIRED_AWS_IDENTITIES:
            raise ValueError('authorization-v3 required_identity is invalid')
        object.__setattr__(self, 'allowed_aws_account_ids', account_ids)
        object.__setattr__(self, 'allowed_locations', locations)
        object.__setattr__(self, 'allowed_market_types', markets)
        object.__setattr__(self, 'allowed_instance_types', instance_types)
        object.__setattr__(self, 'required_identity', _REQUIRED_AWS_IDENTITIES)

    def allows_location(self, region: str, availability_zone: str) -> bool:
        return any(item.region == region and
                   availability_zone in item.availability_zones
                   for item in self.allowed_locations)

    def to_dict(self) -> dict[str, object]:
        return {
            'provider': self.provider,
            'allowed_aws_account_ids': list(self.allowed_aws_account_ids),
            'allowed_locations': [
                item.to_dict() for item in self.allowed_locations
            ],
            'allowed_market_types': list(self.allowed_market_types),
            'allowed_instance_types': list(self.allowed_instance_types),
            'max_host_memory_gib': self.max_host_memory_gib,
            'num_nodes': self.num_nodes,
            'dedicated': self.dedicated,
            'require_new_create': self.require_new_create,
            'required_identity': list(self.required_identity),
        }

    @property
    def sha256(self) -> str:
        return _sha256_json(self.to_dict())


@dataclasses.dataclass(frozen=True)
class TrustedRecoveryAuthorizationV3:
    """One exact server-owned authorization-v3 profile."""

    profile_id: str
    workspace: str
    service_name: str
    service_hash: str
    task_sha256: str
    runtime_image_digest: str
    owned_container_spec: runtime_recovery.OwnedContainerSpec
    owned_container_spec_sha256: str
    execution_envelope_sha256: str
    resource_envelope: AWSRecoveryResourceEnvelope
    authorization_version: int = AUTHORIZATION_VERSION_V3
    runtime_profile_version: int = RUNTIME_PROFILE_VERSION_V3
    required_runtime_capability: str = RUNTIME_CAPABILITY_V3

    def __post_init__(self) -> None:
        for field_name in ('profile_id', 'service_name', 'service_hash'):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f'{field_name} must be a nonempty string')
        if not isinstance(self.workspace, str):
            raise ValueError('workspace must be a string')
        _require_sha256(self.task_sha256, 'task_sha256')
        _require_sha256(self.owned_container_spec_sha256,
                        'owned_container_spec_sha256')
        _require_sha256(self.execution_envelope_sha256,
                        'execution_envelope_sha256')
        if (_SHA256_DIGEST_PATTERN.fullmatch(self.runtime_image_digest)
                is None):
            raise ValueError('runtime_image_digest must be SHA-256')
        if (type(self.authorization_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.authorization_version != AUTHORIZATION_VERSION_V3):
            raise ValueError('authorization_version must be 3')
        if (type(self.runtime_profile_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.runtime_profile_version != RUNTIME_PROFILE_VERSION_V3
                or self.required_runtime_capability != RUNTIME_CAPABILITY_V3):
            raise ValueError('authorization-v3 runtime mapping is invalid')
        if not isinstance(self.owned_container_spec,
                          runtime_recovery.OwnedContainerSpec):
            raise ValueError('owned_container_spec is invalid')
        if not isinstance(self.resource_envelope, AWSRecoveryResourceEnvelope):
            raise ValueError('resource_envelope is invalid')
        if (_sha256_json(self.owned_container_spec.to_dict())
                != self.owned_container_spec_sha256):
            raise ValueError('owned_container_spec digest does not match')
        if (_sha256_json(
                runtime_recovery.RecoveryExecutionEnvelope.standard().to_dict())
                != self.execution_envelope_sha256):
            raise ValueError('execution_envelope digest does not match')
        _, separator, digest = self.owned_container_spec.image.rpartition('@')
        if not separator or digest != self.runtime_image_digest:
            raise ValueError('owned spec image must match runtime image')

    @property
    def capability(self) -> str:
        return self.required_runtime_capability

    @property
    def authorization_sha256(self) -> str:
        return _sha256_json(self.to_dict())

    def launch_plan(self) -> runtime_recovery.RecoveryLaunchPlan:
        return runtime_recovery.RecoveryLaunchPlan.owned_container(
            self.owned_container_spec)

    def to_dict(self) -> dict[str, object]:
        return {
            'authorization_version': self.authorization_version,
            'profile_id': self.profile_id,
            'workspace': self.workspace,
            'service_name': self.service_name,
            'service_hash': self.service_hash,
            'task_sha256': self.task_sha256,
            'runtime_image_digest': self.runtime_image_digest,
            'runtime_profile_version': self.runtime_profile_version,
            'required_runtime_capability': self.required_runtime_capability,
            'owned_container_spec': self.owned_container_spec.to_dict(),
            'owned_container_spec_sha256': self.owned_container_spec_sha256,
            'execution_envelope_sha256': self.execution_envelope_sha256,
            'resource_envelope': self.resource_envelope.to_dict(),
        }


@dataclasses.dataclass(frozen=True)
class RequestedRecoveryAuthorizationV3:
    """Immutable identity persisted before policy and provider selection."""

    profile_id: str
    authorization_sha256: str
    runtime_profile_version: int
    expected_runtime_capability: str
    workspace: str
    resource_envelope_sha256: str
    task_sha256: str
    runtime_image_digest: str
    owned_container_spec_sha256: str
    execution_envelope_sha256: str
    authorization_version: int = AUTHORIZATION_VERSION_V3

    def __post_init__(self) -> None:
        if not isinstance(self.profile_id, str) or not self.profile_id:
            raise ValueError('profile_id must be a nonempty string')
        if not isinstance(self.workspace, str):
            raise ValueError('workspace must be a string')
        for field_name in ('authorization_sha256', 'resource_envelope_sha256',
                           'task_sha256', 'owned_container_spec_sha256',
                           'execution_envelope_sha256'):
            _require_sha256(getattr(self, field_name), field_name)
        if (not isinstance(self.runtime_image_digest, str) or
                _SHA256_DIGEST_PATTERN.fullmatch(
                    self.runtime_image_digest) is None):
            raise ValueError('runtime_image_digest must be SHA-256')
        if (type(self.authorization_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.authorization_version != AUTHORIZATION_VERSION_V3
                or type(self.runtime_profile_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.runtime_profile_version != RUNTIME_PROFILE_VERSION_V3 or
                self.expected_runtime_capability != RUNTIME_CAPABILITY_V3):
            raise ValueError('requested authorization-v3 mapping is invalid')

    @classmethod
    def from_authorization(
        cls, authorization: TrustedRecoveryAuthorizationV3
    ) -> 'RequestedRecoveryAuthorizationV3':
        return cls(
            profile_id=authorization.profile_id,
            authorization_sha256=authorization.authorization_sha256,
            runtime_profile_version=authorization.runtime_profile_version,
            expected_runtime_capability=authorization.capability,
            workspace=authorization.workspace,
            resource_envelope_sha256=authorization.resource_envelope.sha256,
            task_sha256=authorization.task_sha256,
            runtime_image_digest=authorization.runtime_image_digest,
            owned_container_spec_sha256=(
                authorization.owned_container_spec_sha256),
            execution_envelope_sha256=(authorization.execution_envelope_sha256))

    def to_intent_fields(self) -> dict[str, object]:
        """Return the authorization-owned part of launch intent v1."""
        return {
            'version': 1,
            'controller_contract_version':
                (constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION),
            'recovery_authorization_version': self.authorization_version,
            'recovery_authorization_profile_id': self.profile_id,
            'recovery_authorization_sha256': self.authorization_sha256,
            'runtime_profile_version': self.runtime_profile_version,
            'expected_runtime_capability': self.expected_runtime_capability,
            'workspace': self.workspace,
            'resource_envelope_sha256': self.resource_envelope_sha256,
            'task_sha256': self.task_sha256,
            'runtime_image_digest': self.runtime_image_digest,
            'owned_container_spec_sha256': self.owned_container_spec_sha256,
            'execution_envelope_sha256': self.execution_envelope_sha256,
        }


def _canonical_json(value: Any) -> str:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=True)


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode('utf-8')).hexdigest()


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f'{field_name} must be SHA-256')
    return value


def _canonical_nonempty_strings(value: object,
                                field_name: str) -> tuple[str, ...]:
    if (not isinstance(value, (list, tuple)) or not value or
            any(not isinstance(item, str) or not item for item in value)):
        raise ValueError(f'{field_name} must contain nonempty strings')
    normalized = tuple(value)
    if tuple(sorted(normalized)) != normalized:
        raise ValueError(f'{field_name} must be canonical-sorted')
    if len(set(normalized)) != len(normalized):
        raise ValueError(f'{field_name} must be duplicate-free')
    return normalized


def _runtime_resource_identity(resources: Any) -> Any:
    """Keep process-ownership fields while ignoring placement-only fields."""
    if isinstance(resources, list):
        identities = [_runtime_resource_identity(item) for item in resources]
        return sorted(identities, key=_canonical_json)
    if not isinstance(resources, dict):
        return resources
    identity: dict[str, Any] = {}
    for key, value in resources.items():
        if key in ('any_of', 'ordered'):
            identity[key] = _runtime_resource_identity(value)
        elif key == '_resolved_container_image' and isinstance(value, dict):
            identity[key] = {'digest': value.get('digest')}
        elif key in _RUNTIME_RESOURCE_KEYS:
            identity[key] = value
    return identity


def _command_text(command: Any) -> str | None:
    if isinstance(command, str):
        return command
    if (isinstance(command, (list, tuple)) and
            all(isinstance(item, str) for item in command)):
        return '\n'.join(command)
    return None


def _uses_generic_container_image(task: task_lib.Task) -> bool:
    """Whether SkyPilot's persistent outer-container path is requested."""
    for resource in task.resources:
        if (resource.container_image is not None or
                resource.resolved_container_image is not None):
            return True
    return False


def _matches_singleton_aws_authorization_resource(
        task: task_lib.Task, envelope: AWSRecoveryResourceEnvelope) -> bool:
    """Match the exact no-fallback pre-policy resource allowed by v3."""
    resources = tuple(task.resources)
    if (len(resources) != 1 or len(envelope.allowed_aws_account_ids) != 1 or
            len(envelope.allowed_locations) != 1 or
            len(envelope.allowed_market_types) != 1 or
            len(envelope.allowed_instance_types) != 1):
        return False
    location = envelope.allowed_locations[0]
    if len(location.availability_zones) != 1:
        return False
    resource = resources[0]
    cloud = resource.cloud
    market_type = envelope.allowed_market_types[0]
    return (cloud is not None and cloud.canonical_name() == 'aws' and
            resource.instance_type == envelope.allowed_instance_types[0] and
            resource.region == location.region and
            resource.zone == location.availability_zones[0] and
            resource.use_spot_specified and
            resource.use_spot == (market_type == 'spot'))


def runtime_image_digest(task: task_lib.Task) -> str | None:
    """Return the canonical owned-container image digest, or fail closed."""
    if _uses_generic_container_image(task):
        return None
    command = _command_text(task.run)
    if command is None:
        return None
    try:
        spec = runtime_recovery.OwnedContainerSpec.parse(command)
    except ValueError:
        return None
    _, separator, digest = spec.image.rpartition('@')
    return digest if separator else None


def safety_profile_digest(task: task_lib.Task) -> str:
    """Return a canonical digest of effective process-ownership fields."""
    config = copy.deepcopy(
        task_yaml_lib.to_yaml_config(task, redact_secrets=True))
    config.pop('name', None)
    config.pop('service', None)
    config.pop('_user_specified_yaml', None)
    envs = config.get('envs')
    if isinstance(envs, dict) and constants.REPLICA_ID_ENV_VAR in envs:
        replica_id = envs[constants.REPLICA_ID_ENV_VAR]
        if (not isinstance(replica_id, str) or not replica_id.isdecimal() or
                int(replica_id) < 0):
            envs[constants.REPLICA_ID_ENV_VAR] = '<invalid-replica-id>'
        else:
            envs[constants.REPLICA_ID_ENV_VAR] = '<server-replica-id>'
    resources = config.pop('resources', None)
    config['runtime_resources'] = {
        'submitted': _runtime_resource_identity(resources),
        'resolved_image_digest': runtime_image_digest(task),
    }
    return hashlib.sha256(_canonical_json(config).encode('utf-8')).hexdigest()


def create_authorization_v3(
    task: task_lib.Task,
    *,
    profile_id: str,
    workspace: str,
    service_name: str,
    service_hash: str,
    resource_envelope: AWSRecoveryResourceEnvelope,
) -> TrustedRecoveryAuthorizationV3:
    """Construct one authorization from an exact effective replica task.

    All derived digests come from the same typed implementations used by the
    production matcher.  Callers cannot provide or override those digests.
    """
    if (not isinstance(resource_envelope, AWSRecoveryResourceEnvelope) or
            not _matches_singleton_aws_authorization_resource(
                task, resource_envelope) or task.num_nodes != 1 or
            task.managed_secret_refs or _uses_generic_container_image(task)):
        raise ValueError('effective task is not eligible for authorization-v3')
    if not isinstance(task.run, str):
        raise ValueError('effective run command is not canonical')
    owned_spec = runtime_recovery.OwnedContainerSpec.parse(task.run)
    runtime_digest = runtime_image_digest(task)
    if runtime_digest is None:
        raise ValueError('effective runtime image is not an immutable digest')
    _, separator, owned_digest = owned_spec.image.rpartition('@')
    if not separator or owned_digest != runtime_digest:
        raise ValueError('owned runtime image does not match effective task')
    execution_envelope = runtime_recovery.RecoveryExecutionEnvelope.standard()
    return TrustedRecoveryAuthorizationV3(
        profile_id=profile_id,
        workspace=workspace,
        service_name=service_name,
        service_hash=service_hash,
        task_sha256=safety_profile_digest(task),
        runtime_image_digest=runtime_digest,
        owned_container_spec=owned_spec,
        owned_container_spec_sha256=_sha256_json(owned_spec.to_dict()),
        execution_envelope_sha256=_sha256_json(execution_envelope.to_dict()),
        resource_envelope=resource_envelope)


def _resource_envelope_v3_from_dict(
        value: object) -> AWSRecoveryResourceEnvelope:
    fields = {
        'provider', 'allowed_aws_account_ids', 'allowed_locations',
        'allowed_market_types', 'allowed_instance_types', 'max_host_memory_gib',
        'num_nodes', 'dedicated', 'require_new_create', 'required_identity'
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('authorization-v3 resource envelope is invalid')
    raw_locations = value['allowed_locations']
    if not isinstance(raw_locations, list):
        raise ValueError('allowed_locations must be a list')
    locations: list[AWSAuthorizationLocation] = []
    for raw_location in raw_locations:
        if (not isinstance(raw_location, dict) or
                set(raw_location) != {'region', 'availability_zones'}):
            raise ValueError('allowed location is invalid')
        locations.append(
            AWSAuthorizationLocation(
                region=raw_location['region'],
                availability_zones=tuple(raw_location['availability_zones']) if
                isinstance(raw_location['availability_zones'], list) else ()))
    return AWSRecoveryResourceEnvelope(
        provider=value['provider'],
        allowed_aws_account_ids=tuple(value['allowed_aws_account_ids'])
        if isinstance(value['allowed_aws_account_ids'], list) else (),
        allowed_locations=tuple(locations),
        allowed_market_types=tuple(value['allowed_market_types']) if isinstance(
            value['allowed_market_types'], list) else (),
        allowed_instance_types=tuple(value['allowed_instance_types'])
        if isinstance(value['allowed_instance_types'], list) else (),
        max_host_memory_gib=value['max_host_memory_gib'],
        num_nodes=value['num_nodes'],
        dedicated=value['dedicated'],
        require_new_create=value['require_new_create'],
        required_identity=tuple(value['required_identity']) if isinstance(
            value['required_identity'], list) else ())


def _authorization_v3_from_dict(
        value: object) -> TrustedRecoveryAuthorizationV3:
    fields = {
        'authorization_version', 'profile_id', 'workspace', 'service_name',
        'service_hash', 'task_sha256', 'runtime_image_digest',
        'runtime_profile_version', 'required_runtime_capability',
        'owned_container_spec', 'owned_container_spec_sha256',
        'execution_envelope_sha256', 'resource_envelope'
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError('authorization-v3 profile has invalid fields')
    return TrustedRecoveryAuthorizationV3(
        authorization_version=value['authorization_version'],
        profile_id=value['profile_id'],
        workspace=value['workspace'],
        service_name=value['service_name'],
        service_hash=value['service_hash'],
        task_sha256=value['task_sha256'],
        runtime_image_digest=value['runtime_image_digest'],
        runtime_profile_version=value['runtime_profile_version'],
        required_runtime_capability=value['required_runtime_capability'],
        owned_container_spec=runtime_recovery.OwnedContainerSpec.from_dict(
            value['owned_container_spec']),
        owned_container_spec_sha256=value['owned_container_spec_sha256'],
        execution_envelope_sha256=value['execution_envelope_sha256'],
        resource_envelope=_resource_envelope_v3_from_dict(
            value['resource_envelope']))


def parse_authorization_document_v3(
        raw: str) -> tuple[TrustedRecoveryAuthorizationV3, ...]:
    """Parse the exact closed authorization-v3 document contract."""
    if not isinstance(raw, str):
        raise TypeError('authorization-v3 document must be text')
    document = json.loads(raw)
    if (not isinstance(document, dict) or
            set(document) != {'version', 'profiles'} or
            type(document['version']) is not int or  # pylint: disable=unidiomatic-typecheck
            document['version'] != AUTHORIZATION_VERSION_V3
            or not isinstance(document['profiles'], list)):
        raise ValueError('authorization-v3 document is invalid')
    profiles = tuple(
        _authorization_v3_from_dict(value) for value in document['profiles'])
    profile_ids = [profile.profile_id for profile in profiles]
    if len(profile_ids) != len(set(profile_ids)):
        raise ValueError('profile IDs must be unique')
    return profiles


def canonical_authorization_document_v3(
        profiles: tuple[TrustedRecoveryAuthorizationV3, ...]) -> str:
    """Encode nonempty typed profiles as canonical authorization-v3 JSON."""
    if (not isinstance(profiles, tuple) or not profiles or
            any(not isinstance(profile, TrustedRecoveryAuthorizationV3)
                for profile in profiles)):
        raise ValueError('authorization-v3 profiles must be a nonempty tuple')
    ordered = tuple(sorted(profiles, key=lambda profile: profile.profile_id))
    profile_ids = [profile.profile_id for profile in ordered]
    if len(profile_ids) != len(set(profile_ids)):
        raise ValueError('profile IDs must be unique')
    raw = _canonical_json({
        'version': AUTHORIZATION_VERSION_V3,
        'profiles': [profile.to_dict() for profile in ordered],
    })
    # Keep the encoder and production parser coupled. Any future schema change
    # that updates only one side makes generation fail instead of emitting an
    # authorization the server will silently ignore.
    if parse_authorization_document_v3(raw) != ordered:
        raise ValueError('authorization-v3 canonical round trip failed')
    return raw


def _load_authorizations_v3() -> tuple[TrustedRecoveryAuthorizationV3, ...]:
    raw = os.environ.get(constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR)
    if not raw:
        return ()
    try:
        return parse_authorization_document_v3(raw)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        logger.warning('Ignoring invalid internal system-OOM recovery '
                       f'authorization-v3 document: {error}')
        return ()


def _resolve_authorization_v3(
    task: task_lib.Task,
    service_name: object,
    service_hash: object,
    *,
    requested_profile_id: object = None,
    requested_authorization_sha256: object = None,
    requested_resource_envelope_sha256: object = None,
    requested_owned_container_spec_sha256: object = None,
    requested_execution_envelope_sha256: object = None
) -> TrustedRecoveryAuthorizationV3 | None:
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(service_hash, str) or not service_hash or
            task.num_nodes != 1 or task.managed_secret_refs or
            _uses_generic_container_image(task)):
        return None
    image_digest = runtime_image_digest(task)
    if image_digest is None:
        return None
    task_digest = safety_profile_digest(task)
    workspace = skypilot_config.get_active_workspace()
    for authorization in _load_authorizations_v3():
        if not _matches_singleton_aws_authorization_resource(
                task, authorization.resource_envelope):
            continue
        if (authorization.workspace != workspace or
                authorization.service_name != service_name or
                authorization.service_hash != service_hash or
                authorization.task_sha256 != task_digest or
                authorization.runtime_image_digest != image_digest or
                task.run != authorization.owned_container_spec.render()):
            continue
        if (requested_profile_id is not None and
                authorization.profile_id != requested_profile_id):
            continue
        if (requested_authorization_sha256 is not None and
                authorization.authorization_sha256
                != requested_authorization_sha256):
            continue
        if (requested_resource_envelope_sha256 is not None and
                authorization.resource_envelope.sha256
                != requested_resource_envelope_sha256):
            continue
        if (requested_owned_container_spec_sha256 is not None and
                authorization.owned_container_spec_sha256
                != requested_owned_container_spec_sha256):
            continue
        if (requested_execution_envelope_sha256 is not None and
                authorization.execution_envelope_sha256
                != requested_execution_envelope_sha256):
            continue
        return authorization
    return None


def resolve_requested_authorization_v3(
        task: task_lib.Task, *, service_name: object,
        service_hash: object) -> RequestedRecoveryAuthorizationV3 | None:
    """Resolve an inert pre-policy authorization-v3 candidate identity."""
    authorization = _resolve_authorization_v3(task, service_name, service_hash)
    return (None if authorization is None else
            RequestedRecoveryAuthorizationV3.from_authorization(authorization))


_V3_COMMON_CONTEXT_KEYS = frozenset({
    constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY,
    constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_VERSION_KEY,
    constants.SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY,
    constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_SHA256_KEY,
    constants.SYSTEM_OOM_RECOVERY_RUNTIME_PROFILE_VERSION_KEY,
    constants.SYSTEM_OOM_RECOVERY_EXPECTED_RUNTIME_CAPABILITY_KEY,
    constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY,
    constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY,
    constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY,
    constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY,
    constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY,
    constants.SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY,
    constants.SYSTEM_OOM_RECOVERY_LAUNCH_GENERATION_KEY,
    constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY,
    constants.SYSTEM_OOM_RECOVERY_RESOURCE_ENVELOPE_SHA256_KEY,
    constants.SYSTEM_OOM_RECOVERY_TASK_SHA256_KEY,
    constants.SYSTEM_OOM_RECOVERY_RUNTIME_IMAGE_DIGEST_KEY,
    constants.SYSTEM_OOM_RECOVERY_OWNED_CONTAINER_SPEC_SHA256_KEY,
    constants.SYSTEM_OOM_RECOVERY_EXECUTION_ENVELOPE_SHA256_KEY,
})
_V3_UNBOUND_CONTEXT_KEYS = (_V3_COMMON_CONTEXT_KEYS |
                            {constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY})
_V3_BOUND_CONTEXT_KEYS = (_V3_COMMON_CONTEXT_KEYS |
                          {constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY})


def has_v3_system_oom_recovery_context(value: object) -> bool:
    """Whether an extra-launch context claims any recovery-owned field.

    Historical and unknown recovery contexts deliberately return true. The
    API endpoint then validates them against the sole closed v3 contract and
    rejects them before scheduling instead of treating them as ordinary
    retryable launches.
    """
    if not isinstance(value, dict):
        return False
    return any(
        isinstance(key, str) and
        key.startswith('sky_serve_system_oom_recovery_') for key in value)


def _validate_v3_context(value: object, *, bound: bool) -> dict[str, Any]:
    fields = _V3_BOUND_CONTEXT_KEYS if bound else _V3_UNBOUND_CONTEXT_KEYS
    form = 'bound' if bound else 'unbound'
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(f'{form} recovery launch context has invalid fields')
    context = copy.deepcopy(value)
    if (type(context[
            constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY])
            is not int or  # pylint: disable=unidiomatic-typecheck
            context[
                constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY]
            != constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION):
        raise ValueError('recovery controller contract must be 2')
    if (type(context[constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_VERSION_KEY])
            is not int or  # pylint: disable=unidiomatic-typecheck
            context[constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_VERSION_KEY]
            != AUTHORIZATION_VERSION_V3):
        raise ValueError('recovery authorization version must be 3')
    if (type(context[constants.SYSTEM_OOM_RECOVERY_RUNTIME_PROFILE_VERSION_KEY])
            is not int or  # pylint: disable=unidiomatic-typecheck
            context[constants.SYSTEM_OOM_RECOVERY_RUNTIME_PROFILE_VERSION_KEY]
            != RUNTIME_PROFILE_VERSION_V3 or context[
                constants.SYSTEM_OOM_RECOVERY_EXPECTED_RUNTIME_CAPABILITY_KEY]
            != RUNTIME_CAPABILITY_V3):
        raise ValueError('authorization-v3 runtime mapping is invalid')
    for key in (constants.SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY,
                constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY,
                constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY):
        if not isinstance(context[key], str) or not context[key]:
            raise ValueError(f'{key} must be a nonempty string')
    workspace = context[constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY]
    if not isinstance(workspace, str):
        raise ValueError(
            f'{constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY} must be a string')
    for key in (constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_SHA256_KEY,
                constants.SYSTEM_OOM_RECOVERY_RESOURCE_ENVELOPE_SHA256_KEY,
                constants.SYSTEM_OOM_RECOVERY_TASK_SHA256_KEY,
                constants.SYSTEM_OOM_RECOVERY_OWNED_CONTAINER_SPEC_SHA256_KEY,
                constants.SYSTEM_OOM_RECOVERY_EXECUTION_ENVELOPE_SHA256_KEY):
        _require_sha256(context[key], key)
    runtime_image_digest_value = context[
        constants.SYSTEM_OOM_RECOVERY_RUNTIME_IMAGE_DIGEST_KEY]
    if (not isinstance(runtime_image_digest_value, str) or
            _SHA256_DIGEST_PATTERN.fullmatch(runtime_image_digest_value)
            is None):
        raise ValueError('runtime image digest is invalid')
    for key in (constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY,
                constants.SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY,
                constants.SYSTEM_OOM_RECOVERY_LAUNCH_GENERATION_KEY):
        if (type(context[key]) is not int or  # pylint: disable=unidiomatic-typecheck
                context[key] <= 0):
            raise ValueError(f'{key} must be a positive integer')
    controller_pid = context[constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY]
    controller_ip = context[constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY]
    if not (controller_pid is None or type(controller_pid) is int):  # pylint: disable=unidiomatic-typecheck
        raise ValueError('controller PID is invalid')
    if not (controller_ip is None or isinstance(controller_ip, str)):
        raise ValueError('controller IP is invalid')
    form_key = (constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY
                if bound else constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY)
    form_value = context[form_key]
    if bound:
        if not isinstance(form_value, str) or not form_value:
            raise ValueError('bound request ID must be a nonempty string')
    elif (not isinstance(form_value, str) or
          _LAUNCH_NONCE_PATTERN.fullmatch(form_value) is None):
        raise ValueError('launch nonce must be 256-bit lowercase hex')
    return context


def validate_unbound_launch_context(value: object) -> dict[str, Any]:
    return _validate_v3_context(value, bound=False)


def validate_bound_launch_context(value: object) -> dict[str, Any]:
    return _validate_v3_context(value, bound=True)


def _extract_v3_context(value: object, *, bound: bool) -> dict[str, Any]:
    """Extract one recovery envelope from a larger generic launch context.

    The legacy ``/launch`` contract remains exact-key and continues to use
    ``validate_*_launch_context`` directly.  The generalized non-pool binding
    adds its own server-owned fields beside this envelope, so its profile
    adapter must select the closed recovery key set without accepting unknown
    recovery-owned fields.
    """
    if not isinstance(value, dict):
        raise ValueError('recovery launch context must be a mapping')
    fields = _V3_BOUND_CONTEXT_KEYS if bound else _V3_UNBOUND_CONTEXT_KEYS
    recovery_owned = {
        key for key in value if isinstance(key, str) and
        key.startswith('sky_serve_system_oom_recovery_')
    }
    expected_recovery_owned = {
        key for key in fields
        if key.startswith('sky_serve_system_oom_recovery_')
    }
    if recovery_owned != expected_recovery_owned:
        raise ValueError('recovery launch context has invalid owned fields')
    try:
        envelope = {key: value[key] for key in fields}
    except KeyError as error:
        raise ValueError('recovery launch context is incomplete') from error
    return _validate_v3_context(envelope, bound=bound)


def extract_unbound_launch_context(value: object) -> dict[str, Any]:
    """Extract and validate an unbound envelope from a generic context."""
    return _extract_v3_context(value, bound=False)


def extract_bound_launch_context(value: object) -> dict[str, Any]:
    """Extract and validate a bound envelope from a generic context."""
    return _extract_v3_context(value, bound=True)


def is_unbound_launch_context(value: object) -> bool:
    try:
        validate_unbound_launch_context(value)
        return True
    except (TypeError, ValueError):
        return False


def bind_launch_context(value: object, request_id: object) -> dict[str, Any]:
    """Replace a valid one-use nonce with the server-known request ID."""
    context = validate_unbound_launch_context(value)
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('request_id must be a nonempty string')
    context.pop(constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY)
    context[constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY] = request_id
    return validate_bound_launch_context(context)


def new_launch_nonce() -> str:
    return secrets.token_hex(32)


def create_unbound_launch_context(intent: object, *, service_name: object,
                                  service_version: object,
                                  controller_pid: object,
                                  controller_ip: object) -> dict[str, Any]:
    """Create the sole closed endpoint context from a persisted intent."""

    def _field(name: str) -> object:
        if isinstance(intent, dict):
            if name not in intent:
                raise ValueError(f'launch intent is missing {name}')
            return intent[name]
        try:
            return getattr(intent, name)
        except AttributeError as error:
            raise ValueError(f'launch intent is missing {name}') from error

    context = {
        constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY:
            _field('controller_contract_version'),
        constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_VERSION_KEY:
            _field('recovery_authorization_version'),
        constants.SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY:
            _field('recovery_authorization_profile_id'),
        constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_SHA256_KEY:
            _field('recovery_authorization_sha256'),
        constants.SYSTEM_OOM_RECOVERY_RUNTIME_PROFILE_VERSION_KEY:
            _field('runtime_profile_version'),
        constants.SYSTEM_OOM_RECOVERY_EXPECTED_RUNTIME_CAPABILITY_KEY:
            _field('expected_runtime_capability'),
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: service_name,
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: _field('service_hash'),
        constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: service_version,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: controller_pid,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: controller_ip,
        constants.SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY: _field('replica_id'),
        constants.SYSTEM_OOM_RECOVERY_LAUNCH_GENERATION_KEY:
            _field('launch_generation'),
        constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY: _field('launch_nonce'),
        constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY: _field('workspace'),
        constants.SYSTEM_OOM_RECOVERY_RESOURCE_ENVELOPE_SHA256_KEY:
            _field('resource_envelope_sha256'),
        constants.SYSTEM_OOM_RECOVERY_TASK_SHA256_KEY: _field('task_sha256'),
        constants.SYSTEM_OOM_RECOVERY_RUNTIME_IMAGE_DIGEST_KEY:
            _field('runtime_image_digest'),
        constants.SYSTEM_OOM_RECOVERY_OWNED_CONTAINER_SPEC_SHA256_KEY:
            _field('owned_container_spec_sha256'),
        constants.SYSTEM_OOM_RECOVERY_EXECUTION_ENVELOPE_SHA256_KEY:
            _field('execution_envelope_sha256'),
    }
    return validate_unbound_launch_context(context)


def match_trusted_profile(
    task: task_lib.Task, launch_context: dict[str, Any] | None
) -> TrustedRecoveryAuthorizationV3 | None:
    """Match an exact owner-fenced controller contract and effective task."""
    if launch_context is None:
        return None
    contract_version = launch_context.get(
        constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY)
    # Booleans compare equal to integers in Python but are not contract
    # versions.  Require the exact built-in integer representation.
    if (type(contract_version) is not int or  # pylint: disable=unidiomatic-typecheck
            contract_version
            != constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION):
        return None
    try:
        if 'sky_serve_non_pool_binding_protocol_version' in launch_context:
            context = extract_bound_launch_context(launch_context)
        else:
            context = validate_bound_launch_context(launch_context)
    except (TypeError, ValueError):
        return None
    service_name = context[constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY]
    service_hash = context[constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY]
    if context[constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY] != (
            skypilot_config.get_active_workspace()):
        return None
    authorization = _resolve_authorization_v3(
        task,
        service_name,
        service_hash,
        requested_profile_id=context[
            constants.SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY],
        requested_authorization_sha256=context[
            constants.SYSTEM_OOM_RECOVERY_AUTHORIZATION_SHA256_KEY],
        requested_resource_envelope_sha256=context[
            constants.SYSTEM_OOM_RECOVERY_RESOURCE_ENVELOPE_SHA256_KEY],
        requested_owned_container_spec_sha256=context[
            constants.SYSTEM_OOM_RECOVERY_OWNED_CONTAINER_SPEC_SHA256_KEY],
        requested_execution_envelope_sha256=context[
            constants.SYSTEM_OOM_RECOVERY_EXECUTION_ENVELOPE_SHA256_KEY])
    if (authorization is None or
            context[constants.SYSTEM_OOM_RECOVERY_TASK_SHA256_KEY]
            != authorization.task_sha256 or
            context[constants.SYSTEM_OOM_RECOVERY_RUNTIME_IMAGE_DIGEST_KEY]
            != authorization.runtime_image_digest):
        return None
    return authorization
