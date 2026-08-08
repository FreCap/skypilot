"""Pure native-V2 Kubernetes capsule construction.

The durable V2 action graph retains only a compact authority-cohort reference.
Every live construction call must therefore supply the complete, parsed cohort
from the caller's locked row.  This module resolves the pinned renderer leaves,
renders the three Kubernetes request objects, and constructs V2 capsules
directly; it never constructs or parses a V1 seed, renderer input, or capsule.

The final V2 binding and config-access artifacts are an activation gate.  The
resolver below already rejects the installed V1 artifact paths and accepts only
the future V2 paths.  Until those artifacts are packaged, production calls fail
closed during descriptor-safe resolution.
"""

from __future__ import annotations

import dataclasses
import hashlib
import importlib
import json
import os
import re
import stat
import types
from typing import Any, ClassVar
import unicodedata
import uuid

import sky as sky_package
from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_cleanup_v2 as cleanup_v2
from sky.serve import resource_action_provider_artifacts as provider_artifacts
from sky.serve import resource_action_renderer as renderer_v1
from sky.serve import resource_actions as actions

_V1_ARTIFACT_DIRECTORY = (
    'sky/serve/resource_action_artifacts/kubernetes_renderer_v1')
_V2_ARTIFACT_DIRECTORY = (
    'sky/serve/resource_action_artifacts/kubernetes_renderer_v2')
_ARTIFACT_PATHS: tuple[tuple[str, str], ...] = (
    ('outer_template', f'{_V1_ARTIFACT_DIRECTORY}/outer_template.json'),
    ('node_fragment', f'{_V1_ARTIFACT_DIRECTORY}/node_fragment.json'),
    ('binding_schema', f'{_V2_ARTIFACT_DIRECTORY}/binding_schema.json'),
    ('config_access_inventory',
     f'{_V2_ARTIFACT_DIRECTORY}/config_access_inventory.json'),
    ('admitted_object_normalization',
     f'{_V1_ARTIFACT_DIRECTORY}/admitted_object_normalization.json'),
)
_MAX_CANONICAL_BYTES = 65_536
_MAX_TEXT_BYTES = 1_024
_MAX_ARTIFACT_BYTES = 65_536
_EXPLICIT_USER_LABEL_RE = re.compile(r'^[a-z0-9](?:[a-z0-9_-]{0,61}[a-z0-9])?$')
_SELECTOR_KEYS = (
    'component',
    'skypilot-cluster-name',
    'skypilot.co/cluster-record-uuid',
    'skypilot.co/serve-replica-incarnation',
)
_IDENTITY_LABEL_KEYS = (
    'skypilot-cluster-name',
    'skypilot.co/cluster-record-uuid',
    'skypilot.co/serve-replica-incarnation',
)
_V2_BINDING_SCHEMA_SHA256 = (
    '957bc3d3bc489f8714c7830de5a8c53263ea5a726eee3381f96742f7fc3439c7')
_V2_CONFIG_ACCESS_INVENTORY_SHA256 = (
    '1fc41b7eabaafa7375f8690302424502579ea9d317107d628ca6fe8c54e560d2')
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> dict[str, Any]:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if any(type(key) is not str for key in value):
        raise TypeError(f'{name} keys must be text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    encoded = actions.canonical_json_bytes(value)
    if len(encoded) > _MAX_CANONICAL_BYTES:
        raise ValueError(f'{name} exceeds {_MAX_CANONICAL_BYTES} bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if normalized != value:
        raise ValueError(f'{name} is not canonical.')
    return normalized


def _version_two(value: Any, *, name: str) -> int:
    if type(value) is not int or value != 2:
        raise ValueError(f'{name} must be integer 2.')
    return value


def _text(value: Any, *, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    try:
        size = len(value.encode('utf-8'))
    except UnicodeEncodeError as error:
        raise ValueError(f'{name} must be valid UTF-8 text.') from error
    if (size == 0 or size > _MAX_TEXT_BYTES or '\x00' in value or
            unicodedata.normalize('NFC', value) != value or json.loads(
                actions.canonical_json_bytes(value).decode('utf-8')) != value):
        raise ValueError(
            f'{name} must be 1..{_MAX_TEXT_BYTES} canonical UTF-8 bytes.')
    return value


def _sha256(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f'{name} must be a UUID.') from error
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


def _validate_prerequisite_inventory(
    value: Any,
    *,
    name: str,
) -> tuple[actions.ProviderKubernetesPrerequisiteV1, ...]:
    if type(value) is not tuple:
        raise TypeError(f'{name} must be a tuple.')
    role_map = actions.PROVIDER_KUBERNETES_PREREQUISITE_ROLE_MAP_V1
    if (len(value) != len(role_map) or any(
            type(item) is not actions.ProviderKubernetesPrerequisiteV1
            for item in value)):
        raise ValueError(f'{name} must contain the exact typed inventory.')
    if tuple(item.role for item in value) != tuple(
            entry.role for entry in role_map):
        raise ValueError(f'{name} does not match the exact role-map order.')

    authority_release = value[0]
    for alias in (value[3], value[4]):
        authority_value = authority_release.canonical_value()
        alias_value = alias.canonical_value()
        del authority_value['role']
        del alias_value['role']
        if actions.canonical_json_bytes(
                authority_value) != actions.canonical_json_bytes(alias_value):
            raise ValueError(f'{name} Namespace aliases are not byte-equal.')
    nonaliased = (value[0], value[1], value[2], *value[5:])
    live_keys = tuple((item.api_version, item.kind, item.namespace, item.name)
                      for item in nonaliased)
    live_uids = tuple(item.uid for item in nonaliased)
    if (len(set(live_keys)) != len(live_keys) or
            len(set(live_uids)) != len(live_uids)):
        raise ValueError(f'{name} nonaliased identities are not unique.')
    return value


def _prerequisites_from_value(
    value: Any,
    *,
    name: str,
) -> tuple[actions.ProviderKubernetesPrerequisiteV1, ...]:
    if type(value) is not list:
        raise TypeError(f'{name} must be a list.')
    return _validate_prerequisite_inventory(tuple(
        actions.ProviderKubernetesPrerequisiteV1.from_value(item)
        for item in value),
                                            name=name)


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesExecutionCapsuleSeedV2(authority.CanonicalContract):
    """Closed native-V2 launch capsule fields available before rendering."""

    version: int
    implementation_contract: str
    executor_cohort: actions.ProviderAuthorityWorkerCohortReferenceV1
    config_projection: actions.ProviderKubernetesConfigProjectionV1
    config_projection_sha256: str
    scope: actions.ProviderKubernetesScopeV1
    principals: actions.ProviderKubernetesPrincipalsV1
    prerequisites: tuple[actions.ProviderKubernetesPrerequisiteV1, ...]
    request_identity: actions.ProviderKubernetesRequestIdentityV1
    resources: actions.ProviderKubernetesResourceContractV1
    renderer: actions.ProviderKubernetesRendererV1
    post_provision: actions.ProviderKubernetesPostProvisionV1
    endpoint: actions.ProviderKubernetesEndpointContractV1
    scheduling: actions.ProviderKubernetesSchedulingContractV1
    storage: actions.ProviderKubernetesStorageContractV1
    metadata: actions.ProviderKubernetesMetadataContractV1
    security: actions.ProviderKubernetesSecurityContractV1
    topology: actions.ProviderPodTopologyV1
    mutation_contract: actions.ProviderKubernetesLaunchMutationContractV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'implementation_contract', 'executor_cohort',
        'config_projection', 'config_projection_sha256', 'scope', 'principals',
        'prerequisites', 'request_identity', 'resources', 'renderer',
        'post_provision', 'endpoint', 'scheduling', 'storage', 'metadata',
        'security', 'topology', 'mutation_contract'
    })
    _IMPLEMENTATION_CONTRACT: ClassVar[
        str] = 'kubernetes_serve_prebooted_runtime_v1'

    def __post_init__(self) -> None:
        _version_two(self.version, name='launch capsule seed V2 version')
        if type(self.implementation_contract) is not str:
            raise TypeError('launch capsule seed V2 implementation contract '
                            'must be text.')
        if self.implementation_contract != self._IMPLEMENTATION_CONTRACT:
            raise ValueError('launch capsule seed V2 implementation contract '
                             'is unsupported.')
        child_types: tuple[tuple[str, type[Any]], ...] = (
            ('executor_cohort',
             actions.ProviderAuthorityWorkerCohortReferenceV1),
            ('config_projection', actions.ProviderKubernetesConfigProjectionV1),
            ('scope', actions.ProviderKubernetesScopeV1),
            ('principals', actions.ProviderKubernetesPrincipalsV1),
            ('request_identity', actions.ProviderKubernetesRequestIdentityV1),
            ('resources', actions.ProviderKubernetesResourceContractV1),
            ('renderer', actions.ProviderKubernetesRendererV1),
            ('post_provision', actions.ProviderKubernetesPostProvisionV1),
            ('endpoint', actions.ProviderKubernetesEndpointContractV1),
            ('scheduling', actions.ProviderKubernetesSchedulingContractV1),
            ('storage', actions.ProviderKubernetesStorageContractV1),
            ('metadata', actions.ProviderKubernetesMetadataContractV1),
            ('security', actions.ProviderKubernetesSecurityContractV1),
            ('topology', actions.ProviderPodTopologyV1),
            ('mutation_contract',
             actions.ProviderKubernetesLaunchMutationContractV1),
        )
        for field, expected_type in child_types:
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'launch capsule seed V2 {field} has an '
                                'invalid type.')
        object.__setattr__(
            self, 'prerequisites',
            _validate_prerequisite_inventory(
                self.prerequisites,
                name='launch capsule seed V2 prerequisites'))
        projection_hash = _sha256(
            self.config_projection_sha256,
            name='launch capsule seed V2 config projection hash')
        object.__setattr__(self, 'config_projection_sha256', projection_hash)
        if projection_hash != self.config_projection.sha256:
            raise ValueError('launch capsule seed V2 config projection hash '
                             'does not match.')
        if (self.renderer.source.canonical_bytes !=
                self.post_provision.job_submission.run_source.canonical_bytes):
            raise ValueError('launch capsule seed V2 renderer and run source '
                             'are not byte-equal.')
        if (self.config_projection.config_access_inventory.canonical_bytes
                != self.renderer.config_access_inventory.canonical_bytes):
            raise ValueError('launch capsule seed V2 config-access references '
                             'are not byte-equal.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
        cls,
        value: Any,
    ) -> ProviderKubernetesExecutionCapsuleSeedV2:
        raw = _closed_object(value,
                             name='Kubernetes launch capsule seed V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            implementation_contract=raw['implementation_contract'],
            executor_cohort=(
                actions.ProviderAuthorityWorkerCohortReferenceV1.from_value(
                    raw['executor_cohort'])),
            config_projection=actions.ProviderKubernetesConfigProjectionV1.
            from_value(raw['config_projection']),
            config_projection_sha256=raw['config_projection_sha256'],
            scope=actions.ProviderKubernetesScopeV1.from_value(raw['scope']),
            principals=actions.ProviderKubernetesPrincipalsV1.from_value(
                raw['principals']),
            prerequisites=_prerequisites_from_value(
                raw['prerequisites'],
                name='launch capsule seed V2 prerequisites'),
            request_identity=actions.ProviderKubernetesRequestIdentityV1.
            from_value(raw['request_identity']),
            resources=actions.ProviderKubernetesResourceContractV1.from_value(
                raw['resources']),
            renderer=actions.ProviderKubernetesRendererV1.from_value(
                raw['renderer']),
            post_provision=actions.ProviderKubernetesPostProvisionV1.from_value(
                raw['post_provision']),
            endpoint=actions.ProviderKubernetesEndpointContractV1.from_value(
                raw['endpoint']),
            scheduling=actions.ProviderKubernetesSchedulingContractV1.
            from_value(raw['scheduling']),
            storage=actions.ProviderKubernetesStorageContractV1.from_value(
                raw['storage']),
            metadata=actions.ProviderKubernetesMetadataContractV1.from_value(
                raw['metadata']),
            security=actions.ProviderKubernetesSecurityContractV1.from_value(
                raw['security']),
            topology=actions.ProviderPodTopologyV1.from_value(raw['topology']),
            mutation_contract=(
                actions.ProviderKubernetesLaunchMutationContractV1.from_value(
                    raw['mutation_contract'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'implementation_contract': self._IMPLEMENTATION_CONTRACT,
            'executor_cohort': self.executor_cohort.canonical_value(),
            'config_projection': self.config_projection.canonical_value(),
            'config_projection_sha256': self.config_projection_sha256,
            'scope': self.scope.canonical_value(),
            'principals': self.principals.canonical_value(),
            'prerequisites': [
                item.canonical_value() for item in self.prerequisites
            ],
            'request_identity': self.request_identity.canonical_value(),
            'resources': self.resources.canonical_value(),
            'renderer': self.renderer.canonical_value(),
            'post_provision': self.post_provision.canonical_value(),
            'endpoint': self.endpoint.canonical_value(),
            'scheduling': self.scheduling.canonical_value(),
            'storage': self.storage.canonical_value(),
            'metadata': self.metadata.canonical_value(),
            'security': self.security.canonical_value(),
            'topology': self.topology.canonical_value(),
            'mutation_contract': self.mutation_contract.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesRendererInputV2(authority.CanonicalContract):
    """Sole closed native-V2 input accepted by the launch renderer."""

    version: int
    contract: str
    resource_identity: actions.ProviderResourceIdentityV1
    sky_cluster_name: str
    sky_cluster_record_uuid: uuid.UUID
    name_basis: actions.ProviderWorkloadNameBasisV1
    seed: ProviderKubernetesExecutionCapsuleSeedV2
    retained_source: actions.ProviderLaunchContentSourceV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'contract', 'resource_identity', 'sky_cluster_name',
        'sky_cluster_record_uuid', 'name_basis', 'seed', 'retained_source'
    })
    _CONTRACT: ClassVar[str] = 'validated_launch_spec_v2'

    def __post_init__(self) -> None:
        _version_two(self.version, name='Kubernetes renderer input V2 version')
        if type(self.contract) is not str:
            raise TypeError('Kubernetes renderer input V2 contract must be '
                            'text.')
        if self.contract != self._CONTRACT:
            raise ValueError('Kubernetes renderer input V2 contract is '
                             'unsupported.')
        for field, expected_type in (
            ('resource_identity', actions.ProviderResourceIdentityV1),
            ('name_basis', actions.ProviderWorkloadNameBasisV1),
            ('seed', ProviderKubernetesExecutionCapsuleSeedV2),
            ('retained_source', actions.ProviderLaunchContentSourceV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'Kubernetes renderer input V2 {field} has an '
                                'invalid type.')
        object.__setattr__(
            self, 'sky_cluster_name',
            _text(self.sky_cluster_name,
                  name='Kubernetes renderer input V2 sky_cluster_name'))
        object.__setattr__(
            self, 'sky_cluster_record_uuid',
            _uuid(self.sky_cluster_record_uuid,
                  name='Kubernetes renderer input V2 sky_cluster_record_uuid'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesRendererInputV2:
        raw = _closed_object(value,
                             name='Kubernetes renderer input V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            contract=raw['contract'],
            resource_identity=actions.ProviderResourceIdentityV1.from_value(
                raw['resource_identity']),
            sky_cluster_name=raw['sky_cluster_name'],
            sky_cluster_record_uuid=_uuid(
                raw['sky_cluster_record_uuid'],
                name='Kubernetes renderer input V2 sky_cluster_record_uuid'),
            name_basis=actions.ProviderWorkloadNameBasisV1.from_value(
                raw['name_basis']),
            seed=ProviderKubernetesExecutionCapsuleSeedV2.from_value(
                raw['seed']),
            retained_source=actions.ProviderLaunchContentSourceV1.from_value(
                raw['retained_source']))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'contract': self._CONTRACT,
            'resource_identity': self.resource_identity.canonical_value(),
            'sky_cluster_name': self.sky_cluster_name,
            'sky_cluster_record_uuid': str(self.sky_cluster_record_uuid),
            'name_basis': self.name_basis.canonical_value(),
            'seed': self.seed.canonical_value(),
            'retained_source': self.retained_source.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesDownExecutionCapsuleInputV2(authority.CanonicalContract
                                                   ):
    """Closed native-V2 down fields before cleanup-target rederivation."""

    version: int
    implementation_contract: str
    executor_cohort: actions.ProviderAuthorityWorkerCohortReferenceV1
    config_projection: actions.ProviderKubernetesConfigProjectionV1
    config_projection_sha256: str
    scope: actions.ProviderKubernetesScopeV1
    principals: actions.ProviderKubernetesPrincipalsV1
    prerequisites: tuple[actions.ProviderKubernetesPrerequisiteV1, ...]
    mutation_contract: actions.ProviderKubernetesDownMutationContractV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'implementation_contract', 'executor_cohort',
        'config_projection', 'config_projection_sha256', 'scope', 'principals',
        'prerequisites', 'mutation_contract'
    })
    _IMPLEMENTATION_CONTRACT: ClassVar[
        str] = 'kubernetes_serve_exact_cleanup_v1'

    def __post_init__(self) -> None:
        _version_two(self.version, name='down capsule input V2 version')
        if type(self.implementation_contract) is not str:
            raise TypeError('down capsule input V2 implementation contract '
                            'must be text.')
        if self.implementation_contract != self._IMPLEMENTATION_CONTRACT:
            raise ValueError('down capsule input V2 implementation contract '
                             'is unsupported.')
        for field, expected_type in (
            ('executor_cohort',
             actions.ProviderAuthorityWorkerCohortReferenceV1),
            ('config_projection', actions.ProviderKubernetesConfigProjectionV1),
            ('scope', actions.ProviderKubernetesScopeV1),
            ('principals', actions.ProviderKubernetesPrincipalsV1),
            ('mutation_contract',
             actions.ProviderKubernetesDownMutationContractV1),
        ):
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'down capsule input V2 {field} has an invalid '
                                'type.')
        object.__setattr__(
            self, 'prerequisites',
            _validate_prerequisite_inventory(
                self.prerequisites, name='down capsule input V2 prerequisites'))
        projection_hash = _sha256(
            self.config_projection_sha256,
            name='down capsule input V2 config projection hash')
        object.__setattr__(self, 'config_projection_sha256', projection_hash)
        if projection_hash != self.config_projection.sha256:
            raise ValueError('down capsule input V2 config projection hash '
                             'does not match.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
        cls,
        value: Any,
    ) -> ProviderKubernetesDownExecutionCapsuleInputV2:
        raw = _closed_object(value,
                             name='Kubernetes down capsule input V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            implementation_contract=raw['implementation_contract'],
            executor_cohort=(
                actions.ProviderAuthorityWorkerCohortReferenceV1.from_value(
                    raw['executor_cohort'])),
            config_projection=actions.ProviderKubernetesConfigProjectionV1.
            from_value(raw['config_projection']),
            config_projection_sha256=raw['config_projection_sha256'],
            scope=actions.ProviderKubernetesScopeV1.from_value(raw['scope']),
            principals=actions.ProviderKubernetesPrincipalsV1.from_value(
                raw['principals']),
            prerequisites=_prerequisites_from_value(
                raw['prerequisites'],
                name='down capsule input V2 prerequisites'),
            mutation_contract=(
                actions.ProviderKubernetesDownMutationContractV1.from_value(
                    raw['mutation_contract'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'implementation_contract': self._IMPLEMENTATION_CONTRACT,
            'executor_cohort': self.executor_cohort.canonical_value(),
            'config_projection': self.config_projection.canonical_value(),
            'config_projection_sha256': self.config_projection_sha256,
            'scope': self.scope.canonical_value(),
            'principals': self.principals.canonical_value(),
            'prerequisites': [
                item.canonical_value() for item in self.prerequisites
            ],
            'mutation_contract': self.mutation_contract.canonical_value(),
        }


@dataclasses.dataclass(frozen=True, init=False)
class _ExactRendererArtifactDocumentV2:
    """One immutable parsed V2 artifact pinned by its owning reference."""

    _canonical_bytes: bytes = dataclasses.field(repr=False)

    def __init__(self, value: Any) -> None:
        if type(value) is not dict:
            raise TypeError('renderer V2 artifact must be an exact object.')
        object.__setattr__(self, '_canonical_bytes',
                           actions.canonical_json_bytes(value))
        if len(self._canonical_bytes) > _MAX_ARTIFACT_BYTES:
            raise ValueError('renderer V2 artifact exceeds its byte bound.')

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self._canonical_bytes).hexdigest()

    def canonical_value(self) -> dict[str, Any]:
        value = json.loads(self._canonical_bytes.decode('utf-8'))
        assert type(value) is dict
        return value


class ProviderKubernetesBindingSchemaArtifactV2(_ExactRendererArtifactDocumentV2
                                               ):
    """The exact 17-binding V2 schema with no V1 input root."""

    def __init__(self, value: Any) -> None:
        super().__init__(value)
        if self.sha256 != _V2_BINDING_SCHEMA_SHA256:
            raise ValueError('renderer V2 binding schema is not exact.')


class ProviderKubernetesConfigAccessInventoryV2(_ExactRendererArtifactDocumentV2
                                               ):
    """The exact fail-closed V2 call, input, and effect inventory."""

    def __init__(self, value: Any) -> None:
        super().__init__(value)
        if self.sha256 != _V2_CONFIG_ACCESS_INVENTORY_SHA256:
            raise ValueError('renderer V2 config-access inventory is not '
                             'exact.')
        raw = self.canonical_value()
        if raw.get('schema') != (
                'skypilot.serve.prebooted-direct-pod.config-access-inventory.v2'
        ):
            raise ValueError('renderer config-access inventory is not V2.')
        encoded = self.canonical_bytes.decode('utf-8')
        if ('sky.serve.resource_action_renderer.' in encoded or
                'validated_launch_spec_v1' in encoded):
            raise ValueError('renderer V2 config-access inventory references '
                             'the sealed V1 renderer graph.')

    @classmethod
    def from_value(
        cls,
        value: Any,
    ) -> ProviderKubernetesConfigAccessInventoryV2:
        return cls(value)


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesBindingSchemaArtifactV2:
    """Verified raw and parsed preimages for the exact V2 binding schema."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    raw_artifact: provider_artifacts.RawCanonicalRendererArtifactBytesV1
    schema: ProviderKubernetesBindingSchemaArtifactV2

    def __post_init__(self) -> None:
        if (type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1 or
                type(self.raw_artifact)
                is not provider_artifacts.RawCanonicalRendererArtifactBytesV1 or
                type(self.schema)
                is not ProviderKubernetesBindingSchemaArtifactV2):
            raise TypeError('resolved renderer V2 binding artifact has an '
                            'invalid type.')
        if (self.raw_artifact.artifact_ref.canonical_bytes
                != self.artifact_ref.canonical_bytes or
                self.raw_artifact.raw_bytes[:-1]
                != self.schema.canonical_bytes):
            raise ValueError('resolved renderer V2 binding preimages disagree.')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesConfigAccessInventoryArtifactV2:
    """Verified raw and parsed preimages for the V2 access inventory."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    raw_artifact: provider_artifacts.RawCanonicalRendererArtifactBytesV1
    inventory: ProviderKubernetesConfigAccessInventoryV2

    def __post_init__(self) -> None:
        if (type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1 or
                type(self.raw_artifact)
                is not provider_artifacts.RawCanonicalRendererArtifactBytesV1 or
                type(self.inventory)
                is not ProviderKubernetesConfigAccessInventoryV2):
            raise TypeError('resolved renderer V2 inventory artifact has an '
                            'invalid type.')
        if (self.raw_artifact.artifact_ref.canonical_bytes
                != self.artifact_ref.canonical_bytes or
                self.raw_artifact.raw_bytes[:-1]
                != self.inventory.canonical_bytes):
            raise ValueError('resolved renderer V2 inventory preimages '
                             'disagree.')


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesRendererArtifactSetV2:
    """The five renderer leaves, with V2 binding and access evidence."""

    outer_template: renderer_v1.ResolvedProviderKubernetesOuterTemplateArtifactV1
    node_fragment: renderer_v1.ResolvedProviderKubernetesNodeFragmentArtifactV1
    binding_schema: ResolvedProviderKubernetesBindingSchemaArtifactV2
    config_access_inventory: (
        ResolvedProviderKubernetesConfigAccessInventoryArtifactV2)
    admitted_object_normalization: (
        provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1)

    def __post_init__(self) -> None:
        expected_types = (
            ('outer_template',
             renderer_v1.ResolvedProviderKubernetesOuterTemplateArtifactV1),
            ('node_fragment',
             renderer_v1.ResolvedProviderKubernetesNodeFragmentArtifactV1),
            ('binding_schema',
             ResolvedProviderKubernetesBindingSchemaArtifactV2),
            ('config_access_inventory',
             ResolvedProviderKubernetesConfigAccessInventoryArtifactV2),
            ('admitted_object_normalization', provider_artifacts.
             ResolvedProviderKubernetesNormalizationArtifactV1),
        )
        for field, expected_type in expected_types:
            if type(getattr(self, field)) is not expected_type:
                raise TypeError(f'resolved renderer V2 artifact {field} has '
                                'an invalid type.')
        refs = (
            self.outer_template.artifact_ref,
            self.node_fragment.artifact_ref,
            self.binding_schema.artifact_ref,
            self.config_access_inventory.artifact_ref,
            self.admitted_object_normalization.artifact_ref,
        )
        for (role, expected_path), artifact_ref in zip(_ARTIFACT_PATHS, refs):
            if artifact_ref.repo_path != expected_path:
                raise ValueError(f'resolved renderer V2 artifact {role} has '
                                 'an unexpected path.')


def _read_renderer_artifacts_v2(
    renderer_input: ProviderKubernetesRendererInputV2,
) -> ResolvedProviderKubernetesRendererArtifactSetV2:
    """Descriptor-safely load only the mixed leaf/V2 artifact path set."""

    if type(renderer_input) is not ProviderKubernetesRendererInputV2:
        raise TypeError('Kubernetes renderer input V2 has an invalid type.')
    package_init = sky_package.__file__
    if (type(package_init) is not str or not os.path.isabs(package_init) or
            os.path.basename(package_init) != '__init__.py'):
        raise ValueError('the imported sky package location is not regular.')
    package_directory = os.path.dirname(package_init)
    if os.path.basename(package_directory) != 'sky':
        raise ValueError('the imported sky package location is not top-level.')
    distribution_root = os.path.dirname(package_directory)
    required_flags = ('O_CLOEXEC', 'O_DIRECTORY', 'O_NOFOLLOW', 'O_NONBLOCK')
    if any(not hasattr(os, flag) for flag in required_flags):
        raise RuntimeError('descriptor-safe V2 artifact resolution is '
                           'unsupported.')
    read_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    nonblocking_read_flags = read_flags | os.O_NONBLOCK
    directory_flags = read_flags | os.O_DIRECTORY

    renderer = renderer_input.seed.renderer
    refs = (
        renderer.outer_template,
        renderer.node_fragment,
        renderer.binding_schema,
        renderer.config_access_inventory,
        renderer.admitted_object_normalization,
    )
    for (role, expected_path), artifact_ref in zip(_ARTIFACT_PATHS, refs):
        if type(artifact_ref) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError(f'renderer V2 artifact {role} reference has an '
                            'invalid type.')
        if artifact_ref.repo_path != expected_path:
            raise ValueError(f'renderer V2 artifact {role} path is not exact.')
        if artifact_ref.byte_size > _MAX_ARTIFACT_BYTES:
            raise ValueError(f'renderer V2 artifact {role} is oversized.')

    raw_artifacts: dict[
        str, provider_artifacts.RawCanonicalRendererArtifactBytesV1] = {}
    raw_bytes_by_role: dict[str, bytes] = {}
    root_fd = -1
    package_fd = -1
    package_init_fd = -1
    try:
        root_fd = os.open(distribution_root, directory_flags)
        package_fd = os.open('sky', directory_flags, dir_fd=root_fd)
        absolute_package_fd = os.open(package_directory, directory_flags)
        try:
            package_stat = os.fstat(package_fd)
            absolute_package_stat = os.fstat(absolute_package_fd)
            if ((package_stat.st_dev, package_stat.st_ino)
                    != (absolute_package_stat.st_dev,
                        absolute_package_stat.st_ino)):
                raise ValueError('the imported sky package is not bound to '
                                 'the opened distribution root.')
        finally:
            os.close(absolute_package_fd)
        package_init_fd = os.open('__init__.py',
                                  nonblocking_read_flags,
                                  dir_fd=package_fd)
        if not stat.S_ISREG(os.fstat(package_init_fd).st_mode):
            raise ValueError('the imported sky package initializer is not a '
                             'regular file.')
        for (role, expected_path), artifact_ref in zip(_ARTIFACT_PATHS, refs):
            current_fd = os.dup(package_fd)
            file_fd = -1
            try:
                segments = expected_path.split('/')
                if not segments or segments[0] != 'sky':
                    raise ValueError('renderer V2 artifact path is not rooted '
                                     'in the imported package.')
                for index, segment in enumerate(segments[1:]):
                    is_last = index == len(segments[1:]) - 1
                    next_fd = os.open(
                        segment,
                        nonblocking_read_flags if is_last else directory_flags,
                        dir_fd=current_fd)
                    os.close(current_fd)
                    current_fd = -1
                    if is_last:
                        file_fd = next_fd
                    else:
                        current_fd = next_fd
                if file_fd < 0:
                    raise ValueError('renderer V2 artifact path has no file.')
                before = os.fstat(file_fd)
                if (not stat.S_ISREG(before.st_mode) or
                        before.st_size != artifact_ref.byte_size):
                    raise ValueError(f'renderer V2 artifact {role} is not an '
                                     'exact regular file.')
                content = bytearray()
                while len(content) <= artifact_ref.byte_size:
                    chunk = os.read(file_fd,
                                    artifact_ref.byte_size + 1 - len(content))
                    if not chunk:
                        break
                    content.extend(chunk)
                after = os.fstat(file_fd)
                before_identity = (before.st_dev, before.st_ino, before.st_mode,
                                   before.st_size, before.st_mtime_ns,
                                   before.st_ctime_ns)
                after_identity = (after.st_dev, after.st_ino, after.st_mode,
                                  after.st_size, after.st_mtime_ns,
                                  after.st_ctime_ns)
                if before_identity != after_identity:
                    raise ValueError(f'renderer V2 artifact {role} changed '
                                     'while being read.')
                if len(content) != artifact_ref.byte_size:
                    raise ValueError(f'renderer V2 artifact {role} size '
                                     'drifted.')
                raw_bytes = bytes(content)
                raw_artifacts[role] = (
                    provider_artifacts.RawCanonicalRendererArtifactBytesV1.
                    from_verified_bytes(artifact_ref, raw_bytes))
                raw_bytes_by_role[role] = raw_bytes
            finally:
                if current_fd >= 0:
                    os.close(current_fd)
                if file_fd >= 0:
                    os.close(file_fd)
    except OSError as error:
        raise ValueError('descriptor-safe renderer V2 artifact resolution '
                         'failed.') from error
    finally:
        if package_init_fd >= 0:
            os.close(package_init_fd)
        if package_fd >= 0:
            os.close(package_fd)
        if root_fd >= 0:
            os.close(root_fd)

    outer_raw = raw_artifacts['outer_template']
    node_raw = raw_artifacts['node_fragment']
    binding_raw = raw_artifacts['binding_schema']
    inventory_raw = raw_artifacts['config_access_inventory']
    outer = renderer_v1.ResolvedProviderKubernetesOuterTemplateArtifactV1(
        artifact_ref=outer_raw.artifact_ref,
        raw_artifact=outer_raw,
        template=renderer_v1.ProviderKubernetesOuterTemplateArtifactV1.
        from_value(outer_raw.canonical_value()))
    node = renderer_v1.ResolvedProviderKubernetesNodeFragmentArtifactV1(
        artifact_ref=node_raw.artifact_ref,
        raw_artifact=node_raw,
        fragment=renderer_v1.ProviderKubernetesNodeFragmentArtifactV1.
        from_value(node_raw.canonical_value()))
    binding = ResolvedProviderKubernetesBindingSchemaArtifactV2(
        artifact_ref=binding_raw.artifact_ref,
        raw_artifact=binding_raw,
        schema=ProviderKubernetesBindingSchemaArtifactV2(
            binding_raw.canonical_value()))
    inventory = ResolvedProviderKubernetesConfigAccessInventoryArtifactV2(
        artifact_ref=inventory_raw.artifact_ref,
        raw_artifact=inventory_raw,
        inventory=ProviderKubernetesConfigAccessInventoryV2.from_value(
            inventory_raw.canonical_value()))
    normalization = (
        provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1.
        from_verified_bytes(refs[4],
                            raw_bytes_by_role['admitted_object_normalization']))
    return ResolvedProviderKubernetesRendererArtifactSetV2(
        outer_template=outer,
        node_fragment=node,
        binding_schema=binding,
        config_access_inventory=inventory,
        admitted_object_normalization=normalization)


def _validate_renderer_identity_v2(
        renderer_input: ProviderKubernetesRendererInputV2) -> None:
    """Bind independent input identity to its compact native-V2 seed."""

    seed = renderer_input.seed
    name_basis = renderer_input.name_basis
    request_identity = seed.request_identity
    expected_basis = actions.ProviderWorkloadNameBasisV1(
        version=1,
        display_name=renderer_input.sky_cluster_name,
        frozen_user_hash=request_identity.frozen_user_hash,
        max_length=42,
        cluster_name_hash_length=8)
    if (renderer_input.sky_cluster_name != name_basis.display_name or
            name_basis.canonical_bytes != expected_basis.canonical_bytes):
        raise ValueError('Kubernetes renderer input V2 name basis does not '
                         'match its independent inputs.')
    sources = (
        renderer_input.retained_source.canonical_bytes,
        seed.renderer.source.canonical_bytes,
        seed.post_provision.job_submission.run_source.canonical_bytes,
    )
    if len(set(sources)) != 1:
        raise ValueError('Kubernetes renderer input V2 source copies are not '
                         'byte-equal.')
    if (renderer_input.retained_source.service_incarnation
            != renderer_input.resource_identity.service_incarnation):
        raise ValueError('Kubernetes renderer input V2 retained source does '
                         'not match the resource service incarnation.')

    if not request_identity.original_user.isascii():
        raise ValueError('Kubernetes renderer input V2 original user must be '
                         'ASCII.')
    cleaned_user = request_identity.original_user.lower()
    cleaned_user = re.sub(r'[^a-z0-9-_]', '', cleaned_user)
    cleaned_user = re.sub(r'^[0-9-]+', '', cleaned_user)
    cleaned_user = re.sub(r'-$', '', cleaned_user)[:63]
    if _EXPLICIT_USER_LABEL_RE.fullmatch(cleaned_user) is None:
        raise ValueError('Kubernetes renderer input V2 original user does not '
                         'project to a canonical label.')
    projected_identity = actions.ProviderKubernetesRequestIdentityV1(
        cleaned_user=cleaned_user,
        original_user=request_identity.original_user,
        frozen_user_hash=name_basis.frozen_user_hash)
    if projected_identity.canonical_bytes != request_identity.canonical_bytes:
        raise ValueError('Kubernetes renderer input V2 request identity is '
                         'not the frozen explicit-user projection.')

    provider_cluster_name = name_basis.provider_cluster_name
    workload_name = name_basis.workload_name
    cluster_uuid = str(renderer_input.sky_cluster_record_uuid)
    replica_uuid = str(renderer_input.resource_identity.replica_incarnation)
    expected_topology = (
        (actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE, f'{workload_name}-ssh',
         {
             'service-role': 'head_ssh_service',
             'skypilot-cluster-name': provider_cluster_name,
             'skypilot-user': request_identity.cleaned_user,
             'skypilot.co/cluster-record-uuid': cluster_uuid,
             'skypilot.co/serve-replica-incarnation': replica_uuid,
         }),
        (actions.ProviderObjectRoleV1.HEAD_SERVICE, workload_name, {
            'service-role': 'head_service',
            'skypilot-cluster-name': provider_cluster_name,
            'skypilot-user': request_identity.cleaned_user,
            'skypilot.co/cluster-record-uuid': cluster_uuid,
            'skypilot.co/serve-replica-incarnation': replica_uuid,
        }),
        (actions.ProviderObjectRoleV1.HEAD_POD, workload_name, {
            'component': workload_name,
            'skypilot-cluster-name': provider_cluster_name,
            'skypilot-user': request_identity.cleaned_user,
            'skypilot.co/cluster-record-uuid': cluster_uuid,
            'skypilot.co/serve-replica-incarnation': replica_uuid,
        }),
    )
    actual_topology = tuple((item.role, item.name, {
        label.key: label.value for label in item.labels
    }) for item in seed.topology.mutable_objects)
    if actual_topology != expected_topology:
        raise ValueError('Kubernetes renderer input V2 topology does not '
                         'match its independent identity.')


def _validate_common_context_v2(
    reference: actions.ProviderAuthorityWorkerCohortReferenceV1,
    config: actions.ProviderKubernetesConfigProjectionV1,
    scope: actions.ProviderKubernetesScopeV1,
    principals: actions.ProviderKubernetesPrincipalsV1,
    prerequisites: tuple[actions.ProviderKubernetesPrerequisiteV1, ...],
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
    *,
    extra_target_namespaces: tuple[str, ...] = (),
) -> dict[actions.ProviderKubernetesPrerequisiteRoleV1,
          actions.ProviderKubernetesPrerequisiteV1]:
    """Validate cohort, Namespace, principal, and prerequisite projections."""

    authority.validate_locked_action_spec_cohort_v2(reference, resolved_cohort)
    if not scope.in_cluster:
        raise ValueError('Kubernetes capsule V2 scope must be in-cluster.')
    target_namespaces = (
        scope.namespace,
        config.target_namespace,
        principals.workload.namespace,
        principals.caller_authorization.rules.namespace,
        *extra_target_namespaces,
    )
    if any(namespace != scope.namespace for namespace in target_namespaces):
        raise ValueError('Kubernetes capsule V2 target namespaces are not '
                         'byte-equal.')
    caller_scope = (
        scope.caller_service_account_namespace,
        scope.caller_service_account_name,
        scope.caller_service_account_uid,
    )
    caller_principal = (principals.caller.namespace, principals.caller.name,
                        principals.caller.uid)
    workload_scope = (
        scope.workload_service_account_namespace,
        scope.workload_service_account_name,
        scope.workload_service_account_uid,
    )
    workload_principal = (principals.workload.namespace,
                          principals.workload.name, principals.workload.uid)
    if (caller_scope != caller_principal or
            workload_scope != workload_principal):
        raise ValueError('Kubernetes capsule V2 principals do not match its '
                         'scope.')

    by_role = {item.role: item for item in prerequisites}
    authority_namespace = by_role[actions.ProviderKubernetesPrerequisiteRoleV1.
                                  AUTHORITY_RELEASE_NAMESPACE]
    target_namespace = by_role[
        actions.ProviderKubernetesPrerequisiteRoleV1.TARGET_NAMESPACE]
    kube_system_namespace = by_role[
        actions.ProviderKubernetesPrerequisiteRoleV1.KUBE_SYSTEM_NAMESPACE]
    manifest = resolved_cohort.manifest
    if (authority_namespace.name != manifest.namespace or
            authority_namespace.name != principals.caller.namespace or
            manifest.service_account_name != principals.caller.name or
            resolved_cohort.service_account_uid != principals.caller.uid):
        raise ValueError('Kubernetes capsule V2 cohort, authority Namespace, '
                         'and caller principal do not match.')
    if (target_namespace.name != scope.namespace or
            target_namespace.uid != scope.target_namespace_uid or
            kube_system_namespace.name != 'kube-system' or
            kube_system_namespace.uid != scope.kube_system_namespace_uid):
        raise ValueError('Kubernetes capsule V2 Namespace prerequisites do '
                         'not match its scope.')
    for role, principal in (
        (actions.ProviderKubernetesPrerequisiteRoleV1.CALLER_SERVICE_ACCOUNT,
         principals.caller),
        (actions.ProviderKubernetesPrerequisiteRoleV1.WORKLOAD_SERVICE_ACCOUNT,
         principals.workload),
    ):
        prerequisite = by_role[role]
        if type(prerequisite.spec) is not (
                actions.ProviderKubernetesServiceAccountPrerequisiteSpecV1):
            raise ValueError('Kubernetes capsule V2 ServiceAccount '
                             'prerequisite has an invalid spec.')
        if (prerequisite.spec.projection.canonical_bytes
                != principal.canonical_bytes):
            raise ValueError('Kubernetes capsule V2 ServiceAccount '
                             'prerequisite does not match its principal.')
    network_policy = by_role[
        actions.ProviderKubernetesPrerequisiteRoleV1.ENDPOINT_NETWORK_POLICY]
    if network_policy.namespace != scope.namespace:
        raise ValueError('Kubernetes capsule V2 NetworkPolicy Namespace does '
                         'not match its target.')
    return by_role


def _validate_launch_seed_context_v2(
    seed: ProviderKubernetesExecutionCapsuleSeedV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
) -> None:
    """Validate every seed projection using the external complete cohort."""

    if type(seed) is not ProviderKubernetesExecutionCapsuleSeedV2:
        raise TypeError('launch capsule seed V2 has an invalid type.')
    if type(resolved_cohort) is not authority.ProviderAuthorityWorkerCohortV2:
        raise TypeError('resolved cohort must be a parsed locked V2 cohort.')
    by_role = _validate_common_context_v2(seed.executor_cohort,
                                          seed.config_projection, seed.scope,
                                          seed.principals, seed.prerequisites,
                                          resolved_cohort)
    authority_namespace = by_role[actions.ProviderKubernetesPrerequisiteRoleV1.
                                  AUTHORITY_RELEASE_NAMESPACE]
    for projected in seed.endpoint.prerequisite_projection:
        if projected.canonical_bytes != by_role[projected.role].canonical_bytes:
            raise ValueError('launch capsule seed V2 endpoint prerequisite '
                             'projection is not byte-equal.')
    for caller in seed.endpoint.required_callers:
        if (caller.namespace != authority_namespace.name or
                caller.namespace_uid != authority_namespace.uid):
            raise ValueError('launch capsule seed V2 endpoint caller '
                             'Namespace does not match authority release.')

    config = seed.config_projection
    resources = seed.resources
    if (config.port_mode != resources.port_mode or
            seed.endpoint.mode != resources.port_mode or
            seed.endpoint.application_port != resources.application_port or
            seed.topology.application_port != resources.application_port or
            seed.topology.resources_ports != resources.resources_ports or
            seed.scheduling.node_count != seed.topology.node_count or
            seed.scheduling.use_spot):
        raise ValueError('launch capsule seed V2 resource, topology, endpoint, '
                         'and scheduling projections do not match.')
    scheduling_fields = ('runtime_class_name', 'priority_class_name', 'queue',
                         'kueue', 'dws', 'autoscaler', 'detected_network_type')
    if any(
            getattr(config, field) != getattr(seed.scheduling, field)
            for field in scheduling_fields):
        raise ValueError('launch capsule seed V2 scheduling projection does '
                         'not match config.')
    storage_fields = ('persistent_volumes', 'object_stores', 'file_mounts',
                      'workdir', 'fuse', 'docker_cache', 'auto_mounts')
    if any(
            getattr(config, field) != getattr(seed.storage, field)
            for field in storage_fields):
        raise ValueError('launch capsule seed V2 storage projection does not '
                         'match config.')
    metadata_fields = ('global_labels', 'custom_pod_config', 'custom_metadata')
    if any(
            getattr(config, field) != getattr(seed.metadata, field)
            for field in metadata_fields):
        raise ValueError('launch capsule seed V2 metadata projection does not '
                         'match config.')
    security_fields = ('tls_material', 'managed_secrets', 'task_secrets',
                       'service_account_bootstrap', 'rbac_bootstrap')
    if any(
            getattr(config, field) != getattr(seed.security, field)
            for field in security_fields):
        raise ValueError('launch capsule seed V2 security projection does not '
                         'match config.')

    cleaned_user = seed.request_identity.cleaned_user
    for topology_object in seed.topology.mutable_objects:
        labels = {label.key: label.value for label in topology_object.labels}
        expected_labels = {key: labels.get(key) for key in _IDENTITY_LABEL_KEYS}
        expected_labels['skypilot-user'] = cleaned_user
        if topology_object.role is actions.ProviderObjectRoleV1.HEAD_POD:
            expected_labels['component'] = topology_object.name
        else:
            expected_labels['service-role'] = topology_object.role.value
        if labels != expected_labels:
            raise ValueError('launch capsule seed V2 topology role labels are '
                             'not exact.')
    manifest_digest = resources.image.qualification.oci_manifest_digest
    if any(binding.workload_image_digest != manifest_digest
           for binding in seed.post_provision.runtime_artifacts):
        raise ValueError('launch capsule seed V2 runtime artifact image '
                         'digests do not match the workload image.')


def validate_provider_kubernetes_renderer_input_v2(
    renderer_input: ProviderKubernetesRendererInputV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
) -> ProviderKubernetesRendererInputV2:
    """Contextually validate the sole native-V2 launch renderer root."""

    if type(renderer_input) is not ProviderKubernetesRendererInputV2:
        raise TypeError('Kubernetes renderer input V2 has an invalid type.')
    _validate_renderer_identity_v2(renderer_input)
    _validate_launch_seed_context_v2(renderer_input.seed, resolved_cohort)
    return renderer_input


def _seed_from_capsule_v2(
    capsule: actions.ProviderKubernetesExecutionCapsuleV2,
) -> ProviderKubernetesExecutionCapsuleSeedV2:
    return ProviderKubernetesExecutionCapsuleSeedV2(
        version=2,
        implementation_contract=capsule.implementation_contract,
        executor_cohort=capsule.executor_cohort,
        config_projection=capsule.config_projection,
        config_projection_sha256=capsule.config_projection_sha256,
        scope=capsule.scope,
        principals=capsule.principals,
        prerequisites=capsule.prerequisites,
        request_identity=capsule.request_identity,
        resources=capsule.resources,
        renderer=capsule.renderer,
        post_provision=capsule.post_provision,
        endpoint=capsule.endpoint,
        scheduling=capsule.scheduling,
        storage=capsule.storage,
        metadata=capsule.metadata,
        security=capsule.security,
        topology=capsule.topology,
        mutation_contract=capsule.mutation_contract)


def _validate_requested_semantic_projection_v2(
    object_plan: actions.ProviderKubernetesObjectPlanV1,
    request_body: dict[str, Any],
) -> None:
    # This deletion models request-side normalization only.  Keep it on an
    # independent canonical tree so validation can never mutate its caller's
    # request-body operand.
    expected_semantic = json.loads(
        actions.canonical_json_bytes(request_body).decode('utf-8'))
    if object_plan.role is actions.ProviderObjectRoleV1.HEAD_SERVICE:
        del expected_semantic['spec']['clusterIP']
    if (object_plan.requested_semantic.canonical_bytes
            != actions.canonical_json_bytes(expected_semantic)):
        raise ValueError('launch capsule V2 requested semantic does not match '
                         'its request normalization.')


def _validate_head_pod_projection_v2(
    capsule: actions.ProviderKubernetesExecutionCapsuleV2,
    spec: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    if metadata.get('annotations') != {
            'skypilot-user': capsule.request_identity.original_user
    }:
        raise ValueError('launch capsule V2 Pod annotation does not match its '
                         'request identity.')
    if (spec['serviceAccount'] != capsule.principals.workload.name or
            spec['serviceAccountName'] != capsule.principals.workload.name):
        raise ValueError('launch capsule V2 Pod principal does not match.')
    container = spec['containers'][0]
    expected_resources = {
        'requests': {
            'cpu': capsule.resources.pod_cpu_request,
            'memory': capsule.resources.pod_memory_request,
        },
        'limits': {
            'cpu': capsule.resources.pod_cpu_limit,
            'memory': capsule.resources.pod_memory_limit,
        },
    }
    qualification = capsule.resources.image.qualification
    if (container['image'] != qualification.requested_reference or
            container['imagePullPolicy'] != capsule.resources.image_pull_policy
            or container['resources'] != expected_resources):
        raise ValueError('launch capsule V2 Pod image or resources do not '
                         'match.')
    if capsule.post_provision.management_port != '46590':
        raise ValueError('launch capsule V2 management port does not match '
                         'the Pod request.')


def validate_provider_kubernetes_execution_capsule_context_v2(
    capsule: actions.ProviderKubernetesExecutionCapsuleV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
) -> actions.ProviderKubernetesExecutionCapsuleV2:
    """Contextually revalidate one completed native-V2 launch capsule."""

    if type(capsule) is not actions.ProviderKubernetesExecutionCapsuleV2:
        raise TypeError('Kubernetes launch capsule V2 has an invalid type.')
    seed = _seed_from_capsule_v2(capsule)
    _validate_launch_seed_context_v2(seed, resolved_cohort)
    normalization = capsule.renderer.admitted_object_normalization.canonical_bytes
    if any(item.normalization_profile.canonical_bytes != normalization
           for item in capsule.objects):
        raise ValueError('launch capsule V2 object normalization references '
                         'are not byte-equal to the renderer.')
    _validate_common_context_v2(capsule.executor_cohort,
                                capsule.config_projection,
                                capsule.scope,
                                capsule.principals,
                                capsule.prerequisites,
                                resolved_cohort,
                                extra_target_namespaces=tuple(
                                    item.namespace for item in capsule.objects))

    cleaned_user = capsule.request_identity.cleaned_user
    for topology_object, object_plan in zip(capsule.topology.mutable_objects,
                                            capsule.objects):
        if (topology_object.role is not object_plan.role or
                topology_object.kind is not object_plan.kind or
                topology_object.name != object_plan.name):
            raise ValueError('launch capsule V2 object plan does not match its '
                             'topology entry.')
        topology_labels = {
            label.key: label.value for label in topology_object.labels
        }
        expected_identity_labels = tuple(
            (key, topology_labels.get(key)) for key in _IDENTITY_LABEL_KEYS)
        actual_identity_labels = tuple(
            (label.key, label.value)
            for label in object_plan.required_identity_labels)
        if actual_identity_labels != expected_identity_labels:
            raise ValueError('launch capsule V2 object identity labels do not '
                             'match topology.')
        actions.ValidatedKubernetesServeThreeObjectBodyV1(
            role=object_plan.role, body=object_plan.request_body)
        body = object_plan.request_body.canonical_value()
        metadata = body['metadata']
        if (metadata['labels'] != topology_labels or
                topology_labels.get('skypilot-user') != cleaned_user):
            raise ValueError('launch capsule V2 request labels do not match '
                             'topology and request identity.')
        if object_plan.role is actions.ProviderObjectRoleV1.HEAD_POD:
            _validate_head_pod_projection_v2(capsule, body['spec'], metadata)
        _validate_requested_semantic_projection_v2(object_plan, body)
    return capsule


def _validate_down_input_context_v2(
    down_input: ProviderKubernetesDownExecutionCapsuleInputV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
    cleanup_target: actions.ProviderKubernetesCleanupTargetV1,
) -> None:
    if type(down_input) is not ProviderKubernetesDownExecutionCapsuleInputV2:
        raise TypeError('Kubernetes down capsule input V2 has an invalid type.')
    if type(cleanup_target) is not actions.ProviderKubernetesCleanupTargetV1:
        raise TypeError('rederived cleanup target has an invalid type.')
    _validate_common_context_v2(
        down_input.executor_cohort,
        down_input.config_projection,
        down_input.scope,
        down_input.principals,
        down_input.prerequisites,
        resolved_cohort,
        extra_target_namespaces=tuple(
            item.plan.namespace for item in cleanup_target.objects))
    if (cleanup_target.handle is not None and
            cleanup_target.handle.provider_config.scope_sha256
            != down_input.scope.sha256):
        raise ValueError('down capsule input V2 cleanup handle scope does not '
                         'match the current scope.')


def validate_provider_kubernetes_down_execution_capsule_context_v2(
    capsule: actions.ProviderKubernetesDownExecutionCapsuleV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
    rederived_cleanup_target: actions.ProviderKubernetesCleanupTargetV1,
) -> actions.ProviderKubernetesDownExecutionCapsuleV2:
    """Validate down context and the rederiver-owned target byte-for-byte."""

    if type(capsule) is not actions.ProviderKubernetesDownExecutionCapsuleV2:
        raise TypeError('Kubernetes down capsule V2 has an invalid type.')
    if (type(rederived_cleanup_target)
            is not actions.ProviderKubernetesCleanupTargetV1):
        raise TypeError('rederived cleanup target has an invalid type.')
    if (capsule.cleanup_target.canonical_bytes
            != rederived_cleanup_target.canonical_bytes or
            capsule.cleanup_target_sha256 != rederived_cleanup_target.sha256):
        raise ValueError('down capsule V2 cleanup target is not byte-equal to '
                         'the shared rederiver output.')
    down_input = ProviderKubernetesDownExecutionCapsuleInputV2(
        version=2,
        implementation_contract=capsule.implementation_contract,
        executor_cohort=capsule.executor_cohort,
        config_projection=capsule.config_projection,
        config_projection_sha256=capsule.config_projection_sha256,
        scope=capsule.scope,
        principals=capsule.principals,
        prerequisites=capsule.prerequisites,
        mutation_contract=capsule.mutation_contract)
    _validate_down_input_context_v2(down_input, resolved_cohort,
                                    rederived_cleanup_target)
    return capsule


def _validate_config_access_inventory_v2(
    resolved_artifacts: ResolvedProviderKubernetesRendererArtifactSetV2,
) -> None:
    """Pin the exact four native roots to the code-final V2 inventory."""

    if type(resolved_artifacts
           ) is not ResolvedProviderKubernetesRendererArtifactSetV2:
        raise TypeError('resolved renderer V2 artifacts have an invalid type.')
    inventory = resolved_artifacts.config_access_inventory.inventory
    if type(inventory) is not ProviderKubernetesConfigAccessInventoryV2:
        raise TypeError('resolved renderer config-access inventory is not V2.')
    # Importing this exact module lazily is the one unavoidable cycle breaker:
    # representability imports the renderer's typed V2 construction roots.
    # The literal module/name pair is independently frozen below and by the
    # source/inventory parity tests; no caller controls either value.
    representability = importlib.import_module(
        'sky.serve.resource_action_representability')
    actual_roots = (
        ('launch_capsule_constructor',
         construct_provider_kubernetes_execution_capsule_v2),
        ('down_capsule_constructor',
         construct_provider_kubernetes_down_execution_capsule_v2),
        ('cleanup_target_rederiver',
         cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2),
        ('representability_enumerator',
         representability.enumerate_provider_resource_action_representability_v2
        ),
    )
    if any(type(root) is not types.FunctionType for _, root in actual_roots):
        raise TypeError('renderer V2 config-access root is not an exact '
                        'Python function.')
    actual_entrypoints = tuple({
        'sequence': sequence,
        'role': role,
        'qualified_name': f'{root.__module__}.{root.__name__}',
    } for sequence, (role, root) in enumerate(actual_roots))
    raw_entrypoints = inventory.canonical_value()['entrypoints']
    if raw_entrypoints != list(actual_entrypoints):
        raise ValueError('renderer V2 config-access entrypoints do not pin '
                         'the exact four native roots.')


def _resolve_provider_kubernetes_bindings_v2(
    renderer_input: ProviderKubernetesRendererInputV2,
    resolved_artifacts: ResolvedProviderKubernetesRendererArtifactSetV2,
) -> actions.ResolvedProviderKubernetesBindingSetV1:
    if type(renderer_input) is not ProviderKubernetesRendererInputV2:
        raise TypeError('Kubernetes renderer input V2 has an invalid type.')
    if type(resolved_artifacts
           ) is not ResolvedProviderKubernetesRendererArtifactSetV2:
        raise TypeError('resolved renderer V2 artifacts have an invalid type.')
    schema_rows = resolved_artifacts.binding_schema.schema.canonical_value(
    )['bindings']
    topology = renderer_input.seed.topology.mutable_objects
    head_ssh_labels = {label.key: label.value for label in topology[0].labels}
    head_labels = {label.key: label.value for label in topology[1].labels}
    head_pod_labels = {label.key: label.value for label in topology[2].labels}
    selector = {key: head_pod_labels[key] for key in _SELECTOR_KEYS}
    values: tuple[Any, ...] = (
        head_labels,
        topology[1].name,
        head_pod_labels,
        topology[2].name,
        selector,
        head_ssh_labels,
        topology[0].name,
        renderer_input.seed.resources.image_pull_policy,
        renderer_input.seed.request_identity.original_user,
        renderer_input.seed.resources.pod_cpu_limit,
        renderer_input.seed.resources.pod_cpu_request,
        renderer_input.seed.resources.pod_memory_limit,
        renderer_input.seed.resources.pod_memory_request,
        str(renderer_input.resource_identity.replica_id),
        renderer_input.seed.scope.namespace,
        renderer_input.seed.resources.image.qualification.requested_reference,
        renderer_input.seed.principals.workload.name,
    )
    bindings = tuple(
        actions.ResolvedProviderKubernetesBindingV1(
            sequence=index,
            name=row['name'],
            json_type=row['json_type'],
            value=actions.CanonicalJsonValue.from_value(value))
        for index, (row, value) in enumerate(zip(schema_rows, values)))
    return actions.ResolvedProviderKubernetesBindingSetV1(
        version=1,
        contract='skypilot.serve.prebooted-direct-pod.resolved-bindings.v1',
        bindings=bindings)


def _render_provider_kubernetes_objects_v2(
    renderer_input: ProviderKubernetesRendererInputV2,
    resolved_artifacts: ResolvedProviderKubernetesRendererArtifactSetV2,
) -> tuple[actions.CanonicalJsonObject, ...]:
    """Substitute the exact typed bindings into the unchanged leaf templates."""

    resolved_bindings = _resolve_provider_kubernetes_bindings_v2(
        renderer_input, resolved_artifacts)
    binding_values = {
        binding.name: binding.value for binding in resolved_bindings.bindings
    }
    schema_rows = resolved_artifacts.binding_schema.schema.canonical_value(
    )['bindings']
    expected_counts = {row['name']: len(row['targets']) for row in schema_rows}
    actual_counts = {name: 0 for name in expected_counts}
    templates = {
        'outer_template':
            resolved_artifacts.outer_template.template.canonical_value(),
        'node_fragment':
            resolved_artifacts.node_fragment.fragment.canonical_value(),
    }
    for artifact in templates.values():
        root = [artifact]
        stack: list[tuple[Any, Any]] = [(root, 0)]
        while stack:
            parent, key = stack.pop()
            item = parent[key]
            if type(item) is dict and set(item) == {'$binding'}:
                binding_name = item['$binding']
                if (type(binding_name) is not str or
                        binding_name not in binding_values):
                    raise ValueError('renderer V2 template contains an '
                                     'unlisted binding marker.')
                parent[key] = binding_values[binding_name].canonical_value()
                actual_counts[binding_name] += 1
                continue
            if type(item) is dict:
                if '$binding' in item:
                    raise ValueError('renderer V2 marker is not a whole value.')
                stack.extend(
                    (item, child_key) for child_key in reversed(tuple(item)))
            elif type(item) is list:
                stack.extend(
                    (item, index) for index in reversed(range(len(item))))
        if root[0] is not artifact:
            raise ValueError('renderer V2 artifact root cannot be a marker.')
    if actual_counts != expected_counts:
        raise ValueError('renderer V2 marker use does not match its binding '
                         'schema.')
    outer = templates['outer_template']
    node = templates['node_fragment']
    service_templates = outer['service_templates']
    bodies = (service_templates[0]['body'], service_templates[1]['body'],
              node['body'])
    return tuple(
        actions.CanonicalJsonObject.from_value(body) for body in bodies)


def _validate_kubernetes_serve_three_object_body_v2(
    renderer_input: ProviderKubernetesRendererInputV2,
    rendered_bodies: tuple[actions.CanonicalJsonObject, ...],
) -> tuple[actions.ValidatedKubernetesServeThreeObjectBodyV1, ...]:
    if type(renderer_input) is not ProviderKubernetesRendererInputV2:
        raise TypeError('Kubernetes renderer input V2 has an invalid type.')
    if (type(rendered_bodies) is not tuple or len(rendered_bodies) != 3 or any(
            type(body) is not actions.CanonicalJsonObject
            for body in rendered_bodies)):
        raise TypeError('rendered V2 bodies must be the exact three-object '
                        'tuple.')
    topology = renderer_input.seed.topology.mutable_objects
    roles = tuple(
        entry.role for entry in actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
    validated = tuple(
        actions.ValidatedKubernetesServeThreeObjectBodyV1(role=role, body=body)
        for role, body in zip(roles, rendered_bodies))
    pod_labels = {label.key: label.value for label in topology[2].labels}
    expected_selector = {key: pod_labels[key] for key in _SELECTOR_KEYS}
    for index, item in enumerate(validated):
        body = item.body.canonical_value()
        metadata = body['metadata']
        expected_labels = {
            label.key: label.value for label in topology[index].labels
        }
        if (metadata['namespace'] != renderer_input.seed.scope.namespace or
                metadata['name'] != topology[index].name or
                metadata['labels'] != expected_labels):
            raise ValueError('rendered V2 body identity does not match input '
                             'topology.')
        if item.role is actions.ProviderObjectRoleV1.HEAD_POD:
            if metadata['annotations'] != {
                    'skypilot-user':
                        renderer_input.seed.request_identity.original_user
            }:
                raise ValueError('rendered V2 Pod annotation does not match '
                                 'request identity.')
            spec = body['spec']
            if (spec['serviceAccount']
                    != renderer_input.seed.principals.workload.name or
                    spec['serviceAccountName']
                    != renderer_input.seed.principals.workload.name):
                raise ValueError('rendered V2 Pod service account does not '
                                 'match the workload principal.')
            container = spec['containers'][0]
            expected_resources = {
                'limits': {
                    'cpu': renderer_input.seed.resources.pod_cpu_limit,
                    'memory': renderer_input.seed.resources.pod_memory_limit,
                },
                'requests': {
                    'cpu': renderer_input.seed.resources.pod_cpu_request,
                    'memory': renderer_input.seed.resources.pod_memory_request,
                },
            }
            matching_environment = [
                entry for entry in container['env']
                if entry['name'] == 'SKYPILOT_SERVE_REPLICA_ID'
            ]
            if (len(matching_environment) != 1 or
                    matching_environment[0]['value'] != str(
                        renderer_input.resource_identity.replica_id)):
                raise ValueError('rendered V2 Pod replica environment does '
                                 'not match the resource identity.')
            if (container['image'] != renderer_input.seed.resources.image.
                    qualification.requested_reference or
                    container['imagePullPolicy']
                    != renderer_input.seed.resources.image_pull_policy or
                    container['resources'] != expected_resources):
                raise ValueError('rendered V2 Pod resource projection does '
                                 'not match input.')
        elif body['spec']['selector'] != expected_selector:
            raise ValueError('rendered V2 Service selector does not match the '
                             'head Pod labels.')
    return validated


def _build_provider_kubernetes_object_plans_v2(
    validated_bodies: tuple[actions.ValidatedKubernetesServeThreeObjectBodyV1,
                            ...],
    request_normalizations: tuple[
        actions.ProviderKubernetesRequestNormalizationV1, ...],
    normalization_artifact: (
        provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1),
) -> tuple[actions.ProviderKubernetesObjectPlanV1, ...]:
    role_map = actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
    if (type(validated_bodies) is not tuple or
            len(validated_bodies) != len(role_map) or any(
                type(body)
                is not actions.ValidatedKubernetesServeThreeObjectBodyV1
                for body in validated_bodies)):
        raise TypeError('validated V2 bodies must be the exact role tuple.')
    if (type(request_normalizations) is not tuple or
            len(request_normalizations) != len(role_map) or any(
                type(item)
                is not actions.ProviderKubernetesRequestNormalizationV1
                for item in request_normalizations)):
        raise TypeError('V2 request normalizations must be the exact role '
                        'tuple.')
    if type(normalization_artifact) is not (
            provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1
    ):
        raise TypeError('V2 normalization artifact has an invalid type.')
    if tuple(body.role for body in validated_bodies) != tuple(
            entry.role for entry in role_map):
        raise ValueError('validated V2 bodies are not in role order.')
    expected_intents = ('allocate_single_stack_cluster_ip',
                        'headless_single_stack', 'schedule_one_node')
    if tuple(item.requested_allocation_intent
             for item in request_normalizations) != expected_intents:
        raise ValueError('V2 request normalizations have invalid intents.')
    comparison_contract = normalization_artifact.contract.canonical_value(
    )['comparison_contract']
    if comparison_contract != 'kubernetes_admitted_object_v1':
        raise ValueError('V2 object comparison contract is unsupported.')

    plans = []
    for entry, body, normalization in zip(role_map, validated_bodies,
                                          request_normalizations):
        raw_body = body.body.canonical_value()
        expected_semantic = body.body.canonical_value()
        if entry.role is actions.ProviderObjectRoleV1.HEAD_SERVICE:
            del expected_semantic['spec']['clusterIP']
        if (normalization.requested_semantic.canonical_bytes
                != actions.canonical_json_bytes(expected_semantic)):
            raise ValueError('V2 request normalization semantic does not '
                             'match its validated body.')
        metadata = raw_body['metadata']
        labels = metadata['labels']
        identity_labels = tuple(
            actions.ProviderLabelV1(key=key, value=labels[key])
            for key in _IDENTITY_LABEL_KEYS)
        plans.append(
            actions.ProviderKubernetesObjectPlanV1(
                sequence=entry.plan_sequence,
                role=entry.role,
                api_version='v1',
                kind=entry.kind,
                namespace=metadata['namespace'],
                name=metadata['name'],
                required_identity_labels=identity_labels,
                request_body=body.body,
                request_body_sha256=body.body.sha256,
                requested_semantic=normalization.requested_semantic,
                requested_semantic_sha256=normalization.requested_semantic.
                sha256,
                comparison_contract=comparison_contract,
                normalization_profile=normalization_artifact.artifact_ref))
    return tuple(plans)


def _assemble_and_revalidate_provider_kubernetes_execution_capsule_v2(
    renderer_input: ProviderKubernetesRendererInputV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
    object_plans: tuple[actions.ProviderKubernetesObjectPlanV1, ...],
) -> actions.ProviderKubernetesExecutionCapsuleV2:
    if type(renderer_input) is not ProviderKubernetesRendererInputV2:
        raise TypeError('Kubernetes renderer input V2 has an invalid type.')
    role_map = actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
    if (type(object_plans) is not tuple or len(object_plans) != len(role_map) or
            any(
                type(plan) is not actions.ProviderKubernetesObjectPlanV1
                for plan in object_plans)):
        raise TypeError('V2 object plans must be the exact three-role tuple.')
    expected = tuple(
        (entry.plan_sequence, entry.role, entry.kind) for entry in role_map)
    actual = tuple(
        (plan.sequence, plan.role, plan.kind) for plan in object_plans)
    if actual != expected:
        raise ValueError('V2 object plans are not in exact role order.')
    seed = renderer_input.seed
    capsule = actions.ProviderKubernetesExecutionCapsuleV2(
        version=2,
        implementation_contract=seed.implementation_contract,
        executor_cohort=seed.executor_cohort,
        config_projection=seed.config_projection,
        config_projection_sha256=seed.config_projection_sha256,
        scope=seed.scope,
        principals=seed.principals,
        prerequisites=seed.prerequisites,
        request_identity=seed.request_identity,
        resources=seed.resources,
        renderer=seed.renderer,
        objects=object_plans,
        post_provision=seed.post_provision,
        endpoint=seed.endpoint,
        scheduling=seed.scheduling,
        storage=seed.storage,
        metadata=seed.metadata,
        security=seed.security,
        topology=seed.topology,
        mutation_contract=seed.mutation_contract)
    return validate_provider_kubernetes_execution_capsule_context_v2(
        capsule, resolved_cohort)


def construct_provider_kubernetes_execution_capsule_v2(
    renderer_input: ProviderKubernetesRendererInputV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
) -> actions.ProviderKubernetesExecutionCapsuleV2:
    """Run the native nonrecursive V2 launch construction graph."""

    validated_input = validate_provider_kubernetes_renderer_input_v2(
        renderer_input, resolved_cohort)
    resolved_artifacts = _read_renderer_artifacts_v2(validated_input)
    _validate_config_access_inventory_v2(resolved_artifacts)
    rendered_bodies = _render_provider_kubernetes_objects_v2(
        validated_input, resolved_artifacts)
    validated_bodies = _validate_kubernetes_serve_three_object_body_v2(
        validated_input, rendered_bodies)
    roles = tuple(
        entry.role for entry in actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
    request_normalizations = tuple(
        provider_artifacts.normalize_kubernetes_request_object_v1(
            role, body, resolved_artifacts.admitted_object_normalization)
        for role, body in zip(roles, validated_bodies))
    object_plans = _build_provider_kubernetes_object_plans_v2(
        validated_bodies, request_normalizations,
        resolved_artifacts.admitted_object_normalization)
    return _assemble_and_revalidate_provider_kubernetes_execution_capsule_v2(
        validated_input, resolved_cohort, object_plans)


def construct_provider_kubernetes_down_execution_capsule_v2(
    down_input: ProviderKubernetesDownExecutionCapsuleInputV2,
    resolved_cohort: authority.ProviderAuthorityWorkerCohortV2,
    cleanup_rederivation_input: (
        cleanup_v2.ProviderKubernetesCleanupRederivationInputV2),
) -> actions.ProviderKubernetesDownExecutionCapsuleV2:
    """Construct a V2 down capsule through the sole cleanup rederiver."""

    rederived_cleanup_target = (
        cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(
            cleanup_rederivation_input))
    _validate_down_input_context_v2(down_input, resolved_cohort,
                                    rederived_cleanup_target)
    capsule = actions.ProviderKubernetesDownExecutionCapsuleV2(
        version=2,
        implementation_contract=down_input.implementation_contract,
        executor_cohort=down_input.executor_cohort,
        config_projection=down_input.config_projection,
        config_projection_sha256=down_input.config_projection_sha256,
        scope=down_input.scope,
        principals=down_input.principals,
        prerequisites=down_input.prerequisites,
        cleanup_target=rederived_cleanup_target,
        cleanup_target_sha256=rederived_cleanup_target.sha256,
        mutation_contract=down_input.mutation_contract)
    return validate_provider_kubernetes_down_execution_capsule_context_v2(
        capsule, resolved_cohort, rederived_cleanup_target)
