"""Preflight-only authority-worker cohort bootstrap.

This module deliberately owns no request queue, provider mutation, or claim
configuration.  Its Kubernetes facade can only read the calling Pod, its two
controller owners, and the cohort ServiceAccount.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
import copy
import dataclasses
import datetime
import os
import re
import threading
import time
from typing import Any, Protocol
import uuid

from sky.serve import resource_action_state
from sky.serve import resource_actions as actions
from sky.server.requests import postgres as request_postgres
from sky.server.requests import resource_actions as kernel_actions

POD_NAME_ENV_VAR = 'SKYPILOT_POD_NAME'
POD_NAMESPACE_ENV_VAR = 'SKYPILOT_POD_NAMESPACE'
POD_UID_ENV_VAR = 'SKYPILOT_POD_UID'
MANIFEST_PATH = '/etc/skypilot/resource-action-authority/manifest.json'
QUALIFICATION_PATH = (
    '/etc/skypilot/resource-action-authority/qualification.json')

_DNS_LABEL_RE = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
_EVIDENCE_LEASE = datetime.timedelta(minutes=5)
_DEFAULT_RECONCILE_INTERVAL_SECONDS = 30.0


class BootstrapUnavailable(RuntimeError):
    """Live evidence is temporarily unavailable and admission stays closed."""


class BootstrapInvariantViolation(RuntimeError):
    """Immutable cohort evidence drifted and the process must fail closed."""


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _required_text(value: Any, *, name: str) -> str:
    if type(value) is not str or not value or '\x00' in value:
        raise BootstrapInvariantViolation(f'{name} must be nonempty text.')
    return value


def _canonical_timestamp(value: datetime.datetime) -> str:
    if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
            value.utcoffset() is None):
        raise TypeError('database_now must be a timezone-aware datetime.')
    return value.astimezone(
        datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _parse_timestamp(value: str) -> datetime.datetime:
    try:
        parsed = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except (TypeError, ValueError) as e:
        raise BootstrapInvariantViolation(
            'Stored worker timestamp is not canonical UTC text.') from e
    return parsed.replace(tzinfo=datetime.timezone.utc)


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerPodIdentity:
    """The only process identity admitted from the downward API."""

    name: str
    namespace: str
    uid: str

    @classmethod
    def from_environment(
            cls,
            environ: Mapping[str, str] | None = None
    ) -> AuthorityWorkerPodIdentity:
        source = os.environ if environ is None else environ
        name = _required_text(source.get(POD_NAME_ENV_VAR),
                              name=POD_NAME_ENV_VAR)
        namespace = _required_text(source.get(POD_NAMESPACE_ENV_VAR),
                                   name=POD_NAMESPACE_ENV_VAR)
        uid = _required_text(source.get(POD_UID_ENV_VAR), name=POD_UID_ENV_VAR)
        if _DNS_LABEL_RE.fullmatch(name) is None:
            raise BootstrapInvariantViolation(
                f'{POD_NAME_ENV_VAR} must be a DNS label.')
        if _DNS_LABEL_RE.fullmatch(namespace) is None:
            raise BootstrapInvariantViolation(
                f'{POD_NAMESPACE_ENV_VAR} must be a DNS label.')
        try:
            parsed_uid = uuid.UUID(uid)
        except ValueError as e:
            raise BootstrapInvariantViolation(
                f'{POD_UID_ENV_VAR} must be a canonical UUID.') from e
        if str(parsed_uid) != uid:
            raise BootstrapInvariantViolation(
                f'{POD_UID_ENV_VAR} must be a canonical UUID.')
        return cls(name=name, namespace=namespace, uid=uid)


def configure_server_instance_id_from_pod_uid(
    environ: MutableMapping[str, str] | None = None,
) -> AuthorityWorkerPodIdentity:
    """Validate downward identity and derive the PostgreSQL instance UUID."""
    target = os.environ if environ is None else environ
    identity = AuthorityWorkerPodIdentity.from_environment(target)
    existing = target.get(request_postgres.SERVER_INSTANCE_ID_ENV_VAR)
    if existing is not None and existing != identity.uid:
        raise BootstrapInvariantViolation(
            'Authority server instance ID differs from the Pod UID.')
    target[request_postgres.SERVER_INSTANCE_ID_ENV_VAR] = identity.uid
    return identity


@dataclasses.dataclass(frozen=True)
class KubernetesAuthorityWorkerObjects:
    pod: Any
    replica_set: Any
    deployment: Any
    service_account: Any


@dataclasses.dataclass(frozen=True)
class DeploymentSnapshot:
    """Fields that must remain exact throughout one P2a cohort."""

    uid: str
    resource_version: str
    generation: int
    observed_generation: int
    spec_replicas: int
    status_replicas: int
    updated_replicas: int
    ready_replicas: int
    available_replicas: int
    unavailable_replicas: int

    @classmethod
    def from_object(cls, deployment: Any) -> DeploymentSnapshot:
        metadata = _field(deployment, 'metadata')
        spec = _field(deployment, 'spec')
        status = _field(deployment, 'status')
        if metadata is None or spec is None or status is None:
            raise BootstrapUnavailable(
                'Deployment metadata/spec/status is incomplete.')

        def integer(parent: Any,
                    field_name: str,
                    *,
                    zero_if_absent: bool = False) -> int:
            raw = _field(parent, field_name)
            if raw is None and zero_if_absent:
                raw = 0
            if type(raw) is not int or raw < 0:
                raise BootstrapUnavailable(
                    f'Deployment {field_name} is not a nonnegative integer.')
            return raw

        snapshot = cls(uid=_required_text(_field(metadata, 'uid'),
                                          name='deployment.metadata.uid'),
                       resource_version=_required_text(
                           _field(metadata, 'resource_version'),
                           name='deployment.metadata.resourceVersion'),
                       generation=integer(metadata, 'generation'),
                       observed_generation=integer(status,
                                                   'observed_generation'),
                       spec_replicas=integer(spec, 'replicas'),
                       status_replicas=integer(status, 'replicas'),
                       updated_replicas=integer(status, 'updated_replicas'),
                       ready_replicas=integer(status, 'ready_replicas'),
                       available_replicas=integer(status, 'available_replicas'),
                       unavailable_replicas=integer(status,
                                                    'unavailable_replicas',
                                                    zero_if_absent=True))
        strategy = _field(spec, 'strategy')
        if (_field(strategy, 'type') != 'Recreate' or
                _field(strategy, 'rolling_update') is not None):
            raise BootstrapInvariantViolation(
                'Authority Deployment strategy is not exact Recreate.')
        if (snapshot.generation <= 0 or
                snapshot.observed_generation != snapshot.generation or
                snapshot.spec_replicas != 2 or snapshot.status_replicas != 2 or
                snapshot.updated_replicas != 2 or
                snapshot.ready_replicas != 2 or
                snapshot.available_replicas != 2 or
                snapshot.unavailable_replicas != 0):
            raise BootstrapUnavailable(
                'Authority Deployment is not one exact current 2/2 snapshot.')
        return snapshot

    def matches_registration(
            self, registration: actions.ProviderAuthorityWorkerRegistrationV1
    ) -> bool:
        worker = registration.worker
        comparisons = {
            'deployment_spec_replicas': self.spec_replicas,
            'deployment_status_replicas': self.status_replicas,
            'deployment_updated_replicas': self.updated_replicas,
            'deployment_status_observed_generation': self.observed_generation,
            'deployment_ready_replicas': self.ready_replicas,
            'deployment_available_replicas': self.available_replicas,
            'deployment_unavailable_replicas': self.unavailable_replicas,
        }
        return (worker.deployment_uid == self.uid and
                worker.deployment_resource_version == self.resource_version and
                worker.deployment_generation == self.generation and
                worker.deployment_observed_generation
                == self.observed_generation and all(
                    getattr(registration, name) == expected
                    for name, expected in comparisons.items()))


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerObservation:
    """One DB-clock-bound projection of the caller's live objects."""

    cohort_identity: actions.WorkerCohortIdentityV1
    registration: actions.ProviderAuthorityWorkerRegistrationV1
    deployment_snapshot: DeploymentSnapshot

    def __post_init__(self) -> None:
        if not isinstance(self.cohort_identity, actions.WorkerCohortIdentityV1):
            raise TypeError('cohort_identity has an invalid type.')
        if not isinstance(self.registration,
                          actions.ProviderAuthorityWorkerRegistrationV1):
            raise TypeError('registration has an invalid type.')
        self.registration.worker.validate_for_cohort(self.cohort_identity)
        if not self.deployment_snapshot.matches_registration(self.registration):
            raise BootstrapInvariantViolation(
                'Projected worker registration differs from its Deployment.')


class LiveObservationProjector(Protocol):

    def __call__(
        self,
        manifest: actions.ProviderAuthorityWorkerCohortManifestV1,
        database_now: datetime.datetime,
        objects: KubernetesAuthorityWorkerObjects,
        deployment_snapshot: DeploymentSnapshot,
    ) -> AuthorityWorkerObservation:
        ...


class LiveTemplateValidator(Protocol):

    def __call__(
        self,
        manifest: actions.ProviderAuthorityWorkerCohortManifestV1,
        objects: KubernetesAuthorityWorkerObjects,
    ) -> None:
        ...


def _controller_owner(metadata: Any, *, kind: str,
                      expected_kind: str) -> tuple[str, str]:
    references = _field(metadata, 'owner_references')
    if not isinstance(references, (list, tuple)) or len(references) != 1:
        raise BootstrapInvariantViolation(
            f'{kind} must have one closed controller owner reference.')
    owner = references[0]
    if _field(owner, 'controller') is not True:
        raise BootstrapInvariantViolation(
            f'{kind} must have exactly one controller owner reference.')
    if (_field(owner, 'api_version') != 'apps/v1' or
            _field(owner, 'kind') != expected_kind):
        raise BootstrapInvariantViolation(
            f'{kind} controller owner has an invalid type.')
    return (_required_text(_field(owner, 'name'), name=f'{kind}.owner.name'),
            _required_text(_field(owner, 'uid'), name=f'{kind}.owner.uid'))


def _metadata(obj: Any, *, kind: str) -> Any:
    metadata = _field(obj, 'metadata')
    if metadata is None:
        raise BootstrapInvariantViolation(f'{kind} metadata is absent.')
    if _field(metadata, 'deletion_timestamp') is not None:
        raise BootstrapUnavailable(f'{kind} is deleting.')
    return metadata


def _named_item(items: Any, name: str, *, kind: str) -> Any:
    if not isinstance(items, (list, tuple)):
        raise BootstrapInvariantViolation(f'{kind} inventory is absent.')
    matches = [item for item in items if _field(item, 'name') == name]
    if len(matches) != 1:
        raise BootstrapInvariantViolation(
            f'{kind} inventory must contain exactly one {name!r}.')
    return matches[0]


def _runtime_image_identity(
    qualification: actions.ProviderOCIImageQualificationV1,
    raw_image_id: str,
) -> actions.ProviderRuntimeImageIdentityV1:
    raw_image_id = _required_text(raw_image_id,
                                  name='container_status.image_id')
    if raw_image_id.startswith('containerd://'):
        scheme = 'containerd'
        digest = raw_image_id[len('containerd://'):]
        contract = 'qualified_oci_config_digest_v1'
    elif raw_image_id.startswith('cri-o://'):
        scheme = 'cri-o'
        digest = raw_image_id[len('cri-o://'):]
        contract = 'qualified_oci_config_digest_v1'
    elif raw_image_id.startswith('docker-pullable://'):
        scheme = 'docker-pullable'
        digest = raw_image_id.rsplit('@', 1)[-1]
        contract = 'qualified_oci_manifest_digest_v1'
    else:
        scheme = 'oci-reference'
        digest = raw_image_id.rsplit('@', 1)[-1]
        contract = 'qualified_oci_manifest_digest_v1'
    try:
        return actions.ProviderRuntimeImageIdentityV1(
            raw_image_id=raw_image_id,
            runtime_image_id_scheme=scheme,
            runtime_image_id_digest=digest,
            qualified_oci_manifest_digest=qualification.oci_manifest_digest,
            qualified_oci_config_digest=qualification.oci_config_digest,
            qualification_artifact_sha256=(
                qualification.qualification_artifact.sha256),
            runtime_id_contract=contract)
    except (TypeError, ValueError) as e:
        raise BootstrapInvariantViolation(
            'Runtime image ID does not match the qualified OCI identity.'
        ) from e


class DefaultAuthorityWorkerLiveProjector:
    """Project typed cohort evidence after the closed template validator."""

    def __init__(self, template_validator: LiveTemplateValidator) -> None:
        self._template_validator = template_validator

    @staticmethod
    def _require_api_identity(obj: Any, *, api_version: str, kind: str) -> None:
        if (_field(obj, 'api_version') != api_version or
                _field(obj, 'kind') != kind):
            raise BootstrapInvariantViolation(
                f'Live {kind} TypeMeta is not exact.')

    def __call__(
        self,
        manifest: actions.ProviderAuthorityWorkerCohortManifestV1,
        database_now: datetime.datetime,
        objects: KubernetesAuthorityWorkerObjects,
        deployment_snapshot: DeploymentSnapshot,
    ) -> AuthorityWorkerObservation:
        self._require_api_identity(objects.pod, api_version='v1', kind='Pod')
        self._require_api_identity(objects.replica_set,
                                   api_version='apps/v1',
                                   kind='ReplicaSet')
        self._require_api_identity(objects.deployment,
                                   api_version='apps/v1',
                                   kind='Deployment')
        self._require_api_identity(objects.service_account,
                                   api_version='v1',
                                   kind='ServiceAccount')
        self._template_validator(manifest, objects)
        pod_metadata = _metadata(objects.pod, kind='Pod')
        rs_metadata = _metadata(objects.replica_set, kind='ReplicaSet')
        deployment_metadata = _metadata(objects.deployment, kind='Deployment')
        sa_metadata = _metadata(objects.service_account, kind='ServiceAccount')
        pod_spec = _field(objects.pod, 'spec')
        pod_status = _field(objects.pod, 'status')
        if pod_spec is None or pod_status is None:
            raise BootstrapUnavailable('Pod spec/status is incomplete.')
        if _field(pod_spec,
                  'service_account_name') != manifest.service_account_name:
            raise BootstrapInvariantViolation(
                'Pod ServiceAccount differs from the static manifest.')
        container = _named_item(_field(pod_spec, 'containers'),
                                manifest.container_name,
                                kind='Pod containers')
        if (_field(container, 'image') != manifest.image.requested_reference or
                _field(container, 'image_pull_policy') != 'Always'):
            raise BootstrapInvariantViolation(
                'Pod container image or pull policy differs from the manifest.')
        container_status = _named_item(_field(pod_status, 'container_statuses'),
                                       manifest.container_name,
                                       kind='Pod container statuses')
        status_image = _field(container_status, 'image')
        if status_image != manifest.image.requested_reference:
            raise BootstrapInvariantViolation(
                'Pod runtime image reference differs from the manifest.')
        runtime_image = _runtime_image_identity(
            manifest.image, _field(container_status, 'image_id'))
        ready_conditions = [
            condition for condition in (_field(pod_status, 'conditions') or ())
            if _field(condition, 'type') == 'Ready'
        ]
        if (len(ready_conditions) != 1 or
                _field(ready_conditions[0], 'status') != 'True'):
            raise BootstrapUnavailable('Pod is not Ready.')
        pod_owner_name, pod_owner_uid = _controller_owner(
            pod_metadata, kind='Pod', expected_kind='ReplicaSet')
        deployment_owner_name, deployment_owner_uid = _controller_owner(
            rs_metadata, kind='ReplicaSet', expected_kind='Deployment')
        observed_at = _canonical_timestamp(database_now)
        cohort = actions.WorkerCohortIdentityV1(
            version=1,
            manifest=manifest,
            manifest_sha256=manifest.sha256,
            deployment_uid=_required_text(_field(deployment_metadata, 'uid'),
                                          name='Deployment.uid'),
            service_account_uid=_required_text(_field(sa_metadata, 'uid'),
                                               name='ServiceAccount.uid'))
        worker = actions.ProviderAuthorityWorkerIdentityV1(
            namespace=manifest.namespace,
            pod_name=_required_text(_field(pod_metadata, 'name'),
                                    name='Pod.name'),
            pod_uid=_required_text(_field(pod_metadata, 'uid'), name='Pod.uid'),
            pod_resource_version=_required_text(_field(pod_metadata,
                                                       'resource_version'),
                                                name='Pod.resourceVersion'),
            pod_service_account_name=manifest.service_account_name,
            pod_controller_owner=actions.ProviderKubernetesControllerOwnerV1(
                api_version='apps/v1',
                kind='ReplicaSet',
                name=pod_owner_name,
                uid=pod_owner_uid),
            replica_set_name=_required_text(_field(rs_metadata, 'name'),
                                            name='ReplicaSet.name'),
            replica_set_uid=_required_text(_field(rs_metadata, 'uid'),
                                           name='ReplicaSet.uid'),
            replica_set_resource_version=_required_text(
                _field(rs_metadata, 'resource_version'),
                name='ReplicaSet.resourceVersion'),
            replica_set_controller_owner=(
                actions.ProviderKubernetesControllerOwnerV1(
                    api_version='apps/v1',
                    kind='Deployment',
                    name=deployment_owner_name,
                    uid=deployment_owner_uid)),
            deployment_name=manifest.deployment_name,
            deployment_uid=deployment_snapshot.uid,
            deployment_resource_version=deployment_snapshot.resource_version,
            deployment_generation=deployment_snapshot.generation,
            deployment_observed_generation=(
                deployment_snapshot.observed_generation),
            pod_template_contract_sha256=manifest.pod_template_contract.sha256,
            image=actions.ProviderAuthorityWorkerImageV1(
                qualification=manifest.image, runtime=runtime_image),
            service_account_uid=cohort.service_account_uid,
            artifact_inventory_sha256=manifest.artifact_inventory.sha256,
            callable_inventory_sha256=manifest.callable_inventory.sha256,
            handler_allowlist_sha256=actions.canonical_sha256(
                list(manifest.handler_allowlist)),
            observed_at=observed_at)
        registration_value: dict[str, Any] = {
            'worker': worker.canonical_value(),
            'pod_ready': True,
            'deployment_spec_replicas': deployment_snapshot.spec_replicas,
            'deployment_status_replicas': deployment_snapshot.status_replicas,
            'deployment_updated_replicas': deployment_snapshot.updated_replicas,
            'deployment_status_observed_generation':
                deployment_snapshot.observed_generation,
            'deployment_ready_replicas': deployment_snapshot.ready_replicas,
            'deployment_available_replicas':
                deployment_snapshot.available_replicas,
            'deployment_unavailable_replicas':
                deployment_snapshot.unavailable_replicas,
            'registered_at': observed_at,
        }
        registration = (actions.ProviderAuthorityWorkerRegistrationV1.
                        from_value(registration_value))
        return AuthorityWorkerObservation(cohort, registration,
                                          deployment_snapshot)


class CanonicalAuthorityWorkerTemplateValidator:
    """Exact Deployment/ReplicaSet/Pod projections for the v1 cohort."""

    def __init__(self, serializer: Callable[[Any], Any]) -> None:
        if not callable(serializer):
            raise TypeError('serializer must be callable.')
        self._serializer = serializer

    def _serialized_object(self, value: Any, *, name: str) -> dict[str, Any]:
        serialized = copy.deepcopy(value) if isinstance(
            value, dict) else self._serializer(value)
        if type(serialized) is not dict:
            raise BootstrapInvariantViolation(f'{name} is not an object.')
        return serialized

    @staticmethod
    def _normalize_api_stored_template(value: Any, *,
                                       name: str) -> dict[str, Any]:
        if type(value) is not dict:
            raise BootstrapInvariantViolation(f'{name} is not an object.')
        template = copy.deepcopy(value)
        metadata = template.get('metadata')
        if (type(metadata) is not dict or 'creationTimestamp' not in metadata or
                metadata['creationTimestamp'] is not None):
            raise BootstrapInvariantViolation(
                f'{name} creationTimestamp is not the exact null API default.')
        metadata.pop('creationTimestamp')
        return template

    def __call__(
        self,
        manifest: actions.ProviderAuthorityWorkerCohortManifestV1,
        objects: KubernetesAuthorityWorkerObjects,
    ) -> None:
        # pylint: disable=import-outside-toplevel
        from sky.serve import resource_action_provider_preflight
        expected = (resource_action_provider_preflight.
                    materialize_provider_authority_worker_pod_template_v1(
                        manifest.pod_template_binding.release_inputs,
                        manifest.sha256).canonical_value())
        deployment = self._serialized_object(objects.deployment,
                                             name='Deployment')
        try:
            deployment_template = self._normalize_api_stored_template(
                deployment['spec']['template'], name='Deployment Pod template')
            deployment_selector_object = deployment['spec']['selector']
            if (type(deployment_selector_object) is not dict or
                    set(deployment_selector_object) != {'matchLabels'}):
                raise BootstrapInvariantViolation(
                    'Deployment selector is not the closed matchLabels form.')
            deployment_selector = deployment_selector_object['matchLabels']
        except (KeyError, TypeError) as e:
            raise BootstrapInvariantViolation(
                'Deployment template/selector is incomplete.') from e
        if (actions.canonical_json_bytes(deployment_template)
                != actions.canonical_json_bytes(expected) or
                deployment_selector != expected['metadata']['labels']):
            raise BootstrapInvariantViolation(
                'Live Deployment Pod template differs from its release binding.'
            )

        replica_set = self._serialized_object(objects.replica_set,
                                              name='ReplicaSet')
        try:
            rs_template = self._normalize_api_stored_template(
                replica_set['spec']['template'], name='ReplicaSet Pod template')
            rs_selector_object = replica_set['spec']['selector']
            if (type(rs_selector_object) is not dict or
                    set(rs_selector_object) != {'matchLabels'}):
                raise BootstrapInvariantViolation(
                    'ReplicaSet selector is not the closed matchLabels form.')
            rs_selector = rs_selector_object['matchLabels']
            template_hash = rs_template['metadata']['labels'].pop(
                'pod-template-hash')
        except (KeyError, TypeError) as e:
            raise BootstrapInvariantViolation(
                'ReplicaSet Pod-template-hash projection is incomplete.') from e
        if (type(template_hash) is not str or not template_hash or
                rs_selector !=
            {
                **expected['metadata']['labels'], 'pod-template-hash': template_hash
            } or actions.canonical_json_bytes(rs_template)
                != actions.canonical_json_bytes(expected)):
            raise BootstrapInvariantViolation(
                'ReplicaSet template differs beyond pod-template-hash.')

        pod = self._serialized_object(objects.pod, name='Pod')
        try:
            pod_labels = copy.deepcopy(pod['metadata']['labels'])
            pod_hash = pod_labels.pop('pod-template-hash')
            pod_projection = {
                'metadata': {
                    'labels': pod_labels,
                    'annotations': pod['metadata'].get('annotations', {}),
                },
                'spec': copy.deepcopy(pod['spec']),
            }
            pod_projection['spec'].pop('nodeName')
        except (KeyError, TypeError) as e:
            raise BootstrapInvariantViolation(
                'Pod template projection is incomplete.') from e
        if (pod_hash != template_hash or
                actions.canonical_json_bytes(pod_projection)
                != actions.canonical_json_bytes(expected)):
            raise BootstrapInvariantViolation(
                'Live Pod differs from the accepted ReplicaSet template.')


class ReadOnlyKubernetesAuthorityWorkerObserver:
    """Four-GET observer with no list/watch/mutation method in its surface."""

    def __init__(
        self,
        manifest: actions.ProviderAuthorityWorkerCohortManifestV1,
        pod_identity: AuthorityWorkerPodIdentity,
        core_api: Any,
        apps_api: Any,
        projector: LiveObservationProjector,
    ) -> None:
        self._manifest = manifest
        self._pod_identity = pod_identity
        self._core_api = core_api
        self._apps_api = apps_api
        self._projector = projector

    @staticmethod
    def _metadata_identity(obj: Any, *, kind: str) -> tuple[str, str, str, str]:
        metadata = _field(obj, 'metadata')
        if metadata is None:
            raise BootstrapInvariantViolation(f'{kind} metadata is absent.')
        return (
            _required_text(_field(metadata, 'name'), name=f'{kind}.name'),
            _required_text(_field(metadata, 'namespace'),
                           name=f'{kind}.namespace'),
            _required_text(_field(metadata, 'uid'), name=f'{kind}.uid'),
            _required_text(_field(metadata, 'resource_version'),
                           name=f'{kind}.resourceVersion'),
        )

    def observe(self,
                database_now: datetime.datetime) -> AuthorityWorkerObservation:
        namespace = self._pod_identity.namespace
        pod = self._core_api.read_namespaced_pod(self._pod_identity.name,
                                                 namespace)
        pod_name, pod_namespace, pod_uid, _ = self._metadata_identity(
            pod, kind='Pod')
        if ((pod_name, pod_namespace, pod_uid)
                != (self._pod_identity.name, self._pod_identity.namespace,
                    self._pod_identity.uid)):
            raise BootstrapInvariantViolation(
                'Live Pod identity differs from downward-API identity.')
        replica_set_name, replica_set_uid = _controller_owner(
            _field(pod, 'metadata'), kind='Pod', expected_kind='ReplicaSet')
        replica_set = self._apps_api.read_namespaced_replica_set(
            replica_set_name, namespace)
        live_rs_name, live_rs_namespace, live_rs_uid, _ = (
            self._metadata_identity(replica_set, kind='ReplicaSet'))
        if ((live_rs_name, live_rs_namespace, live_rs_uid)
                != (replica_set_name, namespace, replica_set_uid)):
            raise BootstrapInvariantViolation(
                'Live ReplicaSet differs from the Pod owner reference.')
        deployment_name, deployment_uid = _controller_owner(
            _field(replica_set, 'metadata'),
            kind='ReplicaSet',
            expected_kind='Deployment')
        if deployment_name != self._manifest.deployment_name:
            raise BootstrapInvariantViolation(
                'ReplicaSet owner is not the manifest Deployment.')
        deployment = self._apps_api.read_namespaced_deployment(
            deployment_name, namespace)
        live_deployment_name, live_deployment_namespace, live_deployment_uid, _ = (
            self._metadata_identity(deployment, kind='Deployment'))
        if ((live_deployment_name, live_deployment_namespace,
             live_deployment_uid)
                != (deployment_name, namespace, deployment_uid)):
            raise BootstrapInvariantViolation(
                'Live Deployment differs from the ReplicaSet owner reference.')
        service_account = self._core_api.read_namespaced_service_account(
            self._manifest.service_account_name, namespace)
        sa_name, sa_namespace, _, _ = self._metadata_identity(
            service_account, kind='ServiceAccount')
        if ((sa_name, sa_namespace)
                != (self._manifest.service_account_name, namespace)):
            raise BootstrapInvariantViolation(
                'Live ServiceAccount differs from the manifest identity.')
        objects = KubernetesAuthorityWorkerObjects(
            pod=pod,
            replica_set=replica_set,
            deployment=deployment,
            service_account=service_account)
        snapshot = DeploymentSnapshot.from_object(deployment)
        return self._projector(self._manifest, database_now, objects, snapshot)

    def require_same_deployment_snapshot(
        self,
        registrations: actions.WorkerCohortRegistrationSetV1,
    ) -> None:
        deployment = self._apps_api.read_namespaced_deployment(
            self._manifest.deployment_name, self._manifest.namespace)
        snapshot = DeploymentSnapshot.from_object(deployment)
        if any(not snapshot.matches_registration(registration)
               for registration in registrations.registrations):
            raise BootstrapUnavailable(
                'Final Deployment read differs from registered snapshot.')


class AuthorityWorkerRegistrationStore(Protocol):
    """Narrow PostgreSQL lifecycle surface used by the coordinator."""

    def read_database_clock(self) -> datetime.datetime:
        ...

    def get_worker_cohort(
            self,
            cohort_id: str) -> resource_action_state.WorkerCohortRecord | None:
        ...

    def register_worker_cohort(
        self,
        cohort_identity: actions.WorkerCohortIdentityV1,
        registration_attestations: actions.WorkerCohortRegistrationSetV1,
    ) -> resource_action_state.WorkerCohortTransition:
        ...

    def append_worker_cohort_registration(
        self,
        cohort_identity: actions.WorkerCohortIdentityV1,
        expected_revision: int,
        expected_registration_attestations: actions.
        WorkerCohortRegistrationSetV1,
        own_registration: actions.ProviderAuthorityWorkerRegistrationV1,
    ) -> resource_action_state.WorkerCohortTransition:
        ...

    def promote_worker_cohort(
        self,
        cohort_id: str,
        expected_revision: int,
        expected_registration_attestations: actions.
        WorkerCohortRegistrationSetV1,
    ) -> resource_action_state.WorkerCohortTransition:
        ...

    def renew_worker_cohort_registration(
        self,
        cohort_id: str,
        expected_revision: int,
        expected_state: actions.WorkerCohortLifecycleState,
        expected_registration_attestations: actions.
        WorkerCohortRegistrationSetV1,
        own_registration: actions.ProviderAuthorityWorkerRegistrationV1,
    ) -> resource_action_state.WorkerCohortTransition:
        ...


class AuthorityWorkerEvidenceLease:
    """Process-local lease derived only from PostgreSQL evidence time."""

    def __init__(self,
                 *,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._record: resource_action_state.WorkerCohortRecord | None = None
        self._deadline = 0.0

    def clear(self) -> None:
        with self._lock:
            self._record = None
            self._deadline = 0.0

    def adopt(self, record: resource_action_state.WorkerCohortRecord,
              database_now: datetime.datetime, own_pod_uid: str) -> None:
        if record.lifecycle_state not in (
                actions.WorkerCohortLifecycleState.ACCEPTING,
                actions.WorkerCohortLifecycleState.DRAINING):
            raise BootstrapUnavailable(
                'Only accepting or draining evidence can be leased.')
        registrations = record.registration_attestations.registrations
        if len(registrations) != 2 or own_pod_uid not in {
                item.pod_uid for item in registrations
        }:
            raise BootstrapInvariantViolation(
                'Local Pod UID is outside the accepted worker pair.')
        if (not isinstance(database_now, datetime.datetime) or
                database_now.tzinfo is None or
                database_now.utcoffset() is None):
            raise BootstrapUnavailable(
                'PostgreSQL evidence clock is not timezone-aware.')
        normalized_now = database_now.astimezone(datetime.timezone.utc)
        timestamps = tuple(
            timestamp for registration in registrations for timestamp in (
                _parse_timestamp(registration.registered_at),
                _parse_timestamp(registration.worker.observed_at)))
        if any(timestamp > normalized_now for timestamp in timestamps):
            raise BootstrapUnavailable(
                'Accepted worker evidence is in the database future.')
        oldest = min(timestamps)
        remaining = (oldest + _EVIDENCE_LEASE - normalized_now).total_seconds()
        if remaining <= 0:
            raise BootstrapUnavailable('Accepted worker evidence is stale.')
        with self._lock:
            self._record = record
            self._deadline = self._monotonic() + remaining

    def _current_record(
            self) -> resource_action_state.WorkerCohortRecord | None:
        with self._lock:
            if self._record is None:
                return None
            if self._monotonic() >= self._deadline:
                self._record = None
                self._deadline = 0.0
                return None
            return self._record

    def accepted_manifest(
            self) -> actions.ProviderAuthorityWorkerCohortManifestV1 | None:
        record = self._current_record()
        if (record is None or record.lifecycle_state
                is not actions.WorkerCohortLifecycleState.ACCEPTING):
            return None
        return record.cohort_identity.manifest

    def is_locally_accepted(self) -> bool:
        return self.accepted_manifest() is not None

    @property
    def record(self) -> resource_action_state.WorkerCohortRecord | None:
        return self._current_record()


class AuthorityWorkerBootstrapCoordinator:
    """Register, promote, and renew one immutable two-Pod cohort."""

    def __init__(
        self,
        manifest: actions.ProviderAuthorityWorkerCohortManifestV1,
        pod_identity: AuthorityWorkerPodIdentity,
        observer: ReadOnlyKubernetesAuthorityWorkerObserver,
        store: AuthorityWorkerRegistrationStore,
        *,
        evidence_lease: AuthorityWorkerEvidenceLease | None = None,
        static_evidence_loader: (
            Callable[[], actions.ProviderAuthorityWorkerCohortManifestV1] |
            None) = None,
        reconcile_interval_seconds: float = _DEFAULT_RECONCILE_INTERVAL_SECONDS,
    ) -> None:
        if reconcile_interval_seconds <= 0:
            raise ValueError('reconcile_interval_seconds must be positive.')
        self._manifest = manifest
        self._pod_identity = pod_identity
        self._observer = observer
        self._store = store
        self._evidence_lease = evidence_lease or AuthorityWorkerEvidenceLease()
        self._static_evidence_loader = static_evidence_loader
        self._reconcile_interval_seconds = reconcile_interval_seconds
        self._stop = threading.Event()
        self._failure_lock = threading.Lock()
        self._failure: BaseException | None = None
        self._initial_adoption_complete = False
        self._thread = threading.Thread(target=self._run,
                                        name='authority-worker-bootstrap',
                                        daemon=True)

    @property
    def evidence_lease(self) -> AuthorityWorkerEvidenceLease:
        return self._evidence_lease

    @property
    def failure(self) -> BaseException | None:
        with self._failure_lock:
            return self._failure

    def clear_acceptance(self) -> None:
        self._evidence_lease.clear()

    def accepted_manifest(
            self) -> actions.ProviderAuthorityWorkerCohortManifestV1 | None:
        return self._evidence_lease.accepted_manifest()

    @staticmethod
    def _one_registration_set(
        observation: AuthorityWorkerObservation,
    ) -> actions.WorkerCohortRegistrationSetV1:
        return actions.WorkerCohortRegistrationSetV1(
            version=1,
            cohort_identity_sha256=observation.cohort_identity.sha256,
            workers=(observation.registration,))

    def _require_exact_identity(
        self,
        record: resource_action_state.WorkerCohortRecord,
        observation: AuthorityWorkerObservation,
    ) -> None:
        if (record.cohort_identity.canonical_bytes
                != observation.cohort_identity.canonical_bytes):
            raise BootstrapInvariantViolation(
                'Live cohort identity differs from the stored immutable row.')

    @staticmethod
    def _renewal_frozen_value(
        registration: actions.ProviderAuthorityWorkerRegistrationV1,
    ) -> dict[str, Any]:
        value = registration.canonical_value()
        value.pop('registered_at')
        worker = value['worker']
        assert isinstance(worker, dict)
        worker.pop('observed_at')
        worker.pop('pod_resource_version')
        worker.pop('replica_set_resource_version')
        return value

    def _reconcile_registering(
        self,
        record: resource_action_state.WorkerCohortRecord,
        observation: AuthorityWorkerObservation,
    ) -> resource_action_state.WorkerCohortRecord:
        workers = {
            item.pod_uid: item
            for item in record.registration_attestations.registrations
        }
        stored_own = workers.get(self._pod_identity.uid)
        if stored_own is None:
            if record.registration_attestations.count == 2:
                raise BootstrapInvariantViolation(
                    'Replacement Pod UID is outside the registering pair.')
            transition = self._store.append_worker_cohort_registration(
                record.cohort_identity, record.revision,
                record.registration_attestations, observation.registration)
            record = transition.record
            workers = {
                item.pod_uid: item
                for item in record.registration_attestations.registrations
            }
            stored_own = workers.get(self._pod_identity.uid)
        if stored_own is None:
            raise BootstrapInvariantViolation(
                'Store append did not preserve the calling Pod registration.')
        if record.registration_attestations.count != 2:
            self._evidence_lease.clear()
            return record
        self._observer.require_same_deployment_snapshot(
            record.registration_attestations)
        transition = self._store.promote_worker_cohort(
            record.cohort_id, record.revision, record.registration_attestations)
        record = transition.record
        adoption_now = self._store.read_database_clock()
        self._evidence_lease.adopt(record, adoption_now, self._pod_identity.uid)
        self._initial_adoption_complete = True
        return record

    def _reconcile_accepted(
        self,
        record: resource_action_state.WorkerCohortRecord,
        observation: AuthorityWorkerObservation,
    ) -> resource_action_state.WorkerCohortRecord:
        workers = {
            item.pod_uid: item
            for item in record.registration_attestations.registrations
        }
        previous_own = workers.get(self._pod_identity.uid)
        if previous_own is None:
            raise BootstrapInvariantViolation(
                'Replacement Pod UID is outside the accepted pair.')
        if (self._renewal_frozen_value(previous_own)
                != self._renewal_frozen_value(observation.registration)):
            raise BootstrapInvariantViolation(
                'Live worker evidence changed a frozen cohort field.')
        transition = self._store.renew_worker_cohort_registration(
            record.cohort_id, record.revision, record.lifecycle_state,
            record.registration_attestations, observation.registration)
        record = transition.record
        adoption_now = self._store.read_database_clock()
        self._evidence_lease.adopt(record, adoption_now, self._pod_identity.uid)
        self._initial_adoption_complete = True
        return record

    def run_once(self) -> resource_action_state.WorkerCohortRecord:
        """Perform one DB-clock-first live reconciliation."""
        cohort_id = self._manifest.cohort_id
        for _ in range(3):
            # A stale CAS invalidates the complete observation cell.  In
            # particular, never carry a pre-race database timestamp or live
            # Kubernetes resourceVersion into a retry.
            database_now = self._store.read_database_clock()
            if self._static_evidence_loader is not None:
                reloaded_manifest = self._static_evidence_loader()
                if (reloaded_manifest.canonical_bytes
                        != self._manifest.canonical_bytes):
                    raise BootstrapInvariantViolation(
                        'Projected static evidence changed after startup.')
            record = self._store.get_worker_cohort(cohort_id)
            if (record is not None and not self._initial_adoption_complete and
                    record.lifecycle_state
                    in (actions.WorkerCohortLifecycleState.ACCEPTING,
                        actions.WorkerCohortLifecycleState.DRAINING)):
                if (record.cohort_identity.manifest.canonical_bytes
                        != self._manifest.canonical_bytes):
                    raise BootstrapInvariantViolation(
                        'Stored accepted cohort differs from the local static '
                        'manifest.')
                # Initial peer adoption and lost promotion acknowledgement use
                # the exact committed bytes first.  After this one-time path,
                # every lease recovery requires a fresh live observation.
                self._evidence_lease.adopt(record, database_now,
                                           self._pod_identity.uid)
                self._initial_adoption_complete = True
                return record
            observation = self._observer.observe(database_now)
            if (observation.cohort_identity.manifest.canonical_bytes
                    != self._manifest.canonical_bytes or
                    observation.registration.pod_uid != self._pod_identity.uid):
                raise BootstrapInvariantViolation(
                    'Projected observation differs from local manifest/Pod UID.'
                )
            if record is None:
                try:
                    record = self._store.register_worker_cohort(
                        observation.cohort_identity,
                        self._one_registration_set(observation)).record
                except kernel_actions.ActionConflict:
                    record = self._store.get_worker_cohort(cohort_id)
                    if record is None:
                        raise BootstrapInvariantViolation(
                            'Worker cohort insert conflicted outside its exact '
                            'immutable ID; possible Deployment UID or full-key '
                            'collision.') from None
            self._require_exact_identity(record, observation)
            try:
                if record.lifecycle_state is (
                        actions.WorkerCohortLifecycleState.REGISTERING):
                    return self._reconcile_registering(record, observation)
                if record.lifecycle_state in (
                        actions.WorkerCohortLifecycleState.ACCEPTING,
                        actions.WorkerCohortLifecycleState.DRAINING):
                    return self._reconcile_accepted(record, observation)
                self._evidence_lease.clear()
                raise BootstrapUnavailable(
                    'Worker cohort is removal-authorized or retired.')
            except kernel_actions.StaleRevision:
                self._evidence_lease.clear()
                continue
        raise BootstrapUnavailable(
            'Worker cohort changed throughout bounded reconciliation.')

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except (BootstrapInvariantViolation,
                    kernel_actions.InvariantViolation) as e:
                self._evidence_lease.clear()
                with self._failure_lock:
                    self._failure = e
                return
            except Exception:  # pylint: disable=broad-except
                self._evidence_lease.clear()
            self._stop.wait(self._reconcile_interval_seconds)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=10)
        self._evidence_lease.clear()


def build_default_coordinator(
    manifest: actions.ProviderAuthorityWorkerCohortManifestV1,
    pod_identity: AuthorityWorkerPodIdentity,
) -> AuthorityWorkerBootstrapCoordinator:
    """Build the production in-cluster, read-only P2a coordinator."""
    if not isinstance(manifest,
                      actions.ProviderAuthorityWorkerCohortManifestV1):
        raise TypeError('manifest has an invalid type.')
    if not isinstance(pod_identity, AuthorityWorkerPodIdentity):
        raise TypeError('pod_identity has an invalid type.')
    if pod_identity.namespace != manifest.namespace:
        raise BootstrapInvariantViolation(
            'Downward Pod namespace differs from the static manifest.')
    # This import is dedicated-role-only.  The explicit in-cluster context
    # prevents an ambient kubeconfig fallback.
    # pylint: disable=import-outside-toplevel
    from sky.adaptors import kubernetes
    from sky.serve import resource_action_provider_preflight
    context = kubernetes.in_cluster_context_name()
    core_api = kubernetes.core_api(context)
    apps_api = kubernetes.apps_api(context)
    serializer = kubernetes.kubernetes.client.ApiClient(
    ).sanitize_for_serialization
    projector = DefaultAuthorityWorkerLiveProjector(
        CanonicalAuthorityWorkerTemplateValidator(serializer))
    observer = ReadOnlyKubernetesAuthorityWorkerObserver(
        manifest, pod_identity, core_api, apps_api, projector)
    store = resource_action_state.PostgresServeResourceActionStateStore()
    return AuthorityWorkerBootstrapCoordinator(
        manifest,
        pod_identity,
        observer,
        store,
        static_evidence_loader=(
            resource_action_provider_preflight.
            load_provider_authority_worker_static_evidence_v1))
