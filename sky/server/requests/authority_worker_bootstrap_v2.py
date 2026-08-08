"""Additive Serve038 authority-worker membership bootstrap.

This module owns only static/live V2 attestation, registration-lease renewal,
and initial ``REGISTERING -> ACCEPTING`` activation.  It never constructs a
request executor, advertises a claimant, evaluates an action preflight, or
performs provider I/O.  The shipped Serve034 V1 coordinator remains isolated
in :mod:`authority_worker_bootstrap` for its retirement/deselect boundary.
"""
# The additive V2 path deliberately reuses only the frozen V1 Kubernetes
# extraction helpers; its manifest, snapshot, membership, and store contracts
# remain disjoint.
# pylint: disable=protected-access

from __future__ import annotations

from collections.abc import Callable
import dataclasses
import datetime
import os
import threading
from typing import Any, NoReturn, Protocol
import uuid

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_authority_state as authority_state
from sky.serve import resource_actions
from sky.serve import serve_state_schema
from sky.server.requests import authority_worker_bootstrap as bootstrap_v1

_DEFAULT_RECONCILE_INTERVAL_SECONDS = (
    authority.RESOURCE_ACTION_WORKER_REGISTRATION_LEASE_RENEW_SECONDS_V1)
_BOUNDED_RECONCILE_ATTEMPTS = 3
_KUBERNETES_REQUEST_TIMEOUT = (5, 10)
_DEFAULT_STOP_JOIN_TIMEOUT_SECONDS = 10.0
_DEFAULT_MUTATION_FENCE_TIMEOUT_SECONDS = 30.0
_FAIL_STOP_EXIT_CODE = 70


class _BootstrapStopped(bootstrap_v1.BootstrapUnavailable):
    """The local stop fence closed before a publication or mutation."""


@dataclasses.dataclass(frozen=True)
class AuthorityWorkerObservationV2:
    """One DB-clock-bound projection of this Pod and the final Deployment."""

    cohort: authority.ProviderAuthorityWorkerCohortV2
    worker: authority.ProviderAuthorityWorkerIdentityV2
    deployment_snapshot: authority.ProviderAuthorityWorkerDeploymentSnapshotV2

    def __post_init__(self) -> None:
        if type(self.cohort) is not authority.ProviderAuthorityWorkerCohortV2:
            raise TypeError('cohort has an invalid type.')
        if type(self.worker) is not authority.ProviderAuthorityWorkerIdentityV2:
            raise TypeError('worker has an invalid type.')
        if type(self.deployment_snapshot) is not (
                authority.ProviderAuthorityWorkerDeploymentSnapshotV2):
            raise TypeError('deployment_snapshot has an invalid type.')
        self.worker.validate_for_cohort(self.cohort)
        snapshot = self.deployment_snapshot
        if (snapshot.deployment_name != self.worker.deployment_name or
                snapshot.deployment_uid != self.worker.deployment_uid or
                snapshot.deployment_generation
                != self.worker.deployment_generation or
                snapshot.deployment_observed_generation
                != self.worker.deployment_observed_generation or
                snapshot.pod_template_contract_sha256
                != self.worker.pod_template_contract_sha256):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'V2 worker and Deployment snapshot identities differ.')


def _deployment_integer(parent: Any,
                        field_name: str,
                        *,
                        zero_if_absent: bool = False) -> int:
    raw = bootstrap_v1._field(  # pylint: disable=protected-access
        parent, field_name)
    if raw is None and zero_if_absent:
        raw = 0
    if type(raw) is not int or raw < 0:
        raise bootstrap_v1.BootstrapUnavailable(
            f'Deployment {field_name} is not a nonnegative integer.')
    return raw


def project_deployment_snapshot_v2(
    manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
    deployment: Any,
    database_now: datetime.datetime,
) -> authority.ProviderAuthorityWorkerDeploymentSnapshotV2:
    """Project the exact current two-of-two RollingUpdate snapshot."""

    if type(manifest) is not authority.ProviderAuthorityWorkerCohortManifestV2:
        raise TypeError('manifest must be an exact V2 manifest.')
    metadata = bootstrap_v1._field(  # pylint: disable=protected-access
        deployment, 'metadata')
    spec = bootstrap_v1._field(  # pylint: disable=protected-access
        deployment, 'spec')
    status = bootstrap_v1._field(  # pylint: disable=protected-access
        deployment, 'status')
    if metadata is None or spec is None or status is None:
        raise bootstrap_v1.BootstrapUnavailable(
            'Deployment metadata/spec/status is incomplete.')
    strategy = bootstrap_v1._field(  # pylint: disable=protected-access
        spec, 'strategy')
    rolling = bootstrap_v1._field(  # pylint: disable=protected-access
        strategy, 'rolling_update')
    max_surge = bootstrap_v1._field(  # pylint: disable=protected-access
        rolling, 'max_surge')
    max_unavailable = bootstrap_v1._field(  # pylint: disable=protected-access
        rolling, 'max_unavailable')
    if (bootstrap_v1._field(  # pylint: disable=protected-access
            strategy, 'type') != 'RollingUpdate' or rolling is None or
            type(max_surge) is not int or max_surge != 0 or
            type(max_unavailable) is not int or max_unavailable != 1):
        raise bootstrap_v1.BootstrapInvariantViolation(
            'Authority V2 Deployment strategy is not exact RollingUpdate '
            'with integer maxSurge=0 and maxUnavailable=1.')

    generation = _deployment_integer(metadata, 'generation')
    observed_generation = _deployment_integer(status, 'observed_generation')
    spec_replicas = _deployment_integer(spec, 'replicas')
    status_replicas = _deployment_integer(status, 'replicas')
    updated_replicas = _deployment_integer(status, 'updated_replicas')
    ready_replicas = _deployment_integer(status, 'ready_replicas')
    available_replicas = _deployment_integer(status, 'available_replicas')
    unavailable_replicas = _deployment_integer(status,
                                               'unavailable_replicas',
                                               zero_if_absent=True)
    if (generation <= 0 or observed_generation != generation or
            spec_replicas != 2 or status_replicas != 2 or
            updated_replicas != 2 or ready_replicas != 2 or
            available_replicas != 2 or unavailable_replicas != 0):
        raise bootstrap_v1.BootstrapUnavailable(
            'Authority V2 Deployment is not one exact current 2/2 snapshot.')
    name = bootstrap_v1._required_text(  # pylint: disable=protected-access
        bootstrap_v1._field(
            metadata,  # pylint: disable=protected-access
            'name'),
        name='Deployment.name')
    if name != manifest.deployment_name:
        raise bootstrap_v1.BootstrapInvariantViolation(
            'Live Deployment name differs from the V2 manifest.')
    return authority.ProviderAuthorityWorkerDeploymentSnapshotV2(
        version=2,
        deployment_name=name,
        deployment_uid=bootstrap_v1._required_text(  # pylint: disable=protected-access
            bootstrap_v1._field(
                metadata,  # pylint: disable=protected-access
                'uid'),
            name='Deployment.uid'),
        deployment_resource_version=bootstrap_v1._required_text(  # pylint: disable=protected-access
            bootstrap_v1._field(
                metadata,  # pylint: disable=protected-access
                'resource_version'),
            name='Deployment.resourceVersion'),
        deployment_generation=generation,
        deployment_observed_generation=observed_generation,
        pod_template_contract_sha256=manifest.pod_template_contract.sha256,
        deployment_strategy='RollingUpdate',
        deployment_max_surge=0,
        deployment_max_unavailable=1,
        deployment_spec_replicas=spec_replicas,
        deployment_status_replicas=status_replicas,
        deployment_updated_replicas=updated_replicas,
        deployment_ready_replicas=ready_replicas,
        deployment_available_replicas=available_replicas,
        deployment_unavailable_replicas=unavailable_replicas,
        observed_at=authority.datetime_to_timestamp(database_now,
                                                    name='database_now'))


class LiveTemplateValidatorV2(Protocol):

    def __call__(
        self,
        manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        objects: bootstrap_v1.KubernetesAuthorityWorkerObjects,
    ) -> None:
        ...


class CanonicalAuthorityWorkerTemplateValidatorV2:
    """Adapt the shared Pod-template leaf validator to an exact V2 root."""

    def __init__(self, serializer: Callable[[Any], Any]) -> None:
        self._projection_validator = (
            bootstrap_v1.CanonicalAuthorityWorkerTemplateProjectionValidator(
                serializer))

    def __call__(
        self,
        manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        objects: bootstrap_v1.KubernetesAuthorityWorkerObjects,
    ) -> None:
        if type(manifest) is not (
                authority.ProviderAuthorityWorkerCohortManifestV2):
            raise TypeError('manifest must be an exact V2 manifest.')
        self._projection_validator(manifest.pod_template_binding.release_inputs,
                                   manifest.sha256, objects)


class DefaultAuthorityWorkerLiveProjectorV2:
    """Project V2 identity after exact shared Pod-template verification."""

    def __init__(self, template_validator: LiveTemplateValidatorV2) -> None:
        self._template_validator = template_validator

    @staticmethod
    def _require_api_identity(obj: Any, *, api_version: str, kind: str) -> None:
        if (bootstrap_v1._field(  # pylint: disable=protected-access
                obj, 'api_version') != api_version or bootstrap_v1._field(  # pylint: disable=protected-access
                    obj, 'kind') != kind):
            raise bootstrap_v1.BootstrapInvariantViolation(
                f'Live {kind} TypeMeta is not exact.')

    def __call__(
        self,
        manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        database_now: datetime.datetime,
        objects: bootstrap_v1.KubernetesAuthorityWorkerObjects,
        deployment_snapshot: authority.
        ProviderAuthorityWorkerDeploymentSnapshotV2,
    ) -> AuthorityWorkerObservationV2:
        if type(manifest
               ) is not authority.ProviderAuthorityWorkerCohortManifestV2:
            raise TypeError('manifest must be an exact V2 manifest.')
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
        pod_metadata = bootstrap_v1._metadata(  # pylint: disable=protected-access
            objects.pod, kind='Pod')
        rs_metadata = bootstrap_v1._metadata(  # pylint: disable=protected-access
            objects.replica_set,
            kind='ReplicaSet')
        deployment_metadata = bootstrap_v1._metadata(  # pylint: disable=protected-access
            objects.deployment,
            kind='Deployment')
        sa_metadata = bootstrap_v1._metadata(  # pylint: disable=protected-access
            objects.service_account,
            kind='ServiceAccount')
        pod_spec = bootstrap_v1._field(  # pylint: disable=protected-access
            objects.pod, 'spec')
        pod_status = bootstrap_v1._field(  # pylint: disable=protected-access
            objects.pod, 'status')
        if pod_spec is None or pod_status is None:
            raise bootstrap_v1.BootstrapUnavailable(
                'Pod spec/status is incomplete.')
        if bootstrap_v1._field(  # pylint: disable=protected-access
                pod_spec,
                'service_account_name') != manifest.service_account_name:
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Pod ServiceAccount differs from the V2 manifest.')
        container = bootstrap_v1._named_item(  # pylint: disable=protected-access
            bootstrap_v1._field(  # pylint: disable=protected-access
                pod_spec, 'containers'),
            manifest.container_name,
            kind='Pod containers')
        if (bootstrap_v1._field(  # pylint: disable=protected-access
                container, 'image') != manifest.image.requested_reference or
                bootstrap_v1._field(  # pylint: disable=protected-access
                    container, 'image_pull_policy') != 'Always'):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Pod container image or pull policy differs from the manifest.')
        container_status = bootstrap_v1._named_item(  # pylint: disable=protected-access
            bootstrap_v1._field(  # pylint: disable=protected-access
                pod_status, 'container_statuses'),
            manifest.container_name,
            kind='Pod container statuses')
        if bootstrap_v1._field(  # pylint: disable=protected-access
                container_status,
                'image') != manifest.image.requested_reference:
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Pod runtime image reference differs from the manifest.')
        runtime_image = bootstrap_v1._runtime_image_identity(  # pylint: disable=protected-access
            manifest.image,
            bootstrap_v1._field(  # pylint: disable=protected-access
                container_status, 'image_id'))
        ready_conditions = [
            condition for condition in (bootstrap_v1._field(  # pylint: disable=protected-access
                pod_status, 'conditions') or ()) if bootstrap_v1._field(  # pylint: disable=protected-access
                    condition, 'type') == 'Ready'
        ]
        if (len(ready_conditions) != 1 or bootstrap_v1._field(  # pylint: disable=protected-access
                ready_conditions[0], 'status') != 'True'):
            raise bootstrap_v1.BootstrapUnavailable('Pod is not Ready.')
        pod_owner_name, pod_owner_uid = bootstrap_v1._controller_owner(  # pylint: disable=protected-access
            pod_metadata,
            kind='Pod',
            expected_kind='ReplicaSet')
        deployment_owner_name, deployment_owner_uid = (
            bootstrap_v1._controller_owner(  # pylint: disable=protected-access
                rs_metadata,
                kind='ReplicaSet',
                expected_kind='Deployment'))
        deployment_uid = bootstrap_v1._required_text(  # pylint: disable=protected-access
            bootstrap_v1._field(  # pylint: disable=protected-access
                deployment_metadata, 'uid'),
            name='Deployment.uid')
        service_account_uid = bootstrap_v1._required_text(  # pylint: disable=protected-access
            bootstrap_v1._field(  # pylint: disable=protected-access
                sa_metadata, 'uid'),
            name='ServiceAccount.uid')
        cohort = authority.ProviderAuthorityWorkerCohortV2(
            version=2,
            manifest=manifest,
            manifest_sha256=manifest.sha256,
            deployment_uid=deployment_uid,
            service_account_uid=service_account_uid)
        worker = authority.ProviderAuthorityWorkerIdentityV2(
            version=2,
            namespace=manifest.namespace,
            pod_name=bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    pod_metadata, 'name'),
                name='Pod.name'),
            pod_uid=uuid.UUID(
                bootstrap_v1._required_text(  # pylint: disable=protected-access
                    bootstrap_v1._field(  # pylint: disable=protected-access
                        pod_metadata, 'uid'),
                    name='Pod.uid')),
            pod_resource_version=bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    pod_metadata, 'resource_version'),
                name='Pod.resourceVersion'),
            pod_service_account_name=manifest.service_account_name,
            pod_controller_owner=resource_actions.
            ProviderKubernetesControllerOwnerV1(api_version='apps/v1',
                                                kind='ReplicaSet',
                                                name=pod_owner_name,
                                                uid=pod_owner_uid),
            replica_set_name=bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    rs_metadata, 'name'),
                name='ReplicaSet.name'),
            replica_set_uid=bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    rs_metadata, 'uid'),
                name='ReplicaSet.uid'),
            replica_set_resource_version=bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    rs_metadata, 'resource_version'),
                name='ReplicaSet.resourceVersion'),
            replica_set_controller_owner=resource_actions.
            ProviderKubernetesControllerOwnerV1(api_version='apps/v1',
                                                kind='Deployment',
                                                name=deployment_owner_name,
                                                uid=deployment_owner_uid),
            deployment_name=manifest.deployment_name,
            deployment_uid=deployment_snapshot.deployment_uid,
            deployment_generation=deployment_snapshot.deployment_generation,
            deployment_observed_generation=(
                deployment_snapshot.deployment_observed_generation),
            pod_template_contract_sha256=manifest.pod_template_contract.sha256,
            image=resource_actions.ProviderAuthorityWorkerImageV1(
                qualification=manifest.image, runtime=runtime_image),
            service_account_uid=service_account_uid,
            artifact_inventory_sha256=manifest.artifact_inventory.sha256,
            callable_inventory_sha256=manifest.callable_inventory.sha256,
            handler_allowlist_sha256=resource_actions.canonical_sha256(
                list(manifest.handler_allowlist)),
            observed_at=authority.datetime_to_timestamp(database_now,
                                                        name='database_now'))
        return AuthorityWorkerObservationV2(cohort, worker, deployment_snapshot)


class LiveObservationProjectorV2(Protocol):
    """Project a typed V2 observation from the four live objects."""

    def __call__(
        self,
        manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        database_now: datetime.datetime,
        objects: bootstrap_v1.KubernetesAuthorityWorkerObjects,
        deployment_snapshot: authority.
        ProviderAuthorityWorkerDeploymentSnapshotV2,
    ) -> AuthorityWorkerObservationV2:
        ...


class ReadOnlyKubernetesAuthorityWorkerObserverV2:
    """Four-GET self/owner-chain observer with no peer Pod permission."""

    def __init__(
        self,
        manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        pod_identity: bootstrap_v1.AuthorityWorkerPodIdentity,
        core_api: Any,
        apps_api: Any,
        projector: LiveObservationProjectorV2,
    ) -> None:
        if type(manifest
               ) is not authority.ProviderAuthorityWorkerCohortManifestV2:
            raise TypeError('manifest must be an exact V2 manifest.')
        self._manifest = manifest
        self._pod_identity = pod_identity
        self._core_api = core_api
        self._apps_api = apps_api
        self._projector = projector

    @staticmethod
    def _metadata_identity(obj: Any, *, kind: str) -> tuple[str, str, str, str]:
        metadata = bootstrap_v1._field(  # pylint: disable=protected-access
            obj, 'metadata')
        if metadata is None:
            raise bootstrap_v1.BootstrapInvariantViolation(
                f'{kind} metadata is absent.')
        return (
            bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    metadata, 'name'),
                name=f'{kind}.name'),
            bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    metadata, 'namespace'),
                name=f'{kind}.namespace'),
            bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    metadata, 'uid'),
                name=f'{kind}.uid'),
            bootstrap_v1._required_text(  # pylint: disable=protected-access
                bootstrap_v1._field(  # pylint: disable=protected-access
                    metadata, 'resource_version'),
                name=f'{kind}.resourceVersion'),
        )

    def observe(
            self,
            database_now: datetime.datetime) -> AuthorityWorkerObservationV2:
        namespace = self._pod_identity.namespace
        pod = self._core_api.read_namespaced_pod(
            self._pod_identity.name,
            namespace,
            _request_timeout=(_KUBERNETES_REQUEST_TIMEOUT))
        pod_name, pod_namespace, pod_uid, _ = self._metadata_identity(
            pod, kind='Pod')
        if ((pod_name, pod_namespace, pod_uid)
                != (self._pod_identity.name, self._pod_identity.namespace,
                    self._pod_identity.uid)):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Live Pod identity differs from downward-API identity.')
        replica_set_name, replica_set_uid = bootstrap_v1._controller_owner(  # pylint: disable=protected-access
            bootstrap_v1._field(  # pylint: disable=protected-access
                pod, 'metadata'),
            kind='Pod',
            expected_kind='ReplicaSet')
        replica_set = self._apps_api.read_namespaced_replica_set(
            replica_set_name,
            namespace,
            _request_timeout=_KUBERNETES_REQUEST_TIMEOUT)
        live_rs_name, live_rs_namespace, live_rs_uid, _ = (
            self._metadata_identity(replica_set, kind='ReplicaSet'))
        if ((live_rs_name, live_rs_namespace, live_rs_uid)
                != (replica_set_name, namespace, replica_set_uid)):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Live ReplicaSet differs from the Pod owner reference.')
        deployment_name, deployment_uid = bootstrap_v1._controller_owner(  # pylint: disable=protected-access
            bootstrap_v1._field(  # pylint: disable=protected-access
                replica_set, 'metadata'),
            kind='ReplicaSet',
            expected_kind='Deployment')
        if deployment_name != self._manifest.deployment_name:
            raise bootstrap_v1.BootstrapInvariantViolation(
                'ReplicaSet owner is not the V2 manifest Deployment.')
        deployment = self._apps_api.read_namespaced_deployment(
            deployment_name,
            namespace,
            _request_timeout=_KUBERNETES_REQUEST_TIMEOUT)
        live_deployment_name, live_namespace, live_deployment_uid, _ = (
            self._metadata_identity(deployment, kind='Deployment'))
        if ((live_deployment_name, live_namespace, live_deployment_uid)
                != (deployment_name, namespace, deployment_uid)):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Live Deployment differs from the ReplicaSet owner reference.')
        service_account = self._core_api.read_namespaced_service_account(
            self._manifest.service_account_name,
            namespace,
            _request_timeout=_KUBERNETES_REQUEST_TIMEOUT)
        sa_name, sa_namespace, _, _ = self._metadata_identity(
            service_account, kind='ServiceAccount')
        if ((sa_name, sa_namespace)
                != (self._manifest.service_account_name, namespace)):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Live ServiceAccount differs from the V2 manifest identity.')
        objects = bootstrap_v1.KubernetesAuthorityWorkerObjects(
            pod=pod,
            replica_set=replica_set,
            deployment=deployment,
            service_account=service_account)
        snapshot = project_deployment_snapshot_v2(self._manifest, deployment,
                                                  database_now)
        return self._projector(self._manifest, database_now, objects, snapshot)


class AuthorityWorkerRegistrationStoreV2(Protocol):
    """Narrow Serve038 state surface used by membership bootstrap."""

    def read_database_clock(self) -> datetime.datetime:
        ...

    def read_worker_bootstrap_state(
        self,
        cohort_id: str,
        worker_instance_id: uuid.UUID,
    ) -> authority_state.WorkerBootstrapState | None:
        ...

    def register_initial_member(
        self,
        *,
        helm_release_name: str,
        cohort: authority.ProviderAuthorityWorkerCohortV2,
        worker: authority.ProviderAuthorityWorkerIdentityV2,
        operation_id: uuid.UUID,
    ) -> authority_state.WorkerRegistrationMutation:
        ...

    def append_registering_member(
        self,
        *,
        helm_release_name: str,
        cohort: authority.ProviderAuthorityWorkerCohortV2,
        worker: authority.ProviderAuthorityWorkerIdentityV2,
        expected_cohort_revision: int,
        operation_id: uuid.UUID,
    ) -> authority_state.WorkerRegistrationMutation:
        ...

    def activate_initial_cohort(
        self,
        *,
        cohort_id: str,
        expected_cohort_revision: int,
        deployment_snapshot: authority.
        ProviderAuthorityWorkerDeploymentSnapshotV2,
    ) -> authority_state.WorkerCohortActivationMutation:
        ...

    def renew_own_lease(
        self,
        *,
        cohort_id: str,
        worker: authority.ProviderAuthorityWorkerIdentityV2,
        expected_generation: int,
        operation_id: uuid.UUID,
    ) -> authority_state.WorkerRegistrationMutation:
        ...


class AuthorityWorkerObserverV2(Protocol):

    def observe(
            self,
            database_now: datetime.datetime) -> AuthorityWorkerObservationV2:
        ...


class AuthorityWorkerBootstrapCoordinatorV2:
    """Register, activate, and renew one immutable Serve038 worker."""

    def __init__(
        self,
        manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
        pod_identity: bootstrap_v1.AuthorityWorkerPodIdentity,
        observer: AuthorityWorkerObserverV2,
        store: AuthorityWorkerRegistrationStoreV2,
        *,
        static_evidence_loader: (
            Callable[[], authority.ProviderAuthorityWorkerCohortManifestV2] |
            None) = None,
        reconcile_interval_seconds: float = _DEFAULT_RECONCILE_INTERVAL_SECONDS,
        stop_join_timeout_seconds: float = _DEFAULT_STOP_JOIN_TIMEOUT_SECONDS,
        mutation_fence_timeout_seconds: float = (
            _DEFAULT_MUTATION_FENCE_TIMEOUT_SECONDS),
        fail_stop: Callable[[int], NoReturn] = os._exit,
    ) -> None:
        if type(manifest
               ) is not authority.ProviderAuthorityWorkerCohortManifestV2:
            raise TypeError('manifest must be an exact V2 manifest.')
        if type(pod_identity) is not bootstrap_v1.AuthorityWorkerPodIdentity:
            raise TypeError('pod_identity has an invalid type.')
        if pod_identity.namespace != manifest.namespace:
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Downward Pod namespace differs from the V2 manifest.')
        if reconcile_interval_seconds <= 0:
            raise ValueError('reconcile_interval_seconds must be positive.')
        if stop_join_timeout_seconds <= 0:
            raise ValueError('stop_join_timeout_seconds must be positive.')
        if mutation_fence_timeout_seconds <= 0:
            raise ValueError('mutation_fence_timeout_seconds must be positive.')
        if not callable(fail_stop):
            raise TypeError('fail_stop must be callable.')
        self._manifest = manifest
        self._pod_identity = pod_identity
        self._worker_instance_id = uuid.UUID(pod_identity.uid)
        self._observer = observer
        self._store = store
        self._static_evidence_loader = static_evidence_loader
        self._reconcile_interval_seconds = reconcile_interval_seconds
        self._stop_join_timeout_seconds = stop_join_timeout_seconds
        self._mutation_fence_timeout_seconds = mutation_fence_timeout_seconds
        self._fail_stop = fail_stop
        self._stop = threading.Event()
        # Every mutating store call and every local acceptance publication is
        # linearized through this gate.  ``stop()`` first sets the event and
        # then crosses this gate, so an observer/read that returns after the
        # bounded join can no longer mutate or publish.  If a mutation is
        # already in flight, stop waits for it to finish before returning.
        self._mutation_gate = threading.Lock()
        self._stopped_fenced = False
        self._failure_lock = threading.Lock()
        self._failure: BaseException | None = None
        self._accepted_lock = threading.Lock()
        self._accepted: authority_state.WorkerCohortV2Record | None = None
        self._thread = threading.Thread(target=self._run,
                                        name='authority-worker-bootstrap-v2',
                                        daemon=True)

    @property
    def failure(self) -> BaseException | None:
        with self._failure_lock:
            return self._failure

    def clear_acceptance(self) -> None:
        with self._accepted_lock:
            self._accepted = None

    def _require_running(self) -> None:
        if self._stop.is_set() or self._stopped_fenced:
            raise _BootstrapStopped('V2 membership bootstrap has stopped.')

    def _mutate_while_running(self, mutation: Callable[[], Any]) -> Any:
        """Run one DB mutation before the stop fence can complete."""

        with self._mutation_gate:
            self._require_running()
            return mutation()

    def accepted_manifest(
            self) -> authority.ProviderAuthorityWorkerCohortManifestV2 | None:
        """Expose membership for diagnostics, never as a V2 evaluator."""

        with self._accepted_lock:
            record = self._accepted
        if (record is None or record.lifecycle_state
                is not resource_actions.WorkerCohortLifecycleState.ACCEPTING):
            return None
        return record.cohort.manifest

    def _adopt_acceptance(self,
                          record: authority_state.WorkerCohortV2Record) -> None:
        with self._mutation_gate:
            self._require_running()
            with self._accepted_lock:
                self._accepted = record

    def _validate_static_evidence(self) -> None:
        if self._static_evidence_loader is None:
            return
        try:
            reloaded = self._static_evidence_loader()
        except Exception as e:
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Projected V2 static evidence failed closed revalidation.'
            ) from e
        if (type(reloaded)
                is not authority.ProviderAuthorityWorkerCohortManifestV2 or
                reloaded.canonical_bytes != self._manifest.canonical_bytes):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Projected V2 static evidence changed after startup.')

    def _require_observation(
        self,
        observation: AuthorityWorkerObservationV2,
    ) -> None:
        if type(observation) is not AuthorityWorkerObservationV2:
            raise bootstrap_v1.BootstrapInvariantViolation(
                'V2 observer returned an untyped result.')
        if (observation.cohort.manifest.canonical_bytes
                != self._manifest.canonical_bytes or
                observation.worker.pod_uid != self._worker_instance_id):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Projected V2 observation differs from local manifest/Pod UID.')

    @staticmethod
    def _same_stable_worker(
        left: authority.ProviderAuthorityWorkerIdentityV2,
        right: authority.ProviderAuthorityWorkerIdentityV2,
    ) -> bool:
        return (
            authority.project_stable_worker_identity_v1(left).canonical_bytes ==
            authority.project_stable_worker_identity_v1(right).canonical_bytes)

    def _read_or_create(
        self,
        observation: AuthorityWorkerObservationV2,
    ) -> authority_state.WorkerBootstrapState:
        self._require_running()
        state = self._store.read_worker_bootstrap_state(
            self._manifest.cohort_id, self._worker_instance_id)
        self._require_running()
        if state is not None:
            return state
        mutation = self._mutate_while_running(
            lambda: self._store.register_initial_member(
                helm_release_name=(self._manifest.pod_template_binding.
                                   release_inputs.helm_full_name),
                cohort=observation.cohort,
                worker=observation.worker,
                operation_id=uuid.uuid4()))
        return authority_state.WorkerBootstrapState(mutation.cohort,
                                                    mutation.lease)

    def _append_if_needed(
        self,
        state: authority_state.WorkerBootstrapState,
        observation: AuthorityWorkerObservationV2,
    ) -> authority_state.WorkerBootstrapState:
        record = state.cohort
        own_registration = record.registration_set.registration_for(
            self._worker_instance_id)
        if own_registration is not None:
            if not self._same_stable_worker(own_registration.worker,
                                            observation.worker):
                raise bootstrap_v1.BootstrapInvariantViolation(
                    'Live V2 worker changed a stable membership field.')
            return state
        if (record.lifecycle_state
                is not resource_actions.WorkerCohortLifecycleState.REGISTERING
                or record.revision != 1 or
                len(record.registration_set.workers) != 1):
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Replacement Pod UID is outside the current V2 membership.')
        mutation = self._mutate_while_running(
            lambda: self._store.append_registering_member(
                helm_release_name=(self._manifest.pod_template_binding.
                                   release_inputs.helm_full_name),
                cohort=observation.cohort,
                worker=observation.worker,
                expected_cohort_revision=1,
                operation_id=uuid.uuid4()))
        return authority_state.WorkerBootstrapState(mutation.cohort,
                                                    mutation.lease)

    def _renew(
        self,
        state: authority_state.WorkerBootstrapState,
        observation: AuthorityWorkerObservationV2,
    ) -> authority_state.WorkerBootstrapState:
        lease = state.own_lease
        if lease is None:
            raise bootstrap_v1.BootstrapInvariantViolation(
                'Current V2 member has no own registration lease.')
        self._require_running()
        lease_check_now = self._store.read_database_clock()
        self._require_running()
        if not lease.is_fresh(lease_check_now):
            raise bootstrap_v1.BootstrapUnavailable(
                'Own V2 registration lease expired before renewal.')
        mutation = self._mutate_while_running(
            lambda: self._store.renew_own_lease(
                cohort_id=self._manifest.cohort_id,
                worker=observation.worker,
                expected_generation=lease.generation,
                operation_id=uuid.uuid4()))
        return authority_state.WorkerBootstrapState(mutation.cohort,
                                                    mutation.lease)

    def run_once(self) -> authority_state.WorkerCohortV2Record:
        """Perform one bounded DB-clock-first V2 membership reconciliation."""

        self._require_running()
        self.clear_acceptance()
        last_conflict: authority_state.AuthorityStateConflict | None = None
        for _ in range(_BOUNDED_RECONCILE_ATTEMPTS):
            try:
                self._validate_static_evidence()
                self._require_running()
                database_now = self._store.read_database_clock()
                self._require_running()
                observation = self._observer.observe(database_now)
                self._require_running()
                self._require_observation(observation)
                state = self._read_or_create(observation)
                record = state.cohort
                if (record.cohort.canonical_bytes
                        != observation.cohort.canonical_bytes):
                    raise bootstrap_v1.BootstrapInvariantViolation(
                        'Live V2 cohort differs from the retained immutable row.'
                    )
                state = self._append_if_needed(state, observation)
                record = state.cohort
                if record.lifecycle_state in (
                        resource_actions.WorkerCohortLifecycleState.
                        REMOVAL_AUTHORIZED,
                        resource_actions.WorkerCohortLifecycleState.RETIRED):
                    raise bootstrap_v1.BootstrapUnavailable(
                        'V2 cohort is removal-authorized or retired.')
                state = self._renew(state, observation)
                record = state.cohort
                if record.lifecycle_state is (
                        resource_actions.WorkerCohortLifecycleState.REGISTERING
                ):
                    if record.revision == 1:
                        return record
                    if record.revision != 2:
                        raise bootstrap_v1.BootstrapInvariantViolation(
                            'V2 REGISTERING revision is outside the initial '
                            'one/two-member shapes.')
                    self._require_running()
                    final_now = self._store.read_database_clock()
                    self._require_running()
                    final_observation = self._observer.observe(final_now)
                    self._require_running()
                    self._require_observation(final_observation)
                    if (final_observation.cohort.canonical_bytes
                            != record.cohort.canonical_bytes):
                        raise bootstrap_v1.BootstrapInvariantViolation(
                            'Final V2 Deployment observation changed cohort '
                            'identity.')
                    with self._mutation_gate:
                        self._require_running()
                        activated = self._store.activate_initial_cohort(
                            cohort_id=record.cohort_id,
                            expected_cohort_revision=2,
                            deployment_snapshot=(
                                final_observation.deployment_snapshot))
                    self._adopt_acceptance(activated.cohort)
                    return activated.cohort
                if record.lifecycle_state in (
                        resource_actions.WorkerCohortLifecycleState.ACCEPTING,
                        resource_actions.WorkerCohortLifecycleState.DRAINING):
                    self._adopt_acceptance(record)
                    return record
                raise bootstrap_v1.BootstrapInvariantViolation(
                    'V2 cohort lifecycle state is unsupported by bootstrap.')
            except authority_state.AuthorityStateSuperseded as e:
                last_conflict = e
                continue
            except authority_state.AuthorityStateConflict as e:
                last_conflict = e
                continue
        raise bootstrap_v1.BootstrapUnavailable(
            'V2 cohort changed or lacked complete activation evidence '
            'throughout bounded reconciliation.') from last_conflict

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except (bootstrap_v1.BootstrapInvariantViolation,
                    authority_state.AuthorityStateCorruption) as e:
                self.clear_acceptance()
                with self._failure_lock:
                    self._failure = e
                return
            except Exception:  # pylint: disable=broad-except
                self.clear_acceptance()
            self._stop.wait(self._reconcile_interval_seconds)

    def start(self) -> None:
        self._require_running()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self.clear_acceptance()
        # Crossing the same gate as every mutation/publication is the hard
        # fence.  A thread blocked in read-only Kubernetes/DB observation may
        # outlive the bounded join, but it cannot write or publish afterwards.
        # The production store first gets graceful PostgreSQL statement/lock
        # limits.  A DBAPI/network blackhole can defeat those, so this dedicated
        # role has a whole-mutation deadline too.  Missing it invokes a
        # process-boundary NoReturn fail-stop: OS connection close rolls back
        # uncommitted work, and stop never returns into a possible late commit.
        acquired = self._mutation_gate.acquire(
            timeout=self._mutation_fence_timeout_seconds)
        if not acquired:
            self.clear_acceptance()
            self._fail_stop(_FAIL_STOP_EXIT_CODE)
            raise RuntimeError(  # pylint: disable=unreachable
                'Authority worker fail-stop unexpectedly returned.')
        try:
            self._stopped_fenced = True
            with self._accepted_lock:
                self._accepted = None
        finally:
            self._mutation_gate.release()
        if self._thread.is_alive():
            self._thread.join(timeout=self._stop_join_timeout_seconds)


def build_default_coordinator_v2(
    manifest: authority.ProviderAuthorityWorkerCohortManifestV2,
    pod_identity: bootstrap_v1.AuthorityWorkerPodIdentity,
) -> AuthorityWorkerBootstrapCoordinatorV2:
    """Build the in-cluster V2 membership coordinator with authority off."""

    if type(manifest) is not authority.ProviderAuthorityWorkerCohortManifestV2:
        raise TypeError('manifest must be an exact V2 manifest.')
    if type(pod_identity) is not bootstrap_v1.AuthorityWorkerPodIdentity:
        raise TypeError('pod_identity has an invalid type.')
    if pod_identity.namespace != manifest.namespace:
        raise bootstrap_v1.BootstrapInvariantViolation(
            'Downward Pod namespace differs from the V2 manifest.')
    # These imports are dedicated-role-only.  The explicit in-cluster context
    # prevents ambient kubeconfig fallback and the store remains PostgreSQL.
    # pylint: disable=import-outside-toplevel
    from sky.adaptors import kubernetes
    from sky.serve import resource_action_provider_preflight
    context = kubernetes.in_cluster_context_name()
    core_api = kubernetes.core_api(context)
    apps_api = kubernetes.apps_api(context)
    serializer = kubernetes.kubernetes.client.ApiClient(
    ).sanitize_for_serialization
    template_validator = CanonicalAuthorityWorkerTemplateValidatorV2(serializer)
    projector = DefaultAuthorityWorkerLiveProjectorV2(template_validator)
    observer = ReadOnlyKubernetesAuthorityWorkerObserverV2(
        manifest, pod_identity, core_api, apps_api, projector)
    store = authority_state.ServeResourceActionAuthorityStore(
        serve_state_schema.get_database_engine())
    return AuthorityWorkerBootstrapCoordinatorV2(
        manifest,
        pod_identity,
        observer,
        store,
        static_evidence_loader=(
            resource_action_provider_preflight.
            load_provider_authority_worker_static_evidence_v2))


__all__ = [
    'AuthorityWorkerBootstrapCoordinatorV2',
    'AuthorityWorkerObservationV2',
    'CanonicalAuthorityWorkerTemplateValidatorV2',
    'DefaultAuthorityWorkerLiveProjectorV2',
    'ReadOnlyKubernetesAuthorityWorkerObserverV2',
    'build_default_coordinator_v2',
    'project_deployment_snapshot_v2',
]
