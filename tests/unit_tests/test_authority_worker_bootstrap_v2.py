"""Serve038 V2 bootstrap tests with provider authority kept disabled."""
# pylint: disable=missing-class-docstring,protected-access

import copy
import dataclasses
import datetime
import threading
from typing import Any, NoReturn
import uuid

import pytest
from test_serve_resource_action_authority_state_pg import _cohort
from test_serve_resource_action_authority_state_pg import _worker

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_authority_state as authority_state
from sky.serve import resource_action_provider_preflight as provider_preflight
from sky.serve import resource_actions
from sky.server import runtime
from sky.server.requests import authority_worker_bootstrap as bootstrap_v1
from sky.server.requests import authority_worker_bootstrap_v2 as bootstrap_v2

_UTC = datetime.timezone.utc
_NOW = datetime.datetime(2026, 8, 3, 1, 2, 3, 4, tzinfo=_UTC)
_OWN_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_PEER_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')


def _timestamp(value: datetime.datetime) -> str:
    return authority.datetime_to_timestamp(value, name='test timestamp')


def _snapshot(
    observed_at: datetime.datetime,
    *,
    resource_version: str = 'deployment-rv-final',
) -> authority.ProviderAuthorityWorkerDeploymentSnapshotV2:
    cohort = _cohort()
    return authority.ProviderAuthorityWorkerDeploymentSnapshotV2(
        version=2,
        deployment_name=cohort.manifest.deployment_name,
        deployment_uid=cohort.deployment_uid,
        deployment_resource_version=resource_version,
        deployment_generation=5,
        deployment_observed_generation=5,
        pod_template_contract_sha256=cohort.manifest.pod_template_contract.
        sha256,
        deployment_strategy='RollingUpdate',
        deployment_max_surge=0,
        deployment_max_unavailable=1,
        deployment_spec_replicas=2,
        deployment_status_replicas=2,
        deployment_updated_replicas=2,
        deployment_ready_replicas=2,
        deployment_available_replicas=2,
        deployment_unavailable_replicas=0,
        observed_at=_timestamp(observed_at))


def _registration(
    worker: authority.ProviderAuthorityWorkerIdentityV2,
    renewed_at: datetime.datetime,
) -> authority.ProviderAuthorityWorkerRegistrationV2:
    worker = dataclasses.replace(worker, observed_at=_timestamp(renewed_at))
    return authority.ProviderAuthorityWorkerRegistrationV2(
        version=2,
        worker_instance_id=worker.pod_uid,
        worker=worker,
        pod_ready=True,
        registered_at=_timestamp(renewed_at))


def _lease(
    registration: authority.ProviderAuthorityWorkerRegistrationV2,
    renewed_at: datetime.datetime,
    *,
    generation: int = 1,
) -> authority.ProviderAuthorityWorkerLeaseV1:
    operation = (authority.WorkerRegistrationLeaseOperation.INSERT if generation
                 == 1 else authority.WorkerRegistrationLeaseOperation.RENEW)
    return authority.ProviderAuthorityWorkerLeaseV1(
        version=1,
        worker_instance_id=registration.worker_instance_id,
        generation=generation,
        state=authority.WorkerRegistrationLeaseState.ACTIVE,
        renewal_registration=registration,
        renewal_registration_sha256=registration.sha256,
        renewed_at=_timestamp(renewed_at),
        expires_at=_timestamp(renewed_at + datetime.timedelta(seconds=60)),
        revoked_at=None,
        revocation_reason=None,
        revocation_owner_id=None,
        last_operation_id=uuid.uuid4(),
        last_operation_kind=operation,
        revision=generation)


def _record(
    registrations: tuple[authority.ProviderAuthorityWorkerRegistrationV2, ...],
    state: resource_actions.WorkerCohortLifecycleState,
    revision: int,
    *,
    snapshot: authority.ProviderAuthorityWorkerDeploymentSnapshotV2 |
    None = None,
) -> authority_state.WorkerCohortV2Record:
    cohort = _cohort()
    registration_set = authority.ProviderAuthorityWorkerRegistrationSetV2(
        version=2,
        cohort_identity_sha256=cohort.sha256,
        revision=revision,
        deployment_snapshot=snapshot,
        workers=tuple(
            sorted(registrations,
                   key=lambda item: item.worker_instance_id.bytes)))
    return authority_state.WorkerCohortV2Record(
        cohort=cohort,
        registration_set=registration_set,
        lifecycle_state=state,
        revision=revision,
        created_at=_NOW,
        state_changed_at=_NOW,
        removal_authorized_at=None,
        retired_at=None)


def _deployment() -> dict:
    cohort = _cohort()
    return {
        'metadata': {
            'name': cohort.manifest.deployment_name,
            'uid': cohort.deployment_uid,
            'resource_version': 'deployment-rv',
            'generation': 5,
        },
        'spec': {
            'replicas': 2,
            'strategy': {
                'type': 'RollingUpdate',
                'rolling_update': {
                    'max_surge': 0,
                    'max_unavailable': 1,
                },
            },
        },
        'status': {
            'observed_generation': 5,
            'replicas': 2,
            'updated_replicas': 2,
            'ready_replicas': 2,
            'available_replicas': 2,
            'unavailable_replicas': 0,
        },
    }


def _live_objects() -> bootstrap_v1.KubernetesAuthorityWorkerObjects:
    cohort = _cohort()
    manifest = cohort.manifest
    worker = _worker(_OWN_ID, _NOW)
    deployment = _deployment()
    deployment.update({
        'api_version': 'apps/v1',
        'kind': 'Deployment',
    })
    deployment['metadata']['namespace'] = manifest.namespace
    return bootstrap_v1.KubernetesAuthorityWorkerObjects(
        pod={
            'api_version': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': worker.pod_name,
                'namespace': manifest.namespace,
                'uid': str(worker.pod_uid),
                'resource_version': worker.pod_resource_version,
                'owner_references': [{
                    'api_version': 'apps/v1',
                    'kind': 'ReplicaSet',
                    'name': worker.replica_set_name,
                    'uid': worker.replica_set_uid,
                    'controller': True,
                }],
            },
            'spec': {
                'service_account_name': manifest.service_account_name,
                'containers': [{
                    'name': manifest.container_name,
                    'image': manifest.image.requested_reference,
                    'image_pull_policy': 'Always',
                }],
            },
            'status': {
                'container_statuses': [{
                    'name': manifest.container_name,
                    'image': manifest.image.requested_reference,
                    'image_id': worker.image.runtime.raw_image_id,
                }],
                'conditions': [{
                    'type': 'Ready',
                    'status': 'True',
                }],
            },
        },
        replica_set={
            'api_version': 'apps/v1',
            'kind': 'ReplicaSet',
            'metadata': {
                'name': worker.replica_set_name,
                'namespace': manifest.namespace,
                'uid': worker.replica_set_uid,
                'resource_version': worker.replica_set_resource_version,
                'owner_references': [{
                    'api_version': 'apps/v1',
                    'kind': 'Deployment',
                    'name': manifest.deployment_name,
                    'uid': cohort.deployment_uid,
                    'controller': True,
                }],
            },
        },
        deployment=deployment,
        service_account={
            'api_version': 'v1',
            'kind': 'ServiceAccount',
            'metadata': {
                'name': manifest.service_account_name,
                'namespace': manifest.namespace,
                'uid': cohort.service_account_uid,
                'resource_version': 'service-account-rv',
            },
        })


def _template_bound_live_objects(
) -> bootstrap_v1.KubernetesAuthorityWorkerObjects:
    manifest = _cohort().manifest
    objects = _live_objects()
    expected = (provider_preflight.
                materialize_provider_authority_worker_pod_template_v1(
                    manifest.pod_template_binding.release_inputs,
                    manifest.sha256).canonical_value())
    deployment_template = copy.deepcopy(expected)
    deployment_template['metadata']['creationTimestamp'] = None
    objects.deployment['spec']['selector'] = {
        'matchLabels': copy.deepcopy(expected['metadata']['labels'])
    }
    objects.deployment['spec']['template'] = deployment_template

    replica_set_template = copy.deepcopy(deployment_template)
    replica_set_template['metadata']['labels']['pod-template-hash'] = 'abc123'
    objects.replica_set['spec'] = {
        'selector': {
            'matchLabels': copy.deepcopy(
                replica_set_template['metadata']['labels'])
        },
        'template': replica_set_template,
    }
    objects.pod['metadata']['labels'] = copy.deepcopy(
        replica_set_template['metadata']['labels'])
    objects.pod['metadata']['annotations'] = copy.deepcopy(
        expected['metadata']['annotations'])
    objects.pod['spec'] = copy.deepcopy(expected['spec'])
    objects.pod['spec']['nodeName'] = 'worker-node-1'
    return objects


def test_v2_snapshot_requires_exact_integer_rolling_update_contract() -> None:
    snapshot = bootstrap_v2.project_deployment_snapshot_v2(
        _cohort().manifest, _deployment(), _NOW)
    assert snapshot.deployment_strategy == 'RollingUpdate'
    assert snapshot.deployment_max_surge == 0
    assert snapshot.deployment_max_unavailable == 1

    recreate = _deployment()
    recreate['spec']['strategy'] = {
        'type': 'Recreate',
        'rolling_update': None,
    }
    with pytest.raises(bootstrap_v1.BootstrapInvariantViolation,
                       match='RollingUpdate'):
        bootstrap_v2.project_deployment_snapshot_v2(_cohort().manifest,
                                                    recreate, _NOW)
    string_integer = _deployment()
    string_integer['spec']['strategy']['rolling_update']['max_surge'] = '0'
    with pytest.raises(bootstrap_v1.BootstrapInvariantViolation,
                       match='integer'):
        bootstrap_v2.project_deployment_snapshot_v2(_cohort().manifest,
                                                    string_integer, _NOW)


def test_v2_template_validator_uses_shared_leaf_without_crossing_manifest_roots(
) -> None:
    manifest = _cohort().manifest
    objects = _template_bound_live_objects()
    validator: bootstrap_v2.LiveTemplateValidatorV2 = (
        bootstrap_v2.CanonicalAuthorityWorkerTemplateValidatorV2(
            lambda value: value))

    validator(manifest, objects)

    drifted = copy.deepcopy(objects)
    drifted.deployment['spec']['template']['metadata']['annotations'][
        'skypilot.co/resource-action-manifest-sha256'] = '0' * 64
    with pytest.raises(bootstrap_v1.BootstrapInvariantViolation,
                       match='release binding'):
        validator(manifest, drifted)

    v1_value = manifest.canonical_value()
    v1_value['version'] = 1
    v1_value['claim_contract'] = 'frozen_action_cohort_join_v1'
    crossed_manifest: Any = (
        resource_actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
            v1_value))
    with pytest.raises(TypeError, match='exact V2 manifest'):
        validator(crossed_manifest, objects)


def test_v2_observer_projects_only_self_and_owner_chain_four_gets() -> None:
    cohort = _cohort()
    worker = _worker(_OWN_ID, _NOW)
    objects = _live_objects()
    calls: list[tuple[str, str, str]] = []

    class CoreApi:

        def read_namespaced_pod(self, name, namespace, *, _request_timeout):
            assert _request_timeout == (5, 10)
            calls.append(('pod', name, namespace))
            return objects.pod

        def read_namespaced_service_account(self, name, namespace, *,
                                            _request_timeout):
            assert _request_timeout == (5, 10)
            calls.append(('service-account', name, namespace))
            return objects.service_account

    class AppsApi:

        def read_namespaced_replica_set(self, name, namespace, *,
                                        _request_timeout):
            assert _request_timeout == (5, 10)
            calls.append(('replica-set', name, namespace))
            return objects.replica_set

        def read_namespaced_deployment(self, name, namespace, *,
                                       _request_timeout):
            assert _request_timeout == (5, 10)
            calls.append(('deployment', name, namespace))
            return objects.deployment

    projector = bootstrap_v2.DefaultAuthorityWorkerLiveProjectorV2(
        lambda manifest, live_objects: None)
    identity = bootstrap_v1.AuthorityWorkerPodIdentity(
        worker.pod_name, cohort.manifest.namespace, str(_OWN_ID))
    observer = bootstrap_v2.ReadOnlyKubernetesAuthorityWorkerObserverV2(
        cohort.manifest, identity, CoreApi(), AppsApi(), projector)

    observation = observer.observe(_NOW)

    assert observation.cohort.canonical_bytes == cohort.canonical_bytes
    assert observation.worker.canonical_bytes == worker.canonical_bytes
    assert observation.deployment_snapshot.canonical_bytes == (
        bootstrap_v2.project_deployment_snapshot_v2(cohort.manifest,
                                                    objects.deployment,
                                                    _NOW).canonical_bytes)
    assert calls == [
        ('pod', worker.pod_name, cohort.manifest.namespace),
        ('replica-set', worker.replica_set_name, cohort.manifest.namespace),
        ('deployment', cohort.manifest.deployment_name,
         cohort.manifest.namespace),
        ('service-account', cohort.manifest.service_account_name,
         cohort.manifest.namespace),
    ]


@pytest.mark.parametrize(('object_name', 'field', 'value', 'match'), [
    ('pod', 'api_version', 'apps/v1', 'TypeMeta'),
    ('replica_set', 'kind', 'Deployment', 'TypeMeta'),
    ('deployment', 'api_version', 'v1', 'TypeMeta'),
    ('service_account', 'kind', 'Secret', 'TypeMeta'),
])
def test_v2_projector_rejects_crossed_kubernetes_typemeta(
        object_name: str, field: str, value: str, match: str) -> None:
    objects = _live_objects()
    getattr(objects, object_name)[field] = value
    projector = bootstrap_v2.DefaultAuthorityWorkerLiveProjectorV2(
        lambda manifest, live_objects: None)

    with pytest.raises(bootstrap_v1.BootstrapInvariantViolation, match=match):
        projector(
            _cohort().manifest, _NOW, objects,
            bootstrap_v2.project_deployment_snapshot_v2(_cohort().manifest,
                                                        objects.deployment,
                                                        _NOW))


def test_v2_projector_rejects_deleting_or_unready_live_objects() -> None:
    projector = bootstrap_v2.DefaultAuthorityWorkerLiveProjectorV2(
        lambda manifest, live_objects: None)
    deleting = _live_objects()
    deleting.pod['metadata']['deletion_timestamp'] = _timestamp(_NOW)
    with pytest.raises(bootstrap_v1.BootstrapUnavailable, match='deleting'):
        projector(
            _cohort().manifest, _NOW, deleting,
            bootstrap_v2.project_deployment_snapshot_v2(_cohort().manifest,
                                                        deleting.deployment,
                                                        _NOW))

    unready = _live_objects()
    unready.pod['status']['conditions'][0]['status'] = 'False'
    with pytest.raises(bootstrap_v1.BootstrapUnavailable, match='not Ready'):
        projector(
            _cohort().manifest, _NOW, unready,
            bootstrap_v2.project_deployment_snapshot_v2(_cohort().manifest,
                                                        unready.deployment,
                                                        _NOW))


def test_v2_observer_rejects_crossed_owner_uid_before_projection() -> None:
    cohort = _cohort()
    worker = _worker(_OWN_ID, _NOW)
    objects = _live_objects()
    objects.replica_set['metadata']['uid'] = 'crossed-replica-set-uid'

    class CoreApi:

        def read_namespaced_pod(self, *_args, **_kwargs):
            return objects.pod

        def read_namespaced_service_account(self, *_args, **_kwargs):
            return objects.service_account

    class AppsApi:

        def read_namespaced_replica_set(self, *_args, **_kwargs):
            return objects.replica_set

        def read_namespaced_deployment(self, *_args, **_kwargs):
            return objects.deployment

    observer = bootstrap_v2.ReadOnlyKubernetesAuthorityWorkerObserverV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)), CoreApi(),
        AppsApi(),
        bootstrap_v2.DefaultAuthorityWorkerLiveProjectorV2(
            lambda manifest, live_objects: None))

    with pytest.raises(bootstrap_v1.BootstrapInvariantViolation,
                       match='Pod owner reference'):
        observer.observe(_NOW)


def test_v2_coordinator_appends_renews_and_activates_without_claim_surface(
) -> None:
    cohort = _cohort()
    own_worker = _worker(_OWN_ID, _NOW)
    peer_worker = _worker(_PEER_ID, _NOW)
    peer_registration = _registration(peer_worker, _NOW)
    initial = _record((peer_registration,),
                      resource_actions.WorkerCohortLifecycleState.REGISTERING,
                      1)
    initial_state = authority_state.WorkerBootstrapState(initial, None)
    initial_observation = bootstrap_v2.AuthorityWorkerObservationV2(
        cohort, own_worker, _snapshot(_NOW, resource_version='initial-rv'))
    final_observation = bootstrap_v2.AuthorityWorkerObservationV2(
        cohort,
        dataclasses.replace(
            own_worker,
            pod_resource_version='201',
            replica_set_resource_version='202',
            observed_at=_timestamp(_NOW + datetime.timedelta(seconds=1))),
        _snapshot(_NOW + datetime.timedelta(seconds=1)))
    calls: list[str] = []

    class Observer:

        def __init__(self) -> None:
            self._observations = iter((initial_observation, final_observation))

        def observe(self, database_now):
            calls.append('observe')
            observation = next(self._observations)
            assert observation.deployment_snapshot.observed_at == _timestamp(
                database_now)
            return observation

    class Store:

        def __init__(self) -> None:
            self.state = initial_state
            self.own_lease = None
            self.clocks = iter(
                (_NOW, _NOW, _NOW + datetime.timedelta(seconds=1)))

        def read_database_clock(self):
            calls.append('clock')
            return next(self.clocks)

        def read_worker_bootstrap_state(self, cohort_id, worker_instance_id):
            calls.append('read')
            assert cohort_id == cohort.cohort_id
            assert worker_instance_id == _OWN_ID
            return self.state

        def register_initial_member(self, **kwargs):
            raise AssertionError(f'unexpected initial insert: {kwargs!r}')

        def append_registering_member(self, **kwargs):
            calls.append('append')
            assert kwargs['expected_cohort_revision'] == 1
            assert kwargs[
                'worker'].canonical_bytes == own_worker.canonical_bytes
            own_registration = _registration(own_worker, _NOW)
            self.own_lease = _lease(own_registration, _NOW)
            record = _record(
                (peer_registration, own_registration),
                resource_actions.WorkerCohortLifecycleState.REGISTERING, 2)
            self.state = authority_state.WorkerBootstrapState(
                record, self.own_lease)
            return authority_state.WorkerRegistrationMutation(
                record, self.own_lease)

        def renew_own_lease(self, **kwargs):
            calls.append('renew')
            assert self.own_lease is not None
            assert kwargs['expected_generation'] == 1
            registration = _registration(kwargs['worker'], _NOW)
            self.own_lease = _lease(registration, _NOW, generation=2)
            self.state = authority_state.WorkerBootstrapState(
                self.state.cohort, self.own_lease)
            return authority_state.WorkerRegistrationMutation(
                self.state.cohort, self.own_lease)

        def activate_initial_cohort(self, **kwargs):
            calls.append('activate')
            assert kwargs['expected_cohort_revision'] == 2
            accepted = _record(
                self.state.cohort.registration_set.workers,
                resource_actions.WorkerCohortLifecycleState.ACCEPTING,
                3,
                snapshot=kwargs['deployment_snapshot'])
            self.state = authority_state.WorkerBootstrapState(
                accepted, self.own_lease)
            return authority_state.WorkerCohortActivationMutation(accepted)

    static_loads: list[bool] = []

    def load_static():
        static_loads.append(True)
        return cohort.manifest

    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(own_worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)),
        Observer(),
        Store(),
        static_evidence_loader=load_static)
    accepted = coordinator.run_once()

    assert accepted.lifecycle_state is (
        resource_actions.WorkerCohortLifecycleState.ACCEPTING)
    assert coordinator.accepted_manifest() == cohort.manifest
    assert static_loads == [True]
    assert calls == [
        'clock', 'observe', 'read', 'append', 'clock', 'renew', 'clock',
        'observe', 'activate'
    ]


def test_v2_coordinator_rejects_final_cohort_identity_drift() -> None:
    cohort = _cohort()
    own_worker = _worker(_OWN_ID, _NOW)
    peer_registration = _registration(_worker(_PEER_ID, _NOW), _NOW)
    own_registration = _registration(own_worker, _NOW)
    record = _record((peer_registration, own_registration),
                     resource_actions.WorkerCohortLifecycleState.REGISTERING, 2)
    lease = _lease(own_registration, _NOW)
    drifted_cohort = dataclasses.replace(cohort,
                                         service_account_uid='drifted-sa-uid')
    drifted_worker = dataclasses.replace(own_worker,
                                         service_account_uid='drifted-sa-uid')
    observations = iter(
        (bootstrap_v2.AuthorityWorkerObservationV2(cohort, own_worker,
                                                   _snapshot(_NOW)),
         bootstrap_v2.AuthorityWorkerObservationV2(drifted_cohort,
                                                   drifted_worker,
                                                   _snapshot(_NOW))))

    class Store:

        def read_database_clock(self):
            return _NOW

        def read_worker_bootstrap_state(self, *_args):
            return authority_state.WorkerBootstrapState(record, lease)

        def renew_own_lease(self, **_kwargs):
            return authority_state.WorkerRegistrationMutation(record, lease)

        def activate_initial_cohort(self, **_kwargs):
            raise AssertionError('activation reached after final cohort drift')

    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(own_worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)),
        dataclasses.make_dataclass(
            'Observer',
            [('observe', object)])(lambda database_now: next(observations)),
        Store())

    with pytest.raises(bootstrap_v1.BootstrapInvariantViolation,
                       match='Final V2 Deployment observation changed'):
        coordinator.run_once()


def test_v2_stop_fences_observer_unblocked_after_bounded_join() -> None:
    cohort = _cohort()
    worker = _worker(_OWN_ID, _NOW)
    observation = bootstrap_v2.AuthorityWorkerObservationV2(
        cohort, worker, _snapshot(_NOW))
    entered = threading.Event()
    release = threading.Event()
    writes: list[str] = []

    class Observer:

        def observe(self, _database_now):
            entered.set()
            assert release.wait(timeout=2)
            return observation

    class Store:

        def read_database_clock(self):
            return _NOW

        def read_worker_bootstrap_state(self, *_args):
            raise AssertionError('store read reached after stop fence')

        def register_initial_member(self, **_kwargs):
            writes.append('register')
            raise AssertionError('post-stop registration reached')

    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)),
        Observer(),
        Store(),
        reconcile_interval_seconds=60,
        stop_join_timeout_seconds=0.01)
    coordinator.start()
    assert entered.wait(timeout=1)

    coordinator.stop()
    assert coordinator.accepted_manifest() is None
    release.set()
    coordinator._thread.join(timeout=1)  # pylint: disable=protected-access

    assert not coordinator._thread.is_alive()  # pylint: disable=protected-access
    assert not writes


def test_v2_stop_fences_store_read_unblocked_after_bounded_join() -> None:
    cohort = _cohort()
    worker = _worker(_OWN_ID, _NOW)
    observation = bootstrap_v2.AuthorityWorkerObservationV2(
        cohort, worker, _snapshot(_NOW))
    entered = threading.Event()
    release = threading.Event()
    writes: list[str] = []

    class Observer:

        def observe(self, _database_now):
            return observation

    class Store:

        def read_database_clock(self):
            return _NOW

        def read_worker_bootstrap_state(self, *_args):
            entered.set()
            assert release.wait(timeout=2)
            return None

        def register_initial_member(self, **_kwargs):
            writes.append('register')
            raise AssertionError('post-stop registration reached')

    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)),
        Observer(),
        Store(),
        reconcile_interval_seconds=60,
        stop_join_timeout_seconds=0.01)
    coordinator.start()
    assert entered.wait(timeout=1)

    coordinator.stop()
    release.set()
    coordinator._thread.join(timeout=1)  # pylint: disable=protected-access

    assert not coordinator._thread.is_alive()  # pylint: disable=protected-access
    assert not writes
    assert coordinator.accepted_manifest() is None


def test_v2_stop_waits_for_inflight_mutation_before_returning() -> None:
    cohort = _cohort()
    worker = _worker(_OWN_ID, _NOW)
    observation = bootstrap_v2.AuthorityWorkerObservationV2(
        cohort, worker, _snapshot(_NOW))
    registration = _registration(worker, _NOW)
    record = _record((registration,),
                     resource_actions.WorkerCohortLifecycleState.REGISTERING, 1)
    lease = _lease(registration, _NOW)
    mutation_entered = threading.Event()
    mutation_release = threading.Event()
    stop_returned = threading.Event()
    writes: list[str] = []

    class Observer:

        def observe(self, _database_now):
            return observation

    class Store:

        def read_database_clock(self):
            return _NOW

        def read_worker_bootstrap_state(self, *_args):
            return None

        def register_initial_member(self, **_kwargs):
            writes.append('register')
            mutation_entered.set()
            assert mutation_release.wait(timeout=2)
            return authority_state.WorkerRegistrationMutation(record, lease)

    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)),
        Observer(),
        Store(),
        reconcile_interval_seconds=60,
        stop_join_timeout_seconds=0.01)
    coordinator.start()
    assert mutation_entered.wait(timeout=1)

    def stop() -> None:
        coordinator.stop()
        stop_returned.set()

    stopper = threading.Thread(target=stop)
    stopper.start()
    assert not stop_returned.wait(timeout=0.05)
    mutation_release.set()
    assert stop_returned.wait(timeout=1)
    stopper.join(timeout=1)
    coordinator._thread.join(timeout=1)  # pylint: disable=protected-access

    assert writes == ['register']
    assert coordinator.accepted_manifest() is None
    assert not coordinator._thread.is_alive()  # pylint: disable=protected-access


def test_v2_stop_fail_stops_at_whole_mutation_fence_deadline() -> None:
    cohort = _cohort()
    worker = _worker(_OWN_ID, _NOW)
    peer = _worker(_PEER_ID, _NOW)
    observation = bootstrap_v2.AuthorityWorkerObservationV2(
        cohort, worker, _snapshot(_NOW))
    registration = _registration(worker, _NOW)
    peer_registration = _registration(peer, _NOW)
    record = _record((registration,),
                     resource_actions.WorkerCohortLifecycleState.REGISTERING, 1)
    accepted = _record((registration, peer_registration),
                       resource_actions.WorkerCohortLifecycleState.ACCEPTING,
                       3,
                       snapshot=_snapshot(_NOW))
    lease = _lease(registration, _NOW)
    mutation_entered = threading.Event()
    mutation_release = threading.Event()
    fail_stop_codes: list[int] = []

    class Observer:

        def observe(self, _database_now):
            return observation

    class Store:

        def read_database_clock(self):
            return _NOW

        def read_worker_bootstrap_state(self, *_args):
            return None

        def register_initial_member(self, **_kwargs):
            mutation_entered.set()
            assert mutation_release.wait(timeout=2)
            return authority_state.WorkerRegistrationMutation(record, lease)

    class FailStopInvoked(RuntimeError):
        pass

    def fail_stop(code: int) -> NoReturn:
        fail_stop_codes.append(code)
        raise FailStopInvoked

    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)),
        Observer(),
        Store(),
        reconcile_interval_seconds=60,
        stop_join_timeout_seconds=0.01,
        mutation_fence_timeout_seconds=0.01,
        fail_stop=fail_stop)
    with coordinator._accepted_lock:  # pylint: disable=protected-access
        coordinator._accepted = accepted  # pylint: disable=protected-access
    assert coordinator.accepted_manifest() == cohort.manifest
    coordinator.start()
    assert mutation_entered.wait(timeout=1)

    with pytest.raises(FailStopInvoked):
        coordinator.stop()

    assert fail_stop_codes == [bootstrap_v2._FAIL_STOP_EXIT_CODE]
    assert coordinator.accepted_manifest() is None
    mutation_release.set()
    coordinator._thread.join(timeout=1)  # pylint: disable=protected-access
    assert not coordinator._thread.is_alive()  # pylint: disable=protected-access


@pytest.mark.parametrize('lost_ack', ('insert', 'append', 'activation'))
def test_v2_coordinator_adopts_lost_mutation_acknowledgements(
        lost_ack: str) -> None:
    cohort = _cohort()
    own_worker = _worker(_OWN_ID, _NOW)
    peer_worker = _worker(_PEER_ID, _NOW)
    own_registration = _registration(own_worker, _NOW)
    peer_registration = _registration(peer_worker, _NOW)
    observation = bootstrap_v2.AuthorityWorkerObservationV2(
        cohort, own_worker, _snapshot(_NOW))
    calls = {'insert': 0, 'append': 0, 'activation': 0}

    class Observer:

        def observe(self, _database_now):
            return observation

    class Store:

        def __init__(self) -> None:
            self.lease = None
            if lost_ack == 'insert':
                self.record = None
            elif lost_ack == 'append':
                self.record = _record(
                    (peer_registration,),
                    resource_actions.WorkerCohortLifecycleState.REGISTERING, 1)
            else:
                self.record = _record(
                    (peer_registration, own_registration),
                    resource_actions.WorkerCohortLifecycleState.REGISTERING, 2)
                self.lease = _lease(own_registration, _NOW)

        def read_database_clock(self):
            return _NOW

        def read_worker_bootstrap_state(self, *_args):
            if self.record is None:
                return None
            return authority_state.WorkerBootstrapState(self.record, self.lease)

        def register_initial_member(self, **_kwargs):
            calls['insert'] += 1
            self.record = _record(
                (own_registration,),
                resource_actions.WorkerCohortLifecycleState.REGISTERING, 1)
            self.lease = _lease(own_registration, _NOW)
            if lost_ack == 'insert' and calls['insert'] == 1:
                raise authority_state.AuthorityStateConflict(
                    'insert committed before acknowledgement')
            return authority_state.WorkerRegistrationMutation(
                self.record, self.lease)

        def append_registering_member(self, **_kwargs):
            calls['append'] += 1
            self.record = _record(
                (peer_registration, own_registration),
                resource_actions.WorkerCohortLifecycleState.REGISTERING, 2)
            self.lease = _lease(own_registration, _NOW)
            if lost_ack == 'append' and calls['append'] == 1:
                raise authority_state.AuthorityStateConflict(
                    'append committed before acknowledgement')
            return authority_state.WorkerRegistrationMutation(
                self.record, self.lease)

        def renew_own_lease(self, **_kwargs):
            assert self.record is not None and self.lease is not None
            generation = self.lease.generation + 1
            self.lease = _lease(own_registration, _NOW, generation=generation)
            return authority_state.WorkerRegistrationMutation(
                self.record, self.lease)

        def activate_initial_cohort(self, **kwargs):
            calls['activation'] += 1
            assert self.record is not None
            self.record = _record(
                self.record.registration_set.workers,
                resource_actions.WorkerCohortLifecycleState.ACCEPTING,
                3,
                snapshot=kwargs['deployment_snapshot'])
            if lost_ack == 'activation' and calls['activation'] == 1:
                raise authority_state.AuthorityStateConflict(
                    'activation committed before acknowledgement')
            return authority_state.WorkerCohortActivationMutation(self.record)

    store = Store()
    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(own_worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)), Observer(),
        store)

    result = coordinator.run_once()

    if lost_ack == 'insert':
        assert result.lifecycle_state is (
            resource_actions.WorkerCohortLifecycleState.REGISTERING)
        assert calls == {'insert': 1, 'append': 0, 'activation': 0}
    elif lost_ack == 'append':
        assert result.lifecycle_state is (
            resource_actions.WorkerCohortLifecycleState.ACCEPTING)
        assert calls == {'insert': 0, 'append': 1, 'activation': 1}
    else:
        assert result.lifecycle_state is (
            resource_actions.WorkerCohortLifecycleState.ACCEPTING)
        assert calls == {'insert': 0, 'append': 0, 'activation': 1}


def test_v2_coordinator_rejects_v1_static_reload_before_state_write() -> None:
    cohort = _cohort()
    worker = _worker(_OWN_ID, _NOW)

    class Store:

        def read_database_clock(self):
            raise AssertionError('database read reached after crossed manifest')

    v1_value = cohort.manifest.canonical_value()
    v1_value['version'] = 1
    v1_value['claim_contract'] = 'frozen_action_cohort_join_v1'
    v1_manifest = (resource_actions.ProviderAuthorityWorkerCohortManifestV1.
                   from_value(v1_value))
    coordinator = bootstrap_v2.AuthorityWorkerBootstrapCoordinatorV2(
        cohort.manifest,
        bootstrap_v1.AuthorityWorkerPodIdentity(worker.pod_name,
                                                cohort.manifest.namespace,
                                                str(_OWN_ID)),
        dataclasses.make_dataclass('Observer', [])(),
        Store(),
        static_evidence_loader=lambda: v1_manifest)
    with pytest.raises(bootstrap_v1.BootstrapInvariantViolation,
                       match='static evidence changed'):
        coordinator.run_once()


def test_runtime_selects_coordinator_only_by_exact_manifest_type(
        monkeypatch: pytest.MonkeyPatch) -> None:
    v2_manifest = _cohort().manifest
    v1_value = v2_manifest.canonical_value()
    v1_value['version'] = 1
    v1_value['claim_contract'] = 'frozen_action_cohort_join_v1'
    v1_manifest = (resource_actions.ProviderAuthorityWorkerCohortManifestV1.
                   from_value(v1_value))
    pod_identity = bootstrap_v1.AuthorityWorkerPodIdentity(
        'authority-worker', v2_manifest.namespace, str(_OWN_ID))
    v1_result = object()
    v2_result = object()
    v1_builder = lambda manifest, identity: v1_result
    v2_builder = lambda manifest, identity: v2_result
    monkeypatch.setattr(bootstrap_v1, 'build_default_coordinator', v1_builder)
    monkeypatch.setattr(bootstrap_v2, 'build_default_coordinator_v2',
                        v2_builder)

    assert runtime._build_authority_bootstrap_coordinator(  # pylint: disable=protected-access
        v1_manifest, pod_identity) is v1_result
    assert runtime._build_authority_bootstrap_coordinator(  # pylint: disable=protected-access
        v2_manifest, pod_identity) is v2_result
    with pytest.raises(TypeError, match='exact runtime contract'):
        runtime._build_authority_bootstrap_coordinator(  # pylint: disable=protected-access
            object(), pod_identity)
