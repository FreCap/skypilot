"""Closed bootstrap tests for the preflight-only authority role."""

import copy
import datetime
import time
from types import SimpleNamespace

import pytest
import serve_resource_action_test_fixtures as authority_fixtures

from sky.serve import resource_action_provider_preflight as preflight
from sky.serve import resource_action_state
from sky.serve import resource_actions as actions
from sky.server.requests import authority_worker_bootstrap as bootstrap
from sky.server.requests import postgres as request_postgres
from sky.server.requests import resource_actions as kernel_actions

_UTC = datetime.timezone.utc
_NOW = datetime.datetime(2026, 8, 2, 1, 2, 3, 4, tzinfo=_UTC)


def _manifest() -> actions.ProviderAuthorityWorkerCohortManifestV1:
    return actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
        authority_fixtures.authority_manifest_value())


def _cohort() -> actions.WorkerCohortIdentityV1:
    return actions.WorkerCohortIdentityV1.from_value(
        authority_fixtures.authority_cohort_value())


def _timestamp(value: datetime.datetime = _NOW) -> str:
    return value.astimezone(_UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _registration(
    pod_uid: str,
    *,
    at: datetime.datetime = _NOW,
    pod_resource_version: str = '101',
    replica_set_resource_version: str = '102',
    deployment_resource_version: str = '103',
) -> actions.ProviderAuthorityWorkerRegistrationV1:
    worker = authority_fixtures.authority_worker_value(pod_uid)
    worker.update({
        'observed_at': _timestamp(at),
        'pod_resource_version': pod_resource_version,
        'replica_set_resource_version': replica_set_resource_version,
        'deployment_resource_version': deployment_resource_version,
    })
    return actions.ProviderAuthorityWorkerRegistrationV1.from_value({
        'worker': worker,
        'pod_ready': True,
        'deployment_spec_replicas': 2,
        'deployment_status_observed_generation': 5,
        'deployment_status_replicas': 2,
        'deployment_updated_replicas': 2,
        'deployment_ready_replicas': 2,
        'deployment_available_replicas': 2,
        'deployment_unavailable_replicas': 0,
        'registered_at': _timestamp(at),
    })


def _registrations(
    *items: actions.ProviderAuthorityWorkerRegistrationV1,
) -> actions.WorkerCohortRegistrationSetV1:
    cohort = _cohort()
    return actions.WorkerCohortRegistrationSetV1(
        version=1,
        cohort_identity_sha256=cohort.sha256,
        workers=tuple(sorted(items, key=lambda item: item.pod_uid)))


def _record(
    registrations: actions.WorkerCohortRegistrationSetV1,
    state: actions.WorkerCohortLifecycleState,
    revision: int,
) -> resource_action_state.WorkerCohortRecord:
    return resource_action_state.WorkerCohortRecord(
        cohort_identity=_cohort(),
        registration_attestations=registrations,
        lifecycle_state=state,
        revision=revision,
        created_at=_NOW,
        state_changed_at=_NOW,
        retired_at=None)


def _deployment(resource_version: str = '103') -> dict:
    return {
        'api_version': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {
            'name': authority_fixtures.DEPLOYMENT_NAME,
            'namespace': authority_fixtures.NAMESPACE,
            'uid': 'deployment-uid-v1',
            'resource_version': resource_version,
            'generation': 5,
        },
        'spec': {
            'replicas': 2,
            'strategy': {
                'type': 'Recreate',
                'rolling_update': None,
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


def _snapshot(resource_version: str = '103') -> bootstrap.DeploymentSnapshot:
    return bootstrap.DeploymentSnapshot.from_object(
        _deployment(resource_version))


def test_downward_identity_is_canonical_and_owns_server_instance_id() -> None:
    pod_uid = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
    environ = {
        bootstrap.POD_NAME_ENV_VAR: 'authority-pod-0',
        bootstrap.POD_NAMESPACE_ENV_VAR: 'skypilot-system',
        bootstrap.POD_UID_ENV_VAR: pod_uid,
    }

    identity = bootstrap.configure_server_instance_id_from_pod_uid(environ)

    assert identity == bootstrap.AuthorityWorkerPodIdentity(
        'authority-pod-0', 'skypilot-system', pod_uid)
    assert environ[request_postgres.SERVER_INSTANCE_ID_ENV_VAR] == pod_uid
    environ[request_postgres.SERVER_INSTANCE_ID_ENV_VAR] = (
        '22222222-2222-4222-8222-222222222222')
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='differs from the Pod UID'):
        bootstrap.configure_server_instance_id_from_pod_uid(environ)
    environ.pop(request_postgres.SERVER_INSTANCE_ID_ENV_VAR)
    environ[bootstrap.POD_UID_ENV_VAR] = pod_uid.upper()
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='canonical UUID'):
        bootstrap.configure_server_instance_id_from_pod_uid(environ)


def test_deployment_snapshot_requires_exact_current_two_of_two() -> None:
    snapshot = bootstrap.DeploymentSnapshot.from_object(_deployment())
    assert snapshot.spec_replicas == snapshot.status_replicas == 2
    assert snapshot.unavailable_replicas == 0

    unavailable = _deployment()
    unavailable['status']['unavailable_replicas'] = 1
    with pytest.raises(bootstrap.BootstrapUnavailable, match='2/2'):
        bootstrap.DeploymentSnapshot.from_object(unavailable)

    rolling = _deployment()
    rolling['spec']['strategy'] = {
        'type': 'RollingUpdate',
        'rolling_update': {
            'max_surge': '25%',
            'max_unavailable': '25%',
        },
    }
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='exact Recreate'):
        bootstrap.DeploymentSnapshot.from_object(rolling)


def test_template_validator_requires_and_normalizes_only_null_creation_time(
) -> None:
    manifest = _manifest()
    expected = preflight.materialize_provider_authority_worker_pod_template_v1(
        manifest.pod_template_binding.release_inputs,
        manifest.sha256).canonical_value()
    deployment_template = copy.deepcopy(expected)
    deployment_template['metadata']['creationTimestamp'] = None
    replica_set_template = copy.deepcopy(deployment_template)
    replica_set_template['metadata']['labels']['pod-template-hash'] = 'abc123'
    pod_spec = copy.deepcopy(expected['spec'])
    pod_spec['nodeName'] = 'worker-node-1'
    pod_labels = copy.deepcopy(expected['metadata']['labels'])
    pod_labels['pod-template-hash'] = 'abc123'
    objects = bootstrap.KubernetesAuthorityWorkerObjects(
        pod={
            'metadata': {
                'labels': pod_labels,
                'annotations': copy.deepcopy(expected['metadata']['annotations']
                                            ),
            },
            'spec': pod_spec,
        },
        replica_set={
            'spec': {
                'selector': {
                    'matchLabels': copy.deepcopy(
                        replica_set_template['metadata']['labels'])
                },
                'template': replica_set_template,
            }
        },
        deployment={
            'spec': {
                'selector': {
                    'matchLabels': copy.deepcopy(expected['metadata']['labels'])
                },
                'template': deployment_template,
            }
        },
        service_account={})
    validator = bootstrap.CanonicalAuthorityWorkerTemplateValidator(
        lambda value: value)

    validator(manifest, objects)

    extra_selector = copy.deepcopy(objects)
    extra_selector.deployment['spec']['selector']['matchExpressions'] = []
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='closed matchLabels'):
        validator(manifest, extra_selector)
    missing = copy.deepcopy(objects)
    del missing.deployment['spec']['template']['metadata']['creationTimestamp']
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='exact null API default'):
        validator(manifest, missing)
    nonnull = copy.deepcopy(objects)
    nonnull.replica_set['spec']['template']['metadata'][
        'creationTimestamp'] = '2026-08-02T00:00:00Z'
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='exact null API default'):
        validator(manifest, nonnull)


def test_read_only_observer_performs_only_owner_chain_four_gets() -> None:
    calls = []
    pod = {
        'metadata': {
            'name': 'authority-pod-0',
            'namespace': authority_fixtures.NAMESPACE,
            'uid': '11111111-1111-4111-8111-111111111111',
            'resource_version': '101',
            'owner_references': [{
                'api_version': 'apps/v1',
                'kind': 'ReplicaSet',
                'name': f'{authority_fixtures.DEPLOYMENT_NAME}-abc',
                'uid': 'replicaset-uid-v1',
                'controller': True,
            }],
        },
    }
    replica_set = {
        'metadata': {
            'name': f'{authority_fixtures.DEPLOYMENT_NAME}-abc',
            'namespace': authority_fixtures.NAMESPACE,
            'uid': 'replicaset-uid-v1',
            'resource_version': '102',
            'owner_references': [{
                'api_version': 'apps/v1',
                'kind': 'Deployment',
                'name': authority_fixtures.DEPLOYMENT_NAME,
                'uid': 'deployment-uid-v1',
                'controller': True,
            }],
        },
    }
    deployment = _deployment()
    service_account = {
        'metadata': {
            'name': authority_fixtures.DEPLOYMENT_NAME,
            'namespace': authority_fixtures.NAMESPACE,
            'uid': 'service-account-uid-v1',
            'resource_version': '104',
        },
    }

    class CoreApi:

        def read_namespaced_pod(self, name, namespace):
            calls.append(('pod', name, namespace))
            return pod

        def read_namespaced_service_account(self, name, namespace):
            calls.append(('service-account', name, namespace))
            return service_account

    class AppsApi:

        def read_namespaced_replica_set(self, name, namespace):
            calls.append(('replica-set', name, namespace))
            return replica_set

        def read_namespaced_deployment(self, name, namespace):
            calls.append(('deployment', name, namespace))
            return deployment

    projected = SimpleNamespace(value='projected')
    projector_calls = []

    def projector(manifest, database_now, objects, snapshot):
        projector_calls.append((manifest, database_now, objects, snapshot))
        return projected

    identity = bootstrap.AuthorityWorkerPodIdentity(
        'authority-pod-0', authority_fixtures.NAMESPACE,
        '11111111-1111-4111-8111-111111111111')
    observer = bootstrap.ReadOnlyKubernetesAuthorityWorkerObserver(
        _manifest(), identity, CoreApi(), AppsApi(), projector)

    assert observer.observe(_NOW) is projected
    assert calls == [
        ('pod', identity.name, identity.namespace),
        ('replica-set', f'{authority_fixtures.DEPLOYMENT_NAME}-abc',
         identity.namespace),
        ('deployment', authority_fixtures.DEPLOYMENT_NAME, identity.namespace),
        ('service-account', authority_fixtures.DEPLOYMENT_NAME,
         identity.namespace),
    ]
    assert projector_calls[0][1] is _NOW
    observer.require_same_deployment_snapshot(
        _registrations(_registration('pod-a'), _registration('pod-b')))
    assert calls[-1] == ('deployment', authority_fixtures.DEPLOYMENT_NAME,
                         identity.namespace)

    extra_owner = copy.deepcopy(pod['metadata'])
    extra_owner['owner_references'].append({
        'api_version': 'v1',
        'kind': 'Node',
        'name': 'extra-owner',
        'uid': 'extra-owner-uid',
        'controller': False,
    })
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='one closed controller owner'):
        bootstrap._controller_owner(  # pylint: disable=protected-access
            extra_owner,
            kind='Pod',
            expected_kind='ReplicaSet')


def test_runtime_image_identity_accepts_only_qualified_digest_branches(
) -> None:
    qualification = _manifest().image
    containerd = bootstrap._runtime_image_identity(  # pylint: disable=protected-access
        qualification, 'containerd://sha256:' + '2' * 64)
    assert containerd.runtime_image_id_scheme == 'containerd'
    oci_reference = bootstrap._runtime_image_identity(  # pylint: disable=protected-access
        qualification, qualification.requested_reference)
    assert oci_reference.runtime_image_id_scheme == 'oci-reference'
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='qualified OCI'):
        bootstrap._runtime_image_identity(  # pylint: disable=protected-access
            qualification, 'containerd://sha256:' + '9' * 64)


def test_evidence_lease_expires_at_oldest_database_timestamp_deadline() -> None:
    monotonic_now = [100.0]
    evidence = bootstrap.AuthorityWorkerEvidenceLease(
        monotonic=lambda: monotonic_now[0])
    record = _record(
        _registrations(_registration('pod-a'), _registration('pod-b')),
        actions.WorkerCohortLifecycleState.ACCEPTING, 3)

    evidence.adopt(record, _NOW, 'pod-a')
    assert evidence.is_locally_accepted()
    monotonic_now[0] = 399.999
    assert evidence.is_locally_accepted()
    monotonic_now[0] = 400.0
    assert not evidence.is_locally_accepted()
    with pytest.raises(bootstrap.BootstrapUnavailable, match='stale'):
        evidence.adopt(record, _NOW + datetime.timedelta(minutes=5), 'pod-a')
    with pytest.raises(bootstrap.BootstrapUnavailable, match='timezone-aware'):
        evidence.adopt(record, _NOW.replace(tzinfo=None), 'pod-a')
    future_record = _record(
        _registrations(
            _registration('pod-a', at=_NOW + datetime.timedelta(seconds=1)),
            _registration('pod-b')),
        actions.WorkerCohortLifecycleState.ACCEPTING, 3)
    with pytest.raises(bootstrap.BootstrapUnavailable, match='database future'):
        evidence.adopt(future_record, _NOW, 'pod-a')


def test_coordinator_appends_only_own_registration_then_promotes() -> None:
    own = _registration('pod-a')
    peer = _registration('pod-b')
    registering = _record(_registrations(peer),
                          actions.WorkerCohortLifecycleState.REGISTERING, 1)
    observation = bootstrap.AuthorityWorkerObservation(_cohort(), own,
                                                       _snapshot())
    calls = []

    class Store:
        """In-memory REGISTERING lifecycle double."""

        def __init__(self):
            self.record = registering

        def read_database_clock(self):
            calls.append('database-clock')
            return _NOW

        def get_worker_cohort(self, cohort_id):
            assert cohort_id == authority_fixtures.COHORT_ID
            calls.append('get')
            return self.record

        def register_worker_cohort(self, *args):
            raise AssertionError(f'unexpected insert: {args!r}')

        def append_worker_cohort_registration(self, cohort, revision,
                                              predecessor, registration):
            calls.append('append-own')
            assert cohort.canonical_bytes == _cohort().canonical_bytes
            assert revision == 1
            assert predecessor.canonical_bytes == (
                registering.registration_attestations.canonical_bytes)
            assert registration.canonical_bytes == own.canonical_bytes
            self.record = _record(
                _registrations(own, peer),
                actions.WorkerCohortLifecycleState.REGISTERING, 2)
            return resource_action_state.WorkerCohortTransition(self.record)

        def promote_worker_cohort(self, cohort_id, revision, registrations):
            calls.append('promote')
            assert cohort_id == authority_fixtures.COHORT_ID
            assert revision == 2
            self.record = _record(registrations,
                                  actions.WorkerCohortLifecycleState.ACCEPTING,
                                  3)
            return resource_action_state.WorkerCohortTransition(self.record)

        def renew_worker_cohort_registration(self, *args):
            raise AssertionError(f'unexpected renewal: {args!r}')

    class Observer:
        """Live-observation double for initial two-worker registration."""

        def observe(self, database_now):
            calls.append('observe')
            assert database_now is _NOW
            return observation

        def require_same_deployment_snapshot(self, registrations):
            calls.append('final-deployment-read')
            assert registrations.count == 2

    static_loads = []

    def load_static():
        static_loads.append(True)
        return _manifest()

    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-a',
                                             authority_fixtures.NAMESPACE,
                                             'pod-a'),
        Observer(),
        Store(),
        static_evidence_loader=load_static)

    accepted = coordinator.run_once()

    assert accepted.lifecycle_state is (
        actions.WorkerCohortLifecycleState.ACCEPTING)
    assert coordinator.accepted_manifest() == _manifest()
    assert calls == [
        'database-clock', 'get', 'observe', 'append-own',
        'final-deployment-read', 'promote', 'database-clock'
    ]
    assert static_loads == [True]


def test_coordinator_initially_adopts_exact_accepted_row_before_live_read(
) -> None:
    accepted = _record(
        _registrations(_registration('pod-a'), _registration('pod-b')),
        actions.WorkerCohortLifecycleState.ACCEPTING, 3)
    calls = []

    class Store:
        """Accepted-row store double used to prove read-only adoption."""

        def read_database_clock(self):
            calls.append('clock')
            return _NOW

        def get_worker_cohort(self, cohort_id):
            assert cohort_id == authority_fixtures.COHORT_ID
            calls.append('get')
            return accepted

    class Observer:
        """Observer that fails if accepted-row adoption reads Kubernetes."""

        def observe(self, database_now):
            raise AssertionError(f'initial adoption observed: {database_now!r}')

    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-a',
                                             authority_fixtures.NAMESPACE,
                                             'pod-a'), Observer(), Store())

    assert coordinator.run_once() == accepted
    assert coordinator.accepted_manifest() == _manifest()
    assert calls == ['clock', 'get']


def test_coordinator_rejects_frozen_evidence_drift_before_store_renewal(
) -> None:
    own = _registration('pod-a')
    accepted = _record(_registrations(own, _registration('pod-b')),
                       actions.WorkerCohortLifecycleState.ACCEPTING, 3)
    changed = _registration('pod-a', deployment_resource_version='104')
    observation = bootstrap.AuthorityWorkerObservation(_cohort(), changed,
                                                       _snapshot('104'))

    class Store:
        """Accepted-row double which must never receive the drift."""

        def read_database_clock(self):
            return _NOW

        def get_worker_cohort(self, cohort_id):
            del cohort_id
            return accepted

        def renew_worker_cohort_registration(self, *args):
            raise AssertionError(f'renewal reached store: {args!r}')

    observer = SimpleNamespace(observe=lambda database_now: observation)
    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-a',
                                             authority_fixtures.NAMESPACE,
                                             'pod-a'), observer, Store())

    assert coordinator.run_once() == accepted
    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='frozen cohort field'):
        coordinator.run_once()


def test_stale_revision_retry_reobserves_from_a_fresh_database_clock() -> None:
    own = _registration('pod-a')
    peer = _registration('pod-b')
    accepted = _record(_registrations(own, peer),
                       actions.WorkerCohortLifecycleState.ACCEPTING, 3)
    refreshed_at = _NOW + datetime.timedelta(seconds=1)
    refreshed_own = _registration('pod-a',
                                  at=refreshed_at,
                                  pod_resource_version='201',
                                  replica_set_resource_version='202')
    observations = (
        bootstrap.AuthorityWorkerObservation(_cohort(), own, _snapshot()),
        bootstrap.AuthorityWorkerObservation(_cohort(), refreshed_own,
                                             _snapshot()),
    )
    calls = []

    class Store:
        """Stale-revision store double for reconciliation retry behavior."""

        def __init__(self):
            self.record = accepted
            self.renewals = 0

        def read_database_clock(self):
            value = (_NOW if not calls else refreshed_at)
            calls.append(('clock', value))
            return value

        def get_worker_cohort(self, cohort_id):
            assert cohort_id == authority_fixtures.COHORT_ID
            calls.append(('get', self.record.revision))
            return self.record

        def renew_worker_cohort_registration(self, cohort_id, revision, state,
                                             predecessor, registration):
            del cohort_id, state, predecessor
            self.renewals += 1
            calls.append(
                ('renew', revision, registration.worker.pod_resource_version))
            if self.renewals == 1:
                self.record = _record(
                    self.record.registration_attestations,
                    actions.WorkerCohortLifecycleState.ACCEPTING, 4)
                raise kernel_actions.StaleRevision('peer renewed first')
            assert revision == 4
            assert registration.canonical_bytes == refreshed_own.canonical_bytes
            self.record = _record(_registrations(refreshed_own, peer),
                                  actions.WorkerCohortLifecycleState.ACCEPTING,
                                  5)
            return resource_action_state.WorkerCohortTransition(self.record)

    class Observer:
        """Sequence-backed observer for stale-revision retries."""

        def __init__(self):
            self.index = 0

        def observe(self, database_now):
            calls.append(('observe', database_now))
            observation = observations[self.index]
            self.index += 1
            return observation

    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-a',
                                             authority_fixtures.NAMESPACE,
                                             'pod-a'), Observer(), Store())

    assert coordinator.run_once().revision == 3
    assert calls == [('clock', _NOW), ('get', 3)]
    calls.clear()
    record = coordinator.run_once()

    assert record.revision == 5
    assert calls == [('clock', _NOW), ('get', 3), ('observe', _NOW),
                     ('renew', 3, '101'), ('clock', refreshed_at), ('get', 4),
                     ('observe', refreshed_at), ('renew', 4, '201'),
                     ('clock', refreshed_at)]


def test_successful_reconciliation_reclocks_before_lease_adoption() -> None:
    own = _registration('pod-a')
    registrations = _registrations(own, _registration('pod-b'))
    accepted = _record(registrations,
                       actions.WorkerCohortLifecycleState.ACCEPTING, 3)
    post_write_now = _NOW + datetime.timedelta(minutes=4, seconds=59)
    clocks = [_NOW, _NOW, post_write_now]
    monotonic_now = [100.0]
    lease = bootstrap.AuthorityWorkerEvidenceLease(
        monotonic=lambda: monotonic_now[0])

    class Store:
        """Store double exposing a long post-write database-clock interval."""

        def read_database_clock(self):
            return clocks.pop(0)

        def get_worker_cohort(self, cohort_id):
            assert cohort_id == authority_fixtures.COHORT_ID
            return accepted

        def renew_worker_cohort_registration(self, *args):
            del args
            return resource_action_state.WorkerCohortTransition(
                _record(registrations,
                        actions.WorkerCohortLifecycleState.ACCEPTING, 4))

    observation = bootstrap.AuthorityWorkerObservation(_cohort(), own,
                                                       _snapshot())
    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-a',
                                             authority_fixtures.NAMESPACE,
                                             'pod-a'),
        SimpleNamespace(observe=lambda database_now: observation),
        Store(),
        evidence_lease=lease)

    assert coordinator.run_once().revision == 3
    coordinator.run_once()

    assert not clocks
    assert lease.is_locally_accepted()
    monotonic_now[0] = 100.999
    assert lease.is_locally_accepted()
    monotonic_now[0] = 101.0
    assert not lease.is_locally_accepted()


def test_coordinator_rejects_replacement_uid_in_complete_registering_pair(
) -> None:
    registering = _record(
        _registrations(_registration('pod-a'), _registration('pod-b')),
        actions.WorkerCohortLifecycleState.REGISTERING, 2)
    observation = bootstrap.AuthorityWorkerObservation(_cohort(),
                                                       _registration('pod-c'),
                                                       _snapshot())

    class Store:
        """Complete REGISTERING-pair double."""

        def read_database_clock(self):
            return _NOW

        def get_worker_cohort(self, cohort_id):
            del cohort_id
            return registering

        def append_worker_cohort_registration(self, *args):
            raise AssertionError(f'append reached store: {args!r}')

    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-c',
                                             authority_fixtures.NAMESPACE,
                                             'pod-c'),
        SimpleNamespace(observe=lambda database_now: observation), Store())

    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='outside the registering pair'):
        coordinator.run_once()


def test_coordinator_treats_out_of_key_insert_collision_as_fatal() -> None:
    observation = bootstrap.AuthorityWorkerObservation(_cohort(),
                                                       _registration('pod-a'),
                                                       _snapshot())

    class Store:
        """Collision store double used to prove fatal bootstrap handling."""

        def read_database_clock(self):
            return _NOW

        def get_worker_cohort(self, cohort_id):
            assert cohort_id == authority_fixtures.COHORT_ID
            return None

        def register_worker_cohort(self, *args):
            del args
            raise kernel_actions.ActionConflict('deployment UID already bound')

    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-a',
                                             authority_fixtures.NAMESPACE,
                                             'pod-a'),
        SimpleNamespace(observe=lambda database_now: observation), Store())

    with pytest.raises(bootstrap.BootstrapInvariantViolation,
                       match='possible Deployment UID or full-key collision'):
        coordinator.run_once()


def test_coordinator_watchdog_marks_store_invariant_fatal() -> None:

    class Store:
        """Corrupt store double used to exercise watchdog failure capture."""

        def read_database_clock(self):
            raise kernel_actions.InvariantViolation('corrupt row')

    coordinator = bootstrap.AuthorityWorkerBootstrapCoordinator(
        _manifest(),
        bootstrap.AuthorityWorkerPodIdentity('pod-a',
                                             authority_fixtures.NAMESPACE,
                                             'pod-a'),
        SimpleNamespace(),
        Store(),
        reconcile_interval_seconds=0.001)

    coordinator.start()
    for _ in range(100):
        if coordinator.failure is not None:
            break
        time.sleep(0.001)
    coordinator.stop()

    assert isinstance(coordinator.failure, kernel_actions.InvariantViolation)
