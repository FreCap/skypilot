"""Real-PostgreSQL tests for the typed Serve033 state-store slice."""
# pylint: disable=redefined-outer-name,protected-access

import dataclasses
import datetime
import hashlib
import os
import shutil
import uuid

import pytest
import sqlalchemy
from sqlalchemy import orm

from sky.serve import resource_action_state
from sky.serve import resource_action_state_schema
from sky.serve import resource_actions as actions
from sky.serve import serve_state_schema
from sky.server.requests import postgres as request_postgres
from sky.server.requests import resource_actions as kernel_actions

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
testcontainers_postgres = None
if _POSTGRES_URL is None:
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    _POSTGRES_URL is None and shutil.which('docker') is None,
    reason='docker unavailable; skipping Serve033 PostgreSQL store tests')

_SERVICE_UUID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_REPLICA_UUID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CLUSTER_UUID = uuid.UUID('33333333-3333-4333-8333-333333333333')
_UTC = datetime.timezone.utc


@pytest.fixture(scope='module')
def postgres_engine():
    container = None
    admin_engine = None
    temporary_database = None
    if _POSTGRES_URL is None:
        assert testcontainers_postgres is not None
        try:
            container = testcontainers_postgres.PostgresContainer('postgres:16')
            container.start()
        except Exception as e:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {e}')
        assert container is not None
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_033_store_{uuid.uuid4().hex}'
        admin_engine = sqlalchemy.create_engine(_POSTGRES_URL,
                                                isolation_level='AUTOCOMMIT')
        quoted = admin_engine.dialect.identifier_preparer.quote(
            temporary_database)
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE {quoted}')
        except Exception as e:  # pylint: disable=broad-except
            admin_engine.dispose()
            pytest.skip(f'could not create temporary postgres database: {e}')
        postgres_url = sqlalchemy.engine.make_url(_POSTGRES_URL).set(
            database=temporary_database).render_as_string(hide_password=False)
    engine = sqlalchemy.create_engine(postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()
        if temporary_database is not None:
            assert admin_engine is not None
            quoted = admin_engine.dialect.identifier_preparer.quote(
                temporary_database)
            with admin_engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text('SELECT pg_terminate_backend(pid) '
                                    'FROM pg_stat_activity '
                                    'WHERE datname = :database AND '
                                    'pid <> pg_backend_pid()'),
                    {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


@pytest.fixture
def serve033_store(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state_schema.Base.metadata.create_all(postgres_engine)
    resource_action_state_schema.RESOURCE_ACTION_STATE_METADATA.create_all(
        postgres_engine)
    request_postgres.REQUESTS.create(postgres_engine, checkfirst=True)
    return (postgres_engine,
            resource_action_state.PostgresServeResourceActionStateStore(
                postgres_engine))


def _database_now(engine: sqlalchemy.engine.Engine) -> datetime.datetime:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()


def _add_shadow_service(
    engine: sqlalchemy.engine.Engine,
    *,
    candidate_since: datetime.datetime | None = None,
) -> datetime.datetime:
    if candidate_since is None:
        candidate_since = (_database_now(engine) -
                           datetime.timedelta(minutes=1))
    with engine.begin() as connection:
        connection.execute(serve_state_schema.services_table.insert().values(
            name='svc',
            hash=str(_SERVICE_UUID),
            status='READY',
            controller_pid=123,
            controller_ip='10.0.0.1',
            lifecycle_epoch=4,
            resource_action_mode='shadow',
            resource_action_mode_changed_at=candidate_since))
    return candidate_since


def _promotion_report(
    store: resource_action_state.PostgresServeResourceActionStateStore,
) -> resource_action_state.PromotionBlockerReport:
    return store.promotion_blocker_report('svc', str(_SERVICE_UUID))


def _timestamp(value: datetime.datetime) -> str:
    return value.astimezone(_UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _artifact(path: str, digest_character: str) -> dict[str, object]:
    return {
        'repo_path': path,
        'byte_size': 17,
        'sha256': digest_character * 64,
    }


def _qualification() -> dict[str, object]:
    return {
        'requested_reference':
            ('registry.example/authority@sha256:' + '1' * 64),
        'oci_manifest_digest': 'sha256:' + '1' * 64,
        'oci_config_digest': 'sha256:' + '2' * 64,
        'qualification_artifact': _artifact('images/authority.json', '3'),
    }


def _cohort_value() -> dict[str, object]:
    manifest = {
        'version': 1,
        'cohort_id': 'authority-v1',
        'namespace': 'skypilot-system',
        'deployment_name': 'skypilot-authority-v1',
        'service_account_name': 'skypilot-authority-v1',
        'container_name': 'skypilot-authority-worker',
        'image': _qualification(),
        'pod_template_contract': _artifact('charts/worker.yaml', '4'),
        'artifact_inventory': _artifact('inventories/artifacts.json', '5'),
        'callable_inventory': _artifact('inventories/callables.json', '6'),
        'claim_contract': 'frozen_action_cohort_join_v1',
        'handler_allowlist': list(
            actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1),
    }
    return {
        'version': 1,
        'manifest': manifest,
        'manifest_sha256': actions.canonical_sha256(manifest),
        'deployment_uid': 'deployment-uid-v1',
        'service_account_uid': 'service-account-uid-v1',
    }


def _worker_value(cohort: dict[str, object], pod_uid: str,
                  observed_at: str) -> dict[str, object]:
    manifest = cohort['manifest']
    assert isinstance(manifest, dict)
    qualification = _qualification()
    qualification_artifact = qualification['qualification_artifact']
    assert isinstance(qualification_artifact, dict)
    runtime = {
        'raw_image_id': 'containerd://sha256:' + '2' * 64,
        'runtime_image_id_scheme': 'containerd',
        'runtime_image_id_digest': 'sha256:' + '2' * 64,
        'qualified_oci_manifest_digest': 'sha256:' + '1' * 64,
        'qualified_oci_config_digest': 'sha256:' + '2' * 64,
        'qualification_artifact_sha256': qualification_artifact['sha256'],
        'runtime_id_contract': 'qualified_oci_config_digest_v1',
    }
    return {
        'namespace': manifest['namespace'],
        'pod_name': f'worker-{pod_uid}',
        'pod_uid': pod_uid,
        'pod_resource_version': '101',
        'pod_service_account_name': manifest['service_account_name'],
        'pod_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'ReplicaSet',
            'name': 'skypilot-authority-v1-abc',
            'uid': 'replicaset-uid-v1',
        },
        'replica_set_name': 'skypilot-authority-v1-abc',
        'replica_set_uid': 'replicaset-uid-v1',
        'replica_set_resource_version': '102',
        'replica_set_controller_owner': {
            'api_version': 'apps/v1',
            'kind': 'Deployment',
            'name': manifest['deployment_name'],
            'uid': cohort['deployment_uid'],
        },
        'deployment_name': manifest['deployment_name'],
        'deployment_uid': cohort['deployment_uid'],
        'deployment_resource_version': '103',
        'deployment_generation': 5,
        'deployment_observed_generation': 5,
        'pod_template_contract_sha256': manifest['pod_template_contract']
                                        ['sha256'],
        'image': {
            'qualification': qualification,
            'runtime': runtime,
        },
        'service_account_uid': cohort['service_account_uid'],
        'artifact_inventory_sha256': manifest['artifact_inventory']['sha256'],
        'callable_inventory_sha256': manifest['callable_inventory']['sha256'],
        'handler_allowlist_sha256': actions.canonical_sha256(
            manifest['handler_allowlist']),
        'observed_at': observed_at,
    }


def _registration_set(
    cohort_value: dict[str, object],
    pod_uids: tuple[str, ...],
    evidence_time: datetime.datetime,
) -> actions.WorkerCohortRegistrationSetV1:
    timestamp = _timestamp(evidence_time)
    workers = []
    for pod_uid in pod_uids:
        workers.append({
            'worker': _worker_value(cohort_value, pod_uid, timestamp),
            'pod_ready': True,
            'deployment_spec_replicas': 2,
            'deployment_status_observed_generation': 5,
            'deployment_ready_replicas': 2,
            'deployment_available_replicas': 2,
            'registered_at': timestamp,
        })
    return actions.WorkerCohortRegistrationSetV1.from_value({
        'version': 1,
        'cohort_identity_sha256': actions.canonical_sha256(cohort_value),
        'workers': workers,
    })


def _cohort_and_registrations(
    engine: sqlalchemy.engine.Engine,
    *pod_uids: str,
    evidence_time: datetime.datetime | None = None,
) -> tuple[actions.WorkerCohortIdentityV1,
           actions.WorkerCohortRegistrationSetV1]:
    cohort_value = _cohort_value()
    cohort = actions.WorkerCohortIdentityV1.from_value(cohort_value)
    if evidence_time is None:
        evidence_time = _database_now(engine)
    return cohort, _registration_set(cohort_value, tuple(pod_uids),
                                     evidence_time)


def _coverage_identity(
    *,
    replica_id: int = 7,
    generation: int = 3,
    action_type: kernel_actions.ActionKind = kernel_actions.ActionKind.LAUNCH,
) -> actions.CoverageDecisionIdentityV1:
    return actions.CoverageDecisionIdentityV1(version=1,
                                              service_hash=str(_SERVICE_UUID),
                                              service_incarnation=_SERVICE_UUID,
                                              replica_id=replica_id,
                                              replica_incarnation=_REPLICA_UUID,
                                              desired_generation=generation,
                                              action_type=action_type)


def _unsupported_coverage(
    identity: actions.CoverageDecisionIdentityV1,
    *,
    worker_cohort_ref_id: uuid.UUID | None = None,
) -> resource_action_state.NewShadowCoverage:
    if identity.action_type is kernel_actions.ActionKind.LAUNCH:
        reason = actions.ProviderLaunchNotRepresentableReasonV1.REQUEST_CONTRACT
    else:
        reason = actions.ProviderDownNotRepresentableReasonV1.REQUEST_CONTRACT
    return resource_action_state.NewShadowCoverage(
        service_name='svc',
        identity=identity,
        normalization_outcome=actions.NormalizationOutcome.NOT_REPRESENTABLE,
        not_representable_reason=reason,
        worker_cohort_ref_id=worker_cohort_ref_id)


def _reference(
    identity: actions.CoverageDecisionIdentityV1,
    *,
    owner_fence: str = 'owner-fence-7',
    capability_sha256: str | None = None,
) -> actions.WorkerCohortReferenceInputV1:
    if capability_sha256 is None:
        capability_sha256 = actions.canonical_sha256({
            'test_preparation_capability_for': str(identity.decision_id),
        })
    return actions.WorkerCohortReferenceInputV1(
        version=1,
        decision_id=identity.decision_id,
        cohort_id='authority-v1',
        service_hash=identity.service_hash,
        replica_incarnation=identity.replica_incarnation,
        desired_generation=identity.desired_generation,
        action_type=identity.action_type,
        controller_owner_fence=owner_fence,
        lifecycle_epoch=4,
        preparation_capability_sha256=capability_sha256)


def _launch_identity_request(
    identity: actions.CoverageDecisionIdentityV1,
    capability: str,
    *,
    owner_fence: str = '123:10.0.0.1',
    lifecycle_epoch: int = 4,
    capability_sha256: str | None = None,
) -> actions.ProviderLaunchIdentityCanonicalizationRequestV1:
    assert identity.action_type is kernel_actions.ActionKind.LAUNCH
    resource_identity = actions.ProviderResourceIdentityV1(
        service_hash=identity.service_hash,
        service_incarnation=identity.service_incarnation,
        replica_id=identity.replica_id,
        replica_incarnation=identity.replica_incarnation,
        desired_generation=identity.desired_generation)
    canonical_input = actions.ProviderLaunchIdentityCanonicalizationInputV1(
        version=1,
        contract='api_server_effective_launch_identity_v1',
        service_name='svc',
        resource_identity=resource_identity,
        prepared_original_user='prepared@example.com',
        prepared_user_hash='prepared-hash')
    if capability_sha256 is None:
        capability_sha256 = hashlib.sha256(
            bytes.fromhex(capability)).hexdigest()
    context = actions.ProviderLaunchIdentityCanonicalizationContextV1(
        version=1,
        decision_id=identity.decision_id,
        cohort_id='authority-v1',
        action_type=kernel_actions.ActionKind.LAUNCH,
        controller_owner_fence=owner_fence,
        lifecycle_epoch=lifecycle_epoch,
        preparation_reference_revision=1,
        reference_state=actions.WorkerCohortReferenceState.PREPARING,
        preparation_capability_sha256=capability_sha256,
        input=canonical_input,
        input_sha256=canonical_input.sha256)
    return actions.ProviderLaunchIdentityCanonicalizationRequestV1(
        version=1,
        context=context,
        context_sha256=context.sha256,
        preparation_capability=capability)


def _insert_request(
    engine: sqlalchemy.engine.Engine,
    request_id: str,
    *,
    name: str,
    status: str = 'PENDING',
    finished_at: datetime.datetime | None = None,
    resource_action_id: uuid.UUID | None = None,
    resource_action_attempt: int | None = None,
) -> None:
    now = _database_now(engine)
    with engine.begin() as connection:
        connection.execute(request_postgres.REQUESTS.insert().values(
            request_id=request_id,
            name=name,
            handler_name='test-handler',
            payload_type='test-payload',
            payload_format='json',
            payload_version=1,
            producer_version='test',
            payload_json={},
            execution_class='short',
            status=status,
            created_at=now,
            schedule_type='short',
            user_id='test-user',
            should_retry=False,
            finished_at=finished_at,
            ignore_return_value=False,
            retryable=False,
            execution_generation=1,
            resource_action_id=resource_action_id,
            resource_action_attempt=resource_action_attempt,
            updated_at=now))


def _insert_attempt_after_test_fences(
    engine: sqlalchemy.engine.Engine,
    store: resource_action_state.PostgresServeResourceActionStateStore,
    decision_id: uuid.UUID,
    request_sequence: int,
    logical_attempt: int,
    request_role: actions.CoverageAttemptRequestRole,
) -> resource_action_state.CoverageAttemptTransition:
    """Exercise the private ledger primitive after synthetic test fences."""
    with orm.Session(engine) as session, session.begin():
        return store._insert_coverage_attempt_after_external_fences_in_session(
            session, decision_id, request_sequence, logical_attempt,
            request_role)


def _accept_cohort(engine, store):
    cohort, registrations = _cohort_and_registrations(engine, 'pod-a', 'pod-b')
    registered = store.register_worker_cohort(cohort, registrations)
    accepted = store.transition_worker_cohort(
        cohort.cohort_id, registered.record.revision,
        actions.WorkerCohortLifecycleState.REGISTERING,
        actions.WorkerCohortLifecycleState.ACCEPTING)
    return cohort, accepted


def _admit_linked_unsupported(
    engine: sqlalchemy.engine.Engine,
    store: resource_action_state.PostgresServeResourceActionStateStore,
    identity: actions.CoverageDecisionIdentityV1,
) -> None:
    if store.get_worker_cohort('authority-v1') is None:
        _accept_cohort(engine, store)
    reference = _reference(identity, owner_fence='123:10.0.0.1')
    store.prepare_worker_cohort_reference(reference)
    with orm.Session(engine) as session, session.begin():
        store.bind_worker_cohort_reference_in_session(
            session, reference, 1,
            actions.WorkerCohortReferenceState.SHADOW_ACTIVE)
        store.admit_shadow_coverage_in_session(
            session,
            _unsupported_coverage(identity,
                                  worker_cohort_ref_id=identity.decision_id))


def _complete_linked_unsupported(
    engine: sqlalchemy.engine.Engine,
    store: resource_action_state.PostgresServeResourceActionStateStore,
    identity: actions.CoverageDecisionIdentityV1,
    request_id: str,
) -> None:
    _admit_linked_unsupported(engine, store, identity)
    role = (actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH
            if identity.action_type is kernel_actions.ActionKind.LAUNCH else
            actions.CoverageAttemptRequestRole.PRIMARY_DOWN)
    _insert_attempt_after_test_fences(engine, store, identity.decision_id, 1, 1,
                                      role)
    request_name = ('sky.launch' if identity.action_type
                    is kernel_actions.ActionKind.LAUNCH else 'sky.down')
    _insert_request(engine, request_id, name=request_name)
    store.bind_coverage_attempt_request(identity.decision_id, 1, request_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == request_id).values(
                    status='SUCCEEDED',
                    finished_at=sqlalchemy.func.clock_timestamp()))
    store.complete_coverage_attempt(
        identity.decision_id, 1,
        actions.CoverageAttemptTerminalStatus.SUCCEEDED,
        actions.CoverageAttemptRetryDisposition.TERMINAL)


def _activation_evidence(
    *,
    api_revision: str,
    serve_revision: str,
    authority_ready: bool = False,
    candidate_since: datetime.datetime | None = None,
    coverage_inventory_sha256: str = '5' * 64,
) -> resource_action_state.ActivationGateEvidenceV1:
    return resource_action_state.ActivationGateEvidenceV1(
        version=1,
        service_name='svc',
        service_hash=str(_SERVICE_UUID),
        lifecycle_epoch=4,
        candidate_since=candidate_since,
        old_controller_processes_drained=True,
        all_processes_on_approved_image=True,
        approved_image_digest='sha256:' + '1' * 64,
        api_schema_revision=api_revision,
        serve_schema_revision=serve_revision,
        global_user_state_schema_revision='028',
        handler_registered_everywhere=True,
        image_inventory_sha256='2' * 64,
        handler_inventory_sha256='3' * 64,
        provider_profiles_eligible=authority_ready,
        profile_inventory_sha256='4' * 64,
        shadow_coverage_complete=authority_ready,
        coverage_inventory_sha256=coverage_inventory_sha256,
        crash_injection_complete=authority_ready,
        verified_at=datetime.datetime.now(_UTC))


def test_store_and_activation_contract_fail_closed_on_old_dialects() -> None:
    with pytest.raises(RuntimeError, match='requires PostgreSQL'):
        resource_action_state.PostgresServeResourceActionStateStore(
            sqlalchemy.create_engine('sqlite://'))

    _activation_evidence(api_revision='005', serve_revision='033')
    _activation_evidence(api_revision='006', serve_revision='033')
    with pytest.raises(ValueError, match='Serve schema revision 033'):
        _activation_evidence(api_revision='006', serve_revision='032')


def test_worker_cohort_exact_adoption_freshness_and_legal_lifecycle(
        serve033_store) -> None:
    engine, store = serve033_store
    evidence_now = _database_now(engine)
    cohort, first = _cohort_and_registrations(engine,
                                              'pod-a',
                                              evidence_time=evidence_now)
    _, both = _cohort_and_registrations(engine,
                                        'pod-a',
                                        'pod-b',
                                        evidence_time=evidence_now)

    registered = store.register_worker_cohort(cohort, first)
    assert not registered.adopted
    assert store.register_worker_cohort(cohort, first).adopted
    with pytest.raises(ValueError, match='must append'):
        store.transition_worker_cohort(
            cohort.cohort_id, 1, actions.WorkerCohortLifecycleState.REGISTERING,
            actions.WorkerCohortLifecycleState.REGISTERING)

    appended = store.transition_worker_cohort(
        cohort.cohort_id,
        1,
        actions.WorkerCohortLifecycleState.REGISTERING,
        actions.WorkerCohortLifecycleState.REGISTERING,
        registration_attestations=both)
    assert appended.record.revision == 2
    assert store.transition_worker_cohort(
        cohort.cohort_id,
        1,
        actions.WorkerCohortLifecycleState.REGISTERING,
        actions.WorkerCohortLifecycleState.REGISTERING,
        registration_attestations=both).adopted

    accepted = store.transition_worker_cohort(
        cohort.cohort_id, 2, actions.WorkerCohortLifecycleState.REGISTERING,
        actions.WorkerCohortLifecycleState.ACCEPTING)
    assert accepted.record.revision == 3
    assert store.transition_worker_cohort(
        cohort.cohort_id, 2, actions.WorkerCohortLifecycleState.REGISTERING,
        actions.WorkerCohortLifecycleState.ACCEPTING).adopted
    with pytest.raises(ValueError, match='reviewed evidence path'):
        store.transition_worker_cohort(
            cohort.cohort_id, 2,
            actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED,
            actions.WorkerCohortLifecycleState.ACCEPTING)
    with pytest.raises(ValueError, match='reviewed evidence path'):
        store.transition_worker_cohort(
            cohort.cohort_id, 3, actions.WorkerCohortLifecycleState.ACCEPTING,
            actions.WorkerCohortLifecycleState.ACCEPTING)
    draining = store.transition_worker_cohort(
        cohort.cohort_id, 3, actions.WorkerCohortLifecycleState.ACCEPTING,
        actions.WorkerCohortLifecycleState.DRAINING)
    with pytest.raises(ValueError, match='replacement two-worker evidence'):
        store.transition_worker_cohort(
            cohort.cohort_id, 4, actions.WorkerCohortLifecycleState.DRAINING,
            actions.WorkerCohortLifecycleState.ACCEPTING)

    _, replacement = _cohort_and_registrations(engine, 'pod-a', 'pod-b')
    rolled_back = store.transition_worker_cohort(
        cohort.cohort_id,
        draining.record.revision,
        actions.WorkerCohortLifecycleState.DRAINING,
        actions.WorkerCohortLifecycleState.ACCEPTING,
        registration_attestations=replacement)
    assert rolled_back.record.revision == 5
    with pytest.raises(ValueError, match='reviewed evidence path'):
        store.transition_worker_cohort(
            cohort.cohort_id, 5, actions.WorkerCohortLifecycleState.ACCEPTING,
            actions.WorkerCohortLifecycleState.REMOVAL_AUTHORIZED)

    exact_boundary = evidence_now - datetime.timedelta(minutes=5)
    _, boundary = _cohort_and_registrations(engine,
                                            'pod-a',
                                            'pod-b',
                                            evidence_time=exact_boundary)
    resource_action_state._validate_current_worker_registrations(
        cohort, boundary, evidence_now, require_two=True)
    _, stale = _cohort_and_registrations(engine,
                                         'pod-a',
                                         'pod-b',
                                         evidence_time=exact_boundary -
                                         datetime.timedelta(microseconds=1))
    with pytest.raises(kernel_actions.ActionConflict, match='five minutes'):
        resource_action_state._validate_current_worker_registrations(
            cohort, stale, evidence_now, require_two=True)

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                resource_action_state_schema.WORKER_COHORTS).values(
                    cohort_identity_sha256='0' * 64))
    with pytest.raises(kernel_actions.InvariantViolation,
                       match='Invalid Serve worker cohort row'):
        store.get_worker_cohort(cohort.cohort_id)


def test_reference_coverage_and_terminal_release_are_owner_fenced(
        serve033_store) -> None:
    engine, store = serve033_store
    _accept_cohort(engine, store)
    identity = _coverage_identity()
    reference = _reference(identity)

    prepared = store.prepare_worker_cohort_reference(reference)
    assert not prepared.adopted
    assert prepared.record.reference.preparation_capability_sha256 == (
        reference.preparation_capability_sha256)
    assert store.get_worker_cohort_reference(
        reference.decision_id).reference == reference
    assert store.prepare_worker_cohort_reference(reference).adopted
    changed_capability = dataclasses.replace(reference,
                                             preparation_capability_sha256='e' *
                                             64)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='different preparation reference bytes'):
        store.prepare_worker_cohort_reference(changed_capability)
    with pytest.raises(kernel_actions.ClaimLost, match='identity changed'):
        with orm.Session(engine) as session, session.begin():
            store.bind_worker_cohort_reference_in_session(
                session, changed_capability, 1,
                actions.WorkerCohortReferenceState.SHADOW_ACTIVE)
    coverage = _unsupported_coverage(identity,
                                     worker_cohort_ref_id=identity.decision_id)
    with orm.Session(engine) as session, session.begin():
        active = store.bind_worker_cohort_reference_in_session(
            session, reference, 1,
            actions.WorkerCohortReferenceState.SHADOW_ACTIVE)
        admitted = store.admit_shadow_coverage_in_session(session, coverage)
    assert active.record.revision == 2
    assert admitted.record.worker_cohort_ref_id == identity.decision_id
    with orm.Session(engine) as session, session.begin():
        assert store.bind_worker_cohort_reference_in_session(
            session, reference, 1,
            actions.WorkerCohortReferenceState.SHADOW_ACTIVE).adopted
        assert store.admit_shadow_coverage_in_session(session, coverage).adopted
    with pytest.raises(kernel_actions.ClaimLost, match='owner/lifecycle'):
        with orm.Session(engine) as session, session.begin():
            store.bind_worker_cohort_reference_in_session(
                session,
                dataclasses.replace(reference, controller_owner_fence='other'),
                2, actions.WorkerCohortReferenceState.SHADOW_ACTIVE)
    _insert_attempt_after_test_fences(
        engine, store, identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)
    with pytest.raises(kernel_actions.ActionConflict, match='nonterminal'):
        store.release_worker_cohort_reference(reference, 2)
    assert not hasattr(store, 'abandon_coverage_attempt_pre_submit')
    _insert_request(engine, 'reference-request', name='sky.launch')
    store.bind_coverage_attempt_request(identity.decision_id, 1,
                                        'reference-request')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                'reference-request').values(
                    status='SUCCEEDED',
                    finished_at=sqlalchemy.func.clock_timestamp()))
    store.complete_coverage_attempt(
        identity.decision_id, 1,
        actions.CoverageAttemptTerminalStatus.SUCCEEDED,
        actions.CoverageAttemptRetryDisposition.TERMINAL)
    released = store.release_worker_cohort_reference(reference, 2)
    assert released.record.reference_state is actions.WorkerCohortReferenceState.RELEASED
    assert store.release_worker_cohort_reference(reference, 2).adopted

    down_identity = _coverage_identity(
        generation=4, action_type=kernel_actions.ActionKind.DOWN)
    down_reference = _reference(down_identity)
    assert down_reference.preparation_capability_sha256 != (
        reference.preparation_capability_sha256)
    down_prepared = store.prepare_worker_cohort_reference(down_reference)
    assert down_prepared.record.reference.preparation_capability_sha256 == (
        down_reference.preparation_capability_sha256)

    preparing_identity = _coverage_identity(replica_id=8, generation=4)
    preparing_reference = _reference(preparing_identity)
    store.prepare_worker_cohort_reference(preparing_reference)
    with pytest.raises(kernel_actions.StaleRevision,
                       match='Only the expected SHADOW_ACTIVE'):
        store.release_worker_cohort_reference(preparing_reference, 1)

    action_identity = _coverage_identity(replica_id=9, generation=5)
    action_reference = _reference(action_identity)
    store.prepare_worker_cohort_reference(action_reference)
    with orm.Session(engine) as session, session.begin():
        store.bind_worker_cohort_reference_in_session(
            session, action_reference, 1,
            actions.WorkerCohortReferenceState.ACTION_ACTIVE)
    with pytest.raises(kernel_actions.StaleRevision,
                       match='Only the expected SHADOW_ACTIVE'):
        store.release_worker_cohort_reference(action_reference, 2)


def test_reference_reader_rejects_invalid_capability_commitment(
        serve033_store) -> None:
    engine, store = serve033_store
    _accept_cohort(engine, store)
    reference = _reference(_coverage_identity())
    store.prepare_worker_cohort_reference(reference)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'ALTER TABLE serve_resource_action_worker_cohort_refs '
            'DROP CONSTRAINT ck_serve_ra_worker_cohort_refs_capability')
        connection.execute(
            sqlalchemy.update(
                resource_action_state_schema.WORKER_COHORT_REFS).values(
                    preparation_capability_sha256='invalid'))
    with pytest.raises(kernel_actions.InvariantViolation,
                       match='Invalid Serve worker cohort reference row'):
        store.get_worker_cohort_reference(reference.decision_id)


def test_launch_identity_validation_is_one_session_read_only_and_exact(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    _accept_cohort(engine, store)
    identity = _coverage_identity()
    capability = '12' * 32
    commitment = hashlib.sha256(bytes.fromhex(capability)).hexdigest()
    reference = _reference(identity,
                           owner_fence='123:10.0.0.1',
                           capability_sha256=commitment)
    store.prepare_worker_cohort_reference(reference)
    before = store.get_worker_cohort_reference(identity.decision_id)
    checkouts = 0

    def _count_checkout(*_args) -> None:
        nonlocal checkouts
        checkouts += 1

    sqlalchemy.event.listen(engine, 'checkout', _count_checkout)
    try:
        validated = store.validate_launch_identity_canonicalization(
            _launch_identity_request(identity, capability))
    finally:
        sqlalchemy.event.remove(engine, 'checkout', _count_checkout)
    assert checkouts == 1
    assert validated == before
    assert store.get_worker_cohort_reference(identity.decision_id) == before


@pytest.mark.parametrize('request_factory,error_type', [
    (lambda identity: _launch_identity_request(identity, '34' * 32),
     resource_action_state.PreparationCapabilityMismatch),
    (lambda identity: _launch_identity_request(
        identity, '12' * 32, capability_sha256='0' * 64),
     resource_action_state.PreparationCapabilityMismatch),
    (lambda identity: _launch_identity_request(
        identity, '12' * 32, owner_fence='other-owner'),
     kernel_actions.ClaimLost),
    (lambda identity: _launch_identity_request(
        identity, '12' * 32, lifecycle_epoch=5), kernel_actions.ClaimLost),
])
def test_launch_identity_validation_rejects_capability_and_context_drift(
        serve033_store, request_factory, error_type) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    _accept_cohort(engine, store)
    identity = _coverage_identity()
    capability = '12' * 32
    reference = _reference(identity,
                           owner_fence='123:10.0.0.1',
                           capability_sha256=hashlib.sha256(
                               bytes.fromhex(capability)).hexdigest())
    store.prepare_worker_cohort_reference(reference)

    with pytest.raises(error_type):
        store.validate_launch_identity_canonicalization(
            request_factory(identity))


def test_launch_identity_validation_rejects_unknown_active_and_stale_service(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    _accept_cohort(engine, store)
    identity = _coverage_identity()
    capability = '12' * 32
    reference = _reference(identity,
                           owner_fence='123:10.0.0.1',
                           capability_sha256=hashlib.sha256(
                               bytes.fromhex(capability)).hexdigest())
    store.prepare_worker_cohort_reference(reference)

    unknown = _coverage_identity(replica_id=8, generation=4)
    with pytest.raises(kernel_actions.ClaimLost, match='does not exist'):
        store.validate_launch_identity_canonicalization(
            _launch_identity_request(unknown, capability))

    with orm.Session(engine) as session, session.begin():
        store.bind_worker_cohort_reference_in_session(
            session, reference, 1,
            actions.WorkerCohortReferenceState.SHADOW_ACTIVE)
    with pytest.raises(kernel_actions.ClaimLost, match='stale or unequal'):
        store.validate_launch_identity_canonicalization(
            _launch_identity_request(identity, capability))

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                resource_action_state_schema.WORKER_COHORT_REFS).values(
                    reference_state='PREPARING', revision=1, bound_at=None))
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.services_table).values(controller_pid=999))
    with pytest.raises(kernel_actions.ClaimLost, match='service context'):
        store.validate_launch_identity_canonicalization(
            _launch_identity_request(identity, capability))


def test_coverage_is_immutable_deterministic_and_revalidates_raw_rows(
        serve033_store) -> None:
    engine, store = serve033_store
    identity = _coverage_identity()
    coverage = _unsupported_coverage(identity)
    inserted = store.admit_shadow_coverage(coverage)
    assert inserted.record.decision_id == identity.decision_id
    assert not inserted.adopted
    assert store.admit_shadow_coverage(coverage).adopted

    represented = resource_action_state.NewShadowCoverage(
        service_name='svc',
        identity=identity,
        normalization_outcome=actions.NormalizationOutcome.REPRESENTABLE,
        not_representable_reason=None,
        worker_cohort_ref_id=None)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='different immutable coverage bytes'):
        store.admit_shadow_coverage(represented)
    with pytest.raises(TypeError, match='wrong action-kind type'):
        resource_action_state.NewShadowCoverage(
            service_name='svc',
            identity=identity,
            normalization_outcome=actions.NormalizationOutcome.
            NOT_REPRESENTABLE,
            not_representable_reason=(
                actions.ProviderDownNotRepresentableReasonV1.REQUEST_CONTRACT),
            worker_cohort_ref_id=None)

    tampered_identity = _coverage_identity(replica_id=17, generation=8)
    wrong_id = uuid.uuid4()
    assert wrong_id != tampered_identity.decision_id
    with engine.begin() as connection:
        connection.execute(
            resource_action_state_schema.SHADOW_COVERAGE.insert().values(
                decision_id=wrong_id,
                service_name='svc',
                service_hash=tampered_identity.service_hash,
                service_incarnation=tampered_identity.service_incarnation,
                replica_id=tampered_identity.replica_id,
                replica_incarnation=tampered_identity.replica_incarnation,
                desired_generation=tampered_identity.desired_generation,
                action_type=tampered_identity.action_type.value,
                normalizer_contract_version=1,
                normalization_outcome='NOT_REPRESENTABLE',
                not_representable_reason='request_contract',
                worker_cohort_ref_id=None,
                admitted_at=sqlalchemy.func.clock_timestamp()))
    with pytest.raises(kernel_actions.InvariantViolation,
                       match='Invalid Serve shadow coverage row'):
        store.get_shadow_coverage(wrong_id)


def test_coverage_attempt_request_binding_and_terminal_snapshot(
        serve033_store) -> None:
    engine, store = serve033_store
    identity = _coverage_identity()
    store.admit_shadow_coverage(_unsupported_coverage(identity))
    assert not hasattr(store, 'prepare_coverage_attempt')
    assert not hasattr(store, 'prepare_coverage_attempt_in_session')
    prepared = _insert_attempt_after_test_fences(
        engine, store, identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)
    assert prepared.record.phase is actions.CoverageAttemptPhase.PRE_SUBMIT
    assert _insert_attempt_after_test_fences(
        engine, store, identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH).adopted

    with pytest.raises(kernel_actions.ActionConflict,
                       match='missing API request'):
        store.bind_coverage_attempt_request(identity.decision_id, 1,
                                            'request-1')
    _insert_request(engine, 'request-1', name='sky.down')
    with pytest.raises(kernel_actions.ActionConflict, match='kind/correlation'):
        store.bind_coverage_attempt_request(identity.decision_id, 1,
                                            'request-1')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == 'request-1').values(
                    name='sky.launch'))
    bound = store.bind_coverage_attempt_request(identity.decision_id, 1,
                                                'request-1')
    assert bound.record.phase is actions.CoverageAttemptPhase.REQUEST_BOUND
    assert store.bind_coverage_attempt_request(identity.decision_id, 1,
                                               'request-1').adopted

    with pytest.raises(kernel_actions.ActionConflict,
                       match='exact terminal coverage shape'):
        store.complete_coverage_attempt(
            identity.decision_id, 1,
            actions.CoverageAttemptTerminalStatus.SUCCEEDED,
            actions.CoverageAttemptRetryDisposition.RETRY_SAME_DECISION)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == 'request-1').values(
                    name='sky.down',
                    status='SUCCEEDED',
                    finished_at=sqlalchemy.func.clock_timestamp()))
    with pytest.raises(kernel_actions.ActionConflict,
                       match='exact terminal coverage shape'):
        store.complete_coverage_attempt(
            identity.decision_id, 1,
            actions.CoverageAttemptTerminalStatus.SUCCEEDED,
            actions.CoverageAttemptRetryDisposition.RETRY_SAME_DECISION)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == 'request-1').values(
                    name='sky.launch'))
    complete = store.complete_coverage_attempt(
        identity.decision_id, 1,
        actions.CoverageAttemptTerminalStatus.SUCCEEDED,
        actions.CoverageAttemptRetryDisposition.RETRY_SAME_DECISION)
    assert complete.record.phase is actions.CoverageAttemptPhase.COMPLETE
    assert store.complete_coverage_attempt(
        identity.decision_id, 1,
        actions.CoverageAttemptTerminalStatus.SUCCEEDED,
        actions.CoverageAttemptRetryDisposition.RETRY_SAME_DECISION).adopted

    second = _insert_attempt_after_test_fences(
        engine, store, identity.decision_id, 2, 2,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)
    assert second.record.request_sequence == 2
    with pytest.raises(kernel_actions.ActionConflict, match='contiguous'):
        _insert_attempt_after_test_fences(
            engine, store, identity.decision_id, 4, 3,
            actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)

    other_identity = _coverage_identity(replica_id=8, generation=4)
    store.admit_shadow_coverage(_unsupported_coverage(other_identity))
    _insert_attempt_after_test_fences(
        engine, store, other_identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='another shadow attempt'):
        store.bind_coverage_attempt_request(other_identity.decision_id, 1,
                                            'request-1')

    unknown = store.mark_coverage_request_association_unknown(
        identity.decision_id, 2)
    assert unknown.record.phase is actions.CoverageAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN
    assert store.mark_coverage_request_association_unknown(
        identity.decision_id, 2).adopted
    with pytest.raises(kernel_actions.ActionConflict,
                       match='exact retry decision'):
        _insert_attempt_after_test_fences(
            engine, store, identity.decision_id, 3, 3,
            actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)


def test_promotion_requires_coverage_for_live_replica_links(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    missing_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(serve_state_schema.replicas_table.insert().values(
            service_name='svc',
            replica_id=7,
            status='READY',
            replica_incarnation=_REPLICA_UUID,
            desired_generation=3,
            sky_cluster_record_uuid=_CLUSTER_UUID,
            launch_shadow_coverage_id=missing_id))

    report = _promotion_report(store)
    assert missing_id in report.blocking_sample_ids
    assert any(f'decision:{missing_id}:missing_candidate_coverage' in reason
               for reason in report.reasons)


def test_promotion_blocks_live_identified_replica_with_null_launch_links(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    with engine.begin() as connection:
        connection.execute(serve_state_schema.replicas_table.insert().values(
            service_name='svc',
            replica_id=identity.replica_id,
            status='READY',
            replica_incarnation=identity.replica_incarnation,
            desired_generation=identity.desired_generation,
            sky_cluster_record_uuid=_CLUSTER_UUID,
            launch_shadow_coverage_id=None,
            launch_shadow_sample_id=None))

    report = _promotion_report(store)
    assert identity.decision_id in report.blocking_sample_ids
    assert any(f'decision:{identity.decision_id}:'
               'live_replica_missing_launch_coverage' in reason
               for reason in report.reasons)
    assert any(
        f'decision:{identity.decision_id}:missing_candidate_coverage' in reason
        for reason in report.reasons)


def test_promotion_blocks_not_representable_and_active_coverage_attempts(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _admit_linked_unsupported(engine, store, identity)
    _insert_attempt_after_test_fences(
        engine, store, identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)

    report = _promotion_report(store)
    assert identity.decision_id in report.blocking_sample_ids
    assert any(
        'coverage:NOT_REPRESENTABLE' in reason for reason in report.reasons)
    assert any('coverage_attempt_graph:attempt:1:phase:PRE_SUBMIT' in reason
               for reason in report.reasons)

    store.mark_coverage_request_association_unknown(identity.decision_id, 1)
    report = _promotion_report(store)
    assert any('phase:REQUEST_ASSOCIATION_UNKNOWN' in reason
               for reason in report.reasons)


def test_promotion_blocks_unlinked_coverage_and_active_reference(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    unlinked_identity = _coverage_identity()
    store.admit_shadow_coverage(_unsupported_coverage(unlinked_identity))

    _accept_cohort(engine, store)
    reference_identity = _coverage_identity(replica_id=8, generation=4)
    reference = _reference(reference_identity)
    store.prepare_worker_cohort_reference(reference)
    with orm.Session(engine) as session, session.begin():
        store.bind_worker_cohort_reference_in_session(
            session, reference, 1,
            actions.WorkerCohortReferenceState.SHADOW_ACTIVE)

    report = _promotion_report(store)
    assert any('coverage_without_cohort_reference' in reason
               for reason in report.reasons)
    assert any(f'decision:{reference_identity.decision_id}:'
               'active_reference_without_coverage' in reason
               for reason in report.reasons)


def test_promotion_revalidates_reference_and_represented_parent_links(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    mismatched_identity = _coverage_identity()
    _admit_linked_unsupported(engine, store, mismatched_identity)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(resource_action_state_schema.WORKER_COHORT_REFS).
            where(
                resource_action_state_schema.WORKER_COHORT_REFS.c.decision_id ==
                mismatched_identity.decision_id).values(
                    desired_generation=mismatched_identity.desired_generation +
                    1))

    represented_identity = _coverage_identity(replica_id=8, generation=4)
    reference = _reference(represented_identity)
    store.prepare_worker_cohort_reference(reference)
    with orm.Session(engine) as session, session.begin():
        store.bind_worker_cohort_reference_in_session(
            session, reference, 1,
            actions.WorkerCohortReferenceState.SHADOW_ACTIVE)
        store.admit_shadow_coverage_in_session(
            session,
            resource_action_state.NewShadowCoverage(
                service_name='svc',
                identity=represented_identity,
                normalization_outcome=actions.NormalizationOutcome.
                REPRESENTABLE,
                not_representable_reason=None,
                worker_cohort_ref_id=represented_identity.decision_id))

    report = _promotion_report(store)
    assert any(
        'coverage_reference_mismatch' in reason for reason in report.reasons)
    assert any('representable_coverage_missing_parent' in reason
               for reason in report.reasons)


def test_promotion_lock_order_places_coverage_before_parents(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _admit_linked_unsupported(engine, store, identity)
    _insert_attempt_after_test_fences(
        engine, store, identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)
    locked_tables: list[str] = []

    def record_lock_statement(_connection, _cursor, statement, _parameters,
                              _context, _executemany):
        if 'FOR UPDATE' not in statement:
            return
        for table_name in ('services', 'replicas',
                           'ephemeral_storage_cleanup_intents',
                           'serve_resource_action_worker_cohort_refs',
                           'serve_resource_action_shadow_coverage_attempts',
                           'serve_resource_action_shadow_coverage',
                           'serve_resource_action_shadow_samples',
                           'serve_resource_action_shadow_attempts'):
            if f'FROM {table_name}' in statement:
                locked_tables.append(table_name)
                break

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            record_lock_statement)
    try:
        _promotion_report(store)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                record_lock_statement)

    expected_order = ('services', 'replicas',
                      'ephemeral_storage_cleanup_intents',
                      'serve_resource_action_worker_cohort_refs',
                      'serve_resource_action_shadow_coverage',
                      'serve_resource_action_shadow_coverage_attempts',
                      'serve_resource_action_shadow_samples')
    positions = [locked_tables.index(table) for table in expected_order]
    assert positions == sorted(positions)


def test_promotion_recomputes_exact_canonical_coverage_inventory(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _admit_linked_unsupported(engine, store, identity)
    with engine.begin() as connection:
        connection.execute(serve_state_schema.replicas_table.insert().values(
            service_name='svc',
            replica_id=identity.replica_id,
            status='READY',
            replica_incarnation=identity.replica_incarnation,
            desired_generation=identity.desired_generation,
            sky_cluster_record_uuid=_CLUSTER_UUID,
            launch_shadow_coverage_id=identity.decision_id))
    _insert_attempt_after_test_fences(
        engine, store, identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)

    report = _promotion_report(store)
    coverage = store.get_shadow_coverage(identity.decision_id)
    reference = store.get_worker_cohort_reference(identity.decision_id)
    attempts = store.list_coverage_attempts(identity.decision_id)
    assert coverage is not None
    assert reference is not None
    assert reference.bound_at is not None
    expected_inventory = {
        'version': 1,
        'service_name': 'svc',
        'service_hash': str(_SERVICE_UUID),
        'candidate_since': _timestamp(report.candidate_since),
        'decisions': [{
            'decision_id': str(identity.decision_id),
            'coverage': coverage.canonical_value(),
            'cohort_reference': {
                'reference': reference.reference.canonical_value(),
                'reference_state': reference.reference_state.value,
                'revision': reference.revision,
                'created_at': _timestamp(reference.created_at),
                'bound_at': _timestamp(reference.bound_at),
                'released_at': None,
            },
            'replica_links': [{
                'replica_id': identity.replica_id,
                'replica_incarnation': str(identity.replica_incarnation),
                'desired_generation': identity.desired_generation,
                'action_type': identity.action_type.value,
                'coverage_id': str(identity.decision_id),
                'represented_sample_id': None,
            }],
            'represented_parent': None,
            'coverage_attempts': [
                attempt.canonical_value() for attempt in attempts
            ],
        }],
    }
    assert report.coverage_inventory_sha256 == actions.canonical_sha256(
        expected_inventory)

    store.mark_coverage_request_association_unknown(identity.decision_id, 1)
    changed = _promotion_report(store)
    assert changed.coverage_inventory_sha256 != report.coverage_inventory_sha256


def test_authority_transition_requires_locked_inventory_hash_equality(
        serve033_store, monkeypatch) -> None:
    engine, store = serve033_store
    candidate_since = (_database_now(engine) - datetime.timedelta(hours=25))
    _add_shadow_service(engine, candidate_since=candidate_since)
    locked_hash = 'a' * 64
    clean_report = resource_action_state.PromotionBlockerReport(
        service_name='svc',
        service_hash=str(_SERVICE_UUID),
        candidate_since=candidate_since,
        candidate_sample_count=2,
        clean_launch_samples=1,
        clean_down_samples=1,
        blocking_sample_ids=(),
        coverage_inventory_sha256=locked_hash,
        reasons=())
    monkeypatch.setattr(store, '_promotion_report_in_session',
                        lambda *args, **kwargs: clean_report)

    with pytest.raises(kernel_actions.ActionConflict,
                       match='coverage inventory hash'):
        store.transition_service_mode('svc',
                                      str(_SERVICE_UUID), (123, '10.0.0.1'),
                                      actions.ResourceActionMode.SHADOW,
                                      actions.ResourceActionMode.AUTHORITATIVE,
                                      gate_evidence=_activation_evidence(
                                          api_revision='006',
                                          serve_revision='033',
                                          authority_ready=True,
                                          candidate_since=candidate_since,
                                          coverage_inventory_sha256='b' * 64),
                                      expected_lifecycle_epoch=4)

    promoted = store.transition_service_mode(
        'svc',
        str(_SERVICE_UUID), (123, '10.0.0.1'),
        actions.ResourceActionMode.SHADOW,
        actions.ResourceActionMode.AUTHORITATIVE,
        gate_evidence=_activation_evidence(
            api_revision='006',
            serve_revision='033',
            authority_ready=True,
            candidate_since=candidate_since,
            coverage_inventory_sha256=locked_hash),
        expected_lifecycle_epoch=4)
    assert promoted.record.mode is actions.ResourceActionMode.AUTHORITATIVE


def test_promotion_inventory_decision_cap_fails_closed(serve033_store,
                                                       monkeypatch) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    monkeypatch.setattr(resource_action_state,
                        '_MAX_PROMOTION_INVENTORY_DECISIONS', 1)
    with engine.begin() as connection:
        connection.execute(serve_state_schema.replicas_table.insert().values(
            service_name='svc',
            replica_id=7,
            status='READY',
            replica_incarnation=_REPLICA_UUID,
            desired_generation=3,
            sky_cluster_record_uuid=_CLUSTER_UUID,
            launch_shadow_coverage_id=uuid.uuid4(),
            down_shadow_coverage_id=uuid.uuid4()))
    with pytest.raises(kernel_actions.ActionConflict,
                       match='1 decision row cap'):
        _promotion_report(store)


def test_promotion_inventory_attempt_link_cap_fails_closed(
        serve033_store, monkeypatch) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    monkeypatch.setattr(resource_action_state,
                        '_MAX_PROMOTION_INVENTORY_ATTEMPTS_AND_LINKS', 1)
    with engine.begin() as connection:
        connection.execute(serve_state_schema.replicas_table.insert().values(
            service_name='svc',
            replica_id=7,
            status='READY',
            replica_incarnation=_REPLICA_UUID,
            desired_generation=3,
            sky_cluster_record_uuid=_CLUSTER_UUID,
            launch_shadow_coverage_id=uuid.uuid4(),
            down_shadow_coverage_id=uuid.uuid4()))
    with pytest.raises(kernel_actions.ActionConflict,
                       match='1 combined attempt/link row cap'):
        _promotion_report(store)


def test_retention_releases_and_deletes_coverage_only_graph(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _complete_linked_unsupported(engine, store, identity, 'retention-coverage')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))

    result = store.purge_completed_before(
        _database_now(engine) + datetime.timedelta(seconds=1))
    assert result.removed_action_ids == (identity.decision_id,)
    assert store.get_shadow_coverage(identity.decision_id) is None
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(  # pylint: disable=not-callable
                resource_action_state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                    resource_action_state_schema.SHADOW_COVERAGE_ATTEMPTS.c.
                    decision_id == identity.decision_id)).scalar_one() == 0
    reference = store.get_worker_cohort_reference(identity.decision_id)
    assert reference is not None
    assert reference.reference_state is actions.WorkerCohortReferenceState.RELEASED


def test_retention_protects_replica_link_active_reference_and_nonterminal(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _complete_linked_unsupported(engine, store, identity, 'retention-linked')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
        connection.execute(serve_state_schema.replicas_table.insert().values(
            service_name='svc',
            replica_id=identity.replica_id,
            status='READY',
            replica_incarnation=identity.replica_incarnation,
            desired_generation=identity.desired_generation,
            sky_cluster_record_uuid=_CLUSTER_UUID,
            launch_shadow_coverage_id=identity.decision_id))
    cutoff = _database_now(engine) + datetime.timedelta(seconds=1)
    linked = store.purge_completed_before(cutoff)
    assert linked.protected_action_ids == (identity.decision_id,)

    with engine.begin() as connection:
        connection.execute(serve_state_schema.replicas_table.delete())
        connection.execute(
            sqlalchemy.update(
                resource_action_state_schema.WORKER_COHORT_REFS).where(
                    resource_action_state_schema.WORKER_COHORT_REFS.c.
                    decision_id == identity.decision_id).values(
                        reference_state=actions.WorkerCohortReferenceState.
                        ACTION_ACTIVE.value))
    active = store.purge_completed_before(cutoff)
    assert active.protected_action_ids == (identity.decision_id,)

    nonterminal_identity = _coverage_identity(replica_id=8, generation=4)
    _admit_linked_unsupported(engine, store, nonterminal_identity)
    _insert_attempt_after_test_fences(
        engine, store, nonterminal_identity.decision_id, 1, 1,
        actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH)
    nonterminal = store.purge_completed_before(cutoff)
    assert nonterminal_identity.decision_id not in (
        nonterminal.removed_action_ids + nonterminal.protected_action_ids +
        nonterminal.deferred_action_ids)
    assert store.get_shadow_coverage(
        nonterminal_identity.decision_id) is not None


def test_retention_rolls_back_release_and_deletes(serve033_store,
                                                  monkeypatch) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _complete_linked_unsupported(engine, store, identity, 'retention-rollback')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
    original = store._purge_retention_candidate_in_session

    def fail_after_delete(session, decision_id, cutoff):
        assert original(session, decision_id, cutoff) == 'removed'
        raise RuntimeError('inject retention rollback')

    monkeypatch.setattr(store, '_purge_retention_candidate_in_session',
                        fail_after_delete)
    with pytest.raises(RuntimeError, match='inject retention rollback'):
        store.purge_completed_before(
            _database_now(engine) + datetime.timedelta(seconds=1))
    assert store.get_shadow_coverage(identity.decision_id) is not None
    assert len(store.list_coverage_attempts(identity.decision_id)) == 1
    reference = store.get_worker_cohort_reference(identity.decision_id)
    assert reference is not None
    assert reference.reference_state is actions.WorkerCohortReferenceState.SHADOW_ACTIVE


def test_retention_defers_deleted_or_recreated_service(serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _complete_linked_unsupported(engine, store, identity,
                                 'retention-deleted-service')
    cutoff = _database_now(engine) + datetime.timedelta(seconds=1)
    with engine.begin() as connection:
        connection.execute(serve_state_schema.services_table.delete())
    deleted = store.purge_completed_before(cutoff)
    assert deleted.deferred_action_ids == (identity.decision_id,)

    successor_hash = str(uuid.uuid4())
    with engine.begin() as connection:
        connection.execute(serve_state_schema.services_table.insert().values(
            name='svc',
            hash=successor_hash,
            status='READY',
            controller_pid=123,
            controller_ip='10.0.0.1',
            lifecycle_epoch=5,
            resource_action_mode='authoritative'))
    recreated = store.purge_completed_before(cutoff)
    assert recreated.deferred_action_ids == (identity.decision_id,)
    assert store.get_shadow_coverage(identity.decision_id) is not None


def test_retention_lock_order_never_reaches_backward_after_coverage(
        serve033_store) -> None:
    engine, store = serve033_store
    _add_shadow_service(engine)
    identity = _coverage_identity()
    _complete_linked_unsupported(engine, store, identity, 'retention-order')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
    locked_tables = []

    def record_lock_statement(_connection, _cursor, statement, _parameters,
                              _context, _executemany):
        if 'FOR UPDATE' not in statement:
            return
        for table_name in ('services', 'replicas',
                           'ephemeral_storage_cleanup_intents',
                           'serve_resource_action_worker_cohorts',
                           'serve_resource_action_worker_cohort_refs',
                           'serve_resource_action_shadow_coverage_attempts',
                           'serve_resource_action_shadow_coverage',
                           'serve_resource_action_shadow_samples'):
            if f'FROM {table_name}' in statement:
                locked_tables.append(table_name)
                break

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            record_lock_statement)
    try:
        store.purge_completed_before(
            _database_now(engine) + datetime.timedelta(seconds=1))
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                record_lock_statement)
    expected = [
        'services', 'replicas', 'ephemeral_storage_cleanup_intents',
        'serve_resource_action_worker_cohorts',
        'serve_resource_action_worker_cohort_refs',
        'serve_resource_action_shadow_coverage',
        'serve_resource_action_shadow_coverage_attempts',
        'serve_resource_action_shadow_samples'
    ]
    positions = [locked_tables.index(table) for table in expected]
    assert positions == sorted(positions)
