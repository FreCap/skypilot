"""Real-PostgreSQL tests for the typed SkyServe shadow journal store."""
# pylint: disable=redefined-outer-name,protected-access

import concurrent.futures
import datetime
import os
import shutil
import time
import uuid

import pytest
import sqlalchemy

from sky.serve import resource_action_state
from sky.serve import resource_action_state_schema
from sky.serve import resource_actions as actions
from sky.serve import serve_state_schema
from sky.server.requests import resource_actions as kernel_actions

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
testcontainers_postgres = None
if _POSTGRES_URL is None:
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    _POSTGRES_URL is None and shutil.which('docker') is None,
    reason='docker unavailable; skipping Serve shadow PostgreSQL tests')

_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_OTHER_SERVICE_UUID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_CLUSTER_UUID = '33333333-3333-4333-8333-333333333333'
_OWNER = (123, '10.0.0.1')
_LIFECYCLE_EPOCH = 4
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
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_shadow_{uuid.uuid4().hex}'
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
                    sqlalchemy.text(
                        'SELECT pg_terminate_backend(pid) '
                        'FROM pg_stat_activity '
                        'WHERE datname = :database AND pid <> pg_backend_pid()'
                    ), {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


@pytest.fixture
def shadow_database(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state_schema.Base.metadata.create_all(postgres_engine)
    resource_action_state_schema.RESOURCE_ACTION_STATE_METADATA.create_all(
        postgres_engine)
    return postgres_engine, (
        resource_action_state.PostgresServeResourceActionStateStore(
            postgres_engine))


@pytest.fixture
def eligible_profile(monkeypatch):
    """Open only the synthetic store-test profile gate."""
    monkeypatch.setattr(resource_action_state, '_profile_is_authoritative',
                        lambda _: True)


def _identity(generation: int) -> dict:
    return {
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': generation,
    }


def _target() -> dict:
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'cloud': 'kubernetes',
        'region': None,
        'zone': None,
        'sky_cluster_name': 'svc-7',
        'sky_cluster_record_uuid': _CLUSTER_UUID,
        'kubernetes': {
            'cluster_fingerprint_sha256': 'a' * 64,
            'namespace': 'serve-test',
            'workload_kind': 'Pod',
            'workload_name': 'svc-7',
            'cluster_record_uuid_label': _CLUSTER_UUID,
            'replica_incarnation_label': _REPLICA_UUID,
        },
    }


def _launch_invocation(
        generation: int = 1) -> actions.ProviderLifecycleInvocationV1:
    return actions.ProviderLifecycleInvocationV1.from_value({
        'version': 1,
        'profile': 'pod_cluster_v1',
        'redaction_profile': 'provider_lifecycle_redaction_v1',
        'action_kind': 'launch',
        'resource_identity': _identity(generation),
        'requested_target': _target(),
        'launch': {
            'source': {
                'store': 'serve_version_specs',
                'service_name': 'svc',
                'service_incarnation': _SERVICE_UUID,
                'service_version': 1,
                'yaml_content_sha256': 'b' * 64,
                'workspace': 'boltz-test',
            },
            'resources': {
                'version': 1,
                'cloud': 'kubernetes',
                'cluster_fingerprint_sha256': 'a' * 64,
                'namespace': 'serve-test',
                'instance_type': None,
                'accelerator': None,
                'cpus': '2+',
                'memory': '4+',
                'image_id': None,
                'disk_size_gb': 50,
                'disk_tier': None,
                'ports': ['8000'],
                'labels': [],
                'use_spot': False,
            },
            'replica_env': {
                'SKYPILOT_SERVE_REPLICA_ID': '7'
            },
            'security_group_scope': 'serve-svc',
            'admin_policy_input_sha256': 'c' * 64,
            'admin_policy_output_sha256': 'd' * 64,
            'retry_until_up': True,
            'exact_resources_override': True,
            'backend': 'cloud_vm_ray',
            'optimize_target': 'cost',
            'dryrun': False,
            'no_setup': False,
            'clone_disk_from': None,
            'fast': False,
            'file_mounts_blob_id': None,
            'tls_material_ref': None,
        },
        'down': None,
    })


def _down_invocation(
        generation: int = 2) -> actions.ProviderLifecycleInvocationV1:
    return actions.ProviderLifecycleInvocationV1.from_value({
        'version': 1,
        'profile': 'pod_cluster_v1',
        'redaction_profile': 'provider_lifecycle_redaction_v1',
        'action_kind': 'down',
        'resource_identity': _identity(generation),
        'requested_target': _target(),
        'launch': None,
        'down': {
            'cluster_name': 'svc-7',
            'expected_cluster_record_uuid': _CLUSTER_UUID,
            'workspace': 'boltz-test',
            'purge': False,
            'graceful': False,
            'graceful_timeout': None,
        },
    })


def _resolved_target(
    invocation: actions.ProviderLifecycleInvocationV1,
    provider_operation_id: str | None = None,
) -> actions.ResolvedProviderTargetV1:
    return actions.ResolvedProviderTargetV1.from_value({
        'version': 1,
        'requested_target_sha256': invocation.requested_target.sha256,
        'provider_resource_id': 'pod/svc-7',
        'workload_uid': 'uid-7',
        'provider_operation_id': provider_operation_id,
        'resolved_at': '2026-08-01T01:02:03.000004Z',
    })


def _sample(
    action_kind: str = 'launch',
    generation: int = 1,
    eligibility: str = 'UNSUPPORTED',
    placement_sha256: str = 'e' * 64,
) -> tuple[resource_action_state.NewShadowSample,
           actions.ProviderLifecycleInvocationV1]:
    invocation = (_launch_invocation(generation)
                  if action_kind == 'launch' else _down_invocation(generation))
    assert invocation.launch is not None or action_kind == 'down'
    resources_hash = ('1' * 64 if invocation.launch is None else
                      invocation.launch.resources.sha256)
    prior_target = (None if action_kind == 'launch' else
                    _resolved_target(invocation).canonical_value())
    plan = actions.ProviderLifecyclePlanV1.from_value({
        'version': 1,
        'profile': 'pod_cluster_v1',
        'action_kind': action_kind,
        'resource_identity': _identity(generation),
        'placement_decision_sha256': placement_sha256,
        'resources_snapshot_sha256': resources_hash,
        'workspace_identity_sha256': 'f' * 64,
        'requested_target': _target(),
        'prior_resolved_target': prior_target,
        'request_payload_sha256': invocation.sha256,
        'redaction_profile': 'provider_lifecycle_redaction_v1',
    })
    spec = actions.ServeReplicaActionSpecV1.from_value({
        'version': 1,
        'provider_plan': plan.canonical_value(),
        'invocation': invocation.canonical_value(),
    })
    return (resource_action_state.NewShadowSample(
        service_name='svc',
        immutable_spec=spec,
        provider_plan=plan,
        profile_eligibility=actions.ProfileEligibility(eligibility)),
            invocation)


def _observation(
    invocation: actions.ProviderLifecycleInvocationV1,
    *,
    present: bool,
    provider_operation_id: str | None = None,
) -> actions.ProviderLifecycleObservationV1:
    resolved = (_resolved_target(invocation, provider_operation_id)
                if present else None)
    return actions.ProviderLifecycleObservationV1.from_value({
        'version': 1,
        'target_sha256': invocation.requested_target.sha256,
        'state': 'present' if present else 'absent',
        'certainty': 'authoritative',
        'observed_provider_operation_id': provider_operation_id,
        'observed_provider_resource_id': ('pod/svc-7' if present else None),
        'observed_cluster_record_uuid': _CLUSTER_UUID if present else None,
        'observed_workload_uid': 'uid-7' if present else None,
        'observed_replica_incarnation_label': _REPLICA_UUID
                                              if present else None,
        'resolved_target': None
                           if resolved is None else resolved.canonical_value(),
        'ready': True if present else None,
        'evidence_sha256': '4' * 64,
        'observed_at': '2026-08-01T01:02:04.000005Z',
    })


_DEFAULT_OBSERVATION = object()


def _outcome(
    invocation: actions.ProviderLifecycleInvocationV1,
    *,
    disposition: str = 'succeeded',
    observation: actions.ProviderLifecycleObservationV1 | None |
    object = _DEFAULT_OBSERVATION,
    provider_operation_id: str | None = None,
) -> actions.ServeReplicaActionOutcomeV1:
    if observation is _DEFAULT_OBSERVATION:
        observation = (_observation(invocation,
                                    present=invocation.action_kind
                                    is kernel_actions.ActionKind.LAUNCH,
                                    provider_operation_id=provider_operation_id)
                       if disposition == 'succeeded' else None)
    retryable = disposition in ('retryable', 'uncertain')
    retry_class = ('observation_required' if disposition == 'uncertain' else
                   ('transient' if retryable else None))
    return actions.ServeReplicaActionOutcomeV1.from_value({
        'disposition': disposition,
        'certainty': 'observed' if disposition == 'succeeded' else 'unknown',
        'provider_operation_id': provider_operation_id,
        'provider_code': None,
        'retry_class': retry_class,
        'retry_after_seconds': 1 if retryable else None,
        'observation':
            (None if observation is None else observation.canonical_value()),
        'normalized_message': None,
    })


def _retry(
    logical_attempt: int = 1,
    decision: str = 'terminal',
) -> actions.ServeShadowRetryDecisionV1:
    needs_retry = decision in ('retry_same_plan', 'replan_new_generation',
                               'observe')
    return actions.ServeShadowRetryDecisionV1.from_value({
        'version': 1,
        'decision': decision,
        'retry_class': ('observation_required' if decision == 'observe' else
                        ('transient' if needs_retry else None)),
        'delay_seconds': 1 if needs_retry else None,
        'logical_attempt': logical_attempt,
    })


def _projection(
    invocation: actions.ProviderLifecycleInvocationV1,
    *,
    disposition: str = 'succeeded',
    include_resolved_target: bool = True,
) -> actions.ServeShadowProjectionV1:
    launch = invocation.action_kind is kernel_actions.ActionKind.LAUNCH
    succeeded = disposition == 'succeeded'
    return actions.ServeShadowProjectionV1.from_value({
        'version': 1,
        'action_kind': invocation.action_kind.value,
        'row_disposition': 'retained' if launch or not succeeded else 'removed',
        'replica_status': ('READY' if succeeded else 'NOT_READY') if launch else
                          ('READY' if not succeeded else None),
        'capacity_outcome':
            ('success' if succeeded else 'generic_failure') if launch else None,
        'action_disposition': disposition,
        'resolved_target':
            (_resolved_target(invocation).canonical_value()
             if launch and succeeded and include_resolved_target else None),
    })


def _add_service(engine,
                 mode: str = 'shadow',
                 *,
                 changed_at: datetime.datetime | None = None) -> None:
    if mode == 'shadow' and changed_at is None:
        changed_at = datetime.datetime.now(_UTC) - datetime.timedelta(minutes=1)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                hash=_SERVICE_UUID,
                controller_pid=_OWNER[0],
                controller_ip=_OWNER[1],
                lifecycle_epoch=_LIFECYCLE_EPOCH,
                resource_action_mode=mode,
                resource_action_mode_changed_at=changed_at))


def _admit(store, sample):
    return store.admit(sample, _OWNER, _LIFECYCLE_EPOCH)


def _complete_one(store,
                  sample,
                  invocation,
                  request_id: str,
                  *,
                  disposition: str = 'succeeded'):
    parent = _admit(store, sample)
    role = (actions.ShadowRequestRole.PRIMARY_LAUNCH
            if invocation.action_kind is kernel_actions.ActionKind.LAUNCH else
            actions.ShadowRequestRole.PRIMARY_DOWN)
    prepared = store.prepare_attempt(sample.action_id, parent.revision, 1, 1,
                                     role,
                                     actions.PlannedExecutionKind.API_REQUEST,
                                     invocation)
    store.bind_request(sample.action_id, 1, request_id)
    observation = None
    if disposition == 'succeeded':
        outcome = _outcome(invocation)
    else:
        observation = _observation(invocation, present=False)
        outcome = _outcome(invocation,
                           disposition=disposition,
                           observation=observation)
    store.complete_attempt(sample.action_id, 1, outcome, outcome, _retry())
    projection = _projection(invocation, disposition=disposition)
    return store.finalize_parent(sample.action_id, prepared.sample.revision,
                                 projection, projection,
                                 actions.ShadowParityClass.MATCH)


def _gate_evidence(
    *,
    authority_ready: bool = True,
    service_name: str = 'svc',
    service_hash: str = _SERVICE_UUID,
    lifecycle_epoch: int = _LIFECYCLE_EPOCH,
    candidate_since: datetime.datetime | None = None,
    verified_at: datetime.datetime | None = None,
    global_revision: str = '028',
) -> resource_action_state.ActivationGateEvidenceV1:
    return resource_action_state.ActivationGateEvidenceV1(
        version=1,
        service_name=service_name,
        service_hash=service_hash,
        lifecycle_epoch=lifecycle_epoch,
        candidate_since=candidate_since,
        old_controller_processes_drained=True,
        all_processes_on_approved_image=True,
        approved_image_digest='sha256:' + '1' * 64,
        api_schema_revision='005',
        serve_schema_revision='032',
        global_user_state_schema_revision=global_revision,
        handler_registered_everywhere=True,
        image_inventory_sha256='2' * 64,
        handler_inventory_sha256='3' * 64,
        provider_profiles_eligible=authority_ready,
        profile_inventory_sha256='4' * 64,
        shadow_coverage_complete=authority_ready,
        coverage_inventory_sha256='5' * 64,
        crash_injection_complete=authority_ready,
        verified_at=(datetime.datetime.now(_UTC) - datetime.timedelta(seconds=1)
                     if verified_at is None else verified_at))


def test_store_fails_closed_on_non_postgres() -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    with pytest.raises(RuntimeError, match='requires PostgreSQL'):
        resource_action_state.PostgresServeResourceActionStateStore(engine)


def test_provider_profile_cannot_be_made_eligible_by_the_caller(
        shadow_database) -> None:
    engine, store = shadow_database
    _add_service(engine)
    with pytest.raises(ValueError, match='has not cleared'):
        _sample(eligibility='ELIGIBLE')

    sample, _ = _sample()
    admitted = _admit(store, sample)
    assert admitted.profile_eligibility is actions.ProfileEligibility.UNSUPPORTED


def test_admit_exact_adoption_typed_spec_and_borrowed_transaction(
        shadow_database) -> None:
    engine, store = shadow_database
    _add_service(engine)
    sample, _ = _sample()
    admitted = _admit(store, sample)
    assert admitted.action_id == sample.action_id
    assert admitted.immutable_spec == sample.immutable_spec
    assert _admit(store, sample) == admitted

    conflicting, _ = _sample(placement_sha256='9' * 64)
    assert conflicting.action_id == sample.action_id
    with pytest.raises(kernel_actions.ActionConflict,
                       match='different immutable bytes'):
        _admit(store, conflicting)

    with pytest.raises(ValueError, match='not byte-equal'):
        resource_action_state.NewShadowSample(
            service_name='svc',
            immutable_spec=sample.immutable_spec,
            provider_plan=conflicting.provider_plan,
            profile_eligibility=actions.ProfileEligibility.UNSUPPORTED)
    mutated_spec = sample.immutable_spec.canonical_value()
    mutated_spec['invocation']['launch']['admin_policy_input_sha256'] = '8' * 64
    with pytest.raises(ValueError, match='request payload hash'):
        actions.ServeReplicaActionSpecV1.from_value(mutated_spec)

    second, _ = _sample(generation=2)
    with pytest.raises(RuntimeError, match='rollback marker'):
        with sqlalchemy.orm.Session(engine) as session, session.begin():
            store.admit_in_session(session, second, _OWNER, _LIFECYCLE_EPOCH)
            raise RuntimeError('rollback marker')
    assert store.get_sample(second.action_id) is None


def test_admission_revalidates_service_owner_epoch_and_mode(
        shadow_database) -> None:
    engine, store = shadow_database
    sample, _ = _sample()
    with pytest.raises(kernel_actions.ClaimLost, match='no longer exists'):
        _admit(store, sample)
    _add_service(engine, 'legacy')
    with pytest.raises(kernel_actions.ClaimLost, match='fence'):
        _admit(store, sample)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='shadow',
                resource_action_mode_changed_at=sqlalchemy.func.clock_timestamp(
                )))
    with pytest.raises(kernel_actions.ClaimLost, match='fence'):
        store.admit(sample, (999, _OWNER[1]), _LIFECYCLE_EPOCH)
    with pytest.raises(kernel_actions.ClaimLost, match='fence'):
        store.admit(sample, _OWNER, _LIFECYCLE_EPOCH + 1)
    assert _admit(store, sample).action_id == sample.action_id


def test_preexisting_transaction_uses_post_lock_database_timestamp(
        shadow_database) -> None:
    engine, store = shadow_database
    _add_service(engine, 'legacy')
    sample, _ = _sample()
    with sqlalchemy.orm.Session(engine) as early_session, early_session.begin():
        early_session.execute(sqlalchemy.select(sqlalchemy.literal(1)))
        transition = store.transition_service_mode(
            'svc',
            _SERVICE_UUID,
            _OWNER,
            actions.ResourceActionMode.LEGACY,
            actions.ResourceActionMode.SHADOW,
            gate_evidence=_gate_evidence(authority_ready=False),
            expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
        assert transition.record.changed_at is not None
        admitted = store.admit_in_session(early_session, sample, _OWNER,
                                          _LIFECYCLE_EPOCH)
        assert admitted.created_at >= transition.record.changed_at


def test_prepare_is_contiguous_exact_and_request_binding_is_global(
        shadow_database) -> None:
    engine, store = shadow_database
    _add_service(engine)
    sample, invocation = _sample()
    parent = _admit(store, sample)
    with pytest.raises(kernel_actions.ActionConflict, match='contiguous'):
        store.prepare_attempt(sample.action_id, parent.revision, 2, 1,
                              actions.ShadowRequestRole.PRIMARY_LAUNCH,
                              actions.PlannedExecutionKind.API_REQUEST,
                              invocation)
    _, wrong_invocation = _sample(generation=2)
    with pytest.raises(ValueError, match='not byte-equal'):
        store.prepare_attempt(sample.action_id, parent.revision, 1, 1,
                              actions.ShadowRequestRole.PRIMARY_LAUNCH,
                              actions.PlannedExecutionKind.API_REQUEST,
                              wrong_invocation)
    prepared = store.prepare_attempt(sample.action_id, parent.revision, 1, 1,
                                     actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                     actions.PlannedExecutionKind.API_REQUEST,
                                     invocation)
    replay = store.prepare_attempt(sample.action_id, parent.revision, 1, 1,
                                   actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                   actions.PlannedExecutionKind.API_REQUEST,
                                   invocation)
    assert replay.adopted and replay.attempt == prepared.attempt
    bound = store.bind_request(sample.action_id, 1, 'real-request')
    assert store.bind_request(sample.action_id, 1, 'real-request') == bound

    other, other_invocation = _sample(generation=2)
    other_parent = _admit(store, other)
    store.prepare_attempt(other.action_id, other_parent.revision, 1, 1,
                          actions.ShadowRequestRole.PRIMARY_LAUNCH,
                          actions.PlannedExecutionKind.API_REQUEST,
                          other_invocation)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='another shadow attempt'):
        store.bind_request(other.action_id, 1, 'real-request')


def test_unknown_abandon_and_provider_id_are_explicit(shadow_database,
                                                      eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    _add_service(engine)
    unknown_sample, invocation = _sample(eligibility='ELIGIBLE')
    parent = _admit(store, unknown_sample)
    store.prepare_attempt(unknown_sample.action_id, parent.revision, 1, 1,
                          actions.ShadowRequestRole.PRIMARY_LAUNCH,
                          actions.PlannedExecutionKind.API_REQUEST, invocation)
    unknown = store.mark_request_association_unknown(unknown_sample.action_id,
                                                     1)
    operation = store.record_provider_operation_id(unknown_sample.action_id, 1,
                                                   'provider-op')
    assert operation.provider_operation_id == 'provider-op'
    with pytest.raises(kernel_actions.ActionConflict,
                       match='different provider'):
        store.record_provider_operation_id(unknown_sample.action_id, 1,
                                           'other-op')
    final = store.finalize_parent(unknown_sample.action_id, 2, None, None,
                                  actions.ShadowParityClass.AMBIGUOUS)
    assert unknown.phase is (
        actions.ShadowAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN)
    assert final.phase is actions.ShadowParentPhase.AMBIGUOUS

    abandoned_sample, abandoned_invocation = _sample(generation=2,
                                                     eligibility='ELIGIBLE')
    parent = _admit(store, abandoned_sample)
    store.prepare_attempt(abandoned_sample.action_id, parent.revision, 1, 1,
                          actions.ShadowRequestRole.PRIMARY_LAUNCH,
                          actions.PlannedExecutionKind.API_REQUEST,
                          abandoned_invocation)
    with pytest.raises(ValueError, match='requires proof'):
        store.abandon_pre_submit(abandoned_sample.action_id,
                                 1,
                                 mutation_function_was_never_entered=False)
    abandoned = store.abandon_pre_submit(
        abandoned_sample.action_id, 1, mutation_function_was_never_entered=True)
    final = store.finalize_parent(abandoned_sample.action_id, 2, None, None,
                                  actions.ShadowParityClass.ABANDONED)
    assert abandoned.phase is actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT
    assert final.phase is actions.ShadowParentPhase.ABANDONED_PRE_SUBMIT


def test_completion_injects_operation_id_and_rejects_false_match(
        shadow_database, eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    _add_service(engine)
    sample, invocation = _sample(eligibility='ELIGIBLE')
    parent = _admit(store, sample)
    prepared = store.prepare_attempt(sample.action_id, parent.revision, 1, 1,
                                     actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                     actions.PlannedExecutionKind.API_REQUEST,
                                     invocation)
    store.bind_request(sample.action_id, 1, 'proof-request')
    fake_success = _outcome(invocation, observation=None)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='requires an observation'):
        store.complete_attempt(sample.action_id, 1, fake_success, fake_success,
                               _retry())
    store.record_provider_operation_id(sample.action_id, 1, 'provider-op')
    outcome = _outcome(invocation)
    completed = store.complete_attempt(sample.action_id, 1, outcome, outcome,
                                       _retry())
    assert completed.actual_outcome is not None
    assert completed.proposed_outcome is not None
    assert completed.actual_outcome.provider_operation_id == 'provider-op'
    assert completed.proposed_outcome.provider_operation_id == 'provider-op'
    incomplete_projection = _projection(invocation,
                                        include_resolved_target=False)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='exact resolved target'):
        store.finalize_parent(sample.action_id, prepared.sample.revision,
                              incomplete_projection, incomplete_projection,
                              actions.ShadowParityClass.MATCH)
    projection = _projection(invocation)
    final = store.finalize_parent(sample.action_id, prepared.sample.revision,
                                  projection, projection,
                                  actions.ShadowParityClass.MATCH)
    assert final.phase is actions.ShadowParentPhase.COMPLETE

    mismatch_sample, mismatch_invocation = _sample(generation=2,
                                                   eligibility='ELIGIBLE')
    mismatch_parent = _admit(store, mismatch_sample)
    store.prepare_attempt(mismatch_sample.action_id, mismatch_parent.revision,
                          1, 1, actions.ShadowRequestRole.PRIMARY_LAUNCH,
                          actions.PlannedExecutionKind.API_REQUEST,
                          mismatch_invocation)
    store.bind_request(mismatch_sample.action_id, 1, 'mismatch-request')
    actual = _outcome(mismatch_invocation)
    proposed = _outcome(mismatch_invocation, disposition='terminal_error')
    with pytest.raises(kernel_actions.ActionConflict,
                       match='must be byte-equal'):
        store.complete_attempt(mismatch_sample.action_id, 1, actual, proposed,
                               _retry())


def test_parent_cannot_finalize_with_an_unconsumed_retry_or_removed_success(
        shadow_database, eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    _add_service(engine)
    retry_sample, retry_invocation = _sample(eligibility='ELIGIBLE')
    parent = _admit(store, retry_sample)
    prepared = store.prepare_attempt(retry_sample.action_id, parent.revision, 1,
                                     1,
                                     actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                     actions.PlannedExecutionKind.API_REQUEST,
                                     retry_invocation)
    store.bind_request(retry_sample.action_id, 1, 'unconsumed-retry')
    succeeded = _outcome(retry_invocation)
    store.complete_attempt(retry_sample.action_id, 1, succeeded, succeeded,
                           _retry(decision='retry_same_plan'))
    projection = _projection(retry_invocation)
    with pytest.raises(kernel_actions.InvariantViolation,
                       match='missing_primary_retry_successor'):
        store.finalize_parent(retry_sample.action_id, prepared.sample.revision,
                              projection, projection,
                              actions.ShadowParityClass.MATCH)

    removed_sample, removed_invocation = _sample(generation=2,
                                                 eligibility='ELIGIBLE')
    parent = _admit(store, removed_sample)
    prepared = store.prepare_attempt(removed_sample.action_id, parent.revision,
                                     1, 1,
                                     actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                     actions.PlannedExecutionKind.API_REQUEST,
                                     removed_invocation)
    store.bind_request(removed_sample.action_id, 1, 'removed-success')
    succeeded = _outcome(removed_invocation)
    store.complete_attempt(removed_sample.action_id, 1, succeeded, succeeded,
                           _retry())
    removed_value = _projection(removed_invocation).canonical_value()
    removed_value['row_disposition'] = 'removed'
    removed_value['replica_status'] = None
    removed_projection = actions.ServeShadowProjectionV1.from_value(
        removed_value)
    with pytest.raises(kernel_actions.ActionConflict,
                       match='must retain a ready replica'):
        store.finalize_parent(removed_sample.action_id,
                              prepared.sample.revision, removed_projection,
                              removed_projection,
                              actions.ShadowParityClass.MATCH)


def test_progression_rejects_preterminal_unknown_and_abandoned_children(
        shadow_database) -> None:
    engine, store = shadow_database
    _add_service(engine)
    for generation, close in ((1, 'open'), (2, 'unknown'), (3, 'abandoned')):
        sample, invocation = _sample(generation=generation)
        parent = _admit(store, sample)
        prepared = store.prepare_attempt(
            sample.action_id, parent.revision, 1, 1,
            actions.ShadowRequestRole.PRIMARY_LAUNCH,
            actions.PlannedExecutionKind.API_REQUEST, invocation)
        if close == 'unknown':
            store.mark_request_association_unknown(sample.action_id, 1)
        elif close == 'abandoned':
            store.abandon_pre_submit(sample.action_id,
                                     1,
                                     mutation_function_was_never_entered=True)
        with pytest.raises(kernel_actions.ActionConflict,
                           match='every earlier child'):
            store.prepare_attempt(sample.action_id, prepared.sample.revision, 2,
                                  2, actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                  actions.PlannedExecutionKind.API_REQUEST,
                                  invocation)


@pytest.mark.parametrize('decision',
                         ['terminal', 'block', 'replan_new_generation'])
def test_progression_rejects_non_retry_primary_decisions(
        shadow_database, decision: str) -> None:
    engine, store = shadow_database
    _add_service(engine)
    sample, invocation = _sample()
    parent = _admit(store, sample)
    prepared = store.prepare_attempt(sample.action_id, parent.revision, 1, 1,
                                     actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                     actions.PlannedExecutionKind.API_REQUEST,
                                     invocation)
    store.bind_request(sample.action_id, 1, f'{decision}-request')
    failed = _outcome(invocation,
                      disposition='retryable',
                      observation=_observation(invocation, present=False))
    store.complete_attempt(sample.action_id, 1, failed, failed,
                           _retry(decision=decision))
    with pytest.raises(kernel_actions.ActionConflict, match='retry_same_plan'):
        store.prepare_attempt(sample.action_id, prepared.sample.revision, 2, 2,
                              actions.ShadowRequestRole.PRIMARY_LAUNCH,
                              actions.PlannedExecutionKind.API_REQUEST,
                              invocation)


def test_cleanup_chain_must_retry_then_terminalize_before_next_primary(
        shadow_database) -> None:
    engine, store = shadow_database
    _add_service(engine)
    sample, invocation = _sample()
    parent = _admit(store, sample)
    primary = store.prepare_attempt(sample.action_id, parent.revision, 1, 1,
                                    actions.ShadowRequestRole.PRIMARY_LAUNCH,
                                    actions.PlannedExecutionKind.API_REQUEST,
                                    invocation)
    store.bind_request(sample.action_id, 1, 'primary-failed')
    failed = _outcome(invocation,
                      disposition='retryable',
                      observation=_observation(invocation, present=False))
    store.complete_attempt(sample.action_id, 1, failed, failed,
                           _retry(decision='retry_same_plan'))
    cleanup_invocation = sample.immutable_spec.launch_cleanup_down_invocation()
    cleanup = store.prepare_attempt(
        sample.action_id, primary.sample.revision, 2, 1,
        actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN,
        actions.PlannedExecutionKind.API_REQUEST, cleanup_invocation)
    store.bind_request(sample.action_id, 2, 'cleanup-retry')
    cleanup_failed = _outcome(cleanup_invocation,
                              disposition='retryable',
                              observation=_observation(cleanup_invocation,
                                                       present=False))
    store.complete_attempt(sample.action_id, 2, cleanup_failed, cleanup_failed,
                           _retry(decision='retry_same_plan'))
    with pytest.raises(kernel_actions.ActionConflict, match='cleanup chain'):
        store.prepare_attempt(sample.action_id, cleanup.sample.revision, 3, 2,
                              actions.ShadowRequestRole.PRIMARY_LAUNCH,
                              actions.PlannedExecutionKind.API_REQUEST,
                              invocation)
    second_cleanup = store.prepare_attempt(
        sample.action_id, cleanup.sample.revision, 3, 1,
        actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN,
        actions.PlannedExecutionKind.API_REQUEST, cleanup_invocation)
    store.bind_request(sample.action_id, 3, 'cleanup-success')
    cleanup_succeeded = _outcome(cleanup_invocation)
    store.complete_attempt(sample.action_id, 3, cleanup_succeeded,
                           cleanup_succeeded, _retry())
    next_primary = store.prepare_attempt(
        sample.action_id, second_cleanup.sample.revision, 4, 2,
        actions.ShadowRequestRole.PRIMARY_LAUNCH,
        actions.PlannedExecutionKind.API_REQUEST, invocation)
    assert next_primary.attempt.logical_attempt == 2

    succeeded_sample, succeeded_invocation = _sample(generation=2)
    succeeded_parent = _admit(store, succeeded_sample)
    succeeded_primary = store.prepare_attempt(
        succeeded_sample.action_id, succeeded_parent.revision, 1, 1,
        actions.ShadowRequestRole.PRIMARY_LAUNCH,
        actions.PlannedExecutionKind.API_REQUEST, succeeded_invocation)
    store.bind_request(succeeded_sample.action_id, 1, 'primary-success')
    succeeded = _outcome(succeeded_invocation)
    store.complete_attempt(succeeded_sample.action_id, 1, succeeded, succeeded,
                           _retry())
    with pytest.raises(kernel_actions.ActionConflict,
                       match='failed launch primary'):
        store.prepare_attempt(
            succeeded_sample.action_id, succeeded_primary.sample.revision, 2, 1,
            actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN,
            actions.PlannedExecutionKind.API_REQUEST,
            succeeded_sample.immutable_spec.launch_cleanup_down_invocation())


def test_activation_is_exact_fresh_and_has_a_hard_24_hour_window(
        shadow_database) -> None:
    engine, store = shadow_database
    _add_service(engine, 'legacy')
    with pytest.raises(ValueError, match='global-user-state'):
        _gate_evidence(authority_ready=False, global_revision='027')
    with pytest.raises(kernel_actions.ActionConflict, match='must not bind'):
        store.transition_service_mode(
            'svc',
            _SERVICE_UUID,
            _OWNER,
            actions.ResourceActionMode.LEGACY,
            actions.ResourceActionMode.SHADOW,
            gate_evidence=_gate_evidence(
                authority_ready=False,
                candidate_since=datetime.datetime.now(_UTC)),
            expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    transition = store.transition_service_mode(
        'svc',
        _SERVICE_UUID,
        _OWNER,
        actions.ResourceActionMode.LEGACY,
        actions.ResourceActionMode.SHADOW,
        gate_evidence=_gate_evidence(authority_ready=False),
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    assert transition.record.changed_at is not None
    candidate_since = transition.record.changed_at

    for evidence, message in (
        (_gate_evidence(service_hash=_OTHER_SERVICE_UUID,
                        candidate_since=candidate_since), 'another service'),
        (_gate_evidence(lifecycle_epoch=_LIFECYCLE_EPOCH + 1,
                        candidate_since=candidate_since), 'another service'),
        (_gate_evidence(candidate_since=candidate_since -
                        datetime.timedelta(seconds=1)), 'current shadow'),
        (_gate_evidence(candidate_since=candidate_since,
                        verified_at=datetime.datetime.now(_UTC) -
                        datetime.timedelta(minutes=6)), 'older than'),
        (_gate_evidence(candidate_since=candidate_since,
                        verified_at=datetime.datetime.now(_UTC) +
                        datetime.timedelta(minutes=1)), 'database future'),
    ):
        with pytest.raises(kernel_actions.ActionConflict, match=message):
            store.transition_service_mode(
                'svc',
                _SERVICE_UUID,
                _OWNER,
                actions.ResourceActionMode.SHADOW,
                actions.ResourceActionMode.AUTHORITATIVE,
                gate_evidence=evidence,
                expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    with pytest.raises(ValueError, match='24 hours'):
        store.transition_service_mode(
            'svc',
            _SERVICE_UUID,
            _OWNER,
            actions.ResourceActionMode.SHADOW,
            actions.ResourceActionMode.AUTHORITATIVE,
            gate_evidence=_gate_evidence(candidate_since=candidate_since),
            expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
            minimum_window=datetime.timedelta(0))


def test_promotion_recomputes_match_and_counts_only_success(
        shadow_database, eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    old_window = datetime.datetime.now(_UTC) - datetime.timedelta(hours=25)
    _add_service(engine, changed_at=old_window)
    launch, launch_invocation = _sample(eligibility='ELIGIBLE')
    down, down_invocation = _sample('down', 2, 'ELIGIBLE')
    _complete_one(store, launch, launch_invocation, 'promotion-launch')
    _complete_one(store, down, down_invocation, 'promotion-down')
    failed, failed_invocation = _sample('launch', 3, 'ELIGIBLE')
    _complete_one(store,
                  failed,
                  failed_invocation,
                  'promotion-failed',
                  disposition='terminal_error')

    report = store.promotion_blocker_report('svc', _SERVICE_UUID)
    assert report.clean
    assert report.candidate_sample_count == 3
    assert report.clean_launch_samples == 1
    assert report.clean_down_samples == 1
    promoted = store.transition_service_mode(
        'svc',
        _SERVICE_UUID,
        _OWNER,
        actions.ResourceActionMode.SHADOW,
        actions.ResourceActionMode.AUTHORITATIVE,
        gate_evidence=_gate_evidence(candidate_since=old_window),
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    assert promoted.record.mode is actions.ResourceActionMode.AUTHORITATIVE


def test_activation_freshness_is_rechecked_after_the_blocking_audit(
        shadow_database, eligible_profile, monkeypatch) -> None:
    del eligible_profile
    engine, store = shadow_database
    old_window = datetime.datetime.now(_UTC) - datetime.timedelta(hours=25)
    _add_service(engine, changed_at=old_window)
    launch, launch_invocation = _sample(eligibility='ELIGIBLE')
    down, down_invocation = _sample('down', 2, 'ELIGIBLE')
    _complete_one(store, launch, launch_invocation, 'freshness-launch')
    _complete_one(store, down, down_invocation, 'freshness-down')
    audit_reached = False
    original_report = store._promotion_report_in_session

    def delayed_report(*args, **kwargs):
        nonlocal audit_reached
        audit_reached = True
        report = original_report(*args, **kwargs)
        time.sleep(1.2)
        return report

    monkeypatch.setattr(resource_action_state, '_MAX_ACTIVATION_EVIDENCE_AGE',
                        datetime.timedelta(seconds=1))
    monkeypatch.setattr(store, '_promotion_report_in_session', delayed_report)
    evidence = _gate_evidence(candidate_since=old_window,
                              verified_at=datetime.datetime.now(_UTC))
    with pytest.raises(kernel_actions.ActionConflict, match='older than'):
        store.transition_service_mode('svc',
                                      _SERVICE_UUID,
                                      _OWNER,
                                      actions.ResourceActionMode.SHADOW,
                                      actions.ResourceActionMode.AUTHORITATIVE,
                                      gate_evidence=evidence,
                                      expected_lifecycle_epoch=_LIFECYCLE_EPOCH)
    assert audit_reached


def test_promotion_does_not_trust_a_persisted_match_label(
        shadow_database, eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    old_window = datetime.datetime.now(_UTC) - datetime.timedelta(hours=25)
    _add_service(engine, changed_at=old_window)
    sample, invocation = _sample(eligibility='ELIGIBLE')
    final = _complete_one(store, sample, invocation, 'persisted-match')
    altered = actions.ServeShadowProjectionV1.from_value({
        **_projection(invocation).canonical_value(),
        'replica_status': 'NOT_READY',
    })
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                resource_action_state_schema.SHADOW_SAMPLES).where(
                    resource_action_state_schema.SHADOW_SAMPLES.c.
                    would_be_action_id == final.action_id).values(
                        proposed_projection=altered.canonical_value(),
                        proposed_projection_sha256=altered.sha256))
    report = store.promotion_blocker_report('svc', _SERVICE_UUID)
    assert final.action_id in report.blocking_sample_ids
    assert any('match_evidence:final projections are not byte-equal' in reason
               for reason in report.reasons)


def test_retention_protects_links_candidate_windows_and_cleanup_intents(
        shadow_database, eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    _add_service(engine)
    launch, launch_invocation = _sample(eligibility='ELIGIBLE')
    down, down_invocation = _sample('down', 2, 'ELIGIBLE')
    launch_final = _complete_one(store, launch, launch_invocation,
                                 'retention-launch')
    down_final = _complete_one(store, down, down_invocation, 'retention-down')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name='svc',
                replica_id=7,
                launch_shadow_sample_id=launch_final.action_id))
    cutoff = datetime.datetime.now(_UTC) + datetime.timedelta(days=1)
    retained = store.purge_completed_before(cutoff)
    assert launch_final.action_id in retained.protected_action_ids
    assert down_final.action_id in retained.removed_action_ids

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='shadow',
                resource_action_mode_changed_at=sqlalchemy.func.clock_timestamp(
                )))
    candidate, candidate_invocation = _sample('down', 3, 'ELIGIBLE')
    candidate_final = _complete_one(store, candidate, candidate_invocation,
                                    'candidate-down')
    retained = store.purge_completed_before(cutoff)
    assert candidate_final.action_id in retained.protected_action_ids

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
    cleanup, cleanup_invocation = _sample('launch', 4, 'ELIGIBLE')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='shadow',
                resource_action_mode_changed_at=sqlalchemy.func.clock_timestamp(
                )))
    cleanup_final = _complete_one(store, cleanup, cleanup_invocation,
                                  'cleanup-protected')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.
                              ephemeral_storage_cleanup_intents_table).values(
                                  service_name='svc',
                                  resource_scope='scope-1',
                                  storage_generation='storage-1',
                                  yaml_content='service: test',
                                  pool=0,
                                  lifecycle_epoch=_LIFECYCLE_EPOCH,
                                  provisional=0,
                                  created_at=1.0))
    retained = store.purge_completed_before(cutoff)
    assert cleanup_final.action_id in retained.protected_action_ids


def test_retention_typed_reads_children_and_fails_closed(
        shadow_database, eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    _add_service(engine)
    sample, invocation = _sample(eligibility='ELIGIBLE')
    final = _complete_one(store, sample, invocation, 'missing-child')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
        connection.execute(
            sqlalchemy.delete(
                resource_action_state_schema.SHADOW_ATTEMPTS).where(
                    resource_action_state_schema.SHADOW_ATTEMPTS.c.
                    would_be_action_id == final.action_id))
    cutoff = datetime.datetime.now(_UTC) + datetime.timedelta(days=1)
    with pytest.raises(kernel_actions.InvariantViolation,
                       match='missing_attempt'):
        store.purge_completed_before(cutoff)
    assert store.get_sample(final.action_id) is not None


def test_retention_reports_skip_locked_parents_as_deferred(
        shadow_database, eligible_profile) -> None:
    del eligible_profile
    engine, store = shadow_database
    _add_service(engine)
    sample, invocation = _sample('down', 2, 'ELIGIBLE')
    final = _complete_one(store, sample, invocation, 'deferred-parent')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).values(
                resource_action_mode='authoritative'))
    cutoff = datetime.datetime.now(_UTC) + datetime.timedelta(days=1)
    with engine.connect() as locking_connection:
        transaction = locking_connection.begin()
        locking_connection.execute(
            sqlalchemy.select(
                resource_action_state_schema.SHADOW_SAMPLES).where(
                    resource_action_state_schema.SHADOW_SAMPLES.c.
                    would_be_action_id == final.action_id).with_for_update())
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(store.purge_completed_before,
                                     cutoff).result(timeout=10)
        assert result.removed_action_ids == ()
        assert result.protected_action_ids == ()
        assert result.deferred_action_ids == (final.action_id,)
        transaction.rollback()
    assert store.get_sample(final.action_id) is not None
