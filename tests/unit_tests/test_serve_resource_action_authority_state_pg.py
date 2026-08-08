"""Strict values and real-PostgreSQL tests for Serve038 authority state."""
# pylint: disable=protected-access,redefined-outer-name,too-many-locals,unused-import

import concurrent.futures
import copy
import dataclasses
import datetime
import uuid

import pytest
import serve_resource_action_test_fixtures as fixtures
import sqlalchemy
from test_serve_resource_action_schema_038_pg import postgres_engine

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_authority_state as authority_state
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_action_state_schema as state_schema
from sky.serve import resource_actions
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

_UTC = datetime.timezone.utc
_SHA_A = 'a' * 64
_SHA_B = 'b' * 64
_SHA_C = 'c' * 64
_SHA_D = 'd' * 64
_SHA_E = 'e' * 64


def _timestamp(value: datetime.datetime) -> str:
    return value.astimezone(_UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


@pytest.fixture(scope='module')
def authority_database(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.SERVE_DB_NAME, '038')
    return postgres_engine


def _database_now(engine: sqlalchemy.engine.Engine) -> datetime.datetime:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()


@pytest.mark.parametrize('inventory', ('live', 'tombstone'))
def test_release_decoder_rejects_oversized_inventory_before_hash_or_decode(
        inventory: str) -> None:
    """A corrupt retained row cannot spend work decoding >256 entries."""
    now = datetime.datetime(2026, 8, 3, 12, 0, tzinfo=_UTC)
    row = {
        'namespace': fixtures.NAMESPACE,
        'helm_release_name': 'stable-release',
        'installation_id': uuid.UUID(fixtures.INSTALLATION_ID),
        'helm_full_name': fixtures.HELM_FULL_NAME,
        'enabled': True,
        'live_manifests': ([{}] * 257 if inventory == 'live' else []),
        'live_inventory_sha256': 'not-a-hash',
        'tombstone_suffixes':
            ([object()] * 257 if inventory == 'tombstone' else []),
        'tombstone_inventory_sha256': 'not-a-hash',
        'revision': 1,
        'created_at': now,
        'updated_at': now,
    }

    with pytest.raises(authority_state.AuthorityStateCorruption) as error:
        authority_state.decode_authority_release_ledger_row(row)

    assert error.value.__cause__ is not None
    assert 'exceeds 256 entries' in str(error.value.__cause__)


def _manifest() -> authority.ProviderAuthorityWorkerCohortManifestV2:
    value = copy.deepcopy(fixtures.authority_manifest_value())
    value['version'] = 2
    value['claim_contract'] = 'frozen_action_cohort_join_v2'
    return authority.ProviderAuthorityWorkerCohortManifestV2.from_value(value)


def _cohort() -> authority.ProviderAuthorityWorkerCohortV2:
    manifest = _manifest()
    return authority.ProviderAuthorityWorkerCohortV2(
        version=2,
        manifest=manifest,
        manifest_sha256=manifest.sha256,
        deployment_uid='deployment-uid-v2',
        service_account_uid='service-account-uid-v2')


def _worker(
    pod_uid: uuid.UUID,
    observed_at: datetime.datetime,
    *,
    pod_resource_version: str = '101',
    replica_set_resource_version: str = '102'
) -> (authority.ProviderAuthorityWorkerIdentityV2):
    cohort = _cohort()
    manifest = cohort.manifest
    replica_set_name = f'{manifest.deployment_name}-{str(pod_uid)[:8]}'
    replica_set_uid = f'replicaset-{pod_uid}'
    return authority.ProviderAuthorityWorkerIdentityV2(
        version=2,
        namespace=manifest.namespace,
        pod_name=f'worker-{pod_uid}',
        pod_uid=pod_uid,
        pod_resource_version=pod_resource_version,
        pod_service_account_name=manifest.service_account_name,
        pod_controller_owner=resource_actions.
        ProviderKubernetesControllerOwnerV1(api_version='apps/v1',
                                            kind='ReplicaSet',
                                            name=replica_set_name,
                                            uid=replica_set_uid),
        replica_set_name=replica_set_name,
        replica_set_uid=replica_set_uid,
        replica_set_resource_version=replica_set_resource_version,
        replica_set_controller_owner=resource_actions.
        ProviderKubernetesControllerOwnerV1(api_version='apps/v1',
                                            kind='Deployment',
                                            name=manifest.deployment_name,
                                            uid=cohort.deployment_uid),
        deployment_name=manifest.deployment_name,
        deployment_uid=cohort.deployment_uid,
        deployment_generation=5,
        deployment_observed_generation=5,
        pod_template_contract_sha256=manifest.pod_template_contract.sha256,
        image=resource_actions.ProviderAuthorityWorkerImageV1.from_value({
            'qualification': manifest.image.canonical_value(),
            'runtime': {
                'raw_image_id': 'containerd://sha256:' + '2' * 64,
                'runtime_image_id_scheme': 'containerd',
                'runtime_image_id_digest': 'sha256:' + '2' * 64,
                'qualified_oci_manifest_digest': 'sha256:' + '1' * 64,
                'qualified_oci_config_digest': 'sha256:' + '2' * 64,
                'qualification_artifact_sha256':
                    manifest.image.qualification_artifact.sha256,
                'runtime_id_contract': 'qualified_oci_config_digest_v1',
            },
        }),
        service_account_uid=cohort.service_account_uid,
        artifact_inventory_sha256=manifest.artifact_inventory.sha256,
        callable_inventory_sha256=manifest.callable_inventory.sha256,
        handler_allowlist_sha256=resource_actions.canonical_sha256(
            list(manifest.handler_allowlist)),
        observed_at=_timestamp(observed_at))


def _install_release(engine: sqlalchemy.engine.Engine) -> None:
    manifest = _manifest()
    record = authority_state.ServeResourceActionAuthorityStore(
        engine).preflight_authority_release_v2(fixtures.NAMESPACE,
                                               'stable-release',
                                               fixtures.HELM_FULL_NAME,
                                               fixtures.INSTALLATION_ID, True,
                                               (manifest,), ())
    assert record.live_manifests == (manifest,)
    assert record.revision == 1


def _approved_cohort() -> authority.ApprovedAuthorityCohortArtifactV1:
    manifest = _manifest()
    return authority.ApprovedAuthorityCohortArtifactV1(
        cohort_id=manifest.cohort_id,
        oci_manifest_digest=manifest.image.oci_manifest_digest,
        oci_config_digest=manifest.image.oci_config_digest,
        manifest_sha256=manifest.sha256,
        qualification_artifact_sha256=manifest.image.qualification_artifact.
        sha256,
        pod_template_contract_sha256=manifest.pod_template_contract.sha256,
        pod_template_binding_sha256=manifest.pod_template_binding.sha256,
        artifact_inventory_sha256=manifest.artifact_inventory.sha256,
        callable_inventory_sha256=manifest.callable_inventory.sha256,
        handler_allowlist_sha256=resource_actions.canonical_sha256(
            list(manifest.handler_allowlist)),
        claim_contract='frozen_action_cohort_join_v2')


def _policy(character: str) -> authority.ResourceActionQualificationPolicyV1:
    images = tuple(
        authority.ApprovedRoleImageV1(role=role,
                                      oci_manifest_digest='sha256:' +
                                      character * 64,
                                      source_commit=character * 40,
                                      artifact_inventory_sha256=character * 64)
        for role in (authority.ApprovedRole.API,
                     authority.ApprovedRole.ORDINARY_EXECUTOR,
                     authority.ApprovedRole.CONTROLLER))
    return authority.ResourceActionQualificationPolicyV1(
        version=1,
        api_requests_head='007',
        serve_head='035',
        global_user_state_head='028',
        candidate_minimum_seconds=86_400,
        minimum_clean_launches=100,
        minimum_clean_downs=100,
        approved_role_images=images,
        approved_cohorts=(_approved_cohort(),),
        crash_canary_inventory_contract=
        'resource_action_crash_canary_inventory_v1',
        required_crash_canary_inventory_sha256=character * 64)


def _empty_inventory() -> authority.AuthorityNonterminalInventoryV1:
    return authority.AuthorityNonterminalInventoryV1(
        version=1,
        leased_private_request_ids=(),
        nonterminal_action_ids=(),
        active_reference_ids=())


def _hashed(label: str) -> authority.HashedCanonicalObjectV1:
    return authority.HashedCanonicalObjectV1.from_object({
        'version': 1,
        'label': label,
    })


def test_v2_values_are_closed_and_v1_remains_disjoint() -> None:
    manifest = _manifest()
    with pytest.raises(ValueError, match='version'):
        resource_actions.ProviderAuthorityWorkerCohortManifestV1.from_value(
            manifest.canonical_value())
    v1 = fixtures.authority_manifest_value()
    with pytest.raises(ValueError, match='version'):
        authority.ProviderAuthorityWorkerCohortManifestV2.from_value(v1)

    now = datetime.datetime(2026, 8, 3, 12, 0, tzinfo=_UTC)
    registration = authority.ProviderAuthorityWorkerRegistrationV2(
        version=2,
        worker_instance_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        worker=_worker(uuid.UUID('11111111-1111-4111-8111-111111111111'), now),
        pod_ready=True,
        registered_at=_timestamp(now))
    registering = authority.ProviderAuthorityWorkerRegistrationSetV2(
        version=2,
        cohort_identity_sha256=_cohort().sha256,
        revision=1,
        deployment_snapshot=None,
        workers=(registration,))
    registering.validate_registering()
    with pytest.raises(ValueError, match='accepted membership'):
        registering.validate_accepted()
    value = registration.canonical_value()
    value['worker_instance_id'] = 7
    with pytest.raises(TypeError, match='UUID'):
        authority.ProviderAuthorityWorkerRegistrationV2.from_value(value)

    active_lease = authority.ProviderAuthorityWorkerLeaseV1(
        version=1,
        worker_instance_id=registration.worker_instance_id,
        generation=1,
        state=authority.WorkerRegistrationLeaseState.ACTIVE,
        renewal_registration=registration,
        renewal_registration_sha256=registration.sha256,
        renewed_at=_timestamp(now),
        expires_at=_timestamp(now + datetime.timedelta(seconds=60)),
        revoked_at=None,
        revocation_reason=None,
        revocation_owner_id=None,
        last_operation_id=uuid.UUID('77777777-7777-4777-8777-777777777771'),
        last_operation_kind=authority.WorkerRegistrationLeaseOperation.INSERT,
        revision=1)
    accepted = authority.ProviderAuthorityWorkerAcceptedMembershipV2(
        version=2,
        registration=registration,
        registration_set_revision=registering.revision,
        registration_set_sha256=registering.sha256,
        lease=active_lease)
    assert accepted.lease.state is authority.WorkerRegistrationLeaseState.ACTIVE
    revoked_lease = authority.ProviderAuthorityWorkerLeaseV1(
        version=1,
        worker_instance_id=registration.worker_instance_id,
        generation=1,
        state=authority.WorkerRegistrationLeaseState.REVOKED,
        renewal_registration=registration,
        renewal_registration_sha256=registration.sha256,
        renewed_at=_timestamp(now),
        expires_at=_timestamp(now + datetime.timedelta(seconds=60)),
        revoked_at=_timestamp(now + datetime.timedelta(seconds=1)),
        revocation_reason=(
            authority.WorkerRegistrationLeaseRevocationReason.STALE_HANDOFF),
        revocation_owner_id=uuid.UUID('77777777-7777-4777-8777-777777777772'),
        last_operation_id=uuid.UUID('77777777-7777-4777-8777-777777777773'),
        last_operation_kind=authority.WorkerRegistrationLeaseOperation.REVOKE,
        revision=2)
    with pytest.raises(ValueError, match='must be ACTIVE'):
        authority.ProviderAuthorityWorkerAcceptedMembershipV2(
            version=2,
            registration=registration,
            registration_set_revision=registering.revision,
            registration_set_sha256=registering.sha256,
            lease=revoked_lease)

    snapshot_value = {
        'version': 2,
        'deployment_name': manifest.deployment_name,
        'deployment_uid': _cohort().deployment_uid,
        'deployment_resource_version': '103',
        'deployment_generation': 5,
        'deployment_observed_generation': 5,
        'pod_template_contract_sha256': manifest.pod_template_contract.sha256,
        'deployment_strategy': 'RollingUpdate',
        'deployment_max_surge': 0,
        'deployment_max_unavailable': 1,
        'deployment_spec_replicas': 2,
        'deployment_status_replicas': 2,
        'deployment_updated_replicas': 2,
        'deployment_ready_replicas': 2,
        'deployment_available_replicas': 2,
        'deployment_unavailable_replicas': 0,
        'observed_at': _timestamp(now),
    }
    snapshot = authority.ProviderAuthorityWorkerDeploymentSnapshotV2.from_value(
        snapshot_value)
    assert snapshot.canonical_value() == snapshot_value
    invalid_strategies = (
        {
            'deployment_max_surge': 1,
            'deployment_max_unavailable': 0,
        },
        {
            'deployment_max_surge': '0%'
        },
        {
            'deployment_max_unavailable': '1%'
        },
        {
            'deployment_strategy': 'Recreate'
        },
    )
    for overrides in invalid_strategies:
        crossed = copy.deepcopy(snapshot_value)
        crossed.update(overrides)
        with pytest.raises(ValueError):
            authority.ProviderAuthorityWorkerDeploymentSnapshotV2.from_value(
                crossed)


def test_registration_append_renewal_cas_and_exact_adoption(
        authority_database) -> None:
    engine = authority_database
    _install_release(engine)
    store = authority_state.ServeResourceActionAuthorityStore(engine)
    cohort = _cohort()
    now = _database_now(engine)
    first_id = uuid.UUID('11111111-1111-4111-8111-111111111111')
    second_id = uuid.UUID('22222222-2222-4222-8222-222222222222')
    first = _worker(first_id, now)
    second = _worker(second_id, now)
    first_operation = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1')
    second_operation = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2')

    inserted = store.register_initial_member(
        helm_release_name=fixtures.HELM_FULL_NAME,
        cohort=cohort,
        worker=first,
        operation_id=first_operation)
    assert not inserted.adopted
    assert inserted.cohort.revision == 1
    assert inserted.lease.generation == inserted.lease.revision == 1
    bootstrap_state = store.read_worker_bootstrap_state(cohort.cohort_id,
                                                        first_id)
    assert bootstrap_state is not None
    assert bootstrap_state.cohort.cohort.canonical_bytes == cohort.canonical_bytes
    assert bootstrap_state.own_lease is not None
    assert bootstrap_state.own_lease.canonical_bytes == (
        inserted.lease.canonical_bytes)
    assert store.read_database_clock().tzinfo is not None
    assert authority.timestamp_to_datetime(
        inserted.lease.expires_at,
        name='expires_at') - authority.timestamp_to_datetime(
            inserted.lease.renewed_at, name='renewed_at') == datetime.timedelta(
                seconds=60)

    adopted = store.register_initial_member(
        helm_release_name=fixtures.HELM_FULL_NAME,
        cohort=cohort,
        worker=first,
        operation_id=first_operation)
    assert adopted.adopted
    assert adopted.lease.canonical_bytes == inserted.lease.canonical_bytes

    renewed_worker = _worker(first_id,
                             _database_now(engine),
                             pod_resource_version='201',
                             replica_set_resource_version='202')
    renewal_operation = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3')
    renewed = store.renew_own_lease(cohort_id=cohort.cohort_id,
                                    worker=renewed_worker,
                                    expected_generation=1,
                                    operation_id=renewal_operation)
    assert not renewed.adopted
    assert renewed.lease.generation == renewed.lease.revision == 2
    assert store.renew_own_lease(cohort_id=cohort.cohort_id,
                                 worker=renewed_worker,
                                 expected_generation=1,
                                 operation_id=renewal_operation).adopted
    initial_after_renewal = store.register_initial_member(
        helm_release_name=fixtures.HELM_FULL_NAME,
        cohort=cohort,
        worker=first,
        operation_id=first_operation)
    assert initial_after_renewal.adopted
    assert initial_after_renewal.lease.generation == 2

    drifted_worker = dataclasses.replace(renewed_worker,
                                         pod_name='stable-identity-drift')
    with pytest.raises(authority_state.AuthorityStateConflict,
                       match='stable identity drifted'):
        store.renew_own_lease(
            cohort_id=cohort.cohort_id,
            worker=drifted_worker,
            expected_generation=2,
            operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa6'))

    appended = store.append_registering_member(
        helm_release_name=fixtures.HELM_FULL_NAME,
        cohort=cohort,
        worker=second,
        expected_cohort_revision=1,
        operation_id=second_operation)
    assert not appended.adopted
    assert appended.cohort.revision == 2
    assert tuple(
        item.worker_instance_id
        for item in appended.cohort.registration_set.workers) == (first_id,
                                                                  second_id)
    appended_adopted = store.append_registering_member(
        helm_release_name=fixtures.HELM_FULL_NAME,
        cohort=cohort,
        worker=second,
        expected_cohort_revision=1,
        operation_id=second_operation)
    assert appended_adopted.adopted

    second_renewed_worker = _worker(second_id,
                                    _database_now(engine),
                                    pod_resource_version='211',
                                    replica_set_resource_version='212')
    second_renewed = store.renew_own_lease(
        cohort_id=cohort.cohort_id,
        worker=second_renewed_worker,
        expected_generation=1,
        operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa7'))
    assert second_renewed.lease.generation == 2
    append_after_renewal = store.append_registering_member(
        helm_release_name=fixtures.HELM_FULL_NAME,
        cohort=cohort,
        worker=second,
        expected_cohort_revision=1,
        operation_id=second_operation)
    assert append_after_renewal.adopted
    assert append_after_renewal.lease.generation == 2

    race_worker = _worker(first_id,
                          _database_now(engine),
                          pod_resource_version='301',
                          replica_set_resource_version='302')
    operations = (
        uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4'),
        uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5'),
    )

    def renew(operation):
        try:
            return store.renew_own_lease(cohort_id=cohort.cohort_id,
                                         worker=race_worker,
                                         expected_generation=2,
                                         operation_id=operation)
        except authority_state.AuthorityStateSuperseded as error:
            return error

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(renew, operations))
    assert sum(
        isinstance(item, authority_state.WorkerRegistrationMutation)
        for item in outcomes) == 1
    assert sum(
        isinstance(item, authority_state.AuthorityStateSuperseded)
        for item in outcomes) == 1
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(m4_schema.WORKER_REGISTRATION_LEASES).where(
                m4_schema.WORKER_REGISTRATION_LEASES.c.cohort_id ==
                cohort.cohort_id,
                m4_schema.WORKER_REGISTRATION_LEASES.c.worker_instance_id ==
                first_id)).mappings().one()
    assert row['generation'] == row['revision'] == 3
    assert row['expires_at'] - row['renewed_at'] == datetime.timedelta(
        seconds=60)

    candidate_id = uuid.UUID('44444444-4444-4444-8444-444444444444')
    candidate_now = _database_now(engine)
    candidate_worker = _worker(candidate_id, candidate_now)
    candidate_registration = store._registration_at(candidate_worker,
                                                    candidate_now)
    candidate_operation = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa8')
    stale_fence = {'version': 1}
    absence_proof = {'version': 1}
    source_set = appended.cohort.registration_set
    with engine.begin() as connection:
        connection.execute(m4_schema.WORKER_REGISTRATION_LEASES.insert().values(
            **store._lease_values(cohort.cohort_id, candidate_registration,
                                  candidate_operation, candidate_now)))
        connection.execute(
            m4_schema.WORKER_REGISTRATION_HANDOFFS.insert().values(
                cohort_id=cohort.cohort_id,
                handoff_id=uuid.UUID('55555555-5555-4555-8555-555555555555'),
                predecessor_handoff_id=None,
                chain_sequence=1,
                stale_fence_disposition='NEWLY_REVOKED',
                source_cohort_revision=source_set.revision,
                source_cohort_state='ACCEPTING',
                source_registration_set_revision=source_set.revision,
                source_registration_set=source_set.canonical_value(),
                source_registration_set_sha256=source_set.sha256,
                stale_worker_instance_id=first_id,
                stale_pod_name=first.pod_name,
                stale_pod_uid=first_id,
                survivor_worker_instance_id=second_id,
                survivor_pod_uid=second_id,
                candidate_worker_instance_id=candidate_id,
                candidate_pod_name=candidate_worker.pod_name,
                candidate_pod_uid=candidate_id,
                stale_authority_fence=stale_fence,
                stale_authority_fence_sha256=authority.canonical_sha256(
                    stale_fence),
                stale_uid_absence_proof=absence_proof,
                stale_uid_absence_proof_sha256=authority.canonical_sha256(
                    absence_proof),
                candidate_registration=(
                    candidate_registration.canonical_value()),
                candidate_registration_sha256=_SHA_A,
                handoff_state='OPEN',
                revision=1,
                opened_at=candidate_now,
                fenced_at=candidate_now))
    with pytest.raises(authority_state.AuthorityStateCorruption,
                       match='candidate hash'):
        store.renew_own_lease(
            cohort_id=cohort.cohort_id,
            worker=candidate_worker,
            expected_generation=1,
            operation_id=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa9'))

    # Registration must decode the complete retained release row before the
    # existing cohort can be adopted.  These corruptions all satisfy the
    # physical PostgreSQL shape but fail typed/hash/canonical cross-validation.
    release_table = state_schema.AUTHORITY_RELEASES
    manifest_value = cohort.manifest.canonical_value()
    registration_operation = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa10')

    def require_release_corruption(**values) -> None:
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(release_table).where(
                    release_table.c.namespace == fixtures.NAMESPACE,
                    release_table.c.helm_release_name ==
                    'stable-release').values(**values))
        with pytest.raises(authority_state.AuthorityStateCorruption,
                           match='release row'):
            store.register_initial_member(
                helm_release_name=fixtures.HELM_FULL_NAME,
                cohort=cohort,
                worker=first,
                operation_id=registration_operation)

    require_release_corruption(live_inventory_sha256=_SHA_A)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(release_table).where(
                release_table.c.namespace == fixtures.NAMESPACE,
                release_table.c.helm_release_name == 'stable-release').values(
                    live_inventory_sha256=authority.canonical_sha256(
                        [manifest_value])))
    duplicated = [manifest_value, manifest_value]
    require_release_corruption(
        live_manifests=duplicated,
        live_inventory_sha256=authority.canonical_sha256(duplicated))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(release_table).where(
                release_table.c.namespace == fixtures.NAMESPACE,
                release_table.c.helm_release_name == 'stable-release').values(
                    live_manifests=[manifest_value],
                    live_inventory_sha256=authority.canonical_sha256(
                        [manifest_value])))
    overlap = [fixtures.COHORT_SUFFIX]
    require_release_corruption(
        tombstone_suffixes=overlap,
        tombstone_inventory_sha256=authority.canonical_sha256(overlap))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(release_table).where(
                release_table.c.namespace == fixtures.NAMESPACE,
                release_table.c.helm_release_name == 'stable-release').values(
                    tombstone_suffixes=[],
                    tombstone_inventory_sha256=authority.canonical_sha256([])))


def test_uuid_policy_values_are_closed_and_drain_is_one_way(
        authority_database) -> None:
    engine = authority_database
    store = authority_state.ServeResourceActionAuthorityStore(engine)
    service_uuid = uuid.UUID('33333333-3333-4333-8333-333333333333')
    # Policy epochs are opaque UUIDs rather than revision-like v1..v5 tokens.
    candidate_epoch = uuid.UUID('018f0f5e-7b8a-7abc-8def-0123456789ab')
    candidate_since = _database_now(engine) - datetime.timedelta(hours=25)
    policy = _policy('a')
    fence = authority.AuthorityServiceFenceV1(
        service_name='authority-policy-service',
        service_hash=str(service_uuid),
        controller_owner_fence='123:10.0.0.1',
        lifecycle_epoch=7)
    proof = authority.AuthoritativePromotionProofV1(
        version=1,
        service_fence=fence,
        candidate_epoch=candidate_epoch,
        candidate_since=_timestamp(candidate_since),
        verified_at=_timestamp(_database_now(engine)),
        candidate_duration_seconds=90_000,
        qualification_policy_sha256=policy.sha256,
        qualification_binding_sha256=_SHA_B,
        coverage_inventory_sha256=_SHA_C,
        clean_launches=100,
        clean_downs=100,
        blocker_count=0,
        crash_canary_inventory=_hashed('crash'),
        referenced_cohort_inventory=_hashed('cohort'),
        deployment_inventory=_hashed('deployment'),
        elected_version_identity=_hashed('version'),
        live_replica_identity_inventory=_hashed('replicas'),
        schema_heads=authority.AuthoritySchemaHeadsV1(
            api_requests_head='007',
            serve_head='035',
            global_user_state_head='028'))
    parsed_proof = authority.AuthoritativePromotionProofV1.from_value(
        proof.canonical_value())
    assert parsed_proof.canonical_bytes == proof.canonical_bytes
    assert parsed_proof.candidate_epoch == candidate_epoch

    inventory = _empty_inventory()
    successor_policy = _policy('d')
    rotation = authority.ServeAuthorityPolicyRotationProofV1(
        version=1,
        service_fence=fence,
        predecessor_policy_epoch=candidate_epoch,
        predecessor_policy_sha256=policy.sha256,
        schema_heads=authority.AuthoritySchemaHeadsV1(
            api_requests_head='007',
            serve_head='035',
            global_user_state_head='028'),
        successor_policy=successor_policy,
        successor_policy_sha256=successor_policy.sha256,
        successor_authority_binding_sha256=_SHA_E,
        staged_artifact_inventory=_hashed('staged'),
        rollback_artifact_inventory=_hashed('rollback'),
        service_version_inventory=_hashed('versions'),
        cohort_inventory=_hashed('cohorts'),
        nonterminal_inventory=inventory,
        started_at=_timestamp(_database_now(engine)),
        completed_at=_timestamp(_database_now(engine)),
        reason='COMPATIBLE_IMAGE_ROTATION')
    parsed_rotation = authority.ServeAuthorityPolicyRotationProofV1.from_value(
        rotation.canonical_value())
    assert parsed_rotation.canonical_bytes == rotation.canonical_bytes
    assert parsed_rotation.predecessor_policy_epoch == candidate_epoch
    open_rotation = rotation.canonical_value()
    open_rotation['unexpected'] = True
    with pytest.raises(ValueError, match='unknown or missing'):
        authority.ServeAuthorityPolicyRotationProofV1.from_value(open_rotation)

    activated_at = _database_now(engine)
    root_operation = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-bbbbbbbbbbb1')
    with engine.begin() as connection:
        connection.execute(serve_state_schema.services_table.insert().values(
            name=fence.service_name,
            hash=fence.service_hash,
            status='READY',
            controller_pid=123,
            controller_ip='10.0.0.1',
            lifecycle_epoch=7,
            resource_action_mode='authoritative',
            resource_action_mode_changed_at=activated_at,
            resource_action_candidate_epoch=candidate_epoch,
            resource_action_candidate_policy_sha256=policy.sha256,
            resource_action_candidate_binding_sha256=_SHA_B))
        connection.execute(m4_schema.AUTHORITY_POLICY_EPOCHS.insert().values(
            service_hash=fence.service_hash,
            policy_epoch=candidate_epoch,
            predecessor_policy_epoch=None,
            policy=policy.canonical_value(),
            policy_sha256=policy.sha256,
            authority_binding_sha256=_SHA_B,
            rotation_proof=proof.canonical_value(),
            rotation_proof_sha256=proof.sha256,
            nonterminal_inventory=inventory.canonical_value(),
            nonterminal_inventory_sha256=inventory.sha256,
            reason='INITIAL_PROMOTION',
            policy_state=authority_state.AuthorityPolicyState.ACTIVE.value,
            admission_state=(
                authority_state.AuthorityPolicyAdmissionState.OPEN.value),
            admission_revision=1,
            last_operation_id=root_operation,
            last_operation_kind=(
                authority_state.AuthorityPolicyOperation.ACTIVATE.value),
            created_at=activated_at,
            admission_changed_at=activated_at,
            activated_at=activated_at,
            superseded_at=None))

    drain_operation = uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-bbbbbbbbbbb2')
    drained = store.drain_policy(service_fence=fence,
                                 policy_epoch=candidate_epoch,
                                 expected_revision=1,
                                 operation_id=drain_operation)
    assert drained.record.admission_state is (
        authority_state.AuthorityPolicyAdmissionState.DRAINING)
    assert drained.record.admission_revision == 2
    assert store.drain_policy(service_fence=fence,
                              policy_epoch=candidate_epoch,
                              expected_revision=1,
                              operation_id=drain_operation).adopted
    for unsafe_method in ('activate_initial_policy', 'close_policy',
                          'reopen_policy', 'activate_successor_policy'):
        assert not hasattr(store, unsafe_method)
