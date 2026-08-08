"""Real-PostgreSQL tests for provisional Serve V2 class-2 identity state."""
# pylint: disable=protected-access,redefined-outer-name,too-many-locals,unused-import

import concurrent.futures
import dataclasses
import datetime
import hashlib
import threading
import uuid

import pytest
import sqlalchemy
import test_serve_resource_action_authority_state_pg as authority_fixtures
from test_serve_resource_action_schema_038_pg import postgres_engine
import test_serve_resource_action_v2_identity as v2_fixtures

from sky.serve import resource_action_authority as authority_contracts
from sky.serve import resource_action_authority_state as authority_state
from sky.serve import resource_action_identity as identity_projector
from sky.serve import resource_action_identity_state as identity_state
from sky.serve import resource_action_m4_state_schema as m4_schema
from sky.serve import resource_actions
from sky.serve import serve_state_schema
from sky.serve.serve_statuses import ServiceStatus
from sky.server.requests import postgres_schema as request_schema
from sky.server.requests import resource_actions as kernel_actions
from sky.utils.db import migration_utils

_UTC = datetime.timezone.utc
_SERVICE_INCARNATION = uuid.UUID('11111111-1111-4111-8111-111111111111')
_REPLICA_INCARNATION = uuid.UUID('22222222-2222-4222-8222-222222222222')
_CLUSTER_RECORD_UUID = uuid.UUID('33333333-3333-4333-8333-333333333333')
_POLICY_EPOCH = uuid.UUID('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb')
_YAML = '''\
service:
  readiness_probe: /health
  replicas: 1
resources:
  infra: kubernetes
run: python app.py
'''


@pytest.fixture(scope='module')
def identity_database(postgres_engine):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         migration_utils.API_REQUESTS_VERSION)
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.SERVE_DB_NAME, '038')
    return postgres_engine


@pytest.fixture(autouse=True)
def empty_identity_state(identity_database):
    with identity_database.begin() as connection:
        connection.execute(request_schema.RESOURCE_ACTION_ATTEMPTS.delete())
        connection.execute(request_schema.RESOURCE_ACTIONS.delete())
        connection.execute(serve_state_schema.replicas_table.delete())
        connection.execute(serve_state_schema.version_specs_table.delete())
        connection.execute(m4_schema.AUTHORITY_POLICY_EPOCHS.delete())
        connection.execute(serve_state_schema.services_table.delete())
        connection.execute(
            serve_state_schema.service_lifecycle_fences_table.delete())


def _project(
        version: int = 3) -> resource_actions.ServeServiceVersionSpecIdentityV1:
    return (identity_projector.
            project_potential_serve_service_version_spec_identity_v1(
                yaml_content=_YAML,
                service_name='svc',
                service_incarnation=_SERVICE_INCARNATION,
                service_version=version))


def _launch_spec(
    *,
    projected: resource_actions.ServeServiceVersionSpecIdentityV1 | None = None,
    yaml_content_sha256: str | None = None,
) -> resource_actions.ServeReplicaActionSpecV2:
    """Rebind the realistic V2 fixture to this test's immutable SQL YAML."""

    old = resource_actions.serve_replica_action_spec_from_value_v2(
        v2_fixtures._v2_launch_spec())
    old_launch = old.invocation.require_launch()
    digest = (hashlib.sha256(_YAML.encode('utf-8')).hexdigest()
              if yaml_content_sha256 is None else yaml_content_sha256)
    content = dataclasses.replace(old_launch.source.content,
                                  yaml_content_sha256=digest)
    source = resource_actions.project_provider_launch_source_v1(
        content, old_launch.source.identity_canonicalization)
    old_config = old_launch.execution_config
    renderer = dataclasses.replace(old_config.capsule.renderer, source=content)
    job_submission = dataclasses.replace(
        old_config.capsule.post_provision.job_submission, run_source=content)
    post_provision = dataclasses.replace(old_config.capsule.post_provision,
                                         job_submission=job_submission)
    capsule = dataclasses.replace(old_config.capsule,
                                  renderer=renderer,
                                  post_provision=post_provision)
    subject = resource_actions.project_provider_launch_policy_subject_v2(
        old.invocation.resource_identity, source,
        old.invocation.requested_target, old_launch.resources,
        old_launch.topology, old.invocation.resource_identity.replica_id,
        old_launch.retry_until_up, capsule)
    config = dataclasses.replace(
        old_config,
        capsule=capsule,
        execution_capsule_sha256=capsule.sha256,
        policy_subject=subject,
        policy_subject_sha256=subject.sha256,
        controller=v2_fixtures._rebound_proof(old_config.controller,
                                              subject.sha256),
        executor=v2_fixtures._rebound_proof(old_config.executor,
                                            subject.sha256))
    launch = dataclasses.replace(old_launch,
                                 source=source,
                                 execution_config=config)
    invocation = dataclasses.replace(old.invocation, launch=launch)
    plan = dataclasses.replace(old.provider_plan,
                               request_payload_sha256=invocation.sha256)
    identity = projected or _project()
    return resource_actions.ServeReplicaActionSpecV2(
        version=2,
        service_version_spec_identity=identity,
        service_version_spec_identity_sha256=identity.sha256,
        admission_binding=_authoritative_binding(),
        provider_plan=plan,
        invocation=invocation)


def _qualification_policy(
) -> authority_contracts.ResourceActionQualificationPolicyV1:
    return authority_fixtures._policy('e')


def _authoritative_binding(
) -> resource_actions.AuthoritativeActionPolicyBindingV1:
    policy = _qualification_policy()
    return resource_actions.AuthoritativeActionPolicyBindingV1(
        version=1,
        binding_kind='authoritative_action',
        policy_epoch=_POLICY_EPOCH,
        policy_sha256=policy.sha256,
        authority_binding_sha256='f' * 64)


def _down_spec(
    launch: resource_actions.ServeReplicaActionSpecV2,
    *,
    projected: resource_actions.ServeServiceVersionSpecIdentityV1 | None = None,
) -> resource_actions.ServeReplicaActionSpecV2:
    value = v2_fixtures._v2_down_spec(
        launch,
        service_version=(projected.service_version
                         if projected is not None else 3))
    identity = projected or _project()
    value['service_version_spec_identity'] = identity.canonical_value()
    value['service_version_spec_identity_sha256'] = identity.sha256
    parsed = resource_actions.serve_replica_action_spec_from_value_v2(value)
    return dataclasses.replace(parsed,
                               admission_binding=_authoritative_binding())


def _with_prior_source_store(
    down: resource_actions.ServeReplicaActionSpecV2,
    source_store: resource_actions.ProviderPriorLaunchSourceStoreV1,
) -> resource_actions.ServeReplicaActionSpecV2:
    member = down.invocation.require_down()
    basis = dataclasses.replace(member.prior_launch_basis,
                                source_store=source_store)
    old_config = member.execution_config
    subject = resource_actions.project_provider_down_policy_subject_v2(
        down.invocation.requested_target, member.workspace, basis,
        old_config.capsule)
    config = dataclasses.replace(
        old_config,
        policy_subject=subject,
        policy_subject_sha256=subject.sha256,
        controller=v2_fixtures._rebound_proof(old_config.controller,
                                              subject.sha256),
        executor=v2_fixtures._rebound_proof(old_config.executor,
                                            subject.sha256))
    member = dataclasses.replace(member,
                                 prior_launch_basis=basis,
                                 execution_config=config)
    invocation = dataclasses.replace(down.invocation, down=member)
    plan = dataclasses.replace(down.provider_plan,
                               prior_launch_basis_sha256=basis.sha256,
                               request_payload_sha256=invocation.sha256)
    return dataclasses.replace(down, provider_plan=plan, invocation=invocation)


def _owner() -> identity_state.ServeControllerOwnerFenceV1:
    return identity_state.ServeControllerOwnerFenceV1(
        service_name='svc',
        service_incarnation=_SERVICE_INCARNATION,
        lifecycle_epoch=7,
        controller_pid=1234,
        controller_ip='10.0.0.7')


def _replica(
    launch: resource_actions.ServeReplicaActionSpecV2,
    *,
    desired_generation: int = 1,
    down_action_id: uuid.UUID | None = None,
) -> identity_state.ServeReplicaActionRecordFenceV1:
    return identity_state.ServeReplicaActionRecordFenceV1(
        replica_id=7,
        replica_incarnation=_REPLICA_INCARNATION,
        desired_generation=desired_generation,
        creating_service_version=3,
        replica_state_version=1,
        is_spot=False,
        cluster_name='svc-7',
        sky_cluster_record_uuid=_CLUSTER_RECORD_UUID,
        launch_action_id=launch.action_id,
        down_action_id=down_action_id)


def _install_launch_rows(
    engine: sqlalchemy.engine.Engine,
    launch: resource_actions.ServeReplicaActionSpecV2,
    *,
    yaml_content: str = _YAML,
) -> None:
    binding = launch.require_authoritative_action_binding()
    projected = (identity_projector.
                 project_potential_serve_service_version_spec_identity_v1(
                     yaml_content=yaml_content,
                     service_name='svc',
                     service_incarnation=_SERVICE_INCARNATION,
                     service_version=3))
    with engine.begin() as connection:
        connection.execute(
            serve_state_schema.service_lifecycle_fences_table.insert().values(
                name='svc', epoch=7))
        connection.execute(serve_state_schema.services_table.insert().values(
            name='svc',
            workspace='boltz-test',
            status='READY',
            current_version=3,
            pool=0,
            controller_pid=1234,
            controller_ip='10.0.0.7',
            hash=str(_SERVICE_INCARNATION),
            lifecycle_epoch=7,
            resource_scope=str(_SERVICE_INCARNATION),
            resource_action_mode='authoritative',
            resource_action_mode_changed_at=datetime.datetime.now(_UTC),
            resource_action_candidate_epoch=binding.policy_epoch,
            resource_action_candidate_policy_sha256=(binding.policy_sha256),
            resource_action_candidate_binding_sha256=(
                binding.authority_binding_sha256)))
        _insert_open_policy(connection)
        # Invalid pickle bytes are intentional: the identity boundary must
        # never decode this historical compatibility column.
        connection.execute(
            serve_state_schema.version_specs_table.insert().values(
                service_name='svc',
                version=3,
                spec=b'not-a-pickle-and-never-read',
                yaml_content=yaml_content,
                submitted_yaml_content='different-submitted-source',
                resource_action_spec_identity=projected.canonical_value(),
                resource_action_spec_identity_sha256=projected.sha256))
        connection.execute(serve_state_schema.replicas_table.insert().values(
            service_name='svc',
            replica_id=7,
            replica_info=b'not-a-pickle-and-never-read',
            replica_state_version=1,
            status='PROVISIONING',
            version=3,
            cluster_name='svc-7',
            created_at=1.0,
            is_spot=False,
            replica_incarnation=_REPLICA_INCARNATION,
            desired_generation=1,
            sky_cluster_record_uuid=_CLUSTER_RECORD_UUID))


def _insert_launch_action(
    engine: sqlalchemy.engine.Engine,
    launch: resource_actions.ServeReplicaActionSpecV2,
) -> None:
    identity = launch.invocation.resource_identity.action_identity(
        kernel_actions.ActionKind.LAUNCH)
    now = datetime.datetime.now(_UTC)
    with engine.begin() as connection:
        connection.execute(request_schema.RESOURCE_ACTIONS.insert().values(
            action_id=launch.action_id,
            domain='serve',
            resource_type='replica',
            resource_identity=identity.resource_identity,
            desired_generation=identity.desired_generation,
            action_type=kernel_actions.ActionKind.LAUNCH.value,
            immutable_spec=launch.canonical_value(),
            immutable_spec_sha256=launch.sha256,
            kernel_state='READY',
            current_attempt=0,
            next_attempt_at=now,
            revision=1,
            created_at=now,
            updated_at=now))


def _insert_open_policy(connection: sqlalchemy.engine.Connection) -> None:
    policy = _qualification_policy()
    binding = _authoritative_binding()
    now = datetime.datetime.now(_UTC)
    fence = authority_contracts.AuthorityServiceFenceV1(
        service_name='svc',
        service_hash=str(_SERVICE_INCARNATION),
        controller_owner_fence='1234:10.0.0.7',
        lifecycle_epoch=7)
    proof = authority_contracts.AuthoritativePromotionProofV1(
        version=1,
        service_fence=fence,
        candidate_epoch=binding.policy_epoch,
        candidate_since=authority_fixtures._timestamp(now - datetime.timedelta(
            hours=25)),
        verified_at=authority_fixtures._timestamp(now),
        candidate_duration_seconds=90_000,
        qualification_policy_sha256=policy.sha256,
        qualification_binding_sha256=binding.authority_binding_sha256,
        coverage_inventory_sha256='c' * 64,
        clean_launches=100,
        clean_downs=100,
        blocker_count=0,
        crash_canary_inventory=authority_fixtures._hashed('crash'),
        referenced_cohort_inventory=authority_fixtures._hashed('cohort'),
        deployment_inventory=authority_fixtures._hashed('deployment'),
        elected_version_identity=authority_fixtures._hashed('version'),
        live_replica_identity_inventory=authority_fixtures._hashed('replicas'),
        schema_heads=authority_contracts.AuthoritySchemaHeadsV1(
            api_requests_head='007',
            serve_head='035',
            global_user_state_head='028'))
    inventory = authority_fixtures._empty_inventory()
    connection.execute(m4_schema.AUTHORITY_POLICY_EPOCHS.insert().values(
        service_hash=str(_SERVICE_INCARNATION),
        policy_epoch=binding.policy_epoch,
        predecessor_policy_epoch=None,
        policy=policy.canonical_value(),
        policy_sha256=policy.sha256,
        authority_binding_sha256=binding.authority_binding_sha256,
        rotation_proof=proof.canonical_value(),
        rotation_proof_sha256=proof.sha256,
        nonterminal_inventory=inventory.canonical_value(),
        nonterminal_inventory_sha256=inventory.sha256,
        reason='INITIAL_PROMOTION',
        policy_state='ACTIVE',
        admission_state='OPEN',
        admission_revision=1,
        last_operation_id=uuid.uuid4(),
        last_operation_kind='ACTIVATE',
        created_at=now,
        admission_changed_at=now,
        activated_at=now,
        superseded_at=None))


def _prepare_down(
    engine: sqlalchemy.engine.Engine,
    launch: resource_actions.ServeReplicaActionSpecV2,
    down: resource_actions.ServeReplicaActionSpecV2,
) -> None:
    _insert_launch_action(engine, launch)
    version_four = _project(version=4)
    with engine.begin() as connection:
        connection.execute(serve_state_schema.services_table.update().where(
            serve_state_schema.services_table.c.name == 'svc').values(
                current_version=4, resource_action_mode='authoritative'))
        connection.execute(
            serve_state_schema.version_specs_table.update().where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 3).values(
                    resource_action_spec_identity=(
                        launch.service_version_spec_identity.canonical_value()),
                    resource_action_spec_identity_sha256=(
                        launch.service_version_spec_identity_sha256)))
        connection.execute(
            serve_state_schema.version_specs_table.insert().values(
                service_name='svc',
                version=4,
                spec=b'newer-pickle-is-also-never-read',
                yaml_content=_YAML,
                resource_action_spec_identity=version_four.canonical_value(),
                resource_action_spec_identity_sha256=version_four.sha256))
        connection.execute(serve_state_schema.replicas_table.update().where(
            serve_state_schema.replicas_table.c.service_name == 'svc',
            serve_state_schema.replicas_table.c.replica_id == 7).values(
                desired_generation=2,
                launch_action_id=launch.action_id,
                down_action_id=down.action_id,
                resource_action_spec_identity_sha256=(
                    launch.service_version_spec_identity_sha256)))


def test_store_is_postgresql_only_and_requires_caller_transaction(
        identity_database) -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    with pytest.raises(RuntimeError, match='requires PostgreSQL'):
        identity_state.ServeServiceVersionIdentityStore(sqlite)

    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    with identity_database.connect() as connection:
        with pytest.raises(RuntimeError, match='caller-owned transaction'):
            store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)


def test_action_link_slice_rejects_shadow_candidate_binding(
        identity_database) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    shadow_binding = resource_actions.ShadowCandidateActionBindingV1.from_value(
        v2_fixtures._shadow_binding())
    shadow_launch = dataclasses.replace(launch,
                                        admission_binding=shadow_binding)
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityConflict,
                           match='only authoritative-bound'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             stage_authoritative_launch_class2_replica_link_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch),
                 action_spec=shadow_launch))


@pytest.mark.parametrize('status', [
    ServiceStatus.SHUTTING_DOWN.value,
    ServiceStatus.FAILED_CLEANUP.value,
])
def test_authoritative_launch_rejects_canonical_launch_blocking_status(
        identity_database, status: str) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    with identity_database.begin() as connection:
        connection.execute(
            serve_state_schema.services_table.update().values(status=status))
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityConflict,
                           match='status blocks'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             stage_authoritative_launch_class2_replica_link_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch),
                 action_spec=launch))


def test_authoritative_launch_rejects_malformed_service_status(
        identity_database) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    with identity_database.begin() as connection:
        connection.execute(
            serve_state_schema.services_table.update().values(status='ready'))
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityCorruption,
                           match='invalid canonical status'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             stage_authoritative_launch_class2_replica_link_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch),
                 action_spec=launch))


def test_authoritative_launch_rejects_nonpositive_lifecycle_fence(
        identity_database) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    with identity_database.begin() as connection:
        connection.execute(
            serve_state_schema.service_lifecycle_fences_table.update().values(
                epoch=0))
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityCorruption,
                           match='positive integer'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             stage_authoritative_launch_class2_replica_link_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch),
                 action_spec=launch))


def test_launch_replica_link_is_idempotent_and_later_gate_rolls_it_back(
        identity_database) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    store = identity_state.ServeServiceVersionIdentityStore(identity_database)

    class LaterAdmissionGateRejected(RuntimeError):
        pass

    with pytest.raises(LaterAdmissionGateRejected):
        with identity_database.begin() as connection:
            first = store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)
            second = store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)
            assert first.replica_link_initialized
            assert not second.replica_link_initialized
            assert first.class2_evidence.action_identity.identity.canonical_bytes == (
                launch.service_version_spec_identity.canonical_bytes)
            assert (first.class2_evidence.action_identity.yaml_content_sha256 ==
                    hashlib.sha256(_YAML.encode('utf-8')).hexdigest())
            raise LaterAdmissionGateRejected('simulated later cohort gate')

    with identity_database.connect() as connection:
        version = connection.execute(
            sqlalchemy.select(
                serve_state_schema.version_specs_table.c.
                resource_action_spec_identity,
                serve_state_schema.version_specs_table.c.
                resource_action_spec_identity_sha256)).mappings().one()
        replica_hash = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                resource_action_spec_identity_sha256)).scalar_one()
        launch_action_id = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table.c.
                              launch_action_id)).scalar_one()
    assert resource_actions.canonical_json_bytes(
        version['resource_action_spec_identity']) == (
            launch.service_version_spec_identity.canonical_bytes)
    assert version['resource_action_spec_identity_sha256'] == (
        launch.service_version_spec_identity_sha256)
    assert replica_hash is None
    assert launch_action_id is None


def test_launch_rejects_mutated_persisted_version_identity(
        identity_database) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    corrupted = dataclasses.replace(launch.service_version_spec_identity,
                                    effective_task_config_sha256='f' * 64)
    with identity_database.begin() as connection:
        connection.execute(
            serve_state_schema.version_specs_table.update().values(
                resource_action_spec_identity=corrupted.canonical_value(),
                resource_action_spec_identity_sha256=corrupted.sha256))

    with pytest.raises(identity_state.ServeServiceVersionIdentityCorruption,
                       match='differs from immutable YAML'):
        with identity_database.begin() as connection:
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             stage_authoritative_launch_class2_replica_link_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch),
                 action_spec=launch))


def test_launch_never_heals_null_version_identity_beneath_linked_replica(
        identity_database) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    with identity_database.begin() as connection:
        connection.execute(
            serve_state_schema.version_specs_table.update().values(
                resource_action_spec_identity=None,
                resource_action_spec_identity_sha256=None))
        connection.execute(serve_state_schema.replicas_table.update().values(
            launch_action_id=launch.action_id,
            resource_action_spec_identity_sha256=(
                launch.service_version_spec_identity_sha256)))

    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityConflict,
                           match='initialized immutable identity'):
            store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)
    with identity_database.connect() as connection:
        pair = connection.execute(
            sqlalchemy.select(
                serve_state_schema.version_specs_table.c.
                resource_action_spec_identity,
                serve_state_schema.version_specs_table.c.
                resource_action_spec_identity_sha256)).one()
    assert pair == (None, None)


def test_launch_existing_action_link_rejects_competing_shadow_link(
        identity_database) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    identity = launch.service_version_spec_identity
    with identity_database.begin() as connection:
        shadow_id = uuid.uuid4()
        connection.execute(
            serve_state_schema.version_specs_table.update().values(
                resource_action_spec_identity=identity.canonical_value(),
                resource_action_spec_identity_sha256=identity.sha256))
        connection.execute(serve_state_schema.replicas_table.update().values(
            launch_action_id=launch.action_id,
            down_shadow_coverage_id=shadow_id,
            down_shadow_sample_id=shadow_id,
            resource_action_spec_identity_sha256=identity.sha256))

    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityConflict,
                           match='exclusive'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             stage_authoritative_launch_class2_replica_link_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch),
                 action_spec=launch))


@pytest.mark.parametrize('drift', [
    'controller_owner',
    'lifecycle_epoch',
    'lifecycle_fence_epoch',
    'missing_lifecycle_fence',
    'service_incarnation',
    'elected_version',
    'immutable_yaml',
    'replica_version',
    'spot_replica',
    'paid_replica',
    'caller_identity',
    'source_hash',
])
def test_launch_identity_drift_never_stages_replica_link(
        identity_database, drift: str) -> None:
    projected = _project()
    if drift == 'caller_identity':
        projected = dataclasses.replace(projected,
                                        effective_task_config_sha256='f' * 64)
    launch = _launch_spec(
        projected=projected,
        yaml_content_sha256=('e' * 64 if drift == 'source_hash' else None))
    _install_launch_rows(
        identity_database,
        launch,
        yaml_content=(_YAML +
                      '\n# drift\n' if drift == 'immutable_yaml' else _YAML))
    with identity_database.begin() as connection:
        if drift == 'controller_owner':
            connection.execute(
                serve_state_schema.services_table.update().values(
                    controller_pid=9999))
        elif drift == 'lifecycle_epoch':
            connection.execute(
                serve_state_schema.services_table.update().values(
                    lifecycle_epoch=8))
        elif drift == 'lifecycle_fence_epoch':
            connection.execute(
                serve_state_schema.service_lifecycle_fences_table.update(
                ).values(epoch=8))
        elif drift == 'missing_lifecycle_fence':
            connection.execute(
                serve_state_schema.service_lifecycle_fences_table.delete())
        elif drift == 'service_incarnation':
            connection.execute(
                serve_state_schema.services_table.update().values(
                    hash='44444444-4444-4444-8444-444444444444'))
        elif drift == 'elected_version':
            connection.execute(
                serve_state_schema.services_table.update().values(
                    current_version=4))
        elif drift == 'replica_version':
            connection.execute(
                serve_state_schema.replicas_table.update().values(version=4))
        elif drift == 'spot_replica':
            connection.execute(
                serve_state_schema.replicas_table.update().values(is_spot=True))
        elif drift == 'paid_replica':
            connection.execute(
                serve_state_schema.replicas_table.update().values(
                    paid_capacity_pool_key='paid-pool'))

    with pytest.raises(identity_state.ServeServiceVersionIdentityConflict):
        with identity_database.begin() as connection:
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             stage_authoritative_launch_class2_replica_link_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch),
                 action_spec=launch))
    with identity_database.connect() as connection:
        version_pair = connection.execute(
            sqlalchemy.select(
                serve_state_schema.version_specs_table.c.
                resource_action_spec_identity,
                serve_state_schema.version_specs_table.c.
                resource_action_spec_identity_sha256)).one()
        replica_hash = connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                resource_action_spec_identity_sha256)).scalar_one()
    assert resource_actions.canonical_json_bytes(
        version_pair[0]) == (_project().canonical_bytes)
    assert version_pair[1] == _project().sha256
    assert replica_hash is None


def test_version_update_waits_for_identity_transaction_row_lock(
        identity_database, monkeypatch) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    projector_entered = threading.Event()
    release_projector = threading.Event()
    update_finished = threading.Event()
    original = (identity_projector.
                project_potential_serve_service_version_spec_identity_v1)

    def blocking_projector(**kwargs):
        projector_entered.set()
        assert release_projector.wait(timeout=10)
        return original(**kwargs)

    monkeypatch.setattr(
        identity_projector,
        'project_potential_serve_service_version_spec_identity_v1',
        blocking_projector)

    def stage() -> None:
        with identity_database.connect() as connection:
            transaction = connection.begin()
            store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)
            transaction.rollback()

    def update() -> None:
        with identity_database.begin() as connection:
            connection.execute(serve_state_schema.version_specs_table.update(
            ).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 3).values(
                    submitted_yaml_content='concurrent-update'))
        update_finished.set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        stage_future = executor.submit(stage)
        assert projector_entered.wait(timeout=10)
        update_future = executor.submit(update)
        assert not update_finished.wait(timeout=0.25)
        release_projector.set()
        stage_future.result(timeout=10)
        update_future.result(timeout=10)
    assert update_finished.is_set()


def test_lifecycle_claimant_waits_for_class1_admission_lock(
        identity_database, monkeypatch) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    service_lock_entered = threading.Event()
    release_service_lock = threading.Event()
    claimant_finished = threading.Event()
    original = identity_state.ServeServiceVersionIdentityStore._lock_service

    def blocking_service_lock(connection, owner, action_spec, *,
                              require_elected_version):
        service_lock_entered.set()
        assert release_service_lock.wait(timeout=10)
        return original(connection,
                        owner,
                        action_spec,
                        require_elected_version=require_elected_version)

    monkeypatch.setattr(identity_state.ServeServiceVersionIdentityStore,
                        '_lock_service', staticmethod(blocking_service_lock))

    def stage() -> None:
        with identity_database.connect() as connection:
            transaction = connection.begin()
            store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)
            transaction.rollback()

    def claim() -> None:
        with identity_database.begin() as connection:
            connection.execute(
                serve_state_schema.service_lifecycle_fences_table.update(
                ).values(epoch=8))
            connection.execute(
                serve_state_schema.services_table.update().values(
                    lifecycle_epoch=8))
        claimant_finished.set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        stage_future = executor.submit(stage)
        assert service_lock_entered.wait(timeout=10)
        claim_future = executor.submit(claim)
        assert not claimant_finished.wait(timeout=0.25)
        release_service_lock.set()
        stage_future.result(timeout=10)
        claim_future.result(timeout=10)
    assert claimant_finished.is_set()


@pytest.mark.parametrize('transition', ['drain', 'supersede'])
def test_policy_drain_or_rotation_waits_for_admission_lock(
        identity_database, monkeypatch, transition: str) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    projector_entered = threading.Event()
    release_projector = threading.Event()
    transition_finished = threading.Event()
    original = (identity_projector.
                project_potential_serve_service_version_spec_identity_v1)

    def blocking_projector(**kwargs):
        projector_entered.set()
        assert release_projector.wait(timeout=10)
        return original(**kwargs)

    monkeypatch.setattr(
        identity_projector,
        'project_potential_serve_service_version_spec_identity_v1',
        blocking_projector)

    def stage() -> None:
        with identity_database.connect() as connection:
            transaction = connection.begin()
            store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)
            transaction.rollback()

    def transition_policy() -> None:
        now = datetime.datetime.now(_UTC)
        values = {
            'admission_state': 'DRAINING',
            'admission_revision': 2,
            'last_operation_id': uuid.uuid4(),
            'last_operation_kind': 'DRAIN',
            'admission_changed_at': now,
        }
        if transition == 'supersede':
            values.update(policy_state='SUPERSEDED',
                          admission_state='CLOSED',
                          last_operation_kind='SUPERSEDE',
                          superseded_at=now)
        with identity_database.begin() as connection:
            connection.execute(
                m4_schema.AUTHORITY_POLICY_EPOCHS.update().values(**values))
        transition_finished.set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        stage_future = executor.submit(stage)
        assert projector_entered.wait(timeout=10)
        transition_future = executor.submit(transition_policy)
        assert not transition_finished.wait(timeout=0.25)
        release_projector.set()
        stage_future.result(timeout=10)
        transition_future.result(timeout=10)
    assert transition_finished.is_set()


def test_replica_update_waits_for_locked_replica(identity_database,
                                                 monkeypatch) -> None:
    launch = _launch_spec()
    _install_launch_rows(identity_database, launch)
    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    replica_locked = threading.Event()
    release_replica = threading.Event()
    update_finished = threading.Event()
    original = identity_state.ServeServiceVersionIdentityStore._stage_replica_identity

    def blocking_stage(connection, **kwargs):
        replica_locked.set()
        assert release_replica.wait(timeout=10)
        return original(connection, **kwargs)

    monkeypatch.setattr(identity_state.ServeServiceVersionIdentityStore,
                        '_stage_replica_identity', staticmethod(blocking_stage))

    def stage() -> None:
        with identity_database.connect() as connection:
            transaction = connection.begin()
            store.stage_authoritative_launch_class2_replica_link_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch),
                action_spec=launch)
            transaction.rollback()

    def update() -> None:
        with identity_database.begin() as connection:
            connection.execute(
                serve_state_schema.replicas_table.update().values(
                    status='READY'))
        update_finished.set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        stage_future = executor.submit(stage)
        assert replica_locked.wait(timeout=10)
        update_future = executor.submit(update)
        assert not update_finished.wait(timeout=0.25)
        release_replica.set()
        stage_future.result(timeout=10)
        update_future.result(timeout=10)
    assert update_finished.is_set()


def test_down_locks_both_versions_and_reads_exact_prior_launch_snapshot(
        identity_database) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    store = identity_state.ServeServiceVersionIdentityStore(identity_database)

    with identity_database.begin() as connection:
        result = (
            store.
            project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                connection,
                owner=_owner(),
                replica=_replica(launch,
                                 desired_generation=2,
                                 down_action_id=down.action_id),
                down_action_spec=down))

    assert tuple(item.identity.service_version
                 for item in result.class2_evidence.locked_versions) == (3, 4)
    assert result.class2_evidence.action_identity.identity.service_version == 3
    assert result.class2_evidence.elected_identity.identity.service_version == 4
    assert result.class2_evidence.authoritative_policy.binding.canonical_bytes == (
        down.admission_binding.canonical_bytes)
    assert result.class2_evidence.authoritative_policy.record.policy.sha256 == (
        _qualification_policy().sha256)
    assert result.prior_launch_spec.canonical_bytes == launch.canonical_bytes
    assert result.prior_launch_spec_sha256 == launch.sha256
    result.revalidate_locked_prior_launch_spec(launch)
    different_binding = dataclasses.replace(
        launch.require_authoritative_action_binding(),
        authority_binding_sha256='a' * 64)
    with pytest.raises(identity_state.ServeServiceVersionIdentityConflict,
                       match='differs from its optimistic snapshot'):
        result.revalidate_locked_prior_launch_spec(
            dataclasses.replace(launch, admission_binding=different_binding))


def test_api_source_down_rejects_shadow_bound_prior_launch_spec(
        identity_database) -> None:
    launch = _launch_spec()
    shadow_launch = dataclasses.replace(
        launch,
        admission_binding=resource_actions.ShadowCandidateActionBindingV1.
        from_value(v2_fixtures._shadow_binding()))
    down = _down_spec(shadow_launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, shadow_launch, down)
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityCorruption,
                           match='not valid V2'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(shadow_launch,
                                  desired_generation=2,
                                  down_action_id=down.action_id),
                 down_action_spec=down))


def test_api_source_down_allows_distinct_historical_authoritative_policy(
        identity_database) -> None:
    current_launch = _launch_spec()
    historical_binding = resource_actions.AuthoritativeActionPolicyBindingV1(
        version=1,
        binding_kind='authoritative_action',
        policy_epoch=uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'),
        policy_sha256='a' * 64,
        authority_binding_sha256='b' * 64)
    historical_launch = dataclasses.replace(
        current_launch, admission_binding=historical_binding)
    down = _down_spec(historical_launch)
    _install_launch_rows(identity_database, current_launch)
    _prepare_down(identity_database, historical_launch, down)
    with identity_database.begin() as connection:
        result = (identity_state.ServeServiceVersionIdentityStore(
            identity_database
        ).project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
            connection,
            owner=_owner(),
            replica=_replica(historical_launch,
                             desired_generation=2,
                             down_action_id=down.action_id),
            down_action_spec=down))
    assert result.prior_launch_spec.admission_binding == historical_binding
    assert result.class2_evidence.authoritative_policy.binding == (
        _authoritative_binding())


@pytest.mark.parametrize('policy_case', [
    'missing',
    'wrong_hash',
    'wrong_binding',
    'draining',
    'closed',
    'superseded',
])
def test_authoritative_down_requires_exact_active_open_policy_before_versions(
        identity_database, policy_case: str) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    table = m4_schema.AUTHORITY_POLICY_EPOCHS
    now = datetime.datetime.now(_UTC)
    with identity_database.begin() as connection:
        if policy_case == 'missing':
            connection.execute(table.delete())
        elif policy_case == 'wrong_hash':
            connection.execute(table.update().values(policy_sha256='a' * 64))
        elif policy_case == 'wrong_binding':
            connection.execute(
                table.update().values(authority_binding_sha256='a' * 64))
        elif policy_case == 'draining':
            connection.execute(table.update().values(
                admission_state='DRAINING',
                admission_revision=2,
                last_operation_id=uuid.uuid4(),
                last_operation_kind='DRAIN',
                admission_changed_at=now))
        elif policy_case == 'closed':
            connection.execute(table.update().values(
                admission_state='CLOSED',
                admission_revision=2,
                last_operation_id=uuid.uuid4(),
                last_operation_kind='CLOSE',
                admission_changed_at=now))
        else:
            connection.execute(table.update().values(
                policy_state='SUPERSEDED',
                admission_state='CLOSED',
                admission_revision=2,
                last_operation_id=uuid.uuid4(),
                last_operation_kind='SUPERSEDE',
                admission_changed_at=now,
                superseded_at=now))

    with identity_database.begin() as connection:
        with pytest.raises(
                identity_state.ServeServiceVersionIdentityStateError):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch,
                                  desired_generation=2,
                                  down_action_id=down.action_id),
                 down_action_spec=down))


def test_authoritative_policy_uses_strict_typed_evidence_decoder(
        identity_database) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    malformed = {'version': 1, 'unexpected': True}
    with identity_database.begin() as connection:
        connection.execute(m4_schema.AUTHORITY_POLICY_EPOCHS.update().values(
            rotation_proof=malformed,
            rotation_proof_sha256=resource_actions.canonical_sha256(malformed)))
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityCorruption,
                           match='Locked authoritative policy is invalid'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch,
                                  desired_generation=2,
                                  down_action_id=down.action_id),
                 down_action_spec=down))


def test_down_sql_lock_order_and_prior_launch_read_are_explicit(
        identity_database) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    statements: list[tuple[str, object]] = []

    def capture(_connection, _cursor, statement, parameters, _context,
                _executemany) -> None:
        statements.append((statement, parameters))

    sqlalchemy.event.listen(identity_database, 'before_cursor_execute', capture)
    try:
        with identity_database.begin() as connection:
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch,
                                  desired_generation=2,
                                  down_action_id=down.action_id),
                 down_action_spec=down))
    finally:
        sqlalchemy.event.remove(identity_database, 'before_cursor_execute',
                                capture)

    lock_kinds: list[str] = []
    version_parameters: list[int] = []
    for statement, parameters in statements:
        if 'FOR UPDATE' not in statement:
            continue
        if 'service_lifecycle_fences' in statement:
            lock_kinds.append('lifecycle')
        elif 'serve_resource_action_authority_policy_epochs' in statement:
            lock_kinds.append('policy')
        elif 'FROM version_specs' in statement:
            lock_kinds.append('version')
            assert isinstance(parameters, dict)
            version_parameters.append(parameters['version_1'])
        elif 'FROM replicas' in statement:
            lock_kinds.append('replica')
        elif 'FROM services' in statement:
            lock_kinds.append('service')
    assert lock_kinds == [
        'lifecycle', 'service', 'policy', 'version', 'version', 'replica'
    ]
    assert version_parameters == [3, 4]
    action_reads = [
        statement for statement, _ in statements
        if 'api_resource_actions' in statement
    ]
    assert len(action_reads) == 1
    assert 'FOR UPDATE' not in action_reads[0]


def test_down_rejects_shadow_sample_source_before_api_action_read(
        identity_database) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    shadow_down = _with_prior_source_store(
        down, resource_actions.ProviderPriorLaunchSourceStoreV1.
        SERVE_RESOURCE_ACTION_SHADOW_SAMPLES)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    action_read = False

    def capture(_connection, _cursor, statement, _parameters, _context,
                _executemany) -> None:
        nonlocal action_read
        action_read = action_read or 'api_resource_actions' in statement

    sqlalchemy.event.listen(identity_database, 'before_cursor_execute', capture)
    try:
        with identity_database.begin() as connection:
            with pytest.raises(
                    identity_state.ServeServiceVersionIdentityConflict,
                    match='Shadow-sample'):
                (identity_state.ServeServiceVersionIdentityStore(
                    identity_database).
                 project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                     connection,
                     owner=_owner(),
                     replica=_replica(launch,
                                      desired_generation=2,
                                      down_action_id=shadow_down.action_id),
                     down_action_spec=shadow_down))
    finally:
        sqlalchemy.event.remove(identity_database, 'before_cursor_execute',
                                capture)
    assert not action_read


def test_down_rejects_competing_shadow_linkage(identity_database) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    shadow_id = uuid.uuid4()
    with identity_database.begin() as connection:
        connection.execute(serve_state_schema.replicas_table.update().values(
            launch_action_id=None,
            launch_shadow_coverage_id=shadow_id,
            launch_shadow_sample_id=shadow_id))

    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityConflict,
                           match='exclusive'):
            (identity_state.ServeServiceVersionIdentityStore(identity_database).
             project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch,
                                  desired_generation=2,
                                  down_action_id=down.action_id),
                 down_action_spec=down))


@pytest.mark.parametrize('missing_version', [3, 4])
def test_down_requires_both_creating_and_elected_identity_pairs(
        identity_database, missing_version: int) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    with identity_database.begin() as connection:
        connection.execute(
            serve_state_schema.version_specs_table.update().where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version ==
                missing_version).values(
                    resource_action_spec_identity=None,
                    resource_action_spec_identity_sha256=None))

    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    with identity_database.begin() as connection:
        with pytest.raises(identity_state.ServeServiceVersionIdentityConflict,
                           match='every locked service version'):
            (store.
             project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch,
                                  desired_generation=2,
                                  down_action_id=down.action_id),
                 down_action_spec=down))


@pytest.mark.parametrize('drift', ['hash', 'v1_spec', 'replica_hash'])
def test_down_rejects_prior_launch_or_replica_identity_drift(
        identity_database, drift: str) -> None:
    launch = _launch_spec()
    down = _down_spec(launch)
    _install_launch_rows(identity_database, launch)
    _prepare_down(identity_database, launch, down)
    with identity_database.begin() as connection:
        if drift == 'hash':
            connection.execute(request_schema.RESOURCE_ACTIONS.update().values(
                immutable_spec_sha256='f' * 64))
        elif drift == 'v1_spec':
            old = resource_actions.ServeReplicaActionSpecV1.from_value(
                v2_fixtures.v1_fixtures._launch_spec())
            connection.execute(request_schema.RESOURCE_ACTIONS.update().values(
                immutable_spec=old.canonical_value(),
                immutable_spec_sha256=old.sha256))
        else:
            connection.execute(
                serve_state_schema.replicas_table.update().values(
                    resource_action_spec_identity_sha256='f' * 64))

    store = identity_state.ServeServiceVersionIdentityStore(identity_database)
    expected_error = (identity_state.ServeServiceVersionIdentityCorruption
                      if drift != 'replica_hash' else
                      identity_state.ServeServiceVersionIdentityConflict)
    with identity_database.begin() as connection:
        with pytest.raises(expected_error):
            (store.
             project_authoritative_down_class2_and_read_prior_launch_snapshot_in_transaction(
                 connection,
                 owner=_owner(),
                 replica=_replica(launch,
                                  desired_generation=2,
                                  down_action_id=down.action_id),
                 down_action_spec=down))
