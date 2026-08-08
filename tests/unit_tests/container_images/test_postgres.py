"""Real PostgreSQL proofs for the managed image state machine and migration."""
# pylint: disable=protected-access,redefined-outer-name

from __future__ import annotations

from collections.abc import Iterator
import concurrent.futures
import dataclasses
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import types
from typing import Any
import uuid

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy
from sqlalchemy import orm

from sky import global_user_state
from sky.container_images import aws
from sky.container_images import builder_prototype
from sky.container_images import canary_worker_service
from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import copy_worker_service
from sky.container_images import demand_state
from sky.container_images import lifecycle_worker_service
from sky.container_images import models
from sky.container_images import preparation
from sky.container_images import publication
from sky.container_images import qualification
from sky.container_images import runtime
from sky.container_images import schema
from sky.container_images import topology_state
from sky.container_images import transactions
from sky.container_images import worker_lease
from sky.jobs import state_storage
from sky.serve import serve_state
from sky.skylet import constants
from sky.utils.db import migration_utils

_POSTGRES_REQUIRED = os.environ.get(
    'SKYPILOT_REQUIRE_CONTAINER_IMAGE_POSTGRES') == '1'


def _required_import(module: str):
    try:
        return importlib.import_module(module)
    except ImportError:
        if _POSTGRES_REQUIRED:
            pytest.fail(f'{module} is required for container image tests.',
                        pytrace=False)
        pytest.skip(f'{module} unavailable; skipping real-PostgreSQL tests.',
                    allow_module_level=True)


testcontainers_postgres = _required_import('testcontainers.postgres')
_required_import('psycopg2')

if shutil.which('docker') is None:
    if _POSTGRES_REQUIRED:
        pytest.fail('Docker is required for container image PostgreSQL tests.',
                    pytrace=False)
    pytest.skip('docker unavailable; skipping real-PostgreSQL image tests',
                allow_module_level=True)

_DIGEST = 'sha256:' + 'a' * 64
_OTHER_DIGEST = 'sha256:' + 'b' * 64
_CONFIG_DIGEST = 'sha256:' + 'c' * 64
_SOURCE = f'ghcr.io/boltz-bio/runtime@{_DIGEST}'
_OTHER_SOURCE = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
_MANIFEST_MEDIA_TYPE = 'application/vnd.oci.image.manifest.v1+json'


@pytest.fixture(scope='module')
def postgres_engine():
    """Starts one isolated PostgreSQL 16 server for this test module."""
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    try:
        container.start()
    except Exception as error:  # pylint: disable=broad-except
        if _POSTGRES_REQUIRED:
            pytest.fail(f'could not start PostgreSQL container: {error}',
                        pytrace=False)
        pytest.skip(f'could not start PostgreSQL container: {error}')
    engine = sqlalchemy.create_engine(
        container.get_connection_url(),
        connect_args={'options': '-c statement_timeout=15000'},
        pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


@pytest.fixture
def image_database(postgres_engine, monkeypatch: pytest.MonkeyPatch):
    """Creates fresh runtime metadata and routes all repositories to it."""
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    schema.metadata.create_all(postgres_engine)
    monkeypatch.setattr(catalog_state, 'engine', lambda: postgres_engine)
    with postgres_engine.begin() as connection:
        connection.execute(schema.catalog.insert().values(
            id='authority',
            authority_id='00000000-0000-4000-8000-000000000001',
            created_at=1))
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    return postgres_engine


@pytest.mark.parametrize('mutation', [
    'DELETE FROM container_image_catalog',
    "UPDATE container_image_catalog SET authority_id = 'invalid'",
    ("INSERT INTO container_image_catalog (id, authority_id, created_at) "
     "VALUES ('extra', '00000000-0000-4000-8000-000000000002', 1)"),
])
def test_catalog_authority_fails_closed_without_runtime_repair(
        image_database, mutation: str) -> None:
    with image_database.begin() as connection:
        connection.exec_driver_sql(mutation)

    with pytest.raises(RuntimeError, match='missing or malformed'):
        catalog_state.get_catalog_authority_id()


def test_operation_lookup_filters_authorized_kinds_in_postgres(
        image_database) -> None:
    assert image_database is not None
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    operation, _ = catalog_state.create_or_get_operation(
        authority_id=authority,
        scope='research',
        actor_hash='1' * 64,
        kind='PROFILE_CANARY',
        idempotency_key='operation-kind-filter-key',
        request_hash='2' * 64,
        now=10)

    assert catalog_state.get_operation(
        operation.id,
        'research',
        allowed_kinds=catalog_state.PUBLIC_OPERATION_KINDS) is None
    visible = catalog_state.get_operation(
        operation.id,
        'research',
        allowed_kinds=catalog_state.ALL_OPERATION_KINDS)
    assert visible is not None and visible.id == operation.id


def _activate_profile(
    engine: sqlalchemy.engine.Engine,
    profile: models.ManagedRegistryProfile,
    *,
    workspace: str = 'research',
) -> topology_state.ProfileRevisionRecord:
    revision = topology_state.stage_profile_revision(
        workspace=workspace,
        profile=profile.name,
        revision=profile.revision,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        physical_manifest_hash=profile.physical_manifest_hash,
        max_daily_canary_microusd=(
            profile.qualification.max_daily_canary_microusd),
        now=10)
    with orm.Session(engine) as session, session.begin():
        for target in (profile.canonical,) + profile.targets:
            for shard_index in range(target.shard_count):
                fingerprint = hashlib.sha256(
                    f'{target.target_fingerprint}:{shard_index}'.encode(
                    )).hexdigest()
                repository_name = (
                    f'{target.repository_prefix}/test/s{shard_index:02x}')
                topology_state.upsert_qualified_shard(
                    session,
                    workspace=workspace,
                    profile=profile.name,
                    target_id=target.name,
                    provider='aws',
                    partition=profile.partition,
                    account=profile.registry_account,
                    region=target.region,
                    shard_generation=0,
                    shard_index=shard_index,
                    target_fingerprint=target.target_fingerprint,
                    physical_fingerprint=fingerprint,
                    registry=target.registry,
                    repository_name=repository_name,
                    repository_arn=(
                        f'arn:{profile.partition}:ecr:{target.region}:'
                        f'{profile.registry_account}:repository/'
                        f'{repository_name}'),
                    max_manifests=100,
                    max_declared_bytes=1_000_000,
                    max_in_flight=4,
                    now=11)
        session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.workspace == workspace,
            schema.registry_shards.c.profile == profile.name).values(
                state=models.ImageShardState.READY.value,
                qualified_at=11,
                inventory_epoch=1,
                inventory_started_at=10,
                inventory_completed_at=11,
                updated_at=11))
        assert not any(
            session.execute(
                sqlalchemy.select(
                    schema.registry_shards.c.eviction_enabled).where(
                        schema.registry_shards.c.workspace == workspace,
                        schema.registry_shards.c.profile
                        == profile.name)).scalars())
    attested = topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind='terraform',
        evidence={
            'status': 'READY',
            'observed_at': 12,
        },
        expected_generation=revision.desired_generation,
        expected_config_hash=profile.config_hash,
        terraform_hash='f' * 64,
        now=12)
    assert attested.attestations_hash is not None
    for target in (profile.canonical,) + profile.targets:
        attested = topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind=models.profile_attestation_key('terraform_budget', 'aws',
                                                profile.partition,
                                                profile.registry_account,
                                                target.region, 'ecr'),
            evidence={
                'status': 'READY',
                'observed_at': 12,
                'provider': 'aws',
                'partition': profile.partition,
                'account': profile.registry_account,
                'region': target.region,
                'api_family': 'ecr',
                'applied_rate_per_second': 20,
                'burst': 10,
            },
            expected_generation=revision.desired_generation,
            expected_config_hash=profile.config_hash,
            now=12)
    for shard in topology_state.list_shards(workspace, profile.name):
        target = profile.target(shard.target_id)
        live_key = models.profile_attestation_key('infrastructure_shard',
                                                  shard.physical_fingerprint)
        attested = topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind=models.profile_attestation_key('terraform_shard',
                                                shard.physical_fingerprint),
            evidence={
                'status': 'READY',
                'observed_at': 12,
                'physical_fingerprint': shard.physical_fingerprint,
                'target_fingerprint': shard.target_fingerprint,
                'target': shard.target_id,
                'max_manifests': shard.max_manifests,
                'max_declared_bytes': shard.max_declared_bytes,
                'max_in_flight': shard.max_in_flight,
                'terraform_applied_quota': shard.max_manifests + 10,
                'reserved_headroom': 10,
                'live_attestation_key': live_key,
            },
            expected_generation=revision.desired_generation,
            expected_config_hash=profile.config_hash,
            now=12)
        attested = topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind=live_key,
            evidence={
                'status': 'READY',
                'observed_at': 12,
                'physical_fingerprint': shard.physical_fingerprint,
                'target_fingerprint': shard.target_fingerprint,
                'applied_images_per_repository_quota': shard.max_manifests + 10,
                'reserved_headroom': 10,
                'inventory_epoch': shard.inventory_epoch,
                'inventory_completed_at': shard.inventory_completed_at,
            },
            expected_generation=revision.desired_generation,
            expected_config_hash=profile.config_hash,
            now=12)
    for target in (profile.canonical,) + profile.targets:
        for backend, binding_id in target.runtime_pull:
            binding = profile.bindings[binding_id]
            for runtime_id in qualification.runtime_ids(target, backend,
                                                        binding):
                evidence: dict[str, Any] = {
                    'status': 'READY',
                    'observed_at': 12,
                    'target_fingerprint': target.target_fingerprint,
                    'binding_fingerprint': binding.fingerprint,
                    'backend': backend,
                    'platform': profile.qualification.canary_platform,
                    'runtime_id': runtime_id,
                }
                if backend == 'aws_vm':
                    evidence.update(
                        host_image_id=dict(
                            binding.qualified_node_images)[target.region],
                        instance_architecture='x86_64',
                        instance_profile_arn=(
                            models.ec2_instance_profile_arn(binding)),
                        actual_principal=binding.principals[0])
                else:
                    qualified = models.qualified_eks_cluster_for_target(
                        target, binding, runtime_id)
                    evidence.update(context=qualified.context,
                                    cluster_arn=qualified.cluster_arn,
                                    node_role=qualified.node_role,
                                    node_selector=dict(qualified.node_selector),
                                    qualified_node_count=1,
                                    qualified_node_set_hash='d' * 64)
                attested = topology_state.record_profile_attestation(
                    profile_revision_id=revision.id,
                    kind=models.profile_attestation_key('runtime', target.name,
                                                        backend,
                                                        binding.fingerprint,
                                                        runtime_id),
                    evidence=evidence,
                    expected_generation=revision.desired_generation,
                    expected_config_hash=profile.config_hash,
                    now=12)
    assert attested.attestations_hash is not None
    for target in (profile.canonical,) + profile.targets:
        repository_name, repository_arn = (_generated_qualification_repository(
            profile, target))
        attested = topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind=models.profile_attestation_key('terraform_target',
                                                target.name),
            evidence={
                'status': 'READY',
                'observed_at': 12,
                'target_fingerprint': target.target_fingerprint,
                'registry': target.registry,
                'repository_name': repository_name,
                'repository_arn': repository_arn,
                'qualification_repository_generation':
                    target.qualification_repository_generation,
            },
            expected_generation=revision.desired_generation,
            expected_config_hash=profile.config_hash,
            now=12)
    active = transactions.activate_profile(
        profile_revision_id=revision.id,
        expected_generation=revision.desired_generation,
        expected_config_hash=profile.config_hash,
        expected_terraform_hash='f' * 64,
        expected_attestations_hash=attested.attestations_hash,
        required_attestations={'terraform': None},
        now=13)
    shards = topology_state.list_shards(workspace, profile.name)
    expected_eviction = {
        target.name: target.delete_authority is not None
        for target in (profile.canonical,) + profile.targets
    }
    assert all(shard.profile_revision_id == active.id and
               shard.eviction_enabled == expected_eviction[shard.target_id]
               for shard in shards)
    return active


def test_profile_attestation_overwrites_fast_and_slow_producer_clocks(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    with image_database.connect() as connection:
        database_now = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())

    for label, supplied in (('slow', database_now - 86_400),
                            ('fast', database_now + 86_400)):
        evidence = {'status': 'READY', 'observed_at': supplied}
        recorded = topology_state.record_profile_attestation(
            profile_revision_id=active.id,
            kind=f'clock-authority:{label}',
            evidence=evidence,
            expected_generation=active.desired_generation,
            expected_config_hash=active.config_hash)
        with image_database.connect() as connection:
            after = int(
                connection.execute(
                    sqlalchemy.select(catalog_state.database_epoch_expression())
                ).scalar_one())
        observed = recorded.attestations[f'clock-authority:{label}'][
            'observed_at']
        assert database_now <= observed <= after
        assert evidence['observed_at'] == supplied
        database_now = after


def _configure_profile(monkeypatch: pytest.MonkeyPatch,
                       profile: models.ManagedRegistryProfile) -> None:
    policy = models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode.MANAGED_REQUIRED,
        default_profile=profile.name,
        allowed_profiles=(profile.name,),
        publishers=('publisher-1',))
    monkeypatch.setattr(config, 'resolve_profile',
                        lambda distribution, workspace: (profile, policy))
    monkeypatch.setattr(config, 'get_source_binding', lambda _: None)


def _policy_profile(
    profile: models.ManagedRegistryProfile,) -> models.ManagedRegistryProfile:
    write_authority = profile.targets[0].write_authority
    bindings = tuple(
        dataclasses.replace(binding, external_id='candidate-external-id'
                           ) if binding.id == write_authority else binding
        for binding in profile.access_bindings)
    return dataclasses.replace(profile,
                               revision=profile.revision + 1,
                               access_bindings=bindings)


def _workspace_isolated_profile(
    profile: models.ManagedRegistryProfile,
    suffix: str,
) -> models.ManagedRegistryProfile:
    """Uses distinct catalog shards while preserving one shared DB catalog."""
    canonical = dataclasses.replace(
        profile.canonical,
        repository_prefix=f'{profile.canonical.repository_prefix}/{suffix}')
    targets = tuple(
        dataclasses.replace(
            target, repository_prefix=f'{target.repository_prefix}/{suffix}')
        for target in profile.targets)
    return dataclasses.replace(profile, canonical=canonical, targets=targets)


def _stage_candidate_profile(
    profile: models.ManagedRegistryProfile,
    *,
    now: int,
) -> topology_state.ProfileRevisionRecord:
    return topology_state.stage_profile_revision(
        workspace='research',
        profile=profile.name,
        revision=profile.revision,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        physical_manifest_hash=profile.physical_manifest_hash,
        max_daily_canary_microusd=(
            profile.qualification.max_daily_canary_microusd),
        now=now)


def test_qualifying_profile_suppresses_only_its_preceding_active_revision(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    unrelated_profile = _workspace_isolated_profile(profile, 'other-workspace')
    unrelated = _activate_profile(image_database,
                                  unrelated_profile,
                                  workspace='other-workspace')
    candidate = _stage_candidate_profile(_policy_profile(profile), now=20)

    visible = topology_state.list_qualifying_profiles(include_active=True)
    assert [revision.id for revision in visible] == [candidate.id]

    with image_database.begin() as connection:
        changed = connection.execute(schema.profile_revisions.update().where(
            schema.profile_revisions.c.id == candidate.id).values(
                state=models.ImageProfileState.FAILED.value,
                failed_code='QUALIFICATION_FAILED',
                updated_at=21)).rowcount
    assert changed == 1

    resumed_ids = {
        revision.id for revision in topology_state.list_qualifying_profiles(
            include_active=True)
    }
    assert active.id in resumed_ids
    assert unrelated.id in resumed_ids
    assert candidate.id not in resumed_ids


def _request_ec2_canary(
    monkeypatch: pytest.MonkeyPatch,
    profile: models.ManagedRegistryProfile,
    *,
    idempotency_key: str,
    workspace: str = 'research',
) -> catalog_state.OperationRecord:
    _configure_profile(monkeypatch, profile)
    target = profile.targets[0]
    revision = topology_state.get_active_profile(workspace, profile.name)
    assert revision is not None
    repository_arn = _qualification_repository_arn(profile, target)
    revision, _ = qualification.arm_qualification_lifecycle(
        revision, target, repository_arn=repository_arn, runtime_digest=_DIGEST)
    lifecycle_proof_id = qualification.qualification_copy_restoration_proof_id(
        revision, target, _DIGEST)
    assert lifecycle_proof_id is not None
    revision = qualification.record_qualification_copy(
        revision,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='ALREADY_PRESENT',
        expected_lifecycle_proof_id=lifecycle_proof_id)
    assert revision is not None
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    runtime_id = qualification.runtime_ids(target, 'aws_vm',
                                           profile.bindings[binding_id])[0]
    operation, _ = qualification.request_canary(workspace=workspace,
                                                profile_name=profile.name,
                                                target_id=target.name,
                                                backend='aws_vm',
                                                runtime_id=runtime_id,
                                                actor_hash='1' * 64,
                                                idempotency_key=idempotency_key)
    with catalog_state.engine().begin() as connection:
        row = connection.execute(schema.operations.update().where(
            schema.operations.c.id == operation.id).values(
                created_at=1,
                updated_at=1).returning(schema.operations)).mappings().one()
    return catalog_state._operation(row)


def _qualification_repository_arn(profile: models.ManagedRegistryProfile,
                                  target: models.ManagedRegistryTarget) -> str:
    repository_name = f'{target.repository_prefix}/qualification'
    return (f'arn:{profile.partition}:ecr:{target.region}:'
            f'{profile.registry_account}:repository/{repository_name}')


def _generated_qualification_repository(
    profile: models.ManagedRegistryProfile,
    target: models.ManagedRegistryTarget,
) -> tuple[str, str]:
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    repository_name = aws.qualification_repository_name(authority, target)
    repository_arn = (
        f'arn:{profile.partition}:ecr:{target.region}:'
        f'{profile.registry_account}:repository/{repository_name}')
    return repository_name, repository_arn


def _qualification_generation_profile(
        profile: models.ManagedRegistryProfile,
        generation: int) -> models.ManagedRegistryProfile:
    target = dataclasses.replace(profile.targets[0],
                                 qualification_repository_generation=generation)
    return dataclasses.replace(profile, targets=(target,) + profile.targets[1:])


@pytest.mark.parametrize(
    ('target_generation', 'evidence_generation', 'allowed'),
    [
        (0, None, True),
        (0, 1, False),
        (1, None, False),
        (1, 1, True),
        (1, True, False),
    ],
)
def test_qualification_repository_requires_matching_generation(
        image_database, profile: models.ManagedRegistryProfile,
        target_generation: int, evidence_generation: int | bool | None,
        allowed: bool) -> None:
    generated_profile = _qualification_generation_profile(
        profile, target_generation)
    active = _activate_profile(image_database, generated_profile)
    target = generated_profile.targets[0]
    repository_name, repository_arn = _generated_qualification_repository(
        generated_profile, target)
    evidence: dict[str, Any] = {
        'status': 'READY',
        'target_fingerprint': target.target_fingerprint,
        'registry': target.registry,
        'repository_name': repository_name,
        'repository_arn': repository_arn,
    }
    if evidence_generation is not None:
        evidence['qualification_repository_generation'] = evidence_generation
    key = models.profile_attestation_key('terraform_target', target.name)
    revision = dataclasses.replace(active,
                                   attestations={
                                       **active.attestations,
                                       key: evidence,
                                   })

    if allowed:
        assert qualification.qualification_repository(
            revision, target) == (repository_name, repository_arn)
    else:
        with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
            qualification.qualification_repository(revision, target)


def test_qualification_repository_rejects_legacy_loose_authority_path(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    repository_name = (
        f'{target.repository_prefix}/rwrongauthority/qualification/'
        f'{target.region}')
    repository_arn = (
        f'arn:{profile.partition}:ecr:{target.region}:'
        f'{profile.registry_account}:repository/{repository_name}')
    key = models.profile_attestation_key('terraform_target', target.name)
    stale = dataclasses.replace(
        active,
        attestations={
            **active.attestations,
            key: {
                'status': 'READY',
                'target_fingerprint': target.target_fingerprint,
                'registry': target.registry,
                'repository_name': repository_name,
                'repository_arn': repository_arn,
            },
        })

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        qualification.qualification_repository(stale, target)


def test_canary_request_waits_for_exact_copy_restoration(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    revision = _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    copy_key = models.profile_attestation_key('copy', target.name)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    lifecycle_proof_id = '00000000-0000-4000-8000-000000000099'
    revision = topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind=copy_key,
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': _DIGEST,
            'platform': profile.qualification.canary_platform,
        },
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash,
        now=20)
    revision = topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind=lifecycle_key,
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': _DIGEST,
            'exact_absence': True,
            'lifecycle_proof_id': lifecycle_proof_id,
            'protocol_version': 2,
        },
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash,
        now=21)
    lifecycle_observed_at = revision.attestations[lifecycle_key]['observed_at']
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    runtime_id = qualification.runtime_ids(target, 'aws_vm',
                                           profile.bindings[binding_id])[0]

    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        qualification.request_canary(workspace='research',
                                     profile_name=profile.name,
                                     target_id=target.name,
                                     backend='aws_vm',
                                     runtime_id=runtime_id,
                                     actor_hash='1' * 64,
                                     idempotency_key='deleted-copy-canary')
    with image_database.connect() as connection:
        assert not connection.execute(
            sqlalchemy.select(schema.operations.c.id).where(
                schema.operations.c.kind == 'PROFILE_CANARY')).all()

    restored = topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind=copy_key,
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': _DIGEST,
            'platform': profile.qualification.canary_platform,
            'restores_lifecycle_proof_id': lifecycle_proof_id,
        },
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash,
        now=lifecycle_observed_at)

    operation, requested_revision = qualification.request_canary(
        workspace='research',
        profile_name=profile.name,
        target_id=target.name,
        backend='aws_vm',
        runtime_id=runtime_id,
        actor_hash='1' * 64,
        idempotency_key='restored-copy-canary')

    assert qualification.qualification_copy_available(restored, profile, target)
    assert operation.state == models.ImageOperationState.PENDING
    assert requested_revision.id == revision.id


def test_legacy_lifecycle_absence_enters_durable_restoration_barrier(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    profile = _qualification_generation_profile(profile, 1)
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    copy_key = models.profile_attestation_key('copy', target.name)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    legacy = topology_state.record_profile_attestation(
        profile_revision_id=active.id,
        kind=copy_key,
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': _DIGEST,
            'platform': profile.qualification.canary_platform,
        },
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        now=20)
    legacy = topology_state.record_profile_attestation(
        profile_revision_id=active.id,
        kind=lifecycle_key,
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': _DIGEST,
            'exact_absence': True,
        },
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        now=21)

    restoring, proof_id = (
        qualification.begin_qualification_lifecycle_restoration(
            legacy,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            now=22))

    assert proof_id is not None
    lifecycle = restoring.attestations[lifecycle_key]
    assert lifecycle['status'] == 'READY'
    assert lifecycle['protocol_version'] == 2
    assert lifecycle['lifecycle_proof_id'] == proof_id
    assert lifecycle['exact_absence'] is True
    assert not qualification.qualification_copy_available(
        restoring, profile, target)
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'RESTORING'
    assert mutation['owner_profile_revision_id'] == restoring.id
    assert mutation['owner_target'] == target.name
    assert mutation['repository_arn'] == repository_arn
    assert mutation['runtime_digest'] == _DIGEST
    assert mutation['lifecycle_proof_id'] == proof_id

    retry, retry_proof = (
        qualification.begin_qualification_lifecycle_restoration(
            restoring,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            now=22))
    assert retry_proof == proof_id
    assert retry.attestations[lifecycle_key] == lifecycle

    rejected = qualification.record_qualification_copy(
        retry,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=proof_id,
        expected_mutation_proof_id=('00000000-0000-4000-8000-000000000098'),
        now=23)
    assert rejected is None
    assert qualification.get_qualification_mutation() is not None
    unchanged = topology_state.get_profile_revision(restoring.id)
    assert unchanged is not None
    assert unchanged.attestations[copy_key] == legacy.attestations[copy_key]

    restored = qualification.record_qualification_copy(
        retry,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=proof_id,
        expected_mutation_proof_id=proof_id,
        now=24)

    assert restored is not None
    assert qualification.qualification_copy_available(restored, profile, target)
    assert qualification.get_qualification_mutation() is None

    orphan_proof = '00000000-0000-4000-8000-000000000097'
    orphan = topology_state.record_profile_attestation(
        profile_revision_id=restored.id,
        kind=lifecycle_key,
        evidence=qualification.qualification_lifecycle_evidence(
            status='READY',
            target=target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lifecycle_proof_id=orphan_proof,
            exact_absence=True),
        expected_generation=restored.desired_generation,
        expected_config_hash=restored.config_hash,
        now=25)
    adopted, adopted_proof = (
        qualification.begin_qualification_lifecycle_restoration(
            orphan,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            now=26))

    assert adopted_proof == orphan_proof
    assert (qualification.qualification_lifecycle_proof_id(
        adopted.attestations[lifecycle_key]) == orphan_proof)
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'RESTORING'
    assert mutation['lifecycle_proof_id'] == orphan_proof


def test_generation_zero_legacy_lifecycle_absence_is_not_adopted(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    assert target.qualification_repository_generation == 0
    repository_arn = _qualification_repository_arn(profile, target)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    legacy = topology_state.record_profile_attestation(
        profile_revision_id=active.id,
        kind=lifecycle_key,
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': _DIGEST,
            'exact_absence': True,
        },
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        now=20)

    unchanged, proof_id = (
        qualification.begin_qualification_lifecycle_restoration(
            legacy,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            now=21))

    assert proof_id is None
    assert unchanged.attestations[lifecycle_key] == legacy.attestations[
        lifecycle_key]
    assert qualification.get_qualification_mutation() is None


def test_pending_canary_does_not_reserve_cost_after_copy_deletion(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    operation = _request_ec2_canary(
        monkeypatch, profile, idempotency_key='pending-copy-deletion-canary')
    revision = topology_state.get_active_profile('research', profile.name)
    assert revision is not None
    target = profile.targets[0]
    revision = topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind=models.profile_attestation_key('lifecycle', target.name),
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'runtime_digest': _DIGEST,
            'exact_absence': True,
        },
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash)

    assert qualification.claim_canary(worker_id='worker',
                                      lease_seconds=60) is None

    failed = catalog_state.get_operation(operation.id, 'research')
    assert failed is not None
    assert failed.state == models.ImageOperationState.FAILED
    assert failed.error_code == 'QUALIFICATION_FAILED'
    unchanged = topology_state.get_profile_revision(revision.id)
    assert unchanged is not None
    assert unchanged.canary_reserved_microusd == 0


def test_qualification_lifecycle_delete_lease_serializes_takeover_and_completion(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    armed, armed_now = qualification.arm_qualification_lifecycle(
        active,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        now=19)
    assert armed_now
    armed_proof = qualification.qualification_copy_restoration_proof_id(
        armed, target, _DIGEST)
    assert armed_proof is not None
    copied = qualification.record_qualification_copy(
        armed,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=armed_proof,
        now=20)
    assert copied is not None
    assert qualification.qualification_copy_available(copied, profile, target)

    deleting, proof_id, first_token = (
        qualification.begin_qualification_lifecycle_delete(
            copied,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=100))
    assert proof_id is not None
    assert proof_id != armed_proof
    assert first_token is not None
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'DELETING'
    assert mutation['owner_profile_revision_id'] == copied.id
    assert mutation['owner_target'] == target.name
    assert mutation['owner_target_fingerprint'] == target.target_fingerprint
    assert mutation['repository_arn'] == repository_arn
    assert mutation['runtime_digest'] == _DIGEST
    assert mutation['lifecycle_proof_id'] == proof_id
    assert mutation['delete_phase'] == 'PRE_INTENT'
    assert mutation['mutation_lease_token'] == first_token
    assert mutation['mutation_lease_expires_at'] == 160
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    lifecycle = deleting.attestations[lifecycle_key]
    assert lifecycle == {
        'status': 'DELETING',
        'target': target.name,
        'target_fingerprint': target.target_fingerprint,
        'repository_arn': repository_arn,
        'runtime_digest': _DIGEST,
        'lifecycle_proof_id': proof_id,
        'protocol_version': 2,
        'delete_phase': 'PRE_INTENT',
        'mutation_lease_token': first_token,
        'mutation_lease_expires_at': 160,
        'observed_at': 100,
    }
    assert not qualification.qualification_copy_available(
        deleting, profile, target)
    assert qualification.qualification_lifecycle_delete_owned(
        deleting.id,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=first_token,
        now=149)
    assert qualification.heartbeat_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=first_token,
        lease_seconds=60,
        now=150)
    renewed = topology_state.get_profile_revision(deleting.id)
    assert renewed is not None
    renewed_lifecycle = renewed.attestations[lifecycle_key]
    assert renewed_lifecycle['mutation_lease_expires_at'] == 210
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['mutation_lease_token'] == first_token
    assert mutation['mutation_lease_expires_at'] == 210

    unchanged, duplicate_proof, duplicate_token = (
        qualification.begin_qualification_lifecycle_delete(
            renewed,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=209))
    assert duplicate_proof is None
    assert duplicate_token is None
    assert unchanged.attestations[lifecycle_key] == renewed_lifecycle

    taken_over, takeover_proof, takeover_token = (
        qualification.begin_qualification_lifecycle_delete(
            renewed,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=210))
    assert takeover_proof == proof_id
    assert takeover_token is not None and takeover_token != first_token
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'DELETING'
    assert mutation['delete_phase'] == 'PRE_INTENT'
    assert mutation['lifecycle_proof_id'] == proof_id
    assert mutation['mutation_lease_token'] == takeover_token
    assert mutation['mutation_lease_expires_at'] == 270
    assert not qualification.qualification_lifecycle_delete_owned(
        taken_over.id,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=first_token,
        now=211)
    assert qualification.qualification_lifecycle_delete_owned(
        taken_over.id,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=takeover_token,
        now=211)
    assert qualification.begin_qualification_lifecycle_delete_request(
        taken_over,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=takeover_token,
        now=211)
    assert qualification.complete_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=first_token,
        now=211) is None
    assert qualification.mark_qualification_lifecycle_delete_readback(
        taken_over,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=takeover_token,
        now=211)

    completed = qualification.complete_qualification_lifecycle_delete(
        taken_over,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=takeover_token,
        now=211)
    assert completed is not None
    assert completed.attestations[lifecycle_key] == {
        'status': 'READY',
        'target': target.name,
        'target_fingerprint': target.target_fingerprint,
        'repository_arn': repository_arn,
        'runtime_digest': _DIGEST,
        'lifecycle_proof_id': proof_id,
        'protocol_version': 2,
        'exact_absence': True,
        'observed_at': 211,
    }
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'RESTORING'
    assert mutation['delete_phase'] is None
    assert mutation['owner_profile_revision_id'] == completed.id
    assert mutation['lifecycle_proof_id'] == proof_id
    assert mutation['mutation_lease_token'] is None
    assert mutation['mutation_lease_expires_at'] is None
    assert not qualification.qualification_lifecycle_delete_owned(
        completed.id,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=takeover_token,
        now=211)


def _qualification_delete_claim(
    image_database: sqlalchemy.engine.Engine,
    profile: models.ManagedRegistryProfile,
    *,
    now: int = 100,
    lease_seconds: int = 60,
) -> tuple[topology_state.ProfileRevisionRecord, models.ManagedRegistryTarget,
           str, str, str]:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    armed, armed_now = qualification.arm_qualification_lifecycle(
        active,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        now=now - 2)
    assert armed_now
    armed_proof = qualification.qualification_copy_restoration_proof_id(
        armed, target, _DIGEST)
    assert armed_proof is not None
    copied = qualification.record_qualification_copy(
        armed,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=armed_proof,
        now=now - 1)
    assert copied is not None
    deleting, proof_id, token = (
        qualification.begin_qualification_lifecycle_delete(
            copied,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=lease_seconds,
            now=now))
    assert proof_id is not None and token is not None
    return deleting, target, repository_arn, proof_id, token


def test_pre_intent_qualification_delete_defer_shortens_retry_lease(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    deleting, target, repository_arn, proof_id, token = (
        _qualification_delete_claim(image_database, profile))

    assert qualification.defer_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        retry_seconds=3,
        now=110)
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['delete_phase'] == 'PRE_INTENT'
    assert mutation['mutation_lease_token'] != token
    assert mutation['mutation_lease_expires_at'] == 113
    assert not qualification.heartbeat_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        lease_seconds=60,
        now=111)

    _, early_proof, early_token = (
        qualification.begin_qualification_lifecycle_delete(
            deleting,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=112))
    assert early_proof is None and early_token is None

    reclaimed, reclaimed_proof, reclaimed_token = (
        qualification.begin_qualification_lifecycle_delete(
            deleting,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=113))
    assert reclaimed_proof == proof_id
    assert reclaimed_token is not None and reclaimed_token != token
    assert reclaimed.attestations[models.profile_attestation_key(
        'lifecycle', target.name)]['delete_phase'] == 'PRE_INTENT'


@pytest.mark.parametrize('defer_at', [110, 160], ids=('live', 'expired'))
def test_not_started_qualification_delete_rotates_in_flight_to_pre_intent(
        image_database, profile: models.ManagedRegistryProfile,
        defer_at: int) -> None:
    deleting, target, repository_arn, proof_id, in_flight_token = (
        _qualification_delete_claim(image_database, profile))
    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=in_flight_token,
        now=101)

    assert qualification.defer_qualification_lifecycle_delete_not_started(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=in_flight_token,
        retry_seconds=3,
        now=defer_at)

    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'DELETING'
    assert mutation['delete_phase'] == 'PRE_INTENT'
    rotated_token = mutation['mutation_lease_token']
    assert isinstance(rotated_token, str)
    assert rotated_token != in_flight_token
    assert mutation['mutation_lease_expires_at'] == defer_at + 3
    current = topology_state.get_profile_revision(deleting.id)
    assert current is not None
    lifecycle = current.attestations[models.profile_attestation_key(
        'lifecycle', target.name)]
    assert lifecycle['status'] == 'DELETING'
    assert lifecycle['delete_phase'] == 'PRE_INTENT'
    assert lifecycle['mutation_lease_token'] == rotated_token
    assert lifecycle['mutation_lease_expires_at'] == defer_at + 3

    assert not qualification.defer_qualification_lifecycle_delete_not_started(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=in_flight_token,
        retry_seconds=3,
        now=defer_at + 1)
    assert not qualification.defer_qualification_lifecycle_delete_not_started(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=str(uuid.uuid4()),
        retry_seconds=3,
        now=defer_at + 1)


def test_expired_in_flight_qualification_delete_is_quarantined(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    deleting, target, repository_arn, proof_id, token = (
        _qualification_delete_claim(image_database, profile))
    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=101)

    quarantined, takeover_proof, takeover_token = (
        qualification.begin_qualification_lifecycle_delete(
            deleting,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=160))

    assert takeover_proof is None and takeover_token is None
    lifecycle = quarantined.attestations[models.profile_attestation_key(
        'lifecycle', target.name)]
    assert lifecycle['status'] == 'QUARANTINED'
    assert lifecycle['quarantine_reason'] == 'PROVIDER_OUTCOME_AMBIGUOUS'
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'QUARANTINED'
    assert mutation['delete_phase'] is None
    assert mutation['mutation_lease_token'] is None
    assert mutation['mutation_lease_expires_at'] is None
    assert mutation['quarantine_reason'] == 'PROVIDER_OUTCOME_AMBIGUOUS'
    assert not qualification.qualification_lifecycle_delete_owned(
        deleting.id,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        expected_delete_phase=qualification.
        QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
        now=160)
    assert qualification.qualification_copy_barrier_snapshot(
        quarantined,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST) == (False, None)
    with pytest.raises(topology_state.StaleProfileRevisionError):
        qualification.begin_qualification_lifecycle_delete(
            quarantined,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=161)


def test_expired_qualification_readback_takeover_never_rearms_implicitly(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    deleting, target, repository_arn, proof_id, first_token = (
        _qualification_delete_claim(image_database, profile))
    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=first_token,
        now=101)
    assert qualification.mark_qualification_lifecycle_delete_readback(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=first_token,
        now=102)

    recovered, recovered_proof, recovered_token = (
        qualification.begin_qualification_lifecycle_delete(
            deleting,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=160))

    assert recovered_proof == proof_id
    assert recovered_token is not None and recovered_token != first_token
    lifecycle = recovered.attestations[models.profile_attestation_key(
        'lifecycle', target.name)]
    assert lifecycle['delete_phase'] == 'READBACK'
    assert not qualification.begin_qualification_lifecycle_delete_request(
        recovered,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=recovered_token,
        now=161)
    assert qualification.retry_qualification_lifecycle_delete_from_readback(
        recovered,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=recovered_token,
        now=161)
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['delete_phase'] == 'PRE_INTENT'
    assert not qualification.complete_qualification_lifecycle_delete(
        recovered,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=recovered_token,
        now=161)


def test_ambiguous_qualification_delete_quarantines_immediately(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    deleting, target, repository_arn, proof_id, token = (
        _qualification_delete_claim(image_database, profile))
    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=101)

    quarantined = qualification.quarantine_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        reason='DELETE_REQUEST_TIMEOUT',
        now=102)

    assert quarantined is not None
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'QUARANTINED'
    assert mutation['quarantine_reason'] == 'DELETE_REQUEST_TIMEOUT'
    assert topology_state.qualification_repository_quarantined(repository_arn)
    with image_database.connect() as connection:
        tombstone = connection.execute(
            sqlalchemy.select(
                schema.qualification_repository_quarantines).where(
                    schema.qualification_repository_quarantines.c.repository_arn
                    == repository_arn)).mappings().one()
    assert tombstone['owner_profile_revision_id'] == deleting.id
    assert tombstone['owner_target'] == target.name
    assert tombstone['lifecycle_proof_id'] == proof_id
    assert tombstone['quarantine_reason'] == 'DELETE_REQUEST_TIMEOUT'
    assert not qualification.mark_qualification_lifecycle_delete_readback(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=103)


def test_quarantine_cutover_requires_fresh_generation_and_retains_tombstone(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    deleting, target, old_repository_arn, proof_id, token = (
        _qualification_delete_claim(image_database, profile))
    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=old_repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=101)
    quarantined = qualification.quarantine_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=old_repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        reason='DELETE_REQUEST_TIMEOUT',
        now=102)
    assert quarantined is not None
    assert topology_state.qualification_repository_quarantined(
        old_repository_arn)

    same_repository = dataclasses.replace(profile,
                                          revision=profile.revision + 1)
    with pytest.raises(topology_state.QualificationMutationInProgressError):
        _stage_candidate_profile(same_repository, now=103)

    successor_profile = _qualification_generation_profile(
        dataclasses.replace(profile, revision=profile.revision + 1), 1)
    successor_target = successor_profile.target(target.name)
    assert successor_target.target_fingerprint == target.target_fingerprint
    successor = _stage_candidate_profile(successor_profile, now=104)
    with pytest.raises(topology_state.QualificationMutationInProgressError,
                       match='no fresh Terraform repository evidence'):
        topology_state.complete_qualification_quarantine_cutover(
            profile_revision_id=successor.id, now=105)

    successor = topology_state.record_profile_attestation(
        profile_revision_id=successor.id,
        kind='terraform',
        evidence={
            'status': 'READY',
            'observed_at': 106,
        },
        expected_generation=successor.desired_generation,
        expected_config_hash=successor.config_hash,
        terraform_hash='e' * 64,
        now=106)
    with pytest.raises(topology_state.QualificationMutationInProgressError,
                       match='no fresh Terraform repository evidence'):
        topology_state.complete_qualification_quarantine_cutover(
            profile_revision_id=successor.id, now=107)

    repository_name, new_repository_arn = (_generated_qualification_repository(
        successor_profile, successor_target))
    successor = topology_state.record_profile_attestation(
        profile_revision_id=successor.id,
        kind=models.profile_attestation_key('terraform_target', target.name),
        evidence={
            'status': 'READY',
            'observed_at': 108,
            'target_fingerprint': successor_target.target_fingerprint,
            'registry': successor_target.registry,
            'repository_name': repository_name,
            'repository_arn': new_repository_arn,
            'qualification_repository_generation':
                successor_target.qualification_repository_generation,
        },
        expected_generation=successor.desired_generation,
        expected_config_hash=successor.config_hash,
        now=108)
    cutover = topology_state.complete_qualification_quarantine_cutover(
        profile_revision_id=successor.id, now=109)

    assert qualification.get_qualification_mutation() is None
    assert topology_state.qualification_repository_quarantined(
        old_repository_arn)
    assert not topology_state.qualification_repository_quarantined(
        new_repository_arn)
    cutover_evidence = cutover.attestations[models.profile_attestation_key(
        'quarantine_cutover', target.name)]
    assert cutover_evidence['old_repository_arn'] == old_repository_arn
    assert cutover_evidence['new_repository_arn'] == new_repository_arn
    assert cutover_evidence['old_qualification_repository_generation'] == 0
    assert cutover_evidence['new_qualification_repository_generation'] == 1
    with image_database.connect() as connection:
        tombstone_count = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(schema.qualification_repository_quarantines).where(
                schema.qualification_repository_quarantines.c.repository_arn ==
                old_repository_arn)).scalar_one()
    assert tombstone_count == 1

    decreased = _qualification_generation_profile(
        dataclasses.replace(profile, revision=profile.revision + 2), 0)
    with pytest.raises(ValueError,
                       match='repository generations cannot decrease'):
        _stage_candidate_profile(decreased, now=110)

    other_workspace = 'other-workspace'
    other = topology_state.stage_profile_revision(
        workspace=other_workspace,
        profile=profile.name,
        revision=profile.revision,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        physical_manifest_hash=profile.physical_manifest_hash,
        max_daily_canary_microusd=(
            profile.qualification.max_daily_canary_microusd),
        now=111)
    with pytest.raises(topology_state.StaleProfileRevisionError):
        qualification.arm_qualification_lifecycle(
            other,
            target,
            repository_arn=old_repository_arn,
            runtime_digest=_DIGEST,
            now=112)

    other = topology_state.record_profile_attestation(
        profile_revision_id=other.id,
        kind='terraform',
        evidence={
            'status': 'READY',
            'observed_at': 113,
        },
        expected_generation=other.desired_generation,
        expected_config_hash=other.config_hash,
        terraform_hash='d' * 64,
        now=113)
    for configured_target in (profile.canonical,) + profile.targets:
        configured_name, configured_arn = (_generated_qualification_repository(
            profile, configured_target))
        if configured_target.name == target.name:
            configured_arn = old_repository_arn
        other = topology_state.record_profile_attestation(
            profile_revision_id=other.id,
            kind=models.profile_attestation_key('terraform_target',
                                                configured_target.name),
            evidence={
                'status': 'READY',
                'observed_at': 114,
                'target_fingerprint': configured_target.target_fingerprint,
                'registry': configured_target.registry,
                'repository_name': configured_name,
                'repository_arn': configured_arn,
                'qualification_repository_generation':
                    configured_target.qualification_repository_generation,
            },
            expected_generation=other.desired_generation,
            expected_config_hash=other.config_hash,
            now=114)
    assert other.attestations_hash is not None
    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        transactions.activate_profile(
            profile_revision_id=other.id,
            expected_generation=other.desired_generation,
            expected_config_hash=other.config_hash,
            expected_terraform_hash='d' * 64,
            expected_attestations_hash=other.attestations_hash,
            required_attestations={'terraform': None},
            now=115)


def test_staging_allows_removing_nonzero_qualification_generation_target(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    assert image_database is not None
    previous_profile = _qualification_generation_profile(profile, 1)
    previous = _stage_candidate_profile(previous_profile, now=100)
    removed_profile = dataclasses.replace(profile,
                                          revision=profile.revision + 1,
                                          targets=())

    candidate = _stage_candidate_profile(removed_profile, now=101)

    assert candidate.desired_generation == previous.desired_generation + 1
    assert candidate.config_hash == removed_profile.config_hash
    assert candidate.state == models.ImageProfileState.QUALIFYING


def test_staging_allows_generation_decrease_for_new_target_fingerprint(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    assert image_database is not None
    previous_profile = _qualification_generation_profile(profile, 1)
    previous_target = previous_profile.targets[0]
    previous = _stage_candidate_profile(previous_profile, now=100)
    replacement_target = dataclasses.replace(
        profile.targets[0],
        repository_prefix=f'{profile.targets[0].repository_prefix}/replacement',
        qualification_repository_generation=0)
    replacement_profile = dataclasses.replace(profile,
                                              revision=profile.revision + 1,
                                              targets=(replacement_target,))
    assert replacement_target.name == previous_target.name
    assert (replacement_target.target_fingerprint
            != previous_target.target_fingerprint)
    assert (replacement_target.qualification_repository_generation
            < previous_target.qualification_repository_generation)

    candidate = _stage_candidate_profile(replacement_profile, now=101)

    assert candidate.desired_generation == previous.desired_generation + 1
    assert candidate.config_hash == replacement_profile.config_hash
    assert candidate.state == models.ImageProfileState.QUALIFYING


@pytest.mark.parametrize(
    'admission_path',
    (
        'qualification_copy_barrier_snapshot',
        'qualification_copy_provider_allowed',
        'arm_qualification_lifecycle',
        'begin_qualification_lifecycle_restoration',
        'begin_qualification_lifecycle_delete',
        'record_qualification_copy',
    ),
)
def test_catalog_lock_closes_cross_workspace_quarantine_cutover_race(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile, admission_path: str) -> None:
    other = topology_state.stage_profile_revision(
        workspace='other-workspace',
        profile=profile.name,
        revision=profile.revision,
        config_hash=profile.config_hash,
        config_snapshot=profile.to_snapshot(),
        physical_manifest_hash=profile.physical_manifest_hash,
        max_daily_canary_microusd=(
            profile.qualification.max_daily_canary_microusd),
        now=90)
    deleting, target, repository_arn, proof_id, token = (
        _qualification_delete_claim(image_database, profile))
    other = topology_state.record_profile_attestation(
        profile_revision_id=other.id,
        kind=models.profile_attestation_key('terraform_target', target.name),
        evidence={
            'status': 'READY',
            'observed_at': 91,
            'target_fingerprint': target.target_fingerprint,
            'registry': target.registry,
            'repository_name': repository_arn.split('repository/', maxsplit=1)
                               [1],
            'repository_arn': repository_arn,
            'qualification_repository_generation':
                target.qualification_repository_generation,
        },
        expected_generation=other.desired_generation,
        expected_config_hash=other.config_hash,
        now=91)
    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=101)

    admission_paused = threading.Event()
    allow_admission = threading.Event()
    exclusive_attempted = threading.Event()
    exclusive_acquired = threading.Event()
    pause_lock = threading.Lock()
    pause_next_admission = [True]
    cutover_context = threading.local()
    original_available = (
        qualification._qualification_repository_available_in_session)
    original_lock = topology_state.lock_qualification_mutation_in_session

    def _pause_repository_admission(session: orm.Session,
                                    candidate_arn: str) -> bool:
        with pause_lock:
            should_pause = (pause_next_admission[0] and
                            candidate_arn == repository_arn)
            if should_pause:
                pause_next_admission[0] = False
        if should_pause:
            admission_paused.set()
            assert allow_admission.wait(timeout=10)
        return original_available(session, candidate_arn)

    def _observe_catalog_lock(session: orm.Session, *, exclusive: bool) -> None:
        is_cutover = bool(getattr(cutover_context, 'active', False))
        if exclusive and is_cutover:
            exclusive_attempted.set()
        original_lock(session, exclusive=exclusive)
        if exclusive and is_cutover:
            exclusive_acquired.set()

    monkeypatch.setattr(qualification,
                        '_qualification_repository_available_in_session',
                        _pause_repository_admission)
    monkeypatch.setattr(topology_state,
                        'lock_qualification_mutation_in_session',
                        _observe_catalog_lock)

    successor_profile = _qualification_generation_profile(
        dataclasses.replace(profile, revision=profile.revision + 1), 1)
    successor_target = successor_profile.target(target.name)

    def _run_admission(now: int) -> Any:
        if admission_path == 'qualification_copy_barrier_snapshot':
            return qualification.qualification_copy_barrier_snapshot(
                other,
                target,
                repository_arn=repository_arn,
                runtime_digest=_DIGEST)
        if admission_path == 'qualification_copy_provider_allowed':
            return qualification.qualification_copy_provider_allowed(
                other, target, repository_arn=repository_arn)
        if admission_path == 'arm_qualification_lifecycle':
            return qualification.arm_qualification_lifecycle(
                other,
                target,
                repository_arn=repository_arn,
                runtime_digest=_DIGEST,
                now=now)
        if admission_path == 'begin_qualification_lifecycle_restoration':
            return qualification.begin_qualification_lifecycle_restoration(
                other,
                target,
                repository_arn=repository_arn,
                runtime_digest=_DIGEST,
                now=now)
        if admission_path == 'begin_qualification_lifecycle_delete':
            return qualification.begin_qualification_lifecycle_delete(
                other,
                target,
                repository_arn=repository_arn,
                runtime_digest=_DIGEST,
                lease_seconds=60,
                now=now)
        assert admission_path == 'record_qualification_copy'
        return qualification.record_qualification_copy(
            other,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            platform=profile.qualification.canary_platform,
            copy_outcome='COPIED',
            expected_lifecycle_proof_id=None,
            now=now)

    def _quarantine_and_cut_over() -> topology_state.ProfileRevisionRecord:
        cutover_context.active = True
        quarantined = qualification.quarantine_qualification_lifecycle_delete(
            deleting,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lifecycle_proof_id=proof_id,
            mutation_lease_token=token,
            reason='PROVIDER_OUTCOME_AMBIGUOUS',
            now=102)
        assert quarantined is not None
        successor = _stage_candidate_profile(successor_profile, now=103)
        successor = topology_state.record_profile_attestation(
            profile_revision_id=successor.id,
            kind='terraform',
            evidence={
                'status': 'READY',
                'observed_at': 104,
            },
            expected_generation=successor.desired_generation,
            expected_config_hash=successor.config_hash,
            terraform_hash='e' * 64,
            now=104)
        repository_name, successor_arn = (_generated_qualification_repository(
            successor_profile, successor_target))
        topology_state.record_profile_attestation(
            profile_revision_id=successor.id,
            kind=models.profile_attestation_key('terraform_target',
                                                target.name),
            evidence={
                'status': 'READY',
                'observed_at': 105,
                'target_fingerprint': successor_target.target_fingerprint,
                'registry': successor_target.registry,
                'repository_name': repository_name,
                'repository_arn': successor_arn,
                'qualification_repository_generation':
                    successor_target.qualification_repository_generation,
            },
            expected_generation=successor.desired_generation,
            expected_config_hash=successor.config_hash,
            now=105)
        return topology_state.complete_qualification_quarantine_cutover(
            profile_revision_id=successor.id, now=106)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        stale_admission = executor.submit(_run_admission, 107)
        assert admission_paused.wait(timeout=10)
        cutover = executor.submit(_quarantine_and_cut_over)
        assert exclusive_attempted.wait(timeout=10)
        assert not exclusive_acquired.wait(timeout=1)
        allow_admission.set()
        if admission_path == 'arm_qualification_lifecycle':
            with pytest.raises(
                    topology_state.QualificationMutationInProgressError):
                stale_admission.result(timeout=10)
        else:
            stale_result = stale_admission.result(timeout=10)
            if admission_path == 'qualification_copy_barrier_snapshot':
                assert stale_result == (False, None)
            elif admission_path == 'qualification_copy_provider_allowed':
                assert stale_result is False
            elif admission_path == (
                    'begin_qualification_lifecycle_restoration'):
                assert stale_result[1] is None
            elif admission_path == 'begin_qualification_lifecycle_delete':
                assert stale_result[1:] == (None, None)
            else:
                assert admission_path == 'record_qualification_copy'
                assert stale_result is None
        successor = cutover.result(timeout=10)

    assert successor.state == models.ImageProfileState.QUALIFYING
    assert topology_state.qualification_repository_quarantined(repository_arn)
    if admission_path == 'qualification_copy_barrier_snapshot':
        assert _run_admission(108) == (False, None)
    elif admission_path == 'qualification_copy_provider_allowed':
        assert _run_admission(108) is False
    else:
        with pytest.raises(topology_state.StaleProfileRevisionError,
                           match='repository is quarantined'):
            _run_admission(108)


def test_tombstoned_active_history_does_not_starve_fresh_candidate(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    target = profile.targets[0]
    repository_arn = (f'arn:aws:ecr:{target.region}:{profile.registry_account}:'
                      'repository/shared-tombstoned-qualification')
    attestations = json.dumps(
        {
            models.profile_attestation_key('terraform_target', target.name): {
                'status': 'READY',
                'repository_arn': repository_arn,
            },
        },
        sort_keys=True,
        separators=(',', ':'))
    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_profile_revisions (
                    id, workspace, profile, revision, desired_generation,
                    state, config_hash, config_json, physical_manifest_hash,
                    attestations_json, canary_reserved_microusd,
                    max_daily_canary_microusd, created_at, updated_at
                )
                SELECT md5('tombstoned-active-' || series::text)::uuid::text,
                       'tombstoned-workspace-' || series::text,
                       :profile,
                       1,
                       1,
                       'ACTIVE',
                       :config_hash,
                       '{}',
                       :physical_manifest_hash,
                       :attestations,
                       0,
                       0,
                       series,
                       series
                FROM generate_series(1, 100000) AS series
            """), {
                'profile': profile.name,
                'config_hash': 'a' * 64,
                'physical_manifest_hash': 'b' * 64,
                'attestations': attestations,
            })
        owner_id = connection.execute(
            sqlalchemy.select(schema.profile_revisions.c.id).where(
                schema.profile_revisions.c.workspace ==
                'tombstoned-workspace-1')).scalar_one()
        connection.execute(
            schema.qualification_repository_quarantines.insert().values(
                repository_arn=repository_arn,
                owner_profile_revision_id=owner_id,
                owner_target=target.name,
                owner_target_fingerprint=target.target_fingerprint,
                runtime_digest=_DIGEST,
                lifecycle_proof_id=str(uuid.uuid4()),
                quarantine_reason='PROVIDER_OUTCOME_AMBIGUOUS',
                quarantined_at=1))
        connection.exec_driver_sql('ANALYZE container_image_profile_revisions')

    candidate = _stage_candidate_profile(profile, now=100001)
    captured: list[tuple[str, Any]] = []

    def _capture_statement(_connection, _cursor, statement, parameters,
                           _context, _executemany) -> None:
        if ('SELECT' in statement and
                'container_image_profile_revisions' in statement):
            captured.append((statement, parameters))

    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            _capture_statement)
    try:
        visible = topology_state.list_qualifying_profiles(include_active=True,
                                                          limit=8)
    finally:
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                _capture_statement)

    assert [revision.id for revision in visible] == [candidate.id]
    assert len(captured) == 1
    statement, parameters = captured[0]
    with image_database.connect() as connection:
        plan = connection.exec_driver_sql(
            f'EXPLAIN (ANALYZE, FORMAT JSON) {statement}',
            parameters).scalar_one()[0]['Plan']

    def _plan_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield node
        for child in node.get('Plans', []):
            yield from _plan_nodes(child)

    nodes = list(_plan_nodes(plan))
    state_index = next(
        node for node in nodes
        if node.get('Index Name') == 'ix_container_image_profile_state')
    assert "state = 'QUALIFYING'" in state_index['Index Cond']
    assert all(
        node.get('Index Name') !=
        'ix_container_image_profile_qualification_queue' for node in nodes)


@pytest.mark.parametrize('invalid', ({
    'state': 'DELETING',
    'delete_phase': None,
    'mutation_lease_token': 'token',
    'mutation_lease_expires_at': 200,
    'quarantine_reason': None,
}, {
    'state': 'RESTORING',
    'delete_phase': 'READBACK',
    'mutation_lease_token': None,
    'mutation_lease_expires_at': None,
    'quarantine_reason': None,
}, {
    'state': 'QUARANTINED',
    'delete_phase': None,
    'mutation_lease_token': None,
    'mutation_lease_expires_at': None,
    'quarantine_reason': None,
}, {
    'state': 'QUARANTINED',
    'delete_phase': None,
    'mutation_lease_token': 'token',
    'mutation_lease_expires_at': 200,
    'quarantine_reason': 'PROVIDER_OUTCOME_AMBIGUOUS',
}))
def test_qualification_mutation_phase_constraints(
        image_database, profile: models.ManagedRegistryProfile,
        invalid: dict[str, object]) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    values = {
        'id': 'global',
        'owner_profile_revision_id': active.id,
        'owner_target': target.name,
        'owner_target_fingerprint': target.target_fingerprint,
        'repository_arn': _qualification_repository_arn(profile, target),
        'runtime_digest': _DIGEST,
        'lifecycle_proof_id': '00000000-0000-4000-8000-000000000109',
        'updated_at': 100,
        **invalid,
    }

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with image_database.begin() as connection:
            connection.execute(
                schema.qualification_mutation.insert().values(**values))


def test_qualification_lifecycle_samples_clock_after_catalog_mutation_lock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    armed, armed_now = qualification.arm_qualification_lifecycle(
        active,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        now=19)
    assert armed_now
    armed_proof = qualification.qualification_copy_restoration_proof_id(
        armed, target, _DIGEST)
    assert armed_proof is not None
    copied = qualification.record_qualification_copy(
        armed,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=armed_proof,
        now=20)
    assert copied is not None

    events: list[str] = []
    original_lock = topology_state.get_qualification_mutation_in_session
    original_epoch = catalog_state.database_epoch

    def observed_lock(session: orm.Session, *,
                      exclusive: bool) -> sqlalchemy.engine.RowMapping | None:
        mutation = original_lock(session, exclusive=exclusive)
        if exclusive:
            events.append('exclusive-lock-acquired')
        return mutation

    def observed_epoch(session: orm.Session, *, now: int | None = None) -> int:
        events.append('clock-sampled')
        return original_epoch(session, now=now)

    monkeypatch.setattr(topology_state, 'get_qualification_mutation_in_session',
                        observed_lock)
    monkeypatch.setattr(catalog_state, 'database_epoch', observed_epoch)

    deleting, proof_id, token = (
        qualification.begin_qualification_lifecycle_delete(
            copied,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=60,
            now=100))
    assert proof_id is not None and token is not None
    assert events[0] == 'exclusive-lock-acquired'
    assert set(events[1:]) == {'clock-sampled'}

    events.clear()
    assert qualification.qualification_lifecycle_delete_owned(
        deleting.id,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=101)
    assert events[0] == 'exclusive-lock-acquired'
    assert set(events[1:]) == {'clock-sampled'}

    events.clear()
    assert qualification.heartbeat_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        lease_seconds=60,
        now=102)
    assert events[0] == 'exclusive-lock-acquired'
    assert set(events[1:]) == {'clock-sampled'}

    events.clear()
    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=103)
    assert events[0] == 'exclusive-lock-acquired'
    assert set(events[1:]) == {'clock-sampled'}

    events.clear()
    assert qualification.mark_qualification_lifecycle_delete_readback(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=103)
    assert events[0] == 'exclusive-lock-acquired'
    assert set(events[1:]) == {'clock-sampled'}

    events.clear()
    assert qualification.complete_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=token,
        now=103) is not None
    assert events[0] == 'exclusive-lock-acquired'
    assert set(events[1:]) == {'clock-sampled'}


def test_global_qualification_mutation_defers_cross_workspace_canary_until_owner_restores(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    owner = _activate_profile(image_database, profile, workspace='research')
    follower_profile = _workspace_isolated_profile(profile, 'biology')
    _activate_profile(image_database, follower_profile, workspace='biology')
    operation = _request_ec2_canary(
        monkeypatch,
        follower_profile,
        workspace='biology',
        idempotency_key='cross-workspace-mutation-barrier-canary')
    follower = topology_state.get_active_profile('biology',
                                                 follower_profile.name)
    assert follower is not None
    follower_target = follower_profile.targets[0]
    follower_repository_arn = _qualification_repository_arn(
        follower_profile, follower_target)
    follower_proof = qualification.qualification_copy_restoration_proof_id(
        follower, follower_target, _DIGEST)
    assert follower_proof is not None

    owner_target = profile.targets[0]
    owner_repository_arn = _qualification_repository_arn(profile, owner_target)
    armed, armed_now = qualification.arm_qualification_lifecycle(
        owner,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        now=20)
    assert armed_now
    armed_proof = qualification.qualification_copy_restoration_proof_id(
        armed, owner_target, _DIGEST)
    assert armed_proof is not None
    copied = qualification.record_qualification_copy(
        armed,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=armed_proof,
        now=21)
    assert copied is not None

    deleting, mutation_proof, mutation_token = (
        qualification.begin_qualification_lifecycle_delete(
            copied,
            owner_target,
            repository_arn=owner_repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=300,
            now=100))
    assert mutation_proof is not None
    assert mutation_token is not None
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'DELETING'
    assert mutation['delete_phase'] == 'PRE_INTENT'
    assert mutation['owner_profile_revision_id'] == owner.id

    assert qualification.claim_canary(worker_id='worker-deleting',
                                      lease_seconds=60,
                                      now=101) is None
    pending = catalog_state.get_operation(operation.id, 'biology')
    assert pending is not None
    assert pending.state == models.ImageOperationState.PENDING
    unchanged_follower = topology_state.get_profile_revision(follower.id)
    assert unchanged_follower is not None
    assert unchanged_follower.canary_reserved_microusd == 0

    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=mutation_proof,
        mutation_lease_token=mutation_token,
        now=102)
    assert qualification.mark_qualification_lifecycle_delete_readback(
        deleting,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=mutation_proof,
        mutation_lease_token=mutation_token,
        now=102)
    restoring = qualification.complete_qualification_lifecycle_delete(
        deleting,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=mutation_proof,
        mutation_lease_token=mutation_token,
        now=102)
    assert restoring is not None
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'RESTORING'
    assert mutation['mutation_lease_token'] is None
    assert mutation['mutation_lease_expires_at'] is None

    assert qualification.claim_canary(worker_id='worker-restoring',
                                      lease_seconds=60,
                                      now=103) is None
    pending = catalog_state.get_operation(operation.id, 'biology')
    assert pending is not None
    assert pending.state == models.ImageOperationState.PENDING
    unchanged_follower = topology_state.get_profile_revision(follower.id)
    assert unchanged_follower is not None
    assert unchanged_follower.canary_reserved_microusd == 0
    assert qualification.qualification_copy_barrier_snapshot(
        follower,
        follower_target,
        repository_arn=follower_repository_arn,
        runtime_digest=_DIGEST) == (False, None)

    assert qualification.record_qualification_copy(
        follower,
        follower_target,
        repository_arn=follower_repository_arn,
        runtime_digest=_DIGEST,
        platform=follower_profile.qualification.canary_platform,
        copy_outcome='ALREADY_PRESENT',
        expected_lifecycle_proof_id=follower_proof,
        expected_mutation_proof_id=mutation_proof,
        now=104) is None
    assert qualification.record_qualification_copy(
        restoring,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='ALREADY_PRESENT',
        expected_lifecycle_proof_id=mutation_proof,
        expected_mutation_proof_id=str(uuid.uuid4()),
        now=104) is None
    assert qualification.record_qualification_copy(
        restoring,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='ALREADY_PRESENT',
        expected_lifecycle_proof_id=mutation_proof,
        now=104) is None
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None and mutation['state'] == 'RESTORING'

    restored = qualification.record_qualification_copy(
        restoring,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=mutation_proof,
        expected_mutation_proof_id=mutation_proof,
        now=104)
    assert restored is not None
    assert qualification.get_qualification_mutation() is None
    assert qualification.qualification_copy_available(restored, profile,
                                                      owner_target)
    assert qualification.record_qualification_copy(
        restored,
        owner_target,
        repository_arn=owner_repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='ALREADY_PRESENT',
        expected_lifecycle_proof_id=mutation_proof,
        expected_mutation_proof_id=mutation_proof,
        now=104) is None

    claimed = qualification.claim_canary(worker_id='worker-after-restore',
                                         lease_seconds=60,
                                         now=105)
    assert claimed is not None
    assert claimed.id == operation.id
    assert claimed.state == models.ImageOperationState.RUNNING
    assert qualification.claim_canary(worker_id='duplicate-worker',
                                      lease_seconds=60,
                                      now=105) is None
    charged_follower = topology_state.get_profile_revision(follower.id)
    assert charged_follower is not None
    assert charged_follower.canary_reserved_microusd == (
        follower_profile.qualification.canary_worst_case_microusd)


def test_mutation_owner_can_queue_canaries_until_exact_restoration(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    owner = _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    armed, armed_now = qualification.arm_qualification_lifecycle(
        owner,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        now=20)
    assert armed_now
    armed_proof = qualification.qualification_copy_restoration_proof_id(
        armed, target, _DIGEST)
    assert armed_proof is not None
    copied = qualification.record_qualification_copy(
        armed,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=armed_proof,
        now=21)
    assert copied is not None
    deleting, proof_id, lease_token = (
        qualification.begin_qualification_lifecycle_delete(
            copied,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=300,
            now=100))
    assert proof_id is not None
    assert lease_token is not None

    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    runtime_id = qualification.runtime_ids(target, 'aws_vm',
                                           profile.bindings[binding_id])[0]

    def request(key: str, timestamp: int) -> catalog_state.OperationRecord:
        operation, _ = qualification.request_canary(workspace='research',
                                                    profile_name=profile.name,
                                                    target_id=target.name,
                                                    backend='aws_vm',
                                                    runtime_id=runtime_id,
                                                    actor_hash='1' * 64,
                                                    idempotency_key=key)
        with image_database.begin() as connection:
            row = connection.execute(schema.operations.update().where(
                schema.operations.c.id == operation.id).values(
                    created_at=timestamp, updated_at=timestamp).returning(
                        schema.operations)).mappings().one()
        return catalog_state._operation(  # pylint: disable=protected-access
            row)

    deleting_operation = request('owner-deleting-canary', 1)
    assert qualification.claim_canary(worker_id='worker-deleting',
                                      lease_seconds=60,
                                      now=101) is None
    pending_deleting = catalog_state.get_operation(deleting_operation.id,
                                                   'research')
    assert pending_deleting is not None
    assert pending_deleting.state == models.ImageOperationState.PENDING

    assert qualification.begin_qualification_lifecycle_delete_request(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=lease_token,
        now=102)
    assert qualification.mark_qualification_lifecycle_delete_readback(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=lease_token,
        now=102)
    restoring = qualification.complete_qualification_lifecycle_delete(
        deleting,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        lifecycle_proof_id=proof_id,
        mutation_lease_token=lease_token,
        now=102)
    assert restoring is not None
    restoring_operation = request('owner-restoring-canary', 2)
    assert qualification.claim_canary(worker_id='worker-restoring',
                                      lease_seconds=60,
                                      now=103) is None
    pending_restoring = catalog_state.get_operation(restoring_operation.id,
                                                    'research')
    assert pending_restoring is not None
    assert pending_restoring.state == models.ImageOperationState.PENDING
    unchanged_owner = topology_state.get_profile_revision(owner.id)
    assert unchanged_owner is not None
    assert unchanged_owner.canary_reserved_microusd == 0

    restored = qualification.record_qualification_copy(
        restoring,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=proof_id,
        expected_mutation_proof_id=proof_id,
        now=104)
    assert restored is not None
    claimed = qualification.claim_canary(worker_id='worker-after-restore',
                                         lease_seconds=60,
                                         now=105)
    assert claimed is not None
    assert claimed.id == deleting_operation.id
    still_pending = catalog_state.get_operation(restoring_operation.id,
                                                'research')
    assert still_pending is not None
    assert still_pending.state == models.ImageOperationState.PENDING


def test_global_running_canary_prevents_other_workspace_lifecycle_delete(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    owner = _activate_profile(image_database, profile, workspace='research')
    running_profile = _workspace_isolated_profile(profile, 'biology')
    _activate_profile(image_database, running_profile, workspace='biology')
    running_operation = _request_ec2_canary(
        monkeypatch,
        running_profile,
        workspace='biology',
        idempotency_key='cross-workspace-running-canary')
    running = qualification.claim_canary(worker_id='worker',
                                         lease_seconds=300,
                                         now=100)
    assert running is not None
    assert running.id == running_operation.id
    with image_database.begin() as connection:
        connection.execute(schema.operations.update().where(
            schema.operations.c.id == running.id).values(result_kind=None,
                                                         result_id=None,
                                                         result_json=None))

    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    armed, armed_now = qualification.arm_qualification_lifecycle(
        owner,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        now=101)
    assert armed_now
    armed_proof = qualification.qualification_copy_restoration_proof_id(
        armed, target, _DIGEST)
    assert armed_proof is not None
    copied = qualification.record_qualification_copy(
        armed,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=armed_proof,
        now=102)
    assert copied is not None

    unchanged, mutation_proof, mutation_token = (
        qualification.begin_qualification_lifecycle_delete(
            copied,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=300,
            now=103))
    assert mutation_proof is None
    assert mutation_token is None
    assert qualification.get_qualification_mutation() is None
    assert qualification.qualification_copy_available(unchanged, profile,
                                                      target)
    lifecycle_proof_id = '00000000-0000-4000-8000-000000000096'
    deleted = topology_state.record_profile_attestation(
        profile_revision_id=unchanged.id,
        kind=models.profile_attestation_key('lifecycle', target.name),
        evidence=qualification.qualification_lifecycle_evidence(
            status='READY',
            target=target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lifecycle_proof_id=lifecycle_proof_id,
            exact_absence=True),
        expected_generation=unchanged.desired_generation,
        expected_config_hash=unchanged.config_hash,
        now=104)
    restoring, restoration_proof = (
        qualification.begin_qualification_lifecycle_restoration(
            deleted,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            now=105))
    assert restoration_proof == lifecycle_proof_id
    mutation = qualification.get_qualification_mutation()
    assert mutation is not None
    assert mutation['state'] == 'RESTORING'
    assert (qualification.qualification_lifecycle_proof_id(
        restoring.attestations[models.profile_attestation_key(
            'lifecycle', target.name)]) == restoration_proof)
    still_running = catalog_state.get_operation(running.id, 'biology')
    assert still_running is not None
    assert still_running.state == models.ImageOperationState.RUNNING
    assert still_running.result_kind is None
    assert still_running.result_id is None
    charged = topology_state.get_active_profile('biology', running_profile.name)
    assert charged is not None
    assert charged.canary_reserved_microusd == (
        running_profile.qualification.canary_worst_case_microusd)


def test_qualification_copy_acknowledgment_cas_rejects_changed_lifecycle_proof(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    copy_key = models.profile_attestation_key('copy', target.name)
    first_proof = '00000000-0000-4000-8000-000000000101'
    second_proof = '00000000-0000-4000-8000-000000000102'
    first_epoch = topology_state.record_profile_attestation(
        profile_revision_id=active.id,
        kind=lifecycle_key,
        evidence=qualification.qualification_lifecycle_evidence(
            status='READY',
            target=target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lifecycle_proof_id=first_proof,
            exact_absence=True),
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        now=20)
    assert qualification.qualification_copy_restoration_proof_id(
        first_epoch, target, _DIGEST) == first_proof

    second_epoch = topology_state.record_profile_attestation(
        profile_revision_id=active.id,
        kind=lifecycle_key,
        evidence=qualification.qualification_lifecycle_evidence(
            status='READY',
            target=target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lifecycle_proof_id=second_proof,
            exact_absence=True),
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        now=21)
    assert qualification.record_qualification_copy(
        first_epoch,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='ALREADY_PRESENT',
        expected_lifecycle_proof_id=first_proof,
        now=22) is None
    unchanged = topology_state.get_profile_revision(active.id)
    assert unchanged is not None
    assert copy_key not in unchanged.attestations
    assert unchanged.attestations[lifecycle_key][
        'lifecycle_proof_id'] == second_proof

    restored = qualification.record_qualification_copy(
        second_epoch,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='ALREADY_PRESENT',
        expected_lifecycle_proof_id=second_proof,
        now=23)
    assert restored is not None
    assert restored.attestations[copy_key][
        'restores_lifecycle_proof_id'] == second_proof
    assert qualification.qualification_copy_available(restored, profile, target)


def test_canary_claim_and_cost_recheck_copy_after_lifecycle_profile_lock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    operation = _request_ec2_canary(
        monkeypatch,
        profile,
        idempotency_key='canary-copy-admission-profile-lock-key')
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    profile_select_attempted = threading.Event()

    def observe_claim_profile_lock(_connection, _cursor, statement, _parameters,
                                   _context, _executemany) -> None:
        normalized = ' '.join(statement.split()).upper()
        if (threading.current_thread().name.startswith('canary-claim-race') and
                'PG_ADVISORY_XACT_LOCK' in normalized):
            profile_select_attempted.set()

    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            observe_claim_profile_lock)
    lock_session = orm.Session(image_database)
    lock_transaction = lock_session.begin()
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='canary-claim-race')
    try:
        topology_state.lock_profile_revision_mutation_in_session(
            lock_session, active.id)
        database_now = catalog_state.database_epoch(lock_session)
        future = executor.submit(qualification.claim_canary,
                                 worker_id='worker',
                                 lease_seconds=60)
        assert profile_select_attempted.wait(timeout=5)
        assert not future.done()
        topology_state.record_profile_attestation_in_session(
            lock_session,
            profile_revision_id=active.id,
            kind=lifecycle_key,
            evidence=qualification.qualification_lifecycle_evidence(
                status='DELETING',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=_DIGEST,
                lifecycle_proof_id=('00000000-0000-4000-8000-000000000103'),
                delete_phase='PRE_INTENT',
                mutation_lease_token=('00000000-0000-4000-8000-000000000104'),
                mutation_lease_expires_at=database_now + 300),
            expected_generation=active.desired_generation,
            expected_config_hash=active.config_hash,
            now=database_now)
        lock_transaction.commit()
        assert future.result(timeout=10) is None
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_session.close()
        executor.shutdown(wait=True)
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                observe_claim_profile_lock)

    failed = catalog_state.get_operation(operation.id, 'research')
    assert failed is not None
    assert failed.state == models.ImageOperationState.FAILED
    assert failed.error_code == 'QUALIFICATION_FAILED'
    revision = topology_state.get_profile_revision(active.id)
    assert revision is not None
    assert revision.attestations[lifecycle_key]['status'] == 'DELETING'
    assert revision.canary_reserved_microusd == 0


def test_authorize_canary_launch_rejects_lifecycle_unavailability(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-launch-lifecycle-fence-key')
    claimed = qualification.claim_canary(worker_id='worker',
                                         lease_seconds=2000,
                                         now=300)
    assert claimed is not None and claimed.lease_token is not None
    assert claimed.teardown_deadline is not None
    target = profile.targets[0]
    child_id = f'ec2:{target.region}:{claimed.id}'
    assert qualification.attach_canary_child(claimed.id,
                                             claimed.lease_token,
                                             child_id,
                                             now=301)
    assert qualification.authorize_canary_launch(
        claimed.id, claimed.lease_token, child_id,
        now=301) == claimed.teardown_deadline - 301

    repository_arn = _qualification_repository_arn(profile, target)
    current = topology_state.get_profile_revision(active.id)
    assert current is not None
    unchanged, blocked_proof, blocked_token = (
        qualification.begin_qualification_lifecycle_delete(
            current,
            target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lease_seconds=300,
            now=302))
    assert blocked_proof is None
    assert blocked_token is None
    assert qualification.qualification_copy_available(unchanged, profile,
                                                      target)

    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    unavailable = topology_state.record_profile_attestation(
        profile_revision_id=active.id,
        kind=lifecycle_key,
        evidence=qualification.qualification_lifecycle_evidence(
            status='DELETING',
            target=target,
            repository_arn=repository_arn,
            runtime_digest=_DIGEST,
            lifecycle_proof_id='00000000-0000-4000-8000-000000000105',
            delete_phase='PRE_INTENT',
            mutation_lease_token='00000000-0000-4000-8000-000000000106',
            mutation_lease_expires_at=600),
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        now=302)
    assert not qualification.qualification_copy_available(
        unavailable, profile, target)
    assert qualification.authorize_canary_launch(claimed.id,
                                                 claimed.lease_token,
                                                 child_id,
                                                 now=302) is None

    cleanup_owner = catalog_state.get_operation(claimed.id, 'research')
    assert cleanup_owner is not None
    assert cleanup_owner.state == models.ImageOperationState.RUNNING
    assert cleanup_owner.child_launch_id == child_id
    revision = topology_state.get_profile_revision(active.id)
    assert revision is not None
    assert revision.canary_reserved_microusd == (
        profile.qualification.canary_worst_case_microusd)


def test_expired_canary_owner_cannot_attach_or_terminalize_successor_work(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-expired-owner-key')
    first = qualification.claim_canary(worker_id='worker-a',
                                       lease_seconds=10,
                                       now=100)
    assert first is not None and first.lease_token is not None
    child_id = f'ec2:{profile.targets[0].region}:{first.id}'
    assert qualification.attach_canary_child(first.id,
                                             first.lease_token,
                                             child_id,
                                             now=109)
    assert not qualification.attach_canary_child(
        first.id, first.lease_token, child_id, now=110)
    assert not qualification.complete_canary(first, {'teardown_verified': True},
                                             now=110)
    assert not qualification.fail_canary(first, 'CANARY_FAILED', now=110)

    successor = qualification.claim_canary(worker_id='worker-b',
                                           lease_seconds=10,
                                           now=110)
    assert successor is not None and successor.lease_token is not None
    assert successor.lease_token != first.lease_token
    assert qualification.attach_canary_child(successor.id,
                                             successor.lease_token,
                                             child_id,
                                             now=111)
    assert not qualification.fail_canary(first, 'CANARY_FAILED', now=111)
    with pytest.raises(ValueError, match='must remain reclaimable'):
        qualification.fail_canary(successor, 'CANARY_TEARDOWN_FAILED', now=111)
    assert qualification.fail_canary(successor, 'CANARY_FAILED', now=111)


def test_canary_ec2_profile_evidence_is_immutable_and_lease_fenced(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-profile-evidence-key')
    first = qualification.claim_canary(worker_id='worker-a',
                                       lease_seconds=10,
                                       now=100)
    assert first is not None and first.lease_token is not None
    target = profile.targets[0]
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    profile_arn = models.ec2_instance_profile_arn(profile.bindings[binding_id])
    child_id = f'ec2:{target.region}:{first.id}'
    assert qualification.attach_canary_child(first.id,
                                             first.lease_token,
                                             child_id,
                                             now=101)
    assert qualification.record_canary_ec2_instance_profile(first.id,
                                                            first.lease_token,
                                                            child_id,
                                                            profile_arn,
                                                            now=102)
    assert qualification.record_canary_ec2_instance_profile(first.id,
                                                            first.lease_token,
                                                            child_id,
                                                            profile_arn,
                                                            now=103)
    with pytest.raises(ValueError, match='immutable'):
        qualification.record_canary_ec2_instance_profile(
            first.id,
            first.lease_token,
            child_id,
            'arn:aws:iam::123456789012:instance-profile/other',
            now=104)

    observed = catalog_state.get_operation(first.id, 'research')
    assert observed is not None
    assert qualification.canary_ec2_instance_profile_arn(
        observed) == profile_arn
    assert not qualification.record_canary_ec2_instance_profile(
        first.id, 'stale-token', child_id, profile_arn, now=105)

    successor = qualification.claim_canary(worker_id='worker-b',
                                           lease_seconds=10,
                                           now=110)
    assert successor is not None and successor.lease_token is not None
    assert successor.lease_token != first.lease_token
    assert successor.child_launch_id == child_id
    assert qualification.canary_ec2_instance_profile_arn(
        successor) == profile_arn
    assert not qualification.record_canary_ec2_instance_profile(
        first.id, first.lease_token, child_id, profile_arn, now=111)
    assert qualification.fail_canary(successor, 'CANARY_FAILED', now=111)

    terminal = catalog_state.get_operation(first.id, 'research')
    assert terminal is not None
    assert terminal.canary_child_evidence is None


def test_drained_canary_release_is_immediately_reclaimable_and_fenced(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-drained-release-key')
    first = qualification.claim_canary(worker_id='worker-a',
                                       lease_seconds=100,
                                       now=100)
    assert first is not None and first.lease_token is not None
    child_id = f'ec2:{profile.targets[0].region}:{first.id}'
    assert qualification.attach_canary_child(first.id,
                                             first.lease_token,
                                             child_id,
                                             now=101)

    with pytest.raises(ValueError, match='teardown must be verified'):
        qualification.release_drained_canary(first,
                                             teardown_verified=False,
                                             now=102)
    assert qualification.release_drained_canary(first,
                                                teardown_verified=True,
                                                now=102)
    assert not qualification.heartbeat_canary(
        first.id, first.lease_token, lease_seconds=100, now=102)
    successor = qualification.claim_canary(worker_id='worker-b',
                                           lease_seconds=100,
                                           now=102)
    assert successor is not None and successor.lease_token is not None
    assert successor.lease_token != first.lease_token
    assert successor.child_launch_id == child_id
    assert not qualification.release_drained_canary(
        first, teardown_verified=True, now=103)


def test_incompatible_worker_preserves_persisted_canary_child(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    operation = _request_ec2_canary(
        monkeypatch, profile, idempotency_key='canary-future-contract-key')
    claimed = qualification.claim_canary(worker_id='worker-a',
                                         lease_seconds=10,
                                         now=100)
    assert claimed is not None and claimed.lease_token is not None
    child_id = f'ec2:{profile.targets[0].region}:{claimed.id}'
    assert qualification.attach_canary_child(claimed.id,
                                             claimed.lease_token,
                                             child_id,
                                             now=101)
    with image_database.begin() as connection:
        connection.execute(schema.operations.update().where(
            schema.operations.c.id == operation.id).values(
                result_json=json.dumps({'future_contract': 2})))

    assert qualification.claim_canary(worker_id='worker-b',
                                      lease_seconds=10,
                                      now=110) is None

    preserved = catalog_state.get_operation(operation.id, 'research')
    assert preserved is not None
    assert preserved.state == models.ImageOperationState.RUNNING
    assert preserved.child_launch_id == child_id
    assert preserved.lease_token == claimed.lease_token
    assert preserved.updated_at == 110


def test_canary_terminal_fence_rechecks_database_clock_after_blocking_lock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-database-clock-key')
    claimed = qualification.claim_canary(worker_id='worker-a', lease_seconds=1)
    assert claimed is not None and claimed.lease_token is not None
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.select(schema.operations.c.id).where(
            schema.operations.c.id == claimed.id).with_for_update()).one()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(qualification.fail_canary, claimed,
                                     'CANARY_FAILED')
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(1.2)')
            lock_transaction.commit()
            assert not future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    unchanged = catalog_state.get_operation(claimed.id, 'research')
    assert unchanged is not None
    assert unchanged.state == models.ImageOperationState.RUNNING
    assert unchanged.lease_token == claimed.lease_token


def test_canary_worker_failure_does_not_reuse_prelock_application_time(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-worker-clock-key')
    claimed = qualification.claim_canary(worker_id='worker-a', lease_seconds=1)
    assert claimed is not None and claimed.lease_token is not None
    heartbeat = types.SimpleNamespace(assert_owned=lambda: None)
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.select(schema.operations.c.id).where(
            schema.operations.c.id == claimed.id).with_for_update()).one()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(canary_worker_service._fail_owned_canary,
                                     claimed, 'CANARY_FAILED', heartbeat)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(1.2)')
            lock_transaction.commit()
            future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    unchanged = catalog_state.get_operation(claimed.id, 'research')
    assert unchanged is not None
    assert unchanged.state == models.ImageOperationState.RUNNING
    assert unchanged.lease_token == claimed.lease_token


def test_canary_claim_samples_database_clock_after_profile_lock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-fresh-claim-clock-key')
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    started = int(
        lock_connection.execute(
            sqlalchemy.select(
                catalog_state.database_epoch_expression())).scalar_one())
    lock_connection.execute(
        sqlalchemy.select(schema.profile_revisions.c.id).where(
            schema.profile_revisions.c.id ==
            active.id).with_for_update()).one()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(qualification.claim_canary,
                                     worker_id='worker-a',
                                     lease_seconds=1)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(2.1)')
            lock_transaction.commit()
            claimed = future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    assert claimed is not None
    assert claimed.updated_at >= started + 2
    assert claimed.lease_expires_at == claimed.updated_at + 1


def test_provider_grant_samples_database_clock_after_budget_lock(
        image_database) -> None:
    budget = topology_state.upsert_provider_budget(provider='aws',
                                                   partition='aws',
                                                   account='123456789012',
                                                   region='us-east-1',
                                                   api_family='ecr',
                                                   applied_rate_per_second=10,
                                                   burst=10,
                                                   now=1)
    topology_state.register_worker('copy-budget-worker',
                                   models.ImageWorkerKind.COPY,
                                   'test',
                                   1,
                                   now=1)
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    started = int(
        lock_connection.execute(
            sqlalchemy.select(
                catalog_state.database_epoch_expression())).scalar_one())
    lock_connection.execute(schema.provider_budgets.update().where(
        schema.provider_budgets.c.id == budget.id).values(
            blocked_until=started + 1,
            tokens_milli=10_000,
            refilled_at=started,
            updated_at=started))
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(topology_state.acquire_provider_grant,
                                     'copy-budget-worker', budget.id, 1)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(2.1)')
            lock_transaction.commit()
            grant = future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    assert grant is not None
    assert grant.tokens == 1
    assert grant.valid_for_seconds == 1
    refreshed = topology_state.get_provider_budget(provider='aws',
                                                   partition='aws',
                                                   account='123456789012',
                                                   region='us-east-1',
                                                   api_family='ecr')
    assert refreshed is not None
    assert refreshed.refilled_at >= started + 2


def test_deadline_expired_canary_is_teardown_only(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-expired-deadline-key')
    claimed = qualification.claim_canary(worker_id='worker-a',
                                         lease_seconds=2000,
                                         now=200)
    assert claimed is not None and claimed.lease_token is not None
    assert claimed.teardown_deadline is not None
    child_id = f'ec2:{profile.targets[0].region}:{claimed.id}'

    assert not qualification.attach_canary_child(claimed.id,
                                                 claimed.lease_token,
                                                 child_id,
                                                 now=claimed.teardown_deadline)
    assert not qualification.complete_canary(
        claimed, {'teardown_verified': True}, now=claimed.teardown_deadline)
    assert not qualification.fail_canary(
        claimed, 'CANARY_FAILED', now=claimed.teardown_deadline)
    with pytest.raises(ValueError, match='must be verified'):
        qualification.fail_expired_canary(claimed,
                                          'CANARY_TIMEOUT',
                                          teardown_verified=False,
                                          now=claimed.teardown_deadline)
    assert qualification.fail_expired_canary(claimed,
                                             'CANARY_TIMEOUT',
                                             teardown_verified=True,
                                             now=claimed.teardown_deadline)

    _request_ec2_canary(monkeypatch,
                        profile,
                        idempotency_key='canary-expired-child-key')
    original = qualification.claim_canary(worker_id='worker-a',
                                          lease_seconds=10,
                                          now=300)
    assert original is not None and original.lease_token is not None
    assert original.teardown_deadline is not None
    persisted_child = f'ec2:{profile.targets[0].region}:{original.id}'
    assert qualification.attach_canary_child(original.id,
                                             original.lease_token,
                                             persisted_child,
                                             now=301)
    assert qualification.authorize_canary_launch(
        original.id, original.lease_token, persisted_child,
        now=301) == original.teardown_deadline - 301
    assert qualification.authorize_canary_launch(original.id,
                                                 'wrong-lease',
                                                 persisted_child,
                                                 now=301) is None
    assert qualification.authorize_canary_launch(original.id,
                                                 original.lease_token,
                                                 'wrong-child',
                                                 now=301) is None
    assert qualification.authorize_canary_launch(
        original.id,
        original.lease_token,
        persisted_child,
        now=original.teardown_deadline) is None
    cleanup_owner = qualification.claim_canary(worker_id='worker-b',
                                               lease_seconds=2000,
                                               now=original.teardown_deadline)
    assert cleanup_owner is not None and cleanup_owner.lease_token is not None
    assert qualification.attach_canary_child(cleanup_owner.id,
                                             cleanup_owner.lease_token,
                                             persisted_child,
                                             now=original.teardown_deadline)
    assert qualification.fail_expired_canary(cleanup_owner,
                                             'CANARY_TIMEOUT',
                                             teardown_verified=True,
                                             now=original.teardown_deadline)


def test_candidate_attestation_requires_unchanged_operational_inventory(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    candidate = _stage_candidate_profile(_policy_profile(profile), now=20)
    shard = topology_state.list_target_shards('research', profile.name,
                                              profile.targets[0].name)[0]
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                inventory_epoch=7,
                inventory_started_at=21,
                inventory_completed_at=22,
                updated_at=22))
    snapshot = topology_state.get_shard(shard.id)
    assert snapshot is not None
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                inventory_epoch=8,
                inventory_started_at=23,
                inventory_completed_at=24,
                updated_at=24))
    key = models.profile_attestation_key('infrastructure_shard',
                                         shard.physical_fingerprint)
    stale = topology_state.record_candidate_shard_attestation(
        profile_revision_id=candidate.id,
        expected_generation=candidate.desired_generation,
        expected_config_hash=candidate.config_hash,
        shard_id=snapshot.id,
        expected_operational_revision_id=active.id,
        expected_target_fingerprint=snapshot.target_fingerprint,
        expected_physical_fingerprint=snapshot.physical_fingerprint,
        expected_inventory_epoch=snapshot.inventory_epoch,
        expected_inventory_completed_at=snapshot.inventory_completed_at,
        kind=key,
        evidence={
            'status': 'READY',
            'observed_at': 25,
        },
        now=25)
    assert stale is None
    unchanged_candidate = topology_state.get_profile_revision(candidate.id)
    assert unchanged_candidate is not None
    assert key not in unchanged_candidate.attestations

    current = topology_state.get_shard(shard.id)
    assert current is not None and current.inventory_completed_at is not None
    recorded = topology_state.record_candidate_shard_attestation(
        profile_revision_id=candidate.id,
        expected_generation=candidate.desired_generation,
        expected_config_hash=candidate.config_hash,
        shard_id=current.id,
        expected_operational_revision_id=active.id,
        expected_target_fingerprint=current.target_fingerprint,
        expected_physical_fingerprint=current.physical_fingerprint,
        expected_inventory_epoch=current.inventory_epoch,
        expected_inventory_completed_at=current.inventory_completed_at,
        kind=key,
        evidence={
            'status': 'READY',
            'observed_at': 26,
        },
        now=26)
    assert recorded is not None
    assert recorded.attestations[key]['status'] == 'READY'
    unchanged = topology_state.get_shard(shard.id)
    assert unchanged is not None
    assert unchanged.profile_revision_id == active.id
    assert unchanged.state == models.ImageShardState.READY


def test_bootstrap_handoff_waits_between_inventory_pages(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    shard = topology_state.list_target_shards('research', profile.name,
                                              profile.targets[0].name)[0]
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                profile_revision_id=None,
                inventory_started_at=20,
                inventory_completed_at=None,
                inventory_finalizing=False,
                inventory_lease_token=None,
                inventory_lease_expires_at=None,
                updated_at=20))

    with orm.Session(image_database) as session, session.begin():
        with pytest.raises(ValueError,
                           match='inventory must finish before limits change'):
            topology_state.upsert_qualified_shard(
                session,
                workspace=shard.workspace,
                profile=shard.profile,
                target_id=shard.target_id,
                provider=shard.provider,
                partition=shard.partition,
                account=shard.account,
                region=shard.region,
                shard_generation=shard.shard_generation,
                shard_index=shard.shard_index,
                target_fingerprint=shard.target_fingerprint,
                physical_fingerprint=shard.physical_fingerprint,
                registry=shard.registry,
                repository_name=shard.repository_name,
                repository_arn=shard.repository_arn,
                max_manifests=80,
                max_declared_bytes=800_000,
                max_in_flight=3,
                now=21)
    unchanged = topology_state.get_shard(shard.id)
    assert unchanged is not None
    assert unchanged.max_manifests == shard.max_manifests
    assert unchanged.inventory_started_at == 20
    assert unchanged.inventory_completed_at is None


def test_candidate_handoff_is_nonmutating_and_activation_applies_atomically(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    for target in (profile.canonical,) + profile.targets:
        topology_state.upsert_provider_budget(provider='aws',
                                              partition=profile.partition,
                                              account=profile.registry_account,
                                              region=target.region,
                                              api_family='ecr',
                                              applied_rate_per_second=30,
                                              burst=15,
                                              now=14)
    before_shards = topology_state.list_shards('research', profile.name)
    before_budgets = {
        target.region: topology_state.get_provider_budget(
            provider='aws',
            partition=profile.partition,
            account=profile.registry_account,
            region=target.region,
            api_family='ecr')
        for target in (profile.canonical,) + profile.targets
    }
    assert all(item is not None for item in before_budgets.values())

    candidate_profile = _policy_profile(profile)
    candidate = _stage_candidate_profile(candidate_profile, now=20)
    with orm.Session(image_database) as session, session.begin():
        for shard in before_shards:
            topology_state.upsert_qualified_shard(
                session,
                workspace=shard.workspace,
                profile=shard.profile,
                target_id=shard.target_id,
                provider=shard.provider,
                partition=shard.partition,
                account=shard.account,
                region=shard.region,
                shard_generation=shard.shard_generation,
                shard_index=shard.shard_index,
                target_fingerprint=shard.target_fingerprint,
                physical_fingerprint=shard.physical_fingerprint,
                registry=shard.registry,
                repository_name=shard.repository_name,
                repository_arn=shard.repository_arn,
                max_manifests=80,
                max_declared_bytes=800_000,
                max_in_flight=3,
                now=21)
    for target in (profile.canonical,) + profile.targets:
        topology_state.ensure_provider_budget(provider='aws',
                                              partition=profile.partition,
                                              account=profile.registry_account,
                                              region=target.region,
                                              api_family='ecr',
                                              applied_rate_per_second=7,
                                              burst=3,
                                              now=21)
    assert topology_state.list_shards('research', profile.name) == before_shards
    assert {
        target.region: topology_state.get_provider_budget(
            provider='aws',
            partition=profile.partition,
            account=profile.registry_account,
            region=target.region,
            api_family='ecr')
        for target in (profile.canonical,) + profile.targets
    } == before_budgets

    attested = topology_state.record_profile_attestation(
        profile_revision_id=candidate.id,
        kind='terraform',
        evidence={
            'status': 'READY',
            'observed_at': 22,
        },
        expected_generation=candidate.desired_generation,
        expected_config_hash=candidate.config_hash,
        terraform_hash='e' * 64,
        now=22)
    for target in (candidate_profile.canonical,) + candidate_profile.targets:
        attested = topology_state.record_profile_attestation(
            profile_revision_id=candidate.id,
            kind=models.profile_attestation_key(
                'terraform_budget', 'aws', candidate_profile.partition,
                candidate_profile.registry_account, target.region, 'ecr'),
            evidence={
                'status': 'READY',
                'observed_at': 22,
                'provider': 'aws',
                'partition': candidate_profile.partition,
                'account': candidate_profile.registry_account,
                'region': target.region,
                'api_family': 'ecr',
                'applied_rate_per_second': 7,
                'burst': 3,
            },
            expected_generation=candidate.desired_generation,
            expected_config_hash=candidate.config_hash,
            now=22)
        qualification_name, qualification_arn = (
            _generated_qualification_repository(candidate_profile, target))
        attested = topology_state.record_profile_attestation(
            profile_revision_id=candidate.id,
            kind=models.profile_attestation_key('terraform_target',
                                                target.name),
            evidence={
                'status': 'READY',
                'observed_at': 22,
                'target_fingerprint': target.target_fingerprint,
                'registry': target.registry,
                'repository_name': qualification_name,
                'repository_arn': qualification_arn,
                'qualification_repository_generation':
                    target.qualification_repository_generation,
            },
            expected_generation=candidate.desired_generation,
            expected_config_hash=candidate.config_hash,
            now=22)
    for shard in before_shards:
        target = candidate_profile.target(shard.target_id)
        live_key = models.profile_attestation_key('infrastructure_shard',
                                                  shard.physical_fingerprint)
        attested = topology_state.record_profile_attestation(
            profile_revision_id=candidate.id,
            kind=models.profile_attestation_key('terraform_shard',
                                                shard.physical_fingerprint),
            evidence={
                'status': 'READY',
                'observed_at': 22,
                'physical_fingerprint': shard.physical_fingerprint,
                'target_fingerprint': shard.target_fingerprint,
                'target': shard.target_id,
                'max_manifests': 80,
                'max_declared_bytes': 800_000,
                'max_in_flight': 3,
                'terraform_applied_quota': 100,
                'reserved_headroom': 10,
                'live_attestation_key': live_key,
            },
            expected_generation=candidate.desired_generation,
            expected_config_hash=candidate.config_hash,
            now=22)
        recorded = topology_state.record_candidate_shard_attestation(
            profile_revision_id=candidate.id,
            expected_generation=candidate.desired_generation,
            expected_config_hash=candidate.config_hash,
            shard_id=shard.id,
            expected_operational_revision_id=active.id,
            expected_target_fingerprint=shard.target_fingerprint,
            expected_physical_fingerprint=shard.physical_fingerprint,
            expected_inventory_epoch=shard.inventory_epoch,
            expected_inventory_completed_at=shard.inventory_completed_at,
            kind=live_key,
            evidence={
                'status': 'READY',
                'observed_at': 22,
                'physical_fingerprint': shard.physical_fingerprint,
                'target_fingerprint': shard.target_fingerprint,
                'applied_images_per_repository_quota': 100,
                'reserved_headroom': 10,
                'inventory_epoch': shard.inventory_epoch,
                'inventory_completed_at': shard.inventory_completed_at,
            },
            now=22)
        assert recorded is not None
        attested = recorded

    stale_shard = before_shards[-1]
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == stale_shard.id).values(
                inventory_epoch=stale_shard.inventory_epoch + 1,
                inventory_started_at=23,
                inventory_completed_at=23,
                updated_at=23))
    assert attested.attestations_hash is not None
    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        transactions.activate_profile(
            profile_revision_id=candidate.id,
            expected_generation=candidate.desired_generation,
            expected_config_hash=candidate.config_hash,
            expected_terraform_hash='e' * 64,
            expected_attestations_hash=attested.attestations_hash,
            required_attestations={'terraform': None},
            now=24)
    assert topology_state.get_active_profile('research', profile.name) == active
    assert {
        target.region: topology_state.get_provider_budget(
            provider='aws',
            partition=profile.partition,
            account=profile.registry_account,
            region=target.region,
            api_family='ecr')
        for target in (profile.canonical,) + profile.targets
    } == before_budgets
    assert all(
        shard.profile_revision_id == active.id and shard.max_manifests == 100
        and shard.max_declared_bytes == 1_000_000 and shard.max_in_flight == 4
        for shard in topology_state.list_shards('research', profile.name))

    current = topology_state.get_shard(stale_shard.id)
    assert current is not None and current.inventory_completed_at is not None
    refreshed = topology_state.record_candidate_shard_attestation(
        profile_revision_id=candidate.id,
        expected_generation=candidate.desired_generation,
        expected_config_hash=candidate.config_hash,
        shard_id=current.id,
        expected_operational_revision_id=active.id,
        expected_target_fingerprint=current.target_fingerprint,
        expected_physical_fingerprint=current.physical_fingerprint,
        expected_inventory_epoch=current.inventory_epoch,
        expected_inventory_completed_at=current.inventory_completed_at,
        kind=models.profile_attestation_key('infrastructure_shard',
                                            current.physical_fingerprint),
        evidence={
            'status': 'READY',
            'observed_at': 25,
            'physical_fingerprint': current.physical_fingerprint,
            'target_fingerprint': current.target_fingerprint,
            'applied_images_per_repository_quota': 100,
            'reserved_headroom': 10,
            'inventory_epoch': current.inventory_epoch,
            'inventory_completed_at': current.inventory_completed_at,
        },
        now=25)
    assert refreshed is not None and refreshed.attestations_hash is not None
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == current.id).values(
                inventory_finalizing=True, updated_at=26))
    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        transactions.activate_profile(
            profile_revision_id=candidate.id,
            expected_generation=candidate.desired_generation,
            expected_config_hash=candidate.config_hash,
            expected_terraform_hash='e' * 64,
            expected_attestations_hash=refreshed.attestations_hash,
            required_attestations={'terraform': None},
            now=26)
    assert topology_state.get_active_profile('research', profile.name) == active
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == current.id).values(
                inventory_finalizing=False, updated_at=27))
    activated = transactions.activate_profile(
        profile_revision_id=candidate.id,
        expected_generation=candidate.desired_generation,
        expected_config_hash=candidate.config_hash,
        expected_terraform_hash='e' * 64,
        expected_attestations_hash=refreshed.attestations_hash,
        required_attestations={'terraform': None},
        now=28)

    assert activated.id == candidate.id
    assert all(
        shard.profile_revision_id == candidate.id and shard.max_manifests == 80
        and shard.max_declared_bytes == 800_000 and shard.max_in_flight == 3
        for shard in topology_state.list_shards('research', profile.name))
    for target in (profile.canonical,) + profile.targets:
        budget = topology_state.get_provider_budget(
            provider='aws',
            partition=profile.partition,
            account=profile.registry_account,
            region=target.region,
            api_family='ecr')
        assert budget is not None
        assert budget.applied_rate_milli == 7_000
        assert budget.burst_milli == 3_000


def test_qualifying_successor_does_not_block_publish_or_prepare(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    publication_record, canonical = _publish_and_bind(profile)
    _complete_location(canonical, now=30)
    assert publication_record.image_id is not None

    candidate_profile = _policy_profile(profile)
    candidate = _stage_candidate_profile(candidate_profile, now=40)
    _configure_profile(monkeypatch, candidate_profile)

    during_rollout = publication.publish(
        source_ref=_OTHER_SOURCE,
        release='during-profile-rollout',
        distribution=profile.name,
        workspace='research',
        actor_hash='2' * 64,
        idempotency_key='publication-during-profile-rollout')
    prepared = preparation.prepare(
        image_id=publication_record.image_id,
        distribution=profile.name,
        target_id=profile.targets[0].name,
        workspace='research',
        actor_hash='2' * 64,
        idempotency_key='prepare-during-profile-rollout')

    assert candidate.state == models.ImageProfileState.QUALIFYING
    assert during_rollout.publication.profile_revision_id == active.id
    assert prepared.location.target_fingerprint == (
        profile.targets[0].target_fingerprint)
    still_active = topology_state.get_active_profile('research', profile.name)
    assert still_active is not None and still_active.id == active.id


def _publish_and_bind(
    profile: models.ManagedRegistryProfile,
    *,
    source: str = _SOURCE,
    runtime_digest: str = _DIGEST,
    release: str = 'boltz-l4',
    idempotency_key: str = 'publication-idempotency-0001',
    now: int = 20,
) -> tuple[catalog_state.PublicationRecord, topology_state.LocationRecord]:
    mutation = publication.publish(source_ref=source,
                                   release=release,
                                   distribution=profile.name,
                                   workspace='research',
                                   actor_hash='1' * 64,
                                   idempotency_key=idempotency_key,
                                   now=now)
    assert catalog_state.get_ready_release(release, 'research') is None
    claimed = catalog_state.claim_publication_inspection(worker_id='copy-1',
                                                         lease_seconds=60,
                                                         now=now)
    assert claimed is not None and claimed.inspection_lease_token is not None
    _, _, location, bound = transactions.bind_inspected_publication(
        publication_id=claimed.id,
        inspection_lease_token=claimed.inspection_lease_token,
        creator_user_hash='1' * 64,
        runtime_digest=runtime_digest,
        platform='linux/amd64',
        config_digest=_CONFIG_DIGEST,
        source_root_media_type=_MANIFEST_MEDIA_TYPE,
        selected_manifest_media_type=_MANIFEST_MEDIA_TYPE,
        selected_manifest_size_bytes=512,
        declared_size_bytes=4096,
        canonical_target_id=profile.canonical.name,
        max_releases_per_artifact=profile.limits.max_releases_per_artifact,
        now=now + 1)
    assert bound.id == mutation.publication.id
    return bound, location


def test_bound_pending_publication_never_reenters_source_inspection(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    bound, _ = _publish_and_bind(profile)

    with image_database.connect() as connection:
        claimable_at = connection.execute(
            sqlalchemy.select(
                schema.publications.c.inspection_claimable_at).where(
                    schema.publications.c.id == bound.id)).scalar_one()

    assert bound.state == models.ImagePublicationState.PENDING
    assert bound.canonical_location_id is not None
    assert claimable_at is None
    assert catalog_state.claim_publication_inspection(worker_id='copy-2',
                                                      lease_seconds=60,
                                                      now=100) is None


def test_publication_retry_delay_uses_database_epoch(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    mutation = publication.publish(
        source_ref=_SOURCE,
        release='retry-delay',
        distribution=profile.name,
        workspace='research',
        actor_hash='1' * 64,
        idempotency_key='publication-retry-delay-0001',
        now=20)
    claimed = catalog_state.claim_publication_inspection(worker_id='copy-1',
                                                         lease_seconds=60,
                                                         now=20)
    assert claimed is not None and claimed.id == mutation.publication.id
    assert claimed.inspection_lease_token is not None

    assert catalog_state.fail_publication_inspection(
        claimed.id,
        claimed.inspection_lease_token,
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value,
        retry_delay_seconds=9,
        terminal=False,
        now=21)
    failed = catalog_state.get_publication(claimed.id, 'research')
    assert failed is not None and failed.next_retry_at == 30
    pending_operation = catalog_state.get_operation(mutation.operation.id,
                                                    'research')
    assert pending_operation is not None
    assert pending_operation.state == models.ImageOperationState.PENDING
    assert catalog_state.claim_publication_inspection(worker_id='copy-2',
                                                      lease_seconds=60,
                                                      now=29) is None
    reclaimed = catalog_state.claim_publication_inspection(worker_id='copy-2',
                                                           lease_seconds=60,
                                                           now=30)
    assert reclaimed is not None and reclaimed.id == claimed.id


def test_terminal_inspection_failure_terminalizes_publication_operation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    mutation = publication.publish(
        source_ref=_SOURCE,
        release='terminal-inspection-failure',
        distribution=profile.name,
        workspace='research',
        actor_hash='1' * 64,
        idempotency_key='terminal-inspection-failure-key',
        now=20)
    claimed = catalog_state.claim_publication_inspection(worker_id='copy-1',
                                                         lease_seconds=60,
                                                         now=20)
    assert claimed is not None and claimed.inspection_lease_token is not None

    assert catalog_state.fail_publication_inspection(
        claimed.id,
        claimed.inspection_lease_token,
        models.ImageLocationErrorCode.SOURCE_CONTENT_UNSUPPORTED.value,
        terminal=True,
        now=21)

    failed = catalog_state.get_publication(claimed.id, 'research')
    operation = catalog_state.get_operation(mutation.operation.id, 'research')
    assert failed is not None
    assert failed.state == models.ImagePublicationState.FAILED
    assert operation is not None
    assert operation.state == models.ImageOperationState.FAILED
    assert operation.error_code == (
        models.ImageLocationErrorCode.SOURCE_CONTENT_UNSUPPORTED.value)
    assert operation.result_kind == 'publication'
    assert operation.result_id == failed.id
    assert operation.result == {
        'image_id': None,
        'publication_id': failed.id,
        'release': 'terminal-inspection-failure',
        'state': models.ImagePublicationState.FAILED.value,
    }


def _complete_location(location: topology_state.LocationRecord, *,
                       now: int) -> topology_state.LocationRecord:
    claim = topology_state.claim_next_location(worker_id='copy-1',
                                               lease_seconds=60,
                                               workspace='research',
                                               now=now)
    assert claim is not None and claim.id == location.id
    assert claim.lease_token is not None
    assert topology_state.transition_location_to_verifying(claim.id,
                                                           claim.lease_token,
                                                           now=now + 1)
    return transactions.converge_canonical(location_id=claim.id,
                                           lease_token=claim.lease_token,
                                           ready=True,
                                           now=now + 2)


def test_full_shard_dispatches_already_reserved_retry(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    publication_record, location = _publish_and_bind(profile)
    shard = topology_state.get_shard(location.shard_id)
    assert shard is not None and shard.reserved_manifests > 0
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                max_manifests=shard.reserved_manifests,
                state=models.ImageShardState.FULL.value))

    first = topology_state.claim_next_location(worker_id='copy-1',
                                               lease_seconds=60,
                                               workspace='research',
                                               now=30)
    assert first is not None and first.id == location.id
    assert first.lease_token is not None
    retried = transactions.converge_canonical(
        location_id=first.id,
        lease_token=first.lease_token,
        ready=False,
        error_code=(models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value),
        retry_delay_seconds=9,
        terminal=False,
        now=31)
    assert retried.state == models.ImageLocationState.PENDING
    assert retried.next_retry_at == 40
    assert topology_state.claim_next_location(worker_id='copy-2',
                                              lease_seconds=60,
                                              workspace='research',
                                              now=39) is None

    second = topology_state.claim_next_location(worker_id='copy-2',
                                                lease_seconds=60,
                                                workspace='research',
                                                now=40)
    assert second is not None and second.id == location.id
    assert second.state == models.ImageLocationState.COPYING
    final_shard = topology_state.get_shard(shard.id)
    assert final_shard is not None
    assert final_shard.state == models.ImageShardState.FULL
    assert final_shard.in_flight == 1
    assert second.lease_token is not None
    assert topology_state.transition_location_to_verifying(second.id,
                                                           second.lease_token,
                                                           now=41)
    ready = transactions.converge_canonical(location_id=second.id,
                                            lease_token=second.lease_token,
                                            ready=True,
                                            now=42)
    assert ready.state == models.ImageLocationState.READY
    ready_publication = catalog_state.get_publication(publication_record.id,
                                                      'research')
    completed_shard = topology_state.get_shard(shard.id)
    assert ready_publication is not None
    assert ready_publication.state == models.ImagePublicationState.READY
    assert completed_shard is not None
    assert completed_shard.state == models.ImageShardState.FULL
    assert completed_shard.in_flight == 0


@pytest.mark.parametrize(
    ('shard_state', 'allowed'),
    [(models.ImageShardState.FULL, True),
     (models.ImageShardState.DRIFTED, False),
     (models.ImageShardState.DISABLED, False)],
)
def test_canonical_publication_retry_obeys_shard_admission_state(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile,
        shard_state: models.ImageShardState, allowed: bool) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    publication_record, location = _publish_and_bind(profile)
    claim = topology_state.claim_next_location(worker_id='copy-1',
                                               lease_seconds=60,
                                               workspace='research',
                                               now=30)
    assert claim is not None and claim.id == location.id
    assert claim.lease_token is not None
    failed = transactions.converge_canonical(
        location_id=claim.id,
        lease_token=claim.lease_token,
        ready=False,
        error_code=(
            models.ImageLocationErrorCode.DESTINATION_DIGEST_MISMATCH.value),
        terminal=True,
        now=31)
    assert failed.state == models.ImageLocationState.FAILED
    shard = topology_state.get_shard(location.shard_id)
    assert shard is not None
    values: dict[str, Any] = {'state': shard_state.value}
    if shard_state == models.ImageShardState.FULL:
        values['max_manifests'] = shard.reserved_manifests
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(**values))

    def retry() -> publication.PublicationMutation:
        return publication.retry(
            publication_id=publication_record.id,
            workspace='research',
            actor_hash='2' * 64,
            idempotency_key=(
                f'canonical-retry-{shard_state.value.lower()}-0001'),
            now=32)

    if allowed:
        mutation = retry()
        assert mutation.publication.state == models.ImagePublicationState.PENDING
        pending = topology_state.get_location(location.id)
        assert pending is not None
        assert pending.state == models.ImageLocationState.PENDING
        reclaimed = topology_state.claim_next_location(worker_id='copy-2',
                                                       lease_seconds=60,
                                                       workspace='research',
                                                       now=40)
        assert reclaimed is not None and reclaimed.id == location.id
    else:
        with pytest.raises(topology_state.RegistryShardUnavailableError,
                           match='REGISTRY_SHARD_UNAVAILABLE'):
            retry()
        retained = topology_state.get_location(location.id)
        failed_publication = catalog_state.get_publication(
            publication_record.id, 'research')
        assert retained is not None
        assert retained.state == models.ImageLocationState.FAILED
        assert failed_publication is not None
        assert failed_publication.state == models.ImagePublicationState.FAILED


@pytest.mark.parametrize('shard_state', [
    models.ImageShardState.PENDING,
    models.ImageShardState.DRIFTED,
    models.ImageShardState.DISABLED,
])
def test_expired_copy_lease_repairs_blocked_shard_in_flight(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile,
        shard_state: models.ImageShardState) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    _, location = _publish_and_bind(profile)
    first = topology_state.claim_next_location(worker_id='copy-1',
                                               lease_seconds=10,
                                               workspace='research',
                                               now=30)
    assert first is not None and first.id == location.id
    assert first.lease_token is not None
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == location.shard_id).values(
                state=shard_state.value))

    assert topology_state.heartbeat_location(first.id,
                                             first.lease_token,
                                             lease_seconds=10,
                                             now=35)
    assert topology_state.claim_next_location(worker_id='copy-early',
                                              lease_seconds=10,
                                              workspace='research',
                                              now=41) is None

    reclaimed = topology_state.claim_next_location(worker_id='copy-2',
                                                   lease_seconds=10,
                                                   workspace='research',
                                                   now=46)
    assert reclaimed is not None and reclaimed.id == location.id
    assert reclaimed.state == models.ImageLocationState.VERIFYING
    assert reclaimed.lease_kind == 'VERIFY'
    assert reclaimed.lease_token is not None
    during = topology_state.get_shard(location.shard_id)
    assert during is not None and during.in_flight == 1
    pending = transactions.converge_canonical(
        location_id=reclaimed.id,
        lease_token=reclaimed.lease_token,
        ready=False,
        error_code=models.ImageLocationErrorCode.MANIFEST_MISSING.value,
        retry_at=50,
        terminal=False,
        now=47)
    assert pending.state == models.ImageLocationState.PENDING
    repaired = topology_state.get_shard(location.shard_id)
    assert repaired is not None
    assert repaired.state == shard_state
    assert repaired.in_flight == 0
    assert topology_state.claim_next_location(worker_id='copy-3',
                                              lease_seconds=10,
                                              workspace='research',
                                              now=50) is None


def test_drifted_shard_heartbeat_race_never_dispatches_fresh_write(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    _, location = _publish_and_bind(profile)
    claimed = topology_state.claim_next_location(worker_id='copy-1',
                                                 lease_seconds=10,
                                                 workspace='research',
                                                 now=30)
    assert claimed is not None and claimed.id == location.id
    shard = topology_state.get_shard(location.shard_id)
    assert shard is not None
    fresh_image_id = str(uuid.uuid4())
    fresh_location_id = str(uuid.uuid4())
    with image_database.begin() as connection:
        connection.execute(schema.images.insert().values(
            id=fresh_image_id,
            workspace='research',
            runtime_digest=_OTHER_DIGEST,
            platform='linux/amd64',
            config_digest=_CONFIG_DIGEST,
            manifest_media_type=_MANIFEST_MEDIA_TYPE,
            manifest_size_bytes=1,
            declared_size_bytes=1,
            creator_user_hash='3' * 64,
            producer_kind='external_oci',
            created_at=35,
            updated_at=35))
        connection.execute(schema.locations.insert().values(
            id=fresh_location_id,
            workspace='research',
            image_id=fresh_image_id,
            shard_id=shard.id,
            target_fingerprint=shard.target_fingerprint,
            physical_fingerprint=shard.physical_fingerprint,
            runtime_digest=_OTHER_DIGEST,
            canonical=True,
            canonical_location_id=None,
            target_ref=(f'{shard.registry}/{shard.repository_name}@'
                        f'{_OTHER_DIGEST}'),
            state=models.ImageLocationState.PENDING.value,
            attempt_count=0,
            reserved_declared_bytes=1,
            created_at=35,
            updated_at=35))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                reserved_manifests=(
                    schema.registry_shards.c.reserved_manifests + 1),
                reserved_declared_bytes=(
                    schema.registry_shards.c.reserved_declared_bytes + 1),
                state=models.ImageShardState.DRIFTED.value))

    with image_database.connect() as blocker:
        transaction = blocker.begin()
        try:
            blocker.execute(schema.locations.update().where(
                schema.locations.c.id == location.id).values(
                    lease_expires_at=100))
            assert topology_state.claim_next_location(worker_id='copy-2',
                                                      lease_seconds=10,
                                                      workspace='research',
                                                      now=41) is None
        finally:
            transaction.commit()

    fresh = topology_state.get_location(fresh_location_id)
    assert fresh is not None
    assert fresh.state == models.ImageLocationState.PENDING
    assert fresh.lease_token is None
    recovered = topology_state.claim_next_location(worker_id='copy-3',
                                                   lease_seconds=10,
                                                   workspace='research',
                                                   now=101)
    assert recovered is not None and recovered.id == location.id
    assert recovered.state == models.ImageLocationState.VERIFYING


def test_copy_heartbeat_preserves_shard_fairness_floor(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    _, first_location = _publish_and_bind(profile)
    shard = topology_state.get_shard(first_location.shard_id)
    assert shard is not None
    second_image_id = str(uuid.uuid4())
    second_location_id = str(uuid.uuid4())
    with orm.Session(image_database) as session, session.begin():
        session.execute(schema.images.insert().values(
            id=second_image_id,
            workspace='research',
            runtime_digest=_OTHER_DIGEST,
            platform='linux/amd64',
            config_digest=_CONFIG_DIGEST,
            manifest_media_type=_MANIFEST_MEDIA_TYPE,
            manifest_size_bytes=1,
            declared_size_bytes=1,
            creator_user_hash='3' * 64,
            producer_kind='external_oci',
            created_at=22,
            updated_at=22))
        session.execute(schema.locations.insert().values(
            id=second_location_id,
            workspace='research',
            image_id=second_image_id,
            shard_id=shard.id,
            target_fingerprint=shard.target_fingerprint,
            physical_fingerprint=shard.physical_fingerprint,
            runtime_digest=_OTHER_DIGEST,
            canonical=True,
            canonical_location_id=None,
            target_ref=(f'{shard.registry}/{shard.repository_name}@'
                        f'{_OTHER_DIGEST}'),
            state=models.ImageLocationState.PENDING.value,
            attempt_count=0,
            reserved_declared_bytes=1,
            created_at=22,
            updated_at=22))
        session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                reserved_manifests=(
                    schema.registry_shards.c.reserved_manifests + 1),
                reserved_declared_bytes=(
                    schema.registry_shards.c.reserved_declared_bytes + 1)))
        topology_state._refresh_shard_copy_queue_in_session(session,
                                                            shard.id,
                                                            now=22)

    claimed = topology_state.claim_next_location(worker_id='copy-1',
                                                 lease_seconds=20,
                                                 workspace='research',
                                                 now=30)
    assert claimed is not None and claimed.id == first_location.id
    assert claimed.lease_token is not None
    assert topology_state.heartbeat_location(claimed.id,
                                             claimed.lease_token,
                                             lease_seconds=20,
                                             now=31)
    with image_database.connect() as connection:
        copy_next_at, last_dispatch_at = connection.execute(
            sqlalchemy.select(
                schema.registry_shards.c.copy_next_at,
                schema.registry_shards.c.last_dispatch_at).where(
                    schema.registry_shards.c.id == shard.id)).one()
    assert last_dispatch_at == 30
    assert copy_next_at == 30


def _ready_regional(
    engine: sqlalchemy.engine.Engine,
    monkeypatch: pytest.MonkeyPatch,
    profile: models.ManagedRegistryProfile,
) -> tuple[topology_state.ProfileRevisionRecord,
           catalog_state.PublicationRecord, topology_state.LocationRecord,
           topology_state.LocationRecord]:
    active = _activate_profile(engine, profile)
    _configure_profile(monkeypatch, profile)
    publication_record, canonical = _publish_and_bind(profile)
    canonical = _complete_location(canonical, now=30)
    assert publication_record.image_id is not None
    west = profile.targets[0]
    regional = transactions.reserve_regional_location(
        image_id=publication_record.image_id,
        workspace='research',
        profile_revision_id=active.id,
        target_id=west.name,
        canonical_location_id=canonical.id,
        max_regional_locations=16,
        now=40)
    regional = _complete_location(regional, now=41)
    return active, publication_record, canonical, regional


def _begin_delete(
    location: topology_state.LocationRecord,
    *,
    now: int,
) -> topology_state.LocationRecord:
    assert location.lease_token is not None
    assert topology_state.begin_eviction_delete(location.id,
                                                location.lease_token,
                                                now=now)
    updated = topology_state.get_location(location.id)
    assert updated is not None and updated.lease_kind == 'DELETE'
    return updated


def _mark_readback(
    location: topology_state.LocationRecord,
    *,
    now: int,
) -> topology_state.LocationRecord:
    assert location.lease_token is not None
    assert topology_state.mark_eviction_readback(location.id,
                                                 location.lease_token,
                                                 now=now)
    updated = topology_state.get_location(location.id)
    assert updated is not None and updated.lease_kind == 'READBACK'
    return updated


def _start_inventory_finalization(
    engine: sqlalchemy.engine.Engine,
    shard_id: str,
) -> topology_state.ShardRecord:
    with engine.begin() as connection:
        connection.execute(schema.registry_shards.update().values(
            inventory_completed_at=1000, inventory_next_at=1000))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard_id).values(
                inventory_completed_at=11, inventory_next_at=11))
    claimed = topology_state.claim_inventory_shard(worker_id='copy-inventory',
                                                   lease_seconds=60,
                                                   interval_seconds=1,
                                                   now=100)
    assert claimed is not None and claimed.id == shard_id
    assert claimed.inventory_lease_token is not None
    completed = topology_state.record_inventory_page(
        claimed.id, claimed.inventory_lease_token, (), None, now=101)
    assert completed is not None and completed.inventory_finalizing
    return completed


def _start_inventory_listing(
    engine: sqlalchemy.engine.Engine,
    shard_id: str,
    digests: tuple[str, ...],
) -> topology_state.ShardRecord:
    with engine.begin() as connection:
        connection.execute(schema.registry_shards.update().values(
            inventory_completed_at=1000, inventory_next_at=1000))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard_id).values(
                inventory_completed_at=11, inventory_next_at=11))
    claimed = topology_state.claim_inventory_shard(worker_id='copy-listing',
                                                   lease_seconds=60,
                                                   interval_seconds=1,
                                                   now=100)
    assert claimed is not None and claimed.id == shard_id
    assert claimed.inventory_lease_token is not None
    continued = topology_state.record_inventory_page(
        claimed.id,
        claimed.inventory_lease_token,
        digests,
        'next-page',
        now=101)
    assert continued is not None
    assert continued.inventory_completed_at is None
    assert topology_state.release_inventory_claim(continued.id,
                                                  claimed.inventory_lease_token,
                                                  continued.inventory_epoch,
                                                  now=102)
    released = topology_state.get_shard(continued.id)
    assert released is not None
    assert released.inventory_started_at is not None
    assert released.inventory_completed_at is None
    assert released.inventory_lease_token is None
    return released


def test_inventory_abandon_rechecks_database_clock_after_blocking_lock(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    claimed = topology_state.claim_inventory_shard(worker_id='copy-1',
                                                   lease_seconds=1)
    assert claimed is not None
    assert claimed.inventory_lease_token is not None
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.select(schema.registry_shards.c.id).where(
            schema.registry_shards.c.id == claimed.id).with_for_update()).one()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(topology_state.abandon_inventory_claim,
                                     claimed.id, claimed.inventory_lease_token,
                                     claimed.inventory_epoch)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(1.2)')
            lock_transaction.commit()
            assert not future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    unchanged = topology_state.get_shard(claimed.id)
    assert unchanged is not None
    assert unchanged.inventory_epoch == claimed.inventory_epoch
    assert unchanged.inventory_lease_token == claimed.inventory_lease_token


def test_inventory_attestation_and_lease_release_are_atomic(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    claimed = topology_state.claim_inventory_shard(worker_id='copy-1',
                                                   lease_seconds=60,
                                                   interval_seconds=1,
                                                   now=100)
    assert claimed is not None
    assert claimed.inventory_lease_token is not None
    completed = topology_state.record_inventory_page(
        claimed.id, claimed.inventory_lease_token, (), None, now=101)
    assert completed is not None
    assert completed.inventory_completed_at == 101
    assert completed.inventory_finalizing
    assert completed.inventory_lease_token == claimed.inventory_lease_token
    key = f'infrastructure_shard:{claimed.physical_fingerprint}'
    evidence = {
        'status': 'READY',
        'observed_at': 102,
        'inventory_epoch': completed.inventory_epoch,
        'inventory_completed_at': completed.inventory_completed_at,
    }

    stale = topology_state.record_inventory_attestation_and_release(
        profile_revision_id=active.id,
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        shard_id=completed.id,
        inventory_lease_token='wrong-token',
        expected_profile_revision_id=active.id,
        expected_target_fingerprint=completed.target_fingerprint,
        expected_physical_fingerprint=completed.physical_fingerprint,
        expected_inventory_epoch=completed.inventory_epoch,
        expected_inventory_completed_at=completed.inventory_completed_at,
        kind=key,
        evidence=evidence,
        now=102)
    assert stale is None
    unchanged = topology_state.get_profile_revision(active.id)
    assert unchanged is not None and key not in unchanged.attestations
    still_claimed = topology_state.get_shard(completed.id)
    assert still_claimed is not None
    assert still_claimed.inventory_lease_token == claimed.inventory_lease_token

    recorded = topology_state.record_inventory_attestation_and_release(
        profile_revision_id=active.id,
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        shard_id=completed.id,
        inventory_lease_token=claimed.inventory_lease_token,
        expected_profile_revision_id=active.id,
        expected_target_fingerprint=completed.target_fingerprint,
        expected_physical_fingerprint=completed.physical_fingerprint,
        expected_inventory_epoch=completed.inventory_epoch,
        expected_inventory_completed_at=completed.inventory_completed_at,
        kind=key,
        evidence=evidence,
        now=102)
    assert recorded is not None
    assert recorded.attestations[key] == evidence
    released = topology_state.get_shard(completed.id)
    assert released is not None
    assert not released.inventory_finalizing
    assert released.inventory_lease_token is None
    assert released.inventory_lease_expires_at is None


def test_inventory_page_rolls_back_if_lease_expires_on_location_lock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().values(
            inventory_completed_at=4_000_000_000,
            inventory_next_at=4_000_000_000))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == regional.shard_id).values(
                inventory_completed_at=11, inventory_next_at=11))
    before_location = topology_state.get_location(regional.id)
    assert before_location is not None
    claimed = topology_state.claim_inventory_shard(worker_id='copy-1',
                                                   lease_seconds=1,
                                                   interval_seconds=1,
                                                   now=100)
    assert claimed is not None and claimed.id == regional.shard_id
    assert claimed.inventory_lease_token is not None

    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.select(schema.locations.c.id).where(
            schema.locations.c.id == regional.id).with_for_update()).one()
    location_update_started = threading.Event()
    clock_reads = 0

    def database_clock(*, now=None):
        nonlocal clock_reads
        if now is not None:
            return sqlalchemy.literal(now, type_=sqlalchemy.BigInteger())
        clock_reads += 1
        value = 100 if clock_reads == 1 else 200
        return sqlalchemy.literal(value, type_=sqlalchemy.BigInteger())

    def observe_location_update(_connection, _cursor, statement, _parameters,
                                _context, _executemany):
        if 'update container_image_locations ' in statement.lower():
            location_update_started.set()

    monkeypatch.setattr(catalog_state, 'database_epoch_expression',
                        database_clock)
    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            observe_location_update)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(topology_state.record_inventory_page,
                                     claimed.id, claimed.inventory_lease_token,
                                     (regional.runtime_digest,), 'next-page')
            assert location_update_started.wait(timeout=10)
            assert not future.done()
            lock_transaction.commit()
            assert future.result(timeout=10) is None
    finally:
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                observe_location_update)
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()
    assert clock_reads >= 2

    unchanged_shard = topology_state.get_shard(claimed.id)
    assert unchanged_shard is not None
    assert unchanged_shard.inventory_cursor is None
    assert unchanged_shard.inventory_completed_at is None
    assert unchanged_shard.observed_manifests == 0
    unchanged_location = topology_state.get_location(regional.id)
    assert unchanged_location is not None
    assert (unchanged_location.inventory_epoch_seen ==
            before_location.inventory_epoch_seen)


def test_inventory_confirmation_pages_are_successor_resumable(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    shard = topology_state.list_target_shards('research', profile.name,
                                              profile.targets[0].name)[0]
    image_rows = []
    location_rows = []
    for index in range(101):
        digest = f'sha256:{index + 1:064x}'
        image_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f'inventory-image-{index}'))
        location_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f'inventory-location-{index}'))
        image_rows.append({
            'id': image_id,
            'workspace': 'research',
            'runtime_digest': digest,
            'platform': 'linux/amd64',
            'config_digest': _CONFIG_DIGEST,
            'manifest_media_type': _MANIFEST_MEDIA_TYPE,
            'manifest_size_bytes': 1,
            'declared_size_bytes': 1,
            'creator_user_hash': '4' * 64,
            'producer_kind': 'external_oci',
            'created_at': 20,
            'updated_at': 20,
        })
        location_rows.append({
            'id': location_id,
            'workspace': 'research',
            'image_id': image_id,
            'shard_id': shard.id,
            'target_fingerprint': shard.target_fingerprint,
            'physical_fingerprint': shard.physical_fingerprint,
            'runtime_digest': digest,
            'canonical': True,
            'canonical_location_id': None,
            'target_ref': f'{shard.registry}/{shard.repository_name}@{digest}',
            'state': models.ImageLocationState.READY.value,
            'attempt_count': 0,
            'last_verified_at': 20,
            'inventory_epoch_seen': shard.inventory_epoch,
            'reserved_declared_bytes': 1,
            'created_at': 20,
            'updated_at': 20,
        })
    with image_database.begin() as connection:
        connection.execute(schema.images.insert(), image_rows)
        connection.execute(schema.locations.insert(), location_rows)
        connection.execute(schema.registry_shards.update().values(
            inventory_completed_at=1000, inventory_next_at=1000))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                max_manifests=200,
                reserved_manifests=101,
                reserved_declared_bytes=101,
                inventory_completed_at=11,
                inventory_next_at=11,
                updated_at=20))

    first = topology_state.claim_inventory_shard(worker_id='copy-1',
                                                 lease_seconds=60,
                                                 interval_seconds=1,
                                                 now=100)
    assert first is not None and first.id == shard.id
    assert first.inventory_lease_token is not None
    completed = topology_state.record_inventory_page(
        first.id, first.inventory_lease_token, (), None, now=101)
    assert completed is not None and completed.inventory_finalizing
    first_page = topology_state.list_inventory_missing_candidates(
        completed.id, completed.inventory_epoch, limit=100)
    assert len(first_page) == 100
    for location in first_page:
        assert topology_state.complete_inventory_confirmation(
            location.id,
            completed.id,
            completed.inventory_epoch,
            first.inventory_lease_token,
            present=False,
            now=102) is not None

    key = models.profile_attestation_key('infrastructure_shard',
                                         shard.physical_fingerprint)
    evidence = {
        'status': 'READY',
        'observed_at': 103,
        'inventory_epoch': completed.inventory_epoch,
        'inventory_completed_at': completed.inventory_completed_at,
    }
    pending = topology_state.record_inventory_attestation_and_release(
        profile_revision_id=active.id,
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        shard_id=completed.id,
        inventory_lease_token=first.inventory_lease_token,
        expected_profile_revision_id=active.id,
        expected_target_fingerprint=completed.target_fingerprint,
        expected_physical_fingerprint=completed.physical_fingerprint,
        expected_inventory_epoch=completed.inventory_epoch,
        expected_inventory_completed_at=completed.inventory_completed_at,
        kind=key,
        evidence=evidence,
        now=103)
    assert pending is None
    partial = topology_state.get_shard(completed.id)
    assert partial is not None and partial.inventory_finalizing
    assert partial.inventory_lease_token is None

    successor = topology_state.claim_inventory_shard(worker_id='copy-2',
                                                     lease_seconds=60,
                                                     interval_seconds=600,
                                                     now=104)
    assert successor is not None and successor.id == completed.id
    assert successor.inventory_epoch == completed.inventory_epoch
    assert successor.inventory_started_at == completed.inventory_started_at
    assert successor.inventory_completed_at == completed.inventory_completed_at
    assert successor.inventory_lease_token is not None
    final_page = topology_state.list_inventory_missing_candidates(
        successor.id, successor.inventory_epoch, limit=100)
    assert len(final_page) == 1
    assert topology_state.complete_inventory_confirmation(
        final_page[0].id,
        successor.id,
        successor.inventory_epoch,
        successor.inventory_lease_token,
        present=False,
        now=105) is not None
    recorded = topology_state.record_inventory_attestation_and_release(
        profile_revision_id=active.id,
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        shard_id=successor.id,
        inventory_lease_token=successor.inventory_lease_token,
        expected_profile_revision_id=active.id,
        expected_target_fingerprint=successor.target_fingerprint,
        expected_physical_fingerprint=successor.physical_fingerprint,
        expected_inventory_epoch=successor.inventory_epoch,
        expected_inventory_completed_at=successor.inventory_completed_at,
        kind=key,
        evidence=evidence,
        now=106)
    assert recorded is not None
    assert recorded.attestations[key] == {**evidence, 'observed_at': 106}
    assert evidence['observed_at'] == 103
    finalized = topology_state.get_shard(successor.id)
    assert finalized is not None and not finalized.inventory_finalizing
    assert finalized.inventory_lease_token is None
    assert not topology_state.list_inventory_missing_candidates(
        successor.id, successor.inventory_epoch, limit=1)


def test_inventory_list_absence_requires_exact_confirmation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().values(
            inventory_completed_at=1000, inventory_next_at=1000))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == regional.shard_id).values(
                inventory_completed_at=11, inventory_next_at=11))
    claimed = topology_state.claim_inventory_shard(worker_id='copy-1',
                                                   lease_seconds=60,
                                                   interval_seconds=1,
                                                   now=100)
    assert claimed is not None and claimed.id == regional.shard_id
    assert claimed.inventory_lease_token is not None

    completed = topology_state.record_inventory_page(
        claimed.id, claimed.inventory_lease_token, (), None, now=101)

    assert completed is not None
    assert completed.state in (models.ImageShardState.READY,
                               models.ImageShardState.FULL)
    unchanged = topology_state.get_location(regional.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.READY
    candidates = topology_state.list_inventory_missing_candidates(
        claimed.id, completed.inventory_epoch)
    assert [item.id for item in candidates] == [regional.id]

    missing = topology_state.complete_inventory_confirmation(
        regional.id,
        claimed.id,
        completed.inventory_epoch,
        claimed.inventory_lease_token,
        present=False,
        now=102)
    assert missing is not None
    assert missing.state == models.ImageLocationState.MISSING


def test_inventory_exact_presence_refreshes_confirmation_anchor(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    completed = _start_inventory_finalization(image_database, regional.shard_id)
    assert completed.inventory_lease_token is not None
    candidates = topology_state.list_inventory_missing_candidates(
        completed.id, completed.inventory_epoch)
    assert [item.id for item in candidates] == [regional.id]

    present = topology_state.complete_inventory_confirmation(
        regional.id,
        completed.id,
        completed.inventory_epoch,
        completed.inventory_lease_token,
        present=True,
        now=102)

    assert present is not None
    assert present.state == models.ImageLocationState.READY
    assert present.inventory_epoch_seen == completed.inventory_epoch
    assert present.last_verified_at == 102
    assert not topology_state.list_inventory_missing_candidates(
        completed.id, completed.inventory_epoch)


def test_inventory_finalization_blocks_fresh_eviction_claim(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    completed = _start_inventory_finalization(image_database, regional.shard_id)
    assert [
        item.id for item in topology_state.list_inventory_missing_candidates(
            completed.id, completed.inventory_epoch)
    ] == [regional.id]

    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=102)

    assert eviction is None
    unchanged = topology_state.get_location(regional.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.READY


def test_inventory_finalization_blocks_no_io_eviction_restore(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=50)
    assert eviction is not None and eviction.id == regional.id
    assert eviction.lease_token is not None
    _start_inventory_finalization(image_database, regional.shard_id)

    restored = topology_state.complete_eviction(eviction.id,
                                                eviction.lease_token,
                                                present=None,
                                                provider_not_called=True,
                                                now=102)

    assert restored is None
    unchanged = topology_state.get_location(regional.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.EVICTING
    assert unchanged.lease_kind == 'EVICT'


def test_inventory_listing_blocks_fresh_eviction_between_pages(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    listing = _start_inventory_listing(image_database, regional.shard_id,
                                       (regional.runtime_digest,))
    assert listing.observed_manifests == 1

    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=103)

    assert eviction is None
    unchanged = topology_state.get_location(regional.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.READY


def test_inventory_listing_blocks_no_io_eviction_restore(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=50)
    assert eviction is not None and eviction.id == regional.id
    assert eviction.lease_token is not None
    _start_inventory_listing(image_database, regional.shard_id,
                             (regional.runtime_digest,))

    restored = topology_state.complete_eviction(eviction.id,
                                                eviction.lease_token,
                                                present=None,
                                                provider_not_called=True,
                                                now=103)

    assert restored is None
    unchanged = topology_state.get_location(regional.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.EVICTING
    assert unchanged.lease_kind == 'EVICT'


def test_inventory_listing_defers_capacity_releasing_readback(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=50)
    assert eviction is not None and eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=51)
    eviction = _mark_readback(eviction, now=52)
    shard_before = topology_state.get_shard(regional.shard_id)
    assert shard_before is not None
    _start_inventory_listing(image_database, regional.shard_id,
                             (regional.runtime_digest,))

    completed = topology_state.complete_eviction(eviction.id,
                                                 eviction.lease_token,
                                                 present=False,
                                                 now=103)

    assert completed is None
    retained = topology_state.get_location(regional.id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.EVICTING
    assert retained.lease_kind == 'READBACK'
    shard_after = topology_state.get_shard(regional.shard_id)
    assert shard_after is not None
    assert shard_after.reserved_manifests == shard_before.reserved_manifests


def test_expired_readback_is_reclaimed_without_repeating_delete(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=100)
    eviction = _mark_readback(eviction, now=100)
    old_token = eviction.lease_token

    reclaimed = topology_state.claim_next_eviction(worker_id='lifecycle-2',
                                                   unused_before=1000,
                                                   lease_seconds=60,
                                                   now=161)

    assert reclaimed is not None and reclaimed.id == regional.id
    assert reclaimed.state == models.ImageLocationState.EVICTING
    assert reclaimed.lease_kind == 'READBACK'
    assert reclaimed.lease_token != old_token
    shard = topology_state.get_shard(regional.shard_id)
    assert shard is not None and shard.in_flight == 1
    completed = topology_state.complete_eviction(reclaimed.id,
                                                 reclaimed.lease_token,
                                                 present=False,
                                                 now=162)
    assert completed is not None
    assert completed.state == models.ImageLocationState.EVICTED


def test_drifted_shard_rejects_location_readmission(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, canonical, regional = _ready_regional(
        image_database, monkeypatch, profile)
    before = topology_state.get_shard(regional.shard_id)
    assert before is not None
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == regional.shard_id).values(
                state=models.ImageShardState.DRIFTED.value))
        connection.execute(schema.locations.update().where(
            schema.locations.c.id == regional.id).values(
                state=models.ImageLocationState.MISSING.value,
                error_code=(
                    models.ImageLocationErrorCode.MANIFEST_MISSING.value)))

    assert topology_state.retry_location(regional.id, 'research',
                                         now=50) is None
    with pytest.raises(topology_state.RegistryShardUnavailableError,
                       match='REGISTRY_SHARD_UNAVAILABLE'):
        preparation.retry_location(location_id=regional.id,
                                   workspace='research',
                                   actor_hash='publisher',
                                   idempotency_key='retry-drifted-location')
    with pytest.raises(topology_state.RegistryShardUnavailableError,
                       match='REGISTRY_SHARD_UNAVAILABLE'):
        preparation.retry_location(location_id=regional.id,
                                   workspace='research',
                                   actor_hash='publisher',
                                   idempotency_key='retry-drifted-location')
    with image_database.connect() as connection:
        operation = connection.execute(
            sqlalchemy.select(schema.operations).where(
                schema.operations.c.idempotency_key ==
                'retry-drifted-location')).mappings().one()
    assert operation['state'] == models.ImageOperationState.FAILED.value
    assert operation['error_code'] == 'REGISTRY_SHARD_UNAVAILABLE'
    assert operation['result_id'] == regional.id
    assert publication_record.image_id is not None
    with pytest.raises(topology_state.RegistryShardUnavailableError,
                       match='REGISTRY_SHARD_UNAVAILABLE'):
        transactions.reserve_regional_location(
            image_id=publication_record.image_id,
            workspace='research',
            profile_revision_id=active.id,
            target_id=profile.targets[0].name,
            canonical_location_id=canonical.id,
            max_regional_locations=16,
            now=50)
    retained = topology_state.get_location(regional.id)
    after = topology_state.get_shard(regional.shard_id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.MISSING
    assert after is not None
    assert after.state == models.ImageShardState.DRIFTED
    assert after.reserved_manifests == before.reserved_manifests
    assert after.reserved_declared_bytes == before.reserved_declared_bytes


def _runtime_resolution(
    resources: Any,
    profile: models.ManagedRegistryProfile,
    active: topology_state.ProfileRevisionRecord,
    publication_record: catalog_state.PublicationRecord,
    location: topology_state.LocationRecord,
    *,
    current_demand: demand_state.DemandRecord | None = None,
) -> runtime._MetadataResolution:
    assert publication_record.image_id is not None
    artifact = catalog_state.get_artifact(publication_record.image_id,
                                          'research')
    assert artifact is not None
    target = next(target for target in (profile.canonical,) + profile.targets
                  if target.target_fingerprint == location.target_fingerprint)
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    policy = models.WorkspaceImagePolicy(
        mode=models.WorkspaceImageMode.MANAGED_REQUIRED,
        default_profile=profile.name,
        allowed_profiles=(profile.name,))
    return runtime._MetadataResolution(
        resources=resources,
        direct=False,
        profile=profile,
        policy=policy,
        active=active,
        artifact=artifact,
        publication=publication_record,
        location=location,
        target=target,
        binding=binding,
        runtime_principal=binding.principals[0],
        instance_profile=binding.instance_profile,
        current_demand=current_demand)


def _runtime_placement(
    profile: models.ManagedRegistryProfile,
    target: models.ManagedRegistryTarget,
    *,
    backend: str,
    region: str,
    consumer: dict[str, Any],
) -> dict[str, Any]:
    binding_id = target.runtime_binding(backend)
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    placement: dict[str, Any] = {
        'provider': 'aws',
        'region': region,
        'backend': backend,
        'platform': 'linux/amd64',
        'runtime_binding_fingerprint': binding.fingerprint,
        'consumer': consumer,
    }
    if backend == 'aws_vm':
        placement.update(host_image_id=dict(
            binding.qualified_node_images)[region],
                         runtime_principal=binding.principals[0],
                         instance_profile=binding.instance_profile)
    else:
        qualified = models.qualified_eks_cluster_for_target(
            target, binding, region)
        placement.update(kubernetes_cluster_arn=qualified.cluster_arn,
                         kubernetes_node_role=qualified.node_role,
                         kubernetes_node_selector=list(qualified.node_selector))
    return placement


def _warming_demand(
    active: topology_state.ProfileRevisionRecord,
    publication_record: catalog_state.PublicationRecord,
    location: topology_state.LocationRecord,
    profile: models.ManagedRegistryProfile,
    *,
    owner: str = 'boltz-l4:v7',
    consumer_kind: str = 'service_version',
    controller_epoch: str = 'service:boltz-l4:v7',
    controller_sequence: int | None = 7,
    allow_epoch_advance: bool = False,
    request_id: str = 'request-1',
    consumer_metadata: dict[str, Any] | None = None,
    backend: str = 'aws_vm',
    placement_region: str | None = None,
    now: int = 50,
) -> demand_state.DemandRecord:
    assert publication_record.image_id is not None
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    target = next(target for target in (profile.canonical,) + profile.targets
                  if target.target_fingerprint == location.target_fingerprint)
    consumer = {'request_id': request_id}
    if consumer_metadata is not None:
        consumer.update(consumer_metadata)
    region = placement_region or target.region
    return transactions.create_warming_demand_for_controller_epoch(
        authority_id=authority,
        workspace='research',
        consumer_kind=consumer_kind,
        consumer_owner=owner,
        controller_epoch=controller_epoch,
        controller_sequence=controller_sequence,
        allow_epoch_advance=allow_epoch_advance,
        target_key=(f'{publication_record.image_id}:'
                    f'{target.target_fingerprint}'),
        image_id=publication_record.image_id,
        runtime_digest=_DIGEST,
        profile_revision_id=active.id,
        target_fingerprint=target.target_fingerprint,
        location_id=location.id,
        placement=_runtime_placement(profile,
                                     target,
                                     backend=backend,
                                     region=region,
                                     consumer=consumer),
        now=now)


def test_live_service_version_demand_evidence_covers_every_incarnation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    _warming_demand(active,
                    publication_record,
                    regional,
                    profile,
                    owner='svc:incarnation:old-hash:v2',
                    controller_epoch='service:old-hash:v2',
                    controller_sequence=2,
                    request_id='old-unscoped')
    _warming_demand(active,
                    publication_record,
                    regional,
                    profile,
                    owner='svc:incarnation:legacy:colon-hash:v2',
                    controller_epoch='service:legacy-colon-hash:v2',
                    controller_sequence=2,
                    request_id='legacy-colon-hash',
                    now=51)
    _warming_demand(active,
                    publication_record,
                    regional,
                    profile,
                    owner='svc:incarnation:new-hash:v2:target:target-scope',
                    controller_epoch='service:new-hash:v2',
                    controller_sequence=2,
                    request_id='new-target',
                    now=52)
    _warming_demand(active,
                    publication_record,
                    regional,
                    profile,
                    owner='svc:incarnation:new-hash:v20',
                    controller_epoch='service:new-hash:v20',
                    controller_sequence=20,
                    request_id='different-version',
                    now=53)
    _warming_demand(active,
                    publication_record,
                    regional,
                    profile,
                    owner='svc-shadow:incarnation:new-hash:v2',
                    controller_epoch='service:new-hash:v2-shadow',
                    controller_sequence=2,
                    request_id='different-service',
                    now=54)

    all_incarnations = (
        demand_state.get_live_service_version_demand_evidence_any_incarnation(
            'svc', 2))
    exact_current = demand_state.get_live_service_version_demand_evidence(
        'svc', 2, 'new-hash')

    assert all_incarnations.count == 3
    assert exact_current.count == 1
    assert all_incarnations.digest != exact_current.digest

    monkeypatch.setattr(demand_state,
                        '_MAX_SERVICE_VERSION_DEMAND_EVIDENCE_ROWS', 1)
    with pytest.raises(RuntimeError, match='explicit row bound'):
        demand_state.get_live_service_version_demand_evidence_any_incarnation(
            'svc', 2)

    monkeypatch.setattr(demand_state,
                        '_MAX_SERVICE_VERSION_DEMAND_EVIDENCE_ROWS', 0)
    with pytest.raises(RuntimeError, match='explicit row bound'):
        demand_state.get_live_service_version_demand_evidence(
            'svc', 2, 'new-hash')


def _refresh_runtime_attestation(
    active: topology_state.ProfileRevisionRecord,
    profile: models.ManagedRegistryProfile,
    target: models.ManagedRegistryTarget,
    *,
    backend: str,
    runtime_id: str,
    now: int,
) -> topology_state.ProfileRevisionRecord:
    binding_id = target.runtime_binding(backend)
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    key = models.profile_attestation_key('runtime', target.name, backend,
                                         binding.fingerprint, runtime_id)
    evidence = dict(active.attestations[key])
    evidence['observed_at'] = now
    return topology_state.record_profile_attestation(
        profile_revision_id=active.id,
        kind=key,
        evidence=evidence,
        expected_generation=active.desired_generation,
        expected_config_hash=active.config_hash,
        now=now)


def _pull_plan(active: topology_state.ProfileRevisionRecord,
               location: topology_state.LocationRecord) -> dict[str, Any]:
    configured = models.ManagedRegistryProfile.from_snapshot(
        active.config_snapshot)
    target = next(target for target in (configured.canonical,) +
                  configured.targets
                  if target.target_fingerprint == location.target_fingerprint)
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    binding = configured.bindings[binding_id]
    return {
        'version': 1,
        'reference': location.target_ref,
        'runtime_digest': _DIGEST,
        'platform': 'linux/amd64',
        'distribution': configured.name,
        'profile_revision_id': active.id,
        'target_id': target.name,
        'target_fingerprint': location.target_fingerprint,
        'auth_strategy': 'ecr_runtime_identity',
        'credential_helper': 'ecr-login',
        'runtime_principal': binding.principals[0],
        'instance_profile': binding.instance_profile,
        'kubernetes_node_selector': [],
    }


def _cluster_handle_for_demand(
    active: topology_state.ProfileRevisionRecord,
    publication_record: catalog_state.PublicationRecord,
    location: topology_state.LocationRecord,
    profile: models.ManagedRegistryProfile,
    demand: demand_state.DemandRecord,
    *,
    controller_epoch: str,
) -> types.SimpleNamespace:
    pull_plan = _pull_plan(active, location)
    assert publication_record.image_id is not None
    resolved = models.ResolvedContainerImage(
        image_id=publication_record.image_id,
        reference=location.target_ref,
        target_id=str(pull_plan['target_id']),
        digest=_DIGEST,
        auth_strategy='ecr_runtime_identity',
        location_id=location.id,
        distribution=profile.name,
        profile_revision=active.revision,
        policy_fingerprint='a' * 64,
        profile_revision_id=active.id,
        target_fingerprint=location.target_fingerprint,
        demand_id=demand.id,
        demand_generation=demand.consumer_generation,
        controller_epoch=controller_epoch,
        owner_epoch=demand.owner_epoch,
        credential_helper='ecr-login',
        runtime_principal=pull_plan['runtime_principal'],
        instance_profile=pull_plan['instance_profile'])
    resources = types.SimpleNamespace(
        container_image=models.ContainerImage(release='boltz-l4',
                                              distribution=profile.name),
        resolved_container_image=resolved,
        container_image_from_legacy_image_id=False,
        cloud=None,
        region=None,
        zone=None,
        instance_type=None,
        docker_login_config=None)
    return types.SimpleNamespace(launched_resources=resources,
                                 launched_nodes=1,
                                 cached_cluster_info=None)


def test_publication_is_invisible_until_canonical_ready_and_replay_converges(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)

    publication_record, location = _publish_and_bind(profile)
    assert catalog_state.get_ready_release('boltz-l4', 'research') is None
    ready_location = _complete_location(location, now=30)
    assert ready_location.state == models.ImageLocationState.READY

    ready_release = catalog_state.get_ready_release('boltz-l4', 'research')
    assert ready_release is not None
    assert ready_release.id == publication_record.id
    operation = catalog_state.get_operation(publication_record.operation_id,
                                            'research')
    assert operation is not None
    assert operation.state == models.ImageOperationState.SUCCEEDED

    replay = publication.publish(source_ref=_SOURCE,
                                 release='boltz-l4',
                                 distribution=profile.name,
                                 workspace='research',
                                 actor_hash='1' * 64,
                                 idempotency_key='publication-idempotency-0001')
    assert replay.publication.id == publication_record.id

    same_release = publication.publish(
        source_ref=_SOURCE,
        release='boltz-l4',
        distribution=profile.name,
        workspace='research',
        actor_hash='1' * 64,
        idempotency_key='publication-idempotency-0002')
    assert same_release.publication.id == publication_record.id
    with pytest.raises(catalog_state.ReleaseConflictError):
        publication.publish(source_ref=_OTHER_SOURCE,
                            release='boltz-l4',
                            distribution=profile.name,
                            workspace='research',
                            actor_hash='1' * 64,
                            idempotency_key='publication-idempotency-0003')


def test_concurrent_same_release_publications_create_one_reservation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    barrier = threading.Barrier(8)

    def publish_one(index: int) -> str:
        barrier.wait(timeout=10)
        result = publication.publish(
            source_ref=_SOURCE,
            release='concurrent-release',
            distribution=profile.name,
            workspace='research',
            actor_hash='1' * 64,
            idempotency_key=f'concurrent-publication-{index:04d}')
        return result.publication.id

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        ids = list(executor.map(publish_one, range(8)))
    assert len(set(ids)) == 1
    rows = catalog_state.list_workspace_publications('research', limit=20)
    assert [row.requested_release for row in rows] == ['concurrent-release']


def test_demand_fences_eviction_until_two_terminal_observations(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    west = profile.targets[0]
    demand = _warming_demand(active, publication_record, regional, profile)
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None and publication_record.image_id is not None
    replay = transactions.create_warming_demand_for_controller_epoch(
        authority_id=authority,
        workspace='research',
        consumer_kind='service_version',
        consumer_owner='boltz-l4:v7',
        controller_epoch='service:boltz-l4:v7',
        controller_sequence=7,
        allow_epoch_advance=False,
        target_key=f'{publication_record.image_id}:{west.target_fingerprint}',
        image_id=publication_record.image_id,
        runtime_digest=_DIGEST,
        profile_revision_id=active.id,
        target_fingerprint=west.target_fingerprint,
        location_id=regional.id,
        placement=_runtime_placement(profile,
                                     west,
                                     backend='aws_vm',
                                     region=west.region,
                                     consumer={'request_id': 'request-1'}),
        now=51)
    assert replay.id == demand.id and replay.consumer_generation == 0
    demand = transactions.commit_ready_demand(
        demand_id=demand.id,
        consumer_generation=demand.consumer_generation,
        pull_plan=_pull_plan(active, regional),
        now=52)
    assert demand.state == models.ImageDemandState.READY
    assert topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                              unused_before=1000,
                                              lease_seconds=60,
                                              now=100) is None

    assert not demand_state.observe_consumer_terminal(
        demand.id, 'research', authoritative=True, now=100)
    assert not demand_state.observe_consumer_terminal(
        demand.id, 'research', authoritative=True, now=3699)
    assert demand_state.observe_consumer_terminal(demand.id,
                                                  'research',
                                                  authoritative=True,
                                                  now=3700)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=3701)
    assert eviction is not None and eviction.id == regional.id
    assert eviction.lease_token is not None
    shard_before = topology_state.get_shard(regional.shard_id)
    assert shard_before is not None
    eviction = _begin_delete(eviction, now=3701)
    eviction = _mark_readback(eviction, now=3701)
    evicted = topology_state.complete_eviction(eviction.id,
                                               eviction.lease_token,
                                               present=False,
                                               now=3702)
    assert evicted is not None
    assert evicted.state == models.ImageLocationState.EVICTED
    assert evicted.reserved_declared_bytes == 0
    shard_after = topology_state.get_shard(regional.shard_id)
    assert shard_after is not None
    assert shard_after.reserved_manifests == shard_before.reserved_manifests - 1
    assert (shard_after.reserved_declared_bytes ==
            shard_before.reserved_declared_bytes -
            regional.reserved_declared_bytes)
    retried = topology_state.retry_location(regional.id, 'research', now=3703)
    assert retried is not None
    assert retried.state == models.ImageLocationState.PENDING
    assert retried.reserved_declared_bytes == regional.reserved_declared_bytes
    restored_shard = topology_state.get_shard(regional.shard_id)
    assert restored_shard is not None
    assert restored_shard.reserved_manifests == shard_before.reserved_manifests
    assert (restored_shard.reserved_declared_bytes ==
            shard_before.reserved_declared_bytes)


def test_terminal_observation_samples_database_clock_after_owner_locks(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='clocked-service:v1',
                             consumer_kind='service_version',
                             controller_epoch='service:clocked-service:v1',
                             controller_sequence=1,
                             consumer_metadata={
                                 'workload_type': 'service',
                                 'workload_id': 'clocked-service',
                                 'workload_task_id': 1,
                                 'service_hash': 'clocked-service-hash',
                             })
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    started = int(
        lock_connection.execute(
            sqlalchemy.select(
                catalog_state.database_epoch_expression())).scalar_one())
    lock_connection.execute(
        schema.demands.update().where(schema.demands.c.id == demand.id).values(
            first_terminal_observed_at=started - 3599,
            last_terminal_observed_at=started - 3599,
            terminal_observation_count=1,
            updated_at=started - 3599))
    lock_connection.execute(
        sqlalchemy.select(schema.consumer_watermarks.c.consumer_owner).where(
            schema.consumer_watermarks.c.workspace == 'research',
            schema.consumer_watermarks.c.consumer_kind == 'service_version',
            schema.consumer_watermarks.c.consumer_owner ==
            demand.consumer_owner).with_for_update()).one()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(demand_state.observe_consumer_terminal,
                                     demand.id,
                                     'research',
                                     authoritative=True)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(2.1)')
            lock_transaction.commit()
            assert future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    released = demand_state.get_demand(demand.id, 'research')
    assert released is not None
    assert released.state == models.ImageDemandState.RELEASED
    assert released.terminal_at is not None
    assert released.terminal_at >= started + 2


def test_cluster_request_terminal_lookup_is_index_bounded(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='target-cluster',
                             consumer_kind='cluster',
                             request_id='request-target')
    assert demand.request_id == 'request-target'

    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_demands (
                    id, authority_id, workspace, consumer_kind,
                    consumer_owner, request_id, consumer_generation,
                    target_key, owner_epoch, image_id, runtime_digest,
                    profile_revision_id, target_fingerprint, location_id,
                    placement_json, state, consumer_attached, created_at,
                    updated_at
                )
                SELECT
                    md5('unrelated-demand-' || series::text), authority_id,
                    workspace, 'cluster', 'cluster-' || series::text,
                    'request-' || series::text, 0, target_key, series,
                    image_id, runtime_digest, profile_revision_id,
                    target_fingerprint, location_id, placement_json, 'WARMING',
                    false, created_at, updated_at
                FROM container_image_demands
                CROSS JOIN generate_series(1, 20000) AS series
                WHERE id = :demand_id
            """), {'demand_id': demand.id})
        connection.execute(sqlalchemy.text('ANALYZE container_image_demands'))
        plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id
                FROM container_image_demands
                WHERE consumer_kind = 'cluster'
                  AND consumer_attached IS false
                  AND request_id = 'request-target'
                  AND state IN ('WARMING', 'READY', 'FAILED')
                FOR UPDATE
            """)).scalars().all()
        reconciliation_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_demands
                WHERE state IN ('WARMING', 'READY', 'FAILED')
                  AND updated_at <= 100
                ORDER BY updated_at, id
                LIMIT 500
            """)).scalars().all()
        compaction_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT demand.id
                FROM container_image_demands AS demand
                JOIN container_image_consumer_watermarks AS watermark
                  ON watermark.workspace = demand.workspace
                 AND watermark.consumer_kind = demand.consumer_kind
                 AND watermark.consumer_owner = demand.consumer_owner
                WHERE demand.state IN ('SUPERSEDED', 'RELEASED')
                  AND demand.expires_at <= 100
                  AND watermark.owner_deleted_at IS NOT NULL
                  AND demand.consumer_generation
                      <= watermark.max_terminal_generation
                ORDER BY demand.expires_at, demand.id
                LIMIT 500
            """)).scalars().all()
    assert 'ix_container_image_demands_cluster_request' in str(plan)
    assert ('ix_container_image_demands_reconciliation_queue'
            in str(reconciliation_plan))
    assert ('ix_container_image_demands_compaction_queue'
            in str(compaction_plan))

    assert demand_state.mark_cluster_request_terminal('request-target',
                                                      now=100) == 1
    with image_database.connect() as connection:
        observed, unrelated = connection.execute(
            sqlalchemy.text("""
                SELECT
                    count(*) FILTER (
                        WHERE first_terminal_observed_at IS NOT NULL),
                    count(*) FILTER (
                        WHERE request_id <> 'request-target'
                          AND first_terminal_observed_at IS NOT NULL)
                FROM container_image_demands
            """)).one()
    assert observed == 1
    assert unrelated == 0


def test_artifact_demand_history_is_index_bounded_for_sparse_and_dense_images(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    seed = _warming_demand(active,
                           publication_record,
                           regional,
                           profile,
                           owner='demand-history-sparse',
                           controller_epoch='service:demand-history-sparse',
                           controller_sequence=1)
    other_image_id = 'demand-history-dense-image'
    other_location_id = 'demand-history-dense-location'
    other_digest = 'sha256:' + 'd' * 64
    with image_database.begin() as connection:
        # The 100,000-row population is test-fixture setup rather than the
        # bounded production query under proof.  Shared CI PostgreSQL can exceed
        # the connection's 15-second default while enforcing the foreign keys
        # under parallel load, so bound this setup transaction independently.
        connection.execute(
            sqlalchemy.text("SET LOCAL statement_timeout = '60s'"))
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_images (
                    id, workspace, runtime_digest, platform, config_digest,
                    manifest_media_type, manifest_size_bytes,
                    declared_size_bytes, creator_user_hash, producer_kind,
                    producer_spec_hash, builder_version, created_at, updated_at
                )
                SELECT :other_image_id, workspace, :other_digest, platform,
                       :other_config_digest, manifest_media_type,
                       manifest_size_bytes, declared_size_bytes,
                       creator_user_hash, producer_kind, producer_spec_hash,
                       builder_version, created_at, updated_at
                FROM container_images
                WHERE id = :seed_image_id
            """), {
                'other_image_id': other_image_id,
                'other_digest': other_digest,
                'other_config_digest': 'sha256:' + 'e' * 64,
                'seed_image_id': seed.image_id,
            })
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_locations (
                    id, workspace, image_id, shard_id, target_fingerprint,
                    physical_fingerprint, runtime_digest, canonical,
                    canonical_location_id, target_ref, state, attempt_count,
                    last_verified_at, reserved_declared_bytes, created_at,
                    updated_at
                )
                SELECT :other_location_id, workspace, :other_image_id,
                       shard_id, target_fingerprint, physical_fingerprint,
                       :other_digest, TRUE, NULL,
                       'registry.example/demand-history@' || :other_digest,
                       'READY', 0, 10, reserved_declared_bytes, 10, 10
                FROM container_image_locations
                WHERE id = :seed_location_id
            """), {
                'other_location_id': other_location_id,
                'other_image_id': other_image_id,
                'other_digest': other_digest,
                'seed_location_id': seed.location_id,
            })
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_demands (
                    id, authority_id, workspace, consumer_kind,
                    consumer_owner, consumer_generation, target_key,
                    owner_epoch, image_id, runtime_digest,
                    profile_revision_id, target_fingerprint, location_id,
                    placement_json, state, consumer_attached, created_at,
                    updated_at
                )
                SELECT md5('demand-history-' || series::text), authority_id,
                       workspace, 'service_version',
                       'demand-history-' || series::text, 0,
                       :other_image_id || ':' || target_fingerprint, series,
                       :other_image_id, :other_digest, profile_revision_id,
                       target_fingerprint, :other_location_id, placement_json,
                       'WARMING', FALSE, series, series
                FROM container_image_demands
                CROSS JOIN generate_series(1, 100000) AS series
                WHERE id = :seed_demand_id
            """), {
                'other_image_id': other_image_id,
                'other_digest': other_digest,
                'other_location_id': other_location_id,
                'seed_demand_id': seed.id,
            })
        connection.execute(sqlalchemy.text('ANALYZE container_image_demands'))

    # End the fixture-only timeout before proving the production query. A new
    # transaction must inherit the engine's ordinary 15-second ceiling.
    with image_database.begin() as connection:
        assert connection.execute(
            sqlalchemy.text('SHOW statement_timeout')).scalar_one() == '15s'
        plans = []
        for image_id in (seed.image_id, other_image_id):
            plans.append(
                connection.execute(
                    sqlalchemy.text("""
                        EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                        SELECT id
                        FROM container_image_demands
                        WHERE workspace = 'research' AND image_id = :image_id
                        ORDER BY created_at DESC, id DESC
                        LIMIT 51
                    """), {
                        'image_id': image_id
                    }).scalars().all())

    for plan in plans:
        rendered = '\n'.join(plan)
        assert 'ix_container_image_demands_artifact_history' in rendered
        assert 'Sort' not in rendered
    assert [
        record.id for record in demand_state.list_demands(
            seed.image_id, 'research', limit=51)
    ] == [seed.id]
    dense = demand_state.list_demands(other_image_id, 'research', limit=51)
    assert len(dense) == 51
    assert [record.created_at for record in dense
           ] == list(range(100000, 99949, -1))


def test_new_demand_uses_locked_database_qualification_time_and_replay_skips_age(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    observed_at = 12
    last_qualified_at = (
        observed_at + profile.qualification.runtime_attestation_max_age_seconds)
    validation_times: list[int] = []
    original_validate = transactions._validate_runtime_attestation

    def capture_validation_time(**kwargs: Any) -> None:
        validation_times.append(kwargs['now'])
        original_validate(**kwargs)

    monkeypatch.setattr(transactions, '_validate_runtime_attestation',
                        capture_validation_time)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='clock-boundary:v1',
                             controller_epoch='service:clock-boundary:v1',
                             controller_sequence=1,
                             now=last_qualified_at)

    assert validation_times == [last_qualified_at]
    assert demand.created_at == last_qualified_at

    target = next(item for item in (profile.canonical,) + profile.targets
                  if item.target_fingerprint == regional.target_fingerprint)
    refreshed_at = last_qualified_at + 1
    refreshed_active = _refresh_runtime_attestation(active,
                                                    profile,
                                                    target,
                                                    backend='aws_vm',
                                                    runtime_id=target.region,
                                                    now=refreshed_at)
    replay = _warming_demand(refreshed_active,
                             publication_record,
                             regional,
                             profile,
                             owner='clock-boundary:v1',
                             controller_epoch='service:clock-boundary:v1',
                             controller_sequence=1,
                             now=last_qualified_at + 2)
    assert replay.id == demand.id
    assert replay.created_at == last_qualified_at
    assert validation_times == [last_qualified_at]

    stale_owner = 'clock-boundary:v2'
    stale_now = (refreshed_at +
                 profile.qualification.runtime_attestation_max_age_seconds + 1)
    with pytest.raises(transactions.DemandQualificationStaleError,
                       match='QUALIFICATION_STALE'):
        _warming_demand(refreshed_active,
                        publication_record,
                        regional,
                        profile,
                        owner=stale_owner,
                        controller_epoch='service:clock-boundary:v2',
                        controller_sequence=2,
                        now=stale_now)
    assert validation_times == [last_qualified_at, stale_now]
    with image_database.connect() as connection:
        demand_rows = connection.execute(
            sqlalchemy.select(schema.demands.c.id).where(
                schema.demands.c.consumer_owner == stale_owner)).all()
        watermark_rows = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.consumer_owner).where(
                    schema.consumer_watermarks.c.consumer_owner ==
                    stale_owner)).all()
    assert demand_rows == []
    assert watermark_rows == []


def test_unattached_request_terminal_proof_survives_age_gate(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='orphan-cluster:incarnation:request-proof',
                             consumer_kind='cluster',
                             controller_epoch='cluster-request:request-proof',
                             controller_sequence=None,
                             request_id='request-proof',
                             consumer_metadata={
                                 'workload_type': 'cluster',
                                 'workload_id': 'orphan-cluster',
                             },
                             now=50)
    assert demand_state.mark_cluster_request_terminal('request-proof',
                                                      now=100) == 1
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})

    assert lifecycle_worker_service._reconcile_terminal_consumers(200) == 0
    preserved = demand_state.get_demand(demand.id, 'research')
    assert preserved is not None
    assert preserved.first_terminal_observed_at == 100
    assert preserved.last_terminal_observed_at == 100
    assert preserved.terminal_observation_count == 1

    after_age_gate = (
        demand.created_at +
        lifecycle_worker_service._UNATTACHED_REQUEST_RETENTION_SECONDS + 1)
    assert lifecycle_worker_service._reconcile_terminal_consumers(
        after_age_gate) == 1
    released = demand_state.get_demand(demand.id, 'research')
    assert released is not None
    assert released.state == models.ImageDemandState.RELEASED
    with image_database.connect() as connection:
        owner_deleted_at = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.owner_deleted_at).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind == 'cluster',
                    schema.consumer_watermarks.c.consumer_owner ==
                    demand.consumer_owner)).scalar_one()
    assert owner_deleted_at == after_age_gate


def test_restart_stable_owner_epoch_reuses_one_target_fence(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    first = _warming_demand(active,
                            publication_record,
                            regional,
                            profile,
                            request_id='request-before-restart',
                            now=50)
    replay = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             request_id='request-before-restart',
                             now=51)

    assert replay.id == first.id
    assert replay.consumer_generation == 0
    assert replay.placement['consumer'][
        'request_id'] == 'request-before-restart'
    current = demand_state.get_current_demand_for_controller_epoch(
        workspace='research',
        consumer_kind=first.consumer_kind,
        consumer_owner=first.consumer_owner,
        controller_epoch='service:boltz-l4:v7')
    assert current is not None and current.id == first.id


def test_authorized_controller_epoch_advance_is_atomic_and_monotonic(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    first = _warming_demand(active,
                            publication_record,
                            regional,
                            profile,
                            controller_epoch='managed-job:42:task:0:recovery:0',
                            controller_sequence=0)

    with pytest.raises(demand_state.StaleConsumerGenerationError):
        _warming_demand(active,
                        publication_record,
                        regional,
                        profile,
                        controller_epoch='managed-job:42:task:0:recovery:1',
                        controller_sequence=1,
                        allow_epoch_advance=False,
                        now=51)

    successor = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        controller_epoch='managed-job:42:task:0:recovery:1',
        controller_sequence=1,
        allow_epoch_advance=True,
        now=52)
    assert successor.id != first.id
    assert successor.owner_epoch == first.owner_epoch + 1
    assert successor.consumer_generation == first.consumer_generation + 1
    superseded = demand_state.get_demand(first.id, 'research')
    assert superseded is not None
    assert superseded.state == models.ImageDemandState.SUPERSEDED
    assert demand_state.get_current_demand_for_controller_epoch(
        workspace='research',
        consumer_kind=first.consumer_kind,
        consumer_owner=first.consumer_owner,
        controller_epoch='managed-job:42:task:0:recovery:0') is None
    replay = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        controller_epoch='managed-job:42:task:0:recovery:1',
        controller_sequence=1,
        allow_epoch_advance=True,
        now=53)
    assert replay.id == successor.id
    with pytest.raises(demand_state.StaleConsumerGenerationError,
                       match='cannot move backward'):
        _warming_demand(active,
                        publication_record,
                        regional,
                        profile,
                        controller_epoch='managed-job:42:task:0:recovery:0',
                        controller_sequence=0,
                        allow_epoch_advance=True,
                        now=54)
    with pytest.raises(demand_state.StaleConsumerGenerationError):
        transactions.commit_ready_demand(
            demand_id=first.id,
            consumer_generation=first.consumer_generation,
            pull_plan=_pull_plan(active, regional),
            now=55)


def test_authoritative_cluster_deletion_releases_demand_immediately(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='cluster-a:incarnation:launch-hash',
                             consumer_kind='cluster',
                             controller_epoch='cluster-request:launch-a',
                             controller_sequence=None)

    assert demand_state.release_demand_authoritatively(demand.id,
                                                       'research',
                                                       now=51)
    released = demand_state.get_demand(demand.id, 'research')
    assert released is not None
    assert released.state == models.ImageDemandState.RELEASED


def test_stale_authoritative_release_cannot_retire_a_live_generation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    owner = '42:task:0'
    stale = _warming_demand(active,
                            publication_record,
                            regional,
                            profile,
                            owner=owner,
                            consumer_kind='managed_job_task',
                            controller_epoch='managed-job:42:task:0:recovery:0',
                            controller_sequence=0)
    assert demand_state.supersede_demand(stale.id, 'research', now=51)
    current = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        owner=owner,
        consumer_kind='managed_job_task',
        controller_epoch='managed-job:42:task:0:recovery:1',
        controller_sequence=1,
        allow_epoch_advance=True,
        request_id='request-2',
        now=52)

    assert not demand_state.release_demand_authoritatively(
        stale.id, 'research', now=53)
    preserved = demand_state.get_demand(current.id, 'research')
    assert preserved is not None
    assert preserved.state == models.ImageDemandState.WARMING
    with image_database.connect() as connection:
        owner_deleted_at = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.owner_deleted_at).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind ==
                    'managed_job_task',
                    schema.consumer_watermarks.c.consumer_owner ==
                    owner)).scalar_one()
    assert owner_deleted_at is None


def test_cluster_init_persists_validated_consumer_binding(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='cluster-a:incarnation:launch-hash',
                             consumer_kind='cluster',
                             controller_epoch='cluster-request:launch-a',
                             controller_sequence=None)
    pull_plan = _pull_plan(active, regional)
    demand = transactions.commit_ready_demand(
        demand_id=demand.id,
        consumer_generation=demand.consumer_generation,
        pull_plan=pull_plan,
        now=51)
    handle = _cluster_handle_for_demand(
        active,
        publication_record,
        regional,
        profile,
        demand,
        controller_epoch='cluster-request:launch-a')
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    global_user_state.cluster_history_table.create(image_database,
                                                   checkfirst=True)
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)
    monkeypatch.setattr(global_user_state.skypilot_config,
                        'get_active_workspace', lambda: 'research')

    global_user_state.add_or_update_cluster('cluster-a',
                                            handle,
                                            requested_resources=None,
                                            ready=False)
    partial_handle = types.SimpleNamespace(launched_resources=None,
                                           launched_nodes=1,
                                           cached_cluster_info=None)
    global_user_state.add_or_update_cluster('cluster-a',
                                            partial_handle,
                                            requested_resources=None,
                                            ready=True,
                                            is_launch=False)

    direct_resources = types.SimpleNamespace(
        container_image=None,
        resolved_container_image=None,
        container_image_from_legacy_image_id=False,
        cloud=None,
        region=None,
        zone=None,
        instance_type=None,
        docker_login_config=None)
    direct_handle = types.SimpleNamespace(launched_resources=direct_resources,
                                          launched_nodes=1,
                                          cached_cluster_info=None)
    global_user_state.add_or_update_cluster('cluster-direct',
                                            direct_handle,
                                            requested_resources=None,
                                            ready=False)
    with image_database.begin() as connection:
        connection.execute(global_user_state.cluster_table.insert().values(
            name='cluster-legacy', status='UP', workspace='research'))

    assert global_user_state.get_cluster_image_consumers(
        ['cluster-a', 'cluster-direct', 'cluster-legacy']) == {
            'cluster-a': ('cluster', demand.consumer_owner),
            'cluster-direct': (None, None),
            'cluster-legacy': None,
        }


def test_cluster_init_serializes_with_missing_row_reconciliation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    controller_epoch = 'cluster-request:launch-a'
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='cluster-a:incarnation:launch-hash',
                             consumer_kind='cluster',
                             controller_epoch=controller_epoch,
                             controller_sequence=None,
                             consumer_metadata={
                                 'workload_type': 'cluster',
                                 'workload_id': 'cluster-a',
                             })
    demand = transactions.commit_ready_demand(
        demand_id=demand.id,
        consumer_generation=demand.consumer_generation,
        pull_plan=_pull_plan(active, regional),
        now=51)
    assert demand_state.attach_consumer(demand.id, 'research', now=52)
    assert not demand_state.observe_consumer_terminal(
        demand.id, 'research', authoritative=True, now=100)
    handle = _cluster_handle_for_demand(active,
                                        publication_record,
                                        regional,
                                        profile,
                                        demand,
                                        controller_epoch=controller_epoch)
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    global_user_state.cluster_history_table.create(image_database,
                                                   checkfirst=True)
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)
    monkeypatch.setattr(global_user_state.skypilot_config,
                        'get_active_workspace', lambda: 'research')
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})

    init_inserted_uncommitted = threading.Event()
    allow_init_to_commit = threading.Event()
    reconciliation_attempted_lock = threading.Event()
    reconciliation_backend: dict[str, int] = {}

    def _pause_init_after_cluster_insert(_connection, _cursor, statement,
                                         _parameters, _context,
                                         _executemany) -> None:
        normalized = ' '.join(statement.split()).upper()
        if (threading.current_thread().name.startswith('cluster-init') and
                normalized.startswith('INSERT INTO CLUSTERS ')):
            init_inserted_uncommitted.set()
            if not allow_init_to_commit.wait(timeout=10):
                raise TimeoutError('Cluster INIT race test timed out.')

    def _observe_reconciliation_lock(_connection, _cursor, statement,
                                     _parameters, _context,
                                     _executemany) -> None:
        normalized = ' '.join(statement.split()).upper()
        if (threading.current_thread().name.startswith('cluster-reconcile') and
                'PG_ADVISORY_XACT_LOCK' in normalized):
            reconciliation_backend['pid'] = int(
                _cursor.connection.get_backend_pid())
            reconciliation_attempted_lock.set()

    sqlalchemy.event.listen(image_database, 'after_cursor_execute',
                            _pause_init_after_cluster_insert)
    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            _observe_reconciliation_lock)
    init_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='cluster-init')
    reconcile_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='cluster-reconcile')
    try:
        init_future = init_executor.submit(
            global_user_state.add_or_update_cluster,
            'cluster-a',
            handle,
            requested_resources=None,
            ready=False)
        assert init_inserted_uncommitted.wait(timeout=5)
        reconcile_future = reconcile_executor.submit(
            lifecycle_worker_service._reconcile_terminal_consumers, 4000)
        assert reconciliation_attempted_lock.wait(timeout=5)
        assert _wait_for_backend_lock(image_database,
                                      reconciliation_backend['pid'])
        allow_init_to_commit.set()

        cluster_hash = init_future.result(timeout=5)
        assert isinstance(cluster_hash, str)
        assert cluster_hash
        assert reconcile_future.result(timeout=5) == 0
        current = demand_state.get_demand(demand.id, 'research')
        assert current is not None
        assert current.state == models.ImageDemandState.READY
        assert current.consumer_attached
        assert current.first_terminal_observed_at is None
        assert current.last_terminal_observed_at is None
        assert current.terminal_observation_count == 0
        assert global_user_state.get_cluster_image_consumers(['cluster-a']) == {
            'cluster-a': ('cluster', demand.consumer_owner),
        }
        with image_database.connect() as connection:
            stored_cluster_hash = connection.execute(
                sqlalchemy.select(
                    global_user_state.cluster_table.c.cluster_hash).where(
                        global_user_state.cluster_table.c.name ==
                        'cluster-a')).scalar_one()
            deleted_at = connection.execute(
                sqlalchemy.select(
                    schema.consumer_watermarks.c.owner_deleted_at).where(
                        schema.consumer_watermarks.c.workspace == 'research',
                        schema.consumer_watermarks.c.consumer_kind == 'cluster',
                        schema.consumer_watermarks.c.consumer_owner ==
                        demand.consumer_owner)).scalar_one()
        assert stored_cluster_hash == cluster_hash
        assert deleted_at is None
    finally:
        allow_init_to_commit.set()
        sqlalchemy.event.remove(image_database, 'after_cursor_execute',
                                _pause_init_after_cluster_insert)
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                _observe_reconciliation_lock)
        init_executor.shutdown(wait=True)
        reconcile_executor.shutdown(wait=True)


def test_cluster_row_and_demand_release_commit_atomically(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='cluster-a:incarnation:launch-hash',
                             consumer_kind='cluster',
                             controller_epoch='cluster-request:launch-a',
                             controller_sequence=None)
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    with image_database.begin() as connection:
        connection.execute(global_user_state.cluster_table.insert().values(
            name='cluster-a',
            handle=b'unreadable-bound-cluster-handle',
            status='UP',
            workspace='research',
            container_image_binding_known=1,
            container_image_consumer_kind='cluster',
            container_image_consumer_owner=demand.consumer_owner))
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)
    monkeypatch.setattr(global_user_state.time, 'time', lambda: 100)
    with image_database.connect() as connection:
        before = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())

    global_user_state.remove_cluster('cluster-a', terminate=True)

    with image_database.connect() as connection:
        after = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())
        assert connection.execute(
            sqlalchemy.select(global_user_state.cluster_table.c.name).where(
                global_user_state.cluster_table.c.name ==
                'cluster-a')).first() is None
    released = demand_state.get_demand(demand.id, 'research')
    assert released is not None
    assert released.state == models.ImageDemandState.RELEASED
    with image_database.connect() as connection:
        watermark = connection.execute(
            sqlalchemy.select(schema.consumer_watermarks).where(
                schema.consumer_watermarks.c.workspace == 'research',
                schema.consumer_watermarks.c.consumer_kind == 'cluster',
                schema.consumer_watermarks.c.consumer_owner ==
                demand.consumer_owner)).mappings().one()
    owner_deleted_at = int(watermark['owner_deleted_at'])
    assert before <= owner_deleted_at <= after
    assert owner_deleted_at != 100
    assert demand_state.compact_terminal_demands(
        now=owner_deleted_at + demand_state._TERMINAL_RETENTION_SECONDS) == 1
    assert demand_state.get_demand(demand.id, 'research') is None
    with image_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.consumer_owner).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind == 'cluster',
                    schema.consumer_watermarks.c.consumer_owner == demand.
                    consumer_owner)).scalar_one() == demand.consumer_owner
    recreated = _warming_demand(active,
                                publication_record,
                                regional,
                                profile,
                                owner='cluster-a:incarnation:new-launch-hash',
                                consumer_kind='cluster',
                                controller_epoch='cluster-request:launch-b',
                                controller_sequence=None,
                                request_id='launch-b',
                                now=101)
    assert recreated.state == models.ImageDemandState.WARMING
    assert recreated.consumer_owner != demand.consumer_owner


def test_corrupt_legacy_handle_does_not_block_cluster_deletion(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='cluster-a:incarnation:launch-hash',
                             consumer_kind='cluster',
                             controller_epoch='cluster-request:launch-a',
                             controller_sequence=None)
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    with image_database.begin() as connection:
        connection.execute(global_user_state.cluster_table.insert().values(
            name='cluster-a',
            handle=b'unreadable-pre-binding-cluster-handle',
            status='UP',
            workspace='research',
            container_image_binding_known=0))
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)

    global_user_state.remove_cluster('cluster-a', terminate=True)

    with image_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(global_user_state.cluster_table.c.name).where(
                global_user_state.cluster_table.c.name ==
                'cluster-a')).first() is None
        owner_deleted_at = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.owner_deleted_at).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind == 'cluster',
                    schema.consumer_watermarks.c.consumer_owner ==
                    demand.consumer_owner)).scalar_one()
    retained = demand_state.get_demand(demand.id, 'research')
    assert retained is not None
    assert retained.state == models.ImageDemandState.WARMING
    assert owner_deleted_at is None


def test_replica_cluster_deletion_preserves_shared_job_owner(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        owner='42:task:0',
        consumer_kind='managed_job_task',
        controller_epoch='managed-job:42:task:0:recovery:0',
        controller_sequence=0)
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    with image_database.begin() as connection:
        connection.execute(global_user_state.cluster_table.insert().values(
            name='managed-job-replica',
            handle=b'unreadable-bound-replica-handle',
            status='UP',
            workspace='research',
            container_image_binding_known=1,
            container_image_consumer_kind='managed_job_task',
            container_image_consumer_owner=demand.consumer_owner))
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)

    global_user_state.remove_cluster('managed-job-replica', terminate=True)

    with image_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(global_user_state.cluster_table.c.name).where(
                global_user_state.cluster_table.c.name ==
                'managed-job-replica')).first() is None
        watermark = connection.execute(
            sqlalchemy.select(schema.consumer_watermarks).where(
                schema.consumer_watermarks.c.workspace == 'research',
                schema.consumer_watermarks.c.consumer_kind ==
                'managed_job_task', schema.consumer_watermarks.c.consumer_owner
                == demand.consumer_owner)).mappings().one()
    assert watermark['owner_deleted_at'] is None
    preserved = demand_state.get_demand(demand.id, 'research')
    assert preserved is not None
    assert preserved.state == models.ImageDemandState.WARMING


def test_same_name_recreation_cannot_mask_an_orphaned_cluster_incarnation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    first = _warming_demand(active,
                            publication_record,
                            regional,
                            profile,
                            owner='cluster-a:incarnation:first-launch-hash',
                            consumer_kind='cluster',
                            controller_epoch='cluster-request:first',
                            controller_sequence=None,
                            consumer_metadata={
                                'workload_type': 'cluster',
                                'workload_id': 'cluster-a',
                            },
                            now=50)
    assert demand_state.attach_consumer(first.id, 'research', now=51)
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)
    monkeypatch.setattr(lifecycle_worker_service.managed_job_state,
                        'get_job_task_terminal_states', lambda _: {})
    monkeypatch.setattr(lifecycle_worker_service.serve_state,
                        'get_service_version_terminal_states', lambda _: {})

    assert lifecycle_worker_service._reconcile_terminal_consumers(200) == 0
    observed = demand_state.get_demand(first.id, 'research')
    assert observed is not None
    assert observed.first_terminal_observed_at == 200

    with image_database.begin() as connection:
        connection.execute(global_user_state.cluster_table.insert().values(
            name='cluster-a',
            status='INIT',
            workspace='research',
            container_image_binding_known=1,
            container_image_consumer_kind='cluster',
            container_image_consumer_owner=first.consumer_owner))
    assert lifecycle_worker_service._reconcile_terminal_consumers(300) == 0
    current = demand_state.get_demand(first.id, 'research')
    assert current is not None
    assert current.first_terminal_observed_at is None
    assert current.last_terminal_observed_at is None
    assert current.terminal_observation_count == 0

    replacement = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        owner='cluster-a:incarnation:replacement-launch-hash',
        consumer_kind='cluster',
        controller_epoch='cluster-request:replacement',
        controller_sequence=None,
        request_id='replacement-request',
        consumer_metadata={
            'workload_type': 'cluster',
            'workload_id': 'cluster-a',
        },
        now=301)
    with image_database.begin() as connection:
        connection.execute(global_user_state.cluster_table.update().where(
            global_user_state.cluster_table.c.name == 'cluster-a').values(
                container_image_consumer_owner=replacement.consumer_owner))

    new_first_observation = 4000
    assert lifecycle_worker_service._reconcile_terminal_consumers(
        new_first_observation) == 0
    observed = demand_state.get_demand(first.id, 'research')
    assert observed is not None
    assert observed.state == models.ImageDemandState.WARMING
    assert observed.first_terminal_observed_at == new_first_observation

    final_observation = (
        new_first_observation +
        lifecycle_worker_service._TERMINAL_CONFIRMATION_SECONDS)
    assert lifecycle_worker_service._reconcile_terminal_consumers(
        final_observation) == 1
    released = demand_state.get_demand(first.id, 'research')
    assert released is not None
    assert released.state == models.ImageDemandState.RELEASED
    still_live = demand_state.get_demand(replacement.id, 'research')
    assert still_live is not None
    assert still_live.state == models.ImageDemandState.WARMING
    with image_database.connect() as connection:
        deleted_at = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.owner_deleted_at).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind == 'cluster',
                    schema.consumer_watermarks.c.consumer_owner ==
                    first.consumer_owner)).scalar_one()
        replacement_deleted_at = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.owner_deleted_at).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind == 'cluster',
                    schema.consumer_watermarks.c.consumer_owner ==
                    replacement.consumer_owner)).scalar_one()
    assert deleted_at == final_observation
    assert replacement_deleted_at is None
    assert demand_state.compact_terminal_demands(
        now=final_observation + demand_state._TERMINAL_RETENTION_SECONDS) == 1
    assert demand_state.get_demand(first.id, 'research') is None
    assert demand_state.get_demand(replacement.id, 'research') is not None


def test_job_and_serve_reconciliation_retire_and_compact_owners(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    job_identity = (42, 0)
    service_identity = ('boltz-l4', 1, 'service-hash')
    job = _warming_demand(active,
                          publication_record,
                          regional,
                          profile,
                          owner='42:task:0',
                          consumer_kind='managed_job_task',
                          controller_epoch='managed-job:42:task:0:recovery:0',
                          controller_sequence=0,
                          consumer_metadata={
                              'workload_type': 'managed_job',
                              'workload_id': '42',
                              'workload_task_id': 0,
                              'recovery_generation': 0,
                          })
    service = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        owner='boltz-l4:incarnation:service-hash:v1:target:scope',
        consumer_kind='service_version',
        controller_epoch='service:service-hash:v1',
        controller_sequence=1,
        request_id='request-2',
        consumer_metadata={
            'workload_type': 'service',
            'workload_id': 'boltz-l4',
            'workload_task_id': 1,
            'service_hash': 'service-hash',
        },
        now=51)
    assert not demand_state.observe_consumer_terminal(
        job.id, 'research', authoritative=True, now=60)
    assert not demand_state.observe_consumer_terminal(
        service.id, 'research', authoritative=True, now=60)
    current = 60 + lifecycle_worker_service._TERMINAL_CONFIRMATION_SECONDS
    monkeypatch.setattr(lifecycle_worker_service.global_user_state,
                        'get_cluster_image_consumers', lambda _: {})
    monkeypatch.setattr(
        lifecycle_worker_service.managed_job_state,
        'get_job_task_terminal_states', lambda identities:
        {identity: identity == job_identity for identity in identities})
    monkeypatch.setattr(
        lifecycle_worker_service.serve_state,
        'get_service_version_terminal_states', lambda identities:
        {identity: identity == service_identity for identity in identities})

    assert lifecycle_worker_service._reconcile_terminal_consumers(current) == 2
    for demand in (job, service):
        terminal = demand_state.get_demand(demand.id, 'research')
        assert terminal is not None
        assert terminal.state == models.ImageDemandState.RELEASED
        with image_database.connect() as connection:
            watermark = connection.execute(
                sqlalchemy.select(schema.consumer_watermarks).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind ==
                    demand.consumer_kind,
                    schema.consumer_watermarks.c.consumer_owner ==
                    demand.consumer_owner)).mappings().one()
        assert watermark['owner_deleted_at'] == current

    assert demand_state.compact_terminal_demands(
        now=current + demand_state._TERMINAL_RETENTION_SECONDS) == 2
    assert demand_state.get_demand(job.id, 'research') is None
    assert demand_state.get_demand(service.id, 'research') is None
    with image_database.connect() as connection:
        retained_fences = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.consumer_owner).where(
                    schema.consumer_watermarks.c.consumer_owner.in_(
                        [job.consumer_owner, service.consumer_owner]))).all()
    assert len(retained_fences) == 2


def test_retired_profile_allows_exact_demand_replay_but_no_new_owner(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    first = _warming_demand(active, publication_record, regional, profile)
    with image_database.begin() as connection:
        connection.execute(schema.profile_revisions.update().where(
            schema.profile_revisions.c.id == active.id).values(
                state=models.ImageProfileState.RETIRED.value, updated_at=51))

    replay = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             request_id='request-1',
                             now=52)
    assert replay.id == first.id
    with pytest.raises(demand_state.StaleConsumerGenerationError):
        _warming_demand(active,
                        publication_record,
                        regional,
                        profile,
                        owner='new-service:v1',
                        controller_epoch='service:new-service:v1',
                        now=53)


def test_superseded_generation_cannot_commit_ready_after_restart(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    first = _warming_demand(active, publication_record, regional, profile)
    assert demand_state.fail_and_supersede_demand(first.id,
                                                  'COPY_FAILED',
                                                  now=51)
    successor = _warming_demand(active,
                                publication_record,
                                regional,
                                profile,
                                request_id='retry-request',
                                now=52)
    assert successor.consumer_generation == first.consumer_generation + 1
    pull_plan = _pull_plan(active, regional)
    with pytest.raises(demand_state.StaleConsumerGenerationError):
        transactions.commit_ready_demand(
            demand_id=first.id,
            consumer_generation=first.consumer_generation,
            pull_plan=pull_plan,
            now=53)
    ready = transactions.commit_ready_demand(
        demand_id=successor.id,
        consumer_generation=successor.consumer_generation,
        pull_plan=pull_plan,
        now=54)
    assert ready.state == models.ImageDemandState.READY


def test_ready_commit_rejects_any_non_authoritative_pull_plan_field(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active, publication_record, regional, profile)
    expected = _pull_plan(active, regional)

    for field, forged in (
        ('target_id', 'forged-target'),
        ('runtime_principal', 'arn:aws:iam::000000000000:role/Forged'),
        ('instance_profile', 'ForgedProfile'),
        ('credential_helper', None),
        ('kubernetes_node_selector', [('forged', 'selector')]),
    ):
        pull_plan = dict(expected)
        pull_plan[field] = forged
        with pytest.raises(ValueError, match='does not match its demand'):
            transactions.commit_ready_demand(
                demand_id=demand.id,
                consumer_generation=demand.consumer_generation,
                pull_plan=pull_plan,
                now=53)

    with_extra = dict(expected)
    with_extra['inline_credential'] = 'must-not-persist'
    with pytest.raises(ValueError, match='does not match its demand'):
        transactions.commit_ready_demand(
            demand_id=demand.id,
            consumer_generation=demand.consumer_generation,
            pull_plan=with_extra,
            now=54)


def test_ready_commit_accepts_only_profile_qualified_eks_pull_plan(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='boltz-eks:v7',
                             controller_epoch='service:boltz-eks:v7',
                             backend='aws_eks',
                             placement_region='boltz-west')
    target = profile.targets[0]
    binding_id = target.runtime_binding('aws_eks')
    assert binding_id is not None
    binding = profile.bindings[binding_id]
    qualified = next(cluster for cluster in binding.qualified_clusters
                     if cluster.context == 'boltz-west')
    pull_plan = {
        'version': 1,
        'reference': regional.target_ref,
        'runtime_digest': _DIGEST,
        'platform': 'linux/amd64',
        'distribution': profile.name,
        'profile_revision_id': active.id,
        'target_id': target.name,
        'target_fingerprint': regional.target_fingerprint,
        'auth_strategy': 'ecr_runtime_identity',
        'credential_helper': None,
        'runtime_principal': None,
        'instance_profile': None,
        'kubernetes_node_selector': list(qualified.node_selector),
    }

    ready = transactions.commit_ready_demand(
        demand_id=demand.id,
        consumer_generation=demand.consumer_generation,
        pull_plan=pull_plan,
        now=53)

    assert ready.state == models.ImageDemandState.READY
    assert ready.pull_plan is not None
    assert ready.pull_plan['kubernetes_node_selector'] == [
        list(item) for item in qualified.node_selector
    ]


def test_controller_replay_cannot_change_immutable_placement(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    _warming_demand(active, publication_record, regional, profile)

    with pytest.raises(ValueError, match='cannot change image target'):
        _warming_demand(active,
                        publication_record,
                        regional,
                        profile,
                        backend='aws_eks',
                        placement_region='boltz-west',
                        now=51)
    with pytest.raises(ValueError, match='cannot change image target'):
        _warming_demand(active,
                        publication_record,
                        regional,
                        profile,
                        request_id='different-request',
                        now=52)


def test_concurrent_regional_admission_converges_without_false_capacity(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    publication_record, canonical = _publish_and_bind(profile)
    canonical = _complete_location(canonical, now=30)
    assert publication_record.image_id is not None
    west = profile.targets[0]
    barrier = threading.Barrier(8)

    def reserve(index: int) -> str:
        barrier.wait(timeout=10)
        location = transactions.reserve_regional_location(
            image_id=publication_record.image_id,
            workspace='research',
            profile_revision_id=active.id,
            target_id=west.name,
            canonical_location_id=canonical.id,
            max_regional_locations=16,
            now=40 + index)
        return location.id

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        ids = list(executor.map(reserve, range(8)))
    assert len(set(ids)) == 1
    location = topology_state.get_location(ids[0])
    assert location is not None
    shard = topology_state.get_shard(location.shard_id)
    assert shard is not None
    assert shard.reserved_manifests == 1
    assert shard.reserved_declared_bytes == location.reserved_declared_bytes


def test_ready_commit_and_regional_admission_follow_global_lock_order(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, canonical, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active, publication_record, canonical, profile)
    assert publication_record.image_id is not None
    target = profile.targets[0]
    ready_holds_artifact = threading.Event()
    allow_ready_location_lock = threading.Event()
    admission_attempted_artifact = threading.Event()

    def _is_artifact_lock(statement: str) -> bool:
        normalized = ' '.join(statement.split()).upper()
        return (' FROM CONTAINER_IMAGES ' in normalized and
                ' FOR UPDATE' in normalized)

    def _pause_ready_after_artifact(_connection, _cursor, statement,
                                    _parameters, _context,
                                    _executemany) -> None:
        if (threading.current_thread().name.startswith('ready-commit') and
                _is_artifact_lock(statement)):
            ready_holds_artifact.set()
            if not allow_ready_location_lock.wait(timeout=10):
                raise TimeoutError('READY commit lock-order test timed out.')

    def _observe_admission_artifact(_connection, _cursor, statement,
                                    _parameters, _context,
                                    _executemany) -> None:
        if (threading.current_thread().name.startswith('regional-admission') and
                _is_artifact_lock(statement)):
            admission_attempted_artifact.set()

    sqlalchemy.event.listen(image_database, 'after_cursor_execute',
                            _pause_ready_after_artifact)
    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            _observe_admission_artifact)
    ready_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='ready-commit')
    admission_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='regional-admission')
    try:
        ready_future = ready_executor.submit(
            transactions.commit_ready_demand,
            demand_id=demand.id,
            consumer_generation=demand.consumer_generation,
            pull_plan=_pull_plan(active, canonical),
            now=60)
        assert ready_holds_artifact.wait(timeout=5)

        # The READY transaction holds the artifact but must not yet hold the
        # canonical location. The old location-then-artifact order fails this
        # NOWAIT proof before PostgreSQL needs to detect a deadlock.
        with orm.Session(image_database) as observer, observer.begin():
            location_id = observer.execute(
                sqlalchemy.select(schema.locations.c.id).where(
                    schema.locations.c.id == canonical.id).with_for_update(
                        nowait=True)).scalar_one()
            assert location_id == canonical.id

        admission_future = admission_executor.submit(
            transactions.reserve_regional_location,
            image_id=publication_record.image_id,
            workspace='research',
            profile_revision_id=active.id,
            target_id=target.name,
            canonical_location_id=canonical.id,
            max_regional_locations=16,
            now=61)
        assert admission_attempted_artifact.wait(timeout=5)
        allow_ready_location_lock.set()

        assert ready_future.result(timeout=5).id == demand.id
        assert admission_future.result(timeout=5).id == regional.id
    finally:
        allow_ready_location_lock.set()
        sqlalchemy.event.remove(image_database, 'after_cursor_execute',
                                _pause_ready_after_artifact)
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                _observe_admission_artifact)
        ready_executor.shutdown(wait=True)
        admission_executor.shutdown(wait=True)


def test_ready_commit_and_evicted_readmission_follow_global_lock_order(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.id == regional.id
    assert eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=100)
    eviction = _mark_readback(eviction, now=100)
    evicted = topology_state.complete_eviction(eviction.id,
                                               eviction.lease_token,
                                               present=False,
                                               now=101)
    assert evicted is not None
    assert evicted.state == models.ImageLocationState.EVICTED
    demand = _warming_demand(active,
                             publication_record,
                             evicted,
                             profile,
                             now=102)
    ready_holds_artifact = threading.Event()
    allow_ready_location_lock = threading.Event()
    readmission_attempted_artifact = threading.Event()

    def _is_artifact_lock(statement: str) -> bool:
        normalized = ' '.join(statement.split()).upper()
        return (' FROM CONTAINER_IMAGES ' in normalized and
                ' FOR UPDATE' in normalized)

    def _pause_ready_after_artifact(_connection, _cursor, statement,
                                    _parameters, _context,
                                    _executemany) -> None:
        if (threading.current_thread().name.startswith('ready-commit') and
                _is_artifact_lock(statement)):
            ready_holds_artifact.set()
            if not allow_ready_location_lock.wait(timeout=10):
                raise TimeoutError('READY readmission lock test timed out.')

    def _observe_readmission_artifact(_connection, _cursor, statement,
                                      _parameters, _context,
                                      _executemany) -> None:
        if (threading.current_thread().name.startswith('readmission') and
                _is_artifact_lock(statement)):
            readmission_attempted_artifact.set()

    sqlalchemy.event.listen(image_database, 'after_cursor_execute',
                            _pause_ready_after_artifact)
    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            _observe_readmission_artifact)
    ready_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='ready-commit')
    readmission_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='readmission')
    try:
        ready_future = ready_executor.submit(
            transactions.commit_ready_demand,
            demand_id=demand.id,
            consumer_generation=demand.consumer_generation,
            pull_plan=_pull_plan(active, regional),
            now=103)
        assert ready_holds_artifact.wait(timeout=5)
        readmission_future = readmission_executor.submit(
            topology_state.retry_location, regional.id, 'research', now=104)
        assert readmission_attempted_artifact.wait(timeout=5)

        # Readmission has locked its shard and is waiting for the artifact. It
        # must not hold the location, or artifact -> location READY commit can
        # deadlock against location -> artifact retry.
        with orm.Session(image_database) as observer, observer.begin():
            location_id = observer.execute(
                sqlalchemy.select(schema.locations.c.id).where(
                    schema.locations.c.id == regional.id).with_for_update(
                        nowait=True)).scalar_one()
            assert location_id == regional.id

        allow_ready_location_lock.set()
        with pytest.raises(transactions.DemandLocationNotReadyError):
            ready_future.result(timeout=5)
        readmitted = readmission_future.result(timeout=5)
        assert readmitted is not None
        assert readmitted.state == models.ImageLocationState.PENDING
    finally:
        allow_ready_location_lock.set()
        sqlalchemy.event.remove(image_database, 'after_cursor_execute',
                                _pause_ready_after_artifact)
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                _observe_readmission_artifact)
        ready_executor.shutdown(wait=True)
        readmission_executor.shutdown(wait=True)


def _locked_table(statement: str) -> str | None:
    normalized = ' '.join(statement.split()).upper()
    if ' FOR UPDATE' not in normalized:
        return None
    if ' FROM CONTAINER_IMAGE_CONSUMER_WATERMARKS ' in normalized:
        return 'watermark'
    if ' FROM CONTAINER_IMAGE_DEMANDS ' in normalized:
        return 'demand'
    return None


def _wait_for_backend_lock(image_database: sqlalchemy.engine.Engine,
                           backend_pid: int) -> bool:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with image_database.connect() as observer:
            waiting = bool(
                observer.execute(
                    sqlalchemy.text('SELECT wait_event_type = \'Lock\' '
                                    'FROM pg_stat_activity WHERE pid = :pid'), {
                                        'pid': backend_pid
                                    }).scalar())
        if waiting:
            return True
        time.sleep(0.01)
    return False


def test_terminal_compaction_and_release_follow_watermark_lock_order(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    owner = '42:task:0'
    expired = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        owner=owner,
        consumer_kind='managed_job_task',
        controller_epoch='managed-job:42:task:0:recovery:0',
        controller_sequence=0)
    assert demand_state.supersede_demand(expired.id, 'research', now=51)
    retained = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        owner=owner,
        consumer_kind='managed_job_task',
        controller_epoch='managed-job:42:task:0:recovery:1',
        controller_sequence=1,
        allow_epoch_advance=True,
        request_id='request-2',
        now=52)
    assert demand_state.release_demand_authoritatively(retained.id,
                                                       'research',
                                                       now=54)
    with image_database.begin() as connection:
        connection.execute(schema.demands.update().where(
            schema.demands.c.id == expired.id).values(expires_at=100))
        connection.execute(schema.demands.update().where(
            schema.demands.c.id == retained.id).values(expires_at=2000))

    compaction_first_lock = threading.Event()
    allow_compaction_to_continue = threading.Event()
    release_attempted_watermark = threading.Event()
    first_lock_table: dict[str, str] = {}
    release_backend: dict[str, int] = {}

    def _pause_compaction_after_first_lock(_connection, _cursor, statement,
                                           _parameters, _context,
                                           _executemany) -> None:
        if not threading.current_thread().name.startswith('demand-compaction'):
            return
        table = _locked_table(statement)
        if table is None or compaction_first_lock.is_set():
            return
        first_lock_table['name'] = table
        compaction_first_lock.set()
        if not allow_compaction_to_continue.wait(timeout=10):
            raise TimeoutError('Demand compaction lock-order test timed out.')

    def _observe_release_watermark(_connection, _cursor, statement, _parameters,
                                   _context, _executemany) -> None:
        if (threading.current_thread().name.startswith('authoritative-release')
                and _locked_table(statement) == 'watermark'):
            release_backend['pid'] = int(_cursor.connection.get_backend_pid())
            release_attempted_watermark.set()

    sqlalchemy.event.listen(image_database, 'after_cursor_execute',
                            _pause_compaction_after_first_lock)
    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            _observe_release_watermark)
    compaction_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='demand-compaction')
    release_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='authoritative-release')
    try:
        compaction_future = compaction_executor.submit(
            demand_state.compact_terminal_demands, now=1000, limit=10)
        assert compaction_first_lock.wait(timeout=5)
        assert first_lock_table['name'] == 'watermark'

        release_future = release_executor.submit(
            demand_state.release_demand_authoritatively,
            expired.id,
            'research',
            now=1001)
        assert release_attempted_watermark.wait(timeout=5)
        assert _wait_for_backend_lock(image_database, release_backend['pid'])
        allow_compaction_to_continue.set()

        assert compaction_future.result(timeout=5) == 1
        assert release_future.result(timeout=5) is False
        assert demand_state.get_demand(expired.id, 'research') is None
        assert demand_state.get_demand(retained.id, 'research') is not None
    finally:
        allow_compaction_to_continue.set()
        sqlalchemy.event.remove(image_database, 'after_cursor_execute',
                                _pause_compaction_after_first_lock)
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                _observe_release_watermark)
        compaction_executor.shutdown(wait=True)
        release_executor.shutdown(wait=True)


def test_terminal_compaction_cannot_resurrect_deleted_owner(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    owner = '42:task:0'
    expired = _warming_demand(
        active,
        publication_record,
        regional,
        profile,
        owner=owner,
        consumer_kind='managed_job_task',
        controller_epoch='managed-job:42:task:0:recovery:0',
        controller_sequence=0)
    assert demand_state.supersede_demand(expired.id, 'research', now=51)
    assert demand_state.release_demand_authoritatively(expired.id,
                                                       'research',
                                                       now=54)
    with image_database.begin() as connection:
        connection.execute(schema.demands.update().where(
            schema.demands.c.id == expired.id).values(expires_at=100))

    compaction_holds_watermark = threading.Event()
    allow_compaction_to_continue = threading.Event()
    creator_attempted_insert = threading.Event()
    creator_backend: dict[str, int] = {}

    def _pause_compaction(_connection, _cursor, statement, _parameters,
                          _context, _executemany) -> None:
        if (threading.current_thread().name.startswith('demand-compaction') and
                _locked_table(statement) == 'watermark' and
                not compaction_holds_watermark.is_set()):
            compaction_holds_watermark.set()
            if not allow_compaction_to_continue.wait(timeout=10):
                raise TimeoutError('Demand compaction race test timed out.')

    def _observe_creator_insert(_connection, _cursor, statement, _parameters,
                                _context, _executemany) -> None:
        normalized = ' '.join(statement.split()).upper()
        if (threading.current_thread().name.startswith('demand-creator') and
                normalized.startswith(
                    'INSERT INTO CONTAINER_IMAGE_CONSUMER_WATERMARKS')):
            creator_backend['pid'] = int(_cursor.connection.get_backend_pid())
            creator_attempted_insert.set()

    sqlalchemy.event.listen(image_database, 'after_cursor_execute',
                            _pause_compaction)
    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            _observe_creator_insert)
    compaction_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='demand-compaction')
    creator_executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix='demand-creator')
    try:
        compaction_future = compaction_executor.submit(
            demand_state.compact_terminal_demands, now=1000, limit=10)
        assert compaction_holds_watermark.wait(timeout=5)
        creator_future = creator_executor.submit(
            _warming_demand,
            active,
            publication_record,
            regional,
            profile,
            owner=owner,
            consumer_kind='managed_job_task',
            controller_epoch='managed-job:42:task:0:recovery:1',
            controller_sequence=1,
            allow_epoch_advance=True,
            request_id='zombie-replay',
            now=1001)
        assert creator_attempted_insert.wait(timeout=5)
        assert _wait_for_backend_lock(image_database, creator_backend['pid'])
        allow_compaction_to_continue.set()

        with pytest.raises(demand_state.StaleConsumerGenerationError,
                           match='authoritatively deleted'):
            creator_future.result(timeout=5)
        assert compaction_future.result(timeout=5) == 1
        with pytest.raises(demand_state.StaleConsumerGenerationError,
                           match='authoritatively deleted'):
            _warming_demand(active,
                            publication_record,
                            regional,
                            profile,
                            owner=owner,
                            consumer_kind='managed_job_task',
                            controller_epoch='managed-job:42:task:0:recovery:1',
                            controller_sequence=1,
                            allow_epoch_advance=True,
                            request_id='zombie-retry',
                            now=1002)
        assert demand_state.get_demand(expired.id, 'research') is None
        with image_database.connect() as connection:
            watermark = connection.execute(
                sqlalchemy.select(schema.consumer_watermarks).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind ==
                    'managed_job_task',
                    schema.consumer_watermarks.c.consumer_owner ==
                    owner)).mappings().one()
            live_demands = connection.execute(
                sqlalchemy.select(schema.demands.c.id).where(
                    schema.demands.c.workspace == 'research',
                    schema.demands.c.consumer_kind == 'managed_job_task',
                    schema.demands.c.consumer_owner == owner,
                    schema.demands.c.state.in_([
                        models.ImageDemandState.WARMING.value,
                        models.ImageDemandState.READY.value,
                        models.ImageDemandState.FAILED.value,
                    ]))).all()
        assert watermark['owner_deleted_at'] == 54
        assert not live_demands
    finally:
        allow_compaction_to_continue.set()
        sqlalchemy.event.remove(image_database, 'after_cursor_execute',
                                _pause_compaction)
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                _observe_creator_insert)
        compaction_executor.shutdown(wait=True)
        creator_executor.shutdown(wait=True)


def test_terminal_compaction_requires_proof_and_retains_owner_fence(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active, publication_record, regional, profile)
    assert demand_state.supersede_demand(demand.id, 'research', now=51)
    with image_database.begin() as connection:
        connection.execute(schema.demands.update().where(
            schema.demands.c.id == demand.id).values(expires_at=100))

    assert demand_state.compact_terminal_demands(now=1000) == 0
    assert demand_state.get_demand(demand.id, 'research') is not None

    assert demand_state.release_demand_authoritatively(demand.id,
                                                       'research',
                                                       now=900)
    assert demand_state.compact_terminal_demands(now=1000) == 1
    assert demand_state.get_demand(demand.id, 'research') is None
    with pytest.raises(demand_state.StaleConsumerGenerationError,
                       match='no longer exists'):
        transactions.commit_ready_demand(
            demand_id=demand.id,
            consumer_generation=demand.consumer_generation,
            pull_plan={})
    with image_database.connect() as connection:
        watermark = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.consumer_owner).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind ==
                    demand.consumer_kind,
                    schema.consumer_watermarks.c.consumer_owner ==
                    demand.consumer_owner)).first()
    assert watermark is not None

    with pytest.raises(ValueError, match='page size'):
        demand_state.compact_terminal_demands(limit=0)
    with pytest.raises(ValueError, match='page size'):
        demand_state.compact_terminal_demands(limit=1001)


def test_deleted_owner_rejects_legacy_generation_creation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    assert publication_record.image_id is not None
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    owner = 'legacy-cluster'
    placement = {
        'provider': 'aws',
        'region': profile.targets[0].region,
        'backend': 'aws_vm',
        'platform': 'linux/amd64',
        'consumer': {
            'request_id': 'legacy-request',
        },
    }
    demand = transactions.create_warming_demand(
        authority_id=authority,
        workspace='research',
        consumer_kind='cluster',
        consumer_owner=owner,
        consumer_generation=0,
        target_key=(f'{publication_record.image_id}:'
                    f'{regional.target_fingerprint}'),
        owner_epoch=0,
        image_id=publication_record.image_id,
        runtime_digest=_DIGEST,
        profile_revision_id=active.id,
        target_fingerprint=regional.target_fingerprint,
        location_id=regional.id,
        placement=placement,
        now=50)
    assert demand_state.supersede_demand(demand.id, 'research', now=51)
    assert demand_state.release_demand_authoritatively(demand.id,
                                                       'research',
                                                       now=52)

    with pytest.raises(demand_state.StaleConsumerGenerationError,
                       match='authoritatively deleted'):
        transactions.create_warming_demand(
            authority_id=authority,
            workspace='research',
            consumer_kind='cluster',
            consumer_owner=owner,
            consumer_generation=1,
            target_key=(f'{publication_record.image_id}:'
                        f'{regional.target_fingerprint}'),
            owner_epoch=0,
            image_id=publication_record.image_id,
            runtime_digest=_DIGEST,
            profile_revision_id=active.id,
            target_fingerprint=regional.target_fingerprint,
            location_id=regional.id,
            placement=placement,
            now=53)


def test_shard_admission_retries_after_locked_home_fills(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    target = profile.targets[0]
    with image_database.connect() as connection:
        ordered_ids = connection.execute(
            sqlalchemy.select(schema.registry_shards.c.id).where(
                schema.registry_shards.c.workspace == 'research',
                schema.registry_shards.c.profile == profile.name,
                schema.registry_shards.c.target_id == target.name).order_by(
                    sqlalchemy.func.md5(schema.registry_shards.c.id + _DIGEST),
                    schema.registry_shards.c.id)).scalars().all()
    assert len(ordered_ids) == target.shard_count
    home_id, fallback_id = ordered_ids[:2]
    waiter_started = threading.Event()
    waiter: dict[str, int] = {}

    def select_shard() -> str:
        with orm.Session(image_database) as session, session.begin():
            waiter['pid'] = int(
                session.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.pg_backend_pid())).scalar_one())
            waiter_started.set()
            row = transactions._select_and_lock_shard(session,
                                                      workspace='research',
                                                      profile=profile.name,
                                                      target_id=target.name,
                                                      runtime_digest=_DIGEST,
                                                      declared_size_bytes=4096)
            return str(row['id'])

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        with orm.Session(image_database) as blocker, blocker.begin():
            blocker.execute(
                sqlalchemy.select(schema.registry_shards).where(
                    schema.registry_shards.c.id ==
                    home_id).with_for_update()).mappings().one()
            result = executor.submit(select_shard)
            assert waiter_started.wait(timeout=5)
            deadline = time.monotonic() + 5
            blocked = False
            while time.monotonic() < deadline:
                with image_database.connect() as observer:
                    blocked = bool(
                        observer.execute(
                            sqlalchemy.text(
                                'SELECT wait_event_type = \'Lock\' '
                                'FROM pg_stat_activity WHERE pid = :pid'), {
                                    'pid': waiter['pid']
                                }).scalar())
                if blocked:
                    break
                time.sleep(0.01)
            assert blocked
            blocker.execute(schema.registry_shards.update().where(
                schema.registry_shards.c.id == home_id).values(
                    reserved_manifests=schema.registry_shards.c.max_manifests))
        assert result.result(timeout=5) == fallback_id


def test_prepared_location_retention_uses_verified_time_and_workspace_policy(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    assert regional.last_used_at is None
    assert regional.last_verified_at is not None

    assert topology_state.claim_next_eviction(
        worker_id='lifecycle-1',
        unused_before=regional.last_verified_at + 1000,
        workspace_unused_before={'research': None},
        lease_seconds=60,
        now=100) is None
    assert topology_state.claim_next_eviction(
        worker_id='lifecycle-1',
        unused_before=regional.last_verified_at + 1000,
        workspace_unused_before={'research': regional.last_verified_at},
        lease_seconds=60,
        now=100) is None

    claimed = topology_state.claim_next_eviction(
        worker_id='lifecycle-1',
        unused_before=0,
        workspace_unused_before={'research': regional.last_verified_at + 1},
        lease_seconds=60,
        now=100)
    assert claimed is not None and claimed.id == regional.id


def test_copy_completion_cannot_finish_an_eviction_lease(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.id == regional.id
    assert eviction.lease_token is not None
    before = topology_state.get_shard(eviction.shard_id)
    assert before is not None and before.in_flight == 1

    assert topology_state.complete_location_ready(eviction.id,
                                                  eviction.lease_token,
                                                  now=101) is None

    retained = topology_state.get_location(eviction.id)
    after = topology_state.get_shard(eviction.shard_id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.EVICTING
    assert retained.lease_token == eviction.lease_token
    assert after is not None and after.in_flight == 1


def test_provider_result_requires_durable_delete_intent(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.id == regional.id
    assert eviction.lease_token is not None

    assert topology_state.complete_eviction(eviction.id,
                                            eviction.lease_token,
                                            present=False,
                                            now=101) is None
    retained = topology_state.get_location(eviction.id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.EVICTING
    assert retained.lease_kind == 'EVICT'
    assert retained.lease_token == eviction.lease_token
    restored = topology_state.complete_eviction(eviction.id,
                                                eviction.lease_token,
                                                present=None,
                                                provider_not_called=True,
                                                now=102)
    assert restored is not None
    assert restored.state == models.ImageLocationState.READY


def test_no_io_restore_rejects_committed_delete_intent(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.id == regional.id
    assert eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=100)

    assert topology_state.complete_eviction(eviction.id,
                                            eviction.lease_token,
                                            present=None,
                                            provider_not_called=True,
                                            now=101) is None
    retained = topology_state.get_location(eviction.id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.EVICTING
    assert retained.lease_kind == 'DELETE'

    eviction = _mark_readback(eviction, now=102)
    completed = topology_state.complete_eviction(eviction.id,
                                                 eviction.lease_token,
                                                 present=True,
                                                 now=103)
    assert completed is not None
    assert completed.state == models.ImageLocationState.READY


def test_disabled_eviction_policy_blocks_new_claim_but_not_expired_reclaim(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == regional.shard_id).values(
                eviction_enabled=False))
    assert topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                              unused_before=1000,
                                              lease_seconds=60,
                                              now=100) is None

    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == regional.shard_id).values(
                eviction_enabled=True))
    original = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert original is not None and original.id == regional.id
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == regional.shard_id).values(
                eviction_enabled=False))
    reclaimed = topology_state.claim_next_eviction(worker_id='lifecycle-2',
                                                   unused_before=1000,
                                                   lease_seconds=60,
                                                   now=161)
    assert reclaimed is not None and reclaimed.id == regional.id
    assert reclaimed.lease_kind == 'EVICT'


def test_live_eviction_lease_ignores_fast_worker_wall_clock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    with image_database.connect() as connection:
        current = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())
    claimed = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                 unused_before=current + 1,
                                                 lease_seconds=60,
                                                 now=current)
    assert claimed is not None and claimed.lease_token is not None
    monkeypatch.setattr(topology_state.time, 'time', lambda: current + 10_000)

    assert topology_state.claim_next_eviction(worker_id='lifecycle-2',
                                              retention_seconds=0,
                                              lease_seconds=60) is None

    unchanged = topology_state.get_location(regional.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.EVICTING
    assert unchanged.lease_kind == 'EVICT'
    assert unchanged.lease_token == claimed.lease_token


def test_locked_oldest_shard_does_not_block_global_eviction(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, publication_record, canonical, regional = _ready_regional(
        image_database, monkeypatch, profile)
    assert publication_record.image_id is not None
    target = profile.targets[0]
    older_image_id = str(uuid.uuid4())
    older_location_id = str(uuid.uuid4())
    older_shard = next(
        shard for shard in topology_state.list_shards('research', profile.name)
        if shard.target_id == target.name and shard.id != regional.shard_id)
    older_shard_id = older_shard.id
    physical_fingerprint = older_shard.physical_fingerprint
    with orm.Session(image_database) as session, session.begin():
        session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == older_shard_id).values(
                state=models.ImageShardState.READY.value,
                qualified_at=1,
                eviction_enabled=True,
                reserved_manifests=1,
                reserved_declared_bytes=4096))
        session.execute(schema.images.insert().values(
            id=older_image_id,
            workspace='research',
            runtime_digest=_OTHER_DIGEST,
            platform='linux/amd64',
            config_digest='sha256:' + 'd' * 64,
            manifest_media_type=_MANIFEST_MEDIA_TYPE,
            manifest_size_bytes=512,
            declared_size_bytes=4096,
            creator_user_hash='1' * 64,
            producer_kind='external_oci',
            created_at=1,
            updated_at=1))
        session.execute(schema.locations.insert().values(
            id=older_location_id,
            workspace='research',
            image_id=older_image_id,
            shard_id=older_shard_id,
            target_fingerprint=target.target_fingerprint,
            physical_fingerprint=physical_fingerprint,
            runtime_digest=_OTHER_DIGEST,
            canonical=False,
            canonical_location_id=canonical.id,
            target_ref=(f'{target.registry}/{older_shard.repository_name}'
                        f'@{_OTHER_DIGEST}'),
            state=models.ImageLocationState.READY.value,
            attempt_count=1,
            last_verified_at=1,
            reserved_declared_bytes=4096,
            created_at=1,
            updated_at=1))

    with orm.Session(image_database) as blocker, blocker.begin():
        blocker.execute(
            sqlalchemy.select(schema.registry_shards.c.id).where(
                schema.registry_shards.c.id ==
                older_shard_id).with_for_update()).one()
        claim = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                   unused_before=1000,
                                                   lease_seconds=60,
                                                   now=100)
    assert claim is not None
    assert claim.id == regional.id


def test_eviction_exact_absence_with_new_demand_requeues_without_release(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    shard_before = topology_state.get_shard(regional.shard_id)
    assert shard_before is not None
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.lease_token is not None
    _warming_demand(active, publication_record, regional, profile, now=101)
    eviction = _begin_delete(eviction, now=101)
    eviction = _mark_readback(eviction, now=101)

    completed = topology_state.complete_eviction(eviction.id,
                                                 eviction.lease_token,
                                                 present=False,
                                                 now=102)

    assert completed is not None
    assert completed.state == models.ImageLocationState.PENDING
    assert (
        completed.reserved_declared_bytes == regional.reserved_declared_bytes)
    shard_after = topology_state.get_shard(regional.shard_id)
    assert shard_after is not None
    assert shard_after.reserved_manifests == shard_before.reserved_manifests
    assert (shard_after.reserved_declared_bytes ==
            shard_before.reserved_declared_bytes)
    assert shard_after.in_flight == 0


def test_eviction_won_before_demand_commit_reports_typed_warming_state(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             now=101)

    with pytest.raises(transactions.DemandLocationNotReadyError) as exc_info:
        transactions.commit_ready_demand(
            demand_id=demand.id,
            consumer_generation=demand.consumer_generation,
            pull_plan=_pull_plan(active, regional),
            now=102)

    assert exc_info.value.state == models.ImageLocationState.EVICTING
    assert exc_info.value.error_code is None


def test_expired_pre_delete_eviction_with_live_demand_restores_ready(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    first = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                               unused_before=1000,
                                               lease_seconds=60,
                                               now=100)
    assert first is not None and first.lease_token is not None
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             now=101)

    reclaimed = topology_state.claim_next_eviction(worker_id='lifecycle-2',
                                                   unused_before=1000,
                                                   lease_seconds=60,
                                                   now=161)

    assert reclaimed is not None and reclaimed.id == regional.id
    assert reclaimed.state == models.ImageLocationState.READY
    assert reclaimed.lease_token is None
    assert reclaimed.lease_kind is None
    committed = transactions.commit_ready_demand(
        demand_id=demand.id,
        consumer_generation=demand.consumer_generation,
        pull_plan=_pull_plan(active, regional),
        now=162)
    assert committed.state == models.ImageDemandState.READY
    shard = topology_state.get_shard(regional.shard_id)
    assert shard is not None and shard.in_flight == 0


def test_inflight_delete_expiry_quarantines_before_late_completion(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    """A delete that outlives its lease can never restore or recopy READY."""
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    current = int(time.time())
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=current + 1,
                                                  lease_seconds=60,
                                                  now=current)
    assert eviction is not None and eviction.lease_token is not None

    class SynchronousHeartbeat:
        """Runs exact ownership checks without renewing in the background."""

        def __init__(self, heartbeat: Any, _interval: float) -> None:
            self._heartbeat = heartbeat

        def __enter__(self):
            self.assert_owned()
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def assert_owned(self) -> None:
            if not self._heartbeat():
                raise worker_lease.LeaseLostError(
                    'Container image work lease was lost.')

    provider_call_started = threading.Event()
    resume_provider_call = threading.Event()
    delete_completed = threading.Event()

    class PausedRepository:
        """Pauses after the durable hook, with provider I/O in flight."""

        def __init__(self, hooks: Any) -> None:
            self._hooks = hooks

        def delete_request_outcome(self,
                                   digest: str) -> aws.DeleteRequestOutcome:
            assert digest == regional.runtime_digest
            self._hooks.before_call()
            provider_call_started.set()
            assert resume_provider_call.wait(timeout=5)
            delete_completed.set()
            return aws.DeleteRequestOutcome.CONCLUDED

    def repository_from_role(*_args: Any, **kwargs: Any) -> PausedRepository:
        return PausedRepository(kwargs['hooks'])

    limiter = types.SimpleNamespace(before_call=lambda _shard: None,
                                    record_throttle=lambda _shard: None)
    heartbeat_location = topology_state.heartbeat_location

    def fixed_clock_heartbeat(location_id: str, lease_token: str,
                              lease_seconds: int) -> bool:
        return heartbeat_location(location_id,
                                  lease_token,
                                  lease_seconds,
                                  now=current)

    monkeypatch.setattr(topology_state, 'heartbeat_location',
                        fixed_clock_heartbeat)
    monkeypatch.setattr(lifecycle_worker_service, '_LeaseHeartbeat',
                        SynchronousHeartbeat)
    monkeypatch.setattr(lifecycle_worker_service.aws.EcrRepository, 'from_role',
                        repository_from_role)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        stale = executor.submit(lifecycle_worker_service.evict_location,
                                eviction,
                                limiter,
                                lease_seconds=60)
        assert provider_call_started.wait(timeout=5)
        intent = topology_state.get_location(regional.id)
        assert intent is not None and intent.lease_kind == 'DELETE'
        active = _refresh_runtime_attestation(
            active,
            profile,
            profile.targets[0],
            backend='aws_vm',
            runtime_id=profile.targets[0].region,
            now=current + 1)
        demand = _warming_demand(active,
                                 publication_record,
                                 regional,
                                 profile,
                                 now=current + 1)
        recovered = topology_state.claim_next_eviction(worker_id='lifecycle-2',
                                                       unused_before=current +
                                                       1,
                                                       lease_seconds=30,
                                                       now=current + 61)
        assert recovered is not None
        assert recovered.state == models.ImageLocationState.QUARANTINED
        assert recovered.lease_token is None
        with pytest.raises(
                transactions.DemandLocationNotReadyError) as exc_info:
            transactions.commit_ready_demand(
                demand_id=demand.id,
                consumer_generation=demand.consumer_generation,
                pull_plan=_pull_plan(active, regional),
                now=current + 62)
        assert exc_info.value.state == models.ImageLocationState.QUARANTINED

        resume_provider_call.set()
        with pytest.raises(worker_lease.LeaseLostError):
            stale.result(timeout=5)

    assert delete_completed.is_set()
    final = topology_state.get_location(regional.id)
    assert final is not None
    assert final.state == models.ImageLocationState.QUARANTINED
    assert final.error_code == (
        models.ImageLocationErrorCode.PROVIDER_OUTCOME_AMBIGUOUS.value)
    shard = topology_state.get_shard(regional.shard_id)
    assert shard is not None and shard.in_flight == 0
    assert shard.reserved_manifests > 0


def test_eviction_reopens_full_shard_when_reservation_is_released(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    shard = topology_state.get_shard(regional.shard_id)
    assert shard is not None
    with image_database.begin() as connection:
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == shard.id).values(
                state=models.ImageShardState.FULL.value,
                max_manifests=shard.reserved_manifests,
                max_declared_bytes=shard.reserved_declared_bytes))

    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=100)
    eviction = _mark_readback(eviction, now=100)
    evicted = topology_state.complete_eviction(eviction.id,
                                               eviction.lease_token,
                                               present=False,
                                               now=101)

    assert evicted is not None
    assert evicted.state == models.ImageLocationState.EVICTED
    reopened = topology_state.get_shard(regional.shard_id)
    assert reopened is not None
    assert reopened.state == models.ImageShardState.READY
    assert reopened.reserved_manifests < reopened.max_manifests
    assert reopened.reserved_declared_bytes < reopened.max_declared_bytes


def test_new_runtime_demand_readmits_evicted_location(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=100)
    eviction = _mark_readback(eviction, now=100)
    evicted = topology_state.complete_eviction(eviction.id,
                                               eviction.lease_token,
                                               present=False,
                                               now=101)
    assert evicted is not None
    assert evicted.state == models.ImageLocationState.EVICTED
    assert evicted.reserved_declared_bytes == 0
    target = profile.targets[0]
    with image_database.connect() as connection:
        current = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())
    active = _refresh_runtime_attestation(active,
                                          profile,
                                          target,
                                          backend='aws_vm',
                                          runtime_id=target.region,
                                          now=current)
    resources = types.SimpleNamespace(container_image=models.ContainerImage(
        release='boltz-l4', distribution=profile.name))
    metadata = _runtime_resolution(resources, profile, active,
                                   publication_record, evicted)
    monkeypatch.setattr(runtime, '_resolve_metadata',
                        lambda *_args, **_kwargs: metadata)

    with pytest.raises(runtime.ContainerImageWarmingError) as exc_info:
        runtime.resolve_for_placement(
            resources,
            models.Placement(provider='aws',
                             region=target.region,
                             backend='aws_vm',
                             platform='linux/amd64'),
            workspace='research',
            consumer_kind='service_version',
            consumer_owner='boltz-l4:incarnation:hash:v8',
            controller_epoch='service:hash:v8',
            controller_sequence=8,
            allow_epoch_advance=False)

    readmitted = topology_state.get_location(regional.id)
    assert readmitted is not None
    assert readmitted.state == models.ImageLocationState.PENDING
    assert (
        readmitted.reserved_declared_bytes == regional.reserved_declared_bytes)
    demand = demand_state.get_demand(exc_info.value.demand_id, 'research')
    assert demand is not None
    assert demand.state == models.ImageDemandState.WARMING
    assert demand.location_id == regional.id


def test_explicit_prepare_readmits_existing_evicted_location(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=100)
    eviction = _mark_readback(eviction, now=100)
    evicted = topology_state.complete_eviction(eviction.id,
                                               eviction.lease_token,
                                               present=False,
                                               now=101)
    assert evicted is not None
    assert evicted.state == models.ImageLocationState.EVICTED
    assert publication_record.image_id is not None

    mutation = preparation.prepare(image_id=publication_record.image_id,
                                   distribution=profile.name,
                                   target_id=profile.targets[0].name,
                                   workspace='research',
                                   actor_hash='2' * 64,
                                   idempotency_key='prepare-readmit-evicted')

    assert mutation.location.id == regional.id
    assert mutation.location.state == models.ImageLocationState.PENDING
    assert (mutation.location.reserved_declared_bytes ==
            regional.reserved_declared_bytes)
    assert mutation.operation.state == models.ImageOperationState.PENDING


def test_retired_live_demand_readmits_inventory_missing_location(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    owner = 'cluster:incarnation:stable'
    epoch = 'cluster-request:stable'
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner=owner,
                             consumer_kind='cluster',
                             controller_epoch=epoch,
                             controller_sequence=None,
                             now=50)
    with image_database.begin() as connection:
        connection.execute(schema.profile_revisions.update().where(
            schema.profile_revisions.c.id == active.id).values(
                state=models.ImageProfileState.RETIRED.value, updated_at=51))
        connection.execute(schema.locations.update().where(
            schema.locations.c.id == regional.id).values(
                state=models.ImageLocationState.MISSING.value,
                error_code=(
                    models.ImageLocationErrorCode.MANIFEST_MISSING.value),
                updated_at=51))
    retired = topology_state.get_profile_revision(active.id)
    missing = topology_state.get_location(regional.id)
    assert retired is not None
    assert retired.state == models.ImageProfileState.RETIRED
    assert missing is not None
    assert missing.state == models.ImageLocationState.MISSING
    resources = types.SimpleNamespace(container_image=models.ContainerImage(
        release='boltz-l4', distribution=profile.name))
    metadata = _runtime_resolution(resources,
                                   profile,
                                   retired,
                                   publication_record,
                                   missing,
                                   current_demand=demand)
    monkeypatch.setattr(runtime, '_resolve_metadata',
                        lambda *_args, **_kwargs: metadata)

    with pytest.raises(runtime.ContainerImageWarmingError) as exc_info:
        runtime.resolve_for_placement(
            resources,
            models.Placement(provider='aws',
                             region=profile.targets[0].region,
                             backend='aws_vm',
                             platform='linux/amd64'),
            workspace='research',
            consumer_kind='cluster',
            consumer_owner=owner,
            controller_epoch=epoch,
            controller_sequence=None,
            allow_epoch_advance=False,
            consumer_metadata={'request_id': 'request-1'})

    assert exc_info.value.demand_id == demand.id
    readmitted = topology_state.get_location(regional.id)
    assert readmitted is not None
    assert readmitted.state == models.ImageLocationState.PENDING


def test_ambiguous_delete_quarantines_physical_location(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, _, regional = _ready_regional(image_database, monkeypatch, profile)
    shard_before = topology_state.get_shard(regional.shard_id)
    assert shard_before is not None
    eviction = topology_state.claim_next_eviction(worker_id='lifecycle-1',
                                                  unused_before=1000,
                                                  lease_seconds=60,
                                                  now=100)
    assert eviction is not None and eviction.lease_token is not None
    eviction = _begin_delete(eviction, now=100)
    ambiguous = topology_state.complete_eviction(eviction.id,
                                                 eviction.lease_token,
                                                 present=None,
                                                 now=101)
    assert ambiguous is not None
    assert ambiguous.state == models.ImageLocationState.QUARANTINED
    assert ambiguous.lease_token is None
    assert ambiguous.error_code == (
        models.ImageLocationErrorCode.PROVIDER_OUTCOME_AMBIGUOUS.value)
    exact = topology_state.complete_eviction(eviction.id,
                                             eviction.lease_token,
                                             present=True,
                                             now=102)
    assert exact is None
    with pytest.raises(topology_state.RegistryLocationQuarantinedError,
                       match='REGISTRY_LOCATION_QUARANTINED'):
        preparation.retry_location(location_id=eviction.id,
                                   workspace='research',
                                   actor_hash='publisher',
                                   idempotency_key='retry-quarantined-location')
    with pytest.raises(topology_state.RegistryLocationQuarantinedError,
                       match='REGISTRY_LOCATION_QUARANTINED'):
        preparation.retry_location(location_id=eviction.id,
                                   workspace='research',
                                   actor_hash='publisher',
                                   idempotency_key='retry-quarantined-location')
    with image_database.connect() as connection:
        operation = connection.execute(
            sqlalchemy.select(schema.operations).where(
                schema.operations.c.idempotency_key ==
                'retry-quarantined-location')).mappings().one()
    assert operation['state'] == models.ImageOperationState.FAILED.value
    assert operation['error_code'] == 'REGISTRY_LOCATION_QUARANTINED'
    assert operation['result_id'] == eviction.id
    shard_after = topology_state.get_shard(regional.shard_id)
    assert shard_after is not None
    assert shard_after.reserved_manifests == shard_before.reserved_manifests
    assert (shard_after.reserved_declared_bytes ==
            shard_before.reserved_declared_bytes)
    assert shard_after.in_flight == 0
    queues, queues_truncated = topology_state.readiness_queue_stats(
        topology_state.list_shards('research', limit=1001))
    assert not queues_truncated
    queue = next(
        item for item in queues if item['target'] == profile.targets[0].name)
    assert queue['quarantined_count'] == 1
    assert not queue['quarantined_count_at_least']
    assert (queue['quarantined_reserved_declared_bytes'] ==
            regional.reserved_declared_bytes)
    assert not queue['quarantined_reserved_declared_bytes_at_least']


def test_failed_canonical_reap_waits_for_reservation_expiry_and_exact_absence(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    publication_record, location = _publish_and_bind(
        profile,
        source=_OTHER_SOURCE,
        runtime_digest=_OTHER_DIGEST,
        release='failed-release')
    claim = topology_state.claim_next_location(worker_id='copy-1',
                                               lease_seconds=60,
                                               workspace='research',
                                               now=30)
    assert claim is not None and claim.id == location.id
    assert claim.lease_token is not None
    failed = transactions.converge_canonical(location_id=claim.id,
                                             lease_token=claim.lease_token,
                                             ready=False,
                                             error_code='SOURCE_UNAVAILABLE',
                                             terminal=True,
                                             now=31)
    assert failed.state == models.ImageLocationState.FAILED
    assert topology_state.list_failed_canonical_reap_candidates() == []

    reservation_expiry = 31 + 30 * 24 * 60 * 60 + 1
    # The operation expires independently while the retained publication keeps
    # serving as durable release history with a nullable audit link.
    catalog_state.compact_terminal_records(now=reservation_expiry)
    assert catalog_state.get_operation(publication_record.operation_id,
                                       'research') is None
    with image_database.connect() as connection:
        operation_id = connection.execute(
            sqlalchemy.select(schema.publications.c.operation_id).where(
                schema.publications.c.id ==
                publication_record.id)).scalar_one()
    assert operation_id is None
    candidates = topology_state.list_failed_canonical_reap_candidates()
    assert [candidate.id for candidate in candidates] == [failed.id]
    assert not topology_state.reap_failed_canonical_reservation(
        failed.id,
        expected_updated_at=failed.updated_at,
        exact_absence=False,
        now=reservation_expiry + 1)
    assert not topology_state.reap_failed_canonical_reservation(
        failed.id,
        expected_updated_at=failed.updated_at + 1,
        exact_absence=True,
        now=reservation_expiry + 1)
    assert topology_state.reap_failed_canonical_reservation(
        failed.id,
        expected_updated_at=failed.updated_at,
        exact_absence=True,
        now=reservation_expiry + 1)
    assert topology_state.get_location(failed.id) is None


def test_readiness_projection_is_capped_and_index_headed_at_scale(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, _, canonical, regional = _ready_regional(image_database, monkeypatch,
                                                profile)
    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                WITH generated AS (
                    SELECT serial,
                           ('30000000-0000-4000-8000-' ||
                            lpad(serial::text, 12, '0')) AS image_id,
                           ('40000000-0000-4000-8000-' ||
                            lpad(serial::text, 12, '0')) AS location_id,
                           ('sha256:' || lpad(to_hex(serial), 64, '0')) AS digest
                    FROM generate_series(1, 20004) AS serial
                ), inserted_images AS (
                    INSERT INTO container_images
                        (id, workspace, runtime_digest, platform,
                         config_digest, manifest_media_type,
                         manifest_size_bytes, declared_size_bytes,
                         creator_user_hash, producer_kind, created_at,
                         updated_at)
                    SELECT image_id, 'research', digest, 'linux/amd64',
                           :config_digest, :media_type, 1, 1, 'scale-test',
                           'external_oci', serial, serial
                    FROM generated
                    RETURNING id
                )
                INSERT INTO container_image_locations
                    (id, workspace, image_id, shard_id, target_fingerprint,
                     physical_fingerprint, runtime_digest, canonical,
                     canonical_location_id, target_ref, state, attempt_count,
                     reserved_declared_bytes, created_at, updated_at)
                SELECT generated.location_id, 'research', generated.image_id,
                       :shard_id, :target_fingerprint, :physical_fingerprint,
                       generated.digest, FALSE, :canonical_location_id,
                       ('scale.example/repository@' || generated.digest),
                       CASE WHEN generated.serial <= 10002
                            THEN 'PENDING' ELSE 'FAILED' END,
                       0, 1, generated.serial, generated.serial
                FROM generated
                JOIN inserted_images
                  ON inserted_images.id = generated.image_id
            """), {
                'config_digest': _CONFIG_DIGEST,
                'media_type': _MANIFEST_MEDIA_TYPE,
                'shard_id': regional.shard_id,
                'target_fingerprint': regional.target_fingerprint,
                'physical_fingerprint': regional.physical_fingerprint,
                'canonical_location_id': canonical.id,
            })
        connection.execute(sqlalchemy.text('ANALYZE container_image_locations'))

    queues, queues_truncated = topology_state.readiness_queue_stats(
        topology_state.list_shards('research', limit=1001))
    assert not queues_truncated
    queue = next(
        item for item in queues if item['target'] == profile.targets[0].name)
    assert queue['queue_depth'] == 10_000
    assert queue['queue_depth_at_least']
    assert queue['failed_count'] == 10_000
    assert queue['failed_count_at_least']
    assert queue['oldest_queued_at'] == 1

    with image_database.connect() as connection:
        plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (FORMAT JSON)
                SELECT updated_at
                FROM container_image_locations
                WHERE shard_id = :shard_id AND state = 'PENDING'
                ORDER BY updated_at, id
                LIMIT 1
            """), {
                'shard_id': regional.shard_id
            }).scalar_one()
    assert 'ix_container_image_locations_shard_readiness' in str(plan)
    assert 'Limit' in str(plan)


def test_readiness_many_target_groups_use_one_capped_statement(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    template = topology_state.list_shards('research', limit=1001)[0]
    shards = [
        dataclasses.replace(
            template,
            id=f'readiness-scale-shard-{index}',
            target_id=f'readiness-scale-target-{index:03d}',
            target_fingerprint=f'readiness-scale-target-fingerprint-{index}',
            physical_fingerprint=(
                f'readiness-scale-physical-fingerprint-{index}'),
            shard_index=index,
        ) for index in range(105)
    ]
    statements: list[str] = []

    def record_statement(_connection, _cursor, statement, _parameters, _context,
                         _executemany) -> None:
        statements.append(statement)

    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            record_statement)
    try:
        queues, truncated = topology_state.readiness_queue_stats(shards)
    finally:
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                record_statement)

    assert truncated
    assert len(queues) == 100
    assert len(statements) == 1
    assert 'jsonb_to_recordset' in statements[0]
    assert 'array_agg' not in statements[0].lower()


def test_catalog_summary_is_capped_and_index_headed_for_hot_artifact(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, canonical, regional = _ready_regional(
        image_database, monkeypatch, profile)
    assert publication_record.image_id is not None
    authority_id = catalog_state.get_catalog_authority_id()
    assert authority_id is not None
    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_operations (
                    id, authority_id, scope, actor_hash, kind,
                    idempotency_key, request_hash, state, created_at,
                    updated_at, terminal_expires_at
                )
                SELECT 'catalog-scale-operation-' || series,
                       :authority_id, 'research', repeat('1', 64), 'PUBLISH',
                       'catalog-scale-publication-' || series,
                       repeat('2', 64), 'SUCCEEDED', 100 + series,
                       100 + series, 1000000 + series
                FROM generate_series(1, 20000) AS series
            """), {'authority_id': authority_id})
        connection.execute(schema.operations.insert().values(
            id='catalog-expired-operation',
            authority_id=authority_id,
            scope='research',
            actor_hash='1' * 64,
            kind='PUBLISH',
            idempotency_key='catalog-expired-operation-key',
            request_hash='2' * 64,
            state=models.ImageOperationState.SUCCEEDED.value,
            created_at=1,
            updated_at=1,
            terminal_expires_at=1))
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_sources (
                    id, workspace, image_id, source_ref,
                    source_root_digest, source_root_media_type,
                    requested_platform, selected_child_digest, created_at
                )
                SELECT 'catalog-scale-source-' || series, 'research',
                       :image_id,
                       ('registry.example/catalog-' || series ||
                        '@sha256:' || lpad(to_hex(series), 64, '0')),
                       'sha256:' || lpad(to_hex(series), 64, '0'),
                       :media_type, 'linux/amd64', :runtime_digest,
                       100 + series
                FROM generate_series(1, 20000) AS series
            """), {
                'image_id': publication_record.image_id,
                'media_type': _MANIFEST_MEDIA_TYPE,
                'runtime_digest': _DIGEST,
            })
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_publications (
                    id, workspace, operation_id, profile_revision_id,
                    requested_release, reservation_active, source_ref,
                    source_root_digest, requested_platform, state, image_id,
                    source_id, canonical_location_id, created_at, updated_at
                )
                SELECT 'catalog-scale-publication-' || series, 'research',
                       'catalog-scale-operation-' || series,
                       :profile_revision_id,
                       'catalog-scale-release-' || series, TRUE,
                       ('registry.example/catalog-' || series ||
                        '@sha256:' || lpad(to_hex(series), 64, '0')),
                       'sha256:' || lpad(to_hex(series), 64, '0'),
                       'linux/amd64', 'READY', :image_id,
                       'catalog-scale-source-' || series,
                       :canonical_location_id, 100 + series, 100 + series
                FROM generate_series(1, 20000) AS series
            """), {
                'profile_revision_id': active.id,
                'image_id': publication_record.image_id,
                'canonical_location_id': canonical.id,
            })
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_locations (
                    id, workspace, image_id, shard_id, target_fingerprint,
                    physical_fingerprint, runtime_digest, canonical,
                    canonical_location_id, target_ref, state, attempt_count,
                    reserved_declared_bytes, created_at, updated_at
                )
                SELECT 'catalog-scale-location-' || series, 'research',
                       :image_id, :shard_id,
                       'catalog-scale-target-' || series,
                       'catalog-scale-physical-' || series, :runtime_digest,
                       FALSE, :canonical_location_id,
                       ('registry.example/location-' || series || '@' ||
                        :runtime_digest),
                       'READY', 0, 1, 100 + series, 100 + series
                FROM generate_series(1, 20000) AS series
            """), {
                'image_id': publication_record.image_id,
                'shard_id': regional.shard_id,
                'runtime_digest': _DIGEST,
                'canonical_location_id': canonical.id,
            })
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_publications (
                    id, workspace, operation_id, profile_revision_id,
                    requested_release, reservation_active, source_ref,
                    source_root_digest, requested_platform, state,
                    reservation_expires_at, record_expires_at,
                    created_at, updated_at
                )
                SELECT 'catalog-expiring-publication-' || series, 'research',
                       'catalog-scale-operation-' || series,
                       :profile_revision_id,
                       'catalog-expiring-release-' || series, TRUE,
                       :source_ref, :runtime_digest, 'linux/amd64', 'FAILED',
                       series, 1000000 + series, 100 + series, 100 + series
                FROM generate_series(1, 20000) AS series
            """), {
                'profile_revision_id': active.id,
                'source_ref': _OTHER_SOURCE,
                'runtime_digest': _OTHER_DIGEST,
            })
        connection.execute(schema.publications.insert().values(
            id='catalog-terminal-publication',
            workspace='research',
            operation_id='catalog-scale-operation-1',
            profile_revision_id=active.id,
            requested_release='catalog-terminal-release',
            reservation_active=False,
            source_ref=_OTHER_SOURCE,
            source_root_digest=_OTHER_DIGEST,
            requested_platform='linux/amd64',
            state=models.ImagePublicationState.FAILED.value,
            record_expires_at=1,
            created_at=1,
            updated_at=1))
        for table in ('container_image_publications', 'container_image_sources',
                      'container_image_locations',
                      'container_image_operations'):
            connection.execute(sqlalchemy.text(f'ANALYZE {table}'))

    statements: list[str] = []

    def record_statement(_connection, _cursor, statement, _parameters, _context,
                         _executemany) -> None:
        statements.append(statement)

    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            record_statement)
    try:
        summary = catalog_state.catalog_summaries(
            {publication_record.image_id},
            'research')[publication_record.image_id]
    finally:
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                record_statement)

    assert len(statements) == 3
    assert all('LIMIT' in statement for statement in statements)
    assert summary['publications_truncated']
    assert summary['sources_truncated']
    assert summary['locations_truncated']
    assert len(summary['releases']) <= 10
    assert len(summary['source_refs']) <= 10
    assert sum(summary['location_states'].values()) == 10

    with image_database.connect() as connection:
        publication_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT requested_release, created_at, id
                FROM container_image_publications
                WHERE image_id = :image_id AND state = 'READY'
                  AND reservation_active IS TRUE
                ORDER BY created_at, id
                LIMIT 11
            """), {
                'image_id': publication_record.image_id
            }).scalars().all()
        source_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT source_ref, created_at, id
                FROM container_image_sources
                WHERE image_id = :image_id
                ORDER BY created_at, id
                LIMIT 11
            """), {
                'image_id': publication_record.image_id
            }).scalars().all()
        location_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT state, created_at, id
                FROM container_image_locations
                WHERE image_id = :image_id
                ORDER BY created_at, id
                LIMIT 11
            """), {
                'image_id': publication_record.image_id
            }).scalars().all()
        workspace_history_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE workspace = 'research'
                ORDER BY created_at DESC, id DESC
                LIMIT 51
            """)).scalars().all()
        workspace_state_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE workspace = 'research' AND state = 'READY'
                ORDER BY created_at DESC, id DESC
                LIMIT 51
            """)).scalars().all()
        workspace_release_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE workspace = 'research'
                  AND requested_release = 'catalog-scale-release-10000'
                ORDER BY created_at DESC, id DESC
                LIMIT 51
            """)).scalars().all()
        ready_history_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE image_id = :image_id AND state = 'READY'
                  AND reservation_active IS TRUE
                ORDER BY updated_at DESC, id DESC
                LIMIT 51
            """), {
                'image_id': publication_record.image_id
            }).scalars().all()
        failed_expiry_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE state = 'FAILED' AND reservation_active IS TRUE
                  AND reservation_expires_at <= 100
                ORDER BY reservation_expires_at, id
                LIMIT 500 FOR UPDATE SKIP LOCKED
            """)).scalars().all()
        terminal_expiry_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE reservation_active IS FALSE
                  AND record_expires_at <= 100
                ORDER BY record_expires_at, id
                LIMIT 500 FOR UPDATE SKIP LOCKED
            """)).scalars().all()
        fanout_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT DISTINCT publication.canonical_location_id
                FROM container_image_publications AS publication
                JOIN container_image_locations AS location
                  ON location.id = publication.canonical_location_id
                WHERE publication.state = 'PENDING'
                  AND publication.canonical_location_id IS NOT NULL
                  AND location.canonical IS TRUE
                  AND location.state IN ('READY', 'FAILED')
                ORDER BY publication.canonical_location_id
                LIMIT 100
            """)).scalars().all()
        inventory_digest_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_locations
                WHERE shard_id = :shard_id
                  AND runtime_digest = :runtime_digest
            """), {
                'shard_id': regional.shard_id,
                'runtime_digest': _OTHER_DIGEST,
            }).scalars().all()
        operation_expiry_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_operations
                WHERE terminal_expires_at <= 100
                ORDER BY terminal_expires_at, id
                LIMIT 500 FOR UPDATE SKIP LOCKED
            """)).scalars().all()
        operation_link_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE operation_id = 'catalog-scale-operation-10000'
            """)).scalars().all()
    assert 'ix_container_image_publications_active_image' in str(
        publication_plan)
    assert 'ix_container_image_sources_image' in str(source_plan)
    assert 'ix_container_image_locations_artifact' in str(location_plan)
    assert ('ix_container_image_publications_workspace_history'
            in str(workspace_history_plan))
    assert ('ix_container_image_publications_workspace_state_history'
            in str(workspace_state_plan))
    assert ('ix_container_image_publications_workspace_release_history'
            in str(workspace_release_plan))
    assert ('ix_container_image_publications_ready_history'
            in str(ready_history_plan))
    assert ('ix_container_image_publications_failed_reservation_expiry'
            in str(failed_expiry_plan))
    assert ('ix_container_image_publications_terminal_expiry'
            in str(terminal_expiry_plan))
    assert 'ix_container_image_publications_fanout' in str(fanout_plan)
    assert ('ix_container_image_locations_inventory_digest'
            in str(inventory_digest_plan))
    assert 'ix_container_image_operations_expiry' in str(operation_expiry_plan)
    assert ('ix_container_image_publications_operation'
            in str(operation_link_plan))

    compaction_statements: list[str] = []

    def record_compaction(_connection, _cursor, statement, _parameters,
                          _context, _executemany) -> None:
        compaction_statements.append(statement)

    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            record_compaction)
    try:
        deleted_publications, deleted_operations = (
            catalog_state.compact_terminal_records(now=100, batch_size=500))
    finally:
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                record_compaction)
    assert deleted_publications == 1
    assert deleted_operations == 1
    operation_claims = [
        statement for statement in compaction_statements
        if 'FROM container_image_operations' in statement and
        'FOR UPDATE' in statement
    ]
    assert len(operation_claims) == 1
    assert 'EXISTS' not in operation_claims[0].upper()
    assert 'container_image_publications' not in operation_claims[0]


def test_operational_profile_readiness_excludes_unbounded_history_and_uses_indexes(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    snapshot = json.dumps(profile.to_snapshot(),
                          sort_keys=True,
                          separators=(',', ':'))
    rows = [{
        'id': f'history-{index}',
        'workspace': 'research',
        'profile': 'history-heavy',
        'revision': index + 1,
        'desired_generation': index + 1,
        'state': models.ImageProfileState.SUPERSEDED.value,
        'config_hash': profile.config_hash,
        'config_json': snapshot,
        'physical_manifest_hash': profile.physical_manifest_hash,
        'created_at': index + 1,
        'updated_at': index + 1,
    } for index in range(1500)]
    rows.extend(({
        'id': 'operational-active',
        'workspace': 'research',
        'profile': 'operational',
        'revision': 1,
        'desired_generation': 1,
        'state': models.ImageProfileState.ACTIVE.value,
        'config_hash': profile.config_hash,
        'config_json': snapshot,
        'physical_manifest_hash': profile.physical_manifest_hash,
        'created_at': 2001,
        'updated_at': 2001,
    }, {
        'id': 'operational-qualifying',
        'workspace': 'research',
        'profile': 'operational',
        'revision': 2,
        'desired_generation': 2,
        'state': models.ImageProfileState.QUALIFYING.value,
        'config_hash': profile.config_hash,
        'config_json': snapshot,
        'physical_manifest_hash': profile.physical_manifest_hash,
        'created_at': 2002,
        'updated_at': 2002,
    }))
    with image_database.begin() as connection:
        connection.execute(schema.profile_revisions.insert(), rows)
        connection.execute(
            sqlalchemy.text('ANALYZE container_image_profile_revisions'))

    operational = topology_state.list_operational_profile_revisions('research')

    assert [(item.id, item.state) for item in operational] == [
        ('operational-active', models.ImageProfileState.ACTIVE),
        ('operational-qualifying', models.ImageProfileState.QUALIFYING),
    ]
    with image_database.connect() as connection:
        plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id, profile, state FROM (
                    SELECT id, profile, state
                    FROM container_image_profile_revisions
                    WHERE workspace = 'research' AND state = 'ACTIVE'
                    UNION ALL
                    SELECT id, profile, state
                    FROM container_image_profile_revisions
                    WHERE workspace = 'research' AND state = 'QUALIFYING'
                ) AS operational
                ORDER BY profile, state, id
                LIMIT 1001
            """)).scalars().all()
        qualification_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_profile_revisions
                WHERE state IN ('QUALIFYING', 'ACTIVE')
                ORDER BY updated_at, id
                LIMIT 100
            """)).scalars().all()
    assert 'uq_container_image_profile_active' in str(plan)
    assert 'uq_container_image_profile_desired' in str(plan)
    assert ('ix_container_image_profile_qualification_queue'
            in str(qualification_plan))


@pytest.mark.parametrize('state', [
    models.ImageProfileState.QUALIFYING,
    models.ImageProfileState.ACTIVE,
])
def test_profile_staging_exact_operational_replay_is_idempotent(
        image_database, profile: models.ManagedRegistryProfile,
        state: models.ImageProfileState) -> None:
    staged = _stage_candidate_profile(profile, now=10)
    if state == models.ImageProfileState.ACTIVE:
        with image_database.begin() as connection:
            connection.execute(schema.profile_revisions.update().where(
                schema.profile_revisions.c.id == staged.id).values(
                    state=state.value))
    expected = topology_state.get_profile_revision(staged.id)
    assert expected is not None

    replayed = _stage_candidate_profile(profile, now=20)

    assert replayed == expected
    assert topology_state.list_profile_revisions(
        'research', profile=profile.name) == [expected]


@pytest.mark.parametrize(('column', 'mismatched_value'), [
    ('config_hash', '0' * 64),
    ('config_json', '{}'),
    ('physical_manifest_hash', '0' * 64),
])
@pytest.mark.parametrize('state', [
    models.ImageProfileState.QUALIFYING,
    models.ImageProfileState.ACTIVE,
])
def test_profile_staging_rejects_immutable_mismatch_before_mutation(
        image_database, profile: models.ManagedRegistryProfile, column: str,
        mismatched_value: str, state: models.ImageProfileState) -> None:
    staged = _stage_candidate_profile(profile, now=10)
    with image_database.begin() as connection:
        connection.execute(schema.profile_revisions.update().where(
            schema.profile_revisions.c.id == staged.id).values(
                state=state.value, **{column: mismatched_value}))
    statements: list[str] = []

    def record_statements(_connection, _cursor, statement, _parameters,
                          _context, _executemany) -> None:
        statements.append(statement)

    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            record_statements)
    try:
        with pytest.raises(ValueError, match='immutable payload mismatch'):
            _stage_candidate_profile(profile, now=20)
    finally:
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                record_statements)

    assert not any(statement.lstrip().upper().startswith(('INSERT', 'UPDATE'))
                   for statement in statements)
    current = topology_state.get_profile_revision(staged.id)
    assert current is not None
    assert current.state == state
    assert current.desired_generation == staged.desired_generation
    assert len(
        topology_state.list_profile_revisions('research',
                                              profile=profile.name)) == 1


@pytest.mark.parametrize('state', [
    models.ImageProfileState.SUPERSEDED,
    models.ImageProfileState.FAILED,
    models.ImageProfileState.RETIRED,
])
def test_profile_staging_rejects_non_operational_revision_replay(
        image_database, profile: models.ManagedRegistryProfile,
        state: models.ImageProfileState) -> None:
    """Re-staging a settled revision must reject cleanly, not wedge the op.

    A candidate row whose immutable payload matches but whose state is no
    longer operational (superseded/failed/retired/disabled) must raise a
    caller-catchable ValueError. Without the guard the stage falls through to
    the INSERT, which violates uq_container_image_profile_revision and raises
    a bare IntegrityError that ``qualification.ingest_manifest`` cannot catch,
    leaving the operation retrying the same idempotency key forever.
    """
    staged = _stage_candidate_profile(profile, now=10)
    with image_database.begin() as connection:
        connection.execute(schema.profile_revisions.update().where(
            schema.profile_revisions.c.id == staged.id).values(
                state=state.value))
    statements: list[str] = []

    def record_statements(_connection, _cursor, statement, _parameters,
                          _context, _executemany) -> None:
        statements.append(statement)

    sqlalchemy.event.listen(image_database, 'before_cursor_execute',
                            record_statements)
    try:
        with pytest.raises(ValueError, match='no longer operational'):
            _stage_candidate_profile(profile, now=20)
    finally:
        sqlalchemy.event.remove(image_database, 'before_cursor_execute',
                                record_statements)

    # The rejection must happen before any row mutation so the wave stays
    # retryable and the unique constraint is never tripped.
    assert not any(statement.lstrip().upper().startswith(('INSERT', 'UPDATE'))
                   for statement in statements)
    current = topology_state.get_profile_revision(staged.id)
    assert current is not None
    assert current.state == state
    assert current.desired_generation == staged.desired_generation
    assert len(
        topology_state.list_profile_revisions('research',
                                              profile=profile.name)) == 1


def test_qualification_ingest_immutable_revision_conflict_terminalizes_operation(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    existing_profile = dataclasses.replace(profile, realm='legacy')
    staged = _stage_candidate_profile(existing_profile, now=10)
    parsed = types.SimpleNamespace(profile=profile.name,
                                   workspace='research',
                                   manifest_hash='1' * 64)
    monkeypatch.setattr(qualification.aws.TerraformQualificationManifest,
                        'from_json', staticmethod(lambda _payload: parsed))
    monkeypatch.setattr(
        qualification.aws, 'ingest_terraform_qualification',
        lambda _payload: _stage_candidate_profile(profile, now=20))

    with pytest.raises(ValueError, match='^QUALIFICATION_FAILED$'):
        qualification.ingest_manifest(
            profile_name=profile.name,
            manifest={'profile': profile.name},
            actor_hash='2' * 64,
            idempotency_key='qualification-conflict-key')

    with image_database.connect() as connection:
        row = connection.execute(sqlalchemy.select(
            schema.operations)).mappings().one()
    operation = catalog_state._operation(row)
    assert operation.kind == 'PROFILE_QUALIFY'
    assert operation.state == models.ImageOperationState.FAILED
    assert operation.result_kind == 'qualification'
    assert operation.result_id == operation.id
    assert operation.result == {
        'profile': profile.name,
        'state': models.ImageOperationState.FAILED.value,
    }
    assert operation.error_code == 'QUALIFICATION_FAILED'
    assert operation.terminal_expires_at is not None
    current = topology_state.get_profile_revision(staged.id)
    assert current is not None
    assert current.config_hash == existing_profile.config_hash
    assert topology_state.list_profile_revisions(
        'research', profile=profile.name) == [current]


def test_profile_staging_reads_constant_rows_with_large_retained_history(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    candidate = dataclasses.replace(profile,
                                    name='mutation-scale',
                                    revision=20001)
    snapshot = json.dumps(candidate.to_snapshot(),
                          sort_keys=True,
                          separators=(',', ':'))
    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_profile_revisions (
                    id, workspace, profile, revision, desired_generation,
                    state, config_hash, config_json, physical_manifest_hash,
                    created_at, updated_at
                )
                SELECT 'mutation-history-' || value, 'research',
                       'mutation-scale', value, value, 'SUPERSEDED',
                       :config_hash, :config_json, :physical_manifest_hash,
                       value, value
                FROM generate_series(1, 20000) AS value
            """), {
                'config_hash': candidate.config_hash,
                'config_json': snapshot,
                'physical_manifest_hash': candidate.physical_manifest_hash,
            })
        connection.execute(
            sqlalchemy.text('ANALYZE container_image_profile_revisions'))

    returned_rows = []
    statements: list[str] = []

    def record_rows(_connection, cursor, statement, _parameters, _context,
                    _executemany):
        statements.append(statement)
        if statement.lstrip().upper().startswith(('SELECT', 'WITH')):
            returned_rows.append(cursor.rowcount)

    sqlalchemy.event.listen(image_database, 'after_cursor_execute', record_rows)
    try:
        staged = topology_state.stage_profile_revision(
            workspace='research',
            profile=candidate.name,
            revision=candidate.revision,
            config_hash=candidate.config_hash,
            config_snapshot=candidate.to_snapshot(),
            physical_manifest_hash=candidate.physical_manifest_hash,
            max_daily_canary_microusd=(
                candidate.qualification.max_daily_canary_microusd),
            now=20001)
    finally:
        sqlalchemy.event.remove(image_database, 'after_cursor_execute',
                                record_rows)

    assert staged.desired_generation == 20001
    assert returned_rows
    assert max(returned_rows) <= 1
    assert all('container_image_publications' not in statement
               for statement in statements)
    with image_database.connect() as connection:
        generation_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT max(desired_generation)
                FROM container_image_profile_revisions
                WHERE workspace = 'research' AND profile = 'mutation-scale'
            """)).scalars().all()
        revision_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_profile_revisions
                WHERE workspace = 'research' AND profile = 'mutation-scale'
                  AND revision = 20001
            """)).scalars().all()
        custody_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT physical_manifest_hash
                FROM container_image_profile_custody
                WHERE workspace = 'research' AND profile = 'mutation-scale'
            """)).scalars().all()
    assert 'uq_container_image_profile_generation' in str(generation_plan)
    assert 'uq_container_image_profile_revision' in str(revision_plan)
    assert 'container_image_profile_custody_pkey' in str(custody_plan)


def test_profile_staging_is_serialized_by_transaction_advisory_lock(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    first = dataclasses.replace(profile, name='serialized-profile', revision=1)
    second = dataclasses.replace(profile, name='serialized-profile', revision=2)
    lock_key = json.dumps(['research', first.name], separators=(',', ':'))
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
        {'key': f'skypilot:container-image-profile:{lock_key}'})
    barrier = threading.Barrier(3)

    def stage(candidate: models.ManagedRegistryProfile):
        barrier.wait(timeout=5)
        return topology_state.stage_profile_revision(
            workspace='research',
            profile=candidate.name,
            revision=candidate.revision,
            config_hash=candidate.config_hash,
            config_snapshot=candidate.to_snapshot(),
            physical_manifest_hash=candidate.physical_manifest_hash,
            max_daily_canary_microusd=(
                candidate.qualification.max_daily_canary_microusd),
            now=candidate.revision)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(stage, candidate)
                for candidate in (first, second)
            ]
            barrier.wait(timeout=5)
            time.sleep(0.2)
            assert not any(future.done() for future in futures)
            lock_transaction.commit()
            results = [future.result(timeout=10) for future in futures]
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    assert {result.desired_generation for result in results} == {1, 2}
    revisions = topology_state.list_profile_revisions(
        'research', profile='serialized-profile')
    assert len(revisions) == 2
    assert [revision.state for revision in revisions
           ].count(models.ImageProfileState.QUALIFYING) == 1
    assert [revision.state for revision in revisions
           ].count(models.ImageProfileState.SUPERSEDED) == 1


def test_profile_staging_locks_current_candidate_before_catalog_barrier(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    current = _stage_candidate_profile(profile, now=10)
    successor = dataclasses.replace(profile, revision=profile.revision + 1)
    original_mutation = topology_state.get_qualification_mutation_in_session
    observed = False

    def _observe_catalog_lock(
        session: orm.Session,
        *,
        exclusive: bool,
    ) -> sqlalchemy.engine.RowMapping | None:
        nonlocal observed
        assert not exclusive
        with image_database.connect() as connection:
            transaction = connection.begin()
            try:
                with pytest.raises(sqlalchemy.exc.OperationalError) as error:
                    connection.execute(
                        sqlalchemy.select(schema.profile_revisions).where(
                            schema.profile_revisions.c.id ==
                            current.id).with_for_update(nowait=True)).all()
                assert getattr(error.value.orig, 'pgcode', None) == '55P03'
            finally:
                transaction.rollback()
        observed = True
        return original_mutation(session, exclusive=exclusive)

    monkeypatch.setattr(topology_state, 'get_qualification_mutation_in_session',
                        _observe_catalog_lock)

    staged = _stage_candidate_profile(successor, now=20)

    assert observed
    assert staged.revision == successor.revision
    prior = topology_state.get_profile_revision(current.id)
    assert prior is not None
    assert prior.state == models.ImageProfileState.SUPERSEDED


def test_profile_attestation_is_serialized_by_transaction_advisory_lock(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    revision = _activate_profile(image_database, profile)
    lock_key = json.dumps(['research', profile.name], separators=(',', ':'))
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
        {'key': f'skypilot:container-image-profile:{lock_key}'})

    def attest() -> topology_state.ProfileRevisionRecord:
        return topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind='blocking-profile-lock-proof',
            evidence={
                'status': 'READY',
                'observed_at': -1
            },
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=revision.updated_at + 1)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(attest)
            time.sleep(0.2)
            assert not future.done()
            lock_transaction.commit()
            recorded = future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    assert recorded.attestations['blocking-profile-lock-proof'][
        'observed_at'] == revision.updated_at + 1


@pytest.mark.parametrize('host_offset', (-86_400, 86_400))
def test_copy_qualification_due_check_uses_database_clock_under_host_skew(
        image_database, profile: models.ManagedRegistryProfile,
        monkeypatch: pytest.MonkeyPatch, host_offset: int) -> None:
    revision = _activate_profile(image_database, profile)
    target = profile.targets[0]
    database_now = copy_worker_service._qualification_database_epoch()
    attestations = dict(revision.attestations)
    attestations[models.profile_attestation_key('copy', target.name)] = {
        'status': 'READY',
        'observed_at': database_now,
        'target_fingerprint': target.target_fingerprint,
        'runtime_digest': _DIGEST,
        'platform': profile.qualification.canary_platform,
    }
    qualifying = dataclasses.replace(revision,
                                     state=models.ImageProfileState.QUALIFYING,
                                     attestations=attestations)
    monkeypatch.setattr(copy_worker_service.time, 'time',
                        lambda: database_now + host_offset)

    due_now = copy_worker_service._qualification_database_epoch()

    assert not copy_worker_service._qualification_copy_needed(
        qualifying, profile, target, due_now)


def test_exact_absence_requires_same_second_copy_restoration_handshake(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    lifecycle_proof_id = '00000000-0000-4000-8000-000000000099'
    repository_arn = _qualification_repository_arn(profile, target)
    copy_key = models.profile_attestation_key('copy', target.name)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    attestations = dict(active.attestations)
    attestations[copy_key] = {
        'status': 'READY',
        'observed_at': 20,
        'target_fingerprint': target.target_fingerprint,
        'repository_arn': repository_arn,
        'runtime_digest': _DIGEST,
        'platform': profile.qualification.canary_platform,
    }
    attestations[lifecycle_key] = {
        'status': 'READY',
        'observed_at': 20,
        'target_fingerprint': target.target_fingerprint,
        'repository_arn': repository_arn,
        'runtime_digest': _DIGEST,
        'exact_absence': True,
        'lifecycle_proof_id': lifecycle_proof_id,
        'protocol_version': 2,
    }
    deleted = dataclasses.replace(active, attestations=attestations)

    assert not qualification.qualification_copy_available(
        deleted, profile, target)
    assert copy_worker_service._qualification_copy_needed(
        deleted, profile, target, 20)

    restoration = qualification.qualification_copy_restoration_evidence(
        qualification.qualification_copy_restoration_proof_id(
            deleted, target, _DIGEST))
    assert restoration == {
        'restores_lifecycle_proof_id': lifecycle_proof_id,
    }
    attestations[copy_key] = {
        **attestations[copy_key],
        **restoration,
    }
    restored = dataclasses.replace(active, attestations=attestations)

    assert qualification.qualification_copy_available(restored, profile, target)
    assert not copy_worker_service._qualification_copy_needed(
        restored, profile, target, 20)


def _short_lived_qualifying_profile(
    image_database, profile: models.ManagedRegistryProfile
) -> tuple[models.ManagedRegistryProfile, topology_state.ProfileRevisionRecord,
           int]:
    short_lived = dataclasses.replace(
        profile,
        qualification=dataclasses.replace(
            profile.qualification, runtime_attestation_max_age_seconds=2))
    active = _activate_profile(image_database, short_lived)
    requirements = qualification._attestation_requirements(  # pylint: disable=protected-access
        short_lived, active.attestations)
    revision = active
    for key in requirements:
        existing = revision.attestations.get(key)
        evidence = (dict(existing) if isinstance(existing, dict) else {
            'status': 'READY'
        })
        for target in (short_lived.canonical,) + short_lived.targets:
            repository_name, repository_arn = (
                _generated_qualification_repository(short_lived, target))
            lifecycle_proof_id = str(
                uuid.uuid5(uuid.NAMESPACE_DNS, f'skypilot-test:{target.name}'))
            if key == models.profile_attestation_key('terraform_target',
                                                     target.name):
                evidence.update(target_fingerprint=target.target_fingerprint,
                                registry=target.registry,
                                repository_name=repository_name,
                                repository_arn=repository_arn,
                                qualification_repository_generation=(
                                    target.qualification_repository_generation))
            elif key == models.profile_attestation_key('copy', target.name):
                evidence.update(
                    target_fingerprint=target.target_fingerprint,
                    repository_arn=repository_arn,
                    runtime_digest=_DIGEST,
                    platform=short_lived.qualification.canary_platform,
                    restores_lifecycle_proof_id=lifecycle_proof_id)
            elif key == models.profile_attestation_key('lifecycle',
                                                       target.name):
                evidence.update(target_fingerprint=target.target_fingerprint,
                                repository_arn=repository_arn,
                                runtime_digest=_DIGEST,
                                exact_absence=True,
                                lifecycle_proof_id=lifecycle_proof_id,
                                protocol_version=2)
        observed_at = int(time.time())
        evidence.update(status='READY', observed_at=observed_at)
        revision = topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind=key,
            evidence=evidence,
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=observed_at)
    scheduler_now = int(time.time())
    for key, max_age in requirements.items():
        if max_age != 2:
            continue
        evidence = dict(revision.attestations[key])
        evidence['observed_at'] = scheduler_now
        revision = topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind=key,
            evidence=evidence,
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=scheduler_now)
    assert revision.attestations_hash is not None
    with image_database.begin() as connection:
        connection.execute(schema.profile_revisions.update().where(
            schema.profile_revisions.c.id == revision.id).values(
                state=models.ImageProfileState.QUALIFYING.value,
                qualified_at=None))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.workspace == 'research',
            schema.registry_shards.c.profile == short_lived.name).values(
                profile_revision_id=None))
    return short_lived, revision, scheduler_now


@pytest.mark.parametrize(('proof_offset', 'host_offset', 'expected'),
                         ((-1000, -1000, 0), (0, 86_400, 2)))
def test_automatic_canary_scheduler_uses_database_clock_under_host_skew(
        image_database, profile: models.ManagedRegistryProfile,
        monkeypatch: pytest.MonkeyPatch, proof_offset: int, host_offset: int,
        expected: int) -> None:
    active = _activate_profile(image_database, profile)
    with image_database.connect() as connection:
        database_now = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    lifecycle_proof_id = '00000000-0000-4000-8000-000000000096'
    attestations = dict(active.attestations)
    attestations[models.profile_attestation_key('copy', target.name)] = {
        'status': 'READY',
        'observed_at': database_now + proof_offset,
        'target_fingerprint': target.target_fingerprint,
        'repository_arn': repository_arn,
        'runtime_digest': _DIGEST,
        'platform': profile.qualification.canary_platform,
        'restores_lifecycle_proof_id': lifecycle_proof_id,
    }
    attestations[models.profile_attestation_key('lifecycle', target.name)] = {
        'status': 'ARMED',
        'observed_at': database_now + proof_offset,
        'target_fingerprint': target.target_fingerprint,
        'repository_arn': repository_arn,
        'runtime_digest': _DIGEST,
        'lifecycle_proof_id': lifecycle_proof_id,
        'protocol_version': 2,
    }
    revision = dataclasses.replace(active, attestations=attestations)
    monkeypatch.setattr(topology_state, 'list_qualifying_profiles',
                        lambda **_kwargs: [revision])
    requested: list[dict[str, Any]] = []
    monkeypatch.setattr(qualification, 'request_canary',
                        lambda **kwargs: requested.append(kwargs))
    monkeypatch.setattr(time, 'time', lambda: database_now + host_offset)

    assert qualification.schedule_automatic_canaries() == expected
    assert len(requested) == expected


def test_profile_activation_preflight_ignores_fast_host_clock(
        image_database, profile: models.ManagedRegistryProfile,
        monkeypatch: pytest.MonkeyPatch) -> None:
    _, revision, scheduler_now = _short_lived_qualifying_profile(
        image_database, profile)
    monkeypatch.setattr(time, 'time', lambda: scheduler_now + 86_400)

    activated = qualification.maybe_activate_profile(revision.id)

    assert activated is not None
    assert activated.state == models.ImageProfileState.ACTIVE


def test_profile_activation_rejects_mismatched_copy_restoration_proof(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    short_lived, revision, scheduler_now = _short_lived_qualifying_profile(
        image_database, profile)
    target = short_lived.targets[0]
    copy_key = models.profile_attestation_key('copy', target.name)
    copy_evidence = dict(revision.attestations[copy_key])
    copy_evidence['restores_lifecycle_proof_id'] = (
        '00000000-0000-4000-8000-000000000099')
    revision = topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind=copy_key,
        evidence=copy_evidence,
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash,
        now=scheduler_now)

    activated = qualification.maybe_activate_profile(revision.id,
                                                     now=scheduler_now)

    assert activated is None
    assert revision.terraform_hash is not None
    assert revision.attestations_hash is not None
    requirements = qualification._attestation_requirements(  # pylint: disable=protected-access
        short_lived, revision.attestations)
    with pytest.raises(ValueError, match='QUALIFICATION_FAILED'):
        transactions.activate_profile(
            profile_revision_id=revision.id,
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            expected_terraform_hash=revision.terraform_hash,
            expected_attestations_hash=revision.attestations_hash,
            required_attestations=requirements,
            now=scheduler_now)
    unchanged = topology_state.get_profile_revision(revision.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageProfileState.QUALIFYING


def test_profile_activation_accepts_old_exact_lifecycle_proof(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    short_lived, revision, scheduler_now = _short_lived_qualifying_profile(
        image_database, profile)
    old_observed_at = scheduler_now - 86_400
    for target in (short_lived.canonical,) + short_lived.targets:
        lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
        lifecycle = dict(revision.attestations[lifecycle_key])
        revision = topology_state.record_profile_attestation(
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=lifecycle,
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=old_observed_at)

    requirements = qualification._attestation_requirements(  # pylint: disable=protected-access
        short_lived, revision.attestations)
    for target in (short_lived.canonical,) + short_lived.targets:
        assert requirements[models.profile_attestation_key(
            'lifecycle', target.name)] is None
        assert requirements[models.profile_attestation_key(
            'copy', target.name)] == 10 * 60

    activated = qualification.maybe_activate_profile(revision.id,
                                                     now=scheduler_now)

    assert activated is not None
    assert activated.state == models.ImageProfileState.ACTIVE


def test_profile_activation_rechecks_freshness_after_advisory_lock(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    short_lived, revision, _ = _short_lived_qualifying_profile(
        image_database, profile)

    lock_key = json.dumps(['research', short_lived.name], separators=(',', ':'))
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
        {'key': f'skypilot:container-image-profile:{lock_key}'})
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(qualification.maybe_activate_profile,
                                     revision.id)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(3.2)')
            lock_transaction.commit()
            assert future.result(timeout=10) is None
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    unchanged = topology_state.get_profile_revision(revision.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageProfileState.QUALIFYING


def test_lifecycle_reconciliation_does_not_forward_scheduler_clock(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    short_lived, revision, scheduler_now = _short_lived_qualifying_profile(
        image_database, profile)
    lock_key = json.dumps(['research', short_lived.name], separators=(',', ':'))
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
        {'key': f'skypilot:container-image-profile:{lock_key}'})
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                lifecycle_worker_service.reconcile_qualification_lifecycle,
                types.SimpleNamespace(),
                now=scheduler_now)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(3.2)')
            lock_transaction.commit()
            assert future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    unchanged = topology_state.get_profile_revision(revision.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageProfileState.QUALIFYING


def test_canonical_ready_takes_profile_lock_and_permanently_fences_custody(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    _, location = _publish_and_bind(profile)
    claim = topology_state.claim_next_location(worker_id='copy-1',
                                               lease_seconds=60,
                                               workspace='research',
                                               now=30)
    assert claim is not None and claim.id == location.id
    assert claim.lease_token is not None
    assert topology_state.transition_location_to_verifying(claim.id,
                                                           claim.lease_token,
                                                           now=31)
    lock_key = json.dumps(['research', profile.name], separators=(',', ':'))
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
        {'key': f'skypilot:container-image-profile:{lock_key}'})
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(transactions.converge_canonical,
                                     location_id=claim.id,
                                     lease_token=claim.lease_token,
                                     ready=True,
                                     now=32)
            time.sleep(0.2)
            assert not future.done()
            lock_transaction.commit()
            ready = future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    assert ready.state == models.ImageLocationState.READY
    with image_database.connect() as connection:
        custody = connection.execute(
            sqlalchemy.select(schema.profile_custody).where(
                schema.profile_custody.c.workspace == 'research',
                schema.profile_custody.c.profile ==
                profile.name)).mappings().one()
    assert custody['physical_manifest_hash'] == profile.physical_manifest_hash

    changed = dataclasses.replace(
        profile,
        revision=profile.revision + 1,
        canonical=dataclasses.replace(
            profile.canonical,
            repository_prefix='skypilot/images/changed-canonical'))
    with pytest.raises(topology_state.CanonicalCustodyChangeError,
                       match='cannot change'):
        _stage_candidate_profile(changed, now=40)


def test_copy_claim_lease_ignores_fast_worker_wall_clock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    _, location = _publish_and_bind(profile)
    with image_database.connect() as connection:
        before = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())
    monkeypatch.setattr(topology_state.time, 'time', lambda: before + 10_000)

    claim = topology_state.claim_next_location(worker_id='copy-clock-worker',
                                               lease_seconds=60,
                                               workspace='research')

    assert claim is not None and claim.id == location.id
    assert before <= claim.updated_at < before + 1_000
    assert claim.lease_expires_at == claim.updated_at + 60


def test_canonical_completion_rechecks_database_clock_after_blocking_lock(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    publication_record, location = _publish_and_bind(profile)
    with image_database.connect() as connection:
        current = int(
            connection.execute(
                sqlalchemy.select(
                    catalog_state.database_epoch_expression())).scalar_one())
    claim = topology_state.claim_next_location(worker_id='copy-1',
                                               lease_seconds=1,
                                               workspace='research',
                                               now=current)
    assert claim is not None and claim.id == location.id
    assert claim.lease_token is not None
    assert topology_state.transition_location_to_verifying(claim.id,
                                                           claim.lease_token,
                                                           now=current)
    lock_key = json.dumps(['research', profile.name], separators=(',', ':'))
    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.text(
            'SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))'),
        {'key': f'skypilot:container-image-profile:{lock_key}'})
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(transactions.converge_canonical,
                                     location_id=claim.id,
                                     lease_token=claim.lease_token,
                                     ready=True)
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(1.2)')
            lock_transaction.commit()
            with pytest.raises(topology_state.LocationLeaseLostError):
                future.result(timeout=10)
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

    unchanged = topology_state.get_location(claim.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.VERIFYING
    assert unchanged.lease_token == claim.lease_token
    publication_after = catalog_state.get_publication(publication_record.id,
                                                      'research')
    assert publication_after is not None
    assert publication_after.state == models.ImagePublicationState.PENDING


def test_profile_activation_rechecks_existing_custody_marker(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    _, location = _publish_and_bind(profile)
    _complete_location(location, now=30)
    candidate_profile = _policy_profile(profile)
    candidate = _stage_candidate_profile(candidate_profile, now=40)
    with image_database.begin() as connection:
        connection.execute(schema.profile_revisions.update().where(
            schema.profile_revisions.c.id == candidate.id).values(
                physical_manifest_hash='f' * 64,
                terraform_hash='e' * 64,
                attestations_hash='d' * 64))

    with pytest.raises(topology_state.CanonicalCustodyChangeError,
                       match='cannot activate'):
        transactions.activate_profile(
            profile_revision_id=candidate.id,
            expected_generation=candidate.desired_generation,
            expected_config_hash=candidate.config_hash,
            expected_terraform_hash='e' * 64,
            expected_attestations_hash='d' * 64,
            required_attestations={},
            now=41)


def test_profile_activation_reads_constant_profile_rows_with_large_history(
        image_database, profile: models.ManagedRegistryProfile,
        monkeypatch: pytest.MonkeyPatch) -> None:
    original_activate = transactions.activate_profile
    profile_row_counts = []

    def activate_with_history(**kwargs):
        snapshot = json.dumps(profile.to_snapshot(),
                              sort_keys=True,
                              separators=(',', ':'))
        with image_database.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO container_image_profile_revisions (
                        id, workspace, profile, revision, desired_generation,
                        state, config_hash, config_json,
                        physical_manifest_hash, created_at, updated_at
                    )
                    SELECT 'activation-history-' || value, 'research',
                           :profile, value, value, 'SUPERSEDED',
                           :config_hash, :config_json,
                           :physical_manifest_hash, value, value
                    FROM generate_series(2, 20001) AS value
                """), {
                    'profile': profile.name,
                    'config_hash': profile.config_hash,
                    'config_json': snapshot,
                    'physical_manifest_hash': profile.physical_manifest_hash,
                })
            connection.execute(
                sqlalchemy.text('ANALYZE container_image_profile_revisions'))

        def record_rows(_connection, cursor, statement, _parameters, _context,
                        _executemany):
            if 'container_image_profile_revisions' in statement:
                profile_row_counts.append(cursor.rowcount)

        sqlalchemy.event.listen(image_database, 'after_cursor_execute',
                                record_rows)
        try:
            return original_activate(**kwargs)
        finally:
            sqlalchemy.event.remove(image_database, 'after_cursor_execute',
                                    record_rows)

    monkeypatch.setattr(transactions, 'activate_profile', activate_with_history)
    active = _activate_profile(image_database, profile)

    assert active.state == models.ImageProfileState.ACTIVE
    assert profile_row_counts
    assert max(profile_row_counts) <= 1


def test_profile_history_is_keyset_paginated_and_indexed_at_scale(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_profile_revisions (
                    id, workspace, profile, revision, desired_generation,
                    state, config_hash, config_json, physical_manifest_hash,
                    created_at, updated_at
                )
                SELECT md5('scale-profile-' || series::text), 'research',
                       'history-' || series::text, 1, series, 'SUPERSEDED',
                       md5('scale-profile-config-' || series::text), '{}',
                       md5('scale-profile-physical-' || series::text),
                       series, series
                FROM generate_series(1, 20000) AS series
            """))
        connection.execute(
            sqlalchemy.text('ANALYZE container_image_profile_revisions'))

    first = topology_state.list_profile_revision_history('research', limit=51)
    assert len(first) == 51
    assert first[0].created_at == 20_000
    after = (first[-1].created_at, first[-1].id)
    second = topology_state.list_profile_revision_history('research',
                                                          limit=51,
                                                          after=after)
    assert len(second) == 51
    assert all((record.created_at, record.id) < after for record in second)

    assert topology_state.list_active_profile_revisions(
        'research', (profile.name, 'not-configured')) == [active]
    with pytest.raises(ValueError, match='Active profile lookup is invalid'):
        topology_state.list_active_profile_revisions(
            'research', tuple(f'profile-{index}' for index in range(129)))

    with image_database.connect() as connection:
        plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_profile_revisions
                WHERE workspace = 'research'
                  AND (created_at, id) < (:created_at, :id)
                ORDER BY created_at DESC, id DESC
                LIMIT 51
            """), {
                'created_at': after[0],
                'id': after[1],
            }).scalars().all()
        active_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_profile_revisions
                WHERE workspace = 'research'
                  AND profile IN (:profile, 'not-configured')
                  AND state = 'ACTIVE'
                ORDER BY profile, id
                LIMIT 2
            """), {
                'profile': profile.name,
            }).scalars().all()
    assert 'ix_container_image_profile_history' in str(plan)
    assert 'uq_container_image_profile_active' in str(active_plan)


def test_worker_pages_and_cleanup_are_keyset_bounded_and_indexed_at_scale(
        image_database) -> None:
    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_workers (
                    id, kind, version, started_at, heartbeat_at,
                    in_flight, max_in_flight, grant_tokens_milli
                )
                SELECT md5('scale-worker-' || series::text), 'COPY', 'test',
                       series, series / 4, 0, 4, 0
                FROM generate_series(1, 20000) AS series
            """))
        connection.execute(sqlalchemy.text('ANALYZE container_image_workers'))

    first = topology_state.list_workers(limit=51)
    assert len(first) == 51
    first_keys = [(record.heartbeat_at, record.id) for record in first]
    assert first_keys == sorted(first_keys, reverse=True)
    after = first_keys[-1]
    second = topology_state.list_workers(limit=51, after=after)
    second_keys = [(record.heartbeat_at, record.id) for record in second]
    assert len(second) == 51
    assert second_keys == sorted(second_keys, reverse=True)
    assert all(key < after for key in second_keys)
    assert {record.id for record in first
           }.isdisjoint(record.id for record in second)

    with image_database.connect() as connection:
        list_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_workers
                WHERE (heartbeat_at, id) < (:heartbeat_at, :id)
                ORDER BY heartbeat_at DESC, id DESC
                LIMIT 51
            """), {
                'heartbeat_at': after[0],
                'id': after[1],
            }).scalars().all()
        cleanup_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
                SELECT id FROM container_image_workers
                WHERE heartbeat_at < 1000
                ORDER BY heartbeat_at, id
                LIMIT 500 FOR UPDATE SKIP LOCKED
            """)).scalars().all()
    assert 'ix_container_image_workers_heartbeat' in str(list_plan)
    assert 'ix_container_image_workers_heartbeat' in str(cleanup_plan)


def test_hot_claim_queues_use_exact_partial_indexes_at_scale(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    _configure_profile(monkeypatch, profile)
    _, pending_location = _publish_and_bind(profile)
    queued = publication.publish(source_ref=_OTHER_SOURCE,
                                 release='queued-source-inspection',
                                 distribution=profile.name,
                                 workspace='research',
                                 actor_hash='7' * 64,
                                 idempotency_key='queued-source-inspection-key')
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    target = profile.targets[0]
    future = 10_000_000_000

    with image_database.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_operations (
                    id, authority_id, scope, actor_hash, kind,
                    idempotency_key, request_hash, state, created_at,
                    updated_at, terminal_expires_at
                )
                SELECT md5('scale-operation-' || series::text), :authority,
                       'research', :actor, 'PUBLISH',
                       'scale-operation-key-' || series::text, :request_hash,
                       'SUCCEEDED', 1, 1, 2
                FROM generate_series(1, 20000) AS series
            """), {
                'authority': authority,
                'actor': '8' * 64,
                'request_hash': '9' * 64,
            })
        connection.execute(schema.operations.insert(), [
            {
                'id': 'scale-pending-canary',
                'authority_id': authority,
                'scope': 'research',
                'actor_hash': 'a' * 64,
                'kind': 'PROFILE_CANARY',
                'idempotency_key': 'scale-pending-canary-key',
                'request_hash': 'b' * 64,
                'state': models.ImageOperationState.PENDING.value,
                'created_at': 1,
                'updated_at': 1,
            },
            {
                'id': 'scale-expired-canary',
                'authority_id': authority,
                'scope': 'research',
                'actor_hash': 'c' * 64,
                'kind': 'PROFILE_CANARY',
                'idempotency_key': 'scale-expired-canary-key',
                'request_hash': 'd' * 64,
                'state': models.ImageOperationState.RUNNING.value,
                'lease_token': 'scale-worker:token',
                'lease_expires_at': 2,
                'teardown_deadline': 10,
                'created_at': 1,
                'updated_at': 3,
            },
        ])
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_publications (
                    id, workspace, operation_id, profile_revision_id,
                    requested_release, reservation_active, source_ref,
                    source_root_digest, requested_platform, state,
                    attempt_count, created_at, updated_at
                )
                SELECT md5('scale-publication-' || series::text), 'research',
                       md5('scale-operation-' || series::text), :revision,
                       'expired-release-' || series::text, false, :source,
                       :digest, 'linux/amd64', 'FAILED', 1, 1, 1
                FROM generate_series(1, 20000) AS series
            """), {
                'revision': active.id,
                'source': _SOURCE,
                'digest': _DIGEST,
            })
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_registry_shards (
                    id, workspace, profile, profile_revision_id, target_id,
                    provider, partition, account, region, shard_generation,
                    shard_index, target_fingerprint, physical_fingerprint,
                    eviction_enabled, registry, repository_name,
                    repository_arn, max_manifests, max_declared_bytes,
                    max_in_flight, state, qualified_at,
                    inventory_completed_at, inventory_next_at, created_at,
                    updated_at
                )
                SELECT md5('scale-shard-' || series::text), 'research',
                       :profile, :revision, :target, 'aws', :partition,
                       :account, :region, series + 1000, 0,
                       :target_fingerprint,
                       md5('scale-physical-' || series::text), false,
                       :registry, 'scale/repository/' || series::text,
                       'arn:aws:ecr:' || :region || ':' || :account ||
                           ':' || 'repository/scale/' || series::text,
                       100, 1000000, 4, 'READY', 1, 1, :future, 1, 1
                FROM generate_series(1, 20000) AS series
            """), {
                'profile': profile.name,
                'revision': active.id,
                'target': target.name,
                'partition': profile.partition,
                'account': profile.registry_account,
                'region': target.region,
                'target_fingerprint': target.target_fingerprint,
                'registry': target.registry,
                'future': future,
            })
        connection.execute(
            schema.registry_shards.update().values(inventory_next_at=future))
        connection.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.id == pending_location.shard_id).values(
                inventory_next_at=1))
        connection.execute(
            sqlalchemy.text('ANALYZE container_image_publications'))
        connection.execute(
            sqlalchemy.text('ANALYZE container_image_operations'))
        connection.execute(
            sqlalchemy.text('ANALYZE container_image_registry_shards'))
        canary_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_operations
                WHERE canary_claimable_at IS NOT NULL
                  AND canary_claimable_at <= :now
                ORDER BY canary_claimable_at, id
                LIMIT 16 FOR UPDATE SKIP LOCKED
            """), {
                'now': 100
            }).scalars().all()
        publication_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_publications
                WHERE canonical_location_id IS NULL
                  AND state IN ('PENDING', 'INSPECTING')
                  AND inspection_claimable_at <= :now
                ORDER BY inspection_claimable_at, id
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """), {
                'now': future
            }).scalars().all()
        copy_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_registry_shards
                WHERE copy_next_at IS NOT NULL AND copy_next_at <= :now
                ORDER BY copy_next_at, id
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """), {
                'now': 100
            }).scalars().all()
        inventory_plan = connection.execute(
            sqlalchemy.text("""
                EXPLAIN (COSTS OFF)
                SELECT id FROM container_image_registry_shards
                WHERE state IN ('PENDING', 'READY', 'FULL', 'DRIFTED')
                  AND inventory_next_at <= :now
                ORDER BY inventory_finalizing DESC, inventory_next_at, id
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """), {
                'now': 100
            }).scalars().all()

    assert queued.publication.canonical_location_id is None
    assert 'ix_container_image_operations_canary_queue' in str(canary_plan)
    assert ('ix_container_image_publications_inspection_queue'
            in str(publication_plan))
    assert ('ix_container_image_registry_shard_copy_queue' in str(copy_plan))
    assert ('ix_container_image_registry_shard_inventory'
            in str(inventory_plan))


def _migration_call(engine: sqlalchemy.engine.Engine, function: Any) -> None:
    with engine.begin() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            function()


def _autocommit_migration_call(engine: sqlalchemy.engine.Engine,
                               function: Any) -> None:
    """Runs a migration whose concurrent DDL needs Alembic transaction state."""
    with engine.connect() as connection:
        context = migration.MigrationContext.configure(connection)
        with context.begin_transaction():
            with operations.Operations.context(context):
                function()


def _schema_engine(base: sqlalchemy.engine.Engine,
                   schema_name: str) -> sqlalchemy.engine.Engine:
    # Keep the schema in the URL so migration_utils.get_alembic_config() also
    # preserves it when Alembic creates its own engine from this engine's URL.
    url = base.url.update_query_dict(
        {'options': f'-csearch_path={schema_name} -cstatement_timeout=15000'})
    return sqlalchemy.create_engine(url)


@pytest.mark.parametrize('index_ddl', [
    ('CREATE INDEX ix_spot_job_task ON spot (status)'),
    ('CREATE INDEX ix_spot_job_task ON spot '
     "(spot_job_id, task_id) WHERE status = 'SUCCEEDED'"),
    ('CREATE INDEX ix_spot_job_task ON spot '
     '(spot_job_id, (task_id + 0))'),
    ('CREATE INDEX ix_spot_job_task ON spot '
     '(spot_job_id, task_id) INCLUDE (status)'),
    ('CREATE INDEX ix_spot_job_task ON spot '
     '(spot_job_id DESC, task_id)'),
    ('CREATE UNIQUE INDEX ix_spot_job_task ON spot '
     '(spot_job_id, task_id)'),
    ('CREATE INDEX ix_spot_job_task ON spot '
     'USING hash (spot_job_id)'),
],
                         ids=('wrong-columns', 'partial', 'expression',
                              'included-column', 'descending', 'unique',
                              'wrong-method'))
def test_spot_jobs_revision_025_rejects_malformed_identity_index(
        postgres_engine, index_ddl: str) -> None:
    schema_name = f'spot_index_collision_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {schema_name}')
    engine = _schema_engine(postgres_engine, schema_name)
    try:
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SPOT_JOBS_DB_NAME,
                                             '024')
        with engine.begin() as connection:
            connection.exec_driver_sql('DROP INDEX IF EXISTS ix_spot_job_task')
            connection.exec_driver_sql(index_ddl)

        with pytest.raises(RuntimeError, match='unexpected shape'):
            migration_utils.safe_alembic_upgrade(
                engine, migration_utils.SPOT_JOBS_DB_NAME, '025')

        with engine.connect() as connection:
            revision = connection.execute(
                sqlalchemy.text('SELECT version_num FROM '
                                'alembic_version_spot_jobs_db')).scalar_one()
        assert revision == '024'
    finally:
        engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')


def test_spot_jobs_revision_025_repairs_invalid_residue_and_drives_plan(
        postgres_engine) -> None:
    schema_name = f'spot_index_repair_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {schema_name}')
    engine = _schema_engine(postgres_engine, schema_name)
    try:
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SPOT_JOBS_DB_NAME,
                                             '024')
        with engine.begin() as connection:
            connection.exec_driver_sql('DROP INDEX IF EXISTS ix_spot_job_task')
            connection.exec_driver_sql('CREATE INDEX ix_spot_job_task '
                                       'ON spot (spot_job_id, task_id)')
            connection.execute(
                sqlalchemy.text(
                    'UPDATE pg_index '
                    'SET indisvalid = FALSE, indisready = FALSE '
                    "WHERE indexrelid = 'ix_spot_job_task'::regclass"))

        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SPOT_JOBS_DB_NAME,
                                             '025')

        with engine.begin() as connection:
            shape = connection.execute(
                sqlalchemy.text("""
                    SELECT index_row.indisvalid,
                           index_row.indisready,
                           index_row.indpred IS NULL AS unfiltered,
                           index_row.indexprs IS NULL AS expression_free,
                           index_row.indnkeyatts,
                           index_row.indnatts,
                           access_method.amname
                    FROM pg_index AS index_row
                    JOIN pg_class AS index_class
                      ON index_class.oid = index_row.indexrelid
                    JOIN pg_am AS access_method
                      ON access_method.oid = index_class.relam
                    WHERE index_class.oid = 'ix_spot_job_task'::regclass
                    """)).one()
            assert tuple(shape) == (True, True, True, True, 2, 2, 'btree')
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO spot (spot_job_id, task_id, status)
                    SELECT job_id, task_id, 'SUCCEEDED'
                    FROM generate_series(1, 1000) AS job_id
                    CROSS JOIN generate_series(0, 99) AS task_id
                    """))
            connection.exec_driver_sql('ANALYZE spot')
            connection.exec_driver_sql('SET LOCAL enable_seqscan = off')
            identities = ','.join(
                f'({job_id}, {job_id % 100})' for job_id in range(1, 251))
            plan = connection.execute(
                sqlalchemy.text(
                    'EXPLAIN (FORMAT JSON, COSTS OFF) '
                    'SELECT spot_job_id, task_id, status FROM spot '
                    f'WHERE (spot_job_id, task_id) IN ({identities})')
            ).scalar_one()
            revision = connection.execute(
                sqlalchemy.text('SELECT version_num FROM '
                                'alembic_version_spot_jobs_db')).scalar_one()

        indexes = {
            index['name']: index
            for index in sqlalchemy.inspect(engine).get_indexes('spot')
        }
        assert indexes['ix_spot_job_task']['column_names'] == [
            'spot_job_id', 'task_id'
        ]
        assert 'ix_spot_job_task' in json.dumps(plan)
        assert revision == '025'
    finally:
        engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')


def _schema_shape(engine: sqlalchemy.engine.Engine,
                  schema_name: str) -> dict[str, list[tuple[Any, ...]]]:
    with engine.connect() as connection:
        columns = connection.execute(
            sqlalchemy.text("""SELECT table_name, column_name, ordinal_position,
                          data_type, udt_schema, udt_name, is_nullable,
                          column_default, is_generated, generation_expression,
                          is_identity, identity_generation, collation_name,
                          character_maximum_length, numeric_precision,
                          numeric_scale
                   FROM information_schema.columns
                   WHERE table_schema = :schema
                     AND table_name LIKE 'container_image%'
                   ORDER BY table_name, ordinal_position"""), {
                'schema': schema_name
            }).all()
        constraints = connection.execute(
            sqlalchemy.text("""SELECT relation.relname, constraint_row.conname,
                          constraint_row.contype, constraint_row.convalidated,
                          constraint_row.condeferrable,
                          constraint_row.condeferred,
                          pg_get_constraintdef(constraint_row.oid, true)
                   FROM pg_constraint AS constraint_row
                   JOIN pg_class AS relation
                     ON relation.oid = constraint_row.conrelid
                   JOIN pg_namespace AS namespace
                     ON namespace.oid = relation.relnamespace
                   WHERE namespace.nspname = :schema
                     AND relation.relname LIKE 'container_image%'
                   ORDER BY relation.relname, constraint_row.conname"""), {
                'schema': schema_name
            }).all()
        indexes = connection.execute(
            sqlalchemy.text("""SELECT table_relation.relname,
                          index_relation.relname, index_row.indisunique,
                          index_row.indisprimary, index_row.indisvalid,
                          index_row.indisready, index_row.indislive,
                          pg_get_indexdef(index_relation.oid, 0, false)
                   FROM pg_index AS index_row
                   JOIN pg_class AS table_relation
                     ON table_relation.oid = index_row.indrelid
                   JOIN pg_class AS index_relation
                     ON index_relation.oid = index_row.indexrelid
                   JOIN pg_namespace AS namespace
                     ON namespace.oid = table_relation.relnamespace
                   WHERE namespace.nspname = :schema
                     AND table_relation.relname LIKE 'container_image%'
                   ORDER BY table_relation.relname,
                            index_relation.relname"""), {
                'schema': schema_name
            }).all()

    def normalize(row: Any) -> tuple[Any, ...]:
        quoted_schema = f'"{schema_name}"'
        return tuple(
            value.replace(f'{quoted_schema}.', '<schema>.').
            replace(f'{schema_name}.', '<schema>.'
                   ) if isinstance(value, str) else value for value in row)

    return {
        'columns': [normalize(row) for row in columns],
        'constraints': [normalize(row) for row in constraints],
        'indexes': [normalize(row) for row in indexes],
    }


def test_migration_024_matches_runtime_metadata_and_downgrade_is_empty_only(
        postgres_engine) -> None:
    migration_schema = f'image_migration_{uuid.uuid4().hex}'
    runtime_schema = f'image_runtime_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
        connection.exec_driver_sql(f'CREATE SCHEMA {runtime_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    runtime_engine = _schema_engine(postgres_engine, runtime_schema)
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    try:
        with migration_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
            connection.exec_driver_sql(
                "INSERT INTO clusters (name) VALUES ('legacy-cluster')")
        _migration_call(migration_engine, migration_024.upgrade)
        schema.metadata.create_all(runtime_engine)
        with runtime_engine.begin() as connection:
            connection.exec_driver_sql(
                'DROP TABLE '
                'container_image_qualification_repository_quarantines')
            connection.exec_driver_sql(
                'DROP TABLE container_image_qualification_mutation')
            connection.exec_driver_sql(
                'DROP INDEX '
                'ix_container_image_operations_running_canary_revision')
            connection.exec_driver_sql('ALTER TABLE container_image_operations '
                                       'DROP COLUMN canary_child_evidence_json')
        assert _schema_shape(migration_engine,
                             migration_schema) == _schema_shape(
                                 runtime_engine, runtime_schema)
        cluster_columns = {
            column['name'] for column in sqlalchemy.inspect(
                migration_engine).get_columns('clusters')
        }
        assert cluster_columns == {
            'name', 'container_image_binding_known',
            'container_image_consumer_kind', 'container_image_consumer_owner'
        }
        with migration_engine.connect() as connection:
            legacy_binding = connection.execute(
                sqlalchemy.text('SELECT container_image_binding_known, '
                                'container_image_consumer_kind, '
                                'container_image_consumer_owner FROM clusters '
                                "WHERE name = 'legacy-cluster'")).one()
        assert tuple(legacy_binding) == (0, None, None)

        # A preview database may already contain the complete image schema
        # while being stamped at the former image revision 023. The canonical
        # predecessor now owns auth_sessions, so revision 024 must create that
        # missing predecessor table without replaying image DDL.
        with migration_engine.begin() as connection:
            connection.exec_driver_sql('DROP TABLE auth_sessions')
        _migration_call(migration_engine, migration_024.upgrade)
        auth_columns = {
            column['name'] for column in sqlalchemy.inspect(
                migration_engine).get_columns('auth_sessions')
        }
        assert auth_columns == {'code_challenge', 'token', 'created_at'}

        authority = str(uuid.uuid4())
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""INSERT INTO container_image_operations
                       (id, authority_id, scope, actor_hash, kind,
                        idempotency_key, request_hash, state, created_at,
                        updated_at)
                       VALUES (:id, :authority, 'research', :actor, 'TEST',
                               'migration-operation-key', :request_hash,
                               'PENDING', 1, 1)"""), {
                    'id': str(uuid.uuid4()),
                    'authority': authority,
                    'actor': '1' * 64,
                    'request_hash': '2' * 64,
                })
        with pytest.raises(RuntimeError, match='requires all operational'):
            _migration_call(migration_engine, migration_024.downgrade)
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('DELETE FROM container_image_operations'))
        _migration_call(migration_engine, migration_024.downgrade)
        assert set(sqlalchemy.inspect(migration_engine).get_table_names()) == {
            'auth_sessions', 'clusters'
        }
        assert {
            column['name'] for column in sqlalchemy.inspect(
                migration_engine).get_columns('clusters')
        } == {'name'}
    finally:
        migration_engine.dispose()
        runtime_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {runtime_schema} CASCADE')


def test_migration_025_adds_rollback_compatible_canary_child_evidence(
        postgres_engine) -> None:
    migration_schema = f'image_evidence_migration_{uuid.uuid4().hex}'
    runtime_schema = f'image_evidence_runtime_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
        connection.exec_driver_sql(f'CREATE SCHEMA {runtime_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    runtime_engine = _schema_engine(postgres_engine, runtime_schema)
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    migration_025 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '025_container_image_canary_child_evidence')
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    migration_027 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '027_container_image_qualification_delete_phases')
    try:
        with migration_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
        _migration_call(migration_engine, migration_024.upgrade)
        _migration_call(migration_engine, migration_025.upgrade)
        _autocommit_migration_call(migration_engine, migration_026.upgrade)
        _migration_call(migration_engine, migration_027.upgrade)
        schema.metadata.create_all(runtime_engine)

        assert _schema_shape(migration_engine,
                             migration_schema) == _schema_shape(
                                 runtime_engine, runtime_schema)
        evidence_columns = {
            column['name']: column for column in sqlalchemy.inspect(
                migration_engine).get_columns('container_image_operations')
        }
        assert evidence_columns['canary_child_evidence_json']['nullable']

        operation_id = str(uuid.uuid4())
        evidence = json.dumps({
            'backend': 'aws_vm',
            'instance_profile_arn': 'arn:aws:iam::123456789012:instance-profile/skypilot-v1',
        })
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""INSERT INTO container_image_operations
                       (id, authority_id, scope, actor_hash, kind,
                        idempotency_key, request_hash, state,
                        canary_child_evidence_json, created_at, updated_at)
                       VALUES (:id, :authority, 'research', :actor,
                               'PROFILE_CANARY', 'migration-evidence-key',
                               :request_hash, 'PENDING', :evidence, 1, 1)"""), {
                    'id': operation_id,
                    'authority': str(uuid.uuid4()),
                    'actor': '1' * 64,
                    'request_hash': '2' * 64,
                    'evidence': evidence,
                })
        _migration_call(migration_engine, migration_025.downgrade)
        with migration_engine.connect() as connection:
            retained = connection.execute(
                sqlalchemy.text(
                    'SELECT canary_child_evidence_json '
                    'FROM container_image_operations WHERE id = :id'), {
                        'id': operation_id
                    }).scalar_one()
        assert retained == evidence
    finally:
        migration_engine.dispose()
        runtime_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {runtime_schema} CASCADE')


def _prepare_image_schema_for_migration_026(
        engine: sqlalchemy.engine.Engine) -> None:
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    migration_025 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '025_container_image_canary_child_evidence')
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
    _migration_call(engine, migration_024.upgrade)
    _migration_call(engine, migration_025.upgrade)


def _insert_migration_profile_revision(engine: sqlalchemy.engine.Engine,
                                       revision_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO container_image_profile_revisions (
                    id, workspace, profile, revision, desired_generation,
                    state, config_hash, config_json, physical_manifest_hash,
                    created_at, updated_at
                ) VALUES (
                    :id, 'research', 'migration-profile', 1, 1, 'ACTIVE',
                    :config_hash, '{}', :manifest_hash, 1, 1
                )
            """), {
                'id': revision_id,
                'config_hash': '1' * 64,
                'manifest_hash': '2' * 64,
            })


def test_migration_026_adds_qualification_mutation_and_running_canary_fences(
        postgres_engine) -> None:
    migration_schema = f'image_canary_fence_migration_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    index_name = 'ix_container_image_operations_running_canary_revision'
    try:
        _prepare_image_schema_for_migration_026(migration_engine)
        _autocommit_migration_call(migration_engine, migration_026.upgrade)

        inspector = sqlalchemy.inspect(migration_engine)
        indexes = {
            index['name']: index
            for index in inspector.get_indexes('container_image_operations')
        }
        assert indexes[index_name]['column_names'] == ['result_id']
        index_predicate = str(
            indexes[index_name]['dialect_options']['postgresql_where'])
        normalized_predicate = ''.join(
            index_predicate.replace('::text',
                                    '').split()).replace('(',
                                                         '').replace(')', '')
        assert normalized_predicate == (
            "kind='PROFILE_CANARY'ANDstate='RUNNING'")
        mutation_columns = {
            column['name']: column for column in inspector.get_columns(
                'container_image_qualification_mutation')
        }
        assert list(mutation_columns) == [
            'id', 'owner_profile_revision_id', 'owner_target',
            'owner_target_fingerprint', 'repository_arn', 'runtime_digest',
            'lifecycle_proof_id', 'state', 'mutation_lease_token',
            'mutation_lease_expires_at', 'updated_at'
        ]
        assert {
            constraint['name']
            for constraint in inspector.get_check_constraints(
                'container_image_qualification_mutation')
        } == {
            'ck_container_image_qualification_mutation_identity',
            'ck_container_image_qualification_mutation_lease',
            'ck_container_image_qualification_mutation_singleton',
            'ck_container_image_qualification_mutation_state',
        }

        revision_id = str(uuid.uuid4())
        _insert_migration_profile_revision(migration_engine, revision_id)
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO container_image_qualification_mutation (
                        id, owner_profile_revision_id, owner_target,
                        owner_target_fingerprint, repository_arn,
                        runtime_digest, lifecycle_proof_id, state,
                        mutation_lease_token, mutation_lease_expires_at,
                        updated_at
                    ) VALUES (
                        'global', :revision, 'primary', 'target-fingerprint',
                        'arn:aws:ecr:us-east-1:123456789012:repository/q',
                        :digest, :proof, 'DELETING', :token, 120, 100
                    )
                """), {
                    'revision': revision_id,
                    'digest': _DIGEST,
                    'proof': str(uuid.uuid4()),
                    'token': str(uuid.uuid4()),
                })
            connection.execute(
                sqlalchemy.text("""
                    UPDATE container_image_qualification_mutation
                    SET state = 'RESTORING',
                        mutation_lease_token = NULL,
                        mutation_lease_expires_at = NULL,
                        updated_at = 121
                    WHERE id = 'global'
                """))
            mutation = connection.execute(
                sqlalchemy.text("""
                    SELECT id, owner_profile_revision_id, state,
                           mutation_lease_token, mutation_lease_expires_at,
                           updated_at
                    FROM container_image_qualification_mutation
                """)).one()
            assert tuple(mutation) == ('global', revision_id, 'RESTORING', None,
                                       None, 121)
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO container_image_operations
                        (id, authority_id, scope, actor_hash, kind,
                         idempotency_key, request_hash, state, result_kind,
                         result_id, lease_token, lease_expires_at,
                         teardown_deadline, created_at, updated_at)
                    SELECT 'migration-operation-' || value::text, :authority,
                           'research', :actor, 'PROFILE_CANARY',
                           'migration-canary-' || value::text, :request_hash,
                           CASE WHEN value = 1 THEN 'RUNNING' ELSE 'PENDING' END,
                           CASE WHEN value = 1
                                THEN NULL ELSE 'profile_revision' END,
                           CASE WHEN value = 1 THEN NULL ELSE :revision END,
                           CASE WHEN value = 1 THEN 'lease' ELSE NULL END,
                           CASE WHEN value = 1 THEN 1000 ELSE NULL END,
                           CASE WHEN value = 1 THEN 1000 ELSE NULL END,
                           value, value
                    FROM generate_series(1, 1000) AS value
                """), {
                    'authority': str(uuid.uuid4()),
                    'actor': '1' * 64,
                    'request_hash': '2' * 64,
                    'revision': revision_id,
                })
            connection.exec_driver_sql('ANALYZE container_image_operations')
            connection.exec_driver_sql('SET LOCAL enable_seqscan = off')
            plan = connection.execute(
                sqlalchemy.text("""
                    EXPLAIN (FORMAT JSON, COSTS OFF)
                    SELECT 1 FROM container_image_operations
                    WHERE kind = 'PROFILE_CANARY'
                      AND state = 'RUNNING'
                    LIMIT 1
                """)).scalar_one()
        assert index_name in json.dumps(plan)

        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(
                    'DELETE FROM container_image_qualification_mutation'))
        invalid_rows = (
            {
                'id': 'not-global',
                'state': 'RESTORING',
                'token': None,
                'expires': None,
                'updated': 1,
            },
            {
                'id': 'global',
                'state': 'DELETING',
                'token': None,
                'expires': 2,
                'updated': 1,
            },
            {
                'id': 'global',
                'state': 'RESTORING',
                'token': 'unexpected',
                'expires': 2,
                'updated': 1,
            },
            {
                'id': 'global',
                'state': 'DELETING',
                'token': 'lease',
                'expires': 1,
                'updated': 1,
            },
        )
        for row in invalid_rows:
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                with migration_engine.begin() as connection:
                    connection.execute(
                        sqlalchemy.text("""
                            INSERT INTO container_image_qualification_mutation (
                                id, owner_profile_revision_id, owner_target,
                                owner_target_fingerprint, repository_arn,
                                runtime_digest, lifecycle_proof_id, state,
                                mutation_lease_token,
                                mutation_lease_expires_at, updated_at
                            ) VALUES (
                                :id, :revision, 'primary',
                                'target-fingerprint',
                                'arn:aws:ecr:us-east-1:123456789012:repository/q',
                                :digest, :proof, :state, :token, :expires,
                                :updated
                            )
                        """), {
                            **row,
                            'revision': revision_id,
                            'digest': _DIGEST,
                            'proof': str(uuid.uuid4()),
                        })
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def _prepare_image_schema_for_migration_027(
        engine: sqlalchemy.engine.Engine) -> None:
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    _prepare_image_schema_for_migration_026(engine)
    _autocommit_migration_call(engine, migration_026.upgrade)


def test_migration_027_quarantines_legacy_deleting_with_exact_tombstone(
        postgres_engine) -> None:
    migration_schema = f'image_delete_phase_quarantine_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_027 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '027_container_image_qualification_delete_phases')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        revision_id = str(uuid.uuid4())
        lifecycle_proof_id = str(uuid.uuid4())
        repository_arn = (
            'arn:aws:ecr:us-east-1:123456789012:repository/exact-q')
        _insert_migration_profile_revision(migration_engine, revision_id)
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO container_image_qualification_mutation (
                        id, owner_profile_revision_id, owner_target,
                        owner_target_fingerprint, repository_arn,
                        runtime_digest, lifecycle_proof_id, state,
                        mutation_lease_token, mutation_lease_expires_at,
                        updated_at
                    ) VALUES (
                        'global', :revision, 'primary', 'target-fingerprint',
                        :repository_arn,
                        :digest, :proof, 'DELETING', :token, 120, 100
                    )
                """), {
                    'revision': revision_id,
                    'repository_arn': repository_arn,
                    'digest': _DIGEST,
                    'proof': lifecycle_proof_id,
                    'token': str(uuid.uuid4()),
                })

        _migration_call(migration_engine, migration_027.upgrade)

        with migration_engine.connect() as connection:
            mutation = connection.execute(
                sqlalchemy.text("""
                    SELECT state, delete_phase, mutation_lease_token,
                           mutation_lease_expires_at, quarantine_reason,
                           repository_arn, lifecycle_proof_id, updated_at
                    FROM container_image_qualification_mutation
                    WHERE id = 'global'
                """)).mappings().one()
            tombstone = connection.execute(
                sqlalchemy.text("""
                    SELECT owner_profile_revision_id, owner_target,
                           owner_target_fingerprint, repository_arn,
                           runtime_digest, lifecycle_proof_id,
                           quarantine_reason, quarantined_at
                    FROM
                        container_image_qualification_repository_quarantines
                    WHERE repository_arn = :repository_arn
                """), {
                    'repository_arn': repository_arn
                }).mappings().one()
        assert mutation['state'] == 'QUARANTINED'
        assert mutation['delete_phase'] is None
        assert mutation['mutation_lease_token'] is None
        assert mutation['mutation_lease_expires_at'] is None
        assert (
            mutation['quarantine_reason'] == 'LEGACY_DELETE_OUTCOME_UNKNOWN')
        assert mutation['repository_arn'] == repository_arn
        assert mutation['lifecycle_proof_id'] == lifecycle_proof_id
        assert tombstone['owner_profile_revision_id'] == revision_id
        assert tombstone['owner_target'] == 'primary'
        assert tombstone['owner_target_fingerprint'] == 'target-fingerprint'
        assert tombstone['repository_arn'] == repository_arn
        assert tombstone['runtime_digest'] == _DIGEST
        assert tombstone['lifecycle_proof_id'] == lifecycle_proof_id
        assert tombstone['quarantine_reason'] == mutation['quarantine_reason']
        assert tombstone['quarantined_at'] == mutation['updated_at']
        assert tombstone['quarantined_at'] > 100
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def test_migration_027_quarantines_legacy_restoring_with_exact_tombstone(
        postgres_engine) -> None:
    migration_schema = f'image_restoration_quarantine_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_027 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '027_container_image_qualification_delete_phases')
    revision_id = str(uuid.uuid4())
    lifecycle_proof_id = str(uuid.uuid4())
    repository_arn = (
        'arn:aws:ecr:us-east-1:123456789012:repository/exact-restoring-q')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        _insert_migration_profile_revision(migration_engine, revision_id)
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO container_image_qualification_mutation (
                        id, owner_profile_revision_id, owner_target,
                        owner_target_fingerprint, repository_arn,
                        runtime_digest, lifecycle_proof_id, state,
                        mutation_lease_token, mutation_lease_expires_at,
                        updated_at
                    ) VALUES (
                        'global', :revision, 'primary', 'target-fingerprint',
                        :repository_arn, :digest, :proof, 'RESTORING',
                        NULL, NULL, 100
                    )
                """), {
                    'revision': revision_id,
                    'repository_arn': repository_arn,
                    'digest': _DIGEST,
                    'proof': lifecycle_proof_id,
                })
        _migration_call(migration_engine, migration_027.upgrade)

        with migration_engine.connect() as connection:
            mutation = connection.execute(
                sqlalchemy.text("""
                    SELECT state, delete_phase, mutation_lease_token,
                           mutation_lease_expires_at, quarantine_reason,
                           repository_arn, lifecycle_proof_id, updated_at
                    FROM container_image_qualification_mutation
                    WHERE id = 'global'
                """)).mappings().one()
            tombstone = connection.execute(
                sqlalchemy.text("""
                    SELECT owner_profile_revision_id, owner_target,
                           owner_target_fingerprint, repository_arn,
                           runtime_digest, lifecycle_proof_id,
                           quarantine_reason, quarantined_at
                    FROM
                        container_image_qualification_repository_quarantines
                    WHERE repository_arn = :repository_arn
                """), {
                    'repository_arn': repository_arn
                }).mappings().one()

        assert mutation['state'] == 'QUARANTINED'
        assert mutation['delete_phase'] is None
        assert mutation['mutation_lease_token'] is None
        assert mutation['mutation_lease_expires_at'] is None
        assert (mutation['quarantine_reason'] ==
                'LEGACY_RESTORATION_EVIDENCE_INCOMPLETE')
        assert (mutation['quarantine_reason']
                != 'LEGACY_DELETE_OUTCOME_UNKNOWN')
        assert mutation['repository_arn'] == repository_arn
        assert mutation['lifecycle_proof_id'] == lifecycle_proof_id
        assert tombstone['owner_profile_revision_id'] == revision_id
        assert tombstone['owner_target'] == 'primary'
        assert tombstone['owner_target_fingerprint'] == 'target-fingerprint'
        assert tombstone['repository_arn'] == repository_arn
        assert tombstone['runtime_digest'] == _DIGEST
        assert tombstone['lifecycle_proof_id'] == lifecycle_proof_id
        assert tombstone['quarantine_reason'] == mutation['quarantine_reason']
        assert tombstone['quarantined_at'] == mutation['updated_at']
        assert tombstone['quarantined_at'] > 100
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def test_migration_027_upgrades_empty_026_to_runtime_shape(
        postgres_engine) -> None:
    migration_schema = f'image_delete_phase_migration_{uuid.uuid4().hex}'
    runtime_schema = f'image_delete_phase_runtime_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
        connection.exec_driver_sql(f'CREATE SCHEMA {runtime_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    runtime_engine = _schema_engine(postgres_engine, runtime_schema)
    migration_027 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '027_container_image_qualification_delete_phases')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        _migration_call(migration_engine, migration_027.upgrade)
        schema.metadata.create_all(runtime_engine)

        assert _schema_shape(migration_engine,
                             migration_schema) == _schema_shape(
                                 runtime_engine, runtime_schema)
        inspector = sqlalchemy.inspect(migration_engine)
        assert [
            column['name'] for column in inspector.get_columns(
                'container_image_qualification_mutation')
        ] == [
            'id', 'owner_profile_revision_id', 'owner_target',
            'owner_target_fingerprint', 'repository_arn', 'runtime_digest',
            'lifecycle_proof_id', 'state', 'mutation_lease_token',
            'mutation_lease_expires_at', 'updated_at', 'delete_phase',
            'quarantine_reason'
        ]
        assert {
            constraint['name']
            for constraint in inspector.get_check_constraints(
                'container_image_qualification_mutation')
        } == {
            'ck_container_image_qualification_mutation_delete_phase',
            'ck_container_image_qualification_mutation_identity',
            'ck_container_image_qualification_mutation_lease',
            'ck_container_image_qualification_mutation_singleton',
            'ck_container_image_qualification_mutation_state',
        }
        assert [
            column['name'] for column in inspector.get_columns(
                'container_image_qualification_repository_quarantines')
        ] == [
            'repository_arn', 'owner_profile_revision_id', 'owner_target',
            'owner_target_fingerprint', 'runtime_digest', 'lifecycle_proof_id',
            'quarantine_reason', 'quarantined_at'
        ]
        assert {
            constraint['name']
            for constraint in inspector.get_check_constraints(
                'container_image_qualification_repository_quarantines')
        } == {
            'ck_container_image_qualification_repository_quarantine_identity'
        }
        assert {
            index['name'] for index in inspector.get_indexes(
                'container_image_qualification_repository_quarantines')
        } == {
            'ix_container_image_qualification_repository_quarantines_history'
        }
    finally:
        migration_engine.dispose()
        runtime_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {runtime_schema} CASCADE')


def test_migration_027_empty_downgrade_restores_exact_026_shape(
        postgres_engine) -> None:
    migration_schema = f'image_delete_phase_downgrade_{uuid.uuid4().hex}'
    reference_schema = f'image_delete_phase_reference_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
        connection.exec_driver_sql(f'CREATE SCHEMA {reference_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    reference_engine = _schema_engine(postgres_engine, reference_schema)
    migration_027 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '027_container_image_qualification_delete_phases')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        _migration_call(migration_engine, migration_027.upgrade)
        _prepare_image_schema_for_migration_027(reference_engine)

        _migration_call(migration_engine, migration_027.downgrade)

        assert _schema_shape(migration_engine,
                             migration_schema) == _schema_shape(
                                 reference_engine, reference_schema)
        inspector = sqlalchemy.inspect(migration_engine)
        assert not inspector.has_table(
            'container_image_qualification_repository_quarantines')
        assert [
            column['name'] for column in inspector.get_columns(
                'container_image_qualification_mutation')
        ] == [
            'id', 'owner_profile_revision_id', 'owner_target',
            'owner_target_fingerprint', 'repository_arn', 'runtime_digest',
            'lifecycle_proof_id', 'state', 'mutation_lease_token',
            'mutation_lease_expires_at', 'updated_at'
        ]
    finally:
        migration_engine.dispose()
        reference_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {reference_schema} CASCADE')


@pytest.mark.parametrize('residue', ['mutation', 'tombstone'])
def test_migration_027_downgrade_refuses_durable_residue(
        postgres_engine, residue: str) -> None:
    migration_schema = f'image_delete_phase_residue_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_027 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '027_container_image_qualification_delete_phases')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        _migration_call(migration_engine, migration_027.upgrade)
        revision_id = str(uuid.uuid4())
        _insert_migration_profile_revision(migration_engine, revision_id)
        with migration_engine.begin() as connection:
            if residue == 'mutation':
                connection.execute(
                    sqlalchemy.text("""
                        INSERT INTO container_image_qualification_mutation (
                            id, owner_profile_revision_id, owner_target,
                            owner_target_fingerprint, repository_arn,
                            runtime_digest, lifecycle_proof_id, state,
                            delete_phase, mutation_lease_token,
                            mutation_lease_expires_at, quarantine_reason,
                            updated_at
                        ) VALUES (
                            'global', :revision, 'primary',
                            'target-fingerprint', :repository_arn, :digest,
                            :proof, 'RESTORING', NULL, NULL, NULL, NULL, 100
                        )
                    """), {
                        'revision': revision_id,
                        'repository_arn': ('arn:aws:ecr:us-east-1:123456789012:'
                                           'repository/q'),
                        'digest': _DIGEST,
                        'proof': str(uuid.uuid4()),
                    })
            else:
                connection.execute(
                    sqlalchemy.text("""
                        INSERT INTO
                            container_image_qualification_repository_quarantines
                            (repository_arn, owner_profile_revision_id,
                             owner_target, owner_target_fingerprint,
                             runtime_digest, lifecycle_proof_id,
                             quarantine_reason, quarantined_at)
                        VALUES (
                            :repository_arn, :revision, 'primary',
                            'target-fingerprint', :digest, :proof,
                            'PROVIDER_OUTCOME_AMBIGUOUS', 100
                        )
                    """), {
                        'revision': revision_id,
                        'repository_arn': ('arn:aws:ecr:us-east-1:123456789012:'
                                           'repository/q'),
                        'digest': _DIGEST,
                        'proof': str(uuid.uuid4()),
                    })

        with pytest.raises(RuntimeError,
                           match='requires empty qualification mutation'):
            _migration_call(migration_engine, migration_027.downgrade)

        inspector = sqlalchemy.inspect(migration_engine)
        assert inspector.has_table(
            'container_image_qualification_repository_quarantines')
        assert {'delete_phase', 'quarantine_reason'}.issubset({
            column['name'] for column in inspector.get_columns(
                'container_image_qualification_mutation')
        })
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def test_migration_026_empty_downgrade_restores_exact_025_shape(
        postgres_engine) -> None:
    migration_schema = f'image_canary_fence_downgrade_{uuid.uuid4().hex}'
    reference_schema = f'image_canary_fence_reference_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
        connection.exec_driver_sql(f'CREATE SCHEMA {reference_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    reference_engine = _schema_engine(postgres_engine, reference_schema)
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        _prepare_image_schema_for_migration_026(reference_engine)

        _migration_call(migration_engine, migration_026.downgrade)

        assert _schema_shape(migration_engine,
                             migration_schema) == _schema_shape(
                                 reference_engine, reference_schema)
        inspector = sqlalchemy.inspect(migration_engine)
        assert not inspector.has_table('container_image_qualification_mutation')
        assert 'ix_container_image_operations_running_canary_revision' not in {
            index['name']
            for index in inspector.get_indexes('container_image_operations')
        }
    finally:
        migration_engine.dispose()
        reference_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {reference_schema} CASCADE')


def test_migration_026_downgrade_refuses_nonempty_mutation(
        postgres_engine) -> None:
    migration_schema = f'image_canary_fence_residue_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        revision_id = str(uuid.uuid4())
        _insert_migration_profile_revision(migration_engine, revision_id)
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO container_image_qualification_mutation (
                        id, owner_profile_revision_id, owner_target,
                        owner_target_fingerprint, repository_arn,
                        runtime_digest, lifecycle_proof_id, state,
                        mutation_lease_token, mutation_lease_expires_at,
                        updated_at
                    ) VALUES (
                        'global', :revision, 'primary', 'target-fingerprint',
                        'arn:aws:ecr:us-east-1:123456789012:repository/q',
                        :digest, :proof, 'DELETING', :token, 120, 100
                    )
                """), {
                    'revision': revision_id,
                    'digest': _DIGEST,
                    'proof': str(uuid.uuid4()),
                    'token': str(uuid.uuid4()),
                })

        with pytest.raises(RuntimeError, match='requires an empty'):
            _migration_call(migration_engine, migration_026.downgrade)

        inspector = sqlalchemy.inspect(migration_engine)
        assert inspector.has_table('container_image_qualification_mutation')
        assert 'ix_container_image_operations_running_canary_revision' in {
            index['name']
            for index in inspector.get_indexes('container_image_operations')
        }
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def test_migration_027_empty_database_downgrades_through_023(
        postgres_engine) -> None:
    migration_schema = f'image_full_downgrade_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    migration_025 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '025_container_image_canary_child_evidence')
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    migration_027 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '027_container_image_qualification_delete_phases')
    try:
        _prepare_image_schema_for_migration_027(migration_engine)
        _migration_call(migration_engine, migration_027.upgrade)

        _migration_call(migration_engine, migration_027.downgrade)
        _migration_call(migration_engine, migration_026.downgrade)
        _migration_call(migration_engine, migration_025.downgrade)
        _migration_call(migration_engine, migration_024.downgrade)

        inspector = sqlalchemy.inspect(migration_engine)
        assert set(inspector.get_table_names()) == {'auth_sessions', 'clusters'}
        assert {column['name'] for column in inspector.get_columns('clusters')
               } == {'name'}
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


@pytest.mark.parametrize('index_ddl', [
    ('CREATE INDEX '
     'ix_container_image_operations_running_canary_revision '
     'ON container_image_operations (state)'),
    ('CREATE INDEX '
     'ix_container_image_operations_running_canary_revision '
     'ON container_image_operations (result_id) '
     "WHERE kind = 'PROFILE_CANARY' AND state = 'RUNNING' "
     "AND result_kind = 'profile_revision'"),
    ('CREATE UNIQUE INDEX '
     'ix_container_image_operations_running_canary_revision '
     'ON container_image_operations (result_id) '
     "WHERE kind = 'PROFILE_CANARY' AND state = 'RUNNING'"),
],
                         ids=('wrong-column', 'wrong-predicate', 'unique'))
def test_migration_026_rejects_malformed_running_canary_index(
        postgres_engine, index_ddl: str) -> None:
    migration_schema = f'image_canary_collision_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    try:
        _prepare_image_schema_for_migration_026(migration_engine)
        with migration_engine.begin() as connection:
            connection.exec_driver_sql(index_ddl)
        with pytest.raises(RuntimeError, match='unexpected shape'):
            _autocommit_migration_call(migration_engine, migration_026.upgrade)
        assert not sqlalchemy.inspect(migration_engine).has_table(
            'container_image_qualification_mutation')
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def test_migration_026_accepts_exact_existing_running_canary_index(
        postgres_engine) -> None:
    migration_schema = f'image_canary_existing_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    index_name = 'ix_container_image_operations_running_canary_revision'
    try:
        _prepare_image_schema_for_migration_026(migration_engine)
        with migration_engine.begin() as connection:
            connection.exec_driver_sql(
                f'CREATE INDEX {index_name} '
                'ON container_image_operations (result_id) '
                "WHERE kind = 'PROFILE_CANARY' AND state = 'RUNNING'")
        _autocommit_migration_call(migration_engine, migration_026.upgrade)
        assert sqlalchemy.inspect(migration_engine).has_table(
            'container_image_qualification_mutation')
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def test_migration_026_repairs_invalid_running_canary_index_residue(
        postgres_engine) -> None:
    migration_schema = f'image_canary_repair_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    migration_026 = importlib.import_module(
        'sky.schemas.db.global_user_state.'
        '026_container_image_running_canary_index')
    index_name = 'ix_container_image_operations_running_canary_revision'
    try:
        _prepare_image_schema_for_migration_026(migration_engine)
        with migration_engine.begin() as connection:
            connection.exec_driver_sql(
                f'CREATE INDEX {index_name} '
                'ON container_image_operations (result_id) '
                "WHERE kind = 'PROFILE_CANARY' AND state = 'RUNNING'")
            connection.execute(
                sqlalchemy.text('UPDATE pg_index SET indisvalid = FALSE, '
                                'indisready = FALSE '
                                f"WHERE indexrelid = '{index_name}'::regclass"))
        _autocommit_migration_call(migration_engine, migration_026.upgrade)
        with migration_engine.connect() as connection:
            state = connection.execute(
                sqlalchemy.text("""
                    SELECT indisvalid, indisready
                    FROM pg_index
                    WHERE indexrelid =
                        'ix_container_image_operations_running_canary_revision'
                        ::regclass
                """)).one()
        assert tuple(state) == (True, True)
        assert sqlalchemy.inspect(migration_engine).has_table(
            'container_image_qualification_mutation')
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')


def test_safe_alembic_upgrade_serializes_api_processes_with_postgres_lock(
        postgres_engine) -> None:
    race_schema = f'image_migration_race_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {race_schema}')
    race_engine = _schema_engine(postgres_engine, race_schema)
    processes: list[subprocess.Popen[str]] = []
    try:
        with race_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
            connection.exec_driver_sql('CREATE TABLE alembic_version_state_db '
                                       '(version_num VARCHAR(32) PRIMARY KEY)')
            connection.exec_driver_sql(
                "INSERT INTO alembic_version_state_db VALUES ('023')")

        url = postgres_engine.url.update_query_dict(
            {'options': f'-csearch_path={race_schema}'})
        script = """
import contextlib
import os
import sqlalchemy
from sky.utils.db import migration_utils

@contextlib.contextmanager
def unlocked(_section):
    yield

migration_utils.db_lock = unlocked
engine = sqlalchemy.create_engine(os.environ['SKYPILOT_TEST_DATABASE_URL'])
try:
    migration_utils.safe_alembic_upgrade(
        engine, migration_utils.GLOBAL_USER_STATE_DB_NAME,
        migration_utils.GLOBAL_USER_STATE_VERSION)
finally:
    engine.dispose()
"""
        environment = dict(os.environ)
        repository_root = str(Path(__file__).resolve().parents[3])
        environment['PYTHONPATH'] = os.pathsep.join(
            filter(None, (repository_root, environment.get('PYTHONPATH', ''))))
        environment['SKYPILOT_TEST_DATABASE_URL'] = url.render_as_string(
            hide_password=False)

        lock_connection = postgres_engine.connect()
        lock_connection.execute(
            sqlalchemy.text(
                "SELECT pg_advisory_lock(hashtext('skypilot:alembic:state_db'))"
            ))
        processes = [
            subprocess.Popen([sys.executable, '-c', script],
                             cwd=repository_root,
                             env=environment,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             text=True) for _ in range(2)
        ]
        try:
            time.sleep(1)
            assert all(process.poll() is None for process in processes)
        finally:
            lock_connection.execute(
                sqlalchemy.text("SELECT pg_advisory_unlock(hashtext("
                                "'skypilot:alembic:state_db'))"))
            lock_connection.close()

        results = [process.communicate(timeout=90) for process in processes]
        assert [process.returncode for process in processes] == [0, 0], results
        with race_engine.connect() as connection:
            revision = connection.execute(
                sqlalchemy.text(
                    'SELECT version_num FROM alembic_version_state_db')
            ).scalar_one()
        assert revision == migration_utils.GLOBAL_USER_STATE_VERSION
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
        race_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {race_schema} CASCADE')


@pytest.mark.parametrize('revision', ['022', None])
def test_migration_job_rejects_nonempty_predecessor_below_023_without_ddl(
        postgres_engine, revision: str | None) -> None:
    unsafe_schema = f'image_migration_unsafe_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {unsafe_schema}')
    unsafe_engine = _schema_engine(postgres_engine, unsafe_schema)
    try:
        with unsafe_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
            if revision is not None:
                connection.exec_driver_sql(
                    'CREATE TABLE alembic_version_state_db '
                    '(version_num VARCHAR(32) PRIMARY KEY)')
                connection.execute(
                    sqlalchemy.text(
                        'INSERT INTO alembic_version_state_db VALUES (:revision)'
                    ), {'revision': revision})

        with pytest.raises(RuntimeError,
                           match='staged upgrade through revision'):
            migration_utils.safe_alembic_upgrade(
                unsafe_engine,
                migration_utils.GLOBAL_USER_STATE_DB_NAME,
                migration_utils.GLOBAL_USER_STATE_VERSION,
                mode='upgrade')

        inspector = sqlalchemy.inspect(unsafe_engine)
        assert not inspector.has_table('auth_sessions')
        assert not inspector.has_table('container_image_catalog')
        assert {column['name'] for column in inspector.get_columns('clusters')
               } == {'name'}
        if revision is not None:
            with unsafe_engine.connect() as connection:
                assert connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM alembic_version_state_db')
                ).scalar_one() == revision
    finally:
        unsafe_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {unsafe_schema} CASCADE')


@pytest.mark.parametrize('ddl', [
    'CREATE VIEW orphan_view AS SELECT 1 AS value',
    'CREATE MATERIALIZED VIEW orphan_materialized_view AS SELECT 1 AS value',
    'CREATE SEQUENCE orphan_sequence',
    "CREATE TYPE orphan_status AS ENUM ('ready')",
    ('CREATE FUNCTION orphan_function() RETURNS integer '
     'LANGUAGE SQL AS $$ SELECT 1 $$'),
])
@pytest.mark.parametrize('mode', ['upgrade', 'bootstrap'])
def test_migration_job_rejects_every_schema_owned_object_before_ddl(
        postgres_engine, ddl: str, mode: migration_utils.MigrationMode) -> None:
    unsafe_schema = f'image_migration_object_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {unsafe_schema}')
    unsafe_engine = _schema_engine(postgres_engine, unsafe_schema)
    try:
        with unsafe_engine.begin() as connection:
            connection.exec_driver_sql(ddl)

        with pytest.raises(RuntimeError,
                           match='staged upgrade through revision'):
            migration_utils.safe_alembic_upgrade(
                unsafe_engine,
                migration_utils.GLOBAL_USER_STATE_DB_NAME,
                migration_utils.GLOBAL_USER_STATE_VERSION,
                mode=mode)

        inspector = sqlalchemy.inspect(unsafe_engine)
        assert not inspector.has_table('alembic_version_state_db')
        assert not inspector.has_table('container_image_catalog')
        assert not inspector.has_table('auth_sessions')
    finally:
        unsafe_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {unsafe_schema} CASCADE')


@pytest.mark.parametrize('mode', ['auto', 'upgrade'])
def test_regular_migration_mode_rejects_unversioned_empty_schema(
        postgres_engine, mode: migration_utils.MigrationMode) -> None:
    fresh_schema = f'image_migration_regular_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {fresh_schema}')
    fresh_url = postgres_engine.url.update_query_dict(
        {'options': f'-csearch_path={fresh_schema}'})
    fresh_engine = sqlalchemy.create_engine(fresh_url)
    try:
        with pytest.raises(RuntimeError, match='explicitly use bootstrap mode'):
            migration_utils.safe_alembic_upgrade(
                fresh_engine,
                migration_utils.GLOBAL_USER_STATE_DB_NAME,
                migration_utils.GLOBAL_USER_STATE_VERSION,
                mode=mode)

        assert not sqlalchemy.inspect(fresh_engine).has_table(
            'alembic_version_state_db')
        assert not sqlalchemy.inspect(fresh_engine).has_table(
            'container_image_catalog')
    finally:
        fresh_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {fresh_schema} CASCADE')


def test_bootstrap_mode_allows_genuinely_empty_isolated_schema(
        postgres_engine) -> None:
    fresh_schema = f'image_migration_fresh_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {fresh_schema}')
    fresh_url = postgres_engine.url.update_query_dict(
        {'options': f'-csearch_path={fresh_schema}'})
    fresh_engine = sqlalchemy.create_engine(fresh_url)
    try:
        migration_utils.safe_alembic_upgrade(
            fresh_engine,
            migration_utils.GLOBAL_USER_STATE_DB_NAME,
            migration_utils.GLOBAL_USER_STATE_VERSION,
            mode='bootstrap')

        with fresh_engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.text(
                    'SELECT version_num FROM alembic_version_state_db')
            ).scalar_one() == migration_utils.GLOBAL_USER_STATE_VERSION
        assert sqlalchemy.inspect(fresh_engine).has_table(
            'container_image_catalog')
    finally:
        fresh_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {fresh_schema} CASCADE')


def test_database_migration_process_bootstraps_before_config_overlay(
        postgres_engine, tmp_path: Path) -> None:
    """Proves import-time config loading cannot preempt fresh DB bootstrap."""
    fresh_schema = f'migration_process_fresh_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {fresh_schema}')
    fresh_url = postgres_engine.url.update_query_dict(
        {'options': f'-csearch_path={fresh_schema}'})
    empty_config = tmp_path / 'config.yaml'
    empty_config.write_text('', encoding='utf-8')
    environment = os.environ.copy()
    environment.update({
        constants.ENV_VAR_IS_SKYPILOT_SERVER: 'true',
        constants.ENV_VAR_DB_CONNECTION_URI:
            fresh_url.render_as_string(hide_password=False),
        constants.ENV_VAR_STATE_DB_MIGRATION_MODE: 'bootstrap',
        'SKYPILOT_API_REQUEST_BACKEND': 'postgres',
        'SKYPILOT_GLOBAL_CONFIG': str(empty_config),
    })
    repository_root = Path(__file__).resolve().parents[2]
    existing_pythonpath = environment.get('PYTHONPATH')
    environment['PYTHONPATH'] = (
        str(repository_root) if not existing_pythonpath else
        f'{repository_root}{os.pathsep}{existing_pythonpath}')
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'sky.server.database_migrations'],
            cwd=repository_root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=120)
        assert result.returncode == 0, (f'migration stderr:\n{result.stderr}\n'
                                        f'migration stdout:\n{result.stdout}')

        with sqlalchemy.create_engine(fresh_url).connect() as connection:
            revisions = {
                table: connection.execute(
                    sqlalchemy.text(f'SELECT version_num FROM {table}')
                ).scalar_one() for table in (
                    'alembic_version_state_db',
                    'alembic_version_sky_config_db',
                    'alembic_version_serve_state_db',
                    'alembic_version_spot_jobs_db',
                    'alembic_version_api_requests_db',
                    'alembic_version_lifecycle_actions_db',
                    'alembic_version_capacity_state_db',
                )
            }
            store_row = connection.execute(
                sqlalchemy.text("""
                    SELECT store_key, store_uuid, schema_version,
                           writer_authority_digest, created_at,
                           isfinite(created_at) AS created_at_is_finite
                    FROM lifecycle_store_identity
                """)).one()
            scope_row = connection.execute(
                sqlalchemy.text("""
                    SELECT domain, operation_subset, store_mode, routing_mode,
                           minimum_lifecycle_version, ownership_epoch,
                           authority_generation,
                           writer_implementation_digest,
                           reconciler_implementation_digest, updated_at,
                           isfinite(updated_at) AS updated_at_is_finite
                    FROM lifecycle_ownership_scopes
                """)).one()
        assert revisions == {
            'alembic_version_state_db':
                migration_utils.GLOBAL_USER_STATE_VERSION,
            'alembic_version_sky_config_db':
                migration_utils.SKYPILOT_CONFIG_VERSION,
            'alembic_version_serve_state_db': migration_utils.SERVE_VERSION,
            'alembic_version_spot_jobs_db': migration_utils.SPOT_JOBS_VERSION,
            'alembic_version_api_requests_db':
                migration_utils.API_REQUESTS_VERSION,
            'alembic_version_lifecycle_actions_db':
                migration_utils.LIFECYCLE_ACTIONS_VERSION,
            'alembic_version_capacity_state_db':
                migration_utils.CAPACITY_STATE_VERSION,
        }
        store_uuid = uuid.UUID(str(store_row.store_uuid))
        assert store_row.store_key == 'global'
        assert store_uuid.version == 4
        assert store_uuid.variant == uuid.RFC_4122
        assert store_row.schema_version == 1
        assert store_row.writer_authority_digest is None
        assert store_row.created_at.tzinfo is not None
        assert store_row.created_at.utcoffset() is not None
        assert store_row.created_at_is_finite is True
        assert tuple(scope_row[:9]) == (
            'VOLUME',
            'KUBERNETES_PVC_OWNED_LIFECYCLE_V1',
            'CENTRAL_POSTGRESQL',
            'DARK',
            0,
            1,
            0,
            None,
            None,
        )
        assert scope_row.updated_at.tzinfo is not None
        assert scope_row.updated_at.utcoffset() is not None
        assert scope_row.updated_at_is_finite is True
    finally:
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {fresh_schema} CASCADE')


def test_bootstrap_mode_migrates_all_central_schemas_in_one_shared_schema(
        postgres_engine, monkeypatch: pytest.MonkeyPatch) -> None:
    fresh_schema = f'all_central_fresh_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {fresh_schema}')
    fresh_url = postgres_engine.url.update_query_dict(
        {'options': f'-csearch_path={fresh_schema}'})
    fresh_engine = sqlalchemy.create_engine(fresh_url)
    monkeypatch.setenv(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'bootstrap')
    try:
        global_user_state.create_table(fresh_engine)
        serve_state.create_table(fresh_engine)
        state_storage.create_table(fresh_engine)

        with fresh_engine.connect() as connection:
            revisions = {
                table: connection.execute(
                    sqlalchemy.text(f'SELECT version_num FROM {table}')
                ).scalar_one() for table in (
                    'alembic_version_state_db',
                    'alembic_version_serve_state_db',
                    'alembic_version_spot_jobs_db',
                )
            }
        assert revisions == {
            'alembic_version_state_db':
                migration_utils.GLOBAL_USER_STATE_VERSION,
            'alembic_version_serve_state_db': migration_utils.SERVE_VERSION,
            'alembic_version_spot_jobs_db': migration_utils.SPOT_JOBS_VERSION,
        }
    finally:
        fresh_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {fresh_schema} CASCADE')


@pytest.mark.parametrize(('mutations', 'expected_error'), [
    pytest.param(("ALTER TABLE container_images ALTER COLUMN builder_version "
                  "TYPE VARCHAR(64)",),
                 'structurally incompatible.*columns',
                 id='changed-column-type'),
    pytest.param(('ALTER TABLE container_image_workers ALTER COLUMN in_flight '
                  'SET DEFAULT 1',),
                 'structurally incompatible.*columns',
                 id='changed-column-default'),
    pytest.param(('ALTER TABLE clusters ALTER COLUMN '
                  'container_image_binding_known TYPE BIGINT',),
                 'structurally incompatible cluster binding columns',
                 id='changed-cluster-binding-column-type'),
    pytest.param(('ALTER TABLE clusters ALTER COLUMN '
                  'container_image_binding_known SET DEFAULT 1',),
                 'structurally incompatible cluster binding columns',
                 id='changed-cluster-binding-column-default'),
    pytest.param(('ALTER TABLE container_images DROP CONSTRAINT '
                  'ck_container_images_nonnegative_sizes',
                  'ALTER TABLE container_images ADD CONSTRAINT '
                  'ck_container_images_nonnegative_sizes CHECK '
                  '(manifest_size_bytes >= -1 AND declared_size_bytes >= 0)'),
                 'structurally incompatible.*constraints',
                 id='changed-check-constraint'),
    pytest.param(('ALTER TABLE container_image_workers DROP CONSTRAINT '
                  'container_image_workers_grant_budget_id_fkey',
                  'ALTER TABLE container_image_workers ADD CONSTRAINT '
                  'container_image_workers_grant_budget_id_fkey FOREIGN KEY '
                  '(grant_budget_id) REFERENCES '
                  'container_image_provider_budgets(id) ON DELETE CASCADE'),
                 'structurally incompatible.*constraints',
                 id='changed-foreign-key'),
    pytest.param(('DROP INDEX ix_container_images_workspace_created',
                  'CREATE INDEX ix_container_images_workspace_created '
                  'ON container_images (workspace, id)'),
                 'structurally incompatible.*indexes',
                 id='changed-index-definition'),
    pytest.param(
        ('DROP INDEX ix_container_image_publications_workspace_history',
         'CREATE INDEX ix_container_image_publications_workspace_history '
         'ON container_image_publications (workspace, id, created_at)'),
        'structurally incompatible.*indexes',
        id='changed-known-preview-index-definition'),
    pytest.param(
        ('ALTER TABLE container_image_locations DROP COLUMN '
         'copy_claimable_at CASCADE',
         'ALTER TABLE container_image_locations ADD COLUMN '
         'copy_claimable_at BIGINT GENERATED ALWAYS AS '
         "(CASE WHEN state = 'PENDING' THEN updated_at ELSE NULL END) "
         'STORED', 'CREATE INDEX ix_container_image_locations_copy_pending ON '
         'container_image_locations (shard_id, copy_claimable_at, id) '
         "WHERE state = 'PENDING'",
         'CREATE INDEX ix_container_image_locations_copy_recovery ON '
         'container_image_locations (shard_id, copy_claimable_at, id) '
         "WHERE state IN ('COPYING', 'VERIFYING')"),
        'structurally incompatible.*columns',
        id='changed-generated-expression'),
    pytest.param(
        ('ALTER TABLE container_image_operations DROP COLUMN '
         'canary_claimable_at CASCADE',
         'ALTER TABLE container_image_operations ADD COLUMN '
         'canary_claimable_at BIGINT GENERATED ALWAYS AS '
         "(CASE WHEN kind = 'PROFILE_CANARY' AND state = 'PENDING' THEN "
         'created_at ELSE NULL END) STORED',
         'CREATE INDEX ix_container_image_operations_canary_queue ON '
         'container_image_operations (canary_claimable_at, id) '
         'WHERE canary_claimable_at IS NOT NULL'),
        'structurally incompatible.*columns',
        id='changed-canary-generated-expression'),
    pytest.param(('DROP TABLE container_image_workers',),
                 'incomplete managed image state; missing tables',
                 id='missing-table'),
    pytest.param(('CREATE TABLE container_image_preview_legacy (id TEXT)',),
                 'incomplete managed image state; unexpected tables',
                 id='unexpected-table'),
    pytest.param(
        ("UPDATE container_image_catalog SET authority_id = 'invalid'",),
        'requires exactly one catalog authority row',
        id='invalid-authority-uuid'),
    pytest.param(("INSERT INTO container_image_catalog "
                  "(id, authority_id, created_at) VALUES "
                  "('extra', '00000000-0000-4000-8000-000000000002', 1)",),
                 'requires exactly one catalog authority row',
                 id='extra-catalog-row'),
])
def test_migration_024_preview_adoption_requires_exact_schema(
        postgres_engine, mutations: tuple[str, ...],
        expected_error: str) -> None:
    preview_schema = f'image_preview_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {preview_schema}')
    preview_engine = _schema_engine(postgres_engine, preview_schema)
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    try:
        with preview_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
        _migration_call(preview_engine, migration_024.upgrade)
        with preview_engine.begin() as connection:
            connection.exec_driver_sql('DROP TABLE auth_sessions')
            for statement in mutations:
                connection.exec_driver_sql(statement)

        with pytest.raises(RuntimeError, match=expected_error):
            _migration_call(preview_engine, migration_024.upgrade)
        # The failed adoption transaction must not leave the predecessor table
        # or other migration writes behind.
        assert not sqlalchemy.inspect(preview_engine).has_table('auth_sessions')
    finally:
        preview_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {preview_schema} CASCADE')


def test_migration_024_adopts_preview_missing_known_additive_indexes(
        postgres_engine) -> None:
    preview_schema = f'image_preview_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {preview_schema}')
    preview_engine = _schema_engine(postgres_engine, preview_schema)
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    try:
        with preview_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
        _migration_call(preview_engine, migration_024.upgrade)
        compatible_indexes = tuple((name, table_name) for name, table_name, _, _
                                   in migration_024._PREVIEW_COMPATIBLE_INDEXES)
        with preview_engine.begin() as connection:
            connection.exec_driver_sql('DROP TABLE auth_sessions')
            for index_name, _ in compatible_indexes:
                connection.exec_driver_sql(f'DROP INDEX {index_name}')

        _migration_call(preview_engine, migration_024.upgrade)

        inspector = sqlalchemy.inspect(preview_engine)
        restored_indexes = {(item['name'], table_name)
                            for _, table_name in compatible_indexes
                            for item in inspector.get_indexes(table_name)}
        assert set(compatible_indexes) <= restored_indexes
        assert sqlalchemy.inspect(preview_engine).has_table('auth_sessions')
    finally:
        preview_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {preview_schema} CASCADE')


def test_migration_024_adopts_old_preview_custody_and_operation_link(
        postgres_engine) -> None:
    preview_schema = f'image_preview_old_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {preview_schema}')
    preview_engine = _schema_engine(postgres_engine, preview_schema)
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    try:
        with preview_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TABLE clusters (name TEXT PRIMARY KEY)')
        _migration_call(preview_engine, migration_024.upgrade)
        with preview_engine.begin() as connection:
            connection.execute(schema.profile_revisions.insert().values(
                id='preview-profile-revision',
                workspace='research',
                profile='preview-profile',
                revision=1,
                desired_generation=1,
                state=models.ImageProfileState.ACTIVE.value,
                config_hash='config-hash',
                config_json='{}',
                physical_manifest_hash='physical-manifest-hash',
                created_at=1,
                updated_at=1))
            connection.execute(schema.operations.insert().values(
                id='preview-operation',
                authority_id='00000000-0000-4000-8000-000000000001',
                scope='research',
                actor_hash='1' * 64,
                kind='PUBLISH',
                idempotency_key='preview-operation-key',
                request_hash='2' * 64,
                state=models.ImageOperationState.SUCCEEDED.value,
                created_at=1,
                updated_at=1,
                terminal_expires_at=100))
            connection.execute(schema.images.insert().values(
                id='preview-image',
                workspace='research',
                runtime_digest=_DIGEST,
                platform='linux/amd64',
                config_digest=_CONFIG_DIGEST,
                manifest_media_type=_MANIFEST_MEDIA_TYPE,
                manifest_size_bytes=1,
                declared_size_bytes=1,
                creator_user_hash='1' * 64,
                producer_kind='external_oci',
                created_at=1,
                updated_at=1))
            connection.execute(schema.sources.insert().values(
                id='preview-source',
                workspace='research',
                image_id='preview-image',
                source_ref=_SOURCE,
                source_root_digest=_DIGEST,
                source_root_media_type=_MANIFEST_MEDIA_TYPE,
                requested_platform='linux/amd64',
                selected_child_digest=_DIGEST,
                created_at=1))
            connection.execute(schema.registry_shards.insert().values(
                id='preview-shard',
                workspace='research',
                profile='preview-profile',
                profile_revision_id='preview-profile-revision',
                target_id='canonical',
                provider='aws',
                partition='aws',
                account='123456789012',
                region='us-east-1',
                shard_generation=0,
                shard_index=0,
                target_fingerprint='target-fingerprint',
                physical_fingerprint='physical-fingerprint',
                eviction_enabled=False,
                registry='123456789012.dkr.ecr.us-east-1.amazonaws.com',
                repository_name='skypilot/images/s00',
                repository_arn='arn:aws:ecr:us-east-1:123456789012:repository/x',
                max_manifests=100,
                max_declared_bytes=1000,
                max_in_flight=1,
                state=models.ImageShardState.READY.value,
                created_at=1,
                updated_at=1))
            connection.execute(schema.locations.insert().values(
                id='preview-location',
                workspace='research',
                image_id='preview-image',
                shard_id='preview-shard',
                target_fingerprint='target-fingerprint',
                physical_fingerprint='physical-fingerprint',
                runtime_digest=_DIGEST,
                canonical=True,
                target_ref=f'example.invalid/image@{_DIGEST}',
                state=models.ImageLocationState.READY.value,
                reserved_declared_bytes=1,
                created_at=1,
                updated_at=1))
            connection.execute(schema.publications.insert().values(
                id='preview-publication',
                workspace='research',
                operation_id='preview-operation',
                profile_revision_id='preview-profile-revision',
                requested_release='preview-release',
                reservation_active=True,
                source_ref=_SOURCE,
                source_root_digest=_DIGEST,
                requested_platform='linux/amd64',
                state=models.ImagePublicationState.READY.value,
                image_id='preview-image',
                source_id='preview-source',
                canonical_location_id='preview-location',
                created_at=1,
                updated_at=2))
            connection.exec_driver_sql('DROP TABLE auth_sessions')
            connection.exec_driver_sql(
                'DROP TABLE container_image_profile_custody')
            connection.exec_driver_sql(
                'DROP INDEX ix_container_image_publications_operation')
            connection.exec_driver_sql(
                'ALTER TABLE container_image_publications DROP CONSTRAINT '
                'container_image_publications_operation_id_fkey')
            connection.exec_driver_sql(
                'ALTER TABLE container_image_publications ALTER COLUMN '
                'operation_id SET NOT NULL')
            connection.exec_driver_sql(
                'ALTER TABLE container_image_publications ADD CONSTRAINT '
                'container_image_publications_operation_id_fkey FOREIGN KEY '
                '(operation_id) REFERENCES container_image_operations(id)')
            connection.exec_driver_sql(
                'ALTER TABLE container_image_operations DROP COLUMN '
                'canary_claimable_at CASCADE')
            connection.exec_driver_sql(
                'CREATE INDEX ix_container_image_operations_canary_queue ON '
                'container_image_operations (state, lease_expires_at, id) '
                "WHERE kind = 'PROFILE_CANARY' AND state = 'RUNNING'")

        _migration_call(preview_engine, migration_024.upgrade)

        inspector = sqlalchemy.inspect(preview_engine)
        operation_column = next(
            column
            for column in inspector.get_columns('container_image_publications')
            if column['name'] == 'operation_id')
        operation_fk = next(
            foreign_key for foreign_key in inspector.get_foreign_keys(
                'container_image_publications')
            if foreign_key['constrained_columns'] == ['operation_id'])
        canary_column = next(
            column
            for column in inspector.get_columns('container_image_operations')
            if column['name'] == 'canary_claimable_at')
        canary_index = next(
            index
            for index in inspector.get_indexes('container_image_operations')
            if index['name'] == 'ix_container_image_operations_canary_queue')
        assert operation_column['nullable']
        assert operation_fk['options']['ondelete'] == 'SET NULL'
        assert canary_column['computed']['persisted']
        assert canary_index['column_names'] == ['canary_claimable_at', 'id']
        with preview_engine.connect() as connection:
            custody = connection.execute(
                sqlalchemy.text(
                    'SELECT workspace, profile, physical_manifest_hash, '
                    'first_profile_revision_id, acquired_at FROM '
                    'container_image_profile_custody')).one()
        assert tuple(custody) == ('research', 'preview-profile',
                                  'physical-manifest-hash',
                                  'preview-profile-revision', 2)
        assert sqlalchemy.inspect(preview_engine).has_table('auth_sessions')
    finally:
        preview_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {preview_schema} CASCADE')


def test_migration_024_adds_auth_and_cluster_binding_columns_to_sqlite(
) -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'clusters', metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True))
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO clusters (name) VALUES ('legacy-cluster')"))
    migration_024 = importlib.import_module(
        'sky.schemas.db.global_user_state.024_container_images')
    try:
        _migration_call(engine, migration_024.upgrade)
        inspector = sqlalchemy.inspect(engine)
        assert set(inspector.get_table_names()) == {'auth_sessions', 'clusters'}
        assert {column['name'] for column in inspector.get_columns('clusters')
               } == {
                   'name', 'container_image_binding_known',
                   'container_image_consumer_kind',
                   'container_image_consumer_owner'
               }
        with engine.connect() as connection:
            legacy_binding = connection.execute(
                sqlalchemy.text('SELECT container_image_binding_known, '
                                'container_image_consumer_kind, '
                                'container_image_consumer_owner FROM clusters '
                                "WHERE name = 'legacy-cluster'")).one()
        assert tuple(legacy_binding) == (0, None, None)
        _migration_call(engine, migration_024.downgrade)
        assert {
            column['name']
            for column in sqlalchemy.inspect(engine).get_columns('clusters')
        } == {'name'}
        assert {
            column['name'] for column in sqlalchemy.inspect(engine).get_columns(
                'auth_sessions')
        } == {'code_challenge', 'token', 'created_at'}
    finally:
        engine.dispose()


def _prototype_spec() -> builder_prototype.BuildSpec:
    return builder_prototype.BuildSpec.from_dict({
        'base': f'ghcr.io/boltz/runtime@{_DIGEST}',
        'setup': [{
            'run': 'cat /inputs/requirements.txt',
            'inputs': ['requirements.txt'],
        }],
        'context': {
            'include': ['requirements.txt'],
        },
        'source': {
            'mode': 'late_bound',
            'include': [],
        },
        'platform': 'linux/amd64',
        'output': {
            'workspace': 'research',
            'distribution': 'gpu-production',
            'release': 'builder-release',
            'staging_repository':
                ('123456789012.dkr.ecr.us-east-1.amazonaws.com/staging'),
            'source_auth': None,
        },
    })


def test_builder_prototype_postgres_lease_is_exact_and_recoverable(
        postgres_engine, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / 'requirements.txt').write_text('numpy==2.0.0\n',
                                               encoding='utf-8')
    spec = _prototype_spec()
    manifest = builder_prototype.create_context_manifest(tmp_path, spec)
    cache_key = builder_prototype.dependency_cache_key(spec, manifest)
    schema_name = f'skypilot_image_builder_{uuid.uuid4().hex[:16]}'
    current = {'value': 100}
    monkeypatch.setattr(builder_prototype.time, 'time',
                        lambda: current['value'])
    repository = builder_prototype.PrototypeRepository(
        postgres_engine.url.render_as_string(hide_password=False), schema_name)
    try:
        record = repository.create_or_get(
            idempotency_key='builder-idempotency-0001',
            spec=spec,
            manifest=manifest,
            cache_key=cache_key)
        replay = repository.create_or_get(
            idempotency_key='builder-idempotency-0001',
            spec=spec,
            manifest=manifest,
            cache_key=cache_key)
        assert replay.id == record.id
        record = repository.transition(record.id, ('PENDING',), 'UPLOADING')
        record = repository.transition(record.id, ('UPLOADING',), 'QUEUED')
        record, first_token = repository.claim(record.id)
        assert first_token is not None and record.state == 'BUILDING'
        with pytest.raises(RuntimeError, match='BUILDER_LEASE_LOST'):
            repository.complete_build(record.id, 'wrong-token', _DIGEST)
        current['value'] += 30 * 60 + 1
        record, second_token = repository.claim(record.id)
        assert second_token is not None and second_token != first_token
        with pytest.raises(RuntimeError, match='BUILDER_LEASE_LOST'):
            repository.complete_build(record.id, first_token, _DIGEST)
        record = repository.complete_build(record.id, second_token, _DIGEST)
        assert record.state == 'VERIFYING'
        assert record.output_digest == _DIGEST
    finally:
        repository._engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA {schema_name} CASCADE')


def test_profile_activation_locks_ordered_rows_before_catalog_barrier(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    short_lived, revision, scheduler_now = _short_lived_qualifying_profile(
        image_database, profile)
    shard = topology_state.list_shards('research', short_lived.name)[0]
    budget = topology_state.get_provider_budget(
        provider='aws',
        partition=short_lived.partition,
        account=short_lived.registry_account,
        region=short_lived.canonical.region,
        api_family='ecr')
    assert budget is not None
    requirements = qualification._attestation_requirements(  # pylint: disable=protected-access
        short_lived, revision.attestations)
    events: list[str] = []
    original_mutation = (topology_state.get_qualification_mutation_in_session)
    original_epoch = catalog_state.database_epoch

    def _assert_row_locked(statement: sqlalchemy.sql.Select) -> None:
        with image_database.connect() as connection:
            transaction = connection.begin()
            try:
                with pytest.raises(sqlalchemy.exc.OperationalError) as error:
                    connection.execute(
                        statement.with_for_update(nowait=True)).all()
                assert getattr(error.value.orig, 'pgcode', None) == '55P03'
            finally:
                transaction.rollback()

    def _observe_catalog_lock(
        session: orm.Session,
        *,
        exclusive: bool,
    ) -> sqlalchemy.engine.RowMapping | None:
        assert not exclusive
        _assert_row_locked(
            sqlalchemy.select(schema.profile_revisions).where(
                schema.profile_revisions.c.id == revision.id))
        _assert_row_locked(
            sqlalchemy.select(schema.provider_budgets).where(
                schema.provider_budgets.c.id == budget.id))
        _assert_row_locked(
            sqlalchemy.select(schema.registry_shards).where(
                schema.registry_shards.c.id == shard.id))
        mutation = original_mutation(session, exclusive=exclusive)
        events.append('catalog-lock-acquired')
        return mutation

    def _observe_epoch(session: orm.Session, *, now: int | None = None) -> int:
        assert events == ['catalog-lock-acquired']
        events.append('clock-sampled')
        return original_epoch(session, now=now)

    monkeypatch.setattr(topology_state, 'get_qualification_mutation_in_session',
                        _observe_catalog_lock)
    monkeypatch.setattr(catalog_state, 'database_epoch', _observe_epoch)

    activated = transactions.activate_profile(
        profile_revision_id=revision.id,
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash,
        expected_terraform_hash=revision.terraform_hash,
        expected_attestations_hash=revision.attestations_hash,
        required_attestations=requirements,
        now=scheduler_now)

    assert activated.state == models.ImageProfileState.ACTIVE
    assert events == ['catalog-lock-acquired', 'clock-sampled']


def test_record_qualification_copy_uses_minimum_catalog_lock_mode(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active = _activate_profile(image_database, profile)
    target = profile.targets[0]
    repository_arn = _qualification_repository_arn(profile, target)
    armed, armed_now = qualification.arm_qualification_lifecycle(
        active,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        now=20)
    assert armed_now
    ordinary_proof = qualification.qualification_copy_restoration_proof_id(
        armed, target, _DIGEST)
    assert ordinary_proof is not None

    lock_modes: list[bool] = []
    original_mutation = (topology_state.get_qualification_mutation_in_session)

    def _observe_catalog_lock(
        session: orm.Session,
        *,
        exclusive: bool,
    ) -> sqlalchemy.engine.RowMapping | None:
        lock_modes.append(exclusive)
        return original_mutation(session, exclusive=exclusive)

    monkeypatch.setattr(topology_state, 'get_qualification_mutation_in_session',
                        _observe_catalog_lock)
    copied = qualification.record_qualification_copy(
        armed,
        target,
        repository_arn=repository_arn,
        runtime_digest=_DIGEST,
        platform=profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=ordinary_proof,
        now=21)
    assert copied is not None
    assert lock_modes == [False]

    restoration_profile = _qualification_generation_profile(
        _workspace_isolated_profile(profile, 'restoration'), 1)
    restoring_active = _activate_profile(image_database,
                                         restoration_profile,
                                         workspace='restoration')
    restoring_target = restoration_profile.targets[0]
    restoring_arn = _qualification_repository_arn(restoration_profile,
                                                  restoring_target)
    lifecycle_key = models.profile_attestation_key('lifecycle',
                                                   restoring_target.name)
    legacy = topology_state.record_profile_attestation(
        profile_revision_id=restoring_active.id,
        kind=lifecycle_key,
        evidence={
            'status': 'READY',
            'target': restoring_target.name,
            'target_fingerprint': restoring_target.target_fingerprint,
            'repository_arn': restoring_arn,
            'runtime_digest': _DIGEST,
            'exact_absence': True,
        },
        expected_generation=restoring_active.desired_generation,
        expected_config_hash=restoring_active.config_hash,
        now=22)
    restoring, restoration_proof = (
        qualification.begin_qualification_lifecycle_restoration(
            legacy,
            restoring_target,
            repository_arn=restoring_arn,
            runtime_digest=_DIGEST,
            now=23))
    assert restoration_proof is not None
    lock_modes.clear()

    restored = qualification.record_qualification_copy(
        restoring,
        restoring_target,
        repository_arn=restoring_arn,
        runtime_digest=_DIGEST,
        platform=restoration_profile.qualification.canary_platform,
        copy_outcome='COPIED',
        expected_lifecycle_proof_id=restoration_proof,
        expected_mutation_proof_id=restoration_proof,
        now=24)

    assert restored is not None
    assert lock_modes == [True]
