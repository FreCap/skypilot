"""Real PostgreSQL proofs for the managed image state machine and migration."""
# pylint: disable=protected-access,redefined-outer-name

from __future__ import annotations

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
from sky.container_images import catalog_state
from sky.container_images import config
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
    engine: sqlalchemy.engine.Engine, profile: models.ManagedRegistryProfile
) -> topology_state.ProfileRevisionRecord:
    revision = topology_state.stage_profile_revision(
        workspace='research',
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
                    workspace='research',
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
            schema.registry_shards.c.workspace == 'research',
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
                    schema.registry_shards.c.eviction_enabled)).scalars())
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
    for shard in topology_state.list_shards('research', profile.name):
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
    assert attested.attestations_hash is not None
    active = transactions.activate_profile(
        profile_revision_id=revision.id,
        expected_generation=revision.desired_generation,
        expected_config_hash=profile.config_hash,
        expected_terraform_hash='f' * 64,
        expected_attestations_hash=attested.attestations_hash,
        required_attestations={'terraform': None},
        now=13)
    shards = topology_state.list_shards('research', profile.name)
    expected_eviction = {
        target.name: target.delete_authority is not None
        for target in (profile.canonical,) + profile.targets
    }
    assert all(shard.profile_revision_id == active.id and
               shard.eviction_enabled == expected_eviction[shard.target_id]
               for shard in shards)
    return active


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


def _request_ec2_canary(
    monkeypatch: pytest.MonkeyPatch,
    profile: models.ManagedRegistryProfile,
    *,
    idempotency_key: str,
) -> catalog_state.OperationRecord:
    _configure_profile(monkeypatch, profile)
    target = profile.targets[0]
    binding_id = target.runtime_binding('aws_vm')
    assert binding_id is not None
    runtime_id = qualification.runtime_ids(target, 'aws_vm',
                                           profile.bindings[binding_id])[0]
    operation, _ = qualification.request_canary(workspace='research',
                                                profile_name=profile.name,
                                                target_id=target.name,
                                                backend='aws_vm',
                                                runtime_id=runtime_id,
                                                actor_hash='1' * 64,
                                                idempotency_key=idempotency_key)
    return operation


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
    with pytest.MonkeyPatch.context() as clock:
        clock.setattr(catalog_state.time, 'time', lambda: now)
        mutation = publication.publish(source_ref=source,
                                       release=release,
                                       distribution=profile.name,
                                       workspace='research',
                                       actor_hash='1' * 64,
                                       idempotency_key=idempotency_key)
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
        retry_at=40,
        terminal=False,
        now=31)
    assert retried.state == models.ImageLocationState.PENDING
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
        with pytest.MonkeyPatch.context() as clock:
            clock.setattr(transactions.time, 'time', lambda: 32)
            return publication.retry(
                publication_id=publication_record.id,
                workspace='research',
                actor_hash='2' * 64,
                idempotency_key=(
                    f'canonical-retry-{shard_state.value.lower()}-0001'))

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
                                                   interval_seconds=1)
    assert claimed is not None and claimed.id == regional.shard_id
    assert claimed.inventory_lease_token is not None

    lock_connection = image_database.connect()
    lock_transaction = lock_connection.begin()
    lock_connection.execute(
        sqlalchemy.select(schema.locations.c.id).where(
            schema.locations.c.id == regional.id).with_for_update()).one()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(topology_state.record_inventory_page,
                                     claimed.id, claimed.inventory_lease_token,
                                     (regional.runtime_digest,), 'next-page')
            time.sleep(0.1)
            assert not future.done()
            lock_connection.exec_driver_sql('SELECT pg_sleep(1.2)')
            lock_transaction.commit()
            assert future.result(timeout=10) is None
    finally:
        if lock_transaction.is_active:
            lock_transaction.rollback()
        lock_connection.close()

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
    assert recorded is not None and recorded.attestations[key] == evidence
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
        placement={
            'provider': 'aws',
            'region': placement_region or target.region,
            'backend': backend,
            'platform': 'linux/amd64',
            'consumer': consumer,
        },
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
        placement={
            'provider': 'aws',
            'region': west.region,
            'backend': 'aws_vm',
            'platform': 'linux/amd64',
            'consumer': {
                'request_id': 'request-1',
            },
        },
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

    global_user_state.remove_cluster('cluster-a', terminate=True)

    with image_database.connect() as connection:
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
    assert watermark['owner_deleted_at'] == 100
    assert demand_state.compact_terminal_demands(
        now=100 + demand_state._TERMINAL_RETENTION_SECONDS) == 1
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


def test_locked_oldest_shard_does_not_block_global_eviction(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    _, publication_record, canonical, regional = _ready_regional(
        image_database, monkeypatch, profile)
    assert publication_record.image_id is not None
    target = profile.targets[0]
    older_image_id = str(uuid.uuid4())
    older_location_id = str(uuid.uuid4())
    physical_fingerprint = hashlib.sha256(
        f'{target.target_fingerprint}:1'.encode()).hexdigest()
    with orm.Session(image_database) as session, session.begin():
        older_shard = topology_state.upsert_qualified_shard(
            session,
            workspace='research',
            profile=profile.name,
            target_id=target.name,
            provider='aws',
            partition=profile.partition,
            account=profile.registry_account,
            region=target.region,
            shard_generation=0,
            shard_index=1,
            target_fingerprint=target.target_fingerprint,
            physical_fingerprint=physical_fingerprint,
            registry=target.registry,
            repository_name=f'{target.repository_prefix}/test/s01',
            repository_arn=(f'arn:{profile.partition}:ecr:{target.region}:'
                            f'{profile.registry_account}:repository/'
                            f'{target.repository_prefix}/test/s01'),
            max_manifests=100,
            max_declared_bytes=1_000_000,
            max_in_flight=4,
            now=1)
        older_shard_id = older_shard.id
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
            target_ref=(f'{target.registry}/'
                        f'{target.repository_prefix}/test/s01@{_OTHER_DIGEST}'),
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
            sqlalchemy.text('ANALYZE container_image_registry_shards'))
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


def _schema_engine(base: sqlalchemy.engine.Engine,
                   schema_name: str) -> sqlalchemy.engine.Engine:
    return sqlalchemy.create_engine(
        base.url,
        connect_args={
            'options': f'-c search_path={schema_name} -c statement_timeout=15000'
        })


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
        assert revision == '024'
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
            ).scalar_one() == '024'
        assert sqlalchemy.inspect(fresh_engine).has_table(
            'container_image_catalog')
    finally:
        fresh_engine.dispose()
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
        assert operation_column['nullable']
        assert operation_fk['options']['ondelete'] == 'SET NULL'
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
