"""Real PostgreSQL proofs for the managed image state machine and migration."""
# pylint: disable=protected-access,redefined-outer-name

from __future__ import annotations

import concurrent.futures
import hashlib
import importlib
import os
from pathlib import Path
import pickle
import shutil
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
from sky.container_images import builder_prototype
from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import demand_state
from sky.container_images import models
from sky.container_images import publication
from sky.container_images import schema
from sky.container_images import topology_state
from sky.container_images import transactions

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
    authority = catalog_state.get_catalog_authority_id()
    assert authority is not None
    return postgres_engine


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
            fingerprint = hashlib.sha256(
                f'{target.target_fingerprint}:0'.encode()).hexdigest()
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
                shard_index=0,
                physical_fingerprint=fingerprint,
                registry=target.registry,
                repository_name=f'{target.repository_prefix}/test/s00',
                repository_arn=(f'arn:{profile.partition}:ecr:{target.region}:'
                                f'{profile.registry_account}:repository/'
                                f'{target.repository_prefix}/test/s00'),
                max_manifests=100,
                max_declared_bytes=1_000_000,
                max_in_flight=4,
                now=11)
        session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.workspace == 'research',
            schema.registry_shards.c.profile == profile.name).values(
                state=models.ImageShardState.READY.value,
                qualified_at=11,
                updated_at=11))
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
    return transactions.activate_profile(
        profile_revision_id=revision.id,
        expected_generation=revision.desired_generation,
        expected_config_hash=profile.config_hash,
        expected_terraform_hash='f' * 64,
        expected_attestations_hash=attested.attestations_hash,
        required_attestations={'terraform': None},
        now=13)


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
    backend: str = 'aws_vm',
    placement_region: str | None = None,
    now: int = 50,
) -> demand_state.DemandRecord:
    assert publication_record.image_id is not None
    authority = catalog_state.get_catalog_authority_id(create=False)
    assert authority is not None
    target = next(target for target in (profile.canonical,) + profile.targets
                  if target.target_fingerprint == location.target_fingerprint)
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
            'consumer': {
                'request_id': request_id,
            },
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
    authority = catalog_state.get_catalog_authority_id(create=False)
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
                EXPLAIN (COSTS OFF)
                SELECT id
                FROM container_image_demands
                WHERE consumer_kind = 'cluster'
                  AND consumer_attached IS false
                  AND request_id = 'request-target'
                  AND state IN ('WARMING', 'READY', 'FAILED')
                FOR UPDATE
            """)).scalars().all()
    assert 'ix_container_image_demands_cluster_request' in str(plan)

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
                             owner='cluster-a',
                             consumer_kind='cluster',
                             controller_epoch='cluster-request:launch-a',
                             controller_sequence=None)

    assert demand_state.release_demand_authoritatively(demand.id,
                                                       'research',
                                                       now=51)
    released = demand_state.get_demand(demand.id, 'research')
    assert released is not None
    assert released.state == models.ImageDemandState.RELEASED


def test_cluster_row_and_demand_release_commit_atomically(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active,
                             publication_record,
                             regional,
                             profile,
                             owner='cluster-a',
                             consumer_kind='cluster',
                             controller_epoch='cluster-request:launch-a',
                             controller_sequence=None)
    global_user_state.cluster_table.create(image_database, checkfirst=True)
    handle = types.SimpleNamespace(launched_resources=types.SimpleNamespace(
        resolved_container_image=types.SimpleNamespace(demand_id=demand.id)))
    with image_database.begin() as connection:
        connection.execute(global_user_state.cluster_table.insert().values(
            name='cluster-a',
            handle=pickle.dumps(handle),
            status='UP',
            workspace='research'))
    monkeypatch.setattr(global_user_state._db_manager, 'get_engine',
                        lambda: image_database)

    global_user_state.remove_cluster('cluster-a', terminate=True)

    with image_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(global_user_state.cluster_table.c.name).where(
                global_user_state.cluster_table.c.name ==
                'cluster-a')).first() is None
    released = demand_state.get_demand(demand.id, 'research')
    assert released is not None
    assert released.state == models.ImageDemandState.RELEASED


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
                                                       now=53)
    assert demand_state.mark_owner_deleted(workspace='research',
                                           consumer_kind='managed_job_task',
                                           consumer_owner=owner,
                                           credential_expires_at=54,
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

    def _locked_table(statement: str) -> str | None:
        normalized = ' '.join(statement.split()).upper()
        if ' FOR UPDATE' not in normalized:
            return None
        if ' FROM CONTAINER_IMAGE_CONSUMER_WATERMARKS ' in normalized:
            return 'watermark'
        if ' FROM CONTAINER_IMAGE_DEMANDS ' in normalized:
            return 'demand'
        return None

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
        deadline = time.monotonic() + 5
        release_blocked = False
        while time.monotonic() < deadline:
            with image_database.connect() as observer:
                release_blocked = bool(
                    observer.execute(
                        sqlalchemy.text(
                            'SELECT wait_event_type = \'Lock\' '
                            'FROM pg_stat_activity WHERE pid = :pid'), {
                                'pid': release_backend['pid']
                            }).scalar())
            if release_blocked:
                break
            time.sleep(0.01)
        assert release_blocked
        allow_compaction_to_continue.set()

        assert compaction_future.result(timeout=5) == (1, 0)
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


def test_terminal_compaction_requires_proof_and_deletes_empty_owner(
        image_database, monkeypatch: pytest.MonkeyPatch,
        profile: models.ManagedRegistryProfile) -> None:
    active, publication_record, _, regional = _ready_regional(
        image_database, monkeypatch, profile)
    demand = _warming_demand(active, publication_record, regional, profile)
    assert demand_state.supersede_demand(demand.id, 'research', now=51)
    with image_database.begin() as connection:
        connection.execute(schema.demands.update().where(
            schema.demands.c.id == demand.id).values(expires_at=100))

    assert demand_state.compact_terminal_demands(now=1000) == (0, 0)
    assert demand_state.get_demand(demand.id, 'research') is not None

    assert demand_state.mark_owner_deleted(workspace='research',
                                           consumer_kind=demand.consumer_kind,
                                           consumer_owner=demand.consumer_owner,
                                           credential_expires_at=900,
                                           now=900)
    assert demand_state.compact_terminal_demands(now=1000) == (1, 1)
    assert demand_state.get_demand(demand.id, 'research') is None
    with image_database.connect() as connection:
        watermark = connection.execute(
            sqlalchemy.select(
                schema.consumer_watermarks.c.consumer_owner).where(
                    schema.consumer_watermarks.c.workspace == 'research',
                    schema.consumer_watermarks.c.consumer_kind ==
                    demand.consumer_kind,
                    schema.consumer_watermarks.c.consumer_owner ==
                    demand.consumer_owner)).first()
    assert watermark is None

    with pytest.raises(ValueError, match='page size'):
        demand_state.compact_terminal_demands(limit=0)
    with pytest.raises(ValueError, match='page size'):
        demand_state.compact_terminal_demands(limit=1001)


def test_shard_admission_retries_after_locked_home_fills(
        image_database, profile: models.ManagedRegistryProfile) -> None:
    _activate_profile(image_database, profile)
    target = profile.targets[0]
    with orm.Session(image_database) as session, session.begin():
        fingerprint = hashlib.sha256(
            f'{target.target_fingerprint}:1'.encode()).hexdigest()
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
            shard_index=1,
            physical_fingerprint=fingerprint,
            registry=target.registry,
            repository_name=f'{target.repository_prefix}/test/s01',
            repository_arn=(f'arn:{profile.partition}:ecr:{target.region}:'
                            f'{profile.registry_account}:repository/'
                            f'{target.repository_prefix}/test/s01'),
            max_manifests=100,
            max_declared_bytes=1_000_000,
            max_in_flight=4,
            now=12)
        session.execute(schema.registry_shards.update().where(
            schema.registry_shards.c.workspace == 'research',
            schema.registry_shards.c.profile == profile.name,
            schema.registry_shards.c.target_id == target.name).values(
                state=models.ImageShardState.READY.value,
                qualified_at=12,
                updated_at=12))

    with image_database.connect() as connection:
        ordered_ids = connection.execute(
            sqlalchemy.select(schema.registry_shards.c.id).where(
                schema.registry_shards.c.workspace == 'research',
                schema.registry_shards.c.profile == profile.name,
                schema.registry_shards.c.target_id == target.name).order_by(
                    sqlalchemy.func.md5(schema.registry_shards.c.id + _DIGEST),
                    schema.registry_shards.c.id)).scalars().all()
    assert len(ordered_ids) == 2
    home_id, fallback_id = ordered_ids
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


def test_ambiguous_eviction_remains_fenced_until_exact_verification(
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
    ambiguous = topology_state.complete_eviction(eviction.id,
                                                 eviction.lease_token,
                                                 present=None,
                                                 now=101)
    assert ambiguous is not None
    assert ambiguous.state == models.ImageLocationState.EVICTING
    assert ambiguous.error_code == (
        models.ImageLocationErrorCode.PROVIDER_OUTCOME_AMBIGUOUS.value)
    exact = topology_state.complete_eviction(eviction.id,
                                             eviction.lease_token,
                                             present=True,
                                             now=102)
    assert exact is not None and exact.state == models.ImageLocationState.READY
    shard_after = topology_state.get_shard(regional.shard_id)
    assert shard_after is not None
    assert shard_after.reserved_manifests == shard_before.reserved_manifests
    assert (shard_after.reserved_declared_bytes ==
            shard_before.reserved_declared_bytes)
    assert shard_after.in_flight == 0


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
    # The publish operation expires at the same horizon, but its retained
    # publication still owns it through a foreign key until record retention.
    catalog_state.compact_terminal_records(now=reservation_expiry)
    assert catalog_state.get_operation(publication_record.operation_id,
                                       'research') is not None
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

    queues = topology_state.readiness_queue_stats('research')
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
                          data_type, udt_name, is_nullable, column_default
                   FROM information_schema.columns
                   WHERE table_schema = :schema
                     AND table_name LIKE 'container_image%'
                   ORDER BY table_name, ordinal_position"""), {
                'schema': schema_name
            }).all()
        constraints = connection.execute(
            sqlalchemy.text("""SELECT relation.relname, constraint_row.conname,
                          constraint_row.contype,
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
            sqlalchemy.text("""SELECT tablename, indexname, indexdef
                   FROM pg_indexes
                   WHERE schemaname = :schema
                     AND tablename LIKE 'container_image%'
                   ORDER BY tablename, indexname"""), {
                'schema': schema_name
            }).all()
    normalized_indexes = [(table, name,
                           definition.replace(f'{schema_name}.', '<schema>.'))
                          for table, name, definition in indexes]
    return {
        'columns': [tuple(row) for row in columns],
        'constraints': [tuple(row) for row in constraints],
        'indexes': normalized_indexes,
    }


def test_migration_023_matches_runtime_metadata_and_downgrade_is_empty_only(
        postgres_engine) -> None:
    migration_schema = f'image_migration_{uuid.uuid4().hex}'
    runtime_schema = f'image_runtime_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {migration_schema}')
        connection.exec_driver_sql(f'CREATE SCHEMA {runtime_schema}')
    migration_engine = _schema_engine(postgres_engine, migration_schema)
    runtime_engine = _schema_engine(postgres_engine, runtime_schema)
    migration_023 = importlib.import_module(
        'sky.schemas.db.global_user_state.023_container_images')
    try:
        _migration_call(migration_engine, migration_023.upgrade)
        schema.metadata.create_all(runtime_engine)
        assert _schema_shape(migration_engine,
                             migration_schema) == _schema_shape(
                                 runtime_engine, runtime_schema)

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
            _migration_call(migration_engine, migration_023.downgrade)
        with migration_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('DELETE FROM container_image_operations'))
        _migration_call(migration_engine, migration_023.downgrade)
        assert sqlalchemy.inspect(migration_engine).get_table_names() == []
    finally:
        migration_engine.dispose()
        runtime_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {migration_schema} CASCADE')
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {runtime_schema} CASCADE')


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
