"""Real PostgreSQL proofs for the managed image state machine and migration."""
# pylint: disable=protected-access,redefined-outer-name

from __future__ import annotations

import concurrent.futures
import hashlib
import importlib
from pathlib import Path
import shutil
import threading
from typing import Any
import uuid

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy
from sqlalchemy import orm

from sky.container_images import builder_prototype
from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import demand_state
from sky.container_images import models
from sky.container_images import publication
from sky.container_images import schema
from sky.container_images import topology_state
from sky.container_images import transactions

testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    shutil.which('docker') is None,
    reason='docker unavailable; skipping real-PostgreSQL image tests')

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
    active = _activate_profile(image_database, profile)
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

    authority = catalog_state.get_catalog_authority_id(create=False)
    assert authority is not None
    demand = transactions.create_warming_demand_for_owner_epoch(
        authority_id=authority,
        workspace='research',
        consumer_kind='service_version',
        consumer_owner='boltz-l4:v7',
        target_key=f'{publication_record.image_id}:{west.target_fingerprint}',
        owner_epoch=123,
        image_id=publication_record.image_id,
        runtime_digest=_DIGEST,
        profile_revision_id=active.id,
        target_fingerprint=west.target_fingerprint,
        location_id=regional.id,
        placement={
            'provider': 'aws',
            'region': west.region
        },
        now=50)
    replay = transactions.create_warming_demand_for_owner_epoch(
        authority_id=authority,
        workspace='research',
        consumer_kind='service_version',
        consumer_owner='boltz-l4:v7',
        target_key=f'{publication_record.image_id}:{west.target_fingerprint}',
        owner_epoch=123,
        image_id=publication_record.image_id,
        runtime_digest=_DIGEST,
        profile_revision_id=active.id,
        target_fingerprint=west.target_fingerprint,
        location_id=regional.id,
        placement={
            'provider': 'aws',
            'region': west.region
        },
        now=51)
    assert replay.id == demand.id and replay.consumer_generation == 0
    demand = transactions.commit_ready_demand(
        demand_id=demand.id,
        consumer_generation=demand.consumer_generation,
        pull_plan={
            'reference': regional.target_ref,
            'runtime_digest': _DIGEST,
        },
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
    evicted = topology_state.complete_eviction(eviction.id,
                                               eviction.lease_token,
                                               absent=True,
                                               now=3702)
    assert evicted is not None
    assert evicted.state == models.ImageLocationState.EVICTED


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
