"""Real-PostgreSQL proofs for container-image profile revision fencing."""
# pylint: disable=protected-access,redefined-outer-name

import dataclasses
import importlib
import shutil
import threading
import types
import uuid

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy

from sky import global_user_state
from sky import resources as resources_lib
from sky import skypilot_config
from sky.container_images import models
from sky.container_images import references
from sky.container_images import state
from sky.provision import docker_utils

testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
pytest.importorskip('psycopg2')

pytestmark = pytest.mark.skipif(
    shutil.which('docker') is None,
    reason='docker unavailable; skipping real-Postgres image fence tests')

_DIGEST = 'sha256:' + 'a' * 64
_SOURCE = f'ghcr.io/boltz-bio/boltz@{_DIGEST}'
_OTHER_DIGEST = 'sha256:' + 'b' * 64
_OTHER_SOURCE = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
_TEST_PLATFORMS = ('linux/amd64', 'linux/arm64')


def _complete_location(*args, **kwargs) -> bool:
    """Completes test fixtures with a concrete platform proof by default."""
    if len(args) < 5 and 'platforms' not in kwargs:
        kwargs['platforms'] = _TEST_PLATFORMS
    return state.complete_location(*args, **kwargs)


def _profile(revision: int) -> models.RegistryProfile:
    return models.RegistryProfile(
        name='managed',
        ownership=models.RegistryOwnership.MANAGED,
        realm='production',
        namespace='skypilot/{workspace}',
        require_digest_at_runtime=True,
        canonical=models.RegistryTarget(name='canonical',
                                        provider='aws',
                                        account='123456789012',
                                        region='us-east-1',
                                        pull_auth='ecr_runtime_identity'),
        revision=revision,
        targets=(models.RegistryTarget(name='west',
                                       provider='aws',
                                       account='123456789012',
                                       region='us-west-2',
                                       pull_auth='ecr_runtime_identity'),))


def _mock_registry_profile(monkeypatch) -> None:
    data = {
        'container_registries': {
            'default_profile': 'managed',
            'profiles': {
                'managed': {
                    'revision': 1,
                    'ownership': 'managed',
                    'realm': 'production',
                    'namespace': 'skypilot/{workspace}',
                    'canonical': {
                        'provider': 'aws',
                        'account': '123456789012',
                        'region': 'us-east-1',
                        'pull_auth': 'ecr_runtime_identity',
                    },
                    'targets': [{
                        'name': 'west',
                        'provider': 'aws',
                        'account': '123456789012',
                        'region': 'us-west-2',
                        'pull_auth': 'ecr_runtime_identity',
                    }],
                },
            },
        },
        'workspaces': {
            'research': {
                'container_images': {
                    'mode': 'managed_required',
                    'default_profile': 'managed',
                    'allowed_profiles': ['managed'],
                    'locality': 'prefer',
                },
            },
        },
    }

    def get_nested(keys, default_value=None, **_):
        value = data
        for key in keys:
            if not isinstance(value, dict) or key not in value:
                return default_value
            value = value[key]
        return value

    monkeypatch.setattr(skypilot_config, 'get_nested', get_nested)
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'research')


def _ensure_canonical(image_id: str,
                      profile: models.RegistryProfile) -> state.LocationRecord:
    return state.ensure_location(
        image_id,
        profile.name,
        profile.canonical.name,
        profile.physical_fingerprint(profile.canonical),
        _DIGEST,
        policy_fingerprint=profile.policy_fingerprint(profile.canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical=True)


def _publish(source_ref: str, digest: str,
             profile: models.RegistryProfile) -> state.ImageRecord:
    canonical = profile.canonical
    return state.publish_image(
        source_ref=source_ref,
        resolved_source_ref=source_ref,
        source_digest=digest,
        workspace='research',
        creator_user_hash='user-1',
        release='boltz-production',
        profile=profile.name,
        target_id=canonical.name,
        target_fingerprint=profile.physical_fingerprint(canonical),
        policy_fingerprint=profile.policy_fingerprint(canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
    )


@pytest.fixture(scope='module')
def postgres_engine():
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    try:
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        pytest.skip(f'could not start postgres container: {e}')
    engine = sqlalchemy.create_engine(
        container.get_connection_url(),
        connect_args={'options': '-c statement_timeout=10000'})
    global_user_state.Base.metadata.create_all(
        engine,
        tables=[
            global_user_state.cluster_table,
            global_user_state.cluster_history_table,
        ])
    global_user_state.container_image_metadata.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


@pytest.fixture
def image_pg_engine(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                'TRUNCATE container_image_references, '
                'container_image_locations, '
                'container_image_profile_revisions, '
                'container_image_releases, container_image_sources, '
                'container_image_workspace_catalogs, container_image_catalog, '
                'container_images, clusters, cluster_history CASCADE'))
    monkeypatch.setattr(state, '_engine', lambda: postgres_engine)
    monkeypatch.setattr(global_user_state._db_manager, '_engine',
                        postgres_engine)
    return postgres_engine


def test_schema_023_upgrades_and_repairs_postgresql(postgres_engine):
    schema_name = f'container_image_migration_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.execute(sqlalchemy.text(f'CREATE SCHEMA {schema_name}'))
    migration_engine = sqlalchemy.create_engine(
        postgres_engine.url,
        connect_args={
            'options': (f'-c search_path={schema_name} '
                        '-c statement_timeout=10000')
        })
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'clusters', old_metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True))
    old_metadata.create_all(migration_engine)
    schema_023 = importlib.import_module(
        'sky.schemas.db.global_user_state.023_container_images')

    def _upgrade() -> None:
        with migration_engine.connect() as connection:
            context = migration.MigrationContext.configure(connection)
            with operations.Operations.context(context):
                schema_023.upgrade()

    try:
        _upgrade()
        inspector = sqlalchemy.inspect(migration_engine)
        assert {
            'container_images', 'container_image_sources',
            'container_image_releases', 'container_image_profile_revisions',
            'container_image_locations', 'container_image_references',
            'container_image_workspace_catalogs'
        } <= set(inspector.get_table_names())
        constraints = {
            constraint['name'] for constraint in
            inspector.get_check_constraints('container_image_locations')
        }
        assert 'ck_container_image_location_complete_lease' in constraints

        missing_index = ('ix_container_image_locations_regional_pending_queue')
        with migration_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP INDEX {missing_index}')
        _upgrade()
        assert missing_index in {
            index['name'] for index in sqlalchemy.inspect(
                migration_engine).get_indexes('container_image_locations')
        }
    finally:
        migration_engine.dispose()
        with postgres_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text(f'DROP SCHEMA {schema_name} CASCADE'))


def _register_revision_one(
) -> tuple[state.ImageRecord, state.LocationRecord, models.RegistryProfile]:
    profile = _profile(1)
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    return image, _ensure_canonical(image.id, profile), profile


@pytest.mark.parametrize('compressed_size_bytes', [2**31, (1 << 63) - 1])
def test_artifact_size_uses_postgresql_bigint(image_pg_engine,
                                              compressed_size_bytes):
    image, canonical, profile = _register_revision_one()
    claim = state.claim_location(canonical.id, 'import-worker', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    target_ref = references.managed_reference(profile, profile.canonical,
                                              'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id,
                              claim.lease_owner,
                              target_ref,
                              _DIGEST,
                              platforms=('linux/amd64',),
                              compressed_size_bytes=compressed_size_bytes)
    refreshed = state.get_image(image.id, 'research')
    assert refreshed is not None
    assert refreshed.compressed_size_bytes == compressed_size_bytes
    columns = {
        column['name']: column for column in sqlalchemy.inspect(
            image_pg_engine).get_columns('container_images')
    }
    assert isinstance(columns['compressed_size_bytes']['type'],
                      sqlalchemy.BigInteger)


def test_concurrent_canonical_completions_preserve_first_artifact_evidence(
        image_pg_engine):
    """The artifact row lock makes OCI evidence immutable across profiles."""
    del image_pg_engine
    image, first_location, first_profile = _register_revision_one()
    second_profile = dataclasses.replace(first_profile,
                                         name='managed-secondary',
                                         canonical=dataclasses.replace(
                                             first_profile.canonical,
                                             name='canonical-secondary',
                                             region='us-east-2'),
                                         targets=())
    second_location = _ensure_canonical(image.id, second_profile)

    claims = [
        state.claim_location(first_location.id, 'first-importer', 30),
        state.claim_location(second_location.id, 'second-importer', 30),
    ]
    assert all(
        claim is not None and claim.lease_owner is not None for claim in claims)
    evidence = [
        (first_location, first_profile, ('linux/amd64',), 101),
        (second_location, second_profile, ('linux/arm64',), 202),
    ]
    start = threading.Barrier(2)
    outcomes = []
    errors = []
    result_lock = threading.Lock()

    def _complete(index: int) -> None:
        location, profile, platforms, compressed_size = evidence[index]
        claim = claims[index]
        assert claim is not None
        assert claim.lease_owner is not None
        target_ref = references.managed_reference(profile, profile.canonical,
                                                  'research', _SOURCE, _DIGEST)
        try:
            start.wait(timeout=10)
            result = state.complete_location(
                location.id,
                claim.lease_owner,
                target_ref,
                _DIGEST,
                platforms=platforms,
                compressed_size_bytes=compressed_size)
            with result_lock:
                outcomes.append((index, result))
        except Exception as error:  # pylint: disable=broad-except
            with result_lock:
                errors.append(error)

    threads = [
        threading.Thread(target=_complete, args=(index,)) for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert sorted(result for _, result in outcomes) == [False, True]
    winner_index = next(index for index, result in outcomes if result)
    winner_platforms = evidence[winner_index][2]
    winner_size = evidence[winner_index][3]
    refreshed_image = state.get_image(image.id, 'research')
    assert refreshed_image is not None
    assert set(refreshed_image.platforms) == set(winner_platforms)
    assert refreshed_image.compressed_size_bytes == winner_size

    for index, (location, _, _, _) in enumerate(evidence):
        refreshed_location = state.get_location_by_id(location.id)
        assert refreshed_location is not None
        if index == winner_index:
            assert refreshed_location.state == models.ImageLocationState.READY
            assert refreshed_location.last_error is None
        else:
            assert refreshed_location.state == models.ImageLocationState.FAILED
            assert refreshed_location.last_error == (
                models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value)


def test_concurrent_identical_publish_converges(image_pg_engine):
    del image_pg_engine
    profile = _profile(1)
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def _run():
        try:
            barrier.wait(timeout=10)
            results.append(_publish(_SOURCE, _DIGEST, profile))
        except Exception as error:  # pylint: disable=broad-except
            errors.append(error)

    threads = [threading.Thread(target=_run) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert len(results) == 2
    assert results[0].id == results[1].id
    assert len(state.list_images('research')) == 1
    assert len(state.list_sources(results[0].id, 'research')) == 1
    assert len(state.list_releases(results[0].id, 'research')) == 1
    assert len(state.list_locations(results[0].id, profile.name)) == 1


def test_canonical_source_rotation_is_lease_fenced(image_pg_engine,
                                                   monkeypatch):
    del image_pg_engine
    profile = _profile(1)
    mirror = f'quay.io/boltz-bio/boltz-mirror@{_DIGEST}'
    clock = [100]
    monkeypatch.setattr(state.time, 'time', lambda: clock[0])
    image = _publish(_SOURCE, _DIGEST, profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    claim = state.claim_location(canonical.id, 'source-worker', 10)
    assert claim is not None
    assert claim.lease_owner is not None

    with pytest.raises(state.ProfileRevisionBusyError, match='source binding'):
        _publish(mirror, _DIGEST, profile)
    assert state.get_source(mirror, 'research') is None

    clock[0] = 110
    assert _publish(mirror, _DIGEST, profile).id == image.id
    mirror_source = state.get_source(mirror, 'research')
    assert mirror_source is not None
    rotated = state.get_location_by_id(canonical.id)
    assert rotated is not None
    assert rotated.source_id == mirror_source.id
    assert rotated.state == models.ImageLocationState.PENDING
    stale_reference = references.managed_reference(profile, profile.canonical,
                                                   'research', _SOURCE, _DIGEST)
    assert not _complete_location(canonical.id, claim.lease_owner,
                                  stale_reference, _DIGEST)


def test_concurrent_release_conflict_rolls_back_loser(image_pg_engine):
    del image_pg_engine
    profile = _profile(1)
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def _run(source_ref: str, digest: str):
        try:
            barrier.wait(timeout=10)
            results.append(_publish(source_ref, digest, profile))
        except Exception as error:  # pylint: disable=broad-except
            errors.append(error)

    threads = [
        threading.Thread(target=_run, args=(_SOURCE, _DIGEST)),
        threading.Thread(target=_run, args=(_OTHER_SOURCE, _OTHER_DIGEST)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert all(not thread.is_alive() for thread in threads)
    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    winner = results[0]
    loser_digest = (_OTHER_DIGEST
                    if winner.source_digest == _DIGEST else _DIGEST)
    loser_source = (_OTHER_SOURCE if loser_digest == _OTHER_DIGEST else _SOURCE)
    assert state.get_image_by_digest(loser_digest, 'research') is None
    assert state.get_image_by_source_ref(loser_source, 'research') is None
    release = state.get_release('boltz-production', 'research')
    assert release is not None
    assert release.image_id == winner.id
    assert len(state.list_locations(winner.id, profile.name)) == 1


def test_postgresql_atomic_publication_batch_rolls_back_all_candidates(
        image_pg_engine):
    profile = _profile(1)

    def publication(source_ref: str, digest: str) -> state.ImagePublication:
        canonical = profile.canonical
        return state.ImagePublication(
            source_ref=source_ref,
            resolved_source_ref=source_ref,
            source_digest=digest,
            workspace='research',
            creator_user_hash='user-1',
            release='batch-conflict',
            profile=profile.name,
            target_id=canonical.name,
            target_fingerprint=profile.physical_fingerprint(canonical),
            policy_fingerprint=profile.policy_fingerprint(canonical, True),
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
        )

    with pytest.raises(ValueError, match='already bound'):
        state.publish_images_atomically([
            publication(_SOURCE, _DIGEST),
            publication(_OTHER_SOURCE, _OTHER_DIGEST),
        ])

    assert state.list_images('research') == []
    assert state.get_release('batch-conflict', 'research') is None
    assert state.get_profile_revision('research', profile.name) is None
    tables = (
        global_user_state.container_image_source_table,
        global_user_state.container_image_location_table,
    )
    with sqlalchemy.orm.Session(image_pg_engine) as session:
        assert all(
            session.execute(table.select().limit(1)).first() is None
            for table in tables)


def test_crossed_release_batches_follow_global_lock_order(image_pg_engine):
    """Crossed release sets serialize instead of deadlocking PostgreSQL."""
    profile = _profile(1)
    with sqlalchemy.orm.Session(image_pg_engine) as session:
        state._lock_profile_revision(session, 'research', profile.name,
                                     profile.revision,
                                     profile.revision_fingerprint, 1)
        session.commit()

    source_a1 = f'a.example.com/repo-1@{_DIGEST}'
    source_a2 = f'a.example.com/repo-2@{_DIGEST}'
    source_b1 = f'b.example.com/repo-1@{_OTHER_DIGEST}'
    source_b2 = f'b.example.com/repo-2@{_OTHER_DIGEST}'

    def publication(source_ref: str, digest: str,
                    release: str) -> state.ImagePublication:
        canonical = profile.canonical
        return state.ImagePublication(
            source_ref=source_ref,
            resolved_source_ref=source_ref,
            source_digest=digest,
            workspace='research',
            creator_user_hash='user-1',
            release=release,
            profile=profile.name,
            target_id=canonical.name,
            target_fingerprint=profile.physical_fingerprint(canonical),
            policy_fingerprint=profile.policy_fingerprint(canonical, True),
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
        )

    batches = {
        'a': [
            publication(source_a1, _DIGEST, 'release-x'),
            publication(source_a2, _DIGEST, 'release-y'),
        ],
        'b': [
            publication(source_b1, _OTHER_DIGEST, 'release-y'),
            publication(source_b2, _OTHER_DIGEST, 'release-x'),
        ],
    }
    start_barrier = threading.Barrier(2)
    results = []
    errors = []

    def _run(name: str):
        try:
            start_barrier.wait(timeout=10)
            results.append(
                (name, state.publish_images_atomically(batches[name])))
        except Exception as error:  # pylint: disable=broad-except
            errors.append((name, error))

    threads = [
        threading.Thread(target=_run, args=('a',)),
        threading.Thread(target=_run, args=('b',)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert all(not thread.is_alive() for thread in threads)
    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0][1], ValueError)
    assert not isinstance(errors[0][1], sqlalchemy.exc.OperationalError)

    winner_name, winner_records = results[0]
    assert len(winner_records) == 2
    assert winner_records[0].id == winner_records[1].id
    winner = winner_records[0]
    loser_name = 'b' if winner_name == 'a' else 'a'
    loser_digest = _OTHER_DIGEST if loser_name == 'b' else _DIGEST
    for loser_publication in batches[loser_name]:
        assert state.get_image_by_source_ref(loser_publication.source_ref,
                                             'research') is None
    assert state.get_image_by_digest(loser_digest, 'research') is None
    assert len(state.list_sources(winner.id, 'research')) == 2
    assert {
        release.name for release in state.list_releases(winner.id, 'research')
    } == {'release-x', 'release-y'}
    assert len(state.list_locations(winner.id, profile.name)) == 1


def test_republish_and_canonical_completion_share_artifact_first_lock_order(
        image_pg_engine, monkeypatch):
    """Republish cannot deadlock canonical READY metadata publication."""
    engine = image_pg_engine
    profile = _profile(1)
    image = _publish(_SOURCE, _DIGEST, profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    claim = state.claim_location(canonical.id, 'canonical-worker', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    target_ref = references.managed_reference(profile, profile.canonical,
                                              'research', _SOURCE, _DIGEST)

    completion_has_artifact = threading.Event()
    release_completion = threading.Event()
    publisher_attempted_artifact = threading.Event()
    publisher_reached_location = threading.Event()
    completion_results = []
    publish_results = []
    errors = []
    original_image_lock = state._lock_image_for_update
    original_ensure_location = state._ensure_location_in_session

    def _pause_completion(session, image_id):
        locked = original_image_lock(session, image_id)
        if threading.current_thread().name == 'canonical-completion':
            completion_has_artifact.set()
            if not release_completion.wait(timeout=10):
                raise RuntimeError('test timed out releasing completion')
        return locked

    def _observe_publisher_location(*args, **kwargs):
        if threading.current_thread().name == 'image-republish':
            publisher_reached_location.set()
        return original_ensure_location(*args, **kwargs)

    def _observe_artifact_lock(_, __, statement, *___):
        normalized = ' '.join(statement.lower().split())
        if (threading.current_thread().name == 'image-republish' and
                'from container_images' in normalized and
                'for update' in normalized):
            publisher_attempted_artifact.set()

    monkeypatch.setattr(state, '_lock_image_for_update', _pause_completion)
    monkeypatch.setattr(state, '_ensure_location_in_session',
                        _observe_publisher_location)
    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _observe_artifact_lock)

    def _complete():
        try:
            completion_results.append(
                _complete_location(canonical.id, claim.lease_owner, target_ref,
                                   _DIGEST))
        except Exception as error:  # pylint: disable=broad-except
            errors.append(error)

    def _republish():
        try:
            publish_results.append(_publish(_SOURCE, _DIGEST, profile))
        except Exception as error:  # pylint: disable=broad-except
            errors.append(error)

    completion_thread = threading.Thread(target=_complete,
                                         name='canonical-completion')
    publish_thread = threading.Thread(target=_republish, name='image-republish')
    try:
        completion_thread.start()
        assert completion_has_artifact.wait(timeout=10)
        publish_thread.start()
        assert publisher_attempted_artifact.wait(timeout=10)
        assert not publisher_reached_location.wait(timeout=0.3)
    finally:
        release_completion.set()
        completion_thread.join(timeout=10)
        publish_thread.join(timeout=10)
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _observe_artifact_lock)

    assert not completion_thread.is_alive()
    assert not publish_thread.is_alive()
    assert not errors
    assert completion_results == [True]
    assert len(publish_results) == 1
    assert publish_results[0].id == image.id
    assert publisher_reached_location.is_set()


def _ready_regional() -> tuple[state.LocationRecord, state.LocationRecord]:
    image, canonical, profile = _register_revision_one()
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    target = profile.targets[0]
    regional = state.ensure_location(
        image.id,
        profile.name,
        target.name,
        profile.physical_fingerprint(target),
        _DIGEST,
        policy_fingerprint=profile.policy_fingerprint(target, False),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical_location_id=canonical.id,
        auto_evict=True)
    regional_claim = state.claim_location(regional.id, 'copier', 30)
    assert regional_claim is not None
    assert regional_claim.lease_owner is not None
    regional_ref = references.managed_reference(profile, target, 'research',
                                                _SOURCE, _DIGEST)
    assert _complete_location(regional.id, regional_claim.lease_owner,
                              regional_ref, _DIGEST)
    refreshed = state.get_location_by_id(regional.id)
    assert refreshed is not None
    return canonical, refreshed


def test_postgres_indexed_queue_reclaims_copy_with_absent_lease(
        image_pg_engine, monkeypatch):
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    _, canonical, _ = _register_revision_one()
    table = global_user_state.container_image_location_table
    with image_pg_engine.begin() as connection:
        connection.execute(
            table.update().where(table.c.id == canonical.id).values(
                state=models.ImageLocationState.COPYING.value,
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=None,
                updated_at=1000))

    claimed = state.claim_next_reconciliation_candidate(
        'research',
        'postgres-repair-copy',
        materialization_lease_seconds=30,
        verification_lease_seconds=30,
        now=1000)

    assert claimed is not None
    assert claimed.id == canonical.id
    assert claimed.lease_owner is not None
    assert claimed.attempt_count == 1


def test_postgres_indexed_queue_reclaims_eviction_with_absent_lease(
        image_pg_engine, monkeypatch):
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    _, regional = _ready_regional()
    table = global_user_state.container_image_location_table
    with image_pg_engine.begin() as connection:
        connection.execute(
            table.update().where(table.c.id == regional.id).values(
                state=models.ImageLocationState.EVICTING.value,
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=None,
                updated_at=2000))

    claimed = state.claim_next_eviction_candidate('research',
                                                  'postgres-repair-eviction',
                                                  lease_seconds=30,
                                                  unused_before=1500,
                                                  now=2000)

    assert claimed is not None
    assert claimed.id == regional.id
    assert claimed.lease_owner is not None
    assert claimed.attempt_count == 1


@pytest.mark.parametrize('transition', ['copy', 'evict'])
def test_activation_settles_expired_transition_after_canonical_loss(
        image_pg_engine, monkeypatch, transition):
    """Repairing a profile never depends on its lost canonical manifest."""
    del image_pg_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile(1)
    if transition == 'copy':
        image, canonical, _ = _register_revision_one()
        canonical_claim = state.claim_location(canonical.id, 'importer', 30)
        assert canonical_claim is not None
        assert canonical_claim.lease_owner is not None
        canonical_ref = references.managed_reference(profile, profile.canonical,
                                                     'research', _SOURCE,
                                                     _DIGEST)
        assert _complete_location(canonical.id, canonical_claim.lease_owner,
                                  canonical_ref, _DIGEST)
        target = profile.targets[0]
        regional = state.ensure_location(
            image.id,
            profile.name,
            target.name,
            profile.physical_fingerprint(target),
            _DIGEST,
            policy_fingerprint=profile.policy_fingerprint(target, False),
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
            canonical_location_id=canonical.id,
            auto_evict=True)
        transition_claim = state.claim_location(regional.id, 'copier', 30)
        expected_state = models.ImageLocationState.FAILED
    else:
        canonical, regional = _ready_regional()
        image = state.get_image(regional.image_id, 'research')
        assert image is not None
        now[0] = 2000
        transition_claim = state.claim_location_eviction(regional.id,
                                                         'evictor',
                                                         30,
                                                         unused_before=1500)
        target = profile.targets[0]
        expected_state = models.ImageLocationState.MISSING
    assert transition_claim is not None
    assert transition_claim.lease_owner is not None
    stale_token = transition_claim.lease_owner

    assert state.retry_location(canonical.id)
    verification = state.claim_location_verification(canonical.id, 'verifier',
                                                     30)
    assert verification is not None
    assert verification.lease_owner is not None
    assert not state.complete_location_verification(
        canonical.id, verification.lease_owner, _OTHER_DIGEST)
    now[0] += 31

    revision_two = dataclasses.replace(profile, revision=2)
    advanced = _ensure_canonical(image.id, revision_two)
    transferred = state.ensure_location(
        image.id,
        revision_two.name,
        target.name,
        revision_two.physical_fingerprint(target),
        _DIGEST,
        policy_fingerprint=revision_two.policy_fingerprint(target, False),
        profile_revision=revision_two.revision,
        profile_revision_fingerprint=revision_two.revision_fingerprint,
        canonical_location_id=advanced.id,
        auto_evict=True)
    assert transferred.profile_revision == 2
    assert transferred.state == expected_state
    assert transferred.target_ref is None
    assert transferred.lease_owner is None
    assert transferred.next_retry_at == now[0]
    current = state.get_profile_revision('research', profile.name)
    assert current is not None
    assert current.revision == 2
    if transition == 'copy':
        assert not _complete_location(regional.id, stale_token,
                                      f'registry.example/repo@{_DIGEST}',
                                      _DIGEST)
    else:
        assert not state.complete_location_eviction(regional.id, stale_token)


def test_activation_fences_exact_expiry_and_rejects_malformed_copy(
        image_pg_engine, monkeypatch):
    """Old verification tokens cannot act after same-second activation."""
    engine = image_pg_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image, canonical, revision_one = _register_revision_one()
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(revision_one,
                                                 revision_one.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    target = revision_one.targets[0]
    regional = state.ensure_location(
        image.id,
        revision_one.name,
        target.name,
        revision_one.physical_fingerprint(target),
        _DIGEST,
        policy_fingerprint=revision_one.policy_fingerprint(target, False),
        profile_revision=revision_one.revision,
        profile_revision_fingerprint=revision_one.revision_fingerprint,
        canonical_location_id=canonical.id,
        auto_evict=True)
    inactive_target = models.RegistryTarget(name='inactive-cache',
                                            provider='aws',
                                            account='123456789012',
                                            region='us-east-2')
    inactive_failed = state.ensure_location(
        image.id,
        revision_one.name,
        inactive_target.name,
        revision_one.physical_fingerprint(inactive_target),
        _DIGEST,
        policy_fingerprint=revision_one.policy_fingerprint(
            inactive_target, False),
        profile_revision=revision_one.revision,
        profile_revision_fingerprint=revision_one.revision_fingerprint,
        canonical_location_id=canonical.id,
        auto_evict=True)
    assert state.retry_location(canonical.id)
    old_verification = state.claim_location_verification(
        canonical.id, 'revision-one-verifier', 30)
    assert old_verification is not None
    assert old_verification.lease_owner is not None
    table = global_user_state.container_image_location_table
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                table.update().where(table.c.id == regional.id).values(
                    state=models.ImageLocationState.COPYING.value,
                    lease_owner=None,
                    lease_expires_at=9999,
                    heartbeat_at=1000))
    with engine.begin() as connection:
        connection.execute(
            table.update().where(table.c.id == regional.id).values(
                state=models.ImageLocationState.COPYING.value,
                lease_owner='expired-copy-token',
                lease_expires_at=1029,
                heartbeat_at=1000))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                table.update().where(table.c.id == inactive_failed.id).values(
                    state=models.ImageLocationState.FAILED.value,
                    lease_owner='impossible-future-token',
                    lease_expires_at=9999,
                    heartbeat_at=1000))

    now[0] = 1030
    revision_two = dataclasses.replace(revision_one, revision=2)
    advanced = _ensure_canonical(image.id, revision_two)
    transferred = state.ensure_location(
        image.id,
        revision_two.name,
        target.name,
        revision_two.physical_fingerprint(target),
        _DIGEST,
        policy_fingerprint=revision_two.policy_fingerprint(target, False),
        profile_revision=revision_two.revision,
        profile_revision_fingerprint=revision_two.revision_fingerprint,
        canonical_location_id=advanced.id,
        auto_evict=True)
    assert advanced.profile_revision == 2
    assert advanced.state == models.ImageLocationState.READY
    assert advanced.lease_owner is None
    assert transferred.profile_revision == 2
    assert transferred.state == models.ImageLocationState.FAILED
    assert transferred.lease_owner is None
    fenced_inactive = state.get_location_by_id(inactive_failed.id)
    assert fenced_inactive is not None
    assert fenced_inactive.state == models.ImageLocationState.PENDING
    assert fenced_inactive.profile_revision == 1
    assert fenced_inactive.lease_owner is None
    assert state.claim_location(inactive_failed.id, 'stale-worker', 30) is None
    assert not state.complete_location_verification(
        canonical.id, old_verification.lease_owner, _OTHER_DIGEST)
    unchanged = state.get_location_by_id(canonical.id)
    assert unchanged is not None
    assert unchanged.state == models.ImageLocationState.READY
    new_verification = state.claim_location_verification(
        canonical.id, 'revision-two-verifier', 30)
    assert new_verification is not None
    assert new_verification.lease_owner is not None
    assert state.complete_location_verification(canonical.id,
                                                new_verification.lease_owner,
                                                _DIGEST)


def test_claim_before_activation_makes_activation_fail_busy(
        image_pg_engine, monkeypatch):
    """A committed claim is visible after activation waits on its shared lock."""
    del image_pg_engine
    image, canonical, revision_one = _register_revision_one()
    revision_two = dataclasses.replace(revision_one, revision=2)
    work_locked = threading.Event()
    release_work = threading.Event()
    activation_done = threading.Event()
    claim_result = []
    activation_result = []
    errors = []
    original_lock = (
        global_user_state.lock_container_image_profile_revision_for_work)

    def _pause_claim(session, workspace, profile, revision):
        locked = original_lock(session, workspace, profile, revision)
        if threading.current_thread().name == 'image-claim':
            work_locked.set()
            if not release_work.wait(timeout=10):
                raise RuntimeError('test timed out releasing image claim')
        return locked

    monkeypatch.setattr(global_user_state,
                        'lock_container_image_profile_revision_for_work',
                        _pause_claim)

    def _claim():
        try:
            claim_result.append(
                state.claim_location(canonical.id, 'revision-one-worker', 30))
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    def _activate():
        try:
            _ensure_canonical(image.id, revision_two)
            activation_result.append('advanced')
        except state.ProfileRevisionBusyError:
            activation_result.append('busy')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            activation_done.set()

    claim_thread = threading.Thread(target=_claim, name='image-claim')
    claim_thread.start()
    assert work_locked.wait(timeout=10)
    activation_thread = threading.Thread(target=_activate)
    activation_thread.start()
    try:
        assert not activation_done.wait(timeout=0.3)
    finally:
        release_work.set()
    claim_thread.join(timeout=10)
    activation_thread.join(timeout=10)

    assert not claim_thread.is_alive()
    assert not activation_thread.is_alive()
    assert not errors
    assert claim_result[0] is not None
    assert activation_result == ['busy']
    assert state.get_profile_revision('research', 'managed').revision == 1


def test_activation_before_claim_makes_stale_claim_fail(image_pg_engine,
                                                        monkeypatch):
    """A claim waits for activation and then rejects the superseded revision."""
    del image_pg_engine
    image, canonical, revision_one = _register_revision_one()
    revision_two = dataclasses.replace(revision_one, revision=2)
    activation_locked = threading.Event()
    release_activation = threading.Event()
    claim_done = threading.Event()
    claim_result = []
    activation_result = []
    errors = []
    original_activation = state._lock_profile_revision

    def _pause_activation(session, workspace, profile, revision,
                          revision_fingerprint, now):
        original_activation(session, workspace, profile, revision,
                            revision_fingerprint, now)
        if threading.current_thread().name == 'profile-activation':
            activation_locked.set()
            if not release_activation.wait(timeout=10):
                raise RuntimeError(
                    'test timed out releasing profile activation')

    monkeypatch.setattr(state, '_lock_profile_revision', _pause_activation)

    def _activate():
        try:
            activation_result.append(_ensure_canonical(image.id, revision_two))
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)

    def _claim():
        try:
            claim_result.append(
                state.claim_location(canonical.id, 'stale-worker', 30))
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            claim_done.set()

    activation_thread = threading.Thread(target=_activate,
                                         name='profile-activation')
    activation_thread.start()
    assert activation_locked.wait(timeout=10)
    claim_thread = threading.Thread(target=_claim)
    claim_thread.start()
    try:
        assert not claim_done.wait(timeout=0.3)
    finally:
        release_activation.set()
    activation_thread.join(timeout=10)
    claim_thread.join(timeout=10)

    assert not activation_thread.is_alive()
    assert not claim_thread.is_alive()
    assert not errors
    assert len(activation_result) == 1
    assert activation_result[0].profile_revision == 2
    assert claim_result == [None]
    assert state.get_profile_revision('research', 'managed').revision == 2


def test_canonical_loss_before_reference_statement_rejects_new_reference(
        image_pg_engine, monkeypatch):
    """The final reference statement observes canonical loss after routing."""
    engine = image_pg_engine
    canonical, regional = _ready_regional()
    reference_locked = threading.Event()
    release_reference = threading.Event()
    reference_done = threading.Event()
    outcome = []
    errors = []
    original_lock = state._lock_location_profile_revision

    def _pause_reference(session, location_id):
        locked = original_lock(session, location_id)
        if threading.current_thread().name == 'reference-acquisition':
            reference_locked.set()
            if not release_reference.wait(timeout=10):
                raise RuntimeError('test timed out releasing reference')
        return locked

    monkeypatch.setattr(state, '_lock_location_profile_revision',
                        _pause_reference)

    def _acquire_reference():
        try:
            state.acquire_reference(regional.id,
                                    'research',
                                    'service',
                                    'stale-route',
                                    expected_ref=regional.target_ref)
            outcome.append('acquired')
        except ValueError:
            outcome.append('rejected')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            reference_done.set()

    reference_thread = threading.Thread(target=_acquire_reference,
                                        name='reference-acquisition')
    reference_thread.start()
    assert reference_locked.wait(timeout=10)
    assert state.mark_location_missing(canonical.id)
    release_reference.set()
    assert reference_done.wait(timeout=10)
    reference_thread.join(timeout=10)

    assert not reference_thread.is_alive()
    assert not errors
    assert outcome == ['rejected']
    reference_table = global_user_state.container_image_reference_table
    with sqlalchemy.orm.Session(engine) as session:
        assert session.execute(reference_table.select()).first() is None


def test_waiting_reference_serializes_before_canonical_loss(
        image_pg_engine, monkeypatch):
    """A canonical share lock protects a reference waiting on its region."""
    engine = image_pg_engine
    canonical, regional = _ready_regional()
    assert state.retry_location(canonical.id)
    verification = state.claim_location_verification(canonical.id, 'verifier',
                                                     30)
    assert verification is not None
    assert verification.lease_owner is not None

    canonical_locked = threading.Event()
    regional_locked = threading.Event()
    release_regional = threading.Event()
    reference_done = threading.Event()
    loss_done = threading.Event()
    reference_outcome = []
    loss_observations = []
    errors = []
    original_lock = state._lock_exact_canonical_ready

    def _observe_canonical_lock(session, location_id):
        locked = original_lock(session, location_id)
        if (locked and
                threading.current_thread().name == 'reference-acquisition'):
            canonical_locked.set()
        return locked

    monkeypatch.setattr(state, '_lock_exact_canonical_ready',
                        _observe_canonical_lock)

    def _hold_regional():
        try:
            table = global_user_state.container_image_location_table
            with sqlalchemy.orm.Session(engine) as session:
                session.execute(
                    sqlalchemy.select(table.c.id).where(
                        table.c.id == regional.id).with_for_update()).one()
                regional_locked.set()
                if not release_regional.wait(timeout=10):
                    raise RuntimeError('test timed out releasing regional row')
                session.commit()
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
            regional_locked.set()

    def _acquire_reference():
        try:
            state.acquire_reference(regional.id,
                                    'research',
                                    'service',
                                    'overlapping-reference',
                                    expected_ref=regional.target_ref)
            reference_outcome.append('acquired')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            reference_done.set()

    def _lose_canonical():
        try:
            assert not state.complete_location_verification(
                canonical.id, verification.lease_owner, _OTHER_DIGEST)
            reference_table = (
                global_user_state.container_image_reference_table)
            with sqlalchemy.orm.Session(engine) as session:
                loss_observations.append(
                    session.execute(reference_table.select().where(
                        reference_table.c.consumer_id ==
                        'overlapping-reference')).first() is not None)
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            loss_done.set()

    holder = threading.Thread(target=_hold_regional, name='regional-holder')
    holder.start()
    assert regional_locked.wait(timeout=10)
    reference_thread = threading.Thread(target=_acquire_reference,
                                        name='reference-acquisition')
    reference_thread.start()
    assert canonical_locked.wait(timeout=10)
    loss_thread = threading.Thread(target=_lose_canonical,
                                   name='canonical-loss')
    loss_thread.start()
    assert not loss_done.wait(timeout=0.3)
    release_regional.set()

    holder.join(timeout=10)
    reference_thread.join(timeout=10)
    loss_thread.join(timeout=10)
    assert not holder.is_alive()
    assert not reference_thread.is_alive()
    assert not loss_thread.is_alive()
    assert reference_done.is_set()
    assert loss_done.is_set()
    assert not errors
    assert reference_outcome == ['acquired']
    assert loss_observations == [True]
    assert state.get_location_by_id(
        canonical.id).state == models.ImageLocationState.MISSING


def test_waiting_cluster_commit_serializes_before_canonical_loss(
        image_pg_engine, monkeypatch):
    """The cluster row and its reference commit under a canonical lock."""
    _mock_registry_profile(monkeypatch)
    engine = image_pg_engine
    canonical, regional = _ready_regional()
    assert state.retry_location(canonical.id)
    verification = state.claim_location_verification(canonical.id, 'verifier',
                                                     30)
    assert verification is not None
    assert verification.lease_owner is not None
    assert regional.target_ref is not None
    resolved = models.ResolvedContainerImage(
        image_id=regional.image_id,
        location_id=regional.id,
        reference=regional.target_ref,
        target_id=regional.target_id,
        distribution=regional.profile,
        profile_revision=regional.profile_revision,
        policy_fingerprint=regional.policy_fingerprint,
        digest=_DIGEST,
        auth_strategy='ecr_runtime_identity')
    launched = resources_lib.Resources(  # pylint: disable=unexpected-keyword-arg
        cloud='aws',
        region='us-west-2',
        container_image=_SOURCE,
        _resolved_container_image=resolved,
        _docker_login_config=docker_utils.DockerLoginConfig(
            username='',
            password='',
            server=regional.target_ref.split('/', 1)[0]))
    handle = types.SimpleNamespace(launched_resources=launched,
                                   launched_nodes=1)

    canonical_locked = threading.Event()
    regional_locked = threading.Event()
    release_regional = threading.Event()
    cluster_done = threading.Event()
    loss_done = threading.Event()
    cluster_outcome = []
    loss_observations = []
    errors = []
    original_lock = (
        global_user_state.lock_container_image_exact_canonical_for_work)

    def _observe_canonical_lock(session, location_id):
        locked = original_lock(session, location_id)
        if (locked and threading.current_thread().name == 'cluster-commit'):
            canonical_locked.set()
        return locked

    monkeypatch.setattr(global_user_state,
                        'lock_container_image_exact_canonical_for_work',
                        _observe_canonical_lock)

    def _hold_regional():
        try:
            table = global_user_state.container_image_location_table
            with sqlalchemy.orm.Session(engine) as session:
                session.execute(
                    sqlalchemy.select(table.c.id).where(
                        table.c.id == regional.id).with_for_update()).one()
                regional_locked.set()
                if not release_regional.wait(timeout=10):
                    raise RuntimeError('test timed out releasing regional row')
                session.commit()
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
            regional_locked.set()

    def _commit_cluster():
        try:
            global_user_state.add_or_update_cluster('overlapping-cluster',
                                                    handle,
                                                    requested_resources=None,
                                                    ready=False)
            cluster_outcome.append('committed')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            cluster_done.set()

    def _lose_canonical():
        try:
            assert not state.complete_location_verification(
                canonical.id, verification.lease_owner, _OTHER_DIGEST)
            reference_table = (
                global_user_state.container_image_reference_table)
            with sqlalchemy.orm.Session(engine) as session:
                reference_exists = session.execute(
                    reference_table.select().where(
                        reference_table.c.consumer_id ==
                        'overlapping-cluster')).first() is not None
                cluster_exists = session.execute(
                    global_user_state.cluster_table.select().where(
                        global_user_state.cluster_table.c.name ==
                        'overlapping-cluster')).first() is not None
                loss_observations.append((reference_exists, cluster_exists))
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
        finally:
            loss_done.set()

    holder = threading.Thread(target=_hold_regional, name='regional-holder')
    holder.start()
    assert regional_locked.wait(timeout=10)
    cluster_thread = threading.Thread(target=_commit_cluster,
                                      name='cluster-commit')
    cluster_thread.start()
    assert canonical_locked.wait(timeout=10)
    loss_thread = threading.Thread(target=_lose_canonical,
                                   name='canonical-loss')
    loss_thread.start()
    assert not loss_done.wait(timeout=0.3)
    release_regional.set()

    holder.join(timeout=10)
    cluster_thread.join(timeout=10)
    loss_thread.join(timeout=10)
    assert not holder.is_alive()
    assert not cluster_thread.is_alive()
    assert not loss_thread.is_alive()
    assert cluster_done.is_set()
    assert loss_done.is_set()
    assert not errors
    assert cluster_outcome == ['committed']
    assert loss_observations == [(True, True)]
    with sqlalchemy.orm.Session(engine) as session:
        reference = session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'overlapping-cluster')).mappings().one()
        assert reference['location_id'] == regional.id
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'overlapping-cluster')).first() is not None


def test_skip_locked_reselects_another_profile(image_pg_engine):
    """A busy first profile does not make a worker miss other due work."""
    engine = image_pg_engine
    first_profile = _profile(1)
    second_profile = dataclasses.replace(first_profile,
                                         name='managed-two',
                                         canonical=dataclasses.replace(
                                             first_profile.canonical,
                                             region='us-east-2'))
    first_image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                       'user-1')
    second_image = state.register_image(_OTHER_SOURCE, _OTHER_SOURCE,
                                        _OTHER_DIGEST, 'research', 'user-1')
    first_location = _ensure_canonical(first_image.id, first_profile)
    second_location = state.ensure_location(
        second_image.id,
        second_profile.name,
        second_profile.canonical.name,
        second_profile.physical_fingerprint(second_profile.canonical),
        _OTHER_DIGEST,
        policy_fingerprint=second_profile.policy_fingerprint(
            second_profile.canonical, True),
        profile_revision=second_profile.revision,
        profile_revision_fingerprint=second_profile.revision_fingerprint,
        canonical=True)
    ordered = state.list_reconciliation_candidates('research', limit=2)
    assert {candidate.id for candidate in ordered
           } == {first_location.id, second_location.id}
    busy = ordered[0]
    expected = ordered[1]

    location_table = global_user_state.container_image_location_table
    with sqlalchemy.orm.Session(engine) as lock_session:
        lock_session.execute(
            sqlalchemy.select(location_table.c.id).where(
                location_table.c.id == busy.id).with_for_update()).one()
        claimed = state.claim_next_reconciliation_candidate(
            'research',
            'parallel-worker',
            materialization_lease_seconds=30,
            verification_lease_seconds=30)
        assert claimed is not None
        assert claimed.id == expected.id
        lock_session.rollback()


def test_postgres_regional_claims_use_indexed_canonical_ready_snapshot(
        image_pg_engine, monkeypatch):
    """Regional hot queues cannot scan rows behind blocked canonicals."""
    engine = image_pg_engine
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    profile = _profile(1)
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_canonical(image.id, profile)
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    target = profile.targets[0]
    regional = state.ensure_location(
        image.id,
        profile.name,
        target.name,
        profile.physical_fingerprint(target),
        _DIGEST,
        policy_fingerprint=profile.policy_fingerprint(target, False),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical_location_id=canonical.id,
        auto_evict=True)

    statements = []

    def _capture_statement(_connection, _cursor, statement, _parameters,
                           _context, _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _capture_statement)
    try:
        claimed = state.claim_next_reconciliation_candidate('research',
                                                            'regional-worker',
                                                            30,
                                                            30,
                                                            now=1000)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _capture_statement)
    assert claimed is not None
    assert claimed.id == regional.id
    assert any(
        'CANONICAL_READY' in statement.upper() for statement in statements)
    assert not any(
        'JOIN LATERAL' in statement.upper() for statement in statements)
    assert claimed.lease_owner is not None
    regional_ref = references.managed_reference(profile, target, 'research',
                                                _SOURCE, _DIGEST)
    assert _complete_location(regional.id, claimed.lease_owner, regional_ref,
                              _DIGEST)

    statements.clear()
    sqlalchemy.event.listen(engine, 'before_cursor_execute', _capture_statement)
    try:
        evicting = state.claim_next_eviction_candidate('research',
                                                       'eviction-worker',
                                                       lease_seconds=30,
                                                       unused_before=1500,
                                                       now=2000)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _capture_statement)
    assert evicting is not None
    assert evicting.id == regional.id
    assert any(
        'CANONICAL_READY' in statement.upper() for statement in statements)
    assert not any(
        'JOIN LATERAL' in statement.upper() for statement in statements)


def test_same_profile_reconcilers_claim_in_parallel(image_pg_engine,
                                                    monkeypatch):
    """A profile-generation fence does not serialize independent rows."""
    del image_pg_engine
    profile = _profile(1)
    locations = []
    for index in range(8):
        digest = f'sha256:{index + 1:064x}'
        source = f'ghcr.io/boltz-bio/concurrent-{index}@{digest}'
        image = state.register_image(source, source, digest, 'research',
                                     'user-1')
        locations.append(
            state.ensure_location(
                image.id,
                profile.name,
                profile.canonical.name,
                profile.physical_fingerprint(profile.canonical),
                digest,
                policy_fingerprint=profile.policy_fingerprint(
                    profile.canonical, True),
                profile_revision=profile.revision,
                profile_revision_fingerprint=profile.revision_fingerprint,
                canonical=True))

    barrier = threading.Barrier(len(locations))
    profile_locked = threading.Barrier(len(locations))
    original_profile_lock = (
        global_user_state.lock_container_image_profile_revision_for_work)

    def _hold_shared_profile_lock(*args, **kwargs):
        locked = original_profile_lock(*args, **kwargs)
        if locked:
            profile_locked.wait(timeout=10)
        return locked

    monkeypatch.setattr(global_user_state,
                        'lock_container_image_profile_revision_for_work',
                        _hold_shared_profile_lock)
    claims = []
    errors = []
    result_lock = threading.Lock()

    def _claim(index: int):
        try:
            barrier.wait(timeout=10)
            claim = state.claim_next_reconciliation_candidate(
                'research', f'parallel-{index}', 30, 30)
            with result_lock:
                claims.append(claim)
        except Exception as error:  # pylint: disable=broad-except
            with result_lock:
                errors.append(error)

    threads = [
        threading.Thread(target=_claim, args=(index,))
        for index in range(len(locations))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert all(claim is not None for claim in claims)
    assert {claim.id for claim in claims if claim is not None
           } == {location.id for location in locations}


def test_same_profile_eviction_workers_seek_to_distinct_rows(
        image_pg_engine, monkeypatch):
    """Workers that route to the same first row seek forward in parallel."""
    del image_pg_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile(1)
    regional_locations = []
    for index in range(8):
        digest = f'sha256:{index + 1:064x}'
        source = f'ghcr.io/boltz-bio/eviction-{index}@{digest}'
        image = state.register_image(source, source, digest, 'research',
                                     'user-1')
        canonical = state.ensure_location(
            image.id,
            profile.name,
            profile.canonical.name,
            profile.physical_fingerprint(profile.canonical),
            digest,
            policy_fingerprint=profile.policy_fingerprint(
                profile.canonical, True),
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
            canonical=True)
        canonical_claim = state.claim_location(canonical.id, 'importer', 30)
        assert canonical_claim is not None
        assert canonical_claim.lease_owner is not None
        canonical_ref = references.managed_reference(profile, profile.canonical,
                                                     'research', source, digest)
        assert _complete_location(canonical.id, canonical_claim.lease_owner,
                                  canonical_ref, digest)
        target = profile.targets[0]
        regional = state.ensure_location(
            image.id,
            profile.name,
            target.name,
            profile.physical_fingerprint(target),
            digest,
            policy_fingerprint=profile.policy_fingerprint(target, False),
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
            canonical_location_id=canonical.id,
            auto_evict=True)
        regional_claim = state.claim_location(regional.id, 'copier', 30)
        assert regional_claim is not None
        assert regional_claim.lease_owner is not None
        regional_ref = references.managed_reference(profile, target, 'research',
                                                    source, digest)
        assert _complete_location(regional.id, regional_claim.lease_owner,
                                  regional_ref, digest)
        regional_locations.append(regional)

    now[0] = 2000
    start = threading.Barrier(len(regional_locations))
    first_canonical_fence = threading.Barrier(len(regional_locations))
    original_lock = state._lock_exact_canonical_ready
    thread_state = threading.local()

    def _align_first_candidate(session, location_id):
        locked = original_lock(session, location_id)
        if locked and not getattr(thread_state, 'aligned', False):
            thread_state.aligned = True
            first_canonical_fence.wait(timeout=10)
        return locked

    monkeypatch.setattr(state, '_lock_exact_canonical_ready',
                        _align_first_candidate)
    claims = []
    errors = []
    result_lock = threading.Lock()

    def _claim(index: int):
        try:
            start.wait(timeout=10)
            claim = state.claim_next_eviction_candidate(
                'research',
                f'evictor-{index}',
                lease_seconds=30,
                unused_before=1500,
                now=now[0],
            )
            with result_lock:
                claims.append(claim)
        except Exception as error:  # pylint: disable=broad-except
            with result_lock:
                errors.append(error)

    threads = [
        threading.Thread(target=_claim, args=(index,))
        for index in range(len(regional_locations))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert all(claim is not None for claim in claims)
    assert {claim.id for claim in claims if claim is not None
           } == {location.id for location in regional_locations}
