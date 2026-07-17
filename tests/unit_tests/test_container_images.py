"""Tests for managed container image identity and distribution state."""
# pylint: disable=protected-access,redefined-outer-name

import dataclasses
import hashlib
import importlib
import json
import pickle
import subprocess
import threading
import traceback
import types
from unittest import mock
import uuid

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql as sqlalchemy_postgresql
from sqlalchemy.pool import StaticPool

from sky import clouds
from sky import dag as dag_lib
from sky import exceptions
from sky import global_user_state
from sky import resources as resources_lib
from sky import skypilot_config
from sky import task as task_lib
from sky.client.cli import command as cli_command
from sky.container_images import config
from sky.container_images import core
from sky.container_images import models
from sky.container_images import oci
from sky.container_images import providers
from sky.container_images import references
from sky.container_images import resolver
from sky.container_images import state
from sky.container_images import task_utils as container_image_task_utils
from sky.container_images import worker
from sky.provision import docker_utils
from sky.schemas.api import responses
from sky.serve import serve_utils
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import config_utils
from sky.utils import dag_utils
from sky.utils import debug_dump_helpers
from sky.utils import schemas
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_DIGEST = 'sha256:' + 'a' * 64
_OTHER_DIGEST = 'sha256:' + 'b' * 64
_SOURCE = f'ghcr.io/boltz-bio/boltz@{_DIGEST}'
_OTHER_SOURCE = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
_POLICY_FINGERPRINT = 'f' * 64
_WEEK_SECONDS = 7 * 24 * 60 * 60
_ARTIFACT_ID = '11111111-1111-4111-8111-111111111111'
_LOCATION_ID = '22222222-2222-4222-8222-222222222222'
_CANONICAL_LOCATION_ID = '33333333-3333-4333-8333-333333333333'
_REGIONAL_LOCATION_ID = '44444444-4444-4444-8444-444444444444'
_TEST_PLATFORMS = ('linux/amd64', 'linux/arm64')


def _oci_image_manifest() -> bytes:
    return json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.manifest.v1+json',
            'config': {
                'mediaType': 'application/vnd.oci.image.config.v1+json',
                'digest': _OTHER_DIGEST,
                'size': 2,
            },
            'layers': [],
        },
        separators=(',', ':')).encode()


def _oci_config_payload(architecture: str = 'amd64',
                        variant: str | None = None) -> bytes:
    payload = {'os': 'linux', 'architecture': architecture}
    if variant is not None:
        payload['variant'] = variant
    return json.dumps(payload, separators=(',', ':')).encode()


def _complete_location(*args, **kwargs) -> bool:
    """Completes test fixtures with a concrete platform proof by default."""
    if len(args) < 5 and 'platforms' not in kwargs:
        kwargs['platforms'] = _TEST_PLATFORMS
    return state.complete_location(*args, **kwargs)


def _materialization(
    digest: str = _DIGEST,
    platforms: tuple[str, ...] = _TEST_PLATFORMS,
) -> worker.MaterializationResult:
    return worker.MaterializationResult(digest=digest, platforms=platforms)


def _profile() -> models.RegistryProfile:
    return models.RegistryProfile(
        name='managed',
        ownership=models.RegistryOwnership.MANAGED,
        realm='production',
        organization='boltz',
        namespace='skypilot/{organization}/{workspace}',
        require_digest_at_runtime=True,
        revision=1,
        canonical=models.RegistryTarget(
            name='canonical',
            provider='aws',
            account='123456789012',
            region='us-east-1',
            pull_auth='ecr_runtime_identity',
        ),
        targets=(models.RegistryTarget(
            name='aws-us-west-2',
            provider='aws',
            account='123456789012',
            region='us-west-2',
            pull_auth='ecr_runtime_identity',
        ),),
    )


def _profile_config() -> dict:
    return {
        'revision': 1,
        'ownership': 'managed',
        'realm': 'production',
        'organization': 'boltz',
        'namespace': 'skypilot/{organization}/{workspace}',
        'canonical': {
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-east-1',
            'pull_auth': 'ecr_runtime_identity',
        },
        'targets': [{
            'name': 'aws-us-west-2',
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-west-2',
            'pull_auth': 'ecr_runtime_identity',
        }],
    }


def _mock_registry_profile(monkeypatch) -> None:
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': _profile_config(),
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
        })


def _ensure_profile_location(
    image: state.ImageRecord,
    profile: models.RegistryProfile,
    target: models.RegistryTarget,
    *,
    canonical: bool = False,
    auto_evict: bool = False,
) -> state.LocationRecord:
    canonical_location_id = None
    if not canonical:
        canonical_location = state.get_location(
            image.id, profile.name, profile.canonical.name,
            profile.physical_fingerprint(profile.canonical))
        if canonical_location is not None:
            canonical_location_id = canonical_location.id
    return state.ensure_location(
        image.id,
        profile.name,
        target.name,
        profile.physical_fingerprint(target),
        image.source_digest,
        policy_fingerprint=profile.policy_fingerprint(target, canonical),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical=canonical,
        canonical_location_id=canonical_location_id,
        auto_evict=auto_evict,
    )


def _publish_state_image(
    source_ref: str,
    digest: str,
    *,
    release: str | None = 'boltz-production',
    profile: models.RegistryProfile | None = None,
    profile_revision_fingerprint: str | None = None,
) -> state.ImageRecord:
    """Publishes one source through the transaction-level state API."""
    if profile is None:
        profile = _profile()
    canonical = profile.canonical
    return state.publish_image(
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
        profile_revision_fingerprint=(profile_revision_fingerprint or
                                      profile.revision_fingerprint),
    )


def _resolved_location(
    image: state.ImageRecord,
    location: state.LocationRecord,
    reference: str,
    auth_strategy: str = 'ecr_runtime_identity',
) -> models.ResolvedContainerImage:
    return models.ResolvedContainerImage(
        image_id=image.id,
        location_id=location.id,
        reference=reference,
        target_id=location.target_id,
        distribution=location.profile,
        profile_revision=location.profile_revision,
        policy_fingerprint=location.policy_fingerprint,
        digest=location.expected_digest,
        auth_strategy=auth_strategy,
    )


def _ecr_runtime_login(reference: str) -> docker_utils.DockerLoginConfig:
    return docker_utils.DockerLoginConfig(username='',
                                          password='',
                                          server=reference.split('/', 1)[0])


def _mock_config(monkeypatch, data):
    profiles = data.get('container_registries', {}).get('profiles', {})
    for profile in profiles.values():
        profile.setdefault('revision', 1)

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


def _allow_manifest_deletion(monkeypatch):
    adapter = mock.Mock()
    monkeypatch.setattr(providers, 'get_adapter', lambda _: adapter)
    return adapter


@pytest.fixture
def image_state_engine(monkeypatch):
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()
        state._EVICTION_CANDIDATE_CURSORS.clear()
    engine = sqlalchemy.create_engine(
        'sqlite://',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool,
    )
    global_user_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(state, '_engine', lambda: engine)
    monkeypatch.setattr(global_user_state._db_manager, '_engine', engine)
    yield engine
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()
        state._EVICTION_CANDIDATE_CURSORS.clear()


def _sqlite_vm_steps(engine, operation) -> int:
    """Returns a 1000-instruction-granularity SQLite VM step count."""
    progress_calls = 0

    def progress() -> int:
        nonlocal progress_calls
        progress_calls += 1
        return 0

    with engine.connect() as connection:
        raw_connection = connection.connection.driver_connection
        raw_connection.set_progress_handler(progress, 1000)
    try:
        operation()
    finally:
        with engine.connect() as connection:
            connection.connection.driver_connection.set_progress_handler(
                None, 0)
    return progress_calls * 1000


def _sqlite_result_and_vm_steps(engine, operation):
    result = []
    steps = _sqlite_vm_steps(engine, lambda: result.append(operation()))
    return result[0], steps


def _ready_regional_location(
    source_ref: str = _SOURCE,
    digest: str = _DIGEST,
    *,
    workspace: str = 'research',
    auto_evict: bool = True,
) -> tuple[state.ImageRecord, str]:
    """Creates one canonical-ready image and verified regional copy."""
    profile = _profile()
    image = state.register_image(source_ref, source_ref, digest, workspace,
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    canonical_token = canonical_claim.lease_owner
    assert canonical_token is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 workspace, source_ref, digest)
    assert _complete_location(canonical.id, canonical_token, canonical_ref,
                              digest)
    target = profile.target('aws-us-west-2')
    location = _ensure_profile_location(image,
                                        profile,
                                        target,
                                        auto_evict=auto_evict)
    claim = state.claim_location(location.id, 'copier', 30)
    assert claim is not None
    lease_token = claim.lease_owner
    assert lease_token is not None
    target_ref = references.managed_reference(profile, target, workspace,
                                              source_ref, digest)
    assert _complete_location(location.id, lease_token, target_ref, digest)
    return image, target_ref


def _seed_referenced_eviction_queue(
    engine: sqlalchemy.engine.Engine,
    image: state.ImageRecord,
    canonical: state.LocationRecord,
    count: int,
) -> str:
    """Seeds referenced due rows followed by one unreferenced valid row."""
    location_table = global_user_state.container_image_location_table
    prefix = f'referenced-{count}-'
    seed_locations = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(1)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < :row_count
        )
        INSERT INTO container_image_locations (
          id, workspace, image_id, profile, target_id, target_fingerprint,
          policy_fingerprint, profile_revision, canonical,
          canonical_location_id, canonical_ready, target_ref, expected_digest,
          state, attempt_count, last_used_at, auto_evict, updated_at
        )
        SELECT :prefix || printf('%06d', n), 'research', :image_id, 'managed',
               'referenced-target-' || n, printf('%064x', n),
               :policy_fingerprint, 1, 0, :canonical_id, 1,
               'registry.example.com/referenced-' || n || '@' || :digest,
               :digest, 'READY', 0, 1, 1, 1
        FROM synthetic
    """)
    seed_references = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(1)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < :row_count
        )
        INSERT INTO container_image_references (
          id, workspace, location_id, consumer_type, consumer_id, expires_at,
          created_at, updated_at
        )
        SELECT 'reference-' || :prefix || n, 'research',
               :prefix || printf('%06d', n), 'serve',
               'consumer-' || :prefix || n, NULL, 1, 1
        FROM synthetic
    """)
    parameters = {
        'row_count': count,
        'prefix': prefix,
        'image_id': image.id,
        'canonical_id': canonical.id,
        'policy_fingerprint': _POLICY_FINGERPRINT,
        'digest': image.source_digest,
    }
    eventual_id = str(uuid.uuid4())
    with engine.begin() as connection:
        connection.execute(seed_locations, parameters)
        connection.execute(seed_references, parameters)
        connection.execute(location_table.insert().values(
            id=eventual_id,
            workspace='research',
            image_id=image.id,
            profile='managed',
            target_id='eventual-unreferenced',
            target_fingerprint='e' * 64,
            policy_fingerprint=_POLICY_FINGERPRINT,
            profile_revision=1,
            canonical=False,
            canonical_location_id=canonical.id,
            canonical_ready=True,
            target_ref=(f'registry.example.com/eventual@'
                        f'{image.source_digest}'),
            expected_digest=image.source_digest,
            state=models.ImageLocationState.READY.value,
            attempt_count=0,
            last_used_at=2,
            auto_evict=True,
            updated_at=2,
        ))
    return eventual_id


def test_schema_023_upgrades_existing_sqlite_database(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "old.db"}')
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'clusters', old_metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True))
    old_metadata.create_all(engine)

    schema_023 = importlib.import_module(
        'sky.schemas.db.global_user_state.023_container_images')
    with engine.connect() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            schema_023.upgrade()

    inspector = sqlalchemy.inspect(engine)
    assert migration_utils.GLOBAL_USER_STATE_VERSION == '023'
    assert {
        'container_image_catalog', 'container_images',
        'container_image_sources', 'container_image_releases',
        'container_image_profile_revisions', 'container_image_locations',
        'container_image_references', 'container_image_workspace_catalogs'
    } <= set(inspector.get_table_names())
    image_columns = {
        column['name'] for column in inspector.get_columns('container_images')
    }
    assert {
        'workspace', 'source_digest', 'resolved_source_ref', 'producer_kind',
        'producer_spec_hash', 'builder_version'
    } <= image_columns
    compressed_size_type = (
        global_user_state.container_image_table.c.compressed_size_bytes.type)
    assert compressed_size_type.compile(
        dialect=sqlalchemy_postgresql.dialect()) == 'BIGINT'
    location_columns = {
        column['name']
        for column in inspector.get_columns('container_image_locations')
    }
    assert {
        'id', 'workspace', 'image_id', 'profile', 'target_id',
        'target_fingerprint', 'policy_fingerprint', 'profile_revision',
        'canonical', 'canonical_location_id', 'expected_digest', 'state',
        'source_id', 'lease_owner', 'last_used_at', 'auto_evict',
        'verification_requested_at', 'canonical_ready'
    } <= location_columns
    image_indexes = {
        index['name'] for index in inspector.get_indexes('container_images')
    }
    assert 'ix_container_images_scope_source_ref' in image_indexes
    assert 'ix_container_images_scope_created' in image_indexes
    source_indexes = {
        index['name']
        for index in inspector.get_indexes('container_image_sources')
    }
    assert 'ix_container_image_sources_resolved' in source_indexes
    unique_constraints = {
        constraint['name']
        for constraint in inspector.get_unique_constraints('container_images')
    }
    assert {'uq_container_images_scope_digest'} <= unique_constraints
    location_indexes = {
        index['name']
        for index in inspector.get_indexes('container_image_locations')
    }
    assert {
        'ix_container_image_locations_eviction_ready',
        'ix_container_image_locations_eviction_lease',
        'ix_container_image_locations_profile_eviction_ready',
        'ix_container_image_locations_profile_eviction_retry',
        'ix_container_image_locations_profile_eviction_lease',
        'ix_container_image_locations_profile_eviction_incomplete_lease',
        'ix_container_image_locations_materialize_queue',
        'ix_container_image_locations_profile_pending_queue',
        'ix_container_image_locations_regional_pending_queue',
        'ix_container_image_locations_profile_pending_retry',
        'ix_container_image_locations_regional_pending_retry',
        'ix_container_image_locations_profile_copying_queue',
        'ix_container_image_locations_regional_copying_queue',
        'ix_container_image_locations_profile_copying_incomplete_lease',
        'ix_container_image_locations_regional_copying_incomplete_lease',
        'ix_container_image_locations_profile_retry_queue',
        'ix_container_image_locations_regional_retry_queue',
        'ix_container_image_locations_verify_queue',
        'ix_container_image_locations_profile_verification_queue',
        'ix_container_image_locations_regional_verify_queue',
        'ix_container_image_locations_profile_verification_retry',
        'ix_container_image_locations_regional_verification_retry',
        'ix_container_image_locations_profile_copying_active_lease',
        'ix_container_image_locations_profile_evicting_active_lease',
        'ix_container_image_locations_profile_verification_active_lease',
        'ix_container_image_locations_canonical_source',
        'ix_container_image_locations_ready_canonical_dependency',
        'ix_container_image_locations_import_source',
    } <= location_indexes
    reference_indexes = {
        index['name']
        for index in inspector.get_indexes('container_image_references')
    }
    assert 'ix_container_image_references_consumer' in reference_indexes


def test_schema_023_restart_recreates_missing_index(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "partial.db"}')
    schema_023 = importlib.import_module(
        'sky.schemas.db.global_user_state.023_container_images')

    def _upgrade() -> None:
        with engine.connect() as connection:
            context = migration.MigrationContext.configure(connection)
            with operations.Operations.context(context):
                schema_023.upgrade()

    _upgrade()
    missing_index = 'ix_container_image_locations_regional_pending_queue'
    with engine.begin() as connection:
        connection.exec_driver_sql(f'DROP INDEX {missing_index}')
    assert missing_index not in {
        index['name'] for index in sqlalchemy.inspect(engine).get_indexes(
            'container_image_locations')
    }

    _upgrade()
    assert missing_index in {
        index['name'] for index in sqlalchemy.inspect(engine).get_indexes(
            'container_image_locations')
    }


def test_container_image_scalar_object_and_digest_validation():
    docker_repository = f'docker:repo@{_DIGEST}'
    scalar = models.ContainerImage.from_config(docker_repository)
    assert scalar.ref == docker_repository
    assert scalar.to_yaml_config() == docker_repository
    with pytest.raises(ValueError, match='OCI name components'):
        models.ContainerImage.from_config('docker:ubuntu:22.04')
    with pytest.raises(ValueError, match='no whitespace'):
        models.ContainerImage.from_config(f' {docker_repository}')

    detailed = models.ContainerImage.from_config({
        'ref': _SOURCE,
        'profile': 'managed',
        'version': 'boltz-2.1.0',
    })
    assert detailed.digest == _DIGEST
    assert detailed.to_yaml_config() == {
        'ref': _SOURCE,
        'distribution': 'managed',
        'release': 'boltz-2.1.0',
    }
    constructor_compat = models.ContainerImage(ref=_SOURCE,
                                               profile='managed',
                                               version='boltz-2.1.0')
    assert constructor_compat.distribution == 'managed'
    assert constructor_compat.release == 'boltz-2.1.0'
    assert constructor_compat.profile == 'managed'
    assert constructor_compat.version == 'boltz-2.1.0'
    with pytest.raises(ValueError, match='conflicting distribution'):
        models.ContainerImage(ref=_SOURCE, distribution='one', profile='two')
    with pytest.raises(ValueError, match='conflicting release'):
        models.ContainerImage(ref=_SOURCE, release='one', version='two')
    with pytest.raises(ValueError, match='sha256'):
        models.ContainerImage('repo/image@sha256:short')
    with pytest.raises(ValueError, match='invalid OCI image tag'):
        models.ContainerImage('docker:')
    with pytest.raises(ValueError, match='inline userinfo'):
        models.ContainerImage(
            f'user:secret@registry.example.com/repo@{_DIGEST}')
    for invalid in (
            f'registry.example/repo?token=supersecret@{_DIGEST}',
            f'registry.example/repo#fragment@{_DIGEST}',
            f'registry.example/repo%2fsecret@{_DIGEST}',
    ):
        with pytest.raises(ValueError, match='query|fragment|percent'):
            models.ContainerImage(invalid)
        with pytest.raises(ValueError, match='query|fragment|percent'):
            state.register_image(invalid, invalid, _DIGEST, 'research',
                                 'user-1')
    assert (models.ContainerImage('localhost:5000/team/model:Release_1').ref ==
            'localhost:5000/team/model:Release_1')
    canonicalized = models.ContainerImage(
        f'REGISTRY.EXAMPLE.COM.:443/team/model@{_DIGEST}')
    assert canonicalized.ref == f'registry.example.com/team/model@{_DIGEST}'
    repository_at_limit = f'r.io/{"a" * 250}'
    assert len(repository_at_limit) == 255
    assert (models.ContainerImage(f'{repository_at_limit}@{_DIGEST}').ref ==
            f'{repository_at_limit}@{_DIGEST}')
    with pytest.raises(ValueError, match='at most 255'):
        models.ContainerImage(f'r.io/{"a" * 251}@{_DIGEST}')
    with pytest.raises(ValueError, match='invalid registry port'):
        models.ContainerImage(f'https:/example.com/repo@{_DIGEST}')
    with pytest.raises(ValueError, match='unsupported'):
        models.ContainerImage.from_config({'ref': 'ubuntu', 'password': 'x'})
    with pytest.raises(ValueError, match='release'):
        models.ContainerImage(_SOURCE, release='two words')


def test_registry_config_is_redacted_before_semantic_admission():
    secret = 'https://user:registry-secret@registry.example'
    raw_config = {
        'container_registries': {
            'default_profile': 'managed',
            'profiles': {
                'managed': {
                    'ownership': 'external',
                    'realm': 'production',
                    'namespace': 'skypilot/{workspace}',
                    'revision': 1,
                    'canonical': {
                        'provider': 'aws',
                        'account': '123456789012',
                        'region': 'us-east-1',
                        'pull_auth': 'anonymous',
                        'manager_identity': secret,
                    },
                },
            },
        },
        'workspaces': {
            'research': {
                'container_images': {
                    'mode': 'managed_required',
                    'default_profile': 'managed',
                    'allowed_profiles': ['managed'],
                },
            },
        },
    }

    log_safe = skypilot_config._redact_container_image_config_for_logging(
        raw_config)
    dump_safe = debug_dump_helpers.redact_config(raw_config)
    assert secret not in json.dumps(log_safe)
    assert secret not in json.dumps(dump_safe)
    assert log_safe['container_registries'] == '<redacted>'
    assert dump_safe['container_registries'] == '<redacted>'

    with pytest.raises(ValueError) as error:
        skypilot_config._validate_container_image_config(
            raw_config, '<test config>')
    assert secret not in str(error.value)


@pytest.mark.parametrize(
    'workspaces',
    [
        [{
            'container_images': 'workspace-parent-secret'
        }],
        {
            'research': 'workspace-parent-secret'
        },
        {
            'research': {
                'container_images': 'workspace-parent-secret'
            }
        },
    ],
)
def test_malformed_workspace_parents_are_redacted_before_schema_validation(
        workspaces):
    raw_config = {'workspaces': workspaces}

    log_safe = skypilot_config._redact_container_image_config_for_logging(
        raw_config)
    dump_safe = debug_dump_helpers.redact_config(raw_config)
    assert 'workspace-parent-secret' not in json.dumps(log_safe)
    assert 'workspace-parent-secret' not in json.dumps(dump_safe)

    with pytest.raises(ValueError) as error:
        skypilot_config._validate_config(raw_config, '<malformed workspace>')
    assert 'workspace-parent-secret' not in str(error.value)


@pytest.mark.parametrize(
    'workspace_key',
    [
        'https://user:workspace-key-secret@example.com',
        7,
        'a' * 64,
    ],
)
def test_malformed_workspace_keys_are_redacted_and_rejected(workspace_key):
    raw_config = {
        'workspaces': {
            workspace_key: {
                'container_images': {
                    'mode': 'managed_required'
                }
            },
            'research': {
                'gcp': {
                    'project_id': 'safe-project'
                }
            },
        }
    }

    log_safe = skypilot_config._redact_container_image_config_for_logging(
        raw_config)
    dump_safe = debug_dump_helpers.redact_config(raw_config)
    assert log_safe['workspaces'] == '<redacted>'
    assert dump_safe['workspaces'] == '<redacted>'
    assert skypilot_config._has_container_image_config(raw_config)

    with pytest.raises(ValueError) as error:
        skypilot_config._validate_config(raw_config,
                                         '<malformed workspace key>')
    assert 'workspace-key-secret' not in str(error.value)
    assert str(workspace_key) not in str(error.value)


@pytest.mark.parametrize(
    'stored_yaml',
    [
        ('workspaces:\n'
         '  - container_images: database-config-secret\n'),
        ('container_registries: {}\n'
         'container_registries: database-config-secret\n'),
    ],
)
def test_database_server_config_reuses_value_free_config_admission(
        monkeypatch, stored_yaml):
    engine = sqlalchemy.create_engine('sqlite://')
    skypilot_config.Base.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(skypilot_config.config_yaml_table.insert().values(
            key=skypilot_config.API_SERVER_CONFIG_KEY, value=stored_yaml))
    monkeypatch.setattr(skypilot_config._db_manager, 'get_engine',
                        lambda: engine)

    with pytest.raises(ValueError) as error:
        skypilot_config._overlay_db_config(config_utils.Config(), 'ignored')
    assert 'database-config-secret' not in str(error.value)


def test_registry_config_schema_and_cli_errors_never_echo_values():
    secret = 'registry-secret-that-must-not-be-rendered'
    raw_config = {
        'container_registries': {
            'profiles': {
                'managed': {
                    'ownership': 'external',
                    'realm': 'production',
                    'namespace': 'skypilot/{workspace}',
                    'revision': 1,
                    'canonical': {
                        'provider': 'aws',
                        'account': '123456789012',
                        'region': 'us-east-1',
                        'pull_auth': secret,
                    },
                },
            },
        },
    }

    with pytest.raises(ValueError) as schema_error:
        skypilot_config._validate_config(raw_config, '<test config>')
    assert secret not in str(schema_error.value)

    with pytest.raises(ValueError) as cli_error:
        skypilot_config._compose_cli_config([
            'container_registries.profiles.managed.canonical.pull_auth='
            f'{secret}'
        ])
    assert secret not in str(cli_error.value)

    with pytest.raises(ValueError) as malformed_cli_error:
        skypilot_config._compose_cli_config([secret])
    assert secret not in str(malformed_cli_error.value)


def test_invalid_registry_authority_cause_chains_are_value_free():
    secret = 'authority-port-secret'
    hostile = f'registry.example.com:{secret}/team/repo@{_DIGEST}'
    raw_config = {
        'container_registries': {
            'profiles': {
                'managed': {
                    'ownership': 'external',
                    'realm': 'production',
                    'namespace': 'skypilot/{workspace}',
                    'revision': 1,
                    'canonical': {
                        'provider': 'generic',
                        'region': 'global',
                        'registry': f'registry.example.com:{secret}/team',
                        'pull_auth': 'anonymous',
                    },
                },
            },
        },
    }
    operations = (
        lambda: resources_lib.Resources(container_image={'ref': hostile}),
        lambda: oci.OciClient().inspect_metadata(hostile),
        lambda: skypilot_config._validate_config(raw_config, '<test config>'),
    )
    for operation in operations:
        with pytest.raises(ValueError) as error:
            operation()
        rendered = ''.join(
            traceback.format_exception(type(error.value), error.value,
                                       error.value.__traceback__))
        assert secret not in rendered
        assert error.value.__cause__ is None


@pytest.mark.parametrize(
    'shape',
    ['direct', 'any_of', 'ordered', 'malformed_any_of', 'malformed_ordered'])
def test_structured_image_schema_failures_are_value_free(shape):
    secret = 'structured-schema-secret'
    if shape == 'direct':
        resource_config = {
            'container_image': {
                'ref': _SOURCE,
                f'https://user:{secret}@registry.example': 'ignored',
            },
        }
    elif shape == 'any_of':
        resource_config = {
            'any_of': [{
                'container_image': {
                    'artifact_id': {
                        'value': secret,
                    },
                },
            }],
        }
    elif shape == 'ordered':
        resource_config = {
            'ordered': [{
                'container_image': {
                    'release': 'safe-release',
                    'distribution': {
                        'value': secret,
                    },
                },
            }],
        }
    elif shape == 'malformed_any_of':
        resource_config = {
            'any_of': {
                'container_image': {
                    'ref': f'https://user:{secret}@registry.example/repo',
                },
            },
        }
    else:
        resource_config = {
            'ordered': [f'https://user:{secret}@registry.example/repo',],
        }
    task_yaml = yaml_utils.dump_yaml_str({'resources': resource_config})
    operations = (
        lambda: resources_lib.Resources.from_yaml_config(resource_config),
        lambda: task_lib.Task.from_yaml_str(task_yaml),
        lambda: dag_utils.load_chain_dag_from_yaml_str(task_yaml),
    )
    for operation in operations:
        with pytest.raises(ValueError) as error:
            operation()
        rendered = ''.join(
            traceback.format_exception(type(error.value), error.value,
                                       error.value.__traceback__))
        assert secret not in rendered
        assert error.value.__cause__ is None


def test_yaml_policy_parse_failures_are_closed_duplicate_safe_and_value_free(
        tmp_path, caplog):
    malformed = tmp_path / 'malformed.yaml'
    malformed.write_text('container_registries:\n  profiles: [\n')
    with pytest.raises(ValueError, match='Invalid YAML syntax'):
        skypilot_config.parse_and_validate_config_file(str(malformed))

    duplicate = tmp_path / 'duplicate.yaml'
    duplicate.write_text('workspaces:\n'
                         '  research:\n'
                         '    container_images:\n'
                         '      mode: managed_required\n'
                         '      mode: managed_preferred\n')
    with pytest.raises(ValueError, match='Duplicate key'):
        skypilot_config.parse_and_validate_config_file(str(duplicate))

    secret = 'yaml-parser-secret'
    hostile = f'!<https://user:{secret}@registry.example/x> ignored'
    tagged = tmp_path / 'tagged.yaml'
    tagged.write_text(hostile)
    loaders = (
        lambda: skypilot_config.parse_and_validate_config_file(str(tagged)),
        lambda: skypilot_config._compose_cli_config(
            [f'workspaces.research.container_images.mode={hostile}']),
        lambda: task_lib.Task.from_yaml_str(hostile),
        lambda: dag_utils.load_chain_dag_from_yaml_str(hostile),
        lambda: yaml_utils.read_yaml_str(hostile),
    )
    for load in loaders:
        with pytest.raises(ValueError) as error:
            load()
        assert secret not in str(error.value)
        assert secret not in repr(error.value.__cause__)
    is_yaml, _, provided, reason = cli_command._check_yaml_only(str(tagged))
    assert not is_yaml
    assert provided
    assert secret not in reason
    assert secret not in caplog.text


def test_yaml_duplicate_alias_and_complexity_errors_are_bounded_and_value_free(
        monkeypatch):
    secret = 'duplicate-key-secret'
    duplicate = f'{secret}: first\n{secret}: second\n'
    with pytest.raises(ValueError) as duplicate_error:
        yaml_utils.read_yaml_str(duplicate, reject_duplicate_keys=True)
    duplicate_trace = ''.join(
        traceback.format_exception(type(duplicate_error.value),
                                   duplicate_error.value,
                                   duplicate_error.value.__traceback__))
    assert secret not in duplicate_trace
    assert duplicate_error.value.__cause__ is None

    recursive = 'resources: &resources\n  any_of:\n    - *resources\n'
    with pytest.raises(ValueError, match='alias graph contains a cycle'):
        yaml_utils.read_yaml_str(recursive, reject_duplicate_keys=True)

    monkeypatch.setattr(yaml_utils, '_MAX_YAML_GRAPH_EDGES', 4)
    complex_document = 'root:\n  first: one\n  second: two\n  third: three\n'
    with pytest.raises(ValueError, match='document is too complex'):
        yaml_utils.read_yaml_str(complex_document, reject_duplicate_keys=True)


def test_release_labels_cannot_carry_secrets_or_terminal_controls(
        image_state_engine):
    del image_state_engine
    invalid_releases = (
        '\x1b]52;c;YXR0YWNrZXI=\x07release',
        'https://user:supersecret@registry.example.com',
        'team/release',
        'release:tag',
        '-leading-hyphen',
        'two words',
        'a' * 129,
    )
    for release in invalid_releases:
        with pytest.raises(ValueError, match='ASCII'):
            models.ContainerImage(_SOURCE, release=release)
        with pytest.raises(ValueError, match='ASCII'):
            resources_lib.Resources(container_image={'release': release})
        with pytest.raises(ValueError, match='ASCII'):
            state.register_image(_SOURCE,
                                 _SOURCE,
                                 _DIGEST,
                                 'research',
                                 'user-1',
                                 release=release)

        response_payload = {
            'id': _ARTIFACT_ID,
            'workspace': 'research',
            'sources': [_SOURCE],
            'source_digest': _DIGEST,
            'releases': [release],
            'producer_kind': 'external_oci',
            'platforms': [],
            'created_at': 1,
            'updated_at': 1,
            'locations': [],
        }
        with pytest.raises(ValueError, match='ASCII'):
            responses.ContainerImageRecord(**response_payload)

        with pytest.raises(ValueError) as yaml_error:
            resources_lib.Resources.from_yaml_config(
                {'container_image': {
                    'release': release,
                }})
        assert release not in str(yaml_error.value)

        resources = resources_lib.Resources(
            container_image={'release': 'safe-release'})
        pickle_state = resources.__getstate__()
        pickle_state['_container_image'] = {'release': release}
        with pytest.raises(ValueError) as pickle_error:
            resources.__setstate__(pickle_state)
        assert release not in str(pickle_error.value)

    valid = models.ContainerImage(_SOURCE, release='Release_1.2-rc.3')
    assert valid.release == 'Release_1.2-rc.3'
    assert state.list_images('research') == []


def test_catalog_identifiers_cannot_carry_secrets_or_terminal_controls(
        image_state_engine):
    del image_state_engine
    invalid_identifiers = (
        'https://user:supersecret@example.com/repo',
        '\x1b]52;c;YXR0YWNrZXI=\x07name',
        'team/name',
        'a' * 129,
    )
    profile = _profile()
    canonical = profile.canonical
    for invalid in invalid_identifiers:
        for config_value in ({
                'artifact_id': invalid
        }, {
                'release': 'safe-release',
                'distribution': invalid,
        }):
            with pytest.raises(ValueError) as model_error:
                models.ContainerImage.from_config(config_value)
            assert invalid not in str(model_error.value)

            with pytest.raises(ValueError) as yaml_error:
                resources_lib.Resources.from_yaml_config(
                    {'container_image': config_value})
            assert invalid not in str(yaml_error.value)

            resources = resources_lib.Resources(
                container_image={'release': 'safe-release'})
            pickle_state = resources.__getstate__()
            pickle_state['_container_image'] = config_value
            with pytest.raises(ValueError) as pickle_error:
                resources.__setstate__(pickle_state)
            assert invalid not in str(pickle_error.value)

        with pytest.raises(ValueError) as target_error:
            models.RegistryTarget(name=invalid,
                                  provider='generic',
                                  region='global')
        assert invalid not in str(target_error.value)
        with pytest.raises(ValueError) as profile_error:
            dataclasses.replace(profile, name=invalid)
        assert invalid not in str(profile_error.value)

        with pytest.raises(ValueError) as publish_profile_error:
            state.publish_image(
                source_ref=_SOURCE,
                resolved_source_ref=_SOURCE,
                source_digest=_DIGEST,
                workspace='research',
                creator_user_hash='user-1',
                release=None,
                profile=invalid,
                target_id=canonical.name,
                target_fingerprint=profile.physical_fingerprint(canonical),
                policy_fingerprint=profile.policy_fingerprint(canonical, True),
                profile_revision=profile.revision,
                profile_revision_fingerprint=profile.revision_fingerprint)
        assert invalid not in str(publish_profile_error.value)

        with pytest.raises(ValueError) as publish_target_error:
            state.publish_image(
                source_ref=_SOURCE,
                resolved_source_ref=_SOURCE,
                source_digest=_DIGEST,
                workspace='research',
                creator_user_hash='user-1',
                release=None,
                profile=profile.name,
                target_id=invalid,
                target_fingerprint=profile.physical_fingerprint(canonical),
                policy_fingerprint=profile.policy_fingerprint(canonical, True),
                profile_revision=profile.revision,
                profile_revision_fingerprint=profile.revision_fingerprint)
        assert invalid not in str(publish_target_error.value)

        location_payload = {
            'id': _LOCATION_ID,
            'image_id': _ARTIFACT_ID,
            'distribution': invalid,
            'target_id': 'canonical',
            'target_fingerprint': 'e' * 64,
            'policy_fingerprint': _POLICY_FINGERPRINT,
            'profile_revision': 1,
            'canonical': True,
            'expected_digest': _DIGEST,
            'state': 'PENDING',
            'attempt_count': 0,
            'updated_at': 1,
        }
        with pytest.raises(ValueError) as response_error:
            responses.ContainerImageLocationRecord(**location_payload)
        assert invalid not in str(response_error.value)
        location_payload['distribution'] = 'managed'
        location_payload['target_id'] = invalid
        with pytest.raises(ValueError) as target_response_error:
            responses.ContainerImageLocationRecord(**location_payload)
        assert invalid not in str(target_response_error.value)

        record_payload = {
            'id': invalid,
            'workspace': 'research',
            'sources': [_SOURCE],
            'source_digest': _DIGEST,
            'releases': [],
            'producer_kind': 'external_oci',
            'platforms': [],
            'created_at': 1,
            'updated_at': 1,
            'locations': [],
        }
        with pytest.raises(ValueError) as artifact_response_error:
            responses.ContainerImageRecord(**record_payload)
        assert invalid not in str(artifact_response_error.value)

    normalized = models.ContainerImage(artifact_id=_ARTIFACT_ID.upper())
    assert normalized.artifact_id == _ARTIFACT_ID
    assert (models.ContainerImage(
        release='safe-release',
        distribution='Global_GPU.1').distribution == 'Global_GPU.1')
    assert state.list_images('research') == []


def test_container_image_workspaces_are_bounded_and_value_free(
        image_state_engine):
    profile = _profile()
    canonical = profile.canonical
    publication = state.ImagePublication(
        source_ref=_SOURCE,
        resolved_source_ref=_SOURCE,
        source_digest=_DIGEST,
        workspace='research',
        creator_user_hash='user-1',
        release=None,
        profile=profile.name,
        target_id=canonical.name,
        target_fingerprint=profile.physical_fingerprint(canonical),
        policy_fingerprint=profile.policy_fingerprint(canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
    )
    invalid_workspaces = (
        'https://user:workspace-secret@workspace.example',
        '\x1b]52;c;YXR0YWNrZXI=\x07workspace',
        'Uppercase',
        'a' * 64,
    )
    for workspace in invalid_workspaces:
        with pytest.raises(ValueError) as register_error:
            state.register_image(_SOURCE, _SOURCE, _DIGEST, workspace, 'user-1')
        assert workspace not in str(register_error.value)

        with pytest.raises(ValueError) as publication_error:
            state.publish_images_atomically(
                [dataclasses.replace(publication, workspace=workspace)])
        assert workspace not in str(publication_error.value)

        with pytest.raises(ValueError) as core_error:
            core.status(_SOURCE, workspace=workspace)
        assert workspace not in str(core_error.value)

        response_payload = {
            'id': _ARTIFACT_ID,
            'workspace': workspace,
            'sources': [_SOURCE],
            'source_digest': _DIGEST,
            'releases': [],
            'producer_kind': 'external_oci',
            'platforms': [],
            'created_at': 1,
            'updated_at': 1,
            'locations': [],
        }
        with pytest.raises(ValueError) as response_error:
            responses.ContainerImageRecord(**response_payload)
        assert workspace not in str(response_error.value)

    assert state.list_images('research') == []
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    credential = 'https://user:workspace-secret@workspace.example'
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(global_user_state.container_image_table.update().where(
            global_user_state.container_image_table.c.id == image.id).values(
                workspace=credential))
        session.commit()
    with pytest.raises(ValueError) as stored_error:
        state.get_image(image.id)
    assert credential not in str(stored_error.value)


def test_direct_core_rejects_explicit_empty_workspace(monkeypatch):
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'preferred')
    list_images = mock.Mock()
    publish_image = mock.Mock()
    monkeypatch.setattr(state, 'list_images', list_images)
    monkeypatch.setattr(state, 'publish_image', publish_image)

    with pytest.raises(ValueError, match='workspace name'):
        core.status(workspace='')
    with pytest.raises(ValueError, match='workspace name'):
        core.publish(_SOURCE, workspace='')
    list_images.assert_not_called()
    publish_image.assert_not_called()
    assert core._workspace(None) == 'preferred'


def test_producer_metadata_is_closed_bounded_and_redacted(image_state_engine):
    profile = _profile()
    canonical = profile.canonical
    publication = state.ImagePublication(
        source_ref=_SOURCE,
        resolved_source_ref=_SOURCE,
        source_digest=_DIGEST,
        workspace='research',
        creator_user_hash='user-1',
        release=None,
        profile=profile.name,
        target_id=canonical.name,
        target_fingerprint=profile.physical_fingerprint(canonical),
        policy_fingerprint=profile.policy_fingerprint(canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        producer_kind='external_oci',
        producer_spec_hash='c' * 64,
        builder_version='importer-1.2.3',
    )
    response_payload = {
        'id': _ARTIFACT_ID,
        'workspace': 'research',
        'sources': [_SOURCE],
        'source_digest': _DIGEST,
        'releases': [],
        'producer_kind': 'external_oci',
        'producer_spec_hash': 'c' * 64,
        'builder_version': 'importer-1.2.3',
        'platforms': [],
        'created_at': 1,
        'updated_at': 1,
        'locations': [],
    }
    credential = 'https://user:supersecret@provider.example/error'
    invalid_metadata = (
        ('producer_kind', 'future_producer'),
        ('producer_kind', credential),
        ('producer_spec_hash', 'c' * 63),
        ('producer_spec_hash', credential),
        ('builder_version', credential),
        ('builder_version', '\x1b]52;c;YXR0YWNrZXI=\x07version'),
        ('builder_version', 'v' * 129),
    )
    for field, invalid in invalid_metadata:
        register_kwargs = {
            'producer_kind': publication.producer_kind,
            'producer_spec_hash': publication.producer_spec_hash,
            'builder_version': publication.builder_version,
        }
        register_kwargs[field] = invalid
        with pytest.raises(ValueError) as register_error:
            state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1', **register_kwargs)
        assert invalid not in str(register_error.value)

        with pytest.raises(ValueError) as publication_error:
            state.publish_images_atomically(
                [dataclasses.replace(publication, **{field: invalid})])
        assert invalid not in str(publication_error.value)

        invalid_response = dict(response_payload)
        invalid_response[field] = invalid
        with pytest.raises(ValueError) as response_error:
            responses.ContainerImageRecord(**invalid_response)
        assert invalid not in str(response_error.value)

    tables = (
        global_user_state.container_image_table,
        global_user_state.container_image_source_table,
        global_user_state.container_image_release_table,
        global_user_state.container_image_location_table,
        global_user_state.container_image_profile_revision_table,
    )
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert all(
            session.execute(table.select().limit(1)).first() is None
            for table in tables)

    record = state.register_image(_SOURCE,
                                  _SOURCE,
                                  _DIGEST,
                                  'research',
                                  'user-1',
                                  producer_spec_hash='C' * 64,
                                  builder_version='importer-1.2.3')
    assert record.producer_kind == 'external_oci'
    assert record.producer_spec_hash == 'c' * 64
    assert record.builder_version == 'importer-1.2.3'
    validated_response = responses.ContainerImageRecord(**response_payload)
    assert validated_response.producer_spec_hash == 'c' * 64


@pytest.mark.parametrize(
    'column', ('producer_kind', 'producer_spec_hash', 'builder_version'))
def test_stored_producer_metadata_fails_closed_before_status(
        image_state_engine, column):
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    credential = 'https://user:supersecret@provider.example/error'
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(global_user_state.container_image_table.update().where(
            global_user_state.container_image_table.c.id == image.id).values(
                **{column: credential}))
        session.commit()

    with pytest.raises(ValueError) as state_error:
        state.get_image(image.id, 'research')
    assert credential not in str(state_error.value)
    with pytest.raises(ValueError) as status_error:
        core.status(f'artifact_id={image.id}', 'research')
    assert credential not in str(status_error.value)


@pytest.mark.parametrize('column', ('target_fingerprint', 'policy_fingerprint'))
def test_stored_location_fingerprint_fails_closed_before_status(
        image_state_engine, column):
    image = _publish_state_image(_SOURCE, _DIGEST)
    location = state.list_locations(image.id)[0]
    credential = 'https://user:supersecret@provider.example/error'
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(
            global_user_state.container_image_location_table.update().where(
                global_user_state.container_image_location_table.c.id ==
                location.id).values(**{column: credential}))
        session.commit()

    with pytest.raises(ValueError) as state_error:
        state.list_locations(image.id)
    assert credential not in str(state_error.value)
    with pytest.raises(ValueError) as status_error:
        core.status(f'artifact_id={image.id}', 'research')
    assert credential not in str(status_error.value)

    response_payload = {
        'id': location.id,
        'image_id': location.image_id,
        'distribution': location.profile,
        'target_id': location.target_id,
        'target_fingerprint': location.target_fingerprint,
        'policy_fingerprint': location.policy_fingerprint,
        'profile_revision': location.profile_revision,
        'canonical': location.canonical,
        'canonical_location_id': location.canonical_location_id,
        'target_ref': location.target_ref,
        'expected_digest': location.expected_digest,
        'state': location.state.value,
        'attempt_count': location.attempt_count,
        'updated_at': location.updated_at,
    }
    for invalid in (credential, 'A' * 64):
        invalid_response = dict(response_payload)
        invalid_response[column] = invalid
        with pytest.raises(ValueError) as response_error:
            responses.ContainerImageLocationRecord(**invalid_response)
        assert invalid not in str(response_error.value)


def test_location_response_closed_values_never_reflect_input():
    response_payload = {
        'id': _LOCATION_ID,
        'image_id': _ARTIFACT_ID,
        'distribution': 'managed',
        'target_id': 'canonical',
        'target_fingerprint': 'e' * 64,
        'policy_fingerprint': _POLICY_FINGERPRINT,
        'profile_revision': 1,
        'canonical': True,
        'expected_digest': _DIGEST,
        'state': 'PENDING',
        'attempt_count': 0,
        'updated_at': 1,
    }
    credential = 'https://user:supersecret@provider.example/error'
    for field in ('state', 'last_error'):
        invalid_response = dict(response_payload)
        invalid_response[field] = credential
        with pytest.raises(ValueError) as response_error:
            responses.ContainerImageLocationRecord(**invalid_response)
        assert credential not in str(response_error.value)


def test_completion_rejects_and_redacts_credential_bearing_reference(
        image_state_engine):
    del image_state_engine
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    claim = state.claim_location(canonical.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    malicious = ('https://user:supersecret@registry.example.com/repo'
                 f'?token=alsosecret@{_DIGEST}')
    assert not _complete_location(canonical.id, claim.lease_owner, malicious,
                                  _DIGEST)
    failed = state.get_location_by_id(canonical.id)
    assert failed is not None
    assert failed.state == models.ImageLocationState.FAILED
    assert failed.target_ref is None
    assert failed.last_error == (
        models.ImageLocationErrorCode.DESTINATION_REFERENCE_INVALID.value)
    assert 'supersecret' not in str(failed.to_dict())
    assert 'alsosecret' not in str(failed.to_dict())


def test_runtime_pull_models_validate_secret_free_references():
    malicious = ('https://user:supersecret@registry.example.com/repo'
                 f'@{_DIGEST}')
    with pytest.raises(ValueError, match='URL scheme'):
        models.ImageRoute(image_id=_ARTIFACT_ID,
                          location_id=_LOCATION_ID,
                          target_id='canonical',
                          distribution='managed',
                          profile_revision=1,
                          policy_fingerprint=_POLICY_FINGERPRINT,
                          provider='generic',
                          region='global',
                          reference=malicious,
                          digest=_DIGEST,
                          auth_strategy='anonymous',
                          state=models.ImageLocationState.READY)
    payload = {
        'image_id': _ARTIFACT_ID,
        'reference': malicious,
        'target_id': 'canonical',
        'digest': _DIGEST,
        'auth_strategy': 'anonymous',
    }
    with pytest.raises(ValueError, match='URL scheme'):
        models.ResolvedContainerImage.from_dict(payload)
    with pytest.raises(ValueError, match='pull-auth strategy'):
        models.ResolvedContainerImage(
            image_id=_ARTIFACT_ID,
            reference=f'registry.example/repo@{_DIGEST}',
            target_id='canonical',
            digest=_DIGEST,
            auth_strategy='Bearer supersecret')
    with pytest.raises(ValueError, match='secret-free reason code'):
        models.ResolvedContainerImage(
            image_id=_ARTIFACT_ID,
            reference=f'registry.example/repo@{_DIGEST}',
            target_id='source',
            digest=_DIGEST,
            auth_strategy='source_config',
            status='WARMING',
            fallback_reason='Authorization: Bearer top-secret-token')
    normalized = models.ResolvedContainerImage(
        image_id=_ARTIFACT_ID,
        location_id=_LOCATION_ID,
        reference=f'REGISTRY.EXAMPLE.:443/repo@{_DIGEST}',
        target_id='canonical',
        distribution='managed',
        profile_revision=1,
        policy_fingerprint=_POLICY_FINGERPRINT,
        digest=_DIGEST,
        auth_strategy='anonymous')
    assert normalized.reference == f'registry.example/repo@{_DIGEST}'

    with pytest.raises(ValueError, match='managed resolved container image'):
        models.ResolvedContainerImage(
            image_id=_ARTIFACT_ID,
            location_id=_LOCATION_ID,
            reference=f'registry.example/repo@{_DIGEST}',
            target_id='canonical',
            digest=_DIGEST,
            auth_strategy='anonymous')
    with pytest.raises(ValueError, match='UUID'):
        models.ImageRoute(image_id=_ARTIFACT_ID,
                          location_id='',
                          target_id='canonical',
                          distribution='managed',
                          profile_revision=1,
                          policy_fingerprint=_POLICY_FINGERPRINT,
                          provider='generic',
                          region='global',
                          reference=f'registry.example/repo@{_DIGEST}',
                          digest=_DIGEST,
                          auth_strategy='anonymous',
                          state=models.ImageLocationState.READY)


def test_resources_container_image_round_trip_and_legacy_normalization():
    resources = resources_lib.Resources(cloud='aws',
                                        image_id='ami-123',
                                        container_image={
                                            'ref': _SOURCE,
                                            'profile': 'managed',
                                            'version': 'boltz-2.1.0',
                                        })
    assert resources.image_id == {None: 'ami-123'}
    assert resources.extract_docker_image() == _SOURCE
    assert resources.container_image.version == 'boltz-2.1.0'
    loaded = list(
        resources_lib.Resources.from_yaml_config(resources.to_yaml_config()))[0]
    assert loaded.container_image == resources.container_image
    assert loaded.image_id == resources.image_id
    assert loaded.copy().container_image == resources.container_image

    legacy = resources_lib.Resources(cloud='aws', image_id='docker:ubuntu')
    assert legacy.image_id is None
    assert legacy.container_image == models.ContainerImage('ubuntu')
    assert legacy.container_image_from_legacy_image_id
    assert legacy.to_yaml_config()['image_id'] == {'docker': 'ubuntu'}
    assert 'container_image' not in legacy.to_yaml_config()
    modernized = legacy.copy(container_image='debian:stable')
    assert not modernized.container_image_from_legacy_image_id
    assert modernized.to_yaml_config()['container_image'] == 'debian:stable'

    replaced = legacy.copy(image_id='docker:ubuntu:22.04')
    assert replaced.image_id is None
    assert replaced.container_image == models.ContainerImage('ubuntu:22.04')
    assert replaced.container_image_from_legacy_image_id
    assert replaced.to_yaml_config()['image_id'] == {'docker': 'ubuntu:22.04'}
    cleared = legacy.copy(image_id=None)
    assert cleared.image_id is None
    assert cleared.container_image is None
    assert not cleared.container_image_from_legacy_image_id
    assert 'image_id' not in cleared.to_yaml_config()


def test_resources_rejects_two_container_image_sources():
    with pytest.raises(ValueError, match='both container_image'):
        resources_lib.Resources(cloud='aws',
                                image_id='docker:ubuntu',
                                container_image='debian')


def test_changing_container_identity_clears_pinned_route_and_login():
    resolved = models.ResolvedContainerImage(
        image_id=_ARTIFACT_ID,
        location_id=_LOCATION_ID,
        reference=f'registry.example.com/repo@{_DIGEST}',
        target_id='canonical',
        distribution='managed',
        profile_revision=1,
        policy_fingerprint=_POLICY_FINGERPRINT,
        digest=_DIGEST,
        auth_strategy='anonymous',
    )
    resources = resources_lib.Resources(
        cloud='aws',
        container_image=_SOURCE,
        _resolved_container_image=resolved,
    )
    # Simulate a pre-boundary or corrupted object to keep the copy cleanup
    # invariant covered even though new construction rejects this state.
    resources._docker_login_config = docker_utils.DockerLoginConfig(
        username='old-user', password='old-secret', server='old.example.com')
    changed = resources.copy(container_image='ubuntu:22.04')
    assert changed.resolved_container_image is None
    assert changed.docker_login_config is None
    assert 'old-secret' not in str(changed.to_yaml_config())

    moved = resources.copy(region='us-west-2')
    assert moved.resolved_container_image is None
    assert moved.docker_login_config is None

    retyped = resources.copy(instance_type='t4g.small')
    assert retyped.resolved_container_image is None
    assert retyped.docker_login_config is None

    legacy = resources_lib.Resources(
        cloud='aws',
        image_id=f'docker:{_SOURCE}',
        _resolved_container_image=resolved,
        _docker_login_config=docker_utils.DockerLoginConfig(
            username='old-user',
            password='old-secret',
            server='old.example.com',
        ),
    )
    replaced_legacy = legacy.copy(image_id='docker:ubuntu:22.04')
    assert replaced_legacy.container_image == models.ContainerImage(
        'ubuntu:22.04')
    assert replaced_legacy.resolved_container_image is None
    assert replaced_legacy.docker_login_config is None
    cleared_legacy = legacy.copy(image_id=None)
    assert cleared_legacy.container_image is None
    assert cleared_legacy.resolved_container_image is None
    assert cleared_legacy.docker_login_config is None


def test_resource_pickle_uses_builtin_state_for_older_clients():
    pull_plan = models.ResolvedContainerImage(
        image_id=_ARTIFACT_ID,
        location_id=_LOCATION_ID,
        reference=f'registry.example.com/repo@{_DIGEST}',
        target_id='canonical',
        distribution='managed',
        profile_revision=1,
        policy_fingerprint=_POLICY_FINGERPRINT,
        digest=_DIGEST,
        auth_strategy='anonymous',
    )
    resources = resources_lib.Resources(
        cloud='aws',
        container_image={
            'ref': _SOURCE,
            'profile': 'managed',
            'version': 'boltz-2.1.0',
        },
        _resolved_container_image=pull_plan,
    )
    serialized = pickle.dumps(resources)
    assert b'sky.container_images.models' not in serialized
    restored = pickle.loads(serialized)
    assert restored.container_image == resources.container_image
    assert restored.resolved_container_image == pull_plan
    assert restored.extract_docker_image() == pull_plan.reference


def test_restored_explicit_direct_credentials_fail_before_persistence(
        image_state_engine):
    secret = 'restored-direct-secret'
    login_config = docker_utils.DockerLoginConfig(
        username='old-user',
        password=secret,
        server='registry.example.com',
    )
    resources = resources_lib.Resources(
        cloud='aws',
        container_image={
            'ref': _SOURCE,
            'distribution': 'direct',
        },
    )
    # Simulate a Resources object restored from a pre-fence v35 server.
    resources._docker_login_config = login_config

    for serialize in (
            resources.to_yaml_config,
            lambda: resources.to_yaml_config(redact_secrets=True),
            lambda: pickle.dumps(resources),
    ):
        with pytest.raises(ValueError) as error:
            serialize()
        assert secret not in str(error.value)

    state_dict = resources.__dict__.copy()
    state_dict['_version'] = 35
    state_dict['_docker_login_config'] = dataclasses.asdict(login_config)
    restored = resources_lib.Resources.__new__(resources_lib.Resources)
    with pytest.raises(ValueError) as error:
        restored.__setstate__(state_dict)
    assert secret not in str(error.value)

    handle = types.SimpleNamespace(launched_resources=resources,
                                   launched_nodes=1)
    with pytest.raises(ValueError) as error:
        global_user_state.add_or_update_cluster(
            'restored-direct-credentials',
            handle,
            requested_resources=None,
            ready=False,
        )
    assert secret not in str(error.value)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'restored-direct-credentials')).first() is None

    legacy = resources_lib.Resources(
        cloud='aws',
        image_id=f'docker:{_SOURCE}',
        _docker_login_config=login_config,
    )
    legacy_config = legacy.to_yaml_config()
    assert legacy_config['image_id'] == {'docker': _SOURCE}
    assert legacy_config['_docker_login_config']['password'] == secret
    restored_legacy = pickle.loads(pickle.dumps(legacy))
    assert restored_legacy.container_image_from_legacy_image_id
    assert restored_legacy.docker_login_config == login_config


def test_registry_profile_precedence_allowlist_and_schema(monkeypatch):
    profile_config = {
        'ownership': 'managed',
        'realm': 'production',
        'organization': 'boltz',
        'namespace': 'skypilot/{organization}/{workspace}',
        'canonical': {
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-east-1',
            'pull_auth': 'ecr_runtime_identity',
        },
        'targets': [{
            'name': 'aws-us-west-2',
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-west-2',
            'pull_auth': 'ecr_runtime_identity',
        }],
    }
    data = {
        'container_registries': {
            'default_profile': 'server-default',
            'profiles': {
                'managed': profile_config,
                'workspace-default': profile_config,
                'server-default': profile_config,
            },
        },
        'workspaces': {
            'research': {
                'container_images': {
                    'mode': 'managed_required',
                    'default_profile': 'workspace-default',
                    'allowed_profiles': ['managed', 'workspace-default'],
                    'locality': 'require',
                    'regional_cache_retention_weeks': 6,
                },
            },
        },
    }
    _mock_config(monkeypatch, data)
    selected, policy = config.resolve_profile('managed')
    assert selected.name == 'managed'
    assert policy.locality == models.Locality.REQUIRE
    assert policy.regional_cache_retention_weeks == 6
    selected, _ = config.resolve_profile(None)
    assert selected.name == 'workspace-default'
    with pytest.raises(ValueError, match='not allowed'):
        config.resolve_profile('server-default')
    with pytest.raises(ValueError, match='requires managed'):
        config.resolve_profile(config.DIRECT_PROFILE)

    common_utils.validate_schema(data, schemas.get_config_schema(),
                                 'Invalid config')
    missing_revision = dict(data)
    missing_revision['container_registries'] = {
        'profiles': {
            'managed': {
                key: value
                for key, value in profile_config.items()
                if key != 'revision'
            },
        },
    }
    with pytest.raises(ValueError, match='revision'):
        common_utils.validate_schema(missing_revision,
                                     schemas.get_config_schema(),
                                     'Invalid config')
    invalid = dict(data)
    invalid['container_registries'] = {
        'profiles': {
            'managed': {
                **profile_config,
                'password': 'must-not-be-accepted',
            }
        }
    }
    with pytest.raises(ValueError, match='password'):
        common_utils.validate_schema(invalid, schemas.get_config_schema(),
                                     'Invalid config')

    mutable_runtime = dict(data)
    mutable_runtime['container_registries'] = {
        'profiles': {
            'managed': {
                **profile_config,
                'require_digest_at_runtime': False,
            }
        }
    }
    with pytest.raises(ValueError, match='False'):
        common_utils.validate_schema(mutable_runtime,
                                     schemas.get_config_schema(),
                                     'Invalid config')

    data['workspaces']['research']['container_images'][
        'regional_cache_retention_weeks'] = None
    assert config.get_workspace_policy(
        'research').regional_cache_retention_weeks is None
    data['workspaces']['research']['container_images'][
        'regional_cache_retention_weeks'] = 0
    with pytest.raises(ValueError, match='positive integer'):
        config.get_workspace_policy('research')


def test_workspace_cache_retention_defaults_to_eight_weeks(monkeypatch):
    _mock_config(monkeypatch, {})
    assert config.get_workspace_policy(
        'research').regional_cache_retention_weeks == 8


def test_managed_preferred_can_explicitly_use_direct_image(monkeypatch):
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_preferred',
                    },
                },
            },
        })
    selected, policy = config.resolve_profile(config.DIRECT_PROFILE)
    assert selected is None
    assert policy.mode == models.WorkspaceImageMode.MANAGED_PREFERRED


def test_profile_rejects_unsupported_auth_and_registry_userinfo(monkeypatch):
    base = {
        'ownership': 'external',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'generic',
            'region': 'global',
            'registry': 'registry.example.com',
            'pull_auth': 'anonymous',
        },
    }
    data = {
        'container_registries': {
            'profiles': {
                'managed': base,
            },
        },
    }
    _mock_config(monkeypatch, data)
    profile, _ = config.resolve_profile('managed')
    assert profile is not None

    base['canonical']['pull_auth'] = 'inline_token'
    with pytest.raises(ValueError, match='unsupported'):
        config.resolve_profile('managed')
    base['canonical']['pull_auth'] = 'anonymous'
    base['canonical']['registry'] = 'https://user:secret@registry.example.com'
    with pytest.raises(ValueError, match='without a URL scheme'):
        config.resolve_profile('managed')


@pytest.mark.parametrize('field', [
    'provider', 'region', 'account', 'project', 'manager_identity', 'pull_auth'
])
def test_registry_target_control_values_reject_secrets_without_reflection(
        field):
    secret = 'supersecret'
    value = f'token={secret}'
    kwargs = {
        'name': 'canonical',
        'provider': 'generic',
        'region': 'global',
        'registry': 'registry.example.com',
        'pull_auth': 'anonymous',
    }
    kwargs[field] = value
    with pytest.raises(ValueError) as error:
        models.RegistryTarget(**kwargs)
    assert value not in str(error.value)
    assert secret not in str(error.value)

    with pytest.raises(ValueError) as provider_error:
        providers.get_adapter(value)
    assert value not in str(provider_error.value)
    assert secret not in str(provider_error.value)


def test_profile_rejects_canonical_endpoint_alias(monkeypatch):
    profile = {
        'ownership': 'managed',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'aws',
            'region': 'us-east-1',
            'account': '123456789012',
            'pull_auth': 'ecr_runtime_identity',
        },
        'targets': [{
            'name': 'canonical-alias',
            'provider': 'aws',
            'region': 'us-east-1',
            'account': '123456789012',
            'pull_auth': 'ecr_runtime_identity',
        }],
    }
    _mock_config(monkeypatch, {
        'container_registries': {
            'profiles': {
                'managed': profile,
            },
        },
    })
    with pytest.raises(ValueError, match='same physical registry endpoint'):
        config.resolve_profile('managed')

    derived_alias = {
        'ownership': 'managed',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'aws',
            'region': 'US-EAST-1',
            'account': '123456789012',
            'pull_auth': 'ecr_runtime_identity',
        },
        'targets': [{
            'name': 'explicit-ecr-alias',
            'provider': 'generic',
            'region': 'global',
            'registry': ('123456789012.dkr.ecr.us-east-1.amazonaws.com:443'),
            'pull_auth': 'anonymous',
        }],
    }
    _mock_config(monkeypatch, {
        'container_registries': {
            'profiles': {
                'managed': derived_alias,
            },
        },
    })
    with pytest.raises(ValueError, match='same physical registry endpoint'):
        config.resolve_profile('managed')

    default_port_alias = {
        'ownership': 'managed',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'generic',
            'region': 'global',
            'registry': 'REGISTRY.EXAMPLE.COM.',
            'pull_auth': 'anonymous',
        },
        'targets': [{
            'name': 'same-registry',
            'provider': 'generic',
            'region': 'global',
            'registry': 'registry.example.com:443',
            'pull_auth': 'anonymous',
        }],
    }
    _mock_config(monkeypatch, {
        'container_registries': {
            'profiles': {
                'managed': default_port_alias,
            },
        },
    })
    with pytest.raises(ValueError, match='same physical registry endpoint'):
        config.resolve_profile('managed')


def test_config_admission_rejects_reserved_canonical_target_name():
    profile = _profile_config()
    profile['targets'] = [{
        'name': 'canonical',
        'provider': 'aws',
        'region': 'us-west-2',
        'account': '123456789012',
        'pull_auth': 'ecr_runtime_identity',
    }]
    with pytest.raises(ValueError, match='registry configuration is invalid'):
        skypilot_config._validate_container_image_config(
            {'container_registries': {
                'profiles': {
                    'managed': profile,
                },
            }}, 'test')


def test_config_admission_ignores_unrelated_workspace_settings():
    skypilot_config._validate_container_image_config(
        {
            'workspaces': {
                # Existing workspace names can predate the managed-image OCI
                # naming contract. Do not reject them until they opt in to an
                # image policy that needs a catalog namespace.
                'workspaceA': {
                    'kubernetes': {
                        'namespace': 'research'
                    }
                }
            }
        },
        'test')


def test_managed_reference_is_scoped_and_digest_pinned():
    profile = _profile()
    reference = references.managed_reference(profile, profile.canonical,
                                             'research', _SOURCE, _DIGEST)
    assert reference == ('123456789012.dkr.ecr.us-east-1.amazonaws.com/'
                         'skypilot/boltz/research/artifacts-aa@' + _DIGEST)
    assert reference == references.managed_reference(
        profile, profile.canonical, 'research',
        f'quay.io/different/repository@{_DIGEST}', _DIGEST)
    with pytest.raises(ValueError, match='sha256'):
        references.managed_reference(profile, profile.canonical, 'research',
                                     _SOURCE, 'sha256:short')
    with pytest.raises(ValueError, match='must match'):
        references.managed_reference(profile, profile.canonical, 'research',
                                     _SOURCE, _OTHER_DIGEST)
    with pytest.raises(ValueError, match='namespace must use lowercase OCI'):
        models.RegistryProfile(
            **{
                **profile.__dict__,
                'namespace': 'skypilot/{organization}/Invalid Namespace',
            })
    for invalid_namespace in ('.bad/{workspace}', 'a..b/{workspace}',
                              'a___b/{workspace}'):
        with pytest.raises(ValueError,
                           match='namespace must use lowercase OCI'):
            models.RegistryProfile(**{
                **profile.__dict__,
                'namespace': invalid_namespace,
            })
    with pytest.raises(ValueError, match='lowercase OCI name components'):
        references.managed_reference(profile, profile.canonical, 'research',
                                     f'ghcr.io/.bad/model@{_DIGEST}', _DIGEST)


def test_namespace_normalization_is_shared_by_identity_and_rendering():
    canonical = _profile()
    slash_spelling = models.RegistryProfile(
        **{
            **canonical.__dict__,
            'namespace': '/skypilot/{organization}/{workspace}/',
            'revision': 2,
        })
    assert slash_spelling.namespace == canonical.namespace
    assert (slash_spelling.physical_fingerprint(
        slash_spelling.canonical) == canonical.physical_fingerprint(
            canonical.canonical))
    assert references.managed_reference(
        slash_spelling, slash_spelling.canonical, 'research', _SOURCE,
        _DIGEST) == references.managed_reference(canonical, canonical.canonical,
                                                 'research', _SOURCE, _DIGEST)


def test_managed_references_use_bounded_digest_shard_repositories():
    profile = _profile()
    same_shard_digest = 'sha256:aa' + 'b' * 62
    other_shard_digest = 'sha256:bb' + 'c' * 62
    references_by_digest = {
        digest: references.managed_reference(
            profile, profile.canonical, 'research',
            f'ghcr.io/boltz-bio/model@{digest}', digest)
        for digest in (_DIGEST, same_shard_digest, other_shard_digest)
    }
    repositories = {
        digest: models.split_digest(reference)[0]
        for digest, reference in references_by_digest.items()
    }
    assert repositories[_DIGEST] == repositories[same_shard_digest]
    assert repositories[_DIGEST].endswith('/artifacts-aa')
    assert repositories[other_shard_digest].endswith('/artifacts-bb')
    assert len(set(references_by_digest.values())) == 3


def test_resolver_locality_and_auth_fail_closed():
    canonical = models.ImageRoute(
        image_id=_ARTIFACT_ID,
        location_id=_CANONICAL_LOCATION_ID,
        target_id='canonical',
        distribution='managed',
        profile_revision=1,
        policy_fingerprint=_POLICY_FINGERPRINT,
        provider='aws',
        region='us-east-1',
        reference=f'ecr.example/repo@{_DIGEST}',
        digest=_DIGEST,
        auth_strategy='ecr_runtime_identity',
        state=models.ImageLocationState.READY,
        platforms=_TEST_PLATFORMS,
        canonical=True,
    )
    regional = models.ImageRoute(
        image_id=_ARTIFACT_ID,
        location_id=_REGIONAL_LOCATION_ID,
        target_id='aws-us-west-2',
        distribution='managed',
        profile_revision=1,
        policy_fingerprint=_POLICY_FINGERPRINT,
        provider='aws',
        region='us-west-2',
        reference=f'ecr-west.example/repo@{_DIGEST}',
        digest=_DIGEST,
        auth_strategy='ecr_runtime_identity',
        state=models.ImageLocationState.READY,
        platforms=_TEST_PLATFORMS,
    )
    placement = models.Placement(provider='aws',
                                 region='us-west-2',
                                 backend='vm',
                                 platform='linux/amd64')
    selected = resolver.resolve(placement, [canonical, regional],
                                models.Locality.PREFER)
    assert selected.target_id == regional.target_id
    assert selected.reference == regional.reference

    pending_regional = models.ImageRoute(**{
        **regional.__dict__,
        'state': models.ImageLocationState.PENDING,
    })
    selected = resolver.resolve(placement, [canonical, pending_regional],
                                models.Locality.PREFER)
    assert selected.target_id == 'canonical'
    with pytest.raises(resolver.ImageRouteUnavailableError, match='required'):
        resolver.resolve(placement, [canonical, pending_regional],
                         models.Locality.REQUIRE)

    canonical_placement = models.Placement(provider='aws',
                                           region='us-east-1',
                                           backend='vm')
    selected = resolver.resolve(canonical_placement, [canonical],
                                models.Locality.REQUIRE)
    assert selected.target_id == 'canonical'

    unsafe_canonical = models.ImageRoute(**{
        **canonical.__dict__,
        'auth_strategy': None,
    })
    with pytest.raises(resolver.ImageRouteUnavailableError, match='no safe'):
        resolver.resolve(placement, [unsafe_canonical], models.Locality.PREFER)


def test_runtime_platform_filters_only_known_incompatible_routes():
    assert models.runtime_platform_from_architecture('x86_64') == 'linux/amd64'
    assert models.runtime_platform_from_architecture('aarch64') == 'linux/arm64'
    assert models.runtime_platform_from_architecture('unknown') is None
    route = models.ImageRoute(image_id=_ARTIFACT_ID,
                              location_id=_CANONICAL_LOCATION_ID,
                              target_id='canonical',
                              distribution='managed',
                              profile_revision=1,
                              policy_fingerprint=_POLICY_FINGERPRINT,
                              provider='aws',
                              region='us-east-1',
                              reference=f'ecr.example/repo@{_DIGEST}',
                              digest=_DIGEST,
                              auth_strategy='ecr_runtime_identity',
                              state=models.ImageLocationState.READY,
                              platforms=('linux/amd64',),
                              canonical=True)
    arm = models.Placement(provider='aws',
                           region='us-east-1',
                           backend='vm',
                           platform='linux/arm64')
    with pytest.raises(resolver.ImageRouteUnavailableError,
                       match='runtime platform'):
        resolver.resolve(arm, [route], models.Locality.PREFER)

    multi_arch = dataclasses.replace(route,
                                     platforms=('linux/amd64',
                                                'linux/arm64/v8'))
    assert resolver.resolve(arm, [multi_arch],
                            models.Locality.PREFER).image_id == _ARTIFACT_ID
    unknown_placements = (
        models.Placement(provider='nebius', region='eu-north1', backend='vm'),
        models.Placement(provider='kubernetes',
                         region='prod-research-cluster',
                         backend='kubernetes'),
    )
    for unknown in unknown_placements:
        assert resolver.resolve(unknown, [route],
                                models.Locality.PREFER).image_id == _ARTIFACT_ID
        assert resolver.resolve(unknown, [multi_arch],
                                models.Locality.PREFER).image_id == _ARTIFACT_ID


def test_preprovision_rejects_known_incompatible_artifact_platform(
        image_state_engine, monkeypatch):
    del image_state_engine
    _mock_registry_profile(monkeypatch)
    profile = _profile()
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    claim = state.claim_location(canonical.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    reference = references.managed_reference(profile, profile.canonical,
                                             'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id,
                              claim.lease_owner,
                              reference,
                              _DIGEST,
                              platforms=('linux/amd64',))
    requested = resources_lib.Resources(cloud='aws',
                                        region='us-east-1',
                                        container_image={
                                            'artifact_id': image.id,
                                            'distribution': profile.name,
                                        })
    with pytest.raises(resolver.ImageRouteUnavailableError,
                       match='does not support'):
        core.resolve_for_placement(
            requested,
            models.Placement(provider='aws',
                             region='us-east-1',
                             backend='vm',
                             platform='linux/arm64'))

    cloud_vm_ray_backend = importlib.import_module(
        'sky.backends.cloud_vm_ray_backend')
    gcp_requested = resources_lib.Resources(cloud=clouds.GCP(),
                                            region='us-central1',
                                            instance_type='t2a-standard-4',
                                            container_image={
                                                'artifact_id': image.id,
                                                'distribution': profile.name,
                                            })
    assert (clouds.GCP.get_arch_from_instance_type('t2a-standard-4') == 'arm64')
    with pytest.raises(exceptions.ResourcesUnavailableError,
                       match='does not support'):
        cloud_vm_ray_backend._resolve_container_image_for_placement(
            gcp_requested)

    with pytest.raises(ValueError, match='runtime architecture'):
        global_user_state._validate_container_image_runtime_platform(
            json.dumps(['linux/amd64']), gcp_requested)
    global_user_state._validate_container_image_runtime_platform(
        json.dumps(['linux/arm64']), gcp_requested)


def test_cluster_commit_rejects_known_incompatible_artifact_platform():
    launched = resources_lib.Resources(cloud=clouds.AWS(),
                                       instance_type='t4g.small')
    with mock.patch.object(clouds.AWS,
                           'get_arch_from_instance_type',
                           return_value='arm64'):
        with pytest.raises(ValueError, match='runtime architecture'):
            global_user_state._validate_container_image_runtime_platform(
                json.dumps(['linux/amd64']), launched)
        global_user_state._validate_container_image_runtime_platform(
            json.dumps(['linux/amd64', 'linux/arm64']), launched)

    # Clouds without architecture metadata must not force an unrelated arm64
    # build. They retain the verified single-platform image and defer the
    # unknown compatibility decision to the runtime.
    unknown_placements = (
        resources_lib.Resources(cloud=clouds.Nebius(),
                                instance_type='gpu-l4-1gpu'),
        resources_lib.Resources(cloud=clouds.Kubernetes(),
                                instance_type='4CPU--16GB--L4:1'),
    )
    for unknown in unknown_placements:
        global_user_state._validate_container_image_runtime_platform(
            json.dumps(['linux/amd64']), unknown)


def test_service_version_snapshots_one_artifact_across_candidates(
        image_state_engine, monkeypatch):
    del image_state_engine
    _mock_registry_profile(monkeypatch)
    first = _publish_state_image(_SOURCE, _DIGEST, release='model-a')
    state.bind_release(first.id, 'research', 'model-a-alias')
    candidates = [
        resources_lib.Resources(cloud='aws',
                                region=region,
                                container_image={
                                    'release': release,
                                    'distribution': 'managed',
                                })
        for region, release in (('us-east-1', 'model-a'), ('us-west-2',
                                                           'model-a-alias'))
    ]
    task = task_lib.Task().set_resources(candidates)
    assert serve_utils.snapshot_service_container_images(task) == first.id
    snapshotted = list(task.resources)
    assert len(snapshotted) == 2
    assert {resource.container_image.artifact_id for resource in snapshotted
           } == {first.id}
    assert {resource.container_image.distribution for resource in snapshotted
           } == {'managed'}

    _publish_state_image(_OTHER_SOURCE, _OTHER_DIGEST, release='model-b')
    mixed = task_lib.Task().set_resources([
        resources_lib.Resources(cloud='aws',
                                region='us-east-1',
                                container_image={
                                    'release': 'model-a',
                                    'distribution': 'managed',
                                }),
        resources_lib.Resources(cloud='aws',
                                region='us-west-2',
                                container_image={
                                    'release': 'model-b',
                                    'distribution': 'managed',
                                }),
    ])
    with pytest.raises(ValueError, match='same immutable container artifact'):
        serve_utils.snapshot_service_container_images(mixed)


def test_service_mixed_first_use_sources_leave_catalog_empty(
        image_state_engine, monkeypatch):
    _mock_registry_profile(monkeypatch)
    task = task_lib.Task().set_resources([
        resources_lib.Resources(cloud='aws',
                                region='us-east-1',
                                container_image={
                                    'ref': _SOURCE,
                                    'distribution': 'managed',
                                }),
        resources_lib.Resources(cloud='aws',
                                region='us-west-2',
                                container_image={
                                    'ref': _OTHER_SOURCE,
                                    'distribution': 'managed',
                                }),
    ])
    with pytest.raises(ValueError, match='same immutable container artifact'):
        serve_utils.snapshot_service_container_images(task)

    tables = (
        global_user_state.container_image_table,
        global_user_state.container_image_source_table,
        global_user_state.container_image_release_table,
        global_user_state.container_image_location_table,
        global_user_state.container_image_profile_revision_table,
    )
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert all(
            session.execute(table.select().limit(1)).first() is None
            for table in tables)


def test_managed_job_snapshot_survives_controller_serialization(
        image_state_engine, monkeypatch):
    del image_state_engine
    _mock_registry_profile(monkeypatch)
    first = _publish_state_image(_SOURCE, _DIGEST, release='job-a')
    state.bind_release(first.id, 'research', 'job-a-alias')
    second = _publish_state_image(_OTHER_SOURCE, _OTHER_DIGEST, release='job-b')

    first_task = task_lib.Task(run='echo first').set_resources([
        resources_lib.Resources(container_image={
            'release': release,
            'distribution': 'managed',
        }) for release in ('job-a', 'job-a-alias')
    ])
    second_task = task_lib.Task(run='echo second').set_resources([
        resources_lib.Resources(container_image={
            'release': 'job-b',
            'distribution': 'managed',
        })
    ])
    dag = dag_lib.Dag()
    dag.add(first_task)
    dag.add(second_task)
    dag.add_edge(first_task, second_task)
    container_image_task_utils.snapshot_task_container_images(
        dag.tasks, 'research')

    restored = dag_utils.load_chain_dag_from_yaml_str(
        dag_utils.dump_chain_dag_to_yaml_str(dag))
    restored_ids = [{
        resource.container_image.artifact_id for resource in task.resources
    } for task in restored.tasks]
    assert restored_ids == [{first.id}, {second.id}]


def test_service_same_digest_sources_publish_one_atomic_snapshot(
        image_state_engine, monkeypatch):
    del image_state_engine
    _mock_registry_profile(monkeypatch)
    mirror = f'mirror.example.com/boltz@{_DIGEST}'
    task = task_lib.Task().set_resources([
        resources_lib.Resources(cloud='aws',
                                region='us-east-1',
                                container_image={
                                    'ref': _SOURCE,
                                    'release': 'model-primary',
                                    'distribution': 'managed',
                                }),
        resources_lib.Resources(cloud='aws',
                                region='us-west-2',
                                container_image={
                                    'ref': mirror,
                                    'release': 'model-mirror',
                                    'distribution': 'managed',
                                }),
    ])

    artifact_id = serve_utils.snapshot_service_container_images(task)
    assert artifact_id is not None
    assert {
        resource.container_image.artifact_id for resource in task.resources
    } == {artifact_id}
    assert {
        source.source_ref
        for source in state.list_sources(artifact_id, 'research')
    } == {_SOURCE, mirror}
    assert {
        release.name for release in state.list_releases(artifact_id, 'research')
    } == {'model-primary', 'model-mirror'}
    assert len(state.list_locations(artifact_id, 'managed')) == 1


def test_atomic_publication_batch_rolls_back_every_candidate(
        image_state_engine):
    profile = _profile()

    def publication(source_ref: str, digest: str) -> state.ImagePublication:
        canonical = profile.canonical
        return state.ImagePublication(
            source_ref=source_ref,
            resolved_source_ref=source_ref,
            source_digest=digest,
            workspace='research',
            creator_user_hash='user-1',
            release='conflicting-release',
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

    tables = (
        global_user_state.container_image_table,
        global_user_state.container_image_source_table,
        global_user_state.container_image_release_table,
        global_user_state.container_image_location_table,
        global_user_state.container_image_profile_revision_table,
    )
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert all(
            session.execute(table.select().limit(1)).first() is None
            for table in tables)


def test_ecr_aws_cli_bootstrap_selects_host_architecture():
    command = docker_utils.INSTALL_AWS_CLI_CMD
    assert 'uname -m' in command
    assert 'x86_64|amd64' in command
    assert 'aarch64|arm64' in command
    assert 'awscli-exe-linux-${AWS_CLI_ARCH}.zip' in command


def test_provider_runtime_auth_is_placement_scoped():
    profile = _profile()
    aws = providers.get_adapter('aws')
    vm = models.Placement(provider='aws', region='us-east-1', backend='vm')
    strategy = aws.resolve_runtime_pull_auth(profile.canonical, vm)
    assert strategy == 'ecr_runtime_identity'
    login = aws.runtime_login_config(profile.canonical, strategy, vm)
    assert login.server == ('123456789012.dkr.ecr.us-east-1.amazonaws.com')
    assert login.password == ''

    kubernetes = models.Placement(provider='kubernetes',
                                  region='boltz-l4-fleet',
                                  backend='kubernetes')
    assert aws.resolve_runtime_pull_auth(profile.canonical, kubernetes) is None


@pytest.mark.parametrize('provider,region,registry', [
    ('generic', 'global', 'registry.example.com/team'),
    ('nebius', 'eu-north1', 'cr.eu-north1.nebius.cloud/team'),
    ('gcp', 'us-west1', 'us-west1-docker.pkg.dev/project/team'),
])
def test_exact_kubernetes_authority_precedes_anonymous_target_access(
        provider, region, registry):
    target = models.RegistryTarget(name='regional',
                                   provider=provider,
                                   region=region,
                                   registry=registry,
                                   pull_auth='anonymous')
    placement = models.Placement(provider='kubernetes',
                                 region='boltz-context',
                                 backend='kubernetes',
                                 registry_provider=provider,
                                 registry_region=region,
                                 registry_prefix=target.registry_prefix,
                                 registry_auth_strategy='node_identity')
    assert (providers.get_adapter(provider).resolve_runtime_pull_auth(
        target, placement) == 'kubernetes_context:node_identity')


@pytest.mark.parametrize('provider,region,target_kwargs', [
    ('aws', 'us-east-1', {
        'account': '123456789012',
        'pull_auth': 'ecr_runtime_identity',
    }),
    ('gcp', 'us-central1', {
        'project': 'research-project',
        'pull_auth': 'gar_runtime_identity',
    }),
    ('nebius', 'eu-north1', {
        'registry': 'cr.eu-north1.nebius.cloud/team',
        'pull_auth': 'anonymous',
    }),
    ('generic', 'global', {
        'registry': 'registry.example.com/team',
        'pull_auth': 'anonymous',
    }),
])
def test_exact_kubernetes_binding_beats_local_anonymous_sibling(
        image_state_engine, provider, region, target_kwargs):
    del image_state_engine
    bound = models.RegistryTarget(name='z-bound',
                                  provider=provider,
                                  region=region,
                                  **target_kwargs)
    anonymous = models.RegistryTarget(
        name='a-anonymous',
        provider='generic',
        region='global',
        registry='anonymous.example.com/cache',
        pull_auth='anonymous',
        localities=(models.RegistryLocality('kubernetes', 'boltz-context'),),
    )
    profile = models.RegistryProfile(
        name='managed',
        ownership=models.RegistryOwnership.EXTERNAL,
        realm='production',
        namespace='skypilot/{workspace}',
        require_digest_at_runtime=True,
        revision=1,
        canonical=models.RegistryTarget(name='canonical',
                                        provider='generic',
                                        region='global',
                                        registry='canonical.example.com/base',
                                        pull_auth='anonymous'),
        targets=(anonymous, bound),
    )
    placement = models.Placement(provider='kubernetes',
                                 region='boltz-context',
                                 backend='kubernetes',
                                 registry_provider=provider,
                                 registry_region=region,
                                 registry_prefix=bound.registry_prefix,
                                 registry_auth_strategy='node_identity')

    assert core._runtime_local_targets(profile, placement) == [bound]
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    core._ensure_for_placement(image, profile, placement)
    locations = state.list_locations(image.id, profile.name)
    assert {location.target_id for location in locations
           } == {'canonical', 'z-bound'}


def test_provider_registry_authority_matches_adapter():
    aws = providers.get_adapter('aws')
    aws.validate_target(
        models.RegistryTarget(
            name='ecr',
            provider='aws',
            region='us-east-1',
            account='123456789012',
            registry=('123456789012.dkr.ecr.us-east-1.amazonaws.com/team'),
            pull_auth='ecr_runtime_identity'))
    with pytest.raises(ValueError, match='exact ECR authority'):
        aws.validate_target(
            models.RegistryTarget(name='not-ecr',
                                  provider='aws',
                                  region='us-east-1',
                                  account='123456789012',
                                  registry='registry.example.com/team',
                                  pull_auth='ecr_runtime_identity'))

    gcp = providers.get_adapter('gcp')
    gcp.validate_target(
        models.RegistryTarget(
            name='gar',
            provider='gcp',
            region='us-central1',
            project='research-project',
            registry=('us-central1-docker.pkg.dev/research-project/team'),
            pull_auth='gar_runtime_identity'))
    with pytest.raises(ValueError, match='exact GAR project prefix'):
        gcp.validate_target(
            models.RegistryTarget(name='not-gar',
                                  provider='gcp',
                                  region='us-central1',
                                  project='research-project',
                                  registry='registry.example.com/team',
                                  pull_auth='gar_runtime_identity'))

    with pytest.raises(ValueError, match='unsupported pull-auth'):
        aws.validate_target(
            models.RegistryTarget(name='private-ecr',
                                  provider='aws',
                                  region='us-east-1',
                                  account='123456789012',
                                  pull_auth='anonymous'))
    with pytest.raises(ValueError, match='unsupported pull-auth'):
        models.RegistryTarget(name='kubelet',
                              provider='generic',
                              region='global',
                              registry='registry.example.com',
                              pull_auth='service_account_identity')


def test_generic_registry_locality_is_projected_into_runtime_routes(
        image_state_engine):
    del image_state_engine
    profile = models.RegistryProfile(
        name='managed',
        ownership=models.RegistryOwnership.EXTERNAL,
        realm='production',
        namespace='skypilot/{workspace}',
        require_digest_at_runtime=True,
        revision=1,
        canonical=models.RegistryTarget(name='canonical',
                                        provider='generic',
                                        region='global',
                                        registry='canonical.example.com',
                                        pull_auth='anonymous'),
        targets=(models.RegistryTarget(
            name='r2-edge',
            provider='generic',
            region='global',
            registry='r2.example.com/skypilot',
            pull_auth='anonymous',
            localities=(models.RegistryLocality('aws', 'us-west-2'),
                        models.RegistryLocality('nebius', 'eu-north1'))),),
    )
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    target = profile.target('r2-edge')
    regional = _ensure_profile_location(image, profile, target)
    regional_claim = state.claim_location(regional.id, 'copier', 30)
    assert regional_claim is not None
    assert regional_claim.lease_owner is not None
    regional_ref = references.managed_reference(profile, target, 'research',
                                                _SOURCE, _DIGEST)
    assert _complete_location(regional.id, regional_claim.lease_owner,
                              regional_ref, _DIGEST)
    image = state.get_image(image.id, 'research')
    assert image is not None

    for provider, region in (('aws', 'us-west-2'), ('nebius', 'eu-north1')):
        placement = models.Placement(provider=provider,
                                     region=region,
                                     backend='vm',
                                     platform='linux/amd64')
        routes = core.routes_for_image(image, profile, placement)
        selected = resolver.resolve(placement, routes, models.Locality.REQUIRE)
        assert selected.target_id == 'r2-edge'
        assert selected.reference == regional_ref


def test_transient_registry_credentials_cannot_be_logged_or_pickled():
    credentials = providers.TransientRegistryCredentials(
        username='worker',
        password='top-secret',
        server='registry.example.com',
        expires_at=100,
    )
    rendered = repr(credentials)
    assert 'worker' not in rendered
    assert 'top-secret' not in rendered
    with pytest.raises(TypeError, match='must not be pickled'):
        pickle.dumps(credentials)


def test_resolve_for_placement_pins_route_and_runtime_auth(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile_config = {
        'ownership': 'external',
        'realm': 'production',
        'organization': 'boltz',
        'namespace': 'skypilot/{organization}/{workspace}',
        'canonical': {
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-east-1',
            'pull_auth': 'ecr_runtime_identity',
        },
        'targets': [{
            'name': 'aws-us-west-2',
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-west-2',
            'pull_auth': 'ecr_runtime_identity',
        }],
    }
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'workspace-default',
                'profiles': {
                    'managed': profile_config,
                    'workspace-default': profile_config,
                },
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_required',
                        'default_profile': 'workspace-default',
                        'allowed_profiles': ['managed', 'workspace-default'],
                        'locality': 'prefer',
                    },
                },
            },
        })
    active_profile, _ = config.resolve_profile('managed', 'research')
    assert active_profile is not None
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         active_profile,
                                         active_profile.canonical,
                                         canonical=True)
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    canonical_token = canonical_claim.lease_owner
    assert canonical_token is not None
    canonical_ref = references.managed_reference(active_profile,
                                                 active_profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_token, canonical_ref,
                              _DIGEST, ('linux/amd64',), 100)
    regional_target = active_profile.target('aws-us-west-2')
    regional = _ensure_profile_location(image, active_profile, regional_target)
    regional_claim = state.claim_location(regional.id, 'copier', 30)
    assert regional_claim is not None
    regional_token = regional_claim.lease_owner
    assert regional_token is not None
    regional_ref = references.managed_reference(active_profile, regional_target,
                                                'research', _SOURCE, _DIGEST)
    assert _complete_location(regional.id, regional_token, regional_ref,
                              _DIGEST)

    requested = resources_lib.Resources(cloud='aws',
                                        region='us-west-2',
                                        accelerators={'L4': 4},
                                        container_image={
                                            'ref': _SOURCE,
                                            'profile': 'managed',
                                        })
    placement = models.Placement(provider='aws',
                                 region='us-west-2',
                                 backend='vm',
                                 platform='linux/amd64')
    resolved = core.resolve_for_placement(requested, placement, 'research')
    assert resolved.resolved_container_image.target_id == 'aws-us-west-2'
    assert resolved.accelerators == {'L4': 4}
    # Registry resolution is node-scoped: one immutable pull plan is shared by
    # the node runtime regardless of GPU cardinality. Per-GPU processes or EKS
    # pods remain an execution-layer concern.
    assert isinstance(resolved.resolved_container_image,
                      models.ResolvedContainerImage)
    assert resolved.extract_docker_image() == regional_ref
    assert resolved.docker_login_config.server == (
        '123456789012.dkr.ecr.us-west-2.amazonaws.com')
    assert resolved.is_image_managed is None

    serialized = resolved.to_yaml_config()
    assert serialized['_resolved_container_image'] == (
        resolved.resolved_container_image.to_dict())
    common_utils.validate_schema(serialized, schemas.get_resources_schema(),
                                 'Invalid internal resources state: ')
    with pytest.raises(ValueError, match='server-managed launch state'):
        resources_lib.Resources.from_yaml_config(serialized.copy())
    reloaded = resources_lib.Resources._from_yaml_config_single(  # pylint: disable=protected-access
        serialized.copy(),
        _allow_resolved_container_image=True)
    assert reloaded.resolved_container_image == (
        resolved.resolved_container_image)
    assert reloaded.extract_docker_image() == regional_ref

    # Reusing a target name for a changed endpoint disables that location;
    # prefer can still use a safely authenticated canonical fallback.
    profile_config['targets'][0]['region'] = 'us-west-1'
    profile_config['revision'] = 2
    fallback = core.resolve_for_placement(requested, placement, 'research')
    assert fallback.resolved_container_image.target_id == 'canonical'

    # A pinned launch plan is refreshed against later profile edits. Reusing
    # the stale physical route would preserve its old runtime authorization.
    refreshed = core.resolve_for_placement(resolved, placement, 'research')
    assert refreshed is not resolved
    assert refreshed.resolved_container_image.target_id == 'canonical'
    assert refreshed.resolved_container_image.profile_revision == 2
    state.mark_location_missing(regional.id)
    recovered = core.resolve_for_placement(resolved, placement, 'research')
    assert recovered.resolved_container_image.target_id == 'canonical'
    assert recovered.resolved_container_image.profile_revision == 2


def test_locality_rotation_refreshes_pins_and_fences_cluster_commit(
        image_state_engine, monkeypatch):
    config_data = {
        'container_registries': {
            'default_profile': 'managed',
            'profiles': {
                'managed': _profile_config(),
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
    _mock_config(monkeypatch, config_data)
    profile, _ = config.resolve_profile('managed', 'research')
    assert profile is not None
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    canonical_claim = state.claim_location(canonical.id, 'canonical-worker', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    requested = resources_lib.Resources(cloud='aws',
                                        region='us-west-2',
                                        container_image={
                                            'ref': _SOURCE,
                                            'distribution': 'managed',
                                        })
    placement = models.Placement(provider='aws',
                                 region='us-west-2',
                                 backend='vm')
    canonical_pin = core.resolve_for_placement(requested, placement, 'research')
    assert canonical_pin.resolved_container_image.target_id == 'canonical'

    policy = config_data['workspaces']['research']['container_images']
    policy['locality'] = 'require'
    with pytest.raises(resolver.ImageRouteUnavailableError, match='locality'):
        core.resolve_for_placement(canonical_pin, placement, 'research')
    with pytest.raises(ValueError, match='locality policy'):
        global_user_state.add_or_update_cluster(
            'stale-canonical-locality',
            types.SimpleNamespace(launched_resources=canonical_pin,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)

    regional_target = profile.target('aws-us-west-2')
    regional = _ensure_profile_location(image, profile, regional_target)
    regional_claim = state.claim_location(regional.id, 'regional-worker', 30)
    assert regional_claim is not None and regional_claim.lease_owner is not None
    regional_ref = references.managed_reference(profile, regional_target,
                                                'research', _SOURCE, _DIGEST)
    assert _complete_location(regional.id, regional_claim.lease_owner,
                              regional_ref, _DIGEST)
    regional_pin = core.resolve_for_placement(requested, placement, 'research')
    assert regional_pin.resolved_container_image.target_id == 'aws-us-west-2'

    policy['locality'] = 'canonical'
    refreshed = core.resolve_for_placement(regional_pin, placement, 'research')
    assert refreshed.resolved_container_image.target_id == 'canonical'
    with pytest.raises(ValueError, match='locality policy'):
        global_user_state.add_or_update_cluster(
            'stale-regional-locality',
            types.SimpleNamespace(launched_resources=regional_pin,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)
    global_user_state.add_or_update_cluster('current-canonical-locality',
                                            types.SimpleNamespace(
                                                launched_resources=refreshed,
                                                launched_nodes=1),
                                            requested_resources=None,
                                            ready=False)

    with sqlalchemy.orm.Session(image_state_engine) as session:
        names = set(
            session.execute(
                sqlalchemy.select(global_user_state.cluster_table.c.name).where(
                    global_user_state.cluster_table.c.name.in_([
                        'stale-canonical-locality', 'stale-regional-locality',
                        'current-canonical-locality'
                    ]))).scalars().all())
    assert names == {'current-canonical-locality'}


def test_policy_rotation_refreshes_plan_and_rejects_stale_cluster_handle(
        image_state_engine, monkeypatch):
    profile_config = {
        'ownership': 'external',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-east-1',
            'pull_auth': 'ecr_runtime_identity',
        },
    }
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': profile_config,
                },
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_required',
                        'locality': 'prefer',
                    },
                },
            },
        })
    profile, _ = config.resolve_profile('managed', 'research')
    assert profile is not None
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    claim = state.claim_location(canonical.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, claim.lease_owner, canonical_ref,
                              _DIGEST)

    placement = models.Placement(provider='aws',
                                 region='us-east-1',
                                 backend='vm')
    requested = resources_lib.Resources(cloud='aws',
                                        region='us-east-1',
                                        container_image={
                                            'ref': _SOURCE,
                                            'distribution': 'managed',
                                        })
    pinned = core.resolve_for_placement(requested, placement, 'research')
    stale_plan = pinned.resolved_container_image
    assert stale_plan is not None
    assert stale_plan.profile_revision == 1
    assert stale_plan.auth_strategy == 'ecr_runtime_identity'
    assert pinned.docker_login_config is not None

    # Location fields alone are not enough: the final commit independently
    # recomputes placement auth and logical selector identity. A caller cannot
    # combine a current policy fingerprint with a different allowed strategy.
    forged_auth_plan = models.ResolvedContainerImage(**{
        **stale_plan.__dict__,
        'auth_strategy': 'anonymous',
    })
    forged_auth = pinned.copy(_resolved_container_image=forged_auth_plan,
                              _docker_login_config=None)
    with pytest.raises(ValueError, match='runtime pull authority'):
        global_user_state.add_or_update_cluster(
            'forged-auth-handle',
            types.SimpleNamespace(launched_resources=forged_auth,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)

    forged_login = pinned.copy(_docker_login_config=None)
    with pytest.raises(ValueError, match='runtime login instruction'):
        global_user_state.add_or_update_cluster(
            'forged-login-handle',
            types.SimpleNamespace(launched_resources=forged_login,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)

    forged_selector = pinned.copy(container_image={
        'release': 'not-this-artifact',
        'distribution': 'managed',
    },
                                  _resolved_container_image=stale_plan)
    with pytest.raises(ValueError, match='release is not bound'):
        global_user_state.add_or_update_cluster(
            'forged-selector-handle',
            types.SimpleNamespace(launched_resources=forged_selector,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        names = session.execute(
            sqlalchemy.select(global_user_state.cluster_table.c.name).where(
                global_user_state.cluster_table.c.name.in_([
                    'forged-auth-handle', 'forged-login-handle',
                    'forged-selector-handle'
                ]))).all()
        assert names == []

    # The bytes and physical location stay unchanged while management policy
    # rotates. A stopped handle must adopt the new policy snapshot, not keep
    # the serialized revision-one policy fingerprint.
    profile_config['canonical']['manager_identity'] = 'image-manager-v2'
    profile_config['revision'] = 2
    refreshed = core.resolve_for_placement(pinned, placement, 'research')
    refreshed_plan = refreshed.resolved_container_image
    assert refreshed_plan is not None
    assert refreshed_plan.location_id == stale_plan.location_id
    assert refreshed_plan.reference == stale_plan.reference
    assert refreshed_plan.profile_revision == 2
    assert refreshed_plan.policy_fingerprint != stale_plan.policy_fingerprint
    assert refreshed_plan.auth_strategy == 'ecr_runtime_identity'
    assert refreshed.docker_login_config is not None

    stale_handle = types.SimpleNamespace(launched_resources=pinned,
                                         launched_nodes=1)
    with pytest.raises(ValueError, match='policy snapshot'):
        global_user_state.add_or_update_cluster('stale-policy-handle',
                                                stale_handle,
                                                requested_resources=None,
                                                ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'stale-policy-handle')).first() is None

    refreshed_handle = types.SimpleNamespace(launched_resources=refreshed,
                                             launched_nodes=1)
    global_user_state.add_or_update_cluster('current-policy-handle',
                                            refreshed_handle,
                                            requested_resources=None,
                                            ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        reference = session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'current-policy-handle')).mappings().one()
        assert reference['location_id'] == refreshed_plan.location_id

    # The status-refresh fast path is valid only for the exact plan already in
    # the durable handle, not merely for another plan naming the same location.
    forged_refresh_plan = models.ResolvedContainerImage(**{
        **refreshed_plan.__dict__,
        'auth_strategy': 'anonymous',
    })
    forged_refresh = refreshed.copy(
        _resolved_container_image=forged_refresh_plan)
    with pytest.raises(ValueError, match='runtime pull authority'):
        global_user_state.add_or_update_cluster(
            'current-policy-handle',
            types.SimpleNamespace(launched_resources=forged_refresh,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False,
            is_launch=False)
    forged_selector_refresh = refreshed.copy(
        container_image={
            'release': 'not-this-artifact',
            'distribution': 'managed',
        },
        _resolved_container_image=refreshed_plan)
    with pytest.raises(ValueError, match='release is not bound'):
        global_user_state.add_or_update_cluster(
            'current-policy-handle',
            types.SimpleNamespace(launched_resources=forged_selector_refresh,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False,
            is_launch=False)
    forged_login_refresh = refreshed.copy(
        _docker_login_config=docker_utils.DockerLoginConfig(
            username='', password='', server='unexpected.example'))
    with pytest.raises(ValueError, match='runtime login instruction'):
        global_user_state.add_or_update_cluster(
            'current-policy-handle',
            types.SimpleNamespace(launched_resources=forged_login_refresh,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False,
            is_launch=False)
    stored = global_user_state.get_handle_from_cluster_name(
        'current-policy-handle')
    assert (stored.launched_resources.resolved_container_image.auth_strategy ==
            'ecr_runtime_identity')


def test_cluster_commit_rejects_inconsistent_target_fingerprint(
        image_state_engine, monkeypatch):
    _mock_registry_profile(monkeypatch)
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    incorrect_fingerprint = '0' * 64
    assert incorrect_fingerprint != profile.physical_fingerprint(
        profile.canonical)
    location = state.ensure_location(
        image.id,
        profile.name,
        profile.canonical.name,
        incorrect_fingerprint,
        image.source_digest,
        policy_fingerprint=profile.policy_fingerprint(profile.canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical=True)
    claim = state.claim_location(location.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    target_ref = references.managed_reference(profile, profile.canonical,
                                              'research', _SOURCE, _DIGEST)
    assert _complete_location(location.id, claim.lease_owner, target_ref,
                              _DIGEST)

    resolved = _resolved_location(image, location, target_ref)
    launched = resources_lib.Resources(
        cloud='aws',
        region='us-east-1',
        container_image={
            'ref': _SOURCE,
            'distribution': profile.name,
        },
        _resolved_container_image=resolved,
        _docker_login_config=_ecr_runtime_login(target_ref))
    with pytest.raises(ValueError, match='physical registry destination'):
        global_user_state.add_or_update_cluster('inconsistent-physical-target',
                                                types.SimpleNamespace(
                                                    launched_resources=launched,
                                                    launched_nodes=1),
                                                requested_resources=None,
                                                ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'inconsistent-physical-target')).first() is None
        assert session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'inconsistent-physical-target')).first() is None


def test_ready_transition_keeps_init_plan_across_policy_rotation(
        image_state_engine, monkeypatch):
    profile_config = {
        'ownership': 'external',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-east-1',
            'pull_auth': 'ecr_runtime_identity',
        },
    }
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': profile_config,
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
        })
    profile_one, _ = config.resolve_profile('managed', 'research')
    assert profile_one is not None
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    location_one = _ensure_profile_location(image,
                                            profile_one,
                                            profile_one.canonical,
                                            canonical=True)
    claim = state.claim_location(location_one.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    reference_one = references.managed_reference(profile_one,
                                                 profile_one.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(location_one.id, claim.lease_owner, reference_one,
                              _DIGEST)
    placement = models.Placement(provider='aws',
                                 region='us-east-1',
                                 backend='vm')
    launched = core.resolve_for_placement(
        resources_lib.Resources(cloud='aws',
                                region='us-east-1',
                                container_image={
                                    'ref': _SOURCE,
                                    'distribution': 'managed',
                                }), placement, 'research')
    init_plan = launched.resolved_container_image
    assert init_plan is not None
    handle = types.SimpleNamespace(launched_resources=launched,
                                   launched_nodes=1)
    global_user_state.add_or_update_cluster('policy-rotates-during-provision',
                                            handle,
                                            requested_resources=None,
                                            ready=False)

    # A new physical target becomes current after the INIT handle and its
    # reference were committed, but before the already-rendered runtime is UP.
    profile_config['canonical']['account'] = '999999999999'
    profile_config['revision'] = 2
    profile_two, _ = config.resolve_profile('managed', 'research')
    assert profile_two is not None
    location_two = _ensure_profile_location(image,
                                            profile_two,
                                            profile_two.canonical,
                                            canonical=True)
    assert location_two.id != location_one.id
    assert state.profile_revision_matches('research', 'managed', 2,
                                          profile_two.revision_fingerprint)

    # READY is a continuation of the INIT launch, not a new resolution. It
    # keeps the exact plan used by the runtime and its durable eviction fence.
    global_user_state.add_or_update_cluster('policy-rotates-during-provision',
                                            handle,
                                            requested_resources=None,
                                            ready=True)
    stored = global_user_state.get_handle_from_cluster_name(
        'policy-rotates-during-provision')
    assert stored.launched_resources.resolved_container_image == init_plan
    with sqlalchemy.orm.Session(image_state_engine) as session:
        reference = session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'policy-rotates-during-provision')).mappings().one()
        assert reference['location_id'] == location_one.id
        status = session.execute(
            sqlalchemy.select(global_user_state.cluster_table.c.status).where(
                global_user_state.cluster_table.c.name ==
                'policy-rotates-during-provision')).scalar_one()
        assert status == 'UP'


def test_cluster_commit_rejects_nondeterministic_target_reference(
        image_state_engine, monkeypatch):
    _mock_registry_profile(monkeypatch)
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    location = _ensure_profile_location(image,
                                        profile,
                                        profile.canonical,
                                        canonical=True)
    claim = state.claim_location(location.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    unexpected_ref = f'unexpected.example/substitute@{_DIGEST}'
    assert _complete_location(location.id, claim.lease_owner, unexpected_ref,
                              _DIGEST)
    placement = models.Placement(provider='aws',
                                 region='us-east-1',
                                 backend='vm')
    assert not core.routes_for_image(image, profile, placement)

    resolved = _resolved_location(image, location, unexpected_ref)
    expected_ref = references.managed_reference(profile, profile.canonical,
                                                'research', _SOURCE, _DIGEST)
    launched = resources_lib.Resources(
        cloud='aws',
        region='us-east-1',
        container_image={
            'ref': _SOURCE,
            'distribution': profile.name,
        },
        _resolved_container_image=resolved,
        _docker_login_config=_ecr_runtime_login(expected_ref))
    with pytest.raises(ValueError, match='managed registry destination'):
        global_user_state.add_or_update_cluster('inconsistent-target-reference',
                                                types.SimpleNamespace(
                                                    launched_resources=launched,
                                                    launched_nodes=1),
                                                requested_resources=None,
                                                ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'inconsistent-target-reference')).first() is None
        assert session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'inconsistent-target-reference')).first() is None


def test_ensure_on_use_falls_back_while_materializations_warm(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile_config = {
        'ownership': 'external',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-east-1',
            'pull_auth': 'ecr_runtime_identity',
        },
        'targets': [{
            'name': 'aws-us-west-2',
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-west-2',
            'pull_auth': 'ecr_runtime_identity',
        }],
    }
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': profile_config,
                },
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_preferred',
                        'locality': 'prefer',
                    },
                },
            },
        })
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        requested = resources_lib.Resources(cloud=clouds.AWS(),
                                            region='us-west-2',
                                            instance_type='m6i.large',
                                            container_image={
                                                'ref': _SOURCE,
                                                'release': 'boltz-2.1.0',
                                            })
        resolved = core.resolve_for_placement(
            requested,
            models.Placement(provider='aws',
                             region='us-west-2',
                             backend='vm',
                             platform='linux/amd64'), 'research')

    pull_plan = resolved.resolved_container_image
    assert pull_plan is not None
    assert pull_plan.reference == _SOURCE
    assert pull_plan.target_id == 'source'
    assert pull_plan.status == 'WARMING'
    assert pull_plan.fallback_reason == (
        models.ImageFallbackReason.MANAGED_ROUTE_WARMING.value)
    assert pull_plan.location_id is None
    artifact = state.get_image_by_release('boltz-2.1.0', 'research')
    assert artifact is not None
    assert artifact.id == pull_plan.image_id
    assert {(location.target_id, location.state)
            for location in state.list_locations(artifact.id)} == {
                ('canonical', models.ImageLocationState.PENDING),
                ('aws-us-west-2', models.ImageLocationState.PENDING),
            }
    with mock.patch.object(clouds.AWS,
                           'get_arch_from_instance_type',
                           return_value='x86_64'):
        global_user_state.add_or_update_cluster('warming-unverified-platform',
                                                types.SimpleNamespace(
                                                    launched_resources=resolved,
                                                    launched_nodes=1),
                                                requested_resources=None,
                                                ready=False)

    canonical = next(location for location in state.list_locations(artifact.id)
                     if location.canonical)
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    profile, _ = config.resolve_profile('managed', 'research')
    assert profile is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    regional = next(location for location in state.list_locations(artifact.id)
                    if not location.canonical)
    regional_claim = state.claim_location(regional.id, 'copier', 30)
    assert regional_claim is not None
    assert regional_claim.lease_owner is not None
    regional_target = profile.target('aws-us-west-2')
    regional_ref = references.managed_reference(profile, regional_target,
                                                'research', _SOURCE, _DIGEST)
    assert _complete_location(regional.id, regional_claim.lease_owner,
                              regional_ref, _DIGEST)

    upgraded = core.resolve_for_placement(
        resolved,
        models.Placement(provider='aws',
                         region='us-west-2',
                         backend='vm',
                         platform='linux/amd64'), 'research')
    assert upgraded.resolved_container_image is not None
    assert upgraded.resolved_container_image.location_id == regional.id
    assert upgraded.resolved_container_image.status == 'READY'
    assert upgraded.extract_docker_image() == regional_ref


@pytest.mark.parametrize(
    'source,provider,region,auth_strategy,server',
    [
        (f'999999999999.dkr.ecr.us-west-2.amazonaws.com/boltz@{_DIGEST}', 'aws',
         'us-west-2', 'ecr_runtime_identity',
         '999999999999.dkr.ecr.us-west-2.amazonaws.com'),
        (f'us-central1-docker.pkg.dev/source-project/models/boltz@{_DIGEST}',
         'gcp', 'us-central1', 'gar_runtime_identity',
         'us-central1-docker.pkg.dev'),
    ],
)
def test_private_cloud_source_fallback_pins_runtime_identity(
        image_state_engine, monkeypatch, source, provider, region,
        auth_strategy, server):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'external',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'generic',
                            'registry': 'registry.example.com',
                            'region': 'global',
                            'pull_auth': 'anonymous',
                        },
                    },
                },
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_preferred',
                        'locality': 'prefer',
                    },
                },
            },
        })
    requested = resources_lib.Resources(cloud=provider,
                                        region=region,
                                        container_image=source)
    placement = models.Placement(provider=provider, region=region, backend='vm')
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        resolved = core.resolve_for_placement(requested, placement, 'research')

    pull_plan = resolved.resolved_container_image
    assert pull_plan is not None
    assert pull_plan.reference == source
    assert pull_plan.auth_strategy == auth_strategy
    assert resolved.docker_login_config == docker_utils.DockerLoginConfig(
        username='', password='', server=server)
    global_user_state.add_or_update_cluster(
        f'{provider}-source-runtime-identity',
        types.SimpleNamespace(launched_resources=resolved, launched_nodes=1),
        requested_resources=None,
        ready=False)


@pytest.mark.parametrize(
    'provider,region,backend',
    [
        ('aws', 'us-west-2', 'vm'),
        ('kubernetes', 'boltz-k8s', 'kubernetes'),
    ],
)
def test_implicit_docker_hub_source_fallback_has_runtime_authority(
        image_state_engine, monkeypatch, provider, region, backend):
    del image_state_engine
    source = f'ubuntu@{_DIGEST}'
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'external',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'generic',
                            'registry': 'registry.example.com',
                            'region': 'global',
                            'pull_auth': 'anonymous',
                        },
                    },
                },
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_preferred',
                        'locality': 'prefer',
                    },
                },
            },
        })
    requested = resources_lib.Resources(cloud=provider,
                                        region=region,
                                        container_image=source)
    placement = models.Placement(provider=provider,
                                 region=region,
                                 backend=backend)
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        resolved = core.resolve_for_placement(requested, placement, 'research')

    assert models.reference_registry_authority(source, 'source') == 'docker.io'
    assert resolved.resolved_container_image is not None
    assert resolved.resolved_container_image.reference == source
    assert resolved.resolved_container_image.auth_strategy == 'source_config'
    assert resolved.docker_login_config is None
    global_user_state.add_or_update_cluster(
        f'{backend}-implicit-docker-hub-source',
        types.SimpleNamespace(launched_resources=resolved, launched_nodes=1),
        requested_resources=None,
        ready=False)


@pytest.mark.parametrize('registry_prefix',
                         ['docker.io', 'docker.io/privateorg'])
def test_implicit_docker_hub_source_matches_exact_kubernetes_binding(
        registry_prefix):
    source = f'privateorg/model@{_DIGEST}'
    placement = models.Placement(provider='kubernetes',
                                 region='private-context',
                                 backend='kubernetes',
                                 registry_provider='generic',
                                 registry_region='global',
                                 registry_prefix=registry_prefix,
                                 registry_auth_strategy='node_identity')
    auth, login = providers.resolve_source_runtime_pull_auth(
        source, placement, None)
    assert auth == 'kubernetes_context:node_identity'
    assert login is None


@pytest.mark.parametrize(
    'source,provider,region',
    [
        (f'999999999999.dkr.ecr.us-west-2.amazonaws.com/boltz@{_DIGEST}', 'gcp',
         'us-west1'),
        (f'us-central1-docker.pkg.dev/source-project/models/boltz@{_DIGEST}',
         'aws', 'us-east-1'),
    ],
)
def test_private_cloud_source_fallback_fails_closed_off_provider(
        image_state_engine, monkeypatch, source, provider, region):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'external',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'generic',
                            'registry': 'registry.example.com',
                            'region': 'global',
                            'pull_auth': 'anonymous',
                        },
                    },
                },
            },
        })
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        with pytest.raises(resolver.ImageRouteUnavailableError,
                           match='exact Kubernetes registry binding'):
            core.resolve_for_placement(
                resources_lib.Resources(cloud=provider,
                                        region=region,
                                        container_image=source),
                models.Placement(provider=provider, region=region,
                                 backend='vm'), 'research')


def test_private_source_fallback_uses_exact_kubernetes_binding(
        image_state_engine, monkeypatch):
    del image_state_engine
    authority = '999999999999.dkr.ecr.us-west-2.amazonaws.com'
    source = f'{authority}/team/boltz@{_DIGEST}'
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'kubernetes_contexts': {
                    'boltz-eks': {
                        'registry_provider': 'aws',
                        'registry_region': 'us-west-2',
                        'registry': f'{authority}/team',
                        'auth_strategy': 'node_identity',
                    },
                },
                'profiles': {
                    'managed': {
                        'ownership': 'external',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'generic',
                            'registry': 'registry.example.com',
                            'region': 'global',
                            'pull_auth': 'anonymous',
                        },
                    },
                },
            },
        })
    placement = models.Placement(provider='kubernetes',
                                 region='boltz-eks',
                                 backend='kubernetes',
                                 registry_provider='aws',
                                 registry_region='us-west-2',
                                 registry_prefix=f'{authority}/team',
                                 registry_auth_strategy='node_identity')
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        resolved = core.resolve_for_placement(
            resources_lib.Resources(cloud=clouds.Kubernetes(),
                                    region='boltz-eks',
                                    container_image=source), placement,
            'research')

    assert resolved.resolved_container_image is not None
    assert (resolved.resolved_container_image.auth_strategy ==
            'kubernetes_context:node_identity')
    assert resolved.docker_login_config is None
    global_user_state.add_or_update_cluster(
        'kubernetes-source-runtime-identity',
        types.SimpleNamespace(launched_resources=resolved, launched_nodes=1),
        requested_resources=None,
        ready=False)


def test_source_fallback_is_request_scoped_and_never_release_inferred(
        image_state_engine, monkeypatch):
    del image_state_engine
    config_data = {
        'container_registries': {
            'default_profile': 'managed',
            'profiles': {
                'managed': {
                    'ownership': 'managed',
                    'realm': 'production',
                    'namespace': 'skypilot/{workspace}',
                    'canonical': {
                        'provider': 'aws',
                        'account': '123456789012',
                        'region': 'us-east-1',
                        'pull_auth': 'ecr_runtime_identity',
                    },
                },
            },
        },
        'workspaces': {
            'research': {
                'container_images': {
                    'mode': 'managed_preferred',
                    'locality': 'prefer',
                },
            },
        },
    }
    _mock_config(monkeypatch, config_data)
    mirror = f'mirror.example.com/boltz@{_DIGEST}'
    state.register_image(_SOURCE,
                         _SOURCE,
                         _DIGEST,
                         'research',
                         'user-1',
                         release='boltz-2.1.0')
    state.register_image(mirror, mirror, _DIGEST, 'research', 'user-1')
    placement = models.Placement(provider='aws',
                                 region='us-west-2',
                                 backend='vm')
    resolved = core.resolve_for_placement(
        resources_lib.Resources(cloud='aws',
                                region='us-west-2',
                                container_image=mirror), placement, 'research')
    assert resolved.resolved_container_image.reference == mirror
    assert state.get_image_by_source_ref(mirror, 'research') is not None

    pull_plan = resolved.resolved_container_image
    assert pull_plan is not None
    global_user_state.add_or_update_cluster('valid-warming-fallback',
                                            types.SimpleNamespace(
                                                launched_resources=resolved,
                                                launched_nodes=1),
                                            requested_resources=None,
                                            ready=False)

    # The exact secret-free source plan committed during INIT remains valid
    # for that same launch even if policy rotates before the runtime reports
    # UP. A new cluster must still be checked against the new policy below.
    config_data['workspaces']['research']['container_images'][
        'mode'] = 'managed_required'
    global_user_state.add_or_update_cluster('valid-warming-fallback',
                                            types.SimpleNamespace(
                                                launched_resources=resolved,
                                                launched_nodes=1),
                                            requested_resources=None,
                                            ready=True)
    stored = global_user_state.get_handle_from_cluster_name(
        'valid-warming-fallback')
    assert stored.launched_resources.resolved_container_image == pull_plan
    config_data['workspaces']['research']['container_images'][
        'mode'] = 'managed_preferred'

    release_only = resolved.copy(container_image={
        'release': 'boltz-2.1.0',
        'distribution': 'managed',
    },
                                 _resolved_container_image=pull_plan)
    with pytest.raises(ValueError, match='requires the exact source'):
        global_user_state.add_or_update_cluster(
            'release-warming-fallback',
            types.SimpleNamespace(launched_resources=release_only,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)

    unresolved = resources_lib.Resources(cloud='aws',
                                         region='us-west-2',
                                         container_image={
                                             'ref': mirror,
                                             'distribution': 'managed',
                                         })
    with pytest.raises(ValueError, match='requires a resolved runtime'):
        global_user_state.add_or_update_cluster(
            'unresolved-managed-selector',
            types.SimpleNamespace(launched_resources=unresolved,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)

    mismatched_login = resolved.copy(
        _docker_login_config=docker_utils.DockerLoginConfig(
            username='', password='', server='unexpected.example'))
    with pytest.raises(ValueError, match='exact source registry'):
        global_user_state.add_or_update_cluster(
            'mismatched-warming-login',
            types.SimpleNamespace(launched_resources=mismatched_login,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)
    prefixed_login = resolved.copy(
        _docker_login_config=docker_utils.DockerLoginConfig(
            username='', password='', server='mirror.example.com/boltz'))
    with pytest.raises(ValueError, match='exact source registry authority'):
        global_user_state.add_or_update_cluster(
            'prefixed-warming-login',
            types.SimpleNamespace(launched_resources=prefixed_login,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)
    credentialed_login = resolved.copy()
    credentialed_login._docker_login_config = (docker_utils.DockerLoginConfig(
        username='user', password='do-not-store', server='mirror.example.com'))
    with pytest.raises(ValueError, match='inline Docker'):
        global_user_state.add_or_update_cluster(
            'credentialed-warming-login',
            types.SimpleNamespace(launched_resources=credentialed_login,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)

    direct = resources_lib.Resources(cloud='aws',
                                     region='us-west-2',
                                     container_image={
                                         'ref': mirror,
                                         'distribution': 'direct',
                                     })
    global_user_state.add_or_update_cluster('valid-direct-source',
                                            types.SimpleNamespace(
                                                launched_resources=direct,
                                                launched_nodes=1),
                                            requested_resources=None,
                                            ready=False)

    with pytest.raises(resolver.ImageRouteUnavailableError,
                       match='do not authorize.*source fallback'):
        core.resolve_for_placement(
            resources_lib.Resources(container_image={
                'release': 'boltz-2.1.0',
            }), placement, 'research')

    config_data['workspaces']['research']['container_images'][
        'mode'] = 'managed_required'
    with pytest.raises(ValueError, match='no longer authorized'):
        global_user_state.add_or_update_cluster(
            'policy-rejected-warming-fallback',
            types.SimpleNamespace(launched_resources=resolved,
                                  launched_nodes=1),
            requested_resources=None,
            ready=False)


def test_private_source_fallback_never_persists_inline_credentials(
        image_state_engine, monkeypatch):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'managed',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'aws',
                            'account': '123456789012',
                            'region': 'us-east-1',
                            'pull_auth': 'ecr_runtime_identity',
                        },
                    },
                },
            },
        })
    requested = resources_lib.Resources(container_image=_SOURCE)
    # Bypass the constructor to preserve the resolver's defense-in-depth test.
    requested._docker_login_config = docker_utils.DockerLoginConfig(
        username='private-user', password='must-not-persist', server='ghcr.io')
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        with pytest.raises(ValueError, match='cannot persist inline'):
            core.resolve_for_placement(
                requested,
                models.Placement(provider='aws',
                                 region='us-west-2',
                                 backend='vm'), 'research')
    assert 'must-not-persist' not in repr(state.list_images('research'))


@pytest.mark.parametrize('locality', ['canonical', 'require'])
def test_strict_locality_never_uses_source_fallback(image_state_engine,
                                                    monkeypatch, locality):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'managed',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'aws',
                            'account': '123456789012',
                            'region': 'us-east-1',
                            'pull_auth': 'ecr_runtime_identity',
                        },
                    },
                },
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_preferred',
                        'locality': locality,
                    },
                },
            },
        })
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        with pytest.raises(resolver.ImageRouteUnavailableError):
            core.resolve_for_placement(
                resources_lib.Resources(container_image=_SOURCE),
                models.Placement(provider='aws',
                                 region='us-west-2',
                                 backend='vm'), 'research')


@pytest.mark.parametrize('controller_marker', [
    constants.IS_SKYPILOT_SERVE_CONTROLLER,
    constants.OVERRIDE_CONSOLIDATION_MODE,
])
def test_dedicated_controller_requires_exact_catalog_authority(
        image_state_engine, monkeypatch, controller_marker):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'managed',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'aws',
                            'account': '123456789012',
                            'region': 'us-east-1',
                            'pull_auth': 'ecr_runtime_identity',
                        },
                    },
                },
            },
        })
    monkeypatch.setenv(controller_marker, 'true')
    with pytest.raises(ValueError, match='missing its managed container image'):
        core.resolve_for_placement(
            resources_lib.Resources(container_image=_SOURCE),
            models.Placement(provider='aws', region='us-east-1', backend='vm'),
            'research')
    assert state.list_images('research') == []

    monkeypatch.setenv(constants.CONTAINER_IMAGE_CATALOG_AUTHORITY_ENV_VAR,
                       'api-catalog-id')
    monkeypatch.setattr(state, 'catalog_authority_matches', lambda _: False)
    with pytest.raises(ValueError, match='not connected'):
        core.resolve_for_placement(
            resources_lib.Resources(container_image=_SOURCE),
            models.Placement(provider='aws', region='us-east-1', backend='vm'),
            'research')
    assert state.list_images('research') == []


def test_pinned_route_checks_catalog_authority_before_location_lookup(
        image_state_engine, monkeypatch):
    del image_state_engine
    monkeypatch.setenv(constants.CONTAINER_IMAGE_CATALOG_AUTHORITY_ENV_VAR,
                       'api-catalog-id')
    monkeypatch.setattr(state, 'catalog_authority_matches', lambda _: False)
    pinned = resources_lib.Resources(
        container_image=_SOURCE,
        _resolved_container_image=models.ResolvedContainerImage(
            image_id=_ARTIFACT_ID,
            location_id=_LOCATION_ID,
            reference=f'registry.example.com/repo@{_DIGEST}',
            target_id='canonical',
            distribution='managed',
            profile_revision=1,
            policy_fingerprint=_POLICY_FINGERPRINT,
            digest=_DIGEST,
            auth_strategy='anonymous'))
    with mock.patch.object(state, 'get_location_by_id') as get_location:
        with pytest.raises(ValueError, match='not connected'):
            core.resolve_for_placement(
                pinned,
                models.Placement(provider='aws',
                                 region='us-east-1',
                                 backend='vm'), 'research')
    get_location.assert_not_called()


def test_dryrun_resolution_is_catalog_mutation_free(image_state_engine,
                                                    monkeypatch):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'external',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'generic',
                            'registry': 'registry.example.com',
                            'region': 'global',
                            'pull_auth': 'anonymous',
                        },
                    },
                },
            },
        })
    requested = resources_lib.Resources(container_image=_SOURCE)
    result = core.resolve_for_placement(requested,
                                        models.Placement(provider='aws',
                                                         region='us-east-1',
                                                         backend='vm'),
                                        'research',
                                        ensure=False)
    assert result is requested
    assert state.list_images('research') == []


def test_dryrun_fails_closed_for_managed_required(image_state_engine,
                                                  monkeypatch):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'external',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'generic',
                            'registry': 'registry.example.com',
                            'region': 'global',
                            'pull_auth': 'anonymous',
                        },
                    },
                },
            },
            'workspaces': {
                'research': {
                    'container_images': {
                        'mode': 'managed_required',
                    },
                },
            },
        })
    requested = resources_lib.Resources(container_image=_SOURCE)
    with pytest.raises(resolver.ImageRouteUnavailableError,
                       match='cannot create'):
        core.resolve_for_placement(requested,
                                   models.Placement(provider='aws',
                                                    region='us-east-1',
                                                    backend='vm'),
                                   'research',
                                   ensure=False)
    assert state.list_images('research') == []


def test_registry_default_preserves_legacy_docker_tasks(image_state_engine,
                                                        monkeypatch):
    del image_state_engine
    data = {
        'container_registries': {
            'default_profile': 'managed',
            'profiles': {
                'managed': {
                    'ownership': 'managed',
                    'realm': 'production',
                    'namespace': 'skypilot/{workspace}',
                    'canonical': {
                        'provider': 'aws',
                        'account': '123456789012',
                        'region': 'us-east-1',
                        'pull_auth': 'ecr_runtime_identity',
                    },
                },
            },
        },
        'workspaces': {
            'research': {
                'container_images': {
                    'mode': 'managed_preferred',
                },
            },
        },
    }
    _mock_config(monkeypatch, data)
    legacy = resources_lib.Resources(image_id='docker:ubuntu:22.04')
    resolved = core.resolve_for_placement(
        legacy,
        models.Placement(provider='aws', region='us-east-1', backend='vm'),
        'research')
    assert resolved is legacy
    assert state.list_images('research') == []

    data['workspaces']['research']['container_images'][
        'mode'] = 'managed_required'
    with pytest.raises(ValueError, match='Migrate the legacy image_id'):
        core.resolve_for_placement(
            legacy,
            models.Placement(provider='aws', region='us-east-1', backend='vm'),
            'research')


def test_catalog_authority_identifies_the_exact_database(monkeypatch):
    first_engine = sqlalchemy.create_engine('sqlite://')
    second_engine = sqlalchemy.create_engine('sqlite://')
    global_user_state.Base.metadata.create_all(first_engine)
    global_user_state.Base.metadata.create_all(second_engine)
    current_engine = [first_engine]
    monkeypatch.setattr(state, '_engine', lambda: current_engine[0])
    first_authority = state.get_catalog_authority_id()
    assert first_authority is not None
    assert state.catalog_authority_matches(first_authority)

    current_engine[0] = second_engine
    second_authority = state.get_catalog_authority_id()
    assert second_authority is not None
    assert second_authority != first_authority
    assert not state.catalog_authority_matches(first_authority)


def test_sqlite_concurrent_identical_publications_retry_and_converge(
        tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "concurrent-publication.db"}',
        connect_args={
            'check_same_thread': False,
            'timeout': 0.01,
        })
    global_user_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(state, '_engine', lambda: engine)

    blocker = engine.connect()
    blocker.exec_driver_sql('BEGIN IMMEDIATE')
    retry_count = 0
    retry_lock = threading.Lock()
    both_retried = threading.Event()
    resume_retries = threading.Event()

    def _controlled_retry_sleep(_delay):
        nonlocal retry_count
        with retry_lock:
            retry_count += 1
            if retry_count >= 2:
                both_retried.set()
        assert resume_retries.wait(timeout=10)

    monkeypatch.setattr(state.db_retries.time, 'sleep', _controlled_retry_sleep)
    start = threading.Barrier(2)
    results = []
    errors = []

    def _publish():
        try:
            start.wait(timeout=10)
            results.append(_publish_state_image(_SOURCE, _DIGEST))
        except Exception as error:  # pylint: disable=broad-except
            errors.append(error)

    threads = [threading.Thread(target=_publish) for _ in range(2)]
    for thread in threads:
        thread.start()
    assert both_retried.wait(timeout=10)
    blocker.rollback()
    blocker.close()
    resume_retries.set()
    for thread in threads:
        thread.join(timeout=20)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert len(results) == 2
    assert results[0].id == results[1].id
    assert len(state.list_images('research')) == 1
    assert len(state.list_sources(results[0].id, 'research')) == 1
    assert len(state.list_locations(results[0].id, 'managed')) == 1


def test_auth_rotation_does_not_change_materialization_identity():
    original = _profile()
    rotated = models.RegistryProfile(
        **{
            **original.__dict__,
            'canonical': models.RegistryTarget(
                **{
                    **original.canonical.__dict__,
                    'manager_identity': 'rotated-manager',
                    'pull_auth': 'anonymous',
                }),
        })
    assert original.fingerprint == rotated.fingerprint
    assert (original.materialization_fingerprint(
        original.canonical) == rotated.materialization_fingerprint(
            rotated.canonical))
    assert (original.policy_fingerprint(original.canonical, True)
            != rotated.policy_fingerprint(rotated.canonical, True))


def test_registry_endpoint_identity_is_canonical_and_policy_complete():
    normalized = models.RegistryTarget(
        name='cache',
        provider='generic',
        region='west',
        registry='REGISTRY.EXAMPLE.COM.:443/team/cache/',
        pull_auth='anonymous')
    equivalent = models.RegistryTarget(
        name='cache',
        provider='generic',
        region='west',
        registry='registry.example.com/team/cache',
        pull_auth='anonymous')
    assert normalized.registry == 'registry.example.com/team/cache'
    assert normalized.endpoint_identity == equivalent.endpoint_identity
    assert normalized.fingerprint == equivalent.fingerprint

    derived_aws = models.RegistryTarget(name='derived-aws',
                                        provider='AWS',
                                        region='US-EAST-1',
                                        account='123456789012')
    explicit_aws = models.RegistryTarget(
        name='explicit-aws',
        provider='generic',
        region='global',
        registry='123456789012.dkr.ecr.us-east-1.amazonaws.com:443')
    assert derived_aws.region == 'us-east-1'
    assert derived_aws.endpoint_identity == explicit_aws.endpoint_identity
    assert references.registry_endpoint(
        derived_aws) == references.registry_endpoint(explicit_aws)

    derived_gcp = models.RegistryTarget(name='derived-gcp',
                                        provider='GCP',
                                        region='US-CENTRAL1',
                                        project='research-project')
    explicit_gcp = models.RegistryTarget(
        name='explicit-gcp',
        provider='generic',
        region='global',
        registry='us-central1-docker.pkg.dev:443/research-project')
    assert derived_gcp.endpoint_identity == explicit_gcp.endpoint_identity
    assert references.registry_endpoint(
        derived_gcp) == references.registry_endpoint(explicit_gcp)

    profile = models.RegistryProfile(
        name='external',
        ownership=models.RegistryOwnership.EXTERNAL,
        realm='production',
        namespace='skypilot/{workspace}',
        require_digest_at_runtime=True,
        revision=1,
        canonical=normalized,
    )
    assert references.managed_reference(
        profile, normalized, 'research', _SOURCE,
        _DIGEST) == references.managed_reference(profile, equivalent,
                                                 'research', _SOURCE, _DIGEST)
    with pytest.raises(ValueError, match='lowercase OCI name components'):
        models.RegistryTarget(name='invalid',
                              provider='generic',
                              region='west',
                              registry='registry.example.com/Team/cache')
    with pytest.raises(ValueError, match='lowercase OCI name components'):
        models.RegistryTarget(name='invalid',
                              provider='generic',
                              region='west',
                              registry='registry.example.com/team//cache')
    with pytest.raises(ValueError, match='lowercase OCI name components'):
        models.RegistryTarget(name='invalid',
                              provider='generic',
                              region='west',
                              registry='registry.example.com/team/.cache')
    with pytest.raises(ValueError, match='invalid registry host'):
        models.RegistryTarget(name='invalid',
                              provider='generic',
                              region='west',
                              registry='registry.example.com..')

    aws_target = models.RegistryTarget(name='regional',
                                       provider='aws',
                                       region='us-east-1',
                                       account='123456789012',
                                       registry='mirror.example.com/team/cache',
                                       manager_identity='aws-writer',
                                       pull_auth='ecr_runtime_identity')
    gcp_target = models.RegistryTarget(name='regional',
                                       provider='gcp',
                                       region='us-central1',
                                       project='research-project',
                                       registry='mirror.example.com/team/cache',
                                       manager_identity='gcp-writer',
                                       pull_auth='gar_runtime_identity')
    canonical = models.RegistryTarget(name='canonical',
                                      provider='generic',
                                      region='global',
                                      registry='origin.example.com',
                                      pull_auth='anonymous')
    aws_profile = models.RegistryProfile(
        name='distribution',
        ownership=models.RegistryOwnership.EXTERNAL,
        realm='production',
        namespace='skypilot/{workspace}',
        require_digest_at_runtime=True,
        revision=1,
        canonical=canonical,
        targets=(aws_target,))
    gcp_profile = models.RegistryProfile(**{
        **aws_profile.__dict__,
        'targets': (gcp_target,),
    })
    assert (aws_profile.physical_fingerprint(aws_target) ==
            gcp_profile.physical_fingerprint(gcp_target))
    assert (aws_profile.policy_fingerprint(aws_target, False)
            != gcp_profile.policy_fingerprint(gcp_target, False))
    assert aws_profile.revision_fingerprint != gcp_profile.revision_fingerprint


def test_profile_revision_is_monotonic_and_config_bound(image_state_engine):
    del image_state_engine
    revision_one = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         revision_one,
                                         revision_one.canonical,
                                         canonical=True)
    target = revision_one.target('aws-us-west-2')
    regional = _ensure_profile_location(image,
                                        revision_one,
                                        target,
                                        auto_evict=True)
    revision_two = models.RegistryProfile(
        **{
            **revision_one.__dict__,
            'ownership': models.RegistryOwnership.EXTERNAL,
            'revision': 2,
        })
    active_claim = state.claim_location(canonical.id, 'revision-one-worker', 30)
    assert active_claim is not None
    assert active_claim.lease_owner is not None
    with pytest.raises(state.ProfileRevisionBusyError, match='active lease'):
        _ensure_profile_location(image,
                                 revision_two,
                                 revision_two.canonical,
                                 canonical=True)
    assert state.fail_location(
        canonical.id, active_claim.lease_owner,
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED)
    assert state.retry_location(canonical.id)
    updated_canonical = _ensure_profile_location(image,
                                                 revision_two,
                                                 revision_two.canonical,
                                                 canonical=True)
    updated_regional = _ensure_profile_location(
        image, revision_two, revision_two.target(target.name))
    assert updated_canonical.id == canonical.id
    assert updated_regional.id == regional.id
    assert updated_regional.profile_revision == 2
    assert not updated_regional.auto_evict
    catalog_revision = state.get_profile_revision('research', 'managed')
    assert catalog_revision is not None
    assert catalog_revision.revision == 2

    with pytest.raises(state.StaleProfileRevisionError, match='is stale'):
        _ensure_profile_location(image,
                                 revision_one,
                                 revision_one.canonical,
                                 canonical=True)
    stale_edit = models.RegistryProfile(**{
        **revision_two.__dict__,
        'realm': 'edited-without-a-bump',
    })
    with pytest.raises(ValueError, match='without incrementing its revision'):
        _ensure_profile_location(image,
                                 stale_edit,
                                 stale_edit.canonical,
                                 canonical=True)
    retained_revision = state.get_profile_revision('research', 'managed')
    retained_location = state.get_location_by_id(regional.id)
    assert retained_revision is not None
    assert retained_location is not None
    assert retained_revision.revision == 2
    assert retained_location.profile_revision == 2


def test_profile_activation_does_not_rewrite_dominant_location_population(
        image_state_engine):
    revision_one = _profile()
    revision_two = models.RegistryProfile(
        **{
            **revision_one.__dict__,
            'ownership': models.RegistryOwnership.EXTERNAL,
            'revision': 2,
        })
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         revision_one,
                                         revision_one.canonical,
                                         canonical=True)
    table = global_user_state.container_image_location_table
    untouched_id = str(uuid.uuid4())
    rows = []
    for index in range(1000):
        rows.append({
            'id': untouched_id if index == 0 else str(uuid.uuid4()),
            'workspace': 'research',
            'image_id': image.id,
            'profile': revision_one.name,
            'target_id': f'target-{index}',
            'target_fingerprint': hashlib.sha256(f'target-{index}'.encode()
                                                ).hexdigest(),
            'policy_fingerprint': hashlib.sha256(f'policy-{index}'.encode()
                                                ).hexdigest(),
            'profile_revision': 1,
            'canonical': False,
            'canonical_location_id': canonical.id,
            'canonical_ready': False,
            'source_id': None,
            'expected_digest': _DIGEST,
            'auto_evict': True,
            'state': models.ImageLocationState.PENDING.value,
            'attempt_count': 0,
            'updated_at': 1,
        })
    with image_state_engine.begin() as connection:
        connection.execute(table.insert(), rows)

    rowcounts = []

    def _capture_location_updates(conn, cursor, statement, parameters, context,
                                  executemany):
        del conn, parameters, context, executemany
        if statement.lstrip().upper().startswith(
                'UPDATE CONTAINER_IMAGE_LOCATIONS'):
            rowcounts.append(cursor.rowcount)

    sqlalchemy.event.listen(image_state_engine, 'after_cursor_execute',
                            _capture_location_updates)
    try:
        activated = _ensure_profile_location(image,
                                             revision_two,
                                             revision_two.canonical,
                                             canonical=True)
    finally:
        sqlalchemy.event.remove(image_state_engine, 'after_cursor_execute',
                                _capture_location_updates)
    assert activated.profile_revision == 2
    assert rowcounts
    assert max(rowcounts) <= 1
    with image_state_engine.connect() as connection:
        untouched = connection.execute(
            sqlalchemy.select(
                table.c.profile_revision,
                table.c.updated_at).where(table.c.id == untouched_id)).one()
    assert untouched == (1, 1)


def test_profile_activation_active_lease_probes_are_population_independent(
        image_state_engine):
    revision_one = _profile()
    revision_two = models.RegistryProfile(
        **{
            **revision_one.__dict__,
            'ownership': models.RegistryOwnership.EXTERNAL,
            'revision': 2,
        })
    image = _publish_state_image(_SOURCE,
                                 _DIGEST,
                                 release='activation-scale',
                                 profile=revision_one)
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    insert_sql = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(1)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < :row_count
        )
        INSERT INTO container_image_locations (
          id, workspace, image_id, profile, target_id, target_fingerprint,
          policy_fingerprint, profile_revision, canonical,
          canonical_location_id, canonical_ready, expected_digest, state,
          attempt_count, auto_evict, updated_at
        )
        SELECT 'activation-bulk-' || n, 'research', :image_id, 'managed',
               'target-' || n, 'activation-fingerprint-' || n,
               :policy_fingerprint, 1, 0, :canonical_id, 0, :digest,
               'PENDING', 0, 1, 1
        FROM synthetic
    """)
    with image_state_engine.begin() as connection:
        connection.execute(
            insert_sql, {
                'row_count': 200_000,
                'image_id': image.id,
                'policy_fingerprint': _POLICY_FINGERPRINT,
                'canonical_id': canonical.id,
                'digest': _DIGEST,
            })

    activated = None

    def activate() -> None:
        nonlocal activated
        activated = _ensure_profile_location(image,
                                             revision_two,
                                             revision_two.canonical,
                                             canonical=True)

    steps = _sqlite_vm_steps(image_state_engine, activate)
    assert activated is not None
    assert activated.profile_revision == 2
    assert steps < 100_000


def test_profile_revision_settles_expired_copy_after_canonical_loss(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    revision_one = _profile()
    revision_two = models.RegistryProfile(
        **{
            **revision_one.__dict__,
            'ownership': models.RegistryOwnership.EXTERNAL,
            'revision': 2,
        })
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         revision_one,
                                         revision_one.canonical,
                                         canonical=True)
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(revision_one,
                                                 revision_one.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    target = revision_one.target('aws-us-west-2')
    regional = _ensure_profile_location(image,
                                        revision_one,
                                        target,
                                        auto_evict=True)
    first_claim = state.claim_location(regional.id, 'crashed-worker', 30)
    assert first_claim is not None
    assert first_claim.lease_owner is not None
    assert state.retry_location(canonical.id)
    verification = state.claim_location_verification(canonical.id, 'verifier',
                                                     30)
    assert verification is not None
    assert verification.lease_owner is not None
    assert not state.complete_location_verification(
        canonical.id, verification.lease_owner, _OTHER_DIGEST)
    now[0] = 1031

    advanced = _ensure_profile_location(image,
                                        revision_two,
                                        revision_two.canonical,
                                        canonical=True)
    assert advanced.profile_revision == 2
    transferred = _ensure_profile_location(image, revision_two,
                                           revision_two.target(target.name))
    assert transferred.profile_revision == 2
    assert transferred.state == models.ImageLocationState.FAILED
    assert transferred.target_ref is None
    assert transferred.lease_owner is None
    assert transferred.next_retry_at == 1031
    assert not _complete_location(regional.id, first_claim.lease_owner,
                                  f'registry.example/repo@{_DIGEST}', _DIGEST)


def test_profile_revision_settles_expired_eviction_after_canonical_loss(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image, _ = _ready_regional_location()
    revision_one = _profile()
    revision_two = models.RegistryProfile(
        **{
            **revision_one.__dict__,
            'ownership': models.RegistryOwnership.EXTERNAL,
            'revision': 2,
        })
    regional = next(location for location in state.list_locations(image.id)
                    if not location.canonical)
    now[0] = 2000
    eviction = state.claim_location_eviction(regional.id,
                                             'crashed-evictor',
                                             30,
                                             unused_before=1001)
    assert eviction is not None
    assert eviction.lease_owner is not None
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    assert state.retry_location(canonical.id)
    verification = state.claim_location_verification(canonical.id, 'verifier',
                                                     30)
    assert verification is not None
    assert verification.lease_owner is not None
    assert not state.complete_location_verification(
        canonical.id, verification.lease_owner, _OTHER_DIGEST)
    now[0] = 2031

    advanced = _ensure_profile_location(image,
                                        revision_two,
                                        revision_two.canonical,
                                        canonical=True)
    assert advanced.profile_revision == 2
    transferred = _ensure_profile_location(image, revision_two,
                                           revision_two.target('aws-us-west-2'))
    assert transferred.profile_revision == 2
    assert transferred.state == models.ImageLocationState.MISSING
    assert transferred.target_ref is None
    assert transferred.lease_owner is None
    assert transferred.next_retry_at == 2031
    assert not state.complete_location_eviction(regional.id,
                                                eviction.lease_owner)


def test_profile_activation_fences_exact_expiry_and_malformed_copy(
        image_state_engine, monkeypatch):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    revision_one = _profile()
    revision_two = models.RegistryProfile(
        **{
            **revision_one.__dict__,
            'ownership': models.RegistryOwnership.EXTERNAL,
            'revision': 2,
        })
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         revision_one,
                                         revision_one.canonical,
                                         canonical=True)
    claim = state.claim_location(canonical.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    canonical_ref = references.managed_reference(revision_one,
                                                 revision_one.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, claim.lease_owner, canonical_ref,
                              _DIGEST)
    regional = _ensure_profile_location(image,
                                        revision_one,
                                        revision_one.target('aws-us-west-2'),
                                        auto_evict=True)
    inactive_image = state.register_image(_OTHER_SOURCE, _OTHER_SOURCE,
                                          _OTHER_DIGEST, 'research', 'user-1')
    _ensure_profile_location(inactive_image,
                             revision_one,
                             revision_one.canonical,
                             canonical=True)
    inactive_failed = _ensure_profile_location(
        inactive_image,
        revision_one,
        revision_one.target('aws-us-west-2'),
        auto_evict=True)
    assert state.retry_location(canonical.id)
    old_verification = state.claim_location_verification(
        canonical.id, 'revision-one-verifier', 30)
    assert old_verification is not None
    assert old_verification.lease_owner is not None
    table = global_user_state.container_image_location_table
    with image_state_engine.begin() as connection:
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        connection.execute(
            table.update().where(table.c.id == regional.id).values(
                state=models.ImageLocationState.COPYING.value,
                lease_owner=None,
                lease_expires_at=9999,
                heartbeat_at=1000))
        connection.execute(
            table.update().where(table.c.id == inactive_failed.id).values(
                state=models.ImageLocationState.FAILED.value,
                lease_owner='impossible-future-token',
                lease_expires_at=9999,
                heartbeat_at=1000))
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')

    now[0] = 1030
    advanced = _ensure_profile_location(image,
                                        revision_two,
                                        revision_two.canonical,
                                        canonical=True)
    transferred = _ensure_profile_location(image, revision_two,
                                           revision_two.target('aws-us-west-2'))
    assert advanced.profile_revision == 2
    assert advanced.state == models.ImageLocationState.READY
    assert advanced.lease_owner is None
    assert transferred.profile_revision == 2
    assert transferred.state == models.ImageLocationState.FAILED
    assert transferred.lease_owner is None
    fenced_inactive = state.get_location_by_id(inactive_failed.id)
    assert fenced_inactive is not None
    assert fenced_inactive.state == models.ImageLocationState.FAILED
    assert fenced_inactive.profile_revision == 1
    # Activation no longer rewrites every historical row. The old generation
    # is fenced immediately and an exact row is repaired only if revision two
    # later touches that physical destination.
    assert fenced_inactive.lease_owner == 'impossible-future-token'
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


def test_unchanged_generation_reclaims_structurally_incomplete_lease(
        image_state_engine, monkeypatch):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    first = state.claim_location(canonical.id, 'crashed-worker', 30)
    assert first is not None
    assert first.lease_owner is not None

    table = global_user_state.container_image_location_table
    with image_state_engine.begin() as connection:
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        connection.execute(table.update().where(
            table.c.id == canonical.id).values(lease_owner=None,
                                               lease_expires_at=9999,
                                               heartbeat_at=1000))
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')

    reclaimed = state.claim_location(canonical.id, 'repair-worker', 30)
    assert reclaimed is not None
    assert reclaimed.lease_owner is not None
    assert reclaimed.lease_owner != first.lease_owner
    assert reclaimed.lease_expires_at == 1030
    assert reclaimed.heartbeat_at == 1000


@pytest.mark.parametrize('transition', [
    'heartbeat',
    'complete_copy',
    'fail_copy',
    'complete_verification',
    'fail_verification',
    'complete_eviction',
    'fail_eviction',
])
def test_lease_transitions_recheck_expiry_after_all_row_locks(
        image_state_engine, monkeypatch, transition):
    del image_state_engine
    now = [100]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()

    if transition in ('heartbeat', 'complete_copy', 'fail_copy'):
        image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
        location = state.list_locations(image.id, profile.name)[0]
        claim = state.claim_location(location.id, 'copy-worker', 10)
        assert claim is not None
        target_ref = references.managed_reference(profile, profile.canonical,
                                                  'research', _SOURCE, _DIGEST)
    elif transition in ('complete_verification', 'fail_verification'):
        image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
        location = state.list_locations(image.id, profile.name)[0]
        initial = state.claim_location(location.id, 'initial-copy', 30)
        assert initial is not None and initial.lease_owner is not None
        target_ref = references.managed_reference(profile, profile.canonical,
                                                  'research', _SOURCE, _DIGEST)
        assert _complete_location(location.id, initial.lease_owner, target_ref,
                                  _DIGEST)
        assert state.retry_location(location.id)
        claim = state.claim_location_verification(location.id, 'verifier', 10)
        assert claim is not None
    else:
        image, target_ref = _ready_regional_location()
        location = next(item for item in state.list_locations(image.id)
                        if not item.canonical)
        now[0] += 8 * _WEEK_SECONDS + 1
        cutoff = now[0] - 8 * _WEEK_SECONDS
        claim = state.claim_location_eviction(location.id, 'evictor', 10,
                                              cutoff)
        assert claim is not None

    assert claim.lease_owner is not None
    assert claim.lease_expires_at is not None
    lease_token = claim.lease_owner
    lease_expires_at = claim.lease_expires_at
    now[0] = lease_expires_at - 1
    original_lock = state._lock_location_for_update

    def _lock_after_expiry(session, location_id):
        locked = original_lock(session, location_id)
        now[0] = lease_expires_at + 1
        return locked

    monkeypatch.setattr(state, '_lock_location_for_update', _lock_after_expiry)
    if transition == 'heartbeat':
        changed = state.heartbeat_location(location.id, lease_token, 10)
    elif transition == 'complete_copy':
        changed = _complete_location(location.id, lease_token, target_ref,
                                     _DIGEST)
    elif transition == 'fail_copy':
        changed = state.fail_location(
            location.id, lease_token,
            models.ImageLocationErrorCode.MATERIALIZATION_FAILED)
    elif transition == 'complete_verification':
        changed = state.complete_location_verification(location.id, lease_token,
                                                       _DIGEST)
    elif transition == 'fail_verification':
        changed = state.fail_location_verification(
            location.id, lease_token,
            models.ImageLocationErrorCode.REVALIDATION_FAILED)
    elif transition == 'complete_eviction':
        changed = state.complete_location_eviction(location.id, lease_token)
    else:
        assert transition == 'fail_eviction'
        changed = state.fail_location_eviction(
            location.id, lease_token,
            models.ImageLocationErrorCode.EVICTION_FAILED)

    assert not changed
    unchanged = state.get_location_by_id(location.id)
    assert unchanged is not None
    assert unchanged.lease_owner == lease_token
    assert unchanged.lease_expires_at == lease_expires_at


@pytest.mark.parametrize('claim_kind', ['copy', 'verification', 'eviction'])
def test_location_claims_start_lease_after_all_row_locks(
        image_state_engine, monkeypatch, claim_kind):
    del image_state_engine
    now = [100]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    if claim_kind in ('copy', 'verification'):
        image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
        location = state.list_locations(image.id, profile.name)[0]
        if claim_kind == 'verification':
            initial = state.claim_location(location.id, 'initial-copy', 30)
            assert initial is not None and initial.lease_owner is not None
            target_ref = references.managed_reference(profile,
                                                      profile.canonical,
                                                      'research', _SOURCE,
                                                      _DIGEST)
            assert _complete_location(location.id, initial.lease_owner,
                                      target_ref, _DIGEST)
            assert state.retry_location(location.id)
    else:
        image, _ = _ready_regional_location()
        location = next(item for item in state.list_locations(image.id)
                        if not item.canonical)
        now[0] += 8 * _WEEK_SECONDS + 1

    lock_time = now[0] + 5
    original_lock = state._lock_location_for_update

    def _delayed_lock(session, location_id):
        locked = original_lock(session, location_id)
        now[0] = lock_time
        return locked

    monkeypatch.setattr(state, '_lock_location_for_update', _delayed_lock)
    if claim_kind == 'copy':
        claim = state.claim_location(location.id, 'copy-worker', 10)
    elif claim_kind == 'verification':
        claim = state.claim_location_verification(location.id, 'verifier', 10)
    else:
        claim = state.claim_location_eviction(location.id, 'evictor', 10,
                                              now[0] - 8 * _WEEK_SECONDS)

    assert claim is not None
    assert claim.heartbeat_at == lock_time
    assert claim.lease_expires_at == lock_time + 10


def test_unchanged_generation_repairs_impossible_ready_ownership(
        image_state_engine, monkeypatch):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    claim = state.claim_location(canonical.id, 'ready-owner-fixture', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, claim.lease_owner, canonical_ref,
                              _DIGEST)
    table = global_user_state.container_image_location_table
    with sqlalchemy.orm.Session(image_state_engine) as session:
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            session.execute(
                table.update().where(table.c.id == canonical.id).values(
                    lease_owner='schema-rejected-owner',
                    lease_expires_at=9999,
                    heartbeat_at=1000,
                    verification_requested_at=None))
            session.commit()
        session.rollback()
    with image_state_engine.begin() as connection:
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        connection.execute(
            table.update().where(table.c.id == canonical.id).values(
                lease_owner='impossible-ready-owner',
                lease_expires_at=9999,
                heartbeat_at=1000,
                verification_requested_at=None))
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')

    repaired = _ensure_profile_location(image,
                                        profile,
                                        profile.canonical,
                                        canonical=True)
    assert repaired.state == models.ImageLocationState.READY
    assert repaired.lease_owner is None
    assert repaired.lease_expires_at is None
    assert repaired.heartbeat_at is None
    reference = state.acquire_reference(repaired.id, 'research', 'serve',
                                        'impossible-ready-repair')
    assert reference.location_id == repaired.id

    with image_state_engine.begin() as connection:
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        connection.execute(
            table.update().where(table.c.id == canonical.id).values(
                lease_owner='second-impossible-ready-owner',
                lease_expires_at=9999,
                heartbeat_at=1000,
                verification_requested_at=None))
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')
    assert state.retry_location(canonical.id)
    verification = state.claim_location_verification(canonical.id, 'verifier',
                                                     30)
    assert verification is not None
    assert verification.lease_owner is not None


def test_untyped_operational_selector_rejects_namespace_ambiguity(
        image_state_engine):
    del image_state_engine
    first = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    other_source = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
    second = state.register_image(other_source, other_source, _OTHER_DIGEST,
                                  'research', 'user-1')
    state.bind_release(second.id, 'research', first.id)

    with pytest.raises(ValueError, match='ambiguous across'):
        core.status(first.id, 'research')
    assert core.status(f'artifact_id={first.id}', 'research')[0].id == first.id
    assert core.status(f'release={first.id}', 'research')[0].id == second.id
    assert core.status(f'ref={_SOURCE}', 'research')[0].id == first.id


def test_uppercase_uuid_uses_canonical_artifact_namespace(image_state_engine):
    del image_state_engine
    first = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    second = state.register_image(_OTHER_SOURCE, _OTHER_SOURCE, _OTHER_DIGEST,
                                  'research', 'user-1')
    uppercase = first.id.upper()
    assert core.status(uppercase, 'research')[0].id == first.id
    assert core.status(f'artifact_id={uppercase}', 'research')[0].id == first.id

    state.bind_release(second.id, 'research', uppercase)
    with pytest.raises(ValueError, match='ambiguous'):
        core.status(uppercase, 'research')
    assert core.status(f'artifact_id={uppercase}', 'research')[0].id == first.id


def test_explicit_source_selector_requires_registered_alias(image_state_engine):
    del image_state_engine
    artifact = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                    'user-1')
    mirror = f'ghcr.io/boltz-bio/unregistered-mirror@{_DIGEST}'

    assert state.get_image_by_digest(_DIGEST, 'research') == artifact
    assert state.get_image_by_source_ref(mirror, 'research') is None
    with pytest.raises(ValueError, match='not found'):
        core.status(f'ref={mirror}', 'research')

    state.bind_source(artifact.id, 'research', mirror, mirror)
    assert core.status(f'ref={mirror}', 'research')[0].id == artifact.id


def test_unfiltered_status_batches_artifact_associations(image_state_engine):
    first = state.register_image(_SOURCE,
                                 _SOURCE,
                                 _DIGEST,
                                 'research',
                                 'user-1',
                                 release='one')
    other_source = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
    second = state.register_image(other_source,
                                  other_source,
                                  _OTHER_DIGEST,
                                  'research',
                                  'user-1',
                                  release='two')
    profile = _profile()
    _ensure_profile_location(first, profile, profile.canonical, canonical=True)
    _ensure_profile_location(second, profile, profile.canonical, canonical=True)
    statements = []

    def _capture_statement(_, __, statement, *args):
        del args
        if statement.lstrip().upper().startswith('SELECT'):
            statements.append(statement)

    sqlalchemy.event.listen(image_state_engine, 'before_cursor_execute',
                            _capture_statement)
    try:
        records = core.status(workspace='research')
    finally:
        sqlalchemy.event.remove(image_state_engine, 'before_cursor_execute',
                                _capture_statement)
    assert {record.id for record in records} == {first.id, second.id}
    assert {tuple(record.releases) for record in records} == {('one',),
                                                              ('two',)}
    assert len(statements) == 4


def test_status_association_load_is_hard_bounded(image_state_engine,
                                                 monkeypatch):
    del image_state_engine
    image = state.register_image(_SOURCE,
                                 _SOURCE,
                                 _DIGEST,
                                 'research',
                                 'user-1',
                                 max_sources_per_artifact=2)
    mirror = f'quay.io/boltz-bio/boltz@{_DIGEST}'
    state.bind_source(image.id,
                      'research',
                      mirror,
                      mirror,
                      max_sources_per_artifact=2)
    monkeypatch.setattr(core, '_MAX_UNPAGINATED_STATUS_ASSOCIATIONS', 1)
    with pytest.raises(ValueError, match='too many associations'):
        core.status(image.id, 'research')


def test_profile_work_lock_uses_postgresql_key_share():
    session = mock.MagicMock()
    session.get_bind.return_value = types.SimpleNamespace(
        dialect=types.SimpleNamespace(name='postgresql'))
    session.execute.return_value.first.return_value = (1,)
    assert global_user_state.lock_container_image_profile_revision_for_work(
        session, 'research', 'managed', 3)
    statement = session.execute.call_args.args[0]
    compiled = str(
        statement.compile(dialect=sqlalchemy_postgresql.dialect(),
                          compile_kwargs={'literal_binds': True}))
    assert 'FOR KEY SHARE' in compiled
    assert "revision = 3" in compiled


def test_regional_work_requires_exact_current_canonical_revision(
        image_state_engine):
    del image_state_engine
    revision_one = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical_one = _ensure_profile_location(image,
                                             revision_one,
                                             revision_one.canonical,
                                             canonical=True)
    canonical_claim = state.claim_location(canonical_one.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(revision_one,
                                                 revision_one.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical_one.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    target = revision_one.target('aws-us-west-2')
    regional = _ensure_profile_location(image, revision_one, target)
    assert regional.canonical_location_id == canonical_one.id

    canonical_two_target = models.RegistryTarget(**{
        **revision_one.canonical.__dict__,
        'region': 'us-east-2',
    })
    revision_two = models.RegistryProfile(**{
        **revision_one.__dict__,
        'canonical': canonical_two_target,
        'revision': 2,
    })
    canonical_two = _ensure_profile_location(image,
                                             revision_two,
                                             canonical_two_target,
                                             canonical=True)
    assert canonical_two.id != canonical_one.id
    assert state.claim_location(regional.id, 'stale-copier', 30) is None

    rebound = _ensure_profile_location(image, revision_two,
                                       revision_two.target(target.name))
    assert rebound.id == regional.id
    assert rebound.profile_revision == 2
    assert rebound.canonical_location_id == canonical_two.id
    assert state.claim_location(rebound.id, 'early-copier', 30) is None

    claim_two = state.claim_location(canonical_two.id, 'importer-2', 30)
    assert claim_two is not None
    assert claim_two.lease_owner is not None
    canonical_ref_two = references.managed_reference(revision_two,
                                                     canonical_two_target,
                                                     'research', _SOURCE,
                                                     _DIGEST)
    assert _complete_location(canonical_two.id, claim_two.lease_owner,
                              canonical_ref_two, _DIGEST)
    regional_claim = state.claim_location(rebound.id, 'current-copier', 30)
    assert regional_claim is not None
    assert regional_claim.canonical_location_id == canonical_two.id


def test_physical_destination_cannot_change_lifecycle_across_profiles(
        image_state_engine):
    del image_state_engine
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    profile = _profile()
    fingerprint = profile.materialization_fingerprint(profile.canonical)
    canonical = state.ensure_location(
        image.id,
        'primary',
        'canonical',
        fingerprint,
        _DIGEST,
        policy_fingerprint=profile.policy_fingerprint(profile.canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical=True)
    assert canonical.canonical
    with pytest.raises(ValueError, match='cannot be assigned to multiple'):
        state.ensure_location(
            image.id,
            'cache-profile',
            'regional-alias',
            fingerprint,
            _DIGEST,
            policy_fingerprint=profile.policy_fingerprint(
                profile.canonical, False),
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
            auto_evict=True)


def test_profile_revision_can_rename_an_unchanged_physical_target(
        image_state_engine):
    del image_state_engine
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    revision_one = _profile()
    canonical_one = _ensure_profile_location(image,
                                             revision_one,
                                             revision_one.canonical,
                                             canonical=True)
    old_target = revision_one.target('aws-us-west-2')
    regional_one = _ensure_profile_location(image, revision_one, old_target)

    renamed_target = models.RegistryTarget(**{
        **old_target.__dict__,
        'name': 'west-renamed',
    })
    revision_two = models.RegistryProfile(**{
        **revision_one.__dict__,
        'targets': (renamed_target,),
        'revision': 2,
    })
    canonical_two = _ensure_profile_location(image,
                                             revision_two,
                                             revision_two.canonical,
                                             canonical=True)
    regional_two = _ensure_profile_location(image, revision_two, renamed_target)

    assert canonical_two.id == canonical_one.id
    assert regional_two.id == regional_one.id
    assert regional_two.target_id == 'west-renamed'
    assert regional_two.profile_revision == 2
    assert state.get_location(image.id, revision_two.name,
                              old_target.name) is None


def test_managed_namespace_must_isolate_workspaces(monkeypatch):
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'unsafe',
                'profiles': {
                    'unsafe': {
                        'ownership': 'managed',
                        'realm': 'production',
                        'namespace': 'shared/images',
                        'canonical': {
                            'provider': 'generic',
                            'registry': 'registry.example.com',
                            'region': 'global',
                            'pull_auth': 'anonymous',
                        },
                    },
                },
            },
        })
    with pytest.raises(ValueError, match='workspace.*placeholder'):
        config.resolve_profile(None, 'research')


def test_kubernetes_context_binding_enables_explicit_private_registry_route(
        monkeypatch):
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'kubernetes_contexts': {
                    'boltz-eks': {
                        'registry_provider': 'aws',
                        'registry_region': 'us-west-2',
                        'registry': ('123456789012.dkr.ecr.us-west-2.'
                                     'amazonaws.com'),
                        'auth_strategy': 'node_identity',
                    },
                },
            },
        })
    assert config.get_kubernetes_registry_binding('boltz-eks') == (
        'aws', 'us-west-2', '123456789012.dkr.ecr.us-west-2.amazonaws.com',
        'node_identity')
    target = _profile().target('aws-us-west-2')
    placement = models.Placement(
        provider='kubernetes',
        region='boltz-eks',
        backend='kubernetes',
        registry_provider='aws',
        registry_region='us-west-2',
        registry_prefix=('123456789012.dkr.ecr.us-west-2.amazonaws.com'),
        registry_auth_strategy='node_identity',
    )
    auth = providers.get_adapter('aws').resolve_runtime_pull_auth(
        target, placement)
    assert auth == 'kubernetes_context:node_identity'
    assert providers.get_adapter('aws').runtime_login_config(
        target, auth, placement) is None
    wrong_account = dataclasses.replace(
        placement,
        registry_prefix='999999999999.dkr.ecr.us-west-2.amazonaws.com')
    assert providers.get_adapter('aws').resolve_runtime_pull_auth(
        target, wrong_account) is None

    missing_registry = {
        'container_registries': {
            'kubernetes_contexts': {
                'boltz-eks': {
                    'registry_provider': 'aws',
                    'registry_region': 'us-west-2',
                    'auth_strategy': 'node_identity',
                },
            },
        },
    }
    with pytest.raises(ValueError, match='registry'):
        common_utils.validate_schema(missing_registry,
                                     schemas.get_config_schema(),
                                     'Invalid config')


def test_kubernetes_registry_region_is_normalized_and_value_free(monkeypatch):
    assert models.normalize_registry_region('US', 'GCP region', 'gcp') == 'us'
    binding = {
        'registry_provider': 'aws',
        'registry_region': ' US-WEST-2 ',
        'registry': ('123456789012.dkr.ecr.us-west-2.amazonaws.com/team'),
        'auth_strategy': 'node_identity',
    }
    raw_config = {
        'container_registries': {
            'kubernetes_contexts': {
                'boltz-eks': binding,
            },
        },
    }
    skypilot_config._validate_container_image_config(raw_config,
                                                     '<test config>')
    _mock_config(monkeypatch, raw_config)
    assert config.get_kubernetes_registry_binding('boltz-eks')[1] == (
        'us-west-2')
    placement = models.Placement(provider='kubernetes',
                                 region='boltz-eks',
                                 backend='kubernetes',
                                 registry_provider='aws',
                                 registry_region=' US-WEST-2 ')
    assert placement.registry_region == 'us-west-2'

    secret = 'Bearer-supersecret'
    hostile_config = {
        'container_registries': {
            'kubernetes_contexts': {
                'boltz-eks': {
                    **binding,
                    'registry_region': secret,
                },
            },
        },
    }
    with pytest.raises(ValueError) as admission_error:
        skypilot_config._validate_container_image_config(
            hostile_config, '<test config>')
    assert secret not in str(admission_error.value)
    with pytest.raises(ValueError) as placement_error:
        models.Placement(provider='kubernetes',
                         region='boltz-eks',
                         backend='kubernetes',
                         registry_provider='aws',
                         registry_region=secret)
    assert secret not in str(placement_error.value)


def test_kubernetes_first_use_warms_exact_authorized_registry(
        image_state_engine):
    del image_state_engine
    cases = [
        {
            'provider': 'aws',
            'canonical_region': 'us-east-1',
            'local_region': 'us-west-2',
            'identity_key': 'account',
            'canonical_identity': '000000000000',
            'wrong_identity': '111111111111',
            'right_identity': '222222222222',
            'pull_auth': 'ecr_runtime_identity',
        },
        {
            'provider': 'gcp',
            'canonical_region': 'us-central1',
            'local_region': 'us-west1',
            'identity_key': 'project',
            'canonical_identity': 'canonical-project',
            'wrong_identity': 'wrong-project',
            'right_identity': 'right-project',
            'pull_auth': 'gar_runtime_identity',
        },
    ]
    for case in cases:
        provider = case['provider']
        identity_key = case['identity_key']
        canonical = models.RegistryTarget(
            name='canonical',
            provider=provider,
            region=case['canonical_region'],
            pull_auth=case['pull_auth'],
            **{identity_key: case['canonical_identity']})
        wrong = models.RegistryTarget(name='a-wrong',
                                      provider=provider,
                                      region=case['local_region'],
                                      pull_auth=case['pull_auth'],
                                      **{identity_key: case['wrong_identity']})
        right = models.RegistryTarget(name='z-right',
                                      provider=provider,
                                      region=case['local_region'],
                                      pull_auth=case['pull_auth'],
                                      **{identity_key: case['right_identity']})
        profile = models.RegistryProfile(
            name=f'managed-{provider}',
            ownership=models.RegistryOwnership.MANAGED,
            realm='production',
            namespace='skypilot/{workspace}',
            require_digest_at_runtime=True,
            revision=1,
            canonical=canonical,
            targets=(wrong, right))
        workspace = f'research-{provider}'
        image = state.register_image(_SOURCE, _SOURCE, _DIGEST, workspace,
                                     'user-1')
        placement = models.Placement(provider='kubernetes',
                                     region=f'{provider}-context',
                                     backend='kubernetes',
                                     registry_provider=provider,
                                     registry_region=case['local_region'],
                                     registry_prefix=right.registry_prefix,
                                     registry_auth_strategy='node_identity')
        core._ensure_for_placement(image, profile, placement)
        assert {
            location.target_id for location in state.list_locations(image.id)
        } == {'canonical', 'z-right'}


def test_vm_same_region_registry_identity_ambiguity_fails_closed(
        image_state_engine):
    del image_state_engine
    cases = [
        {
            'provider': 'aws',
            'canonical_region': 'us-east-1',
            'local_region': 'us-west-2',
            'identity_key': 'account',
            'canonical_identity': '000000000000',
            'first_identity': '111111111111',
            'second_identity': '222222222222',
            'pull_auth': 'ecr_runtime_identity',
        },
        {
            'provider': 'gcp',
            'canonical_region': 'us-central1',
            'local_region': 'us-west1',
            'identity_key': 'project',
            'canonical_identity': 'canonical-project',
            'first_identity': 'first-project',
            'second_identity': 'second-project',
            'pull_auth': 'gar_runtime_identity',
        },
    ]
    for case in cases:
        provider = case['provider']
        identity_key = case['identity_key']
        canonical = models.RegistryTarget(
            name='canonical',
            provider=provider,
            region=case['canonical_region'],
            pull_auth=case['pull_auth'],
            **{identity_key: case['canonical_identity']})
        first = models.RegistryTarget(name='a-first',
                                      provider=provider,
                                      region=case['local_region'],
                                      pull_auth=case['pull_auth'],
                                      **{identity_key: case['first_identity']})
        second = models.RegistryTarget(
            name='z-second',
            provider=provider,
            region=case['local_region'],
            pull_auth=case['pull_auth'],
            **{identity_key: case['second_identity']})
        profile = models.RegistryProfile(
            name=f'managed-{provider}',
            ownership=models.RegistryOwnership.MANAGED,
            realm='production',
            namespace='skypilot/{workspace}',
            require_digest_at_runtime=True,
            revision=1,
            canonical=canonical,
            targets=(first, second))
        workspace = f'vm-ambiguous-{provider}'
        image = state.register_image(_SOURCE, _SOURCE, _DIGEST, workspace,
                                     'user-1')
        placement = models.Placement(provider=provider,
                                     region=case['local_region'],
                                     backend='vm')
        with pytest.raises(ValueError, match='ambiguous.*VM'):
            core._ensure_for_placement(image, profile, placement)
        assert state.list_locations(image.id) == []
        with pytest.raises(ValueError, match='ambiguous.*VM'):
            core.routes_for_image(image, profile, placement)


def test_external_materialization_can_be_verified_or_copied(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile_config = {
        'ownership': 'external',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'generic',
            'registry': 'registry.example.com',
            'region': 'global',
            'pull_auth': 'anonymous',
        },
        'targets': [{
            'name': 'regional',
            'provider': 'generic',
            'registry': 'west.registry.example.com',
            'region': 'west',
            'pull_auth': 'anonymous',
        }],
    }
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'external',
                'profiles': {
                    'external': profile_config,
                },
            },
        })
    profile, _ = config.resolve_profile(None, 'research')
    assert profile is not None
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    inspected = []
    assert not worker.adopt_external_location(canonical.id, 'legacy-adopter',
                                              lambda *_: _DIGEST)
    rejected = state.get_location_by_id(canonical.id)
    assert rejected is not None
    assert rejected.state == models.ImageLocationState.FAILED
    assert state.retry_location(canonical.id)
    assert worker.adopt_external_location(
        canonical.id, 'adopter', lambda destination, _: inspected.append(
            destination) or _materialization())
    assert inspected and inspected[0].endswith(f'@{_DIGEST}')
    assert state.get_location_by_id(
        canonical.id).state == models.ImageLocationState.READY

    target = profile.target('regional')
    regional = _ensure_profile_location(image, profile, target)
    copied = []

    def copy(source: str, destination: str, digest: str,
             _) -> worker.MaterializationResult:
        copied.append((source, destination, digest))
        return _materialization(digest)

    assert worker.materialize_location(regional.id, 'copier', copy)
    assert copied[0][0] == inspected[0]
    assert copied[0][1].startswith('west.registry.example.com/')
    assert state.get_location_by_id(
        regional.id).state == models.ImageLocationState.READY


def test_materialization_persists_verified_artifact_metadata(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    adapter = mock.Mock()
    monkeypatch.setattr(providers, 'get_adapter', lambda _: adapter)
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    assert worker.materialize_location(
        canonical.id, 'copy-worker', lambda *_: worker.MaterializationResult(
            digest=_DIGEST,
            platforms=('linux/amd64', 'linux/arm64'),
            compressed_size_bytes=123456))
    refreshed = state.get_image(image.id, 'research')
    assert refreshed is not None
    assert refreshed.platforms == ('linux/amd64', 'linux/arm64')
    assert refreshed.compressed_size_bytes == 123456
    expected_reference = references.managed_reference(profile,
                                                      profile.canonical,
                                                      'research', _SOURCE,
                                                      _DIGEST)
    expected_repository, _ = models.split_digest(expected_reference)
    adapter.ensure_target_repository.assert_called_once_with(
        profile.canonical, profile, 'research', expected_repository)


def test_ready_rejects_empty_platform_metadata(image_state_engine):
    del image_state_engine
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    claim = state.claim_location(canonical.id, 'copy-worker', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    reference = references.managed_reference(profile, profile.canonical,
                                             'research', _SOURCE, _DIGEST)
    assert not state.complete_location(
        canonical.id, claim.lease_owner, reference, _DIGEST, platforms=())
    failed = state.get_location_by_id(canonical.id)
    assert failed is not None
    assert failed.state == models.ImageLocationState.FAILED
    assert failed.last_error == (
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value)
    assert not state.get_image(image.id, 'research').platforms
    assert not models.platforms_support_runtime([], 'linux/amd64')


def test_untrusted_artifact_metadata_is_value_free_and_never_persisted(
        image_state_engine, monkeypatch):
    profile = _profile()
    secret = 'Authorization-Bearer-supersecret'
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    monkeypatch.setattr(providers, 'get_adapter', lambda _: mock.Mock())
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)

    with pytest.raises(ValueError) as metadata_error:
        worker.MaterializationResult(digest=_DIGEST, platforms=(secret,))
    assert secret not in str(metadata_error.value)

    def invalid_copy(*_):
        return worker.MaterializationResult(digest=_DIGEST, platforms=(secret,))

    assert not worker.materialize_location(canonical.id, 'copy-worker',
                                           invalid_copy)
    refreshed = state.get_image(image.id, 'research')
    assert refreshed is not None
    assert not refreshed.platforms
    failed = state.get_location_by_id(canonical.id)
    assert failed is not None
    assert failed.state == models.ImageLocationState.FAILED
    assert failed.last_error == (
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value)

    direct_image = state.register_image(_OTHER_SOURCE, _OTHER_SOURCE,
                                        _OTHER_DIGEST, 'research', 'user-1')
    direct_canonical = _ensure_profile_location(direct_image,
                                                profile,
                                                profile.canonical,
                                                canonical=True)
    direct_claim = state.claim_location(direct_canonical.id, 'direct-worker',
                                        30)
    assert direct_claim is not None
    assert direct_claim.lease_owner is not None
    direct_ref = references.managed_reference(profile, profile.canonical,
                                              'research', _OTHER_SOURCE,
                                              _OTHER_DIGEST)
    assert not _complete_location(direct_canonical.id,
                                  direct_claim.lease_owner,
                                  direct_ref,
                                  _OTHER_DIGEST,
                                  platforms=(secret,))
    direct_failed = state.get_location_by_id(direct_canonical.id)
    assert direct_failed is not None
    assert direct_failed.state == models.ImageLocationState.FAILED
    assert direct_failed.last_error == (
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value)

    response_payload = {
        'id': image.id,
        'workspace': 'research',
        'sources': [_SOURCE],
        'source_digest': _DIGEST,
        'releases': [],
        'producer_kind': 'external_oci',
        'platforms': [secret],
        'created_at': 1,
        'updated_at': 1,
        'locations': [],
    }
    with pytest.raises(ValueError) as response_error:
        responses.ContainerImageRecord(**response_payload)
    assert secret not in str(response_error.value)

    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(global_user_state.container_image_table.update().where(
            global_user_state.container_image_table.c.id == image.id).values(
                platforms_json=json.dumps([secret])))
        session.commit()
    with pytest.raises(ValueError) as stored_error:
        state.get_image(image.id, 'research')
    assert secret not in str(stored_error.value)


def test_worker_rejects_overlong_destination_before_registry_io(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile = models.RegistryProfile(name='managed',
                                     ownership=models.RegistryOwnership.MANAGED,
                                     realm='production',
                                     namespace=f'{"a" * 230}/{{workspace}}',
                                     require_digest_at_runtime=True,
                                     revision=1,
                                     canonical=models.RegistryTarget(
                                         name='canonical',
                                         provider='generic',
                                         registry='registry.example.com',
                                         region='global',
                                         pull_auth='anonymous'))
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    get_adapter = mock.Mock()
    monkeypatch.setattr(providers, 'get_adapter', get_adapter)
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    copy = mock.Mock(return_value=_DIGEST)
    assert not worker.materialize_location(canonical.id, 'copy-worker', copy)
    get_adapter.assert_not_called()
    copy.assert_not_called()
    failed = state.get_location_by_id(canonical.id)
    assert failed is not None
    assert failed.state == models.ImageLocationState.FAILED
    assert failed.last_error == (
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value)


def test_worker_rejects_stale_policy_before_registry_io(image_state_engine,
                                                        monkeypatch):
    del image_state_engine
    profile_config = {
        'ownership': 'external',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'generic',
            'registry': 'registry.example.com',
            'region': 'global',
            'pull_auth': 'anonymous',
        },
    }
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'external',
                'profiles': {
                    'external': profile_config,
                },
            },
        })
    profile, _ = config.resolve_profile(None, 'research')
    assert profile is not None
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    profile_config['realm'] = 'rotated-policy'
    inspect = mock.Mock(return_value=_DIGEST)
    assert not worker.adopt_external_location(canonical.id, 'worker', inspect)
    inspect.assert_not_called()
    failed = state.get_location_by_id(canonical.id)
    assert failed is not None
    assert failed.state == models.ImageLocationState.FAILED
    assert failed.last_error == (
        models.ImageLocationErrorCode.EXTERNAL_ADOPTION_FAILED.value)


def test_lost_lease_cancels_io_and_never_publishes_ready(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile = models.RegistryProfile(
        name='external',
        ownership=models.RegistryOwnership.EXTERNAL,
        realm='production',
        namespace='skypilot/{workspace}',
        require_digest_at_runtime=True,
        revision=1,
        canonical=models.RegistryTarget(name='canonical',
                                        provider='generic',
                                        registry='registry.example.com',
                                        region='global',
                                        pull_auth='anonymous'),
    )
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    cancel_events = []

    def inspect(_reference, cancel_event):
        cancel_events.append(cancel_event)
        return _materialization()

    heartbeats = iter((True, False))
    monkeypatch.setattr(state, 'heartbeat_location',
                        lambda *_: next(heartbeats))
    assert not worker.adopt_external_location(canonical.id, 'worker', inspect)
    assert cancel_events[0].is_set()
    failed = state.get_location_by_id(canonical.id)
    assert failed is not None
    assert failed.state == models.ImageLocationState.FAILED
    assert failed.target_ref is None


def test_lease_heartbeat_signals_cancellation_while_callback_runs(monkeypatch):
    heartbeat = mock.Mock(return_value=False)
    monkeypatch.setattr(state, 'heartbeat_location', heartbeat)
    guard = worker._LeaseHeartbeat(_LOCATION_ID, 'owner:token', 3)
    guard._interval = 0.01
    with pytest.raises(worker.LeaseLostError):
        with guard:
            pass
    assert guard.cancel_event.is_set()
    heartbeat.assert_called()


def test_atomic_reconciliation_claims_do_not_hoard_or_duplicate_work(
        image_state_engine):
    engine = image_state_engine
    profile = _profile()
    first_image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                       'user-1')
    other_source = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
    second_image = state.register_image(other_source, other_source,
                                        _OTHER_DIGEST, 'research', 'user-1')
    first = _ensure_profile_location(first_image,
                                     profile,
                                     profile.canonical,
                                     canonical=True)
    second = _ensure_profile_location(second_image,
                                      profile,
                                      profile.canonical,
                                      canonical=True)
    first_claim = state.claim_next_reconciliation_candidate(
        'research', 'replica-1', 30, 30)
    second_claim = state.claim_next_reconciliation_candidate(
        'research', 'replica-2', 30, 30)
    assert first_claim is not None
    assert second_claim is not None
    assert {first_claim.id, second_claim.id} == {first.id, second.id}
    assert state.claim_next_reconciliation_candidate('research', 'replica-3',
                                                     30, 30) is None

    # A corrupt READY row without a published reference must not enter the
    # verification queue and trip the worker's READY invariant.
    table = global_user_state.container_image_location_table
    with engine.begin() as connection:
        connection.execute(table.update().where(table.c.id == first.id).values(
            state=models.ImageLocationState.READY.value,
            target_ref=None,
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None,
            verification_requested_at=1,
            next_retry_at=None))
    assert state.claim_next_reconciliation_candidate('research', 'replica-4',
                                                     30, 30) is None


def test_reconciliation_claims_use_indexed_profile_fair_queue(
        image_state_engine):
    engine = image_state_engine
    first_profile = _profile()
    second_profile = dataclasses.replace(first_profile,
                                         name='managed-two',
                                         canonical=dataclasses.replace(
                                             first_profile.canonical,
                                             region='us-east-2'))
    sources = [
        (_SOURCE, _DIGEST, first_profile),
        (f'ghcr.io/boltz-bio/first-two@sha256:{"c" * 64}', f'sha256:{"c" * 64}',
         first_profile),
        (_OTHER_SOURCE, _OTHER_DIGEST, second_profile),
        (f'ghcr.io/boltz-bio/second-two@sha256:{"d" * 64}',
         f'sha256:{"d" * 64}', second_profile),
    ]
    for source_ref, digest, profile in sources:
        image = state.register_image(source_ref, source_ref, digest, 'research',
                                     'user-1')
        _ensure_profile_location(image,
                                 profile,
                                 profile.canonical,
                                 canonical=True)

    statements = []

    def _capture_statement(_connection, _cursor, statement, _parameters,
                           _context, _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _capture_statement)
    try:
        claims = [
            state.claim_next_reconciliation_candidate('research',
                                                      f'worker-{index}', 30, 30)
            for index in range(4)
        ]
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _capture_statement)

    assert all(claim is not None for claim in claims)
    claimed_profiles = [claim.profile for claim in claims if claim is not None]
    assert claimed_profiles[0] != claimed_profiles[1]
    assert claimed_profiles[0] == claimed_profiles[2]
    assert claimed_profiles[1] == claimed_profiles[3]
    claim_sql = '\n'.join(statements).upper()
    assert 'GROUP BY' not in claim_sql
    assert 'EXISTS' in claim_sql
    assert 'CONTAINER_IMAGE_PROFILE_REVISIONS' in claim_sql


def test_reconciliation_filters_blocked_canonical_dependencies_before_seek(
        image_state_engine):
    del image_state_engine
    profile = _profile()
    target = profile.target('aws-us-west-2')
    for index in range(70):
        digest = f'sha256:{index + 16:064x}'
        source = f'registry.example.com/blocked-{index}@{digest}'
        image = _publish_state_image(source,
                                     digest,
                                     release=None,
                                     profile=profile)
        canonical = next(location for location in state.list_locations(image.id)
                         if location.canonical)
        assert state.claim_location(canonical.id, f'blocked-{index}', 3600)
        _ensure_profile_location(image, profile, target)

    eligible_digest = f'sha256:{4096:064x}'
    eligible_source = f'registry.example.com/eligible@{eligible_digest}'
    eligible_image = _publish_state_image(eligible_source,
                                          eligible_digest,
                                          release=None,
                                          profile=profile)
    eligible_canonical = next(
        location for location in state.list_locations(eligible_image.id)
        if location.canonical)
    canonical_claim = state.claim_location(eligible_canonical.id, 'ready', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', eligible_source,
                                                 eligible_digest)
    assert _complete_location(eligible_canonical.id,
                              canonical_claim.lease_owner, canonical_ref,
                              eligible_digest)
    eligible = _ensure_profile_location(eligible_image, profile, target)

    original_lock = state._lock_exact_canonical_ready
    with mock.patch.object(state,
                           '_lock_exact_canonical_ready',
                           wraps=original_lock) as lock_canonical:
        claimed = state.claim_next_reconciliation_candidate(
            'research', 'worker', 30, 30)

    assert claimed is not None
    assert claimed.id == eligible.id
    lock_canonical.assert_called_once_with(mock.ANY, eligible.id)


def test_reconciliation_candidate_seek_budget_is_hard_bounded(
        image_state_engine):
    del image_state_engine
    profile = _profile()
    target = profile.target('aws-us-west-2')
    for index in range(state._MAX_CLAIM_CANDIDATE_SEEKS + 1):
        digest = f'sha256:{index + 8192:064x}'
        source = f'registry.example.com/contended-{index}@{digest}'
        image = _publish_state_image(source,
                                     digest,
                                     release=None,
                                     profile=profile)
        canonical = next(location for location in state.list_locations(image.id)
                         if location.canonical)
        claim = state.claim_location(canonical.id, f'ready-{index}', 30)
        assert claim is not None
        assert claim.lease_owner is not None
        canonical_ref = references.managed_reference(profile, profile.canonical,
                                                     'research', source, digest)
        assert _complete_location(canonical.id, claim.lease_owner,
                                  canonical_ref, digest)
        _ensure_profile_location(image, profile, target)

    with mock.patch.object(state,
                           '_lock_exact_canonical_ready',
                           return_value=False) as lock_canonical:
        assert state.claim_next_reconciliation_candidate(
            'research', 'worker', 30, 30) is None
    assert lock_canonical.call_count == state._MAX_CLAIM_CANDIDATE_SEEKS


def test_policy_transfer_reuses_bytes_and_revokes_eviction_authority(
        image_state_engine):
    del image_state_engine
    managed = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    target = managed.target('aws-us-west-2')
    original = _ensure_profile_location(image, managed, target, auto_evict=True)
    external = models.RegistryProfile(
        **{
            **managed.__dict__,
            'ownership': models.RegistryOwnership.EXTERNAL,
            'revision': 2,
        })
    transferred = _ensure_profile_location(image, external,
                                           external.target(target.name))
    assert transferred.id == original.id
    assert transferred.target_fingerprint == original.target_fingerprint
    assert transferred.policy_fingerprint != original.policy_fingerprint
    assert not transferred.auto_evict


def test_reconciler_orders_canonical_retries_and_ready_verification(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    monkeypatch.setattr(worker.time, 'time', lambda: now[0])
    profile_config = {
        'ownership': 'external',
        'realm': 'production',
        'namespace': 'skypilot/{workspace}',
        'canonical': {
            'provider': 'generic',
            'registry': 'registry.example.com',
            'region': 'global',
            'pull_auth': 'anonymous',
        },
        'targets': [{
            'name': 'regional',
            'provider': 'generic',
            'registry': 'west.registry.example.com',
            'region': 'west',
            'pull_auth': 'anonymous',
        }],
    }
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'external',
                'profiles': {
                    'external': profile_config,
                },
            },
        })
    profile, _ = config.resolve_profile(None, 'research')
    assert profile is not None
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    regional_target = profile.target('regional')
    regional = _ensure_profile_location(image, profile, regional_target)
    copy_attempts = []

    def inspect(candidate, reference, cancel_event):
        del candidate, reference, cancel_event
        return _materialization()

    def fail_copy(candidate, source, destination, digest, cancel_event):
        del cancel_event
        copy_attempts.append((candidate.id, source, destination, digest))
        raise RuntimeError('temporary registry throttle')

    first = worker.reconcile_once('research',
                                  'worker-1',
                                  fail_copy,
                                  inspect,
                                  now=now[0],
                                  retry_seconds=60)
    assert first == worker.ReconciliationSweepResult(candidates=2,
                                                     materialized=1,
                                                     revalidated=0,
                                                     failed=1)
    assert state.get_location_by_id(
        canonical.id).state == models.ImageLocationState.READY
    failed_regional = state.get_location_by_id(regional.id)
    assert failed_regional is not None
    assert failed_regional.state == models.ImageLocationState.FAILED
    assert failed_regional.next_retry_at is not None
    assert failed_regional.next_retry_at >= 1060
    assert state.list_reconciliation_candidates(
        'research', now=failed_regional.next_retry_at - 1) == []

    now[0] = failed_regional.next_retry_at
    second = worker.reconcile_once(
        'research',
        'worker-2',
        lambda candidate, source, destination, digest, cancel_event:
        _materialization(digest),
        inspect,
        now=now[0])
    assert second.materialized == 1
    assert second.failed == 0
    assert state.get_location_by_id(
        regional.id).state == models.ImageLocationState.READY

    regional_ready = state.get_location_by_id(regional.id)
    assert regional_ready is not None
    assert regional_ready.target_ref is not None
    ready_ref = regional_ready.target_ref
    assert state.retry_location(regional.id)
    queued = state.get_location_by_id(regional.id)
    assert queued is not None
    assert queued.state == models.ImageLocationState.READY
    assert queued.target_ref == ready_ref
    assert queued.verification_requested_at == now[0]
    verified = worker.reconcile_once('research',
                                     'worker-3',
                                     lambda *_: _materialization(),
                                     inspect,
                                     now=now[0])
    assert verified.revalidated == 1
    assert state.get_location_by_id(
        regional.id).state == models.ImageLocationState.READY

    assert state.retry_location(regional.id)
    drifted = worker.reconcile_once('research',
                                    'worker-4',
                                    lambda *_: _materialization(),
                                    lambda candidate, reference, cancel_event:
                                    _materialization(_OTHER_DIGEST),
                                    now=now[0])
    assert drifted.failed == 1
    missing = state.get_location_by_id(regional.id)
    assert missing is not None
    assert missing.state == models.ImageLocationState.MISSING
    assert missing.next_retry_at is not None
    assert missing.next_retry_at > now[0]


def test_reconciler_refreshes_time_for_each_lease_and_retry(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile = models.RegistryProfile(
        name='external',
        ownership=models.RegistryOwnership.EXTERNAL,
        realm='production',
        namespace='skypilot/{workspace}',
        require_digest_at_runtime=True,
        revision=1,
        canonical=models.RegistryTarget(name='canonical',
                                        provider='generic',
                                        registry='registry.example.com',
                                        region='global',
                                        pull_auth='anonymous'))
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    first = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    other_source = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
    second = state.register_image(other_source, other_source, _OTHER_DIGEST,
                                  'research', 'user-1')
    _ensure_profile_location(first, profile, profile.canonical, canonical=True)
    _ensure_profile_location(second, profile, profile.canonical, canonical=True)
    clock = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: clock[0])
    lease_expirations = []
    failed_location_ids = []

    def inspect(candidate, reference, cancel_event):
        del reference, cancel_event
        lease_expirations.append(candidate.lease_expires_at)
        clock[0] += 100
        if len(lease_expirations) == 2:
            failed_location_ids.append(candidate.id)
            raise RuntimeError('late transient failure')
        return _materialization(candidate.expected_digest)

    result = worker.reconcile_once('research',
                                   'worker',
                                   lambda *_: mock.sentinel.unused,
                                   inspect,
                                   limit=2,
                                   lease_seconds=900,
                                   retry_seconds=60)
    assert result == worker.ReconciliationSweepResult(candidates=2,
                                                      materialized=1,
                                                      revalidated=0,
                                                      failed=1)
    assert lease_expirations == [1900, 2000]
    assert len(failed_location_ids) == 1
    failed = state.get_location_by_id(failed_location_ids[0])
    assert failed is not None
    assert failed.next_retry_at is not None
    assert failed.next_retry_at >= 1260


@pytest.mark.parametrize(
    'second_platforms,second_size',
    [(('linux/arm64',), 100), (('linux/amd64',), 200)],
)
def test_canonical_completions_cannot_rewrite_artifact_evidence(
        image_state_engine, second_platforms, second_size):
    del image_state_engine
    first_profile = _profile()
    second_profile = dataclasses.replace(first_profile,
                                         name='mirror',
                                         canonical=dataclasses.replace(
                                             first_profile.canonical,
                                             region='us-east-2'))
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')

    first = _ensure_profile_location(image,
                                     first_profile,
                                     first_profile.canonical,
                                     canonical=True)
    first_claim = state.claim_location(first.id, 'first-importer', 30)
    assert first_claim is not None and first_claim.lease_owner is not None
    first_ref = references.managed_reference(first_profile,
                                             first_profile.canonical,
                                             'research', _SOURCE, _DIGEST)
    assert _complete_location(first.id, first_claim.lease_owner, first_ref,
                              _DIGEST, ('linux/amd64',), 100)

    second = _ensure_profile_location(image,
                                      second_profile,
                                      second_profile.canonical,
                                      canonical=True)
    second_claim = state.claim_location(second.id, 'second-importer', 30)
    assert second_claim is not None and second_claim.lease_owner is not None
    second_ref = references.managed_reference(second_profile,
                                              second_profile.canonical,
                                              'research', _SOURCE, _DIGEST)
    assert not _complete_location(second.id, second_claim.lease_owner,
                                  second_ref, _DIGEST, second_platforms,
                                  second_size)

    stored = state.get_image(image.id, 'research')
    assert stored is not None
    assert stored.platforms == ('linux/amd64',)
    assert stored.compressed_size_bytes == 100
    rejected = state.get_location_by_id(second.id)
    assert rejected is not None
    assert rejected.state == models.ImageLocationState.FAILED
    assert rejected.last_error == (
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value)


def test_canonical_dependency_updates_only_current_profile_generation(
        image_state_engine):
    profile_one = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile_one,
                                         profile_one.canonical,
                                         canonical=True)
    old_regional = _ensure_profile_location(image, profile_one,
                                            profile_one.target('aws-us-west-2'))
    claim = state.claim_location(canonical.id, 'first-importer', 30)
    assert claim is not None and claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile_one,
                                                 profile_one.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, claim.lease_owner, canonical_ref,
                              _DIGEST)

    profile_two = dataclasses.replace(profile_one,
                                      revision=2,
                                      targets=(dataclasses.replace(
                                          profile_one.targets[0],
                                          region='us-west-1'),))
    current_canonical = _ensure_profile_location(image,
                                                 profile_two,
                                                 profile_two.canonical,
                                                 canonical=True)
    assert current_canonical.id == canonical.id
    new_regional = _ensure_profile_location(image, profile_two,
                                            profile_two.targets[0])
    assert new_regional.id != old_regional.id
    assert old_regional.profile_revision == 1
    assert new_regional.profile_revision == 2

    table = global_user_state.container_image_location_table
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(
            table.c.id.in_([old_regional.id,
                            new_regional.id])).values(canonical_ready=False))
        session.commit()
    assert state.mark_location_missing(canonical.id)
    assert state.retry_location(canonical.id)
    retry_claim = state.claim_location(canonical.id, 'second-importer', 30)
    assert retry_claim is not None and retry_claim.lease_owner is not None
    assert _complete_location(canonical.id, retry_claim.lease_owner,
                              canonical_ref, _DIGEST)
    assert not state.get_location_by_id(old_regional.id).canonical_ready
    assert state.get_location_by_id(new_regional.id).canonical_ready

    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(
            table.c.id == old_regional.id).values(canonical_ready=True))
        session.commit()
    assert state.mark_location_missing(canonical.id)
    assert state.get_location_by_id(old_regional.id).canonical_ready
    assert not state.get_location_by_id(new_regional.id).canonical_ready


def test_preprovision_boundary_resolves_once_and_classifies_errors():
    # Load this only for the integration-boundary test because the backend has a
    # much heavier import surface than the image model and state layers.
    cloud_vm_ray_backend = importlib.import_module(
        'sky.backends.cloud_vm_ray_backend')

    requested = resources_lib.Resources(cloud='aws',
                                        region='us-west-2',
                                        container_image=_SOURCE)
    pull_plan = models.ResolvedContainerImage(
        image_id=_ARTIFACT_ID,
        location_id=_LOCATION_ID,
        reference='ecr-west.example/repo@' + _DIGEST,
        target_id='aws-us-west-2',
        distribution='managed',
        profile_revision=1,
        policy_fingerprint=_POLICY_FINGERPRINT,
        digest=_DIGEST,
        auth_strategy='ecr_runtime_identity',
    )
    pinned = requested.copy(_resolved_container_image=pull_plan)
    with mock.patch.object(cloud_vm_ray_backend.container_images_core,
                           'resolve_for_placement',
                           return_value=pinned) as resolve_mock:
        actual = cloud_vm_ray_backend._resolve_container_image_for_placement(
            requested)
    assert actual is pinned
    placement = resolve_mock.call_args.args[1]
    assert placement == models.Placement(provider='aws',
                                         region='us-west-2',
                                         backend='vm')

    ssh_requested = resources_lib.Resources(cloud=clouds.SSH(),
                                            region='ssh-pool',
                                            container_image=_SOURCE)
    ssh_binding = ('aws', 'us-west-2',
                   '123456789012.dkr.ecr.us-west-2.amazonaws.com',
                   'node_identity')
    with mock.patch.object(cloud_vm_ray_backend.container_images_config,
                           'get_kubernetes_registry_binding',
                           return_value=ssh_binding) as get_binding, \
         mock.patch.object(cloud_vm_ray_backend.container_images_core,
                           'resolve_for_placement',
                           return_value=pinned) as ssh_resolve:
        assert cloud_vm_ray_backend._resolve_container_image_for_placement(
            ssh_requested) is pinned
    get_binding.assert_called_once_with('ssh-pool')
    assert ssh_resolve.call_args.args[1] == models.Placement(
        provider='ssh',
        region='ssh-pool',
        backend='kubernetes',
        registry_provider='aws',
        registry_region='us-west-2',
        registry_prefix='123456789012.dkr.ecr.us-west-2.amazonaws.com',
        registry_auth_strategy='node_identity')
    private_ssh_source = (
        f'123456789012.dkr.ecr.us-west-2.amazonaws.com/boltz@{_DIGEST}')
    ssh_auth, ssh_login = providers.resolve_source_runtime_pull_auth(
        private_ssh_source, ssh_resolve.call_args.args[1], None)
    assert ssh_auth == 'kubernetes_context:node_identity'
    assert ssh_login is None
    assert (global_user_state._container_image_backend(
        clouds.SSH()) == 'kubernetes')
    assert (global_user_state._container_image_backend(
        clouds.Kubernetes()) == 'kubernetes')

    requested._cloud = clouds.AWS()
    requested._instance_type = 't4g.small'
    assert requested.cloud is not None
    with mock.patch.object(type(requested.cloud),
                           'get_arch_from_instance_type',
                           return_value='arm64'), mock.patch.object(
                               cloud_vm_ray_backend.container_images_core,
                               'resolve_for_placement',
                               return_value=pinned) as arm_resolve:
        assert cloud_vm_ray_backend._resolve_container_image_for_placement(
            requested) is pinned
    assert arm_resolve.call_args.args[1].platform == 'linux/arm64'

    handle = cloud_vm_ray_backend.CloudVmRayResourceHandle(
        cluster_name='test',
        cluster_name_on_cloud='test-123',
        cluster_yaml='/tmp/test.yaml',
        launched_nodes=1,
        launched_resources=pinned,
    )
    restored_handle = cloud_vm_ray_backend.CloudVmRayResourceHandle.from_dict(
        handle.to_dict())
    assert (restored_handle.launched_resources.resolved_container_image ==
            pull_plan)

    with mock.patch.object(
            cloud_vm_ray_backend.container_images_core,
            'resolve_for_placement',
            side_effect=resolver.ImageRouteUnavailableError('not ready')):
        with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
            cloud_vm_ray_backend._resolve_container_image_for_placement(
                requested)
    assert not exc_info.value.no_failover

    with mock.patch.object(cloud_vm_ray_backend.container_images_core,
                           'resolve_for_placement',
                           side_effect=ValueError('bad profile')):
        with pytest.raises(exceptions.ResourcesUnavailableError) as exc_info:
            cloud_vm_ray_backend._resolve_container_image_for_placement(
                requested)
    assert exc_info.value.no_failover


def test_catalog_claims_verification_retry_and_missing(image_state_engine,
                                                       monkeypatch):
    del image_state_engine
    now = 1000
    monkeypatch.setattr(state.time, 'time', lambda: now)
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    duplicate = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                     'user-2')
    assert duplicate.id == image.id
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    claim = state.claim_location(canonical.id, 'worker-1', 30)
    assert claim.state == models.ImageLocationState.COPYING
    first_canonical_token = claim.lease_owner
    assert first_canonical_token is not None
    assert first_canonical_token.startswith('worker-1:')
    assert state.claim_location(canonical.id, 'worker-2', 30) is None
    assert state.heartbeat_location(canonical.id, first_canonical_token, 30)
    assert not _complete_location(canonical.id, first_canonical_token,
                                  f'ecr/repo@{_OTHER_DIGEST}', _OTHER_DIGEST)
    assert state.get_location_by_id(
        canonical.id).state == models.ImageLocationState.FAILED

    assert state.retry_location(canonical.id)
    claim = state.claim_location(canonical.id, 'worker-2', 30)
    assert claim is not None
    second_canonical_token = claim.lease_owner
    assert second_canonical_token is not None
    assert _complete_location(canonical.id, second_canonical_token,
                              f'ecr/repo@{_DIGEST}', _DIGEST,
                              ('linux/amd64', 'linux/arm64'), 123)
    ready = state.get_image(image.id)
    assert ready.platforms == ('linux/amd64', 'linux/arm64')

    regional_target = profile.target('aws-us-west-2')
    location = _ensure_profile_location(image, profile, regional_target)
    assert location.state == models.ImageLocationState.PENDING
    revised = state.ensure_location(
        image.id,
        profile.name,
        regional_target.name,
        'f' * 64,
        _DIGEST,
        policy_fingerprint=profile.policy_fingerprint(regional_target, False),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical_location_id=canonical.id)
    assert revised.id != location.id
    claimed = state.claim_location(location.id, 'copy-1', 30)
    assert claimed.state == models.ImageLocationState.COPYING
    first_location_token = claimed.lease_owner
    assert first_location_token is not None
    assert first_location_token.startswith('copy-1:')
    assert state.claim_location(location.id, 'copy-2', 30) is None
    claimed_image = state.get_image(image.id)
    assert claimed_image is not None
    status_payload = core._response(claimed_image).model_dump()
    assert 'creator_user_hash' not in status_payload
    assert 'lease_owner' not in status_payload['locations'][0]
    with pytest.raises(TypeError, match='closed error code'):
        state.fail_location(
            location.id, first_location_token,
            'token=secret https://user:password@example.com/image')
    assert state.fail_location(
        location.id, first_location_token,
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED)
    failed_location = state.get_location_by_id(location.id)
    assert failed_location.last_error == (
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value)
    assert state.retry_location(location.id)
    second_location_claim = state.claim_location(location.id, 'copy-2', 30)
    assert second_location_claim is not None
    second_location_token = second_location_claim.lease_owner
    assert second_location_token is not None
    assert _complete_location(location.id, second_location_token,
                              f'ecr-west/repo@{_DIGEST}', _DIGEST)
    assert state.mark_location_missing(location.id)
    assert state.get_location_by_id(
        location.id).state == models.ImageLocationState.MISSING


def test_materialization_attempt_budget_requires_explicit_retry(
        image_state_engine, monkeypatch):
    now = 1000
    monkeypatch.setattr(state.time, 'time', lambda: now)
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(
            global_user_state.container_image_location_table.update().where(
                global_user_state.container_image_location_table.c.id ==
                canonical.id).values(attempt_count=19))
        session.commit()

    final_claim = state.claim_location(canonical.id, 'worker', 30)
    assert final_claim is not None
    assert final_claim.attempt_count == 20
    assert final_claim.lease_owner is not None
    assert state.fail_location(
        canonical.id,
        final_claim.lease_owner,
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED,
        retry_at=now + 1)
    exhausted = state.get_location_by_id(canonical.id)
    assert exhausted is not None
    assert exhausted.state == models.ImageLocationState.FAILED
    assert exhausted.next_retry_at is None
    assert state.claim_location(canonical.id, 'worker', 30) is None
    assert state.list_reconciliation_candidates('research', now=now + 100) == []

    assert state.retry_location(canonical.id)
    retried = state.claim_location(canonical.id, 'operator-retry', 30)
    assert retried is not None
    assert retried.attempt_count == 1


def test_crash_at_attempt_budget_is_operator_recoverable_for_copy_and_eviction(
        image_state_engine, monkeypatch):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    table = global_user_state.container_image_location_table
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(table.c.id == canonical.id).values(
            attempt_count=19))
        session.commit()
    copy_claim = state.claim_location(canonical.id, 'copy-at-budget', 1)
    assert copy_claim is not None
    assert copy_claim.attempt_count == 20
    now[0] += 2
    assert state.claim_location(canonical.id, 'automatic-copy', 30) is None
    assert state.claim_next_reconciliation_candidate(
        'research',
        'automatic-copy',
        materialization_lease_seconds=30,
        verification_lease_seconds=30,
        now=now[0]) is None
    assert state.retry_location(canonical.id)
    recovered_copy = state.claim_location(canonical.id, 'operator-copy', 30)
    assert recovered_copy is not None
    assert recovered_copy.attempt_count == 1

    regional_image, _ = _ready_regional_location(_OTHER_SOURCE, _OTHER_DIGEST)
    regional = next(
        location for location in state.list_locations(regional_image.id)
        if not location.canonical)
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(table.c.id == regional.id).values(
            attempt_count=19))
        session.commit()
    eviction = state.claim_location_eviction(regional.id, 'evict-at-budget', 1,
                                             cutoff)
    assert eviction is not None
    assert eviction.attempt_count == 20
    now[0] += 2
    assert state.claim_location_eviction(regional.id, 'automatic-evict', 30,
                                         cutoff) is None
    assert state.claim_next_eviction_candidate('research',
                                               'automatic-evict',
                                               lease_seconds=30,
                                               unused_before=cutoff,
                                               now=now[0]) is None
    assert state.retry_location(regional.id)
    recovered_eviction = state.claim_location(regional.id, 'operator-recopy',
                                              30)
    assert recovered_eviction is not None
    assert recovered_eviction.state == models.ImageLocationState.COPYING
    assert recovered_eviction.attempt_count == 1


def test_verification_attempt_budget_requires_explicit_retry(
        image_state_engine, monkeypatch):
    now = 1000
    monkeypatch.setattr(state.time, 'time', lambda: now)
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    materialization = state.claim_location(canonical.id, 'importer', 30)
    assert materialization is not None
    assert materialization.lease_owner is not None
    assert _complete_location(canonical.id, materialization.lease_owner,
                              f'ecr.example/repo@{_DIGEST}', _DIGEST)
    assert state.retry_location(canonical.id)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(
            global_user_state.container_image_location_table.update().where(
                global_user_state.container_image_location_table.c.id ==
                canonical.id).values(attempt_count=19))
        session.commit()

    final_claim = state.claim_location_verification(canonical.id, 'verifier',
                                                    30)
    assert final_claim is not None
    assert final_claim.attempt_count == 20
    assert final_claim.lease_owner is not None
    assert state.fail_location_verification(
        canonical.id,
        final_claim.lease_owner,
        models.ImageLocationErrorCode.REVALIDATION_FAILED,
        retry_at=now + 1)
    exhausted = state.get_location_by_id(canonical.id)
    assert exhausted is not None
    assert exhausted.state == models.ImageLocationState.READY
    assert exhausted.next_retry_at is None
    assert exhausted.verification_requested_at is None
    assert state.claim_location_verification(canonical.id, 'verifier',
                                             30) is None
    assert state.list_reconciliation_candidates('research', now=now + 100) == []

    assert state.retry_location(canonical.id)
    retried = state.claim_location_verification(canonical.id, 'operator-retry',
                                                30)
    assert retried is not None
    assert retried.attempt_count == 1


def test_release_versions_bind_immutably_to_one_digest(image_state_engine):
    del image_state_engine
    versioned = state.register_image(_SOURCE,
                                     _SOURCE,
                                     _DIGEST,
                                     'research',
                                     'user-1',
                                     release='boltz-2.1.0')
    duplicate = state.register_image(_SOURCE,
                                     _SOURCE,
                                     _DIGEST,
                                     'research',
                                     'user-2',
                                     release='boltz-2.1.0')
    assert duplicate.id == versioned.id
    assert state.get_image_by_release('boltz-2.1.0', 'research') == versioned

    other_source = f'ghcr.io/boltz-bio/boltz@{_OTHER_DIGEST}'
    with pytest.raises(ValueError, match='already bound'):
        state.register_image(other_source,
                             other_source,
                             _OTHER_DIGEST,
                             'research',
                             'user-2',
                             release='boltz-2.1.0')
    relabeled = state.register_image(_SOURCE,
                                     _SOURCE,
                                     _DIGEST,
                                     'research',
                                     'user-2',
                                     release='boltz-latest')
    assert relabeled.id == versioned.id
    assert [
        release.name
        for release in state.list_releases(versioned.id, 'research')
    ] == ['boltz-2.1.0', 'boltz-latest']

    unversioned = state.register_image(other_source, other_source,
                                       _OTHER_DIGEST, 'staging', 'user-1')
    bound = state.register_image(other_source,
                                 other_source,
                                 _OTHER_DIGEST,
                                 'staging',
                                 'user-1',
                                 release='boltz-2.2.0')
    assert bound.id == unversioned.id
    assert state.get_image_by_release('boltz-2.2.0', 'staging') == bound


def test_prepare_validates_and_registers_every_source_selector_field(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile = _profile()
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    artifact = state.register_image(_SOURCE,
                                    _SOURCE,
                                    _DIGEST,
                                    'research',
                                    'user-1',
                                    release='boltz-2.1.0')
    conflicting_source = f'ghcr.io/boltz-bio/other@{_OTHER_DIGEST}'
    with pytest.raises(ValueError, match='different immutable artifacts'):
        core.prepare(
            {
                'ref': conflicting_source,
                'release': 'boltz-2.1.0',
                'distribution': profile.name,
            }, ['canonical'], 'research')

    source_alias = f'ghcr.io/boltz-bio/boltz-alias@{_DIGEST}'
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-2'
        prepared = core.prepare(
            {
                'ref': source_alias,
                'release': 'boltz-2.1.0',
                'distribution': profile.name,
            }, ['canonical'], 'research')
    assert prepared.id == artifact.id
    assert state.get_image_by_source_ref(source_alias, 'research') == artifact

    bare_source_alias = f'ghcr.io/boltz-bio/bare-alias@{_DIGEST}'
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-3'
        bare_prepared = core.prepare(bare_source_alias, ['canonical'],
                                     'research', profile.name)
    assert bare_prepared.id == artifact.id
    assert state.get_image_by_source_ref(bare_source_alias,
                                         'research') == artifact


def test_prepare_validates_all_targets_before_writing_catalog_state(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile = _profile()
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))

    with pytest.raises(ValueError, match='Unknown target'):
        core.prepare(_OTHER_SOURCE, ['aws-us-west-2', 'misspelled-region'],
                     'research', profile.name)

    assert state.get_image_by_digest(_OTHER_DIGEST, 'research') is None
    assert state.get_image_by_source_ref(_OTHER_SOURCE, 'research') is None
    assert state.list_images('research') == []


def test_prepare_rolls_back_aliases_and_earlier_targets_on_late_conflict(
        image_state_engine):
    del image_state_engine
    profile = _profile()
    published = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    existing_locations = state.list_locations(published.id, profile.name)
    assert len(existing_locations) == 1
    existing_canonical = existing_locations[0]
    mirror = f'quay.io/boltz-bio/boltz@{_DIGEST}'
    publication = state.ImagePublication(
        source_ref=mirror,
        resolved_source_ref=mirror,
        source_digest=_DIGEST,
        workspace='research',
        creator_user_hash='user-2',
        release='prepare-rollback',
        profile=profile.name,
        target_id=profile.canonical.name,
        target_fingerprint=profile.physical_fingerprint(profile.canonical),
        policy_fingerprint=profile.policy_fingerprint(profile.canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
    )
    new_canonical_fingerprint = '1' * 64
    assert new_canonical_fingerprint != existing_canonical.target_fingerprint

    with pytest.raises(ValueError, match='canonical and regional'):
        state.prepare_image_atomically(
            existing_image_id=None,
            publication=publication,
            workspace='research',
            profile=profile.name,
            profile_revision=profile.revision,
            profile_revision_fingerprint=profile.revision_fingerprint,
            expected_digest=_DIGEST,
            intents=[
                state.ImageLocationIntent(
                    target_id='new-canonical',
                    target_fingerprint=new_canonical_fingerprint,
                    policy_fingerprint='2' * 64,
                    canonical=True,
                ),
                state.ImageLocationIntent(
                    target_id='conflicting-regional',
                    target_fingerprint=existing_canonical.target_fingerprint,
                    policy_fingerprint='3' * 64,
                    canonical=False,
                    auto_evict=True,
                ),
            ])

    assert state.get_image_by_source_ref(mirror, 'research') is None
    assert state.get_release('prepare-rollback', 'research') is None
    assert state.list_locations(published.id,
                                profile.name) == existing_locations


def test_publish_transaction_rolls_back_every_losing_binding(
        image_state_engine):
    del image_state_engine
    profile = _profile()
    published = _publish_state_image(_SOURCE, _DIGEST, profile=profile)

    with pytest.raises(ValueError, match='already bound'):
        _publish_state_image(_OTHER_SOURCE, _OTHER_DIGEST, profile=profile)

    assert state.get_image_by_digest(_OTHER_DIGEST, 'research') is None
    assert state.get_image_by_source_ref(_OTHER_SOURCE, 'research') is None
    assert state.get_release('boltz-production',
                             'research').image_id == (published.id)
    assert state.list_locations(published.id)[0].canonical

    repeated = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    assert repeated.id == published.id
    assert len(state.list_sources(published.id, 'research')) == 1
    assert len(state.list_releases(published.id, 'research')) == 1
    assert len(state.list_locations(published.id, profile.name)) == 1

    conflicting_fingerprint = '0' * 64
    if conflicting_fingerprint == profile.revision_fingerprint:
        conflicting_fingerprint = '1' * 64
    with pytest.raises(ValueError, match='revision'):
        _publish_state_image(
            _OTHER_SOURCE,
            _OTHER_DIGEST,
            release='profile-conflict',
            profile=profile,
            profile_revision_fingerprint=conflicting_fingerprint)

    assert state.get_image_by_digest(_OTHER_DIGEST, 'research') is None
    assert state.get_image_by_source_ref(_OTHER_SOURCE, 'research') is None
    assert state.get_release('profile-conflict', 'research') is None


def test_catalog_quotas_fail_atomically_without_orphan_aliases(
        image_state_engine):
    del image_state_engine
    profile = _profile()
    canonical = profile.canonical

    def _publish(source: str, digest: str,
                 release: str | None) -> state.ImageRecord:
        return state.publish_image(
            source_ref=source,
            resolved_source_ref=source,
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
            max_artifacts=1,
            max_sources_per_artifact=1,
            max_releases_per_artifact=1,
        )

    image = _publish(_SOURCE, _DIGEST, 'boltz-production')
    mirror = f'quay.io/boltz-bio/boltz@{_DIGEST}'
    with pytest.raises(ValueError, match='source alias quota'):
        _publish(mirror, _DIGEST, 'source-quota-release')
    assert state.get_image_by_source_ref(mirror, 'research') is None
    assert state.get_release('source-quota-release', 'research') is None

    with pytest.raises(ValueError, match='release alias quota'):
        _publish(_SOURCE, _DIGEST, 'second-release')
    assert state.get_release('second-release', 'research') is None

    with pytest.raises(ValueError, match='artifact quota'):
        _publish(_OTHER_SOURCE, _OTHER_DIGEST, 'other-release')
    assert state.get_image_by_digest(_OTHER_DIGEST, 'research') is None
    assert state.get_image_by_source_ref(_OTHER_SOURCE, 'research') is None
    assert state.get_release('other-release', 'research') is None
    assert state.list_images('research') == [image]


def test_failed_canonical_republish_rotates_bound_source_and_fences_old_lease(
        image_state_engine, monkeypatch):
    profile = _profile()
    mirror = f'quay.io/boltz-bio/boltz-mirror@{_DIGEST}'
    clock = [100]
    monkeypatch.setattr(state.time, 'time', lambda: clock[0])
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'research')
    adapter = mock.Mock()
    adapter.resolve_runtime_pull_auth.return_value = 'ecr_runtime_identity'
    monkeypatch.setattr(providers, 'get_adapter', lambda _: adapter)

    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    original_source = state.get_source(_SOURCE, 'research')
    assert original_source is not None
    assert canonical.source_id == original_source.id

    old_claim = state.claim_location(canonical.id, 'old-source-worker', 10)
    assert old_claim is not None
    assert old_claim.lease_owner is not None
    with pytest.raises(state.ProfileRevisionBusyError, match='source binding'):
        _publish_state_image(mirror, _DIGEST, profile=profile)
    assert state.get_source(mirror, 'research') is None

    # Exact expiry makes the old worker ineligible to complete. The new
    # publication atomically binds the canonical intent to the new immutable
    # source and clears the expired lease before any worker can claim it.
    clock[0] = 110
    republished = _publish_state_image(mirror, _DIGEST, profile=profile)
    assert republished.id == image.id
    mirror_source = state.get_source(mirror, 'research')
    assert mirror_source is not None
    rotated = state.get_location_by_id(canonical.id)
    assert rotated is not None
    assert rotated.source_id == mirror_source.id
    assert rotated.state == models.ImageLocationState.PENDING

    # Artifact/release operations carry no explicit source. They must retain
    # the canonical intent's rotated source B rather than defaulting back to
    # the artifact's oldest alias A. Explicit ref B launch resolution has the
    # same invariant.
    core.prepare({
        'release': 'boltz-production',
        'distribution': profile.name,
    }, ['canonical'],
                 workspace='research')
    core.retry('boltz-production',
               'canonical',
               workspace='research',
               distribution=profile.name)
    placement = models.Placement(provider='aws',
                                 region='us-east-1',
                                 backend='vm')
    selectors = ({
        'release': 'boltz-production',
        'distribution': profile.name,
    }, {
        'artifact_id': image.id,
        'distribution': profile.name,
    }, {
        'ref': mirror,
        'distribution': profile.name,
    })
    for selector in selectors:
        spec = models.ContainerImage.from_config(selector)
        selected = core._record_for_selector(spec, 'research')
        assert selected is not None
        core._ensure_for_placement(selected, profile, placement)
        assert state.get_location_by_id(
            canonical.id).source_id == mirror_source.id

    active_b = state.claim_location(canonical.id, 'active-source-b', 10)
    assert active_b is not None
    assert active_b.lease_owner is not None
    core._ensure_for_placement(image, profile, placement)
    during_b = state.get_location_by_id(canonical.id)
    assert during_b is not None
    assert during_b.source_id == mirror_source.id
    assert during_b.lease_owner == active_b.lease_owner
    assert state.fail_location(
        canonical.id, active_b.lease_owner,
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED)
    assert state.retry_location(canonical.id)

    old_destination = references.managed_reference(profile, profile.canonical,
                                                   'research', _SOURCE, _DIGEST)
    assert not _complete_location(canonical.id, old_claim.lease_owner,
                                  old_destination, _DIGEST)

    copied = []

    def copy(source: str, destination: str, digest: str,
             _) -> worker.MaterializationResult:
        copied.append((source, destination, digest))
        return _materialization(digest)

    assert worker.materialize_location(canonical.id, 'new-source-worker', copy)
    assert copied[0][0] == mirror
    assert copied[0][1] == references.managed_reference(profile,
                                                        profile.canonical,
                                                        'research', mirror,
                                                        _DIGEST)
    ready = state.get_location_by_id(canonical.id)
    assert ready is not None
    assert ready.state == models.ImageLocationState.READY
    assert ready.source_id == mirror_source.id

    # The durable cluster transaction must derive its expected destination
    # from the canonical intent's rotated source binding too. Both a logical
    # release and the newly published source alias select the same artifact;
    # neither may be rejected because the artifact row remembers source A.
    assert ready.target_ref is not None
    runtime_login = _ecr_runtime_login(ready.target_ref)
    adapter.runtime_login_config.return_value = runtime_login
    resolved = _resolved_location(image, ready, ready.target_ref)
    launch_selectors = ({
        'release': 'boltz-production',
        'distribution': profile.name,
    }, {
        'ref': mirror,
        'distribution': profile.name,
    })
    cluster_names = ('rotated-release-source', 'rotated-explicit-source')
    for cluster_name, selector in zip(cluster_names, launch_selectors):
        launched = resources_lib.Resources(cloud='aws',
                                           region='us-east-1',
                                           container_image=selector,
                                           _resolved_container_image=resolved,
                                           _docker_login_config=runtime_login)
        global_user_state.add_or_update_cluster(cluster_name,
                                                types.SimpleNamespace(
                                                    launched_resources=launched,
                                                    launched_nodes=1),
                                                requested_resources=None,
                                                ready=False)

    with sqlalchemy.orm.Session(image_state_engine) as session:
        references_by_consumer = dict(
            session.execute(
                sqlalchemy.select(
                    global_user_state.container_image_reference_table.c.
                    consumer_id, global_user_state.
                    container_image_reference_table.c.location_id).where(
                        global_user_state.container_image_reference_table.c.
                        consumer_id.in_(cluster_names))).all())
    assert references_by_consumer == dict.fromkeys(cluster_names, ready.id)


def test_regional_route_survives_canonical_source_repository_rotation(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile = _profile()
    mirror = f'quay.io/different/repository@{_DIGEST}'
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    adapter = mock.Mock()
    adapter.resolve_runtime_pull_auth.return_value = 'ecr_runtime_identity'
    monkeypatch.setattr(providers, 'get_adapter', lambda _: adapter)

    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    assert worker.materialize_location(canonical.id, 'canonical-a',
                                       lambda *args: _materialization())
    original_canonical = state.get_location_by_id(canonical.id)
    assert original_canonical is not None
    assert original_canonical.target_ref is not None

    regional = _ensure_profile_location(image,
                                        profile,
                                        profile.target('aws-us-west-2'),
                                        auto_evict=True)
    assert worker.materialize_location(regional.id, 'regional-a',
                                       lambda *args: _materialization())
    original_regional = state.get_location_by_id(regional.id)
    assert original_regional is not None
    assert original_regional.state == models.ImageLocationState.READY
    assert original_regional.target_ref is not None

    assert state.mark_location_missing(canonical.id)
    republished = _publish_state_image(mirror, _DIGEST, profile=profile)
    assert republished.id == image.id
    mirror_source = state.get_source(mirror, 'research')
    assert mirror_source is not None
    rotated = state.get_location_by_id(canonical.id)
    assert rotated is not None
    assert rotated.source_id == mirror_source.id
    assert rotated.state == models.ImageLocationState.PENDING
    still_ready_regional = state.get_location_by_id(regional.id)
    assert still_ready_regional is not None
    assert still_ready_regional.state == models.ImageLocationState.READY
    assert still_ready_regional.target_ref == original_regional.target_ref

    copied = []

    def copy(source: str, destination: str, digest: str,
             _) -> worker.MaterializationResult:
        copied.append((source, destination, digest))
        return _materialization(digest)

    assert worker.materialize_location(canonical.id, 'canonical-b', copy)
    assert copied == [(mirror, original_canonical.target_ref, _DIGEST)]

    placement = models.Placement(provider='aws',
                                 region='us-west-2',
                                 backend='vm')
    routes = core.routes_for_image(image, profile, placement)
    regional_routes = [
        route for route in routes if route.location_id == regional.id
    ]
    assert len(regional_routes) == 1
    assert regional_routes[0].reference == original_regional.target_ref

    assert state.retry_location(regional.id)
    inspected = []
    assert worker.revalidate_location(
        regional.id, 'regional-verifier',
        lambda reference, _: inspected.append(reference) or _DIGEST)
    assert inspected == [original_regional.target_ref]
    revalidated = state.get_location_by_id(regional.id)
    assert revalidated is not None
    assert revalidated.state == models.ImageLocationState.READY


def test_task_release_version_resolves_to_bound_digest(image_state_engine,
                                                       monkeypatch):
    del image_state_engine
    _mock_config(
        monkeypatch, {
            'container_registries': {
                'default_profile': 'managed',
                'profiles': {
                    'managed': {
                        'ownership': 'managed',
                        'realm': 'production',
                        'namespace': 'skypilot/{workspace}',
                        'canonical': {
                            'provider': 'aws',
                            'account': '123456789012',
                            'region': 'us-east-1',
                            'pull_auth': 'ecr_runtime_identity',
                        },
                    },
                },
            },
        })
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        registered = core.publish({
            'ref': _SOURCE,
            'release': 'boltz-2.1.0',
        }, 'research')
    assert registered.releases == ['boltz-2.1.0']
    assert core.status('boltz-2.1.0', 'research')[0].id == registered.id

    profile, _ = config.resolve_profile(None, 'research')
    assert profile is not None
    canonical = state.get_location(
        registered.id, profile.name, profile.canonical.name,
        profile.materialization_fingerprint(profile.canonical))
    assert canonical is not None
    assert [(location.target_id, location.state)
            for location in state.list_locations(registered.id)] == [
                ('canonical', models.ImageLocationState.PENDING),
            ]
    claim = state.claim_location(canonical.id, 'importer', 30)
    assert claim is not None
    assert claim.lease_owner is not None
    target_ref = references.managed_reference(profile, profile.canonical,
                                              'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, claim.lease_owner, target_ref,
                              _DIGEST, _TEST_PLATFORMS, None)
    requested = resources_lib.Resources(container_image={
        'release': 'boltz-2.1.0',
    })
    resolved = core.resolve_for_placement(
        requested,
        models.Placement(provider='aws', region='us-east-1', backend='vm'),
        'research')
    assert resolved.resolved_container_image.image_id == registered.id
    assert resolved.resolved_container_image.digest == _DIGEST


def test_regional_cache_eviction_is_managed_fenced_and_repairable(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    _allow_manifest_deletion(monkeypatch)
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    image, target_ref = _ready_regional_location()

    # External profiles use the same preparation state machine but can never
    # grant SkyPilot deletion authority.
    other_source = f'ghcr.io/boltz-bio/boltz@{_OTHER_DIGEST}'
    external, _ = _ready_regional_location(other_source,
                                           _OTHER_DIGEST,
                                           auto_evict=False)
    regional = next(location for location in state.list_locations(image.id)
                    if location.target_id == 'aws-us-west-2')
    external_regional = next(
        location for location in state.list_locations(external.id)
        if location.target_id == 'aws-us-west-2')

    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    candidates = state.list_eviction_candidates('research', cutoff, 10)
    assert [
        (candidate.image_id, candidate.target_id) for candidate in candidates
    ] == [(image.id, 'aws-us-west-2')]
    assert state.claim_location_eviction(external_regional.id, 'evictor', 30,
                                         cutoff) is None

    deleted = []
    assert worker.evict_location(regional.id, 'evictor', cutoff,
                                 lambda reference, _: deleted.append(reference))
    assert deleted == [target_ref]
    evicted = state.get_location_by_id(regional.id)
    assert evicted is not None
    assert evicted.state == models.ImageLocationState.EVICTED
    assert evicted.target_ref is None

    target = profile.target('aws-us-west-2')
    requeued = _ensure_profile_location(image, profile, target, auto_evict=True)
    assert requeued.state == models.ImageLocationState.PENDING
    copy_claim = state.claim_location(requeued.id, 'copier-2', 30)
    assert copy_claim is not None
    assert copy_claim.lease_owner is not None
    assert _complete_location(requeued.id, copy_claim.lease_owner, target_ref,
                              _DIGEST)

    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS

    def fail_delete(_: str, __) -> None:
        raise RuntimeError('token=do-not-persist')

    assert not worker.evict_location(requeued.id, 'evictor', cutoff,
                                     fail_delete)
    failed = state.get_location_by_id(requeued.id)
    assert failed is not None
    assert failed.state == models.ImageLocationState.MISSING
    assert failed.target_ref is None
    assert 'do-not-persist' not in (failed.last_error or '')
    assert failed.last_error == models.ImageLocationErrorCode.EVICTION_FAILED.value


def test_eviction_rechecks_current_deletion_authority(image_state_engine,
                                                      monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    managed = _profile()
    image, _ = _ready_regional_location()
    regional = next(location for location in state.list_locations(image.id)
                    if not location.canonical)
    external = models.RegistryProfile(**{
        **managed.__dict__,
        'ownership': models.RegistryOwnership.EXTERNAL,
    })
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (external, models.WorkspaceImagePolicy()))
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    delete = mock.Mock()
    assert not worker.evict_location(regional.id, 'evictor', cutoff, delete)
    delete.assert_not_called()
    retained = state.get_location_by_id(regional.id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.READY
    assert retained.target_ref is not None


def test_eviction_requires_provider_repository_ownership_proof(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    image, _ = _ready_regional_location()
    regional = next(location for location in state.list_locations(image.id)
                    if not location.canonical)
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    delete = mock.Mock()

    assert not worker.evict_location(regional.id, 'evictor', cutoff, delete)
    delete.assert_not_called()
    retained = state.get_location_by_id(regional.id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.READY
    assert retained.target_ref is not None
    assert retained.last_error == models.ImageLocationErrorCode.EVICTION_FAILED.value


def test_regional_verification_and_eviction_completion_require_exact_canonical(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image, _ = _ready_regional_location()
    locations = state.list_locations(image.id)
    canonical = next(location for location in locations if location.canonical)
    regional = next(
        location for location in locations if not location.canonical)
    assert canonical.target_ref is not None
    canonical_ref = canonical.target_ref

    assert state.retry_location(regional.id)
    assert state.mark_location_missing(canonical.id)
    assert regional.id not in {
        candidate.id
        for candidate in state.list_reconciliation_candidates('research')
    }
    assert state.claim_location_verification(regional.id, 'verifier',
                                             30) is None

    assert state.retry_location(canonical.id)
    canonical_claim = state.claim_location(canonical.id, 'repairer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    verification = state.claim_location_verification(regional.id, 'verifier',
                                                     30)
    assert verification is not None
    assert verification.lease_owner is not None
    assert state.complete_location_verification(regional.id,
                                                verification.lease_owner,
                                                _DIGEST)

    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    eviction = state.claim_location_eviction(regional.id, 'evictor', 30, cutoff)
    assert eviction is not None
    assert eviction.lease_owner is not None
    assert state.mark_location_missing(canonical.id)
    assert not state.complete_location_eviction(regional.id,
                                                eviction.lease_owner)
    retained = state.get_location_by_id(regional.id)
    assert retained is not None
    assert retained.state == models.ImageLocationState.EVICTING


def test_worker_marks_regional_missing_when_canonical_changes_during_delete(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    _allow_manifest_deletion(monkeypatch)
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    image, _ = _ready_regional_location()
    locations = state.list_locations(image.id)
    canonical = next(location for location in locations if location.canonical)
    regional = next(
        location for location in locations if not location.canonical)
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS

    def delete(_: str, __) -> None:
        assert state.mark_location_missing(canonical.id)

    assert not worker.evict_location(regional.id, 'evictor', cutoff, delete)
    missing = state.get_location_by_id(regional.id)
    assert missing is not None
    assert missing.state == models.ImageLocationState.MISSING
    assert missing.target_ref is None


def test_regional_state_refuses_canonical_manifest_alias(
        image_state_engine, monkeypatch):
    del image_state_engine
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    profile = _profile()
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    canonical_claim = state.claim_location(canonical.id, 'importer', 30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    canonical_ref = f'ecr.example/repo@{_DIGEST}'
    assert _complete_location(canonical.id, canonical_claim.lease_owner,
                              canonical_ref, _DIGEST)
    target = profile.target('aws-us-west-2')
    location = _ensure_profile_location(image, profile, target, auto_evict=True)
    copy_claim = state.claim_location(location.id, 'copier', 30)
    assert copy_claim is not None
    assert copy_claim.lease_owner is not None
    assert not _complete_location(location.id, copy_claim.lease_owner,
                                  canonical_ref, _DIGEST)
    assert state.get_location_by_id(
        location.id).state == models.ImageLocationState.FAILED

    # The catalog constraint prevents two logical routes from publishing the
    # same physical manifest reference.
    table = global_user_state.container_image_location_table
    with sqlalchemy.orm.Session(state._engine()) as session:
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            session.execute(
                table.update().where(table.c.id == location.id).values(
                    state=models.ImageLocationState.READY.value,
                    target_ref=canonical_ref,
                    last_used_at=1,
                    next_retry_at=None))
        session.rollback()

    # A corrupted mutable legacy reference still cannot cross the final
    # eviction guard.
    with sqlalchemy.orm.Session(state._engine()) as session:
        session.execute(table.update().where(table.c.id == location.id).values(
            state=models.ImageLocationState.READY.value,
            target_ref='ecr-west.example/repo:mutable'))
        session.commit()
    delete = mock.Mock()
    assert not worker.evict_location(location.id, 'evictor', 1000, delete)
    delete.assert_not_called()


def test_eviction_policy_protects_durable_references_and_acquire_wins_race(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image, target_ref = _ready_regional_location()
    _mock_config(
        monkeypatch, {
            'workspaces': {
                'research': {
                    'container_images': {
                        'regional_cache_retention_weeks': 8,
                    },
                },
            },
        })

    now[0] += 8 * _WEEK_SECONDS + 1
    location = next(location for location in state.list_locations(image.id)
                    if location.target_id == 'aws-us-west-2')
    state.acquire_reference(location.id,
                            'research',
                            'service',
                            'cluster-1',
                            expected_ref=target_ref)
    assert core.eviction_candidates('research', now=now[0]) == []
    assert state.release_reference('research', 'service', 'cluster-1')
    now[0] += 8 * _WEEK_SECONDS + 1
    candidates = core.eviction_candidates('research', now=now[0])
    assert [candidate.image_id for candidate in candidates] == [image.id]

    stale_cutoff = now[0] - 8 * _WEEK_SECONDS
    state.acquire_reference(location.id,
                            'research',
                            'service',
                            'cluster-2',
                            expected_ref=target_ref)
    assert state.claim_location_eviction(location.id, 'evictor', 30,
                                         stale_cutoff) is None


def test_reference_acquire_returns_its_locked_transaction_snapshot(
        image_state_engine, monkeypatch):
    del image_state_engine
    image, target_ref = _ready_regional_location()
    location = next(location for location in state.list_locations(image.id)
                    if location.target_id == 'aws-us-west-2')

    def _post_commit_lookup_is_a_race(_):
        raise AssertionError('acquire_reference must return its locked row')

    monkeypatch.setattr(state, 'get_reference', _post_commit_lookup_is_a_race)
    first = state.acquire_reference(location.id,
                                    'research',
                                    'service',
                                    'service-1',
                                    expected_ref=target_ref)
    second = state.acquire_reference(location.id,
                                     'research',
                                     'service',
                                     'service-1',
                                     expected_ref=target_ref)
    assert first.id == second.id
    assert second.location_id == location.id


def test_new_regional_references_recheck_exact_canonical(
        image_state_engine, monkeypatch):
    _mock_registry_profile(monkeypatch)
    image, target_ref = _ready_regional_location()
    locations = state.list_locations(image.id)
    canonical = next(location for location in locations if location.canonical)
    regional = next(
        location for location in locations if not location.canonical)
    assert state.mark_location_missing(canonical.id)

    with pytest.raises(ValueError, match='no longer READY'):
        state.acquire_reference(regional.id,
                                'research',
                                'service',
                                'standalone-stale-route',
                                expected_ref=target_ref)

    with pytest.raises(ValueError, match='committed atomically'):
        state.acquire_reference(regional.id,
                                'research',
                                'cluster',
                                'must-use-cluster-transaction',
                                expected_ref=target_ref)

    resolved = _resolved_location(image, regional, target_ref)
    launched = resources_lib.Resources(
        cloud='aws',
        region='us-west-2',
        container_image=_SOURCE,
        _resolved_container_image=resolved,
        _docker_login_config=_ecr_runtime_login(target_ref))
    handle = types.SimpleNamespace(launched_resources=launched,
                                   launched_nodes=1)
    with pytest.raises(ValueError, match='no longer READY'):
        global_user_state.add_or_update_cluster('atomic-stale-route',
                                                handle,
                                                requested_resources=None,
                                                ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id.
                in_(['standalone-stale-route',
                     'atomic-stale-route']))).first() is None
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'atomic-stale-route')).first() is None


def test_committed_launch_acquires_durable_materialization_reference(
        image_state_engine, monkeypatch):
    _mock_registry_profile(monkeypatch)
    image, target_ref = _ready_regional_location()
    location = next(location for location in state.list_locations(image.id)
                    if location.target_id == 'aws-us-west-2')
    regional = _resolved_location(image, location, target_ref)
    launched = resources_lib.Resources(
        cloud='aws',
        region='us-west-2',
        container_image=_SOURCE,
        _resolved_container_image=regional,
        _docker_login_config=_ecr_runtime_login(target_ref),
    )
    handle = types.SimpleNamespace(launched_resources=launched,
                                   launched_nodes=1)
    global_user_state.add_or_update_cluster('stopped-regional',
                                            handle,
                                            requested_resources=None,
                                            ready=False)
    assert state.list_eviction_candidates('research', 10**10, 10) == []
    with pytest.raises(ValueError, match='released atomically'):
        state.release_reference('research', 'cluster', 'stopped-regional')
    assert state.list_eviction_candidates('research', 10**10, 10) == []

    substituted = launched.copy(container_image='ubuntu:22.04')
    with pytest.raises(ValueError, match='metadata-only'):
        global_user_state.update_cluster_handle(
            'stopped-regional',
            types.SimpleNamespace(launched_resources=substituted,
                                  launched_nodes=1))
    stored_handle = global_user_state.get_handle_from_cluster_name(
        'stopped-regional')
    assert global_user_state._container_image_execution_state(
        stored_handle.launched_resources
    ) == global_user_state._container_image_execution_state(launched)
    assert state.list_eviction_candidates('research', 10**10, 10) == []

    with sqlalchemy.orm.Session(image_state_engine) as session:
        reference = session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'stopped-regional')).mappings().one()
        assert reference['location_id'] == location.id

        session.execute(
            global_user_state.container_image_location_table.update().where(
                global_user_state.container_image_location_table.c.id ==
                location.id).values(updated_at=123))
        session.commit()

    # A status refresh for an already pinned running cluster does not recheck
    # transient catalog health and does not write the image row. The existing
    # durable reference remains the eviction fence.
    assert state.mark_location_missing(location.id)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(
            global_user_state.container_image_location_table.update().where(
                global_user_state.container_image_location_table.c.id ==
                location.id).values(updated_at=123))
        session.commit()
    global_user_state.add_or_update_cluster('stopped-regional',
                                            handle,
                                            requested_resources=None,
                                            ready=False,
                                            is_launch=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        refreshed_location = session.execute(
            global_user_state.container_image_location_table.select().where(
                global_user_state.container_image_location_table.c.id ==
                location.id)).mappings().one()
        assert refreshed_location['updated_at'] == 123
        reference = session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'stopped-regional')).mappings().one()
        assert reference['location_id'] == location.id

    # A real relaunch must revalidate the route even when the durable reference
    # and serialized handle are unchanged. Only status refreshes may use the
    # no-op fast path above.
    with pytest.raises(ValueError, match='no longer READY'):
        global_user_state.add_or_update_cluster('stopped-regional',
                                                handle,
                                                requested_resources=None,
                                                ready=False,
                                                is_launch=True)

    # Replacing the durable handle with a direct image releases the old fence
    # in the same transaction.
    direct_handle = types.SimpleNamespace(
        launched_resources=resources_lib.Resources(cloud='aws'),
        launched_nodes=1)
    global_user_state.add_or_update_cluster('stopped-regional',
                                            direct_handle,
                                            requested_resources=None,
                                            ready=False,
                                            is_launch=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'stopped-regional')).first() is None

    # A stale route rolls back both a new cluster row and reference insertion.
    with pytest.raises(ValueError, match='no longer READY'):
        global_user_state.add_or_update_cluster('stale-route',
                                                handle,
                                                requested_resources=None,
                                                ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'stale-route')).first() is None
        assert session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'stale-route')).first() is None


def test_expired_eviction_lease_is_discoverable_and_fenced(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    _allow_manifest_deletion(monkeypatch)
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    image, target_ref = _ready_regional_location()
    location = next(location for location in state.list_locations(image.id)
                    if location.target_id == 'aws-us-west-2')
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    first = state.claim_location_eviction(location.id, 'evictor-1', 30, cutoff)
    assert first is not None
    first_token = first.lease_owner
    assert first_token is not None

    now[0] += 31
    candidates = state.list_eviction_candidates('research', cutoff, 10)
    assert [candidate.image_id for candidate in candidates] == [image.id]
    deleted = []
    assert worker.evict_location(location.id, 'evictor-2', cutoff,
                                 lambda reference, _: deleted.append(reference))
    assert deleted == [target_ref]
    assert not state.complete_location_eviction(location.id, first_token)


def test_new_cluster_reference_waits_for_ready_verification(
        image_state_engine, monkeypatch):
    _mock_registry_profile(monkeypatch)
    image, target_ref = _ready_regional_location()
    location = next(location for location in state.list_locations(image.id)
                    if location.target_id == 'aws-us-west-2')
    assert state.retry_location(location.id)
    verification = state.claim_location_verification(location.id, 'verifier',
                                                     30)
    assert verification is not None
    resolved = _resolved_location(image, location, target_ref)
    handle = types.SimpleNamespace(
        launched_resources=resources_lib.Resources(
            cloud='aws',
            region='us-west-2',
            container_image=_SOURCE,
            _resolved_container_image=resolved,
            _docker_login_config=_ecr_runtime_login(target_ref),
        ),
        launched_nodes=1,
    )
    with pytest.raises(ValueError, match='being verified'):
        global_user_state.add_or_update_cluster('new-during-verification',
                                                handle,
                                                requested_resources=None,
                                                ready=False)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        reference = session.execute(
            global_user_state.container_image_reference_table.select().where(
                global_user_state.container_image_reference_table.c.consumer_id
                == 'new-during-verification')).first()
        assert reference is None


def test_periodic_worker_sweep_applies_workspace_retention(
        image_state_engine, monkeypatch):
    engine = image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    _allow_manifest_deletion(monkeypatch)
    monkeypatch.setattr(config, 'resolve_profile', lambda *_:
                        (profile, models.WorkspaceImagePolicy()))
    image, target_ref = _ready_regional_location()
    policy = {
        'workspaces': {
            'research': {
                'container_images': {
                    'regional_cache_retention_weeks': 8,
                },
            },
        },
    }
    _mock_config(monkeypatch, policy)
    now[0] += 8 * _WEEK_SECONDS + 1
    deleted = []
    statements = []

    def _capture_statement(_connection, _cursor, statement, _parameters,
                           _context, _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _capture_statement)
    try:
        result = worker.sweep_evictions(
            'research',
            'eviction-worker',
            lambda image_id, target_id, reference, cancel_event: deleted.append(
                (image_id, target_id, reference)),
            now=now[0],
        )
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                _capture_statement)
    assert result == worker.EvictionSweepResult(candidates=1,
                                                evicted=1,
                                                failed=0)
    assert deleted == [(image.id, 'aws-us-west-2', target_ref)]
    claim_sql = '\n'.join(statements).upper()
    assert 'GROUP BY' not in claim_sql
    assert 'EXISTS' in claim_sql
    assert 'CONTAINER_IMAGE_PROFILE_REVISIONS' in claim_sql

    policy['workspaces']['research']['container_images'][
        'regional_cache_retention_weeks'] = None
    assert worker.sweep_evictions('research',
                                  'eviction-worker',
                                  mock.Mock(),
                                  now=now[0]) == worker.EvictionSweepResult(
                                      candidates=0, evicted=0, failed=0)


def test_eviction_queue_seeks_past_candidate_that_loses_fence(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    first_image, _ = _ready_regional_location()
    second_image, _ = _ready_regional_location(_OTHER_SOURCE, _OTHER_DIGEST)
    regional_locations = [
        location for image in (first_image, second_image)
        for location in state.list_locations(image.id) if not location.canonical
    ]
    assert all(
        location.last_used_at is not None for location in regional_locations)
    regional_locations.sort(
        key=lambda location: (location.last_used_at, location.id))
    regional_ids = {location.id for location in regional_locations}
    first_position = regional_locations[0]
    with state._PROFILE_CURSOR_LOCK:
        state._EVICTION_CANDIDATE_CURSORS[('eviction-claim-candidate',
                                           'research', 'managed', 1,
                                           2)] = (first_position.last_used_at,
                                                  first_position.id)
    now[0] += 8 * _WEEK_SECONDS + 1
    original_lock = state._lock_exact_canonical_ready
    attempted = []

    def _lose_first_fence(session, location_id):
        attempted.append(location_id)
        if len(attempted) == 1:
            return False
        return original_lock(session, location_id)

    monkeypatch.setattr(state, '_lock_exact_canonical_ready', _lose_first_fence)
    claimed = state.claim_next_eviction_candidate(
        'research',
        'eviction-worker',
        lease_seconds=30,
        unused_before=now[0] - 8 * _WEEK_SECONDS,
        now=now[0],
    )

    assert claimed is not None
    assert claimed.id in regional_ids
    assert attempted[0] != claimed.id
    assert len(attempted) >= 2


def test_eviction_filters_blocked_canonical_dependencies_before_seek(
        image_state_engine):
    del image_state_engine
    for index in range(70):
        digest = f'sha256:{index + 16384:064x}'
        source = f'registry.example.com/eviction-blocked-{index}@{digest}'
        image, _ = _ready_regional_location(source, digest)
        canonical = next(location for location in state.list_locations(image.id)
                         if location.canonical)
        assert state.mark_location_missing(canonical.id)

    eligible_digest = f'sha256:{32768:064x}'
    eligible_source = f'registry.example.com/eviction-eligible@{eligible_digest}'
    eligible_image, _ = _ready_regional_location(eligible_source,
                                                 eligible_digest)
    eligible = next(
        location for location in state.list_locations(eligible_image.id)
        if not location.canonical)
    now = int(state.time.time()) + 8 * _WEEK_SECONDS + 1
    original_lock = state._lock_exact_canonical_ready
    with mock.patch.object(state,
                           '_lock_exact_canonical_ready',
                           wraps=original_lock) as lock_canonical:
        claimed = state.claim_next_eviction_candidate('research',
                                                      'eviction-worker',
                                                      lease_seconds=30,
                                                      unused_before=now -
                                                      8 * _WEEK_SECONDS,
                                                      now=now)

    assert claimed is not None
    assert claimed.id == eligible.id
    lock_canonical.assert_called_once_with(mock.ANY, eligible.id)


def test_catalog_diagnostics_accept_only_closed_error_codes():
    assert state._error_code(models.ImageLocationErrorCode.
                             MATERIALIZATION_FAILED) == 'materialization_failed'
    for error in ('{"password":"json-value"}',
                  'Authorization: Bearer bearer-value',
                  'https://user:url-value@registry.example.com/repo'):
        with pytest.raises(TypeError, match='closed error code'):
            state._error_code(error)


def test_distribution_revisions_coexist_without_changing_artifact_identity(
        image_state_engine, monkeypatch):
    del image_state_engine
    profile_config = {
        'ownership': 'managed',
        'realm': 'production',
        'organization': 'boltz',
        'namespace': 'skypilot/{organization}/{workspace}',
        'canonical': {
            'provider': 'aws',
            'account': '123456789012',
            'region': 'us-east-1',
            'pull_auth': 'ecr_runtime_identity',
        },
    }
    data = {
        'container_registries': {
            'default_profile': 'managed',
            'profiles': {
                'managed': profile_config,
            },
        },
        'workspaces': {
            'research': {
                'container_images': {
                    'default_profile': 'managed',
                    'allowed_profiles': ['managed'],
                },
            },
        },
    }
    _mock_config(monkeypatch, data)
    with mock.patch.object(common_utils, 'get_current_user') as current_user:
        current_user.return_value.id = 'user-1'
        registered = core.register(_SOURCE, 'research')

    # Every complete administrative configuration has a monotonic revision;
    # adding a target does not change artifact identity.
    profile_config['targets'] = [{
        'name': 'aws-us-west-2',
        'provider': 'aws',
        'account': '123456789012',
        'region': 'us-west-2',
        'pull_auth': 'ecr_runtime_identity',
    }]
    profile_config['revision'] = 2
    core.prepare(registered.id, ['aws-us-west-2'], 'research')
    locations = state.list_locations(registered.id, 'managed')
    regional = next(location for location in locations
                    if location.target_id == 'aws-us-west-2')
    assert regional.auto_evict is True
    original_policy_fingerprint = regional.policy_fingerprint

    # Rotating auth configuration is operational and does not change physical
    # materialization identity.
    profile_config['targets'][0]['manager_identity'] = 'rotated-manager'
    profile_config['revision'] = 3
    core.prepare(registered.id, ['aws-us-west-2'], 'research')
    assert len([
        location for location in state.list_locations(registered.id, 'managed')
        if location.target_id == 'aws-us-west-2'
    ]) == 1
    rotated = state.get_location_by_id(regional.id)
    assert rotated is not None
    assert rotated.policy_fingerprint != original_policy_fingerprint

    # Reusing a target ID for a different physical endpoint creates a new
    # versioned materialization; it never reinterprets the old verified row.
    profile_config['targets'][0]['region'] = 'us-west-1'
    profile_config['revision'] = 4
    core.prepare(registered.id, ['aws-us-west-2'], 'research')
    assert len([
        location for location in state.list_locations(registered.id, 'managed')
        if location.target_id == 'aws-us-west-2'
    ]) == 2

    # A realm-only policy revision reuses bytes when the namespace does not
    # render realm into the physical repository.
    profile_config['realm'] = 'replacement'
    profile_config['revision'] = 5
    core.prepare(registered.id, ['canonical'], 'research')
    assert len([
        location for location in state.list_locations(registered.id, 'managed')
        if location.canonical
    ]) == 1

    # Changing the rendered namespace creates a new physical location while
    # retaining one immutable content artifact.
    profile_config['namespace'] = 'replacement/{organization}/{workspace}'
    profile_config['revision'] = 6
    core.prepare(registered.id, ['canonical'], 'research')
    assert len([
        location for location in state.list_locations(registered.id, 'managed')
        if location.canonical
    ]) == 2
    assert state.get_image(registered.id, 'research').source_digest == _DIGEST


def test_workspace_status_fails_bounded_until_pagination_exists():
    with mock.patch.object(state,
                           'list_images',
                           return_value=[mock.sentinel.image] *
                           1001) as list_images:
        with pytest.raises(ValueError, match='more than 1000'):
            core.status(workspace='research')
    list_images.assert_called_once_with('research', limit=1001)


def test_expired_location_lease_is_reclaimable(image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    regional_target = profile.target('aws-us-west-2')
    canonical = _ensure_profile_location(image,
                                         profile,
                                         profile.canonical,
                                         canonical=True)
    regional = _ensure_profile_location(image, profile, regional_target)
    assert state.claim_location(regional.id, 'copy-1', 30) is None
    canonical_claim = state.claim_location(canonical.id, 'import-worker', 30)
    assert canonical_claim is not None
    canonical_token = canonical_claim.lease_owner
    assert canonical_token is not None
    assert _complete_location(canonical.id, canonical_token,
                              f'ecr/repo@{_DIGEST}', _DIGEST)
    first_claim = state.claim_location(regional.id, 'copy-worker', 30)
    assert first_claim is not None
    first_token = first_claim.lease_owner
    assert first_token is not None
    now[0] = 1031
    assert not _complete_location(regional.id, first_token,
                                  f'ecr-west/repo@{_DIGEST}', _DIGEST)
    reclaimed = state.claim_location(regional.id, 'copy-worker', 30)
    assert reclaimed.lease_owner != first_token
    assert reclaimed.lease_owner.startswith('copy-worker:')
    assert reclaimed.attempt_count == 2
    assert not _complete_location(regional.id, first_token,
                                  f'ecr-west/repo@{_DIGEST}', _DIGEST)


def test_expired_canonical_lease_is_reclaimable(image_state_engine,
                                                monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    profile = _profile()
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    canonical = state.ensure_location(
        image.id,
        profile.name,
        profile.canonical.name,
        profile.materialization_fingerprint(profile.canonical),
        _DIGEST,
        policy_fingerprint=profile.policy_fingerprint(profile.canonical, True),
        profile_revision=profile.revision,
        profile_revision_fingerprint=profile.revision_fingerprint,
        canonical=True)
    first_claim = state.claim_location(canonical.id, 'import-worker', 30)
    assert first_claim is not None
    first_token = first_claim.lease_owner
    assert first_token is not None
    now[0] = 1031
    assert not _complete_location(canonical.id, first_token,
                                  f'ecr/repo@{_DIGEST}', _DIGEST)
    reclaimed = state.claim_location(canonical.id, 'import-worker', 30)
    assert reclaimed.lease_owner != first_token
    assert reclaimed.lease_owner.startswith('import-worker:')
    assert reclaimed.attempt_count == 2
    assert not _complete_location(canonical.id, first_token,
                                  f'ecr/repo@{_DIGEST}', _DIGEST)


def test_oci_copy_uses_all_and_verifies_raw_digest():
    raw_manifest = _oci_image_manifest()
    config_payload = _oci_config_payload()
    expected = 'sha256:' + hashlib.sha256(raw_manifest).hexdigest()
    missing = subprocess.CalledProcessError(1, ['skopeo', 'inspect'])
    copied = subprocess.CompletedProcess([], 0, stdout=b'')
    inspected = subprocess.CompletedProcess([], 0, stdout=raw_manifest)
    client = oci.OciClient()
    configured = subprocess.CompletedProcess([], 0, stdout=config_payload)
    with mock.patch('subprocess.run',
                    side_effect=[missing, copied, inspected,
                                 configured]) as run:
        result = client.copy_and_verify('source/repo@' + expected,
                                        'dest/repo@' + expected,
                                        expected,
                                        source_authfile='/src/auth.json',
                                        destination_authfile='/dst/auth.json')
        assert result.digest == expected
        assert result.platforms == ('linux/amd64',)
    assert run.call_args_list[0].args[0][-1] == f'docker://dest/repo@{expected}'
    copy_command = run.call_args_list[1].args[0]
    assert copy_command[:4] == ['skopeo', 'copy', '--all', '--preserve-digests']
    assert '--src-authfile' in copy_command
    assert '--dest-authfile' in copy_command
    assert copy_command[-2:] == [
        f'docker://source/repo@{expected}',
        f'docker://dest/repo:{expected.replace(":", "-", 1)}',
    ]
    assert run.call_args_list[2].args[0][-1] == f'docker://dest/repo@{expected}'
    assert run.call_args_list[3].args[0][-1] == f'docker://dest/repo@{expected}'
    assert run.call_args_list[0].kwargs['timeout'] == 300
    assert run.call_args_list[1].kwargs['timeout'] == 3600
    assert run.call_args_list[2].kwargs['timeout'] == 300
    assert run.call_args_list[3].kwargs['timeout'] == 300


def test_oci_copy_retry_recovers_committed_immutable_tag():
    raw_manifest = _oci_image_manifest()
    config_payload = _oci_config_payload()
    expected = 'sha256:' + hashlib.sha256(raw_manifest).hexdigest()
    missing = subprocess.CalledProcessError(1, ['skopeo', 'inspect'])
    copied = subprocess.CompletedProcess([], 0, stdout=b'')
    verification_timeout = subprocess.TimeoutExpired(['skopeo', 'inspect'], 300)
    inspected = subprocess.CompletedProcess([], 0, stdout=raw_manifest)
    configured = subprocess.CompletedProcess([], 0, stdout=config_payload)
    client = oci.OciClient()
    with mock.patch('subprocess.run',
                    side_effect=[
                        missing, copied, verification_timeout, inspected,
                        configured
                    ]) as run:
        with pytest.raises(subprocess.TimeoutExpired):
            client.copy_and_verify('source/repo@' + expected,
                                   'dest/repo@' + expected, expected)
        assert client.copy_and_verify('source/repo@' + expected,
                                      'dest/repo@' + expected,
                                      expected).digest == expected

    copy_commands = [
        call.args[0] for call in run.call_args_list if call.args[0][1] == 'copy'
    ]
    assert len(copy_commands) == 1


def test_oci_copy_accepts_ambiguous_failure_only_after_exact_digest():
    raw_manifest = _oci_image_manifest()
    config_payload = _oci_config_payload()
    expected = 'sha256:' + hashlib.sha256(raw_manifest).hexdigest()
    missing = subprocess.CalledProcessError(1, ['skopeo', 'inspect'])
    ambiguous_copy = subprocess.TimeoutExpired(['skopeo', 'copy'], 3600)
    inspected = subprocess.CompletedProcess([], 0, stdout=raw_manifest)
    configured = subprocess.CompletedProcess([], 0, stdout=config_payload)
    client = oci.OciClient()
    with mock.patch(
            'subprocess.run',
            side_effect=[missing, ambiguous_copy, inspected,
                         configured]) as run:
        assert client.copy_and_verify('source/repo@' + expected,
                                      'dest/repo@' + expected,
                                      expected).digest == expected
    assert [call.args[0][1] for call in run.call_args_list
           ] == ['inspect', 'copy', 'inspect', 'inspect']

    wrong_manifest = (b'{"schemaVersion":2,"manifests":['
                      b'{"platform":{"os":"linux","architecture":"arm64"}}]}')
    wrong = subprocess.CompletedProcess([], 0, stdout=wrong_manifest)
    with mock.patch('subprocess.run',
                    side_effect=[missing, ambiguous_copy, wrong]) as run:
        with pytest.raises(subprocess.TimeoutExpired):
            client.copy_and_verify('source/repo@' + expected,
                                   'dest/repo@' + expected, expected)
    assert [call.args[0][1] for call in run.call_args_list
           ] == ['inspect', 'copy', 'inspect']


def test_oci_single_manifest_reads_platform_from_image_config():
    raw_manifest = _oci_image_manifest()
    config_payload = _oci_config_payload('arm64', 'v8')
    inspected = subprocess.CompletedProcess([], 0, stdout=raw_manifest)
    configured = subprocess.CompletedProcess([], 0, stdout=config_payload)
    client = oci.OciClient()
    with mock.patch('subprocess.run', side_effect=[inspected,
                                                   configured]) as run:
        result = client.inspect_metadata('registry.example/repo@' + _DIGEST)
    assert result.platforms == ('linux/arm64/v8',)
    assert [call.args[0][2] for call in run.call_args_list
           ] == ['--raw', '--config']


def test_oci_index_proves_runnable_child_manifest_and_config_platform():
    child_manifest = _oci_image_manifest()
    child_digest = 'sha256:' + hashlib.sha256(child_manifest).hexdigest()
    root_index = json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'manifests': [{
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': child_digest,
                'size': len(child_manifest),
                'platform': {
                    'os': 'linux',
                    'architecture': 'arm64',
                    'variant': 'v8',
                },
            }],
        },
        separators=(',', ':')).encode()
    outputs = [
        subprocess.CompletedProcess([], 0, stdout=root_index),
        subprocess.CompletedProcess([], 0, stdout=child_manifest),
        subprocess.CompletedProcess([],
                                    0,
                                    stdout=_oci_config_payload('arm64', 'v8')),
    ]
    with mock.patch('subprocess.run', side_effect=outputs) as run:
        result = oci.OciClient().inspect_metadata(
            f'registry.example/repo@{_DIGEST}')

    assert result.digest == 'sha256:' + hashlib.sha256(root_index).hexdigest()
    assert result.platforms == ('linux/arm64/v8',)
    assert [call.args[0][2] for call in run.call_args_list
           ] == ['--raw', '--raw', '--config']


def test_oci_index_rejects_artifact_manifest_as_platform_evidence():
    artifact_manifest = json.loads(_oci_image_manifest())
    artifact_manifest['artifactType'] = 'application/vnd.example.sbom'
    artifact_bytes = json.dumps(artifact_manifest,
                                separators=(',', ':')).encode()
    artifact_digest = 'sha256:' + hashlib.sha256(artifact_bytes).hexdigest()
    root_index = json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'manifests': [{
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': artifact_digest,
                'size': len(artifact_bytes),
                'platform': {
                    'os': 'linux',
                    'architecture': 'amd64',
                },
            }],
        },
        separators=(',', ':')).encode()
    outputs = [
        subprocess.CompletedProcess([], 0, stdout=root_index),
        subprocess.CompletedProcess([], 0, stdout=artifact_bytes),
    ]
    with mock.patch('subprocess.run', side_effect=outputs) as run, \
         pytest.raises(ValueError, match='at least one OCI platform'):
        oci.OciClient().inspect_metadata(f'registry.example/repo@{_DIGEST}')
    assert [call.args[0][2] for call in run.call_args_list
           ] == ['--raw', '--raw']


def test_oci_rejects_root_artifact_index_without_fetching_children():
    root_index = json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'artifactType': 'application/vnd.example.collection',
            'manifests': [],
        },
        separators=(',', ':')).encode()
    with mock.patch(
            'subprocess.run',
            return_value=subprocess.CompletedProcess([], 0,
                                                     stdout=root_index)) as run, \
         pytest.raises(ValueError, match='index metadata is invalid'):
        oci.OciClient().inspect_metadata(f'registry.example/repo@{_DIGEST}')
    assert run.call_count == 1


def test_oci_malformed_image_child_cannot_hide_beside_valid_child():
    malformed = json.loads(_oci_image_manifest())
    malformed['schemaVersion'] = 1
    malformed_bytes = json.dumps(malformed, separators=(',', ':')).encode()
    malformed_digest = 'sha256:' + hashlib.sha256(malformed_bytes).hexdigest()
    valid_bytes = _oci_image_manifest()
    valid_digest = 'sha256:' + hashlib.sha256(valid_bytes).hexdigest()
    root_index = json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'manifests': [{
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': malformed_digest,
                'size': len(malformed_bytes),
                'platform': {
                    'os': 'linux',
                    'architecture': 'amd64',
                },
            }, {
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': valid_digest,
                'size': len(valid_bytes),
                'platform': {
                    'os': 'linux',
                    'architecture': 'arm64',
                },
            }],
        },
        separators=(',', ':')).encode()
    outputs = [
        subprocess.CompletedProcess([], 0, stdout=root_index),
        subprocess.CompletedProcess([], 0, stdout=malformed_bytes),
    ]
    with mock.patch('subprocess.run', side_effect=outputs) as run, \
         pytest.raises(ValueError, match='schema version 2'):
        oci.OciClient().inspect_metadata(f'registry.example/repo@{_DIGEST}')
    assert run.call_count == 2


@pytest.mark.parametrize(
    'malformed_descriptor',
    [
        None,
        [],
        {},
        {
            'mediaType': [],
            'digest': _DIGEST,
            'size': 1,
        },
        {
            'mediaType': 'https://user:descriptor-secret@example.com',
            'digest': _DIGEST,
            'size': 1,
        },
        {
            'mediaType': 'application/vnd.example.attachment',
            'digest': 'not-a-digest',
            'size': 1,
        },
        {
            'mediaType': 'application/vnd.example.attachment',
            'digest': _DIGEST,
            'size': True,
        },
        {
            'mediaType': 'application/vnd.example.attachment',
            'digest': _DIGEST,
            'size': -1,
        },
        {
            'mediaType': 'application/vnd.example.attachment',
            'digest': _DIGEST,
            'size': 1,
            'artifactType': 'not-a-media-type',
        },
        {
            'mediaType': 'application/vnd.example.attachment',
            'digest': _DIGEST,
            'size': 1,
            'annotations': {
                'kind': []
            },
        },
        {
            'mediaType': 'application/vnd.example.attachment',
            'digest': _DIGEST,
            'size': 1,
            'urls': 'https://example.com/blob',
        },
        {
            'mediaType': 'application/vnd.example.attachment',
            'digest': _DIGEST,
            'size': 1,
            'data': [],
        },
    ],
)
def test_oci_index_rejects_malformed_descriptor_structure_and_types(
        malformed_descriptor):
    valid_child = _oci_image_manifest()
    valid_digest = 'sha256:' + hashlib.sha256(valid_child).hexdigest()
    root_index = json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'manifests': [
                malformed_descriptor, {
                    'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                    'digest': valid_digest,
                    'size': len(valid_child),
                    'platform': {
                        'os': 'linux',
                        'architecture': 'amd64',
                    },
                }
            ],
        },
        separators=(',', ':')).encode()
    with mock.patch(
            'subprocess.run',
            return_value=subprocess.CompletedProcess([], 0,
                                                     stdout=root_index)) as run, \
         pytest.raises(ValueError, match='index descriptor'):
        oci.OciClient().inspect_metadata(f'registry.example/repo@{_DIGEST}')
    assert run.call_count == 1


@pytest.mark.parametrize(
    ('descriptor_field', 'descriptor_value', 'error_match'),
    [
        ('config', {
            'mediaType': 'application/vnd.oci.image.config.v1+json',
            'digest': _OTHER_DIGEST,
            'size': 1 << 63,
        }, 'image config descriptor'),
        ('config', {
            'mediaType': 'application/vnd.oci.image.config.v1+json',
            'digest': _OTHER_DIGEST,
            'size': 2,
            'annotations': {
                'kind': []
            },
        }, 'image config descriptor'),
        ('layers', [None], 'layer descriptor'),
        ('layers', [{
            'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
            'digest': _OTHER_DIGEST,
            'size': 1,
            'urls': 'https://example.com/layer',
        }], 'layer descriptor'),
        ('layers', [{
            'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
            'digest': _OTHER_DIGEST,
            'size': 1,
            'artifactType': [],
        }], 'layer descriptor'),
    ],
)
def test_oci_manifest_uses_the_same_descriptor_structure_boundary(
        descriptor_field, descriptor_value, error_match):
    manifest = json.loads(_oci_image_manifest())
    manifest[descriptor_field] = descriptor_value
    raw_manifest = json.dumps(manifest, separators=(',', ':')).encode()
    with mock.patch(
            'subprocess.run',
            return_value=subprocess.CompletedProcess([], 0,
                                                     stdout=raw_manifest)) as run, \
         pytest.raises(ValueError, match=error_match):
        oci.OciClient().inspect_metadata(f'registry.example/repo@{_DIGEST}')
    assert run.call_count == 1


def test_oci_index_ignores_structurally_valid_non_image_referrers():
    valid_child = _oci_image_manifest()
    valid_digest = 'sha256:' + hashlib.sha256(valid_child).hexdigest()
    root_index = json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'manifests': [{
                'mediaType': 'application/vnd.example.signature',
                'digest': 'sha512:' + 'a' * 128,
                'size': 10,
                'artifactType': 'application/vnd.example.signature',
                'annotations': {
                    'org.opencontainers.image.title': 'signature'
                },
                'urls': ['https://example.com/signature'],
                'data': 'c2lnbmF0dXJl',
            }, {
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': valid_digest,
                'size': len(valid_child),
                'platform': {
                    'os': 'linux',
                    'architecture': 'amd64',
                },
            }],
        },
        separators=(',', ':')).encode()
    outputs = [
        subprocess.CompletedProcess([], 0, stdout=root_index),
        subprocess.CompletedProcess([], 0, stdout=valid_child),
        subprocess.CompletedProcess([], 0, stdout=_oci_config_payload()),
    ]
    with mock.patch('subprocess.run', side_effect=outputs) as run:
        result = oci.OciClient().inspect_metadata(
            f'registry.example/repo@{_DIGEST}')
    assert result.platforms == ('linux/amd64',)
    assert [call.args[0][2] for call in run.call_args_list
           ] == ['--raw', '--raw', '--config']


def test_oci_image_descriptor_size_must_match_fetched_bytes():
    child = _oci_image_manifest()
    child_digest = 'sha256:' + hashlib.sha256(child).hexdigest()
    root_index = json.dumps(
        {
            'schemaVersion': 2,
            'mediaType': 'application/vnd.oci.image.index.v1+json',
            'manifests': [{
                'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'digest': child_digest,
                'size': len(child) + 1,
                'platform': {
                    'os': 'linux',
                    'architecture': 'amd64',
                },
            }],
        },
        separators=(',', ':')).encode()
    outputs = [
        subprocess.CompletedProcess([], 0, stdout=root_index),
        subprocess.CompletedProcess([], 0, stdout=child),
    ]
    with mock.patch('subprocess.run', side_effect=outputs), \
         pytest.raises(ValueError, match='descriptor size'):
        oci.OciClient().inspect_metadata(f'registry.example/repo@{_DIGEST}')


def test_oci_subprocess_honors_lease_cancellation():
    cancelled = threading.Event()
    cancelled.set()
    with pytest.raises(RuntimeError, match='cancelled after lease loss'):
        oci.OciClient._run(['/bin/sh', '-c', 'sleep 10'], 30, cancelled)


def test_oci_delete_is_digest_scoped_and_bounded():
    client = oci.OciClient()
    with mock.patch('subprocess.run') as run:
        client.delete(f'ecr-west.example/repo@{_DIGEST}',
                      authfile='/dst/auth.json')
    assert run.call_args.args[0] == [
        'skopeo', 'delete', '--authfile', '/dst/auth.json',
        f'docker://ecr-west.example/repo@{_DIGEST}'
    ]
    assert run.call_args.kwargs['timeout'] == 300
    with mock.patch('subprocess.run') as run, \
         pytest.raises(ValueError, match='digest-pinned'):
        client.delete('ecr-west.example/repo:mutable')
    run.assert_not_called()


def test_image_models_revalidate_frozen_instances_at_every_boundary(
        image_state_engine):
    secret = 'inline-model-secret'
    hostile_reference = (f'user:{secret}@registry.example.com/repo@{_DIGEST}')
    image = models.ContainerImage(ref=_SOURCE)
    object.__setattr__(image, 'ref', hostile_reference)

    for operation in (
            lambda: models.ContainerImage.from_config(image),
            image.to_yaml_config,
            lambda: resources_lib.Resources(container_image=image),
    ):
        with pytest.raises(ValueError) as error:
            operation()
        assert secret not in str(error.value)

    resolved = models.ResolvedContainerImage(
        image_id=_ARTIFACT_ID,
        location_id=_LOCATION_ID,
        reference=f'registry.example.com/repo@{_DIGEST}',
        target_id='canonical',
        distribution='managed',
        profile_revision=1,
        policy_fingerprint=_POLICY_FINGERPRINT,
        digest=_DIGEST,
        auth_strategy='anonymous',
    )
    object.__setattr__(resolved, 'reference', hostile_reference)
    for operation in (
            lambda: models.ResolvedContainerImage.from_dict(resolved),
            resolved.to_dict,
            lambda: resources_lib.Resources(
                container_image=_SOURCE,
                _resolved_container_image=resolved,
            ),
    ):
        with pytest.raises(ValueError) as error:
            operation()
        assert secret not in str(error.value)

    resources = resources_lib.Resources(container_image=_SOURCE)
    assert resources.container_image is not None
    object.__setattr__(resources.container_image, 'ref', hostile_reference)
    for operation in (resources.to_yaml_config,
                      lambda: pickle.dumps(resources)):
        with pytest.raises(ValueError) as error:
            operation()
        assert secret not in str(error.value)

    restored = resources_lib.Resources.__new__(resources_lib.Resources)
    restored_state = resources.__dict__.copy()
    restored_state['_version'] = resources._VERSION
    with pytest.raises(ValueError) as error:
        restored.__setstate__(restored_state)
    assert secret not in str(error.value)

    handle = types.SimpleNamespace(launched_resources=resources,
                                   launched_nodes=1)
    with pytest.raises(ValueError) as error:
        global_user_state.add_or_update_cluster(
            'tampered-container-image',
            handle,
            requested_resources=None,
            ready=False,
        )
    assert secret not in str(error.value)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'tampered-container-image')).first() is None


def test_resolved_image_field_set_errors_are_value_free():
    secret = 'hostile-field-secret'
    payload = {
        'image_id': _ARTIFACT_ID,
        'location_id': _LOCATION_ID,
        'reference': f'registry.example.com/repo@{_DIGEST}',
        'target_id': 'canonical',
        'distribution': 'managed',
        'profile_revision': 1,
        'policy_fingerprint': _POLICY_FINGERPRINT,
        'digest': _DIGEST,
        'auth_strategy': 'anonymous',
        secret: 'ignored',
    }
    with pytest.raises(ValueError) as error:
        models.ResolvedContainerImage.from_dict(payload)
    assert secret not in str(error.value)


def test_resource_runtime_image_must_match_revalidated_models(
        image_state_engine):
    secret = 'runtime-state-secret'
    resources = resources_lib.Resources(container_image=_SOURCE)
    resources._docker_image = f'registry.example.com/{secret}:latest'
    for operation in (resources.to_yaml_config,
                      lambda: pickle.dumps(resources)):
        with pytest.raises(ValueError) as error:
            operation()
        assert secret not in str(error.value)
    handle = types.SimpleNamespace(launched_resources=resources,
                                   launched_nodes=1)
    with pytest.raises(ValueError) as error:
        global_user_state.add_or_update_cluster('inconsistent-runtime-image',
                                                handle,
                                                requested_resources=None,
                                                ready=False)
    assert secret not in str(error.value)
    with sqlalchemy.orm.Session(image_state_engine) as session:
        assert session.execute(global_user_state.cluster_table.select().where(
            global_user_state.cluster_table.c.name ==
            'inconsistent-runtime-image')).first() is None


def test_variant_platforms_fail_closed_without_exact_cpu_feature_proof():
    assert not models.platforms_support_runtime(
        ('linux/amd64/v4',), 'linux/amd64')
    assert not models.platforms_support_runtime(
        ('linux/arm64/v9',), 'linux/arm64')
    assert not models.platforms_support_runtime(
        ('linux/amd64/v4', 'linux/arm64/v9'), None)
    assert models.platforms_support_runtime(('linux/amd64/v1',), 'linux/amd64')
    assert models.platforms_support_runtime(('linux/arm64/v8',), 'linux/arm64')
    assert models.platforms_support_runtime(('linux/amd64',), None)
    assert models.platforms_support_runtime(('linux/arm64/v8',), None)
    assert not models.platforms_support_runtime(('linux/ppc64le',), None)
    assert not models.platforms_support_runtime(('windows/amd64',), None)
    assert models.platforms_support_runtime(
        ('linux/amd64/v1', 'linux/arm64/v8'), None)
    assert models.platforms_support_runtime(('linux/amd64/v4',),
                                            'linux/amd64/v4')
    assert not models.platforms_support_runtime(
        ('linux/amd64/v4',), 'linux/amd64/v3')

    launched = resources_lib.Resources(cloud=clouds.AWS(),
                                       instance_type='m6i.large')
    with mock.patch.object(clouds.AWS,
                           'get_arch_from_instance_type',
                           return_value='x86_64'), \
         pytest.raises(ValueError, match='runtime architecture'):
        global_user_state._validate_container_image_runtime_platform(
            json.dumps(['linux/amd64/v4']), launched)


def test_ecr_authorities_cover_aws_china_partition():
    authority = ('123456789012.dkr.ecr.cn-north-1.'
                 'amazonaws.com.cn')
    derived = models.RegistryTarget(name='china',
                                    provider='aws',
                                    region='cn-north-1',
                                    account='123456789012',
                                    pull_auth='ecr_runtime_identity')
    assert derived.registry_prefix == authority
    providers.get_adapter('aws').validate_target(derived)
    explicit = models.RegistryTarget(name='china-explicit',
                                     provider='aws',
                                     region='cn-north-1',
                                     account='123456789012',
                                     registry=f'{authority}/team',
                                     pull_auth='ecr_runtime_identity')
    providers.get_adapter('aws').validate_target(explicit)
    with pytest.raises(ValueError, match='exact ECR authority'):
        providers.get_adapter('aws').validate_target(
            models.RegistryTarget(name='china-wrong-suffix',
                                  provider='aws',
                                  region='cn-north-1',
                                  account='123456789012',
                                  registry=('123456789012.dkr.ecr.cn-north-1.'
                                            'amazonaws.com/team'),
                                  pull_auth='ecr_runtime_identity'))

    reference = f'{authority}/team/model@{_DIGEST}'
    strategy, login = providers.resolve_source_runtime_pull_auth(
        reference,
        models.Placement(provider='aws', region='cn-north-1', backend='vm'),
        None)
    assert strategy == 'ecr_runtime_identity'
    assert login is not None
    assert login.server == authority
    assert docker_utils._extract_region_from_ecr_server(
        authority) == 'cn-north-1'
    assert docker_utils._ECR_SERVER_PATTERN.fullmatch(authority)


def test_oci_primitives_reject_credential_references_before_subprocess():
    secret = 'oci-inline-secret'
    hostile = f'user:{secret}@registry.example.com/repo@{_DIGEST}'
    valid = f'registry.example.com/repo@{_DIGEST}'
    client = oci.OciClient()
    client._run = mock.Mock()
    operations = (
        lambda: client.copy_all(hostile, valid),
        lambda: client.copy_all(valid, hostile),
        lambda: client.inspect_digest(hostile),
        lambda: client.inspect_metadata(hostile),
        lambda: client._inspect_config_platform(hostile, None, None),
        lambda: client.copy_and_verify(hostile, valid, _DIGEST),
        lambda: client.copy_and_verify(valid, hostile, _DIGEST),
        lambda: client.delete(hostile),
    )
    for operation in operations:
        client._run.reset_mock()
        with pytest.raises(ValueError) as error:
            operation()
        assert secret not in str(error.value)
        client._run.assert_not_called()


def test_missing_transition_leases_are_reclaimed_by_both_indexed_queues(
        image_state_engine, monkeypatch):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image = _publish_state_image(_SOURCE, _DIGEST, release='missing-copy-lease')
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    table = global_user_state.container_image_location_table
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(table.c.id == canonical.id).values(
            state=models.ImageLocationState.COPYING.value,
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None,
            updated_at=now[0]))
        session.commit()
    copy_claim = state.claim_next_reconciliation_candidate(
        'research',
        'repair-copy',
        materialization_lease_seconds=30,
        verification_lease_seconds=30,
        now=now[0])
    assert copy_claim is not None
    assert copy_claim.id == canonical.id
    assert copy_claim.lease_owner is not None
    assert copy_claim.attempt_count == 1

    regional_image, _ = _ready_regional_location(_OTHER_SOURCE, _OTHER_DIGEST)
    regional = next(
        location for location in state.list_locations(regional_image.id)
        if not location.canonical)
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(table.c.id == regional.id).values(
            state=models.ImageLocationState.EVICTING.value,
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None,
            updated_at=now[0]))
        session.commit()
    eviction_claim = state.claim_next_eviction_candidate('research',
                                                         'repair-eviction',
                                                         lease_seconds=30,
                                                         unused_before=cutoff,
                                                         now=now[0])
    assert eviction_claim is not None
    assert eviction_claim.id == regional.id
    assert eviction_claim.lease_owner is not None
    assert eviction_claim.attempt_count == 1


def test_direct_claims_recover_missing_transition_leases(
        image_state_engine, monkeypatch):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image = _publish_state_image(_SOURCE,
                                 _DIGEST,
                                 release='direct-missing-copy-lease')
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    table = global_user_state.container_image_location_table
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(table.c.id == canonical.id).values(
            state=models.ImageLocationState.COPYING.value,
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None))
        session.commit()
    claimed = state.claim_location(canonical.id, 'direct-copy', 30)
    assert claimed is not None
    assert claimed.attempt_count == 1

    regional_image, _ = _ready_regional_location(_OTHER_SOURCE, _OTHER_DIGEST)
    regional = next(
        location for location in state.list_locations(regional_image.id)
        if not location.canonical)
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    with sqlalchemy.orm.Session(image_state_engine) as session:
        session.execute(table.update().where(table.c.id == regional.id).values(
            state=models.ImageLocationState.EVICTING.value,
            lease_owner=None,
            lease_expires_at=None,
            heartbeat_at=None))
        session.commit()
    eviction = state.claim_location_eviction(regional.id, 'direct-eviction', 30,
                                             cutoff)
    assert eviction is not None
    assert eviction.attempt_count == 1


@pytest.mark.parametrize('partial_lease', [
    {
        'lease_owner': 'historical-owner',
        'lease_expires_at': None,
        'heartbeat_at': 900,
    },
    {
        'lease_owner': None,
        'lease_expires_at': 900,
        'heartbeat_at': 900,
    },
    {
        'lease_owner': '',
        'lease_expires_at': 900,
        'heartbeat_at': 900,
    },
])
def test_indexed_reconciliation_repairs_partial_historical_lease(
        image_state_engine, monkeypatch, partial_lease):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image = _publish_state_image(_SOURCE, _DIGEST, release='partial-copy-lease')
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    table = global_user_state.container_image_location_table
    with image_state_engine.connect() as connection:
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        connection.execute(
            table.update().where(table.c.id == canonical.id).values(
                state=models.ImageLocationState.COPYING.value, **partial_lease))
        connection.commit()
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')

    assert [
        candidate.id for candidate in state.list_reconciliation_candidates(
            'research', now=now[0], limit=10)
    ] == [canonical.id]
    claimed = state.claim_next_reconciliation_candidate(
        'research',
        'repair-partial-copy',
        materialization_lease_seconds=30,
        verification_lease_seconds=30,
        now=now[0])
    assert claimed is not None
    assert claimed.id == canonical.id
    assert claimed.lease_owner is not None
    assert claimed.lease_expires_at == 1030


@pytest.mark.parametrize('partial_lease', [
    {
        'lease_owner': 'historical-owner',
        'lease_expires_at': None,
        'heartbeat_at': 900,
    },
    {
        'lease_owner': None,
        'lease_expires_at': 900,
        'heartbeat_at': 900,
    },
])
def test_indexed_eviction_repairs_partial_historical_lease(
        image_state_engine, monkeypatch, partial_lease):
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image, _ = _ready_regional_location()
    regional = next(location for location in state.list_locations(image.id)
                    if not location.canonical)
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    table = global_user_state.container_image_location_table
    with image_state_engine.connect() as connection:
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = ON')
        connection.execute(
            table.update().where(table.c.id == regional.id).values(
                state=models.ImageLocationState.EVICTING.value,
                **partial_lease))
        connection.commit()
        connection.exec_driver_sql('PRAGMA ignore_check_constraints = OFF')

    assert [
        candidate.id
        for candidate in state.list_eviction_candidates('research', cutoff, 10)
    ] == [regional.id]
    claimed = state.claim_next_eviction_candidate('research',
                                                  'repair-partial-eviction',
                                                  lease_seconds=30,
                                                  unused_before=cutoff,
                                                  now=now[0])
    assert claimed is not None
    assert claimed.id == regional.id
    assert claimed.lease_owner is not None


def test_eviction_retries_stop_at_automatic_attempt_limit(
        image_state_engine, monkeypatch):
    del image_state_engine
    now = [1000]
    monkeypatch.setattr(state.time, 'time', lambda: now[0])
    image, _ = _ready_regional_location()
    regional = next(location for location in state.list_locations(image.id)
                    if not location.canonical)
    now[0] += 8 * _WEEK_SECONDS + 1
    cutoff = now[0] - 8 * _WEEK_SECONDS
    for _ in range(state._MAX_AUTOMATIC_LOCATION_ATTEMPTS):
        claimed = state.claim_location_eviction(regional.id, 'evictor', 30,
                                                cutoff)
        assert claimed is not None
        assert claimed.lease_owner is not None
        assert state.fail_location_eviction(
            regional.id,
            claimed.lease_owner,
            models.ImageLocationErrorCode.EVICTION_FAILED,
            retry_at=now[0])
    exhausted = state.get_location_by_id(regional.id)
    assert exhausted is not None
    assert exhausted.attempt_count == state._MAX_AUTOMATIC_LOCATION_ATTEMPTS
    assert exhausted.next_retry_at is None
    assert state.claim_location_eviction(regional.id, 'evictor', 30,
                                         cutoff) is None
    assert state.claim_next_eviction_candidate('research',
                                               'evictor',
                                               lease_seconds=30,
                                               unused_before=cutoff,
                                               now=now[0]) is None
    assert state.list_eviction_candidates('research', cutoff, 10) == []


def test_future_retry_queues_are_due_indexed_at_large_cardinality(
        image_state_engine):
    image, _ = _ready_regional_location()
    canonical = next(location for location in state.list_locations(image.id)
                     if location.canonical)
    seed_verification = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(1)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < :row_count
        )
        INSERT INTO container_image_locations (
          id, workspace, image_id, profile, target_id, target_fingerprint,
          policy_fingerprint, profile_revision, canonical, canonical_ready,
          target_ref, expected_digest, state, attempt_count, next_retry_at,
          verification_requested_at, auto_evict, updated_at
        )
        SELECT 'verify-future-' || n, 'research', :image_id, 'managed',
               'verify-target-' || n, 'verify-fingerprint-' || n,
               :policy_fingerprint, 1, 1, 0,
               'verify.example/repo-' || n || '@' || :digest, :digest,
               'READY', 1, 4000000000, 1, 0, 1
        FROM synthetic
    """)
    seed_eviction = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(1)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < :row_count
        )
        INSERT INTO container_image_locations (
          id, workspace, image_id, profile, target_id, target_fingerprint,
          policy_fingerprint, profile_revision, canonical,
          canonical_location_id, canonical_ready, target_ref, expected_digest,
          state, attempt_count, next_retry_at, last_used_at, auto_evict,
          updated_at
        )
        SELECT 'evict-future-' || n, 'research', :image_id, 'managed',
               'evict-target-' || n, 'evict-fingerprint-' || n,
               :policy_fingerprint, 1, 0, :canonical_id, 1,
               'evict.example/repo-' || n || '@' || :digest, :digest,
               'READY', 1, 4000000000, 1, 1, 1
        FROM synthetic
    """)
    parameters = {
        'row_count': 100_000,
        'image_id': image.id,
        'policy_fingerprint': _POLICY_FINGERPRINT,
        'canonical_id': canonical.id,
        'digest': _DIGEST,
    }
    with image_state_engine.begin() as connection:
        connection.execute(seed_verification, parameters)
        connection.execute(seed_eviction, parameters)

    operations = (
        ('list reconciliation', lambda: state.list_reconciliation_candidates(
            'research', now=1000, limit=10)),
        ('claim reconciliation', lambda: state.
         claim_next_reconciliation_candidate('research',
                                             'scale-reconciler',
                                             materialization_lease_seconds=30,
                                             verification_lease_seconds=30,
                                             now=1000)),
        ('list eviction',
         lambda: state.list_eviction_candidates('research', 100, 10)),
        ('claim eviction',
         lambda: state.claim_next_eviction_candidate('research',
                                                     'scale-evictor',
                                                     lease_seconds=30,
                                                     unused_before=100,
                                                     now=1000)),
    )
    for label, operation in operations:
        result, steps = _sqlite_result_and_vm_steps(image_state_engine,
                                                    operation)
        assert not result
        assert steps < 100_000, f'{label} used {steps} SQLite VM steps'


def test_referenced_eviction_backlog_has_constant_bounded_probe_cost(
        image_state_engine, monkeypatch):
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    profile = _profile()
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    claim = state.claim_location(canonical.id, 'canonical-worker', 30)
    assert claim is not None and claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, claim.lease_owner, canonical_ref,
                              _DIGEST)
    _seed_referenced_eviction_queue(image_state_engine, image, canonical,
                                    100_000)
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()
        state._EVICTION_CANDIDATE_CURSORS.clear()

    listed, list_steps = _sqlite_result_and_vm_steps(
        image_state_engine,
        lambda: state.list_eviction_candidates('research', 100, 1))
    claimed, claim_steps = _sqlite_result_and_vm_steps(
        image_state_engine,
        lambda: state.claim_next_eviction_candidate('research',
                                                    'bounded-evictor',
                                                    lease_seconds=30,
                                                    unused_before=100,
                                                    now=1000))

    assert listed == []
    assert claimed is None
    assert list_steps < 200_000
    assert claim_steps < 1_000_000


def test_eviction_reference_pages_eventually_reach_later_unreferenced_work(
        image_state_engine, monkeypatch):
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    profile = _profile()
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    claim = state.claim_location(canonical.id, 'canonical-worker', 30)
    assert claim is not None and claim.lease_owner is not None
    canonical_ref = references.managed_reference(profile, profile.canonical,
                                                 'research', _SOURCE, _DIGEST)
    assert _complete_location(canonical.id, claim.lease_owner, canonical_ref,
                              _DIGEST)
    eventual_id = _seed_referenced_eviction_queue(image_state_engine, image,
                                                  canonical, 65)
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()
        state._EVICTION_CANDIDATE_CURSORS.clear()

    assert state.list_eviction_candidates('research', 100, 1) == []
    listed = state.list_eviction_candidates('research', 100, 1)
    assert [candidate.id for candidate in listed] == [eventual_id]

    assert state.claim_next_eviction_candidate('research',
                                               'paged-evictor',
                                               lease_seconds=30,
                                               unused_before=100,
                                               now=1000) is None
    claimed = state.claim_next_eviction_candidate('research',
                                                  'paged-evictor',
                                                  lease_seconds=30,
                                                  unused_before=100,
                                                  now=1000)
    assert claimed is not None
    assert claimed.id == eventual_id


def test_failed_retry_probe_uses_sqlite_partial_queue_index(
        image_state_engine, monkeypatch):
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    profile = _profile()
    image = _publish_state_image(_SOURCE, _DIGEST, profile=profile)
    canonical = state.list_locations(image.id, profile.name)[0]
    claim = state.claim_location(canonical.id, 'failed-retry-fixture', 30)
    assert claim is not None and claim.lease_owner is not None
    assert state.fail_location(
        canonical.id,
        claim.lease_owner,
        models.ImageLocationErrorCode.MATERIALIZATION_FAILED,
        retry_at=1000)
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()

    executed = []

    def _capture(_connection, _cursor, statement, parameters, _context,
                 _executemany):
        if "IN ('FAILED', 'MISSING')" in statement:
            executed.append((statement, parameters))

    sqlalchemy.event.listen(image_state_engine, 'before_cursor_execute',
                            _capture)
    try:
        candidates = state.list_reconciliation_candidates('research',
                                                          now=1000,
                                                          limit=1)
    finally:
        sqlalchemy.event.remove(image_state_engine, 'before_cursor_execute',
                                _capture)
    assert [candidate.id for candidate in candidates] == [canonical.id]
    assert len(executed) == 1
    statement, parameters = executed[0]
    with image_state_engine.connect() as connection:
        plan = connection.connection.driver_connection.execute(
            f'EXPLAIN QUERY PLAN {statement}', parameters).fetchall()
    plan_text = ' '.join(str(row) for row in plan)
    assert 'ix_container_image_locations_profile_retry_queue' in plan_text


def test_queue_profile_discovery_is_bounded_at_large_cardinality(
        image_state_engine):
    seed_profiles = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(1)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < :row_count
        )
        INSERT INTO container_image_profile_revisions (
          workspace, profile, revision, revision_fingerprint, created_at,
          updated_at
        )
        SELECT 'research', 'empty-' || printf('%06d', n), 1,
               :revision_fingerprint, 1, 1
        FROM synthetic
    """)
    with image_state_engine.begin() as connection:
        connection.execute(seed_profiles, {
            'row_count': 200_000,
            'revision_fingerprint': _POLICY_FINGERPRINT,
        })
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()

    operations = (
        ('list reconciliation', lambda: state.list_reconciliation_candidates(
            'research', now=1000, limit=1)),
        ('claim reconciliation', lambda: state.
         claim_next_reconciliation_candidate('research',
                                             'profile-page-reconciler',
                                             materialization_lease_seconds=30,
                                             verification_lease_seconds=30,
                                             now=1000)),
        ('list eviction',
         lambda: state.list_eviction_candidates('research', 100, 1)),
        ('claim eviction',
         lambda: state.claim_next_eviction_candidate('research',
                                                     'profile-page-evictor',
                                                     lease_seconds=30,
                                                     unused_before=100,
                                                     now=1000)),
    )
    for label, operation in operations:
        result, steps = _sqlite_result_and_vm_steps(image_state_engine,
                                                    operation)
        assert not result
        assert steps < 100_000, f'{label} used {steps} SQLite VM steps'


def test_bounded_profile_pages_eventually_revisit_due_work(image_state_engine):
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    seed_profiles = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(0)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < 129
        )
        INSERT INTO container_image_profile_revisions (
          workspace, profile, revision, revision_fingerprint, created_at,
          updated_at
        )
        SELECT 'research', 'page-' || printf('%03d', n), 1,
               :revision_fingerprint, 1, 1
        FROM synthetic
    """)
    with image_state_engine.begin() as connection:
        connection.execute(seed_profiles,
                           {'revision_fingerprint': _POLICY_FINGERPRINT})
    due = state.ensure_location(
        image.id,
        'page-129',
        'canonical',
        'e' * 64,
        _DIGEST,
        policy_fingerprint='d' * 64,
        profile_revision=1,
        profile_revision_fingerprint=_POLICY_FINGERPRINT,
        canonical=True)
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()

    page_calls = ((130 + state._MAX_PROFILE_PROBES_PER_CALL - 1) //
                  state._MAX_PROFILE_PROBES_PER_CALL)
    listed = []
    for _ in range(page_calls):
        listed.extend(
            state.list_reconciliation_candidates('research', now=1000, limit=1))
    assert [candidate.id for candidate in listed] == [due.id]

    claimed = None
    for _ in range(page_calls):
        claimed = state.claim_next_reconciliation_candidate(
            'research',
            'paged-reconciler',
            materialization_lease_seconds=30,
            verification_lease_seconds=30,
            now=1000)
        if claimed is not None:
            break
    assert claimed is not None
    assert claimed.id == due.id


def test_bounded_eviction_profile_pages_eventually_revisit_due_work(
        image_state_engine, monkeypatch):
    monkeypatch.setattr(state.time, 'time', lambda: 1000)
    image = state.register_image(_SOURCE, _SOURCE, _DIGEST, 'research',
                                 'user-1')
    seed_profiles = sqlalchemy.text("""
        WITH RECURSIVE synthetic(n) AS (
          VALUES(0)
          UNION ALL
          SELECT n + 1 FROM synthetic WHERE n < 129
        )
        INSERT INTO container_image_profile_revisions (
          workspace, profile, revision, revision_fingerprint, created_at,
          updated_at
        )
        SELECT 'research', 'evict-page-' || printf('%03d', n), 1,
               :revision_fingerprint, 1, 1
        FROM synthetic
    """)
    with image_state_engine.begin() as connection:
        connection.execute(seed_profiles,
                           {'revision_fingerprint': _POLICY_FINGERPRINT})

    profile_name = 'evict-page-129'
    canonical = state.ensure_location(
        image.id,
        profile_name,
        'canonical',
        'e' * 64,
        _DIGEST,
        policy_fingerprint='d' * 64,
        profile_revision=1,
        profile_revision_fingerprint=_POLICY_FINGERPRINT,
        canonical=True)
    regional = state.ensure_location(
        image.id,
        profile_name,
        'regional',
        'f' * 64,
        _DIGEST,
        policy_fingerprint='c' * 64,
        profile_revision=1,
        profile_revision_fingerprint=_POLICY_FINGERPRINT,
        canonical=False,
        canonical_location_id=canonical.id,
        auto_evict=True)
    canonical_claim = state.claim_location(canonical.id, 'canonical-importer',
                                           30)
    assert canonical_claim is not None
    assert canonical_claim.lease_owner is not None
    assert _complete_location(
        canonical.id, canonical_claim.lease_owner,
        f'registry.example.com/skypilot/canonical@{_DIGEST}', _DIGEST)
    regional_claim = state.claim_location(regional.id, 'regional-importer', 30)
    assert regional_claim is not None
    assert regional_claim.lease_owner is not None
    assert _complete_location(
        regional.id, regional_claim.lease_owner,
        f'registry.example.com/skypilot/regional@{_DIGEST}', _DIGEST)
    with state._PROFILE_CURSOR_LOCK:
        state._PROFILE_CURSORS.clear()

    page_calls = ((130 + state._MAX_PROFILE_PROBES_PER_CALL - 1) //
                  state._MAX_PROFILE_PROBES_PER_CALL)
    listed = []
    for _ in range(page_calls):
        listed.extend(
            state.list_eviction_candidates('research',
                                           unused_before=1001,
                                           limit=1))
    assert [candidate.id for candidate in listed] == [regional.id]

    claimed = None
    for _ in range(page_calls):
        claimed = state.claim_next_eviction_candidate('research',
                                                      'paged-evictor',
                                                      lease_seconds=30,
                                                      unused_before=1001,
                                                      now=1000)
        if claimed is not None:
            break
    assert claimed is not None
    assert claimed.id == regional.id


def test_partial_work_queue_indexes_exclude_exhausted_attempts(
        image_state_engine):
    queue_indexes = {
        'ix_container_image_locations_profile_eviction_ready',
        'ix_container_image_locations_profile_eviction_retry',
        'ix_container_image_locations_profile_eviction_lease',
        'ix_container_image_locations_profile_eviction_incomplete_lease',
        'ix_container_image_locations_profile_pending_queue',
        'ix_container_image_locations_regional_pending_queue',
        'ix_container_image_locations_profile_pending_retry',
        'ix_container_image_locations_regional_pending_retry',
        'ix_container_image_locations_profile_copying_queue',
        'ix_container_image_locations_regional_copying_queue',
        'ix_container_image_locations_profile_copying_incomplete_lease',
        'ix_container_image_locations_regional_copying_incomplete_lease',
        'ix_container_image_locations_profile_retry_queue',
        'ix_container_image_locations_regional_retry_queue',
        'ix_container_image_locations_profile_verification_queue',
        'ix_container_image_locations_regional_verify_queue',
        'ix_container_image_locations_profile_verification_retry',
        'ix_container_image_locations_regional_verification_retry',
    }
    active_indexes = {
        'ix_container_image_locations_profile_copying_active_lease',
        'ix_container_image_locations_profile_evicting_active_lease',
        'ix_container_image_locations_profile_verification_active_lease',
    }
    placeholders = ', '.join('?' for _ in queue_indexes)
    with image_state_engine.connect() as connection:
        rows = connection.exec_driver_sql(
            'SELECT name, sql FROM sqlite_master WHERE type = \'index\' '
            f'AND name IN ({placeholders})',
            tuple(sorted(queue_indexes))).all()
        active_rows = connection.exec_driver_sql(
            "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND "
            f"name IN ({', '.join('?' for _ in active_indexes)})",
            tuple(sorted(active_indexes))).all()
    assert {name for name, _ in rows} == queue_indexes
    for _, sql in rows:
        assert 'attempt_count < 20' in sql
    assert {name for name, _ in active_rows} == active_indexes
    for _, sql in active_rows:
        assert 'lease_owner IS NOT NULL' in sql


def test_table_and_index_races_do_not_skip_later_index_repairs():
    already_exists = sqlalchemy.exc.OperationalError(
        'CREATE', {}, RuntimeError('already exists'))
    table = mock.Mock()
    table.create.side_effect = already_exists
    first_index = mock.Mock()
    first_index.create.side_effect = already_exists
    second_index = mock.Mock()
    table.indexes = [first_index, second_index]
    metadata = mock.Mock()
    metadata.tables = {'locations': table}
    engine = mock.sentinel.engine

    db_utils.add_all_tables_to_db_sqlalchemy(metadata, engine)

    table.create.assert_called_once_with(bind=engine, checkfirst=True)
    first_index.create.assert_called_once_with(bind=engine, checkfirst=True)
    second_index.create.assert_called_once_with(bind=engine, checkfirst=True)


def test_service_snapshot_uses_explicit_durable_workspace(
        image_state_engine, monkeypatch):
    del image_state_engine
    _mock_registry_profile(monkeypatch)
    image = _publish_state_image(_SOURCE, _DIGEST, release='workspace-snapshot')
    monkeypatch.setattr(skypilot_config, 'get_active_workspace',
                        lambda: 'different-workspace')
    task = task_lib.Task().set_resources(
        resources_lib.Resources(container_image={
            'release': 'workspace-snapshot',
            'distribution': 'managed',
        }))

    assert serve_utils.snapshot_service_container_images(
        task, workspace='research') == image.id
    assert next(iter(task.resources)).container_image.artifact_id == image.id
