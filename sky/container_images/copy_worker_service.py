"""Independently deployable copy and source-inspection worker service."""

from __future__ import annotations

import base64
from collections.abc import Callable
import concurrent.futures
import contextlib
import hashlib
import json
import os
import pathlib
import re
import signal
import threading
import time
from typing import Any
import uuid

from sqlalchemy import orm

from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import kubernetes
from sky.container_images import aws
from sky.container_images import budgets
from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import models
from sky.container_images import oci
from sky.container_images import providers
from sky.container_images import qualification
from sky.container_images import topology_state
from sky.container_images import transactions
from sky.container_images import worker_health
from sky.container_images import worker_lease
from sky.server import database_migrations

_ECR_AUTHORITY = re.compile(
    r'^(?P<account>[0-9]{12})\.dkr\.ecr\.(?P<region>[a-z0-9-]+)\.'
    r'amazonaws\.com(?:\.cn)?$')
_DEFAULT_LEASE_SECONDS = 15 * 60
_CONFIG_REFRESH_SECONDS = 60
_QUALIFICATION_MANIFEST_LIMIT = 256
_INVENTORY_CONFIRMATION_LIMIT = 100
_CANDIDATE_SHARD_PROBE_LIMIT = 16
_QUALIFICATION_ACTOR_HASH = hashlib.sha256(
    b'skypilot-image-qualification-manifest-ingestor').hexdigest()

logger = sky_logging.init_logger(__name__)


def _qualification_database_epoch(*, now: int | None = None) -> int:
    """Returns the shared clock used by qualification freshness checks."""
    with orm.Session(catalog_state.engine()) as session:
        return catalog_state.database_epoch(session, now=now)


def _ingest_qualification_manifests(directory: str) -> int:
    """Ingests a bounded ConfigMap projection without provider I/O."""
    root = pathlib.Path(directory)
    if not root.is_dir():
        logger.warning('Image qualification manifest directory is unavailable.')
        return 0
    paths = sorted(root.glob('*.json'))
    if len(paths) > _QUALIFICATION_MANIFEST_LIMIT:
        logger.warning('Image qualification manifest directory is over limit.')
        paths = paths[:_QUALIFICATION_MANIFEST_LIMIT]
    ingested = 0
    for path in paths:
        try:
            payload = path.read_bytes()
            if len(payload) > 4 * 1024 * 1024:
                raise ValueError('Qualification manifest exceeds 4 MiB.')
            manifest = json.loads(payload)
            if not isinstance(manifest, dict):
                raise ValueError('Qualification manifest must be an object.')
            profile = manifest.get('profile')
            if not isinstance(profile, str):
                raise ValueError('Qualification manifest profile is missing.')
            digest = hashlib.sha256(payload).hexdigest()
            qualification.ingest_manifest(
                profile_name=profile,
                manifest=manifest,
                actor_hash=_QUALIFICATION_ACTOR_HASH,
                idempotency_key=f'qualification-manifest:{digest}')
            ingested += 1
        except (OSError, TypeError, ValueError):
            # Keep values and paths out of logs. The readiness projection and
            # explicit qualification endpoint expose the bounded error state.
            logger.warning('Image qualification manifest ingestion failed.')
    return ingested


_LeaseHeartbeat = worker_lease.LeaseHeartbeat


def _docker_config_credentials(binding: models.RegistryAccessBinding,
                               authority: str) -> providers.SourceCredentials:
    assert binding.reference is not None
    try:
        configured_allowlist = json.loads(
            os.environ.get('SKYPILOT_IMAGE_SOURCE_SECRET_ALLOWLIST', '[]'))
    except ValueError:
        raise ValueError('AUTH_BINDING_UNAVAILABLE') from None
    if (not isinstance(configured_allowlist, list) or
            len(configured_allowlist) > 64 or
            any(not isinstance(item, dict) or set(item) !=
                {'namespace', 'name'} or not isinstance(item['namespace'], str)
                or not isinstance(item['name'], str)
                for item in configured_allowlist)):
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    allowed = {
        (item['namespace'], item['name']) for item in configured_allowlist
    }
    secret_identity = (binding.reference['namespace'],
                       binding.reference['name'])
    if secret_identity not in allowed:
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    secret = kubernetes.core_api().read_namespaced_secret(
        binding.reference['name'],
        binding.reference['namespace'],
        _request_timeout=kubernetes.API_TIMEOUT)
    encoded = (secret.data or {}).get(binding.reference['key'])
    if not isinstance(encoded, str):
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    try:
        document = json.loads(base64.b64decode(encoded))
    except (ValueError, UnicodeDecodeError):
        raise ValueError('AUTH_BINDING_UNAVAILABLE') from None
    auths = document.get('auths')
    if not isinstance(auths, dict):
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    matching: dict[str, Any] | None = None
    for registry, value in auths.items():
        normalized = str(registry).removeprefix('https://').removeprefix(
            'http://').rstrip('/')
        if normalized == authority and isinstance(value, dict):
            matching = value
            break
    if matching is None:
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    identity_token = matching.get('identitytoken')
    if isinstance(identity_token, str) and identity_token:
        return providers.SourceCredentials(bearer_token=identity_token)
    username = matching.get('username')
    password = matching.get('password')
    if isinstance(username, str) and isinstance(password, str):
        return providers.SourceCredentials(username=username, password=password)
    encoded_auth = matching.get('auth')
    if not isinstance(encoded_auth, str):
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    try:
        username, password = base64.b64decode(encoded_auth).decode().split(
            ':', 1)
    except (ValueError, UnicodeDecodeError):
        raise ValueError('AUTH_BINDING_UNAVAILABLE') from None
    return providers.SourceCredentials(username=username, password=password)


def _source_reader(
    source: catalog_state.SourceRecord,
    profile_name: str,
    provider_fence: Callable[[], None] | None = None,
) -> providers.RegistryV2Source:
    binding = config.get_source_binding(source.source_auth_binding_id)
    if binding is not None and binding.fingerprint != source.source_auth_fingerprint:
        raise ValueError('AUTH_BINDING_UNAVAILABLE')
    cached: list[providers.SourceCredentials | None] = []

    def resolve() -> providers.SourceCredentials | None:
        if cached:
            return cached[0]
        if binding is None:
            credentials = None
        elif (binding.kind ==
              models.RegistryAccessBindingKind.KUBERNETES_DOCKERCONFIG_SECRET):
            authority = models.reference_registry_authority(
                source.source_ref, 'OCI source reference')
            credentials = _docker_config_credentials(binding, authority)
        elif binding.kind == models.RegistryAccessBindingKind.AWS_ASSUME_ROLE:
            authority = models.reference_registry_authority(
                source.source_ref, 'OCI source reference')
            match = _ECR_AUTHORITY.fullmatch(authority)
            if match is None or binding.authority is None:
                raise ValueError('AUTH_BINDING_UNAVAILABLE')
            credentials = aws.mint_ecr_source_credentials(
                aws.AwsRoleBinding(
                    role_arn=binding.authority,
                    external_id=binding.external_id,
                    session_name=f'sky-img-source-{uuid.uuid4().hex[:16]}',
                    catalog_tag=catalog_state.get_catalog_authority_id(),
                    profile_tag=profile_name),
                region=match.group('region'),
                account=match.group('account'),
                expected_authority=authority,
                provider_fence=provider_fence)
        else:
            raise ValueError('AUTH_BINDING_UNAVAILABLE')
        cached.append(credentials)
        return credentials

    return providers.RegistryV2Source(source.source_ref,
                                      resolve,
                                      provider_fence=provider_fence)


def _inspection_graph(source_reader: providers.RegistryV2Source,
                      requested_platform: str,
                      max_artifact_bytes: int) -> oci.OciContentGraph:
    limits = oci.OciInspectionLimits(max_artifact_bytes=max_artifact_bytes)
    return oci.build_content_graph(
        raw_root=source_reader.read_root(max_bytes=limits.max_root_bytes),
        expected_root_digest=source_reader.digest,
        requested_platform=requested_platform,
        fetch_manifest=lambda digest: source_reader.read_manifest(
            digest, max_bytes=limits.max_manifest_bytes),
        fetch_blob=lambda digest: source_reader.read_blob_bytes(
            digest, max_bytes=limits.max_config_bytes),
        limits=limits)


def inspect_publication(publication: catalog_state.PublicationRecord,
                        *,
                        lease_seconds: int = _DEFAULT_LEASE_SECONDS) -> bool:
    token = publication.inspection_lease_token
    if token is None:
        return False
    revision = topology_state.get_profile_revision(
        publication.profile_revision_id)
    if revision is None:
        raise ValueError('PROFILE_NOT_ACTIVE')
    profile = models.ManagedRegistryProfile.from_snapshot(
        revision.config_snapshot)
    operation = catalog_state.get_operation(publication.operation_id,
                                            publication.workspace)
    if operation is None:
        raise ValueError('Publication operation is missing.')
    source = catalog_state.SourceRecord(
        id='00000000-0000-4000-8000-000000000000',
        workspace=publication.workspace,
        image_id='00000000-0000-4000-8000-000000000000',
        source_ref=publication.source_ref,
        source_root_digest=publication.source_root_digest,
        source_root_media_type='',
        requested_platform=publication.requested_platform,
        selected_child_digest='',
        source_auth_binding_id=publication.source_auth_binding_id,
        source_auth_fingerprint=publication.source_auth_fingerprint,
        created_at=publication.created_at)
    heartbeat = _LeaseHeartbeat(
        lambda: catalog_state.heartbeat_publication_inspection(
            publication.id, token, lease_seconds), max(1.0, lease_seconds / 3))
    try:
        with heartbeat:
            reader = _source_reader(source, profile.name,
                                    heartbeat.assert_owned)
            graph = _inspection_graph(reader, publication.requested_platform,
                                      profile.limits.max_artifact_bytes)
            heartbeat.assert_owned()
            transactions.bind_inspected_publication(
                publication_id=publication.id,
                inspection_lease_token=token,
                creator_user_hash=operation.actor_hash,
                runtime_digest=graph.runtime_digest,
                platform=graph.platform,
                config_digest=graph.config.digest,
                source_root_media_type=graph.source_root_media_type,
                selected_manifest_media_type=graph.runtime_media_type,
                selected_manifest_size_bytes=len(graph.raw_runtime_manifest),
                declared_size_bytes=graph.declared_size_bytes,
                canonical_target_id=profile.canonical.name,
                max_releases_per_artifact=(
                    profile.limits.max_releases_per_artifact))
        return True
    except topology_state.RegistryCapacityExhaustedError:
        catalog_state.fail_publication_inspection(
            publication.id,
            token,
            models.ImageLocationErrorCode.REGISTRY_CAPACITY_EXHAUSTED.value,
            retry_at=None,
            terminal=True)
        return False
    except transactions.ImageLimitExceededError:
        catalog_state.fail_publication_inspection(publication.id,
                                                  token,
                                                  'IMAGE_LIMIT_EXCEEDED',
                                                  retry_at=None,
                                                  terminal=True)
        return False
    except (ValueError, TypeError):
        catalog_state.fail_publication_inspection(
            publication.id,
            token,
            models.ImageLocationErrorCode.SOURCE_CONTENT_UNSUPPORTED.value,
            retry_at=None,
            terminal=True)
        return False
    except Exception:  # pylint: disable=broad-except
        catalog_state.fail_publication_inspection(
            publication.id,
            token,
            models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value,
            retry_delay_seconds=min(3600, 2**min(publication.attempt_count,
                                                 10)),
            terminal=False)
        return False


def _aws_role(binding: models.RegistryAccessBinding,
              profile: models.ManagedRegistryProfile,
              purpose: str) -> aws.AwsRoleBinding:
    if (binding.kind != models.RegistryAccessBindingKind.AWS_ASSUME_ROLE or
            purpose not in binding.purposes or binding.authority is None):
        raise ValueError('Registry access binding cannot perform this purpose.')
    return aws.AwsRoleBinding(
        role_arn=binding.authority,
        external_id=binding.external_id,
        session_name=f'sky-img-copy-{uuid.uuid4().hex[:16]}',
        catalog_tag=catalog_state.get_catalog_authority_id(),
        profile_tag=profile.name)


def _ecr_hooks(
    limiter: budgets.ProviderBudgetLimiter,
    shard: topology_state.ShardRecord,
    heartbeat: worker_lease.LeaseHeartbeat | None = None,
) -> aws.EcrCallHooks:

    def before_call() -> None:
        if heartbeat is not None:
            heartbeat.assert_owned()
        limiter.before_call(shard)
        if heartbeat is not None:
            heartbeat.assert_owned()

    return aws.EcrCallHooks(before_call=before_call,
                            on_throttle=lambda: limiter.record_throttle(shard))


def _graph_for_location(
    location: topology_state.LocationRecord,
    artifact: catalog_state.ArtifactRecord,
    profile: models.ManagedRegistryProfile,
    limiter: budgets.ProviderBudgetLimiter,
    heartbeat: worker_lease.LeaseHeartbeat,
) -> tuple[oci.OciContentGraph, Callable[[oci.OciDescriptor], Any]]:
    if location.canonical:
        source = catalog_state.source_for_canonical_location(location.id)
        if source is None:
            raise ValueError('Canonical location has no retained source.')
        reader = _source_reader(source, profile.name, heartbeat.assert_owned)
        graph = _inspection_graph(reader, artifact.platform,
                                  profile.limits.max_artifact_bytes)
        return graph, reader.read_blob
    if location.canonical_location_id is None:
        raise ValueError('Regional location has no canonical source.')
    canonical = topology_state.get_location(location.canonical_location_id)
    if canonical is None or canonical.state != models.ImageLocationState.READY:
        raise ValueError('Regional location canonical source is not READY.')
    source_shard = topology_state.get_shard(canonical.shard_id)
    if source_shard is None:
        raise ValueError('Canonical registry shard is missing.')
    source_target = profile.target(source_shard.target_id)
    source_binding = profile.bindings[source_target.write_authority]
    source_role = _aws_role(source_binding, profile, 'source_read')
    heartbeat.assert_owned()
    source_repository = aws.EcrRepository.from_role(
        source_role,
        source_shard.region,
        source_shard.repository_name,
        hooks=_ecr_hooks(limiter, source_shard, heartbeat),
        provider_fence=heartbeat.assert_owned)
    heartbeat.assert_owned()
    limits = oci.OciInspectionLimits(
        max_artifact_bytes=profile.limits.max_artifact_bytes)
    raw = source_repository.read_manifest(artifact.runtime_digest)
    graph = oci.build_content_graph(
        raw_root=raw,
        expected_root_digest=artifact.runtime_digest,
        requested_platform=artifact.platform,
        fetch_manifest=source_repository.read_manifest,
        fetch_blob=lambda digest: source_repository.read_blob_bytes(
            digest, max_bytes=limits.max_config_bytes),
        limits=limits)
    return graph, source_repository.read_blob


def _profile_for_location(
    location: topology_state.LocationRecord,
    shard: topology_state.ShardRecord,
) -> models.ManagedRegistryProfile | None:
    """Finds a qualified snapshot matching every physical copy endpoint."""
    if (shard.target_fingerprint != location.target_fingerprint or
            shard.profile_revision_id is None):
        return None
    canonical = None
    canonical_shard = None
    if not location.canonical:
        if location.canonical_location_id is None:
            return None
        canonical = topology_state.get_location(location.canonical_location_id)
        if canonical is None:
            return None
        canonical_shard = topology_state.get_shard(canonical.shard_id)
        if (canonical_shard is None or canonical_shard.profile_revision_id
                != shard.profile_revision_id):
            return None
    revision = topology_state.get_profile_revision(shard.profile_revision_id)
    if (revision is None or revision.workspace != location.workspace or
            revision.profile != shard.profile or
            revision.state not in (models.ImageProfileState.ACTIVE,
                                   models.ImageProfileState.RETIRED)):
        return None
    profile = models.ManagedRegistryProfile.from_snapshot(
        revision.config_snapshot)
    try:
        target = profile.target(shard.target_id)
        if target.target_fingerprint != location.target_fingerprint:
            return None
        if canonical is not None and canonical_shard is not None:
            source = profile.target(canonical_shard.target_id)
            if (source.target_fingerprint != canonical.target_fingerprint or
                    canonical_shard.target_fingerprint
                    != canonical.target_fingerprint):
                return None
    except ValueError:
        return None
    return profile


def copy_location(location: topology_state.LocationRecord,
                  *,
                  limiter: budgets.ProviderBudgetLimiter,
                  lease_seconds: int = _DEFAULT_LEASE_SECONDS) -> bool:
    token = location.lease_token
    if token is None:
        return False
    try:
        artifact = catalog_state.get_artifact(location.image_id,
                                              location.workspace)
        shard = topology_state.get_shard(location.shard_id)
        if artifact is None or shard is None:
            raise ValueError('Location artifact or shard is missing.')
        profile = _profile_for_location(location, shard)
        if profile is None:
            raise ValueError('PROFILE_NOT_ACTIVE')
        target = profile.target(shard.target_id)
        write_binding = profile.bindings[target.write_authority]
        heartbeat = _LeaseHeartbeat(
            lambda: topology_state.heartbeat_location(location.id, token,
                                                      lease_seconds),
            max(1.0, lease_seconds / 3))
        with heartbeat:
            graph, read_blob = _graph_for_location(location, artifact, profile,
                                                   limiter, heartbeat)
            if (graph.runtime_digest != artifact.runtime_digest or
                    graph.config.digest != artifact.config_digest or
                    graph.platform != artifact.platform):
                raise aws.DestinationContentMismatchError(
                    'Source evidence no longer matches catalog artifact.')
            destination_role = _aws_role(write_binding, profile,
                                         'destination_write')
            heartbeat.assert_owned()
            destination = aws.EcrRepository.from_role(
                destination_role,
                shard.region,
                shard.repository_name,
                hooks=_ecr_hooks(limiter, shard, heartbeat),
                provider_fence=(heartbeat.assert_owned))
            heartbeat.assert_owned()
            if location.state == models.ImageLocationState.COPYING:
                outcome = destination.copy_graph(graph, read_blob,
                                                 heartbeat.cancel_event)
                heartbeat.assert_owned()
                if not topology_state.transition_location_to_verifying(
                        location.id,
                        token,
                        ambiguous=outcome == aws.CopyOutcome.AMBIGUOUS):
                    raise RuntimeError('Location copy lease was lost.')
            verified = destination.verify_graph(graph)
            heartbeat.assert_owned()
        transactions.converge_canonical(
            location_id=location.id,
            lease_token=token,
            ready=verified,
            error_code=(None if verified else
                        models.ImageLocationErrorCode.MANIFEST_MISSING.value),
            retry_delay_seconds=None if verified else 30,
            terminal=False)
        return verified
    except (aws.ProviderThrottledError, budgets.ProviderBudgetUnavailableError):
        transactions.converge_canonical(
            location_id=location.id,
            lease_token=token,
            ready=False,
            error_code=models.ImageLocationErrorCode.PROVIDER_THROTTLED.value,
            retry_delay_seconds=30,
            terminal=False)
        return False
    except aws.DestinationContentMismatchError:
        transactions.converge_canonical(
            location_id=location.id,
            lease_token=token,
            ready=False,
            error_code=(models.ImageLocationErrorCode.
                        DESTINATION_DIGEST_MISMATCH.value),
            terminal=True)
        return False
    except Exception:  # pylint: disable=broad-except
        try:
            transactions.converge_canonical(
                location_id=location.id,
                lease_token=token,
                ready=False,
                error_code=(
                    models.ImageLocationErrorCode.MATERIALIZATION_FAILED.value),
                retry_delay_seconds=min(3600, 2**min(location.attempt_count,
                                                     10)),
                terminal=False)
        except topology_state.LocationLeaseLostError:
            pass
        return False


def _profile_for_shard(
    shard: topology_state.ShardRecord,
) -> tuple[topology_state.ProfileRevisionRecord, models.ManagedRegistryProfile]:
    allowed_states: tuple[models.ImageProfileState, ...]
    if shard.profile_revision_id is not None:
        operational = topology_state.get_profile_revision(
            shard.profile_revision_id)
        revisions = [] if operational is None else [operational]
        allowed_states = (models.ImageProfileState.ACTIVE,
                          models.ImageProfileState.RETIRED)
    else:
        revisions = topology_state.list_profile_revisions(shard.workspace,
                                                          profile=shard.profile,
                                                          limit=1001)
        allowed_states = (models.ImageProfileState.QUALIFYING,)
    attestation_key = models.profile_attestation_key('terraform_shard',
                                                     shard.physical_fingerprint)
    for revision in revisions:
        if (revision.workspace != shard.workspace or
                revision.profile != shard.profile or
                revision.state not in allowed_states):
            continue
        profile = models.ManagedRegistryProfile.from_snapshot(
            revision.config_snapshot)
        try:
            target = profile.target(shard.target_id)
        except ValueError:
            continue
        expected = revision.attestations.get(attestation_key)
        if (target.target_fingerprint == shard.target_fingerprint and
                isinstance(expected, dict) and
                expected.get('status') == 'READY' and
                expected.get('physical_fingerprint')
                == shard.physical_fingerprint):
            return revision, profile
    raise ValueError('Registry shard has no matching usable profile revision.')


def _expected_shard_attestation(
    revision: topology_state.ProfileRevisionRecord,
    shard: topology_state.ShardRecord,
) -> tuple[str, dict[str, Any]]:
    key = models.profile_attestation_key('terraform_shard',
                                         shard.physical_fingerprint)
    expected = revision.attestations.get(key)
    if (not isinstance(expected, dict) or expected.get('status') != 'READY' or
            expected.get('physical_fingerprint') != shard.physical_fingerprint
            or not isinstance(expected.get('live_attestation_key'), str)):
        raise LookupError('Terraform shard attestation is not committed yet.')
    return str(expected['live_attestation_key']), expected


def _matching_shard_metadata(
    repository: aws.EcrRepository,
    role: aws.AwsRoleBinding,
    shard: topology_state.ShardRecord,
    expected: dict[str, Any],
    provider_fence: Callable[[], None] | None = None,
) -> tuple[dict[str, Any], int, int] | None:
    """Returns exact live facts only when Terraform and quota still agree."""
    metadata = repository.repository_metadata()
    quota_kwargs = ({
        'provider_fence': provider_fence
    } if provider_fence is not None else {})
    applied_quota = aws.applied_ecr_images_per_repository_quota(
        role, shard.region, **quota_kwargs)
    expected_values = {
        'repository_arn': expected.get('repository_arn'),
        'repository_uri': expected.get('repository_uri'),
        'tag_mutability': expected.get('tag_mutability'),
        'encryption_type': expected.get('encryption_type'),
        'kms_key': expected.get('kms_key'),
        'scanning_mode': expected.get('scanning_mode'),
        'policy_hash': expected.get('policy_hash'),
        'ownership_tags_hash': expected.get('ownership_tags_hash'),
    }
    headroom = expected.get('reserved_headroom')
    terraform_quota = expected.get('terraform_applied_quota')
    max_manifests = expected.get('max_manifests')
    if (metadata != expected_values or type(headroom) is not int or
            type(terraform_quota) is not int or
            type(max_manifests) is not int or applied_quota < terraform_quota or
            max_manifests + headroom > applied_quota):
        return None
    return metadata, applied_quota, headroom


def _reconcile_candidate_shard_attestation(
    revision: topology_state.ProfileRevisionRecord,
    profile: models.ManagedRegistryProfile,
    target: models.ManagedRegistryTarget,
    *,
    limiter: budgets.ProviderBudgetLimiter,
    now: int,
    state_now: int | None = None,
) -> bool:
    """Probes one candidate authority without mutating operational inventory."""
    if revision.state != models.ImageProfileState.QUALIFYING:
        return True
    shards = topology_state.list_target_shards(revision.workspace, profile.name,
                                               target.name)
    if (len(shards) != target.shard_count or
            any(shard.target_fingerprint != target.target_fingerprint
                for shard in shards)):
        return False
    probed = 0
    for shard in shards:
        live_key, expected = _expected_shard_attestation(revision, shard)
        evidence = revision.attestations.get(live_key)
        if (isinstance(evidence, dict) and evidence.get('status') == 'READY' and
                isinstance(evidence.get('observed_at'), int) and 0 <=
                now - evidence['observed_at'] <= _CONFIG_REFRESH_SECONDS * 10):
            continue
        if probed >= _CANDIDATE_SHARD_PROBE_LIMIT:
            return False
        # Before first activation, resumable inventory owns both the physical
        # scan and candidate attestation. A later revision may reuse only a
        # fresh operational epoch and cannot mutate the shared shard.
        if (shard.profile_revision_id is None or
                shard.state not in (models.ImageShardState.READY,
                                    models.ImageShardState.FULL) or
                shard.inventory_completed_at is None or
                not 0 <= now - shard.inventory_completed_at <=
                _CONFIG_REFRESH_SECONDS * 10):
            return False
        operational = topology_state.get_profile_revision(
            shard.profile_revision_id)
        if (operational is None or
                operational.state != models.ImageProfileState.ACTIVE):
            return False
        binding = profile.bindings[target.write_authority]
        role = _aws_role(binding, profile, 'verify')
        repository = aws.EcrRepository.from_role(role,
                                                 shard.region,
                                                 shard.repository_name,
                                                 hooks=_ecr_hooks(
                                                     limiter, shard))
        verified = _matching_shard_metadata(repository, role, shard, expected)
        if verified is None:
            return False
        metadata, applied_quota, headroom = verified
        recorded = topology_state.record_candidate_shard_attestation(
            profile_revision_id=revision.id,
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            shard_id=shard.id,
            expected_operational_revision_id=shard.profile_revision_id,
            expected_target_fingerprint=shard.target_fingerprint,
            expected_physical_fingerprint=shard.physical_fingerprint,
            expected_inventory_epoch=shard.inventory_epoch,
            expected_inventory_completed_at=shard.inventory_completed_at,
            kind=live_key,
            evidence={
                'status': 'READY',
                'physical_fingerprint': shard.physical_fingerprint,
                'target_fingerprint': shard.target_fingerprint,
                **metadata,
                'applied_images_per_repository_quota': applied_quota,
                'reserved_headroom': headroom,
                'inventory_epoch': shard.inventory_epoch,
                'inventory_completed_at': shard.inventory_completed_at,
            },
            now=state_now)
        if recorded is None:
            return False
        probed += 1
    return True


def _qualification_copy_needed(revision: topology_state.ProfileRevisionRecord,
                               profile: models.ManagedRegistryProfile,
                               target: models.ManagedRegistryTarget,
                               now: int) -> bool:
    copy_key = models.profile_attestation_key('copy', target.name)
    copy_evidence = revision.attestations.get(copy_key)
    copy_fresh = (
        isinstance(copy_evidence, dict) and
        copy_evidence.get('status') == 'READY' and
        isinstance(copy_evidence.get('observed_at'), int) and
        0 <= now - copy_evidence['observed_at'] <= _CONFIG_REFRESH_SECONDS * 10)
    if revision.state == models.ImageProfileState.QUALIFYING:
        return not copy_fresh
    for backend, binding_id in target.runtime_pull:
        binding = profile.bindings[binding_id]
        for runtime_id in qualification.runtime_ids(target, backend, binding):
            runtime_key = models.profile_attestation_key(
                'runtime', target.name, backend, binding.fingerprint,
                runtime_id)
            runtime = revision.attestations.get(runtime_key)
            if (not isinstance(runtime, dict) or
                    runtime.get('status') != 'READY' or
                    not isinstance(runtime.get('observed_at'), int) or
                    now - runtime['observed_at'] >=
                    profile.qualification.runtime_attestation_max_age_seconds):
                return not copy_fresh
    return False


def reconcile_qualification_copy(revision: topology_state.ProfileRevisionRecord,
                                 target: models.ManagedRegistryTarget,
                                 *,
                                 limiter: budgets.ProviderBudgetLimiter,
                                 now: int | None = None) -> bool:
    """Attests live infrastructure and copies the fixed canary as copy role."""
    profile = models.ManagedRegistryProfile.from_snapshot(
        revision.config_snapshot)
    shard = topology_state.get_target_shard(revision.workspace, profile.name,
                                            target.name)
    if shard is None:
        return False
    repository_name, repository_arn = qualification.qualification_repository(
        revision, target)
    binding = profile.bindings[target.write_authority]
    destination = aws.EcrRepository.from_role(_aws_role(binding, profile,
                                                        'destination_write'),
                                              target.region,
                                              repository_name,
                                              hooks=_ecr_hooks(limiter, shard))
    metadata = destination.repository_metadata()
    expected_uri = f'{target.registry}/{repository_name}'
    if (metadata['repository_arn'] != repository_arn or
            metadata['repository_uri'] != expected_uri or
            metadata['tag_mutability'] != 'IMMUTABLE'):
        raise ValueError('Qualification repository live identity drifted.')
    revision = topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind=models.profile_attestation_key('infrastructure', target.name),
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'repository_uri': expected_uri,
            'tag_mutability': metadata['tag_mutability'],
            'encryption_type': metadata['encryption_type'],
            'kms_key': metadata['kms_key'],
        },
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash,
        now=now)
    candidate_now = _qualification_database_epoch(now=now)
    _reconcile_candidate_shard_attestation(revision,
                                           profile,
                                           target,
                                           limiter=limiter,
                                           now=candidate_now,
                                           state_now=now)
    copy_due_now = _qualification_database_epoch(now=now)
    if not _qualification_copy_needed(revision, profile, target, copy_due_now):
        return True
    reader = providers.RegistryV2Source(profile.qualification.canary_ref,
                                        lambda: None)
    graph = _inspection_graph(reader, profile.qualification.canary_platform,
                              profile.limits.max_artifact_bytes)
    outcome = destination.copy_graph(graph, reader.read_blob, threading.Event())
    if (outcome == aws.CopyOutcome.AMBIGUOUS and
            not destination.verify_graph(graph)):
        raise aws.AmbiguousProviderOutcomeError(
            'Qualification canary copy requires readback retry.')
    if not destination.verify_graph(graph):
        raise aws.DestinationContentMismatchError(
            'Qualification canary did not verify after copy.')
    topology_state.record_profile_attestation(
        profile_revision_id=revision.id,
        kind=models.profile_attestation_key('copy', target.name),
        evidence={
            'status': 'READY',
            'target': target.name,
            'target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': graph.runtime_digest,
            'platform': graph.platform,
            'copy_outcome': outcome.value,
        },
        expected_generation=revision.desired_generation,
        expected_config_hash=revision.config_hash,
        now=now)
    return True


def reconcile_qualification_profiles(limiter: budgets.ProviderBudgetLimiter,
                                     *,
                                     limit: int = 8,
                                     now: int | None = None) -> int:
    """Runs a bounded fair page of independent copy-role attestations."""
    completed = 0
    for revision in topology_state.list_qualifying_profiles(include_active=True,
                                                            limit=limit):
        profile = models.ManagedRegistryProfile.from_snapshot(
            revision.config_snapshot)
        for target in (profile.canonical,) + profile.targets:
            try:
                if reconcile_qualification_copy(revision,
                                                target,
                                                limiter=limiter,
                                                now=now):
                    completed += 1
            except Exception:  # pylint: disable=broad-except
                logger.warning('Managed image copy qualification probe failed.')
    return completed


def _qualification_maintenance(limiter: budgets.ProviderBudgetLimiter) -> bool:
    transactions.reconcile_pending_canonical_publications()
    reconcile_qualification_profiles(limiter)
    qualification.schedule_automatic_canaries()
    return True


def reconcile_inventory(
    shard: topology_state.ShardRecord,
    *,
    limiter: budgets.ProviderBudgetLimiter,
    lease_seconds: int = _DEFAULT_LEASE_SECONDS,
) -> bool:
    """Advances one provider or finalization page under durable authority."""
    token = shard.inventory_lease_token
    if token is None:
        return False
    heartbeat = _LeaseHeartbeat(
        lambda: topology_state.heartbeat_inventory_shard(
            shard.id, token, lease_seconds), max(1.0, lease_seconds / 3))
    try:
        with heartbeat:
            revision, profile = _profile_for_shard(shard)
            target = profile.target(shard.target_id)
            binding = profile.bindings[target.write_authority]
            role = _aws_role(binding, profile, 'verify')
            heartbeat.assert_owned()
            repository = aws.EcrRepository.from_role(
                role,
                shard.region,
                shard.repository_name,
                hooks=_ecr_hooks(limiter, shard, heartbeat),
                provider_fence=(heartbeat.assert_owned))
            heartbeat.assert_owned()
            live_key, expected = _expected_shard_attestation(revision, shard)
            verified = _matching_shard_metadata(
                repository,
                role,
                shard,
                expected,
                provider_fence=heartbeat.assert_owned)
            if verified is None:
                return topology_state.mark_shard_drifted(shard.id, token)
            metadata, applied_quota, headroom = verified
            completed: topology_state.ShardRecord | None
            if shard.inventory_finalizing:
                completed = shard
            else:
                digests, cursor = repository.inventory_page(
                    next_token=shard.inventory_cursor)
                completed = topology_state.record_inventory_page(
                    shard.id, token, digests, cursor)
                if completed is None:
                    return False
                if cursor is not None or completed.state not in (
                        models.ImageShardState.READY,
                        models.ImageShardState.FULL):
                    return topology_state.release_inventory_claim(
                        shard.id, token, completed.inventory_epoch)
            candidates = topology_state.list_inventory_missing_candidates(
                shard.id,
                completed.inventory_epoch,
                limit=_INVENTORY_CONFIRMATION_LIMIT)
            for location in candidates:
                present = repository.exact_manifest_exists(
                    location.runtime_digest)
                confirmed = topology_state.complete_inventory_confirmation(
                    location.id,
                    shard.id,
                    completed.inventory_epoch,
                    token,
                    present=present)
                if confirmed is None:
                    return False
            if completed.inventory_completed_at is None:
                return False
            recorded = (topology_state.record_inventory_attestation_and_release(
                profile_revision_id=revision.id,
                expected_generation=revision.desired_generation,
                expected_config_hash=revision.config_hash,
                shard_id=shard.id,
                inventory_lease_token=token,
                expected_profile_revision_id=shard.profile_revision_id,
                expected_target_fingerprint=shard.target_fingerprint,
                expected_physical_fingerprint=shard.physical_fingerprint,
                expected_inventory_epoch=completed.inventory_epoch,
                expected_inventory_completed_at=(
                    completed.inventory_completed_at),
                kind=live_key,
                evidence={
                    'status': 'READY',
                    'physical_fingerprint': shard.physical_fingerprint,
                    'target_fingerprint': shard.target_fingerprint,
                    **metadata,
                    'applied_images_per_repository_quota': applied_quota,
                    'reserved_headroom': headroom,
                    'inventory_epoch': completed.inventory_epoch,
                    'inventory_completed_at':
                        (completed.inventory_completed_at),
                }))
            return recorded is not None
    except (aws.ProviderThrottledError, aws.AmbiguousProviderOutcomeError,
            budgets.ProviderBudgetUnavailableError):
        topology_state.abandon_inventory_claim(shard.id, token,
                                               shard.inventory_epoch)
        return False
    except LookupError:
        topology_state.abandon_inventory_claim(shard.id, token,
                                               shard.inventory_epoch)
        return False
    except Exception as error:  # pylint: disable=broad-except
        topology_state.abandon_inventory_claim(
            shard.id,
            token,
            shard.inventory_epoch,
            invalid_cursor=(shard.inventory_cursor is not None and
                            aws.is_invalid_inventory_cursor(error)))
        return False


class CopyWorkerService:
    """Bounded concurrent claim loop with clean lease-based shutdown."""

    def __init__(self,
                 *,
                 worker_id: str,
                 version: str,
                 max_in_flight: int,
                 lease_seconds: int = _DEFAULT_LEASE_SECONDS,
                 health: worker_health.WorkerHealth | None = None) -> None:
        self.worker_id = worker_id
        self.version = version
        self.max_in_flight = max_in_flight
        self.lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._claim_inspection_next = True
        self._claims_since_inventory = 16
        self._budget_limiter = budgets.ProviderBudgetLimiter(worker_id)
        self._health = health

    def stop(self) -> None:
        self._stop.set()

    def _claim(self) -> tuple[str, Any] | None:
        if self._claims_since_inventory >= 16:
            inventory = topology_state.claim_inventory_shard(
                worker_id=self.worker_id, lease_seconds=self.lease_seconds)
            self._claims_since_inventory = 0
            if inventory is not None:
                return 'inventory', inventory
        for _ in range(2):
            if self._claim_inspection_next:
                publication = catalog_state.claim_publication_inspection(
                    worker_id=self.worker_id, lease_seconds=self.lease_seconds)
                self._claim_inspection_next = False
                if publication is not None:
                    self._claims_since_inventory += 1
                    return 'publication', publication
            else:
                location = topology_state.claim_next_location(
                    worker_id=self.worker_id, lease_seconds=self.lease_seconds)
                self._claim_inspection_next = True
                if location is not None:
                    self._claims_since_inventory += 1
                    return 'location', location
        inventory = topology_state.claim_inventory_shard(
            worker_id=self.worker_id, lease_seconds=self.lease_seconds)
        self._claims_since_inventory = 0
        if inventory is not None:
            return 'inventory', inventory
        return None

    def run_forever(self) -> None:
        topology_state.register_worker(self.worker_id,
                                       models.ImageWorkerKind.COPY,
                                       self.version, self.max_in_flight)
        if self._health is not None:
            self._health.registered()
        last_config_refresh = 0.0
        last_qualification_refresh = 0.0
        manifest_directory = os.environ.get(
            'SKYPILOT_IMAGE_QUALIFICATION_MANIFEST_DIR')
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_in_flight,
                thread_name_prefix='image-copy') as executor:
            futures: set[concurrent.futures.Future[bool]] = set()
            qualification_future: concurrent.futures.Future[bool] | None = None
            while not self._stop.is_set():
                if self._health is not None:
                    self._health.tick(len(futures))
                done = {future for future in futures if future.done()}
                for future in done:
                    with contextlib.suppress(Exception):
                        future.result()
                futures -= done
                if (qualification_future is not None and
                        qualification_future.done()):
                    qualification_future = None
                schedule_now = time.monotonic()
                if (schedule_now - last_config_refresh
                        >= _CONFIG_REFRESH_SECONDS):
                    try:
                        skypilot_config.safe_reload_config()
                        if manifest_directory is not None:
                            _ingest_qualification_manifests(manifest_directory)
                    except (OSError, TypeError, ValueError):
                        logger.warning(
                            'Image worker configuration refresh failed.')
                    last_config_refresh = schedule_now
                if (schedule_now - last_qualification_refresh
                        >= _CONFIG_REFRESH_SECONDS and
                        qualification_future is None and
                        len(futures) < self.max_in_flight):
                    qualification_future = executor.submit(
                        _qualification_maintenance, self._budget_limiter)
                    futures.add(qualification_future)
                    last_qualification_refresh = schedule_now
                heartbeat_ok = topology_state.heartbeat_worker(
                    self.worker_id, in_flight=len(futures), success=bool(done))
                if self._health is not None:
                    self._health.heartbeat(heartbeat_ok)
                while len(futures
                         ) < self.max_in_flight and not self._stop.is_set():
                    claim = self._claim()
                    if claim is None:
                        break
                    kind, record = claim
                    if kind == 'publication':
                        futures.add(
                            executor.submit(inspect_publication,
                                            record,
                                            lease_seconds=self.lease_seconds))
                    elif kind == 'location':
                        futures.add(
                            executor.submit(copy_location,
                                            record,
                                            limiter=self._budget_limiter,
                                            lease_seconds=self.lease_seconds))
                    else:
                        futures.add(
                            executor.submit(reconcile_inventory,
                                            record,
                                            limiter=self._budget_limiter,
                                            lease_seconds=self.lease_seconds))
                self._stop.wait(1 if futures else 5)


def main() -> None:
    max_in_flight = int(os.environ.get('SKYPILOT_IMAGE_MAX_IN_FLIGHT', '4'))
    if max_in_flight <= 0:
        raise ValueError('SKYPILOT_IMAGE_MAX_IN_FLIGHT must be positive.')
    # Provider-mutating workers must observe the same atomic deployment gate as
    # the API and lifecycle worker before they advertise health.
    database_migrations.initialize_central_databases()
    health = worker_health.WorkerHealth(
        'copy',
        liveness_deadline_seconds=int(
            os.environ.get('SKYPILOT_IMAGE_LIVENESS_DEADLINE_SECONDS', '30')))
    health_server = worker_health.HealthServer(
        health, int(os.environ.get('SKYPILOT_IMAGE_HEALTH_PORT', '8081')))
    service = CopyWorkerService(
        worker_id=os.environ.get('SKYPILOT_IMAGE_WORKER_ID', str(uuid.uuid4())),
        version=os.environ.get('SKYPILOT_IMAGE_WORKER_VERSION', 'dev'),
        max_in_flight=max_in_flight,
        lease_seconds=int(
            os.environ.get('SKYPILOT_IMAGE_LEASE_SECONDS',
                           str(_DEFAULT_LEASE_SECONDS))),
        health=health)
    signal.signal(signal.SIGTERM, lambda *_: service.stop())
    signal.signal(signal.SIGINT, lambda *_: service.stop())
    health_server.start()
    try:
        service.run_forever()
    finally:
        health_server.stop()


if __name__ == '__main__':
    main()
