"""Independently permissioned lifecycle and bounded compaction worker."""

from __future__ import annotations

from collections.abc import Callable
import concurrent.futures
import contextlib
import functools
import os
import signal
import threading
import time
import uuid

from sqlalchemy import orm

from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.container_images import aws
from sky.container_images import budgets
from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import demand_state
from sky.container_images import models
from sky.container_images import qualification
from sky.container_images import topology_state
from sky.container_images import transactions
from sky.container_images import worker_health
from sky.container_images import worker_lease
from sky.jobs import state as managed_job_state
from sky.serve import serve_state
from sky.server import database_migrations

_DEFAULT_LEASE_SECONDS = 15 * 60
_QUALIFICATION_DELETE_LEASE_SECONDS = 5 * 60
_READBACK_ATTEMPTS = 3
_CONSUMER_RECONCILIATION_SECONDS = 60
_TERMINAL_CONFIRMATION_SECONDS = 60 * 60
_UNATTACHED_REQUEST_RETENTION_SECONDS = 24 * 60 * 60

logger = sky_logging.init_logger(__name__)

_LeaseHeartbeat = worker_lease.LeaseHeartbeat


class _QualificationDrainRequested(RuntimeError):
    """Stops nondurable qualification work after process drain is observed."""


def _raise_if_stopping(should_stop: Callable[[], bool] | None) -> None:
    if should_stop is not None and should_stop():
        raise _QualificationDrainRequested()


def _qualification_delete_provider_fence(
    should_stop: Callable[[], bool] | None,
    heartbeat: worker_lease.LeaseHeartbeat,
    ownership_check: Callable[[], bool],
) -> None:
    """Fences one destructive provider call on the exact durable epoch."""
    _raise_if_stopping(should_stop)
    try:
        heartbeat.assert_owned()
    except worker_lease.LeaseLostError:
        raise _QualificationDrainRequested() from None
    if not ownership_check():
        raise _QualificationDrainRequested()


def _ecr_hooks(
    limiter: budgets.ProviderBudgetLimiter,
    shard: topology_state.ShardRecord,
    provider_fence: Callable[[], None] | None = None,
) -> aws.EcrCallHooks:
    """Binds provider-budget callbacks to one immutable shard record."""

    def before_call() -> None:
        if provider_fence is not None:
            provider_fence()
        limiter.before_call(shard)
        if provider_fence is not None:
            provider_fence()

    return aws.EcrCallHooks(before_call=before_call,
                            on_throttle=lambda: limiter.record_throttle(shard))


def _lifecycle_role(
        binding: models.RegistryAccessBinding,
        profile: models.ManagedRegistryProfile) -> aws.AwsRoleBinding:
    if (binding.kind != models.RegistryAccessBindingKind.AWS_ASSUME_ROLE or
            'lifecycle_delete' not in binding.purposes or
            'verify' not in binding.purposes or binding.authority is None):
        raise ValueError('Lifecycle binding cannot delete this target.')
    return aws.AwsRoleBinding(
        role_arn=binding.authority,
        external_id=binding.external_id,
        session_name=f'sky-img-lifecycle-{uuid.uuid4().hex[:12]}',
        catalog_tag=catalog_state.get_catalog_authority_id(),
        profile_tag=profile.name)


def _profile_target_for_location(
    location: topology_state.LocationRecord,
    shard: topology_state.ShardRecord,
) -> tuple[models.ManagedRegistryProfile, models.ManagedRegistryTarget] | None:
    """Resolves a worker snapshot only for the location's physical target."""
    if (shard.target_fingerprint != location.target_fingerprint or
            shard.profile_revision_id is None):
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
    except ValueError:
        return None
    if target.target_fingerprint != location.target_fingerprint:
        return None
    return profile, target


def _workspace_eviction_retentions() -> dict[str, int | None]:
    seconds_per_week = 7 * 24 * 60 * 60
    return {
        workspace: (
            None if policy.regional_cache_retention_weeks is None else
            policy.regional_cache_retention_weeks * seconds_per_week
        ) for workspace, policy in config.list_workspace_policies().items()
    }


def _refresh_workspace_eviction_retentions(
    previous: dict[str, int | None] | None,) -> dict[str, int | None] | None:
    """Reloads retention policy without killing the lifecycle claim loop."""
    try:
        skypilot_config.safe_reload_config()
        return _workspace_eviction_retentions()
    except (OSError, TypeError, ValueError):
        logger.warning('Image lifecycle policy refresh failed.')
        return previous


def _exact_presence_with_retry(
    repository: aws.EcrRepository,
    digest: str,
    heartbeat: worker_lease.LeaseHeartbeat,
) -> bool | None:
    """Retries only transient reads after destructive I/O has concluded."""
    for attempt in range(_READBACK_ATTEMPTS):
        try:
            return repository.exact_manifest_exists(digest)
        except (aws.ProviderThrottledError, aws.AmbiguousProviderOutcomeError,
                budgets.ProviderBudgetUnavailableError):
            if attempt + 1 == _READBACK_ATTEMPTS:
                return None
            heartbeat.assert_owned()
    return None


def evict_location(
    location: topology_state.LocationRecord,
    limiter: budgets.ProviderBudgetLimiter,
    *,
    lease_seconds: int = _DEFAULT_LEASE_SECONDS,
) -> bool:
    token = location.lease_token
    if (token is None or location.canonical or
            location.lease_kind not in ('EVICT', 'READBACK')):
        return False
    shard = topology_state.get_shard(location.shard_id)
    if shard is None:
        return False
    resolved = _profile_target_for_location(location, shard)
    if resolved is None:
        if location.lease_kind == 'EVICT':
            topology_state.complete_eviction(location.id,
                                             token,
                                             present=None,
                                             provider_not_called=True)
        return False
    profile, target = resolved
    if target.delete_authority is None:
        if location.lease_kind == 'EVICT':
            topology_state.complete_eviction(location.id,
                                             token,
                                             present=None,
                                             provider_not_called=True)
        return False
    binding = profile.bindings[target.delete_authority]
    heartbeat = _LeaseHeartbeat(
        lambda: topology_state.heartbeat_location(location.id, token,
                                                  lease_seconds),
        max(1.0, lease_seconds / 3))
    with heartbeat:
        delete_intent = False
        destructive = location.lease_kind == 'EVICT'

        def before_call() -> None:
            nonlocal delete_intent
            # Provider budgets can wait. Fence the exact lease both before and
            # after that wait. The first call then commits durable DELETE intent
            # before the SDK can send anything destructive.
            heartbeat.assert_owned()
            limiter.before_call(shard)
            heartbeat.assert_owned()
            if destructive and not delete_intent:
                if not topology_state.begin_eviction_delete(location.id, token):
                    raise worker_lease.LeaseLostError(
                        'Container image eviction lease was lost.')
                delete_intent = True
                heartbeat.assert_owned()

        heartbeat.assert_owned()
        repository = aws.EcrRepository.from_role(
            _lifecycle_role(binding, profile),
            shard.region,
            shard.repository_name,
            hooks=aws.EcrCallHooks(
                before_call=before_call,
                on_throttle=lambda: limiter.record_throttle(shard)),
            provider_fence=heartbeat.assert_owned)
        heartbeat.assert_owned()
        if not destructive:
            present = _exact_presence_with_retry(repository,
                                                 location.runtime_digest,
                                                 heartbeat)
            if present is None:
                return False
            heartbeat.assert_owned()
            completed = topology_state.complete_eviction(location.id,
                                                         token,
                                                         present=present)
            return completed is not None and not present

        request = repository.delete_request_outcome(location.runtime_digest)
        if request == aws.DeleteRequestOutcome.NOT_STARTED:
            if (delete_intent and not topology_state.cancel_eviction_delete(
                    location.id, token)):
                return False
            topology_state.complete_eviction(location.id,
                                             token,
                                             present=None,
                                             provider_not_called=True)
            return False
        if request == aws.DeleteRequestOutcome.AMBIGUOUS:
            topology_state.complete_eviction(location.id, token, present=None)
            return False
        heartbeat.assert_owned()
        if not topology_state.mark_eviction_readback(location.id, token):
            return False
        heartbeat.assert_owned()
        present = _exact_presence_with_retry(repository,
                                             location.runtime_digest, heartbeat)
        if present is None:
            return False
        heartbeat.assert_owned()
        completed = topology_state.complete_eviction(location.id,
                                                     token,
                                                     present=present)
        return completed is not None and not present


def _reconcile_publication_fanout(
        limit: int = 100,
        *,
        should_stop: Callable[[], bool] | None = None) -> int:
    return transactions.reconcile_pending_canonical_publications(
        limit, should_stop=should_stop)


def _reconcile_cluster_terminal(demand: demand_state.DemandRecord,
                                cluster_name: str,
                                *,
                                now: int | None = None) -> bool:
    """Re-proves cluster absence and observes it in one transaction."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        global_user_state.lock_container_image_cluster_lifecycle_in_session(
            session, cluster_name)
        row_exists, active_consumer = (
            global_user_state.get_cluster_image_consumer_in_session(
                session, cluster_name, for_update=True))
        legacy_name_owner = ':incarnation:' not in demand.consumer_owner
        if (row_exists and
            (legacy_name_owner or active_consumer is None or
             active_consumer == (demand.consumer_kind, demand.consumer_owner))):
            demand_state.defer_consumer_reconciliation_in_session(session,
                                                                  demand.id,
                                                                  now=now)
            return False
        return demand_state.observe_consumer_terminal_in_session(
            session, demand.id, demand.workspace, authoritative=True, now=now)


def _reconcile_terminal_consumers(
        now: int | None = None,
        limit: int = 500,
        *,
        should_stop: Callable[[], bool] | None = None) -> int:
    """Releases fences only from each consumer's authoritative lifecycle."""
    if should_stop is not None and should_stop():
        return 0
    if now is None:
        with orm.Session(catalog_state.engine()) as session:
            current = catalog_state.database_epoch(session)
    else:
        current = now
    if should_stop is not None and should_stop():
        return 0
    candidates = demand_state.list_consumer_reconciliation_candidates(
        older_than=current - _CONSUMER_RECONCILIATION_SECONDS, limit=limit)
    if should_stop is not None and should_stop():
        return 0
    cluster_names: set[str] = set()
    service_identities: list[tuple[str, int, str]] = []
    job_identities: list[tuple[int, int]] = []
    demand_cluster_identity: dict[str, str] = {}
    demand_service_identity: dict[str, tuple[str, int, str]] = {}
    demand_job_identity: dict[str, tuple[int, int]] = {}
    for demand in candidates:
        if should_stop is not None and should_stop():
            return 0
        consumer = demand.placement.get('consumer')
        if not isinstance(consumer, dict):
            continue
        if demand.consumer_kind == 'cluster':
            workload_id = consumer.get('workload_id')
            if isinstance(workload_id, str) and workload_id:
                cluster_names.add(workload_id)
                demand_cluster_identity[demand.id] = workload_id
            elif ':incarnation:' not in demand.consumer_owner:
                # Compatibility for demands created before cluster owners were
                # scoped by their durable launch incarnation.
                cluster_names.add(demand.consumer_owner)
                demand_cluster_identity[demand.id] = demand.consumer_owner
        elif demand.consumer_kind == 'service_version':
            name = consumer.get('workload_id')
            version = consumer.get('workload_task_id')
            service_hash = consumer.get('service_hash')
            if (isinstance(name, str) and name and type(version) is int and
                    version > 0 and isinstance(service_hash, str) and
                    service_hash):
                service_identity = (name, version, service_hash)
                service_identities.append(service_identity)
                demand_service_identity[demand.id] = service_identity
        elif demand.consumer_kind == 'managed_job_task':
            job_id = consumer.get('workload_id')
            task_id = consumer.get('workload_task_id')
            if (isinstance(job_id, bool) or not isinstance(job_id, (int, str))):
                continue
            try:
                parsed_job_id = int(job_id)
            except (TypeError, ValueError):
                continue
            if type(task_id) is int and task_id >= 0:
                job_identity = (parsed_job_id, task_id)
                job_identities.append(job_identity)
                demand_job_identity[demand.id] = job_identity
    if should_stop is not None and should_stop():
        return 0
    cluster_consumers = global_user_state.get_cluster_image_consumers(
        sorted(cluster_names))
    if should_stop is not None and should_stop():
        return 0
    try:
        if should_stop is not None and should_stop():
            return 0
        service_states = serve_state.get_service_version_terminal_states(
            list(set(service_identities)))
    except Exception:  # pylint: disable=broad-except
        service_states = {}
    if should_stop is not None and should_stop():
        return 0
    try:
        if should_stop is not None and should_stop():
            return 0
        job_states = managed_job_state.get_job_task_terminal_states(
            list(set(job_identities)))
    except Exception:  # pylint: disable=broad-except
        job_states = {}
    if should_stop is not None and should_stop():
        return 0
    reconciled = 0
    for demand in candidates:
        if should_stop is not None and should_stop():
            break
        authoritative_terminal = False
        if demand.consumer_kind == 'cluster':
            cluster_name = demand_cluster_identity.get(demand.id)
            if cluster_name is None:
                if should_stop is not None and should_stop():
                    break
                demand_state.defer_consumer_reconciliation(demand.id, now=now)
                continue
            legacy_name_owner = ':incarnation:' not in demand.consumer_owner
            active_consumer = cluster_consumers.get(cluster_name)
            if (cluster_name in cluster_consumers and
                (legacy_name_owner or active_consumer is None or active_consumer
                 == (demand.consumer_kind, demand.consumer_owner))):
                if should_stop is not None and should_stop():
                    break
                demand_state.defer_consumer_reconciliation(demand.id, now=now)
                continue
            if not demand.consumer_attached:
                if demand.first_terminal_observed_at is None:
                    if should_stop is not None and should_stop():
                        break
                    demand_state.defer_consumer_reconciliation(demand.id,
                                                               now=now)
                    continue
                if (current - demand.created_at
                        < _UNATTACHED_REQUEST_RETENTION_SECONDS):
                    if should_stop is not None and should_stop():
                        break
                    demand_state.defer_terminal_confirmation(demand.id, now=now)
                    continue
            if should_stop is not None and should_stop():
                break
            if _reconcile_cluster_terminal(demand, cluster_name, now=now):
                reconciled += 1
            continue
        elif demand.consumer_kind == 'service_version':
            current_service_identity = demand_service_identity.get(demand.id)
            state = (service_states.get(current_service_identity)
                     if current_service_identity is not None else None)
            if state is not True:
                if should_stop is not None and should_stop():
                    break
                demand_state.defer_consumer_reconciliation(demand.id, now=now)
                continue
            authoritative_terminal = True
        elif demand.consumer_kind == 'managed_job_task':
            current_job_identity = demand_job_identity.get(demand.id)
            state = (job_states.get(current_job_identity)
                     if current_job_identity is not None else None)
            if state is not True:
                if should_stop is not None and should_stop():
                    break
                demand_state.defer_consumer_reconciliation(demand.id, now=now)
                continue
            authoritative_terminal = True
        else:
            if should_stop is not None and should_stop():
                break
            demand_state.defer_consumer_reconciliation(demand.id, now=now)
            continue
        if (demand.first_terminal_observed_at is not None and
                current - demand.first_terminal_observed_at
                < _TERMINAL_CONFIRMATION_SECONDS):
            if should_stop is not None and should_stop():
                break
            demand_state.defer_terminal_confirmation(demand.id, now=now)
            continue
        if should_stop is not None and should_stop():
            break
        if demand_state.observe_consumer_terminal(
                demand.id,
                demand.workspace,
                authoritative=(authoritative_terminal),
                now=now):
            reconciled += 1
    return reconciled


def reconcile_qualification_lifecycle(
        limiter: budgets.ProviderBudgetLimiter,
        *,
        limit: int = 8,
        now: int | None = None,
        should_stop: Callable[[], bool] | None = None) -> bool:
    """Deletes canaries only after every declared runtime tuple proved pull."""
    if should_stop is not None and should_stop():
        return False

    mutation = qualification.get_qualification_mutation()
    revisions = topology_state.list_qualifying_profiles(include_active=True,
                                                        limit=limit)
    recovery_owner_id: str | None = None
    recovery_target: str | None = None
    if mutation is not None and mutation.get('state') == 'DELETING':
        owner_id = mutation.get('owner_profile_revision_id')
        owner_target = mutation.get('owner_target')
        if isinstance(owner_id, str) and isinstance(owner_target, str):
            recovery_owner_id = owner_id
            recovery_target = owner_target
            owner = next((item for item in revisions if item.id == owner_id),
                         None)
            if owner is None:
                owner = topology_state.get_profile_revision(owner_id)
            if owner is not None:
                revisions = [owner] + [
                    item for item in revisions if item.id != owner_id
                ]
                revisions = revisions[:limit]
    if should_stop is not None and should_stop():
        return False
    for revision in revisions:
        if should_stop is not None and should_stop():
            return False
        profile = models.ManagedRegistryProfile.from_snapshot(
            revision.config_snapshot)
        targets = (profile.canonical,) + profile.targets
        if revision.id == recovery_owner_id and recovery_target is not None:
            targets = tuple(
                sorted(targets,
                       key=lambda target: target.name != recovery_target))
        for target in targets:
            if should_stop is not None and should_stop():
                return False
            copy_key = models.profile_attestation_key('copy', target.name)
            copy_evidence = revision.attestations.get(copy_key)
            if (not isinstance(copy_evidence, dict) or
                    copy_evidence.get('status') != 'READY' or
                    not isinstance(copy_evidence.get('observed_at'), int) or
                    not isinstance(copy_evidence.get('runtime_digest'), str)):
                continue
            copy_observed_at = copy_evidence['observed_at']
            digest = models.validate_sha256_digest(
                copy_evidence['runtime_digest'], 'Qualification canary digest')
            repository_name, repository_arn = (
                qualification.qualification_repository(revision, target))
            lifecycle_key = models.profile_attestation_key(
                'lifecycle', target.name)
            lifecycle = revision.attestations.get(lifecycle_key)
            lifecycle_complete = (
                isinstance(lifecycle, dict) and
                lifecycle.get('status') == 'READY' and
                lifecycle.get('target_fingerprint') == target.target_fingerprint
                and lifecycle.get('runtime_digest')
                == copy_evidence['runtime_digest'] and
                lifecycle.get('repository_arn') == repository_arn and
                isinstance(lifecycle.get('observed_at'), int) and
                lifecycle.get('exact_absence') is True)
            if lifecycle_complete:
                if not qualification.qualification_copy_available(
                        revision, profile, target):
                    if should_stop is not None and should_stop():
                        return False
                    revision, _ = (
                        qualification.begin_qualification_lifecycle_restoration(
                            revision,
                            target,
                            repository_arn=repository_arn,
                            runtime_digest=digest,
                            now=now))
                continue
            lifecycle_deleting = (
                isinstance(lifecycle, dict) and
                lifecycle.get('status') == 'DELETING' and
                lifecycle.get('protocol_version') == 2 and
                qualification.qualification_lifecycle_proof_id(lifecycle)
                is not None and lifecycle.get('target_fingerprint')
                == target.target_fingerprint and
                lifecycle.get('repository_arn') == repository_arn and
                lifecycle.get('runtime_digest') == digest)
            if not lifecycle_deleting:
                lifecycle_epoch = (
                    qualification.qualification_copy_restoration_proof_id(
                        revision, target, digest))
                if lifecycle_epoch is None:
                    revision, _ = qualification.arm_qualification_lifecycle(
                        revision,
                        target,
                        repository_arn=repository_arn,
                        runtime_digest=digest,
                        now=now)
                    # Copy must re-verify and acknowledge the new epoch before
                    # admission or any lifecycle mutation can proceed.
                    continue
                if not qualification.qualification_copy_available(
                        revision, profile, target):
                    continue
                runtime_ready = True
                for backend, binding_id in target.runtime_pull:
                    if should_stop is not None and should_stop():
                        return False
                    binding = profile.bindings[binding_id]
                    for runtime_id in qualification.runtime_ids(
                            target, backend, binding):
                        if should_stop is not None and should_stop():
                            return False
                        runtime_key = models.profile_attestation_key(
                            'runtime', target.name, backend,
                            binding.fingerprint, runtime_id)
                        runtime = revision.attestations.get(runtime_key)
                        if (not isinstance(runtime, dict) or
                                runtime.get('status') != 'READY' or
                                runtime.get('runtime_digest') != digest or
                                not isinstance(runtime.get('observed_at'), int)
                                or runtime['observed_at'] < copy_observed_at):
                            runtime_ready = False
                            break
                    if not runtime_ready:
                        break
                if not runtime_ready:
                    continue
            if should_stop is not None and should_stop():
                return False
            shard = topology_state.get_target_shard(revision.workspace,
                                                    profile.name, target.name)
            if should_stop is not None and should_stop():
                return False
            if shard is None:
                continue
            revision, lifecycle_proof_id, mutation_lease_token = (
                qualification.begin_qualification_lifecycle_delete(
                    revision,
                    target,
                    repository_arn=repository_arn,
                    runtime_digest=digest,
                    lease_seconds=_QUALIFICATION_DELETE_LEASE_SECONDS,
                    now=now))
            if lifecycle_proof_id is None or mutation_lease_token is None:
                continue
            binding = profile.bindings[target.qualification_delete_authority]
            if should_stop is not None and should_stop():
                return False
            mutation_heartbeat = worker_lease.LeaseHeartbeat(
                functools.partial(
                    qualification.heartbeat_qualification_lifecycle_delete,
                    revision,
                    target,
                    repository_arn=repository_arn,
                    runtime_digest=digest,
                    lifecycle_proof_id=lifecycle_proof_id,
                    mutation_lease_token=mutation_lease_token,
                    lease_seconds=_QUALIFICATION_DELETE_LEASE_SECONDS,
                    now=now), max(1.0, _QUALIFICATION_DELETE_LEASE_SECONDS / 3))
            delete_provider_fence = functools.partial(
                _qualification_delete_provider_fence, should_stop,
                mutation_heartbeat,
                functools.partial(
                    qualification.qualification_lifecycle_delete_owned,
                    revision.id,
                    target,
                    repository_arn=repository_arn,
                    runtime_digest=digest,
                    lifecycle_proof_id=lifecycle_proof_id,
                    mutation_lease_token=mutation_lease_token,
                    now=now))
            try:
                with mutation_heartbeat:
                    repository = aws.EcrRepository.from_role(
                        _lifecycle_role(binding, profile),
                        target.region,
                        repository_name,
                        hooks=_ecr_hooks(limiter,
                                         shard,
                                         provider_fence=delete_provider_fence),
                        provider_fence=delete_provider_fence)
                    delete_provider_fence()
                    request_outcome = repository.delete_request_outcome(digest)
                    delete_provider_fence()
                    if request_outcome != aws.DeleteRequestOutcome.CONCLUDED:
                        raise aws.AmbiguousProviderOutcomeError(
                            'ECR qualification deletion did not conclude.')
                    try:
                        present = repository.exact_manifest_exists(digest)
                    except _QualificationDrainRequested:
                        raise
                    except Exception as error:  # pylint: disable=broad-except
                        raise aws.AmbiguousProviderOutcomeError(
                            'ECR qualification deletion has no exact final '
                            'presence proof.') from error
                    delete_provider_fence()
                    if present:
                        continue
                    completed = (
                        qualification.complete_qualification_lifecycle_delete(
                            revision,
                            target,
                            repository_arn=repository_arn,
                            runtime_digest=digest,
                            lifecycle_proof_id=lifecycle_proof_id,
                            mutation_lease_token=mutation_lease_token,
                            now=now))
                    if completed is None:
                        return False
                    revision = completed
            except (worker_lease.LeaseLostError, _QualificationDrainRequested):
                return False
        if should_stop is not None and should_stop():
            return False
        qualification.maybe_activate_profile(revision.id)
    return True


def reconcile_failed_canonical_reservations(
        limiter: budgets.ProviderBudgetLimiter,
        *,
        limit: int = 8,
        should_stop: Callable[[], bool] | None = None) -> bool:
    """Reclaims capacity only after an exact canonical-absence proof."""
    if should_stop is not None and should_stop():
        return False

    def provider_fence() -> None:
        _raise_if_stopping(should_stop)

    locations = topology_state.list_failed_canonical_reap_candidates(
        limit=limit)
    if should_stop is not None and should_stop():
        return False
    for location in locations:
        if should_stop is not None and should_stop():
            return False
        shard = topology_state.get_shard(location.shard_id)
        if should_stop is not None and should_stop():
            return False
        if (shard is None or
                shard.target_fingerprint != location.target_fingerprint):
            continue
        resolved = _profile_target_for_location(location, shard)
        if should_stop is not None and should_stop():
            return False
        if resolved is None:
            continue
        profile, target = resolved
        binding = profile.bindings[target.qualification_delete_authority]
        if should_stop is not None and should_stop():
            return False
        try:
            repository = aws.EcrRepository.from_role(
                _lifecycle_role(binding, profile),
                shard.region,
                shard.repository_name,
                hooks=_ecr_hooks(limiter, shard, provider_fence=provider_fence),
                provider_fence=provider_fence)
        except _QualificationDrainRequested:
            return False
        if should_stop is not None and should_stop():
            return False
        try:
            present = repository.exact_manifest_exists(location.runtime_digest)
            if present:
                continue
            if should_stop is not None and should_stop():
                return False
            topology_state.reap_failed_canonical_reservation(
                location.id,
                expected_updated_at=location.updated_at,
                exact_absence=True)
        except (aws.ProviderThrottledError,
                budgets.ProviderBudgetUnavailableError):
            continue
        except _QualificationDrainRequested:
            return False
        except Exception:  # pylint: disable=broad-except
            continue
    return True


class LifecycleWorkerService:
    """Runs bounded eviction, cleanup, and demand-reconciliation work."""

    def __init__(self,
                 *,
                 worker_id: str,
                 version: str,
                 max_in_flight: int,
                 retention_seconds: int,
                 lease_seconds: int = _DEFAULT_LEASE_SECONDS,
                 health: worker_health.WorkerHealth | None = None) -> None:
        self.worker_id = worker_id
        self.version = version
        self.max_in_flight = max_in_flight
        self.retention_seconds = retention_seconds
        self.lease_seconds = lease_seconds
        self._stop = threading.Event()
        self._budget_limiter = budgets.ProviderBudgetLimiter(worker_id)
        self._health = health

    def stop(self) -> None:
        self._stop.set()

    def _maintenance(self) -> None:
        if self._stop.is_set():
            return
        _reconcile_publication_fanout(should_stop=self._stop.is_set)
        if self._stop.is_set():
            return
        catalog_state.compact_terminal_records()
        if self._stop.is_set():
            return
        demand_state.compact_terminal_demands()
        if self._stop.is_set():
            return
        topology_state.compact_stale_workers(max_age_seconds=24 * 60 * 60)

    def run_forever(self) -> None:
        topology_state.register_worker(self.worker_id,
                                       models.ImageWorkerKind.LIFECYCLE,
                                       self.version, self.max_in_flight)
        if self._health is not None:
            self._health.registered()
        last_maintenance = 0.0
        last_consumer_reconciliation = 0.0
        last_qualification_reconciliation = 0.0
        last_canonical_reconciliation = 0.0
        last_policy_refresh = 0.0
        workspace_retentions: dict[str, int | None] | None = None
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_in_flight,
                thread_name_prefix='image-lifecycle') as executor:
            futures: set[concurrent.futures.Future[bool]] = set()
            qualification_future: concurrent.futures.Future[bool] | None = None
            canonical_future: concurrent.futures.Future[bool] | None = None
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
                if canonical_future is not None and canonical_future.done():
                    canonical_future = None
                schedule_now = time.monotonic()
                heartbeat_ok = topology_state.heartbeat_worker(
                    self.worker_id, in_flight=len(futures), success=bool(done))
                if self._health is not None:
                    self._health.heartbeat(heartbeat_ok)
                if self._stop.is_set():
                    break
                if schedule_now - last_maintenance >= 5 * 60:
                    if self._stop.is_set():
                        break
                    self._maintenance()
                    last_maintenance = schedule_now
                if self._stop.is_set():
                    break
                if (schedule_now - last_consumer_reconciliation
                        >= _CONSUMER_RECONCILIATION_SECONDS):
                    _reconcile_terminal_consumers(should_stop=self._stop.is_set)
                    last_consumer_reconciliation = schedule_now
                if self._stop.is_set():
                    break
                if (schedule_now - last_qualification_reconciliation
                        >= _CONSUMER_RECONCILIATION_SECONDS and
                        qualification_future is None and
                        len(futures) < self.max_in_flight):
                    if self._stop.is_set():
                        break
                    qualification_future = worker_lease.submit_if_not_stopped(
                        executor,
                        self._stop,
                        reconcile_qualification_lifecycle,
                        self._budget_limiter,
                        should_stop=self._stop.is_set)
                    if qualification_future is None:
                        break
                    futures.add(qualification_future)
                    last_qualification_reconciliation = schedule_now
                if self._stop.is_set():
                    break
                if (schedule_now - last_canonical_reconciliation
                        >= _CONSUMER_RECONCILIATION_SECONDS and
                        canonical_future is None and
                        len(futures) < self.max_in_flight):
                    if self._stop.is_set():
                        break
                    canonical_future = worker_lease.submit_if_not_stopped(
                        executor,
                        self._stop,
                        reconcile_failed_canonical_reservations,
                        self._budget_limiter,
                        should_stop=self._stop.is_set)
                    if canonical_future is None:
                        break
                    futures.add(canonical_future)
                    last_canonical_reconciliation = schedule_now
                if self._stop.is_set():
                    break
                if schedule_now - last_policy_refresh >= 60:
                    workspace_retentions = (
                        _refresh_workspace_eviction_retentions(
                            workspace_retentions))
                    last_policy_refresh = schedule_now
                if self._stop.is_set():
                    break
                if workspace_retentions is None:
                    self._stop.wait(1 if futures else 5)
                    continue
                while len(futures
                         ) < self.max_in_flight and not self._stop.is_set():
                    claim = topology_state.claim_next_eviction(
                        worker_id=self.worker_id,
                        retention_seconds=self.retention_seconds,
                        workspace_retention_seconds=workspace_retentions,
                        lease_seconds=self.lease_seconds,
                    )
                    if claim is None:
                        break
                    if self._stop.is_set():
                        break
                    submitted_future = worker_lease.submit_if_not_stopped(
                        executor,
                        self._stop,
                        evict_location,
                        claim,
                        self._budget_limiter,
                        lease_seconds=self.lease_seconds)
                    if submitted_future is None:
                        break
                    futures.add(submitted_future)
                self._stop.wait(1 if futures else 5)


def main() -> None:
    max_in_flight = int(os.environ.get('SKYPILOT_IMAGE_MAX_IN_FLIGHT', '4'))
    retention_seconds = int(
        os.environ.get('SKYPILOT_IMAGE_RETENTION_SECONDS',
                       str(8 * 7 * 24 * 60 * 60)))
    if max_in_flight <= 0 or retention_seconds <= 0:
        raise ValueError('Image worker limits must be positive.')
    # This worker reconciles global image state with Serve and managed-job
    # terminal state. Verify all three schemas before advertising liveness.
    database_migrations.initialize_central_databases()
    health = worker_health.WorkerHealth(
        'lifecycle',
        liveness_deadline_seconds=int(
            os.environ.get('SKYPILOT_IMAGE_LIVENESS_DEADLINE_SECONDS', '30')))
    health_server = worker_health.HealthServer(
        health, int(os.environ.get('SKYPILOT_IMAGE_HEALTH_PORT', '8081')))
    service = LifecycleWorkerService(
        worker_id=os.environ.get('SKYPILOT_IMAGE_WORKER_ID', str(uuid.uuid4())),
        version=os.environ.get('SKYPILOT_IMAGE_WORKER_VERSION', 'dev'),
        max_in_flight=max_in_flight,
        retention_seconds=retention_seconds,
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
