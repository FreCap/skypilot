"""Idempotent profile qualification and canary intent services."""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
import functools
import hashlib
import json
import secrets
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.container_images import aws
from sky.container_images import catalog_state
from sky.container_images import config
from sky.container_images import models
from sky.container_images import schema
from sky.container_images import topology_state
from sky.container_images import transactions

_AUTOMATIC_ACTOR_HASH = hashlib.sha256(
    b'skypilot-managed-image-qualification-scheduler').hexdigest()
_AUTOMATIC_WINDOW_SECONDS = 10 * 60
_COPY_RESTORES_LIFECYCLE_KEY = 'restores_lifecycle_proof_id'
_LIFECYCLE_PROOF_KEY = 'lifecycle_proof_id'
_LIFECYCLE_PROTOCOL_KEY = 'protocol_version'
_LIFECYCLE_PROTOCOL_VERSION = 2
_QUALIFICATION_MUTATION_ID = 'global'
_QUALIFICATION_MUTATION_DELETING = 'DELETING'
_QUALIFICATION_MUTATION_RESTORING = 'RESTORING'
_QUALIFICATION_MUTATION_QUARANTINED = 'QUARANTINED'
QUALIFICATION_DELETE_PHASE_PRE_INTENT = 'PRE_INTENT'
QUALIFICATION_DELETE_PHASE_IN_FLIGHT = 'IN_FLIGHT'
QUALIFICATION_DELETE_PHASE_READBACK = 'READBACK'
_QUALIFICATION_DELETE_PHASES = (
    QUALIFICATION_DELETE_PHASE_PRE_INTENT,
    QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
    QUALIFICATION_DELETE_PHASE_READBACK,
)


def _database_epoch(*, now: int | None = None) -> int:
    """Samples the central clock for non-mutating qualification decisions."""
    with orm.Session(catalog_state.engine()) as session:
        return catalog_state.database_epoch(session, now=now)


def _runtime_attestation_key(target: models.ManagedRegistryTarget, backend: str,
                             binding: models.RegistryAccessBinding,
                             runtime_id: str) -> str:
    return models.profile_attestation_key('runtime', target.name, backend,
                                          binding.fingerprint, runtime_id)


def runtime_ids(target: models.ManagedRegistryTarget, backend: str,
                binding: models.RegistryAccessBinding) -> tuple[str, ...]:
    if backend == 'aws_vm':
        return ((target.region,)
                if target.region in dict(binding.qualified_node_images) else ())
    if backend == 'aws_eks':
        return tuple(
            cluster.context
            for cluster in binding.qualified_clusters
            if models.eks_cluster_region(cluster.cluster_arn) == target.region)
    return ()


def _attestation_requirements(
    profile: models.ManagedRegistryProfile,
    attestations: dict[str, Any] | None = None,
) -> dict[str, int | None]:
    required: dict[str, int | None] = {'terraform': None}
    for target in (profile.canonical,) + profile.targets:
        required[models.profile_attestation_key('terraform_budget', 'aws',
                                                profile.partition,
                                                profile.registry_account,
                                                target.region, 'ecr')] = None
        required[models.profile_attestation_key('terraform_target',
                                                target.name)] = None
        required[models.profile_attestation_key(
            'infrastructure', target.name)] = _AUTOMATIC_WINDOW_SECONDS
        required[models.profile_attestation_key(
            'copy', target.name)] = _AUTOMATIC_WINDOW_SECONDS
        required[models.profile_attestation_key('lifecycle',
                                                target.name)] = None
        for backend, binding_id in target.runtime_pull:
            binding = profile.bindings[binding_id]
            for runtime_id in runtime_ids(target, backend, binding):
                required[_runtime_attestation_key(
                    target, backend, binding, runtime_id
                )] = (profile.qualification.runtime_attestation_max_age_seconds)
    for key, evidence in (attestations or {}).items():
        if not key.startswith('terraform_shard:'):
            continue
        live_key = (evidence.get('live_attestation_key') if isinstance(
            evidence, dict) else None)
        if not isinstance(live_key, str) or not live_key:
            raise ValueError('Terraform shard attestation is invalid.')
        required[live_key] = _AUTOMATIC_WINDOW_SECONDS
    return required


def _fresh(evidence: Any, *, now: int, max_age_seconds: int) -> bool:
    return (isinstance(evidence, dict) and evidence.get('status') == 'READY' and
            isinstance(evidence.get('observed_at'), int) and
            0 <= now - evidence['observed_at'] <= max_age_seconds)


def qualification_lifecycle_proof_id(evidence: Any) -> str | None:
    """Returns one canonical opaque lifecycle proof ID, if present."""
    if not isinstance(evidence, dict):
        return None
    proof_id = evidence.get(_LIFECYCLE_PROOF_KEY)
    if not isinstance(proof_id, str):
        return None
    try:
        parsed = uuid.UUID(proof_id)
    except ValueError:
        return None
    return proof_id if str(parsed) == proof_id else None


def qualification_copy_available(revision: topology_state.ProfileRevisionRecord,
                                 profile: models.ManagedRegistryProfile,
                                 target: models.ManagedRegistryTarget) -> bool:
    """Returns whether the fixed qualification digest is currently present."""
    return models.qualification_copy_proof_matches(revision.attestations,
                                                   profile, target)


def qualification_copy_restoration_proof_id(
        revision: topology_state.ProfileRevisionRecord,
        target: models.ManagedRegistryTarget,
        runtime_digest: str) -> str | None:
    """Returns the exact lifecycle epoch eligible for copy acknowledgement."""
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    lifecycle = revision.attestations.get(lifecycle_key)
    if (not isinstance(lifecycle, dict) or
            lifecycle.get('target_fingerprint') != target.target_fingerprint or
            lifecycle.get('runtime_digest') != runtime_digest or
            lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
            != _LIFECYCLE_PROTOCOL_VERSION or
            not isinstance(lifecycle.get('observed_at'), int)):
        return None
    status = lifecycle.get('status')
    if status == 'ARMED':
        if lifecycle.get('exact_absence') is not None:
            return None
    elif status == 'READY':
        if lifecycle.get('exact_absence') is not True:
            return None
    else:
        return None
    return qualification_lifecycle_proof_id(lifecycle)


def qualification_copy_restoration_evidence(
        lifecycle_proof_id: str | None) -> dict[str, str]:
    """Acknowledges the exact same-digest lifecycle proof a copy restores."""
    if lifecycle_proof_id is None:
        return {}
    return {_COPY_RESTORES_LIFECYCLE_KEY: lifecycle_proof_id}


def qualification_lifecycle_evidence(
        *,
        status: str,
        target: models.ManagedRegistryTarget,
        repository_arn: str,
        runtime_digest: str,
        lifecycle_proof_id: str,
        delete_phase: str | None = None,
        mutation_lease_token: str | None = None,
        mutation_lease_expires_at: int | None = None,
        exact_absence: bool = False,
        quarantine_reason: str | None = None) -> dict[str, Any]:
    """Builds one protocol-versioned lifecycle mutation attestation."""
    if status not in ('ARMED', 'DELETING', 'READY', 'QUARANTINED'):
        raise ValueError('Lifecycle status is invalid.')
    lease_present = mutation_lease_token is not None
    if ((mutation_lease_token is None) != (mutation_lease_expires_at is None)):
        raise ValueError('Lifecycle mutation lease is incomplete.')
    deleting = status == 'DELETING'
    quarantined = status == 'QUARANTINED'
    if (deleting != lease_present or deleting != (delete_phase is not None) or
        (delete_phase is not None and
         delete_phase not in _QUALIFICATION_DELETE_PHASES) or
        (status == 'READY') != exact_absence or
            quarantined != (quarantine_reason is not None) or
        (quarantine_reason is not None and not quarantine_reason)):
        raise ValueError('Lifecycle state evidence is inconsistent.')
    evidence: dict[str, Any] = {
        'status': status,
        'target': target.name,
        'target_fingerprint': target.target_fingerprint,
        'repository_arn': repository_arn,
        'runtime_digest': runtime_digest,
        _LIFECYCLE_PROOF_KEY: lifecycle_proof_id,
        _LIFECYCLE_PROTOCOL_KEY: _LIFECYCLE_PROTOCOL_VERSION,
    }
    if mutation_lease_token is not None:
        evidence['delete_phase'] = delete_phase
        evidence['mutation_lease_token'] = mutation_lease_token
        evidence['mutation_lease_expires_at'] = mutation_lease_expires_at
    if exact_absence:
        evidence['exact_absence'] = True
    if quarantine_reason is not None:
        evidence['quarantine_reason'] = quarantine_reason
    return evidence


def _qualification_mutation_matches(
    mutation: Mapping[str, Any] | None,
    *,
    state: str,
    revision_id: str,
    target: models.ManagedRegistryTarget,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    delete_phase: str | None,
    mutation_lease_token: str | None,
) -> bool:
    if mutation is None:
        return False
    return bool(
        mutation['id'] == _QUALIFICATION_MUTATION_ID and
        mutation['state'] == state and
        mutation['owner_profile_revision_id'] == revision_id and
        mutation['owner_target'] == target.name and
        mutation['owner_target_fingerprint'] == target.target_fingerprint and
        mutation['repository_arn'] == repository_arn and
        mutation['runtime_digest'] == runtime_digest and
        mutation['lifecycle_proof_id'] == lifecycle_proof_id and
        mutation['delete_phase'] == delete_phase and
        mutation['mutation_lease_token'] == mutation_lease_token)


def get_qualification_mutation() -> dict[str, Any] | None:
    """Returns a diagnostic snapshot of the catalog-wide mutation barrier."""
    with orm.Session(catalog_state.engine()) as session:
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=False)
        return dict(mutation) if mutation is not None else None


def _record_qualification_repository_quarantine_in_session(
    session: orm.Session,
    *,
    revision_id: str,
    target: models.ManagedRegistryTarget,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    reason: str,
    now: int,
) -> None:
    """Persists the physical tombstone before the global barrier can clear."""
    session.execute(
        postgresql.insert(schema.qualification_repository_quarantines).values(
            repository_arn=repository_arn,
            owner_profile_revision_id=revision_id,
            owner_target=target.name,
            owner_target_fingerprint=target.target_fingerprint,
            runtime_digest=runtime_digest,
            lifecycle_proof_id=lifecycle_proof_id,
            quarantine_reason=reason,
            quarantined_at=now).on_conflict_do_nothing(index_elements=[
                schema.qualification_repository_quarantines.c.repository_arn
            ]))
    if not topology_state.qualification_repository_quarantined_in_session(
            session, repository_arn):
        raise RuntimeError(
            'Qualification repository quarantine tombstone was not recorded.')


def _revision_owns_qualification_work_in_session(
        session: orm.Session,
        revision: topology_state.ProfileRevisionRecord) -> bool:
    return topology_state.qualification_revision_owns_work_in_session(
        session,
        profile_revision_id=revision.id,
        workspace=revision.workspace,
        profile=revision.profile,
        state=revision.state)


def _qualification_repository_available_in_session(session: orm.Session,
                                                   repository_arn: str) -> bool:
    return not topology_state.qualification_repository_quarantined_in_session(
        session, repository_arn)


def _qualification_copy_requestable(
        revision: topology_state.ProfileRevisionRecord,
        profile: models.ManagedRegistryProfile,
        target: models.ManagedRegistryTarget) -> bool:
    """Allows intent queueing while this exact revision owns restoration."""
    if qualification_copy_available(revision, profile, target):
        return True
    copy_key = models.profile_attestation_key('copy', target.name)
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    copy_evidence = revision.attestations.get(copy_key)
    lifecycle = revision.attestations.get(lifecycle_key)
    if (not isinstance(copy_evidence, dict) or
            copy_evidence.get('status') != 'READY' or
            copy_evidence.get('target_fingerprint') != target.target_fingerprint
            or copy_evidence.get('platform')
            != profile.qualification.canary_platform or
            not isinstance(copy_evidence.get('repository_arn'), str) or
            not isinstance(copy_evidence.get('runtime_digest'), str) or
            not isinstance(copy_evidence.get('observed_at'), int) or
            not isinstance(lifecycle, dict) or
            lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
            != _LIFECYCLE_PROTOCOL_VERSION or
            lifecycle.get('target_fingerprint') != target.target_fingerprint or
            lifecycle.get('repository_arn') != copy_evidence['repository_arn']
            or
            lifecycle.get('runtime_digest') != copy_evidence['runtime_digest']):
        return False
    proof_id = qualification_lifecycle_proof_id(lifecycle)
    mutation = get_qualification_mutation()
    if proof_id is None or mutation is None:
        return False
    state = mutation.get('state')
    if state == _QUALIFICATION_MUTATION_DELETING:
        token = lifecycle.get('mutation_lease_token')
        delete_phase = lifecycle.get('delete_phase')
        if (lifecycle.get('status') != 'DELETING' or
                delete_phase not in _QUALIFICATION_DELETE_PHASES or
                not isinstance(token, str)):
            return False
        mutation_token: str | None = token
    elif state == _QUALIFICATION_MUTATION_RESTORING:
        if (lifecycle.get('status') != 'READY' or
                lifecycle.get('exact_absence') is not True):
            return False
        delete_phase = None
        mutation_token = None
    else:
        return False
    return _qualification_mutation_matches(
        mutation,
        state=str(state),
        revision_id=revision.id,
        target=target,
        repository_arn=copy_evidence['repository_arn'],
        runtime_digest=copy_evidence['runtime_digest'],
        lifecycle_proof_id=proof_id,
        delete_phase=delete_phase,
        mutation_lease_token=mutation_token)


def qualification_copy_barrier_snapshot(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
) -> tuple[bool, str | None]:
    """Snapshots whether copy may run and any restoration proof it must clear."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current = topology_state._profile(  # pylint: disable=protected-access
            row)
        if (current.state not in (models.ImageProfileState.QUALIFYING,
                                  models.ImageProfileState.ACTIVE) or
                current.desired_generation != revision.desired_generation or
                current.config_hash != revision.config_hash or
                not _revision_owns_qualification_work_in_session(
                    session, current)):
            return False, None
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=False)
        if not _qualification_repository_available_in_session(
                session, repository_arn):
            return False, None
        if mutation is None:
            return True, None
        proof_id = mutation['lifecycle_proof_id']
        if (isinstance(proof_id, str) and _qualification_mutation_matches(
                mutation,
                state=_QUALIFICATION_MUTATION_RESTORING,
                revision_id=revision.id,
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=proof_id,
                delete_phase=None,
                mutation_lease_token=None)):
            return True, proof_id
        return False, None


def qualification_copy_provider_allowed(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
) -> bool:
    """Fences every qualification-copy provider call at its admission point."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current = topology_state._profile(  # pylint: disable=protected-access
            row)
        if (current.state not in (models.ImageProfileState.QUALIFYING,
                                  models.ImageProfileState.ACTIVE) or
                current.desired_generation != revision.desired_generation or
                current.config_hash != revision.config_hash or
                not _revision_owns_qualification_work_in_session(
                    session, current)):
            return False
        terraform_target = current.attestations.get(
            models.profile_attestation_key('terraform_target', target.name))
        if (not isinstance(terraform_target, dict) or
                terraform_target.get('status') != 'READY' or
                terraform_target.get('target_fingerprint')
                != target.target_fingerprint or
                terraform_target.get('repository_arn') != repository_arn):
            return False
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=False)
        if not _qualification_repository_available_in_session(
                session, repository_arn):
            return False
        if mutation is None:
            return True
        return bool(mutation['state'] == _QUALIFICATION_MUTATION_RESTORING and
                    mutation['owner_profile_revision_id'] == current.id and
                    mutation['owner_target'] == target.name and
                    mutation['owner_target_fingerprint']
                    == target.target_fingerprint and
                    mutation['repository_arn'] == repository_arn)


def arm_qualification_lifecycle(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    now: int | None = None,
) -> tuple[topology_state.ProfileRevisionRecord, bool]:
    """Creates the protocol epoch that an initial copy must acknowledge."""
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current = topology_state._profile(  # pylint: disable=protected-access
            row)
        if (current.state not in (models.ImageProfileState.QUALIFYING,
                                  models.ImageProfileState.ACTIVE) or
                current.desired_generation != revision.desired_generation or
                current.config_hash != revision.config_hash or
                not _revision_owns_qualification_work_in_session(
                    session, current)):
            raise topology_state.StaleProfileRevisionError(
                'Lifecycle epoch no longer matches the desired revision.')
        topology_state.lock_qualification_mutation_in_session(session,
                                                              exclusive=False)
        if not _qualification_repository_available_in_session(
                session, repository_arn):
            raise topology_state.StaleProfileRevisionError(
                'Lifecycle epoch repository is quarantined.')
        lifecycle = current.attestations.get(lifecycle_key)
        if isinstance(lifecycle, dict):
            same_identity = (lifecycle.get('target_fingerprint')
                             == target.target_fingerprint and
                             lifecycle.get('repository_arn') == repository_arn
                             and
                             lifecycle.get('runtime_digest') == runtime_digest)
            valid_epoch = (
                same_identity and
                (qualification_copy_restoration_proof_id(
                    current, target, runtime_digest) is not None or
                 (lifecycle.get('status') == 'DELETING' and
                  lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
                  == _LIFECYCLE_PROTOCOL_VERSION and
                  qualification_lifecycle_proof_id(lifecycle) is not None)))
            if valid_epoch or lifecycle.get('status') == 'DELETING':
                return current, False
        topology_state.assert_qualification_mutation_idle_in_session(session)
        updated = topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='ARMED',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=str(uuid.uuid4())),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=now)
        return updated, True


def begin_qualification_lifecycle_restoration(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    now: int | None = None,
) -> tuple[topology_state.ProfileRevisionRecord, str | None]:
    """Adopts exact legacy absence into the durable restoration barrier."""
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current = topology_state._profile(  # pylint: disable=protected-access
            row)
        if (current.state not in (models.ImageProfileState.QUALIFYING,
                                  models.ImageProfileState.ACTIVE) or
                current.desired_generation != revision.desired_generation or
                current.config_hash != revision.config_hash or
                not _revision_owns_qualification_work_in_session(
                    session, current)):
            raise topology_state.StaleProfileRevisionError(
                'Lifecycle restoration no longer matches the desired revision.')
        lifecycle = current.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        if not _qualification_repository_available_in_session(
                session, repository_arn):
            raise topology_state.StaleProfileRevisionError(
                'Lifecycle restoration repository is quarantined.')
        current_time = catalog_state.database_epoch(session, now=now)
        if (not isinstance(lifecycle, dict) or
                lifecycle.get('status') != 'READY' or
                lifecycle.get('target_fingerprint') != target.target_fingerprint
                or lifecycle.get('repository_arn') != repository_arn or
                lifecycle.get('runtime_digest') != runtime_digest or
                lifecycle.get('exact_absence') is not True):
            return current, None
        profile = models.ManagedRegistryProfile.from_snapshot(
            current.config_snapshot)
        if qualification_copy_available(current, profile, target):
            return current, None

        proof_id = qualification_lifecycle_proof_id(lifecycle)
        upgrade_legacy = proof_id is None
        if upgrade_legacy:
            # Generation zero may name a pre-existing repository whose prior
            # contents and deletion history are not fenced by this protocol.
            # A positive generation is the explicit fresh-repository boundary
            # that makes exact legacy absence safe to adopt without deleting
            # again. Protocol-2 evidence remains self-authenticating below.
            if target.qualification_repository_generation <= 0:
                return current, None
            # Reject malformed partial protocol-2 evidence. Only the exact
            # pre-protocol shape may be adopted without repeating deletion.
            if (lifecycle.get(_LIFECYCLE_PROTOCOL_KEY) is not None or
                    lifecycle.get(_LIFECYCLE_PROOF_KEY) is not None):
                return current, None
            proof_id = str(uuid.uuid4())
        elif (lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
              != _LIFECYCLE_PROTOCOL_VERSION):
            return current, None
        assert proof_id is not None

        if mutation is not None:
            if _qualification_mutation_matches(
                    mutation,
                    state=_QUALIFICATION_MUTATION_RESTORING,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=proof_id,
                    delete_phase=None,
                    mutation_lease_token=None):
                return current, proof_id
            return current, None

        session.execute(schema.qualification_mutation.insert().values(
            id=_QUALIFICATION_MUTATION_ID,
            owner_profile_revision_id=revision.id,
            owner_target=target.name,
            owner_target_fingerprint=target.target_fingerprint,
            repository_arn=repository_arn,
            runtime_digest=runtime_digest,
            lifecycle_proof_id=proof_id,
            state=_QUALIFICATION_MUTATION_RESTORING,
            delete_phase=None,
            mutation_lease_token=None,
            mutation_lease_expires_at=None,
            quarantine_reason=None,
            updated_at=current_time))
        if not upgrade_legacy:
            return current, proof_id
        updated = topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='READY',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=proof_id,
                exact_absence=True),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=current_time)
        return updated, proof_id


def qualification_repository(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    catalog_authority: str | None = None,
    profile: models.ManagedRegistryProfile | None = None,
    configured_target: models.ManagedRegistryTarget | None = None,
) -> tuple[str, str]:
    """Returns the Terraform-attested non-catalog repository identity."""
    if profile is None:
        profile = models.ManagedRegistryProfile.from_snapshot(
            revision.config_snapshot)
    if configured_target is None:
        try:
            configured_target = profile.target(target.name)
        except ValueError:
            raise ValueError('QUALIFICATION_FAILED') from None
    elif configured_target.name != target.name:
        raise ValueError('QUALIFICATION_FAILED')
    if configured_target.target_fingerprint != target.target_fingerprint:
        raise ValueError('QUALIFICATION_FAILED')
    if catalog_authority is None:
        catalog_authority = catalog_state.get_catalog_authority_id()
    if catalog_authority is None:
        raise ValueError('QUALIFICATION_FAILED')
    expected_repository_name = aws.qualification_repository_name(
        catalog_authority, configured_target)
    expected_repository_arn = (
        f'arn:{profile.partition}:ecr:{configured_target.region}:'
        f'{profile.registry_account}:repository/{expected_repository_name}')
    key = models.profile_attestation_key('terraform_target', target.name)
    evidence = revision.attestations.get(key)
    evidence_generation = (evidence.get('qualification_repository_generation',
                                        0) if isinstance(evidence, dict) else 0)
    if (not isinstance(evidence, dict) or evidence.get('status') != 'READY' or
            evidence.get('target_fingerprint') != target.target_fingerprint or
            evidence.get('registry') != target.registry or
            not isinstance(evidence_generation, int) or
            isinstance(evidence_generation, bool) or
            evidence_generation != target.qualification_repository_generation or
            evidence.get('repository_name') != expected_repository_name or
            evidence.get('repository_arn') != expected_repository_arn):
        raise ValueError('QUALIFICATION_FAILED')
    return expected_repository_name, expected_repository_arn


def _running_canary_exists_in_session(session: orm.Session) -> bool:
    """Returns whether any catalog canary still owns provider cleanup."""
    return bool(
        session.execute(
            sqlalchemy.select(sqlalchemy.exists().where(
                schema.operations.c.kind == 'PROFILE_CANARY',
                schema.operations.c.state ==
                models.ImageOperationState.RUNNING.value))).scalar())


def begin_qualification_lifecycle_delete(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lease_seconds: int,
    now: int | None = None,
) -> tuple[topology_state.ProfileRevisionRecord, str | None, str | None]:
    """Claims fresh, pre-intent, or concluded-readback lifecycle work.

    Expired pre-intent work is safe to reclaim because no provider delete could
    begin without a later durable phase transition. Expired readback work is
    also reclaimable, but its phase tells the caller that it may only read.
    Expired in-flight work is quarantined because an older request may still
    arrive after any successor read.
    """
    if lease_seconds <= 0:
        raise ValueError('Lifecycle mutation lease must be positive.')
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current = topology_state._profile(  # pylint: disable=protected-access
            row)
        if (current.state not in (models.ImageProfileState.QUALIFYING,
                                  models.ImageProfileState.ACTIVE) or
                current.desired_generation != revision.desired_generation or
                current.config_hash != revision.config_hash or
                not _revision_owns_qualification_work_in_session(
                    session, current)):
            raise topology_state.StaleProfileRevisionError(
                'Lifecycle delete no longer matches the desired revision.')
        lifecycle = current.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        if not _qualification_repository_available_in_session(
                session, repository_arn):
            raise topology_state.StaleProfileRevisionError(
                'Lifecycle delete repository is quarantined.')
        current_time = catalog_state.database_epoch(session, now=now)
        proof_id: str | None = None
        prior_token: str | None = None
        delete_phase = QUALIFICATION_DELETE_PHASE_PRE_INTENT
        takeover = False
        if (isinstance(lifecycle, dict) and lifecycle.get('target_fingerprint')
                == target.target_fingerprint and
                lifecycle.get('repository_arn') == repository_arn and
                lifecycle.get('runtime_digest') == runtime_digest):
            if (lifecycle.get('status') == 'READY' and
                    lifecycle.get('exact_absence') is True):
                return current, None, None
            status = lifecycle.get('status')
            proof_id = qualification_lifecycle_proof_id(lifecycle)
            if (status == 'DELETING' and lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
                    == _LIFECYCLE_PROTOCOL_VERSION and proof_id is not None and
                    lifecycle.get('delete_phase')
                    in _QUALIFICATION_DELETE_PHASES and
                    isinstance(lifecycle.get('mutation_lease_token'), str) and
                    isinstance(lifecycle.get('mutation_lease_expires_at'),
                               int)):
                if lifecycle['mutation_lease_expires_at'] > current_time:
                    return current, None, None
                prior_token = lifecycle['mutation_lease_token']
                delete_phase = str(lifecycle['delete_phase'])
                takeover = True
            elif (status == 'ARMED' and proof_id is not None and
                  qualification_copy_restoration_proof_id(
                      current, target, runtime_digest) == proof_id):
                # Deletion is a new epoch. Reusing the acknowledged ARMED epoch
                # would make the pre-delete copy look available again.
                proof_id = str(uuid.uuid4())
            else:
                return current, None, None
        else:
            # A protocol-2 worker must arm the target before deletion. Unknown
            # or legacy state is never interpreted as mutation authority.
            return current, None, None
        assert proof_id is not None
        mutation_expires_at = (mutation['mutation_lease_expires_at']
                               if mutation is not None else None)
        if takeover:
            if (prior_token is None or not _qualification_mutation_matches(
                    mutation,
                    state=_QUALIFICATION_MUTATION_DELETING,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=proof_id,
                    delete_phase=delete_phase,
                    mutation_lease_token=prior_token) or
                    not isinstance(mutation_expires_at, int) or
                    mutation_expires_at > current_time):
                return current, None, None
            if delete_phase == QUALIFICATION_DELETE_PHASE_IN_FLIGHT:
                reason = 'PROVIDER_OUTCOME_AMBIGUOUS'
                changed = session.execute(
                    schema.qualification_mutation.update().where(
                        schema.qualification_mutation.c.id ==
                        _QUALIFICATION_MUTATION_ID,
                        schema.qualification_mutation.c.state ==
                        _QUALIFICATION_MUTATION_DELETING,
                        schema.qualification_mutation.c.delete_phase ==
                        QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
                        schema.qualification_mutation.c.lifecycle_proof_id ==
                        proof_id,
                        schema.qualification_mutation.c.mutation_lease_token ==
                        prior_token).values(
                            state=_QUALIFICATION_MUTATION_QUARANTINED,
                            delete_phase=None,
                            mutation_lease_token=None,
                            mutation_lease_expires_at=None,
                            quarantine_reason=reason,
                            updated_at=current_time)).rowcount
                if changed != 1:
                    return current, None, None
                _record_qualification_repository_quarantine_in_session(
                    session,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=proof_id,
                    reason=reason,
                    now=current_time)
                quarantined = (
                    topology_state.record_profile_attestation_in_session(
                        session,
                        profile_revision_id=revision.id,
                        kind=lifecycle_key,
                        evidence=qualification_lifecycle_evidence(
                            status='QUARANTINED',
                            target=target,
                            repository_arn=repository_arn,
                            runtime_digest=runtime_digest,
                            lifecycle_proof_id=proof_id,
                            quarantine_reason=reason),
                        expected_generation=revision.desired_generation,
                        expected_config_hash=revision.config_hash,
                        now=current_time))
                return quarantined, None, None
        else:
            if mutation is not None or _running_canary_exists_in_session(
                    session):
                return current, None, None
        lease_token = str(uuid.uuid4())
        mutation_values = {
            'state': _QUALIFICATION_MUTATION_DELETING,
            'owner_profile_revision_id': revision.id,
            'owner_target': target.name,
            'owner_target_fingerprint': target.target_fingerprint,
            'repository_arn': repository_arn,
            'runtime_digest': runtime_digest,
            'lifecycle_proof_id': proof_id,
            'delete_phase': delete_phase,
            'mutation_lease_token': lease_token,
            'mutation_lease_expires_at': current_time + lease_seconds,
            'quarantine_reason': None,
            'updated_at': current_time,
        }
        if takeover:
            changed = session.execute(schema.qualification_mutation.update(
            ).where(
                schema.qualification_mutation.c.id ==
                _QUALIFICATION_MUTATION_ID,
                schema.qualification_mutation.c.state ==
                _QUALIFICATION_MUTATION_DELETING,
                schema.qualification_mutation.c.lifecycle_proof_id == proof_id,
                schema.qualification_mutation.c.mutation_lease_token ==
                prior_token).values(**mutation_values)).rowcount
            if changed != 1:
                return current, None, None
        else:
            session.execute(schema.qualification_mutation.insert().values(
                id=_QUALIFICATION_MUTATION_ID, **mutation_values))
        updated = topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='DELETING',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=proof_id,
                delete_phase=delete_phase,
                mutation_lease_token=lease_token,
                mutation_lease_expires_at=current_time + lease_seconds),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=now)
        return updated, proof_id, lease_token


def qualification_lifecycle_delete_owned(revision_id: str,
                                         target: models.ManagedRegistryTarget,
                                         *,
                                         repository_arn: str,
                                         runtime_digest: str,
                                         lifecycle_proof_id: str,
                                         mutation_lease_token: str,
                                         expected_delete_phase: str |
                                         None = None,
                                         now: int | None = None) -> bool:
    """Returns whether one worker still owns the destructive provider fence."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        try:
            row = topology_state.lock_profile_revision_mutation_in_session(
                session, revision_id)
        except topology_state.StaleProfileRevisionError:
            return False
        revision = topology_state._profile(  # pylint: disable=protected-access
            row)
        lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
        lifecycle = revision.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        current = catalog_state.database_epoch(session, now=now)
        mutation_expires_at = (mutation['mutation_lease_expires_at']
                               if mutation is not None else None)
        delete_phase = (lifecycle.get('delete_phase') if isinstance(
            lifecycle, dict) else None)
        return bool(
            revision.state in (models.ImageProfileState.QUALIFYING,
                               models.ImageProfileState.ACTIVE) and
            isinstance(lifecycle, dict) and
            lifecycle.get('status') == 'DELETING' and
            lifecycle.get('target_fingerprint') == target.target_fingerprint and
            lifecycle.get('repository_arn') == repository_arn and
            lifecycle.get('runtime_digest') == runtime_digest and
            lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
            == _LIFECYCLE_PROTOCOL_VERSION and
            delete_phase in _QUALIFICATION_DELETE_PHASES and
            (expected_delete_phase is None or
             delete_phase == expected_delete_phase) and
            qualification_lifecycle_proof_id(lifecycle) == lifecycle_proof_id
            and
            lifecycle.get('mutation_lease_token') == mutation_lease_token and
            isinstance(lifecycle.get('mutation_lease_expires_at'), int) and
            lifecycle['mutation_lease_expires_at'] > current and
            _qualification_mutation_matches(
                mutation,
                state=_QUALIFICATION_MUTATION_DELETING,
                revision_id=revision_id,
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=lifecycle_proof_id,
                delete_phase=str(delete_phase),
                mutation_lease_token=mutation_lease_token) and
            isinstance(mutation_expires_at, int) and
            mutation_expires_at > current)


def heartbeat_qualification_lifecycle_delete(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    lease_seconds: int,
    expected_delete_phase: str | None = None,
    now: int | None = None,
) -> bool:
    """Renews one exact lifecycle mutation lease under the profile lock."""
    if lease_seconds <= 0:
        raise ValueError('Lifecycle mutation lease must be positive.')
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current_revision = topology_state._profile(  # pylint: disable=protected-access
            row)
        lifecycle = current_revision.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        current = catalog_state.database_epoch(session, now=now)
        mutation_expires_at = (mutation['mutation_lease_expires_at']
                               if mutation is not None else None)
        delete_phase = (lifecycle.get('delete_phase') if isinstance(
            lifecycle, dict) else None)
        if (current_revision.state not in (models.ImageProfileState.QUALIFYING,
                                           models.ImageProfileState.ACTIVE) or
                current_revision.desired_generation
                != revision.desired_generation or
                current_revision.config_hash != revision.config_hash or
                not isinstance(lifecycle, dict) or
                lifecycle.get('status') != 'DELETING' or
                lifecycle.get('target_fingerprint') != target.target_fingerprint
                or lifecycle.get('repository_arn') != repository_arn or
                lifecycle.get('runtime_digest') != runtime_digest or
                lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
                != _LIFECYCLE_PROTOCOL_VERSION or
                delete_phase not in _QUALIFICATION_DELETE_PHASES or
            (expected_delete_phase is not None and
             delete_phase != expected_delete_phase) or
                qualification_lifecycle_proof_id(lifecycle)
                != lifecycle_proof_id or
                lifecycle.get('mutation_lease_token') != mutation_lease_token or
                not isinstance(lifecycle.get('mutation_lease_expires_at'), int)
                or lifecycle['mutation_lease_expires_at'] <= current or
                not _qualification_mutation_matches(
                    mutation,
                    state=_QUALIFICATION_MUTATION_DELETING,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=lifecycle_proof_id,
                    delete_phase=str(delete_phase),
                    mutation_lease_token=mutation_lease_token) or
                not isinstance(mutation_expires_at, int) or
                mutation_expires_at <= current):
            return False
        changed = session.execute(schema.qualification_mutation.update().where(
            schema.qualification_mutation.c.id == _QUALIFICATION_MUTATION_ID,
            schema.qualification_mutation.c.state ==
            _QUALIFICATION_MUTATION_DELETING,
            schema.qualification_mutation.c.delete_phase == delete_phase,
            schema.qualification_mutation.c.lifecycle_proof_id ==
            lifecycle_proof_id,
            schema.qualification_mutation.c.mutation_lease_token ==
            mutation_lease_token).values(mutation_lease_expires_at=current +
                                         lease_seconds,
                                         updated_at=current)).rowcount
        if changed != 1:
            return False
        topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='DELETING',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=lifecycle_proof_id,
                delete_phase=str(delete_phase),
                mutation_lease_token=mutation_lease_token,
                mutation_lease_expires_at=current + lease_seconds),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=now)
        return True


def _defer_qualification_lifecycle_delete(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    retry_seconds: int,
    allowed_delete_phases: tuple[str, ...],
    require_live_lease: bool,
    now: int | None = None,
) -> bool:
    """Normalizes a provably safe claim into a short pre-intent retry lease.

    Token rotation fences a heartbeat that passed its process-local stop check
    before this transaction. It therefore cannot race the short retry back to
    the ordinary five-minute failure lease.
    """
    if retry_seconds <= 0:
        raise ValueError('Lifecycle retry delay must be positive.')
    if (not allowed_delete_phases or
            any(phase not in _QUALIFICATION_DELETE_PHASES
                for phase in allowed_delete_phases)):
        raise ValueError('Lifecycle retry phases are invalid.')
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current_revision = topology_state._profile(  # pylint: disable=protected-access
            row)
        lifecycle = current_revision.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        current = catalog_state.database_epoch(session, now=now)
        lifecycle_expires_at = (lifecycle.get('mutation_lease_expires_at')
                                if isinstance(lifecycle, dict) else None)
        delete_phase = (lifecycle.get('delete_phase') if isinstance(
            lifecycle, dict) else None)
        mutation_expires_at = (mutation['mutation_lease_expires_at']
                               if mutation is not None else None)
        if (current_revision.state not in (models.ImageProfileState.QUALIFYING,
                                           models.ImageProfileState.ACTIVE) or
                current_revision.desired_generation
                != revision.desired_generation or
                current_revision.config_hash != revision.config_hash or
                not isinstance(lifecycle, dict) or
                lifecycle.get('status') != 'DELETING' or
                delete_phase not in allowed_delete_phases or
                lifecycle.get('target_fingerprint') != target.target_fingerprint
                or lifecycle.get('repository_arn') != repository_arn or
                lifecycle.get('runtime_digest') != runtime_digest or
                lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
                != _LIFECYCLE_PROTOCOL_VERSION or
                qualification_lifecycle_proof_id(lifecycle)
                != lifecycle_proof_id or
                lifecycle.get('mutation_lease_token') != mutation_lease_token or
                not isinstance(lifecycle_expires_at, int) or
            (require_live_lease and lifecycle_expires_at <= current) or
                not _qualification_mutation_matches(
                    mutation,
                    state=_QUALIFICATION_MUTATION_DELETING,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=lifecycle_proof_id,
                    delete_phase=str(delete_phase),
                    mutation_lease_token=mutation_lease_token) or
                not isinstance(mutation_expires_at, int) or
            (require_live_lease and mutation_expires_at <= current)):
            return False
        deferred_token = str(uuid.uuid4())
        deferred_expiry = current + retry_seconds
        changed = session.execute(schema.qualification_mutation.update().where(
            schema.qualification_mutation.c.id == _QUALIFICATION_MUTATION_ID,
            schema.qualification_mutation.c.state ==
            _QUALIFICATION_MUTATION_DELETING,
            schema.qualification_mutation.c.delete_phase.in_(
                allowed_delete_phases),
            schema.qualification_mutation.c.lifecycle_proof_id ==
            lifecycle_proof_id,
            schema.qualification_mutation.c.mutation_lease_token ==
            mutation_lease_token).values(
                delete_phase=QUALIFICATION_DELETE_PHASE_PRE_INTENT,
                mutation_lease_token=deferred_token,
                mutation_lease_expires_at=deferred_expiry,
                updated_at=current)).rowcount
        if changed != 1:
            return False
        topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='DELETING',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=lifecycle_proof_id,
                delete_phase=QUALIFICATION_DELETE_PHASE_PRE_INTENT,
                mutation_lease_token=deferred_token,
                mutation_lease_expires_at=deferred_expiry),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=current)
        return True


def defer_qualification_lifecycle_delete(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    retry_seconds: int,
    now: int | None = None,
) -> bool:
    """Rotates a safe pre-intent claim into a short retry lease."""
    return _defer_qualification_lifecycle_delete(
        revision,
        target,
        repository_arn=repository_arn,
        runtime_digest=runtime_digest,
        lifecycle_proof_id=lifecycle_proof_id,
        mutation_lease_token=mutation_lease_token,
        retry_seconds=retry_seconds,
        allowed_delete_phases=(QUALIFICATION_DELETE_PHASE_PRE_INTENT,),
        require_live_lease=True,
        now=now)


def defer_qualification_lifecycle_delete_not_started(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    retry_seconds: int,
    now: int | None = None,
) -> bool:
    """Defers a delete that the provider adapter proved never started.

    The intent transition may have committed even when its response was lost,
    so this CAS accepts either durable phase and always rotates back to a fresh
    pre-intent token.
    """
    return _defer_qualification_lifecycle_delete(
        revision,
        target,
        repository_arn=repository_arn,
        runtime_digest=runtime_digest,
        lifecycle_proof_id=lifecycle_proof_id,
        mutation_lease_token=mutation_lease_token,
        retry_seconds=retry_seconds,
        allowed_delete_phases=(
            QUALIFICATION_DELETE_PHASE_PRE_INTENT,
            QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
        ),
        require_live_lease=False,
        now=now)


def _transition_qualification_lifecycle_delete_phase(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    from_phase: str,
    to_phase: str,
    now: int | None,
) -> bool:
    """Moves one live exact delete lease between durable provider phases."""
    if (from_phase not in _QUALIFICATION_DELETE_PHASES or
            to_phase not in _QUALIFICATION_DELETE_PHASES):
        raise ValueError('Lifecycle delete phase is invalid.')
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current_revision = topology_state._profile(  # pylint: disable=protected-access
            row)
        lifecycle = current_revision.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        current = catalog_state.database_epoch(session, now=now)
        mutation_expires_at = (mutation['mutation_lease_expires_at']
                               if mutation is not None else None)
        lifecycle_expires_at = (lifecycle.get('mutation_lease_expires_at')
                                if isinstance(lifecycle, dict) else None)
        if (current_revision.state not in (models.ImageProfileState.QUALIFYING,
                                           models.ImageProfileState.ACTIVE) or
                current_revision.desired_generation
                != revision.desired_generation or
                current_revision.config_hash != revision.config_hash or
                not isinstance(lifecycle, dict) or
                lifecycle.get('status') != 'DELETING' or
                lifecycle.get('delete_phase') != from_phase or
                lifecycle.get('target_fingerprint') != target.target_fingerprint
                or lifecycle.get('repository_arn') != repository_arn or
                lifecycle.get('runtime_digest') != runtime_digest or
                lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
                != _LIFECYCLE_PROTOCOL_VERSION or
                qualification_lifecycle_proof_id(lifecycle)
                != lifecycle_proof_id or
                lifecycle.get('mutation_lease_token') != mutation_lease_token or
                not isinstance(lifecycle_expires_at, int) or
                lifecycle_expires_at <= current or
                not _qualification_mutation_matches(
                    mutation,
                    state=_QUALIFICATION_MUTATION_DELETING,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=lifecycle_proof_id,
                    delete_phase=from_phase,
                    mutation_lease_token=mutation_lease_token) or
                not isinstance(mutation_expires_at, int) or
                mutation_expires_at <= current):
            return False
        changed = session.execute(schema.qualification_mutation.update().where(
            schema.qualification_mutation.c.id == _QUALIFICATION_MUTATION_ID,
            schema.qualification_mutation.c.state ==
            _QUALIFICATION_MUTATION_DELETING,
            schema.qualification_mutation.c.delete_phase == from_phase,
            schema.qualification_mutation.c.lifecycle_proof_id ==
            lifecycle_proof_id,
            schema.qualification_mutation.c.mutation_lease_token ==
            mutation_lease_token).values(delete_phase=to_phase,
                                         updated_at=current)).rowcount
        if changed != 1:
            return False
        topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='DELETING',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=lifecycle_proof_id,
                delete_phase=to_phase,
                mutation_lease_token=mutation_lease_token,
                mutation_lease_expires_at=mutation_expires_at),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=current)
        return True


def begin_qualification_lifecycle_delete_request(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    now: int | None = None,
) -> bool:
    """Commits provider-call intent before the raw ECR delete may begin."""
    return _transition_qualification_lifecycle_delete_phase(
        revision,
        target,
        repository_arn=repository_arn,
        runtime_digest=runtime_digest,
        lifecycle_proof_id=lifecycle_proof_id,
        mutation_lease_token=mutation_lease_token,
        from_phase=QUALIFICATION_DELETE_PHASE_PRE_INTENT,
        to_phase=QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
        now=now)


def cancel_qualification_lifecycle_delete_request(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    now: int | None = None,
) -> bool:
    """Returns to pre-intent only after the adapter proves no call began."""
    return _transition_qualification_lifecycle_delete_phase(
        revision,
        target,
        repository_arn=repository_arn,
        runtime_digest=runtime_digest,
        lifecycle_proof_id=lifecycle_proof_id,
        mutation_lease_token=mutation_lease_token,
        from_phase=QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
        to_phase=QUALIFICATION_DELETE_PHASE_PRE_INTENT,
        now=now)


def mark_qualification_lifecycle_delete_readback(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    now: int | None = None,
) -> bool:
    """Persists a conclusive delete response before exact readback."""
    return _transition_qualification_lifecycle_delete_phase(
        revision,
        target,
        repository_arn=repository_arn,
        runtime_digest=runtime_digest,
        lifecycle_proof_id=lifecycle_proof_id,
        mutation_lease_token=mutation_lease_token,
        from_phase=QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
        to_phase=QUALIFICATION_DELETE_PHASE_READBACK,
        now=now)


def retry_qualification_lifecycle_delete_from_readback(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    now: int | None = None,
) -> bool:
    """Rearms deletion after concluded readback proves exact presence."""
    return _transition_qualification_lifecycle_delete_phase(
        revision,
        target,
        repository_arn=repository_arn,
        runtime_digest=runtime_digest,
        lifecycle_proof_id=lifecycle_proof_id,
        mutation_lease_token=mutation_lease_token,
        from_phase=QUALIFICATION_DELETE_PHASE_READBACK,
        to_phase=QUALIFICATION_DELETE_PHASE_PRE_INTENT,
        now=now)


def quarantine_qualification_lifecycle_delete(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    reason: str = 'PROVIDER_OUTCOME_AMBIGUOUS',
    now: int | None = None,
) -> topology_state.ProfileRevisionRecord | None:
    """Permanently fences one request that may still reach the provider."""
    if not reason:
        raise ValueError('Lifecycle delete quarantine reason is required.')
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current_revision = topology_state._profile(  # pylint: disable=protected-access
            row)
        lifecycle = current_revision.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        current = catalog_state.database_epoch(session, now=now)
        if (current_revision.state not in (models.ImageProfileState.QUALIFYING,
                                           models.ImageProfileState.ACTIVE) or
                current_revision.desired_generation
                != revision.desired_generation or
                current_revision.config_hash != revision.config_hash or
                not isinstance(lifecycle, dict) or
                lifecycle.get('status') != 'DELETING' or
                lifecycle.get('delete_phase')
                != QUALIFICATION_DELETE_PHASE_IN_FLIGHT or
                lifecycle.get('target_fingerprint') != target.target_fingerprint
                or lifecycle.get('repository_arn') != repository_arn or
                lifecycle.get('runtime_digest') != runtime_digest or
                lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
                != _LIFECYCLE_PROTOCOL_VERSION or
                qualification_lifecycle_proof_id(lifecycle)
                != lifecycle_proof_id or
                lifecycle.get('mutation_lease_token') != mutation_lease_token or
                not _qualification_mutation_matches(
                    mutation,
                    state=_QUALIFICATION_MUTATION_DELETING,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=lifecycle_proof_id,
                    delete_phase=QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
                    mutation_lease_token=mutation_lease_token)):
            return None
        changed = session.execute(schema.qualification_mutation.update().where(
            schema.qualification_mutation.c.id == _QUALIFICATION_MUTATION_ID,
            schema.qualification_mutation.c.state ==
            _QUALIFICATION_MUTATION_DELETING,
            schema.qualification_mutation.c.delete_phase ==
            QUALIFICATION_DELETE_PHASE_IN_FLIGHT,
            schema.qualification_mutation.c.lifecycle_proof_id ==
            lifecycle_proof_id,
            schema.qualification_mutation.c.mutation_lease_token ==
            mutation_lease_token).values(
                state=_QUALIFICATION_MUTATION_QUARANTINED,
                delete_phase=None,
                mutation_lease_token=None,
                mutation_lease_expires_at=None,
                quarantine_reason=reason,
                updated_at=current)).rowcount
        if changed != 1:
            return None
        _record_qualification_repository_quarantine_in_session(
            session,
            revision_id=revision.id,
            target=target,
            repository_arn=repository_arn,
            runtime_digest=runtime_digest,
            lifecycle_proof_id=lifecycle_proof_id,
            reason=reason,
            now=current)
        return topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='QUARANTINED',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=lifecycle_proof_id,
                quarantine_reason=reason),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=current)


def complete_qualification_lifecycle_delete(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    lifecycle_proof_id: str,
    mutation_lease_token: str,
    now: int | None = None,
) -> topology_state.ProfileRevisionRecord | None:
    """Commits exact absence only for the current destructive lease owner."""
    lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current = topology_state._profile(  # pylint: disable=protected-access
            row)
        lifecycle = current.attestations.get(lifecycle_key)
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=True)
        current_time = catalog_state.database_epoch(session, now=now)
        mutation_expires_at = (mutation['mutation_lease_expires_at']
                               if mutation is not None else None)
        if (current.state not in (models.ImageProfileState.QUALIFYING,
                                  models.ImageProfileState.ACTIVE) or
                not isinstance(lifecycle, dict) or
                lifecycle.get('status') != 'DELETING' or
                lifecycle.get('delete_phase')
                != QUALIFICATION_DELETE_PHASE_READBACK or
                lifecycle.get('target_fingerprint') != target.target_fingerprint
                or lifecycle.get('repository_arn') != repository_arn or
                lifecycle.get('runtime_digest') != runtime_digest or
                lifecycle.get(_LIFECYCLE_PROTOCOL_KEY)
                != _LIFECYCLE_PROTOCOL_VERSION or
                qualification_lifecycle_proof_id(lifecycle)
                != lifecycle_proof_id or
                lifecycle.get('mutation_lease_token') != mutation_lease_token or
                not isinstance(lifecycle.get('mutation_lease_expires_at'), int)
                or lifecycle['mutation_lease_expires_at'] <= current_time or
                not _qualification_mutation_matches(
                    mutation,
                    state=_QUALIFICATION_MUTATION_DELETING,
                    revision_id=revision.id,
                    target=target,
                    repository_arn=repository_arn,
                    runtime_digest=runtime_digest,
                    lifecycle_proof_id=lifecycle_proof_id,
                    delete_phase=QUALIFICATION_DELETE_PHASE_READBACK,
                    mutation_lease_token=mutation_lease_token) or
                not isinstance(mutation_expires_at, int) or
                mutation_expires_at <= current_time):
            return None
        changed = session.execute(schema.qualification_mutation.update().where(
            schema.qualification_mutation.c.id == _QUALIFICATION_MUTATION_ID,
            schema.qualification_mutation.c.state ==
            _QUALIFICATION_MUTATION_DELETING,
            schema.qualification_mutation.c.delete_phase ==
            QUALIFICATION_DELETE_PHASE_READBACK,
            schema.qualification_mutation.c.lifecycle_proof_id ==
            lifecycle_proof_id,
            schema.qualification_mutation.c.mutation_lease_token ==
            mutation_lease_token).values(
                state=_QUALIFICATION_MUTATION_RESTORING,
                delete_phase=None,
                mutation_lease_token=None,
                mutation_lease_expires_at=None,
                quarantine_reason=None,
                updated_at=current_time)).rowcount
        if changed != 1:
            return None
        return topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=lifecycle_key,
            evidence=qualification_lifecycle_evidence(
                status='READY',
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=lifecycle_proof_id,
                exact_absence=True),
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=now)


def record_qualification_copy(
    revision: topology_state.ProfileRevisionRecord,
    target: models.ManagedRegistryTarget,
    *,
    repository_arn: str,
    runtime_digest: str,
    platform: str,
    copy_outcome: str,
    expected_lifecycle_proof_id: str | None,
    expected_mutation_proof_id: str | None = None,
    now: int | None = None,
) -> topology_state.ProfileRevisionRecord | None:
    """Records a copy and atomically clears its owned restoration barrier."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = topology_state.lock_profile_revision_mutation_in_session(
            session, revision.id)
        current = topology_state._profile(  # pylint: disable=protected-access
            row)
        if (current.state not in (models.ImageProfileState.QUALIFYING,
                                  models.ImageProfileState.ACTIVE) or
                current.desired_generation != revision.desired_generation or
                current.config_hash != revision.config_hash or
                not _revision_owns_qualification_work_in_session(
                    session, current)):
            raise topology_state.StaleProfileRevisionError(
                'Qualification copy no longer matches the desired revision.')
        mutation = topology_state.get_qualification_mutation_in_session(
            session, exclusive=expected_mutation_proof_id is not None)
        if not _qualification_repository_available_in_session(
                session, repository_arn):
            raise topology_state.StaleProfileRevisionError(
                'Qualification copy repository is quarantined.')
        if expected_mutation_proof_id is None:
            if mutation is not None:
                return None
        elif not _qualification_mutation_matches(
                mutation,
                state=_QUALIFICATION_MUTATION_RESTORING,
                revision_id=revision.id,
                target=target,
                repository_arn=repository_arn,
                runtime_digest=runtime_digest,
                lifecycle_proof_id=expected_mutation_proof_id,
                delete_phase=None,
                mutation_lease_token=None):
            return None
        current_proof_id = qualification_copy_restoration_proof_id(
            current, target, runtime_digest)
        lifecycle_key = models.profile_attestation_key('lifecycle', target.name)
        lifecycle = current.attestations.get(lifecycle_key)
        if expected_lifecycle_proof_id is None:
            if isinstance(lifecycle, dict):
                return None
        elif (current_proof_id != expected_lifecycle_proof_id or
              not isinstance(lifecycle, dict) or
              lifecycle.get('repository_arn') != repository_arn):
            return None
        if (expected_mutation_proof_id is not None and
                expected_lifecycle_proof_id != expected_mutation_proof_id):
            return None
        updated = topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=models.profile_attestation_key('copy', target.name),
            evidence={
                'status': 'READY',
                'target': target.name,
                'target_fingerprint': target.target_fingerprint,
                'repository_arn': repository_arn,
                'runtime_digest': runtime_digest,
                'platform': platform,
                'copy_outcome': copy_outcome,
                **qualification_copy_restoration_evidence(expected_lifecycle_proof_id),
            },
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            now=now)
        if expected_mutation_proof_id is not None:
            cleared = session.execute(
                schema.qualification_mutation.delete().where(
                    schema.qualification_mutation.c.id ==
                    _QUALIFICATION_MUTATION_ID,
                    schema.qualification_mutation.c.state ==
                    _QUALIFICATION_MUTATION_RESTORING,
                    schema.qualification_mutation.c.owner_profile_revision_id ==
                    revision.id,
                    schema.qualification_mutation.c.owner_target_fingerprint ==
                    target.target_fingerprint,
                    schema.qualification_mutation.c.repository_arn ==
                    repository_arn,
                    schema.qualification_mutation.c.runtime_digest ==
                    runtime_digest,
                    schema.qualification_mutation.c.lifecycle_proof_id ==
                    expected_mutation_proof_id)).rowcount
            if cleared != 1:
                raise RuntimeError(
                    'Qualification restoration barrier CAS drifted.')
        return updated


def _hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True,
                   separators=(',', ':')).encode()).hexdigest()


def ingest_manifest(
    *, profile_name: str, manifest: dict[str, Any], actor_hash: str,
    idempotency_key: str
) -> tuple[catalog_state.OperationRecord, topology_state.ProfileRevisionRecord]:
    """Persists one secret-free Terraform handoff without provider I/O."""
    profile_name = models.validate_control_plane_identifier(
        profile_name, 'Qualification profile')
    payload = json.dumps(manifest, sort_keys=True,
                         separators=(',', ':')).encode()
    parsed = aws.TerraformQualificationManifest.from_json(payload)
    if parsed.profile != profile_name:
        raise ValueError('Qualification manifest profile does not match path.')
    authority_id = catalog_state.get_catalog_authority_id()
    assert authority_id is not None
    request_hash = _hash({
        'authority_id': authority_id,
        'workspace': parsed.workspace,
        'profile': profile_name,
        'manifest_hash': parsed.manifest_hash,
    })
    operation, _ = catalog_state.create_or_get_operation(
        authority_id=authority_id,
        scope=parsed.workspace,
        actor_hash=actor_hash,
        kind='PROFILE_QUALIFY',
        idempotency_key=idempotency_key,
        request_hash=request_hash)
    if operation.state == models.ImageOperationState.FAILED:
        raise ValueError(operation.error_code or 'QUALIFICATION_FAILED')
    if operation.result_id is not None:
        revision = topology_state.get_profile_revision(operation.result_id)
        if revision is None or operation.result_kind != 'profile_revision':
            raise ValueError('Qualification operation result is unavailable.')
        return operation, revision
    try:
        revision = aws.ingest_terraform_qualification(payload)
    except topology_state.QualificationMutationInProgressError:
        # Keep the idempotent operation retryable until the catalog-wide
        # qualification restoration barrier is cleared.
        raise
    except (TypeError, ValueError):
        catalog_state.bind_operation_result(
            operation.id,
            result_kind='qualification',
            result_id=operation.id,
            result={
                'profile': profile_name,
                'state': models.ImageOperationState.FAILED.value,
            },
            terminal_state=models.ImageOperationState.FAILED,
            error_code='QUALIFICATION_FAILED')
        raise ValueError('QUALIFICATION_FAILED') from None
    operation = catalog_state.bind_operation_result(
        operation.id,
        result_kind='profile_revision',
        result_id=revision.id,
        result={
            'profile_revision_id': revision.id,
            'state': revision.state.value,
        },
        terminal_state=models.ImageOperationState.SUCCEEDED)
    return operation, revision


def request_canary(
    *,
    workspace: str,
    profile_name: str,
    target_id: str,
    backend: str,
    runtime_id: str | None = None,
    actor_hash: str,
    idempotency_key: str
) -> tuple[catalog_state.OperationRecord, topology_state.ProfileRevisionRecord]:
    """Creates one durable canary intent; a background runner owns launch."""
    profile, _ = config.resolve_profile(profile_name, workspace)
    if profile is None:
        raise ValueError('PROFILE_NOT_ACTIVE')
    target = profile.target(target_id)
    binding_id = target.runtime_binding(backend)
    if binding_id is None:
        raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
    binding = profile.bindings[binding_id]
    candidate_runtime_ids = runtime_ids(target, backend, binding)
    if runtime_id is None:
        if len(candidate_runtime_ids) != 1:
            raise ValueError('CANARY_RUNTIME_ID_REQUIRED')
        runtime_id = candidate_runtime_ids[0]
    runtime_id = models.validate_control_plane_identifier(
        runtime_id, 'Canary runtime ID')
    if runtime_id not in candidate_runtime_ids:
        raise ValueError('IMAGE_LOCALITY_UNSUPPORTED')
    desired = topology_state.get_desired_profile(workspace, profile.name)
    if desired is None:
        desired = topology_state.get_active_profile(workspace, profile.name)
    if (desired is None or desired.revision != profile.revision or
            desired.config_hash != profile.config_hash):
        raise ValueError('QUALIFICATION_FAILED')
    if not _qualification_copy_requestable(desired, profile, target):
        raise ValueError('QUALIFICATION_FAILED')
    authority_id = catalog_state.get_catalog_authority_id()
    assert authority_id is not None
    request_hash = _hash({
        'authority_id': authority_id,
        'workspace': workspace,
        'profile_revision_id': desired.id,
        'target': target.name,
        'target_fingerprint': target.target_fingerprint,
        'backend': backend,
        'binding_id': binding_id,
        'runtime_id': runtime_id,
    })
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session)
        operation, created = catalog_state.begin_operation(
            session,
            authority_id=authority_id,
            scope=workspace,
            actor_hash=actor_hash,
            kind='PROFILE_CANARY',
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            now=current)
        if created:
            payload = {
                'profile_revision_id': desired.id,
                'desired_generation': desired.desired_generation,
                'config_hash': desired.config_hash,
                'target': target.name,
                'target_fingerprint': target.target_fingerprint,
                'backend': backend,
                'binding_id': binding_id,
                'binding_fingerprint': binding.fingerprint,
                'runtime_id': runtime_id,
                'nonce': secrets.token_hex(16),
                'worst_case_microusd':
                    (profile.qualification.canary_worst_case_microusd),
                'timeout_seconds': profile.qualification.canary_timeout_seconds,
            }
            row = session.execute(schema.operations.update().where(
                schema.operations.c.id == operation.id).values(
                    result_kind='profile_revision',
                    result_id=desired.id,
                    result_json=json.dumps(payload,
                                           sort_keys=True,
                                           separators=(',', ':')),
                    updated_at=current).returning(
                        schema.operations)).mappings().one()
            operation = catalog_state._operation(  # pylint: disable=protected-access
                row)
        elif (operation.result_id != desired.id or
              operation.result_kind != 'profile_revision'):
            raise ValueError('Canary operation result is unavailable.')
    return operation, desired


def canary_payload(operation: catalog_state.OperationRecord) -> dict[str, Any]:
    payload = operation.result
    required = {
        'profile_revision_id', 'desired_generation', 'config_hash', 'target',
        'target_fingerprint', 'backend', 'binding_id', 'binding_fingerprint',
        'runtime_id', 'nonce', 'worst_case_microusd', 'timeout_seconds'
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError('Canary operation payload is invalid.')
    if (not isinstance(payload['desired_generation'], int) or not isinstance(
            payload['worst_case_microusd'], int
    ) or not isinstance(payload['timeout_seconds'], int) or any(
            not isinstance(payload[key], str) or not payload[key] for key in
            required -
        {'desired_generation', 'worst_case_microusd', 'timeout_seconds'})):
        raise ValueError('Canary operation payload types are invalid.')
    return payload


def _canary_copy_available(
        payload: dict[str, Any],
        revision: topology_state.ProfileRevisionRecord) -> bool:
    if (revision.id != payload['profile_revision_id'] or
            revision.desired_generation != payload['desired_generation'] or
            revision.config_hash != payload['config_hash']):
        return False
    profile = models.ManagedRegistryProfile.from_snapshot(
        revision.config_snapshot)
    target = profile.target(payload['target'])
    if target.target_fingerprint != payload['target_fingerprint']:
        return False
    return qualification_copy_available(revision, profile, target)


def _canary_admission_available_in_session(
    session: orm.Session,
    payload: dict[str, Any],
    revision: topology_state.ProfileRevisionRecord,
) -> bool:
    topology_state.assert_qualification_mutation_idle_in_session(session)
    target_evidence = revision.attestations.get(
        models.profile_attestation_key('terraform_target', payload['target']))
    repository_arn = (target_evidence.get('repository_arn') if isinstance(
        target_evidence, dict) else None)
    return (isinstance(repository_arn, str) and
            _qualification_repository_available_in_session(
                session, repository_arn) and
            _revision_owns_qualification_work_in_session(session, revision) and
            _canary_copy_available(payload, revision))


def _validate_canary_ec2_instance_profile_arn(instance_profile_arn: Any) -> str:
    if (not isinstance(instance_profile_arn, str) or
            not instance_profile_arn.startswith('arn:') or
            len(instance_profile_arn) > 2048 or
            any(character.isspace() for character in instance_profile_arn)):
        raise ValueError('Canary child evidence is invalid.')
    return instance_profile_arn


def canary_ec2_instance_profile_arn(
        operation: catalog_state.OperationRecord) -> str | None:
    """Returns one strictly typed durable EC2 profile observation."""
    evidence = operation.canary_child_evidence
    if evidence is None:
        return None
    if (not isinstance(evidence, dict) or
            set(evidence) != {'backend', 'instance_profile_arn'} or
            evidence.get('backend') != 'aws_vm'):
        raise ValueError('Canary child evidence is invalid.')
    return _validate_canary_ec2_instance_profile_arn(
        evidence.get('instance_profile_arn'))


def record_canary_ec2_instance_profile(operation_id: str,
                                       lease_token: str,
                                       child_launch_id: str,
                                       instance_profile_arn: str,
                                       *,
                                       now: int | None = None) -> bool:
    """Lease-fences one immutable provider-observed profile ARN."""
    instance_profile_arn = _validate_canary_ec2_instance_profile_arn(
        instance_profile_arn)
    evidence = {
        'backend': 'aws_vm',
        'instance_profile_arn': instance_profile_arn,
    }
    table = schema.operations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(table).where(table.c.id == operation_id).
            with_for_update()).mappings().first()
        if row is None:
            return False
        current = catalog_state.database_epoch(session, now=now)
        operation = catalog_state._operation(  # pylint: disable=protected-access
            row)
        payload = canary_payload(operation)
        if payload['backend'] != 'aws_vm':
            raise ValueError('Canary child evidence backend is invalid.')
        live_owner = (operation.kind == 'PROFILE_CANARY' and
                      operation.state == models.ImageOperationState.RUNNING and
                      operation.lease_token == lease_token and
                      operation.lease_expires_at is not None and
                      operation.lease_expires_at > current and
                      operation.teardown_deadline is not None and
                      operation.teardown_deadline > current and
                      operation.child_launch_id == child_launch_id)
        if not live_owner:
            return False
        existing = canary_ec2_instance_profile_arn(operation)
        if existing is not None:
            if existing != instance_profile_arn:
                raise ValueError('Canary child evidence is immutable.')
            return True
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(table.update().where(
            table.c.id == operation_id, table.c.kind == 'PROFILE_CANARY',
            table.c.state == models.ImageOperationState.RUNNING.value,
            table.c.lease_token == lease_token,
            table.c.lease_expires_at.is_not(None),
            table.c.lease_expires_at > clock,
            table.c.teardown_deadline.is_not(None), table.c.teardown_deadline
            > clock, table.c.child_launch_id == child_launch_id,
            table.c.canary_child_evidence_json.is_(None)).values(
                canary_child_evidence_json=json.dumps(evidence,
                                                      sort_keys=True,
                                                      separators=(',', ':')),
                updated_at=clock)).rowcount
        return changed == 1


def claim_canary(
        *,
        worker_id: str,
        lease_seconds: int,
        now: int | None = None) -> catalog_state.OperationRecord | None:
    """Claims one canary and atomically reserves its worst-case daily cost."""
    if lease_seconds <= 0:
        raise ValueError('Canary lease must be positive.')
    table = schema.operations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        current = catalog_state.database_epoch(session, now=now)
        clock = catalog_state.database_epoch_expression(now=now)
        rows = session.execute(
            sqlalchemy.select(table).where(
                table.c.canary_claimable_at.is_not(None),
                table.c.canary_claimable_at
                <= clock).order_by(table.c.canary_claimable_at,
                                   table.c.id).limit(16).with_for_update(
                                       skip_locked=True)).mappings().all()
        for row in rows:
            operation = catalog_state._operation(  # pylint: disable=protected-access
                row)
            claim_time = current
            try:
                payload = canary_payload(operation)
                if operation.state == models.ImageOperationState.PENDING:
                    _, claim_time = topology_state.reserve_canary_cost(
                        session,
                        profile_revision_id=payload['profile_revision_id'],
                        expected_generation=payload['desired_generation'],
                        worst_case_microusd=payload['worst_case_microusd'],
                        admission_check=functools.partial(
                            _canary_admission_available_in_session, session,
                            payload),
                        now=now)
                else:
                    claim_time = catalog_state.database_epoch(session, now=now)
            except topology_state.QualificationMutationInProgressError:
                # The operation stays PENDING and cost-free while the rare
                # catalog-wide delete/restoration window drains.
                continue
            except (TypeError, ValueError,
                    topology_state.StaleProfileRevisionError) as error:
                if (operation.state == models.ImageOperationState.RUNNING and
                        operation.child_launch_id is not None):
                    # A future or rolled-back worker may still understand the
                    # immutable child contract. Keep its durable owner and
                    # rotate the poison row behind other runnable canaries.
                    clock = catalog_state.database_epoch_expression(now=now)
                    session.execute(table.update().where(
                        table.c.id == operation.id, table.c.state ==
                        models.ImageOperationState.RUNNING.value,
                        table.c.lease_token == operation.lease_token,
                        table.c.child_launch_id == operation.child_launch_id,
                        table.c.lease_expires_at.is_not(None),
                        table.c.lease_expires_at
                        <= clock).values(updated_at=clock))
                    continue
                error_code = ('CANARY_DAILY_COST_LIMIT'
                              if str(error) == 'CANARY_DAILY_COST_LIMIT' else
                              'QUALIFICATION_FAILED')
                catalog_state.fail_operation(session,
                                             operation.id,
                                             error_code,
                                             result_kind='profile_revision',
                                             result_id=operation.result_id,
                                             result=operation.result,
                                             now=now)
                continue
            token = f'{worker_id}:{uuid.uuid4()}'
            teardown_deadline = (operation.teardown_deadline or
                                 claim_time + payload['timeout_seconds'])
            claimed = session.execute(
                table.update().where(table.c.id == operation.id).values(
                    state=models.ImageOperationState.RUNNING.value,
                    lease_token=token,
                    lease_expires_at=claim_time + lease_seconds,
                    teardown_deadline=teardown_deadline,
                    updated_at=claim_time).returning(table)).mappings().one()
            return catalog_state._operation(  # pylint: disable=protected-access
                claimed)
    return None


def heartbeat_canary(operation_id: str,
                     lease_token: str,
                     lease_seconds: int,
                     *,
                     now: int | None = None) -> bool:
    with orm.Session(catalog_state.engine()) as session, session.begin():
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(schema.operations.update().where(
            schema.operations.c.id == operation_id,
            schema.operations.c.kind == 'PROFILE_CANARY',
            schema.operations.c.state ==
            models.ImageOperationState.RUNNING.value,
            schema.operations.c.lease_token == lease_token,
            schema.operations.c.lease_expires_at
            > clock).values(lease_expires_at=clock + lease_seconds,
                            updated_at=clock)).rowcount
    return changed == 1


def release_drained_canary(operation: catalog_state.OperationRecord,
                           *,
                           teardown_verified: bool,
                           now: int | None = None) -> bool:
    """Makes a child-free or teardown-verified canary promptly reclaimable."""
    if teardown_verified is not True:
        raise ValueError(
            'Canary drain teardown must be verified before release.')
    if operation.lease_token is None:
        return False
    table = schema.operations
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(
                table.c.kind, table.c.state, table.c.lease_token,
                table.c.lease_expires_at).where(table.c.id == operation.id).
            with_for_update()).mappings().first()
        if (row is None or row['kind'] != 'PROFILE_CANARY' or
                row['state'] != models.ImageOperationState.RUNNING.value or
                row['lease_token'] != operation.lease_token or
                row['lease_expires_at'] is None):
            return False
        current = catalog_state.database_epoch(session, now=now)
        if int(row['lease_expires_at']) <= current:
            return False
        changed = session.execute(table.update().where(
            table.c.id == operation.id, table.c.kind == 'PROFILE_CANARY',
            table.c.state == models.ImageOperationState.RUNNING.value,
            table.c.lease_token == operation.lease_token,
            table.c.lease_expires_at
            > current).values(lease_expires_at=current,
                              updated_at=current)).rowcount
    return changed == 1


def attach_canary_child(operation_id: str,
                        lease_token: str,
                        child_launch_id: str,
                        *,
                        now: int | None = None) -> bool:
    """Persists the provider child before waiting, closing relaunch races."""
    if (not isinstance(child_launch_id, str) or not child_launch_id or
            len(child_launch_id) > 2048 or
            any(character.isspace() for character in child_launch_id)):
        raise ValueError('Canary child launch ID is invalid.')
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(schema.operations).where(
                schema.operations.c.id ==
                operation_id).with_for_update()).mappings().first()
        if row is None:
            return False
        current = catalog_state.database_epoch(session, now=now)
        live_owner = (str(row['kind']) == 'PROFILE_CANARY' and str(
            row['state']) == models.ImageOperationState.RUNNING.value and
                      row['lease_token'] == lease_token and
                      row['lease_expires_at'] is not None and
                      int(row['lease_expires_at']) > current)
        if (live_owner and
                row['child_launch_id'] not in (None, child_launch_id)):
            raise ValueError('Canary operation already owns another child.')
        clock = catalog_state.database_epoch_expression(now=now)
        changed = session.execute(schema.operations.update().where(
            schema.operations.c.id == operation_id,
            schema.operations.c.kind == 'PROFILE_CANARY',
            schema.operations.c.state ==
            models.ImageOperationState.RUNNING.value,
            schema.operations.c.lease_token == lease_token,
            schema.operations.c.lease_expires_at.is_not(None),
            schema.operations.c.lease_expires_at > clock,
            sqlalchemy.or_(
                schema.operations.c.child_launch_id == child_launch_id,
                sqlalchemy.and_(
                    schema.operations.c.child_launch_id.is_(None),
                    schema.operations.c.teardown_deadline.is_not(None),
                    schema.operations.c.teardown_deadline
                    > clock))).values(child_launch_id=child_launch_id,
                                      updated_at=clock)).rowcount
        return changed == 1


def authorize_canary_launch(operation_id: str,
                            lease_token: str,
                            child_launch_id: str,
                            *,
                            now: int | None = None) -> int | None:
    """Returns DB-authoritative launch time for the exact durable child."""
    with orm.Session(catalog_state.engine()) as session, session.begin():
        row = session.execute(
            sqlalchemy.select(schema.operations).where(
                schema.operations.c.id ==
                operation_id).with_for_update()).mappings().first()
        if row is None:
            return None
        current = catalog_state.database_epoch(session, now=now)
        deadline = row['teardown_deadline']
        if (str(row['kind']) != 'PROFILE_CANARY' or
                str(row['state']) != models.ImageOperationState.RUNNING.value or
                row['lease_token'] != lease_token or
                row['child_launch_id'] != child_launch_id or
                row['lease_expires_at'] is None or
                int(row['lease_expires_at']) <= current or deadline is None or
                int(deadline) <= current):
            return None
        try:
            operation = catalog_state._operation(  # pylint: disable=protected-access
                row)
            payload = canary_payload(operation)
            revision_row = (
                topology_state.lock_profile_revision_mutation_in_session(
                    session, payload['profile_revision_id']))
            revision = topology_state._profile(  # pylint: disable=protected-access
                revision_row)
            if not _canary_admission_available_in_session(
                    session, payload, revision):
                return None
        except (TypeError, ValueError,
                topology_state.StaleProfileRevisionError):
            return None
        return int(deadline) - current


def complete_canary(operation: catalog_state.OperationRecord,
                    evidence: dict[str, Any],
                    *,
                    now: int | None = None) -> bool:
    """Atomically records one runtime proof and closes its owned operation."""
    if (operation.lease_token is None or
            evidence.get('teardown_verified') is not True):
        return False
    payload = canary_payload(operation)
    revision = topology_state.get_profile_revision(
        payload['profile_revision_id'])
    if revision is None:
        return False
    profile = models.ManagedRegistryProfile.from_snapshot(
        revision.config_snapshot)
    target = profile.target(payload['target'])
    binding = profile.bindings[payload['binding_id']]
    key = _runtime_attestation_key(target, payload['backend'], binding,
                                   payload['runtime_id'])
    with orm.Session(catalog_state.engine()) as session, session.begin():
        locked = session.execute(
            sqlalchemy.select(schema.operations.c.id).where(
                schema.operations.c.id ==
                operation.id).with_for_update()).first()
        if locked is None:
            return False
        current = catalog_state.database_epoch(session, now=now)
        completed = catalog_state.complete_operation(
            session,
            operation.id,
            result_kind='profile_revision',
            result_id=revision.id,
            result={
                **payload,
                'attestation_key': key,
                'observed_at': current,
            },
            lease_token=operation.lease_token,
            deadline_expired=False,
            now=now)
        if not completed:
            return False
        topology_state.record_profile_attestation_in_session(
            session,
            profile_revision_id=revision.id,
            kind=key,
            evidence={
                **evidence,
                'observed_at': current,
            },
            expected_generation=payload['desired_generation'],
            expected_config_hash=payload['config_hash'],
            now=current)
        return True


def fail_canary(operation: catalog_state.OperationRecord,
                error_code: str,
                *,
                now: int | None = None) -> bool:
    """Fails only the still-owned canary operation after teardown."""
    if error_code == 'CANARY_TEARDOWN_FAILED':
        raise ValueError('Unverified canary teardown must remain reclaimable.')
    if operation.lease_token is None:
        return False
    with orm.Session(catalog_state.engine()) as session, session.begin():
        locked = session.execute(
            sqlalchemy.select(schema.operations.c.id).where(
                schema.operations.c.id ==
                operation.id).with_for_update()).first()
        if locked is None:
            return False
        return catalog_state.fail_operation(session,
                                            operation.id,
                                            error_code,
                                            result_kind=operation.result_kind,
                                            result_id=operation.result_id,
                                            result=operation.result,
                                            lease_token=operation.lease_token,
                                            deadline_expired=False,
                                            now=now)


def fail_expired_canary(operation: catalog_state.OperationRecord,
                        error_code: str,
                        *,
                        teardown_verified: bool,
                        now: int | None = None) -> bool:
    """Fails a live deadline-expired owner after teardown, never launch."""
    if error_code != 'CANARY_TIMEOUT':
        raise ValueError('Expired canary failure code is invalid.')
    if not teardown_verified:
        raise ValueError('Expired canary teardown must be verified.')
    if operation.lease_token is None:
        return False
    with orm.Session(catalog_state.engine()) as session, session.begin():
        locked = session.execute(
            sqlalchemy.select(schema.operations.c.id).where(
                schema.operations.c.id ==
                operation.id).with_for_update()).first()
        if locked is None:
            return False
        return catalog_state.fail_operation(session,
                                            operation.id,
                                            error_code,
                                            result_kind=operation.result_kind,
                                            result_id=operation.result_id,
                                            result=operation.result,
                                            lease_token=operation.lease_token,
                                            deadline_expired=True,
                                            now=now)


def fail_owned_canary(operation: catalog_state.OperationRecord,
                      error_code: str,
                      *,
                      teardown_verified: bool,
                      now: int | None = None) -> bool:
    """Fails the exact live owner after classifying its locked deadline."""
    if error_code == 'CANARY_TEARDOWN_FAILED':
        raise ValueError('Unverified canary teardown must remain reclaimable.')
    if not teardown_verified:
        raise ValueError('Canary teardown must be verified before failure.')
    if operation.lease_token is None:
        return False
    with orm.Session(catalog_state.engine()) as session, session.begin():
        locked = session.execute(
            sqlalchemy.select(schema.operations.c.teardown_deadline).where(
                schema.operations.c.id ==
                operation.id).with_for_update()).first()
        if locked is None:
            return False
        current = catalog_state.database_epoch(session, now=now)
        deadline = locked.teardown_deadline
        deadline_expired = (deadline is not None and current >= int(deadline))
        return catalog_state.fail_operation(
            session,
            operation.id,
            'CANARY_TIMEOUT' if deadline_expired else error_code,
            result_kind=operation.result_kind,
            result_id=operation.result_id,
            result=operation.result,
            lease_token=operation.lease_token,
            deadline_expired=deadline_expired,
            now=now)


def schedule_automatic_canaries(
        *,
        limit: int = 100,
        now: int | None = None,
        should_stop: Callable[[], bool] | None = None) -> int:
    """Creates bounded idempotent runtime canaries only after copy readiness."""
    if should_stop is not None and should_stop():
        return 0
    current = _database_epoch(now=now)
    scheduled = 0
    if should_stop is not None and should_stop():
        return scheduled
    revisions = topology_state.list_qualifying_profiles(include_active=True,
                                                        limit=limit)
    if should_stop is not None and should_stop():
        return scheduled
    for revision in revisions:
        if should_stop is not None and should_stop():
            break
        profile = models.ManagedRegistryProfile.from_snapshot(
            revision.config_snapshot)
        if not profile.qualification.automatic_canaries:
            continue
        for target in (profile.canonical,) + profile.targets:
            if should_stop is not None and should_stop():
                return scheduled
            copy_key = models.profile_attestation_key('copy', target.name)
            if (not qualification_copy_available(revision, profile, target) or
                    not _fresh(revision.attestations.get(copy_key),
                               now=current,
                               max_age_seconds=_AUTOMATIC_WINDOW_SECONDS)):
                continue
            for backend, binding_id in target.runtime_pull:
                if should_stop is not None and should_stop():
                    return scheduled
                binding = profile.bindings[binding_id]
                for runtime_id in runtime_ids(target, backend, binding):
                    if should_stop is not None and should_stop():
                        return scheduled
                    runtime_key = _runtime_attestation_key(
                        target, backend, binding, runtime_id)
                    if _fresh(revision.attestations.get(runtime_key),
                              now=current,
                              max_age_seconds=(
                                  profile.qualification.
                                  runtime_attestation_max_age_seconds)):
                        continue
                    identity = _hash({
                        'revision': revision.id,
                        'target': target.name,
                        'backend': backend,
                        'binding': binding.fingerprint,
                        'runtime_id': runtime_id,
                        'window': current // _AUTOMATIC_WINDOW_SECONDS,
                    })
                    if should_stop is not None and should_stop():
                        return scheduled
                    try:
                        request_canary(
                            workspace=revision.workspace,
                            profile_name=profile.name,
                            target_id=target.name,
                            backend=backend,
                            runtime_id=runtime_id,
                            actor_hash=_AUTOMATIC_ACTOR_HASH,
                            idempotency_key=f'auto-canary:{identity}')
                        scheduled += 1
                    except (catalog_state.IdempotencyKeyReusedError,
                            ValueError):
                        continue
    return scheduled


def maybe_activate_profile(
        profile_revision_id: str,
        *,
        now: int | None = None) -> topology_state.ProfileRevisionRecord | None:
    """Activates exactly the still-desired revision after all proofs converge."""
    revision = topology_state.get_profile_revision(profile_revision_id)
    if revision is None:
        return None
    if revision.state == models.ImageProfileState.ACTIVE:
        return revision
    if (revision.state != models.ImageProfileState.QUALIFYING or
            revision.terraform_hash is None or
            revision.attestations_hash is None):
        return None
    profile = models.ManagedRegistryProfile.from_snapshot(
        revision.config_snapshot)
    preflight_current = _database_epoch(now=now)
    catalog_authority = catalog_state.get_catalog_authority_id()
    if catalog_authority is None:
        return None
    for target in (profile.canonical,) + profile.targets:
        try:
            _, repository_arn = qualification_repository(
                revision,
                target,
                catalog_authority=catalog_authority,
                profile=profile,
                configured_target=target)
        except ValueError:
            return None
        if topology_state.qualification_repository_quarantined(repository_arn):
            return None
        if not qualification_copy_available(revision, profile, target):
            return None
    try:
        requirements = _attestation_requirements(profile, revision.attestations)
    except ValueError:
        return None
    for key, max_age in requirements.items():
        evidence = revision.attestations.get(key)
        if max_age is None:
            ready = (isinstance(evidence, dict) and
                     evidence.get('status') == 'READY' and
                     isinstance(evidence.get('observed_at'), int) and
                     evidence['observed_at'] <= preflight_current)
        else:
            ready = _fresh(evidence,
                           now=preflight_current,
                           max_age_seconds=max_age)
        if not ready:
            return None
    try:
        return transactions.activate_profile(
            profile_revision_id=revision.id,
            expected_generation=revision.desired_generation,
            expected_config_hash=revision.config_hash,
            expected_terraform_hash=revision.terraform_hash,
            expected_attestations_hash=revision.attestations_hash,
            required_attestations=requirements,
            now=now)
    except (topology_state.StaleProfileRevisionError, ValueError):
        return None
