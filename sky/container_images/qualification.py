"""Idempotent profile qualification and canary intent services."""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm

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
        return tuple(cluster.context
                     for cluster in binding.qualified_clusters
                     if f':{target.region}:' in cluster.cluster_arn)
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
        required[models.profile_attestation_key(
            'lifecycle', target.name)] = _AUTOMATIC_WINDOW_SECONDS
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


def qualification_repository(
        revision: topology_state.ProfileRevisionRecord,
        target: models.ManagedRegistryTarget) -> tuple[str, str]:
    """Returns the Terraform-attested non-catalog repository identity."""
    key = models.profile_attestation_key('terraform_target', target.name)
    evidence = revision.attestations.get(key)
    if (not isinstance(evidence, dict) or evidence.get('status') != 'READY' or
            evidence.get('target_fingerprint') != target.target_fingerprint or
            evidence.get('registry') != target.registry or
            not isinstance(evidence.get('repository_name'), str) or
            not isinstance(evidence.get('repository_arn'), str)):
        raise ValueError('QUALIFICATION_FAILED')
    return str(evidence['repository_name']), str(evidence['repository_arn'])


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
        operation, created = catalog_state.begin_operation(
            session,
            authority_id=authority_id,
            scope=workspace,
            actor_hash=actor_hash,
            kind='PROFILE_CANARY',
            idempotency_key=idempotency_key,
            request_hash=request_hash)
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
                    updated_at=int(time.time())).returning(
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
                table.c.kind == 'PROFILE_CANARY',
                sqlalchemy.or_(
                    table.c.state == models.ImageOperationState.PENDING.value,
                    sqlalchemy.and_(
                        table.c.state ==
                        models.ImageOperationState.RUNNING.value,
                        table.c.lease_expires_at <= clock))).order_by(
                            table.c.updated_at,
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
                        now=now)
                else:
                    claim_time = catalog_state.database_epoch(session, now=now)
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
            evidence=evidence,
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


def schedule_automatic_canaries(*,
                                limit: int = 100,
                                now: int | None = None) -> int:
    """Creates bounded idempotent runtime canaries only after copy readiness."""
    current = int(time.time()) if now is None else now
    scheduled = 0
    for revision in topology_state.list_qualifying_profiles(include_active=True,
                                                            limit=limit):
        profile = models.ManagedRegistryProfile.from_snapshot(
            revision.config_snapshot)
        if not profile.qualification.automatic_canaries:
            continue
        for target in (profile.canonical,) + profile.targets:
            copy_key = models.profile_attestation_key('copy', target.name)
            if not _fresh(revision.attestations.get(copy_key),
                          now=current,
                          max_age_seconds=_AUTOMATIC_WINDOW_SECONDS):
                continue
            for backend, binding_id in target.runtime_pull:
                binding = profile.bindings[binding_id]
                for runtime_id in runtime_ids(target, backend, binding):
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
    current = int(time.time()) if now is None else now
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
                     evidence['observed_at'] <= current)
        else:
            ready = _fresh(evidence, now=current, max_age_seconds=max_age)
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
            now=current)
    except (topology_state.StaleProfileRevisionError, ValueError):
        return None
