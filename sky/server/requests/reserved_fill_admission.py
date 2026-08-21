"""Atomic reserved-fill replica and executable-request admission."""

from __future__ import annotations

import dataclasses
import enum
from typing import Any
import uuid

import sqlalchemy

from sky import global_user_state_schema
from sky.client import sdk
from sky.serve import ordinary_launch_binding
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation
from sky.server import constants as server_constants
from sky.server.requests import non_pool_admission
from sky.server.requests import postgres as request_postgres
from sky.skylet import constants as skylet_constants


class AdmissionDisposition(enum.Enum):
    COMMITTED = 'COMMITTED'
    REJECTED = 'REJECTED'
    AMBIGUOUS = 'AMBIGUOUS'


@dataclasses.dataclass(frozen=True)
class AdmissionSpec:
    prepared_request: sdk.PreparedLaunchRequest
    submission_id: uuid.UUID
    authority: ordinary_launch_binding.ControllerBindingAuthority
    replica_info: Any
    actuation_lease: zero_cost_actuation.IntentLease


@dataclasses.dataclass(frozen=True)
class AdmissionReceipt:
    replica_id: int
    replica_record_id: str
    association_id: str
    request_id: str
    launch_generation: int


@dataclasses.dataclass(frozen=True)
class AdmissionResult:
    disposition: AdmissionDisposition
    receipt: AdmissionReceipt | None = None
    detail: str | None = None


class _Rejected(RuntimeError):
    pass


class AdmissionAmbiguousError(RuntimeError):
    """Caller must preserve the durable intent for exact later hydration."""


def _frozen_identity(
    spec: AdmissionSpec,) -> tuple[Any, ordinary_launch_binding.BindingIntent]:
    if (not isinstance(spec.prepared_request, sdk.PreparedLaunchRequest) or
            not isinstance(spec.submission_id, uuid.UUID) or
            not isinstance(spec.authority,
                           ordinary_launch_binding.ControllerBindingAuthority)
            or not isinstance(spec.actuation_lease,
                              zero_cost_actuation.IntentLease)):
        raise ValueError('Reserved-fill admission input is malformed.')
    authority = spec.authority
    if (authority.capable is not True or authority.binding_mode
            is not ordinary_launch_binding.BindingMode.BOUND or
            not authority.generic_launches_required):
        raise ValueError('Reserved-fill admission requires generic authority.')
    body = spec.prepared_request.body
    if (not body.is_launched_by_sky_serve_controller or body.dryrun or
            body.down or body.clone_disk_from is not None or
            body.file_mounts_blob_id is not None or
            body.override_skypilot_config_path is not None):
        raise ValueError(
            'Reserved-fill launch is not server-only executable work.')
    override = body.override_skypilot_config
    if (not isinstance(override, dict) or
            override.get('active_workspace') != authority.service_workspace):
        raise ValueError('Reserved-fill launch workspace is not immutable.')
    intent = ordinary_launch_binding.parse_unbound_launch_context(
        body.extra_launch_context)
    if (intent.service_name != authority.service_name or
            intent.service_hash != authority.service_hash or
            intent.lifecycle_epoch != authority.service_lifecycle_epoch or
            intent.binding_epoch != authority.binding_epoch or
            intent.controller_incarnation != authority.controller_incarnation or
            intent.controller_owner_epoch != authority.controller_owner_epoch or
            intent.controller_pid != authority.controller_pid or
            intent.controller_ip != authority.controller_ip or
            intent.replica_id != spec.replica_info.replica_id or
            str(intent.replica_record_id)
            != spec.replica_info.replica_record_id):
        raise ValueError('Frozen launch does not match controller authority.')
    return body, intent


def _stage_and_bind_in_savepoint(
    connection: sqlalchemy.engine.Connection,
    spec: AdmissionSpec,
    lease_token: int | None,
    *,
    require_existing: bool,
) -> tuple[serve_state.StagedReservedFillReplica, AdmissionReceipt]:
    staged = (
        serve_state.stage_protocol_v2_reserved_fill_replica_in_transaction(
            connection,
            spec.authority.service_name,
            spec.replica_info.replica_id,
            spec.replica_info,
            pool_key=spec.replica_info.reserved_fill_pool_key,
            expected_epoch=spec.actuation_lease.intent.pool_epoch,
            expected_service_hash=spec.authority.service_hash,
            expected_controller_owner=(spec.authority.controller_pid,
                                       spec.authority.controller_ip),
            expected_service_generation=(
                spec.replica_info.reserved_fill_service_generation),
            expected_physical_cluster_uid=(
                spec.replica_info.reserved_fill_physical_cluster_uid),
            expected_ordinary_zero_cost_admission_sequence=(
                spec.actuation_lease.intent.
                ordinary_zero_cost_admission_sequence),
            expected_lease_token=lease_token,
            expected_actuation_mode=(
                zero_cost_actuation.ActuationMode.DURABLE_INTENT.value),
            actuation_lease=spec.actuation_lease))
    if staged is None:
        raise _Rejected('Reserved-fill materialization authority was rejected.')
    if require_existing != staged.already_committed:
        raise AdmissionAmbiguousError(
            'Lost-ACK hydration found a partial handoff.')

    body, _ = _frozen_identity(spec)
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            spec.authority.service_name)).mappings().one()
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            spec.authority.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            spec.replica_info.replica_id)).mappings().one()
    tenant = service.get('owner_user_id')
    attested_creator = service.get('owner_user_name')
    if (not isinstance(tenant, str) or not tenant or
            not isinstance(attested_creator, str) or not attested_creator):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'The durable service has no attested owner identity.')
    owner = connection.execute(
        sqlalchemy.select(
            global_user_state_schema.user_table.c.id,
            global_user_state_schema.user_table.c.name).where(
                global_user_state_schema.user_table.c.id ==
                tenant).with_for_update(read=True)).mappings().one_or_none()
    current_creator = None if owner is None else owner['name']
    if (owner is None or owner['id'] != tenant or
            not isinstance(current_creator, str) or not current_creator or
            service['workspace'] != spec.authority.service_workspace):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Launch tenant/workspace does not match the durable service owner.')
    profile = ordinary_launch_binding.resolve_non_pool_launch_profile_in_connection(
        connection, service, replica)
    if profile.kind is not ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Atomic fill admission resolved a non-fill launch profile.')
    cohort_epoch = spec.authority.non_pool_capability_cohort_epoch
    profile_set_digest = spec.authority.non_pool_profile_set_digest
    receipt_protocol_version = (
        spec.authority.non_pool_receipt_protocol_version)
    if (type(cohort_epoch) is not int or
            not isinstance(profile_set_digest, str) or
            type(receipt_protocol_version) is not int):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Atomic fill admission authority is incomplete.')
    # There is no client transport for atomic fill. Stamp every trusted field
    # before the shared builder computes its pre-normalization digest so the
    # submission identity also equals the durable executable-body digest.
    body.env_vars[skylet_constants.USER_ID_ENV_VAR] = tenant
    body.env_vars[skylet_constants.USER_ENV_VAR] = attested_creator
    body.client_api_version = server_constants.API_VERSION
    built = non_pool_admission.build(
        body,
        spec.submission_id,
        profile,
        non_pool_admission.AdmissionAuthority(
            tenant_id=tenant,
            # The durable body/digest must survive a later display-name
            # change and exact lost-ACK hydration.  Execution resolves the
            # current users.name by immutable tenant ID without upserting this
            # attested audit value back into the users table.
            creator_name=attested_creator,
            service_workspace=spec.authority.service_workspace,
            capability_cohort_epoch=cohort_epoch,
            capability_profile_set_digest=profile_set_digest,
            receipt_protocol_version=receipt_protocol_version),
        auth_user=None,
        client_api_version=server_constants.API_VERSION)
    if (ordinary_launch_binding.canonical_launch_digest(
            built.request.request_body) != built.identity.input_digest):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Atomic fill request body does not match its canonical digest.')
    admission = non_pool_admission.bind_in_transaction(connection, built)
    if require_existing and admission.created:
        raise AdmissionAmbiguousError(
            'Lost-ACK hydration found no existing request.')
    return staged, AdmissionReceipt(
        replica_id=staged.replica_id,
        replica_record_id=staged.persisted_info.replica_record_id,
        association_id=admission.association_id,
        request_id=admission.request_id,
        launch_generation=admission.launch_generation)


def _stage_and_bind(
    connection: sqlalchemy.engine.Connection,
    spec: AdmissionSpec,
    lease_token: int | None,
    *,
    require_existing: bool,
) -> tuple[serve_state.StagedReservedFillReplica, AdmissionReceipt]:
    """Stage the complete tuple behind one caller-usable savepoint."""
    savepoint = connection.begin_nested()
    try:
        result = _stage_and_bind_in_savepoint(connection,
                                              spec,
                                              lease_token,
                                              require_existing=require_existing)
    except BaseException as error:
        try:
            savepoint.rollback()
        except BaseException as rollback_error:
            if not isinstance(error, Exception):
                raise error from rollback_error
            if not isinstance(rollback_error, Exception):
                raise
            raise AdmissionAmbiguousError(
                'Atomic admission savepoint rollback was not acknowledged.'
            ) from rollback_error
        raise
    try:
        savepoint.commit()
    except BaseException as commit_error:
        if not isinstance(commit_error, Exception):
            raise
        raise AdmissionAmbiguousError(
            'Atomic admission savepoint release was not acknowledged.'
        ) from commit_error
    return result


def _transaction(
    spec: AdmissionSpec, lease_token: int | None, *, require_existing: bool
) -> tuple[serve_state.StagedReservedFillReplica, AdmissionReceipt]:
    engine = request_postgres.initialize_and_get_db()
    connection = engine.connect()
    transaction_error: BaseException | None = None
    staged_receipt: (tuple[serve_state.StagedReservedFillReplica,
                           AdmissionReceipt] | None) = None
    try:
        transaction = connection.begin()
        try:
            staged_receipt = _stage_and_bind(connection,
                                             spec,
                                             lease_token,
                                             require_existing=require_existing)
        except BaseException as error:
            try:
                transaction.rollback()
            except BaseException as rollback_error:
                if not isinstance(error, Exception):
                    raise error from rollback_error
                if not isinstance(rollback_error, Exception):
                    raise
                raise AdmissionAmbiguousError(
                    'Reserved-fill admission rollback was not acknowledged.'
                ) from rollback_error
            if not isinstance(error, Exception):
                raise
            if require_existing or isinstance(error, AdmissionAmbiguousError):
                raise
            raise _Rejected(str(error)) from error
        try:
            transaction.commit()
        except BaseException as error:
            if not isinstance(error, Exception):
                raise
            raise AdmissionAmbiguousError(
                'Reserved-fill admission commit was not acknowledged.'
            ) from error
        assert staged_receipt is not None
        return staged_receipt
    except BaseException as error:
        transaction_error = error
        raise
    finally:
        try:
            connection.close()
        except BaseException as close_error:
            # Preserve KeyboardInterrupt/SystemExit from any transaction
            # boundary.  admit() must hydrate the durable tuple and then
            # re-raise that signal; a close ACK failure cannot replace it.
            if (transaction_error is not None and
                    not isinstance(transaction_error, Exception)):
                raise transaction_error from close_error
            raise


def admit(spec: AdmissionSpec) -> AdmissionResult:
    """Commit one complete tuple, returning a closed three-way result."""
    try:
        _frozen_identity(spec)
        if not request_postgres.non_pool_launch_binding_fleet_capable():
            raise _Rejected('The generic request fleet is not yet capable.')
    except BaseException as error:  # pylint: disable=broad-exception-caught
        if not isinstance(error, Exception):
            raise
        # No transaction has begun, so every failure is a definite rejection.
        return AdmissionResult(AdmissionDisposition.REJECTED, detail=str(error))
    deferred_interrupt: BaseException | None = None
    try:
        staged, receipt = reserved_capacity_broker.run_fill_persist_transaction(
            lambda token: _transaction(spec, token, require_existing=False))
    except (_Rejected,
            reserved_capacity_broker.ReservedFillPersistRejected) as error:
        return AdmissionResult(AdmissionDisposition.REJECTED, detail=str(error))
    except BaseException as commit_error:  # pylint: disable=broad-except
        if not isinstance(commit_error, Exception):
            deferred_interrupt = commit_error
        try:
            staged, receipt = (
                reserved_capacity_broker.run_fill_persist_transaction(
                    lambda token: _transaction(
                        spec, token, require_existing=True)))
        except BaseException as read_error:  # pylint: disable=broad-except
            if deferred_interrupt is not None:
                # Once an operator interrupt crosses an uncertain commit
                # boundary, hydration is evidence collection only.  A second
                # interrupt or teardown failure cannot replace the original
                # signal that the caller must observe.
                raise deferred_interrupt from read_error
            if not isinstance(read_error, Exception):
                raise
            return AdmissionResult(
                AdmissionDisposition.AMBIGUOUS,
                detail=(f'{type(commit_error).__name__}; '
                        f'hydration={type(read_error).__name__}'))
    try:
        staged.publish_after_commit()
    except BaseException as error:  # pylint: disable=broad-except
        if deferred_interrupt is not None:
            raise deferred_interrupt from error
        if not isinstance(error, Exception):
            raise
        return AdmissionResult(AdmissionDisposition.AMBIGUOUS,
                               receipt=receipt,
                               detail=f'postcommit={type(error).__name__}')
    if deferred_interrupt is not None:
        raise deferred_interrupt
    return AdmissionResult(AdmissionDisposition.COMMITTED, receipt=receipt)
