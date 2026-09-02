"""Fused executable-request binding for accepted paid launch candidates."""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
import dataclasses
import pathlib
from typing import Any
import uuid

import prometheus_client as prom
import sqlalchemy

from sky import global_user_state_schema
from sky.client import sdk
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import serve_state_schema
from sky.server import constants as server_constants
from sky.server.requests import non_pool_admission
from sky.server.requests import postgres as request_postgres
from sky.skylet import constants as skylet_constants
from sky.utils.db import db_utils

ADMISSION_EVENTS = prom.Counter(
    'sky_serve_paid_wave_admission_events_total',
    'Fused paid-wave request-materialization outcomes.', ('outcome',))


@dataclasses.dataclass(frozen=True)
class FusedBindingReceiptMember:
    """One executable identity committed with its paid replica and claim."""

    replica_id: int
    replica_record_id: str
    submission_id: uuid.UUID
    association_id: str
    request_id: str
    launch_generation: int
    context: ordinary_launch_binding.BoundNonPoolLaunchContext
    request_log_path: pathlib.Path


def record_singleton_paid_compatibility_use() -> None:
    """Instrument the temporary ordinary-paid singleton HTTP path."""
    ADMISSION_EVENTS.labels(outcome='singleton_paid_compatibility').inc()


def record_fused_commit(member_count: int) -> None:
    """Record members only after the enclosing capacity transaction commits."""
    if type(member_count) is not int or member_count < 0:  # pylint: disable=unidiomatic-typecheck
        raise ValueError('Fused paid member count must be non-negative.')
    if member_count:
        ADMISSION_EVENTS.labels(outcome='fused_committed').inc(member_count)


def _prepared_body(
    spec: paid_capacity.PaidLaunchSpec,
    authority: ordinary_launch_binding.ControllerBindingAuthority,
) -> Any:
    try:
        prepared = sdk.PreparedLaunchRequest(
            submitted_bytes=spec.prepared_launch_request)
    except (TypeError, ValueError) as error:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid launch executable bytes are malformed.') from error
    body = prepared.body
    if (not body.is_launched_by_sky_serve_controller or
            body.is_launched_by_jobs_controller or body.dryrun or body.down or
            body.clone_disk_from is not None or
            body.file_mounts_blob_id is not None or
            body.override_skypilot_config_path is not None or
            body.cluster_name != spec.cluster_name_seed or body.env_vars or
            body.client_api_version != server_constants.API_VERSION):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid launch is not one immutable server-only executable body.')
    override = body.override_skypilot_config
    if (not isinstance(override, dict) or
            override.get('active_workspace') != authority.service_workspace):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid launch executable workspace is not immutable.')
    intent = ordinary_launch_binding.parse_unbound_launch_context(
        body.extra_launch_context)
    if (intent.service_name != spec.service_name or
            intent.service_hash != spec.service_hash or
            intent.service_version != spec.service_version or
            intent.replica_id != spec.replica_id or
            str(intent.replica_record_id) != spec.replica_record_id or
            intent.lifecycle_epoch != spec.service_lifecycle_epoch or
            intent.binding_epoch != authority.binding_epoch or
            intent.controller_incarnation != authority.controller_incarnation or
            intent.controller_owner_epoch != authority.controller_owner_epoch or
            intent.controller_pid != authority.controller_pid or
            intent.controller_ip != authority.controller_ip):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid launch executable does not match its candidate identity.')
    return body


def validate_prepared_specs(
    specs: Sequence[paid_capacity.PaidLaunchSpec],
    authority: ordinary_launch_binding.ControllerBindingAuthority,
) -> None:
    """Validate immutable executable bytes before paid arbitration."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise ValueError('Paid launch binding authority is malformed.')
    for spec in specs:
        _prepared_body(spec, authority)


def _service_owner(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
) -> tuple[str, str]:
    tenant_id = service.get('owner_user_id')
    attested_creator = service.get('owner_user_name')
    if (not isinstance(tenant_id, str) or not tenant_id or
            not isinstance(attested_creator, str) or not attested_creator):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid launch service has no attested owner identity.')
    owner = connection.execute(
        sqlalchemy.select(
            global_user_state_schema.user_table.c.id,
            global_user_state_schema.user_table.c.name).where(
                global_user_state_schema.user_table.c.id ==
                tenant_id).with_for_update(read=True)).mappings().one_or_none()
    if (owner is None or owner['id'] != tenant_id or
            not isinstance(owner['name'], str) or not owner['name']):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Paid launch service owner is no longer valid.')
    return tenant_id, attested_creator


def bind_accepted_in_transaction(
    connection: sqlalchemy.engine.Connection,
    *,
    service: Mapping[str, Any],
    authority: ordinary_launch_binding.ControllerBindingAuthority,
    accepted: Sequence[tuple[paid_capacity.PaidLaunchReceiptMember,
                             paid_capacity.PaidLaunchSpec]],
) -> tuple[FusedBindingReceiptMember, ...]:
    """Join accepted paid rows to executable requests before the same commit."""
    if (connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value
            or not connection.in_transaction()):
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Fused paid launch binding requires an active PostgreSQL '
            'transaction.')
    accepted = tuple(accepted)
    if (not authority.generic_launches_required or
            service.get('name') != authority.service_name or
            service.get('hash') != authority.service_hash or
            service.get('workspace') != authority.service_workspace or
            service.get('lifecycle_epoch')
            != authority.service_lifecycle_epoch):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Fused paid launch authority is stale.')
    if accepted and not request_postgres.non_pool_launch_binding_fleet_capable(
            connection=connection):
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'The generic request fleet is not yet capable.')
    tenant_id, creator_name = _service_owner(connection, service)
    admission_authority = non_pool_admission.AdmissionAuthority(
        tenant_id=tenant_id,
        creator_name=creator_name,
        service_workspace=authority.service_workspace,
        capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))

    receipts = []
    for member, spec in accepted:
        if ((member.replica_id, member.replica_record_id, member.pool_key,
             member.accelerator, member.physical_gpu_units)
                != (spec.replica_id, spec.replica_record_id, spec.pool_key,
                    spec.accelerator, spec.physical_gpu_units)):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Paid receipt and executable candidate disagree.')
        body = _prepared_body(spec, authority)
        body.env_vars[skylet_constants.USER_ID_ENV_VAR] = tenant_id
        body.env_vars[skylet_constants.USER_ENV_VAR] = creator_name
        body.client_api_version = server_constants.API_VERSION
        submission_id = uuid.UUID(
            request_postgres.
            stable_bound_ordinary_launch_submission_id_in_connection(
                connection, spec.service_name, spec.replica_id,
                spec.replica_record_id))
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                spec.service_name,
                serve_state_schema.replicas_table.c.replica_id ==
                spec.replica_id)).mappings().one_or_none()
        if replica is None:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Accepted paid launch lost its replica row.')
        profile = ordinary_launch_binding.resolve_non_pool_launch_profile_in_connection(
            connection, service, replica, protocol_and_service_prelocked=True)
        if profile.kind is not (
                ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Accepted paid launch resolved a non-paid profile.')
        built = non_pool_admission.build(
            body,
            submission_id,
            profile,
            admission_authority,
            auth_user=None,
            client_api_version=server_constants.API_VERSION)
        if ordinary_launch_binding.canonical_launch_digest(
                built.request.request_body) != built.identity.input_digest:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Fused paid request body does not match its canonical digest.')
        admission = non_pool_admission.bind_in_transaction(connection, built)
        if not admission.created:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Fresh paid capacity resolved an existing launch request.')
        context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
            built.request.request_body.extra_launch_context)
        receipts.append(
            FusedBindingReceiptMember(
                replica_id=member.replica_id,
                replica_record_id=member.replica_record_id,
                submission_id=submission_id,
                association_id=admission.association_id,
                request_id=admission.request_id,
                launch_generation=admission.launch_generation,
                context=context,
                request_log_path=built.request.log_path))
    return tuple(receipts)
