"""Transport-neutral construction for bound non-pool launch requests."""

from __future__ import annotations

import dataclasses
import uuid

import sqlalchemy

from sky import models
from sky.serve import ordinary_launch_binding
from sky.server import versions
from sky.server.requests import executor
from sky.server.requests import non_pool_launch
from sky.server.requests import payloads
from sky.server.requests import postgres as request_postgres
from sky.server.requests import preconditions
from sky.server.requests import request_names
from sky.server.requests import requests as api_requests
from sky.skylet import constants as skylet_constants

_SERVER_OWNED_CONTEXT_KEYS = (
    ordinary_launch_binding.SUBMISSION_ID_KEY,
    ordinary_launch_binding.ASSOCIATION_ID_KEY,
    ordinary_launch_binding.LAUNCH_GENERATION_KEY,
    ordinary_launch_binding.BOUND_REQUEST_ID_KEY,
    ordinary_launch_binding.INPUT_DIGEST_KEY,
    ordinary_launch_binding.OWNER_REVISION_KEY,
    ordinary_launch_binding.BINDING_PROTOCOL_VERSION_KEY,
    ordinary_launch_binding.PROFILE_KIND_KEY,
    ordinary_launch_binding.PROFILE_VERSION_KEY,
    ordinary_launch_binding.PROFILE_DIGEST_KEY,
    ordinary_launch_binding.CAPABILITY_COHORT_EPOCH_KEY,
    ordinary_launch_binding.CAPABILITY_PROFILE_SET_DIGEST_KEY,
    ordinary_launch_binding.RECEIPT_PROTOCOL_VERSION_KEY,
    ordinary_launch_binding.AUTHORIZATION_KIND_KEY,
    ordinary_launch_binding.AUTHORIZATION_REFERENCE_KEY,
    ordinary_launch_binding.AUTHORIZATION_GENERATION_KEY,
    ordinary_launch_binding.AUTHORIZATION_DIGEST_KEY,
)


@dataclasses.dataclass(frozen=True)
class AdmissionAuthority:
    """Identity and capability authority resolved by an entrypoint."""

    tenant_id: str
    creator_name: str
    service_workspace: str
    capability_cohort_epoch: int
    capability_profile_set_digest: str
    receipt_protocol_version: int


@dataclasses.dataclass(frozen=True)
class BuiltAdmission:
    request: api_requests.Request
    identity: ordinary_launch_binding.NonPoolBindingIdentity


def build(
    launch_body: payloads.LaunchBody,
    submission_id: uuid.UUID,
    profile: ordinary_launch_binding.NonPoolLaunchProfile,
    authority: AdmissionAuthority,
    *,
    auth_user: models.User | None,
    client_api_version: int | None = None,
) -> BuiltAdmission:
    """Build the canonical request/identity suffix without persistence."""
    if (not isinstance(submission_id, uuid.UUID) or not isinstance(
            profile, ordinary_launch_binding.NonPoolLaunchProfile) or
            not isinstance(authority, AdmissionAuthority)):
        raise ValueError('Non-pool launch admission input is malformed.')
    if (not authority.tenant_id or not authority.creator_name or
            not authority.service_workspace):
        raise ValueError('Non-pool launch admission authority is incomplete.')
    if auth_user is not None and (auth_user.id != authority.tenant_id or
                                  auth_user.name != authority.creator_name):
        raise ValueError('Authenticated non-pool launch identity does not '
                         'match admission authority.')
    if any(key in launch_body.extra_launch_context
           for key in _SERVER_OWNED_CONTEXT_KEYS):
        raise ValueError('Non-pool binding identity must be server-generated.')
    intent = ordinary_launch_binding.parse_unbound_launch_context(
        launch_body.extra_launch_context)
    # Preserve the established submission identity across rolling API
    # upgrades: hash the exact prepared body before request construction
    # normalizes authenticated identity fields and API metadata. Internal
    # callers that own those fields must stamp them before invoking build().
    input_digest = ordinary_launch_binding.canonical_launch_digest(launch_body)
    launch_body.env_vars[skylet_constants.USER_ID_ENV_VAR] = authority.tenant_id
    launch_body.env_vars[skylet_constants.USER_ENV_VAR] = authority.creator_name
    effective_client_api_version = (versions.get_remote_api_version()
                                    if client_api_version is None else
                                    client_api_version)
    launch_body.client_api_version = effective_client_api_version
    identity = ordinary_launch_binding.build_non_pool_binding_identity(
        intent,
        submission_id=submission_id,
        tenant_scope=authority.tenant_id,
        service_workspace=authority.service_workspace,
        cluster_name=launch_body.cluster_name,
        input_digest=input_digest,
        profile=profile,
        capability_cohort_epoch=authority.capability_cohort_epoch,
        capability_profile_set_digest=(authority.capability_profile_set_digest),
        receipt_protocol_version=authority.receipt_protocol_version)
    request = executor._build_request(  # pylint: disable=protected-access
        request_id=identity.request_id,
        request_name=request_names.RequestName.CLUSTER_LAUNCH,
        request_body=launch_body,
        func=non_pool_launch.launch,
        request_cluster_name=launch_body.cluster_name,
        schedule_type=api_requests.ScheduleType.LONG,
        auth_user=auth_user,
        retryable=False,
        should_enqueue=True,
        precondition=preconditions.OrdinaryLaunchBindingPrecondition(
            identity.request_id, str(identity.association_id)),
        client_api_version=effective_client_api_version)
    if (request.user_id != authority.tenant_id or
            request.request_body.env_vars[skylet_constants.USER_ID_ENV_VAR]
            != authority.tenant_id or
            request.request_body.env_vars[skylet_constants.USER_ENV_VAR]
            != authority.creator_name):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Request identity does not match non-pool admission authority.')
    return BuiltAdmission(request=request, identity=identity)


def bind_in_transaction(
    connection: sqlalchemy.engine.Connection,
    admission: BuiltAdmission,
) -> ordinary_launch_binding.BindingAdmission:
    """Bind the canonical suffix on a caller-owned PostgreSQL transaction."""
    result = request_postgres.bind_and_enqueue_non_pool_launch_in_transaction(
        connection, admission.request, admission.identity)
    validate_result(result, admission)
    return result


def validate_result(
    result: ordinary_launch_binding.BindingAdmission,
    admission: BuiltAdmission,
) -> None:
    """Validate one backend receipt against the canonical derived identity."""
    if (not isinstance(result, ordinary_launch_binding.BindingAdmission) or
            result.association_id != str(admission.identity.association_id) or
            result.request_id != admission.identity.request_id or
            type(result.launch_generation) is not int or
            result.launch_generation < 1 or type(result.created) is not bool):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Transactional non-pool binding returned an inconsistent '
            'identity.')
