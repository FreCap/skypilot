"""Capability-fenced no-enqueue launch identity canonicalization endpoint."""

import asyncio
import json

import fastapi
import sqlalchemy

from sky.serve import resource_action_state
from sky.serve import resource_actions
from sky.server import common as server_common
from sky.server.requests import resource_actions as kernel_actions

router = fastapi.APIRouter()

_PATH = '/internal/resource-actions/v1/launch-identity/canonicalize'
_MAX_BODY_BYTES = 65_536


def _state_store(
) -> resource_action_state.PostgresServeResourceActionStateStore:
    return resource_action_state.PostgresServeResourceActionStateStore()


def _error(status_code: int, detail: str) -> fastapi.HTTPException:
    return fastapi.HTTPException(status_code=status_code, detail=detail)


async def _read_exact_json_body(request: fastapi.Request) -> bytes:
    if request.headers.get('content-type') != 'application/json':
        raise _error(415, 'Content-Type must be application/json.')
    if 'content-encoding' in request.headers:
        raise _error(415, 'Content-Encoding is not supported.')

    content_length = request.headers.get('content-length')
    if content_length is not None:
        try:
            parsed_content_length = int(content_length)
        except ValueError:
            raise _error(400, 'Content-Length is invalid.') from None
        if parsed_content_length < 0:
            raise _error(400, 'Content-Length is invalid.')
        if parsed_content_length > _MAX_BODY_BYTES:
            raise _error(413, 'Payload is too large.')

    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_BODY_BYTES:
            raise _error(413, 'Payload is too large.')
        body.extend(chunk)
    return bytes(body)


@router.post(_PATH, include_in_schema=False)
async def canonicalize_launch_identity(
        request: fastapi.Request) -> fastapi.Response:
    """Return a post-auth identity proof without enqueueing any request."""
    # InitializeRequestAuthUserMiddleware always installs this attribute,
    # including in legacy no-auth mode where its explicit value is None.  A
    # missing attribute is a middleware/configuration failure, not permission
    # to trust the submitted identity pair.
    auth_user = request.state.auth_user
    body = await _read_exact_json_body(request)
    try:
        decoded = json.loads(body.decode('utf-8'))
        typed_request = (
            resource_actions.ProviderLaunchIdentityCanonicalizationRequestV1.
            from_value(decoded))
        if typed_request.canonical_bytes != body:
            raise ValueError('request body is not canonical JSON.')
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError,
            ValueError):
        raise _error(400, 'Canonicalization request is invalid.') from None

    try:
        await asyncio.to_thread(
            _state_store().validate_launch_identity_canonicalization,
            typed_request)
    except resource_action_state.PreparationCapabilityMismatch:
        raise _error(403, 'Preparation capability is invalid.') from None
    except (kernel_actions.ActionConflict, kernel_actions.ClaimLost,
            kernel_actions.InvariantViolation, kernel_actions.StaleRevision):
        raise _error(409, 'Preparation context is stale or unequal.') from None
    except (sqlalchemy.exc.InterfaceError, sqlalchemy.exc.OperationalError,
            sqlalchemy.exc.DisconnectionError, sqlalchemy.exc.TimeoutError):
        raise _error(503,
                     'Preparation state is temporarily unavailable.') from None

    context = typed_request.context
    try:
        effective_original_user, effective_user_hash = (
            server_common.resolve_effective_request_identity(
                auth_user, context.input.prepared_original_user,
                context.input.prepared_user_hash))
        proof = resource_actions.ProviderLaunchIdentityCanonicalizationProofV1(
            version=1,
            boundary='api_server_post_auth_no_enqueue',
            context=context,
            context_sha256=typed_request.context_sha256,
            effective_original_user=effective_original_user,
            effective_user_hash=effective_user_hash)
        response = (
            resource_actions.ProviderLaunchIdentityCanonicalizationResponseV1(
                version=1,
                decision_id=context.decision_id,
                context_sha256=typed_request.context_sha256,
                proof=proof,
                proof_sha256=proof.sha256))
    except (TypeError, ValueError):
        raise _error(400, 'Effective request identity is invalid.') from None
    return fastapi.Response(content=response.canonical_bytes,
                            media_type='application/json')
