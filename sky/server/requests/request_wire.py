"""Stateless client-wire projections for API requests."""

from collections.abc import Callable
from typing import Any, TypeVar

import orjson

from sky.server.requests import payloads

_RequestT = TypeVar('_RequestT')


def status_value_for_client(
    status_value: str,
    *,
    waiting_status_value: str,
    running_status_value: str,
    get_remote_api_version: Callable[[], int | None],
    min_waiting_status_api_version: int,
) -> str:
    """Map a status to the representation understood by the client."""
    remote_api_version = get_remote_api_version()
    if (status_value == waiting_status_value and
            remote_api_version is not None and
            remote_api_version < min_waiting_status_api_version):
        return running_status_value
    return status_value


def encode_request(
    request: Any,
    *,
    validate_request_body: Callable[[payloads.RequestBody], None],
    get_serializer: Callable[[str], Callable[[Any], str]],
    pickle_and_encode: Callable[[Any], str],
    project_status: Callable[[str], str],
    logger: Any,
) -> payloads.RequestPayload:
    """Serialize one request for the full client wire format."""
    assert isinstance(request.request_body,
                      payloads.RequestBody), (request.name,
                                              request.request_body)
    # Pydantic validates normal request construction, but this final fence
    # also covers internally constructed or restored bodies before their task
    # text can enter the durable request row.
    validate_request_body(request.request_body)
    try:
        serializer = get_serializer(request.name)
        return payloads.RequestPayload(
            request_id=request.request_id,
            name=request.name,
            entrypoint=pickle_and_encode(request.entrypoint),
            request_body=pickle_and_encode(request.request_body),
            status=project_status(request.status.value),
            return_value=serializer(request.return_value),
            error=orjson.dumps(request.error).decode('utf-8'),
            pid=request.pid,
            created_at=request.created_at,
            schedule_type=request.schedule_type.value,
            user_id=request.user_id,
            cluster_name=request.cluster_name,
            status_msg=request.status_msg,
            should_retry=request.should_retry,
            finished_at=request.finished_at,
            file_mounts_blob_id=request.file_mounts_blob_id,
        )
    except (TypeError, ValueError) as e:
        # The error is unexpected, so we don't suppress the stack trace.
        logger.error(
            f'Error encoding: {e}\n'
            f'  {request.request_id}\n'
            f'  {request.name}\n'
            f'  {request.request_body}\n'
            f'  {request.return_value}\n'
            f'  {request.created_at}\n',
            exc_info=e)
        raise


def decode_entrypoint(
    encoded_entrypoint: str,
    *,
    decode_and_unpickle: Callable[[str], Callable],
    unresolved_entrypoint: Callable,
    logger: Any,
) -> Callable:
    """Decode an entrypoint, tolerating an unresolvable reference."""
    try:
        return decode_and_unpickle(encoded_entrypoint)
    except (AttributeError, ImportError) as e:
        logger.debug(
            'Could not resolve the request entrypoint while decoding '
            f'(likely a client/server version skew): {e}. The entrypoint '
            'is not used on the client, so falling back to a placeholder.')
        return unresolved_entrypoint


def decode_request(
    payload: payloads.RequestPayload,
    *,
    request_factory: Callable[..., _RequestT],
    entrypoint_decoder: Callable[[str], Callable],
    decode_and_unpickle: Callable[[str], Any],
    status_cls: Callable[[str], Any],
    schedule_type_cls: Callable[[str], Any],
    logger: Any,
) -> _RequestT:
    """Deserialize one request from the full client wire format."""
    try:
        return request_factory(
            request_id=payload.request_id,
            name=payload.name,
            entrypoint=entrypoint_decoder(payload.entrypoint),
            request_body=decode_and_unpickle(payload.request_body),
            status=status_cls(payload.status),
            return_value=orjson.loads(payload.return_value),
            error=orjson.loads(payload.error),
            pid=payload.pid,
            created_at=payload.created_at,
            schedule_type=schedule_type_cls(payload.schedule_type),
            user_id=payload.user_id,
            cluster_name=payload.cluster_name,
            status_msg=payload.status_msg,
            should_retry=payload.should_retry,
            finished_at=payload.finished_at,
            file_mounts_blob_id=payload.file_mounts_blob_id,
        )
    except (TypeError, ValueError) as e:
        logger.error(
            f'Error decoding: {e}\n'
            f'  {payload.request_id}\n'
            f'  {payload.name}\n'
            f'  {payload.entrypoint}\n'
            f'  {payload.request_body}\n'
            f'  {payload.created_at}\n',
            exc_info=e)
        # The error is unexpected, so we don't suppress the stack trace.
        raise


def encode_requests(
    requests: list[Any],
    *,
    get_all_users: Callable[[], list[Any]],
    project_status: Callable[[str], str],
) -> list[payloads.RequestPayload]:
    """Serialize requests for the compact client display format."""
    encoded_requests = []
    all_users = get_all_users()
    all_users_map = {user.id: user.name for user in all_users}
    for request in requests:
        if request.request_body is not None:
            assert isinstance(request.request_body,
                              payloads.RequestBody), (request.name,
                                                      request.request_body)
        user_name = all_users_map.get(request.user_id)
        payload = payloads.RequestPayload(
            request_id=request.request_id,
            name=request.name,
            entrypoint=request.entrypoint.__name__
            if request.entrypoint is not None else '',
            request_body=request.request_body.model_dump_json()
            if request.request_body is not None else
            orjson.dumps(None).decode('utf-8'),
            status=project_status(request.status.value),
            return_value=orjson.dumps(None).decode('utf-8'),
            error=orjson.dumps(None).decode('utf-8'),
            pid=None,
            created_at=request.created_at,
            schedule_type=request.schedule_type.value,
            user_id=request.user_id,
            user_name=user_name,
            cluster_name=request.cluster_name,
            status_msg=request.status_msg,
            should_retry=request.should_retry,
            finished_at=request.finished_at,
            file_mounts_blob_id=request.file_mounts_blob_id,
        )
        encoded_requests.append(payload)
    return encoded_requests
