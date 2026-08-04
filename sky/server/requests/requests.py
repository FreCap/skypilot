"""Utilities for REST API."""
import asyncio
import atexit
from collections.abc import Callable
from collections.abc import Generator
import contextlib
import dataclasses
import enum
import functools
import os
import pathlib
import shutil
import signal
import sqlite3
import threading
import time
import traceback
from typing import Any, NamedTuple, NoReturn, TypeVar
import uuid

import anyio
import colorama
import filelock
import orjson

import sky
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.container_images import errors as container_image_errors
from sky.metrics import utils as metrics_lib
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import daemons
from sky.server import versions
from sky.server.blob import blob_storage as bs
from sky.server.requests import cutover as request_cutover
from sky.server.requests import payloads
from sky.server.requests import registry as request_registry
from sky.server.requests import request_names
from sky.server.requests import request_wire
from sky.server.requests import storage as request_storage
from sky.server.requests.serializers import decoders
from sky.server.requests.serializers import encoders
from sky.server.requests.serializers import return_value_serializers
from sky.skylet import constants as skylet_constants
from sky.utils import asyncio_utils
from sky.utils import common_utils
from sky.utils import status_lib
from sky.utils import ux_utils
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

_ErrorT = TypeVar('_ErrorT', bound=BaseException)


def _unresolved_entrypoint(*args: Any, **kwargs: Any) -> NoReturn:
    """Placeholder for a request entrypoint that could not be unpickled.

    Used by ``Request.decode`` when the encoded entrypoint references a symbol
    this (older) client does not have. The entrypoint is never invoked on the
    client; this only guards the unlikely case of someone calling it.
    """
    raise RuntimeError(
        'This request entrypoint could not be resolved on the client, likely '
        'due to a client/server version mismatch. Upgrade the SkyPilot client '
        'to match the API server version.')


# Tables in task.db.
REQUEST_TABLE = 'requests'
COL_CLUSTER_NAME = 'cluster_name'
COL_USER_ID = 'user_id'
COL_STATUS_MSG = 'status_msg'
COL_SHOULD_RETRY = 'should_retry'
COL_FINISHED_AT = 'finished_at'
COL_FILE_MOUNTS_BLOB_ID = 'file_mounts_blob_id'
# Enqueue flags, persisted so that queued requests can be re-enqueued after
# a server restart. Not part of RequestPayload: they are server-internal and
# never sent to clients.
COL_IGNORE_RETURN_VALUE = 'ignore_return_value'
COL_RETRYABLE = 'retryable'
DURABLE_PAYLOAD_FORMAT = 'pydantic-json'
DURABLE_PAYLOAD_VERSION = 1
# Legacy path for backward compatibility - GC will clean up logs from both
# the new and legacy paths to handle server upgrades gracefully.
LEGACY_REQUEST_LOG_PATH_PREFIX = '~/sky_logs/api_server/requests'

DEFAULT_REQUESTS_RETENTION_HOURS = 24  # 1 day
# Interval between requests GC runs. Retention only controls the age cutoff
# of the rows being cleaned; the GC itself always runs at this cadence so
# the table does not grow unboundedly between runs under high request rates.
_GC_INTERVAL_SECONDS = 3600
_REQUEST_LOG_PRESSURE_CHECK_INTERVAL_SECONDS = 10
_REQUEST_LOG_PRESSURE_CLEANUP_INTERVAL_SECONDS = 300
_REQUEST_LOG_HARD_PRESSURE_CLEANUP_INTERVAL_SECONDS = 60
_REQUEST_LOG_PRESSURE_CLEANUP_GRACE_SECONDS = 5
_GIB = 1024 * 1024 * 1024
_REQUEST_LOG_SOFT_FREE_FRACTION = 0.10
_REQUEST_LOG_HARD_FREE_FRACTION = 0.05
_REQUEST_LOG_SOFT_FREE_MIN_BYTES = 2 * _GIB
_REQUEST_LOG_SOFT_FREE_MAX_BYTES = 20 * _GIB
_REQUEST_LOG_HARD_FREE_MIN_BYTES = 1 * _GIB
_REQUEST_LOG_HARD_FREE_MAX_BYTES = 10 * _GIB

# These requests proxy remote logs through their local request log. Their
# completed rows are the safest first target when the shared filesystem is
# under pressure: deleting them preserves active streams and ordinary request
# history while removing duplicated transport data.
STREAMING_REQUEST_NAMES = tuple(server_constants.REQUEST_NAME_PREFIX +
                                name.value for name in (
                                    request_names.RequestName.CLUSTER_JOB_LOGS,
                                    request_names.RequestName.JOBS_LOGS,
                                    request_names.RequestName.JOBS_POOL_LOGS,
                                    request_names.RequestName.SERVE_LOGS,
                                ))


class RequestLogStorageUsage(NamedTuple):
    """Filesystem usage and pressure thresholds for API request logs."""

    free_bytes: int
    soft_free_bytes: int
    hard_free_bytes: int


def _clamp(value: int, minimum: int, maximum: int) -> int:
    return min(maximum, max(minimum, value))


def get_request_log_storage_usage() -> RequestLogStorageUsage:
    """Return an O(1) request-log filesystem pressure snapshot."""
    log_dir = pathlib.Path(
        server_constants.REQUEST_LOG_PATH_PREFIX).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(log_dir)
    soft_free_bytes = _clamp(int(usage.total * _REQUEST_LOG_SOFT_FREE_FRACTION),
                             _REQUEST_LOG_SOFT_FREE_MIN_BYTES,
                             _REQUEST_LOG_SOFT_FREE_MAX_BYTES)
    hard_free_bytes = _clamp(int(usage.total * _REQUEST_LOG_HARD_FREE_FRACTION),
                             _REQUEST_LOG_HARD_FREE_MIN_BYTES,
                             _REQUEST_LOG_HARD_FREE_MAX_BYTES)
    return RequestLogStorageUsage(usage.free, soft_free_bytes, hard_free_bytes)


# Escape hatch: set to '1' to restore the legacy behavior of wiping the
# request DB and logs on API server startup instead of recovering them.
RESET_REQUESTS_ON_STARTUP_ENV_VAR = 'SKYPILOT_RESET_REQUESTS_ON_STARTUP'

# Request names whose entrypoints are safe to re-execute from scratch after a
# server restart, provided their cluster is still INIT (see
# _find_interrupted_launches_to_requeue). Graceful shutdown leaves these rows
# RUNNING instead of cancelling them, and startup recovery requeues them, so
# a server redeploy completes an in-flight provisioning instead of dropping
# it.
# Request rows persist the prefixed name (executor stamps
# REQUEST_NAME_PREFIX + request_name at creation), so match that form.
REPLAYABLE_REQUEST_NAMES = (server_constants.REQUEST_NAME_PREFIX +
                            request_names.RequestName.CLUSTER_LAUNCH.value,)

# TODO(zhwu): For scalability, there are several TODOs:
# [x] Have a way to queue requests.
# [ ] Move logs to persistent place.
# [ ] Deploy API server in a autoscaling fashion.


class RequestStatus(enum.Enum):
    """The status of a request."""

    PENDING = 'PENDING'
    WAITING = 'WAITING'
    RUNNING = 'RUNNING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'

    def __gt__(self, other):
        return (list(RequestStatus).index(self)
                > list(RequestStatus).index(other))

    def colored_str(self):
        color = _STATUS_TO_COLOR[self]
        return f'{color}{self.value}{colorama.Style.RESET_ALL}'

    @classmethod
    def finished_status(cls) -> list['RequestStatus']:
        return [cls.SUCCEEDED, cls.FAILED, cls.CANCELLED]

    @classmethod
    def active_statuses(cls) -> list['RequestStatus']:
        """Statuses of requests that are not finished yet."""
        return [cls.PENDING, cls.WAITING, cls.RUNNING]

    @classmethod
    def executable_statuses(cls) -> list['RequestStatus']:
        """Statuses from which a dequeued request may start executing.

        A request is enqueued as PENDING. It may also be re-enqueued while in
        WAITING -- the state it is parked in while waiting to resume (e.g. a
        retry backoff or an external continue-condition). In both cases the
        worker should pick it up and run it. Any other status (RUNNING, a
        finished status, or CANCELLED) means the request must not be executed.
        """
        return [cls.PENDING, cls.WAITING]


_STATUS_TO_COLOR = {
    RequestStatus.PENDING: colorama.Fore.BLUE,
    RequestStatus.WAITING: colorama.Fore.YELLOW,
    RequestStatus.RUNNING: colorama.Fore.GREEN,
    RequestStatus.SUCCEEDED: colorama.Fore.GREEN,
    RequestStatus.FAILED: colorama.Fore.RED,
    RequestStatus.CANCELLED: colorama.Fore.WHITE,
}


def _status_value_for_client(status_value: str) -> str:
    """Map WAITING to RUNNING for clients that predate the WAITING status.

    Older clients parse the status string straight into the RequestStatus enum
    and crash on an unknown value, so downgrade it to the closest status they
    understand on the wire.
    """
    return request_wire.status_value_for_client(
        status_value,
        waiting_status_value=RequestStatus.WAITING.value,
        running_status_value=RequestStatus.RUNNING.value,
        get_remote_api_version=versions.get_remote_api_version,
        min_waiting_status_api_version=(
            server_constants.MIN_WAITING_STATUS_API_VERSION),
    )


def _build_error_dict(error: BaseException) -> dict[str, Any]:
    """Build the serializable error payload persisted for a failed request."""
    # TODO(zhwu): pickle.dump does not work well with custom exceptions if
    # it has more than 1 arguments.
    serialized = exceptions.serialize_exception(error)
    return {
        'object': encoders.pickle_and_encode(serialized),
        'type': type(error).__name__,
        'message': str(error),
    }


def sanitize_request_error(
    name: str | None,
    error: _ErrorT,
    request_body: payloads.RequestBody | None = None,
) -> _ErrorT | ValueError:
    """Strips values only from failures marked by the image boundary."""
    del name, request_body
    safe_error = container_image_errors.find_safe_error(error)
    if safe_error is not None:
        # Return a fresh built-in exception. It carries neither the wrapper's
        # provider values nor typed-error attributes such as a demand ID.
        return ValueError(str(safe_error))
    return error


def _encoded_return_value(name: str, request_id: str, return_value: Any) -> Any:
    """Encode a return value.

    Durable terminal-transition implementations catch encoder failures and
    atomically persist FAILED plus the encoding error.  Keeping this helper
    strict prevents a malformed private-handler result from becoming a
    successful JSON null.
    """
    encoder = encoders.get_encoder(name)
    del request_id
    return encoder(return_value)


REQUEST_COLUMNS = [
    'request_id',
    'name',
    'entrypoint',
    'request_body',
    'status',
    'return_value',
    'error',
    'pid',
    'created_at',
    COL_CLUSTER_NAME,
    'schedule_type',
    COL_USER_ID,
    COL_STATUS_MSG,
    COL_SHOULD_RETRY,
    COL_FINISHED_AT,
    COL_FILE_MOUNTS_BLOB_ID,
    COL_IGNORE_RETURN_VALUE,
    COL_RETRYABLE,
]


class ScheduleType(enum.Enum):
    """The schedule type for the requests."""
    LONG = 'long'
    # Queue for requests that should be executed quickly for a quick response.
    SHORT = 'short'


@dataclasses.dataclass
class Request:
    """A SkyPilot API request."""

    request_id: str
    name: str
    entrypoint: Callable
    request_body: payloads.RequestBody
    status: RequestStatus
    created_at: float
    user_id: str
    return_value: Any = None
    error: dict[str, Any] | None = None
    # The pid of the request worker that is(was) running this request.
    pid: int | None = None
    schedule_type: ScheduleType = ScheduleType.LONG
    # Resources the request operates on.
    cluster_name: str | None = None
    # Status message of the request, indicates the reason of current status.
    status_msg: str | None = None
    # Whether the request should be retried.
    should_retry: bool = False
    # When the request finished.
    finished_at: float | None = None
    # Blob ID of uploaded file mounts
    file_mounts_blob_id: str | None = None
    # Enqueue flags (see the queue tuple in executor.RequestQueue). Persisted
    # so that queued requests survive a server restart and can be re-enqueued
    # with the same dispatch semantics. Server-internal: not part of
    # RequestPayload and never sent to clients.
    ignore_return_value: bool = False
    retryable: bool = False
    # PostgreSQL-only durable execution metadata. SQLite keeps its historical
    # pickle representation during the migration window.
    handler_name: str | None = None
    payload_type: str | None = None
    payload_format: str = DURABLE_PAYLOAD_FORMAT
    payload_version: int = DURABLE_PAYLOAD_VERSION
    producer_version: str = sky.__version__
    execution_class: str | None = None
    execution_generation: int = 0
    claim_token: str | None = None
    worker_instance_id: str | None = None
    controller_generation: int | None = None
    lease_expires_at: float | None = None
    heartbeat_at: float | None = None
    cancel_requested_at: float | None = None
    cancel_acknowledged_at: float | None = None
    interrupted_reason: str | None = None
    # PostgreSQL-only, server-owned context for actor-aware operational
    # events. It is intentionally absent from the client RequestPayload and
    # legacy SQLite row shape.
    event_context: dict[str, Any] | None = None
    # In-memory cause required when a generic update context performs a
    # terminal transition. Never persisted or exposed to clients.
    terminal_cause: str | None = None
    # Queue intent is set only while creating a newly scheduled request.
    # Durable backends consume it in the same transaction as request creation;
    # it is not exposed on the client wire format.
    should_enqueue: bool = False
    precondition_type: str | None = None
    precondition_payload: dict[str, Any] | None = None
    precondition_deadline: float | None = None

    @property
    def log_path(self) -> pathlib.Path:
        log_path_prefix = pathlib.Path(
            server_constants.REQUEST_LOG_PATH_PREFIX).expanduser().absolute()
        log_path_prefix.mkdir(parents=True, exist_ok=True)
        log_path = (log_path_prefix / self.request_id).with_suffix('.log')
        return log_path

    def set_error(self, error: BaseException) -> None:
        """Set the error."""
        sanitized_error = sanitize_request_error(self.name, error,
                                                 self.request_body)
        if sanitized_error is not error:
            _set_value_free_exception_stacktrace(sanitized_error)
        self.error = _build_error_dict(sanitized_error)

    def get_error(self) -> dict[str, Any] | None:
        """Get the error."""
        if self.error is None:
            return None
        unpickled = decoders.decode_and_unpickle(self.error['object'])
        deserialized = exceptions.deserialize_exception(unpickled)
        return {
            'object': deserialized,
            'type': self.error['type'],
            'message': self.error['message'],
        }

    def set_return_value(self, return_value: Any) -> None:
        """Set the encoded return value, raising if validation fails."""
        self.return_value = _encoded_return_value(self.name, self.request_id,
                                                  return_value)

    def durable_values(self) -> dict[str, Any]:
        """Return the non-pickle PostgreSQL representation of this request."""
        registration = request_registry.registration_for_handler(
            self.entrypoint)
        payload_type, payload_json = request_registry.encode_payload(
            self.request_body)
        if self.handler_name is not None and self.handler_name not in (
                registration.name, *registration.aliases):
            raise ValueError(f'Request {self.request_id} handler changed from '
                             f'{self.handler_name!r} to {registration.name!r}.')
        if self.payload_type is not None and self.payload_type != payload_type:
            raise ValueError(f'Request {self.request_id} payload changed from '
                             f'{self.payload_type!r} to {payload_type!r}.')
        if (self.execution_class is not None and
                self.execution_class != registration.execution_class.value):
            raise ValueError(
                f'Request {self.request_id} execution class '
                f'{self.execution_class!r} does not match handler-owned class '
                f'{registration.execution_class.value!r}.')
        self.handler_name = registration.name
        self.payload_type = payload_type
        self.execution_class = registration.execution_class.value
        return {
            'request_id': self.request_id,
            'name': self.name,
            'handler_name': registration.name,
            'payload_type': payload_type,
            'payload_format': self.payload_format,
            'payload_version': self.payload_version,
            'producer_version': self.producer_version,
            'payload_json': payload_json,
            'execution_class': registration.execution_class.value,
            'status': self.status.value,
            'return_value': self.return_value,
            'error': self.error,
            'pid': self.pid,
            'created_at': self.created_at,
            COL_CLUSTER_NAME: self.cluster_name,
            'schedule_type': self.schedule_type.value,
            COL_USER_ID: self.user_id,
            COL_STATUS_MSG: self.status_msg,
            COL_SHOULD_RETRY: self.should_retry,
            COL_FINISHED_AT: self.finished_at,
            COL_FILE_MOUNTS_BLOB_ID: self.file_mounts_blob_id,
            COL_IGNORE_RETURN_VALUE: self.ignore_return_value,
            COL_RETRYABLE: self.retryable,
            'execution_generation': self.execution_generation,
            'claim_token': self.claim_token,
            'worker_instance_id': self.worker_instance_id,
            'controller_generation': self.controller_generation,
            'lease_expires_at': self.lease_expires_at,
            'heartbeat_at': self.heartbeat_at,
            'cancel_requested_at': self.cancel_requested_at,
            'cancel_acknowledged_at': self.cancel_acknowledged_at,
            'interrupted_reason': self.interrupted_reason,
            'event_context': self.event_context,
        }

    @classmethod
    def from_durable_values(cls, values: dict[str, Any]) -> 'Request':
        """Decode a request row through the closed handler/payload registries."""
        registration = request_registry.resolve_handler(values['handler_name'])
        execution_class = values['execution_class']
        if execution_class != registration.execution_class.value:
            raise ValueError(
                f'Durable request {values["request_id"]} has execution class '
                f'{execution_class!r}, but handler {registration.name!r} owns '
                f'{registration.execution_class.value!r}.')
        if values['payload_format'] != DURABLE_PAYLOAD_FORMAT:
            raise ValueError(f'Unsupported request payload format '
                             f'{values["payload_format"]!r}.')
        if int(values['payload_version']) != DURABLE_PAYLOAD_VERSION:
            raise ValueError(f'Unsupported request payload version '
                             f'{values["payload_version"]!r}.')
        request_body = request_registry.decode_payload(values['payload_type'],
                                                       values['payload_json'])
        return cls(
            request_id=values['request_id'],
            name=values['name'],
            entrypoint=registration.func,
            request_body=request_body,
            status=RequestStatus(values['status']),
            created_at=float(values['created_at']),
            user_id=values[COL_USER_ID],
            return_value=values.get('return_value'),
            error=values.get('error'),
            pid=values.get('pid'),
            schedule_type=ScheduleType(values['schedule_type']),
            cluster_name=values.get(COL_CLUSTER_NAME),
            status_msg=values.get(COL_STATUS_MSG),
            should_retry=bool(values.get(COL_SHOULD_RETRY, False)),
            finished_at=values.get(COL_FINISHED_AT),
            file_mounts_blob_id=values.get(COL_FILE_MOUNTS_BLOB_ID),
            ignore_return_value=bool(values.get(COL_IGNORE_RETURN_VALUE,
                                                False)),
            retryable=bool(values.get(COL_RETRYABLE, False)),
            handler_name=registration.name,
            payload_type=values['payload_type'],
            payload_format=values['payload_format'],
            payload_version=int(values['payload_version']),
            producer_version=values['producer_version'],
            execution_class=execution_class,
            execution_generation=int(values.get('execution_generation', 0)),
            claim_token=(str(values['claim_token'])
                         if values.get('claim_token') is not None else None),
            worker_instance_id=(str(values['worker_instance_id'])
                                if values.get('worker_instance_id') is not None
                                else None),
            controller_generation=(int(values['controller_generation'])
                                   if values.get('controller_generation')
                                   is not None else None),
            lease_expires_at=values.get('lease_expires_at'),
            heartbeat_at=values.get('heartbeat_at'),
            cancel_requested_at=values.get('cancel_requested_at'),
            cancel_acknowledged_at=values.get('cancel_acknowledged_at'),
            interrupted_reason=values.get('interrupted_reason'),
            event_context=values.get('event_context'),
        )

    def get_return_value(self) -> Any:
        """Get the return value."""
        return decoders.get_decoder(self.name)(self.return_value)

    @classmethod
    def from_row(cls, row: tuple[Any, ...]) -> 'Request':
        content = dict(zip(REQUEST_COLUMNS, row))
        # The enqueue flags are server-internal DB columns that are not part
        # of RequestPayload; pop them and set them on the decoded request.
        # NULL (a row written by an older server) coerces to False.
        ignore_return_value = bool(content.pop(COL_IGNORE_RETURN_VALUE, None))
        retryable = bool(content.pop(COL_RETRYABLE, None))
        request = cls.decode(payloads.RequestPayload(**content))
        request.ignore_return_value = ignore_return_value
        request.retryable = retryable
        return request

    def to_row(self) -> tuple[Any, ...]:
        payload = self.encode()
        # encode() may downgrade WAITING -> RUNNING for clients on an older API
        # version; that is a wire-only concern. to_row() feeds the database, so
        # always persist the true status regardless of the request context.
        payload.status = self.status.value
        row: list[Any] = []
        for k in REQUEST_COLUMNS:
            if k == COL_IGNORE_RETURN_VALUE:
                row.append(int(self.ignore_return_value))
            elif k == COL_RETRYABLE:
                row.append(int(self.retryable))
            else:
                row.append(getattr(payload, k))
        return tuple(row)

    def readable_encode(self) -> payloads.RequestPayload:
        """Serialize the SkyPilot API request for display purposes.

        This function should be called on the server side to serialize the
        request body into human readable format, e.g., the entrypoint should
        be a string, and the pid, error, or return value are not needed.

        The returned value will then be displayed on the client side in request
        table.

        We do not use `encode` for display to avoid a large amount of data being
        sent to the client side, especially for the request table could include
        all the requests.
        """
        # Delegate to the batched encoder so the display field list lives in a
        # single place and the two paths cannot drift apart.
        return encode_requests([self])[0]

    def encode(self) -> payloads.RequestPayload:
        """Serialize the SkyPilot API request."""
        return request_wire.encode_request(
            self,
            validate_request_body=(
                payloads.validate_task_request_body_for_persistence),
            get_serializer=return_value_serializers.get_serializer,
            pickle_and_encode=encoders.pickle_and_encode,
            project_status=_status_value_for_client,
            logger=logger,
        )

    @staticmethod
    def _decode_entrypoint(encoded_entrypoint: str) -> Callable:
        """Unpickle the entrypoint, tolerating an unresolvable reference.

        The entrypoint is a server-side callable that is pickled by reference
        (module + qualname). The client deserializes it for bookkeeping but
        never invokes it. When the client is older than the server, the server
        may reference a symbol this client does not have (e.g. a newly-added
        ``sky.core`` function), which makes unpickling raise ``AttributeError``
        /``ImportError``. Since the value is never called on the client, fall
        back to a placeholder instead of failing the whole request.
        """
        return request_wire.decode_entrypoint(
            encoded_entrypoint,
            decode_and_unpickle=decoders.decode_and_unpickle,
            unresolved_entrypoint=_unresolved_entrypoint,
            logger=logger,
        )

    @classmethod
    def decode(cls, payload: payloads.RequestPayload) -> 'Request':
        """Deserialize the SkyPilot API request."""
        return request_wire.decode_request(
            payload,
            request_factory=cls,
            entrypoint_decoder=cls._decode_entrypoint,
            decode_and_unpickle=decoders.decode_and_unpickle,
            status_cls=RequestStatus,
            schedule_type_cls=ScheduleType,
            logger=logger,
        )


def get_new_request_id() -> str:
    """Get a new request ID."""
    return str(uuid.uuid4())


def encode_requests(requests: list[Request]) -> list[payloads.RequestPayload]:
    """Serialize the SkyPilot API request for display purposes.

        This function should be called on the server side to serialize the
        request body into human readable format, e.g., the entrypoint should
        be a string, and the pid, error, or return value are not needed.

        The returned value will then be displayed on the client side in request
        table.

        We do not use `encode` for display to avoid a large amount of data being
        sent to the client side, especially for the request table could include
        all the requests.
        """
    return request_wire.encode_requests(
        requests,
        get_all_users=global_user_state.get_all_users,
        project_status=_status_value_for_client,
    )


def _update_request_row_fields(
        row: tuple[Any, ...],
        fields: list[str] | None = None) -> tuple[Any, ...]:
    """Update the request row fields."""
    if not fields:
        return row

    # Convert tuple to dictionary for easier manipulation
    content = dict(zip(fields, row))

    # Required fields in RequestPayload
    if 'request_id' not in fields:
        content['request_id'] = ''
    if 'name' not in fields:
        content['name'] = ''
    if 'entrypoint' not in fields:
        content['entrypoint'] = server_constants.EMPTY_PICKLED_VALUE
    if 'request_body' not in fields:
        content['request_body'] = server_constants.EMPTY_PICKLED_VALUE
    if 'status' not in fields:
        content['status'] = RequestStatus.PENDING.value
    if 'created_at' not in fields:
        content['created_at'] = 0
    if 'user_id' not in fields:
        content['user_id'] = ''
    if 'return_value' not in fields:
        content['return_value'] = orjson.dumps(None).decode('utf-8')
    if 'error' not in fields:
        content['error'] = orjson.dumps(None).decode('utf-8')
    if 'schedule_type' not in fields:
        content['schedule_type'] = ScheduleType.SHORT.value
    # Optional fields in RequestPayload
    if 'pid' not in fields:
        content['pid'] = None
    if 'cluster_name' not in fields:
        content['cluster_name'] = None
    if 'status_msg' not in fields:
        content['status_msg'] = None
    if 'should_retry' not in fields:
        content['should_retry'] = False
    if 'finished_at' not in fields:
        content['finished_at'] = None
    if COL_FILE_MOUNTS_BLOB_ID not in fields:
        content[COL_FILE_MOUNTS_BLOB_ID] = None
    if COL_IGNORE_RETURN_VALUE not in fields:
        content[COL_IGNORE_RETURN_VALUE] = False
    if COL_RETRYABLE not in fields:
        content[COL_RETRYABLE] = False

    # Convert back to tuple in the same order as REQUEST_COLUMNS
    return tuple(content[col] for col in REQUEST_COLUMNS)


def create_table(cursor, conn):
    # Enable WAL mode to avoid locking issues.
    # See: issue #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if not common_utils.is_wsl():
        try:
            cursor.execute('PRAGMA journal_mode=WAL')
            # Safe with WAL (no corruption on crash) and avoids an fsync on
            # every commit.
            cursor.execute('PRAGMA synchronous=NORMAL')
        except sqlite3.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    # Table for Requests
    cursor.execute(f"""\
        CREATE TABLE IF NOT EXISTS {REQUEST_TABLE} (
        request_id TEXT PRIMARY KEY,
        name TEXT,
        entrypoint TEXT,
        request_body TEXT,
        status TEXT,
        created_at REAL,
        return_value TEXT,
        error BLOB,
        pid INTEGER,
        {COL_CLUSTER_NAME} TEXT,
        schedule_type TEXT,
        {COL_USER_ID} TEXT,
        {COL_STATUS_MSG} TEXT,
        {COL_SHOULD_RETRY} INTEGER,
        {COL_FINISHED_AT} REAL,
        {COL_IGNORE_RETURN_VALUE} INTEGER,
        {COL_RETRYABLE} INTEGER
        )""")

    db_utils.add_column_to_table(cursor, conn, REQUEST_TABLE, COL_STATUS_MSG,
                                 'TEXT')
    db_utils.add_column_to_table(cursor, conn, REQUEST_TABLE, COL_SHOULD_RETRY,
                                 'INTEGER')
    db_utils.add_column_to_table(cursor, conn, REQUEST_TABLE, COL_FINISHED_AT,
                                 'REAL')
    db_utils.add_column_to_table(cursor, conn, REQUEST_TABLE,
                                 COL_FILE_MOUNTS_BLOB_ID, 'TEXT')
    db_utils.add_column_to_table(cursor, conn, REQUEST_TABLE,
                                 COL_IGNORE_RETURN_VALUE, 'INTEGER')
    db_utils.add_column_to_table(cursor, conn, REQUEST_TABLE, COL_RETRYABLE,
                                 'INTEGER')

    # Add an index on (status, name) to speed up queries
    # that filter on these columns.
    cursor.execute(f"""\
        CREATE INDEX IF NOT EXISTS status_name_idx ON {REQUEST_TABLE} (status, name) WHERE status IN ('PENDING', 'WAITING', 'RUNNING');
    """)
    # Add an index on cluster_name to speed up queries
    # that filter on this column.
    cursor.execute(f"""\
        CREATE INDEX IF NOT EXISTS cluster_name_idx ON {REQUEST_TABLE} ({COL_CLUSTER_NAME}) WHERE status IN ('PENDING', 'WAITING', 'RUNNING');
    """)
    # Add an index on created_at to speed up queries that sort on this column.
    cursor.execute(f"""\
        CREATE INDEX IF NOT EXISTS created_at_idx ON {REQUEST_TABLE} (created_at);
    """)
    # Add an index on finished_at for terminal rows to speed up the requests
    # GC, which repeatedly queries finished requests older than the retention.
    cursor.execute(f"""\
        CREATE INDEX IF NOT EXISTS finished_at_idx ON {REQUEST_TABLE} ({COL_FINISHED_AT}) WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED');
    """)


_DB = None
_init_db_lock = threading.Lock()


def _init_db_within_lock():
    global _DB
    if _DB is None:
        db_path = os.path.expanduser(
            server_constants.API_SERVER_REQUEST_DB_PATH)
        pathlib.Path(db_path).parents[0].mkdir(parents=True, exist_ok=True)
        _DB = db_utils.SQLiteConn(db_path, create_table)


def _close_db_within_lock():
    """Close the calling thread's DB connection and drop the handle.

    The next DB access re-initializes the handle, re-creating the database
    file and its tables if needed. ``_DB`` is thread-local, so only the
    calling thread's connection can be closed here: this is only safe during
    single-threaded startup, before any other thread or event loop has
    touched the request DB.
    """
    global _DB
    if _DB is None:
        return
    _DB.conn.close()
    _DB = None


def _ensure_db_initialized():
    """Ensure the database is initialized.

    Standalone function for use in context managers where the @init_db
    decorator cannot be applied.
    """
    if _DB is not None:
        return
    with _init_db_lock:
        _init_db_within_lock()


def init_db(func):
    """Initialize the database."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if _DB is not None:
            return func(*args, **kwargs)
        with _init_db_lock:
            _init_db_within_lock()
        return func(*args, **kwargs)

    return wrapper


def init_db_async(func):
    """Async version of init_db."""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if _DB is not None:
            return await func(*args, **kwargs)
        # If _DB is not initialized, init_db_async will be blocked if there
        # is a thread initializing _DB, this is fine since it occurs on process
        # startup.
        with _init_db_lock:
            _init_db_within_lock()
        return await func(*args, **kwargs)

    return wrapper


def _log_orphaned_inflight_requests() -> None:
    """Log any requests still in-flight when the API server last stopped.

    ``reset_db_and_logs`` (the legacy full-wipe path, run when startup
    recovery is disabled or fails) wipes the request DB and its logs, and the
    executor child processes that ran those requests died
    with the previous process. So a request that was still PENDING/WAITING/
    RUNNING -- notably a long provisioning launch held by a long worker -- is
    silently dropped: the caller (CLI, or a serve/jobs controller) awaiting it
    sees the request vanish, while any half-provisioned cluster lives on in the
    separate cluster-state DB and leaks until a later status refresh reaps it.

    We cannot resume those requests here (their worker processes are gone), but
    we can refuse to lose them silently: enumerate them at WARNING so the drop
    is alertable and any leaked clusters are reconcilable. Best effort -- a scan
    failure (e.g. an incompatible on-disk schema after an upgrade) must never
    block startup.
    """
    try:
        # Select only the plain columns needed for the log: the rows were
        # written by the previous server version, and unpickling entrypoint
        # or request_body can fail across an upgrade, which would silence
        # the entire report.
        orphaned = request_storage.get_request_backend().query_requests(
            req_filter=RequestTaskFilter(
                status=RequestStatus.active_statuses(),
                fields=['request_id', 'name', 'status', COL_CLUSTER_NAME]))
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('Could not scan for orphaned in-flight requests during '
                     f'API server startup (continuing): {e}')
        return
    # Internal daemon requests sit in RUNNING for the whole life of the
    # server and are recreated on every startup, so their rows are not
    # dropped work; skip them like the other kill paths do.
    orphaned = [
        req for req in orphaned
        if not daemons.is_daemon_request_id(req.request_id)
    ]
    if not orphaned:
        return
    logger.warning(
        f'API server startup is clearing {len(orphaned)} request(s) that were '
        'still in-flight when the server last stopped; their executor '
        'processes are gone and the request rows are being wiped. Any clusters '
        'they were provisioning may leak until the next status refresh:')
    for req in orphaned:
        cluster = f' cluster={req.cluster_name}' if req.cluster_name else ''
        logger.warning(f'  dropped in-flight request {req.request_id} '
                       f'name={req.name!r} status={req.status.value}{cluster}')


def _request_log_tombstones() -> list[pathlib.Path]:
    """List request-log tombstone dirs left by this or a previous startup."""
    log_dir = pathlib.Path(
        server_constants.REQUEST_LOG_PATH_PREFIX).expanduser()
    return list(log_dir.parent.glob(f'{log_dir.name}.deleting.*'))


def _rmtree_in_background(paths: list[pathlib.Path]) -> threading.Thread | None:
    """Delete directories in a background thread; returns it for tests."""
    if not paths:
        return None

    def _rm():
        for path in paths:
            shutil.rmtree(path, ignore_errors=True)

    thread = threading.Thread(target=_rm,
                              name='request-logs-cleanup',
                              daemon=True)
    thread.start()
    return thread


def _clear_request_logs_in_background() -> threading.Thread | None:
    """Clear the request-logs dir without blocking startup.

    The dir scales with the number of requests since the last wipe, so an
    inline rmtree delays the port bind by O(requests). Rename it to a
    tombstone (O(1), same filesystem) and delete the tombstone in a
    background thread; also sweep tombstones left by a previous crash.
    """
    to_delete = _request_log_tombstones()
    log_dir = pathlib.Path(
        server_constants.REQUEST_LOG_PATH_PREFIX).expanduser()
    if log_dir.exists():
        tombstone = log_dir.parent / (f'{log_dir.name}.deleting.'
                                      f'{os.getpid()}-{uuid.uuid4().hex[:8]}')
        try:
            log_dir.rename(tombstone)
            to_delete.append(tombstone)
        except OSError:
            # Rename failed (e.g. concurrent removal); fall back to the
            # legacy inline delete rather than deleting a live dir that new
            # request logs may be written into.
            shutil.rmtree(log_dir, ignore_errors=True)
    return _rmtree_in_background(to_delete)


def reset_db_and_logs():
    """Clear local state and re-initialize the request storage backend."""
    # Surface any requests still in-flight when the server stopped BEFORE we
    # wipe them, so the drop is alertable rather than silent (see helper).
    _log_orphaned_inflight_requests()
    # The scan may have initialized the module-level DB handle against the
    # database file that is about to be removed. Drop the handle before the
    # wipe so reset_on_startup() below re-creates the fresh database and its
    # tables, instead of leaving this thread's connection bound to the
    # unlinked file.
    with _init_db_lock:
        _close_db_within_lock()
    logger.debug('clearing local API server database')
    server_common.clear_local_api_server_database()
    logger.debug('clearing local API server logs directory at '
                 f'{server_constants.REQUEST_LOG_PATH_PREFIX}')
    _clear_request_logs_in_background()
    # Also clear legacy path for backward compatibility cleanup
    logger.debug('clearing legacy API server logs directory at '
                 f'{LEGACY_REQUEST_LOG_PATH_PREFIX}')
    shutil.rmtree(pathlib.Path(LEGACY_REQUEST_LOG_PATH_PREFIX).expanduser(),
                  ignore_errors=True)
    bs.get_blob_storage().reset_on_startup()
    request_storage.get_request_backend().reset_on_startup()


def _find_interrupted_launches_to_requeue() -> list[str]:
    """List interrupted launch rows that are safe to re-execute.

    A launch whose executor died mid-provision leaves its cluster in INIT.
    The launch entrypoint is re-runnable by construction up to the point the
    cluster reaches UP: re-executing it resumes provisioning on the existing
    cluster record (the same thing a manual relaunch does), and the task's
    run section is only submitted after the cluster row is marked UP, so a
    cluster still in INIT cannot have started user work.

    Any other case falls to the generic CANCELLED + ``should_retry`` path:
    past UP the remaining launch work has client-visible side effects (job
    submission) that must not be silently repeated, and a launch with no
    cluster record yet may have pre-provision side effects (e.g. storage
    creation) whose re-run semantics are not established. A failed cluster
    status lookup likewise only disqualifies rows from replay -- it must not
    abort recovery of everything else.

    Runs outside the recovery transaction: the batched cluster-status lookup
    hits the (possibly remote) cluster-state database and must not extend the
    request-DB transaction. Safe because startup is single-threaded -- no
    executor is running yet to change the rows in between.
    """
    assert _DB is not None
    replayable_names = ', '.join(['?'] * len(REPLAYABLE_REQUEST_NAMES))
    cursor = _DB.conn.cursor()
    cursor.execute(
        f'SELECT request_id, {COL_CLUSTER_NAME} FROM {REQUEST_TABLE} '
        f'WHERE name IN ({replayable_names}) '
        f'AND (status = ? '
        f'OR (status = ? AND ({COL_RETRYABLE} IS NULL '
        f'OR {COL_RETRYABLE} = 0)))',
        (*REPLAYABLE_REQUEST_NAMES, RequestStatus.RUNNING.value,
         RequestStatus.WAITING.value))
    rows = cursor.fetchall()
    cluster_names = list({name for _, name in rows if name is not None})
    try:
        status_fields = global_user_state.get_cluster_status_fields(
            cluster_names)
    except Exception as e:  # pylint: disable=broad-except
        # A failed lookup disqualifies rows from replay, not from recovery:
        # every launch falls back to the client-retry path (CANCELLED +
        # should_retry), same as an individually unresolvable cluster.
        logger.warning(
            'Could not check cluster statuses while recovering launch '
            f'requests; leaving them to the client-retry path: {e}')
        status_fields = {}
    requeue_ids = []
    for request_id, cluster_name in rows:
        if cluster_name is None:
            continue
        status_str, _ = status_fields.get(cluster_name, (None, None))
        if status_str == status_lib.ClusterStatus.INIT.value:
            requeue_ids.append(request_id)
    return requeue_ids


def _recover_requests() -> tuple[int, int]:
    """Reconcile request rows left over from the previous server process.

    All executor processes died with the previous server, so no recovered
    row has a live worker. Reconcile each non-terminal row:

    - Internal daemon rows are deleted: a stale row would make
      ``schedule_internal_daemon_async``'s create-or-refresh path skip the
      enqueue and the daemon would never run this boot.
    - Interrupted launch rows whose cluster is still INIT are flipped back
      to PENDING for re-execution
      (``_find_interrupted_launches_to_requeue``): re-running a launch is
      safe until the cluster is UP, and this is what lets a server redeploy
      complete an in-flight provisioning instead of wedging the cluster in
      INIT.
    - Other RUNNING rows, and WAITING rows that are not retryable, are
      marked CANCELLED with ``should_retry`` set so polling clients get the
      retry signal (HTTP 503) instead of a 404.
    - PENDING rows and retryable WAITING rows are left untouched for
      re-enqueue (``executor.reenqueue_recovered_requests``): a PENDING row
      never started executing (the execution wrapper flips it to RUNNING
      before invoking the entrypoint) and retryable WAITING rows were
      already parked for a full re-run.
    - Terminal rows (and their logs) are preserved; the requests GC daemon
      bounds their growth via the configured retention.

    Returns:
        A tuple of (number of rows marked for client retry, number of rows
        left queued for re-enqueue).
    """
    with _init_db_lock:
        _init_db_within_lock()
    assert _DB is not None
    daemon_ids = sorted(d.id for d in daemons.INTERNAL_REQUEST_DAEMONS)
    # Resolved before the transaction: the cluster-status lookups may be
    # remote and must not extend it (see the helper's docstring).
    requeue_ids = _find_interrupted_launches_to_requeue()
    with _DB.conn:
        cursor = _DB.conn.cursor()
        placeholders = ', '.join(['?'] * len(daemon_ids))
        cursor.execute(
            f'DELETE FROM {REQUEST_TABLE} '
            f'WHERE request_id IN ({placeholders})', daemon_ids)
        if requeue_ids:
            placeholders = ', '.join(['?'] * len(requeue_ids))
            cursor.execute(
                f'UPDATE {REQUEST_TABLE} '
                f'SET status = ?, pid = NULL, {COL_SHOULD_RETRY} = 0, '
                f'{COL_FINISHED_AT} = NULL '
                f'WHERE request_id IN ({placeholders})',
                (RequestStatus.PENDING.value, *requeue_ids))
        cursor.execute(
            f'UPDATE {REQUEST_TABLE} '
            f'SET status = ?, {COL_SHOULD_RETRY} = 1, {COL_FINISHED_AT} = ? '
            f'WHERE status = ? '
            f'OR (status = ? AND ({COL_RETRYABLE} IS NULL '
            f'OR {COL_RETRYABLE} = 0))',
            (RequestStatus.CANCELLED.value, time.time(),
             RequestStatus.RUNNING.value, RequestStatus.WAITING.value))
        interrupted = cursor.rowcount
        if requeue_ids:
            logger.info(f'Re-queued {len(requeue_ids)} interrupted launch '
                        'request(s) whose clusters are still INIT; they will '
                        'be re-executed to complete provisioning.')
        cursor.execute(
            f'SELECT COUNT(*) FROM {REQUEST_TABLE} '
            'WHERE status IN (?, ?)',
            (RequestStatus.PENDING.value, RequestStatus.WAITING.value))
        replayable = cursor.fetchone()[0]
    return interrupted, replayable


def recover_db_and_logs() -> bool:
    """Initialize request state on startup, preserving prior requests.

    Replaces the legacy behavior of wiping the request DB and logs on every
    startup, which destroyed queued work and made clients polling in-flight
    requests fail with 404 after any restart (hard crashes included). The
    legacy wipe remains available via ``RESET_REQUESTS_ON_STARTUP_ENV_VAR``
    and as the fallback if recovery fails for any reason, so startup is
    never blocked.

    Returns:
        True if the recovery transitions ran and completed, i.e. every
        remaining PENDING/WAITING row has been reconciled and it is safe
        for the caller to re-enqueue them
        (``executor.reenqueue_recovered_requests``). False whenever the
        legacy wipe path was taken instead (explicit env-var reset, plugin
        request backend, or recovery failure): rows that survive a wipe --
        e.g. rows owned by a plugin backend whose ``reset_on_startup`` is a
        no-op -- were never reconciled and must NOT be re-enqueued.
    """
    backend = request_storage.get_request_backend()
    if backend.uses_durable_queue is True:
        if os.environ.get(RESET_REQUESTS_ON_STARTUP_ENV_VAR) == '1':
            raise RuntimeError(
                f'{RESET_REQUESTS_ON_STARTUP_ENV_VAR}=1 cannot wipe the '
                'durable PostgreSQL request store. Use the explicit migration '
                'or administrative cleanup workflow.')
        recover = getattr(backend, 'recover_on_startup', None)
        if not callable(recover):
            raise RuntimeError(
                'A durable request backend must implement recover_on_startup.')
        return bool(recover())  # pylint: disable=not-callable
    if os.environ.get(RESET_REQUESTS_ON_STARTUP_ENV_VAR) == '1':
        reset_db_and_logs()
        return False
    if not isinstance(backend, SqliteRequestBackend):
        # A plugin request backend owns its own restart semantics via
        # reset_on_startup(); the sqlite-level recovery below would not see
        # its rows (and reenqueue_recovered_requests would then replay rows
        # recovery never reconciled). Keep the legacy behavior for it.
        reset_db_and_logs()
        return False
    try:
        interrupted, replayable = _recover_requests()
        if interrupted or replayable:
            logger.warning(
                'Recovered request state from the previous API server run: '
                f'{interrupted} interrupted request(s) marked for client '
                f'retry, {replayable} queued request(s) will be re-enqueued.')
        # NOTE: bs.get_blob_storage().reset_on_startup() is intentionally not
        # called on the recovery path: it wipes each client dir except
        # file_mounts/blobs, and preserved PENDING/WAITING request bodies may
        # reference legacy (non-blob) file-mount uploads under those client
        # dirs (process_mounts_in_task_on_api_server resolves
        # file_mounts_mapping against the client dir when file_mounts_blob_id
        # is unset), which replay needs intact. The transient state stays
        # bounded: the full wipe still runs whenever recovery is disabled or
        # fails, and blob GC runs in the background.
        # Sweep request-log tombstones left by a previous wipe that crashed
        # mid-delete, and clear the legacy logs dir (cheap and one-time).
        _rmtree_in_background(_request_log_tombstones())
        shutil.rmtree(pathlib.Path(LEGACY_REQUEST_LOG_PATH_PREFIX).expanduser(),
                      ignore_errors=True)
        request_storage.get_request_backend().reset_on_startup()
        return True
    except Exception as e:  # pylint: disable=broad-except
        # Recovery must never block startup (e.g. a corrupted DB file):
        # fall back to the legacy full wipe, which starts from a clean slate.
        logger.warning('Failed to recover request state from the previous '
                       'API server run; falling back to a full reset: '
                       f'{common_utils.format_exception(e)}')
        reset_db_and_logs()
        return False


def surface_interrupted_cluster_launches(delay_seconds: float = 0) -> None:
    """Record a cluster event for INIT clusters whose in-flight work died.

    Runs once per startup, after the request rows from the previous server
    run have been reconciled (``recover_db_and_logs``). A cluster is left in
    INIT by a launch (or restart) that did not complete; if the request
    driving it died with the previous server process -- or its request row
    died with the pod's ephemeral disk on a redeploy, in which case recovery
    finds nothing to reconcile or requeue -- the cluster wedges in INIT with
    no user-visible explanation: ``sky status`` shows INIT indefinitely and
    the provision-log endpoint 404s (the log file lived on the old server's
    disk). We cannot resume that work, so we record why the cluster is stuck.

    Clusters that have an active request row at scan time are skipped: those
    are either recovered rows about to be re-executed or new work submitted
    after startup (a launch creates its request row before its cluster row
    turns INIT, so a fresh launch cannot be misread as a dead one). This
    makes the scan safe to run in the background after the server starts
    serving; ``delay_seconds`` additionally postpones it past the previous
    replica's shutdown grace, so a launch still finishing on an overlapping
    old replica (whose request rows this instance cannot see) is not misread
    as interrupted. Best effort -- a failure here must never affect the
    server.
    """
    try:
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        init_clusters = global_user_state.get_cluster_names_by_status(
            status_lib.ClusterStatus.INIT)
        if not init_clusters:
            return
        active = request_storage.get_request_backend().query_requests(
            req_filter=RequestTaskFilter(
                status=RequestStatus.active_statuses(),
                cluster_names=init_clusters,
                fields=['request_id', COL_CLUSTER_NAME]))
        clusters_with_active_request = {req.cluster_name for req in active}
        for cluster_name in init_clusters:
            if cluster_name in clusters_with_active_request:
                continue
            global_user_state.add_cluster_event(
                cluster_name,
                None,
                'API server restarted while this cluster was in INIT with no '
                'live request operating on it; an in-flight launch appears '
                'to have been interrupted and its provision logs may be '
                'lost. Re-run `sky launch` to recover the cluster, or '
                '`sky down` to release its resources.',
                global_user_state.ClusterEventType.STATUS_CHANGE,
                nop_if_duplicate=True)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('Could not surface interrupted cluster launches during '
                     f'API server startup (continuing): {e}')


def request_lock_path(request_id: str) -> str:
    lock_path = os.path.expanduser(server_constants.REQUEST_LOG_PATH_PREFIX)
    os.makedirs(lock_path, exist_ok=True)
    return os.path.join(lock_path, f'.{request_id}.lock')


def kill_cluster_requests(cluster_name: str, exclude_request_name: str):
    """Kill all pending and running requests for a cluster.

    Args:
        cluster_name: the name of the cluster.
        exclude_request_names: exclude requests with these names. This is to
            prevent killing the caller request.
    """
    storage = request_storage.get_request_backend()
    request_ids = [
        request_task.request_id
        for request_task in storage.query_requests(req_filter=RequestTaskFilter(
            status=RequestStatus.active_statuses(),
            exclude_request_names=[exclude_request_name],
            cluster_names=[cluster_name],
            fields=['request_id']))
    ]
    _kill_requests(request_ids)


def kill_requests(request_ids: list[str] | None = None,
                  user_id: str | None = None) -> list[str]:
    """Kill requests with a given request ID prefix."""
    expanded_request_ids: list[str] | None = None
    if request_ids is not None:
        expanded_request_ids = []
        for request_id in request_ids:
            request_tasks = get_requests_with_prefix(request_id,
                                                     fields=['request_id'])
            if request_tasks is None or len(request_tasks) == 0:
                continue
            if len(request_tasks) > 1:
                raise ValueError(f'Multiple requests found for '
                                 f'request ID prefix: {request_id}')
            expanded_request_ids.append(request_tasks[0].request_id)
    return _kill_requests(request_ids=expanded_request_ids, user_id=user_id)


# needed for backward compatibility. Remove by v0.10.7 or v0.12.0
# and rename kill_requests to kill_requests_with_prefix.
kill_requests_with_prefix = kill_requests


def _should_kill_request(request_id: str,
                         request_record: Request | None) -> bool:
    if request_record is None:
        logger.debug(f'No request ID {request_id}')
        return False
    # Skip internal requests. The internal requests are scheduled with
    # request_id in range(len(INTERNAL_REQUEST_EVENTS)).
    if request_record.request_id in set(
            event.id for event in daemons.INTERNAL_REQUEST_DAEMONS):
        return False
    if request_record.status > RequestStatus.RUNNING:
        logger.debug(f'Request {request_id} already finished')
        return False
    return True


def _kill_requests(request_ids: list[str] | None = None,
                   user_id: str | None = None) -> list[str]:
    """Kill SkyPilot API requests and set their status to cancelled.

    Delegates to the registered request backend, which handles local
    process killing and (for multi-replica backends) cross-replica
    cancellation.
    """
    return request_storage.get_request_backend().kill_requests(
        request_ids=request_ids, user_id=user_id)


@asyncio_utils.shield
async def kill_request_async(request_id: str) -> bool:
    """Kill a SkyPilot API request and set its status to cancelled.

    Returns:
        True if the request was killed, False otherwise.
    """
    return await request_storage.get_request_backend().kill_request_async(
        request_id)


@contextlib.contextmanager
@metrics_lib.time_me
def update_request(request_id: str) -> Generator[Request | None, None, None]:
    """Get and update a SkyPilot API request."""
    with request_storage.get_request_backend().update_request(
            request_id) as request:
        yield request


@metrics_lib.time_me
def try_mark_running(request_id: str,
                     pid: int | None,
                     execution_generation: int = 0,
                     claim_token: str | None = None) -> bool:
    """Atomically flip a request to RUNNING if it is still executable.

    Returns:
        True iff the request was in an executable status (PENDING/WAITING)
        and is now RUNNING with `pid` recorded and any stale retry-backoff
        status_msg cleared.
    """
    return request_storage.get_request_backend().try_mark_running(
        request_id, pid, execution_generation, claim_token)


@metrics_lib.time_me
@asyncio_utils.shield
async def update_status_async(request_id: str, status: RequestStatus) -> None:
    """Update the status of a request"""
    await request_storage.get_request_backend().update_status_async(
        request_id, status)


@metrics_lib.time_me
@asyncio_utils.shield
async def update_status_msg_async(request_id: str, status_msg: str) -> None:
    """Update the status message of a request"""
    await request_storage.get_request_backend().update_status_msg_async(
        request_id, status_msg)


def set_event_workspace(request_id: str, workspace: str) -> bool:
    """Persist the authoritative workspace for an opted-in event request."""
    return request_storage.get_request_backend().set_event_workspace(
        request_id, workspace)


def set_event_target_id(request_id: str, target_id: str) -> bool:
    """Enrich the primary operational event target identity."""
    return request_storage.get_request_backend().set_event_target_id(
        request_id, target_id)


def _get_request_no_lock(request_id: str,
                         fields: list[str] | None = None) -> Request | None:
    """Get a SkyPilot API request."""
    assert _DB is not None
    columns_str = ', '.join(REQUEST_COLUMNS)
    if fields:
        columns_str = ', '.join(fields)
    with _DB.conn:
        cursor = _DB.conn.cursor()
        # Exact match on the primary key: LIKE-prefix matching would force a
        # full table scan here (TEXT PK with default BINARY collation disables
        # SQLite's LIKE-prefix index optimization). Prefix expansion is the
        # caller's job via the *_with_prefix APIs.
        cursor.execute((f'SELECT {columns_str} FROM {REQUEST_TABLE} '
                        'WHERE request_id = ?'), (request_id,))
        row = cursor.fetchone()
        if row is None:
            return None
    if fields:
        row = _update_request_row_fields(row, fields)
    return Request.from_row(row)


async def _get_request_no_lock_async(request_id: str,
                                     fields: list[str] | None = None
                                    ) -> Request | None:
    """Async version of _get_request_no_lock."""
    assert _DB is not None
    columns_str = ', '.join(REQUEST_COLUMNS)
    if fields:
        columns_str = ', '.join(fields)
    # Exact match on the primary key; see _get_request_no_lock.
    async with _DB.execute_fetchall_async(
        (f'SELECT {columns_str} FROM {REQUEST_TABLE} '
         'WHERE request_id = ?'), (request_id,)) as rows:
        row = rows[0] if rows else None
        if row is None:
            return None
    if fields:
        row = _update_request_row_fields(row, fields)
    return Request.from_row(row)


@metrics_lib.time_me
async def get_latest_request_id_async() -> str | None:
    """Get the latest request ID."""
    return await request_storage.get_request_backend(
    ).get_latest_request_id_async()


@metrics_lib.time_me
def get_request(request_id: str,
                fields: list[str] | None = None) -> Request | None:
    """Get a SkyPilot API request."""
    return request_storage.get_request_backend().get_request(request_id, fields)


@metrics_lib.time_me_async
@asyncio_utils.shield
async def get_request_async(request_id: str,
                            fields: list[str] | None = None) -> Request | None:
    """Async version of get_request."""
    return await request_storage.get_request_backend().get_request_async(
        request_id, fields)


@metrics_lib.time_me
def get_requests_with_prefix(
        request_id_prefix: str,
        fields: list[str] | None = None) -> list[Request] | None:
    """Get requests with a given request ID prefix."""
    return request_storage.get_request_backend().get_requests_with_prefix(
        request_id_prefix, fields)


@metrics_lib.time_me_async
@asyncio_utils.shield
async def get_requests_async_with_prefix(
        request_id_prefix: str,
        fields: list[str] | None = None) -> list[Request] | None:
    """Async version of get_request_with_prefix."""
    return await request_storage.get_request_backend(
    ).get_requests_async_with_prefix(request_id_prefix, fields)


class StatusWithMsg(NamedTuple):
    status: RequestStatus
    status_msg: str | None = None


@metrics_lib.time_me_async
async def get_request_status_async(
    request_id: str,
    include_msg: bool = False,
) -> StatusWithMsg | None:
    """Get the status of a request.

    Args:
        request_id: The ID of the request.
        include_msg: Whether to include the status message.

    Returns:
        The status of the request. If the request is not found, returns
        None.
    """
    return await request_storage.get_request_backend().get_request_status_async(
        request_id, include_msg)


@metrics_lib.time_me_async
@asyncio_utils.shield
async def create_if_not_exists_async(request: Request) -> bool:
    """Create a request if it does not exist, otherwise do nothing.

    Returns:
        True if a new request is created, False if the request already exists.
    """
    return await request_storage.get_request_backend(
    ).create_if_not_exists_async(request)


def build_internal_daemon_request(
        daemon: 'daemons.InternalRequestDaemon') -> Request:
    """Build a fresh `Request` for an internal daemon.

    Captures the current process's `os.environ` via `payloads.RequestBody()`.
    Status starts at PENDING with no `pid`. The returned object is not yet
    persisted.
    """
    body = payloads.RequestBody()
    return Request(
        request_id=daemon.id,
        name=server_constants.REQUEST_NAME_PREFIX + daemon.name,
        entrypoint=daemon.run_event,
        request_body=body,
        status=RequestStatus.PENDING,
        created_at=time.time(),
        schedule_type=ScheduleType.SHORT,
        user_id=skylet_constants.SKYPILOT_SYSTEM_USER_ID,
        # Matches the retryable=True used when scheduling daemon requests
        # (executor.schedule_internal_daemon_async).
        retryable=True,
        should_enqueue=True,
    )


async def create_or_refresh_internal_daemon_async(request: Request) -> bool:
    """Insert or refresh an internal daemon's row.

    Thin module-level wrapper. See
    `RequestBackend.create_or_refresh_internal_daemon_async` for the
    contract.
    """
    return await request_storage.get_request_backend(
    ).create_or_refresh_internal_daemon_async(request)


async def delete_orphan_internal_daemons_async(
    internal_daemons: list['daemons.InternalRequestDaemon'],) -> None:
    """Delete persisted daemon rows whose id is not in `internal_daemons`.

    Thin module-level wrapper. See
    `RequestBackend.delete_orphan_internal_daemons_async` for the
    contract.
    """
    return await request_storage.get_request_backend(
    ).delete_orphan_internal_daemons_async(internal_daemons)


@dataclasses.dataclass
class RequestTaskFilter:
    """Filter for requests.

    Args:
        status: a list of statuses of the requests to filter on.
        cluster_names: a list of cluster names to filter requests on.
        exclude_request_names: a list of request names to exclude from results.
            Mutually exclusive with include_request_names.
        user_id: the user ID to filter requests on.
            If None, all users are included.
        include_request_names: a list of request names to filter on.
            Mutually exclusive with exclude_request_names.
        finished_before: if provided, only include requests finished before this
            timestamp.
        include_missing_finished_at: when used with ``finished_before``, also
            include terminal requests whose row has no ``finished_at``
            timestamp, using ``created_at`` as the fallback.
        finished_after: if provided, only include requests finished at or after
            this timestamp. Requests still in progress (finished_at IS NULL)
            are always included.
        retention_safe: internal GC guard that excludes correlated requests
            until their exact resource-action attempt is settled. PostgreSQL
            enforces this; SQLite has no central resource-action correlation.
        limit: the number of requests to show. If None, show all requests.

    Raises:
        ValueError: If both exclude_request_names and include_request_names are
            provided.
    """
    status: list[RequestStatus] | None = None
    cluster_names: list[str] | None = None
    user_id: str | None = None
    exclude_request_names: list[str] | None = None
    include_request_names: list[str] | None = None
    finished_before: float | None = None
    include_missing_finished_at: bool = False
    finished_after: float | None = None
    retention_safe: bool = False
    limit: int | None = None
    fields: list[str] | None = None
    sort: bool = False

    def __post_init__(self):
        if (self.exclude_request_names is not None and
                self.include_request_names is not None):
            raise ValueError(
                'Only one of exclude_request_names or include_request_names '
                'can be provided, not both.')

    def build_query(self) -> tuple[str, list[Any]]:
        """Build the SQL query and filter parameters.

        Returns:
            A tuple of (SQL, SQL parameters).
        """
        filters = []
        filter_params: list[Any] = []
        if self.status is not None:
            status_list_str = ','.join(
                repr(status.value) for status in self.status)
            filters.append(f'status IN ({status_list_str})')
        if self.include_request_names is not None:
            request_names_str = ','.join(
                repr(name) for name in self.include_request_names)
            filters.append(f'name IN ({request_names_str})')
        if self.exclude_request_names is not None:
            exclude_request_names_str = ','.join(
                repr(name) for name in self.exclude_request_names)
            filters.append(f'name NOT IN ({exclude_request_names_str})')
        if self.cluster_names is not None:
            if len(self.cluster_names) == 0:
                # Empty IN () is invalid SQL in PostgreSQL.
                # An empty list means "match nothing".
                filters.append('1=0')
            else:
                cluster_names_str = ','.join(
                    repr(name) for name in self.cluster_names)
                filters.append(f'{COL_CLUSTER_NAME} IN ({cluster_names_str})')
        if self.user_id is not None:
            filters.append(f'{COL_USER_ID} = ?')
            filter_params.append(self.user_id)
        if self.finished_before is not None:
            if self.include_missing_finished_at:
                terminal_statuses = ','.join(
                    repr(status.value)
                    for status in RequestStatus.finished_status())
                filters.append(
                    '(finished_at < ? OR (finished_at IS NULL AND '
                    f'status IN ({terminal_statuses}) AND created_at < ?))')
                filter_params.extend(
                    [self.finished_before, self.finished_before])
            else:
                filters.append('finished_at < ?')
                filter_params.append(self.finished_before)
        if self.finished_after is not None:
            filters.append('(finished_at >= ? OR finished_at IS NULL)')
            filter_params.append(self.finished_after)
        filter_str = ' AND '.join(filters)
        if filter_str:
            filter_str = f' WHERE {filter_str}'
        columns_str = ', '.join(REQUEST_COLUMNS)
        if self.fields:
            columns_str = ', '.join(self.fields)
        sort_str = ''
        if self.sort:
            sort_str = ' ORDER BY created_at DESC'
        query_str = (f'SELECT {columns_str} FROM {REQUEST_TABLE}{filter_str}'
                     f'{sort_str}')
        if self.limit is not None:
            query_str += f' LIMIT {self.limit}'
        return query_str, filter_params


@metrics_lib.time_me
def get_request_tasks(req_filter: RequestTaskFilter) -> list[Request]:
    """Get a list of requests that match the given filters.

    Args:
        req_filter: the filter to apply to the requests. Refer to
            RequestTaskFilter for the details.
    """
    return request_storage.get_request_backend().query_requests(req_filter)


@metrics_lib.time_me_async
async def get_request_tasks_async(
        req_filter: RequestTaskFilter) -> list[Request]:
    """Async version of get_request_tasks."""
    return await request_storage.get_request_backend().query_requests_async(
        req_filter)


@metrics_lib.time_me_async
async def get_api_request_ids_start_with(incomplete: str) -> list[str]:
    """Get a list of API request ids for shell completion."""
    return await request_storage.get_request_backend(
    ).get_api_request_ids_start_with(incomplete)


def get_active_file_mounts_blob_ids() -> set:
    """Get file_mounts_blob_ids referenced by active requests."""
    return request_storage.get_request_backend(
    ).get_active_file_mounts_blob_ids()


_add_or_update_request_sql = (f'INSERT OR REPLACE INTO {REQUEST_TABLE} '
                              f'({", ".join(REQUEST_COLUMNS)}) VALUES '
                              f'({", ".join(["?"] * len(REQUEST_COLUMNS))})')

_EXECUTABLE_STATUS_VALUES = tuple(
    s.value for s in RequestStatus.executable_statuses())
_TERMINAL_STATUS_VALUES = tuple(
    s.value for s in RequestStatus.finished_status())

_try_mark_running_sql = (
    f'UPDATE {REQUEST_TABLE} SET status = ?, pid = ?, '
    f'{COL_STATUS_MSG} = NULL WHERE request_id = ? AND status IN '
    f'({", ".join(["?"] * len(_EXECUTABLE_STATUS_VALUES))})')


def _finish_request_update_sql(request_id: str, status: RequestStatus,
                               name: str | None, error: BaseException | None,
                               result: Any | None) -> tuple[str, list[Any]]:
    """Build the targeted UPDATE that persists a terminal status.

    Only the transitioned scalar columns are written; entrypoint and
    request_body are never rewritten after insert. The NOT-IN guard makes
    the terminal write and the shutdown sweep's CANCELLED+should_retry
    marker (interrupt_request_for_retry) mutually exclusive: whichever
    lands first wins, mirroring the terminal-status guard on the kill
    paths. The guard alone only protects UPDATE-vs-UPDATE ordering;
    executing it must additionally hold the per-request FileLock so it
    also serializes with full-row read-modify-write writers
    (update_request / update_request_async).
    """
    serialized_result = None
    result_encoding_failed = False
    should_encode_result = result is not None
    if (name is not None and status == RequestStatus.SUCCEEDED and
            encoders.requires_strict_return_value(name)):
        should_encode_result = True
    if should_encode_result:
        assert name is not None, request_id
        serializer = return_value_serializers.get_serializer(name)
        # A serializer failure must not raise: an exception here escapes the
        # executor wrapper after its try/except and leaves the row stuck in
        # RUNNING forever (the same hazard `_encoded_return_value` guards
        # against). Surface it as a request failure instead of a silent
        # success with a null return value.
        try:
            serialized_result = serializer(
                _encoded_return_value(name, request_id, result))
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to serialize return value for request '
                f'{request_id} ({name}); marking the request failed.',
                exc_info=True)
            status = RequestStatus.FAILED
            result_encoding_failed = True
            if error is None:
                error = e
    set_clauses = ['status = ?', f'{COL_FINISHED_AT} = ?']
    params: list[Any] = [status.value, time.time()]
    if serialized_result is not None:
        set_clauses.append('return_value = ?')
        params.append(serialized_result)
    elif result_encoding_failed:
        # Do not retain a result from an earlier delivery when validating the
        # terminal result for this delivery failed.
        set_clauses.append('return_value = ?')
        params.append(return_value_serializers.default_serializer(None))
    if error is not None:
        set_clauses.append('error = ?')
        params.append(orjson.dumps(_build_error_dict(error)).decode('utf-8'))
    sql = (f'UPDATE {REQUEST_TABLE} SET {", ".join(set_clauses)} '
           f'WHERE request_id = ? AND status NOT IN '
           f'({", ".join(["?"] * len(_TERMINAL_STATUS_VALUES))})')
    params.append(request_id)
    params.extend(_TERMINAL_STATUS_VALUES)
    return sql, params


def _add_or_update_request_no_lock(request: Request):
    """Add or update a REST request into the database."""
    assert _DB is not None
    if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
        logger.debug(f'Start adding or updating request {request.request_id}')
    try:
        with _DB.conn:
            cursor = _DB.conn.cursor()
            cursor.execute(_add_or_update_request_sql, request.to_row())
    finally:
        if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
            logger.debug(f'End adding or updating request {request.request_id}')


async def _add_or_update_request_no_lock_async(request: Request):
    """Async version of _add_or_update_request_no_lock."""
    assert _DB is not None
    await _DB.execute_and_commit_async(_add_or_update_request_sql,
                                       request.to_row())


def set_exception_stacktrace(e: BaseException) -> None:
    with ux_utils.enable_traceback():
        stacktrace = traceback.format_exc()
    setattr(e, 'stacktrace', stacktrace)


def _set_value_free_exception_stacktrace(e: BaseException) -> None:
    """Attaches an exception-only trace without the active raw exception."""
    stacktrace = ''.join(traceback.format_exception_only(type(e), e))
    setattr(e, 'stacktrace', stacktrace)


def _mark_container_image_request_terminal(request_id: str) -> None:
    """Best-effort image-fence observation after request state is durable."""
    try:
        database = global_user_state.initialize_and_get_db()
        if (database.dialect.name
                != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
            return
        # Importing at module load would create requests -> demand_state ->
        # global_user_state -> server initialization recursion.
        # pylint: disable=import-outside-toplevel
        from sky.container_images import demand_state as image_demand_state

        # pylint: enable=import-outside-toplevel
        image_demand_state.mark_cluster_request_terminal(request_id)
    except Exception as e:  # pylint: disable=broad-except
        # Losing this hint is fail-safe: reconciliation retains the fence.
        logger.warning('Failed to record container image request termination: '
                       f'{common_utils.format_exception(e)}')


def set_request_failed(request_id: str, e: BaseException) -> None:
    """Set a request to failed and populate the error message."""
    request = get_request(request_id, fields=['name', 'request_body'])
    sanitized_error = (sanitize_request_error(
        request.name, e, request.request_body) if request is not None else e)
    if sanitized_error is not e:
        e = sanitized_error
        _set_value_free_exception_stacktrace(e)
    else:
        set_exception_stacktrace(e)
    transitioned = request_storage.get_request_backend(
    ).transition_request_terminal(request_id,
                                  RequestStatus.FAILED,
                                  'handler_failed',
                                  error=e)
    # Older plugin backends may still return None from this internal hook.
    if transitioned is not False:
        _mark_container_image_request_terminal(request_id)


@metrics_lib.time_me_async
@asyncio_utils.shield
async def set_request_failed_async(request_id: str, e: BaseException) -> None:
    """Set a request to failed and populate the error message."""
    request = await get_request_async(request_id,
                                      fields=['name', 'request_body'])
    sanitized_error = (sanitize_request_error(
        request.name, e, request.request_body) if request is not None else e)
    if sanitized_error is not e:
        e = sanitized_error
        _set_value_free_exception_stacktrace(e)
    else:
        set_exception_stacktrace(e)
    transitioned = await request_storage.get_request_backend(
    ).transition_request_terminal_async(request_id,
                                        RequestStatus.FAILED,
                                        'handler_failed',
                                        error=e)
    if transitioned is not False:
        await asyncio.to_thread(_mark_container_image_request_terminal,
                                request_id)


def set_request_succeeded(request_id: str, result: Any | None) -> None:
    """Set a request to succeeded and populate the result."""
    transitioned = request_storage.get_request_backend(
    ).transition_request_terminal(request_id,
                                  RequestStatus.SUCCEEDED,
                                  'handler_succeeded',
                                  result=result)
    if transitioned is not False:
        _mark_container_image_request_terminal(request_id)


@metrics_lib.time_me_async
@asyncio_utils.shield
async def set_request_succeeded_async(request_id: str,
                                      result: Any | None) -> None:
    """Set a request to succeeded and populate the result."""
    transitioned = await request_storage.get_request_backend(
    ).transition_request_terminal_async(request_id,
                                        RequestStatus.SUCCEEDED,
                                        'handler_succeeded',
                                        result=result)
    if transitioned is not False:
        await asyncio.to_thread(_mark_container_image_request_terminal,
                                request_id)


@metrics_lib.time_me_async
@asyncio_utils.shield
async def set_request_cancelled_async(request_id: str) -> None:
    """Set a pending or running request to cancelled."""
    storage = request_storage.get_request_backend()
    async with storage.update_request_async(request_id) as request_task:
        assert request_task is not None, request_id
        # Already finished or cancelled.
        if request_task.status > RequestStatus.RUNNING:
            return
        request_task.finished_at = time.time()
        request_task.status = RequestStatus.CANCELLED
        request_task.terminal_cause = 'coroutine_disconnected'
    await asyncio.to_thread(_mark_container_image_request_terminal, request_id)


@metrics_lib.time_me
async def _delete_requests(request_ids: list[str]):
    """Clean up requests by their IDs."""
    await request_storage.get_request_backend().delete_requests(request_ids)


# TODO Remove this function on or after v0.15.0
def _get_legacy_log_path(request_id: str) -> pathlib.Path:
    """Get the legacy log path for a request (for backward compatibility).

    This is used during GC to clean up log files from the old location
    (~/sky_logs/api_server/requests/) after server upgrades.
    """
    legacy_path_prefix = pathlib.Path(
        LEGACY_REQUEST_LOG_PATH_PREFIX).expanduser().absolute()
    return (legacy_path_prefix / request_id).with_suffix('.log')


# TODO Remove this function on or after v0.15.0
def _cleanup_legacy_directory_if_empty_sync():
    """Synchronously remove the legacy request log directory if empty.

    This helps clean up the legacy directory once all old logs have been
    garbage collected after a server upgrade.
    """
    legacy_path = pathlib.Path(LEGACY_REQUEST_LOG_PATH_PREFIX).expanduser()
    try:
        if not legacy_path.exists():
            return
        # Check if directory is empty (no .log or .lock files)
        if not any(legacy_path.iterdir()):
            logger.info(f'Removing empty legacy log directory: {legacy_path}')
            legacy_path.rmdir()
    except Exception as e:  # pylint: disable=broad-except
        # Don't fail GC if cleanup fails
        logger.debug(f'Failed to cleanup legacy directory: {e}')


async def _cleanup_legacy_directory_if_empty():
    """Remove the legacy request log directory if empty."""
    await asyncio.to_thread(_cleanup_legacy_directory_if_empty_sync)


async def clean_finished_requests_with_retention(
        retention_seconds: int,
        batch_size: int = 1000,
        include_request_names: list[str] | None = None):
    """Clean up finished requests older than the retention period.

    This function removes old finished requests (SUCCEEDED, FAILED, CANCELLED)
    from the database and cleans up their associated log files.

    For backward compatibility, it also cleans up log files from the legacy
    path (~/sky_logs/api_server/requests/) to handle server upgrades.

    Args:
        retention_seconds: Requests older than this many seconds will be
            deleted.
        batch_size: batch delete 'batch_size' requests at a time to
            avoid using too much memory and once and to let each
            db query complete in a reasonable time. All stale
            requests older than the retention period will be deleted
            regardless of the batch size.
        include_request_names: If set, clean only these request names.
    """
    debug_log_dir = pathlib.Path(sky_logging.DEBUG_LOG_DIR)
    total_deleted = 0
    while True:
        reqs = await get_request_tasks_async(
            req_filter=RequestTaskFilter(status=RequestStatus.finished_status(),
                                         include_request_names=(
                                             include_request_names),
                                         finished_before=time.time() -
                                         retention_seconds,
                                         include_missing_finished_at=True,
                                         retention_safe=True,
                                         limit=batch_size,
                                         fields=['request_id']))
        if len(reqs) == 0:
            break
        futs = []
        for req in reqs:
            # req.log_path is derived from request_id,
            # so it's ok to just grab the request_id in the above query.
            # Delete from current path
            futs.append(
                asyncio.create_task(
                    anyio.Path(
                        req.log_path.absolute()).unlink(missing_ok=True)))
            # Also delete from legacy path for backward compatibility
            # TODO Remove this on or after v0.15.0
            legacy_log_path = _get_legacy_log_path(req.request_id)
            futs.append(
                asyncio.create_task(
                    anyio.Path(legacy_log_path).unlink(missing_ok=True)))
            # Delete debug log if it exists
            debug_log_path = (debug_log_dir /
                              req.request_id).with_suffix('.log')
            futs.append(
                asyncio.create_task(
                    anyio.Path(debug_log_path).unlink(missing_ok=True)))
            # Delete the per-request lock file, which otherwise accumulates
            # for the whole server uptime. Safe: the request finished longer
            # ago than the retention, and the lock file is recreated
            # harmlessly if a late reader locks this id again.
            futs.append(
                asyncio.create_task(
                    anyio.Path(request_lock_path(
                        req.request_id)).unlink(missing_ok=True)))
        await asyncio.gather(*futs)

        await _delete_requests([req.request_id for req in reqs])
        total_deleted += len(reqs)
        if len(reqs) < batch_size:
            break

    # Try to clean up the legacy directory if it's empty
    # TODO Remove this on or after v0.15.0
    await _cleanup_legacy_directory_if_empty()

    # To avoid leakage of the log file, logs must be deleted before the
    # request task in the database.
    logger.info(f'Cleaned up {total_deleted} finished requests '
                f'older than {retention_seconds} seconds')


async def cleanup_streaming_requests_under_pressure(
        usage: RequestLogStorageUsage | None = None) -> bool:
    """Reclaim terminal streaming spools only when disk headroom is low."""
    if usage is None:
        usage = get_request_log_storage_usage()
    if usage.free_bytes >= usage.soft_free_bytes:
        return False
    logger.warning(
        'Request-log filesystem pressure detected: '
        f'free={usage.free_bytes} soft_limit={usage.soft_free_bytes}; '
        'cleaning terminal streaming requests')
    if usage.free_bytes < usage.hard_free_bytes:
        # Process-backed streams do not use the coroutine log writer. Stop all
        # active streaming producers once the hard reserve is crossed, then
        # collect their now-terminal spools below.
        active_requests = await get_request_tasks_async(
            req_filter=RequestTaskFilter(status=RequestStatus.active_statuses(),
                                         include_request_names=list(
                                             STREAMING_REQUEST_NAMES),
                                         fields=['request_id']))
        results = await asyncio.gather(
            *(kill_request_async(req.request_id) for req in active_requests),
            return_exceptions=True)
        cancelled = sum(result is True for result in results)
        failures = sum(isinstance(result, BaseException) for result in results)
        logger.warning('Hard request-log reserve crossed: '
                       f'cancelled={cancelled} active streaming requests; '
                       f'cancel_failures={failures}')
    # Leave newly cancelled rows visible long enough for their executors to
    # observe cancellation and stop. The next pressure pass reclaims them.
    await clean_finished_requests_with_retention(
        _REQUEST_LOG_PRESSURE_CLEANUP_GRACE_SECONDS,
        include_request_names=list(STREAMING_REQUEST_NAMES))
    updated_usage = get_request_log_storage_usage()
    logger.info('Request-log pressure cleanup finished: '
                f'free={updated_usage.free_bytes} '
                f'soft_limit={updated_usage.soft_free_bytes}')
    return True


async def requests_gc_daemon():
    """Garbage collect finished requests periodically."""
    last_retention_gc = float('-inf')
    last_pressure_cleanup = float('-inf')
    last_hard_pressure_cleanup = float('-inf')
    while True:
        now = time.monotonic()
        # Protect the disk reserve before starting the potentially longer
        # ordinary retention pass, especially during server startup.
        try:
            usage = get_request_log_storage_usage()
            if usage.free_bytes >= usage.soft_free_bytes:
                # A new pressure episode should reclaim terminal streams
                # immediately instead of inheriting the previous cooldown.
                last_pressure_cleanup = float('-inf')
                last_hard_pressure_cleanup = float('-inf')
            elif (usage.free_bytes < usage.hard_free_bytes and
                  now - last_hard_pressure_cleanup
                  >= _REQUEST_LOG_HARD_PRESSURE_CLEANUP_INTERVAL_SECONDS):
                # Crossing the hard reserve bypasses a recent soft cleanup,
                # but repeated emergency database work remains bounded.
                last_hard_pressure_cleanup = now
                last_pressure_cleanup = now
                await cleanup_streaming_requests_under_pressure(usage)
            elif (usage.free_bytes >= usage.hard_free_bytes and
                  now - last_pressure_cleanup
                  >= _REQUEST_LOG_PRESSURE_CLEANUP_INTERVAL_SECONDS):
                # Advance before the database query so a failure cannot spin
                # on every O(1) filesystem probe.
                last_pressure_cleanup = now
                await cleanup_streaming_requests_under_pressure(usage)
        except asyncio.CancelledError:
            logger.info('Requests GC daemon cancelled')
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Error running request-log pressure cleanup: {e}; '
                         f'traceback: {traceback.format_exc()}')
        if now - last_retention_gc >= _GC_INTERVAL_SECONDS:
            # Advance the deadline before touching config or the database. A
            # broken normal-retention pass must not turn an hourly operation
            # into a retry every pressure-check tick.
            last_retention_gc = now
            try:
                logger.info('Running requests GC daemon...')
                # Use the latest config for the normal retention pass.
                skypilot_config.reload_config()
                retention_seconds = skypilot_config.get_nested(
                    ('api_server', 'requests_retention_hours'),
                    DEFAULT_REQUESTS_RETENTION_HOURS) * 3600
                # Negative value disables normal retention, but pressure
                # cleanup remains a safety invariant.
                if retention_seconds >= 0:
                    await clean_finished_requests_with_retention(
                        retention_seconds)
            except asyncio.CancelledError:
                logger.info('Requests GC daemon cancelled')
                raise
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'Error running normal requests GC: {e}; '
                             f'traceback: {traceback.format_exc()}')
        await asyncio.sleep(_REQUEST_LOG_PRESSURE_CHECK_INTERVAL_SECONDS)


async def close_db_async() -> None:
    """Close this process's SQLite request DB, if it was initialized.

    Uvicorn workers must call this before their serving event loop exits.
    Otherwise, the non-daemon aiosqlite connection thread can keep a worker
    alive after application startup fails. Plugin request backends do not
    initialize this module-level database, so this is a no-op for them.
    """
    global _DB
    with _init_db_lock:
        db = _DB
        _DB = None
    if db is not None:
        await db.close()


def _cleanup():
    asyncio.run(close_db_async())


atexit.register(_cleanup)


class SqliteRequestBackend(request_storage.RequestBackend):
    """SQLite-based request backend."""

    @init_db
    def get_request(self,
                    request_id: str,
                    fields: list[str] | None = None) -> Request | None:
        with filelock.FileLock(request_lock_path(request_id)):
            return _get_request_no_lock(request_id, fields)

    @init_db_async
    @asyncio_utils.shield
    async def get_request_async(
            self,
            request_id: str,
            fields: list[str] | None = None) -> Request | None:
        async with filelock.AsyncFileLock(request_lock_path(request_id)):
            return await _get_request_no_lock_async(request_id, fields)

    @contextlib.contextmanager
    def update_request(
            self, request_id: str) -> Generator[Request | None, None, None]:
        _ensure_db_initialized()
        with filelock.FileLock(request_lock_path(request_id)):
            request = _get_request_no_lock(request_id)
            yield request
            if request is not None:
                _add_or_update_request_no_lock(request)

    @contextlib.asynccontextmanager
    async def update_request_async(self, request_id: str):
        _ensure_db_initialized()
        async with filelock.AsyncFileLock(request_lock_path(request_id)):
            request = await _get_request_no_lock_async(request_id)
            yield request
            if request is not None:
                await _add_or_update_request_no_lock_async(request)

    @init_db_async
    @asyncio_utils.shield
    @db_utils.retry_on_sqlite_busy_async
    async def create_if_not_exists_async(self, request: Request) -> bool:
        request_cutover.require_legacy_submissions_allowed()
        assert _DB is not None
        request_columns = ', '.join(REQUEST_COLUMNS)
        values_str = ', '.join(['?'] * len(REQUEST_COLUMNS))
        sql_statement = (f'INSERT INTO {REQUEST_TABLE} '
                         f'({request_columns}) VALUES '
                         f'({values_str}) ON CONFLICT(request_id) DO NOTHING '
                         f'RETURNING ROWID')
        request_row = request.to_row()
        if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
            logger.debug(f'Start creating request {request.request_id}')
        try:
            row = await _DB.execute_get_returning_value_async(
                sql_statement, request_row)
        finally:
            if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
                logger.debug(f'End creating request {request.request_id}')
        return True if row else False

    @init_db_async
    @asyncio_utils.shield
    async def create_or_refresh_internal_daemon_async(self,
                                                      request: Request) -> bool:
        assert _DB is not None
        # Try insert first (the dedup primitive: only one concurrent
        # caller wins the conflict).
        inserted = await self.create_if_not_exists_async(request)
        if inserted:
            return True
        # Lost the insert race: an existing row remains. UPDATE the
        # env-bearing columns so the persisted row reflects this
        # process's `os.environ` (and the matching `name` /
        # `schedule_type` from the current code). Concurrent UPDATEs
        # from sibling uvicorn workers in the same process write the
        # same values; cross-pod UPDATEs from a newer generation win
        # by virtue of happening last.
        encoded_body = encoders.pickle_and_encode(request.request_body)
        await _DB.execute_and_commit_async(
            f'UPDATE {REQUEST_TABLE} '
            f'SET request_body=?, name=?, schedule_type=? '
            f'WHERE request_id=?',
            (encoded_body, request.name, request.schedule_type.value,
             request.request_id))
        return False

    @init_db_async
    @asyncio_utils.shield
    async def delete_orphan_internal_daemons_async(
        self,
        internal_daemons: list['daemons.InternalRequestDaemon'],
    ) -> None:
        assert _DB is not None
        keep_ids = {d.id for d in internal_daemons}
        # SQLite has no `is_daemon` column; use the `*-daemon` naming
        # convention (verified against sky/server/daemons.py).
        # TODO(cooperc): replace LIKE with a dedicated marker column if
        # a non-daemon request_id ever ends in `-daemon`.
        async with _DB.execute_fetchall_async(
            f'SELECT request_id FROM {REQUEST_TABLE} '
            f'WHERE request_id LIKE \'%-daemon\'') as rows:
            existing = [r[0] for r in rows if r[0].endswith('-daemon')]
        stale_ids = [rid for rid in existing if rid not in keep_ids]
        if not stale_ids:
            return
        id_list_str = ','.join(repr(rid) for rid in stale_ids)
        await _DB.execute_and_commit_async(
            f'DELETE FROM {REQUEST_TABLE} '
            f'WHERE request_id IN ({id_list_str})')
        logger.info(f'Deleted orphan internal daemon rows: {stale_ids}')

    @init_db
    def query_requests(self, req_filter: RequestTaskFilter) -> list[Request]:
        assert _DB is not None
        with _DB.conn:
            cursor = _DB.conn.cursor()
            cursor.execute(*req_filter.build_query())
            rows = cursor.fetchall()
            if rows is None:
                return []
        if req_filter.fields:
            rows = [
                _update_request_row_fields(row, req_filter.fields)
                for row in rows
            ]
        return [Request.from_row(row) for row in rows]

    @init_db_async
    async def query_requests_async(
            self, req_filter: RequestTaskFilter) -> list[Request]:
        assert _DB is not None
        async with _DB.execute_fetchall_async(
                *req_filter.build_query()) as rows:
            if not rows:
                return []
        if req_filter.fields:
            rows = [
                _update_request_row_fields(row, req_filter.fields)
                for row in rows
            ]
        return [Request.from_row(row) for row in rows]

    @init_db_async
    @init_db_async
    async def delete_requests(self, request_ids: list[str]) -> None:
        if not request_ids:
            return
        assert _DB is not None
        id_list_str = ','.join(repr(rid) for rid in request_ids)
        if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
            logger.debug(f'Start deleting requests {request_ids}')
        try:
            await _DB.execute_and_commit_async(
                f'DELETE FROM {REQUEST_TABLE} '
                f'WHERE request_id IN ({id_list_str})')
        finally:
            if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
                logger.debug(f'End deleting requests {request_ids}')

    # --- Status updates ---

    @init_db_async
    @asyncio_utils.shield
    async def update_status_async(self, request_id: str,
                                  status: RequestStatus) -> None:
        async with filelock.AsyncFileLock(request_lock_path(request_id)):
            request = await _get_request_no_lock_async(request_id)
            if request is not None:
                request.status = status
                await _add_or_update_request_no_lock_async(request)

    @init_db_async
    @asyncio_utils.shield
    async def update_status_msg_async(self, request_id: str,
                                      status_msg: str) -> None:
        async with filelock.AsyncFileLock(request_lock_path(request_id)):
            request = await _get_request_no_lock_async(request_id)
            if request is not None:
                request.status_msg = status_msg
                await _add_or_update_request_no_lock_async(request)

    @init_db
    @db_utils.retry_on_sqlite_busy
    def try_mark_running(self,
                         request_id: str,
                         pid: int | None,
                         execution_generation: int = 0,
                         claim_token: str | None = None) -> bool:
        del execution_generation, claim_token
        assert _DB is not None
        # The per-request FileLock is required for composition with
        # update_request()'s full-row read-modify-write writers (kill
        # paths, interrupt_request_for_retry): the status IN (...) guard
        # is atomic only at UPDATE time and cannot protect against a
        # writer that read the row before this UPDATE and later REPLACEs
        # the full (stale) row back.
        with filelock.FileLock(request_lock_path(request_id)):
            with _DB.conn:
                cursor = _DB.conn.cursor()
                cursor.execute(_try_mark_running_sql,
                               (RequestStatus.RUNNING.value, pid, request_id) +
                               _EXECUTABLE_STATUS_VALUES)
                return cursor.rowcount == 1

    @init_db
    @db_utils.retry_on_sqlite_busy
    def set_request_finished(self,
                             request_id: str,
                             status: RequestStatus,
                             error: BaseException | None = None,
                             result: Any | None = None) -> bool:
        assert _DB is not None
        name = None
        if result is not None or status == RequestStatus.SUCCEEDED:
            # The return-value encoder is looked up by request name; a
            # single-column primary-key read is far cheaper than the full
            # row (which would unpickle entrypoint and request_body).
            with _DB.conn:
                cursor = _DB.conn.cursor()
                cursor.execute(
                    f'SELECT name FROM {REQUEST_TABLE} WHERE request_id = ?',
                    (request_id,))
                row = cursor.fetchone()
            if row is None:
                return False
            name = row[0]
        sql, params = _finish_request_update_sql(request_id, status, name,
                                                 error, result)
        # The per-request FileLock is required for composition with
        # update_request()'s full-row read-modify-write writers (kill
        # paths, interrupt_request_for_retry): the NOT IN (...) status
        # guard alone is insufficient against a writer that read the row
        # before this UPDATE and later REPLACEs the full (stale) row back,
        # which would clobber the terminal result written here.
        with filelock.FileLock(request_lock_path(request_id)):
            with _DB.conn:
                cursor = _DB.conn.cursor()
                cursor.execute(sql, params)
                return cursor.rowcount == 1

    @init_db_async
    @asyncio_utils.shield
    @db_utils.retry_on_sqlite_busy_async
    async def set_request_finished_async(self,
                                         request_id: str,
                                         status: RequestStatus,
                                         error: BaseException | None = None,
                                         result: Any | None = None) -> bool:
        assert _DB is not None
        name = None
        if result is not None or status == RequestStatus.SUCCEEDED:
            async with _DB.execute_fetchall_async(
                    f'SELECT name FROM {REQUEST_TABLE} WHERE request_id = ?',
                (request_id,)) as rows:
                if not rows:
                    return False
                name = rows[0][0]
        sql, params = _finish_request_update_sql(request_id, status, name,
                                                 error, result)
        # See set_request_finished(): the per-request FileLock is required
        # for composition with update_request_async()'s full-row
        # read-modify-write writers; the SQL status guard alone cannot
        # prevent a stale full-row REPLACE from clobbering this write.
        async with filelock.AsyncFileLock(request_lock_path(request_id)):
            row = await _DB.execute_get_returning_value_async(
                f'{sql} RETURNING request_id', params)
            return row is not None

    @init_db
    def kill_requests(self,
                      request_ids: list[str] | None = None,
                      user_id: str | None = None) -> list[str]:
        if request_ids is None:
            request_ids = [
                r.request_id
                for r in self.query_requests(req_filter=RequestTaskFilter(
                    status=RequestStatus.active_statuses(),
                    exclude_request_names=['sky.api_cancel'],
                    user_id=user_id,
                    fields=['request_id']))
            ]
        cancelled = []
        for request_id in request_ids:
            with self.update_request(request_id) as request_record:
                if not _should_kill_request(request_id, request_record):
                    continue
                assert request_record is not None
                if request_record.pid is not None:
                    logger.debug(
                        f'Killing request process {request_record.pid}')
                    os.kill(request_record.pid, signal.SIGTERM)
                request_record.status = RequestStatus.CANCELLED
                request_record.finished_at = time.time()
                cancelled.append(request_id)
        return cancelled

    @init_db_async
    @asyncio_utils.shield
    async def kill_request_async(self, request_id: str) -> bool:
        async with filelock.AsyncFileLock(request_lock_path(request_id)):
            request = await _get_request_no_lock_async(request_id)
            if not _should_kill_request(request_id, request):
                return False
            assert request is not None
            if request.pid is not None:
                logger.debug(f'Killing request process {request.pid}')
                os.kill(request.pid, signal.SIGTERM)
            request.status = RequestStatus.CANCELLED
            request.finished_at = time.time()
            await _add_or_update_request_no_lock_async(request)
        return True

    # --- Specialized queries ---

    @init_db_async
    async def get_latest_request_id_async(self) -> str | None:
        assert _DB is not None
        async with _DB.execute_fetchall_async(
                f'SELECT request_id FROM {REQUEST_TABLE} '
                'ORDER BY created_at DESC LIMIT 1') as rows:
            return rows[0][0] if rows else None

    @init_db
    def get_requests_with_prefix(
            self,
            request_id_prefix: str,
            fields: list[str] | None = None) -> list[Request] | None:
        assert _DB is not None
        if fields:
            columns_str = ', '.join(fields)
        else:
            columns_str = ', '.join(REQUEST_COLUMNS)
        with _DB.conn:
            cursor = _DB.conn.cursor()
            cursor.execute((f'SELECT {columns_str} FROM {REQUEST_TABLE} '
                            'WHERE request_id LIKE ?'),
                           (request_id_prefix + '%',))
            rows = cursor.fetchall()
            if not rows:
                return None
            if fields:
                rows = [_update_request_row_fields(row, fields) for row in rows]
            return [Request.from_row(row) for row in rows]

    @init_db_async
    @asyncio_utils.shield
    async def get_requests_async_with_prefix(
            self,
            request_id_prefix: str,
            fields: list[str] | None = None) -> list[Request] | None:
        assert _DB is not None
        if fields:
            columns_str = ', '.join(fields)
        else:
            columns_str = ', '.join(REQUEST_COLUMNS)
        async with _DB.execute_fetchall_async(
            (f'SELECT {columns_str} FROM {REQUEST_TABLE} '
             'WHERE request_id LIKE ?'), (request_id_prefix + '%',)) as rows:
            if not rows:
                return None
            if fields:
                rows = [_update_request_row_fields(row, fields) for row in rows]
            return [Request.from_row(row) for row in rows]

    @init_db_async
    async def get_request_status_async(
            self,
            request_id: str,
            include_msg: bool = False) -> StatusWithMsg | None:
        assert _DB is not None
        columns = 'status'
        if include_msg:
            columns += ', status_msg'
        # Exact match on the primary key: this query runs in the /api/get
        # poll loop every 10-100ms per waiting client, and LIKE-prefix
        # matching would force a full table scan (see _get_request_no_lock).
        sql = (f'SELECT {columns} FROM {REQUEST_TABLE} '
               f'WHERE request_id = ?')
        async with _DB.execute_fetchall_async(sql, (request_id,)) as rows:
            if rows is None or len(rows) == 0:
                return None
            status = RequestStatus(rows[0][0])
            status_msg = rows[0][1] if include_msg else None
            return StatusWithMsg(status, status_msg)

    @init_db_async
    async def get_api_request_ids_start_with(self,
                                             incomplete: str) -> list[str]:
        assert _DB is not None
        async with _DB.execute_fetchall_async(
                f"""SELECT request_id FROM {REQUEST_TABLE}
                    WHERE request_id LIKE ?
                    ORDER BY
                        CASE
                            WHEN status IN ('PENDING', 'RUNNING') THEN 0
                            ELSE 1
                        END,
                        created_at DESC
                    LIMIT 1000""", (f'{incomplete}%',)) as rows:
            if not rows:
                return []
        return [row[0] for row in rows]

    @init_db
    def get_active_file_mounts_blob_ids(self) -> set[str]:
        assert _DB is not None
        with _DB.conn:
            cursor = _DB.conn.cursor()
            active_values = [s.value for s in RequestStatus.active_statuses()]
            placeholders = ', '.join('?' * len(active_values))
            cursor.execute(
                f'SELECT DISTINCT {COL_FILE_MOUNTS_BLOB_ID} '
                f'FROM {REQUEST_TABLE} '
                f'WHERE status IN ({placeholders}) '
                f'AND {COL_FILE_MOUNTS_BLOB_ID} IS NOT NULL', active_values)
            return {row[0] for row in cursor.fetchall()}

    def get_shutdown_active_requests(self) -> list[tuple[str, str]]:
        """Get (request_id, name) pairs to wait for during graceful shutdown."""

        # Wait on every non-terminal request. Use active_statuses() rather than
        # re-hardcoding the list: this query was the lone outlier that drifted
        # out of sync and silently dropped the (fork-added) WAITING status. A
        # request parked in WAITING -- on a retry backoff or an external
        # continue-condition, with its resume timer living only in an in-memory
        # monitor thread -- would otherwise be neither waited for nor handed to
        # interrupt_request_for_retry, so should_retry would never be set; its
        # timer would die with the process and reset_db_and_logs would wipe the
        # row on the next boot, silently dropping it on a clean restart.
        tasks = self.query_requests(
            RequestTaskFilter(
                status=RequestStatus.active_statuses(),
                fields=['request_id', 'name'],
            ))
        return [(t.request_id, t.name) for t in tasks]

    # --- Lifecycle ---

    def reset_on_startup(self) -> None:
        with _init_db_lock:
            _init_db_within_lock()
        assert _DB is not None
        with _DB.conn:
            cursor = _DB.conn.cursor()
            cursor.execute('SELECT sqlite_version()')
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError('Failed to get SQLite version')
            version_str = row[0]
            version_parts = version_str.split('.')
            assert len(version_parts) >= 2, \
                f'Invalid version string: {version_str}'
            major, minor = int(version_parts[0]), int(version_parts[1])
            if not ((major > 3) or (major == 3 and minor >= 35)):
                raise RuntimeError(
                    f'SQLite version {version_str} is not supported. '
                    'Please upgrade to SQLite 3.35.0 or later.')
