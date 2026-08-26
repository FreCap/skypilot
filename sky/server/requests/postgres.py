"""PostgreSQL request persistence and leased queue delivery."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Generator
from collections.abc import Mapping
import contextlib
import dataclasses
import datetime
import enum
import hashlib
import json
import math
import os
import signal
import threading
import time
import typing
from typing import Any
import uuid

import psutil
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import asyncio as sqlalchemy_async

import sky
from sky import backends
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.events import api_models as event_api_models
from sky.jobs import controller_fencing as managed_job_controller_fencing
from sky.serve import constants as serve_constants
from sky.server import constants as server_constants
from sky.server import daemons
from sky.server.events import emission as event_emission
from sky.server.events import models as event_models
from sky.server.requests import non_pool_launch as non_pool_launch_request
from sky.server.requests import ordinary_launch as ordinary_launch_request
from sky.server.requests import payloads
from sky.server.requests import postgres_schema
from sky.server.requests import preconditions
from sky.server.requests import registry as request_registry
from sky.server.requests import requests as requests_lib
from sky.server.requests import storage as request_storage
from sky.server.requests.queues import base as queue_base
from sky.skylet import constants as skylet_constants
from sky.utils import controller_capability
from sky.utils import locks
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

if typing.TYPE_CHECKING:
    from sky.serve import capacity_authority as capacity_authority_lib
    from sky.serve import ordinary_launch_binding as ordinary_launch_binding_lib
    from sky.serve import replica_managers

logger = sky_logging.init_logger(__name__)
ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')
capacity_admission = adaptors_common.LazyImport('sky.serve.capacity_admission')
capacity_authority = adaptors_common.LazyImport('sky.serve.capacity_authority')
capacity_policy = adaptors_common.LazyImport('sky.provision.capacity_policy')
paid_capacity = adaptors_common.LazyImport('sky.serve.paid_capacity')
serve_state = adaptors_common.LazyImport('sky.serve.serve_state')
serve_state_schema = adaptors_common.LazyImport('sky.serve.serve_state_schema')
serve_statuses = adaptors_common.LazyImport('sky.serve.serve_statuses')
route_projection_schema = adaptors_common.LazyImport(
    'sky.serve.route_projection_schema')
paid_retirement = adaptors_common.LazyImport('sky.serve.paid_retirement')
kueue_lane_lineage_schema = adaptors_common.LazyImport(
    'sky.serve.kueue_lane_lineage_schema')
zero_cost_actuation = adaptors_common.LazyImport(
    'sky.serve.zero_cost_actuation')
managed_job_state_schema = adaptors_common.LazyImport('sky.jobs.state_schema')

REQUEST_BACKEND_ENV_VAR = 'SKYPILOT_API_REQUEST_BACKEND'
POSTGRES_REQUEST_BACKEND = 'postgres'
ORDERED_CAPACITY_ADMISSION_PROTOCOL_VERSION = 2
ORDERED_CAPACITY_ADMISSION_COHORT_EPOCH = 2
POSTGRES_REQUEST_STORAGE_BACKEND_TYPE = (
    'sky.server.requests.postgres.PostgresRequestBackend')
POSTGRES_REQUEST_QUEUE_BACKEND_TYPE = (
    'sky.server.requests.postgres.PostgresQueueFactory')
EXECUTION_QUIESCENCE_BACKEND_GUARD_ENV_VAR = (
    'SKYPILOT_API_REQUIRE_EXECUTION_QUIESCENCE_BACKENDS')
SERVER_INSTANCE_ID_ENV_VAR = 'SKYPILOT_API_SERVER_INSTANCE_ID'
SERVER_ROLE_ENV_VAR = 'SKYPILOT_API_SERVER_ROLE'
CONTROLLER_GENERATION_ENV_VAR = (server_constants.CONTROLLER_GENERATION_ENV_VAR)
CONTROLLER_INSTANCE_ID_ENV_VAR = (
    server_constants.CONTROLLER_INSTANCE_ID_ENV_VAR)
ROLE_DRAIN_MARKER_PATH = request_storage.ROLE_DRAIN_MARKER_PATH

_CLAIM_LEASE_SECONDS = 30
_CLAIM_HEARTBEAT_INTERVAL_SECONDS = 10
_MAX_EXPIRED_CLAIMS_PER_SWEEP = 100
_GRACEFUL_SHUTDOWN_RETRY_REASON = (
    'Graceful server shutdown requested; waiting for exact execution '
    'quiescence before exposing retry.')
_INSTANCE_HEARTBEAT_INTERVAL_SECONDS = 5
# Public because operational safety checks outside the request backend must
# use the same freshness boundary as the instance registry itself.
INSTANCE_STALE_AFTER_SECONDS = 20
ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS = 70
_VALID_SERVER_ROLES = frozenset({'all', 'api', 'executor', 'controller'})
_CONTROLLER_LEADERSHIP_KEY = 'api-controller'
_CONTROLLER_LEADER_LOCK_ID = 'skypilot:api-controller-leader:v1'
_CONTROLLER_GENERATION_LOCK_PREFIX = ('skypilot:api-controller-generation:v1:')
_LEGACY_DAEMON_TRANSITION_LOCK_ID = ('skypilot:runtime-daemon-transition:v1')
_EXECUTOR_TERMINATION_EVIDENCE_NAMESPACE = uuid.UUID(
    '78ac727a-35da-50ff-b667-e5509dba7091')
EXECUTOR_TERMINATION_EVIDENCE_PROTOCOL_VERSION = 2
_ORDINARY_LAUNCH_SUBMISSION_NAMESPACE = uuid.UUID(
    '58a82cb0-534c-5a5d-bb5d-681759e60469')
_BOUND_CANCEL_QUIESCENCE_WAIT_SECONDS = 5.0
_BOUND_CANCEL_QUIESCENCE_POLL_SECONDS = 0.1
_LEGACY_ORDINARY_LAUNCH_HANDLER_NAME = 'sky.execution:launch'
_LEGACY_ORDINARY_LAUNCH_PAYLOAD_TYPE = (
    'sky.server.requests.payloads:LaunchBody')

# This is intentionally a closed map.  API009 exposes only the distinct bound
# ordinary-launch handler as provider-mutating queue work.  When another typed
# provider handler is persisted, adding its enum member and mapping here makes
# both the reserved selector and the generic exclusion advance together.
_PROVIDER_MUTATION_HANDLER_KINDS = {
    ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME:
        queue_base.ProviderMutationRequestKind.BOUND_ORDINARY_LAUNCH,
    non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME:
        queue_base.ProviderMutationRequestKind.NON_POOL_LAUNCH,
}
_PROVIDER_MUTATION_HANDLER_NAMES = tuple(
    _PROVIDER_MUTATION_HANDLER_KINDS.keys())
if frozenset(_PROVIDER_MUTATION_HANDLER_KINDS.values()) != frozenset(
        queue_base.ProviderMutationRequestKind):
    raise RuntimeError('PostgreSQL provider-mutation classification is not '
                       'exhaustive.')


@dataclasses.dataclass(frozen=True)
class BoundOrdinaryLaunchRequestFacts:
    """Locked request, delivery, claim, result, and quiescence evidence."""

    association_id: uuid.UUID
    request_id: str
    exists: bool
    status: requests_lib.RequestStatus | None
    terminal_cause: event_api_models.EventCause | None
    execution_generation: int | None
    claim_token: uuid.UUID | None
    worker_instance_id: uuid.UUID | None
    lease_expires_at: datetime.datetime | None
    claim_exists: bool
    claim_active: bool
    claim_expired: bool
    queue_exists: bool
    queue_delivery_state: str | None
    queue_claim_generation: int | None
    execution_quiescence_required: bool
    execution_quiesced_generation: int | None
    execution_quiesced_at: datetime.datetime | None
    quiescent: bool
    retention_pin_active: bool
    return_value: Any
    error: Any
    error_decode_failed: bool


def _canonical_evidence_sha256(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(dict(value),
                           sort_keys=True,
                           separators=(',', ':'),
                           ensure_ascii=False,
                           allow_nan=False).encode('utf-8')
    return hashlib.sha256(canonical).hexdigest()


def _evidence_value(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.astimezone(datetime.timezone.utc).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def read_legacy_launch_request_evidence(
    identity: ordinary_launch_binding_lib.LegacyLaunchIdentity,
    *,
    executor_terminated_at: datetime.datetime | None = None,
    executor_termination_evidence: Mapping[str, Any] | None = None,
) -> ordinary_launch_binding_lib.LegacyReconciliationEvidence:
    """Snapshot one historical unbound ``sky.launch`` request.

    This deliberately reports the stored facts as they are. In particular,
    generation zero, a false quiescence-required bit, and a missing
    ``finished_at`` never become quiescence or effect-absence evidence.
    """
    if not isinstance(identity, ordinary_launch_binding.LegacyLaunchIdentity):
        raise TypeError('identity must be LegacyLaunchIdentity.')
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Legacy request evidence requires central PostgreSQL state.')
    with engine.begin() as connection:
        request_row = connection.execute(
            sqlalchemy.select(REQUESTS).where(
                REQUESTS.c.request_id == identity.request_id).with_for_update()
        ).mappings().one_or_none()
        queue_row = connection.execute(
            sqlalchemy.select(QUEUE).where(
                QUEUE.c.request_id == identity.request_id).with_for_update()
        ).mappings().one_or_none()
        if (request_row is None or request_row['handler_name'] !=
                _LEGACY_ORDINARY_LAUNCH_HANDLER_NAME or
                request_row['cluster_name'] != identity.cluster_name or
                request_row['ordinary_launch_association_id'] is not None or
                request_row['binding_protocol_version'] is not None):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Legacy request evidence does not name the exact historical '
                'unbound launch.')
        worker_row = None
        worker_instance_id = request_row['worker_instance_id']
        if worker_instance_id is not None:
            worker_row = connection.execute(
                sqlalchemy.select(SERVER_INSTANCES).where(
                    SERVER_INSTANCES.c.instance_id == worker_instance_id).
                with_for_update()).mappings().one_or_none()
        observed_at = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()

    result_payload = {
        'error': request_row['error'],
        'return_value': request_row['return_value'],
    }
    result_digest = (None if all(
        value is None for value in result_payload.values()) else
                     _canonical_evidence_sha256(result_payload))
    request_payload = request_row['payload_json']
    request_evidence = {
        'cancel_acknowledged_at': _evidence_value(
            request_row['cancel_acknowledged_at']),
        'cancel_requested_at': _evidence_value(
            request_row['cancel_requested_at']),
        'claim_token': _evidence_value(request_row['claim_token']),
        'cluster_name': request_row['cluster_name'],
        'execution_process_start_time_ticks':
            request_row['execution_process_start_time_ticks'],
        'execution_quiesced_at': _evidence_value(
            request_row['execution_quiesced_at']),
        'execution_quiesced_generation':
            request_row['execution_quiesced_generation'],
        'execution_quiescence_required':
            request_row['execution_quiescence_required'],
        'finished_at': _evidence_value(request_row['finished_at']),
        'handler_name': request_row['handler_name'],
        'lease_expires_at': _evidence_value(request_row['lease_expires_at']),
        'payload_sha256': _canonical_evidence_sha256({
            'payload': request_payload,
            'payload_format': request_row['payload_format'],
            'payload_type': request_row['payload_type'],
            'payload_version': request_row['payload_version'],
        }),
        'pid': request_row['pid'],
        'queue_claim_generation':
            (None if queue_row is None else queue_row['claim_generation']),
        'queue_delivery_state':
            (None if queue_row is None else queue_row['delivery_state']),
        'request_id': request_row['request_id'],
        'terminal_cause': request_row['terminal_cause'],
        'worker_instance': (None if worker_row is None else {
            'draining_at': _evidence_value(worker_row['draining_at']),
            'heartbeat_at': _evidence_value(worker_row['heartbeat_at']),
            'instance_id': _evidence_value(worker_row['instance_id']),
            'pod_name': worker_row['pod_name'],
            'pod_uid': worker_row['pod_uid'],
            'ready': worker_row['ready'],
            'role': worker_row['role'],
            'version': worker_row['version'],
        }),
        'worker_instance_id': _evidence_value(worker_instance_id),
    }
    claim_present = any(request_row[field] is not None
                        for field in ('claim_token', 'worker_instance_id',
                                      'lease_expires_at', 'pid',
                                      'execution_process_start_time_ticks'))
    return ordinary_launch_binding.LegacyReconciliationEvidence(
        observed_request_status=str(request_row['status']),
        observed_request_execution_generation=int(
            request_row['execution_generation']),
        observed_request_queue_present=queue_row is not None,
        observed_request_claim_present=claim_present,
        observed_request_result_digest=result_digest,
        observed_request_at=observed_at,
        observed_request_evidence=request_evidence,
        executor_terminated_at=executor_terminated_at,
        executor_termination_evidence=executor_termination_evidence,
        provider_evidence=ordinary_launch_binding.ProviderEvidence.NOT_QUERIED,
        provider_evidence_observed_at=None,
        provider_evidence_payload=None)


class OrdinaryLaunchReductionDisposition(str, enum.Enum):
    ADOPT_ACTIVE = 'ADOPT_ACTIVE'
    WAIT_QUIESCENCE = 'WAIT_QUIESCENCE'
    PROJECTED = 'PROJECTED'
    PRE_EFFECT_TERMINAL = 'PRE_EFFECT_TERMINAL'
    AMBIGUOUS = 'AMBIGUOUS'


@dataclasses.dataclass(frozen=True)
class ServerPodIdentity:
    """Pod identity captured once before request environment overrides."""

    name: str
    namespace: str
    uid: str
    ip: str | None

    @classmethod
    def from_environment(cls) -> ServerPodIdentity:
        """Capture the role supervisor's deployment-provided identity once."""
        return cls(name=(os.environ.get('SKYPILOT_POD_NAME') or
                         os.environ.get('HOSTNAME') or '').strip(),
                   namespace=os.environ.get('SKYPILOT_POD_NAMESPACE',
                                            '').strip(),
                   uid=os.environ.get('SKYPILOT_POD_UID', '').strip(),
                   ip=os.environ.get('POD_IP'))


@dataclasses.dataclass(frozen=True)
class ExecutorTerminationObservation:
    """One exact final successful Pod deletion observed from Kubernetes."""

    kubernetes_cluster_uid: str
    pod_namespace: str
    pod_name: str
    pod_uid: str
    container_name: str
    pod_resource_version: str
    pod_event_type: str
    pod_phase: str
    pod_deletion_timestamp: datetime.datetime
    container_finished_at: datetime.datetime
    container_exit_code: int
    container_reason: str | None


@dataclasses.dataclass(frozen=True)
class BoundOrdinaryLaunchProjectionInput:
    """Typed terminal result supplied to an atomic ReplicaInfo projector."""

    context: ordinary_launch_binding_lib.BoundLaunchContext
    request: BoundOrdinaryLaunchRequestFacts
    locked_replica_info: replica_managers.ReplicaInfo
    status: requests_lib.RequestStatus
    cause: event_api_models.EventCause
    service_job_id: int | None
    pre_effect_terminal: bool
    cancel_reason: str | None
    paid_capacity_pool_key: str | None
    provider_evidence: ordinary_launch_binding_lib.ProviderEvidence | None = (
        None)
    provider_evidence_payload: Mapping[str, Any] | None = None


class BoundOrdinaryLaunchReplicaProjector(typing.Protocol):
    """Persist one exact terminal ordinary-launch projection."""

    # pylint: disable=unnecessary-ellipsis

    def __call__(
        self,
        connection: sqlalchemy.engine.Connection,
        projection: BoundOrdinaryLaunchProjectionInput,
    ) -> bool:
        """Persist the exact ReplicaInfo and any typed paid-pool outcome."""
        ...


@dataclasses.dataclass(frozen=True)
class OrdinaryLaunchReduction:
    """Controller-visible outcome of one exact association inspection."""

    context: ordinary_launch_binding_lib.BoundLaunchContext
    disposition: OrdinaryLaunchReductionDisposition
    request: BoundOrdinaryLaunchRequestFacts
    service_job_id: int | None
    cancel_reason: str | None
    projected: bool


@dataclasses.dataclass(frozen=True)
class BoundOrdinaryLaunchCancelTarget:
    """Non-authorizing pointer snapshot used only to address exact cancel."""

    context: ordinary_launch_binding_lib.BoundLaunchContext
    cancel_reason: str | None


def _is_owned_executor_process(pid: int) -> bool:
    """Whether PID is an executor child owned by this server process tree."""

    def _has_title(process: psutil.Process, prefix: str) -> bool:
        command = process.cmdline()
        return bool(command) and command[0].startswith(prefix)

    try:
        target = psutil.Process(pid)
        if not _has_title(target, 'SkyPilot:executor:'):
            return False
        # RequestWorker dispatchers are threads in production, so both the
        # short and long ProcessPool children have the executor server process
        # itself as their OS parent.  Requiring a synthetic worker *process*
        # title rejects the real topology and makes cancellation hang.  A
        # direct-child check is also stronger than accepting any descendant:
        # only this server instance can own and signal the claimed PID.
        caller = psutil.Process()
        return target.ppid() == caller.pid
    except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError,
            PermissionError):
        return False


def _signal_exact_executor_process(pid: int, expected_start_time_ticks: int,
                                   signum: int) -> bool:
    """Signal only the kernel process bound to PID and procfs birth identity."""
    pidfd_open = getattr(os, 'pidfd_open', None)
    pidfd_send_signal = getattr(signal, 'pidfd_send_signal', None)
    if not callable(pidfd_open) or not callable(pidfd_send_signal):
        logger.warning('Exact executor signalling requires Linux pidfds; '
                       'cancellation is failing closed.')
        return False
    try:
        pidfd = pidfd_open(pid, 0)  # pylint: disable=not-callable
    except (OSError, ProcessLookupError) as e:
        logger.info(f'Exact executor PID {pid} is unavailable: {e}')
        return False
    try:
        observed_start_time: int
        try:
            observed_start_time = (
                request_storage.read_linux_process_start_time_ticks(pid))
        except (OSError, ValueError) as e:
            logger.warning(f'Could not attest executor PID {pid}: {e}')
            return False
        if observed_start_time != expected_start_time_ticks:
            logger.warning('Refusing to signal reused executor PID '
                           f'{pid}: expected start ticks '
                           f'{expected_start_time_ticks}, observed '
                           f'{observed_start_time}.')
            return False
        if not _is_owned_executor_process(pid):
            logger.warning(f'Refusing to signal PID {pid}: it is not this '
                           'dispatcher\'s executor child.')
            return False
        try:
            pidfd_send_signal(pidfd, signum, None, 0)  # pylint: disable=not-callable
        except (OSError, ProcessLookupError) as e:
            logger.info(f'Exact executor PID {pid} exited before signal: {e}')
            return False
        return True
    finally:
        os.close(pidfd)


def role_is_draining() -> bool:
    """Return whether Kubernetes has started the pod drain interval."""
    return os.path.exists(ROLE_DRAIN_MARKER_PATH)


_METADATA = postgres_schema.metadata
REQUESTS = postgres_schema.REQUESTS
REQUEST_RETENTION_PINS = postgres_schema.REQUEST_RETENTION_PINS
RESOURCE_ACTIONS = postgres_schema.RESOURCE_ACTIONS
RESOURCE_ACTION_ATTEMPTS = postgres_schema.RESOURCE_ACTION_ATTEMPTS
QUEUE = postgres_schema.QUEUE
STORE_METADATA = postgres_schema.STORE_METADATA
SERVER_INSTANCES = postgres_schema.SERVER_INSTANCES
EXECUTOR_TERMINATION_EVIDENCE = (postgres_schema.EXECUTOR_TERMINATION_EVIDENCE)
CONTROLLER_LEADERSHIP = postgres_schema.CONTROLLER_LEADERSHIP
CONTROLLER_ACTION_RESERVATIONS = (
    postgres_schema.CONTROLLER_ACTION_RESERVATIONS)
_PG_LOCKS = postgres_schema.PG_LOCKS

_DATETIME_FIELDS = frozenset({
    'created_at',
    'finished_at',
    'lease_expires_at',
    'heartbeat_at',
    'cancel_requested_at',
    'cancel_acknowledged_at',
    'execution_quiesced_at',
})
_MANAGED_JOB_ORIGIN_FIELDS = (
    'managed_job_id',
    'managed_job_controller_instance_id',
    'managed_job_controller_generation',
    'managed_job_controller_slot_id',
    'managed_job_controller_slot_attempt',
)
_MANAGED_JOB_ORIGIN_UUID_FIELDS = (
    'managed_job_controller_instance_id',
    'managed_job_controller_slot_attempt',
)
_MANAGED_JOB_ACTIVE_SCHEDULE_STATES = (
    'LAUNCHING',
    'ALIVE',
    'ALIVE_WAITING',
    'ALIVE_BACKOFF',
)
_MANAGED_JOB_QUIESCE_REASON = (
    'The owning managed-job controller slot stopped; waiting for exact '
    'nested-request execution quiescence.')

ORDINARY_LAUNCH_RETENTION_PIN_KIND = 'serve-ordinary-launch.v1'


def ensure_server_instance_id() -> str:
    """Return one stable UUID inherited by every process in a server pod."""
    value = os.environ.get(SERVER_INSTANCE_ID_ENV_VAR)
    if value is None:
        value = str(uuid.uuid4())
        os.environ[SERVER_INSTANCE_ID_ENV_VAR] = value
    try:
        return str(uuid.UUID(value))
    except ValueError as e:
        raise ValueError(
            f'{SERVER_INSTANCE_ID_ENV_VAR} must be a UUID, got {value!r}.'
        ) from e


def _validate_server_role(role: str) -> str:
    if role not in _VALID_SERVER_ROLES:
        raise ValueError(f'Invalid API server role {role!r}; expected one of '
                         f'{sorted(_VALID_SERVER_ROLES)}.')
    return role


def _supported_handlers(role: str) -> list[str]:
    registrations = request_registry.registered_handlers()
    if role == 'api':
        return []
    if role == 'controller':
        return sorted(registration.name
                      for registration in registrations
                      if registration.execution_class is
                      request_registry.ExecutionClass.CONTROLLER)
    if role == 'executor':
        return sorted(registration.name
                      for registration in registrations
                      if registration.execution_class is
                      request_registry.ExecutionClass.NORMAL)
    # The compatibility all-role process remains the only consumer for both
    # execution classes.
    return sorted(registration.name for registration in registrations)


def _supported_payload_versions() -> dict[str, dict[str, int]]:
    return {
        requests_lib.DURABLE_PAYLOAD_FORMAT: {
            'minimum': requests_lib.DURABLE_PAYLOAD_VERSION,
            'maximum': requests_lib.DURABLE_PAYLOAD_VERSION,
        }
    }


def _qualified_type_name(value: Any) -> str:
    value_type = type(value)
    return f'{value_type.__module__}.{value_type.__qualname__}'


def _resolved_request_backend_capability() -> tuple[str, str, bool]:
    """Return the actual storage/queue types and strong-cancellation support."""
    storage_backend = request_storage.get_request_backend()
    queue_factory = queue_base.get_queue_backend_factory()
    storage_type = _qualified_type_name(storage_backend)
    queue_type = _qualified_type_name(queue_factory)
    capable = (type(storage_backend) is PostgresRequestBackend and
               type(queue_factory) is PostgresQueueFactory)
    return storage_type, queue_type, capable


def _ordinary_launch_binding_process_capable(role: str,
                                             backend_capable: bool) -> bool:
    """Whether this exact process implements its API009 protocol duties."""
    if not backend_capable:
        return False
    registered = {
        registration.name
        for registration in request_registry.registered_handlers()
    }
    if ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME not in (
            registered):
        return False
    # API processes own admission and retention GC but intentionally claim no
    # handlers. Executors/all-role processes must advertise the distinct local
    # handler before they can participate in a bound-launch fleet.
    if role in ('all', 'executor'):
        return (ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME
                in _supported_handlers(role))
    return True


def _non_pool_launch_binding_process_capable(role: str,
                                             backend_capable: bool) -> bool:
    """Whether this process implements every API011 profile duty."""
    if not backend_capable:
        return False
    registered = {
        registration.name
        for registration in request_registry.registered_handlers()
    }
    if non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME not in registered:
        return False
    if role in ('all', 'executor'):
        return (non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME
                in _supported_handlers(role))
    return True


def _ordered_capacity_admission_process_capable(role: str,
                                                backend_capable: bool) -> bool:
    """Whether this process can enforce the API012 paid-authority tuple."""
    return (_non_pool_launch_binding_process_capable(role, backend_capable) and
            role in ('all', 'api', 'executor', 'controller'))


def _executor_termination_evidence_process_capable(
    role: str,
    backend_capable: bool,
    pod_identity: ServerPodIdentity,
    instance_id: str,
) -> bool:
    """Whether this executor identity can be certified by Pod UID."""
    if not backend_capable or role not in ('all', 'executor', 'controller'):
        return False
    if not all((pod_identity.name, pod_identity.namespace, pod_identity.uid)):
        return False
    try:
        return str(uuid.UUID(pod_identity.uid)) == instance_id
    except (AttributeError, TypeError, ValueError):
        return False


def execution_quiescence_backend_guard_enabled() -> bool:
    return os.environ.get(EXECUTION_QUIESCENCE_BACKEND_GUARD_ENV_VAR) == 'true'


def require_builtin_execution_quiescence_backends(*,
                                                  required: bool = False
                                                 ) -> None:
    """Require exact durable backends in every PostgreSQL process context."""
    if not required and not execution_quiescence_backend_guard_enabled():
        return
    if os.environ.get(REQUEST_BACKEND_ENV_VAR) != POSTGRES_REQUEST_BACKEND:
        raise RuntimeError(
            'Execution-quiescence backend enforcement requires '
            f'{REQUEST_BACKEND_ENV_VAR}={POSTGRES_REQUEST_BACKEND}.')
    storage_type, queue_type, capable = _resolved_request_backend_capability()
    if not capable:
        raise RuntimeError(
            'PostgreSQL API request execution requires the built-in request '
            'storage and queue backends for exact-generation quiescence; '
            f'resolved storage={storage_type!r}, queue={queue_type!r}.')


class ServerInstanceLease:
    """PostgreSQL-backed liveness and readiness for one role supervisor."""

    def __init__(
        self,
        role: str,
        *,
        heartbeat_interval_seconds: float = (
            _INSTANCE_HEARTBEAT_INTERVAL_SECONDS),
        stale_after_seconds: float = INSTANCE_STALE_AFTER_SECONDS,
        pod_identity: ServerPodIdentity | None = None,
    ) -> None:
        self.role = _validate_server_role(role)
        self.instance_id = ensure_server_instance_id()
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._stale_after_seconds = stale_after_seconds
        self._ready = False
        self._draining = False
        self._health_detail: dict[str, Any] = {'phase': 'initializing'}
        self._last_success_monotonic: float | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._heartbeat_lock = threading.Lock()
        self._pod_identity = (pod_identity or
                              ServerPodIdentity.from_environment())

    @property
    def pod_identity(self) -> ServerPodIdentity:
        """Return the immutable identity used by every lease heartbeat."""
        return self._pod_identity

    def _values(self, *, include_started_at: bool) -> dict[str, Any]:
        now = sqlalchemy.func.clock_timestamp()
        storage_type, queue_type, quiescence_capable = (
            _resolved_request_backend_capability())
        binding_capable = _ordinary_launch_binding_process_capable(
            self.role, quiescence_capable)
        non_pool_capable = _non_pool_launch_binding_process_capable(
            self.role, quiescence_capable)
        ordered_capacity_capable = (_ordered_capacity_admission_process_capable(
            self.role, quiescence_capable))
        termination_evidence_capable = (
            _executor_termination_evidence_process_capable(
                self.role, quiescence_capable, self._pod_identity,
                self.instance_id))
        with self._state_lock:
            ready = self._ready
            draining = self._draining
            health_detail = dict(self._health_detail)
        if role_is_draining():
            draining = True
            ready = False
            health_detail = {'phase': 'draining'}
        values: dict[str, Any] = {
            'instance_id': uuid.UUID(self.instance_id),
            'role': self.role,
            'pod_name': self._pod_identity.name or None,
            'pod_uid': self._pod_identity.uid or None,
            'pod_namespace': self._pod_identity.namespace or None,
            'pod_ip': self._pod_identity.ip,
            'version': sky.__version__,
            'heartbeat_at': now,
            'draining_at': now if draining else None,
            'ready': ready and not draining,
            'health_detail': health_detail,
            'supported_handlers': _supported_handlers(self.role),
            'supported_payload_versions': _supported_payload_versions(),
            'request_storage_backend': storage_type,
            'request_queue_backend': queue_type,
            'execution_quiescence_capable': quiescence_capable,
            # Legacy/plugin processes and binaries without the distinct local
            # handler retain the API009 false default so mixed fleets fail
            # admission closed.
            'ordinary_launch_binding_capable': binding_capable,
            'non_pool_launch_binding_capable': non_pool_capable,
            'non_pool_launch_binding_protocol_version':
                (ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION
                 if non_pool_capable else None),
            'non_pool_launch_capability_profile_set_digest': (
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
                if non_pool_capable else None),
            'non_pool_launch_capability_cohort_epoch':
                (ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH
                 if non_pool_capable else None),
            'non_pool_launch_receipt_protocol_version':
                (ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION
                 if non_pool_capable else None),
            'ordered_capacity_admission_capable': ordered_capacity_capable,
            'ordered_capacity_admission_protocol_version':
                (ORDERED_CAPACITY_ADMISSION_PROTOCOL_VERSION
                 if ordered_capacity_capable else None),
            'ordered_capacity_admission_cohort_epoch':
                (ORDERED_CAPACITY_ADMISSION_COHORT_EPOCH
                 if ordered_capacity_capable else None),
            'executor_termination_evidence_capable': termination_evidence_capable,
            'executor_termination_evidence_protocol_version':
                (EXECUTOR_TERMINATION_EVIDENCE_PROTOCOL_VERSION
                 if termination_evidence_capable else None),
        }
        if include_started_at:
            values['started_at'] = now
        return values

    def _record_heartbeat_success(self) -> None:
        with self._state_lock:
            self._last_success_monotonic = time.monotonic()

    def _register_unlocked(self) -> None:
        engine = initialize_and_get_db()
        values = self._values(include_started_at=True)
        update_values = dict(values)
        update_values.pop('instance_id')
        with engine.begin() as connection:
            connection.execute(
                postgresql.insert(SERVER_INSTANCES).values(
                    **values).on_conflict_do_update(
                        index_elements=[SERVER_INSTANCES.c.instance_id],
                        set_=update_values))
        self._record_heartbeat_success()

    def _register(self) -> None:
        with self._heartbeat_lock:
            self._register_unlocked()

    def _heartbeat(self, *, lock_timeout: float | None = None) -> bool:
        if lock_timeout is None:
            acquired = self._heartbeat_lock.acquire()
        else:
            acquired = self._heartbeat_lock.acquire(timeout=lock_timeout)
        if not acquired:
            return False
        try:
            engine = initialize_and_get_db()
            values = self._values(include_started_at=False)
            values.pop('instance_id')
            # ``draining_at`` is the start of the exact instance's retirement,
            # not its latest heartbeat.  Keep that witness immutable while the
            # draining owner continues heartbeating through child/receipt
            # convergence.
            if values['draining_at'] is not None:
                values['draining_at'] = sqlalchemy.func.coalesce(
                    SERVER_INSTANCES.c.draining_at, values['draining_at'])
            with engine.begin() as connection:
                result = connection.execute(
                    sqlalchemy.update(SERVER_INSTANCES).where(
                        SERVER_INSTANCES.c.instance_id == uuid.UUID(
                            self.instance_id)).values(**values))
            if result.rowcount != 1:
                self._register_unlocked()
                return True
            self._record_heartbeat_success()
            return True
        finally:
            self._heartbeat_lock.release()

    def start(self) -> None:
        """Register the instance and begin heartbeating."""
        self._register()
        if self._thread is not None:
            return

        def heartbeat_loop() -> None:
            while not self._stop_event.wait(self._heartbeat_interval_seconds):
                try:
                    self._heartbeat()
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(f'Failed to heartbeat {self.role} instance '
                                   f'{self.instance_id}: {e}')

        self._thread = threading.Thread(
            target=heartbeat_loop,
            name=f'skypilot-{self.role}-instance-heartbeat',
            daemon=True)
        self._thread.start()

    def set_ready(self,
                  ready: bool,
                  *,
                  health_detail: dict[str, Any] | None = None) -> None:
        """Publish an immediate readiness transition."""
        with self._state_lock:
            self._ready = ready
            if health_detail is not None:
                self._health_detail = health_detail
        self._heartbeat()

    def is_locally_ready(self) -> bool:
        """Return readiness using the latest successful database heartbeat."""
        if role_is_draining():
            return False
        with self._state_lock:
            last_success = self._last_success_monotonic
            return (
                self._ready and not self._draining and
                last_success is not None and
                time.monotonic() - last_success <= self._stale_after_seconds)

    def stop(self) -> None:
        """Mark the instance draining before stopping its heartbeat."""
        self._set_draining_locally()
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1, self._heartbeat_interval_seconds *
                                          2))
        self._publish_draining()

    def _set_draining_locally(self) -> None:
        with self._state_lock:
            self._draining = True
            self._ready = False
            self._health_detail = {'phase': 'draining'}

    def _publish_draining(self) -> None:
        try:
            if not self._heartbeat(lock_timeout=1):
                logger.warning(f'Timed out marking {self.role} instance '
                               f'{self.instance_id} draining.')
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to mark {self.role} instance '
                           f'{self.instance_id} draining: {e}')

    def begin_draining(self) -> None:
        """Publish early retirement while retaining the instance heartbeat."""
        self._set_draining_locally()
        self._publish_draining()


def current_instance_is_ready() -> bool:
    """Check the current supervisor's durable readiness using the DB clock."""
    if role_is_draining():
        return False
    instance_id = uuid.UUID(ensure_server_instance_id())
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        return bool(
            connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(SERVER_INSTANCES).where(
                    SERVER_INSTANCES.c.instance_id == instance_id,
                    SERVER_INSTANCES.c.ready,
                    SERVER_INSTANCES.c.draining_at.is_(None),
                    SERVER_INSTANCES.c.heartbeat_at >=
                    sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                        seconds=INSTANCE_STALE_AFTER_SECONDS))).scalar_one())


def ordinary_launch_binding_fleet_capable(
    *,
    connection: sqlalchemy.engine.Connection | None = None,
    quiescence_seconds: float = (
        ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS),
) -> bool:
    """Whether every live launch participant advertises API009 binding.

    Every recent ``all|api|executor|controller`` process must advertise it,
    including non-ready and draining processes whose workers or retention GC
    may not yet have quiesced. Separately, at least one ready, non-draining API
    acceptor and ordinary executor must exist. Rollout waits a full stale
    rollout-quiescence window before excluding an old process.
    """
    if (isinstance(quiescence_seconds, bool) or
            not isinstance(quiescence_seconds, (int, float)) or
            not math.isfinite(quiescence_seconds) or quiescence_seconds < 0):
        raise ValueError('quiescence_seconds must be finite and non-negative.')
    engine = initialize_and_get_db()

    def _read(active_connection: sqlalchemy.engine.Connection) -> bool:
        rows = active_connection.execute(
            sqlalchemy.select(
                SERVER_INSTANCES.c.instance_id, SERVER_INSTANCES.c.role,
                SERVER_INSTANCES.c.ready, SERVER_INSTANCES.c.draining_at,
                SERVER_INSTANCES.c.supported_handlers,
                SERVER_INSTANCES.c.ordinary_launch_binding_capable).where(
                    SERVER_INSTANCES.c.role.in_(
                        ('all', 'api', 'executor', 'controller')),
                    SERVER_INSTANCES.c.heartbeat_at >=
                    sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                        seconds=quiescence_seconds))).mappings().all()
        acceptors = [
            row for row in rows
            if row['ready'] and row['draining_at'] is None and row['role'] in (
                'all', 'api')
        ]
        executors = [
            row for row in rows
            if row['ready'] and row['draining_at'] is None and row['role'] in (
                'all', 'executor')
        ]
        for row in rows:
            if not bool(row['ordinary_launch_binding_capable']):
                return False
            if row['role'] in ('all', 'executor'):
                handlers = row['supported_handlers']
                if (not isinstance(handlers, list) or ordinary_launch_request.
                        BOUND_ORDINARY_LAUNCH_HANDLER_NAME not in handlers):
                    return False
        return bool(acceptors and executors)

    if connection is not None:
        return _read(connection)
    with engine.connect() as owned_connection:
        return _read(owned_connection)


def non_pool_launch_binding_fleet_capable(
    *,
    connection: sqlalchemy.engine.Connection | None = None,
    quiescence_seconds: float = (
        ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS),
) -> bool:
    """Require one exact API011 capability cohort across live participants."""
    if (isinstance(quiescence_seconds, bool) or
            not isinstance(quiescence_seconds, (int, float)) or
            not math.isfinite(quiescence_seconds) or quiescence_seconds < 0):
        raise ValueError('quiescence_seconds must be finite and non-negative.')
    engine = initialize_and_get_db()
    expected_digest = (
        ordinary_launch_binding.supported_non_pool_profile_set_digest())

    def _read(active_connection: sqlalchemy.engine.Connection) -> bool:
        rows = active_connection.execute(
            sqlalchemy.select(
                SERVER_INSTANCES.c.role, SERVER_INSTANCES.c.ready,
                SERVER_INSTANCES.c.draining_at,
                SERVER_INSTANCES.c.supported_handlers,
                SERVER_INSTANCES.c.non_pool_launch_binding_capable,
                SERVER_INSTANCES.c.non_pool_launch_binding_protocol_version,
                SERVER_INSTANCES.c.
                non_pool_launch_capability_profile_set_digest,
                SERVER_INSTANCES.c.non_pool_launch_capability_cohort_epoch,
                SERVER_INSTANCES.c.non_pool_launch_receipt_protocol_version).
            where(
                SERVER_INSTANCES.c.role.in_(
                    ('all', 'api', 'executor', 'controller')),
                SERVER_INSTANCES.c.heartbeat_at >=
                sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                    seconds=quiescence_seconds))).mappings().all()
        acceptor = False
        executor = False
        for row in rows:
            if (row['non_pool_launch_binding_capable'] is not True or
                    row['non_pool_launch_binding_protocol_version'] !=
                    ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION or
                    row['non_pool_launch_capability_profile_set_digest'] !=
                    expected_digest or
                    row['non_pool_launch_capability_cohort_epoch'] !=
                    ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH or
                    row['non_pool_launch_receipt_protocol_version'] !=
                    ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION):
                return False
            ready = row['ready'] and row['draining_at'] is None
            if ready and row['role'] in ('all', 'api'):
                acceptor = True
            if ready and row['role'] in ('all', 'executor'):
                handlers = row['supported_handlers']
                if (not isinstance(handlers, list) or
                        non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME
                        not in handlers):
                    return False
                executor = True
        return bool(acceptor and executor)

    if connection is not None:
        return _read(connection)
    with engine.connect() as owned_connection:
        return _read(owned_connection)


def ordered_capacity_admission_fleet_capable(
    *,
    connection: sqlalchemy.engine.Connection | None = None,
    quiescence_seconds: float = (
        ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS),
) -> bool:
    """Require one exact API015 cohort across every live participant."""
    if (isinstance(quiescence_seconds, bool) or
            not isinstance(quiescence_seconds, (int, float)) or
            not math.isfinite(quiescence_seconds) or quiescence_seconds < 0):
        raise ValueError('quiescence_seconds must be finite and non-negative.')
    engine = initialize_and_get_db()

    def _read(active_connection: sqlalchemy.engine.Connection) -> bool:
        rows = active_connection.execute(
            sqlalchemy.select(
                SERVER_INSTANCES.c.role, SERVER_INSTANCES.c.ready,
                SERVER_INSTANCES.c.draining_at,
                SERVER_INSTANCES.c.ordered_capacity_admission_capable,
                SERVER_INSTANCES.c.ordered_capacity_admission_protocol_version,
                SERVER_INSTANCES.c.ordered_capacity_admission_cohort_epoch).
            where(
                SERVER_INSTANCES.c.role.in_(
                    ('all', 'api', 'executor', 'controller')),
                SERVER_INSTANCES.c.heartbeat_at >=
                sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                    seconds=quiescence_seconds))).mappings().all()
        acceptor = False
        executor = False
        for row in rows:
            if (row['ordered_capacity_admission_capable'] is not True or
                    row['ordered_capacity_admission_protocol_version'] !=
                    ORDERED_CAPACITY_ADMISSION_PROTOCOL_VERSION or
                    row['ordered_capacity_admission_cohort_epoch'] !=
                    ORDERED_CAPACITY_ADMISSION_COHORT_EPOCH):
                return False
            ready = row['ready'] and row['draining_at'] is None
            acceptor = acceptor or bool(ready and row['role'] in ('all', 'api'))
            executor = executor or bool(ready and
                                        row['role'] in ('all', 'executor'))
        return acceptor and executor

    if connection is not None:
        return _read(connection)
    with engine.connect() as owned_connection:
        return _read(owned_connection)


def recent_legacy_controller_consumers(quiescence_seconds: float) -> list[str]:
    """Return recent all/executor instances that can claim controller work.

    M2 executors advertised controller handlers and marked themselves draining
    before their worker pools had fully exited. M3 leaders therefore wait for a
    full termination-grace window after the last such heartbeat, not merely for
    Ready=false, before starting the specialized controller pool.
    """
    if quiescence_seconds < 0:
        raise ValueError('Controller cutover quiescence must be non-negative.')
    controller_handlers = frozenset(_supported_handlers('controller'))
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                SERVER_INSTANCES.c.instance_id,
                SERVER_INSTANCES.c.supported_handlers).where(
                    SERVER_INSTANCES.c.role.in_(['all', 'executor']),
                    SERVER_INSTANCES.c.heartbeat_at >=
                    sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                        seconds=quiescence_seconds))).mappings().all()
    blockers = []
    for row in rows:
        advertised = row['supported_handlers']
        if not isinstance(advertised, list):
            blockers.append(str(row['instance_id']))
            continue
        if controller_handlers.intersection(advertised):
            blockers.append(str(row['instance_id']))
    return blockers


def recent_legacy_daemon_handler_instances(
        quiescence_seconds: float = INSTANCE_STALE_AFTER_SECONDS) -> list[str]:
    """Return recent processes that advertise a retired daemon handler."""
    if (isinstance(quiescence_seconds, bool) or
            not isinstance(quiescence_seconds, (int, float)) or
            not math.isfinite(quiescence_seconds) or quiescence_seconds < 0):
        raise ValueError('Daemon transition quiescence must be finite and '
                         'non-negative.')
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                SERVER_INSTANCES.c.instance_id,
                SERVER_INSTANCES.c.supported_handlers).where(
                    SERVER_INSTANCES.c.heartbeat_at >=
                    sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                        seconds=quiescence_seconds))).mappings().all()
    blockers = []
    for row in rows:
        advertised = row['supported_handlers']
        if (not isinstance(advertised, list) or any(
                isinstance(handler, str) and handler.startswith('daemon:')
                for handler in advertised)):
            blockers.append(str(row['instance_id']))
    return blockers


@contextlib.contextmanager
def legacy_daemon_transition(*,
                             poll_seconds: float = 1
                            ) -> Generator[None, None, None]:
    """Hold the one-way legacy-daemon cutover around startup mutations.

    The caller performs generation-fenced row retirement, stale-claim fencing,
    and request recovery while this coordination session is held. The lock is
    not proof that arbitrary effects in an old process stopped, so the server
    instance stale window remains the admission gate before yielding.
    """
    if poll_seconds <= 0:
        raise ValueError('Daemon transition poll interval must be positive.')
    transition_lock = locks.PostgresLock(_LEGACY_DAEMON_TRANSITION_LOCK_ID)
    transition_lock.acquire()
    try:
        while True:
            if not transition_lock.is_session_alive():
                raise RuntimeError('Legacy daemon transition lock was lost.')
            blockers = recent_legacy_daemon_handler_instances()
            if not blockers:
                break
            logger.info('Waiting for legacy internal-daemon executors to '
                        f'become stale: {len(blockers)} instance(s).')
            time.sleep(poll_seconds)
        yield
        if not transition_lock.is_session_alive():
            raise RuntimeError('Legacy daemon transition lock was lost during '
                               'startup recovery.')
    finally:
        transition_lock.release()


def _utc_datetime(timestamp: float | None) -> datetime.datetime | None:
    if timestamp is None:
        return None
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def _timestamp(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.timestamp()
    return value


def _request_values_for_db(request: requests_lib.Request) -> dict[str, Any]:
    values = request.durable_values()
    for field in _DATETIME_FIELDS:
        values[field] = _utc_datetime(values.get(field))
    present_origin_fields = tuple(field for field in _MANAGED_JOB_ORIGIN_FIELDS
                                  if values.get(field) is not None)
    if present_origin_fields and len(present_origin_fields) != len(
            _MANAGED_JOB_ORIGIN_FIELDS):
        missing_fields = sorted(
            set(_MANAGED_JOB_ORIGIN_FIELDS) - set(present_origin_fields))
        raise ValueError('Managed-job request origin must be entirely absent '
                         'or contain all five fields; missing '
                         f'{missing_fields}.')
    if present_origin_fields:
        for field, minimum in (('managed_job_id',
                                1), ('managed_job_controller_generation', 1),
                               ('managed_job_controller_slot_id', 0)):
            value = values[field]
            if (isinstance(value, bool) or not isinstance(value, int) or
                    value < minimum):
                raise ValueError(f'{field} must be an integer greater than or '
                                 f'equal to {minimum}, got {value!r}.')
    for field in ('claim_token', 'worker_instance_id',
                  *_MANAGED_JOB_ORIGIN_UUID_FIELDS):
        if values.get(field) is not None:
            try:
                values[field] = uuid.UUID(str(values[field]))
            except (AttributeError, TypeError, ValueError) as e:
                raise ValueError(f'{field} must be a UUID, got '
                                 f'{values[field]!r}.') from e
    values['updated_at'] = sqlalchemy.func.clock_timestamp()
    return values


def _managed_job_origin(
    values: Mapping[str, Any],
) -> tuple[int, uuid.UUID, int, int, uuid.UUID] | None:
    """Decode one complete immutable managed-job request origin."""
    raw = tuple(values.get(field) for field in _MANAGED_JOB_ORIGIN_FIELDS)
    if all(value is None for value in raw):
        return None
    if any(value is None for value in raw):
        raise ValueError('Managed-job request origin is incomplete.')
    try:
        job_id = int(typing.cast(Any, raw[0]))
        instance_id = uuid.UUID(str(raw[1]))
        generation = int(typing.cast(Any, raw[2]))
        slot_id = int(typing.cast(Any, raw[3]))
        attempt = uuid.UUID(str(raw[4]))
    except (TypeError, ValueError) as e:
        raise ValueError('Managed-job request origin is malformed.') from e
    if job_id <= 0 or generation <= 0 or slot_id < 0:
        raise ValueError('Managed-job request origin has invalid values.')
    return job_id, instance_id, generation, slot_id, attempt


def _lock_live_controller_leadership(
    connection: sqlalchemy.engine.Connection,) -> Mapping[str, Any] | None:
    """Lock the singleton live outer-controller row, independent of owner."""
    return connection.execute(
        sqlalchemy.select(CONTROLLER_LEADERSHIP).where(
            CONTROLLER_LEADERSHIP.c.leadership_key ==
            _CONTROLLER_LEADERSHIP_KEY,
            CONTROLLER_LEADERSHIP.c.released_at.is_(None),
            _controller_session_locks_are_live()).with_for_update(
                read=True)).mappings().one_or_none()


def _lock_managed_job_origin(
    connection: sqlalchemy.engine.Connection,
    values: Mapping[str, Any],
    *,
    require_admission: bool,
) -> bool:
    """Lock and validate outer owner then exact managed-job attempt.

    This is the canonical prefix for nested-request creation, queue claim, and
    RUNNING admission.  Callers acquire request and queue locks only after it.
    """
    origin = _managed_job_origin(values)
    if origin is None:
        return True
    job_id, instance_id, generation, slot_id, attempt = origin
    leadership = _lock_live_controller_leadership(connection)
    if (leadership is None or leadership['instance_id'] != instance_id or
            int(leadership['generation']) != generation):
        return False
    job_info = managed_job_state_schema.job_info_table
    conditions: tuple[Any, ...] = (
        job_info.c.spot_job_id == job_id,
        job_info.c.controller_instance_id == str(instance_id),
        job_info.c.controller_generation == generation,
        job_info.c.controller_slot_id == slot_id,
        job_info.c.controller_slot_attempt == str(attempt),
    )
    if require_admission:
        conditions += (
            job_info.c.controller_slot_quiescing.is_(False),
            job_info.c.schedule_state.in_(_MANAGED_JOB_ACTIVE_SCHEDULE_STATES),
        )
    return connection.execute(
        sqlalchemy.select(
            job_info.c.spot_job_id).where(*conditions).with_for_update(
                read=True)).scalar_one_or_none() == (job_id)


def _same_managed_job_origin(left: Mapping[str, Any],
                             right: Mapping[str, Any]) -> bool:
    return all(
        left.get(field) == right.get(field)
        for field in _MANAGED_JOB_ORIGIN_FIELDS)


def _request_from_mapping(
        mapping: sqlalchemy.engine.RowMapping) -> requests_lib.Request:
    values = dict(mapping)
    for field in _DATETIME_FIELDS:
        values[field] = _timestamp(values.get(field))
    return requests_lib.Request.from_durable_values(values)


def request_from_mapping(
        mapping: sqlalchemy.engine.RowMapping) -> requests_lib.Request:
    """Decode a complete durable row for same-database binding checks."""
    return _request_from_mapping(mapping)


_SCALAR_REQUEST_PROJECTION_FIELDS = (
    requests_lib.SCALAR_REQUEST_QUERY_FIELD_SET)


def _scalar_projection_entrypoint() -> None:
    """Placeholder callable for a display-only scalar projection."""


def _request_from_scalar_mapping(
        mapping: sqlalchemy.engine.RowMapping) -> requests_lib.Request:
    """Build a display-only request without decoding its durable payload."""
    values = dict(mapping)
    status = requests_lib.RequestStatus(
        values.get('status', requests_lib.RequestStatus.PENDING.value))
    schedule_type = requests_lib.ScheduleType(
        values.get('schedule_type', requests_lib.ScheduleType.SHORT.value))
    return requests_lib.Request(
        request_id=str(values.get('request_id', '')),
        name=str(values.get('name', '')),
        entrypoint=_scalar_projection_entrypoint,
        # The public status encoder emits JSON null for this display-only body.
        request_body=payloads.RequestBody.projection_placeholder(),
        status=status,
        created_at=float(_timestamp(values.get('created_at')) or 0),
        user_id=str(values.get('user_id', '')),
        pid=values.get('pid'),
        schedule_type=schedule_type,
        cluster_name=values.get('cluster_name'),
        status_msg=values.get('status_msg'),
        should_retry=bool(values.get('should_retry', False)),
        finished_at=_timestamp(values.get('finished_at')),
        file_mounts_blob_id=values.get('file_mounts_blob_id'),
        execution_generation=int(values.get('execution_generation', 0)),
        execution_quiescence_required=bool(
            values.get('execution_quiescence_required', False)),
        execution_quiesced_generation=(
            int(values['execution_quiesced_generation']) if
            values.get('execution_quiesced_generation') is not None else None),
        execution_quiesced_at=_timestamp(values.get('execution_quiesced_at')),
        execution_process_start_time_ticks=(
            int(values['execution_process_start_time_ticks'])
            if values.get('execution_process_start_time_ticks') is not None else
            None),
    )


def _request_projection_statement(
        fields: list[str] | None) -> sqlalchemy.sql.Select:
    if fields and set(fields).issubset(_SCALAR_REQUEST_PROJECTION_FIELDS):
        return sqlalchemy.select(*(REQUESTS.c[field] for field in fields))
    return sqlalchemy.select(REQUESTS)


def _request_projection_decoder(
    fields: list[str] | None,
) -> Callable[[sqlalchemy.engine.RowMapping], requests_lib.Request]:
    if fields and set(fields).issubset(_SCALAR_REQUEST_PROJECTION_FIELDS):
        return _request_from_scalar_mapping
    return _request_from_mapping


def _initialize_schema(engine: sqlalchemy.engine.Engine) -> None:
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'The durable API request backend requires PostgreSQL.')
    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.API_REQUESTS_DB_NAME,
        migration_utils.API_REQUESTS_VERSION,
        mode=migration_utils.configured_migration_mode())


_DB_MANAGER = db_utils.DatabaseManager('api_requests',
                                       _initialize_schema,
                                       engine_namespace='api-requests-control')


def initialize_and_get_db() -> sqlalchemy.engine.Engine:
    """Initialize the request schema and return its synchronous engine."""
    return _DB_MANAGER.get_engine()


async def _get_async_engine() -> sqlalchemy_async.AsyncEngine:
    return await _DB_MANAGER.get_async_engine()


def _controller_owner_from_environment() -> tuple[str, int] | None:
    """Return one request-scoped split-controller owner, if present.

    PostgreSQL ``all`` mode passes its runtime owner explicitly to startup
    maintenance. It must not publish that identity process-wide because the
    same process also executes normal-class requests.
    """
    role = os.environ.get(SERVER_ROLE_ENV_VAR, 'all')
    if role == 'all':
        return None
    if role != 'controller':
        raise RuntimeError(
            f'Controller-owned work cannot run in the {role!r} server role.')
    instance_id = os.environ.get(CONTROLLER_INSTANCE_ID_ENV_VAR)
    generation = os.environ.get(CONTROLLER_GENERATION_ENV_VAR)
    if instance_id is None or generation is None:
        raise RuntimeError(
            'The controller role has no active leadership generation.')
    try:
        uuid.UUID(instance_id)
        parsed_generation = int(generation)
    except (TypeError, ValueError) as e:
        raise RuntimeError(
            'The controller leadership identity is invalid.') from e
    if parsed_generation <= 0:
        raise RuntimeError('The controller leadership generation must be '
                           'positive.')
    return instance_id, parsed_generation


def controller_owner_from_environment() -> tuple[str, int] | None:
    """Return the current split-role controller identity, if one is active.

    This public seam lets controller-owned subsystems persist the same outer
    generation without duplicating environment parsing or treating a
    pod-local PID as a durable owner.
    """
    return _controller_owner_from_environment()


def _pg_advisory_lock_key(
    lock: sqlalchemy.sql.Alias,) -> sqlalchemy.ColumnElement[int]:
    """Reconstruct an int8 advisory key from one ``pg_locks`` row."""
    high_bits = sqlalchemy.cast(lock.c.classid, sqlalchemy.BigInteger)
    low_bits = sqlalchemy.cast(lock.c.objid, sqlalchemy.BigInteger)
    shift = sqlalchemy.literal(32, type_=sqlalchemy.Integer())
    return high_bits.op('<<')(shift).op('|')(low_bits)


def _controller_session_locks_are_live() -> sqlalchemy.ColumnElement[bool]:
    """Prove both controller locks are held by the recorded PG session."""
    election_lock = _PG_LOCKS.alias('controller_election_lock')
    generation_lock = _PG_LOCKS.alias('controller_generation_lock')
    base_lock_key = locks.postgres_lock_key(_CONTROLLER_LEADER_LOCK_ID)
    statement = sqlalchemy.select(sqlalchemy.literal(1)).select_from(
        election_lock.join(
            generation_lock,
            generation_lock.c.pid == election_lock.c.pid)).where(
                election_lock.c.pid == CONTROLLER_LEADERSHIP.c.lock_backend_pid,
                election_lock.c.locktype == 'advisory',
                election_lock.c.objsubid == 1,
                election_lock.c.mode == 'ExclusiveLock',
                election_lock.c.granted,
                _pg_advisory_lock_key(election_lock) == base_lock_key,
                generation_lock.c.locktype == 'advisory',
                generation_lock.c.objsubid == 1,
                generation_lock.c.mode == 'ExclusiveLock',
                generation_lock.c.granted,
                _pg_advisory_lock_key(generation_lock) ==
                CONTROLLER_LEADERSHIP.c.generation_lock_key)
    return sqlalchemy.exists(statement)


def _controller_leadership_is_current_predicate(
    instance_id: Any,
    generation: Any,
) -> sqlalchemy.ColumnElement[bool]:
    """Build the row and live-session controller ownership predicate."""
    return sqlalchemy.and_(
        CONTROLLER_LEADERSHIP.c.leadership_key == _CONTROLLER_LEADERSHIP_KEY,
        CONTROLLER_LEADERSHIP.c.generation == generation,
        CONTROLLER_LEADERSHIP.c.instance_id == instance_id,
        CONTROLLER_LEADERSHIP.c.released_at.is_(None),
        _controller_session_locks_are_live(),
    )


def _current_controller_leadership_statement(
    instance_id: str,
    generation: int,
    *,
    lock: bool = False,
) -> sqlalchemy.sql.Select:
    """Build the durable ownership lookup used to serialize leader writes."""
    statement = sqlalchemy.select(CONTROLLER_LEADERSHIP.c.generation).where(
        _controller_leadership_is_current_predicate(uuid.UUID(instance_id),
                                                    generation))
    if lock:
        # FOR SHARE makes generation advancement wait for this short
        # transaction. A stale generation either commits before the handoff or
        # observes the replacement generation and fails closed.
        statement = statement.with_for_update(read=True)
    return statement


def current_controller_leadership_statement(
    instance_id: str,
    generation: int,
    *,
    lock: bool = False,
) -> sqlalchemy.sql.Select:
    """Build the live controller ownership lookup for a caller transaction.

    With ``lock=True`` the caller holds a shared lock on the singleton
    leadership row until its transaction commits. Generation advancement uses
    an update of that row, so a subsystem claim and a handoff have one
    serialization point even when they use different SQLAlchemy engines.
    """
    return _current_controller_leadership_statement(instance_id,
                                                    generation,
                                                    lock=lock)


def _lock_current_controller_leadership(
        connection: sqlalchemy.engine.Connection, instance_id: str,
        generation: int) -> bool:
    return connection.execute(
        _current_controller_leadership_statement(
            instance_id, generation,
            lock=True)).scalar_one_or_none() is not None


class ControllerLeaderLease:
    """Dedicated-session controller leadership with a durable generation."""

    def __init__(self, instance_id: str | None = None) -> None:
        self.instance_id = instance_id or ensure_server_instance_id()
        self.generation: int | None = None
        self._origin_capability: str | None = None
        self._generation_lock_key: int | None = None
        self._lock = locks.PostgresLock(_CONTROLLER_LEADER_LOCK_ID, timeout=0)

    def _advance_generation(self, connection: Any,
                            capability_digest: bytes) -> tuple[int, int]:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO api_controller_leadership (
                    leadership_key, generation, instance_id,
                    lock_backend_pid, generation_lock_key,
                    origin_capability_sha256, acquired_at, heartbeat_at,
                    released_at
                )
                VALUES (%s, 1, %s::uuid, pg_backend_pid(), 0, %s,
                        clock_timestamp(), clock_timestamp(), NULL)
                ON CONFLICT (leadership_key) DO UPDATE SET
                    generation =
                        api_controller_leadership.generation + 1,
                    instance_id = EXCLUDED.instance_id,
                    lock_backend_pid = pg_backend_pid(),
                    generation_lock_key = 0,
                    origin_capability_sha256 =
                        EXCLUDED.origin_capability_sha256,
                    acquired_at = clock_timestamp(),
                    heartbeat_at = clock_timestamp(),
                    released_at = NULL
                RETURNING generation
                """, (_CONTROLLER_LEADERSHIP_KEY, self.instance_id,
                      capability_digest))
            generation = int(cursor.fetchone()[0])
            generation_lock_key = locks.postgres_lock_key(
                f'{_CONTROLLER_GENERATION_LOCK_PREFIX}{generation}')
            if generation_lock_key == locks.postgres_lock_key(
                    _CONTROLLER_LEADER_LOCK_ID):
                raise RuntimeError(
                    'Controller election and generation lock keys collided.')
            cursor.execute('SELECT pg_try_advisory_lock(%s)',
                           (generation_lock_key,))
            if not bool(cursor.fetchone()[0]):
                raise RuntimeError(
                    'Failed to acquire the controller generation lock.')
            cursor.execute(
                """
                UPDATE api_controller_leadership
                SET generation_lock_key = %s
                WHERE leadership_key = %s
                  AND generation = %s
                  AND instance_id = %s::uuid
                  AND lock_backend_pid = pg_backend_pid()
                  AND released_at IS NULL
                """, (generation_lock_key, _CONTROLLER_LEADERSHIP_KEY,
                      generation, self.instance_id))
            if cursor.rowcount != 1:
                raise RuntimeError(
                    'Controller generation changed during lock binding.')
            connection.commit()
            return generation, generation_lock_key
        except BaseException:
            connection.rollback()
            raise
        finally:
            cursor.close()

    def try_acquire(self) -> bool:
        """Acquire leadership without blocking and advance its generation."""
        if self.generation is not None:
            return True
        try:
            self._lock.acquire(blocking=False)
        except locks.LockTimeout:
            return False
        try:
            capability = controller_capability.generate()
            capability_digest = controller_capability.digest(capability)
            generation, generation_lock_key = self._lock.run_in_lock_session(
                lambda connection: self._advance_generation(
                    connection, capability_digest))
            self.generation = generation
            self._generation_lock_key = generation_lock_key
            self._origin_capability = capability
        except BaseException:
            self._origin_capability = None
            self._lock.release()
            raise
        logger.info('Acquired API controller leadership generation '
                    f'{self.generation} as {self.instance_id}.')
        return True

    @property
    def origin_capability(self) -> str:
        """Return this live generation's raw, process-local authority."""
        if self.generation is None or self._origin_capability is None:
            raise RuntimeError('Controller leadership is not acquired.')
        return self._origin_capability

    def heartbeat(self) -> bool:
        """Prove the lock session and refresh only this durable generation."""
        if self.generation is None or not self._lock.is_session_alive():
            return False

        def update(connection: Any) -> bool:
            cursor = connection.cursor()
            try:
                assert self._generation_lock_key is not None
                cursor.execute(
                    """
                    UPDATE api_controller_leadership
                    SET heartbeat_at = clock_timestamp()
                    WHERE leadership_key = %s
                      AND generation = %s
                      AND instance_id = %s::uuid
                      AND lock_backend_pid = pg_backend_pid()
                      AND generation_lock_key = %s
                      AND released_at IS NULL
                    """, (_CONTROLLER_LEADERSHIP_KEY, self.generation,
                          self.instance_id, self._generation_lock_key))
                matched = cursor.rowcount == 1
                if matched:
                    connection.commit()
                else:
                    connection.rollback()
                return matched
            except BaseException:
                connection.rollback()
                raise
            finally:
                cursor.close()

        try:
            return self._lock.run_in_lock_session(update)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Controller leadership heartbeat failed for '
                           f'generation {self.generation}: {e}')
            return False

    def backend_pid(self) -> int | None:
        """Return the exact lock-session backend PID for failure injection."""
        if self.generation is None:
            return None

        def get_pid(connection: Any) -> int:
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT pg_backend_pid()')
                pid = int(cursor.fetchone()[0])
                connection.commit()
                return pid
            except BaseException:
                connection.rollback()
                raise
            finally:
                cursor.close()

        try:
            return self._lock.run_in_lock_session(get_pid)
        except Exception:  # pylint: disable=broad-except
            return None

    def release(self) -> None:
        """Release this generation after marking it durably inactive."""
        generation = self.generation
        generation_lock_key = self._generation_lock_key
        if (generation is not None and generation_lock_key is not None and
                self._lock.is_session_alive()):

            def mark_released(connection: Any) -> None:
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        """
                        UPDATE api_controller_leadership
                        SET released_at = clock_timestamp(),
                            heartbeat_at = clock_timestamp()
                        WHERE leadership_key = %s
                          AND generation = %s
                          AND instance_id = %s::uuid
                          AND lock_backend_pid = pg_backend_pid()
                          AND generation_lock_key = %s
                          AND released_at IS NULL
                        """, (_CONTROLLER_LEADERSHIP_KEY, generation,
                              self.instance_id, generation_lock_key))
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                finally:
                    cursor.close()

            try:
                self._lock.run_in_lock_session(mark_released)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Failed to mark controller generation '
                               f'{generation} released: {e}')
        self._lock.release()
        self.generation = None
        self._generation_lock_key = None
        self._origin_capability = None


def controller_leadership_is_current(instance_id: str, generation: int) -> bool:
    """Return whether the instance owns the current unreleased generation."""
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        return connection.execute(
            _current_controller_leadership_statement(
                instance_id, generation)).scalar_one_or_none() is not None


class ExecutorTerminationEvidenceRejected(RuntimeError):
    """The observation cannot prove termination of an exact executor claim."""


class ExecutorTerminationEvidenceConflict(RuntimeError):
    """An execution tuple already has different immutable evidence."""


def _canonical_termination_timestamp(value: datetime.datetime,
                                     field: str) -> str:
    if not isinstance(value, datetime.datetime) or value.tzinfo is None:
        raise ExecutorTerminationEvidenceRejected(
            f'{field} must be a timezone-aware datetime.')
    return value.astimezone(datetime.timezone.utc).isoformat()


def record_executor_termination_evidence(
    observation: ExecutorTerminationObservation,
    *,
    observer_owner: tuple[str, int],
) -> tuple[str, ...]:
    """Append evidence for claims owned by one exactly terminated Pod.

    The transaction holds a shared lock on the live controller generation and
    never rewrites request, queue, or quiescence state.  A repeated Kubernetes
    event is idempotent only when its canonical evidence is byte-equivalent.
    """
    if not isinstance(observation, ExecutorTerminationObservation):
        raise TypeError('observation must be ExecutorTerminationObservation.')
    try:
        observer_instance_id = uuid.UUID(observer_owner[0])
        observer_generation = int(observer_owner[1])
        worker_instance_id = uuid.UUID(observation.pod_uid)
    except (AttributeError, TypeError, ValueError) as e:
        raise ExecutorTerminationEvidenceRejected(
            'Observer and Pod identities must be UUIDs.') from e
    if (str(observer_instance_id) != observer_owner[0] or
            isinstance(observer_owner[1], bool) or observer_generation <= 0 or
            str(worker_instance_id) != observation.pod_uid):
        raise ExecutorTerminationEvidenceRejected(
            'Observer generation and UUID identities must be canonical.')
    required_text = {
        'kubernetes_cluster_uid': observation.kubernetes_cluster_uid,
        'pod_namespace': observation.pod_namespace,
        'pod_name': observation.pod_name,
        'container_name': observation.container_name,
        'pod_resource_version': observation.pod_resource_version,
        'pod_event_type': observation.pod_event_type,
        'pod_phase': observation.pod_phase,
    }
    if any(not isinstance(value, str) or not value.strip() or
           value != value.strip() for value in required_text.values()):
        raise ExecutorTerminationEvidenceRejected(
            'Kubernetes termination identity fields must be canonical and '
            'nonempty.')
    if (observation.container_reason is not None and
        (not isinstance(observation.container_reason, str) or
         not observation.container_reason.strip() or
         observation.container_reason != observation.container_reason.strip())):
        raise ExecutorTerminationEvidenceRejected(
            'Container reason must be canonical nonempty text when present.')
    if (isinstance(observation.container_exit_code, bool) or
            not isinstance(observation.container_exit_code, int) or
            observation.container_exit_code < 0):
        raise ExecutorTerminationEvidenceRejected(
            'Container exit code must be a non-negative integer.')
    if (observation.pod_event_type != 'DELETED' or
            observation.pod_phase != 'Succeeded' or
            observation.container_exit_code != 0):
        raise ExecutorTerminationEvidenceRejected(
            'Termination evidence requires a successful final DELETED event.')
    deletion_timestamp = _canonical_termination_timestamp(
        observation.pod_deletion_timestamp, 'pod_deletion_timestamp')
    finished_at = _canonical_termination_timestamp(
        observation.container_finished_at, 'container_finished_at')

    expected_containers = {
        'all': 'skypilot-api',
        'executor': 'skypilot-executor',
        'controller': 'skypilot-controller',
    }
    engine = initialize_and_get_db()
    recorded: list[str] = []
    with engine.begin() as connection:
        if not _lock_current_controller_leadership(
                connection, str(observer_instance_id), observer_generation):
            raise ExecutorTerminationEvidenceRejected(
                'Observer no longer owns the exact controller generation.')
        worker = connection.execute(
            sqlalchemy.select(SERVER_INSTANCES).where(
                SERVER_INSTANCES.c.instance_id == worker_instance_id).
            with_for_update(read=True)).mappings().one_or_none()
        if worker is None:
            raise ExecutorTerminationEvidenceRejected(
                'Terminated Pod has no exact registered server instance.')
        worker_role = str(worker['role'])
        if (worker['pod_uid'] != observation.pod_uid or
                worker['pod_name'] != observation.pod_name or
                worker['pod_namespace'] != observation.pod_namespace or
                expected_containers.get(worker_role) !=
                observation.container_name or
                worker['executor_termination_evidence_capable'] is not True or
                worker['executor_termination_evidence_protocol_version'] !=
                EXECUTOR_TERMINATION_EVIDENCE_PROTOCOL_VERSION):
            raise ExecutorTerminationEvidenceRejected(
                'Terminated Pod does not match a capable registered executor.')
        db_observed_at = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        # API-server deletionTimestamp, kubelet finishedAt, and PostgreSQL
        # observed_at are different clock domains.  Keep all three for
        # diagnostics, but never infer ordering across them.  A later consumer
        # orders new provider observation after observed_at in PostgreSQL.
        requests = connection.execute(
            sqlalchemy.select(
                REQUESTS.c.request_id, REQUESTS.c.execution_generation,
                REQUESTS.c.claim_token, REQUESTS.c.worker_instance_id).where(
                    REQUESTS.c.worker_instance_id == worker_instance_id,
                    REQUESTS.c.execution_generation > 0,
                    REQUESTS.c.claim_token.is_not(None),
                    REQUESTS.c.execution_quiescence_required).with_for_update(
                        read=True)).mappings().all()
        for request in requests:
            claim_token = request['claim_token']
            if not isinstance(claim_token, uuid.UUID):
                raise ExecutorTerminationEvidenceRejected(
                    'Claim token must be an exact UUID.')
            execution_generation = int(request['execution_generation'])
            request_id = str(request['request_id'])
            evidence_id = uuid.uuid5(
                _EXECUTOR_TERMINATION_EVIDENCE_NAMESPACE, ':'.join(
                    (request_id, str(execution_generation), str(claim_token),
                     str(worker_instance_id))))
            payload = {
                'container_exit_code': observation.container_exit_code,
                'container_finished_at': finished_at,
                'container_name': observation.container_name,
                'container_reason': observation.container_reason,
                'execution_generation': execution_generation,
                'kubernetes_cluster_uid': observation.kubernetes_cluster_uid,
                'observer_controller_generation': observer_generation,
                'observer_instance_id': str(observer_instance_id),
                'pod_deletion_timestamp': deletion_timestamp,
                'pod_event_type': observation.pod_event_type,
                'pod_name': observation.pod_name,
                'pod_namespace': observation.pod_namespace,
                'pod_phase': observation.pod_phase,
                'pod_resource_version': observation.pod_resource_version,
                'pod_uid': observation.pod_uid,
                'request_id': request_id,
                'source': 'KUBERNETES_POD_FINAL_SUCCEEDED_V2',
                'worker_instance_id': str(worker_instance_id),
                'worker_role': worker_role,
                'claim_token': str(claim_token),
            }
            digest = _canonical_evidence_sha256(payload)
            values = {
                'evidence_id': evidence_id,
                'request_id': request_id,
                'execution_generation': execution_generation,
                'claim_token': claim_token,
                'worker_instance_id': worker_instance_id,
                'worker_role': worker_role,
                'kubernetes_cluster_uid': observation.kubernetes_cluster_uid,
                'pod_namespace': observation.pod_namespace,
                'pod_name': observation.pod_name,
                'pod_uid': observation.pod_uid,
                'container_name': observation.container_name,
                'pod_resource_version': observation.pod_resource_version,
                'pod_event_type': observation.pod_event_type,
                'pod_phase': observation.pod_phase,
                'pod_deletion_timestamp': observation.pod_deletion_timestamp,
                'container_finished_at': observation.container_finished_at,
                'container_exit_code': observation.container_exit_code,
                'container_reason': observation.container_reason,
                'source': 'KUBERNETES_POD_FINAL_SUCCEEDED_V2',
                'evidence_payload': payload,
                'evidence_digest': digest,
                'observer_instance_id': observer_instance_id,
                'observer_controller_generation': observer_generation,
                'observed_at': db_observed_at,
            }
            inserted = connection.execute(
                postgresql.insert(EXECUTOR_TERMINATION_EVIDENCE).values(
                    **values).on_conflict_do_nothing(index_elements=[
                        EXECUTOR_TERMINATION_EVIDENCE.c.request_id,
                        EXECUTOR_TERMINATION_EVIDENCE.c.execution_generation,
                        EXECUTOR_TERMINATION_EVIDENCE.c.claim_token,
                        EXECUTOR_TERMINATION_EVIDENCE.c.worker_instance_id,
                    ]).returning(EXECUTOR_TERMINATION_EVIDENCE.c.evidence_id)
            ).scalar_one_or_none()
            if inserted is None:
                existing = connection.execute(
                    sqlalchemy.select(EXECUTOR_TERMINATION_EVIDENCE).where(
                        EXECUTOR_TERMINATION_EVIDENCE.c.request_id ==
                        request_id,
                        EXECUTOR_TERMINATION_EVIDENCE.c.execution_generation ==
                        execution_generation,
                        EXECUTOR_TERMINATION_EVIDENCE.c.claim_token ==
                        claim_token,
                        EXECUTOR_TERMINATION_EVIDENCE.c.worker_instance_id ==
                        worker_instance_id)).mappings().one()
                if (existing['evidence_id'] != evidence_id or
                        existing['evidence_digest'] != digest or
                        existing['evidence_payload'] != payload):
                    raise ExecutorTerminationEvidenceConflict(
                        'Execution tuple already has different termination '
                        'evidence.')
            recorded.append(str(evidence_id))
    return tuple(recorded)


def controller_origin_capability_is_current(instance_id: str, generation: int,
                                            capability: str) -> bool:
    """Authenticate one controller origin against its live generation row."""
    try:
        capability_digest = controller_capability.digest(capability)
        canonical_instance_id = str(uuid.UUID(instance_id))
        parsed_generation = int(generation)
    except (AttributeError, TypeError, ValueError):
        return False
    if (canonical_instance_id != instance_id or parsed_generation <= 0 or
            isinstance(generation, bool)):
        return False
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        statement = sqlalchemy.select(CONTROLLER_LEADERSHIP.c.generation).where(
            _controller_leadership_is_current_predicate(
                uuid.UUID(canonical_instance_id), parsed_generation),
            CONTROLLER_LEADERSHIP.c.origin_capability_sha256 ==
            capability_digest)
        return connection.execute(statement).scalar_one_or_none() is not None


def fence_stale_controller_claims(instance_id: str,
                                  generation: int) -> dict[str, int]:
    """Fence claims from older controller owners before this leader starts."""
    engine = initialize_and_get_db()
    replayed = 0
    interrupted = 0
    now = sqlalchemy.func.clock_timestamp()
    with engine.begin() as connection:
        if not _lock_current_controller_leadership(connection, instance_id,
                                                   generation):
            raise RuntimeError('Controller leadership changed before stale '
                               'claims could be fenced.')
        rows = connection.execute(
            sqlalchemy.select(REQUESTS, QUEUE).join(
                QUEUE, QUEUE.c.request_id == REQUESTS.c.request_id).where(
                    REQUESTS.c.execution_class ==
                    request_registry.ExecutionClass.CONTROLLER.value,
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]), QUEUE.c.delivery_state == 'claimed',
                    sqlalchemy.or_(
                        REQUESTS.c.worker_instance_id != uuid.UUID(instance_id),
                        REQUESTS.c.controller_generation.is_(None),
                        REQUESTS.c.controller_generation !=
                        generation)).with_for_update()).mappings().all()
        for row in rows:
            registration = request_registry.resolve_handler(row['handler_name'])
            replayable = registration.replay_policy in (
                request_registry.ReplayPolicy.READ_ONLY,
                request_registry.ReplayPolicy.RECONCILE)
            if replayable:
                if row['pid'] is None:
                    if _requeue_locked_pre_effect_claim(
                            connection,
                            row,
                            status_msg=('Controller leadership changed before '
                                        'execution admission; reconciling'),
                            interrupted_reason=(
                                'Controller leadership changed before the '
                                'exact claim crossed its guarded RUNNING '
                                'transition.')):
                        replayed += 1
                    continue
                result = connection.execute(
                    sqlalchemy.update(REQUESTS).
                    where(REQUESTS.c.request_id == row['request_id']).values(
                        # Replay policy permits a later invocation; it does
                        # not prove that the old one stopped. Keep its exact
                        # owner and claimed delivery intact until that
                        # wrapper publishes generation-bound quiescence.
                        cancel_requested_at=sqlalchemy.func.coalesce(
                            REQUESTS.c.cancel_requested_at, now),
                        execution_quiescence_required=True,
                        should_retry=True,
                        status_msg=('Controller leadership changed; '
                                    'waiting for execution quiescence'),
                        interrupted_reason=(
                            'Controller leadership changed; replay is '
                            'deferred until the exact execution stops.'),
                        updated_at=now))
                replayed += int(result.rowcount == 1)
                continue
            transitioned = _terminalize_locked_request(
                connection,
                row,
                status=requests_lib.RequestStatus.CANCELLED,
                cause=event_api_models.EventCause.CONTROLLER_LEADERSHIP_LOST,
                values={
                    # Retain the exact old-owner tombstone so its dispatcher
                    # can observe controller-generation revocation, signal the
                    # effect-bearing wrapper, and publish a real quiescence
                    # receipt. Request retention owns eventual identity GC.
                    'cancel_requested_at': now,
                    'execution_quiescence_required': True,
                    'should_retry': True,
                    'finished_at': now,
                    'interrupted_reason':
                        ('Controller leadership changed with an ambiguous '
                         'mutating outcome.'),
                })
            if not transitioned:
                continue
            connection.execute(
                sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
                    CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id ==
                    row['request_id'],
                    CONTROLLER_ACTION_RESERVATIONS.c.state.in_(
                        ['reserved', 'running'])).values(state='ambiguous',
                                                         reconciliation_at=now,
                                                         updated_at=now))
            interrupted += 1
        _requeue_quiesced_replayable_requests(connection)
    return {'replayed': replayed, 'interrupted': interrupted}


async def run_distributed_singleton(
    lock_name: str,
    task_factory: Callable[[], Coroutine[Any, Any, None]],
    *,
    retry_interval_seconds: float = 5,
    connection_check_interval_seconds: float = 5,
) -> None:
    """Run one coroutine while this process owns a PostgreSQL session lock.

    The dedicated connection is also the failure detector. If the session
    becomes unusable, the owned task is cancelled before another process can
    acquire the released lock and start a replacement.
    """
    lock_statement = sqlalchemy.text(
        'SELECT pg_try_advisory_lock('
        'hashtextextended(CAST(:lock_name AS text), 0))')
    unlock_statement = sqlalchemy.text(
        'SELECT pg_advisory_unlock('
        'hashtextextended(CAST(:lock_name AS text), 0))')
    liveness_statement = sqlalchemy.text('SELECT 1')
    while True:
        owned_task: asyncio.Task | None = None
        acquired = False
        try:
            engine = await _get_async_engine()
            async with engine.connect() as connection:
                acquired = bool(
                    (await
                     connection.execute(lock_statement,
                                        {'lock_name': lock_name})).scalar_one())
                # The lock is session-scoped and survives commit. End the
                # implicit SELECT transaction before starting owned work or
                # closing a losing connection for its retry sleep.
                await connection.commit()
                if acquired:
                    logger.info(f'Acquired distributed singleton {lock_name}.')
                    owned_task = asyncio.create_task(task_factory())
                    try:
                        while not owned_task.done():
                            done, _ = await asyncio.wait(
                                {owned_task},
                                timeout=connection_check_interval_seconds)
                            if done:
                                await owned_task
                                return
                            await connection.execute(liveness_statement)
                            await connection.commit()
                    finally:
                        if owned_task is not None and not owned_task.done():
                            owned_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await owned_task
                        with contextlib.suppress(Exception):
                            await connection.execute(unlock_statement,
                                                     {'lock_name': lock_name})
                            await connection.commit()
                        logger.info(
                            f'Released distributed singleton {lock_name}.')
            if not acquired:
                await asyncio.sleep(retry_interval_seconds)
        except asyncio.CancelledError:
            if owned_task is not None and not owned_task.done():
                owned_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await owned_task
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                f'Distributed singleton {lock_name} lost its PostgreSQL '
                f'session: {e}')
            await asyncio.sleep(retry_interval_seconds)


def _queue_values(request: requests_lib.Request) -> dict[str, Any]:
    now = sqlalchemy.func.clock_timestamp()
    return {
        'request_id': request.request_id,
        'schedule_type': request.schedule_type.value,
        'priority': 0,
        'available_at': now,
        'enqueued_at': now,
        'ignore_return_value': request.ignore_return_value,
        'retryable': request.retryable,
        'precondition_type': request.precondition_type,
        'precondition_payload': request.precondition_payload,
        'precondition_deadline': _utc_datetime(request.precondition_deadline),
        'precondition_attempts': 0,
        'delivery_state': 'queued',
        'claim_generation': None,
        'updated_at': now,
    }


def _requeue_quiesced_replayable_requests(
    connection: sqlalchemy.engine.Connection,
    *,
    request_id: str | None = None,
) -> int:
    """Consume exact quiescence into the one canonical replay queue path.

    Lease expiry and controller handoff revoke an invocation, but never prove
    that its effect-bearing wrapper stopped.  Those paths retain the claimed
    delivery and complete owner tuple.  This reducer is the only path that may
    erase that tuple and make a replayable request executable again, and it does
    so only while consuming the exact generation's durable receipt.

    Terminal requests are never reopened.
    """
    exact_receipt = sqlalchemy.and_(
        REQUESTS.c.execution_quiescence_required,
        REQUESTS.c.execution_quiesced_generation ==
        REQUESTS.c.execution_generation,
        REQUESTS.c.execution_quiesced_at.is_not(None))
    active_barrier = sqlalchemy.and_(
        REQUESTS.c.status.in_([
            status.value
            for status in requests_lib.RequestStatus.active_statuses()
        ]), REQUESTS.c.cancel_requested_at.is_not(None), exact_receipt)
    shutdown_retry = sqlalchemy.and_(
        REQUESTS.c.status.in_([
            status.value
            for status in requests_lib.RequestStatus.active_statuses()
        ]), exact_receipt,
        REQUESTS.c.interrupted_reason == _GRACEFUL_SHUTDOWN_RETRY_REASON)
    replayable_handler_names = tuple(
        name for registration in request_registry.registered_handlers()
        if registration.replay_policy in (
            request_registry.ReplayPolicy.READ_ONLY,
            request_registry.ReplayPolicy.RECONCILE)
        for name in (registration.name, *registration.aliases))
    statement = sqlalchemy.select(REQUESTS).where(
        sqlalchemy.or_(
            shutdown_retry,
            sqlalchemy.and_(
                active_barrier,
                REQUESTS.c.handler_name.in_(replayable_handler_names))))
    if request_id is not None:
        statement = statement.where(REQUESTS.c.request_id == request_id)
    else:
        statement = statement.order_by(
            REQUESTS.c.updated_at).limit(_MAX_EXPIRED_CLAIMS_PER_SWEEP)
    rows = connection.execute(
        statement.with_for_update(skip_locked=True)).mappings().all()
    replayed = 0
    now = sqlalchemy.func.clock_timestamp()
    for row in rows:
        try:
            registration = request_registry.resolve_handler(row['handler_name'])
        except ValueError:
            # A removed plugin cannot be reconstructed safely by this
            # build.  Retain its evidence for operator reconciliation.
            continue
        policy = registration.replay_policy
        status = requests_lib.RequestStatus(str(row['status']))
        if status in requests_lib.RequestStatus.finished_status():
            continue
        graceful_shutdown_retry = (
            row['interrupted_reason'] == _GRACEFUL_SHUTDOWN_RETRY_REASON)
        if (not graceful_shutdown_retry and
                policy not in (request_registry.ReplayPolicy.READ_ONLY,
                               request_registry.ReplayPolicy.RECONCILE)):
            continue

        queue_row = connection.execute(
            sqlalchemy.select(QUEUE).where(
                QUEUE.c.request_id ==
                row['request_id']).with_for_update()).mappings().one_or_none()
        generation = int(row['execution_generation'])
        if graceful_shutdown_retry:
            if (queue_row is None or queue_row['delivery_state'] != 'claimed' or
                    queue_row['claim_generation'] != generation or
                    row['claim_token'] is None or
                    row['worker_instance_id'] is None or
                    row['lease_expires_at'] is None):
                continue
            if _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=event_api_models.EventCause.GRACEFUL_SHUTDOWN_RETRY,
                    values={
                        'should_retry': True,
                        'finished_at': now,
                        'status_msg': None,
                    }):
                replayed += 1
            continue
        if (queue_row is None or queue_row['delivery_state'] != 'claimed' or
                queue_row['claim_generation'] != generation or
                row['claim_token'] is None or
                row['worker_instance_id'] is None or
                row['lease_expires_at'] is None):
            continue
        next_status = requests_lib.RequestStatus.WAITING

        result = connection.execute(
            sqlalchemy.update(REQUESTS).where(
                REQUESTS.c.request_id == row['request_id'],
                REQUESTS.c.execution_generation == generation,
                REQUESTS.c.status == status.value,
                REQUESTS.c.execution_quiescence_required,
                REQUESTS.c.execution_quiesced_generation == generation,
                REQUESTS.c.execution_quiesced_at.is_not(None)).values(
                    status=next_status.value,
                    terminal_cause=None,
                    return_value=None,
                    error=None,
                    pid=None,
                    execution_process_start_time_ticks=None,
                    claim_token=None,
                    worker_instance_id=None,
                    controller_generation=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                    cancel_requested_at=None,
                    cancel_acknowledged_at=None,
                    execution_quiescence_required=False,
                    execution_quiesced_generation=None,
                    execution_quiesced_at=None,
                    should_retry=False,
                    finished_at=None,
                    status_msg=None,
                    interrupted_reason=None,
                    updated_at=now))
        if result.rowcount != 1:
            continue
        queue_result = connection.execute(
            sqlalchemy.update(QUEUE).where(
                QUEUE.c.request_id == row['request_id'],
                QUEUE.c.delivery_state == 'claimed',
                QUEUE.c.claim_generation == generation).values(
                    delivery_state='queued',
                    claim_generation=None,
                    available_at=now,
                    updated_at=now))
        if queue_result.rowcount != 1:
            raise RuntimeError('Replay barrier lost its locked delivery.')
        replayed += 1
    return replayed


def _requeue_locked_pre_effect_claim(
    connection: sqlalchemy.engine.Connection,
    row: Mapping[str, Any],
    *,
    status_msg: str,
    interrupted_reason: str,
) -> bool:
    """Prove and replay one locked claim that never entered RUNNING.

    A claimed row with no PID has not crossed ``try_mark_running()``.  While
    both the request and delivery are locked, revoking that exact generation
    before recording its quiescence receipt makes every late child admission
    lose the claim predicates.  This is therefore authoritative pre-effect
    proof, shared by worker loss, lease expiry, and controller handoff.
    """
    if (row['pid'] is not None or
            row['execution_process_start_time_ticks'] is not None or
            row['claim_token'] is None or row['worker_instance_id'] is None or
            row['lease_expires_at'] is None or
            row['delivery_state'] != 'claimed' or
            row['claim_generation'] != row['execution_generation'] or
            requests_lib.RequestStatus(str(row['status']))
            not in requests_lib.RequestStatus.executable_statuses()):
        return False
    generation = int(row['execution_generation'])
    now = sqlalchemy.func.clock_timestamp()
    receipt = connection.execute(
        sqlalchemy.update(REQUESTS).where(
            REQUESTS.c.request_id == row['request_id'],
            REQUESTS.c.status.in_([
                status.value
                for status in requests_lib.RequestStatus.executable_statuses()
            ]),
            REQUESTS.c.execution_generation == generation,
            REQUESTS.c.claim_token == row['claim_token'],
            REQUESTS.c.worker_instance_id == row['worker_instance_id'],
            REQUESTS.c.lease_expires_at == row['lease_expires_at'],
            REQUESTS.c.pid.is_(None),
            REQUESTS.c.execution_process_start_time_ticks.is_(None),
        ).values(cancel_requested_at=sqlalchemy.func.coalesce(
            REQUESTS.c.cancel_requested_at, now),
                 execution_quiescence_required=True,
                 execution_quiesced_generation=generation,
                 execution_quiesced_at=now,
                 should_retry=True,
                 status_msg=status_msg,
                 interrupted_reason=interrupted_reason,
                 updated_at=now))
    if receipt.rowcount != 1:
        return False
    replayed = _requeue_quiesced_replayable_requests(connection,
                                                     request_id=str(
                                                         row['request_id']))
    if replayed != 1:
        raise RuntimeError('Pre-effect proof did not reach the canonical '
                           'replay reducer.')
    return True


def _terminalize_stale_managed_job_request(
    connection: sqlalchemy.engine.Connection,
    row: Mapping[str, Any],
) -> bool:
    """Consume one locked stale nested delivery as exact pre-effect work."""
    if _managed_job_origin(row) is None:
        return False
    status = requests_lib.RequestStatus(str(row['status']))
    if status not in requests_lib.RequestStatus.executable_statuses():
        return False
    if (row['pid'] is not None or
            row['execution_process_start_time_ticks'] is not None):
        return False
    generation = int(row['execution_generation'])
    now = sqlalchemy.func.clock_timestamp()
    return _terminalize_locked_request(
        connection,
        row,
        status=requests_lib.RequestStatus.CANCELLED,
        cause=event_api_models.EventCause.CONTROLLER_LEADERSHIP_LOST,
        values={
            'cancel_requested_at': now,
            'execution_quiescence_required': True,
            'execution_quiesced_generation': generation,
            'execution_quiesced_at': now,
            'should_retry': False,
            'finished_at': now,
            'status_msg': 'Managed-job controller attempt is no longer current.',
            'interrupted_reason': _MANAGED_JOB_QUIESCE_REASON,
        })


def _insert_request_and_queue(
    connection: sqlalchemy.engine.Connection,
    request: requests_lib.Request,
    *,
    resource_action_id: uuid.UUID | None = None,
    resource_action_attempt: int | None = None,
    ordinary_launch_association_id: uuid.UUID | None = None,
    non_pool_profile_values: Mapping[str, Any] | None = None,
) -> bool:
    """Insert one request and its queue row in the caller's transaction."""
    if ((resource_action_id is None) != (resource_action_attempt is None)):
        raise ValueError('Resource-action request correlation requires both '
                         'an action ID and an attempt number.')
    values = _request_values_for_db(request)
    if not _lock_managed_job_origin(connection, values, require_admission=True):
        raise request_storage.ManagedJobRequestQuiescenceError(
            'Managed-job nested request creation lost its exact controller '
            'attempt authority.')
    legacy_service_name = _legacy_ordinary_launch_service_name(values)
    if legacy_service_name is not None:
        # Promotion owns the exclusive side of this service-scoped transaction
        # lock before it scans legacy requests.  Taking the shared side before
        # INSERT closes the admission phantom without table locks (which can
        # deadlock a queue claimant's ROW SHARE -> ROW EXCLUSIVE upgrade).
        ordinary_launch_binding.lock_legacy_request_admission_in_connection(
            connection, legacy_service_name)
    values['resource_action_id'] = resource_action_id
    values['resource_action_attempt'] = resource_action_attempt
    values['ordinary_launch_association_id'] = (ordinary_launch_association_id)
    if non_pool_profile_values is not None:
        expected_fields = frozenset({
            'binding_protocol_version',
            'profile_kind',
            'profile_version',
            'profile_digest',
            'capability_cohort_epoch',
            'capability_profile_set_digest',
            'receipt_protocol_version',
        })
        if (ordinary_launch_association_id is None or
                frozenset(non_pool_profile_values) != expected_fields):
            raise ValueError(
                'Generic launch request profile values are incomplete.')
        values.update(non_pool_profile_values)
    result = connection.execute(
        postgresql.insert(REQUESTS).values(**values).on_conflict_do_nothing(
            index_elements=[REQUESTS.c.request_id]).returning(
                REQUESTS.c.request_id))
    inserted = result.scalar_one_or_none() is not None
    if inserted and request.should_enqueue:
        connection.execute(
            postgresql.insert(QUEUE).values(
                **_queue_values(request)).on_conflict_do_nothing(
                    index_elements=[QUEUE.c.request_id]))
    return inserted


def _legacy_ordinary_launch_service_name(
        values: Mapping[str, Any]) -> str | None:
    """Return the exact service scope for one valid legacy launch payload."""
    if (values.get('handler_name') != _LEGACY_ORDINARY_LAUNCH_HANDLER_NAME or
            values.get('payload_type') != _LEGACY_ORDINARY_LAUNCH_PAYLOAD_TYPE):
        return None
    payload_json = values.get('payload_json')
    if (not isinstance(payload_json, dict) or
            payload_json.get('is_launched_by_sky_serve_controller')
            is not True):
        return None
    context = payload_json.get('extra_launch_context')
    if not isinstance(context, dict):
        return None
    service_name = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
    if not isinstance(service_name, str) or not service_name:
        return None
    return service_name


def _validate_retention_pin(pin_kind: str, pin_id: uuid.UUID) -> None:
    if not isinstance(pin_kind, str) or not 1 <= len(pin_kind) <= 128:
        raise ValueError('Retention pin kind must contain 1-128 characters.')
    if not isinstance(pin_id, uuid.UUID):
        raise ValueError('Retention pin ID must be a UUID.')


def insert_request_retention_pin_in_transaction(
    connection: sqlalchemy.engine.Connection,
    request_id: str,
    pin_kind: str,
    pin_id: uuid.UUID,
) -> None:
    """Insert one active retention pin in the caller's transaction."""
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('A retention pin requires a request ID.')
    _validate_retention_pin(pin_kind, pin_id)
    connection.execute(
        sqlalchemy.insert(REQUEST_RETENTION_PINS).values(request_id=request_id,
                                                         pin_kind=pin_kind,
                                                         pin_id=pin_id))


def delete_request_retention_pin_in_transaction(
    connection: sqlalchemy.engine.Connection,
    request_id: str,
    pin_kind: str,
    pin_id: uuid.UUID,
) -> bool:
    """Delete one exact active pin after its owner durably settles."""
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('A retention pin requires a request ID.')
    _validate_retention_pin(pin_kind, pin_id)
    result = connection.execute(
        sqlalchemy.delete(REQUEST_RETENTION_PINS).where(
            REQUEST_RETENTION_PINS.c.request_id == request_id,
            REQUEST_RETENTION_PINS.c.pin_kind == pin_kind,
            REQUEST_RETENTION_PINS.c.pin_id == pin_id))
    return result.rowcount == 1


def request_retention_pin_is_active_in_transaction(
    connection: sqlalchemy.engine.Connection,
    request_id: str,
    pin_kind: str,
    pin_id: uuid.UUID,
) -> bool:
    """Return whether one exact active pin exists without taking a lock."""
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('A retention pin requires a request ID.')
    _validate_retention_pin(pin_kind, pin_id)
    result = connection.execute(
        sqlalchemy.select(sqlalchemy.literal(True)).where(
            sqlalchemy.exists().where(
                REQUEST_RETENTION_PINS.c.request_id == request_id,
                REQUEST_RETENTION_PINS.c.pin_kind == pin_kind,
                REQUEST_RETENTION_PINS.c.pin_id ==
                pin_id))).scalar_one_or_none()
    return bool(result)


def _validate_bound_launch_claim_in_transaction(
    connection: sqlalchemy.engine.Connection,
    association_id: uuid.UUID,
    claim: request_storage.ExecutionClaim,
    *,
    handler_name: str,
    require_non_pool_profile: bool,
) -> bool:
    """Lock and validate the exact request/queue/pin effect authority.

    Serve calls this only after locking its association. The function then
    follows the global suffix order: request, queue, retention pin. A valid
    claim proves only request-executor authority; Serve still owns the replica
    and effect-phase fence.
    """
    if (not isinstance(association_id, uuid.UUID) or
            claim.worker_instance_id is None):
        return False
    try:
        claim_token = uuid.UUID(claim.claim_token)
        worker_instance_id = uuid.UUID(claim.worker_instance_id)
    except (TypeError, ValueError):
        return False
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    predicates: list[sqlalchemy.ColumnElement[bool]] = [
        REQUESTS.c.request_id == claim.request_id,
        REQUESTS.c.handler_name == handler_name,
        REQUESTS.c.ordinary_launch_association_id == association_id,
        REQUESTS.c.status == requests_lib.RequestStatus.RUNNING.value,
        REQUESTS.c.execution_generation == claim.execution_generation,
        REQUESTS.c.claim_token == claim_token,
        REQUESTS.c.worker_instance_id == worker_instance_id,
        REQUESTS.c.lease_expires_at > sqlalchemy.func.clock_timestamp(),
        REQUESTS.c.execution_quiescence_required,
        QUEUE.c.delivery_state == 'claimed',
        QUEUE.c.claim_generation == claim.execution_generation,
        associations.c.association_id == association_id,
    ]
    if require_non_pool_profile:
        predicates.extend((
            REQUESTS.c.binding_protocol_version ==
            associations.c.binding_protocol_version,
            REQUESTS.c.profile_kind == associations.c.profile_kind,
            REQUESTS.c.profile_version == associations.c.profile_version,
            REQUESTS.c.profile_digest == associations.c.profile_digest,
            REQUESTS.c.capability_cohort_epoch ==
            associations.c.capability_cohort_epoch,
            REQUESTS.c.capability_profile_set_digest ==
            associations.c.capability_profile_set_digest,
            REQUESTS.c.receipt_protocol_version ==
            associations.c.receipt_protocol_version,
        ))
    else:
        predicates.append(REQUESTS.c.binding_protocol_version.is_(None))
    row = connection.execute(
        sqlalchemy.select(REQUESTS.c.request_id).select_from(
            REQUESTS.join(
                QUEUE, QUEUE.c.request_id == REQUESTS.c.request_id).join(
                    associations, associations.c.association_id ==
                    REQUESTS.c.ordinary_launch_association_id)).where(
                        *predicates).with_for_update()).scalar_one_or_none()
    if row is None:
        return False
    pin = connection.execute(
        sqlalchemy.select(REQUEST_RETENTION_PINS.c.pin_id).where(
            REQUEST_RETENTION_PINS.c.pin_kind ==
            ORDINARY_LAUNCH_RETENTION_PIN_KIND,
            REQUEST_RETENTION_PINS.c.pin_id == association_id,
            REQUEST_RETENTION_PINS.c.request_id ==
            claim.request_id).with_for_update()).scalar_one_or_none()
    return pin == association_id


def validate_bound_ordinary_launch_claim_in_transaction(
    connection: sqlalchemy.engine.Connection,
    association_id: uuid.UUID,
    claim: request_storage.ExecutionClaim,
) -> bool:
    """Validate one exact protocol-v1 ordinary execution claim."""
    return _validate_bound_launch_claim_in_transaction(
        connection,
        association_id,
        claim,
        handler_name=(
            ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME),
        require_non_pool_profile=False)


def validate_bound_non_pool_launch_claim_in_transaction(
    connection: sqlalchemy.engine.Connection,
    association_id: uuid.UUID,
    claim: request_storage.ExecutionClaim,
) -> bool:
    """Validate one exact protocol-v2 request/profile execution claim."""
    return _validate_bound_launch_claim_in_transaction(
        connection,
        association_id,
        claim,
        handler_name=non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME,
        require_non_pool_profile=True)


def _request_has_no_active_retention_pin() -> sqlalchemy.ColumnElement[bool]:
    return ~sqlalchemy.exists().where(
        REQUEST_RETENTION_PINS.c.request_id == REQUESTS.c.request_id)


def _request_is_retention_safe() -> sqlalchemy.ColumnElement[bool]:
    """Require action and execution evidence to be durably settled."""
    settled_attempt = sqlalchemy.exists().where(
        RESOURCE_ACTION_ATTEMPTS.c.action_id == REQUESTS.c.resource_action_id,
        RESOURCE_ACTION_ATTEMPTS.c.attempt ==
        REQUESTS.c.resource_action_attempt,
        RESOURCE_ACTION_ATTEMPTS.c.request_id == REQUESTS.c.request_id,
        RESOURCE_ACTION_ATTEMPTS.c.mutation_boundary == 'SETTLED')
    action_is_safe = sqlalchemy.or_(REQUESTS.c.resource_action_id.is_(None),
                                    settled_attempt)
    execution_is_safe = sqlalchemy.or_(
        ~REQUESTS.c.execution_quiescence_required,
        sqlalchemy.and_(
            REQUESTS.c.execution_quiesced_generation ==
            REQUESTS.c.execution_generation,
            REQUESTS.c.execution_quiesced_at.is_not(None)))
    return sqlalchemy.and_(action_is_safe, execution_is_safe,
                           _request_has_no_active_retention_pin())


def insert_bound_request_and_queue_in_transaction(
    connection: sqlalchemy.engine.Connection,
    request: requests_lib.Request,
    *,
    ordinary_launch_association_id: uuid.UUID,
) -> bool:
    """Insert one bound request and queue row in the caller transaction."""
    if not request.should_enqueue:
        raise ValueError('Bound request insertion must enqueue atomically.')
    if request.retryable:
        raise ValueError('Bound request execution cannot use generic retry.')
    if not isinstance(ordinary_launch_association_id, uuid.UUID):
        raise ValueError('Bound request association ID must be a UUID.')
    registration = request_registry.registration_for_handler(request.entrypoint)
    if registration.name != (
            ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME):
        raise ValueError('A bound request requires the distinct ordinary '
                         'launch handler.')
    inserted = _insert_request_and_queue(
        connection,
        request,
        ordinary_launch_association_id=ordinary_launch_association_id)
    if inserted:
        insert_request_retention_pin_in_transaction(
            connection, request.request_id, ORDINARY_LAUNCH_RETENTION_PIN_KIND,
            ordinary_launch_association_id)
    return inserted


def _non_pool_request_profile_values(
    identity: ordinary_launch_binding_lib.NonPoolBindingIdentity,
) -> dict[str, Any]:
    """Return the API011 tuple correlated with the Serve047 association."""
    return {
        'binding_protocol_version':
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        'profile_kind': identity.profile.kind.value,
        'profile_version': identity.profile.version,
        'profile_digest': identity.profile.digest,
        'capability_cohort_epoch': identity.capability_cohort_epoch,
        'capability_profile_set_digest': identity.capability_profile_set_digest,
        'receipt_protocol_version': identity.receipt_protocol_version,
    }


def insert_bound_non_pool_request_and_queue_in_transaction(
    connection: sqlalchemy.engine.Connection,
    request: requests_lib.Request,
    *,
    identity: ordinary_launch_binding_lib.NonPoolBindingIdentity,
) -> bool:
    """Insert one generic request, queue delivery, and pin atomically."""
    if not request.should_enqueue or request.retryable:
        raise ValueError(
            'Generic bound request requires one non-retryable delivery.')
    if not isinstance(identity, ordinary_launch_binding.NonPoolBindingIdentity):
        raise ValueError('Generic request requires a v2 binding identity.')
    registration = request_registry.registration_for_handler(request.entrypoint)
    if registration.name != non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME:
        raise ValueError('Generic bound request requires its distinct handler.')
    inserted = _insert_request_and_queue(
        connection,
        request,
        ordinary_launch_association_id=identity.association_id,
        non_pool_profile_values=_non_pool_request_profile_values(identity))
    if inserted:
        insert_request_retention_pin_in_transaction(
            connection, request.request_id, ORDINARY_LAUNCH_RETENTION_PIN_KIND,
            identity.association_id)
    return inserted


def _validate_existing_bound_request_in_transaction(
    connection: sqlalchemy.engine.Connection,
    request: requests_lib.Request,
    identity: ordinary_launch_binding_lib.BindingIdentity,
    admission: ordinary_launch_binding_lib.BindingAdmission,
) -> None:
    """Lock and verify the API half of one exact admission retry."""
    request_row = connection.execute(
        sqlalchemy.select(REQUESTS).where(
            REQUESTS.c.request_id ==
            request.request_id).with_for_update()).mappings().one_or_none()
    if request_row is None:
        if admission.expects_active_request:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Unsettled binding has no correlated API request.')
        return

    expected = _request_values_for_db(request)
    immutable_columns = (
        'request_id',
        'name',
        'handler_name',
        'payload_type',
        'payload_format',
        'payload_version',
        'payload_json',
        'execution_class',
        'cluster_name',
        'schedule_type',
        'user_id',
        'ignore_return_value',
        'retryable',
    )
    if (request_row['ordinary_launch_association_id'] != identity.association_id
            or any(request_row[column] != expected[column]
                   for column in immutable_columns)):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Existing API request does not match the exact binding intent.')
    if isinstance(identity, ordinary_launch_binding.NonPoolBindingIdentity):
        profile_values = _non_pool_request_profile_values(identity)
        if any(request_row[field] != value
               for field, value in profile_values.items()):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Existing API request has a different generic profile.')
    elif request_row['binding_protocol_version'] is not None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Historical ordinary request unexpectedly has a generic profile.')

    # Global cross-lineage lock suffix: request, queue, then active pin.
    queue_row = connection.execute(
        sqlalchemy.select(QUEUE).where(
            QUEUE.c.request_id ==
            request.request_id).with_for_update()).mappings().one_or_none()
    active = (str(request_row['status']) in {
        status.value for status in requests_lib.RequestStatus.active_statuses()
    })
    if active and queue_row is None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Active bound API request has no durable queue delivery.')
    if not active and queue_row is not None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Terminal bound API request retained a queue delivery.')
    if queue_row is not None:
        expected_queue = _queue_values(request)
        queue_columns = (
            'schedule_type',
            'ignore_return_value',
            'retryable',
            'precondition_type',
            'precondition_payload',
            'precondition_deadline',
        )
        if (queue_row['delivery_state'] not in ('queued', 'claimed') or
                any(queue_row[column] != expected_queue[column]
                    for column in queue_columns)):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Existing queue delivery does not match the bound request.')
        if ((queue_row['delivery_state'] == 'queued') !=
            (queue_row['claim_generation'] is None)):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Existing bound queue claim state is inconsistent.')
        if (queue_row['delivery_state'] == 'claimed' and
                int(queue_row['claim_generation']) != int(
                    request_row['execution_generation'])):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Existing bound queue generation is inconsistent.')

    pin_row = connection.execute(
        sqlalchemy.select(REQUEST_RETENTION_PINS).where(
            REQUEST_RETENTION_PINS.c.pin_kind ==
            ORDINARY_LAUNCH_RETENTION_PIN_KIND,
            REQUEST_RETENTION_PINS.c.pin_id == identity.association_id).
        with_for_update()).mappings().one_or_none()
    if admission.expects_active_request:
        if (pin_row is None or pin_row['request_id'] != request.request_id):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Unsettled binding has no exact active retention pin.')
    elif pin_row is not None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Settled binding retained an active request pin.')


def bind_and_enqueue_ordinary_launch(
    request: requests_lib.Request,
    identity: ordinary_launch_binding_lib.BindingIdentity,
) -> ordinary_launch_binding_lib.BindingAdmission:
    """Atomically create or verify both halves of a bound Serve launch."""
    if request.request_id != identity.request_id:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Request ID does not match the server-derived binding identity.')
    if request.retryable or not request.should_enqueue:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Bound ordinary launch requires one non-retryable queue delivery.')
    registration = request_registry.registration_for_handler(request.entrypoint)
    if registration.name != (
            ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Bound ordinary launch requires its distinct durable handler.')
    _, _, backend_capable = _resolved_request_backend_capability()
    if not backend_capable:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Ordinary launch binding requires the built-in PostgreSQL '
            'request and queue backends.')

    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Ordinary launch binding requires central PostgreSQL state.')
    with engine.begin() as connection:
        if not ordinary_launch_binding_fleet_capable(connection=connection):
            raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
                'Ordinary launch binding is waiting for one fully capable '
                'API/executor fleet stale window.')
        admission = ordinary_launch_binding.insert_or_get_locked(
            connection, identity)
        if (admission.association_id != str(identity.association_id) or
                admission.request_id != identity.request_id):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Serve admission returned a different deterministic identity.')
        ordinary_launch_binding.install_bound_context(
            request.request_body, identity, admission.launch_generation)
        if admission.created:
            inserted = insert_bound_request_and_queue_in_transaction(
                connection,
                request,
                ordinary_launch_association_id=identity.association_id)
            if not inserted:
                raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                    'New Serve association collided with an API request ID.')
        else:
            _validate_existing_bound_request_in_transaction(
                connection, request, identity, admission)
        return admission


def bind_and_enqueue_non_pool_launch_in_transaction(
    connection: sqlalchemy.engine.Connection,
    request: requests_lib.Request,
    identity: ordinary_launch_binding_lib.NonPoolBindingIdentity,
) -> ordinary_launch_binding_lib.BindingAdmission:
    """Commit one generic association/request/queue/pin on a caller txn."""
    if not isinstance(identity, ordinary_launch_binding.NonPoolBindingIdentity):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Generic launch admission requires a protocol-v2 identity.')
    if (request.request_id != identity.request_id or request.retryable or
            not request.should_enqueue):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Generic launch requires its exact non-retryable queue delivery.')
    registration = request_registry.registration_for_handler(request.entrypoint)
    if registration.name != non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Generic launch requires its distinct durable handler.')
    _, _, backend_capable = _resolved_request_backend_capability()
    if not backend_capable:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Generic launch binding requires the built-in PostgreSQL '
            'request and queue backends.')
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Generic launch binding requires central PostgreSQL state.')
    submitted_user_id = request.request_body.env_vars.get(
        skylet_constants.USER_ID_ENV_VAR)
    if (request.user_id != identity.tenant_scope or
            submitted_user_id != identity.tenant_scope):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Generic launch request, binding, and LaunchBody tenant scopes '
            'must be identical.')
    admission = ordinary_launch_binding.insert_or_get_locked(
        connection, identity)
    if (admission.association_id != str(identity.association_id) or
            admission.request_id != identity.request_id):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Generic admission returned a different deterministic identity.')
    (ordinary_launch_binding.
     validate_non_pool_submission_execution_context_in_connection(
         connection, identity, request.request_body.extra_launch_context))
    ordinary_launch_binding.install_bound_non_pool_context(
        request.request_body, identity, admission.launch_generation)
    if admission.created:
        inserted = insert_bound_non_pool_request_and_queue_in_transaction(
            connection, request, identity=identity)
        if not inserted:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'New generic association collided with an API request ID.')
    else:
        _validate_existing_bound_request_in_transaction(connection, request,
                                                        identity, admission)
    return admission


def bind_and_enqueue_non_pool_launch(
    request: requests_lib.Request,
    identity: ordinary_launch_binding_lib.NonPoolBindingIdentity,
) -> ordinary_launch_binding_lib.BindingAdmission:
    """Atomically commit one generic association/request/queue/pin tuple."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Generic launch binding requires central PostgreSQL state.')
    # SERVER_INSTANCES is heartbeat-owned rather than transactional request
    # authority. Observe rollout readiness before taking service/replica locks;
    # the locked service capability tuple remains the commit-time fence.
    if not non_pool_launch_binding_fleet_capable():
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Generic launch binding is waiting for one exact capable '
            'API/executor cohort stale window.')
    with engine.begin() as connection:
        return bind_and_enqueue_non_pool_launch_in_transaction(
            connection, request, identity)


def _bound_context_from_association(
    association: Mapping[str, Any],
) -> ordinary_launch_binding_lib.BoundLaunchContext:
    """Build the immutable controller view without trusting process state."""
    return ordinary_launch_binding.bound_context_from_association(association)


def _lock_bound_request_evidence(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundLaunchContext,
) -> tuple[BoundOrdinaryLaunchRequestFacts, Mapping[str, Any] | None,
           Mapping[str, Any] | None, Mapping[str, Any] | None]:
    """Lock the request/queue/pin suffix after Serve authority is locked."""
    request_row = connection.execute(
        sqlalchemy.select(REQUESTS).where(
            REQUESTS.c.request_id ==
            context.request_id).with_for_update()).mappings().one_or_none()
    queue_row = connection.execute(
        sqlalchemy.select(QUEUE).where(
            QUEUE.c.request_id ==
            context.request_id).with_for_update()).mappings().one_or_none()
    pin_row = connection.execute(
        sqlalchemy.select(REQUEST_RETENTION_PINS).where(
            REQUEST_RETENTION_PINS.c.pin_kind ==
            ORDINARY_LAUNCH_RETENTION_PIN_KIND,
            REQUEST_RETENTION_PINS.c.pin_id ==
            context.association_id).with_for_update()).mappings().one_or_none()
    if pin_row is not None and pin_row['request_id'] != context.request_id:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Association retention pin names a different API request.')
    if request_row is None:
        facts = BoundOrdinaryLaunchRequestFacts(
            association_id=context.association_id,
            request_id=context.request_id,
            exists=False,
            status=None,
            terminal_cause=None,
            execution_generation=None,
            claim_token=None,
            worker_instance_id=None,
            lease_expires_at=None,
            claim_exists=False,
            claim_active=False,
            claim_expired=False,
            queue_exists=queue_row is not None,
            queue_delivery_state=(None if queue_row is None else str(
                queue_row['delivery_state'])),
            queue_claim_generation=(None if queue_row is None or
                                    queue_row['claim_generation'] is None else
                                    int(queue_row['claim_generation'])),
            execution_quiescence_required=False,
            execution_quiesced_generation=None,
            execution_quiesced_at=None,
            quiescent=False,
            retention_pin_active=pin_row is not None,
            return_value=None,
            error=None,
            error_decode_failed=False)
        return facts, None, queue_row, pin_row
    is_non_pool = isinstance(context,
                             ordinary_launch_binding.BoundNonPoolLaunchContext)
    expected_handler = (
        non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME if is_non_pool else
        ordinary_launch_request.BOUND_ORDINARY_LAUNCH_HANDLER_NAME)
    if (request_row['handler_name'] != expected_handler or
            request_row['ordinary_launch_association_id'] !=
            context.association_id):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Correlated request does not name the exact bound handler.')
    if is_non_pool:
        assert isinstance(context,
                          ordinary_launch_binding.BoundNonPoolLaunchContext)
        expected_profile = {
            'binding_protocol_version':
                ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION,
            'profile_kind': context.profile.kind.value,
            'profile_version': context.profile.version,
            'profile_digest': context.profile.digest,
            'capability_cohort_epoch': context.capability_cohort_epoch,
            'capability_profile_set_digest':
                context.capability_profile_set_digest,
            'receipt_protocol_version': context.receipt_protocol_version,
        }
        if any(request_row[field] != value
               for field, value in expected_profile.items()):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Correlated request generic profile does not match its '
                'association.')
    elif request_row['binding_protocol_version'] is not None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Protocol-v1 request unexpectedly carries a generic profile.')

    generation = int(request_row['execution_generation'])
    claim_token = request_row['claim_token']
    worker_instance_id = request_row['worker_instance_id']
    lease_expires_at = request_row['lease_expires_at']
    queue_claimed = bool(queue_row is not None and
                         queue_row['delivery_state'] == 'claimed')
    queue_generation = (None if queue_row is None or
                        queue_row['claim_generation'] is None else int(
                            queue_row['claim_generation']))
    claim_exists = bool(claim_token is not None or
                        worker_instance_id is not None or
                        lease_expires_at is not None or queue_claimed)
    database_now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    status = requests_lib.RequestStatus(str(request_row['status']))
    exact_owner_shape = bool(claim_token is not None and
                             worker_instance_id is not None and
                             lease_expires_at is not None and generation > 0)
    # Before terminalization the queue delivery is part of the exact claim.
    # A terminal transition atomically deletes that delivery but deliberately
    # retains its token/worker/lease so the real owner can still acknowledge
    # quiescence from its finally block.  Both shapes identify one exact
    # generation; no other terminal request shape may use lease expiry as
    # evidence.
    exact_claim_shape = bool(
        exact_owner_shape and
        ((status in requests_lib.RequestStatus.active_statuses() and
          queue_claimed and queue_generation == generation and
          (status != requests_lib.RequestStatus.RUNNING or
           request_row['pid'] is not None)) or
         (status in requests_lib.RequestStatus.finished_status() and
          queue_row is None)))
    claim_active = bool(
        status in requests_lib.RequestStatus.active_statuses() and
        exact_claim_shape and lease_expires_at > database_now)
    claim_expired = bool(exact_claim_shape and lease_expires_at <= database_now)
    quiesced_generation = request_row['execution_quiesced_generation']
    quiesced_at = request_row['execution_quiesced_at']
    raw_cause = request_row['terminal_cause']
    terminal_cause = (None if raw_cause is None else
                      event_api_models.EventCause(str(raw_cause)))
    decoded_error = None
    error_decode_failed = False
    if request_row['error'] is not None:
        try:
            candidate_error = _request_from_mapping(
                typing.cast(sqlalchemy.engine.RowMapping,
                            request_row)).get_error()
            if not requests_lib.decoded_error_is_valid(candidate_error):
                error_decode_failed = True
            else:
                decoded_error = candidate_error
        except Exception:  # pylint: disable=broad-except
            # This is pure decoding of bytes already read under the row lock;
            # database transport failures happen before this narrow block.
            # Preserve the distinction so the reducer can durably fail closed
            # instead of retrying a corrupt terminal row forever.
            error_decode_failed = True
    facts = BoundOrdinaryLaunchRequestFacts(
        association_id=context.association_id,
        request_id=context.request_id,
        exists=True,
        status=status,
        terminal_cause=terminal_cause,
        execution_generation=generation,
        claim_token=claim_token,
        worker_instance_id=worker_instance_id,
        lease_expires_at=lease_expires_at,
        claim_exists=claim_exists,
        claim_active=claim_active,
        claim_expired=claim_expired,
        queue_exists=queue_row is not None,
        queue_delivery_state=(None if queue_row is None else str(
            queue_row['delivery_state'])),
        queue_claim_generation=queue_generation,
        execution_quiescence_required=bool(
            request_row['execution_quiescence_required']),
        execution_quiesced_generation=(None if quiesced_generation is None else
                                       int(quiesced_generation)),
        execution_quiesced_at=quiesced_at,
        quiescent=((quiesced_generation == generation and
                    quiesced_at is not None) or
                   (not request_row['execution_quiescence_required'] and
                    generation == 0 and not claim_exists)),
        retention_pin_active=pin_row is not None,
        return_value=request_row['return_value'],
        error=decoded_error,
        error_decode_failed=error_decode_failed)
    return facts, request_row, queue_row, pin_row


def read_bound_ordinary_launch_request_facts_in_transaction(
    connection: sqlalchemy.engine.Connection,
    association_id: uuid.UUID,
    request_id: str,
) -> BoundOrdinaryLaunchRequestFacts:
    """Read exact request facts under the canonical request lock suffix.

    A cross-layer caller must lock Serve lifecycle/service/pool/replica/
    association authority before calling this function.
    """
    if not isinstance(association_id, uuid.UUID):
        raise ValueError('association_id must be a UUID.')
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('request_id must be non-empty text.')
    context = ordinary_launch_binding.BoundLaunchContext(
        association_id=association_id,
        request_id=request_id,
        service_name='_request-facts-only',
        replica_id=1,
        replica_record_id=uuid.UUID(int=0),
        launch_generation=1,
        input_digest='_request-facts-only')
    facts, _, _, _ = _lock_bound_request_evidence(connection, context)
    return facts


def _bound_association_for_replica(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> Mapping[str, Any] | None:
    association = ordinary_launch_binding.get_for_replica(
        service_name, replica_id, replica_record_id)
    return typing.cast(Mapping[str, Any] | None, association)


def stable_bound_ordinary_launch_submission_id(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> str:
    """Derive a retry-stable submission UUID from durable generation state."""
    try:
        record_uuid = uuid.UUID(replica_record_id)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(
            'replica_record_id must be a canonical UUID string.') from error
    if str(record_uuid) != replica_record_id:
        raise ValueError('replica_record_id must be a canonical UUID string.')
    if isinstance(replica_id,
                  bool) or not isinstance(replica_id, int) or replica_id < 1:
        raise ValueError('replica_id must be a positive integer.')
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        latest = connection.execute(
            sqlalchemy.select(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                submission_id, ordinary_launch_binding.
                ordinary_launch_associations_table.c.launch_generation,
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                resolution, ordinary_launch_binding.
                ordinary_launch_associations_table.c.cancel_reason).where(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.service_name == service_name,
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.replica_id == replica_id,
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.replica_record_id == record_uuid).
            order_by(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                launch_generation.desc()).limit(1)).mappings().one_or_none()
    if latest is None:
        generation = 1
    else:
        resolution = ordinary_launch_binding.Resolution(
            str(latest['resolution']))
        if resolution in ordinary_launch_binding.UNSETTLED_RESOLUTIONS:
            return str(latest['submission_id'])
        if resolution != ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'A post-effect projected launch cannot admit a successor for '
                'the same replica record.')
        if latest['cancel_reason'] is not None:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'A cancelled pre-effect launch cannot admit a successor for '
                'the same replica record.')
        generation = int(latest['launch_generation']) + 1
    material = f'{service_name}\0{replica_id}\0{record_uuid}\0{generation}'
    return str(uuid.uuid5(_ORDINARY_LAUNCH_SUBMISSION_NAMESPACE, material))


def inspect_bound_ordinary_launch(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> OrdinaryLaunchReduction | None:
    """Return a non-authorizing adoption snapshot without advisory guards.

    This snapshot may become stale immediately and never grants provider or
    projection permission. Avoiding the shared advisory guard is essential:
    exact cancellation must remain addressable even if an exclusive ownership
    transfer is contending behind opaque provider work.
    """
    association = _bound_association_for_replica(service_name, replica_id,
                                                 replica_record_id)
    if association is None:
        return None
    context = _bound_context_from_association(association)
    engine = initialize_and_get_db()
    with engine.begin() as connection:
        locked = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        facts, _, _, _ = _lock_bound_request_evidence(connection, context)
    resolution = ordinary_launch_binding.Resolution(str(locked['resolution']))
    if resolution == ordinary_launch_binding.Resolution.AMBIGUOUS:
        disposition = OrdinaryLaunchReductionDisposition.AMBIGUOUS
    elif (facts.exists and
          facts.status in requests_lib.RequestStatus.active_statuses() and
          ((facts.queue_delivery_state == 'queued' and
            facts.queue_claim_generation is None and not facts.claim_exists) or
           (facts.queue_delivery_state == 'claimed' and facts.claim_active and
            facts.queue_claim_generation == facts.execution_generation))):
        disposition = OrdinaryLaunchReductionDisposition.ADOPT_ACTIVE
    elif facts.exists:
        disposition = OrdinaryLaunchReductionDisposition.WAIT_QUIESCENCE
    else:
        disposition = OrdinaryLaunchReductionDisposition.AMBIGUOUS
    service_job_id = locked['service_job_id']
    return OrdinaryLaunchReduction(
        context=_bound_context_from_association(association),
        disposition=disposition,
        request=facts,
        service_job_id=(None
                        if service_job_id is None else int(service_job_id)),
        cancel_reason=locked.get('cancel_reason'),
        projected=False)


def read_bound_reserved_fill_active_snapshot(
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> OrdinaryLaunchReduction | None:
    """Return one active reserved-fill snapshot without row/advisory locks.

    This read keeps an already-adopted active request out of the mutation
    reducer's global protocol/lifecycle lock prefix. It grants no provider,
    projection, cancellation, cleanup, or ownership authority. A concurrent
    transition may make the result stale immediately; the adopter will then
    enter the canonical locked reducer on its next poll.

    False negatives deliberately fall through to that reducer. Every immutable
    identity, current controller/lifecycle tuple, replica projection, zero-paid
    invariant, request, pin, queue, and live-claim predicate needed for a
    positive result is checked in one PostgreSQL SELECT without FOR UPDATE or
    an advisory lock.
    """
    if (not isinstance(context,
                       ordinary_launch_binding.BoundNonPoolLaunchContext) or
            context.profile.kind is not ordinary_launch_binding.
            NonPoolLaunchProfileKind.RESERVED_FILL):
        return None
    try:
        context.profile.validate()
    except ValueError:
        return None
    if (not isinstance(authority,
                       ordinary_launch_binding.ControllerBindingAuthority) or
            not authority.retained_non_pool_settlement_allowed or
            authority.service_name != context.service_name or
            authority.non_pool_capability_cohort_epoch !=
            context.capability_cohort_epoch or
            authority.non_pool_profile_set_digest !=
            context.capability_profile_set_digest or
            authority.non_pool_receipt_protocol_version !=
            context.receipt_protocol_version):
        return None
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None

    association = ordinary_launch_binding.ordinary_launch_associations_table
    service = serve_state_schema.services_table
    lifecycle = serve_state_schema.service_lifecycle_fences_table
    replica = serve_state_schema.replicas_table
    paid_claim = serve_state_schema.paid_capacity_claims_table
    profile = context.profile
    active_statuses = tuple(
        status.value for status in requests_lib.RequestStatus.active_statuses())
    executable_statuses = tuple(
        status.value
        for status in requests_lib.RequestStatus.executable_statuses())
    queued = sqlalchemy.and_(
        REQUESTS.c.status.in_(executable_statuses),
        QUEUE.c.delivery_state == 'queued',
        QUEUE.c.claim_generation.is_(None),
        REQUESTS.c.claim_token.is_(None),
        REQUESTS.c.worker_instance_id.is_(None),
        REQUESTS.c.lease_expires_at.is_(None),
        ~REQUESTS.c.execution_quiescence_required,
    )
    claimed = sqlalchemy.and_(
        QUEUE.c.delivery_state == 'claimed',
        QUEUE.c.claim_generation == REQUESTS.c.execution_generation,
        REQUESTS.c.execution_generation > 0,
        REQUESTS.c.claim_token.is_not(None),
        REQUESTS.c.worker_instance_id.is_not(None),
        REQUESTS.c.lease_expires_at.is_not(None),
        REQUESTS.c.lease_expires_at > sqlalchemy.func.clock_timestamp(),
        sqlalchemy.or_(
            REQUESTS.c.status != requests_lib.RequestStatus.RUNNING.value,
            REQUESTS.c.pid.is_not(None)),
        REQUESTS.c.execution_quiescence_required,
    )
    joined = association.join(
        service, service.c.name == association.c.service_name).join(
            lifecycle, lifecycle.c.name == association.c.service_name).join(
                replica,
                sqlalchemy.and_(
                    replica.c.service_name == association.c.service_name,
                    replica.c.replica_id == association.c.replica_id,
                    replica.c.ordinary_launch_association_id ==
                    association.c.association_id,
                )).join(
                    REQUESTS,
                    sqlalchemy.and_(
                        REQUESTS.c.request_id == association.c.request_id,
                        REQUESTS.c.ordinary_launch_association_id ==
                        association.c.association_id,
                    )).join(
                        QUEUE,
                        QUEUE.c.request_id == association.c.request_id).join(
                            REQUEST_RETENTION_PINS,
                            sqlalchemy.and_(
                                REQUEST_RETENTION_PINS.c.request_id ==
                                association.c.request_id,
                                REQUEST_RETENTION_PINS.c.pin_kind ==
                                ORDINARY_LAUNCH_RETENTION_PIN_KIND,
                                REQUEST_RETENTION_PINS.c.pin_id ==
                                association.c.association_id,
                            ))
    statement = sqlalchemy.select(
        association,
        lifecycle.c.epoch.label('_fence_epoch'),
        service.c.hash.label('_service_hash'),
        service.c.workspace.label('_service_workspace'),
        service.c.lifecycle_epoch.label('_service_lifecycle_epoch'),
        service.c.ordinary_launch_binding_mode.label('_service_binding_mode'),
        service.c.ordinary_launch_binding_epoch.label('_service_binding_epoch'),
        service.c.ordinary_launch_binding_capable.label('_service_capable'),
        service.c.controller_incarnation.label('_service_incarnation'),
        service.c.controller_owner_epoch.label('_service_owner_epoch'),
        service.c.non_pool_launch_binding_capable.label(
            '_service_non_pool_capable'),
        service.c.non_pool_launch_controller_incarnation.label(
            '_service_non_pool_incarnation'),
        service.c.non_pool_launch_binding_protocol_version.label(
            '_service_non_pool_protocol'),
        service.c.non_pool_launch_capability_profile_set_digest.label(
            '_service_non_pool_profile_set'),
        service.c.non_pool_launch_capability_cohort_epoch.label(
            '_service_non_pool_cohort'),
        service.c.non_pool_launch_receipt_protocol_version.label(
            '_service_non_pool_receipt'),
        service.c.status.label('_service_status'),
        replica.c.replica_id.label('_replica_id'),
        replica.c.replica_state_version.label('_replica_state_version'),
        replica.c.replica_state.label('_replica_state'),
        replica.c.status.label('_replica_status'),
        replica.c.version.label('_replica_version'),
        replica.c.cluster_name.label('_replica_cluster_name'),
        replica.c.paid_capacity_pool_key.label('_replica_paid_pool_key'),
        REQUESTS.c.status.label('_request_status'),
        REQUESTS.c.execution_generation.label('_request_generation'),
        REQUESTS.c.claim_token.label('_request_claim_token'),
        REQUESTS.c.worker_instance_id.label('_request_worker_instance_id'),
        REQUESTS.c.lease_expires_at.label('_request_lease_expires_at'),
        REQUESTS.c.execution_quiescence_required.label(
            '_request_quiescence_required'),
        QUEUE.c.delivery_state.label('_queue_delivery_state'),
        QUEUE.c.claim_generation.label('_queue_claim_generation'),
    ).select_from(joined).where(
        association.c.association_id == context.association_id,
        association.c.request_id == context.request_id,
        association.c.service_name == context.service_name,
        association.c.replica_id == context.replica_id,
        association.c.replica_record_id == context.replica_record_id,
        association.c.launch_generation == context.launch_generation,
        association.c.input_digest == context.input_digest,
        association.c.resolution ==
        ordinary_launch_binding.Resolution.BOUND.value,
        association.c.cancel_reason.is_(None),
        association.c.cancel_requested_at.is_(None),
        association.c.reconciliation_outcome ==
        ordinary_launch_binding.ReconciliationOutcome.ACTIVE_ADOPT.value,
        association.c.provider_evidence ==
        ordinary_launch_binding.ProviderEvidence.NOT_QUERIED.value,
        association.c.paid_capacity_pool_key.is_(None),
        association.c.binding_protocol_version ==
        ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        association.c.profile_kind == context.profile.kind.value,
        association.c.profile_version == profile.version,
        association.c.profile_digest == profile.digest,
        association.c.capability_cohort_epoch ==
        context.capability_cohort_epoch,
        association.c.capability_profile_set_digest ==
        context.capability_profile_set_digest,
        association.c.receipt_protocol_version ==
        context.receipt_protocol_version,
        association.c.authorization_kind == profile.authorization_kind.value,
        association.c.authorization_reference ==
        profile.authorization_reference,
        association.c.authorization_generation ==
        profile.authorization_generation,
        association.c.authorization_digest == profile.authorization_digest,
        replica.c.paid_capacity_pool_key.is_(None),
        service.c.owner_user_id == association.c.tenant_scope,
        service.c.reserved_fill_actuation_mode ==
        zero_cost_actuation.ActuationMode.DURABLE_INTENT.value,
        service.c.reserved_fill_actuation_capable.is_(True),
        service.c.reserved_fill_actuation_controller_incarnation ==
        association.c.owner_controller_incarnation,
        service.c.reserved_fill_actuation_protocol_version ==
        zero_cost_actuation.PROTOCOL_VERSION,
        REQUESTS.c.handler_name ==
        non_pool_launch_request.NON_POOL_LAUNCH_HANDLER_NAME,
        REQUESTS.c.user_id == association.c.tenant_scope,
        REQUESTS.c.cluster_name == association.c.cluster_name,
        REQUESTS.c.binding_protocol_version ==
        ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION,
        REQUESTS.c.profile_kind == context.profile.kind.value,
        REQUESTS.c.profile_version == profile.version,
        REQUESTS.c.profile_digest == profile.digest,
        REQUESTS.c.capability_cohort_epoch == context.capability_cohort_epoch,
        REQUESTS.c.capability_profile_set_digest ==
        context.capability_profile_set_digest,
        REQUESTS.c.receipt_protocol_version == context.receipt_protocol_version,
        REQUESTS.c.status.in_(active_statuses),
        REQUESTS.c.terminal_cause.is_(None),
        REQUESTS.c.return_value.is_(None),
        REQUESTS.c.error.is_(None),
        REQUESTS.c.finished_at.is_(None),
        REQUESTS.c.cancel_requested_at.is_(None),
        REQUESTS.c.cancel_acknowledged_at.is_(None),
        REQUESTS.c.execution_quiesced_generation.is_(None),
        REQUESTS.c.execution_quiesced_at.is_(None),
        sqlalchemy.or_(queued, claimed),
        ~sqlalchemy.exists(
            sqlalchemy.select(paid_claim.c.replica_id).where(
                paid_claim.c.service_name == association.c.service_name,
                paid_claim.c.replica_id == association.c.replica_id)),
    ).limit(1)
    with engine.connect() as connection:
        row = connection.execute(statement).mappings().one_or_none()
    if row is None or not _controller_authority_matches_reduction(
            row, authority):
        return None

    lifecycle_snapshot = {'epoch': row['_fence_epoch']}
    service_snapshot = {
        'hash': row['_service_hash'],
        'workspace': row['_service_workspace'],
        'lifecycle_epoch': row['_service_lifecycle_epoch'],
        'ordinary_launch_binding_mode': row['_service_binding_mode'],
        'ordinary_launch_binding_epoch': row['_service_binding_epoch'],
        'ordinary_launch_binding_capable': row['_service_capable'],
        'controller_incarnation': row['_service_incarnation'],
        'controller_owner_epoch': row['_service_owner_epoch'],
        'non_pool_launch_binding_capable': row['_service_non_pool_capable'],
        'non_pool_launch_controller_incarnation':
            row['_service_non_pool_incarnation'],
        'non_pool_launch_binding_protocol_version':
            row['_service_non_pool_protocol'],
        'non_pool_launch_capability_profile_set_digest':
            row['_service_non_pool_profile_set'],
        'non_pool_launch_capability_cohort_epoch':
            row['_service_non_pool_cohort'],
        'non_pool_launch_receipt_protocol_version':
            row['_service_non_pool_receipt'],
        'status': row['_service_status'],
    }
    replica_snapshot = {
        'replica_id': row['_replica_id'],
        'replica_state_version': row['_replica_state_version'],
        'replica_state': row['_replica_state'],
        'status': row['_replica_status'],
        'version': row['_replica_version'],
        'cluster_name': row['_replica_cluster_name'],
        'paid_capacity_pool_key': row['_replica_paid_pool_key'],
        'ordinary_launch_association_id': context.association_id,
    }
    if not ordinary_launch_binding.retained_reduction_snapshot_matches(
            lifecycle_snapshot, service_snapshot, replica_snapshot, row,
            context):
        return None
    try:
        service_status = serve_statuses.ServiceStatus[str(
            row['_service_status'])]
    except (KeyError, TypeError):
        return None
    if (row['_replica_status'] not in ('PENDING', 'PROVISIONING') or
            service_status
            in serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        return None

    try:
        request_status = requests_lib.RequestStatus(str(row['_request_status']))
        generation = int(row['_request_generation'])
    except (TypeError, ValueError):
        return None
    queue_state = str(row['_queue_delivery_state'])
    claimed_request = queue_state == 'claimed'
    queue_generation = row['_queue_claim_generation']
    facts = BoundOrdinaryLaunchRequestFacts(
        association_id=context.association_id,
        request_id=context.request_id,
        exists=True,
        status=request_status,
        terminal_cause=None,
        execution_generation=generation,
        claim_token=row['_request_claim_token'],
        worker_instance_id=row['_request_worker_instance_id'],
        lease_expires_at=row['_request_lease_expires_at'],
        claim_exists=claimed_request,
        claim_active=claimed_request,
        claim_expired=False,
        queue_exists=True,
        queue_delivery_state=queue_state,
        queue_claim_generation=(None if queue_generation is None else
                                int(queue_generation)),
        execution_quiescence_required=bool(row['_request_quiescence_required']),
        execution_quiesced_generation=None,
        execution_quiesced_at=None,
        quiescent=not claimed_request and generation == 0,
        retention_pin_active=True,
        return_value=None,
        error=None,
        error_decode_failed=False)
    return _reduction_result(OrdinaryLaunchReductionDisposition.ADOPT_ACTIVE,
                             facts, row)


def lookup_bound_ordinary_launch_cancel_target(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> BoundOrdinaryLaunchCancelTarget | None:
    """Resolve an exact cancellation address without advisory authority.

    This snapshot grants no mutation permission. It exists so teardown can
    address the direct canonical-row-lock cancel transaction even when opaque
    provider work owns shared authority. That transaction re-locks and
    revalidates every lifecycle/service/replica/association predicate.
    """
    association = _bound_association_for_replica(service_name, replica_id,
                                                 replica_record_id)
    if association is None:
        return None
    cancel_reason = association.get('cancel_reason')
    if cancel_reason is not None and (not isinstance(cancel_reason, str) or
                                      not cancel_reason):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Bound cancellation target has a malformed durable reason.')
    return BoundOrdinaryLaunchCancelTarget(
        context=_bound_context_from_association(association),
        cancel_reason=cancel_reason)


def _terminal_evidence(
    facts: BoundOrdinaryLaunchRequestFacts,
) -> ordinary_launch_binding_lib.TerminalEvidence:
    if (facts.status not in requests_lib.RequestStatus.finished_status() or
            facts.terminal_cause is None or
            facts.execution_generation is None or not facts.quiescent):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Bound terminal evidence is incomplete or not quiescent.')
    return ordinary_launch_binding.TerminalEvidence(
        status=ordinary_launch_binding.TerminalStatus(facts.status.value),
        cause=facts.terminal_cause.value,
        execution_generation=facts.execution_generation,
        quiescence_required=True,
        quiesced_generation=facts.execution_quiesced_generation,
        quiesced_at=facts.execution_quiesced_at)


def _request_service_job_id(request_row: Mapping[str, Any],
                            expected_cluster_name: str) -> int | None:
    if request_row['status'] != requests_lib.RequestStatus.SUCCEEDED.value:
        return None
    try:
        request = _request_from_mapping(
            typing.cast(sqlalchemy.engine.RowMapping, request_row))
        result = request.get_return_value()
    except Exception as e:  # pylint: disable=broad-except
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Succeeded bound launch has a malformed or mismatched exact '
            'service-job result.') from e
    if (not isinstance(result, tuple) or len(result) != 2 or
            isinstance(result[0], bool) or not isinstance(result[0], int) or
            result[0] < 1 or
            not isinstance(result[1], backends.CloudVmRayResourceHandle) or
            result[1].cluster_name != expected_cluster_name):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Succeeded bound launch has a malformed or mismatched exact '
            'service-job result.')
    return result[0]


def _reduction_result(
    disposition: OrdinaryLaunchReductionDisposition,
    facts: BoundOrdinaryLaunchRequestFacts,
    association: Mapping[str, Any],
    *,
    projected: bool = False,
) -> OrdinaryLaunchReduction:
    service_job_id = association['service_job_id']
    return OrdinaryLaunchReduction(
        context=_bound_context_from_association(association),
        disposition=disposition,
        request=facts,
        service_job_id=(None
                        if service_job_id is None else int(service_job_id)),
        cancel_reason=association.get('cancel_reason'),
        projected=projected)


def _controller_authority_matches_reduction(
    association: Mapping[str, Any],
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> bool:
    base_matches = bool(
        isinstance(authority,
                   ordinary_launch_binding.ControllerBindingAuthority) and
        authority.capable is True and
        authority.binding_mode == ordinary_launch_binding.BindingMode.BOUND and
        authority.service_name == association['service_name'] and
        authority.service_hash == association['service_hash'] and
        authority.service_workspace == association['service_workspace'] and
        authority.service_lifecycle_epoch
        == association['service_lifecycle_epoch'] and
        authority.binding_epoch == association['service_binding_epoch'] and
        authority.controller_incarnation
        == association['owner_controller_incarnation'] and
        authority.controller_owner_epoch
        == association['owner_controller_epoch'])
    if not base_matches:
        return False
    if association.get('binding_protocol_version') is None:
        return True
    return bool(authority.retained_non_pool_settlement_allowed and
                association['binding_protocol_version']
                == authority.non_pool_binding_protocol_version and
                association['capability_cohort_epoch']
                == authority.non_pool_capability_cohort_epoch and
                association['capability_profile_set_digest']
                == authority.non_pool_profile_set_digest and
                association['receipt_protocol_version']
                == authority.non_pool_receipt_protocol_version)


def _paid_capacity_claim_is_exact(
    connection: sqlalchemy.engine.Connection,
    association: Mapping[str, Any],
) -> bool:
    claims = serve_state_schema.paid_capacity_claims_table
    rows = connection.execute(
        sqlalchemy.select(claims.c.service_hash, claims.c.pool_key).where(
            claims.c.service_name == association['service_name'],
            claims.c.replica_id == association['replica_id'])).all()
    pool_key = association['paid_capacity_pool_key']
    if pool_key is None:
        return not rows
    pools = serve_state_schema.paid_capacity_pools_table
    pool_exists = connection.execute(
        sqlalchemy.select(sqlalchemy.literal(True)).where(
            sqlalchemy.exists().where(
                pools.c.pool_key == pool_key))).scalar_one_or_none()
    replica_pool_key = connection.execute(
        sqlalchemy.select(
            serve_state_schema.replicas_table.c.paid_capacity_pool_key).where(
                serve_state_schema.replicas_table.c.service_name ==
                association['service_name'],
                serve_state_schema.replicas_table.c.replica_id ==
                association['replica_id'])).scalar_one_or_none()
    return bool(pool_exists and replica_pool_key == pool_key and
                len(rows) == 1 and rows[0][0] == association['service_hash'] and
                rows[0][1] == pool_key)


def _paid_capacity_claim_is_released(
    connection: sqlalchemy.engine.Connection,
    association: Mapping[str, Any],
) -> bool:
    """Require an ordinary-paid historical identity with no live debit."""
    pool_key = association['paid_capacity_pool_key']
    if not isinstance(pool_key, str) or not pool_key:
        return False
    claims = serve_state_schema.paid_capacity_claims_table
    claim_count = connection.execute(
        sqlalchemy.select(sqlalchemy.func.count()).select_from(claims).where(
            claims.c.service_name == association['service_name'],
            claims.c.replica_id == association['replica_id'])).scalar_one()
    replica_pool_key = connection.execute(
        sqlalchemy.select(
            serve_state_schema.replicas_table.c.paid_capacity_pool_key).where(
                serve_state_schema.replicas_table.c.service_name ==
                association['service_name'],
                serve_state_schema.replicas_table.c.replica_id ==
                association['replica_id'])).scalar_one_or_none()
    return int(claim_count) == 0 and replica_pool_key == pool_key


def _mark_reduction_ambiguous(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    association: Mapping[str, Any],
    facts: BoundOrdinaryLaunchRequestFacts,
    code: str,
) -> OrdinaryLaunchReduction:
    if association[
            'resolution'] != ordinary_launch_binding.Resolution.AMBIGUOUS.value:
        ordinary_launch_binding.mark_ambiguous_in_connection(
            connection, context, code)
    return _reduction_result(OrdinaryLaunchReductionDisposition.AMBIGUOUS,
                             facts, association)


def _startup_request_facts(
    facts: BoundOrdinaryLaunchRequestFacts,
) -> ordinary_launch_binding_lib.RequestStartupFacts:
    return ordinary_launch_binding.RequestStartupFacts(
        exists=facts.exists,
        status=(None if facts.status is None else facts.status.value),
        queue_exists=facts.queue_exists,
        execution_generation=facts.execution_generation,
        claim_exists=facts.claim_exists,
        quiescent=facts.quiescent)


def _settle_projected_paid_capacity_claim_in_transaction(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    association: Mapping[str, Any],
    *,
    pre_effect: bool,
) -> bool:
    """Release the exact claim when one action reaches a settled result."""
    if (pre_effect and association['cancel_reason'] is None and not isinstance(
            context, ordinary_launch_binding.BoundNonPoolLaunchContext)):
        # Protocol-v1 retains its historical same-record generation+1 retry
        # until the stacked cleanup removes that transition path.
        return True
    return (ordinary_launch_binding.
            release_projected_paid_capacity_claim_in_connection(
                connection, context))


def reduce_bound_ordinary_launch_in_transaction(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    *,
    project_replica_result: BoundOrdinaryLaunchReplicaProjector,
) -> OrdinaryLaunchReduction:
    """Reduce one exact bound request under canonical cross-layer locks."""
    # The terminal result may be the first provider-visible success of a
    # zero-cost replica. Acquire the global event sequencer before the
    # lifecycle/service/replica/association authority below; the projector
    # reuses this transaction-owned lock only when a stamp is required.
    serve_state.lock_zero_cost_protocol_for_bound_launch_projection(connection)
    association = ordinary_launch_binding.lock_reduction_authority_in_connection(
        connection, context)
    facts, request_row, queue_row, _ = _lock_bound_request_evidence(
        connection, context)
    if not _controller_authority_matches_reduction(association, authority):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Controller reduction authority is stale or belongs to another '
            'service incarnation.')
    if association[
            'resolution'] == ordinary_launch_binding.Resolution.AMBIGUOUS.value:
        return _reduction_result(OrdinaryLaunchReductionDisposition.AMBIGUOUS,
                                 facts, association)
    if not _paid_capacity_claim_is_exact(connection, association):
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts, 'paid-capacity-claim-mismatch')
    if not facts.exists:
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts, 'correlated-request-missing')
    if facts.error_decode_failed:
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts, 'request-error-malformed')
    if not facts.retention_pin_active:
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts, 'request-retention-pin-missing')

    exact_expired_claim = bool(
        facts.execution_generation is not None and
        facts.execution_generation > 0 and facts.claim_token is not None and
        facts.worker_instance_id is not None and
        facts.lease_expires_at is not None and facts.claim_expired and
        ((facts.status in requests_lib.RequestStatus.active_statuses() and
          facts.queue_delivery_state == 'claimed' and
          facts.queue_claim_generation == facts.execution_generation) or
         (facts.status in requests_lib.RequestStatus.finished_status() and
          not facts.queue_exists and not facts.quiescent)))
    if exact_expired_claim:
        if association['effect_phase'] != (
                ordinary_launch_binding.EffectPhase.NOT_STARTED.value):
            if facts.status in requests_lib.RequestStatus.active_statuses():
                assert request_row is not None
                now = sqlalchemy.func.clock_timestamp()
                transitioned = _terminalize_locked_request(
                    connection,
                    request_row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=(event_api_models.EventCause.EXECUTION_LEASE_EXPIRED),
                    values={
                        # Provider I/O may still be blocked while holding
                        # cluster locks. Revoke it durably, retain exact owner
                        # identity for marker-gated interruption, and wait for
                        # the wrapper's real quiescence receipt.
                        'cancel_requested_at': now,
                        'execution_quiescence_required': True,
                        'should_retry': False,
                        'finished_at': now,
                        'interrupted_reason':
                            ('Execution lease expired after the bound ordinary '
                             'launch crossed its provider-effect boundary.'),
                    })
                if not transitioned:
                    raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                        'Expired effectful bound request terminalization lost '
                        'its exact row.')
                facts, request_row, queue_row, _ = (
                    _lock_bound_request_evidence(connection, context))
            return _mark_reduction_ambiguous(
                connection, context, association, facts,
                'expired-claim-after-provider-effect')
        assert request_row is not None
        # The distinct bound handler is excluded from the generic queue
        # reaper. At NOT_STARTED the expired exact owner cannot acquire the
        # association/provider fence, so this reducer is the sole authority
        # that may publish its generation-bound quiescence proof. Retain the
        # owner identity so a late genuine acknowledgement remains idempotent.
        now = sqlalchemy.func.clock_timestamp()
        if facts.status in requests_lib.RequestStatus.active_statuses():
            terminal_values: dict[str, Any] = {
                'should_retry': True,
                'finished_at': now,
                'execution_quiescence_required': True,
                'interrupted_reason':
                    ('Execution lease expired before the bound ordinary '
                     'launch crossed its provider-effect boundary.'),
            }
            if not facts.quiescent:
                terminal_values.update({
                    'execution_quiesced_generation': facts.execution_generation,
                    'execution_quiesced_at': now,
                })
            transitioned = _terminalize_locked_request(
                connection,
                request_row,
                status=requests_lib.RequestStatus.CANCELLED,
                cause=event_api_models.EventCause.EXECUTION_LEASE_EXPIRED,
                values=terminal_values)
            if not transitioned:
                raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                    'Expired bound request terminalization lost its exact '
                    'row.')
        else:
            # Explicit cancellation (or normal executor terminalization) has
            # already removed the queue delivery. Complete only the missing
            # proof while matching the retained exact owner generation.
            completed = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == context.request_id,
                    REQUESTS.c.execution_generation ==
                    facts.execution_generation,
                    REQUESTS.c.claim_token == facts.claim_token,
                    REQUESTS.c.worker_instance_id == facts.worker_instance_id,
                    REQUESTS.c.lease_expires_at <=
                    sqlalchemy.func.clock_timestamp(),
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.finished_status()
                    ]), REQUESTS.c.execution_quiesced_generation.is_(None),
                    REQUESTS.c.execution_quiesced_at.is_(None)).
                values(execution_quiescence_required=True,
                       execution_quiesced_generation=facts.execution_generation,
                       execution_quiesced_at=now,
                       updated_at=now))
            if completed.rowcount != 1:
                raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                    'Expired terminal bound request lost its exact owner '
                    'evidence.')
        facts, request_row, queue_row, _ = _lock_bound_request_evidence(
            connection, context)

    classification = ordinary_launch_binding.classify_startup(
        association, _startup_request_facts(facts))
    if classification == ordinary_launch_binding.StartupClassification.ADOPT_ACTIVE:
        if (facts.queue_delivery_state not in ('queued', 'claimed') or
            (facts.queue_delivery_state == 'queued' and
             facts.queue_claim_generation is not None) or
            (facts.queue_delivery_state == 'claimed' and
             (facts.queue_claim_generation != facts.execution_generation or
              not facts.claim_active))):
            return _mark_reduction_ambiguous(connection, context, association,
                                             facts,
                                             'request-delivery-claim-mismatch')
        return _reduction_result(
            OrdinaryLaunchReductionDisposition.ADOPT_ACTIVE, facts, association)
    if classification == ordinary_launch_binding.StartupClassification.WAIT_QUIESCENCE:
        if facts.terminal_cause is None:
            return _mark_reduction_ambiguous(connection, context, association,
                                             facts,
                                             'request-terminal-cause-missing')
        return _reduction_result(
            OrdinaryLaunchReductionDisposition.WAIT_QUIESCENCE, facts,
            association)
    if classification == ordinary_launch_binding.StartupClassification.AMBIGUOUS:
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts,
                                         'request-startup-state-ambiguous')
    if classification == ordinary_launch_binding.StartupClassification.SETTLED:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'A settled association cannot retain its replica pointer.')

    if (classification == ordinary_launch_binding.StartupClassification.
            PRE_EFFECT_TERMINALIZE and
            facts.status in requests_lib.RequestStatus.active_statuses()):
        assert request_row is not None
        now = sqlalchemy.func.clock_timestamp()
        transitioned = _terminalize_locked_request(
            connection,
            request_row,
            status=requests_lib.RequestStatus.CANCELLED,
            cause=event_api_models.EventCause.DISPATCHER_SUBMIT_FAILED,
            values={
                'cancel_requested_at': now,
                'execution_quiescence_required': True,
                'execution_quiesced_generation': 0,
                'execution_quiesced_at': now,
                'finished_at': now,
                'interrupted_reason':
                    ('Bound ordinary launch lost its generation-zero queue '
                     'before any provider effect.'),
            })
        if not transitioned:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Pre-effect request terminalization lost its exact row.')
        facts, request_row, queue_row, _ = _lock_bound_request_evidence(
            connection, context)
    if (facts.status not in requests_lib.RequestStatus.finished_status() or
            queue_row is not None):
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts,
                                         'request-terminal-delivery-mismatch')
    if facts.terminal_cause is None:
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts,
                                         'request-terminal-cause-missing')
    if not facts.quiescent:
        return _reduction_result(
            OrdinaryLaunchReductionDisposition.WAIT_QUIESCENCE, facts,
            association)
    assert request_row is not None
    try:
        request_service_job_id = _request_service_job_id(
            request_row, str(association['cluster_name']))
    except ordinary_launch_binding.OrdinaryLaunchBindingConflict:
        return _mark_reduction_ambiguous(
            connection, context, association, facts,
            'service-job-result-malformed-or-mismatched')
    recorded_job_id = association['service_job_id']
    if (request_service_job_id is not None and
            request_service_job_id != recorded_job_id):
        return _mark_reduction_ambiguous(connection, context, association,
                                         facts, 'service-job-result-mismatch')

    reduced = ordinary_launch_binding.record_terminal_in_connection(
        connection, context, _terminal_evidence(facts))
    if reduced == ordinary_launch_binding.StartupClassification.AMBIGUOUS:
        return _reduction_result(OrdinaryLaunchReductionDisposition.AMBIGUOUS,
                                 facts, association)
    if reduced == ordinary_launch_binding.StartupClassification.REDUCE_TERMINAL:
        if recorded_job_id is None:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Recorded service-job phase has no exact job ID.')
        service_job_id = int(recorded_job_id)
        pre_effect = False
        disposition = OrdinaryLaunchReductionDisposition.PROJECTED
    elif reduced == ordinary_launch_binding.StartupClassification.PRE_EFFECT_TERMINALIZE:
        service_job_id = None
        pre_effect = True
        disposition = OrdinaryLaunchReductionDisposition.PRE_EFFECT_TERMINAL
    else:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            f'Unexpected terminal reduction {reduced.value}.')

    locked_replica_info = (
        serve_state.read_replica_for_bound_ordinary_launch_in_transaction(
            connection, context.service_name, context.replica_id,
            str(context.replica_record_id), context.association_id))
    projection = BoundOrdinaryLaunchProjectionInput(
        context=context,
        request=facts,
        locked_replica_info=locked_replica_info,
        status=facts.status,
        cause=facts.terminal_cause,
        service_job_id=service_job_id,
        pre_effect_terminal=pre_effect,
        cancel_reason=association.get('cancel_reason'),
        paid_capacity_pool_key=association['paid_capacity_pool_key'])
    if project_replica_result(connection, projection) is not True:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Replica result persistence lost its exact row identity.')
    # The projection callback may update paid-pool feedback, but claim release
    # belongs to the reducer and must remain pending through final Serve
    # revalidation.
    if not _paid_capacity_claim_is_exact(connection, association):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Replica projection changed paid-capacity claim identity.')
    projected = ordinary_launch_binding.project_from_request(
        connection,
        context,
        pre_effect_terminal=pre_effect,
        service_job_id=service_job_id,
        release_pin=lambda conn, request_id, association_id:
        (delete_request_retention_pin_in_transaction(
            conn, request_id, ORDINARY_LAUNCH_RETENTION_PIN_KIND, association_id
        )))
    if not projected:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Exact ordinary launch result was not projectable.')
    # One planner intent owns one action. Every settled result releases its
    # exact claim; PRE_EFFECT terminal rows are retired and replanned instead
    # of retaining authority for a same-record generation+1 path.
    if not _settle_projected_paid_capacity_claim_in_transaction(
            connection, context, association, pre_effect=pre_effect):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Exact projected paid-capacity claim was not releasable.')
    return OrdinaryLaunchReduction(
        context=context,
        disposition=disposition,
        request=facts,
        service_job_id=service_job_id,
        cancel_reason=association.get('cancel_reason'),
        projected=True)


def reduce_bound_ordinary_launch(
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    *,
    project_replica_result: BoundOrdinaryLaunchReplicaProjector,
) -> OrdinaryLaunchReduction:
    """Own and commit one exact reducer transaction."""
    # Canonical lifecycle/service/replica/association/request/queue/pin row
    # locks are the reducer authority. Do not queue for the exclusive advisory
    # guard here: an expired provider owner may still hold its shared session,
    # while this transaction only (a) settles NOT_STARTED ownership that can no
    # longer pass the live-lease provider fence, (b) records post-effect
    # ambiguity, or (c) projects an exact terminal quiescence receipt produced
    # after the handler left its provider guard. Owner transfer takes the same
    # row locks and is revalidated below.
    engine = initialize_and_get_db()
    with engine.begin() as connection:
        return reduce_bound_ordinary_launch_in_transaction(
            connection,
            context,
            authority,
            project_replica_result=project_replica_result)


def _ordinary_paid_provider_absence_payload_from_locked_request(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    association: Mapping[str, Any],
    facts: BoundOrdinaryLaunchRequestFacts,
    request_row: sqlalchemy.engine.RowMapping | None,
    queue_row: sqlalchemy.engine.RowMapping | None,
    *,
    expected_reconciliation_outcome: str,
    require_paid_claim: bool = True,
    require_retention_pin: bool = True,
) -> dict[str, Any] | None:
    """Re-extract one exact zero-effect AWS receipt under request locks."""
    if (context.profile.kind !=
            ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID or
            request_row is None or association['effect_phase'] !=
            ordinary_launch_binding.EffectPhase.PROVIDER_IO.value or
            association['reconciliation_outcome'] !=
            expected_reconciliation_outcome or
            not isinstance(association['paid_capacity_pool_key'], str) or
            not association['paid_capacity_pool_key'] or
            association['service_job_id'] is not None or
            facts.status is not requests_lib.RequestStatus.FAILED or
            facts.terminal_cause
            is not event_api_models.EventCause.HANDLER_FAILED or
            facts.execution_generation is None or
            facts.execution_generation < 1 or
            facts.execution_quiescence_required is not True or
            facts.execution_quiesced_generation != facts.execution_generation or
            facts.execution_quiesced_at is None or not facts.quiescent or
            queue_row is not None or
            facts.retention_pin_active is not require_retention_pin or
            facts.return_value is not None or facts.error_decode_failed or
            not requests_lib.decoded_error_is_valid(facts.error)):
        return None
    claim_is_exact = (_paid_capacity_claim_is_exact(connection, association) if
                      require_paid_claim else _paid_capacity_claim_is_released(
                          connection, association))
    if not claim_is_exact:
        return None
    try:
        request = _request_from_mapping(
            typing.cast(sqlalchemy.engine.RowMapping, request_row))
        parsed_context = ordinary_launch_binding.parse_bound_non_pool_launch_context(
            request.request_body.extra_launch_context)
        if (parsed_context != context or
                not _provider_present_cleanup_input_digest_matches(
                    connection, association, request_row, request, context) or
                _request_service_job_id(
                    request_row, str(association['cluster_name'])) is not None):
            return None
    except Exception:  # pylint: disable=broad-except
        return None
    error_object = facts.error['object']
    receipt = capacity_policy.extract_provider_negative_ack(error_object)
    try:
        expected_cluster_name_on_cloud = (
            ordinary_launch_binding.ordinary_paid_cluster_name_on_cloud(
                association))
        expected_client_token = (
            ordinary_launch_binding.ordinary_paid_aws_client_token(context))
        expected_aws_account_id = (
            ordinary_launch_binding.ordinary_paid_aws_account_id_from_pool_key(
                association['paid_capacity_pool_key']))
    except (TypeError, ValueError,
            ordinary_launch_binding.OrdinaryLaunchBindingConflict):
        return None
    canonical_receipt = capacity_policy.validate_provider_negative_ack(
        receipt,
        cluster_name=expected_cluster_name_on_cloud,
        client_token=expected_client_token,
        expected_aws_account_id=expected_aws_account_id)
    if canonical_receipt is None or receipt != canonical_receipt:
        return None
    pool_identity = paid_capacity.pool_key_payload(
        str(association['paid_capacity_pool_key']))
    if (pool_identity is None or pool_identity['cloud'] != 'aws' or
            pool_identity['use_spot'] is not True or
            pool_identity['region'] != canonical_receipt['region'] or
            pool_identity['zone'] != canonical_receipt['availability_zone'] or
            pool_identity['instance_type'] != canonical_receipt['instance_type']
            or
            pool_identity['num_nodes'] != canonical_receipt['requested_count']):
        return None
    return {
        'association_id': str(context.association_id),
        'cluster_name': str(association['cluster_name']),
        'probe_contract': 'aws-run-instances-negative-ack-v1',
        'profile_kind': ordinary_launch_binding.NonPoolLaunchProfileKind.
                        ORDINARY_PAID.value,
        'receipt': canonical_receipt,
        'replica_record_id': str(context.replica_record_id),
        'result': ordinary_launch_binding.ProviderEvidence.ABSENT.value,
    }


def _gcp_launch_task_supports_plain_compute_disk_reconciliation(
        task_yaml: str) -> bool:
    """Whether a GCP task has a supported plain-compute disk identity.

    Newly initialized data volumes may set an arbitrary ``diskName``.  Older
    executors stamped only the generic managed marker on those disks, so no
    provider read can attribute an orphan to one launch after its VM and
    cluster row disappear.  A task with any volume therefore remains UNKNOWN.
    The normal boot disk has no custom name and inherits the generated VM name.
    A task-level MIG override is also unsupported because its provider object
    is not represented by the exact VM-label observer.
    """
    if not isinstance(task_yaml, str) or not task_yaml:
        return False
    try:
        configs = yaml_utils.read_yaml_all_str(task_yaml,
                                               reject_duplicate_keys=True)
    except Exception:  # pylint: disable=broad-except
        return False
    saw_task = False
    for config in configs:
        if not isinstance(config, Mapping):
            return False
        # ``dump_chain_dag_to_yaml_str`` emits one optional name-only header.
        if set(config) == {'name'}:
            continue
        saw_task = True
        if config.get('volumes') not in (None, {}, []):
            return False
        resources = config.get('resources')
        if resources is None:
            continue

        def _has_unsupported_provider_identity(value: Any) -> bool:
            if isinstance(value, Mapping):
                for key, child in value.items():
                    if key == 'volumes' and child not in (None, {}, []):
                        return True
                    if key == 'diskName' and child is not None:
                        return True
                    if (key == 'managed_instance_group' and child is not None):
                        return True
                    if _has_unsupported_provider_identity(child):
                        return True
            elif isinstance(value, list):
                return any(
                    _has_unsupported_provider_identity(item) for item in value)
            return False

        if _has_unsupported_provider_identity(resources):
            return False
    return saw_task


def _gcp_paid_pool_is_plain_compute(pool_identity: Mapping[str, Any]) -> bool:
    """Reject TPU-VM pools from the compute-instance evidence contract."""
    accelerators = pool_identity.get('accelerators')
    if not isinstance(accelerators, list):
        return False
    for accelerator in accelerators:
        if (not isinstance(accelerator, list) or len(accelerator) != 2 or
                not isinstance(accelerator[0], str) or
                'tpu' in accelerator[0].casefold()):
            return False
    instance_type = pool_identity.get('instance_type')
    return (isinstance(instance_type, str) and bool(instance_type) and
            'tpu' not in instance_type.casefold())


def _ordinary_paid_gcp_provider_identity_from_locked_request(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    association: Mapping[str, Any],
    facts: BoundOrdinaryLaunchRequestFacts,
    request_row: sqlalchemy.engine.RowMapping | None,
    queue_row: sqlalchemy.engine.RowMapping | None,
    *,
    expected_reconciliation_outcome: str,
    require_paid_claim: bool = True,
    require_retention_pin: bool = True,
) -> dict[str, Any] | None:
    """Recover exact GCP scope from the immutable retained launch request."""
    pool_identity = paid_capacity.pool_key_payload(
        str(association.get('paid_capacity_pool_key')))
    gcp_cohort_floor = (ordinary_launch_binding.
                        ORDINARY_PAID_GCP_OPERATION_EVIDENCE_COHORT_FLOOR)
    if (not ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) or
        (context.capability_cohort_epoch != gcp_cohort_floor - 1 and
         not (gcp_cohort_floor <= context.capability_cohort_epoch <=
              ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH)) or
            not isinstance(pool_identity, Mapping) or
            pool_identity.get('cloud') != 'gcp' or request_row is None or
            association['effect_phase'] !=
            ordinary_launch_binding.EffectPhase.PROVIDER_IO.value or
            association['reconciliation_outcome'] !=
            expected_reconciliation_outcome or
            association['service_job_id'] is not None or
            not ordinary_launch_binding.
            ordinary_paid_provider_terminal_shape_matches(
                facts.status, facts.terminal_cause,
                association.get('paid_capacity_pool_key')) or
            facts.execution_generation is None or
            facts.execution_generation < 1 or
            facts.execution_quiescence_required is not True or
            facts.execution_quiesced_generation != facts.execution_generation or
            facts.execution_quiesced_at is None or not facts.quiescent or
            queue_row is not None or
            facts.retention_pin_active is not require_retention_pin or
            facts.return_value is not None or facts.error_decode_failed):
        return None
    claim_is_exact = (_paid_capacity_claim_is_exact(connection, association) if
                      require_paid_claim else _paid_capacity_claim_is_released(
                          connection, association))
    if not claim_is_exact:
        return None
    try:
        request = _request_from_mapping(
            typing.cast(sqlalchemy.engine.RowMapping, request_row))
        parsed_context = (
            ordinary_launch_binding.parse_bound_non_pool_launch_context(
                request.request_body.extra_launch_context))
        request_identity_matches = _ordinary_paid_request_identity_matches(
            connection, association, request_row, request, context)
        if (parsed_context != context or
                not request_identity_matches or _request_service_job_id(
                    request_row, str(association['cluster_name'])) is not None):
            return None
        if (not _gcp_paid_pool_is_plain_compute(pool_identity) or
                not _gcp_launch_task_supports_plain_compute_disk_reconciliation(
                    request.request_body.task)):
            return None
        config_snapshot = request.request_body.override_skypilot_config
        if not isinstance(config_snapshot, Mapping):
            return None
        workspace = association['service_workspace']
        if (workspace != pool_identity.get('workspace') or
                config_snapshot.get('active_workspace') != workspace):
            return None
        managed_instance_group = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'gcp', ('managed_instance_group',),
                region=pool_identity['region'],
                workspace=workspace))
        if managed_instance_group is not None:
            return None
        project_id = (
            skypilot_config.get_effective_workspace_region_config_from_snapshot(
                config_snapshot,
                'gcp', ('project_id',),
                region=pool_identity['region'],
                workspace=workspace))
        return ordinary_launch_binding.ordinary_paid_gcp_provider_identity(
            association, project_id=project_id)
    except Exception:  # pylint: disable=broad-except
        return None


def _ordinary_paid_gcp_absence_settle_horizon_elapsed(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    facts: BoundOrdinaryLaunchRequestFacts,
    provider_identity: Mapping[str, Any],
    completed_create_targets: object,
) -> bool:
    """Validate current operation retention or conservative cohort-11 proof."""
    floor = (ordinary_launch_binding.
             ORDINARY_PAID_GCP_OPERATION_EVIDENCE_COHORT_FLOOR)
    if (context.capability_cohort_epoch != floor - 1 and
            not (floor <= context.capability_cohort_epoch <=
                 ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH)):
        return False
    completed = completed_create_targets
    if (isinstance(completed, list) and
            len(set(completed)) >= provider_identity['num_nodes'] and all(
                ordinary_launch_binding.ordinary_paid_gcp_resource_name_matches(
                    provider_identity, target) for target in completed)):
        # A complete set of DONE exact inserts (including DONE-with-error)
        # cannot materialize later. This is the preferred evidence for retained
        # cohort-11 rows and bypasses the conservative propagation horizon.
        return True
    quiesced_at = facts.execution_quiesced_at
    if quiesced_at is None:
        return False
    now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    # Even cohort 12 can lose the create response before the operation becomes
    # list-visible. Retention prevents later evidence loss, while this horizon
    # prevents a just-accepted insert from passing two early empty censuses.
    return bool(now >= quiesced_at +
                datetime.timedelta(seconds=ordinary_launch_binding.
                                   ORDINARY_PAID_GCP_ABSENCE_SETTLE_SECONDS))


def _ordinary_paid_provider_payload_from_locked_request(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    association: Mapping[str, Any],
    facts: BoundOrdinaryLaunchRequestFacts,
    request_row: sqlalchemy.engine.RowMapping | None,
    queue_row: sqlalchemy.engine.RowMapping | None,
    evidence: ordinary_launch_binding_lib.ProviderEvidence,
    candidate_payload: Mapping[str, Any] | None,
    *,
    expected_reconciliation_outcome: str,
    require_paid_claim: bool = True,
    require_retention_pin: bool = True,
) -> dict[str, Any] | None:
    """Validate provider evidence against one exact locked paid request."""
    pool_identity = paid_capacity.pool_key_payload(
        str(association.get('paid_capacity_pool_key')))
    cloud = pool_identity.get('cloud') if isinstance(pool_identity,
                                                     Mapping) else None
    if cloud == 'aws':
        if evidence is not ordinary_launch_binding.ProviderEvidence.ABSENT:
            return None
        payload = _ordinary_paid_provider_absence_payload_from_locked_request(
            connection,
            context,
            association,
            facts,
            request_row,
            queue_row,
            expected_reconciliation_outcome=expected_reconciliation_outcome,
            require_paid_claim=require_paid_claim,
            require_retention_pin=require_retention_pin)
        if candidate_payload is not None and payload != dict(candidate_payload):
            return None
        return payload
    if cloud != 'gcp' or evidence not in (
            ordinary_launch_binding.ProviderEvidence.ABSENT,
            ordinary_launch_binding.ProviderEvidence.PRESENT):
        return None
    provider_identity = (
        _ordinary_paid_gcp_provider_identity_from_locked_request(
            connection,
            context,
            association,
            facts,
            request_row,
            queue_row,
            expected_reconciliation_outcome=expected_reconciliation_outcome,
            require_paid_claim=require_paid_claim,
            require_retention_pin=require_retention_pin))
    if provider_identity is None or not isinstance(candidate_payload, Mapping):
        return None
    if candidate_payload.get('provider_identity') != provider_identity:
        return None
    if evidence is ordinary_launch_binding.ProviderEvidence.ABSENT:
        create_targets = candidate_payload.get('create_operation_targets')
        failed_targets = (create_targets.get('failed') if isinstance(
            create_targets, Mapping) else None)
        succeeded_targets = (create_targets.get('succeeded') if isinstance(
            create_targets, Mapping) else None)
        completed_targets = (sorted(failed_targets + succeeded_targets)
                             if isinstance(failed_targets, list) and
                             isinstance(succeeded_targets, list) else None)
        if not _ordinary_paid_gcp_absence_settle_horizon_elapsed(
                connection, context, facts, provider_identity,
                completed_targets):
            return None
    try:
        canonical, _ = ordinary_launch_binding._ordinary_paid_provider_evidence(  # pylint: disable=protected-access
            association,
            str(association['cluster_name']),
            evidence,
            evidence_payload=candidate_payload)
    except ordinary_launch_binding.OrdinaryLaunchBindingConflict:
        return None
    return canonical


def bound_non_pool_gcp_provider_identity(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> dict[str, Any] | None:
    """Read exact GCP scope only after terminal request quiescence."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None
    with engine.begin() as connection:
        association = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        if (not _controller_authority_matches_reduction(association, authority)
                or association['resolution'] !=
                ordinary_launch_binding.Resolution.AMBIGUOUS.value):
            return None
        facts, request_row, queue_row, _ = _lock_bound_request_evidence(
            connection, context)
        return _ordinary_paid_gcp_provider_identity_from_locked_request(
            connection,
            context,
            association,
            facts,
            request_row,
            queue_row,
            expected_reconciliation_outcome=(
                ordinary_launch_binding.ReconciliationOutcome.
                POST_EFFECT_AMBIGUOUS.value))


def bound_non_pool_gcp_provider_absence_is_settled(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    *,
    completed_create_targets: list[str],
) -> bool:
    """Fence GCP absence behind retained operations or legacy quiet time."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    with engine.begin() as connection:
        association = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        if (not _controller_authority_matches_reduction(association, authority)
                or association['resolution'] !=
                ordinary_launch_binding.Resolution.AMBIGUOUS.value):
            return False
        facts, request_row, queue_row, _ = _lock_bound_request_evidence(
            connection, context)
        identity = _ordinary_paid_gcp_provider_identity_from_locked_request(
            connection,
            context,
            association,
            facts,
            request_row,
            queue_row,
            expected_reconciliation_outcome=(
                ordinary_launch_binding.ReconciliationOutcome.
                POST_EFFECT_AMBIGUOUS.value))
        return bool(
            identity is not None and
            _ordinary_paid_gcp_absence_settle_horizon_elapsed(
                connection, context, facts, identity, completed_create_targets))


def bound_non_pool_terminal_provider_absence_payload(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> dict[str, Any] | None:
    """Read a non-authorizing exact paid-create negative acknowledgement."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None
    with engine.begin() as connection:
        association = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        if (not _controller_authority_matches_reduction(association, authority)
                or association['resolution'] !=
                ordinary_launch_binding.Resolution.AMBIGUOUS.value):
            return None
        facts, request_row, queue_row, _ = _lock_bound_request_evidence(
            connection, context)
        return _ordinary_paid_provider_absence_payload_from_locked_request(
            connection,
            context,
            association,
            facts,
            request_row,
            queue_row,
            expected_reconciliation_outcome=(
                ordinary_launch_binding.ReconciliationOutcome.
                POST_EFFECT_AMBIGUOUS.value))


def record_bound_non_pool_provider_evidence(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    evidence: ordinary_launch_binding_lib.ProviderEvidence,
    payload: Mapping[str, Any],
) -> bool:
    """Atomically require exact request quiescence and record provider data."""

    def _request_terminal_evidence(
        connection: sqlalchemy.engine.Connection,
        locked_context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    ) -> ordinary_launch_binding_lib.TerminalEvidence | None:
        facts, request_row, queue_row, _ = _lock_bound_request_evidence(
            connection, locked_context)
        if (ordinary_launch_binding.is_paid_provider_reconciliation_profile(
                locked_context.profile.kind) and
                evidence in (ordinary_launch_binding.ProviderEvidence.ABSENT,
                             ordinary_launch_binding.ProviderEvidence.PRESENT)):
            association = (
                ordinary_launch_binding.lock_reduction_authority_in_connection(
                    connection, locked_context))
            expected_payload = _ordinary_paid_provider_payload_from_locked_request(
                connection,
                locked_context,
                association,
                facts,
                request_row,
                queue_row,
                evidence,
                payload,
                expected_reconciliation_outcome=(
                    ordinary_launch_binding.ReconciliationOutcome.
                    POST_EFFECT_AMBIGUOUS.value))
            if expected_payload is None or dict(payload) != expected_payload:
                return None
        ready = bool(
            facts.exists and
            facts.status in requests_lib.RequestStatus.finished_status() and
            facts.quiescent and queue_row is None and
            facts.retention_pin_active and facts.terminal_cause is not None)
        return _terminal_evidence(facts) if ready else None

    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Generic provider reconciliation requires PostgreSQL.')
    with engine.begin() as connection:
        return ordinary_launch_binding.record_non_pool_provider_evidence(
            connection, context, authority, evidence, payload,
            _request_terminal_evidence)


def _lock_bound_non_pool_provider_present_cleanup(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> tuple[Mapping[str, Any], BoundOrdinaryLaunchRequestFacts,
           replica_managers.ReplicaInfo]:
    """Lock and validate the complete provider-present cleanup authority."""
    # The ReplicaInfo projector touches zero-cost sequencing.  Preserve its
    # global lock order before locking lifecycle/service/replica/association.
    serve_state.lock_zero_cost_protocol_for_bound_launch_projection(connection)
    association = ordinary_launch_binding.lock_reduction_authority_in_connection(
        connection, context)
    if not _controller_authority_matches_reduction(association, authority):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider-present cleanup writer no longer owns this '
            'association.')
    facts, request_row, queue_row, _ = _lock_bound_request_evidence(
        connection, context)
    if (request_row is None or not facts.exists or
            facts.status not in requests_lib.RequestStatus.finished_status() or
            facts.terminal_cause is None or
            facts.execution_generation is None or
            facts.execution_generation < 1 or
            facts.execution_quiescence_required is not True or
            facts.execution_quiesced_generation != facts.execution_generation or
            facts.execution_quiesced_at is None or not facts.quiescent or
            queue_row is not None or not facts.retention_pin_active or
            facts.error_decode_failed or facts.return_value is not None):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider-present cleanup requires one exact terminal request '
            'generation, quiescence receipt, and active retention pin.')
    try:
        request = _request_from_mapping(
            typing.cast(sqlalchemy.engine.RowMapping, request_row))
        parsed_context = (
            ordinary_launch_binding.parse_bound_non_pool_launch_context(
                request.request_body.extra_launch_context))
        request_service_job_id = _request_service_job_id(
            request_row, str(association['cluster_name']))
    except ordinary_launch_binding.OrdinaryLaunchBindingConflict:
        raise
    except Exception as error:  # pylint: disable=broad-except
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider-present cleanup could not decode the exact bound '
            'request payload.') from error
    if ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind):
        request_identity_matches = _ordinary_paid_request_identity_matches(
            connection, association, request_row, request, context)
    else:
        request_identity_matches = _provider_present_cleanup_input_digest_matches(
            connection, association, request_row, request, context)
    if (parsed_context != context or not request_identity_matches or
            request_service_job_id is not None or
            association['service_job_id'] is not None):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider-present cleanup request identity, profile, digest, or '
            'service-job result is inconsistent.')
    if (ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            context.profile.kind) and
            _ordinary_paid_provider_payload_from_locked_request(
                connection,
                context,
                association,
                facts,
                request_row,
                queue_row,
                ordinary_launch_binding.ProviderEvidence.PRESENT,
                association.get('provider_evidence_payload'),
                expected_reconciliation_outcome=(
                    ordinary_launch_binding.ReconciliationOutcome.
                    POST_EFFECT_AMBIGUOUS.value)) is None):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider-present cleanup lost its immutable paid GCP identity.')
    terminal_evidence = _terminal_evidence(facts)
    checked_association, locked_replica_info = (
        ordinary_launch_binding.
        provider_presence_cleanup_authority_in_connection(
            connection, context, terminal_evidence))
    if not _paid_capacity_claim_is_exact(connection, checked_association):
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Provider-present cleanup found unexpected paid capacity.')
    return checked_association, facts, locked_replica_info


def _provider_present_cleanup_input_digest_matches(
    connection: sqlalchemy.engine.Connection,
    association: Mapping[str, Any],
    request_row: sqlalchemy.engine.RowMapping,
    request: requests_lib.Request,
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
) -> bool:
    """Match the exact atomic executable digest and service owner."""
    body = request.request_body
    env_vars = getattr(body, 'env_vars', None)
    tenant_scope = association.get('tenant_scope')
    cluster_name = association.get('cluster_name')
    if (not isinstance(env_vars, Mapping) or
            request_row['user_id'] != tenant_scope or
            env_vars.get(skylet_constants.USER_ID_ENV_VAR) != tenant_scope or
            request_row['cluster_name'] != cluster_name or
            getattr(body, 'cluster_name', None) != cluster_name):
        return False
    try:
        executable_exact = (ordinary_launch_binding.canonical_launch_digest(
            body) == context.input_digest)
    except ValueError:
        return False
    service_owner = connection.execute(
        sqlalchemy.select(
            serve_state_schema.services_table.c.owner_user_id,
            serve_state_schema.services_table.c.owner_user_name).where(
                serve_state_schema.services_table.c.name ==
                context.service_name)).mappings().one_or_none()
    if service_owner is None:
        return False
    owner_user_id = service_owner['owner_user_id']
    owner_user_name = service_owner['owner_user_name']
    if (not isinstance(owner_user_id, str) or not owner_user_id or
            not isinstance(owner_user_name, str) or not owner_user_name):
        return False
    # Atomic fill stamps the immutable service-owner tuple before hashing.
    return bool(executable_exact and tenant_scope == owner_user_id and
                env_vars.get(skylet_constants.USER_ENV_VAR) == owner_user_name)


def _ordinary_paid_request_identity_matches(
    connection: sqlalchemy.engine.Connection,
    association: Mapping[str, Any],
    request_row: sqlalchemy.engine.RowMapping,
    request: requests_lib.Request,
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
) -> bool:
    """Match the durable post-normalization ordinary-paid request identity.

    HTTP non-pool admission hashes the prepared body before replacing its
    submitted service-owner environment and client API metadata with the
    authenticated request actor.  Reconstruct that exact pre-normalization
    shape from the durable service-owner snapshot before checking the digest.
    The immutable association/profile context is validated by the caller;
    this check also retains the exact request, authenticated tenant, and
    cluster relations written by admission.
    """
    body = request.request_body
    env_vars = getattr(body, 'env_vars', None)
    tenant_scope = association.get('tenant_scope')
    cluster_name = association.get('cluster_name')
    request_id = association.get('request_id')
    normalized_identity_matches = bool(
        isinstance(tenant_scope, str) and bool(tenant_scope) and
        isinstance(cluster_name, str) and bool(cluster_name) and
        isinstance(request_id, str) and bool(request_id) and
        isinstance(env_vars, Mapping) and request.request_id == request_id and
        request_row['request_id'] == request_id and
        request_row['user_id'] == tenant_scope and
        env_vars.get(skylet_constants.USER_ID_ENV_VAR) == tenant_scope and
        isinstance(env_vars.get(skylet_constants.USER_ENV_VAR), str) and
        bool(env_vars[skylet_constants.USER_ENV_VAR]) and
        request_row['cluster_name'] == cluster_name and
        getattr(body, 'cluster_name', None) == cluster_name)
    if not normalized_identity_matches:
        return False
    service_owner = connection.execute(
        sqlalchemy.select(
            serve_state_schema.services_table.c.owner_user_id,
            serve_state_schema.services_table.c.owner_user_name).where(
                serve_state_schema.services_table.c.name ==
                context.service_name)).mappings().one_or_none()
    if service_owner is None:
        return False
    owner_user_id = service_owner['owner_user_id']
    owner_user_name = service_owner['owner_user_name']
    if (not isinstance(owner_user_id, str) or not owner_user_id or
            not isinstance(owner_user_name, str) or not owner_user_name):
        return False
    try:
        if (ordinary_launch_binding.canonical_launch_digest(body) ==
                context.input_digest):
            return True
        prepared_body = body.model_copy(deep=True)
        prepared_body.env_vars[skylet_constants.USER_ID_ENV_VAR] = owner_user_id
        prepared_body.env_vars[skylet_constants.USER_ENV_VAR] = owner_user_name
        prepared_body.client_api_version = None
        return (ordinary_launch_binding.canonical_launch_digest(prepared_body)
                == context.input_digest)
    except (AttributeError, TypeError, ValueError):
        return False


def authorize_bound_non_pool_provider_present_cleanup(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    *,
    project_replica_result: BoundOrdinaryLaunchReplicaProjector,
) -> bool:
    """Atomically enter fenced teardown for one exact PRESENT allocation."""
    if not callable(project_replica_result):
        raise TypeError('project_replica_result must be callable.')
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Generic provider reconciliation requires PostgreSQL.')
    with engine.begin() as connection:
        association, facts, locked_replica_info = (
            _lock_bound_non_pool_provider_present_cleanup(
                connection, context, authority))
        if ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
                locked_replica_info):
            return True
        projection = BoundOrdinaryLaunchProjectionInput(
            context=context,
            request=facts,
            locked_replica_info=locked_replica_info,
            status=typing.cast(requests_lib.RequestStatus, facts.status),
            cause=typing.cast(event_api_models.EventCause,
                              facts.terminal_cause),
            service_job_id=None,
            pre_effect_terminal=False,
            cancel_reason=association.get('cancel_reason'),
            paid_capacity_pool_key=association['paid_capacity_pool_key'],
            provider_evidence=ordinary_launch_binding.ProviderEvidence.PRESENT)
        if project_replica_result(connection, projection) is not True:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider-present cleanup persistence lost its exact row.')
        persisted = (
            serve_state.read_replica_for_bound_ordinary_launch_in_transaction(
                connection, context.service_name, context.replica_id,
                str(context.replica_record_id), context.association_id))
        if not ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
                persisted, require_scheduled=True):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider-present cleanup projector did not persist the '
                'closed immediate-cleanup marker.')
        if not _paid_capacity_claim_is_exact(connection, association):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider-present cleanup changed paid-capacity identity.')
    return True


def bound_non_pool_provider_present_cleanup_is_authorized(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> bool:
    """Revalidate a durable PRESENT cleanup marker without provider I/O."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    try:
        with engine.begin() as connection:
            _, _, locked_replica_info = (
                _lock_bound_non_pool_provider_present_cleanup(
                    connection, context, authority))
            return ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
                locked_replica_info)
    except ordinary_launch_binding.OrdinaryLaunchBindingConflict:
        return False


def project_bound_non_pool_provider_absence(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    *,
    project_replica_result: BoundOrdinaryLaunchReplicaProjector,
) -> bool:
    """Atomically project one exact quiescent typed provider absence."""
    if not callable(project_replica_result):
        raise TypeError('project_replica_result must be callable.')
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ordinary_launch_binding.OrdinaryLaunchBindingUnavailable(
            'Generic provider reconciliation requires PostgreSQL.')
    with engine.begin() as connection:
        # The ReplicaInfo projector can touch the zero-cost materialization
        # sequencer. Preserve its global lock order even though exact absence
        # must never stamp a successful materialization.
        serve_state.lock_zero_cost_protocol_for_bound_launch_projection(
            connection)
        association = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        if not _controller_authority_matches_reduction(association, authority):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider absence writer no longer owns this association.')
        facts, request_row, queue_row, _ = _lock_bound_request_evidence(
            connection, context)
        if (not facts.exists or facts.status
                not in requests_lib.RequestStatus.finished_status() or
                facts.terminal_cause is None or not facts.quiescent or
                queue_row is not None or not facts.retention_pin_active):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider absence projection requires exact terminal request '
                'quiescence and its active retention pin.')
        terminal_evidence = _terminal_evidence(facts)
        paid_provider_absence = (
            ordinary_launch_binding.is_paid_provider_reconciliation_profile(
                context.profile.kind))
        expected_payload = None
        if paid_provider_absence:
            expected_payload = _ordinary_paid_provider_payload_from_locked_request(
                connection,
                context,
                association,
                facts,
                request_row,
                queue_row,
                ordinary_launch_binding.ProviderEvidence.ABSENT,
                association.get('provider_evidence_payload'),
                expected_reconciliation_outcome=(
                    ordinary_launch_binding.ReconciliationOutcome.
                    POST_EFFECT_AMBIGUOUS.value))
            if expected_payload is None:
                raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                    'Paid provider absence has no exact locked request '
                    'receipt.')
        checked_association, locked_replica_info = (
            ordinary_launch_binding.
            provider_absence_projection_authority_in_connection(
                connection,
                context,
                terminal_evidence,
                expected_provider_evidence_payload=expected_payload))
        if not _paid_capacity_claim_is_exact(connection, checked_association):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider absence projection found unexpected paid capacity.')
        if paid_provider_absence:
            if checked_association[
                    'provider_evidence_payload'] != expected_payload:
                raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                    'Paid provider absence no longer matches the exact locked '
                    'request receipt.')
        projection_paid_pool_key = (
            checked_association['paid_capacity_pool_key']
            if paid_provider_absence else None)
        projection = BoundOrdinaryLaunchProjectionInput(
            context=context,
            request=facts,
            locked_replica_info=locked_replica_info,
            status=facts.status,
            cause=facts.terminal_cause,
            service_job_id=None,
            pre_effect_terminal=False,
            cancel_reason=checked_association.get('cancel_reason'),
            paid_capacity_pool_key=projection_paid_pool_key,
            provider_evidence=ordinary_launch_binding.ProviderEvidence.ABSENT,
            provider_evidence_payload=expected_payload)
        if project_replica_result(connection, projection) is not True:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider absence replica projection lost its exact row.')
        if not _paid_capacity_claim_is_exact(connection, checked_association):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider absence projection changed paid-capacity identity.')
        ordinary_launch_binding.project_provider_absence_in_connection(
            connection,
            context,
            terminal_evidence,
            expected_provider_evidence_payload=expected_payload)
        if not delete_request_retention_pin_in_transaction(
                connection, context.request_id,
                ORDINARY_LAUNCH_RETENTION_PIN_KIND, context.association_id):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider absence could not release its exact request pin.')
        if not (ordinary_launch_binding.
                release_projected_paid_capacity_claim_in_connection(
                    connection, context)):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Provider absence could not close paid-capacity authority.')
    return True


def bound_non_pool_projected_provider_absence_is_authorized(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> bool:
    """Authorize only replica-row removal after committed exact ABSENT."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    try:
        with engine.begin() as connection:
            association, _ = (
                ordinary_launch_binding.
                projected_provider_absence_cleanup_authority_in_connection(
                    connection, service_name, replica_id, replica_record_id))
            context = _bound_context_from_association(association)
            if not isinstance(
                    context, ordinary_launch_binding.BoundNonPoolLaunchContext):
                return False
            facts, _, queue_row, pin_row = _lock_bound_request_evidence(
                connection, context)
            paid_profile = (
                ordinary_launch_binding.is_paid_provider_reconciliation_profile(
                    context.profile.kind))
            claim_state_is_closed = (_paid_capacity_claim_is_released(
                connection, association) if paid_profile else
                                     _paid_capacity_claim_is_exact(
                                         connection, association))
            if (queue_row is not None or pin_row is not None or
                    facts.retention_pin_active or not claim_state_is_closed):
                return False
            if facts.exists:
                request_status = facts.status
                terminal_cause = facts.terminal_cause
                if (request_status is None or request_status
                        not in requests_lib.RequestStatus.finished_status() or
                        terminal_cause is None or
                        request_status.value != association['terminal_status']
                        or
                        terminal_cause.value != association['terminal_cause'] or
                        facts.execution_generation !=
                        association['terminal_execution_generation'] or
                        facts.execution_quiescence_required is not True or
                        facts.execution_quiesced_generation !=
                        association['execution_quiesced_generation'] or
                        facts.execution_quiesced_at !=
                        association['execution_quiesced_at'] or
                        not facts.quiescent):
                    return False
            return True
    except ordinary_launch_binding.OrdinaryLaunchBindingConflict:
        return False


def retire_bound_non_pool_projected_paid_provider_absence(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> bool:
    """Atomically validate and delete one provider-free paid replica row."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    try:
        record_uuid = uuid.UUID(replica_record_id)
    except (AttributeError, TypeError, ValueError):
        return False
    if str(record_uuid) != replica_record_id:
        return False
    try:
        with engine.begin() as connection:
            association, info = (
                ordinary_launch_binding.
                projected_provider_absence_cleanup_authority_in_connection(
                    connection, service_name, replica_id, replica_record_id))
            profile = ordinary_launch_binding.NonPoolLaunchProfileKind(
                str(association['profile_kind']))
            if (not ordinary_launch_binding.
                    is_paid_provider_reconciliation_profile(profile) or
                    not ordinary_launch_binding.
                    replica_has_projected_provider_absence_cleanup_marker(info)
               ):
                return False
            context = _bound_context_from_association(association)
            if not isinstance(
                    context, ordinary_launch_binding.BoundNonPoolLaunchContext):
                return False
            facts, request_row, queue_row, pin_row = (
                _lock_bound_request_evidence(connection, context))
            if queue_row is not None or pin_row is not None:
                return False
            # The retention pin is released by the projection transaction, so
            # the request may be collected before a restarted controller
            # removes this replica row.  If it remains, compare it byte-for-
            # byte with the projected receipt.  If it has been collected, the
            # cleanup authority above still revalidates the immutable copied
            # terminal/quiescence facts, canonical receipt/digest, released
            # paid claim, null replica pointer, and exact row marker.
            if request_row is not None:
                expected_payload = _ordinary_paid_provider_payload_from_locked_request(
                    connection,
                    context,
                    association,
                    facts,
                    request_row,
                    queue_row,
                    ordinary_launch_binding.ProviderEvidence.ABSENT,
                    association.get('provider_evidence_payload'),
                    expected_reconciliation_outcome=(
                        ordinary_launch_binding.ReconciliationOutcome.PROJECTED.
                        value),
                    require_paid_claim=False,
                    require_retention_pin=False)
                if (expected_payload is None or
                        association['provider_evidence_payload'] !=
                        expected_payload):
                    return False

            route_leases = (
                route_projection_schema.serve_route_replica_leases_table)
            paid_retirements = (
                paid_retirement.serve_paid_replica_retirements_table)
            kueue_admissions = (
                kueue_lane_lineage_schema.serve_kueue_admissions_table)
            dependent_counts = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        route_leases).where(
                            route_leases.c.service_name == service_name,
                            route_leases.c.replica_id == replica_id,
                            route_leases.c.replica_record_id ==
                            record_uuid).scalar_subquery(),
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        paid_retirements).where(
                            paid_retirements.c.service_name == service_name,
                            paid_retirements.c.replica_id ==
                            replica_id).scalar_subquery(),
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        kueue_admissions).where(
                            kueue_admissions.c.service_name == service_name,
                            kueue_admissions.c.replica_id ==
                            replica_id).scalar_subquery())).one()
            if any(int(count) != 0 for count in dependent_counts):
                return False
            replicas = serve_state_schema.replicas_table
            deleted = connection.execute(
                sqlalchemy.delete(replicas).where(
                    replicas.c.service_name == service_name,
                    replicas.c.replica_id == replica_id,
                    replicas.c.ordinary_launch_association_id.is_(None),
                    replicas.c.paid_capacity_pool_key ==
                    association['paid_capacity_pool_key'],
                    replicas.c.replica_state['replica_record_id'].as_string() ==
                    replica_record_id))
            if deleted.rowcount != 1:
                raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                    'Paid provider-absence retirement lost its exact row.')
        return True
    except ordinary_launch_binding.OrdinaryLaunchBindingConflict:
        return False


def bound_non_pool_provider_reconciliation_ready(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> bool:
    """Fence a provider read behind exact terminal executor quiescence."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    with engine.begin() as connection:
        association = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        if (not _controller_authority_matches_reduction(association, authority)
                or association['resolution'] !=
                ordinary_launch_binding.Resolution.AMBIGUOUS.value or
                association['reconciliation_outcome'] != ordinary_launch_binding
                .ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value):
            return False
        facts, _, queue_row, _ = _lock_bound_request_evidence(
            connection, context)
        return bool(
            facts.exists and
            facts.status in requests_lib.RequestStatus.finished_status() and
            facts.quiescent and queue_row is None and
            facts.retention_pin_active)


def bound_non_pool_provider_absence_is_recorded(
    context: ordinary_launch_binding_lib.BoundNonPoolLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
) -> bool:
    """Return whether immutable exact absence is ready for projection."""
    engine = initialize_and_get_db()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    with engine.begin() as connection:
        association = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        return bool(
            _controller_authority_matches_reduction(association, authority) and
            association['resolution']
            == ordinary_launch_binding.Resolution.AMBIGUOUS.value and
            association['reconciliation_outcome'] == ordinary_launch_binding.
            ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value and
            association['provider_evidence']
            == ordinary_launch_binding.ProviderEvidence.ABSENT.value)


def _commit_bound_ordinary_launch_cancel_intent(
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    reason: str,
) -> None:
    """Commit Serve-owned cancel intent before touching the API request."""
    # Cancellation must not wait for the shared opaque-provider guard.  A
    # retry-until-up provider call can hold that guard indefinitely, while the
    # request cancellation below is what interrupts its executor.  Canonical
    # lifecycle/service/replica/association row locks still serialize this
    # intent with phase advance and projection exactly.
    engine = initialize_and_get_db()
    with engine.begin() as connection:
        association = ordinary_launch_binding.lock_reduction_authority_in_connection(
            connection, context)
        if not _controller_authority_matches_reduction(association, authority):
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'Bound cancellation authority belongs to another controller '
                'incarnation.')
        if association[
                'resolution'] == ordinary_launch_binding.Resolution.AMBIGUOUS.value:
            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                'An ambiguous association cannot authorize cancellation.')
        if association['resolution'] == (
                ordinary_launch_binding.Resolution.RESULT_RECORDED.value):
            return
        ordinary_launch_binding.request_cancel_in_connection(
            connection, context, reason)


def _cancel_bound_ordinary_launch_request_in_transaction(
    connection: sqlalchemy.engine.Connection,
    context: ordinary_launch_binding_lib.BoundLaunchContext,
) -> BoundOrdinaryLaunchRequestFacts:
    """Publish exact request cancellation after Serve intent is durable."""
    facts, request_row, queue_row, _ = _lock_bound_request_evidence(
        connection, context)
    if request_row is None or facts.status in (
            requests_lib.RequestStatus.finished_status()):
        return facts
    assert facts.execution_generation is not None
    no_execution_owner = bool(not facts.claim_exists and
                              (queue_row is None or
                               (queue_row['delivery_state'] == 'queued' and
                                queue_row['claim_generation'] is None)))
    now = sqlalchemy.func.clock_timestamp()
    values: dict[str, Any] = {
        'cancel_requested_at': now,
        'execution_quiescence_required': True,
        'finished_at': now,
    }
    if no_execution_owner:
        values.update({
            'execution_quiesced_generation': facts.execution_generation,
            'execution_quiesced_at': now,
        })
    transitioned = _terminalize_locked_request(
        connection,
        request_row,
        status=requests_lib.RequestStatus.CANCELLED,
        cause=event_api_models.EventCause.EXPLICIT_CANCEL,
        values=values)
    if not transitioned:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Bound request cancellation lost its exact request row.')
    # The owning executor polls durable status and publishes its exact
    # generation acknowledgement from its finally block.  A claimed request
    # therefore remains WAIT_QUIESCENCE; this transaction never invents proof.
    updated, _, _, _ = _lock_bound_request_evidence(connection, context)
    return updated


def cancel_bound_ordinary_launch_request(
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    reason: str,
    *,
    project_replica_result: BoundOrdinaryLaunchReplicaProjector,
) -> OrdinaryLaunchReduction:
    """Commit cancel intent, deliver it exactly, then reduce if quiescent."""
    request_bound_ordinary_launch_cancel(context, authority, reason)
    return reduce_bound_ordinary_launch(
        context, authority, project_replica_result=project_replica_result)


def request_bound_ordinary_launch_cancel(
    context: ordinary_launch_binding_lib.BoundLaunchContext,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    reason: str,
) -> BoundOrdinaryLaunchRequestFacts:
    """Durably deliver exact cancellation before provider quiescence."""
    _commit_bound_ordinary_launch_cancel_intent(context, authority, reason)
    engine = initialize_and_get_db()
    with engine.begin() as connection:
        return _cancel_bound_ordinary_launch_request_in_transaction(
            connection, context)


def _legacy_projected_cleanup_drains_request_in_transaction(
    connection: sqlalchemy.engine.Connection,
    row: Mapping[str, Any],
    context: Mapping[str, Any],
    service_name: str,
) -> bool:
    """Return whether an exact Serve047 tombstone contains this request."""
    service_hash = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
    replica_version = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY)
    replica_id = context.get(
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY)
    replica_record_id = context.get(
        serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY)
    if replica_id is None:
        replica_id = context.get(ordinary_launch_binding.REPLICA_ID_KEY)
    if replica_record_id is None:
        replica_record_id = context.get(
            ordinary_launch_binding.REPLICA_RECORD_ID_KEY)
    try:
        replica_record_uuid = uuid.UUID(str(replica_record_id))
    except (AttributeError, TypeError, ValueError):
        return False
    if (not isinstance(service_hash, str) or not service_hash or
            type(replica_version) is not int or replica_version < 1 or
            type(replica_id) is not int or replica_id < 1):
        return False

    ledger = ordinary_launch_binding.legacy_reconciliations_table
    predicates = [
        ledger.c.service_name == service_name,
        ledger.c.service_hash == service_hash,
        ledger.c.replica_id == replica_id,
        ledger.c.replica_record_id == replica_record_uuid,
        ledger.c.replica_version == replica_version,
        ledger.c.cluster_name == row['cluster_name'],
        ledger.c.request_id == row['request_id'],
        ledger.c.observed_request_status == str(row['status']),
        ledger.c.observed_request_execution_generation ==
        row['execution_generation'],
        ledger.c.observed_request_queue_present.is_(False),
        ledger.c.resolution ==
        ordinary_launch_binding.LegacyReconciliationResolution.PROJECTED.value,
    ]
    provider_context = context.get(
        serve_constants.RESERVED_FILL_LAUNCH_KUBERNETES_CONTEXT_KEY)
    physical_uid = context.get(
        serve_constants.RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY)
    if provider_context is not None:
        predicates.append(ledger.c.provider_context == provider_context)
    if physical_uid is not None:
        predicates.append(
            ledger.c.provider_physical_resource_uid == physical_uid)
    return bool(
        connection.execute(
            sqlalchemy.select(
                sqlalchemy.exists().where(*predicates))).scalar_one())


def _legacy_ordinary_launch_requests_drained_in_transaction(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> bool:
    """Close generic admission and prove this service's legacy drain."""
    # The promotion transaction already owns the exclusive per-service launch
    # authority lock. Every valid legacy admission takes its shared side before
    # INSERT (see ``_insert_request_and_queue``), so there is no request phantom:
    # an earlier admission commits before promotion acquires authority and is
    # scanned here; a later admission waits for bound mode and then fails its
    # legacy effect fence. Avoid table locks here: a claimant owns row locks
    # before upgrading to ROW EXCLUSIVE for its status write, which would form a
    # lock cycle if promotion held a compatible SHARE lock while waiting for
    # that same row.
    rows = connection.execute(
        sqlalchemy.select(REQUESTS).where(
            REQUESTS.c.handler_name == _LEGACY_ORDINARY_LAUNCH_HANDLER_NAME,
            REQUESTS.c.payload_type == _LEGACY_ORDINARY_LAUNCH_PAYLOAD_TYPE,
            REQUESTS.c.payload_json['is_launched_by_sky_serve_controller'].
            as_boolean().is_(True)).order_by(
                REQUESTS.c.request_id).with_for_update()).mappings().all()
    for row in rows:
        payload_json = row['payload_json']
        if not isinstance(payload_json, dict):
            return False
        context = payload_json.get('extra_launch_context')
        if not isinstance(context, dict):
            return False
        candidate_service = context.get(
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
        if not isinstance(candidate_service, str) or not candidate_service:
            return False
        if candidate_service != service_name:
            continue
        status = requests_lib.RequestStatus(str(row['status']))
        if status not in requests_lib.RequestStatus.finished_status():
            return False
        queue_exists = connection.execute(
            sqlalchemy.select(sqlalchemy.literal(True)).where(
                sqlalchemy.exists().where(
                    QUEUE.c.request_id ==
                    row['request_id']))).scalar_one_or_none()
        if queue_exists:
            return False
        has_request_receipt = (row['execution_quiescence_required'] is True and
                               row['execution_generation'] is not None and
                               row['execution_quiesced_generation']
                               == row['execution_generation'] and
                               row['execution_quiesced_at'] is not None)
        if (not has_request_receipt and
                not _legacy_projected_cleanup_drains_request_in_transaction(
                    connection, row, context, service_name)):
            return False
    return True


def _bound_ordinary_launch_requests_clear_in_transaction(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> bool:
    """Prove every settled association has copied, unpinned request state."""
    associations = connection.execute(
        sqlalchemy.select(
            ordinary_launch_binding.ordinary_launch_associations_table).where(
                ordinary_launch_binding.ordinary_launch_associations_table.c.
                service_name == service_name).order_by(
                    ordinary_launch_binding.ordinary_launch_associations_table.
                    c.request_id)).mappings().all()
    for association in associations:
        if association['resolution'] not in tuple(
                value.value
                for value in ordinary_launch_binding.SETTLED_RESOLUTIONS):
            return False
        context = _bound_context_from_association(association)
        facts, _, queue_row, pin_row = _lock_bound_request_evidence(
            connection, context)
        if pin_row is not None or queue_row is not None:
            return False
        if not facts.exists:
            # Request GC is permitted only after the association copied exact
            # terminal evidence and released its pin.
            if (association['terminal_status'] is None or
                    association['terminal_cause'] is None or
                    association['terminal_execution_generation'] is None or
                    association['execution_quiesced_at'] is None):
                return False
            continue
        if (facts.status not in requests_lib.RequestStatus.finished_status() or
                facts.terminal_cause is None or not facts.quiescent or
                association['terminal_status'] != facts.status.value or
                association['terminal_cause'] != facts.terminal_cause.value or
                association['terminal_execution_generation'] !=
                facts.execution_generation or
                association['execution_quiesced_generation'] !=
                facts.execution_quiesced_generation):
            return False
    return True


def _transition_authority_is_current(
    connection: sqlalchemy.engine.Connection,
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    expected_mode: ordinary_launch_binding_lib.BindingMode,
) -> bool:
    row = connection.execute(
        sqlalchemy.select(
            serve_state_schema.services_table,
            serve_state_schema.service_lifecycle_fences_table.c.epoch.label(
                '_fence_epoch')).join(
                    serve_state_schema.service_lifecycle_fences_table,
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    serve_state_schema.services_table.c.name).where(
                        serve_state_schema.services_table.c.name ==
                        authority.service_name)).mappings().one_or_none()
    return bool(
        row is not None and row['hash'] == authority.service_hash and
        row['workspace'] == authority.service_workspace and
        row['lifecycle_epoch'] == authority.service_lifecycle_epoch and
        row['_fence_epoch'] == authority.service_lifecycle_epoch and
        row['controller_incarnation'] == authority.controller_incarnation and
        row['controller_owner_epoch'] == authority.controller_owner_epoch and
        row['ordinary_launch_binding_capable'] is True and
        row['non_pool_launch_binding_capable'] is authority.non_pool_capable and
        row['non_pool_launch_binding_protocol_version']
        == authority.non_pool_binding_protocol_version and
        row['non_pool_launch_capability_profile_set_digest']
        == authority.non_pool_profile_set_digest and
        row['non_pool_launch_capability_cohort_epoch']
        == authority.non_pool_capability_cohort_epoch and
        row['non_pool_launch_receipt_protocol_version']
        == authority.non_pool_receipt_protocol_version and
        row['ordinary_launch_binding_mode'] == expected_mode.value and
        row['ordinary_launch_binding_mode'] == authority.binding_mode.value and
        row['ordinary_launch_binding_epoch'] == authority.binding_epoch)


def _ordinary_launch_binding_participants_quiesced_in_transaction(
    connection: sqlalchemy.engine.Connection,) -> bool:
    """Close participant-heartbeat phantoms before promotion."""
    connection.execute(
        sqlalchemy.text('LOCK TABLE api_server_instances IN SHARE MODE'))
    return ordinary_launch_binding_fleet_capable(
        connection=connection,
        quiescence_seconds=(
            ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS))


def _non_pool_launch_binding_participants_quiesced_in_transaction(
    connection: sqlalchemy.engine.Connection,) -> bool:
    """Close capability-heartbeat phantoms before cohort rotation."""
    connection.execute(
        sqlalchemy.text('LOCK TABLE api_server_instances IN SHARE MODE'))
    return non_pool_launch_binding_fleet_capable(
        connection=connection,
        quiescence_seconds=(
            ORDINARY_LAUNCH_BINDING_PARTICIPANT_QUIESCENCE_SECONDS))


def promote_ordinary_launch_binding_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,) -> int:
    """Explicitly promote one service under fleet and legacy-drain barriers."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        epoch = ordinary_launch_binding.promote_service_in_connection(
            connection,
            service_name=authority.service_name,
            controller_incarnation=authority.controller_incarnation,
            controller_owner_epoch=authority.controller_owner_epoch,
            expected_binding_epoch=authority.binding_epoch,
            participant_barrier_passed=lambda conn:
            (_transition_authority_is_current(
                conn, authority, ordinary_launch_binding.BindingMode.LEGACY) and
             _ordinary_launch_binding_participants_quiesced_in_transaction(conn
                                                                          )),
            legacy_requests_drained=lambda conn:
            (_legacy_ordinary_launch_requests_drained_in_transaction(
                conn, authority.service_name)))
        session.commit()
        return epoch


def promote_non_pool_launch_binding_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,) -> int:
    """Promote one bound service after exact v2 fleet and v1 drain gates."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        epoch = (
            ordinary_launch_binding.
            promote_non_pool_launch_service_in_connection(
                connection,
                service_name=authority.service_name,
                controller_incarnation=authority.controller_incarnation,
                controller_owner_epoch=authority.controller_owner_epoch,
                expected_binding_epoch=authority.binding_epoch,
                participant_barrier_passed=lambda conn:
                (_transition_authority_is_current(
                    conn, authority, ordinary_launch_binding.BindingMode.BOUND
                ) and
                 _non_pool_launch_binding_participants_quiesced_in_transaction(
                     conn)),
                legacy_requests_drained=lambda conn:
                (_bound_ordinary_launch_requests_clear_in_transaction(
                    conn, authority.service_name) and
                 _legacy_ordinary_launch_requests_drained_in_transaction(
                     conn, authority.service_name))))
        session.commit()
        return epoch


def promote_ordered_capacity_admission_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,) -> int:
    """Promote durable demand after the exact API012 fleet barrier."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        connection.execute(
            sqlalchemy.text('LOCK TABLE api_server_instances IN SHARE MODE'))
        epoch = capacity_admission.promote_service_in_connection(
            connection,
            service_name=authority.service_name,
            controller_incarnation=authority.controller_incarnation,
            participant_barrier_passed=lambda conn:
            ordered_capacity_admission_fleet_capable(connection=conn))
        session.commit()
        return epoch


def promote_capacity_authorities_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    expected_demand_source_epoch: int,
    expected_zero_cost_actuation_epoch: int,
) -> capacity_authority_lib.CapacityAuthorityEpochs:
    """Atomically promote durable demand and grant-before-row actuation."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        connection.execute(
            sqlalchemy.text('LOCK TABLE api_server_instances IN SHARE MODE'))
        epochs = capacity_authority.promote_service_in_connection(
            connection,
            service_name=authority.service_name,
            controller_incarnation=authority.controller_incarnation,
            expected_demand_source_epoch=expected_demand_source_epoch,
            expected_zero_cost_actuation_epoch=(
                expected_zero_cost_actuation_epoch),
            participant_barrier_passed=lambda conn:
            ordered_capacity_admission_fleet_capable(connection=conn))
        session.commit()
        return epochs


def promote_zero_cost_actuation_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    expected_actuation_epoch: int,
) -> int:
    """Promote grant-before-row fill after the exact API015 fleet barrier."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        connection.execute(
            sqlalchemy.text('LOCK TABLE api_server_instances IN SHARE MODE'))
        epoch = zero_cost_actuation.promote_service_in_connection(
            connection,
            service_name=authority.service_name,
            controller_incarnation=authority.controller_incarnation,
            expected_actuation_epoch=expected_actuation_epoch,
            participant_barrier_passed=(
                ordered_capacity_admission_fleet_capable(
                    connection=connection)))
        session.commit()
        return epoch


def demote_ordered_capacity_admission_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,
    expected_source_epoch: int,
) -> int:
    """Demote durable demand only after every planner claim settles."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        epoch = capacity_admission.demote_service_in_connection(
            session.connection(),
            service_name=authority.service_name,
            controller_incarnation=authority.controller_incarnation,
            expected_source_epoch=expected_source_epoch)
        session.commit()
        return epoch


def demote_non_pool_launch_binding_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,) -> int:
    """Rollback a generic service after every protocol-v2 request settles."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        epoch = (
            ordinary_launch_binding.
            demote_non_pool_launch_service_in_connection(
                connection,
                service_name=authority.service_name,
                controller_incarnation=authority.controller_incarnation,
                controller_owner_epoch=authority.controller_owner_epoch,
                expected_binding_epoch=authority.binding_epoch,
                request_barrier_clear=lambda conn:
                (_transition_authority_is_current(
                    conn, authority, ordinary_launch_binding.BindingMode.BOUND)
                 and _bound_ordinary_launch_requests_clear_in_transaction(
                     conn, authority.service_name))))
        session.commit()
        return epoch


def demote_ordinary_launch_binding_service(
    authority: ordinary_launch_binding_lib.ControllerBindingAuthority,) -> int:
    """Explicitly demote after every bound generation is fully settled."""
    if not isinstance(authority,
                      ordinary_launch_binding.ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        epoch = ordinary_launch_binding.demote_service_in_connection(
            connection,
            service_name=authority.service_name,
            controller_incarnation=authority.controller_incarnation,
            controller_owner_epoch=authority.controller_owner_epoch,
            expected_binding_epoch=authority.binding_epoch,
            request_barrier_clear=lambda conn:
            (_transition_authority_is_current(
                conn, authority, ordinary_launch_binding.BindingMode.BOUND
            ) and _bound_ordinary_launch_requests_clear_in_transaction(
                conn, authority.service_name) and
             _legacy_ordinary_launch_requests_drained_in_transaction(
                 conn, authority.service_name)))
        session.commit()
        return epoch


def gc_bound_ordinary_launch_tombstones_in_transaction(
    connection: sqlalchemy.engine.Connection,
    *,
    limit: int = 100,
) -> int:
    """Collect settled associations only after proving request-layer absence.

    Selection, the request/pin absence predicates, and deletion share one
    transaction.  In particular, no caller-supplied boolean can stand in for
    request-layer evidence that may change between a check and the DELETE.
    """
    # API009 deliberately upgrades independently of Serve042.  Request GC
    # remains usable during either migration order and before the Serve
    # relation exists.
    relation_exists = connection.execute(
        sqlalchemy.text(
            "SELECT to_regclass('serve_ordinary_launch_associations') "
            'IS NOT NULL')).scalar_one()
    if not relation_exists:
        return 0
    if (isinstance(limit, bool) or not isinstance(limit, int) or
            not 1 <= limit <= ordinary_launch_binding.MAX_GC_BATCH_SIZE):
        raise ValueError('limit must be 1-'
                         f'{ordinary_launch_binding.MAX_GC_BATCH_SIZE}.')
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    request_reference_exists = sqlalchemy.exists().where(
        sqlalchemy.or_(
            REQUESTS.c.request_id == associations.c.request_id,
            REQUESTS.c.ordinary_launch_association_id ==
            associations.c.association_id))
    pin_reference_exists = sqlalchemy.exists().where(
        sqlalchemy.or_(
            REQUEST_RETENTION_PINS.c.request_id == associations.c.request_id,
            sqlalchemy.and_(
                REQUEST_RETENTION_PINS.c.pin_kind ==
                ORDINARY_LAUNCH_RETENTION_PIN_KIND,
                REQUEST_RETENTION_PINS.c.pin_id ==
                associations.c.association_id)))
    replica_reference_exists = sqlalchemy.exists().where(
        serve_state_schema.replicas_table.c.ordinary_launch_association_id ==
        associations.c.association_id)
    # Serve057 may be installed before or after API009.  Compile no reference
    # to its relation until PostgreSQL proves it exists, while preserving the
    # admission -> association RESTRICT edge whenever it does.  Otherwise one
    # protected Kueue association poisons the entire GC batch at DELETE time.
    admission_relation_exists = connection.execute(
        sqlalchemy.text(
            "SELECT to_regclass('serve_kueue_admissions') IS NOT NULL")
    ).scalar_one()
    admission_absence_predicates: tuple[Any, ...] = ()
    if admission_relation_exists:
        admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
        admission_reference_exists = sqlalchemy.exists().where(
            admissions.c.association_id == associations.c.association_id)
        admission_absence_predicates = (~admission_reference_exists,)
    candidate_rows = connection.execute(
        sqlalchemy.select(associations.c.association_id).where(
            associations.c.resolution.in_(
                tuple(
                    value.value
                    for value in ordinary_launch_binding.SETTLED_RESOLUTIONS)),
            associations.c.pin_released_at.is_not(None),
            associations.c.tombstone_not_before <=
            sqlalchemy.func.clock_timestamp(), ~replica_reference_exists,
            ~request_reference_exists, ~pin_reference_exists,
            *admission_absence_predicates).order_by(
                associations.c.tombstone_not_before,
                associations.c.association_id).limit(limit).with_for_update(
                    skip_locked=True)).scalars().all()
    if not candidate_rows:
        return 0
    result = connection.execute(
        sqlalchemy.delete(associations).where(
            associations.c.association_id.in_(candidate_rows),
            associations.c.resolution.in_(
                tuple(
                    value.value
                    for value in ordinary_launch_binding.SETTLED_RESOLUTIONS)),
            associations.c.pin_released_at.is_not(None),
            associations.c.tombstone_not_before <=
            sqlalchemy.func.clock_timestamp(), ~replica_reference_exists,
            ~request_reference_exists, ~pin_reference_exists,
            *admission_absence_predicates))
    return result.rowcount


def _request_filter_statement(
    req_filter: requests_lib.RequestTaskFilter,) -> sqlalchemy.sql.Select:
    statement = _request_projection_statement(req_filter.fields)
    if req_filter.status is not None:
        statement = statement.where(
            REQUESTS.c.status.in_(
                [status.value for status in req_filter.status]))
    if req_filter.request_ids is not None:
        statement = statement.where(
            REQUESTS.c.request_id.in_(req_filter.request_ids))
    if req_filter.execution_quiescence_candidates_only:
        execution_unproved = sqlalchemy.and_(
            REQUESTS.c.execution_quiescence_required,
            sqlalchemy.or_(
                REQUESTS.c.execution_quiesced_generation.is_distinct_from(
                    REQUESTS.c.execution_generation),
                REQUESTS.c.execution_quiesced_at.is_(None)))
        statement = statement.where(
            sqlalchemy.or_(
                REQUESTS.c.status.in_([
                    status.value
                    for status in requests_lib.RequestStatus.active_statuses()
                ]), execution_unproved))
    if req_filter.cluster_names is not None:
        statement = statement.where(
            REQUESTS.c.cluster_name.in_(req_filter.cluster_names))
    if req_filter.user_id is not None:
        statement = statement.where(REQUESTS.c.user_id == req_filter.user_id)
    if req_filter.exclude_request_names is not None:
        statement = statement.where(
            REQUESTS.c.name.not_in(req_filter.exclude_request_names))
    if req_filter.include_request_names is not None:
        statement = statement.where(
            REQUESTS.c.name.in_(req_filter.include_request_names))
    if req_filter.finished_before is not None:
        before = _utc_datetime(req_filter.finished_before)
        if req_filter.include_missing_finished_at:
            statement = statement.where(
                sqlalchemy.or_(
                    REQUESTS.c.finished_at < before,
                    sqlalchemy.and_(
                        REQUESTS.c.finished_at.is_(None),
                        REQUESTS.c.status.in_([
                            status.value for status in
                            requests_lib.RequestStatus.finished_status()
                        ]), REQUESTS.c.created_at < before)))
        else:
            statement = statement.where(REQUESTS.c.finished_at < before)
    if req_filter.finished_after is not None:
        after = _utc_datetime(req_filter.finished_after)
        statement = statement.where(
            sqlalchemy.or_(REQUESTS.c.finished_at >= after,
                           REQUESTS.c.finished_at.is_(None)))
    if req_filter.retention_safe:
        statement = statement.where(_request_is_retention_safe())
    if req_filter.sort:
        statement = statement.order_by(REQUESTS.c.created_at.desc())
    if req_filter.limit is not None:
        statement = statement.limit(req_filter.limit)
    return statement


def _controller_claim_is_current() -> sqlalchemy.ColumnElement[bool]:
    """Correlated predicate fencing controller writes by durable generation."""
    current_leadership = sqlalchemy.exists().where(
        _controller_leadership_is_current_predicate(
            REQUESTS.c.worker_instance_id, REQUESTS.c.controller_generation))
    return sqlalchemy.or_(
        REQUESTS.c.execution_class !=
        request_registry.ExecutionClass.CONTROLLER.value,
        sqlalchemy.and_(REQUESTS.c.controller_generation.is_not(None),
                        current_leadership))


def _reserve_controller_action(connection: sqlalchemy.engine.Connection,
                               request: sqlalchemy.engine.RowMapping,
                               instance_id: str,
                               controller_generation: int | None) -> bool:
    """Reserve a non-replayable controller mutation for this generation."""
    if request['execution_class'] != (
            request_registry.ExecutionClass.CONTROLLER.value):
        return True
    registration = request_registry.resolve_handler(request['handler_name'])
    if registration.replay_policy is not request_registry.ReplayPolicy.NEVER:
        return True
    if controller_generation is None:
        return False
    values = {
        'logical_action_id': request['request_id'],
        'resource_identity': request['cluster_name'] or request['request_id'],
        'action_type': request['handler_name'],
        'state': 'reserved',
        'controller_generation': controller_generation,
        'controller_instance_id': uuid.UUID(instance_id),
        'created_at': sqlalchemy.func.clock_timestamp(),
        'updated_at': sqlalchemy.func.clock_timestamp(),
    }
    inserted = connection.execute(
        postgresql.insert(CONTROLLER_ACTION_RESERVATIONS).values(
            **values).on_conflict_do_nothing(index_elements=[
                CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id
            ]).returning(CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id)
    ).scalar_one_or_none()
    if inserted is not None:
        return True
    existing = connection.execute(
        sqlalchemy.select(CONTROLLER_ACTION_RESERVATIONS).where(
            CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id ==
            request['request_id']).with_for_update()).mappings().one()
    return (existing['controller_generation'] == controller_generation and
            str(existing['controller_instance_id']) == instance_id and
            existing['state'] in ('reserved', 'running'))


def _mark_controller_action_state(
    connection: sqlalchemy.engine.Connection,
    request_id: str,
    instance_id: str,
    state: str,
) -> None:
    """Advance this controller generation's optional action reservation."""
    connection.execute(
        sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
            CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id == request_id,
            CONTROLLER_ACTION_RESERVATIONS.c.controller_instance_id ==
            uuid.UUID(instance_id),
            CONTROLLER_ACTION_RESERVATIONS.c.controller_generation ==
            sqlalchemy.select(REQUESTS.c.controller_generation).where(
                REQUESTS.c.request_id == request_id).scalar_subquery(),
            CONTROLLER_ACTION_RESERVATIONS.c.state.in_([
                'reserved', 'running'
            ])).values(state=state,
                       updated_at=sqlalchemy.func.clock_timestamp()))


def _terminalize_locked_request(
    connection: sqlalchemy.engine.Connection,
    request_row: sqlalchemy.engine.RowMapping | dict[str, Any],
    *,
    status: requests_lib.RequestStatus,
    cause: event_api_models.EventCause,
    values: dict[str, Any],
    extra_predicates: tuple[sqlalchemy.ColumnElement[bool], ...] = (),
    delete_queue: bool = True,
) -> bool:
    """Commit one guarded terminal transition and its event atomically.

    The caller must hold a row lock for ``request_row`` in this transaction.
    """
    if status not in requests_lib.RequestStatus.finished_status():
        raise ValueError(f'Not a terminal request status: {status.value}')
    existing_status = requests_lib.RequestStatus(str(request_row['status']))
    if existing_status in requests_lib.RequestStatus.finished_status():
        return False

    terminal_values = dict(values)
    # A claimed executable row with no process identity has not crossed the
    # guarded RUNNING transition.  Because this request row is locked, taking
    # the matching delivery lock makes terminalization and exact quiescence one
    # atomic pre-effect proof.  Centralizing the rule here covers cancellation,
    # dispatcher submission failure, leadership loss, and claim expiry without
    # maintaining parallel terminalization paths.
    if (delete_queue and existing_status
            in requests_lib.RequestStatus.executable_statuses() and
            request_row['pid'] is None and
            request_row['execution_process_start_time_ticks'] is None and
            request_row['claim_token'] is not None and
            request_row['worker_instance_id'] is not None and
            request_row['lease_expires_at'] is not None):
        delivery = connection.execute(
            sqlalchemy.select(
                QUEUE.c.delivery_state, QUEUE.c.claim_generation).where(
                    QUEUE.c.request_id == request_row['request_id']).
            with_for_update()).mappings().one_or_none()
        if (delivery is not None and delivery['delivery_state'] == 'claimed' and
                delivery['claim_generation']
                == request_row['execution_generation']):
            terminal_values.update({
                'execution_quiescence_required': True,
                'execution_quiesced_generation': int(
                    request_row['execution_generation']),
                'execution_quiesced_at': sqlalchemy.func.clock_timestamp(),
            })
    terminal_values['status'] = status.value
    terminal_values['terminal_cause'] = cause.value
    terminal_values['updated_at'] = sqlalchemy.func.clock_timestamp()
    result = connection.execute(
        sqlalchemy.update(REQUESTS).where(
            REQUESTS.c.request_id == request_row['request_id'],
            REQUESTS.c.status.in_([
                active.value
                for active in requests_lib.RequestStatus.active_statuses()
            ]), *extra_predicates).values(**terminal_values))
    if result.rowcount != 1:
        return False

    emission_row = dict(request_row)
    emission_row.update({
        key: value
        for key, value in terminal_values.items()
        if not isinstance(value, sqlalchemy.sql.elements.ClauseElement)
    })
    emission_row['status'] = status.value
    if delete_queue:
        connection.execute(
            sqlalchemy.delete(QUEUE).where(
                QUEUE.c.request_id == request_row['request_id']))
    # Allocate the globally ordered event sequence after all generic request
    # and delivery work, minimizing how long this transaction holds the
    # sequence metadata row lock. Caller-specific reservation updates may
    # still follow in the same short transaction.
    event_emission.emit_terminal_event(connection,
                                       emission_row,
                                       status=status.value,
                                       cause=cause)
    return True


class PostgresRequestBackend(request_storage.RequestBackend):
    """PostgreSQL implementation of request persistence."""

    def __init__(self) -> None:
        request_registry.register_builtin_handlers()
        self._instance_id = ensure_server_instance_id()

    @property
    def uses_durable_queue(self) -> bool:
        return True

    @property
    def claim_heartbeat_interval_seconds(self) -> float:
        return _CLAIM_HEARTBEAT_INTERVAL_SECONDS

    @property
    def instance_id(self) -> str:
        return self._instance_id

    def _fenced_request_predicates(
            self,
            request_id: str) -> tuple[sqlalchemy.ColumnElement[bool], ...]:
        """Return write predicates for the claim in this execution context."""
        claim = request_storage.active_execution_claim()
        if claim is None:
            return ()
        if claim.request_id != request_id:
            return (sqlalchemy.false(),)
        return (
            REQUESTS.c.execution_generation == claim.execution_generation,
            REQUESTS.c.claim_token == uuid.UUID(claim.claim_token),
            REQUESTS.c.worker_instance_id == uuid.UUID(self._instance_id),
            REQUESTS.c.lease_expires_at > sqlalchemy.func.clock_timestamp(),
            _controller_claim_is_current(),
        )

    def _claim_predicates(
            self, execution_generation: int, claim_token: uuid.UUID
    ) -> tuple[sqlalchemy.ColumnElement[bool], ...]:
        return (*self._claim_identity_predicates(execution_generation,
                                                 claim_token),
                REQUESTS.c.lease_expires_at > sqlalchemy.func.clock_timestamp(),
                _controller_claim_is_current())

    def _claim_identity_predicates(
            self, execution_generation: int, claim_token: uuid.UUID
    ) -> tuple[sqlalchemy.ColumnElement[bool], ...]:
        """Match one durable owner without treating lease age as identity."""
        return (
            REQUESTS.c.execution_generation == execution_generation,
            REQUESTS.c.claim_token == claim_token,
            REQUESTS.c.worker_instance_id == uuid.UUID(self._instance_id),
        )

    def _update_event_context(
        self,
        request_id: str,
        update: Callable[[event_models.EventContext],
                         event_models.EventContext],
    ) -> bool:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS.c.event_context).where(
                    REQUESTS.c.request_id == request_id,
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]), *self._fenced_request_predicates(
                        request_id)).with_for_update()).mappings().first()
            if row is None:
                return False
            # Requests written by an older binary during a rolling upgrade
            # intentionally opt out. They must still execute normally.
            if row['event_context'] is None:
                return True
            context = event_models.EventContext.model_validate(
                row['event_context'])
            updated = update(context)
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]), *self._fenced_request_predicates(request_id)).values(
                        event_context=updated.durable_dict(),
                        updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount != 1:
                raise RuntimeError(
                    f'Operational event context update lost its execution '
                    f'fence for {request_id}.')
        return True

    def set_event_workspace(self, request_id: str, workspace: str) -> bool:

        def update(
                context: event_models.EventContext
        ) -> event_models.EventContext:
            if context.workspace is not None and context.workspace != workspace:
                raise RuntimeError(
                    f'Operational event workspace changed for {request_id}.')
            return context.with_workspace(workspace)

        return self._update_event_context(request_id, update)

    def set_event_target_id(self, request_id: str, target_id: str) -> bool:

        def update(
                context: event_models.EventContext
        ) -> event_models.EventContext:
            current = context.targets[0].id
            if current is not None and current != target_id:
                raise RuntimeError(
                    f'Operational event target changed for {request_id}.')
            return context.with_primary_target_id(target_id)

        return self._update_event_context(request_id, update)

    def get_request(
            self,
            request_id: str,
            fields: list[str] | None = None) -> requests_lib.Request | None:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            row = connection.execute(
                _request_projection_statement(fields).where(
                    REQUESTS.c.request_id == request_id)).mappings().first()
        decoder = _request_projection_decoder(fields)
        return decoder(row) if row is not None else None

    async def get_request_async(
            self,
            request_id: str,
            fields: list[str] | None = None) -> requests_lib.Request | None:
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                _request_projection_statement(fields).where(
                    REQUESTS.c.request_id == request_id))
            row = result.mappings().first()
        decoder = _request_projection_decoder(fields)
        return decoder(row) if row is not None else None

    @contextlib.contextmanager
    def update_request(
            self, request_id: str
    ) -> Generator[requests_lib.Request | None, None, None]:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(
                        request_id)).with_for_update()).mappings().first()
            request = _request_from_mapping(row) if row is not None else None
            yield request
            if request is not None:
                values = _request_values_for_db(request)
                values.pop('request_id')
                original_status = requests_lib.RequestStatus(row['status'])
                if (original_status
                        in requests_lib.RequestStatus.active_statuses() and
                        request.status
                        in requests_lib.RequestStatus.finished_status()):
                    if request.terminal_cause is None:
                        raise RuntimeError(
                            'PostgreSQL terminal request updates require a '
                            'closed terminal cause.')
                    _terminalize_locked_request(
                        connection,
                        row,
                        status=request.status,
                        cause=event_api_models.EventCause(
                            request.terminal_cause),
                        values=values,
                        extra_predicates=self._fenced_request_predicates(
                            request_id))
                elif (original_status
                      in requests_lib.RequestStatus.finished_status() and
                      request.status != original_status):
                    raise RuntimeError('A terminal PostgreSQL request cannot '
                                       'be reopened through update_request().')
                else:
                    connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id == request_id,
                            *self._fenced_request_predicates(
                                request_id)).values(**values))

    @contextlib.asynccontextmanager
    async def update_request_async(
            self, request_id: str
    ) -> AsyncGenerator[requests_lib.Request | None, None]:
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            result = await connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(
                        request_id)).with_for_update())
            row = result.mappings().first()
            request = _request_from_mapping(row) if row is not None else None
            yield request
            if request is not None:
                values = _request_values_for_db(request)
                values.pop('request_id')
                original_status = requests_lib.RequestStatus(row['status'])
                if (original_status
                        in requests_lib.RequestStatus.active_statuses() and
                        request.status
                        in requests_lib.RequestStatus.finished_status()):
                    if request.terminal_cause is None:
                        raise RuntimeError(
                            'PostgreSQL terminal request updates require a '
                            'closed terminal cause.')
                    await connection.run_sync(
                        lambda sync_connection: _terminalize_locked_request(
                            sync_connection,
                            row,
                            status=request.status,
                            cause=event_api_models.EventCause(request.
                                                              terminal_cause),
                            values=values,
                            extra_predicates=self._fenced_request_predicates(
                                request_id)))
                elif (original_status
                      in requests_lib.RequestStatus.finished_status() and
                      request.status != original_status):
                    raise RuntimeError('A terminal PostgreSQL request cannot '
                                       'be reopened through update_request().')
                else:
                    await connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id == request_id,
                            *self._fenced_request_predicates(
                                request_id)).values(**values))

    async def create_if_not_exists_async(self,
                                         request: requests_lib.Request) -> bool:
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            return await connection.run_sync(_insert_request_and_queue, request)

    def retire_legacy_internal_daemon_rows(
        self,
        *,
        controller_owner: tuple[str, int] | None = None,
    ) -> int:
        """Retire allowlisted daemon rows under current controller ownership."""
        engine = initialize_and_get_db()
        daemon_ids = sorted(daemons.LEGACY_REQUEST_DAEMON_IDS)
        owner = (controller_owner if controller_owner is not None else
                 _controller_owner_from_environment())
        if owner is None:
            raise RuntimeError('Legacy daemon retirement requires an active '
                               'controller generation.')
        with engine.begin() as connection:
            if not _lock_current_controller_leadership(connection, *owner):
                raise RuntimeError('Controller leadership changed before '
                                   'legacy daemon retirement.')
            connection.execute(
                sqlalchemy.delete(REQUEST_RETENTION_PINS).where(
                    REQUEST_RETENTION_PINS.c.request_id.in_(daemon_ids)))
            connection.execute(
                sqlalchemy.delete(QUEUE).where(
                    QUEUE.c.request_id.in_(daemon_ids)))
            result = connection.execute(
                sqlalchemy.delete(REQUESTS).where(
                    REQUESTS.c.request_id.in_(daemon_ids)))
        if result.rowcount:
            logger.info('Retired '
                        f'{result.rowcount} legacy internal daemon row(s).')
        return int(result.rowcount)

    def query_requests(
        self, req_filter: requests_lib.RequestTaskFilter
    ) -> list[requests_lib.Request]:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            rows = connection.execute(
                _request_filter_statement(req_filter)).mappings().all()
        decoder = _request_projection_decoder(req_filter.fields)
        return [decoder(row) for row in rows]

    async def query_requests_async(
        self, req_filter: requests_lib.RequestTaskFilter
    ) -> list[requests_lib.Request]:
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                _request_filter_statement(req_filter))
            rows = result.mappings().all()
        decoder = _request_projection_decoder(req_filter.fields)
        return [decoder(row) for row in rows]

    async def delete_requests(self, request_ids: list[str]) -> None:
        if not request_ids:
            return
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            await connection.execute(
                sqlalchemy.delete(REQUESTS).where(
                    REQUESTS.c.request_id.in_(request_ids),
                    _request_is_retention_safe()))

    async def gc_request_owned_tombstones(self) -> int:
        """Collect one bounded Serve association batch under request locks."""
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            return await connection.run_sync(
                gc_bound_ordinary_launch_tombstones_in_transaction)

    async def update_status_async(self, request_id: str,
                                  status: requests_lib.RequestStatus) -> None:
        if status in requests_lib.RequestStatus.finished_status():
            raise ValueError('Use a cause-aware terminal transition for '
                             'PostgreSQL requests.')
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            await connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(request_id)).values(
                        status=status.value,
                        updated_at=sqlalchemy.func.clock_timestamp()))

    async def update_status_msg_async(self, request_id: str,
                                      status_msg: str) -> None:
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            await connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(request_id)).values(
                        status_msg=status_msg,
                        updated_at=sqlalchemy.func.clock_timestamp()))

    def try_mark_running(self,
                         request_id: str,
                         pid: int | None,
                         execution_generation: int = 0,
                         claim_token: str | None = None,
                         process_start_time_ticks: int | None = None) -> bool:
        if claim_token is not None:
            if (isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 or
                    process_start_time_ticks is None or
                    isinstance(process_start_time_ticks, bool) or
                    not isinstance(process_start_time_ticks, int) or
                    process_start_time_ticks <= 0):
                raise ValueError('A durable running claim requires positive '
                                 'PID and process start-time identity.')
        elif process_start_time_ticks is not None:
            raise ValueError('An unclaimed request cannot persist durable '
                             'process start-time identity.')
        token = uuid.UUID(claim_token) if claim_token is not None else None
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            # The first read is deliberately only an immutable-origin hint.
            # Authority is acquired below in global order: outer leadership,
            # managed-job row, request row, then queue row.
            snapshot = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id ==
                    request_id)).mappings().one_or_none()
            if snapshot is None:
                return False
            if snapshot['execution_class'] == (
                    request_registry.ExecutionClass.CONTROLLER.value):
                controller_generation = snapshot['controller_generation']
                if (controller_generation is None or
                        not _lock_current_controller_leadership(
                            connection, str(snapshot['worker_instance_id']),
                            int(controller_generation))):
                    return False
            if not _lock_managed_job_origin(
                    connection, snapshot, require_admission=True):
                return False
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id ==
                    request_id).with_for_update()).mappings().one_or_none()
            if row is None or not _same_managed_job_origin(snapshot, row):
                return False
            status = requests_lib.RequestStatus(str(row['status']))
            if status not in requests_lib.RequestStatus.executable_statuses():
                return False
            predicates: list[sqlalchemy.ColumnElement[bool]] = [
                REQUESTS.c.request_id == request_id,
                REQUESTS.c.status == status.value,
            ]
            if token is not None:
                queue_row = connection.execute(
                    sqlalchemy.select(QUEUE).where(
                        QUEUE.c.request_id ==
                        request_id).with_for_update()).mappings().one_or_none()
                if (queue_row is None or
                        queue_row['delivery_state'] != 'claimed' or
                        queue_row['claim_generation'] != execution_generation):
                    return False
                predicates.extend(
                    self._claim_predicates(execution_generation, token))
            else:
                if _managed_job_origin(row) is not None:
                    # Managed-job nested work always uses the disposable
                    # request boundary; an API-process coroutine has no exact
                    # process-family receipt protocol.
                    return False
                if connection.execute(
                        sqlalchemy.select(QUEUE.c.request_id).where(
                            QUEUE.c.request_id == request_id).with_for_update()
                ).scalar_one_or_none() is not None:
                    return False
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(*predicates).values(
                    status=requests_lib.RequestStatus.RUNNING.value,
                    pid=pid,
                    execution_process_start_time_ticks=process_start_time_ticks,
                    status_msg=None,
                    heartbeat_at=sqlalchemy.func.clock_timestamp(),
                    updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount == 1:
                _mark_controller_action_state(connection, request_id,
                                              self._instance_id, 'running')
        return result.rowcount == 1

    def heartbeat_claim(self, claim: request_storage.ExecutionClaim) -> bool:
        """Extend a lease only for its current generation and owner."""
        engine = initialize_and_get_db()
        now = sqlalchemy.func.clock_timestamp()
        claimed_delivery = sqlalchemy.exists().where(
            QUEUE.c.request_id == claim.request_id,
            QUEUE.c.delivery_state == 'claimed',
            QUEUE.c.claim_generation == claim.execution_generation)
        statement = sqlalchemy.update(REQUESTS).where(
            REQUESTS.c.request_id == claim.request_id,
            *self._claim_predicates(claim.execution_generation,
                                    uuid.UUID(claim.claim_token)),
            REQUESTS.c.status.in_([
                status.value
                for status in requests_lib.RequestStatus.active_statuses()
            ]), claimed_delivery).values(
                heartbeat_at=now,
                lease_expires_at=(
                    now + datetime.timedelta(seconds=_CLAIM_LEASE_SECONDS)),
                updated_at=now)
        with engine.begin() as connection:
            result = connection.execute(statement)
        return result.rowcount == 1

    def interrupt_cancelled_claim(
            self, claim: request_storage.ExecutionClaim) -> bool:
        """Interrupt one exact local invocation after durable revocation.

        ``cancel_acknowledged_at`` retains its legacy signal-delivery meaning.
        Only ``acknowledge_execution_quiescence`` can publish proof that the
        handler has stopped running effect-bearing code.
        """
        engine = initialize_and_get_db()
        claim_token = uuid.UUID(claim.claim_token)
        with engine.begin() as connection:
            claimed_delivery = sqlalchemy.exists().where(
                QUEUE.c.request_id == claim.request_id,
                QUEUE.c.delivery_state == 'claimed',
                QUEUE.c.claim_generation == claim.execution_generation)
            revoked_running = sqlalchemy.and_(
                REQUESTS.c.status == requests_lib.RequestStatus.RUNNING.value,
                sqlalchemy.or_(
                    REQUESTS.c.lease_expires_at <=
                    sqlalchemy.func.clock_timestamp(), ~claimed_delivery,
                    ~_controller_claim_is_current()))
            cancelled_unquiesced = sqlalchemy.and_(
                REQUESTS.c.status == requests_lib.RequestStatus.CANCELLED.value,
                REQUESTS.c.cancel_requested_at.is_not(None))
            row = connection.execute(
                sqlalchemy.select(
                    REQUESTS.c.pid,
                    REQUESTS.c.execution_process_start_time_ticks,
                    REQUESTS.c.execution_quiesced_generation,
                    REQUESTS.c.execution_quiesced_at).where(
                        REQUESTS.c.request_id == claim.request_id,
                        *self._claim_identity_predicates(
                            claim.execution_generation, claim_token),
                        REQUESTS.c.execution_quiescence_required,
                        sqlalchemy.or_(
                            revoked_running,
                            cancelled_unquiesced)).with_for_update()).first()
            if row is None:
                return False
            if (row.execution_quiesced_generation == claim.execution_generation
                    and row.execution_quiesced_at is not None):
                return True
            if (row.pid is None or
                    row.execution_process_start_time_ticks is None):
                return False
            pid = int(row.pid)
            if not _signal_exact_executor_process(
                    pid, int(row.execution_process_start_time_ticks),
                    signal.SIGTERM):
                return False
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == claim.request_id,
                    *self._claim_identity_predicates(claim.execution_generation,
                                                     claim_token),
                    REQUESTS.c.execution_quiescence_required).values(
                        cancel_acknowledged_at=(
                            sqlalchemy.func.clock_timestamp()),
                        updated_at=sqlalchemy.func.clock_timestamp()))
        return result.rowcount == 1

    def acknowledge_execution_quiescence(
            self, claim: request_storage.ExecutionClaim) -> bool:
        """Record quiescence for one exact completed execution generation."""
        engine = initialize_and_get_db()
        claim_token = uuid.UUID(claim.claim_token)
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(
                    REQUESTS.c.execution_quiesced_generation,
                    REQUESTS.c.execution_quiesced_at).where(
                        REQUESTS.c.request_id == claim.request_id,
                        REQUESTS.c.execution_generation ==
                        claim.execution_generation,
                        REQUESTS.c.claim_token == claim_token,
                        REQUESTS.c.worker_instance_id == uuid.UUID(
                            self._instance_id),
                        REQUESTS.c.execution_quiescence_required).
                with_for_update()).first()
            if row is None:
                return False
            if (row.execution_quiesced_generation is not None or
                    row.execution_quiesced_at is not None):
                acknowledged = (row.execution_quiesced_generation
                                == claim.execution_generation and
                                row.execution_quiesced_at is not None)
                if not acknowledged:
                    return False
            else:
                # Preserve the complete claim identity as a tombstone until a
                # replay reducer atomically consumes this receipt. NEVER claims
                # retain it until normal request GC removes the terminal row.
                result = connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == claim.request_id,
                        REQUESTS.c.execution_generation ==
                        claim.execution_generation,
                        REQUESTS.c.claim_token == claim_token,
                        REQUESTS.c.worker_instance_id == uuid.UUID(
                            self._instance_id),
                        REQUESTS.c.execution_quiesced_generation.is_(None),
                        REQUESTS.c.execution_quiesced_at.is_(None)).values(
                            execution_quiesced_generation=(
                                claim.execution_generation),
                            execution_quiesced_at=(
                                sqlalchemy.func.clock_timestamp()),
                            updated_at=sqlalchemy.func.clock_timestamp()))
                if result.rowcount != 1:
                    return False
            _requeue_quiesced_replayable_requests(connection,
                                                  request_id=claim.request_id)
            return True

    def quiesce_managed_job_slot_requests(
            self,
            identity: request_storage.ManagedJobControllerSlotIdentity,
            *,
            timeout_seconds: float = 60.0,
            poll_seconds: float = 0.1) -> int:
        """Close one slot's nested admission and await exact receipts."""
        return self._quiesce_managed_job_attempt_requests(
            identity[:2],
            identity,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds)

    def _quiesce_managed_job_attempt_requests(self, authority_owner: tuple[
        str, int], identity: request_storage.ManagedJobControllerSlotIdentity,
                                              *, timeout_seconds: float,
                                              poll_seconds: float) -> int:
        """Quiesce one target attempt under a current outer authority."""
        if (len(identity) != 4 or isinstance(identity[1], bool) or
                not isinstance(identity[1], int) or identity[1] <= 0 or
                isinstance(identity[2], bool) or
                not isinstance(identity[2], int) or identity[2] < 0):
            raise ValueError('Managed-job slot identity is malformed.')
        try:
            instance_id = uuid.UUID(identity[0])
            attempt = uuid.UUID(identity[3])
        except (TypeError, ValueError) as e:
            raise ValueError('Managed-job slot identity is malformed.') from e
        if (not isinstance(timeout_seconds,
                           (int, float)) or isinstance(timeout_seconds, bool) or
                not math.isfinite(timeout_seconds) or timeout_seconds < 0 or
                not isinstance(poll_seconds, (int, float)) or
                isinstance(poll_seconds, bool) or
                not math.isfinite(poll_seconds) or poll_seconds <= 0):
            raise ValueError('Managed-job quiescence timing is invalid.')
        generation = identity[1]
        slot_id = identity[2]
        engine = initialize_and_get_db()
        deadline = time.monotonic() + float(timeout_seconds)
        affected_request_ids: set[str] = set()
        signal_failures: set[str] = set()
        while True:
            signal_targets: list[tuple[str, int, int, int, uuid.UUID,
                                       uuid.UUID]] = []
            pending: list[str] = []
            with engine.begin() as connection:
                if not _lock_current_controller_leadership(
                        connection, authority_owner[0], authority_owner[1]):
                    raise request_storage.ManagedJobRequestQuiescenceError(
                        'Outer controller leadership changed during nested '
                        'request quiescence.')
                job_info = managed_job_state_schema.job_info_table
                job_rows = connection.execute(
                    sqlalchemy.select(job_info.c.spot_job_id).where(
                        job_info.c.controller_instance_id == str(instance_id),
                        job_info.c.controller_generation == generation,
                        job_info.c.controller_slot_id == slot_id,
                        job_info.c.controller_slot_attempt ==
                        str(attempt)).order_by(
                            job_info.c.spot_job_id).with_for_update()).all()
                job_ids = [int(row.spot_job_id) for row in job_rows]
                if job_ids:
                    result = connection.execute(
                        sqlalchemy.update(job_info).where(
                            job_info.c.spot_job_id.in_(job_ids),
                            job_info.c.controller_instance_id == str(
                                instance_id),
                            job_info.c.controller_generation == generation,
                            job_info.c.controller_slot_id == slot_id,
                            job_info.c.controller_slot_attempt == str(attempt)).
                        values(controller_slot_quiescing=True))
                    if result.rowcount != len(job_ids):
                        raise request_storage.ManagedJobRequestQuiescenceError(
                            'Managed-job ownership changed while closing '
                            'nested request admission.')
                rows = connection.execute(
                    sqlalchemy.select(REQUESTS).where(
                        REQUESTS.c.managed_job_controller_instance_id ==
                        instance_id,
                        REQUESTS.c.managed_job_controller_generation ==
                        generation,
                        REQUESTS.c.managed_job_controller_slot_id == slot_id,
                        REQUESTS.c.managed_job_controller_slot_attempt ==
                        attempt).order_by(REQUESTS.c.request_id).
                    with_for_update()).mappings().all()
                for row in rows:
                    request_id = str(row['request_id'])
                    affected_request_ids.add(request_id)
                    status = requests_lib.RequestStatus(str(row['status']))
                    generation_id = int(row['execution_generation'])
                    quiesced = bool(row['execution_quiescence_required'] and
                                    row['execution_quiesced_generation']
                                    == generation_id and
                                    row['execution_quiesced_at'] is not None)
                    if quiesced:
                        connection.execute(
                            sqlalchemy.delete(QUEUE).where(
                                QUEUE.c.request_id == request_id))
                        continue
                    connection.execute(
                        sqlalchemy.select(QUEUE).where(
                            QUEUE.c.request_id == request_id).with_for_update()
                    ).mappings().one_or_none()
                    pre_effect = bool(
                        status
                        in requests_lib.RequestStatus.executable_statuses() and
                        row['pid'] is None and
                        row['execution_process_start_time_ticks'] is None)
                    if pre_effect:
                        now = sqlalchemy.func.clock_timestamp()
                        if not _terminalize_locked_request(
                                connection,
                                row,
                                status=requests_lib.RequestStatus.CANCELLED,
                                cause=(event_api_models.EventCause.
                                       CONTROLLER_LEADERSHIP_LOST),
                                values=
                            {
                                'cancel_requested_at': now,
                                'execution_quiescence_required': True,
                                'execution_quiesced_generation': generation_id,
                                'execution_quiesced_at': now,
                                'should_retry': False,
                                'finished_at': now,
                                'status_msg': _MANAGED_JOB_QUIESCE_REASON,
                                'interrupted_reason': _MANAGED_JOB_QUIESCE_REASON,
                            }):
                            raise request_storage.ManagedJobRequestQuiescenceError(
                                'Lost a locked pre-effect nested request.')
                        continue
                    if status not in requests_lib.RequestStatus.finished_status(
                    ):
                        now = sqlalchemy.func.clock_timestamp()
                        if not _terminalize_locked_request(
                                connection,
                                row,
                                status=requests_lib.RequestStatus.CANCELLED,
                                cause=(event_api_models.EventCause.
                                       CONTROLLER_LEADERSHIP_LOST),
                                values=
                            {
                                'cancel_requested_at': now,
                                'execution_quiescence_required': True,
                                'should_retry': False,
                                'finished_at': now,
                                'status_msg': _MANAGED_JOB_QUIESCE_REASON,
                                'interrupted_reason': _MANAGED_JOB_QUIESCE_REASON,
                            },
                                delete_queue=True):
                            raise request_storage.ManagedJobRequestQuiescenceError(
                                'Lost a locked running nested request.')
                    else:
                        connection.execute(
                            sqlalchemy.update(REQUESTS).where(
                                REQUESTS.c.request_id == request_id,
                                REQUESTS.c.execution_generation == generation_id
                            ).values(
                                cancel_requested_at=sqlalchemy.func.coalesce(
                                    REQUESTS.c.cancel_requested_at,
                                    sqlalchemy.func.clock_timestamp()),
                                execution_quiescence_required=True,
                                should_retry=False,
                                status_msg=_MANAGED_JOB_QUIESCE_REASON,
                                interrupted_reason=_MANAGED_JOB_QUIESCE_REASON,
                                updated_at=sqlalchemy.func.clock_timestamp()))
                    if (row['pid'] is None or
                            row['execution_process_start_time_ticks'] is None or
                            row['claim_token'] is None or
                            row['worker_instance_id'] is None):
                        pending.append(request_id)
                        continue
                    signal_targets.append(
                        (request_id, int(row['pid']),
                         int(row['execution_process_start_time_ticks']),
                         generation_id, uuid.UUID(str(row['claim_token'])),
                         uuid.UUID(str(row['worker_instance_id']))))
                    pending.append(request_id)
            for (request_id, pid, start_ticks, generation_id, claim_token,
                 worker_instance_id) in signal_targets:
                if worker_instance_id != uuid.UUID(self._instance_id):
                    # A remote executor observes the durable CANCELLED row on
                    # its next claim heartbeat and signals its exact local PID.
                    continue
                if not _signal_exact_executor_process(pid, start_ticks,
                                                      signal.SIGTERM):
                    signal_failures.add(request_id)
                    continue
                with engine.begin() as connection:
                    connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id == request_id,
                            REQUESTS.c.execution_generation == generation_id,
                            REQUESTS.c.claim_token == claim_token,
                            REQUESTS.c.worker_instance_id == worker_instance_id,
                        ).values(cancel_acknowledged_at=(
                            sqlalchemy.func.clock_timestamp()),
                                 updated_at=sqlalchemy.func.clock_timestamp()))
            if not pending:
                return len(affected_request_ids)
            if time.monotonic() >= deadline:
                details = ', '.join(sorted(set(pending) | signal_failures))
                raise request_storage.ManagedJobRequestQuiescenceError(
                    'Timed out waiting for exact managed-job nested request '
                    f'quiescence: {details}.')
            time.sleep(float(poll_seconds))

    def quiesce_stale_managed_job_requests(self,
                                           current_owner: tuple[str, int],
                                           *,
                                           timeout_seconds: float = 60.0,
                                           poll_seconds: float = 0.1) -> int:
        """Quiesce every exact nested attempt before stale-job reset."""
        try:
            current_instance = uuid.UUID(current_owner[0])
            current_generation = int(current_owner[1])
        except (IndexError, TypeError, ValueError) as e:
            raise ValueError('Current managed-job owner is malformed.') from e
        if current_generation <= 0:
            raise ValueError('Current managed-job generation must be positive.')
        engine = initialize_and_get_db()
        job_info = managed_job_state_schema.job_info_table
        target_identities: set[
            request_storage.ManagedJobControllerSlotIdentity] = set()
        with engine.begin() as connection:
            if not _lock_current_controller_leadership(
                    connection, str(current_instance), current_generation):
                raise request_storage.ManagedJobRequestQuiescenceError(
                    'Current outer controller leadership is not live.')
            stale_jobs = connection.execute(
                sqlalchemy.select(job_info).where(
                    job_info.c.schedule_state.is_not(None),
                    job_info.c.schedule_state.not_in(('INACTIVE', 'DONE')),
                    sqlalchemy.or_(
                        job_info.c.controller_instance_id.is_(None),
                        job_info.c.controller_generation.is_(None),
                        job_info.c.controller_slot_id.is_(None),
                        job_info.c.controller_slot_attempt.is_(None),
                        job_info.c.controller_instance_id !=
                        str(current_instance),
                        job_info.c.controller_generation != current_generation,
                    )).order_by(job_info.c.spot_job_id).with_for_update()
            ).mappings().all()
            stale_job_ids = [int(row['spot_job_id']) for row in stale_jobs]
            legacy_job_ids: list[int] = []
            for row in stale_jobs:
                try:
                    identity = (managed_job_controller_fencing.
                                persisted_job_attempt_identity(
                                    row, (str(current_instance),
                                          current_generation)))
                except ValueError as e:
                    raise request_storage.ManagedJobRequestQuiescenceError(
                        f'Managed job {row["spot_job_id"]} has unsafe prior '
                        'controller identity.') from e
                if identity is None:
                    legacy_job_ids.append(int(row['spot_job_id']))
                else:
                    target_identities.add(identity)
            if stale_job_ids:
                connection.execute(
                    sqlalchemy.update(job_info).where(
                        job_info.c.spot_job_id.in_(stale_job_ids)).values(
                            controller_slot_quiescing=True))
            if legacy_job_ids:
                correlated_legacy_requests = connection.execute(
                    sqlalchemy.select(
                        REQUESTS.c.request_id, REQUESTS.c.managed_job_id).where(
                            REQUESTS.c.managed_job_id.in_(legacy_job_ids)).
                    order_by(REQUESTS.c.managed_job_id,
                             REQUESTS.c.request_id).with_for_update()).all()
                if correlated_legacy_requests:
                    details = ', '.join(f'{row.managed_job_id}:{row.request_id}'
                                        for row in correlated_legacy_requests)
                    raise request_storage.ManagedJobRequestQuiescenceError(
                        'Cannot adopt pre-slot managed jobs with correlated '
                        f'nested requests: {details}.')
            # Include retained nested tombstones even when their job has
            # already become terminal or a prior failed reset cleared it.
            request_origins = connection.execute(
                sqlalchemy.select(
                    REQUESTS.c.managed_job_controller_instance_id,
                    REQUESTS.c.managed_job_controller_generation,
                    REQUESTS.c.managed_job_controller_slot_id,
                    REQUESTS.c.managed_job_controller_slot_attempt).
                where(
                    REQUESTS.c.managed_job_id.is_not(None),
                    sqlalchemy.or_(
                        REQUESTS.c.managed_job_controller_instance_id.is_(None),
                        REQUESTS.c.managed_job_controller_generation.is_(None),
                        REQUESTS.c.managed_job_controller_slot_id.is_(None),
                        REQUESTS.c.managed_job_controller_slot_attempt.is_(
                            None), REQUESTS.c.managed_job_controller_instance_id
                        != current_instance,
                        REQUESTS.c.managed_job_controller_generation !=
                        current_generation)).distinct()).all()
            for row in request_origins:
                if any(value is None for value in row):
                    raise request_storage.ManagedJobRequestQuiescenceError(
                        'A stale nested request has an incomplete origin.')
                target_identities.add(
                    (str(row.managed_job_controller_instance_id),
                     int(row.managed_job_controller_generation),
                     int(row.managed_job_controller_slot_id),
                     str(row.managed_job_controller_slot_attempt)))
        affected = 0
        deadline = time.monotonic() + float(timeout_seconds)
        for identity in sorted(target_identities):
            remaining = max(0.0, deadline - time.monotonic())
            affected += self._quiesce_managed_job_attempt_requests(
                (str(current_instance), current_generation),
                identity,
                timeout_seconds=remaining,
                poll_seconds=poll_seconds)
        return affected

    def converge_execution_completion(
            self,
            claim: request_storage.ExecutionClaim,
            error: BaseException | None = None,
            terminal_cause: str = 'handler_failed') -> bool:
        """Atomically settle a parent-proven outcome and exact receipt."""
        try:
            cause = event_api_models.EventCause(terminal_cause)
        except ValueError as e:
            raise ValueError('Invalid execution completion cause.') from e
        engine = initialize_and_get_db()
        claim_token = uuid.UUID(claim.claim_token)
        worker_instance_id = uuid.UUID(claim.worker_instance_id or
                                       self._instance_id)
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == claim.request_id,
                    REQUESTS.c.execution_generation ==
                    claim.execution_generation,
                    REQUESTS.c.claim_token == claim_token,
                    REQUESTS.c.worker_instance_id == worker_instance_id).
                with_for_update()).mappings().one_or_none()
            if row is None or not row['execution_quiescence_required']:
                # Exact identity was consumed, superseded, or garbage-collected.
                return False
            status = requests_lib.RequestStatus(str(row['status']))
            if (error is not None and
                    status in requests_lib.RequestStatus.active_statuses()):
                request = _request_from_mapping(row)
                request.status = requests_lib.RequestStatus.FAILED
                request.finished_at = time.time()
                request.set_error(error)
                values = _request_values_for_db(request)
                values.pop('request_id')
                transitioned = _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.FAILED,
                    cause=cause,
                    values=values,
                    extra_predicates=(
                        REQUESTS.c.execution_generation ==
                        claim.execution_generation,
                        REQUESTS.c.claim_token == claim_token,
                        REQUESTS.c.worker_instance_id == worker_instance_id,
                    ))
                if not transitioned:
                    # The row is locked, so losing this exact transition means
                    # the durable identity no longer belongs to this monitor.
                    return False
                _mark_controller_action_state(connection, claim.request_id,
                                              str(worker_instance_id), 'failed')

            receipt = connection.execute(
                sqlalchemy.select(
                    REQUESTS.c.execution_quiesced_generation,
                    REQUESTS.c.execution_quiesced_at).where(
                        REQUESTS.c.request_id == claim.request_id,
                        REQUESTS.c.execution_generation ==
                        claim.execution_generation,
                        REQUESTS.c.claim_token == claim_token,
                        REQUESTS.c.worker_instance_id == worker_instance_id,
                        REQUESTS.c.execution_quiescence_required).
                with_for_update()).first()
            if receipt is None:
                return False
            if (receipt.execution_quiesced_generation is not None or
                    receipt.execution_quiesced_at is not None):
                if (receipt.execution_quiesced_generation !=
                        claim.execution_generation or
                        receipt.execution_quiesced_at is None):
                    return False
            else:
                result = connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == claim.request_id,
                        REQUESTS.c.execution_generation ==
                        claim.execution_generation,
                        REQUESTS.c.claim_token == claim_token,
                        REQUESTS.c.worker_instance_id == worker_instance_id,
                        REQUESTS.c.execution_quiesced_generation.is_(None),
                        REQUESTS.c.execution_quiesced_at.is_(None)).values(
                            execution_quiesced_generation=(
                                claim.execution_generation),
                            execution_quiesced_at=(
                                sqlalchemy.func.clock_timestamp()),
                            updated_at=sqlalchemy.func.clock_timestamp()))
                if result.rowcount != 1:
                    return False
            _requeue_quiesced_replayable_requests(connection,
                                                  request_id=claim.request_id)
            return True

    def handoff_execution_retry(self, claim: request_storage.ExecutionClaim,
                                status_msg: str,
                                retry_wait_seconds: float) -> bool:
        """Atomically consume one exact family proof into delayed delivery."""
        if (not isinstance(retry_wait_seconds, (int, float)) or
                isinstance(retry_wait_seconds, bool) or
                not math.isfinite(retry_wait_seconds) or
                retry_wait_seconds < 0):
            raise ValueError(
                'Retry delay must be a finite non-negative number.')
        claim_token = uuid.UUID(claim.claim_token)
        worker_instance_id = uuid.UUID(claim.worker_instance_id or
                                       self._instance_id)
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            preview = connection.execute(
                sqlalchemy.select(
                    REQUESTS.c.execution_class,
                    REQUESTS.c.controller_generation,
                    *(REQUESTS.c[field]
                      for field in _MANAGED_JOB_ORIGIN_FIELDS)).where(
                          REQUESTS.c.request_id == claim.request_id,
                          REQUESTS.c.execution_generation ==
                          claim.execution_generation,
                          REQUESTS.c.claim_token == claim_token,
                          REQUESTS.c.worker_instance_id ==
                          worker_instance_id)).mappings().one_or_none()
            if preview is None:
                return False
            if preview['execution_class'] == (
                    request_registry.ExecutionClass.CONTROLLER.value):
                controller_generation = preview['controller_generation']
                if (controller_generation is None or
                        not _lock_current_controller_leadership(
                            connection, str(worker_instance_id),
                            int(controller_generation))):
                    return False
            if not _lock_managed_job_origin(
                    connection, preview, require_admission=True):
                return False
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == claim.request_id,
                    REQUESTS.c.execution_generation ==
                    claim.execution_generation,
                    REQUESTS.c.claim_token == claim_token,
                    REQUESTS.c.worker_instance_id == worker_instance_id,
                    REQUESTS.c.execution_quiescence_required).with_for_update()
            ).mappings().one_or_none()
            if row is None or not _same_managed_job_origin(preview, row):
                return False
            delivery = connection.execute(
                sqlalchemy.select(QUEUE).where(
                    QUEUE.c.request_id == claim.request_id).with_for_update()
            ).mappings().one_or_none()
            if (requests_lib.RequestStatus(str(
                    row['status'])) != requests_lib.RequestStatus.RUNNING or
                    row['cancel_requested_at'] is not None or
                    delivery is None or
                    delivery['delivery_state'] != 'claimed' or
                    delivery['claim_generation'] != claim.execution_generation):
                return False
            # Exact identity plus the parent-observed Future result is the
            # authority to consume this receipt.  Lease age is deliberately
            # irrelevant: a retry delay equal to the lease must not strand a
            # completed generation before this transaction can run.
            if (row['execution_class'] != preview['execution_class'] or
                    row['controller_generation'] !=
                    preview['controller_generation']):
                return False
            now = sqlalchemy.func.clock_timestamp()
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == claim.request_id, REQUESTS.c.status
                    == requests_lib.RequestStatus.RUNNING.value,
                    REQUESTS.c.execution_generation ==
                    claim.execution_generation,
                    REQUESTS.c.claim_token == claim_token,
                    REQUESTS.c.worker_instance_id == worker_instance_id,
                    REQUESTS.c.execution_quiescence_required,
                    REQUESTS.c.cancel_requested_at.is_(None)).values(
                        status=requests_lib.RequestStatus.WAITING.value,
                        terminal_cause=None,
                        return_value=None,
                        error=None,
                        pid=None,
                        execution_process_start_time_ticks=None,
                        claim_token=None,
                        worker_instance_id=None,
                        controller_generation=None,
                        lease_expires_at=None,
                        heartbeat_at=None,
                        cancel_requested_at=None,
                        cancel_acknowledged_at=None,
                        execution_quiescence_required=False,
                        execution_quiesced_generation=None,
                        execution_quiesced_at=None,
                        should_retry=False,
                        finished_at=None,
                        status_msg=status_msg,
                        interrupted_reason=None,
                        updated_at=now))
            if result.rowcount != 1:
                return False
            delivery_result = connection.execute(
                sqlalchemy.update(QUEUE).where(
                    QUEUE.c.request_id == claim.request_id,
                    QUEUE.c.delivery_state == 'claimed',
                    QUEUE.c.claim_generation == claim.execution_generation).
                values(
                    delivery_state='queued',
                    claim_generation=None,
                    available_at=(
                        now +
                        datetime.timedelta(seconds=float(retry_wait_seconds))),
                    updated_at=now))
            if delivery_result.rowcount != 1:
                raise RuntimeError('Retry handoff lost its locked delivery.')
            return True

    def interrupt_request_for_shutdown_retry(self, request_id: str) -> bool:
        """Record retry intent, then signal only the exact Linux process."""
        engine = initialize_and_get_db()
        signal_target: tuple[int, int,
                             request_storage.ExecutionClaim] | None = (None)
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id ==
                    request_id).with_for_update()).mappings().one_or_none()
            if row is None:
                return False
            status = requests_lib.RequestStatus(str(row['status']))
            if status in requests_lib.RequestStatus.finished_status():
                return False
            delivery = connection.execute(
                sqlalchemy.select(QUEUE).where(
                    QUEUE.c.request_id ==
                    request_id).with_for_update()).mappings().one_or_none()
            generation = int(row['execution_generation'])
            queued_pre_effect = bool(
                status in requests_lib.RequestStatus.executable_statuses() and
                row['pid'] is None and delivery is not None and
                ((delivery['delivery_state'] == 'queued' and
                  delivery['claim_generation'] is None) or
                 (delivery['delivery_state'] == 'claimed' and
                  delivery['claim_generation'] == generation)))
            now = sqlalchemy.func.clock_timestamp()
            if queued_pre_effect:
                return _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=(event_api_models.EventCause.GRACEFUL_SHUTDOWN_RETRY),
                    values={
                        'cancel_requested_at': now,
                        'execution_quiescence_required': True,
                        'execution_quiesced_generation': generation,
                        'execution_quiesced_at': now,
                        'should_retry': True,
                        'finished_at': now,
                    })
            if (status is not requests_lib.RequestStatus.RUNNING or
                    delivery is None or
                    delivery['delivery_state'] != 'claimed' or
                    delivery['claim_generation'] != generation or
                    row['pid'] is None or
                    row['execution_process_start_time_ticks'] is None or
                    row['claim_token'] is None or
                    str(row['worker_instance_id']) != self._instance_id):
                return False
            claim = request_storage.ExecutionClaim(
                request_id, generation, str(row['claim_token']),
                str(row['worker_instance_id']))
            connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    REQUESTS.c.execution_generation == generation,
                    REQUESTS.c.claim_token == row['claim_token'],
                    REQUESTS.c.worker_instance_id ==
                    row['worker_instance_id']).values(
                        cancel_requested_at=sqlalchemy.func.coalesce(
                            REQUESTS.c.cancel_requested_at, now),
                        execution_quiescence_required=True,
                        should_retry=False,
                        status_msg=('Graceful shutdown is waiting for exact '
                                    'execution quiescence'),
                        interrupted_reason=_GRACEFUL_SHUTDOWN_RETRY_REASON,
                        updated_at=now))
            signal_target = (int(row['pid']),
                             int(row['execution_process_start_time_ticks']),
                             claim)
        assert signal_target is not None
        pid, process_start_time_ticks, claim = signal_target
        if not _signal_exact_executor_process(pid, process_start_time_ticks,
                                              signal.SIGTERM):
            return False
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    REQUESTS.c.execution_generation == generation,
                    REQUESTS.c.claim_token == uuid.UUID(claim.claim_token),
                    REQUESTS.c.worker_instance_id == uuid.UUID(
                        self._instance_id), REQUESTS.c.interrupted_reason ==
                    _GRACEFUL_SHUTDOWN_RETRY_REASON).values(
                        cancel_acknowledged_at=(
                            sqlalchemy.func.clock_timestamp()),
                        updated_at=sqlalchemy.func.clock_timestamp()))
        return True

    def set_request_finished(self,
                             request_id: str,
                             status: requests_lib.RequestStatus,
                             error: BaseException | None = None,
                             result: Any | None = None) -> bool:
        if status == requests_lib.RequestStatus.SUCCEEDED:
            cause = event_api_models.EventCause.HANDLER_SUCCEEDED
        elif status == requests_lib.RequestStatus.FAILED:
            cause = event_api_models.EventCause.HANDLER_FAILED
        else:
            cause = event_api_models.EventCause.EXPLICIT_CANCEL
        return self.transition_request_terminal(request_id,
                                                status,
                                                cause.value,
                                                error=error,
                                                result=result)

    def transition_request_terminal(self,
                                    request_id: str,
                                    status: requests_lib.RequestStatus,
                                    cause: str,
                                    error: BaseException | None = None,
                                    result: Any | None = None) -> bool:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(
                        request_id)).with_for_update()).mappings().first()
            if row is None:
                return False
            request = _request_from_mapping(row)
            if request.status in requests_lib.RequestStatus.finished_status():
                return False
            request.status = status
            request.finished_at = time.time()
            if error is not None:
                request.set_error(error)
            if result is not None:
                try:
                    request.set_return_value(result)
                except Exception as encoding_error:  # pylint: disable=broad-except
                    logger.error(
                        f'Failed to encode return value for request '
                        f'{request_id} ({request.name}); marking the request '
                        'failed.',
                        exc_info=True)
                    status = requests_lib.RequestStatus.FAILED
                    cause = event_api_models.EventCause.HANDLER_FAILED.value
                    request.status = status
                    request.return_value = None
                    request.set_error(encoding_error)
            values = _request_values_for_db(request)
            values.pop('request_id')
            transitioned = _terminalize_locked_request(
                connection,
                row,
                status=status,
                cause=event_api_models.EventCause(cause),
                values=values,
                extra_predicates=self._fenced_request_predicates(request_id))
            if transitioned:
                action_state = ('completed' if status
                                == requests_lib.RequestStatus.SUCCEEDED else
                                'failed')
                _mark_controller_action_state(connection, request_id,
                                              self._instance_id, action_state)
                return True
            return False

    async def set_request_finished_async(self,
                                         request_id: str,
                                         status: requests_lib.RequestStatus,
                                         error: BaseException | None = None,
                                         result: Any | None = None) -> bool:
        return await asyncio.to_thread(self.set_request_finished, request_id,
                                       status, error, result)

    async def transition_request_terminal_async(
            self,
            request_id: str,
            status: requests_lib.RequestStatus,
            cause: str,
            error: BaseException | None = None,
            result: Any | None = None) -> bool:
        return await asyncio.to_thread(self.transition_request_terminal,
                                       request_id, status, cause, error, result)

    def kill_requests(self,
                      request_ids: list[str] | None = None,
                      user_id: str | None = None) -> list[str]:
        engine = initialize_and_get_db()
        active = [
            status.value
            for status in requests_lib.RequestStatus.active_statuses()
        ]
        statement = sqlalchemy.select(REQUESTS).where(
            REQUESTS.c.status.in_(active),
            REQUESTS.c.name != 'sky.api_cancel',
            # Correlated bound launches require Serve association cancellation
            # and request terminalization in one transaction.  Generic API
            # cancellation cannot safely invent or omit that durable intent.
            REQUESTS.c.ordinary_launch_association_id.is_(None))
        if request_ids is not None:
            statement = statement.where(REQUESTS.c.request_id.in_(request_ids))
        if user_id is not None:
            statement = statement.where(REQUESTS.c.user_id == user_id)
        cancelled: list[str] = []
        now = sqlalchemy.func.clock_timestamp()
        with engine.begin() as connection:
            rows = connection.execute(
                statement.with_for_update()).mappings().all()
            delivery_rows = connection.execute(
                sqlalchemy.select(QUEUE.c.request_id, QUEUE.c.delivery_state,
                                  QUEUE.c.claim_generation).where(
                                      QUEUE.c.request_id.in_([
                                          row['request_id'] for row in rows
                                      ]))).mappings().all()
            deliveries = {row['request_id']: row for row in delivery_rows}
            for row in rows:
                delivery = deliveries.get(row['request_id'])
                matching_delivery = (delivery is not None and (
                    (delivery['delivery_state'] == 'queued' and
                     delivery['claim_generation'] is None) or
                    (delivery['delivery_state'] == 'claimed' and
                     delivery['claim_generation']
                     == row['execution_generation'])))
                has_quiescence_contract = bool(
                    row['execution_quiescence_required'] or matching_delivery or
                    row['claim_token'] is not None)
                execution_is_quiescent = bool(
                    (row['status'] == requests_lib.RequestStatus.PENDING.value
                     and matching_delivery) or
                    (row['execution_quiesced_generation']
                     == row['execution_generation'] and
                     row['execution_quiesced_at'] is not None))
                terminal_values: dict[str, Any] = {
                    'cancel_requested_at': now,
                    'execution_quiescence_required': has_quiescence_contract,
                    'finished_at': now,
                }
                if execution_is_quiescent:
                    terminal_values.update({
                        'execution_quiesced_generation': int(
                            row['execution_generation']),
                        'execution_quiesced_at': now,
                    })
                transitioned = _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=event_api_models.EventCause.EXPLICIT_CANCEL,
                    values=terminal_values)
                if not transitioned:
                    continue
                cancelled.append(row['request_id'])
                connection.execute(
                    sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
                        CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id ==
                        row['request_id'],
                        CONTROLLER_ACTION_RESERVATIONS.c.state.in_(
                            ['reserved',
                             'running'])).values(state='ambiguous',
                                                 reconciliation_at=now,
                                                 updated_at=now))
                if (not execution_is_quiescent and row['pid'] is not None and
                        row['execution_process_start_time_ticks'] is not None
                        and row['claim_token'] is not None and
                        str(row['worker_instance_id']) == self._instance_id and
                        row['lease_expires_at'] is not None):
                    if not _signal_exact_executor_process(
                            int(row['pid']),
                            int(row['execution_process_start_time_ticks']),
                            signal.SIGTERM):
                        continue
                    connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id == row['request_id'],
                            REQUESTS.c.execution_generation ==
                            row['execution_generation'],
                            REQUESTS.c.claim_token == row['claim_token'],
                            REQUESTS.c.worker_instance_id ==
                            row['worker_instance_id']).values(
                                cancel_acknowledged_at=(
                                    sqlalchemy.func.clock_timestamp()),
                                updated_at=sqlalchemy.func.clock_timestamp()))
        return cancelled

    async def kill_request_async(self, request_id: str) -> bool:
        return bool(await asyncio.to_thread(self.kill_requests, [request_id],
                                            None))

    async def get_latest_request_id_async(self) -> str | None:
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                sqlalchemy.select(REQUESTS.c.request_id).order_by(
                    REQUESTS.c.created_at.desc()).limit(1))
            return result.scalar_one_or_none()

    def get_requests_with_prefix(self,
                                 request_id_prefix: str,
                                 fields: list[str] | None = None
                                ) -> list[requests_lib.Request] | None:
        engine = initialize_and_get_db()
        escaped = db_utils.glob_to_like_pattern(request_id_prefix) + '%'
        with engine.connect() as connection:
            rows = connection.execute(
                _request_projection_statement(fields).where(
                    REQUESTS.c.request_id.like(
                        escaped,
                        escape=db_utils.LIKE_ESCAPE_CHAR))).mappings().all()
        decoder = _request_projection_decoder(fields)
        return ([decoder(row) for row in rows] if rows else None)

    async def get_requests_async_with_prefix(
            self,
            request_id_prefix: str,
            fields: list[str] | None = None
    ) -> list[requests_lib.Request] | None:
        return await asyncio.to_thread(self.get_requests_with_prefix,
                                       request_id_prefix, fields)

    async def get_request_status_async(
            self,
            request_id: str,
            include_msg: bool = False) -> requests_lib.StatusWithMsg | None:
        columns = [REQUESTS.c.status]
        if include_msg:
            columns.append(REQUESTS.c.status_msg)
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                sqlalchemy.select(*columns).where(
                    REQUESTS.c.request_id == request_id))
            row = result.first()
        if row is None:
            return None
        return requests_lib.StatusWithMsg(requests_lib.RequestStatus(row[0]),
                                          row[1] if include_msg else None)

    async def get_api_request_ids_start_with(self,
                                             incomplete: str) -> list[str]:
        engine = await _get_async_engine()
        pattern = db_utils.glob_to_like_pattern(incomplete) + '%'
        active_first = sqlalchemy.case(
            (REQUESTS.c.status.in_(['PENDING', 'WAITING', 'RUNNING']), 0),
            else_=1)
        async with engine.connect() as connection:
            result = await connection.execute(
                sqlalchemy.select(REQUESTS.c.request_id).where(
                    REQUESTS.c.request_id.like(
                        pattern, escape=db_utils.LIKE_ESCAPE_CHAR)).order_by(
                            active_first,
                            REQUESTS.c.created_at.desc()).limit(1000))
            return list(result.scalars())

    def get_active_file_mounts_blob_ids(self) -> set[str]:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            return set(
                connection.execute(
                    sqlalchemy.select(
                        REQUESTS.c.file_mounts_blob_id).distinct().where(
                            REQUESTS.c.status.in_([
                                status.value for status in
                                requests_lib.RequestStatus.active_statuses()
                            ]), REQUESTS.c.file_mounts_blob_id.is_not(
                                None))).scalars())

    def get_shutdown_active_requests(self) -> list[tuple[str, str]]:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            return [(str(row[0]), str(row[1])) for row in connection.execute(
                sqlalchemy.select(REQUESTS.c.request_id, REQUESTS.c.name).where(
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]))).all()]

    def reset_on_startup(self) -> None:
        initialize_and_get_db()

    def recover_on_startup(
        self,
        *,
        controller_owner: tuple[str, int] | None = None,
    ) -> bool:
        """Validate durable state without wiping or re-enqueueing rows."""
        owner = (controller_owner if controller_owner is not None else
                 _controller_owner_from_environment())
        if owner is None:
            raise RuntimeError('Request recovery requires an active '
                               'controller generation.')
        engine = initialize_and_get_db()
        active = [
            status.value
            for status in requests_lib.RequestStatus.active_statuses()
        ]
        with engine.begin() as connection:
            if not _lock_current_controller_leadership(connection, *owner):
                raise RuntimeError('Controller leadership changed before '
                                   'request recovery.')
            # Coroutine requests intentionally have no queue delivery. They
            # cannot survive loss of the all-role compatibility process, so
            # preserve the row and surface a retry instead of silently
            # manufacturing a queue message with different execution
            # semantics.
            rows = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.status.in_(active), ~sqlalchemy.exists().where(
                        QUEUE.c.request_id == REQUESTS.c.request_id)).
                with_for_update()).mappings().all()
            for row in rows:
                now = sqlalchemy.func.clock_timestamp()
                _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=event_api_models.EventCause.COMPATIBILITY_RESTART,
                    values={
                        'should_retry': True,
                        'finished_at': now,
                        'interrupted_reason':
                            ('The compatibility process stopped while a '
                             'non-queued coroutine request was active.'),
                    })
        # Queue rows are already durable and must not be copied back through
        # the legacy in-memory re-enqueue path.
        return False


class PostgresQueueBackend(queue_base.QueueBackend):
    """One schedule-class view over the durable PostgreSQL queue."""

    def __init__(
        self,
        schedule_type: str,
        *,
        execution_classes: frozenset[str] | None = None,
        controller_generation: int | None = None,
        supported_handler_names: frozenset[str] | None = None,
    ):
        self._schedule_type = schedule_type
        self._instance_id = ensure_server_instance_id()
        valid_classes = frozenset(
            execution_class.value
            for execution_class in request_registry.ExecutionClass)
        if execution_classes is not None and not execution_classes:
            raise ValueError('At least one execution class must be allowed.')
        if (execution_classes is not None and
                not execution_classes.issubset(valid_classes)):
            raise ValueError('Invalid queue execution classes: '
                             f'{sorted(execution_classes - valid_classes)}')
        if (execution_classes is not None and
                request_registry.ExecutionClass.CONTROLLER.value
                in execution_classes and controller_generation is None):
            raise ValueError('A controller-scoped queue requires an active '
                             'controller generation.')
        self._execution_classes = execution_classes
        self._controller_generation = controller_generation
        role = os.environ.get(SERVER_ROLE_ENV_VAR, 'all')
        self._supported_handler_names = (frozenset(_supported_handlers(role))
                                         if supported_handler_names is None else
                                         supported_handler_names)

    def _role_predicates(self) -> tuple[sqlalchemy.ColumnElement[bool], ...]:
        # Handler filtering is repeated in candidate selection, locked claim,
        # and the guarded request UPDATE because observing a queue row never
        # grants execution authority by itself.
        predicates: list[sqlalchemy.ColumnElement[bool]] = [
            REQUESTS.c.handler_name.in_(self._supported_handler_names)
        ]
        if self._execution_classes is not None:
            predicates.append(
                REQUESTS.c.execution_class.in_(self._execution_classes))
        controller_class = request_registry.ExecutionClass.CONTROLLER.value
        if self._controller_generation is None:
            # An unscoped queue without outer authority remains usable for
            # normal work, but is never capable of admitting controller work.
            predicates.append(REQUESTS.c.execution_class != controller_class)
        else:
            predicates.append(
                sqlalchemy.or_(
                    REQUESTS.c.execution_class != controller_class,
                    sqlalchemy.exists().where(
                        _controller_leadership_is_current_predicate(
                            uuid.UUID(self._instance_id),
                            self._controller_generation))))
        return tuple(predicates)

    def _lock_controller_leadership(
            self, connection: sqlalchemy.engine.Connection) -> bool:
        if self._controller_generation is None:
            return True
        return _lock_current_controller_leadership(connection,
                                                   self._instance_id,
                                                   self._controller_generation)

    def put(self, item: queue_base.QueueItemLike) -> None:
        normalized = queue_base.normalize_queue_item(item)
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            if not self._lock_controller_leadership(connection):
                logger.warning('Ignoring requeue after controller leadership '
                               f'changed for {normalized.request_id}.')
                return
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == normalized.request_id,
                    *self._role_predicates()).with_for_update()).mappings(
                    ).first()
            if row is None:
                return
            queue_row = connection.execute(
                sqlalchemy.select(QUEUE).where(
                    QUEUE.c.request_id == normalized.request_id).
                with_for_update()).mappings().first()
            if queue_row is None:
                if row['status'] not in ('PENDING', 'WAITING'):
                    return
                connection.execute(
                    postgresql.insert(QUEUE).values(
                        request_id=normalized.request_id,
                        schedule_type=row['schedule_type'],
                        priority=0,
                        available_at=sqlalchemy.func.clock_timestamp(),
                        enqueued_at=sqlalchemy.func.clock_timestamp(),
                        ignore_return_value=normalized.ignore_return_value,
                        retryable=normalized.retryable,
                        precondition_type=None,
                        precondition_payload=None,
                        precondition_deadline=None,
                        precondition_attempts=0,
                        delivery_state='queued',
                        claim_generation=None,
                        updated_at=sqlalchemy.func.clock_timestamp()))
                return
            if queue_row['delivery_state'] == 'queued':
                return
            if (normalized.claim_token is None or row['claim_token'] is None or
                    str(row['claim_token']) != normalized.claim_token or
                    int(row['execution_generation']) !=
                    normalized.execution_generation or
                    str(row['worker_instance_id']) != self._instance_id):
                logger.warning('Ignoring stale requeue for request '
                               f'{normalized.request_id}.')
                return
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == normalized.request_id,
                    REQUESTS.c.execution_generation ==
                    normalized.execution_generation,
                    REQUESTS.c.claim_token == uuid.UUID(normalized.claim_token),
                    REQUESTS.c.worker_instance_id == uuid.UUID(
                        self._instance_id), REQUESTS.c.lease_expires_at >
                    sqlalchemy.func.clock_timestamp(),
                    _controller_claim_is_current()).values(
                        status=requests_lib.RequestStatus.WAITING.value,
                        pid=None,
                        execution_process_start_time_ticks=None,
                        claim_token=None,
                        worker_instance_id=None,
                        controller_generation=None,
                        lease_expires_at=None,
                        heartbeat_at=None,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount != 1:
                logger.warning('Ignoring expired requeue for request '
                               f'{normalized.request_id}.')
                return
            connection.execute(
                sqlalchemy.update(QUEUE).where(
                    QUEUE.c.request_id == normalized.request_id).values(
                        delivery_state='queued',
                        claim_generation=None,
                        available_at=sqlalchemy.func.clock_timestamp(),
                        updated_at=sqlalchemy.func.clock_timestamp()))

    async def put_async(self, item: queue_base.QueueItemLike) -> None:
        await asyncio.to_thread(self.put, item)

    def _reap_expired_claims(self,
                             connection: sqlalchemy.engine.Connection) -> None:
        now = sqlalchemy.func.clock_timestamp()
        rows = connection.execute(
            sqlalchemy.select(REQUESTS, QUEUE).
            join(QUEUE, QUEUE.c.request_id == REQUESTS.c.request_id).where(
                QUEUE.c.schedule_type == self._schedule_type,
                QUEUE.c.delivery_state == 'claimed',
                # The association-aware reducer is the sole expiry
                # authority for bound ordinary launches. It knows whether
                # the durable effect fence was crossed; the generic reaper
                # cannot safely distinguish pre-effect from ambiguous I/O.
                REQUESTS.c.ordinary_launch_association_id.is_(None),
                REQUESTS.c.status.in_([
                    status.value
                    for status in requests_lib.RequestStatus.active_statuses()
                ]),
                REQUESTS.c.cancel_requested_at.is_(None),
                REQUESTS.c.lease_expires_at < sqlalchemy.func.clock_timestamp(),
                *self._role_predicates()).order_by(
                    REQUESTS.c.lease_expires_at, QUEUE.c.sequence).limit(
                        _MAX_EXPIRED_CLAIMS_PER_SWEEP).with_for_update(
                            skip_locked=True)).mappings().all()
        for row in rows:
            try:
                registration = request_registry.resolve_handler(
                    row['handler_name'])
            except ValueError:
                replayable = False
            else:
                replayable = registration.replay_policy in (
                    request_registry.ReplayPolicy.READ_ONLY,
                    request_registry.ReplayPolicy.RECONCILE)
            if replayable:
                if row['pid'] is None:
                    if _requeue_locked_pre_effect_claim(
                            connection,
                            row,
                            status_msg=(
                                'Execution lease expired before execution '
                                'admission; reconciling'),
                            interrupted_reason=(
                                'Execution lease expired before the exact claim '
                                'crossed its guarded RUNNING transition.')):
                        continue
                connection.execute(
                    sqlalchemy.update(REQUESTS).
                    where(REQUESTS.c.request_id == row['request_id']).values(
                        # Expiry revokes the generation; it is not stop
                        # proof. Keep the claimed delivery and exact owner
                        # tuple until its wrapper publishes quiescence.
                        cancel_requested_at=sqlalchemy.func.coalesce(
                            REQUESTS.c.cancel_requested_at, now),
                        execution_quiescence_required=True,
                        should_retry=True,
                        status_msg=('Execution lease expired; waiting for '
                                    'execution quiescence'),
                        interrupted_reason=(
                            'Execution lease expired; replay is deferred '
                            'until the exact execution stops.'),
                        updated_at=now))
            else:
                terminal_values: dict[str, Any] = {
                    # Lease expiry revokes mutation authority but is not
                    # evidence that the executor or its children stopped.
                    # Retain the complete claim tombstone even after the
                    # wrapper publishes generation-bound quiescence: the
                    # claim schema makes token/worker/lease atomic, and row
                    # retention is the one canonical identity cleanup path.
                    'cancel_requested_at': now,
                    'execution_quiescence_required': True,
                    'should_retry': True,
                    'finished_at': now,
                    'interrupted_reason':
                        ('Execution lease expired with an ambiguous mutating '
                         'outcome.'),
                }
                transitioned = _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=(event_api_models.EventCause.EXECUTION_LEASE_EXPIRED),
                    values=terminal_values)
                if not transitioned:
                    continue
                if row['execution_class'] == (
                        request_registry.ExecutionClass.CONTROLLER.value):
                    connection.execute(
                        sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
                            CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id
                            == row['request_id'],
                            CONTROLLER_ACTION_RESERVATIONS.c.state.in_(
                                ['reserved',
                                 'running'])).values(state='ambiguous',
                                                     reconciliation_at=now,
                                                     updated_at=now))
        _requeue_quiesced_replayable_requests(connection)

    @staticmethod
    def _provider_mutation_predicate(
            provider_mutation: bool) -> sqlalchemy.ColumnElement[bool]:
        if provider_mutation:
            return REQUESTS.c.handler_name.in_(_PROVIDER_MUTATION_HANDLER_NAMES)
        return REQUESTS.c.handler_name.not_in(_PROVIDER_MUTATION_HANDLER_NAMES)

    def _candidate(
        self,
        connection: sqlalchemy.engine.Connection,
        *,
        provider_mutation: bool,
        request_id: str | None = None,
    ) -> sqlalchemy.engine.RowMapping | None:
        statement = sqlalchemy.select(
            QUEUE, REQUESTS.c.execution_class, REQUESTS.c.handler_name,
            *(REQUESTS.c[field] for field in _MANAGED_JOB_ORIGIN_FIELDS),
            sqlalchemy.func.clock_timestamp().label('_database_now')).join(
                REQUESTS, REQUESTS.c.request_id == QUEUE.c.request_id).where(
                    QUEUE.c.schedule_type == self._schedule_type,
                    QUEUE.c.delivery_state == 'queued',
                    QUEUE.c.available_at <= sqlalchemy.func.clock_timestamp(),
                    self._provider_mutation_predicate(provider_mutation),
                    *self._role_predicates())
        if request_id is not None:
            statement = statement.where(QUEUE.c.request_id == request_id)
        return connection.execute(
            statement.order_by(QUEUE.c.priority.desc(),
                               QUEUE.c.sequence).limit(1)).mappings().first()

    def _claim_candidate(
        self,
        candidate: sqlalchemy.engine.RowMapping,
        *,
        provider_mutation: bool,
    ) -> queue_base.QueueItem | None:
        engine = initialize_and_get_db()
        met = True
        status_msg = None
        precondition_error: BaseException | None = None
        deadline = candidate['precondition_deadline']
        if deadline is not None and deadline <= candidate['_database_now']:
            precondition_error = TimeoutError(
                f'Request {candidate["request_id"]} precondition timed out.')
        elif candidate['precondition_type'] is not None:
            try:
                met, status_msg = preconditions.check_once(
                    candidate['precondition_type'],
                    candidate['precondition_payload'], candidate['request_id'])
            except Exception as e:  # pylint: disable=broad-exception-caught
                precondition_error = e

        with engine.begin() as connection:
            if not self._lock_controller_leadership(connection):
                return None
            managed_job_origin_current = _lock_managed_job_origin(
                connection, candidate, require_admission=True)
            # `_lock_controller_leadership()` above acquired any controller
            # outer lock before this managed-job outer/job prefix.  No outer
            # lock may be upgraded after the job row is locked.
            request_row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == candidate['request_id'],
                    self._provider_mutation_predicate(provider_mutation),
                    *self._role_predicates()).with_for_update(
                        skip_locked=True)).mappings().first()
            if request_row is None:
                return None
            queue_row = connection.execute(
                sqlalchemy.select(QUEUE).where(
                    QUEUE.c.request_id == candidate['request_id'],
                    QUEUE.c.delivery_state == 'queued', QUEUE.c.available_at <=
                    sqlalchemy.func.clock_timestamp()).with_for_update(
                        skip_locked=True)).mappings().first()
            if queue_row is None:
                return None
            locked = dict(queue_row)
            locked.update(dict(request_row))
            if not _same_managed_job_origin(candidate, locked):
                return None
            if not managed_job_origin_current:
                _terminalize_stale_managed_job_request(connection, locked)
                return None
            if precondition_error is not None:
                request = _request_from_mapping(locked)
                request.status = requests_lib.RequestStatus.FAILED
                request.finished_at = time.time()
                request.set_error(precondition_error)
                values = _request_values_for_db(request)
                values.pop('request_id')
                # No executor was admitted: this queued generation is
                # effect-quiescent at the same atomic terminal transition.
                values['execution_quiesced_generation'] = int(
                    locked['execution_generation'])
                values['execution_quiesced_at'] = (
                    sqlalchemy.func.clock_timestamp())
                values['execution_quiescence_required'] = True
                _terminalize_locked_request(
                    connection,
                    locked,
                    status=requests_lib.RequestStatus.FAILED,
                    cause=event_api_models.EventCause.PRECONDITION_FAILED,
                    values=values)
                return None
            if not met:
                interval = float(candidate['precondition_payload'].get(
                    'check_interval', 1))
                connection.execute(
                    sqlalchemy.update(QUEUE).where(
                        QUEUE.c.request_id == candidate['request_id']).values(
                            available_at=(sqlalchemy.func.clock_timestamp() +
                                          datetime.timedelta(seconds=interval)),
                            precondition_attempts=(
                                QUEUE.c.precondition_attempts + 1),
                            updated_at=sqlalchemy.func.clock_timestamp()))
                if status_msg is not None:
                    connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id ==
                            candidate['request_id']).values(
                                status_msg=status_msg,
                                updated_at=sqlalchemy.func.clock_timestamp()))
                return None

            generation = int(locked['execution_generation']) + 1
            token = uuid.uuid4()
            execution_class = locked['execution_class']
            controller_generation = (
                self._controller_generation if execution_class
                == request_registry.ExecutionClass.CONTROLLER.value else None)
            claim_statement = sqlalchemy.update(REQUESTS).where(
                REQUESTS.c.request_id == candidate['request_id'],
                REQUESTS.c.status.in_([
                    status.value for status in
                    requests_lib.RequestStatus.executable_statuses()
                ]), *self._role_predicates())
            if controller_generation is not None:
                claim_statement = claim_statement.where(
                    sqlalchemy.exists().where(
                        _controller_leadership_is_current_predicate(
                            uuid.UUID(self._instance_id),
                            controller_generation)))
            result = connection.execute(
                claim_statement.values(
                    execution_generation=generation,
                    terminal_cause=None,
                    pid=None,
                    execution_process_start_time_ticks=None,
                    claim_token=token,
                    worker_instance_id=uuid.UUID(self._instance_id),
                    controller_generation=controller_generation,
                    lease_expires_at=(
                        sqlalchemy.func.clock_timestamp() +
                        datetime.timedelta(seconds=_CLAIM_LEASE_SECONDS)),
                    heartbeat_at=sqlalchemy.func.clock_timestamp(),
                    execution_quiescence_required=True,
                    execution_quiesced_generation=None,
                    execution_quiesced_at=None,
                    updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount != 1:
                return None
            if (controller_generation is not None and
                    not _reserve_controller_action(connection, locked,
                                                   self._instance_id,
                                                   controller_generation)):
                now = sqlalchemy.func.clock_timestamp()
                reservation_row = dict(locked)
                reservation_row.update({
                    'execution_generation': generation,
                    'claim_token': token,
                    'worker_instance_id': uuid.UUID(self._instance_id),
                    'controller_generation': controller_generation,
                })
                _terminalize_locked_request(
                    connection,
                    reservation_row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=(event_api_models.EventCause.
                           CONTROLLER_RESERVATION_CONFLICT),
                    values={
                        'pid': None,
                        'execution_process_start_time_ticks': None,
                        'claim_token': None,
                        'worker_instance_id': None,
                        'lease_expires_at': None,
                        'heartbeat_at': None,
                        'execution_quiesced_generation': generation,
                        'execution_quiesced_at': now,
                        'should_retry': True,
                        'finished_at': now,
                        'interrupted_reason':
                            ('Controller action is already owned by a '
                             'different leadership generation.'),
                    })
                return None
            connection.execute(
                sqlalchemy.update(QUEUE).where(
                    QUEUE.c.request_id == candidate['request_id']).values(
                        delivery_state='claimed',
                        claim_generation=generation,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            managed_job_origin = None
            if locked['managed_job_id'] is not None:
                managed_job_origin = (
                    int(locked['managed_job_id']),
                    str(locked['managed_job_controller_instance_id']),
                    int(locked['managed_job_controller_generation']),
                    int(locked['managed_job_controller_slot_id']),
                    str(locked['managed_job_controller_slot_attempt']),
                )
            return queue_base.QueueItem(request_id=candidate['request_id'],
                                        ignore_return_value=bool(
                                            locked['ignore_return_value']),
                                        retryable=bool(locked['retryable']),
                                        execution_generation=generation,
                                        claim_token=str(token),
                                        worker_instance_id=self._instance_id,
                                        managed_job_origin=managed_job_origin)

    def peek_provider_mutation(
            self) -> queue_base.ProviderMutationCandidate | None:
        """Read a bound provider request without claiming its queue row."""
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            candidate = self._candidate(connection, provider_mutation=True)
        if candidate is None:
            return None
        kind = _PROVIDER_MUTATION_HANDLER_KINDS.get(candidate['handler_name'])
        if kind is None:
            raise RuntimeError(
                'Provider mutation handler classification drifted.')
        return queue_base.ProviderMutationCandidate(
            request_id=candidate['request_id'], kind=kind)

    def claim_provider_mutation(
        self, candidate: queue_base.ProviderMutationCandidate
    ) -> queue_base.QueueItem | None:
        """Claim one exact provider request after process-slot reservation."""
        if not isinstance(candidate, queue_base.ProviderMutationCandidate):
            raise ValueError('Invalid provider mutation candidate.')
        if not isinstance(candidate.kind,
                          queue_base.ProviderMutationRequestKind):
            raise ValueError('Invalid provider mutation candidate kind.')
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            observed = self._candidate(connection,
                                       provider_mutation=True,
                                       request_id=candidate.request_id)
        if observed is None:
            return None
        if (_PROVIDER_MUTATION_HANDLER_KINDS.get(observed['handler_name'])
                is not candidate.kind):
            return None
        return self._claim_candidate(observed, provider_mutation=True)

    def get(self) -> queue_base.QueueItem | None:
        """Claim generic work; provider mutations use the reserved path."""
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            if not self._lock_controller_leadership(connection):
                return None
            self._reap_expired_claims(connection)
            # Lease expiry revokes future mutation authority, but does not
            # prove that an already-running handler/process has stopped. Only
            # the exact execution owner may publish generation-bound
            # quiescence; timeout-based synthesis could otherwise authorize a
            # bound reducer while opaque provider I/O is still running.
            candidate = self._candidate(connection, provider_mutation=False)
        if candidate is None:
            return None
        return self._claim_candidate(candidate, provider_mutation=False)

    def qsize(self) -> int:
        engine = initialize_and_get_db()
        statement = sqlalchemy.select(
            sqlalchemy.func.count()  # pylint: disable=not-callable
        ).select_from(
            QUEUE.join(REQUESTS,
                       REQUESTS.c.request_id == QUEUE.c.request_id)).where(
                           QUEUE.c.schedule_type == self._schedule_type,
                           QUEUE.c.delivery_state == 'queued')
        statement = statement.where(*self._role_predicates())
        with engine.connect() as connection:
            return int(connection.execute(statement).scalar_one())


class PostgresQueueFactory(queue_base.QueueBackendFactory):
    """Create schedule-specific views over one PostgreSQL queue table."""

    def __init__(
        self,
        *,
        execution_classes: frozenset[str] | None = None,
        controller_generation: int | None = None,
        supported_handler_names: frozenset[str] | None = None,
    ) -> None:
        self._execution_classes = execution_classes
        self._controller_generation = controller_generation
        self._supported_handler_names = supported_handler_names

    def create_queue(self, schedule_type: str) -> queue_base.QueueBackend:
        return PostgresQueueBackend(
            schedule_type,
            execution_classes=self._execution_classes,
            controller_generation=self._controller_generation,
            supported_handler_names=self._supported_handler_names)
