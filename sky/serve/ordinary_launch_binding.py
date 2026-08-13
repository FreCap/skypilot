"""PostgreSQL state machine for durable ordinary SkyServe launches.

The API request backend owns request serialization, claims, queue delivery,
and retention pins.  This module owns only Serve-side identity, lifecycle
fences, replica association, effect phases, and result projection.  Every
cross-lineage operation accepts an existing SQLAlchemy connection and never
commits it, so the request backend can compose one atomic transaction without
Serve importing request-table definitions.
"""
# pylint: disable=not-callable

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
import contextlib
import contextvars
import dataclasses
import datetime
import enum
import hashlib
import json
import re
from typing import Any, Protocol
import uuid

import sqlalchemy

from sky.serve import constants as serve_constants
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.utils import locks
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

SUBMISSION_ID_KEY = 'sky_serve_ordinary_launch_submission_id'
ASSOCIATION_ID_KEY = 'sky_serve_ordinary_launch_association_id'
REPLICA_ID_KEY = 'sky_serve_ordinary_launch_replica_id'
REPLICA_RECORD_ID_KEY = 'sky_serve_ordinary_launch_replica_record_id'
LAUNCH_GENERATION_KEY = 'sky_serve_ordinary_launch_generation'
BOUND_REQUEST_ID_KEY = 'sky_serve_ordinary_launch_request_id'
INPUT_DIGEST_KEY = 'sky_serve_ordinary_launch_input_digest'
CONTROLLER_INCARNATION_KEY = 'sky_serve_controller_incarnation'
CONTROLLER_OWNER_EPOCH_KEY = 'sky_serve_controller_owner_epoch'
OWNER_REVISION_KEY = 'sky_serve_ordinary_launch_owner_revision'
LIFECYCLE_EPOCH_KEY = 'sky_serve_lifecycle_epoch'
BINDING_EPOCH_KEY = 'sky_serve_ordinary_launch_binding_epoch'

# A bound request retains the legacy service/hash/version fields so old
# preconditions recognize that this is controller-originated, while an
# impossible PID/IP makes an old executor fail before provider I/O.
LEGACY_FAIL_CLOSED_CONTROLLER_PID = -1
LEGACY_FAIL_CLOSED_CONTROLLER_IP = 'ordinary-launch-binding.invalid'

DIGEST_VERSION = 'serve-bound-launch.v1'
TOMBSTONE_RETENTION_DAYS = 60
MAX_GC_BATCH_SIZE = 500
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_ASSOCIATION_NAMESPACE = uuid.UUID('5ab85493-af88-4e82-bdda-8cbe1a8b15ea')
_REQUEST_NAMESPACE = uuid.UUID('f77cfdf5-95c4-4882-a768-30496fd23c97')


class EffectPhase(str, enum.Enum):
    """Monotonic external-effect boundary."""

    NOT_STARTED = 'NOT_STARTED'
    PROVIDER_IO = 'PROVIDER_IO'
    SERVICE_JOB_IO = 'SERVICE_JOB_IO'
    SERVICE_JOB_RECORDED = 'SERVICE_JOB_RECORDED'


class Resolution(str, enum.Enum):
    """Closed association resolution state."""

    BOUND = 'BOUND'
    CANCEL_REQUESTED = 'CANCEL_REQUESTED'
    RESULT_RECORDED = 'RESULT_RECORDED'
    PROJECTED = 'PROJECTED'
    PRE_EFFECT_TERMINAL = 'PRE_EFFECT_TERMINAL'
    AMBIGUOUS = 'AMBIGUOUS'


class BindingMode(str, enum.Enum):
    LEGACY = 'legacy'
    BOUND = 'bound'


class AdmissionDisposition(str, enum.Enum):
    """Result of the Serve half of atomic admission."""

    CREATE = 'CREATE'
    EXISTING_EXACT = 'EXISTING_EXACT'


class StartupClassification(str, enum.Enum):
    """Conservative startup disposition for request-layer evidence."""

    ADOPT_ACTIVE = 'ADOPT_ACTIVE'
    WAIT_QUIESCENCE = 'WAIT_QUIESCENCE'
    REDUCE_TERMINAL = 'REDUCE_TERMINAL'
    PRE_EFFECT_TERMINALIZE = 'PRE_EFFECT_TERMINALIZE'
    SETTLED = 'SETTLED'
    AMBIGUOUS = 'AMBIGUOUS'


class TerminalStatus(str, enum.Enum):
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


UNSETTLED_RESOLUTIONS = frozenset({
    Resolution.BOUND,
    Resolution.CANCEL_REQUESTED,
    Resolution.RESULT_RECORDED,
    Resolution.AMBIGUOUS,
})
SETTLED_RESOLUTIONS = frozenset({
    Resolution.PROJECTED,
    Resolution.PRE_EFFECT_TERMINAL,
})

# Transitional aliases retained for callers while the stack is assembled.
STATE_BOUND = Resolution.BOUND.value
STATE_AMBIGUOUS = Resolution.AMBIGUOUS.value
STATE_PROJECTED = Resolution.PROJECTED.value
STATE_TERMINAL_UNPROJECTED = Resolution.RESULT_RECORDED.value
STATE_VALUES = tuple(state.value for state in Resolution)

_EFFECT_PHASE_SQL = ', '.join(f"'{value.value}'" for value in EffectPhase)
_RESOLUTION_SQL = ', '.join(f"'{value.value}'" for value in Resolution)
_TERMINAL_STATUS_SQL = ', '.join(f"'{value.value}'" for value in TerminalStatus)
_UNSETTLED_SQL = ', '.join(
    f"'{value.value}'" for value in UNSETTLED_RESOLUTIONS)

metadata = sqlalchemy.MetaData()
ordinary_launch_associations_table = sqlalchemy.Table(
    'serve_ordinary_launch_associations',
    metadata,
    sqlalchemy.Column('association_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('submission_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('tenant_scope', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('service_binding_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_record_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    # Immutable snapshot of the exact paid-capacity claim admitted for this
    # generation.  It lets every effect/reducer transaction lock the global
    # provider pool before the replica without a mutable pre-lock lookup.
    sqlalchemy.Column('paid_capacity_pool_key', sqlalchemy.Text),
    sqlalchemy.Column('launch_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('input_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('digest_version',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=DIGEST_VERSION),
    sqlalchemy.Column('owner_controller_incarnation',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('owner_controller_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('owner_revision',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('owner_transferred_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('effect_phase',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=EffectPhase.NOT_STARTED.value),
    sqlalchemy.Column('effect_phase_changed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('resolution',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=Resolution.BOUND.value),
    sqlalchemy.Column('cancel_reason', sqlalchemy.Text),
    sqlalchemy.Column('cancel_requested_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('terminal_status', sqlalchemy.Text),
    sqlalchemy.Column('terminal_cause', sqlalchemy.Text),
    sqlalchemy.Column('terminal_execution_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('execution_quiescence_required', sqlalchemy.Boolean),
    sqlalchemy.Column('execution_quiesced_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('execution_quiesced_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('service_job_id', sqlalchemy.BigInteger),
    sqlalchemy.Column('result_recorded_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('ambiguity_code', sqlalchemy.Text),
    sqlalchemy.Column('projected_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('pin_released_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('tombstone_not_before',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.CheckConstraint('length(tenant_scope) > 0',
                               name='serve_ordinary_binding_tenant_scope'),
    sqlalchemy.CheckConstraint('length(service_name) > 0',
                               name='serve_ordinary_binding_service_name'),
    sqlalchemy.CheckConstraint('length(service_hash) > 0',
                               name='serve_ordinary_binding_service_hash'),
    sqlalchemy.CheckConstraint('length(service_workspace) > 0',
                               name='serve_ordinary_binding_workspace'),
    sqlalchemy.CheckConstraint('service_lifecycle_epoch > 0',
                               name='serve_ordinary_binding_lifecycle_epoch'),
    sqlalchemy.CheckConstraint('service_binding_epoch > 0',
                               name='serve_ordinary_binding_binding_epoch'),
    sqlalchemy.CheckConstraint('service_version > 0',
                               name='serve_ordinary_binding_service_version'),
    sqlalchemy.CheckConstraint('replica_id > 0',
                               name='serve_ordinary_binding_replica_id'),
    sqlalchemy.CheckConstraint(
        'paid_capacity_pool_key IS NULL OR '
        'length(paid_capacity_pool_key) > 0',
        name='serve_ordinary_binding_paid_pool'),
    sqlalchemy.CheckConstraint('launch_generation > 0',
                               name='serve_ordinary_binding_generation'),
    sqlalchemy.CheckConstraint('length(cluster_name) > 0',
                               name='serve_ordinary_binding_cluster_name'),
    sqlalchemy.CheckConstraint('length(request_id) > 0',
                               name='serve_ordinary_binding_request_id'),
    sqlalchemy.CheckConstraint("input_digest ~ '^[0-9a-f]{64}$'",
                               name='serve_ordinary_binding_input_digest'),
    sqlalchemy.CheckConstraint(f"digest_version = '{DIGEST_VERSION}'",
                               name='serve_ordinary_binding_digest_version'),
    sqlalchemy.CheckConstraint('owner_controller_epoch > 0',
                               name='serve_ordinary_binding_owner_epoch'),
    sqlalchemy.CheckConstraint('owner_revision > 0',
                               name='serve_ordinary_binding_owner_revision'),
    sqlalchemy.CheckConstraint(f'effect_phase IN ({_EFFECT_PHASE_SQL})',
                               name='serve_ordinary_binding_effect_phase'),
    sqlalchemy.CheckConstraint(f'resolution IN ({_RESOLUTION_SQL})',
                               name='serve_ordinary_binding_resolution'),
    sqlalchemy.CheckConstraint(
        f'terminal_status IS NULL OR terminal_status IN '
        f'({_TERMINAL_STATUS_SQL})',
        name='serve_ordinary_binding_terminal_status'),
    sqlalchemy.CheckConstraint(
        'terminal_execution_generation IS NULL OR '
        'terminal_execution_generation >= 0',
        name='serve_ordinary_binding_terminal_generation'),
    sqlalchemy.CheckConstraint(
        'execution_quiesced_generation IS NULL OR '
        'execution_quiesced_generation >= 0',
        name='serve_ordinary_binding_quiesced_generation'),
    sqlalchemy.CheckConstraint('service_job_id IS NULL OR service_job_id > 0',
                               name='serve_ordinary_binding_service_job_id'),
    sqlalchemy.CheckConstraint(
        "(resolution = 'AMBIGUOUS') = (ambiguity_code IS NOT NULL)",
        name='serve_ordinary_binding_ambiguity'),
    sqlalchemy.CheckConstraint(
        "resolution <> 'CANCEL_REQUESTED' OR "
        '(cancel_reason IS NOT NULL AND cancel_requested_at IS NOT NULL)',
        name='serve_ordinary_binding_cancel'),
    sqlalchemy.CheckConstraint(
        '(cancel_reason IS NULL) = (cancel_requested_at IS NULL)',
        name='serve_ordinary_binding_cancel_pair'),
    sqlalchemy.CheckConstraint(
        "(effect_phase = 'SERVICE_JOB_RECORDED') = "
        '(service_job_id IS NOT NULL)',
        name='serve_ordinary_binding_service_job'),
    sqlalchemy.CheckConstraint(
        "resolution NOT IN ('RESULT_RECORDED', 'PROJECTED') OR "
        "effect_phase = 'SERVICE_JOB_RECORDED'",
        name='serve_ordinary_binding_result_effect'),
    sqlalchemy.CheckConstraint(
        "resolution NOT IN ('RESULT_RECORDED', 'PROJECTED', "
        "'PRE_EFFECT_TERMINAL') OR "
        '(terminal_status IS NOT NULL AND '
        'terminal_execution_generation IS NOT NULL AND '
        'execution_quiescence_required IS NOT NULL)',
        name='serve_ordinary_binding_terminal_evidence'),
    sqlalchemy.CheckConstraint(
        '(execution_quiescence_required IS DISTINCT FROM TRUE) OR '
        '(execution_quiesced_generation = terminal_execution_generation AND '
        'execution_quiesced_at IS NOT NULL)',
        name='serve_ordinary_binding_quiescence'),
    sqlalchemy.CheckConstraint(
        "resolution <> 'PRE_EFFECT_TERMINAL' OR "
        "effect_phase = 'NOT_STARTED'",
        name='serve_ordinary_binding_pre_effect'),
    sqlalchemy.CheckConstraint(
        "resolution NOT IN ('PROJECTED', 'PRE_EFFECT_TERMINAL') OR "
        '(projected_at IS NOT NULL AND pin_released_at IS NOT NULL AND '
        'tombstone_not_before IS NOT NULL)',
        name='serve_ordinary_binding_projection'),
    sqlalchemy.CheckConstraint(
        "pin_released_at IS NULL OR resolution IN "
        "('PROJECTED', 'PRE_EFFECT_TERMINAL')",
        name='serve_ordinary_binding_pin_release'),
)
sqlalchemy.Index('uq_serve_ordinary_binding_submission',
                 ordinary_launch_associations_table.c.tenant_scope,
                 ordinary_launch_associations_table.c.service_workspace,
                 ordinary_launch_associations_table.c.submission_id,
                 unique=True)
sqlalchemy.Index('uq_serve_ordinary_binding_request',
                 ordinary_launch_associations_table.c.request_id,
                 unique=True)
sqlalchemy.Index('uq_serve_ordinary_binding_generation',
                 ordinary_launch_associations_table.c.service_name,
                 ordinary_launch_associations_table.c.replica_record_id,
                 ordinary_launch_associations_table.c.launch_generation,
                 unique=True)
sqlalchemy.Index(
    'uq_serve_ordinary_binding_unsettled',
    ordinary_launch_associations_table.c.service_name,
    ordinary_launch_associations_table.c.replica_record_id,
    unique=True,
    postgresql_where=ordinary_launch_associations_table.c.resolution.in_(
        tuple(value.value for value in UNSETTLED_RESOLUTIONS)))
sqlalchemy.Index('ix_serve_ordinary_binding_replica',
                 ordinary_launch_associations_table.c.service_name,
                 ordinary_launch_associations_table.c.replica_id,
                 ordinary_launch_associations_table.c.created_at)
sqlalchemy.Index(
    'ix_serve_ordinary_binding_gc',
    ordinary_launch_associations_table.c.tombstone_not_before,
    postgresql_where=ordinary_launch_associations_table.c.resolution.in_(
        tuple(value.value for value in SETTLED_RESOLUTIONS)))


class OrdinaryLaunchBindingError(RuntimeError):
    """Base error for the closed ordinary-launch binding protocol."""


class OrdinaryLaunchBindingUnavailable(OrdinaryLaunchBindingError):
    """The selected store cannot safely perform the protocol."""


class OrdinaryLaunchBindingConflict(OrdinaryLaunchBindingError):
    """Durable state no longer matches an exact binding identity."""


class OrdinaryLaunchBindingBusy(OrdinaryLaunchBindingError):
    """Exclusive authority is busy; the caller must retry without blocking."""


@dataclasses.dataclass(frozen=True)
class BindingIntent:
    """Validated controller submission before server-derived identity."""

    service_name: str
    service_hash: str
    service_version: int
    replica_id: int
    replica_record_id: uuid.UUID
    lifecycle_epoch: int
    binding_epoch: int
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int
    controller_pid: int | None
    controller_ip: str | None


@dataclasses.dataclass(frozen=True)
class BindingIdentity:
    """Complete immutable identity validated during atomic admission."""

    submission_id: uuid.UUID
    association_id: uuid.UUID
    request_id: str
    tenant_scope: str
    service_name: str
    service_hash: str
    service_workspace: str
    service_lifecycle_epoch: int
    service_binding_epoch: int
    service_version: int
    replica_id: int
    replica_record_id: uuid.UUID
    cluster_name: str
    input_digest: str
    digest_version: str
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int


@dataclasses.dataclass(frozen=True)
class BindingAdmission:
    """Serve-side outcome of exact atomic admission."""

    disposition: AdmissionDisposition
    association_id: str
    request_id: str
    launch_generation: int
    owner_revision: int
    resolution: Resolution
    effect_phase: EffectPhase

    @property
    def created(self) -> bool:
        return self.disposition == AdmissionDisposition.CREATE

    @property
    def expects_active_request(self) -> bool:
        return self.resolution in UNSETTLED_RESOLUTIONS


# Compatibility name used by the endpoint while the stack is rebased.
BindingReservation = BindingAdmission


@dataclasses.dataclass(frozen=True)
class BoundLaunchContext:
    association_id: uuid.UUID
    request_id: str
    service_name: str
    replica_id: int
    replica_record_id: uuid.UUID
    launch_generation: int
    input_digest: str


class ExecutionClaim(Protocol):
    """Request-layer claim shape without importing the request subsystem."""

    request_id: str
    execution_generation: int
    claim_token: str
    worker_instance_id: str | None


ClaimValidator = Callable[
    [sqlalchemy.engine.Connection, uuid.UUID, ExecutionClaim], bool]
TransitionBarrier = Callable[[sqlalchemy.engine.Connection], bool]


@dataclasses.dataclass(frozen=True)
class EffectAuthorization:
    context: BoundLaunchContext
    claim: ExecutionClaim
    owner_revision: int
    guard: Any
    claim_validator: ClaimValidator


@dataclasses.dataclass(frozen=True)
class TerminalEvidence:
    status: TerminalStatus
    cause: str
    execution_generation: int
    quiescence_required: bool
    quiesced_generation: int | None
    quiesced_at: datetime.datetime | None


@dataclasses.dataclass(frozen=True)
class RequestStartupFacts:
    exists: bool
    status: str | None
    queue_exists: bool
    execution_generation: int | None
    claim_exists: bool
    quiescent: bool


@dataclasses.dataclass(frozen=True)
class ControllerBindingAuthority:
    """Durable authority installed for one controller subprocess."""

    service_name: str
    service_hash: str
    service_workspace: str
    service_lifecycle_epoch: int
    controller_pid: int | None
    controller_ip: str | None
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int
    capable: bool
    binding_mode: BindingMode
    binding_epoch: int

    @property
    def incarnation_uuid(self) -> uuid.UUID:
        return self.controller_incarnation

    @property
    def owner_epoch(self) -> int:
        return self.controller_owner_epoch


# Compatibility name while the controller integration is assembled.
ServiceOwner = ControllerBindingAuthority


class ServiceTeardownDisposition(str, enum.Enum):
    """Atomic teardown publication result for one service owner."""

    UNSUPPORTED = 'UNSUPPORTED'
    MARKED_LEGACY = 'MARKED_LEGACY'
    MARKED_BOUND = 'MARKED_BOUND'


@dataclasses.dataclass(frozen=True)
class ServiceTeardownResult:
    """Mode classification committed with the teardown status write."""

    disposition: ServiceTeardownDisposition
    authority: ControllerBindingAuthority | None

    def __post_init__(self) -> None:
        has_authority = self.authority is not None
        if has_authority != (
                self.disposition == ServiceTeardownDisposition.MARKED_BOUND):
            raise ValueError(
                'Only marked-bound teardown may carry controller authority.')


def _canonical_uuid(value: Any, field_name: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(
            f'{field_name} must be a canonical UUID string.') from error
    if str(parsed) != value:
        raise ValueError(f'{field_name} must be a canonical UUID string.')
    return parsed


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f'{field_name} must be a positive integer.')
    return value


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f'{field_name} must be a non-negative integer.')
    return value


def _nonempty(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{field_name} must be non-empty text.')
    return value


def parse_unbound_launch_context(context: Mapping[str, Any]) -> BindingIntent:
    """Parse a controller submission accepted by the private endpoint."""
    if not isinstance(context, Mapping):
        raise ValueError('Ordinary launch context must be a mapping.')
    server_owned_keys = (
        SUBMISSION_ID_KEY,
        ASSOCIATION_ID_KEY,
        LAUNCH_GENERATION_KEY,
        BOUND_REQUEST_ID_KEY,
        INPUT_DIGEST_KEY,
        OWNER_REVISION_KEY,
    )
    if any(key in context for key in server_owned_keys):
        raise ValueError('Ordinary launch context contains server-owned IDs.')
    service_name = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
    service_hash = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
    controller_pid = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY)
    controller_ip = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY)
    if not (controller_pid is None or
            type(controller_pid) is int and controller_pid > 0):
        raise ValueError('Ordinary launch controller PID is malformed.')
    if not (controller_ip is None or isinstance(controller_ip, str)):
        raise ValueError('Ordinary launch controller IP is malformed.')
    return BindingIntent(
        service_name=_nonempty(service_name, 'service_name'),
        service_hash=_nonempty(service_hash, 'service_hash'),
        service_version=_positive_int(
            context.get(
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY),
            'service_version'),
        replica_id=_positive_int(context.get(REPLICA_ID_KEY), 'replica_id'),
        replica_record_id=_canonical_uuid(context.get(REPLICA_RECORD_ID_KEY),
                                          'replica_record_id'),
        lifecycle_epoch=_positive_int(context.get(LIFECYCLE_EPOCH_KEY),
                                      'lifecycle_epoch'),
        binding_epoch=_positive_int(context.get(BINDING_EPOCH_KEY),
                                    'binding_epoch'),
        controller_incarnation=_canonical_uuid(
            context.get(CONTROLLER_INCARNATION_KEY), 'controller_incarnation'),
        controller_owner_epoch=_positive_int(
            context.get(CONTROLLER_OWNER_EPOCH_KEY), 'controller_owner_epoch'),
        controller_pid=controller_pid,
        controller_ip=controller_ip,
    )


def has_bound_launch_context(context: Mapping[str, Any]) -> bool:
    """Whether any server-owned binding field is present.

    Callers use this as a fail-closed discriminator: no fields selects the
    legacy path, while one or more fields selects bound parsing, which rejects
    partial or malformed context.
    """
    return isinstance(context, Mapping) and any(key in context for key in (
        SUBMISSION_ID_KEY,
        ASSOCIATION_ID_KEY,
        LAUNCH_GENERATION_KEY,
        BOUND_REQUEST_ID_KEY,
        INPUT_DIGEST_KEY,
        OWNER_REVISION_KEY,
    ))


def canonical_launch_digest(request_body: Any) -> str:
    """Hash canonical prepared LaunchBody bytes before mutable normalization."""
    try:
        payload = json.loads(request_body.model_dump_json())
        launch_context = payload.get('extra_launch_context')
        if isinstance(launch_context, dict):
            # Server-bound and legacy routing owner fields are mutable and are
            # not part of the stable submission identity.
            for key in (ASSOCIATION_ID_KEY, LAUNCH_GENERATION_KEY,
                        BOUND_REQUEST_ID_KEY, INPUT_DIGEST_KEY,
                        OWNER_REVISION_KEY, CONTROLLER_INCARNATION_KEY,
                        CONTROLLER_OWNER_EPOCH_KEY,
                        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY,
                        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY):
                launch_context.pop(key, None)
        canonical = json.dumps(payload,
                               sort_keys=True,
                               separators=(',', ':'),
                               ensure_ascii=False,
                               allow_nan=False).encode('utf-8')
    except (AttributeError, TypeError, UnicodeError, ValueError) as error:
        raise ValueError(
            'Ordinary launch body is not canonical JSON.') from error
    return hashlib.sha256(canonical).hexdigest()


def derive_binding_ids(tenant_scope: str, service_workspace: str,
                       submission_id: uuid.UUID) -> tuple[uuid.UUID, str]:
    tenant_scope = _nonempty(tenant_scope, 'tenant_scope')
    service_workspace = _nonempty(service_workspace, 'service_workspace')
    submission_id = _canonical_uuid(submission_id, 'submission_id')
    key = json.dumps([tenant_scope, service_workspace,
                      str(submission_id)],
                     ensure_ascii=True,
                     separators=(',', ':'))
    association_id = uuid.uuid5(_ASSOCIATION_NAMESPACE, key)
    request_id = str(uuid.uuid5(_REQUEST_NAMESPACE, key))
    return association_id, request_id


def build_binding_identity(
    intent: BindingIntent,
    *,
    submission_id: uuid.UUID,
    tenant_scope: str,
    service_workspace: str,
    cluster_name: str,
    input_digest: str,
) -> BindingIdentity:
    if not isinstance(intent, BindingIntent):
        raise ValueError('intent must be a BindingIntent.')
    submission_id = _canonical_uuid(submission_id, 'submission_id')
    tenant_scope = _nonempty(tenant_scope, 'tenant_scope')
    service_workspace = _nonempty(service_workspace, 'service_workspace')
    cluster_name = _nonempty(cluster_name, 'cluster_name')
    if not isinstance(input_digest,
                      str) or not _SHA256_RE.fullmatch(input_digest):
        raise ValueError('input_digest must be lowercase SHA-256.')
    association_id, request_id = derive_binding_ids(tenant_scope,
                                                    service_workspace,
                                                    submission_id)
    return BindingIdentity(
        submission_id=submission_id,
        association_id=association_id,
        request_id=request_id,
        tenant_scope=tenant_scope,
        service_name=intent.service_name,
        service_hash=intent.service_hash,
        service_workspace=service_workspace,
        service_lifecycle_epoch=intent.lifecycle_epoch,
        service_binding_epoch=intent.binding_epoch,
        service_version=intent.service_version,
        replica_id=intent.replica_id,
        replica_record_id=intent.replica_record_id,
        cluster_name=cluster_name,
        input_digest=input_digest,
        digest_version=DIGEST_VERSION,
        controller_incarnation=intent.controller_incarnation,
        controller_owner_epoch=intent.controller_owner_epoch,
    )


def install_bound_context(request_body: Any, identity: BindingIdentity,
                          launch_generation: int) -> None:
    """Install only immutable, server-derived identity in a queued body."""
    context = dict(request_body.extra_launch_context)
    # These values fenced admission, but authority is mutable.  A controller
    # takeover adopts this exact queued body and resolves the new owner from
    # the locked service/association rows at each effect boundary.
    context.pop(CONTROLLER_INCARNATION_KEY, None)
    context.pop(CONTROLLER_OWNER_EPOCH_KEY, None)
    context.pop(OWNER_REVISION_KEY, None)
    context.update({
        ASSOCIATION_ID_KEY: str(identity.association_id),
        REPLICA_ID_KEY: identity.replica_id,
        REPLICA_RECORD_ID_KEY: str(identity.replica_record_id),
        LAUNCH_GENERATION_KEY: launch_generation,
        BOUND_REQUEST_ID_KEY: identity.request_id,
        INPUT_DIGEST_KEY: identity.input_digest,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: LEGACY_FAIL_CLOSED_CONTROLLER_PID,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: LEGACY_FAIL_CLOSED_CONTROLLER_IP,
    })
    request_body.extra_launch_context = context


# Temporary compatibility for stack-local callers; the owner revision was
# intentionally removed because controller takeover must adopt the same body.
_install_bound_context = install_bound_context


def parse_bound_launch_context(
        context: Mapping[str, Any]) -> BoundLaunchContext:
    if not isinstance(context, Mapping):
        raise OrdinaryLaunchBindingConflict(
            'Bound ordinary-launch context must be a mapping.')
    return BoundLaunchContext(
        association_id=_canonical_uuid(context.get(ASSOCIATION_ID_KEY),
                                       'association_id'),
        request_id=_nonempty(context.get(BOUND_REQUEST_ID_KEY), 'request_id'),
        service_name=_nonempty(
            context.get(serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY),
            'service_name'),
        replica_id=_positive_int(context.get(REPLICA_ID_KEY), 'replica_id'),
        replica_record_id=_canonical_uuid(context.get(REPLICA_RECORD_ID_KEY),
                                          'replica_record_id'),
        launch_generation=_positive_int(context.get(LAUNCH_GENERATION_KEY),
                                        'launch_generation'),
        input_digest=_nonempty(context.get(INPUT_DIGEST_KEY), 'input_digest'),
    )


def _require_postgres(connection: sqlalchemy.engine.Connection) -> None:
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Ordinary launch binding requires central PostgreSQL state.')


def lock_legacy_request_admission_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> None:
    """Pair one legacy request INSERT with the service transition lock.

    Binding promotion already owns the exclusive service launch-authority
    advisory lock on its transaction.  A valid legacy Serve launch admission
    takes the shared side of that exact lock before inserting its request and
    queue rows.  Therefore an admission that wins first commits and is visible
    to promotion's legacy drain, while an admission that loses waits until the
    bound mode is durable and its legacy pre-effect/provider fences fail closed.

    This transaction-scoped lock deliberately replaces a table ``SHARE`` lock.
    A queue claimant first owns request/queue row locks and then updates those
    tables; a table lock compatible with its initial ``ROW SHARE`` lock can
    otherwise deadlock that later lock upgrade while promotion waits for the
    claimant's row.
    """
    _require_postgres(connection)
    service_name = _nonempty(service_name, 'service_name')
    if not connection.in_transaction():
        raise OrdinaryLaunchBindingUnavailable(
            'Legacy launch admission locking requires an active transaction.')
    # This is the same stable key used by
    # ``service_replica_launch_authority_write_session``.  Keep key derivation
    # single-owned by Serve so request admission cannot silently drift from the
    # transition/provider lock domain.
    lock_id = serve_state._replica_launch_authority_lock_id(  # pylint: disable=protected-access
        service_name, connection.engine)
    connection.execute(
        sqlalchemy.text(
            'SELECT pg_catalog.pg_advisory_xact_lock_shared(:lock_key)'),
        {'lock_key': locks.postgres_lock_key(lock_id)})


def _replica_record_id(row: Mapping[str, Any]) -> str | None:
    replica_state = row.get('replica_state')
    if not isinstance(replica_state, dict):
        return None
    value = replica_state.get('replica_record_id')
    return value if isinstance(value, str) else None


def _replica_snapshot_matches_association(
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
    *,
    require_launch_authorized: bool,
) -> bool:
    """Validate both query columns and the full versioned replica payload."""
    state_version = replica.get('replica_state_version')
    state = replica.get('replica_state')
    if (type(state_version) is not int or not isinstance(state, dict) or
            replica.get('replica_id') != association['replica_id'] or
            replica.get('version') != association['service_version'] or
            replica.get('cluster_name') != association['cluster_name'] or
            replica.get('paid_capacity_pool_key')
            != association.get('paid_capacity_pool_key') or
            _replica_record_id(replica) != str(
                association['replica_record_id'])):
        return False
    try:
        info = serve_state.decode_replica_state_for_authority(
            state_version, state)
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
        return False
    if (info.replica_id != association['replica_id'] or
            info.version != association['service_version'] or
            info.cluster_name != association['cluster_name'] or
            info.replica_record_id != str(association['replica_record_id']) or
            info.paid_capacity_pool_key != replica.get('paid_capacity_pool_key')
            or info.status.value != replica.get('status')):
        return False
    # The binding state machine deliberately owns only ordinary paid-demand
    # launches.  Persisted special profiles retain independent retry and
    # effect authorities; accepting one here would allow both contracts to
    # launch the same replica.  Require the complete current ordinary default
    # state at admission and at every later effect/reduction fence so a corrupt
    # or concurrently reclassified row fails closed.
    if not replica_has_narrow_ordinary_profile(info):
        return False
    if require_launch_authorized and info.status.value not in ('PENDING',
                                                               'PROVISIONING'):
        return False
    return True


def replica_has_narrow_ordinary_profile(info: Any) -> bool:
    """Whether a decoded replica has no retained special-launch authority."""
    try:
        disposition = info.system_recovery_disposition.value
    except AttributeError:
        return False
    return bool(
        info.reserved_fill is False and info.reserved_fill_pool_key is None and
        info.reserved_fill_service_generation is None and
        info.reserved_fill_physical_cluster_uid is None and
        info.reserved_fill_kubernetes_context is None and
        info.is_zero_cost is False and
        info.unknown_capacity_replacement is False and
        info.cost_rebalance_for_replica_id is None and
        info.system_recovery_launch_intent is None and
        disposition == 'ORDINARY' and info.launch_request_id is None and
        info.service_job_id is None and
        info.candidate_ready_observed_at is None and
        info.ordinary_release_not_before is None and
        info.system_recovery_revision == 0 and info.system_recovery is None and
        info.system_recovery_quarantine is None)


def _identity_values(
        identity: BindingIdentity,
        launch_generation: int,
        *,
        paid_capacity_pool_key: str | None = None) -> dict[str, Any]:
    return {
        'association_id': identity.association_id,
        'submission_id': identity.submission_id,
        'tenant_scope': identity.tenant_scope,
        'service_name': identity.service_name,
        'service_hash': identity.service_hash,
        'service_workspace': identity.service_workspace,
        'service_lifecycle_epoch': identity.service_lifecycle_epoch,
        'service_binding_epoch': identity.service_binding_epoch,
        'service_version': identity.service_version,
        'replica_id': identity.replica_id,
        'replica_record_id': identity.replica_record_id,
        'paid_capacity_pool_key': paid_capacity_pool_key,
        'launch_generation': launch_generation,
        'cluster_name': identity.cluster_name,
        'request_id': identity.request_id,
        'input_digest': identity.input_digest,
        'digest_version': identity.digest_version,
    }


def _existing_identity_matches(row: Mapping[str, Any],
                               identity: BindingIdentity) -> bool:
    immutable = _identity_values(
        identity,
        int(row['launch_generation']),
        paid_capacity_pool_key=row['paid_capacity_pool_key'])
    return all(row[key] == value for key, value in immutable.items())


def _active_paid_capacity_pool_key(
    connection: sqlalchemy.engine.Connection,
    identity: BindingIdentity,
    replica: Mapping[str, Any],
) -> str | None:
    """Read an exact claim while the service and replica are locked."""
    rows = connection.execute(
        sqlalchemy.select(
            serve_state_schema.paid_capacity_claims_table.c.service_hash,
            serve_state_schema.paid_capacity_claims_table.c.pool_key).where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                identity.service_name,
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                identity.replica_id)).all()
    if not rows:
        if replica['paid_capacity_pool_key'] is not None:
            raise OrdinaryLaunchBindingConflict(
                'A paid bound replica has no exact capacity claim.')
        return None
    if len(rows) != 1:
        raise OrdinaryLaunchBindingConflict(
            'Replica has multiple paid-capacity claim incarnations.')
    service_hash, pool_key = rows[0]
    if (service_hash != identity.service_hash or
            replica['paid_capacity_pool_key'] != pool_key or
            not isinstance(pool_key, str) or not pool_key):
        raise OrdinaryLaunchBindingConflict(
            'Paid-capacity claim does not match the bound replica identity.')
    return pool_key


def _unsettled_paid_capacity_claim_matches(
    connection: sqlalchemy.engine.Connection,
    association: Mapping[str, Any],
) -> bool:
    """Validate the immutable association-to-claim edge under service lock."""
    rows = connection.execute(
        sqlalchemy.select(
            serve_state_schema.paid_capacity_claims_table.c.service_hash,
            serve_state_schema.paid_capacity_claims_table.c.pool_key).where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                association['service_name'],
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                association['replica_id'])).all()
    expected = association['paid_capacity_pool_key']
    if expected is None:
        return not rows
    return (len(rows) == 1 and rows[0][0] == association['service_hash'] and
            rows[0][1] == expected)


def _lock_admission_rows(
    connection: sqlalchemy.engine.Connection,
    identity: BindingIdentity,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any],
           Mapping[str, Any] | None]:
    lifecycle = connection.execute(
        sqlalchemy.select(serve_state_schema.service_lifecycle_fences_table).
        where(
            serve_state_schema.service_lifecycle_fences_table.c.name ==
            identity.service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            identity.service_name).with_for_update()).mappings().one_or_none()
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            identity.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            identity.replica_id).with_for_update()).mappings().one_or_none()
    if lifecycle is None or service is None or replica is None:
        raise OrdinaryLaunchBindingConflict(
            'Lifecycle, service, or replica disappeared before binding.')
    pointer = replica['ordinary_launch_association_id']
    current = None
    if pointer is not None:
        current = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                pointer).with_for_update()).mappings().one_or_none()
        if current is None:
            raise OrdinaryLaunchBindingConflict(
                'Replica points to a missing ordinary-launch association.')
    return lifecycle, service, replica, current


def _validate_admission_target(connection: sqlalchemy.engine.Connection,
                               lifecycle: Mapping[str, Any],
                               service: Mapping[str,
                                                Any], replica: Mapping[str,
                                                                       Any],
                               identity: BindingIdentity) -> None:
    derived = derive_binding_ids(identity.tenant_scope,
                                 identity.service_workspace,
                                 identity.submission_id)
    if derived != (identity.association_id, identity.request_id):
        raise OrdinaryLaunchBindingConflict(
            'Association/request IDs are not server-derived identity.')
    if (lifecycle['epoch'] != identity.service_lifecycle_epoch or
            service['lifecycle_epoch'] != identity.service_lifecycle_epoch):
        raise OrdinaryLaunchBindingConflict('Service lifecycle epoch changed.')
    workspace = service['workspace']
    if (service['hash'] != identity.service_hash or
            workspace != identity.service_workspace or service['pool'] != 0 or
            service['ordinary_launch_binding_mode'] != 'bound' or
            service['ordinary_launch_binding_epoch']
            != identity.service_binding_epoch or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != identity.controller_incarnation
            or service['controller_owner_epoch']
            != identity.controller_owner_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Service identity, workspace, binding mode, or owner changed.')
    try:
        service_status = serve_statuses.ServiceStatus[str(service['status'])]
    except (KeyError, TypeError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Service status is malformed.') from error
    if service_status in (
            serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Service no longer authorizes replica launch.')
    identity_snapshot = {
        'replica_id': identity.replica_id,
        'replica_record_id': identity.replica_record_id,
        'service_version': identity.service_version,
        'cluster_name': identity.cluster_name,
        'paid_capacity_pool_key': replica.get('paid_capacity_pool_key'),
    }
    if (not _replica_snapshot_matches_association(
            replica, identity_snapshot, require_launch_authorized=True) or
            _elected_recovery_version_in_connection(
                connection, identity.service_name) != identity.service_version):
        raise OrdinaryLaunchBindingConflict(
            'Replica identity, version, state, or cluster changed.')


def _admission_from_row(row: Mapping[str, Any],
                        disposition: AdmissionDisposition) -> BindingAdmission:
    return BindingAdmission(
        disposition=disposition,
        association_id=str(row['association_id']),
        request_id=str(row['request_id']),
        launch_generation=int(row['launch_generation']),
        owner_revision=int(row['owner_revision']),
        resolution=Resolution(str(row['resolution'])),
        effect_phase=EffectPhase(str(row['effect_phase'])),
    )


def insert_or_get_locked(
    connection: sqlalchemy.engine.Connection,
    identity: BindingIdentity,
) -> BindingAdmission:
    """Create or validate Serve association state without committing.

    Lock order is lifecycle, service, replica, current association, then
    association history.  The caller may subsequently insert or verify the API
    request, retention pin, and queue row on this same connection.
    """
    _require_postgres(connection)
    if not isinstance(identity, BindingIdentity):
        raise ValueError('identity must be a BindingIdentity.')
    lifecycle, service, replica, current = _lock_admission_rows(
        connection, identity)
    _validate_admission_target(connection, lifecycle, service, replica,
                               identity)

    existing = current
    if existing is None or existing['association_id'] != identity.association_id:
        existing = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id == identity.
                association_id).with_for_update()).mappings().one_or_none()
    if existing is not None:
        if not _existing_identity_matches(existing, identity):
            raise OrdinaryLaunchBindingConflict(
                'Submission ID was reused with a different launch intent.')
        resolution = Resolution(str(existing['resolution']))
        if (resolution in UNSETTLED_RESOLUTIONS and
                not _unsettled_paid_capacity_claim_matches(
                    connection, existing)):
            raise OrdinaryLaunchBindingConflict(
                'Unsettled association lost its exact paid-capacity claim.')
        pointer = replica['ordinary_launch_association_id']
        if ((resolution in UNSETTLED_RESOLUTIONS and
             pointer != identity.association_id) or
            (resolution in SETTLED_RESOLUTIONS and pointer is not None)):
            raise OrdinaryLaunchBindingConflict(
                'Exact association and replica pointer disagree.')
        return _admission_from_row(existing,
                                   AdmissionDisposition.EXISTING_EXACT)

    if current is not None:
        raise OrdinaryLaunchBindingConflict(
            'Replica already has a different unsettled association.')
    history = connection.execute(
        sqlalchemy.select(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.service_name ==
            identity.service_name,
            ordinary_launch_associations_table.c.replica_record_id ==
            identity.replica_record_id).order_by(
                ordinary_launch_associations_table.c.launch_generation.desc()).
        with_for_update()).mappings().all()
    if history:
        predecessor = history[0]
        if (predecessor['resolution'] != Resolution.PRE_EFFECT_TERMINAL.value or
                predecessor['pin_released_at'] is None or
                predecessor['service_binding_epoch']
                != identity.service_binding_epoch):
            raise OrdinaryLaunchBindingConflict(
                'Predecessor does not prove pre-effect terminal settlement.')
        if predecessor['cancel_reason'] is not None:
            raise OrdinaryLaunchBindingConflict(
                'A cancelled pre-effect predecessor cannot admit a '
                'successor.')
        launch_generation = int(predecessor['launch_generation']) + 1
    else:
        launch_generation = 1
    paid_capacity_pool_key = _active_paid_capacity_pool_key(
        connection, identity, replica)
    values = _identity_values(identity,
                              launch_generation,
                              paid_capacity_pool_key=paid_capacity_pool_key)
    values.update({
        'owner_controller_incarnation': identity.controller_incarnation,
        'owner_controller_epoch': identity.controller_owner_epoch,
        'owner_revision': 1,
        'effect_phase': EffectPhase.NOT_STARTED.value,
        'resolution': Resolution.BOUND.value,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    })
    connection.execute(
        sqlalchemy.insert(ordinary_launch_associations_table).values(**values))
    pointed = connection.execute(
        sqlalchemy.update(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            identity.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            identity.replica_id,
            serve_state_schema.replicas_table.c.ordinary_launch_association_id.
            is_(None)).values(
                ordinary_launch_association_id=identity.association_id))
    if pointed.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Replica association pointer changed during admission.')
    row = dict(values)
    row['launch_generation'] = launch_generation
    return _admission_from_row(row, AdmissionDisposition.CREATE)


def get_binding(association_id: str) -> dict[str, Any] | None:
    association_uuid = _canonical_uuid(association_id, 'association_id')
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Ordinary launch binding requires central PostgreSQL state.')
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                association_uuid)).mappings().one_or_none()
    if row is None:
        return None
    values = dict(row)
    for field in ('association_id', 'submission_id', 'replica_record_id',
                  'owner_controller_incarnation'):
        values[field] = str(values[field])
    return values


def _serve042_supported(engine: sqlalchemy.engine.Engine) -> bool:
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    revision = migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)
    return revision is not None and int(revision) >= 42


def binding_mode(service_name: str) -> BindingMode | None:
    """Return the durable service cutover mode, or None before Serve042."""
    service_name = _nonempty(service_name, 'service_name')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.
                ordinary_launch_binding_mode, serve_state_schema.services_table.
                c.ordinary_launch_binding_capable).where(
                    serve_state_schema.services_table.c.name ==
                    service_name)).one_or_none()
    if row is None:
        raise OrdinaryLaunchBindingConflict('Service does not exist.')
    mode = BindingMode(str(row[0]))
    if mode == BindingMode.BOUND and row[1] is not True:
        raise OrdinaryLaunchBindingConflict(
            'Bound service is owned by an incapable controller.')
    return mode


def get_unsettled_binding_for_replica(
    service_name: str,
    replica_record_id: str,
) -> dict[str, Any] | None:
    record_uuid = _canonical_uuid(replica_record_id, 'replica_record_id')
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                service_name,
                ordinary_launch_associations_table.c.replica_record_id ==
                record_uuid,
                ordinary_launch_associations_table.c.resolution.in_(
                    tuple(value.value for value in UNSETTLED_RESOLUTIONS)))
        ).mappings().one_or_none()
    return None if row is None else dict(row)


def get_for_replica(service_name: str, replica_id: int,
                    replica_record_id: str) -> dict[str, Any] | None:
    """Resolve the exact association named by one replica scalar pointer."""
    service_name = _nonempty(service_name, 'service_name')
    replica_id = _positive_int(replica_id, 'replica_id')
    record_uuid = _canonical_uuid(replica_record_id, 'replica_record_id')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).join(
                serve_state_schema.replicas_table,
                sqlalchemy.and_(
                    serve_state_schema.replicas_table.c.service_name ==
                    ordinary_launch_associations_table.c.service_name,
                    serve_state_schema.replicas_table.c.replica_id ==
                    ordinary_launch_associations_table.c.replica_id,
                    serve_state_schema.replicas_table.c.
                    ordinary_launch_association_id ==
                    ordinary_launch_associations_table.c.association_id)).where(
                        ordinary_launch_associations_table.c.service_name ==
                        service_name,
                        ordinary_launch_associations_table.c.replica_id ==
                        replica_id,
                        ordinary_launch_associations_table.c.replica_record_id
                        == record_uuid)).mappings().one_or_none()
    return None if row is None else dict(row)


def binding_allows_request(association_id: str, request_id: str) -> bool:
    """Conservative non-locking pre-claim qualification.

    This read can only reject queue work early.  It never grants provider
    authority; the later shared-guard transaction locks and revalidates the
    complete request/Serve tuple.
    """
    try:
        association_uuid = _canonical_uuid(association_id, 'association_id')
        request_id = _nonempty(request_id, 'request_id')
    except ValueError:
        return False
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return False
    association = ordinary_launch_associations_table
    service = serve_state_schema.services_table
    replica = serve_state_schema.replicas_table
    lifecycle = serve_state_schema.service_lifecycle_fences_table
    statement = sqlalchemy.select(
        association,
        service.c.hash.label('_current_service_hash'),
        service.c.workspace.label('_current_workspace'),
        service.c.lifecycle_epoch.label('_current_lifecycle_epoch'),
        service.c.ordinary_launch_binding_mode.label('_current_binding_mode'),
        service.c.ordinary_launch_binding_epoch.label('_current_binding_epoch'),
        service.c.ordinary_launch_binding_capable.label('_current_capable'),
        service.c.controller_incarnation.label('_current_incarnation'),
        service.c.controller_owner_epoch.label('_current_owner_epoch'),
        service.c.status.label('_current_service_status'),
        lifecycle.c.epoch.label('_fence_epoch'),
        replica.c.replica_id.label('_replica_id'),
        replica.c.replica_state_version.label('_replica_state_version'),
        replica.c.replica_state.label('_replica_state'),
        replica.c.status.label('_replica_status'),
        replica.c.version.label('_replica_version'),
        replica.c.cluster_name.label('_replica_cluster_name'),
        replica.c.paid_capacity_pool_key.label('_replica_paid_pool_key'),
        replica.c.ordinary_launch_association_id.label('_replica_pointer'),
    ).join(service, service.c.name == association.c.service_name).join(
        lifecycle, lifecycle.c.name == association.c.service_name).join(
            replica,
            sqlalchemy.and_(
                replica.c.service_name == association.c.service_name,
                replica.c.replica_id == association.c.replica_id)).where(
                    association.c.association_id == association_uuid,
                    association.c.request_id == request_id,
                    association.c.resolution == Resolution.BOUND.value)
    with engine.connect() as connection:
        row = connection.execute(statement).mappings().one_or_none()
        elected_version = (None if row is None else
                           _elected_recovery_version_in_connection(
                               connection, str(row['service_name'])))
    if row is None:
        return False
    try:
        status = serve_statuses.ServiceStatus[str(
            row['_current_service_status'])]
    except (KeyError, TypeError):
        return False
    replica_snapshot = {
        'replica_id': row['_replica_id'],
        'replica_state_version': row['_replica_state_version'],
        'replica_state': row['_replica_state'],
        'status': row['_replica_status'],
        'version': row['_replica_version'],
        'cluster_name': row['_replica_cluster_name'],
        'paid_capacity_pool_key': row['_replica_paid_pool_key'],
    }
    return bool(
        row['_replica_pointer'] == association_uuid and
        _replica_snapshot_matches_association(
            replica_snapshot, row, require_launch_authorized=True) and
        elected_version == row['service_version'] and
        row['_fence_epoch'] == row['service_lifecycle_epoch'] and
        row['_current_lifecycle_epoch'] == row['service_lifecycle_epoch'] and
        row['_current_service_hash'] == row['service_hash'] and
        row['_current_workspace'] == row['service_workspace'] and
        row['_current_binding_mode'] == BindingMode.BOUND.value and
        row['_current_binding_epoch'] == row['service_binding_epoch'] and
        row['_current_capable'] is True and
        row['_current_incarnation'] == row['owner_controller_incarnation'] and
        row['_current_owner_epoch'] == row['owner_controller_epoch'] and status
        not in serve_statuses.ServiceStatus.replica_launch_blocking_statuses())


def _lock_effect_rows(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    *,
    require_paid_claim: bool = True,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[
        str, Any]]:
    lifecycle = connection.execute(
        sqlalchemy.select(serve_state_schema.service_lifecycle_fences_table).
        where(serve_state_schema.service_lifecycle_fences_table.c.name ==
              context.service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            context.service_name).with_for_update()).mappings().one_or_none()
    # The pool identity is immutable association data.  Reading it after the
    # service mutex but before its association row lock is safe: every
    # association writer holds this same service row, and the Serve042 trigger
    # rejects identity changes.  This pre-read is what preserves the global
    # service -> paid pool -> replica order used by paid-capacity admission.
    preassociation = connection.execute(
        sqlalchemy.select(
            ordinary_launch_associations_table.c.association_id,
            ordinary_launch_associations_table.c.service_name,
            ordinary_launch_associations_table.c.service_hash,
            ordinary_launch_associations_table.c.replica_id,
            ordinary_launch_associations_table.c.paid_capacity_pool_key,
        ).where(ordinary_launch_associations_table.c.association_id ==
                context.association_id)).mappings().one_or_none()
    if service is None or preassociation is None:
        raise OrdinaryLaunchBindingConflict(
            'Bound effect service or association disappeared.')
    pool_key = preassociation['paid_capacity_pool_key']
    if pool_key is not None:
        pool = connection.execute(
            sqlalchemy.select(
                serve_state_schema.paid_capacity_pools_table.c.pool_key).where(
                    serve_state_schema.paid_capacity_pools_table.c.pool_key ==
                    pool_key).with_for_update()).scalar_one_or_none()
        if pool is None and require_paid_claim:
            raise OrdinaryLaunchBindingConflict(
                'Bound paid-capacity pool disappeared before reduction.')
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            context.service_name, serve_state_schema.replicas_table.c.replica_id
            == context.replica_id).with_for_update()).mappings().one_or_none()
    claim_rows = connection.execute(
        sqlalchemy.select(
            serve_state_schema.paid_capacity_claims_table.c.service_hash,
            serve_state_schema.paid_capacity_claims_table.c.pool_key).where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                context.service_name,
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                context.replica_id).with_for_update()).all()
    association = connection.execute(
        sqlalchemy.select(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).with_for_update()).mappings().one_or_none()
    if any(row is None for row in (lifecycle, service, replica, association)):
        raise OrdinaryLaunchBindingConflict(
            'Bound effect authority disappeared before provider I/O.')
    assert lifecycle is not None and service is not None
    assert replica is not None and association is not None
    if any(association[key] != preassociation[key]
           for key in ('association_id', 'service_name', 'service_hash',
                       'replica_id', 'paid_capacity_pool_key')):
        raise OrdinaryLaunchBindingConflict(
            'Paid-capacity association identity changed while locking.')
    expected_claim = association['paid_capacity_pool_key']
    claim_mismatch = (expected_claim is None and claim_rows) or (
        expected_claim is not None and
        (len(claim_rows) != 1 or claim_rows[0][0] != association['service_hash']
         or claim_rows[0][1] != expected_claim or
         replica['paid_capacity_pool_key'] != expected_claim))
    if require_paid_claim and claim_mismatch:
        raise OrdinaryLaunchBindingConflict(
            'Unsettled association does not hold its exact paid-capacity '
            'claim.')
    return lifecycle, service, replica, association


def _validate_effect_rows(
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
    context: BoundLaunchContext,
    *,
    allowed_resolutions: frozenset[Resolution] = frozenset({Resolution.BOUND}),
    require_launch_authorized: bool = False,
) -> None:
    if (association['request_id'] != context.request_id or
            association['service_name'] != context.service_name or
            association['replica_id'] != context.replica_id or
            association['replica_record_id'] != context.replica_record_id or
            association['launch_generation'] != context.launch_generation or
            association['input_digest'] != context.input_digest or
            association['resolution'] not in tuple(
                resolution.value for resolution in allowed_resolutions)):
        raise OrdinaryLaunchBindingConflict(
            'Bound request identity, revision, or resolution changed.')
    if (replica['ordinary_launch_association_id'] != context.association_id or
            not _replica_snapshot_matches_association(
                replica,
                association,
                require_launch_authorized=require_launch_authorized)):
        raise OrdinaryLaunchBindingConflict(
            'Replica pointer, identity, version, status, or cluster changed.')
    if (lifecycle['epoch'] != association['service_lifecycle_epoch'] or
            service['lifecycle_epoch'] != association['service_lifecycle_epoch']
            or service['hash'] != association['service_hash'] or
            service['workspace'] != association['service_workspace'] or
            service['ordinary_launch_binding_mode'] != 'bound' or
            service['ordinary_launch_binding_epoch']
            != association['service_binding_epoch'] or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation']
            != association['owner_controller_incarnation'] or
            service['controller_owner_epoch']
            != association['owner_controller_epoch']):
        raise OrdinaryLaunchBindingConflict(
            'Service lifecycle, binding epoch, or owner changed.')
    if require_launch_authorized:
        try:
            service_status = serve_statuses.ServiceStatus[str(
                service['status'])]
        except (KeyError, TypeError) as error:
            raise OrdinaryLaunchBindingConflict(
                'Bound effect encountered an unknown service status.'
            ) from error
        if service_status in (serve_statuses.ServiceStatus.
                              replica_launch_blocking_statuses()):
            raise OrdinaryLaunchBindingConflict(
                'Service no longer authorizes provider effects.')


def lock_reduction_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
) -> Mapping[str, Any]:
    """Lock canonical Serve rows before request result and pin evidence."""
    _require_postgres(connection)
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context, require_paid_claim=False)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=UNSETTLED_RESOLUTIONS)
    return dict(association)


def validate_effect_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    claim: ExecutionClaim,
    claim_validator: ClaimValidator,
) -> Mapping[str, Any]:
    _require_postgres(connection)
    if (getattr(claim, 'request_id', None) != context.request_id or
            getattr(claim, 'execution_generation', 0) < 1 or
            not getattr(claim, 'claim_token', None) or
            not getattr(claim, 'worker_instance_id', None)):
        raise OrdinaryLaunchBindingConflict(
            'The exact API request execution claim is no longer active.')
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          require_launch_authorized=True)
    if (_elected_recovery_version_in_connection(connection,
                                                context.service_name)
            != association['service_version']):
        raise OrdinaryLaunchBindingConflict(
            'Bound request service version is no longer elected.')
    if not claim_validator(connection, context.association_id, claim):
        raise OrdinaryLaunchBindingConflict(
            'The exact API request execution claim is no longer active.')
    return association


def _advance_effect_phase(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    claim: ExecutionClaim,
    claim_validator: ClaimValidator,
    expected: EffectPhase,
    target: EffectPhase,
    *,
    service_job_id: int | None = None,
) -> int:
    association = validate_effect_authority_in_connection(
        connection, context, claim, claim_validator)
    if association['effect_phase'] == target.value:
        if (target != EffectPhase.SERVICE_JOB_RECORDED or
                association['service_job_id'] == service_job_id):
            return int(association['owner_revision'])
        raise OrdinaryLaunchBindingConflict(
            'Service-job replay used a different exact job ID.')
    if association['effect_phase'] != expected.value:
        raise OrdinaryLaunchBindingConflict(
            f'Effect phase is {association["effect_phase"]!r}, expected '
            f'{expected.value!r}.')
    next_revision = int(association['owner_revision']) + 1
    values: dict[str, Any] = {
        'effect_phase': target.value,
        'effect_phase_changed_at': sqlalchemy.func.clock_timestamp(),
        'owner_revision': next_revision,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    }
    if target == EffectPhase.SERVICE_JOB_RECORDED:
        values['service_job_id'] = _positive_int(service_job_id,
                                                 'service_job_id')
    current_revision = int(association['owner_revision'])
    updated = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.owner_revision ==
            current_revision,
            ordinary_launch_associations_table.c.effect_phase == expected.value,
            ordinary_launch_associations_table.c.resolution ==
            Resolution.BOUND.value).values(**values))
    if updated.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Effect phase lost its exact compare-and-swap.')
    return next_revision


_ACTIVE_EFFECT_AUTHORIZATION: contextvars.ContextVar[
    EffectAuthorization | None] = contextvars.ContextVar(
        'ordinary_launch_effect_authorization', default=None)


@contextlib.contextmanager
def provider_effect_guard(
    launch_context: Mapping[str, Any],
    claim: ExecutionClaim,
    *,
    claim_validator: ClaimValidator,
) -> Iterator[EffectAuthorization | None]:
    """Fence and record provider I/O, then hold shared authority through it."""
    if not has_bound_launch_context(launch_context):
        yield None
        return
    context = parse_bound_launch_context(launch_context)
    if claim.request_id != context.request_id:
        raise OrdinaryLaunchBindingConflict(
            'Active request claim does not name the bound request.')
    with serve_state.service_replica_launch_authority_guard(
            context.service_name) as guard:
        if not serve_state.service_replica_launch_authority_guard_is_valid(
                guard):
            raise OrdinaryLaunchBindingConflict(
                'Service launch authority guard lost its database session.')
        engine = serve_state.get_database_engine()
        with engine.begin() as connection:
            next_revision = _advance_effect_phase(connection, context, claim,
                                                  claim_validator,
                                                  EffectPhase.NOT_STARTED,
                                                  EffectPhase.PROVIDER_IO)
        authorization = EffectAuthorization(context, claim, next_revision,
                                            guard, claim_validator)
        token = _ACTIVE_EFFECT_AUTHORIZATION.set(authorization)
        try:
            yield authorization
        finally:
            _ACTIVE_EFFECT_AUTHORIZATION.reset(token)


# Name requested by the distinct bound request handler.
authorize_provider_io = provider_effect_guard


def _active_authorization(
        launch_context: Mapping[str, Any]) -> EffectAuthorization | None:
    if not has_bound_launch_context(launch_context):
        return None
    requested = parse_bound_launch_context(launch_context)
    authorization = _ACTIVE_EFFECT_AUTHORIZATION.get()
    if (authorization is None or authorization.context != requested or
            not serve_state.service_replica_launch_authority_guard_is_valid(
                authorization.guard)):
        raise OrdinaryLaunchBindingConflict(
            'Service-job I/O requires the active provider authority guard.')
    return authorization


def require_active_provider_effect_authorization(
        launch_context: Mapping[str, Any]) -> None:
    """Prove the exact bound request still owns its outer provider guard.

    Cloud backends retain a legacy per-provider guard for ordinary Serve
    requests.  A bound request must bypass that PID/IP fence because controller
    takeover deliberately replaces those values with fail-closed sentinels,
    but only while this exact association is inside ``provider_effect_guard``.
    """
    if _active_authorization(launch_context) is None:
        raise OrdinaryLaunchBindingConflict(
            'Bound provider I/O has no active association authorization.')


def begin_service_job_io(launch_context: Mapping[str, Any]) -> int | None:
    authorization = _active_authorization(launch_context)
    if authorization is None:
        return None
    engine = serve_state.get_database_engine()
    with engine.begin() as connection:
        next_revision = _advance_effect_phase(connection, authorization.context,
                                              authorization.claim,
                                              authorization.claim_validator,
                                              EffectPhase.PROVIDER_IO,
                                              EffectPhase.SERVICE_JOB_IO)
    _ACTIVE_EFFECT_AUTHORIZATION.set(
        dataclasses.replace(authorization, owner_revision=next_revision))
    return next_revision


def record_service_job(launch_context: Mapping[str, Any],
                       job_id: int) -> int | None:
    authorization = _active_authorization(launch_context)
    if authorization is None:
        return None
    engine = serve_state.get_database_engine()
    with engine.begin() as connection:
        next_revision = _advance_effect_phase(connection,
                                              authorization.context,
                                              authorization.claim,
                                              authorization.claim_validator,
                                              EffectPhase.SERVICE_JOB_IO,
                                              EffectPhase.SERVICE_JOB_RECORDED,
                                              service_job_id=job_id)
    _ACTIVE_EFFECT_AUTHORIZATION.set(
        dataclasses.replace(authorization, owner_revision=next_revision))
    return next_revision


def transfer_service_owner_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    expected_incarnation: uuid.UUID,
    expected_owner_epoch: int,
    new_incarnation: uuid.UUID,
    new_controller_pid: int | None,
    new_controller_ip: str | None,
    capable: bool,
) -> ControllerBindingAuthority:
    """Transfer service and every unsettled association in one transaction."""
    _require_postgres(connection)
    expected_incarnation = _canonical_uuid(expected_incarnation,
                                           'expected_incarnation')
    new_incarnation = _canonical_uuid(new_incarnation, 'new_incarnation')
    if new_incarnation == expected_incarnation:
        raise ValueError('A controller takeover requires a fresh incarnation.')
    expected_owner_epoch = _positive_int(expected_owner_epoch,
                                         'expected_owner_epoch')
    lifecycle = connection.execute(
        sqlalchemy.select(
            serve_state_schema.service_lifecycle_fences_table).where(
                serve_state_schema.service_lifecycle_fences_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            service_name).with_for_update()).mappings().one_or_none()
    if (lifecycle is None or service is None or
            lifecycle['epoch'] != service['lifecycle_epoch'] or
            service['controller_incarnation'] != expected_incarnation or
            service['controller_owner_epoch'] != expected_owner_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Service owner changed before controller transfer.')
    if service['ordinary_launch_binding_mode'] == 'bound' and not capable:
        raise OrdinaryLaunchBindingConflict(
            'An incapable controller cannot own a bound service.')
    new_epoch = expected_owner_epoch + 1
    updated = connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name,
            serve_state_schema.services_table.c.controller_pid ==
            service['controller_pid'],
            serve_state_schema.services_table.c.controller_ip ==
            service['controller_ip'],
            serve_state_schema.services_table.c.controller_incarnation ==
            expected_incarnation,
            serve_state_schema.services_table.c.controller_owner_epoch ==
            expected_owner_epoch).values(
                controller_incarnation=new_incarnation,
                controller_owner_epoch=new_epoch,
                ordinary_launch_binding_capable=capable,
                controller_pid=new_controller_pid,
                controller_ip=new_controller_ip,
                controller_port=None))
    if updated.rowcount != 1:
        raise OrdinaryLaunchBindingConflict('Service owner transfer lost CAS.')
    connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.service_name == service_name,
            ordinary_launch_associations_table.c.resolution.in_(
                tuple(value.value for value in UNSETTLED_RESOLUTIONS))).values(
                    owner_controller_incarnation=new_incarnation,
                    owner_controller_epoch=new_epoch,
                    owner_revision=(
                        ordinary_launch_associations_table.c.owner_revision +
                        1),
                    owner_transferred_at=sqlalchemy.func.clock_timestamp(),
                    updated_at=sqlalchemy.func.clock_timestamp()))
    return ControllerBindingAuthority(
        service_name=service_name,
        service_hash=str(service['hash']),
        service_workspace=str(service['workspace']),
        service_lifecycle_epoch=int(service['lifecycle_epoch']),
        controller_pid=new_controller_pid,
        controller_ip=new_controller_ip,
        controller_incarnation=new_incarnation,
        controller_owner_epoch=new_epoch,
        capable=capable,
        binding_mode=BindingMode(str(service['ordinary_launch_binding_mode'])),
        binding_epoch=int(service['ordinary_launch_binding_epoch']),
    )


def _controller_owner_pair(
    value: tuple[int | None, str | None],
    field_name: str,
) -> tuple[int | None, str | None]:
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError(f'{field_name} must be a PID/IP pair.')
    controller_pid, controller_ip = value
    if (controller_pid is not None and
        (type(controller_pid) is not int or controller_pid < 1)):
        raise ValueError(f'{field_name} PID must be positive or None.')
    if controller_ip is not None and not isinstance(controller_ip, str):
        raise ValueError(f'{field_name} IP must be text or None.')
    return controller_pid, controller_ip


def _elected_recovery_version_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> int | None:
    versions = serve_state_schema.version_specs_table
    rows = connection.execute(
        sqlalchemy.select(versions.c.version, versions.c.quarantined_at,
                          versions.c.controller_applied_at).where(
                              versions.c.service_name == service_name,
                              versions.c.yaml_content.is_not(None)).order_by(
                                  versions.c.version.desc())).mappings().all()
    latest_applicable = next(
        (int(row['version']) for row in rows if row['quarantined_at'] is None),
        None)
    latest_quarantined = next((int(row['version'])
                               for row in rows
                               if row['quarantined_at'] is not None), None)
    latest_applied_applicable = next((int(row['version'])
                                      for row in rows
                                      if row['quarantined_at'] is None and
                                      row['controller_applied_at'] is not None),
                                     None)
    if (latest_quarantined is not None and
        (latest_applicable is None or latest_applicable < latest_quarantined)):
        return latest_applied_applicable
    return latest_applicable


def begin_service_teardown_if_owner(
    service_name: str,
    expected_service_hash: str,
    expected_parent_owner: tuple[int | None, str | None],
) -> ServiceTeardownResult:
    """Atomically classify binding mode and publish terminal admission.

    A provider effect owns the shared launch-authority advisory guard for its
    entire retry loop. Teardown must publish its terminal intent and deliver
    request cancellation before waiting for that guard, or the cancellation
    needed to end the provider loop is unreachable. This transaction uses the
    canonical lifecycle/service row locks to classify the exact binding epoch
    and close new admissions in one commit. A concurrent promotion therefore
    orders entirely before this transaction (and returns bound authority) or
    entirely after it (and is rejected by terminal status).

    Serve042 legacy mode is marked in the same transaction and needs no second
    status CAS. Stores without Serve042 return ``UNSUPPORTED`` without writing;
    callers retain their established legacy status transition there. A marked
    bound result grants the exact old authority needed to cancel and reduce
    associations before taking any exclusive advisory authority.
    """
    service_name = _nonempty(service_name, 'service_name')
    expected_service_hash = _nonempty(expected_service_hash,
                                      'expected_service_hash')
    expected_parent_owner = _controller_owner_pair(expected_parent_owner,
                                                   'expected_parent_owner')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return ServiceTeardownResult(ServiceTeardownDisposition.UNSUPPORTED,
                                     None)
    with engine.begin() as connection:
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    service_name).with_for_update()).mappings().one_or_none()
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
        if lifecycle is None or service is None:
            raise OrdinaryLaunchBindingConflict(
                'Teardown lost the service lifecycle authority.')
        if (service['hash'] != expected_service_hash or
                lifecycle['epoch'] != service['lifecycle_epoch'] or
            (service['controller_pid'], service['controller_ip'])
                != expected_parent_owner):
            raise OrdinaryLaunchBindingConflict(
                'Teardown does not match the parent-owned service.')
        try:
            mode = BindingMode(str(service['ordinary_launch_binding_mode']))
        except ValueError as error:
            raise OrdinaryLaunchBindingConflict(
                'Teardown encountered an unknown binding mode.') from error
        if (mode == BindingMode.BOUND and
            (service['pool'] != 0 or
             service['ordinary_launch_binding_capable'] is not True)):
            raise OrdinaryLaunchBindingConflict(
                'A bound teardown requires a capable non-pool service.')
        binding_epoch = _nonnegative_int(
            service['ordinary_launch_binding_epoch'],
            'ordinary_launch_binding_epoch')
        updated = connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == service_name,
                serve_state_schema.services_table.c.hash ==
                expected_service_hash,
                serve_state_schema.services_table.c.controller_pid ==
                expected_parent_owner[0],
                serve_state_schema.services_table.c.controller_ip ==
                expected_parent_owner[1],
                serve_state_schema.services_table.c.lifecycle_epoch ==
                lifecycle['epoch'],
                serve_state_schema.services_table.c.ordinary_launch_binding_mode
                == mode.value, serve_state_schema.services_table.c.
                ordinary_launch_binding_epoch == binding_epoch).values(
                    status=serve_statuses.ServiceStatus.SHUTTING_DOWN.value))
        if updated.rowcount != 1:
            raise OrdinaryLaunchBindingConflict(
                'Teardown status transition lost its owner or binding CAS.')
        if mode == BindingMode.LEGACY:
            return ServiceTeardownResult(
                ServiceTeardownDisposition.MARKED_LEGACY, None)
        incarnation = _canonical_uuid(service['controller_incarnation'],
                                      'controller_incarnation')
        owner_epoch = _positive_int(service['controller_owner_epoch'],
                                    'controller_owner_epoch')
        authority = ControllerBindingAuthority(
            service_name=service_name,
            service_hash=expected_service_hash,
            service_workspace=str(service['workspace']),
            service_lifecycle_epoch=int(service['lifecycle_epoch']),
            controller_pid=expected_parent_owner[0],
            controller_ip=expected_parent_owner[1],
            controller_incarnation=incarnation,
            controller_owner_epoch=owner_epoch,
            capable=True,
            binding_mode=mode,
            binding_epoch=binding_epoch)
        return ServiceTeardownResult(ServiceTeardownDisposition.MARKED_BOUND,
                                     authority)


def claim_controller_incarnation(
    service_name: str,
    expected_service_hash: str,
    expected_parent_owner: tuple[int | None, str | None],
    incarnation_uuid: uuid.UUID | str,
    *,
    new_parent_owner: tuple[int | None, str | None] | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_status: serve_statuses.ServiceStatus | None = None,
    expected_recovery_version: int | None = None,
    wait_for_authority: bool = True,
) -> ControllerBindingAuthority | None:
    """Claim a fresh capable controller incarnation under exclusive authority.

    Local SQLite and pre-Serve042 stores retain the legacy PID/IP protocol and
    return ``None``.  A Serve042 PostgreSQL row either performs the exact
    service-plus-unresolved-association transfer or raises closed.
    """
    service_name = _nonempty(service_name, 'service_name')
    expected_service_hash = _nonempty(expected_service_hash,
                                      'expected_service_hash')
    expected_parent_owner = _controller_owner_pair(expected_parent_owner,
                                                   'expected_parent_owner')
    if new_parent_owner is None:
        new_parent_owner = expected_parent_owner
    else:
        new_parent_owner = _controller_owner_pair(new_parent_owner,
                                                  'new_parent_owner')
    if expected_lifecycle_epoch is not None:
        expected_lifecycle_epoch = _positive_int(expected_lifecycle_epoch,
                                                 'expected_lifecycle_epoch')
    if (expected_status is not None and
            not isinstance(expected_status, serve_statuses.ServiceStatus)):
        raise TypeError('expected_status must be a ServiceStatus or None.')
    if expected_recovery_version is not None:
        expected_recovery_version = _positive_int(expected_recovery_version,
                                                  'expected_recovery_version')
    if not isinstance(wait_for_authority, bool):
        raise TypeError('wait_for_authority must be bool.')
    incarnation = _canonical_uuid(incarnation_uuid, 'incarnation_uuid')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return None
    authority_session = (
        serve_state.service_replica_launch_authority_write_session
        if wait_for_authority else
        serve_state.try_service_replica_launch_authority_write_session)
    with authority_session(service_name) as locked_session:
        if locked_session is None:
            raise OrdinaryLaunchBindingBusy(
                'Controller claim is waiting behind active provider work.')
        _, session = locked_session
        connection = session.connection()
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    service_name).with_for_update()).mappings().one_or_none()
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
        if lifecycle is None or service is None:
            raise OrdinaryLaunchBindingConflict(
                'Controller claim lost the service lifecycle authority.')
        if (service['hash'] != expected_service_hash or
                lifecycle['epoch'] != service['lifecycle_epoch'] or
            (service['controller_pid'], service['controller_ip'])
                != expected_parent_owner):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim does not match the parent-owned service.')
        if (expected_lifecycle_epoch is not None and
                service['lifecycle_epoch'] != expected_lifecycle_epoch):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim lifecycle fence changed.')
        if (expected_status is not None and
                service['status'] != expected_status.value):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim status fence changed.')
        try:
            current_status = serve_statuses.ServiceStatus[str(
                service['status'])]
        except (KeyError, TypeError) as error:
            raise OrdinaryLaunchBindingConflict(
                'Controller claim encountered an unknown service status.'
            ) from error
        if (current_status in serve_statuses.ServiceStatus.
                replica_launch_blocking_statuses() and
                expected_status != current_status):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim is blocked by terminal service status.')
        if (expected_recovery_version is not None and
                _elected_recovery_version_in_connection(
                    connection, service_name) != expected_recovery_version):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim recovery-version fence changed.')
        mode = BindingMode(str(service['ordinary_launch_binding_mode']))
        if mode == BindingMode.BOUND and service[
                'ordinary_launch_binding_capable'] is not True:
            raise OrdinaryLaunchBindingConflict(
                'An incapable controller cannot own a bound service.')
        old_incarnation = service['controller_incarnation']
        old_epoch = int(service['controller_owner_epoch'])
        if incarnation == old_incarnation:
            raise OrdinaryLaunchBindingConflict(
                'Every controller startup requires a fresh incarnation UUID.')
        authority = transfer_service_owner_in_connection(
            connection,
            service_name=service_name,
            expected_incarnation=old_incarnation,
            expected_owner_epoch=old_epoch,
            new_incarnation=incarnation,
            new_controller_pid=new_parent_owner[0],
            new_controller_ip=new_parent_owner[1],
            capable=True)
        session.commit()
        return authority


def validate_controller_authority(
    authority: ControllerBindingAuthority | None,
    *,
    service_name: str,
    service_hash: str | None,
    controller_pid: int | None,
    controller_ip: str | None,
) -> ControllerBindingAuthority | None:
    """Validate the parent's exact claim before manager construction."""
    service_name = _nonempty(service_name, 'service_name')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        if authority is not None:
            raise OrdinaryLaunchBindingConflict(
                'Controller authority cannot be verified by this store.')
        return None
    if authority is None:
        raise OrdinaryLaunchBindingConflict(
            'Serve042 controller startup has no claimed incarnation.')
    if not isinstance(authority, ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority or None.')
    if (authority.service_name != service_name or
            authority.service_hash != service_hash or
            authority.controller_pid != controller_pid or
            authority.controller_ip != controller_ip or
            authority.capable is not True):
        raise OrdinaryLaunchBindingConflict(
            'Controller authority does not match its startup arguments.')
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table,
                serve_state_schema.service_lifecycle_fences_table.c.epoch.label(
                    '_fence_epoch')).join(
                        serve_state_schema.service_lifecycle_fences_table,
                        serve_state_schema.service_lifecycle_fences_table.c.name
                        == serve_state_schema.services_table.c.name).where(
                            serve_state_schema.services_table.c.name ==
                            service_name)).mappings().one_or_none()
    if row is None:
        raise OrdinaryLaunchBindingConflict(
            'Claimed controller service no longer exists.')
    try:
        current_mode = BindingMode(str(row['ordinary_launch_binding_mode']))
        current_status = serve_statuses.ServiceStatus[str(row['status'])]
    except (KeyError, TypeError, ValueError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Claimed controller state is malformed.') from error
    if (row['hash'] != authority.service_hash or
            row['workspace'] != authority.service_workspace or
            row['lifecycle_epoch'] != authority.service_lifecycle_epoch or
            row['_fence_epoch'] != authority.service_lifecycle_epoch or
            row['controller_pid'] != authority.controller_pid or
            row['controller_ip'] != authority.controller_ip or
            row['controller_incarnation'] != authority.controller_incarnation or
            row['controller_owner_epoch'] != authority.controller_owner_epoch or
            row['ordinary_launch_binding_capable'] is not True or
            current_mode != authority.binding_mode or
            row['ordinary_launch_binding_epoch'] != authority.binding_epoch or
            current_status
            in serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Claimed controller authority is no longer current.')
    return authority


@contextlib.contextmanager
def refresh_controller_authority(
    previous_authority: ControllerBindingAuthority,
) -> Iterator[ControllerBindingAuthority]:
    """Refresh a live controller's binding mode under shared authority.

    Binding promotion and demotion intentionally advance the binding epoch
    without replacing the controller process.  Every other authority field is
    immutable for that process.  Keep the shared launch-authority guard held
    through the caller's use of the refreshed value so an exclusive owner or
    binding transition cannot race the caller's row persistence and thread
    registration.
    """
    if not isinstance(previous_authority, ControllerBindingAuthority):
        raise TypeError(
            'previous_authority must be ControllerBindingAuthority.')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        raise OrdinaryLaunchBindingUnavailable(
            'Controller authority cannot be refreshed by this store.')
    with serve_state.service_replica_launch_authority_guard(
            previous_authority.service_name) as guard:
        if not serve_state.service_replica_launch_authority_guard_is_valid(
                guard):
            raise OrdinaryLaunchBindingConflict(
                'Controller authority guard lost its database session.')
        with engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(
                    serve_state_schema.services_table,
                    serve_state_schema.service_lifecycle_fences_table.c.epoch.
                    label('_fence_epoch')).join(
                        serve_state_schema.service_lifecycle_fences_table,
                        serve_state_schema.service_lifecycle_fences_table.c.name
                        == serve_state_schema.services_table.c.name).where(
                            serve_state_schema.services_table.c.name ==
                            previous_authority.service_name)).mappings(
                            ).one_or_none()
        if row is None:
            raise OrdinaryLaunchBindingConflict(
                'Controller authority service no longer exists.')
        try:
            current_mode = BindingMode(str(row['ordinary_launch_binding_mode']))
            current_epoch = int(row['ordinary_launch_binding_epoch'])
            current_status = serve_statuses.ServiceStatus[str(row['status'])]
        except (KeyError, TypeError, ValueError) as error:
            raise OrdinaryLaunchBindingConflict(
                'Controller authority state is malformed.') from error
        if (row['hash'] != previous_authority.service_hash or
                row['workspace'] != previous_authority.service_workspace or
                row['lifecycle_epoch']
                != previous_authority.service_lifecycle_epoch or
                row['_fence_epoch']
                != previous_authority.service_lifecycle_epoch or
                row['controller_pid'] != previous_authority.controller_pid or
                row['controller_ip'] != previous_authority.controller_ip or
                row['controller_incarnation']
                != previous_authority.controller_incarnation or
                row['controller_owner_epoch']
                != previous_authority.controller_owner_epoch or
                previous_authority.capable is not True or
                row['ordinary_launch_binding_capable'] is not True or
                current_epoch < previous_authority.binding_epoch or
            (current_epoch == previous_authority.binding_epoch and
             current_mode != previous_authority.binding_mode) or
                current_status in serve_statuses.ServiceStatus.
                replica_launch_blocking_statuses()):
            raise OrdinaryLaunchBindingConflict(
                'Controller authority changed outside a binding transition.')
        refreshed = dataclasses.replace(previous_authority,
                                        binding_mode=current_mode,
                                        binding_epoch=current_epoch)
        yield refreshed
        if not serve_state.service_replica_launch_authority_guard_is_valid(
                guard):
            raise OrdinaryLaunchBindingConflict(
                'Controller authority guard was lost during guarded work.')


def publish_controller_port_if_authority(
    authority: ControllerBindingAuthority,
    controller_port: int,
) -> bool:
    """Publish a ready port under an exact incarnation without advancing it."""
    if not isinstance(authority, ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    controller_port = _positive_int(controller_port, 'controller_port')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        raise OrdinaryLaunchBindingUnavailable(
            'Controller authority cannot be published by this store.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    authority.service_name).with_for_update()).mappings(
                    ).one_or_none()
        result = connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                authority.service_name, serve_state_schema.services_table.c.hash
                == authority.service_hash,
                serve_state_schema.services_table.c.workspace ==
                authority.service_workspace,
                serve_state_schema.services_table.c.lifecycle_epoch ==
                authority.service_lifecycle_epoch,
                serve_state_schema.services_table.c.controller_pid ==
                authority.controller_pid,
                serve_state_schema.services_table.c.controller_ip ==
                authority.controller_ip,
                serve_state_schema.services_table.c.controller_incarnation ==
                authority.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch ==
                authority.controller_owner_epoch,
                serve_state_schema.services_table.c.
                ordinary_launch_binding_capable.is_(True),
                serve_state_schema.services_table.c.ordinary_launch_binding_mode
                == authority.binding_mode.value,
                serve_state_schema.services_table.c.
                ordinary_launch_binding_epoch == authority.binding_epoch,
                serve_state_schema.services_table.c.status.not_in(
                    tuple(status.value
                          for status in serve_statuses.ServiceStatus.
                          replica_launch_blocking_statuses()))).values(
                              controller_port=controller_port))
        lifecycle_matches = (lifecycle is not None and lifecycle['epoch']
                             == authority.service_lifecycle_epoch)
        if result.rowcount != 1 or not lifecycle_matches:
            session.rollback()
            return False
        session.commit()
        return True


def _lock_transition_rows(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any], list[Mapping[str, Any]],
           list[Mapping[str, Any]]]:
    """Lock the complete Serve transition surface in canonical order."""
    lifecycle = connection.execute(
        sqlalchemy.select(
            serve_state_schema.service_lifecycle_fences_table).where(
                serve_state_schema.service_lifecycle_fences_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            service_name).with_for_update()).mappings().one_or_none()
    if lifecycle is None or service is None:
        raise OrdinaryLaunchBindingConflict('Service disappeared.')
    if lifecycle['epoch'] != service['lifecycle_epoch']:
        raise OrdinaryLaunchBindingConflict(
            'Service lifecycle changed before binding transition.')
    replicas = list(
        connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                service_name).order_by(
                    serve_state_schema.replicas_table.c.replica_id).
            with_for_update()).mappings())
    associations = list(
        connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                service_name).order_by(
                    ordinary_launch_associations_table.c.association_id).
            with_for_update()).mappings())
    return lifecycle, service, replicas, associations


def _transition_barrier_passes(
    connection: sqlalchemy.engine.Connection,
    barrier: TransitionBarrier | bool,
    description: str,
) -> bool:
    """Evaluate a request-side barrier only after canonical Serve locks."""
    if callable(barrier):
        return barrier(connection) is True
    if barrier is False:
        return False
    if barrier is True:
        raise OrdinaryLaunchBindingUnavailable(
            f'A precomputed passing {description} cannot authorize a '
            'transition; provide a transaction-local callback.')
    raise TypeError(f'{description} must be a boolean or callable.')


def promote_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    expected_binding_epoch: int,
    participant_barrier_passed: TransitionBarrier | bool,
    legacy_requests_drained: TransitionBarrier | bool,
) -> int:
    """Promote one non-pool service only after both external barriers pass."""
    _require_postgres(connection)
    controller_incarnation = _canonical_uuid(controller_incarnation,
                                             'controller_incarnation')
    # Serve042 installs every legacy service at epoch zero.  The first
    # promotion, including its exact lost-response retry, must therefore
    # accept zero as the source epoch.
    expected_binding_epoch = _nonnegative_int(expected_binding_epoch,
                                              'expected_binding_epoch')
    _, service, replicas, _ = _lock_transition_rows(connection, service_name)
    try:
        service_status = serve_statuses.ServiceStatus(str(service['status']))
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Binding promotion encountered an unknown service status.'
        ) from error
    if (service_status
            in serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Binding promotion is blocked by terminal service status.')
    if service['ordinary_launch_binding_mode'] == 'bound':
        if (service['ordinary_launch_binding_capable'] is not True or
                service['controller_incarnation'] != controller_incarnation or
                service['controller_owner_epoch'] != controller_owner_epoch or
                service['ordinary_launch_binding_epoch']
                != expected_binding_epoch + 1):
            raise OrdinaryLaunchBindingConflict(
                'Already-bound service belongs to different controller '
                'authority or binding epoch.')
        return int(service['ordinary_launch_binding_epoch'])
    if service['ordinary_launch_binding_epoch'] != expected_binding_epoch:
        raise OrdinaryLaunchBindingConflict(
            'Binding promotion source epoch changed before transition.')
    if (not _transition_barrier_passes(connection, participant_barrier_passed,
                                       'participant capability barrier') or
            not _transition_barrier_passes(connection, legacy_requests_drained,
                                           'legacy-request drain barrier')):
        raise OrdinaryLaunchBindingUnavailable(
            'Promotion requires participant capability and legacy drain.')
    pending = sum(replica['status'] in ('PENDING', 'PROVISIONING')
                  for replica in replicas)
    if (service['pool'] != 0 or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != controller_incarnation or
            service['controller_owner_epoch'] != controller_owner_epoch or
            pending != 0):
        raise OrdinaryLaunchBindingConflict(
            'Service is not eligible for ordinary-launch binding promotion.')
    next_epoch = int(service['ordinary_launch_binding_epoch']) + 1
    connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name).values(
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=next_epoch))
    return next_epoch


def demote_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    expected_binding_epoch: int,
    request_barrier_clear: TransitionBarrier | bool,
) -> int:
    """Demote only after every association is settled, unpinned, and clear.

    An exact already-legacy observation is a successful retry.  This matters
    when the demotion transaction committed but the controller died or lost the
    HTTP response before installing/refetching its process-local authority.
    """
    _require_postgres(connection)
    controller_incarnation = _canonical_uuid(controller_incarnation,
                                             'controller_incarnation')
    controller_owner_epoch = _positive_int(controller_owner_epoch,
                                           'controller_owner_epoch')
    expected_binding_epoch = _positive_int(expected_binding_epoch,
                                           'expected_binding_epoch')
    _, service, replicas, associations = _lock_transition_rows(
        connection, service_name)
    if (service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != controller_incarnation or
            service['controller_owner_epoch'] != controller_owner_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Binding demotion belongs to different controller authority.')
    if service['ordinary_launch_binding_mode'] == 'legacy':
        if (service['ordinary_launch_binding_epoch']
                != expected_binding_epoch + 1):
            raise OrdinaryLaunchBindingConflict(
                'Binding demotion retry observed a different binding epoch.')
        return int(service['ordinary_launch_binding_epoch'])
    if service['ordinary_launch_binding_epoch'] != expected_binding_epoch:
        raise OrdinaryLaunchBindingConflict(
            'Binding demotion source epoch changed before transition.')
    if not _transition_barrier_passes(connection, request_barrier_clear,
                                      'request/pin quiescence barrier'):
        raise OrdinaryLaunchBindingUnavailable(
            'Demotion requires the request/pin quiescence barrier.')
    unresolved = sum(association['resolution'] in tuple(
        value.value
        for value in UNSETTLED_RESOLUTIONS) or
                     association['pin_released_at'] is None
                     for association in associations)
    pointers = sum(replica['ordinary_launch_association_id'] is not None
                   for replica in replicas)
    if unresolved or pointers:
        raise OrdinaryLaunchBindingConflict(
            'Bound associations remain active, unprojected, or pinned.')
    next_epoch = int(service['ordinary_launch_binding_epoch']) + 1
    connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name).values(
                ordinary_launch_binding_mode='legacy',
                ordinary_launch_binding_epoch=next_epoch))
    return next_epoch


def request_cancel_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    reason: str,
) -> int:
    reason = _nonempty(reason, 'cancel_reason')
    if len(reason) > 128:
        raise ValueError('cancel_reason must be at most 128 characters.')
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=frozenset(
                              {Resolution.BOUND, Resolution.CANCEL_REQUESTED}))
    if association['resolution'] == Resolution.CANCEL_REQUESTED.value:
        if association['cancel_reason'] != reason:
            raise OrdinaryLaunchBindingConflict(
                'Cancel intent replay used a different exact reason.')
        return int(association['owner_revision'])
    next_revision = int(association['owner_revision']) + 1
    result = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.resolution ==
            Resolution.BOUND.value,
            ordinary_launch_associations_table.c.owner_revision == int(
                association['owner_revision'])).values(
                    resolution=Resolution.CANCEL_REQUESTED.value,
                    cancel_reason=reason,
                    cancel_requested_at=sqlalchemy.func.clock_timestamp(),
                    owner_revision=next_revision,
                    updated_at=sqlalchemy.func.clock_timestamp()))
    if result.rowcount != 1:
        raise OrdinaryLaunchBindingConflict('Cancel intent lost its CAS.')
    return next_revision


def commit_cancel_intent(context: BoundLaunchContext | Mapping[str, Any],
                         reason: str) -> int:
    """Commit an exact owner-fenced cancellation intent before API cancel."""
    if isinstance(context, Mapping):
        context = parse_bound_launch_context(context)
    if not isinstance(context, BoundLaunchContext):
        raise ValueError('context must be a bound ordinary-launch context.')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        raise OrdinaryLaunchBindingUnavailable(
            'Ordinary launch cancellation requires Serve042 PostgreSQL.')
    with serve_state.service_replica_launch_authority_write_session(
            context.service_name) as (_, session):
        revision = request_cancel_in_connection(session.connection(), context,
                                                reason)
        session.commit()
        return revision


def _terminal_values(evidence: TerminalEvidence) -> dict[str, Any]:
    if not isinstance(evidence, TerminalEvidence):
        raise ValueError('evidence must be TerminalEvidence.')
    if not isinstance(evidence.status, TerminalStatus):
        raise ValueError('terminal status must be closed.')
    cause = _nonempty(evidence.cause, 'terminal_cause')
    if len(cause) > 256:
        raise ValueError('terminal_cause must be at most 256 characters.')
    generation = _nonnegative_int(evidence.execution_generation,
                                  'execution_generation')
    if evidence.quiescence_required:
        if (evidence.quiesced_generation != generation or
                evidence.quiesced_at is None):
            raise ValueError('Required exact quiescence is not proven.')
    return {
        'terminal_status': evidence.status.value,
        'terminal_cause': cause,
        'terminal_execution_generation': generation,
        'execution_quiescence_required': evidence.quiescence_required,
        'execution_quiesced_generation': evidence.quiesced_generation,
        'execution_quiesced_at': evidence.quiesced_at,
    }


def record_terminal_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    evidence: TerminalEvidence,
) -> StartupClassification:
    """Copy immutable terminal evidence and classify the safe next action."""
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    # Terminal reduction is permitted after a committed cancel intent too.
    if association['resolution'] == Resolution.CANCEL_REQUESTED.value:
        adjusted = dict(association)
        adjusted['resolution'] = Resolution.BOUND.value
        _validate_effect_rows(lifecycle, service, replica, adjusted, context)
    else:
        _validate_effect_rows(lifecycle, service, replica, association, context)
    values = _terminal_values(evidence)
    existing_terminal = association['terminal_status']
    if existing_terminal is not None:
        if any(association[key] != value for key, value in values.items()):
            raise OrdinaryLaunchBindingConflict(
                'Terminal evidence replay does not match the copied result.')
        if association['resolution'] == Resolution.AMBIGUOUS.value:
            return StartupClassification.AMBIGUOUS
        if association['resolution'] in (Resolution.PROJECTED.value,
                                         Resolution.PRE_EFFECT_TERMINAL.value):
            return StartupClassification.SETTLED
    phase = EffectPhase(str(association['effect_phase']))
    next_revision = int(association['owner_revision']) + 1
    values.update({
        'owner_revision': next_revision,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    })
    if phase == EffectPhase.NOT_STARTED:
        # Projection and request-pin release must happen in the caller's one
        # cross-layer transaction before this becomes a settled state.
        connection.execute(
            sqlalchemy.update(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                context.association_id).values(**values))
        return StartupClassification.PRE_EFFECT_TERMINALIZE
    if phase == EffectPhase.SERVICE_JOB_RECORDED:
        values.update({
            'resolution': Resolution.RESULT_RECORDED.value,
            'result_recorded_at': sqlalchemy.func.clock_timestamp(),
        })
        connection.execute(
            sqlalchemy.update(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                context.association_id).values(**values))
        return StartupClassification.REDUCE_TERMINAL
    values.update({
        'resolution': Resolution.AMBIGUOUS.value,
        'ambiguity_code': 'terminal-after-unrecorded-effect',
    })
    connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).values(**values))
    return StartupClassification.AMBIGUOUS


def mark_ambiguous_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    ambiguity_code: str,
) -> bool:
    ambiguity_code = _nonempty(ambiguity_code, 'ambiguity_code')
    if len(ambiguity_code) > 128:
        raise ValueError('ambiguity_code must be at most 128 characters.')
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context, require_paid_claim=False)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=frozenset({
                              Resolution.BOUND, Resolution.CANCEL_REQUESTED,
                              Resolution.AMBIGUOUS
                          }))
    if association['resolution'] == Resolution.AMBIGUOUS.value:
        if association['ambiguity_code'] != ambiguity_code:
            raise OrdinaryLaunchBindingConflict(
                'Ambiguity replay used a different exact reason.')
        return False
    result = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.resolution.in_(
                (Resolution.BOUND.value,
                 Resolution.CANCEL_REQUESTED.value))).values(
                     resolution=Resolution.AMBIGUOUS.value,
                     ambiguity_code=ambiguity_code,
                     owner_revision=int(association['owner_revision']) + 1,
                     updated_at=sqlalchemy.func.clock_timestamp()))
    return result.rowcount == 1


def project_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    *,
    pre_effect_terminal: bool,
    service_job_id: int | None,
) -> bool:
    """Settle Serve association/pointer inside a cross-layer transaction.

    The request layer must delete the exact active retention pin later in this
    same transaction.  Any failure rolls back these Serve writes too.
    """
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    adjusted = dict(association)
    adjusted['resolution'] = Resolution.BOUND.value
    _validate_effect_rows(lifecycle, service, replica, adjusted, context)
    if pre_effect_terminal:
        if (association['effect_phase'] != EffectPhase.NOT_STARTED.value or
                association['terminal_status'] is None or
                association['resolution']
                not in (Resolution.BOUND.value,
                        Resolution.CANCEL_REQUESTED.value)):
            return False
        target = Resolution.PRE_EFFECT_TERMINAL
    else:
        if (association['resolution'] != Resolution.RESULT_RECORDED.value or
                association['effect_phase']
                != EffectPhase.SERVICE_JOB_RECORDED.value or
                association['service_job_id'] != service_job_id):
            return False
        target = Resolution.PROJECTED
    cleared = connection.execute(
        sqlalchemy.update(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            context.service_name, serve_state_schema.replicas_table.c.replica_id
            == context.replica_id,
            serve_state_schema.replicas_table.c.ordinary_launch_association_id
            == context.association_id).values(
                ordinary_launch_association_id=None))
    if cleared.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Projection could not clear the exact replica pointer.')
    now = sqlalchemy.func.clock_timestamp()
    updated = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).values(
                resolution=target.value,
                projected_at=now,
                pin_released_at=now,
                tombstone_not_before=sqlalchemy.text(
                    "transaction_timestamp() + INTERVAL '60 days'"),
                owner_revision=int(association['owner_revision']) + 1,
                updated_at=now))
    return updated.rowcount == 1


def project_from_request(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    *,
    pre_effect_terminal: bool,
    service_job_id: int | None,
    release_pin: Callable[[sqlalchemy.engine.Connection, str, uuid.UUID], bool],
) -> bool:
    """Project Serve state and delete its exact request pin atomically."""
    changed = project_in_connection(connection,
                                    context,
                                    pre_effect_terminal=pre_effect_terminal,
                                    service_job_id=service_job_id)
    if changed and not release_pin(connection, context.request_id,
                                   context.association_id):
        raise OrdinaryLaunchBindingConflict(
            'Projection could not release the exact request retention pin.')
    return changed


def release_projected_paid_capacity_claim_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
) -> bool:
    """Delete the exact paid claim after projection in the same transaction.

    The caller must already hold the canonical lifecycle, service, paid-pool,
    replica, and association locks through ``project_from_request``.  Keeping
    this as a post-projection step lets the final project revalidation still
    observe the claim; all writes remain invisible until the shared transaction
    commits.
    """
    association = connection.execute(
        sqlalchemy.select(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).with_for_update()).mappings().one_or_none()
    if (association is None or
            association['request_id'] != context.request_id or
            association['service_name'] != context.service_name or
            association['replica_id'] != context.replica_id or
            association['replica_record_id'] != context.replica_record_id or
            association['launch_generation'] != context.launch_generation or
            association['input_digest'] != context.input_digest or
            association['resolution'] not in tuple(
                value.value for value in SETTLED_RESOLUTIONS)):
        raise OrdinaryLaunchBindingConflict(
            'Paid-capacity release lost the exact projected association.')
    claims = serve_state_schema.paid_capacity_claims_table
    predicates = (
        claims.c.service_name == association['service_name'],
        claims.c.replica_id == association['replica_id'],
    )
    pool_key = association['paid_capacity_pool_key']
    if pool_key is None:
        remaining = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(claims).where(
                    *predicates)).scalar_one()
        return int(remaining) == 0
    result = connection.execute(
        sqlalchemy.delete(claims).where(
            *predicates, claims.c.service_hash == association['service_hash'],
            claims.c.pool_key == pool_key))
    return result.rowcount == 1


# Compatibility name for the reducer integration while the stack is assembled.
mark_projected_in_connection = project_in_connection


def classify_startup(
    association: Mapping[str, Any],
    request: RequestStartupFacts,
) -> StartupClassification:
    resolution = Resolution(str(association['resolution']))
    if resolution in SETTLED_RESOLUTIONS:
        return StartupClassification.SETTLED
    if resolution == Resolution.AMBIGUOUS:
        return StartupClassification.AMBIGUOUS
    phase = EffectPhase(str(association['effect_phase']))
    if not request.exists:
        return StartupClassification.AMBIGUOUS
    if request.status in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
        return (StartupClassification.REDUCE_TERMINAL
                if request.quiescent else StartupClassification.WAIT_QUIESCENCE)
    if request.status not in ('PENDING', 'WAITING', 'RUNNING'):
        return StartupClassification.AMBIGUOUS
    if request.queue_exists:
        return StartupClassification.ADOPT_ACTIVE
    if (request.execution_generation == 0 and not request.claim_exists and
            phase == EffectPhase.NOT_STARTED):
        return StartupClassification.PRE_EFFECT_TERMINALIZE
    return StartupClassification.AMBIGUOUS
