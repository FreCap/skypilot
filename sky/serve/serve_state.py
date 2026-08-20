"""The database for services information."""
import collections
from collections.abc import Mapping
import contextlib
import copy
import dataclasses
import datetime
import enum
import functools
import hashlib
import json
import math
import os
import pickle
import re
import time
import typing
from typing import Any, Optional
import uuid

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import global_user_state_schema
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.serve import capacity_admission
from sky.serve import constants
from sky.serve import demand_state_schema
from sky.serve import ephemeral_storage_contract
from sky.serve import lb_cutover_state
from sky.serve import lb_ha
from sky.serve import maintenance
from sky.serve import paid_capacity
from sky.serve import placement_normalization_authority
from sky.serve import placement_normalization_identity
from sky.serve import placement_normalization_manifest
from sky.serve import placement_policy
from sky.serve import pool_capacity_observation
from sky.serve import pool_capacity_observation_schema
from sky.serve import reserved_fill_projection_authority
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import reserved_fill_reclaim_proofs
from sky.serve import resource_action_m4_state_schema
from sky.serve import route_projection_schema
from sky.serve import serve_state_schema
from sky.serve.lb_cutover_state import lb_cutover_kubernetes_guard as _lb_guard
from sky.serve.serve_statuses import ReplicaStatus
from sky.serve.serve_statuses import ServiceStatus
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils import locks
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

# These modules import Serve state through ReplicaInfo/controller paths. Keep
# their runtime imports lazy so recovery-only PostgreSQL helpers do not form an
# import cycle during ``import sky``.
if typing.TYPE_CHECKING:
    from sqlalchemy.engine import row

    from sky.serve import demand_state
    from sky.serve import kubernetes_identity
    from sky.serve import ordinary_launch_binding
    from sky.serve import paid_retirement
    from sky.serve import placement_contract_normalization
    from sky.serve import replica_managers
    from sky.serve import reserved_fill_planner
    from sky.serve import resource_action_state
    from sky.serve import route_projection
    from sky.serve import service_spec
    from sky.serve import zero_cost_actuation
else:
    demand_state = adaptors_common.LazyImport('sky.serve.demand_state')
    placement_contract_normalization = adaptors_common.LazyImport(
        'sky.serve.placement_contract_normalization')
    paid_retirement = adaptors_common.LazyImport('sky.serve.paid_retirement')
    kubernetes_identity = adaptors_common.LazyImport(
        'sky.serve.kubernetes_identity')
    replica_managers = adaptors_common.LazyImport('sky.serve.replica_managers')
    reserved_fill_planner = adaptors_common.LazyImport(
        'sky.serve.reserved_fill_planner')
    resource_action_state = adaptors_common.LazyImport(
        'sky.serve.resource_action_state')
    route_projection = adaptors_common.LazyImport('sky.serve.route_projection')
    service_spec = adaptors_common.LazyImport('sky.serve.service_spec')
    zero_cost_actuation = adaptors_common.LazyImport(
        'sky.serve.zero_cost_actuation')

replica_info_lib = adaptors_common.LazyImport('sky.serve.replica_info')
reserved_capacity = adaptors_common.LazyImport('sky.serve.reserved_capacity')
system_oom_recovery = adaptors_common.LazyImport(
    'sky.serve.system_oom_recovery')
logger = sky_logging.init_logger(__name__)

_TERMINAL_IDENTITY_QUERY_BATCH_SIZE = 250
_REPLICA_LAUNCH_AUTHORITY_LOCK_PREFIX = 'skyserve-replica-launch-authority'
_RESERVED_FILL_RECLAIM_GATE_LOCK_ID = (
    'skyserve-reserved-fill-reclaim-gate-authority')
_PLACEMENT_NORMALIZATION_RECEIPT_MAX_ROWS = (
    placement_normalization_manifest.MAX_INVENTORY_ROWS)

Base = serve_state_schema.Base
# Keep every public Serve table on the one canonical metadata graph.  Local
# SQLite remains physically capped at Serve037, so the one legacy whole-row
# query below explicitly selects only columns present at that revision.
services_table = serve_state_schema.services_table
replicas_table = serve_state_schema.replicas_table
version_specs_table = serve_state_schema.version_specs_table
_SERVE038_SERVICE_COLUMN_NAMES = frozenset(
    column.name
    for column in resource_action_m4_state_schema.service_candidate_columns())
_POST_SERVE037_SERVICE_COLUMN_NAMES = frozenset({
    'owner_user_id',
    'owner_user_name',
    'controller_incarnation',
    'controller_owner_epoch',
    'ordinary_launch_binding_capable',
    'ordinary_launch_binding_mode',
    'ordinary_launch_binding_epoch',
    'non_pool_launch_binding_capable',
    'non_pool_launch_controller_incarnation',
    'non_pool_launch_binding_protocol_version',
    'non_pool_launch_capability_profile_set_digest',
    'non_pool_launch_capability_cohort_epoch',
    'non_pool_launch_receipt_protocol_version',
    'route_source_mode',
    'route_source_epoch',
    'route_projection_capable',
    'route_projection_controller_incarnation',
    'route_projection_protocol_version',
    'demand_source_mode',
    'demand_source_epoch',
    'demand_authority_capable',
    'demand_authority_controller_incarnation',
    'demand_authority_protocol_version',
    'reserved_fill_actuation_mode',
    'reserved_fill_actuation_epoch',
    'reserved_fill_actuation_capable',
    'reserved_fill_actuation_controller_incarnation',
    'reserved_fill_actuation_protocol_version',
})
_SERVE037_SERVICE_COLUMNS = tuple(
    column for column in services_table.c
    if column.name not in (_SERVE038_SERVICE_COLUMN_NAMES |
                           _POST_SERVE037_SERVICE_COLUMN_NAMES))
placement_normalization_runs_table = (
    serve_state_schema.placement_normalization_runs_table)
placement_normalization_rows_table = (
    serve_state_schema.placement_normalization_rows_table)
ephemeral_storage_cleanup_intents_table = (
    serve_state_schema.ephemeral_storage_cleanup_intents_table)
serve_ha_recovery_script_table = (
    serve_state_schema.serve_ha_recovery_script_table)
service_lifecycle_fences_table = (
    serve_state_schema.service_lifecycle_fences_table)
reserved_fill_claims_table = serve_state_schema.reserved_fill_claims_table
reserved_fill_protocol_state_table = (
    serve_state_schema.reserved_fill_protocol_state_table)
reserved_fill_service_claim_sets_table = (
    serve_state_schema.reserved_fill_service_claim_sets_table)
reserved_fill_pool_claims_table = (
    serve_state_schema.reserved_fill_pool_claims_table)
reserved_fill_rounds_table = serve_state_schema.reserved_fill_rounds_table
reserved_fill_lease_table = serve_state_schema.reserved_fill_lease_table
demand_capacity_observations_table = (
    serve_state_schema.demand_capacity_observations_table)
paid_capacity_pools_table = serve_state_schema.paid_capacity_pools_table
paid_capacity_claims_table = serve_state_schema.paid_capacity_claims_table
paid_capacity_waiters_table = serve_state_schema.paid_capacity_waiters_table
create_table = serve_state_schema.create_table
_db_manager = serve_state_schema._db_manager  # pylint: disable=protected-access
ensure_tables_initialized = serve_state_schema.ensure_tables_initialized
get_database_engine = serve_state_schema.get_database_engine

_PLACEMENT_PROJECTION_COLUMN_NAMES = frozenset({
    'controller_job_projection',
    'controller_work_cache',
    'worker_placement_projections',
})


class LogicalRetirementCommitState(str, enum.Enum):
    """Outcome of the durable logical-retirement commit boundary."""

    COMMITTED = 'committed'
    REJECTED = 'rejected'
    AMBIGUOUS = 'ambiguous'


@dataclasses.dataclass(frozen=True)
class LogicalRetirementCommitResult:
    """Typed destructive-commit result; ambiguity never authorizes a worker."""

    state: LogicalRetirementCommitState
    replica_info: Any | None = None


def _placement_projection_columns_available(
        engine: sqlalchemy.engine.Engine) -> bool:
    """Preserve officially supported Serve037 local SQLite databases."""
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return True
    column_names = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    return _PLACEMENT_PROJECTION_COLUMN_NAMES <= column_names


def _require_projection_columns_if_nonnull(
    engine: sqlalchemy.engine.Engine,
    controller_job_projection: dict[str, Any] | None,
    controller_work_cache: dict[str, Any] | None,
    worker_placement_projections: list[dict[str, Any]] | None,
) -> bool:
    available = _placement_projection_columns_available(engine)
    if (not available and
            any(value is not None
                for value in (controller_job_projection, controller_work_cache,
                              worker_placement_projections))):
        raise RuntimeError('Immutable Serve placement projections require '
                           'central PostgreSQL schema revision 043.')
    return available


RESERVED_FILL_PROTOCOL_V1 = 1
RESERVED_FILL_PROTOCOL_V2 = 2
RESERVED_FILL_CLAIM_SET_MIGRATION_SHADOW = 'migration_shadow'
RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2 = 'authoritative_v2'


@dataclasses.dataclass(frozen=True)
class ReservedFillWriterInstance:
    """One recent API request-server lease relevant to fill execution."""

    instance_id: str
    role: str
    pod_name: str | None
    pod_uid: str | None
    version: str
    ready: bool
    draining: bool
    request_storage_backend: str
    request_queue_backend: str
    execution_quiescence_capable: bool


def claim_service_lifecycle_epoch(service_name: str,
                                  lock_connection: Any | None = None) -> int:
    """Advance and return the durable fencing token for ``service_name``.

    PostgreSQL callers pass the DBAPI connection that owns the name's
    advisory lock.  Advancing the token on that exact session is essential:
    if the session was lost, the statement fails instead of allowing a stale
    holder to mint a token after a replacement acquired the lock.  SQLite
    callers hold the process-global FileLock and use a normal ORM session.

    Existing rows are stamped with the new epoch in the same transaction.
    The fence row itself is never deleted, so same-name recreation cannot
    reset the epoch.
    """
    if lock_connection is not None:
        cursor = lock_connection.cursor()
        try:
            cursor.execute(
                'INSERT INTO service_lifecycle_fences (name, epoch) '
                'VALUES (%s, 1) ON CONFLICT (name) DO UPDATE SET '
                'epoch = service_lifecycle_fences.epoch + 1 '
                'RETURNING epoch', (service_name,))
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError('Lifecycle epoch claim returned no row.')
            epoch = int(row[0])
            cursor.execute(
                'UPDATE services SET lifecycle_epoch = %s WHERE name = %s',
                (epoch, service_name))
            lock_connection.commit()
            return epoch
        except Exception:
            lock_connection.rollback()
            raise
        finally:
            cursor.close()

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(service_lifecycle_fences_table.c.epoch).where(
                service_lifecycle_fences_table.c.name ==
                service_name)).fetchone()
        if row is None:
            epoch = 1
            session.execute(
                sqlalchemy.insert(service_lifecycle_fences_table).values(
                    name=service_name, epoch=epoch))
        else:
            epoch = int(row[0]) + 1
            session.execute(
                sqlalchemy.update(service_lifecycle_fences_table).where(
                    service_lifecycle_fences_table.c.name ==
                    service_name).values(epoch=epoch))
        session.execute(
            sqlalchemy.update(services_table).where(
                services_table.c.name == service_name).values(
                    lifecycle_epoch=epoch))
        session.commit()
    return epoch


def read_service_lifecycle_epoch(service_name: str,
                                 lock_connection: Any) -> int:
    """Read one live service's epoch on its advisory-lock session.

    Controller-preserving mutations serialize on the same name-scoped
    advisory lock as destructive lifecycles, but must not replace the live
    controller's durable identity.  Read both copies on the lock-owning
    session so loss of that session fails the operation instead of silently
    falling back to an unrelated database connection.
    """
    cursor = lock_connection.cursor()
    try:
        cursor.execute(
            'SELECT service.lifecycle_epoch, fence.epoch '
            'FROM services AS service '
            'JOIN service_lifecycle_fences AS fence '
            'ON fence.name = service.name '
            'WHERE service.name = %s', (service_name,))
        row = cursor.fetchone()
        if (row is None or not isinstance(row[0], int) or row[0] < 1 or
                row[0] != row[1]):
            raise RuntimeError(
                f'Service {service_name!r} has no consistent live lifecycle '
                'epoch.')
        lock_connection.commit()
        return int(row[0])
    except Exception:
        lock_connection.rollback()
        raise
    finally:
        cursor.close()


def service_lifecycle_epoch_matches(service_name: str, epoch: int) -> bool:
    """Whether ``epoch`` is still the latest token for ``service_name``."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(service_lifecycle_fences_table.c.epoch).where(
                service_lifecycle_fences_table.c.name ==
                service_name)).fetchone()
    return row is not None and int(row[0]) == epoch


def _lifecycle_epoch_matches_in_session(
    session: orm.Session | sqlalchemy.engine.Connection,
    service_name: str,
    epoch: int | None,
) -> bool:
    """Lock and validate a lifecycle fence row inside a mutation txn."""
    bind = session.bind if isinstance(session, orm.Session) else session
    is_postgres = (bind is not None and bind.dialect.name
                   == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    if epoch is None and not is_postgres:
        # Compatibility for old direct/unit-test callers. Production lifecycle
        # entrypoints always supply an epoch.
        return True
    stmt = sqlalchemy.select(service_lifecycle_fences_table.c.epoch).where(
        service_lifecycle_fences_table.c.name == service_name)
    if is_postgres:
        stmt = stmt.with_for_update()
    row = session.execute(stmt).fetchone()
    if epoch is None:
        # PostgreSQL whole-row writers still take the durable lifecycle mutex
        # even for legacy callers that do not carry an epoch.  Absence keeps
        # their historical compatibility behavior but cannot authorize a
        # recovery mutation, whose stricter primitive rejects a missing row.
        return True
    return row is not None and int(row[0]) == epoch


def _begin_immediate_if_sqlite(session: orm.Session,
                               engine: sqlalchemy.engine.Engine,
                               enabled: bool = True) -> None:
    """Make a SQLite read-then-write fence one atomic writer transaction."""
    if (enabled and
            engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value):
        session.execute(sqlalchemy.text('BEGIN IMMEDIATE'))


_UNIQUE_CONSTRAINT_FAILED_ERROR_MSGS = [
    # sqlite
    'UNIQUE constraint failed: services.name',
    # postgres
    'duplicate key value violates unique constraint "services_pkey"',
]


class OrphanedReplicaRecordsError(RuntimeError):
    """Fresh registration found replica inventory without a service row."""


class OrphanedStorageCleanupIntentsError(RuntimeError):
    """Fresh registration found predecessor storage awaiting cleanup."""


class OrphanedVersionRecordsError(RuntimeError):
    """Fresh registration found predecessor version cleanup inventory."""


class MalformedReplicaResourceActionIdentityError(RuntimeError):
    """A replica row has an unsafe partial resource-action identity."""


class ServiceOwnerAuthorityError(RuntimeError):
    """The immutable service launch tenant cannot be safely established."""


@dataclasses.dataclass(frozen=True)
class ReplicaResourceActionIdentity:
    """Exact persisted resource identity used to fence replica teardown."""

    replica_id: int
    cluster_name: str
    replica_incarnation: uuid.UUID
    desired_generation: int
    sky_cluster_record_uuid: uuid.UUID


def _ephemeral_storage_generation_from_yaml(
        yaml_content: str | None,
        expected_resource_scope: str | None) -> str | None:
    """Read and owner-fence the typed internal Task-YAML storage scope."""
    scope = ephemeral_storage_contract.parse_ephemeral_storage_scope(
        yaml_content)
    if scope is None:
        return None
    if scope.resource_scope != expected_resource_scope:
        raise ephemeral_storage_contract.EphemeralStorageContractError(
            'Task-YAML storage scope does not match its service owner.')
    return scope.storage_generation


def _adopt_exact_ephemeral_storage_cleanup_intent(
        session: orm.Session, service_name: str, resource_scope: str,
        storage_generation: str, yaml_content: str, pool: bool,
        lifecycle_epoch: int | None, version_created_at: Any) -> None:
    """Commit one exact cleanup intent with an all-column preimage CAS."""
    if (type(lifecycle_epoch) is not int or lifecycle_epoch < 1 or
            type(pool) is not bool or isinstance(version_created_at, bool) or
            not isinstance(version_created_at, (int, float)) or
            not math.isfinite(float(version_created_at)) or
            version_created_at < 0):
        raise ephemeral_storage_contract.EphemeralStorageContractError(
            'Scoped storage handoff requires an exact service owner.')
    intent = session.execute(
        sqlalchemy.select(ephemeral_storage_cleanup_intents_table).where(
            ephemeral_storage_cleanup_intents_table.c.service_name ==
            service_name,
            ephemeral_storage_cleanup_intents_table.c.resource_scope ==
            resource_scope,
            ephemeral_storage_cleanup_intents_table.c.storage_generation ==
            storage_generation).with_for_update()).mappings().one_or_none()
    if intent is None:
        raise ephemeral_storage_contract.EphemeralStorageContractError(
            'Scoped storage commit has no exact cleanup intent.')
    created_at = intent['created_at']
    intent_lifecycle_epoch = intent['lifecycle_epoch']
    provisional = intent['provisional']
    if (type(intent['pool']) is not int or intent['pool'] not in (0, 1) or
            type(intent_lifecycle_epoch) is not int or
            intent_lifecycle_epoch < 1 or type(provisional) is not int or
            provisional not in (0, 1) or isinstance(created_at, bool) or
            not isinstance(created_at, (int, float)) or
            not math.isfinite(float(created_at)) or created_at < 0 or
            float(created_at) > float(version_created_at) or
            intent['yaml_content'] != yaml_content or
            intent['pool'] != int(pool) or
        (provisional == 1 and intent_lifecycle_epoch != lifecycle_epoch) or
        (provisional == 0 and intent_lifecycle_epoch > lifecycle_epoch)):
        raise ephemeral_storage_contract.EphemeralStorageContractError(
            'Scoped storage cleanup intent does not match its commit.')
    adopted = session.execute(
        sqlalchemy.update(ephemeral_storage_cleanup_intents_table).where(
            ephemeral_storage_cleanup_intents_table.c.service_name ==
            intent['service_name'],
            ephemeral_storage_cleanup_intents_table.c.resource_scope ==
            intent['resource_scope'],
            ephemeral_storage_cleanup_intents_table.c.storage_generation ==
            intent['storage_generation'],
            ephemeral_storage_cleanup_intents_table.c.yaml_content ==
            intent['yaml_content'],
            ephemeral_storage_cleanup_intents_table.c.pool == intent['pool'],
            ephemeral_storage_cleanup_intents_table.c.lifecycle_epoch ==
            intent['lifecycle_epoch'],
            ephemeral_storage_cleanup_intents_table.c.provisional ==
            intent['provisional'],
            ephemeral_storage_cleanup_intents_table.c.created_at ==
            created_at).values(provisional=0))
    if adopted.rowcount != 1:
        raise ephemeral_storage_contract.EphemeralStorageContractError(
            'Scoped storage cleanup intent changed during commit.')


_ReservedFillLockedFunction = typing.TypeVar('_ReservedFillLockedFunction',
                                             bound=typing.Callable[..., Any])


def _with_reserved_fill_broker_lock(
    function: _ReservedFillLockedFunction,) -> _ReservedFillLockedFunction:
    """Serialize service-name creation/teardown with broker state changes."""

    @functools.wraps(function)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        lock = locks.get_lock(constants.RESERVED_FILL_BROKER_LOCK_ID)
        with lock.acquire(blocking=True):
            return function(*args, **kwargs)

    return typing.cast(_ReservedFillLockedFunction, wrapped)


def _replica_launch_authority_lock_id(
        service_name: str,
        engine: sqlalchemy.engine.Engine | None = None) -> str:
    """Return one stable lock key across service incarnations."""
    if not isinstance(service_name, str) or not service_name:
        raise ValueError('Service name must be a non-empty string.')
    identity = service_name
    if (engine is not None and
            engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value):
        database = engine.url.database
        if database is None or database == ':memory:':
            database = f'in-memory-engine-{id(engine)}'
        else:
            database = os.path.realpath(
                os.path.abspath(os.path.expanduser(str(database))))
        # Test workers and local clients may own independent SQLite files but
        # use the same logical service name. Namespace by the database path so
        # only processes that can mutate the same file serialize each other.
        identity = f'{database}\0{service_name}'
    digest = hashlib.sha256(identity.encode('utf-8')).hexdigest()
    return f'{_REPLICA_LAUNCH_AUTHORITY_LOCK_PREFIX}-{digest}'


def _get_replica_launch_authority_lock(service_name: str, *,
                                       shared: bool) -> locks.DistributedLock:
    """Return the cross-pod session lock for provider authorization.

    Provider calls take the shared side; every mutation that can invalidate
    ``service_replica_launch_fence_holds`` takes the exclusive side before it
    opens a SQL transaction.  The key is derived only from the logical service
    name, so deletion and same-name recreation cannot escape an in-flight
    predecessor launch by changing an incarnation-scoped path or hash.
    """
    engine = _db_manager.get_engine()
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return locks.PostgresLock(_replica_launch_authority_lock_id(
            service_name, engine),
                                  shared_lock=shared,
                                  engine=engine)
    elif engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        # FileLock has no shared mode, so local SQLite serializes providers.
        return locks.get_lock(_replica_launch_authority_lock_id(
            service_name, engine),
                              lock_type='filelock',
                              shared_lock=shared)
    else:
        raise RuntimeError('Unsupported database dialect for replica launch '
                           f'authority: {engine.dialect.name!r}.')


@contextlib.contextmanager
def service_replica_launch_authority_guard(
        service_name: str) -> typing.Iterator[locks.DistributedLock]:
    """Hold shared launch authority across one opaque provider operation."""
    lock = _get_replica_launch_authority_lock(service_name, shared=True)
    with lock.acquire(blocking=True):
        yield lock


@contextlib.contextmanager
def reserved_fill_reclaim_gate_authority_guard(
        *, shared: bool) -> typing.Iterator[locks.DistributedLock]:
    """Serialize one-way gate activation against provider effects.

    Terminal launches hold the shared side across provider mutation. The
    transition holds the broker lock and exclusive fleet side on one
    PostgreSQL session around its claim reread and CAS. Losing that session
    therefore drops both authorities and makes the transaction on it fail,
    instead of letting a replacement broker writer race activation.
    Deployment-policy reads must happen before entering this guard.
    """
    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('Reserved-fill reclaim gate authority requires '
                           'PostgreSQL.')
    if shared:
        lock = locks.PostgresLock(_RESERVED_FILL_RECLAIM_GATE_LOCK_ID,
                                  shared_lock=True,
                                  engine=engine)
        with lock.acquire(blocking=True):
            yield lock
        return

    # Preserve the global lock order used by claim writers: broker first,
    # fleet reclaim second. Both keys live on the same session so activation
    # cannot retain one authority after silently losing the other.
    lock = locks.PostgresLock(constants.RESERVED_FILL_BROKER_LOCK_ID,
                              shared_lock=False,
                              engine=engine)
    with lock.acquire(blocking=True):
        with lock.acquire_additional(_RESERVED_FILL_RECLAIM_GATE_LOCK_ID,
                                     shared_lock=False):
            yield lock


def reserved_fill_reclaim_gate_authority_guard_is_valid(
        lock: locks.DistributedLock) -> bool:
    """Whether the fleet-wide advisory-lock session is still authoritative."""
    return isinstance(lock, locks.PostgresLock) and lock.is_session_alive()


_ReservedFillActivationTransactionResult = typing.TypeVar(
    '_ReservedFillActivationTransactionResult')


def run_reserved_fill_reclaim_activation_transaction(
    lock: locks.DistributedLock,
    operation: typing.Callable[[sqlalchemy.engine.Connection],
                               _ReservedFillActivationTransactionResult],
) -> _ReservedFillActivationTransactionResult:
    """Run activation state changes on the exact composite-guard session.

    The operation owns no transaction lifecycle. This wrapper begins and ends
    its one transaction while retaining the session-level broker and fleet
    advisory locks across the commit. It deliberately does not close the
    SQLAlchemy facade because doing so would return the guard's DBAPI session
    to the pool and release its advisory locks.
    """
    if not isinstance(lock, locks.PostgresLock):
        raise RuntimeError('Reserved-fill activation requires a PostgreSQL '
                           'authority session.')
    engine = _db_manager.get_engine()

    def _run(lock_connection: Any) -> _ReservedFillActivationTransactionResult:
        connection = sqlalchemy.engine.Connection(engine,
                                                  lock_connection,
                                                  _allow_revalidate=False)
        transaction = connection.begin()
        try:
            result = operation(connection)
            transaction.commit()
            return result
        except BaseException:
            if transaction.is_active:
                transaction.rollback()
            raise

    return lock.run_in_lock_session(_run)


def service_replica_launch_authority_guard_is_valid(
        lock: locks.DistributedLock) -> bool:
    """Whether a held provider guard still owns its backing lock session."""
    if isinstance(lock, locks.PostgresLock):
        return lock.is_session_alive()
    return lock.is_locked()


@contextlib.contextmanager
# pylint: disable=contextmanager-generator-missing-cleanup
def service_replica_launch_authority_write_session(
    service_name: str,
) -> typing.Iterator[tuple[sqlalchemy.engine.Engine, orm.Session]]:
    """Hold exclusive launch authority for a controller/association mutation.

    This narrow public wrapper lets the ordinary-launch state machine compose
    its own typed transaction without depending on this module's private lock
    implementation.
    """
    with _replica_launch_authority_write_session(service_name) as value:
        yield value


@contextlib.contextmanager
# pylint: disable=contextmanager-generator-missing-cleanup
def try_service_replica_launch_authority_write_session(
    service_name: str,
) -> typing.Iterator[tuple[sqlalchemy.engine.Engine, orm.Session] | None]:
    """Try one exclusive PostgreSQL authority transaction without waiting.

    Dead-child supervision uses this narrow path so a provider retry holding
    shared authority cannot block the parent loop that must observe teardown
    and deliver that retry's cancellation.  ``None`` means another authority
    participant is active; no mutation has occurred.
    """
    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('Nonblocking replica launch authority requires '
                           'PostgreSQL.')
    lock_id = _replica_launch_authority_lock_id(service_name, engine)
    guard_engine = db_utils.get_postgres_lock_engine(engine)
    with orm.Session(guard_engine) as session:
        acquired = session.execute(
            sqlalchemy.text('SELECT pg_try_advisory_xact_lock(:lock_key)'), {
                'lock_key': locks.postgres_lock_key(lock_id)
            }).scalar_one()
        if not acquired:
            yield None
            return
        yield engine, session


@contextlib.contextmanager
def _replica_launch_authority_write_session(
    service_name: str,
    *,
    invalidates_launch_authority: bool = True,
) -> typing.Iterator[tuple[sqlalchemy.engine.Engine, orm.Session]]:
    """Open a DB transaction holding the authorization writer guard.

    PostgreSQL uses a transaction advisory lock on the mutation's own ORM
    connection.  If that connection is lost, PostgreSQL aborts the mutation
    and releases the lock together; a separate session lock would permit the
    mutation to commit after its guard silently disappeared.  SQLite has no
    advisory locks, so its local-file fallback is acquired before opening the
    ORM session.
    """
    engine = _db_manager.get_engine()
    if not invalidates_launch_authority:
        with orm.Session(engine) as session:
            yield engine, session
        return
    lock_id = _replica_launch_authority_lock_id(service_name, engine)
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        # Do not block on the ordinary bounded Serve QueuePool: the shared
        # provider phase may need that pool for its final authorization read.
        # The dedicated NullPool connection still targets the exact same
        # PostgreSQL database, and the mutation itself runs on the transaction
        # that owns the exclusive advisory lock.
        guard_engine = db_utils.get_postgres_lock_engine(engine)
        with orm.Session(guard_engine) as session:
            session.execute(
                sqlalchemy.text('SELECT pg_advisory_xact_lock(:lock_key)'),
                {'lock_key': locks.postgres_lock_key(lock_id)})
            yield engine, session
        return
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        lock = locks.get_lock(lock_id, lock_type='filelock')
        with lock.acquire(blocking=True):
            with orm.Session(engine) as session:
                yield engine, session
        return
    raise RuntimeError('Unsupported database dialect for replica launch '
                       f'authority: {engine.dialect.name!r}.')


def _serialize_current_service_spec(
        spec: 'service_spec.SkyServiceSpec') -> bytes:
    """Serialize one intentional write from an explicit v2 object state."""
    if type(spec) is not service_spec.SkyServiceSpec:
        raise TypeError('Persisted Serve spec must use the exact '
                        'SkyServiceSpec class.')
    state = spec.__dict__
    if not isinstance(state, dict):
        raise TypeError('Persisted SkyServiceSpec state must be a dictionary.')
    try:
        _, version = placement_policy.decode_contract_state(state)
    except (TypeError, ValueError) as exc:
        raise ValueError('Persisted SkyServiceSpec has an invalid placement '
                         'contract.') from exc
    if (version != placement_policy.PLACEMENT_CONTRACT_VERSION_V2 or
            placement_policy.ROLLBACK_REPLICA_UNIT_FIELD in state):
        raise ValueError('New Serve versions require an explicit mirror-free '
                         'v2 placement contract.')
    return pickle.dumps(spec, protocol=4)


def _validated_placement_projections(
    controller_job_projection: dict[str, Any] | None,
    controller_work_cache: dict[str, Any] | None,
    worker_placement_projections: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[dict[str, Any]] |
           None]:
    """Validate and copy immutable platform projections at the DB boundary."""
    controller = kubernetes_identity.validate_controller_job_projection(
        controller_job_projection)
    controller_cache = (
        kubernetes_identity.validate_controller_work_cache_projection(
            controller_work_cache))
    workers = kubernetes_identity.validate_worker_placement_projections(
        worker_placement_projections)
    return controller, controller_cache, workers


@_with_reserved_fill_broker_lock
def add_service(
        name: str,
        controller_job_id: int,
        policy: str,
        requested_resources_str: str,
        load_balancing_policy: str,
        status: ServiceStatus,
        tls_encrypted: bool,
        pool: bool,
        controller_pid: int,
        entrypoint: str,
        spec: 'service_spec.SkyServiceSpec',
        yaml_content: str,
        workspace: str | None = None,
        controller_ip: str | None = None,
        service_hash: str | None = None,
        lifecycle_epoch: int | None = None,
        resource_scope: str | None = None,
        owner_user_id: str | None = None,
        owner_user_name: str | None = None,
        created_by: str | None = None,
        submitted_yaml_content: str | None = None,
        placement_catalog: dict[str, Any] | None = None,
        controller_config: bytes | None = None,
        controller_config_digest: str | None = None,
        controller_config_snapshot_id: str | None = None,
        controller_job_projection: dict[str, Any] | None = None,
        controller_work_cache: dict[str, Any] | None = None,
        worker_placement_projections: list[dict[str, Any]] | None = None
) -> bool:
    """Atomically add a service and its initial version to the database.

    The `services` row and the initial `version_specs` row (at
    `constants.INITIAL_VERSION`, matching the `current_version` column
    default) are written in a single transaction. Writing them in two
    separate commits leaves a crash window in which a `services` row exists
    with no `version_specs` row; the latest-version inner join
    (`_build_services_with_latest_version_query`) then hides that service
    from status, recovery and teardown, and the duplicate name blocks
    re-`up`.

    Returns:
        True if the service is added successfully, False if the service already
        exists.
    """
    serialized_spec = _serialize_current_service_spec(spec)
    controller_config_snapshot = _validate_controller_config_snapshot(
        controller_config, controller_config_digest,
        controller_config_snapshot_id)
    (controller_job_projection, controller_work_cache,
     worker_placement_projections) = (_validated_placement_projections(
         controller_job_projection, controller_work_cache,
         worker_placement_projections))
    engine = _db_manager.get_engine()
    if owner_user_id is None:
        owner_user_id = os.environ.get(skylet_constants.USER_ID_ENV_VAR)
    if owner_user_name is None:
        owner_user_name = os.environ.get(skylet_constants.USER_ENV_VAR)
    if not common_utils.is_valid_user_hash(owner_user_id):
        raise ValueError('Service owner_user_id is invalid.')
    owner_columns_available = {'owner_user_id', 'owner_user_name'} <= {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('services')
    }
    if not isinstance(owner_user_name, str) or not owner_user_name:
        raise ValueError('Service owner_user_name is invalid.')
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value and
            not owner_columns_available):
        raise RuntimeError('Serve055 owner authority is not installed.')
    projection_columns_available = _require_projection_columns_if_nonnull(
        engine, controller_job_projection, controller_work_cache,
        worker_placement_projections)
    lb_ha_enabled = bool(spec.lb_high_availability)
    if (lb_ha_enabled and
            engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise RuntimeError('External load balancer high availability requires '
                           'the central PostgreSQL Serve database.')
    storage_generation = _ephemeral_storage_generation_from_yaml(
        yaml_content, resource_scope)
    try:
        with _replica_launch_authority_write_session(name) as (_, session):
            _begin_immediate_if_sqlite(session, engine, lifecycle_epoch
                                       is not None)
            if not _lifecycle_epoch_matches_in_session(session, name,
                                                       lifecycle_epoch):
                session.rollback()
                return False
            existing_service = session.execute(
                sqlalchemy.select(services_table.c.name).where(
                    services_table.c.name ==
                    name).with_for_update()).fetchone()
            if existing_service is not None:
                session.rollback()
                return False
            if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
                insert_func = sqlite.insert
            elif (engine.dialect.name ==
                  db_utils.SQLAlchemyDialect.POSTGRESQL.value):
                insert_func = postgresql.insert
            else:
                raise ValueError('Unsupported database dialect')

            # Upserting only v1 leaves orphan v2+ rows behind; MAX(version)
            # then exposes the predecessor's spec/yaml. Replica rows may be
            # the last inventory of live billable clusters. These resource
            # checks apply in both topologies: the remote controller DB is the
            # authority for non-consolidated pools even though it has no API-
            # local lifecycle epoch.
            orphan_replica_rows = session.execute(
                sqlalchemy.select(
                    replicas_table.c.replica_id,
                    replicas_table.c.cluster_name).where(
                        replicas_table.c.service_name == name)).fetchall()
            if orphan_replica_rows:
                identities = [
                    str(cluster_name)
                    if cluster_name else f'replica-id:{replica_id}'
                    for replica_id, cluster_name in orphan_replica_rows
                ]
                session.rollback()
                raise OrphanedReplicaRecordsError(
                    f'Cannot safely reuse service name {name!r}: orphan '
                    'replica records may still identify live clusters: '
                    f'{", ".join(identities)}. Terminate or reconcile '
                    'those clusters before retrying.')
            orphan_versions = session.execute(
                sqlalchemy.select(version_specs_table.c.version).where(
                    version_specs_table.c.service_name == name).order_by(
                        version_specs_table.c.version)).scalars().all()
            if orphan_versions:
                session.rollback()
                raise OrphanedVersionRecordsError(
                    f'Cannot safely reuse service name {name!r}: orphan '
                    'version metadata may be the only cleanup inventory '
                    f'for predecessor storage (versions {orphan_versions}). '
                    'Run down --purge for this name before retrying.')
            orphan_storage_scopes = session.execute(
                sqlalchemy.select(
                    ephemeral_storage_cleanup_intents_table.c.resource_scope).
                where(
                    ephemeral_storage_cleanup_intents_table.c.service_name ==
                    name,
                    ephemeral_storage_cleanup_intents_table.c.resource_scope
                    != resource_scope).distinct()).scalars().all()
            if orphan_storage_scopes:
                session.rollback()
                raise OrphanedStorageCleanupIntentsError(
                    f'Cannot safely reuse service name {name!r}: scoped '
                    'storage from predecessor incarnation(s) still awaits '
                    f'cleanup: {", ".join(sorted(orphan_storage_scopes))}. '
                    'Run down --purge for this name before retrying.')
            if lifecycle_epoch is not None:
                # The API parent publishes the current lifecycle's HA script
                # before spawning this controller.  Deleting it here leaves a
                # successfully registered service without any recovery path
                # after an API-pod replacement.  The fenced script upsert has
                # already replaced a same-name predecessor, so only stale
                # reserved-fill claims need to be cleared during registration.
                session.execute(
                    sqlalchemy.delete(reserved_fill_claims_table).where(
                        reserved_fill_claims_table.c.service_name == name))

            session.execute(
                insert_func(services_table).values(
                    name=name,
                    workspace=workspace,
                    controller_job_id=controller_job_id,
                    status=status.value,
                    policy=policy,
                    requested_resources_str=requested_resources_str,
                    load_balancing_policy=load_balancing_policy,
                    tls_encrypted=int(tls_encrypted),
                    pool=int(pool),
                    controller_pid=controller_pid,
                    controller_ip=controller_ip,
                    hash=(str(uuid.uuid4())
                          if service_hash is None else service_hash),
                    **({
                        'owner_user_id': owner_user_id,
                        'owner_user_name': owner_user_name,
                    } if owner_columns_available else {}),
                    lifecycle_epoch=lifecycle_epoch,
                    resource_scope=resource_scope,
                    entrypoint=entrypoint,
                    logical_replica_semantics=int(
                        spec.uses_logical_replicas is True),
                    lb_ha_enabled=int(lb_ha_enabled),
                    lb_active_slot=(lb_ha.LbSlot.A.value
                                    if lb_ha_enabled else None),
                    lb_cutover_generation=1 if lb_ha_enabled else 0,
                    lb_pending_slot=None,
                    lb_cutover_phase=lb_ha.LbCutoverPhase.STABLE.value,
                    lb_drain_started_at=None,
                    lb_demand_handoff_generation=None,
                    lb_demand_handoff_snapshot=None,
                    lb_demand_handoff_complete_at=None,
                    lb_last_demand_snapshot=None))
            initial_version_created_at = time.time()
            projection_values = ({
                'controller_job_projection': controller_job_projection,
                'controller_work_cache': controller_work_cache,
                'worker_placement_projections': worker_placement_projections,
            } if projection_columns_available else {})
            version_insert_stmt = insert_func(version_specs_table).values(
                service_name=name,
                version=constants.INITIAL_VERSION,
                spec=serialized_spec,
                yaml_content=yaml_content,
                submitted_yaml_content=submitted_yaml_content,
                placement_catalog=placement_catalog,
                controller_config=(None if controller_config_snapshot is None
                                   else controller_config_snapshot[0]),
                controller_config_digest=(None
                                          if controller_config_snapshot is None
                                          else controller_config_snapshot[1]),
                controller_config_snapshot_id=(
                    None if controller_config_snapshot is None else
                    controller_config_snapshot[2]),
                **projection_values,
                created_at=initial_version_created_at,
                # Fresh registration establishes v1 as the controller's
                # bootstrap baseline. Persist that fact independently of
                # replica readiness so it remains available at scale-to-zero.
                controller_applied_at=initial_version_created_at,
                created_by=created_by)
            if lifecycle_epoch is None:
                # Compatibility for legacy callers without the distributed
                # name fence: overwrite v1 only, but never delete arbitrary
                # child rows when absence cannot be serialized.
                version_insert_stmt = version_insert_stmt.on_conflict_do_update(
                    index_elements=['service_name', 'version'],
                    set_={
                        'spec': version_insert_stmt.excluded.spec,
                        'yaml_content':
                            version_insert_stmt.excluded.yaml_content,
                        'submitted_yaml_content':
                            version_insert_stmt.excluded.submitted_yaml_content,
                        'placement_catalog':
                            version_insert_stmt.excluded.placement_catalog,
                        **({
                            'controller_job_projection': version_insert_stmt.excluded.controller_job_projection,
                            'controller_work_cache': version_insert_stmt.excluded.controller_work_cache,
                            'worker_placement_projections': version_insert_stmt.excluded.worker_placement_projections,
                        } if projection_columns_available else {}),
                        'controller_config':
                            version_insert_stmt.excluded.controller_config,
                        'controller_config_digest':
                            version_insert_stmt.excluded.
                            controller_config_digest,
                        'controller_config_snapshot_id':
                            version_insert_stmt.excluded.
                            controller_config_snapshot_id,
                        'controller_applied_at':
                            version_insert_stmt.excluded.controller_applied_at,
                    })
            session.execute(version_insert_stmt)
            if storage_generation is not None:
                if resource_scope is None:
                    raise ephemeral_storage_contract.EphemeralStorageContractError(
                        'Scoped storage commit has no exact service owner.')
                _adopt_exact_ephemeral_storage_cleanup_intent(
                    session, name, resource_scope, storage_generation,
                    yaml_content, pool, lifecycle_epoch,
                    initial_version_created_at)
            session.commit()

    except sqlalchemy_exc.IntegrityError as e:
        for msg in _UNIQUE_CONSTRAINT_FAILED_ERROR_MSGS:
            if msg in str(e):
                return False
        raise RuntimeError('Unexpected database error') from e
    return True


def attest_service_owner_user_id(
    authority: 'ordinary_launch_binding.ControllerBindingAuthority',
    frozen_user_id: str,
    frozen_user_name: str,
) -> None:
    """Attest one retained service owner under the exact controller fence."""
    if not common_utils.is_valid_user_hash(frozen_user_id):
        raise ServiceOwnerAuthorityError(
            'Controller has no valid frozen service owner identity.')
    if not isinstance(frozen_user_name, str) or not frozen_user_name:
        raise ServiceOwnerAuthorityError(
            'Controller has no valid frozen service owner name.')
    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ServiceOwnerAuthorityError(
            'Service owner attestation requires central PostgreSQL.')
    with _replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        lifecycle = session.execute(
            sqlalchemy.select(service_lifecycle_fences_table).where(
                service_lifecycle_fences_table.c.name == authority.service_name
            ).with_for_update()).mappings().one_or_none()
        service = session.execute(
            sqlalchemy.select(services_table).where(
                services_table.c.name == authority.service_name).
            with_for_update()).mappings().one_or_none()
        if (lifecycle is None or service is None or
                lifecycle['epoch'] != authority.service_lifecycle_epoch or
                service['hash'] != authority.service_hash or
                service['workspace'] != authority.service_workspace or
                service['lifecycle_epoch'] != authority.service_lifecycle_epoch
                or service['controller_pid'] != authority.controller_pid or
                service['controller_ip'] != authority.controller_ip or
                service['controller_incarnation']
                != authority.controller_incarnation or
                service['controller_owner_epoch']
                != authority.controller_owner_epoch):
            raise ServiceOwnerAuthorityError(
                'Controller lost authority before service-owner attestation.')
        user = session.execute(
            sqlalchemy.select(global_user_state_schema.user_table.c.id).where(
                global_user_state_schema.user_table.c.id == frozen_user_id).
            with_for_update(read=True)).scalar_one_or_none()
        if user != frozen_user_id:
            raise ServiceOwnerAuthorityError(
                'Frozen service owner no longer exists.')
        current_owner = service['owner_user_id']
        current_owner_name = service['owner_user_name']
        if ((current_owner is None) != (current_owner_name is None)):
            raise ServiceOwnerAuthorityError(
                'Durable service owner identity is malformed.')
        if (current_owner is not None and
            (current_owner != frozen_user_id or
             current_owner_name != frozen_user_name)):
            raise ServiceOwnerAuthorityError(
                'Frozen controller identity does not match service owner.')
        if current_owner is None:
            updated = session.execute(
                sqlalchemy.update(services_table).where(
                    services_table.c.name == authority.service_name,
                    services_table.c.hash == authority.service_hash,
                    services_table.c.lifecycle_epoch ==
                    authority.service_lifecycle_epoch,
                    services_table.c.controller_pid == authority.controller_pid,
                    services_table.c.controller_ip == authority.controller_ip,
                    services_table.c.controller_incarnation ==
                    authority.controller_incarnation,
                    services_table.c.controller_owner_epoch ==
                    authority.controller_owner_epoch,
                    services_table.c.owner_user_id.is_(None)).values(
                        owner_user_id=frozen_user_id,
                        owner_user_name=frozen_user_name))
            if updated.rowcount != 1:
                raise ServiceOwnerAuthorityError(
                    'Service owner attestation lost its controller fence.')
        session.commit()


def set_service_workspace_if_owner(service_name: str, workspace: str,
                                   expected_service_hash: str) -> bool:
    """Backfills one legacy service workspace under an incarnation fence."""
    if not isinstance(workspace, str) or not workspace:
        raise ValueError('Service workspace must be a non-empty string.')
    if (not isinstance(expected_service_hash, str) or
            not expected_service_hash):
        raise ValueError('Expected service hash must be a non-empty string.')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            sqlalchemy.or_(services_table.c.workspace.is_(None),
                           services_table.c.workspace == '')).update(
                               {services_table.c.workspace: workspace})
        session.commit()
    return count > 0


def update_service_controller_pid_if_owner(
        service_name: str,
        expected_service_hash: str | None,
        expected_controller_pid: int | None,
        expected_controller_ip: str | None,
        controller_pid: int,
        controller_ip: str | None,
        *,
        expected_lifecycle_epoch: int | None = None,
        expected_status: ServiceStatus | None = None,
        expected_recovery_version: int | None = None) -> bool:
    """Preclaim recovery only if the incarnation and old owner still match.

    A name-only preclaim can overwrite a service that was purged and recreated
    while recovery was loading its spec. The hash fences same-name successors;
    the expected PID+IP fence another recovery process that already claimed the
    original incarnation (PIDs alone collide across Kubernetes pods). On
    success, publish the new PID+IP and clear the port atomically so the stable
    proxy fails closed with 503 until the new controller is actually ready.
    """
    filters = [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
        services_table.c.controller_pid == expected_controller_pid,
        services_table.c.controller_ip == expected_controller_ip,
    ]
    if expected_lifecycle_epoch is not None:
        filters.append(
            services_table.c.lifecycle_epoch == expected_lifecycle_epoch)
    if expected_status is not None:
        filters.append(services_table.c.status == expected_status.value)
    if expected_recovery_version is not None:
        (latest_applicable, latest_quarantined,
         latest_applied_applicable) = _quarantine_aware_version_aggregates()
        elected_version = sqlalchemy.select(
            _quarantine_aware_version_sql_expression(
                latest_applicable, latest_quarantined,
                latest_applied_applicable)).where(
                    version_specs_table.c.service_name ==
                    service_name).scalar_subquery()
        filters.append(elected_version == expected_recovery_version)
    with _replica_launch_authority_write_session(service_name) as (_, session):
        count = session.query(services_table).filter(*filters).update({
            services_table.c.controller_pid: controller_pid,
            services_table.c.controller_ip: controller_ip,
            services_table.c.controller_port: None,
        })
        session.commit()
    return count > 0


def update_service_controller_pid_ip_and_port(
        service_name: str, controller_pid: int, controller_ip: str | None,
        controller_port: int, expected_service_hash: str | None,
        expected_controller_pid: int | None,
        expected_controller_ip: str | None) -> bool:
    """CAS-publish controller pid + IP + port for one service incarnation.

    Used during HA recovery: the controller subprocess on the new pod must be
    listening on the chosen port before we flip DB to point requests at it.
    By updating all three fields in one statement, clients never see a
    half-flipped row (e.g. new pid + old ip, or new ip + stale port that
    points at a different service's listener on the new pod).

    Recovery picks the port locally (find_free_port on the recovery pod) —
    it must NOT reuse the previous pod's port — so the port change has to
    propagate to DB together with the pid/ip flip. The hash and expected owner
    filters prevent a booting process from publishing into a row that was
    purged/recreated or claimed by another recovery during the readiness wait.

    Returns:
        True if the original incarnation and expected owner were updated;
        False if ownership was lost or the row no longer exists.
    """
    with _replica_launch_authority_write_session(service_name) as (_, session):
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == expected_controller_pid,
            services_table.c.controller_ip == expected_controller_ip).update({
                services_table.c.controller_pid: controller_pid,
                services_table.c.controller_ip: controller_ip,
                services_table.c.controller_port: controller_port,
            })
        session.commit()
    return count > 0


def set_service_controller_ip(service_name: str,
                              controller_ip: str | None) -> None:
    """Sets the controller IP of a service."""
    with _replica_launch_authority_write_session(service_name) as (_, session):
        session.query(services_table).filter(
            services_table.c.name == service_name).update(
                {services_table.c.controller_ip: controller_ip})
        session.commit()


@_with_reserved_fill_broker_lock
def remove_service(service_name: str) -> None:
    """Removes a service from the database."""
    with _replica_launch_authority_write_session(service_name) as (_, session):
        session.execute(
            sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name))
        session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(paid_capacity_claims_table).where(
                paid_capacity_claims_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(paid_capacity_waiters_table).where(
                paid_capacity_waiters_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(services_table).where(
                services_table.c.name == service_name))
        session.commit()


def service_owner_matches(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Whether the exact service incarnation (and optional owner) exists."""
    if not expected_service_hash:
        return False
    predicates = [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
    ]
    if expected_controller_owner is not None:
        expected_pid, expected_ip = expected_controller_owner
        predicates.extend([
            services_table.c.controller_pid == expected_pid,
            services_table.c.controller_ip == expected_ip,
        ])
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.name).where(*predicates)).fetchone()
    return row is not None


def service_uses_logical_replica_semantics(service_name: str) -> bool:
    """Whether logical replica semantics were durably activated."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(services_table.c.logical_replica_semantics).where(
                services_table.c.name == service_name)).fetchone()
    return bool(row[0]) if row is not None else False


@_with_reserved_fill_broker_lock
def remove_service_completely(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    expected_lifecycle_epoch: int | None = None,
) -> bool:
    """Atomically remove one exact service incarnation and all child rows.

    Deletes from `services`, `replicas`, `version_specs`,
    `serve_ha_recovery_script`, `reserved_fill_claims`, and the exact
    incarnation's storage cleanup intents in a single transaction. These were
    the tables whose sequential
    teardown left orphan rows when a subprocess died mid-cleanup; the
    claim row must go too, or a torn-down fill-enabled service keeps
    absorbing broker entitlement until its claim TTL expires.

    The service row is conditionally deleted first inside the transaction.
    If its durable hash (and, for a live controller, PID/IP owner) no longer
    matches, no child table is touched. Once that delete succeeds, a same-name
    successor cannot insert until this transaction commits, so deleting the
    child rows cannot cross an A-to-B reuse boundary.

    Returns:
        True when the expected incarnation was removed; False when ownership
        was already lost and nothing was changed.
    """
    if not expected_service_hash:
        return False
    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        _begin_immediate_if_sqlite(session, engine, expected_lifecycle_epoch
                                   is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        predicates = [
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
        ]
        if expected_lifecycle_epoch is not None:
            predicates.append(
                services_table.c.lifecycle_epoch == expected_lifecycle_epoch)
        if expected_controller_owner is not None:
            expected_pid, expected_ip = expected_controller_owner
            predicates.extend([
                services_table.c.controller_pid == expected_pid,
                services_table.c.controller_ip == expected_ip,
            ])
        service_row = session.execute(
            sqlalchemy.select(services_table.c.resource_scope).where(
                *predicates).with_for_update()).fetchone()
        if service_row is None:
            session.rollback()
            return False
        resource_scope = service_row[0]
        session.execute(sqlalchemy.delete(services_table).where(*predicates))
        session.execute(
            sqlalchemy.delete(replicas_table).where(
                replicas_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(version_specs_table).where(
                version_specs_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(serve_ha_recovery_script_table).where(
                serve_ha_recovery_script_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name))
        session.execute(
            sqlalchemy.delete(paid_capacity_claims_table).where(
                paid_capacity_claims_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(paid_capacity_waiters_table).where(
                paid_capacity_waiters_table.c.service_name == service_name))
        if resource_scope is not None:
            session.execute(
                sqlalchemy.delete(
                    ephemeral_storage_cleanup_intents_table).where(
                        ephemeral_storage_cleanup_intents_table.c.service_name
                        == service_name,
                        ephemeral_storage_cleanup_intents_table.c.resource_scope
                        == resource_scope))
        session.commit()
    return True


def set_service_uptime(
    service_name: str,
    uptime: int,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Set uptime, optionally fenced to one service incarnation."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        predicates = [services_table.c.name == service_name]
        if expected_service_hash is not None:
            predicates.append(services_table.c.hash == expected_service_hash)
        if expected_controller_owner is not None:
            expected_pid, expected_ip = expected_controller_owner
            predicates.extend([
                services_table.c.controller_pid == expected_pid,
                services_table.c.controller_ip == expected_ip,
            ])
        count = session.query(services_table).filter(*predicates).update(
            {services_table.c.uptime: uptime})
        session.commit()
    return count > 0


def _revoke_routes_for_service_status_in_session(
    session: orm.Session,
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    status: ServiceStatus,
    active_versions: list[int] | None,
    updated_count: int,
) -> None:
    """Keep service/version route retirement atomic with its state write."""
    if (updated_count < 1 or
            engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        return
    if status in ServiceStatus.replica_launch_blocking_statuses():
        route_projection.revoke_service_leases_in_session(
            session, service_name, 'service_became_route_ineligible')
    elif active_versions is not None:
        route_projection.revoke_service_leases_in_session(
            session,
            service_name,
            'service_version_retired',
            active_versions=set(active_versions))


def set_service_status_and_active_versions(
        service_name: str,
        status: ServiceStatus,
        active_versions: list[int] | None = None) -> None:
    """Sets the service status."""
    update_dict = {services_table.c.status: status.value}
    if active_versions is not None:
        update_dict[services_table.c.active_versions] = json.dumps(
            active_versions)

    with _replica_launch_authority_write_session(
            service_name,
            invalidates_launch_authority=status
            in ServiceStatus.replica_launch_blocking_statuses()) as (engine,
                                                                     session):
        count = session.query(services_table).filter(
            services_table.c.name == service_name).update(update_dict)
        _revoke_routes_for_service_status_in_session(session, engine,
                                                     service_name, status,
                                                     active_versions, count)
        session.commit()


def set_service_status_and_active_versions_if_owner(
        service_name: str,
        expected_service_hash: str,
        expected_controller_pid: int | None,
        expected_controller_ip: str | None,
        status: ServiceStatus,
        active_versions: list[int] | None = None,
        expected_status: ServiceStatus | None = None,
        expected_lifecycle_epoch: int | None = None) -> bool:
    """CAS a status write on the exact hash/PID/IP controller owner."""
    update_dict = {services_table.c.status: status.value}
    if active_versions is not None:
        update_dict[services_table.c.active_versions] = json.dumps(
            active_versions)
    predicates = [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
        services_table.c.controller_pid == expected_controller_pid,
        services_table.c.controller_ip == expected_controller_ip,
    ]
    if expected_status is not None:
        predicates.append(services_table.c.status == expected_status.value)
    if expected_lifecycle_epoch is not None:
        predicates.append(
            services_table.c.lifecycle_epoch == expected_lifecycle_epoch)
    with _replica_launch_authority_write_session(
            service_name,
            invalidates_launch_authority=status
            in ServiceStatus.replica_launch_blocking_statuses()) as (engine,
                                                                     session):
        _begin_immediate_if_sqlite(session, engine, expected_lifecycle_epoch
                                   is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        count = session.query(services_table).filter(
            *predicates).update(update_dict)
        _revoke_routes_for_service_status_in_session(session, engine,
                                                     service_name, status,
                                                     active_versions, count)
        session.commit()
    return count > 0


def set_service_status_and_active_versions_if_hash(
        service_name: str,
        expected_service_hash: str,
        status: ServiceStatus,
        active_versions: list[int] | None = None,
        expected_status: ServiceStatus | None = None,
        expected_lifecycle_epoch: int | None = None) -> bool:
    """CAS a status write on a durable service incarnation."""
    update_dict = {services_table.c.status: status.value}
    if active_versions is not None:
        update_dict[services_table.c.active_versions] = json.dumps(
            active_versions)
    predicates = [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
    ]
    if expected_status is not None:
        predicates.append(services_table.c.status == expected_status.value)
    if expected_lifecycle_epoch is not None:
        predicates.append(
            services_table.c.lifecycle_epoch == expected_lifecycle_epoch)
    with _replica_launch_authority_write_session(
            service_name,
            invalidates_launch_authority=status
            in ServiceStatus.replica_launch_blocking_statuses()) as (engine,
                                                                     session):
        _begin_immediate_if_sqlite(session, engine, expected_lifecycle_epoch
                                   is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        count = session.query(services_table).filter(
            *predicates).update(update_dict)
        _revoke_routes_for_service_status_in_session(session, engine,
                                                     service_name, status,
                                                     active_versions, count)
        session.commit()
    return count > 0


def set_service_controller_port(service_name: str,
                                controller_port: int) -> None:
    """Sets the controller port of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update(
                {services_table.c.controller_port: controller_port})
        session.commit()


def set_service_controller_port_if_owner(service_name: str,
                                         expected_service_hash: str | None,
                                         controller_pid: int,
                                         controller_ip: str | None,
                                         controller_port: int) -> bool:
    """Sets the controller port only if `controller_pid` still owns the row.

    Compare-and-swap for the in-place controller respawn: a parent whose
    ownership has been taken over by HA recovery on another pod (which
    atomically flipped pid/ip/port) must not clobber the new owner's port.
    Hash + PID + IP are all required: the hash changes on same-name reuse,
    while namespace-local PIDs can be identical on two pods.

    Returns:
        True if the full owner tuple still owns the service, False if
        ownership was lost or the row no longer exists.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == controller_pid,
            services_table.c.controller_ip == controller_ip).update(
                {services_table.c.controller_port: controller_port})
        session.commit()
    return count > 0


def acknowledge_service_controller_teardown_if_owner(
        service_name: str, expected_service_hash: str, controller_pid: int,
        controller_ip: str | None) -> bool:
    """Atomically enter teardown and publish that the child is gone.

    Unexpected parent failures can reach finalization from a routable status
    such as READY. Publishing SHUTTING_DOWN in the same exact-owner write as
    the child-teardown sentinel prevents update/apply from starting new work
    while cleanup waits for the lifecycle lock.
    """
    with _replica_launch_authority_write_session(service_name) as (_, session):
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == controller_pid,
            services_table.c.controller_ip == controller_ip).update({
                services_table.c.status: ServiceStatus.SHUTTING_DOWN.value,
                services_table.c.controller_port:
                    constants.CONTROLLER_TEARDOWN_ACK_PORT
            })
        session.commit()
    return count > 0


def claim_orphaned_service_teardown(
        service_name: str,
        expected_service_hash: str,
        expected_controller_pid: int | None,
        expected_controller_ip: str | None,
        controller_pid: int,
        controller_ip: str | None,
        expected_lifecycle_epoch: int | None = None) -> bool:
    """Claim a terminal row that has no recovery script or live child.

    Callers must establish the absence of a recovery script while holding the
    per-service lifecycle lock. The status predicate prevents claiming a
    healthy incarnation.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, expected_lifecycle_epoch
                                   is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == expected_controller_pid,
            services_table.c.controller_ip == expected_controller_ip,
            services_table.c.status == ServiceStatus.SHUTTING_DOWN.value,
            *([services_table.c.lifecycle_epoch == expected_lifecycle_epoch]
              if expected_lifecycle_epoch is not None else []),
        ).update({
            services_table.c.controller_pid: controller_pid,
            services_table.c.controller_ip: controller_ip,
            services_table.c.controller_port:
                constants.CONTROLLER_TEARDOWN_ACK_PORT,
        })
        session.commit()
    return count > 0


def claim_unrecoverable_service_teardown(
        service_name: str,
        expected_service_hash: str,
        expected_controller_pid: int | None,
        expected_controller_ip: str | None,
        controller_pid: int,
        controller_ip: str | None,
        expected_lifecycle_epoch: int | None = None) -> bool:
    """Claim a terminal service that has no bootable version.

    A recovery script cannot make progress without a committed yaml version.
    Atomically require that invariant, move controller ownership to the purge
    process, publish the teardown acknowledgement, and delete the now-useless
    recovery script.  This lets purge recover old partial-registration rows
    without treating the mere presence of an impossible script as evidence of
    a controller that may still come back.
    """
    committed_version_exists = sqlalchemy.exists().where(
        version_specs_table.c.service_name == service_name,
        version_specs_table.c.yaml_content.isnot(None))
    predicates = [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
        services_table.c.controller_pid == expected_controller_pid,
        services_table.c.controller_ip == expected_controller_ip,
        services_table.c.status == ServiceStatus.SHUTTING_DOWN.value,
        ~committed_version_exists,
    ]
    if expected_lifecycle_epoch is not None:
        predicates.append(
            services_table.c.lifecycle_epoch == expected_lifecycle_epoch)
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, True)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.controller_pid: controller_pid,
            services_table.c.controller_ip: controller_ip,
            services_table.c.controller_port:
                constants.CONTROLLER_TEARDOWN_ACK_PORT,
        })
        if count == 0:
            session.rollback()
            return False
        session.execute(
            sqlalchemy.delete(serve_ha_recovery_script_table).where(
                serve_ha_recovery_script_table.c.service_name == service_name))
        session.commit()
    return True


def mark_unrecoverable_service_for_cleanup(service_name: str,
                                           expected_service_hash: str,
                                           pool: bool) -> bool:
    """Retire an HA recovery script that can never boot a controller.

    The status transition and script removal are conditional on there still
    being no committed yaml version.  A concurrent version commit therefore
    wins atomically; otherwise future recovery sweeps stop launching an
    impossible script and ``down --purge`` can claim the orphan immediately.
    """
    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        _begin_immediate_if_sqlite(session, engine, True)
        # Lock the authoritative service row before checking for committed
        # versions.  On PostgreSQL an UPDATE whose WHERE includes NOT EXISTS
        # can take its statement snapshot before it waits on a concurrent
        # version writer's service-row lock.  That stale snapshot can then
        # miss the just-committed yaml and incorrectly retire a bootable
        # service.  A separate post-lock SELECT gets a fresh READ COMMITTED
        # snapshot after the writer has committed.
        service_row = session.execute(
            sqlalchemy.select(services_table.c.hash, services_table.c.pool,
                              services_table.c.status).where(
                                  services_table.c.name ==
                                  service_name).with_for_update()).fetchone()
        if (service_row is None or service_row.hash != expected_service_hash or
                bool(service_row.pool) != pool or
                service_row.status == ServiceStatus.SHUTTING_DOWN.value):
            session.rollback()
            return False
        committed_version_exists = session.execute(
            sqlalchemy.select(sqlalchemy.exists().where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.yaml_content.isnot(None)))).scalar()
        if committed_version_exists:
            session.rollback()
            return False
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.pool == int(pool),
            services_table.c.status != ServiceStatus.SHUTTING_DOWN.value,
        ).update({
            services_table.c.status: ServiceStatus.FAILED_CLEANUP.value,
        })
        if count == 0:
            session.rollback()
            return False
        session.execute(
            sqlalchemy.delete(serve_ha_recovery_script_table).where(
                serve_ha_recovery_script_table.c.service_name == service_name))
        session.commit()
    return True


def set_service_load_balancer_port_if_owner(service_name: str,
                                            expected_service_hash: str | None,
                                            controller_pid: int,
                                            controller_ip: str | None,
                                            load_balancer_port: int) -> bool:
    """Sets the load balancer port only if `controller_pid` owns the row.

    Compare-and-swap for external-LB port publication: the plain setter below
    is a name-only write, so a stale process racing a purge + same-name re-up
    could write to the successor's row and prematurely unblock registration.
    Filtering on hash + PID + IP makes the full ownership check and write one
    atomic UPDATE.

    Returns:
        True if the full owner tuple owns the service, False if ownership was
        lost or the row no longer exists.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == controller_pid,
            services_table.c.controller_ip == controller_ip).update(
                {services_table.c.load_balancer_port: load_balancer_port})
        session.commit()
    return count > 0


def set_service_load_balancer_port(service_name: str,
                                   load_balancer_port: int) -> None:
    """Sets the load balancer port of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update(
                {services_table.c.load_balancer_port: load_balancer_port})
        session.commit()


def _get_service_from_row(r: 'row.RowMapping') -> dict[str, Any]:
    # Get the max_version from the first column (from the subquery)
    current_version = r['max_version']

    record = {
        'name': r['name'],
        'controller_job_id': r['controller_job_id'],
        'controller_port': r['controller_port'],
        'load_balancer_port': r['load_balancer_port'],
        'status': ServiceStatus[r['status']],
        'uptime': r['uptime'],
        'policy': r['policy'],
        # The version of the autoscaler/replica manager are on. It can be larger
        # than the active versions as the load balancer may not consider the
        # latest version to be active for serving traffic.
        'version': current_version,
        # The version selected by the latest durable update. This is separate
        # from active_versions because a safe rollout can take time to move
        # traffic to the elected configuration.
        'elected_version': r['current_version'],
        # The versions that is active for the load balancer. This is a list of
        # integers in json format. This is mainly for display purpose.
        'active_versions': json.loads(r['active_versions'])
                           if r['active_versions'] else [],
        'requested_resources_str': r['requested_resources_str'],
        'load_balancing_policy': r['load_balancing_policy'],
        'tls_encrypted': bool(r['tls_encrypted']),
        'pool': bool(r['pool']),
        'controller_pid': r['controller_pid'],
        'controller_ip': r['controller_ip'],
        'workspace': r['workspace'],
        'hash': r['hash'],
        'lifecycle_epoch': r['lifecycle_epoch'],
        'resource_scope': r['resource_scope'],
        'entrypoint': r['entrypoint'],
        'logical_replica_semantics': bool(r['logical_replica_semantics']),
        'lb_ha_enabled': bool(r['lb_ha_enabled']),
        'lb_active_slot': r['lb_active_slot'],
        'lb_cutover_generation': r['lb_cutover_generation'],
        'lb_pending_slot': r['lb_pending_slot'],
        'lb_cutover_phase': r['lb_cutover_phase'],
        'yaml_content': r.get('yaml_content'),
    }
    latest_spec = pickle.loads(r['spec']) if r.get('spec') is not None else None
    if latest_spec is not None:
        record['policy'] = latest_spec.autoscaling_policy_str()
        record['load_balancing_policy'] = latest_spec.load_balancing_policy
    return record


def _build_services_with_latest_version_query(
        service_name: str | None = None) -> sqlalchemy.sql.Select:
    """Build a query joining services with their latest version metadata.

    Args:
        service_name: If provided, filter to this service only.

    Returns:
        A SQLAlchemy selectable for fetching rows, including columns:
        - max_version (latest version per service)
        - Serve037 service columns (the legacy adapter contract)
        - spec/yaml_content (from version_specs_table for latest version)
    """
    subquery = sqlalchemy.select(
        version_specs_table.c.service_name,
        sqlalchemy.func.max(version_specs_table.c.version).label('max_version'),
    ).group_by(version_specs_table.c.service_name).alias('v')

    query = sqlalchemy.select(
        subquery.c.max_version,
        *_SERVE037_SERVICE_COLUMNS,
        version_specs_table.c.spec,
        version_specs_table.c.yaml_content,
    ).select_from(
        services_table.join(
            subquery, services_table.c.name == subquery.c.service_name).join(
                version_specs_table,
                sqlalchemy.and_(
                    version_specs_table.c.service_name == services_table.c.name,
                    version_specs_table.c.version == subquery.c.max_version,
                ),
            ))
    if service_name is not None:
        query = query.where(services_table.c.name == service_name)
    return query


def get_services() -> list[dict[str, Any]]:
    """Get all existing service records."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = _build_services_with_latest_version_query()
        rows = session.execute(query).fetchall()
    records = []
    for row in rows:
        records.append(_get_service_from_row(row._mapping))  # pylint: disable=protected-access
    return records


def get_num_services(pool: bool | None = None) -> int:
    """Get the number of raw service rows, optionally filtered by mode."""
    engine = _db_manager.get_engine()
    query = sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(services_table)
    if pool is not None:
        query = query.where(services_table.c.pool == int(pool))
    with orm.Session(engine) as session:
        return session.execute(query).fetchone()[0]


def service_owner_attestation_transition_active() -> bool:
    """Whether the schema can still admit a service without an owner.

    User deletion must fail closed for the whole Serve055 transition.  A
    per-user scan cannot close the race with an old writer that is still able
    to insert a NULL owner tuple.
    """
    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return True
    columns = {
        str(column['name']): column
        for column in sqlalchemy.inspect(engine).get_columns('services')
    }
    return any(name not in columns or columns[name].get('nullable') is not False
               for name in ('owner_user_id', 'owner_user_name'))


def get_service_names_owned_by_user_id(owner_user_id: str) -> list[str]:
    """List durable services that prevent deletion of one owner identity."""
    if not isinstance(owner_user_id, str) or not owner_user_id:
        raise ValueError('Service owner user ID is invalid.')
    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return []
    if 'owner_user_id' not in {
            column['name']
            for column in sqlalchemy.inspect(engine).get_columns('services')
    }:
        return []
    with orm.Session(engine) as session:
        return list(
            session.execute(
                sqlalchemy.select(services_table.c.name).where(
                    services_table.c.owner_user_id == owner_user_id).order_by(
                        services_table.c.name)).scalars())


def get_service_from_name(service_name: str) -> dict[str, Any] | None:
    """Get all existing service records."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = _build_services_with_latest_version_query(service_name)
        rows = session.execute(query).fetchall()
    for row in rows:
        return _get_service_from_row(row._mapping)  # pylint: disable=protected-access
    return None


def get_service_liveness_snapshots(pool: bool) -> list[dict[str, Any]]:
    """Read the slim, version-backed rows used by the liveness sweep.

    Materializing all rows in one statement keeps a sweep on one database
    snapshot and avoids repeatedly joining and deserializing latest-version
    specs.  Requiring a version row preserves ``get_service_from_name()``'s
    behavior for orphan service rows left by interrupted legacy registration.

    ``yaml_content`` carries the latest version's raw yaml (possibly NULL for
    a placeholder version row) so liveness callers can detect unbootable
    placeholder rows without a per-service joined read. ``recovery_version``
    is elected from that same snapshot using the quarantine-aware controller
    recovery policy. Only the presence of its immutable controller config is
    projected here; callers fetch and verify the bytes only when the config
    protocol is active.
    """
    latest_version = sqlalchemy.select(
        version_specs_table.c.service_name,
        sqlalchemy.func.max(version_specs_table.c.version).label('max_version'),
    ).group_by(version_specs_table.c.service_name).alias('v')
    (latest_applicable, latest_quarantined,
     latest_applied_applicable) = _quarantine_aware_version_aggregates()
    config_protocol_active = sqlalchemy.func.max(
        sqlalchemy.case((sqlalchemy.or_(
            version_specs_table.c.controller_config.isnot(None),
            version_specs_table.c.controller_config_digest.isnot(None),
            version_specs_table.c.controller_config_snapshot_id.isnot(None),
        ), 1),
                        else_=0)).label('config_protocol_active')
    version_candidates = sqlalchemy.select(
        version_specs_table.c.service_name,
        latest_applicable,
        latest_quarantined,
        latest_applied_applicable,
        config_protocol_active,
    ).group_by(version_specs_table.c.service_name).alias('version_candidates')
    recovery_versions = sqlalchemy.select(
        version_candidates.c.service_name,
        _quarantine_aware_version_sql_expression(
            version_candidates.c.latest_applicable_version,
            version_candidates.c.latest_quarantined_version,
            version_candidates.c.latest_applied_applicable_version,
        ).label('recovery_version'),
        version_candidates.c.config_protocol_active,
    ).alias('recovery_versions')
    latest_version_spec = version_specs_table.alias('latest_version_spec')
    recovery_version_spec = version_specs_table.alias('recovery_version_spec')
    recovery_config_present = sqlalchemy.and_(
        recovery_version_spec.c.controller_config.isnot(None),
        recovery_version_spec.c.controller_config_digest.isnot(None),
        recovery_version_spec.c.controller_config_snapshot_id.isnot(None),
    ).label('recovery_config_present')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                services_table.c.name,
                services_table.c.status,
                services_table.c.controller_job_id,
                services_table.c.controller_pid,
                services_table.c.controller_ip,
                services_table.c.hash,
                services_table.c.resource_scope,
                services_table.c.workspace,
                latest_version_spec.c.yaml_content,
                recovery_versions.c.recovery_version,
                recovery_versions.c.config_protocol_active,
                recovery_config_present,
            ).select_from(
                services_table.join(
                    latest_version,
                    services_table.c.name == latest_version.c.service_name).
                join(
                    latest_version_spec,
                    sqlalchemy.and_(
                        latest_version_spec.c.service_name ==
                        services_table.c.name,
                        latest_version_spec.c.version ==
                        latest_version.c.max_version,
                    )).join(
                        recovery_versions, recovery_versions.c.service_name ==
                        services_table.c.name).outerjoin(
                            recovery_version_spec,
                            sqlalchemy.and_(
                                recovery_version_spec.c.service_name ==
                                services_table.c.name,
                                recovery_version_spec.c.version ==
                                recovery_versions.c.recovery_version,
                            ))).where(
                                services_table.c.pool == int(pool)).order_by(
                                    services_table.c.name)).fetchall()
    return [{
        'name': row.name,
        'status': ServiceStatus[row.status],
        'controller_job_id': row.controller_job_id,
        'controller_pid': row.controller_pid,
        'controller_ip': row.controller_ip,
        'hash': row.hash,
        'resource_scope': row.resource_scope,
        'workspace': row.workspace,
        'yaml_content': row.yaml_content,
        'recovery_version': row.recovery_version,
        'config_protocol_active': bool(row.config_protocol_active),
        'recovery_config_present': bool(row.recovery_config_present),
    } for row in rows]


def get_service_runtime_snapshot(
        service_name: str,
        require_version: bool = False) -> dict[str, Any] | None:
    """Read the slim runtime fields used by controller control loops.

    Unlike :func:`get_service_from_name`, this helper stays on the
    ``services`` table: no ``version_specs`` join and no latest-spec
    deserialization. ``require_version`` preserves callers whose old joined
    read treated an orphan/versionless service row as missing.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            services_table.c.hash,
            services_table.c.controller_pid,
            services_table.c.controller_ip,
            services_table.c.active_versions,
        ).where(services_table.c.name == service_name)
        if require_version:
            query = query.where(sqlalchemy.exists().where(
                version_specs_table.c.service_name == services_table.c.name))
        row = session.execute(query).fetchone()
    if row is None:
        return None
    mapping = row._mapping  # pylint: disable=protected-access
    return {
        'hash': mapping['hash'],
        'controller_pid': mapping['controller_pid'],
        'controller_ip': mapping['controller_ip'],
        'active_versions': json.loads(mapping['active_versions'])
                           if mapping['active_versions'] else [],
    }


def _select_quarantine_aware_version(
        latest_applicable_version: int | None,
        latest_quarantined_version: int | None,
        latest_applied_applicable_version: int | None) -> int | None:
    """Choose the same version used to reconstruct a Serve controller."""
    if (latest_quarantined_version is not None and
        (latest_applicable_version is None or
         latest_applicable_version < latest_quarantined_version)):
        # A committed intermediate generation is not proof that the
        # controller transitioned to it.  Fall back only to a generation with
        # a durable applied receipt; None deliberately fails closed.
        return latest_applied_applicable_version
    return latest_applicable_version


def _quarantine_aware_version_aggregates() -> tuple[Any, Any, Any]:
    """Build the common SQL candidates for launch/lifecycle election."""
    committed = version_specs_table.c.yaml_content.isnot(None)
    applicable = sqlalchemy.and_(committed,
                                 version_specs_table.c.quarantined_at.is_(None))
    latest_applicable = sqlalchemy.func.max(
        sqlalchemy.case(
            (applicable,
             version_specs_table.c.version))).label('latest_applicable_version')
    latest_quarantined = sqlalchemy.func.max(
        sqlalchemy.case((
            version_specs_table.c.quarantined_at.isnot(None),
            version_specs_table.c.version))).label('latest_quarantined_version')
    latest_applied_applicable = sqlalchemy.func.max(
        sqlalchemy.case((sqlalchemy.and_(
            applicable,
            version_specs_table.c.controller_applied_at.isnot(None)),
                         version_specs_table.c.version
                        ))).label('latest_applied_applicable_version')
    return (latest_applicable, latest_quarantined, latest_applied_applicable)


def _quarantine_aware_version_sql_expression(
        latest_applicable: Any, latest_quarantined: Any,
        latest_applied_applicable: Any) -> Any:
    """Build the SQL equivalent of `_select_quarantine_aware_version`."""
    quarantine_dominates = sqlalchemy.and_(
        latest_quarantined.isnot(None),
        sqlalchemy.or_(latest_applicable.is_(None), latest_applicable
                       < latest_quarantined))
    return sqlalchemy.case((quarantine_dominates, latest_applied_applicable),
                           else_=latest_applicable)


def get_service_ha_recovery_snapshot(
    service_name: str,
    expected_service_hash: str | None = None,
) -> dict[str, Any] | None:
    """Read one just-in-time, incarnation-fenced HA recovery snapshot.

    The service owner, quarantine-aware elected version, protocol activation,
    and exact selected controller-config bytes must come from one PostgreSQL
    statement.  In particular, a long fleet sweep must not pair an old
    incarnation or version election with a same-name successor's recovery
    script.  No pickled service spec is selected or deserialized on this path.

    Args:
        service_name: Service whose controller may need recovery.
        expected_service_hash: Optional incarnation observed by an earlier
            liveness sweep.  A mismatch returns ``None``.

    Returns:
        A service/owner/election snapshot, or ``None`` if the service (or exact
        expected incarnation) no longer exists. ``controller_config_snapshot``
        is the verified ``(bytes, digest, nonce)`` tuple for the selected
        generation, and is ``None`` for a legacy or unelectable generation.

    Raises:
        ControllerConfigCorruptionError: If the selected config tuple is
            partial or fails its digest validation.
    """
    if expected_service_hash is not None and (not isinstance(
            expected_service_hash, str) or not expected_service_hash):
        raise ValueError('Expected service hash must be a non-empty string.')

    (latest_applicable, latest_quarantined,
     latest_applied_applicable) = _quarantine_aware_version_aggregates()
    config_protocol_active = sqlalchemy.func.max(
        sqlalchemy.case((sqlalchemy.or_(
            version_specs_table.c.controller_config.isnot(None),
            version_specs_table.c.controller_config_digest.isnot(None),
            version_specs_table.c.controller_config_snapshot_id.isnot(None),
        ), 1),
                        else_=0)).label('config_protocol_active')
    candidates = sqlalchemy.select(
        version_specs_table.c.service_name.label('service_name'),
        latest_applicable,
        latest_quarantined,
        latest_applied_applicable,
        config_protocol_active,
    ).where(version_specs_table.c.service_name == service_name).group_by(
        version_specs_table.c.service_name).subquery('ha_recovery_candidates')
    recovery_version = _quarantine_aware_version_sql_expression(
        candidates.c.latest_applicable_version,
        candidates.c.latest_quarantined_version,
        candidates.c.latest_applied_applicable_version).label(
            'recovery_version')
    election = sqlalchemy.select(
        candidates.c.service_name,
        recovery_version,
        candidates.c.config_protocol_active,
    ).subquery('ha_recovery_election')
    selected_config = version_specs_table.alias('ha_recovery_selected_config')

    query = sqlalchemy.select(
        services_table.c.name,
        services_table.c.hash,
        services_table.c.lifecycle_epoch,
        services_table.c.controller_pid,
        services_table.c.controller_ip,
        services_table.c.workspace,
        services_table.c.resource_scope,
        services_table.c.status,
        election.c.recovery_version,
        election.c.config_protocol_active,
        selected_config.c.controller_config,
        selected_config.c.controller_config_digest,
        selected_config.c.controller_config_snapshot_id,
        serve_ha_recovery_script_table.c.script.label('ha_recovery_script'),
    ).select_from(
        services_table.outerjoin(
            election,
            election.c.service_name == services_table.c.name).outerjoin(
                selected_config,
                sqlalchemy.and_(
                    selected_config.c.service_name == services_table.c.name,
                    selected_config.c.version == election.c.recovery_version,
                )).outerjoin(
                    serve_ha_recovery_script_table,
                    serve_ha_recovery_script_table.c.service_name ==
                    services_table.c.name)).where(
                        services_table.c.name == service_name)
    if expected_service_hash is not None:
        query = query.where(services_table.c.hash == expected_service_hash)

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(query).fetchone()
    if row is None:
        return None

    controller_config = row.controller_config
    if isinstance(controller_config, memoryview):
        controller_config = controller_config.tobytes()
    try:
        controller_config_snapshot = _validate_controller_config_snapshot(
            controller_config,
            row.controller_config_digest,
            row.controller_config_snapshot_id,
            argument_name='HA recovery controller config snapshot')
    except ValueError as e:
        raise ControllerConfigCorruptionError(
            f'Controller config snapshot for service {service_name!r}, '
            f'version {row.recovery_version} failed integrity validation: '
            f'{e}') from e

    return {
        'service_name': row.name,
        'hash': row.hash,
        'lifecycle_epoch': row.lifecycle_epoch,
        'controller_pid': row.controller_pid,
        'controller_ip': row.controller_ip,
        'workspace': row.workspace,
        'resource_scope': row.resource_scope,
        'status': ServiceStatus[row.status],
        'recovery_version': row.recovery_version,
        'config_protocol_active': bool(row.config_protocol_active),
        'controller_config_snapshot': controller_config_snapshot,
        'ha_recovery_script': row.ha_recovery_script,
    }


def get_service_version_terminal_states(
    identities: list[tuple[str, int,
                           str]],) -> dict[tuple[str, int, str], bool]:
    """Returns authoritative terminal state for bounded service versions.

    Missing entries are intentionally unknown. A service version is live while
    it is current, routed, owns any replica row, or has a non-quarantined
    controller-applied receipt. Applied history is retained for the service
    lifetime because a later quarantine can legally elect it again. A version
    otherwise becomes terminal only after Serve's rollout/drain state has moved
    past it, or after its exact service incarnation is gone.
    """
    if not identities:
        return {}
    if len(identities) > 1000:
        raise ValueError('Service-version terminal-state batch is too large.')
    version_identities = sorted({
        (identity[0], identity[1]) for identity in identities
    })
    engine = _db_manager.get_engine()
    version_states = []
    with orm.Session(engine) as session:
        for start in range(0, len(version_identities),
                           _TERMINAL_IDENTITY_QUERY_BATCH_SIZE):
            version_batch = version_identities[
                start:start + _TERMINAL_IDENTITY_QUERY_BATCH_SIZE]
            wanted = sqlalchemy.values(
                sqlalchemy.column('service_name', sqlalchemy.Text),
                sqlalchemy.column('version', sqlalchemy.Integer),
            ).data(version_batch).cte('wanted_service_versions')
            version_exists = sqlalchemy.exists(
                sqlalchemy.select(sqlalchemy.literal(1)).where(
                    version_specs_table.c.service_name == wanted.c.service_name,
                    version_specs_table.c.version == wanted.c.version))
            applied_nonquarantined_exists = sqlalchemy.exists(
                sqlalchemy.select(sqlalchemy.literal(1)).where(
                    version_specs_table.c.service_name == wanted.c.service_name,
                    version_specs_table.c.version == wanted.c.version,
                    version_specs_table.c.yaml_content.isnot(None),
                    version_specs_table.c.controller_applied_at.isnot(None),
                    version_specs_table.c.quarantined_at.is_(None)))
            replica_exists = sqlalchemy.exists(
                sqlalchemy.select(sqlalchemy.literal(1)).where(
                    replicas_table.c.service_name == wanted.c.service_name,
                    replicas_table.c.version == wanted.c.version))
            version_states.extend(
                session.execute(
                    sqlalchemy.select(
                        wanted.c.service_name,
                        wanted.c.version,
                        services_table.c.hash,
                        services_table.c.status,
                        services_table.c.current_version,
                        services_table.c.active_versions,
                        version_exists.label('version_exists'),
                        applied_nonquarantined_exists.label(
                            'applied_nonquarantined_exists'),
                        replica_exists.label('replica_exists'),
                    ).select_from(
                        wanted.outerjoin(
                            services_table, services_table.c.name ==
                            wanted.c.service_name))).mappings().all())
    states = {
        (str(row['service_name']), int(row['version'])): row
        for row in version_states
    }
    result: dict[tuple[str, int, str], bool] = {}
    for identity in identities:
        name, version, service_hash = identity
        row = states.get((name, version))
        if row is None or row['hash'] != service_hash:
            result[identity] = True
            continue
        if ServiceStatus[str(
                row['status'])] in ServiceStatus.terminal_statuses():
            result[identity] = True
            continue
        if not row['version_exists']:
            # A matching live incarnation without the claimed immutable version
            # is inconsistent, not proof that the owner is terminal.
            continue
        current_version = row['current_version']
        active_versions = (json.loads(row['active_versions'])
                           if row['active_versions'] else [])
        if (current_version == version or version in active_versions or
                row['replica_exists'] or row['applied_nonquarantined_exists']):
            result[identity] = False
        elif current_version is not None and version < int(current_version):
            result[identity] = True
    return result


def get_service_status_snapshot(
        service_name: str,
        require_version: bool = False) -> dict[str, Any] | None:
    """Read the slim status fields used by control and liveness helpers.

    Unlike :func:`get_service_from_name`, this helper stays on the
    ``services`` table: no ``version_specs`` join and no latest-spec
    deserialization. ``require_version`` preserves callers whose old joined
    read treated an orphan/versionless service row as missing.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            services_table.c.name,
            services_table.c.controller_job_id,
            services_table.c.controller_port,
            services_table.c.load_balancer_port,
            services_table.c.status,
            services_table.c.pool,
            services_table.c.controller_pid,
            services_table.c.controller_ip,
            services_table.c.hash,
            services_table.c.lifecycle_epoch,
            services_table.c.resource_scope,
            services_table.c.workspace,
            services_table.c.uptime,
            services_table.c.policy,
            services_table.c.requested_resources_str,
            services_table.c.load_balancing_policy,
            services_table.c.tls_encrypted,
            services_table.c.current_version,
            services_table.c.active_versions,
            services_table.c.logical_replica_semantics,
        ).where(services_table.c.name == service_name)
        if require_version:
            query = query.where(sqlalchemy.exists().where(
                version_specs_table.c.service_name == services_table.c.name))
        row = session.execute(query).fetchone()
    if row is None:
        return None
    mapping = row._mapping  # pylint: disable=protected-access
    return {
        'name': mapping['name'],
        'controller_job_id': mapping['controller_job_id'],
        'controller_port': mapping['controller_port'],
        'load_balancer_port': mapping['load_balancer_port'],
        'status': ServiceStatus[mapping['status']],
        'pool': bool(mapping['pool']),
        'controller_pid': mapping['controller_pid'],
        'controller_ip': mapping['controller_ip'],
        'hash': mapping['hash'],
        'lifecycle_epoch': mapping['lifecycle_epoch'],
        'resource_scope': mapping['resource_scope'],
        'workspace': mapping['workspace'],
        'uptime': mapping['uptime'],
        'policy': mapping['policy'],
        'requested_resources_str': mapping['requested_resources_str'],
        'load_balancing_policy': mapping['load_balancing_policy'],
        'tls_encrypted': bool(mapping['tls_encrypted']),
        # This slim query deliberately avoids the latest-version join. The
        # elected version is the best persisted version available without
        # deserializing latest-version metadata; summary enrichment replaces it.
        'version': mapping['current_version'],
        'elected_version': mapping['current_version'],
        'active_versions': (json.loads(mapping['active_versions'])
                            if mapping['active_versions'] else []),
        'logical_replica_semantics': bool(mapping['logical_replica_semantics']),
        'replica_unit': ('logical_slot' if mapping['logical_replica_semantics']
                         else 'physical_backend'),
    }


def _controller_owner_record(mapping: Any) -> dict[str, Any]:
    """Build the common controller-owner identity from one SQL row."""
    return {
        'hash': mapping['hash'],
        'status': ServiceStatus[mapping['status']],
        'controller_pid': mapping['controller_pid'],
        'controller_ip': mapping['controller_ip'],
        'controller_port': mapping['controller_port'],
        'lifecycle_epoch': mapping['lifecycle_epoch'],
        'pool': bool(mapping['pool']),
        'resource_scope': mapping['resource_scope'],
    }


def get_service_controller_owner(
        service_name: str,
        require_version: bool = False,
        include_lb_state: bool = False,
        include_route_owner_state: bool = False) -> dict[str, Any] | None:
    """Get only the fields needed to route to a service controller.

    Unlike :func:`get_service_from_name`, this hot-path lookup does not join
    ``version_specs``, deserialize the latest spec, or issue a second query.
    The service hash distinguishes a same-name successor from the row read
    before a proxied request; status lets the proxy reject terminal rows.
    ``require_version`` preserves callers whose old joined read treated an
    orphan/versionless service row as missing, using an indexed existence
    check without loading version metadata. ``include_lb_state`` adds the
    cutover fields only for HA lifecycle callers. The promotion path may also
    request ``include_route_owner_state`` for the exact durable-route owner;
    keeping that opt-in separate preserves the original narrow HA contract for
    cleanup and ordinary lifecycle reads.
    """
    if include_route_owner_state and not include_lb_state:
        raise ValueError('include_route_owner_state requires include_lb_state')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        columns = [
            services_table.c.hash,
            services_table.c.status,
            services_table.c.controller_pid,
            services_table.c.controller_ip,
            services_table.c.controller_port,
            services_table.c.lifecycle_epoch,
            services_table.c.pool,
            services_table.c.resource_scope,
        ]
        if include_lb_state:
            columns.extend([
                services_table.c.lb_ha_enabled,
                services_table.c.lb_active_slot,
                services_table.c.lb_cutover_generation,
                services_table.c.lb_pending_slot,
                services_table.c.lb_cutover_phase,
                services_table.c.lb_drain_started_at,
            ])
        if include_route_owner_state:
            columns.extend([
                services_table.c.current_version,
                services_table.c.controller_incarnation,
                services_table.c.controller_owner_epoch,
                services_table.c.route_source_mode,
                services_table.c.route_source_epoch,
                services_table.c.route_projection_capable,
                services_table.c.route_projection_controller_incarnation,
                services_table.c.route_projection_protocol_version,
            ])
        query = sqlalchemy.select(*columns).where(
            services_table.c.name == service_name)
        if require_version:
            query = query.where(sqlalchemy.exists().where(
                version_specs_table.c.service_name == services_table.c.name))
        row = session.execute(query).fetchone()
    if row is None:
        return None
    mapping = row._mapping  # pylint: disable=protected-access
    record = _controller_owner_record(mapping)
    if include_lb_state:
        record.update({
            'lb_ha_enabled': bool(mapping['lb_ha_enabled']),
            'lb_active_slot': mapping['lb_active_slot'],
            'lb_cutover_generation': mapping['lb_cutover_generation'],
            'lb_pending_slot': mapping['lb_pending_slot'],
            'lb_cutover_phase': mapping['lb_cutover_phase'],
            'lb_drain_started_at': mapping['lb_drain_started_at'],
        })
    if include_route_owner_state:
        record.update({
            'current_version': mapping['current_version'],
            'controller_incarnation': mapping['controller_incarnation'],
            'controller_owner_epoch': mapping['controller_owner_epoch'],
            'route_source_mode': mapping['route_source_mode'],
            'route_source_epoch': mapping['route_source_epoch'],
            'route_projection_capable': bool(mapping['route_projection_capable']
                                            ),
            'route_projection_controller_incarnation':
                mapping['route_projection_controller_incarnation'],
            'route_projection_protocol_version':
                mapping['route_projection_protocol_version'],
        })
    return record


def get_service_replica_launch_authorization(
        service_name: str,
        *,
        binding_excluded_replica_id: int | None = None
) -> dict[str, Any] | None:
    """Atomically read a controller owner and its launch-authorized version.

    A newly committed version is normally the only generation authorized to
    launch.  If that generation is durably quarantined, controller recovery
    instead elects the newest applied, committed, non-quarantined version. The
    owner identity and all version candidates are aggregated by one SQL
    statement so a launch cannot pair one ownership snapshot with another
    transaction's quarantine decision.
    """
    engine = _db_manager.get_engine()
    binding_mode_supported = False
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        revision = migration_utils.get_current_alembic_revision(
            engine, migration_utils.SERVE_DB_NAME)
        binding_mode_supported = (revision is not None and int(revision) >= 42)
    (latest_applicable, latest_quarantined,
     latest_applied_applicable) = _quarantine_aware_version_aggregates()
    launch_authorized_version = _quarantine_aware_version_sql_expression(
        latest_applicable, latest_quarantined,
        latest_applied_applicable).label('launch_authorized_version')
    config_protocol_active = sqlalchemy.func.max(
        sqlalchemy.case((sqlalchemy.or_(
            version_specs_table.c.controller_config.isnot(None),
            version_specs_table.c.controller_config_digest.isnot(None),
            version_specs_table.c.controller_config_snapshot_id.isnot(None),
        ), 1),
                        else_=0)).label('config_protocol_active')
    owner_columns = (
        services_table.c.hash,
        services_table.c.status,
        services_table.c.controller_pid,
        services_table.c.controller_ip,
        services_table.c.controller_port,
        services_table.c.lifecycle_epoch,
        services_table.c.pool,
        services_table.c.resource_scope,
        *((services_table.c.ordinary_launch_binding_mode,)
          if binding_mode_supported else ()),
    )
    binding_excluded_columns: tuple[Any, ...] = ()
    if binding_excluded_replica_id is not None:
        excluded_replica_state = sqlalchemy.select(
            replicas_table.c.replica_state).where(
                replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id == binding_excluded_replica_id
            ).scalar_subquery().label('_binding_excluded_replica_state')
        excluded_replica_status = sqlalchemy.select(
            replicas_table.c.status).where(
                replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id == binding_excluded_replica_id
            ).scalar_subquery().label('_binding_excluded_replica_status')
        excluded_replica_state_version = sqlalchemy.select(
            replicas_table.c.replica_state_version).where(
                replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id == binding_excluded_replica_id
            ).scalar_subquery().label('_binding_excluded_replica_state_version')
        excluded_replica_version = sqlalchemy.select(
            replicas_table.c.version).where(
                replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id == binding_excluded_replica_id
            ).scalar_subquery().label('_binding_excluded_replica_version')
        binding_excluded_columns = (excluded_replica_state,
                                    excluded_replica_status,
                                    excluded_replica_state_version,
                                    excluded_replica_version)
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                *owner_columns, launch_authorized_version,
                config_protocol_active, *binding_excluded_columns).select_from(
                    services_table.outerjoin(
                        version_specs_table, version_specs_table.c.service_name
                        == services_table.c.name)).where(
                            services_table.c.name == service_name).group_by(
                                *owner_columns)).fetchone()
    if row is None:
        return None
    mapping = row._mapping  # pylint: disable=protected-access
    record = _controller_owner_record(mapping)
    # Keep the raw integer alongside the public boolean projection.  The
    # maintenance hold may exempt only an exactly persisted pool discriminator;
    # truthiness would turn corrupt values such as 2 into launch authority.
    record['pool_discriminator'] = mapping['pool']
    record['launch_authorized_version'] = mapping['launch_authorized_version']
    record['launch_version_required'] = bool(mapping['config_protocol_active'])
    if binding_mode_supported:
        record['ordinary_launch_binding_mode'] = mapping[
            'ordinary_launch_binding_mode']
    if binding_excluded_columns:
        record['binding_excluded_replica_state'] = mapping[
            '_binding_excluded_replica_state']
        record['binding_excluded_replica_status'] = mapping[
            '_binding_excluded_replica_status']
        record['binding_excluded_replica_state_version'] = mapping[
            '_binding_excluded_replica_state_version']
        record['binding_excluded_replica_version'] = mapping[
            '_binding_excluded_replica_version']
    return record


def normalize_binding_excluded_launch_context(
        launch_context: object) -> dict[str, Any] | None:
    """Return the closed excluded-profile discriminator, if one is claimed."""
    if not isinstance(launch_context, dict):
        return None
    profile_key = constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY
    replica_id_key = (constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY)
    record_id_key = (
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY)
    request_id_key = (constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY)
    generation_key = (constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_GENERATION_KEY)
    claimed_keys = {
        key for key in launch_context if isinstance(key, str) and
        key.startswith(constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PREFIX)
    }
    if claimed_keys:
        profile = launch_context.get(profile_key)
        if profile == constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE:
            expected_keys = {profile_key, replica_id_key, record_id_key}
        elif profile == (
                constants.
                ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE):
            expected_keys = {
                profile_key, replica_id_key, request_id_key, generation_key
            }
        else:
            raise ValueError('Unknown ordinary-launch exclusion profile.')
        if claimed_keys != expected_keys:
            raise ValueError(
                'Ordinary-launch exclusion discriminator is incomplete.')
        normalized = {key: launch_context[key] for key in expected_keys}
    elif constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY in launch_context:
        normalized = {
            profile_key:
                constants.
                ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE,
            replica_id_key: launch_context.get(
                constants.SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY),
            request_id_key: launch_context.get(
                constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY),
            generation_key: launch_context.get(
                constants.SYSTEM_OOM_RECOVERY_LAUNCH_GENERATION_KEY),
        }
    else:
        return None

    replica_id = normalized[replica_id_key]
    if type(replica_id) is not int or replica_id < 1:
        raise ValueError('Excluded-profile replica ID must be positive.')
    if normalized[profile_key] == (
            constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE):
        record_id = normalized[record_id_key]
        if not isinstance(record_id, str):
            raise ValueError('Excluded-profile replica record ID is invalid.')
        try:
            parsed_record_id = uuid.UUID(record_id)
        except ValueError as error:
            raise ValueError(
                'Excluded-profile replica record ID is invalid.') from error
        if str(parsed_record_id) != record_id:
            raise ValueError(
                'Excluded-profile replica record ID must be canonical.')
    else:
        request_id = normalized[request_id_key]
        generation = normalized[generation_key]
        if not isinstance(request_id, str) or not request_id:
            raise ValueError('Excluded system-recovery request ID is invalid.')
        if type(generation) is not int or generation < 1:
            raise ValueError(
                'Excluded system-recovery generation must be positive.')
    return normalized


def _binding_excluded_replica_if_matches(
    owner: Mapping[str, Any],
    normalized: Mapping[str, Any],
    service_version: int | None,
) -> 'replica_managers.ReplicaInfo | None':
    """Decode the exact excluded replica from one owner/row snapshot."""
    state = owner.get('binding_excluded_replica_state')
    status = owner.get('binding_excluded_replica_status')
    state_version = owner.get('binding_excluded_replica_state_version')
    row_version = owner.get('binding_excluded_replica_version')
    if (not isinstance(state, dict) or
            state_version != _REPLICA_STATE_VERSION or
            type(service_version) is not int or service_version < 1 or
            row_version != service_version or
            status not in (ReplicaStatus.PENDING.value,
                           ReplicaStatus.PROVISIONING.value)):
        return None
    try:
        info = _replica_from_state(state_version, state)
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
        return None
    replica_id = normalized[
        constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY]
    if (info.replica_id != replica_id or info.version != service_version or
            info.status.value != status):
        return None
    profile = normalized[constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY]
    if profile == constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE:
        if info.replica_record_id != normalized[
                constants.
                ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY]:
            return None
        persisted_special = bool(
            info.reserved_fill is True or info.is_zero_cost is True or
            info.unknown_capacity_replacement is True or
            type(info.cost_rebalance_for_replica_id) is int)
        # A failed system-recovery admission may irreversibly demote its exact
        # row before the already-running launch worker falls back to the
        # ordinary request contract.  Permit only that closed state.  An
        # active candidate, capable recovery, bound request, captured job, or
        # quarantined row must retain the system-recovery contract and cannot
        # be downgraded by a persistent-special claim.
        demoted_recovery_retry = bool(
            info.system_recovery_launch_intent is not None and
            info.system_recovery_disposition.value == 'ORDINARY' and
            info.system_recovery is None and
            info.system_recovery_quarantine is None and
            info.launch_request_id is None and info.service_job_id is None)
        has_system_recovery_lifecycle = bool(
            info.system_recovery_launch_intent is not None or
            info.system_recovery_disposition.value != 'ORDINARY' or
            info.system_recovery is not None or
            info.system_recovery_quarantine is not None or
            info.launch_request_id is not None or
            info.service_job_id is not None)
        if has_system_recovery_lifecycle:
            return info if demoted_recovery_retry else None
        return info if persisted_special else None
    if profile != (
            constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_SYSTEM_RECOVERY_PROFILE):
        return None
    intent = info.system_recovery_launch_intent
    matches = bool(
        intent is not None and intent.replica_id == replica_id and
        intent.launch_generation
        == normalized[constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_GENERATION_KEY]
        and info.launch_request_id
        == normalized[constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REQUEST_ID_KEY]
        and info.system_recovery_disposition.value == 'CANDIDATE' and
        info.system_recovery_quarantine is None)
    return info if matches else None


def _binding_excluded_replica_matches(
    owner: Mapping[str, Any],
    normalized: Mapping[str, Any],
    service_version: int | None,
) -> bool:
    """Whether one excluded request matches the exact durable replica row."""
    return (_binding_excluded_replica_if_matches(owner, normalized,
                                                 service_version) is not None)


@dataclasses.dataclass(frozen=True)
class ServiceReplicaLaunchFenceSnapshot:
    """One authorized service owner and its exact excluded replica row."""

    durable_replica_info: 'replica_managers.ReplicaInfo | None'


def service_replica_launch_fence_snapshot(
    launch_context: dict[str, Any],
    binding_excluded_launch_context: dict[str, Any] | None = None,
) -> ServiceReplicaLaunchFenceSnapshot | None:
    """Return exact durable launch authority from one database snapshot.

    The service owner, launch version and optional excluded ReplicaInfo row are
    selected by one SQL statement.  Returning the decoded row lets terminal
    reserved-fill authorization compare queued authority to the same durable
    snapshot that authorized the service fence.
    """
    service_name = launch_context.get(
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
    service_hash = launch_context.get(
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
    service_version = launch_context.get(
        constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY)
    controller_pid = launch_context.get(
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY)
    controller_ip = launch_context.get(
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY)
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(service_hash, str) or not service_hash or
            not (service_version is None or
                 type(service_version) is int and service_version > 0) or
            not (controller_pid is None or isinstance(controller_pid, int)) or
            not (controller_ip is None or isinstance(controller_ip, str))):
        return None

    try:
        normalized_exclusion = normalize_binding_excluded_launch_context(
            launch_context if binding_excluded_launch_context is
            None else binding_excluded_launch_context)
    except ValueError:
        return None
    excluded_replica_id = (
        None if normalized_exclusion is None else normalized_exclusion[
            constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY])
    owner = get_service_replica_launch_authorization(
        service_name,
        **({} if excluded_replica_id is None else {
            'binding_excluded_replica_id': excluded_replica_id
        }))
    if owner is None:
        return None
    durable_replica_info = None
    if normalized_exclusion is not None:
        durable_replica_info = _binding_excluded_replica_if_matches(
            owner, normalized_exclusion, service_version)
    binding_mode_allows = bool(
        owner.get('ordinary_launch_binding_mode') != 'bound' or
        (normalized_exclusion is not None and durable_replica_info is not None))
    authorized = bool(
        binding_mode_allows and
        (not maintenance.is_controller_hold_active() or
         (type(owner.get('pool_discriminator')) is int and
          owner.get('pool_discriminator') == 1)) and
        owner.get('hash') == service_hash and
        (owner.get('controller_pid'), owner.get('controller_ip'))
        == (controller_pid, controller_ip) and owner.get('status')
        not in ServiceStatus.replica_launch_blocking_statuses() and
        ((service_version is None and
          not owner.get('launch_version_required', False)) or
         owner.get('launch_authorized_version') == service_version))
    if not authorized:
        return None
    return ServiceReplicaLaunchFenceSnapshot(durable_replica_info)


def service_replica_launch_fence_holds(
    launch_context: dict[str, Any],
    binding_excluded_launch_context: dict[str, Any] | None = None,
) -> bool:
    """Check one persisted replica request against its current DB authority.

    The check deliberately performs a fresh, single-snapshot authorization
    read on every call.  Callers use it at both request admission and the
    terminal provider boundary so a controller/API crash cannot let an already
    admitted request provision after a newer config generation is elected.
    Database failures propagate: a caller that cannot prove current authority
    must fail closed rather than treating the request as launchable.
    """
    return service_replica_launch_fence_snapshot(
        launch_context, binding_excluded_launch_context) is not None


def reserved_fill_reclaim_launch_authority_holds(
    scope: reserved_fill_reclaim_attestation.ReclaimLaunchScope | None,
    authorization: (reserved_fill_reclaim_attestation.ReclaimLaunchAuthorization
                    | None),
    launch_context: dict[str, Any],
    launch_snapshot: ServiceReplicaLaunchFenceSnapshot | None,
) -> bool:
    """Revalidate durable row, immutable gate and policy ticket before effect."""
    if (launch_snapshot is None or
            launch_snapshot.durable_replica_info is None or
            launch_snapshot.durable_replica_info.reserved_fill is not True):
        return False
    try:
        fence = reserved_capacity.parse_protocol_v2_launch_fence(launch_context)
        if fence is not None:
            reserved_capacity.validate_protocol_v2_launch_fence_against_replica(
                fence, launch_snapshot.durable_replica_info)
    except (AttributeError, KeyError, TypeError, ValueError):
        return False
    try:
        engine = _db_manager.get_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            return False
        with engine.begin() as connection:
            # Lock the one global selector before any service or claim row.
            # The provider guard holds the fleet advisory lock across this
            # read and the effect; this row lock gives the read one exact SQL
            # snapshot and preserves the global->service lock order.
            authority_table = (
                pool_capacity_observation_schema.protocol_state_sequence_table)
            authority = connection.execute(
                sqlalchemy.select(authority_table).where(
                    authority_table.c.id == 1).with_for_update(
                        read=True)).mappings().one_or_none()
            if authority is None:
                return False
            gate_state = authority['reconciliation_gate_state']
            if gate_state == pool_capacity_observation_schema.LEGACY_ACTIVE:
                return (authorization is None and scope is None and
                        (fence is None or not fence.policy_bound))
            if (gate_state != pool_capacity_observation_schema.SEQUENCED_ACTIVE
                    or authority['protocol_version']
                    != RESERVED_FILL_PROTOCOL_V2 or fence is None or
                    not fence.policy_bound or scope is None or
                    authorization is None):
                # A queued pre-policy request cannot cross the one-way gate.
                return False
            gate_generation = authority['reconciliation_gate_generation']
            if type(gate_generation) is not int or gate_generation <= 0:
                return False
            identity = reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
                fleet_bundle_sha256=authority['reclaim_fleet_bundle_sha256'],
                policy_revision=authority['reclaim_policy_revision'],
                provider_inventory_sha256=authority[
                    'reclaim_provider_inventory_sha256'])
            protocol = connection.execute(
                sqlalchemy.select(
                    reserved_fill_protocol_state_table.c.claim_generation).
                where(reserved_fill_protocol_state_table.c.id ==
                      1).with_for_update(read=True)).mappings().one_or_none()
            if (protocol is None or
                    type(protocol['claim_generation']) is not int or
                    protocol['claim_generation'] < 0):
                return False

            service_name = launch_context[
                constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY]
            service_hash = launch_context[
                constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY]
            service_row = connection.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.current_version,
                    services_table.c.resource_scope).where(
                        services_table.c.name == service_name).with_for_update(
                            read=True)).mappings().one_or_none()
            if (service_row is None or service_row['hash'] != service_hash or
                    service_row['resource_scope'] != service_hash or
                    service_row['current_version'] != fence.service_version):
                return False
            version_row = connection.execute(
                sqlalchemy.select(
                    version_specs_table.c.worker_placement_projections).where(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.version == fence.service_version,
                        version_specs_table.c.yaml_content.isnot(None),
                        version_specs_table.c.quarantined_at.is_(None),
                        version_specs_table.c.retired_at.is_(None),
                    ).with_for_update(read=True)).mappings().one_or_none()
            claim_set = connection.execute(
                sqlalchemy.select(reserved_fill_service_claim_sets_table).where(
                    reserved_fill_service_claim_sets_table.c.service_name ==
                    service_name).with_for_update(
                        read=True)).mappings().one_or_none()
            edge_rows = connection.execute(
                sqlalchemy.select(reserved_fill_pool_claims_table).where(
                    reserved_fill_pool_claims_table.c.service_name ==
                    service_name).with_for_update(read=True)).mappings().all()
            matching_edge = next((edge for edge in edge_rows
                                  if edge['pool_key'] == fence.pool_key), None)
            current_service_generation = (None if claim_set is None else
                                          claim_set['generation'])
            if (version_row is None or claim_set is None or
                    claim_set['claim_set_state']
                    != RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2 or
                    claim_set['service_version'] != fence.service_version or
                    type(current_service_generation) is not int or
                    current_service_generation < fence.service_generation or
                    current_service_generation > protocol['claim_generation'] or
                    len(edge_rows) != claim_set['edge_count'] or
                    matching_edge is None or
                    any(edge['service_generation'] != current_service_generation
                        for edge in edge_rows)):
                return False
            if (current_service_generation > fence.service_generation and
                    not zero_cost_actuation.
                    committed_intent_matches_replica_in_connection(
                        connection,
                        service_name=service_name,
                        service_hash=service_hash,
                        replica_info=launch_snapshot.durable_replica_info)):
                # A committed intent is the immutable handoff from its broker
                # generation.  A newer, still-compatible claim set may retain
                # that launch, but an uncommitted or ambiguous legacy row must
                # continue to fail closed.
                return False
            _, projected_admission = (
                reserved_capacity.require_reclaim_worker_projection(
                    fence, version_row['worker_placement_projections']))
            digest_map = _decode_reserved_fill_projection_digest_map(
                matching_edge['worker_projection_sha256_by_accelerator'],
                matching_edge['accelerator_names'])
            if (matching_edge['access_context'] != fence.kubernetes_context or
                    matching_edge['physical_cluster_uid']
                    != fence.physical_cluster_uid or
                    matching_edge['gpus_per_replica'] != fence.accelerator_count
                    or digest_map.get(fence.accelerator.casefold())
                    != projected_admission.worker_projection_sha256):
                return False
            expected_scope = (
                reserved_fill_reclaim_attestation.ReclaimLaunchScope(
                    service_name=service_name,
                    service_version=fence.service_version,
                    pool_key=fence.pool_key,
                    service_generation=fence.service_generation,
                    physical_cluster_uid=fence.physical_cluster_uid,
                    kubernetes_context=fence.kubernetes_context,
                    accelerator=fence.accelerator,
                    accelerator_count=fence.accelerator_count,
                    projected_admission=projected_admission))
            if scope != expected_scope:
                return False
            (reserved_fill_reclaim_attestation.
             require_exact_launch_authorization)(
                 authorization,
                 expected_identity=identity,
                 expected_gate_generation=gate_generation,
                 expected_scope=scope)
            if not (reserved_fill_reclaim_proofs.
                    provider_proof_reference_holds_in_connection)(
                        connection,
                        authorization.provider_proof_reference,
                        expected_physical_cluster_uid=(
                            scope.physical_cluster_uid)):
                return False
    except (reserved_fill_reclaim_attestation.ReclaimAttestationError, KeyError,
            TypeError, ValueError):
        return False
    return True


_require_postgresql_lb_cutover = (
    lb_cutover_state._require_postgresql_lb_cutover  # pylint: disable=protected-access
)
get_lb_cutover_state = lb_cutover_state.get_lb_cutover_state
_lb_cutover_owner_predicates = (
    lb_cutover_state._lb_cutover_owner_predicates  # pylint: disable=protected-access
)
begin_lb_ha_migration = lb_cutover_state.begin_lb_ha_migration
finish_lb_ha_migration = lb_cutover_state.finish_lb_ha_migration
begin_lb_ha_rollback = lb_cutover_state.begin_lb_ha_rollback
finish_lb_ha_rollback = lb_cutover_state.finish_lb_ha_rollback
begin_lb_cutover = lb_cutover_state.begin_lb_cutover
record_lb_active_demand_snapshot = (
    lb_cutover_state.record_lb_active_demand_snapshot)
get_lb_last_demand_snapshot = lb_cutover_state.get_lb_last_demand_snapshot
commit_lb_cutover = lb_cutover_state.commit_lb_cutover
finish_lb_cutover_drain = lb_cutover_state.finish_lb_cutover_drain
get_lb_demand_handoff = lb_cutover_state.get_lb_demand_handoff
mark_lb_demand_handoff_complete = (
    lb_cutover_state.mark_lb_demand_handoff_complete)
clear_lb_demand_handoff = lb_cutover_state.clear_lb_demand_handoff
lb_cutover_kubernetes_guard = _lb_guard
abort_lb_cutover_preparation = (lb_cutover_state.abort_lb_cutover_preparation)


def get_service_hash(service_name: str) -> str | None:
    """Get the hash of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(services_table.c.hash).where(
                services_table.c.name == service_name)).fetchone()
    return result[0] if result else None


def get_service_mode_and_hash(
        service_name: str) -> tuple[bool, str | None] | None:
    """Read the raw mode/hash identity without joining version metadata."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.pool, services_table.c.hash).where(
                    services_table.c.name == service_name)).fetchone()
    if row is None:
        return None
    return bool(row[0]), row[1]


def get_service_mode_and_hashes(
        service_names: list[str]) -> dict[str, tuple[bool, str | None]]:
    """Batch raw mode/hash identity reads for existing service rows."""
    if not service_names:
        return {}
    names = sorted(set(service_names))
    engine = _db_manager.get_engine()
    rows = []
    with orm.Session(engine) as session:
        for start in range(0, len(names), _TERMINAL_IDENTITY_QUERY_BATCH_SIZE):
            name_batch = names[start:start +
                               _TERMINAL_IDENTITY_QUERY_BATCH_SIZE]
            rows.extend(
                session.execute(
                    sqlalchemy.select(
                        services_table.c.name, services_table.c.pool,
                        services_table.c.hash).where(
                            services_table.c.name.in_(name_batch))).fetchall())
    return {row.name: (bool(row.pool), row.hash) for row in rows}


def add_ephemeral_storage_cleanup_intent(service_name: str, resource_scope: str,
                                         storage_generation: str,
                                         yaml_content: str, pool: bool,
                                         lifecycle_epoch: int,
                                         provisional: bool) -> bool:
    """Persist exact scoped cleanup inventory before external storage writes.

    The lifecycle fence is locked before the optional service row.  A missing
    service row is valid for fresh ``up``; an existing row must belong to this
    exact resource scope.  Existing generations are never re-owned by a later
    operation (notably workers-only updates which intentionally reuse storage).
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, True)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   lifecycle_epoch):
            session.rollback()
            return False
        service_row = session.execute(
            sqlalchemy.select(services_table.c.resource_scope).where(
                services_table.c.name ==
                service_name).with_for_update()).fetchone()
        if (service_row is not None and
                service_row.resource_scope != resource_scope):
            session.rollback()
            return False
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')
        stmt = insert_func(ephemeral_storage_cleanup_intents_table).values(
            service_name=service_name,
            resource_scope=resource_scope,
            storage_generation=storage_generation,
            yaml_content=yaml_content,
            pool=int(pool),
            lifecycle_epoch=lifecycle_epoch,
            provisional=int(provisional),
            created_at=time.time())
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                'service_name', 'resource_scope', 'storage_generation'
            ],
            # A second call after sync enriches the pre-mutation manifest with
            # resolved store handles. Never transfer lifecycle/provisional
            # ownership on conflict: workers-only updates reuse a committed
            # generation created by an earlier operation.
            set_={'yaml_content': stmt.excluded.yaml_content})
        session.execute(stmt)
        session.commit()
    return True


def get_ephemeral_storage_cleanup_intents(
        service_name: str,
        resource_scope: str | None = None,
        lifecycle_epoch: int | None = None,
        provisional: bool | None = None) -> list[dict[str, Any]]:
    """Return durable scoped storage cleanup manifests."""
    predicates = [
        ephemeral_storage_cleanup_intents_table.c.service_name == service_name
    ]
    if resource_scope is not None:
        predicates.append(ephemeral_storage_cleanup_intents_table.c.
                          resource_scope == resource_scope)
    if lifecycle_epoch is not None:
        predicates.append(ephemeral_storage_cleanup_intents_table.c.
                          lifecycle_epoch == lifecycle_epoch)
    if provisional is not None:
        predicates.append(ephemeral_storage_cleanup_intents_table.c.provisional
                          == int(provisional))
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(ephemeral_storage_cleanup_intents_table).where(
                *predicates).order_by(ephemeral_storage_cleanup_intents_table.c.
                                      created_at)).mappings().all()
    return [dict(row) for row in rows]


def remove_provisional_ephemeral_storage_cleanup_intents(
        service_name: str, resource_scope: str, intent_lifecycle_epoch: int,
        current_lifecycle_epoch: int) -> bool:
    """Forget successfully cleaned provisional intents under the owner fence."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, True)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   current_lifecycle_epoch):
            session.rollback()
            return False
        session.execute(
            sqlalchemy.delete(ephemeral_storage_cleanup_intents_table).where(
                ephemeral_storage_cleanup_intents_table.c.service_name ==
                service_name,
                ephemeral_storage_cleanup_intents_table.c.resource_scope ==
                resource_scope,
                ephemeral_storage_cleanup_intents_table.c.lifecycle_epoch ==
                intent_lifecycle_epoch,
                ephemeral_storage_cleanup_intents_table.c.provisional == 1))
        session.commit()
    return True


def get_orphaned_service_child_names(
        service_names: list[str] | None = None) -> list[str]:
    """Get resource-bearing child names with no authoritative service row.

    HA scripts and reserved-capacity claims carry no external resource or
    service/pool mode. They are intentionally excluded: fenced registration
    clears those rows atomically, while exposing them to mode-scoped purge
    would create an unresolvable ambiguous-mode orphan.
    """
    child_name_queries = [
        sqlalchemy.select(table.c.service_name.label('service_name'))
        for table in (replicas_table, version_specs_table,
                      ephemeral_storage_cleanup_intents_table)
    ]
    child_names = sqlalchemy.union(*child_name_queries).subquery()
    query = sqlalchemy.select(
        child_names.c.service_name).where(~sqlalchemy.exists().where(
            services_table.c.name == child_names.c.service_name))
    if service_names is not None:
        patterns = [
            db_utils.glob_to_like_pattern(name) for name in service_names
        ]
        query = query.where(
            sqlalchemy.or_(*[
                child_names.c.service_name.like(
                    pattern, escape=db_utils.LIKE_ESCAPE_CHAR)
                for pattern in patterns
            ]))
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(query).scalars().all()
    return sorted(set(rows))


def get_orphaned_service_child_mode(service_name: str) -> bool | None:
    """Infer a child-only name's pool bit, failing closed on ambiguity."""
    modes: set[bool] = set()
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        intent_modes = session.execute(
            sqlalchemy.select(
                ephemeral_storage_cleanup_intents_table.c.pool).where(
                    ephemeral_storage_cleanup_intents_table.c.service_name ==
                    service_name)).scalars().all()
        modes.update(bool(mode) for mode in intent_modes)
        replica_rows = session.execute(
            sqlalchemy.select(replicas_table.c.replica_state).where(
                replicas_table.c.service_name == service_name)).scalars().all()
        version_rows = session.execute(
            sqlalchemy.select(version_specs_table.c.spec,
                              version_specs_table.c.yaml_content,
                              version_specs_table.c.retired_yaml_content).where(
                                  version_specs_table.c.service_name ==
                                  service_name)).fetchall()
    for replica_state in replica_rows:
        try:
            modes.add(replica_state['replica_port'] == '-')
        except Exception:  # pylint: disable=broad-except
            return None
    for spec_bytes, yaml_content, retired_yaml_content in version_rows:
        try:
            spec = typing.cast('service_spec.SkyServiceSpec | None',
                               pickle.loads(spec_bytes))
            if spec is not None:
                modes.add(bool(spec.pool))
                continue
        except Exception:  # pylint: disable=broad-except
            pass
        cleanup_yaml_content = (yaml_content if yaml_content is not None else
                                retired_yaml_content)
        try:
            config = yaml_utils.safe_load(cleanup_yaml_content)
        except Exception:  # pylint: disable=broad-except
            return None
        if not isinstance(config, dict) or not isinstance(
                config.get('service'), dict):
            return None
        modes.add(bool(config['service'].get('pool', False)))
    if len(modes) != 1:
        return None
    return modes.pop()


def remove_orphaned_service_children(service_name: str,
                                     lifecycle_epoch: int) -> bool:
    """Delete child-only metadata after its external resources are confirmed."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, True)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   lifecycle_epoch):
            session.rollback()
            return False
        service_row = session.execute(
            sqlalchemy.select(services_table.c.name).where(
                services_table.c.name ==
                service_name).with_for_update()).fetchone()
        if service_row is not None:
            session.rollback()
            return False
        for table in (replicas_table, version_specs_table,
                      serve_ha_recovery_script_table,
                      reserved_fill_claims_table,
                      ephemeral_storage_cleanup_intents_table):
            session.execute(
                sqlalchemy.delete(table).where(
                    table.c.service_name == service_name))
        session.commit()
    return True


def get_service_versions(service_name: str) -> list[int]:
    """Gets all versions of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(version_specs_table.c.version).where(
                version_specs_table.c.service_name ==
                service_name).distinct()).fetchall()
    return [row[0] for row in rows]


def get_glob_service_names(service_names: list[str] | None = None,
                           pool: bool | None = None) -> list[str]:
    """Get service names matching the glob patterns.

    Args:
        service_names: A list of glob patterns. If None, return all service
            names.
        pool: When set, only return services whose mode matches the flag.

    Returns:
        A list of non-duplicated service names.
    """
    engine = _db_manager.get_engine()

    def _with_pool_filter(query):
        if pool is None:
            return query
        return query.where(services_table.c.pool == int(pool))

    query = sqlalchemy.select(services_table.c.name)
    if service_names is not None:
        if not service_names:
            return []
        query = query.where(
            sqlalchemy.or_(*(services_table.c.name.like(
                db_utils.glob_to_like_pattern(service_name),
                escape=db_utils.LIKE_ESCAPE_CHAR)
                             for service_name in service_names)))
    with orm.Session(engine) as session:
        rows = session.execute(_with_pool_filter(query)).fetchall()
    return list({row[0] for row in rows})


def get_service_pool_from_db(service_name: str) -> bool | None:
    """Reads the raw `pool` flag for a service straight from the services row.

    Unlike `get_service_from_name`, this does NOT inner-join `version_specs`,
    so it still returns a value for a `services` row that has no version row
    (an orphan stranded by an interrupted first-run registration). Returns
    None if no row exists for `service_name`.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(services_table.c.pool).where(
                services_table.c.name == service_name)).fetchone()
    return bool(row[0]) if row is not None else None


# === Replica functions ===

_SQLITE_MAX_BIND_PARAMS = 999
_POSTGRESQL_REPLICA_UPSERT_CHUNK_SIZE = 300
_REPLICA_DELETE_CHUNK_SIZE = 500
_REPLICA_STATE_VERSION = 1
_REPLICA_ROW_COLUMNS = (
    'service_name',
    'replica_id',
    'replica_state_version',
    'status',
    'sky_down_status',
    'version',
    'cluster_name',
    'created_at',
    'is_spot',
    'paid_capacity_pool_key',
    'replica_state',
)
_ACTION_OWNED_REPLICA_COLUMNS = frozenset({
    'replica_incarnation',
    'desired_generation',
    'sky_cluster_record_uuid',
    'launch_action_id',
    'down_action_id',
    'launch_shadow_coverage_id',
    'down_shadow_coverage_id',
    'launch_shadow_sample_id',
    'down_shadow_sample_id',
    'resource_action_spec_identity_sha256',
    'ordinary_launch_association_id',
    'non_pool_launch_authorization',
})
_PAID_CAPACITY_UNRESOLVED_STATUSES = (
    ReplicaStatus.PENDING.value,
    ReplicaStatus.PROVISIONING.value,
)

_SYSTEM_RECOVERY_STORAGE_FIELDS_FALLBACK = (
    'system_recovery_launch_intent',
    'system_recovery_disposition',
    'launch_request_id',
    'service_job_id',
    'candidate_ready_observed_at',
    'ordinary_release_not_before',
    'system_recovery_revision',
    'system_recovery',
    'system_recovery_quarantine',
)


class ReplicaSystemRecoveryStateError(RuntimeError):
    """Base class for a rejected durable recovery-state mutation."""


class ReplicaSystemRecoveryRevisionConflict(ReplicaSystemRecoveryStateError):
    """A caller reduced an older recovery revision than the locked row."""

    def __init__(self, expected_revision: int, current_revision: int) -> None:
        super().__init__('Replica system-recovery revision changed: expected '
                         f'{expected_revision}, found {current_revision}.')
        self.expected_revision = expected_revision
        self.current_revision = current_revision


class ReplicaSystemRecoveryMutationRejected(ReplicaSystemRecoveryStateError):
    """The locked owner, identity, or absorbing state rejected a mutation."""


def system_recovery_persistence_available() -> bool:
    """Whether this controller can use central recovery-state persistence.

    Recovery admission is deliberately PostgreSQL-only.  A local/SQLite Serve
    controller must remain ordinary; it cannot participate in the endpoint's
    cross-process nonce bind.
    """
    try:
        engine = _db_manager.get_engine()
    except Exception:  # pylint: disable=broad-except
        return False
    return (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)


def _require_system_recovery_postgres() -> sqlalchemy.engine.Engine:
    engine = _db_manager.get_engine()
    if (engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise ReplicaSystemRecoveryMutationRejected(
            'Replica system recovery requires central PostgreSQL state.')
    return engine


def _system_recovery_storage_fields() -> tuple[str, ...]:
    fields = replica_info_lib.SYSTEM_RECOVERY_STORAGE_FIELDS
    if (not isinstance(fields, tuple) or
            fields != _SYSTEM_RECOVERY_STORAGE_FIELDS_FALLBACK):
        raise ReplicaSystemRecoveryMutationRejected(
            'Replica system-recovery storage fields do not match the '
            'accepted v13 contract.')
    return fields


def _copy_system_recovery_fields(source: 'replica_managers.ReplicaInfo',
                                 destination: 'replica_managers.ReplicaInfo',
                                 *,
                                 increment_revision: bool = False) -> None:
    """Copy a recovery bundle from an authoritative source object.

    Generic writers pass the locked database row as ``source`` and an
    untrusted, potentially stale whole-row object as ``destination``.  The
    destination revision therefore cannot participate in deciding which
    bundle wins: every recovery field must come from the locked row.
    Recovery transitions use the same primitive only after their expected
    revision has been checked under lock.
    """
    replica_info_lib.copy_system_recovery_fields(
        source, destination, increment_revision=increment_revision)


def _system_recovery_revision(
        replica_info: 'replica_managers.ReplicaInfo') -> int:
    revision = replica_info.system_recovery_revision
    if (isinstance(revision, bool) or not isinstance(revision, int) or
            revision < 0):
        raise ReplicaSystemRecoveryMutationRejected(
            'Replica has an invalid system-recovery revision.')
    return revision


def _system_recovery_snapshot(
        replica_info: 'replica_managers.ReplicaInfo') -> tuple[Any, ...]:
    _system_recovery_storage_fields()
    return (
        copy.deepcopy(replica_info.system_recovery_launch_intent),
        copy.deepcopy(replica_info.system_recovery_disposition),
        copy.deepcopy(replica_info.launch_request_id),
        copy.deepcopy(replica_info.service_job_id),
        copy.deepcopy(replica_info.candidate_ready_observed_at),
        copy.deepcopy(replica_info.ordinary_release_not_before),
        copy.deepcopy(replica_info.system_recovery_revision),
        copy.deepcopy(replica_info.system_recovery),
        copy.deepcopy(replica_info.system_recovery_quarantine),
    )


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, enum.Enum) else value


def _system_recovery_disposition(
        replica_info: 'replica_managers.ReplicaInfo') -> str:
    disposition = _enum_value(replica_info.system_recovery_disposition)
    if disposition not in ('ORDINARY', 'CANDIDATE', 'CAPABLE'):
        raise ReplicaSystemRecoveryMutationRejected(
            'Replica has an invalid system-recovery disposition.')
    return typing.cast(str, disposition)


def _has_system_recovery_teardown_intent(
        replica_info: 'replica_managers.ReplicaInfo') -> bool:
    status = replica_info.status_property
    return bool(replica_info.is_terminal or
                status.sky_down_status is not None or status.preempted or
                status.purged or status.is_scale_down)


def _nested_system_recovery_is_exhausted(
        replica_info: 'replica_managers.ReplicaInfo') -> bool:
    recovery = replica_info.system_recovery
    if recovery is None:
        return False
    return _enum_value(recovery.state) == 'EXHAUSTED'


def _validate_system_recovery_transition(
        current: 'replica_managers.ReplicaInfo',
        desired: 'replica_managers.ReplicaInfo') -> None:
    """Validate monotonic recovery fields against the locked latest row."""
    current_revision = _system_recovery_revision(current)
    if _system_recovery_revision(desired) != current_revision:
        raise ReplicaSystemRecoveryMutationRejected(
            'A recovery patch must carry the locked expected revision.')

    current_snapshot = _system_recovery_snapshot(current)
    desired_snapshot = _system_recovery_snapshot(desired)
    if current_snapshot == desired_snapshot:
        return

    if (_has_system_recovery_teardown_intent(current) or
            current.system_recovery_quarantine is not None or
            _nested_system_recovery_is_exhausted(current)):
        raise ReplicaSystemRecoveryMutationRejected(
            'Terminal teardown, exhaustion, and quarantine are absorbing.')

    current_intent = current.system_recovery_launch_intent
    desired_intent = desired.system_recovery_launch_intent
    if current_intent is not None and desired_intent != current_intent:
        raise ReplicaSystemRecoveryMutationRejected(
            'A persisted recovery launch intent is immutable.')
    if current_intent is None and desired_intent is None and (
            _system_recovery_disposition(desired) != 'ORDINARY'):
        raise ReplicaSystemRecoveryMutationRejected(
            'Candidate/capable state requires an exact launch intent.')

    current_request_id = current.launch_request_id
    desired_request_id = desired.launch_request_id
    if (current_request_id is not None and
            desired_request_id != current_request_id):
        raise ReplicaSystemRecoveryMutationRejected(
            'A bound launch request ID is immutable.')
    current_job_id = current.service_job_id
    desired_job_id = desired.service_job_id
    if current_job_id is not None and desired_job_id != current_job_id:
        raise ReplicaSystemRecoveryMutationRejected(
            'A bound service job ID is immutable.')

    current_quarantine = current.system_recovery_quarantine
    desired_quarantine = desired.system_recovery_quarantine
    if current_quarantine is not None and desired_quarantine != current_quarantine:
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery quarantine is absorbing.')
    # Entering quarantine is always a legal fail-closed transition.  It does
    # not authorize any simultaneous capability promotion.
    if desired_quarantine is not None:
        if (_system_recovery_disposition(desired)
                != _system_recovery_disposition(current)):
            raise ReplicaSystemRecoveryMutationRejected(
                'Quarantine cannot change recovery disposition.')
        return

    current_disposition = _system_recovery_disposition(current)
    desired_disposition = _system_recovery_disposition(desired)
    if current_disposition == 'ORDINARY':
        if current_intent is not None:
            raise ReplicaSystemRecoveryMutationRejected(
                'A demoted ordinary recovery intent is absorbing.')
        if desired_disposition not in ('ORDINARY', 'CANDIDATE'):
            raise ReplicaSystemRecoveryMutationRejected(
                'An ordinary replica cannot become capable directly.')
    elif current_disposition == 'CANDIDATE':
        if desired_disposition not in ('CANDIDATE', 'CAPABLE', 'ORDINARY'):
            raise ReplicaSystemRecoveryMutationRejected(
                'Invalid candidate recovery transition.')
    elif desired_disposition != 'CAPABLE':
        raise ReplicaSystemRecoveryMutationRejected(
            'A capable recovery disposition cannot be demoted or reset.')


def _lock_zero_cost_protocol_sequence_for_update(
    executor: orm.Session | sqlalchemy.engine.Connection,
) -> sqlalchemy.engine.RowMapping:
    """Lock and validate the global zero-cost event sequencer.

    This row is the first SQL mutex for every new zero-cost admission and
    every possible first-success materialization.  Observation publication and
    allocation already use the same protocol-first order, so replica writers
    must acquire it before lifecycle/service/replica rows.
    """
    table = pool_capacity_observation_schema.protocol_state_sequence_table
    row = executor.execute(
        sqlalchemy.select(table).where(
            table.c.id == 1).with_for_update()).mappings().one_or_none()
    if row is None:
        raise RuntimeError('Reserved-fill sequencer singleton is missing at '
                           'zero-cost replica mutation.')
    admission = row['zero_cost_admission_sequence']
    ordinary = row['ordinary_zero_cost_admission_sequence']
    materialization = row['zero_cost_materialization_sequence']
    if (type(admission) is not int or admission < 0 or
            type(ordinary) is not int or ordinary < 0 or ordinary > admission or
            type(materialization) is not int or materialization < 0):
        raise RuntimeError('Reserved-fill zero-cost event sequences are '
                           'malformed.')
    return row


def lock_zero_cost_protocol_for_bound_launch_projection(
        connection: sqlalchemy.engine.Connection) -> None:
    """Acquire the protocol-first mutex for a possible bound success.

    The request reducer cannot know whether its locked replica is zero-cost
    until after it takes lifecycle/service/replica locks.  It therefore calls
    this narrow helper unconditionally at transaction entry.  The later
    projector reuses the transaction-owned row lock only if it actually needs
    to stamp a materialization.
    """
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    _lock_zero_cost_protocol_sequence_for_update(connection)


def _replica_write_may_touch_zero_cost_sequence(
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    *,
    expected_replica_exists: bool,
) -> bool:
    """Whether a batch can admit or first-materialize a zero-cost row."""
    for _, info in replica_infos:
        if not expected_replica_exists and info.is_zero_cost is True:
            return True
        if (expected_replica_exists and info.status_property.sky_launch_status
                == common_utils.ProcessStatus.SUCCEEDED):
            # The locked current row owns immutable cost provenance.  A stale
            # incoming copy can be merged from is_zero_cost=False to True, so
            # any existing-row success must take protocol before reading that
            # row; otherwise the later materialization stamp could invert the
            # global protocol->service->replica order.
            return True
    return False


@dataclasses.dataclass(frozen=True)
class StagedReservedFillReplica:
    """Replica persistence staged on a caller-owned PostgreSQL transaction."""

    replica_id: int
    caller_info: 'replica_managers.ReplicaInfo'
    persisted_info: 'replica_managers.ReplicaInfo'
    already_committed: bool

    def publish_after_commit(self) -> None:
        """Publish database-assigned sequences after the outer commit."""
        _publish_committed_zero_cost_sequences(
            [(self.replica_id, self.caller_info)],
            [(self.replica_id, self.persisted_info)])


def add_replica_if_round_epoch(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    pool_key: str,
    expected_epoch: int,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    expected_protocol_version: int = RESERVED_FILL_PROTOCOL_V1,
    expected_service_generation: int | None = None,
    expected_physical_cluster_uid: str | None = None,
    expected_ordinary_zero_cost_admission_sequence: int | None = None,
    expected_lease_token: int | None = None,
    expected_actuation_mode: str | None = None,
    actuation_lease: 'zero_cost_actuation.IntentLease | None' = None,
) -> bool:
    """Commit one historical protocol-v1 fill replica."""
    if (expected_protocol_version != RESERVED_FILL_PROTOCOL_V1 or
            expected_actuation_mode is not None or actuation_lease is not None):
        raise ValueError('Standalone fill persistence is protocol-v1 only.')
    engine = _db_manager.get_engine()
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return _persist_protocol_v1_sqlite(
            engine,
            service_name,
            replica_id,
            replica_info,
            pool_key=pool_key,
            expected_epoch=expected_epoch,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('Unsupported reserved-fill database dialect.')
    if expected_lease_token is None:
        return False
    connection = engine.connect()
    operation_error: BaseException | None = None
    try:
        transaction = connection.begin()
        try:
            staged = _stage_postgres_replica_if_round_epoch(
                connection,
                service_name,
                replica_id,
                replica_info,
                pool_key=pool_key,
                expected_epoch=expected_epoch,
                expected_service_hash=expected_service_hash,
                expected_controller_owner=expected_controller_owner,
                expected_protocol_version=RESERVED_FILL_PROTOCOL_V1,
                expected_service_generation=expected_service_generation,
                expected_physical_cluster_uid=expected_physical_cluster_uid,
                expected_ordinary_zero_cost_admission_sequence=(
                    expected_ordinary_zero_cost_admission_sequence),
                expected_lease_token=expected_lease_token)
            if staged is None:
                transaction.rollback()
                return False
            transaction.commit()
        except BaseException as error:
            if transaction.is_active:
                try:
                    transaction.rollback()
                except BaseException as rollback_error:
                    if not isinstance(error, Exception):
                        raise error from rollback_error
                    if not isinstance(rollback_error, Exception):
                        raise
                    raise
            raise
    except BaseException as error:
        operation_error = error
        raise
    finally:
        try:
            connection.close()
        except BaseException as close_error:
            if (operation_error is not None and
                    not isinstance(operation_error, Exception)):
                raise operation_error from close_error
            raise
    staged.publish_after_commit()
    return True


def stage_protocol_v2_reserved_fill_replica_in_transaction(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    pool_key: str,
    expected_epoch: int,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    expected_service_generation: int | None = None,
    expected_physical_cluster_uid: str | None = None,
    expected_ordinary_zero_cost_admission_sequence: int | None = None,
    expected_lease_token: int | None = None,
    expected_actuation_mode: str | None = None,
    actuation_lease: 'zero_cost_actuation.IntentLease | None' = None,
) -> StagedReservedFillReplica | None:
    """Stage one protocol-v2 replica on the caller-owned transaction."""
    if actuation_lease is None:
        raise ValueError('Atomic request admission requires one protocol-v2 '
                         'durable actuation lease.')
    return _stage_postgres_replica_if_round_epoch(
        connection,
        service_name,
        replica_id,
        replica_info,
        pool_key=pool_key,
        expected_epoch=expected_epoch,
        expected_service_hash=expected_service_hash,
        expected_controller_owner=expected_controller_owner,
        expected_protocol_version=RESERVED_FILL_PROTOCOL_V2,
        expected_service_generation=expected_service_generation,
        expected_physical_cluster_uid=expected_physical_cluster_uid,
        expected_ordinary_zero_cost_admission_sequence=(
            expected_ordinary_zero_cost_admission_sequence),
        expected_lease_token=expected_lease_token,
        expected_actuation_mode=expected_actuation_mode,
        actuation_lease=actuation_lease)


def _prelock_zero_cost_protocol_for_replica_write(
    session: orm.Session,
    engine: sqlalchemy.engine.Engine,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    *,
    expected_replica_exists: bool,
) -> None:
    """Take the protocol mutex before any lifecycle/service row lock."""
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value and
            _replica_write_may_touch_zero_cost_sequence(
                replica_infos,
                expected_replica_exists=expected_replica_exists)):
        _lock_zero_cost_protocol_sequence_for_update(session)


def _reject_generic_reserved_fill_insert(
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    *,
    expected_replica_exists: bool,
) -> None:
    """Keep typed fill admission behind its complete authority transaction."""
    if (not expected_replica_exists and any(
            getattr(info, 'reserved_fill', False) is True
            for _, info in replica_infos)):
        raise ValueError('A new reserved-fill replica must use the typed '
                         'round/allocation-fenced persistence path.')


def _lock_service_row_if_present_for_replica_write(session: orm.Session,
                                                   service_name: str) -> None:
    """Take lifecycle/service mutexes before any PostgreSQL replica row."""
    if (session.bind is None or session.bind.dialect.name
            != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        return
    _lifecycle_epoch_matches_in_session(session, service_name, None)
    session.execute(
        sqlalchemy.select(services_table.c.name).where(
            services_table.c.name ==
            service_name).with_for_update()).fetchone()


def _lock_and_merge_existing_replica_rows_in_session(
    session: orm.Session,
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
) -> list[tuple[int, 'replica_managers.ReplicaInfo']] | None:
    """Lock expected records and merge recovery fields, or reject the batch.

    The service/lifecycle mutex is acquired before the replica rows.  Callers
    must hold a SQLite immediate transaction before entering this helper.
    Returning ``None`` is an all-or-nothing existence/record-ID conflict: no
    bookkeeping row in the batch may be written. The identity comparison
    precedes recovery-field copying so a stale same-key object cannot adopt a
    newly recreated row's fence.
    """
    if not replica_infos:
        return replica_infos
    is_postgres = (
        engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    if is_postgres:
        _lock_service_row_if_present_for_replica_write(session, service_name)
    replica_ids = sorted({replica_id for replica_id, _ in replica_infos})
    stmt = sqlalchemy.select(
        replicas_table.c.replica_id, replicas_table.c.replica_state_version,
        replicas_table.c.replica_state).where(
            replicas_table.c.service_name == service_name,
            replicas_table.c.replica_id.in_(replica_ids)).order_by(
                replicas_table.c.replica_id)
    if is_postgres:
        stmt = stmt.with_for_update()
    rows = session.execute(stmt).fetchall()
    if {int(row.replica_id) for row in rows} != set(replica_ids):
        return None
    latest_by_id = {
        int(row.replica_id): _replica_from_state(
            row.replica_state_version, row.replica_state) for row in rows
    }
    if any(incoming.replica_record_id !=
           latest_by_id[replica_id].replica_record_id
           for replica_id, incoming in replica_infos):
        return None
    merged = []
    for replica_id, incoming in replica_infos:
        current = latest_by_id[replica_id]
        refreshed = copy.deepcopy(incoming)
        _copy_system_recovery_fields(current, refreshed)
        # Database-assigned event identities are immutable. A stale manager
        # snapshot may legitimately predate either assignment, so whole-row
        # bookkeeping must merge them from the locked record rather than
        # clearing or trusting the incoming copy.
        incoming_admission = refreshed.zero_cost_admission_sequence
        current_admission = current.zero_cost_admission_sequence
        if (incoming_admission is not None and
                incoming_admission != current_admission):
            raise ValueError('A zero-cost admission sequence is immutable.')
        incoming_materialization = (
            refreshed.zero_cost_materialization_sequence)
        current_materialization = (current.zero_cost_materialization_sequence)
        if (incoming_materialization is not None and
                incoming_materialization != current_materialization):
            raise ValueError('A zero-cost materialization sequence is '
                             'immutable.')
        refreshed.zero_cost_admission_sequence = current_admission
        refreshed.zero_cost_materialization_sequence = current_materialization
        # Placement-cost provenance is assigned before the initial insert and
        # cannot be reclassified by a stale whole-row status update.
        refreshed.is_zero_cost = current.is_zero_cost
        merged.append((replica_id, refreshed))
    return merged


def _validate_replica_row_identity(
        replica_id: int, replica_info: 'replica_managers.ReplicaInfo') -> None:
    """Require the physical key and versioned payload to name one replica."""
    payload_replica_id = replica_info.replica_id
    if (isinstance(replica_id, bool) or not isinstance(replica_id, int) or
            isinstance(payload_replica_id, bool) or
            not isinstance(payload_replica_id, int) or
            payload_replica_id != replica_id):
        raise ValueError('Replica row key must match ReplicaInfo.replica_id.')


def _replica_row_values(
        service_name: str, replica_id: int,
        replica_info: 'replica_managers.ReplicaInfo') -> dict[str, Any]:
    """Build the authoritative versioned JSON replica state."""
    _validate_replica_row_identity(replica_id, replica_info)
    replica_state = replica_info.to_storage_dict()
    sky_down_status = replica_info.status_property.sky_down_status
    values = {
        'service_name': service_name,
        'replica_id': replica_id,
        'replica_state_version': _REPLICA_STATE_VERSION,
        'status': replica_info.status.value,
        'sky_down_status':
            (sky_down_status.value if sky_down_status is not None else None),
        'version': replica_info.version,
        'cluster_name': replica_info.cluster_name,
        'created_at': replica_info.created_at,
        'is_spot': replica_info.is_spot,
        'paid_capacity_pool_key': replica_info.paid_capacity_pool_key,
        'replica_state': replica_state,
    }
    assert tuple(values) == _REPLICA_ROW_COLUMNS
    return values


def _initial_replica_row_values(
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
) -> dict[str, Any]:
    """Add typed planner authority only to an initial PostgreSQL insert."""
    values = _replica_row_values(service_name, replica_id, replica_info)
    authorization = getattr(replica_info, 'non_pool_launch_authorization', None)
    if authorization is None:
        return values
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ValueError('Non-pool planner authorization requires PostgreSQL.')
    if not isinstance(authorization, dict):
        raise ValueError('Non-pool planner authorization must be a mapping.')
    values['non_pool_launch_authorization'] = copy.deepcopy(authorization)
    return values


def _stamp_new_zero_cost_replica_admissions_in_session(
    session: orm.Session,
    engine: sqlalchemy.engine.Engine,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
) -> dict[int, int]:
    """Assign commit-order sequence numbers to ordinary zero-cost inserts.

    The protocol singleton is the shared serialization point for observation
    snapshots and zero-cost admissions. The all-row sequence provides durable
    attribution; the ordinary-only sequence invalidates stale fill maps.
    Typed reserved-fill inserts use :func:`add_replica_if_round_epoch`, which
    validates the ordinary high-water and advances only the all-row sequence
    in its final transaction.
    """
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return {}
    candidates = sorted(((replica_id, replica_info)
                         for replica_id, replica_info in replica_infos
                         if replica_info.is_zero_cost is True),
                        key=lambda item: item[0])
    if not candidates:
        return {}
    if any(info.zero_cost_admission_sequence is not None
           for _, info in candidates):
        raise ValueError('A new zero-cost replica admission sequence must be '
                         'assigned by PostgreSQL.')
    if any(info.zero_cost_materialization_sequence is not None
           for _, info in candidates):
        raise ValueError('A new zero-cost replica materialization sequence '
                         'must be assigned by PostgreSQL.')

    table = pool_capacity_observation_schema.protocol_state_sequence_table
    row = _lock_zero_cost_protocol_sequence_for_update(session)
    gate_state = row['reconciliation_gate_state']
    if gate_state == pool_capacity_observation_schema.LEGACY_ACTIVE:
        return {}
    if gate_state != pool_capacity_observation_schema.SEQUENCED_ACTIVE:
        raise RuntimeError('Reserved-fill reconciliation gate is malformed at '
                           'zero-cost replica admission.')
    current = row['zero_cost_admission_sequence']
    ordinary_current = row['ordinary_zero_cost_admission_sequence']
    if (type(current) is not int or current < 0 or  # pylint: disable=unidiomatic-typecheck
            type(ordinary_current) is not int or ordinary_current < 0
            or ordinary_current > current
            or current > 2**63 - 1 - len(candidates)
            or ordinary_current > 2**63 - 1 - len(candidates)):
        raise RuntimeError('Reserved-fill admission sequences are malformed '
                           'or exhausted.')
    successor = current + len(candidates)
    ordinary_successor = ordinary_current + len(candidates)
    update = session.execute(
        sqlalchemy.update(table).where(
            table.c.id == 1, table.c.zero_cost_admission_sequence == current,
            table.c.ordinary_zero_cost_admission_sequence ==
            ordinary_current).values(
                zero_cost_admission_sequence=successor,
                ordinary_zero_cost_admission_sequence=ordinary_successor))
    if update.rowcount != 1:
        raise RuntimeError('Reserved-fill admission sequence lost its locked '
                           'compare-and-swap.')
    # Return transaction-local assignments. The sequencer never mutates a
    # manager-owned object: its caller applies these only to a private copy
    # used for row encoding, then publishes them to the caller after commit.
    return {
        replica_id: current + offset
        for offset, (replica_id, _) in enumerate(candidates, start=1)
    }


def _stamp_zero_cost_replica_materializations_in_session(
    session: orm.Session | sqlalchemy.engine.Connection,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    *,
    provider_successful_replica_ids: frozenset[int] = frozenset(),
) -> dict[int, int]:
    """Assign commit order to first provider-visible zero-cost launches.

    ``sky_launch_status == SUCCEEDED`` is persisted only after the opaque
    provider operation reports success.  Bound-request reducers additionally
    pass their durable provider-success evidence explicitly: teardown may have
    already made ``INTERRUPTED`` the absorbing launch status, but that must not
    hide provider-visible occupancy.  The marker is written in the projection
    transaction, never before provider visibility.  Retrying a later status
    update preserves the immutable marker copied from the locked replica row.
    """
    replica_ids = {replica_id for replica_id, _ in replica_infos}
    if (any(
            type(replica_id) is not int  # pylint: disable=unidiomatic-typecheck
            for replica_id in provider_successful_replica_ids) or
            not provider_successful_replica_ids.issubset(replica_ids)):
        raise ValueError('Provider-success materialization evidence must name '
                         'an exact replica in this transaction.')
    candidates = sorted(
        ((replica_id, info)
         for replica_id, info in replica_infos
         if info.is_zero_cost is True and
         (info.status_property.sky_launch_status == common_utils.ProcessStatus.
          SUCCEEDED or replica_id in provider_successful_replica_ids) and
         info.zero_cost_materialization_sequence is None),
        key=lambda item: item[0])
    if not candidates:
        return {}
    table = pool_capacity_observation_schema.protocol_state_sequence_table
    row = _lock_zero_cost_protocol_sequence_for_update(session)
    gate_state = row['reconciliation_gate_state']
    if gate_state == pool_capacity_observation_schema.LEGACY_ACTIVE:
        # The one-way gate, rather than the separately deployed protocol
        # version, selects the sequencing authority. A protocol-v2 image can
        # run in legacy mode before activation; its historical rows must not
        # start consuming only one of the new event counters.
        return {}
    if (gate_state != pool_capacity_observation_schema.SEQUENCED_ACTIVE or
            int(row['protocol_version']) != RESERVED_FILL_PROTOCOL_V2):
        raise RuntimeError('Reserved-fill materialization sequencer is not in '
                           'a valid active state.')
    current = row['zero_cost_materialization_sequence']
    if (type(current) is not int or current < 0 or
            current > 2**63 - 1 - len(candidates)):
        raise RuntimeError('Reserved-fill materialization sequence is '
                           'malformed or exhausted.')
    successor = current + len(candidates)
    update = session.execute(
        sqlalchemy.update(table).where(
            table.c.id == 1,
            table.c.zero_cost_materialization_sequence == current).values(
                zero_cost_materialization_sequence=successor))
    if update.rowcount != 1:
        raise RuntimeError('Reserved-fill materialization sequence lost its '
                           'locked compare-and-swap.')
    return {
        replica_id: current + offset
        for offset, (replica_id, _) in enumerate(candidates, start=1)
    }


def _apply_zero_cost_sequence_assignments(
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    *,
    admissions: Mapping[int, int] | None = None,
    materializations: Mapping[int, int] | None = None,
) -> None:
    """Apply database assignments to transaction-owned replica copies."""
    admissions = admissions or {}
    materializations = materializations or {}
    for replica_id, replica_info in replica_infos:
        if replica_id in admissions:
            replica_info.zero_cost_admission_sequence = admissions[replica_id]
        if replica_id in materializations:
            replica_info.zero_cost_materialization_sequence = (
                materializations[replica_id])


def _publish_committed_zero_cost_sequences(
    caller_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    persisted_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
) -> None:
    """Publish committed identities back to manager-owned zero-cost objects."""
    persisted_by_id = dict(persisted_infos)
    for replica_id, caller_info in caller_infos:
        if caller_info.is_zero_cost is not True:
            # An existing-row update may have supplied stale cost provenance;
            # database truth was still merged and persisted, but attaching a
            # marker to that stale object would make its local state invalid.
            continue
        persisted = persisted_by_id[replica_id]
        caller_info.zero_cost_admission_sequence = (
            persisted.zero_cost_admission_sequence)
        caller_info.zero_cost_materialization_sequence = (
            persisted.zero_cost_materialization_sequence)


def update_replica_for_bound_ordinary_launch_in_transaction(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    service_hash: str,
    replica_id: int,
    replica_record_id: str,
    association_id: uuid.UUID,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    provider_launch_succeeded: bool,
    paid_capacity_pool_key: str | None,
    paid_capacity_outcome: paid_capacity.LaunchOutcome | None,
) -> bool:
    """Persist one exact bound-launch result on its reducer transaction.

    The request reducer has already locked lifecycle, service, replica, and
    association rows in canonical order.  This update-only helper deliberately
    performs no commit and preserves the scalar association pointer until the
    reducer clears it after the replica state is durable.  The explicit
    provider-success bit comes from that transaction's locked terminal request
    row; it is intentionally independent of the replica's absorbing teardown
    status.
    """
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    try:
        record_uuid = uuid.UUID(replica_record_id)
    except (AttributeError, TypeError, ValueError):
        return False
    if (str(record_uuid) != replica_record_id or not service_hash or
            not isinstance(association_id, uuid.UUID) or
            type(provider_launch_succeeded) is not bool or  # pylint: disable=unidiomatic-typecheck
            replica_info.replica_record_id != replica_record_id
            or replica_info.replica_id != replica_id):
        return False
    transaction_replica_info = copy.deepcopy(replica_info)
    claim = None
    pool = None
    if paid_capacity_pool_key is None:
        if paid_capacity_outcome is not None:
            return False
    else:
        if (not isinstance(paid_capacity_pool_key, str) or
                not paid_capacity_pool_key or not isinstance(
                    paid_capacity_outcome, paid_capacity.LaunchOutcome) or
                transaction_replica_info.paid_capacity_pool_key
                != paid_capacity_pool_key):
            return False
        claim = connection.execute(
            sqlalchemy.select(paid_capacity_claims_table).where(
                paid_capacity_claims_table.c.service_name == service_name,
                paid_capacity_claims_table.c.service_hash == service_hash,
                paid_capacity_claims_table.c.replica_id == replica_id,
                paid_capacity_claims_table.c.pool_key ==
                paid_capacity_pool_key).with_for_update()).one_or_none()
        pool = connection.execute(
            sqlalchemy.select(paid_capacity_pools_table).where(
                paid_capacity_pools_table.c.pool_key ==
                paid_capacity_pool_key).with_for_update()).one_or_none()
        if claim is None or pool is None:
            return False
    transaction_infos = [(replica_id, transaction_replica_info)]
    materializations = _stamp_zero_cost_replica_materializations_in_session(
        connection,
        transaction_infos,
        provider_successful_replica_ids=(frozenset(
            (replica_id,)) if provider_launch_succeeded else frozenset()))
    _apply_zero_cost_sequence_assignments(transaction_infos,
                                          materializations=materializations)
    values = _replica_row_values(service_name, replica_id,
                                 transaction_replica_info)
    result = connection.execute(
        sqlalchemy.update(replicas_table).where(
            replicas_table.c.service_name == service_name,
            replicas_table.c.replica_id == replica_id,
            replicas_table.c.ordinary_launch_association_id == association_id,
            replicas_table.c.replica_state['replica_record_id'].as_string() ==
            replica_record_id).values({
                key: value
                for key, value in values.items()
                if key not in ('service_name', 'replica_id')
            }))
    if result.rowcount != 1:
        return False
    if paid_capacity_pool_key is None:
        return True

    assert paid_capacity_outcome is not None and claim is not None
    assert pool is not None
    now = _paid_capacity_clock_timestamp(connection, None)
    base_limit = paid_capacity.base_limit()
    max_limit = paid_capacity.max_limit()
    success_ttl = paid_capacity.success_ttl_seconds()
    failure_cooldown = paid_capacity.failure_cooldown_seconds()
    if paid_capacity_outcome in (paid_capacity.LaunchOutcome.CAPACITY_FAILURE,
                                 paid_capacity.LaunchOutcome.QUOTA_FAILURE):
        connection.execute(
            sqlalchemy.update(paid_capacity_pools_table).where(
                paid_capacity_pools_table.c.pool_key ==
                paid_capacity_pool_key).values(current_limit=base_limit,
                                               successes_since_resize=0,
                                               last_success_at=None,
                                               last_failure_at=now,
                                               updated_at=now))
        return True
    if paid_capacity_outcome != paid_capacity.LaunchOutcome.SUCCESS:
        return True

    if pool.last_failure_at is not None:
        if not (pool.current_limit == 1 and
                claim.claimed_at >= pool.last_failure_at + failure_cooldown):
            return True
        ramp_update = paid_capacity.record_outcomes(
            base_limit,
            0,
            None, [paid_capacity.LaunchOutcome.SUCCESS],
            bootstrap_limit=base_limit,
            ceiling_limit=max_limit,
            now=now,
            ttl_seconds=success_ttl)
        connection.execute(
            sqlalchemy.update(paid_capacity_pools_table).where(
                paid_capacity_pools_table.c.pool_key ==
                paid_capacity_pool_key).values(
                    current_limit=ramp_update.current_limit,
                    successes_since_resize=ramp_update.successes_since_resize,
                    last_success_at=now,
                    last_failure_at=None,
                    updated_at=now))
        return True

    ramp_update = paid_capacity.record_outcomes(
        pool.current_limit,
        pool.successes_since_resize,
        pool.last_success_at, [paid_capacity.LaunchOutcome.SUCCESS],
        bootstrap_limit=base_limit,
        ceiling_limit=max_limit,
        now=now,
        ttl_seconds=success_ttl)
    connection.execute(
        sqlalchemy.update(paid_capacity_pools_table).where(
            paid_capacity_pools_table.c.pool_key ==
            paid_capacity_pool_key).values(
                current_limit=ramp_update.current_limit,
                successes_since_resize=ramp_update.successes_since_resize,
                last_success_at=now,
                updated_at=now))
    return True


def read_replica_for_bound_ordinary_launch_in_transaction(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    replica_id: int,
    replica_record_id: str,
    association_id: uuid.UUID,
) -> 'replica_managers.ReplicaInfo':
    """Decode the already-locked current row for an atomic result reducer."""
    row = connection.execute(
        sqlalchemy.select(
            replicas_table.c.replica_state_version,
            replicas_table.c.replica_state).where(
                replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id == replica_id,
                replicas_table.c.ordinary_launch_association_id ==
                association_id,
                replicas_table.c.replica_state['replica_record_id'].as_string()
                == replica_record_id)).one_or_none()
    if row is None:
        raise ReplicaSystemRecoveryStateError(
            'Bound ordinary launch lost its exact locked replica row.')
    info = _replica_from_state(row.replica_state_version, row.replica_state)
    if info.replica_id != replica_id or info.replica_record_id != replica_record_id:
        raise ReplicaSystemRecoveryStateError(
            'Bound ordinary launch replica identity is malformed.')
    return info


def _upsert_replica_rows_in_session(
    session: orm.Session,
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    *,
    expected_replica_exists: bool = False,
) -> list[tuple[int, 'replica_managers.ReplicaInfo']] | None:
    """Persist replica rows in dialect-safe bounded batches.

    Expected-existing bookkeeping is deliberately UPDATE-only.  The locked
    precondition rejects the whole batch before its first write if any row is
    absent, so a stale manager snapshot cannot recreate terminally deleted
    replicas. Explicit initial-admission callers use an INSERT-only path.
    """
    for replica_id, replica_info in replica_infos:
        _validate_replica_row_identity(replica_id, replica_info)
    # Sequence assignment must never leak out of a transaction that later
    # rejects or rolls back. All merging, stamping and row encoding therefore
    # operate on private copies; public callers copy the committed identities
    # back only after session.commit() succeeds.
    replica_infos = [(replica_id, copy.deepcopy(replica_info))
                     for replica_id, replica_info in replica_infos]
    if expected_replica_exists:
        merged_infos = _lock_and_merge_existing_replica_rows_in_session(
            session, engine, service_name, replica_infos)
        if merged_infos is None:
            return None
        replica_infos = merged_infos
    else:
        admissions = _stamp_new_zero_cost_replica_admissions_in_session(
            session, engine, replica_infos)
        _apply_zero_cost_sequence_assignments(replica_infos,
                                              admissions=admissions)
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        materializations = (
            _stamp_zero_cost_replica_materializations_in_session(
                session, replica_infos))
        _apply_zero_cost_sequence_assignments(replica_infos,
                                              materializations=materializations)
        if expected_replica_exists:
            for replica_id, replica_info in replica_infos:
                if (replica_info.status != ReplicaStatus.READY or
                        replica_info.status_property.is_scale_down is True or
                        replica_info.system_recovery_quarantine is not None):
                    route_projection.revoke_replica_lease_in_session(
                        session, service_name, replica_id,
                        replica_info.replica_record_id,
                        'replica_became_route_ineligible')
    chunk_size = (max(1, _SQLITE_MAX_BIND_PARAMS //
                      len(_REPLICA_ROW_COLUMNS)) if engine.dialect.name
                  == db_utils.SQLAlchemyDialect.SQLITE.value else
                  _POSTGRESQL_REPLICA_UPSERT_CHUNK_SIZE)
    if expected_replica_exists:
        value_column_names = tuple(
            column_name for column_name in _REPLICA_ROW_COLUMNS
            if column_name not in ('service_name', 'replica_id'))
        update_stmt = sqlalchemy.update(replicas_table).where(
            replicas_table.c.service_name == sqlalchemy.bindparam(
                '_expected_service_name'), replicas_table.c.replica_id ==
            sqlalchemy.bindparam('_expected_replica_id')).values({
                column_name: sqlalchemy.bindparam(
                    f'_replica_{column_name}',
                    type_=replicas_table.c[column_name].type)
                for column_name in value_column_names
            })
        for start in range(0, len(replica_infos), chunk_size):
            chunk = replica_infos[start:start + chunk_size]
            parameters = []
            for replica_id, replica_info in chunk:
                row_values = _replica_row_values(service_name, replica_id,
                                                 replica_info)
                parameter = {
                    '_expected_service_name': service_name,
                    '_expected_replica_id': replica_id,
                }
                parameter.update({
                    f'_replica_{column_name}': row_values[column_name]
                    for column_name in value_column_names
                })
                parameters.append(parameter)
            result = session.execute(update_stmt, parameters)
            if result.rowcount >= 0 and result.rowcount != len(chunk):
                return None
        return replica_infos
    insert_func = _upsert_insert_func(engine)
    for start in range(0, len(replica_infos), chunk_size):
        chunk = replica_infos[start:start + chunk_size]
        insert_rows = [
            _initial_replica_row_values(engine, service_name, replica_id,
                                        replica_info)
            for replica_id, replica_info in chunk
        ]
        if any('non_pool_launch_authorization' in values
               for values in insert_rows):
            for values in insert_rows:
                values.setdefault('non_pool_launch_authorization', None)
        insert_stmt = insert_func(replicas_table).values(insert_rows)
        session.execute(insert_stmt)
    return replica_infos


def _replica_from_state(
        replica_state_version: int,
        replica_state: dict[str, Any]) -> 'replica_managers.ReplicaInfo':
    if replica_state_version != _REPLICA_STATE_VERSION:
        raise RuntimeError('Unsupported replica state version: '
                           f'{replica_state_version!r}')
    replica = replica_managers.ReplicaInfo.from_storage_dict(replica_state)
    quarantine = replica.system_recovery_quarantine
    if quarantine is not None:
        # This operational warning belongs at the ordinary runtime row-read
        # boundary. The pure decoder is also used by secret-safe maintenance
        # operations, which must never emit persisted row identities.
        logger.warning(
            'Quarantined system recovery state for replica %s (%s); '
            'the row remains off-route pending legacy cleanup.',
            replica.replica_id, quarantine.reason.value)
    if replica.status == ReplicaStatus.UNKNOWN:
        logger.error('Decoded replica row projected UNKNOWN status; keeping '
                     'it off-route pending state reconciliation.')
    return replica


def decode_replica_state_for_authority(
        replica_state_version: int,
        replica_state: dict[str, Any]) -> 'replica_managers.ReplicaInfo':
    """Decode one replica row for a cross-table authority decision."""
    return _replica_from_state(replica_state_version, replica_state)


def _lock_service_owner_row_in_session(
    session: orm.Session,
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None] | None,
    *,
    require_launch_allowed: bool,
) -> sqlalchemy.engine.Row | None:
    """Lock and return one valid service controller owner row."""
    _lifecycle_epoch_matches_in_session(session, service_name, None)
    owner = session.execute(
        sqlalchemy.select(services_table.c.hash,
                          services_table.c.controller_pid,
                          services_table.c.controller_ip,
                          services_table.c.status,
                          services_table.c.resource_scope,
                          services_table.c.current_version).where(
                              services_table.c.name ==
                              service_name).with_for_update()).fetchone()
    if (owner is None or owner[0] != expected_service_hash or
        (expected_controller_owner is not None and
         (owner[1], owner[2]) != expected_controller_owner)):
        return None
    if require_launch_allowed and owner[3] in {
            status.value
            for status in ServiceStatus.replica_launch_blocking_statuses()
    }:
        return None
    return owner


def _lock_service_owner_in_session(
    session: orm.Session,
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None] | None,
    *,
    require_launch_allowed: bool,
) -> bool:
    """Lock and validate one service controller owner."""
    return _lock_service_owner_row_in_session(
        session,
        service_name,
        expected_service_hash,
        expected_controller_owner,
        require_launch_allowed=require_launch_allowed) is not None


def _lock_system_recovery_service_owner_in_session(
    session: orm.Session,
    service_name: str,
    expected_service_hash: str,
    expected_lifecycle_epoch: int | None,
    expected_controller_owner: tuple[int | None, str | None],
    *,
    require_launch_allowed: bool,
) -> sqlalchemy.engine.Row:
    """Lock lifecycle then the exclusive service mutex for one mutation."""
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(expected_service_hash, str) or
            not expected_service_hash or
            not isinstance(expected_controller_owner, tuple) or
            len(expected_controller_owner) != 2):
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery owner identity is invalid.')
    fence = session.execute(
        sqlalchemy.select(service_lifecycle_fences_table.c.epoch).where(
            service_lifecycle_fences_table.c.name ==
            service_name).with_for_update()).fetchone()
    if fence is None:
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery lifecycle fence is absent.')
    locked_epoch = int(fence.epoch)
    if locked_epoch < 1:
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery lifecycle fence is invalid.')
    if (expected_lifecycle_epoch is not None and
        (isinstance(expected_lifecycle_epoch, bool) or
         not isinstance(expected_lifecycle_epoch, int) or
         expected_lifecycle_epoch < 1 or
         expected_lifecycle_epoch != locked_epoch)):
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery lifecycle fence changed.')

    owner = session.execute(
        sqlalchemy.select(
            services_table.c.name,
            services_table.c.hash,
            services_table.c.lifecycle_epoch,
            services_table.c.controller_pid,
            services_table.c.controller_ip,
            services_table.c.status,
            services_table.c.pool,
            services_table.c.resource_action_mode,
            services_table.c.workspace,
        ).where(services_table.c.name ==
                service_name).with_for_update()).fetchone()
    if (owner is None or owner.hash != expected_service_hash or
            owner.lifecycle_epoch != locked_epoch or
        (owner.controller_pid, owner.controller_ip) != expected_controller_owner
            or bool(owner.pool) or owner.resource_action_mode != 'legacy'):
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery service owner no longer matches.')
    launch_blocking_statuses = {
        status.value
        for status in ServiceStatus.replica_launch_blocking_statuses()
    }
    if require_launch_allowed and owner.status in launch_blocking_statuses:
        raise ReplicaSystemRecoveryMutationRejected(
            'Service teardown blocks system-recovery mutation.')
    return owner


def _lock_replica_info_for_system_recovery(
    session: orm.Session,
    service_name: str,
    replica_id: int,
) -> 'replica_managers.ReplicaInfo':
    if (isinstance(replica_id, bool) or not isinstance(replica_id, int) or
            replica_id <= 0):
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery replica ID must be positive.')
    row = session.execute(
        sqlalchemy.select(replicas_table.c.replica_state_version,
                          replicas_table.c.replica_state).where(
                              replicas_table.c.service_name == service_name,
                              replicas_table.c.replica_id ==
                              replica_id).with_for_update()).fetchone()
    if row is None:
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery replica row is absent.')
    try:
        replica_info = _replica_from_state(row.replica_state_version,
                                           row.replica_state)
    except Exception as error:
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery replica row is unreadable.') from error
    if replica_info.replica_id != replica_id:
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery replica identity changed.')
    return replica_info


def _write_locked_replica_info_in_session(
        session: orm.Session, service_name: str, replica_id: int,
        replica_info: 'replica_managers.ReplicaInfo') -> None:
    try:
        values = _replica_row_values(service_name, replica_id, replica_info)
    except (AttributeError, TypeError, ValueError) as error:
        raise ReplicaSystemRecoveryMutationRejected(
            'Replica recovery state could not be serialized.') from error
    result = session.execute(
        sqlalchemy.update(replicas_table).where(
            replicas_table.c.service_name == service_name,
            replicas_table.c.replica_id == replica_id).values({
                key: value
                for key, value in values.items()
                if key not in ('service_name', 'replica_id')
            }))
    if result.rowcount != 1:
        raise ReplicaSystemRecoveryMutationRejected(
            'System-recovery update lost its replica row.')


def _mutate_replica_system_recovery(
    service_name: str,
    replica_id: int,
    transition: typing.Callable[['replica_managers.ReplicaInfo'],
                                'replica_managers.ReplicaInfo'],
    *,
    expected_service_hash: str,
    expected_lifecycle_epoch: int,
    expected_controller_owner: tuple[int | None, str | None],
    expected_revision: int,
) -> 'replica_managers.ReplicaInfo':
    engine = _require_system_recovery_postgres()
    if (isinstance(expected_revision, bool) or
            not isinstance(expected_revision, int) or expected_revision < 0):
        raise ValueError('expected_revision must be a nonnegative integer.')
    with orm.Session(engine) as session, session.begin():
        owner = _lock_system_recovery_service_owner_in_session(
            session,
            service_name,
            expected_service_hash,
            expected_lifecycle_epoch,
            expected_controller_owner,
            require_launch_allowed=True)
        current = _lock_replica_info_for_system_recovery(
            session, service_name, replica_id)
        current_revision = _system_recovery_revision(current)
        if current_revision != expected_revision:
            raise ReplicaSystemRecoveryRevisionConflict(expected_revision,
                                                        current_revision)
        desired = transition(copy.deepcopy(current))
        if (not isinstance(desired, replica_managers.ReplicaInfo) or
                desired.replica_id != replica_id):
            raise ReplicaSystemRecoveryMutationRejected(
                'Recovery transition returned a different replica.')
        desired_intent = desired.system_recovery_launch_intent
        if (desired_intent is not None and
            (desired_intent.service_hash != expected_service_hash or
             desired_intent.replica_id != replica_id or
             desired_intent.launch_generation != replica_id or
             desired_intent.workspace != owner.workspace)):
            raise ReplicaSystemRecoveryMutationRejected(
                'Recovery intent does not match its locked service generation.')
        _validate_system_recovery_transition(current, desired)
        if _system_recovery_snapshot(current) == _system_recovery_snapshot(
                desired):
            return current
        try:
            _copy_system_recovery_fields(desired,
                                         current,
                                         increment_revision=True)
        except (AttributeError, TypeError, ValueError) as error:
            raise ReplicaSystemRecoveryMutationRejected(
                'Recovery transition produced an invalid v13 bundle.'
            ) from error
        if _system_recovery_revision(current) != current_revision + 1:
            raise ReplicaSystemRecoveryMutationRejected(
                'Recovery transition did not increment its revision once.')
        _write_locked_replica_info_in_session(session, service_name, replica_id,
                                              current)
        return current


def patch_replica_system_recovery(
    service_name: str,
    replica_id: int,
    desired_info: 'replica_managers.ReplicaInfo',
    *,
    expected_service_hash: str,
    expected_lifecycle_epoch: int,
    expected_controller_owner: tuple[int | None, str | None],
    expected_revision: int,
) -> 'replica_managers.ReplicaInfo':
    """Apply a caller-reduced recovery patch to the locked latest replica.

    The nine mutable recovery fields are copied only after the immutable
    record identity is proven equal; the identity itself is never copied.
    Every other field comes from the locked row, so a stale callback cannot
    overwrite concurrent readiness/teardown state. Revision conflict is
    explicit: callers refresh and rerun their pure reducer rather than
    replaying a stale output.
    """
    if (not isinstance(desired_info, replica_managers.ReplicaInfo) or
            desired_info.replica_id != replica_id):
        raise ValueError('desired_info must match replica_id.')
    return _mutate_replica_system_recovery(
        service_name,
        replica_id,
        lambda _: desired_info,
        expected_service_hash=expected_service_hash,
        expected_lifecycle_epoch=expected_lifecycle_epoch,
        expected_controller_owner=expected_controller_owner,
        expected_revision=expected_revision)


def create_replica_system_recovery_candidate(
    service_name: str,
    replica_id: int,
    desired_info: 'replica_managers.ReplicaInfo',
    *,
    expected_service_hash: str,
    expected_lifecycle_epoch: int,
    expected_controller_owner: tuple[int | None, str | None],
    expected_revision: int,
) -> 'replica_managers.ReplicaInfo':
    """Persist the first owner-fenced CANDIDATE transition."""
    if (_system_recovery_disposition(desired_info) != 'CANDIDATE' or
            desired_info.system_recovery_launch_intent is None):
        raise ValueError('Candidate persistence requires an exact intent.')
    return patch_replica_system_recovery(
        service_name,
        replica_id,
        desired_info,
        expected_service_hash=expected_service_hash,
        expected_lifecycle_epoch=expected_lifecycle_epoch,
        expected_controller_owner=expected_controller_owner,
        expected_revision=expected_revision)


def demote_replica_system_recovery_to_ordinary(
    service_name: str,
    replica_id: int,
    desired_info: 'replica_managers.ReplicaInfo',
    *,
    expected_service_hash: str,
    expected_lifecycle_epoch: int,
    expected_controller_owner: tuple[int | None, str | None],
    expected_revision: int,
) -> 'replica_managers.ReplicaInfo':
    """Persist one irreversible CANDIDATE-to-ORDINARY reduction."""
    if _system_recovery_disposition(desired_info) != 'ORDINARY':
        raise ValueError('Demotion target must be ORDINARY.')
    return patch_replica_system_recovery(
        service_name,
        replica_id,
        desired_info,
        expected_service_hash=expected_service_hash,
        expected_lifecycle_epoch=expected_lifecycle_epoch,
        expected_controller_owner=expected_controller_owner,
        expected_revision=expected_revision)


def bind_replica_system_recovery_launch_request(
    unbound_context: dict[str, Any],
    request_id: str,
) -> 'replica_managers.ReplicaInfo':
    """Consume one launch nonce and bind the API server's request ID."""
    context = system_oom_recovery.validate_unbound_launch_context(
        unbound_context)
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('request_id must be a nonempty string.')
    service_name = context[constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY]
    service_hash = context[constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY]
    controller_owner = (
        context[constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY],
        context[constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY],
    )
    replica_id = context[constants.SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY]
    workspace = context[constants.SYSTEM_OOM_RECOVERY_WORKSPACE_KEY]
    engine = _require_system_recovery_postgres()
    with orm.Session(engine) as session, session.begin():
        owner = _lock_system_recovery_service_owner_in_session(
            session,
            service_name,
            service_hash,
            None,
            controller_owner,
            require_launch_allowed=True)
        if owner.workspace != workspace:
            raise ReplicaSystemRecoveryMutationRejected(
                'System-recovery workspace changed before request bind.')
        current = _lock_replica_info_for_system_recovery(
            session, service_name, replica_id)
        if (_has_system_recovery_teardown_intent(current) or
                current.system_recovery_quarantine is not None or
                _nested_system_recovery_is_exhausted(current) or
                _system_recovery_disposition(current) != 'CANDIDATE'):
            raise ReplicaSystemRecoveryMutationRejected(
                'Only a live CANDIDATE may bind a launch request.')
        if current.launch_request_id is not None:
            raise ReplicaSystemRecoveryMutationRejected(
                'Recovery launch nonce was already consumed.')
        intent = current.system_recovery_launch_intent
        if intent is None:
            raise ReplicaSystemRecoveryMutationRejected(
                'Recovery candidate has no launch intent.')
        expected_context = system_oom_recovery.create_unbound_launch_context(
            intent,
            service_name=service_name,
            service_version=current.version,
            controller_pid=owner.controller_pid,
            controller_ip=owner.controller_ip)
        if context != expected_context:
            raise ReplicaSystemRecoveryMutationRejected(
                'Recovery launch context does not match the locked intent.')
        current.launch_request_id = request_id
        current.system_recovery_revision = _system_recovery_revision(
            current) + 1
        _write_locked_replica_info_in_session(session, service_name, replica_id,
                                              current)
        return current


def set_replica_system_recovery_job_id(
    service_name: str,
    replica_id: int,
    service_job_id: int,
    *,
    expected_launch_request_id: str,
    expected_service_hash: str,
    expected_lifecycle_epoch: int,
    expected_controller_owner: tuple[int | None, str | None],
    expected_revision: int,
) -> 'replica_managers.ReplicaInfo':
    """Bind the exact ordinary request result's service job ID once."""
    if (isinstance(service_job_id, bool) or
            not isinstance(service_job_id, int) or service_job_id <= 0 or
            not isinstance(expected_launch_request_id, str) or
            not expected_launch_request_id):
        raise ValueError('Job/request IDs are invalid.')

    def _set_job_id(
        current: 'replica_managers.ReplicaInfo',
    ) -> 'replica_managers.ReplicaInfo':
        if current.launch_request_id != expected_launch_request_id:
            raise ReplicaSystemRecoveryMutationRejected(
                'Launch request association changed before job bind.')
        if current.service_job_id not in (None, service_job_id):
            raise ReplicaSystemRecoveryMutationRejected(
                'A different service job ID is already bound.')
        current.service_job_id = service_job_id
        return current

    return _mutate_replica_system_recovery(
        service_name,
        replica_id,
        _set_job_id,
        expected_service_hash=expected_service_hash,
        expected_lifecycle_epoch=expected_lifecycle_epoch,
        expected_controller_owner=expected_controller_owner,
        expected_revision=expected_revision)


def get_service_placement_policy_states(
        service_name: str) -> dict[str, dict[str, Any] | None] | None:
    """Read restart-safe placer and economic-stabilization state."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.spot_placement_state,
                services_table.c.cost_rebalance_state,
            ).where(services_table.c.name == service_name)).fetchone()
    if row is None:
        return None
    return {
        'spot_placement_state': row.spot_placement_state if isinstance(
            row.spot_placement_state, dict) else None,
        'cost_rebalance_state': row.cost_rebalance_state if isinstance(
            row.cost_rebalance_state, dict) else None,
    }


def _set_service_placement_policy_state(
    service_name: str,
    service_hash: str,
    controller_owner: tuple[int | None, str | None] | None,
    *,
    column: sqlalchemy.Column[Any],
    state: dict[str, Any],
    require_launch_allowed: bool,
) -> bool:
    """Persist one controller-owned policy state under the service fence."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_service_owner_in_session(
                session,
                service_name,
                service_hash,
                controller_owner,
                require_launch_allowed=require_launch_allowed):
            session.rollback()
            return False
        session.execute(
            sqlalchemy.update(services_table).where(
                services_table.c.name == service_name).values({column: state}))
        session.commit()
    return True


def set_service_spot_placement_state(
    service_name: str,
    service_hash: str,
    controller_owner: tuple[int | None, str | None] | None,
    state: dict[str, Any],
) -> bool:
    """Persist exact-location bench evidence, including during teardown."""
    return _set_service_placement_policy_state(
        service_name,
        service_hash,
        controller_owner,
        column=services_table.c.spot_placement_state,
        state=state,
        require_launch_allowed=False)


def set_service_cost_rebalance_state(
    service_name: str,
    service_hash: str,
    controller_owner: tuple[int | None, str | None] | None,
    state: dict[str, Any],
) -> bool:
    """Persist candidate stabilization before a replacement can launch."""
    return _set_service_placement_policy_state(
        service_name,
        service_hash,
        controller_owner,
        column=services_table.c.cost_rebalance_state,
        state=state,
        require_launch_allowed=True)


def _valid_paid_capacity_claims_in_session(
    session: orm.Session, pool_key: str
) -> tuple[list[tuple[str, str, int]], list[tuple[str, str, int]]]:
    """Return valid and stale claim identities for one locked pool."""
    rows = session.execute(
        sqlalchemy.select(
            paid_capacity_claims_table.c.service_name,
            paid_capacity_claims_table.c.service_hash,
            paid_capacity_claims_table.c.replica_id,
            services_table.c.hash,
            replicas_table.c.status,
            replicas_table.c.paid_capacity_pool_key,
            replicas_table.c.ordinary_launch_association_id,
        ).select_from(
            paid_capacity_claims_table.outerjoin(
                services_table, services_table.c.name ==
                paid_capacity_claims_table.c.service_name).outerjoin(
                    replicas_table,
                    sqlalchemy.and_(
                        replicas_table.c.service_name ==
                        paid_capacity_claims_table.c.service_name,
                        replicas_table.c.replica_id ==
                        paid_capacity_claims_table.c.replica_id))).
        where(paid_capacity_claims_table.c.pool_key == pool_key)).fetchall()
    valid = []
    stale = []
    for (service_name, service_hash, replica_id, current_hash, status, row_pool,
         association_id) in rows:
        identity = (service_name, service_hash, replica_id)
        if (current_hash == service_hash and
            (status in _PAID_CAPACITY_UNRESOLVED_STATUSES or
             association_id is not None) and row_pool == pool_key):
            valid.append(identity)
        else:
            stale.append(identity)
    return valid, stale


def _valid_paid_capacity_service_claims_in_session(
    session: orm.Session,
    service_name: str,
    service_hash: str,
) -> tuple[list[tuple[int, str]], list[tuple[str, str, int]]]:
    """Return one locked service incarnation's valid and stale claims."""
    rows = session.execute(
        sqlalchemy.select(
            paid_capacity_claims_table.c.replica_id,
            paid_capacity_claims_table.c.pool_key,
            replicas_table.c.status,
            replicas_table.c.paid_capacity_pool_key,
            replicas_table.c.ordinary_launch_association_id,
        ).select_from(
            paid_capacity_claims_table.outerjoin(
                replicas_table,
                sqlalchemy.and_(
                    replicas_table.c.service_name ==
                    paid_capacity_claims_table.c.service_name,
                    replicas_table.c.replica_id ==
                    paid_capacity_claims_table.c.replica_id))).where(
                        paid_capacity_claims_table.c.service_name ==
                        service_name, paid_capacity_claims_table.c.service_hash
                        == service_hash)).fetchall()
    valid = []
    stale = []
    for replica_id, pool_key, status, row_pool, association_id in rows:
        if ((status in _PAID_CAPACITY_UNRESOLVED_STATUSES or
             association_id is not None) and row_pool == pool_key):
            valid.append((replica_id, pool_key))
        else:
            stale.append((service_name, service_hash, replica_id))
    return valid, stale


def _delete_paid_capacity_claims_in_session(
        session: orm.Session, identities: list[tuple[str, str, int]]) -> None:
    if not identities:
        return
    session.execute(
        sqlalchemy.delete(paid_capacity_claims_table).where(
            sqlalchemy.tuple_(
                paid_capacity_claims_table.c.service_name,
                paid_capacity_claims_table.c.service_hash,
                paid_capacity_claims_table.c.replica_id).in_(identities)))


def _withdraw_ineligible_frontier_waiters_in_session(
    session: orm.Session,
    service_name: str,
    service_hash: str,
    service_claims: list[tuple[int, str]],
    frontier_limit: int,
    frontier_limits_by_key: dict[paid_capacity.FrontierKey, int] | None = None,
) -> None:
    """Remove waiters on every card whose exploration frontier is full."""
    owned_by_frontier: dict[paid_capacity.FrontierKey,
                            set[str]] = collections.defaultdict(set)
    unknown_owned_pool_keys = set()
    for _, pool_key in service_claims:
        parsed = paid_capacity.frontier_key_from_pool_key(pool_key)
        if parsed is None:
            unknown_owned_pool_keys.add(pool_key)
        else:
            owned_by_frontier[parsed].add(pool_key)
    waiter_pool_keys = session.execute(
        sqlalchemy.select(paid_capacity_waiters_table.c.pool_key).where(
            paid_capacity_waiters_table.c.service_name == service_name,
            paid_capacity_waiters_table.c.service_hash ==
            service_hash)).scalars().all()
    withdraw = []
    for pool_key in waiter_pool_keys:
        parsed = paid_capacity.frontier_key_from_pool_key(pool_key)
        if parsed is None:
            owned_pool_keys = unknown_owned_pool_keys
        else:
            owned_pool_keys = (owned_by_frontier.get(parsed, set()) |
                               unknown_owned_pool_keys)
        effective_limit = frontier_limit
        if parsed is not None and frontier_limits_by_key is not None:
            effective_limit = frontier_limits_by_key.get(
                parsed, effective_limit)
        if (pool_key not in owned_pool_keys and
                len(owned_pool_keys) >= effective_limit):
            withdraw.append(pool_key)
    if withdraw:
        session.execute(
            sqlalchemy.delete(paid_capacity_waiters_table).where(
                paid_capacity_waiters_table.c.service_name == service_name,
                paid_capacity_waiters_table.c.service_hash == service_hash,
                paid_capacity_waiters_table.c.pool_key.in_(withdraw)))


def _withdraw_all_paid_capacity_waiters_in_session(
    session: orm.Session,
    service_name: str,
    service_hash: str,
) -> None:
    """Remove waiters when one service cannot acquire any new paid claim."""
    session.execute(
        sqlalchemy.delete(paid_capacity_waiters_table).where(
            paid_capacity_waiters_table.c.service_name == service_name,
            paid_capacity_waiters_table.c.service_hash == service_hash))


def _reconcile_ineligible_paid_capacity_waiters(
    service_name: str,
    service_hash: str,
    *,
    service_limit: int | None,
    frontier_limit: int | None,
    frontier_limits_by_key: dict[paid_capacity.FrontierKey, int] | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None,
) -> bool:
    """Withdraw newly ineligible waiters without holding any pool lock."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_service_owner_in_session(session,
                                              service_name,
                                              service_hash,
                                              expected_controller_owner,
                                              require_launch_allowed=False):
            session.rollback()
            return False
        service_claims, stale_claims = (
            _valid_paid_capacity_service_claims_in_session(
                session, service_name, service_hash))
        _delete_paid_capacity_claims_in_session(session, stale_claims)
        if service_limit is not None and len(service_claims) >= service_limit:
            _withdraw_all_paid_capacity_waiters_in_session(
                session, service_name, service_hash)
        elif frontier_limit is not None:
            _withdraw_ineligible_frontier_waiters_in_session(
                session, service_name, service_hash, service_claims,
                frontier_limit, frontier_limits_by_key)
        session.commit()
    return True


def _ensure_paid_capacity_pool_in_session(session: orm.Session,
                                          engine: sqlalchemy.engine.Engine,
                                          pool_key: str, base_limit: int,
                                          now: float | None) -> None:
    updated_at = now
    if updated_at is None:
        updated_at = sqlalchemy.extract('epoch',
                                        sqlalchemy.func.clock_timestamp())
    insert_stmt = _upsert_insert_func(engine)(paid_capacity_pools_table).values(
        pool_key=pool_key,
        current_limit=base_limit,
        successes_since_resize=0,
        updated_at=updated_at)
    session.execute(
        insert_stmt.on_conflict_do_nothing(index_elements=['pool_key']))


def _paid_capacity_pool_row_for_update(session: orm.Session,
                                       pool_key: str) -> Any:
    row = session.execute(
        sqlalchemy.select(paid_capacity_pools_table).where(
            paid_capacity_pools_table.c.pool_key ==
            pool_key).with_for_update()).fetchone()
    if row is None:
        raise RuntimeError('Paid-capacity pool disappeared while locked.')
    return row


def _paid_capacity_clock_timestamp(session: orm.Session,
                                   test_now: float | None) -> float:
    """Sample PostgreSQL wall time after the caller holds the pool lock."""
    if test_now is not None:
        # Deterministic PostgreSQL policy tests inject a synthetic DB clock.
        return float(test_now)
    value = session.execute(
        sqlalchemy.select(
            sqlalchemy.extract(
                'epoch', sqlalchemy.func.clock_timestamp()))).scalar_one()
    return float(value)


def get_paid_capacity_pool_states(
    pool_keys: list[str],
    *,
    base_limit: int,
    max_limit: int,
    now: float | None,
    success_ttl_seconds: float,
    failure_cooldown_seconds: float = 10 * 60,
) -> dict[str, dict[str, Any]]:
    """Read advisory shared headroom for exact paid provider pools."""
    pool_keys = list(dict.fromkeys(pool_keys))
    if not pool_keys:
        return {}
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        now = _paid_capacity_clock_timestamp(session, now)
        pool_rows = {
            row.pool_key: row for row in session.execute(
                sqlalchemy.select(paid_capacity_pools_table).where(
                    paid_capacity_pools_table.c.pool_key.in_(pool_keys)))
        }
        valid_counts = {pool_key: 0 for pool_key in pool_keys}
        rows = session.execute(
            sqlalchemy.select(
                paid_capacity_claims_table.c.pool_key,
                paid_capacity_claims_table.c.service_hash,
                services_table.c.hash,
                replicas_table.c.status,
                replicas_table.c.paid_capacity_pool_key,
                replicas_table.c.ordinary_launch_association_id,
            ).select_from(
                paid_capacity_claims_table.outerjoin(
                    services_table, services_table.c.name ==
                    paid_capacity_claims_table.c.service_name).outerjoin(
                        replicas_table,
                        sqlalchemy.and_(
                            replicas_table.c.service_name ==
                            paid_capacity_claims_table.c.service_name,
                            replicas_table.c.replica_id ==
                            paid_capacity_claims_table.c.replica_id))).where(
                                paid_capacity_claims_table.c.pool_key.in_(
                                    pool_keys))).fetchall()
        for (pool_key, claim_hash, current_hash, status, row_pool,
             association_id) in rows:
            if (claim_hash == current_hash and
                (status in _PAID_CAPACITY_UNRESOLVED_STATUSES or
                 association_id is not None) and row_pool == pool_key):
                valid_counts[pool_key] += 1
    result = {}
    for pool_key in pool_keys:
        row = pool_rows.get(pool_key)
        current_limit = base_limit if row is None else row.current_limit
        last_success_at = None if row is None else row.last_success_at
        last_failure_at = None if row is None else row.last_failure_at
        learned_limit, expired = paid_capacity.effective_limit(
            current_limit,
            last_success_at,
            bootstrap_limit=base_limit,
            ceiling_limit=max_limit,
            now=now,
            ttl_seconds=success_ttl_seconds)
        admission = paid_capacity.effective_admission_limit(
            current_limit,
            last_success_at,
            last_failure_at,
            bootstrap_limit=base_limit,
            ceiling_limit=max_limit,
            now=now,
            success_ttl=success_ttl_seconds,
            failure_cooldown=failure_cooldown_seconds)
        active_claims = valid_counts[pool_key]
        result[pool_key] = {
            'current_limit':
                (current_limit if last_failure_at is not None else learned_limit
                ),
            'learned_limit':
                (base_limit if last_failure_at is not None else learned_limit),
            'admission_limit': admission.limit,
            'admission_state': admission.state,
            'cooldown_until': admission.cooldown_until,
            'active_claims': active_claims,
            'legacy_overage': max(0, active_claims - admission.limit),
            'remaining': max(0, admission.limit - active_claims),
            'successes_since_resize':
                (0 if row is None or expired or last_failure_at is not None else
                 int(row.successes_since_resize)),
            'last_success_at': (
                None if expired and last_failure_at is None else last_success_at
            ),
            'last_failure_at': last_failure_at,
        }
    return result


def _replica_has_zero_cost_authority(
        replica_info: 'replica_managers.ReplicaInfo') -> bool:
    """Whether a row carries state forbidden from the paid-claim path."""
    state = vars(replica_info)
    authority_fields = (
        'zero_cost_admission_sequence',
        'zero_cost_materialization_sequence',
        'reserved_fill_pool_key',
        'reserved_fill_service_generation',
        'reserved_fill_physical_cluster_uid',
        'reserved_fill_kubernetes_context',
        'reserved_fill_allocation_generation',
        'reserved_fill_allocation_input_sha256',
        'reserved_fill_allocation_claim_generation',
        'reserved_fill_reconciliation_gate_generation',
        'reserved_fill_reclaim_fleet_bundle_sha256',
        'reserved_fill_reclaim_policy_revision',
        'reserved_fill_reclaim_provider_inventory_sha256',
        'reserved_fill_observation_generation',
        'reserved_fill_observation_sequence',
        'reserved_fill_intent_idempotency_key',
    )
    return bool(
        state.get('is_zero_cost') is True or
        state.get('reserved_fill') is True or
        any(state.get(field) is not None for field in authority_fields))


def try_add_replica_with_paid_capacity_claim(
    service_name: str,
    service_hash: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    pool_key: str,
    priority: int,
    base_limit: int,
    max_limit: int,
    service_limit: int | None = None,
    now: float | None,
    success_ttl_seconds: float,
    failure_cooldown_seconds: float = 10 * 60,
    waiter_ttl_seconds: float,
    expected_controller_owner: tuple[int | None, str | None] | None,
    frontier_key: paid_capacity.FrontierKey | None = None,
    frontier_limit: int | None = None,
    frontier_default_limit: int | None = None,
    frontier_limits_by_key: dict[paid_capacity.FrontierKey, int] | None = None,
    capacity_plan_claim: Mapping[str, Any] | None = None,
) -> str:
    """Atomically persist one replica and its global paid-capacity claim."""
    _validate_replica_row_identity(replica_id, replica_info)
    if _replica_has_zero_cost_authority(replica_info):
        raise ValueError('A zero-cost or reserved-fill replica cannot enter '
                         'the paid-capacity claim path.')
    transaction_replica_info = copy.deepcopy(replica_info)
    engine = _db_manager.get_engine()
    reconcile_waiters = False
    with orm.Session(engine) as session:
        if not _lock_service_owner_in_session(session,
                                              service_name,
                                              service_hash,
                                              expected_controller_owner,
                                              require_launch_allowed=True):
            session.rollback()
            return 'ownership_lost'
        if service_limit is not None and service_limit <= 0:
            raise ValueError('Paid-capacity service limit must be positive.')
        if frontier_limit is not None and frontier_limit <= 0:
            raise ValueError('Paid-capacity frontier must be positive.')
        if frontier_default_limit is not None and frontier_default_limit <= 0:
            raise ValueError('Paid-capacity default frontier must be positive.')
        if frontier_limits_by_key is not None and any(
                limit <= 0 for limit in frontier_limits_by_key.values()):
            raise ValueError(
                'Paid-capacity per-card frontiers must be positive.')
        if (frontier_key is None) != (frontier_limit is None):
            raise ValueError(
                'Paid-capacity frontier key and limit must be set together.')

        identity = (service_name, service_hash, replica_id)
        service_claims, stale_service_claims = (
            _valid_paid_capacity_service_claims_in_session(
                session, service_name, service_hash))
        _delete_paid_capacity_claims_in_session(session, stale_service_claims)
        is_existing_service_claim = any(
            claim_replica_id == replica_id
            for claim_replica_id, _ in service_claims)
        existing_service_pool_key = next(
            (claim_pool_key
             for claim_replica_id, claim_pool_key in service_claims
             if claim_replica_id == replica_id), None)
        if (existing_service_pool_key is not None and
                existing_service_pool_key != pool_key):
            raise ValueError(
                'A paid-capacity replica claim cannot move between exact '
                'provider pools during a recovery re-drive.')
        frontier_owned_pool_keys: set[str] | None = None
        if (service_limit is not None and
                len(service_claims) >= service_limit and
                not is_existing_service_claim):
            # The service row is the only admission lock held here, so
            # deleting waiters across pools cannot form a pool-lock cycle.
            _withdraw_all_paid_capacity_waiters_in_session(
                session, service_name, service_hash)
            session.commit()
            return 'service_saturated'

        if frontier_key is not None:
            assert frontier_limit is not None
            candidate_frontier_key = (
                paid_capacity.frontier_key_from_pool_key(pool_key))
            if (candidate_frontier_key is not None and
                    candidate_frontier_key != frontier_key):
                raise ValueError(
                    'Paid-capacity pool and frontier identities disagree.')
            frontier_owned_pool_keys = {
                claim_pool_key for _, claim_pool_key in service_claims
                if (paid_capacity.frontier_key_from_pool_key(claim_pool_key) in
                    (None, frontier_key))
            }
            if (pool_key not in frontier_owned_pool_keys and
                    len(frontier_owned_pool_keys) >= frontier_limit):
                _withdraw_ineligible_frontier_waiters_in_session(
                    session, service_name, service_hash, service_claims,
                    frontier_default_limit or frontier_limit,
                    frontier_limits_by_key)
                session.commit()
                return 'feedback_pending'
        _ensure_paid_capacity_pool_in_session(session, engine, pool_key,
                                              base_limit, now)
        pool = _paid_capacity_pool_row_for_update(session, pool_key)
        now = _paid_capacity_clock_timestamp(session, now)
        if pool.last_failure_at is None:
            effective_limit, reset = paid_capacity.effective_limit(
                pool.current_limit,
                pool.last_success_at,
                bootstrap_limit=base_limit,
                ceiling_limit=max_limit,
                now=now,
                ttl_seconds=success_ttl_seconds)
            if reset or effective_limit != pool.current_limit:
                session.execute(
                    sqlalchemy.update(paid_capacity_pools_table).where(
                        paid_capacity_pools_table.c.pool_key == pool_key).
                    values(current_limit=effective_limit,
                           successes_since_resize=(0 if reset else
                                                   pool.successes_since_resize),
                           last_success_at=(None
                                            if reset else pool.last_success_at),
                           updated_at=now))
        else:
            admission = paid_capacity.effective_admission_limit(
                pool.current_limit,
                pool.last_success_at,
                pool.last_failure_at,
                bootstrap_limit=base_limit,
                ceiling_limit=max_limit,
                now=now,
                success_ttl=success_ttl_seconds,
                failure_cooldown=failure_cooldown_seconds)
            effective_limit = admission.limit

        valid_claims, stale_claims = _valid_paid_capacity_claims_in_session(
            session, pool_key)
        _delete_paid_capacity_claims_in_session(session, stale_claims)

        is_existing_claim = identity in valid_claims
        locked_service = session.execute(
            sqlalchemy.select(services_table).where(
                services_table.c.name ==
                service_name).with_for_update()).mappings().one()
        if is_existing_claim:
            existing_claim = session.execute(
                sqlalchemy.select(paid_capacity_claims_table).where(
                    paid_capacity_claims_table.c.service_name == service_name,
                    paid_capacity_claims_table.c.service_hash == service_hash,
                    paid_capacity_claims_table.c.replica_id ==
                    replica_id)).mappings().one()
            prospective_claim = dict(existing_claim)
        else:
            prospective_claim = dict(capacity_plan_claim or {})
            prospective_claim.update(service_name=service_name,
                                     service_hash=service_hash,
                                     replica_id=replica_id)
        capacity_admission.validate_paid_claim_in_connection(
            session.connection(),
            locked_service,
            prospective_claim,
            prospective=not is_existing_claim,
            require_planner=not bool(
                transaction_replica_info.cost_rebalance_for_replica_id
                is not None or
                transaction_replica_info.unknown_capacity_replacement or
                transaction_replica_info.system_recovery_launch_intent
                is not None))
        if is_existing_claim:
            existing_replica = session.execute(
                sqlalchemy.select(
                    replicas_table.c.replica_state_version,
                    replicas_table.c.replica_state).where(
                        replicas_table.c.service_name == service_name,
                        replicas_table.c.replica_id ==
                        replica_id).with_for_update()).one_or_none()
            if existing_replica is None:
                session.rollback()
                return 'ownership_lost'
            persisted_replica = _replica_from_state(
                existing_replica.replica_state_version,
                existing_replica.replica_state)
            if _replica_has_zero_cost_authority(persisted_replica):
                raise ValueError('A zero-cost or reserved-fill row cannot be '
                                 'replayed through a paid-capacity claim.')
        if not is_existing_claim:
            session.execute(
                sqlalchemy.delete(paid_capacity_waiters_table).where(
                    paid_capacity_waiters_table.c.pool_key == pool_key,
                    paid_capacity_waiters_table.c.heartbeat_at
                    < now - waiter_ttl_seconds))
            current_service_incarnation = sqlalchemy.exists().where(
                services_table.c.name ==
                paid_capacity_waiters_table.c.service_name,
                services_table.c.hash ==
                paid_capacity_waiters_table.c.service_hash)
            session.execute(
                sqlalchemy.delete(paid_capacity_waiters_table).where(
                    paid_capacity_waiters_table.c.pool_key == pool_key,
                    sqlalchemy.not_(current_service_incarnation)))
            waiter_insert = _upsert_insert_func(engine)(
                paid_capacity_waiters_table).values(pool_key=pool_key,
                                                    service_name=service_name,
                                                    service_hash=service_hash,
                                                    priority=priority,
                                                    first_wait_at=now,
                                                    heartbeat_at=now)
            session.execute(
                waiter_insert.on_conflict_do_update(
                    index_elements=['pool_key', 'service_name', 'service_hash'],
                    set_={
                        'priority': priority,
                        'heartbeat_at': now,
                    }))
            best_waiter = session.execute(
                sqlalchemy.select(
                    paid_capacity_waiters_table.c.service_name,
                    paid_capacity_waiters_table.c.service_hash).where(
                        paid_capacity_waiters_table.c.pool_key ==
                        pool_key).order_by(
                            paid_capacity_waiters_table.c.priority.desc(),
                            paid_capacity_waiters_table.c.first_wait_at,
                            paid_capacity_waiters_table.c.service_name).limit(
                                1)).fetchone()
            if best_waiter is None:
                raise RuntimeError(
                    'Paid-capacity waiter disappeared during admission.')
            if len(valid_claims) >= effective_limit:
                session.commit()
                return 'saturated'
            if (best_waiter.service_name,
                    best_waiter.service_hash) != (service_name, service_hash):
                session.commit()
                return 'higher_priority_waiting'
            if pool.last_failure_at is not None:
                # The row-lock-serialized first post-cooldown claim marks the
                # sole probe. A revision-027 controller may conservatively
                # clobber this marker, but can never clear last_failure_at.
                session.execute(
                    sqlalchemy.update(paid_capacity_pools_table).where(
                        paid_capacity_pools_table.c.pool_key ==
                        pool_key).values(current_limit=1,
                                         successes_since_resize=0,
                                         last_success_at=None,
                                         updated_at=now))

        transaction_replica_info.paid_capacity_pool_key = pool_key
        if is_existing_claim:
            # Re-delivery of an exact durable claim updates the same replica
            # record.  The immutable record identity fence prevents a stale
            # manager incarnation from adopting that claim.
            persisted_infos = _upsert_replica_rows_in_session(
                session,
                engine,
                service_name, [(replica_id, transaction_replica_info)],
                expected_replica_exists=True)
            if persisted_infos is None:
                session.rollback()
                return 'ownership_lost'
        else:
            replica_insert = _upsert_insert_func(engine)(replicas_table).values(
                **_initial_replica_row_values(engine, service_name, replica_id,
                                              transaction_replica_info))
            session.execute(replica_insert)
        claim_values = {
            'service_name': service_name,
            'service_hash': service_hash,
            'replica_id': replica_id,
            'pool_key': pool_key,
            'priority': priority,
            'claimed_at': now,
            **dict(capacity_plan_claim or {}),
        }
        claim_insert = _upsert_insert_func(engine)(
            paid_capacity_claims_table).values(**claim_values)
        session.execute(
            claim_insert.on_conflict_do_update(
                index_elements=['service_name', 'service_hash', 'replica_id'],
                set_={
                    'pool_key': pool_key,
                    'priority': priority,
                }))
        service_claim_count_after = (len(service_claims) +
                                     (0 if is_existing_service_claim else 1))
        if (service_limit is not None and
                service_claim_count_after >= service_limit):
            reconcile_waiters = True
        if frontier_owned_pool_keys is not None:
            assert frontier_key is not None
            assert frontier_limit is not None
            frontier_owned_pool_keys.add(pool_key)
            if len(frontier_owned_pool_keys) >= frontier_limit:
                reconcile_waiters = True
        if not is_existing_claim:
            session.execute(
                sqlalchemy.delete(paid_capacity_waiters_table).where(
                    paid_capacity_waiters_table.c.pool_key == pool_key,
                    paid_capacity_waiters_table.c.service_name == service_name,
                    paid_capacity_waiters_table.c.service_hash == service_hash))
        session.commit()
    # Publish caller-visible paid provenance only after the row and claim are
    # durable. A rejected exact-row replay or failed commit leaves the manager
    # object unchanged, matching the zero-cost sequence publication contract.
    replica_info.paid_capacity_pool_key = pool_key
    if reconcile_waiters:
        # Cross-pool waiter cleanup must not share a transaction with a pool
        # row lock: crossed waiters for two services could otherwise deadlock.
        # The claim is already durable, so cleanup is best effort and the
        # waiter TTL remains the bounded fallback after a process crash.
        try:
            _reconcile_ineligible_paid_capacity_waiters(
                service_name,
                service_hash,
                service_limit=service_limit,
                frontier_limit=frontier_default_limit or frontier_limit,
                frontier_limits_by_key=frontier_limits_by_key,
                expected_controller_owner=expected_controller_owner)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                'Committed paid-capacity claim but failed to withdraw '
                'ineligible waiters; they will expire by TTL. '
                f'Details: {common_utils.format_exception(e)}')
    return 'acquired'


def adopt_paid_capacity_claims(
    service_name: str,
    service_hash: str,
    claims: list[tuple[int, str, int, 'replica_managers.ReplicaInfo']],
    *,
    base_limit: int,
    now: float | None,
    expected_controller_owner: tuple[int | None, str | None] | None,
) -> bool:
    """Attach pre-migration unresolved rows to shared pool claims."""
    if not claims:
        return True
    for replica_id, _, _, replica_info in claims:
        _validate_replica_row_identity(replica_id, replica_info)
        if _replica_has_zero_cost_authority(replica_info):
            raise ValueError('A zero-cost or reserved-fill replica cannot be '
                             'adopted into paid-capacity claims.')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, True)
        if not _lock_service_owner_in_session(session,
                                              service_name,
                                              service_hash,
                                              expected_controller_owner,
                                              require_launch_allowed=False):
            session.rollback()
            return False
        for pool_key in sorted({pool_key for _, pool_key, _, _ in claims}):
            _ensure_paid_capacity_pool_in_session(session, engine, pool_key,
                                                  base_limit, now)
            _paid_capacity_pool_row_for_update(session, pool_key)
        claims_by_replica_id = {
            replica_id: (pool_key, priority)
            for replica_id, pool_key, priority, _ in claims
        }
        merged_infos = _lock_and_merge_existing_replica_rows_in_session(
            session, engine, service_name,
            [(replica_id, replica_info)
             for replica_id, _, _, replica_info in claims])
        if merged_infos is None:
            session.rollback()
            return False
        if any(
                _replica_has_zero_cost_authority(replica_info)
                for _, replica_info in merged_infos):
            raise ValueError('A persisted zero-cost or reserved-fill replica '
                             'cannot be adopted into paid-capacity claims.')
        for replica_id, replica_info in merged_infos:
            pool_key, priority = claims_by_replica_id[replica_id]
            row = session.execute(
                sqlalchemy.select(replicas_table.c.status).where(
                    replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id == replica_id)).fetchone()
            if (row is None or
                    row.status not in _PAID_CAPACITY_UNRESOLVED_STATUSES):
                continue
            replica_info.paid_capacity_pool_key = pool_key
            row_values = _replica_row_values(service_name, replica_id,
                                             replica_info)
            session.execute(
                sqlalchemy.update(replicas_table).where(
                    replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id == replica_id).values(
                        paid_capacity_pool_key=pool_key,
                        replica_state=row_values['replica_state']))
            claim_insert = _upsert_insert_func(engine)(
                paid_capacity_claims_table).values(service_name=service_name,
                                                   service_hash=service_hash,
                                                   replica_id=replica_id,
                                                   pool_key=pool_key,
                                                   priority=priority,
                                                   claimed_at=0)
            session.execute(
                claim_insert.on_conflict_do_update(index_elements=[
                    'service_name', 'service_hash', 'replica_id'
                ],
                                                   set_={
                                                       'pool_key': pool_key,
                                                       'priority': priority,
                                                   }))
        session.commit()
    return True


def add_or_update_replicas_with_paid_capacity_outcomes(
    service_name: str,
    service_hash: str,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    outcomes: dict[int, paid_capacity.LaunchOutcome],
    *,
    base_limit: int,
    max_limit: int,
    now: float | None,
    success_ttl_seconds: float,
    failure_cooldown_seconds: float = 10 * 60,
    expected_controller_owner: tuple[int | None, str | None] | None,
    applied_outcome_pool_keys: set[str] | None = None,
) -> bool:
    """Persist a completed launch wave and release claims atomically."""
    replica_ids = {replica_id for replica_id, _ in replica_infos}
    if set(outcomes) - replica_ids:
        raise ValueError('Paid-capacity outcomes must identify updated '
                         'replica rows.')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, True)
        _prelock_zero_cost_protocol_for_replica_write(
            session, engine, replica_infos, expected_replica_exists=True)
        if not _lock_service_owner_in_session(session,
                                              service_name,
                                              service_hash,
                                              expected_controller_owner,
                                              require_launch_allowed=False):
            session.rollback()
            return False
        persisted_infos = _upsert_replica_rows_in_session(
            session,
            engine,
            service_name,
            replica_infos,
            expected_replica_exists=True)
        if persisted_infos is None:
            session.rollback()
            return False
        if not outcomes:
            session.commit()
            _publish_committed_zero_cost_sequences(replica_infos,
                                                   persisted_infos)
            return True
        claim_rows = session.execute(
            sqlalchemy.select(
                paid_capacity_claims_table.c.replica_id,
                paid_capacity_claims_table.c.pool_key,
                paid_capacity_claims_table.c.claimed_at).where(
                    paid_capacity_claims_table.c.service_name == service_name,
                    paid_capacity_claims_table.c.service_hash == service_hash,
                    paid_capacity_claims_table.c.replica_id.in_(
                        list(outcomes)))).fetchall()
        outcomes_by_pool: dict[
            str, list[tuple[paid_capacity.LaunchOutcome,
                            float]]] = collections.defaultdict(list)
        identities = []
        for replica_id, pool_key, claimed_at in claim_rows:
            outcomes_by_pool[pool_key].append(
                (outcomes[replica_id], claimed_at))
            identities.append((service_name, service_hash, replica_id))
        for pool_key in sorted(outcomes_by_pool):
            _paid_capacity_pool_row_for_update(session, pool_key)
        now = _paid_capacity_clock_timestamp(session, now)
        _delete_paid_capacity_claims_in_session(session, identities)
        for pool_key, pool_outcomes in outcomes_by_pool.items():
            pool = session.execute(
                sqlalchemy.select(paid_capacity_pools_table).where(
                    paid_capacity_pools_table.c.pool_key ==
                    pool_key)).fetchone()
            if pool is None:
                raise RuntimeError('Paid-capacity pool disappeared.')
            capacity_failed = any(
                outcome in (paid_capacity.LaunchOutcome.CAPACITY_FAILURE,
                            paid_capacity.LaunchOutcome.QUOTA_FAILURE)
                for outcome, _ in pool_outcomes)
            if capacity_failed:
                session.execute(
                    sqlalchemy.update(paid_capacity_pools_table).where(
                        paid_capacity_pools_table.c.pool_key ==
                        pool_key).values(current_limit=base_limit,
                                         successes_since_resize=0,
                                         last_success_at=None,
                                         last_failure_at=now,
                                         updated_at=now))
                continue
            if pool.last_failure_at is not None:
                probe_succeeded = (pool.current_limit == 1 and any(
                    outcome == paid_capacity.LaunchOutcome.SUCCESS and
                    claimed_at >= (pool.last_failure_at +
                                   failure_cooldown_seconds)
                    for outcome, claimed_at in pool_outcomes))
                if not probe_succeeded:
                    continue
                ramp_update = paid_capacity.record_outcomes(
                    base_limit,
                    0,
                    None, [paid_capacity.LaunchOutcome.SUCCESS],
                    bootstrap_limit=base_limit,
                    ceiling_limit=max_limit,
                    now=now,
                    ttl_seconds=success_ttl_seconds)
                session.execute(
                    sqlalchemy.update(paid_capacity_pools_table).where(
                        paid_capacity_pools_table.c.pool_key ==
                        pool_key).values(
                            current_limit=ramp_update.current_limit,
                            successes_since_resize=(
                                ramp_update.successes_since_resize),
                            last_success_at=now,
                            last_failure_at=None,
                            updated_at=now))
                continue
            evidence = [
                outcome for outcome, _ in pool_outcomes
                if outcome == paid_capacity.LaunchOutcome.SUCCESS
            ]
            if not evidence:
                continue
            ramp_update = paid_capacity.record_outcomes(
                pool.current_limit,
                pool.successes_since_resize,
                pool.last_success_at,
                evidence,
                bootstrap_limit=base_limit,
                ceiling_limit=max_limit,
                now=now,
                ttl_seconds=success_ttl_seconds)
            session.execute(
                sqlalchemy.update(paid_capacity_pools_table).where(
                    paid_capacity_pools_table.c.pool_key == pool_key).values(
                        current_limit=ramp_update.current_limit,
                        successes_since_resize=(
                            ramp_update.successes_since_resize),
                        last_success_at=now,
                        updated_at=now))
        session.commit()
        _publish_committed_zero_cost_sequences(replica_infos, persisted_infos)
        if applied_outcome_pool_keys is not None:
            # Expose only claim-backed outcomes after their transaction is
            # durable. Callers use this as the authorization boundary for an
            # immediate replacement tick.
            applied_outcome_pool_keys.update(outcomes_by_pool)
    return True


def replica_info_has_binding_excluded_profile(
        replica_info: 'replica_managers.ReplicaInfo') -> bool:
    """Whether this row can authorize the retained special launch contract."""
    # Use the versioned object's explicit storage fields instead of a runtime
    # class-symbol check: tests and embedding processes may replace the public
    # ``ReplicaInfo`` alias, while these exact markers are the durable
    # authorization contract. Missing or dynamically synthesized attributes
    # must remain ordinary and fail closed.
    fields = vars(replica_info)
    return bool(
        fields.get('reserved_fill') is True or
        fields.get('is_zero_cost') is True or
        fields.get('unknown_capacity_replacement') is True or
        type(fields.get('cost_rebalance_for_replica_id')) is int or
        fields.get('system_recovery_launch_intent') is not None or
        fields.get('system_recovery') is not None or
        fields.get('system_recovery_quarantine') is not None)


def add_or_update_replica(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    *,
    expected_replica_exists: bool = False,
    guard_launch_exclusion: bool = False,
) -> bool:
    """Persist one replica, optionally requiring its row to already exist.

    ``expected_replica_exists`` is the update-only bookkeeping path.  A
    missing row rejects the mutation instead of falling through to an insert.
    Callers admitting a new replica must leave it false explicitly; that path
    is INSERT-only and surfaces a primary-key conflict.
    """
    _reject_generic_reserved_fill_insert(
        [(replica_id, replica_info)],
        expected_replica_exists=expected_replica_exists)
    with _replica_launch_authority_write_session(
            service_name,
            invalidates_launch_authority=guard_launch_exclusion) as (engine,
                                                                     session):
        _begin_immediate_if_sqlite(
            session, engine, expected_service_hash is not None or
            expected_lifecycle_epoch is not None or
            expected_controller_owner is not None or expected_replica_exists)
        _prelock_zero_cost_protocol_for_replica_write(
            session,
            engine, [(replica_id, replica_info)],
            expected_replica_exists=expected_replica_exists)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        if (expected_service_hash is not None or
                expected_controller_owner is not None):
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
                    services_table.c.controller_pid,
                    services_table.c.controller_ip).where(
                        services_table.c.name ==
                        service_name).with_for_update()).fetchone()
            if (owner is None or (expected_service_hash is not None and
                                  owner[0] != expected_service_hash) or
                (expected_lifecycle_epoch is not None and
                 owner[1] != expected_lifecycle_epoch) or
                (expected_controller_owner is not None and
                 (owner[2], owner[3]) != expected_controller_owner)):
                session.rollback()
                return False
        caller_infos = [(replica_id, replica_info)]
        persisted_infos = _upsert_replica_rows_in_session(
            session,
            engine,
            service_name,
            caller_infos,
            expected_replica_exists=expected_replica_exists)
        if persisted_infos is None:
            session.rollback()
            return False
        session.commit()
        _publish_committed_zero_cost_sequences(caller_infos, persisted_infos)
    return True


def _transition_paid_retirement(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    action: str,
    authority: 'paid_retirement.FreshZeroAuthority | None' = None,
    positive_demand_generation: int | None = None,
    requires_idle_proof: bool = True,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
) -> dict[str, Any] | bool:
    """Atomically pair paid-retirement authority with replica off-route state."""
    if action not in ('admit', 'commit', 'cancel'):
        raise ValueError(f'Unsupported paid-retirement action: {action!r}.')
    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RuntimeError('Paid retirement requires PostgreSQL.')
        owner = session.execute(
            sqlalchemy.select(services_table.c.hash,
                              services_table.c.controller_pid,
                              services_table.c.controller_ip).where(
                                  services_table.c.name ==
                                  service_name).with_for_update()).fetchone()
        if (owner is None or owner[0] != expected_service_hash or
            (owner[1], owner[2]) != expected_controller_owner):
            session.rollback()
            return False
        locked_record_ids = _lock_replica_record_ids_in_session(
            session, engine, service_name, [replica_id])
        if (locked_record_ids is None or locked_record_ids.get(replica_id)
                != replica_info.replica_record_id):
            session.rollback()
            return False
        try:
            if action == 'admit':
                assert authority is not None
                result: dict[str, Any] | bool = dict(
                    paid_retirement.admit_in_session(
                        session, service_name, replica_id,
                        replica_info.replica_record_id, replica_info.version,
                        requires_idle_proof, authority,
                        expected_controller_owner))
            elif action == 'commit':
                assert authority is not None
                result = paid_retirement.commit_in_session(
                    session, service_name, replica_id,
                    replica_info.replica_record_id, authority,
                    expected_controller_owner)
                if result is not True:
                    session.rollback()
                    return False
            else:
                assert positive_demand_generation is not None
                result = paid_retirement.cancel_in_session(
                    session, service_name, replica_id,
                    replica_info.replica_record_id, positive_demand_generation,
                    expected_service_hash, expected_controller_owner)
                if result is not True:
                    session.rollback()
                    return False
        except paid_retirement.PaidRetirementError:
            session.rollback()
            return False
        persisted_infos = _upsert_replica_rows_in_session(
            session,
            engine,
            service_name, [(replica_id, replica_info)],
            expected_replica_exists=True)
        if persisted_infos is None:
            session.rollback()
            return False
        session.commit()
    return result


def admit_paid_retirement(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    authority: 'paid_retirement.FreshZeroAuthority',
    *,
    requires_idle_proof: bool,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
) -> dict[str, Any] | None:
    """Persist a fresh-zero intent and its off-route replica state."""
    result = _transition_paid_retirement(
        service_name,
        replica_id,
        replica_info,
        action='admit',
        authority=authority,
        requires_idle_proof=requires_idle_proof,
        expected_service_hash=expected_service_hash,
        expected_controller_owner=expected_controller_owner)
    return result if isinstance(result, dict) else None


def commit_paid_retirement(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    authority: 'paid_retirement.FreshZeroAuthority',
    *,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
) -> bool:
    """Commit teardown after exact idle proof without a generation gap."""
    return _transition_paid_retirement(
        service_name,
        replica_id,
        replica_info,
        action='commit',
        authority=authority,
        expected_service_hash=expected_service_hash,
        expected_controller_owner=expected_controller_owner) is True


def _logical_retirement_receipt_watermark(
        rows: list[Mapping[str, Any]]) -> tuple[tuple[str, int, str], ...]:
    """Return the exact canonical receipt tuple used by a reconcile token."""
    return tuple((str(row['reporter_session_id']), int(row['sequence']),
                  str(row['payload_sha256'])) for row in rows)


def _logical_retirement_reports_are_current(
    rows: list[Mapping[str, Any]],
    service: Mapping[str, Any],
    authority: 'demand_state.DurableReconcileAuthority',
    now: datetime.datetime,
) -> bool:
    """Revalidate exact LB and occupancy evidence at the database clock."""
    if (not rows or _logical_retirement_receipt_watermark(rows)
            != authority.receipt_watermark or
            not demand_state.reports_match_current_lb_authority(rows, service)):
        return False
    complete = all(row['complete'] is True and row['protocol_version'] == 2
                   for row in rows)
    allow_zero = bool(authority.fresh_aggregate_zero and
                      service['route_projection_protocol_version'] == 2 and
                      demand_state.reports_prove_fresh_aggregate_zero(rows))
    if not (complete or allow_zero):
        return False

    ledger = lb_ha.LbSessionLedger(constants.LB_DEMAND_REPORT_TTL_SECONDS,
                                   constants.LB_OCCUPANCY_PROBE_MAX_AGE_SECONDS)
    stream_owners: set[str] = set()
    now_epoch = now.timestamp()
    for row in rows:
        payload = row['payload']
        if (not isinstance(payload, Mapping) or
                payload.get('protocol_version') != 2 or
                payload.get('routing_version') != authority.service_version or
                payload.get('route_projection_generation')
                != authority.route_generation or
                payload.get('route_projection_sha256') != authority.route_sha256
                or payload.get('route_source_epoch')
                != authority.route_source_epoch):
            return False
        try:
            role = lb_ha.LbRole(payload.get('applied_role'))
            sample_ages = payload['occupancy_sample_age_seconds']
            if not isinstance(sample_ages, Mapping):
                return False
            ledger_payload = dict(payload)
            elapsed = max(0.0, now_epoch - row['received_at'].timestamp())
            ledger_payload['occupancy_sample_age_seconds'] = {
                str(url): float(age) + elapsed
                for url, age in sample_ages.items()
            }
            session_id = str(row['reporter_session_id'])
            slot = lb_ha.parse_slot(row['lb_slot']) or lb_ha.LbSlot.A
            if not ledger.update(session_id,
                                 slot,
                                 role,
                                 int(payload['applied_generation']),
                                 ledger_payload,
                                 now=now_epoch):
                return False
        except (KeyError, TypeError, ValueError, OverflowError):
            return False
        if role in (lb_ha.LbRole.ACTIVE, lb_ha.LbRole.DRAINING):
            stream_owners.add(session_id)
    if not stream_owners:
        return False
    aggregate = ledger.aggregate(stream_owners, now=now_epoch)
    return bool(aggregate.complete and tuple(aggregate.occupancy_sampled_urls)
                == authority.occupancy_sampled_urls)


def _logical_retirement_precommit_matches(
    current: 'replica_managers.ReplicaInfo',
    expected: 'replica_managers.ReplicaInfo',
    authority: 'demand_state.DurableReconcileAuthority',
    expected_logical_controller_epoch: str,
) -> bool:
    """Validate the exact, still-reversible replica state before teardown."""
    current_status = current.status_property
    expected_status = expected.status_property
    fields = (
        'sky_launch_status',
        'sky_down_status',
        'is_scale_down',
        'preempted',
        'purged',
        'wait_for_idle_before_termination',
        'logical_retirement_version',
        'logical_retirement_controller_epoch',
        'logical_retirement_generation',
        'logical_retirement_target_capacity',
        'logical_retirement_confirmed_generation',
        'logical_retirement_bounded_deadline',
        'logical_retirement_committed',
    )
    if (current.replica_id != expected.replica_id or
            current.replica_record_id != expected.replica_record_id or
            current.version != expected.version or any(
                getattr(current_status, field) != getattr(
                    expected_status, field) for field in fields)):
        return False
    selection_generation = current_status.logical_retirement_generation
    selection_target = current_status.logical_retirement_target_capacity
    confirmation = current_status.logical_retirement_confirmed_generation
    retirement_version = current_status.logical_retirement_version
    return bool(
        current_status.sky_launch_status == common_utils.ProcessStatus.SUCCEEDED
        and current_status.sky_down_status
        == common_utils.ProcessStatus.SCHEDULED and
        current_status.is_scale_down is True and
        current_status.preempted is False and current_status.purged is False and
        current_status.wait_for_idle_before_termination is False and
        current_status.logical_retirement_committed is False and
        current_status.logical_retirement_controller_epoch
        == expected_logical_controller_epoch and
        type(retirement_version) is int and  # pylint: disable=unidiomatic-typecheck
        retirement_version == authority.service_version and type(
            current.version) is int and current.version <= retirement_version
        and type(selection_generation) is int and selection_generation >= 0
        and selection_generation < authority.demand_feed_generation
        and type(selection_target) is int and selection_target >= 0 and
        (confirmation is None or (type(confirmation) is int and  # pylint: disable=unidiomatic-typecheck
                                  selection_generation <= confirmation <=
                                  authority.demand_feed_generation))
        and type(current_status.logical_retirement_bounded_deadline) is bool)


def commit_logical_retirement(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    authority: 'demand_state.DurableReconcileAuthority',
    *,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_logical_controller_epoch: str,
) -> LogicalRetirementCommitResult:
    """Atomically authorize logical teardown against current durable demand.

    The global zero-cost protocol row comes first, followed by the lifecycle
    fence and service row, preserving the established
    protocol -> lifecycle -> service -> replica lock order shared by
    admissions, materializations, lifecycle takeover, and fill.  The service
    row remains the first service-local SQL mutex shared with
    ``demand_state.ingest_report``.  A report that commits first invalidates
    the token; a report blocked behind this transaction is ordered after the
    retirement.  Only ``COMMITTED`` authorizes the caller to start a provider
    worker.  A commit-call failure is deliberately ``AMBIGUOUS`` and requires
    fresh row readback before any worker can be reconstructed.
    """
    try:
        authority_valid_until = authority.valid_until
        if (not isinstance(authority_valid_until, datetime.datetime) or
                authority_valid_until.tzinfo is None or
                authority_valid_until.utcoffset() is None or
                authority.service_name != service_name or
                authority.service_hash != expected_service_hash or
                authority.controller_pid != expected_controller_owner[0] or
                authority.controller_ip != expected_controller_owner[1] or
                not isinstance(expected_logical_controller_epoch, str) or
                not expected_logical_controller_epoch):
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)
    except (AttributeError, TypeError, ValueError):
        return LogicalRetirementCommitResult(
            LogicalRetirementCommitState.REJECTED)

    caller_infos = [(replica_id, replica_info)]
    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RuntimeError('Logical retirement requires PostgreSQL.')
        _prelock_zero_cost_protocol_for_replica_write(
            session, engine, caller_infos, expected_replica_exists=True)
        # Lifecycle acquisition advances the durable fence before updating
        # the service row. Lock and validate that fence before taking the
        # service mutex too; taking it later through the generic replica
        # upsert would invert lifecycle takeover's lifecycle -> service order.
        if not _lifecycle_epoch_matches_in_session(
                session, service_name, authority.service_lifecycle_epoch):
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)
        service = session.execute(
            sqlalchemy.select(services_table).where(
                services_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
        if service is None:
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)
        try:
            controller_incarnation = str(service['controller_incarnation'])
            service_matches = bool(
                service['hash'] == expected_service_hash and
                service['pool'] == 0 and service['lifecycle_epoch']
                == authority.service_lifecycle_epoch and
                service['current_version'] == authority.service_version and
                controller_incarnation == authority.controller_incarnation and
                service['controller_owner_epoch']
                == authority.controller_owner_epoch and
                (service['controller_pid'],
                 service['controller_ip']) == expected_controller_owner and
                service['demand_source_mode'] == 'DURABLE_FEED' and
                service['demand_source_epoch'] == authority.demand_source_epoch
                and service['demand_authority_capable'] is True and
                service['demand_authority_controller_incarnation']
                == service['controller_incarnation'] and
                service['demand_authority_protocol_version'] == 1 and
                service['route_source_mode'] == 'DURABLE_PROJECTED' and
                service['route_source_epoch'] == authority.route_source_epoch
                and service['route_projection_capable'] is True and
                service['route_projection_controller_incarnation']
                == service['controller_incarnation'] and
                service['route_projection_protocol_version'] in (1, 2) and
                (service['lb_ha_enabled'] == 1) == authority.lb_ha_enabled and
                service['lb_active_slot'] == authority.lb_active_slot and
                service['lb_cutover_generation']
                == authority.lb_cutover_generation)
        except (AttributeError, KeyError, TypeError, ValueError):
            service_matches = False
        now = session.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        if not service_matches or authority_valid_until <= now:
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)

        generations = demand_state_schema.serve_demand_feed_generations_table
        reports = demand_state_schema.serve_lb_demand_reports_table
        current_generation = session.execute(
            sqlalchemy.select(generations.c.generation).where(
                generations.c.service_name == service_name,
                generations.c.service_hash ==
                expected_service_hash).with_for_update()).scalar_one_or_none()
        fresh_reports = session.execute(
            sqlalchemy.select(reports).where(
                reports.c.service_name == service_name,
                reports.c.service_hash == expected_service_hash,
                reports.c.valid_until
                > now).order_by(reports.c.reporter_session_id).with_for_update(
                )).mappings().all()
        if (current_generation != authority.demand_feed_generation or
                not _logical_retirement_reports_are_current(
                    fresh_reports, service, authority, now)):
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)

        route_heads = route_projection_schema.serve_route_heads_table
        routes = route_projection_schema.serve_route_snapshots_table
        route_head = session.execute(
            sqlalchemy.select(route_heads).where(
                route_heads.c.service_name ==
                service_name).with_for_update()).mappings().one_or_none()
        route = session.execute(
            sqlalchemy.select(routes).where(
                routes.c.service_name == service_name, routes.c.generation ==
                authority.route_generation)).mappings().one_or_none()
        if (route_head is None or route is None or
                route_head['generation'] != authority.route_generation or
                route_head['valid_until'] <= now or
                route['content_sha256'] != authority.route_sha256 or
                route['service_hash'] != expected_service_hash or
                route['service_lifecycle_epoch']
                != authority.service_lifecycle_epoch or
                route['service_version'] != authority.service_version or
                str(route['controller_incarnation'])
                != authority.controller_incarnation or
                route['controller_owner_epoch']
                != authority.controller_owner_epoch or
                route['protocol_version'] != 1 or
                route['producer_protocol_version']
                != service['route_projection_protocol_version']):
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)
        try:
            route_projection.RouteProjectionRepository.validate_snapshot_row(
                route)
        except route_projection.RouteProjectionError:
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)
        if not route_projection.snapshot_owner_matches(route, service):
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)

        row = session.execute(
            sqlalchemy.select(replicas_table.c.replica_state_version,
                              replicas_table.c.replica_state).where(
                                  replicas_table.c.service_name == service_name,
                                  replicas_table.c.replica_id ==
                                  replica_id).with_for_update()).one_or_none()
        if row is None:
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)
        current = _replica_from_state(row.replica_state_version,
                                      row.replica_state)
        if not _logical_retirement_precommit_matches(
                current, replica_info, authority,
                expected_logical_controller_epoch):
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)

        current_status = current.status_property
        current_status.logical_retirement_confirmed_generation = (
            authority.demand_feed_generation)
        current_status.logical_retirement_committed = True
        current_status.sky_down_status = common_utils.ProcessStatus.RUNNING
        current_status.wait_for_idle_before_termination = False
        route_projection.revoke_replica_lease_in_session(
            session, service_name, replica_id, current.replica_record_id,
            'logical_retirement_committed')
        persisted_infos = _upsert_replica_rows_in_session(
            session,
            engine,
            service_name, [(replica_id, current)],
            expected_replica_exists=True)
        if persisted_infos is None:
            session.rollback()
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.REJECTED)
        try:
            session.commit()
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Logical-retirement commit outcome is ambiguous for service '
                f'{service_name!r}, replica {replica_id}: '
                f'{common_utils.format_exception(error)}')
            return LogicalRetirementCommitResult(
                LogicalRetirementCommitState.AMBIGUOUS)
    _publish_committed_zero_cost_sequences(caller_infos, persisted_infos)
    return LogicalRetirementCommitResult(LogicalRetirementCommitState.COMMITTED,
                                         persisted_infos[0][1])


def cancel_paid_retirement(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    positive_demand_generation: int,
    *,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
) -> bool:
    """Cancel an uncommitted retirement only under newer positive demand."""
    return _transition_paid_retirement(
        service_name,
        replica_id,
        replica_info,
        action='cancel',
        positive_demand_generation=positive_demand_generation,
        expected_service_hash=expected_service_hash,
        expected_controller_owner=expected_controller_owner) is True


def add_or_update_replica_with_launch_shadow(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    new_sample: 'resource_action_state.NewShadowSample',
    *,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> 'resource_action_state.ShadowSampleRecord':
    """Atomically admit one initial replica intent and its launch shadow.

    This PostgreSQL-only primitive is deliberately separate from generic
    replica persistence: only it may initialize action-owned identity/link
    columns, while later status updates continue to preserve those columns.
    """
    _reject_generic_reserved_fill_insert([(replica_id, replica_info)],
                                         expected_replica_exists=False)
    engine = _db_manager.get_engine()
    store = resource_action_state.PostgresServeResourceActionStateStore(engine)
    caller_infos = [(replica_id, replica_info)]
    transaction_replica_info = copy.deepcopy(replica_info)
    transaction_infos = [(replica_id, transaction_replica_info)]
    with orm.Session(engine) as session, session.begin():
        if transaction_replica_info.is_zero_cost is True:
            # Protocol is the first SQL mutex. Since every sequenced zero-cost
            # insert shares it, this pre-service existence read is sufficient
            # to distinguish a new row from an exact lost-ack replay; the
            # resource-action store performs the authoritative service/replica
            # lock and replay validation below.
            _lock_zero_cost_protocol_sequence_for_update(session)
            row_exists = session.execute(
                sqlalchemy.select(replicas_table.c.replica_id).where(
                    replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id == replica_id)).first()
            if row_exists is None:
                admissions = (
                    _stamp_new_zero_cost_replica_admissions_in_session(
                        session, engine, transaction_infos))
                _apply_zero_cost_sequence_assignments(transaction_infos,
                                                      admissions=admissions)
                materializations = (
                    _stamp_zero_cost_replica_materializations_in_session(
                        session, transaction_infos))
                _apply_zero_cost_sequence_assignments(
                    transaction_infos, materializations=materializations)
        replica_values = _replica_row_values(service_name, replica_id,
                                             transaction_replica_info)
        result = store.admit_launch_replica_in_session(
            session, new_sample, replica_values, expected_controller_owner,
            expected_lifecycle_epoch)
        # The store accepts an existing row only as an exact lost-ack replay.
        # Reload that locked row before publication: the replaying caller may
        # predate database-assigned sequence identities, while publishing its
        # unstamped input copy would incorrectly clear the caller's view after
        # a successful no-op transaction.
        persisted_row = session.execute(
            sqlalchemy.select(
                replicas_table.c.replica_state_version,
                replicas_table.c.replica_state).where(
                    replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id == replica_id)).one_or_none()
        if persisted_row is None:
            raise RuntimeError('Launch-shadow admission lost its exact '
                               'persisted replica row.')
        persisted_info = _replica_from_state(
            persisted_row.replica_state_version, persisted_row.replica_state)
        for field in ('zero_cost_admission_sequence',
                      'zero_cost_materialization_sequence'):
            caller_value = getattr(transaction_replica_info, field)
            persisted_value = getattr(persisted_info, field)
            if caller_value is not None and caller_value != persisted_value:
                raise ValueError(f'A {field.replace("_", " ")} is immutable.')
        persisted_infos = [(replica_id, persisted_info)]
    _publish_committed_zero_cost_sequences(caller_infos, persisted_infos)
    return result


def add_or_update_replicas(
    service_name: str,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    *,
    expected_replica_exists: bool = False,
    validate_fence_on_empty: bool = False,
    guard_launch_exclusion: bool = False,
) -> bool:
    """Persist a batch of replicas in one transaction.

    The probe round persists per-replica bookkeeping for every probed
    replica; issuing those as individual updates serializes one DB
    round-trip per replica under the replica-manager lock (at ~1k replicas
    on Postgres that alone exceeds the probe period). The expected-existing
    path locks every requested row before its first write, aborts the whole
    batch if one is absent or has a different record identity, and executes
    UPDATE only. Explicit initial admission is INSERT-only. Empty batches are
    normally no-ops; callers that transfer an in-memory authority-sensitive
    side effect may request an owner-fence transaction before committing it.
    """
    _reject_generic_reserved_fill_insert(
        replica_infos, expected_replica_exists=expected_replica_exists)
    if not replica_infos:
        if not validate_fence_on_empty:
            return True
        if (not isinstance(expected_service_hash, str) or
                not expected_service_hash or
                not isinstance(expected_controller_owner, tuple) or
                len(expected_controller_owner) != 2):
            return False
        expected_pid, expected_ip = expected_controller_owner
        if (isinstance(expected_pid, bool) or
                not isinstance(expected_pid, int) or expected_pid < 1 or
                not isinstance(expected_ip, str) or not expected_ip):
            return False
    with _replica_launch_authority_write_session(
            service_name,
            invalidates_launch_authority=guard_launch_exclusion) as (engine,
                                                                     session):
        _begin_immediate_if_sqlite(
            session, engine, expected_service_hash is not None or
            expected_lifecycle_epoch is not None or
            expected_controller_owner is not None or expected_replica_exists)
        _prelock_zero_cost_protocol_for_replica_write(
            session,
            engine,
            replica_infos,
            expected_replica_exists=expected_replica_exists)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        if (expected_service_hash is not None or
                expected_controller_owner is not None):
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
                    services_table.c.controller_pid,
                    services_table.c.controller_ip).where(
                        services_table.c.name ==
                        service_name).with_for_update()).fetchone()
            if (owner is None or (expected_service_hash is not None and
                                  owner[0] != expected_service_hash) or
                (expected_lifecycle_epoch is not None and
                 owner[1] != expected_lifecycle_epoch) or
                (expected_controller_owner is not None and
                 (owner[2], owner[3]) != expected_controller_owner)):
                session.rollback()
                return False
        # Older SQLite builds cap SQLITE_MAX_VARIABLE_NUMBER at 999, while
        # PostgreSQL can preserve the prior 300-row batches. The helper derives
        # SQLite's safe chunk from the live table width.
        persisted_infos = _upsert_replica_rows_in_session(
            session,
            engine,
            service_name,
            replica_infos,
            expected_replica_exists=expected_replica_exists)
        if persisted_infos is None:
            session.rollback()
            return False
        session.commit()
        _publish_committed_zero_cost_sequences(replica_infos, persisted_infos)
    return True


def _lock_replica_record_ids_in_session(
    session: orm.Session,
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    replica_ids: list[int],
) -> dict[int, str] | None:
    """Lock sorted replica rows and decode their immutable record identities."""
    record_ids: dict[int, str] = {}
    is_postgres = (
        engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    for start in range(0, len(replica_ids), _REPLICA_DELETE_CHUNK_SIZE):
        chunk = replica_ids[start:start + _REPLICA_DELETE_CHUNK_SIZE]
        stmt = sqlalchemy.select(
            replicas_table.c.replica_id,
            replicas_table.c.replica_state_version,
            replicas_table.c.replica_state,
        ).where(replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id.in_(chunk)).order_by(
                    replicas_table.c.replica_id)
        if is_postgres:
            stmt = stmt.with_for_update()
        rows = session.execute(stmt).fetchall()
        try:
            for row in rows:
                replica_id = int(row.replica_id)
                info = _replica_from_state(row.replica_state_version,
                                           row.replica_state)
                record_id = info.replica_record_id
                if (info.replica_id != replica_id or
                        not isinstance(record_id, str)):
                    return None
                record_ids[replica_id] = record_id
        except Exception:  # pylint: disable=broad-except
            # A terminal delete cannot guess through unreadable identity state.
            return None
    return record_ids


def _validate_expected_replica_record_id(record_id: Any) -> None:
    """Reject malformed delete-fence identities before opening a transaction."""
    if not isinstance(record_id, str):
        raise ValueError('Expected replica record IDs must be strings.')
    try:
        parsed = uuid.UUID(record_id)
    except (ValueError, AttributeError) as exc:
        raise ValueError(
            'Expected replica record IDs must be canonical UUIDs.') from exc
    if str(parsed) != record_id:
        raise ValueError('Expected replica record IDs must be canonical UUIDs.')


def remove_replica(
    service_name: str,
    replica_id: int,
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    *,
    expected_replica_record_id: str,
) -> bool:
    """Remove one replica under service and immutable-record fences."""
    _validate_expected_replica_record_id(expected_replica_record_id)
    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        _begin_immediate_if_sqlite(
            session, engine, expected_service_hash is not None or
            expected_lifecycle_epoch is not None or
            expected_controller_owner is not None or
            expected_replica_record_id is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        _lock_service_row_if_present_for_replica_write(session, service_name)
        predicates = [
            replicas_table.c.service_name == service_name,
            replicas_table.c.replica_id == replica_id,
        ]
        if (expected_service_hash is not None or
                expected_controller_owner is not None):
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
                    services_table.c.controller_pid,
                    services_table.c.controller_ip).where(
                        services_table.c.name ==
                        service_name).with_for_update()).fetchone()
            if (owner is None or (expected_service_hash is not None and
                                  owner[0] != expected_service_hash) or
                (expected_lifecycle_epoch is not None and
                 owner[1] != expected_lifecycle_epoch) or
                (expected_controller_owner is not None and
                 (owner[2], owner[3]) != expected_controller_owner)):
                session.rollback()
                return False
        locked_record_ids = _lock_replica_record_ids_in_session(
            session, engine, service_name, [replica_id])
        if locked_record_ids is None:
            session.rollback()
            return False
        current_record_id = locked_record_ids.get(replica_id)
        if (current_record_id is not None and
                current_record_id != expected_replica_record_id):
            session.rollback()
            return False
        session.execute(
            sqlalchemy.delete(paid_capacity_claims_table).where(
                paid_capacity_claims_table.c.service_name == service_name,
                paid_capacity_claims_table.c.replica_id == replica_id))
        if current_record_id is not None:
            if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
                route_projection.revoke_replica_lease_in_session(
                    session, service_name, replica_id, current_record_id,
                    'replica_teardown')
                paid_retirement.delete_in_session(session, service_name,
                                                  [replica_id])
            result = session.execute(
                sqlalchemy.delete(replicas_table).where(*predicates))
            if result.rowcount != 1:
                session.rollback()
                return False
        session.commit()
    # Once exact ownership is proven, an already-absent child is the desired
    # idempotent cleanup state, not evidence of ownership loss.
    return True


def remove_replicas(
    service_name: str,
    replica_ids: list[int],
    expected_service_hash: str,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    *,
    expected_replica_record_ids: dict[int, str],
) -> bool:
    """Atomically remove replicas fenced to one service incarnation.

    Large failed-launch inventories can contain thousands of replicas whose
    clusters were never created. Removing those rows one transaction at a
    time makes teardown scale with retained history. This helper proves the
    service hash, lifecycle epoch, and optional controller owner once, then
    deletes the requested children in bounded chunks within that transaction.

    An already-absent child is the desired idempotent state. No history table
    is touched; aggregate Serve history has its own retention policy.
    """
    if not expected_service_hash:
        return False
    if len(set(replica_ids)) != len(replica_ids):
        raise ValueError('replica_ids must not contain duplicates.')
    replica_ids = sorted(replica_ids)
    if set(expected_replica_record_ids) != set(replica_ids):
        raise ValueError(
            'expected_replica_record_ids must cover every replica.')
    for record_id in expected_replica_record_ids.values():
        _validate_expected_replica_record_id(record_id)
    if not replica_ids:
        return True
    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        _begin_immediate_if_sqlite(session, engine, True)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        owner = session.execute(
            sqlalchemy.select(services_table.c.hash,
                              services_table.c.lifecycle_epoch,
                              services_table.c.controller_pid,
                              services_table.c.controller_ip).where(
                                  services_table.c.name ==
                                  service_name).with_for_update()).fetchone()
        if (owner is None or owner[0] != expected_service_hash or
            (expected_lifecycle_epoch is not None and
             owner[1] != expected_lifecycle_epoch) or
            (expected_controller_owner is not None and
             (owner[2], owner[3]) != expected_controller_owner)):
            session.rollback()
            return False
        locked_record_ids = _lock_replica_record_ids_in_session(
            session, engine, service_name, replica_ids)
        if locked_record_ids is None:
            session.rollback()
            return False
        if any(expected_replica_record_ids[replica_id] != record_id
               for replica_id, record_id in locked_record_ids.items()):
            session.rollback()
            return False
        # Rows already absent are idempotently complete. Do not include them in
        # the DELETE, so even an out-of-protocol concurrent insertion cannot be
        # consumed by this stale terminal callback.
        present_replica_ids = sorted(locked_record_ids)
        if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            for replica_id, record_id in locked_record_ids.items():
                route_projection.revoke_replica_lease_in_session(
                    session, service_name, replica_id, record_id,
                    'replica_teardown')
            paid_retirement.delete_in_session(session, service_name,
                                              present_replica_ids)
        for start in range(0, len(replica_ids), _REPLICA_DELETE_CHUNK_SIZE):
            chunk = replica_ids[start:start + _REPLICA_DELETE_CHUNK_SIZE]
            session.execute(
                sqlalchemy.delete(paid_capacity_claims_table).where(
                    paid_capacity_claims_table.c.service_name == service_name,
                    paid_capacity_claims_table.c.replica_id.in_(chunk)))
        for start in range(0, len(present_replica_ids),
                           _REPLICA_DELETE_CHUNK_SIZE):
            chunk = present_replica_ids[start:start +
                                        _REPLICA_DELETE_CHUNK_SIZE]
            result = session.execute(
                sqlalchemy.delete(replicas_table).where(
                    replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id.in_(chunk)))
            if result.rowcount != len(chunk):
                session.rollback()
                return False
        session.commit()
    return True


def _replica_resource_action_identity_from_row(
    row: sqlalchemy.engine.Row,) -> ReplicaResourceActionIdentity | None:
    """Decode one no-pickle identity row and reject partial commitments."""
    values = row._mapping  # pylint: disable=protected-access
    identity_values = (
        values['replica_incarnation'],
        values['desired_generation'],
        values['sky_cluster_record_uuid'],
    )
    link_names = (
        'launch_action_id',
        'down_action_id',
        'launch_shadow_coverage_id',
        'down_shadow_coverage_id',
        'launch_shadow_sample_id',
        'down_shadow_sample_id',
    )
    link_values = {name: values[name] for name in link_names}
    if all(value is None for value in identity_values):
        if any(value is not None for value in link_values.values()):
            raise MalformedReplicaResourceActionIdentityError(
                'A legacy replica row has resource-action links.')
        return None
    replica_incarnation, desired_generation, cluster_record_uuid = (
        identity_values)
    if (not isinstance(replica_incarnation, uuid.UUID) or
            type(desired_generation) is not int or desired_generation <= 0 or
            not isinstance(cluster_record_uuid, uuid.UUID)):
        raise MalformedReplicaResourceActionIdentityError(
            'A replica row has a partial or invalid resource-action identity.')
    for name, value in link_values.items():
        if value is not None and not isinstance(value, uuid.UUID):
            raise MalformedReplicaResourceActionIdentityError(
                f'A replica row has an invalid {name}.')
    launch_coverage = link_values['launch_shadow_coverage_id']
    down_coverage = link_values['down_shadow_coverage_id']
    if (link_values['launch_action_id'] is not None and
            launch_coverage is not None):
        raise MalformedReplicaResourceActionIdentityError(
            'A replica row has competing launch action owners.')
    if (link_values['down_action_id'] is not None and
            down_coverage is not None):
        raise MalformedReplicaResourceActionIdentityError(
            'A replica row has competing down action owners.')
    if (link_values['launch_shadow_sample_id'] is not None and
            link_values['launch_shadow_sample_id'] != launch_coverage):
        raise MalformedReplicaResourceActionIdentityError(
            'A replica row has an unbound launch shadow sample.')
    if (link_values['down_shadow_sample_id'] is not None and
            link_values['down_shadow_sample_id'] != down_coverage):
        raise MalformedReplicaResourceActionIdentityError(
            'A replica row has an unbound down shadow sample.')
    replica_id = values['replica_id']
    cluster_name = values['cluster_name']
    if (type(replica_id) is not int or replica_id < 0 or
            not isinstance(cluster_name, str) or not cluster_name):
        raise MalformedReplicaResourceActionIdentityError(
            'A replica row has an invalid physical target.')
    return ReplicaResourceActionIdentity(
        replica_id=replica_id,
        cluster_name=cluster_name,
        replica_incarnation=replica_incarnation,
        desired_generation=desired_generation,
        sky_cluster_record_uuid=cluster_record_uuid,
    )


def get_replica_resource_action_identities(
    service_name: str,
    replica_ids: list[int],
) -> dict[int, ReplicaResourceActionIdentity | None]:
    """Read exact action-aware teardown identities without deserializing rows.

    Legacy all-null rows map to ``None`` and missing rows are omitted.  Any
    partial identity or contradictory action/shadow linkage fails the entire
    snapshot closed so a caller cannot fall back to an unsafe name-only
    teardown for an action-owned resource.
    """
    replica_ids = list(dict.fromkeys(replica_ids))
    if not replica_ids:
        return {}
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                replicas_table.c.replica_id,
                replicas_table.c.cluster_name,
                replicas_table.c.replica_incarnation,
                replicas_table.c.desired_generation,
                replicas_table.c.sky_cluster_record_uuid,
                replicas_table.c.launch_action_id,
                replicas_table.c.down_action_id,
                replicas_table.c.launch_shadow_coverage_id,
                replicas_table.c.down_shadow_coverage_id,
                replicas_table.c.launch_shadow_sample_id,
                replicas_table.c.down_shadow_sample_id,
            ).where(replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id.in_(replica_ids))).fetchall()
    identities: dict[int, ReplicaResourceActionIdentity | None] = {}
    for row in rows:
        identity = _replica_resource_action_identity_from_row(row)
        replica_id = row._mapping['replica_id']  # pylint: disable=protected-access
        identities[replica_id] = identity
    return identities


def get_replica_resource_action_identity(
    service_name: str,
    replica_id: int,
) -> ReplicaResourceActionIdentity | None:
    """Read one exact action-aware identity; return None for legacy/missing."""
    return get_replica_resource_action_identities(service_name,
                                                  [replica_id]).get(replica_id)


def get_replica_info_with_resource_action_identity(
    service_name: str,
    replica_id: int,
) -> tuple['replica_managers.ReplicaInfo', ReplicaResourceActionIdentity |
           None] | None:
    """Atomically snapshot one replica projection and its teardown fence."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                replicas_table.c.replica_id,
                replicas_table.c.cluster_name,
                replicas_table.c.replica_state_version,
                replicas_table.c.replica_state,
                replicas_table.c.replica_incarnation,
                replicas_table.c.desired_generation,
                replicas_table.c.sky_cluster_record_uuid,
                replicas_table.c.launch_action_id,
                replicas_table.c.down_action_id,
                replicas_table.c.launch_shadow_coverage_id,
                replicas_table.c.down_shadow_coverage_id,
                replicas_table.c.launch_shadow_sample_id,
                replicas_table.c.down_shadow_sample_id,
            ).where(replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id == replica_id)).fetchone()
    if row is None:
        return None
    values = row._mapping  # pylint: disable=protected-access
    info = _replica_from_state(values['replica_state_version'],
                               values['replica_state'])
    if (info.replica_id != replica_id or
            info.cluster_name != values['cluster_name']):
        raise MalformedReplicaResourceActionIdentityError(
            'Replica JSON state differs from its physical target columns.')
    return info, _replica_resource_action_identity_from_row(row)


def get_replica_info_from_id(
        service_name: str,
        replica_id: int) -> Optional['replica_managers.ReplicaInfo']:
    """Gets a replica info from the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(
                replicas_table.c.replica_state_version,
                replicas_table.c.replica_state).where(
                    sqlalchemy.and_(
                        replicas_table.c.service_name == service_name,
                        replicas_table.c.replica_id == replica_id))).fetchone()
    return _replica_from_state(result[0], result[1]) if result else None


def replica_from_storage_state(
        replica_state_version: int,
        replica_state: dict[str, Any]) -> 'replica_managers.ReplicaInfo':
    """Decode one current row for an already-owned database transaction."""
    return _replica_from_state(replica_state_version, replica_state)


def get_replica_infos_from_ids(
        service_name: str,
        replica_ids: list[int]) -> dict[int, 'replica_managers.ReplicaInfo']:
    """Gets replica infos for the given replica ids in one query.

    Ids without a matching row are omitted from the returned dict.
    """
    if not replica_ids:
        return {}

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                replicas_table.c.replica_id,
                replicas_table.c.replica_state_version,
                replicas_table.c.replica_state).where(
                    sqlalchemy.and_(
                        replicas_table.c.service_name == service_name,
                        replicas_table.c.replica_id.in_(sorted(
                            set(replica_ids)))))).fetchall()
    return {row[0]: _replica_from_state(row[1], row[2]) for row in rows}


def get_replica_ids(service_name: str) -> set[int]:
    """Gets the ids of all replica rows of a service (no unpickling)."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(replicas_table.c.replica_id).where(
                replicas_table.c.service_name == service_name)).fetchall()
    return {row[0] for row in rows}


def get_replica_cluster_names() -> set[str]:
    """Gets exact cluster identities for all current replica rows.

    The JSON-state migration backfilled and verifies this plain column, so
    ownership discovery need not deserialize every replica state blob.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(replicas_table.c.cluster_name).where(
                replicas_table.c.cluster_name.is_not(None))).fetchall()
    return {str(row[0]) for row in rows}


def get_replica_infos(
        service_name: str) -> list['replica_managers.ReplicaInfo']:
    """Gets all replica infos of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                replicas_table.c.replica_state_version,
                replicas_table.c.replica_state).where(
                    replicas_table.c.service_name == service_name)).fetchall()
    return [_replica_from_state(row[0], row[1]) for row in rows]


def get_ready_replica_infos(
        service_name: str) -> list['replica_managers.ReplicaInfo']:
    """Gets ready replica infos without decoding retained terminal history.

    The scalar status is transactionally maintained with the JSON state and is
    used only as a conservative SQL prefilter.  Callers must still validate the
    decoded ``ReplicaInfo`` before treating a row as ready capacity.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(replicas_table.c.replica_state_version,
                              replicas_table.c.replica_state).where(
                                  replicas_table.c.service_name == service_name,
                                  replicas_table.c.status ==
                                  ReplicaStatus.READY.value)).fetchall()
    return [_replica_from_state(row[0], row[1]) for row in rows]


def get_scale_planning_state_fingerprint(service_name: str,
                                         require_version: bool = False
                                        ) -> str | None:
    """Return a compact mutation fingerprint for autoscaler planning state.

    Shape-aware autoscalers may block while resolving legacy cluster handles.
    The controller samples this fingerprint before reading its planning rows
    and again after that blocking preload. Equality proves that the service's
    runtime fields and every replica row stayed unchanged across the read and
    preload window; a mismatch makes the tick retry from durable state.

    PostgreSQL's per-row ``xmin`` is used as the mutation revision so a large
    terminal history contributes only ``(replica_id, revision)`` rather than
    transferring every JSON document again. SQLite remains a supported local
    controller/test database, so its fallback hashes the complete JSON rows.
    The central API-server path is PostgreSQL-only.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        runtime_query = sqlalchemy.select(
            services_table.c.hash,
            services_table.c.controller_pid,
            services_table.c.controller_ip,
            services_table.c.active_versions,
        ).where(services_table.c.name == service_name)
        if require_version:
            runtime_query = runtime_query.where(sqlalchemy.exists().where(
                version_specs_table.c.service_name == services_table.c.name))
        runtime_row = session.execute(runtime_query).fetchone()
        if runtime_row is None:
            return None

        if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            revision = sqlalchemy.literal_column('replicas.xmin::text').label(
                '_row_revision')
            replica_rows = session.execute(
                sqlalchemy.select(replicas_table.c.replica_id, revision).where(
                    replicas_table.c.service_name == service_name).order_by(
                        replicas_table.c.replica_id)).fetchall()
            replica_material: list[Any] = [
                (int(row.replica_id), row._mapping['_row_revision'])  # pylint: disable=protected-access
                for row in replica_rows
            ]
        else:
            replica_rows = session.execute(
                sqlalchemy.select(
                    replicas_table.c.replica_id,
                    replicas_table.c.replica_state_version,
                    replicas_table.c.replica_state,
                ).where(replicas_table.c.service_name == service_name).order_by(
                    replicas_table.c.replica_id)).fetchall()
            replica_material = [(int(row.replica_id), row.replica_state_version,
                                 row.replica_state) for row in replica_rows]

    runtime = runtime_row._mapping  # pylint: disable=protected-access
    material = {
        'runtime': {
            'hash': runtime['hash'],
            'controller_pid': runtime['controller_pid'],
            'controller_ip': runtime['controller_ip'],
            'active_versions': (json.loads(runtime['active_versions'])
                                if runtime['active_versions'] else []),
        },
        'replicas': replica_material,
    }
    encoded = json.dumps(material, sort_keys=True,
                         separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def get_replica_infos_grouped(
) -> dict[str, list['replica_managers.ReplicaInfo']]:
    """Gets every replica info grouped by its owning service in one query."""
    engine = _db_manager.get_engine()
    infos_by_service: dict[
        str, list[replica_managers.ReplicaInfo]] = collections.defaultdict(list)
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(replicas_table.c.service_name,
                              replicas_table.c.replica_state_version,
                              replicas_table.c.replica_state))
        # Iterate the cursor instead of materializing a second list of every
        # serialized blob alongside the decoded snapshot.
        for service_name, state_version, replica_state in rows:
            infos_by_service[service_name].append(
                _replica_from_state(state_version, replica_state))
    return dict(infos_by_service)


def get_replica_service_names() -> list[str]:
    """Every service that currently has any replica row (a cheap DISTINCT).

    Used by the reserved-fill broker's round debit to scan rows of FORMER
    claimants too: a disabled/pruned/moved claimant's fill rows still
    occupy (or are about to occupy) their pool, and scanning only current
    claimants would feed those slots to a peer.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                replicas_table.c.service_name).distinct()).fetchall()
    return [row[0] for row in rows]


def get_replica_status_counts(service_name: str) -> dict[str, int]:
    """Return persisted replica counts grouped by status in one SQL query."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                replicas_table.c.status,
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).where(replicas_table.c.service_name == service_name).group_by(
                replicas_table.c.status)).fetchall()
    return {status: count for status, count in rows}


def get_replica_status_and_capacity_counts(
        service_name: str) -> tuple[dict[str, int], dict[str, int]]:
    """Return physical row and planned-capacity counts grouped by status.

    This reads only the compact JSON state used for replica recovery. It avoids
    unpickling ReplicaInfo objects, resolving cluster handles, or contacting
    any cloud/cluster API on dashboard summary requests.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                replicas_table.c.status, replicas_table.c.replica_state).where(
                    replicas_table.c.service_name == service_name)).fetchall()
    status_counts: collections.defaultdict[str,
                                           int] = (collections.defaultdict(int))
    capacity_counts: collections.defaultdict[str, int] = (
        collections.defaultdict(int))
    for status, replica_state in rows:
        status_counts[status] += 1
        planned_capacity = (replica_state.get('planned_capacity', 1)
                            if isinstance(replica_state, dict) else 1)
        if (not isinstance(planned_capacity, int) or
                isinstance(planned_capacity, bool) or planned_capacity < 1):
            planned_capacity = 1
        capacity_counts[status] += planned_capacity
    return dict(status_counts), dict(capacity_counts)


def total_number_provisioning_replicas() -> int:
    """Returns the total number of provisioning replicas."""
    provisioning_count, _ = get_replica_launch_budget_counts()
    return provisioning_count


def total_number_terminating_replicas() -> int:
    """Returns the total number of terminating replicas."""
    _, terminating_count = get_replica_launch_budget_counts()
    return terminating_count


def get_replica_launch_budget_counts() -> tuple[int, int]:
    """Returns provisioning and terminating replica counts in one query.

    Returns:
        A ``(provisioning_count, terminating_count)`` tuple.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                sqlalchemy.func.sum(
                    sqlalchemy.case((replicas_table.c.status
                                     == ReplicaStatus.PROVISIONING.value, 1),
                                    else_=0)),
                sqlalchemy.func.sum(
                    sqlalchemy.case(
                        (replicas_table.c.sky_down_status
                         == common_utils.ProcessStatus.RUNNING.value, 1),
                        else_=0)),
            )).one()
    return int(row[0] or 0), int(row[1] or 0)


# === Version functions ===
class VersionCommitResult(enum.Enum):
    """Outcome of committing the immutable contents of a service version."""

    COMMITTED = 'committed'
    IDEMPOTENT_RETRY = 'idempotent_retry'
    REJECTED = 'rejected'
    CONTENT_CONFLICT = 'content_conflict'
    SEMANTIC_CONFLICT = 'semantic_conflict'
    LB_HA_CONFLICT = 'lb_ha_conflict'
    STALE_VERSION = 'stale_version'

    def __bool__(self) -> bool:
        # Preserve the historical bool contract for internal callers while
        # allowing the controller to distinguish a content conflict from an
        # ownership/terminal rejection.
        return self in (VersionCommitResult.COMMITTED,
                        VersionCommitResult.IDEMPOTENT_RETRY)


ControllerConfigSnapshot = tuple[bytes, str, str]


@dataclasses.dataclass(frozen=True)
class PlacementNormalizationRequest:
    """Exact service/version generation awaiting a controller-load receipt."""

    run_id: uuid.UUID
    recovery_version: int
    current_version: int
    lifecycle_epoch: int | None


class ControllerConfigCorruptionError(RuntimeError):
    """A persisted controller-config snapshot failed integrity validation."""


def _validate_controller_config_snapshot(
    controller_config: bytes | None,
    controller_config_digest: str | None,
    controller_config_snapshot_id: str | None,
    *,
    argument_name: str = 'controller config snapshot',
) -> ControllerConfigSnapshot | None:
    """Validate an all-or-none controller config snapshot tuple."""
    values = (controller_config, controller_config_digest,
              controller_config_snapshot_id)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ValueError(f'{argument_name} must provide config bytes, digest, '
                         'and snapshot ID together.')
    if not isinstance(controller_config, bytes):
        raise ValueError(f'{argument_name} config must be bytes.')
    if (not isinstance(controller_config_digest, str) or
            re.fullmatch(r'[0-9a-f]{64}', controller_config_digest) is None):
        raise ValueError(f'{argument_name} digest must be a lowercase SHA-256 '
                         'hex digest.')
    if hashlib.sha256(
            controller_config).hexdigest() != controller_config_digest:
        raise ValueError(
            f'{argument_name} digest does not match its config bytes.')
    if (not isinstance(controller_config_snapshot_id, str) or re.fullmatch(
            r'[0-9a-f]{64}', controller_config_snapshot_id) is None):
        raise ValueError(f'{argument_name} snapshot ID must be 64 lowercase '
                         'hex characters.')
    return (controller_config, controller_config_digest,
            controller_config_snapshot_id)


def _validate_legacy_controller_config_snapshot(
    snapshot: ControllerConfigSnapshot | None,
) -> ControllerConfigSnapshot | None:
    if snapshot is None:
        return None
    if not isinstance(snapshot, tuple) or len(snapshot) != 3:
        raise ValueError('legacy controller config snapshot must be a '
                         '(bytes, digest, snapshot ID) tuple.')
    return _validate_controller_config_snapshot(
        snapshot[0],
        snapshot[1],
        snapshot[2],
        argument_name='legacy controller config snapshot')


def _lock_service_for_version_mutation(session: orm.Session,
                                       service_name: str) -> bool:
    """Lock the parent row and return whether version writes are allowed.

    A missing parent is kept as a legacy/test-compatible case. Production
    updates always have a service row; once that row is terminal, teardown has
    won and no placeholder or YAML commit may be written behind it.
    """
    row = session.execute(
        sqlalchemy.select(services_table.c.status).where(
            services_table.c.name ==
            service_name).with_for_update()).fetchone()
    if row is None:
        return True
    return ServiceStatus[row[0]] not in ServiceStatus.terminal_statuses()


def add_version(service_name: str,
                expected_service_hash: str | None = None,
                expected_lifecycle_epoch: int | None = None,
                created_by: str | None = None) -> int:
    """Add a version, optionally fenced to one lifecycle/incarnation."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine)
        if not _lock_service_for_version_mutation(session, service_name):
            session.rollback()
            raise RuntimeError(f'Service {service_name!r} entered terminal '
                               'status before adding a version.')
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            raise RuntimeError('Service lifecycle ownership was lost before '
                               'adding a version.')
        if (expected_service_hash is not None or
                expected_lifecycle_epoch is not None):
            row = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
                    services_table.c.status).where(
                        services_table.c.name ==
                        service_name).with_for_update()).fetchone()
            if (row is None or (expected_lifecycle_epoch is not None and
                                row[1] != expected_lifecycle_epoch)):
                session.rollback()
                raise RuntimeError('Service lifecycle ownership was lost '
                                   'before adding a version.')
            if (expected_service_hash is not None and
                    row[0] != expected_service_hash):
                session.rollback()
                raise RuntimeError('Service incarnation changed before '
                                   'adding a version.')
            if ServiceStatus[row[2]] in ServiceStatus.terminal_statuses():
                session.rollback()
                raise RuntimeError('Service entered terminal status before '
                                   'adding a version.')
        # Insert new version with MAX(version) + 1 in a single atomic operation
        max_version_subquery = sqlalchemy.select(
            sqlalchemy.func.coalesce(
                sqlalchemy.func.max(version_specs_table.c.version), 0) +
            1).where(version_specs_table.c.service_name ==
                     service_name).scalar_subquery()

        # Use INSERT with subquery and RETURNING
        insert_stmt = sqlalchemy.insert(version_specs_table).values(
            service_name=service_name,
            version=max_version_subquery,
            spec=pickle.dumps(None, protocol=4),
            created_by=created_by).returning(version_specs_table.c.version)

        result = session.execute(insert_stmt)
        new_version = result.scalar()
        session.commit()
    return new_version


def add_or_update_version(
    service_name: str,
    version: int,
    spec: 'service_spec.SkyServiceSpec',
    yaml_content: str,
    submitted_yaml_content: str | None = None,
    placement_catalog: dict[str, Any] | None = None,
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    ha_recovery_script: str | None = None,
    controller_config: bytes | None = None,
    controller_config_digest: str | None = None,
    controller_config_snapshot_id: str | None = None,
    legacy_controller_config_snapshot: ControllerConfigSnapshot | None = None,
    legacy_controller_applied_version: int | None = None,
    controller_job_projection: dict[str, Any] | None = None,
    controller_work_cache: dict[str, Any] | None = None,
    worker_placement_projections: list[dict[str, Any]] | None = None,
) -> VersionCommitResult:
    """Commit a version placeholder once, or accept an identical retry.

    A non-NULL YAML row is immutable: replica rows and controller recovery use
    the version number as its identity. Overwriting its content could leave a
    live controller running one spec while a respawn boots another.
    """
    controller_config_snapshot = _validate_controller_config_snapshot(
        controller_config, controller_config_digest,
        controller_config_snapshot_id)
    (controller_job_projection, controller_work_cache,
     worker_placement_projections) = (_validated_placement_projections(
         controller_job_projection, controller_work_cache,
         worker_placement_projections))
    engine = _db_manager.get_engine()
    projection_columns_available = _require_projection_columns_if_nonnull(
        engine, controller_job_projection, controller_work_cache,
        worker_placement_projections)
    legacy_controller_config_snapshot = (
        _validate_legacy_controller_config_snapshot(
            legacy_controller_config_snapshot))
    if ((legacy_controller_config_snapshot is None)
            != (legacy_controller_applied_version is None)):
        raise ValueError('First protocol activation must provide the legacy '
                         'controller config and exact applied version '
                         'together.')
    if legacy_controller_applied_version is not None:
        if (isinstance(legacy_controller_applied_version, bool) or
                not isinstance(legacy_controller_applied_version, int) or
                legacy_controller_applied_version < 1 or
                legacy_controller_applied_version >= version):
            raise ValueError('The legacy applied version must identify an '
                             'earlier positive service version.')
        if (not expected_service_hash or expected_controller_owner is None):
            raise ValueError('First protocol activation requires an exact '
                             'service incarnation and controller owner fence.')
    resource_scope: str | None = None
    service_pool: bool | None = None
    service_lifecycle_epoch: int | None = None
    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        _begin_immediate_if_sqlite(session, engine)
        if not _lock_service_for_version_mutation(session, service_name):
            session.rollback()
            return VersionCommitResult.REJECTED
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return VersionCommitResult.REJECTED
        if (expected_service_hash is not None or
                expected_lifecycle_epoch is not None or
                expected_controller_owner is not None):
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
                    services_table.c.controller_pid,
                    services_table.c.controller_ip, services_table.c.status,
                    services_table.c.resource_scope,
                    services_table.c.pool).where(
                        services_table.c.name ==
                        service_name).with_for_update()).fetchone()
            if (owner is None or (expected_service_hash is not None and
                                  owner[0] != expected_service_hash) or
                (expected_lifecycle_epoch is not None and
                 owner[1] != expected_lifecycle_epoch) or
                (expected_controller_owner is not None and
                 (owner[2], owner[3]) != expected_controller_owner)):
                session.rollback()
                return VersionCommitResult.REJECTED
            if ServiceStatus[owner[4]] in ServiceStatus.terminal_statuses():
                session.rollback()
                return VersionCommitResult.REJECTED
            resource_scope = owner[5]
            if type(owner[6]) is not int or owner[6] not in (0, 1):
                raise ephemeral_storage_contract.EphemeralStorageContractError(
                    'Scoped storage commit has an invalid parent pool bit.')
            service_pool = owner[6] == 1
            service_lifecycle_epoch = owner[1]
        storage_generation = _ephemeral_storage_generation_from_yaml(
            yaml_content, resource_scope)
        if engine.dialect.name not in (
                db_utils.SQLAlchemyDialect.SQLITE.value,
                db_utils.SQLAlchemyDialect.POSTGRESQL.value):
            raise ValueError('Unsupported database dialect')

        existing = session.execute(
            sqlalchemy.select(
                version_specs_table.c.yaml_content,
                version_specs_table.c.placement_catalog,
                version_specs_table.c.controller_config,
                version_specs_table.c.controller_config_digest,
                version_specs_table.c.controller_config_snapshot_id,
                version_specs_table.c.submitted_yaml_content,
                version_specs_table.c.retired_at,
                version_specs_table.c.created_at,
                *([
                    version_specs_table.c.controller_job_projection,
                    version_specs_table.c.controller_work_cache,
                    version_specs_table.c.worker_placement_projections,
                ] if projection_columns_available else [])).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version)).fetchone()
        # A retired history row deliberately resembles an interrupted
        # placeholder to old readers (NULL committed YAML plus pickled None),
        # but its version identity is permanently consumed.  Never let the
        # ordinary commit path refill that tombstone.
        if existing is not None and existing[6] is not None:
            session.rollback()
            return VersionCommitResult.CONTENT_CONFLICT
        identical_retry = existing is not None and existing[0] == yaml_content
        projection_conflict = (identical_retry and
                               projection_columns_available and
                               (existing[8] != controller_job_projection or
                                existing[9] != controller_work_cache or
                                existing[10] != worker_placement_projections))
        if projection_conflict:
            session.rollback()
            return VersionCommitResult.CONTENT_CONFLICT
        if existing is not None and existing[
                0] is not None and not identical_retry:
            session.rollback()
            return VersionCommitResult.CONTENT_CONFLICT
        if identical_retry:
            stored_controller_config_snapshot = (None if all(
                value is None for value in existing[2:5]) else tuple(
                    existing[2:5]))
            if stored_controller_config_snapshot != controller_config_snapshot:
                session.rollback()
                return VersionCommitResult.CONTENT_CONFLICT
            if (controller_config_snapshot is not None and
                (existing[1] != placement_catalog or
                 existing[5] != submitted_yaml_content)):
                session.rollback()
                return VersionCommitResult.CONTENT_CONFLICT
        serialized_spec: bytes | None = None
        if not identical_retry:
            serialized_spec = _serialize_current_service_spec(spec)
        if not identical_retry and controller_config_snapshot is not None:
            marker = constants.VERSIONED_HA_CONFIG_RECOVERY_MARKER
            if (ha_recovery_script is None or
                    marker not in ha_recovery_script.splitlines() or
                    '# SKY_SERVE_CONFIG_SNAPSHOT_BEGIN' in ha_recovery_script or
                    '# SKY_SERVE_CONFIG_SNAPSHOT_END' in ha_recovery_script):
                raise ValueError('A versioned controller config must commit '
                                 'with a scrubbed versioned HA recovery '
                                 'script.')
        if (not identical_retry and
                legacy_controller_config_snapshot is not None):
            # Lock and validate every historical tuple before switching the
            # service-global recovery script. A partial/corrupt row must not
            # be hidden by backfilling only all-NULL peers and then failing on
            # the next pod recovery.
            historical_rows = session.execute(
                sqlalchemy.select(
                    version_specs_table.c.version,
                    version_specs_table.c.controller_config,
                    version_specs_table.c.controller_config_digest,
                    version_specs_table.c.controller_config_snapshot_id).where(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.version < version,
                        version_specs_table.c.yaml_content.isnot(
                            None)).with_for_update()).fetchall()
            for historical_row in historical_rows:
                historical_values = list(historical_row[1:4])
                if all(value is None for value in historical_values):
                    continue
                if isinstance(historical_values[0], memoryview):
                    historical_values[0] = historical_values[0].tobytes()
                try:
                    _validate_controller_config_snapshot(
                        historical_values[0],
                        historical_values[1],
                        historical_values[2],
                        argument_name=(
                            f'historical version {historical_row[0]} '
                            'controller config snapshot'))
                except ValueError as e:
                    raise ControllerConfigCorruptionError(
                        f'Cannot activate versioned recovery for service '
                        f'{service_name!r}: {e}') from e
        if ha_recovery_script is not None and identical_retry:
            existing_recovery_script = session.execute(
                sqlalchemy.select(
                    serve_ha_recovery_script_table.c.script).where(
                        serve_ha_recovery_script_table.c.service_name ==
                        service_name).with_for_update()).scalar_one_or_none()
            if existing_recovery_script != ha_recovery_script:
                session.rollback()
                return VersionCommitResult.CONTENT_CONFLICT
        if not identical_retry:
            higher_committed_version = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.max(version_specs_table.c.version)).where(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.version > version,
                        version_specs_table.c.yaml_content.isnot(
                            None))).scalar()
            if higher_committed_version is not None:
                session.rollback()
                return VersionCommitResult.STALE_VERSION

        uses_logical_replicas = spec.uses_logical_replicas is True
        semantics_row = session.execute(
            sqlalchemy.select(services_table.c.logical_replica_semantics,
                              services_table.c.lb_ha_enabled).where(
                                  services_table.c.name ==
                                  service_name).with_for_update()).fetchone()
        # The fence applies to new commits, not to lost-response retries of a
        # version that was already committed before logical activation.
        if (not identical_retry and semantics_row is not None and
                bool(semantics_row[0]) and not uses_logical_replicas):
            session.rollback()
            return VersionCommitResult.SEMANTIC_CONFLICT
        requested_lb_ha = bool(spec.lb_high_availability)
        if (not identical_retry and semantics_row is not None and
                bool(semantics_row[1]) != requested_lb_ha):
            # Enabling and disabling HA move Kubernetes traffic authority and
            # therefore require the dedicated selector saga. A normal Serve
            # version commit cannot safely perform that cross-store mutation.
            session.rollback()
            return VersionCommitResult.LB_HA_CONFLICT
        committed_version_created_at = (existing[7]
                                        if identical_retry else time.time())
        projection_values = ({
            'controller_job_projection': controller_job_projection,
            'controller_work_cache': controller_work_cache,
            'worker_placement_projections': worker_placement_projections,
        } if projection_columns_available else {})
        if existing is None:
            assert serialized_spec is not None
            session.execute(version_specs_table.insert().values(
                service_name=service_name,
                version=version,
                spec=serialized_spec,
                yaml_content=yaml_content,
                submitted_yaml_content=submitted_yaml_content,
                placement_catalog=placement_catalog,
                controller_config=(None if controller_config_snapshot is None
                                   else controller_config_snapshot[0]),
                controller_config_digest=(None
                                          if controller_config_snapshot is None
                                          else controller_config_snapshot[1]),
                controller_config_snapshot_id=(
                    None if controller_config_snapshot is None else
                    controller_config_snapshot[2]),
                **projection_values,
                created_at=committed_version_created_at))
        elif existing[0] is None:
            # `add_version` reserves a NULL-YAML placeholder. The service-row
            # lock above serializes the one transition that fills it.
            assert serialized_spec is not None
            filled_placeholder = session.execute(
                sqlalchemy.update(version_specs_table).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version,
                    version_specs_table.c.yaml_content.is_(None),
                    version_specs_table.c.retired_at.is_(None)).values(
                        spec=serialized_spec,
                        yaml_content=yaml_content,
                        submitted_yaml_content=submitted_yaml_content,
                        placement_catalog=placement_catalog,
                        controller_config=(None
                                           if controller_config_snapshot is None
                                           else controller_config_snapshot[0]),
                        controller_config_digest=(
                            None if controller_config_snapshot is None else
                            controller_config_snapshot[1]),
                        controller_config_snapshot_id=(
                            None if controller_config_snapshot is None else
                            controller_config_snapshot[2]),
                        **projection_values,
                        created_at=committed_version_created_at))
            if filled_placeholder.rowcount != 1:
                session.rollback()
                return VersionCommitResult.CONTENT_CONFLICT
        elif identical_retry and existing[1] is None and placement_catalog:
            # A retry may be the first new binary to touch a version committed
            # by an older controller. Backfill only the absent catalog; the
            # immutable YAML and pickled spec bytes remain untouched.
            session.execute(
                sqlalchemy.update(version_specs_table).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version,
                    version_specs_table.c.placement_catalog.is_(None)).values(
                        placement_catalog=placement_catalog))
        if not identical_retry and legacy_controller_config_snapshot is not None:
            # A first config-aware update can make historical versions
            # independently recoverable.  Only committed, older, entirely
            # NULL snapshots are backfilled; an existing snapshot is immutable.
            session.execute(
                sqlalchemy.update(version_specs_table).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version < version,
                    version_specs_table.c.yaml_content.isnot(None),
                    version_specs_table.c.controller_config.is_(None),
                    version_specs_table.c.controller_config_digest.is_(None),
                    version_specs_table.c.controller_config_snapshot_id.is_(
                        None)).
                values(
                    controller_config=legacy_controller_config_snapshot[0],
                    controller_config_digest=legacy_controller_config_snapshot[
                        1],
                    controller_config_snapshot_id=
                    legacy_controller_config_snapshot[2]))
            applied_result = session.execute(
                sqlalchemy.update(version_specs_table).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version ==
                    legacy_controller_applied_version,
                    version_specs_table.c.yaml_content.isnot(None),
                    version_specs_table.c.quarantined_at.is_(None)).values(
                        controller_applied_at=sqlalchemy.func.coalesce(
                            version_specs_table.c.controller_applied_at,
                            time.time())))
            if applied_result.rowcount != 1:
                session.rollback()
                return VersionCommitResult.REJECTED
        if (not identical_retry and semantics_row is not None and
                uses_logical_replicas):
            session.execute(
                sqlalchemy.update(services_table).where(
                    services_table.c.name == service_name).values(
                        logical_replica_semantics=1))
        if not identical_retry:
            # Elect the immutable version in the same transaction that commits
            # its contents. A controller or dashboard must never observe an
            # elected pointer whose spec is still a NULL-yaml placeholder.
            session.execute(
                sqlalchemy.update(services_table).where(
                    services_table.c.name == service_name).values(
                        current_version=version))
        if ha_recovery_script is not None:
            insert_func = (sqlite.insert if engine.dialect.name
                           == db_utils.SQLAlchemyDialect.SQLITE.value else
                           postgresql.insert)
            recovery_insert = insert_func(
                serve_ha_recovery_script_table).values(
                    service_name=service_name, script=ha_recovery_script)
            session.execute(
                recovery_insert.on_conflict_do_update(
                    index_elements=['service_name'],
                    set_={'script': recovery_insert.excluded.script}))
        # An identical committed YAML is an idempotent retry. Keep both the
        # original YAML and pickled spec bytes untouched.
        if storage_generation is not None:
            if resource_scope is None or service_pool is None:
                raise ephemeral_storage_contract.EphemeralStorageContractError(
                    'Scoped storage commit has no exact service owner.')
            _adopt_exact_ephemeral_storage_cleanup_intent(
                session, service_name, resource_scope, storage_generation,
                yaml_content, service_pool, service_lifecycle_epoch,
                committed_version_created_at)
        session.commit()
    return (VersionCommitResult.IDEMPOTENT_RETRY
            if identical_retry else VersionCommitResult.COMMITTED)


def get_spec(service_name: str,
             version: int) -> Optional['service_spec.SkyServiceSpec']:
    """Gets spec from the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(version_specs_table.c.spec).where(
                sqlalchemy.and_(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version))).fetchone()
    return pickle.loads(result[0]) if result else None


def get_placement_catalog(service_name: str,
                          version: int) -> dict[str, Any] | None:
    """Return the immutable centralized catalog for one service version."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(version_specs_table.c.placement_catalog).where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.version == version)).fetchone()
    return result[0] if result is not None else None


def get_placement_projection_record(
    service_name: str,
    version: int,
) -> tuple[bool, dict[str, Any] | None, dict[str, Any] | None,
           list[dict[str, Any]] | None]:
    """Return existence and immutable platform projections for one version."""
    engine = _db_manager.get_engine()
    if not _placement_projection_columns_available(engine):
        with orm.Session(engine) as session:
            exists = session.execute(
                sqlalchemy.select(version_specs_table.c.version).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version,
                    version_specs_table.c.yaml_content.isnot(None))).fetchone()
        return exists is not None, None, None, None
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(
                version_specs_table.c.controller_job_projection,
                version_specs_table.c.controller_work_cache,
                version_specs_table.c.worker_placement_projections).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version,
                    version_specs_table.c.yaml_content.isnot(None))).fetchone()
    if result is None:
        return False, None, None, None
    return True, result[0], result[1], result[2]


def get_version_controller_config(
    service_name: str,
    version: int,
) -> ControllerConfigSnapshot | None:
    """Return and verify one version's sanitized controller config snapshot."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(
                version_specs_table.c.controller_config,
                version_specs_table.c.controller_config_digest,
                version_specs_table.c.controller_config_snapshot_id).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version)).fetchone()
    if result is None or all(value is None for value in result):
        return None
    controller_config = result[0]
    if isinstance(controller_config, memoryview):
        controller_config = controller_config.tobytes()
    try:
        snapshot = _validate_controller_config_snapshot(
            controller_config,
            result[1],
            result[2],
            argument_name='persisted controller config snapshot')
    except ValueError as e:
        raise ControllerConfigCorruptionError(
            f'Controller config snapshot for service {service_name!r}, '
            f'version {version} failed integrity validation: {e}') from e
    if snapshot is None:
        # The all-NULL case returned above; reaching this branch would mean
        # validation accepted an internally inconsistent database row.
        raise ControllerConfigCorruptionError(
            f'Controller config snapshot for service {service_name!r}, '
            f'version {version} is incomplete.')
    return snapshot


def get_service_config_recovery_identity(
        service_name: str) -> tuple[str, str] | None:
    """Return the minimal durable incarnation/workspace recovery fence."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(
                services_table.c.hash, services_table.c.workspace).where(
                    services_table.c.name == service_name)).fetchone()
    if (result is None or not isinstance(result[0], str) or not result[0] or
            not isinstance(result[1], str) or not result[1]):
        return None
    return result[0], result[1]


def set_placement_catalog_if_missing(service_name: str, version: int,
                                     placement_catalog: dict[str, Any]) -> bool:
    """Compare-and-set a legacy version's one-time catalog backfill.

    Returns true only for the writer that filled the null column. A concurrent
    loser must reread the catalog selected by the winner.
    """
    if not isinstance(placement_catalog, dict):
        raise ValueError('Placement catalog must be a mapping.')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.update(version_specs_table).where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.version == version,
                version_specs_table.c.yaml_content.isnot(None),
                version_specs_table.c.placement_catalog.is_(None)).values(
                    placement_catalog=placement_catalog))
        session.commit()
    return result.rowcount == 1


def mark_bound_replica_launch_running_if_active(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> bool:
    """Advance only an active bound row from SCHEDULED to RUNNING.

    The launch worker starts before this bookkeeping write.  Its reducer may
    therefore project a result first.  Lock and decode the latest row, and
    require the scalar association pointer to remain present, so a stale
    parent snapshot can never overwrite that projection.
    """
    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    with _replica_launch_authority_write_session(
            service_name, invalidates_launch_authority=False) as (_, session):
        _lock_service_row_if_present_for_replica_write(session, service_name)
        row = session.execute(
            sqlalchemy.select(
                replicas_table.c.replica_state_version,
                replicas_table.c.replica_state,
                replicas_table.c.ordinary_launch_association_id,
            ).where(replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id ==
                    replica_id).with_for_update()).mappings().one_or_none()
        if row is None or row['ordinary_launch_association_id'] is None:
            return False
        info = _replica_from_state(row['replica_state_version'],
                                   row['replica_state'])
        if (info.replica_record_id != replica_record_id or
                info.status_property.sky_launch_status
                != common_utils.ProcessStatus.SCHEDULED):
            return False
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        values = _replica_row_values(service_name, replica_id, info)
        result = session.execute(
            sqlalchemy.update(replicas_table).where(
                replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id == replica_id,
                replicas_table.c.ordinary_launch_association_id ==
                row['ordinary_launch_association_id'],
                replicas_table.c.replica_state['replica_record_id'].as_string()
                == replica_record_id).values({
                    key: value
                    for key, value in values.items()
                    if key not in ('service_name', 'replica_id')
                }))
        if result.rowcount != 1:
            return False
        session.commit()
        return True


def get_specs(
        service_name: str, versions: list[int]
) -> dict[int, Optional['service_spec.SkyServiceSpec']]:
    """Gets specs for a service's versions in one query."""
    if not versions:
        return {}

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.spec).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.version.in_(sorted(
                            set(versions)))))).fetchall()
    return {row[0]: pickle.loads(row[1]) for row in rows}


def get_yaml_contents(service_name: str,
                      versions: list[int]) -> dict[int, str | None]:
    """Gets yaml contents for a service's versions in one query."""
    if not versions:
        return {}

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.yaml_content).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.version.in_(sorted(
                            set(versions)))))).fetchall()
    return {row[0]: row[1] for row in rows}


def get_version_yaml_contents(service_name: str) -> dict[int, str]:
    """Gets cleanup YAML for all of a service's versions in one query.

    A retired history row has no live ``yaml_content`` by construction, but
    its copied ``retired_yaml_content`` remains cleanup inventory.  Live
    election and recovery readers intentionally do not use this accessor.
    Rows missing both representations are omitted, and keys are returned in
    ascending version order.
    """
    cleanup_yaml = sqlalchemy.func.coalesce(
        version_specs_table.c.yaml_content,
        version_specs_table.c.retired_yaml_content).label('cleanup_yaml')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(version_specs_table.c.version, cleanup_yaml).
            where(version_specs_table.c.service_name == service_name).order_by(
                version_specs_table.c.version)).fetchall()
    return {row[0]: row[1] for row in rows if row[1] is not None}


def get_system_recovery_authorization_snapshot(
        service_name: str) -> dict[str, Any] | None:
    """Read one elected service/task snapshot for authorization bootstrap.

    The single statement prevents a generator from pairing one incarnation's
    hash or elected version with another version's spec/YAML.  This helper is
    deliberately read-only; the caller applies the PostgreSQL, zero-replica,
    and recovery-eligibility gates before producing any authorization bytes.
    """
    replica_count = (
        sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                         ).select_from(replicas_table).where(
                             replicas_table.c.service_name ==
                             services_table.c.name).scalar_subquery())
    (latest_applicable, latest_quarantined,
     latest_applied_applicable) = _quarantine_aware_version_aggregates()
    election_candidates = sqlalchemy.select(
        version_specs_table.c.service_name.label('service_name'),
        latest_applicable,
        latest_quarantined,
        latest_applied_applicable,
    ).where(version_specs_table.c.service_name == service_name).group_by(
        version_specs_table.c.service_name).subquery(
            'system_recovery_election_candidates')
    elected_version = _quarantine_aware_version_sql_expression(
        election_candidates.c.latest_applicable_version,
        election_candidates.c.latest_quarantined_version,
        election_candidates.c.latest_applied_applicable_version).label(
            'elected_version')
    election = sqlalchemy.select(
        election_candidates.c.service_name,
        elected_version,
    ).subquery('system_recovery_election')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.name,
                services_table.c.hash,
                services_table.c.workspace,
                election.c.elected_version,
                services_table.c.status,
                services_table.c.pool,
                services_table.c.resource_action_mode,
                version_specs_table.c.spec,
                version_specs_table.c.yaml_content,
                version_specs_table.c.quarantined_at,
                replica_count.label('replica_count'),
            ).select_from(
                services_table.join(
                    election,
                    election.c.service_name == services_table.c.name).join(
                        version_specs_table,
                        sqlalchemy.and_(
                            version_specs_table.c.service_name ==
                            services_table.c.name,
                            version_specs_table.c.version ==
                            election.c.elected_version,
                        ))).where(
                            services_table.c.name == service_name)).fetchone()
    if row is None:
        return None
    spec = pickle.loads(row.spec) if row.spec is not None else None
    try:
        status = ServiceStatus[row.status]
    except (KeyError, TypeError):
        status = None
    return {
        'service_name': row.name,
        'service_hash': row.hash,
        'workspace': row.workspace,
        'version': row.elected_version,
        'status': status,
        'pool': None if row.pool is None else bool(row.pool),
        'resource_action_mode': row.resource_action_mode,
        'spec': spec,
        'yaml_content': row.yaml_content,
        'quarantined_at': row.quarantined_at,
        'replica_count': row.replica_count,
    }


def _version_record_from_row(row: Any,
                             projection_columns_available: bool,
                             include_yaml: bool = True) -> dict[str, Any]:
    """Decode one committed version row without changing its public shape."""
    return {
        'version': row.version,
        'spec': pickle.loads(row.spec) if row.spec is not None else None,
        'yaml_content': row.yaml_content if include_yaml else None,
        'submitted_yaml_content':
            (row.submitted_yaml_content if include_yaml else None),
        'created_at': row.created_at,
        'created_by': row.created_by,
        'quarantined_at': row.quarantined_at,
        'quarantine_reason': row.quarantine_reason,
        'controller_job_projection': (row.controller_job_projection if
                                      projection_columns_available else None),
        'controller_work_cache':
            (row.controller_work_cache if projection_columns_available else None
            ),
        'worker_placement_projections':
            (row.worker_placement_projections
             if projection_columns_available else None),
    }


def _version_record_columns(projection_columns_available: bool,
                            include_yaml: bool = True) -> list[Any]:
    columns = [
        version_specs_table.c.version,
        version_specs_table.c.spec,
        version_specs_table.c.created_at,
        version_specs_table.c.created_by,
        version_specs_table.c.quarantined_at,
        version_specs_table.c.quarantine_reason,
    ]
    if include_yaml:
        columns.extend([
            version_specs_table.c.yaml_content,
            version_specs_table.c.submitted_yaml_content,
        ])
    if projection_columns_available:
        columns.extend([
            version_specs_table.c.controller_job_projection,
            version_specs_table.c.controller_work_cache,
            version_specs_table.c.worker_placement_projections,
        ])
    return columns


def get_version_records(service_name: str,
                        include_yaml: bool = True) -> list[dict[str, Any]]:
    """Gets committed version contents and provenance in one query."""
    engine = _db_manager.get_engine()
    projection_columns_available = _placement_projection_columns_available(
        engine)
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(*_version_record_columns(
                projection_columns_available, include_yaml)).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.yaml_content.isnot(None),
                ).order_by(version_specs_table.c.version)).fetchall()
    return [
        _version_record_from_row(row, projection_columns_available,
                                 include_yaml) for row in rows
    ]


def get_version_record(service_name: str,
                       version: int) -> dict[str, Any] | None:
    """Gets one committed version without materializing all retained YAML."""
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError('Version must be a positive integer.')
    engine = _db_manager.get_engine()
    projection_columns_available = _placement_projection_columns_available(
        engine)
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                *_version_record_columns(projection_columns_available)).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version,
                    version_specs_table.c.yaml_content.isnot(None),
                )).fetchone()
    if row is None:
        return None
    return _version_record_from_row(row, projection_columns_available)


def mark_version_controller_applied(
    service_name: str,
    version: int,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    applied_at: float | None = None,
) -> bool:
    """Record the first owner-fenced application of one committed version.

    Commits and runtime reconciliation intentionally overlap: v2 may finish
    applying after v3 has committed. The exact committed v2 row can therefore
    still receive its receipt under the same controller owner. The receipt is
    idempotent, never overwrites its first timestamp, and cannot revive a
    quarantined generation.
    """
    if not expected_service_hash or expected_controller_owner is None:
        return False
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        return False
    if applied_at is None:
        applied_at = time.time()
    owner_predicates = [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
    ]
    expected_pid, expected_ip = expected_controller_owner
    owner_predicates.extend([
        services_table.c.controller_pid == expected_pid,
        services_table.c.controller_ip == expected_ip,
    ])
    with _replica_launch_authority_write_session(service_name) as (_, session):
        result = session.execute(
            sqlalchemy.update(version_specs_table).where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.version == version,
                version_specs_table.c.yaml_content.isnot(None),
                version_specs_table.c.quarantined_at.is_(None),
                sqlalchemy.exists().where(*owner_predicates),
            ).values(controller_applied_at=sqlalchemy.func.coalesce(
                version_specs_table.c.controller_applied_at, applied_at)))
        if result.rowcount != 1:
            session.rollback()
            return False
        session.commit()
    return True


def _placement_normalization_raw_spec_bytes(row: Mapping[str, Any],
                                            prefix: str) -> bytes:
    raw_spec = row[f'{prefix}_spec']
    if isinstance(raw_spec, memoryview):
        raw_spec = raw_spec.tobytes()
    if not isinstance(raw_spec, bytes):
        raise RuntimeError(
            f'Placement normalization {prefix} persisted spec is not bytes.')
    return raw_spec


def _bind_placement_normalization_receipt_authority(
        session: orm.Session, engine: sqlalchemy.engine.Engine) -> None:
    """Bind a receipt transaction to the exact revision-040 schema."""
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('Placement normalization receipts require '
                           f'PostgreSQL; found {engine.dialect.name!r}.')
    try:
        authority = (
            placement_normalization_authority.assert_reader_database_authority(
                session.connection()))
        placement_normalization_authority.bind_session_to_authority(
            session, authority)
    except (placement_normalization_authority.
            PlacementNormalizationAuthorityError) as exc:
        raise RuntimeError(
            'Placement normalization receipt database authority is absent '
            'or invalid.') from exc


def _validate_raw_explicit_placement_contract(
    row: Mapping[str, Any],
    prefix: str,
    *,
    require_cleanup_contract: bool,
) -> None:
    """Require DB bytes to encode an allowed version without repair."""
    raw_spec = _placement_normalization_raw_spec_bytes(row, prefix)
    analysis = placement_contract_normalization.analyze_spec_pickle(raw_spec)
    allowed: tuple[Any, ...]
    if require_cleanup_contract:
        allowed = (placement_contract_normalization.Classification.EXPLICIT_V2,)
        expected = 'explicit mirror-free v2 placement contract'
    else:
        allowed = (
            placement_contract_normalization.Classification.FIELDLESS_SUPPORTED,
            placement_contract_normalization.Classification.EXPLICIT_V1,
            placement_contract_normalization.Classification.EXPLICIT_V2,
            placement_contract_normalization.Classification.
            HISTORICAL_PHYSICAL_PER_GPU,
        )
        expected = 'supported fieldless/v1/v2/historical placement contract'
    if analysis.classification not in allowed:
        detail = (analysis.blocker_reason if analysis.blocker_reason is not None
                  else analysis.classification.value)
        raise RuntimeError(
            f'Placement normalization {prefix} raw persisted spec is not an '
            f'{expected}: {detail}.')


def _placement_normalization_receipt_query(
    service_name: str,
    recovery_version: int,
    current_version: int,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    *,
    require_ledger: bool,
) -> sqlalchemy.Select:
    """Build one owner/version snapshot with both raw-ledger proofs."""
    expected_pid, expected_ip = expected_controller_owner
    recovery_row = version_specs_table.alias(
        'placement_normalization_recovery_version')
    current_row = version_specs_table.alias(
        'placement_normalization_current_version')
    recovery_ledger = placement_normalization_rows_table.alias(
        'placement_normalization_recovery_ledger')
    current_ledger = placement_normalization_rows_table.alias(
        'placement_normalization_current_ledger')
    requested_run = placement_normalization_runs_table.alias(
        'placement_normalization_requested_run')
    source = services_table.join(
        recovery_row,
        sqlalchemy.and_(
            recovery_row.c.service_name == services_table.c.name,
            recovery_row.c.version == recovery_version,
            recovery_row.c.yaml_content.isnot(None),
            recovery_row.c.retired_at.is_(None),
            recovery_row.c.quarantined_at.is_(None),
        )).join(
            current_row,
            sqlalchemy.and_(
                current_row.c.service_name == services_table.c.name,
                current_row.c.version == current_version,
                current_row.c.yaml_content.isnot(None),
                current_row.c.retired_at.is_(None),
            )).outerjoin(
                requested_run, requested_run.c.run_id ==
                services_table.c.placement_normalization_requested_run_id)
    ledger_join = source.join if require_ledger else source.outerjoin
    source = ledger_join(
        recovery_ledger,
        sqlalchemy.and_(
            recovery_ledger.c.run_id ==
            services_table.c.placement_normalization_requested_run_id,
            recovery_ledger.c.service_name == services_table.c.name,
            recovery_ledger.c.version == recovery_version,
        ))
    ledger_join = source.join if require_ledger else source.outerjoin
    source = ledger_join(
        current_ledger,
        sqlalchemy.and_(
            current_ledger.c.run_id ==
            services_table.c.placement_normalization_requested_run_id,
            current_ledger.c.service_name == services_table.c.name,
            current_ledger.c.version == current_version,
        ))
    service_ledger_anchor = placement_normalization_rows_table.alias(
        'placement_normalization_service_ledger_anchor')
    anchor_predicates = (
        service_ledger_anchor.c.run_id ==
        services_table.c.placement_normalization_requested_run_id,
        service_ledger_anchor.c.service_name == services_table.c.name,
    )
    service_ledger_anchor_count = sqlalchemy.select(
        sqlalchemy.func.count(  # pylint: disable=not-callable
        )).select_from(service_ledger_anchor).where(
            *anchor_predicates).correlate(services_table).scalar_subquery()
    service_ledger_matching_hash_count = sqlalchemy.select(
        sqlalchemy.func.count(  # pylint: disable=not-callable
        )).select_from(service_ledger_anchor).where(
            *anchor_predicates, service_ledger_anchor.c.service_hash ==
            expected_service_hash).correlate(services_table).scalar_subquery()
    return sqlalchemy.select(
        services_table.c.lifecycle_epoch.label('lifecycle_epoch'),
        services_table.c.placement_normalization_requested_run_id.label(
            'requested_run_id'),
        services_table.c.placement_normalization_loaded_run_id.label(
            'loaded_run_id'),
        services_table.c.placement_normalization_loaded_image_commit.label(
            'loaded_image_commit'),
        services_table.c.placement_normalization_loaded_controller_pid.label(
            'loaded_controller_pid'),
        services_table.c.placement_normalization_loaded_controller_ip.label(
            'loaded_controller_ip'),
        services_table.c.placement_normalization_loaded_boot_id.label(
            'loaded_boot_id'),
        services_table.c.placement_normalization_loaded_at.label('loaded_at'),
        requested_run.c.run_id.label('manifest_run_id'),
        requested_run.c.mode.label('manifest_mode'),
        requested_run.c.normalizer_version.label('manifest_normalizer_version'),
        requested_run.c.schema_revision.label('manifest_schema_revision'),
        requested_run.c.release_version.label('manifest_release_version'),
        requested_run.c.started_at.label('manifest_started_at'),
        requested_run.c.completed_at.label('manifest_completed_at'),
        requested_run.c.row_bound.label('manifest_row_bound'),
        requested_run.c.row_count.label('manifest_row_count'),
        requested_run.c.classification_counts.label(
            'manifest_classification_counts'),
        requested_run.c.pre_inventory_sha256.label(
            'manifest_pre_inventory_sha256'),
        requested_run.c.post_inventory_sha256.label(
            'manifest_post_inventory_sha256'),
        requested_run.c.freeze_evidence_sha256.label(
            'manifest_freeze_evidence_sha256'),
        recovery_row.c.spec.label('recovery_spec'),
        recovery_row.c.created_at.label('recovery_created_at'),
        recovery_ledger.c.classification.label('recovery_classification'),
        recovery_ledger.c.outcome.label('recovery_outcome'),
        recovery_ledger.c.result_spec_sha256.label(
            'recovery_result_spec_sha256'),
        recovery_ledger.c.service_hash.label('recovery_service_hash'),
        recovery_ledger.c.service_lifecycle_epoch.label(
            'recovery_service_lifecycle_epoch'),
        current_row.c.spec.label('current_spec'),
        current_row.c.created_at.label('current_created_at'),
        current_ledger.c.classification.label('current_classification'),
        current_ledger.c.outcome.label('current_outcome'),
        current_ledger.c.result_spec_sha256.label('current_result_spec_sha256'),
        current_ledger.c.service_hash.label('current_service_hash'),
        current_ledger.c.service_lifecycle_epoch.label(
            'current_service_lifecycle_epoch'),
        service_ledger_anchor_count.label('service_ledger_anchor_count'),
        service_ledger_matching_hash_count.label(
            'service_ledger_matching_hash_count'),
    ).select_from(source).where(
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
        services_table.c.controller_pid == expected_pid,
        services_table.c.controller_ip == expected_ip,
        services_table.c.current_version == current_version,
    )


def _lock_placement_normalization_receipt_query(
        query: sqlalchemy.Select,
        engine: sqlalchemy.engine.Engine) -> sqlalchemy.Select:
    """Lock only the non-nullable service row in a receipt snapshot."""
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        # The snapshot outer-joins the optional requested-run manifest.
        # PostgreSQL rejects an unqualified FOR UPDATE because that would also
        # lock the nullable side of the outer join.
        return query.with_for_update(of=services_table)
    return query


def _raise_placement_normalization_manifest_error(
    context: str,
    error: placement_normalization_manifest.PlacementNormalizationManifestError
) -> typing.NoReturn:
    first_mismatch = error.mismatches[0] if error.mismatches else None
    raise RuntimeError(
        f'Placement normalization {context} is invalid; first mismatch is '
        f'{first_mismatch!r}.') from None


def _placement_normalization_current_inventory_query(
        candidate_services: typing.Sequence[str]) -> sqlalchemy.Select:
    """Project the frozen protocol-4 version schema under a fixed bound."""
    version_columns = tuple(
        version_specs_table.c[column_name] for column_name in
        placement_normalization_manifest.VERSION_SPEC_COLUMNS)
    return sqlalchemy.select(*version_columns).where(
        version_specs_table.c.service_name.in_(candidate_services)).order_by(
            version_specs_table.c.service_name,
            version_specs_table.c.version).limit(
                _PLACEMENT_NORMALIZATION_RECEIPT_MAX_ROWS + 1)


def _validate_protocol_v4_current_inventory(
    session: orm.Session,
    run: Mapping[str, Any],
    entries: list[dict[str, Any]],
) -> None:
    """Bind a terminal protocol-4 receipt to current version state."""
    if not placement_normalization_manifest.is_terminal_protocol4_manifest(
            run, entries):
        return
    candidate_services = sorted({
        entry['service_name']
        for entry in entries
        if entry.get('classification') == 'historical_physical_per_gpu' and
        entry.get('outcome') == 'retired'
    })
    current_rows = [
        dict(row) for row in session.execute(
            _placement_normalization_current_inventory_query(
                candidate_services)).mappings().all()
    ]
    if len(current_rows) > _PLACEMENT_NORMALIZATION_RECEIPT_MAX_ROWS:
        raise RuntimeError(
            'Placement normalization terminal current inventory exceeds the '
            'fixed receipt-reader bound.')

    current_service_hashes = {
        service_name: placement_normalization_manifest.ServiceHashObservation(
            False, None) for service_name in candidate_services
    }
    service_rows = session.execute(
        sqlalchemy.select(services_table.c.name, services_table.c.hash).where(
            services_table.c.name.in_(candidate_services))).mappings().all()
    for service_row in service_rows:
        current_service_hashes[service_row['name']] = (
            placement_normalization_manifest.ServiceHashObservation(
                True, service_row['hash']))

    current_classifications = {}
    for current_row in current_rows:
        identity = (current_row['service_name'], current_row['version'])
        analysis = placement_contract_normalization.analyze_spec_pickle(
            current_row['spec'])
        current_classifications[identity] = analysis.classification.value
    try:
        placement_normalization_manifest.validate_current_inventory(
            run, entries, current_rows, current_service_hashes,
            current_classifications)
    except placement_normalization_manifest.PlacementNormalizationManifestError as error:
        _raise_placement_normalization_manifest_error(
            'terminal current inventory', error)


def _load_and_validate_placement_normalization_manifest(
    session: orm.Session,
    run_id: uuid.UUID,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load and validate a complete manifest under fixed reader bounds."""
    run = session.execute(
        sqlalchemy.select(placement_normalization_runs_table).where(
            placement_normalization_runs_table.c.run_id ==
            run_id)).mappings().one_or_none()
    if run is None:
        raise RuntimeError(
            'Placement normalization requested run manifest is missing.')
    entries = session.execute(
        sqlalchemy.select(placement_normalization_rows_table).where(
            placement_normalization_rows_table.c.run_id == run_id).order_by(
                placement_normalization_rows_table.c.service_name,
                placement_normalization_rows_table.c.version).limit(
                    _PLACEMENT_NORMALIZATION_RECEIPT_MAX_ROWS +
                    1)).mappings().all()
    if len(entries) > _PLACEMENT_NORMALIZATION_RECEIPT_MAX_ROWS:
        raise RuntimeError(
            'Placement normalization requested run ledger exceeds the fixed '
            'receipt-reader bound.')
    run_dict = dict(run)
    entry_dicts = [dict(entry) for entry in entries]
    try:
        placement_normalization_manifest.validate_completed_manifest(
            run_dict, entry_dicts)
    except placement_normalization_manifest.PlacementNormalizationManifestError as error:
        _raise_placement_normalization_manifest_error('requested run manifest',
                                                      error)
    _validate_protocol_v4_current_inventory(session, run_dict, entry_dicts)
    return run_dict, entry_dicts


def _placement_normalization_manifest_identity(
    row: Mapping[str, Any],
) -> tuple[placement_normalization_identity.PlacementNormalizationIdentity,
           str]:
    """Parse the shared exact protocol/mode dispatcher inputs."""
    try:
        identity = placement_normalization_identity.parse_normalizer_identity(
            row['manifest_normalizer_version'])
        mode = placement_normalization_identity.parse_manifest_mode(
            row['manifest_mode'])
    except placement_normalization_identity.PlacementNormalizationIdentityError:
        raise RuntimeError('Placement normalization requested run manifest '
                           'has invalid release identity.') from None
    return identity, mode


def _validate_placement_normalization_run_manifest(
        row: Mapping[str, Any], requested_run_id: uuid.UUID) -> float:
    """Validate the bounded metadata for a requested completed run."""
    if row['manifest_run_id'] != requested_run_id:
        raise RuntimeError('Placement normalization requested run manifest '
                           'is missing or belongs to another generation.')
    _placement_normalization_manifest_identity(row)
    if (not isinstance(row['manifest_release_version'], str) or
            not row['manifest_release_version'] or
            row['manifest_schema_revision'] != '037'):
        raise RuntimeError('Placement normalization requested run manifest '
                           'has invalid release identity.')
    started_at = row['manifest_started_at']
    completed_at = row['manifest_completed_at']
    if (isinstance(started_at, bool) or not isinstance(started_at,
                                                       (int, float)) or
            not math.isfinite(float(started_at)) or started_at < 0 or
            isinstance(completed_at, bool) or
            not isinstance(completed_at, (int, float)) or
            not math.isfinite(float(completed_at)) or
            completed_at < started_at):
        raise RuntimeError('Placement normalization requested run manifest '
                           'has invalid completion timestamps.')
    row_bound = row['manifest_row_bound']
    row_count = row['manifest_row_count']
    if (type(row_bound) is not int or type(row_count) is not int or
            not 0 <= row_count <= row_bound):
        raise RuntimeError('Placement normalization requested run manifest '
                           'has invalid inventory bounds.')
    classification_counts = row['manifest_classification_counts']
    if (not isinstance(classification_counts, dict) or
            any(not isinstance(name, str) or not name or
                type(count) is not int or count < 0
                for name, count in classification_counts.items()) or
            sum(classification_counts.values()) != row_count):
        raise RuntimeError('Placement normalization requested run manifest '
                           'has invalid classification counts.')
    for field in ('pre_inventory_sha256', 'post_inventory_sha256',
                  'freeze_evidence_sha256'):
        digest = row[f'manifest_{field}']
        if (not isinstance(digest, str) or
                re.fullmatch(r'[0-9a-f]{64}', digest) is None):
            raise RuntimeError('Placement normalization requested run '
                               f'manifest has an invalid {field} digest.')
    return float(completed_at)


def _validate_placement_normalization_ledger_result(
    row: Mapping[str, Any],
    prefix: str,
    expected_service_hash: str,
) -> None:
    """Validate immutable result bytes and service incarnation in a ledger."""
    classification = row[f'{prefix}_classification']
    outcome = row[f'{prefix}_outcome']
    identity, mode = _placement_normalization_manifest_identity(row)
    if not placement_normalization_identity.is_loadable_manifest_outcome(
            identity, mode, classification, outcome):
        raise RuntimeError(
            f'Placement normalization {prefix} ledger row is absent or does '
            'not prove a loadable explicit contract result.')
    result_digest = row[f'{prefix}_result_spec_sha256']
    if (not isinstance(result_digest, str) or
            re.fullmatch(r'[0-9a-f]{64}', result_digest) is None):
        raise RuntimeError(
            f'Placement normalization {prefix} ledger result digest is '
            'invalid.')
    raw_spec = _placement_normalization_raw_spec_bytes(row, prefix)
    if hashlib.sha256(raw_spec).hexdigest() != result_digest:
        raise RuntimeError(
            f'Placement normalization {prefix} persisted spec does not match '
            'its requested-run result digest.')
    _validate_placement_normalization_ledger_service_incarnation(
        row, prefix, expected_service_hash)


def _validate_placement_normalization_ledger_service_incarnation(
    row: Mapping[str, Any],
    prefix: str,
    expected_service_hash: str,
) -> None:
    """Bind one inventoried identity to the current service incarnation."""
    if row[f'{prefix}_service_hash'] != expected_service_hash:
        raise RuntimeError(
            f'Placement normalization {prefix} ledger service incarnation '
            'does not match the current service.')


def _validate_placement_normalization_pending_ledger_proof(
    row: Mapping[str, Any],
    prefix: str,
    expected_service_hash: str,
    lifecycle_epoch: int | None,
) -> None:
    """Validate a pending load against its exact normalization generation."""
    _validate_placement_normalization_ledger_result(row, prefix,
                                                    expected_service_hash)
    if row[f'{prefix}_service_lifecycle_epoch'] != lifecycle_epoch:
        raise RuntimeError(
            f'Placement normalization {prefix} ledger lifecycle epoch does '
            'not match the current service.')


def _validate_placement_normalization_completed_ledger_result(
    row: Mapping[str, Any],
    prefix: str,
    expected_service_hash: str,
    manifest_completed_at: float,
) -> None:
    """Validate immutable inventoried bytes after the receipt is complete."""
    classification = row[f'{prefix}_classification']
    outcome = row[f'{prefix}_outcome']
    ledger_outcome = (classification, outcome)
    if classification is None:
        created_at = row[f'{prefix}_created_at']
        if (isinstance(created_at, bool) or
                not isinstance(created_at, (int, float)) or
                not math.isfinite(float(created_at)) or
                created_at <= manifest_completed_at):
            raise RuntimeError(
                f'Placement normalization {prefix} version predates the '
                'requested run completion but has no ledger inventory row.')
        # A version created after the completed run is an ordinary later
        # version and need not appear in the old inventory.
        return
    identity, mode = _placement_normalization_manifest_identity(row)
    if placement_normalization_identity.is_fillable_manifest_outcome(
            identity, mode, *ledger_outcome):
        # A placeholder had no contract to load.  Filling it through the
        # v2-only version writer is an ordinary later commit, but the old
        # inventory identity must still belong to this service incarnation.
        _validate_placement_normalization_ledger_service_incarnation(
            row, prefix, expected_service_hash)
        return
    _validate_placement_normalization_ledger_result(row, prefix,
                                                    expected_service_hash)


def _validate_placement_normalization_completed_service_incarnation(
        row: Mapping[str, Any], expected_service_hash: str) -> None:
    """Require one completed run to inventory only this service incarnation."""
    if not isinstance(expected_service_hash, str) or not expected_service_hash:
        raise RuntimeError(
            'Placement normalization service incarnation is invalid.')
    anchor_count = row['service_ledger_anchor_count']
    matching_count = row['service_ledger_matching_hash_count']
    if (type(anchor_count) is not int or anchor_count < 1 or
            type(matching_count) is not int or matching_count != anchor_count):
        raise RuntimeError(
            'Placement normalization completed receipt has no exact '
            'service incarnation ledger anchor.')


def _validate_placement_normalization_loaded_receipt(
        row: Mapping[str, Any], requested_run_id: uuid.UUID | None,
        manifest_completed_at: float | None) -> None:
    loaded_values = (
        row['loaded_run_id'],
        row['loaded_image_commit'],
        row['loaded_controller_pid'],
        row['loaded_controller_ip'],
        row['loaded_boot_id'],
        row['loaded_at'],
    )
    loaded_run_id = loaded_values[0]
    if requested_run_id is None:
        if manifest_completed_at is not None:
            raise RuntimeError('Placement normalization completion exists '
                               'without a requested run.')
        if any(value is not None for value in loaded_values):
            raise RuntimeError('Placement normalization receipt exists '
                               'without a requested run.')
        return
    if manifest_completed_at is None:
        raise RuntimeError('Placement normalization requested run has no '
                           'validated completion timestamp.')
    if loaded_run_id is None:
        if any(value is not None for value in loaded_values[1:]):
            raise RuntimeError('Placement normalization loaded receipt is '
                               'only partially populated.')
        return
    loaded_commit, loaded_pid, loaded_ip, loaded_boot_id, loaded_at = (
        loaded_values[1:])
    if loaded_run_id != requested_run_id:
        raise RuntimeError('Placement normalization loaded receipt does not '
                           'match the requested run.')
    if not isinstance(loaded_commit, str) or not loaded_commit:
        raise RuntimeError('Placement normalization loaded receipt has no '
                           'image commit.')
    if type(loaded_pid) is not int or loaded_pid < 1:
        raise RuntimeError('Placement normalization loaded receipt has an '
                           'invalid controller PID.')
    if loaded_ip is not None and (not isinstance(loaded_ip, str) or
                                  not loaded_ip):
        raise RuntimeError('Placement normalization loaded receipt has an '
                           'invalid controller IP.')
    if (not isinstance(loaded_boot_id, str) or
            re.fullmatch(r'[0-9a-f]{32}', loaded_boot_id) is None):
        raise RuntimeError('Placement normalization loaded receipt has an '
                           'invalid boot ID.')
    if (isinstance(loaded_at, bool) or not isinstance(loaded_at,
                                                      (int, float)) or
            not math.isfinite(float(loaded_at)) or loaded_at < 0):
        raise RuntimeError('Placement normalization loaded receipt has an '
                           'invalid timestamp.')
    if float(loaded_at) < manifest_completed_at:
        raise RuntimeError('Placement normalization loaded receipt predates '
                           'its run completion.')


def _validated_placement_normalization_request(
    row: Mapping[str, Any],
    requested_run_id: uuid.UUID | None,
    manifest_completed_at: float | None,
    recovery_version: int,
    current_version: int,
    expected_service_hash: str,
) -> PlacementNormalizationRequest | None:
    """Finish a receipt decision while its database gate lock is held."""
    lifecycle_epoch = row['lifecycle_epoch']
    if (lifecycle_epoch is not None and
        (type(lifecycle_epoch) is not int or lifecycle_epoch < 1)):
        raise RuntimeError('Service lifecycle epoch is invalid.')
    require_cleanup_contract = requested_run_id is not None
    _validate_raw_explicit_placement_contract(
        row, 'recovery', require_cleanup_contract=require_cleanup_contract)
    _validate_raw_explicit_placement_contract(
        row, 'current', require_cleanup_contract=require_cleanup_contract)
    _validate_placement_normalization_loaded_receipt(row, requested_run_id,
                                                     manifest_completed_at)
    if requested_run_id is None:
        return None
    # The ledger proves the one forced post-normalization load.  A completed
    # receipt does not make an inventoried version mutable: when the requested
    # run still has a row for either loaded version, verify its exact bytes.
    if row['loaded_run_id'] == requested_run_id:
        assert manifest_completed_at is not None
        _validate_placement_normalization_completed_service_incarnation(
            row, expected_service_hash)
        for prefix in ('recovery', 'current'):
            _validate_placement_normalization_completed_ledger_result(
                row, prefix, expected_service_hash, manifest_completed_at)
        return None
    _validate_placement_normalization_pending_ledger_proof(
        row, 'recovery', expected_service_hash, lifecycle_epoch)
    _validate_placement_normalization_pending_ledger_proof(
        row, 'current', expected_service_hash, lifecycle_epoch)
    return PlacementNormalizationRequest(
        run_id=requested_run_id,
        recovery_version=recovery_version,
        current_version=current_version,
        lifecycle_epoch=lifecycle_epoch,
    )


def get_placement_normalization_request(
    service_name: str,
    recovery_version: int,
    current_version: int,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
) -> PlacementNormalizationRequest | None:
    """Read the normalization generation owned by one exact controller.

    ``None`` means the exact owner/version generation has no requested reload.
    A requested run is returned only when its per-version ledger proves that
    both raw persisted specs are explicit loadable results.  A missing proof,
    ownership change, or version change fails instead of looking like "no
    request".
    """
    if not isinstance(service_name, str) or not service_name:
        raise ValueError('Service name must be a non-empty string.')
    if not isinstance(expected_service_hash, str) or not expected_service_hash:
        raise ValueError('Service hash must be a non-empty string.')
    if (type(recovery_version) is not int or recovery_version < 1 or
            type(current_version) is not int or current_version < 1):
        raise ValueError('Recovery and current versions must be positive '
                         'integers.')
    expected_pid, expected_ip = expected_controller_owner
    if type(expected_pid) is not int or expected_pid < 1:
        raise ValueError('Controller owner PID must be a positive integer.')
    if (expected_ip is not None and
        (not isinstance(expected_ip, str) or not expected_ip)):
        raise ValueError('Controller owner IP must be a non-empty string or '
                         'None.')

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _bind_placement_normalization_receipt_authority(session, engine)
        row = session.execute(
            _placement_normalization_receipt_query(
                service_name,
                recovery_version,
                current_version,
                expected_service_hash,
                expected_controller_owner,
                require_ledger=False)).mappings().one_or_none()
        if row is None:
            raise RuntimeError(
                'Placement normalization receipt read lost its exact service '
                'owner, recovery-version, or current-version fence.')
        requested_run_id = row['requested_run_id']
        if requested_run_id is not None and not isinstance(
                requested_run_id, uuid.UUID):
            raise RuntimeError('Placement normalization requested run ID is '
                               'not a UUID.')
        manifest_completed_at: float | None = None
        if requested_run_id is not None:
            manifest_completed_at = (
                _validate_placement_normalization_run_manifest(
                    row, requested_run_id))
            _load_and_validate_placement_normalization_manifest(
                session, requested_run_id)
        return _validated_placement_normalization_request(
            row, requested_run_id, manifest_completed_at, recovery_version,
            current_version, expected_service_hash)


def acknowledge_placement_normalization_loaded(
    service_name: str,
    request: PlacementNormalizationRequest,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    image_commit: str,
    child_controller_pid: int,
    boot_id: str,
    loaded_at: float | None = None,
) -> bool:
    """CAS the one-time controller-load receipt for a pending generation."""
    if not isinstance(request, PlacementNormalizationRequest):
        raise TypeError('request must be a PlacementNormalizationRequest.')
    if not isinstance(request.run_id, uuid.UUID):
        raise ValueError('Requested normalization run ID must be a UUID.')
    if (type(request.recovery_version) is not int or
            request.recovery_version < 1 or
            type(request.current_version) is not int or
            request.current_version < 1):
        raise ValueError('Requested recovery and current versions must be '
                         'positive integers.')
    if (request.lifecycle_epoch is not None and
        (type(request.lifecycle_epoch) is not int or
         request.lifecycle_epoch < 1)):
        raise ValueError('Requested lifecycle epoch must be a positive '
                         'integer or None.')
    if not isinstance(service_name, str) or not service_name:
        raise ValueError('Service name must be a non-empty string.')
    if not isinstance(expected_service_hash, str) or not expected_service_hash:
        raise ValueError('Service hash must be a non-empty string.')
    expected_pid, expected_ip = expected_controller_owner
    if type(expected_pid) is not int or expected_pid < 1:
        raise ValueError('Controller owner PID must be a positive integer.')
    if (expected_ip is not None and
        (not isinstance(expected_ip, str) or not expected_ip)):
        raise ValueError('Controller owner IP must be a non-empty string or '
                         'None.')
    if not isinstance(image_commit, str) or not image_commit:
        raise ValueError('Image commit must be a non-empty string.')
    if type(child_controller_pid) is not int or child_controller_pid < 1:
        raise ValueError('Child controller PID must be a positive integer.')
    if (not isinstance(boot_id, str) or
            re.fullmatch(r'[0-9a-f]{32}', boot_id) is None):
        raise ValueError('Controller boot ID must be 32 lowercase hexadecimal '
                         'characters.')
    if loaded_at is None:
        loaded_at = time.time()
    if (isinstance(loaded_at, bool) or not isinstance(loaded_at,
                                                      (int, float)) or
            not math.isfinite(float(loaded_at)) or loaded_at < 0):
        raise ValueError('Loaded-at timestamp must be a finite nonnegative '
                         'number.')

    with _replica_launch_authority_write_session(service_name) as (engine,
                                                                   session):
        _begin_immediate_if_sqlite(session, engine)
        _bind_placement_normalization_receipt_authority(session, engine)
        query = _placement_normalization_receipt_query(
            service_name,
            request.recovery_version,
            request.current_version,
            expected_service_hash,
            expected_controller_owner,
            require_ledger=False).where(
                services_table.c.placement_normalization_requested_run_id ==
                request.run_id)
        query = _lock_placement_normalization_receipt_query(query, engine)
        row = session.execute(query).mappings().one_or_none()
        if row is None or row['lifecycle_epoch'] != request.lifecycle_epoch:
            session.rollback()
            return False
        _validate_raw_explicit_placement_contract(row,
                                                  'recovery',
                                                  require_cleanup_contract=True)
        _validate_raw_explicit_placement_contract(row,
                                                  'current',
                                                  require_cleanup_contract=True)
        manifest_completed_at = _validate_placement_normalization_run_manifest(
            row, request.run_id)
        _load_and_validate_placement_normalization_manifest(
            session, request.run_id)
        _validate_placement_normalization_loaded_receipt(
            row, request.run_id, manifest_completed_at)
        if row['loaded_run_id'] is not None:
            # A completed receipt is immutable evidence.  Even an exact retry
            # must not refresh its process identity or observation timestamp.
            session.rollback()
            return False
        _validate_placement_normalization_pending_ledger_proof(
            row, 'recovery', expected_service_hash, request.lifecycle_epoch)
        _validate_placement_normalization_pending_ledger_proof(
            row, 'current', expected_service_hash, request.lifecycle_epoch)
        if float(loaded_at) < manifest_completed_at:
            raise RuntimeError('Placement normalization load timestamp '
                               'predates its run completion.')
        predicates = [
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == expected_pid,
            services_table.c.controller_ip == expected_ip,
            services_table.c.current_version == request.current_version,
            services_table.c.placement_normalization_requested_run_id ==
            request.run_id,
            services_table.c.placement_normalization_loaded_run_id.is_(None),
            services_table.c.placement_normalization_loaded_image_commit.is_(
                None),
            services_table.c.placement_normalization_loaded_controller_pid.is_(
                None),
            services_table.c.placement_normalization_loaded_controller_ip.is_(
                None),
            services_table.c.placement_normalization_loaded_boot_id.is_(None),
            services_table.c.placement_normalization_loaded_at.is_(None),
        ]
        if request.lifecycle_epoch is not None:
            predicates.append(
                services_table.c.lifecycle_epoch == request.lifecycle_epoch)
        result = session.execute(
            sqlalchemy.update(services_table).where(*predicates).values(
                placement_normalization_loaded_run_id=request.run_id,
                placement_normalization_loaded_image_commit=image_commit,
                placement_normalization_loaded_controller_pid=
                child_controller_pid,
                placement_normalization_loaded_controller_ip=expected_ip,
                placement_normalization_loaded_boot_id=boot_id,
                placement_normalization_loaded_at=float(loaded_at)))
        if result.rowcount != 1:
            session.rollback()
            return False
        session.commit()
    return True


def quarantine_version(
    service_name: str,
    version: int,
    reason: str,
    quarantined_at: float | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Durably make one committed version ineligible for application."""
    if quarantined_at is None:
        quarantined_at = time.time()
    with _replica_launch_authority_write_session(service_name) as (_, session):
        predicates = [
            version_specs_table.c.service_name == service_name,
            version_specs_table.c.version == version,
            version_specs_table.c.yaml_content.isnot(None),
            version_specs_table.c.quarantined_at.is_(None),
        ]
        if (expected_service_hash is not None or
                expected_controller_owner is not None):
            owner_predicates = [services_table.c.name == service_name]
            if expected_service_hash is not None:
                owner_predicates.append(
                    services_table.c.hash == expected_service_hash)
            if expected_controller_owner is not None:
                expected_pid, expected_ip = expected_controller_owner
                owner_predicates.extend([
                    services_table.c.controller_pid == expected_pid,
                    services_table.c.controller_ip == expected_ip,
                ])
            predicates.append(sqlalchemy.exists().where(*owner_predicates))
        result = session.execute(
            sqlalchemy.update(version_specs_table).where(*predicates).values(
                quarantined_at=quarantined_at, quarantine_reason=reason))
        if result.rowcount == 0:
            if (expected_service_hash is not None or
                    expected_controller_owner is not None):
                owner = session.execute(
                    sqlalchemy.select(
                        services_table.c.hash,
                        services_table.c.controller_pid,
                        services_table.c.controller_ip,
                    ).where(services_table.c.name == service_name)).fetchone()
                if (owner is None or (expected_service_hash is not None and
                                      owner.hash != expected_service_hash) or
                    (expected_controller_owner is not None and
                     (owner.controller_pid, owner.controller_ip)
                     != expected_controller_owner)):
                    session.rollback()
                    return False
            existing = session.execute(
                sqlalchemy.select(version_specs_table.c.quarantined_at).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version,
                    version_specs_table.c.yaml_content.isnot(None))).fetchone()
            if existing is None or existing.quarantined_at is None:
                session.rollback()
                return False
        session.commit()
    return True


def get_latest_quarantined_version(service_name: str) -> dict[str, Any] | None:
    """Return the newest quarantined version's durable diagnostics."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.quarantined_at,
                version_specs_table.c.quarantine_reason,
            ).where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.quarantined_at.isnot(None),
            ).order_by(
                version_specs_table.c.version.desc()).limit(1)).fetchone()
    if row is None:
        return None
    return {
        'version': row.version,
        'quarantined_at': row.quarantined_at,
        'quarantine_reason': row.quarantine_reason,
    }


def get_yaml_content(service_name: str, version: int) -> str | None:
    """Gets the yaml content of a version."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(version_specs_table.c.yaml_content).where(
                sqlalchemy.and_(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version))).fetchone()
    return result[0] if result else None


def get_submitted_yaml_content(service_name: str, version: int) -> str | None:
    """Gets the user-submitted YAML retained for a version."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(
                version_specs_table.c.submitted_yaml_content).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.version == version))).fetchone()
    return result[0] if result else None


def delete_all_versions(service_name: str) -> None:
    """Deletes all versions from the database."""
    with _replica_launch_authority_write_session(service_name) as (_, session):
        session.execute(
            sqlalchemy.delete(version_specs_table).where(
                version_specs_table.c.service_name == service_name))
        session.commit()


def get_latest_version(service_name: str) -> int | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(sqlalchemy.func.max(
                version_specs_table.c.version)).where(
                    version_specs_table.c.service_name ==
                    service_name)).fetchone()
    return result[0] if result else None


def get_latest_committed_version(service_name: str) -> int | None:
    """Returns the latest version whose yaml was fully committed.

    `add_version` inserts a protocol-4 placeholder row
    (`spec=pickle.dumps(None, protocol=4)`, `yaml_content=NULL`) and only later
    does `add_or_update_version` fill in the real spec/yaml. A restart in that
    window can leave such a placeholder as
    MAX(version). Recovery must skip it and resume the latest version that
    actually has its yaml persisted -- booting a controller at a NULL-yaml
    version crash-loops it (SkyPilotReplicaManager asserts yaml is not None).
    Returns None if no version has committed yaml yet.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(sqlalchemy.func.max(
                version_specs_table.c.version)).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.yaml_content.isnot(
                            None)))).fetchone()
    return result[0] if result else None


def get_latest_committed_versions(service_names: list[str]) -> dict[str, int]:
    """Return the latest committed version for each requested service."""
    if not service_names:
        return {}
    names = sorted(set(service_names))
    engine = _db_manager.get_engine()
    rows = []
    with orm.Session(engine) as session:
        for start in range(0, len(names), _TERMINAL_IDENTITY_QUERY_BATCH_SIZE):
            name_batch = names[start:start +
                               _TERMINAL_IDENTITY_QUERY_BATCH_SIZE]
            rows.extend(
                session.execute(
                    sqlalchemy.select(
                        version_specs_table.c.service_name,
                        sqlalchemy.func.max(
                            version_specs_table.c.version).label('version'),
                    ).where(
                        sqlalchemy.and_(
                            version_specs_table.c.service_name.in_(name_batch),
                            version_specs_table.c.yaml_content.isnot(None),
                        )).group_by(
                            version_specs_table.c.service_name)).fetchall())
    return {row.service_name: int(row.version) for row in rows}


def get_latest_committed_version_spec(
        service_name: str) -> tuple[int, 'service_spec.SkyServiceSpec'] | None:
    """Returns the latest committed version and spec from one row snapshot.

    A controller-child respawn must not pair a version selected in one
    transaction with a spec fetched in another.  In particular, if the spec
    row disappears between those reads, falling back to the parent loop's
    captured version can resurrect stale configuration after an update.

    Returns None when no row has committed YAML or its spec is unusable.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.spec).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.yaml_content.isnot(None))).
            order_by(version_specs_table.c.version.desc()).limit(1)).fetchone()
    if result is None:
        return None
    spec = pickle.loads(result[1])
    if spec is None:
        return None
    return result[0], spec


def get_latest_applicable_version_spec(
        service_name: str) -> tuple[int, 'service_spec.SkyServiceSpec'] | None:
    """Return the newest committed version not durably quarantined."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.spec).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.yaml_content.isnot(None),
                        version_specs_table.c.quarantined_at.is_(None))).
            order_by(version_specs_table.c.version.desc()).limit(1)).fetchone()
    if result is None:
        return None
    spec = pickle.loads(result[1])
    if spec is None:
        return None
    return result[0], spec


def get_recovery_version_spec(
        service_name: str) -> tuple[int, 'service_spec.SkyServiceSpec'] | None:
    """Return the safest committed version for controller reconstruction.

    Normally this is the newest non-quarantined commit. If a newer version was
    quarantined after runtime mutation, however, an unproven intermediate
    commit may sit between it and the version the controller actually applied.
    Recovery must prefer the newest durably applied, non-quarantined version in
    that case. This receipt survives scale-to-zero; active routing versions do
    not. A commit newer than the quarantine still supersedes it.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.spec,
                version_specs_table.c.quarantined_at,
                version_specs_table.c.controller_applied_at,
            ).where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.yaml_content.isnot(None),
            ).order_by(version_specs_table.c.version.desc())).fetchall()
    applicable = next((row for row in rows if row.quarantined_at is None), None)
    quarantined_version = next(
        (row.version for row in rows if row.quarantined_at is not None), None)
    applied = next(
        (row for row in rows
         if row.quarantined_at is None and row.controller_applied_at is not None
        ), None)
    selected_version = _select_quarantine_aware_version(
        None if applicable is None else applicable.version, quarantined_version,
        None if applied is None else applied.version)
    applicable = next((row for row in rows if row.version == selected_version),
                      None)
    if applicable is None:
        return None
    spec = pickle.loads(applicable.spec)
    if spec is None:
        return None
    return applicable.version, spec


def get_ha_recovery_script(service_name: str) -> str | None:
    """Gets the HA recovery script for a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(serve_ha_recovery_script_table.c.script).where(
                serve_ha_recovery_script_table.c.service_name ==
                service_name)).fetchone()
    return result[0] if result else None


def set_ha_recovery_script(service_name: str,
                           script: str,
                           expected_lifecycle_epoch: int | None = None) -> bool:
    """Set the recovery script only for the current lifecycle epoch."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, expected_lifecycle_epoch
                                   is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmt = insert_func(serve_ha_recovery_script_table).values(
            service_name=service_name, script=script)

        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['service_name'],
            set_={'script': insert_stmt.excluded.script})

        session.execute(insert_stmt)
        session.commit()
    return True


def remove_ha_recovery_script(service_name: str) -> None:
    """Removes the HA recovery script for a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(serve_ha_recovery_script_table).where(
                serve_ha_recovery_script_table.c.service_name == service_name))
        session.commit()


def remove_ha_recovery_script_if_owner(
        service_name: str, expected_service_hash: str,
        expected_controller_pid: int | None,
        expected_controller_ip: str | None) -> bool:
    """Delete a recovery script only while the exact controller owns DB."""
    owner_exists = sqlalchemy.exists().where(
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
        services_table.c.controller_pid == expected_controller_pid,
        services_table.c.controller_ip == expected_controller_ip)
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.delete(serve_ha_recovery_script_table).where(
                serve_ha_recovery_script_table.c.service_name == service_name,
                owner_exists))
        session.commit()
    return result.rowcount > 0


# === Reserved-fill broker state (see sky/serve/reserved_capacity_broker.py).
# These functions own ALL SQL for the broker tables; the broker module owns
# the allocation logic. Multi-statement functions run in one session so each
# is a single transaction.


def _upsert_insert_func(engine: sqlalchemy.engine.Engine):
    """Dialect-specific INSERT with ON CONFLICT support."""
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return sqlite.insert
    if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return postgresql.insert
    raise ValueError('Unsupported database dialect')


def _require_reserved_fill_v2_postgresql(
        engine: sqlalchemy.engine.Engine) -> None:
    """Require central PostgreSQL for normalized protocol-v2 state."""
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('Reserved-fill protocol v2 requires the central '
                           'PostgreSQL Serve database.')


def get_recent_reserved_fill_writer_instances(
        stale_after_seconds: int) -> tuple[ReservedFillWriterInstance, ...]:
    """Return database-wide live leases for fill request-server roles."""
    if (isinstance(stale_after_seconds, bool) or
            not isinstance(stale_after_seconds, int) or
            stale_after_seconds <= 0):
        raise ValueError('Writer-instance stale horizon must be positive.')
    engine = _db_manager.get_engine()
    _require_reserved_fill_v2_postgresql(engine)
    cutoff = (sqlalchemy.func.clock_timestamp() -
              datetime.timedelta(seconds=stale_after_seconds))
    statement = sqlalchemy.select(
        request_postgres_schema.SERVER_INSTANCES.c.instance_id,
        request_postgres_schema.SERVER_INSTANCES.c.role,
        request_postgres_schema.SERVER_INSTANCES.c.pod_name,
        request_postgres_schema.SERVER_INSTANCES.c.pod_uid,
        request_postgres_schema.SERVER_INSTANCES.c.version,
        request_postgres_schema.SERVER_INSTANCES.c.ready,
        request_postgres_schema.SERVER_INSTANCES.c.draining_at,
        request_postgres_schema.SERVER_INSTANCES.c.request_storage_backend,
        request_postgres_schema.SERVER_INSTANCES.c.request_queue_backend,
        request_postgres_schema.SERVER_INSTANCES.c.execution_quiescence_capable,
    ).where(
        request_postgres_schema.SERVER_INSTANCES.c.role.in_(
            ('all', 'api', 'controller', 'executor')),
        request_postgres_schema.SERVER_INSTANCES.c.heartbeat_at >= cutoff)
    try:
        with engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
    except sqlalchemy_exc.SQLAlchemyError as error:
        raise RuntimeError(
            'The live writer-process inventory could not be read.') from error
    result = []
    for row in rows:
        instance_id = str(row['instance_id'])
        role = row['role']
        version = row['version']
        ready = row['ready']
        request_storage_backend = row['request_storage_backend']
        request_queue_backend = row['request_queue_backend']
        execution_quiescence_capable = row['execution_quiescence_capable']
        if (not instance_id or not isinstance(role, str) or not role or
                not isinstance(version, str) or not version or
                not isinstance(ready, bool) or
                not isinstance(request_storage_backend, str) or
                not request_storage_backend or
                not isinstance(request_queue_backend, str) or
                not request_queue_backend or
                not isinstance(execution_quiescence_capable, bool)):
            raise RuntimeError(
                'A live writer-process inventory row is malformed.')
        result.append(
            ReservedFillWriterInstance(
                instance_id=instance_id,
                role=role,
                pod_name=row['pod_name'],
                pod_uid=row['pod_uid'],
                version=version,
                ready=ready,
                draining=row['draining_at'] is not None,
                request_storage_backend=(request_storage_backend),
                request_queue_backend=(request_queue_backend),
                execution_quiescence_capable=(execution_quiescence_capable)))
    return tuple(
        sorted(result,
               key=lambda item:
               (item.role, item.pod_uid or '', item.instance_id)))


def _reserved_fill_protocol_row_in_session(
        session: orm.Session | sqlalchemy.engine.Connection,
        engine: sqlalchemy.engine.Engine,
        *,
        for_update: bool = False) -> sqlalchemy.engine.Row:
    """Return the singleton protocol row, seeding v1 for metadata-only DBs."""
    insert_stmt = _upsert_insert_func(engine)(
        reserved_fill_protocol_state_table).values(
            id=1,
            protocol_version=RESERVED_FILL_PROTOCOL_V1,
            claim_generation=0,
            changed_at=0.0)
    insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['id'])
    session.execute(insert_stmt)
    query = sqlalchemy.select(reserved_fill_protocol_state_table).where(
        reserved_fill_protocol_state_table.c.id == 1)
    if for_update:
        query = query.with_for_update()
    row = session.execute(query).fetchone()
    if row is None:
        raise RuntimeError('Reserved-fill protocol singleton is missing.')
    return row


def _next_reserved_fill_claim_generation_in_session(
        session: orm.Session, protocol_row: sqlalchemy.engine.Row) -> int:
    """Allocate one globally unique protocol-v2 claim generation.

    The caller must have selected the protocol singleton ``FOR UPDATE`` (or
    opened SQLite's immediate transaction).  The singleton outlives every
    service claim set, making the allocated value immune to disable/re-enable
    and same-name service reuse ABA.
    """
    previous = int(protocol_row.claim_generation)
    if previous < 0:
        raise RuntimeError('Reserved-fill claim generation is negative.')
    # PostgreSQL BIGINT's positive range is the durable wire contract.
    if previous >= 2**63 - 1:
        raise RuntimeError('Reserved-fill claim generation is exhausted.')
    generation = previous + 1
    updated = session.execute(
        sqlalchemy.update(reserved_fill_protocol_state_table).where(
            reserved_fill_protocol_state_table.c.id == 1,
            reserved_fill_protocol_state_table.c.claim_generation ==
            previous).values(claim_generation=generation))
    if updated.rowcount != 1:
        raise RuntimeError('Reserved-fill claim generation allocation lost '
                           'its singleton fence.')
    return generation


def get_reserved_fill_protocol_state() -> dict[str, Any]:
    """Return the durable reserved-fill protocol and its rollout proof."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = _reserved_fill_protocol_row_in_session(session, engine)
        result = dict(row._mapping)  # pylint: disable=protected-access
        session.commit()
    return result


def _canonical_reserved_fill_accelerators(value: Any) -> tuple[str, ...] | None:
    """Return one canonical accelerator tuple, or ``None`` when malformed."""
    if isinstance(value, str):
        raw_names = (value,)
    elif isinstance(value, list):
        raw_names = tuple(value)
    else:
        return None
    if not raw_names or any(
            not isinstance(name, str) or not name or name != name.strip()
            for name in raw_names):
        return None
    return tuple(sorted({name.lower() for name in raw_names}))


def _demotion_legacy_projection(
    claim_set: sqlalchemy.engine.Row,
    edge: sqlalchemy.engine.Row,
    *,
    global_generation: int,
) -> dict[str, Any] | None:
    """Validate one authoritative edge and derive its complete v1 projection."""
    try:
        generation = int(claim_set.generation)
        edge_generation = int(edge.service_generation)
        edge_count = int(claim_set.edge_count)
        pool_position = int(edge.pool_position)
        weight = float(edge.weight)
        floor_replicas = int(edge.floor_replicas)
        gpus_per_replica = int(edge.gpus_per_replica)
        holdings_fill = int(edge.holdings_fill)
        effective_cap = (None if edge.effective_cap is None else int(
            edge.effective_cap))
        launchable = int(edge.launchable)
        heartbeat_ts = float(edge.heartbeat_ts)
        encoded_accelerators = json.loads(edge.accelerator_names)
        legacy_pool_key = json.loads(edge.legacy_pool_key)
        physical_pool_key = json.loads(edge.pool_key)
    except (AttributeError, TypeError, ValueError):
        return None
    accelerators = _canonical_reserved_fill_accelerators(encoded_accelerators)
    legacy_accelerators = (_canonical_reserved_fill_accelerators(
        legacy_pool_key[1]) if isinstance(legacy_pool_key, list) and
                           len(legacy_pool_key) == 2 else None)
    physical_accelerators = (_canonical_reserved_fill_accelerators(
        physical_pool_key[2]) if isinstance(physical_pool_key, list) and
                             len(physical_pool_key) == 3 and
                             physical_pool_key[0] == 'v2' else None)
    if (generation <= 0 or generation > global_generation or
            edge_generation != generation or edge_count != 1 or
            not isinstance(claim_set.semantic_hash, str) or
            not claim_set.semantic_hash or pool_position < 0 or
            not math.isfinite(weight) or weight <= 0 or floor_replicas < 0 or
            gpus_per_replica <= 0 or holdings_fill < 0 or
        (effective_cap is not None and effective_cap < 0) or
            launchable not in (0, 1) or not math.isfinite(heartbeat_ts) or
            heartbeat_ts < 0 or accelerators is None or
            legacy_accelerators != accelerators or
            physical_accelerators != accelerators or
            legacy_pool_key[0] != edge.access_context or
            physical_pool_key[1] != edge.physical_cluster_uid or
            not isinstance(edge.access_context, str) or
            not edge.access_context or
            not isinstance(edge.physical_cluster_uid, str) or
            not edge.physical_cluster_uid or
            edge.demonstrated_need is not None or edge.boot_hold is not None or
            edge.activity_ts is not None):
        return None
    return {
        'legacy_pool_key': edge.legacy_pool_key,
        'weight': weight,
        'floor_replicas': floor_replicas,
        'gpus_per_replica': gpus_per_replica,
        'holdings_fill': holdings_fill,
        'effective_cap': effective_cap,
        'launchable': launchable,
        'demonstrated_need': None,
        'boot_hold': None,
        'activity_ts': None,
        'heartbeat_ts': heartbeat_ts,
    }


def _legacy_projection_matches(row: sqlalchemy.engine.Row,
                               projection: dict[str, Any]) -> bool:
    """Whether a persisted legacy row is exactly the rebuilt projection."""
    expected = dict(projection)
    expected['pool_key'] = expected.pop('legacy_pool_key')
    return all(row._mapping[column] == value  # pylint: disable=protected-access
               for column, value in expected.items())


def set_reserved_fill_protocol_version(
    protocol_version: int,
    *,
    expected_protocol_version: int,
    image_digest: str | None = None,
    deployment_generation: str | None = None,
    deployment_uid: str | None = None,
    pod_inventory_count: int | None = None,
    pod_inventory_sha256: str | None = None,
    changed_at: float | None = None,
    rollout_proof: dict[str, Any] | None = None,
) -> bool:
    """CAS the durable protocol gate after the caller takes the broker lock.

    The state layer records and validates the immutable rollout proof.  The
    activation command is responsible for collecting that proof from the
    fully rolled-out Deployment while holding the exact global broker lock.
    Demotion fails closed while any authoritative service still owns multiple
    edges.  Queued launches are fenced separately by
    ``add_replica_if_round_epoch`` comparing their carried protocol with this
    row in the insert transaction.
    """
    if protocol_version not in (RESERVED_FILL_PROTOCOL_V1,
                                RESERVED_FILL_PROTOCOL_V2):
        raise ValueError('Reserved-fill protocol must be 1 or 2.')
    if expected_protocol_version not in (RESERVED_FILL_PROTOCOL_V1,
                                         RESERVED_FILL_PROTOCOL_V2):
        raise ValueError('Expected reserved-fill protocol must be 1 or 2.')
    if protocol_version == expected_protocol_version:
        raise ValueError('Reserved-fill protocol transition must change it.')
    if rollout_proof is not None:
        if not isinstance(rollout_proof, dict):
            raise TypeError('Reserved-fill rollout_proof must be a mapping.')
        if (image_digest is not None or deployment_generation is not None or
                deployment_uid is not None or pod_inventory_count is not None or
                pod_inventory_sha256 is not None):
            raise ValueError('Pass rollout proof either as fields or as one '
                             'mapping, not both.')
        image_digest = rollout_proof.get(
            'image_digest', rollout_proof.get('expected_image_digest'))
        observed_digest = rollout_proof.get('observed_image_digest',
                                            image_digest)
        if observed_digest != image_digest:
            raise ValueError('Expected and observed rollout image digests do '
                             'not match.')
        raw_generation = rollout_proof.get('deployment_generation')
        if raw_generation is not None and not isinstance(raw_generation, bool):
            deployment_generation = str(raw_generation)
        deployment_uid = rollout_proof.get('deployment_uid')
        pod_inventory_count = rollout_proof.get('pod_inventory_count')
        pod_inventory_sha256 = rollout_proof.get('pod_inventory_sha256')
    if protocol_version == RESERVED_FILL_PROTOCOL_V2:
        if (not isinstance(image_digest, str) or len(image_digest) != 71 or
                not image_digest.startswith('sha256:') or
                any(character not in '0123456789abcdef'
                    for character in image_digest[7:])):
            raise ValueError('Protocol-v2 activation requires a sha256 image '
                             'digest.')
        if (not isinstance(deployment_generation, str) or
                not deployment_generation.strip()):
            raise ValueError('Protocol-v2 activation requires a Deployment '
                             'generation proof.')
        if not isinstance(deployment_uid, str) or not deployment_uid:
            raise ValueError('Protocol-v2 activation requires a Deployment '
                             'UID proof.')
        if (isinstance(pod_inventory_count, bool) or
                not isinstance(pod_inventory_count, int) or
                pod_inventory_count <= 0):
            raise ValueError('Protocol-v2 activation requires a positive pod '
                             'inventory count.')
        if (not isinstance(pod_inventory_sha256, str) or
                len(pod_inventory_sha256) != 64 or
                any(character not in '0123456789abcdef'
                    for character in pod_inventory_sha256)):
            raise ValueError('Protocol-v2 activation requires a pod inventory '
                             'sha256 proof.')
    transition_time = time.time() if changed_at is None else float(changed_at)
    engine = _db_manager.get_engine()
    _require_reserved_fill_v2_postgresql(engine)
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine)
        row = _reserved_fill_protocol_row_in_session(session,
                                                     engine,
                                                     for_update=True)
        if int(row.protocol_version) != expected_protocol_version:
            session.rollback()
            return False
        if protocol_version == RESERVED_FILL_PROTOCOL_V1:
            # The table lock closes the PostgreSQL predicate gap: a v1 writer
            # that does not know about the protocol singleton cannot insert a
            # new legacy-only row after the inventory check and before the gate
            # flip.  Current v2 writers also take the singleton row first, so
            # this preserves their existing lock order.
            session.execute(
                sqlalchemy.text(
                    'LOCK TABLE reserved_fill_service_claim_sets, '
                    'reserved_fill_pool_claims, reserved_fill_claims '
                    'IN SHARE ROW EXCLUSIVE MODE'))
            authoritative_sets = session.execute(
                sqlalchemy.select(reserved_fill_service_claim_sets_table).where(
                    reserved_fill_service_claim_sets_table.c.claim_set_state ==
                    RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2).with_for_update()
            ).fetchall()
            service_names = [
                str(item.service_name) for item in authoritative_sets
            ]
            normalized_rows = []
            if service_names:
                normalized_rows = session.execute(
                    sqlalchemy.select(reserved_fill_pool_claims_table).where(
                        reserved_fill_pool_claims_table.c.service_name.in_(
                            service_names)).with_for_update()).fetchall()
            legacy_rows = session.execute(
                sqlalchemy.select(
                    reserved_fill_claims_table).with_for_update()).fetchall()
            normalized_by_service: dict[str, list[Any]] = (
                collections.defaultdict(list))
            for normalized in normalized_rows:
                normalized_by_service[str(
                    normalized.service_name)].append(normalized)
            legacy_by_service = {
                str(legacy.service_name): legacy for legacy in legacy_rows
            }
            authoritative_names = set(service_names)
            if set(legacy_by_service) - authoritative_names:
                # A legacy-only row would become authoritative at the v1 gate
                # flip but has no normalized source from which this transaction
                # can prove and rebuild it.
                session.rollback()
                return False
            projections: dict[str, dict[str, Any]] = {}
            for claim_set in authoritative_sets:
                name = str(claim_set.service_name)
                edges = normalized_by_service.get(name, [])
                if len(edges) != 1:
                    session.rollback()
                    return False
                projection = _demotion_legacy_projection(
                    claim_set,
                    edges[0],
                    global_generation=int(row.claim_generation))
                if projection is None:
                    session.rollback()
                    return False
                projections[name] = projection
            # Rebuild every projection even when a legacy writer moved or
            # partially corrupted the old row while v2 remained authoritative.
            for name, projection in projections.items():
                _write_reserved_fill_legacy_projection_in_session(
                    session, engine, name, projection)
            rebuilt_rows = session.execute(
                sqlalchemy.select(
                    reserved_fill_claims_table).with_for_update()).fetchall()
            rebuilt_by_service = {
                str(legacy.service_name): legacy for legacy in rebuilt_rows
            }
            if (set(rebuilt_by_service) != authoritative_names or
                    any(not _legacy_projection_matches(rebuilt_by_service[name],
                                                       projection)
                        for name, projection in projections.items())):
                session.rollback()
                return False
        updated = session.execute(
            sqlalchemy.update(reserved_fill_protocol_state_table).where(
                reserved_fill_protocol_state_table.c.id == 1,
                reserved_fill_protocol_state_table.c.protocol_version ==
                expected_protocol_version).values(
                    protocol_version=protocol_version,
                    image_digest=(image_digest if image_digest is not None else
                                  row.image_digest),
                    deployment_generation=(deployment_generation
                                           if deployment_generation is not None
                                           else row.deployment_generation),
                    deployment_uid=(deployment_uid if deployment_uid is not None
                                    else row.deployment_uid),
                    pod_inventory_count=(pod_inventory_count
                                         if pod_inventory_count is not None else
                                         row.pod_inventory_count),
                    pod_inventory_sha256=(pod_inventory_sha256
                                          if pod_inventory_sha256 is not None
                                          else row.pod_inventory_sha256),
                    changed_at=transition_time))
        if updated.rowcount != 1:
            session.rollback()
            return False
        session.commit()
    return True


def _reserved_fill_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        # Validate caller-provided serialized state before persisting it.
        json.loads(value)
        return value
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _reserved_fill_decoded_json(value: Any) -> Any:
    if value is None or not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _decode_reserved_fill_projection_digest_map(
    value: Any,
    accelerator_names: Any,
) -> dict[str, str]:
    """Decode one closed, case-folded worker-projection digest map."""
    value = _reserved_fill_decoded_json(value)
    accelerator_names = _reserved_fill_decoded_json(accelerator_names)
    if (not isinstance(value, Mapping) or not value or
            not isinstance(accelerator_names,
                           (list, tuple)) or not accelerator_names):
        raise ValueError('Reserved-fill worker projection digest authority is '
                         'not a nonempty closed map.')
    expected_cards: set[str] = set()
    for raw_card in accelerator_names:
        if not isinstance(raw_card, str) or not raw_card:
            raise ValueError('Reserved-fill accelerator authority is '
                             'malformed.')
        card = raw_card.casefold()
        if card in expected_cards:
            raise ValueError('Reserved-fill accelerator authority contains a '
                             'case-folded duplicate.')
        expected_cards.add(card)
    decoded: dict[str, str] = {}
    for raw_card, raw_digest in value.items():
        if (not isinstance(raw_card, str) or not raw_card or
                not isinstance(raw_digest, str) or
                re.fullmatch(r'[0-9a-f]{64}', raw_digest) is None):
            raise ValueError('Reserved-fill worker projection digest '
                             'authority contains a malformed entry.')
        card = raw_card.casefold()
        if card in decoded:
            raise ValueError('Reserved-fill worker projection digest '
                             'authority contains a case-folded duplicate.')
        decoded[card] = raw_digest
    if set(decoded) != expected_cards:
        raise ValueError('Reserved-fill worker projection digest authority '
                         'does not exactly cover its accelerators.')
    return dict(sorted(decoded.items()))


def _clear_reserved_fill_allocation_in_session(session: orm.Session,
                                               service_name: str) -> None:
    """Clear planner authority whenever its claim generation changes."""
    allocation_table = (
        pool_capacity_observation_schema.reserved_fill_service_allocation_table)
    result = session.execute(
        sqlalchemy.update(allocation_table).where(
            allocation_table.c.service_name == service_name).values(
                allocation_generation=0,
                allocation_input_sha256=None,
                allocation_claim_generation=None,
                allocation_map=None,
                allocation_published_at=None,
                allocation_gate_generation=None))
    if result.rowcount != 1:
        raise RuntimeError('Reserved-fill claim set lost its allocation '
                           'projection while clearing stale authority.')


def _normalize_reserved_fill_pool_edge(edge: dict[str, Any],
                                       heartbeat_ts: float) -> dict[str, Any]:
    """Validate and normalize one complete-set edge for durable storage."""
    required_text = ('pool_key', 'legacy_pool_key', 'access_context',
                     'physical_cluster_uid')
    normalized = dict(edge)
    for name in required_text:
        value = normalized.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f'Reserved-fill edge {name} must be non-empty.')
    position = normalized.get('pool_position')
    if (isinstance(position, bool) or not isinstance(position, int) or
            position < 0):
        raise ValueError('Reserved-fill edge pool_position must be a '
                         'nonnegative integer.')
    for name in ('floor_replicas', 'gpus_per_replica', 'holdings_fill'):
        value = normalized.get(name)
        if (isinstance(value, bool) or not isinstance(value, int) or
                value < 0 or (name == 'gpus_per_replica' and value == 0)):
            raise ValueError(f'Reserved-fill edge {name} is invalid.')
    cap = normalized.get('effective_cap')
    if cap is not None and (isinstance(cap, bool) or not isinstance(cap, int) or
                            cap < 0):
        raise ValueError('Reserved-fill edge effective_cap is invalid.')
    weight = normalized.get('weight')
    if (isinstance(weight, bool) or not isinstance(weight, (int, float)) or
            float(weight) <= 0):
        raise ValueError('Reserved-fill edge weight must be positive.')
    accelerator_names = normalized.get('accelerator_names')
    if not isinstance(accelerator_names, (list, tuple)) or not all(
            isinstance(name, str) and name for name in accelerator_names):
        raise ValueError('Reserved-fill edge accelerator_names is invalid.')
    folded_accelerators = [name.casefold() for name in accelerator_names]
    if len(set(folded_accelerators)) != len(folded_accelerators):
        raise ValueError('Reserved-fill edge accelerator_names contains a '
                         'case-folded duplicate.')
    raw_projection_map = normalized.get(
        'worker_projection_sha256_by_accelerator')
    projection_map = (None if raw_projection_map is None else
                      _decode_reserved_fill_projection_digest_map(
                          raw_projection_map, accelerator_names))
    return {
        'pool_key': normalized['pool_key'],
        'legacy_pool_key': normalized['legacy_pool_key'],
        'pool_position': position,
        'access_context': normalized['access_context'],
        'physical_cluster_uid': normalized['physical_cluster_uid'],
        'accelerator_names': _reserved_fill_json(list(accelerator_names)),
        'worker_projection_sha256_by_accelerator': projection_map,
        'weight': float(weight),
        'floor_replicas': normalized['floor_replicas'],
        'gpus_per_replica': normalized['gpus_per_replica'],
        'holdings_fill': normalized['holdings_fill'],
        'effective_cap': cap,
        'launchable': int(bool(normalized.get('launchable', True))),
        # Protocol v2 advances utilization exactly once on the set row.
        'demonstrated_need': None,
        'boot_hold': None,
        'activity_ts': None,
        'heartbeat_ts': float(heartbeat_ts),
    }


def reserved_fill_reclaim_projected_admissions(
    worker_projections: Any,
    *,
    access_context: str,
    accelerator_names: typing.Sequence[str],
    accelerator_count: int,
) -> tuple[reserved_fill_reclaim_attestation.ReclaimProjectedAdmission, ...]:
    """Compatibility facade for the one canonical projection adapter."""
    return reserved_fill_projection_authority.projected_admissions_for_edge(
        worker_projections,
        access_context=access_context,
        accelerator_names=accelerator_names,
        accelerator_count=accelerator_count)


def _reserved_fill_projection_digest_map(
    admissions: typing.Sequence[
        reserved_fill_reclaim_attestation.ReclaimProjectedAdmission],
) -> dict[str, str]:
    return (reserved_fill_projection_authority.projection_sha256_by_accelerator(
        admissions))


def _reserved_fill_reclaim_edge(
    edge: dict[str, Any],
    projected_admissions: tuple[
        reserved_fill_reclaim_attestation.ReclaimProjectedAdmission, ...],
) -> reserved_fill_reclaim_attestation.ReclaimClaimEdge:
    """Project one normalized durable edge into the policy contract."""
    raw_names = edge['accelerator_names']
    if isinstance(raw_names, str):
        raw_names = json.loads(raw_names)
    if (not isinstance(raw_names, list) or not raw_names or
            any(not isinstance(name, str) or not name for name in raw_names)):
        raise ValueError('Normalized claim accelerator names are malformed.')
    return reserved_fill_reclaim_attestation.ReclaimClaimEdge(
        pool_key=str(edge['pool_key']),
        access_context=str(edge['access_context']),
        physical_cluster_uid=str(edge['physical_cluster_uid']),
        accelerator_names=tuple(sorted({name.casefold() for name in raw_names
                                       })),
        projected_admissions=projected_admissions,
    )


def _write_reserved_fill_legacy_projection_in_session(
        session: orm.Session, engine: sqlalchemy.engine.Engine,
        service_name: str, edge: dict[str, Any] | None) -> None:
    if edge is None:
        session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.service_name == service_name))
        return
    values = {
        'service_name': service_name,
        'pool_key': edge['legacy_pool_key'],
        'weight': edge['weight'],
        'floor_replicas': edge['floor_replicas'],
        'gpus_per_replica': edge['gpus_per_replica'],
        'holdings_fill': edge['holdings_fill'],
        'effective_cap': edge['effective_cap'],
        'launchable': edge['launchable'],
        'demonstrated_need': None,
        'boot_hold': None,
        'activity_ts': None,
        'heartbeat_ts': edge['heartbeat_ts'],
    }
    insert_stmt = _upsert_insert_func(engine)(
        reserved_fill_claims_table).values(**values)
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['service_name'],
        set_={
            key: insert_stmt.excluded[key]
            for key in values
            if key != 'service_name'
        })
    session.execute(insert_stmt)


def replace_reserved_fill_claim_set(
    service_name: str,
    *,
    semantic_hash: str,
    global_headroom: int,
    utilization_ceiling: int,
    utilization_state: Any,
    edges: typing.Sequence[dict[str, Any]],
    heartbeat_ts: float,
    expected_service_hash: str,
    service_version: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    reclaim_claim_scope: (reserved_fill_reclaim_attestation.ReclaimClaimSetScope
                          | None) = None,
    reclaim_claim_authorization: (
        reserved_fill_reclaim_attestation.ReclaimClaimAuthorization |
        None) = None,
) -> int | None:
    """Owner-fenced atomic replacement of one complete protocol-v2 set.

    Callers acquire the global reserved-fill broker lock before entering.
    ``None`` means the protocol or service owner fence was lost; otherwise the
    returned monotonic generation names every row written by this transaction.
    """
    if not isinstance(semantic_hash, str) or not semantic_hash:
        raise ValueError('Reserved-fill semantic_hash must be non-empty.')
    for name, value in (('global_headroom', global_headroom),
                        ('utilization_ceiling', utilization_ceiling)):
        if (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ValueError(f'Reserved-fill {name} must be nonnegative.')
    if service_version is not None and (type(service_version) is not int or
                                        service_version < 1):
        raise ValueError('Reserved-fill service_version must be positive or '
                         'None.')
    normalized_edges = [
        _normalize_reserved_fill_pool_edge(edge, heartbeat_ts) for edge in edges
    ]
    if not normalized_edges:
        raise ValueError('Reserved-fill authoritative set cannot be empty.')
    pool_keys = [edge['pool_key'] for edge in normalized_edges]
    positions = [edge['pool_position'] for edge in normalized_edges]
    if len(set(pool_keys)) != len(pool_keys):
        raise ValueError('Reserved-fill pool keys must be unique per service.')
    if len(set(positions)) != len(positions):
        raise ValueError('Reserved-fill pool positions must be unique.')
    normalized_edges.sort(key=lambda edge: edge['pool_position'])
    engine = _db_manager.get_engine()
    _require_reserved_fill_v2_postgresql(engine)
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine)
        sequence_row = _lock_zero_cost_protocol_sequence_for_update(session)
        protocol = _reserved_fill_protocol_row_in_session(session,
                                                          engine,
                                                          for_update=True)
        if int(protocol.protocol_version) != RESERVED_FILL_PROTOCOL_V2:
            session.rollback()
            return None
        gate_state = sequence_row['reconciliation_gate_state']
        if gate_state == pool_capacity_observation_schema.LEGACY_ACTIVE:
            if (service_version is not None or any(
                    edge['worker_projection_sha256_by_accelerator'] is not None
                    for edge in normalized_edges) or
                    reclaim_claim_scope is not None or
                    reclaim_claim_authorization is not None):
                session.rollback()
                return None
        elif gate_state == pool_capacity_observation_schema.SEQUENCED_ACTIVE:
            if (service_version is None or reclaim_claim_scope is None or
                    reclaim_claim_authorization is None):
                session.rollback()
                return None
        else:
            raise RuntimeError('Reserved-fill reconciliation gate is '
                               'malformed at claim persistence.')
        owner = _lock_service_owner_row_in_session(session,
                                                   service_name,
                                                   expected_service_hash,
                                                   expected_controller_owner,
                                                   require_launch_allowed=False)
        if owner is None:
            session.rollback()
            return None
        resource_scope = owner.resource_scope
        if (not isinstance(resource_scope, str) or not resource_scope or
                resource_scope != expected_service_hash):
            # Protocol selection is global: a legacy/pre-scope service cannot
            # remain on v1 after activation. Withdraw any prior v2 authority in
            # this owner-locked transaction so it cannot absorb grants it can
            # never launch.
            session.execute(
                sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                    reserved_fill_pool_claims_table.c.service_name ==
                    service_name))
            session.execute(
                sqlalchemy.delete(reserved_fill_service_claim_sets_table).where(
                    reserved_fill_service_claim_sets_table.c.service_name ==
                    service_name))
            session.execute(
                sqlalchemy.delete(reserved_fill_claims_table).where(
                    reserved_fill_claims_table.c.service_name == service_name))
            session.commit()
            return None
        if gate_state == pool_capacity_observation_schema.SEQUENCED_ACTIVE:
            assert service_version is not None
            assert reclaim_claim_authorization is not None
            if owner.current_version != service_version:
                session.rollback()
                return None
            version_row = session.execute(
                sqlalchemy.select(
                    version_specs_table.c.worker_placement_projections).where(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.version == service_version,
                        version_specs_table.c.yaml_content.isnot(None),
                        version_specs_table.c.quarantined_at.is_(None),
                        version_specs_table.c.retired_at.is_(None),
                    ).with_for_update(read=True)).mappings().one_or_none()
            if version_row is None:
                session.rollback()
                return None
            try:
                expected_edges = []
                for edge in normalized_edges:
                    raw_names = _reserved_fill_decoded_json(
                        edge['accelerator_names'])
                    projected_admissions = (
                        reserved_fill_reclaim_projected_admissions(
                            version_row['worker_placement_projections'],
                            access_context=str(edge['access_context']),
                            accelerator_names=raw_names,
                            accelerator_count=int(edge['gpus_per_replica'])))
                    if edge['worker_projection_sha256_by_accelerator'] != (
                            _reserved_fill_projection_digest_map(
                                projected_admissions)):
                        session.rollback()
                        return None
                    expected_edges.append(
                        _reserved_fill_reclaim_edge(edge, projected_admissions))
                expected_scope = (
                    reserved_fill_reclaim_attestation.ReclaimClaimSetScope(
                        service_name=service_name,
                        service_incarnation=expected_service_hash,
                        service_version=service_version,
                        semantic_hash=semantic_hash,
                        edges=tuple(sorted(expected_edges)),
                    ))
                identity = (
                    reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
                        fleet_bundle_sha256=sequence_row[
                            'reclaim_fleet_bundle_sha256'],
                        policy_revision=sequence_row['reclaim_policy_revision'],
                        provider_inventory_sha256=sequence_row[
                            'reclaim_provider_inventory_sha256']))
                (reserved_fill_reclaim_attestation.
                 require_exact_claim_authorization)(
                     reclaim_claim_authorization,
                     expected_identity=identity,
                     expected_gate_generation=sequence_row[
                         'reconciliation_gate_generation'],
                     expected_scope=expected_scope)
            except (reserved_fill_reclaim_attestation.ReclaimAttestationError,
                    TypeError, ValueError):
                session.rollback()
                return None
            if reclaim_claim_scope != expected_scope:
                session.rollback()
                return None
        previous = session.execute(
            sqlalchemy.select(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name).with_for_update()).fetchone()
        previous_edges = session.execute(
            sqlalchemy.select(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name ==
                service_name).with_for_update()).fetchall()
        previous_generation = 0 if previous is None else int(
            previous.generation)
        if previous_generation > int(protocol.claim_generation):
            raise RuntimeError('Reserved-fill claim-set generation exceeds '
                               'the global generation fence.')
        unchanged = (previous is not None and previous_generation > 0 and
                     previous.claim_set_state
                     == RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2 and
                     previous.semantic_hash == semantic_hash and
                     previous.service_version == service_version and
                     len(previous_edges) == int(previous.edge_count) and
                     {row.pool_key for row in previous_edges
                     } == set(pool_keys) and all(
                         int(row.service_generation) == previous_generation and
                         row.worker_projection_sha256_by_accelerator == next(
                             edge['worker_projection_sha256_by_accelerator']
                             for edge in normalized_edges
                             if edge['pool_key'] == row.pool_key)
                         for row in previous_edges))
        generation = (previous_generation if unchanged else
                      _next_reserved_fill_claim_generation_in_session(
                          session, protocol))
        set_values = {
            'service_name': service_name,
            'claim_set_state': RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2,
            'generation': generation,
            'edge_count': len(normalized_edges),
            'semantic_hash': semantic_hash,
            'service_version': service_version,
            'global_headroom': global_headroom,
            'utilization_ceiling': utilization_ceiling,
            'utilization_state': _reserved_fill_json(utilization_state),
            'heartbeat_ts': float(heartbeat_ts),
        }
        set_insert = _upsert_insert_func(engine)(
            reserved_fill_service_claim_sets_table).values(**set_values)
        set_insert = set_insert.on_conflict_do_update(
            index_elements=['service_name'],
            set_={
                key: set_insert.excluded[key]
                for key in set_values
                if key != 'service_name'
            })
        session.execute(set_insert)
        if not unchanged:
            _clear_reserved_fill_allocation_in_session(session, service_name)
        for edge in normalized_edges:
            edge_values = {
                'service_name': service_name,
                'service_generation': generation,
                **edge,
            }
            edge_insert = _upsert_insert_func(engine)(
                reserved_fill_pool_claims_table).values(**edge_values)
            edge_insert = edge_insert.on_conflict_do_update(
                index_elements=['service_name', 'pool_key'],
                set_={
                    key: edge_insert.excluded[key]
                    for key in edge_values
                    if key not in ('service_name', 'pool_key')
                })
            session.execute(edge_insert)
        session.execute(
            sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name == service_name,
                reserved_fill_pool_claims_table.c.pool_key.not_in(pool_keys)))
        _write_reserved_fill_legacy_projection_in_session(
            session, engine, service_name, normalized_edges[0])
        session.commit()
    return generation


def get_reserved_fill_service_claim_set(
        service_name: str) -> dict[str, Any] | None:
    """Return one raw set and its ordered edges for poller reconciliation."""
    engine = _db_manager.get_engine()
    _require_reserved_fill_v2_postgresql(engine)
    with orm.Session(engine) as session:
        protocol = _reserved_fill_protocol_row_in_session(session, engine)
        set_row = session.execute(
            sqlalchemy.select(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name)).fetchone()
        if set_row is None:
            return None
        edge_rows = session.execute(
            sqlalchemy.select(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name ==
                service_name).order_by(
                    reserved_fill_pool_claims_table.c.pool_position,
                    reserved_fill_pool_claims_table.c.pool_key)).fetchall()
    result = dict(set_row._mapping)  # pylint: disable=protected-access
    result['utilization_state'] = _reserved_fill_decoded_json(
        result.get('utilization_state'))
    edges = []
    for row in edge_rows:
        edge = dict(row._mapping)  # pylint: disable=protected-access
        edge['accelerator_names'] = _reserved_fill_decoded_json(
            edge.get('accelerator_names'))
        edge['worker_projection_sha256_by_accelerator'] = (
            _reserved_fill_decoded_json(
                edge.get('worker_projection_sha256_by_accelerator')))
        edges.append(edge)
    result['edges'] = edges
    generation = int(result['generation'])
    projection_maps = [
        edge['worker_projection_sha256_by_accelerator'] for edge in edges
    ]
    version_projection_pair_valid = (
        result.get('service_version') is None and
        all(mapping is None for mapping in projection_maps))
    if (type(result.get('service_version')) is int and
            result['service_version'] > 0):
        try:
            for edge in edges:
                edge['worker_projection_sha256_by_accelerator'] = (
                    _decode_reserved_fill_projection_digest_map(
                        edge['worker_projection_sha256_by_accelerator'],
                        edge['accelerator_names']))
            version_projection_pair_valid = True
        except ValueError:
            version_projection_pair_valid = False
    result['integrity_valid'] = (
        int(protocol.protocol_version) == RESERVED_FILL_PROTOCOL_V2 and
        result['claim_set_state'] == RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2
        and generation > 0 and generation <= int(protocol.claim_generation) and
        len(edges) == int(result['edge_count']) and
        version_projection_pair_valid and
        all(int(edge['service_generation']) == generation for edge in edges))
    return result


def _get_authoritative_reserved_fill_claims_in_executor(
    executor: Any,
    engine: sqlalchemy.engine.Engine,
    pool_key: str | None,
    expired_before: float | None,
) -> list[dict[str, Any]]:
    """Read the selected claim representation without ending a transaction."""
    protocol = _reserved_fill_protocol_row_in_session(executor, engine)
    version = int(protocol.protocol_version)
    if version == RESERVED_FILL_PROTOCOL_V1:
        query = sqlalchemy.select(reserved_fill_claims_table)
        if pool_key is not None:
            query = query.where(
                reserved_fill_claims_table.c.pool_key == pool_key)
        if expired_before is not None:
            query = query.where(
                reserved_fill_claims_table.c.heartbeat_ts >= expired_before)
        rows = executor.execute(query).fetchall()
        result = []
        for row in rows:
            claim = dict(row._mapping)  # pylint: disable=protected-access
            claim.update({
                'protocol_version': RESERVED_FILL_PROTOCOL_V1,
                'service_generation': 0,
                'legacy_pool_key': claim['pool_key'],
                'access_context': None,
                'physical_cluster_uid': None,
                'accelerator_names': None,
                'pool_position': 0,
            })
            result.append(claim)
        return result
    _require_reserved_fill_v2_postgresql(engine)
    global_generation = int(protocol.claim_generation)
    set_query = sqlalchemy.select(reserved_fill_service_claim_sets_table).where(
        reserved_fill_service_claim_sets_table.c.claim_set_state ==
        RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2)
    if expired_before is not None:
        set_query = set_query.where(reserved_fill_service_claim_sets_table.c.
                                    heartbeat_ts >= expired_before)
    set_rows = executor.execute(set_query).fetchall()
    sets = {str(row.service_name): row for row in set_rows}
    if not sets:
        return []
    edge_rows = executor.execute(
        sqlalchemy.select(reserved_fill_pool_claims_table).where(
            reserved_fill_pool_claims_table.c.service_name.in_(
                sets))).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for row in edge_rows:
        edge = dict(row._mapping)  # pylint: disable=protected-access
        grouped[str(edge['service_name'])].append(edge)
    result = []
    for service_name, set_row in sets.items():
        edges = grouped.get(service_name, [])
        generation = int(set_row.generation)
        if (generation <= 0 or generation > global_generation or
                len(edges) != int(set_row.edge_count) or any(
                    int(edge['service_generation']) != generation
                    for edge in edges) or (expired_before is not None and any(
                        float(edge['heartbeat_ts']) < expired_before
                        for edge in edges))):
            continue
        for edge in edges:
            if pool_key is not None and edge['pool_key'] != pool_key:
                continue
            edge['protocol_version'] = RESERVED_FILL_PROTOCOL_V2
            edge['service_version'] = set_row.service_version
            edge['accelerator_names'] = _reserved_fill_decoded_json(
                edge.get('accelerator_names'))
            edge['worker_projection_sha256_by_accelerator'] = (
                _reserved_fill_decoded_json(
                    edge.get('worker_projection_sha256_by_accelerator')))
            result.append(edge)
    return sorted(result,
                  key=lambda edge:
                  (str(edge['service_name']), int(edge['pool_position'])))


def get_authoritative_reserved_fill_claims(
    pool_key: str | None = None,
    *,
    expired_before: float | None = None,
) -> list[dict[str, Any]]:
    """Read exactly one representation selected by the durable gate.

    Protocol v1 returns only legacy rows.  Protocol v2 returns only complete,
    current-generation authoritative normalized sets; shadows, corrupt sets,
    and expired sets contribute nothing and never fall back to legacy.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = _get_authoritative_reserved_fill_claims_in_executor(
            session, engine, pool_key, expired_before)
        session.commit()
        return result


def get_authoritative_reserved_fill_claims_in_connection(
    connection: sqlalchemy.engine.Connection,
    pool_key: str | None = None,
    *,
    expired_before: float | None = None,
) -> list[dict[str, Any]]:
    """Read authoritative claims inside a caller-owned PG transaction."""
    engine = connection.engine
    _require_reserved_fill_v2_postgresql(engine)
    return _get_authoritative_reserved_fill_claims_in_executor(
        connection, engine, pool_key, expired_before)


def remove_reserved_fill_claim_set(
    service_name: str,
    *,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
) -> bool:
    """Atomically remove normalized state and its legacy projection."""
    engine = _db_manager.get_engine()
    _require_reserved_fill_v2_postgresql(engine)
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine)
        if not _lock_service_owner_in_session(session,
                                              service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              require_launch_allowed=False):
            session.rollback()
            return False
        normalized = session.execute(
            sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name ==
                service_name)).rowcount
        claim_set = session.execute(
            sqlalchemy.delete(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name)).rowcount
        legacy = session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.service_name ==
                service_name)).rowcount
        session.commit()
    return bool(normalized or claim_set or legacy)


def remove_authoritative_reserved_fill_claim(
    service_name: str,
    pool_key: str | None,
    *,
    expected_service_generation: int | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
) -> bool:
    """Remove one v2 edge and generation-fence every remaining edge."""
    if pool_key is None and expected_service_hash is not None:
        return remove_reserved_fill_claim_set(
            service_name,
            expected_service_hash=expected_service_hash,
            expected_controller_owner=expected_controller_owner)
    engine = _db_manager.get_engine()
    _require_reserved_fill_v2_postgresql(engine)
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine)
        protocol = _reserved_fill_protocol_row_in_session(session,
                                                          engine,
                                                          for_update=True)
        if int(protocol.protocol_version) != RESERVED_FILL_PROTOCOL_V2:
            session.rollback()
            return False
        if expected_service_hash is not None and not _lock_service_owner_in_session(
                session,
                service_name,
                expected_service_hash,
                expected_controller_owner,
                require_launch_allowed=False):
            session.rollback()
            return False
        set_row = session.execute(
            sqlalchemy.select(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name).with_for_update()).fetchone()
        if (set_row is None or set_row.claim_set_state
                != RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2 or
            (expected_service_generation is not None and
             int(set_row.generation) != expected_service_generation)):
            session.rollback()
            return False
        if int(set_row.generation) > int(protocol.claim_generation):
            raise RuntimeError('Reserved-fill claim-set generation exceeds '
                               'the global generation fence.')
        if pool_key is None:
            session.execute(
                sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                    reserved_fill_pool_claims_table.c.service_name ==
                    service_name))
            session.execute(
                sqlalchemy.delete(reserved_fill_service_claim_sets_table).where(
                    reserved_fill_service_claim_sets_table.c.service_name ==
                    service_name))
            _write_reserved_fill_legacy_projection_in_session(
                session, engine, service_name, None)
            session.commit()
            return True
        rows = session.execute(
            sqlalchemy.select(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name ==
                service_name).order_by(
                    reserved_fill_pool_claims_table.c.pool_position,
                    reserved_fill_pool_claims_table.c.pool_key).with_for_update(
                    )).fetchall()
        edges = [
            dict(row._mapping)  # pylint: disable=protected-access
            for row in rows
        ]
        if not any(edge['pool_key'] == pool_key for edge in edges):
            session.rollback()
            return False
        remaining = [edge for edge in edges if edge['pool_key'] != pool_key]
        if not remaining:
            session.execute(
                sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                    reserved_fill_pool_claims_table.c.service_name ==
                    service_name))
            session.execute(
                sqlalchemy.delete(reserved_fill_service_claim_sets_table).where(
                    reserved_fill_service_claim_sets_table.c.service_name ==
                    service_name))
            _write_reserved_fill_legacy_projection_in_session(
                session, engine, service_name, None)
            session.commit()
            return True
        generation = _next_reserved_fill_claim_generation_in_session(
            session, protocol)
        session.execute(
            sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name == service_name,
                reserved_fill_pool_claims_table.c.pool_key == pool_key))
        session.execute(
            sqlalchemy.update(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name ==
                service_name).values(service_generation=generation))
        invalidated_hash = f'reconciled:{generation}'
        session.execute(
            sqlalchemy.update(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name).values(generation=generation,
                                     edge_count=len(remaining),
                                     semantic_hash=invalidated_hash))
        _clear_reserved_fill_allocation_in_session(session, service_name)
        remaining[0]['service_generation'] = generation
        _write_reserved_fill_legacy_projection_in_session(
            session, engine, service_name, remaining[0])
        session.commit()
    return True


def prune_authoritative_reserved_fill_claim_sets(
        expired_before: float) -> list[str]:
    """Delete expired authoritative sets and their rollback projections."""
    engine = _db_manager.get_engine()
    _require_reserved_fill_v2_postgresql(engine)
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine)
        rows = session.execute(
            sqlalchemy.select(
                reserved_fill_service_claim_sets_table.c.service_name).where(
                    reserved_fill_service_claim_sets_table.c.claim_set_state ==
                    RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2,
                    reserved_fill_service_claim_sets_table.c.heartbeat_ts
                    < expired_before).with_for_update()).fetchall()
        names = [str(row[0]) for row in rows]
        if names:
            session.execute(
                sqlalchemy.delete(reserved_fill_pool_claims_table).where(
                    reserved_fill_pool_claims_table.c.service_name.in_(names)))
            session.execute(
                sqlalchemy.delete(reserved_fill_service_claim_sets_table).where(
                    reserved_fill_service_claim_sets_table.c.service_name.in_(
                        names),
                    reserved_fill_service_claim_sets_table.c.heartbeat_ts
                    < expired_before))
            session.execute(
                sqlalchemy.delete(reserved_fill_claims_table).where(
                    reserved_fill_claims_table.c.service_name.in_(names)))
        session.commit()
    return names


def remove_authoritative_reserved_fill_claims_for_pool(
        pool_key: str) -> list[tuple[str, str]]:
    """Remove every authoritative edge on one pool under the caller's lock."""
    claims = get_authoritative_reserved_fill_claims(pool_key=pool_key)
    removed: list[tuple[str, str]] = []
    for claim in claims:
        service_name = str(claim['service_name'])
        if remove_authoritative_reserved_fill_claim(
                service_name,
                pool_key,
                expected_service_generation=int(claim['service_generation'])):
            removed.append((service_name, pool_key))
    return removed


def prune_authoritative_reserved_fill_claims(
        expired_before: float) -> list[str]:
    """Compatibility name for pruning complete expired v2 claim sets."""
    return prune_authoritative_reserved_fill_claim_sets(expired_before)


def upsert_reserved_fill_claim(
    service_name: str,
    *,
    pool_key: str,
    weight: float,
    floor_replicas: int,
    gpus_per_replica: int,
    holdings_fill: int,
    effective_cap: int | None,
    launchable: bool,
    heartbeat_ts: float,
    demonstrated_need: int | None = None,
    boot_hold: bool | None = None,
    activity_ts: float | None = None,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Upserts a service's reserved-fill claim (the per-poll heartbeat)."""
    engine = _db_manager.get_engine()
    values = {
        'service_name': service_name,
        'pool_key': pool_key,
        'weight': weight,
        'floor_replicas': floor_replicas,
        'gpus_per_replica': gpus_per_replica,
        'holdings_fill': holdings_fill,
        'effective_cap': effective_cap,
        'launchable': int(launchable),
        # Written unconditionally. A NULL activity_ts is the durable static
        # opt-out/pre-gate shape; a fresh activity_ts paired with NULL need is
        # armed-but-blind. The latter must clear any previous numeric sample
        # in the same statement that advances heartbeat_ts, or the broker
        # could keep trusting a measurement nothing is refreshing.
        'demonstrated_need': demonstrated_need,
        'boot_hold': None if boot_hold is None else int(boot_hold),
        'activity_ts': activity_ts,
        'heartbeat_ts': heartbeat_ts,
    }
    with orm.Session(engine) as session:
        if expected_service_hash is not None:
            if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
                session.execute(sqlalchemy.text('BEGIN IMMEDIATE'))
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.controller_pid,
                    services_table.c.controller_ip).where(
                        services_table.c.name ==
                        service_name).with_for_update()).fetchone()
            if (owner is None or owner[0] != expected_service_hash or
                (expected_controller_owner is not None and
                 (owner[1], owner[2]) != expected_controller_owner)):
                session.rollback()
                return False
        insert_stmt = _upsert_insert_func(engine)(
            reserved_fill_claims_table).values(**values)
        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['service_name'],
            set_={
                key: insert_stmt.excluded[key]
                for key in values
                if key != 'service_name'
            })
        session.execute(insert_stmt)
        session.commit()
    return True


def remove_reserved_fill_claim(
    service_name: str,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if expected_service_hash is not None:
            if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
                session.execute(sqlalchemy.text('BEGIN IMMEDIATE'))
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.controller_pid,
                    services_table.c.controller_ip).where(
                        services_table.c.name ==
                        service_name).with_for_update()).fetchone()
            if (owner is None or owner[0] != expected_service_hash or
                (expected_controller_owner is not None and
                 (owner[1], owner[2]) != expected_controller_owner)):
                session.rollback()
                return False
        result = session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.service_name == service_name))
        session.commit()
    return result.rowcount > 0


def remove_reserved_fill_claims_for_pool(pool_key: str) -> None:
    """Drops every claim on a pool (phantom-pool rejection)."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.pool_key == pool_key))
        session.commit()


def prune_reserved_fill_claims(expired_before: float) -> list[str]:
    """Deletes claims whose heartbeat predates `expired_before`.

    Returns the actually-pruned service names (for loud logging by the
    broker). The DELETE itself carries the staleness predicate: a
    heartbeat refreshed after the candidate SELECT can never be deleted
    (the previous select-then-delete-BY-NAME pair raced exactly that
    upsert and killed fresh claims). The report is the candidate set
    minus post-delete survivors, so a row spared by the predicate is
    never reported as pruned.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        candidates = [
            row[0] for row in session.execute(
                sqlalchemy.select(reserved_fill_claims_table.c.service_name).
                where(reserved_fill_claims_table.c.heartbeat_ts <
                      expired_before)).fetchall()
        ]
        if not candidates:
            session.commit()
            return []
        session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.heartbeat_ts < expired_before))
        survivors = {
            row[0] for row in session.execute(
                sqlalchemy.select(reserved_fill_claims_table.c.service_name).
                where(reserved_fill_claims_table.c.service_name.in_(
                    candidates))).fetchall()
        }
        session.commit()
    return [name for name in candidates if name not in survivors]


def get_reserved_fill_claims(
        pool_key: str | None = None) -> list[dict[str, Any]]:
    """All claim rows (optionally restricted to one pool), as dicts."""
    engine = _db_manager.get_engine()
    query = sqlalchemy.select(reserved_fill_claims_table)
    if pool_key is not None:
        query = query.where(reserved_fill_claims_table.c.pool_key == pool_key)
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    return [dict(row._mapping) for row in rows]  # pylint: disable=protected-access


def get_reserved_fill_round(pool_key: str) -> dict[str, Any] | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(reserved_fill_rounds_table).where(
                reserved_fill_rounds_table.c.pool_key == pool_key)).fetchone()
        if row is None:
            return None
        result = dict(row._mapping)  # pylint: disable=protected-access
        if engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            provenance = session.execute(
                sqlalchemy.select(
                    pool_capacity_observation_schema.
                    reserved_fill_round_observation_table).where(
                        pool_capacity_observation_schema.
                        reserved_fill_round_observation_table.c.pool_key ==
                        pool_key)).mappings().one_or_none()
            if provenance is None:
                raise RuntimeError('Reserved-fill round lost its PostgreSQL '
                                   'observation-provenance projection.')
            result.update({
                'observation_generation': provenance['observation_generation'],
                'observation_sequence': provenance['observation_sequence'],
                'observation_materialization_sequence':
                    provenance['observation_materialization_sequence'],
                'observation_payload_sha256':
                    provenance['observation_payload_sha256'],
            })
    return result


def get_demand_capacity_observations(
        contexts: typing.Iterable[str]) -> dict[str, dict[str, Any]]:
    """Return the latest shared demand-capacity observation per context."""
    context_names = sorted(set(contexts))
    if not context_names:
        return {}
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(demand_capacity_observations_table).where(
                demand_capacity_observations_table.c.context.in_(
                    context_names))).fetchall()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        mapping = row._mapping  # pylint: disable=protected-access
        result[str(mapping['context'])] = dict(mapping)
    return result


def upsert_demand_capacity_observation(
        context: str, snapshot_time: float, completed_at: float,
        availability: dict[str, int] | None) -> None:
    """Publish one raw free-GPU observation for cross-controller reuse."""
    engine = _db_manager.get_engine()
    values = {
        'context': context,
        'snapshot_time': snapshot_time,
        'completed_at': completed_at,
        'availability': (None if availability is None else json.dumps(
            availability, sort_keys=True)),
    }
    with orm.Session(engine) as session:
        insert_stmt = _upsert_insert_func(engine)(
            demand_capacity_observations_table).values(**values)
        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['context'],
            set_={
                key: insert_stmt.excluded[key]
                for key in values
                if key != 'context'
            })
        session.execute(insert_stmt)
        session.commit()


def advance_reserved_fill_persist_token(lock_connection: Any) -> int | None:
    """Advance the global lease epoch before one fill-row persist.

    PostgreSQL callers pass the DBAPI session that owns the broker advisory
    lock.  If that session dies after this commit, a replacement round advances
    the same epoch before scanning replicas.  The stale persist transaction
    therefore either validates and locks this exact token before the replacement
    (so its row commits before the replacement scan), or observes the
    replacement token and fails closed.

    The persist does not refresh ``expires_at``: only a driven broker round is a
    lease heartbeat.  ``None`` means the int4 epoch is malformed or exhausted.
    """
    max_epoch = 2**31 - 1
    cursor = lock_connection.cursor()
    try:
        cursor.execute(
            'INSERT INTO reserved_fill_lease (id, epoch, expires_at) '
            'VALUES (1, 1, NULL) '
            'ON CONFLICT (id) DO UPDATE SET epoch = '
            'reserved_fill_lease.epoch + 1 '
            'WHERE reserved_fill_lease.epoch >= 0 '
            'AND reserved_fill_lease.epoch < %s RETURNING epoch', (max_epoch,))
        result = cursor.fetchone()
        lock_connection.commit()
    except BaseException:
        lock_connection.rollback()
        raise
    finally:
        cursor.close()
    if result is None:
        return None
    token = int(result[0])
    return token if token > 0 else None


def acquire_reserved_fill_lease_token(
        *, now: float, expires_at: float) -> tuple[int, bool] | None:
    """Reads, expiry-checks and CAS-advances the global lease atomically.

    TOKEN-FIRST ordering invariant (the other half lives in
    reserved_capacity_broker._run_round_locked): this acquisition is the
    round's ENTRY POINT, committed before the writer reads ANY claim or
    round state and before its slow cluster query. The returned epoch is
    the writer's OWNERSHIP TOKEN: publish_reserved_fill_round only lands
    while the lease still holds this exact token, and any replacement
    writer must advance it again. Because every input the publish
    persists is read AFTER this commit, a writer that lost its advisory
    lock mid-round can never publish from pre-acquisition (stale) state
    -- the replacement's own advance invalidates the token first, so the
    stale publish fails closed instead of regressing a pool's fencing
    epoch or clearing a peer's fence_pending marker (the lease epoch is
    advanced unconditionally per driven round, so two same-epoch
    publishes cannot both succeed either).

    The lease read, its expiry check and the CAS ride ONE transaction
    (the row is read FOR UPDATE where the dialect supports it; on sqlite
    the epoch-filtered UPDATE still fails closed if the row moved). An
    expired lease -- a dead gap with no rounds at all -- additionally
    stamps fence_pending=1 on every pool's round row in the same
    transaction: the acquisition itself commits a fresh expires_at,
    consuming the only other evidence of the gap, so if this writer dies
    before publishing, only the persisted marker still forces the
    per-pool epoch bump the next publish must carry. The marker survives
    any number of aborted token advances, fails actuation closed while
    set (see add_replica_if_round_epoch), and is cleared per pool
    exclusively by a successful publish (see publish_reserved_fill_round).
    Rounds are marked per pool because rounds and their fencing epochs
    are per-pool; a single global flag could be cleared by one pool's
    publish while another pool never bumped.

    Returns (token, lease_was_expired), or None when the CAS lost a race
    (another writer advanced the lease concurrently -- a lock-bypass
    signal; the caller must abort the round).
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(reserved_fill_lease_table.c.epoch,
                              reserved_fill_lease_table.c.expires_at).where(
                                  reserved_fill_lease_table.c.id ==
                                  1).with_for_update()).fetchone()
        if row is None:
            # First lease ever (also an expired "lease": no rounds have
            # ever run). A PK collision means we lost a race that the
            # round lock should have prevented -- fail closed.
            lease_expired = True
            try:
                session.execute(
                    sqlalchemy.insert(reserved_fill_lease_table).values(
                        id=1, epoch=1, expires_at=expires_at))
            except sqlalchemy_exc.IntegrityError:
                session.rollback()
                return None
            token = 1
        else:
            prev_epoch = int(row[0])
            lease_expired = row[1] is None or float(row[1]) < now
            token = prev_epoch + 1
            count = session.query(reserved_fill_lease_table).filter(
                reserved_fill_lease_table.c.id == 1,
                reserved_fill_lease_table.c.epoch == prev_epoch).update({
                    reserved_fill_lease_table.c.epoch: token,
                    reserved_fill_lease_table.c.expires_at: expires_at,
                })
            if count == 0:
                session.rollback()
                return None
        if lease_expired:
            session.execute(
                sqlalchemy.update(reserved_fill_rounds_table).values(
                    fence_pending=1))
        session.commit()
    return token, lease_expired


def publish_reserved_fill_round(
        pool_key: str,
        *,
        round_id: int,
        snapshot_time: float,
        epoch: int,
        grants: str,
        feeds: str,
        raw_grants: str,
        feed_state: str,
        sum_holdings: int,
        last_observed_free: int | None,
        last_observed_free_ts: float | None,
        phantom_streak: int,
        shrink_baseline: int | None,
        lease_token: int,
        lease_expires_at: float,
        utilization_state: str | None = None,
        protocol_version: int = RESERVED_FILL_PROTOCOL_V1,
        claim_generations: dict[str, int] | str | None = None,
        feed_by_accelerator: str | None = None,
        observation_generation: int | None = None,
        observation_sequence: int | None = None,
        observation_materialization_sequence: int | None = None,
        observation_payload_sha256: str | None = None) -> bool:
    """Publishes a round iff the lease still holds the writer's token.

    `epoch` is the POOL's fencing epoch, stored on the round row (per-pool:
    the launch fence compares against it, and one pool's grant churn must
    not fence another pool's launches). `lease_token` is the ownership
    token the writer committed via acquire_reserved_fill_lease_token BEFORE
    its cluster query.

    The lease update is filtered on the exact token (the *_if_owner CAS
    pattern): a replacement writer that re-advanced the lease while this
    writer's slow query ran makes rowcount 0 and the whole round (lease +
    round row) rolls back -- a stale observation can never overwrite the
    replacement's round. The broker holds the cross-process round lock, so
    a CAS failure means that lock was lost or bypassed; failing closed
    (discarding this writer's observation) is the only safe reaction.

    A successful publish always clears the pool's dead-gap fence marker
    (fence_pending=0): the broker forces the epoch bump whenever the
    marker was set, so clearing rides the same transaction. Clearing
    unconditionally is safe against a marker set AFTER this writer read
    its round row: the marker is only ever set together with a lease
    advance, which invalidates this writer's token and fails this publish.

    In ``SEQUENCED_ACTIVE`` the four observation fields are mandatory and
    identify one digest-valid, unexpired successful repository row.  That row,
    the protocol sequencer, and the round publication are checked while the
    same PostgreSQL transaction owns the protocol singleton lock.  Legacy
    operation accepts no provenance so there is one unambiguous authority path
    on either side of the one-way gate.

    Returns True if the round was published.
    """
    provenance_values = (observation_generation, observation_sequence,
                         observation_materialization_sequence,
                         observation_payload_sha256)
    has_provenance = any(value is not None for value in provenance_values)
    if has_provenance and not all(value is not None
                                  for value in provenance_values):
        raise ValueError('Reserved-fill observation provenance must be all '
                         'present or all absent.')
    if has_provenance:
        if (isinstance(observation_generation, bool) or
                not isinstance(observation_generation, int) or
                observation_generation <= 0 or
                isinstance(observation_sequence, bool) or
                not isinstance(observation_sequence, int) or
                observation_sequence < 0 or
                isinstance(observation_materialization_sequence, bool) or
                not isinstance(observation_materialization_sequence, int) or
                observation_materialization_sequence < 0 or
                not isinstance(observation_payload_sha256, str) or re.fullmatch(
                    r'[0-9a-f]{64}', observation_payload_sha256) is None):
            raise ValueError('Reserved-fill observation provenance is '
                             'malformed.')
    engine = _db_manager.get_engine()
    is_postgres = (
        engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    if has_provenance and not is_postgres:
        raise ValueError('Sequenced observation provenance is '
                         'PostgreSQL-only.')
    with orm.Session(engine) as session:
        protocol_authority = None
        if is_postgres:
            # Canonical SQL order is event protocol before lifecycle/service,
            # rounds and replica state. Besides fencing gate changes, this
            # keeps publication from crossing zero-cost write transactions.
            protocol_authority = (
                _lock_zero_cost_protocol_sequence_for_update(session))
        protocol = _reserved_fill_protocol_row_in_session(session,
                                                          engine,
                                                          for_update=True)
        if int(protocol.protocol_version) != protocol_version:
            session.rollback()
            return False
        sequenced_active = False
        if is_postgres:
            assert protocol_authority is not None
            gate_state = protocol_authority['reconciliation_gate_state']
            if gate_state not in (
                    pool_capacity_observation_schema.LEGACY_ACTIVE,
                    pool_capacity_observation_schema.SEQUENCED_ACTIVE):
                raise RuntimeError('Reserved-fill reconciliation gate is '
                                   'malformed.')
            sequenced_active = (
                gate_state == pool_capacity_observation_schema.SEQUENCED_ACTIVE)
            if sequenced_active:
                if (protocol_version != RESERVED_FILL_PROTOCOL_V2 or
                        not has_provenance):
                    session.rollback()
                    return False
                assert observation_generation is not None
                assert observation_sequence is not None
                assert observation_materialization_sequence is not None
                assert observation_payload_sha256 is not None
                if observation_sequence > int(
                        protocol_authority['zero_cost_admission_sequence']
                ) or observation_materialization_sequence > int(
                        protocol_authority['zero_cost_materialization_sequence']
                ):
                    session.rollback()
                    return False
                observation_row = session.execute(
                    sqlalchemy.select(
                        pool_capacity_observation_schema.
                        demand_capacity_observations_v2_table).where(
                            pool_capacity_observation_schema.
                            demand_capacity_observations_v2_table.c.pool_key ==
                            pool_key,
                            pool_capacity_observation_schema.
                            demand_capacity_observations_v2_table.c.
                            observation_generation == observation_generation,
                            pool_capacity_observation_schema.
                            demand_capacity_observations_v2_table.c.
                            observation_sequence == observation_sequence,
                            pool_capacity_observation_schema.
                            demand_capacity_observations_v2_table.c.
                            payload_sha256 == observation_payload_sha256,
                        ).with_for_update(read=True)).mappings().one_or_none()
                if observation_row is None:
                    session.rollback()
                    return False
                committed_observation = (
                    pool_capacity_observation.decode_completed_observation(
                        observation_row))
                database_now = float(
                    session.execute(
                        sqlalchemy.text('SELECT EXTRACT(EPOCH FROM '
                                        'clock_timestamp())::double precision')
                    ).scalar_one())
                if (committed_observation is None or
                        not committed_observation.is_authoritative_at(
                            database_now) or
                        committed_observation.materialization_sequence
                        != observation_materialization_sequence or
                        committed_observation.observed_at != snapshot_time):
                    session.rollback()
                    return False
            elif has_provenance:
                # Before the one-way cutover the callback-based path remains
                # the only authority.  Rejecting a mixed representation keeps
                # activation auditable and prevents a silent per-round mode.
                session.rollback()
                return False
        count = session.query(reserved_fill_lease_table).filter(
            reserved_fill_lease_table.c.id == 1,
            reserved_fill_lease_table.c.epoch == lease_token).update({
                reserved_fill_lease_table.c.expires_at: lease_expires_at,
            })
        if count == 0:
            session.rollback()
            return False
        values = {
            'pool_key': pool_key,
            'round_id': round_id,
            'snapshot_time': snapshot_time,
            'epoch': epoch,
            'protocol_version': protocol_version,
            'claim_generations': _reserved_fill_json(claim_generations or {}),
            'grants': grants,
            'feeds': feeds,
            'feed_by_accelerator': feed_by_accelerator,
            'raw_grants': raw_grants,
            'feed_state': feed_state,
            'sum_holdings': sum_holdings,
            'last_observed_free': last_observed_free,
            'last_observed_free_ts': last_observed_free_ts,
            'phantom_streak': phantom_streak,
            'shrink_baseline': shrink_baseline,
            'fence_pending': 0,
            # Always written, never conditionally omitted: a round in which
            # no claimant is gated must CLEAR the state, otherwise disarming
            # the gate would leave a stale release target that re-arms into
            # a decay already half-finished. Carrying the state forward
            # across rounds is the broker's job, not this writer's.
            'utilization_state': utilization_state,
        }
        insert_stmt = _upsert_insert_func(engine)(
            reserved_fill_rounds_table).values(**values)
        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['pool_key'],
            set_={
                key: insert_stmt.excluded[key]
                for key in values
                if key != 'pool_key'
            })
        session.execute(insert_stmt)
        if is_postgres:
            provenance_update = session.execute(
                sqlalchemy.update(
                    pool_capacity_observation_schema.
                    reserved_fill_round_observation_table).where(
                        pool_capacity_observation_schema.
                        reserved_fill_round_observation_table.c.pool_key ==
                        pool_key).values(
                            observation_generation=(observation_generation if
                                                    sequenced_active else None),
                            observation_sequence=(observation_sequence if
                                                  sequenced_active else None),
                            observation_materialization_sequence=(
                                observation_materialization_sequence
                                if sequenced_active else None),
                            observation_payload_sha256=(
                                observation_payload_sha256
                                if sequenced_active else None),
                        ))
            if provenance_update.rowcount != 1:
                raise RuntimeError('Reserved-fill round provenance did not '
                                   'update the published pool row.')
        session.commit()
    return True


# Bounded retry budget for the sqlite persist fence when another writer
# holds the database write lock past the driver's busy timeout: a lost
# race must degrade into a fence-skip (launch retried next tick), never
# into an exception that aborts the whole scale-up batch.
_SQLITE_FENCE_BUSY_RETRIES = 3
_SQLITE_FENCE_BUSY_BACKOFF_SECONDS = 0.05


def _is_sqlite_busy_error(error: sqlalchemy_exc.OperationalError) -> bool:
    """SQLITE_BUSY-family errors ('database is locked', BUSY_SNAPSHOT)."""
    message = str(error.orig if error.orig is not None else error).lower()
    return 'locked' in message or 'busy' in message


def _reserved_fill_pool_key_protocol(pool_key: Any) -> int | None:
    """Strictly identify the protocol embedded in one broker pool key."""
    if not isinstance(pool_key, str) or not pool_key:
        return None
    try:
        decoded = json.loads(pool_key)
    except (TypeError, ValueError):
        return None
    if not isinstance(decoded, list):
        return None
    if (len(decoded) == 2 and isinstance(decoded[0], str) and decoded[0]):
        protocol_version = RESERVED_FILL_PROTOCOL_V1
        encoded_names = decoded[1]
    elif (len(decoded) == 3 and decoded[0] == 'v2' and
          isinstance(decoded[1], str) and decoded[1]):
        protocol_version = RESERVED_FILL_PROTOCOL_V2
        encoded_names = decoded[2]
    else:
        return None
    if isinstance(encoded_names, str):
        names = (encoded_names,)
    elif isinstance(encoded_names, list):
        names = tuple(encoded_names)
    else:
        return None
    if not names or not all(isinstance(name, str) and name for name in names):
        return None
    return protocol_version


def _reserved_fill_replica_row_values(
        service_name: str, replica_id: int,
        replica_info: 'replica_managers.ReplicaInfo', *, pool_key: str,
        expected_protocol_version: int) -> dict[str, Any] | None:
    """Build a row only for exact, protocol-attributed fill provenance."""
    if replica_info.reserved_fill is not True:
        return None
    persisted_pool_key = replica_info.reserved_fill_pool_key
    if (persisted_pool_key != pool_key or
            _reserved_fill_pool_key_protocol(persisted_pool_key)
            != expected_protocol_version):
        return None
    row_values = _replica_row_values(service_name, replica_id, replica_info)
    replica_state = row_values.get('replica_state')
    if (not isinstance(replica_state, dict) or
            replica_state.get('reserved_fill') is not True or
            replica_state.get('reserved_fill_pool_key') != pool_key):
        return None
    return row_values


def _reserved_fill_exact_location_snapshot(
    replica_info: 'replica_managers.ReplicaInfo',
) -> 'reserved_fill_planner.LocationSnapshot | None':
    """Return one internally consistent exact Kubernetes launch shape."""
    resources_override = replica_info.resources_override
    if type(resources_override) is not dict:
        return None
    try:
        location = replica_info.get_spot_location()
        override_location = (reserved_fill_planner.spot_placer.Location.
                             from_resources_override(resources_override))
        if (location is None or override_location is None or
                location != override_location):
            return None
        return reserved_fill_planner.LocationSnapshot.from_pickleable(
            location.to_pickleable())
    except (AssertionError, KeyError, TypeError, ValueError):
        return None


def _stage_postgres_replica_if_round_epoch(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    pool_key: str,
    expected_epoch: int,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
    expected_protocol_version: int,
    expected_service_generation: int | None = None,
    expected_physical_cluster_uid: str | None = None,
    expected_ordinary_zero_cost_admission_sequence: int | None = None,
    expected_lease_token: int | None = None,
    expected_actuation_mode: str | None = None,
    actuation_lease: 'zero_cost_actuation.IntentLease | None' = None,
) -> StagedReservedFillReplica | None:
    """Stage a fill replica without committing the caller-owned transaction."""
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise ValueError('Reserved-fill staging requires PostgreSQL.')
    if expected_protocol_version not in (RESERVED_FILL_PROTOCOL_V1,
                                         RESERVED_FILL_PROTOCOL_V2):
        raise ValueError('Expected reserved-fill protocol must be 1 or 2.')
    if (expected_lease_token is None or
            isinstance(expected_lease_token, bool) or
            not isinstance(expected_lease_token, int) or
            expected_lease_token <= 0):
        raise ValueError('Expected reserved-fill lease token must be a '
                         'positive integer.')
    if (expected_ordinary_zero_cost_admission_sequence is not None and
        (type(expected_ordinary_zero_cost_admission_sequence) is not int or
         expected_ordinary_zero_cost_admission_sequence < 0)):
        raise ValueError('Expected ordinary zero-cost admission sequence must '
                         'be a nonnegative integer.')
    if (replica_info.is_zero_cost is True and
        (replica_info.zero_cost_admission_sequence is not None or
         replica_info.zero_cost_materialization_sequence is not None)):
        raise ValueError('New zero-cost replica sequences must be assigned '
                         'by PostgreSQL.')
    if actuation_lease is None:
        if (expected_protocol_version != RESERVED_FILL_PROTOCOL_V1 or
                expected_actuation_mode is not None):
            raise ValueError('Standalone staging is protocol-v1 only.')
    elif (expected_protocol_version != RESERVED_FILL_PROTOCOL_V2 or
          expected_actuation_mode
          != zero_cost_actuation.ActuationMode.DURABLE_INTENT.value):
        raise ValueError('Protocol-v2 staging requires one durable intent.')
    engine = connection.engine
    session = connection
    transaction_replica_info = copy.deepcopy(replica_info)
    transaction_infos = [(replica_id, transaction_replica_info)]
    # The event sequencer is the first SQL mutex for every zero-cost
    # insert, including typed fill.  Generic ordinary writers use the
    # same order before lifecycle/service rows; taking this after the
    # service owner would create a crossed-lock deadlock.
    sequence_row = _lock_zero_cost_protocol_sequence_for_update(session)
    protocol = _reserved_fill_protocol_row_in_session(session,
                                                      engine,
                                                      for_update=True)
    if int(protocol.protocol_version) != expected_protocol_version:
        return None
    if expected_lease_token is not None:
        lease = session.execute(
            sqlalchemy.select(reserved_fill_lease_table.c.epoch).where(
                reserved_fill_lease_table.c.id ==
                1).with_for_update()).fetchone()
        if (lease is None or int(lease.epoch) != expected_lease_token):
            return None
    gate_state = sequence_row['reconciliation_gate_state']
    _lifecycle_epoch_matches_in_session(session, service_name, None)
    owner = None
    owner_required = (
        expected_service_hash is not None or
        expected_actuation_mode is not None or
        (expected_protocol_version == RESERVED_FILL_PROTOCOL_V2 and
         gate_state == pool_capacity_observation_schema.SEQUENCED_ACTIVE))
    if owner_required:
        owner = session.execute(
            sqlalchemy.select(
                services_table.c.hash, services_table.c.controller_pid,
                services_table.c.controller_ip,
                services_table.c.current_version,
                services_table.c.logical_replica_semantics,
                services_table.c.status, services_table.c.resource_scope,
                services_table.c.reserved_fill_actuation_mode).where(
                    services_table.c.name ==
                    service_name).with_for_update()).fetchone()
        if owner is None:
            return None
        if expected_service_hash is not None and (
                owner[0] != expected_service_hash or
            (expected_controller_owner is not None and
             (owner[1], owner[2]) != expected_controller_owner)):
            return None
        if (expected_actuation_mode is not None and
                owner[7] != expected_actuation_mode):
            return None
    replica_record_id: uuid.UUID | None = None
    actuation_already_committed = False
    if actuation_lease is not None:
        try:
            replica_record_id = uuid.UUID(
                transaction_replica_info.replica_record_id)
            # Normalize the cross-subsystem lock order before the
            # capacity-ledger replica scan: service -> intent ->
            # replicas. Grant publication uses the same order.
            actuation_already_committed = (
                zero_cost_actuation.lock_materialization_lease_in_connection(
                    connection,
                    actuation_lease,
                    service_name=service_name,
                    replica_id=replica_id,
                    replica_record_id=replica_record_id))
        except (TypeError, ValueError,
                zero_cost_actuation.ZeroCostActuationError):
            return None
    if actuation_already_committed:
        assert actuation_lease is not None
        # This can only be a lost acknowledgement of the exact outer
        # transaction. Historical partial materializations are not
        # repaired here; the request layer must exact-match the
        # existing association/request suffix before committing.
        replica_row = session.execute(
            sqlalchemy.select(replicas_table).where(
                replicas_table.c.service_name == service_name,
                replicas_table.c.replica_id ==
                replica_id).with_for_update()).mappings().one_or_none()
        if replica_row is None:
            return None
        try:
            persisted_info = _replica_from_state(
                replica_row['replica_state_version'],
                replica_row['replica_state'])
            committed_intent = (
                zero_cost_actuation.committed_intent_for_replica_in_connection(
                    connection,
                    service_name=service_name,
                    service_hash=(actuation_lease.intent.service_incarnation),
                    replica_info=persisted_info))
        except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
            return None
        if (persisted_info.replica_record_id != str(replica_record_id) or
                committed_intent is None or committed_intent.idempotency_key
                != actuation_lease.intent.idempotency_key):
            return None
        return StagedReservedFillReplica(replica_id=replica_id,
                                         caller_info=replica_info,
                                         persisted_info=persisted_info,
                                         already_committed=True)
    if (expected_protocol_version == RESERVED_FILL_PROTOCOL_V2 and
            gate_state == pool_capacity_observation_schema.SEQUENCED_ACTIVE):
        assert owner is not None
        if (not isinstance(expected_service_hash, str) or
                not expected_service_hash or
                owner[6] != expected_service_hash or owner[5] in {
                    status.value for status in
                    ServiceStatus.replica_launch_blocking_statuses()
                }):
            return None
        current_version = owner[3]
        if (type(current_version) is not int or current_version < 1 or
                transaction_replica_info.version != current_version):
            return None
    row = session.execute(
        sqlalchemy.select(reserved_fill_rounds_table.c.epoch,
                          reserved_fill_rounds_table.c.fence_pending,
                          reserved_fill_rounds_table.c.protocol_version,
                          reserved_fill_rounds_table.c.claim_generations).where(
                              reserved_fill_rounds_table.c.pool_key ==
                              pool_key).with_for_update(read=True)).fetchone()
    if (row is not None and
        ((expected_protocol_version == RESERVED_FILL_PROTOCOL_V1 and
          int(row.epoch) != expected_epoch) or bool(row.fence_pending) or
         int(row.protocol_version) != expected_protocol_version)):
        return None
    if expected_protocol_version == RESERVED_FILL_PROTOCOL_V1:
        # Preserve the v1 missing-round behavior: the carried protocol
        # still fences activation, and no v2 decision can use it.
        claim = session.execute(
            sqlalchemy.select(reserved_fill_claims_table.c.service_name).where(
                reserved_fill_claims_table.c.service_name == service_name,
                reserved_fill_claims_table.c.pool_key ==
                pool_key).with_for_update(read=True)).fetchone()
        if claim is None:
            return None
    else:
        if (expected_service_generation is None or
                expected_service_generation <= 0 or
                not expected_physical_cluster_uid):
            return None
        set_row = session.execute(
            sqlalchemy.select(reserved_fill_service_claim_sets_table).where(
                reserved_fill_service_claim_sets_table.c.service_name ==
                service_name).with_for_update(read=True)).fetchone()
        edge_rows = session.execute(
            sqlalchemy.select(reserved_fill_pool_claims_table).where(
                reserved_fill_pool_claims_table.c.service_name ==
                service_name).with_for_update(read=True)).fetchall()
        matching = next(
            (edge for edge in edge_rows if edge.pool_key == pool_key), None)
        transaction_location = _reserved_fill_exact_location_snapshot(
            transaction_replica_info)
        transaction_accelerator = (None if transaction_location is None else
                                   transaction_location.accelerator.casefold())
        if set_row is None:
            return None
        current_service_generation = int(set_row.generation)
        if (set_row.claim_set_state != RESERVED_FILL_CLAIM_SET_AUTHORITATIVE_V2
                or
                current_service_generation > int(protocol.claim_generation) or
                len(edge_rows) != int(set_row.edge_count) or any(
                    int(edge.service_generation) != current_service_generation
                    for edge in edge_rows) or matching is None or
                matching.physical_cluster_uid != expected_physical_cluster_uid
                or transaction_replica_info.reserved_fill_service_generation
                != expected_service_generation or
                transaction_replica_info.reserved_fill_physical_cluster_uid
                != expected_physical_cluster_uid or
                not isinstance(transaction_accelerator, str)):
            return None
        sequence_table = (
            pool_capacity_observation_schema.protocol_state_sequence_table)
        attribution = (
            transaction_replica_info.reserved_fill_allocation_generation,
            transaction_replica_info.reserved_fill_allocation_input_sha256,
            transaction_replica_info.reserved_fill_allocation_claim_generation,
            transaction_replica_info.
            reserved_fill_reconciliation_gate_generation,
            transaction_replica_info.reserved_fill_reclaim_fleet_bundle_sha256,
            transaction_replica_info.reserved_fill_reclaim_policy_revision,
            transaction_replica_info.
            reserved_fill_reclaim_provider_inventory_sha256,
            transaction_replica_info.reserved_fill_observation_generation,
            transaction_replica_info.reserved_fill_observation_sequence,
            transaction_replica_info.reserved_fill_intent_idempotency_key,
        )
        if gate_state == pool_capacity_observation_schema.LEGACY_ACTIVE:
            if (set_row.service_version is not None or
                    any(edge.worker_projection_sha256_by_accelerator is not None
                        for edge in edge_rows) or
                    any(value is not None for value in attribution)):
                # A typed planner decision cannot bypass activation.
                return None
        elif gate_state == (pool_capacity_observation_schema.SEQUENCED_ACTIVE):
            try:
                matching_projection_map = (
                    _decode_reserved_fill_projection_digest_map(
                        matching.worker_projection_sha256_by_accelerator,
                        matching.accelerator_names))
            except ValueError:
                return None
            if (set_row.service_version != transaction_replica_info.version or
                    matching_projection_map.get(transaction_accelerator)
                    != transaction_replica_info.
                    reserved_fill_worker_projection_sha256 or
                    any(value is None for value in attribution)):
                # An old speculative decision cannot persist after the
                # one-way gate selects authenticated allocation maps.
                return None
            (allocation_generation, allocation_sha256,
             allocation_claim_generation, gate_generation,
             reclaim_fleet_bundle_sha256, reclaim_policy_revision,
             reclaim_provider_inventory_sha256, observation_generation,
             observation_sequence, intent_idempotency_key) = attribution
            if (type(allocation_generation) is not int or
                    allocation_generation <= 0 or
                    not isinstance(allocation_sha256, str) or
                    re.fullmatch(r'[0-9a-f]{64}', allocation_sha256) is None or
                    allocation_claim_generation != expected_service_generation
                    or type(gate_generation) is not int or
                    gate_generation <= 0 or
                    not isinstance(reclaim_fleet_bundle_sha256, str) or
                    re.fullmatch(r'[0-9a-f]{64}', reclaim_fleet_bundle_sha256)
                    is None or not isinstance(reclaim_policy_revision, str) or
                    not reclaim_policy_revision or
                    not isinstance(reclaim_provider_inventory_sha256, str) or
                    re.fullmatch(r'[0-9a-f]{64}',
                                 reclaim_provider_inventory_sha256) is None or
                    type(observation_generation) is not int or
                    observation_generation <= 0 or
                    type(observation_sequence) is not int or
                    observation_sequence < 0 or
                    not isinstance(intent_idempotency_key, str) or re.fullmatch(
                        r'[0-9a-f]{64}', intent_idempotency_key) is None):
                return None
            if (gate_generation
                    != sequence_row['reconciliation_gate_generation'] or
                    reclaim_fleet_bundle_sha256
                    != sequence_row['reclaim_fleet_bundle_sha256'] or
                    reclaim_policy_revision
                    != sequence_row['reclaim_policy_revision'] or
                    reclaim_provider_inventory_sha256
                    != sequence_row['reclaim_provider_inventory_sha256']):
                return None
            allocation_table = (pool_capacity_observation_schema.
                                reserved_fill_service_allocation_table)
            allocation_row = session.execute(
                sqlalchemy.select(allocation_table).where(
                    allocation_table.c.service_name ==
                    service_name)).mappings().one_or_none()
            if (allocation_row is None or
                    allocation_row['allocation_gate_generation']
                    != gate_generation):
                return None
            allocation_map = allocation_row['allocation_map']
            if (type(allocation_map) is not dict or allocation_map.get(
                    'ordinary_zero_cost_admission_sequence_high_water')
                    != expected_ordinary_zero_cost_admission_sequence or
                    allocation_map.get('reconciliation_gate_generation')
                    != gate_generation or
                    allocation_map.get('reclaim_fleet_bundle_sha256')
                    != reclaim_fleet_bundle_sha256 or
                    allocation_map.get('reclaim_policy_revision')
                    != reclaim_policy_revision or
                    allocation_map.get('reclaim_provider_inventory_sha256')
                    != reclaim_provider_inventory_sha256):
                return None
            current_sequence = sequence_row['zero_cost_admission_sequence']
            current_ordinary_sequence = sequence_row[
                'ordinary_zero_cost_admission_sequence']
            if (expected_ordinary_zero_cost_admission_sequence is None or
                    type(current_sequence) is not int or current_sequence < 0 or
                    type(current_ordinary_sequence) is not int or
                    current_ordinary_sequence < 0 or
                    current_ordinary_sequence > current_sequence or
                    current_ordinary_sequence
                    != expected_ordinary_zero_cost_admission_sequence or
                    current_sequence >= 2**63 - 1):
                return None
            admission_sequence = current_sequence + 1
            sequence_update = session.execute(
                sqlalchemy.update(sequence_table).where(
                    sequence_table.c.id == 1,
                    sequence_table.c.zero_cost_admission_sequence ==
                    current_sequence).values(
                        zero_cost_admission_sequence=(admission_sequence)))
            if sequence_update.rowcount != 1:
                raise RuntimeError(
                    'Reserved-fill admission sequence lost its locked '
                    'compare-and-swap.')
            transaction_replica_info.zero_cost_admission_sequence = (
                admission_sequence)
        else:
            raise RuntimeError('Reserved-fill reconciliation gate is '
                               'malformed at replica admission.')
    materializations = (_stamp_zero_cost_replica_materializations_in_session(
        session, transaction_infos))
    _apply_zero_cost_sequence_assignments(transaction_infos,
                                          materializations=materializations)
    row_values = _reserved_fill_replica_row_values(
        service_name,
        replica_id,
        transaction_replica_info,
        pool_key=pool_key,
        expected_protocol_version=expected_protocol_version)
    if row_values is None:
        return None
    insert_stmt = _upsert_insert_func(engine)(replicas_table).values(
        **row_values)
    session.execute(insert_stmt)
    if actuation_lease is not None:
        try:
            assert replica_record_id is not None
            zero_cost_actuation.commit_lease_in_connection(
                connection,
                actuation_lease,
                service_name=service_name,
                replica_id=replica_id,
                replica_record_id=replica_record_id,
                replica_info=transaction_replica_info)
        except (TypeError, ValueError,
                zero_cost_actuation.ZeroCostActuationError):
            return None

    return StagedReservedFillReplica(replica_id=replica_id,
                                     caller_info=replica_info,
                                     persisted_info=transaction_replica_info,
                                     already_committed=False)


def _persist_protocol_v1_sqlite(
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    pool_key: str,
    expected_epoch: int,
    expected_service_hash: str | None,
    expected_controller_owner: tuple[int | None, str | None] | None,
) -> bool:
    """Keep the historical local-controller protocol-v1 SQLite fence."""
    expected_protocol_version = RESERVED_FILL_PROTOCOL_V1
    with orm.Session(engine) as session:
        _reserved_fill_protocol_row_in_session(session, engine)
        session.commit()
    row_values = _reserved_fill_replica_row_values(
        service_name,
        replica_id,
        replica_info,
        pool_key=pool_key,
        expected_protocol_version=expected_protocol_version)
    if row_values is None:
        return False
    stale_round = sqlalchemy.select(
        reserved_fill_rounds_table.c.pool_key).where(
            reserved_fill_rounds_table.c.pool_key == pool_key,
            sqlalchemy.or_(
                reserved_fill_rounds_table.c.epoch != expected_epoch,
                reserved_fill_rounds_table.c.fence_pending != 0,
                reserved_fill_rounds_table.c.protocol_version
                != expected_protocol_version)).exists()
    live_claim = sqlalchemy.select(
        reserved_fill_claims_table.c.service_name).where(
            reserved_fill_claims_table.c.service_name == service_name,
            reserved_fill_claims_table.c.pool_key == pool_key).exists()
    current_protocol = sqlalchemy.select(
        reserved_fill_protocol_state_table.c.id).where(
            reserved_fill_protocol_state_table.c.id == 1,
            reserved_fill_protocol_state_table.c.protocol_version ==
            expected_protocol_version).exists()
    current_incarnation = sqlalchemy.true()
    if expected_service_hash is not None:
        owner_predicates = [
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
        ]
        if expected_controller_owner is not None:
            expected_pid, expected_ip = expected_controller_owner
            owner_predicates.extend([
                services_table.c.controller_pid == expected_pid,
                services_table.c.controller_ip == expected_ip,
            ])
        current_incarnation = sqlalchemy.select(
            services_table.c.name).where(*owner_predicates).exists()
    columns = [replicas_table.c[name] for name in row_values]
    select_stmt = sqlalchemy.select(*[
        sqlalchemy.literal(row_values[column.name], type_=column.type)
        for column in columns
    ]).where(sqlalchemy.not_(stale_round), live_claim, current_protocol,
             current_incarnation)
    insert_stmt = sqlite.insert(replicas_table).from_select(
        [column.name for column in columns], select_stmt)
    for attempt in range(_SQLITE_FENCE_BUSY_RETRIES):
        try:
            with orm.Session(engine) as session:
                persisted = session.execute(insert_stmt).rowcount > 0
                session.commit()
            return persisted
        except sqlalchemy_exc.OperationalError as e:
            if not _is_sqlite_busy_error(e):
                raise
            if attempt + 1 < _SQLITE_FENCE_BUSY_RETRIES:
                time.sleep(_SQLITE_FENCE_BUSY_BACKOFF_SECONDS * (attempt + 1))
    return False
