"""The database for services information."""
import collections
import contextlib
import enum
import json
import pickle
import time
import typing
from typing import Any, Optional
import uuid

import colorama
import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import declarative

from sky.adaptors import common as adaptors_common
from sky.serve import constants
from sky.serve import lb_ha
from sky.utils import common_utils
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

if typing.TYPE_CHECKING:
    from sqlalchemy.engine import row

    from sky.serve import replica_managers
    from sky.serve import service_spec

replica_managers = adaptors_common.LazyImport('sky.serve.replica_managers')

Base = declarative.declarative_base()

# === Database schema ===
services_table = sqlalchemy.Table(
    'services',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
    # Durable user workspace for every replica launch and recovery. The
    # controller itself may run in the system/default workspace.
    sqlalchemy.Column('workspace', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('controller_job_id',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('controller_port',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('load_balancer_port',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('status', sqlalchemy.Text),
    sqlalchemy.Column('uptime', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('policy', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('auto_restart', sqlalchemy.Integer, server_default=None),
    sqlalchemy.Column('requested_resources',
                      sqlalchemy.LargeBinary,
                      server_default=None),
    sqlalchemy.Column('requested_resources_str', sqlalchemy.Text),
    sqlalchemy.Column('current_version',
                      sqlalchemy.Integer,
                      server_default=str(constants.INITIAL_VERSION)),
    sqlalchemy.Column('active_versions',
                      sqlalchemy.Text,
                      server_default=json.dumps([])),
    sqlalchemy.Column('load_balancing_policy',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('tls_encrypted', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('pool', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('controller_pid', sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('hash', sqlalchemy.Text, server_default=None),
    # Monotonic name-fence token claimed by the lifecycle operation that most
    # recently owns this row.  Unlike ``hash`` (which changes only when the
    # service is recreated), this advances on every up/update/down/purge lock
    # acquisition.  Destructive commits validate both values.
    sqlalchemy.Column('lifecycle_epoch',
                      sqlalchemy.Integer,
                      server_default=None),
    # External resource namespace for this incarnation.  New rows store their
    # service hash here; NULL identifies a legacy row whose files, clusters,
    # and LB objects predate incarnation-scoped names.  Keeping the distinction
    # durable lets a same-name successor use a disjoint namespace without
    # moving live legacy resources during a rolling upgrade.
    sqlalchemy.Column('resource_scope', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('entrypoint', sqlalchemy.Text, server_default=None),
    # Pod IP where the controller process is running.
    # Written by the sky.serve.service process at startup.
    sqlalchemy.Column('controller_ip', sqlalchemy.Text, server_default=None),
    # Durable one-way activation fence. Logical per-GPU semantics may be
    # enabled by an update, but cannot safely be changed back to physical
    # backend counts in place. This parent-row bit makes that rule atomic with
    # a version commit and survives controller restarts/version retirement.
    sqlalchemy.Column('logical_replica_semantics',
                      sqlalchemy.Integer,
                      server_default='0'),
    # Controller-fenced warm-standby authority. External LB HA is supported
    # only on the central PostgreSQL Serve database. Existing service rows keep
    # the disabled default until an explicit migration enables the new mode.
    sqlalchemy.Column('lb_ha_enabled',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('lb_active_slot', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('lb_cutover_generation',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('lb_pending_slot', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('lb_cutover_phase',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=lb_ha.LbCutoverPhase.STABLE.value),
    sqlalchemy.Column('lb_drain_started_at', sqlalchemy.Float),
    sqlalchemy.Column('lb_demand_handoff_generation', sqlalchemy.Integer),
    sqlalchemy.Column('lb_demand_handoff_snapshot', sqlalchemy.Text),
    sqlalchemy.Column('lb_demand_handoff_complete_at', sqlalchemy.Float),
    # Latest demand reported by the selected ACTIVE slot. This is independent
    # from an in-progress handoff so a controller restart before PREPARING
    # cannot erase the scale-down floor copied into the next cutover.
    sqlalchemy.Column('lb_last_demand_snapshot', sqlalchemy.Text),
)

replicas_table = sqlalchemy.Table(
    'replicas',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('replica_info', sqlalchemy.LargeBinary),
    sqlalchemy.Column('replica_state_version', sqlalchemy.Integer),
    sqlalchemy.Column('status', sqlalchemy.Text),
    sqlalchemy.Column('sky_down_status', sqlalchemy.Text),
    sqlalchemy.Column('version', sqlalchemy.Integer),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text),
    sqlalchemy.Column('created_at', sqlalchemy.Float),
    sqlalchemy.Column('is_spot', sqlalchemy.Boolean),
    sqlalchemy.Column(
        'replica_state',
        sqlalchemy.JSON().with_variant(postgresql.JSONB(), 'postgresql')),
)
sqlalchemy.Index('replicas_service_status_idx', replicas_table.c.service_name,
                 replicas_table.c.status)

version_specs_table = sqlalchemy.Table(
    'version_specs',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
    sqlalchemy.Column('yaml_content', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('submitted_yaml_content',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('created_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('created_by', sqlalchemy.Text, server_default=None),
)

# Durable cleanup inventory is intentionally separate from ``version_specs``.
# Version rows are immutable deployment history, while cleanup intents track
# external storage ownership and survive until full service teardown.
ephemeral_storage_cleanup_intents_table = sqlalchemy.Table(
    'ephemeral_storage_cleanup_intents',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('resource_scope', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('storage_generation', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('yaml_content', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('pool', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('lifecycle_epoch', sqlalchemy.Integer, nullable=False),
    # True only until the operation has handed the generation to a committed
    # service/version. Ordinary exceptions may eagerly clean these rows;
    # committed generations remain until full service teardown.
    sqlalchemy.Column('provisional', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('created_at', sqlalchemy.Float, nullable=False),
)

serve_ha_recovery_script_table = sqlalchemy.Table(
    'serve_ha_recovery_script',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('script', sqlalchemy.Text),
)

# Per-name fencing token.  This row deliberately outlives the corresponding
# service row: deleting and recreating a name must advance, never reset, the
# token so an operation whose PostgreSQL advisory-lock session died cannot
# commit after a successor has acquired the name.
service_lifecycle_fences_table = sqlalchemy.Table(
    'service_lifecycle_fences',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('epoch', sqlalchemy.Integer, nullable=False),
)

# [boltz fork] Reserved-fill broker state (multi-service arbitration of the
# zero-cost fill pools; see sky/serve/reserved_capacity_broker.py). One claim
# row per fill-enabled service, upserted by its controller's capacity poller
# every poll interval (the heartbeat). Only FILL holdings are reported: they
# are broker property (arbitrated by grants); demand-placed zero-cost
# replicas are demand-protected, exempt from the grant ceiling, and derived
# from live replica rows where needed.
reserved_fill_claims_table = sqlalchemy.Table(
    'reserved_fill_claims',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    # json.dumps([kubernetes_context, gpu_name_lower]): the pool identity two
    # services collide on. Deliberately NOT full Location equality --
    # differing image_id/disk_tier/zone must still collide.
    sqlalchemy.Column('pool_key', sqlalchemy.Text),
    sqlalchemy.Column('weight', sqlalchemy.Float),
    sqlalchemy.Column('floor_replicas', sqlalchemy.Integer),
    # v1 requires all claimants of a pool to agree on this (mixed pools are
    # rejected); GPU-unit bookkeeping is v2.
    sqlalchemy.Column('gpus_per_replica', sqlalchemy.Integer),
    sqlalchemy.Column('holdings_fill', sqlalchemy.Integer),
    # Real capacity cap the claimant can materialize right now
    # (max(0, max_replicas - demand_target)); NULL = unbounded. The broker
    # clamps the effective floor, the headroom (weighted share above the
    # floor, derived at allocation time) and the feed need by it, so an
    # unattainable floor cannot permanently absorb entitlement and feed the
    # service never launches (its excess joins the burst remainder).
    sqlalchemy.Column('effective_cap', sqlalchemy.Integer, server_default=None),
    # Whether the claimant can launch on the pool right now (its zero-cost
    # tier is not benched): feeds to un-launchable claimants are wasted for a
    # whole round, so the feed split redistributes them.
    sqlalchemy.Column('launchable', sqlalchemy.Integer, server_default='1'),
    sqlalchemy.Column('heartbeat_ts', sqlalchemy.Float),
)

# Latest published broker round per pool (overwritten in place each round).
# Grants/feeds are the authoritative allocation record readers act on; the
# remaining columns are the broker's cross-round memory (damping baselines,
# feed stickiness, last good free measurement for blackout handling).
reserved_fill_rounds_table = sqlalchemy.Table(
    'reserved_fill_rounds',
    Base.metadata,
    sqlalchemy.Column('pool_key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('round_id', sqlalchemy.Integer),
    # Taken BEFORE the (slow) cluster query, mirroring the #108
    # snapshot/debit invariant at broker level.
    sqlalchemy.Column('snapshot_time', sqlalchemy.Float),
    # The POOL's fencing epoch: bumps only when this pool's allocation
    # changes. Readers actuating a grant compare their carried epoch
    # against it (see reserved_capacity_broker.current_epoch) -- per-pool,
    # so one pool's grant churn never fences another pool's launches.
    sqlalchemy.Column('epoch', sqlalchemy.Integer),
    # JSON {service: grant}; null grant = single-claimant fast path (no
    # ceiling, #108 identity).
    sqlalchemy.Column('grants', sqlalchemy.Text),
    # JSON {service: feed}; sum(feeds) <= observed free by construction.
    sqlalchemy.Column('feeds', sqlalchemy.Text),
    # JSON {service: raw undamped entitlement} of THIS round; next round's
    # damping baseline (a move must persist across two rounds to apply).
    sqlalchemy.Column('raw_grants', sqlalchemy.Text),
    # JSON {service: {'amount': int, 'since': ts}}: sticky feed assignments.
    sqlalchemy.Column('feed_state', sqlalchemy.Text),
    # Conserved fill holdings (live + draining) at the last MEASURED round
    # (blackout rounds carry it unchanged, staying transparent to the
    # shrink confirmation): a confirmed shrink means pods are physically
    # gone, making grant down-moves immediate (no damping).
    sqlalchemy.Column('sum_holdings', sqlalchemy.Integer),
    # Last SUCCESSFULLY measured free level + its timestamp (carried
    # unchanged through measurement blackouts, which also carry the grants
    # instead of recomputing -- a blackout must not trigger releases).
    sqlalchemy.Column('last_observed_free', sqlalchemy.Integer),
    sqlalchemy.Column('last_observed_free_ts', sqlalchemy.Float),
    # Consecutive phantom observations (successful query, no labeled nodes
    # for the claimed GPU). Persisted so the consecutive-phantom claim
    # rejection gate survives writer rotation; a non-phantom observation
    # resets it to 0.
    sqlalchemy.Column('phantom_streak', sqlalchemy.Integer, server_default='0'),
    # Pre-shrink conserved-holdings baseline of an UNCONFIRMED shrink seen
    # last round (NULL = none pending). A conserved-total shrink only
    # bypasses grant damping once it persists across two consecutive
    # rounds: a drain completing between the cluster query and the row
    # scan makes both terms omit the slot for exactly one round, and
    # firing the bypass on that phantom shrink culls a warm replica.
    sqlalchemy.Column('shrink_baseline',
                      sqlalchemy.Integer,
                      server_default=None),
    # Dead-gap fence marker: set (for every pool) atomically with a
    # POST-EXPIRY lease-token acquisition and cleared only by a successful
    # publish, which is forced to bump this pool's epoch while the marker
    # is set. Without it, a post-expiry writer that acquired its token
    # (committing a fresh expires_at) and died before publishing would
    # leave the NEXT writer seeing an unexpired lease -- with unchanged
    # grants/feeds it would republish the old epoch and launches queued
    # before the dead gap would keep passing the fence unrevalidated.
    # While set, actuation fails CLOSED: the launch fence reads it as
    # never-matching (reserved_capacity_broker.current_epoch) and the
    # atomic persist refuses (add_replica_if_round_epoch), so a pool that
    # never publishes again (claims gone) cannot leak a pre-gap launch.
    sqlalchemy.Column('fence_pending', sqlalchemy.Integer, server_default='0'),
)

# Singleton lease row (id=1). The epoch only moves forward; it is the round
# writer's OWNERSHIP TOKEN and the round's ENTRY POINT: CAS-advanced (and
# committed) BEFORE the writer reads any claim/round state and before its
# slow cluster query, and the publish only lands while the lease still
# holds that exact token -- so everything a successful publish persisted
# was read AFTER the token (see acquire_reserved_fill_lease_token). A
# replacement writer (e.g. after the original's advisory-lock session died
# mid-query) advances it again, so the stale writer's publish fails and
# its observation is discarded.
# Fencing for actuation is the per-pool round epoch above.
reserved_fill_lease_table = sqlalchemy.Table(
    'reserved_fill_lease',
    Base.metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('epoch', sqlalchemy.Integer),
    sqlalchemy.Column('expires_at', sqlalchemy.Float),
)

# Shared raw Kubernetes accelerator observations used by demand placement.
# One row per context lets every service/controller reuse the same expensive
# cluster-wide query. ``availability`` is JSON {gpu_name_lower: free_gpus};
# NULL records a failed query and rate-limits retry storms while preserving
# the important distinction from a successful empty/zero observation.
# ``snapshot_time`` is the query start used to debit replicas that raced the
# observation; ``completed_at`` is the freshness/rate-limit clock, so a slow
# query does not publish a result that is immediately stale.
demand_capacity_observations_table = sqlalchemy.Table(
    'demand_capacity_observations',
    Base.metadata,
    sqlalchemy.Column('context', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('snapshot_time', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('completed_at', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('availability', sqlalchemy.Text, server_default=None),
)


def create_table(engine: sqlalchemy.engine.Engine):
    """Creates the service and replica tables if they do not exist."""

    # Enable WAL mode to avoid locking issues.
    # See: issue #3863, #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value and
            not common_utils.is_wsl()):
        try:
            with orm.Session(engine) as session:
                session.execute(sqlalchemy.text('PRAGMA journal_mode=WAL'))
                session.commit()
        except sqlalchemy_exc.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         migration_utils.SERVE_VERSION)


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


def service_lifecycle_epoch_matches(service_name: str, epoch: int) -> bool:
    """Whether ``epoch`` is still the latest token for ``service_name``."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(service_lifecycle_fences_table.c.epoch).where(
                service_lifecycle_fences_table.c.name ==
                service_name)).fetchone()
    return row is not None and int(row[0]) == epoch


def _lifecycle_epoch_matches_in_session(session: orm.Session, service_name: str,
                                        epoch: int | None) -> bool:
    """Lock and validate a lifecycle fence row inside a mutation txn."""
    if epoch is None:
        # Compatibility for old direct/unit-test callers. Production lifecycle
        # entrypoints always supply an epoch.
        return True
    stmt = sqlalchemy.select(service_lifecycle_fences_table.c.epoch).where(
        service_lifecycle_fences_table.c.name == service_name)
    if session.bind is not None and session.bind.dialect.name == (
            db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        stmt = stmt.with_for_update()
    row = session.execute(stmt).fetchone()
    return row is not None and int(row[0]) == epoch


def _begin_immediate_if_sqlite(session: orm.Session,
                               engine: sqlalchemy.engine.Engine,
                               enabled: bool = True) -> None:
    """Make a SQLite read-then-write fence one atomic writer transaction."""
    if (enabled and
            engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value):
        session.execute(sqlalchemy.text('BEGIN IMMEDIATE'))


_db_manager = db_utils.DatabaseManager('serve/services', create_table)


def ensure_tables_initialized() -> None:
    """Run pending Serve DB migrations before raw lock-session SQL."""
    _db_manager.get_engine()


def get_database_engine() -> sqlalchemy.engine.Engine:
    """Return the initialized database engine for Serve state."""
    return _db_manager.get_engine()


_UNIQUE_CONSTRAINT_FAILED_ERROR_MSGS = [
    # sqlite
    'UNIQUE constraint failed: services.name',
    # postgres
    'duplicate key value violates unique constraint "services_pkey"',
]


# === Statuses ===
class ReplicaStatus(enum.Enum):
    """Replica status."""

    # The `sky.launch` is pending due to max number of simultaneous launches.
    PENDING = 'PENDING'

    # The replica VM is being provisioned. i.e., the `sky.launch` is still
    # running.
    PROVISIONING = 'PROVISIONING'

    # The replica VM is provisioned and the service is starting. This indicates
    # user's `setup` section or `run` section is still running, and the
    # readiness probe fails.
    STARTING = 'STARTING'

    # The replica VM is provisioned and the service is ready, i.e. the
    # readiness probe is passed.
    READY = 'READY'

    # The service was ready before, but it becomes not ready now, i.e. the
    # readiness probe fails.
    NOT_READY = 'NOT_READY'

    # The replica VM is being shut down. i.e., the `sky down` is still running.
    SHUTTING_DOWN = 'SHUTTING_DOWN'

    # The replica fails due to user's run/setup.
    FAILED = 'FAILED'

    # The replica fails due to initial delay exceeded.
    FAILED_INITIAL_DELAY = 'FAILED_INITIAL_DELAY'

    # The replica fails due to healthiness check.
    FAILED_PROBING = 'FAILED_PROBING'

    # The replica fails during launching
    FAILED_PROVISION = 'FAILED_PROVISION'

    # `sky.down` failed during service teardown.
    # This could mean resource leakage.
    # TODO(tian): This status should be removed in the future, at which point
    # we should guarantee no resource leakage like regular sky.
    FAILED_CLEANUP = 'FAILED_CLEANUP'

    # The replica's underlying capacity was interrupted by the provider, such
    # as a spot VM preemption or zero-cost Kubernetes pod reclamation.
    PREEMPTED = 'PREEMPTED'

    # Unknown. This should never happen (used only for unexpected errors).
    UNKNOWN = 'UNKNOWN'

    @classmethod
    def failed_statuses(cls) -> list['ReplicaStatus']:
        return [
            cls.FAILED, cls.FAILED_CLEANUP, cls.FAILED_INITIAL_DELAY,
            cls.FAILED_PROBING, cls.FAILED_PROVISION, cls.UNKNOWN
        ]

    @classmethod
    def terminal_statuses(cls) -> list['ReplicaStatus']:
        return [cls.SHUTTING_DOWN, cls.PREEMPTED, cls.UNKNOWN
               ] + cls.failed_statuses()

    @classmethod
    def scale_down_decision_order(cls) -> list['ReplicaStatus']:
        # Scale down replicas in the order of replica initialization
        return [
            cls.PENDING, cls.PROVISIONING, cls.STARTING, cls.NOT_READY,
            cls.READY
        ]

    def colored_str(self) -> str:
        color = _REPLICA_STATUS_TO_COLOR[self]
        return f'{color}{self.value}{colorama.Style.RESET_ALL}'


_REPLICA_STATUS_TO_COLOR = {
    ReplicaStatus.PENDING: colorama.Fore.YELLOW,
    ReplicaStatus.PROVISIONING: colorama.Fore.BLUE,
    ReplicaStatus.STARTING: colorama.Fore.CYAN,
    ReplicaStatus.READY: colorama.Fore.GREEN,
    ReplicaStatus.NOT_READY: colorama.Fore.YELLOW,
    ReplicaStatus.SHUTTING_DOWN: colorama.Fore.MAGENTA,
    ReplicaStatus.FAILED: colorama.Fore.RED,
    ReplicaStatus.FAILED_INITIAL_DELAY: colorama.Fore.RED,
    ReplicaStatus.FAILED_PROBING: colorama.Fore.RED,
    ReplicaStatus.FAILED_PROVISION: colorama.Fore.RED,
    ReplicaStatus.FAILED_CLEANUP: colorama.Fore.RED,
    ReplicaStatus.PREEMPTED: colorama.Fore.MAGENTA,
    ReplicaStatus.UNKNOWN: colorama.Fore.RED,
}


class ServiceStatus(enum.Enum):
    """Service status as recorded in table 'services'."""

    # Controller is initializing
    CONTROLLER_INIT = 'CONTROLLER_INIT'

    # Replica is initializing and no failure
    REPLICA_INIT = 'REPLICA_INIT'

    # Controller failed to initialize / controller or load balancer process
    # status abnormal
    CONTROLLER_FAILED = 'CONTROLLER_FAILED'

    # At least one replica is ready
    READY = 'READY'

    # Service is being shutting down
    SHUTTING_DOWN = 'SHUTTING_DOWN'

    # At least one replica is failed and no replica is ready
    FAILED = 'FAILED'

    # Clean up failed
    FAILED_CLEANUP = 'FAILED_CLEANUP'

    # No replica
    NO_REPLICA = 'NO_REPLICA'

    @classmethod
    def failed_statuses(cls) -> list['ServiceStatus']:
        return [cls.CONTROLLER_FAILED, cls.FAILED_CLEANUP]

    @classmethod
    def terminal_statuses(cls) -> list['ServiceStatus']:
        """States in which the service is either dying or already broken
        and cannot accept new operations like update/apply. SHUTTING_DOWN
        is included because it's a transient state that the service may
        never leave on its own (the previous cleanup may have died
        mid-flight, leaving a zombie row — see _cleanup)."""
        return [cls.CONTROLLER_FAILED, cls.FAILED_CLEANUP, cls.SHUTTING_DOWN]

    @classmethod
    def replica_launch_blocking_statuses(cls) -> list['ServiceStatus']:
        """States that durably fence new replica provisioning.

        CONTROLLER_FAILED is intentionally excluded: it is a recoverable data
        plane/controller degradation under the same live owner, which may need
        to launch replacement replicas before the parent heals the status.
        """
        return [cls.FAILED_CLEANUP, cls.SHUTTING_DOWN]

    def colored_str(self) -> str:
        color = _SERVICE_STATUS_TO_COLOR[self]
        return f'{color}{self.value}{colorama.Style.RESET_ALL}'

    @classmethod
    def from_replica_statuses(
            cls, replica_statuses: list[ReplicaStatus]) -> 'ServiceStatus':
        status2num = collections.Counter(replica_statuses)
        # If one replica is READY, the service is READY.
        if status2num[ReplicaStatus.READY] > 0:
            return cls.READY
        if sum(status2num[status]
               for status in ReplicaStatus.failed_statuses()) > 0:
            return cls.FAILED
        # When min_replicas = 0, there is no (provisioning) replica.
        if not replica_statuses:
            return cls.NO_REPLICA
        return cls.REPLICA_INIT


_SERVICE_STATUS_TO_COLOR = {
    ServiceStatus.CONTROLLER_INIT: colorama.Fore.BLUE,
    ServiceStatus.REPLICA_INIT: colorama.Fore.BLUE,
    ServiceStatus.CONTROLLER_FAILED: colorama.Fore.RED,
    ServiceStatus.READY: colorama.Fore.GREEN,
    ServiceStatus.SHUTTING_DOWN: colorama.Fore.YELLOW,
    ServiceStatus.FAILED: colorama.Fore.RED,
    ServiceStatus.FAILED_CLEANUP: colorama.Fore.RED,
    ServiceStatus.NO_REPLICA: colorama.Fore.MAGENTA,
}


class OrphanedReplicaRecordsError(RuntimeError):
    """Fresh registration found replica inventory without a service row."""


class OrphanedStorageCleanupIntentsError(RuntimeError):
    """Fresh registration found predecessor storage awaiting cleanup."""


class OrphanedVersionRecordsError(RuntimeError):
    """Fresh registration found predecessor version cleanup inventory."""


def _ephemeral_storage_generation_from_yaml(
        yaml_content: str | None) -> str | None:
    """Read the scoped storage generation without constructing a Task."""
    if yaml_content is None:
        return None
    try:
        config = yaml_utils.safe_load(yaml_content)
    except Exception:  # pylint: disable=broad-except
        return None
    if not isinstance(config, dict):
        return None
    metadata = config.get('metadata')
    if not isinstance(metadata, dict):
        return None
    scope_metadata = metadata.get(
        constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY)
    if not isinstance(scope_metadata, dict):
        return None
    storage_generation = scope_metadata.get('storage_generation')
    return storage_generation if isinstance(storage_generation, str) else None


def add_service(name: str,
                controller_job_id: int,
                policy: str,
                requested_resources_str: str,
                load_balancing_policy: str,
                status: ServiceStatus,
                tls_encrypted: bool,
                pool: bool,
                controller_pid: int,
                entrypoint: str,
                spec: Optional['service_spec.SkyServiceSpec'],
                yaml_content: str,
                workspace: str | None = None,
                controller_ip: str | None = None,
                service_hash: str | None = None,
                lifecycle_epoch: int | None = None,
                resource_scope: str | None = None,
                created_by: str | None = None,
                submitted_yaml_content: str | None = None) -> bool:
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
    engine = _db_manager.get_engine()
    lb_ha_enabled = bool(spec is not None and
                         getattr(spec, 'lb_high_availability', False))
    if (lb_ha_enabled and
            engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise RuntimeError('External load balancer high availability requires '
                           'the central PostgreSQL Serve database.')
    storage_generation = _ephemeral_storage_generation_from_yaml(yaml_content)
    try:
        with orm.Session(engine) as session:
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
                    lifecycle_epoch=lifecycle_epoch,
                    resource_scope=resource_scope,
                    entrypoint=entrypoint,
                    logical_replica_semantics=int(
                        getattr(spec, 'uses_logical_replicas', False) is True),
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
            version_insert_stmt = insert_func(version_specs_table).values(
                service_name=name,
                version=constants.INITIAL_VERSION,
                spec=pickle.dumps(spec),
                yaml_content=yaml_content,
                submitted_yaml_content=submitted_yaml_content,
                created_at=time.time(),
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
                            version_insert_stmt.excluded.submitted_yaml_content
                    })
            session.execute(version_insert_stmt)
            if resource_scope is not None and storage_generation is not None:
                session.query(ephemeral_storage_cleanup_intents_table).filter(
                    ephemeral_storage_cleanup_intents_table.c.service_name ==
                    name,
                    ephemeral_storage_cleanup_intents_table.c.resource_scope ==
                    resource_scope,
                    ephemeral_storage_cleanup_intents_table.c.storage_generation
                    == storage_generation,
                    *([
                        ephemeral_storage_cleanup_intents_table.c.
                        lifecycle_epoch == lifecycle_epoch
                    ] if lifecycle_epoch is not None else []),
                ).update(
                    {ephemeral_storage_cleanup_intents_table.c.provisional: 0})
            session.commit()

    except sqlalchemy_exc.IntegrityError as e:
        for msg in _UNIQUE_CONSTRAINT_FAILED_ERROR_MSGS:
            if msg in str(e):
                return False
        raise RuntimeError('Unexpected database error') from e
    return True


def update_service_controller_pid_if_owner(service_name: str,
                                           expected_service_hash: str | None,
                                           expected_controller_pid: int | None,
                                           expected_controller_ip: str | None,
                                           controller_pid: int,
                                           controller_ip: str | None) -> bool:
    """Preclaim recovery only if the incarnation and old owner still match.

    A name-only preclaim can overwrite a service that was purged and recreated
    while recovery was loading its spec. The hash fences same-name successors;
    the expected PID+IP fence another recovery process that already claimed the
    original incarnation (PIDs alone collide across Kubernetes pods). On
    success, publish the new PID+IP and clear the port atomically so the stable
    proxy fails closed with 503 until the new controller is actually ready.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == expected_controller_pid,
            services_table.c.controller_ip == expected_controller_ip).update({
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update(
                {services_table.c.controller_ip: controller_ip})
        session.commit()


def remove_service(service_name: str) -> None:
    """Removes a service from the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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


def set_service_status_and_active_versions(
        service_name: str,
        status: ServiceStatus,
        active_versions: list[int] | None = None) -> None:
    """Sets the service status."""
    engine = _db_manager.get_engine()
    update_dict = {services_table.c.status: status.value}
    if active_versions is not None:
        update_dict[services_table.c.active_versions] = json.dumps(
            active_versions)

    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update(update_dict)
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, expected_lifecycle_epoch
                                   is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        count = session.query(services_table).filter(
            *predicates).update(update_dict)
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(session, engine, expected_lifecycle_epoch
                                   is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        count = session.query(services_table).filter(
            *predicates).update(update_dict)
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
        - services_table.*
        - spec/yaml_content (from version_specs_table for latest version)
    """
    subquery = sqlalchemy.select(
        version_specs_table.c.service_name,
        sqlalchemy.func.max(version_specs_table.c.version).label('max_version'),
    ).group_by(version_specs_table.c.service_name).alias('v')

    query = sqlalchemy.select(
        subquery.c.max_version,
        services_table,
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


def get_num_services() -> int:
    """Get the number of services."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        return session.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(services_table)).fetchone()[0]


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
    """
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
            ).where(
                services_table.c.pool == int(pool),
                sqlalchemy.exists().where(version_specs_table.c.service_name ==
                                          services_table.c.name),
            ).order_by(services_table.c.name)).fetchall()
    return [{
        'name': row.name,
        'status': ServiceStatus[row.status],
        'controller_job_id': row.controller_job_id,
        'controller_pid': row.controller_pid,
        'controller_ip': row.controller_ip,
        'hash': row.hash,
        'resource_scope': row.resource_scope,
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
    }


def get_service_controller_owner(
        service_name: str,
        require_version: bool = False,
        include_lb_state: bool = False) -> dict[str, Any] | None:
    """Get only the fields needed to route to a service controller.

    Unlike :func:`get_service_from_name`, this hot-path lookup does not join
    ``version_specs``, deserialize the latest spec, or issue a second query.
    The service hash distinguishes a same-name successor from the row read
    before a proxied request; status lets the proxy reject terminal rows.
    ``require_version`` preserves callers whose old joined read treated an
    orphan/versionless service row as missing, using an indexed existence
    check without loading version metadata. ``include_lb_state`` adds the
    cutover fields only for HA lifecycle callers, keeping the routing identity
    contract small for all other hot paths.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            services_table.c.hash,
            services_table.c.status,
            services_table.c.controller_pid,
            services_table.c.controller_ip,
            services_table.c.controller_port,
            services_table.c.lifecycle_epoch,
            services_table.c.pool,
            services_table.c.resource_scope,
            services_table.c.lb_ha_enabled,
            services_table.c.lb_active_slot,
            services_table.c.lb_cutover_generation,
            services_table.c.lb_pending_slot,
            services_table.c.lb_cutover_phase,
        ).where(services_table.c.name == service_name)
        if require_version:
            query = query.where(sqlalchemy.exists().where(
                version_specs_table.c.service_name == services_table.c.name))
        row = session.execute(query).fetchone()
    if row is None:
        return None
    mapping = row._mapping  # pylint: disable=protected-access
    record = {
        'hash': mapping['hash'],
        'status': ServiceStatus[mapping['status']],
        'controller_pid': mapping['controller_pid'],
        'controller_ip': mapping['controller_ip'],
        'controller_port': mapping['controller_port'],
        'lifecycle_epoch': mapping['lifecycle_epoch'],
        'pool': bool(mapping['pool']),
        'resource_scope': mapping['resource_scope'],
    }
    if include_lb_state:
        record.update({
            'lb_ha_enabled': bool(mapping['lb_ha_enabled']),
            'lb_active_slot': mapping['lb_active_slot'],
            'lb_cutover_generation': mapping['lb_cutover_generation'],
            'lb_pending_slot': mapping['lb_pending_slot'],
            'lb_cutover_phase': mapping['lb_cutover_phase'],
        })
    return record


def _require_postgresql_lb_cutover(engine: sqlalchemy.engine.Engine) -> None:
    if (engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise RuntimeError('External load balancer HA cutover state is '
                           'supported only on PostgreSQL.')


def get_lb_cutover_state(service_name: str) -> lb_ha.LbCutoverState | None:
    """Read and validate one service's durable LB authority state."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.lb_ha_enabled,
                services_table.c.lb_active_slot,
                services_table.c.lb_cutover_generation,
                services_table.c.lb_pending_slot,
                services_table.c.lb_cutover_phase,
                services_table.c.lb_drain_started_at,
                services_table.c.lifecycle_epoch,
            ).where(services_table.c.name == service_name)).fetchone()
    if row is None:
        return None
    enabled = bool(row.lb_ha_enabled)
    if enabled:
        _require_postgresql_lb_cutover(engine)
    active_slot = lb_ha.parse_slot(row.lb_active_slot)
    pending_slot = lb_ha.parse_slot(row.lb_pending_slot)
    phase = lb_ha.parse_phase(row.lb_cutover_phase)
    generation = row.lb_cutover_generation
    if (phase is None or not isinstance(generation, int) or generation < 0 or
        (enabled and (active_slot is None or generation < 1)) or
        (not enabled and
         (active_slot is not None or generation != 0 or pending_slot is not None
          or phase is not lb_ha.LbCutoverPhase.STABLE)) or
        (phase is lb_ha.LbCutoverPhase.PREPARING and pending_slot is None) or
        (phase is lb_ha.LbCutoverPhase.DRAINING and pending_slot is None)):
        raise RuntimeError(f'Malformed LB cutover state for {service_name!r}.')
    return lb_ha.LbCutoverState(enabled=enabled,
                                active_slot=active_slot,
                                generation=generation,
                                pending_slot=pending_slot,
                                phase=phase,
                                lifecycle_epoch=row.lifecycle_epoch,
                                drain_started_at=row.lb_drain_started_at)


def _lb_cutover_owner_predicates(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> list[Any]:
    return [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
        services_table.c.controller_pid == expected_controller_owner[0],
        services_table.c.controller_ip == expected_controller_owner[1],
        services_table.c.lifecycle_epoch == expected_lifecycle_epoch,
        services_table.c.lb_ha_enabled == 1,
    ]


def begin_lb_ha_migration(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> bool:
    """Durably enter legacy-to-two-slot migration without moving traffic."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == expected_controller_owner[0],
            services_table.c.controller_ip == expected_controller_owner[1],
            services_table.c.lifecycle_epoch == expected_lifecycle_epoch,
            services_table.c.lb_ha_enabled == 0,
            services_table.c.lb_active_slot.is_(None),
            services_table.c.lb_cutover_generation == 0,
            services_table.c.lb_pending_slot.is_(None),
            services_table.c.lb_cutover_phase ==
            lb_ha.LbCutoverPhase.STABLE.value,
        ).update({
            services_table.c.lb_ha_enabled: 1,
            services_table.c.lb_active_slot: lb_ha.LbSlot.A.value,
            services_table.c.lb_cutover_generation: 1,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.MIGRATING.value,
        })
        session.commit()
    return count == 1


def finish_lb_ha_migration(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> bool:
    """Commit slot A after the stable Service selector has moved to it."""
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == lb_ha.LbSlot.A.value,
        services_table.c.lb_cutover_generation == 1,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.MIGRATING.value,
    ])
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
        })
        session.commit()
    return count == 1


def begin_lb_ha_rollback(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Durably enter two-slot-to-legacy rollback without moving traffic."""
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase == lb_ha.LbCutoverPhase.STABLE.value,
    ])
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.ROLLING_BACK.value,
        })
        session.commit()
    return count == 1


def finish_lb_ha_rollback(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Disable HA after the stable Service selector has moved to legacy."""
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.ROLLING_BACK.value,
    ])
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_ha_enabled: 0,
            services_table.c.lb_active_slot: None,
            services_table.c.lb_cutover_generation: 0,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
            services_table.c.lb_drain_started_at: None,
            services_table.c.lb_demand_handoff_generation: None,
            services_table.c.lb_demand_handoff_snapshot: None,
            services_table.c.lb_demand_handoff_complete_at: None,
            services_table.c.lb_last_demand_snapshot: None,
        })
        session.commit()
    return count == 1


def begin_lb_cutover(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    expected_active_slot: lb_ha.LbSlot,
    expected_generation: int,
    target_slot: lb_ha.LbSlot,
    demand_snapshot: lb_ha.DemandSnapshot | None = None,
) -> lb_ha.LbCutoverState | None:
    """CAS STABLE N to PREPARING N+1 for the opposite slot."""
    if target_slot is not expected_active_slot.other:
        raise ValueError('LB cutover target must be the opposite slot.')
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    next_generation = expected_generation + 1
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == expected_active_slot.value,
        services_table.c.lb_cutover_generation == expected_generation,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase == lb_ha.LbCutoverPhase.STABLE.value,
    ])
    with orm.Session(engine) as session:
        serialized_snapshot = (json.dumps(demand_snapshot.to_dict())
                               if demand_snapshot is not None else
                               services_table.c.lb_last_demand_snapshot)
        row = session.execute(
            sqlalchemy.update(services_table).where(*predicates).values(
                lb_pending_slot=target_slot.value,
                lb_cutover_generation=next_generation,
                lb_cutover_phase=lb_ha.LbCutoverPhase.PREPARING.value,
                lb_demand_handoff_generation=next_generation,
                lb_demand_handoff_snapshot=serialized_snapshot,
                lb_demand_handoff_complete_at=None).returning(
                    services_table.c.lifecycle_epoch)).fetchone()
        session.commit()
    if row is None:
        return None
    return lb_ha.LbCutoverState(enabled=True,
                                active_slot=expected_active_slot,
                                generation=next_generation,
                                pending_slot=target_slot,
                                phase=lb_ha.LbCutoverPhase.PREPARING,
                                lifecycle_epoch=row.lifecycle_epoch)


def record_lb_active_demand_snapshot(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    generation: int,
    demand_snapshot: lb_ha.DemandSnapshot,
) -> bool:
    """Persist demand only while the reporter remains the selected ACTIVE."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_cutover_phase.in_((
            lb_ha.LbCutoverPhase.STABLE.value,
            lb_ha.LbCutoverPhase.DRAINING.value,
        )),
    ])
    serialized_snapshot = json.dumps(demand_snapshot.to_dict())
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_last_demand_snapshot: serialized_snapshot,
        })
        session.commit()
    return count == 1


def get_lb_last_demand_snapshot(
        service_name: str) -> lb_ha.DemandSnapshot | None:
    """Read the restart-safe latest demand from the selected ACTIVE slot."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(services_table.c.lb_last_demand_snapshot).where(
                services_table.c.name == service_name)).fetchone()
    if row is None or row.lb_last_demand_snapshot is None:
        return None
    try:
        return lb_ha.DemandSnapshot.from_dict(
            json.loads(row.lb_last_demand_snapshot))
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        raise RuntimeError('Malformed durable LB demand snapshot.') from e


def commit_lb_cutover(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    previous_slot: lb_ha.LbSlot,
    target_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Commit a selector-switched target and retain the old slot as DRAINING."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == previous_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot == target_slot.value,
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.PREPARING.value,
    ])
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_active_slot: target_slot.value,
            services_table.c.lb_pending_slot: previous_slot.value,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.DRAINING.value,
            services_table.c.lb_drain_started_at: time.time(),
        })
        session.commit()
    return count == 1


def finish_lb_cutover_drain(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    draining_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """CAS DRAINING to STABLE after every former stream owner is clean/gone."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot == draining_slot.value,
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.DRAINING.value,
    ])
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_pending_slot: None,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
            services_table.c.lb_drain_started_at: None,
        })
        session.commit()
    return count == 1


def get_lb_demand_handoff(
    service_name: str,
) -> tuple[int | None, lb_ha.DemandSnapshot | None, float | None]:
    """Read the restart-safe demand floor for the current promotion."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.lb_demand_handoff_generation,
                services_table.c.lb_demand_handoff_snapshot,
                services_table.c.lb_demand_handoff_complete_at,
            ).where(services_table.c.name == service_name)).fetchone()
    if row is None:
        return None, None, None
    snapshot = None
    if row.lb_demand_handoff_snapshot is not None:
        try:
            snapshot = lb_ha.DemandSnapshot.from_dict(
                json.loads(row.lb_demand_handoff_snapshot))
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            raise RuntimeError('Malformed durable LB demand handoff.') from e
    return (row.lb_demand_handoff_generation, snapshot,
            row.lb_demand_handoff_complete_at)


def mark_lb_demand_handoff_complete(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    generation: int,
) -> float | None:
    """Record the first complete report from the promoted active."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_demand_handoff_generation == generation,
    ])
    completed_at = time.time()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.update(services_table).where(
                *predicates,
                services_table.c.lb_demand_handoff_complete_at.is_(None)).
            values(lb_demand_handoff_complete_at=completed_at).returning(
                services_table.c.lb_demand_handoff_complete_at)).fetchone()
        if row is None:
            existing = session.execute(
                sqlalchemy.select(
                    services_table.c.lb_demand_handoff_complete_at).where(
                        *predicates)).scalar_one_or_none()
            session.rollback()
            return existing
        session.commit()
    return float(row.lb_demand_handoff_complete_at)


def clear_lb_demand_handoff(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    generation: int,
) -> bool:
    """Clear an expired demand floor without touching cutover authority."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.append(
        services_table.c.lb_demand_handoff_generation == generation)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_demand_handoff_generation: None,
            services_table.c.lb_demand_handoff_snapshot: None,
            services_table.c.lb_demand_handoff_complete_at: None,
        })
        session.commit()
    return count == 1


@contextlib.contextmanager
def lb_cutover_kubernetes_guard(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    expected_active_slot: lb_ha.LbSlot,
    expected_generation: int,
    expected_phase: lb_ha.LbCutoverPhase,
    expected_pending_slot: lb_ha.LbSlot | None,
):
    """Hold the service row lock across one external Kubernetes mutation.

    Controller ownership updates write the same PostgreSQL row and therefore
    wait for this transaction. This closes the otherwise unavoidable window
    in which a stale controller could pass a DB check and patch the Service
    selector after its successor took ownership.
    """
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == expected_active_slot.value,
        services_table.c.lb_cutover_generation == expected_generation,
        services_table.c.lb_cutover_phase == expected_phase.value,
        (services_table.c.lb_pending_slot.is_(None)
         if expected_pending_slot is None else services_table.c.lb_pending_slot
         == expected_pending_slot.value),
    ])
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(services_table.c.name).where(
                *predicates).with_for_update()).fetchone()
        try:
            yield row is not None
        finally:
            session.rollback()


def abort_lb_cutover_preparation(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    target_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Abort an unselected armed target without reusing its generation."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot == target_slot.value,
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.PREPARING.value,
    ])
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_pending_slot: None,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
            services_table.c.lb_demand_handoff_generation: None,
            services_table.c.lb_demand_handoff_snapshot: None,
            services_table.c.lb_demand_handoff_complete_at: None,
        })
        session.commit()
    return count == 1


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
        patterns = [name.replace('*', '%') for name in service_names]
        query = query.where(
            sqlalchemy.or_(*[
                child_names.c.service_name.like(pattern) for pattern in patterns
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
                              version_specs_table.c.yaml_content).where(
                                  version_specs_table.c.service_name ==
                                  service_name)).fetchall()
    for replica_state in replica_rows:
        try:
            modes.add(replica_state['replica_port'] == '-')
        except Exception:  # pylint: disable=broad-except
            return None
    for spec_bytes, yaml_content in version_rows:
        try:
            spec = pickle.loads(spec_bytes)
            if spec is not None and hasattr(spec, 'pool'):
                modes.add(bool(spec.pool))
                continue
        except Exception:  # pylint: disable=broad-except
            pass
        try:
            config = yaml_utils.safe_load(yaml_content)
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
            sqlalchemy.select(version_specs_table.c.version.distinct()).where(
                version_specs_table.c.service_name == service_name)).fetchall()
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
            sqlalchemy.or_(
                *(services_table.c.name.like(service_name.replace('*', '%'))
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

# 999 (the oldest SQLITE_MAX_VARIABLE_NUMBER default) // 11 fields per row,
# rounded down for headroom.
_REPLICA_UPSERT_CHUNK_SIZE = 90
_POSTGRESQL_REPLICA_UPSERT_CHUNK_SIZE = 300
_REPLICA_STATE_VERSION = 1


def _replica_row_values(
        service_name: str, replica_id: int,
        replica_info: 'replica_managers.ReplicaInfo') -> dict[str, Any]:
    """Build the legacy rollback blob and the authoritative query state."""
    replica_state = replica_info.to_storage_dict()
    sky_down_status = replica_info.status_property.sky_down_status
    return {
        'service_name': service_name,
        'replica_id': replica_id,
        # TODO(fcapponi): After 2026-07-20, delete the pickle column and this
        # dual-write once production validation confirms every row has a
        # supported replica_state_version and JSON/pickle parity.
        'replica_info': pickle.dumps(replica_info),
        'replica_state_version': _REPLICA_STATE_VERSION,
        'status': replica_info.status.value,
        'sky_down_status':
            (sky_down_status.value if sky_down_status is not None else None),
        'version': replica_info.version,
        'cluster_name': replica_info.cluster_name,
        'created_at': getattr(replica_info, 'created_at', None),
        'is_spot': replica_info.is_spot,
        'replica_state': replica_state,
    }


def _replica_from_state(
        replica_state_version: int,
        replica_state: dict[str, Any]) -> 'replica_managers.ReplicaInfo':
    if replica_state_version != _REPLICA_STATE_VERSION:
        raise RuntimeError('Unsupported replica state version: '
                           f'{replica_state_version!r}')
    return replica_managers.ReplicaInfo.from_storage_dict(replica_state)


def add_or_update_replica(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Adds a replica to the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(
            session, engine, expected_service_hash is not None or
            expected_lifecycle_epoch is not None or
            expected_controller_owner is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        if expected_service_hash is not None:
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
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
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmt = insert_func(replicas_table).values(
            **_replica_row_values(service_name, replica_id, replica_info))

        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['service_name', 'replica_id'],
            set_={
                column.name: getattr(insert_stmt.excluded, column.name)
                for column in replicas_table.c
                if column.name not in ('service_name', 'replica_id')
            })

        session.execute(insert_stmt)
        session.commit()
    return True


def add_or_update_replicas(
    service_name: str,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Upserts a batch of replicas in one statement/transaction.

    The probe round persists per-replica bookkeeping for every probed
    replica; issuing those as individual upserts serializes one DB
    round-trip per replica under the replica-manager lock (at ~1k replicas
    on Postgres that alone exceeds the probe period). Multi-row
    ON CONFLICT upsert keeps the round O(1) in round-trips.
    """
    if not replica_infos:
        return True
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(
            session, engine, expected_service_hash is not None or
            expected_lifecycle_epoch is not None or
            expected_controller_owner is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        if expected_service_hash is not None:
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
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
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        # Older SQLite builds cap SQLITE_MAX_VARIABLE_NUMBER at 999, while
        # PostgreSQL can preserve the prior 300-row batches. Keep the SQLite
        # chunk below 999 / 11 bind params without slowing the production
        # PostgreSQL probe loop.
        chunk_size = (_REPLICA_UPSERT_CHUNK_SIZE if engine.dialect.name
                      == db_utils.SQLAlchemyDialect.SQLITE.value else
                      _POSTGRESQL_REPLICA_UPSERT_CHUNK_SIZE)
        for start in range(0, len(replica_infos), chunk_size):
            chunk = replica_infos[start:start + chunk_size]
            insert_stmt = insert_func(replicas_table).values([
                _replica_row_values(service_name, replica_id, replica_info)
                for replica_id, replica_info in chunk
            ])

            insert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['service_name', 'replica_id'],
                set_={
                    column.name: getattr(insert_stmt.excluded, column.name)
                    for column in replicas_table.c
                    if column.name not in ('service_name', 'replica_id')
                })

            session.execute(insert_stmt)
        session.commit()
    return True


def remove_replica(
    service_name: str,
    replica_id: int,
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Remove a replica, optionally fenced to one lifecycle/incarnation."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _begin_immediate_if_sqlite(
            session, engine, expected_service_hash is not None or
            expected_lifecycle_epoch is not None or
            expected_controller_owner is not None)
        if not _lifecycle_epoch_matches_in_session(session, service_name,
                                                   expected_lifecycle_epoch):
            session.rollback()
            return False
        predicates = [
            replicas_table.c.service_name == service_name,
            replicas_table.c.replica_id == replica_id,
        ]
        if expected_service_hash is not None:
            owner = session.execute(
                sqlalchemy.select(
                    services_table.c.hash, services_table.c.lifecycle_epoch,
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
        result = session.execute(
            sqlalchemy.delete(replicas_table).where(*predicates))
        session.commit()
    # Once exact ownership is proven, an already-absent child is the desired
    # idempotent cleanup state, not evidence of ownership loss.
    return expected_service_hash is not None or result.rowcount > 0


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
            spec=pickle.dumps(None),
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
    expected_service_hash: str | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> VersionCommitResult:
    """Commit a version placeholder once, or accept an identical retry.

    A non-NULL YAML row is immutable: replica rows and controller recovery use
    the version number as its identity. Overwriting its content could leave a
    live controller running one spec while a respawn boots another.
    """
    engine = _db_manager.get_engine()
    storage_generation = _ephemeral_storage_generation_from_yaml(yaml_content)
    resource_scope: str | None = None
    with orm.Session(engine) as session:
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
                    services_table.c.resource_scope).where(
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
        if engine.dialect.name not in (
                db_utils.SQLAlchemyDialect.SQLITE.value,
                db_utils.SQLAlchemyDialect.POSTGRESQL.value):
            raise ValueError('Unsupported database dialect')

        existing = session.execute(
            sqlalchemy.select(version_specs_table.c.yaml_content).where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.version == version)).fetchone()
        identical_retry = existing is not None and existing[0] == yaml_content
        if existing is not None and existing[
                0] is not None and not identical_retry:
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

        uses_logical_replicas = (getattr(spec, 'uses_logical_replicas', False)
                                 is True)
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
        requested_lb_ha = bool(getattr(spec, 'lb_high_availability', False))
        if (not identical_retry and semantics_row is not None and
                bool(semantics_row[1]) != requested_lb_ha):
            # Enabling and disabling HA move Kubernetes traffic authority and
            # therefore require the dedicated selector saga. A normal Serve
            # version commit cannot safely perform that cross-store mutation.
            session.rollback()
            return VersionCommitResult.LB_HA_CONFLICT
        if existing is None:
            session.execute(version_specs_table.insert().values(
                service_name=service_name,
                version=version,
                spec=pickle.dumps(spec),
                yaml_content=yaml_content,
                submitted_yaml_content=submitted_yaml_content,
                created_at=time.time()))
        elif existing[0] is None:
            # `add_version` reserves a NULL-YAML placeholder. The service-row
            # lock above serializes the one transition that fills it.
            session.execute(
                sqlalchemy.update(version_specs_table).where(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version,
                    version_specs_table.c.yaml_content.is_(None)).values(
                        spec=pickle.dumps(spec),
                        yaml_content=yaml_content,
                        submitted_yaml_content=submitted_yaml_content,
                        created_at=time.time()))
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
        # An identical committed YAML is an idempotent retry. Keep both the
        # original YAML and pickled spec bytes untouched.
        if resource_scope is not None and storage_generation is not None:
            session.query(ephemeral_storage_cleanup_intents_table).filter(
                ephemeral_storage_cleanup_intents_table.c.service_name ==
                service_name,
                ephemeral_storage_cleanup_intents_table.c.resource_scope ==
                resource_scope,
                ephemeral_storage_cleanup_intents_table.c.storage_generation ==
                storage_generation,
                *([
                    ephemeral_storage_cleanup_intents_table.c.lifecycle_epoch
                    == expected_lifecycle_epoch
                ] if expected_lifecycle_epoch is not None else []),
            ).update({ephemeral_storage_cleanup_intents_table.c.provisional: 0})
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
    """Gets yaml contents for all of a service's versions in one query.

    Versions whose yaml content is missing (NULL) are omitted. Keys are
    returned in ascending version order.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(version_specs_table.c.version,
                              version_specs_table.c.yaml_content).
            where(version_specs_table.c.service_name == service_name).order_by(
                version_specs_table.c.version)).fetchall()
    return {row[0]: row[1] for row in rows if row[1] is not None}


def get_version_records(service_name: str) -> list[dict[str, Any]]:
    """Gets committed version contents and provenance in one query."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.spec,
                version_specs_table.c.yaml_content,
                version_specs_table.c.submitted_yaml_content,
                version_specs_table.c.created_at,
                version_specs_table.c.created_by,
            ).where(
                version_specs_table.c.service_name == service_name,
                version_specs_table.c.yaml_content.isnot(None),
            ).order_by(version_specs_table.c.version)).fetchall()
    return [{
        'version': row.version,
        'spec': pickle.loads(row.spec) if row.spec is not None else None,
        'yaml_content': row.yaml_content,
        'submitted_yaml_content': row.submitted_yaml_content,
        'created_at': row.created_at,
        'created_by': row.created_by,
    } for row in rows]


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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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

    `add_version` inserts a placeholder row (spec=pickle.dumps(None),
    yaml_content=NULL) and only later does `add_or_update_version` fill in the
    real spec/yaml. A restart in that window can leave such a placeholder as
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
                key: getattr(insert_stmt.excluded, key)
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
    return None if row is None else dict(row._mapping)  # pylint: disable=protected-access


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
                key: getattr(insert_stmt.excluded, key)
                for key in values
                if key != 'context'
            })
        session.execute(insert_stmt)
        session.commit()


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


def publish_reserved_fill_round(pool_key: str, *, round_id: int,
                                snapshot_time: float, epoch: int, grants: str,
                                feeds: str, raw_grants: str, feed_state: str,
                                sum_holdings: int,
                                last_observed_free: int | None,
                                last_observed_free_ts: float | None,
                                phantom_streak: int,
                                shrink_baseline: int | None, lease_token: int,
                                lease_expires_at: float) -> bool:
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

    Returns True if the round was published.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
            'grants': grants,
            'feeds': feeds,
            'raw_grants': raw_grants,
            'feed_state': feed_state,
            'sum_holdings': sum_holdings,
            'last_observed_free': last_observed_free,
            'last_observed_free_ts': last_observed_free_ts,
            'phantom_streak': phantom_streak,
            'shrink_baseline': shrink_baseline,
            'fence_pending': 0,
        }
        insert_stmt = _upsert_insert_func(engine)(
            reserved_fill_rounds_table).values(**values)
        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['pool_key'],
            set_={
                key: getattr(insert_stmt.excluded, key)
                for key in values
                if key != 'pool_key'
            })
        session.execute(insert_stmt)
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


def add_replica_if_round_epoch(
    service_name: str,
    replica_id: int,
    replica_info: 'replica_managers.ReplicaInfo',
    *,
    pool_key: str,
    expected_epoch: int,
    expected_service_hash: str | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None
) -> bool:
    """Persists a fill replica row iff the launch's allocation is current.

    Three predicates, all evaluated atomically with the row upsert:

    - Round epoch: the launch path's cheap epoch pre-check is TOCTOU (a
      broker round can publish a new epoch between that check and the row
      persist, making a stale fill launch durable against capacity
      already re-fed to a peer), so the recheck is atomic with the
      persist. A missing round row fails open (persists), mirroring the
      pre-check: no broker ever ran, there is no newer allocation to
      defer to.
    - fence_pending fails CLOSED: the marker means every grant issued
      before a lease-dead gap is suspect, and only an epoch-bumping
      publish may clear it. Without this predicate a pool whose marker
      can never be cleared (its claims are gone, so no round is ever
      published) would let a stalled controller's pre-gap decision pass
      the epoch check indefinitely.
    - Live same-pool claim: the launching service must still hold a claim
      on this pool. A disabled/pruned/moved claimant's queued fill launch
      would otherwise start against a slot the broker no longer
      attributes to it (its rows only count as unclaimed occupancy in the
      round debit -- see _occupying_debit).

    The atomicity needs a dialect split:

    - PostgreSQL: the round and claim rows are read FOR SHARE in the
      upsert's transaction, blocking concurrent round/claim UPDATEs until
      commit, so neither predicate can move between the read and the
      upsert.
    - sqlite: FOR SHARE is a no-op AND the legacy sqlite3 transaction mode
      does not even open a transaction for the SELECT, so the two-statement
      shape keeps the exact read/publish/upsert interleaving it was meant
      to close (or, under WAL snapshot upgrades, aborts with a BUSY error
      instead of fencing). Chosen shape: ONE conditional statement --
      INSERT ... SELECT literals WHERE NOT EXISTS(round with a DIFFERENT
      epoch or a pending fence) AND EXISTS(live same-pool claim) --
      because a single DML statement is atomic under sqlite's writer lock
      by construction: the predicates are evaluated inside the very
      statement that writes the row, leaving no window at all and no
      BEGIN IMMEDIATE/busy-handshake choreography to maintain. rowcount 0
      means a fence held (nothing written). SQLITE_BUSY-family errors
      (another writer holding the lock past the busy timeout) are retried
      a few times and then degrade into a fence-skip: the launch is simply
      re-emitted on a later tick, exactly like a fenced pre-check.

    Callers must additionally serialize this persist against broker
    rounds via the cross-process broker lock (see
    reserved_capacity_broker.persist_fill_replica): the epoch predicate
    alone cannot see a round that has finished its debit scan but not yet
    published.

    Returns whether the row was persisted; False = a predicate failed (or
    sqlite stayed busy), nothing was written, the caller must skip the
    launch exactly like a fenced pre-check.
    """
    engine = _db_manager.get_engine()
    row_values = _replica_row_values(service_name, replica_id, replica_info)
    if engine.dialect.name != db_utils.SQLAlchemyDialect.SQLITE.value:
        with orm.Session(engine) as session:
            if expected_service_hash is not None:
                owner = session.execute(
                    sqlalchemy.select(services_table.c.hash,
                                      services_table.c.controller_pid,
                                      services_table.c.controller_ip).where(
                                          services_table.c.name == service_name
                                      ).with_for_update(read=True)).fetchone()
                if (owner is None or owner[0] != expected_service_hash or
                    (expected_controller_owner is not None and
                     (owner[1], owner[2]) != expected_controller_owner)):
                    session.rollback()
                    return False
            row = session.execute(
                sqlalchemy.select(
                    reserved_fill_rounds_table.c.epoch,
                    reserved_fill_rounds_table.c.fence_pending).where(
                        reserved_fill_rounds_table.c.pool_key ==
                        pool_key).with_for_update(read=True)).fetchone()
            if row is not None and (int(row[0]) != expected_epoch or
                                    bool(row[1])):
                session.rollback()
                return False
            claim = session.execute(
                sqlalchemy.select(reserved_fill_claims_table.c.service_name).
                where(reserved_fill_claims_table.c.service_name == service_name,
                      reserved_fill_claims_table.c.pool_key ==
                      pool_key).with_for_update(read=True)).fetchone()
            if claim is None:
                session.rollback()
                return False
            insert_stmt = _upsert_insert_func(engine)(replicas_table).values(
                **row_values)
            insert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['service_name', 'replica_id'],
                set_={
                    column.name: getattr(insert_stmt.excluded, column.name)
                    for column in replicas_table.c
                    if column.name not in ('service_name', 'replica_id')
                })
            session.execute(insert_stmt)
            session.commit()
        return True
    # sqlite: every fence predicate is the WHERE clause of the insert
    # itself.
    stale_round = sqlalchemy.select(
        reserved_fill_rounds_table.c.pool_key).where(
            reserved_fill_rounds_table.c.pool_key == pool_key,
            sqlalchemy.or_(reserved_fill_rounds_table.c.epoch != expected_epoch,
                           reserved_fill_rounds_table.c.fence_pending
                           != 0)).exists()
    live_claim = sqlalchemy.select(
        reserved_fill_claims_table.c.service_name).where(
            reserved_fill_claims_table.c.service_name == service_name,
            reserved_fill_claims_table.c.pool_key == pool_key).exists()
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
    ]).where(sqlalchemy.not_(stale_round), live_claim, current_incarnation)
    insert_stmt = sqlite.insert(replicas_table).from_select(
        [column.name for column in columns], select_stmt)
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['service_name', 'replica_id'],
        set_={
            column.name: getattr(insert_stmt.excluded, column.name)
            for column in replicas_table.c
            if column.name not in ('service_name', 'replica_id')
        })
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
