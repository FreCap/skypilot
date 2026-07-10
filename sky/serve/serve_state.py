"""The database for services information."""
import collections
import enum
import json
import pickle
import time
import typing
from typing import Any, Dict, List, Optional, Tuple
import uuid

import colorama
import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import declarative

from sky.serve import constants
from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

if typing.TYPE_CHECKING:
    from sqlalchemy.engine import row

    from sky.serve import replica_managers
    from sky.serve import service_spec

Base = declarative.declarative_base()

# === Database schema ===
services_table = sqlalchemy.Table(
    'services',
    Base.metadata,
    sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
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
    sqlalchemy.Column('entrypoint', sqlalchemy.Text, server_default=None),
    # Pod IP where the controller process is running.
    # Written by the sky.serve.service process at startup.
    sqlalchemy.Column('controller_ip', sqlalchemy.Text, server_default=None),
)

replicas_table = sqlalchemy.Table(
    'replicas',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('replica_info', sqlalchemy.LargeBinary),
)

version_specs_table = sqlalchemy.Table(
    'version_specs',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
    sqlalchemy.Column('yaml_content', sqlalchemy.Text, server_default=None),
)

serve_ha_recovery_script_table = sqlalchemy.Table(
    'serve_ha_recovery_script',
    Base.metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('script', sqlalchemy.Text),
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


_db_manager = db_utils.DatabaseManager('serve/services', create_table)

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

    # The replica is a spot VM and it is preempted by the cloud provider.
    PREEMPTED = 'PREEMPTED'

    # Unknown. This should never happen (used only for unexpected errors).
    UNKNOWN = 'UNKNOWN'

    @classmethod
    def failed_statuses(cls) -> List['ReplicaStatus']:
        return [
            cls.FAILED, cls.FAILED_CLEANUP, cls.FAILED_INITIAL_DELAY,
            cls.FAILED_PROBING, cls.FAILED_PROVISION, cls.UNKNOWN
        ]

    @classmethod
    def terminal_statuses(cls) -> List['ReplicaStatus']:
        return [cls.SHUTTING_DOWN, cls.PREEMPTED, cls.UNKNOWN
               ] + cls.failed_statuses()

    @classmethod
    def scale_down_decision_order(cls) -> List['ReplicaStatus']:
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
    def failed_statuses(cls) -> List['ServiceStatus']:
        return [cls.CONTROLLER_FAILED, cls.FAILED_CLEANUP]

    @classmethod
    def terminal_statuses(cls) -> List['ServiceStatus']:
        """States in which the service is either dying or already broken
        and cannot accept new operations like update/apply. SHUTTING_DOWN
        is included because it's a transient state that the service may
        never leave on its own (the previous cleanup may have died
        mid-flight, leaving a zombie row — see _cleanup)."""
        return [cls.CONTROLLER_FAILED, cls.FAILED_CLEANUP, cls.SHUTTING_DOWN]

    def colored_str(self) -> str:
        color = _SERVICE_STATUS_TO_COLOR[self]
        return f'{color}{self.value}{colorama.Style.RESET_ALL}'

    @classmethod
    def from_replica_statuses(
            cls, replica_statuses: List[ReplicaStatus]) -> 'ServiceStatus':
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
                controller_ip: Optional[str] = None,
                service_hash: Optional[str] = None) -> bool:
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
    try:
        with orm.Session(engine) as session:
            if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
                insert_func = sqlite.insert
            elif (engine.dialect.name ==
                  db_utils.SQLAlchemyDialect.POSTGRESQL.value):
                insert_func = postgresql.insert
            else:
                raise ValueError('Unsupported database dialect')

            session.execute(
                insert_func(services_table).values(
                    name=name,
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
                    entrypoint=entrypoint))
            version_insert_stmt = insert_func(version_specs_table).values(
                service_name=name,
                version=constants.INITIAL_VERSION,
                spec=pickle.dumps(spec),
                yaml_content=yaml_content)
            # Upsert (like `add_or_update_version`): a stale version row with
            # no `services` row, left behind by an interrupted teardown on an
            # older controller, must not block re-registration of the name.
            session.execute(
                version_insert_stmt.on_conflict_do_update(
                    index_elements=['service_name', 'version'],
                    set_={
                        'spec': version_insert_stmt.excluded.spec,
                        'yaml_content':
                            version_insert_stmt.excluded.yaml_content
                    }))
            session.commit()

    except sqlalchemy_exc.IntegrityError as e:
        for msg in _UNIQUE_CONSTRAINT_FAILED_ERROR_MSGS:
            if msg in str(e):
                return False
        raise RuntimeError('Unexpected database error') from e
    return True


def update_service_controller_pid_if_owner(
        service_name: str, expected_service_hash: Optional[str],
        expected_controller_pid: Optional[int],
        expected_controller_ip: Optional[str], controller_pid: int,
        controller_ip: Optional[str]) -> bool:
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
        service_name: str, controller_pid: int, controller_ip: Optional[str],
        controller_port: int, expected_service_hash: Optional[str],
        expected_controller_pid: Optional[int],
        expected_controller_ip: Optional[str]) -> bool:
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
                              controller_ip: Optional[str]) -> None:
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
    expected_controller_owner: Optional[Tuple[Optional[int],
                                              Optional[str]]] = None
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


def remove_service_completely(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: Optional[Tuple[Optional[int],
                                              Optional[str]]] = None
) -> bool:
    """Atomically remove one exact service incarnation and all child rows.

    Deletes from `services`, `replicas`, `version_specs`,
    `serve_ha_recovery_script`, and `reserved_fill_claims` in a single
    transaction. These were the tables whose sequential
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
        result = session.execute(
            sqlalchemy.delete(services_table).where(*predicates))
        if result.rowcount == 0:
            session.rollback()
            return False
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
        session.commit()
    return True


def set_service_uptime(service_name: str, uptime: int) -> None:
    """Sets the uptime of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update(
                {services_table.c.uptime: uptime})
        session.commit()


def set_service_status_and_active_versions(
        service_name: str,
        status: ServiceStatus,
        active_versions: Optional[List[int]] = None) -> None:
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
        expected_controller_pid: Optional[int],
        expected_controller_ip: Optional[str],
        status: ServiceStatus,
        active_versions: Optional[List[int]] = None,
        expected_status: Optional[ServiceStatus] = None) -> bool:
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            *predicates).update(update_dict)
        session.commit()
    return count > 0


def set_service_status_and_active_versions_if_hash(
        service_name: str,
        expected_service_hash: str,
        status: ServiceStatus,
        active_versions: Optional[List[int]] = None,
        expected_status: Optional[ServiceStatus] = None) -> bool:
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
                                         expected_service_hash: Optional[str],
                                         controller_pid: int,
                                         controller_ip: Optional[str],
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
        controller_ip: Optional[str]) -> bool:
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


def claim_orphaned_service_teardown(service_name: str,
                                    expected_service_hash: str,
                                    expected_controller_pid: Optional[int],
                                    expected_controller_ip: Optional[str],
                                    controller_pid: int,
                                    controller_ip: Optional[str]) -> bool:
    """Claim a terminal row that has no recovery script or live child.

    Callers must establish the absence of a recovery script while holding the
    per-service lifecycle lock. The status predicate prevents claiming a
    healthy incarnation.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == expected_controller_pid,
            services_table.c.controller_ip == expected_controller_ip,
            services_table.c.status == ServiceStatus.SHUTTING_DOWN.value,
        ).update({
            services_table.c.controller_pid: controller_pid,
            services_table.c.controller_ip: controller_ip,
            services_table.c.controller_port:
                constants.CONTROLLER_TEARDOWN_ACK_PORT,
        })
        session.commit()
    return count > 0


def set_service_load_balancer_port_if_owner(
        service_name: str, expected_service_hash: Optional[str],
        controller_pid: int, controller_ip: Optional[str],
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


def _get_service_from_row(r: 'row.RowMapping') -> Dict[str, Any]:
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
        'hash': r['hash'],
        'entrypoint': r['entrypoint'],
        'yaml_content': r.get('yaml_content'),
    }
    latest_spec = get_spec(r['name'], current_version)
    if latest_spec is not None:
        record['policy'] = latest_spec.autoscaling_policy_str()
        record['load_balancing_policy'] = latest_spec.load_balancing_policy
    return record


def _build_services_with_latest_version_query(
        service_name: Optional[str] = None) -> sqlalchemy.sql.Select:
    """Builds a query joining services with their latest version and yaml.

    Args:
        service_name: If provided, filter to this service only.

    Returns:
        A SQLAlchemy selectable for fetching rows, including columns:
        - max_version (latest version per service)
        - services_table.*
        - yaml_content (from version_specs_table for latest version)
    """
    subquery = sqlalchemy.select(
        version_specs_table.c.service_name,
        sqlalchemy.func.max(version_specs_table.c.version).label('max_version'),
    ).group_by(version_specs_table.c.service_name).alias('v')

    query = sqlalchemy.select(
        subquery.c.max_version,
        services_table,
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


def get_services() -> List[Dict[str, Any]]:
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


def get_service_from_name(service_name: str) -> Optional[Dict[str, Any]]:
    """Get all existing service records."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = _build_services_with_latest_version_query(service_name)
        rows = session.execute(query).fetchall()
    for row in rows:
        return _get_service_from_row(row._mapping)  # pylint: disable=protected-access
    return None


def get_service_controller_owner(service_name: str) -> Optional[Dict[str, Any]]:
    """Get only the fields needed to route to a service controller.

    Unlike :func:`get_service_from_name`, this hot-path lookup does not join
    ``version_specs``, deserialize the latest spec, or issue a second query.
    The service hash distinguishes a same-name successor from the row read
    before a proxied request; status lets the proxy reject terminal rows.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.hash,
                services_table.c.status,
                services_table.c.controller_pid,
                services_table.c.controller_ip,
                services_table.c.controller_port,
            ).where(services_table.c.name == service_name)).fetchone()
    if row is None:
        return None
    mapping = row._mapping  # pylint: disable=protected-access
    return {
        'hash': mapping['hash'],
        'status': ServiceStatus[mapping['status']],
        'controller_pid': mapping['controller_pid'],
        'controller_ip': mapping['controller_ip'],
        'controller_port': mapping['controller_port'],
    }


def get_service_hash(service_name: str) -> Optional[str]:
    """Get the hash of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(services_table.c.hash).where(
                services_table.c.name == service_name)).fetchone()
    return result[0] if result else None


def get_service_mode_and_hash(
        service_name: str) -> Optional[Tuple[bool, Optional[str]]]:
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


def get_service_versions(service_name: str) -> List[int]:
    """Gets all versions of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(version_specs_table.c.version.distinct()).where(
                version_specs_table.c.service_name == service_name)).fetchall()
    return [row[0] for row in rows]


def get_glob_service_names(
        service_names: Optional[List[str]] = None) -> List[str]:
    """Get service names matching the glob patterns.

    Args:
        service_names: A list of glob patterns. If None, return all service
            names.

    Returns:
        A list of non-duplicated service names.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if service_names is None:
            rows = session.execute(sqlalchemy.select(
                services_table.c.name)).fetchall()
        else:
            rows = []
            for service_name in service_names:
                pattern_rows = session.execute(
                    sqlalchemy.select(services_table.c.name).where(
                        services_table.c.name.like(
                            service_name.replace('*', '%')))).fetchall()
                rows.extend(pattern_rows)
    return list({row[0] for row in rows})


def get_service_pool_from_db(service_name: str) -> Optional[bool]:
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

# 999 (the oldest SQLITE_MAX_VARIABLE_NUMBER default) // 3 params per row,
# rounded down for headroom.
_REPLICA_UPSERT_CHUNK_SIZE = 300


def add_or_update_replica(service_name: str, replica_id: int,
                          replica_info: 'replica_managers.ReplicaInfo') -> None:
    """Adds a replica to the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmt = insert_func(replicas_table).values(
            service_name=service_name,
            replica_id=replica_id,
            replica_info=pickle.dumps(replica_info))

        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['service_name', 'replica_id'],
            set_={'replica_info': insert_stmt.excluded.replica_info})

        session.execute(insert_stmt)
        session.commit()


def add_or_update_replicas(
        service_name: str,
        replica_infos: List[Tuple[int,
                                  'replica_managers.ReplicaInfo']]) -> None:
    """Upserts a batch of replicas in one statement/transaction.

    The probe round persists per-replica bookkeeping for every probed
    replica; issuing those as individual upserts serializes one DB
    round-trip per replica under the replica-manager lock (at ~1k replicas
    on Postgres that alone exceeds the probe period). Multi-row
    ON CONFLICT upsert keeps the round O(1) in round-trips.
    """
    if not replica_infos:
        return
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        # Chunked: 3 bind params per row, and older SQLite builds cap
        # SQLITE_MAX_VARIABLE_NUMBER at 999 — an unchunked 1k-replica round
        # would fail exactly on the deployments this batching targets.
        # 300 rows/chunk keeps a 1k-replica round at ~4 round-trips.
        for start in range(0, len(replica_infos), _REPLICA_UPSERT_CHUNK_SIZE):
            chunk = replica_infos[start:start + _REPLICA_UPSERT_CHUNK_SIZE]
            insert_stmt = insert_func(replicas_table).values([{
                'service_name': service_name,
                'replica_id': replica_id,
                'replica_info': pickle.dumps(replica_info),
            } for replica_id, replica_info in chunk])

            insert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['service_name', 'replica_id'],
                set_={'replica_info': insert_stmt.excluded.replica_info})

            session.execute(insert_stmt)
        session.commit()


def remove_replica(service_name: str, replica_id: int) -> None:
    """Removes a replica from the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(replicas_table).where(
                sqlalchemy.and_(replicas_table.c.service_name == service_name,
                                replicas_table.c.replica_id == replica_id)))
        session.commit()


def get_replica_info_from_id(
        service_name: str,
        replica_id: int) -> Optional['replica_managers.ReplicaInfo']:
    """Gets a replica info from the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(replicas_table.c.replica_info).where(
                sqlalchemy.and_(
                    replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id == replica_id))).fetchone()
    return pickle.loads(result[0]) if result else None


def get_replica_infos(
        service_name: str) -> List['replica_managers.ReplicaInfo']:
    """Gets all replica infos of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(replicas_table.c.replica_info).where(
                replicas_table.c.service_name == service_name)).fetchall()
    return [pickle.loads(row[0]) for row in rows]


def get_replica_service_names() -> List[str]:
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


def total_number_provisioning_replicas() -> int:
    """Returns the total number of provisioning replicas."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(sqlalchemy.select(
            replicas_table.c.replica_info)).fetchall()
    provisioning_count = 0
    for row in rows:
        replica_info: 'replica_managers.ReplicaInfo' = pickle.loads(row[0])
        if replica_info.status == ReplicaStatus.PROVISIONING:
            provisioning_count += 1
    return provisioning_count


def total_number_terminating_replicas() -> int:
    """Returns the total number of terminating replicas."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(sqlalchemy.select(
            replicas_table.c.replica_info)).fetchall()
    terminating_count = 0
    for row in rows:
        replica_info: 'replica_managers.ReplicaInfo' = pickle.loads(row[0])
        if (replica_info.status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING):
            terminating_count += 1
    return terminating_count


def get_replicas_at_status(
    service_name: str,
    status: ReplicaStatus,
) -> List['replica_managers.ReplicaInfo']:
    replicas = get_replica_infos(service_name)
    return [replica for replica in replicas if replica.status == status]


# === Version functions ===
def add_version(service_name: str) -> int:
    """Adds a version to the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
            spec=pickle.dumps(None)).returning(version_specs_table.c.version)

        result = session.execute(insert_stmt)
        new_version = result.scalar()
        session.commit()
    return new_version


def add_or_update_version(service_name: str, version: int,
                          spec: 'service_spec.SkyServiceSpec',
                          yaml_content: str) -> None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmt = insert_func(version_specs_table).values(
            service_name=service_name,
            version=version,
            spec=pickle.dumps(spec),
            yaml_content=yaml_content)

        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['service_name', 'version'],
            set_={
                'spec': insert_stmt.excluded.spec,
                'yaml_content': insert_stmt.excluded.yaml_content
            })

        session.execute(insert_stmt)
        session.commit()


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


def get_yaml_content(service_name: str, version: int) -> Optional[str]:
    """Gets the yaml content of a version."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(version_specs_table.c.yaml_content).where(
                sqlalchemy.and_(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version))).fetchone()
    return result[0] if result else None


def delete_version(service_name: str, version: int) -> None:
    """Deletes a version from the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(version_specs_table).where(
                sqlalchemy.and_(
                    version_specs_table.c.service_name == service_name,
                    version_specs_table.c.version == version)))
        session.commit()


def delete_all_versions(service_name: str) -> None:
    """Deletes all versions from the database."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(version_specs_table).where(
                version_specs_table.c.service_name == service_name))
        session.commit()


def get_latest_version(service_name: str) -> Optional[int]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(sqlalchemy.func.max(
                version_specs_table.c.version)).where(
                    version_specs_table.c.service_name ==
                    service_name)).fetchone()
    return result[0] if result else None


def get_latest_committed_version(service_name: str) -> Optional[int]:
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


def get_ha_recovery_script(service_name: str) -> Optional[str]:
    """Gets the HA recovery script for a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(serve_ha_recovery_script_table.c.script).where(
                serve_ha_recovery_script_table.c.service_name ==
                service_name)).fetchone()
    return result[0] if result else None


def set_ha_recovery_script(service_name: str, script: str) -> None:
    """Sets the HA recovery script for a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
        expected_controller_pid: Optional[int],
        expected_controller_ip: Optional[str]) -> bool:
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


def upsert_reserved_fill_claim(service_name: str, *, pool_key: str,
                               weight: float, floor_replicas: int,
                               gpus_per_replica: int, holdings_fill: int,
                               effective_cap: Optional[int], launchable: bool,
                               heartbeat_ts: float) -> None:
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


def remove_reserved_fill_claim(service_name: str) -> None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.service_name == service_name))
        session.commit()


def remove_reserved_fill_claims_for_pool(pool_key: str) -> None:
    """Drops every claim on a pool (phantom-pool rejection)."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(reserved_fill_claims_table).where(
                reserved_fill_claims_table.c.pool_key == pool_key))
        session.commit()


def prune_reserved_fill_claims(expired_before: float) -> List[str]:
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
                where(reserved_fill_claims_table.c.heartbeat_ts < expired_before
                     )).fetchall()
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
        pool_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """All claim rows (optionally restricted to one pool), as dicts."""
    engine = _db_manager.get_engine()
    query = sqlalchemy.select(reserved_fill_claims_table)
    if pool_key is not None:
        query = query.where(reserved_fill_claims_table.c.pool_key == pool_key)
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    return [dict(row._mapping) for row in rows]  # pylint: disable=protected-access


def get_reserved_fill_round(pool_key: str) -> Optional[Dict[str, Any]]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(reserved_fill_rounds_table).where(
                reserved_fill_rounds_table.c.pool_key == pool_key)).fetchone()
    return None if row is None else dict(row._mapping)  # pylint: disable=protected-access


def acquire_reserved_fill_lease_token(
        *, now: float, expires_at: float) -> Optional[Tuple[int, bool]]:
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
                                last_observed_free: Optional[int],
                                last_observed_free_ts: Optional[float],
                                phantom_streak: int,
                                shrink_baseline: Optional[int],
                                lease_token: int,
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


def add_replica_if_round_epoch(service_name: str, replica_id: int,
                               replica_info: 'replica_managers.ReplicaInfo', *,
                               pool_key: str, expected_epoch: int) -> bool:
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
    pickled_info = pickle.dumps(replica_info)
    if engine.dialect.name != db_utils.SQLAlchemyDialect.SQLITE.value:
        with orm.Session(engine) as session:
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
                service_name=service_name,
                replica_id=replica_id,
                replica_info=pickled_info)
            insert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['service_name', 'replica_id'],
                set_={'replica_info': insert_stmt.excluded.replica_info})
            session.execute(insert_stmt)
            session.commit()
        return True
    # sqlite: every fence predicate is the WHERE clause of the insert
    # itself.
    stale_round = sqlalchemy.select(
        reserved_fill_rounds_table.c.pool_key).where(
            reserved_fill_rounds_table.c.pool_key == pool_key,
            sqlalchemy.or_(
                reserved_fill_rounds_table.c.epoch != expected_epoch,
                reserved_fill_rounds_table.c.fence_pending != 0)).exists()
    live_claim = sqlalchemy.select(
        reserved_fill_claims_table.c.service_name).where(
            reserved_fill_claims_table.c.service_name == service_name,
            reserved_fill_claims_table.c.pool_key == pool_key).exists()
    select_stmt = sqlalchemy.select(
        sqlalchemy.literal(service_name),
        sqlalchemy.literal(replica_id),
        sqlalchemy.literal(pickled_info, sqlalchemy.LargeBinary()),
    ).where(sqlalchemy.not_(stale_round), live_claim)
    insert_stmt = sqlite.insert(replicas_table).from_select(
        ['service_name', 'replica_id', 'replica_info'], select_stmt)
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['service_name', 'replica_id'],
        set_={'replica_info': insert_stmt.excluded.replica_info})
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
