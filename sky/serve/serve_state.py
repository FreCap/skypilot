"""The database for services information."""
import collections
import enum
import json
import pickle
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
# every poll interval (the heartbeat). holdings are split fill/demand because
# only fill holdings are broker property -- demand-placed zero-cost replicas
# are demand-protected and exempt from the grant ceiling.
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
    sqlalchemy.Column('holdings_demand', sqlalchemy.Integer),
    # Cap on the weighted share ABOVE the floor
    # (max_replicas - demand_target - floor); NULL = unbounded. Reported by
    # the claimant so the water-fill can redistribute share it can never use.
    sqlalchemy.Column('headroom', sqlalchemy.Integer, server_default=None),
    # Real capacity cap the claimant can materialize right now
    # (max(0, max_replicas - demand_target)); NULL = unbounded. The broker
    # clamps both the effective floor and the feed need by it, so an
    # unattainable floor cannot permanently absorb entitlement and feed the
    # service never launches (its excess joins the burst remainder).
    sqlalchemy.Column('effective_cap', sqlalchemy.Integer, server_default=None),
    # Whether the claimant can launch on the pool right now (its zero-cost
    # tier is not benched): feeds to un-launchable claimants are wasted for a
    # whole round, so the feed split redistributes them.
    sqlalchemy.Column('launchable', sqlalchemy.Integer, server_default='1'),
    sqlalchemy.Column('heartbeat_ts', sqlalchemy.Float),
    # The broker epoch the claimant last observed; diagnostic only in v1.
    sqlalchemy.Column('owner_epoch', sqlalchemy.Integer, server_default=None),
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
    # Sum of fill holdings at round time: a shrink here means pods are
    # physically gone, making grant down-moves immediate (no damping).
    sqlalchemy.Column('sum_holdings', sqlalchemy.Integer),
    # Last SUCCESSFULLY measured free level + its timestamp: a failed query
    # decays from this instead of reading raw 0 (a measurement blackout must
    # not trigger releases).
    sqlalchemy.Column('last_observed_free', sqlalchemy.Integer),
    sqlalchemy.Column('last_observed_free_ts', sqlalchemy.Float),
)

# Singleton lease row (id=1). The epoch only moves forward; its sole role
# is the publish CAS in publish_reserved_fill_round (fencing for actuation
# is the per-pool round epoch above).
reserved_fill_lease_table = sqlalchemy.Table(
    'reserved_fill_lease',
    Base.metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('owner_service', sqlalchemy.Text),
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
                controller_ip: Optional[str] = None) -> bool:
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
                    hash=str(uuid.uuid4()),
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


def update_service_controller_pid(service_name: str,
                                  controller_pid: int) -> None:
    """Updates the controller pid of a service.

    This is used to update the controller pid of a service on ha recovery.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update(
                {services_table.c.controller_pid: controller_pid})
        session.commit()


def update_service_controller_pid_ip_and_port(service_name: str,
                                              controller_pid: int,
                                              controller_ip: Optional[str],
                                              controller_port: int) -> None:
    """Atomically updates controller pid + IP + port for a service.

    Used during HA recovery: the controller subprocess on the new pod must be
    listening on the chosen port before we flip DB to point requests at it.
    By updating all three fields in one statement, clients never see a
    half-flipped row (e.g. new pid + old ip, or new ip + stale port that
    points at a different service's listener on the new pod).

    Recovery picks the port locally (find_free_port on the recovery pod) —
    it must NOT reuse the previous pod's port — so the port change has to
    propagate to DB together with the pid/ip flip.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update({
                services_table.c.controller_pid: controller_pid,
                services_table.c.controller_ip: controller_ip,
                services_table.c.controller_port: controller_port,
            })
        session.commit()


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


def remove_service_completely(service_name: str) -> None:
    """Atomically remove the service-level DB state for a service.

    Deletes from `services`, `version_specs`, and
    `serve_ha_recovery_script` in a single transaction. These were the
    three tables whose sequential teardown left orphan rows when a
    subprocess died mid-cleanup.

    Replicas are intentionally NOT touched here. Both callers
    (`_cleanup` success path in `_start`, and `_terminate_failed_services`
    on the `--purge` path) iterate replicas one-by-one before this call
    so they can run per-replica logic (cluster-existence probes for
    leak reporting, terminate-thread join, failure marking).
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.delete(services_table).where(
                services_table.c.name == service_name))
        session.execute(
            sqlalchemy.delete(version_specs_table).where(
                version_specs_table.c.service_name == service_name))
        session.execute(
            sqlalchemy.delete(serve_ha_recovery_script_table).where(
                serve_ha_recovery_script_table.c.service_name == service_name))
        session.commit()


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


def set_service_controller_port(service_name: str,
                                controller_port: int) -> None:
    """Sets the controller port of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(services_table).filter(
            services_table.c.name == service_name).update(
                {services_table.c.controller_port: controller_port})
        session.commit()


def set_service_controller_port_if_owner(service_name: str, controller_pid: int,
                                         controller_port: int) -> bool:
    """Sets the controller port only if `controller_pid` still owns the row.

    Compare-and-swap for the in-place controller respawn: a parent whose
    ownership has been taken over by HA recovery on another pod (which
    atomically flipped pid/ip/port) must not clobber the new owner's port,
    which would recreate the half-flipped row (new pid/ip + stale port) that
    `update_service_controller_pid_ip_and_port` exists to prevent.

    Returns:
        True if the row was updated (the pid still owns the service), False
        if ownership was lost or the row no longer exists.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.controller_pid == controller_pid).update(
                {services_table.c.controller_port: controller_port})
        session.commit()
    return count > 0


def set_service_load_balancer_port_if_owner(service_name: str,
                                            controller_pid: int,
                                            load_balancer_port: int) -> bool:
    """Sets the load balancer port only if `controller_pid` owns the row.

    Compare-and-swap for recovery's external-LB port republish: the plain
    setter below is a name-only write, so a stale recovery process racing a
    purge + same-name re-up could write to the successor's row and
    prematurely unblock its registration (`wait_service_registration` returns
    on any non-null port). Filtering on controller_pid makes the
    ownership check and the write one atomic UPDATE.

    Returns:
        True if the row was updated (the pid owns the service), False if
        ownership was lost or the row no longer exists.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.controller_pid == controller_pid).update(
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


def get_service_hash(service_name: str) -> Optional[str]:
    """Get the hash of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(services_table.c.hash).where(
                services_table.c.name == service_name)).fetchone()
    return result[0] if result else None


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


def get_service_controller_port(service_name: str) -> Optional[int]:
    """Gets the controller port of a service (None if not yet assigned)."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(services_table.c.controller_port).where(
                services_table.c.name == service_name)).fetchone()
        if result is None:
            raise ValueError(f'Service {service_name} does not exist.')
        return result[0]


def get_service_load_balancer_port(service_name: str) -> int:
    """Gets the load balancer port of a service."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(services_table.c.load_balancer_port).where(
                services_table.c.name == service_name)).fetchone()
        if result is None:
            raise ValueError(f'Service {service_name} does not exist.')
        return result[0]


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
                               holdings_demand: int, headroom: Optional[int],
                               effective_cap: Optional[int], launchable: bool,
                               heartbeat_ts: float,
                               owner_epoch: Optional[int]) -> None:
    """Upserts a service's reserved-fill claim (the per-poll heartbeat)."""
    engine = _db_manager.get_engine()
    values = {
        'service_name': service_name,
        'pool_key': pool_key,
        'weight': weight,
        'floor_replicas': floor_replicas,
        'gpus_per_replica': gpus_per_replica,
        'holdings_fill': holdings_fill,
        'holdings_demand': holdings_demand,
        'headroom': headroom,
        'effective_cap': effective_cap,
        'launchable': int(launchable),
        'heartbeat_ts': heartbeat_ts,
        'owner_epoch': owner_epoch,
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

    Returns the pruned service names (for loud logging by the broker).
    Delete-then-report runs in one transaction so a concurrent heartbeat
    upsert either survives (it re-creates the row) or is reported.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(reserved_fill_claims_table.c.service_name).where(
                reserved_fill_claims_table.c.heartbeat_ts < expired_before)
        ).fetchall()
        names = [row[0] for row in rows]
        if names:
            session.execute(
                sqlalchemy.delete(reserved_fill_claims_table).where(
                    reserved_fill_claims_table.c.service_name.in_(names)))
        session.commit()
    return names


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


def get_reserved_fill_lease() -> Optional[Dict[str, Any]]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(reserved_fill_lease_table).where(
                reserved_fill_lease_table.c.id == 1)).fetchone()
    return None if row is None else dict(row._mapping)  # pylint: disable=protected-access


def publish_reserved_fill_round(pool_key: str, *, round_id: int,
                                snapshot_time: float, epoch: int, grants: str,
                                feeds: str, raw_grants: str, feed_state: str,
                                sum_holdings: int,
                                last_observed_free: Optional[int],
                                last_observed_free_ts: Optional[float],
                                owner_service: str, prev_epoch: Optional[int],
                                lease_epoch: int,
                                lease_expires_at: float) -> bool:
    """Atomically CAS-advances the lease and publishes a round.

    `epoch` is the POOL's fencing epoch, stored on the round row (per-pool:
    the launch fence compares against it, and one pool's grant churn must
    not fence another pool's launches). The lease carries a separate global
    epoch stream (prev_epoch -> lease_epoch) whose only role is the CAS
    below.

    The lease update is a filtered UPDATE on the previous epoch (the
    *_if_owner CAS pattern): a racing writer that already advanced the epoch
    makes rowcount 0 and the whole round (lease + round row) rolls back --
    grants can never be published under an epoch that is not current. The
    broker holds the cross-process round lock, so a CAS failure indicates a
    lock-bypass bug or manual DB surgery; failing closed is the only safe
    reaction.

    Returns True if the round was published.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if prev_epoch is None:
            # First lease ever: INSERT; a PK collision means we lost a race
            # that the round lock should have prevented -- fail closed.
            try:
                session.execute(
                    sqlalchemy.insert(reserved_fill_lease_table).values(
                        id=1,
                        owner_service=owner_service,
                        epoch=lease_epoch,
                        expires_at=lease_expires_at))
            except sqlalchemy_exc.IntegrityError:
                session.rollback()
                return False
        else:
            count = session.query(reserved_fill_lease_table).filter(
                reserved_fill_lease_table.c.id == 1,
                reserved_fill_lease_table.c.epoch == prev_epoch).update({
                    reserved_fill_lease_table.c.owner_service: owner_service,
                    reserved_fill_lease_table.c.epoch: lease_epoch,
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
