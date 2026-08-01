"""The database for services information."""
import collections
import enum
import json
import pickle
import time
import typing
from typing import Any, Optional
import uuid

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.serve import constants
from sky.serve import lb_cutover_state
from sky.serve import lb_ha
from sky.serve import paid_capacity
from sky.serve import serve_state_schema
from sky.serve.serve_statuses import ReplicaStatus
from sky.serve.serve_statuses import ServiceStatus
from sky.utils import common_utils
from sky.utils import yaml_utils
from sky.utils.db import db_utils

if typing.TYPE_CHECKING:
    from sqlalchemy.engine import row

    from sky.serve import replica_managers
    from sky.serve import service_spec

replica_managers = adaptors_common.LazyImport('sky.serve.replica_managers')
logger = sky_logging.init_logger(__name__)

_TERMINAL_IDENTITY_QUERY_BATCH_SIZE = 250

Base = serve_state_schema.Base
services_table = serve_state_schema.services_table
replicas_table = serve_state_schema.replicas_table
version_specs_table = serve_state_schema.version_specs_table
ephemeral_storage_cleanup_intents_table = (
    serve_state_schema.ephemeral_storage_cleanup_intents_table)
serve_ha_recovery_script_table = (
    serve_state_schema.serve_ha_recovery_script_table)
service_lifecycle_fences_table = (
    serve_state_schema.service_lifecycle_fences_table)
reserved_fill_claims_table = serve_state_schema.reserved_fill_claims_table
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
                submitted_yaml_content: str | None = None,
                placement_catalog: dict[str, Any] | None = None) -> bool:
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
                placement_catalog=placement_catalog,
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
                            version_insert_stmt.excluded.submitted_yaml_content,
                        'placement_catalog':
                            version_insert_stmt.excluded.placement_catalog,
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


def get_num_services(pool: bool | None = None) -> int:
    """Get the number of raw service rows, optionally filtered by mode."""
    engine = _db_manager.get_engine()
    query = sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(services_table)
    if pool is not None:
        query = query.where(services_table.c.pool == int(pool))
    with orm.Session(engine) as session:
        return session.execute(query).fetchone()[0]


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
    placeholder rows without a per-service joined read.
    """
    latest_version = sqlalchemy.select(
        version_specs_table.c.service_name,
        sqlalchemy.func.max(version_specs_table.c.version).label('max_version'),
    ).group_by(version_specs_table.c.service_name).alias('v')
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
                version_specs_table.c.yaml_content,
            ).select_from(
                services_table.join(
                    latest_version, services_table.c.name ==
                    latest_version.c.service_name).join(
                        version_specs_table,
                        sqlalchemy.and_(
                            version_specs_table.c.service_name ==
                            services_table.c.name,
                            version_specs_table.c.version ==
                            latest_version.c.max_version,
                        ))).where(services_table.c.pool == int(pool)).order_by(
                            services_table.c.name)).fetchall()
    return [{
        'name': row.name,
        'status': ServiceStatus[row.status],
        'controller_job_id': row.controller_job_id,
        'controller_pid': row.controller_pid,
        'controller_ip': row.controller_ip,
        'hash': row.hash,
        'resource_scope': row.resource_scope,
        'yaml_content': row.yaml_content,
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


def get_service_version_terminal_states(
    identities: list[tuple[str, int,
                           str]],) -> dict[tuple[str, int, str], bool]:
    """Returns authoritative terminal state for bounded service versions.

    Missing entries are intentionally unknown. A service version is live while
    it is current, routed, or owns any replica row. It becomes terminal only
    after Serve's own rollout/drain state has moved past it, or after its exact
    service incarnation is gone.
    """
    if not identities:
        return {}
    if len(identities) > 1000:
        raise ValueError('Service-version terminal-state batch is too large.')
    names = sorted({identity[0] for identity in identities})
    version_identities = sorted({
        (identity[0], identity[1]) for identity in identities
    })
    engine = _db_manager.get_engine()
    service_rows = []
    version_probes = []
    with orm.Session(engine) as session:
        for start in range(0, len(names), _TERMINAL_IDENTITY_QUERY_BATCH_SIZE):
            name_batch = names[start:start +
                               _TERMINAL_IDENTITY_QUERY_BATCH_SIZE]
            service_rows.extend(
                session.execute(
                    sqlalchemy.select(
                        services_table.c.name,
                        services_table.c.hash,
                        services_table.c.status,
                        services_table.c.current_version,
                        services_table.c.active_versions,
                    ).where(services_table.c.name.in_(
                        name_batch))).mappings().all())
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
            replica_exists = sqlalchemy.exists(
                sqlalchemy.select(sqlalchemy.literal(1)).where(
                    replicas_table.c.service_name == wanted.c.service_name,
                    replicas_table.c.version == wanted.c.version))
            version_probes.extend(
                session.execute(
                    sqlalchemy.select(
                        wanted.c.service_name,
                        wanted.c.version,
                        version_exists.label('version_exists'),
                        replica_exists.label('replica_exists'),
                    ).select_from(wanted)).mappings().all())
    services = {str(row['name']): row for row in service_rows}
    versions = {(str(row['service_name']), int(row['version']))
                for row in version_probes
                if row['version_exists']}
    replicas = {(str(row['service_name']), int(row['version']))
                for row in version_probes
                if row['replica_exists']}
    result: dict[tuple[str, int, str], bool] = {}
    for identity in identities:
        name, version, service_hash = identity
        row = services.get(name)
        if row is None or row['hash'] != service_hash:
            result[identity] = True
            continue
        if ServiceStatus[str(
                row['status'])] in ServiceStatus.terminal_statuses():
            result[identity] = True
            continue
        if (name, version) not in versions:
            # A matching live incarnation without the claimed immutable version
            # is inconsistent, not proof that the owner is terminal.
            continue
        current_version = row['current_version']
        active_versions = (json.loads(row['active_versions'])
                           if row['active_versions'] else [])
        if (current_version == version or version in active_versions or
            (name, version) in replicas):
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


_require_postgresql_lb_cutover = getattr(lb_cutover_state,
                                         '_require_postgresql_lb_cutover')
get_lb_cutover_state = lb_cutover_state.get_lb_cutover_state
_lb_cutover_owner_predicates = getattr(lb_cutover_state,
                                       '_lb_cutover_owner_predicates')
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
lb_cutover_kubernetes_guard: typing.Callable[
    ..., typing.ContextManager[bool]] = typing.cast(
        typing.Callable[..., typing.ContextManager[bool]],
        getattr(lb_cutover_state, 'lb_cutover_kubernetes_guard'))
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
_PAID_CAPACITY_UNRESOLVED_STATUSES = (
    ReplicaStatus.PENDING.value,
    ReplicaStatus.PROVISIONING.value,
)


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
        'paid_capacity_pool_key': getattr(replica_info,
                                          'paid_capacity_pool_key', None),
        'replica_state': replica_state,
    }


def _upsert_replica_rows_in_session(
    session: orm.Session,
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    replica_infos: list[tuple[int, 'replica_managers.ReplicaInfo']],
) -> None:
    """Upsert replica rows in dialect-safe bounded batches."""
    chunk_size = (max(1, _SQLITE_MAX_BIND_PARAMS // len(replicas_table.c)) if
                  engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value
                  else _POSTGRESQL_REPLICA_UPSERT_CHUNK_SIZE)
    insert_func = _upsert_insert_func(engine)
    for start in range(0, len(replica_infos), chunk_size):
        chunk = replica_infos[start:start + chunk_size]
        insert_stmt = insert_func(replicas_table).values([
            _replica_row_values(service_name, replica_id, replica_info)
            for replica_id, replica_info in chunk
        ])
        session.execute(
            insert_stmt.on_conflict_do_update(
                index_elements=['service_name', 'replica_id'],
                set_={
                    column.name: getattr(insert_stmt.excluded, column.name)
                    for column in replicas_table.c
                    if column.name not in ('service_name', 'replica_id')
                }))


def _replica_from_state(
        replica_state_version: int,
        replica_state: dict[str, Any]) -> 'replica_managers.ReplicaInfo':
    if replica_state_version != _REPLICA_STATE_VERSION:
        raise RuntimeError('Unsupported replica state version: '
                           f'{replica_state_version!r}')
    return replica_managers.ReplicaInfo.from_storage_dict(replica_state)


def _lock_service_owner_in_session(
    session: orm.Session,
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None] | None,
    *,
    require_launch_allowed: bool,
) -> bool:
    """Lock and validate one service controller owner."""
    owner = session.execute(
        sqlalchemy.select(services_table.c.hash,
                          services_table.c.controller_pid,
                          services_table.c.controller_ip,
                          services_table.c.status).where(
                              services_table.c.name ==
                              service_name).with_for_update()).fetchone()
    if (owner is None or owner[0] != expected_service_hash or
        (expected_controller_owner is not None and
         (owner[1], owner[2]) != expected_controller_owner)):
        return False
    return (not require_launch_allowed or
            owner[3] not in ServiceStatus.replica_launch_blocking_statuses())


def get_service_placement_policy_states(
        service_name: str) -> dict[str, dict[str, Any] | None] | None:
    """Read restart-safe placer and economic-stabilization state."""
    engine = _db_manager.get_engine()
    try:
        with orm.Session(engine) as session:
            row = session.execute(
                sqlalchemy.select(
                    services_table.c.spot_placement_state,
                    services_table.c.cost_rebalance_state,
                ).where(services_table.c.name == service_name)).fetchone()
    except sqlalchemy.exc.SQLAlchemyError as e:
        if _placement_policy_columns_missing(e):
            return None
        raise
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
    try:
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
                    services_table.c.name == service_name).values(
                        {column: state}))
            session.commit()
    except sqlalchemy.exc.SQLAlchemyError as e:
        if _placement_policy_columns_missing(e):
            # Mixed rollout compatibility. Migration 029 is ordered before
            # controller deployment; until it lands, retain process-local
            # behavior instead of blocking every placer-backed launch.
            return True
        raise
    return True


def _placement_policy_columns_missing(
        error: sqlalchemy.exc.SQLAlchemyError) -> bool:
    """Whether an old schema lacks migration-029 policy columns."""
    original = getattr(error, 'orig', None)
    sqlstate = (getattr(original, 'sqlstate', None) or
                getattr(original, 'pgcode', None))
    message = str(error).casefold()
    mentions_column = ('spot_placement_state' in message or
                       'cost_rebalance_state' in message)
    return mentions_column and (sqlstate == '42703' or 'no such column'
                                in message or 'undefined column' in message)


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
    for service_name, service_hash, replica_id, current_hash, status, row_pool in rows:
        identity = (service_name, service_hash, replica_id)
        if (current_hash == service_hash and
                status in _PAID_CAPACITY_UNRESOLVED_STATUSES and
                row_pool == pool_key):
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
    for replica_id, pool_key, status, row_pool in rows:
        if (status in _PAID_CAPACITY_UNRESOLVED_STATUSES and
                row_pool == pool_key):
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
        if (pool_key not in owned_pool_keys and
                len(owned_pool_keys) >= frontier_limit):
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
                frontier_limit)
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
        for pool_key, claim_hash, current_hash, status, row_pool in rows:
            if (claim_hash == current_hash and
                    status in _PAID_CAPACITY_UNRESOLVED_STATUSES and
                    row_pool == pool_key):
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
) -> str:
    """Atomically persist one replica and its global paid-capacity claim."""
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
                    frontier_limit)
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

        replica_info.paid_capacity_pool_key = pool_key
        replica_insert = _upsert_insert_func(engine)(replicas_table).values(
            **_replica_row_values(service_name, replica_id, replica_info))
        session.execute(
            replica_insert.on_conflict_do_update(
                index_elements=['service_name', 'replica_id'],
                set_={
                    column.name: getattr(replica_insert.excluded, column.name)
                    for column in replicas_table.c
                    if column.name not in ('service_name', 'replica_id')
                }))
        claim_insert = _upsert_insert_func(engine)(
            paid_capacity_claims_table).values(service_name=service_name,
                                               service_hash=service_hash,
                                               replica_id=replica_id,
                                               pool_key=pool_key,
                                               priority=priority,
                                               claimed_at=now)
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
                frontier_limit=frontier_limit,
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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
        for replica_id, pool_key, priority, replica_info in claims:
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
                        replica_info=row_values['replica_info'],
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
) -> bool:
    """Persist a completed launch wave and release claims atomically."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_service_owner_in_session(session,
                                              service_name,
                                              service_hash,
                                              expected_controller_owner,
                                              require_launch_allowed=False):
            session.rollback()
            return False
        _upsert_replica_rows_in_session(session, engine, service_name,
                                        replica_infos)
        if not outcomes:
            session.commit()
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
    return True


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
        # Older SQLite builds cap SQLITE_MAX_VARIABLE_NUMBER at 999, while
        # PostgreSQL can preserve the prior 300-row batches. The helper derives
        # SQLite's safe chunk from the live table width.
        _upsert_replica_rows_in_session(session, engine, service_name,
                                        replica_infos)
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
        session.execute(
            sqlalchemy.delete(paid_capacity_claims_table).where(
                paid_capacity_claims_table.c.service_name == service_name,
                paid_capacity_claims_table.c.replica_id == replica_id))
        result = session.execute(
            sqlalchemy.delete(replicas_table).where(*predicates))
        session.commit()
    # Once exact ownership is proven, an already-absent child is the desired
    # idempotent cleanup state, not evidence of ownership loss.
    return expected_service_hash is not None or result.rowcount > 0


def remove_replicas(
    service_name: str,
    replica_ids: list[int],
    expected_service_hash: str,
    expected_lifecycle_epoch: int | None = None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
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
    replica_ids = list(dict.fromkeys(replica_ids))
    if not replica_ids:
        return True
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
        for start in range(0, len(replica_ids), _REPLICA_DELETE_CHUNK_SIZE):
            chunk = replica_ids[start:start + _REPLICA_DELETE_CHUNK_SIZE]
            session.execute(
                sqlalchemy.delete(paid_capacity_claims_table).where(
                    paid_capacity_claims_table.c.service_name == service_name,
                    paid_capacity_claims_table.c.replica_id.in_(chunk)))
            session.execute(
                sqlalchemy.delete(replicas_table).where(
                    replicas_table.c.service_name == service_name,
                    replicas_table.c.replica_id.in_(chunk)))
        session.commit()
    return True


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
    placement_catalog: dict[str, Any] | None = None,
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
            sqlalchemy.select(
                version_specs_table.c.yaml_content,
                version_specs_table.c.placement_catalog).where(
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
                placement_catalog=placement_catalog,
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
                        placement_catalog=placement_catalog,
                        created_at=time.time()))
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
                version_specs_table.c.quarantined_at,
                version_specs_table.c.quarantine_reason,
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
        'quarantined_at': row.quarantined_at,
        'quarantine_reason': row.quarantine_reason,
    } for row in rows]


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
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
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
    commit may sit between it and the version still published to the load
    balancer. Recovery must prefer the newest active, non-quarantined version
    in that case. A commit newer than the quarantine still supersedes it.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        applicable = session.execute(
            sqlalchemy.select(
                version_specs_table.c.version,
                version_specs_table.c.spec).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.yaml_content.isnot(None),
                        version_specs_table.c.quarantined_at.is_(None))).
            order_by(version_specs_table.c.version.desc()).limit(1)).fetchone()
        quarantined_version = session.execute(
            sqlalchemy.select(sqlalchemy.func.max(
                version_specs_table.c.version)).where(
                    sqlalchemy.and_(
                        version_specs_table.c.service_name == service_name,
                        version_specs_table.c.quarantined_at.isnot(
                            None)))).scalar_one_or_none()
        if (quarantined_version is not None and
            (applicable is None or applicable.version < quarantined_version)):
            active_versions_json = session.execute(
                sqlalchemy.select(services_table.c.active_versions).where(
                    services_table.c.name ==
                    service_name)).scalar_one_or_none()
            active_versions = (json.loads(active_versions_json)
                               if active_versions_json else [])
            if active_versions:
                active = session.execute(
                    sqlalchemy.select(version_specs_table.c.version,
                                      version_specs_table.c.spec).
                    where(
                        sqlalchemy.and_(
                            version_specs_table.c.service_name == service_name,
                            version_specs_table.c.version.in_(active_versions),
                            version_specs_table.c.yaml_content.isnot(None),
                            version_specs_table.c.quarantined_at.is_(
                                None))).order_by(
                                    version_specs_table.c.version.desc()).limit(
                                        1)).fetchone()
                if active is not None:
                    applicable = active
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
        # Written unconditionally, including as an explicit NULL triple when
        # the caller has no sample. A claimant that goes blind must CLEAR its
        # previous signal in the same statement that advances heartbeat_ts;
        # leaving a stale need behind would let the broker keep gating on a
        # measurement nothing is refreshing.
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


def publish_reserved_fill_round(pool_key: str,
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
                                utilization_state: str | None = None) -> bool:
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
