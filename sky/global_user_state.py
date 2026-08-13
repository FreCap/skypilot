"""Global user state, backed by a sqlite database.

Concepts:
- Cluster name: a user-supplied or auto-generated unique name to identify a
  cluster.
- Cluster handle: (non-user facing) an opaque backend handle for us to
  interact with a cluster.
"""
import asyncio
import contextlib
import enum
import json
import os
import pickle
import re
import time
import typing
from typing import Any, Literal, Optional
import uuid

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import asyncio as sql_async

from sky import global_user_state_cloud_checks
from sky import global_user_state_cluster_control_plane_reads
from sky import global_user_state_cluster_events
from sky import global_user_state_cluster_history
from sky import global_user_state_cluster_listing
from sky import global_user_state_cluster_raw_snapshots
from sky import global_user_state_cluster_record_identity
from sky import global_user_state_cluster_yaml
from sky import global_user_state_notifications
from sky import global_user_state_schema
from sky import global_user_state_service_account_tokens
from sky import global_user_state_skylet_tunnels
from sky import global_user_state_storage
from sky import global_user_state_system_config
from sky import global_user_state_users
from sky import global_user_state_volumes
from sky import models
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.metrics import utils as metrics_lib
from sky.skylet import constants
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import context_utils
from sky.utils import status_lib
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils
from sky.utils.db import retries as db_retries

if typing.TYPE_CHECKING:
    from sky import backends
    from sky import clouds
    from sky.backends import skylet_transport
    from sky.clouds import cloud
    from sky.data import Storage
else:
    skylet_transport = adaptors_common.LazyImport(
        'sky.backends.skylet_transport')

logger = sky_logging.init_logger(__name__)

DEFAULT_CLUSTER_EVENT_RETENTION_HOURS = 30 * 24.0
DEBUG_CLUSTER_EVENT_RETENTION_HOURS = 30 * 24.0
TERMINAL_CLUSTER_EVENT_RETENTION_HOURS = 30 * 24.0
MIN_CLUSTER_EVENT_DAEMON_INTERVAL_SECONDS = 3600
_UNIQUE_CONSTRAINT_FAILED_ERROR_MSGS = [
    # sqlite
    'UNIQUE constraint failed',
    # postgres
    'duplicate key value violates unique constraint',
]


class ManagedClusterStatusFields(typing.NamedTuple):
    """Plain reconciliation snapshot bound to one cluster generation."""

    status: str | None
    status_updated_at: int | None
    cluster_hash: str | None


class ClusterRefreshFields(typing.NamedTuple):
    """Plain snapshot fencing status refresh under the cluster lock."""

    status: str | None
    status_updated_at: int | None
    autostop: int
    to_down: bool
    cluster_hash: str | None
    is_managed: bool
    workload_type: str | None


Base = global_user_state_schema.Base
auth_session_table = global_user_state_schema.auth_session_table
cluster_event_table = global_user_state_schema.cluster_event_table
cluster_history_table = global_user_state_schema.cluster_history_table
cluster_table = global_user_state_schema.cluster_table
cluster_yaml_table = global_user_state_schema.cluster_yaml_table
config_table = global_user_state_schema.config_table
estimated_spend_daily_table = global_user_state_schema.estimated_spend_daily_table
estimated_spend_state_table = global_user_state_schema.estimated_spend_state_table
operator_notification_cursor_table = global_user_state_schema.operator_notification_cursor_table
operator_notification_sequence_table = global_user_state_schema.operator_notification_sequence_table
operator_notification_table = global_user_state_schema.operator_notification_table
service_account_token_table = global_user_state_schema.service_account_token_table
ssh_key_table = global_user_state_schema.ssh_key_table
storage_table = global_user_state_schema.storage_table
system_config_table = global_user_state_schema.system_config_table
user_table = global_user_state_schema.user_table
volume_table = global_user_state_schema.volume_table

# These historical helpers remain available through this facade.
# pylint: disable=protected-access
_operator_notification_insert_func = (
    global_user_state_notifications._operator_notification_insert_func)
_next_operator_notification_sequence = (
    global_user_state_notifications._next_operator_notification_sequence)
# Cloud-check key helpers are stateless direct aliases.  Retain their
# historical facade identity for protected import and inspection compatibility.
_get_enabled_clouds_key = (
    global_user_state_cloud_checks._get_enabled_clouds_key)
_get_enabled_clouds_key.__module__ = __name__
_get_check_results_key = global_user_state_cloud_checks._get_check_results_key
_get_check_results_key.__module__ = __name__
_get_allowed_clouds_key = (
    global_user_state_cloud_checks._get_allowed_clouds_key)
_get_allowed_clouds_key.__module__ = __name__
# pylint: enable=protected-access


def lock_container_image_cluster_lifecycle_in_session(
        session: orm.Session, cluster_name: str) -> None:
    """Serializes cluster-row presence with image-owner reconciliation."""
    bind = session.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    lock_key = json.dumps(('container_image_cluster_lifecycle', cluster_name),
                          separators=(',', ':'))
    session.execute(
        sqlalchemy.text('SELECT pg_advisory_xact_lock('
                        'hashtextextended(CAST(:lock_key AS text), 0))'),
        {'lock_key': lock_key})


# Container image catalog tables live exclusively in
# sky.container_images.schema. Cluster state stores only a validated resolved
# plan and its demand ID; it does not mirror image-plane tables.
def _container_image_execution_state(resources: Any) -> tuple[Any, ...]:
    """Returns every image field that may affect one runtime pull."""
    if resources is None:
        return (None, None, None, None, None, None)
    cloud = getattr(resources, 'cloud', None)
    return (
        getattr(resources, 'container_image', None),
        getattr(resources, 'resolved_container_image', None),
        str(cloud).lower() if cloud is not None else None,
        getattr(resources, 'region', None),
        getattr(resources, 'instance_type', None),
        getattr(resources, 'docker_login_config', None),
    )


def _validate_container_image_resolution(
    session: orm.Session,
    launched_resources: Any,
    resolved_image: Any,
    workspace: str,
) -> tuple[str, str] | None:
    """Rejects forged or stale managed pull plans before persistence."""
    # pylint: disable=import-outside-toplevel
    from sky.container_images import config as container_image_config
    from sky.container_images import models as container_image_models
    from sky.container_images import schema as container_image_schema

    image_spec = launched_resources.container_image
    if image_spec is None:
        return None
    if resolved_image is not None:
        if (resolved_image.location_id is None or
                resolved_image.demand_id is None or
                resolved_image.demand_generation is None or
                resolved_image.controller_epoch is None or
                resolved_image.owner_epoch is None or
                resolved_image.profile_revision_id is None or
                resolved_image.target_fingerprint is None):
            raise ValueError('Managed container image pull plan is incomplete.')
        demand = container_image_schema.demands
        location = container_image_schema.locations
        profile = container_image_schema.profile_revisions
        artifact = container_image_schema.images
        watermark = container_image_schema.consumer_watermarks
        row = session.execute(
            sqlalchemy.select(
                demand.c.id,
                demand.c.consumer_kind,
                demand.c.consumer_owner,
            ).join(location, location.c.id == demand.c.location_id).join(
                profile, profile.c.id == demand.c.profile_revision_id).
            join(artifact, artifact.c.id == demand.c.image_id).where(
                demand.c.id == resolved_image.demand_id,
                demand.c.workspace == workspace, demand.c.consumer_generation
                == resolved_image.demand_generation, demand.c.state ==
                container_image_models.ImageDemandState.READY.value,
                demand.c.image_id == resolved_image.image_id,
                demand.c.runtime_digest == resolved_image.digest,
                demand.c.profile_revision_id
                == resolved_image.profile_revision_id,
                demand.c.target_fingerprint
                == resolved_image.target_fingerprint,
                demand.c.owner_epoch == resolved_image.owner_epoch,
                sqlalchemy.exists().where(
                    watermark.c.workspace == demand.c.workspace,
                    watermark.c.consumer_kind == demand.c.consumer_kind,
                    watermark.c.consumer_owner == demand.c.consumer_owner,
                    watermark.c.controller_epoch ==
                    resolved_image.controller_epoch,
                    watermark.c.owner_epoch == resolved_image.owner_epoch),
                location.c.id == resolved_image.location_id,
                location.c.target_ref == resolved_image.reference,
                location.c.state ==
                container_image_models.ImageLocationState.READY.value,
                location.c.lease_token.is_(None),
                profile.c.profile == resolved_image.distribution,
                artifact.c.runtime_digest == resolved_image.digest)).first()
        if row is None:
            raise ValueError('Managed container image pull plan is stale.')
        return str(row.consumer_kind), str(row.consumer_owner)
    if launched_resources.container_image_from_legacy_image_id:
        return None
    profile, _ = container_image_config.resolve_profile(image_spec.distribution,
                                                        workspace)
    if profile is not None:
        raise ValueError('A managed container selector requires a resolved '
                         'runtime pull plan before cluster persistence.')
    if image_spec.ref is None:
        raise ValueError('A release or artifact container selector requires a '
                         'managed resolved runtime pull plan.')
    return None


def _validate_container_image_inline_credentials(resources: Any) -> None:
    """Fences cluster-handle persistence against restored unsafe resources."""
    resource_validator = getattr(
        resources, '_validate_container_image_docker_credentials', None)
    if resource_validator is not None:
        resource_validator()
    image_spec = getattr(resources, 'container_image', None)
    from_legacy = getattr(resources, 'container_image_from_legacy_image_id',
                          False)
    login_config = getattr(resources, 'docker_login_config', None)
    if image_spec is None or from_legacy or login_config is None:
        return
    if isinstance(login_config, dict):
        username = login_config.get('username')
        password = login_config.get('password')
    else:
        username = getattr(login_config, 'username', None)
        password = getattr(login_config, 'password', None)
    if username or password:
        raise ValueError(
            'container_image does not support inline Docker username or '
            'password credentials. Use a public source or a server-side '
            'workload identity.')


class ClusterEventType(enum.Enum):
    """Type of cluster event."""
    DEBUG = 'DEBUG'
    """Detailed debugging information from the cloud"""

    STATUS_CHANGE = 'STATUS_CHANGE'
    """Used to denote events that modify cluster status."""

    TERMINAL = 'TERMINAL'
    """Used to denote events that are directly related to
    a cluster's termination."""

    # Progress milestones emitted during a cluster launch
    # (e.g. 'Launching (Kubernetes cluster is autoscaling)',
    # 'Launching (1 pod(s) pending due to Pulling)'). Read for the
    # LAUNCHING-state badge tooltip on the dashboard.
    LAUNCH_PROGRESS = 'LAUNCH_PROGRESS'


def _glob_to_similar(glob_pattern):
    """Converts a glob pattern to a PostgreSQL LIKE pattern."""

    # Escape special LIKE characters that are not special in glob
    glob_pattern = glob_pattern.replace('%', '\\%').replace('_', '\\_')

    # Convert glob wildcards to LIKE wildcards
    like_pattern = glob_pattern.replace('*', '%').replace('?', '_')

    # Handle character classes, including negation
    def replace_char_class(match):
        group = match.group(0)
        if group.startswith('[!'):
            return '[^' + group[2:-1] + ']'
        return group

    like_pattern = re.sub(r'\[(!)?.*?\]', replace_char_class, like_pattern)
    return like_pattern


def create_table(engine: sqlalchemy.engine.Engine):
    # Enable WAL mode to avoid locking issues.
    # See: issue #1441 and PR #1509
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

    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.GLOBAL_USER_STATE_DB_NAME,
        migration_utils.GLOBAL_USER_STATE_VERSION,
        mode=migration_utils.configured_migration_mode())


@annotations.lru_cache(scope='global', maxsize=1)
def _sqlite_supports_returning() -> bool:
    """Check if SQLite (3.35.0+) and SQLAlchemy (2.0+) support RETURNING.

    See https://sqlite.org/lang_returning.html and
    https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#insert-update-delete-returning  # pylint: disable=line-too-long
    """
    sqlalchemy_version_parts = sqlalchemy.__version__.split('.')
    assert len(sqlalchemy_version_parts) >= 1, \
        f'Invalid SQLAlchemy version: {sqlalchemy.__version__}'
    sqlalchemy_major = int(sqlalchemy_version_parts[0])
    if sqlalchemy_major < 2:
        return False

    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.SQLITE.value:
        return False
    with orm.Session(engine) as session:
        result = session.execute(sqlalchemy.text('SELECT sqlite_version()'))
        version_str = result.scalar()
        version_parts = version_str.split('.')
        assert len(version_parts) >= 2, \
            f'Invalid version string: {version_str}'
        major, minor = int(version_parts[0]), int(version_parts[1])
        return (major > 3) or (major == 3 and minor >= 35)


_db_manager = db_utils.DatabaseManager(
    'state', create_table, post_init_fn=lambda _: _sqlite_supports_returning())
initialize_and_get_db = _db_manager.get_engine


@contextlib.contextmanager
def _session_scope(session: 'orm.Session | None' = None):
    """Yield the caller's session, or open (and close) a fresh one.

    Reusing an already-open session keeps a single logical operation on a
    single pooled connection. A helper that opens its own session while the
    caller still holds one needs a second concurrent connection; on the
    synchronous PostgreSQL engine every ``state``/``spot``/``serve`` module
    shares one process-local ``QueuePool``, so with ``max_overflow=0`` and a
    small ``pool_size`` (down to 1 for the API server main process) the second
    checkout self-deadlocks until ``pool_timeout`` and raises. Threading the
    session through nested helpers avoids that starvation without relaxing the
    strict per-process connection budget.
    """
    if session is not None:
        yield session
    else:
        with orm.Session(_db_manager.get_engine()) as owned_session:
            yield owned_session


ClusterRecordIdentityWriteOutcome = (
    global_user_state_cluster_record_identity.ClusterRecordIdentityWriteOutcome)
ClusterRecordIdentityConflictError = (global_user_state_cluster_record_identity.
                                      ClusterRecordIdentityConflictError)
ClusterRecordHandleChangedError = (
    global_user_state_cluster_record_identity.ClusterRecordHandleChangedError)
ClusterRecordRemovalOutcome = (
    global_user_state_cluster_record_identity.ClusterRecordRemovalOutcome)

SkyletSSHTunnelMetadata = (
    global_user_state_skylet_tunnels.SkyletSSHTunnelMetadata)
ClusterSkyletSSHTunnelSnapshotV1 = (
    global_user_state_skylet_tunnels.ClusterSkyletSSHTunnelSnapshotV1)

ClusterRecordIdentitySnapshot = (
    global_user_state_cluster_record_identity.ClusterRecordIdentitySnapshot)
_canonical_cluster_record_uuid = (
    global_user_state_cluster_record_identity.canonical_cluster_record_uuid)
_lock_cluster_record_uuid_in_session = (
    global_user_state_cluster_record_identity.
    lock_cluster_record_uuid_in_session)

_CLUSTER_RECORD_HANDLE_UNSET = object()


def _commit_cluster_record_identity_in_session(
    session: orm.Session,
    cluster_name: str,
    cluster_record_uuid: uuid.UUID | str,
    *,
    insert_values: typing.Mapping[str, Any] | None = None,
) -> ClusterRecordIdentityWriteOutcome:
    """Insert or exactly adopt one identity in a caller-owned transaction.

    A caller inserting a missing row must either pass all ordinary insert
    values or finish populating it before committing this same transaction.
    """
    return global_user_state_cluster_record_identity.commit_cluster_record_identity_in_session(
        session,
        cluster_table,
        lock_container_image_cluster_lifecycle_in_session,
        _lock_cluster_record_uuid_in_session,
        cluster_name,
        cluster_record_uuid,
        insert_values=insert_values,
    )


def _read_cluster_record_identity_in_session(
    session: orm.Session,
    cluster_name: str,
    expected_cluster_record_uuid: uuid.UUID | str,
) -> ClusterRecordIdentitySnapshot | None:
    """Read one exact action-aware cluster row in a caller transaction.

    The absence result is authoritative only for the row in this PostgreSQL
    transaction.  A present legacy/null or differently identified same-name
    row is a conflict, never an absence result.
    """
    return global_user_state_cluster_record_identity.read_cluster_record_identity_in_session(
        session,
        cluster_table,
        lock_container_image_cluster_lifecycle_in_session,
        _lock_cluster_record_uuid_in_session,
        cluster_name,
        expected_cluster_record_uuid,
    )


@db_retries.retry
def get_cluster_record_identity_snapshot(
    cluster_name: str,
    expected_cluster_record_uuid: uuid.UUID | str,
) -> ClusterRecordIdentitySnapshot | None:
    """Read one expected-UUID cluster row through the action-aware fence."""
    with orm.Session(_db_manager.get_engine()) as session, session.begin():
        return _read_cluster_record_identity_in_session(
            session, cluster_name, expected_cluster_record_uuid)


@typing.overload
def add_or_update_user(
    user: models.User,
    allow_duplicate_name: bool = True,
    return_user: Literal[False] = False,
) -> bool:
    ...


@typing.overload
def add_or_update_user(
    user: models.User,
    allow_duplicate_name: bool = True,
    *,
    return_user: Literal[True],
) -> tuple[bool, models.User]:
    ...


@typing.overload
def add_or_update_user(
    user: models.User,
    allow_duplicate_name: bool = True,
    return_user: bool = ...,
) -> bool | tuple[bool, models.User]:
    ...


@metrics_lib.time_me
def add_or_update_user(
        user: models.User,
        allow_duplicate_name: bool = True,
        return_user: bool = False) -> bool | tuple[bool, models.User]:
    """Store the mapping from user hash to user name for display purposes.

    Returns:
        If return_user=False: bool (whether the user is newly added)
        If return_user=True: Tuple[bool, models.User]
    """
    return global_user_state_users.add_or_update_user(
        _db_manager.get_engine, orm.Session, sqlite, postgresql, user_table,
        _sqlite_supports_returning, time.time, user, allow_duplicate_name,
        return_user)


@metrics_lib.time_me
def get_user(user_id: str,
             session: 'orm.Session | None' = None) -> models.User | None:
    return global_user_state_users.get_user(_session_scope(session), user_table,
                                            user_id)


def _get_users_in_session(session: 'orm.Session',
                          user_ids: set[str]) -> dict[str, models.User]:
    return global_user_state_users.get_users(_session_scope(session),
                                             user_table, user_ids)


@metrics_lib.time_me
def get_users(user_ids: set[str]) -> dict[str, models.User]:
    return global_user_state_users.get_users(_session_scope(None), user_table,
                                             user_ids)


@metrics_lib.time_me
def get_user_by_name(username: str) -> list[models.User]:
    return global_user_state_users.get_user_by_name(_db_manager.get_engine(),
                                                    orm.Session, user_table,
                                                    username)


@metrics_lib.time_me
def get_user_by_name_match(username_match: str) -> list[models.User]:
    return global_user_state_users.get_user_by_name_match(
        _db_manager.get_engine(), orm.Session, user_table, username_match)


@metrics_lib.time_me
def delete_user(user_id: str) -> None:
    global_user_state_users.delete_user(_db_manager.get_engine(), orm.Session,
                                        user_table, user_id)


@metrics_lib.time_me
def get_all_users() -> list[models.User]:
    return global_user_state_users.get_all_users(_db_manager.get_engine(),
                                                 orm.Session, user_table)


@db_retries.retry
@metrics_lib.time_me
def set_user_preferred_workspace(user_id: str, workspace: str | None) -> bool:
    """Sets (or clears with None) the user's preferred workspace.

    This is the raw DB write; RBAC validation that the user has access to the
    target workspace MUST be done by the caller in sky/workspaces/ before
    invoking this. Returns True if a row was updated, False if the user_id
    does not exist.
    """
    return global_user_state_users.set_user_preferred_workspace(
        _db_manager.get_engine(), orm.Session, user_table, user_id, workspace)


@metrics_lib.time_me
def add_or_update_cluster(
        cluster_name: str,
        cluster_handle: 'backends.ResourceHandle',
        requested_resources: set[Any] | None,
        ready: bool,
        is_launch: bool = True,
        config_hash: str | None = None,
        task_config: dict[str, Any] | None = None,
        is_managed: bool = False,
        provision_log_path: str | None = None,
        existing_cluster_hash: str | None = None,
        workload_type: str | None = None,
        workload_id: str | None = None,
        workload_task_id: int | None = None,
        cluster_record_uuid: uuid.UUID | str | None = None) -> str:
    """Adds or updates cluster_name -> cluster_handle mapping.

    Args:
        cluster_name: Name of the cluster.
        cluster_handle: backends.ResourceHandle of the cluster.
        requested_resources: Resources requested for cluster.
        ready: Whether the cluster is ready to use. If False, the cluster will
            be marked as INIT, otherwise it will be marked as UP.
        is_launch: if the cluster is firstly launched. If True, the launched_at
            and last_use will be updated. Otherwise, use the old value.
        config_hash: Configuration hash for the cluster.
        task_config: The config of the task being launched.
        is_managed: Whether the cluster is launched by the
            controller.
        provision_log_path: Absolute path to provision.log, if available.
        existing_cluster_hash: If specified, the cluster will be updated
            only if the cluster_hash matches. If a cluster does not exist,
            it will not be inserted and an error will be raised.
        workload_type: Best-effort cost attribution type.
        workload_id: Best-effort cost attribution identifier.
        workload_task_id: Managed-job task ID, when available.
        cluster_record_uuid: Internal action-aware write-once cluster identity.
            Ordinary callers must omit this argument.  When present, the
            PostgreSQL identity primitive inserts or exactly adopts it in the
            same transaction as the ordinary cluster row fields.

    Returns:
        The stable hash identifying the inserted or updated cluster generation.
    """
    engine = _db_manager.get_engine()
    parsed_cluster_record_uuid = (
        None if cluster_record_uuid is None else
        _canonical_cluster_record_uuid(cluster_record_uuid))
    if (parsed_cluster_record_uuid is not None and
            engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise RuntimeError(
            'Action-aware cluster identity requires the central PostgreSQL '
            'database.')

    # Restored or internally constructed handles can bypass current Resources
    # construction. Fence them before pickle allocates any durable bytes.
    pre_pickle_resources = getattr(cluster_handle, 'launched_resources', None)
    if pre_pickle_resources is not None:
        _validate_container_image_inline_credentials(pre_pickle_resources)

    # FIXME: launched_at will be changed when `sky launch -c` is called.
    handle = pickle.dumps(cluster_handle)
    cluster_launched_at = int(time.time()) if is_launch else None
    last_use = common_utils.get_current_command() if is_launch else None
    status = status_lib.ClusterStatus.INIT
    if ready:
        status = status_lib.ClusterStatus.UP
    status_updated_at = int(time.time())

    # Extract cloud/region/zone from launched_resources for efficient filtering
    cloud = None
    region = None
    zone = None
    resolved_image = None
    launched_resources = None
    if hasattr(cluster_handle, 'launched_resources'):
        launched_resources = cluster_handle.launched_resources
        if launched_resources is not None:
            cloud = (str(launched_resources.cloud) if getattr(
                launched_resources, 'cloud', None) else None)
            region = (str(launched_resources.region) if getattr(
                launched_resources, 'region', None) else None)
            zone = (str(launched_resources.zone) if getattr(
                launched_resources, 'zone', None) else None)
            resolved_image = getattr(launched_resources,
                                     'resolved_container_image', None)

    if (resolved_image is not None and
            engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise RuntimeError(
            'Managed container image state requires the central PostgreSQL '
            'database.')

    # Extract node_names from cached_cluster_info and merge with lineage.
    # Also opportunistically compute cloud-provider instance console URLs for
    # the dashboard's External Links section (mirrors the managed-job flow in
    # sky/jobs/recovery_strategy.py).
    current_names = None
    instance_links: dict[str, str] | None = None
    if hasattr(cluster_handle, 'cached_cluster_info'):
        ci = cluster_handle.cached_cluster_info
        if ci is not None:
            current_names = ci.get_node_names()
            if ready:
                # Lazy import: sky.utils.instance_links pulls in
                # sky.provision.common which transitively imports
                # sky.global_user_state during cold start, so a top-level
                # import here would deadlock.
                # pylint: disable-next=import-outside-toplevel
                from sky.utils import instance_links as instance_links_utils
                try:
                    generated = instance_links_utils.generate_instance_links(
                        ci, cluster_name)
                    if generated:
                        instance_links = generated
                except Exception as e:  # pylint: disable=broad-except
                    # Never fail a launch because instance-link generation
                    # tripped over a missing field on the cluster info.
                    logger.debug(f'Failed to generate instance links for '
                                 f'cluster {cluster_name}: {e}')

    # TODO (sumanth): Cluster history table will have multiple entries
    # when the cluster failover through multiple regions (one entry per region).
    # It can be more inaccurate for the multi-node cluster
    # as the failover can have the nodes partially UP.
    cluster_hash = (existing_cluster_hash or
                    _get_hash_for_existing_cluster(cluster_name) or
                    str(uuid.uuid4()))
    usage_intervals = _get_cluster_usage_intervals(cluster_hash)
    usage_intervals_changed = False

    # first time a cluster is being launched
    if not usage_intervals:
        usage_intervals = []

    # if this is the cluster init or we are starting after a stop
    if not usage_intervals or usage_intervals[-1][-1] is not None:
        if cluster_launched_at is None:
            # This could happen when the cluster is restarted manually on the
            # cloud console. In this case, we will use the current time as the
            # cluster launched time.
            # TODO(zhwu): We should use the time when the cluster is restarted
            # to be more accurate.
            cluster_launched_at = int(time.time())
        usage_intervals.append((cluster_launched_at, None))
        usage_intervals_changed = True

    user_hash = common_utils.get_current_user().id
    active_workspace = skypilot_config.get_active_workspace()
    history_workspace = active_workspace
    history_hash = user_hash
    container_image_binding_known = True
    container_image_consumer_kind: str | None = None
    container_image_consumer_owner: str | None = None

    conditional_values: dict[str, Any] = {}
    if is_launch:
        conditional_values.update({
            'launched_at': cluster_launched_at,
            'last_use': last_use
        })

    if int(ready) == 1:
        conditional_values.update({
            'cluster_ever_up': 1,
        })

    if config_hash is not None:
        conditional_values.update({
            'config_hash': config_hash,
        })

    with orm.Session(engine) as session:
        lock_container_image_cluster_lifecycle_in_session(session, cluster_name)
        if parsed_cluster_record_uuid is not None:
            # Every action-aware writer acquires name -> UUID -> row.  Taking
            # the UUID before the initial row lock prevents an inverse claimant
            # for another name and the same UUID from deadlocking adoption.
            _lock_cluster_record_uuid_in_session(session,
                                                 parsed_cluster_record_uuid)
        # with_for_update() locks the row until commit() or rollback()
        # is called, or until the code escapes the with block.
        cluster_row = session.query(cluster_table).filter_by(
            name=cluster_name).with_for_update().first()
        if cluster_row is not None and launched_resources is None:
            # A partial handle cannot prove that an existing binding vanished.
            container_image_binding_known = bool(
                cluster_row.container_image_binding_known)
            container_image_consumer_kind = (
                cluster_row.container_image_consumer_kind)
            container_image_consumer_owner = (
                cluster_row.container_image_consumer_owner)
        cluster_workspace = (cluster_row.workspace
                             if cluster_row is not None and
                             cluster_row.workspace else active_workspace or
                             constants.SKYPILOT_DEFAULT_WORKSPACE)
        if (launched_resources is not None and getattr(
                launched_resources, 'container_image', None) is not None):
            container_image_consumer = _validate_container_image_resolution(
                session, launched_resources, resolved_image, cluster_workspace)
            if container_image_consumer is not None:
                (container_image_consumer_kind,
                 container_image_consumer_owner) = container_image_consumer

        # Merge current node names into existing lineage
        existing_node_names = (cluster_row.node_names if cluster_row else None)
        node_names = common_utils.merge_node_names_lineage(
            existing_node_names, current_names)

        if (not cluster_row or
                cluster_row.status == status_lib.ClusterStatus.STOPPED.value):
            conditional_values.update({
                'autostop': -1,
                'to_down': 0,
            })
        if not cluster_row or not cluster_row.user_hash:
            conditional_values.update({
                'user_hash': user_hash,
            })
        if not cluster_row or not cluster_row.workspace:
            conditional_values.update({
                'workspace': active_workspace,
            })
        if is_launch and (cluster_row is None or cluster_row.status
                          != status_lib.ClusterStatus.UP.value):
            conditional_values.update({
                'last_creation_yaml': yaml_utils.dump_yaml_str(task_config)
                                      if task_config else None,
                'last_creation_command': last_use,
            })
        if provision_log_path is not None:
            conditional_values.update({
                'provision_log_path': provision_log_path,
            })
        if workload_type is not None:
            conditional_values['workload_type'] = workload_type
        if workload_id is not None:
            conditional_values['workload_id'] = workload_id
        if workload_task_id is not None:
            conditional_values['workload_task_id'] = workload_task_id

        # Merge newly generated instance links with any existing links so
        # repeated launches (e.g., post-stop start) don't clobber prior entries.
        if instance_links:
            existing_links = (cluster_row.links
                              if cluster_row is not None else None) or {}
            merged_links: dict[str, str] = {}
            if isinstance(existing_links, dict):
                merged_links.update(existing_links)
            merged_links.update(instance_links)
            conditional_values.update({
                'links': merged_links,
            })

        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            session.rollback()
            raise ValueError('Unsupported database dialect')

        row_insert_values = {
            **conditional_values,
            'handle': handle,
            'status': status.value,
            # set metadata to server default ('{}')
            # set owner to server default (null)
            'cluster_hash': cluster_hash,
            # set storage_mounts_metadata to server default (null)
            'status_updated_at': status_updated_at,
            'is_managed': int(is_managed),
            'cloud': cloud,
            'region': region,
            'zone': zone,
            'node_names': node_names,
            'container_image_binding_known': int(container_image_binding_known),
            'container_image_consumer_kind': container_image_consumer_kind,
            'container_image_consumer_owner': container_image_consumer_owner,
        }
        if parsed_cluster_record_uuid is not None:
            # A generation-fenced update must retain its old missing-row
            # rejection.  It may adopt only the row already selected above.
            if existing_cluster_hash is None:
                _commit_cluster_record_identity_in_session(
                    session,
                    cluster_name,
                    parsed_cluster_record_uuid,
                    insert_values=row_insert_values,
                )
            elif cluster_row is not None:
                _commit_cluster_record_identity_in_session(
                    session, cluster_name, parsed_cluster_record_uuid)

        if existing_cluster_hash is not None:
            count = session.query(cluster_table).filter_by(
                name=cluster_name, cluster_hash=existing_cluster_hash
            ).update({
                **conditional_values,
                cluster_table.c.handle: handle,
                cluster_table.c.status: status.value,
                cluster_table.c.status_updated_at: status_updated_at,
                cluster_table.c.cloud: cloud,
                cluster_table.c.region: region,
                cluster_table.c.zone: zone,
                cluster_table.c.node_names: node_names,
                cluster_table.c.container_image_binding_known:
                    int(container_image_binding_known),
                cluster_table.c.container_image_consumer_kind: container_image_consumer_kind,
                cluster_table.c.container_image_consumer_owner: container_image_consumer_owner,
            })
            assert count <= 1
            if count == 0:
                raise ValueError(f'Cluster {cluster_name} with hash '
                                 f'{existing_cluster_hash} not found.')
        else:
            insert_stmnt = insert_func(cluster_table).values(
                name=cluster_name,
                **row_insert_values,
            )
            insert_or_update_stmt = insert_stmnt.on_conflict_do_update(
                index_elements=[cluster_table.c.name],
                set_={
                    **conditional_values,
                    cluster_table.c.handle: handle,
                    cluster_table.c.status: status.value,
                    # do not update metadata value
                    # do not update owner value
                    cluster_table.c.cluster_hash: cluster_hash,
                    # do not update storage_mounts_metadata
                    cluster_table.c.status_updated_at: status_updated_at,
                    # do not update user_hash
                    cluster_table.c.cloud: cloud,
                    cluster_table.c.region: region,
                    cluster_table.c.zone: zone,
                    cluster_table.c.node_names: node_names,
                    cluster_table.c.container_image_binding_known:
                        int(container_image_binding_known),
                    cluster_table.c.container_image_consumer_kind: container_image_consumer_kind,
                    cluster_table.c.container_image_consumer_owner: container_image_consumer_owner,
                })
            session.execute(insert_or_update_stmt)

        # Modify cluster history table
        launched_nodes = getattr(cluster_handle, 'launched_nodes', None)
        launched_resources = getattr(cluster_handle, 'launched_resources', None)
        if cluster_row and cluster_row.workspace:
            history_workspace = cluster_row.workspace
        if cluster_row and cluster_row.user_hash:
            history_hash = cluster_row.user_hash
        creation_info = {}
        if conditional_values.get('last_creation_yaml') is not None:
            creation_info = {
                'last_creation_yaml':
                    conditional_values.get('last_creation_yaml'),
                'last_creation_command':
                    conditional_values.get('last_creation_command'),
            }

        # Calculate last_activity_time and launched_at from usage_intervals
        last_activity_time = _get_cluster_last_activity_time(usage_intervals)
        launched_at = _get_cluster_launch_time(usage_intervals)
        history_update_values = {
            cluster_history_table.c.name: cluster_name,
            cluster_history_table.c.num_nodes: launched_nodes,
            cluster_history_table.c.requested_resources:
                pickle.dumps(requested_resources),
            cluster_history_table.c.launched_resources:
                pickle.dumps(launched_resources),
            cluster_history_table.c.usage_intervals:
                pickle.dumps(usage_intervals),
            cluster_history_table.c.user_hash: history_hash,
            cluster_history_table.c.workspace: history_workspace,
            cluster_history_table.c.provision_log_path: provision_log_path,
            cluster_history_table.c.last_activity_time: last_activity_time,
            cluster_history_table.c.launched_at: launched_at,
            cluster_history_table.c.cloud: cloud,
            cluster_history_table.c.region: region,
            cluster_history_table.c.zone: zone,
            cluster_history_table.c.node_names: node_names,
            **creation_info,
        }
        if workload_type is not None:
            history_update_values[
                cluster_history_table.c.workload_type] = workload_type
        if workload_id is not None:
            history_update_values[
                cluster_history_table.c.workload_id] = workload_id
        if workload_task_id is not None:
            history_update_values[
                cluster_history_table.c.workload_task_id] = workload_task_id
        if usage_intervals_changed:
            history_update_values[
                cluster_history_table.c.usage_updated_at] = status_updated_at

        insert_stmnt = insert_func(cluster_history_table).values(
            cluster_hash=cluster_hash,
            name=cluster_name,
            num_nodes=launched_nodes,
            requested_resources=pickle.dumps(requested_resources),
            launched_resources=pickle.dumps(launched_resources),
            usage_intervals=pickle.dumps(usage_intervals),
            user_hash=user_hash,
            workspace=history_workspace,
            provision_log_path=provision_log_path,
            last_activity_time=last_activity_time,
            launched_at=launched_at,
            cloud=cloud,
            region=region,
            zone=zone,
            node_names=node_names,
            is_managed=int(is_managed),
            workload_type=workload_type,
            workload_id=workload_id,
            workload_task_id=workload_task_id,
            usage_updated_at=status_updated_at,
            **creation_info,
        )
        do_update_stmt = insert_stmnt.on_conflict_do_update(
            index_elements=[cluster_history_table.c.cluster_hash],
            set_={
                # Intentionally do not update is_managed here (mirrors the
                # clusters table above, which only sets it on insert).
                # add_or_update_cluster is called multiple times during a
                # managed-job launch and is_managed defaults to False on
                # subsequent calls; overwriting it would reset the flag to 0
                # and leak managed-job clusters into the history view.
                **history_update_values,
            })
        session.execute(do_update_stmt)

        session.commit()

    if (resolved_image is not None and resolved_image.demand_id is not None):
        # The READY demand already fences eviction before this cluster commit.
        # Attachment is a post-commit lifecycle hint, so a process crash cannot
        # produce a cluster handle without an eviction fence.
        # Import locally because demand_state -> catalog_state imports this
        # module for the central engine during module initialization.
        # pylint: disable=import-outside-toplevel
        from sky.container_images import demand_state as image_demand_state

        # pylint: enable=import-outside-toplevel
        image_demand_state.attach_consumer(resolved_image.demand_id,
                                           cluster_workspace)
    return cluster_hash


@db_retries.retry
@metrics_lib.time_me
def add_cluster_event(cluster_name: str,
                      new_status: status_lib.ClusterStatus | None,
                      reason: str,
                      event_type: ClusterEventType,
                      nop_if_duplicate: bool = False,
                      duplicate_regex: str | None = None,
                      expose_duplicate_error: bool = False,
                      transitioned_at: int | None = None,
                      existing_cluster_hash: str | None = None) -> None:
    """Add a cluster event.

    Args:
        cluster_name: Name of the cluster.
        new_status: New status of the cluster.
        reason: Reason for the event.
        event_type: Type of the event.
        nop_if_duplicate: If True, do not add the event if it is a duplicate.
        duplicate_regex: If provided, do not add the event if it matches the
            regex. Only used if nop_if_duplicate is True.
        expose_duplicate_error: If True, raise an error if the event is a
            duplicate. Only used if nop_if_duplicate is True.
        transitioned_at: If provided, use this timestamp for the event.
        existing_cluster_hash: If provided, add the event only when the current
            row has this cluster-generation hash.
    """
    global_user_state_cluster_events.add_cluster_event(
        _db_manager.get_engine, orm.Session, sqlite, postgresql, cluster_table,
        cluster_event_table, get_last_cluster_event, logger,
        common_utils.get_current_request_id, time.time,
        _UNIQUE_CONSTRAINT_FAILED_ERROR_MSGS, cluster_name, new_status, reason,
        event_type, nop_if_duplicate, duplicate_regex, expose_duplicate_error,
        transitioned_at, existing_cluster_hash)


def get_last_cluster_event(cluster_hash: str,
                           event_type: ClusterEventType,
                           session: 'orm.Session | None' = None) -> str | None:
    return global_user_state_cluster_events.get_last_cluster_event(
        _session_scope(session), cluster_event_table, cluster_hash, event_type)


def get_terminal_or_last_status_change_event(cluster_hash: str) -> str | None:
    return (global_user_state_cluster_events.
            get_terminal_or_last_status_change_event(_db_manager.get_engine(),
                                                     orm.Session,
                                                     cluster_event_table,
                                                     ClusterEventType,
                                                     cluster_hash))


def _get_last_or_terminal_cluster_event_multiple(
        cluster_hashes: set[str]) -> dict[str, str]:
    """Returns the last or terminal cluster event for each cluster."""
    return (global_user_state_cluster_events.
            get_last_or_terminal_cluster_event_multiple(
                _db_manager.get_engine(), orm.Session, cluster_event_table,
                ClusterEventType, cluster_hashes))


def get_last_cluster_event_of_type_multiple(
        cluster_hashes: set[str],
        event_type: ClusterEventType) -> dict[str, str]:
    """Returns the latest event of `event_type` per cluster_hash.

    Mirrors _get_last_or_terminal_cluster_event_multiple but filters to a
    single event type (no TERMINAL-priority ordering).
    """
    return (global_user_state_cluster_events.
            get_last_cluster_event_of_type_multiple(_db_manager.get_engine,
                                                    orm.Session,
                                                    cluster_event_table,
                                                    cluster_hashes, event_type))


def get_last_status_change_times(
        cluster_hashes: set[str],
        ending_status: status_lib.ClusterStatus) -> dict[str, int]:
    """Latest STATUS_CHANGE.transitioned_at per cluster for an ending_status.

    Returns a mapping from cluster_hash to the epoch-seconds at which that
    cluster most recently transitioned into ``ending_status``. Clusters
    with no matching STATUS_CHANGE row are omitted.

    Chunks the ``cluster_hash IN (...)`` predicate by
    ``_CLUSTER_IN_QUERY_CHUNK_SIZE`` to stay under SQLite's 999-parameter
    cap (PostgreSQL has no such cap but the chunking is harmless there).
    """
    return global_user_state_cluster_events.get_last_status_change_times(
        _db_manager.get_engine, orm.Session, cluster_event_table,
        _CLUSTER_IN_QUERY_CHUNK_SIZE, ClusterEventType.STATUS_CHANGE.value,
        cluster_hashes, ending_status)


def get_first_status_change_time_since(cluster_hash: str,
                                       ending_status: status_lib.ClusterStatus,
                                       since: float) -> int | None:
    """Earliest STATUS_CHANGE.transitioned_at into ``ending_status``.

    Only rows at or after ``since`` are considered, so callers can scope the
    answer to the current cluster generation. Returns None when no such row
    exists.

    Unlike :func:`get_last_status_change_times`, this answers "when did the
    cluster first enter this status", which is the right question whenever a
    status can be re-entered by a transient probe failure: the repeated entry
    writes a fresh row, and the latest one would keep sliding forward.
    """
    return global_user_state_cluster_events.get_first_status_change_time_since(
        _db_manager.get_engine(), orm.Session, cluster_event_table,
        ClusterEventType.STATUS_CHANGE.value, cluster_hash, ending_status,
        since)


def cleanup_cluster_events_with_retention(retention_hours: float,
                                          event_type: ClusterEventType) -> None:
    global_user_state_cluster_events.cleanup_cluster_events_with_retention(
        _db_manager.get_engine(), orm.Session, cluster_event_table, logger,
        time.time, retention_hours, event_type)


async def cluster_event_retention_daemon():
    """Garbage collect cluster events periodically."""
    while True:
        logger.info('Running cluster event retention daemon...')
        # Use the latest config.
        skypilot_config.reload_config()
        retention_hours = skypilot_config.get_nested(
            ('api_server', 'cluster_event_retention_hours'),
            DEFAULT_CLUSTER_EVENT_RETENTION_HOURS)
        debug_retention_hours = skypilot_config.get_nested(
            ('api_server', 'cluster_debug_event_retention_hours'),
            DEBUG_CLUSTER_EVENT_RETENTION_HOURS)
        terminal_retention_hours = skypilot_config.get_nested(
            ('api_server', 'cluster_terminal_event_retention_hours'),
            TERMINAL_CLUSTER_EVENT_RETENTION_HOURS)
        try:
            if retention_hours >= 0:
                logger.debug('Cleaning up cluster events with retention '
                             f'{retention_hours} hours.')
                cleanup_cluster_events_with_retention(
                    retention_hours, ClusterEventType.STATUS_CHANGE)
            if debug_retention_hours >= 0:
                logger.debug('Cleaning up debug cluster events with retention '
                             f'{debug_retention_hours} hours.')
                cleanup_cluster_events_with_retention(debug_retention_hours,
                                                      ClusterEventType.DEBUG)
                # LAUNCH_PROGRESS shares debug retention semantics: short-lived
                # observability info, no business-record value once the launch
                # is over.
                cleanup_cluster_events_with_retention(
                    debug_retention_hours, ClusterEventType.LAUNCH_PROGRESS)
            if terminal_retention_hours >= 0:
                logger.debug(
                    'Cleaning up terminal cluster events with retention '
                    f'{terminal_retention_hours} hours.')
                cleanup_cluster_events_with_retention(terminal_retention_hours,
                                                      ClusterEventType.TERMINAL)
        except asyncio.CancelledError:
            logger.info('Cluster event retention daemon cancelled')
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Error running cluster event retention daemon: {e}')

        # Run daemon at most once every hour to avoid too frequent cleanup.
        sleep_amount = max(
            min(retention_hours * 3600, debug_retention_hours * 3600),
            MIN_CLUSTER_EVENT_DAEMON_INTERVAL_SECONDS)
        await asyncio.sleep(sleep_amount)


@typing.overload
def get_cluster_events(
    cluster_name: str | None,
    cluster_hash: str | None,
    event_type: ClusterEventType | list[ClusterEventType],
    include_timestamps: Literal[False] = False,
    limit: int | None = ...,
) -> list[str]:
    ...


@typing.overload
def get_cluster_events(
    cluster_name: str | None,
    cluster_hash: str | None,
    event_type: ClusterEventType | list[ClusterEventType],
    include_timestamps: Literal[True],
    limit: int | None = ...,
) -> list[dict[str, str | int]]:
    ...


@typing.overload
def get_cluster_events(
    cluster_name: str | None,
    cluster_hash: str | None,
    event_type: ClusterEventType | list[ClusterEventType],
    include_timestamps: bool = ...,
    limit: int | None = ...,
) -> list[str] | list[dict[str, str | int]]:
    ...


@db_retries.retry
def get_cluster_events(
        cluster_name: str | None,
        cluster_hash: str | None,
        event_type: ClusterEventType | list[ClusterEventType],
        include_timestamps: bool = False,
        limit: int | None = None) -> list[str] | list[dict[str, str | int]]:
    """Returns the cluster events for the cluster.

    Args:
        cluster_name: Name of the cluster. Cannot be specified if cluster_hash
            is specified.
        cluster_hash: Hash of the cluster. Cannot be specified if cluster_name
            is specified.
        event_type: Event type, or a list of event types to include.
        include_timestamps: If True, returns list of dicts with 'reason' and
            'transitioned_at' fields. If False, returns list of reason strings.
        limit: If specified, returns at most this many events (most recent),
            across all the requested event types. If None, returns all events.

    Returns:
        If include_timestamps is False: List of reason strings.
        If include_timestamps is True: List of dicts with 'reason' and
            'transitioned_at' (unix timestamp) fields.
        Events are ordered from oldest to newest.
    """
    engine = _db_manager.get_engine()

    cluster_hash = _resolve_cluster_hash(cluster_hash, cluster_name)
    if cluster_hash is None:
        raise ValueError(f'Hash for cluster {cluster_name} not found.')
    return global_user_state_cluster_events.get_cluster_events(
        engine, orm.Session, cluster_event_table, cluster_hash,
        ClusterEventType, event_type, include_timestamps, limit)


_CLUSTER_EVENT_NAMES_CHUNK = 500


@db_retries.retry
def get_cluster_events_by_names(
    cluster_names: list[str],
    event_types: list[ClusterEventType],
    limit: int | None = None,
) -> list[dict[str, str | int]]:
    """Returns cluster events looked up by persisted cluster names.

    Unlike get_cluster_events, this filters on the cluster_events ``name``
    column directly instead of resolving the name to a hash via the clusters
    table. This means events remain queryable after the cluster row (and its
    name->hash mapping) has been removed on teardown, which matters for
    finished managed jobs whose clusters have already been torn down.

    Args:
        cluster_names: Names of the clusters.
        event_types: Event types to include.
        limit: If specified, returns at most this many events (most recent),
            across all names and requested event types.

    Returns:
        List of dicts with 'reason' and 'transitioned_at' (unix timestamp)
        fields, ordered from newest to oldest.
    """
    return global_user_state_cluster_events.get_cluster_events_by_names(
        _db_manager.get_engine, orm.Session, cluster_event_table,
        _CLUSTER_EVENT_NAMES_CHUNK, cluster_names, event_types, limit)


def _get_user_hash_or_current_user(user_hash: str | None) -> str:
    """Returns the user hash or the current user hash, if user_hash is None.

    This is to ensure that the clusters created before the client-server
    architecture (no user hash info previously) are associated with the current
    user.
    """
    if user_hash is not None:
        return user_hash
    return common_utils.get_user_hash()


@metrics_lib.time_me
def update_cluster_handle(cluster_name: str,
                          cluster_handle: 'backends.ResourceHandle',
                          existing_cluster_hash: str | None = None) -> None:
    engine = _db_manager.get_engine()
    handle = pickle.dumps(cluster_handle)

    # Extract current node names and merge with existing lineage
    current_names = None
    if hasattr(cluster_handle, 'cached_cluster_info'):
        ci = cluster_handle.cached_cluster_info
        if ci is not None:
            current_names = ci.get_node_names()

    update_dict: dict[Any, Any] = {cluster_table.c.handle: handle}

    with orm.Session(engine) as session:
        query = session.query(cluster_table).filter_by(name=cluster_name)
        if existing_cluster_hash is not None:
            query = query.filter_by(cluster_hash=existing_cluster_hash)
        cluster_row = query.with_for_update().first()
        if cluster_row is None:
            count = 0
        else:
            try:
                stored_handle = pickle.loads(cluster_row.handle)
            except Exception as e:  # pylint: disable=broad-except
                raise ValueError(
                    'Cannot safely apply a metadata-only cluster handle '
                    'update because the stored handle is unreadable.') from e
            stored_resources = getattr(stored_handle, 'launched_resources',
                                       None)
            updated_resources = getattr(cluster_handle, 'launched_resources',
                                        None)
            if (_container_image_execution_state(stored_resources)
                    != _container_image_execution_state(updated_resources)):
                raise ValueError(
                    'update_cluster_handle() is metadata-only and cannot '
                    'change container image execution state. Use '
                    'add_or_update_cluster() so the durable image reference '
                    'is updated atomically.')

            if current_names is not None:
                node_names = common_utils.merge_node_names_lineage(
                    cluster_row.node_names, current_names)
                update_dict[cluster_table.c.node_names] = node_names

            count = query.update(update_dict)
        session.commit()
    assert count <= 1, count
    if count == 0 and existing_cluster_hash is not None:
        raise ValueError(f'Cluster {cluster_name} with hash '
                         f'{existing_cluster_hash} not found.')


@metrics_lib.time_me
def update_last_use(cluster_name: str):
    """Updates the last used command for the cluster."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(cluster_table).filter_by(name=cluster_name).update(
            {cluster_table.c.last_use: common_utils.get_current_command()})
        session.commit()


@db_retries.retry
@metrics_lib.time_me
def remove_cluster(
    cluster_name: str,
    terminate: bool,
    existing_cluster_hash: str | None = None,
    *,
    expected_cluster_record_uuid: uuid.UUID | str | None = None,
    expected_cluster_handle: Any = _CLUSTER_RECORD_HANDLE_UNSET
) -> ClusterRecordRemovalOutcome | None:
    """Removes or stops a cluster mapping.

    If ``existing_cluster_hash`` is provided, only that cluster generation is
    mutated. A missing or replaced generation is a no-op.

    ``expected_cluster_record_uuid`` is the internal action-aware teardown
    fence.  It is PostgreSQL-only, requires ``terminate=True`` and an explicit
    ``expected_cluster_handle`` (``None`` means the admission-time row was
    absent), and compares a present row's exact persisted handle bytes before
    deletion.  A missing row is an idempotent ``ALREADY_ABSENT`` result; a
    legacy/null, differently identified, or byte-different row is a conflict.
    """
    engine = _db_manager.get_engine()
    parsed_cluster_record_uuid = (
        None if expected_cluster_record_uuid is None else
        _canonical_cluster_record_uuid(expected_cluster_record_uuid))
    if parsed_cluster_record_uuid is not None:
        if existing_cluster_hash is not None:
            raise ValueError('Expected cluster-record UUID and legacy cluster '
                             'hash fences are mutually exclusive.')
        if not terminate:
            raise ValueError('Expected cluster-record UUID removal requires '
                             'terminate=True.')
        if expected_cluster_handle is _CLUSTER_RECORD_HANDLE_UNSET:
            raise ValueError('Expected cluster-record UUID removal requires '
                             'an explicit expected handle or None.')
        if (engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
            raise RuntimeError(
                'Action-aware cluster identity requires the central '
                'PostgreSQL database.')
    elif expected_cluster_handle is not _CLUSTER_RECORD_HANDLE_UNSET:
        raise ValueError('An expected cluster handle requires an expected '
                         'cluster-record UUID.')
    with orm.Session(engine) as session:
        lock_container_image_cluster_lifecycle_in_session(session, cluster_name)
        if parsed_cluster_record_uuid is not None:
            _lock_cluster_record_uuid_in_session(session,
                                                 parsed_cluster_record_uuid)
        # Read every clusters-table field this function needs in one snapshot;
        # the stop path below writes the handle back in the same session.
        query = session.query(
            cluster_table.c.cluster_hash, cluster_table.c.provision_log_path,
            cluster_table.c.handle, cluster_table.c.cluster_record_uuid,
            cluster_table.c.workspace,
            cluster_table.c.container_image_binding_known,
            cluster_table.c.container_image_consumer_kind,
            cluster_table.c.container_image_consumer_owner).filter_by(
                name=cluster_name)
        if existing_cluster_hash is not None:
            query = query.filter_by(cluster_hash=existing_cluster_hash)
        row = query.with_for_update().first()
        if row is None and existing_cluster_hash is not None:
            return None
        if parsed_cluster_record_uuid is not None:
            if row is None:
                session.commit()
                return ClusterRecordRemovalOutcome.ALREADY_ABSENT
            observed_uuid = row.cluster_record_uuid
            if observed_uuid != parsed_cluster_record_uuid:
                observed = ('null'
                            if observed_uuid is None else str(observed_uuid))
                raise ClusterRecordIdentityConflictError(
                    f'Cluster {cluster_name!r} has incompatible '
                    f'cluster-record UUID {observed}; expected '
                    f'{parsed_cluster_record_uuid}.')
            if expected_cluster_handle is None:
                raise ClusterRecordIdentityConflictError(
                    f'Cluster {cluster_name!r} unexpectedly has a row for '
                    f'cluster-record UUID {parsed_cluster_record_uuid}.')
            expected_handle_bytes = pickle.dumps(expected_cluster_handle)
            if row.handle != expected_handle_bytes:
                raise ClusterRecordIdentityConflictError(
                    f'Cluster {cluster_name!r} has a different persisted '
                    f'handle for cluster-record UUID '
                    f'{parsed_cluster_record_uuid}.')
        cluster_hash = row.cluster_hash if row is not None else None
        provision_log_path = (row.provision_log_path
                              if row is not None else None)
        terminal_demand_id = None
        terminal_workspace = None
        terminal_binding_known = False
        terminal_consumer_kind = None
        terminal_consumer_owner = None
        if terminate and row is not None:
            terminal_workspace = (row.workspace or
                                  constants.SKYPILOT_DEFAULT_WORKSPACE)
            terminal_binding_known = bool(row.container_image_binding_known)
            terminal_consumer_kind = row.container_image_consumer_kind
            terminal_consumer_owner = row.container_image_consumer_owner
        binding_valid = ((terminal_consumer_kind
                          is None) == (terminal_consumer_owner is None))
        if (terminate and row is not None and row.handle and
            (not terminal_binding_known or not binding_valid)):
            try:
                prior_handle = pickle.loads(row.handle)
                prior_resources = getattr(prior_handle, 'launched_resources',
                                          None)
                prior_resolution = getattr(prior_resources,
                                           'resolved_container_image', None)
                terminal_demand_id = getattr(prior_resolution, 'demand_id',
                                             None)
            except Exception:  # pylint: disable=broad-except
                # A corrupt pre-binding handle cannot prove which owner to
                # retire. Deleting the cluster row is safe; the independent
                # two-observation reconciler will later release its fence.
                terminal_demand_id = None
        # Reuse this session: remove_cluster already holds a pooled connection
        # (advisory + row locks) here, so the usage-interval read/write must
        # not open nested sessions that self-deadlock a single-connection sync
        # pool. The write joins this transaction and commits with it below.
        usage_intervals = _get_cluster_usage_intervals(cluster_hash,
                                                       session=session)

        # Close the currently-open interval, if there is one. An interval that
        # is already closed must never be reopened and re-closed at "now": the
        # status-refresh daemon reaches this function on every sweep of an
        # already-STOPPED cluster (all nodes report STOPPED, so
        # backend_utils calls post_teardown_cleanup again), and extending the
        # last interval each time would accrue uptime for the whole period the
        # cluster sits stopped. This mirrors add_or_update_cluster, which only
        # appends a new open interval when the last one is closed.
        if usage_intervals and usage_intervals[-1][1] is None:
            assert cluster_hash is not None, cluster_name
            start_time = usage_intervals[-1][0]
            usage_intervals[-1] = (start_time, int(time.time()))
            _set_cluster_usage_intervals(cluster_hash,
                                         usage_intervals,
                                         session=session)

        if provision_log_path:
            assert cluster_hash is not None, cluster_name
            session.query(cluster_history_table).filter_by(
                cluster_hash=cluster_hash
            ).filter(
                cluster_history_table.c.provision_log_path.is_(None)
            ).update({
                cluster_history_table.c.provision_log_path: provision_log_path
            })

        mutation_query = session.query(cluster_table).filter_by(
            name=cluster_name)
        if existing_cluster_hash is not None:
            mutation_query = mutation_query.filter_by(
                cluster_hash=existing_cluster_hash)
        if parsed_cluster_record_uuid is not None:
            mutation_query = mutation_query.filter_by(
                cluster_record_uuid=parsed_cluster_record_uuid)
        if terminate:
            if (terminal_workspace is not None and
                ((terminal_binding_known and terminal_consumer_kind == 'cluster'
                  and terminal_consumer_owner is not None) or
                 terminal_demand_id is not None)):
                # Import locally to avoid global_user_state -> demand_state ->
                # catalog_state -> global_user_state initialization recursion.
                # pylint: disable=import-outside-toplevel
                from sky.container_images import demand_state

                # pylint: enable=import-outside-toplevel
                if (terminal_binding_known and
                        terminal_consumer_kind == 'cluster' and
                        terminal_consumer_owner is not None):
                    demand_state.release_owner_authoritatively_in_session(
                        session, terminal_workspace, terminal_consumer_kind,
                        terminal_consumer_owner)
                else:
                    assert terminal_demand_id is not None
                    # Compatibility for pre-binding cluster rows. The kind
                    # guard leaves shared job and Serve owners untouched.
                    demand_state.release_demand_authoritatively_in_session(
                        session,
                        terminal_demand_id,
                        terminal_workspace,
                        expected_consumer_kind='cluster')
            count = mutation_query.delete()
        else:
            if row is None or row.handle is None:
                return None
            handle = pickle.loads(row.handle)
            # Must invalidate IP list to avoid directly trying to ssh into a
            # stopped VM, which leads to timeout.
            if hasattr(handle, 'stable_internal_external_ips'):
                handle = typing.cast('backends.CloudVmRayResourceHandle',
                                     handle)
                handle.stable_internal_external_ips = None
            current_time = int(time.time())
            count = mutation_query.update({
                cluster_table.c.handle: pickle.dumps(handle),
                cluster_table.c.status: status_lib.ClusterStatus.STOPPED.value,
                cluster_table.c.status_updated_at: current_time
            })
        assert count <= 1, count
        if parsed_cluster_record_uuid is not None and count != 1:
            raise ClusterRecordIdentityConflictError(
                f'Cluster {cluster_name!r} changed during exact removal.')
        session.commit()
        if parsed_cluster_record_uuid is not None:
            return ClusterRecordRemovalOutcome.REMOVED_EXACT
        return None


@db_retries.retry
@metrics_lib.time_me
def get_handle_from_cluster_name(
    cluster_name: str,
    existing_cluster_hash: str | None = None
) -> Optional['backends.ResourceHandle']:
    engine = _db_manager.get_engine()
    assert cluster_name is not None, 'cluster_name cannot be None'
    with orm.Session(engine) as session:
        query = session.query(
            cluster_table.c.handle).filter_by(name=cluster_name)
        if existing_cluster_hash is not None:
            query = query.filter_by(cluster_hash=existing_cluster_hash)
        row = query.first()
    if row is None:
        return None
    return pickle.loads(row.handle)


@db_retries.retry
@metrics_lib.time_me
def get_cluster_handle_status_from_name(
    cluster_name: str,
    existing_cluster_hash: str | None = None
) -> tuple[Optional['backends.ResourceHandle'], status_lib.ClusterStatus |
           None]:
    """Returns one cluster row's handle and status from a single query."""
    engine = _db_manager.get_engine()
    assert cluster_name is not None, 'cluster_name cannot be None'
    with orm.Session(engine) as session:
        query = session.query(cluster_table.c.handle, cluster_table.c.status)
        query = query.filter_by(name=cluster_name)
        if existing_cluster_hash is not None:
            query = query.filter_by(cluster_hash=existing_cluster_hash)
        row = query.first()
    if row is None:
        return None, None
    return pickle.loads(row.handle), status_lib.ClusterStatus[row.status]


@db_retries.retry
@metrics_lib.time_me
def get_handles_from_cluster_names(
        cluster_names: set[str]
) -> dict[str, Optional['backends.ResourceHandle']]:
    # Chunk the IN list to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER
    # (default 999 on sqlite < 3.32) and avoid huge IN-clause planning on
    # PostgreSQL. See _CLUSTER_IN_QUERY_CHUNK_SIZE for the rationale.
    result: dict[str, backends.ResourceHandle | None] = {}
    if not cluster_names:
        return result
    engine = _db_manager.get_engine()
    names_list = list(cluster_names)
    with orm.Session(engine) as session:
        for offset in range(0, len(names_list), _CLUSTER_IN_QUERY_CHUNK_SIZE):
            batch = names_list[offset:offset + _CLUSTER_IN_QUERY_CHUNK_SIZE]
            rows = session.query(cluster_table.c.name,
                                 cluster_table.c.handle).filter(
                                     cluster_table.c.name.in_(batch)).all()
            for row in rows:
                result[row.name] = (pickle.loads(row.handle)
                                    if row is not None else None)
    return result


@metrics_lib.time_me
def get_cluster_name_to_handle_map(
    is_managed: bool | None = None,
) -> dict[str, Optional['backends.ResourceHandle']]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = session.query(cluster_table.c.name, cluster_table.c.handle)
        if is_managed is not None:
            query = query.filter(cluster_table.c.is_managed == int(is_managed))
        rows = query.all()
    name_to_handle = {}
    for row in rows:
        if row.handle and len(row.handle) > 0:
            name_to_handle[row.name] = pickle.loads(row.handle)
        else:
            name_to_handle[row.name] = None
    return name_to_handle


@metrics_lib.time_me
async def get_status_from_cluster_name_async(
        cluster_name: str) -> status_lib.ClusterStatus | None:
    """Get the status of a cluster."""
    engine = await _db_manager.get_async_engine()
    assert cluster_name is not None, 'cluster_name cannot be None'
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(cluster_table.c.status).where(
                cluster_table.c.name == cluster_name))
        row = result.first()

        if row is None:
            return None
        return status_lib.ClusterStatus(row[0])


@metrics_lib.time_me
def get_status_from_cluster_name(
        cluster_name: str) -> status_lib.ClusterStatus | None:
    engine = _db_manager.get_engine()
    assert cluster_name is not None, 'cluster_name cannot be None'
    with orm.Session(engine) as session:
        row = session.query(
            cluster_table.c.status).filter_by(name=cluster_name).first()
    if row is None:
        return None
    return status_lib.ClusterStatus[row.status]


@metrics_lib.time_me
def get_glob_cluster_names(
        cluster_names: list[str],
        workspaces_filter: set[str] | None = None) -> list[str]:
    engine = _db_manager.get_engine()
    if not cluster_names:
        return []
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            match_filters = [
                cluster_table.c.name.op('GLOB')(cluster_name)
                for cluster_name in cluster_names
            ]
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            match_filters = [
                cluster_table.c.name.op('SIMILAR TO')(
                    _glob_to_similar(cluster_name))
                for cluster_name in cluster_names
            ]
        else:
            raise ValueError('Unsupported database dialect')
        query = session.query(cluster_table.c.name).filter(
            sqlalchemy.or_(*match_filters))
        if workspaces_filter is not None:
            query = query.filter(
                cluster_table.c.workspace.in_(workspaces_filter))
        rows = query.all()
    return [row.name for row in rows]


@db_retries.retry
@metrics_lib.time_me
def set_cluster_status(cluster_name: str,
                       status: status_lib.ClusterStatus) -> int:
    """Sets the status of a cluster.

    Returns:
        The status_updated_at timestamp written to the database, so callers
        holding the cluster lock can patch an in-memory record instead of
        re-reading the full row.
    """
    engine = _db_manager.get_engine()
    current_time = int(time.time())
    with orm.Session(engine) as session:
        count = session.query(cluster_table).filter_by(
            name=cluster_name).update({
                cluster_table.c.status: status.value,
                cluster_table.c.status_updated_at: current_time
            })
        session.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')
    return current_time


@metrics_lib.time_me
def set_cluster_autostop_value(cluster_name: str, idle_minutes: int,
                               to_down: bool) -> None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(cluster_table).filter_by(
            name=cluster_name).update({
                cluster_table.c.autostop: idle_minutes,
                cluster_table.c.to_down: int(to_down)
            })
        session.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@metrics_lib.time_me
def get_cluster_launch_time(cluster_name: str) -> int | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.query(
            cluster_table.c.launched_at).filter_by(name=cluster_name).first()
    if row is None or row.launched_at is None:
        return None
    return int(row.launched_at)


@metrics_lib.time_me
def get_cluster_info(cluster_name: str) -> dict[str, Any] | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.query(
            cluster_table.c.metadata).filter_by(name=cluster_name).first()
    if row is None or row.metadata is None:
        return None
    return json.loads(row.metadata)


@metrics_lib.time_me
def get_cluster_provision_log_path(cluster_name: str) -> str | None:
    """Returns provision_log_path from clusters table, if recorded."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.query(cluster_table.c.provision_log_path).filter_by(
            name=cluster_name).first()
    if row is None:
        return None
    return row.provision_log_path


@metrics_lib.time_me
def get_cluster_history_provision_log_path(cluster_name: str) -> str | None:
    """Returns provision_log_path from cluster_history for this name.

    If the cluster currently exists, we use its hash. Otherwise, we look up
    historical rows by name and choose the most recent one based on
    usage_intervals.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        # Try current cluster first (fast path)
        cluster_hash = _get_hash_for_existing_cluster(cluster_name)
        if cluster_hash is not None:
            row = session.query(cluster_history_table).filter_by(
                cluster_hash=cluster_hash).first()
            if row is not None:
                return getattr(row, 'provision_log_path', None)

        # Fallback: search history by name and pick the latest by
        # usage_intervals
        rows = session.query(cluster_history_table).filter_by(
            name=cluster_name).all()
        if not rows:
            return None

        def latest_timestamp(usages_bin) -> int:
            try:
                intervals = pickle.loads(usages_bin)
                # intervals: List[Tuple[int, Optional[int]]]
                if not intervals:
                    return -1
                _, end = intervals[-1]
                return end if end is not None else int(time.time())
            except Exception:  # pylint: disable=broad-except
                return -1

        latest_row = max(rows,
                         key=lambda r: latest_timestamp(r.usage_intervals))
        return getattr(latest_row, 'provision_log_path', None)


@metrics_lib.time_me
def set_cluster_info(cluster_name: str, metadata: dict[str, Any]) -> None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(cluster_table).filter_by(
            name=cluster_name).update(
                {cluster_table.c.metadata: json.dumps(metadata)})
        session.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@metrics_lib.time_me
def get_cluster_storage_mounts_metadata(
        cluster_name: str) -> dict[str, Any] | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = (session.query(cluster_table.c.storage_mounts_metadata).filter_by(
            name=cluster_name).first())
    if row is None or row.storage_mounts_metadata is None:
        return None
    return pickle.loads(row.storage_mounts_metadata)


@metrics_lib.time_me
def set_cluster_storage_mounts_metadata(
        cluster_name: str, storage_mounts_metadata: dict[str, Any]) -> None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(cluster_table).filter_by(
            name=cluster_name).update({
                cluster_table.c.storage_mounts_metadata:
                    pickle.dumps(storage_mounts_metadata)
            })
        session.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster {cluster_name} not found.')


@metrics_lib.time_me
def get_cluster_skylet_ssh_tunnel_snapshot(
        cluster_name: str) -> ClusterSkyletSSHTunnelSnapshotV1 | None:
    return (
        global_user_state_skylet_tunnels.get_cluster_skylet_ssh_tunnel_snapshot(
            _db_manager.get_engine, orm.Session, cluster_table, cluster_name))


@metrics_lib.time_me
def get_cluster_skylet_ssh_tunnel_metadata(cluster_name: str) -> object | None:
    """Compatibility facade returning only decoded tunnel metadata."""
    snapshot = get_cluster_skylet_ssh_tunnel_snapshot(cluster_name)
    if snapshot is None:
        return None
    return snapshot.metadata


@metrics_lib.time_me
def compare_and_set_cluster_skylet_ssh_tunnel_metadata(
    cluster_name: str,
    *,
    observed: ClusterSkyletSSHTunnelSnapshotV1,
    replacement: SkyletSSHTunnelMetadata | None,
) -> 'skylet_transport.TunnelMutationResult':
    return (global_user_state_skylet_tunnels.
            compare_and_set_cluster_skylet_ssh_tunnel_metadata(
                _db_manager.get_engine,
                orm.Session,
                cluster_table,
                cluster_name,
                observed=observed,
                replacement=replacement,
            ))


@metrics_lib.time_me
def _get_cluster_usage_intervals(
    cluster_hash: str | None,
    session: 'orm.Session | None' = None
) -> list[tuple[int, int | None]] | None:
    if cluster_hash is None:
        return None
    with _session_scope(session) as active_session:
        row = active_session.query(
            cluster_history_table.c.usage_intervals).filter_by(
                cluster_hash=cluster_hash).first()
    if row is None or row.usage_intervals is None:
        return None
    return pickle.loads(row.usage_intervals)


def _get_cluster_launch_time(
        usage_intervals: list[tuple[int, int | None]] | None) -> int | None:
    if usage_intervals is None:
        return None
    return usage_intervals[0][0]


def _get_cluster_duration(
        usage_intervals: list[tuple[int, int | None]] | None) -> int:
    total_duration = 0

    if usage_intervals is None:
        return total_duration

    for i, (start_time, end_time) in enumerate(usage_intervals):
        # duration from latest start time to time of query
        if start_time is None:
            continue
        if end_time is None:
            assert i == len(usage_intervals) - 1, i
            end_time = int(time.time())
        start_time, end_time = int(start_time), int(end_time)
        total_duration += end_time - start_time
    return total_duration


def _get_cluster_last_activity_time(
        usage_intervals: list[tuple[int, int | None]] | None) -> int | None:
    last_activity_time = None
    if usage_intervals:
        last_interval = usage_intervals[-1]
        last_activity_time = (last_interval[1] if last_interval[1] is not None
                              else last_interval[0])
    return last_activity_time


@metrics_lib.time_me
def _set_cluster_usage_intervals(cluster_hash: str,
                                 usage_intervals: list[tuple[int, int | None]],
                                 session: 'orm.Session | None' = None) -> None:
    # Calculate last_activity_time from usage_intervals
    last_activity_time = _get_cluster_last_activity_time(usage_intervals)
    usage_updated_at = int(time.time())

    # When the caller supplies a session this write joins that transaction and
    # the caller owns the commit; committing here would end the caller's
    # transaction early (e.g. release remove_cluster's advisory/row locks).
    owns_session = session is None
    with _session_scope(session) as active_session:
        count = active_session.query(cluster_history_table).filter_by(
            cluster_hash=cluster_hash).update({
                cluster_history_table.c.usage_intervals:
                    pickle.dumps(usage_intervals),
                cluster_history_table.c.last_activity_time: last_activity_time,
                cluster_history_table.c.usage_updated_at: usage_updated_at,
            })
        if owns_session:
            active_session.commit()
    assert count <= 1, count
    if count == 0:
        raise ValueError(f'Cluster hash {cluster_hash} not found.')


@metrics_lib.time_me
def set_owner_identity_for_cluster(
        cluster_name: str,
        owner_identity: list[str] | None,
        existing_cluster_hash: str | None = None) -> None:
    engine = _db_manager.get_engine()
    if owner_identity is None:
        return
    owner_identity_str = json.dumps(owner_identity)
    with orm.Session(engine) as session:
        query = session.query(cluster_table).filter_by(name=cluster_name)
        if existing_cluster_hash is not None:
            query = query.filter_by(cluster_hash=existing_cluster_hash)
        count = query.update({cluster_table.c.owner: owner_identity_str})
        session.commit()
    assert count <= 1, count
    if count == 0:
        suffix = (f' with hash {existing_cluster_hash}'
                  if existing_cluster_hash is not None else '')
        raise ValueError(f'Cluster {cluster_name}{suffix} not found.')


@metrics_lib.time_me
def _get_hash_for_existing_cluster(cluster_name: str) -> str | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = (session.query(
            cluster_table.c.cluster_hash).filter_by(name=cluster_name).first())
    if row is None or row.cluster_hash is None:
        return None
    return row.cluster_hash


def get_cluster_hash_for_name(cluster_name: str) -> str | None:
    """Return the stable generation ID for a live cluster, if present."""
    return _get_hash_for_existing_cluster(cluster_name)


def _resolve_cluster_hash(cluster_hash: str | None = None,
                          cluster_name: str | None = None) -> str | None:
    """Resolve cluster_hash from either cluster_hash or cluster_name.

    Validates that exactly one of cluster_hash or cluster_name is provided,
    then resolves cluster_name to cluster_hash if needed.

    Args:
        cluster_hash: Direct cluster hash, if known.
        cluster_name: Cluster name to resolve to hash.

    Returns:
        The cluster_hash string, or None if cluster_name was provided but
        the cluster doesn't exist.

    Raises:
        ValueError: If both or neither of cluster_hash/cluster_name are
        provided.
    """
    if cluster_hash is not None and cluster_name is not None:
        raise ValueError(f'Cannot specify both cluster_hash ({cluster_hash}) '
                         f'and cluster_name ({cluster_name})')

    if cluster_hash is None and cluster_name is None:
        raise ValueError('Must specify either cluster_hash or cluster_name')

    if cluster_name is not None:
        return _get_hash_for_existing_cluster(cluster_name)

    return cluster_hash


@metrics_lib.time_me
def get_launched_resources_from_cluster_hash(
        cluster_hash: str) -> tuple[int, Any] | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.query(
            cluster_history_table.c.num_nodes,
            cluster_history_table.c.launched_resources).filter_by(
                cluster_hash=cluster_hash).first()
    if row is None:
        return None
    num_nodes = row.num_nodes
    launched_resources = row.launched_resources

    if num_nodes is None or launched_resources is None:
        return None
    launched_resources = pickle.loads(launched_resources)
    return num_nodes, launched_resources


def _load_owner(record_owner: str | None) -> list[str] | None:
    if record_owner is None:
        return None
    try:
        result = json.loads(record_owner)
        if result is not None and not isinstance(result, list):
            # Backwards compatibility for old records, which were stored as
            # a string instead of a list. It is possible that json.loads
            # will parse the string with all numbers as an int or escape
            # some characters, such as \n, so we need to use the original
            # record_owner.
            return [record_owner]
        return result
    except json.JSONDecodeError:
        # Backwards compatibility for old records, which were stored as
        # a string instead of a list. This will happen when the previous
        # UserId is a string instead of an int.
        return [record_owner]


def _load_storage_mounts_metadata(
    record_storage_mounts_metadata: bytes | None
) -> dict[str, 'Storage.StorageMetadata'] | None:
    if not record_storage_mounts_metadata:
        return None
    return pickle.loads(record_storage_mounts_metadata)


def _cluster_user_join_key(current_user_hash: str) -> sqlalchemy.ColumnElement:
    """Resolve legacy NULL user rows to the current user inside the join."""
    return sqlalchemy.func.coalesce(cluster_table.c.user_hash,
                                    sqlalchemy.literal(current_user_hash))


@db_retries.retry
@metrics_lib.time_me
@context_utils.cancellation_guard
def get_cluster_from_name(
        cluster_name: str | None,
        *,
        include_user_info: bool = True,
        summary_response: bool = False) -> dict[str, Any] | None:
    return global_user_state_cluster_control_plane_reads.get_cluster_from_name(
        _db_manager.get_engine,
        orm.Session,
        cluster_table,
        user_table,
        common_utils.get_user_hash,
        _cluster_user_join_key,
        _load_owner,
        get_terminal_or_last_status_change_event,
        cluster_name,
        include_user_info=include_user_info,
        summary_response=summary_response)


# Bound the IN list per query so we stay under SQLite's
# SQLITE_MAX_VARIABLE_NUMBER (default 999 on sqlite < 3.32, 32766+ on newer
# builds) and avoid pathological IN-clause planning on PostgreSQL. 500 is
# comfortably under both ceilings.
# Module-level so tests can monkeypatch.
_CLUSTER_IN_QUERY_CHUNK_SIZE = 500


@metrics_lib.time_me
def get_clusters_from_names(
    cluster_names: list[str],
    *,
    include_user_info: bool = False,
) -> dict[str, dict[str, Any] | None]:
    """Batched ``get_cluster_from_name`` for many cluster names at once.

    Returns records in the same shape as
    ``get_cluster_from_name(summary_response=True)``. The verbose
    ``summary_response=False`` mode is intentionally not exposed here: it
    would also require batching ``get_terminal_or_last_status_change_event``
    (another per-row DB call), which is out of scope for the callers that
    motivated this helper. Use ``get_cluster_from_name`` for those fields.

    Args:
        cluster_names: List of cluster names to look up.
        include_user_info: If True, resolve user_hash → user through one
            batched user snapshot while the cluster batch session is still
            open. This remains off by default because callers that do not need
            display names should stay on the plain cluster-row hot path.

    Returns:
        Dict mapping ``cluster_name`` to its record, or to ``None`` for
        names that don't exist in the cluster table.
    """
    return global_user_state_cluster_control_plane_reads.get_clusters_from_names(
        _db_manager.get_engine,
        orm.Session,
        cluster_table,
        _CLUSTER_IN_QUERY_CHUNK_SIZE,
        _get_user_hash_or_current_user,
        _get_users_in_session,
        _load_owner,
        cluster_names,
        include_user_info=include_user_info)


@metrics_lib.time_me
def get_cluster_status_fields(
    cluster_names: list[str] | None,
    *,
    exclude_managed_clusters: bool = False,
) -> dict[str, tuple[str | None, int | None]]:
    """Returns the raw (status, status_updated_at) columns for clusters.

    Unlike ``get_clusters_from_names``, this reads only plain columns and
    does no per-row deserialization (no handle unpickling, metadata parsing,
    or enum conversion), so a corrupt row cannot make the lookup raise. If
    ``cluster_names`` is None, all matching clusters are returned. Names not
    present in the cluster table are omitted from the result.
    """
    return global_user_state_cluster_raw_snapshots.get_cluster_status_fields(
        _db_manager.get_engine,
        orm.Session,
        cluster_table,
        _CLUSTER_IN_QUERY_CHUNK_SIZE,
        cluster_names,
        exclude_managed_clusters=exclude_managed_clusters)


@metrics_lib.time_me
def get_cluster_status_fields_by_prefix(
    cluster_name_prefix: str,
    *,
    row_limit: int,
) -> dict[str, tuple[str | None, int | None]]:
    """Return a bounded, ordered plain-column cluster prefix inventory.

    This applies the namespace predicate in the database, so callers proving
    facts about one reserved prefix never load unrelated cluster rows.  The
    extra selected row makes overflow fail closed without an unbounded query.
    """
    return (global_user_state_cluster_raw_snapshots.
            get_cluster_status_fields_by_prefix(_db_manager.get_engine,
                                                orm.Session,
                                                cluster_table,
                                                cluster_name_prefix,
                                                row_limit=row_limit))


@metrics_lib.time_me
def get_managed_cluster_status_fields(
    workload_type: str,) -> dict[str, ManagedClusterStatusFields]:
    """Returns generation-fenced status fields for one managed workload type.

    This is intentionally separate from ``get_cluster_status_fields``:
    ordinary cluster refresh excludes every managed cluster, while a
    workload owner may use this narrower inventory to nominate only rows for
    which it no longer has an exact child record. Rows without a non-empty
    cluster hash cannot be safely fenced and are omitted.
    """
    return (global_user_state_cluster_raw_snapshots.
            get_managed_cluster_status_fields(_db_manager.get_engine,
                                              orm.Session, cluster_table,
                                              ManagedClusterStatusFields,
                                              workload_type))


@metrics_lib.time_me
def get_managed_job_cluster_cleanup_candidates() -> dict[str, str | None]:
    """Returns managed-job cluster names and their durable workload ids.

    Rows written before workload attribution was added have a NULL
    ``workload_type`` and ``workload_id``. Include those legacy managed rows so
    the managed-jobs reconciler can prove ownership from the generated cluster
    name before attempting cleanup. Rows attributed to another workload type
    are excluded here.
    """
    return (global_user_state_cluster_raw_snapshots.
            get_managed_job_cluster_cleanup_candidates(_db_manager.get_engine,
                                                       orm.Session,
                                                       cluster_table))


@metrics_lib.time_me
def get_cluster_image_consumers(
    cluster_names: list[str],
) -> dict[str, tuple[str | None, str | None] | None]:
    """Returns exact managed-image bindings without decoding cluster handles.

    Mapping membership is the authoritative cluster-row existence check. A
    ``None`` value denotes a pre-binding or indeterminate row; a pair of
    ``None`` fields denotes a current writer's validated absence of a consumer.
    """
    result: dict[str, tuple[str | None, str | None] | None] = {}
    if not cluster_names:
        return result
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = session.query(cluster_table.c.name,
                              cluster_table.c.container_image_binding_known,
                              cluster_table.c.container_image_consumer_kind,
                              cluster_table.c.container_image_consumer_owner)
        for offset in range(0, len(cluster_names),
                            _CLUSTER_IN_QUERY_CHUNK_SIZE):
            batch = cluster_names[offset:offset + _CLUSTER_IN_QUERY_CHUNK_SIZE]
            rows = query.filter(cluster_table.c.name.in_(batch)).all()
            for row in rows:
                result[str(row.name)] = _cluster_image_consumer_binding(row)
    return result


def _cluster_image_consumer_binding(
        row: Any) -> tuple[str | None, str | None] | None:
    kind = row.container_image_consumer_kind
    owner = row.container_image_consumer_owner
    binding_valid = (kind is None) == (owner is None)
    if not row.container_image_binding_known or not binding_valid:
        return None
    return kind, owner


def get_cluster_image_consumer_in_session(
    session: orm.Session,
    cluster_name: str,
    *,
    for_update: bool = False,
) -> tuple[bool, tuple[str | None, str | None] | None]:
    """Returns row existence and its binding inside a caller transaction."""
    query = session.query(
        cluster_table.c.container_image_binding_known,
        cluster_table.c.container_image_consumer_kind,
        cluster_table.c.container_image_consumer_owner).filter(
            cluster_table.c.name == cluster_name)
    if for_update:
        query = query.with_for_update()
    row = query.first()
    if row is None:
        return False, None
    return True, _cluster_image_consumer_binding(row)


@metrics_lib.time_me
def get_cluster_refresh_fields(
        cluster_name: str) -> ClusterRefreshFields | None:
    """Returns plain columns that can change status-refresh behavior.

    This avoids deserializing the handle or fetching presentation fields while
    still fencing concurrent autostop updates and row replacement, neither of
    which necessarily bumps ``status_updated_at``.
    """
    return global_user_state_cluster_raw_snapshots.get_cluster_refresh_fields(
        _db_manager.get_engine, orm.Session, cluster_table,
        ClusterRefreshFields, cluster_name)


@metrics_lib.time_me
@context_utils.cancellation_guard
def cluster_with_name_exists(cluster_name: str) -> bool:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.query(
            cluster_table.c.name).filter_by(name=cluster_name).first()
    if row is None:
        return False
    return True


@metrics_lib.time_me
def get_clusters(
    *,  # keyword only separator
    exclude_managed_clusters: bool = False,
    workspaces_filter: set[str] | None = None,
    user_hashes_filter: set[str] | None = None,
    cluster_names: list[str] | None = None,
    summary_response: bool = False,
) -> list[dict[str, Any]]:
    """Get clusters from the database.

    Args:
        exclude_managed_clusters: If True, exclude clusters that have
            is_managed field set to True.
        workspaces_filter: If specified, only include clusters whose
            workspace is in this set. Use workspace names.
        user_hashes_filter: If specified, only include clusters
            that has user_hash field set to one of the values.
        cluster_names: If specified, only include clusters
            that has name field set to one of the values.
    """
    return global_user_state_cluster_listing.get_clusters(
        _db_manager.get_engine,
        orm.Session,
        cluster_table,
        user_table,
        _CLUSTER_IN_QUERY_CHUNK_SIZE,
        get_last_cluster_event_of_type_multiple,
        _get_last_or_terminal_cluster_event_multiple,
        ClusterEventType.LAUNCH_PROGRESS,
        _cluster_user_join_key,
        _load_owner,
        exclude_managed_clusters=exclude_managed_clusters,
        workspaces_filter=workspaces_filter,
        user_hashes_filter=user_hashes_filter,
        cluster_names=cluster_names,
        summary_response=summary_response)


@metrics_lib.time_me
def get_cluster_names(exclude_managed_clusters: bool = False,) -> list[str]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = session.query(cluster_table.c.name)
        if exclude_managed_clusters:
            query = query.filter(cluster_table.c.is_managed == int(False))
        rows = query.all()
    return [row[0] for row in rows]


@metrics_lib.time_me
def get_cluster_names_by_status(status: status_lib.ClusterStatus) -> list[str]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.query(cluster_table.c.name).filter(
            cluster_table.c.status == status.value).all()
    return [row[0] for row in rows]


@metrics_lib.time_me
def get_clusters_from_history(
        days: int | None = None,
        abbreviate_response: bool = False,
        cluster_hashes: list[str] | None = None,
        cluster_names: list[str] | None = None,
        exclude_managed_clusters: bool = False) -> list[dict[str, Any]]:
    """Get cluster reports from history.

    Args:
        days: If specified, only include historical clusters (those not
              currently active) that were last used within the past 'days'
              days. Active clusters are always included regardless of this
              parameter.
        cluster_hashes: If specified, only include clusters whose hash is in
              this list.
        cluster_names: If specified, only include clusters whose name is in
              this list. When both cluster_hashes and cluster_names are
              specified, rows matching either are returned (logical OR).
              Note that a single cluster name can map to multiple history
              records when a name is reused across launches.
        exclude_managed_clusters: If True, exclude clusters launched by a
              controller (managed jobs and services). Rows recorded before the
              is_managed column existed are treated as not managed.

    Returns:
        List of cluster records with history information.
    """
    return global_user_state_cluster_history.get_clusters_from_history(
        _db_manager.get_engine,
        orm.Session,
        cluster_history_table,
        cluster_table,
        time.time,
        common_utils.get_user_hash,
        get_users,
        _get_last_or_terminal_cluster_event_multiple,
        _get_cluster_duration,
        common_utils.get_display_node_names,
        days=days,
        abbreviate_response=abbreviate_response,
        cluster_hashes=cluster_hashes,
        cluster_names=cluster_names,
        exclude_managed_clusters=exclude_managed_clusters)


@metrics_lib.time_me
def get_cluster_names_start_with(starts_with: str) -> list[str]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.query(cluster_table.c.name).filter(
            cluster_table.c.name.like(f'{starts_with}%')).all()
    return [row[0] for row in rows]


@metrics_lib.time_me
def get_cached_enabled_clouds(cloud_capability: 'cloud.CloudCapability',
                              workspace: str) -> list['clouds.Cloud']:
    return global_user_state_cloud_checks.get_cached_enabled_clouds(
        _db_manager.get_engine(), cloud_capability, workspace)


@metrics_lib.time_me
def set_enabled_clouds(enabled_clouds: list[str],
                       cloud_capability: 'cloud.CloudCapability',
                       workspace: str) -> None:
    global_user_state_cloud_checks.set_enabled_clouds(_db_manager.get_engine(),
                                                      enabled_clouds,
                                                      cloud_capability,
                                                      workspace)


@metrics_lib.time_me
def get_cached_check_results(
        workspace: str) -> dict[str, dict[str, dict[str, Any]]]:
    """Return the persisted check_results dict for a workspace, or {}.

    Shape:
        {cloud_repr: {context_or_empty_str: {"enabled": bool, "reason": str}}}.
    """
    return global_user_state_cloud_checks.get_cached_check_results(
        _db_manager.get_engine(), workspace, logger)


@metrics_lib.time_me
def set_check_results(
    results: dict[str, dict[str, dict[str, Any]]],
    workspace: str,
    *,
    is_full_workspace_run: bool,
) -> None:
    """Persist `results` for `workspace`.

    `is_full_workspace_run=True` replaces the entire row (drops clouds /
    contexts not present in `results`).  `False` merges at *context*
    granularity within a cloud: read the existing row, update only the
    individual leaves under each `cloud_repr` in `results`, and preserve
    sibling contexts that the scoped run didn't probe.  Per-context
    merge (rather than replacing the whole cloud entry) is required so a
    single-context recheck — e.g. a per-context lookup on a multi-
    context Kubernetes cloud — does not clobber prior results for
    sibling contexts that the current run didn't iterate.  Stale leaves
    for contexts that have since been removed from a cloud will linger
    until the next full-workspace run rewrites the row.
    """
    global_user_state_cloud_checks.set_check_results(
        _db_manager.get_engine(),
        results,
        workspace,
        logger,
        is_full_workspace_run=is_full_workspace_run)


@metrics_lib.time_me
def get_allowed_clouds(workspace: str) -> list[str]:
    return global_user_state_cloud_checks.get_allowed_clouds(
        _db_manager.get_engine(), workspace)


@metrics_lib.time_me
def set_allowed_clouds(allowed_clouds: list[str], workspace: str) -> None:
    global_user_state_cloud_checks.set_allowed_clouds(_db_manager.get_engine(),
                                                      allowed_clouds, workspace)


@metrics_lib.time_me
def add_or_update_storage(storage_name: str,
                          storage_handle: 'Storage.StorageMetadata',
                          storage_status: status_lib.StorageStatus):
    global_user_state_storage.add_or_update_storage(
        _db_manager.get_engine(), orm.Session, sqlite, postgresql,
        storage_table, storage_name, storage_handle, storage_status)


@metrics_lib.time_me
def remove_storage(storage_name: str):
    """Removes Storage from Database"""
    global_user_state_storage.remove_storage(_db_manager.get_engine(),
                                             orm.Session, storage_table,
                                             storage_name)


@metrics_lib.time_me
def set_storage_status(storage_name: str,
                       status: status_lib.StorageStatus) -> None:
    global_user_state_storage.set_storage_status(_db_manager.get_engine(),
                                                 orm.Session, storage_table,
                                                 storage_name, status)


@metrics_lib.time_me
def get_storage_status(storage_name: str) -> status_lib.StorageStatus | None:
    return global_user_state_storage.get_storage_status(
        _db_manager.get_engine(), orm.Session, storage_table, storage_name)


@metrics_lib.time_me
def set_storage_handle(storage_name: str,
                       handle: 'Storage.StorageMetadata') -> None:
    global_user_state_storage.set_storage_handle(_db_manager.get_engine(),
                                                 orm.Session, storage_table,
                                                 storage_name, handle)


@metrics_lib.time_me
def get_handle_from_storage_name(
        storage_name: str | None) -> Optional['Storage.StorageMetadata']:
    return global_user_state_storage.get_handle_from_storage_name(
        _db_manager.get_engine(), orm.Session, storage_table, storage_name)


@metrics_lib.time_me
def get_glob_storage_name(storage_name: str) -> list[str]:
    return global_user_state_storage.get_glob_storage_name(
        _db_manager.get_engine(), orm.Session, storage_table, storage_name,
        _glob_to_similar)


@metrics_lib.time_me
def get_storage_names_start_with(starts_with: str) -> list[str]:
    return global_user_state_storage.get_storage_names_start_with(
        _db_manager.get_engine(), orm.Session, storage_table, starts_with)


@metrics_lib.time_me
def get_storage() -> list[dict[str, Any]]:
    return global_user_state_storage.get_storage(_db_manager.get_engine(),
                                                 orm.Session, storage_table)


@metrics_lib.time_me
def get_volume_names_start_with(starts_with: str) -> list[str]:
    return global_user_state_volumes.get_volume_names_start_with(
        _db_manager.get_engine(), orm.Session, volume_table, starts_with)


@metrics_lib.time_me
def get_volumes(is_ephemeral: bool | None = None,
                name: str | None = None) -> list[dict[str, Any]]:
    return global_user_state_volumes.get_volumes(_db_manager.get_engine(),
                                                 orm.Session, volume_table,
                                                 is_ephemeral, name)


@metrics_lib.time_me
def get_volume_configs_by_names(
        names: list[str]) -> dict[str, models.VolumeConfig]:
    """Returns one snapshot of the requested volume configs, keyed by name."""
    return global_user_state_volumes.get_volume_configs_by_names(
        _db_manager.get_engine, orm.Session, volume_table, names)


@metrics_lib.time_me
def get_volume_by_name(name: str) -> dict[str, Any] | None:
    return global_user_state_volumes.get_volume_by_name(
        _db_manager.get_engine(), orm.Session, volume_table, name)


@metrics_lib.time_me
def add_volume(
    name: str,
    config: models.VolumeConfig,
    status: status_lib.VolumeStatus,
    is_ephemeral: bool = False,
    creation_yaml: str | None = None,
) -> None:
    global_user_state_volumes.add_volume(_db_manager.get_engine(), orm.Session,
                                         sqlite, postgresql, volume_table, name,
                                         config, status, is_ephemeral,
                                         creation_yaml)


@metrics_lib.time_me
def update_volume_config(name: str, config: models.VolumeConfig) -> None:
    global_user_state_volumes.update_volume_config(_db_manager.get_engine(),
                                                   orm.Session, volume_table,
                                                   name, config)


@metrics_lib.time_me
def update_volume(name: str, last_attached_at: int,
                  status: status_lib.VolumeStatus) -> None:
    global_user_state_volumes.update_volume(_db_manager.get_engine(),
                                            orm.Session, volume_table, name,
                                            last_attached_at, status)


@metrics_lib.time_me
def update_volume_status(name: str,
                         status: status_lib.VolumeStatus,
                         error_message: str | None = None,
                         usedby_pods: list[str] | None = None,
                         usedby_clusters: list[str] | None = None) -> None:
    """Update volume status and related fields.

    Args:
        name: Volume name.
        status: New volume status.
        error_message: Error message (None clears it).
        usedby_pods: List of pods using the volume (None keeps existing value).
        usedby_clusters: List of clusters using the volume (None keeps it).
    """
    global_user_state_volumes.update_volume_status(_db_manager.get_engine(),
                                                   orm.Session, volume_table,
                                                   name, status, error_message,
                                                   usedby_pods, usedby_clusters)


@metrics_lib.time_me
def delete_volume(name: str) -> None:
    global_user_state_volumes.delete_volume(_db_manager.get_engine(),
                                            orm.Session, volume_table, name)


@metrics_lib.time_me
def get_ssh_keys(user_hash: str) -> tuple[str, str, bool]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.query(ssh_key_table).filter_by(
            user_hash=user_hash).first()
    if row:
        return row.ssh_public_key, row.ssh_private_key, True
    return '', '', False


@metrics_lib.time_me
def set_ssh_keys(user_hash: str, ssh_public_key: str, ssh_private_key: str):
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmnt = insert_func(ssh_key_table).values(
            user_hash=user_hash,
            ssh_public_key=ssh_public_key,
            ssh_private_key=ssh_private_key)
        do_update_stmt = insert_stmnt.on_conflict_do_update(
            index_elements=[ssh_key_table.c.user_hash],
            set_={
                ssh_key_table.c.ssh_public_key: ssh_public_key,
                ssh_key_table.c.ssh_private_key: ssh_private_key
            })
        session.execute(do_update_stmt)
        session.commit()


@metrics_lib.time_me
def add_service_account_token(token_id: str,
                              token_name: str,
                              token_hash: str,
                              creator_user_hash: str,
                              service_account_user_id: str,
                              expires_at: int | None = None) -> None:
    """Add a service account token to the database."""
    engine = _db_manager.get_engine()
    created_at = int(time.time())
    global_user_state_service_account_tokens.add_service_account_token(
        engine, orm.Session, sqlite.insert, postgresql.insert, token_id,
        token_name, token_hash, creator_user_hash, service_account_user_id,
        expires_at, created_at)


@metrics_lib.time_me
def get_service_account_token(token_id: str) -> dict[str, Any] | None:
    """Get a service account token by token_id."""
    return global_user_state_service_account_tokens.get_service_account_token(
        _db_manager.get_engine(), orm.Session, token_id)


@metrics_lib.time_me
def get_service_account_token_by_hash(token_hash: str) -> dict[str, Any] | None:
    """Get a service account token by its sha256 hash.

    Used by the request-auth middleware: hashing the incoming bearer token
    and matching against this column is what makes revocation and rotation
    take effect (the DB row's hash is updated on rotation, so old JWTs
    stop matching). Relies on the unique index on token_hash.
    """
    return global_user_state_service_account_tokens.get_service_account_token_by_hash(
        _db_manager.get_engine(), orm.Session, token_hash)


@metrics_lib.time_me
def get_user_service_account_tokens(user_hash: str) -> list[dict[str, Any]]:
    """Get all service account tokens for a user (as creator)."""
    return global_user_state_service_account_tokens.get_user_service_account_tokens(
        _db_manager.get_engine(), orm.Session, user_hash)


@metrics_lib.time_me
def update_service_account_token_last_used(token_id: str) -> None:
    """Update the last_used_at timestamp for a service account token."""
    engine = _db_manager.get_engine()
    last_used_at = int(time.time())

    global_user_state_service_account_tokens.update_service_account_token_last_used(
        engine, orm.Session, token_id, last_used_at)


@db_retries.retry
@metrics_lib.time_me
def delete_service_account_token(token_id: str) -> bool:
    """Delete a service account token.

    Returns:
        True if token was found and deleted.
    """
    return global_user_state_service_account_tokens.delete_service_account_token(
        _db_manager.get_engine(), orm.Session, token_id)


@metrics_lib.time_me
def rotate_service_account_token(token_id: str,
                                 new_token_hash: str,
                                 new_expires_at: int | None = None) -> None:
    """Rotate a service account token by updating its hash and expiration.

    Args:
        token_id: The token ID to rotate.
        new_token_hash: The new hashed token value.
        new_expires_at: New expiration timestamp, or None for no expiration.
    """
    engine = _db_manager.get_engine()
    current_time = int(time.time())

    global_user_state_service_account_tokens.rotate_service_account_token(
        engine, orm.Session, token_id, new_token_hash, new_expires_at,
        current_time)


@db_retries.retry
@metrics_lib.time_me
def get_cluster_yaml_str(cluster_yaml_path: str | None) -> str | None:
    """Get the cluster yaml from the database or the local file system.
    If the cluster yaml is not in the database, check if it exists on the
    local file system and migrate it to the database.

    It is assumed that the cluster yaml file is named as <cluster_name>.yml.
    """
    engine = _db_manager.get_engine()
    if cluster_yaml_path is None:
        raise ValueError('Attempted to read a None YAML.')
    cluster_file_name = os.path.basename(cluster_yaml_path)
    cluster_name, _ = os.path.splitext(cluster_file_name)
    found, yaml_str = global_user_state_cluster_yaml.get_cluster_yaml(
        engine, orm.Session, cluster_yaml_table, cluster_name)
    if not found:
        return _set_cluster_yaml_from_file(cluster_yaml_path, cluster_name)
    return yaml_str


def get_cluster_yaml_str_multiple(
        cluster_yaml_paths: list[str]) -> list[str | None]:
    """Get cluster YAMLs while preserving input order and cardinality."""
    if not cluster_yaml_paths:
        return []

    engine = _db_manager.get_engine()
    cluster_names = []
    cluster_names_to_yaml_paths: dict[str, str] = {}
    for cluster_yaml_path in cluster_yaml_paths:
        cluster_name, _ = os.path.splitext(os.path.basename(cluster_yaml_path))
        cluster_names.append(cluster_name)
        cluster_names_to_yaml_paths[cluster_name] = cluster_yaml_path

    unique_cluster_names = list(cluster_names_to_yaml_paths)
    cluster_names_to_yaml = global_user_state_cluster_yaml.get_cluster_yamls(
        engine, orm.Session, cluster_yaml_table, unique_cluster_names)

    for cluster_name in unique_cluster_names:
        if cluster_name not in cluster_names_to_yaml:
            cluster_names_to_yaml[cluster_name] = _set_cluster_yaml_from_file(
                cluster_names_to_yaml_paths[cluster_name], cluster_name)
    return [cluster_names_to_yaml[name] for name in cluster_names]


def _set_cluster_yaml_from_file(cluster_yaml_path: str,
                                cluster_name: str) -> str | None:
    """Set the cluster yaml in the database from a file."""
    # If the cluster yaml is not in the database, check if it exists
    # on the local file system and migrate it to the database.
    # TODO(syang): remove this check once we have a way to migrate the
    # cluster from file to database. Remove on v0.12.0.
    if cluster_yaml_path is not None:
        # First try the exact path
        path_to_read = None
        if os.path.exists(cluster_yaml_path):
            path_to_read = cluster_yaml_path
        # Fallback: try with .debug suffix (when debug logging was enabled)
        # Debug logging causes YAML files to be saved with .debug suffix
        # but the path stored in the handle doesn't include it
        debug_path = cluster_yaml_path + '.debug'
        if os.path.exists(debug_path):
            path_to_read = debug_path
        if path_to_read is not None:
            with open(path_to_read, encoding='utf-8') as f:
                yaml_str = f.read()
            set_cluster_yaml(cluster_name, yaml_str)
            return yaml_str
    return None


def get_cluster_yaml_dict(cluster_yaml_path: str | None) -> dict[str, Any]:
    """Get the cluster yaml as a dictionary from the database.

    It is assumed that the cluster yaml file is named as <cluster_name>.yml.
    """
    yaml_str = get_cluster_yaml_str(cluster_yaml_path)
    if yaml_str is None:
        raise ValueError(f'Cluster yaml {cluster_yaml_path} not found.')
    return yaml_utils.safe_load(yaml_str)


def get_cluster_yaml_dict_multiple(
        cluster_yaml_paths: list[str]) -> list[dict[str, Any]]:
    """Get the cluster yaml as a dictionary from the database."""
    yaml_strs = get_cluster_yaml_str_multiple(cluster_yaml_paths)
    yaml_dicts = []
    for idx, yaml_str in enumerate(yaml_strs):
        if yaml_str is None:
            raise ValueError(
                f'Cluster yaml {cluster_yaml_paths[idx]} not found.')
        yaml_dicts.append(yaml_utils.safe_load(yaml_str))
    return yaml_dicts


@metrics_lib.time_me
def set_cluster_yaml(cluster_name: str, yaml_str: str) -> None:
    """Set the cluster yaml in the database."""
    engine = _db_manager.get_engine()
    global_user_state_cluster_yaml.set_cluster_yaml(engine,
                                                    _db_manager.get_engine(),
                                                    orm.Session, sqlite,
                                                    postgresql,
                                                    cluster_yaml_table,
                                                    cluster_name, yaml_str)


@metrics_lib.time_me
def remove_cluster_yaml(cluster_name: str):
    engine = _db_manager.get_engine()
    global_user_state_cluster_yaml.remove_cluster_yaml(engine, orm.Session,
                                                       cluster_yaml_table,
                                                       cluster_name)


@metrics_lib.time_me
def get_expired_service_account_tokens_by_name_prefix(
        name_prefix: str, now: int) -> list[dict[str, Any]]:
    """Return service-account tokens that have expired and match a name prefix.

    Tokens with no expiration are excluded. The LIKE pattern is built with
    SQLAlchemy parameterization so the prefix cannot inject SQL.
    """
    return global_user_state_service_account_tokens.get_expired_service_account_tokens_by_name_prefix(
        _db_manager.get_engine(), orm.Session, name_prefix, now)


@metrics_lib.time_me
def get_all_service_account_tokens() -> list[dict[str, Any]]:
    """Get all service account tokens across all users (for admin access)."""
    return global_user_state_service_account_tokens.get_all_service_account_tokens(
        _db_manager.get_engine(), orm.Session)


@metrics_lib.time_me
def get_system_config(config_key: str) -> str | None:
    """Get a system configuration value by key."""
    return global_user_state_system_config.get_system_config(
        _db_manager.get_engine(), config_key)


@metrics_lib.time_me
def get_or_set_system_config(config_key: str, default_value: str) -> str:
    """Atomically return an existing configuration or install a default.

    This is the multi-replica-safe form of a read followed by
    ``set_system_config``. Concurrent first writers may propose different
    defaults, but every caller returns the single value that won the unique-key
    insert.
    """
    return global_user_state_system_config.get_or_set_system_config(
        _db_manager.get_engine(), config_key, default_value, int(time.time()),
        sqlite, postgresql)


@metrics_lib.time_me
def set_system_config(config_key: str, config_value: str) -> None:
    """Set a system configuration value."""
    global_user_state_system_config.set_system_config(_db_manager.get_engine(),
                                                      config_key, config_value,
                                                      int(time.time()), sqlite,
                                                      postgresql)


@metrics_lib.time_me
def record_operator_notification(category: str,
                                 message: str,
                                 dedupe_window_seconds: int,
                                 emitted_at: int | None = None) -> None:
    """Record a low-cardinality operator notification.

    One row is retained per category. Occurrences inside a continuous incident
    update its message/count/last-seen time but preserve its sequence, so an
    operator who acknowledges it is not alerted again until the category has
    been quiet for ``dedupe_window_seconds``.
    """
    if emitted_at is None:
        emitted_at = int(time.time())
    if dedupe_window_seconds < 0:
        raise ValueError('dedupe_window_seconds must be non-negative')

    engine = _db_manager.get_engine()
    global_user_state_notifications.record_operator_notification(
        engine, category, message, dedupe_window_seconds, emitted_at)


@metrics_lib.time_me
def get_operator_notifications(user_id: str, since: int) -> dict[str, Any]:
    """Return recent notification categories and this user's unread state."""
    engine = _db_manager.get_engine()
    return global_user_state_notifications.get_operator_notifications(
        engine, user_id, since)


@db_retries.retry
@metrics_lib.time_me
def mark_operator_notifications_read(user_id: str,
                                     through_sequence: int,
                                     updated_at: int | None = None) -> int:
    """Monotonically advance a user's cursor, clamped to issued sequences."""
    if through_sequence < 0:
        raise ValueError('through_sequence must be non-negative')
    if updated_at is None:
        updated_at = int(time.time())

    engine = _db_manager.get_engine()
    return global_user_state_notifications.mark_operator_notifications_read(
        engine, user_id, through_sequence, updated_at)


def get_max_db_connections() -> int | None:
    """Get PostgreSQL connection capacity available to ordinary clients."""
    engine = _db_manager.get_engine()
    if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        return None
    with sqlalchemy.orm.Session(engine) as session:
        settings = session.execute(
            sqlalchemy.text(
                "SELECT current_setting('max_connections'), "
                "current_setting('superuser_reserved_connections'), "
                "current_setting('reserved_connections', true)")).one()
        max_connections, superuser_reserved, reserved = settings
        if max_connections is None or superuser_reserved is None:
            return None
        # ``reserved_connections`` was added in PostgreSQL 16. The
        # missing_ok=true lookup returns NULL on older supported versions.
        return max(
            0,
            int(max_connections) - int(superuser_reserved) - int(reserved or 0))
