"""Constants for the database schemas."""

import contextlib
import logging
import os
import threading
import time
from typing import cast, Literal

from alembic import command as alembic_command
from alembic.config import Config
from alembic.runtime import migration
import filelock
import sqlalchemy

from sky import sky_logging
from sky.skylet import constants
from sky.utils.db import db_utils

logger = sky_logging.init_logger(__name__)

DB_INIT_LOCK_TIMEOUT_SECONDS = 10
_DISTRIBUTED_MIGRATION_LOCK_POLL_SECONDS = 0.1

# Serialize all Alembic migrations within a process. Alembic's
# EnvironmentContext stores the active migration context in a module-level
# global (alembic.context._proxy), not in threading.local(). Concurrent
# alembic.command.upgrade() calls from different threads overwrite each
# other's proxy, corrupting migration state. The per-section file locks
# (db_lock) only protect the same section across OS processes; they don't
# prevent two threads from running different sections concurrently within
# the same process.
_alembic_thread_lock = threading.Lock()

MigrationMode = Literal['auto', 'upgrade', 'bootstrap', 'verify']


def configured_migration_mode() -> MigrationMode:
    """Returns the process-wide central database migration ownership mode."""
    return cast(
        MigrationMode,
        os.environ.get(constants.ENV_VAR_STATE_DB_MIGRATION_MODE, 'auto'))


GLOBAL_USER_STATE_DB_NAME = 'state_db'
GLOBAL_USER_STATE_VERSION = '028'  # action-aware cluster record identity
GLOBAL_USER_STATE_JOB_MINIMUM_REVISION = '023'
GLOBAL_USER_STATE_LOCK_PATH = f'~/.sky/locks/.{GLOBAL_USER_STATE_DB_NAME}.lock'

SPOT_JOBS_DB_NAME = 'spot_jobs_db'
SPOT_JOBS_VERSION = '028'  # runtime-owned managed-job controller slots
SPOT_JOBS_LOCK_PATH = f'~/.sky/locks/.{SPOT_JOBS_DB_NAME}.lock'

SERVE_DB_NAME = 'serve_db'
SERVE_VERSION = '047'  # generic non-pool binding and legacy evidence
SERVE_NON_POSTGRES_VERSION = '037'  # retained local/controller SQLite head
SERVE_LOCK_PATH = f'~/.sky/locks/.{SERVE_DB_NAME}.lock'
SERVE_MIGRATION_CEILING_ENV_VAR = (
    'SKYPILOT_SERVER_SERVE_SCHEMA_MIGRATION_CEILING')


def serve_target_version(engine: sqlalchemy.engine.Engine) -> str:
    """Return the dialect-safe Serve migration target.

    Serve038+ install PostgreSQL-only authority state and deliberately refuse
    a SQLite stamp.  The chart-owned cleanup ceiling is meaningful only for
    the consolidated PostgreSQL database.  Local/controller SQLite remains at
    its independent 037 head; every other dialect is unsupported.
    """
    configured_ceiling = os.environ.get(SERVE_MIGRATION_CEILING_ENV_VAR)
    dialect = engine.dialect.name
    if configured_ceiling is not None:
        if configured_ceiling != SERVE_NON_POSTGRES_VERSION:
            raise RuntimeError(
                f'{SERVE_MIGRATION_CEILING_ENV_VAR} must be exactly '
                f'{SERVE_NON_POSTGRES_VERSION!r} when present.')
        if dialect != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RuntimeError(
                f'{SERVE_MIGRATION_CEILING_ENV_VAR} applies only to the '
                'PostgreSQL Serve database.')
        return configured_ceiling
    if dialect == db_utils.SQLAlchemyDialect.SQLITE.value:
        return SERVE_NON_POSTGRES_VERSION
    if dialect == db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return SERVE_VERSION
    raise RuntimeError(f'Unsupported SkyServe database dialect {dialect!r}.')


def _require_exact_serve_migration_ceiling(
    engine: sqlalchemy.engine.Engine,
    section: str,
    target_revision: str,
    alembic_ini_path: str | None,
) -> None:
    """Fail closed unless a configured retirement pin is already exact."""
    configured_ceiling = os.environ.get(SERVE_MIGRATION_CEILING_ENV_VAR)
    if configured_ceiling is None or section != SERVE_DB_NAME:
        return
    if target_revision != configured_ceiling:
        raise RuntimeError(
            f'{SERVE_MIGRATION_CEILING_ENV_VAR}={configured_ceiling!r} '
            f'conflicts with Serve target {target_revision!r}.')
    current_revision = get_current_alembic_revision(engine, section,
                                                    alembic_ini_path)
    if current_revision != configured_ceiling:
        observed = current_revision or 'uninitialized'
        raise RuntimeError(
            f'{section} database is at revision {observed}, but '
            f'{SERVE_MIGRATION_CEILING_ENV_VAR} requires exact revision '
            f'{configured_ceiling}. Accepted-V1 retirement must finish before '
            'the Serve schema advances.')


SKYPILOT_CONFIG_DB_NAME = 'sky_config_db'
SKYPILOT_CONFIG_VERSION = '001'  # initial alembic for config_yaml table
SKYPILOT_CONFIG_LOCK_PATH = f'~/.sky/locks/.{SKYPILOT_CONFIG_DB_NAME}.lock'

KV_CACHE_DB_NAME = 'kv_cache_db'
KV_CACHE_VERSION = '001'  # initial kv_cache table for AWS AMIs
KV_CACHE_LOCK_PATH = f'~/.sky/locks/.{KV_CACHE_DB_NAME}.lock'

RECIPES_DB_NAME = 'recipes_db'
RECIPES_VERSION = '001'
RECIPES_LOCK_PATH = f'~/.sky/locks/.{RECIPES_DB_NAME}.lock'

API_REQUESTS_DB_NAME = 'api_requests_db'
API_REQUESTS_VERSION = '011'
API_REQUESTS_LOCK_PATH = f'~/.sky/locks/.{API_REQUESTS_DB_NAME}.lock'

LIFECYCLE_ACTIONS_DB_NAME = 'lifecycle_actions_db'
LIFECYCLE_ACTIONS_VERSION = '001'  # inert lifecycle store identity and scope
LIFECYCLE_ACTIONS_LOCK_PATH = (
    f'~/.sky/locks/.{LIFECYCLE_ACTIONS_DB_NAME}.lock')

CAPACITY_STATE_DB_NAME = 'capacity_state_db'
CAPACITY_STATE_VERSION = '001'  # read-only physical-capacity projection core
CAPACITY_STATE_LOCK_PATH = f'~/.sky/locks/.{CAPACITY_STATE_DB_NAME}.lock'


@contextlib.contextmanager
def db_lock(db_name: str):
    lock_path = os.path.expanduser(f'~/.sky/locks/.{db_name}.lock')
    try:
        with filelock.FileLock(lock_path, timeout=DB_INIT_LOCK_TIMEOUT_SECONDS):
            yield
    except filelock.Timeout as e:
        raise RuntimeError(f'Failed to initialize database due to a timeout '
                           f'when trying to acquire the lock at '
                           f'{lock_path}. '
                           'Please try again or manually remove the lock '
                           f'file if you believe it is stale.') from e


def get_alembic_config(engine: sqlalchemy.engine.Engine,
                       section: str,
                       alembic_ini_path: str | None = None):
    """Get Alembic configuration for the given section.

    Args:
        engine: SQLAlchemy engine for the database.
        section: Alembic section name (e.g., 'state_db' or 'spot_jobs_db').
        alembic_ini_path: Optional path to a custom alembic.ini file.
            If not provided, uses the default SkyPilot alembic.ini.
    """
    if alembic_ini_path is None:
        # Default to SkyPilot's alembic.ini
        alembic_ini_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'setup_files', 'alembic.ini')
    alembic_cfg = Config(alembic_ini_path, ini_section=section)

    # Override the database URL to match SkyPilot's current connection
    # Use render_as_string to get the full URL with password
    url = engine.url.render_as_string(hide_password=False)
    # Replace % with %% to escape the % character in the URL
    # set_section_option uses variable interpolation, which treats % as a
    # special character.
    # any '%' symbol not used for interpolation needs to be escaped.
    url = url.replace('%', '%%')
    alembic_cfg.set_section_option(section, 'sqlalchemy.url', url)

    return alembic_cfg


def get_current_alembic_revision(
    engine: sqlalchemy.engine.Engine,
    section: str,
    alembic_ini_path: str | None = None,
) -> str | None:
    """Returns the current revision without creating or mutating schema."""
    alembic_config = get_alembic_config(engine, section, alembic_ini_path)
    version_table = alembic_config.get_section_option(
        alembic_config.config_ini_section, 'version_table', 'alembic_version')
    with engine.connect() as connection:
        context = migration.MigrationContext.configure(
            connection, opts={'version_table': version_table})
        return context.get_current_revision()


def needs_upgrade(engine: sqlalchemy.engine.Engine,
                  section: str,
                  target_revision: str,
                  alembic_ini_path: str | None = None):
    """Check if the database needs to be upgraded.

    Args:
        engine: SQLAlchemy engine for the database.
        section: Alembic section to upgrade (e.g., 'state_db' or
        'spot_jobs_db').
        target_revision: Target revision to upgrade to (e.g., '001').
        alembic_ini_path: Optional path to a custom alembic.ini file.
    """
    current_rev = get_current_alembic_revision(engine, section,
                                               alembic_ini_path)

    target_rev_num = int(target_revision)
    if current_rev is None:
        if os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is not None:
            logger.debug(f'{section} database currently uninitialized, '
                         f'targeting revision {target_rev_num}')
        return True

    # Compare revisions - assuming they are numeric strings like '001', '002'
    current_rev_num = int(current_rev)
    if (current_rev_num < target_rev_num and
            os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is not None):
        logger.debug(
            f'{section} database currently at revision {current_rev_num}, '
            f'targeting revision {target_rev_num}')

    return current_rev_num < target_rev_num


def verify_alembic_revision(engine: sqlalchemy.engine.Engine,
                            section: str,
                            target_revision: str,
                            alembic_ini_path: str | None = None) -> None:
    """Refuses startup until another process has applied the target revision."""
    current_revision = get_current_alembic_revision(engine, section,
                                                    alembic_ini_path)
    if current_revision is None or int(current_revision) < int(target_revision):
        observed = current_revision or 'uninitialized'
        raise RuntimeError(
            f'{section} database is at revision {observed}, but this process '
            f'requires revision {target_revision}. Run the database migration '
            'job before starting API replicas.')


@contextlib.contextmanager
def _distributed_migration_lock(engine: sqlalchemy.engine.Engine, section: str):
    """Serializes Alembic across hosts sharing one PostgreSQL database."""
    if engine.dialect.name != 'postgresql':
        yield
        return
    lock_name = f'skypilot:alembic:{section}'
    # PostgreSQL session advisory locks do not require a transaction. Keep this
    # connection in DBAPI autocommit and poll with pg_try_advisory_lock. A
    # blocking pg_advisory_lock statement still owns a transaction while it
    # waits, which can deadlock with CREATE INDEX CONCURRENTLY running under
    # the current lock owner.
    lock_engine = db_utils.get_postgres_lock_engine(engine)
    lock_query = sqlalchemy.text('SELECT pg_try_advisory_lock(hashtext(:name))')
    while True:
        with lock_engine.connect().execution_options(
                isolation_level='AUTOCOMMIT') as connection:
            acquired = bool(
                connection.execute(lock_query, {
                    'name': lock_name
                }).scalar_one())
            if acquired:
                try:
                    yield
                finally:
                    connection.execute(
                        sqlalchemy.text(
                            'SELECT pg_advisory_unlock(hashtext(:name))'),
                        {'name': lock_name})
                return
        # Do not retain a nonwinning PostgreSQL session while waiting.
        time.sleep(_DISTRIBUTED_MIGRATION_LOCK_POLL_SECONDS)


def _validate_global_user_state_upgrade_start(
    engine: sqlalchemy.engine.Engine,
    section: str,
    target_revision: str,
    alembic_ini_path: str | None,
    mode: MigrationMode,
) -> None:
    """Prevents revision 024 racing migration code that predates its lock."""
    if (engine.dialect.name != 'postgresql' or
            section != GLOBAL_USER_STATE_DB_NAME or
            int(target_revision) < int(GLOBAL_USER_STATE_VERSION)):
        return
    current_revision = get_current_alembic_revision(engine, section,
                                                    alembic_ini_path)
    if (current_revision is not None and int(current_revision)
            >= int(GLOBAL_USER_STATE_JOB_MINIMUM_REVISION)):
        return
    if (mode == 'bootstrap' and current_revision is None and
            _postgres_effective_schema_is_empty(engine)):
        return
    observed = current_revision or 'uninitialized nonempty schema'
    if current_revision is None and mode != 'bootstrap':
        observed = 'uninitialized schema'
    raise RuntimeError(
        f'{section} database is at revision {observed}. Revision '
        f'{target_revision} requires a staged upgrade through revision '
        f'{GLOBAL_USER_STATE_JOB_MINIMUM_REVISION} before the migration job can '
        'run. Drain older API binaries, complete that predecessor migration, '
        'then retry the job. For a new isolated empty schema, explicitly use '
        'bootstrap mode.')


def _postgres_effective_schema_is_empty(
        engine: sqlalchemy.engine.Engine) -> bool:
    """Proves the connection's target schema owns no user objects."""
    query = sqlalchemy.text("""
        WITH target AS (
            SELECT oid
            FROM pg_catalog.pg_namespace
            WHERE nspname = current_schema()
        ), owned_object AS (
            SELECT 1 FROM pg_catalog.pg_class, target
            WHERE relnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_type, target
            WHERE typnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_proc, target
            WHERE pronamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_operator, target
            WHERE oprnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_collation, target
            WHERE collnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_conversion, target
            WHERE connamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_opclass, target
            WHERE opcnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_opfamily, target
            WHERE opfnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_ts_config, target
            WHERE cfgnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_ts_dict, target
            WHERE dictnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_ts_parser, target
            WHERE prsnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_ts_template, target
            WHERE tmplnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_statistic_ext, target
            WHERE stxnamespace = target.oid
            UNION ALL
            SELECT 1 FROM pg_catalog.pg_extension, target
            WHERE extnamespace = target.oid
        )
        SELECT current_schema(), EXISTS (SELECT 1 FROM owned_object)
    """)
    with engine.connect() as connection:
        schema_name, has_objects = connection.execute(query).one()
    # A search path with no valid target schema is not a provably empty target.
    return schema_name is not None and not bool(has_objects)


def safe_alembic_upgrade(engine: sqlalchemy.engine.Engine,
                         section: str,
                         target_revision: str,
                         alembic_ini_path: str | None = None,
                         *,
                         mode: MigrationMode = 'auto'):
    """Verify or upgrade one schema under local and distributed locks.

    Args:
        engine: SQLAlchemy engine for the database.
        section: Alembic section to upgrade (e.g., 'state_db' or
        'spot_jobs_db').
        target_revision: Target revision to upgrade to (e.g., '001').
        alembic_ini_path: Optional path to a custom alembic.ini file.
        mode: ``verify`` performs no DDL. ``auto`` and ``upgrade`` converge an
            initialized schema. ``bootstrap`` additionally permits the central
            state schema to start from a proven-empty isolated PostgreSQL
            schema; the distinct names make deployment intent explicit.
    """
    if mode not in ('auto', 'upgrade', 'bootstrap', 'verify'):
        raise ValueError(f'Invalid database migration mode: {mode!r}.')
    _require_exact_serve_migration_ceiling(engine, section, target_revision,
                                           alembic_ini_path)
    # set alembic logger to warning level
    alembic_logger = logging.getLogger('alembic')
    alembic_logger.setLevel(logging.WARNING)

    if mode == 'verify':
        verify_alembic_revision(engine, section, target_revision,
                                alembic_ini_path)
        return

    alembic_config = get_alembic_config(engine, section, alembic_ini_path)

    # only acquire lock if db needs upgrade
    if needs_upgrade(engine, section, target_revision, alembic_ini_path):
        with _alembic_thread_lock:
            with db_lock(section):
                with _distributed_migration_lock(engine, section):
                    # Check again after the cross-host lock. The migration Job
                    # or a transitional auto-mode API pod may have completed
                    # the same migration while this process was waiting.
                    if needs_upgrade(engine, section, target_revision,
                                     alembic_ini_path):
                        _validate_global_user_state_upgrade_start(
                            engine, section, target_revision, alembic_ini_path,
                            mode)
                        alembic_command.upgrade(alembic_config, target_revision)
