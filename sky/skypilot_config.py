"""Immutable user configurations (EXPERIMENTAL).

On module import, we attempt to parse the config located at _GLOBAL_CONFIG_PATH
(default: ~/.sky/config.yaml). Caller can then use

  >> skypilot_config.loaded()

to check if the config is successfully loaded.

To read a nested-key config:

  >> skypilot_config.get_nested(('auth', 'some_auth_config'), default_value)

The config can be overridden by the configs in task YAMLs. Callers are
responsible to provide the override_configs. If the nested key is part of
OVERRIDEABLE_CONFIG_KEYS, override_configs must be provided (can be empty):

  >> skypilot_config.get_nested(('docker', 'run_options'), default_value
                        override_configs={'docker': {'run_options': 'value'}})

To set a value in the nested-key config:

  >> config_dict = skypilot_config.set_nested(('auth', 'some_key'), value)

This operation returns a deep-copy dict, and is safe in that any key not found
will not raise an error.

Example usage:

Consider the following config contents:

    a:
        nested: 1
    b: 2

then:

    # Assuming ~/.sky/config.yaml exists and can be loaded:
    skypilot_config.loaded()  # ==> True

    skypilot_config.get_nested(('a', 'nested'), None)    # ==> 1
    skypilot_config.get_nested(('a', 'nonexist'), None)  # ==> None
    skypilot_config.get_nested(('a',), None)             # ==> {'nested': 1}

    # If ~/.sky/config.yaml doesn't exist or failed to be loaded:
    skypilot_config.loaded()  # ==> False
    skypilot_config.get_nested(('a', 'nested'), None)    # ==> None
    skypilot_config.get_nested(('a', 'nonexist'), None)  # ==> None
    skypilot_config.get_nested(('a',), None)             # ==> None
"""
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
import contextlib
import contextvars
import copy
import dataclasses
import hashlib
import json
import os
import pathlib
import re
import secrets
import tempfile
import threading
import typing
from typing import Any

import filelock
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import declarative

from sky import exceptions
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import config_utils
from sky.utils import context
from sky.utils import controller_constants
from sky.utils import schemas
from sky.utils import ux_utils
from sky.utils import yaml_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils
from sky.utils.kubernetes import config_map_utils

if typing.TYPE_CHECKING:
    import yaml
else:
    yaml = adaptors_common.LazyImport('yaml')

logger = sky_logging.init_logger(__name__)

# The config is generated as described below:
#
# (*) (Used internally) If env var {ENV_VAR_SKYPILOT_CONFIG} exists, use its
#     path as the config file. Do not use any other config files.
#     This behavior is subject to change and should not be relied on by users.
# Else,
# (1) If env var {ENV_VAR_GLOBAL_CONFIG} exists, use its path as the user
#     config file. Else, use the default path {_GLOBAL_CONFIG_PATH}.
# (2) If env var {ENV_VAR_PROJECT_CONFIG} exists, use its path as the project
#     config file. Else, use the default path {_PROJECT_CONFIG_PATH}.
# (3) Override any config keys in (1) with the ones in (2).
# (4) Validate the final config.
#
# (*) is used internally to implement the behavior of the jobs controller.
#     It is not intended to be used by end users.
# (1) and (2) are used by end users to set non-default user and project config
#     files on clients.

# (Used internally) An env var holding the path to the local config file. This
# is only used by jobs controller tasks to ensure recoveries of the same job
# use the same config file.
ENV_VAR_SKYPILOT_CONFIG = f'{constants.SKYPILOT_ENV_VAR_PREFIX}CONFIG'
ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_KIND = (
    f'{constants.SKYPILOT_ENV_VAR_PREFIX}SERVER_CONFIG_SNAPSHOT_KIND')
ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_PATH = (
    f'{constants.SKYPILOT_ENV_VAR_PREFIX}SERVER_CONFIG_SNAPSHOT_PATH')
ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_DIGEST = (
    f'{constants.SKYPILOT_ENV_VAR_PREFIX}SERVER_CONFIG_SNAPSHOT_DIGEST')
ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_IDENTITY = (
    f'{constants.SKYPILOT_ENV_VAR_PREFIX}SERVER_CONFIG_SNAPSHOT_IDENTITY')

# Environment variables for setting non-default server and user
# config files.
ENV_VAR_GLOBAL_CONFIG = f'{constants.SKYPILOT_ENV_VAR_PREFIX}GLOBAL_CONFIG'
# Environment variables for setting non-default project config files.
ENV_VAR_PROJECT_CONFIG = f'{constants.SKYPILOT_ENV_VAR_PREFIX}PROJECT_CONFIG'
# Server-owned selector for the guarded-HA configuration authority.  This is
# intentionally not a generic user-facing backend switch: ``postgres`` is the
# only supported value and is emitted by the guarded-HA Helm topology.
ENV_VAR_SERVER_CONFIG_MODE = (
    f'{constants.SKYPILOT_ENV_VAR_PREFIX}SERVER_CONFIG_MODE')
SERVER_CONFIG_MODE_POSTGRES = 'postgres'
INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE = 'serve'
INTERNAL_CONFIG_SNAPSHOT_KIND_MANAGED_JOB = 'managed-job'
_INTERNAL_CONFIG_SNAPSHOT_KINDS = frozenset({
    INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE,
    INTERNAL_CONFIG_SNAPSHOT_KIND_MANAGED_JOB,
})

# Path to the client config files.
_GLOBAL_CONFIG_PATH = '~/.sky/config.yaml'
_PROJECT_CONFIG_PATH = '.sky.yaml'

API_SERVER_CONFIG_KEY = 'api_server_config'
WORKSPACE_PERMISSION_GENERATION_KEY = 'workspace_permission_generation'

_POSTGRES_SERVER_CONFIG_ADVISORY_LOCK_KEY = (
    'skypilot.guarded_ha.central_config.v1')

Base = declarative.declarative_base()

config_yaml_table = sqlalchemy.Table(
    'config_yaml',
    Base.metadata,
    sqlalchemy.Column('key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('value', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('revision', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('digest', sqlalchemy.String(64), nullable=False),
)


@dataclasses.dataclass(frozen=True)
class ServerConfigIdentity:
    """Exact optimistic-concurrency identity for one central config row."""

    revision: int
    digest: str


@dataclasses.dataclass(frozen=True)
class ServerConfigRecord:
    """One validated central config generation and its durable identity."""

    config: config_utils.Config
    identity: ServerConfigIdentity
    value: str


@dataclasses.dataclass(frozen=True)
class WorkspacePermissionGeneration:
    """Receipt fencing cached workspace-policy decisions."""

    generation: int
    config_identity: ServerConfigIdentity
    row_identity: ServerConfigIdentity


class StaleServerConfigError(RuntimeError):
    """Raised when a guarded-HA writer presents an obsolete config CAS."""


class ConfigContext:
    """Config state (loaded config, path, override flag) for one context."""

    def __init__(self,
                 config: config_utils.Config | None = None,
                 config_path: str | None = None,
                 config_overridden: bool = False,
                 server_config_identity: ServerConfigIdentity | None = None):
        # A default of config_utils.Config() would be evaluated once at
        # function definition, silently sharing one mutable Config between
        # every context created without an explicit config.
        self.config = config if config is not None else config_utils.Config()
        self.config_path = config_path
        self.config_overridden = config_overridden
        self.server_config_identity = server_config_identity


# The global loaded config.
_active_workspace_context = threading.local()
_global_config_context = ConfigContext()

SKYPILOT_CONFIG_LOCK_PATH = '~/.sky/locks/.skypilot_config.lock'
_CENTRAL_CONFIG_RELOAD_LOCK_PATH = (
    '/var/run/skypilot/.central_config_reload.lock')


def _postgres_server_config_is_authoritative() -> bool:
    """Returns whether guarded HA requires PostgreSQL-only server config."""
    mode = os.environ.get(ENV_VAR_SERVER_CONFIG_MODE)
    if mode is None:
        return False
    if mode != SERVER_CONFIG_MODE_POSTGRES:
        raise RuntimeError(f'{ENV_VAR_SERVER_CONFIG_MODE} must be exactly '
                           f'{SERVER_CONFIG_MODE_POSTGRES!r} when set.')
    return True


def get_skypilot_config_lock_path() -> str:
    """Get the path for the SkyPilot config lock file."""
    # This generic lock still fences the shared legacy Serve config snapshot
    # promote/restore/scrub protocol.  It cannot become pod-local until that
    # snapshot path is removed in D6.
    lock_path = os.path.expanduser(SKYPILOT_CONFIG_LOCK_PATH)
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    return lock_path


def get_central_config_reload_lock_path() -> str:
    """Return the pod-local lock for one process's central-config reload."""
    os.makedirs(os.path.dirname(_CENTRAL_CONFIG_RELOAD_LOCK_PATH),
                exist_ok=True)
    return _CENTRAL_CONFIG_RELOAD_LOCK_PATH


def _get_config_context() -> ConfigContext:
    """Get config context for current context.

    If no context is available, the global config context is returned.
    """
    ctx = context.get()
    if not ctx:
        return _global_config_context
    if ctx.config_context is None:
        # Config context for current context is not initialized, inherit from
        # one captured global generation.  A concurrent internal controller
        # refresh swaps this reference atomically; reading it once prevents a
        # request context from combining one generation's config with another
        # generation's path metadata.
        global_context = _global_config_context
        ctx.config_context = ConfigContext(
            config=copy.deepcopy(global_context.config),
            config_path=global_context.config_path,
            config_overridden=global_context.config_overridden,
            server_config_identity=global_context.server_config_identity,
        )
    return ctx.config_context


def _get_loaded_config() -> config_utils.Config:
    return _get_config_context().config


def _set_loaded_config(config: config_utils.Config) -> None:
    _get_config_context().config = config


def _get_loaded_config_path() -> list[str | None]:
    serialized = _get_config_context().config_path
    if not serialized:
        return []
    config_paths = json.loads(serialized)
    if config_paths is None:
        return []
    return config_paths


def _set_loaded_config_path(path: str | list[str | None] | None) -> None:
    if not path:
        # Must return: falling through re-assigned json.dumps(None), i.e. the
        # literal string 'null'. That is not "no path" to any consumer that
        # only checks for None -- it round-trips through the request body and
        # decodes back to None on the API server, where it is concatenated to
        # a list. See override_skypilot_config.
        _get_config_context().config_path = None
        return
    if isinstance(path, str):
        path = [path]
    _get_config_context().config_path = json.dumps(path)


def _set_loaded_config_path_serialized(path: str | None) -> None:
    _get_config_context().config_path = path


def _is_config_overridden() -> bool:
    return _get_config_context().config_overridden


def _set_config_overridden(config_overridden: bool) -> None:
    _get_config_context().config_overridden = config_overridden


def _replace_config_context(
        config: config_utils.Config,
        config_path: str | list[str | None] | None,
        *,
        config_overridden: bool | None = None,
        server_config_identity: (ServerConfigIdentity | None) = None) -> None:
    """Publish a complete config generation to the applicable context.

    Config reloads can run either process-wide or inside an API request's
    context.  Replacing the ``ConfigContext`` reference, rather than mutating
    its config and path fields separately, prevents concurrent readers from
    observing fields from different generations.
    """
    global _global_config_context
    current_sky_context = context.get()
    current_config_context = _global_config_context
    if (current_sky_context is not None and
            current_sky_context.config_context is not None):
        current_config_context = typing.cast(ConfigContext,
                                             current_sky_context.config_context)

    if isinstance(config_path, str):
        config_path = [config_path]
    serialized_path = json.dumps(config_path)
    if config_overridden is None:
        config_overridden = current_config_context.config_overridden
    replacement = ConfigContext(config=config,
                                config_path=serialized_path,
                                config_overridden=config_overridden,
                                server_config_identity=(server_config_identity))

    if current_sky_context is None:
        _global_config_context = replacement
    else:
        current_sky_context.config_context = replacement


def get_loaded_server_config_identity() -> ServerConfigIdentity:
    """Return the exact central-config generation loaded in this context."""
    identity = _get_config_context().server_config_identity
    if identity is None:
        raise RuntimeError('No PostgreSQL server-config identity is loaded in '
                           'this process context.')
    return identity


def get_user_config_path() -> str:
    """Returns the path to the user config file."""
    return _GLOBAL_CONFIG_PATH


def _get_config_from_path(path: str | None) -> config_utils.Config:
    if path is None:
        return config_utils.Config()
    return parse_and_validate_config_file(path)


def _redact_container_image_config_for_logging(
        config: dict[str, Any]) -> dict[str, Any]:
    """Redacts untrusted registry policy before validation or debug output."""
    redacted = copy.deepcopy(dict(config))
    if redacted.get('container_registries') is not None:
        redacted['container_registries'] = '<redacted>'
    if 'workspaces' not in redacted:
        return redacted
    workspaces = redacted['workspaces']
    if not isinstance(workspaces, dict):
        # A malformed parent can otherwise make the generic schema validator
        # reflect its complete value before the image-policy boundary is known.
        redacted['workspaces'] = '<redacted>'
        return redacted
    if any(not common_utils.is_valid_workspace_name(workspace)
           for workspace in workspaces):
        # Mapping keys are values too. A credential-shaped workspace key can
        # otherwise survive value redaction and be emitted by config logging
        # or a debug dump before semantic admission rejects it.
        redacted['workspaces'] = '<redacted>'
        return redacted
    for workspace, workspace_config in list(workspaces.items()):
        if not isinstance(workspace_config, dict):
            workspaces[workspace] = '<redacted>'
        elif 'container_images' in workspace_config:
            workspace_config['container_images'] = '<redacted>'
    return redacted


def _has_container_image_config(config: dict[str, Any]) -> bool:
    """Returns whether untrusted config contains image distribution policy."""
    if config.get('container_registries') is not None:
        return True
    if 'workspaces' not in config:
        return False
    workspaces = config['workspaces']
    if not isinstance(workspaces, dict):
        return True
    return (any(not common_utils.is_valid_workspace_name(workspace)
                for workspace in workspaces) or
            any(not isinstance(workspace_config, dict) or
                'container_images' in workspace_config
                for workspace_config in workspaces.values()))


def resolve_user_config_path() -> str | None:
    # find the user config file path, None if not resolved.
    user_config_path = _get_config_file_path(ENV_VAR_GLOBAL_CONFIG)
    if user_config_path:
        logger.debug('using user config file specified by '
                     f'{ENV_VAR_GLOBAL_CONFIG}: {user_config_path}')
        user_config_path = os.path.expanduser(user_config_path)
        if not os.path.exists(user_config_path):
            with ux_utils.print_exception_no_traceback():
                raise FileNotFoundError(
                    'Config file specified by env var '
                    f'{ENV_VAR_GLOBAL_CONFIG} ({user_config_path!r}) '
                    'does not exist. Please double check the path or unset the '
                    f'env var: unset {ENV_VAR_GLOBAL_CONFIG}')
    else:
        user_config_path = get_user_config_path()
        logger.debug(f'using default user config file: {user_config_path}')
        user_config_path = os.path.expanduser(user_config_path)
    if os.path.exists(user_config_path):
        return user_config_path
    return None


def get_user_config() -> config_utils.Config:
    """Returns the user config."""
    return _get_config_from_path(resolve_user_config_path())


def _resolve_project_config_path() -> str | None:
    # find the project config file
    project_config_path = _get_config_file_path(ENV_VAR_PROJECT_CONFIG)
    if project_config_path:
        logger.debug('using project config file specified by '
                     f'{ENV_VAR_PROJECT_CONFIG}: {project_config_path}')
        project_config_path = os.path.expanduser(project_config_path)
        if not os.path.exists(project_config_path):
            with ux_utils.print_exception_no_traceback():
                raise FileNotFoundError(
                    'Config file specified by env var '
                    f'{ENV_VAR_PROJECT_CONFIG} ({project_config_path!r}) '
                    'does not exist. Please double check the path or unset the '
                    f'env var: unset {ENV_VAR_PROJECT_CONFIG}')
    else:
        logger.debug(
            f'using default project config file: {_PROJECT_CONFIG_PATH}')
        project_config_path = _PROJECT_CONFIG_PATH
        project_config_path = os.path.expanduser(project_config_path)
    if os.path.exists(project_config_path):
        return project_config_path
    return None


def _resolve_server_config_path() -> str | None:
    # find the server config file
    server_config_path = _get_config_file_path(ENV_VAR_GLOBAL_CONFIG)
    if server_config_path:
        logger.debug('using server config file specified by '
                     f'{ENV_VAR_GLOBAL_CONFIG}: {server_config_path}')
        server_config_path = os.path.expanduser(server_config_path)
        if not os.path.exists(server_config_path):
            with ux_utils.print_exception_no_traceback():
                raise FileNotFoundError(
                    'Config file specified by env var '
                    f'{ENV_VAR_GLOBAL_CONFIG} ({server_config_path!r}) '
                    'does not exist. Please double check the path or unset the '
                    f'env var: unset {ENV_VAR_GLOBAL_CONFIG}')
    else:
        server_config_path = _GLOBAL_CONFIG_PATH
        logger.debug(f'using default server config file: {server_config_path}')
        server_config_path = os.path.expanduser(server_config_path)
    if os.path.exists(server_config_path):
        return server_config_path
    return None


def get_server_config() -> config_utils.Config:
    """Returns the server config."""
    if _postgres_server_config_is_authoritative():
        db_url = os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI)
        if db_url is None:
            raise RuntimeError(
                'Guarded HA PostgreSQL server-config authority requires '
                f'{constants.ENV_VAR_DB_CONNECTION_URI}.')
        return _overlay_db_config(config_utils.Config(), db_url)
    return _get_config_from_path(_resolve_server_config_path())


def _config_value_digest(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def internal_config_snapshot_digest(config_bytes: bytes) -> str:
    """Return the immutable content identity for an internal child snapshot."""
    return hashlib.sha256(config_bytes).hexdigest()


def internal_config_snapshot_identity(kind: str, path: str, digest: str) -> str:
    """Bind one server-issued child snapshot to kind, path, and bytes."""
    if kind not in _INTERNAL_CONFIG_SNAPSHOT_KINDS:
        raise ValueError(f'Unsupported internal config snapshot kind {kind!r}.')
    if not path:
        raise ValueError('Internal config snapshot path must be non-empty.')
    if re.fullmatch(r'[0-9a-f]{64}', digest) is None:
        raise ValueError('Invalid internal config snapshot digest.')
    identity_bytes = b'\x00'.join(
        (kind.encode('utf-8'), path.encode('utf-8'), digest.encode('ascii')))
    return hashlib.sha256(identity_bytes).hexdigest()


def internal_config_snapshot_environment(kind: str, path: str,
                                         config_bytes: bytes) -> dict[str, str]:
    """Build the exact environment receipt for a server-issued snapshot."""
    digest = internal_config_snapshot_digest(config_bytes)
    return {
        ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_KIND: kind,
        ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_PATH: path,
        ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_DIGEST: digest,
        ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_IDENTITY:
            internal_config_snapshot_identity(kind, path, digest),
    }


def _validate_identity_fields(*, key: str, revision: Any,
                              digest: Any) -> ServerConfigIdentity:
    if (not isinstance(revision, int) or isinstance(revision, bool) or
            revision < 1 or not isinstance(digest, str) or
            re.fullmatch(r'[0-9a-f]{64}', digest) is None):
        raise ValueError(f'Invalid PostgreSQL config row identity for {key!r}.')
    return ServerConfigIdentity(revision=revision, digest=digest)


def _validate_config_row_identity(*, key: str, value: Any, revision: Any,
                                  digest: Any) -> ServerConfigIdentity:
    identity = _validate_identity_fields(key=key,
                                         revision=revision,
                                         digest=digest)
    if not isinstance(value, str) or _config_value_digest(value) != digest:
        raise ValueError(f'Invalid PostgreSQL config row identity for {key!r}.')
    return identity


def _parse_server_config_record(row: Any) -> ServerConfigRecord:
    value = row.value
    identity = _validate_config_row_identity(
        key=API_SERVER_CONFIG_KEY,
        value=value,
        revision=row.revision,
        digest=row.digest,
    )
    try:
        config_dict = yaml_utils.read_yaml_str(value,
                                               reject_duplicate_keys=True)
        db_config = config_utils.Config.from_dict(config_dict)
    except (TypeError, ValueError, yaml.YAMLError):
        raise ValueError(
            'Invalid database server config YAML syntax.') from None
    db_config.pop_nested(('db',), None)
    _validate_config(db_config, '<database server config>')
    return ServerConfigRecord(config=db_config, identity=identity, value=value)


def _get_server_config_record_in_session(
        session: orm.Session,
        *,
        for_update: bool = False) -> ServerConfigRecord | None:
    statement = sqlalchemy.select(config_yaml_table).where(
        config_yaml_table.c.key == API_SERVER_CONFIG_KEY)
    if for_update:
        statement = statement.with_for_update()
    row = session.execute(statement).one_or_none()
    if row is None:
        return None
    return _parse_server_config_record(row)


def _get_server_config_record_from_db() -> ServerConfigRecord | None:
    with orm.Session(_db_manager.get_engine()) as session:
        return _get_server_config_record_in_session(session)


def _get_config_yaml_from_db(key: str) -> config_utils.Config | None:
    """Read and validate one server configuration row."""
    if key != API_SERVER_CONFIG_KEY:
        raise ValueError(f'Unsupported server config key: {key!r}.')
    record = _get_server_config_record_from_db()
    return None if record is None else record.config


def _permission_generation_value(generation: int,
                                 config_identity: ServerConfigIdentity) -> str:
    return json.dumps(
        {
            'config_digest': config_identity.digest,
            'config_revision': config_identity.revision,
            'generation': generation,
        },
        separators=(',', ':'),
        sort_keys=True)


def _parse_workspace_permission_generation(
        row: Any) -> WorkspacePermissionGeneration:
    row_identity = _validate_config_row_identity(
        key=WORKSPACE_PERMISSION_GENERATION_KEY,
        value=row.value,
        revision=row.revision,
        digest=row.digest,
    )
    try:
        value = json.loads(row.value)
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError('Invalid workspace permission generation receipt.') \
            from e
    if (not isinstance(value, dict) or
            set(value) != {'config_digest', 'config_revision', 'generation'}):
        raise ValueError('Invalid workspace permission generation receipt.')
    generation = value['generation']
    config_revision = value['config_revision']
    config_digest = value['config_digest']
    if (not isinstance(generation, int) or isinstance(generation, bool) or
            generation < 0):
        raise ValueError('Invalid workspace permission generation receipt.')
    config_identity = _validate_identity_fields(
        key='workspace permission config binding',
        revision=config_revision,
        digest=config_digest,
    )
    return WorkspacePermissionGeneration(generation=generation,
                                         config_identity=config_identity,
                                         row_identity=row_identity)


def _get_workspace_permission_generation_in_session(
        session: orm.Session,
        *,
        for_update: bool = False) -> WorkspacePermissionGeneration:
    statement = sqlalchemy.select(config_yaml_table).where(
        config_yaml_table.c.key == WORKSPACE_PERMISSION_GENERATION_KEY)
    if for_update:
        statement = statement.with_for_update()
    row = session.execute(statement).one_or_none()
    if row is None:
        raise RuntimeError('Guarded HA requires the PostgreSQL workspace '
                           'permission generation receipt.')
    return _parse_workspace_permission_generation(row)


def get_workspace_permission_generation() -> WorkspacePermissionGeneration:
    """Read the current guarded-HA workspace authorization generation."""
    if not _postgres_server_config_is_authoritative():
        raise RuntimeError('Workspace permission generations are guarded-HA '
                           'PostgreSQL state.')
    with orm.Session(_db_manager.get_engine()) as session:
        return _get_workspace_permission_generation_in_session(session)


def _overlay_db_config(server_config: config_utils.Config,
                       db_url: str) -> config_utils.Config:
    """Overlay the DB-stored api_server_config onto ``server_config``."""
    logger.debug('retrieving config from database')
    del db_url  # Connection resolved via the env var by the engine.
    db_config = _get_config_yaml_from_db(API_SERVER_CONFIG_KEY)
    if _postgres_server_config_is_authoritative():
        if db_config is None:
            # The migration job is the only writer allowed to seed the row.
            # Runtime roles must never fall back to a shared or projected file
            # after guarded HA is activated, including an accidentally
            # configured ``auto`` migration mode.
            if migration_utils.configured_migration_mode() in ('bootstrap',
                                                               'upgrade'):
                return server_config
            raise RuntimeError(
                'Guarded HA requires the PostgreSQL api_server_config row. '
                'Run the database migration job before starting runtime roles.')
        return db_config

    if len(server_config.keys()) > 1:
        raise ValueError(
            'If db config is specified, no other config is allowed')
    if db_config:
        server_config = overlay_skypilot_config(server_config, db_config)
    return server_config


def get_effective_server_config() -> config_utils.Config:
    """The LIVE server config (server file + DB overlay), snapshot-immune.

    Consolidation-mode controller processes run under a per-service
    ``SKYPILOT_CONFIG`` snapshot created at `serve up` and refreshed only by a
    successfully committed `serve update`. Their *loaded* config can therefore
    disagree with the server between a central config change and the next
    service update. Control-plane TOPOLOGY decisions (e.g. whether the serve
    load balancer runs as an external Deployment) must instead agree across
    the server and every controller in the pod: this resolves the same config
    the API server itself uses, ignoring the per-service snapshot.
    """
    db_url = os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI)
    postgres_authority = _postgres_server_config_is_authoritative()
    if postgres_authority and db_url is None:
        raise RuntimeError('Guarded HA PostgreSQL server-config authority '
                           f'requires {constants.ENV_VAR_DB_CONNECTION_URI}.')
    if postgres_authority:
        # Use the same direct PostgreSQL path as diagnostic callers of
        # get_server_config().  Neither entrypoint resolves a projected or
        # shared file in guarded HA.
        return get_server_config()

    server_config_path = _resolve_server_config_path()
    server_config = _get_config_from_path(server_config_path)
    if db_url:
        server_config = _overlay_db_config(server_config, db_url)
    return server_config


def get_nested(keys: tuple[str, ...],
               default_value: Any,
               override_configs: dict[str, Any] | None = None) -> Any:
    """Gets a nested key.

    If any key is not found, or any intermediate key does not point to a dict
    value, returns 'default_value'.

    When 'keys' is within OVERRIDEABLE_CONFIG_KEYS, 'override_configs' must be
    provided (can be empty). Otherwise, 'override_configs' must not be provided.

    Args:
        keys: A tuple of strings representing the nested keys.
        default_value: The default value to return if the key is not found.
        override_configs: A dict of override configs with the same schema as
            the config file, but only containing the keys to override.

    Returns:
        The value of the nested key, or 'default_value' if not found.
    """
    return _get_loaded_config().get_nested(
        keys,
        default_value,
        override_configs,
        allowed_override_keys=constants.OVERRIDEABLE_CONFIG_KEYS_IN_TASK,
        disallowed_override_keys=None)


def get_effective_workspace_region_config_from_snapshot(
        config_snapshot: Mapping[str, Any],
        cloud: str,
        keys: tuple[str, ...],
        region: str | None = None,
        default_value: Any | None = None,
        *,
        workspace: str | None,
        override_configs: dict[str, Any] | None = None) -> Any:
    """Resolve effective config from an explicit immutable-time snapshot.

    This has the same workspace, region, cloud, and resource-override
    precedence as :func:`get_effective_workspace_region_config`, but never
    reads the active workspace or the process-global loaded config. Passing
    ``workspace=None`` deliberately skips the workspace layer.

    Callers that need one coherent decision should capture the config and
    workspace once, then use this function for every read in that decision.
    The input mappings are not mutated.
    """
    snapshot = config_utils.Config(dict(config_snapshot))
    workspaced_config_value = None
    if workspace is not None:
        workspace_cloud_config = snapshot.get_nested(keys=(
            'workspaces',
            workspace,
        ),
                                                     default_value=None)
        if workspace_cloud_config is not None:
            workspaced_config_value = (
                config_utils.get_cloud_config_value_from_dict(
                    dict_config=workspace_cloud_config,
                    cloud=cloud,
                    keys=keys,
                    region=region,
                    default_value=None,
                    override_configs=override_configs))
    if workspaced_config_value is not None:
        return workspaced_config_value
    return config_utils.get_cloud_config_value_from_dict(
        dict_config=snapshot,
        cloud=cloud,
        keys=keys,
        region=region,
        default_value=default_value,
        override_configs=override_configs)


def get_effective_workspace_region_config(
        cloud: str,
        keys: tuple[str, ...],
        region: str | None = None,
        default_value: Any | None = None,
        workspace: str | None = None,
        override_configs: dict[str, Any] | None = None) -> Any:
    if workspace is None:
        workspace = get_active_workspace()
    config_snapshot = _get_loaded_config()
    return get_effective_workspace_region_config_from_snapshot(
        config_snapshot=config_snapshot,
        cloud=cloud,
        keys=keys,
        region=region,
        default_value=default_value,
        workspace=workspace,
        override_configs=override_configs)


def get_effective_region_config(cloud: str,
                                keys: tuple[str, ...],
                                region: str | None = None,
                                default_value: Any | None = None,
                                override_configs: dict[str, Any] | None = None,
                                merge_dicts: bool = False) -> Any:
    """Returns the nested key value by reading from config
    Order to get the property_name value:
    1. if region is specified,
       try to get the value from <cloud>/<region_key>/<region>/keys
    2. if no region or no override,
       try to get it at the cloud level <cloud>/keys
    3. if not found at cloud level,
       return either default_value if specified or None

    If merge_dicts is True and both levels return dicts, the region-level
    dict is shallow-merged into the cloud-level dict (region keys override).
    """
    return config_utils.get_cloud_config_value_from_dict(
        dict_config=_get_loaded_config(),
        cloud=cloud,
        keys=keys,
        region=region,
        default_value=default_value,
        override_configs=override_configs,
        merge_dicts=merge_dicts)


def get_workspace_cloud(cloud: str,
                        workspace: str | None = None) -> config_utils.Config:
    """Returns the workspace cloud config, deep-merged with global cloud config.

    Workspace-specific values override global values. Fields not set in the
    workspace block are inherited from the global cloud config. This allows
    workspaces to override only the fields they need (e.g., namespace)
    without losing other global settings (e.g., allowed_contexts).
    """
    if workspace is None:
        workspace = get_active_workspace()

    # Get workspace-specific cloud overrides
    workspace_clouds = get_nested(keys=(
        'workspaces',
        workspace,
    ),
                                  default_value=None)
    workspace_cloud = None
    if isinstance(workspace_clouds, dict):
        ws = workspace_clouds.get(cloud.lower())
        if isinstance(ws, dict):
            workspace_cloud = ws

    # Deep-merge workspace cloud config on top of global cloud config.
    # get_nested internally does deepcopy + _recursive_update.
    merged = _get_loaded_config().get_nested(
        keys=(cloud.lower(),),
        default_value=config_utils.Config(),
        override_configs={cloud.lower(): workspace_cloud}
        if workspace_cloud else None)
    if isinstance(merged, dict):
        return config_utils.Config(merged)
    return config_utils.Config()


@contextlib.contextmanager
def local_active_workspace_ctx(workspace: str) -> Iterator[None]:
    """Temporarily set the active workspace IN CURRENT THREAD.

    Note: having this function thread-local is error-prone, as wrapping some
    operations with this will not have the underlying threads to get the
    correct active workspace. However, we cannot make it global either, as
    backend_utils.refresh_cluster_status() will be called in multiple threads,
    and they may have different active workspaces for different threads.

    # TODO(zhwu): make this function global by default and able to be set
    # it to thread-local with an argument.

    Args:
        workspace: The workspace to set as active.

    Raises:
        RuntimeError: If called from a non-main thread.
    """
    if get_active_workspace() == workspace:
        # No change, do nothing.
        yield
        return
    # Capture whether the thread-local attribute was set, NOT the
    # resolved active_workspace string. The two differ when nothing was
    # set previously: `get_active_workspace()` falls back to the literal
    # SKYPILOT_DEFAULT_WORKSPACE ('default'), and unconditionally
    # restoring to that string would leave the attribute SET to 'default'
    # — making `is_active_workspace_set()` return True for the next
    # request handled by the same worker process, which causes the
    # workspace resolver gate to silently skip and fall back to the
    # literal 'default' (broken for users without 'default' access).
    had_workspace = hasattr(_active_workspace_context, 'workspace')
    prior_value: str | None = None
    if had_workspace:
        prior_value = _active_workspace_context.workspace
    _active_workspace_context.workspace = workspace
    logger.debug(f'Set context workspace: {workspace}')
    # try/finally is required: a caller that lets an exception escape the
    # `with` block would otherwise leak this thread-local workspace to
    # subsequent callers in the same worker process (ProcessPoolExecutor
    # reuses workers). For most callers the leak is masked because
    # `override_skypilot_config` rebinds `_active_workspace_context` to a
    # fresh `threading.local()` per request, but tightening this here is
    # the right shape for a contextmanager and removes the implicit
    # dependency on that downstream reset.
    try:
        yield
    finally:
        if had_workspace:
            logger.debug(f'Reset context workspace: {prior_value}')
            _active_workspace_context.workspace = prior_value
        else:
            logger.debug('Reset context workspace: <unset>')
            try:
                del _active_workspace_context.workspace
            except AttributeError:
                pass


def get_active_workspace(force_user_workspace: bool = False) -> str:
    context_workspace = getattr(_active_workspace_context, 'workspace', None)
    if not force_user_workspace and context_workspace is not None:
        logger.debug(f'Got context workspace: {context_workspace}')
        return context_workspace
    active_workspace = get_nested(keys=('active_workspace',),
                                  default_value=None)
    if active_workspace is None:
        logger.debug(f'No active workspace found, using default workspace: '
                     f'{constants.SKYPILOT_DEFAULT_WORKSPACE}')
        active_workspace = constants.SKYPILOT_DEFAULT_WORKSPACE
    else:
        logger.debug(f'Got active workspace: {active_workspace}')
    return active_workspace


def is_active_workspace_set() -> bool:
    """Returns True iff active_workspace was explicitly set somewhere.

    Distinguishes "user set active_workspace" from "fell back to the
    SKYPILOT_DEFAULT_WORKSPACE literal because nothing was set". The two are
    indistinguishable through `get_active_workspace()` (both return a string)
    but are different on the wire: the override config sent by the client
    omits the key entirely when unset. The server-side per-user resolver
    should only kick in for the unset case — explicit intent (including
    explicit `'default'`) is respected as-is.
    """
    context_workspace = getattr(_active_workspace_context, 'workspace', None)
    if context_workspace is not None:
        return True
    return get_nested(keys=('active_workspace',),
                      default_value=None) is not None


def set_nested(keys: tuple[str, ...], value: Any) -> dict[str, Any]:
    """Returns a deep-copied config with the nested key set to value.

    Like get_nested(), if any key is not found, this will not raise an error.
    """
    copied_dict = copy.deepcopy(_get_loaded_config())
    copied_dict.set_nested(keys, value)
    return dict(**copied_dict)


def to_dict() -> config_utils.Config:
    """Returns a deep-copied version of the current config."""
    return copy.deepcopy(_get_loaded_config())


def _get_config_file_path(envvar: str) -> str | None:
    config_path_via_env_var = os.environ.get(envvar)
    if config_path_via_env_var is not None:
        return os.path.expanduser(config_path_via_env_var)
    return None


def _validate_config(config: dict[str, Any], config_source: str) -> None:
    """Validates the config."""
    try:
        common_utils.validate_schema(
            config,
            schemas.get_config_schema(),
            f'Invalid config YAML from ({config_source}). See: '
            'https://docs.skypilot.co/en/latest/reference/config.html. '  # pylint: disable=line-too-long
            'Error: ',
            skip_none=False)
    except ValueError:
        # jsonschema includes the rejected instance in many error messages.
        # Registry policy is secret-free by contract, but it is untrusted at
        # this boundary and a malformed value must not become a log or terminal
        # exfiltration channel.
        if _has_container_image_config(config):
            raise ValueError(
                f'Invalid config YAML from ({config_source}). Container image '
                'registry configuration does not match the supported schema.'
            ) from None
        raise
    _validate_dashboard_external_links(config, config_source)
    _validate_container_image_config(config, config_source)


def _validate_container_image_config(config: dict[str, Any],
                                     config_source: str) -> None:
    """Fully validates secret-free registry models at config admission."""
    # Lazy imports avoid adding registry modules to the ordinary CLI import
    # path and avoid a cycle through sky.container_images.config.
    # pylint: disable=import-outside-toplevel
    from sky.container_images import config as image_config_lib
    from sky.container_images import models as image_models

    registries = config.get('container_registries') or {}
    try:
        bindings = image_config_lib.parse_access_bindings(
            registries.get('access_bindings'))
        profiles = image_config_lib.parse_profiles(registries.get('profiles'),
                                                   bindings)

        default_profile = registries.get('default_profile')
        if default_profile == 'direct':
            raise ValueError("'direct' cannot be the default registry profile.")
        if default_profile is not None and default_profile not in profiles:
            raise ValueError(
                f'Default registry profile {default_profile!r} is not '
                'configured.')
        for workspace, workspace_config in (config.get('workspaces') or
                                            {}).items():
            raw_policy = workspace_config.get('container_images')
            if raw_policy is None:
                continue
            image_models.validate_workspace_name(
                workspace, 'Container image policy workspace')
            policy = image_config_lib.parse_workspace_policy(raw_policy)
            selected_profiles = set(policy.allowed_profiles)
            if policy.default_profile is not None:
                selected_profiles.add(policy.default_profile)
            if 'direct' in selected_profiles:
                raise ValueError(
                    f'Workspace {workspace!r} cannot configure direct as a '
                    'default or allowed registry profile.')
            missing = sorted(selected_profiles - profiles.keys())
            if missing:
                raise ValueError(
                    f'Workspace {workspace!r} references unconfigured '
                    f'registry profiles: {missing!r}.')
    except (KeyError, TypeError, ValueError):
        raise ValueError(
            f'Invalid config YAML from ({config_source}). Container image '
            'registry configuration is invalid. Check registry profile, '
            'target, Kubernetes binding, and workspace policy fields.'
        ) from None


def _validate_dashboard_external_links(config: dict[str, Any],
                                       config_source: str) -> None:
    """Ensures every dashboard.external_links regex is compilable."""
    dashboard = config.get('dashboard') if isinstance(config, dict) else None
    if not isinstance(dashboard, dict):
        return
    external_links = dashboard.get('external_links')
    if not isinstance(external_links, list):
        return
    for idx, entry in enumerate(external_links):
        if not isinstance(entry, dict):
            continue
        regex = entry.get('regex')
        if not isinstance(regex, str):
            continue
        try:
            re.compile(regex)
        except re.error as e:
            raise ValueError(
                f'Invalid config YAML from ({config_source}). '
                f'dashboard.external_links[{idx}].regex is not a valid regex: '
                f'{regex!r} ({e}).') from e


def overlay_skypilot_config(
        original_config: config_utils.Config | None,
        override_configs: config_utils.Config | None) -> config_utils.Config:
    """Overlays the override configs on the original configs."""
    if original_config is None:
        original_config = config_utils.Config()
    config = original_config.get_nested(keys=tuple(),
                                        default_value=None,
                                        override_configs=override_configs,
                                        allowed_override_keys=None,
                                        disallowed_override_keys=None)
    return config


def safe_reload_config() -> None:
    """Reloads the config, safe to be called concurrently."""
    central_guarded_role = (_postgres_server_config_is_authoritative() and
                            os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER)
                            is not None and
                            _guarded_ha_scoped_child_snapshot() is None)
    lock_path = (get_central_config_reload_lock_path()
                 if central_guarded_role else get_skypilot_config_lock_path())
    with filelock.FileLock(lock_path):
        reload_config()


def _guarded_ha_scoped_child_snapshot() -> str | None:
    """Validate and classify a server-owned child snapshot receipt."""
    internal_config_path = os.environ.get(ENV_VAR_SKYPILOT_CONFIG)
    if internal_config_path is None:
        return None
    kind: str | None = None
    if os.environ.get(constants.IS_SKYPILOT_SERVE_CONTROLLER) == 'true':
        kind = INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE
    managed_job_identity = (
        controller_constants.MANAGED_JOB_ID_ENV_VAR,
        controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR,
        controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR,
    )
    if all(os.environ.get(name) is not None for name in managed_job_identity):
        if kind is not None:
            raise RuntimeError('A guarded child cannot be both Serve and '
                               'Managed Jobs.')
        kind = INTERNAL_CONFIG_SNAPSHOT_KIND_MANAGED_JOB
    if kind is None:
        return None

    receipt_kind = os.environ.get(ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_KIND)
    receipt_path = os.environ.get(ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_PATH)
    receipt_digest = os.environ.get(ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_DIGEST)
    receipt_identity = os.environ.get(ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_IDENTITY)
    if None in (receipt_kind, receipt_path, receipt_digest, receipt_identity):
        raise RuntimeError(
            'Guarded child config requires an exact server-issued '
            'snapshot receipt.')
    assert receipt_kind is not None
    assert receipt_path is not None
    assert receipt_digest is not None
    assert receipt_identity is not None
    if receipt_kind != kind or receipt_path != internal_config_path:
        raise RuntimeError('Guarded child config snapshot scope does not match '
                           'its server-issued receipt.')
    expected_identity = internal_config_snapshot_identity(
        receipt_kind, receipt_path, receipt_digest)
    if not secrets.compare_digest(receipt_identity, expected_identity):
        raise RuntimeError('Guarded child config snapshot identity is invalid.')
    return kind


def reload_config() -> None:
    internal_config_path = os.environ.get(ENV_VAR_SKYPILOT_CONFIG)
    is_server = (os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER)
                 is not None)
    if is_server and _postgres_server_config_is_authoritative():
        # Central API/controller/executor/image-worker roles always resolve the
        # PostgreSQL authority, even if an inherited SKYPILOT_CONFIG happens to
        # be present.  Only server-issued Serve/Managed-Jobs child snapshots
        # retain their explicitly scoped immutable-file semantics until D6.
        snapshot_kind = _guarded_ha_scoped_child_snapshot()
        if internal_config_path is not None and snapshot_kind is not None:
            _reload_config_from_guarded_child_snapshot(internal_config_path,
                                                       snapshot_kind)
        else:
            _reload_config_as_server()
        return
    if internal_config_path is not None:
        # {ENV_VAR_SKYPILOT_CONFIG} is used internally.
        # When this environment variable is set, the config loading
        # behavior is not defined in the public interface.
        # SkyPilot reserves the right to change the config loading behavior
        # at any time when this environment variable is set.
        _reload_config_from_internal_file(internal_config_path)
        return

    if is_server:
        _reload_config_as_server()
    else:
        _reload_config_as_client()


def parse_and_validate_config_bytes(
        config_bytes: bytes,
        config_source: str,
        *,
        log_config: bool = True,
        apply_db_env: bool = True) -> config_utils.Config:
    """Parse one immutable config byte string and validate it."""
    config = config_utils.Config()
    try:
        config_dict = yaml_utils.read_yaml_str(config_bytes.decode('utf-8'),
                                               reject_duplicate_keys=True)
        config = config_utils.Config.from_dict(config_dict)
        # pop the db url from the config, and set it to the env var.
        # this is to avoid db url (considered a sensitive value)
        # being printed with the rest of the config.
        db_url = config.pop_nested(('db',), None)
        if db_url and apply_db_env:
            os.environ[constants.ENV_VAR_DB_CONNECTION_URI] = db_url
        if (log_config and
                sky_logging.logging_enabled(logger, sky_logging.DEBUG)):
            safe_config = _redact_container_image_config_for_logging(config)
            logger.debug(f'Config loaded from {config_source}:\n'
                         f'{yaml_utils.dump_yaml_str(safe_config)}')
    except (UnicodeDecodeError, yaml.YAMLError):
        # read_yaml() converts parser failures to a value-free ValueError.
        # Keep this defensive branch for alternate YAML implementations.
        raise ValueError('Invalid config YAML syntax.') from None
    if config:
        _validate_config(config, config_source)

    logger.debug(f'Config syntax check passed for path: {config_source}')
    return config


def parse_and_validate_config_file(config_path: str) -> config_utils.Config:
    with open(config_path, 'rb') as config_file:
        return parse_and_validate_config_bytes(config_file.read(), config_path)


def install_internal_config_snapshot(config: config_utils.Config,
                                     config_path: str) -> None:
    """Atomically publish an already-validated internal process config.

    The caller must serialize file promotion separately. This function must be
    called without a request-local SkyPilot context so one reference swap
    updates long-lived controller threads without exposing reload_config()'s
    temporary empty state.
    """
    if context.get() is not None:
        raise RuntimeError('Internal config snapshots must be installed in '
                           'the process-global SkyPilot context.')
    expanded_path = os.path.expanduser(config_path)
    if not os.path.exists(expanded_path):
        raise FileNotFoundError(expanded_path)
    if (_postgres_server_config_is_authoritative() and
            os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is not None):
        snapshot_kind: str | None = None
        if os.environ.get(constants.IS_SKYPILOT_SERVE_CONTROLLER) == 'true':
            snapshot_kind = INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE
        managed_job_identity = (
            controller_constants.MANAGED_JOB_ID_ENV_VAR,
            controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ID_ENV_VAR,
            controller_constants.MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT_ENV_VAR,
        )
        if all(
                os.environ.get(name) is not None
                for name in managed_job_identity):
            if snapshot_kind is not None:
                raise RuntimeError('A guarded child cannot be both Serve and '
                                   'Managed Jobs.')
            snapshot_kind = INTERNAL_CONFIG_SNAPSHOT_KIND_MANAGED_JOB
        if snapshot_kind is not None:
            with open(expanded_path, 'rb') as snapshot_file:
                snapshot_bytes = snapshot_file.read()
            os.environ.update(
                internal_config_snapshot_environment(snapshot_kind,
                                                     expanded_path,
                                                     snapshot_bytes))
    os.environ[ENV_VAR_SKYPILOT_CONFIG] = expanded_path
    _replace_config_context(copy.deepcopy(config),
                            expanded_path,
                            config_overridden=False)


def _parse_dotlist(dotlist: list[str]) -> config_utils.Config:
    """Parse a single key-value pair into a dictionary.

    Args:
        dotlist: A single key-value pair.

    Returns:
        A config_utils.Config object with the parsed key-value pairs.
    """
    config: config_utils.Config = config_utils.Config()
    for arg in dotlist:
        try:
            key, value = arg.split('=', 1)
        except ValueError as e:
            raise ValueError('Invalid config override. Please use the format: '
                             'key=value') from e
        if len(key) == 0 or len(value) == 0:
            raise ValueError('Invalid config override. Please use the format: '
                             'key=value')
        value = yaml_utils.safe_load_value_free(value)
        nested_keys = tuple(key.split('.'))
        config.set_nested(nested_keys, value)
    return config


def _reload_config_from_internal_file(internal_config_path: str) -> None:
    config_path = os.path.expanduser(internal_config_path)
    if not os.path.exists(config_path):
        with ux_utils.print_exception_no_traceback():
            raise FileNotFoundError(
                'Config file specified by env var '
                f'{ENV_VAR_SKYPILOT_CONFIG} ({config_path!r}) does not '
                'exist. Please double check the path or unset the env var: '
                f'unset {ENV_VAR_SKYPILOT_CONFIG}')
    logger.debug(f'Using config path: {config_path}')
    loaded_config = parse_and_validate_config_file(config_path)
    _replace_config_context(loaded_config, config_path)


def _reload_config_from_guarded_child_snapshot(internal_config_path: str,
                                               snapshot_kind: str) -> None:
    """Read one attested child snapshot once and reject path/content drift."""
    del snapshot_kind  # Kind is bound into the receipt validated by the caller.
    config_path = os.path.expanduser(internal_config_path)
    receipt_digest = os.environ.get(ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_DIGEST)
    if receipt_digest is None:
        raise RuntimeError('Guarded child config snapshot digest is missing.')
    try:
        with open(config_path, 'rb') as snapshot_file:
            config_bytes = snapshot_file.read()
    except OSError as e:
        raise RuntimeError(
            'Guarded child config snapshot is unavailable.') from e
    actual_digest = internal_config_snapshot_digest(config_bytes)
    if not secrets.compare_digest(actual_digest, receipt_digest):
        raise RuntimeError(
            'Guarded child config snapshot content does not match '
            'its server-issued digest.')
    loaded_config = parse_and_validate_config_bytes(
        config_bytes,
        config_path,
        apply_db_env=False,
    )
    _replace_config_context(loaded_config, config_path)


def _create_table(engine: sqlalchemy.engine.Engine):
    """Initialize the config database with migrations."""
    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.SKYPILOT_CONFIG_DB_NAME,
        migration_utils.SKYPILOT_CONFIG_VERSION,
        mode=migration_utils.configured_migration_mode())


# We only store config in the DB when using Postgres,
# so no need to pass in db_name here.
_db_manager = db_utils.DatabaseManager(db_name='config',
                                       create_table_fn=_create_table)
initialize_and_get_db = _db_manager.get_engine


def _lock_postgres_server_config_transaction(session: orm.Session) -> None:
    """Acquire the one transaction-scoped guarded-HA config writer lock."""
    bind = session.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'Guarded HA server-config authority requires PostgreSQL.')
    session.execute(
        sqlalchemy.text('SELECT pg_advisory_xact_lock('
                        'hashtextextended(CAST(:lock_key AS text), 0))'),
        {'lock_key': _POSTGRES_SERVER_CONFIG_ADVISORY_LOCK_KEY})


def initialize_postgres_server_config_authority(
    transaction_hook: Callable[[orm.Session, ServerConfigRecord], Any] |
    None = None,
) -> None:
    """Seed or verify guarded HA's sole structured config authority.

    The chart disallows inline configuration when an external database is
    configured, so a fresh installation starts with an empty row and is then
    configured through the server API.  An existing row always wins, so an
    ordinary Helm upgrade cannot overwrite a live configuration.  Only an
    explicitly owned migration process may seed; verify/runtime processes are
    read-only and fail closed if the migration did not commit the row.
    """
    if not _postgres_server_config_is_authoritative():
        return
    if os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI) is None:
        raise RuntimeError('Guarded HA PostgreSQL server-config authority '
                           f'requires {constants.ENV_VAR_DB_CONNECTION_URI}.')

    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'Guarded HA server-config authority requires PostgreSQL.')

    migration_mode = migration_utils.configured_migration_mode()
    if migration_mode in ('bootstrap', 'upgrade'):
        config_str = yaml_utils.dump_yaml_str({})
        config_digest = _config_value_digest(config_str)
        insert_stmt = postgresql.insert(config_yaml_table).values(
            key=API_SERVER_CONFIG_KEY,
            value=config_str,
            revision=1,
            digest=config_digest,
        )
        insert_stmt = insert_stmt.on_conflict_do_nothing(
            index_elements=[config_yaml_table.c.key])
        with orm.Session(engine) as session, session.begin():
            _lock_postgres_server_config_transaction(session)
            session.execute(insert_stmt)
            config_record = _get_server_config_record_in_session(
                session, for_update=True)
            if config_record is None:
                raise RuntimeError(
                    'Failed to initialize PostgreSQL server config.')
            generation_value = _permission_generation_value(
                0, config_record.identity)
            generation_insert = postgresql.insert(config_yaml_table).values(
                key=WORKSPACE_PERMISSION_GENERATION_KEY,
                value=generation_value,
                revision=1,
                digest=_config_value_digest(generation_value),
            )
            generation_insert = generation_insert.on_conflict_do_nothing(
                index_elements=[config_yaml_table.c.key])
            session.execute(generation_insert)
            if transaction_hook is not None:
                transaction_hook(session, config_record)

    # Validate the retained winner, including the pre-existing-row and
    # verify-only cases.  Validation performs no DML.
    record = _get_server_config_record_from_db()
    if record is None:
        if migration_mode in ('bootstrap', 'upgrade'):
            raise RuntimeError('Failed to initialize PostgreSQL server config.')
        raise RuntimeError(
            'Guarded HA requires the PostgreSQL api_server_config row. Run '
            'the database migration job before starting runtime roles.')
    generation = get_workspace_permission_generation()
    if generation.config_identity.revision > record.identity.revision:
        raise RuntimeError('Workspace permission receipt refers to a future '
                           'PostgreSQL server config revision.')
    if (generation.config_identity.revision == record.identity.revision and
            generation.config_identity.digest != record.identity.digest):
        raise RuntimeError('Workspace permission receipt does not match the '
                           'PostgreSQL server config digest.')


def _reload_config_as_server() -> None:
    db_url = os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI)
    postgres_authority = _postgres_server_config_is_authoritative()
    server_config_identity: ServerConfigIdentity | None = None
    server_config_path: str | None
    if postgres_authority:
        if db_url is None:
            raise RuntimeError('Guarded HA PostgreSQL server-config authority '
                               'requires '
                               f'{constants.ENV_VAR_DB_CONNECTION_URI}.')
        server_config_path = '<database server config>'
        # A fresh bootstrap has no config table yet.  The migration entrypoint
        # creates it and immediately seeds the authoritative empty row.  Every
        # other role/mode reads PostgreSQL without first touching a file.
        migration_mode = migration_utils.configured_migration_mode()
        if migration_mode == 'bootstrap':
            server_config = config_utils.Config()
        else:
            record = _get_server_config_record_from_db()
            if record is None:
                if migration_mode == 'upgrade':
                    # The explicit migration owner seeds a legitimately absent
                    # schema-001 row after module imports complete. Runtime and
                    # verify modes must never take this path.
                    server_config = config_utils.Config()
                else:
                    raise RuntimeError(
                        'Guarded HA requires the PostgreSQL api_server_config '
                        'row. Run the database migration job before starting '
                        'runtime roles.')
            else:
                server_config = record.config
                server_config_identity = record.identity
    else:
        server_config_path = _resolve_server_config_path()
        server_config = _get_config_from_path(server_config_path)
        # Get the db url from the env var. _get_config_from_path should have
        # moved a db url specified in the config file to the environment.
        db_url = os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI)

    # A fresh-schema migration job must initialize global user state before
    # any companion schema creates objects in the shared PostgreSQL schema.
    # Importing ``sky`` loads this module before the migration entrypoint can
    # call global_user_state.initialize_and_get_db(), so defer the config
    # overlay only for explicit bootstrap mode.  The migration entrypoint
    # initializes this config schema immediately after global user state.
    if (not postgres_authority and db_url and
            migration_utils.configured_migration_mode() != 'bootstrap'):
        server_config = _overlay_db_config(server_config, db_url)
    if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
        safe_server_config = _redact_container_image_config_for_logging(
            server_config)
        logger.debug(f'server config: \n'
                     f'{yaml_utils.dump_yaml_str(safe_server_config)}')
    _replace_config_context(
        server_config,
        server_config_path,
        server_config_identity=server_config_identity,
    )


def _reload_config_as_client() -> None:
    overrides: list[config_utils.Config] = []
    user_config_path = resolve_user_config_path()
    user_config = _get_config_from_path(user_config_path)
    if user_config:
        overrides.append(user_config)
    project_config_path = _resolve_project_config_path()
    project_config = _get_config_from_path(project_config_path)
    if project_config:
        overrides.append(project_config)

    # layer the configs on top of each other based on priority
    overlaid_client_config: config_utils.Config = config_utils.Config()
    for override in overrides:
        overlaid_client_config = overlay_skypilot_config(
            original_config=overlaid_client_config, override_configs=override)
    if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
        safe_client_config = _redact_container_image_config_for_logging(
            overlaid_client_config)
        logger.debug(f'client config (before task and CLI overrides): \n'
                     f'{yaml_utils.dump_yaml_str(safe_client_config)}')
    _replace_config_context(overlaid_client_config,
                            [user_config_path, project_config_path])


def loaded_config_path() -> str | None:
    """Returns the path to the loaded config file, or '<overridden>' if the
    config is overridden."""
    path = [p for p in set(_get_loaded_config_path()) if p is not None]
    if len(path) == 0:
        return '<overridden>' if _is_config_overridden() else None
    if len(path) == 1:
        return path[0]

    header = 'overridden' if _is_config_overridden() else 'merged'
    path_str = ', '.join(p for p in path if p is not None)
    return f'<{header} ({path_str})>'


def loaded_config_path_serialized() -> str | None:
    """Returns the json serialized config path list"""
    return _get_config_context().config_path


# Load on import, synchronization is guaranteed by python interpreter.
reload_config()


def loaded() -> bool:
    """Returns if the user configurations are loaded."""
    return bool(_get_loaded_config())


@contextlib.contextmanager
def override_skypilot_config(
        override_configs: dict[str, Any] | None,
        override_config_path_serialized: str | None = None) -> Iterator[None]:
    """Overrides the user configurations."""
    # TODO(SKY-1215): allow admin user to extend the disallowed keys or specify
    # allowed keys.
    if not override_configs:
        # If no override configs (None or empty dict), do nothing.
        yield
        return
    original_config = _get_loaded_config()
    original_config_path = loaded_config_path_serialized()
    override_configs = config_utils.Config(override_configs)
    override_config_path: list[str | None]
    if override_config_path_serialized is None:
        override_config_path = []
    else:
        # `or []`: a serialized JSON null decodes to None, not to a list. Older
        # clients (and any request body already in flight) can carry the string
        # 'null' here, and concatenating that below raised
        # "can only concatenate list (not NoneType) to list" -- inside the
        # request executor's context manager, before the request log was even
        # opened, so every affected launch failed with no diagnosable output.
        override_config_path = json.loads(override_config_path_serialized) or []

    skipped_keys = config_utils.expand_nested_key_patterns(
        override_configs, constants.SKIPPED_CLIENT_OVERRIDE_KEYS)
    if _config_requires_managed_kueue(original_config):
        # Request config is merged into the loaded config below.  Ignoring only
        # Resources.cluster_config_overrides at placement time is insufficient:
        # a client queue would already be present in that merged config.  Once
        # an API server has any strict Kueue placement, keep all queue routing
        # server-owned so a request cannot redirect a strict workload.
        queue_override_patterns = []
        for queue_key in _QUEUE_NAME_KEYS:
            queue_override_patterns.extend([
                ('kubernetes',) + queue_key,
                ('kubernetes', 'context_configs', '*') + queue_key,
            ])
        for key in config_utils.expand_nested_key_patterns(
                override_configs, queue_override_patterns):
            if key not in skipped_keys:
                skipped_keys.append(key)
    disallowed_diff_keys = []
    for key in skipped_keys:
        if key == ('db',):
            # since db key is popped out of server config, the key is expected
            # to be different between client and server.
            continue
        value = override_configs.pop_nested(key, default_value=None)
        if (value is not None and
                value != original_config.get_nested(key, default_value=None)):
            disallowed_diff_keys.append('.'.join(key))
    # Only warn if there is a diff in disallowed override keys, as the client
    # use the same config file when connecting to a local server.
    if disallowed_diff_keys:
        logger.warning(
            f'The following keys ({json.dumps(disallowed_diff_keys)}) have '
            'different values in the client SkyPilot config with the server '
            'and will be ignored. Remove these keys to disable this warning. '
            'If you want to specify it, please modify it on server side or '
            'contact your administrator.')
    config = original_config.get_nested(keys=tuple(),
                                        default_value=None,
                                        override_configs=dict(override_configs),
                                        allowed_override_keys=None,
                                        disallowed_override_keys=skipped_keys)
    workspace = config.get_nested(
        keys=('active_workspace',),
        default_value=constants.SKYPILOT_DEFAULT_WORKSPACE)
    if (workspace != constants.SKYPILOT_DEFAULT_WORKSPACE and workspace
            not in get_nested(keys=('workspaces',), default_value={})):
        raise ValueError(f'Workspace {workspace} does not exist. '
                         'Use `sky check` to see if it is defined on the API '
                         'server and try again.')
    # Initialize the active workspace context to the workspace specified, so
    # that a new request is not affected by the previous request's workspace.
    global _active_workspace_context
    _active_workspace_context = threading.local()

    try:
        _validate_config(config, '<request override>')
        _set_config_overridden(True)
        _set_loaded_config(config)
        _set_loaded_config_path(_get_loaded_config_path() +
                                override_config_path)
        yield
    except exceptions.InvalidSkyPilotConfigError as e:
        safe_original_config = _redact_container_image_config_for_logging(
            original_config)
        safe_override_configs = _redact_container_image_config_for_logging(
            override_configs)
        with ux_utils.print_exception_no_traceback():
            raise exceptions.InvalidSkyPilotConfigError(
                'Failed to override the SkyPilot config on API '
                'server with your local SkyPilot config:\n'
                '=== SkyPilot config on API server ===\n'
                f'{yaml_utils.dump_yaml_str(safe_original_config)}\n'
                '=== Your local SkyPilot config ===\n'
                f'{yaml_utils.dump_yaml_str(safe_override_configs)}\n'
                f'Details: {e}') from e
    finally:
        _set_loaded_config(original_config)
        _set_config_overridden(False)
        _set_loaded_config_path_serialized(original_config_path)


@contextlib.contextmanager
def replace_skypilot_config(new_configs: config_utils.Config) -> Iterator[None]:
    """Replaces the global config with the new configs.

    This function is concurrent safe when it is:
    1. called in different processes;
    2. or called in a same process but with different context, refer to
       sky_utils.context for more details.
    """
    original_config = _get_loaded_config()
    original_config_path = loaded_config_path_serialized()
    original_env_var = os.environ.get(ENV_VAR_SKYPILOT_CONFIG)
    if new_configs != original_config:
        # Modify the global config of current process or context
        _set_loaded_config(new_configs)
        with tempfile.NamedTemporaryFile(delete=False,
                                         mode='w',
                                         prefix='mutated-skypilot-config-',
                                         suffix='.yaml') as temp_file:
            yaml_utils.dump_yaml(temp_file.name, dict(**new_configs))
        # Modify the env var of current process or context so that the
        # new config will be used by spawned sub-processes.
        # Note that this code modifies os.environ directly because it
        # will be hijacked to be context-aware if a context is active.
        os.environ[ENV_VAR_SKYPILOT_CONFIG] = temp_file.name
        _set_loaded_config_path(temp_file.name)
        yield
        # Restore the original config and env var.
        _set_loaded_config(original_config)
        _set_loaded_config_path_serialized(original_config_path)
        if original_env_var:
            os.environ[ENV_VAR_SKYPILOT_CONFIG] = original_env_var
        else:
            os.environ.pop(ENV_VAR_SKYPILOT_CONFIG, None)
    else:
        yield


@contextlib.contextmanager
def replace_skypilot_config_in_memory(
        new_configs: config_utils.Config) -> Iterator[None]:
    """Replace only the current in-process context, without a temp file."""
    original_config = _get_loaded_config()
    original_config_path = loaded_config_path_serialized()
    _set_loaded_config(new_configs)
    _set_loaded_config_path(None)
    try:
        yield
    finally:
        _set_loaded_config(original_config)
        _set_loaded_config_path_serialized(original_config_path)


_QUEUE_NAME_KEYS: list[tuple[str, ...]] = [
    # Order matters: `get_effective_queue_name` returns the first hit at a
    # given scope, so `quota.queue` wins over `kueue.local_queue_name` when
    # both are set.
    ('quota', 'queue'),
    ('kueue', 'local_queue_name'),
]

_KUEUE_REQUIRE_MANAGED_KEYS: list[tuple[str, ...]] = [
    ('kueue', 'require_managed'),
]

_NAMESPACE_KEYS: list[tuple[str, ...]] = [('namespace',)]


def _config_requires_managed_kueue(config: config_utils.Config) -> bool:
    """Whether any API-server Kubernetes scope enables strict Kueue."""

    def get_mapping_value(mapping: Mapping[str, Any],
                          keys: tuple[str, ...]) -> Any | None:
        value: Any = mapping
        for key in keys:
            if not isinstance(value, Mapping):
                return None
            value = value.get(key)
        return value

    def kubernetes_scope_requires(kubernetes_config: Any) -> bool:
        if not isinstance(kubernetes_config, Mapping):
            return False
        kueue_config = kubernetes_config.get('kueue', {})
        if (isinstance(kueue_config, Mapping) and
                kueue_config.get('require_managed') is True):
            return True
        # Queue selection itself opts into Kueue.  Treat it as strict even if
        # require_managed was omitted or explicitly false, otherwise forgetting
        # a second flag recreates a fail-open path.
        if any(
                bool(get_mapping_value(kubernetes_config, queue_key))
                for queue_key in _QUEUE_NAME_KEYS):
            return True
        context_configs = kubernetes_config.get('context_configs', {})
        if not isinstance(context_configs, Mapping):
            return False
        return any(
            kubernetes_scope_requires(context_config)
            for context_config in context_configs.values())

    if kubernetes_scope_requires(
            config.get_nested(('kubernetes',), default_value={})):
        return True
    workspaces = config.get_nested(('workspaces',), default_value={})
    if not isinstance(workspaces, Mapping):
        return False
    return any(
        isinstance(workspace_config, Mapping) and
        kubernetes_scope_requires(workspace_config.get('kubernetes', {}))
        for workspace_config in workspaces.values())


# Hooks invoked after a canonical config writer has committed and reloaded the
# new config in-process. Plugins use this to invalidate caches derived from the
# config (e.g. a request that memoized `get_nested(...)` for a TTL). Registered
# at server startup during single-threaded plugin loading, so no lock is needed.
_CONFIG_UPDATE_HOOKS: list[Callable[[], None]] = []


def register_config_update_hook(fn: Callable[[], None]) -> None:
    """Register a callback to be invoked when the API server config is updated.

    Called at server startup during plugin loading (single-threaded), so no
    lock is needed. The callback runs after the new config has been
    persisted and reloaded; exceptions are caught and logged so a misbehaving
    hook cannot fail the config update.
    """
    if fn not in _CONFIG_UPDATE_HOOKS:
        _CONFIG_UPDATE_HOOKS.append(fn)


def _get_effective_k8s_config_value(
        cloud: str,
        property_keys: list[tuple[str, ...]],
        region: str | None = None,
        workspace: str | None = None,
        override_configs: dict[str, Any] | None = None) -> Any | None:
    """Generic Kubernetes config-value resolver.

    Resolution precedence (most specific first):

    1. ``workspaces.<workspace>.<cloud>.context_configs.<region>.<property>``
    2. ``workspaces.<workspace>.<cloud>.<property>``
    3. ``<cloud>.context_configs.<region>.<property>``
    4. ``<cloud>.<property>``
    5. ``None`` — caller is responsible for any default.

    Within a scope, ``property_keys`` are tried in order; the first non-None
    hit wins. For single-spelling fields pass ``[('namespace',)]``; for
    multi-spelling fields pass e.g. ``[('quota', 'queue'),
    ('kueue', 'local_queue_name')]`` to express "quota.queue wins over
    kueue.local_queue_name when both are set at the same scope".
    """
    if workspace is None:
        workspace = get_active_workspace()

    # `override_configs` are cloud-level; looking up relative to a scope
    # (rather than prefixing the scope into `keys`) ensures they apply at
    # the correct depth even when the scope is a workspace subtree.
    scope_configs: list[config_utils.Config] = []
    if workspace is not None:
        ws_config = get_nested(keys=('workspaces', workspace),
                               default_value=None)
        if ws_config is not None:
            scope_configs.append(config_utils.Config(ws_config))
    scope_configs.append(config_utils.Config(_get_loaded_config()))

    for scope_config in scope_configs:
        if override_configs is not None:
            # Merge overrides once per scope so the per-key lookups below
            # don't re-run `_recursive_update` for every spelling.
            scope_config = config_utils.Config(
                scope_config.get_nested(keys=(),
                                        default_value={},
                                        override_configs=override_configs))
        if region is not None:
            for property_key in property_keys:
                value = scope_config.get_nested(
                    keys=(cloud, 'context_configs', region) + property_key,
                    default_value=None)
                if value is not None:
                    return value
        for property_key in property_keys:
            value = scope_config.get_nested(keys=(cloud,) + property_key,
                                            default_value=None)
            if value is not None:
                return value
    return None


def get_effective_queue_name(
        cloud: str,
        region: str | None = None,
        workspace: str | None = None,
        override_configs: dict[str, Any] | None = None) -> str | None:
    """Returns the effective Kueue local queue name from config.

    Supports two equivalent spellings, ``kueue.local_queue_name`` and
    ``quota.queue``. Scope precedence (workspace > global; context > cloud)
    takes priority over spelling; within the same scope, ``quota.queue``
    wins over ``kueue.local_queue_name`` when both are set.
    """
    return _get_effective_k8s_config_value(cloud=cloud,
                                           property_keys=_QUEUE_NAME_KEYS,
                                           region=region,
                                           workspace=workspace,
                                           override_configs=override_configs)


def get_effective_kueue_require_managed(
        cloud: str,
        region: str | None = None,
        workspace: str | None = None,
        override_configs: dict[str, Any] | None = None) -> bool:
    """Whether Pods at this placement must be managed by Kueue.

    The caller controls whether request-scoped overrides are eligible.  The
    Kubernetes cloud intentionally omits them so this safety boundary remains
    server-owned.  Any effective queue implies required management; the
    explicit setting is an optional assertion, not a way to downgrade a queued
    placement.
    """
    value = _get_effective_k8s_config_value(
        cloud=cloud,
        property_keys=_KUEUE_REQUIRE_MANAGED_KEYS,
        region=region,
        workspace=workspace,
        override_configs=override_configs)
    queue_name = get_effective_queue_name(cloud=cloud,
                                          region=region,
                                          workspace=workspace,
                                          override_configs=override_configs)
    return bool(value) or bool(queue_name)


def get_effective_namespace(
        cloud: str,
        region: str | None = None,
        workspace: str | None = None,
        override_configs: dict[str, Any] | None = None) -> str | None:
    """Returns the effective Kubernetes namespace from config.

    Resolution precedence, most specific first:

    1. ``workspaces.<workspace>.<cloud>.context_configs.<region>.namespace``
    2. ``workspaces.<workspace>.<cloud>.namespace``
    3. ``<cloud>.context_configs.<region>.namespace``
    4. ``<cloud>.namespace``
    5. ``None`` — caller is responsible for the kubeconfig-default fallback.
    """
    return _get_effective_k8s_config_value(cloud=cloud,
                                           property_keys=_NAMESPACE_KEYS,
                                           region=region,
                                           workspace=workspace,
                                           override_configs=override_configs)


def register_queue_name_key(key: tuple[str, ...]) -> None:
    """Register a new queue name key to be removed from the config.

    This is called during plugin loading at server startup, which is
    single-threaded, so no lock is needed.
    """
    if key not in _QUEUE_NAME_KEYS:
        _QUEUE_NAME_KEYS.append(key)


@contextlib.contextmanager
def remove_queue_name_from_config() -> Iterator[None]:
    """Disables Kueue queueing for SkyPilot system-controller launches."""
    config = to_dict()

    def update_to_none_if_set(keys: tuple[str, ...]) -> None:
        for queue_key in _QUEUE_NAME_KEYS:
            if config.get_nested(keys + queue_key, None) is not None:
                logger.debug(f'removing local queue name: setting '
                             f'{keys + queue_key} to None')
                config.set_nested(keys + queue_key, None)
        for require_key in _KUEUE_REQUIRE_MANAGED_KEYS:
            if config.get_nested(keys + require_key, None) is not None:
                logger.debug(f'disabling required Kueue management: setting '
                             f'{keys + require_key} to False')
                config.set_nested(keys + require_key, False)

    def remove_from_context_configs(keys: tuple[str, ...]) -> None:
        for context_name, _ in config.get_nested((*keys, 'context_configs'),
                                                 {}).items():
            update_to_none_if_set((*keys, 'context_configs', context_name))

    # remove from global config
    update_to_none_if_set(('kubernetes',))
    remove_from_context_configs(('kubernetes',))
    # remove from all workspaces configs
    for workspace_name, _ in config.get_nested(('workspaces',), {}).items():
        update_to_none_if_set(('workspaces', workspace_name, 'kubernetes'))
        remove_from_context_configs(
            ('workspaces', workspace_name, 'kubernetes'))
    safe_config = _redact_container_image_config_for_logging(config)
    logger.debug('config without Kueue queueing: '
                 f'{yaml_utils.dump_yaml_str(safe_config)}')
    with replace_skypilot_config(config):
        yield


def _compose_cli_config(cli_config: list[str] | None) -> config_utils.Config:
    """Composes the skypilot CLI config.
    CLI config can either be:
    - A path to a config file
    - A single key-value pair
    """

    if not cli_config:
        return config_utils.Config()

    config_source = 'CLI'
    try:
        maybe_config_path = os.path.expanduser(cli_config[0])
        if os.path.isfile(maybe_config_path):
            if len(cli_config) != 1:
                raise ValueError(
                    'Cannot use multiple --config flags with a config file.')
            config_source = maybe_config_path
            # cli_config is a path to a config file
            parsed_config = parse_and_validate_config_file(maybe_config_path)
        else:  # cli_config is a single key-value pair
            parsed_config = _parse_dotlist(cli_config)
        _validate_config(parsed_config, config_source)
    except ValueError as e:
        raise ValueError(f'Invalid config override. '
                         f'Check if the config file exists or if the dotlist '
                         f'is formatted as: key1=value1,key2=value2.\n'
                         f'Details: {e}') from e
    logger.debug('CLI overrides config syntax check passed.')

    return parsed_config


def apply_cli_config(cli_config: list[str] | None) -> dict[str, Any]:
    """Applies the CLI provided config.
    SAFETY:
    This function directly modifies the global _dict variable.
    This is considered fine in CLI context because the program will exit after
    a single CLI command is executed.
    Args:
        cli_config: A path to a config file or a comma-separated
        list of key-value pairs.
    """
    parsed_config = _compose_cli_config(cli_config)
    if sky_logging.logging_enabled(logger, sky_logging.DEBUG):
        safe_cli_config = _redact_container_image_config_for_logging(
            parsed_config)
        logger.debug(f'applying following CLI overrides: \n'
                     f'{yaml_utils.dump_yaml_str(safe_cli_config)}')
    _set_loaded_config(
        overlay_skypilot_config(original_config=_get_loaded_config(),
                                override_configs=parsed_config))
    return parsed_config


def _require_api_server_config_writer() -> None:
    # Pytest exercises repository contracts without booting the server.
    if ('PYTEST_CURRENT_TEST' not in os.environ and
            os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is None):
        raise ValueError('This function can only be called by the API Server.')


def _get_postgres_server_config_writer_engine() -> sqlalchemy.engine.Engine:
    """Return the validated guarded-HA config writer engine."""
    _require_api_server_config_writer()
    if not _postgres_server_config_is_authoritative():
        raise RuntimeError('PostgreSQL config mutation requires guarded HA.')
    if os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI) is None:
        raise RuntimeError(
            'Guarded HA PostgreSQL server-config authority requires '
            f'{constants.ENV_VAR_DB_CONNECTION_URI}.')
    engine = _db_manager.get_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'Guarded HA server-config authority requires PostgreSQL.')
    return engine


@contextlib.contextmanager
def locked_postgres_server_config_transaction(
    expected_identity: ServerConfigIdentity,
) -> Iterator[tuple[orm.Session, ServerConfigRecord]]:
    """Yield the one caller-owned, locked, exact-CAS config transaction.

    Repositories joining a config mutation must use the yielded Session and
    must not commit it or open an independent transaction.  Returning from the
    context commits; any exception rolls every participant back together.
    """
    engine = _get_postgres_server_config_writer_engine()
    with orm.Session(engine) as session, session.begin():
        _lock_postgres_server_config_transaction(session)
        current = _get_server_config_record_in_session(session, for_update=True)
        if current is None:
            raise RuntimeError(
                'Guarded HA requires the PostgreSQL api_server_config row.')
        if current.identity != expected_identity:
            raise StaleServerConfigError(
                'PostgreSQL server config changed while this update was '
                'being validated. Please retry the update.')
        yield session, current


def _run_config_update_hooks() -> None:
    for hook in list(_CONFIG_UPDATE_HOOKS):
        try:
            hook()
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Config-update hook {hook!r} raised: {e}',
                           exc_info=True)


def _reload_central_config_after_commit() -> None:
    """Reload the process-global PostgreSQL config under a pod-local lock."""

    def _reload_globally() -> None:
        with filelock.FileLock(get_central_config_reload_lock_path()):
            _reload_config_as_server()

    # A config endpoint normally runs inside a request Context.  Reloading in
    # an empty context publishes the generation to future requests and every
    # long-lived process-global reader instead of updating only that request's
    # private snapshot.
    contextvars.Context().run(_reload_globally)
    _run_config_update_hooks()


def advance_workspace_permission_generation_in_session(
        session: orm.Session, config_identity: ServerConfigIdentity) -> int:
    """Advance the workspace cache receipt in the caller's config txn."""
    receipt = _get_workspace_permission_generation_in_session(session,
                                                              for_update=True)
    generation = receipt.generation + 1
    value = _permission_generation_value(generation, config_identity)
    digest = _config_value_digest(value)
    result = session.execute(
        sqlalchemy.update(config_yaml_table).where(
            config_yaml_table.c.key == WORKSPACE_PERMISSION_GENERATION_KEY,
            config_yaml_table.c.revision == receipt.row_identity.revision,
            config_yaml_table.c.digest == receipt.row_identity.digest,
        ).values(
            value=value,
            revision=receipt.row_identity.revision + 1,
            digest=digest,
        ))
    if result.rowcount != 1:
        raise StaleServerConfigError(
            'Workspace permission generation changed during its locked '
            'transaction.')
    return generation


def mutate_postgres_server_config(
    modifier: Callable[[dict[str, Any]], None],
    *,
    expected_identity: ServerConfigIdentity,
    transaction_hook: Callable[
        [orm.Session, ServerConfigRecord, ServerConfigRecord], Any] |
    None = None,
) -> tuple[ServerConfigRecord, Any]:
    """Commit one guarded-HA config RMW under distributed lock and CAS.

    ``transaction_hook`` runs after the config CAS but before commit on the
    exact same SQLAlchemy Session.  It must neither commit nor open a second
    transaction.  A failure rolls back the config CAS and every hook write.
    """
    hook_result: Any = None
    next_record: ServerConfigRecord | None = None
    with locked_postgres_server_config_transaction(expected_identity) as (
            session, current):
        next_dict = copy.deepcopy(dict(current.config))
        modifier(next_dict)
        next_config = config_utils.Config.from_dict(next_dict)
        new_db_url = next_config.pop_nested(('db',), None)
        existing_db_url = os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI)
        if new_db_url and new_db_url != existing_db_url:
            raise ValueError('Cannot change db url while server is running')
        _validate_config(next_config, '<PostgreSQL server config update>')
        next_value = yaml_utils.dump_yaml_str(dict(next_config))
        next_identity = ServerConfigIdentity(
            revision=current.identity.revision + 1,
            digest=_config_value_digest(next_value),
        )
        update_result = session.execute(
            sqlalchemy.update(config_yaml_table).where(
                config_yaml_table.c.key == API_SERVER_CONFIG_KEY,
                config_yaml_table.c.revision == current.identity.revision,
                config_yaml_table.c.digest == current.identity.digest,
            ).values(
                value=next_value,
                revision=next_identity.revision,
                digest=next_identity.digest,
            ))
        if update_result.rowcount != 1:
            raise StaleServerConfigError(
                'PostgreSQL server config CAS lost despite the writer lock.')
        next_record = ServerConfigRecord(config=next_config,
                                         identity=next_identity,
                                         value=next_value)
        if transaction_hook is not None:
            hook_result = transaction_hook(session, current, next_record)

    assert next_record is not None
    _reload_central_config_after_commit()
    return next_record, hook_result


def update_api_server_config_no_lock(config: config_utils.Config) -> None:
    """Persists and reloads one API-server configuration update.

    Guarded HA writes PostgreSQL directly and never resolves or mirrors a
    configuration file. Other installations retain the file/ConfigMap path.

    Args:
        config: The config to save and sync.
    """

    _require_api_server_config_writer()

    if _postgres_server_config_is_authoritative():
        raise RuntimeError(
            'Guarded HA config writers must use '
            'mutate_postgres_server_config() with an exact revision/digest '
            'CAS; the legacy no-lock writer is disabled.')
    global_config_path = _resolve_server_config_path()
    if global_config_path is None:
        # Fallback to ~/.sky/config.yaml, and make sure it exists.
        global_config_path = os.path.expanduser(get_user_config_path())
        pathlib.Path(global_config_path).touch(exist_ok=True)

    db_updated = False
    if os.environ.get(constants.ENV_VAR_IS_SKYPILOT_SERVER) is not None:
        existing_db_url = os.environ.get(constants.ENV_VAR_DB_CONNECTION_URI)
        config = copy.deepcopy(config)
        new_db_url = config.pop_nested(('db',), None)
        if new_db_url and new_db_url != existing_db_url:
            raise ValueError('Cannot change db url while server is running')
        if existing_db_url:

            def _set_config_yaml_to_db(key: str, config: config_utils.Config):
                engine = _db_manager.get_engine()
                config_str = yaml_utils.dump_yaml_str(dict(config))
                config_digest = _config_value_digest(config_str)
                with orm.Session(engine) as session:
                    if (engine.dialect.name ==
                            db_utils.SQLAlchemyDialect.SQLITE.value):
                        insert_func = sqlite.insert
                    elif (engine.dialect.name ==
                          db_utils.SQLAlchemyDialect.POSTGRESQL.value):
                        insert_func = postgresql.insert
                    else:
                        raise ValueError('Unsupported database dialect')
                    insert_stmnt = insert_func(config_yaml_table).values(
                        key=key,
                        value=config_str,
                        revision=1,
                        digest=config_digest,
                    )
                    do_update_stmt = insert_stmnt.on_conflict_do_update(
                        index_elements=[config_yaml_table.c.key],
                        set_={
                            config_yaml_table.c.value: config_str,
                            config_yaml_table.c.revision:
                                config_yaml_table.c.revision + 1,
                            config_yaml_table.c.digest: config_digest,
                        })
                    session.execute(do_update_stmt)
                    session.commit()

            logger.debug('saving api_server config to db')
            _set_config_yaml_to_db(API_SERVER_CONFIG_KEY, config)
            db_updated = True

    if not db_updated:
        # save to the local file (PVC in Kubernetes, local file otherwise)
        yaml_utils.dump_yaml(global_config_path, dict(config))

        if config_map_utils.is_running_in_kubernetes():
            # In Kubernetes, sync the PVC config to ConfigMap for user
            # convenience.
            # PVC file is the source of truth, ConfigMap is just a mirror for
            # easy access.
            config_map_utils.patch_configmap_with_config(
                config, global_config_path)

    reload_config()
    _run_config_update_hooks()
