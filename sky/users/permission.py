"""Permission service for SkyPilot API Server."""
from collections.abc import Generator
from collections.abc import Mapping
import contextlib
import hashlib
import logging
import os
import threading
import time
from typing import Optional

import casbin
from casbin import util as casbin_util
import filelock
import sqlalchemy
import sqlalchemy_adapter

from sky import global_user_state
from sky import models
from sky import sky_logging
from sky import skypilot_config
from sky.skylet import constants
from sky.users import rbac
from sky.utils import common
from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import kv_cache

logging.getLogger('casbin.policy').setLevel(sky_logging.ERROR)
logging.getLogger('casbin.role').setLevel(sky_logging.ERROR)
logging.getLogger('casbin.model').setLevel(sky_logging.ERROR)
logging.getLogger('casbin.rbac').setLevel(sky_logging.ERROR)
logger = sky_logging.init_logger(__name__)

# Filelocks for the policy update.
POLICY_UPDATE_LOCK_PATH = os.path.expanduser('~/.sky/.policy_update.lock')
POLICY_UPDATE_LOCK_TIMEOUT_SECONDS = 20

_enforcer_instance: Optional['PermissionService'] = None

# KV cache constants for workspace permission checks.
_WORKSPACE_PERM_CACHE_PREFIX = 'perm:ws:'
_WORKSPACE_PERM_CACHE_KEY_SEP = ':'
# Long TTL as safety net; primary freshness is explicit invalidation on update.
_WORKSPACE_PERM_CACHE_TTL_SECONDS = 60 * 60  # 1h


class PermissionService:
    """Permission service for SkyPilot API Server."""

    def __init__(self):
        self.enforcer: casbin.SyncedEnforcer | None = None
        self._lock = threading.Lock()
        self._workspace_generation_lock = threading.Lock()
        self._observed_workspace_permission_generation: int | None = None
        # Viewer role's endpoint allowlist, materialised at boot.
        self._viewer_allowlist: list[tuple] = []

    def initialize(self):
        self._lazy_initialize(full_initialize=True)

    def _lazy_initialize(self, full_initialize: bool = False):
        if self.enforcer is not None:
            return
        with self._lock:
            if self.enforcer is not None:
                return
            global _enforcer_instance
            if _enforcer_instance is None:
                engine = global_user_state.initialize_and_get_db()
                if full_initialize:
                    db_utils.add_all_tables_to_db_sqlalchemy(
                        sqlalchemy_adapter.Base.metadata, engine)
                adapter = sqlalchemy_adapter.Adapter(
                    engine, db_class=sqlalchemy_adapter.CasbinRule)
                model_path = os.path.join(os.path.dirname(__file__),
                                          'model.conf')
                # Use SyncedEnforcer for thread safety. It uses a
                # read-write lock internally: concurrent reads (enforce,
                # get_roles_for_user) take a shared read lock, while
                # writes (load_policy, add_policy) take an exclusive
                # write lock. This prevents the RuntimeError from
                # concurrent iteration/mutation of RoleManager.all_roles.
                enforcer = casbin.SyncedEnforcer(model_path, adapter)
                self.enforcer = enforcer
                # Only set the enforcer instance once the enforcer
                # is successfully initialized, if we change it and then fail
                # we will set it to None and all subsequent calls will fail.
                _enforcer_instance = self
                if full_initialize:
                    with _policy_lock():
                        self._maybe_initialize_policies()
                        self._maybe_initialize_basic_auth_user()
            else:
                assert _enforcer_instance is not None
                self.enforcer = _enforcer_instance.enforcer
            # The viewer allowlist is in-process state (not stored in
            # casbin). It MUST be populated in every process that handles
            # requests.
            self._build_viewer_allowlist_no_lock()
            if skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
                # Workspace rules deliberately do not participate in the
                # legacy file-locked initializer above. Reconcile their exact
                # set under the central PostgreSQL config transaction, then
                # load and attest that committed generation in this process.
                self._synchronize_guarded_workspace_policies()

    def _ensure_enforcer(self) -> casbin.SyncedEnforcer:
        """Ensure enforcer is initialized and return it."""
        self._lazy_initialize()
        assert self.enforcer is not None, (
            'Enforcer should be initialized after _lazy_initialize()')
        return self.enforcer

    def _get_plugin_rbac_rules(self):
        """Get RBAC rules from loaded plugins.

        Returns:
            Dictionary of plugin RBAC rules, or empty dict if plugins module
            is not available or no rules are defined.
        """
        try:
            # pylint: disable=import-outside-toplevel
            from sky.server import plugins as server_plugins
            return server_plugins.get_plugin_rbac_rules()
        except ImportError:
            # Plugin module not available (e.g., not running as server)
            logger.debug(
                'Plugin module not available, skipping plugin RBAC rules')
            return {}
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to get plugin RBAC rules: {e}')
            return {}

    def _get_plugin_viewer_allowlist(self) -> list[dict]:
        """Get viewer-allowlist entries from loaded plugins.

        Lazily populates the module-level plugin allowlist cache if
        it's empty — this matters in uvicorn worker processes which
        re-import `sky.server.plugins` from scratch and would otherwise
        see an empty cache (only the main server process calls
        `load_plugin_viewer_allowlist()` at startup).

        Returns:
            List of `{path, method}` records, or empty list if plugins
            module is not available or no rules are defined.
        """
        try:
            # pylint: disable=import-outside-toplevel
            from sky.server import plugins as server_plugins
            cached = server_plugins.get_plugin_viewer_allowlist()
            if cached:
                return cached
            # Cache empty — could be either "no plugin entries" or
            # "loader hasn't run in this process". Try to populate it;
            # `load_plugin_viewer_allowlist` is side-effect-free
            # (instantiates each plugin but doesn't call install) and
            # idempotent.
            try:
                return server_plugins.load_plugin_viewer_allowlist()
            except AttributeError:
                return cached
        except ImportError:
            logger.debug('Plugin module not available, '
                         'skipping plugin viewer allowlist')
            return []
        except AttributeError:
            # Old plugin module that doesn't export this loader.
            logger.debug('Plugin module does not expose '
                         'get_plugin_viewer_allowlist; skipping')
            return []
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to get plugin viewer allowlist: {e}')
            return []

    def _build_viewer_allowlist_no_lock(self) -> None:
        """Build `self._viewer_allowlist` from defaults + plugin entries.

        Read-only with respect to casbin/DB state — no policy lock
        required. Safe to call from any process (main server or uvicorn
        worker); the result is per-process in-memory state.
        """
        plugin_viewer_allow = self._get_plugin_viewer_allowlist()
        self._viewer_allowlist = [(rule['path'], rule['method'])
                                  for rule in rbac.get_viewer_allowlist(
                                      plugin_allowlist=plugin_viewer_allow)]
        logger.debug(f'Viewer allowlist has {len(self._viewer_allowlist)} '
                     'entries')

    def _maybe_initialize_basic_auth_user(self) -> None:
        """Initialize basic auth user if it is enabled."""
        basic_auth = os.environ.get(constants.SKYPILOT_INITIAL_BASIC_AUTH)
        if not basic_auth:
            return
        username, password = basic_auth.split(':', 1)
        if username and password:
            # MD5 only derives a stable user id from the (non-secret)
            # username; the password is checked separately. Not a security use.
            user_hash = hashlib.md5(username.encode(), usedforsecurity=False
                                   ).hexdigest()[:common_utils.USER_HASH_LENGTH]
            user_info = global_user_state.get_user(user_hash)
            if user_info:
                logger.debug(f'Basic auth user {username} already exists')
                return
            global_user_state.add_or_update_user(
                models.User(id=user_hash,
                            name=username,
                            password=password,
                            user_type=models.UserType.BASIC.value))
            enforcer = self._ensure_enforcer()
            enforcer.add_grouping_policy(user_hash, rbac.RoleName.ADMIN.value)
            if not skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
                enforcer.save_policy()
            logger.info(f'Basic auth user {username} initialized')

    def _maybe_initialize_policies(self) -> None:
        """Initialize policies if they don't already exist."""
        logger.debug(f'Initializing policies in process: {os.getpid()}')

        policy_updated = False

        # Check if policies are already initialized by looking for existing
        # permission policies in the enforcer
        enforcer = self._ensure_enforcer()
        # Convert existing policies to set of tuples for O(1) lookups
        existing_policies = {tuple(p) for p in enforcer.get_policy()}

        # Get plugin RBAC rules dynamically
        plugin_rules = self._get_plugin_rbac_rules()

        # Viewer allowlist is built in `_lazy_initialize` (called above
        # via `_ensure_enforcer`) so worker processes that never reach
        # this method still get it. Operator-config changes to
        # `rbac.roles.viewer.permissions.allowlist` still require a
        # server restart — same semantics as the existing blocklist
        # for the user role.

        # If we already have policies for the expected roles, skip
        # initialization
        role_permissions = rbac.get_role_permissions(plugin_rules=plugin_rules)
        expected_policies = []
        for role, permissions in role_permissions.items():
            if permissions.get('permissions'
                              ) and 'blocklist' in permissions['permissions']:
                blocklist = permissions['permissions']['blocklist']
                for item in blocklist:
                    expected_policies.append(
                        (role, item['path'], item['method']))

        # Guarded-HA workspace policies have their own PostgreSQL transaction
        # below. They must never enter this file-locked/global-save path, since
        # save_policy() can replace unrelated rows from a stale in-memory view.
        guarded_ha = skypilot_config._postgres_server_config_is_authoritative()  # pylint: disable=protected-access
        workspace_policy_permissions = rbac.get_workspace_policy_permissions()
        logger.debug(f'Workspace policy permissions from config: '
                     f'{workspace_policy_permissions}')

        if not guarded_ha:
            for workspace_name, users in workspace_policy_permissions.items():
                for user in users:
                    expected_policies.append((user, workspace_name, '*'))
        # Check if all expected policies already exist and find missing ones
        missing_policies = [
            p for p in expected_policies if p not in existing_policies
        ]
        # Find policies to remove
        expected_policies_set = set(expected_policies)
        redundant_policies = [
            p for p in existing_policies
            if p not in expected_policies_set and not (guarded_ha and len(
                p) >= 3 and p[2] == '*' and not str(p[1]).startswith('/'))
        ]
        if missing_policies:
            # Add missing policies
            logger.debug(f'Found {len(missing_policies)} missing policies, '
                         'initializing...')
            for p in missing_policies:
                logger.debug(f'Adding policy: {p}')
                enforcer.add_policy(*p)
                policy_updated = True
            logger.debug('Missing policies added successfully')

        if redundant_policies:
            # Remove redundant policies
            logger.debug(f'Found {len(redundant_policies)} redundant policies, '
                         'cleaning up...')
            for p in redundant_policies:
                logger.debug(f'Removing policy: {p}')
                enforcer.remove_policy(*p)
                policy_updated = True
            logger.debug('Redundant policies removed successfully')

        if not missing_policies and not redundant_policies:
            logger.debug('Policies already in sync, skipping initialization')

        # Built-in identities have fixed roles.  Define them before assigning
        # defaults so a system identity already present in the users table can
        # never be initialized as an ordinary user.
        system_users = [
            (common.SERVER_ID, rbac.RoleName.ADMIN.value),
            (constants.SKYPILOT_SYSTEM_USER_ID, rbac.RoleName.ADMIN.value),
            (constants.SKYPILOT_SERVE_CONTROLLER_SYSTEM_USER_ID,
             rbac.RoleName.ADMIN.value),
            (constants.SKYPILOT_SYSTEM_VIEWER_USER_ID,
             rbac.RoleName.VIEWER.value),
        ]
        system_user_ids = {user_id for user_id, _ in system_users}

        # Always ensure ordinary users have default roles (this is idempotent).
        # Get users who already have roles (g policies) to avoid redundant calls.
        roles_by_user: dict[str, list[str]] = {}
        for grouping in enforcer.get_grouping_policy():
            if len(grouping) < 2:
                continue
            roles_by_user.setdefault(str(grouping[0]),
                                     []).append(str(grouping[1]))
        users_with_roles = set(roles_by_user)
        all_users = global_user_state.get_all_users()
        for existing_user in all_users:
            if str(existing_user.id) in system_user_ids:
                continue
            if str(existing_user.id) not in users_with_roles:
                logger.debug(f'Adding role for user: {existing_user.name}'
                             f'({existing_user.id})')
                user_added = self._add_user_if_not_exists_no_lock(
                    existing_user.id)
                policy_updated = policy_updated or user_added
        system_permission_cache_invalidations = []
        for system_user_id, system_user_role in system_users:
            global_user_state.add_or_update_user(
                models.User(id=system_user_id,
                            name=system_user_id,
                            user_type=models.UserType.SYSTEM.value))
            current_roles = roles_by_user.get(system_user_id, [])
            if current_roles == [system_user_role]:
                continue
            logger.debug(f'Enforcing role for system user: {system_user_id} '
                         f'({system_user_role})')
            for current_role in current_roles:
                enforcer.remove_grouping_policy(system_user_id, current_role)
            enforcer.add_grouping_policy(system_user_id, system_user_role)
            system_permission_cache_invalidations.append(system_user_id)
            policy_updated = True
        if policy_updated and not guarded_ha:
            enforcer.save_policy()
        for system_user_id in system_permission_cache_invalidations:
            self.invalidate_user_permission_cache(system_user_id)

    def add_user_if_not_exists(self, user_id: str) -> None:
        """Add user role relationship."""
        self._lazy_initialize()
        with _policy_lock():
            self._add_user_if_not_exists_no_lock(user_id)

    def _add_user_if_not_exists_no_lock(self,
                                        user_id: str,
                                        role: str | None = None) -> bool:
        """Add user role relationship without lock.

        Returns:
            True if the user was added, False otherwise.
        """
        enforcer = self._ensure_enforcer()
        user_roles = enforcer.get_roles_for_user(user_id)
        if not user_roles:
            enforcer.add_grouping_policy(user_id, role or
                                         rbac.get_default_role())
            return True
        return False

    def delete_user(self, user_id: str) -> None:
        """Delete user role relationship."""
        with _policy_lock():
            self._load_policy_no_lock()
            enforcer = self._ensure_enforcer()
            current_roles = enforcer.get_roles_for_user(user_id)
            if not current_roles:
                logger.debug(f'User {user_id} has no roles')
                return
            enforcer.remove_grouping_policy(user_id, current_roles[0])
            if not skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
                enforcer.save_policy()
            self.invalidate_user_permission_cache(user_id)

    def update_role(self, user_id: str, new_role: str) -> None:
        """Update user role relationship."""
        with _policy_lock():
            self._load_policy_no_lock()
            enforcer = self._ensure_enforcer()
            current_roles = enforcer.get_roles_for_user(user_id)
            if not current_roles:
                logger.debug(f'User {user_id} has no roles')
            else:
                # TODO(hailong): how to handle multiple roles?
                current_role = current_roles[0]
                if current_role == new_role:
                    logger.debug(f'User {user_id} already has role {new_role}')
                    return
                enforcer.remove_grouping_policy(user_id, current_role)

            # Update user role
            enforcer.add_grouping_policy(user_id, new_role)
            if not skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
                enforcer.save_policy()
            # Always invalidate: even a first role assignment can grant
            # workspace access that was previously denied and cached.
            self.invalidate_user_permission_cache(user_id)

    def get_user_roles(self, user_id: str) -> list[str]:
        """Get all roles for a user.

        This method returns all roles that the user has, including inherited
        roles. For example, if a user has role 'admin' and 'admin' inherits
        from 'user', this method will return ['admin', 'user'].

        Args:
            user: The user ID to get roles for.

        Returns:
            A list of role names that the user has.
        """
        self._load_policy_no_lock()
        enforcer = self._ensure_enforcer()
        return enforcer.get_roles_for_user(user_id)

    def get_users_for_role(self, role: str) -> list[str]:
        """Get all users for a role."""
        self._load_policy_no_lock()
        enforcer = self._ensure_enforcer()
        return enforcer.get_users_for_role(role)

    def get_accessible_workspace_names(
            self,
            user_id: str,
            workspace_names: set[str],
            *,
            roles: list[str] | None = None) -> set[str]:
        """Return workspace names the user can access (batch, O(1) enforcer).

        Use instead of check_workspace_permission in a loop when filtering
        many workspaces, to avoid N enforcer calls.
        """
        if os.getenv(constants.ENV_VAR_IS_SKYPILOT_SERVER) is None:
            return workspace_names
        self._ensure_workspace_permission_generation_current()
        enforcer = self._ensure_enforcer()
        if roles is None:
            if skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
                # Role mutations remain on the legacy D6 path and do not
                # advance the workspace generation yet. Preserve their prior
                # read freshness with one generation-bracketed policy load.
                self._ensure_workspace_permission_generation_current(
                    force_reload=True)
                roles = enforcer.get_roles_for_user(user_id)
            else:
                roles = self.get_user_roles(user_id)
        if rbac.RoleName.ADMIN.value in roles:
            return workspace_names
        # Scan policy rules directly for workspace access.
        # NOTE: this only matches direct (user_id, workspace, '*') and wildcard
        # ('*', workspace, '*') policies.  It does NOT traverse casbin role
        # hierarchies (the g() function in the model matcher).  If role-based
        # workspace grants are ever added, this method must be updated to use
        # enforcer.enforce() per workspace or expand roles via
        # enforcer.get_implicit_permissions_for_user().
        accessible = set()
        for rule in enforcer.get_policy():
            if len(rule) >= 3 and rule[2] == '*' and (rule[0] == user_id or
                                                      rule[0] == '*'):
                if rule[1] in workspace_names:
                    accessible.add(rule[1])
        return accessible

    def check_endpoint_permission(self, user_id: str, path: str,
                                  method: str) -> bool:
        """Check permission.

        Return True to BLOCK the request (RBAC middleware turns truthy
        return into 403). Return False to allow.

        Admin / user roles use the Casbin blocklist semantics:
        True iff a `(role, path, method)` policy matches.

        Viewer role uses an in-memory allowlist:
        True (block) unless the (path, method) matches an entry in
        `self._viewer_allowlist`.
        """
        # We intentionally don't load the policy here, as it is a hot path, and
        # we don't support updating the policy.
        # We don't hold the lock for checking permission, as it is read only and
        # it is a hot path in every request. It is ok to have a stale policy,
        # as long as it is eventually consistent.
        # self._load_policy_no_lock()
        enforcer = self._ensure_enforcer()
        # Read roles from in-memory enforcer state. Do NOT use
        # self.get_user_roles(...) here — that does a DB roundtrip via
        # _load_policy_no_lock and would put a query on the request hot
        # path.
        roles = enforcer.get_roles_for_user(user_id)
        # Admin wins over viewer when a user holds both — viewer's
        # default-deny semantics shouldn't restrict an admin.
        if (rbac.RoleName.VIEWER.value in roles and
                rbac.RoleName.ADMIN.value not in roles):
            return not self._is_viewer_allowed(path, method)
        return enforcer.enforce(user_id, path, method)

    def _is_viewer_allowed(self, path: str, method: str) -> bool:
        """Test (path, method) against the viewer allowlist."""
        for allow_path, allow_method in self._viewer_allowlist:
            if allow_method != method:
                continue
            # casbin_util.key_match2: arg1 is the request key, arg2 is
            # the policy pattern. Pattern supports `:name` placeholders
            # and `*` wildcards.
            if casbin_util.key_match2(path, allow_path):
                return True
        return False

    def _load_policy_no_lock(self):
        """Load policy from storage."""
        enforcer = self._ensure_enforcer()
        enforcer.load_policy()

    def load_policy(self):
        """Load policy from storage with lock."""
        with _policy_lock():
            self._load_policy_no_lock()

    def _ensure_workspace_permission_generation_current(
            self, *, force_reload: bool = False) -> int | None:
        """Reload Casbin before using a newer guarded-HA policy generation."""
        if not skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
            return None
        receipt = skypilot_config.get_workspace_permission_generation()
        self._ensure_config_covers_workspace_receipt(receipt)
        observed = self._observed_workspace_permission_generation
        if observed == receipt.generation and not force_reload:
            return receipt.generation
        with self._workspace_generation_lock:
            # Another request in this process may have completed the reload.
            receipt = skypilot_config.get_workspace_permission_generation()
            self._ensure_config_covers_workspace_receipt(receipt)
            observed = self._observed_workspace_permission_generation
            if observed == receipt.generation and not force_reload:
                return receipt.generation
            if observed is not None and receipt.generation < observed:
                raise RuntimeError(
                    'Workspace permission generation regressed; refusing a '
                    'potentially stale authorization decision.')

            # Bracket the adapter's policy read with generation reads.  If a
            # writer commits between them, retry until the policy snapshot and
            # receipt are from one stable committed generation.
            while True:
                before = receipt.generation
                self._load_policy_no_lock()
                after_receipt = (
                    skypilot_config.get_workspace_permission_generation())
                self._ensure_config_covers_workspace_receipt(after_receipt)
                if after_receipt.generation == before:
                    self._observed_workspace_permission_generation = before
                    return before
                if after_receipt.generation < before:
                    raise RuntimeError(
                        'Workspace permission generation regressed during '
                        'policy reload.')
                receipt = after_receipt

    def _ensure_config_covers_workspace_receipt(
        self,
        receipt: skypilot_config.WorkspacePermissionGeneration,
    ) -> None:
        """Fail closed unless this context covers the receipt's config CAS."""
        loaded = skypilot_config.get_loaded_server_config_identity()
        if loaded.revision < receipt.config_identity.revision:
            skypilot_config.safe_reload_config()
            loaded = skypilot_config.get_loaded_server_config_identity()
        if loaded.revision < receipt.config_identity.revision:
            raise RuntimeError('Loaded server config predates the workspace '
                               'permission generation; refusing a stale '
                               'authorization decision.')
        if (loaded.revision == receipt.config_identity.revision and
                loaded.digest != receipt.config_identity.digest):
            raise RuntimeError('Workspace permission generation is bound to a '
                               'different server-config digest.')

    @staticmethod
    def _workspace_policy_predicate(casbin_rule):
        """Return the exact SQL predicate for workspace Casbin rules."""
        return sqlalchemy.and_(
            casbin_rule.c.ptype == 'p',
            casbin_rule.c.v2 == '*',
            casbin_rule.c.v1.not_like('/%'),
        )

    def _synchronize_guarded_workspace_policies(self) -> None:
        """Normalize the full workspace-rule set under the config lock."""
        generation: int | None = None
        while True:
            expected_identity = (
                skypilot_config.get_loaded_server_config_identity())
            policies = rbac.get_workspace_policy_permissions()
            try:
                with skypilot_config.locked_postgres_server_config_transaction(
                        expected_identity) as (session, current):
                    # The exact CAS guarantees ``policies`` came from this row.
                    generation = self.replace_all_workspace_policies_in_session(
                        session, policies, current.identity)
                break
            except skypilot_config.StaleServerConfigError:
                skypilot_config.safe_reload_config()
        assert generation is not None
        self.reload_workspace_policy_after_commit(generation)

    def replace_all_workspace_policies_in_session(
            self, session, policies: Mapping[str, list[str]],
            config_identity: skypilot_config.ServerConfigIdentity) -> int:
        """Replace every workspace rule, if needed, in the caller's txn."""
        if not skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
            raise RuntimeError(
                'Transactional workspace policies require guarded HA.')
        bind = session.get_bind()
        if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RuntimeError(
                'Transactional workspace policies require PostgreSQL.')
        casbin_rule = sqlalchemy_adapter.CasbinRule.__table__
        predicate = self._workspace_policy_predicate(casbin_rule)
        existing = {(str(row.v0), str(row.v1), str(row.v2))
                    for row in session.execute(
                        sqlalchemy.select(casbin_rule.c.v0, casbin_rule.c.v1,
                                          casbin_rule.c.v2).where(predicate))}
        desired = {(user, workspace_name, '*')
                   for workspace_name, users in policies.items()
                   for user in set(users)}
        if existing == desired:
            return (
                skypilot_config._get_workspace_permission_generation_in_session(
                    session)  # pylint: disable=protected-access
                .generation)
        session.execute(sqlalchemy.delete(casbin_rule).where(predicate))
        for user, workspace_name, action in sorted(desired):
            session.execute(
                sqlalchemy.insert(casbin_rule).values(ptype='p',
                                                      v0=user,
                                                      v1=workspace_name,
                                                      v2=action))
        return skypilot_config.advance_workspace_permission_generation_in_session(
            session, config_identity)

    def replace_workspace_policies_in_session(
            self, session, policies: Mapping[str, list[str] | None],
            config_identity: skypilot_config.ServerConfigIdentity) -> int:
        """Replace exact workspace rules in the caller's guarded-HA txn."""
        if not skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
            raise RuntimeError(
                'Transactional workspace policies require guarded HA.')
        bind = session.get_bind()
        if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RuntimeError(
                'Transactional workspace policies require PostgreSQL.')
        casbin_rule = sqlalchemy_adapter.CasbinRule.__table__
        for workspace_name, users in sorted(policies.items()):
            session.execute(
                sqlalchemy.delete(casbin_rule).where(
                    casbin_rule.c.ptype == 'p',
                    casbin_rule.c.v1 == workspace_name,
                    casbin_rule.c.v2 == '*',
                ))
            if users is None:
                continue
            for user in sorted(set(users)):
                session.execute(
                    sqlalchemy.insert(casbin_rule).values(
                        ptype='p',
                        v0=user,
                        v1=workspace_name,
                        v2='*',
                    ))
        return skypilot_config.advance_workspace_permission_generation_in_session(
            session, config_identity)

    def reload_workspace_policy_after_commit(self,
                                             expected_generation: int) -> None:
        """Publish one committed workspace policy to this process."""
        retry = False
        with self._workspace_generation_lock:
            receipt = skypilot_config.get_workspace_permission_generation()
            if receipt.generation < expected_generation:
                raise RuntimeError(
                    'Committed workspace permission generation is not '
                    'visible after config commit.')
            self._load_policy_no_lock()
            confirmed = skypilot_config.get_workspace_permission_generation()
            if confirmed.generation != receipt.generation:
                # A later writer committed during reload.  The ordinary reader
                # path will loop and load that newer exact generation now.
                self._observed_workspace_permission_generation = None
                retry = True
            else:
                self._observed_workspace_permission_generation = (
                    confirmed.generation)
        if retry:
            self._ensure_workspace_permission_generation_current()

    def _workspace_perm_cache_key(self,
                                  workspace_name: str,
                                  user_id: str,
                                  generation: int | None = None) -> str:
        """Build a KV cache key for a workspace permission entry."""
        generation_component = ('' if generation is None else
                                f'{generation}{_WORKSPACE_PERM_CACHE_KEY_SEP}')
        return (f'{_WORKSPACE_PERM_CACHE_PREFIX}'
                f'{generation_component}'
                f'{workspace_name}'
                f'{_WORKSPACE_PERM_CACHE_KEY_SEP}'
                f'{user_id}')

    def invalidate_workspace_permission_cache(self,
                                              workspace_name: str) -> None:
        """Invalidate all cached permission entries for a workspace."""
        prefix = (f'{_WORKSPACE_PERM_CACHE_PREFIX}'
                  f'{workspace_name}'
                  f'{_WORKSPACE_PERM_CACHE_KEY_SEP}')
        kv_cache.delete_cache_entries_by_prefix(prefix)

    def invalidate_user_permission_cache(self, user_id: str) -> None:
        """Invalidate all cached permission entries for a user."""
        kv_cache.delete_cache_entries_by_prefix_suffix(
            prefix=_WORKSPACE_PERM_CACHE_PREFIX,
            suffix=f'{_WORKSPACE_PERM_CACHE_KEY_SEP}{user_id}')

    def check_workspace_permission(self, user_id: str,
                                   workspace_name: str) -> bool:
        """Check workspace permission.

        This method checks if a user has permission to access a specific
        workspace.  Results are cached in a DB-backed KV cache so that all
        server/executor processes share the same view.

        For private workspaces, the user must have explicit permission.

        For public workspaces, the permission is granted via a wildcard policy
        ('*').
        """
        if os.getenv(constants.ENV_VAR_IS_SKYPILOT_SERVER) is None:
            # When it is not on API server, we allow all users to access all
            # workspaces, as the workspace check has been done on API server.
            return True

        # Check DB-backed KV cache (covers both admin and non-admin results).
        generation = self._ensure_workspace_permission_generation_current()
        cache_key = self._workspace_perm_cache_key(workspace_name, user_id,
                                                   generation)
        cached = kv_cache.get_cache_entry(cache_key)
        if cached is not None:
            return cached == '1'

        # Cache miss — compute the permission.
        # Admin users have access to all workspaces.
        enforcer = self._ensure_enforcer()
        if skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
            # Non-workspace role mutations remain on the legacy D6 path and do
            # not advance the workspace receipt. Force one bracketed load on a
            # cache miss so those role changes remain visible without allowing
            # a workspace-policy generation to cross the load.
            refreshed_generation = (
                self._ensure_workspace_permission_generation_current(
                    force_reload=True))
            if refreshed_generation != generation:
                generation = refreshed_generation
                cache_key = self._workspace_perm_cache_key(
                    workspace_name, user_id, generation)
                cached = kv_cache.get_cache_entry(cache_key)
                if cached is not None:
                    return cached == '1'
            role = enforcer.get_roles_for_user(user_id)
        else:
            role = self.get_user_roles(user_id)
        if rbac.RoleName.ADMIN.value in role:
            result = True
        else:
            # The Casbin model matcher already handles the wildcard '*' case:
            # m = (g(r.sub, p.sub)|| p.sub == '*') && r.obj == p.obj &&
            # r.act == p.act
            # This means if there's a policy ('*', workspace_name, '*'), it
            # will match any user
            result = enforcer.enforce(user_id, workspace_name, '*')

        logger.debug(f'Workspace permission check: user={user_id}, '
                     f'workspace={workspace_name}, result={result}')

        # Cache the result; failures are non-critical.
        try:
            kv_cache.add_or_update_cache_entry(
                cache_key, '1' if result else '0',
                time.time() + _WORKSPACE_PERM_CACHE_TTL_SECONDS)
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Failed to cache workspace permission: {e}')

        return result

    def check_service_account_token_permission(self, user_id: str,
                                               token_owner_id: str,
                                               action: str) -> bool:
        """Check service account token permission.

        This method checks if a user has permission to perform an action on
        a service account token owned by another user.

        Args:
            user_id: The ID of the user requesting the action
            token_owner_id: The ID of the user who owns the token
            action: The action being performed (e.g., 'delete', 'view')

        Returns:
            True if the user has permission, False otherwise
        """
        del action

        user_roles = self.get_user_roles(user_id)
        # Admin can manage any token — check this first so a user
        # holding both admin and viewer isn't blocked by the viewer rule.
        if rbac.RoleName.ADMIN.value in user_roles:
            return True

        # Viewers cannot manage ANY service-account tokens.
        if rbac.RoleName.VIEWER.value in user_roles:
            return False

        # Users can always manage their own tokens
        if user_id == token_owner_id:
            return True

        # Regular users cannot manage tokens owned by others
        return False

    def add_workspace_policy(self, workspace_name: str,
                             users: list[str]) -> None:
        """Add workspace policy.

        Args:
            workspace_name: Name of the workspace
            users: List of user IDs that should have access.
                   For public workspaces, this should be ['*'].
                   For private workspaces, this should be specific user IDs.
        """
        if skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
            raise RuntimeError('Guarded-HA workspace policy writes must join '
                               'the central config transaction.')
        with _policy_lock():
            enforcer = self._ensure_enforcer()
            for user in users:
                logger.debug(f'Adding workspace policy: user={user}, '
                             f'workspace={workspace_name}')
                enforcer.add_policy(user, workspace_name, '*')
            enforcer.save_policy()
            # Invalidate stale cached denials (e.g. from checks between a
            # workspace deletion and its re-creation with the same name).
            self.invalidate_workspace_permission_cache(workspace_name)

    def update_workspace_policy(self, workspace_name: str,
                                users: list[str]) -> None:
        """Update workspace policy.

        Args:
            workspace_name: Name of the workspace
            users: List of user IDs that should have access.
                   For public workspaces, this should be ['*'].
                   For private workspaces, this should be specific user IDs.
        """
        if skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
            raise RuntimeError('Guarded-HA workspace policy writes must join '
                               'the central config transaction.')
        with _policy_lock():
            self._load_policy_no_lock()
            enforcer = self._ensure_enforcer()
            # Remove all existing policies for this workspace
            enforcer.remove_filtered_policy(1, workspace_name)
            # Add new policies
            for user in users:
                logger.debug(f'Updating workspace policy: user={user}, '
                             f'workspace={workspace_name}')
                enforcer.add_policy(user, workspace_name, '*')
            enforcer.save_policy()
            # Invalidate cached permission entries after the policy is
            # persisted so other processes re-compute permissions on next
            # check.
            self.invalidate_workspace_permission_cache(workspace_name)

    def remove_workspace_policy(self, workspace_name: str) -> None:
        """Remove workspace policy."""
        if skypilot_config._postgres_server_config_is_authoritative():  # pylint: disable=protected-access
            raise RuntimeError('Guarded-HA workspace policy writes must join '
                               'the central config transaction.')
        with _policy_lock():
            enforcer = self._ensure_enforcer()
            enforcer.remove_filtered_policy(1, workspace_name)
            enforcer.save_policy()
            # Invalidate cached permission entries after the policy is
            # persisted so other processes re-compute permissions on next
            # check.
            self.invalidate_workspace_permission_cache(workspace_name)


@contextlib.contextmanager
def _policy_lock() -> Generator[None, None, None]:
    """Legacy/non-workspace policy lock retained until D6.

    Guarded-HA workspace add/update/delete and batch mutation bypass this file
    lock by joining the PostgreSQL central-config transaction. Remaining D6
    callers are permission initialization, basic-auth/user-role mutation, and
    explicit policy reloads.
    """
    try:
        with filelock.FileLock(POLICY_UPDATE_LOCK_PATH,
                               POLICY_UPDATE_LOCK_TIMEOUT_SECONDS):
            yield
    except filelock.Timeout as e:
        raise RuntimeError(f'Failed to reload policy due to a timeout '
                           f'when trying to acquire the lock at '
                           f'{POLICY_UPDATE_LOCK_PATH}. '
                           'Please try again or manually remove the lock '
                           f'file if you believe it is stale.') from e


# Singleton instance of PermissionService for other modules to use.
permission_service = PermissionService()
