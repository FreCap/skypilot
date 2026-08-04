"""Kubernetes adaptors

Thread safety notes:

The API functions (core_api, batch_api, etc.) return cached clients that are
created with context-specific ApiClient instances.

Set SKYPILOT_KUBECONFIG_REFRESH_INTERVAL_SECONDS (seconds) to refresh the
client proactively at a fixed interval so it is rebuilt from the updated
kubeconfig (e.g. for short-lived certs).
"""
import base64
from collections.abc import Callable
import contextlib
import copy
import dataclasses
import functools
import hmac
import json
import logging
import os
import platform
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
from types import MappingProxyType
import typing
from typing import Any
import weakref

from sky import sky_logging
from sky.adaptors import common
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import kubernetes
    import urllib3
    import urllib3.exceptions
else:
    _IMPORT_ERROR_MESSAGE = ('Failed to import dependencies for Kubernetes. '
                             'Try running: pip install "skypilot[kubernetes]"')
    kubernetes = common.LazyImport('kubernetes',
                                   import_error_message=_IMPORT_ERROR_MESSAGE)
    urllib3 = common.LazyImport('urllib3',
                                import_error_message=_IMPORT_ERROR_MESSAGE)

# Timeout to use for API calls
API_TIMEOUT = 5

# Check if KUBECONFIG is set, and use it if it is.
DEFAULT_KUBECONFIG_PATH = '~/.kube/config'
# From kubernetes package, keep a copy here to avoid actually importing
# kubernetes package when parsing the KUBECONFIG env var to do credential
# file mounts.
ENV_KUBECONFIG_PATH_SEPARATOR = ';' if platform.system() == 'Windows' else ':'

DEFAULT_IN_CLUSTER_REGION = 'in-cluster'
IN_CLUSTER_IDENTITY_PREFIX = 'skypilot-in-cluster-identity'
# The name for the environment variable that stores the in-cluster context name
# for Kubernetes clusters. This is used to associate a name with the current
# context when running with in-cluster auth. If not set, the context name is
# set to DEFAULT_IN_CLUSTER_REGION.
IN_CLUSTER_CONTEXT_NAME_ENV_VAR = 'SKYPILOT_IN_CLUSTER_CONTEXT_NAME'
IN_CLUSTER_NAMESPACE_ENV_VAR = 'SKYPILOT_IN_CLUSTER_NAMESPACE'
IN_CLUSTER_NAMESPACE_PATH = (
    '/var/run/secrets/kubernetes.io/serviceaccount/namespace')
IN_CLUSTER_TOKEN_PATH = ('/var/run/secrets/kubernetes.io/serviceaccount/token')
IN_CLUSTER_CA_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
# If set (positive seconds), client is refreshed proactively after this
# interval.
KUBECONFIG_REFRESH_INTERVAL_ENV_VAR = (
    'SKYPILOT_KUBECONFIG_REFRESH_INTERVAL_SECONDS')
_MAX_EXEC_CREDENTIAL_OUTPUT_BYTES = 1024 * 1024
_EXEC_CREDENTIAL_IO_CHUNK_BYTES = 64 * 1024
_EXEC_CREDENTIAL_POLL_SECONDS = 0.05
_EXEC_CREDENTIAL_TERMINATION_SECONDS = 1
_IN_CLUSTER_CREDENTIAL_REFRESH_SECONDS = 60
_MAX_IN_CLUSTER_CA_BYTES = 1024 * 1024
_MAX_KUBECONFIG_CA_BYTES = 1024 * 1024
_MAX_KUBECONFIG_TOKEN_BYTES = 1024 * 1024

logger = sky_logging.init_logger(__name__)


@dataclasses.dataclass(frozen=True)
class KubernetesContextIdentity:
    """Non-secret identity resolved by the same load as an API client."""

    context_name: str
    identity: tuple[str, ...]
    in_cluster: bool
    namespace: str


@dataclasses.dataclass(frozen=True)
class KubernetesContextInventory:
    """Non-secret context discovery from one uncached kubeconfig load."""

    available_context_names: tuple[str, ...]
    kubeconfig_current_context_name: str | None
    in_cluster_available: bool
    in_cluster_context_name: str

    def __post_init__(self) -> None:
        if (type(self.available_context_names) is not tuple or any(
                type(context_name) is not str or not context_name
                for context_name in self.available_context_names)):
            raise ValueError(
                'available_context_names must contain nonempty strings.')
        current_context = self.kubeconfig_current_context_name
        if (current_context is not None and
            (type(current_context) is not str or not current_context)):
            raise ValueError(
                'kubeconfig_current_context_name must be a nonempty string '
                'or None.')
        if (current_context is not None and
                current_context not in self.available_context_names):
            raise ValueError(
                'kubeconfig_current_context_name must be available.')
        if type(self.in_cluster_available) is not bool:
            raise ValueError('in_cluster_available must be a bool.')
        if (type(self.in_cluster_context_name) is not str or
                not self.in_cluster_context_name):
            raise ValueError(
                'in_cluster_context_name must be a nonempty string.')


def normalize_kubernetes_context_identity(context: Any) -> str:
    """Return the legacy owner identity for one kubeconfig context.

    This deliberately preserves the historical underscore-delimited wire
    value and its ambiguity when names themselves contain underscores.
    """
    context_data = context['context']
    namespace = (context_data['namespace']
                 if 'namespace' in context_data else 'default')
    user = context_data['user']
    cluster = context_data['cluster']
    return f'{cluster}_{user}_{namespace}'


def normalize_kubernetes_in_cluster_identity(context_name: str) -> str:
    """Return the legacy owner identity for one in-cluster context name."""
    return f'{IN_CLUSTER_IDENTITY_PREFIX}-{context_name}'


def _remove_capture_file(path: str | None) -> None:
    """Best-effort removal for a capture-owned temporary file."""
    if path is None:
        return
    try:
        os.unlink(path)
    except OSError:
        # Cleanup must remain safe during interpreter shutdown and after an
        # explicit close has already removed the file.
        pass


def _best_effort_getattr(obj: Any, name: str) -> Any | None:
    """Read one cleanup input without blocking independent cleanup phases."""
    try:
        return getattr(obj, name, None)
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        return None  # noqa: ASYNC104


def _best_effort_setattr(obj: Any, name: str, value: Any) -> None:
    """Detach one cleanup reference without replacing the primary result."""
    try:
        setattr(obj, name, value)
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        pass


def _best_effort_call(callback: Any) -> None:
    """Run one cleanup callback without skipping later cleanup phases."""
    if not callable(callback):
        return
    try:
        callback()
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        pass


def _best_effort_clear_dict_attribute(obj: Any, name: str) -> None:
    """Clear one dictionary attribute, detaching it if clear itself fails."""
    value = _best_effort_getattr(obj, name)
    if isinstance(value, dict):
        try:
            value.clear()
            return
        except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            pass
    _best_effort_setattr(obj, name, None)


class _KubernetesClientFiles:
    """Owns the complete TLS file set for one exact API client target."""

    __slots__ = ('_closed', '_directory', '_paths')

    def __init__(self) -> None:
        self._directory = tempfile.mkdtemp(prefix='skypilot-k8s-target-')
        os.chmod(self._directory, stat.S_IRWXU)
        self._paths: list[str] = []
        self._closed = False

    def write(self, filename: str, data: bytes) -> str:
        if self._closed:
            raise RuntimeError('Kubernetes client files are closed.')
        if (type(filename) is not str or not filename or
                os.path.basename(filename) != filename):
            raise ValueError('Kubernetes client filename is invalid.')
        path = os.path.join(self._directory, filename)
        file_descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                  stat.S_IRUSR)
        self._paths.append(path)
        try:
            file = os.fdopen(file_descriptor, 'wb')
            file_descriptor = -1
            with file:
                file.write(data)
        except BaseException:  # pylint: disable=broad-exception-caught
            if file_descriptor >= 0:
                try:
                    os.close(file_descriptor)
                except OSError:
                    pass
            self.close()
            raise
        return path

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for path in self._paths:
            _remove_capture_file(path)
        self._paths.clear()
        try:
            os.rmdir(self._directory)
        except OSError:
            pass


def _scrub_kubernetes_configuration_credentials(
        configuration: Any | None) -> None:
    """Best-effort removal of credentials from a partial configuration."""
    if configuration is None:
        return
    # kubernetes 20 and 24 install in-cluster refresh by replacing this method
    # on the Configuration instance. Dropping only api_key and
    # refresh_api_key_hook leaves the bound loader, its token, and its live
    # token path reachable and callable after close.
    configuration_attributes = _best_effort_getattr(configuration, '__dict__')
    if isinstance(configuration_attributes, dict):
        try:
            configuration_attributes.pop('get_api_key_with_prefix', None)
        except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            pass

    refresh_hook = _best_effort_getattr(configuration, 'refresh_api_key_hook')
    close_refresh_hook = _best_effort_getattr(refresh_hook, 'close')
    _best_effort_call(close_refresh_hook)
    _best_effort_setattr(configuration, 'refresh_api_key_hook', None)

    for name in ('api_key', 'api_key_prefix'):
        _best_effort_clear_dict_attribute(configuration, name)
    for name in ('username', 'password', 'cert_file', 'key_file',
                 'key_password', 'proxy', 'proxy_headers', 'host'):
        value = _best_effort_getattr(configuration, name)
        if isinstance(value, dict):
            try:
                value.clear()
            except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
                pass
        _best_effort_setattr(configuration, name, None)


def _scrub_kubernetes_api_client_transport(  # pylint: disable=redefined-outer-name
        api_client: Any | None) -> None:
    """Make a retired urllib3 transport unreachable and credential-free."""
    if api_client is None:
        return
    rest_client = _best_effort_getattr(api_client, 'rest_client')
    pool_manager = _best_effort_getattr(rest_client, 'pool_manager')
    if pool_manager is not None:
        _best_effort_call(_best_effort_getattr(pool_manager, 'clear'))
        for name in ('connection_pool_kw', 'headers', 'proxy_headers'):
            _best_effort_clear_dict_attribute(pool_manager, name)
        for name in ('proxy', 'proxy_config', 'proxy_ssl_context'):
            _best_effort_setattr(pool_manager, name, None)
    if rest_client is not None:
        _best_effort_setattr(rest_client, 'pool_manager', None)


def _close_api_client_resources(  # pylint: disable=redefined-outer-name
        api_client: Any,
        owned_files: _KubernetesClientFiles | None = None) -> None:
    """Shared scrub, transport-close, and TLS-file ownership boundary."""
    try:
        _scrub_bounded_api_client_credentials(api_client)
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        pass
    try:
        api_client.close()
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        pass
    if owned_files is not None:
        _best_effort_call(_best_effort_getattr(owned_files, 'close'))


class KubernetesApiClientTarget:
    """Owns one raw client and its exact, same-load resolved identity.

    ``close()`` is deterministic and idempotent. It drops this object's
    credential-bearing client reference, scrubs the client configuration,
    closes the transport, and immediately removes the capture-owned CA copy.
    A finalizer provides the same cleanup if an owner forgets to close it.
    """

    __slots__ = ('_api_client', '_close_condition', '_closed',
                 '_closing_thread_id', '_context_identity', '_finalizer',
                 '__weakref__')

    def __init__(  # pylint: disable=redefined-outer-name
            self, *, api_client: Any,
            context_identity: KubernetesContextIdentity,
            owned_files: _KubernetesClientFiles | None) -> None:
        self._api_client = api_client
        self._close_condition = threading.Condition()
        self._closed = False
        self._closing_thread_id: int | None = None
        self._context_identity = context_identity
        self._finalizer = weakref.finalize(self, _close_api_client_resources,
                                           api_client, owned_files)

    @property
    def api_client(self) -> Any:
        with self._close_condition:
            client = self._api_client
            if client is None:
                raise RuntimeError('Kubernetes API client target is closed.')
            return client

    @property
    def context_identity(self) -> KubernetesContextIdentity:
        return self._context_identity

    @property
    def closed(self) -> bool:
        with self._close_condition:
            return self._closed

    def close(self) -> None:
        """Immediately releases all owned resources; safe to call repeatedly."""
        thread_id = threading.get_ident()
        with self._close_condition:
            while self._closing_thread_id is not None:
                if self._closing_thread_id == thread_id:
                    # A refresh-hook cleanup may reenter its owning target.
                    # The outer call already owns and will finish the cleanup.
                    return
                self._close_condition.wait()
            if self._closed:
                return
            self._closing_thread_id = thread_id
            # Stop new users from borrowing the client before scrubbing starts.
            self._api_client = None
        try:
            # Cleanup can close a refresh hook, so never hold the target lock
            # while invoking the finalizer callback.
            self._finalizer()
        finally:
            with self._close_condition:
                self._closed = True
                self._closing_thread_id = None
                self._close_condition.notify_all()

    def __enter__(self) -> 'KubernetesApiClientTarget':
        if self.closed:
            raise RuntimeError('Kubernetes API client target is closed.')
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def __repr__(self) -> str:
        return (f'{type(self).__name__}('
                f'context_identity={self._context_identity!r}, '
                f'closed={self.closed!r})')


class _KubernetesApiClientTargetResult(typing.NamedTuple):
    """Credential-free result crossing the target-construction boundary."""

    target: KubernetesApiClientTarget | None
    failure_message: str | None
    failure_kind: str | None


def _api_client_target_failure(
        message: str,
        *,
        failure_kind: str = 'config') -> _KubernetesApiClientTargetResult:
    return _KubernetesApiClientTargetResult(None, message, failure_kind)


class KubernetesContextLoadSession:
    """One-load, capture-scoped source of exact Kubernetes client targets.

    The retained kubeconfig snapshot can contain credentials. It is never
    included in ``repr`` and is discarded by ``close()``. Unsupported exec and
    auth-provider modes are classified from the captured tree and rejected
    before an upstream client loader is constructed or invoked.
    """

    __slots__ = (
        '_closed',
        '_in_cluster_ca_data',
        '_in_cluster_environment',
        '_in_cluster_load_error',
        '_in_cluster_namespace',
        '_inventory',
        '_kubeconfig_snapshot',
        '_unsupported_credential_modes',
    )

    def __init__(
        self,
        *,
        inventory: KubernetesContextInventory,
        kubeconfig_snapshot: Any | None,
        unsupported_credential_modes: dict[str, str],
        in_cluster_ca_data: bytes | None,
        in_cluster_environment: dict[str, str],
        in_cluster_load_error: str | None,
        in_cluster_namespace: str,
    ) -> None:
        self._inventory = inventory
        self._kubeconfig_snapshot = kubeconfig_snapshot
        self._unsupported_credential_modes = MappingProxyType(
            dict(unsupported_credential_modes))
        self._in_cluster_ca_data = in_cluster_ca_data
        self._in_cluster_environment = MappingProxyType(
            dict(in_cluster_environment))
        self._in_cluster_load_error = in_cluster_load_error
        self._in_cluster_namespace = in_cluster_namespace
        self._closed = False

    @property
    def inventory(self) -> KubernetesContextInventory:
        return self._inventory

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Discard the retained credential-bearing kubeconfig snapshot."""
        if self._closed:
            return
        self._closed = True
        self._kubeconfig_snapshot = None
        self._unsupported_credential_modes = MappingProxyType({})
        self._in_cluster_ca_data = None
        self._in_cluster_environment = MappingProxyType({})
        self._in_cluster_load_error = None

    def __enter__(self) -> 'KubernetesContextLoadSession':
        if self.closed:
            raise RuntimeError('Kubernetes context load session is closed.')
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def __repr__(self) -> str:
        return (f'{type(self).__name__}(inventory={self._inventory!r}, '
                f'closed={self.closed!r})')

    def new_api_client_target(self,
                              context_name: str) -> KubernetesApiClientTarget:
        """Construct a target without exposing credential-bearing frames."""
        result = _new_api_client_target_isolated(self, context_name)
        # An exception raised by this public boundary must not retain ``self``:
        # the session owns the credential-bearing ConfigNode snapshot.
        del self, context_name
        if result.target is not None:
            return result.target
        failure_message = result.failure_message
        failure_kind = result.failure_kind
        del result
        assert failure_message is not None
        if failure_kind == 'runtime':
            raise RuntimeError(failure_message) from None
        if failure_kind == 'value':
            raise ValueError(failure_message) from None
        config_exception_cls = (
            kubernetes.config.config_exception.ConfigException)
        raise config_exception_cls(failure_message) from None


def _decorate_methods(obj: Any, decorator: Callable, decoration_type: str):
    for attr_name in dir(obj):
        attr = getattr(obj, attr_name)
        # Skip methods starting with '__' since they are invoked through one
        # of the main methods, which are already decorated.
        if callable(attr) and not attr_name.startswith('__'):
            decorated_types: set[str] = getattr(attr, '_sky_decorator_types',
                                                set())
            if decoration_type not in decorated_types:
                decorated_attr = decorator(attr)
                decorated_attr._sky_decorator_types = (  # pylint: disable=protected-access
                    decorated_types | {decoration_type})
                setattr(obj, attr_name, decorated_attr)
    return obj


def _api_logging_decorator(logger_src: str, level: int):
    """Decorator to set logging level for API calls.

    This is used to suppress the verbose logging from urllib3 when calls to the
    Kubernetes API timeout.
    """

    def decorated_api(api):

        def wrapped(*args, **kwargs):
            obj = api(*args, **kwargs)
            _decorate_methods(obj,
                              sky_logging.set_logging_level(logger_src, level),
                              'api_log')
            return obj

        return wrapped

    return decorated_api


def _get_config_file() -> str:
    # Kubernetes load the kubeconfig from the KUBECONFIG env var on
    # package initialization. So we have to reload the KUBECONFIG env var
    # everytime in case the KUBECONFIG env var is changed.
    return os.environ.get('KUBECONFIG', DEFAULT_KUBECONFIG_PATH)


def _capture_config_file_paths(config_file: str) -> str:
    """Freeze relative and user-relative kubeconfig paths at capture time."""
    return ENV_KUBECONFIG_PATH_SEPARATOR.join(
        os.path.abspath(os.path.expanduser(path)) if path else path
        for path in config_file.split(ENV_KUBECONFIG_PATH_SEPARATOR))


def _raise_kubeconfig_load_error(context: str | None,
                                 error: Exception) -> typing.NoReturn:
    """Raise the historical user-facing kubeconfig load error."""
    suffix = common_utils.format_exception(error, use_bracket=True)
    context_name = '(current-context)' if context is None else context
    is_ssh_node_pool = False
    if context_name.startswith('ssh-'):
        context_name = common_utils.removeprefix(context_name, 'ssh-')
        is_ssh_node_pool = True
    if 'Expected key current-context' in str(error):
        if is_ssh_node_pool:
            context_name = common_utils.removeprefix(context_name, 'ssh-')
            err_str = ('Failed to load SSH Node Pool configuration for '
                       f'{context_name!r}.\n'
                       f'    Run `sky ssh up --infra {context_name}` to '
                       'set up or repair the cluster.')
        else:
            err_str = ('Failed to load Kubernetes configuration for '
                       f'{context_name!r}. '
                       'Kubeconfig does not contain any valid context(s).'
                       f'\n{suffix}\n'
                       '    If you were running a local Kubernetes '
                       'cluster, run `sky local up` to start the cluster.')
    else:
        kubeconfig_path = os.environ.get('KUBECONFIG', '~/.kube/config')
        if is_ssh_node_pool:
            err_str = (f'Failed to load SSH Node Pool configuration for '
                       f'{context_name!r}. Run `sky ssh up --infra '
                       f'{context_name}` to set up or repair the cluster.')
        else:
            err_str = ('Failed to load Kubernetes configuration for '
                       f'{context_name!r}. Please check if your kubeconfig '
                       f'file exists at {kubeconfig_path} and is valid.'
                       f'\n{suffix}\n')
    if is_ssh_node_pool:
        err_str += (f'\nTo disable SSH Node Pool {context_name!r}: '
                    'run `sky check`.')
    else:
        err_str += ('\nHint: Kubernetes attempted to query the current-context '
                    'set in kubeconfig. Check if the current-context is valid.')
    with ux_utils.print_exception_no_traceback():
        raise ValueError(err_str) from None


def _get_in_cluster_namespace() -> str:
    """Resolve the namespace without consulting kubeconfig."""
    namespace = os.environ.get(IN_CLUSTER_NAMESPACE_ENV_VAR)
    if namespace:
        return namespace
    if os.path.exists(IN_CLUSTER_NAMESPACE_PATH):
        with open(IN_CLUSTER_NAMESPACE_PATH, encoding='utf-8') as file:
            return file.read().strip()
    return 'default'


def _is_in_cluster_config_available() -> bool:
    """Return whether the service-account token existed at capture time."""
    return os.path.exists(IN_CLUSTER_TOKEN_PATH)


def _read_in_cluster_ca() -> bytes:
    """Read one bounded CA value for a capture-scoped target."""
    try:
        with open(IN_CLUSTER_CA_PATH, 'rb') as ca_file:
            if not stat.S_ISREG(os.fstat(ca_file.fileno()).st_mode):
                raise kubernetes.config.config_exception.ConfigException(
                    'Service certification file is not regular.')
            ca_data = ca_file.read(_MAX_IN_CLUSTER_CA_BYTES + 1)
    except OSError as error:
        raise kubernetes.config.config_exception.ConfigException(
            'Service certification file could not be read.') from error
    if not ca_data:
        raise kubernetes.config.config_exception.ConfigException(
            'Cert file exists but empty.')
    if len(ca_data) > _MAX_IN_CLUSTER_CA_BYTES:
        raise kubernetes.config.config_exception.ConfigException(
            'Service certification file exceeds 1 MiB.')
    return ca_data


def _read_bounded_client_file(path: str, *, description: str) -> bytes:
    """Read one regular TLS input without reflecting its path in errors."""
    if type(path) is not str or not path:
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} path is invalid.')
    try:
        with open(path, 'rb') as source_file:
            if not stat.S_ISREG(os.fstat(source_file.fileno()).st_mode):
                raise kubernetes.config.config_exception.ConfigException(
                    f'{description} must be a regular file.')
            data = source_file.read(_MAX_KUBECONFIG_CA_BYTES + 1)
    except OSError as error:
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} could not be read.') from error
    if not data:
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} is empty.')
    if len(data) > _MAX_KUBECONFIG_CA_BYTES:
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} exceeds 1 MiB.')
    return data


def _decode_bounded_client_data(value: Any, *, description: str) -> bytes:
    """Decode an upstream-compatible bounded kubeconfig data field."""
    if not isinstance(value, (str, bytes)):
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} data is invalid.')
    encoded = value.encode() if isinstance(value, str) else value
    if len(encoded) > (_MAX_KUBECONFIG_CA_BYTES * 2):
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} exceeds 1 MiB.')
    try:
        data = base64.standard_b64decode(encoded)
    except (TypeError, ValueError) as error:
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} data is invalid.') from error
    if not data:
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} is empty.')
    if len(data) > _MAX_KUBECONFIG_CA_BYTES:
        raise kubernetes.config.config_exception.ConfigException(
            f'{description} exceeds 1 MiB.')
    return data


class _KubernetesTokenFileReadResult(typing.NamedTuple):
    token: str | None
    failure_message: str | None


def _token_file_read_failure(message: str) -> _KubernetesTokenFileReadResult:
    return _KubernetesTokenFileReadResult(None, message)


def _read_bounded_kubeconfig_token_file_impl(
        path: Any) -> _KubernetesTokenFileReadResult:
    """Reads a token without reflecting its path or contents in failures."""
    if type(path) is not str or not path:
        return _token_file_read_failure(
            'Kubernetes token file path is invalid.')
    file_descriptor = -1
    try:
        if stat.S_ISLNK(os.lstat(path).st_mode):
            return _token_file_read_failure(
                'Kubernetes token file must not be a symbolic link.')
        open_flags = os.O_RDONLY
        for optional_flag_name in ('O_NONBLOCK', 'O_CLOEXEC', 'O_NOFOLLOW',
                                   'O_BINARY'):
            open_flags |= getattr(os, optional_flag_name, 0)
        file_descriptor = os.open(path, open_flags)
        if not stat.S_ISREG(os.fstat(file_descriptor).st_mode):
            return _token_file_read_failure(
                'Kubernetes token file must be a regular file.')
        with os.fdopen(file_descriptor, 'rb') as token_file:
            file_descriptor = -1
            token_data = token_file.read(_MAX_KUBECONFIG_TOKEN_BYTES + 1)
    except OSError:
        return _token_file_read_failure(
            'Kubernetes token file could not be read safely.')
    finally:
        if file_descriptor >= 0:
            try:
                os.close(file_descriptor)
            except OSError:
                pass
    if not token_data:
        return _token_file_read_failure('Kubernetes token file is empty.')
    if len(token_data) > _MAX_KUBECONFIG_TOKEN_BYTES:
        return _token_file_read_failure('Kubernetes token file exceeds 1 MiB.')
    try:
        token = token_data.decode('utf-8')
    except UnicodeDecodeError:
        token_data = b''
        return _token_file_read_failure(
            'Kubernetes token file is not valid UTF-8.')
    token_data = b''
    return _KubernetesTokenFileReadResult(token, None)


def _read_bounded_kubeconfig_token_file(
        path: Any) -> _KubernetesTokenFileReadResult:
    """Isolation boundary for a live, externally rotated token file."""
    try:
        return _read_bounded_kubeconfig_token_file_impl(path)
    except BaseException as error:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        error = error.with_traceback(None)
        error.__cause__ = None
        error.__context__ = None
        return _token_file_read_failure(  # noqa: ASYNC104
            'Kubernetes token file could not be read safely.')


class _KubernetesTokenFileRefresh:
    """Thread-safe, close-aware refresh for one frozen token-file path."""

    __slots__ = ('_lock', '_path')

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._path: str | None = path

    def __call__(self, configuration: Any) -> None:
        failure_message: str | None = None
        with self._lock:
            path = self._path
            if path is None:
                result = _token_file_read_failure(
                    'Kubernetes token-file refresh is closed.')
            else:
                result = _read_bounded_kubeconfig_token_file(path)
            if result.token is not None:
                configuration.api_key['authorization'] = (
                    f'Bearer {result.token}')
            else:
                # A target with an unreadable rotated credential must not keep
                # using the last token or repeatedly retain the sensitive path.
                self._path = None
                api_key = getattr(configuration, 'api_key', None)
                if isinstance(api_key, dict):
                    api_key.pop('authorization', None)
                if getattr(configuration, 'refresh_api_key_hook', None) is self:
                    configuration.refresh_api_key_hook = None
                failure_message = result.failure_message
            del path, result
        if failure_message is not None:
            config_exception_cls = (
                kubernetes.config.config_exception.ConfigException)
            raise config_exception_cls(failure_message) from None

    def close(self) -> None:
        with self._lock:
            self._path = None

    def __repr__(self) -> str:
        return f'{type(self).__name__}(closed={self._path is None!r})'


def _materialize_bounded_kubeconfig_token_file(
        loader: Any) -> _KubernetesTokenFileRefresh | None:
    """Replaces upstream's unbounded tokenFile read with a bounded value."""
    user = loader._user  # pylint: disable=protected-access
    if user is None or 'token' in user or 'tokenFile' not in user:
        # FileOrData gives an explicitly present inline token priority, even
        # when its value is empty. Preserve that behavior exactly.
        return None
    source_path = user['tokenFile']
    if type(source_path) is not str or not source_path:
        raise kubernetes.config.config_exception.ConfigException(
            'Kubernetes token file path is invalid.')
    base_path = loader._get_base_path(user.path)  # pylint: disable=protected-access
    resolved_path = os.path.normpath(os.path.join(base_path, source_path))
    result = _read_bounded_kubeconfig_token_file(resolved_path)
    if result.token is None:
        assert result.failure_message is not None
        raise kubernetes.config.config_exception.ConfigException(
            result.failure_message)
    token = result.token
    user.value.pop('tokenFile', None)
    user.value['token'] = token
    return _KubernetesTokenFileRefresh(resolved_path)


def _materialize_client_file_or_data(
    *,
    loader: Any,
    node: Any,
    file_key: str,
    data_key: str,
    description: str,
    output_name: str,
    copy_external_file: bool,
    owned_files: _KubernetesClientFiles,
) -> None:
    """Replace one selected TLS input with a capture-owned exact byte copy."""
    if node is None:
        return
    data: bytes | None = None
    if data_key in node:
        encoded = node[data_key]
        if encoded:
            data = _decode_bounded_client_data(encoded, description=description)
        else:
            # FileOrData gives an explicitly present empty data key priority
            # over a file key. Preserve that upstream behavior.
            return
    elif file_key in node:
        if not copy_external_file:
            # Ordinary external client-certificate and key paths retain the
            # existing Kubernetes client's live file-rotation semantics.
            return
        source_path = node[file_key]
        if type(source_path) is not str or not source_path:
            raise kubernetes.config.config_exception.ConfigException(
                f'{description} path is invalid.')
        base_path = loader._get_base_path(node.path)  # pylint: disable=protected-access
        resolved_path = os.path.normpath(os.path.join(base_path, source_path))
        data = _read_bounded_client_file(resolved_path, description=description)
    if data is None:
        return
    owned_path = owned_files.write(output_name, data)
    node.value.pop(data_key, None)
    node.value[file_key] = owned_path


def _materialize_kubeconfig_tls_files(loader: Any) -> _KubernetesClientFiles:
    """Give one target exclusive ownership of all selected TLS files."""
    owned_files = _KubernetesClientFiles()
    try:
        _materialize_client_file_or_data(
            loader=loader,
            node=loader._cluster,  # pylint: disable=protected-access
            file_key='certificate-authority',
            data_key='certificate-authority-data',
            description='Kubernetes CA bundle',
            output_name='ca.crt',
            copy_external_file=True,
            owned_files=owned_files,
        )
        user = loader._user  # pylint: disable=protected-access
        _materialize_client_file_or_data(
            loader=loader,
            node=user,
            file_key='client-certificate',
            data_key='client-certificate-data',
            description='Kubernetes client certificate',
            output_name='client.crt',
            copy_external_file=False,
            owned_files=owned_files,
        )
        _materialize_client_file_or_data(
            loader=loader,
            node=user,
            file_key='client-key',
            data_key='client-key-data',
            description='Kubernetes client key',
            output_name='client.key',
            copy_external_file=False,
            owned_files=owned_files,
        )
        return owned_files
    except BaseException:  # pylint: disable=broad-exception-caught
        owned_files.close()
        raise


def _new_frozen_in_cluster_api_client_target(
    *,
    context_name: str,
    namespace: str,
    ca_data: bytes,
    environment: dict[str, str],
) -> KubernetesApiClientTarget:
    """Build a target from frozen endpoint/CA and a live projected token."""
    owned_files = _KubernetesClientFiles()
    api_client_instance: Any | None = None
    configuration: Any | None = None
    try:
        frozen_ca_path = owned_files.write('ca.crt', ca_data)
        loader = kubernetes.config.incluster_config.InClusterConfigLoader(
            token_filename=IN_CLUSTER_TOKEN_PATH,
            cert_filename=frozen_ca_path,
            try_refresh_token=True,
            environ=MappingProxyType(dict(environment)),
        )
        configuration = kubernetes.client.Configuration()
        loader.load_and_set(configuration)
        api_client_instance = kubernetes.client.ApiClient(
            configuration=configuration)
        return KubernetesApiClientTarget(
            api_client=api_client_instance,
            context_identity=KubernetesContextIdentity(
                context_name=context_name,
                identity=(
                    normalize_kubernetes_in_cluster_identity(context_name),),
                in_cluster=True,
                namespace=namespace,
            ),
            owned_files=owned_files,
        )
    except BaseException:  # pylint: disable=broad-exception-caught
        if api_client_instance is not None:
            _close_api_client_resources(api_client_instance, owned_files)
        else:
            _scrub_kubernetes_configuration_credentials(configuration)
            owned_files.close()
        raise


def _new_kubeconfig_api_client_target(
        loader: Any, expected_context_name: str) -> KubernetesApiClientTarget:
    """Build a target and derive identity from the exact populating loader."""
    token_file_refresh = _materialize_bounded_kubeconfig_token_file(loader)
    owned_files = _materialize_kubeconfig_tls_files(loader)
    configuration: Any | None = None
    api_client_instance: Any | None = None
    try:
        configuration = kubernetes.client.Configuration()
        loader.load_and_set(configuration)
        if token_file_refresh is not None:
            configuration.refresh_api_key_hook = token_file_refresh
        resolved_context = loader.current_context
        resolved_context_name = resolved_context['name']
        if resolved_context_name != expected_context_name:
            raise kubernetes.config.config_exception.ConfigException(
                'Captured Kubernetes context identity changed during '
                'loading.')
        context_data = resolved_context['context']
        cluster_name = context_data['cluster']
        user_name = context_data['user']
        namespace = context_data.get('namespace', 'default')
        if any(
                type(value) is not str or not value
                for value in (resolved_context_name, cluster_name, user_name,
                              namespace)):
            raise kubernetes.config.config_exception.ConfigException(
                'Captured Kubernetes context identity is invalid.')
        api_client_instance = kubernetes.client.ApiClient(
            configuration=configuration)
        return KubernetesApiClientTarget(
            api_client=api_client_instance,
            context_identity=KubernetesContextIdentity(
                context_name=resolved_context_name,
                identity=(
                    normalize_kubernetes_context_identity(resolved_context),),
                in_cluster=False,
                namespace=namespace,
            ),
            owned_files=owned_files,
        )
    except BaseException:  # pylint: disable=broad-exception-caught
        if api_client_instance is not None:
            _close_api_client_resources(api_client_instance, owned_files)
        else:
            _scrub_kubernetes_configuration_credentials(configuration)
            owned_files.close()
        raise


def _new_api_client_target_isolated_impl(
    session: KubernetesContextLoadSession,
    context_name: str,
) -> _KubernetesApiClientTargetResult:
    """Build a target without raising from credential-bearing local state."""
    if session._closed:  # pylint: disable=protected-access
        return _api_client_target_failure(
            'Kubernetes context load session is closed.',
            failure_kind='runtime')
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    if type(context_name) is not str or not context_name:
        return _api_client_target_failure(
            'Kubernetes context name must be a nonempty string.',
            failure_kind='value')
    inventory = session._inventory  # pylint: disable=protected-access
    if context_name not in inventory.available_context_names:
        return _api_client_target_failure(
            'Kubernetes context was unavailable when contexts were captured.')

    if context_name == inventory.in_cluster_context_name:
        if not inventory.in_cluster_available:
            return _api_client_target_failure(
                'In-cluster configuration was unavailable when Kubernetes '
                'contexts were captured.')
        load_error = session._in_cluster_load_error  # pylint: disable=protected-access
        ca_data = session._in_cluster_ca_data  # pylint: disable=protected-access
        if load_error is not None or ca_data is None:
            return _api_client_target_failure(
                load_error or
                'In-cluster configuration could not be captured safely.')
        return _KubernetesApiClientTargetResult(
            _new_frozen_in_cluster_api_client_target(
                context_name=context_name,
                namespace=session._in_cluster_namespace,  # pylint: disable=protected-access
                ca_data=ca_data,
                environment=dict(session._in_cluster_environment),  # pylint: disable=protected-access
            ),
            None,
            None)

    credential_mode = session._unsupported_credential_modes.get(  # pylint: disable=protected-access
        context_name)
    if credential_mode is not None:
        # This fixed message intentionally excludes user names, commands,
        # arguments, environment, provider configuration, and file paths.
        return _api_client_target_failure(
            'Kubernetes observation does not support kubeconfig exec or '
            'auth-provider credentials.')

    kubeconfig_snapshot = session._kubeconfig_snapshot  # pylint: disable=protected-access
    if kubeconfig_snapshot is None:
        return _api_client_target_failure(
            'Kubernetes kubeconfig was unavailable when contexts were '
            'captured.')
    # ConfigNode path metadata from KubeConfigMerger resolves relative files in
    # merged kubeconfigs. No persister is supplied: a captured tree must never
    # overwrite an ambient file that changed later.
    loader = kubernetes.config.kube_config.KubeConfigLoader(
        config_dict=copy.deepcopy(kubeconfig_snapshot),
        active_context=context_name,
        config_base_path=None,
    )
    return _KubernetesApiClientTargetResult(
        _new_kubeconfig_api_client_target(loader, context_name), None, None)


def _new_api_client_target_isolated(
    session: KubernetesContextLoadSession,
    context_name: str,
) -> _KubernetesApiClientTargetResult:
    """Total isolation boundary for credential-bearing target construction."""
    try:
        return _new_api_client_target_isolated_impl(session, context_name)
    except BaseException as error:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        # Drop the originating traceback and all exception links before the
        # sensitive inner frame can leave this boundary.
        error = error.with_traceback(None)
        error.__cause__ = None
        error.__context__ = None
        return _api_client_target_failure(  # noqa: ASYNC104
            'Kubernetes observation client target could not be created safely.')


def _unsupported_context_credential_modes(
        kubeconfig_snapshot: Any) -> dict[str, str]:
    """Classify unsafe credential modes without constructing a client loader."""
    unsupported_users: dict[str, str] = {}
    users = kubeconfig_snapshot.safe_get('users')
    if users is not None:
        for user_entry in users:
            user_name = user_entry.safe_get('name')
            user_config = (user_entry['user'] if 'user' in user_entry else None)
            if type(user_name) is not str or user_config is None:
                continue
            has_exec = 'exec' in user_config
            has_auth_provider = 'auth-provider' in user_config
            if has_exec or has_auth_provider:
                unsupported_users[user_name] = (
                    'exec+auth-provider' if has_exec and has_auth_provider else
                    'exec' if has_exec else 'auth-provider')

    unsupported_contexts: dict[str, str] = {}
    contexts = kubeconfig_snapshot.safe_get('contexts')
    if contexts is None:
        return unsupported_contexts
    for context_entry in contexts:
        context_name = context_entry.safe_get('name')
        context_config = (context_entry['context']
                          if 'context' in context_entry else None)
        if type(context_name) is not str or context_config is None:
            continue
        user_name = context_config.safe_get('user')
        if type(user_name) is str and user_name in unsupported_users:
            unsupported_contexts[context_name] = unsupported_users[user_name]
    return unsupported_contexts


class _KubeconfigCaptureResult(typing.NamedTuple):
    kubeconfig_snapshot: Any | None
    context_names: tuple[str, ...]
    current_context_name: str | None
    unsupported_credential_modes: dict[str, str]
    control_error: BaseException | None


def _empty_kubeconfig_capture() -> _KubeconfigCaptureResult:
    return _KubeconfigCaptureResult(None, (), None, {}, None)


def _capture_kubeconfig_isolated(config_file: str) -> _KubeconfigCaptureResult:
    """Loads one kubeconfig tree without exporting credential-bearing errors."""
    try:
        candidate_merger = kubernetes.config.kube_config.KubeConfigMerger(
            config_file)
        if candidate_merger.config is None:
            raise kubernetes.config.config_exception.ConfigException(
                'Invalid kube-config file. No configuration found.')
        # Constructor-only discovery resolves current context and structural
        # references but never invokes load_and_set or credential plugins.
        discovery_loader = kubernetes.config.kube_config.KubeConfigLoader(
            config_dict=candidate_merger.config,
            config_base_path=None,
        )
        context_names = tuple(
            context['name'] for context in discovery_loader.list_contexts())
        current_context_name = discovery_loader.current_context['name']
        kubeconfig_snapshot = copy.deepcopy(candidate_merger.config)
        unsupported_credential_modes = _unsupported_context_credential_modes(
            kubeconfig_snapshot)
        # Validate every public primitive while the credential-bearing tree is
        # still inside this isolation boundary.
        KubernetesContextInventory(
            available_context_names=context_names,
            kubeconfig_current_context_name=current_context_name,
            in_cluster_available=False,
            in_cluster_context_name=DEFAULT_IN_CLUSTER_REGION,
        )
        return _KubeconfigCaptureResult(
            kubeconfig_snapshot,
            context_names,
            current_context_name,
            unsupported_credential_modes,
            None,
        )
    except Exception as error:  # pylint: disable=broad-exception-caught
        # PyYAML parser errors, Unicode failures, filesystem failures, malformed
        # merge shapes, and ConfigException can all retain the raw kubeconfig in
        # upstream traceback frames. Invalid discovery is an empty inventory,
        # so detach and discard every ordinary failure at this boundary.
        error = error.with_traceback(None)
        error.__cause__ = None
        error.__context__ = None
        return _empty_kubeconfig_capture()
    except BaseException as error:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        # Preserve cancellation and process-control semantics without exporting
        # the credential-bearing merger or upstream frames.
        return _KubeconfigCaptureResult(  # noqa: ASYNC104
            None, (), None, {}, _detach_control_error(error))


def load_kubernetes_contexts_uncached() -> KubernetesContextLoadSession:
    """Capture context inventory and client inputs from one kubeconfig load.

    An absent or invalid kubeconfig contributes no contexts, matching legacy
    discovery, while in-cluster discovery remains independent. The returned
    session never rereads the ambient kubeconfig.
    """
    config_file = _capture_config_file_paths(_get_config_file())
    in_cluster_name = in_cluster_context_name()
    in_cluster_available = _is_in_cluster_config_available()
    in_cluster_namespace = (_get_in_cluster_namespace()
                            if in_cluster_available else 'default')
    in_cluster_environment = {
        key: os.environ[key]
        for key in ('KUBERNETES_SERVICE_HOST', 'KUBERNETES_SERVICE_PORT')
        if os.environ.get(key)
    }
    in_cluster_ca_data: bytes | None = None
    in_cluster_load_error: str | None = None
    if in_cluster_available:
        if len(in_cluster_environment) != 2:
            in_cluster_load_error = (
                'In-cluster Kubernetes endpoint was unavailable when '
                'contexts were captured.')
        else:
            try:
                in_cluster_ca_data = _read_in_cluster_ca()
            except kubernetes.config.config_exception.ConfigException:
                # Discovery historically uses token presence. Preserve that
                # candidate and surface only a fixed failure if selected.
                in_cluster_load_error = (
                    'In-cluster configuration could not be captured safely.')
        if in_cluster_ca_data is None and in_cluster_load_error is None:
            # Discovery historically uses token presence. Preserve that
            # candidate and surface only a fixed failure if it is selected.
            in_cluster_load_error = (
                'In-cluster configuration could not be captured safely.')

    capture = _capture_kubeconfig_isolated(config_file)
    if capture.control_error is not None:
        control_error = capture.control_error
        # This public traceback boundary must retain neither the credential-free
        # result wrapper nor independently captured in-cluster client inputs.
        del (capture, config_file, in_cluster_ca_data, in_cluster_environment,
             in_cluster_load_error, in_cluster_namespace, in_cluster_name,
             in_cluster_available)
        raise control_error.with_traceback(None) from None

    available_context_names = capture.context_names
    if in_cluster_available:
        available_context_names += (in_cluster_name,)
    inventory = KubernetesContextInventory(
        available_context_names=available_context_names,
        kubeconfig_current_context_name=capture.current_context_name,
        in_cluster_available=in_cluster_available,
        in_cluster_context_name=in_cluster_name,
    )
    return KubernetesContextLoadSession(
        inventory=inventory,
        kubeconfig_snapshot=capture.kubeconfig_snapshot,
        unsupported_credential_modes=capture.unsupported_credential_modes,
        in_cluster_ca_data=in_cluster_ca_data,
        in_cluster_environment=in_cluster_environment,
        in_cluster_load_error=in_cluster_load_error,
        in_cluster_namespace=in_cluster_namespace,
    )


def core_api_from_api_client(  # pylint: disable=redefined-outer-name
        api_client: Any) -> Any:
    """Build an uncached CoreV1 facade without taking client ownership."""
    return kubernetes.client.CoreV1Api(api_client=api_client)


@contextlib.contextmanager
def in_cluster_core_and_apps_apis_for_token(
        token: str) -> typing.Iterator[tuple[Any, Any]]:
    """Yield Core/Apps facades authenticated by exactly ``token``.

    The explicit in-cluster load prevents kubeconfig fallback.  Comparing the
    installed bearer credential with the caller's mounted-token snapshot binds
    unverified JWT identity parsing to the credential Kubernetes authenticates.
    Refresh is disabled so projected-token rotation cannot decouple that
    identity from either API read during the activation transaction.
    """
    if not isinstance(token, str) or not token:
        raise ValueError('An in-cluster service-account token is required.')
    api_client_instance: Any | None = None
    try:
        api_client_instance = _get_api_client(in_cluster_context_name())
        configuration = getattr(api_client_instance, 'configuration', None)
        if configuration is None:
            raise kubernetes.config.config_exception.ConfigException(
                'In-cluster authentication configuration is missing.')
        api_keys = getattr(configuration, 'api_key', None)
        if not isinstance(api_keys, dict):
            raise kubernetes.config.config_exception.ConfigException(
                'In-cluster authentication keyring is malformed.')
        authorization = api_keys.get('authorization')
        if not isinstance(authorization, str):
            raise kubernetes.config.config_exception.ConfigException(
                'In-cluster authentication did not install a bearer token.')
        scheme, separator, configured_token = authorization.partition(' ')
        if (not separator or scheme.lower() != 'bearer' or
                not hmac.compare_digest(configured_token, token)):
            raise kubernetes.config.config_exception.ConfigException(
                'The mounted in-cluster token changed during client binding.')
        configuration_attributes = getattr(configuration, '__dict__', None)
        if not isinstance(configuration_attributes, dict):
            raise kubernetes.config.config_exception.ConfigException(
                'In-cluster authentication configuration is malformed.')
        # Some Kubernetes client versions bind the token loader directly onto
        # this instance.  Removing the override makes the class method read the
        # frozen api_key below; clearing only refresh_api_key_hook is not a
        # sufficient no-rotation guarantee on those versions.
        configuration_attributes.pop('get_api_key_with_prefix', None)
        configuration.refresh_api_key_hook = None
        frozen_authorization = api_keys.get('authorization')
        if (not isinstance(frozen_authorization, str) or
                not hmac.compare_digest(frozen_authorization, authorization)):
            raise kubernetes.config.config_exception.ConfigException(
                'In-cluster authentication changed while being frozen.')
        core = kubernetes.client.CoreV1Api(api_client=api_client_instance)
        apps = kubernetes.client.AppsV1Api(api_client=api_client_instance)
        del authorization, configured_token
        yield core, apps
    finally:
        if api_client_instance is not None:
            _close_api_client_resources(api_client_instance)


def _get_api_client(context: str | None = None) -> Any:
    """Get an ApiClient for the given context without modifying global config.

    This is fully thread-safe because it creates isolated Configuration
    objects for each client rather than modifying the global
    kubernetes.client.configuration.

    Args:
        context: The Kubernetes context to use. If None, tries in-cluster config
            first, then falls back to kubeconfig current-context.

    Returns:
        A kubernetes.client.ApiClient configured for the specified context.

    Raises:
        ValueError: If the configuration cannot be loaded.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _get_api_client_from_kubeconfig(context: str | None = None) -> Any:
        """Load kubeconfig, return ApiClient without modifying global state."""
        try:
            # new_client_from_config returns an ApiClient configured for the
            # specified context WITHOUT modifying the global configuration.
            # This is the key to thread-safety.
            return kubernetes.config.new_client_from_config(
                config_file=_get_config_file(), context=context)
        except kubernetes.config.config_exception.ConfigException as error:
            _raise_kubeconfig_load_error(context, error)

    if context == in_cluster_context_name() or context is None:
        try:
            # Load in-cluster config if running in a pod and context is None.
            # Use InClusterConfigLoader with an explicit Configuration object
            # to avoid modifying global state (thread-safe).
            #
            # Workaround: Kubernetes service discovery environment variables
            # may not show up in SkyPilot tasks. We set them to DNS names as
            # a fallback. See: github.com/skypilot-org/skypilot/issues/2287
            if 'KUBERNETES_SERVICE_HOST' not in os.environ:
                os.environ['KUBERNETES_SERVICE_HOST'] = 'kubernetes.default.svc'
            if 'KUBERNETES_SERVICE_PORT' not in os.environ:
                os.environ['KUBERNETES_SERVICE_PORT'] = '443'

            config = kubernetes.client.Configuration()
            kubernetes.config.load_incluster_config(config)
            return kubernetes.client.ApiClient(configuration=config)
        except kubernetes.config.config_exception.ConfigException:
            if context == in_cluster_context_name():
                # Explicitly requested in-cluster context but not in a cluster
                raise
            # Otherwise, if context is None, fall through to kubeconfig

    return _get_api_client_from_kubeconfig(context)


class _ExecCredentialResult(typing.NamedTuple):
    status: dict[str, Any] | None
    refresh_deadline_monotonic: float | None
    failure_message: str | None
    control_error: BaseException | None


class _BoundedCoreApiResult(typing.NamedTuple):
    core: Any | None
    refresh_deadline_monotonic: float | None
    failure_message: str | None
    control_error: BaseException | None


# The bounded credential path is entirely synchronous and contains no
# cancellation checkpoint. Its BaseException boundaries deliberately contain
# plugin/control failures until credentials are scrubbed or tracebacks detached.
# The narrow ASYNC103/104 suppressions below preserve that security contract.
class _ExecCredentialProcessState:
    """Owns cleanup if an unexpected failure escapes process collection."""

    def __init__(self) -> None:
        self.process: Any | None = None
        self.readers: list[threading.Thread] = []
        self.outputs: list[bytearray] = []

    def terminate_and_scrub(self) -> None:
        process = self.process
        if process is not None:
            try:
                _terminate_exec_credential_process_group(process)
            except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
                # Cleanup cannot replace the safe result.
                pass
            for pipe_name in ('stdout', 'stderr'):
                try:
                    pipe = getattr(process, pipe_name, None)
                    if pipe is not None:
                        pipe.close()
                except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
                    # Cleanup is deliberately best effort.
                    pass
        for reader in self.readers:
            try:
                reader.join(timeout=_EXEC_CREDENTIAL_TERMINATION_SECONDS)
            except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
                # A thread may have failed before start().
                pass
        for output in self.outputs:
            output.clear()
        self.release()

    def release(self) -> None:
        self.process = None
        self.readers.clear()
        self.outputs.clear()


def _detach_control_error(error: BaseException) -> BaseException:
    """Removes any credential-bearing call stack from a control exception."""
    error = error.with_traceback(None)
    error.__cause__ = None
    error.__context__ = None
    return error


def _capture_provider_fence(
        provider_fence: Callable[[], None]) -> BaseException | None:
    """Runs a fence without returning an exception's originating traceback."""
    try:
        provider_fence()
    except BaseException as error:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        # A drain/deadline error must retain its type.
        return _detach_control_error(error)  # noqa: ASYNC104
    return None


def _exec_credential_failure(message: str) -> _ExecCredentialResult:
    return _ExecCredentialResult(None, None, message, None)


def _exec_credential_control(error: BaseException) -> _ExecCredentialResult:
    return _ExecCredentialResult(None, None, None, _detach_control_error(error))


def _read_bounded_exec_credential_output(pipe: Any, output: bytearray,
                                         overflow: threading.Event,
                                         read_failure: threading.Event) -> None:
    """Reads one process pipe without retaining more than the hard limit."""
    try:
        while True:
            remaining = _MAX_EXEC_CREDENTIAL_OUTPUT_BYTES - len(output)
            if remaining == 0:
                if pipe.read(1):
                    overflow.set()
                return
            chunk = pipe.read(min(_EXEC_CREDENTIAL_IO_CHUNK_BYTES, remaining))
            if not chunk:
                return
            output.extend(chunk)
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        # A close caused by timeout/overflow may set this too. The caller gives
        # those earlier classifications priority, but an otherwise unexpected
        # read error must never be mistaken for clean EOF or reach
        # threading.excepthook with credential-bearing diagnostics.
        read_failure.set()


def _decode_exec_credential_output(output: bytearray) -> tuple[bool, Any]:
    """Decodes outside the caller's exception chain to discard raw payloads."""
    try:
        return True, json.loads(output.decode('utf-8'))
    except (UnicodeDecodeError, ValueError):
        return False, None


def _exec_credential_refresh_deadline(
        expiration: Any) -> tuple[bool, float | None]:
    """Converts one declared wall expiry into a stable monotonic deadline."""
    if expiration is None:
        return True, None
    if not isinstance(expiration, str) or not expiration:
        return False, None
    try:
        expiration_wall = kubernetes.config.kube_config.parse_rfc3339(
            expiration).timestamp()
    except (TypeError, ValueError, OverflowError):
        return False, None
    remaining = expiration_wall - time.time() - API_TIMEOUT
    if remaining <= 0:
        return False, None
    return True, time.monotonic() + remaining


def _credential_refresh_deadline_elapsed(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _signal_exec_credential_process_group(process: Any, *, force: bool) -> None:
    """Signals the isolated plugin tree without ever waiting unboundedly."""
    if platform.system() == 'Windows':
        command = ['taskkill', '/PID', str(process.pid), '/T']
        if force:
            command.append('/F')
        try:
            subprocess.run(command,
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL,
                           timeout=_EXEC_CREDENTIAL_TERMINATION_SECONDS,
                           check=False)
        except (OSError, subprocess.TimeoutExpired):
            try:
                if force:
                    process.kill()
                else:
                    process.terminate()
            except (OSError, ProcessLookupError):
                pass
        return
    try:
        os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
    except (OSError, ProcessLookupError):
        pass


def _terminate_exec_credential_process_group(process: Any) -> None:
    """Terminates the complete plugin group and reaps its direct child."""
    _signal_exec_credential_process_group(process, force=False)
    try:
        process.wait(timeout=_EXEC_CREDENTIAL_TERMINATION_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    # Signal the group even when the direct child exited after SIGTERM: a
    # descendant may still own an inherited stdout or stderr pipe.
    _signal_exec_credential_process_group(process, force=True)
    try:
        process.wait(timeout=_EXEC_CREDENTIAL_TERMINATION_SECONDS)
    except subprocess.TimeoutExpired:
        # SIGKILL should make this unreachable. Keep the worker bound even if a
        # platform process API fails to report the reaped child promptly.
        pass


def _run_bounded_exec_credential_isolated_impl(
        exec_config: Any, cluster: Any, cwd: str | None, *,
        timeout_seconds: float, provider_fence: Callable[[], None],
        process_state: _ExecCredentialProcessState) -> _ExecCredentialResult:
    """Runs and validates one plugin without raising from sensitive frames."""
    for key in ('command', 'apiVersion'):
        if key not in exec_config:
            return _exec_credential_failure(
                f'exec: malformed request. missing key {key!r}')
    if timeout_seconds <= 0:
        return _exec_credential_failure(
            'Kubernetes exec credential timeout must be positive.')
    try:
        args = [exec_config['command']]
        if exec_config.safe_get('args'):
            args.extend(exec_config['args'])
        env = os.environ.copy()
        if exec_config.safe_get('env'):
            for item in exec_config['env']:
                env[item['name']] = item['value']
        is_interactive = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
        exec_info: dict[str, Any] = {
            'apiVersion': exec_config['apiVersion'],
            'kind': 'ExecCredential',
            'spec': {
                'interactive': is_interactive,
            },
        }
        if exec_config.safe_get('provideClusterInfo'):
            cluster_value = cluster.value
            for extension in cluster_value.get('extensions', []):
                if extension.get('name') == 'client.authentication.k8s.io/exec':
                    cluster_value['config'] = extension.get('extension')
                    break
            exec_info['spec']['cluster'] = cluster_value
        env['KUBERNETES_EXEC_INFO'] = json.dumps(exec_info)
    except (KeyError, TypeError, ValueError):
        return _exec_credential_failure('exec: malformed request')
    control_error = _capture_provider_fence(provider_fence)
    if control_error is not None:
        return _exec_credential_control(control_error)
    is_windows = platform.system() == 'Windows'
    popen_kwargs: dict[str, Any] = {}
    if is_windows:
        popen_kwargs['creationflags'] = getattr(subprocess,
                                                'CREATE_NEW_PROCESS_GROUP', 0)
    else:
        popen_kwargs['start_new_session'] = True
    try:
        process = subprocess.Popen(args,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   stdin=sys.stdin if is_interactive else None,
                                   cwd=cwd or None,
                                   env=env,
                                   shell=is_windows,
                                   **popen_kwargs)
    except (OSError, TypeError, ValueError):
        return _exec_credential_failure(
            'exec: failed to start credential command')
    process_state.process = process
    assert process.stdout is not None
    assert process.stderr is not None
    stdout = bytearray()
    stderr = bytearray()
    process_state.outputs.extend((stdout, stderr))
    stdout_overflow = threading.Event()
    stderr_overflow = threading.Event()
    stdout_read_failure = threading.Event()
    stderr_read_failure = threading.Event()
    readers = [
        threading.Thread(target=_read_bounded_exec_credential_output,
                         args=(process.stdout, stdout, stdout_overflow,
                               stdout_read_failure),
                         name='kubernetes-exec-stdout',
                         daemon=True),
        threading.Thread(target=_read_bounded_exec_credential_output,
                         args=(process.stderr, stderr, stderr_overflow,
                               stderr_read_failure),
                         name='kubernetes-exec-stderr',
                         daemon=True),
    ]
    process_state.readers.extend(readers)
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    while True:
        if (stdout_overflow.is_set() or stderr_overflow.is_set() or
                stdout_read_failure.is_set() or stderr_read_failure.is_set()):
            break
        if process.poll() is not None and all(
                not reader.is_alive() for reader in readers):
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            break
        time.sleep(min(_EXEC_CREDENTIAL_POLL_SECONDS, remaining))
    if (timed_out or stdout_overflow.is_set() or stderr_overflow.is_set() or
            stdout_read_failure.is_set() or stderr_read_failure.is_set()):
        _terminate_exec_credential_process_group(process)
    for reader in readers:
        reader.join(timeout=_EXEC_CREDENTIAL_TERMINATION_SECONDS)
    if timed_out:
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: credential command exceeded its bounded timeout')
    if stdout_overflow.is_set():
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: credential response exceeds 1 MiB')
    if stderr_overflow.is_set():
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: credential diagnostics exceed 1 MiB')
    if stdout_read_failure.is_set() or stderr_read_failure.is_set():
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: failed to read credential process output')
    if process.returncode != 0:
        # Exec plugin diagnostics may contain credential material. Preserve only
        # the bounded status, never raw stderr, in the exception surface.
        returncode = process.returncode
        stdout.clear()
        stderr.clear()
        if isinstance(returncode, int):
            return _exec_credential_failure(
                f'exec: process returned {returncode}')
        return _exec_credential_failure('exec: credential process failed')
    decoded, payload = _decode_exec_credential_output(stdout)
    if not decoded:
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure('exec: failed to decode process output')
    if not isinstance(payload, dict):
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure('exec: malformed response object')
    for key in ('apiVersion', 'kind', 'status'):
        if key not in payload:
            payload.clear()
            stdout.clear()
            stderr.clear()
            return _exec_credential_failure(
                f'exec: malformed response. missing key {key!r}')
    if payload['apiVersion'] != exec_config['apiVersion']:
        payload.clear()
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: response api version does not match request')
    if payload['kind'] != 'ExecCredential':
        payload.clear()
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: response kind is not ExecCredential')
    status = payload['status']
    if not isinstance(status, dict):
        payload.clear()
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure('exec: malformed response status')
    token = status.get('token')
    certificate = status.get('clientCertificateData')
    key = status.get('clientKeyData')
    has_token = isinstance(token, str) and bool(token)
    has_any_certificate = certificate is not None or key is not None
    has_complete_certificate = (isinstance(certificate, str) and
                                bool(certificate) and isinstance(key, str) and
                                bool(key))
    if (not has_token and
            not has_complete_certificate) or (has_any_certificate and
                                              not has_complete_certificate):
        status.clear()
        payload.clear()
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: missing token or complete client certificate data')
    expiration_valid, refresh_deadline = _exec_credential_refresh_deadline(
        status.get('expirationTimestamp'))
    if not expiration_valid:
        status.clear()
        payload.clear()
        stdout.clear()
        stderr.clear()
        return _exec_credential_failure(
            'exec: unusable credential expiration timestamp')
    # The validated status is the only credential-bearing object allowed to
    # leave this frame on success. Raw byte buffers and the enclosing payload
    # are destroyed before the final drain/deadline fence.
    del payload['status']
    payload.clear()
    stdout.clear()
    stderr.clear()
    control_error = _capture_provider_fence(provider_fence)
    if control_error is not None:
        status.clear()
        return _exec_credential_control(control_error)
    return _ExecCredentialResult(status, refresh_deadline, None, None)


def _run_bounded_exec_credential_isolated(
        exec_config: Any, cluster: Any, cwd: str | None, *,
        timeout_seconds: float,
        provider_fence: Callable[[], None]) -> _ExecCredentialResult:
    """Total isolation wrapper for credential-bearing implementation state."""
    process_state = _ExecCredentialProcessState()
    try:
        result = _run_bounded_exec_credential_isolated_impl(
            exec_config,
            cluster,
            cwd,
            timeout_seconds=timeout_seconds,
            provider_fence=provider_fence,
            process_state=process_state)
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        # Never expose the sensitive inner frame.
        process_state.terminate_and_scrub()
        return _exec_credential_failure(  # noqa: ASYNC104
            'exec: credential command failed inside isolation boundary')
    process_state.release()
    return result


def _bounded_core_api_failure(message: str) -> _BoundedCoreApiResult:
    return _BoundedCoreApiResult(None, None, message, None)


def _bounded_core_api_control(error: BaseException) -> _BoundedCoreApiResult:
    return _BoundedCoreApiResult(None, None, None, _detach_control_error(error))


def _scrub_bounded_kubeconfig_state(status: dict[str, Any] | None,
                                    user: Any | None,
                                    configuration: Any | None) -> None:
    """Best-effort erases credentials before a failed isolation result."""
    if status is not None:
        status.clear()
    user_value = getattr(user, 'value', None)
    if isinstance(user_value, dict):
        for key in ('exec', 'token', 'clientCertificateData', 'clientKeyData'):
            user_value.pop(key, None)
    _scrub_kubernetes_configuration_credentials(configuration)


def _scrub_bounded_api_client_credentials(client_api: Any | None) -> None:
    """Best-effort removes installed credentials from a retired API client."""
    if client_api is None:
        return
    configuration = _best_effort_getattr(client_api, 'configuration')
    if configuration is not None:
        try:
            _scrub_kubernetes_configuration_credentials(configuration)
        except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            pass
    _best_effort_clear_dict_attribute(client_api, 'default_headers')
    _best_effort_setattr(client_api, 'cookie', None)
    try:
        _scrub_kubernetes_api_client_transport(client_api)
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        pass


def _close_bounded_api_client(client_api: Any | None) -> None:
    if client_api is None:
        return
    _close_api_client_resources(client_api)


def _close_bounded_core(core: Any | None) -> None:
    if core is not None:
        _close_bounded_api_client(getattr(core, 'api_client', None))


def _bounded_core_api_isolated_impl(
        context: str, *, exec_credential_timeout_seconds: float,
        provider_fence: Callable[[], None]) -> _BoundedCoreApiResult:
    """Builds a client while keeping credential state out of exceptions."""
    control_error = _capture_provider_fence(provider_fence)
    if control_error is not None:
        return _bounded_core_api_control(control_error)
    if context == in_cluster_context_name():
        client_api: Any | None = None
        try:
            client_api = _get_api_client(context)
            client_api.configuration.refresh_api_key_hook = None
            in_cluster_core = kubernetes.client.CoreV1Api(api_client=client_api)
            in_cluster_refresh_deadline = (
                time.monotonic() + _IN_CLUSTER_CREDENTIAL_REFRESH_SECONDS)
        except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            _close_bounded_api_client(client_api)
            return _bounded_core_api_failure(  # noqa: ASYNC104
                'Failed to load bounded in-cluster Kubernetes credentials.')
        control_error = _capture_provider_fence(provider_fence)
        if control_error is not None:
            _close_bounded_core(in_cluster_core)
            return _bounded_core_api_control(control_error)
        if _credential_refresh_deadline_elapsed(in_cluster_refresh_deadline):
            _close_bounded_core(in_cluster_core)
            return _bounded_core_api_failure(
                'Kubernetes credential expired before client admission.')
        return _BoundedCoreApiResult(in_cluster_core,
                                     in_cluster_refresh_deadline, None, None)

    status: dict[str, Any] | None = None
    user: Any | None = None
    configuration: Any | None = None
    core: Any | None = None
    try:
        kube_config = kubernetes.config.kube_config
        loader = kube_config._get_kube_config_loader_for_yaml_file(  # pylint: disable=protected-access
            _get_config_file(),
            active_context=context)
        user = loader._user  # pylint: disable=protected-access
        if user is None:
            return _bounded_core_api_failure(
                'Kubeconfig context has no user credentials.')
        if 'auth-provider' in user:
            return _bounded_core_api_failure(
                'Bounded EKS canaries do not support kubeconfig auth-provider '
                'credential commands.')
        refresh_deadline: float | None = None
        if 'exec' in user:
            base_path = loader._get_base_path(  # pylint: disable=protected-access
                loader._cluster.path)  # pylint: disable=protected-access
            exec_result = _run_bounded_exec_credential_isolated(
                user['exec'],
                loader._cluster,  # pylint: disable=protected-access
                base_path,
                timeout_seconds=exec_credential_timeout_seconds,
                provider_fence=provider_fence)
            if exec_result.control_error is not None:
                return _bounded_core_api_control(exec_result.control_error)
            if exec_result.failure_message is not None:
                return _bounded_core_api_failure(exec_result.failure_message)
            assert exec_result.status is not None
            status = exec_result.status
            refresh_deadline = exec_result.refresh_deadline_monotonic
            del user.value['exec']
            token = status.get('token')
            if isinstance(token, str) and token:
                user.value['token'] = token
            else:
                loader.cert_file = kube_config.FileOrData(  # pylint: disable=protected-access
                    status,
                    None,
                    data_key_name='clientCertificateData',
                    file_base_path=base_path,
                    base64_file_content=False,
                    temp_file_path=loader._temp_file_path).as_file()  # pylint: disable=protected-access
                loader.key_file = kube_config.FileOrData(  # pylint: disable=protected-access
                    status,
                    None,
                    data_key_name='clientKeyData',
                    file_base_path=base_path,
                    base64_file_content=False,
                    temp_file_path=loader._temp_file_path).as_file()  # pylint: disable=protected-access
        configuration = kubernetes.client.Configuration()
        loader.load_and_set(configuration)
        # The wrapper owns refresh. The library may not execute kubeconfig code
        # between a drain/deadline fence and a raw API call.
        configuration.refresh_api_key_hook = None
        core = kubernetes.client.CoreV1Api(
            api_client=kubernetes.client.ApiClient(configuration=configuration))
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        _close_bounded_core(core)
        _scrub_bounded_kubeconfig_state(status, user, configuration)
        return _bounded_core_api_failure(  # noqa: ASYNC104
            'Failed to install bounded Kubernetes credentials.')
    if status is not None:
        status.clear()
        status = None
    control_error = _capture_provider_fence(provider_fence)
    if control_error is not None:
        _close_bounded_core(core)
        _scrub_bounded_kubeconfig_state(status, user, configuration)
        return _bounded_core_api_control(control_error)
    if _credential_refresh_deadline_elapsed(refresh_deadline):
        _close_bounded_core(core)
        _scrub_bounded_kubeconfig_state(status, user, configuration)
        return _bounded_core_api_failure(
            'Kubernetes credential expired before client admission.')
    return _BoundedCoreApiResult(core, refresh_deadline, None, None)


def _bounded_core_api_isolated(
        context: str, *, exec_credential_timeout_seconds: float,
        provider_fence: Callable[[], None]) -> _BoundedCoreApiResult:
    """Total isolation and post-failure fence for bounded client creation."""
    try:
        result = _bounded_core_api_isolated_impl(
            context,
            exec_credential_timeout_seconds=exec_credential_timeout_seconds,
            provider_fence=provider_fence)
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        # Never expose the sensitive inner frame.
        result = _bounded_core_api_failure(
            'Failed to build Kubernetes client inside isolation boundary.')
    if result.core is None and result.control_error is None:
        control_error = _capture_provider_fence(provider_fence)
        if control_error is not None:
            return _bounded_core_api_control(control_error)
    return result


def _bounded_core_api(
        context: str, *, exec_credential_timeout_seconds: float,
        provider_fence: Callable[[], None]) -> tuple[Any, float | None]:
    """Builds one CoreV1Api without a transparent unbounded exec refresh."""
    result = _bounded_core_api_isolated(
        context,
        exec_credential_timeout_seconds=exec_credential_timeout_seconds,
        provider_fence=provider_fence)
    if result.control_error is not None:
        raise result.control_error.with_traceback(None) from None
    if result.failure_message is not None:
        config_error_cls = kubernetes.config.config_exception.ConfigException
        raise config_error_cls(result.failure_message) from None
    assert result.core is not None
    return result.core, result.refresh_deadline_monotonic


class ProviderFencedCoreApi:
    """Refreshes bounded kubeconfig credentials before a fenced raw API call."""

    def __init__(self, context: str, *, exec_credential_timeout_seconds: float,
                 provider_fence: Callable[[], None]) -> None:
        self._context = context
        self._exec_credential_timeout_seconds = (
            exec_credential_timeout_seconds)
        self._refresh_lock = threading.Lock()
        client, refresh_deadline = _bounded_core_api(
            context,
            exec_credential_timeout_seconds=exec_credential_timeout_seconds,
            provider_fence=provider_fence)
        if _credential_refresh_deadline_elapsed(refresh_deadline):
            _close_bounded_core(client)
            client = None
            raise kubernetes.config.config_exception.ConfigException(
                'Kubernetes credential expired before client admission.'
            ) from None
        self._client = client
        self._credential_refresh_deadline = refresh_deadline
        self._last_refresh_monotonic = time.monotonic()

    @property
    def api_client(self) -> Any:
        if self._client is None:
            raise RuntimeError(
                'Provider-fenced Kubernetes client requires refresh.') from None
        return self._client.api_client

    def _should_refresh(self) -> bool:
        if self._client is None:
            return True
        interval = _get_kubeconfig_refresh_interval_seconds()
        now = time.monotonic()
        if (interval > 0 and now - self._last_refresh_monotonic >= interval):
            return True
        return (self._credential_refresh_deadline is not None and
                now >= self._credential_refresh_deadline)

    @staticmethod
    def _close(client: Any) -> None:
        if client is None:
            return
        try:
            client_api = getattr(client, 'api_client', None)
        except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            if logger is not None:
                logger.debug('Error closing provider-fenced Kubernetes client.')
            return  # noqa: ASYNC104
        if client_api is not None:
            _close_api_client_resources(client_api)

    def _invalidate(self) -> None:
        """Makes every object reachable from this wrapper credential-free."""
        client = self._client
        self._client = None
        self._credential_refresh_deadline = None
        self._close(client)

    def close(self) -> None:
        """Closes and scrubs the installed client. This method is idempotent."""
        self._invalidate()

    def _refresh(self, provider_fence: Callable[[], None]) -> None:
        if not self._should_refresh():
            return
        with self._refresh_lock:
            if not self._should_refresh():
                return
            build_result = _bounded_core_api_isolated(
                self._context,
                exec_credential_timeout_seconds=(
                    self._exec_credential_timeout_seconds),
                provider_fence=provider_fence)
            if build_result.control_error is not None:
                control_error = build_result.control_error
                del build_result
                self._invalidate()
                raise control_error.with_traceback(None) from None
            if build_result.failure_message is not None:
                failure_message = build_result.failure_message
                del build_result
                self._invalidate()
                config_error_cls = (
                    kubernetes.config.config_exception.ConfigException)
                raise config_error_cls(failure_message) from None
            assert build_result.core is not None
            new_client = build_result.core
            refresh_deadline = build_result.refresh_deadline_monotonic
            del build_result
            post_build_control_error = _capture_provider_fence(provider_fence)
            if post_build_control_error is not None:
                self._close(new_client)
                new_client = None
                self._invalidate()
                raise post_build_control_error.with_traceback(None) from None
            if _credential_refresh_deadline_elapsed(refresh_deadline):
                self._close(new_client)
                new_client = None
                self._invalidate()
                config_error_cls = (
                    kubernetes.config.config_exception.ConfigException)
                raise config_error_cls(
                    'Kubernetes credential expired before client admission.'
                ) from None
            old_client = self._client
            self._client = new_client
            self._credential_refresh_deadline = refresh_deadline
            self._last_refresh_monotonic = time.monotonic()
            self._close(old_client)

    def call_with_provider_fence(self, method_name: str,
                                 provider_fence: Callable[[], None],
                                 on_start: Callable[[], None] | None, *args:
                                 Any, **kwargs: Any) -> Any:
        control_error = _capture_provider_fence(provider_fence)
        if control_error is not None:
            self._invalidate()
            raise control_error.with_traceback(None) from None
        refresh_error: BaseException | None = None
        try:
            self._refresh(provider_fence)
        except BaseException as error:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            refresh_error = _detach_control_error(error)
        if refresh_error is not None:
            self._invalidate()
            raise refresh_error.with_traceback(None) from None
        control_error = _capture_provider_fence(provider_fence)
        if control_error is not None:
            self._invalidate()
            raise control_error.with_traceback(None) from None
        if _credential_refresh_deadline_elapsed(
                self._credential_refresh_deadline):
            self._invalidate()
            config_error_cls = (
                kubernetes.config.config_exception.ConfigException)
            raise config_error_cls(
                'Kubernetes credential expired before provider call admission.'
            ) from None
        assert self._client is not None
        start_error: BaseException | None = None
        method: Any = None
        try:
            method = getattr(self._client, method_name)
            if not callable(method):
                raise TypeError(
                    f'Provider attribute {method_name!r} is not callable.')
            if on_start is not None:
                on_start()
        except BaseException as error:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            start_error = _detach_control_error(error)
        if start_error is not None:
            method = None
            args = ()
            kwargs = {}
            self._invalidate()
            raise start_error.with_traceback(None) from None
        if _credential_refresh_deadline_elapsed(
                self._credential_refresh_deadline):
            method = None
            args = ()
            kwargs = {}
            self._invalidate()
            config_error_cls = (
                kubernetes.config.config_exception.ConfigException)
            raise config_error_cls(
                'Kubernetes credential expired before provider call admission.'
            ) from None
        method_error: BaseException | None = None
        result: Any = None
        try:
            result = method(*args, **kwargs)
        except BaseException as error:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            method_error = _detach_control_error(error)
        if method_error is not None:
            control_error = _capture_provider_fence(provider_fence)
            if control_error is not None:
                method_error = None
                method = None
                args = ()
                kwargs = {}
                self._invalidate()
                raise control_error.with_traceback(None) from None
            method = None
            args = ()
            kwargs = {}
            self._invalidate()
            raise method_error.with_traceback(None) from None
        control_error = _capture_provider_fence(provider_fence)
        if control_error is not None:
            result = None
            method = None
            args = ()
            kwargs = {}
            self._invalidate()
            raise control_error.with_traceback(None) from None
        return result

    def __getattr__(self, name: str) -> Any:
        if self._client is None:
            raise AttributeError(name)
        return getattr(self._client, name)

    def __del__(self) -> None:
        client = self.__dict__.get('_client')
        if client is not None:
            self._close(client)


def provider_fenced_core_api(
        context: str, *, exec_credential_timeout_seconds: float,
        provider_fence: Callable[[], None]) -> ProviderFencedCoreApi:
    """Returns the canary-only bounded and dynamically fenced CoreV1 client."""
    return ProviderFencedCoreApi(
        context,
        exec_credential_timeout_seconds=exec_credential_timeout_seconds,
        provider_fence=provider_fence)


def list_kube_config_contexts():
    return kubernetes.config.list_kube_config_contexts(_get_config_file())


@functools.cache
def _get_kubeconfig_refresh_interval_seconds() -> float:
    """Parse refresh interval from env; 0 means disabled.

    Result is cached because this is called on every k8s API method invocation
    and the env var is not expected to change at runtime.
    """
    raw = os.environ.get(KUBECONFIG_REFRESH_INTERVAL_ENV_VAR, '').strip()
    if not raw:
        return 0.0
    try:
        val = float(raw)
        return max(0.0, val)
    except ValueError:
        logger.warning(
            f'Invalid value for {KUBECONFIG_REFRESH_INTERVAL_ENV_VAR}: '
            f'"{raw}". Expected a numeric value. Disabling client '
            'refresh interval.')
        return 0.0


class RetryableClientWrapper:
    """Wrap a kubernetes client for interval-based refresh and resource cleanup.

    Each wrapper tracks its own last-refresh time and refreshes only its
    underlying client when the configured interval has elapsed, without
    invalidating other wrappers or global caches. Closes the underlying
    ApiClient on GC to release external resources (e.g. semaphores).
    """

    def __init__(self, client: Any, getter: Callable, getter_args: tuple,
                 getter_kwargs: dict):
        self._client = client
        self._getter = getter
        self._getter_args = getter_args
        self._getter_kwargs = getter_kwargs
        self._last_refresh_time = time.time()
        self._refresh_lock = threading.Lock()

    def _should_refresh(self) -> bool:
        """True if this wrapper's refresh interval has elapsed."""
        interval = _get_kubeconfig_refresh_interval_seconds()
        if interval <= 0:
            return False
        return (time.time() - self._last_refresh_time) >= interval

    def _close_client(self, client: Any) -> None:
        """Close the underlying ApiClient to release external resources."""
        try:
            real_client = None
            if isinstance(client, kubernetes.client.ApiClient):
                real_client = client
            elif isinstance(client, kubernetes.watch.Watch):
                real_client = getattr(client, '_api_client', None)
            else:
                # Typed clients (CoreV1Api etc.) are codegen'd and all have
                # an 'api_client' attribute pointing to the real ApiClient.
                real_client = getattr(client, 'api_client', None)
            if real_client is not None:
                real_client.close()
            else:
                # logger may already be cleaned up during __del__ at shutdown
                if logger is not None:
                    logger.debug(f'No client found for {client}')
        except Exception as e:  # pylint: disable=broad-except
            if logger is not None:
                logger.debug(f'Error closing Kubernetes client: {e}')

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        @functools.wraps(attr)
        def with_refresh(*args, **kwargs):
            if self._should_refresh():
                with self._refresh_lock:
                    if self._should_refresh():
                        logger.debug(
                            'Refreshing Kubernetes client from kubeconfig '
                            'due to interval expiry.')
                        old_client = self._client
                        self._client = self._getter(*self._getter_args,
                                                    **self._getter_kwargs)
                        self._last_refresh_time = time.time()
                        self._close_client(old_client)
            method = getattr(self._client, name)
            return method(*args, **kwargs)

        # Cache on the instance so repeated accesses to the same method name
        # return the same closure without going through __getattr__ again.
        # The closure always reads self._client at call time, so it stays
        # correct after a client refresh.
        self.__dict__[name] = with_refresh
        return with_refresh

    def __del__(self):
        self._close_client(self._client)


def _retryable_kubernetes_client(getter: Callable) -> Callable:
    """Wrap a kubernetes client getter in a RetryableClientWrapper.

    On each call the getter is invoked to obtain the raw client, which is then
    wrapped so it can be transparently refreshed when the configured kubeconfig
    refresh interval has elapsed.
    """

    @functools.wraps(getter)
    def wrapper(*args: Any, **kwargs: Any) -> RetryableClientWrapper:
        client = getter(*args, **kwargs)
        return RetryableClientWrapper(client, getter, args, kwargs)

    return wrapper


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def core_api(context: str | None = None):
    return kubernetes.client.CoreV1Api(api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def storage_api(context: str | None = None):
    return kubernetes.client.StorageV1Api(api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def auth_api(context: str | None = None):
    return kubernetes.client.RbacAuthorizationV1Api(
        api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def authz_api(context: str | None = None):
    # AuthorizationV1Api exposes SelfSubjectAccessReview, unlike auth_api()
    # (RbacAuthorizationV1Api). Used for startup RBAC preflight checks.
    return kubernetes.client.AuthorizationV1Api(
        api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def networking_api(context: str | None = None):
    return kubernetes.client.NetworkingV1Api(
        api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def custom_objects_api(context: str | None = None):
    return kubernetes.client.CustomObjectsApi(
        api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def node_api(context: str | None = None):
    return kubernetes.client.NodeV1Api(api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def apps_api(context: str | None = None):
    return kubernetes.client.AppsV1Api(api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def policy_api(context: str | None = None):
    return kubernetes.client.PolicyV1Api(api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def batch_api(context: str | None = None):
    return kubernetes.client.BatchV1Api(api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def api_client(context: str | None = None):
    return _get_api_client(context)


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def custom_resources_api(context: str | None = None):
    return kubernetes.client.CustomObjectsApi(
        api_client=_get_api_client(context))


@_api_logging_decorator('urllib3', logging.ERROR)
@annotations.lru_cache(scope='request')
@_retryable_kubernetes_client
def watch(context: str | None = None):
    w = kubernetes.watch.Watch()
    w._api_client = _get_api_client(context)  # pylint: disable=protected-access
    return w


def api_exception():
    return kubernetes.client.rest.ApiException


def config_exception():
    return kubernetes.config.config_exception.ConfigException


def max_retry_error():
    return urllib3.exceptions.MaxRetryError


def stream():
    return kubernetes.stream.stream


def in_cluster_context_name() -> str:
    """Returns the name of the in-cluster context from the environment.

    If the environment variable is not set, returns the default in-cluster
    context name.
    """
    return (os.environ.get(IN_CLUSTER_CONTEXT_NAME_ENV_VAR) or
            DEFAULT_IN_CLUSTER_REGION)


def in_cluster_identity() -> list[str]:
    """Returns the cluster owner identity for contexts launched with
    in-cluster authentication."""
    return [normalize_kubernetes_in_cluster_identity(in_cluster_context_name())]
