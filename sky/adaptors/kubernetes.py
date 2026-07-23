"""Kubernetes adaptors

Thread safety notes:

The API functions (core_api, batch_api, etc.) return cached clients that are
created with context-specific ApiClient instances.

Set SKYPILOT_KUBECONFIG_REFRESH_INTERVAL_SECONDS (seconds) to refresh the
client proactively at a fixed interval so it is rebuilt from the updated
kubeconfig (e.g. for short-lived certs).
"""
from collections.abc import Callable
import functools
import json
import logging
import os
import platform
import signal
import subprocess
import sys
import threading
import time
import typing
from typing import Any

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
# If set (positive seconds), client is refreshed proactively after this
# interval.
KUBECONFIG_REFRESH_INTERVAL_ENV_VAR = (
    'SKYPILOT_KUBECONFIG_REFRESH_INTERVAL_SECONDS')
_MAX_EXEC_CREDENTIAL_OUTPUT_BYTES = 1024 * 1024
_EXEC_CREDENTIAL_IO_CHUNK_BYTES = 64 * 1024
_EXEC_CREDENTIAL_POLL_SECONDS = 0.05
_EXEC_CREDENTIAL_TERMINATION_SECONDS = 1
_IN_CLUSTER_CREDENTIAL_REFRESH_SECONDS = 60

logger = sky_logging.init_logger(__name__)


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
        except kubernetes.config.config_exception.ConfigException as e:
            suffix = common_utils.format_exception(e, use_bracket=True)
            context_name = '(current-context)' if context is None else context
            is_ssh_node_pool = False
            if context_name.startswith('ssh-'):
                context_name = common_utils.removeprefix(context_name, 'ssh-')
                is_ssh_node_pool = True
            # Check if exception was due to no current-context
            if 'Expected key current-context' in str(e):
                if is_ssh_node_pool:
                    context_name = common_utils.removeprefix(
                        context_name, 'ssh-')
                    err_str = (
                        'Failed to load SSH Node Pool configuration for '
                        f'{context_name!r}.\n'
                        f'    Run `sky ssh up --infra {context_name}` to '
                        'set up or repair the cluster.')
                else:
                    err_str = (
                        'Failed to load Kubernetes configuration for '
                        f'{context_name!r}. '
                        'Kubeconfig does not contain any valid context(s).'
                        f'\n{suffix}\n'
                        '    If you were running a local Kubernetes '
                        'cluster, run `sky local up` to start the cluster.')
            else:
                kubeconfig_path = os.environ.get('KUBECONFIG', '~/.kube/config')
                if is_ssh_node_pool:
                    err_str = (
                        f'Failed to load SSH Node Pool configuration for '
                        f'{context_name!r}. Run `sky ssh up --infra '
                        f'{context_name}` to set up or repair the cluster.')
                else:
                    err_str = (
                        'Failed to load Kubernetes configuration for '
                        f'{context_name!r}. Please check if your kubeconfig '
                        f'file exists at {kubeconfig_path} and is valid.'
                        f'\n{suffix}\n')
            if is_ssh_node_pool:
                err_str += (f'\nTo disable SSH Node Pool {context_name!r}: '
                            'run `sky check`.')
            else:
                err_str += (
                    '\nHint: Kubernetes attempted to query the current-context '
                    'set in kubeconfig. Check if the current-context is valid.')
            with ux_utils.print_exception_no_traceback():
                raise ValueError(err_str) from None

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
    if configuration is not None:
        api_key = getattr(configuration, 'api_key', None)
        if isinstance(api_key, dict):
            api_key.clear()
        for name in ('cert_file', 'key_file'):
            if hasattr(configuration, name):
                setattr(configuration, name, None)


def _scrub_bounded_api_client_credentials(client_api: Any | None) -> None:
    """Best-effort removes installed credentials from a retired API client."""
    if client_api is None:
        return
    try:
        configuration = getattr(client_api, 'configuration', None)
        if configuration is not None:
            for name in ('api_key', 'api_key_prefix'):
                value = getattr(configuration, name, None)
                if isinstance(value, dict):
                    value.clear()
            for name in ('username', 'password', 'cert_file', 'key_file'):
                if hasattr(configuration, name):
                    setattr(configuration, name, None)
            if hasattr(configuration, 'refresh_api_key_hook'):
                configuration.refresh_api_key_hook = None
        headers = getattr(client_api, 'default_headers', None)
        if isinstance(headers, dict):
            for name in tuple(headers):
                if str(name).lower() in {
                        'authorization', 'cookie', 'proxy-authorization'
                }:
                    headers.pop(name, None)
        if hasattr(client_api, 'cookie'):
            client_api.cookie = None
    except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
        # Scrubbing must not replace the fixed failure or control result.
        pass


def _close_bounded_api_client(client_api: Any | None) -> None:
    if client_api is None:
        return
    _scrub_bounded_api_client_credentials(client_api)
    try:
        client_api.close()
    except Exception:  # pylint: disable=broad-except
        pass


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
        _scrub_bounded_api_client_credentials(client_api)
        try:
            if client_api is not None:
                client_api.close()
        except BaseException:  # pylint: disable=broad-exception-caught  # noqa: ASYNC103
            if logger is not None:
                logger.debug('Error closing provider-fenced Kubernetes client.')

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
    return [f'{IN_CLUSTER_IDENTITY_PREFIX}-{in_cluster_context_name()}']
