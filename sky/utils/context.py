"""SkyPilot context for threads and coroutines."""

import asyncio
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import MutableMapping
import contextvars
import copy
import fcntl
import functools
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import threading
from typing import Any, NamedTuple, TextIO, TYPE_CHECKING, TypeVar
import uuid

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    pass

_PROCESS_GLOBAL_VARS = {}

_logger = logging.getLogger(__name__)

REQUEST_LOG_TRUNCATION_MARKER_PREFIX = (
    '[SkyPilot] Earlier request output was truncated; generation=')
_REQUEST_LOG_TRUNCATION_GENERATION_LENGTH = 32
_REQUEST_LOG_TRUNCATION_START_SEPARATOR = '; start='
_REQUEST_LOG_TRUNCATION_START_LENGTH = 16
REQUEST_LOG_TRUNCATION_MARKER_MAX_BYTES = len(
    REQUEST_LOG_TRUNCATION_MARKER_PREFIX.encode('utf-8')) + (
        _REQUEST_LOG_TRUNCATION_GENERATION_LENGTH +
        len(_REQUEST_LOG_TRUNCATION_START_SEPARATOR.encode('utf-8')) +
        _REQUEST_LOG_TRUNCATION_START_LENGTH + 1)
REQUEST_LOG_DISK_PRESSURE_MARKER = (
    '[SkyPilot] Request output stopped because the API server filesystem '
    'reached its reserved free-space limit. Retry after old request logs are '
    'cleaned up.\n')
_DEFAULT_DISK_CHECK_INTERVAL_BYTES = 1024 * 1024


class RequestLogTruncationMarker(NamedTuple):
    """Metadata needed to map a follower across an in-place rollover."""

    generation: str
    logical_start: int
    byte_length: int


def parse_request_log_truncation_marker(
        prefix: bytes) -> RequestLogTruncationMarker | None:
    """Parse a bounded request-log marker, if present."""
    marker_prefix = REQUEST_LOG_TRUNCATION_MARKER_PREFIX.encode('utf-8')
    if not prefix.startswith(marker_prefix):
        return None
    marker_line, separator, _ = prefix.partition(b'\n')
    if not separator:
        return None
    encoded_metadata = marker_line[len(marker_prefix):]
    start_separator = _REQUEST_LOG_TRUNCATION_START_SEPARATOR.encode('utf-8')
    encoded_generation, separator, encoded_start = encoded_metadata.partition(
        start_separator)
    if not separator:
        return None
    if len(encoded_generation) != _REQUEST_LOG_TRUNCATION_GENERATION_LENGTH:
        return None
    if len(encoded_start) != _REQUEST_LOG_TRUNCATION_START_LENGTH:
        return None
    try:
        generation = encoded_generation.decode('ascii')
        int(generation, 16)
        logical_start = int(encoded_start, 16)
    except (UnicodeDecodeError, ValueError):
        return None
    return RequestLogTruncationMarker(generation, logical_start,
                                      len(marker_line) + 1)


def _format_request_log_truncation_marker(logical_start: int) -> str:
    if logical_start < 0 or logical_start >= 1 << 64:
        raise ValueError('request log logical offset is out of range')
    return (REQUEST_LOG_TRUNCATION_MARKER_PREFIX + uuid.uuid4().hex +
            _REQUEST_LOG_TRUNCATION_START_SEPARATOR + f'{logical_start:016x}\n')


class _TruncatingLogFile:
    """Append-only text stream that bounds an actively streamed log file."""

    def __init__(
            self,
            path: pathlib.Path,
            max_bytes: int,
            min_free_bytes: int | None = None,
            disk_check_interval_bytes: int = _DEFAULT_DISK_CHECK_INTERVAL_BYTES
    ):
        if max_bytes <= REQUEST_LOG_TRUNCATION_MARKER_MAX_BYTES:
            raise ValueError('max_bytes must be larger than the truncation '
                             'marker')
        if min_free_bytes is not None and min_free_bytes < 0:
            raise ValueError('min_free_bytes must be non-negative')
        if disk_check_interval_bytes <= 0:
            raise ValueError('disk_check_interval_bytes must be positive')
        self._path = path
        self._file = open(path, 'a+', encoding='utf-8')
        self._max_bytes = max_bytes
        self._min_free_bytes = min_free_bytes
        self._disk_check_interval_bytes = disk_check_interval_bytes
        # Check the filesystem on the first write, then only once per bounded
        # amount of output. This keeps the healthy path cheap while bounding
        # concurrent overshoot when the filesystem approaches its reserve.
        self._bytes_since_disk_check = disk_check_interval_bytes
        self._disk_pressure_reached = False
        # Leave substantial headroom after a rollover. Retaining all the way
        # back to the cap would make every subsequent small write rewrite the
        # entire production-sized log while holding the file lock.
        minimum_rollover_bytes = min(
            max_bytes, REQUEST_LOG_TRUNCATION_MARKER_MAX_BYTES * 2)
        self._rollover_bytes = max(minimum_rollover_bytes, max_bytes // 2)
        self._size_bytes = path.stat().st_size
        self._lock = threading.Lock()

    def _filesystem_under_pressure(self, incoming_bytes: int) -> bool:
        if self._min_free_bytes is None:
            return False
        self._bytes_since_disk_check += incoming_bytes
        if self._bytes_since_disk_check < self._disk_check_interval_bytes:
            return False
        self._bytes_since_disk_check %= self._disk_check_interval_bytes
        # Include this pending write in the decision so a single large output
        # chunk cannot cross the reserve after observing a healthy snapshot.
        return (shutil.disk_usage(self._path.parent).free - incoming_bytes <
                self._min_free_bytes)

    def _replace_with_disk_pressure_marker(self, fd: int) -> None:
        """Release this spool's payload and leave a follower-visible marker."""
        prefix = os.pread(fd, REQUEST_LOG_TRUNCATION_MARKER_MAX_BYTES, 0)
        previous_marker = parse_request_log_truncation_marker(prefix)
        payload_offset = (previous_marker.byte_length
                          if previous_marker is not None else 0)
        logical_start = (previous_marker.logical_start
                         if previous_marker is not None else 0)
        logical_end = logical_start + max(0, self._size_bytes - payload_offset)
        content = (_format_request_log_truncation_marker(logical_end) +
                   REQUEST_LOG_DISK_PRESSURE_MARKER)
        self._file.seek(0)
        self._file.truncate()
        self._file.write(content)
        self._file.flush()
        self._size_bytes = len(content.encode('utf-8'))
        self._disk_pressure_reached = True

    def write(self, content: str) -> int:
        original_length = len(content)
        if self._disk_pressure_reached:
            return original_length
        encoded_content = content.encode('utf-8')
        with self._lock:
            if self._disk_pressure_reached:
                return original_length
            fd = self._file.fileno()
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                # File locks coordinate copied contexts and active followers.
                # Refresh from the kernel so a second writer cannot use a
                # stale per-handle size and exceed the cap.
                self._file.flush()
                self._size_bytes = os.fstat(fd).st_size
                if self._filesystem_under_pressure(len(encoded_content)):
                    self._replace_with_disk_pressure_marker(fd)
                    return original_length
                if self._size_bytes + len(encoded_content) > self._max_bytes:
                    prefix = os.pread(fd,
                                      REQUEST_LOG_TRUNCATION_MARKER_MAX_BYTES,
                                      0)
                    previous_marker = parse_request_log_truncation_marker(
                        prefix)
                    payload_offset = (previous_marker.byte_length
                                      if previous_marker is not None else 0)
                    logical_start = (previous_marker.logical_start
                                     if previous_marker is not None else 0)
                    payload_size = self._size_bytes - payload_offset
                    logical_end = logical_start + payload_size

                    # Retain the newest complete UTF-8 window across the old
                    # payload and this write. A few extra old bytes let a slice
                    # beginning inside a multibyte character recover at the
                    # next complete character.
                    available_bytes = (self._rollover_bytes -
                                       REQUEST_LOG_TRUNCATION_MARKER_MAX_BYTES)
                    desired_old_bytes = max(
                        0, available_bytes - len(encoded_content))
                    old_tail_size = min(payload_size, desired_old_bytes + 3)
                    old_tail = os.pread(
                        fd, old_tail_size,
                        payload_offset + payload_size - old_tail_size)
                    combined = old_tail + encoded_content
                    slice_offset = max(0, len(combined) - available_bytes)
                    retained_bytes = combined[slice_offset:]
                    retained_content = retained_bytes.decode('utf-8',
                                                             errors='ignore')
                    encoded_content = retained_content.encode('utf-8')
                    # The only invalid bytes can be a partial character at the
                    # beginning of the byte slice.
                    dropped_partial_bytes = (len(retained_bytes) -
                                             len(encoded_content))
                    retained_logical_start = (logical_end - len(old_tail) +
                                              slice_offset +
                                              dropped_partial_bytes)
                    marker = _format_request_log_truncation_marker(
                        retained_logical_start)
                    marker_bytes = marker.encode('utf-8')
                    self._file.seek(0)
                    self._file.truncate()
                    self._file.write(marker)
                    self._size_bytes = len(marker_bytes)
                    content = retained_content
                written = self._file.write(content)
                self._size_bytes += len(encoded_content)
                # Publish the marker and replacement bytes before readers can
                # acquire their shared lock.
                self._file.flush()
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
            if written != original_length:
                return original_length
            return written

    def writelines(self, lines: Iterable[str]) -> None:
        """Write every line through the same byte cap as :meth:`write`."""
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        with self._lock:
            self._file.flush()

    def close(self) -> None:
        with self._lock:
            self._file.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._file, name)


class SkyPilotContext:
    """SkyPilot typed context vars for threads and coroutines.

    This is a wrapper around `contextvars.ContextVar` that provides a typed
    interface for the SkyPilot specific context variables that can be accessed
    at any layer of the call stack. ContextVar is coroutine local, an empty
    Context will be initialized for each coroutine when it is created.

    Adding a new context variable for a new feature is as simple as:
    1. Add a new instance variable to the Context class.
    2. (Optional) Add new accessor methods if the variable should be protected.

    To propagate the context to a new thread/coroutine, use
    `contextvars.copy_context()`.

    Example:
        import asyncio
        import contextvars
        import time
        from sky.utils import context

        def sync_task():
            while True:
                if context.get().is_canceled():
                    break
                time.sleep(1)

        async def fastapi_handler():
            # context.initialize() has been called in lifespan
            ctx = contextvars.copy_context()
            # asyncio.to_thread copies current context implicitly
            task = asyncio.to_thread(sync_task)
            # Or explicitly:
            # loop = asyncio.get_running_loop()
            # ctx = contextvars.copy_context()
            # task = loop.run_in_executor(None, ctx.run, sync_task)
            await asyncio.sleep(1)
            context.get().cancel()
            await task
    """

    def __init__(self):
        self._canceled = asyncio.Event()
        self._log_file = None
        self._log_file_handle = None
        self._log_file_max_bytes = None
        self._log_file_min_free_bytes = None
        self.env_overrides = {}
        self.config_context = None
        self.request_context = None
        self.vars = {}
        # Callbacks invoked exactly once when cancel() is called. Used to
        # propagate cancellation into blocking sync work that cannot poll
        # is_canceled() — e.g. a gRPC streaming iterator stuck in
        # threading.Condition.wait() inside __next__.
        self._cancel_callbacks: list[Callable[[], None]] = []
        self._cancel_callbacks_lock = threading.Lock()

    def cancel(self):
        """Cancel the context. Idempotent."""
        with self._cancel_callbacks_lock:
            if self._canceled.is_set():
                return
            self._canceled.set()
            callbacks = self._cancel_callbacks
            self._cancel_callbacks = []
        for cb in callbacks:
            try:
                cb()
            except Exception:  # pylint: disable=broad-except
                _logger.debug('cancel callback raised', exc_info=True)

    def is_canceled(self):
        """Check if the context is canceled."""
        return self._canceled.is_set()

    def register_cancel_callback(self, callback: Callable[[], None]) -> None:
        """Register a callback fired when ``cancel()`` is called.

        Callbacks run on whatever thread invokes ``cancel()`` — they must be
        thread-safe and non-blocking (e.g. ``grpc_call.cancel()``).

        Closes the cancel-before-register race: if the context is already
        cancelled when ``register_cancel_callback`` is called, the callback
        is invoked synchronously here.
        """
        with self._cancel_callbacks_lock:
            if not self._canceled.is_set():
                self._cancel_callbacks.append(callback)
                return
        # Already cancelled — fire immediately, outside the lock.
        try:
            callback()
        except Exception:  # pylint: disable=broad-except
            _logger.debug('cancel callback raised', exc_info=True)

    def unregister_cancel_callback(self, callback: Callable[[], None]) -> None:
        """Remove a previously registered cancel callback (best effort)."""
        with self._cancel_callbacks_lock:
            try:
                self._cancel_callbacks.remove(callback)
            except ValueError:
                pass

    def redirect_log(self,
                     log_file: pathlib.Path | None,
                     max_bytes: int | None = None,
                     min_free_bytes: int | None = None) -> pathlib.Path | None:
        """Redirect the stdout and stderr of current context to a file.

        Args:
            log_file: The log file to redirect to. If None, the stdout and
                stderr will be restored to the original streams.
            max_bytes: If set, truncate earlier output whenever the file would
                grow beyond this many bytes. The active stream can continue
                writing after truncation.
            min_free_bytes: If set with ``max_bytes``, stop growing the file
                when its filesystem has less than this many free bytes.

        Returns:
            The old log file, or None if the stdout and stderr were not
            redirected.
        """
        original_log_file = self._log_file
        original_log_handle = self._log_file_handle
        if log_file is None:
            self._log_file_handle = None
        elif max_bytes is not None:
            self._log_file_handle = _TruncatingLogFile(
                log_file, max_bytes, min_free_bytes=min_free_bytes)
        else:
            self._log_file_handle = open(log_file, 'a', encoding='utf-8')
        self._log_file = log_file
        self._log_file_max_bytes = max_bytes
        self._log_file_min_free_bytes = min_free_bytes
        if original_log_handle is not None:
            original_log_handle.close()
        return original_log_file

    def output_stream(self, fallback: TextIO) -> TextIO:
        if self._log_file_handle is None:
            return fallback
        else:
            return self._log_file_handle

    def override_envs(self, envs: dict[str, str]):
        for k, v in envs.items():
            self.env_overrides[k] = v

    def cleanup(self):
        """Clean up the context."""
        if self._log_file_handle is not None:
            self._log_file_handle.close()
            self._log_file_handle = None

    def set_var(self, key: str, value: Any):
        self.vars[key] = value

    def get_var(self, key: str) -> Any | None:
        return self.vars.get(key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del exc_type, exc_val, exc_tb
        self.cleanup()

    def copy(self, *, inherit_log: bool = True) -> 'SkyPilotContext':
        """Create a copy of the context.

        Changes to the current context after this call will not affect the copy.
        The new context will get its own handle/fd for the log file.
        The new context will get an independent copy of the env var overrides.
        The new context will get an independent copy of the config context.
        Cancellation of the current context will not be propagated to the copy.
        """
        new_context = SkyPilotContext()
        if inherit_log:
            new_context.redirect_log(self._log_file, self._log_file_max_bytes,
                                     self._log_file_min_free_bytes)
        new_context.env_overrides = self.env_overrides.copy()
        new_context.config_context = copy.deepcopy(self.config_context)
        return new_context


_CONTEXT = contextvars.ContextVar[SkyPilotContext | None]('sky_context',
                                                          default=None)


def get() -> SkyPilotContext | None:
    """Get the current SkyPilot context.

    If the context is not initialized, get() will return None. This helps
    sync code to check whether it runs in a cancellable context and avoid
    polling the cancellation event if it is not.
    """
    return _CONTEXT.get()


def set_context_var(key: str, value: Any):
    ctx = get()
    if ctx is not None:
        # Set the var in context
        ctx.set_var(key, value)
    else:
        # Fallback to process-isolated assumption, where we thought
        # modifying process-scope vars is safe.
        _PROCESS_GLOBAL_VARS[key] = value


def get_context_var(key: str) -> Any:
    ctx = get()
    if ctx is not None:
        # Use `in` to check for key existence to distinguish
        # "key not found" from "key's value is None".
        if key in ctx.vars:
            return ctx.get_var(key)
    # Fallback to the variable set in process-scope
    return _PROCESS_GLOBAL_VARS.get(key)


class ContextualEnviron(MutableMapping[str, str]):
    """Environment variables wrapper with contextual overrides.

    An instance of ContextualEnviron will typically be used to replace
    os.environ to make the envron access of current process contextual
    aware.

    Behavior of spawning a subprocess:
    - The contextual overrides will not be applied to the subprocess by
      default.
    - When using env=os.environ to pass the environment variables to the
      subprocess explicitly. The subprocess will inherit the contextual
      environment variables at the time of the spawn, that is, it will not
      see the updates to the environment variables after the spawn. Also,
      os.environ of the subprocess will not be a ContextualEnviron unless
      the subprocess hijacks os.environ explicitly.
    - Optionally, context.Popen() can be used to automatically pass
      os.environ with overrides to subprocess.


    Example:
    1. Parent process:
       # Hijack os.environ to be a ContextualEnviron
       os.environ = ContextualEnviron(os.environ)
       ctx = context.get()
       ctx.override_envs({'FOO': 'BAR1'})
       proc = subprocess.Popen(..., env=os.environ)
       # Or use context.Popen instead
       # proc = context.Popen(...)
       ctx.override_envs({'FOO': 'BAR2'})
    2. Subprocess:
       assert os.environ['FOO'] == 'BAR1'
       ctx = context.get()
       # Override the contextual env var in the subprocess does not take
       # effect since the os.environ is not hijacked.
       ctx.override_envs({'FOO': 'BAR3'})
       assert os.environ['FOO'] == 'BAR1'
    """

    def __init__(self, environ: 'os._Environ[str]') -> None:
        self._environ = environ

    def __getitem__(self, key: str) -> str:
        ctx = get()
        if ctx is not None:
            if key in ctx.env_overrides:
                value = ctx.env_overrides[key]
                # None is used to indicate that the key is deleted in the
                # context.
                if value is None:
                    raise KeyError(key)
                return value
        return self._environ[key]

    def __iter__(self) -> Iterator[str]:

        def iter_from_context(ctx: SkyPilotContext) -> Iterator[str]:
            # Snapshot env_overrides to avoid RuntimeError: dictionary
            # changed size during iteration when another thread sharing
            # the same SkyPilotContext modifies env_overrides between
            # generator yields.
            overrides_snapshot = ctx.env_overrides.copy()
            deleted_keys = set()
            for key, value in overrides_snapshot.items():
                if value is None:
                    deleted_keys.add(key)
                else:
                    yield key
            for key in self._environ:
                # Deduplicate the keys
                if key not in ctx.env_overrides and key not in deleted_keys:
                    yield key

        ctx = get()
        if ctx is not None:
            return iter_from_context(ctx)
        else:
            return self._environ.__iter__()

    def __len__(self) -> int:
        return len(dict(self))

    def __setitem__(self, key: str, value: str) -> None:
        ctx = get()
        if ctx is not None:
            ctx.env_overrides[key] = value
        else:
            self._environ.__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        ctx = get()
        if ctx is not None:
            if key in self._environ:
                # If the key is set in the environ of the process, we mark it as
                # deleted in the context by setting the value to None.
                # Note: we must do this even if it was also set in the context,
                # since it could be set in both, and deleting should delete it
                # from both.
                ctx.env_overrides[key] = None
            elif key in ctx.env_overrides:
                # If the key is set in the context, but not the original
                # environ, we can just delete the override.
                del ctx.env_overrides[key]
            else:
                # The key is not set in the context nor the process.
                raise KeyError(key)
        else:
            self._environ.__delitem__(key)

    def __repr__(self) -> str:
        # Adapted from os._Environ.__repr__
        formatted_items = ', '.join(
            f'{key!r}: {value!r}' for key, value in self.items())
        return f'ctx_environ({{{formatted_items}}})'

    def copy(self) -> dict[str, str]:
        copied = self._environ.copy()
        ctx = get()
        if ctx is not None:
            # Snapshot to avoid RuntimeError from concurrent modification.
            overrides_snapshot = ctx.env_overrides.copy()
            for key, value in overrides_snapshot.items():
                if value is None:
                    copied.pop(key, None)
                else:
                    copied[key] = value
        return copied

    def setdefault(self, key: str, default: str) -> str:
        return self._environ.setdefault(key, default)

    def __ior__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        self.update(other)
        return self

    def __or__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        new = dict(self)
        new.update(other)
        return new

    def __ror__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        new = dict(other)
        new.update(self)
        return new


class Popen(subprocess.Popen):

    def __init__(self, *args, **kwargs):
        env = kwargs.pop('env', None)
        if env is None:
            # Pass a copy of current context.environ to avoid race condition
            # when the context is updated after the Popen is created.
            env = os.environ.copy()
        super().__init__(*args, env=env,
                         **kwargs)  # type: ignore[call-overload]


P = ParamSpec('P')
T = TypeVar('T')


def contextual(func: Callable[P, T]) -> Callable[P, T]:
    """Decorator to initialize a context before executing the function.

    If a context is already initialized, this decorator will create a new
    context that inherits the values from the existing context.
    """

    def run_in_context(*args: P.args, **kwargs: P.kwargs) -> T:
        # Within the new contextvars Context, set up the SkyPilotContext.
        original_ctx = get()
        with initialize(original_ctx):
            return func(*args, **kwargs)

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        # Create a copy of the current contextvars Context so that setting the
        # SkyPilotContext does not affect the caller's context in async
        # environments.
        context = contextvars.copy_context()
        return context.run(run_in_context, *args, **kwargs)

    return wrapper


def contextual_without_log(func: Callable[P, T]) -> Callable[P, T]:
    """Run with inherited config/environment but no diagnostic log sink."""

    def run_in_context(*args: P.args, **kwargs: P.kwargs) -> T:
        original_ctx = get()
        with initialize(original_ctx, inherit_log=False):
            return func(*args, **kwargs)

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        copied_context = contextvars.copy_context()
        return copied_context.run(run_in_context, *args, **kwargs)

    return wrapper


def contextual_async(
    func: Callable[P, Coroutine[Any, Any, T]]
) -> Callable[P, Coroutine[Any, Any, T]]:
    """Decorator to initialize a context before executing the function.

    If a context is already initialized, this decorator will create a new
    context that inherits the values from the existing context.
    """

    async def run_in_context(*args: P.args, **kwargs: P.kwargs) -> T:
        # Within the new contextvars Context, set up the SkyPilotContext.
        original_ctx = get()
        with initialize(original_ctx):
            return await func(*args, **kwargs)

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        # Create a copy of the current contextvars Context so that setting the
        # SkyPilotContext does not affect the caller's context in async
        # environments.
        context = contextvars.copy_context()
        return await context.run(run_in_context, *args, **kwargs)

    return wrapper


def initialize(base_context: SkyPilotContext | None = None,
               *,
               inherit_log: bool = True) -> SkyPilotContext:
    """Initialize the current SkyPilot context."""
    new_context = (base_context.copy(inherit_log=inherit_log)
                   if base_context is not None else SkyPilotContext())
    _CONTEXT.set(new_context)
    return new_context


class _ContextualStream:
    """A base class for streams that are contextually aware.

    This class implements the TextIO interface via __getattr__ to delegate
    attribute access to the original or contextual stream.
    """
    _original_stream: TextIO

    def __init__(self, original_stream: TextIO):
        self._original_stream = original_stream

    def __getattr__(self, attr: str):
        return getattr(self._active_stream(), attr)

    def _active_stream(self) -> TextIO:
        ctx = get()
        if ctx is None:
            return self._original_stream
        return ctx.output_stream(self._original_stream)


class Stdout(_ContextualStream):

    def __init__(self):
        super().__init__(sys.stdout)


class Stderr(_ContextualStream):

    def __init__(self):
        super().__init__(sys.stderr)
