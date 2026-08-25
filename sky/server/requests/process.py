"""One-shot process boundaries for API request execution.

Every accepted invocation has a dedicated outer guardian, inner warden, and
handler.  The two owners are Linux child subreapers.  They keep arbitrary
session-changing descendants inside one invocation-specific ancestry boundary
and report completion only after every invocation descendant is absent.
"""

from collections.abc import Callable
import concurrent.futures
import ctypes
import dataclasses
import enum
import errno
import logging
import math
import multiprocessing
from multiprocessing import connection as multiprocessing_connection
from multiprocessing import process as multiprocessing_process
from multiprocessing import reduction as multiprocessing_reduction
import os
import pathlib
import pickle
import secrets
import signal
import sys
import threading
import time
from typing import Any

import setproctitle

from sky import exceptions
from sky.utils import controller_capability

logger = logging.getLogger(__name__)

_PR_SET_CHILD_SUBREAPER = 36
_PR_GET_CHILD_SUBREAPER = 37
_PR_GET_DUMPABLE = 3
_PR_SET_DUMPABLE = 4
_BOUNDARY_START_TIMEOUT_SECONDS = 30
_HANDLER_CANCEL_GRACE_SECONDS = 0.5
_BOUNDARY_POLL_SECONDS = 0.05
_STABLE_EMPTY_SCANS = 3
_PROCESS_REAP_PROOF_TIMEOUT_SECONDS = 5
_BOUNDARY_SHUTDOWN_WAIT_SECONDS = 5
_BOUNDARY_START_CLEANUP_TIMEOUT_SECONDS = 5
_PIDFD_SYSCALLS = {
    # pidfd_send_signal, pidfd_open on supported Linux architectures.
    'x86_64': (424, 434),
    'aarch64': (424, 434),
}


def _require_admission_deadline(deadline_monotonic: float | None, *,
                                admission_gated: bool,
                                receipt_required: bool) -> float | None:
    """Validate one optional automatic-admission deadline before capacity."""
    if deadline_monotonic is None:
        return None
    if (isinstance(deadline_monotonic, bool) or
            not isinstance(deadline_monotonic, (int, float))):
        raise TypeError('admission_deadline_monotonic must be a number.')
    deadline = float(deadline_monotonic)
    if not math.isfinite(deadline):
        raise ValueError('admission_deadline_monotonic must be finite.')
    if admission_gated:
        raise ValueError('An automatic-admission deadline cannot be combined '
                         'with admission_gated=True.')
    if receipt_required:
        raise ValueError(
            'admission_deadline_monotonic cannot be combined with '
            'receipt_required: a deadline failure after admission has no '
            'caller-owned Future that can durably acknowledge effects.')
    if time.monotonic() >= deadline:
        raise TimeoutError('Invocation admission deadline already expired.')
    return deadline


class InvocationOutcomeKind(enum.Enum):
    """Closed execution outcome produced before boundary completion."""

    SUCCEEDED = 'succeeded'
    PRE_EFFECT = 'pre_effect'
    CANCELLED = 'cancelled'
    RETRYABLE = 'retryable'
    FAILED = 'failed'


@dataclasses.dataclass(frozen=True)
class ProcessIdentity:
    """Linux PID plus its non-reusable procfs birth identity."""

    pid: int
    start_time_ticks: int


@dataclasses.dataclass(frozen=True)
class InvocationOutcome:
    """Handler result before the invocation family is converged."""

    kind: InvocationOutcomeKind
    value: Any = None
    error: BaseException | None = None


@dataclasses.dataclass(frozen=True)
class BoundaryResult:
    """Typed outcome whose entire invocation process family is proven absent."""

    guardian: ProcessIdentity
    outcome: InvocationOutcome
    family_drained: bool = True


class BoundaryExecutionError(RuntimeError):
    """An invocation boundary failed before a transportable handler result."""


class AmbiguousBoundaryError(RuntimeError):
    """Both boundary owners vanished without authoritative family proof."""


class BoundaryShutdownPendingError(RuntimeError):
    """Shutdown retains boundaries whose durable receipts are still pending."""

    def __init__(self,
                 guardians: tuple[ProcessIdentity, ...],
                 starting_workers: int = 0):
        self.guardians = guardians
        self.starting_workers = starting_workers
        super().__init__('Invocation boundary shutdown is waiting for durable '
                         'receipt acknowledgement from guardians '
                         f'{[identity.pid for identity in guardians]}' +
                         (f' and {starting_workers} starting boundaries.'
                          if starting_workers else '.'))


class FamilyEnumerationError(RuntimeError):
    """The boundary could not prove the state of its process family."""


class _ExecutorLane(enum.Enum):
    GUARANTEED = 'guaranteed'
    BURST = 'burst'


@dataclasses.dataclass(frozen=True)
class IdleWorkerReservation:
    """Opaque one-use reservation for immediately available capacity."""

    owner_token: object
    reservation_id: int
    lane: _ExecutorLane


class _Command(enum.Enum):
    ADMIT = 'admit'
    CANCEL = 'cancel'
    RECEIPT = 'receipt'
    FINALIZE = 'finalize'


class _Event(enum.Enum):
    READY = 'ready'
    INNER_READY = 'inner_ready'
    INNER_DRAINED = 'inner_drained'
    RESULT = 'result'


@dataclasses.dataclass(frozen=True)
class _BoundaryEnvelope:
    token: str
    event: _Event
    payload: Any = None


@dataclasses.dataclass(frozen=True)
class _HandlerReport:
    outcome: InvocationOutcome


@dataclasses.dataclass(frozen=True)
class _Invocation:
    """Opaque invocation decoded only inside the isolated handler process."""

    fn: Callable
    initializer: Callable | None
    initargs: tuple
    args: tuple
    kwargs: dict[str, Any]


@dataclasses.dataclass
class _InvocationRecord:
    guardian: multiprocessing_process.BaseProcess
    future: 'InvocationFuture'
    monitor: threading.Thread | None = None


_SPAWN_CONTEXT = multiprocessing.get_context('spawn')


def _read_process_stat(pid: int) -> tuple[ProcessIdentity, str, int]:
    """Read one process's exact identity, state, and parent from procfs."""
    content = (pathlib.Path('/proc') / str(pid) /
               'stat').read_text(encoding='utf-8')
    comm_end = content.rfind(')')
    if comm_end < 2 or not content.startswith(f'{pid} ('):
        raise ValueError(f'Malformed procfs identity for PID {pid}.')
    fields_after_comm = content[comm_end + 1:].split()
    if len(fields_after_comm) <= 19:
        raise ValueError(f'Malformed procfs identity for PID {pid}.')
    state = fields_after_comm[0]
    parent_pid = int(fields_after_comm[1])
    start_time_ticks = int(fields_after_comm[19])
    if len(state) != 1 or parent_pid < 0 or start_time_ticks <= 0:
        raise ValueError(f'Invalid procfs birth identity for PID {pid}.')
    return (ProcessIdentity(pid, start_time_ticks), state, parent_pid)


def _read_process_start_time_ticks(pid: int) -> int:
    return _read_process_stat(pid)[0].start_time_ticks


def _reap_exact_direct_child_zombie(child: multiprocessing_process.BaseProcess,
                                    expected_identity: ProcessIdentity) -> bool:
    """Reap a proven exact zombie when its spawn sentinel remains open.

    ``BaseProcess.join(timeout=...)`` waits on the spawn sentinel before it
    calls ``waitpid``.  A leaked writer can therefore leave an already-exited
    direct child as a zombie while the timed join reports no lifetime proof.
    Only an authenticated boundary result may call this helper.  It accepts
    exactly the recorded birth identity in zombie state with this process as
    its direct parent, then performs the missing nonblocking reap itself.
    """
    child_pid = child.pid
    if child_pid is None or child_pid != expected_identity.pid:
        return False
    popen = getattr(child, '_popen', None)
    if (popen is None or getattr(popen, 'pid', None) != child_pid or
            getattr(popen, 'returncode', None) is not None):
        return False
    try:
        observed_identity, state, parent_pid = _read_process_stat(child_pid)
    except (FileNotFoundError, ProcessLookupError, OSError, ValueError):
        return False
    if (observed_identity != expected_identity or state != 'Z' or
            parent_pid != os.getpid()):
        return False
    try:
        waited_pid, wait_status = os.waitpid(child_pid, os.WNOHANG)
    except (ChildProcessError, OSError):
        return False
    if waited_pid != child_pid:
        return False
    popen.returncode = os.waitstatus_to_exitcode(wait_status)
    # ``Popen.wait`` now returns the cached status without touching the leaked
    # sentinel, while public ``join`` removes the reaped process from the
    # multiprocessing active-child registry.
    child.join(timeout=0)
    return True


def _wait_for_exact_direct_child_reap(
        child: multiprocessing_process.BaseProcess,
        expected_identity: ProcessIdentity, timeout: float) -> bool:
    """Poll waitpid for one exact child without trusting its spawn sentinel."""
    if timeout < 0:
        raise ValueError('Process reap timeout must be nonnegative.')
    child_pid = child.pid
    if child_pid is None or child_pid != expected_identity.pid:
        return False
    popen = getattr(child, '_popen', None)
    if popen is None or getattr(popen, 'pid', None) != child_pid:
        return False
    if getattr(popen, 'returncode', None) is not None:
        return True
    deadline = time.monotonic() + timeout
    while True:
        if _reap_exact_direct_child_zombie(child, expected_identity):
            return True
        if getattr(popen, 'returncode', None) is not None:
            return True
        try:
            observed_identity, _, parent_pid = _read_process_stat(child_pid)
        except (FileNotFoundError, ProcessLookupError, OSError, ValueError):
            # Absence without this owner obtaining waitpid status is not proof.
            return False
        if (observed_identity != expected_identity or
                parent_pid != os.getpid()):
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(_BOUNDARY_POLL_SECONDS, remaining))


def _enable_subreaper() -> None:
    if not sys.platform.startswith('linux'):
        raise OSError('Request invocation boundaries require Linux.')
    libc = ctypes.CDLL(None, use_errno=True)
    enabled = ctypes.c_int(0)
    if libc.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if libc.prctl(_PR_GET_CHILD_SUBREAPER, ctypes.byref(enabled), 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if enabled.value != 1:
        raise OSError('Kernel did not enable child-subreaper semantics.')


def _make_process_non_dumpable() -> None:
    """Disable core/ptrace exposure before accepting a bearer transport."""
    if not sys.platform.startswith('linux'):
        raise OSError('Controller capability transport requires Linux.')
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    dumpable = libc.prctl(_PR_GET_DUMPABLE, 0, 0, 0, 0)
    if dumpable < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if dumpable != 0:
        raise OSError('Kernel did not disable process dumpability.')


def _quarantine_fd_slots(open_fds: tuple[int, ...],
                         keep_fds: frozenset[int]) -> None:
    """Replace unwanted descriptor slots without making them reusable.

    ``os.close()`` cannot invalidate the Python objects that owned an inherited
    descriptor before ``fork()``.  If a protocol pipe later reuses that numeric
    slot, the stale object's normal cleanup can close the live pipe.  Replacing
    the underlying descriptor atomically with ``/dev/null`` removes access to
    the inherited resource while reserving its number until the stale owner
    closes it.  The one-shot process then provides the final lifetime bound for
    any ownerless quarantine slots.

    Callers run this in a single-threaded fork child before allocating any
    later protocol descriptors or executing invocation-specific Python code.
    A stale owner therefore cannot release a placeholder early enough for a
    later protocol descriptor to reuse its number.
    """
    quarantine_fds = tuple(fd for fd in open_fds if fd not in keep_fds)
    if not quarantine_fds:
        return
    flags = os.O_RDWR
    if hasattr(os, 'O_CLOEXEC'):
        flags |= os.O_CLOEXEC
    null_fd = os.open(os.devnull, flags)
    try:
        for fd in quarantine_fds:
            if fd == null_fd:
                continue
            try:
                os.fstat(fd)
            except OSError as e:
                if e.errno == errno.EBADF:
                    continue
                raise BoundaryExecutionError(
                    f'Could not inspect inherited descriptor {fd}: {e}') from e
            try:
                os.dup2(null_fd, fd, inheritable=False)
            except OSError as e:
                raise BoundaryExecutionError(
                    f'Could not quarantine inherited descriptor {fd}: {e}'
                ) from e
    finally:
        os.close(null_fd)


def _quarantine_unrelated_fds(keep_fds: frozenset[int]) -> None:
    """Remove inherited access without permitting stale descriptor aliases.

    In particular, Python's spawn implementation leaves the guardian side of
    its multiprocessing sentinel open.  A forked handler retaining that writer
    would make the API's ``Process.join()`` wait forever after both owners die.
    Internal descendants therefore retain access only to their explicit
    protocol endpoints and standard streams; every other numeric descriptor
    slot is quarantined until its stale Python owner or the process exits.
    """
    keep_fds = keep_fds | frozenset({0, 1, 2})
    try:
        open_fds = tuple(
            int(name) for name in os.listdir('/proc/self/fd') if name.isdigit())
    except OSError as e:
        raise BoundaryExecutionError(
            f'Could not enumerate inherited process descriptors: {e}') from e
    _quarantine_fd_slots(open_fds, keep_fds)


def _identity_matches(identity: ProcessIdentity) -> bool:
    try:
        return (_read_process_start_time_ticks(
            identity.pid) == identity.start_time_ticks)
    except (FileNotFoundError, ProcessLookupError, OSError, ValueError):
        return False


def _direct_child_pids(pid: int) -> tuple[int, ...]:
    task_directory = pathlib.Path('/proc') / str(pid) / 'task'
    try:
        tasks = tuple(task_directory.iterdir())
    except FileNotFoundError:
        return ()
    except OSError as e:
        raise FamilyEnumerationError(
            f'Could not enumerate tasks of PID {pid}: {e}') from e
    children: set[int] = set()
    for task in tasks:
        if not task.name.isdigit():
            continue
        try:
            raw = (task / 'children').read_text(encoding='utf-8').strip()
        except FileNotFoundError:
            # A thread may exit between listing the task directory and reading
            # its children.  A stable scan observes any adopted survivors.
            continue
        except OSError as e:
            raise FamilyEnumerationError(
                f'Could not enumerate children of PID {pid} task '
                f'{task.name}: {e}') from e
        if not raw:
            continue
        try:
            children.update(int(value) for value in raw.split())
        except ValueError as e:
            raise FamilyEnumerationError(
                f'Malformed procfs child list for PID {pid} task '
                f'{task.name}.') from e
    return tuple(children)


def _scan_descendants() -> dict[int, ProcessIdentity]:
    """Snapshot this subreaper's descendants via procfs."""
    identities: dict[int, ProcessIdentity] = {}
    pending = list(_direct_child_pids(os.getpid()))
    while pending:
        pid = pending.pop()
        if pid in identities:
            continue
        try:
            identity = ProcessIdentity(pid, _read_process_start_time_ticks(pid))
        except (FileNotFoundError, ProcessLookupError):
            # An exited child is either gone or is adopted by this subreaper;
            # the next stable scan observes any surviving descendants.
            continue
        except (OSError, ValueError) as e:
            raise FamilyEnumerationError(
                f'Could not identify invocation descendant {pid}: {e}') from e
        identities[pid] = identity
        pending.extend(_direct_child_pids(pid))
    return identities


def _send_exact_signal(identity: ProcessIdentity, signum: int) -> None:
    syscall_numbers = _PIDFD_SYSCALLS.get(os.uname().machine)
    if syscall_numbers is None:
        raise FamilyEnumerationError(
            'Exact invocation cleanup requires '
            'Linux pidfds on a supported architecture.')
    pidfd_send_signal_number, pidfd_open_number = syscall_numbers
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        pidfd = libc.syscall(pidfd_open_number, identity.pid, 0)
        if pidfd < 0:
            error_number = ctypes.get_errno()
            if error_number == errno.ESRCH:
                return
            raise OSError(error_number, os.strerror(error_number))
    except OSError as e:
        raise FamilyEnumerationError(
            f'Could not open pidfd for descendant {identity.pid}: {e}') from e
    try:
        if not _identity_matches(identity):
            return
        try:
            result = libc.syscall(pidfd_send_signal_number, pidfd, signum, 0, 0)
            if result < 0:
                error_number = ctypes.get_errno()
                if error_number == errno.ESRCH:
                    return
                raise OSError(error_number, os.strerror(error_number))
        except OSError as e:
            raise FamilyEnumerationError(
                f'Could not signal descendant {identity.pid}: {e}') from e
    finally:
        os.close(pidfd)


def _protected_pids(identities: dict[int, ProcessIdentity],
                    keep_exact: frozenset[ProcessIdentity]) -> set[int]:
    exact_by_pid = {identity.pid: identity for identity in keep_exact}
    return {
        pid for pid, identity in identities.items()
        if exact_by_pid.get(pid) == identity
    }


def _reap_unprotected_direct_children(protected: set[int]) -> None:
    for pid in _direct_child_pids(os.getpid()):
        if pid in protected:
            continue
        try:
            os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            continue
        except OSError as e:
            raise FamilyEnumerationError(
                f'Could not reap invocation descendant {pid}: {e}') from e


def _converge_family(
        *,
        keep_exact: frozenset[ProcessIdentity] = frozenset(),
) -> None:
    """Freeze, kill, reap, and prove a stable allowed descendant set."""
    stable_scans = 0
    while stable_scans < _STABLE_EMPTY_SCANS:
        try:
            identities = _scan_descendants()
            protected = _protected_pids(identities, keep_exact)
            targets = [
                identity for pid, identity in identities.items()
                if pid not in protected
            ]
            if targets:
                stable_scans = 0
                # Stop every currently known forker before killing.  A later
                # scan catches last-moment children adopted by this subreaper.
                for identity in targets:
                    _send_exact_signal(identity, signal.SIGSTOP)
                for identity in targets:
                    _send_exact_signal(identity, signal.SIGKILL)
            else:
                stable_scans += 1
            _reap_unprotected_direct_children(protected)
        except FamilyEnumerationError as e:
            # Uncertainty is not absence.  Retain the boundary and retry.
            stable_scans = 0
            logger.error('Invocation family convergence retry: %s', e)
        time.sleep(_BOUNDARY_POLL_SECONDS)


def _transportable_error(error: BaseException) -> BaseException:
    try:
        pickle.dumps(error)
    # Pure synchronous serialization probe: an arbitrary user exception may
    # itself fail pickling and must be converted into transportable data.
    except BaseException:  # noqa: ASYNC103  # pylint: disable=broad-except
        return BoundaryExecutionError(  # noqa: ASYNC104
            f'Untransportable {type(error).__name__}: {error}')
    return error


def _normalize_outcome(value: Any) -> InvocationOutcome:
    if isinstance(value, InvocationOutcome):
        return value
    return InvocationOutcome(InvocationOutcomeKind.SUCCEEDED, value=value)


def _serialize_invocation(fn: Callable, initializer: Callable | None,
                          initargs: tuple, args: tuple,
                          kwargs: dict[str, Any]) -> bytes:
    """Freeze an invocation for decoding only in the isolated handler."""
    return pickle.dumps(_Invocation(fn, initializer, initargs, args, kwargs),
                        protocol=pickle.HIGHEST_PROTOCOL)


def _deserialize_invocation(payload: bytes) -> _Invocation:
    """Decode one parent-created invocation after raw FD isolation."""
    invocation = pickle.loads(payload)
    if not isinstance(invocation, _Invocation):
        raise BoundaryExecutionError('Invocation payload has an invalid type.')
    return invocation


def _handler_main(invocation_payload: bytes,
                  report_connection: multiprocessing_connection.Connection,
                  finalize_connection: multiprocessing_connection.Connection,
                  capability_fd: int | None) -> None:
    """Run one handler, report its outcome, then retain its family root."""
    try:
        os.setsid()
        try:
            # This stdlib-only primitive is the first extension boundary in
            # the raw-bearing handler.  It closes the transport on every path,
            # installs PID-bound authority only here, and protects memory
            # before setproctitle, initializers, plugins, or request callbacks.
            if capability_fd is None:
                controller_capability.clear_process_local()
            else:
                controller_capability.install_process_local_from_fd_protected(
                    capability_fd)
                capability_fd = None
            setproctitle.setproctitle(
                f'SkyPilot:executor:handler:{os.getpid()}')
            invocation = _deserialize_invocation(invocation_payload)
            if invocation.initializer is not None:
                invocation.initializer(*invocation.initargs)
            outcome = _normalize_outcome(
                invocation.fn(*invocation.args, **invocation.kwargs))
        except KeyboardInterrupt as e:
            outcome = InvocationOutcome(InvocationOutcomeKind.CANCELLED,
                                        error=_transportable_error(e))
        except exceptions.ExecutionRetryableError as e:
            outcome = InvocationOutcome(InvocationOutcomeKind.RETRYABLE,
                                        error=_transportable_error(e))
        # The handler is a synchronous child process. Every callable failure
        # is transported as an outcome before the family is drained.
        except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
            outcome = InvocationOutcome(InvocationOutcomeKind.FAILED,
                                        error=_transportable_error(e))
        try:
            report_connection.send(_HandlerReport(outcome))
        except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
            fallback = InvocationOutcome(
                InvocationOutcomeKind.FAILED,
                error=BoundaryExecutionError(
                    f'Could not transport handler outcome: {type(e).__name__}: '
                    f'{e}'))
            try:
                report_connection.send(_HandlerReport(fallback))
            except BaseException:  # noqa: ASYNC103  # pylint: disable=broad-except
                pass
        finally:
            report_connection.close()

        # The handler remains the exact live family root until the warden has
        # drained descendants and explicitly permits exit.  EOF means the
        # warden died; stay alive so the outer guardian can adopt and drain us.
        while True:
            try:
                if finalize_connection.poll(_BOUNDARY_POLL_SECONDS):
                    command = finalize_connection.recv()
                    if command is _Command.FINALIZE:
                        break
            except (EOFError, OSError):
                # The warden died.  Preserve this root for adoption by the
                # outer guardian, while still reaping already-dead children so
                # they cannot prevent the outer stable-empty proof.
                pass
            while True:
                waited_pid = 0
                try:
                    waited_pid, _ = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    break
                if waited_pid == 0:
                    break
    finally:
        finalize_connection.close()


def _spawn_handler(
    invocation_payload: bytes,
    capability_fd: int | None,
    inherited_connections: tuple[multiprocessing_connection.Connection, ...],
) -> tuple[ProcessIdentity, multiprocessing_connection.Connection,
           multiprocessing_connection.Connection]:
    report_parent, report_child = multiprocessing.Pipe(duplex=False)
    finalize_child, finalize_parent = multiprocessing.Pipe(duplex=False)
    pid = os.fork()
    if pid == 0:
        report_parent.close()
        finalize_parent.close()
        # The handler and arbitrary descendants must never retain an API or
        # outer-owner endpoint.  Otherwise killing both boundary owners would
        # leave the parent monitor waiting on a pipe held by unowned effects.
        for inherited in inherited_connections:
            inherited.close()
        kept_fds = {report_child.fileno(), finalize_child.fileno()}
        if capability_fd is not None:
            kept_fds.add(capability_fd)
        _quarantine_unrelated_fds(frozenset(kept_fds))
        try:
            _handler_main(invocation_payload, report_child, finalize_child,
                          capability_fd)
        finally:
            os._exit(0)  # pylint: disable=protected-access
    report_child.close()
    finalize_child.close()
    return (ProcessIdentity(pid, _read_process_start_time_ticks(pid)),
            report_parent, finalize_parent)


def _send_handler_term(identity: ProcessIdentity) -> None:
    if not _identity_matches(identity):
        return
    try:
        os.killpg(identity.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError as e:
        raise FamilyEnumerationError(
            f'Could not terminate handler group {identity.pid}: {e}') from e


def _wait_for_handler_report(
    control: multiprocessing_connection.Connection,
    report: multiprocessing_connection.Connection,
    handler: ProcessIdentity,
) -> tuple[InvocationOutcome, bool]:
    """Return the handler outcome and whether the outer owner remains live."""
    outer_available = True
    cancellation_requested = False
    cancellation_started_at: float | None = None
    while True:
        ready = multiprocessing_connection.wait([control, report],
                                                timeout=_BOUNDARY_POLL_SECONDS)
        if control in ready:
            try:
                command = control.recv()
            except (EOFError, OSError):
                outer_available = False
                cancellation_requested = True
                command = None
            if command is _Command.CANCEL:
                cancellation_requested = True
        if cancellation_requested and cancellation_started_at is None:
            _send_handler_term(handler)
            cancellation_started_at = time.monotonic()
        if report in ready:
            try:
                payload = report.recv()
            except (EOFError, OSError):
                payload = None
            if not isinstance(payload, _HandlerReport):
                outcome = InvocationOutcome(
                    InvocationOutcomeKind.FAILED,
                    error=BoundaryExecutionError(
                        'Handler exited without a typed outcome.'))
            else:
                outcome = payload.outcome
            if cancellation_requested:
                outcome = InvocationOutcome(
                    InvocationOutcomeKind.CANCELLED,
                    error=concurrent.futures.CancelledError())
            return outcome, outer_available
        if (cancellation_started_at is not None and
                time.monotonic() - cancellation_started_at
                >= _HANDLER_CANCEL_GRACE_SECONDS):
            return (InvocationOutcome(
                InvocationOutcomeKind.CANCELLED,
                error=concurrent.futures.CancelledError()), outer_available)
        if not _identity_matches(handler):
            return (InvocationOutcome(
                InvocationOutcomeKind.FAILED,
                error=BoundaryExecutionError(
                    'Handler process exited without a typed outcome.')),
                    outer_available)


def _send_boundary_result_and_wait_receipt(
        parent_connection: multiprocessing_connection.Connection,
        result: BoundaryResult, boundary_token: str) -> bool:
    """Publish a boundary result; return whether the API acknowledged it."""
    try:
        parent_connection.send(
            _BoundaryEnvelope(boundary_token, _Event.RESULT, result))
    except (BrokenPipeError, EOFError, OSError):
        return False
    while True:
        try:
            command = parent_connection.recv()
        except (EOFError, OSError):
            return False
        if command is _Command.RECEIPT:
            return True


def _inner_warden_main(
    outer_connection: multiprocessing_connection.Connection,
    parent_connection: multiprocessing_connection.Connection,
    guardian: ProcessIdentity,
    boundary_token: str,
    invocation_payload: bytes,
    capability_fd: int | None,
) -> None:
    """Own handler effects and take over publication if the outer dies."""
    termination_requested = threading.Event()

    def request_termination(_signum: int, _frame: Any) -> None:
        termination_requested.set()

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, request_termination)
    os.setsid()
    _enable_subreaper()
    outer_connection.send(_Event.INNER_READY)
    handler: ProcessIdentity | None = None
    report_connection: multiprocessing_connection.Connection | None = None
    finalize_connection: multiprocessing_connection.Connection | None = None
    outer_available = True
    outcome: InvocationOutcome
    try:
        admitted = False
        while not admitted and not termination_requested.is_set():
            ready = multiprocessing_connection.wait(
                [outer_connection], timeout=_BOUNDARY_POLL_SECONDS)
            if not ready:
                continue
            command = None
            try:
                command = outer_connection.recv()
            except (EOFError, OSError):
                outer_available = False
                break
            if command is _Command.ADMIT:
                admitted = True
            elif command is _Command.CANCEL:
                termination_requested.set()
        if not admitted:
            outcome = InvocationOutcome(
                InvocationOutcomeKind.PRE_EFFECT,
                error=concurrent.futures.CancelledError())
        else:
            handler, report_connection, finalize_connection = _spawn_handler(
                invocation_payload,
                capability_fd,
                inherited_connections=(outer_connection, parent_connection))
            if capability_fd is not None:
                os.close(capability_fd)
                capability_fd = None
            outcome, report_outer_available = _wait_for_handler_report(
                outer_connection, report_connection, handler)
            outer_available = outer_available and report_outer_available

        if handler is not None:
            if outcome.kind is InvocationOutcomeKind.SUCCEEDED:
                _converge_family(keep_exact=frozenset({handler}))
                assert finalize_connection is not None
                try:
                    finalize_connection.send(_Command.FINALIZE)
                except (BrokenPipeError, EOFError, OSError):
                    pass
                while _identity_matches(handler):
                    try:
                        os.waitpid(handler.pid, os.WNOHANG)
                    except ChildProcessError:
                        break
                    time.sleep(_BOUNDARY_POLL_SECONDS)
            else:
                # Stop the family root before enumeration closes the fork race;
                # then kill/reap the whole family under this subreaper.  The
                # durable identity is the still-live outer guardian, so no
                # observer can mistake handler reaping for quiescence.
                _send_exact_signal(handler, signal.SIGSTOP)
                _converge_family()
            _converge_family()

        result = BoundaryResult(guardian, outcome)
        if outer_available:
            try:
                outer_connection.send((_Event.INNER_DRAINED, result))
            except (BrokenPipeError, EOFError, OSError):
                outer_available = False
        if outer_available:
            while True:
                command = None
                try:
                    command = outer_connection.recv()
                except (EOFError, OSError):
                    outer_available = False
                    break
                if command is _Command.FINALIZE:
                    break
        if not outer_available:
            _send_boundary_result_and_wait_receipt(parent_connection, result,
                                                   boundary_token)
        _converge_family()
    finally:
        if capability_fd is not None:
            os.close(capability_fd)
        if report_connection is not None:
            report_connection.close()
        if finalize_connection is not None:
            finalize_connection.close()
        outer_connection.close()
        parent_connection.close()


def _outer_guardian_main(
    parent_connection: multiprocessing_connection.Connection,
    boundary_token: str,
    invocation_payload: bytes,
    capability_handle: Any | None,
) -> None:
    """Publish admission identity and own the final invocation boundary."""
    termination_requested = threading.Event()

    def request_termination(_signum: int, _frame: Any) -> None:
        termination_requested.set()

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, request_termination)
    _enable_subreaper()
    capability_fd = None
    if capability_handle is not None:
        # Redeem the process-local resource-sharer token before admission.  A
        # cancelled pre-effect invocation therefore cannot strand a duplicate
        # bearer FD in the API parent's resource-sharer process.  The raw
        # capability remains unread in the pipe until the admitted handler.
        _make_process_non_dumpable()
        capability_fd = capability_handle.detach()
        os.set_inheritable(capability_fd, False)
    guardian = ProcessIdentity(os.getpid(),
                               _read_process_start_time_ticks(os.getpid()))
    outer_connection, inner_connection = multiprocessing.Pipe(duplex=True)
    inner_pid = os.fork()
    if inner_pid == 0:
        outer_connection.close()
        kept_fds = {inner_connection.fileno(), parent_connection.fileno()}
        if capability_fd is not None:
            kept_fds.add(capability_fd)
        _quarantine_unrelated_fds(frozenset(kept_fds))
        try:
            _inner_warden_main(inner_connection, parent_connection, guardian,
                               boundary_token, invocation_payload,
                               capability_fd)
        finally:
            os._exit(0)  # pylint: disable=protected-access
    if capability_fd is not None:
        os.close(capability_fd)
        capability_fd = None
    inner_connection.close()
    inner = ProcessIdentity(inner_pid,
                            _read_process_start_time_ticks(inner_pid))
    result: BoundaryResult | None = None
    receipt_received = False
    inner_available = True
    parent_available = True
    try:
        admitted = False
        inner_ready = False
        while result is None and not inner_ready:
            if termination_requested.is_set() and inner_available:
                try:
                    outer_connection.send(_Command.CANCEL)
                except (BrokenPipeError, EOFError, OSError):
                    inner_available = False
            watched = [parent_connection] if parent_available else []
            if inner_available:
                watched.append(outer_connection)
            if not watched:
                termination_requested.set()
            else:
                ready = multiprocessing_connection.wait(
                    watched, timeout=_BOUNDARY_POLL_SECONDS)
                if parent_connection in ready:
                    try:
                        command = parent_connection.recv()
                    except (EOFError, OSError):
                        parent_available = False
                        termination_requested.set()
                        command = None
                    if command is _Command.CANCEL:
                        termination_requested.set()
                if inner_available and outer_connection in ready:
                    try:
                        payload = outer_connection.recv()
                    except (EOFError, OSError):
                        inner_available = False
                        payload = None
                    if payload is _Event.INNER_READY:
                        inner_ready = True
                    elif payload is not None:
                        inner_available = False
            if inner_available and not _identity_matches(inner):
                inner_available = False
            if termination_requested.is_set() or not inner_available:
                _converge_family()
                if termination_requested.is_set():
                    outcome = InvocationOutcome(
                        InvocationOutcomeKind.PRE_EFFECT,
                        error=concurrent.futures.CancelledError())
                else:
                    outcome = InvocationOutcome(
                        InvocationOutcomeKind.FAILED,
                        error=BoundaryExecutionError(
                            'Inner warden failed admission setup.'))
                result = BoundaryResult(guardian, outcome)

        if result is None:
            try:
                parent_connection.send(
                    _BoundaryEnvelope(boundary_token, _Event.READY, guardian))
            except (BrokenPipeError, EOFError, OSError):
                parent_available = False
                termination_requested.set()

        while result is None:
            if termination_requested.is_set() and inner_available:
                try:
                    outer_connection.send(_Command.CANCEL)
                except (BrokenPipeError, EOFError, OSError):
                    inner_available = False
            watched = [parent_connection] if parent_available else []
            if inner_available:
                watched.append(outer_connection)
            ready = multiprocessing_connection.wait(
                watched, timeout=_BOUNDARY_POLL_SECONDS)
            if parent_connection in ready:
                try:
                    command = parent_connection.recv()
                except (EOFError, OSError):
                    parent_available = False
                    termination_requested.set()
                    command = None
                if command is _Command.ADMIT and not admitted and not (
                        termination_requested.is_set()):
                    admitted = True
                    outer_connection.send(_Command.ADMIT)
                elif command is _Command.CANCEL:
                    termination_requested.set()
            if inner_available and outer_connection in ready:
                try:
                    payload = outer_connection.recv()
                except (EOFError, OSError):
                    inner_available = False
                    payload = None
                if (isinstance(payload, tuple) and len(payload) == 2 and
                        payload[0] is _Event.INNER_DRAINED and
                        isinstance(payload[1], BoundaryResult)):
                    result = payload[1]
            if inner_available and not _identity_matches(inner):
                inner_available = False
            if not inner_available and result is None:
                _converge_family()
                result = BoundaryResult(
                    guardian,
                    InvocationOutcome(
                        InvocationOutcomeKind.FAILED,
                        error=BoundaryExecutionError(
                            'Inner warden exited before boundary completion.')))

        keep_inner = (frozenset({inner}) if inner_available and
                      _identity_matches(inner) else frozenset())
        _converge_family(keep_exact=keep_inner)
        try:
            parent_connection.send(
                _BoundaryEnvelope(boundary_token, _Event.RESULT, result))
        except (BrokenPipeError, EOFError, OSError):
            parent_available = False
            termination_requested.set()

        # A cancellation signal requests guardian-owned drain; it does not
        # waive the durable receipt protocol.  Only loss of the parent endpoint
        # proves that no API process remains available to acknowledge receipt.
        while parent_available and not receipt_received:
            watched = [parent_connection]
            if inner_available:
                watched.append(outer_connection)
            ready = multiprocessing_connection.wait(
                watched, timeout=_BOUNDARY_POLL_SECONDS)
            if parent_connection in ready:
                try:
                    command = parent_connection.recv()
                except (EOFError, OSError):
                    parent_available = False
                    termination_requested.set()
                    command = None
                if command is _Command.RECEIPT:
                    receipt_received = True
            if inner_available and outer_connection in ready:
                try:
                    outer_connection.recv()
                except (EOFError, OSError):
                    inner_available = False

        if inner_available:
            try:
                outer_connection.send(_Command.FINALIZE)
            except (BrokenPipeError, EOFError, OSError):
                inner_available = False
        _converge_family()
        try:
            os.waitpid(inner_pid, 0)
        except ChildProcessError:
            pass
        _converge_family()
    finally:
        outer_connection.close()
        parent_connection.close()


class InvocationFuture(concurrent.futures.Future):
    """Future with explicit admission, cancellation, and receipt controls."""

    def __init__(self, guardian: ProcessIdentity,
                 control: multiprocessing_connection.Connection,
                 receipt_required: bool, boundary_token: str):
        super().__init__()
        self.guardian_identity = guardian
        self._control = control
        self._receipt_required = receipt_required
        self._boundary_token = boundary_token
        self._control_lock = threading.Lock()
        self._admitted = False
        self._cancel_requested = False
        self._receipt_acknowledged = False
        self._boundary_result: BoundaryResult | None = None
        self._boundary_released = threading.Event()
        self._boundary_release_error: AmbiguousBoundaryError | None = None

    @property
    def boundary_result(self) -> BoundaryResult | None:
        return self._boundary_result

    def wait_for_boundary_release(self, timeout: float | None = None) -> bool:
        """Wait until the guardian is reaped and its executor lane is free."""
        released = self._boundary_released.wait(timeout)
        if not released:
            return False
        if self._boundary_release_error is not None:
            raise self._boundary_release_error
        return True

    def _publish_boundary_release(self,
                                  error: AmbiguousBoundaryError | None = None
                                 ) -> None:
        """Publish the monitor's exact lane-release proof or ambiguity."""
        self._boundary_release_error = error
        self._boundary_released.set()

    def admit(self) -> None:
        """Permit handler effects after durable guardian publication."""
        with self._control_lock:
            if self._cancel_requested:
                raise RuntimeError('Cannot admit a cancelled invocation.')
            if self._admitted:
                raise RuntimeError('Invocation was already admitted.')
            self._control.send(_Command.ADMIT)
            self._admitted = True

    def request_cancel(self) -> None:
        """Request guardian-owned drain without claiming Future cancellation."""
        with self._control_lock:
            if self._receipt_acknowledged:
                return
            self._cancel_requested = True
            try:
                self._control.send(_Command.CANCEL)
            except (BrokenPipeError, EOFError, OSError):
                pass

    def acknowledge_receipt(self) -> None:
        """Release the guardian only after durable completion convergence."""
        with self._control_lock:
            if self._receipt_acknowledged:
                return
            if self._boundary_result is None:
                raise RuntimeError('Boundary result is not available yet.')
            self._acknowledge_receipt_locked()

    def _acknowledge_receipt_locked(self) -> None:
        """Send one receipt while holding ``_control_lock``."""
        if self._receipt_acknowledged:
            return
        try:
            self._control.send(_Command.RECEIPT)
        except (BrokenPipeError, EOFError, OSError):
            # A proven BoundaryResult already establishes family absence.
            # Losing the lifetime pipe after the caller made the receipt
            # durable cannot invalidate that proof or wedge its monitor.
            pass
        self._receipt_acknowledged = True

    def _make_self_acknowledging(self) -> None:
        """Waive caller receipt ownership without racing result publication."""
        with self._control_lock:
            self._receipt_required = False
            if self._boundary_result is not None:
                self._acknowledge_receipt_locked()

    def _publish_boundary_result(self, result: BoundaryResult) -> None:
        with self._control_lock:
            if self._boundary_result is not None:
                return
            self._boundary_result = result
            if not self._receipt_required:
                self._acknowledge_receipt_locked()
        outcome = result.outcome
        if outcome.kind is InvocationOutcomeKind.SUCCEEDED:
            self.set_result(outcome.value)
        elif outcome.kind in (InvocationOutcomeKind.PRE_EFFECT,
                              InvocationOutcomeKind.CANCELLED):
            error = outcome.error
            self.set_exception(error if isinstance(error, Exception) else
                               concurrent.futures.CancelledError())
        elif outcome.kind is InvocationOutcomeKind.RETRYABLE:
            error = outcome.error
            self.set_exception(
                error if isinstance(error, Exception) else
                BoundaryExecutionError('Retryable outcome had no exception.'))
        else:
            error = outcome.error
            if isinstance(error, Exception):
                self.set_exception(error)
            elif isinstance(error, BaseException):
                wrapped = BoundaryExecutionError(
                    f'Handler terminated with {type(error).__name__}: {error}')
                wrapped.__cause__ = error
                self.set_exception(wrapped)
            else:
                self.set_exception(
                    BoundaryExecutionError('Invocation boundary failed.'))


class DisposableExecutor:
    """Finite executor that creates one owned process boundary per task.

    Callables, initializer state, and arguments must support standard pickle.
    """

    def __init__(
        self,
        max_workers: int | None = None,
        initializer: Callable | None = None,
        initargs: tuple = (),
        on_ambiguous_boundary: Callable[[AmbiguousBoundaryError], None] |
        None = None):
        self.max_workers = max_workers
        self.workers: dict[int, multiprocessing_process.BaseProcess] = {}
        self._invocations: dict[int, _InvocationRecord] = {}
        self._shutdown = False
        self._lock = threading.Lock()
        self._start_condition = threading.Condition(self._lock)
        self._starting_workers = 0
        self._initializer = initializer
        self._initargs = initargs
        self._ambiguity_error: AmbiguousBoundaryError | None = None
        self._on_ambiguous_boundary = on_ambiguous_boundary

    @property
    def poisoned(self) -> bool:
        """Whether missing boundary proof permanently poisoned this lane."""
        with self._lock:
            return self._ambiguity_error is not None

    def _raise_if_poisoned_locked(self) -> None:
        if self._ambiguity_error is not None:
            error = AmbiguousBoundaryError(
                'Invocation executor lane is poisoned by an unproven process '
                'family and cannot accept or release capacity.')
            error.__cause__ = self._ambiguity_error
            raise error

    def _poison(self, error: AmbiguousBoundaryError) -> None:
        callback = None
        with self._start_condition:
            if self._ambiguity_error is None:
                self._ambiguity_error = error
                callback = self._on_ambiguous_boundary
            self._start_condition.notify_all()
        if callback is not None:
            try:
                callback(error)
            # Synchronous poison hook: boundary ownership remains poisoned even
            # when the process-level termination callback itself fails.
            except BaseException:  # noqa: ASYNC103  # pylint: disable=broad-except
                logger.exception('Ambiguous-boundary termination hook failed.')

    def _claim_start_slot(self,
                          admission_deadline_monotonic: float | None = None
                         ) -> None:
        """Atomically convert available capacity into a starting invocation."""
        with self._start_condition:
            self._raise_if_poisoned_locked()
            if self._shutdown:
                raise RuntimeError(
                    'Cannot submit task after executor shutdown.')
            if (admission_deadline_monotonic is not None and
                    time.monotonic() >= admission_deadline_monotonic):
                raise TimeoutError(
                    'Invocation admission deadline expired before capacity '
                    'claim.')
            if (self.max_workers is not None and
                    len(self.workers) + self._starting_workers
                    >= self.max_workers):
                raise exceptions.ExecutionPoolFullError(
                    'Maximum workers reached.')
            self._starting_workers += 1

    def _monitor_boundary(
            self, record: _InvocationRecord,
            connection: multiprocessing_connection.Connection) -> None:
        result_seen = False
        receive_error: BaseException | None = None
        connection_closed = False
        release_error: AmbiguousBoundaryError | None = None
        try:
            while True:
                payload = None
                try:
                    payload = connection.recv()
                except EOFError:
                    break
                # Dedicated synchronous boundary monitor: transport failures
                # become proof failures rather than escaping the owner thread.
                except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
                    receive_error = e
                    break  # noqa: ASYNC104
                if (isinstance(payload, _BoundaryEnvelope) and
                        payload.token == record.future._boundary_token and  # pylint: disable=protected-access
                        payload.event is _Event.RESULT
                        and isinstance(payload.payload, BoundaryResult)):
                    result = payload.payload
                    if result.guardian != record.future.guardian_identity:
                        receive_error = BoundaryExecutionError(
                            'Guardian identity changed across boundary result.')
                        record.future.request_cancel()
                        connection.close()
                        connection_closed = True
                        break
                    if not result.family_drained:
                        receive_error = BoundaryExecutionError(
                            'Invocation result lacks family-drain proof.')
                        record.future.request_cancel()
                        connection.close()
                        connection_closed = True
                        break
                    if not result_seen:
                        result_seen = True
                        record.future._publish_boundary_result(  # pylint: disable=protected-access
                            result)
            guardian_reaped = False
            if result_seen:
                # A spawn sentinel can remain open in an already-drained orphan
                # descendant. Poll the authenticated direct child itself across
                # the existing reap horizon instead of spending that horizon on
                # a false-open sentinel and checking waitpid only once afterward.
                guardian_reaped = _wait_for_exact_direct_child_reap(
                    record.guardian, record.future.guardian_identity,
                    _PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
            else:
                try:
                    record.guardian.join(
                        timeout=_PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
                except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
                    receive_error = e
                guardian_reaped = record.guardian.exitcode is not None
            if not guardian_reaped and receive_error is None:
                receive_error = BoundaryExecutionError(
                    f'Invocation guardian {record.guardian.pid} did not reap '
                    'after its result channel closed.')
            if not result_seen or not guardian_reaped:
                ambiguity_error = AmbiguousBoundaryError(
                    f'Invocation guardian {record.guardian.pid} exited without '
                    'boundary proof or complete lifetime proof '
                    f'(exit code {record.guardian.exitcode}).')
                ambiguity_error.__cause__ = receive_error
                release_error = ambiguity_error
                self._poison(ambiguity_error)
                if not record.future.done():
                    record.future.set_exception(ambiguity_error)
        finally:
            if not connection_closed:
                connection.close()
            pid = record.guardian.pid
            if pid is not None:
                with self._start_condition:
                    self.workers.pop(pid, None)
                    self._invocations.pop(pid, None)
                    self._start_condition.notify_all()
            record.future._publish_boundary_release(  # pylint: disable=protected-access
                release_error)

    def _cleanup_unregistered_start(
            self, guardian: multiprocessing_process.BaseProcess,
            connection: multiprocessing_connection.Connection,
            boundary_token: str, identity: ProcessIdentity) -> bool:
        """Boundedly cancel a start and return authenticated drain proof."""
        proof_seen = False

        def receive_available() -> None:
            nonlocal proof_seen
            while True:
                try:
                    available = connection.poll(0)
                except (EOFError, OSError):
                    return
                if not available:
                    return
                try:
                    payload = connection.recv()
                except (EOFError, OSError):
                    return
                if (isinstance(payload, _BoundaryEnvelope) and
                        payload.token == boundary_token and
                        payload.event is _Event.RESULT and
                        isinstance(payload.payload, BoundaryResult) and
                        payload.payload.guardian == identity and
                        payload.payload.family_drained):
                    proof_seen = True
                    try:
                        connection.send(_Command.RECEIPT)
                    except (BrokenPipeError, EOFError, OSError):
                        pass

        try:
            connection.send(_Command.CANCEL)
        except (BrokenPipeError, EOFError, OSError):
            pass
        deadline = time.monotonic() + _BOUNDARY_START_CLEANUP_TIMEOUT_SECONDS
        while _identity_matches(identity) and time.monotonic() < deadline:
            try:
                connection.poll(
                    min(_BOUNDARY_POLL_SECONDS,
                        max(0, deadline - time.monotonic())))
            except (EOFError, OSError):
                time.sleep(_BOUNDARY_POLL_SECONDS)
            receive_available()
        receive_available()

        if _identity_matches(identity):
            _send_exact_signal(identity, signal.SIGTERM)
            term_deadline = time.monotonic() + _HANDLER_CANCEL_GRACE_SECONDS
            while (_identity_matches(identity) and
                   time.monotonic() < term_deadline):
                try:
                    connection.poll(_BOUNDARY_POLL_SECONDS)
                except (EOFError, OSError):
                    time.sleep(_BOUNDARY_POLL_SECONDS)
                receive_available()
        if _identity_matches(identity):
            _send_exact_signal(identity, signal.SIGKILL)

        guardian.join(timeout=_PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
        receive_available()
        return (proof_seen and not _identity_matches(identity) and
                guardian.exitcode is not None)

    def submit(self,
               fn: Callable,
               *args: Any,
               admission_gated: bool = False,
               receipt_required: bool = False,
               capability_fd: int | None = None,
               admission_deadline_monotonic: float | None = None,
               **kwargs: Any) -> InvocationFuture:
        """Start one boundary; optionally leave handler effects unadmitted."""
        admission_deadline_monotonic = _require_admission_deadline(
            admission_deadline_monotonic,
            admission_gated=admission_gated,
            receipt_required=receipt_required)
        self._claim_start_slot(admission_deadline_monotonic)
        return self._submit_claimed(
            fn,
            *args,
            admission_gated=admission_gated,
            receipt_required=receipt_required,
            capability_fd=capability_fd,
            admission_deadline_monotonic=(admission_deadline_monotonic),
            **kwargs)

    def _submit_claimed(self,
                        fn: Callable,
                        *args: Any,
                        admission_gated: bool = False,
                        receipt_required: bool = False,
                        capability_fd: int | None = None,
                        admission_deadline_monotonic: float | None = None,
                        **kwargs: Any) -> InvocationFuture:
        """Start a boundary after this executor already owns one start slot."""
        parent_connection = None
        guardian_connection = None
        guardian = None
        boundary_token = secrets.token_hex(32)
        startup_identity = None
        startup_proof_seen = False
        registered = False
        try:
            # Keep invocation-specific deserialization, reducers, and their
            # descriptor-backed objects out of both boundary owners.  The final
            # handler decodes this payload only after quarantining inherited raw
            # descriptors; a stale object can therefore never close a reused
            # protocol FD.
            invocation_payload = _serialize_invocation(fn, self._initializer,
                                                       self._initargs, args,
                                                       kwargs)
            if (admission_deadline_monotonic is not None and
                    time.monotonic() >= admission_deadline_monotonic):
                raise TimeoutError(
                    'Invocation admission deadline expired during '
                    'serialization.')
            parent_connection, guardian_connection = multiprocessing.Pipe(
                duplex=True)
            guardian = _SPAWN_CONTEXT.Process(
                target=_outer_guardian_main,
                args=(guardian_connection, boundary_token, invocation_payload,
                      (multiprocessing_reduction.DupFd(capability_fd)
                       if capability_fd is not None else None)),
                daemon=False)
            guardian.start()
            guardian_connection.close()
            guardian_connection = None
            guardian_pid = guardian.pid
            if guardian_pid is None:
                raise BoundaryExecutionError(
                    'Invocation guardian has no parent-observed PID.')
            try:
                startup_identity = ProcessIdentity(
                    guardian_pid, _read_process_start_time_ticks(guardian_pid))
            except (FileNotFoundError, ProcessLookupError, OSError,
                    ValueError) as e:
                raise BoundaryExecutionError(
                    'Invocation guardian vanished before its process birth '
                    'identity could be observed.') from e
            startup_timeout = float(_BOUNDARY_START_TIMEOUT_SECONDS)
            if admission_deadline_monotonic is not None:
                remaining = admission_deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        'Invocation admission deadline expired before ready.')
                startup_timeout = min(startup_timeout, remaining)
            if not parent_connection.poll(startup_timeout):
                raise TimeoutError('Invocation guardian did not become ready.')
            ready = parent_connection.recv()
            if (admission_deadline_monotonic is not None and
                    time.monotonic() >= admission_deadline_monotonic):
                raise TimeoutError(
                    'Invocation admission deadline expired after ready.')
            if (isinstance(ready, _BoundaryEnvelope) and
                    ready.token == boundary_token and
                    ready.event is _Event.RESULT and
                    isinstance(ready.payload, BoundaryResult) and
                    ready.payload.guardian == startup_identity and
                    ready.payload.family_drained):
                startup_proof_seen = True
                try:
                    parent_connection.send(_Command.RECEIPT)
                except (BrokenPipeError, EOFError, OSError):
                    pass
                raise BoundaryExecutionError(
                    'Invocation guardian failed before admission readiness.')
            if (not isinstance(ready, _BoundaryEnvelope) or
                    ready.token != boundary_token or
                    ready.event is not _Event.READY or
                    not isinstance(ready.payload, ProcessIdentity)):
                raise BoundaryExecutionError(
                    'Invocation guardian returned invalid admission identity.')
            identity = ready.payload
            if startup_identity != identity:
                raise BoundaryExecutionError(
                    'Invocation guardian admission identity does not match '
                    'the parent-observed process birth identity.')
            future = InvocationFuture(identity, parent_connection,
                                      receipt_required, boundary_token)
            assert future.set_running_or_notify_cancel()
            record = _InvocationRecord(guardian, future)
            monitor = threading.Thread(target=self._monitor_boundary,
                                       args=(record, parent_connection),
                                       name=(f'invocation-boundary-monitor-'
                                             f'{identity.pid}'),
                                       daemon=False)
            record.monitor = monitor
            monitor_start_error: BaseException | None = None
            with self._start_condition:
                self.workers[identity.pid] = guardian
                self._invocations[identity.pid] = record
                registered = True
                try:
                    # Holding the condition here makes registration and monitor
                    # ownership atomic to shutdown.  Process startup happened
                    # before acquiring this lock.
                    monitor.start()
                # Thread creation is synchronous; retain ownership and run the
                # monitor inline for every BaseException.
                except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
                    monitor_start_error = e
                self._starting_workers -= 1
                self._start_condition.notify_all()
            if monitor_start_error is not None:
                # No caller owns this accepted boundary yet.  Cancel it before
                # admission and synchronously run the same result/reap owner.
                future._make_self_acknowledging()  # pylint: disable=protected-access
                future.request_cancel()
                self._monitor_boundary(record, parent_connection)
                raise monitor_start_error
            if not admission_gated:
                try:
                    if (admission_deadline_monotonic is not None and
                            time.monotonic() >= admission_deadline_monotonic):
                        raise TimeoutError(
                            'Invocation admission deadline expired before '
                            'admission.')
                    future.admit()
                    if (admission_deadline_monotonic is not None and
                            time.monotonic() >= admission_deadline_monotonic):
                        future.request_cancel()
                        raise TimeoutError(
                            'Invocation admission deadline expired during '
                            'admission.')
                except BaseException as admission_error:
                    # The monitor owns the accepted boundary, but no caller owns
                    # its Future yet.  Make it self-acknowledging, cancel it,
                    # and synchronously prove lane release before exposing the
                    # admission failure.  Otherwise a deadline crossing inside
                    # ``admit()`` could leave an unobservable live boundary.
                    future._make_self_acknowledging()  # pylint: disable=protected-access
                    future.request_cancel()
                    release_timeout = (_BOUNDARY_START_CLEANUP_TIMEOUT_SECONDS +
                                       _PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
                    if not future.wait_for_boundary_release(release_timeout):
                        ambiguity = AmbiguousBoundaryError(
                            'Invocation automatic admission failed without '
                            'guardian-reap and executor-lane-release proof.')
                        self._poison(ambiguity)
                        raise ambiguity from admission_error
                    raise
            return future
        except BaseException as submit_error:
            if guardian_connection is not None:
                try:
                    guardian_connection.close()
                except OSError:
                    pass
            if not registered:
                cleanup_proven = guardian is None or guardian.pid is None
                cleanup_error: BaseException | None = None
                try:
                    if parent_connection is not None:
                        if (guardian is not None and
                                startup_identity is not None):
                            if startup_proof_seen:
                                guardian.join(timeout=(
                                    _PROCESS_REAP_PROOF_TIMEOUT_SECONDS))
                                cleanup_proven = (
                                    not _identity_matches(startup_identity) and
                                    guardian.exitcode is not None)
                            else:
                                cleanup_proven = (
                                    self._cleanup_unregistered_start(
                                        guardian, parent_connection,
                                        boundary_token, startup_identity))
                    elif guardian is not None and guardian.pid is not None:
                        guardian.join(
                            timeout=_PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
                # Synchronous startup cleanup must convert every failure into an
                # ambiguity proof before releasing process ownership.
                except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
                    cleanup_error = e
                    cleanup_proven = False
                finally:
                    if parent_connection is not None:
                        try:
                            parent_connection.close()
                        except OSError:
                            pass
                    with self._start_condition:
                        self._starting_workers -= 1
                        self._start_condition.notify_all()
                if not cleanup_proven:
                    ambiguity = AmbiguousBoundaryError(
                        'Invocation guardian startup failed without an '
                        'authenticated family-drain result.')
                    ambiguity.__cause__ = cleanup_error or submit_error
                    self._poison(ambiguity)
                    raise ambiguity from submit_error  # noqa: ASYNC104
            raise

    def has_idle_workers(self) -> bool:
        with self._lock:
            if self._ambiguity_error is not None:
                return False
            if self.max_workers is None:
                return True
            return (len(self.workers) + self._starting_workers
                    < self.max_workers)

    def available_slots(self) -> int | None:
        """Return immediately admissible capacity, or None if unbounded."""
        with self._lock:
            if self._ambiguity_error is not None:
                return 0
            if self.max_workers is None:
                return None
            return max(
                0,
                self.max_workers - len(self.workers) - self._starting_workers)

    def shutdown(self,
                 timeout: float = _BOUNDARY_SHUTDOWN_WAIT_SECONDS) -> None:
        """Cancel boundaries, or retain them for a retry after ``timeout``.

        Receipt-required boundaries are intentionally not acknowledged here:
        their durable owner must first commit convergence.  A bounded pending
        error leaves their monitor and process records intact, so callers can
        acknowledge the receipt and retry shutdown safely.
        """
        if timeout < 0:
            raise ValueError('Shutdown timeout must be non-negative.')
        deadline = time.monotonic() + timeout
        with self._start_condition:
            self._shutdown = True
            while self._starting_workers:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BoundaryShutdownPendingError(
                        tuple(record.future.guardian_identity
                              for record in self._invocations.values()),
                        self._starting_workers)
                self._start_condition.wait(timeout=remaining)
            records = tuple(self._invocations.values())
        for record in records:
            record.future.request_cancel()
        for record in records:
            if record.monitor is not None:
                record.monitor.join(timeout=max(0, deadline - time.monotonic()))
        pending = tuple(
            record.future.guardian_identity
            for record in records
            if record.monitor is not None and record.monitor.is_alive())
        if pending:
            raise BoundaryShutdownPendingError(pending)
        _require_processes_reaped([record.guardian for record in records])
        with self._lock:
            self._raise_if_poisoned_locked()


def _require_processes_reaped(
        processes: list[multiprocessing_process.BaseProcess]) -> None:
    """Fail closed unless every supplied process is stopped and reaped."""
    unreaped: list[int] = []
    for child in processes:
        pid = child.pid
        if pid is None:
            continue
        try:
            child.join(timeout=_PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
            if child.is_alive() or child.exitcode is None:
                unreaped.append(pid)
        except ValueError:
            continue
        except (AssertionError, OSError) as e:
            raise RuntimeError(
                f'Could not prove executor process {pid} was reaped.') from e
    if unreaped:
        raise RuntimeError('Executor shutdown could not prove child process '
                           f'reaping for PIDs {sorted(unreaped)}.')


class BurstableExecutor:
    """Finite two-lane facade over the canonical disposable executor."""

    def __init__(self,
                 garanteed_workers: int,
                 burst_workers: int = 0,
                 **kwargs: Any):
        self._reservation_lock = threading.Lock()
        self._reservation_owner = object()
        self._next_reservation_id = 1
        self._reservations: dict[int, IdleWorkerReservation] = {}
        self._reserved_by_lane = {
            _ExecutorLane.GUARANTEED: 0,
            _ExecutorLane.BURST: 0,
        }
        self._shutdown = False
        self._ambiguity_error: AmbiguousBoundaryError | None = None
        external_ambiguity_callback = kwargs.pop('on_ambiguous_boundary', None)

        def poison_facade(error: AmbiguousBoundaryError) -> None:
            callback = None
            with self._reservation_lock:
                if self._ambiguity_error is None:
                    self._ambiguity_error = error
                    callback = external_ambiguity_callback
            if callback is not None:
                callback(error)

        self._executor = (DisposableExecutor(
            garanteed_workers, on_ambiguous_boundary=poison_facade, **kwargs)
                          if garanteed_workers > 0 else None)
        self._burst_executor = (DisposableExecutor(
            burst_workers, on_ambiguous_boundary=poison_facade, **kwargs)
                                if burst_workers > 0 else None)

    def _raise_if_poisoned_locked(self) -> None:
        if self._ambiguity_error is not None:
            error = AmbiguousBoundaryError(
                'Burstable executor is poisoned by an unproven process family '
                'and cannot accept or release capacity.')
            error.__cause__ = self._ambiguity_error
            raise error

    def _lane_executor(self, lane: _ExecutorLane) -> DisposableExecutor | None:
        if lane is _ExecutorLane.GUARANTEED:
            return self._executor
        return self._burst_executor

    def _lane_has_unreserved_capacity(self, lane: _ExecutorLane) -> bool:
        executor = self._lane_executor(lane)
        if executor is None:
            return False
        available = executor.available_slots()
        return (available is None or self._reserved_by_lane[lane] < available)

    def try_reserve_idle_worker(self) -> IdleWorkerReservation | None:
        with self._reservation_lock:
            if self._shutdown or self._ambiguity_error is not None:
                return None
            for lane in (_ExecutorLane.GUARANTEED, _ExecutorLane.BURST):
                if self._lane_has_unreserved_capacity(lane):
                    reservation = IdleWorkerReservation(
                        self._reservation_owner, self._next_reservation_id,
                        lane)
                    self._next_reservation_id += 1
                    self._reservations[reservation.reservation_id] = reservation
                    self._reserved_by_lane[lane] += 1
                    return reservation
            return None

    def _pop_reservation_locked(
            self, reservation: IdleWorkerReservation) -> _ExecutorLane:
        if (not isinstance(reservation, IdleWorkerReservation) or
                reservation.owner_token is not self._reservation_owner):
            raise ValueError('Idle-worker reservation belongs to another '
                             'executor.')
        current = self._reservations.get(reservation.reservation_id)
        if current is not reservation:
            raise ValueError('Idle-worker reservation is stale or consumed.')
        del self._reservations[reservation.reservation_id]
        self._reserved_by_lane[reservation.lane] -= 1
        return reservation.lane

    def release_idle_worker_reservation(
            self, reservation: IdleWorkerReservation) -> None:
        with self._reservation_lock:
            self._pop_reservation_locked(reservation)

    def submit_reserved(self,
                        reservation: IdleWorkerReservation,
                        fn: Callable,
                        *args: Any,
                        admission_gated: bool = False,
                        receipt_required: bool = False,
                        capability_fd: int | None = None,
                        admission_deadline_monotonic: float | None = None,
                        **kwargs: Any) -> InvocationFuture:
        admission_deadline_monotonic = _require_admission_deadline(
            admission_deadline_monotonic,
            admission_gated=admission_gated,
            receipt_required=receipt_required)
        with self._reservation_lock:
            self._raise_if_poisoned_locked()
            if self._shutdown:
                raise RuntimeError(
                    'Cannot submit task after executor shutdown.')
            lane = self._pop_reservation_locked(reservation)
            lane_executor = self._lane_executor(lane)
            if lane_executor is None:
                raise RuntimeError('Reserved executor lane is absent.')
            # Convert the facade reservation into the lane's starting-capacity
            # claim before exposing that slot to another reservation attempt.
            # This operation acquires no process resources.
            lane_executor._claim_start_slot(  # pylint: disable=protected-access
                admission_deadline_monotonic)
        # Process startup is deliberately outside the reservation lock.  The
        # opaque reservation and corresponding lane capacity were both claimed
        # atomically above.
        return lane_executor._submit_claimed(  # pylint: disable=protected-access
            fn,
            *args,
            admission_gated=admission_gated,
            receipt_required=receipt_required,
            capability_fd=capability_fd,
            admission_deadline_monotonic=admission_deadline_monotonic,
            **kwargs)

    def shutdown(self,
                 timeout: float = _BOUNDARY_SHUTDOWN_WAIT_SECONDS) -> None:
        if timeout < 0:
            raise ValueError('Shutdown timeout must be non-negative.')
        deadline = time.monotonic() + timeout
        with self._reservation_lock:
            self._shutdown = True
            self._reservations.clear()
            self._reserved_by_lane = {
                _ExecutorLane.GUARANTEED: 0,
                _ExecutorLane.BURST: 0,
            }
            executors = tuple(executor for executor in (self._executor,
                                                        self._burst_executor)
                              if executor is not None)
        errors: list[BaseException] = []
        for executor in executors:
            try:
                executor.shutdown(timeout=max(0, deadline - time.monotonic()))
            # Synchronous shutdown aggregates every lane proof failure before
            # choosing the authoritative fail-closed result.
            except BaseException as e:  # noqa: ASYNC103  # pylint: disable=broad-except
                errors.append(e)
        if errors:
            with self._reservation_lock:
                ambiguity_error = self._ambiguity_error
            if ambiguity_error is not None:
                error = AmbiguousBoundaryError(
                    'Burstable executor shutdown cannot prove absence after '
                    'an ambiguous invocation boundary.')
                error.__cause__ = ambiguity_error
                raise error
            pending_errors = [
                error for error in errors
                if isinstance(error, BoundaryShutdownPendingError)
            ]
            if len(pending_errors) == len(errors):
                guardians = tuple(identity for error in pending_errors
                                  for identity in error.guardians)
                starting_workers = sum(
                    error.starting_workers for error in pending_errors)
                raise BoundaryShutdownPendingError(
                    guardians, starting_workers) from pending_errors[0]
            raise RuntimeError('One or more invocation lanes did not prove '
                               'boundary shutdown.') from errors[0]
        with self._reservation_lock:
            self._raise_if_poisoned_locked()
