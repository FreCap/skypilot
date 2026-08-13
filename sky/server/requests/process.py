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


@dataclasses.dataclass
class _InvocationRecord:
    guardian: multiprocessing_process.BaseProcess
    future: 'InvocationFuture'
    monitor: threading.Thread | None = None


_SPAWN_CONTEXT = multiprocessing.get_context('spawn')


def _read_process_start_time_ticks(pid: int) -> int:
    content = (pathlib.Path('/proc') / str(pid) /
               'stat').read_text(encoding='utf-8')
    comm_end = content.rfind(')')
    if comm_end < 2 or not content.startswith(f'{pid} ('):
        raise ValueError(f'Malformed procfs identity for PID {pid}.')
    fields_after_comm = content[comm_end + 1:].split()
    if len(fields_after_comm) <= 19:
        raise ValueError(f'Malformed procfs identity for PID {pid}.')
    value = int(fields_after_comm[19])
    if value <= 0:
        raise ValueError(f'Invalid procfs birth identity for PID {pid}.')
    return value


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


def _close_unrelated_fds(keep_fds: frozenset[int]) -> None:
    """Close raw descriptors inherited across an internal ``fork()``.

    In particular, Python's spawn implementation leaves the guardian side of
    its multiprocessing sentinel open.  A forked handler retaining that writer
    would make the API's ``Process.join()`` wait forever after both owners die.
    Internal descendants therefore inherit only their explicit protocol
    endpoints and standard streams.
    """
    keep_fds = keep_fds | frozenset({0, 1, 2})
    try:
        open_fds = tuple(
            int(name) for name in os.listdir('/proc/self/fd') if name.isdigit())
    except OSError as e:
        raise BoundaryExecutionError(
            f'Could not enumerate inherited process descriptors: {e}') from e
    for fd in open_fds:
        if fd in keep_fds:
            continue
        try:
            os.close(fd)
        except OSError as e:
            if e.errno != errno.EBADF:
                raise BoundaryExecutionError(
                    f'Could not close inherited descriptor {fd}: {e}') from e


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
    except BaseException:  # pylint: disable=broad-except
        return BoundaryExecutionError(
            f'Untransportable {type(error).__name__}: {error}')
    return error


def _normalize_outcome(value: Any) -> InvocationOutcome:
    if isinstance(value, InvocationOutcome):
        return value
    return InvocationOutcome(InvocationOutcomeKind.SUCCEEDED, value=value)


def _handler_main(fn: Callable, initializer: Callable | None, initargs: tuple,
                  report_connection: multiprocessing_connection.Connection,
                  finalize_connection: multiprocessing_connection.Connection,
                  capability_fd: int | None, args: tuple,
                  kwargs: dict[str, Any]) -> None:
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
            if initializer is not None:
                initializer(*initargs)
            outcome = _normalize_outcome(fn(*args, **kwargs))
        except KeyboardInterrupt as e:
            outcome = InvocationOutcome(InvocationOutcomeKind.CANCELLED,
                                        error=_transportable_error(e))
        except exceptions.ExecutionRetryableError as e:
            outcome = InvocationOutcome(InvocationOutcomeKind.RETRYABLE,
                                        error=_transportable_error(e))
        except BaseException as e:  # pylint: disable=broad-except
            outcome = InvocationOutcome(InvocationOutcomeKind.FAILED,
                                        error=_transportable_error(e))
        try:
            report_connection.send(_HandlerReport(outcome))
        except BaseException as e:  # pylint: disable=broad-except
            fallback = InvocationOutcome(
                InvocationOutcomeKind.FAILED,
                error=BoundaryExecutionError(
                    f'Could not transport handler outcome: {type(e).__name__}: '
                    f'{e}'))
            try:
                report_connection.send(_HandlerReport(fallback))
            except BaseException:  # pylint: disable=broad-except
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
    fn: Callable,
    initializer: Callable | None,
    initargs: tuple,
    args: tuple,
    kwargs: dict[str, Any],
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
        _close_unrelated_fds(frozenset(kept_fds))
        try:
            _handler_main(fn, initializer, initargs, report_child,
                          finalize_child, capability_fd, args, kwargs)
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
    fn: Callable,
    initializer: Callable | None,
    initargs: tuple,
    capability_fd: int | None,
    args: tuple,
    kwargs: dict[str, Any],
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
                fn,
                initializer,
                initargs,
                args,
                kwargs,
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
    fn: Callable,
    initializer: Callable | None,
    initargs: tuple,
    capability_handle: Any | None,
    args: tuple,
    kwargs: dict[str, Any],
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
        _close_unrelated_fds(frozenset(kept_fds))
        try:
            _inner_warden_main(inner_connection, parent_connection, guardian,
                               boundary_token, fn, initializer, initargs,
                               capability_fd, args, kwargs)
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

    @property
    def boundary_result(self) -> BoundaryResult | None:
        return self._boundary_result

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
            try:
                self._control.send(_Command.RECEIPT)
            except (BrokenPipeError, EOFError, OSError):
                # A proven BoundaryResult already establishes family absence.
                # Losing the lifetime pipe after the caller made the receipt
                # durable cannot invalidate that proof or wedge its monitor.
                pass
            self._receipt_acknowledged = True

    def _publish_boundary_result(self, result: BoundaryResult) -> None:
        if self._boundary_result is not None:
            return
        self._boundary_result = result
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
    """Finite executor that creates one owned process boundary per task."""

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
            except BaseException:  # pylint: disable=broad-except
                logger.exception('Ambiguous-boundary termination hook failed.')

    def _claim_start_slot(self) -> None:
        """Atomically convert available capacity into a starting invocation."""
        with self._start_condition:
            self._raise_if_poisoned_locked()
            if self._shutdown:
                raise RuntimeError(
                    'Cannot submit task after executor shutdown.')
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
        try:
            while True:
                payload = None
                try:
                    payload = connection.recv()
                except EOFError:
                    break
                except BaseException as e:  # pylint: disable=broad-except
                    receive_error = e
                    break
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
                        if not record.future._receipt_required:  # pylint: disable=protected-access
                            record.future.acknowledge_receipt()
            try:
                record.guardian.join(
                    timeout=_PROCESS_REAP_PROOF_TIMEOUT_SECONDS)
            except BaseException as e:  # pylint: disable=broad-except
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
               **kwargs: Any) -> InvocationFuture:
        """Start one boundary; optionally leave handler effects unadmitted."""
        self._claim_start_slot()
        return self._submit_claimed(fn,
                                    *args,
                                    admission_gated=admission_gated,
                                    receipt_required=receipt_required,
                                    capability_fd=capability_fd,
                                    **kwargs)

    def _submit_claimed(self,
                        fn: Callable,
                        *args: Any,
                        admission_gated: bool = False,
                        receipt_required: bool = False,
                        capability_fd: int | None = None,
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
            parent_connection, guardian_connection = multiprocessing.Pipe(
                duplex=True)
            guardian = _SPAWN_CONTEXT.Process(
                target=_outer_guardian_main,
                args=(guardian_connection, boundary_token, fn,
                      self._initializer, self._initargs,
                      (multiprocessing_reduction.DupFd(capability_fd)
                       if capability_fd is not None else None), args, kwargs),
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
            if not parent_connection.poll(_BOUNDARY_START_TIMEOUT_SECONDS):
                raise TimeoutError('Invocation guardian did not become ready.')
            ready = parent_connection.recv()
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
                except BaseException as e:  # pylint: disable=broad-except
                    monitor_start_error = e
                self._starting_workers -= 1
                self._start_condition.notify_all()
            if monitor_start_error is not None:
                # No caller owns this accepted boundary yet.  Cancel it before
                # admission and synchronously run the same result/reap owner.
                future.request_cancel()
                future._receipt_required = False  # pylint: disable=protected-access
                self._monitor_boundary(record, parent_connection)
                raise monitor_start_error
            if not admission_gated:
                try:
                    future.admit()
                except BaseException:
                    # The monitor owns the accepted boundary, but no caller owns
                    # its Future yet.  Make it self-acknowledging and cancel it.
                    future._receipt_required = False  # pylint: disable=protected-access
                    future.request_cancel()
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
                except BaseException as e:  # pylint: disable=broad-except
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
                    raise ambiguity from submit_error
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
                        **kwargs: Any) -> InvocationFuture:
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
            lane_executor._claim_start_slot()  # pylint: disable=protected-access
        # Process startup is deliberately outside the reservation lock.  The
        # opaque reservation and corresponding lane capacity were both claimed
        # atomically above.
        return lane_executor._submit_claimed(  # pylint: disable=protected-access
            fn,
            *args,
            admission_gated=admission_gated,
            receipt_required=receipt_required,
            capability_fd=capability_fd,
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
            except BaseException as e:  # pylint: disable=broad-except
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
