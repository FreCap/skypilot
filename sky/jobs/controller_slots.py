"""Runtime-owned fixed slots for disposable managed-job controllers."""

import dataclasses
import json
import os
import pathlib
import socket
import stat
import subprocess
import sys
import threading
import time
import typing
import uuid

from sky import sky_logging
from sky.jobs import constants as managed_job_constants
from sky.jobs import controller_fencing
from sky.jobs import state as managed_job_state
from sky.utils import controller_capability
from sky.utils import controller_utils

logger = sky_logging.init_logger(__name__)

_ADMISSION_TIMEOUT_SECONDS = 35
_JOIN_TIMEOUT_SECONDS = 10
_SHORT_LIVED_FAMILY_SECONDS = 30
_RESTART_BACKOFF_INITIAL_SECONDS = 1
_RESTART_BACKOFF_MAX_SECONDS = 30
_MAX_CONSECUTIVE_SHORT_LIVED_FAMILIES = 3


class ControllerSlotError(RuntimeError):
    """A fixed controller slot could not prove its ownership boundary."""


class ControllerSlotProofError(ControllerSlotError):
    """A slot family exited without exact stable-empty proof."""


class ControllerSlotNestedRequestProofError(ControllerSlotProofError):
    """An admitted slot's nested request family is not yet proven quiet."""


def _open_capability_transport(capability: str) -> int:
    """Return a CLOEXEC read FD containing one canonical raw capability."""
    controller_capability.digest(capability)
    if hasattr(os, 'pipe2'):
        read_fd, write_fd = os.pipe2(os.O_CLOEXEC)
    else:
        read_fd, write_fd = os.pipe()
        os.set_inheritable(read_fd, False)
        os.set_inheritable(write_fd, False)
    keep_read_fd = False
    try:
        payload = capability.encode('ascii')
        offset = 0
        while offset < len(payload):
            written = os.write(write_fd, payload[offset:])
            if written <= 0:
                raise OSError('Controller capability pipe made no progress.')
            offset += written
        keep_read_fd = True
        return read_fd
    finally:
        os.close(write_fd)
        if not keep_read_fd:
            os.close(read_fd)


class LocalControllerOriginCapabilityAuthority:
    """Private same-host authority for one local controller process birth."""

    def __init__(self, owner: tuple[str, int]) -> None:
        self._owner = owner
        self._capability: str | None = None
        self._path: str | None = None

    @property
    def capability(self) -> str:
        if self._capability is None:
            raise RuntimeError(
                'Local controller-origin capability is not published.')
        return self._capability

    @property
    def path(self) -> str:
        if self._path is None:
            raise RuntimeError(
                'Local controller-origin capability is not published.')
        return self._path

    @staticmethod
    def _authority_path(instance_id: str) -> pathlib.Path:
        return pathlib.Path(
            controller_capability.local_authority_path(instance_id))

    @staticmethod
    def _require_private_owned_directory(path: pathlib.Path) -> None:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory_stat = path.lstat()
        if (not stat.S_ISDIR(directory_stat.st_mode) or
                directory_stat.st_uid != os.geteuid()):
            raise ControllerSlotError(
                'Local controller-origin authority directory is not an '
                'owned directory.')
        os.chmod(path, 0o700)

    def publish(self) -> None:
        """Atomically publish a process-birth-bound capability hash."""
        if self._capability is not None or self._path is not None:
            raise RuntimeError(
                'Local controller-origin capability already published.')
        if controller_capability.get_process_local() is not None:
            raise RuntimeError(
                'Another process-local controller capability is installed.')
        # Lock down same-UID /proc reads before a raw bearer ever enters this
        # process, including direct callers outside LocalManagedJobRuntime.
        controller_capability.make_process_non_dumpable()
        capability = controller_capability.generate()
        capability_sha256 = controller_capability.digest_hex(capability)
        owner_pid = os.getpid()
        owner_start_ticks = _read_process_start_time_ticks(owner_pid)
        authority_path = self._authority_path(self._owner[0])
        self._require_private_owned_directory(authority_path.parent)
        payload = {
            'controller_instance_id': self._owner[0],
            'controller_generation': self._owner[1],
            'origin_capability_sha256': capability_sha256,
            'owner_pid': owner_pid,
            'owner_process_start_time_ticks': owner_start_ticks,
        }
        temporary_path = authority_path.with_name(
            f'.{authority_path.name}.{uuid.uuid4().hex}.tmp')
        descriptor = os.open(temporary_path,
                             os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        published = False
        authority_replaced = False
        try:
            with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
                json.dump(payload,
                          stream,
                          sort_keys=True,
                          separators=(',', ':'))
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, authority_path)
            authority_replaced = True
            authority_stat = authority_path.lstat()
            if (not stat.S_ISREG(authority_stat.st_mode) or
                    authority_stat.st_uid != os.geteuid() or
                    stat.S_IMODE(authority_stat.st_mode) != 0o600):
                raise ControllerSlotError(
                    'Local controller-origin authority is not a private owned '
                    'regular file.')
            directory_fd = os.open(authority_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            if not controller_capability.verify_local_authority(
                    str(authority_path), self._owner[0], self._owner[1],
                    capability):
                raise ControllerSlotError(
                    'Local controller-origin authority failed self-'
                    'verification.')
            published = True
        finally:
            if not published:
                try:
                    os.remove(temporary_path)
                except FileNotFoundError:
                    pass
                if authority_replaced:
                    try:
                        os.remove(authority_path)
                    except FileNotFoundError:
                        pass
        self._capability = capability
        self._path = str(authority_path)
        os.environ.pop(
            managed_job_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR, None)
        os.environ.pop(
            managed_job_constants.
            CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR, None)
        controller_capability.install_process_local(capability)

    def remove(self) -> None:
        """Remove authority only after every owned effect family has drained."""
        if self._path is not None:
            try:
                os.remove(self._path)
            except FileNotFoundError:
                pass
        if (self._capability is not None and
                controller_capability.get_process_local() == self._capability):
            controller_capability.clear_process_local()
        os.environ.pop(
            managed_job_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR, None)
        os.environ.pop(
            managed_job_constants.
            CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR, None)
        self._capability = None
        self._path = None


@dataclasses.dataclass
class _SlotFamily:
    identity: managed_job_state.ControllerSlotIdentity
    process: subprocess.Popen[bytes]
    control: socket.socket
    admitted: bool = False
    admitted_at: float | None = None


def _read_process_start_time_ticks(pid: int) -> int:
    with open(f'/proc/{pid}/stat', encoding='utf-8') as stream:
        content = stream.read()
    comm_end = content.rfind(')')
    if comm_end < 2 or not content.startswith(f'{pid} ('):
        raise ControllerSlotError(
            f'Malformed runtime owner identity for PID {pid}.')
    fields_after_comm = content[comm_end + 1:].split()
    if len(fields_after_comm) <= 19:
        raise ControllerSlotError(
            f'Malformed runtime owner identity for PID {pid}.')
    value = int(fields_after_comm[19])
    if value <= 0:
        raise ControllerSlotError(
            f'Invalid runtime owner identity for PID {pid}.')
    return value


def _read_message(control: socket.socket) -> dict[str, typing.Any] | None:
    data = bytearray()
    while True:
        chunk = control.recv(1)
        if not chunk:
            return None
        if chunk == b'\n':
            break
        data.extend(chunk)
        if len(data) > 64 * 1024:
            raise ControllerSlotError(
                'Managed-job controller slot message exceeded its bound.')
    value = json.loads(data.decode('utf-8'))
    if not isinstance(value, dict):
        raise ControllerSlotError(
            'Managed-job controller slot sent a malformed message.')
    return value


def _write_message(control: socket.socket, value: dict[str,
                                                       typing.Any]) -> None:
    control.sendall(
        json.dumps(value, separators=(',', ':')).encode('utf-8') + b'\n')


class ManagedJobControllerSlotSupervisor:
    """Own one eager, fixed set of local ControllerManager families."""

    def __init__(self,
                 owner: tuple[str, int],
                 slot_count: int | None = None,
                 on_failure: typing.Callable[[], None] | None = None,
                 child_env: dict[str, str] | None = None,
                 origin_capability: str | None = None):
        if slot_count is None:
            slot_count = controller_utils.get_number_of_jobs_controllers()
        if slot_count <= 0:
            raise ValueError(
                'Managed-job controller slot count must be positive.')
        self._owner = owner
        self._slot_count = slot_count
        self._on_failure = on_failure
        self._child_env = dict(child_env or {})
        environment_capability = self._child_env.get(
            managed_job_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR)
        if environment_capability is not None:
            raise ControllerSlotError(
                'Managed-job controller capability cannot use child_env.')
        self._origin_capability = origin_capability
        if self._origin_capability is not None:
            controller_capability.digest(self._origin_capability)
        # Child processes receive authority only through the one-shot pipe.
        # Scrub even caller-supplied values so no future callsite can silently
        # restore an inheritable raw/path representation.
        for sensitive_name in (
                managed_job_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR,
                managed_job_constants.
                CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
                managed_job_constants.CONTROLLER_CAPABILITY_FD_ENV_VAR):
            self._child_env.pop(sensitive_name, None)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._families: dict[int, _SlotFamily] = {}
        self._threads: list[threading.Thread] = []
        self._failure: BaseException | None = None
        self._unsafe_failure: BaseException | None = None
        self._pending_nested_quiescence: set[
            managed_job_state.ControllerSlotIdentity] = set()
        self._startup_complete = threading.Event()
        self._startup_succeeded = False
        self._started = False
        self._failure_callback_called = False

    @property
    def slot_count(self) -> int:
        return self._slot_count

    def _identity(self,
                  slot_id: int) -> managed_job_state.ControllerSlotIdentity:
        return self._owner[0], self._owner[1], slot_id, str(uuid.uuid4())

    def _manager_child_environment(self, slot_id: int,
                                   attempt: str) -> dict[str, str]:
        """Build a slot environment with no inheritable capability state."""
        child_env = dict(os.environ)
        child_env.update(self._child_env)
        for sensitive_name in (
                managed_job_constants.CONTROLLER_ORIGIN_CAPABILITY_ENV_VAR,
                managed_job_constants.
                CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH_ENV_VAR,
                managed_job_constants.CONTROLLER_CAPABILITY_FD_ENV_VAR):
            child_env.pop(sensitive_name, None)
        child_env[managed_job_constants.CONTROLLER_SLOT_ID_ENV_VAR] = str(
            slot_id)
        child_env[
            managed_job_constants.CONTROLLER_SLOT_ATTEMPT_ENV_VAR] = attempt
        return child_env

    def _prepare_family(
            self,
            identity: managed_job_state.ControllerSlotIdentity) -> _SlotFamily:
        instance_id, generation, slot_id, attempt = identity
        runtime_control, runner_control = socket.socketpair()
        runtime_control.settimeout(_ADMISSION_TIMEOUT_SECONDS)
        capability_fd: int | None = None
        process: subprocess.Popen[bytes] | None = None
        family: _SlotFamily | None = None
        runner_path = pathlib.Path(__file__).with_name(
            'managed_job_controller_runner.py')
        logs_dir = pathlib.Path(
            os.path.expanduser(managed_job_constants.JOBS_CONTROLLER_LOGS_DIR))
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = logs_dir / f'controller_slot_{slot_id}_{attempt}.log'
        controller_uuid = f'slot-{slot_id}-{attempt}'
        bootstrap_path = pathlib.Path(__file__).with_name(
            'managed_job_controller_bootstrap.py')
        command = json.dumps([
            sys.executable,
            '-u',
            '-S',
            str(bootstrap_path),
            controller_uuid,
            str(slot_id),
            attempt,
        ],
                             separators=(',', ':'))
        child_env = self._manager_child_environment(slot_id, attempt)
        try:
            if self._origin_capability is None:
                raise ControllerSlotError(
                    'Managed-job controller capability is not published.')
            capability_fd = _open_capability_transport(self._origin_capability)
            with log_path.open('ab', buffering=0) as controller_log:
                process = subprocess.Popen(  # pylint: disable=consider-using-with
                    [
                        sys.executable,
                        '-S',
                        str(runner_path),
                        str(runner_control.fileno()),
                        str(capability_fd),
                        str(os.getpid()),
                        str(_read_process_start_time_ticks(os.getpid())),
                        instance_id,
                        str(generation),
                        str(slot_id),
                        attempt,
                        command,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=controller_log,
                    stderr=subprocess.STDOUT,
                    env=child_env,
                    pass_fds=(runner_control.fileno(), capability_fd),
                    close_fds=True,
                )
            os.close(capability_fd)
            capability_fd = None
            family = _SlotFamily(identity, process, runtime_control)
            runner_control.close()
            ready = _read_message(runtime_control)
            if (ready is None or ready.get('type') != 'ready' or
                    ready.get('controller_instance_id') != instance_id or
                    ready.get('controller_generation') != str(generation) or
                    ready.get('controller_slot_id') != slot_id or
                    ready.get('controller_slot_attempt') != attempt or
                    ready.get('pid') != process.pid or
                    os.getpgid(process.pid) != process.pid):
                raise ControllerSlotError(
                    f'Managed-job controller slot {slot_id} returned an '
                    'invalid guardian identity.')
            if self._stop.is_set():
                raise ControllerSlotError(
                    f'Managed-job controller slot {slot_id} stopped during '
                    'preparation.')
            return family
        except BaseException as prepare_error:
            runner_control.close()
            if capability_fd is not None:
                os.close(capability_fd)
            if family is not None:
                try:
                    _write_message(runtime_control, {'type': 'terminate'})
                except OSError:
                    pass
                # Even a pre-admission helper is runtime-owned.  Do not drop
                # its local handle until both wardens publish exact emptiness.
                try:
                    self._wait_family_completion(family)
                except ControllerSlotProofError as proof_error:
                    self._set_family(slot_id, family)
                    with self._lock:
                        if self._unsafe_failure is None:
                            self._unsafe_failure = proof_error
                    raise proof_error from prepare_error
            else:
                runtime_control.close()
            raise

    def _send_admission(self, family: _SlotFamily) -> None:
        """Open one prepared family's immutable effect-admission gate."""
        # Treat a send attempt as effect-bearing before touching the socket.
        # If sendall reports an error after the full frame reached the guardian,
        # conservative shutdown must still quiesce any nested work it admitted.
        family.admitted = True
        family.admitted_at = time.monotonic()
        _write_message(family.control, {
            'type': 'admit',
            'controller_slot_attempt': family.identity[3],
        })

    def _publish_and_admit(self, family: _SlotFamily) -> bool:
        """Atomically publish a family and open its effect-admission gate.

        ``request_shutdown()`` takes the same lock before setting ``_stop``.
        It therefore either sees this family and terminates it, or wins first
        and prevents admission.  A prepared-but-unpublished replacement is
        synchronously drained by its slot thread in the latter case.
        """
        with self._lock:
            slot_id = family.identity[2]
            self._families[slot_id] = family
            if self._stop.is_set():
                return False
            self._send_admission(family)
            return True

    def _terminate_and_drain_unadmitted(self, family: _SlotFamily) -> None:
        """Drain a family that lost the runtime admission/shutdown race."""
        try:
            _write_message(family.control, {'type': 'terminate'})
        except OSError:
            pass
        self._wait_family_effect_quiescence(family)

    def _wait_for_started(self, family: _SlotFamily) -> None:
        slot_id = family.identity[2]
        attempt = family.identity[3]
        started = _read_message(family.control)
        if (started is None or started.get('type') != 'started' or
                started.get('controller_slot_id') != slot_id or
                started.get('controller_slot_attempt') != attempt):
            reason = None if started is None else started.get('reason')
            raise ControllerSlotError(
                f'Managed-job controller slot {slot_id} failed admission: '
                f'{reason!r}.')
        family.control.settimeout(None)

    @staticmethod
    def _completion_matches(
            message: dict[str, typing.Any],
            identity: managed_job_state.ControllerSlotIdentity) -> bool:
        instance_id, generation, slot_id, attempt = identity
        return (message.get('type') == 'complete' and
                message.get('controller_instance_id') == instance_id and
                message.get('controller_generation') == str(generation) and
                message.get('controller_slot_id') == slot_id and
                message.get('controller_slot_attempt') == attempt and
                message.get('descendants_empty') is True)

    def _wait_family_completion(self, family: _SlotFamily) -> None:
        completion_seen = False
        while True:
            try:
                message = _read_message(family.control)
            except TimeoutError:
                continue
            except (OSError, ValueError, json.JSONDecodeError) as e:
                raise ControllerSlotProofError(
                    f'Could not read managed-job slot {family.identity[2]} '
                    'completion proof.') from e
            if message is None:
                break
            if self._completion_matches(message, family.identity):
                completion_seen = True
        family.control.close()
        while family.process.poll() is None:
            try:
                family.process.wait(timeout=_JOIN_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                logger.error('Managed-job slot guardian is still draining; '
                             'retaining runtime ownership.')
        if not completion_seen:
            raise ControllerSlotProofError(
                f'Managed-job slot {family.identity[2]} exited without exact '
                'stable-empty proof.')

    def _set_family(self, slot_id: int, family: _SlotFamily | None) -> None:
        with self._lock:
            if family is None:
                self._families.pop(slot_id, None)
            else:
                self._families[slot_id] = family

    def _quiesce_nested_requests(
            self, identity: managed_job_state.ControllerSlotIdentity) -> int:
        """Close exact nested admission and prove every request family quiet."""
        # Import at the handoff boundary to avoid jobs.state -> requests ->
        # jobs.state initialization cycles.  The request facade is the one
        # canonical backend dispatcher for PostgreSQL and local SQLite.
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import requests as api_requests
        try:
            return api_requests.quiesce_managed_job_slot_requests(identity)
        except Exception as e:
            # Missing request-family proof is equivalent to missing local
            # family proof: retain the outer owner and fail closed rather than
            # publishing a replacement attempt that could overlap effects.
            raise ControllerSlotNestedRequestProofError(
                'Managed-job slot nested requests did not prove exact '
                'quiescence.') from e

    def _wait_nested_request_quiescence(
            self, identity: managed_job_state.ControllerSlotIdentity) -> int:
        """Retryably prove an admitted, locally drained slot's requests."""
        with self._lock:
            self._pending_nested_quiescence.add(identity)
        quiesced_requests = self._quiesce_nested_requests(identity)
        with self._lock:
            self._pending_nested_quiescence.discard(identity)
        return quiesced_requests

    def _wait_family_effect_quiescence(self, family: _SlotFamily) -> int:
        """Prove one local family and all work it admitted have stopped."""
        self._wait_family_completion(family)
        self._set_family(family.identity[2], None)
        if not family.admitted:
            return 0
        return self._wait_nested_request_quiescence(family.identity)

    def _run_slot(self, initial_family: _SlotFamily) -> None:
        family = initial_family
        slot_id = family.identity[2]
        consecutive_short_lived_families = 0
        restart_backoff_seconds = _RESTART_BACKOFF_INITIAL_SECONDS
        try:
            self._startup_complete.wait()
            if not self._startup_succeeded:
                return
            while True:
                quiesced_requests = self._wait_family_effect_quiescence(family)
                if self._stop.is_set():
                    return
                admitted_at = family.admitted_at
                if (admitted_at is None or time.monotonic() - admitted_at
                        < _SHORT_LIVED_FAMILY_SECONDS):
                    consecutive_short_lived_families += 1
                else:
                    consecutive_short_lived_families = 0
                    restart_backoff_seconds = _RESTART_BACKOFF_INITIAL_SECONDS
                if (consecutive_short_lived_families
                        >= _MAX_CONSECUTIVE_SHORT_LIVED_FAMILIES):
                    raise ControllerSlotError(
                        f'Managed-job controller slot {slot_id} exited before '
                        'readiness remained stable too many times.')
                reset = managed_job_state.reset_jobs_for_controller_slot(
                    family.identity)
                logger.warning(
                    'Managed-job controller slot %s attempt %s '
                    'exited; quiesced %s nested request(s) and reset %s owned '
                    'job(s).', slot_id, family.identity[3], quiesced_requests,
                    reset)
                if self._stop.is_set():
                    return
                if consecutive_short_lived_families > 0:
                    if self._stop.wait(restart_backoff_seconds):
                        return
                    restart_backoff_seconds = min(restart_backoff_seconds * 2,
                                                  _RESTART_BACKOFF_MAX_SECONDS)
                family = self._prepare_family(self._identity(slot_id))
                if not self._publish_and_admit(family):
                    self._terminate_and_drain_unadmitted(family)
                    return
                self._wait_for_started(family)
        except BaseException as e:  # pylint: disable=broad-except
            self.request_shutdown()
            # An admission/readiness failure can leave a published guardian
            # draining after this monitor would otherwise exit.  This slot
            # thread remains the authoritative reader of its completion
            # channel and must obtain exact stable-empty proof before asking
            # the runtime to release leadership.
            with self._lock:
                published_family = self._families.get(slot_id)
            if published_family is not None:
                try:
                    _write_message(published_family.control,
                                   {'type': 'terminate'})
                except OSError:
                    pass
                try:
                    self._wait_family_effect_quiescence(published_family)
                except ControllerSlotProofError as proof_error:
                    if not isinstance(proof_error,
                                      ControllerSlotNestedRequestProofError):
                        with self._lock:
                            if self._unsafe_failure is None:
                                self._unsafe_failure = proof_error
            callback = None
            with self._lock:
                if self._failure is None:
                    self._failure = e
                if (isinstance(e, ControllerSlotProofError) and
                        not isinstance(e, ControllerSlotNestedRequestProofError)
                        and self._unsafe_failure is None):
                    self._unsafe_failure = e
                if (self._on_failure is not None and
                        not self._failure_callback_called):
                    self._failure_callback_called = True
                    callback = self._on_failure
            if callback is not None:
                try:
                    callback()
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        'Managed-job controller slot failure callback failed.')
            logger.exception('Managed-job controller slot %s failed closed.',
                             slot_id)

    def start(self) -> None:
        """Transactionally admit every fixed slot before returning."""
        if self._started:
            raise RuntimeError('Managed-job controller slots already started.')
        prepared: list[_SlotFamily] = []
        try:
            for slot_id in range(self._slot_count):
                family = self._prepare_family(self._identity(slot_id))
                prepared.append(family)
                # Publication is intentionally separate from admission during
                # startup: every fixed family and monitor must exist before
                # the first manager receives effect authority.
                with self._lock:
                    self._families[slot_id] = family
                    if self._stop.is_set():
                        raise ControllerSlotError(
                            'Managed-job controller slot startup was stopped.')
            # Install every monitor before any ControllerManager can execute.
            # The monitors wait on `_startup_complete` while this thread owns
            # all admission acknowledgements, avoiding concurrent socket reads.
            for family in prepared:
                thread = threading.Thread(
                    target=self._run_slot,
                    args=(family,),
                    name=f'managed-job-controller-slot-{family.identity[2]}',
                    daemon=True)
                thread.start()
                self._threads.append(thread)
            # Open all gates before awaiting individual started messages.  A
            # partially admitted set is never exposed to request workers; any
            # error below drains the entire prepared set before propagating.
            for family in prepared:
                with self._lock:
                    if self._stop.is_set():
                        raise ControllerSlotError(
                            'Managed-job controller slot startup was stopped.')
                    self._send_admission(family)
            for family in prepared:
                self._wait_for_started(family)
            self._startup_succeeded = True
            self._started = True
        except BaseException:
            self._stop.set()
            for family in prepared:
                try:
                    _write_message(family.control, {'type': 'terminate'})
                except OSError:
                    pass
            for family in prepared:
                try:
                    self._wait_family_effect_quiescence(family)
                except ControllerSlotProofError as e:
                    if not isinstance(e, ControllerSlotNestedRequestProofError):
                        with self._lock:
                            if self._unsafe_failure is None:
                                self._unsafe_failure = e
            raise
        finally:
            self._startup_complete.set()

    def raise_if_failed(self) -> None:
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise ControllerSlotError(
                'A managed-job controller slot lost its exact family proof.'
            ) from failure

    def request_shutdown(self) -> None:
        """Stop replacement and ask every admitted family to drain."""
        with self._lock:
            self._stop.set()
            families = list(self._families.values())
        for family in families:
            try:
                _write_message(family.control, {'type': 'terminate'})
            except OSError:
                pass

    def wait_for_shutdown(self) -> None:
        """Join every slot only after its guardian proof has completed."""
        for thread in self._threads:
            thread.join(timeout=_JOIN_TIMEOUT_SECONDS)
        alive = [thread.name for thread in self._threads if thread.is_alive()]
        if alive:
            raise ControllerSlotError('Managed-job controller slots still '
                                      f'draining: {alive}.')
        # A request backend can recover after a bounded proof attempt.  Retry
        # each exact identity on every outer convergence pass instead of
        # caching a transient database/request-worker failure forever.
        with self._lock:
            pending_nested_quiescence = tuple(
                sorted(self._pending_nested_quiescence))
        for identity in pending_nested_quiescence:
            self._wait_nested_request_quiescence(identity)
        # A reset/restart failure after exact family proof should restart this
        # runtime, but it must not permanently block release once every other
        # family has also drained.  Only missing stable-empty proof is unsafe
        # for leadership handoff.
        with self._lock:
            unsafe_failure = self._unsafe_failure
        if unsafe_failure is not None:
            raise ControllerSlotProofError(
                'A managed-job slot family remains unproven.') from (
                    unsafe_failure)


def start_controller_slots(
    owner: tuple[str, int],
    origin_capability: str,
) -> ManagedJobControllerSlotSupervisor:
    """Create and transactionally start the generation's eager fixed slots."""
    supervisor = ManagedJobControllerSlotSupervisor(
        owner, origin_capability=origin_capability)
    supervisor.start()
    return supervisor


class LocalManagedJobControllerRuntime:
    """Own the canonical fixed-slot runtime under one local process birth.

    This is the outer lifecycle used by a separate, non-consolidated jobs
    controller.  Cross-host authority is unnecessary there: the Skylet is the
    sole local parent, and every child write proves the published parent PID
    and Linux process-start tick before it proves its exact slot attempt.

    The owner publication deliberately outlives any failed or timed-out drain.
    Callers may clear it only after :meth:`wait_for_shutdown` has accepted the
    supervisor's exact stable-empty proofs.
    """

    def __init__(
        self,
        *,
        on_failure: typing.Callable[[], None] | None = None,
        slot_count: int | None = None,
        capability_authority_callback: typing.Callable[[str], None] |
        None = None,
    ) -> None:
        self._on_failure = on_failure
        self._slot_count = slot_count
        self._capability_authority_callback = capability_authority_callback
        self._owner: tuple[str, int] | None = None
        self._supervisor: ManagedJobControllerSlotSupervisor | None = None
        self._owner_published = False
        self._shutdown_proven = False
        self._started = False
        self._capability_authority: (LocalControllerOriginCapabilityAuthority |
                                     None) = None

    @property
    def owner(self) -> tuple[str, int] | None:
        return self._owner

    @property
    def capability_authority_path(self) -> str | None:
        if self._capability_authority is None:
            return None
        return self._capability_authority.path

    @property
    def started(self) -> bool:
        return self._started and not self._shutdown_proven

    def _clear_owner_after_proven_shutdown(self) -> None:
        if self._capability_authority is not None:
            self._capability_authority.remove()
            self._capability_authority = None
        if self._owner_published:
            current_owner = controller_fencing.get_current_owner()
            if current_owner != self._owner:
                raise ControllerSlotError(
                    'Managed-job local runtime owner changed before shutdown.')
            controller_fencing.clear_owner()
        self._owner_published = False
        self._shutdown_proven = True
        self._started = False

    def start(self) -> None:
        """Recover stale ownership, then transactionally admit every slot."""
        if self._supervisor is not None or self._owner is not None:
            raise RuntimeError('Managed-job local runtime already started.')
        if controller_fencing.get_current_owner() is not None:
            raise ControllerSlotError(
                'Another managed-job runtime owner is already published.')

        owner = (str(uuid.uuid4()), _read_process_start_time_ticks(os.getpid()))
        self._owner = owner
        try:
            authority = LocalControllerOriginCapabilityAuthority(owner)
            self._capability_authority = authority
            authority.publish()
            if self._capability_authority_callback is not None:
                self._capability_authority_callback(authority.path)
            controller_fencing.publish_owner(
                owner, mode=controller_fencing.LOCAL_OWNER_MODE)
            self._owner_published = True
            supervisor = ManagedJobControllerSlotSupervisor(
                owner,
                slot_count=self._slot_count,
                on_failure=self._on_failure,
                origin_capability=authority.capability)
            self._supervisor = supervisor
            # A replacement Skylet never consults a foreign PID.  Its fresh
            # process-birth owner makes every earlier/null owner stale, and the
            # exact reset runs before any new manager receives admission.
            reset = managed_job_state.reset_stale_jobs_for_current_controller()
            if reset:
                logger.info(
                    'Reset %s managed job(s) from a stale remote '
                    'controller runtime.', reset)
            supervisor.start()
            self._started = True
        except BaseException:
            if self._supervisor is not None:
                self._supervisor.request_shutdown()
            # `start()` already attempts to drain partially admitted families,
            # but this second convergence call is intentionally authoritative
            # and keeps the owner published if proof is still unavailable.
            if self._supervisor is not None:
                self._supervisor.wait_for_shutdown()
            self._clear_owner_after_proven_shutdown()
            raise

    def raise_if_failed(self) -> None:
        if self._supervisor is not None:
            self._supervisor.raise_if_failed()

    def request_shutdown(self) -> None:
        if self._supervisor is not None:
            self._supervisor.request_shutdown()

    def wait_for_shutdown(self) -> None:
        """Accept every family proof before removing the local owner."""
        if self._shutdown_proven:
            return
        if self._supervisor is not None:
            self._supervisor.wait_for_shutdown()
        self._clear_owner_after_proven_shutdown()
