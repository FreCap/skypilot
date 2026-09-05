"""Opaque capability primitives for authenticated controller-origin work.

The raw capability is process-local authority.  Only its SHA-256 digest may be
written to durable storage.  Keeping generation, hashing, and local authority
verification here gives PostgreSQL and process-local controller runtimes one
wire contract without importing either runtime implementation.
"""

from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import json
import os
import stat
import sys
import uuid

_CAPABILITY_BYTES = 32
_CAPABILITY_ENCODED_LENGTH = 43
_CAPABILITY_TRANSPORT_MAX_BYTES = _CAPABILITY_ENCODED_LENGTH + 1
_AUTHORITY_FILE_MAX_BYTES = 4096
_LOCAL_AUTHORITY_DIRECTORY = (
    '.sky/locks/managed_job_controller_origin_authority')
_LOCAL_AUTHORITY_KEYS = frozenset({
    'controller_instance_id',
    'controller_generation',
    'origin_capability_sha256',
    'owner_pid',
    'owner_process_start_time_ticks',
})
_PR_SET_DUMPABLE = 4
_PR_GET_DUMPABLE = 3
# Linux keeps procfs birth identity for zombies (Z) and dead tasks (X/x).
# Those terminal tasks cannot exercise controller authority.
_TERMINAL_PROCESS_STATES = frozenset({'Z', 'X', 'x'})

# The raw value is deliberately bound to the exact process that consumed its
# one-shot transport.  A forked user hook therefore cannot turn the inherited
# Python object into controller authority, and exec discards it altogether.
_PROCESS_LOCAL_CAPABILITY: tuple[int, str] | None = None


def generate() -> str:
    """Return a canonical URL-safe capability with 256 bits of entropy."""
    return base64.urlsafe_b64encode(
        os.urandom(_CAPABILITY_BYTES)).rstrip(b'=').decode('ascii')


def digest(capability: str) -> bytes:
    """Validate and hash one raw controller-origin capability.

    Validation prevents alternate encodings from becoming parallel wire
    representations.  The raw value must never be persisted or logged.
    """
    if not isinstance(capability,
                      str) or len(capability) != _CAPABILITY_ENCODED_LENGTH:
        raise ValueError('Controller-origin capability is not canonical.')
    try:
        decoded = base64.b64decode(capability + '=',
                                   altchars=b'-_',
                                   validate=True)
    except (ValueError, TypeError) as e:
        raise ValueError(
            'Controller-origin capability is not canonical.') from e
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b'=').decode('ascii')
    if len(decoded) != _CAPABILITY_BYTES or not hmac.compare_digest(
            canonical, capability):
        raise ValueError('Controller-origin capability is not canonical.')
    return hashlib.sha256(capability.encode('ascii')).digest()


def digest_hex(capability: str) -> str:
    """Return the lowercase digest representation used by local authority."""
    return digest(capability).hex()


def install_process_local(capability: str) -> None:
    """Install controller authority in this exact process, never its env."""
    digest(capability)
    global _PROCESS_LOCAL_CAPABILITY
    current = _PROCESS_LOCAL_CAPABILITY
    if current is not None:
        current_pid, current_capability = current
        if (current_pid != os.getpid() or
                not hmac.compare_digest(current_capability, capability)):
            raise RuntimeError(
                'Another process-local controller capability is installed.')
        return
    _PROCESS_LOCAL_CAPABILITY = (os.getpid(), capability)


def get_process_local() -> str | None:
    """Return authority only to the exact process that installed it."""
    current = _PROCESS_LOCAL_CAPABILITY
    if current is None or current[0] != os.getpid():
        return None
    return current[1]


def clear_process_local() -> None:
    """Forget controller authority in the exact installing process."""
    global _PROCESS_LOCAL_CAPABILITY
    current = _PROCESS_LOCAL_CAPABILITY
    if current is not None and current[0] == os.getpid():
        _PROCESS_LOCAL_CAPABILITY = None


def install_process_local_from_fd(file_descriptor: int) -> None:
    """Consume one bounded capability pipe and close it on every outcome."""
    if (not isinstance(file_descriptor, int) or
            isinstance(file_descriptor, bool) or file_descriptor < 0):
        raise ValueError('Controller capability transport FD is invalid.')
    try:
        payload = bytearray()
        while len(payload) < _CAPABILITY_TRANSPORT_MAX_BYTES:
            chunk = os.read(file_descriptor,
                            _CAPABILITY_TRANSPORT_MAX_BYTES - len(payload))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) != _CAPABILITY_ENCODED_LENGTH:
            raise ValueError(
                'Controller capability transport payload is invalid.')
        capability = ''
        try:
            capability = bytes(payload).decode('ascii')
        except UnicodeDecodeError as e:
            raise ValueError(
                'Controller capability transport payload is invalid.') from e
        install_process_local(capability)
    finally:
        os.close(file_descriptor)


def install_process_local_from_fd_protected(file_descriptor: int) -> None:
    """Protect this process, then consume and install one bearer transport."""
    try:
        make_process_non_dumpable()
    except BaseException:
        try:
            os.close(file_descriptor)
        except OSError:
            pass
        raise
    install_process_local_from_fd(file_descriptor)


def make_process_non_dumpable() -> None:
    """Deny same-UID descendants access to this process's private memory."""
    if not sys.platform.startswith('linux'):
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    dumpable = libc.prctl(_PR_GET_DUMPABLE, 0, 0, 0, 0)
    if dumpable < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if dumpable != 0:
        raise OSError('Kernel did not disable controller process dumps.')


def local_authority_path(instance_id: str) -> str:
    """Return the one same-host authority path for a canonical owner UUID."""
    canonical_instance_id = str(uuid.UUID(instance_id))
    if canonical_instance_id != instance_id:
        raise ValueError('Controller instance identity is not canonical.')
    runtime_root = os.path.expanduser(os.environ.get('SKY_RUNTIME_DIR', '~'))
    return os.path.join(runtime_root, _LOCAL_AUTHORITY_DIRECTORY,
                        f'{canonical_instance_id}.json')


def read_live_process_start_time_ticks(pid: int) -> int:
    """Return one live Linux process birth identity, excluding terminal tasks."""
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError('Controller authority process PID is invalid.')
    with open(f'/proc/{pid}/stat', encoding='utf-8') as stream:
        content = stream.read()
    comm_end = content.rfind(')')
    if comm_end < 2 or not content.startswith(f'{pid} ('):
        raise ValueError('Malformed controller authority process identity.')
    fields_after_comm = content[comm_end + 1:].split()
    if len(fields_after_comm) <= 19:
        raise ValueError('Malformed controller authority process identity.')
    state = fields_after_comm[0]
    if len(state) != 1:
        raise ValueError('Malformed controller authority process state.')
    if state in _TERMINAL_PROCESS_STATES:
        raise ProcessLookupError(
            'Controller authority process is no longer live.')
    value = int(fields_after_comm[19])
    if value <= 0:
        raise ValueError('Invalid controller authority process identity.')
    return value


def _local_authority_is_current(
    path: str,
    instance_id: str,
    generation: int,
    expected_digest: bytes | None,
) -> bool:
    """Validate one private authority file and its exact owner process birth."""
    file_descriptor: int | None = None
    try:
        canonical_instance_id = str(uuid.UUID(instance_id))
        if (canonical_instance_id != instance_id or
                not isinstance(generation, int) or
                isinstance(generation, bool) or generation <= 0):
            return False
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, 'O_NOFOLLOW'):
            flags |= os.O_NOFOLLOW
        if hasattr(os, 'O_NONBLOCK'):
            flags |= os.O_NONBLOCK
        parent_stat = os.lstat(os.path.dirname(path))
        if (not stat.S_ISDIR(parent_stat.st_mode) or
                parent_stat.st_uid != os.geteuid() or
                stat.S_IMODE(parent_stat.st_mode) & 0o077):
            return False
        file_descriptor = os.open(path, flags)
        file_stat = os.fstat(file_descriptor)
        if (not stat.S_ISREG(file_stat.st_mode) or
                file_stat.st_uid != os.geteuid() or
                stat.S_IMODE(file_stat.st_mode) & 0o077):
            return False
        chunks = []
        remaining = _AUTHORITY_FILE_MAX_BYTES + 1
        while remaining > 0:
            chunk = os.read(file_descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b''.join(chunks)
        if len(payload) > _AUTHORITY_FILE_MAX_BYTES:
            return False
        authority = json.loads(payload.decode('utf-8'))
        if (not isinstance(authority, dict) or
                frozenset(authority) != _LOCAL_AUTHORITY_KEYS):
            return False
        authority_instance = authority['controller_instance_id']
        authority_generation = authority['controller_generation']
        authority_digest = authority['origin_capability_sha256']
        owner_pid = authority['owner_pid']
        owner_start_ticks = authority['owner_process_start_time_ticks']
        if (not isinstance(authority_instance, str) or
                not isinstance(authority_generation, int) or
                isinstance(authority_generation, bool) or
                not isinstance(authority_digest, str) or
                not isinstance(owner_pid, int) or isinstance(owner_pid, bool) or
                not isinstance(owner_start_ticks, int) or
                isinstance(owner_start_ticks, bool)):
            return False
        if (authority_instance != canonical_instance_id or
                authority_generation != generation or owner_pid <= 0 or
                owner_start_ticks <= 0):
            return False
        stored_digest = bytes.fromhex(authority_digest)
        if (len(stored_digest) != hashlib.sha256().digest_size or
                stored_digest.hex() != authority_digest):
            return False
        process_stat = os.stat(f'/proc/{owner_pid}')
        if process_stat.st_uid != os.geteuid():
            return False
        if read_live_process_start_time_ticks(owner_pid) != owner_start_ticks:
            return False
        return (expected_digest is None or
                hmac.compare_digest(stored_digest, expected_digest))
    except (AttributeError, FileNotFoundError, json.JSONDecodeError, OSError,
            TypeError, ValueError):
        return False
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)


def verify_local_authority(path: str, instance_id: str, generation: int,
                           capability: str) -> bool:
    """Verify a same-host controller capability against a hash-only file.

    This is deliberately narrower than PostgreSQL authority.  The file must be
    a regular, owner-only file owned by the API process uid, and the recorded
    process-birth identity must still be live.  A crash-stale file therefore
    cannot keep granting controller authority.
    """
    try:
        expected_digest = digest(capability)
    except (TypeError, ValueError):
        return False
    return _local_authority_is_current(path, instance_id, generation,
                                       expected_digest)


def local_authority_owner_is_current(instance_id: str, generation: int) -> bool:
    """Prove liveness for an already-authenticated persisted local origin.

    The raw capability is required at the HTTP boundary and is never persisted
    on requests.  Subsequent queue/RUNNING admission instead trusts only the
    canonical owner-only authority file and its exact same-UID PID/start-ticks
    identity.  Missing, stale, malformed, permissive, or symlinked authority
    fails closed.
    """
    try:
        path = local_authority_path(instance_id)
    except (AttributeError, TypeError, ValueError):
        return False
    return _local_authority_is_current(path, instance_id, generation, None)
