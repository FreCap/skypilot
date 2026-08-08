"""Fail-closed helpers for one SkyServe Ray node-OOM replay.

This module is deliberately not used by ordinary SkyPilot jobs.  The generated
Ray driver opts one eligible run task into this path and passes a unique attempt
context to :mod:`sky.skylet.subprocess_supervisor`.
"""

from collections.abc import Callable
import contextlib
import dataclasses
import enum
import functools
import inspect
import json
import math
import os
import pathlib
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import typing
from typing import Any
import uuid

from sky import sky_logging
from sky.skylet import constants
from sky.skylet import job_lib

if typing.TYPE_CHECKING:
    import psutil
else:
    from sky.adaptors import common as adaptors_common
    psutil = adaptors_common.LazyImport('psutil')

logger = sky_logging.init_logger(__name__)

PROFILE_VERSION_OWNED_CONTAINER = 2
CAPABILITY_V2 = 'subreaper-v2+owned-local-docker-v1'
CAPABILITY_BY_PROFILE_VERSION = {
    PROFILE_VERSION_OWNED_CONTAINER: CAPABILITY_V2,
}
MARKER_SCHEMA_BY_PROFILE_VERSION = {
    PROFILE_VERSION_OWNED_CONTAINER: 2,
}
RECOVERY_ROOT = '~/.sky/system_oom_recovery'
MARKER_RETENTION_SECONDS = 24 * 60 * 60
BOOT_ID_PATH = '/proc/sys/kernel/random/boot_id'
DOCKER_HOST = 'unix:///var/run/docker.sock'
DOCKER_PID_PATH = '/var/run/docker.pid'
DOCKER_COMMAND_TIMEOUT_SECONDS = 5
DOCKER_EMPTY_SAMPLES = 3
DOCKER_EMPTY_SAMPLE_INTERVAL_SECONDS = 1
RECOVERY_TIMEOUT_SECONDS = 120
FIRST_EVENT_VISIBILITY_SECONDS = 35
SYSTEM_RECOVERY_ARM_WINDOW_SECONDS = 35
SYSTEM_RECOVERY_REPLAY_QUIESCENCE_SECONDS = 83
MAX_RECOVERY_HOST_MEMORY_GIB = 16
MAX_RECOVERY_HOST_MEMORY_BYTES = MAX_RECOVERY_HOST_MEMORY_GIB * 1024**3
MEMORY_SAFE_SAMPLES = 3
MEMORY_SAFE_SAMPLE_INTERVAL_SECONDS = 1
DEFAULT_RAY_MEMORY_USAGE_THRESHOLD = 0.95
MAX_REPLAY_MEMORY_USAGE_FRACTION = 0.90
RAY_MEMORY_HYSTERESIS = 0.05

_DRIVER_ADMISSION_STAGES = frozenset({'arm', 'replay_memory', 'replay_runtime'})
_DRIVER_ADMISSION_DECISIONS = frozenset({'accepted', 'rejected'})
_DRIVER_ADMISSION_REASONS = frozenset({
    'accepted',
    'arm_window_expired',
    'boot_changed',
    'capability_unavailable_at_oom',
    'cgroup_above_16_gib',
    'cgroup_invalid',
    'cgroup_unavailable',
    'marker_malformed',
    'marker_rejected',
    'memory_safe',
    'memory_watermark_timeout',
    'placement_group_not_created',
    'placement_group_unavailable',
    'ray_session_changed',
    'ray_session_unavailable',
    'recovery_state_mismatch',
    'recovery_state_unavailable',
    'task_completed_before_arm',
    'task_failed_before_arm',
})
_DRIVER_ADMISSION_CGROUP_STATES = frozenset({
    'at_most_16_gib', 'above_16_gib', 'invalid', 'not_checked', 'unavailable',
    'unknown'
})
_DRIVER_ADMISSION_RAY_SESSION_STATES = frozenset(
    {'captured', 'changed', 'not_checked', 'unavailable', 'unchanged'})


def _log_driver_admission(*, stage: str, decision: str, reason: str,
                          cgroup_state: str, ray_session_state: str,
                          profile_version: int | None) -> None:
    """Emit one closed, non-identifying driver admission decision."""
    if stage not in _DRIVER_ADMISSION_STAGES:
        stage = 'arm'
    if decision not in _DRIVER_ADMISSION_DECISIONS:
        decision = 'rejected'
    if reason not in _DRIVER_ADMISSION_REASONS:
        reason = 'recovery_state_unavailable'
    if cgroup_state not in _DRIVER_ADMISSION_CGROUP_STATES:
        cgroup_state = 'unknown'
    if ray_session_state not in _DRIVER_ADMISSION_RAY_SESSION_STATES:
        ray_session_state = 'unavailable'
    if (type(profile_version) is not int or  # pylint: disable=unidiomatic-typecheck
            profile_version not in CAPABILITY_BY_PROFILE_VERSION):
        profile_version = 0
    # Deliberately omit service/job/session/account/instance identities.  Every
    # value below comes from a closed, bounded vocabulary.
    logger.info('system_oom_recovery_driver_admission '
                f'stage={stage} decision={decision} reason={reason} '
                f'profile_version={profile_version} '
                f'cgroup={cgroup_state} ray_session={ray_session_state}')


_SHA256_IMAGE_PATTERN = re.compile(r'^[^\s@]+@sha256:[0-9a-f]{64}$')
_ENVIRONMENT_NAME_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_CONTAINER_ID_PATTERN = re.compile(r'^[0-9a-f]{64}$')
_RESERVED_CONTAINER_OPTIONS = frozenset({
    '--attach', '--cidfile', '--detach', '--interactive', '--label', '--name',
    '--remove', '--restart', '--rm', '--sig-proxy', '--tty', '-a', '-d', '-i',
    '-l', '-t'
})
_ALLOWED_BOOLEAN_CREATE_OPTIONS = frozenset(
    {'--init', '--privileged', '--publish-all', '--read-only'})
_ALLOWED_VALUE_CREATE_OPTIONS = frozenset({
    '--add-host', '--cap-add', '--cap-drop', '--cgroup-parent', '--cgroupns',
    '--cpus', '--cpuset-cpus', '--device', '--dns', '--dns-option',
    '--dns-search', '--entrypoint', '--expose', '--gpus', '--group-add',
    '--health-cmd', '--health-interval', '--health-retries',
    '--health-start-period', '--health-timeout', '--hostname', '--ipc',
    '--log-driver', '--log-opt', '--memory', '--memory-reservation',
    '--memory-swap', '--mount', '--network', '--oom-score-adj', '--pid',
    '--pids-limit', '--platform', '--publish', '--runtime', '--security-opt',
    '--shm-size', '--sysctl', '--tmpfs', '--ulimit', '--user', '--userns',
    '--uts', '--volume', '--workdir', '-p', '-u', '-v', '-w'
})


def _strict_string_tuple(name: str, value: object) -> tuple[str, ...]:
    if (not isinstance(value, (list, tuple)) or
            not all(isinstance(item, str) and item for item in value)):
        raise ValueError(f'{name} must contain nonempty strings')
    return tuple(value)


@dataclasses.dataclass(frozen=True)
class OwnedContainerSpec:
    """Canonical, supervisor-owned Docker workload description."""

    image: str
    create_options: tuple[str, ...] = ()
    argv: tuple[str, ...] = ()
    inherited_environment_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if _SHA256_IMAGE_PATTERN.fullmatch(self.image) is None:
            raise ValueError('image must be an immutable OCI digest reference')
        options = _strict_string_tuple('create_options', self.create_options)
        argv = _strict_string_tuple('argv', self.argv)
        inherited = _strict_string_tuple('inherited_environment_names',
                                         self.inherited_environment_names)
        if len(set(inherited)) != len(inherited):
            raise ValueError('inherited environment names must be unique')
        for name in inherited:
            if (_ENVIRONMENT_NAME_PATTERN.fullmatch(name) is None or
                    name in ('DOCKER_HOST', 'DOCKER_CONTEXT')):
                raise ValueError(f'invalid inherited environment name: {name}')
        self._validate_create_options(options)
        object.__setattr__(self, 'create_options', options)
        object.__setattr__(self, 'argv', argv)
        object.__setattr__(self, 'inherited_environment_names', inherited)

    @staticmethod
    def _validate_create_options(options: tuple[str, ...]) -> None:
        index = 0
        while index < len(options):
            option = options[index]
            if option in _RESERVED_CONTAINER_OPTIONS:
                raise ValueError(
                    f'container option is supervisor-owned: {option}')
            if option in _ALLOWED_BOOLEAN_CREATE_OPTIONS:
                index += 1
                continue
            name, separator, inline_value = option.partition('=')
            if name not in _ALLOWED_VALUE_CREATE_OPTIONS:
                raise ValueError(f'unsupported container create option: {name}')
            if separator:
                if not inline_value:
                    raise ValueError(f'container option has no value: {name}')
                value = inline_value
                index += 1
            else:
                if index + 1 >= len(options):
                    raise ValueError(f'container option has no value: {name}')
                value = options[index + 1]
                if value.startswith('-'):
                    raise ValueError(
                        f'container option has invalid value: {name}')
                index += 2
            if name in ('--volume', '--mount', '-v') and 'docker.sock' in value:
                raise ValueError('mounting a Docker socket is not supported')

    def docker_run_argv(self) -> tuple[str, ...]:
        environment = tuple(item for name in self.inherited_environment_names
                            for item in ('--env', name))
        return ('docker', 'run', *self.create_options, *environment, self.image,
                *self.argv)

    def render(self) -> str:
        """Render the only shell form accepted by the profile matcher."""
        return shlex.join(self.docker_run_argv())

    @classmethod
    def parse(cls, command: str) -> 'OwnedContainerSpec':
        """Parse only this type's canonical renderer, rejecting inference."""
        if not isinstance(command, str) or not command:
            raise ValueError('owned container command must be nonempty')
        try:
            tokens = shlex.split(command, posix=True)
        except ValueError as e:
            raise ValueError(f'invalid owned container command: {e}') from e
        if len(tokens) < 3 or tokens[:2] != ['docker', 'run']:
            raise ValueError('owned container command must be docker run')
        index = 2
        options: list[str] = []
        inherited: list[str] = []
        while index < len(tokens) and tokens[index].startswith('-'):
            option = tokens[index]
            name = option.partition('=')[0]
            if name == '--env':
                if '=' in option or index + 1 >= len(tokens):
                    raise ValueError('environment option is not canonical')
                inherited.append(tokens[index + 1])
                index += 2
                continue
            options.append(option)
            if (name not in _ALLOWED_BOOLEAN_CREATE_OPTIONS and
                    '=' not in option):
                if index + 1 >= len(tokens):
                    raise ValueError('container option is missing its value')
                options.append(tokens[index + 1])
                index += 2
            else:
                index += 1
        if index >= len(tokens):
            raise ValueError('owned container command has no image')
        result = cls(image=tokens[index],
                     create_options=tuple(options),
                     argv=tuple(tokens[index + 1:]),
                     inherited_environment_names=tuple(inherited))
        if result.render() != command:
            raise ValueError('owned container command is not canonical')
        return result

    def docker_create_argv(self, name: str,
                           labels: dict[str, str]) -> tuple[str, ...]:
        label_options = tuple(item for key, value in sorted(labels.items())
                              for item in ('--label', f'{key}={value}'))
        environment = tuple(
            item for env_name in self.inherited_environment_names
            for item in ('--env', env_name))
        return ('create', '--name', name, *label_options, *self.create_options,
                *environment, self.image, *self.argv)

    def to_dict(self) -> dict[str, object]:
        return {
            'image': self.image,
            'create_options': list(self.create_options),
            'argv': list(self.argv),
            'inherited_environment_names': list(self.inherited_environment_names
                                               ),
        }

    @classmethod
    def from_dict(cls, value: object) -> 'OwnedContainerSpec':
        if not isinstance(value, dict) or set(value) != {
                'image', 'create_options', 'argv', 'inherited_environment_names'
        }:
            raise ValueError('owned container spec has invalid fields')
        return cls(image=value['image'],
                   create_options=_strict_string_tuple('create_options',
                                                       value['create_options']),
                   argv=_strict_string_tuple('argv', value['argv']),
                   inherited_environment_names=_strict_string_tuple(
                       'inherited_environment_names',
                       value['inherited_environment_names']))


def build_rclone_flush_script() -> str:
    """Return the canonical cached-mount postlude used by task codegen."""
    # Kept here so the typed v2 envelope and ordinary task codegen share one
    # byte-identical source of truth.
    return textwrap.dedent(f"""\

        __skypilot_user_exit_code=$?
        # Only waits if cached mount is enabled (RCLONE_MOUNT_CACHED_LOG_DIR is not empty)
        # findmnt alone is not enough, as some clouds (e.g. AWS on ARM64) uses
        # rclone for normal mounts as well.
        if [ $(findmnt -t fuse.rclone --noheading | wc -l) -gt 0 ] && \
           [ -d {constants.RCLONE_MOUNT_CACHED_LOG_DIR} ] && \
           [ "$(ls -A {constants.RCLONE_MOUNT_CACHED_LOG_DIR})" ]; then
            FLUSH_START_TIME=$(date +%s)
            flushed=0
            # extra second on top of --vfs-cache-poll-interval to
            # avoid race condition between rclone log line creation and this check.
            sleep 1
            while [ $flushed -eq 0 ]; do
                # sleep for the same interval as --vfs-cache-poll-interval
                sleep {constants.RCLONE_CACHE_REFRESH_INTERVAL}
                flushed=1
                for file in {constants.RCLONE_MOUNT_CACHED_LOG_DIR}/*; do
                    exitcode=0
                    tac $file | grep "vfs cache: cleaned:" -m 1 | grep "in use 0, to upload 0, uploading 0" -q || exitcode=$?
                    if [ $exitcode -ne 0 ]; then
                        ELAPSED=$(($(date +%s) - FLUSH_START_TIME))
                        # Extract the last vfs cache status line to show what we're waiting for
                        CACHE_STATUS=$(tac $file | grep "vfs cache: cleaned:" -m 1 | sed 's/.*vfs cache: cleaned: //' 2>/dev/null)
                        # Extract currently uploading files from recent log lines (show up to 2 files)
                        UPLOADING_FILES=$(tac $file | head -30 | grep -E "queuing for upload" | head -2 | sed 's/.*INFO  : //' | sed 's/: vfs cache:.*//' | tr '\\n' ',' | sed 's/,$//' | sed 's/,/, /g' 2>/dev/null)
                        # Build status message with available info
                        if [ -n "$CACHE_STATUS" ] && [ -n "$UPLOADING_FILES" ]; then
                            echo "skypilot: cached mount is still uploading (elapsed: ${{ELAPSED}}s) [${{CACHE_STATUS}}] uploading: ${{UPLOADING_FILES}}"
                        elif [ -n "$CACHE_STATUS" ]; then
                            echo "skypilot: cached mount is still uploading (elapsed: ${{ELAPSED}}s) [${{CACHE_STATUS}}]"
                        else
                            # Fallback: show last non-empty line from log
                            LAST_LINE=$(tac $file | grep -v "^$" | head -1 | sed 's/.*INFO  : //' | sed 's/.*ERROR : //' | sed 's/.*NOTICE: //' 2>/dev/null)
                            if [ -n "$LAST_LINE" ]; then
                                echo "skypilot: cached mount is still uploading (elapsed: ${{ELAPSED}}s) ${{LAST_LINE}}"
                            else
                                echo "skypilot: cached mount is still uploading (elapsed: ${{ELAPSED}}s)"
                            fi
                        fi
                        flushed=0
                        break
                    fi
                done
            done
            TOTAL_FLUSH_TIME=$(($(date +%s) - FLUSH_START_TIME))
            echo "skypilot: cached mount upload complete (took ${{TOTAL_FLUSH_TIME}}s)"
        fi
        exit $__skypilot_user_exit_code""")


@dataclasses.dataclass(frozen=True)
class RecoveryExecutionEnvelope:
    """Code-owned host execution semantics surrounding an owned container."""

    working_directory: str
    unset_environment_names: tuple[str, ...]
    postlude_script: str
    environment: tuple[tuple[str, str], ...] = ()
    schema_version: int = 1

    def __post_init__(self) -> None:
        if (type(self.schema_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.schema_version != 1):
            raise ValueError('unsupported execution envelope schema')
        if not isinstance(self.working_directory,
                          str) or not self.working_directory:
            raise ValueError('working_directory must be nonempty')
        unset_names = _strict_string_tuple('unset_environment_names',
                                           self.unset_environment_names)
        if any(
                _ENVIRONMENT_NAME_PATTERN.fullmatch(name) is None
                for name in unset_names):
            raise ValueError('invalid unset environment name')
        if not isinstance(self.postlude_script,
                          str) or not self.postlude_script:
            raise ValueError('postlude_script must be nonempty')
        environment: list[tuple[str, str]] = []
        for item in self.environment:
            if (not isinstance(item, (list, tuple)) or len(item) != 2 or
                    not isinstance(item[0], str) or
                    _ENVIRONMENT_NAME_PATTERN.fullmatch(item[0]) is None or
                    not isinstance(item[1], str)):
                raise ValueError('execution environment is invalid')
            environment.append((item[0], item[1]))
        if len({name for name, _ in environment}) != len(environment):
            raise ValueError('execution environment names must be unique')
        object.__setattr__(self, 'unset_environment_names', unset_names)
        object.__setattr__(self, 'environment', tuple(environment))

    @classmethod
    def standard(cls) -> 'RecoveryExecutionEnvelope':
        return cls(working_directory=constants.SKY_REMOTE_WORKDIR,
                   unset_environment_names=('RAY_RAYLET_PID',),
                   postlude_script=build_rclone_flush_script())

    def bind_environment(
            self,
            environment: dict[str, str] | None) -> 'RecoveryExecutionEnvelope':
        if self.environment:
            raise ValueError('execution envelope environment is already bound')
        normalized = tuple(
            sorted((str(name), str(value))
                   for name, value in (environment or {}).items()))
        return dataclasses.replace(self, environment=normalized)

    def render_prelude(self,
                       environment_aliases: tuple[str, ...] | None = None
                      ) -> str:
        """Render the ordinary prelude without placing secrets in argv.

        A bound envelope must be paired with same-length, code-owned
        environment aliases.  The values themselves remain in the child
        environment; exports occur after bashrc/conda setup just as they do in
        the ordinary generated task script.
        """
        if self.environment:
            if (environment_aliases is None or
                    len(environment_aliases) != len(self.environment) or any(
                        _ENVIRONMENT_NAME_PATTERN.fullmatch(alias) is None
                        for alias in environment_aliases)):
                raise ValueError(
                    'bound execution environment requires private aliases')
            exports = '\n'.join(
                f'export {name}="${{{alias}}}"\nunset {alias}'
                for (name,
                     _), alias in zip(self.environment, environment_aliases))
        else:
            if environment_aliases:
                raise ValueError('unbound execution environment has aliases')
            exports = ''
        return self._render_prelude(exports)

    def render_private_file_prelude(self) -> str:
        """Render the prelude for a private 0600 script, including values."""
        exports = '\n'.join(f'export {name}={shlex.quote(value)}'
                            for name, value in self.environment)
        return self._render_prelude(exports)

    def _render_prelude(self, exports: str) -> str:
        unsets = '\n'.join(
            f'unset {name}' for name in self.unset_environment_names)
        if self.working_directory == '~':
            working_directory = '"$HOME"'
        elif self.working_directory.startswith('~/'):
            working_directory = ('"$HOME"/' +
                                 shlex.quote(self.working_directory[2:]))
        else:
            working_directory = shlex.quote(self.working_directory)
        return f'''source ~/.bashrc
set -a
. $(conda info --base 2> /dev/null)/etc/profile.d/conda.sh > /dev/null 2>&1 || true
set +a
{constants.DEACTIVATE_SKY_REMOTE_PYTHON_ENV}
export PYTHONUNBUFFERED=1
cd {working_directory}
{exports}
{unsets}'''

    def to_dict(self) -> dict[str, object]:
        return {
            'schema_version': self.schema_version,
            'working_directory': self.working_directory,
            'unset_environment_names': list(self.unset_environment_names),
            'postlude_script': self.postlude_script,
            'environment': [list(item) for item in self.environment],
        }

    @classmethod
    def from_dict(cls, value: object) -> 'RecoveryExecutionEnvelope':
        if not isinstance(value, dict) or set(value) != {
                'schema_version', 'working_directory',
                'unset_environment_names', 'postlude_script', 'environment'
        }:
            raise ValueError('execution envelope has invalid fields')
        return cls(schema_version=value['schema_version'],
                   working_directory=value['working_directory'],
                   unset_environment_names=_strict_string_tuple(
                       'unset_environment_names',
                       value['unset_environment_names']),
                   postlude_script=value['postlude_script'],
                   environment=tuple(
                       tuple(item) for item in value['environment']))


@dataclasses.dataclass(frozen=True)
class RecoveryLaunchPlan:
    """Exact runtime authority emitted by the server-side profile matcher."""

    profile_version: int
    owned_container_spec: OwnedContainerSpec | None = None
    execution_envelope: RecoveryExecutionEnvelope | None = None

    def __post_init__(self) -> None:
        if type(self.profile_version) is not int:  # pylint: disable=unidiomatic-typecheck
            raise ValueError('profile_version must be an integer')
        if self.profile_version != PROFILE_VERSION_OWNED_CONTAINER:
            raise ValueError('unsupported recovery profile version')
        if (not isinstance(self.owned_container_spec, OwnedContainerSpec) or
                not isinstance(self.execution_envelope,
                               RecoveryExecutionEnvelope)):
            raise ValueError(
                'recovery plan requires an owned spec and envelope')
        unbound_envelope = dataclasses.replace(self.execution_envelope,
                                               environment=())
        if unbound_envelope != RecoveryExecutionEnvelope.standard():
            raise ValueError('recovery plan must use the exact code-owned '
                             'execution envelope')

    @property
    def capability(self) -> str:
        return CAPABILITY_BY_PROFILE_VERSION[self.profile_version]

    @classmethod
    def owned_container(cls, spec: OwnedContainerSpec) -> 'RecoveryLaunchPlan':
        return cls(profile_version=PROFILE_VERSION_OWNED_CONTAINER,
                   owned_container_spec=spec,
                   execution_envelope=RecoveryExecutionEnvelope.standard())

    def bind_environment(
            self, environment: dict[str, str] | None) -> 'RecoveryLaunchPlan':
        assert self.execution_envelope is not None
        return dataclasses.replace(
            self,
            execution_envelope=self.execution_envelope.bind_environment(
                environment))

    def to_dict(self) -> dict[str, object]:
        return {
            'profile_version': self.profile_version,
            'owned_container_spec': (None if self.owned_container_spec is None
                                     else self.owned_container_spec.to_dict()),
            'execution_envelope': (None if self.execution_envelope is None else
                                   self.execution_envelope.to_dict()),
        }

    @classmethod
    def from_dict(cls,
                  value: object,
                  *,
                  allow_bound: bool = True) -> 'RecoveryLaunchPlan':
        if not isinstance(value, dict) or set(value) != {
                'profile_version', 'owned_container_spec', 'execution_envelope'
        }:
            raise ValueError('recovery launch plan has invalid fields')
        raw_spec = value['owned_container_spec']
        raw_envelope = value['execution_envelope']
        spec = None if raw_spec is None else OwnedContainerSpec.from_dict(
            raw_spec)
        envelope = (None if raw_envelope is None else
                    RecoveryExecutionEnvelope.from_dict(raw_envelope))
        if (envelope is not None and envelope.environment and not allow_bound):
            raise ValueError('bound execution envelope is not accepted here')
        return cls(profile_version=value['profile_version'],
                   owned_container_spec=spec,
                   execution_envelope=envelope)


class RecoveryError(RuntimeError):
    """A recovery precondition could not be proven."""


def _is_strict_int(value: object) -> typing.TypeGuard[int]:
    """Return whether value is an integer but not a boolean."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: object) -> typing.TypeGuard[int | float]:
    """Return whether value is a finite, non-boolean number."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


@dataclasses.dataclass(frozen=True)
class DockerIdentity:
    """Identity of the one local Docker engine accepted by this feature."""

    host: str
    daemon_id: str
    daemon_pid: int
    daemon_pid_create_time: float

    def to_dict(self) -> dict[str, object]:
        """Serialize this identity for an attempt-scoped marker."""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> 'DockerIdentity':
        """Validate and deserialize a marker Docker identity."""
        if not isinstance(value, dict):
            raise RecoveryError('Docker identity is not an object.')
        try:
            host = value['host']
            daemon_id = value['daemon_id']
            daemon_pid = value['daemon_pid']
            create_time = value['daemon_pid_create_time']
        except KeyError as e:
            raise RecoveryError(
                f'Docker identity is missing {e.args[0]}.') from e
        if host != DOCKER_HOST or not isinstance(daemon_id,
                                                 str) or not daemon_id:
            raise RecoveryError('Docker identity contains invalid values.')
        if not _is_strict_int(daemon_pid) or daemon_pid <= 1:
            raise RecoveryError('Docker identity contains invalid values.')
        if not _is_finite_number(create_time):
            raise RecoveryError('Docker identity contains invalid values.')
        return cls(host=host,
                   daemon_id=daemon_id,
                   daemon_pid=daemon_pid,
                   daemon_pid_create_time=float(create_time))


def read_boot_id() -> str:
    """Return the current Linux boot ID, or an empty string if unavailable."""
    try:
        with open(BOOT_ID_PATH, encoding='utf-8') as file:
            boot_id = file.read().strip()
    except OSError:
        return ''
    return boot_id


def capture_ray_session_identity(ray_module: Any) -> str | None:
    """Capture Ray's exact process-local session name, or fail closed."""
    try:
        if ray_module.is_initialized() is not True:
            return None
        worker_module = ray_module._private.worker  # pylint: disable=protected-access
        global_node = worker_module._global_node  # pylint: disable=protected-access
        session_name = global_node.session_name
    except Exception:  # pylint: disable=broad-except
        return None
    if not isinstance(session_name, str) or not session_name:
        return None
    return session_name


def _attempt_fields(context: dict[str, Any]) -> dict[str, object]:
    """Return the fields binding a marker to exactly one attempt."""
    return {
        'attempt_id': context['attempt_id'],
        'job_id': context['job_id'],
        'task_index': context['task_index'],
        'attempt_number': context['attempt_number'],
        'node_boot_id': context['node_boot_id'],
        'profile_version': context['profile_version'],
        'capability': context['capability'],
    }


def _validate_attempt_context(context: object) -> dict[str, Any]:
    """Validate an attempt context and its private marker paths."""
    if not isinstance(context, dict):
        raise RecoveryError('Attempt context is not an object.')
    required = {
        'schema_version', 'attempt_id', 'job_id', 'task_index',
        'attempt_number', 'node_boot_id', 'created_at', 'marker_dir',
        'capability_path', 'cleanup_path', 'profile_version', 'capability',
        'require_armed_start', 'expected_docker_identity', 'expected_parent_pid'
    }
    missing = required - set(context)
    if missing:
        raise RecoveryError(f'Attempt context is missing {sorted(missing)!r}.')
    profile_version = context['profile_version']
    if (type(profile_version) is not int or  # pylint: disable=unidiomatic-typecheck
            profile_version != PROFILE_VERSION_OWNED_CONTAINER):
        raise RecoveryError('Attempt profile version is unsupported.')
    if (type(context['schema_version']) is not int or  # pylint: disable=unidiomatic-typecheck
            context['schema_version']
            != MARKER_SCHEMA_BY_PROFILE_VERSION[profile_version]):
        raise RecoveryError('Attempt context schema is unsupported.')
    if context['capability'] != CAPABILITY_BY_PROFILE_VERSION[profile_version]:
        raise RecoveryError('Attempt capability does not match its profile.')
    try:
        parsed_uuid = uuid.UUID(str(context['attempt_id']))
    except (ValueError, AttributeError) as e:
        raise RecoveryError('Attempt ID is not a UUID.') from e
    if str(parsed_uuid) != context['attempt_id']:
        raise RecoveryError('Attempt ID is not in canonical UUID form.')
    for key in ('job_id', 'task_index', 'attempt_number'):
        value = context[key]
        if not _is_strict_int(value) or value < 0:
            raise RecoveryError(f'{key} must be a nonnegative integer.')
    if (not isinstance(context['node_boot_id'], str) or
            not context['node_boot_id']):
        raise RecoveryError('Node boot ID is invalid.')
    if context['require_armed_start'] is not True:
        raise RecoveryError('Attempt must require an armed start.')
    expected_docker = context['expected_docker_identity']
    if expected_docker is not None:
        DockerIdentity.from_dict(expected_docker)
    if context['attempt_number'] > 0 and expected_docker is None:
        raise RecoveryError(
            'Replacement attempt lacks mandatory Docker continuity.')
    expected_parent_pid = context['expected_parent_pid']
    if (expected_parent_pid is not None and
        (not _is_strict_int(expected_parent_pid) or expected_parent_pid <= 1)):
        raise RecoveryError('Expected Ray worker PID is invalid.')
    created_at = context['created_at']
    if not _is_finite_number(created_at):
        raise RecoveryError('Attempt creation time is invalid.')

    for path_key in ('marker_dir', 'capability_path', 'cleanup_path'):
        if not isinstance(context[path_key], str) or not context[path_key]:
            raise RecoveryError(f'Attempt {path_key} is invalid.')

    marker_dir = pathlib.Path(context['marker_dir']).resolve()
    capability_path = pathlib.Path(context['capability_path']).resolve()
    cleanup_path = pathlib.Path(context['cleanup_path']).resolve()
    expected_root = pathlib.Path(os.path.expanduser(RECOVERY_ROOT)).resolve()
    try:
        marker_dir.relative_to(expected_root)
    except ValueError as e:
        raise RecoveryError('Attempt marker directory escapes its root.') from e
    if (marker_dir.name != context['attempt_id'] or
            capability_path != marker_dir / 'capability.json' or
            cleanup_path != marker_dir / 'cleanup.json'):
        raise RecoveryError('Attempt marker paths do not match the attempt ID.')
    return context


def _safe_recovery_root() -> pathlib.Path:
    root = pathlib.Path(os.path.expanduser(RECOVERY_ROOT)).resolve()
    if root == pathlib.Path(root.anchor):
        raise RecoveryError('Recovery root is unsafe.')
    return root


def prune_attempt_directories(
        *,
        now: float | None = None,
        retention_seconds: float = MARKER_RETENTION_SECONDS) -> None:
    """Safely remove attempt directories older than the diagnostic window."""
    if now is None:
        now = time.time()
    if not _is_finite_number(now) or not _is_finite_number(
            retention_seconds) or retention_seconds < 0:
        raise ValueError('invalid recovery marker retention parameters')
    root = _safe_recovery_root()
    if not root.exists():
        return
    for job_directory in tuple(root.iterdir()):
        if job_directory.is_symlink() or not job_directory.is_dir():
            continue
        try:
            resolved_job = job_directory.resolve(strict=True)
            resolved_job.relative_to(root)
        except (OSError, ValueError):
            continue
        for attempt_directory in tuple(job_directory.iterdir()):
            if (attempt_directory.is_symlink() or
                    not attempt_directory.is_dir()):
                continue
            try:
                uuid.UUID(attempt_directory.name)
                resolved_attempt = attempt_directory.resolve(strict=True)
                if resolved_attempt.parent != resolved_job:
                    continue
                cleanup_path = resolved_attempt / 'cleanup.json'
                with cleanup_path.open(encoding='utf-8') as cleanup_file:
                    cleanup = json.load(cleanup_file)
                if (not isinstance(cleanup, dict) or
                        cleanup.get('kind') != 'cleanup' or
                        cleanup.get('attempt_id') != resolved_attempt.name or
                        not _is_finite_number(cleanup.get('completed_at'))):
                    continue
                age = float(now) - float(cleanup['completed_at'])
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if age < retention_seconds:
                continue
            # Resolve and bind again immediately before deletion so a path swap
            # cannot widen this cleanup beyond one direct UUID child.
            try:
                delete_target = attempt_directory.resolve(strict=True)
            except OSError:
                continue
            if (attempt_directory.is_symlink() or
                    delete_target != resolved_attempt or
                    delete_target.parent != resolved_job):
                continue
            shutil.rmtree(resolved_attempt)
        with contextlib.suppress(OSError):
            job_directory.rmdir()


def new_attempt_context(
    job_id: int,
    task_index: int,
    attempt_number: int,
    recovery_plan: RecoveryLaunchPlan,
    *,
    expected_boot_id: str | None = None,
    expected_docker_identity: DockerIdentity | None = None,
) -> dict[str, Any]:
    """Create one immutable, attempt-scoped supervisor context."""
    for name, value in (('job_id', job_id), ('task_index', task_index),
                        ('attempt_number', attempt_number)):
        if not _is_strict_int(value) or value < 0:
            raise ValueError(f'{name} must be a nonnegative integer.')
    if not isinstance(recovery_plan, RecoveryLaunchPlan):
        raise TypeError('recovery_plan must be a RecoveryLaunchPlan')
    if attempt_number > 0 and expected_docker_identity is None:
        raise ValueError('replacement attempt requires Docker identity')
    prune_attempt_directories()
    attempt_id = str(uuid.uuid4())
    marker_dir = (_safe_recovery_root() / str(job_id) / attempt_id).resolve()
    marker_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.chmod(marker_dir, 0o700)
    context: dict[str, Any] = {
        'schema_version': MARKER_SCHEMA_BY_PROFILE_VERSION[
            recovery_plan.profile_version],
        'attempt_id': attempt_id,
        'job_id': job_id,
        'task_index': task_index,
        'attempt_number': attempt_number,
        'node_boot_id':
            (read_boot_id() if expected_boot_id is None else expected_boot_id),
        'created_at': time.time(),
        'marker_dir': str(marker_dir),
        'capability_path': str(marker_dir / 'capability.json'),
        'cleanup_path': str(marker_dir / 'cleanup.json'),
        'profile_version': recovery_plan.profile_version,
        'capability': recovery_plan.capability,
        'require_armed_start': True,
        'expected_docker_identity': (None if expected_docker_identity is None
                                     else expected_docker_identity.to_dict()),
        'expected_parent_pid': None,
    }
    return _validate_attempt_context(context)


def bind_supervisor_parent(context: dict[str, Any],
                           parent_pid: int) -> dict[str, Any]:
    """Bind an attempt copy to the exact Ray worker starting its supervisor."""
    context = _validate_attempt_context(context)
    if not _is_strict_int(parent_pid) or parent_pid <= 1:
        raise RecoveryError('Ray worker PID is invalid.')
    existing = context['expected_parent_pid']
    if existing is not None and existing != parent_pid:
        raise RecoveryError('Attempt is already bound to another Ray worker.')
    bound = dict(context)
    bound['expected_parent_pid'] = parent_pid
    return _validate_attempt_context(bound)


def atomic_write_marker(path: str, payload: dict[str, object]) -> None:
    """Atomically publish a private JSON marker and durably rename it."""
    target = pathlib.Path(path)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix=f'.{target.name}.',
                                          suffix='.tmp',
                                          dir=target.parent)
    published = False
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as file:
            json.dump(payload, file, sort_keys=True, separators=(',', ':'))
            file.write('\n')
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temporary_path, 0o600)
        os.replace(temporary_path, target)
        published = True
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # The atomic rename remains valid on filesystems that do not allow
            # fsync on a directory.  A missing marker only disables recovery.
            pass
    finally:
        if not published:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary_path)


def _atomic_write_private_text(path: pathlib.Path, content: str) -> None:
    """Atomically publish one mode-0600 attempt-scoped text file."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix=f'.{path.name}.',
                                          suffix='.tmp',
                                          dir=path.parent)
    published = False
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temporary_path, 0o600)
        os.replace(temporary_path, path)
        published = True
    finally:
        if not published:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary_path)


def _read_marker(path: str) -> dict[str, object] | None:
    try:
        with open(path, encoding='utf-8') as file:
            payload = json.load(file)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as e:
        raise RecoveryError(f'Cannot read recovery marker: {e}') from e
    if not isinstance(payload, dict):
        raise RecoveryError('Recovery marker is not an object.')
    return payload


def build_supervisor_command(context: dict[str, Any],
                             recovery_plan: RecoveryLaunchPlan) -> list[str]:
    """Build the argv that makes the supervisor the command's real parent."""
    context = _validate_attempt_context(context)
    if context['expected_parent_pid'] is None:
        raise RecoveryError('Supervisor context is not bound to a Ray worker.')
    if not isinstance(recovery_plan, RecoveryLaunchPlan):
        raise TypeError('recovery_plan must be a RecoveryLaunchPlan')
    if recovery_plan.profile_version != context['profile_version']:
        raise ValueError('recovery plan does not match attempt context')
    plan_path = pathlib.Path(str(context['marker_dir'])) / 'plan.json'
    atomic_write_marker(str(plan_path), recovery_plan.to_dict())
    supervisor_command = [
        sys.executable, '-m', 'sky.skylet.subprocess_supervisor',
        '--context-json',
        json.dumps(context, separators=(',', ':')), '--plan-path',
        str(plan_path)
    ]
    assert recovery_plan.execution_envelope is not None
    launch_path = pathlib.Path(str(context['marker_dir'])) / 'launch.sh'
    launch_script = (
        '#!/bin/bash\n'
        f'{recovery_plan.execution_envelope.render_private_file_prelude()}\n'
        f'rm -f -- {shlex.quote(str(launch_path))}\n'
        f'exec {shlex.join(supervisor_command)}\n')
    _atomic_write_private_text(launch_path, launch_script)
    # Bash applies the ordinary task prelude exactly once, then execs the
    # supervisor in the same process/session/environment. The private script
    # removes itself before the exec and no secret appears in this argv.
    return ['/bin/bash', '-i', str(launch_path)]


def consume_private_recovery_plan(
        plan_path: str, context: dict[str, Any]) -> RecoveryLaunchPlan:
    """Read then unlink the exact private supervisor plan file."""
    context = _validate_attempt_context(context)
    expected = pathlib.Path(str(context['marker_dir'])) / 'plan.json'
    supplied = pathlib.Path(plan_path)
    try:
        supplied_resolved = supplied.resolve(strict=True)
    except OSError as e:
        raise RecoveryError(
            f'Cannot resolve supervisor recovery plan: {e}') from e
    if supplied.is_symlink() or supplied_resolved != expected.resolve():
        raise RecoveryError('Supervisor plan path is not attempt-scoped.')
    file_descriptor: int | None = None
    try:
        flags = os.O_RDONLY | getattr(os, 'O_CLOEXEC', 0)
        flags |= getattr(os, 'O_NOFOLLOW', 0)
        file_descriptor = os.open(supplied, flags)
        stat_result = os.fstat(file_descriptor)
        if (not stat.S_ISREG(stat_result.st_mode) or
                stat_result.st_uid != os.getuid() or
                stat_result.st_mode & 0o077):
            raise RecoveryError('Supervisor plan file is not private.')
        with os.fdopen(file_descriptor, encoding='utf-8') as plan_file:
            file_descriptor = None
            payload = json.load(plan_file)
    except (OSError, json.JSONDecodeError) as e:
        raise RecoveryError(f'Cannot read supervisor recovery plan: {e}') from e
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        with contextlib.suppress(FileNotFoundError):
            supplied.unlink()
    try:
        return RecoveryLaunchPlan.from_dict(payload)
    except (TypeError, ValueError) as e:
        raise RecoveryError(f'Invalid supervisor recovery plan: {e}') from e


def _docker_environment() -> dict[str, str]:
    configured_host = os.environ.get('DOCKER_HOST')
    if configured_host not in (None, '', DOCKER_HOST, '/var/run/docker.sock'):
        raise RecoveryError('DOCKER_HOST does not select the local socket.')
    configured_context = os.environ.get('DOCKER_CONTEXT')
    if configured_context not in (None, '', 'default'):
        raise RecoveryError('DOCKER_CONTEXT is not the local default context.')
    environment = os.environ.copy()
    environment['DOCKER_HOST'] = DOCKER_HOST
    environment.pop('DOCKER_CONTEXT', None)
    return environment


def _run_docker(arguments: list[str], *, force_local: bool = True) -> str:
    environment = (_docker_environment() if force_local else os.environ.copy())
    try:
        result = subprocess.run(['docker', *arguments],
                                check=False,
                                capture_output=True,
                                text=True,
                                timeout=DOCKER_COMMAND_TIMEOUT_SECONDS,
                                env=environment)
    except (OSError, subprocess.SubprocessError) as e:
        raise RecoveryError(f'Local Docker command failed: {e}') from e
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RecoveryError(
            f'Local Docker command exited {result.returncode}: {detail}')
    return result.stdout.strip()


def get_docker_identity() -> DockerIdentity:
    """Prove the local engine ID and its PID-reuse-safe process identity."""
    # Resolve the current CLI context before overriding DOCKER_HOST.  This
    # prevents an otherwise hidden remote current context from being armed.
    # First inspect the user's actual CLI selection.  Later calls pin the
    # accepted local socket, preventing a context change during the handshake.
    _docker_environment()
    context_name = _run_docker(['context', 'show'], force_local=False)
    if context_name != 'default':
        raise RecoveryError('The active Docker context is not default.')
    try:
        with open(DOCKER_PID_PATH, encoding='utf-8') as file:
            daemon_pid = int(file.read().strip())
    except (OSError, ValueError) as e:
        raise RecoveryError('Cannot read the local Docker daemon PID.') from e
    if daemon_pid <= 1:
        raise RecoveryError('The local Docker daemon PID is invalid.')
    try:
        daemon_process = psutil.Process(daemon_pid)
        daemon_create_time = daemon_process.create_time()
    except (psutil.Error, OSError) as e:
        raise RecoveryError('Cannot identify the local Docker daemon.') from e
    daemon_id = _run_docker(['info', '--format', '{{.ID}}'])
    if not daemon_id:
        raise RecoveryError('The local Docker daemon returned no engine ID.')
    return DockerIdentity(host=DOCKER_HOST,
                          daemon_id=daemon_id,
                          daemon_pid=daemon_pid,
                          daemon_pid_create_time=float(daemon_create_time))


def docker_container_inventory() -> tuple[str, ...]:
    """Return the complete, untruncated local Docker container inventory."""
    output = _run_docker(['ps', '-aq', '--no-trunc'])
    return tuple(line.strip() for line in output.splitlines() if line.strip())


def docker_identity_matches(expected: DockerIdentity) -> bool:
    """Return whether the current local engine has the expected identity."""
    try:
        return get_docker_identity() == expected
    except RecoveryError:
        return False


def wait_for_stable_empty_docker(
        expected: DockerIdentity,
        deadline_monotonic: float,
        *,
        samples: int = DOCKER_EMPTY_SAMPLES,
        interval_seconds: float = DOCKER_EMPTY_SAMPLE_INTERVAL_SECONDS) -> bool:
    """Require the same Docker engine and an empty inventory repeatedly."""
    consecutive = 0
    while consecutive < samples:
        if time.monotonic() > deadline_monotonic:
            return False
        try:
            if (get_docker_identity() != expected or
                    docker_container_inventory()):
                return False
        except RecoveryError:
            return False
        consecutive += 1
        if consecutive < samples:
            remaining = deadline_monotonic - time.monotonic()
            if remaining < interval_seconds:
                return False
            time.sleep(interval_seconds)
    return True


def _validate_marker_binding(marker: dict[str, object], kind: str,
                             context: dict[str, Any]) -> None:
    marker_schema = marker.get('schema_version')
    if (type(marker_schema) is not int or  # pylint: disable=unidiomatic-typecheck
            marker_schema != context['schema_version']
            or marker.get('kind') != kind):
        raise RecoveryError(f'{kind} marker header is invalid.')
    for key, expected in _attempt_fields(context).items():
        actual = marker.get(key)
        if ((_is_strict_int(expected) and not _is_strict_int(actual)) or
                actual != expected):
            raise RecoveryError(f'{kind} marker {key} does not match.')


def read_capability_marker(context: dict[str, Any]) -> dict[str, object] | None:
    """Read and validate the attempt's optional capability marker."""
    context = _validate_attempt_context(context)
    marker = _read_marker(str(context['capability_path']))
    if marker is None:
        return None
    _validate_marker_binding(marker, 'capability', context)
    if marker.get('capability') != context['capability']:
        raise RecoveryError('Capability marker names an unknown capability.')
    if marker.get('node_boot_id') != read_boot_id():
        raise RecoveryError('Capability marker belongs to another boot.')
    marker_time = marker.get('written_at')
    context_created_at = context['created_at']
    assert isinstance(context_created_at, (int, float))
    if (not _is_finite_number(marker_time) or
            float(marker_time) + 1 < float(context_created_at) or
            float(marker_time) > time.time() + 30):
        raise RecoveryError('Capability marker timestamp is stale.')
    armed = marker.get('armed')
    if not isinstance(armed, bool):
        raise RecoveryError('Capability marker armed state is invalid.')
    owned_container_id = marker.get('owned_container_id')
    if (armed and
        (not isinstance(owned_container_id, str) or
         _CONTAINER_ID_PATTERN.fullmatch(owned_container_id) is None)):
        raise RecoveryError('Capability owned container ID is invalid.')
    if (not armed and owned_container_id is not None and
        (not isinstance(owned_container_id, str) or
         _CONTAINER_ID_PATTERN.fullmatch(owned_container_id) is None)):
        raise RecoveryError('Capability owned container ID is invalid.')
    if armed:
        DockerIdentity.from_dict(marker.get('docker_identity'))
        _validate_supervisor_identity(marker.get('supervisor'))
    return marker


def _validate_supervisor_identity(value: object) -> tuple[int, float]:
    if not isinstance(value, dict):
        raise RecoveryError('Supervisor identity is missing.')
    pid = value.get('pid')
    create_time = value.get('pid_create_time')
    if (not _is_strict_int(pid) or pid <= 1 or
            not _is_finite_number(create_time)):
        raise RecoveryError('Supervisor identity is invalid.')
    return pid, float(create_time)


def validate_cleanup_marker(  # pylint: disable=too-many-return-statements
        context: dict[str, Any], deadline_monotonic: float) -> tuple[bool, str]:
    """Validate the exact graceful marker, then recheck Docker quiescence."""
    try:
        capability = read_capability_marker(context)
        if capability is None:
            return False, 'capability marker is missing'
        if not capability.get('armed'):
            return False, 'attempt did not arm recovery'
        cleanup = _read_marker(str(context['cleanup_path']))
        if cleanup is None:
            return False, 'cleanup marker is missing'
        _validate_marker_binding(cleanup, 'cleanup', context)
        if _validate_supervisor_identity(
                cleanup.get('supervisor')) != (_validate_supervisor_identity(
                    capability.get('supervisor'))):
            return False, 'cleanup supervisor identity does not match'
        if cleanup.get('docker_identity') != capability.get('docker_identity'):
            return False, 'cleanup Docker identity does not match'
        cleanup_container_id = cleanup.get('owned_container_id')
        capability_container_id = capability.get('owned_container_id')
        if (not isinstance(cleanup_container_id, str) or
                _CONTAINER_ID_PATTERN.fullmatch(cleanup_container_id) is None or
                cleanup_container_id != capability_container_id):
            return False, 'cleanup owned container ID does not match'
        incomplete_cleanup = (
            cleanup.get('forced') is not False,
            cleanup.get('timed_out') is not False,
            cleanup.get('descendants_empty') is not True,
            cleanup.get('docker_empty') is not True,
            cleanup.get('enumeration_proven') is not True,
            cleanup.get('graceful') is not True,
        )
        if any(incomplete_cleanup):
            return False, 'cleanup was forced, timed out, or incomplete'
        completed_at = cleanup.get('completed_at')
        capability_written_at = capability.get('written_at')
        if (not _is_finite_number(completed_at) or
                not _is_finite_number(capability_written_at) or
                float(completed_at) < float(capability_written_at) or
                float(completed_at) > time.time() + 30):
            return False, 'cleanup timestamp is stale'
        expected_docker = DockerIdentity.from_dict(
            cleanup.get('docker_identity'))
        if not wait_for_stable_empty_docker(expected_docker,
                                            deadline_monotonic):
            return False, 'Docker identity or empty inventory is not stable'
    except RecoveryError as e:
        return False, str(e)
    return True, ''


def wait_for_cleanup_marker(context: dict[str, Any],
                            deadline_monotonic: float) -> tuple[bool, str]:
    """Wait for the attempt-scoped cleanup marker within one global budget."""
    last_reason = 'cleanup marker is missing'
    while time.monotonic() <= deadline_monotonic:
        valid, reason = validate_cleanup_marker(context, deadline_monotonic)
        if valid:
            return True, ''
        last_reason = reason
        if reason != 'cleanup marker is missing':
            return False, reason
        remaining = deadline_monotonic - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.5, remaining))
    return False, last_reason


def _ray_memory_threshold() -> float:
    raw_threshold = os.environ.get('RAY_memory_usage_threshold')
    try:
        threshold = (DEFAULT_RAY_MEMORY_USAGE_THRESHOLD
                     if raw_threshold is None else float(raw_threshold))
    except ValueError:
        threshold = DEFAULT_RAY_MEMORY_USAGE_THRESHOLD
    if not math.isfinite(threshold) or threshold <= 0 or threshold > 1:
        threshold = DEFAULT_RAY_MEMORY_USAGE_THRESHOLD
    return min(MAX_REPLAY_MEMORY_USAGE_FRACTION,
               max(0.0, threshold - RAY_MEMORY_HYSTERESIS))


def wait_for_safe_memory(
    ray_module: Any,
    deadline_monotonic: float,
    *,
    profile_version: int | None = None,
) -> tuple[bool, str]:
    """Wait for Ray's cgroup-aware memory readings below a stable watermark."""

    def _result(accepted: bool, message: str, reason: str,
                cgroup_state: str) -> tuple[bool, str]:
        _log_driver_admission(stage='replay_memory',
                              decision=('accepted' if accepted else 'rejected'),
                              reason=reason,
                              cgroup_state=cgroup_state,
                              ray_session_state='not_checked',
                              profile_version=profile_version)
        return accepted, message

    try:
        ray_utils = ray_module._private.utils  # pylint: disable=protected-access
        get_used_memory = ray_utils.get_used_memory
        get_system_memory = ray_utils.get_system_memory
    except AttributeError:
        return _result(False, 'Ray cgroup-aware memory helpers are unavailable',
                       'cgroup_unavailable', 'unavailable')
    threshold = _ray_memory_threshold()
    consecutive = 0
    while time.monotonic() <= deadline_monotonic:
        try:
            used = float(get_used_memory())
            total = float(get_system_memory())
        except Exception as e:  # pylint: disable=broad-except
            return _result(False, f'Ray memory reading failed: {e}',
                           'cgroup_unavailable', 'unavailable')
        if (not math.isfinite(used) or not math.isfinite(total) or total <= 0 or
                used < 0):
            return _result(False, 'Ray memory reading is invalid',
                           'cgroup_invalid', 'invalid')
        if total > MAX_RECOVERY_HOST_MEMORY_BYTES:
            return _result(False, 'cgroup-aware host memory exceeds 16 GiB',
                           'cgroup_above_16_gib', 'above_16_gib')
        if used / total < threshold:
            consecutive += 1
            if consecutive >= MEMORY_SAFE_SAMPLES:
                return _result(True, '', 'memory_safe', 'at_most_16_gib')
        else:
            consecutive = 0
        remaining = deadline_monotonic - time.monotonic()
        if remaining < MEMORY_SAFE_SAMPLE_INTERVAL_SECONDS:
            break
        time.sleep(MEMORY_SAFE_SAMPLE_INTERVAL_SECONDS)
    return _result(False, 'memory did not reach the stable recovery watermark',
                   'memory_watermark_timeout', 'at_most_16_gib')


def ray_runtime_is_healthy(
    ray_module: Any,
    ray_util_module: Any,
    placement_group: Any,
    expected_boot_id: str,
    expected_ray_session_identity: str | None,
    *,
    profile_version: int | None = None,
) -> tuple[bool, str]:
    """Check the same boot/Ray session and CREATED placement group."""

    def _result(accepted: bool, message: str, reason: str,
                ray_session_state: str) -> tuple[bool, str]:
        _log_driver_admission(stage='replay_runtime',
                              decision=('accepted' if accepted else 'rejected'),
                              reason=reason,
                              cgroup_state='not_checked',
                              ray_session_state=ray_session_state,
                              profile_version=profile_version)
        return accepted, message

    if not expected_boot_id or read_boot_id() != expected_boot_id:
        return _result(False, 'node boot identity changed', 'boot_changed',
                       'not_checked')
    if (not isinstance(expected_ray_session_identity, str) or
            not expected_ray_session_identity):
        return _result(False, 'Ray session identity is unavailable',
                       'ray_session_unavailable', 'unavailable')
    current_ray_session_identity = capture_ray_session_identity(ray_module)
    if current_ray_session_identity is None:
        return _result(False, 'Ray session identity is unavailable',
                       'ray_session_unavailable', 'unavailable')
    if current_ray_session_identity != expected_ray_session_identity:
        return _result(False, 'Ray session identity changed',
                       'ray_session_changed', 'changed')
    try:
        table = ray_util_module.placement_group_table(placement_group)
    except Exception as e:  # pylint: disable=broad-except
        return _result(False, f'cannot query placement group: {e}',
                       'placement_group_unavailable', 'unchanged')
    if not isinstance(table, dict) or table.get('state') != 'CREATED':
        return _result(False, 'placement group is no longer CREATED',
                       'placement_group_not_created', 'unchanged')
    return _result(True, '', 'accepted', 'unchanged')


_REQUIRED_RECOVERY_PHASES = frozenset({
    'ARMED', 'WAITING_CLEANUP', 'WAITING_MEMORY', 'RESUBMITTING',
    'RETRY_SUBMITTED', 'EXHAUSTED'
})


class RecoveryArmState(enum.Enum):
    """One-way local admission latch; deliberately not a remote API field."""

    PENDING = 'PENDING'
    ARMED = 'ARMED'
    DISABLED = 'DISABLED'


@functools.lru_cache(maxsize=1)
def validate_runtime_capability() -> None:
    """Validate the exact remote runtime contract once per driver process."""
    if job_lib.JOB_SYSTEM_RECOVERY_API_VERSION != 1:
        raise RecoveryError('unsupported job system-recovery API version')
    if ({phase.name for phase in job_lib.JobSystemRecoveryPhase}
            != _REQUIRED_RECOVERY_PHASES):
        raise RecoveryError('job system-recovery phases do not match API v1')
    if not dataclasses.is_dataclass(job_lib.JobSystemRecoveryInfo):
        raise RecoveryError('job system-recovery info type is unavailable')
    expected_fields = (('capability', str), ('phase',
                                             job_lib.JobSystemRecoveryPhase),
                       ('original_attempt_id', str), ('replacement_attempt_id',
                                                      str | None),
                       ('task_index', int), ('node_boot_id',
                                             str), ('occurrence_count', int),
                       ('armed_at', float), ('updated_at', float), ('event_id',
                                                                    str | None),
                       ('reason', str | None), ('occurred_at', float | None),
                       ('deadline_at', float | None), ('summary', str | None))
    actual_fields = tuple(
        (field.name, field.type)
        for field in dataclasses.fields(job_lib.JobSystemRecoveryInfo))
    if actual_fields != expected_fields:
        raise RecoveryError('job system-recovery info schema does not match v1')
    required_operations = {
        job_lib.arm_job_system_recovery: ('job_id', 'info'),
        job_lib.arm_job_system_recovery_no_lock: ('job_id', 'info'),
        job_lib.transition_job_system_recovery:
            ('job_id', 'expected_phase', 'info'),
        job_lib.transition_job_system_recovery_no_lock:
            ('job_id', 'expected_phase', 'info'),
        job_lib.exhaust_job_system_recovery:
            ('job_id', 'expected_phase', 'info'),
        job_lib.exhaust_job_system_recovery_no_lock:
            ('job_id', 'expected_phase', 'info'),
        job_lib.get_job_system_recovery_info: ('job_id',),
        job_lib.get_status: ('job_id',),
        job_lib.fail_job_system_recovery_no_lock: ('job_id',),
        job_lib.job_status_lock: ('job_id',),
    }
    for operation, expected_parameters in required_operations.items():
        if (not callable(operation) or
                tuple(inspect.signature(operation).parameters)
                != expected_parameters):
            raise RecoveryError('job system-recovery API v1 is incomplete')


@dataclasses.dataclass
class RecoverySession:  # pylint: disable=too-many-instance-attributes
    """Single owner of identities and transitions for one bounded replay."""

    job_id: int
    recovery_plan: RecoveryLaunchPlan
    original_context: dict[str, Any]
    original_future: Any
    submitter: Callable[[int, dict[str, Any]], Any]
    arm_started_monotonic: float | None = dataclasses.field(default=None,
                                                            repr=False)
    expected_ray_session_identity: str | None = dataclasses.field(default=None,
                                                                  repr=False)
    wall_clock: Callable[[], float] = dataclasses.field(default=time.time,
                                                        repr=False)
    monotonic_clock: Callable[[],
                              float] = dataclasses.field(default=time.monotonic,
                                                         repr=False)
    wait: Callable[[float], None] = dataclasses.field(default=time.sleep,
                                                      repr=False)
    current_context: dict[str, Any] = dataclasses.field(init=False)
    current_future: Any = dataclasses.field(init=False)
    phase: job_lib.JobSystemRecoveryPhase = dataclasses.field(
        init=False, default=job_lib.JobSystemRecoveryPhase.ARMED)
    armed_info: job_lib.JobSystemRecoveryInfo | None = dataclasses.field(
        init=False, default=None)
    event_id: str | None = dataclasses.field(init=False, default=None)
    occurrence_count: int = dataclasses.field(init=False, default=0)
    occurred_at: float | None = dataclasses.field(init=False, default=None)
    deadline_at: float | None = dataclasses.field(init=False, default=None)
    deadline_monotonic: float | None = dataclasses.field(init=False,
                                                         default=None)
    first_event_visible_monotonic: float | None = dataclasses.field(
        init=False, default=None)
    cleanup_proof_completed_monotonic: float | None = dataclasses.field(
        init=False, default=None)
    event_visibility_confirmed: bool = dataclasses.field(init=False,
                                                         default=False)
    replacement_context: dict[str, Any] | None = dataclasses.field(init=False,
                                                                   default=None)
    replacement_future: Any = dataclasses.field(init=False, default=None)
    arm_state: RecoveryArmState = dataclasses.field(
        init=False, default=RecoveryArmState.PENDING)
    arm_deadline_monotonic: float = dataclasses.field(init=False)
    arm_disabled_reason: str | None = dataclasses.field(init=False,
                                                        default=None)
    arm_admission_logged: bool = dataclasses.field(init=False,
                                                   default=False,
                                                   repr=False)

    def __post_init__(self) -> None:
        validate_runtime_capability()
        self.original_context = _validate_attempt_context(self.original_context)
        if self.original_context['attempt_number'] != 0:
            raise RecoveryError('original attempt number must be zero')
        if self.original_context[
                'profile_version'] != self.recovery_plan.profile_version:
            raise RecoveryError('attempt context does not match launch plan')
        self.current_context = self.original_context
        self.current_future = self.original_future
        if self.arm_started_monotonic is None:
            self.arm_started_monotonic = self.monotonic_clock()
        if (not _is_finite_number(self.arm_started_monotonic) or
                self.arm_started_monotonic < 0):
            raise RecoveryError('arm-window monotonic start is invalid')
        self.arm_started_monotonic = float(self.arm_started_monotonic)
        self.arm_deadline_monotonic = (self.arm_started_monotonic +
                                       SYSTEM_RECOVERY_ARM_WINDOW_SECONDS)
        if (not isinstance(self.expected_ray_session_identity, str) or
                not self.expected_ray_session_identity):
            self.disable_arm('Ray session identity is unavailable',
                             admission_reason='ray_session_unavailable',
                             ray_session_state='unavailable')

    @property
    def is_replacement(self) -> bool:
        return self.phase == job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED

    def _info(self, phase: job_lib.JobSystemRecoveryPhase,
              summary: str | None) -> job_lib.JobSystemRecoveryInfo:
        if summary is not None:
            summary = summary[:job_lib.JOB_SYSTEM_RECOVERY_SUMMARY_MAX_CHARS]
        armed_at = (self.wall_clock()
                    if self.armed_info is None else self.armed_info.armed_at)
        updated_at = max(self.wall_clock(), armed_at)
        return job_lib.JobSystemRecoveryInfo(
            capability=self.recovery_plan.capability,
            phase=phase,
            original_attempt_id=str(self.original_context['attempt_id']),
            replacement_attempt_id=(
                None if self.replacement_context is None else str(
                    self.replacement_context['attempt_id'])),
            task_index=int(self.original_context['task_index']),
            node_boot_id=str(self.original_context['node_boot_id']),
            occurrence_count=self.occurrence_count,
            armed_at=armed_at,
            updated_at=updated_at,
            event_id=self.event_id,
            reason=('RAY_NODE_OOM' if self.event_id is not None else None),
            occurred_at=self.occurred_at,
            deadline_at=self.deadline_at,
            summary=summary)

    def _log_arm_admission_once(self, *, decision: str, reason: str,
                                cgroup_state: str,
                                ray_session_state: str) -> None:
        if self.arm_admission_logged:
            return
        self.arm_admission_logged = True
        _log_driver_admission(
            stage='arm',
            decision=decision,
            reason=reason,
            cgroup_state=cgroup_state,
            ray_session_state=ray_session_state,
            profile_version=self.recovery_plan.profile_version)

    def disable_arm(self,
                    reason: str,
                    *,
                    admission_reason: str = 'recovery_state_unavailable',
                    cgroup_state: str = 'unknown',
                    ray_session_state: str = 'captured') -> None:
        if self.arm_state in (RecoveryArmState.ARMED,
                              RecoveryArmState.DISABLED):
            return
        self.arm_state = RecoveryArmState.DISABLED
        self.arm_disabled_reason = reason
        self._log_arm_admission_once(decision='rejected',
                                     reason=admission_reason,
                                     cgroup_state=cgroup_state,
                                     ray_session_state=ray_session_state)

    def try_arm(self, marker: dict[str, object] | None,
                ray_module: Any) -> bool:
        if self.armed_info is not None:
            self.arm_state = RecoveryArmState.ARMED
            self._log_arm_admission_once(decision='accepted',
                                         reason='accepted',
                                         cgroup_state='unknown',
                                         ray_session_state='captured')
            return True
        if self.arm_state == RecoveryArmState.DISABLED:
            return False
        if self.monotonic_clock() >= self.arm_deadline_monotonic:
            self.disable_arm('recovery arm window expired',
                             admission_reason='arm_window_expired')
            return False
        if marker is None:
            return False
        if marker.get('armed') is not True:
            self.disable_arm('runtime capability marker rejected arming',
                             admission_reason='marker_rejected')
            return False
        written_at = marker.get('written_at')
        if not _is_finite_number(written_at):
            self.disable_arm('runtime capability marker is malformed',
                             admission_reason='marker_malformed')
            return False
        try:
            ray_utils = ray_module._private.utils  # pylint: disable=protected-access
            total_memory = float(ray_utils.get_system_memory())
        except Exception as error:  # pylint: disable=broad-except
            self.disable_arm(f'cgroup-aware memory reading failed: {error}',
                             admission_reason='cgroup_unavailable',
                             cgroup_state='unavailable')
            return False
        if not math.isfinite(total_memory) or total_memory <= 0:
            self.disable_arm('cgroup-aware host memory is invalid',
                             admission_reason='cgroup_invalid',
                             cgroup_state='invalid')
            return False
        if total_memory > MAX_RECOVERY_HOST_MEMORY_BYTES:
            self.disable_arm('cgroup-aware host memory exceeds 16 GiB',
                             admission_reason='cgroup_above_16_gib',
                             cgroup_state='above_16_gib')
            return False
        info = job_lib.JobSystemRecoveryInfo(
            capability=self.recovery_plan.capability,
            phase=job_lib.JobSystemRecoveryPhase.ARMED,
            original_attempt_id=str(self.original_context['attempt_id']),
            replacement_attempt_id=None,
            task_index=int(self.original_context['task_index']),
            node_boot_id=str(self.original_context['node_boot_id']),
            occurrence_count=0,
            armed_at=float(written_at),
            updated_at=max(self.wall_clock(), float(written_at)))
        try:
            with job_lib.job_status_lock(self.job_id):
                if self.monotonic_clock() >= self.arm_deadline_monotonic:
                    self.disable_arm(
                        'recovery arm window expired while '
                        'acquiring the job lock',
                        admission_reason='arm_window_expired',
                        cgroup_state='at_most_16_gib')
                    return False
                if job_lib.arm_job_system_recovery_no_lock(self.job_id, info):
                    self.armed_info = info
                    self.arm_state = RecoveryArmState.ARMED
                    self._log_arm_admission_once(decision='accepted',
                                                 reason='accepted',
                                                 cgroup_state='at_most_16_gib',
                                                 ray_session_state='captured')
                    return True
                existing = job_lib.get_job_system_recovery_info(self.job_id)
        except Exception as error:  # pylint: disable=broad-except
            self.disable_arm(f'cannot persist recovery ARMED: {error}',
                             admission_reason='recovery_state_unavailable',
                             cgroup_state='at_most_16_gib')
            return False
        if existing == info:
            self.armed_info = existing
            self.arm_state = RecoveryArmState.ARMED
            self._log_arm_admission_once(decision='accepted',
                                         reason='accepted',
                                         cgroup_state='at_most_16_gib',
                                         ray_session_state='captured')
            return True
        self.disable_arm('existing recovery state does not match ARMED',
                         admission_reason='recovery_state_mismatch',
                         cgroup_state='at_most_16_gib')
        return False

    def observe_oom(self) -> None:
        self.occurrence_count += 1
        if self.event_id is None:
            now = self.wall_clock()
            self.event_id = str(uuid.uuid4())
            self.occurred_at = now
            self.deadline_at = now + RECOVERY_TIMEOUT_SECONDS
            self.deadline_monotonic = (self.monotonic_clock() +
                                       RECOVERY_TIMEOUT_SECONDS)

    def wait_for_first_event_visibility(self) -> tuple[bool, str]:
        """Hold replay until the first OOM has been observable for 35s."""
        if (self.event_id is None or self.occurred_at is None or
                self.deadline_monotonic is None or
                self.first_event_visible_monotonic is None):
            return False, 'first recovery event is unavailable'
        if not self.event_visibility_confirmed:
            remaining_visibility = max(
                0.0, self.first_event_visible_monotonic +
                FIRST_EVENT_VISIBILITY_SECONDS - self.monotonic_clock())
            remaining_deadline = max(
                0.0, self.deadline_monotonic - self.monotonic_clock())
            wait_seconds = min(remaining_visibility, remaining_deadline)
            try:
                if wait_seconds > 0:
                    self.wait(wait_seconds)
            except Exception as e:  # pylint: disable=broad-except
                return False, f'first-event visibility wait failed: {e}'
            if self.monotonic_clock() >= self.deadline_monotonic:
                return False, ('recovery deadline expired during first-event '
                               'visibility wait')
            if (self.monotonic_clock() < self.first_event_visible_monotonic +
                    FIRST_EVENT_VISIBILITY_SECONDS):
                return False, ('first event did not remain visible for '
                               f'{FIRST_EVENT_VISIBILITY_SECONDS} seconds')
        try:
            status = job_lib.get_status(self.job_id)
        except Exception as e:  # pylint: disable=broad-except
            return False, f'cannot recheck job state after visibility wait: {e}'
        if status != job_lib.JobStatus.RUNNING:
            return False, ('job is no longer running after first-event '
                           'visibility wait')
        self.event_visibility_confirmed = True
        return True, ''

    def record_cleanup_proof_completed(self) -> None:
        """Capture when exact process/container cleanup was proven positive."""
        if self.cleanup_proof_completed_monotonic is not None:
            raise RecoveryError('cleanup proof completion was already recorded')
        completed = self.monotonic_clock()
        if not _is_finite_number(completed) or completed < 0:
            raise RecoveryError('cleanup proof monotonic completion is invalid')
        self.cleanup_proof_completed_monotonic = float(completed)

    def wait_for_replay_quiescence(self) -> tuple[bool, str]:
        """Wait until cleanup-proof +83s without extending the OOM deadline."""
        if (self.cleanup_proof_completed_monotonic is None or
                self.deadline_monotonic is None):
            return False, 'positive cleanup proof timing is unavailable'
        replay_not_before = (self.cleanup_proof_completed_monotonic +
                             SYSTEM_RECOVERY_REPLAY_QUIESCENCE_SECONDS)
        now = self.monotonic_clock()
        remaining_quiescence = max(0.0, replay_not_before - now)
        remaining_deadline = max(0.0, self.deadline_monotonic - now)
        wait_seconds = min(remaining_quiescence, remaining_deadline)
        try:
            if wait_seconds > 0:
                self.wait(wait_seconds)
        except Exception as e:  # pylint: disable=broad-except
            return False, f'replay quiescence wait failed: {e}'
        now = self.monotonic_clock()
        if now >= self.deadline_monotonic:
            return False, 'recovery deadline expired during replay quiescence'
        if now < replay_not_before:
            return False, ('positive cleanup proof did not remain quiescent '
                           f'for {SYSTEM_RECOVERY_REPLAY_QUIESCENCE_SECONDS} '
                           'seconds')
        return True, ''

    def transition(self, phase: job_lib.JobSystemRecoveryPhase,
                   summary: str) -> bool:
        info = self._info(phase, summary)
        try:
            persisted = job_lib.transition_job_system_recovery(
                self.job_id, self.phase, info)
        except Exception:  # pylint: disable=broad-except
            persisted = False
        if persisted:
            self.phase = phase
            if phase == job_lib.JobSystemRecoveryPhase.WAITING_CLEANUP:
                self.first_event_visible_monotonic = self.monotonic_clock()
        return persisted

    def _fallback_fail_no_lock(self) -> None:
        try:
            job_lib.fail_job_system_recovery_no_lock(self.job_id)
        except Exception:  # pylint: disable=broad-except
            return

    def exhaust_locked(self, summary: str) -> bool:
        info = self._info(job_lib.JobSystemRecoveryPhase.EXHAUSTED, summary)
        try:
            exhausted = job_lib.exhaust_job_system_recovery_no_lock(
                self.job_id, self.phase, info)
        except Exception:  # pylint: disable=broad-except
            exhausted = False
        if exhausted:
            self.phase = job_lib.JobSystemRecoveryPhase.EXHAUSTED
            return True
        self._fallback_fail_no_lock()
        return False

    def exhaust(self, summary: str) -> None:
        info = self._info(job_lib.JobSystemRecoveryPhase.EXHAUSTED, summary)
        try:
            exhausted = job_lib.exhaust_job_system_recovery(
                self.job_id, self.phase, info)
        except Exception:  # pylint: disable=broad-except
            exhausted = False
        if exhausted:
            self.phase = job_lib.JobSystemRecoveryPhase.EXHAUSTED
            return
        try:
            with job_lib.job_status_lock(self.job_id):
                self._fallback_fail_no_lock()
        except Exception:  # pylint: disable=broad-except
            return

    def submit_one_retry(self, ray_module: Any) -> bool:
        """Durably allocate, submit, and adopt exactly one replacement."""
        if self.replacement_context is not None or self.replacement_future is not None:
            self.exhaust('replacement attempt was already allocated')
            return False
        visibility_ok, visibility_reason = self.wait_for_first_event_visibility(
        )
        if not visibility_ok:
            self.exhaust(visibility_reason)
            return False
        quiescence_ok, quiescence_reason = self.wait_for_replay_quiescence()
        if not quiescence_ok:
            self.exhaust(quiescence_reason)
            return False
        assert self.deadline_monotonic is not None
        if self.monotonic_clock() >= self.deadline_monotonic:
            self.exhaust('recovery deadline expired before resubmission')
            return False
        try:
            capability = read_capability_marker(self.original_context)
            if capability is None or capability.get('armed') is not True:
                raise RecoveryError('original capability marker is unavailable')
            docker_identity = DockerIdentity.from_dict(
                capability.get('docker_identity'))
            candidate_context = new_attempt_context(
                self.job_id,
                int(self.original_context['task_index']),
                1,
                self.recovery_plan,
                expected_boot_id=str(self.original_context['node_boot_id']),
                expected_docker_identity=docker_identity)
        except Exception as e:  # pylint: disable=broad-except
            self.exhaust(f'cannot create replacement context: {e}')
            return False

        cancel_replacement = False
        adoption_protocol_failed = False
        try:
            with job_lib.job_status_lock(self.job_id):
                if self.monotonic_clock() >= self.deadline_monotonic:
                    self.exhaust_locked(
                        'recovery deadline expired while acquiring the job lock'
                    )
                    return False
                resubmitting = dataclasses.replace(
                    self._info(job_lib.JobSystemRecoveryPhase.RESUBMITTING,
                               'submitting one replacement Ray task'),
                    replacement_attempt_id=str(candidate_context['attempt_id']))
                try:
                    persisted = job_lib.transition_job_system_recovery_no_lock(
                        self.job_id, self.phase, resubmitting)
                except Exception:  # pylint: disable=broad-except
                    persisted = False
                if not persisted:
                    self.exhaust_locked('cannot persist RESUBMITTING')
                    return False
                self.replacement_context = candidate_context
                self.phase = job_lib.JobSystemRecoveryPhase.RESUBMITTING
                try:
                    replacement_future = self.submitter(1, candidate_context)
                except Exception as e:  # pylint: disable=broad-except
                    self.exhaust_locked(
                        f'Ray replacement submission failed: {e}')
                    return False
                if replacement_future is None:
                    self.exhaust_locked(
                        'Ray replacement submission returned no ObjectRef')
                    return False
                self.replacement_future = replacement_future
                if self.monotonic_clock() >= self.deadline_monotonic:
                    self.exhaust_locked(
                        'recovery deadline expired during Ray replacement '
                        'submission')
                    cancel_replacement = True
                else:
                    retry_info = self._info(
                        job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED,
                        'replacement Ray task submitted')
                    try:
                        adopted = (
                            job_lib.transition_job_system_recovery_no_lock(
                                self.job_id, self.phase, retry_info))
                    except Exception:  # pylint: disable=broad-except
                        adopted = False
                    if not adopted:
                        self.exhaust_locked('cannot persist RETRY_SUBMITTED')
                        cancel_replacement = True
                    else:
                        self.phase = (
                            job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED)
                        self.current_context = candidate_context
                        self.current_future = replacement_future
        except Exception as e:  # pylint: disable=broad-except
            # A lock/context-manager failure must not escape and leave a
            # replacement unaccounted for. A durably adopted ref remains
            # adopted; every other submitted ref follows the cancel path.
            if self.phase == job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED:
                return True
            self.exhaust(f'replacement adoption protocol failed: {e}')
            adoption_protocol_failed = True
            cancel_replacement = self.replacement_future is not None
        if cancel_replacement:
            with contextlib.suppress(Exception):
                ray_module.cancel(self.replacement_future, force=True)
            return False
        return not adoption_protocol_failed


def get_or_fail_with_recovery(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-return-statements,too-many-branches,too-many-statements
    ray_module: Any,
    ray_util_module: Any,
    future: Any,
    placement_group: Any,
    submitter: Callable[[int, dict[str, Any]], Any],
    initial_context: dict[str, Any],
    job_id: int,
    recovery_plan: RecoveryLaunchPlan,
    arm_started_monotonic: float | None = None,
    expected_ray_session_identity: str | None = None,
) -> tuple[list[int], list[int | None]]:
    """Wait for one task and manually replay one positively-cleaned Ray OOM."""
    try:
        validate_runtime_capability()
        session = RecoverySession(
            job_id,
            recovery_plan,
            initial_context,
            future,
            submitter,
            arm_started_monotonic=arm_started_monotonic,
            expected_ray_session_identity=(expected_ray_session_identity))
        oom_type: type[BaseException] = ray_module.exceptions.OutOfMemoryError
        while True:
            if (session.armed_info is None and
                    session.arm_state == RecoveryArmState.PENDING):
                try:
                    marker = read_capability_marker(session.original_context)
                except RecoveryError as error:
                    session.disable_arm(
                        f'runtime capability marker is malformed: {error}',
                        admission_reason='marker_malformed')
                    marker = None
                session.try_arm(marker, ray_module)
            ready: list[Any] = []
            try:
                ready, _ = ray_module.wait([session.current_future], timeout=1)
            except Exception as e:  # pylint: disable=broad-except
                if not session.is_replacement:
                    raise
                session.exhaust(f'waiting for replacement failed: {e}')
                return [1], [None]
            if not ready:
                continue
            result: Any = None
            try:
                result = ray_module.get(session.current_future)
            except oom_type:
                if session.armed_info is None:
                    try:
                        marker = read_capability_marker(
                            session.original_context)
                    except RecoveryError as error:
                        session.disable_arm(
                            f'runtime capability marker is malformed: {error}',
                            admission_reason='marker_malformed')
                        marker = None
                    session.try_arm(marker, ray_module)
                session.observe_oom()
                if session.occurrence_count > 1:
                    session.exhaust('second Ray node OOM')
                    return [1], [None]
                if session.armed_info is None:
                    if session.arm_state == RecoveryArmState.PENDING:
                        session.disable_arm(
                            'capability marker was unavailable at Ray OOM',
                            admission_reason='capability_unavailable_at_oom')
                    session.exhaust('attempt did not arm recovery')
                    return [1], [None]
                if not session.transition(
                        job_lib.JobSystemRecoveryPhase.WAITING_CLEANUP,
                        'Ray killed the service worker for node memory'):
                    session.exhaust('cannot persist WAITING_CLEANUP')
                    return [1], [None]
                assert session.deadline_monotonic is not None
                cleanup_ok, cleanup_reason = wait_for_cleanup_marker(
                    session.original_context, session.deadline_monotonic)
                if not cleanup_ok:
                    session.exhaust(cleanup_reason)
                    return [1], [None]
                try:
                    session.record_cleanup_proof_completed()
                except RecoveryError as error:
                    session.exhaust(str(error))
                    return [1], [None]
                if not session.transition(
                        job_lib.JobSystemRecoveryPhase.WAITING_MEMORY,
                        'attempt cleanup was positively verified'):
                    session.exhaust('cannot persist WAITING_MEMORY')
                    return [1], [None]
                visibility_ok, visibility_reason = (
                    session.wait_for_first_event_visibility())
                if not visibility_ok:
                    session.exhaust(visibility_reason)
                    return [1], [None]
                memory_ok, memory_reason = wait_for_safe_memory(
                    ray_module,
                    session.deadline_monotonic,
                    profile_version=recovery_plan.profile_version)
                if not memory_ok:
                    session.exhaust(memory_reason)
                    return [1], [None]
                quiescence_ok, quiescence_reason = (
                    session.wait_for_replay_quiescence())
                if not quiescence_ok:
                    session.exhaust(quiescence_reason)
                    return [1], [None]
                healthy, health_reason = ray_runtime_is_healthy(
                    ray_module,
                    ray_util_module,
                    placement_group,
                    str(session.original_context['node_boot_id']),
                    session.expected_ray_session_identity,
                    profile_version=recovery_plan.profile_version)
                if not healthy:
                    session.exhaust(health_reason)
                    return [1], [None]
                if not session.submit_one_retry(ray_module):
                    return [1], [None]
                continue
            except Exception as e:  # pylint: disable=broad-except
                if not session.is_replacement:
                    if session.arm_state == RecoveryArmState.PENDING:
                        session.disable_arm(
                            'original Ray task failed before recovery armed',
                            admission_reason='task_failed_before_arm')
                    raise
                session.exhaust(f'replacement Ray task failed: {e}')
                return [1], [None]

            if session.arm_state == RecoveryArmState.PENDING:
                session.disable_arm(
                    'original Ray task completed before recovery armed',
                    admission_reason='task_completed_before_arm')
            if not isinstance(result, dict):
                if not session.is_replacement:
                    raise RecoveryError('Ray task returned an invalid result.')
                session.exhaust('replacement returned a non-object result')
                return [1], [None]
            returncode = 1
            raw_pid: Any = None
            try:
                returncode = int(result['return_code'])
                raw_pid = result.get('pid')
            except (KeyError, TypeError, ValueError) as e:
                if not session.is_replacement:
                    raise RecoveryError(
                        'Ray task returned an invalid result.') from e
                session.exhaust(f'replacement returned an invalid result: {e}')
                return [1], [None]
            if session.is_replacement and returncode != 0:
                session.exhaust(f'replacement exited with {returncode}')
            pid = raw_pid if isinstance(raw_pid, int) else None
            return [returncode], [pid]
    finally:
        ray_util_module.remove_placement_group(placement_group)
        sys.stdout.flush()
