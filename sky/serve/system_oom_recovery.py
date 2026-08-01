"""Server-owned authorization for SkyServe system-OOM recovery."""

import copy
import dataclasses
import hashlib
import json
import os
import re
from typing import Any

from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky import task_yaml as task_yaml_lib
from sky.serve import constants
from sky.skylet import system_oom_recovery as runtime_recovery

logger = sky_logging.init_logger(__name__)

_SHA256_DIGEST_PATTERN = re.compile(r'^sha256:[0-9a-f]{64}$')

# Deprecated profile-v1 parser.  The already-authored stacked removal change
# deletes this table and scanner after all numbered migration gates in the
# canonical design pass.
_DOCKER_BOOLEAN_OPTIONS = frozenset({
    'detach', 'disable-content-trust', 'help', 'init', 'interactive',
    'oom-kill-disable', 'privileged', 'publish-all', 'quiet', 'read-only',
    'remove', 'rm', 'sig-proxy', 'tty'
})
_DOCKER_VALUE_OPTIONS = frozenset({
    'add-host', 'annotation', 'attach', 'blkio-weight', 'blkio-weight-device',
    'cap-add', 'cap-drop', 'cgroup-parent', 'cgroupns', 'cidfile', 'cpu-period',
    'cpu-quota', 'cpu-rt-period', 'cpu-rt-runtime', 'cpu-shares', 'cpus',
    'cpuset-cpus', 'cpuset-mems', 'device', 'device-cgroup-rule',
    'device-read-bps', 'device-read-iops', 'device-write-bps',
    'device-write-iops', 'dns', 'dns-option', 'dns-search', 'domainname',
    'entrypoint', 'env', 'env-file', 'expose', 'gpus', 'group-add',
    'health-cmd', 'health-interval', 'health-retries', 'health-start-interval',
    'health-start-period', 'health-timeout', 'hostname', 'init-path',
    'io-maxbandwidth', 'io-maxiops', 'ip', 'ip6', 'ipc', 'isolation',
    'kernel-memory', 'label', 'label-file', 'link', 'link-local-ip',
    'log-driver', 'log-opt', 'mac-address', 'memory', 'memory-reservation',
    'memory-swap', 'memory-swappiness', 'mount', 'name', 'network',
    'network-alias', 'oom-score-adj', 'pid', 'pids-limit', 'platform',
    'publish', 'pull', 'restart', 'runtime', 'security-opt', 'shm-size',
    'stop-signal', 'stop-timeout', 'storage-opt', 'sysctl', 'tmpfs', 'ulimit',
    'user', 'userns', 'uts', 'volume', 'volume-driver', 'volumes-from',
    'workdir'
})
_DOCKER_SHORT_BOOLEAN_OPTIONS = frozenset({'d', 'i', 'P', 't'})
_DOCKER_SHORT_VALUE_OPTIONS = frozenset(
    {'a', 'c', 'e', 'h', 'l', 'm', 'p', 'u', 'v', 'w'})
_SHELL_COMMAND_START_BOUNDARIES = frozenset({';', '&&', '||', '|', '(', '\n'})
_SHELL_COMMAND_END_BOUNDARIES = _SHELL_COMMAND_START_BOUNDARIES | {')'}
_RUNTIME_RESOURCE_KEYS = frozenset({
    'image_id', 'container_image', 'volumes', '_resolved_container_image',
    '_cluster_config_overrides', '_docker_login_config'
})


@dataclasses.dataclass(frozen=True)
class _ShellToken:
    value: str
    is_operator: bool = False


@dataclasses.dataclass(frozen=True)
class TrustedRecoveryProfile:
    """One operator-authorized service/task recovery profile."""

    profile_id: str
    profile_version: int
    workspace: str
    service_name: str
    service_hash: str
    task_digest: str
    runtime_image_digest: str
    owned_container_spec: runtime_recovery.OwnedContainerSpec | None = None

    def __post_init__(self) -> None:
        for field_name in ('profile_id', 'workspace', 'service_name',
                           'service_hash'):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f'{field_name} must be a nonempty string')
        if (not isinstance(self.task_digest, str) or
                re.fullmatch(r'[0-9a-f]{64}', self.task_digest) is None):
            raise ValueError('task_digest must be SHA-256')
        if (not isinstance(self.runtime_image_digest, str) or
                _SHA256_DIGEST_PATTERN.fullmatch(
                    self.runtime_image_digest) is None):
            raise ValueError('runtime_image_digest must be SHA-256')
        if (type(self.profile_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.profile_version
                not in (runtime_recovery.PROFILE_VERSION_DIRECT_SHELL,
                        runtime_recovery.PROFILE_VERSION_OWNED_CONTAINER)):
            raise ValueError('unsupported recovery profile version')
        if self.profile_version == runtime_recovery.PROFILE_VERSION_DIRECT_SHELL:
            if self.owned_container_spec is not None:
                raise ValueError('profile v1 cannot contain an owned spec')
        else:
            if self.owned_container_spec is None:
                raise ValueError('profile v2 requires an owned spec')
            _, separator, digest = self.owned_container_spec.image.rpartition(
                '@')
            if not separator or digest != self.runtime_image_digest:
                raise ValueError(
                    'owned spec image must match runtime_image_digest')

    @property
    def capability(self) -> str:
        return runtime_recovery.CAPABILITY_BY_PROFILE_VERSION[
            self.profile_version]

    def launch_plan(self) -> runtime_recovery.RecoveryLaunchPlan:
        if self.profile_version == runtime_recovery.PROFILE_VERSION_DIRECT_SHELL:
            return runtime_recovery.RecoveryLaunchPlan.direct_shell()
        assert self.owned_container_spec is not None
        return runtime_recovery.RecoveryLaunchPlan.owned_container(
            self.owned_container_spec)


@dataclasses.dataclass(frozen=True)
class RequestedRecoveryProfile:
    """Minimal server-owned profile identity persisted before launch."""

    profile_id: str
    profile_version: int

    def __post_init__(self) -> None:
        if not isinstance(self.profile_id, str) or not self.profile_id:
            raise ValueError('profile_id must be a nonempty string')
        if (type(self.profile_version) is not int or  # pylint: disable=unidiomatic-typecheck
                self.profile_version
                not in (runtime_recovery.PROFILE_VERSION_DIRECT_SHELL,
                        runtime_recovery.PROFILE_VERSION_OWNED_CONTAINER)):
            raise ValueError('unsupported recovery profile version')

    @classmethod
    def from_profile(
            cls, profile: TrustedRecoveryProfile) -> 'RequestedRecoveryProfile':
        return cls(profile_id=profile.profile_id,
                   profile_version=profile.profile_version)


def _canonical_json(value: Any) -> str:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=True)


def _runtime_resource_identity(resources: Any) -> Any:
    """Keep process-ownership fields while ignoring placement-only fields."""
    if isinstance(resources, list):
        identities = [_runtime_resource_identity(item) for item in resources]
        return sorted(identities, key=_canonical_json)
    if not isinstance(resources, dict):
        return resources
    identity: dict[str, Any] = {}
    for key, value in resources.items():
        if key in ('any_of', 'ordered'):
            identity[key] = _runtime_resource_identity(value)
        elif key == '_resolved_container_image' and isinstance(value, dict):
            identity[key] = {'digest': value.get('digest')}
        elif key in _RUNTIME_RESOURCE_KEYS:
            identity[key] = value
    return identity


def _command_text(command: Any) -> str | None:
    if isinstance(command, str):
        return command
    if (isinstance(command, (list, tuple)) and
            all(isinstance(item, str) for item in command)):
        return '\n'.join(command)
    return None


def _uses_generic_container_image(task: task_lib.Task) -> bool:
    """Whether SkyPilot's persistent outer-container path is requested."""
    for resource in task.resources:
        if (resource.container_image is not None or
                resource.resolved_container_image is not None):
            return True
    return False


def _docker_run_image_digest(tokens: list[str]) -> str | None:
    """Parse one conservative Docker CLI form for deprecated profile v1."""
    if len(tokens) < 3 or tokens[0] != 'docker':
        return None
    index = 1
    if tokens[index] == 'container':
        index += 1
    if index >= len(tokens) or tokens[index] != 'run':
        return None
    index += 1
    while index < len(tokens):
        token = tokens[index]
        if token == '--':
            index += 1
            break
        if token.startswith('--'):
            name_value = token[2:].split('=', 1)
            name = name_value[0]
            if name in _DOCKER_BOOLEAN_OPTIONS:
                if (len(name_value) == 2 and
                        name_value[1].lower() not in ('true', 'false')):
                    return None
                index += 1
                continue
            if name not in _DOCKER_VALUE_OPTIONS:
                return None
            if len(name_value) == 2:
                if not name_value[1]:
                    return None
                index += 1
                continue
            index += 2
            if index > len(tokens):
                return None
            continue
        if token.startswith('-') and token != '-':
            short_options = token[1:]
            if all(option in _DOCKER_SHORT_BOOLEAN_OPTIONS
                   for option in short_options):
                index += 1
                continue
            first = short_options[0]
            if first not in _DOCKER_SHORT_VALUE_OPTIONS:
                return None
            if len(short_options) == 1:
                index += 2
                if index > len(tokens):
                    return None
            else:
                index += 1
            continue
        break
    if index >= len(tokens):
        return None
    image_match = re.fullmatch(r'[^@\s]+@(?P<digest>sha256:[0-9a-f]{64})',
                               tokens[index])
    return None if image_match is None else image_match.group('digest')


def _tokenize_shell(command: str) -> list[_ShellToken] | None:
    """Tokenize real shell boundaries while retaining quote provenance.

    ``shlex`` returns the same string for a real semicolon and for ``";"`` or
    ``\\;``.  The distinction is authorization-relevant here: the latter two
    are ordinary arguments and cannot prove that a following Docker token is
    an executable.  This small closed lexer preserves that distinction and
    fails on unterminated quotes or escapes.
    """
    tokens: list[_ShellToken] = []
    word: list[str] = []
    word_started = False

    def _flush_word() -> None:
        nonlocal word_started
        if word_started:
            tokens.append(_ShellToken(''.join(word)))
            word.clear()
            word_started = False

    index = 0
    while index < len(command):
        character = command[index]
        if character in ' \t\r':
            _flush_word()
            index += 1
            continue
        if character == '\n':
            _flush_word()
            tokens.append(_ShellToken('\n', is_operator=True))
            index += 1
            continue
        if character == '#' and not word_started:
            # Shell comments extend to, but do not consume, the newline.
            newline = command.find('\n', index + 1)
            index = len(command) if newline == -1 else newline
            continue
        if character == '\\':
            if index + 1 >= len(command):
                return None
            if command[index + 1] == '\n':
                index += 2
                continue
            word_started = True
            word.append(command[index + 1])
            index += 2
            continue
        if (character == '<' and index + 1 < len(command) and
                command[index + 1] == '<'):
            # Heredoc bodies require a full shell grammar to distinguish data
            # from executable tokens.  The deprecated scanner deliberately
            # rejects them instead of inferring across that boundary.
            return None
        if character in ("'", '"'):
            quote = character
            word_started = True
            index += 1
            while index < len(command) and command[index] != quote:
                if (quote == '"' and command[index] == '\\' and
                        index + 1 < len(command)):
                    escaped = command[index + 1]
                    if escaped == '\n':
                        index += 2
                        continue
                    if escaped not in ('$', '`', '"', '\\'):
                        # Bash preserves this backslash inside double quotes.
                        word.append('\\')
                    word.append(escaped)
                    index += 2
                else:
                    word.append(command[index])
                    index += 1
            if index >= len(command):
                return None
            index += 1
            continue
        if character in ';&|()':
            _flush_word()
            operator = character
            if (character in '&|' and index + 1 < len(command) and
                    command[index + 1] == character):
                operator += character
                index += 1
            tokens.append(_ShellToken(operator, is_operator=True))
            index += 1
            continue
        word_started = True
        word.append(character)
        index += 1
    _flush_word()
    return tokens


def _is_docker_run_start(tokens: list[_ShellToken], index: int) -> bool:
    if (tokens[index].is_operator or tokens[index].value != 'docker' or
            index + 1 >= len(tokens)):
        return False
    if not tokens[index + 1].is_operator and tokens[index + 1].value == 'run':
        return True
    return (index + 2 < len(tokens) and not tokens[index + 1].is_operator and
            tokens[index + 1].value == 'container' and
            not tokens[index + 2].is_operator and
            tokens[index + 2].value == 'run')


def _is_closed_docker_command_start(tokens: list[_ShellToken],
                                    index: int) -> bool:
    """Require literal Docker at a real boundary with only known prefixes."""
    command_start = index
    if (command_start > 0 and not tokens[command_start - 1].is_operator and
            tokens[command_start - 1].value == 'sudo'):
        command_start -= 1
    if (command_start > 0 and not tokens[command_start - 1].is_operator and
            tokens[command_start - 1].value == 'exec'):
        command_start -= 1
    if command_start == 0:
        return True
    prior = tokens[command_start - 1]
    return (prior.is_operator and
            prior.value in _SHELL_COMMAND_START_BOUNDARIES)


def runtime_image_digest(task: task_lib.Task) -> str | None:
    """Return one direct-Docker immutable image digest, or fail closed."""
    if _uses_generic_container_image(task):
        return None
    command = _command_text(task.run)
    if command is None:
        return None
    command = command.replace('\\\n', ' ')
    tokens = _tokenize_shell(command)
    if tokens is None:
        return None
    docker_run_starts = [
        index for index in range(len(tokens))
        if _is_docker_run_start(tokens, index)
    ]
    if (not docker_run_starts or
            any(not _is_closed_docker_command_start(tokens, index)
                for index in docker_run_starts)):
        return None
    digests = set()
    for start in docker_run_starts:
        end = start + 1
        while (end < len(tokens) and
               not (tokens[end].is_operator and
                    tokens[end].value in _SHELL_COMMAND_END_BOUNDARIES)):
            end += 1
        digest = _docker_run_image_digest(
            [token.value for token in tokens[start:end]])
        if digest is None:
            return None
        digests.add(digest)
    return next(iter(digests)) if len(digests) == 1 else None


def safety_profile_digest(task: task_lib.Task) -> str:
    """Return a canonical digest of effective process-ownership fields."""
    config = copy.deepcopy(
        task_yaml_lib.to_yaml_config(task, redact_secrets=True))
    config.pop('name', None)
    config.pop('service', None)
    config.pop('_user_specified_yaml', None)
    envs = config.get('envs')
    if isinstance(envs, dict) and constants.REPLICA_ID_ENV_VAR in envs:
        replica_id = envs[constants.REPLICA_ID_ENV_VAR]
        if (not isinstance(replica_id, str) or not replica_id.isdecimal() or
                int(replica_id) < 0):
            envs[constants.REPLICA_ID_ENV_VAR] = '<invalid-replica-id>'
        else:
            envs[constants.REPLICA_ID_ENV_VAR] = '<server-replica-id>'
    resources = config.pop('resources', None)
    config['runtime_resources'] = {
        'submitted': _runtime_resource_identity(resources),
        'resolved_image_digest': runtime_image_digest(task),
    }
    return hashlib.sha256(_canonical_json(config).encode('utf-8')).hexdigest()


def _profile_from_dict(value: object,
                       profile_version: int) -> TrustedRecoveryProfile:
    if not isinstance(value, dict):
        raise ValueError('profile must be an object')
    common_fields = {
        'profile_id', 'workspace', 'service_name', 'service_hash',
        'task_digest', 'runtime_image_digest'
    }
    if profile_version == runtime_recovery.PROFILE_VERSION_DIRECT_SHELL:
        if set(value) != common_fields:
            raise ValueError('profile v1 has invalid fields')
        owned_spec = None
    elif profile_version == runtime_recovery.PROFILE_VERSION_OWNED_CONTAINER:
        if set(value) != common_fields | {'owned_container_spec'}:
            raise ValueError('profile v2 has invalid fields')
        owned_spec = runtime_recovery.OwnedContainerSpec.from_dict(
            value['owned_container_spec'])
    else:
        raise ValueError('unsupported profile version')
    return TrustedRecoveryProfile(
        profile_id=value['profile_id'],
        profile_version=profile_version,
        workspace=value['workspace'],
        service_name=value['service_name'],
        service_hash=value['service_hash'],
        task_digest=value['task_digest'],
        runtime_image_digest=value['runtime_image_digest'],
        owned_container_spec=owned_spec)


def _load_profiles() -> tuple[TrustedRecoveryProfile, ...]:
    raw = os.environ.get(constants.SYSTEM_OOM_RECOVERY_PROFILES_ENV_VAR)
    if not raw:
        return ()
    try:
        document = json.loads(raw)
        if (not isinstance(document, dict) or
                set(document) != {'version', 'profiles'} or
                type(document['version']) is not int or  # pylint: disable=unidiomatic-typecheck
                document['version']
                not in (runtime_recovery.PROFILE_VERSION_DIRECT_SHELL,
                        runtime_recovery.PROFILE_VERSION_OWNED_CONTAINER)):
            raise ValueError('profile document has invalid fields or version')
        raw_profiles = document['profiles']
        if not isinstance(raw_profiles, list):
            raise ValueError('profiles must be a list')
        profiles = tuple(
            _profile_from_dict(value, document['version'])
            for value in raw_profiles)
        profile_ids = [profile.profile_id for profile in profiles]
        if len(profile_ids) != len(set(profile_ids)):
            raise ValueError('profile IDs must be unique')
        return profiles
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        logger.warning('Ignoring invalid internal system-OOM recovery profile '
                       f'document: {error}')
        return ()


def _resolve_profile(
        task: task_lib.Task,
        service_name: object,
        service_hash: object,
        requested_profile_id: object = None,
        requested_profile_version: object = None
) -> TrustedRecoveryProfile | None:
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(service_hash, str) or not service_hash or
            task.managed_secret_refs or _uses_generic_container_image(task)):
        return None
    image_digest = runtime_image_digest(task)
    if image_digest is None:
        return None
    task_digest = safety_profile_digest(task)
    workspace = skypilot_config.get_active_workspace()
    for profile in _load_profiles():
        if (profile.workspace != workspace or
                profile.service_name != service_name or
                profile.service_hash != service_hash or
                profile.task_digest != task_digest or
                profile.runtime_image_digest != image_digest):
            continue
        if (requested_profile_id is not None and
                profile.profile_id != requested_profile_id):
            continue
        if (requested_profile_version is not None and
                profile.profile_version != requested_profile_version):
            continue
        if profile.profile_version == (
                runtime_recovery.PROFILE_VERSION_OWNED_CONTAINER):
            assert profile.owned_container_spec is not None
            if task.run != profile.owned_container_spec.render():
                continue
        return profile
    return None


def resolve_requested_profile(
        task: task_lib.Task, *, service_name: object,
        service_hash: object) -> RequestedRecoveryProfile | None:
    """Resolve the profile identity a controller must persist before launch.

    This does not confer runtime authority: it neither accepts provisioning
    evidence nor creates a launch plan.  The backend independently rematches
    the effective post-policy task after receiving the exact persisted
    contract tuple.
    """
    profile = _resolve_profile(task, service_name, service_hash)
    return (None if profile is None else
            RequestedRecoveryProfile.from_profile(profile))


def match_trusted_profile(
        task: task_lib.Task,
        launch_context: dict[str, Any] | None) -> TrustedRecoveryProfile | None:
    """Match an exact owner-fenced controller contract and effective task."""
    if launch_context is None:
        return None
    contract_version = launch_context.get(
        constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION_KEY)
    # Booleans compare equal to integers in Python but are not contract
    # versions.  Require the exact built-in integer representation.
    if (type(contract_version) is not int or  # pylint: disable=unidiomatic-typecheck
            contract_version
            != constants.SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION):
        return None
    profile_id = launch_context.get(
        constants.SYSTEM_OOM_RECOVERY_PROFILE_ID_KEY)
    profile_version = launch_context.get(
        constants.SYSTEM_OOM_RECOVERY_PROFILE_VERSION_KEY)
    if (not isinstance(profile_id, str) or not profile_id or
            type(profile_version) is not int):  # pylint: disable=unidiomatic-typecheck
        return None
    service_name = launch_context.get(
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
    service_hash = launch_context.get(
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
    return _resolve_profile(task,
                            service_name,
                            service_hash,
                            requested_profile_id=profile_id,
                            requested_profile_version=profile_version)
