"""AWS SSM ProxyCommand compatibility policy."""

import re
import shlex

# Adaptive-retry settings for the SSM ProxyCommand: make the AWS CLI wait
# out StartSession throttling (low account-wide TPS quota) with client-side
# rate limiting instead of failing the SSH connection.
_SSM_ADAPTIVE_RETRY_ENV = 'AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12'
_SSM_ADAPTIVE_RETRY_WRAPPER_PREFIX = (
    f'env {_SSM_ADAPTIVE_RETRY_ENV} /bin/sh -c ')
_SSM_TARGET_VARIABLE = 'skypilot_ssm_target'
_SSM_TARGET_NOT_FOUND_MESSAGE = (
    'SkyPilot SSM target instance not found for SSH host %h')
_SSM_LEGACY_TARGET_NOT_FOUND_PRINTF = "printf '%s\\n'"
# ProxyCommand percent tokens are expanded by OpenSSH before the shell runs.
# Escape printf's literal percent so OpenSSH passes ``%s`` to /bin/sh.
_SSM_TARGET_NOT_FOUND_PRINTF = "printf '%%s\\n'"
_SSM_START_SESSION_WITH_LOOKUP_PATTERN = re.compile(
    r'^aws ssm start-session --target "\$\((?P<lookup>.*)\)" '
    r'(?P<arguments>.*)$', re.DOTALL)
# The broken prefix form shipped briefly: OpenSSH executes ProxyCommand as
# `$SHELL -c "exec <command>"`, so a multi-command string starting with
# `export ...;` makes the shell try to exec the `export` builtin and fail
# hard (`/bin/sh: 1: exec: export: not found`) — EVERY proxied SSH dies,
# and the serve controller then classifies healthy just-launched replicas
# as preempted (job-status walk + forced refresh both SSH). Kept only to
# recognize and repair commands persisted in that form.
_SSM_LEGACY_BROKEN_EXPORT_PREFIX = ('export AWS_RETRY_MODE=adaptive '
                                    'AWS_MAX_ATTEMPTS=12;')


def _guard_ssm_proxy_command_target(ssm_proxy_command: str) -> str:
    """Avoid invoking SSM StartSession with an empty EC2 lookup result.

    A stale cluster record can outlive its EC2 instance.  The generated
    ProxyCommand resolves the instance ID from ``%h`` at connection time; if
    that lookup returns no rows, passing an empty ``--target`` to the AWS CLI
    emits a misleading parameter-validation error.  Preserve a failed lookup's
    exit status, and classify a successful empty lookup explicitly before
    StartSession is invoked.
    """
    if f'{_SSM_TARGET_VARIABLE}=' in ssm_proxy_command:
        # Repair guarded commands persisted by releases that emitted an
        # unescaped printf format.  OpenSSH rejects ``%s`` as an unknown
        # ProxyCommand token before executing the otherwise healthy command.
        return ssm_proxy_command.replace(_SSM_LEGACY_TARGET_NOT_FOUND_PRINTF,
                                         _SSM_TARGET_NOT_FOUND_PRINTF)
    match = _SSM_START_SESSION_WITH_LOOKUP_PATTERN.fullmatch(ssm_proxy_command)
    if match is None:
        return ssm_proxy_command
    lookup = match.group('lookup')
    arguments = match.group('arguments')
    message = shlex.quote(_SSM_TARGET_NOT_FOUND_MESSAGE)
    return (f'{_SSM_TARGET_VARIABLE}="$({lookup})"; '
            f'skypilot_ssm_lookup_status=$?; '
            'if [ "$skypilot_ssm_lookup_status" -ne 0 ]; then '
            'exit "$skypilot_ssm_lookup_status"; fi; '
            f'if [ -z "${_SSM_TARGET_VARIABLE}" ]; then '
            f'{_SSM_TARGET_NOT_FOUND_PRINTF} {message} >&2; '
            'exit 255; fi; '
            'exec aws ssm start-session '
            f'--target "${_SSM_TARGET_VARIABLE}" {arguments}')


def _wrap_ssm_proxy_command_with_adaptive_retry(ssm_proxy_command: str) -> str:
    """exec-safe adaptive-retry wrapper for an SSM ProxyCommand.

    `env VAR=... /bin/sh -c '<cmd>'` stays a single exec-able command under
    OpenSSH's `exec` wrapping, and the variables still reach the
    describe-instances $() command substitution because the inner shell
    inherits them (verified live; an `export ...;` prefix instead kills the
    connection before any handshake).
    """
    return (_SSM_ADAPTIVE_RETRY_WRAPPER_PREFIX + shlex.quote(ssm_proxy_command))


def _upgrade_legacy_ssm_proxy_command(
        ssh_proxy_command: str | None) -> str | None:
    """Normalize persisted SSM proxy commands to the adaptive-retry form.

    Clusters keep their originally-written auth section forever: on
    re-provision it is restored verbatim from the old YAML (see
    _RAY_YAML_KEYS_TO_RESTORE_FOR_BACK_COMPATIBILITY). Applied in every
    credential read path so (1) pre-retry-prefix clusters also wait out
    StartSession throttling and (2) commands persisted with the broken
    `export ...;` prefix are repaired instead of failing every SSH, and
    (3) both forms reject an empty instance lookup before StartSession.
    """
    if ssh_proxy_command is None:
        return None
    if ssh_proxy_command.startswith(_SSM_LEGACY_BROKEN_EXPORT_PREFIX):
        stripped = ssh_proxy_command[len(_SSM_LEGACY_BROKEN_EXPORT_PREFIX
                                        ):].lstrip()
        guarded = _guard_ssm_proxy_command_target(stripped)
        return _wrap_ssm_proxy_command_with_adaptive_retry(guarded)
    if (ssh_proxy_command.startswith('aws ssm start-session') and
            'AWS_RETRY_MODE' not in ssh_proxy_command):
        guarded = _guard_ssm_proxy_command_target(ssh_proxy_command)
        return _wrap_ssm_proxy_command_with_adaptive_retry(guarded)
    if ssh_proxy_command.startswith(_SSM_ADAPTIVE_RETRY_WRAPPER_PREFIX):
        quoted_inner = ssh_proxy_command[len(_SSM_ADAPTIVE_RETRY_WRAPPER_PREFIX
                                            ):]
        try:
            parsed_inner = shlex.split(quoted_inner)
        except ValueError:
            return ssh_proxy_command
        if len(parsed_inner) != 1:
            return ssh_proxy_command
        guarded = _guard_ssm_proxy_command_target(parsed_inner[0])
        if guarded != parsed_inner[0]:
            return _wrap_ssm_proxy_command_with_adaptive_retry(guarded)
    return ssh_proxy_command


# Keep historical identities for the backend_utils facade and private pickles.
_guard_ssm_proxy_command_target.__module__ = 'sky.backends.backend_utils'
_wrap_ssm_proxy_command_with_adaptive_retry.__module__ = (
    'sky.backends.backend_utils')
_upgrade_legacy_ssm_proxy_command.__module__ = 'sky.backends.backend_utils'
