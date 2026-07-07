"""SSM ProxyCommand adaptive-retry wrapping must stay exec-safe.

OpenSSH executes ProxyCommand as `$SHELL -c "exec <command>"`. A
multi-command string (`export ...; aws ...`) makes the shell exec the
`export` builtin and fail hard, killing every proxied SSH — the serve
controller then classifies healthy just-launched replicas as preempted
(observed live 2026-07-06: `/bin/sh: 1: exec: export: not found`).
"""
import shlex
import subprocess

from sky.backends import backend_utils


def _upgrade(cmd):
    return backend_utils._upgrade_legacy_ssm_proxy_command(cmd)  # pylint: disable=protected-access


SSM_CMD = ('aws ssm start-session --target '
           '"$(aws ec2 describe-instances --region us-east-2 '
           '--filters Name=ip-address,Values=%h '
           '--query "Reservations[].Instances[].InstanceId" '
           '--profile p --output text)" --region us-east-2 --profile p '
           '--document-name AWS-StartSSHSession --parameters portNumber=%p')


def test_plain_ssm_command_gets_wrapped():
    wrapped = _upgrade(SSM_CMD)
    assert wrapped.startswith('env AWS_RETRY_MODE=adaptive')
    assert shlex.quote(SSM_CMD) in wrapped


def test_wrapped_form_is_a_single_exec_able_command():
    # The first word must be an executable (env), not a shell builtin:
    # OpenSSH will prepend `exec` to the whole string.
    wrapped = _upgrade(SSM_CMD)
    first_word = wrapped.split()[0]
    assert first_word == 'env'
    # `exec <wrapped>` must parse and run under dash/sh semantics. Use a
    # harmless probe: replace the wrapped inner command so nothing real
    # runs, but keep the exact wrapper shape.
    probe = backend_utils._wrap_ssm_proxy_command_with_adaptive_retry(  # pylint: disable=protected-access
        'printenv AWS_RETRY_MODE && printf inner-ok')
    result = subprocess.run(['/bin/sh', '-c', f'exec {probe}'],
                            capture_output=True,
                            text=True,
                            check=False)
    assert result.returncode == 0, result.stderr
    assert 'adaptive' in result.stdout
    assert 'inner-ok' in result.stdout


def test_broken_export_prefixed_form_is_repaired():
    broken = ('export AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12; '
              f'{SSM_CMD}')
    repaired = _upgrade(broken)
    assert repaired.startswith('env AWS_RETRY_MODE=adaptive')
    assert not repaired.startswith('export')
    assert shlex.quote(SSM_CMD) in repaired


def test_upgrade_is_idempotent():
    wrapped = _upgrade(SSM_CMD)
    assert _upgrade(wrapped) == wrapped


def test_non_ssm_commands_untouched():
    assert _upgrade(None) is None
    assert _upgrade('ssh -W %h:%p jump-host') == 'ssh -W %h:%p jump-host'
