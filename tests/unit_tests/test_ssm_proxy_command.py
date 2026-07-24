"""SSM ProxyCommand adaptive-retry wrapping must stay exec-safe.

OpenSSH executes ProxyCommand as `$SHELL -c "exec <command>"`. A
multi-command string (`export ...; aws ...`) makes the shell exec the
`export` builtin and fail hard, killing every proxied SSH — the serve
controller then classifies healthy just-launched replicas as preempted
(observed live 2026-07-06: `/bin/sh: 1: exec: export: not found`).
"""
import subprocess

from sky.backends import backend_utils
from sky.utils import cluster_utils
from sky.utils import command_runner


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
    assert 'skypilot_ssm_target=' in wrapped
    assert '%%s\\n' in wrapped
    assert backend_utils._SSM_TARGET_NOT_FOUND_MESSAGE in wrapped  # pylint: disable=protected-access


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
    assert 'skypilot_ssm_target=' in repaired


def test_existing_wrapped_form_gets_empty_target_guard():
    wrapped = backend_utils._wrap_ssm_proxy_command_with_adaptive_retry(  # pylint: disable=protected-access
        SSM_CMD)
    upgraded = _upgrade(wrapped)
    assert upgraded.startswith('env AWS_RETRY_MODE=adaptive')
    assert 'skypilot_ssm_target=' in upgraded
    assert backend_utils._SSM_TARGET_NOT_FOUND_MESSAGE in upgraded  # pylint: disable=protected-access


def test_existing_guarded_form_repairs_openssh_percent_escape():
    fixed = _upgrade(SSM_CMD)
    broken = fixed.replace('%%s\\n', '%s\\n')
    assert broken != fixed
    assert _upgrade(broken) == fixed


def test_docker_hop_preserves_inner_openssh_percent_escape():
    wrapped = _upgrade(SSM_CMD)
    runner = command_runner.SSHCommandRunner(node=('203.0.113.1', 22),
                                             ssh_user='ubuntu',
                                             ssh_private_key=None,
                                             ssh_control_name=None,
                                             ssh_proxy_command=wrapped,
                                             docker_user='root')
    command = runner.ssh_base_command(
        ssh_mode=command_runner.SshMode.NON_INTERACTIVE,
        port_forward=None,
        connect_timeout=1)
    proxy_option = next(
        arg for arg in command if arg.startswith('ProxyCommand='))
    # Outer OpenSSH turns ``%%%%s`` into ``%%s`` before invoking the nested
    # SSH command; inner OpenSSH then passes the intended ``%s`` to printf.
    assert '%%%%s\\n' in proxy_option
    assert 'Values=203.0.113.1' in proxy_option
    assert 'portNumber=22' in proxy_option


def test_docker_ssh_config_preserves_inner_openssh_percent_escape(
        tmp_path, monkeypatch):
    raw = ("aws ssm start-session --target \"$(printf '')\" "
           '--region us-east-2 --document-name AWS-StartSSHSession '
           '--parameters portNumber=%p')
    wrapped = _upgrade(raw)
    ssh_config_path = tmp_path / 'ssh' / 'config'
    ssh_cluster_path = str(tmp_path / 'sky' / 'ssh' / '{}')
    monkeypatch.setattr(cluster_utils.SSHConfigHelper, 'ssh_conf_path',
                        str(ssh_config_path))
    monkeypatch.setattr(cluster_utils.SSHConfigHelper, 'ssh_cluster_path',
                        ssh_cluster_path)
    monkeypatch.setattr(cluster_utils.common_utils, 'is_wsl', lambda: False)

    # Bypass the decorator's process-wide SSH config lock. All paths written by
    # the underlying method are isolated under tmp_path.
    cluster_utils.SSHConfigHelper.add_cluster.__wrapped__(
        cluster_utils.SSHConfigHelper,
        cluster_name='nested-test',
        cluster_name_on_cloud='nested-test',
        ips=['203.0.113.1'],
        auth_config={
            'ssh_user': 'ubuntu',
            'ssh_private_key': str(tmp_path / 'key'),
            'ssh_proxy_command': wrapped,
        },
        ports=[22],
        docker_user='root')

    generated = (tmp_path / 'sky' / 'ssh' / 'nested-test').read_text()
    # Outer OpenSSH turns ``%%%%s`` into ``%%s`` before invoking the nested
    # SSH command; inner OpenSSH then passes the intended ``%s`` to printf.
    assert '%%%%s\\n' in generated
    result = subprocess.run(
        ['ssh', '-F', str(ssh_config_path), 'nested-test'],
        capture_output=True,
        text=True,
        check=False,
        timeout=5)
    assert result.returncode == 255
    assert backend_utils._SSM_TARGET_NOT_FOUND_MESSAGE.replace(  # pylint: disable=protected-access
        '%h', '203.0.113.1') in result.stderr
    assert 'unknown key %s' not in result.stderr


def test_empty_target_fails_before_start_session():
    raw = ("aws ssm start-session --target \"$(printf '')\" "
           '--region us-east-2 --document-name AWS-StartSSHSession '
           '--parameters portNumber=%p')
    upgraded = _upgrade(raw)
    # Exercise OpenSSH's real ProxyCommand token expansion.  A literal ``%s``
    # is rejected before the proxy shell starts; ``%%s`` reaches printf as the
    # intended format string.  The empty lookup exits before any network call.
    result = subprocess.run([
        'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=1', '-o',
        f'ProxyCommand={upgraded}', '203.0.113.1'
    ],
                            capture_output=True,
                            text=True,
                            check=False)
    assert result.returncode == 255
    assert backend_utils._SSM_TARGET_NOT_FOUND_MESSAGE.replace(  # pylint: disable=protected-access
        '%h', '203.0.113.1') in result.stderr
    assert 'Invalid length for parameter Target' not in result.stderr


def test_lookup_failure_exit_status_is_preserved():
    raw = ('aws ssm start-session --target "$(false)" '
           '--region us-east-2 --document-name AWS-StartSSHSession '
           '--parameters portNumber=22')
    result = subprocess.run(['/bin/sh', '-c', f'exec {_upgrade(raw)}'],
                            capture_output=True,
                            text=True,
                            check=False)
    assert result.returncode == 1
    assert backend_utils._SSM_TARGET_NOT_FOUND_MESSAGE not in result.stderr  # pylint: disable=protected-access


def test_upgrade_is_idempotent():
    wrapped = _upgrade(SSM_CMD)
    assert _upgrade(wrapped) == wrapped


def test_non_ssm_commands_untouched():
    assert _upgrade(None) is None
    assert _upgrade('ssh -W %h:%p jump-host') == 'ssh -W %h:%p jump-host'
