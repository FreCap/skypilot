"""Unit tests for SSH node pool deployment."""
# pylint: disable=protected-access

import pickle

from sky.ssh_node_pools.deploy import deploy


def _run_single_cluster(monkeypatch, tmp_path, *, monitoring_failures=False):
    remote_calls = []
    local_calls = []

    def fake_run_remote(node, command, user, ssh_key, **kwargs):
        del node, user, ssh_key
        remote_calls.append((command, kwargs))
        if "echo 'SSH connection successful" in command:
            return 'SSH connection successful (head)'
        if "awk '{for(i=1;i<=NF" in command:
            return '10.0.0.1'
        if monitoring_failures and ('helm install gpu-operator' in command or
                                    'DCGM_DS=' in command or
                                    'helm upgrade --install skypilot-prometheus'
                                    in command):
            return None
        if 'DCGM_DS=' in command:
            return 'app: dcgm-exporter'
        return 'ok'

    def fake_run_command(command, **kwargs):
        local_calls.append((command, kwargs))
        if command[0] == 'scp':
            with open(command[-1], 'w', encoding='utf-8') as file:
                file.write('''apiVersion: v1
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: default
users:
- name: default
  user: {}
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
''')
        if command[:4] == ['kubectl', 'config', 'view', '--flatten']:
            return 'apiVersion: v1\n'
        return ''

    monkeypatch.setattr(deploy.deploy_utils, 'run_remote', fake_run_remote)
    monkeypatch.setattr(deploy.deploy_utils, 'run_command', fake_run_command)
    monkeypatch.setattr(deploy.deploy_utils, 'check_gpu',
                        lambda *args, **kwargs: True)
    monkeypatch.setattr(deploy.deploy_utils, 'ensure_directory_exists',
                        lambda path: None)
    monkeypatch.setattr(deploy.tunnel_utils, 'setup_kubectl_ssh_tunnel',
                        lambda *args, **kwargs: None)

    kubeconfig_path = str(tmp_path / 'kubeconfig')
    result = deploy.deploy_single_cluster(cluster_name='test-pool',
                                          head_node='head',
                                          worker_nodes=[],
                                          ssh_user='user',
                                          ssh_key='key',
                                          context_name='ssh-test-pool',
                                          password=None,
                                          head_use_ssh_config=False,
                                          worker_use_ssh_config=[],
                                          kubeconfig_path=kubeconfig_path,
                                          cleanup=False,
                                          worker_hosts=[])
    return result, remote_calls, local_calls


def test_prometheus_install_cmd_contains_required_fields():
    askpass_block = 'echo "askpass"'
    cmd = deploy._prometheus_install_cmd(askpass_block)

    # Must include the askpass block verbatim (consistent with sibling helpers).
    assert askpass_block in cmd

    # Must self-install helm if missing — the gpu-operator path installs
    # helm for GPU pools, but CPU-only pools skip that step.
    assert 'command -v helm' in cmd
    assert 'get-helm-3' in cmd

    # Must use the prometheus-community repo and the plain prometheus chart
    # (NOT kube-prometheus-stack — see spec "Do NOT use kube-prometheus-stack").
    assert 'prometheus-community' in cmd
    assert 'prometheus-community/prometheus' in cmd
    assert 'kube-prometheus-stack' not in cmd

    # Repo-scoped update is cheaper than a global `helm repo update`.
    assert 'helm repo update prometheus-community' in cmd

    # Must be idempotent (upgrade --install).
    assert 'helm upgrade --install' in cmd

    # Must target the correct kubeconfig on the remote head node.
    assert '--kubeconfig ~/.kube/config' in cmd
    assert '--namespace skypilot' in cmd
    assert '--create-namespace' in cmd

    # Release name hardcoded.
    assert 'skypilot-prometheus' in cmd

    # Must NOT pass --kube-context. The command runs on the pool's head node,
    # where `~/.kube/config` only has the default context k3s wrote — any
    # `ssh-<pool>` context name only exists in the client's merged kubeconfig.
    # The sibling `_dcgm_exporter_service_cmd` correctly omits it.
    assert '--kube-context' not in cmd

    # Values file must be created via mktemp so concurrent pool deploys don't
    # race on a shared path.
    assert 'mktemp' in cmd

    # Helm exit code must be explicitly captured and re-raised. The rm-after-
    # helm pattern would otherwise mask a helm failure with a clean exit 0.
    assert 'HELM_RET=$?' in cmd
    assert 'exit $HELM_RET' in cmd

    # Must enable node-exporter (the deliberate deviation from the skill example).
    assert 'prometheus-node-exporter' in cmd

    # pushgateway and alertmanager explicitly disabled.
    assert 'prometheus-pushgateway' in cmd
    assert 'alertmanager' in cmd


def test_prometheus_install_cmd_node_exporter_enabled_not_disabled():
    """Regression: guard against ever flipping node-exporter to disabled."""
    cmd = deploy._prometheus_install_cmd('')
    # Find the prometheus-node-exporter section and verify it's enabled: true,
    # not enabled: false.
    ne_section = cmd[cmd.index('prometheus-node-exporter'):]
    # The first 'enabled:' after the node-exporter key must be 'true'.
    enabled_line = ne_section[ne_section.index('enabled:'):].splitlines()[0]
    assert enabled_line.strip() == 'enabled: true'


def test_monitoring_command_builders_keep_facade_and_pickle_identity():
    for command_builder in (deploy._dcgm_exporter_service_cmd,
                            deploy._prometheus_install_cmd):
        assert command_builder.__module__ == deploy.__name__
        assert pickle.loads(pickle.dumps(command_builder)) is command_builder


def test_deploy_single_cluster_monitoring_remote_call_order(
        monkeypatch, tmp_path):
    result, remote_calls, local_calls = _run_single_cluster(
        monkeypatch, tmp_path)

    assert not result
    commands = [call[0] for call in remote_calls]
    monitoring_indexes = [
        next(i
             for i, command in enumerate(commands)
             if marker in command)
        for marker in ('helm install gpu-operator', 'DCGM_DS=', "kind: Service",
                       'helm upgrade --install skypilot-prometheus')
    ]
    assert monitoring_indexes == sorted(monitoring_indexes)
    assert all(remote_calls[index][1].get('print_output') is True
               for index in monitoring_indexes[1:])
    assert any(command == ['sky', 'check', 'ssh'] for command, _ in local_calls)


def test_deploy_single_cluster_monitoring_failures_are_best_effort(
        monkeypatch, tmp_path):
    result, remote_calls, local_calls = _run_single_cluster(
        monkeypatch, tmp_path, monitoring_failures=True)

    assert not result
    commands = [call[0] for call in remote_calls]
    assert any('helm install gpu-operator' in command for command in commands)
    assert any('DCGM_DS=' in command for command in commands)
    assert not any('kind: Service' in command for command in commands)
    assert any('helm upgrade --install skypilot-prometheus' in command
               for command in commands)
    assert any(command == ['sky', 'check', 'ssh'] for command, _ in local_calls)
