"""Tests the control-plane API-server reconciliation command."""

import os
import pathlib
import re
import subprocess
import tempfile
import textwrap
import unittest

_MODULE_DIR = pathlib.Path(__file__).resolve().parents[1]
_CONFIG_SEED_PATH = _MODULE_DIR / 'config_seed.tf'
_HCL_VALUES = {
    '${var.host_cluster_name}': 'platform-eks',
    '${var.aws_region}': 'us-east-1',
    '${var.namespace}': 'skypilot',
    '${var.release_name}': 'skypilot',
    '${local.api_server_rollout_timeout_seconds}': '600',
}


def _reconcile_command() -> str:
    config = _CONFIG_SEED_PATH.read_text(encoding='utf-8')
    resource = re.search(
        r'resource "terraform_data" "reconcile_api_server" \{(?P<body>.*)\n\}',
        config,
        flags=re.DOTALL,
    )
    if resource is None:
        raise AssertionError('reconcile_api_server resource not found')
    heredoc = re.search(
        r'command\s+=\s+<<-EOT\n(?P<command>.*?)\n\s+EOT',
        resource.group('body'),
        flags=re.DOTALL,
    )
    if heredoc is None:
        raise AssertionError('reconcile_api_server command not found')

    command = textwrap.dedent(heredoc.group('command'))
    for expression, value in _HCL_VALUES.items():
        command = command.replace(expression, value)
    command = command.replace('$${', '${')
    if re.search(r'\$\{(?:local|var)\.', command):
        raise AssertionError('unresolved HCL interpolation in test command')
    return command


class ReconcileApiServerTest(unittest.TestCase):

    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)
        temp_path = pathlib.Path(self._tempdir.name)
        bin_dir = temp_path / 'bin'
        bin_dir.mkdir()

        self._aws_args_path = temp_path / 'aws-args'
        aws = bin_dir / 'aws'
        aws.write_text(
            '#!/usr/bin/env bash\n'
            'printf \'%s\\0\' "$@" > "$AWS_ARGS_PATH"\n'
            'printf \'aws arg: %s\\n\' "$@"\n',
            encoding='utf-8',
        )
        aws.chmod(0o755)

        self._kubectl_args_path = temp_path / 'kubectl-args'
        kubectl = bin_dir / 'kubectl'
        kubectl.write_text(
            '#!/usr/bin/env bash\n'
            'printf \'%s\\0\' "$@" >> "$KUBECTL_ARGS_PATH"\n'
            'printf \'\\n\' >> "$KUBECTL_ARGS_PATH"\n',
            encoding='utf-8',
        )
        kubectl.chmod(0o755)

        self._env = os.environ.copy()
        self._env['PATH'] = f'{bin_dir}{os.pathsep}{self._env["PATH"]}'
        self._env['AWS_ARGS_PATH'] = str(self._aws_args_path)
        self._env['KUBECTL_ARGS_PATH'] = str(self._kubectl_args_path)

    def _run(
        self,
        proxy_url: str | None,
        high_availability: bool = False
    ) -> tuple[list[str], subprocess.CompletedProcess[str]]:
        env = self._env.copy()
        env['SKYPILOT_HIGH_AVAILABILITY_ENABLED'] = str(
            high_availability).lower()
        if proxy_url is None:
            env.pop('KUBE_PROXY_URL', None)
        else:
            env['KUBE_PROXY_URL'] = proxy_url

        result = subprocess.run(
            ['bash', '-c', _reconcile_command()],
            check=False,
            capture_output=True,
            env=env,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        args = [
            arg.decode()
            for arg in self._aws_args_path.read_bytes().split(b'\0')
            if arg
        ]
        return args, result

    def _kubectl_invocations(self) -> list[list[str]]:
        if not self._kubectl_args_path.exists():
            return []
        return [[
            arg.decode() for arg in invocation.split(b'\0') if arg
        ] for invocation in self._kubectl_args_path.read_bytes().splitlines()]

    def test_unset_proxy_preserves_direct_update_kubeconfig_command(
            self) -> None:
        args, _ = self._run(None)

        self.assertEqual(args[:6], [
            'eks',
            'update-kubeconfig',
            '--name',
            'platform-eks',
            '--region',
            'us-east-1',
        ])
        self.assertEqual(args[6], '--kubeconfig')
        self.assertEqual(len(args), 8)
        self.assertNotIn('--proxy-url', args)
        self.assertEqual(
            [args[4:] for args in self._kubectl_invocations()],
            [
                ['rollout', 'restart', 'deployment/skypilot-api-server'],
                [
                    'rollout',
                    'status',
                    'deployment/skypilot-api-server',
                    '--timeout=600s',
                ],
            ],
        )

    def test_set_proxy_is_passed_as_one_argument_without_output(self) -> None:
        proxy_url = 'http://proxy.example:18080/path?left=1&right=2'
        args, result = self._run(proxy_url)

        self.assertEqual(args[-2:], ['--proxy-url', proxy_url])
        self.assertNotIn(proxy_url, result.stdout)
        self.assertNotIn(proxy_url, result.stderr)

    def test_high_availability_restarts_and_waits_for_every_role(self) -> None:
        self._run(None, high_availability=True)

        self.assertEqual(
            [args[4:] for args in self._kubectl_invocations()],
            [
                ['rollout', 'restart', 'deployment/skypilot-api-server'],
                ['rollout', 'restart', 'deployment/skypilot-executor'],
                ['rollout', 'restart', 'deployment/skypilot-controller'],
                [
                    'rollout',
                    'status',
                    'deployment/skypilot-api-server',
                    '--timeout=600s',
                ],
                [
                    'rollout',
                    'status',
                    'deployment/skypilot-executor',
                    '--timeout=600s',
                ],
                [
                    'rollout',
                    'status',
                    'deployment/skypilot-controller',
                    '--timeout=600s',
                ],
            ],
        )


if __name__ == '__main__':
    unittest.main()
