"""Tests for Kubernetes context command construction."""

import subprocess
from unittest import mock

from sky.provision.kubernetes import context_utils


def test_get_kubeconfig_text_passes_context_as_single_argument(monkeypatch):
    context = 'research team\'s "gpu" context'
    run = mock.Mock(return_value=subprocess.CompletedProcess(
        args=[], returncode=0, stdout=b'config', stderr=b''))
    monkeypatch.setattr(context_utils.subprocess, 'run', run)

    assert context_utils.get_kubeconfig_text_for_context(context) == 'config'
    run.assert_called_once_with(
        ['kubectl', 'config', 'view', '--minify', f'--context={context}'],
        check=False,
        env=mock.ANY,
        capture_output=True)
