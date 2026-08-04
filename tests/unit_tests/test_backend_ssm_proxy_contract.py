"""Characterization contract for backend SSM ProxyCommand policy."""

import ast
import hashlib
import inspect
import pickle
import subprocess
import sys
import textwrap
from unittest import mock

from sky.backends import backend_utils
from sky.backends import ssm_proxy

_EXPECTED_AST_SHA256 = {
    '_guard_ssm_proxy_command_target': 'b63f825f90e639714aaf22fbde629d47c0e8b2ba2c5593e5a93cd674ebc85c08',
    '_wrap_ssm_proxy_command_with_adaptive_retry': '05b6fc6c0172db28664dcc7f01e701ef12cf18bf67e79f7b59f72d87c89455c0',
    '_upgrade_legacy_ssm_proxy_command': '77404a67e38929ee5ce5b6ae7d9d63092be2178197bf8b1cbda8d34da8f41714',
}

_EXPECTED_CONSTANTS = {
    '_SSM_ADAPTIVE_RETRY_ENV': 'AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12',
    '_SSM_ADAPTIVE_RETRY_WRAPPER_PREFIX': 'env AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12 /bin/sh -c ',
    '_SSM_TARGET_VARIABLE': 'skypilot_ssm_target',
    '_SSM_TARGET_NOT_FOUND_MESSAGE': 'SkyPilot SSM target instance not found for SSH host %h',
    '_SSM_LEGACY_TARGET_NOT_FOUND_PRINTF': "printf '%s\\n'",
    '_SSM_TARGET_NOT_FOUND_PRINTF': "printf '%%s\\n'",
    '_SSM_LEGACY_BROKEN_EXPORT_PREFIX': 'export AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=12;'
}


def _ast_sha256(symbol) -> str:
    source = inspect.getsource(inspect.unwrap(symbol))
    node = ast.parse(textwrap.dedent(source)).body[0]
    dump_kwargs = {'include_attributes': False}
    if 'show_empty' in inspect.signature(ast.dump).parameters:
        dump_kwargs['show_empty'] = True
    normalized = ast.dump(node, **dump_kwargs).replace(', type_params=[]', '')
    return hashlib.sha256(normalized.encode()).hexdigest()


def test_ssm_proxy_policy_structure_and_historical_identity():
    for name, expected_digest in _EXPECTED_AST_SHA256.items():
        symbol = getattr(backend_utils, name)
        assert symbol is getattr(ssm_proxy, name)
        assert symbol.__module__ == 'sky.backends.backend_utils'
        assert inspect.unwrap(symbol).__module__ == 'sky.backends.backend_utils'
        assert _ast_sha256(symbol) == expected_digest

    for name, expected_value in _EXPECTED_CONSTANTS.items():
        assert getattr(backend_utils, name) == expected_value
        assert getattr(backend_utils, name) == getattr(ssm_proxy, name)
    assert backend_utils._SSM_START_SESSION_WITH_LOOKUP_PATTERN is (  # pylint: disable=protected-access
        ssm_proxy._SSM_START_SESSION_WITH_LOOKUP_PATTERN)  # pylint: disable=protected-access
    assert backend_utils._SSM_START_SESSION_WITH_LOOKUP_PATTERN.pattern == (  # pylint: disable=protected-access
        r'^aws ssm start-session --target "\$\((?P<lookup>.*)\)" '
        r'(?P<arguments>.*)$')


def test_single_credential_read_normalizes_once_through_facade():
    config = {
        'auth': {
            'ssh_user': 'ubuntu',
            'ssh_proxy_command': 'legacy-proxy',
        },
        'cluster_name': 'cluster',
        'provider': {
            'module': 'sky.providers.aws'
        },
    }
    with mock.patch.object(backend_utils,
                           '_upgrade_legacy_ssm_proxy_command',
                           return_value='normalized-proxy') as upgrade:
        credentials = backend_utils.ssh_credential_from_yaml(None,
                                                             config=config)

    upgrade.assert_called_once_with('legacy-proxy')
    assert credentials == {
        'ssh_user': 'ubuntu',
        'ssh_private_key': None,
        'ssh_control_name': 'cluster',
        'ssh_proxy_command': 'normalized-proxy',
    }


def test_ssm_proxy_policy_import_order_and_pickle_identity():
    assertions = """
import pickle
for name in (
    '_guard_ssm_proxy_command_target',
    '_wrap_ssm_proxy_command_with_adaptive_retry',
    '_upgrade_legacy_ssm_proxy_command',
):
    facade_symbol = getattr(facade, name)
    assert facade_symbol is getattr(implementation, name)
    assert pickle.loads(pickle.dumps(facade_symbol)) is facade_symbol
"""
    programs = (
        """
from sky.backends import ssm_proxy as implementation
from sky.backends import backend_utils as facade
""",
        """
from sky.backends import backend_utils as facade
from sky.backends import ssm_proxy as implementation
""",
    )
    for imports in programs:
        subprocess.run([sys.executable, '-c', imports + assertions], check=True)

    for name in _EXPECTED_AST_SHA256:
        symbol = getattr(backend_utils, name)
        assert pickle.loads(pickle.dumps(symbol)) is symbol
