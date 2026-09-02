"""Compatibility contract for SkyServe scheduling request metadata."""

# This contract intentionally pins historical private facade members.
# pylint: disable=protected-access

import ast
import hashlib
import inspect
import pickle
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import fastapi
import pytest

from sky.serve import load_balancer
from sky.serve import load_balancer_request_metadata

_CALLABLE_CONTRACT = {
    '_priority_header_error': (
        staticmethod,
        '(detail: str) -> fastapi.exceptions.HTTPException',
        '0b0c9e207bcc441716ec8b041838373a645bbb9cb9011b7699e78dec677c1a0f',
    ),
    '_parse_request_priority': (
        classmethod,
        '(cls, request: starlette.requests.Request) -> int',
        'ec2f4763f1c49b76e7063675fd13be1a642097f1225d14226e9801fcec4f99ce',
    ),
    '_accelerator_header_error': (
        staticmethod,
        '(detail: str, status_code: int = 400) -> '
        'fastapi.exceptions.HTTPException',
        'c1bbd149b7793633fc90b9b973fa0564665e03d0fa255a375796b3955cdc4b62',
    ),
    '_parse_request_accelerators': (
        type(lambda: None),
        '(self, request: starlette.requests.Request) -> tuple[str, ...] | None',
        '49fdb6f601d7b88ba1efe0952c26f50fb1b60986ab4168034a8ed16f38c5df96',
    ),
    '_headers_without_request_priority': (
        staticmethod,
        '(request: starlette.requests.Request) -> Any',
        '7af76f48739fd6cff3825738cfbba423675d218398cf500b024155f5bb88132e',
    ),
}


def _request(raw_headers: list[tuple[bytes, bytes]]) -> fastapi.Request:
    return fastapi.Request({
        'type': 'http',
        'method': 'POST',
        'path': '/',
        'headers': raw_headers,
    })


def _version_stable_ast(value):
    """Serialize AST fields shared by the supported Python versions."""
    if isinstance(value, ast.AST):
        return (type(value).__name__,
                tuple((field, _version_stable_ast(getattr(value, field)))
                      for field in value._fields
                      if field != 'type_params'))
    if isinstance(value, list):
        return tuple(_version_stable_ast(item) for item in value)
    return value


def _normalized_ast_sha256(function) -> str:
    node = ast.parse(textwrap.dedent(inspect.getsource(function))).body[0]
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    node.decorator_list = []
    normalized = repr(_version_stable_ast(node))
    return hashlib.sha256(normalized.encode()).hexdigest()


@pytest.mark.parametrize('name', _CALLABLE_CONTRACT)
def test_request_metadata_callable_contract(name: str) -> None:
    expected_descriptor, expected_signature, expected_fingerprint = (
        _CALLABLE_CONTRACT[name])
    descriptor = load_balancer.SkyServeLoadBalancer.__dict__[name]
    assert type(descriptor) is expected_descriptor
    function = (descriptor.__func__ if isinstance(
        descriptor, (staticmethod, classmethod)) else descriptor)
    assert str(inspect.signature(function)) == expected_signature
    assert function.__module__ == 'sky.serve.load_balancer'
    assert function.__qualname__ == f'SkyServeLoadBalancer.{name}'
    assert function is getattr(load_balancer_request_metadata, name)
    if name == '_parse_request_priority':
        # Historical classmethod functions are not directly picklable because
        # attribute lookup returns a bound method rather than the function.
        with pytest.raises(pickle.PicklingError):
            pickle.dumps(function)
    else:
        assert pickle.loads(pickle.dumps(function)) is function
    assert _normalized_ast_sha256(function) == expected_fingerprint


@pytest.mark.parametrize('metadata_first', [False, True],
                         ids=['facade-first', 'metadata-first'])
def test_request_metadata_fresh_import_orders(metadata_first: bool) -> None:
    if metadata_first:
        imports = '''
            import sys
            from sky.serve import load_balancer_request_metadata
            assert 'sky.serve.load_balancer' not in sys.modules
            from sky.serve import load_balancer
        '''
    else:
        imports = '''
            import sys
            from sky.serve import load_balancer
            assert 'sky.serve.load_balancer_request_metadata' in sys.modules
            from sky.serve import load_balancer_request_metadata
        '''
    script = textwrap.dedent(imports) + textwrap.dedent('''
        import pickle

        names = (
            '_priority_header_error',
            '_parse_request_priority',
            '_accelerator_header_error',
            '_parse_request_accelerators',
            '_headers_without_request_priority',
        )
        for name in names:
            descriptor = load_balancer.SkyServeLoadBalancer.__dict__[name]
            function = (descriptor.__func__ if isinstance(
                descriptor, (staticmethod, classmethod)) else descriptor)
            assert function is getattr(load_balancer_request_metadata, name)
            if name == '_parse_request_priority':
                try:
                    pickle.dumps(function)
                except pickle.PicklingError:
                    pass
                else:
                    raise AssertionError('classmethod function became picklable')
            else:
                assert pickle.loads(pickle.dumps(function)) is function
    ''')
    subprocess.run([sys.executable, '-c', script], check=True)


def test_request_metadata_error_and_translation_contract() -> None:
    lb = load_balancer.SkyServeLoadBalancer('http://controller:8001', 0)
    lb._configured_accelerators = ('L4', 'A100-80GB')
    lb._request_accelerator_compatibility_version = 1

    assert lb._parse_request_priority(
        _request([(b'X-SkyServe-Priority', b'00037')])) == 37
    with pytest.raises(fastapi.HTTPException) as priority_error:
        lb._parse_request_priority(
            _request([(b'x-skyserve-priority', b'1'),
                      (b'X-SkyServe-Priority', b'2')]))
    assert priority_error.value.status_code == 400
    assert priority_error.value.detail == (
        'X-SkyServe-Priority must appear at most once.')

    assert lb._parse_request_accelerators(
        _request([(b'x-skyserve-compatible-accelerators', b'l4, A100-80GB')
                 ])) == ('L4', 'A100-80GB')
    lb._request_accelerator_compatibility_version = None
    with pytest.raises(fastapi.HTTPException) as accelerator_error:
        lb._parse_request_accelerators(
            _request([(b'x-skyserve-compatible-accelerators', b'L4')]))
    assert accelerator_error.value.status_code == 503
    assert accelerator_error.value.headers == {'Retry-After': '10'}
    assert accelerator_error.value.detail == (
        'X-SkyServe-Compatible-Accelerators cannot be honored until the '
        'controller publishes the exact accelerator catalog; retry after '
        'synchronization.')

    request = _request([
        (b'x-keep', b'first'),
        (b'X-SkyServe-Priority', b'7'),
        (b'x-skyserve-compatible-accelerators', b'L4'),
        (b'x-skyserve-async-attempt-id',
         b'11111111-1111-4111-8111-111111111111'),
        (b'x-skyserve-async-attempt-no', b'1'),
        (b'x-skyserve-async-ledger-revision', b'1'),
        (b'x-skyserve-async-ledger-state', b'ACCEPTED'),
        (b'x-keep', b'second'),
    ])
    assert lb._headers_without_request_priority(request) == [
        (b'x-keep', b'first'),
        (b'x-keep', b'second'),
    ]


def test_request_metadata_mapping_fallback_contract() -> None:
    request = SimpleNamespace(headers={
        'X-SkyServe-Priority': '09',
        'X-Keep': 'value'
    })
    assert (load_balancer.SkyServeLoadBalancer._parse_request_priority(request)
            == 9)
    assert (load_balancer.SkyServeLoadBalancer.
            _headers_without_request_priority(request) == [('X-Keep', 'value')])
