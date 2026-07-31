"""Compatibility contract for SkyServe scheduling request metadata."""

# This contract intentionally pins historical private facade members.
# pylint: disable=protected-access

import ast
import hashlib
import inspect
import textwrap
from types import SimpleNamespace

import fastapi
import pytest

from sky.serve import load_balancer

_CALLABLE_CONTRACT = {
    '_priority_header_error': (
        staticmethod,
        '(detail: str) -> fastapi.exceptions.HTTPException',
        '5efdb998ebf7028df74cf13ba14d2249763edb83ce85dfbb19ab0d1b0d277c37',
    ),
    '_parse_request_priority': (
        classmethod,
        '(cls, request: starlette.requests.Request) -> int',
        'f2a18b44149fe568ebf0d6b6f6371ad40c952894e11066932e8e024b78b32aad',
    ),
    '_accelerator_header_error': (
        staticmethod,
        '(detail: str, status_code: int = 400) -> '
        'fastapi.exceptions.HTTPException',
        '1bdb7e64d558960fd8af5d440425dd0914404b27d44fe063808600a5354bfcf0',
    ),
    '_parse_request_accelerators': (
        type(lambda: None),
        '(self, request: starlette.requests.Request) -> tuple[str, ...] | None',
        '7faccd849ee7ffa7c28d38b599597a9dc4993d47238e93b79d3c292950310157',
    ),
    '_headers_without_request_priority': (
        staticmethod,
        '(request: starlette.requests.Request) -> Any',
        'd47f092f0b5bda5d71ffb16469f1259de32fa23ab5a960fbb39382333dc6d4e1',
    ),
}


def _request(raw_headers: list[tuple[bytes, bytes]]) -> fastapi.Request:
    return fastapi.Request({
        'type': 'http',
        'method': 'POST',
        'path': '/',
        'headers': raw_headers,
    })


def _normalized_ast_sha256(function) -> str:
    node = ast.parse(textwrap.dedent(inspect.getsource(function))).body[0]
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    node.decorator_list = []
    normalized = ast.dump(node, include_attributes=False)
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
    assert _normalized_ast_sha256(function) == expected_fingerprint


def test_request_metadata_error_and_translation_contract() -> None:
    lb = object.__new__(load_balancer.SkyServeLoadBalancer)
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
