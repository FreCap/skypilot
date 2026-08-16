"""Characterization contract for SkyServe load-balancer retry policy."""

# This contract intentionally pins historical private facade members.
# pylint: disable=protected-access

import ast
import hashlib
import inspect
import pickle
import subprocess
import sys
import textwrap
from unittest import mock

import httpx
import pytest

from sky.serve import load_balancer
from sky.serve import load_balancer_retry

_SYMBOL_CONTRACT = {
    '_RetriableStatusError': (
        '33ef5ce45eed8cdfcccdf565c816646fd8cc3bb5ceb8517f710f4b73f2b7b25e',
        None,
    ),
    '_PreDispatchError': (
        '33e9b21d603b86d2b79b5bd7dad35ea46d8a6abc630b96524264e85af5411268',
        None,
    ),
    '_is_dead_connection_error': (
        '92d512e8dae978618813434980f8e1ba902d1b48d0c61e353d556d0a8dfe6c29',
        '(exc: Exception) -> bool',
    ),
    '_is_definitely_not_dispatched': (
        'bcb7bac90e751e75a37039ceb3703f4db556ba76731c4ea8b21beae6bc4772a6',
        '(exc: Exception) -> bool',
    ),
    '_can_retry_proxy_failure': (
        'f88ff160fb6181ead3d8aad5ad27fa0adce0bfe40399c4effb51059acafd3e8f',
        '(method: str, exc: Exception) -> bool',
    ),
}


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


def _ast_sha256(name: str, symbol) -> str:
    try:
        source = inspect.getsource(symbol)
        node = ast.parse(textwrap.dedent(source)).body[0]
    except OSError:
        # Restoring a moved class's historical __module__ makes inspect search
        # the facade. Read the implementation AST explicitly in that case.
        module_tree = ast.parse(inspect.getsource(load_balancer_retry))
        node = next(
            item for item in module_tree.body
            if isinstance(item, (ast.ClassDef,
                                 ast.FunctionDef)) and item.name == name)
    normalized = repr(_version_stable_ast(node))
    return hashlib.sha256(normalized.encode()).hexdigest()


@pytest.mark.parametrize('name', _SYMBOL_CONTRACT)
def test_retry_policy_symbol_contract(name: str) -> None:
    expected_fingerprint, expected_signature = _SYMBOL_CONTRACT[name]
    symbol = getattr(load_balancer, name)

    assert symbol.__module__ == load_balancer.__name__
    assert symbol.__qualname__ == name
    assert symbol is getattr(load_balancer_retry, name)
    assert pickle.loads(pickle.dumps(symbol)) is symbol
    assert _ast_sha256(name, symbol) == expected_fingerprint
    if expected_signature is not None:
        assert str(inspect.signature(symbol)) == expected_signature


def test_retry_policy_exception_contract() -> None:
    retriable = load_balancer._RetriableStatusError(429, 'http://replica')
    assert isinstance(retriable, Exception)
    assert retriable.status_code == 429
    assert str(
        retriable) == 'replica http://replica answered retriable status 429'

    pre_dispatch = load_balancer._PreDispatchError('client unavailable')
    assert isinstance(pre_dispatch, RuntimeError)
    assert str(pre_dispatch) == 'client unavailable'


def test_retry_policy_keeps_facade_dependency_patch_surface() -> None:
    for name in ('_is_dead_connection_error', '_is_definitely_not_dispatched',
                 '_can_retry_proxy_failure'):
        assert getattr(load_balancer, name).__globals__ is vars(load_balancer)

    class ReplacementPreDispatchError(RuntimeError):
        pass

    error = ReplacementPreDispatchError('replacement')
    with mock.patch.object(load_balancer, '_PreDispatchError',
                           ReplacementPreDispatchError):
        assert load_balancer._is_definitely_not_dispatched(error)
        assert load_balancer._can_retry_proxy_failure('POST', error)

    class ReplacementRetriableStatusError(Exception):
        pass

    with mock.patch.object(load_balancer, '_RetriableStatusError',
                           ReplacementRetriableStatusError):
        assert load_balancer._can_retry_proxy_failure(
            'POST', ReplacementRetriableStatusError('replacement'))

    with mock.patch.object(load_balancer, '_IDEMPOTENT_METHODS',
                           frozenset({'POST'})):
        assert load_balancer._can_retry_proxy_failure('POST', RuntimeError())

    with mock.patch.object(load_balancer,
                           '_is_definitely_not_dispatched',
                           return_value=True):
        assert load_balancer._can_retry_proxy_failure('POST', RuntimeError())


@pytest.mark.parametrize('first_module',
                         ['load_balancer', 'load_balancer_retry'])
def test_retry_policy_import_order_contract(first_module: str) -> None:
    second_module = ('load_balancer_retry'
                     if first_module == 'load_balancer' else 'load_balancer')
    script = f'''\
from sky.serve import {first_module}
from sky.serve import {second_module}
from sky.serve import load_balancer
from sky.serve import load_balancer_retry
for name in {tuple(_SYMBOL_CONTRACT)!r}:
    facade = getattr(load_balancer, name)
    assert facade is getattr(load_balancer_retry, name)
    assert facade.__module__ == load_balancer.__name__
'''
    subprocess.run([sys.executable, '-c', script], check=True)


@pytest.mark.parametrize(
    ('error', 'dead', 'definitely_not_dispatched'),
    [
        (httpx.ConnectError('refused'), True, True),
        (httpx.ConnectTimeout('connect timeout'), False, True),
        (httpx.PoolTimeout('pool timeout'), False, True),
        (load_balancer._PreDispatchError('no client'), False, True),
        (httpx.ReadError('reset while reading'), True, False),
        (httpx.ProtocolError('bad protocol'), True, False),
        (httpx.ReadTimeout('saturated'), False, False),
        (RuntimeError('unexpected'), False, False),
    ],
)
def test_transport_classification_contract(
        error: Exception, dead: bool, definitely_not_dispatched: bool) -> None:
    assert load_balancer._is_dead_connection_error(error) is dead
    assert (load_balancer._is_definitely_not_dispatched(error)
            is definitely_not_dispatched)


@pytest.mark.parametrize('method',
                         ['GET', 'head', 'Put', 'DELETE', 'OPTIONS', 'TRACE'])
def test_idempotent_methods_retry_ambiguous_failures(method: str) -> None:
    assert load_balancer._can_retry_proxy_failure(
        method, httpx.ReadTimeout('ambiguous'))


def test_non_idempotent_retry_contract() -> None:
    retriable_status = load_balancer._RetriableStatusError(
        503, 'http://replica')
    assert load_balancer._can_retry_proxy_failure('POST', retriable_status)

    for error in (load_balancer._PreDispatchError('no client'),
                  httpx.ConnectError('refused'),
                  httpx.ConnectTimeout('connect timeout'),
                  httpx.PoolTimeout('pool timeout')):
        assert load_balancer._can_retry_proxy_failure('POST', error)

    for error in (httpx.ReadError('reset while reading'),
                  httpx.WriteError('reset while writing'),
                  httpx.ProtocolError('bad protocol'),
                  httpx.ReadTimeout('ambiguous'), RuntimeError('unexpected')):
        assert not load_balancer._can_retry_proxy_failure('PATCH', error)
