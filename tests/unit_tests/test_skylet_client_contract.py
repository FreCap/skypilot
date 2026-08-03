"""Characterization tests for the public Skylet client gateway."""

import ast
import base64
import hashlib
import inspect
from pathlib import Path
import pickle
import subprocess
import sys
import typing
from unittest import mock

import pytest

from sky import backends
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend


def _runtime_source_hash(obj) -> str:
    source = Path(obj.__init__.__code__.co_filename).read_text(encoding='utf-8')
    tree = ast.parse(source)
    class_node = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == obj.__name__)
    class_source = ast.get_source_segment(source, class_node)
    assert class_source is not None
    # The extracted dynamic proxy needs an explicit return type at its new
    # path for basedpyright. Ignore that type-only annotation while pinning
    # the unchanged runtime implementation.
    class_source = class_source.replace(' -> Callable[..., Any]:', ':')
    return hashlib.sha256(class_source.encode()).hexdigest()


def test_public_import_and_pickle_identity():
    client = cloud_vm_ray_backend.SkyletClient

    assert backends.SkyletClient is client
    assert client.__module__ == 'sky.backends.cloud_vm_ray_backend'
    assert pickle.loads(pickle.dumps(client)) is client


@pytest.mark.parametrize('imports', [
    ('import sky.backends.skylet_client as implementation; '
     'import sky.backends.cloud_vm_ray_backend as facade'),
    ('import sky.backends.cloud_vm_ray_backend as facade; '
     'import sky.backends.skylet_client as implementation'),
])
def test_import_orders_preserve_direct_identity(imports):
    code = (f'{imports}; '
            'assert implementation.SkyletClient is facade.SkyletClient')
    subprocess.run([sys.executable, '-c', code], check=True)


def test_historical_pickle_resolves_in_clean_process():
    payload = base64.b64encode(pickle.dumps(
        cloud_vm_ray_backend.SkyletClient)).decode()
    code = ('import base64, pickle; '
            'from sky.backends import cloud_vm_ray_backend; '
            f'client = pickle.loads(base64.b64decode({payload!r})); '
            'assert client is cloud_vm_ray_backend.SkyletClient')

    subprocess.run([sys.executable, '-c', code], check=True)


def test_runtime_type_hints_resolve_for_every_gateway_method():
    methods = inspect.getmembers(cloud_vm_ray_backend.SkyletClient,
                                 inspect.isfunction)

    assert methods
    for _, method in methods:
        assert typing.get_type_hints(method)


def test_gateway_implementation_fingerprints():
    assert _runtime_source_hash(
        cloud_vm_ray_backend._CancelAwareStub  # pylint: disable=protected-access
    ) == '0471f7f7f6e26ce328d8fc2806c2c0aea3189d1c7dc932479724025954463f25'
    assert _runtime_source_hash(
        cloud_vm_ray_backend.SkyletClient
    ) == 'bf8a14c657fab601eec8e8d9488b0746ca87b2151637be033f602829da692a81'


def test_cancel_aware_stub_resolves_unary_transport_through_facade():
    raw_stub = mock.Mock()
    raw_stub.GetStatus = mock.Mock()
    proxy = cloud_vm_ray_backend._CancelAwareStub(  # pylint: disable=protected-access
        raw_stub)

    with mock.patch.object(backend_utils,
                           'invoke_grpc_unary',
                           return_value='response') as invoke:
        assert proxy.GetStatus('request', timeout=3) == 'response'

    invoke.assert_called_once_with(raw_stub.GetStatus, 'request', timeout=3)


def test_cancel_aware_stub_resolves_streaming_transport_through_facade():
    raw_stub = mock.Mock()
    raw_stub.TailLogs = mock.Mock()
    proxy = cloud_vm_ray_backend._CancelAwareStub(  # pylint: disable=protected-access
        raw_stub,
        streaming_methods=('TailLogs',))

    with mock.patch.object(backend_utils,
                           'invoke_grpc_streaming',
                           return_value=iter(('line',))) as invoke:
        assert list(proxy.TailLogs('request', timeout=None)) == ['line']

    invoke.assert_called_once_with(raw_stub.TailLogs, 'request', timeout=None)
