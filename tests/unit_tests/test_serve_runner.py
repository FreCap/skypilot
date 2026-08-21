"""Tests for sky.serve.runner registry and _DefaultServiceStatusRunner.

Covers the strategy/registry pattern that lets plugins swap out the
default codegen+subprocess status fetcher for an in-process one when
the controller is consolidated into the API server.
"""
# pylint: disable=invalid-name,protected-access
import contextlib
import pickle
from unittest import mock

import pytest

from sky import backends
from sky import exceptions
from sky.serve import runner as serve_runner
from sky.serve.server import impl
from sky.serve.server import status as serve_status


@pytest.fixture(autouse=True)
def _reset_registry():
    """Ensure each test starts with no plugin runner registered."""
    serve_runner.reset_for_testing()
    yield
    serve_runner.reset_for_testing()


def _handle_mock(grpc_enabled: bool = True):
    h = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
    h.is_grpc_enabled_with_flag = grpc_enabled
    return h


def _backend_mock():
    return mock.MagicMock(spec=backends.CloudVmRayBackend)


class TestRegistry:
    """Runner registry: default construction and override."""

    def test_impl_exports_keep_historical_module_identity(self):
        assert (impl._external_service_endpoint_url
                is serve_status.external_service_endpoint_url)
        assert impl.status is serve_status.status
        assert (impl._DefaultServiceStatusRunner
                is serve_status.DefaultServiceStatusRunner)
        assert (impl._external_service_endpoint_url.__module__ == impl.__name__)
        assert (impl._external_service_endpoint_url.__name__ ==
                '_external_service_endpoint_url')
        assert impl.status.__module__ == impl.__name__
        assert impl._DefaultServiceStatusRunner.__module__ == impl.__name__
        assert (impl._DefaultServiceStatusRunner.__name__ ==
                '_DefaultServiceStatusRunner')
        for exported_symbol in (impl._external_service_endpoint_url,
                                impl.status, impl._DefaultServiceStatusRunner):
            assert pickle.loads(
                pickle.dumps(exported_symbol)) is exported_symbol

    def test_current_returns_default_when_unregistered(self):
        runner = serve_runner.current()
        assert isinstance(runner, impl._DefaultServiceStatusRunner)

    def test_current_caches_default(self):
        a = serve_runner.current()
        b = serve_runner.current()
        assert a is b

    def test_register_replaces_default(self):
        custom = mock.Mock()
        serve_runner.register(custom)
        assert serve_runner.current() is custom

    def test_register_last_wins(self):
        first = mock.Mock()
        second = mock.Mock()
        serve_runner.register(first)
        serve_runner.register(second)
        assert serve_runner.current() is second


class TestDefaultRunnerRpcPath:
    """gRPC path: ``is_grpc_enabled_with_flag`` true and RpcRunner returns."""

    def test_rpc_success_no_legacy_fallback(self):
        runner = impl._DefaultServiceStatusRunner()
        handle = _handle_mock(grpc_enabled=True)
        expected = [{'name': 'p1', 'status': 'READY'}]
        with mock.patch(
                'sky.serve.server.status.serve_rpc_utils.RpcRunner.'
                'get_service_status',
                return_value=expected) as rpc, \
             mock.patch(
                'sky.serve.server.status.serve_utils.ServeCodeGen.'
                'get_service_status') as codegen, \
             mock.patch(
                'sky.serve.server.status.backend_utils.'
                'get_backend_from_handle') as get_backend:
            result = runner.get_service_status(handle=handle,
                                               service_names=['p1'],
                                               pool=True)
        assert result == expected
        rpc.assert_called_once_with(handle, ['p1'],
                                    True,
                                    summary_only=False,
                                    metadata_only=False,
                                    include_target_num_replicas=None)
        codegen.assert_not_called()
        # RPC path must not even materialize a backend.
        get_backend.assert_not_called()

    def test_rpc_not_implemented_falls_back_to_legacy(self):
        runner = impl._DefaultServiceStatusRunner()
        handle = _handle_mock(grpc_enabled=True)
        backend = _backend_mock()
        backend.run_on_head.return_value = (0, b'PAYLOAD', '')
        legacy_records = [{'name': 'p2'}]
        with mock.patch(
                'sky.serve.server.status.serve_rpc_utils.RpcRunner.'
                'get_service_status',
                side_effect=exceptions.SkyletMethodNotImplementedError(
                    'old skylet')), \
             mock.patch(
                'sky.serve.server.status.serve_utils.ServeCodeGen.'
                'get_service_status',
                return_value='CODE') as codegen, \
             mock.patch(
                'sky.serve.server.status.serve_utils.load_service_status',
                return_value=legacy_records) as load, \
             mock.patch(
                'sky.serve.server.status.backend_utils.'
                'get_backend_from_handle',
                return_value=backend):
            result = runner.get_service_status(handle=handle,
                                               service_names=None,
                                               pool=False)
        assert result == legacy_records
        codegen.assert_called_once_with(None,
                                        pool=False,
                                        summary_only=False,
                                        metadata_only=False,
                                        include_target_num_replicas=None)
        backend.run_on_head.assert_called_once()
        load.assert_called_once_with(b'PAYLOAD')


class TestDefaultRunnerLegacyPath:
    """Direct legacy path: ``is_grpc_enabled_with_flag`` false."""

    def test_legacy_when_grpc_disabled(self):
        runner = impl._DefaultServiceStatusRunner()
        handle = _handle_mock(grpc_enabled=False)
        backend = _backend_mock()
        backend.run_on_head.return_value = (0, b'PAYLOAD', '')
        with mock.patch(
                'sky.serve.server.status.serve_rpc_utils.RpcRunner.'
                'get_service_status') as rpc, \
             mock.patch(
                'sky.serve.server.status.serve_utils.ServeCodeGen.'
                'get_service_status',
                return_value='CODE'), \
             mock.patch(
                'sky.serve.server.status.serve_utils.load_service_status',
                return_value=[{'name': 'p3'}]), \
             mock.patch(
                'sky.serve.server.status.backend_utils.'
                'get_backend_from_handle',
                return_value=backend):
            result = runner.get_service_status(handle=handle,
                                               service_names=['p3'],
                                               pool=True)
        rpc.assert_not_called()
        backend.run_on_head.assert_called_once()
        assert result == [{'name': 'p3'}]

    def test_metadata_uses_v9_compatible_codegen_even_when_grpc_enabled(self):
        runner = impl._DefaultServiceStatusRunner()
        handle = _handle_mock(grpc_enabled=True)
        backend = _backend_mock()
        backend.run_on_head.return_value = (0, b'PAYLOAD', '')
        with mock.patch(
                'sky.serve.server.status.serve_rpc_utils.RpcRunner.'
                'get_service_status') as rpc, \
             mock.patch(
                'sky.serve.server.status.serve_utils.ServeCodeGen.'
                'get_service_status', return_value='CODE') as codegen, \
             mock.patch(
                'sky.serve.server.status.serve_utils.load_service_status',
                return_value=[{'name': 'svc', 'metadata_only': True}]), \
             mock.patch(
                'sky.serve.server.status.backend_utils.'
                'get_backend_from_handle', return_value=backend):
            result = runner.get_service_status(handle=handle,
                                               service_names=['svc'],
                                               pool=False,
                                               metadata_only=True)

        rpc.assert_not_called()
        codegen.assert_called_once_with(['svc'],
                                        pool=False,
                                        summary_only=False,
                                        metadata_only=True,
                                        include_target_num_replicas=None)
        backend.run_on_head.assert_called_once()
        assert result == [{'name': 'svc', 'metadata_only': True}]

    def test_legacy_command_error_surfaces_as_runtimeerror(self):
        runner = impl._DefaultServiceStatusRunner()
        handle = _handle_mock(grpc_enabled=False)
        backend = _backend_mock()
        backend.run_on_head.return_value = (1, b'', 'boom')
        with mock.patch(
                'sky.serve.server.status.serve_utils.ServeCodeGen.'
                'get_service_status',
                return_value='CODE'), \
             mock.patch(
                'sky.serve.server.status.subprocess_utils.handle_returncode',
                side_effect=exceptions.CommandError(returncode=1,
                                                    command='CODE',
                                                    error_msg='boom failed',
                                                    detailed_reason=None)), \
             mock.patch(
                'sky.serve.server.status.backend_utils.'
                'get_backend_from_handle',
                return_value=backend):
            with pytest.raises(RuntimeError, match='boom failed'):
                runner.get_service_status(handle=handle,
                                          service_names=None,
                                          pool=True)


class TestStatusDelegatesToRunner:
    """`status()` entry point passes the right args to the registered runner."""

    def _common_patches(self, handle=None, get_backend_return=None):
        if handle is None:
            handle = _handle_mock(grpc_enabled=True)
        return [
            mock.patch('sky.serve.server.status.backend_utils.'
                       'check_network_connection'),
            mock.patch(
                'sky.serve.server.status.controller_utils.get_controller_for_pool'
            ),
            mock.patch(
                'sky.serve.server.status.backend_utils.is_controller_accessible',
                return_value=handle),
            mock.patch(
                'sky.serve.server.status.backend_utils.get_backend_from_handle',
                return_value=get_backend_return or _backend_mock()),
        ]

    def test_calls_registered_runner_with_normalized_args(self):
        captured = {}

        def fake_get(*,
                     handle,
                     service_names,
                     pool,
                     summary_only=False,
                     include_target_num_replicas=None):
            captured['handle'] = handle
            captured['service_names'] = service_names
            captured['pool'] = pool
            captured['summary_only'] = summary_only
            captured['include_target_num_replicas'] = (
                include_target_num_replicas)
            # Pool path: skip the endpoint-augmentation loop.
            return []

        serve_runner.register(mock.Mock(get_service_status=fake_get))

        with contextlib.ExitStack() as stack:
            for p in self._common_patches():
                stack.enter_context(p)
            impl.status(service_names='single', pool=True)

        # service_names should be normalized from str -> [str].
        assert captured['service_names'] == ['single']
        assert captured['pool'] is True
        assert captured['summary_only'] is False
        assert captured['include_target_num_replicas'] is None

    def test_authenticated_owner_scope_filters_wildcards_before_controller(
            self):
        records = [{
            'name': 'owned-a',
            'load_balancer_port': None,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            get_names = stack.enter_context(
                mock.patch.object(serve_status.serve_state,
                                  'get_glob_service_names',
                                  return_value=['owned-a']))
            get_owned_names = stack.enter_context(
                mock.patch.object(serve_status.serve_state,
                                  'get_service_names_owned_by_user_id',
                                  return_value=['owned-a']))
            result = impl.status(
                service_names=['owned-*', 'other-*'],
                pool=False,
                authorized_owner_user_id='owner-a',
            )

        get_names.assert_called_once_with(['owned-*', 'other-*'],
                                          pool=False,
                                          owner_user_id='owner-a')
        get_owned_names.assert_called_once_with('owner-a')
        assert runner.get_service_status.call_args.kwargs['service_names'] == [
            'owned-a'
        ]
        assert [record['name'] for record in result] == ['owned-a']

    def test_authenticated_scope_drops_same_name_successor_after_controller(
            self):
        runner = mock.Mock()
        runner.get_service_status.return_value = [{
            'name': 'recreated',
            'load_balancer_port': None,
            'tls_encrypted': False,
        }]
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.object(serve_status.serve_state,
                                  'get_glob_service_names',
                                  return_value=['recreated']))
            get_owned_names = stack.enter_context(
                mock.patch.object(serve_status.serve_state,
                                  'get_service_names_owned_by_user_id',
                                  return_value=[]))
            result = impl.status(
                service_names=['recreated'],
                pool=False,
                authorized_owner_user_id='owner-a',
            )

        get_owned_names.assert_called_once_with('owner-a')
        assert result == []

    def test_empty_authenticated_owner_scope_skips_controller(self):
        runner = mock.Mock()
        serve_runner.register(runner)

        with mock.patch.object(serve_status.serve_state,
                               'get_glob_service_names',
                               return_value=[]) as get_names, \
             mock.patch.object(serve_status.backend_utils,
                               'check_network_connection') as network:
            result = impl.status(
                service_names=None,
                pool=False,
                authorized_owner_user_id='owner-a',
            )

        get_names.assert_called_once_with(None,
                                          pool=False,
                                          owner_user_id='owner-a')
        assert result == []
        network.assert_not_called()
        runner.get_service_status.assert_not_called()

    def test_rpc_then_legacy_fallback_end_to_end_via_status(self):
        """status() -> default runner -> RPC raises NotImplemented -> legacy.

        This is the operationally important path (old skylets) and the
        only one that exercises the full status() + default-runner chain
        without registering a mock runner.
        """
        handle = _handle_mock(grpc_enabled=True)
        backend = _backend_mock()
        backend.run_on_head.return_value = (0, b'PAYLOAD', '')
        legacy_records = [{
            'name': 'svc',
            'load_balancer_port': None,
            'tls_encrypted': False,
        }]
        with contextlib.ExitStack() as stack:
            for p in self._common_patches(handle=handle,
                                          get_backend_return=backend):
                stack.enter_context(p)
            rpc = stack.enter_context(
                mock.patch(
                    'sky.serve.server.status.serve_rpc_utils.RpcRunner.'
                    'get_service_status',
                    side_effect=exceptions.SkyletMethodNotImplementedError(
                        'old skylet')))
            codegen = stack.enter_context(
                mock.patch(
                    'sky.serve.server.status.serve_utils.ServeCodeGen.'
                    'get_service_status',
                    return_value='CODE'))
            stack.enter_context(
                mock.patch(
                    'sky.serve.server.status.serve_utils.load_service_status',
                    return_value=legacy_records))
            result = impl.status(pool=False)

        rpc.assert_called_once()
        codegen.assert_called_once()
        backend.run_on_head.assert_called_once()
        # Endpoint augmentation runs (serve path, load_balancer_port=None).
        assert result == [{
            'name': 'svc',
            'load_balancer_port': None,
            'tls_encrypted': False,
            'endpoint': None,
        }]

    def test_returns_runner_output(self):
        records = [{
            'name': 'svc',
            'load_balancer_port': None,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for p in self._common_patches():
                stack.enter_context(p)
            result = impl.status(pool=False)

        # serve path with no load_balancer_port → endpoint stays None
        assert result == [{
            'name': 'svc',
            'load_balancer_port': None,
            'tls_encrypted': False,
            'endpoint': None,
        }]

    def test_summary_only_skips_external_service_endpoint_resolution(self):
        records = [{
            'name': 'svc-a',
            'load_balancer_port': 30001,
            'tls_encrypted': False,
        }, {
            'name': 'svc-b',
            'load_balancer_port': 30001,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for p in self._common_patches():
                stack.enter_context(p)
            external_endpoint = stack.enter_context(
                mock.patch.object(serve_status.lb_k8s,
                                  'lb_service_endpoint_or_none',
                                  side_effect=AssertionError(
                                      'summary-only endpoint resolution')))
            result = impl.status(pool=False, summary_only=True)

        assert [record['endpoint'] for record in result] == [None, None]
        external_endpoint.assert_not_called()

    def test_metadata_only_reaches_runner_and_skips_endpoint_resolution(self):
        records = [{
            'name': 'svc',
            'load_balancer_port': 30001,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            external_endpoint = stack.enter_context(
                mock.patch.object(serve_status.lb_k8s,
                                  'lb_service_endpoint_or_none'))
            result = impl.status(pool=False, metadata_only=True)

        assert result[0]['endpoint'] is None
        external_endpoint.assert_not_called()
        assert runner.get_service_status.call_args.kwargs[
            'metadata_only'] is True

    def test_summary_can_defer_then_opt_into_endpoint_resolution(self):
        records = [{
            'name': 'svc',
            'resource_scope': 'scope-a',
            'load_balancer_port': 30001,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            endpoint = stack.enter_context(
                mock.patch.object(
                    serve_status.lb_k8s,
                    'lb_service_endpoint_or_none',
                    return_value='skypilot-serve-lb-svc.ns.svc:30001'))
            result = impl.status(pool=False,
                                 summary_only=True,
                                 include_endpoints=True)

        endpoint.assert_called_once_with('svc', 'scope-a')
        assert (result[0]['endpoint'] ==
                'http://skypilot-serve-lb-svc.ns.svc:30001')

    def test_status_history_requires_exactly_one_service(self):
        with pytest.raises(ValueError, match='requires exactly one service'):
            impl.status(pool=False, history_hours=12)
        with pytest.raises(ValueError, match='requires exactly one service'):
            impl.status(service_names=['svc-a', 'svc-b'],
                        pool=False,
                        history_hours=12)

    def test_status_attaches_postgres_history_to_named_service(self):
        records = [{
            'name': 'svc',
            'hash': 'hash-a',
            'load_balancer_port': None,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)
        history_payload = {
            'available': True,
            'bucket_seconds': 60,
            'samples': [],
        }

        with contextlib.ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            get_history = stack.enter_context(
                mock.patch.object(serve_status.serve_history,
                                  'get_status_history',
                                  return_value=history_payload))
            result = impl.status(service_names='svc',
                                 pool=False,
                                 summary_only=True,
                                 history_hours=12)

        get_history.assert_called_once_with('svc',
                                            hours=12,
                                            expected_service_hash='hash-a')
        assert result[0]['replica_status_history'] == history_payload

    @pytest.mark.parametrize('tls_encrypted', [False, True])
    def test_status_uses_only_external_service_endpoint(self, tls_encrypted):
        records = [{
            'name': 'svc',
            # Registration sentinel only; it must not affect the URL.
            'load_balancer_port': 39999,
            'tls_encrypted': tls_encrypted,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for p in self._common_patches():
                stack.enter_context(p)
            external_endpoint = stack.enter_context(
                mock.patch.object(
                    serve_status.lb_k8s,
                    'lb_service_endpoint_or_none',
                    return_value='skypilot-serve-lb-svc.ns.svc:30001'))
            legacy_endpoint = stack.enter_context(
                mock.patch.object(
                    serve_status.backend_utils,
                    'get_endpoints',
                    side_effect=AssertionError('legacy endpoint fallback')))
            result = impl.status(pool=False)

        # A legacy TLS-marked row must not advertise HTTPS: TLS terminates at
        # platform ingress and the per-service LB itself is HTTP-only.
        assert (result[0]['endpoint'] ==
                'http://skypilot-serve-lb-svc.ns.svc:30001')
        external_endpoint.assert_called_once_with('svc')
        legacy_endpoint.assert_not_called()

    def test_status_keeps_endpoint_unavailable_without_external_runtime(self):
        records = [{
            'name': 'svc',
            'load_balancer_port': 30001,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for p in self._common_patches():
                stack.enter_context(p)
            external_endpoint = stack.enter_context(
                mock.patch.object(serve_status.lb_k8s,
                                  'lb_service_endpoint_or_none',
                                  return_value=None))
            legacy_endpoint = stack.enter_context(
                mock.patch.object(
                    serve_status.backend_utils,
                    'get_endpoints',
                    side_effect=AssertionError('legacy endpoint fallback')))
            result = impl.status(pool=False)

        assert result[0]['endpoint'] is None
        external_endpoint.assert_called_once_with('svc')
        legacy_endpoint.assert_not_called()

    def test_pool_status_never_constructs_inference_endpoint(self):
        records = [{
            'name': 'pool',
            # Pools retain this DB registration sentinel for compatibility.
            'load_balancer_port': 30001,
            'tls_encrypted': False,
        }]
        runner = mock.Mock()
        runner.get_service_status.return_value = records
        serve_runner.register(runner)

        with contextlib.ExitStack() as stack:
            for p in self._common_patches():
                stack.enter_context(p)
            external_endpoint = stack.enter_context(
                mock.patch.object(
                    serve_status.lb_k8s,
                    'lb_service_endpoint_or_none',
                    side_effect=AssertionError('pool endpoint construction')))
            result = impl.status(pool=True)

        assert result[0]['endpoint'] is None
        external_endpoint.assert_not_called()
