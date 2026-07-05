"""Tests for the skylet HealthService gRPC health probe.

The cluster health probe historically SSH-exec'd `ray status` from the API
server; transient SSH transport failures could flag a healthy cluster as
abnormal (and, for multi-node managed jobs, trigger a whole-cluster
recovery). The HealthService runs the same command locally on the head via
skylet and ships the raw (returncode, stdout, stderr) back, so the caller
keeps the exact legacy parser and the SSH path stays as fallback.
"""
# pylint: disable=protected-access
import json
import subprocess
from unittest import mock

from sky.backends import backend_utils
from sky.schemas.generated import healthv1_pb2
from sky.skylet import constants as skylet_constants
from sky.skylet import services


class TestHealthServiceImpl:

    def _run(self, tmp_path, run_side_effect):
        port_file = tmp_path / 'ray_port.json'
        port_file.write_text(json.dumps({'ray_port': 12345}))
        impl = services.HealthServiceImpl()
        captured = {}

        def _fake_run(cmd, **kwargs):
            captured['cmd'] = cmd
            captured['env'] = kwargs.get('env')
            return run_side_effect(cmd, **kwargs)

        with mock.patch.object(services.runtime_utils,
                               'get_runtime_dir_path',
                               return_value=str(port_file)), \
             mock.patch.object(services.subprocess, 'run',
                               side_effect=_fake_run):
            response = impl.GetRayStatus(healthv1_pb2.GetRayStatusRequest(),
                                         context=mock.Mock())
        return response, captured

    def test_returns_raw_ray_status_result(self, tmp_path):

        def _ok(cmd, **kwargs):
            del cmd, kwargs
            return subprocess.CompletedProcess(args='ray status',
                                               returncode=0,
                                               stdout='1 ray.head.default\n',
                                               stderr='')

        response, captured = self._run(tmp_path, _ok)
        assert response.returncode == 0
        assert response.stdout == '1 ray.head.default\n'
        assert response.stderr == ''
        # Targets the ray port SkyPilot launched (from the port file), on
        # loopback — no SSH involved.
        assert captured['env']['RAY_ADDRESS'] == '127.0.0.1:12345'

    def test_nonzero_returncode_is_reported_not_raised(self, tmp_path):
        # Ray being down is a RESULT (the probe transport worked), not an
        # RPC error: the caller must see it exactly like a failed SSH'd
        # `ray status`.
        def _fail(cmd, **kwargs):
            del cmd, kwargs
            return subprocess.CompletedProcess(args='ray status',
                                               returncode=1,
                                               stdout='',
                                               stderr='no ray')

        response, _ = self._run(tmp_path, _fail)
        assert response.returncode == 1
        assert response.stderr == 'no ray'

    def test_timeout_maps_to_failed_status(self, tmp_path):

        def _hang(cmd, **kwargs):
            del kwargs
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=30)

        response, _ = self._run(tmp_path, _hang)
        assert response.returncode != 0

    def test_missing_port_file_falls_back_to_default_port(self, tmp_path):

        def _ok(cmd, **kwargs):
            del cmd, kwargs
            return subprocess.CompletedProcess(args='ray status',
                                               returncode=0,
                                               stdout='',
                                               stderr='')

        impl = services.HealthServiceImpl()
        captured = {}

        def _fake_run(cmd, **kwargs):
            captured['env'] = kwargs.get('env')
            return _ok(cmd, **kwargs)

        with mock.patch.object(services.runtime_utils,
                               'get_runtime_dir_path',
                               return_value=str(tmp_path / 'missing.json')), \
             mock.patch.object(services.subprocess, 'run',
                               side_effect=_fake_run):
            impl.GetRayStatus(healthv1_pb2.GetRayStatusRequest(),
                              context=mock.Mock())
        assert captured['env']['RAY_ADDRESS'] == (
            f'127.0.0.1:{skylet_constants.SKY_REMOTE_RAY_PORT}')


class TestRayStatusViaSkyletGrpc:
    """The client helper must fall back (return None) on ANY gRPC-layer
    failure — the gRPC path may never make the health check stricter than
    the SSH path — and pass through skylet's raw result otherwise."""

    def test_disabled_flag_returns_none(self):
        handle = mock.Mock()
        handle.is_grpc_enabled_with_flag = False
        assert backend_utils._ray_status_via_skylet_grpc(handle) is None

    def test_success_returns_triple(self):
        handle = mock.Mock()
        handle.is_grpc_enabled_with_flag = True
        response = healthv1_pb2.GetRayStatusResponse(
            returncode=0, stdout='1 ray.head.default\n', stderr='')
        with mock.patch.object(backend_utils,
                               'invoke_skylet_with_retries',
                               return_value=response):
            result = backend_utils._ray_status_via_skylet_grpc(handle)
        assert result == (0, '1 ray.head.default\n', '')

    def test_unhealthy_ray_is_passed_through_not_swallowed(self):
        handle = mock.Mock()
        handle.is_grpc_enabled_with_flag = True
        response = healthv1_pb2.GetRayStatusResponse(returncode=1,
                                                     stdout='',
                                                     stderr='no ray')
        with mock.patch.object(backend_utils,
                               'invoke_skylet_with_retries',
                               return_value=response):
            result = backend_utils._ray_status_via_skylet_grpc(handle)
        # Non-zero rc flows to the caller (which raises CommandError like
        # the SSH path); it must NOT be treated as "gRPC unavailable".
        assert result == (1, '', 'no ray')

    def test_any_rpc_failure_returns_none(self):
        handle = mock.Mock()
        handle.is_grpc_enabled_with_flag = True
        with mock.patch.object(backend_utils,
                               'invoke_skylet_with_retries',
                               side_effect=RuntimeError('channel broke')):
            assert backend_utils._ray_status_via_skylet_grpc(handle) is None
