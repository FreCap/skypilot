"""Transport deadline tests for SkyServe replica termination."""

from unittest import mock

from sky.backends import cloud_vm_ray_backend


def test_rpc_covers_controller_acceptance_budget():
    client = object.__new__(cloud_vm_ray_backend.SkyletClient)
    client._serve_stub = mock.Mock()  # pylint: disable=protected-access
    request = mock.Mock()

    client.terminate_replica(request)

    expected_timeout = (
        cloud_vm_ray_backend.serve_constants.TERMINATE_REPLICA_TIMEOUT_SECONDS +
        10)
    client._serve_stub.TerminateReplica.assert_called_once_with(  # pylint: disable=protected-access
        request, timeout=expected_timeout)


def test_rpc_honors_explicit_timeout():
    client = object.__new__(cloud_vm_ray_backend.SkyletClient)
    client._serve_stub = mock.Mock()  # pylint: disable=protected-access
    request = mock.Mock()

    client.terminate_replica(request, timeout=5)

    client._serve_stub.TerminateReplica.assert_called_once_with(  # pylint: disable=protected-access
        request, timeout=5)
