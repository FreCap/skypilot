"""Rpc Utilities for SkyServe"""

import typing
from typing import Any

from sky import backends
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.serve import serve_utils

if typing.TYPE_CHECKING:
    from sky.schemas.generated import servev1_pb2
else:
    servev1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.servev1_pb2')

# ======================= gRPC Converters for Sky Serve =======================


class GetServiceStatusRequestConverter:
    """Converter for GetServiceStatusRequest"""

    @classmethod
    def to_proto(
        cls,
        service_names: list[str] | None,
        pool: bool,
        summary_only: bool = False,
        include_target_num_replicas: bool | None = None,
        metadata_only: bool = False,
    ) -> 'servev1_pb2.GetServiceStatusRequest':
        request = servev1_pb2.GetServiceStatusRequest()
        request.pool = pool
        request.summary_only = summary_only
        request.metadata_only = metadata_only
        if include_target_num_replicas is not None:
            request.include_target_num_replicas = include_target_num_replicas
        if service_names is not None:
            request.service_names.names.extend(service_names)
        return request

    @classmethod
    def from_proto(
        cls, proto: 'servev1_pb2.GetServiceStatusRequest'
    ) -> tuple[list[str] | None, bool, bool, bool | None, bool]:
        pool = proto.pool
        if proto.HasField('service_names'):
            service_names = list(proto.service_names.names)
        else:
            service_names = None
        if proto.HasField('include_target_num_replicas'):
            include_target_num_replicas = proto.include_target_num_replicas
        else:
            include_target_num_replicas = None
        return (service_names, pool, proto.summary_only,
                include_target_num_replicas, proto.metadata_only)


class GetServiceStatusResponseConverter:
    """Converter for GetServiceStatusResponse"""

    @classmethod
    def to_proto(
        cls,
        statuses: list[dict[str,
                            str]]) -> 'servev1_pb2.GetServiceStatusResponse':
        response = servev1_pb2.GetServiceStatusResponse()
        for status in statuses:
            added = response.statuses.add()
            added.status.update(status)
        return response

    @classmethod
    def from_proto(
            cls, proto: 'servev1_pb2.GetServiceStatusResponse'
    ) -> list[dict[str, str]]:
        pickled = [dict(status.status) for status in proto.statuses]
        return pickled


class TerminateServicesRequestConverter:
    """Converter for TerminateServicesRequest"""

    @classmethod
    def to_proto(cls, service_names: list[str] | None, purge: bool,
                 pool: bool) -> 'servev1_pb2.TerminateServicesRequest':
        request = servev1_pb2.TerminateServicesRequest()
        request.purge = purge
        request.pool = pool
        if service_names is not None:
            request.service_names.names.extend(service_names)
        return request

    @classmethod
    def from_proto(
        cls, proto: 'servev1_pb2.TerminateServicesRequest'
    ) -> tuple[list[str] | None, bool, bool]:
        purge = proto.purge
        pool = proto.pool
        if proto.HasField('service_names'):
            service_names = list(proto.service_names.names)
        else:
            service_names = None
        return service_names, purge, pool


# ========================= gRPC Runner for Sky Serve =========================


class RpcRunner:
    """gRPC Runner for Sky Serve

    The RPC runner does not catch errors, and assumes that backend handle has
    grpc enabled.

    Common exceptions raised:
        exceptions.FetchClusterInfoError
        exceptions.SkyletInternalError
        grpc.RpcError
        grpc.FutureTimeoutError
        AssertionError
    """

    @classmethod
    def get_service_status(
        cls,
        handle: backends.CloudVmRayResourceHandle,
        service_names: list[str] | None,
        pool: bool,
        summary_only: bool = False,
        include_target_num_replicas: bool | None = None,
        metadata_only: bool = False,
    ) -> list[dict[str, Any]]:
        assert handle.is_grpc_enabled_with_flag
        request = GetServiceStatusRequestConverter.to_proto(
            service_names,
            pool,
            summary_only,
            include_target_num_replicas=include_target_num_replicas,
            metadata_only=metadata_only)
        response = backend_utils.invoke_skylet_with_retries(
            lambda: backends.SkyletClient(handle.get_grpc_channel()
                                         ).get_service_status(request))
        pickled = GetServiceStatusResponseConverter.from_proto(response)
        return serve_utils.unpickle_service_status(pickled)

    @classmethod
    def add_version(cls, handle: backends.CloudVmRayResourceHandle,
                    service_name: str) -> int:
        assert handle.is_grpc_enabled_with_flag
        request = servev1_pb2.AddVersionRequest(service_name=service_name)
        response = backend_utils.invoke_skylet_with_retries(
            lambda: backends.SkyletClient(handle.get_grpc_channel()
                                         ).add_serve_version(request))
        return response.version

    @classmethod
    def terminate_services(cls, handle: backends.CloudVmRayResourceHandle,
                           service_names: list[str] | None, purge: bool,
                           pool: bool) -> str:
        assert handle.is_grpc_enabled_with_flag
        request = TerminateServicesRequestConverter.to_proto(
            service_names, purge, pool)
        response = backend_utils.invoke_skylet_with_retries(
            lambda: backends.SkyletClient(handle.get_grpc_channel()
                                         ).terminate_services(request))
        return response.message

    @classmethod
    def terminate_replica(cls, handle: backends.CloudVmRayResourceHandle,
                          service_name: str, replica_id: int,
                          purge: bool) -> str:
        assert handle.is_grpc_enabled_with_flag
        request = servev1_pb2.TerminateReplicaRequest(service_name=service_name,
                                                      replica_id=replica_id,
                                                      purge=purge)
        response = backend_utils.invoke_skylet_with_retries(
            lambda: backends.SkyletClient(handle.get_grpc_channel()
                                         ).terminate_replica(request),
            max_attempts=1)
        return response.message

    @classmethod
    def wait_service_registration(cls,
                                  handle: backends.CloudVmRayResourceHandle,
                                  service_name: str, job_id: int,
                                  pool: bool) -> int:
        assert handle.is_grpc_enabled_with_flag
        request = servev1_pb2.WaitServiceRegistrationRequest(
            service_name=service_name, job_id=job_id, pool=pool)
        response = backend_utils.invoke_skylet_with_retries(
            lambda: backends.SkyletClient(handle.get_grpc_channel()
                                         ).wait_service_registration(request))
        return response.lb_port

    @classmethod
    def update_service(cls, handle: backends.CloudVmRayResourceHandle,
                       service_name: str, version: int,
                       mode: serve_utils.UpdateMode, pool: bool) -> None:
        assert handle.is_grpc_enabled_with_flag
        request = servev1_pb2.UpdateServiceRequest(service_name=service_name,
                                                   version=version,
                                                   mode=mode.value,
                                                   pool=pool)
        backend_utils.invoke_skylet_with_retries(lambda: backends.SkyletClient(
            handle.get_grpc_channel()).update_service(request))
