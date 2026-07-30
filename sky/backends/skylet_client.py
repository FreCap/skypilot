"""Typed client gateway for Skylet gRPC services."""
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
import typing
from typing import Any

from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.serve import constants as serve_constants
from sky.skylet import constants

if typing.TYPE_CHECKING:
    import grpc

    from sky.schemas.generated import autostopv1_pb2
    from sky.schemas.generated import autostopv1_pb2_grpc
    from sky.schemas.generated import healthv1_pb2
    from sky.schemas.generated import healthv1_pb2_grpc
    from sky.schemas.generated import jobsv1_pb2
    from sky.schemas.generated import jobsv1_pb2_grpc
    from sky.schemas.generated import managed_jobsv1_pb2
    from sky.schemas.generated import managed_jobsv1_pb2_grpc
    from sky.schemas.generated import servev1_pb2
    from sky.schemas.generated import servev1_pb2_grpc
else:
    autostopv1_pb2_grpc = adaptors_common.LazyImport(
        'sky.schemas.generated.autostopv1_pb2_grpc')
    healthv1_pb2_grpc = adaptors_common.LazyImport(
        'sky.schemas.generated.healthv1_pb2_grpc')
    jobsv1_pb2_grpc = adaptors_common.LazyImport(
        'sky.schemas.generated.jobsv1_pb2_grpc')
    servev1_pb2_grpc = adaptors_common.LazyImport(
        'sky.schemas.generated.servev1_pb2_grpc')
    managed_jobsv1_pb2_grpc = adaptors_common.LazyImport(
        'sky.schemas.generated.managed_jobsv1_pb2_grpc')


class _CancelAwareStub:
    """Proxy that makes a gRPC stub honor the current SkyPilotContext cancel.

    Each method becomes cancellable: when the active context is cancelled
    (e.g. on client disconnect), the in-flight RPC is aborted instead of
    leaving the worker thread blocked in gRPC's ``Condition.wait()``.

    Methods listed in ``streaming_methods`` go through
    ``invoke_grpc_streaming``; everything else uses ``invoke_grpc_unary``.
    """

    def __init__(self, stub: Any, streaming_methods: Sequence[str] = ()):
        self._stub = stub
        self._streaming = frozenset(streaming_methods)

    def __getattr__(self, name: str) -> Callable[..., Any]:
        method = getattr(self._stub, name)
        if name in self._streaming:

            def wrapped_streaming(*args, **kwargs):
                return backend_utils.invoke_grpc_streaming(
                    method, *args, **kwargs)

            return wrapped_streaming

        def wrapped_unary(*args, **kwargs):
            return backend_utils.invoke_grpc_unary(method, *args, **kwargs)

        return wrapped_unary


class SkyletClient:
    """The client to interact with a remote cluster through Skylet."""

    def __init__(self, channel: 'grpc.Channel'):
        self._autostop_stub = _CancelAwareStub(
            autostopv1_pb2_grpc.AutostopServiceStub(channel))
        self._jobs_stub = _CancelAwareStub(
            jobsv1_pb2_grpc.JobsServiceStub(channel),
            streaming_methods=('TailLogs',))
        self._serve_stub = _CancelAwareStub(
            servev1_pb2_grpc.ServeServiceStub(channel))
        self._managed_jobs_stub = _CancelAwareStub(
            managed_jobsv1_pb2_grpc.ManagedJobsServiceStub(channel))
        self._health_stub = _CancelAwareStub(
            healthv1_pb2_grpc.HealthServiceStub(channel))

    def set_autostop(
        self,
        request: 'autostopv1_pb2.SetAutostopRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'autostopv1_pb2.SetAutostopResponse':
        return self._autostop_stub.SetAutostop(request, timeout=timeout)

    def is_autostopping(
        self,
        request: 'autostopv1_pb2.IsAutostoppingRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'autostopv1_pb2.IsAutostoppingResponse':
        return self._autostop_stub.IsAutostopping(request, timeout=timeout)

    def add_job(
        self,
        request: 'jobsv1_pb2.AddJobRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.AddJobResponse':
        return self._jobs_stub.AddJob(request, timeout=timeout)

    def set_job_info_without_job_id(
        self,
        request: 'jobsv1_pb2.SetJobInfoWithoutJobIdRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.SetJobInfoWithoutJobIdResponse':
        return self._jobs_stub.SetJobInfoWithoutJobId(request, timeout=timeout)

    def queue_job(
        self,
        request: 'jobsv1_pb2.QueueJobRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.QueueJobResponse':
        return self._jobs_stub.QueueJob(request, timeout=timeout)

    def update_status(
        self,
        request: 'jobsv1_pb2.UpdateStatusRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.UpdateStatusResponse':
        return self._jobs_stub.UpdateStatus(request, timeout=timeout)

    def get_job_queue(
        self,
        request: 'jobsv1_pb2.GetJobQueueRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.GetJobQueueResponse':
        return self._jobs_stub.GetJobQueue(request, timeout=timeout)

    def cancel_jobs(
        self,
        request: 'jobsv1_pb2.CancelJobsRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.CancelJobsResponse':
        return self._jobs_stub.CancelJobs(request, timeout=timeout)

    def fail_all_in_progress_jobs(
        self,
        request: 'jobsv1_pb2.FailAllInProgressJobsRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.FailAllInProgressJobsResponse':
        return self._jobs_stub.FailAllInProgressJobs(request, timeout=timeout)

    def get_job_status(
        self,
        request: 'jobsv1_pb2.GetJobStatusRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.GetJobStatusResponse':
        return self._jobs_stub.GetJobStatus(request, timeout=timeout)

    def get_job_submitted_timestamp(
        self,
        request: 'jobsv1_pb2.GetJobSubmittedTimestampRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.GetJobSubmittedTimestampResponse':
        return self._jobs_stub.GetJobSubmittedTimestamp(request,
                                                        timeout=timeout)

    def get_job_ended_timestamp(
        self,
        request: 'jobsv1_pb2.GetJobEndedTimestampRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.GetJobEndedTimestampResponse':
        return self._jobs_stub.GetJobEndedTimestamp(request, timeout=timeout)

    def get_log_dirs_for_jobs(
        self,
        request: 'jobsv1_pb2.GetLogDirsForJobsRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.GetLogDirsForJobsResponse':
        return self._jobs_stub.GetLogDirsForJobs(request, timeout=timeout)

    def get_job_exit_codes(
        self,
        request: 'jobsv1_pb2.GetJobExitCodesRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'jobsv1_pb2.GetJobExitCodesResponse':
        return self._jobs_stub.GetJobExitCodes(request, timeout=timeout)

    def tail_logs(
        self,
        request: 'jobsv1_pb2.TailLogsRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> Iterator['jobsv1_pb2.TailLogsResponse']:
        return self._jobs_stub.TailLogs(request, timeout=timeout)

    def get_service_status(
        self,
        request: 'servev1_pb2.GetServiceStatusRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'servev1_pb2.GetServiceStatusResponse':
        return self._serve_stub.GetServiceStatus(request, timeout=timeout)

    def add_serve_version(
        self,
        request: 'servev1_pb2.AddVersionRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'servev1_pb2.AddVersionResponse':
        return self._serve_stub.AddVersion(request, timeout=timeout)

    def terminate_services(
        self,
        request: 'servev1_pb2.TerminateServicesRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'servev1_pb2.TerminateServicesResponse':
        return self._serve_stub.TerminateServices(request, timeout=timeout)

    def terminate_replica(
        self,
        request: 'servev1_pb2.TerminateReplicaRequest',
        timeout: float |
        None = (serve_constants.TERMINATE_REPLICA_TIMEOUT_SECONDS + 10)
    ) -> 'servev1_pb2.TerminateReplicaResponse':
        # The controller acknowledges only after the replica-manager lock has
        # admitted and durably scheduled teardown. Give that acceptance wait
        # transport margin instead of preempting it at the generic 10s RPC
        # deadline.
        return self._serve_stub.TerminateReplica(request, timeout=timeout)

    def wait_service_registration(
        self,
        request: 'servev1_pb2.WaitServiceRegistrationRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'servev1_pb2.WaitServiceRegistrationResponse':
        # The skylet-side handler gives controller setup and service
        # registration independent timeout budgets. Keep the outer RPC alive
        # for both phases, with margin for polling and transport overhead.
        if timeout is not None:
            timeout = max(
                timeout, serve_constants.CONTROLLER_SETUP_TIMEOUT_SECONDS +
                serve_constants.SERVICE_REGISTER_TIMEOUT_SECONDS + 10)
        return self._serve_stub.WaitServiceRegistration(request,
                                                        timeout=timeout)

    def update_service(
        self,
        request: 'servev1_pb2.UpdateServiceRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'servev1_pb2.UpdateServiceResponse':
        # The skylet-side handler waits on the controller's replica-manager
        # lock for up to UPDATE_SERVICE_TIMEOUT_SECONDS (see
        # sky/serve/constants.py); give the outer gRPC deadline margin over
        # that, mirroring wait_service_registration above.
        if timeout is not None:
            timeout = max(timeout,
                          serve_constants.UPDATE_SERVICE_TIMEOUT_SECONDS + 10)
        return self._serve_stub.UpdateService(request, timeout=timeout)

    def get_ray_status(
        self,
        request: 'healthv1_pb2.GetRayStatusRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'healthv1_pb2.GetRayStatusResponse':
        """Run `ray status` locally on the head node via skylet.

        Replaces the SSH-exec'd ray status of the legacy health probe;
        old skylets raise UNIMPLEMENTED and the caller falls back.
        """
        return self._health_stub.GetRayStatus(request, timeout=timeout)

    def get_managed_job_controller_version(
        self,
        request: 'managed_jobsv1_pb2.GetVersionRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'managed_jobsv1_pb2.GetVersionResponse':
        return self._managed_jobs_stub.GetVersion(request, timeout=timeout)

    def get_managed_job_table(
        self,
        request: 'managed_jobsv1_pb2.GetJobTableRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'managed_jobsv1_pb2.GetJobTableResponse':
        return self._managed_jobs_stub.GetJobTable(request, timeout=timeout)

    def get_all_managed_job_ids_by_name(
        self,
        request: 'managed_jobsv1_pb2.GetAllJobIdsByNameRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'managed_jobsv1_pb2.GetAllJobIdsByNameResponse':
        return self._managed_jobs_stub.GetAllJobIdsByName(request,
                                                          timeout=timeout)

    def cancel_managed_jobs(
        self,
        request: 'managed_jobsv1_pb2.CancelJobsRequest',
        timeout: float | None = constants.SKYLET_GRPC_TIMEOUT_SECONDS
    ) -> 'managed_jobsv1_pb2.CancelJobsResponse':
        return self._managed_jobs_stub.CancelJobs(request, timeout=timeout)


# Preserve reflection and pickle compatibility with the historical facade.
_CancelAwareStub.__module__ = 'sky.backends.cloud_vm_ray_backend'
SkyletClient.__module__ = 'sky.backends.cloud_vm_ray_backend'
