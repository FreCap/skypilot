"""skylet: a daemon running on the head node of a cluster."""

import argparse
import concurrent.futures
import os
import signal
import sys
import threading
import time
import uuid

import grpc

import sky
from sky import sky_logging
from sky.jobs import constants as managed_job_constants
from sky.jobs import controller_slots
from sky.schemas.generated import autostopv1_pb2_grpc
from sky.schemas.generated import healthv1_pb2_grpc
from sky.schemas.generated import jobsv1_pb2_grpc
from sky.schemas.generated import managed_jobsv1_pb2_grpc
from sky.schemas.generated import servev1_pb2_grpc
from sky.schemas.generated import skyletv1_pb2_grpc
from sky.skylet import autostop_lib
from sky.skylet import constants
from sky.skylet import events
from sky.skylet import hook_executor
from sky.skylet import services

# Use the explicit logger name so that the logger is under the
# `sky.skylet.skylet` namespace when executed directly, so as
# to inherit the setup from the `sky` logger.
logger = sky_logging.init_logger('sky.skylet.skylet')
logger.info(f'Skylet started with version {constants.SKYLET_VERSION}; '
            f'SkyPilot v{sky.__version__} (commit: {sky.__commit__})')

EVENTS = [
    events.StopEvent(),
    events.JobSchedulerEvent(),
    # The managed job update event should be after the job update event.
    # Otherwise, the abnormal managed job status update will be delayed
    # until the next job update event.
    events.ManagedJobEvent(),
    # This is for monitoring controller job status. If it becomes
    # unhealthy, this event will correctly update the controller
    # status to CONTROLLER_FAILED.
    events.ServiceUpdateEvent(pool=False),
    # Status refresh for pool.
    events.ServiceUpdateEvent(pool=True),
    # Report usage heartbeat every 10 minutes.
    events.UsageHeartbeatReportEvent(),
]

_MANAGED_JOB_RUNTIME_POLL_SECONDS = 1.0


class _RemoteManagedJobControllerRuntime:
    """Marker-gated fixed-slot runtime owned by this exact Skylet birth."""

    def __init__(self) -> None:
        self._failure = threading.Event()
        self._runtime: controller_slots.LocalManagedJobControllerRuntime | None = (
            None)

    @property
    def started(self) -> bool:
        return self._runtime is not None and self._runtime.started

    def _on_failure(self) -> None:
        # The slot monitor cannot raise on the main Skylet thread. Wake its
        # bounded poll so that thread observes the exact failure and enters the
        # normal subsystem-specific drain path.
        self._failure.set()

    def start_if_configured(self) -> None:
        """Start once the provisioned jobs-controller marker is visible."""
        if self._runtime is not None:
            self._runtime.raise_if_failed()
            return
        indicator_path = os.path.expanduser(
            managed_job_constants.JOB_CONTROLLER_INDICATOR_FILE)
        if not os.path.exists(indicator_path):
            return
        runtime = controller_slots.LocalManagedJobControllerRuntime(
            on_failure=self._on_failure)
        # Publish the handle before startup. A partial-admission failure must
        # remain reachable by finally and retain its owner until all family
        # proofs converge.
        self._runtime = runtime
        runtime.start()
        runtime.raise_if_failed()
        logger.info('Remote jobs-controller fixed-slot runtime is ready.')

    def wait(self, timeout: float) -> None:
        self._failure.wait(timeout)
        self.raise_if_failed()

    def raise_if_failed(self) -> None:
        if self._runtime is not None:
            self._runtime.raise_if_failed()

    def request_shutdown(self) -> None:
        if self._runtime is not None:
            self._runtime.request_shutdown()

    def wait_for_shutdown(self) -> None:
        if self._runtime is not None:
            self._runtime.wait_for_shutdown()


def start_grpc_server(port: int = constants.SKYLET_GRPC_PORT) -> grpc.Server:
    """Start the gRPC server."""
    # This is the default value in Python 3.9 - 3.12,
    # putting it here for visibility.
    # TODO(kevin): Determine the optimal max number of threads.
    max_workers = min(32, (os.cpu_count() or 1) + 4)
    # There's only a single skylet process per cluster, so disable
    # SO_REUSEPORT to raise an error if the port is already in use.
    options = (('grpc.so_reuseport', 0),)
    server = grpc.server(
        concurrent.futures.ThreadPoolExecutor(max_workers=max_workers),
        options=options)

    autostopv1_pb2_grpc.add_AutostopServiceServicer_to_server(
        services.AutostopServiceImpl(), server)
    jobsv1_pb2_grpc.add_JobsServiceServicer_to_server(
        services.JobsServiceImpl(), server)
    servev1_pb2_grpc.add_ServeServiceServicer_to_server(
        services.ServeServiceImpl(), server)
    managed_jobsv1_pb2_grpc.add_ManagedJobsServiceServicer_to_server(
        services.ManagedJobsServiceImpl(), server)
    healthv1_pb2_grpc.add_HealthServiceServicer_to_server(
        services.HealthServiceImpl(), server)
    skyletv1_pb2_grpc.add_CapabilitiesServiceServicer_to_server(
        services.CapabilitiesServiceImpl(str(uuid.uuid4())), server)

    listen_addr = f'0.0.0.0:{port}'
    server.add_insecure_port(listen_addr)

    server.start()
    logger.info(f'gRPC server started on {listen_addr}')

    return server


def run_event_loop(managed_job_runtime: _RemoteManagedJobControllerRuntime |
                   None = None):
    """Run the existing event loop."""

    if managed_job_runtime is None:
        managed_job_runtime = _RemoteManagedJobControllerRuntime()

    for event in EVENTS:
        event.start()

    next_event_at = time.monotonic() + events.EVENT_CHECKING_INTERVAL_SECONDS
    while True:
        # Skylet starts before a newly provisioned controller's user setup
        # creates its marker. Poll that one-way configuration fact separately
        # from the 20-second maintenance cadence so fixed slots are admitted
        # eagerly and never depend on submit_jobs() or a PID inventory.
        managed_job_runtime.start_if_configured()
        delay = max(
            0.0,
            min(_MANAGED_JOB_RUNTIME_POLL_SECONDS,
                next_event_at - time.monotonic()))
        managed_job_runtime.wait(delay)
        if time.monotonic() < next_event_at:
            continue
        next_event_at = (time.monotonic() +
                         events.EVENT_CHECKING_INTERVAL_SECONDS)
        for event in EVENTS:
            # The consolidated API runtime calls ManagedJobEvent through its
            # own refresh owner. Inside Skylet, only the separate controller
            # runtime may scan and reconcile managed jobs.
            if (isinstance(event, events.ManagedJobEvent) and
                    not managed_job_runtime.started):
                continue
            event.run()


def _sigterm_handler(signum, frame):  # pylint: disable=unused-argument
    """Run preemption hooks on SIGTERM before the pod is SIGKILLed.

    On Kubernetes the kubelet sends SIGTERM for preemption / eviction
    / drain. We claim the preemption teardown slot via the file-lock
    CAS so a concurrent `sky down` subprocess sees the claim and
    skips its own hooks, then run any matching preemption hooks, then
    exit cleanly within the pod's terminationGracePeriodSeconds.
    """
    logger.info('Skylet received SIGTERM; running preemption hooks.')
    if hook_executor.try_claim_teardown(hook_executor.PREEMPTION):
        try:
            hook_executor.run(hook_executor.PREEMPTION,
                              autostop_lib.get_hooks())
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Preemption hook execution failed: {e}')
    sys.exit(0)


def _should_install_preemption_sigterm_handler() -> bool:
    """True iff the skylet is running inside a Kubernetes pod.

    SIGTERM-driven preemption handling is K8s-specific: kubelet sends
    SIGTERM to pod containers on delete / scale-down / eviction. On VM
    clouds (AWS/GCP/Azure), preemption is detected via the metadata
    poller (introduced in PR2), so installing a SIGTERM handler here
    would be dead code and could mask normal-shutdown signal handling.

    Detection uses the standard ``KUBERNETES_SERVICE_HOST`` env var
    that the kubelet injects into every pod.
    """
    return 'KUBERNETES_SERVICE_HOST' in os.environ


def main():
    parser = argparse.ArgumentParser(description='Start skylet daemon')
    parser.add_argument('--port',
                        type=int,
                        default=constants.SKYLET_GRPC_PORT,
                        help=f'gRPC port to listen on (default: '
                        f'{constants.SKYLET_GRPC_PORT})')
    args = parser.parse_args()

    # Clear any stale teardown-claim marker from a prior crashed skylet so
    # this fresh boot does not see a blocked slot.
    hook_executor.clear_teardown_claim()
    if _should_install_preemption_sigterm_handler():
        signal.signal(signal.SIGTERM, _sigterm_handler)

    managed_job_runtime = _RemoteManagedJobControllerRuntime()
    grpc_server = None
    try:
        # Existing jobs-controller markers are handled before the Skylet opens
        # its RPC surface. On first provisioning the marker is created later
        # by setup and the bounded event-loop poll performs the same startup.
        managed_job_runtime.start_if_configured()
        grpc_server = start_grpc_server(port=args.port)
        run_event_loop(managed_job_runtime)
    except KeyboardInterrupt:
        logger.info('Shutting down skylet...')
    finally:
        try:
            if grpc_server is not None:
                grpc_server.stop(grace=5)
        finally:
            managed_job_runtime.request_shutdown()
            # The local owner is cleared only after every exact guardian proof
            # has been accepted. A failed proof intentionally propagates and
            # leaves the owner published while this process remains alive.
            managed_job_runtime.wait_for_shutdown()


if __name__ == '__main__':
    main()
