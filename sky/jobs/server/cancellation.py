"""Managed jobs cancellation transport gateway."""
import typing

from sky import backends
from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.jobs import runner as managed_job_runner
from sky.usage import usage_lib
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky.schemas.generated import managed_jobsv1_pb2
else:
    managed_jobsv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.managed_jobsv1_pb2')

# Preserve the historical logger name used before this implementation moved
# behind sky.jobs.server.core.cancel.
logger = sky_logging.init_logger('sky.jobs.server.core')

_CANCEL_TRANSPORT_UPGRADE_HINT = (
    'Graceful managed job cancellation requires a jobs controller with the gRPC '
    '`cancel_managed_jobs` endpoint. Please upgrade the jobs controller and '
    'retry.')


def _requires_modern_cancel_transport(graceful: bool,
                                      graceful_timeout: int | None) -> bool:
    return graceful or graceful_timeout is not None


def _build_cancel_request(
    *,
    current_workspace: str | None,
    all_users: bool,
    all: bool,  # pylint: disable=redefined-builtin
    job_ids: list[int] | None,
    name: str | None,
    pool: str | None,
    graceful: bool,
    graceful_timeout: int | None,
) -> 'managed_jobsv1_pb2.CancelJobsRequest':
    request = managed_jobsv1_pb2.CancelJobsRequest(
        current_workspace=current_workspace,
        graceful=graceful,
        graceful_timeout=graceful_timeout)
    if all_users or all or job_ids:
        request.all_users = all_users
        if all:
            request.user_hash = common_utils.get_user_hash()
        if job_ids is not None:
            request.job_ids.CopyFrom(managed_jobsv1_pb2.JobIds(ids=job_ids))
    elif name is not None:
        request.job_name = name
    else:
        assert pool is not None, (job_ids, name, pool, all)
        request.pool_name = pool
    return request


@usage_lib.entrypoint
# pylint: disable=redefined-builtin
def cancel(name: str | None = None,
           job_ids: list[int] | None = None,
           all: bool = False,
           all_users: bool = False,
           pool: str | None = None,
           graceful: bool = False,
           graceful_timeout: int | None = None) -> None:
    # NOTE(dev): Keep the docstring consistent between the Python API and CLI.
    """Cancels managed jobs.

    Please refer to sky.cli.job_cancel for documentation.

    Raises:
        sky.exceptions.ClusterNotUpError: the jobs controller is not up.
        RuntimeError: failed to cancel the job.
    """
    with rich_utils.safe_status(
            ux_utils.spinner_message('Cancelling managed jobs')):
        job_ids = [] if job_ids is None else job_ids
        handle = backend_utils.is_controller_accessible(
            controller=controller_utils.Controllers.JOBS_CONTROLLER,
            stopped_message='All managed jobs should have finished.')

        job_id_str = ','.join(map(str, job_ids))
        if sum([
                bool(job_ids), name is not None, pool is not None, all or
                all_users
        ]) != 1:
            arguments = []
            arguments += [f'job_ids={job_id_str}'] if job_ids else []
            arguments += [f'name={name}'] if name is not None else []
            arguments += [f'pool={pool}'] if pool is not None else []
            arguments += ['all'] if all else []
            arguments += ['all_users'] if all_users else []
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    'Can only specify one of JOB_IDS, name, pool, or all/'
                    f'all_users. Provided {" ".join(arguments)!r}.')

        job_ids = None if (all_users or all) else job_ids

        requires_modern_transport = _requires_modern_cancel_transport(
            graceful, graceful_timeout)
        if requires_modern_transport and not handle.is_grpc_enabled_with_flag:
            raise exceptions.NotSupportedError(_CANCEL_TRANSPORT_UPGRADE_HINT)

        backend = backend_utils.get_backend_from_handle(handle)
        assert isinstance(backend, backends.CloudVmRayBackend)
        use_legacy = not handle.is_grpc_enabled_with_flag
        stdout = None
        if not use_legacy:
            request = _build_cancel_request(
                current_workspace=skypilot_config.get_active_workspace(),
                all_users=all_users,
                all=all,
                job_ids=job_ids,
                name=name,
                pool=pool,
                graceful=graceful,
                graceful_timeout=graceful_timeout)
            try:
                response = backend_utils.invoke_skylet_with_retries(
                    lambda: cloud_vm_ray_backend.SkyletClient(
                        handle.get_grpc_channel()).cancel_managed_jobs(request))
                stdout = response.message
            except exceptions.SkyletMethodNotImplementedError as e:
                if requires_modern_transport:
                    raise exceptions.NotSupportedError(
                        _CANCEL_TRANSPORT_UPGRADE_HINT) from e
                use_legacy = True

        if use_legacy:
            stdout = managed_job_runner.current().cancel_managed_jobs(
                handle=handle,
                backend=backend,
                all_users=all_users,
                all=all,
                job_ids=job_ids,
                name=name,
                pool=pool,
                graceful=graceful,
                graceful_timeout=graceful_timeout,
            )

        if stdout is None:
            raise RuntimeError('Managed job cancellation produced no output.')
        logger.info(stdout)
        if 'Multiple jobs found with name' in stdout:
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError(
                    'Please specify the job ID instead of the job name.')
