"""Real-time accelerator availability across fixed-size clusters."""

from sky import catalog
from sky import clouds
from sky import models
from sky import sky_logging
from sky.provision.kubernetes import constants as kubernetes_constants
from sky.utils import common_utils
from sky.utils import subprocess_utils

logger = sky_logging.init_logger(__name__)


def realtime_kubernetes_gpu_availability(
    context: str | None = None,
    name_filter: str | None = None,
    quantity_filter: int | None = None,
    is_ssh: bool | None = None
) -> list[tuple[str, list[models.RealtimeGpuAvailability]]]:
    """Gets real-time Kubernetes or SSH GPU availability."""
    if context is None:
        # Include contexts from both Kubernetes and SSH clouds
        kubernetes_contexts = clouds.Kubernetes.existing_allowed_contexts()
        ssh_contexts = clouds.SSH.existing_allowed_contexts()
        if is_ssh is None:
            context_list = kubernetes_contexts + ssh_contexts
        elif is_ssh:
            context_list = ssh_contexts
        else:
            context_list = kubernetes_contexts
    else:
        context_list = [context]

    def _realtime_kubernetes_gpu_availability_single(
        context: str | None = None,
        name_filter: str | None = None,
        quantity_filter: int | None = None
    ) -> list[models.RealtimeGpuAvailability]:
        counts, capacity, available = catalog.list_accelerator_realtime(
            gpus_only=True,
            clouds='ssh' if is_ssh else 'kubernetes',
            name_filter=name_filter,
            region_filter=context,
            quantity_filter=quantity_filter,
            case_sensitive=False)

        all_keys = set(counts.keys()) | set(capacity.keys()) | set(
            available.keys())
        counts = {key: counts.get(key, []) for key in all_keys}
        capacity = {key: capacity.get(key, 0) for key in all_keys}
        available = {key: available.get(key, 0) for key in all_keys}

        realtime_gpu_availability_list: list[
            models.RealtimeGpuAvailability] = []

        for gpu, _ in sorted(counts.items()):
            realtime_gpu_availability_list.append(
                models.RealtimeGpuAvailability(
                    gpu,
                    counts.pop(gpu),
                    capacity[gpu],
                    available[gpu],
                ))
        return realtime_gpu_availability_list

    availability_lists: list[tuple[str,
                                   list[models.RealtimeGpuAvailability]]] = []
    cumulative_count = 0
    parallel_queried = subprocess_utils.run_in_parallel(
        lambda ctx: _realtime_kubernetes_gpu_availability_single(
            context=ctx,
            name_filter=name_filter,
            quantity_filter=quantity_filter), context_list)

    cloud_identity = 'ssh' if is_ssh else 'kubernetes'
    cloud_identity_capital = 'SSH' if is_ssh else 'Kubernetes'

    for ctx, queried in zip(context_list, parallel_queried):
        cumulative_count += len(queried)
        if len(queried) == 0:
            # don't add gpu results for clusters that don't have any
            logger.debug(f'No gpus found in {cloud_identity} cluster {ctx}')
            continue
        availability_lists.append((ctx, queried))

    if cumulative_count == 0:
        err_msg = f'No GPUs found in any {cloud_identity_capital} clusters. '
        debug_msg = 'To further debug, run: sky check '
        if name_filter is not None:
            gpu_info_msg = f' {name_filter!r}'
            if quantity_filter is not None:
                gpu_info_msg += (' with requested quantity'
                                 f' {quantity_filter}')
            err_msg = (f'Resources{gpu_info_msg} not found '
                       f'in {cloud_identity_capital} clusters. ')
            debug_msg = (f'To show available accelerators on {cloud_identity}, '
                         f' run: sky gpus list --cloud {cloud_identity} ')
        full_err_msg = (err_msg + kubernetes_constants.NO_GPU_HELP_MESSAGE +
                        debug_msg)
        raise ValueError(full_err_msg)
    return availability_lists


def realtime_slurm_gpu_availability(
    slurm_cluster_name: str | None = None,
    name_filter: str | None = None,
    quantity_filter: int | None = None,
    env_vars: dict[str, str] | None = None,
    **kwargs,
) -> list[tuple[str, list[models.RealtimeGpuAvailability], str | None]]:
    """Gets real-time Slurm GPU availability grouped by partition."""
    del env_vars, kwargs  # Currently unused

    if slurm_cluster_name is None:
        slurm_cluster_names = clouds.Slurm.existing_allowed_clusters()
    else:
        slurm_cluster_names = [slurm_cluster_name]

    def realtime_slurm_gpu_availability_single(
        slurm_cluster_name: str,
    ) -> tuple[list[models.RealtimeGpuAvailability], str | None]:
        try:
            # This function returns aggregated data per GPU type:
            # (qtys_map, total_capacity, total_available).
            accelerator_counts, total_capacity, total_available = (
                catalog.list_accelerator_realtime(
                    gpus_only=True,
                    name_filter=name_filter,
                    region_filter=slurm_cluster_name,
                    quantity_filter=quantity_filter,
                    clouds='slurm',
                    case_sensitive=False,
                ))
        except ValueError as e:
            # No GPUs found matching the filters for this cluster
            logger.debug(f'No matching GPUs in Slurm cluster '
                         f'{slurm_cluster_name!r}: '
                         f'{common_utils.format_exception(e)}')
            return [], None
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(
                f'Error querying Slurm cluster {slurm_cluster_name!r}: '
                f'{common_utils.format_exception(e, use_bracket=True)}')
            return [], (f'Could not query Slurm cluster for info: '
                        f'{common_utils.format_exception(e)}')

        realtime_gpu_availability_list: list[
            models.RealtimeGpuAvailability] = []
        for gpu_type, _ in sorted(accelerator_counts.items()):
            realtime_gpu_availability_list.append(
                models.RealtimeGpuAvailability(
                    gpu_type,
                    accelerator_counts.pop(gpu_type),
                    total_capacity[gpu_type],
                    total_available[gpu_type],
                ))
        return realtime_gpu_availability_list, None

    parallel_queried = subprocess_utils.run_in_parallel(
        realtime_slurm_gpu_availability_single, slurm_cluster_names)
    availability_lists: list[tuple[str, list[models.RealtimeGpuAvailability],
                                   str | None]] = []
    for name, (queried, error) in zip(slurm_cluster_names, parallel_queried):
        if len(queried) == 0 and error is None:
            logger.debug(f'No gpus found in Slurm cluster {name}')
            continue
        availability_lists.append((name, queried, error))
    return availability_lists
