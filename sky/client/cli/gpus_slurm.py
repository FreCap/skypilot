"""Slurm GPU availability transport and presentation for the GPU CLI."""

import collections
from collections.abc import Callable
from collections.abc import Generator
import typing
from typing import Optional

import colorama

from sky import models
from sky import sky_logging
from sky.client import sdk
from sky.utils import common_utils
from sky.utils import log_utils

if typing.TYPE_CHECKING:
    import prettytable

logger = sky_logging.init_logger('sky.client.cli.command')


def get_realtime_gpu_tables(
    list_to_str: Callable[[list[int | float]], str],
    name_filter: str | None = None,
    quantity_filter: int | None = None,
    slurm_cluster_name: str | None = None,
) -> tuple[list[tuple[str, 'prettytable.PrettyTable']],
           Optional['prettytable.PrettyTable'], list[tuple[str, str]]]:
    """Get Slurm GPU availability tables.

    Args:
        list_to_str: Formatter for requestable GPU quantities.
        name_filter: Filter GPUs by name.
        quantity_filter: Filter GPUs by quantity.

    Returns:
        A tuple of (realtime_gpu_infos, total_realtime_gpu_table,
        failed_infos).
    """
    if quantity_filter:
        qty_header = 'QTY_FILTER'
    else:
        qty_header = 'REQUESTABLE_QTY_PER_NODE'

    realtime_gpu_availability_lists = sdk.stream_and_get(
        sdk.realtime_slurm_gpu_availability(
            name_filter=name_filter,
            quantity_filter=quantity_filter,
            slurm_cluster_name=slurm_cluster_name))
    if not realtime_gpu_availability_lists:
        err_msg = 'No GPUs found in any Slurm partition. '
        debug_msg = 'To further debug, run: sky check slurm '
        if name_filter is not None:
            gpu_info_msg = f' {name_filter!r}'
            if quantity_filter is not None:
                gpu_info_msg += (' with requested quantity'
                                 f' {quantity_filter}')
            err_msg = (f'Resources{gpu_info_msg} not found '
                       'in any Slurm partition. ')
            debug_msg = ('To show available accelerators on Slurm,'
                         ' run: sky gpus list --cloud slurm ')
        raise ValueError(err_msg + debug_msg)

    realtime_gpu_infos = []
    failed_infos: list[tuple[str, str]] = []
    total_gpu_info: dict[str,
                         list[int]] = collections.defaultdict(lambda: [0, 0])

    for entry in realtime_gpu_availability_lists:
        # Handle both 2-element (old server) and 3-element (new server)
        # tuples for backward compatibility.
        # TODO(kevin): remove this in v0.13.0
        if len(entry) == 3:
            slurm_cluster, availability_list, error = entry
        else:
            slurm_cluster, availability_list = entry
            error = None

        if error is not None:
            failed_infos.append((slurm_cluster, error))
            continue

        realtime_gpu_table = log_utils.create_table(
            ['GPU', qty_header, 'UTILIZATION'])
        for realtime_gpu_availability in sorted(availability_list):
            gpu_availability = models.RealtimeGpuAvailability(
                *realtime_gpu_availability)
            # Use the counts directly from the backend, which are already
            # generated in powers of 2 (plus any actual maximums)
            requestable_quantities = gpu_availability.counts
            realtime_gpu_table.add_row([
                gpu_availability.gpu,
                list_to_str(requestable_quantities),
                (f'{gpu_availability.available} of '
                 f'{gpu_availability.capacity} free'),
            ])
            gpu = gpu_availability.gpu
            capacity = gpu_availability.capacity
            available = gpu_availability.available
            if capacity > 0:
                total_gpu_info[gpu][0] += capacity
                total_gpu_info[gpu][1] += available
        realtime_gpu_infos.append((slurm_cluster, realtime_gpu_table))

    # Display an aggregated table for all partitions if there are more than
    # one partitions with GPUs.
    if len(realtime_gpu_infos) > 1:
        total_realtime_gpu_table = log_utils.create_table(
            ['GPU', 'UTILIZATION'])
        for gpu, stats in total_gpu_info.items():
            total_realtime_gpu_table.add_row(
                [gpu, f'{stats[1]} of {stats[0]} free'])
    else:
        total_realtime_gpu_table = None

    return realtime_gpu_infos, total_realtime_gpu_table, failed_infos


def _format_partition_info(slurm_cluster_names: list[str]) -> str:
    partition_table = log_utils.create_table([
        'CLUSTER',
        'PARTITION',
        'GPU',
        'UTILIZATION',
    ])

    # TODO(kevin): Create a new endpoint that returns per-partition info.
    request_ids = [(cluster_name,
                    sdk.slurm_node_info(slurm_cluster_name=cluster_name))
                   for cluster_name in slurm_cluster_names]

    failed_clusters = []
    # Aggregate GPU counts by (cluster, partition, gpu_type).
    # Each value is [total_gpus, free_gpus].
    gpu_counts: dict[tuple[str, str, str],
                     list[int]] = collections.defaultdict(lambda: [0, 0])
    for cluster_name, request_id in request_ids:
        try:
            nodes_info = sdk.stream_and_get(request_id)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to get partition info for '
                           f'Slurm cluster {cluster_name!r}: '
                           f'{common_utils.format_exception(e)}')
            failed_clusters.append(cluster_name)
            continue

        for node_info in nodes_info:
            gpu_type = node_info.get('gpu_type') or ''
            total = node_info.get('total_gpus', 0)
            free = node_info.get('free_gpus', 0)
            partitions = node_info.get('partition', '').split(',')
            for partition in partitions:
                key = (cluster_name, partition.strip(), gpu_type)
                gpu_counts[key][0] += total
                gpu_counts[key][1] += free

    for key in sorted(gpu_counts):
        cluster_name, partition, gpu_type = key
        total, free = gpu_counts[key]
        partition_table.add_row([
            cluster_name,
            partition,
            gpu_type,
            f'{free} of {total} free',
        ])

    slurm_per_partition_msg = 'Slurm per-partition accelerator availability'
    if failed_clusters:
        slurm_per_partition_msg += (f' (skipped unreachable clusters: '
                                    f'{", ".join(failed_clusters)})')

    return (f'{colorama.Fore.LIGHTMAGENTA_EX}{colorama.Style.NORMAL}'
            f'{slurm_per_partition_msg}'
            f'{colorama.Style.RESET_ALL}\n'
            f'{partition_table.get_string()}')


def format_realtime_gpu(
    total_table: Optional['prettytable.PrettyTable'],
    slurm_realtime_infos: list[tuple[str, 'prettytable.PrettyTable']],
    failed_infos: list[tuple[str, str]],
    show_node_info: bool,
) -> Generator[str, None, None]:
    """Yield the complete Slurm GPU availability section."""
    yield (f'{colorama.Fore.GREEN}{colorama.Style.BRIGHT}'
           'Slurm GPUs'
           f'{colorama.Style.RESET_ALL}\n')
    if total_table is not None:
        yield from total_table.get_string()
        yield '\n'

    for partition, slurm_realtime_table in slurm_realtime_infos:
        partition_str = f'Slurm Cluster: {partition}'
        yield (f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
               f'{partition_str}'
               f'{colorama.Style.RESET_ALL}\n')
        yield from slurm_realtime_table.get_string()
        yield '\n'

    for cluster_name, error_msg in failed_infos:
        yield (f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
               f'Slurm Cluster: {cluster_name}'
               f'{colorama.Style.RESET_ALL}\n')
        yield (f'{colorama.Fore.YELLOW}'
               f'Error: {error_msg}'
               f'{colorama.Style.RESET_ALL}\n')

    if show_node_info and slurm_realtime_infos:
        cluster_names = [cluster for cluster, _ in slurm_realtime_infos]
        yield _format_partition_info(cluster_names)
