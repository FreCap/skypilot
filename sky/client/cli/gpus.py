"""Commands for inspecting and labeling GPU accelerators."""

import collections
from collections.abc import Generator
import json
import sys
import typing
from typing import Optional

import click
import colorama

from sky import catalog
from sky import clouds
from sky import models
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import common as adaptors_common
from sky.client import sdk
from sky.client.cli import click_utils
from sky.client.cli import deprecation_utils
from sky.client.cli import flags
from sky.client.cli import utils as cli_utils
from sky.provision.kubernetes import constants as kubernetes_constants
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.skylet import constants
from sky.usage import usage_lib
from sky.utils import common_utils
from sky.utils import log_utils
from sky.utils import registry

if typing.TYPE_CHECKING:
    import prettytable

pd = adaptors_common.LazyImport('pandas')
# Keep warning provenance stable for users and log collectors while the
# implementation lives behind the command module facade.
logger = sky_logging.init_logger('sky.client.cli.command')
_handle_infra_cloud_region_zone_options = (
    cli_utils.handle_infra_cloud_region_zone_options)


@click.command()
@flags.config_option(expose_value=False)
@click.argument('accelerator_str', required=False)
@flags.all_option('Show details of all GPU/TPU/accelerator offerings.')
@click.option('--infra',
              default=None,
              type=str,
              help='Infrastructure to query. Examples: "aws", "aws/us-east-1"')
@click.option('--cloud',
              default=None,
              type=str,
              help='Cloud provider to query.',
              hidden=True)
@click.option(
    '--region',
    required=False,
    type=str,
    help=
    ('The region to use. If not specified, shows accelerators from all regions.'
    ),
    hidden=True,
)
@click.option(
    '--all-regions',
    is_flag=True,
    default=False,
    help='Show pricing and instance details for a specified accelerator across '
    'all regions and clouds.')
@flags.verbose_option()
@catalog.fallback_to_default_catalog
@usage_lib.entrypoint
def show_gpus(
        accelerator_str: str | None,
        all: bool,  # pylint: disable=redefined-builtin
        infra: str | None,
        cloud: str | None,
        region: str | None,
        all_regions: bool,
        verbose: bool):
    """Show supported GPU/TPU/accelerators and their prices.

    NOTE: This command is deprecated. Use ``sky gpus list`` instead.

    The names and counts shown can be set in the ``accelerators`` field in task
    YAMLs, or in the ``--gpus`` flag in CLI commands. For example, if this
    table shows 8x V100s are supported, then the string ``V100:8`` will be
    accepted by the above.

    To show the detailed information of a GPU/TPU type (its price, which clouds
    offer it, the quantity in each VM type, etc.), use ``sky gpus list <gpu>``.

    To show all accelerators, including less common ones and their detailed
    information, use ``sky gpus list --all``.

    To show all regions for a specified accelerator, use
    ``sky gpus list <accelerator> --all-regions``.

    If ``--region`` or ``--all-regions`` is not specified, the price displayed
    for each instance type is the lowest across all regions for both on-demand
    and spot instances. There may be multiple regions with the same lowest
    price.

    If ``--cloud kubernetes`` or ``--cloud k8s`` is specified, it will show the
    maximum quantities of the GPU available on a single node and the real-time
    availability of the GPU across all nodes in the Kubernetes cluster.

    If ``--cloud slurm`` is specified, it will show the maximum quantities of
    the GPU available on a single node and the real-time availability of the
    GPU across all nodes in the Slurm cluster. Use ``-v`` to show per-partition
    accelerator details.

    Definitions of certain fields:

    * ``DEVICE_MEM``: Memory of a single device; does not depend on the device
      count of the instance (VM).

    * ``HOST_MEM``: Memory of the host instance (VM).

    * ``QTY_PER_NODE`` (Kubernetes only): GPU quantities that can be requested
      on a single node.

    * ``UTILIZATION`` (Kubernetes only): Total number of GPUs free / available
      in the Kubernetes cluster.
    """
    # Call the shared implementation
    _show_gpus_impl(accelerator_str,
                    all,
                    infra,
                    cloud,
                    region,
                    all_regions,
                    verbose=verbose)


def _show_gpus_impl(
        accelerator_str: str | None,
        all: bool,  # pylint: disable=redefined-builtin
        infra: str | None,
        cloud: str | None,
        region: str | None,
        all_regions: bool,
        verbose: bool = False,
        output_format: str = 'table'):
    """Shared implementation for show_gpus and gpus_list commands."""
    cloud, region, _ = _handle_infra_cloud_region_zone_options(infra,
                                                               cloud,
                                                               region,
                                                               zone=None)

    # cloud and region could be '*' from _handle_infra_cloud_region_zone_options
    # which normally indicates to
    # _make_task_or_dag_from_entrypoint_with_overrides -> _parse_override_params
    # to disregard the cloud and region from the YAML.
    # In show_gpus, there is no YAML, so we need to handle the '*' value
    # directly here. We should use None instead to indicate "any".
    if cloud == '*':
        cloud = None
    if region == '*':
        region = None

    # validation for the --region flag
    if region is not None and cloud is None:
        raise click.UsageError(
            'The --region flag is only valid when the --cloud flag is set.')

    # validation for the --all-regions flag
    if all_regions and accelerator_str is None:
        raise click.UsageError(
            'The --all-regions flag is only valid when an accelerator '
            'is specified.')
    if all_regions and region is not None:
        raise click.UsageError(
            '--all-regions and --region flags cannot be used simultaneously.')

    # This will validate 'cloud' and raise if not found.
    cloud_obj = registry.CLOUD_REGISTRY.from_str(cloud)
    cloud_name = str(cloud_obj).lower() if cloud is not None else None
    show_all = all
    if show_all and accelerator_str is not None:
        raise click.UsageError('--all is only allowed without a GPU name.')

    # Kubernetes specific bools
    enabled_clouds = sdk.get(sdk.enabled_clouds())
    cloud_is_kubernetes = isinstance(
        cloud_obj, clouds.Kubernetes) and not isinstance(cloud_obj, clouds.SSH)
    cloud_is_ssh = isinstance(cloud_obj, clouds.SSH)
    cloud_is_slurm = isinstance(cloud_obj, clouds.Slurm)

    # TODO(romilb): We should move this to the backend.
    kubernetes_autoscaling = skypilot_config.get_effective_region_config(
        cloud='kubernetes',
        region=region,
        keys=('autoscaler',),
        default_value=None) is not None
    kubernetes_is_enabled = clouds.Kubernetes.canonical_name() in enabled_clouds
    ssh_is_enabled = clouds.SSH.canonical_name() in enabled_clouds
    slurm_is_enabled = clouds.Slurm.canonical_name() in enabled_clouds
    query_k8s_realtime_gpu = (kubernetes_is_enabled and
                              (cloud_name is None or cloud_is_kubernetes))
    query_ssh_realtime_gpu = (ssh_is_enabled and
                              (cloud_name is None or cloud_is_ssh))

    if output_format == flags.OUTPUT_FORMAT_JSON:
        name, quantity = None, None
        if accelerator_str is not None:
            parts = accelerator_str.split(':')
            name = parts[0]
            if len(parts) == 2:
                quantity = int(parts[1])
        result = sdk.stream_and_get(
            sdk.list_accelerators(gpus_only=True,
                                  name_filter=name,
                                  quantity_filter=quantity,
                                  region_filter=region,
                                  clouds=cloud_name,
                                  case_sensitive=False,
                                  all_regions=all_regions))
        json_result = {
            gpu: [item._asdict() for item in items]
            for gpu, items in result.items()
        }
        click.echo(json.dumps(json_result, indent=2))
        return

    def _list_to_str(lst):

        def format_number(n):
            # If it's a float that's a whole number, display as int
            if isinstance(n, float) and n.is_integer():
                return str(int(n))
            return str(n)

        return ', '.join([format_number(n) for n in lst])

    # TODO(zhwu,romilb): We should move most of these kubernetes related
    # queries into the backend, especially behind the server.
    def _get_kubernetes_realtime_gpu_tables(
        context: str | None = None,
        name_filter: str | None = None,
        quantity_filter: int | None = None,
        is_ssh: bool = False,
    ) -> tuple[list[tuple[str, 'prettytable.PrettyTable']],
               Optional['prettytable.PrettyTable'], list[tuple[
                   str, 'models.KubernetesNodesInfo']]]:
        if quantity_filter:
            qty_header = 'QTY_FILTER'
        else:
            qty_header = 'REQUESTABLE_QTY_PER_NODE'

        realtime_gpu_availability_lists = sdk.stream_and_get(
            sdk.realtime_kubernetes_gpu_availability(
                context=context,
                name_filter=name_filter,
                quantity_filter=quantity_filter,
                is_ssh=is_ssh))
        if not realtime_gpu_availability_lists:
            # Customize message based on context
            identity = ('SSH Node Pool'
                        if is_ssh else 'any allowed Kubernetes cluster')
            cloud_name = 'ssh' if is_ssh else 'kubernetes'
            err_msg = f'No GPUs found in {identity}. '
            debug_msg = (f'To further debug, run: sky check {cloud_name}')
            if name_filter is not None:
                gpu_info_msg = f' {name_filter!r}'
                if quantity_filter is not None:
                    gpu_info_msg += (' with requested quantity'
                                     f' {quantity_filter}')
                err_msg = (f'Resources{gpu_info_msg} not found '
                           f'in {identity}. ')
                identity_short = 'SSH Node Pool' if is_ssh else 'Kubernetes'
                debug_msg = (
                    f'To show available accelerators in {identity_short}, '
                    f'run: sky gpus list --cloud {cloud_name}')
            full_err_msg = (err_msg + kubernetes_constants.NO_GPU_HELP_MESSAGE +
                            debug_msg)
            raise ValueError(full_err_msg)
        no_permissions_str = '<no permissions>'
        realtime_gpu_infos = []
        # Stores per-GPU totals as [ready_capacity, available, not_ready].
        total_gpu_info: dict[str, list[int]] = collections.defaultdict(
            lambda: [0, 0, 0])
        all_nodes_info = []

        # display an aggregated table for all contexts
        # if there are more than one contexts with GPUs.
        def _filter_ctx(ctx: str) -> bool:
            ctx_is_ssh = ctx and ctx.startswith('ssh-')
            return ctx_is_ssh is is_ssh

        num_filtered_contexts = 0

        def _count_not_ready_gpus(
            nodes_info: Optional['models.KubernetesNodesInfo']
        ) -> dict[str, int]:
            """Return counts of GPUs on not ready nodes keyed by GPU type."""
            not_ready_counts: dict[str, int] = collections.defaultdict(int)
            if nodes_info is None:
                return not_ready_counts

            node_info_dict = getattr(nodes_info, 'node_info_dict', {}) or {}
            for node_info in node_info_dict.values():
                accelerator_type = getattr(node_info, 'accelerator_type', None)
                if not accelerator_type:
                    continue

                total_info = getattr(node_info, 'total', {})
                accelerator_count = 0
                if isinstance(total_info, dict):
                    accelerator_count = int(
                        total_info.get('accelerator_count', 0))
                if accelerator_count <= 0:
                    continue

                node_is_ready = getattr(node_info, 'is_ready', True)
                node_is_cordoned = getattr(node_info, 'is_cordoned', False)
                node_taints = getattr(node_info, 'taints', None)
                # Only un-tolerated taints count toward the "not ready" GPU
                # tally. Taints matched by `kubernetes.pod_config.spec
                # .tolerations` arrive with `tolerated=True` and don't make
                # the node unschedulable for the user's workloads.
                if (not node_is_ready or node_is_cordoned or
                        kubernetes_utils.has_untolerated_taint(node_taints)):
                    not_ready_counts[accelerator_type] += accelerator_count
            return not_ready_counts

        if realtime_gpu_availability_lists:
            for (ctx, availability_list) in realtime_gpu_availability_lists:
                if not _filter_ctx(ctx):
                    continue
                if is_ssh:
                    display_ctx = common_utils.removeprefix(ctx, 'ssh-')
                else:
                    display_ctx = ctx
                num_filtered_contexts += 1
                # Collect node info for this context before building tables so
                # we can exclude GPUs on not ready nodes from the totals.
                nodes_info = sdk.stream_and_get(
                    sdk.kubernetes_node_info(context=ctx))
                context_not_ready_counts = _count_not_ready_gpus(nodes_info)

                realtime_gpu_table = log_utils.create_table(
                    ['GPU', qty_header, 'UTILIZATION'])
                for realtime_gpu_availability in sorted(availability_list):
                    gpu_availability = models.RealtimeGpuAvailability(
                        *realtime_gpu_availability)
                    available_qty = (gpu_availability.available
                                     if gpu_availability.available != -1 else
                                     no_permissions_str)
                    # Exclude GPUs on not ready nodes from capacity counts.
                    not_ready_count = min(
                        context_not_ready_counts.get(gpu_availability.gpu, 0),
                        gpu_availability.capacity)
                    # Ensure capacity is never below the reported available
                    # quantity (if available is unknown, treat as 0 for totals).
                    available_for_totals = max(
                        gpu_availability.available
                        if gpu_availability.available != -1 else 0, 0)
                    effective_capacity = max(
                        gpu_availability.capacity - not_ready_count,
                        available_for_totals)
                    utilization = (
                        f'{available_qty} of {effective_capacity} free')
                    if not_ready_count > 0:
                        utilization += f' ({not_ready_count} not ready)'
                    realtime_gpu_table.add_row([
                        gpu_availability.gpu,
                        _list_to_str(gpu_availability.counts),
                        utilization,
                    ])
                    gpu = gpu_availability.gpu
                    # we want total, so skip permission denied.
                    if effective_capacity > 0 or not_ready_count > 0:
                        total_gpu_info[gpu][0] += effective_capacity
                        total_gpu_info[gpu][1] += available_for_totals
                        total_gpu_info[gpu][2] += not_ready_count
                realtime_gpu_infos.append((display_ctx, realtime_gpu_table))
                all_nodes_info.append((display_ctx, nodes_info))
        if num_filtered_contexts > 1:
            total_realtime_gpu_table = log_utils.create_table(
                ['GPU', 'UTILIZATION'])
            for gpu, stats in total_gpu_info.items():
                not_ready = stats[2]
                utilization = f'{stats[1]} of {stats[0]} free'
                if not_ready > 0:
                    utilization += f' ({not_ready} not ready)'
                total_realtime_gpu_table.add_row([gpu, utilization])
        else:
            total_realtime_gpu_table = None

        return realtime_gpu_infos, total_realtime_gpu_table, all_nodes_info

    def _get_slurm_realtime_gpu_tables(
        name_filter: str | None = None,
        quantity_filter: int | None = None,
        slurm_cluster_name: str | None = None,
    ) -> tuple[list[tuple[str, 'prettytable.PrettyTable']],
               Optional['prettytable.PrettyTable'], list[tuple[str, str]]]:
        """Get Slurm GPU availability tables.

        Args:
            name_filter: Filter GPUs by name.
            quantity_filter: Filter GPUs by quantity.

        Returns:
            A tuple of (realtime_gpu_infos, total_realtime_gpu_table).
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
        total_gpu_info: dict[str, list[int]] = collections.defaultdict(
            lambda: [0, 0])

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
                    _list_to_str(requestable_quantities),
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

        # display an aggregated table for all partitions
        # if there are more than one partitions with GPUs
        if len(realtime_gpu_infos) > 1:
            total_realtime_gpu_table = log_utils.create_table(
                ['GPU', 'UTILIZATION'])
            for gpu, stats in total_gpu_info.items():
                total_realtime_gpu_table.add_row(
                    [gpu, f'{stats[1]} of {stats[0]} free'])
        else:
            total_realtime_gpu_table = None

        return realtime_gpu_infos, total_realtime_gpu_table, failed_infos

    def _format_kubernetes_node_info_combined(
            contexts_info: list[tuple[str, 'models.KubernetesNodesInfo']],
            cloud_str: str = 'Kubernetes',
            context_title_str: str = 'CONTEXT') -> str:
        node_table = log_utils.create_table([
            context_title_str, 'NODE', 'vCPU', 'Memory (GB)', 'GPU',
            'GPU UTILIZATION', 'NODE STATUS'
        ])

        no_permissions_str = '<no permissions>'
        hints = []

        for context, nodes_info in contexts_info:
            context_name = context if context else 'default'
            if nodes_info.hint:
                hints.append(f'{context_name}: {nodes_info.hint}')

            for node_name, node_info in nodes_info.node_info_dict.items():
                available = node_info.free[
                    'accelerators_available'] if node_info.free[
                        'accelerators_available'] != -1 else no_permissions_str
                acc_type = node_info.accelerator_type
                if acc_type is None:
                    acc_type = '-'

                # Format CPU and memory: "X of Y free" or just "Y" if
                # free is unknown
                cpu_str = '-'
                if node_info.cpu_count is not None:
                    cpu_total_str = common_utils.format_float(
                        node_info.cpu_count, precision=0)

                    # Check if we have free CPU info (use hasattr to
                    # check if field exists, then access directly)
                    cpu_free = None
                    if hasattr(node_info, 'cpu_free'):
                        cpu_free = node_info.cpu_free
                    if cpu_free is not None:
                        cpu_free_str = common_utils.format_float(cpu_free,
                                                                 precision=0)
                        cpu_str = f'{cpu_free_str} of {cpu_total_str} free'
                    else:
                        cpu_str = cpu_total_str

                memory_str = '-'
                if node_info.memory_gb is not None:
                    memory_total_str = common_utils.format_float(
                        node_info.memory_gb, precision=0)

                    # Check if we have free memory info (use hasattr
                    # to check if field exists, then access directly)
                    memory_free_gb = None
                    if hasattr(node_info, 'memory_free_gb'):
                        memory_free_gb = node_info.memory_free_gb
                    if memory_free_gb is not None:
                        memory_free_str = common_utils.format_float(
                            memory_free_gb, precision=0)
                        memory_str = (
                            f'{memory_free_str} of {memory_total_str} free')
                    else:
                        memory_str = memory_total_str

                utilization_str = (
                    f'{available} of '
                    f'{node_info.total["accelerator_count"]} free')

                # Build node status string
                status_info = []
                # Check if node is ready (defaults to True for backward
                # compatibility with older server versions)
                node_is_ready = getattr(node_info, 'is_ready', True)
                if not node_is_ready:
                    status_info.append('NotReady')
                node_is_cordoned = getattr(node_info, 'is_cordoned', False)
                if node_is_cordoned:
                    status_info.append('Cordoned')
                # Add taint info grouped by effect. Only un-tolerated taints
                # count toward node status — taints matched by the user's
                # configured `kubernetes.pod_config.spec.tolerations` arrive
                # with `tolerated=True` and don't make the node unhealthy.
                untolerated_taints = [
                    t for t in (getattr(node_info, 'taints', None) or [])
                    if not t.get('tolerated', False)
                ]
                if untolerated_taints:
                    # Group taints by effect: 'NoSchedule Taint [key1, key2],
                    # NoExecute Taint [key3]'
                    taints_by_effect: dict[str, list[str]] = {}
                    for taint in untolerated_taints:
                        effect = taint['effect']
                        key = taint['key']
                        if effect not in taints_by_effect:
                            taints_by_effect[effect] = []
                        taints_by_effect[effect].append(key)
                    taints_strs = [
                        f'{effect} Taint [{", ".join(keys)}]'
                        for effect, keys in taints_by_effect.items()
                    ]
                    if taints_strs:
                        status_info.append(', '.join(taints_strs))

                status_str = ', '.join(
                    status_info) if status_info else 'Healthy'
                node_table.add_row([
                    context_name, node_name, cpu_str, memory_str, acc_type,
                    utilization_str, status_str
                ])

        k8s_per_node_acc_message = (f'{cloud_str} per-node GPU availability')
        if hints:
            k8s_per_node_acc_message += ' (' + '; '.join(hints) + ')'

        return (f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
                f'{k8s_per_node_acc_message}'
                f'{colorama.Style.RESET_ALL}\n'
                f'{node_table.get_string()}')

    def _format_slurm_partition_info(slurm_cluster_names: list[str]) -> str:
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

        slurm_per_partition_msg = (
            'Slurm per-partition accelerator availability')
        if failed_clusters:
            slurm_per_partition_msg += (f' (skipped unreachable clusters: '
                                        f'{", ".join(failed_clusters)})')

        return (f'{colorama.Fore.LIGHTMAGENTA_EX}{colorama.Style.NORMAL}'
                f'{slurm_per_partition_msg}'
                f'{colorama.Style.RESET_ALL}\n'
                f'{partition_table.get_string()}')

    def _get_labeled_zero_gpu_hint(
            all_nodes_info: list[tuple[str,
                                       'models.KubernetesNodesInfo']]) -> str:
        """Returns a hint if any nodes have GPU labels but 0 GPU resources."""
        # Collect nodes with GPU labels but 0 GPU resources
        labeled_zero_gpu_nodes = [
            (context, node_name)
            for context, nodes_info in all_nodes_info
            for node_name, node_info in nodes_info.node_info_dict.items()
            if (node_info.accelerator_type is not None and
                node_info.total.get('accelerator_count', 0) == 0)
        ]

        if not labeled_zero_gpu_nodes:
            return ''

        num_affected_nodes = len(labeled_zero_gpu_nodes)
        node_list = ', '.join(
            f'{ctx}/{name}' for ctx, name in labeled_zero_gpu_nodes[:3])
        ellipsis = '...' if len(labeled_zero_gpu_nodes) > 3 else ''
        return (f'Note: Some Kubernetes nodes have GPU labels but report 0 GPU '
                f'resources. Please check the node labels and configuration. '
                f'Affected {num_affected_nodes} node(s): {node_list}{ellipsis}')

    def _format_kubernetes_realtime_gpu(
            total_table: Optional['prettytable.PrettyTable'],
            k8s_realtime_infos: list[tuple[str, 'prettytable.PrettyTable']],
            all_nodes_info: list[tuple[str, 'models.KubernetesNodesInfo']],
            show_node_info: bool, is_ssh: bool) -> Generator[str, None, None]:
        identity = 'SSH Node Pool' if is_ssh else 'Kubernetes'
        yield (f'{colorama.Fore.GREEN}{colorama.Style.BRIGHT}'
               f'{identity} GPUs'
               f'{colorama.Style.RESET_ALL}')
        # print total table
        if total_table is not None:
            yield '\n'
            yield from total_table.get_string()

        ctx_name = 'SSH Node Pool' if is_ssh else 'Context'
        ctx_column_title = 'NODE_POOL' if is_ssh else 'CONTEXT'

        # print individual infos.
        for (ctx, k8s_realtime_table) in k8s_realtime_infos:
            yield '\n'
            # Print context header separately
            if ctx:
                context_str = f'{ctx_name}: {ctx}'
            else:
                context_str = f'Default {ctx_name}'
            yield (
                f'{colorama.Fore.CYAN}{context_str}{colorama.Style.RESET_ALL}\n'
            )
            yield from k8s_realtime_table.get_string()

        if show_node_info:
            yield '\n'
            yield _format_kubernetes_node_info_combined(all_nodes_info,
                                                        identity,
                                                        ctx_column_title)

    def _possibly_show_k8s_like_realtime(
            is_ssh: bool = False
    ) -> Generator[str, None, tuple[bool, bool, str]]:
        # If cloud is kubernetes, we want to show real-time capacity
        k8s_messages = ''
        print_section_titles = False
        if (is_ssh and query_ssh_realtime_gpu or query_k8s_realtime_gpu):
            context = region

            try:
                # If --cloud kubernetes is not specified, we want to catch
                # the case where no GPUs are available on the cluster and
                # print the warning at the end.
                k8s_realtime_infos, total_table, all_nodes_info = (
                    _get_kubernetes_realtime_gpu_tables(context, is_ssh=is_ssh))
            except ValueError as e:
                if not (cloud_is_kubernetes or cloud_is_ssh):
                    # Make it a note if cloud is not kubernetes
                    k8s_messages += 'Note: '
                k8s_messages += str(e)
            else:
                print_section_titles = True

                yield from _format_kubernetes_realtime_gpu(total_table,
                                                           k8s_realtime_infos,
                                                           all_nodes_info,
                                                           show_node_info=True,
                                                           is_ssh=is_ssh)

                # Check for nodes with GPU labels but 0 GPU resources
                labeled_zero_hint = _get_labeled_zero_gpu_hint(all_nodes_info)
                if labeled_zero_hint:
                    k8s_messages += labeled_zero_hint

            if kubernetes_autoscaling:
                k8s_messages += ('\n' +
                                 kubernetes_utils.KUBERNETES_AUTOSCALER_NOTE)
        if is_ssh:
            if cloud_is_ssh:
                if not ssh_is_enabled:
                    yield ('SSH Node Pools are not enabled. To fix, run: '
                           'sky check ssh ')
                if k8s_messages and print_section_titles:
                    yield '\n\n'
                yield k8s_messages
                return True, print_section_titles, ''
        else:
            if cloud_is_kubernetes:
                if not kubernetes_is_enabled:
                    yield ('Kubernetes is not enabled. To fix, run: '
                           'sky check kubernetes ')
                if k8s_messages and print_section_titles:
                    yield '\n\n'
                yield k8s_messages
                return True, print_section_titles, ''
        return False, print_section_titles, k8s_messages

    def _possibly_show_k8s_like_realtime_for_acc(
            name: str | None,
            quantity: int | None,
            is_ssh: bool = False) -> Generator[str, None, tuple[bool, bool]]:
        k8s_messages = ''
        print_section_titles = False
        if (is_ssh and query_ssh_realtime_gpu or
                query_k8s_realtime_gpu) and not show_all:
            print_section_titles = True
            # TODO(romilb): Show filtered per node GPU availability here as well
            try:
                (k8s_realtime_infos, total_table,
                 all_nodes_info) = _get_kubernetes_realtime_gpu_tables(
                     context=region,
                     name_filter=name,
                     quantity_filter=quantity,
                     is_ssh=is_ssh)

                yield from _format_kubernetes_realtime_gpu(total_table,
                                                           k8s_realtime_infos,
                                                           all_nodes_info,
                                                           show_node_info=False,
                                                           is_ssh=is_ssh)

                # Check for nodes with GPU labels but 0 GPU resources
                labeled_zero_hint = _get_labeled_zero_gpu_hint(all_nodes_info)
                if labeled_zero_hint:
                    k8s_messages += labeled_zero_hint
            except ValueError as e:
                # In the case of a specific accelerator, show the error message
                # immediately (e.g., "Resources H100 not found ...")
                yield common_utils.format_exception(e, use_bracket=True)
            if kubernetes_autoscaling:
                k8s_messages += ('\n' +
                                 kubernetes_utils.KUBERNETES_AUTOSCALER_NOTE)
            yield k8s_messages
        if is_ssh:
            if cloud_is_ssh:
                if not ssh_is_enabled:
                    yield ('SSH Node Pools are not enabled. To fix, run: '
                           'sky check ssh ')
                return True, print_section_titles
        else:
            if cloud_is_kubernetes:
                if not kubernetes_is_enabled:
                    yield ('Kubernetes is not enabled. To fix, run: '
                           'sky check kubernetes ')
                return True, print_section_titles
        return False, print_section_titles

    def _format_slurm_realtime_gpu(
            total_table, slurm_realtime_infos, failed_infos,
            show_node_info: bool) -> Generator[str, None, None]:
        # print total table
        yield (f'{colorama.Fore.GREEN}{colorama.Style.BRIGHT}'
               'Slurm GPUs'
               f'{colorama.Style.RESET_ALL}\n')
        if total_table is not None:
            yield from total_table.get_string()
            yield '\n'

        # print individual infos.
        for (partition, slurm_realtime_table) in slurm_realtime_infos:
            partition_str = f'Slurm Cluster: {partition}'
            yield (f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
                   f'{partition_str}'
                   f'{colorama.Style.RESET_ALL}\n')
            yield from slurm_realtime_table.get_string()
            yield '\n'

        for (cluster_name, error_msg) in failed_infos:
            yield (f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
                   f'Slurm Cluster: {cluster_name}'
                   f'{colorama.Style.RESET_ALL}\n')
            yield (f'{colorama.Fore.YELLOW}'
                   f'Error: {error_msg}'
                   f'{colorama.Style.RESET_ALL}\n')

        if show_node_info and slurm_realtime_infos:
            cluster_names = [cluster for cluster, _ in slurm_realtime_infos]
            yield _format_slurm_partition_info(cluster_names)

    def _output() -> Generator[str, None, None]:
        gpu_table = log_utils.create_table(
            ['COMMON_GPU', 'AVAILABLE_QUANTITIES'])
        tpu_table = log_utils.create_table(
            ['GOOGLE_TPU', 'AVAILABLE_QUANTITIES'])
        other_table = log_utils.create_table(
            ['OTHER_GPU', 'AVAILABLE_QUANTITIES'])

        name, quantity = None, None

        # Optimization - do not poll for Kubernetes API for fetching
        # common GPUs because that will be fetched later for the table after
        # common GPUs.
        clouds_to_list: str | None | list[str] = cloud_name
        if cloud_name is None:
            clouds_to_list = [
                c for c in constants.ALL_CLOUDS
                if c != 'kubernetes' and c != 'ssh' and c != 'slurm'
            ]

        k8s_messages = ''
        slurm_messages = ''
        k8s_printed = False
        if accelerator_str is None:
            # Collect k8s related messages in k8s_messages and print them at end
            print_section_titles = False
            stop_iter = False
            k8s_messages = ''
            prev_print_section_titles = False
            for is_ssh in [False, True]:
                if prev_print_section_titles:
                    yield '\n\n'
                stop_iter_one, print_section_titles_one, k8s_messages_one = (
                    yield from _possibly_show_k8s_like_realtime(is_ssh))
                k8s_printed = True
                stop_iter = stop_iter or stop_iter_one
                print_section_titles = (print_section_titles or
                                        print_section_titles_one)
                if k8s_messages and k8s_messages_one:
                    k8s_messages += '\n'
                k8s_messages += k8s_messages_one
                prev_print_section_titles = print_section_titles_one
            if stop_iter:
                return
            # If cloud is slurm, we want to show real-time capacity
            if slurm_is_enabled and (cloud_name is None or cloud_is_slurm):
                try:
                    # If --cloud slurm is not specified, we want to catch
                    # the case where no GPUs are available on the cluster and
                    # print the warning at the end.
                    slurm_realtime_infos, total_table, failed_infos = (
                        _get_slurm_realtime_gpu_tables(
                            slurm_cluster_name=region))
                except ValueError as e:
                    if not cloud_is_slurm:
                        # Make it a note if cloud is not slurm
                        slurm_messages += 'Note: '
                    slurm_messages += str(e)
                else:
                    print_section_titles = True
                    if k8s_printed:
                        yield '\n'

                    yield from _format_slurm_realtime_gpu(
                        total_table,
                        slurm_realtime_infos,
                        failed_infos,
                        show_node_info=verbose)

            if cloud_is_slurm:
                # Do not show clouds if --cloud slurm is specified
                if not slurm_is_enabled:
                    yield ('Slurm is not enabled. To fix, run: '
                           'sky check slurm ')
                yield slurm_messages
                return

            # For show_all, show the k8s message at the start since output is
            # long and the user may not scroll to the end.
            if show_all and (k8s_messages or slurm_messages):
                if k8s_messages:
                    yield k8s_messages
                if slurm_messages:
                    if k8s_messages:
                        yield '\n'
                    yield slurm_messages
                yield '\n\n'

            list_accelerator_counts_result = sdk.stream_and_get(
                sdk.list_accelerator_counts(
                    gpus_only=True,
                    clouds=clouds_to_list,
                    region_filter=region,
                ))
            # TODO(zhwu): handle the case where no accelerators are found,
            # especially when --region specified a non-existent region.

            if print_section_titles:
                # If section titles were printed above, print again here
                yield '\n\n'
                yield (f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
                       f'Cloud GPUs{colorama.Style.RESET_ALL}\n')

            # "Common" GPUs
            for gpu in catalog.get_common_gpus():
                if gpu in list_accelerator_counts_result:
                    gpu_table.add_row([
                        gpu,
                        _list_to_str(list_accelerator_counts_result.pop(gpu))
                    ])
            yield from gpu_table.get_string()

            # Google TPUs
            for tpu in catalog.get_tpus():
                if tpu in list_accelerator_counts_result:
                    tpu_table.add_row([
                        tpu,
                        _list_to_str(list_accelerator_counts_result.pop(tpu))
                    ])
            if tpu_table.get_string():
                yield '\n\n'
            yield from tpu_table.get_string()

            # Other GPUs
            if show_all:
                yield '\n\n'
                for gpu, qty in sorted(list_accelerator_counts_result.items()):
                    other_table.add_row([gpu, _list_to_str(qty)])
                yield from other_table.get_string()
                yield '\n\n'
            else:
                yield ('\n\nHint: use -a/--all to see all accelerators '
                       '(including non-common ones) and pricing.')
                if k8s_messages or slurm_messages:
                    yield '\n'
                    yield k8s_messages
                    yield slurm_messages
                return
        else:
            # Parse accelerator string
            accelerator_split = accelerator_str.split(':')
            if len(accelerator_split) > 2:
                raise click.UsageError(
                    f'Invalid accelerator string {accelerator_str}. '
                    'Expected format: <accelerator_name>[:<quantity>].')
            if len(accelerator_split) == 2:
                name = accelerator_split[0]
                # Check if quantity is valid
                try:
                    quantity = int(accelerator_split[1])
                    if quantity <= 0:
                        raise ValueError(
                            'Quantity cannot be non-positive integer.')
                except ValueError as invalid_quantity:
                    raise click.UsageError(
                        f'Invalid accelerator quantity {accelerator_split[1]}. '
                        'Expected a positive integer.') from invalid_quantity
            else:
                name, quantity = accelerator_str, None

        print_section_titles = False
        stop_iter = False
        prev_print_section_titles = False
        for is_ssh in [False, True]:
            if prev_print_section_titles:
                yield '\n\n'
            stop_iter_one, print_section_titles_one = (
                yield from _possibly_show_k8s_like_realtime_for_acc(
                    name, quantity, is_ssh))
            stop_iter = stop_iter or stop_iter_one
            print_section_titles = (print_section_titles or
                                    print_section_titles_one)
            prev_print_section_titles = print_section_titles_one
        if stop_iter:
            return

        # Handle Slurm filtering by name and quantity
        if (slurm_is_enabled and (cloud_name is None or cloud_is_slurm) and
                not show_all):
            # Print section title if not showing all and instead a specific
            # accelerator is requested
            print_section_titles = True
            try:
                slurm_realtime_infos, total_table, failed_infos = (
                    _get_slurm_realtime_gpu_tables(name_filter=name,
                                                   quantity_filter=quantity,
                                                   slurm_cluster_name=region))

                yield from _format_slurm_realtime_gpu(total_table,
                                                      slurm_realtime_infos,
                                                      failed_infos,
                                                      show_node_info=False)
            except ValueError as e:
                # In the case of a specific accelerator, show the error message
                # immediately (e.g., "Resources A10G not found ...")
                yield str(e)
            yield slurm_messages
        if cloud_is_slurm:
            # Do not show clouds if --cloud slurm is specified
            if not slurm_is_enabled:
                yield ('Slurm is not enabled. To fix, run: '
                       'sky check slurm ')
            return
        # For clouds other than Kubernetes, get the accelerator details
        # Case-sensitive
        list_accelerators_result = sdk.stream_and_get(
            sdk.list_accelerators(gpus_only=True,
                                  name_filter=name,
                                  quantity_filter=quantity,
                                  region_filter=region,
                                  clouds=clouds_to_list,
                                  case_sensitive=False,
                                  all_regions=all_regions))
        # Import here to save module load speed.
        # pylint: disable=import-outside-toplevel,line-too-long
        from sky.catalog import common as catalog_common

        # For each gpu name (count not included):
        #   - Group by cloud
        #   - Sort within each group by prices
        #   - Sort groups by each cloud's (min price, min spot price)
        new_result: dict[str, list[catalog_common.InstanceTypeInfo]] = {}
        for i, (gpu, items) in enumerate(list_accelerators_result.items()):
            df = pd.DataFrame([t._asdict() for t in items])
            # Determine the minimum prices for each cloud.
            min_price_df = df.groupby('cloud').agg(min_price=('price', 'min'),
                                                   min_spot_price=('spot_price',
                                                                   'min'))
            df = df.merge(min_price_df, on='cloud')
            df = df.sort_values(
                by=['min_price', 'min_spot_price', 'price', 'spot_price'])
            df = df.drop(columns=['min_price', 'min_spot_price'])
            sorted_dataclasses = [
                catalog_common.InstanceTypeInfo(*row)
                for row in df.to_records(index=False)
            ]
            new_result[gpu] = sorted_dataclasses
        list_accelerators_result = new_result

        if print_section_titles and not show_all:
            yield '\n\n'
            yield (f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
                   f'Cloud GPUs{colorama.Style.RESET_ALL}\n')

        if not list_accelerators_result:
            quantity_str = (f' with requested quantity {quantity}'
                            if quantity else '')
            cloud_str = f' on {cloud_obj}.' if cloud_name else ' in cloud catalogs.'
            yield f'Resources \'{name}\'{quantity_str} not found{cloud_str} '
            yield 'To show available accelerators, run: sky gpus list --all'
            return

        for i, (gpu, items) in enumerate(list_accelerators_result.items()):
            accelerator_table_headers = [
                'GPU',
                'QTY',
                'CLOUD',
                'INSTANCE_TYPE',
                'DEVICE_MEM',
                'vCPUs',
                'HOST_MEM',
                'HOURLY_PRICE',
                'HOURLY_SPOT_PRICE',
            ]
            if not show_all:
                accelerator_table_headers.append('REGION')
            accelerator_table = log_utils.create_table(
                accelerator_table_headers)
            for item in items:
                instance_type_str = item.instance_type if not pd.isna(
                    item.instance_type) else '(attachable)'
                cpu_count = item.cpu_count
                if not pd.isna(cpu_count) and isinstance(
                        cpu_count, (float, int)):
                    if int(cpu_count) == cpu_count:
                        cpu_str = str(int(cpu_count))
                    else:
                        cpu_str = f'{cpu_count:.1f}'
                else:
                    cpu_str = '-'
                device_memory_str = (f'{item.device_memory:.0f}GB' if
                                     not pd.isna(item.device_memory) else '-')
                host_memory_str = f'{item.memory:.0f}GB' if not pd.isna(
                    item.memory) else '-'
                price_str = f'$ {item.price:.3f}' if not pd.isna(
                    item.price) else '-'
                spot_price_str = f'$ {item.spot_price:.3f}' if not pd.isna(
                    item.spot_price) else '-'
                region_str = item.region if not pd.isna(item.region) else '-'
                accelerator_table_vals = [
                    item.accelerator_name,
                    item.accelerator_count,
                    item.cloud,
                    instance_type_str,
                    device_memory_str,
                    cpu_str,
                    host_memory_str,
                    price_str,
                    spot_price_str,
                ]
                if not show_all:
                    accelerator_table_vals.append(region_str)
                accelerator_table.add_row(accelerator_table_vals)

            if i != 0:
                yield '\n\n'
            yield from accelerator_table.get_string()

    outputs = _output()
    if show_all:
        click.echo_via_pager(outputs)
    else:
        for out in outputs:
            click.echo(out, nl=False)
        click.echo()


@click.group('gpus', cls=click_utils.NaturalOrderGroup)
def gpus_cli():
    """SkyPilot GPU/Accelerator CLI."""
    pass


@gpus_cli.command('list', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.argument('accelerator_str', required=False)
@flags.all_option('Show details of all GPU/TPU/accelerator offerings.')
@click.option('--infra',
              default=None,
              type=str,
              help='Infrastructure to query. Examples: "aws", "aws/us-east-1"')
@click.option('--cloud',
              default=None,
              type=str,
              help='Cloud provider to query.',
              hidden=True)
@click.option(
    '--region',
    required=False,
    type=str,
    help=
    ('The region to use. If not specified, shows accelerators from all regions.'
    ),
    hidden=True,
)
@click.option(
    '--all-regions',
    is_flag=True,
    default=False,
    help='Show pricing and instance details for a specified accelerator across '
    'all regions and clouds.')
@flags.verbose_option()
@flags.output_format_option()
@catalog.fallback_to_default_catalog
@usage_lib.entrypoint
def gpus_list(
        accelerator_str: str | None,
        all: bool,  # pylint: disable=redefined-builtin
        infra: str | None,
        cloud: str | None,
        region: str | None,
        all_regions: bool,
        verbose: bool,
        output_format: str = 'table'):
    """Show supported GPU/TPU/accelerators and their prices.

    The names and counts shown can be set in the ``accelerators`` field in task
    YAMLs, or in the ``--gpus`` flag in CLI commands. For example, if this
    table shows 8x V100s are supported, then the string ``V100:8`` will be
    accepted by the above.

    To show the detailed information of a GPU/TPU type (its price, which clouds
    offer it, the quantity in each VM type, etc.), use ``sky gpus list <gpu>``.

    To show all accelerators, including less common ones and their detailed
    information, use ``sky gpus list --all``.

    To show all regions for a specified accelerator, use
    ``sky gpus list <accelerator> --all-regions``.

    If ``--region`` or ``--all-regions`` is not specified, the price displayed
    for each instance type is the lowest across all regions for both on-demand
    and spot instances. There may be multiple regions with the same lowest
    price.

    If ``--cloud kubernetes`` or ``--cloud k8s`` is specified, it will show the
    maximum quantities of the GPU available on a single node and the real-time
    availability of the GPU across all nodes in the Kubernetes cluster.

    If ``--cloud slurm`` is specified, it will show the maximum quantities of
    the GPU available on a single node and the real-time availability of the
    GPU across all nodes in the Slurm cluster. Use ``-v`` to show per-partition
    accelerator details.

    Definitions of certain fields:

    * ``DEVICE_MEM``: Memory of a single device; does not depend on the device
      count of the instance (VM).

    * ``HOST_MEM``: Memory of the host instance (VM).

    * ``QTY_PER_NODE`` (Kubernetes only): GPU quantities that can be requested
      on a single node.

    * ``UTILIZATION`` (Kubernetes only): Total number of GPUs free / available
      in the Kubernetes cluster.
    """
    # Call the shared implementation
    _show_gpus_impl(accelerator_str,
                    all,
                    infra,
                    cloud,
                    region,
                    all_regions,
                    verbose=verbose,
                    output_format=output_format)


@gpus_cli.command('label', cls=click_utils.DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@click.option('--context',
              '-c',
              type=str,
              default=None,
              help='Kubernetes context to use. If not specified, uses the '
              'current context from the API server.')
@click.option('--cleanup',
              is_flag=True,
              default=False,
              help='Only cleanup existing GPU labeler resources.')
@click.option('--async',
              'async_mode',
              is_flag=True,
              default=False,
              help='Do not wait for GPU labeling to complete.')
@usage_lib.entrypoint
def gpus_label(context: str | None, cleanup: bool, async_mode: bool):
    """Label GPU nodes in a Kubernetes cluster for use with SkyPilot.

    This command runs on the API server to label GPU nodes with
    skypilot.co/accelerator labels. This is required for SkyPilot to
    identify GPU types on nodes that don't have pre-configured labels.

    Note: This command currently only supports NVIDIA GPUs. AMD GPUs
    must be labeled manually.

    Example usage:

      # Label GPUs in the current Kubernetes context
      sky gpus label

      # Label GPUs in a specific context
      sky gpus label --context my-k8s-cluster

      # Cleanup labeling resources
      sky gpus label --cleanup

      # Start labeling without waiting for completion
      sky gpus label --async
    """
    request_id = sdk.kubernetes_label_gpus(
        context=context,
        cleanup_only=cleanup,
        wait_for_completion=not async_mode,
    )
    # Stream logs to show progress (spinners, node info, etc.)
    # The actual output is in the streamed logs, not just the return value
    result = sdk.stream_and_get(request_id)

    # Exit with appropriate code based on success
    if not result.get('success', False):
        sys.exit(1)


# Deprecate 'sky show-gpus' in favor of 'sky gpus list'
# pylint: disable=protected-access
deprecation_utils._deprecate_and_hide_command(
    group=None,
    command_to_deprecate=show_gpus,
    alternative_command='sky gpus list')
