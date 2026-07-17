"""Console reporting for optimizer plans and candidates."""
import collections
import json

import colorama
import prettytable

from sky import clouds
from sky import resources as resources_lib
from sky import task as task_lib
from sky.utils import env_options
from sky.utils import log_utils
from sky.utils import resources_utils

_TaskToCostMap = dict[task_lib.Task, dict[resources_lib.Resources, float]]
_PerCloudCandidates = dict[clouds.Cloud, list[resources_lib.Resources]]
_TaskToPerCloudCandidates = dict[task_lib.Task, _PerCloudCandidates]


def _create_table(field_names: list[str]) -> prettytable.PrettyTable:
    table_kwargs = {
        'hrules': prettytable.FRAME,
        'vrules': prettytable.NONE,
        'border': True,
    }
    return log_utils.create_table(field_names, **table_kwargs)


def print_egress_plan(graph, plan, minimize_cost, *, get_egress_info,
                      egress_cost, egress_time, dummy_source_name, logger):
    message_data = []
    for parent, child in graph.edges():
        src_cloud, dst_cloud, nbytes = get_egress_info(parent, plan[parent],
                                                       child, plan[child])
        if not nbytes:
            # nbytes can be None, if the task has no inputs/outputs.
            return 0
        assert src_cloud is not None and dst_cloud is not None, (src_cloud,
                                                                 dst_cloud,
                                                                 nbytes)

        if minimize_cost:
            fn = egress_cost
        else:
            fn = egress_time
        cost_or_time = fn(src_cloud, dst_cloud, nbytes)

        if cost_or_time > 0:
            if parent.name == dummy_source_name:
                egress = [
                    f'{child.get_inputs()} ({src_cloud})',
                    f'{child} ({dst_cloud})'
                ]
            else:
                egress = [f'{parent} ({src_cloud})', f'{child} ({dst_cloud})']
            message_data.append((*egress, nbytes, cost_or_time))

    if message_data:
        metric = 'COST ($)' if minimize_cost else 'TIME (s)'
        table = _create_table(['SOURCE', 'TARGET', 'SIZE (GB)', metric])
        table.add_rows(list(reversed(message_data)))
        logger.info(f'Egress plan:\n{table}\n')


def print_optimized_plan(graph, topo_order: list[task_lib.Task],
                         best_plan: dict[task_lib.Task,
                                         resources_lib.Resources],
                         total_time: float, total_cost: float,
                         node_to_cost_map: _TaskToCostMap, minimize_cost: bool,
                         *, print_egress_plan_fn,
                         dummy_task_names: tuple[str, str], logger):
    ordered_node_to_cost_map = collections.OrderedDict()
    ordered_best_plan = collections.OrderedDict()
    for node in topo_order:
        if node.name not in dummy_task_names:
            ordered_node_to_cost_map[node] = node_to_cost_map[node]
            ordered_best_plan[node] = best_plan[node]

    is_trivial = all(len(v) == 1 for v in node_to_cost_map.values())
    if not is_trivial and not env_options.Options.MINIMIZE_LOGGING.get():
        metric_str = 'cost' if minimize_cost else 'run time'
        logger.info(f'{colorama.Style.BRIGHT}Target:{colorama.Style.RESET_ALL}'
                    f' minimizing {metric_str}')

    print_hourly_cost = False
    if len(ordered_node_to_cost_map) == 1:
        node = list(ordered_node_to_cost_map.keys())[0]
        if (node.time_estimator_func is None and node.get_inputs() is None and
                node.get_outputs() is None):
            print_hourly_cost = True

    if not env_options.Options.MINIMIZE_LOGGING.get():
        if print_hourly_cost:
            logger.info(f'{colorama.Style.BRIGHT}Estimated cost: '
                        f'{colorama.Style.RESET_ALL}${total_cost:.1f} / hour\n')
        else:
            logger.info(f'{colorama.Style.BRIGHT}Estimated total runtime: '
                        f'{colorama.Style.RESET_ALL}{total_time / 3600:.1f} '
                        'hours\n'
                        f'{colorama.Style.BRIGHT}Estimated total cost: '
                        f'{colorama.Style.RESET_ALL}${total_cost:.1f}\n')

    def _instance_type_str(resources: 'resources_lib.Resources') -> str:
        instance_type = resources.instance_type
        assert instance_type is not None, 'Instance type must be specified'
        if isinstance(resources.cloud, (clouds.Kubernetes, clouds.Slurm)):
            instance_type = '-'
            if resources.use_spot:
                instance_type = ''
        return instance_type

    def _get_resources_element_list(
            resources: 'resources_lib.Resources') -> list[str]:
        accelerators = resources.get_accelerators_str()
        spot = resources.get_spot_str()
        cloud = resources.cloud
        assert cloud is not None, 'Cloud must be specified'
        assert (resources.instance_type is not None), \
            'Instance type must be specified'
        vcpus_, mem_ = cloud.get_vcpus_mem_from_instance_type(
            resources.instance_type)

        def format_number(x: float | None) -> str:
            if x is None:
                return '-'
            elif x.is_integer():
                return str(int(x))
            else:
                return f'{x:.1f}'

        vcpus = format_number(vcpus_)
        mem = format_number(mem_)

        # Format infra as CLOUD (REGION/ZONE)
        infra = resources.infra.formatted_str()

        return [
            infra,
            _instance_type_str(resources) + spot,
            vcpus,
            mem,
            str(accelerators),
        ]

    Row = collections.namedtuple('Row', [
        'infra', 'instance', 'vcpus', 'mem', 'accelerators', 'cost_str',
        'chosen_str'
    ])

    def _get_resources_named_tuple(resources: 'resources_lib.Resources',
                                   cost_str: str, chosen: bool) -> Row:

        accelerators = resources.get_accelerators_str()
        spot = resources.get_spot_str()
        resources = resources.assert_launchable()
        cloud = resources.cloud
        vcpus_, mem_ = cloud.get_vcpus_mem_from_instance_type(
            resources.instance_type)

        def format_number(x: float | None) -> str:
            if x is None:
                return '-'
            elif x.is_integer():
                return str(int(x))
            else:
                return f'{x:.1f}'

        vcpus = format_number(vcpus_)
        mem = format_number(mem_)

        infra = resources.infra.formatted_str()

        chosen_str = ''
        if chosen:
            chosen_str = (colorama.Fore.GREEN + '   ' + '\u2714' +
                          colorama.Style.RESET_ALL)
        row = Row(infra,
                  _instance_type_str(resources) + spot, vcpus, mem,
                  str(accelerators), cost_str, chosen_str)

        return row

    def _get_resource_group_hash(resources: 'resources_lib.Resources'):
        resource_key_dict = {
            'cloud': f'{resources.cloud}',
            'accelerators': f'{resources.accelerators}',
            'use_spot': resources.use_spot
        }

        # Handle special case for Kubernetes, SSH, and SLURM clouds
        if isinstance(resources.cloud, (clouds.Kubernetes, clouds.Slurm)):
            # Region for Kubernetes-like clouds (SSH, Kubernetes) is the
            # context name, i.e. different Kubernetes clusters.
            # Region for SLURM is the cluster name.
            # We add region to the key to show all the clusters in the
            # optimizer table for better UX.

            if resources.cloud.__class__.__name__ == 'SSH':
                resource_key_dict[
                    'cloud'] = 'SSH'  # Force the cloud name to be SSH
            resource_key_dict['region'] = resources.region

        return json.dumps(resource_key_dict, sort_keys=True)

    # Print the list of resouces that the optimizer considered.
    resource_fields = ['INFRA', 'INSTANCE', 'vCPUs', 'Mem(GB)', 'GPUS']
    if len(ordered_best_plan) > 1:
        best_plan_rows = []
        for t, r in ordered_best_plan.items():
            assert t.name is not None, t
            best_plan_rows.append([t.name, str(t.num_nodes)] +
                                  _get_resources_element_list(r))
        logger.info(
            f'{colorama.Style.BRIGHT}Best plan: {colorama.Style.RESET_ALL}')
        best_plan_table = _create_table(['TASK', '#NODES'] + resource_fields)
        best_plan_table.add_rows(best_plan_rows)
        logger.info(f'{best_plan_table}')

    # Print the egress plan if any data egress is scheduled.
    print_egress_plan_fn(graph, best_plan, minimize_cost)

    metric = 'COST ($)' if minimize_cost else 'TIME (hr)'
    field_names = resource_fields + [metric, 'CHOSEN']

    num_tasks = len(ordered_node_to_cost_map)
    for task, v in ordered_node_to_cost_map.items():
        # Hack: convert the dictionary values
        # (resources) to their yaml config
        # For dictionary comparison later.
        v_yaml = {
            json.dumps(resource.to_yaml_config()): cost
            for resource, cost in v.items()
        }
        task_str = (f'for task {task.name!r} ' if num_tasks > 1 else '')
        plural = 's' if task.num_nodes > 1 else ''
        if num_tasks > 1:
            # Add a new line for better readability, when there are multiple
            # tasks.
            logger.info('')
        logger.info(f'Considered resources {task_str}'
                    f'({task.num_nodes} node{plural}):')

        # Only print 1 row per cloud.
        # The following code is to generate the table
        # of optimizer table for display purpose.
        best_per_resource_group: dict[str, tuple[resources_lib.Resources,
                                                 float]] = {}
        for resources, cost in v.items():
            resource_table_key = _get_resource_group_hash(resources)
            if resource_table_key in best_per_resource_group:
                if cost < best_per_resource_group[resource_table_key][1]:
                    best_per_resource_group[resource_table_key] = (resources,
                                                                   cost)
            else:
                best_per_resource_group[resource_table_key] = (resources, cost)

        # If the DAG has multiple tasks, the chosen resources may not be
        # the best resources for the task.
        chosen_resources = best_plan[task]
        resource_table_key = _get_resource_group_hash(chosen_resources)
        best_per_resource_group[resource_table_key] = (
            chosen_resources,
            v_yaml[json.dumps(chosen_resources.to_yaml_config())])
        rows = []
        for resources, cost in best_per_resource_group.values():
            if minimize_cost:
                cost_str = f'{cost:.2f}'
            else:
                cost_str = f'{cost / 3600:.2f}'

            row = _get_resources_named_tuple(resources, cost_str,
                                             resources == best_plan[task])
            rows.append(row)

        # NOTE: we've converted the cost to a string above, so we should
        # convert it back to float for sorting.
        if isinstance(task.resources, list):
            accelerator_spot_list = [
                r.get_accelerators_str() + r.get_spot_str()
                for r in list(task.resources)
            ]

            def sort_key(row, accelerator_spot_list=accelerator_spot_list):
                accelerator_index = accelerator_spot_list.index(
                    row.accelerators +
                    ('[Spot]' if '[Spot]' in row.instance else ''))
                cost = float(row.cost_str)
                return (accelerator_index, cost)

            rows = sorted(rows, key=sort_key)
        else:
            rows = sorted(rows, key=lambda row: float(row.cost_str))

        row_list = []
        for row in rows:
            row_in_list = []
            if row.chosen_str != '':
                for _, cell in enumerate(row):
                    row_in_list.append(f'{colorama.Style.BRIGHT}{cell}'
                                       f'{colorama.Style.RESET_ALL}')
            else:
                row_in_list = list(row)
            row_list.append(row_in_list)

        table = _create_table(field_names)
        table.add_rows(rows)
        logger.info(f'{table}')

        # Warning message for using disk_tier=ultra
        # TODO(yi): Consider price of disks in optimizer and
        # move this warning there.
        if chosen_resources.disk_tier == resources_utils.DiskTier.ULTRA:
            logger.warning(
                'Using disk_tier=ultra will utilize more advanced disks '
                '(io2 Block Express on AWS and extreme persistent disk on '
                'GCP), which can lead to significant higher costs (~$2/h).')


def print_candidates(node_to_candidate_map: _TaskToPerCloudCandidates, *,
                     logger):
    for node, candidate_set in node_to_candidate_map.items():
        best_resources = node.best_resources
        if best_resources is None:
            best_resources = list(node.resources)[0]
        is_multi_instances = False
        acc_name = None
        if best_resources.accelerators:
            acc_name, acc_count = list(best_resources.accelerators.items())[0]
            for cloud, candidate_list in candidate_set.items():
                # Filter only the candidates matching the best
                # resources chosen by the optimizer.
                best_resources_candidates = [
                    res for res in candidate_list
                    if res.get_accelerators_str() == f'{acc_name}:{acc_count}'
                ]
                if len(best_resources_candidates) > 1:
                    is_multi_instances = True
                    instance_list = set([
                        res.instance_type
                        for res in best_resources_candidates
                        if res.instance_type is not None
                    ])
                    candidate_str = resources_utils.format_resource(
                        best_resources, simplified_only=True)[0]

                    logger.info(
                        f'{colorama.Style.DIM}🔍 Multiple {cloud} instances '
                        f'satisfy {acc_name}:{int(acc_count)}. '
                        f'The cheapest {candidate_str} is considered '
                        f'among: {", ".join(instance_list)}.'
                        f'{colorama.Style.RESET_ALL}')
        if is_multi_instances:
            assert acc_name is not None
            logger.info(
                f'To list more details, run: sky gpus list {acc_name}\n')


def print_job_group_plan(tasks: list[task_lib.Task], *, logger) -> None:
    """Print the optimizer table for a job group."""
    resource_fields = ['INFRA', 'INSTANCE', 'vCPUs', 'Mem(GB)', 'GPUS']
    table = _create_table(['TASK', '#NODES'] + resource_fields)

    rows = []
    for task in tasks:
        best_resources = task.best_resources
        if best_resources is None:
            continue

        # Get instance type string (display '-' for K8s/Slurm in table)
        instance_type = best_resources.instance_type
        if instance_type is None:
            display_instance_type = '-'
        elif isinstance(best_resources.cloud,
                        (clouds.Kubernetes, clouds.Slurm)):
            display_instance_type = '-'
        else:
            display_instance_type = instance_type

        # Get vCPUs and memory
        vcpus = '-'
        mem = '-'
        if best_resources.cloud is not None and instance_type is not None:
            cloud = best_resources.cloud
            vcpus_, mem_ = cloud.get_vcpus_mem_from_instance_type(instance_type)
            if vcpus_ is not None:
                vcpus = (str(int(vcpus_))
                         if vcpus_.is_integer() else f'{vcpus_:.1f}')
            if mem_ is not None:
                mem = (str(int(mem_)) if mem_.is_integer() else f'{mem_:.1f}')

        # Get accelerators
        accelerators = best_resources.get_accelerators_str()

        # Get spot string
        spot = best_resources.get_spot_str()

        # Get infra string
        infra = best_resources.infra.formatted_str()

        row = [
            task.name,
            str(task.num_nodes), infra, display_instance_type + spot, vcpus,
            mem,
            str(accelerators)
        ]
        rows.append(row)

    if rows:
        table.add_rows(rows)
        logger.info(f'{colorama.Style.BRIGHT}Best plan: '
                    f'{colorama.Style.RESET_ALL}')
        logger.info(f'{table}')
