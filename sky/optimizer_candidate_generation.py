"""Candidate generation and resource validation for the optimizer."""

import collections
from collections.abc import Iterable
from typing import Any

import colorama

from sky import check as sky_check
from sky import clouds
from sky import dag as dag_lib
from sky import exceptions
from sky import resources as resources_lib
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.clouds import cloud as sky_cloud
from sky.container_images import config as container_image_config
from sky.container_images import models as container_image_models
from sky.container_images import runtime as container_image_runtime
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils import registry
from sky.utils import resources_utils
from sky.utils import subprocess_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger('sky.optimizer')

_PerCloudCandidates = dict[clouds.Cloud, list[resources_lib.Resources]]


def _managed_image_placement(
    resources: resources_lib.Resources,
    workspace: str,
) -> container_image_models.Placement:
    cloud = resources.cloud
    assert cloud is not None, resources
    assert resources.region is not None, resources
    if isinstance(cloud, clouds.AWS):
        provider = 'aws'
        backend = 'aws_vm'
    elif isinstance(cloud, clouds.Kubernetes):
        image = resources.container_image
        assert image is not None, resources
        if container_image_config.is_declared_managed_eks_context(
                image, resources.region, workspace):
            provider = 'aws'
            backend = 'aws_eks'
        else:
            provider = 'kubernetes'
            backend = 'direct'
    else:
        provider = str(cloud).lower()
        backend = 'direct'
    architecture = None
    if resources.instance_type is not None:
        try:
            architecture = cloud.get_arch_from_instance_type(
                resources.instance_type)
        except NotImplementedError:
            pass
    platform = container_image_models.runtime_platform_from_architecture(
        architecture)
    host_image_id = None
    if resources.image_id is not None:
        host_image_id = resources.image_id.get(resources.region,
                                               resources.image_id.get(None))
    return container_image_models.Placement(provider=provider,
                                            region=resources.region,
                                            backend=backend,
                                            platform=platform,
                                            host_image_id=host_image_id)


def _prepare_managed_image_candidates(
    candidates: list[resources_lib.Resources],
    cache: dict[tuple[Any, ...], Any],
    locality_ranks: dict[resources_lib.Resources, int],
) -> list[resources_lib.Resources]:
    workspace = (skypilot_config.get_active_workspace() or
                 skylet_constants.SKYPILOT_DEFAULT_WORKSPACE)
    prepared = []
    for candidate in candidates:
        placement = _managed_image_placement(candidate, workspace)
        result = container_image_runtime.prepare_metadata_only_with_rank(
            candidate, placement, workspace, cache)
        if result is not None:
            eligible, rank = result
            prepared.append(eligible)
            locality_ranks[eligible] = min(rank,
                                           locality_ranks.get(eligible, rank))
    return prepared


def _filter_managed_image_locality(
    launchable: dict[resources_lib.Resources, list[resources_lib.Resources]],
    locality_ranks: dict[resources_lib.Resources, int],
) -> None:
    """Keeps the best image locality class across all task alternatives."""
    managed_candidates = [
        candidate for requested, candidates in launchable.items()
        if requested.container_image is not None for candidate in candidates
    ]
    if not managed_candidates:
        return
    winning_rank = min(
        locality_ranks[candidate] for candidate in managed_candidates)
    for requested, candidates in launchable.items():
        if requested.container_image is None:
            continue
        launchable[requested] = [
            candidate for candidate in candidates
            if locality_ranks[candidate] == winning_rank
        ]


def filter_out_blocked_launchable_resources(
        launchable_resources: Iterable[resources_lib.Resources],
        blocked_resources: Iterable[resources_lib.Resources]):
    """Whether the resources are blocked."""
    available_resources = []
    for resources in launchable_resources:
        for blocked in blocked_resources:
            if resources.should_be_blocked_by(blocked):
                break
        else:  # non-blocked launchable resources. (no break)
            available_resources.append(resources)
    return available_resources


def check_specified_clouds(dag: 'dag_lib.Dag') -> None:
    """Check if specified clouds are enabled in cache and refresh if needed.

    Our enabled cloud list is cached in a local database, and if a user
    specified a cloud that is not enabled, we should refresh the cache for that
    cloud in case the cloud access has been enabled since the last cache update.

    Args:
        dag: The DAG specified by a user.
    """
    enabled_clouds = sky_check.get_cached_enabled_clouds_or_refresh(
        capability=sky_cloud.CloudCapability.COMPUTE,
        raise_if_no_cloud_access=True)

    global_disabled_clouds: set[str] = set()
    for task in dag.tasks:
        # Recheck the enabled clouds if the task's requested resources are on a
        # cloud that is not enabled in the cached enabled_clouds.
        all_clouds_specified: set[str] = set()
        clouds_need_recheck: set[str] = set()
        for resources in task.resources:
            cloud_str = str(resources.cloud)
            if (resources.cloud is not None and not clouds.cloud_in_iterable(
                    resources.cloud, enabled_clouds)):
                # Explicitly check again to update the enabled cloud list.
                clouds_need_recheck.add(cloud_str)
            all_clouds_specified.add(cloud_str)

        # Explicitly check again to update the enabled cloud list.
        clouds_to_check_again = list(clouds_need_recheck -
                                     global_disabled_clouds)
        if len(clouds_to_check_again) > 0:
            sky_check.check_capability(
                sky_cloud.CloudCapability.COMPUTE,
                quiet=True,
                clouds=clouds_to_check_again,
                workspace=skypilot_config.get_active_workspace())
        enabled_clouds = sky_check.get_cached_enabled_clouds_or_refresh(
            capability=sky_cloud.CloudCapability.COMPUTE,
            raise_if_no_cloud_access=True)
        disabled_clouds = (clouds_need_recheck -
                           {str(c) for c in enabled_clouds})
        global_disabled_clouds.update(disabled_clouds)
        if disabled_clouds:
            is_or_are = 'is' if len(disabled_clouds) == 1 else 'are'
            task_name = f' {task.name!r}' if task.name is not None else ''
            disabled_display_names = []
            for c in disabled_clouds:
                cloud_obj_one = registry.CLOUD_REGISTRY.from_str(c)
                if cloud_obj_one is not None:
                    disabled_display_names.append(cloud_obj_one.display_name())
            cloud_names = ', '.join(disabled_display_names)
            msg = (f'Task{task_name} requires {cloud_names} '
                   f'which {is_or_are} not enabled. To enable access, change '
                   f'the task cloud requirement or run: {colorama.Style.BRIGHT}'
                   f'sky check {" ".join(c.lower() for c in disabled_clouds)}'
                   f'{colorama.Style.RESET_ALL}')
            if all_clouds_specified == disabled_clouds:
                # If all resources are specified with a disabled cloud, we
                # should raise an error as no resource can satisfy the
                # requirement. Otherwise, we should just skip the resource.
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.ResourcesUnavailableError(msg)
            logger.warning(
                f'{colorama.Fore.YELLOW}{msg}{colorama.Style.RESET_ALL}')

        check_specified_regions(task)


def check_specified_regions(task: task_lib.Task) -> None:
    """Check if specified regions (Kubernetes/SSH contexts) are enabled.

    Args:
        task: The task to check.
    """
    # Only check for Kubernetes/SSH for now
    # Below check works because SSH inherits Kubernetes cloud.
    if not all(
            isinstance(resources.cloud, clouds.Kubernetes)
            for resources in task.resources):
        return
    # Kubernetes region is a context if set
    for resources in task.resources:
        if resources.region is None:
            continue

        is_ssh = isinstance(resources.cloud, clouds.SSH)
        if is_ssh:
            existing_contexts = clouds.SSH.existing_allowed_contexts()
        else:
            existing_contexts = clouds.Kubernetes.existing_allowed_contexts()

        region = resources.region
        task_name = f' {task.name!r}' if task.name is not None else ''
        msg = f'Task{task_name} requires '
        if region not in existing_contexts:
            if is_ssh:
                infra_str = f'SSH/{common_utils.removeprefix(region, "ssh-")}'
            else:
                infra_str = f'Kubernetes/{region}'
            logger.warning(f'{infra_str} is not enabled.')
            volume_mounts_str = ''
            if task.volume_mounts:
                if len(task.volume_mounts) > 1:
                    volume_mounts_str += 'volumes '
                else:
                    volume_mounts_str += 'volume '
                volume_mounts_str += ', '.join(
                    [f'{v.volume_name}' for v in task.volume_mounts])
                volume_mounts_str += f' with infra {infra_str}'
            if volume_mounts_str:
                msg += volume_mounts_str
            else:
                msg += f'infra {infra_str}'
            msg += (
                f' which is not enabled. To enable access, change '
                f'the task infra requirement or run: {colorama.Style.BRIGHT}'
                f'sky check {colorama.Style.RESET_ALL}'
                f'to ensure the infra is enabled.')
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesUnavailableError(msg)


def fill_in_launchable_resources(
    task: task_lib.Task,
    blocked_resources: Iterable[resources_lib.Resources] | None,
    quiet: bool = False
) -> tuple[dict[resources_lib.Resources, list[resources_lib.Resources]],
           _PerCloudCandidates, list[str], dict[resources_lib.Resources,
                                                list[str]]]:
    """Fills in the launchable resources for the task.

    Returns:
      A tuple of:
        Dict mapping the task's requested Resources to a list of launchable
          Resources,
        Dict mapping Cloud to a list of feasible Resources (for printing),
        Sorted list of fuzzy candidates (alternative GPU names).
        Dict mapping requested Resources and a list of hints for why the
          resource is unavailable if so.
    Raises:
      ResourcesUnavailableError: if all resources required by the task are on
        a cloud that is not enabled.
    """
    enabled_clouds = sky_check.get_cached_enabled_clouds_or_refresh(
        capability=sky_cloud.CloudCapability.COMPUTE,
        raise_if_no_cloud_access=True)

    launchable: dict[resources_lib.Resources, list[resources_lib.Resources]] = (
        collections.defaultdict(list))
    all_fuzzy_candidates = set()
    cloud_candidates: _PerCloudCandidates = collections.defaultdict(list)
    resource_hints: dict[resources_lib.Resources,
                         list[str]] = collections.defaultdict(list)
    image_metadata_cache: dict[tuple[Any, ...], Any] = {}
    image_locality_ranks: dict[resources_lib.Resources, int] = {}
    if blocked_resources is None:
        blocked_resources = []
    for resources in task.resources:
        # Validate the resources first which may fill in missing fields
        # automatically for the resources.
        resources.validate()
        if (resources.cloud is not None and
                not clouds.cloud_in_iterable(resources.cloud, enabled_clouds)):
            # Skip the resources that are on a cloud that is not enabled. The
            # hint has been printed in _check_specified_clouds.
            launchable[resources] = []
            continue
        clouds_list = ([resources.cloud]
                       if resources.cloud is not None else enabled_clouds)
        # If clouds provide hints, store them for later printing.
        hints: dict[clouds.Cloud, str] = {}

        feasible_list = subprocess_utils.run_in_parallel(
            lambda cloud, r=resources, n=task.num_nodes:
            (cloud, cloud.get_feasible_launchable_resources(r, n)),
            clouds_list)
        for cloud, feasible_resources in feasible_list:
            if feasible_resources.hint is not None:
                hints[cloud] = feasible_resources.hint
                resource_hints[resources].append(feasible_resources.hint)
            if feasible_resources.resources_list:
                # Assume feasible_resources is sorted by prices. Guaranteed by
                # the implementation of get_feasible_launchable_resources and
                # the underlying catalog filtering
                cheapest = feasible_resources.resources_list[0]
                # Generate region/zone-specified resources.
                generated = (resources_utils.
                             make_launchables_for_valid_region_zones(cheapest))
                eligible = generated
                if resources.container_image is not None:
                    eligible = _prepare_managed_image_candidates(
                        generated, image_metadata_cache, image_locality_ranks)
                launchable[resources].extend(eligible)
                # Each cloud can occur multiple times in feasible_list,
                # for different region/zone.
                if eligible:
                    cloud_candidates[cloud].extend(
                        feasible_resources.resources_list)
                elif resources.container_image is not None:
                    resource_hints[resources].append(
                        'Managed container image policy excludes every '
                        f'candidate on {cloud.display_name()}.')
            else:
                all_fuzzy_candidates.update(
                    feasible_resources.fuzzy_candidate_list)
        launchable[resources] = filter_out_blocked_launchable_resources(
            launchable[resources], blocked_resources)
        if not launchable[resources]:
            clouds_str = str(clouds_list) if len(clouds_list) > 1 else str(
                clouds_list[0])
            num_node_str = ''
            if task.num_nodes > 1:
                num_node_str = f'{task.num_nodes}x '
            if not (quiet or resources.no_missing_accel_warnings):
                logger.info(
                    f'No resource satisfying {num_node_str}'
                    f'{resources.repr_with_region_zone} on {clouds_str}.')
                if all_fuzzy_candidates:
                    logger.info('Did you mean: '
                                f'{colorama.Fore.CYAN}'
                                f'{sorted(all_fuzzy_candidates)}'
                                f'{colorama.Style.RESET_ALL}')
                else:
                    if resources.cpus is not None:
                        logger.info(f'{colorama.Fore.LIGHTBLACK_EX}'
                                    '- Try specifying a different CPU count, '
                                    'or add "+" to the end of the CPU count '
                                    'to allow for larger instances.'
                                    f'{colorama.Style.RESET_ALL}')
                    if resources.memory is not None:
                        logger.info(f'{colorama.Fore.LIGHTBLACK_EX}'
                                    '- Try specifying a different memory size, '
                                    'or add "+" to the end of the memory size '
                                    'to allow for larger instances.'
                                    f'{colorama.Style.RESET_ALL}')
                    if resources.local_disk is not None:
                        logger.info(
                            f'{colorama.Fore.LIGHTBLACK_EX}'
                            '- Try using "+" suffix for at-least matching '
                            '(e.g., "nvme:500+"), or reduce the size '
                            f'requirement. {colorama.Style.RESET_ALL}')
                    if resources.max_hourly_cost is not None:
                        logger.info(f'{colorama.Fore.LIGHTBLACK_EX}'
                                    '- Max hourly cost limit '
                                    f'(${resources.max_hourly_cost}/hr) may be '
                                    'too restrictive. Try increasing the limit '
                                    'or removing it to see available options.'
                                    f'{colorama.Style.RESET_ALL}')
                for cloud, hint in hints.items():
                    logger.info(f'{colorama.Fore.LIGHTBLACK_EX}'
                                f'{repr(cloud)}: {hint}'
                                f'{colorama.Style.RESET_ALL}')
    _filter_managed_image_locality(launchable, image_locality_ranks)
    return launchable, cloud_candidates, list(
        sorted(all_fuzzy_candidates)), resource_hints
