"""Durable container-image snapshots for controller-owned workloads."""

from collections.abc import Sequence
import dataclasses
import typing

from sky.container_images import core
from sky.container_images import models

if typing.TYPE_CHECKING:
    from sky import task as task_lib


@dataclasses.dataclass(frozen=True)
class _TaskSnapshotGroup:
    task: 'task_lib.Task'
    resource_type: type
    resources: tuple[typing.Any, ...]
    images: tuple[models.ContainerImage, ...]


def snapshot_task_container_images(
    tasks: Sequence['task_lib.Task'],
    workspace: str | None = None,
) -> list[str]:
    """Pins every controller-owned task before durable YAML serialization.

    Each task's resource candidates must converge independently. All managed
    source publications across the full task set commit atomically, so a
    conflict in a later DAG task cannot leave earlier catalog mutations.
    """
    groups: list[_TaskSnapshotGroup] = []
    for task in tasks:
        resource_type = type(task.resources)
        resources = tuple(task.resources)
        images = tuple(resource.container_image for resource in resources)
        if not images or all(image is None for image in images):
            continue
        legacy_direct = tuple(
            image is not None and resource.container_image_from_legacy_image_id
            and image.distribution is None
            for resource, image in zip(resources, images))
        # Legacy image_id candidates historically support heterogeneous task
        # shapes: for example, Kubernetes placements may run in a Docker image
        # while VM placements use the host image and launch Docker from `run`.
        # They also support different direct images by placement. Keep that
        # behavior outside the managed catalog until the task explicitly opts
        # in with resources.container_image.
        if all(image is None or legacy
               for image, legacy in zip(images, legacy_direct)):
            continue
        if any(image is None for image in images) or any(legacy_direct):
            raise ValueError(
                'All resource candidates in one workload version must '
                'resolve to the same immutable container artifact. Use one '
                'managed artifact across placements or one identical direct '
                'image.')
        concrete_images = typing.cast(tuple[models.ContainerImage, ...], images)
        groups.append(
            _TaskSnapshotGroup(task, resource_type, resources, concrete_images))

    if not groups:
        return []
    snapshots_by_group = core.snapshot_for_deployment_groups(
        [group.images for group in groups], workspace)
    artifact_ids: list[str] = []
    for group, snapshots in zip(groups, snapshots_by_group):
        rewritten = []
        for resource, (snapshot, artifact_id) in zip(group.resources,
                                                     snapshots):
            if artifact_id is None:
                rewritten.append(resource)
            else:
                artifact_ids.append(artifact_id)
                rewritten.append(resource.copy(container_image=snapshot))
        group.task.set_resources(group.resource_type(rewritten))
    return artifact_ids
