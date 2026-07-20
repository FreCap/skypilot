"""Mutation-free validation for controller-owned image selectors."""

from collections.abc import Sequence
import typing

from sky.container_images import models

if typing.TYPE_CHECKING:
    from sky import task as task_lib


def _identity(image: models.ContainerImage) -> tuple[str | None, ...]:
    return (image.ref, image.release, image.artifact_id)


def snapshot_task_container_images(
    tasks: Sequence['task_lib.Task'],
    workspace: str | None = None,
) -> list[str]:
    """Validates durable selectors without adopting sources or publishing.

    Release names are immutable once READY, and digest references are already
    content-addressed. Controllers therefore persist the original selector and
    let placement create only a target demand. ``workspace`` is accepted for
    API compatibility but deliberately causes no catalog read or write.
    """
    del workspace
    artifact_ids: list[str] = []
    for task in tasks:
        resources = tuple(task.resources)
        images = tuple(resource.container_image for resource in resources)
        if not images or all(image is None for image in images):
            continue
        legacy = tuple(
            image is not None and resource.container_image_from_legacy_image_id
            for resource, image in zip(resources, images))
        if all(image is None or is_legacy
               for image, is_legacy in zip(images, legacy)):
            continue
        if any(image is None for image in images) or any(legacy):
            raise ValueError(
                'Every resource candidate in one workload version must use '
                'the same immutable container image identity.')
        concrete = typing.cast(tuple[models.ContainerImage, ...], images)
        first = _identity(concrete[0])
        if any(_identity(image) != first for image in concrete[1:]):
            raise ValueError(
                'Every resource candidate in one workload version must use '
                'the same immutable container image identity.')
        artifact_id = concrete[0].artifact_id
        if artifact_id is not None:
            artifact_ids.append(artifact_id)
    return artifact_ids
