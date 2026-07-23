"""Shared runtime placement classification for managed container images."""

from sky import clouds
from sky import resources as resources_lib
from sky.container_images import config
from sky.container_images import models


def classify(resources: resources_lib.Resources,
             workspace: str) -> models.Placement:
    """Classifies one concrete resource candidate without provider calls."""
    cloud = resources.cloud
    assert cloud is not None, resources
    assert resources.region is not None, resources
    image = resources.container_image
    assert image is not None, resources

    architecture = None
    if resources.instance_type is not None:
        try:
            architecture = cloud.get_arch_from_instance_type(
                resources.instance_type)
        except NotImplementedError:
            pass
    platform = models.runtime_platform_from_architecture(architecture)

    if isinstance(cloud, clouds.AWS):
        provider = 'aws'
        backend = 'aws_vm'
    elif isinstance(cloud, clouds.Kubernetes):
        exact_ref_only = (image.ref is not None and image.release is None and
                          image.artifact_id is None)
        declared_eks = False
        if not (exact_ref_only and
                platform not in (None, models.V0_MANAGED_RUNTIME_PLATFORM)):
            try:
                declared_eks = config.is_declared_managed_eks_context(
                    image, resources.region, workspace)
            except ValueError:
                if not exact_ref_only:
                    raise
        if declared_eks:
            provider = 'aws'
            backend = 'aws_eks'
        else:
            provider = 'kubernetes'
            backend = 'direct'
    else:
        provider = str(cloud).lower()
        backend = 'direct'

    host_image_id = None
    if resources.image_id is not None:
        host_image_id = resources.image_id.get(resources.region,
                                               resources.image_id.get(None))
    return models.Placement(provider=provider,
                            region=resources.region,
                            backend=backend,
                            platform=platform,
                            host_image_id=host_image_id)
