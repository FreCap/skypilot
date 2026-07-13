"""This module provides functions to generate GraphQL mutations for deploying
spot instance Pods on RunPod.

Reference:
    https://github.com/runpod/runpod-python/blob/main/runpod/api/ctl_commands.py

Functions:
    generate_spot_pod_deployment_mutation: Generates a GraphQL mutation string
        for deploying a spot instance Pod on RunPod.

Example:
    >>> mutation = generate_spot_pod_deployment_mutation(
            name='test',
            image_name='runpod/stack',
            gpu_type_id='NVIDIA GeForce RTX 3070',
            bid_per_gpu=0.3
        )
"""

from sky.adaptors import runpod
from sky.provision.runpod.api.pods import generate_spot_pod_deployment_mutation

_INTERRUPTABLE_POD_FIELD: str = 'podRentInterruptable'
_RESPONSE_DATA_FIELD: str = 'data'


def create_spot_pod(
    name: str,
    image_name: str,
    gpu_type_id: str,
    bid_per_gpu: float,
    cloud_type: str = 'ALL',
    volume_mount_path: str = '/runpod-volume',
    gpu_count: int | None = 1,
    min_memory_in_gb: int | None = 1,
    min_vcpu_count: int | None = 1,
    container_disk_in_gb: int | None = None,
    volume_in_gb: int | None = 0,
    ports: str | None = None,
    start_ssh: bool | None = True,
    start_jupyter: bool | None = False,
    env: dict | None = None,
    docker_args: str | None = '',
    support_public_ip: bool | None = True,
    terminate_after: str | None = None,
    stop_after: str | None = None,
    data_center_id: str | None = None,
    country_code: str | None = None,
    network_volume_id: str | None = None,
    allowed_cuda_versions: list[str] | None = None,
    min_download: int | None = None,
    min_upload: int | None = None,
    cuda_version: str | None = None,
    template_id: str | None = None,
    volume_key: str | None = None,
) -> dict:
    """This module provides functions to generate GraphQL mutations for
    deploying spot instance Pods on RunPod.

    Functions:
        generate_spot_pod_deployment_mutation: Generates a GraphQL mutation
            string for deploying a spot instance Pod on RunPod.

    Example:
        >>> mutation = generate_spot_pod_deployment_mutation(
                name='test',
                image_name='runpod/stack',
                gpu_type_id='NVIDIA GeForce RTX 3070',
                bid_per_gpu=0.3
            )
    """
    runpod.runpod.get_gpu(gpu_type_id)
    # refer to https://graphql-spec.runpod.io/#definition-CloudTypeEnum
    if cloud_type not in ['ALL', 'COMMUNITY', 'SECURE']:
        raise ValueError('cloud_type must be one of ALL, COMMUNITY or SECURE')

    if network_volume_id and data_center_id is None:
        user_info = runpod.runpod.get_user()
        for network_volume in user_info['networkVolumes']:
            if network_volume['id'] == network_volume_id:
                data_center_id = network_volume['dataCenterId']
                break

    if container_disk_in_gb is None and template_id is None:
        container_disk_in_gb = 10

    mutation = generate_spot_pod_deployment_mutation(
        name=name,
        image_name=image_name,
        gpu_type_id=gpu_type_id,
        bid_per_gpu=bid_per_gpu,
        cloud_type=cloud_type,
        gpu_count=gpu_count,
        min_memory_in_gb=min_memory_in_gb,
        min_vcpu_count=min_vcpu_count,
        container_disk_in_gb=container_disk_in_gb,
        volume_in_gb=volume_in_gb,
        volume_mount_path=volume_mount_path,
        ports=ports,
        start_ssh=start_ssh,
        start_jupyter=start_jupyter,
        env=env,
        docker_args=docker_args,
        support_public_ip=support_public_ip,
        terminate_after=terminate_after,
        stop_after=stop_after,
        data_center_id=data_center_id,
        country_code=country_code,
        network_volume_id=network_volume_id,
        allowed_cuda_versions=allowed_cuda_versions,
        min_download=min_download,
        min_upload=min_upload,
        cuda_version=cuda_version,
        template_id=template_id,
        volume_key=volume_key,
    )
    response = runpod.runpod.api.graphql.run_graphql_query(mutation)
    return response[_RESPONSE_DATA_FIELD][_INTERRUPTABLE_POD_FIELD]
