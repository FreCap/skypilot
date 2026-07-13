"""IBM Offerings Catalog.

This module loads the service catalog file and can be used to query
instance types and pricing information for IBM.
"""

from sky import sky_logging
from sky.adaptors import ibm
from sky.catalog import common
from sky.clouds import cloud
from sky.utils import resources_utils

logger = sky_logging.init_logger(__name__)

_DEFAULT_INSTANCE_FAMILY = 'bx2'
_DEFAULT_NUM_VCPUS = '8'
_DEFAULT_MEMORY = 32

_df = common.read_catalog('ibm/vms.csv')


def instance_type_exists(instance_type: str) -> bool:
    return common.instance_type_exists_impl(_df, instance_type)


def validate_region_zone(region: str | None, zone: str | None):
    return common.validate_region_zone_impl('IBM', _df, region, zone)


def get_hourly_cost(instance_type: str,
                    use_spot: bool = False,
                    region: str | None = None,
                    zone: str | None = None) -> float:
    return common.get_hourly_cost_impl(_df, instance_type, use_spot, region,
                                       zone)


def get_vcpus_mem_from_instance_type(
        instance_type: str) -> tuple[float | None, float | None]:
    return common.get_vcpus_mem_from_instance_type_impl(_df, instance_type)


def get_accelerators_from_instance_type(
        instance_type: str) -> dict[str, int | float] | None:
    return common.get_accelerators_from_instance_type_impl(_df, instance_type)


def get_instance_type_for_accelerator(
    acc_name: str,
    acc_count: int,
    cpus: str | None = None,
    memory: str | None = None,
    use_spot: bool = False,
    local_disk: str | None = None,
    region: str | None = None,
    zone: str | None = None,
    max_hourly_cost: float | None = None,
) -> tuple[list[str] | None, list[str]]:
    """Filter the instance types based on resource requirements.

    Returns a list of instance types satisfying the required count of
    accelerators with sorted prices and a list of candidates with fuzzy search.
    """
    del local_disk  # unused
    return common.get_instance_type_for_accelerator_impl(
        df=_df,
        acc_name=acc_name,
        acc_count=acc_count,
        cpus=cpus,
        memory=memory,
        use_spot=use_spot,
        region=region,
        zone=zone,
        max_hourly_cost=max_hourly_cost)


def get_region_zones_for_instance_type(instance_type: str,
                                       use_spot: bool) -> list[cloud.Region]:
    df = _df[_df['InstanceType'] == instance_type]
    return common.get_region_zones(df, use_spot)


def list_accelerators(
        gpus_only: bool,
        name_filter: str | None,
        region_filter: str | None,
        quantity_filter: int | None,
        case_sensitive: bool = True,
        all_regions: bool = False,
        require_price: bool = True) -> dict[str, list[common.InstanceTypeInfo]]:
    """Returns all instance types in IBM offering accelerators."""
    del require_price  # Unused.
    return common.list_accelerators_impl('IBM', _df, gpus_only, name_filter,
                                         region_filter, quantity_filter,
                                         case_sensitive, all_regions)


def get_default_instance_type(
        cpus: str | None = None,
        memory: str | None = None,
        disk_tier: resources_utils.DiskTier | None = None,
        local_disk: str | None = None,
        region: str | None = None,
        zone: str | None = None,
        use_spot: bool = False,
        max_hourly_cost: float | None = None) -> str | None:
    del disk_tier, local_disk  # unused
    if cpus is None and memory is None:
        cpus = f'{_DEFAULT_NUM_VCPUS}+'

    if memory is None:
        memory_gb_or_ratio = f'{_DEFAULT_MEMORY}+'
    else:
        memory_gb_or_ratio = memory
    instance_type_prefix = f'{_DEFAULT_INSTANCE_FAMILY}-'
    df = _df[_df['InstanceType'].str.startswith(instance_type_prefix)]
    return common.get_instance_type_for_cpus_mem_impl(df, cpus,
                                                      memory_gb_or_ratio,
                                                      region, zone, use_spot,
                                                      max_hourly_cost)


def is_image_tag_valid(tag: str, region: str | None) -> bool:
    """Returns whether the image tag is valid."""
    vpc_client = ibm.client(region=region)
    try:
        vpc_client.get_image(tag)
    except ibm.ibm_cloud_sdk_core.ApiException as e:
        logger.error(e.message)
        return False
    return True
