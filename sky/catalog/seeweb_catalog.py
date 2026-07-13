"""Seeweb service catalog.

This module loads the service catalog file and can be used to
query instance types and pricing information for Seeweb.
"""

import typing

from sky.adaptors import common as adaptors_common
from sky.catalog import common
from sky.utils import resources_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import pandas as pd

    from sky.clouds import cloud
else:
    pd = adaptors_common.LazyImport('pandas')

_PULL_FREQUENCY_HOURS = 8
_df = None


def _get_df():
    """Get the dataframe, loading it lazily if needed."""
    global _df
    if _df is None:
        _df = common.read_catalog('seeweb/vms.csv',
                                  pull_frequency_hours=_PULL_FREQUENCY_HOURS)
    return _df


def instance_type_exists(instance_type: str) -> bool:
    result = common.instance_type_exists_impl(_get_df(), instance_type)
    return result


def validate_region_zone(region: str | None,
                         zone: str | None) -> tuple[str | None, str | None]:
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Seeweb does not support zones.')

    result = common.validate_region_zone_impl('Seeweb', _get_df(), region, zone)
    return result


def get_hourly_cost(instance_type: str,
                    use_spot: bool = False,
                    region: str | None = None,
                    zone: str | None = None) -> float:
    """Returns the cost, or the cheapest cost among all zones for spot."""
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Seeweb does not support zones.')

    result = common.get_hourly_cost_impl(_get_df(), instance_type, use_spot,
                                         region, zone)
    return result


def get_vcpus_mem_from_instance_type(
        instance_type: str) -> tuple[float | None, float | None]:
    result = common.get_vcpus_mem_from_instance_type_impl(
        _get_df(), instance_type)
    return result


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
    result = common.get_instance_type_for_cpus_mem_impl(_get_df(), cpus, memory,
                                                        region, zone, use_spot,
                                                        max_hourly_cost)
    return result


def get_accelerators_from_instance_type(
        instance_type: str) -> dict[str, int] | None:
    # Filter the dataframe for the specific instance type
    df = _get_df()
    df_filtered = df[df['InstanceType'] == instance_type]
    if df_filtered.empty:
        return None

    # Get the first row (all rows for same instance
    # type should have same accelerator info)
    row = df_filtered.iloc[0]
    acc_name = row['AcceleratorName']
    acc_count = row['AcceleratorCount']

    # Check if the instance has accelerators
    if pd.isna(acc_name) or pd.isna(
            acc_count) or acc_name == '' or acc_count == '':
        return None

    # Convert accelerator count to int/float
    try:
        if int(acc_count) == acc_count:
            acc_count = int(acc_count)
        else:
            acc_count = float(acc_count)
    except (ValueError, TypeError):
        return None

    result = {acc_name: acc_count}
    return result


def get_instance_type_for_accelerator(
        acc_name: str,
        acc_count: int,
        cpus: str | None = None,
        memory: str | None = None,
        use_spot: bool = False,
        local_disk: str | None = None,
        region: str | None = None,
        zone: str | None = None,
        max_hourly_cost: float | None = None
) -> tuple[list[str] | None, list[str]]:
    """Returns a list of instance types satisfying
    the required count of accelerators."""
    del local_disk  # unused
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Seeweb does not support zones.')

    result = common.get_instance_type_for_accelerator_impl(
        df=_get_df(),
        acc_name=acc_name,
        acc_count=acc_count,
        cpus=cpus,
        memory=memory,
        use_spot=use_spot,
        region=region,
        zone=zone,
        max_hourly_cost=max_hourly_cost)
    return result


def regions() -> list['cloud.Region']:
    result = common.get_region_zones(_get_df(), use_spot=False)
    return result


def get_region_zones_for_instance_type(instance_type: str,
                                       use_spot: bool = False
                                      ) -> list['cloud.Region']:
    """Returns a list of regions for a given instance type."""
    # Filter the dataframe for the specific instance type
    df = _get_df()
    df_filtered = df[df['InstanceType'] == instance_type]
    if df_filtered.empty:
        return []

    # Use common.get_region_zones() like all other providers
    region_list = common.get_region_zones(df_filtered, use_spot)

    # Default region: Frosinone (it-fr2)
    # Other regions: Milano (it-mi2), Lugano (ch-lug1), Bulgaria (bg-sof1)
    priority_regions = ['it-fr2']
    prioritized_regions = []
    other_regions = []

    # First, add regions in priority order if they exist
    for priority_region in priority_regions:
        for region in region_list:
            if region.name == priority_region:
                prioritized_regions.append(region)
                break

    # Then, add any remaining regions that weren't in the priority list
    for region in region_list:
        if region.name not in priority_regions:
            other_regions.append(region)

    result = prioritized_regions + other_regions
    return result


def list_accelerators(
        gpus_only: bool,
        name_filter: str | None,
        region_filter: str | None,
        quantity_filter: int | None,
        case_sensitive: bool = True,
        all_regions: bool = False,
        require_price: bool = True) -> dict[str, list[common.InstanceTypeInfo]]:
    """Lists accelerators offered in Seeweb."""
    # Filter out rows with empty or null regions (indicating unavailability)
    df = _get_df()
    df_filtered = df.dropna(subset=['Region'])
    df_filtered = df_filtered[df_filtered['Region'].str.strip() != '']

    result = common.list_accelerators_impl('Seeweb', df_filtered, gpus_only,
                                           name_filter, region_filter,
                                           quantity_filter, case_sensitive,
                                           all_regions, require_price)
    return result
