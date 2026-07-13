""" Vast | Catalog

This module loads the service catalog file and can be used to
query instance types and pricing information for Vast.ai.
"""

import typing

import pandas as pd

from sky.catalog import common
from sky.utils import resources_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky.clouds import cloud

_df = common.read_catalog('vast/vms.csv')


def _apply_datacenter_filter(df: pd.DataFrame,
                             datacenter_only: bool) -> pd.DataFrame:
    """Filter dataframe by hosting_type if datacenter_only is True.

    hosting_type: 0 = Consumer hosted, 1 = Datacenter hosted
    """
    if not datacenter_only or 'HostingType' not in df.columns:
        return df
    return df[df['HostingType'] >= 1]


def instance_type_exists(instance_type: str) -> bool:
    return common.instance_type_exists_impl(_df, instance_type)


def validate_region_zone(region: str | None,
                         zone: str | None) -> tuple[str | None, str | None]:
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Vast does not support zones.')
    return common.validate_region_zone_impl('vast', _df, region, zone)


def get_hourly_cost(instance_type: str,
                    use_spot: bool = False,
                    region: str | None = None,
                    zone: str | None = None) -> float:
    """Returns the cost, or the cheapest cost among all zones for spot."""
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Vast does not support zones.')
    return common.get_hourly_cost_impl(_df, instance_type, use_spot, region,
                                       zone)


def get_vcpus_mem_from_instance_type(
        instance_type: str) -> tuple[float | None, float | None]:
    return common.get_vcpus_mem_from_instance_type_impl(_df, instance_type)


def get_default_instance_type(cpus: str | None = None,
                              memory: str | None = None,
                              disk_tier: resources_utils.DiskTier | None = None,
                              local_disk: str | None = None,
                              region: str | None = None,
                              zone: str | None = None,
                              use_spot: bool = False,
                              max_hourly_cost: float | None = None,
                              datacenter_only: bool = False) -> str | None:
    del disk_tier, local_disk
    # NOTE: After expanding catalog to multiple entries, you may
    # want to specify a default instance type or family.
    df = _apply_datacenter_filter(_df, datacenter_only)
    return common.get_instance_type_for_cpus_mem_impl(df, cpus, memory, region,
                                                      zone, use_spot,
                                                      max_hourly_cost)


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
        datacenter_only: bool = False) -> tuple[list[str] | None, list[str]]:
    """Returns a list of instance types that have the given accelerator.

    Args:
        datacenter_only: If True, only return instances hosted in datacenters
            (hosting_type >= 1).
    """
    del local_disk  # unused
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Vast does not support zones.')
    df = _apply_datacenter_filter(_df, datacenter_only)
    return common.get_instance_type_for_accelerator_impl(
        df=df,
        acc_name=acc_name,
        acc_count=acc_count,
        cpus=cpus,
        memory=memory,
        use_spot=use_spot,
        region=region,
        zone=zone,
        max_hourly_cost=max_hourly_cost)


def get_region_zones_for_instance_type(instance_type: str,
                                       use_spot: bool) -> list['cloud.Region']:
    df = _df[_df['InstanceType'] == instance_type]
    return common.get_region_zones(df, use_spot)


# TODO: this differs from the fluffy catalog version
def list_accelerators(
        gpus_only: bool,
        name_filter: str | None,
        region_filter: str | None,
        quantity_filter: int | None,
        case_sensitive: bool = True,
        all_regions: bool = False,
        require_price: bool = True) -> dict[str, list[common.InstanceTypeInfo]]:
    """Returns all instance types in Vast offering GPUs."""
    del require_price  # Unused.
    return common.list_accelerators_impl('Vast', _df, gpus_only, name_filter,
                                         region_filter, quantity_filter,
                                         case_sensitive, all_regions)
