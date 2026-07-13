"""Nebius Catalog.

This module loads the service catalog file and can be used to query
instance types and pricing information for Nebius.
"""
import typing

from sky.catalog import common
from sky.clouds import Nebius
from sky.utils import resources_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky.clouds import cloud

# Keep it synced with the frequency in
# skypilot-catalog/.github/workflows/update-Nebius-catalog.yml
_PULL_FREQUENCY_HOURS = 7

_df = common.read_catalog('nebius/vms.csv')


def instance_type_exists(instance_type: str) -> bool:
    return common.instance_type_exists_impl(_df, instance_type)


def validate_region_zone(region: str | None,
                         zone: str | None) -> tuple[str | None, str | None]:
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Nebius does not support zones.')
    return common.validate_region_zone_impl('nebius', _df, region, zone)


def get_hourly_cost(instance_type: str,
                    use_spot: bool = False,
                    region: str | None = None,
                    zone: str | None = None) -> float:
    """Returns the cost, or the cheapest cost among all zones for spot."""
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Nebius does not support zones.')
    return common.get_hourly_cost_impl(_df, instance_type, use_spot, region,
                                       zone)


def get_vcpus_mem_from_instance_type(
        instance_type: str) -> tuple[float | None, float | None]:
    return common.get_vcpus_mem_from_instance_type_impl(_df, instance_type)


def get_default_instance_type(
        cpus: str | None = None,
        memory: str | None = None,
        disk_tier: resources_utils.DiskTier | None = None,
        local_disk: str | None = None,
        region: str | None = None,
        zone: str | None = None,
        use_spot: bool = False,
        max_hourly_cost: float | None = None) -> str | None:
    del local_disk  # unused

    def _filter_disk_type(instance_type: str) -> bool:
        valid, _ = Nebius.check_disk_tier(instance_type, disk_tier)
        return valid

    df = _df.loc[_df['InstanceType'].apply(_filter_disk_type)]
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
        max_hourly_cost: float | None = None
) -> tuple[list[str] | None, list[str]]:
    """Filter the instance types based on resource requirements.

    Returns a list of instance types satisfying the required count of
    accelerators with sorted prices and a list of candidates with fuzzy search.
    """
    del local_disk  # unused
    if zone is not None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError('Nebius does not support zones.')
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


def regions() -> list['cloud.Region']:
    return common.get_region_zones(_df, use_spot=False)


def get_region_zones_for_instance_type(instance_type: str,
                                       use_spot: bool) -> list['cloud.Region']:
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
    """Returns all instance types in Nebius offering GPUs."""

    del require_price  # Unused.
    return common.list_accelerators_impl('nebius', _df, gpus_only, name_filter,
                                         region_filter, quantity_filter,
                                         case_sensitive, all_regions)
