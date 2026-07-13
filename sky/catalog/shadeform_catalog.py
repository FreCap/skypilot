""" Shadeform | Catalog

This module loads pricing and instance information from the Shadeform API
and can be used to query instance types and pricing information for Shadeform.
"""

import typing

from sky.adaptors import common as adaptors_common
from sky.catalog import common

if typing.TYPE_CHECKING:
    import pandas as pd

    from sky.clouds import cloud
else:
    pd = adaptors_common.LazyImport('pandas')

# We'll use dynamic fetching, so no static CSV file to load
_df = None


def _get_df():
    """Get the dataframe, fetching from API if needed."""
    global _df
    if _df is None:
        # For now, we'll fall back to a minimal static catalog
        # In a full implementation, this would call the Shadeform API
        # to dynamically fetch the latest instance types and pricing
        try:
            df = common.read_catalog('shadeform/vms.csv')
        except FileNotFoundError:
            # If no static catalog exists, create an empty one
            # This would be replaced with dynamic API fetching
            _df = pd.DataFrame(columns=[
                'InstanceType', 'AcceleratorName', 'AcceleratorCount', 'vCPUs',
                'MemoryGiB', 'Price', 'Region', 'GpuInfo', 'SpotPrice'
            ])
        else:
            df = df[df['InstanceType'].notna()]
            if 'AcceleratorName' in df.columns:
                df = df[df['AcceleratorName'].notna()]
                df = df.assign(AcceleratorName=df['AcceleratorName'].astype(
                    str).str.strip())
            _df = df.reset_index(drop=True)
    return _df


def _is_not_found_error(err: ValueError) -> bool:
    msg = str(err).lower()
    return 'not found' in msg or 'not supported' in msg


def _call_or_default(func, default):
    try:
        return func()
    except ValueError as err:
        if _is_not_found_error(err):
            return default
        raise


def instance_type_exists(instance_type: str) -> bool:
    """Check if an instance type exists."""
    return common.instance_type_exists_impl(_get_df(), instance_type)


def validate_region_zone(region: str | None,
                         zone: str | None) -> tuple[str | None, str | None]:
    """Validate region and zone for Shadeform."""
    return common.validate_region_zone_impl('shadeform', _get_df(), region,
                                            zone)


def get_hourly_cost(instance_type: str,
                    use_spot: bool = False,
                    region: str | None = None,
                    zone: str | None = None) -> float:
    """Returns the cost, or the cheapest cost among all zones for spot."""
    # Shadeform doesn't support spot instances currently
    if use_spot:
        raise ValueError('Spot instances are not supported on Shadeform')

    return common.get_hourly_cost_impl(_get_df(), instance_type, use_spot,
                                       region, zone)


def get_vcpus_mem_from_instance_type(
        instance_type: str) -> tuple[float | None, float | None]:
    """Get vCPUs and memory from instance type."""
    return _call_or_default(
        lambda: common.get_vcpus_mem_from_instance_type_impl(
            _get_df(), instance_type), (None, None))


def get_default_instance_type(
        cpus: str | None = None,
        memory: str | None = None,
        disk_tier: str | None = None,
        local_disk: str | None = None,
        region: str | None = None,
        zone: str | None = None,
        use_spot: bool = False,
        max_hourly_cost: float | None = None) -> str | None:
    """Get default instance type based on requirements."""
    del disk_tier, local_disk  # Shadeform doesn't support custom disk tiers yet
    return _call_or_default(
        lambda: common.get_instance_type_for_cpus_mem_impl(
            _get_df(), cpus, memory, region, zone, use_spot, max_hourly_cost),
        None)


def get_accelerators_from_instance_type(
        instance_type: str) -> dict[str, int | float] | None:
    """Get accelerator information from instance type."""
    return _call_or_default(
        lambda: common.get_accelerators_from_instance_type_impl(
            _get_df(), instance_type), None)


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
    """Returns a list of instance types that have the given accelerator."""
    del local_disk  # unused
    if use_spot:
        # Return empty lists since spot is not supported
        return None, ['Spot instances are not supported on Shadeform']

    return _call_or_default(
        lambda: common.get_instance_type_for_accelerator_impl(
            df=_get_df(),
            acc_name=acc_name,
            acc_count=acc_count,
            cpus=cpus,
            memory=memory,
            use_spot=use_spot,
            region=region,
            zone=zone,
            max_hourly_cost=max_hourly_cost), (None, []))


def get_region_zones_for_instance_type(instance_type: str,
                                       use_spot: bool) -> list['cloud.Region']:
    """Get regions and zones for an instance type."""
    if use_spot:
        return []  # No spot support

    df = _get_df()
    df_filtered = df[df['InstanceType'] == instance_type]
    return _call_or_default(
        lambda: common.get_region_zones(df_filtered, use_spot), [])


def list_accelerators(
        gpus_only: bool,
        name_filter: str | None,
        region_filter: str | None,
        quantity_filter: int | None,
        case_sensitive: bool = True,
        all_regions: bool = False,
        require_price: bool = True) -> dict[str, list[common.InstanceTypeInfo]]:
    """Returns all instance types in Shadeform offering GPUs."""
    del require_price  # Unused.
    return common.list_accelerators_impl('Shadeform', _get_df(), gpus_only,
                                         name_filter, region_filter,
                                         quantity_filter, case_sensitive,
                                         all_regions)
