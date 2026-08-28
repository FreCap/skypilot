"""AWS Offerings Catalog.

This module loads the service catalog file and can be used to query
instance types and pricing information for AWS.
"""
import glob
import hashlib
import os
import re
import tempfile
import threading
import typing
from typing import Optional

import filelock

from sky import exceptions
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.catalog import common
from sky.catalog import config
from sky.catalog.data_fetchers import fetch_aws
from sky.clouds import aws
from sky.utils import annotations
from sky.utils import common_utils
from sky.utils import resources_utils
from sky.utils import rich_utils
from sky.utils import timeline
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import pandas as pd

    from sky.clouds import cloud
else:
    pd = adaptors_common.LazyImport('pandas')

logger = sky_logging.init_logger(__name__)

# We will select from the following six instance families. The suffix
# 'd' denotes that instance storage is attached:
_DEFAULT_INSTANCE_FAMILY = [
    # This is the latest general-purpose instance family as of Mar 2023.
    # CPU: Intel Ice Lake 8375C.
    # Memory: 4 GiB RAM per 1 vCPU;
    'm6i',
    'm6id',
    # This is the latest general-purpose instance family as of Jul 2025.
    # CPU: Intel Sapphire Rapids.
    # Memory: 4 GiB RAM per 1 vCPU;
    'm7i',
    # This is the latest memory-optimized instance family as of Mar 2023.
    # CPU: Intel Ice Lake 8375C
    # Memory: 8 GiB RAM per 1 vCPU;
    'r6i',
    'r6id',
    # This is the latest memory-optimized instance family as of Jul 2025.
    # CPU: Intel Sapphire Rapids.
    # Memory: 8 GiB RAM per 1 vCPU;
    'r7i',
    # This is the latest compute-optimized instance family as of Mar 2023.
    # CPU: Intel Ice Lake 8375C
    # Memory: 2 GiB RAM per 1 vCPU;
    'c6i',
    'c6id',
    # This is the latest compute-optimized instance family as of Jul 2025.
    # CPU: Intel Sapphire Rapids.
    # Memory: 2 GiB RAM per 1 vCPU;
    'c7i',
]
_DEFAULT_NUM_VCPUS = 8
_DEFAULT_MEMORY_CPU_RATIO = 4

# Keep it synced with the frequency in
# skypilot-catalog/.github/workflows/update-aws-catalog.yml
_PULL_FREQUENCY_HOURS = 7

_AMI_ID_PATTERN = re.compile(r'ami-[0-9a-f]{8}(?:[0-9a-f]{9})?')

# The main catalog dataframe.
#   - _default_df: default non-account-specific catalog
#     The AvailabilityZone column is a zone ID (e.g. use1-az1).
#   - _user_df: account-specific catalog (i.e., regions that the account
#     doesn't have enabled are dropped; AZ mapping is applied, etc.)
#     Creating this requires AWS credentials. It is created at most once
#     (and cached) per a process' lifetime.
#     The AvailabilityZone column is a zone name (e.g. us-east-1a).
# `_apply_az_mapping_lock` protects reading/writing `_user_df`.
_default_df = common.read_catalog('aws/vms.csv',
                                  pull_frequency_hours=_PULL_FREQUENCY_HOURS)
_user_df = None
_apply_az_mapping_lock = threading.Lock()

_image_df = common.read_catalog('aws/images.csv',
                                pull_frequency_hours=_PULL_FREQUENCY_HOURS)

_quotas_df = common.read_catalog('aws/instance_quota_mapping.csv',
                                 pull_frequency_hours=_PULL_FREQUENCY_HOURS)


def _get_az_mappings(aws_user_hash: str) -> Optional['pd.DataFrame']:
    filename = f'aws/az_mappings-{aws_user_hash}.csv'
    az_mapping_path = common.get_catalog_path(filename)
    az_mapping_md5_path = common.get_catalog_path(f'.meta/{filename}.md5')
    vms_catalog_path = common.get_catalog_path('aws/vms.csv')

    def _needs_refresh() -> bool:
        if not os.path.exists(az_mapping_path):
            return True
        # Region enablement can change independently of the account identity.
        # A newly downloaded VM catalog may therefore contain zone IDs that an
        # indefinitely cached account mapping would silently drop below.
        return (aws_user_hash != 'default' and
                os.path.exists(vms_catalog_path) and
                os.path.getmtime(vms_catalog_path)
                > os.path.getmtime(az_mapping_path))

    if aws_user_hash == 'default' and not os.path.exists(az_mapping_path):
        return None

    if _needs_refresh():
        os.makedirs(os.path.dirname(az_mapping_path), exist_ok=True)
        os.makedirs(os.path.dirname(az_mapping_md5_path), exist_ok=True)
        with filelock.FileLock(az_mapping_path + '.lock'):
            if _needs_refresh():
                try:
                    with rich_utils.safe_status(
                            ux_utils.spinner_message(
                                'AWS: Fetching availability zones mapping')):
                        az_mappings = (
                            fetch_aws.fetch_availability_zone_mappings())
                except RuntimeError as e:
                    if not os.path.exists(az_mapping_path):
                        raise
                    logger.warning('Failed to refresh AWS availability zone '
                                   f'mapping; using the cached mapping: {e}')
                else:
                    # Publish atomically so concurrent readers never observe a
                    # partially written mapping.
                    with tempfile.NamedTemporaryFile(
                            mode='w',
                            dir=os.path.dirname(az_mapping_path),
                            delete=False,
                            encoding='utf-8') as f:
                        az_mappings.to_csv(f, index=False)
                        tmp_path = f.name
                    os.replace(tmp_path, az_mapping_path)
                    # Write md5 of the az_mapping file to a file so we can
                    # check for changes when uploading to the controller.
                    with open(az_mapping_path, encoding='utf-8') as f:
                        az_mapping_hash = hashlib.md5(
                            f.read().encode(),
                            usedforsecurity=False).hexdigest()
                    with open(az_mapping_md5_path, 'w', encoding='utf-8') as f:
                        f.write(az_mapping_hash)

    return pd.read_csv(az_mapping_path)


@timeline.event
def _fetch_and_apply_az_mapping(df: common.LazyDataFrame) -> 'pd.DataFrame':
    """Maps zone IDs (use1-az1) to zone names (us-east-1x).

    The upper-level functions that use the availability zone information
    should be able to handle the case where the zone name is not correct,
    due to the credentials not being configured.

    Such mappings are account-specific and determined by AWS. We fetch the
    mappings from AWS, which requires AWS credentials. If the user does not
    have AWS credentials configured, we use original zone id. It is ok to
    use the default mapping because the user will not be able to provision
    instances with those wrong availablity zones due to the lack of
    credentials.

    The mappings will also serve to remove from 'df' the regions that are
    not supported by the user account.

    Returns:
        A dataframe with column 'AvailabilityZone' that's correctly replaced
        with the zone name (e.g. us-east-1a).
    """
    try:
        user_identity_list = aws.AWS.get_active_user_identity()
        assert user_identity_list, user_identity_list
        user_identity = user_identity_list[0]
        aws_user_hash = hashlib.md5(user_identity.encode(),
                                    usedforsecurity=False).hexdigest()[:8]
    except (exceptions.CloudUserIdentityError, ImportError):
        # If failed to get user identity, or import aws dependencies, we use the
        # latest mapping file or the default mapping file.
        # The import error can happen on the client side when the user does not
        # have AWS dependencies installed.
        # TODO(zhwu): we should avoid the dependency of the availability zone
        # mapping so as to get rid of the import error.
        glob_name = common.get_catalog_path('aws/az_mappings-*.csv')
        # Find the most recent file that matches the glob.
        # We check the existing files because the user could remove the
        # credentials after a cluster is created. Using the latest mapping
        # file is better than using the default mapping file because the
        # former is more likely to be correct.
        glob_files = glob.glob(glob_name)
        if glob_files:
            glob_files.sort(key=os.path.getmtime)
            aws_user_hash = os.path.basename(glob_files[-1]).split('-')[1]
            # aws_user_hash can be set to `default` if the user never
            # configured AWS credentials.
            aws_user_hash = aws_user_hash.split('.')[0]
        else:
            aws_user_hash = 'default'
        logger.debug(
            'Failed to get AWS user identity. Using the latest mapping '
            f'file for user {aws_user_hash!r}.')

    az_mappings = _get_az_mappings(aws_user_hash)
    if az_mappings is None:
        # Returning the original dataframe directly, as no cloud
        # identity can be fetched which suggests there are no
        # credentials.
        return df
    # Use inner join to drop rows with unknown AZ IDs, which are likely
    # because the user does not have access to that Region. Otherwise,
    # there will be rows with NaN in the AvailabilityZone column.
    df = df.merge(az_mappings, on=['AvailabilityZone'], how='inner')
    df = df.drop(columns=['AvailabilityZone']).rename(
        columns={'AvailabilityZoneName': 'AvailabilityZone'})
    return df


def _get_df() -> 'pd.DataFrame':
    global _user_df
    with _apply_az_mapping_lock:
        if _user_df is None:
            try:
                _user_df = _fetch_and_apply_az_mapping(_default_df)
            except (RuntimeError, ImportError) as e:
                if config.get_use_default_catalog_if_failed():
                    logger.warning('Failed to fetch availability zone mapping. '
                                   f'{common_utils.format_exception(e)}')
                    return _default_df
                else:
                    raise
    return _user_df


def get_quota_code(instance_type: str, use_spot: bool) -> str | None:
    """Get the quota code based on `instance_type` and `use_spot`.

    The quota code is fetched from `_quotas_df` based on the instance type
    specified, and will then be utilized in a botocore API command in order
    to check its quota.
    """

    if use_spot:
        spot_header = 'SpotInstanceCode'
    else:
        spot_header = 'OnDemandInstanceCode'
    try:
        quota_code = _quotas_df.loc[_quotas_df['InstanceType'] == instance_type,
                                    spot_header].values[0]
        return quota_code

    except IndexError:
        return None


def instance_type_exists(instance_type: str) -> bool:
    return common.instance_type_exists_impl(_get_df(), instance_type)


def validate_region_zone(region: str | None,
                         zone: str | None) -> tuple[str | None, str | None]:
    return common.validate_region_zone_impl('aws', _get_df(), region, zone)


def get_hourly_cost(instance_type: str,
                    use_spot: bool = False,
                    region: str | None = None,
                    zone: str | None = None) -> float:
    return common.get_hourly_cost_impl(_get_df(), instance_type, use_spot,
                                       region, zone)


def get_vcpus_mem_from_instance_type(
        instance_type: str) -> tuple[float | None, float | None]:
    return common.get_vcpus_mem_from_instance_type_impl(_get_df(),
                                                        instance_type)


def get_default_instance_type(
        cpus: str | None = None,
        memory: str | None = None,
        disk_tier: resources_utils.DiskTier | None = None,
        local_disk: str | None = None,
        region: str | None = None,
        zone: str | None = None,
        use_spot: bool = False,
        max_hourly_cost: float | None = None) -> str | None:
    del disk_tier  # unused
    if cpus is None and memory is None:
        cpus = f'{_DEFAULT_NUM_VCPUS}+'

    if memory is None:
        memory_gb_or_ratio = f'{_DEFAULT_MEMORY_CPU_RATIO}x'
    else:
        memory_gb_or_ratio = memory
    instance_type_prefix = tuple(
        f'{family}.' for family in _DEFAULT_INSTANCE_FAMILY)
    df = _get_df()
    df = df[df['InstanceType'].str.startswith(instance_type_prefix)]
    df = common.filter_with_local_disk(df, local_disk)
    return common.get_instance_type_for_cpus_mem_impl(df, cpus,
                                                      memory_gb_or_ratio,
                                                      region, zone, use_spot,
                                                      max_hourly_cost)


def get_accelerators_from_instance_type(
        instance_type: str) -> dict[str, int | float] | None:
    return common.get_accelerators_from_instance_type_impl(
        _get_df(), instance_type)


def get_arch_from_instance_type(instance_type: str) -> str | None:
    return common.get_arch_from_instance_type_impl(_get_df(), instance_type)


def get_local_disk_from_instance_type(instance_type: str) -> str | None:
    return common.get_local_disk_from_instance_type_impl(
        _get_df(), instance_type)


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
    accelerators/cpus/memory with sorted prices and a list of candidates with
    fuzzy search.
    """
    df = common.filter_with_local_disk(_get_df(), local_disk)
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
    df = _get_df()
    df = df[df['InstanceType'] == instance_type]
    region_list = common.get_region_zones(df, use_spot)
    # Hack: Enforce US regions are always tried first:
    #   [US regions sorted by price] + [non-US regions sorted by price]
    us_region_list = []
    other_region_list = []
    for region in region_list:
        if region.name.startswith('us-'):
            us_region_list.append(region)
        else:
            other_region_list.append(region)
    return us_region_list + other_region_list


def list_accelerators(
        gpus_only: bool,
        name_filter: str | None,
        region_filter: str | None,
        quantity_filter: int | None,
        case_sensitive: bool = True,
        all_regions: bool = False,
        require_price: bool = True) -> dict[str, list[common.InstanceTypeInfo]]:
    """Returns all instance types in AWS offering accelerators."""
    del require_price  # Unused.
    return common.list_accelerators_impl('AWS', _get_df(), gpus_only,
                                         name_filter, region_filter,
                                         quantity_filter, case_sensitive,
                                         all_regions)


@annotations.lru_cache(scope='request')
def _fresh_image_catalog() -> common.LazyDataFrame:
    """Returns one forced-refresh image catalog per request."""
    return common.read_catalog('aws/images.csv', pull_frequency_hours=0)


def _is_ami_id(image_id: str | None) -> bool:
    return (image_id is not None and
            _AMI_ID_PATTERN.fullmatch(image_id) is not None)


def get_image_id_from_tag(tag: str, region: str | None) -> str | None:
    """Returns the image id from the tag."""
    global _image_df

    image_id = common.get_image_id_from_tag_impl(_image_df, tag, region)
    if not _is_ami_id(image_id):
        # Refresh once per request when the tag is absent or contains a
        # generator placeholder.  Regionless placement checks several regions
        # together; reusing one refreshed LazyDataFrame prevents one download
        # for every missing region.
        logger.debug('Refreshing the image catalog and trying again.')
        _image_df = _fresh_image_catalog()
        image_id = common.get_image_id_from_tag_impl(_image_df, tag, region)
    return image_id if _is_ami_id(image_id) else None


def is_image_tag_valid(tag: str, region: str | None) -> bool:
    """Returns whether the image tag is valid."""
    return get_image_id_from_tag(tag, region) is not None
