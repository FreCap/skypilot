"""Storage and Store Classes for Sky Data."""
from collections.abc import Callable
import dataclasses
import enum
import logging
import os
import re
import typing
from typing import Any, Optional
import urllib.parse

from sky import check as sky_check
from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import cloudflare
from sky.adaptors import coreweave
from sky.adaptors import huggingface
from sky.adaptors import oci_s3
from sky.adaptors import vastdata
from sky.clouds import cloud as sky_cloud
from sky.data import data_utils
from sky.utils import common_utils
from sky.utils import schemas
from sky.utils import status_lib
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import mypy_boto3_s3

logger: logging.Logger = sky_logging.init_logger(__name__)

StorageHandle = Any
StorageStatus = status_lib.StorageStatus
Path = str
SourceType = Path | list[Path]

# Clouds with object storage implemented in this module. Azure Blob
# Storage isn't supported yet (even though Azure is).
# TODO(Doyoung): need to add clouds.CLOUDFLARE() to support
# R2 to be an option as preferred store type
STORE_ENABLED_CLOUDS: list[str] = [
    str(clouds.AWS()),
    str(clouds.GCP()),
    str(clouds.Azure()),
    str(clouds.IBM()),
    str(clouds.OCI()),
    str(clouds.Nebius()),
    cloudflare.NAME,
    coreweave.NAME,
    vastdata.NAME,
    huggingface.NAME,
]

# Maximum number of concurrent rsync upload processes
_MAX_CONCURRENT_UPLOADS = 32

_BUCKET_FAIL_TO_CONNECT_MESSAGE = (
    'Failed to access existing bucket {name!r}. '
    'This is likely because it is a private bucket you do not have access to.\n'
    'To fix: \n'
    '  1. If you are trying to create a new bucket: use a different name.\n'
    '  2. If you are trying to connect to an existing bucket: make sure '
    'your cloud credentials have access to it.')

_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE = (
    'Bucket {bucket_name!r} does not exist. '
    'It may have been deleted externally.')

_STORAGE_LOG_FILE_NAME = 'storage_sync.log'


def get_cached_enabled_storage_cloud_names_or_refresh(
        raise_if_no_cloud_access: bool = False) -> list[str]:
    # This is a temporary solution until https://github.com/skypilot-org/skypilot/issues/1943 # pylint: disable=line-too-long
    # is resolved by implementing separate 'enabled_storage_clouds'
    enabled_clouds = sky_check.get_cached_enabled_clouds_or_refresh(
        sky_cloud.CloudCapability.STORAGE)
    enabled_clouds = [str(cloud) for cloud in enabled_clouds]

    r2_is_enabled, _ = cloudflare.check_storage_credentials()
    if r2_is_enabled:
        enabled_clouds.append(cloudflare.NAME)

    # Similarly, handle CoreWeave storage credentials
    coreweave_is_enabled, _ = coreweave.check_storage_credentials()
    if coreweave_is_enabled:
        enabled_clouds.append(coreweave.NAME)

    vastdata_is_enabled, _ = vastdata.check_storage_credentials()
    if vastdata_is_enabled:
        enabled_clouds.append(vastdata.NAME)

    hf_is_enabled, _ = huggingface.check_storage_credentials()
    if hf_is_enabled:
        enabled_clouds.append(huggingface.NAME)

    if raise_if_no_cloud_access and not enabled_clouds:
        raise exceptions.NoCloudAccessError(
            'No cloud access available for storage. '
            'Please check your cloud credentials.')
    return enabled_clouds


def _is_storage_cloud_enabled(cloud_name: str,
                              try_fix_with_sky_check: bool = True) -> bool:
    enabled_storage_cloud_names = (
        get_cached_enabled_storage_cloud_names_or_refresh())
    if cloud_name in enabled_storage_cloud_names:
        return True
    if try_fix_with_sky_check:
        sky_check.check_capability(
            sky_cloud.CloudCapability.STORAGE,
            quiet=True,
            clouds=[cloud_name],
            workspace=skypilot_config.get_active_workspace())
        return _is_storage_cloud_enabled(cloud_name,
                                         try_fix_with_sky_check=False)
    return False


class StoreType(enum.Enum):
    """Enum for the different types of stores."""
    S3 = 'S3'
    GCS = 'GCS'
    AZURE = 'AZURE'
    R2 = 'R2'
    IBM = 'IBM'
    OCI = 'OCI'
    NEBIUS = 'NEBIUS'
    COREWEAVE = 'COREWEAVE'
    VASTDATA = 'VASTDATA'
    HF = 'HF'
    VOLUME = 'VOLUME'

    @classmethod
    def _get_s3_compatible_store_by_cloud(cls, cloud_name: str) -> str | None:
        """Get S3-compatible store type by cloud name."""
        for store_type, store_class in _S3_COMPATIBLE_STORES.items():
            config = store_class.get_config()
            if config.cloud_name.lower() == cloud_name:
                return store_type
        return None

    @classmethod
    def _get_s3_compatible_config(
            cls, store_type: str) -> Optional['S3CompatibleConfig']:
        """Get S3-compatible store configuration by store type."""
        store_class = _S3_COMPATIBLE_STORES.get(store_type)
        if store_class:
            return store_class.get_config()
        return None

    @classmethod
    def find_s3_compatible_config_by_prefix(
            cls, source: str) -> Optional['StoreType']:
        """Get S3-compatible store type by URL prefix."""
        for store_type, store_class in _S3_COMPATIBLE_STORES.items():
            config = store_class.get_config()
            if source.startswith(config.url_prefix):
                return StoreType(store_type)
        return None

    @classmethod
    def from_cloud(cls, cloud: str) -> 'StoreType':
        cloud_lower = cloud.lower()
        if cloud_lower == str(clouds.GCP()).lower():
            return StoreType.GCS
        elif cloud_lower == str(clouds.IBM()).lower():
            return StoreType.IBM
        elif cloud_lower == str(clouds.Azure()).lower():
            return StoreType.AZURE
        elif cloud_lower == str(clouds.OCI()).lower():
            return StoreType.OCI
        elif cloud_lower == huggingface.NAME.lower():
            return StoreType.HF
        elif cloud_lower == str(clouds.Lambda()).lower():
            with ux_utils.print_exception_no_traceback():
                raise ValueError('Lambda Cloud does not provide cloud storage.')
        elif cloud_lower == str(clouds.SCP()).lower():
            with ux_utils.print_exception_no_traceback():
                raise ValueError('SCP does not provide cloud storage.')
        else:
            s3_store_type = cls._get_s3_compatible_store_by_cloud(cloud_lower)
            if s3_store_type:
                return cls(s3_store_type)

        raise ValueError(f'Unsupported cloud for StoreType: {cloud}')

    def to_cloud(self) -> str:
        config = self._get_s3_compatible_config(self.value)
        if config:
            return config.cloud_name

        if self == StoreType.GCS:
            return str(clouds.GCP())
        elif self == StoreType.AZURE:
            return str(clouds.Azure())
        elif self == StoreType.IBM:
            return str(clouds.IBM())
        elif self == StoreType.OCI:
            return str(clouds.OCI())
        elif self == StoreType.HF:
            return huggingface.NAME
        else:
            raise ValueError(f'Unknown store type: {self}')

    @classmethod
    def from_store(cls, store: 'AbstractStore') -> 'StoreType':
        if isinstance(store, S3CompatibleStore):
            return cls(store.get_store_type())

        if isinstance(store, GcsStore):
            return StoreType.GCS
        elif isinstance(store, AzureBlobStore):
            return StoreType.AZURE
        elif isinstance(store, IBMCosStore):
            return StoreType.IBM
        elif isinstance(store, OciStore):
            return StoreType.OCI
        elif isinstance(store, HuggingFaceStore):
            return StoreType.HF
        else:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Unknown store type: {store}')

    def store_prefix(self) -> str:
        config = self._get_s3_compatible_config(self.value)
        if config:
            return config.url_prefix

        if self == StoreType.GCS:
            return 'gs://'
        elif self == StoreType.AZURE:
            return 'https://'
        elif self == StoreType.IBM:
            return 'cos://'
        elif self == StoreType.OCI:
            return 'oci://'
        elif self == StoreType.HF:
            return huggingface.HF_BUCKETS_URL_PREFIX
        else:
            with ux_utils.print_exception_no_traceback():
                raise ValueError(f'Unknown store type: {self}')

    @classmethod
    def get_endpoint_url(cls, store: 'AbstractStore', path: str) -> str:
        """Generates the endpoint URL for a given store and path.

        Args:
            store: Store object implementing AbstractStore.
            path: Path within the store.

        Returns:
            Endpoint URL of the bucket as a string.
        """
        store_type = cls.from_store(store)
        if store_type == StoreType.AZURE:
            assert isinstance(store, AzureBlobStore)
            storage_account_name = store.storage_account_name
            bucket_endpoint_url = data_utils.AZURE_CONTAINER_URL.format(
                storage_account_name=storage_account_name, container_name=path)
        else:
            bucket_endpoint_url = f'{store_type.store_prefix()}{path}'
        return bucket_endpoint_url

    @classmethod
    def get_fields_from_store_url(
            cls, store_url: str
    ) -> tuple['StoreType', str, str, str | None, str | None]:
        """Returns the store type, bucket name, and sub path from
        a store URL, and the storage account name and region if applicable.

        Args:
            store_url: str; The store URL.
        """
        # The full path from the user config of IBM COS contains the region,
        # and Azure Blob Storage contains the storage account name, we need to
        # pass these information to the store constructor.
        storage_account_name = None
        region = None
        bucket_name = None
        sub_path = None
        for store_type in StoreType:
            if store_type == StoreType.VOLUME:
                continue
            if store_url.startswith(store_type.store_prefix()):
                if store_type == StoreType.AZURE:
                    storage_account_name, bucket_name, sub_path = \
                        data_utils.split_az_path(store_url)
                elif store_type == StoreType.IBM:
                    bucket_name, sub_path, region = data_utils.split_cos_path(
                        store_url)
                elif store_type == StoreType.GCS:
                    bucket_name, sub_path = data_utils.split_gcs_path(store_url)
                elif store_type == StoreType.HF:
                    bucket_name, sub_path = data_utils.split_hf_path(store_url)
                else:
                    # Check compatible stores
                    for compatible_store_type, store_class in \
                        _S3_COMPATIBLE_STORES.items():
                        if store_type.value == compatible_store_type:
                            config = store_class.get_config()
                            bucket_name, sub_path = config.split_path(store_url)
                            break
                    else:
                        # If we get here, it's an unknown S3-compatible store
                        raise ValueError(
                            f'Unknown S3-compatible store type: {store_type}')
                if bucket_name is None or sub_path is None:
                    raise ValueError(f'Failed to parse store URL: {store_url}')
                return store_type, bucket_name, \
                    sub_path, storage_account_name, region
        raise ValueError(f'Unknown store URL: {store_url}')


class StorageMode(enum.Enum):
    MOUNT = 'MOUNT'
    COPY = 'COPY'
    MOUNT_CACHED = 'MOUNT_CACHED'


MOUNTABLE_STORAGE_MODES = [
    StorageMode.MOUNT,
    StorageMode.MOUNT_CACHED,
]

DEFAULT_STORAGE_MODE = StorageMode.MOUNT


class FileMountType(enum.Enum):
    """Pre-defined parameter types for MOUNT_CACHED mode.

    Each type maps to a MountCachedConfig with tuned rclone parameters
    for a specific use-case. Users can override individual parameters
    via config.mount_cached on top of a type.
    """
    # Read-only access to model weights/checkpoints.
    # Optimized for large sequential reads with 16 parallel chunk streams
    # and 32MB chunk size (benchmarked sweet spot for model loading).
    MODEL_CHECKPOINT_RO = 'MODEL_CHECKPOINT_RO'
    # Read-write access to model weights/checkpoints.
    # Same read optimizations as MODEL_CHECKPOINT_RO, plus 8 parallel
    # transfers for writing sharded checkpoints (one per GPU rank).
    MODEL_CHECKPOINT_RW = 'MODEL_CHECKPOINT_RW'
    # Read-only access to datasets.
    # Optimized for smaller sequential reads with no parallel chunk streams
    # and 8MB chunk size.
    DATASET_RO = 'DATASET_RO'
    # Read-write access to datasets.
    # Same read optimizations as DATASET_RO, plus 16 parallel transfers
    # for writing.
    DATASET_RW = 'DATASET_RW'


# Mapping from FileMountType enum to base MountCachedConfig field values.
# These are the "defaults" that a type provides; any explicit
# config.mount_cached fields in the YAML override them.
_MOUNT_CACHED_PRESET_CONFIGS: dict['FileMountType', dict[str, Any]] = {
    FileMountType.MODEL_CHECKPOINT_RO: {
        'vfs_read_chunk_streams': 16,
        'vfs_read_chunk_size': '32M',
        'read_only': True,
    },
    FileMountType.MODEL_CHECKPOINT_RW: {
        'vfs_read_chunk_streams': 16,
        'vfs_read_chunk_size': '32M',
        'transfers': 8,
    },
    FileMountType.DATASET_RO: {
        'vfs_read_chunk_streams': 0,
        'vfs_read_chunk_size': '8M',
        'read_only': True,
    },
    FileMountType.DATASET_RW: {
        'vfs_read_chunk_streams': 0,
        'vfs_read_chunk_size': '8M',
        'transfers': 16,
    },
}


def merge_mount_cached_config(
    file_mount_type: 'FileMountType',
    overrides: Optional['MountCachedConfig'] = None,
) -> 'MountCachedConfig':
    """Resolve a FileMountType into a MountCachedConfig, with optional
    overrides.

    Args:
        file_mount_type: The file mount type to resolve.
        overrides: Optional MountCachedConfig whose non-None fields
            take precedence over the type defaults.

    Returns:
        A MountCachedConfig with type values, overridden by any
        non-None fields from overrides.
    """
    base = _MOUNT_CACHED_PRESET_CONFIGS[file_mount_type].copy()
    if overrides is not None:
        for field in dataclasses.fields(overrides):
            value = getattr(overrides, field.name)
            if value is not None:
                base[field.name] = value
    return MountCachedConfig(**base)


@dataclasses.dataclass
class MountCachedConfig:
    """Per-bucket configuration for MOUNT_CACHED mode (rclone flags).

    Each field maps to a specific rclone flag. None means "use the default
    from get_mount_cached_cmd" (i.e., the flag is not overridden).
    """
    # Number of file transfers to run in parallel.
    # rclone flag: --transfers
    transfers: int | None = None
    # In-memory buffer size per transfer (e.g. "64M").
    # rclone flag: --buffer-size
    buffer_size: str | None = None
    # Maximum total size of the VFS cache on disk (e.g. "20G").
    # rclone flag: --vfs-cache-max-size
    vfs_cache_max_size: str | None = None
    # Maximum age of objects in the VFS cache (e.g. "1h").
    # rclone flag: --vfs-cache-max-age
    vfs_cache_max_age: str | None = None
    # Read-ahead bytes beyond what was requested (e.g. "128M").
    # rclone flag: --vfs-read-ahead
    vfs_read_ahead: str | None = None
    # Initial chunk size for each read (e.g. "32M").
    # rclone flag: --vfs-read-chunk-size
    vfs_read_chunk_size: str | None = None
    # Number of parallel streams for chunked reading.
    # When set, disables the default exponential chunk-size growth.
    # rclone flag: --vfs-read-chunk-streams
    vfs_read_chunk_streams: int | None = None
    # Delay before writing back to remote (e.g. "5s").
    # rclone flag: --vfs-write-back
    vfs_write_back: str | None = None
    # Mount as read-only.
    # rclone flag: --read-only
    read_only: bool | None = None

    def to_rclone_flags(self) -> str:
        """Convert non-None fields to rclone CLI flag string."""
        flags = []
        if self.transfers is not None:
            flags.append(f'--transfers {self.transfers}')
            # Automate checkers. It is recommend practice that checkers are
            # normally twice as many as transfers. However, research into
            # different examples reveal that at a very high transfer count
            # like 100, it is a bit pointless to have 200 checkers, so the
            # second part of the min provides a plateaued increase for
            # higher number of transfers.
            checkers = min(self.transfers * 2, 30 + self.transfers * 1.2)
            flags.append(f'--checkers {max(checkers, 4)}')
        if self.buffer_size is not None:
            flags.append(f'--buffer-size {self.buffer_size.upper()}')
        if self.vfs_cache_max_size is not None:
            flags.append(
                f'--vfs-cache-max-size {self.vfs_cache_max_size.upper()}')
        else:
            flags.append('--vfs-cache-max-size 10G')
        if self.vfs_cache_max_age is not None:
            flags.append(f'--vfs-cache-max-age {self.vfs_cache_max_age}')
        if self.vfs_read_ahead is not None:
            flags.append(f'--vfs-read-ahead {self.vfs_read_ahead.upper()}')
        if self.vfs_read_chunk_size is not None:
            flags.append(
                f'--vfs-read-chunk-size {self.vfs_read_chunk_size.upper()}')
        if self.vfs_read_chunk_streams is not None:
            flags.append(
                f'--vfs-read-chunk-streams {self.vfs_read_chunk_streams}')
        flags.append(f'--vfs-write-back {self.vfs_write_back or "1s"}')
        if self.read_only:
            flags.append('--read-only')
        return ' '.join(flags)

    def to_yaml_config(self) -> dict[str, Any]:
        """Serialize non-None fields to a dict for YAML round-tripping."""
        result = {}
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value is not None:
                result[field.name] = value
        return result

    @classmethod
    def from_yaml_config(cls, config: dict[str, Any]) -> 'MountCachedConfig':
        """Create from a dict parsed from YAML."""
        return cls(**config)


@dataclasses.dataclass
class MountConfig:
    """Per-bucket configuration for MOUNT mode.

    Each field is threaded through to the mount command generating functions.
    None means "use the default" (i.e., the flag is not overridden).
    """
    # Mount as read-only.
    read_only: bool | None = None
    # Extra CLI flags forwarded verbatim to ``hf-mount`` (Hugging Face stores
    # only; ignored by every other store). Each element is one shell token,
    # e.g. ``['--cache-dir', '/mnt/nvme/hf-cache', '--advanced-writes']``.
    hf_mount_args: list[str] | None = None

    def to_yaml_config(self) -> dict[str, Any]:
        """Serialize non-None fields to a dict for YAML round-tripping."""
        result = {}
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value is not None:
                result[field.name] = value
        return result

    @classmethod
    def from_yaml_config(cls, config: dict[str, Any]) -> 'MountConfig':
        """Create from a dict parsed from YAML."""
        return cls(**config)


class AbstractStore:
    """AbstractStore abstracts away the different storage types exposed by
    different clouds.

    Storage objects are backed by AbstractStores, each representing a store
    present in a cloud.
    """

    class StoreMetadata:
        """A pickle-able representation of Store

        Allows store objects to be written to and reconstructed from
        global_user_state.
        """

        def __init__(self,
                     *,
                     name: str,
                     source: SourceType | None,
                     region: str | None = None,
                     is_sky_managed: bool | None = None,
                     _bucket_sub_path: str | None = None):
            self.name = name
            self.source = source
            self.region = region
            self.is_sky_managed = is_sky_managed
            self._bucket_sub_path = _bucket_sub_path

        def __repr__(self):
            return (f'StoreMetadata('
                    f'\n\tname={self.name},'
                    f'\n\tsource={self.source},'
                    f'\n\tregion={self.region},'
                    f'\n\tis_sky_managed={self.is_sky_managed},'
                    f'\n\t_bucket_sub_path={self._bucket_sub_path})')

    def __init__(self,
                 name: str,
                 source: SourceType | None,
                 region: str | None = None,
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool | None = True,
                 _bucket_sub_path: str | None = None):  # pylint: disable=invalid-name
        """Initialize AbstractStore

        Args:
            name: Store name
            source: Data source for the store
            region: Region to place the bucket in
            is_sky_managed: Whether the store is managed by Sky. If None, it
              must be populated by the implementing class during initialization.
            sync_on_reconstruction: bool; Whether to sync data if the storage
              object is found in the global_user_state and reconstructed from
              there. This is set to false when the Storage object is created not
              for direct use, e.g. for 'sky storage delete', or the storage is
              being re-used, e.g., for `sky start` on a stopped cluster.
            _bucket_sub_path: str; The prefix of the bucket directory to be
              created in the store, e.g. if _bucket_sub_path=my-dir, the files
              will be uploaded to s3://<bucket>/my-dir/.
              This only works if source is a local directory.
              # TODO(zpoint): Add support for non-local source.
        Raises:
            StorageBucketCreateError: If bucket creation fails
            StorageBucketGetError: If fetching existing bucket fails
            StorageInitError: If general initialization fails
        """
        self.name = name
        self.source = source
        self.region = region
        self.is_sky_managed = is_sky_managed
        self.sync_on_reconstruction = sync_on_reconstruction

        # To avoid mypy error
        self._bucket_sub_path: str | None = None
        # Trigger the setter to strip any leading/trailing slashes.
        self.bucket_sub_path = _bucket_sub_path
        # Whether sky is responsible for the lifecycle of the Store.
        self._validate()
        self.initialize()

    @property
    def bucket_sub_path(self) -> str | None:
        """Get the bucket_sub_path."""
        return self._bucket_sub_path

    @bucket_sub_path.setter
    # pylint: disable=invalid-name
    def bucket_sub_path(self, bucket_sub_path: str | None) -> None:
        """Set the bucket_sub_path, stripping any leading/trailing slashes."""
        if bucket_sub_path is not None:
            self._bucket_sub_path = bucket_sub_path.strip('/')
        else:
            self._bucket_sub_path = None

    @classmethod
    def from_metadata(cls, metadata: StoreMetadata, **override_args):
        """Create a Store from a StoreMetadata object.

        Used when reconstructing Storage and Store objects from
        global_user_state.
        """
        return cls(
            name=override_args.get('name', metadata.name),
            source=override_args.get('source', metadata.source),
            region=override_args.get('region', metadata.region),
            is_sky_managed=override_args.get('is_sky_managed',
                                             metadata.is_sky_managed),
            sync_on_reconstruction=override_args.get('sync_on_reconstruction',
                                                     True),
            # Backward compatibility
            # TODO: remove the hasattr check after v0.11.0
            _bucket_sub_path=override_args.get(
                '_bucket_sub_path',
                metadata._bucket_sub_path  # pylint: disable=protected-access
            ) if hasattr(metadata, '_bucket_sub_path') else None)

    def get_metadata(self) -> StoreMetadata:
        return self.StoreMetadata(name=self.name,
                                  source=self.source,
                                  region=self.region,
                                  is_sky_managed=self.is_sky_managed,
                                  _bucket_sub_path=self._bucket_sub_path)

    def initialize(self):
        """Initializes the Store object on the cloud.

        Initialization involves fetching bucket if exists, or creating it if
        it does not.

        Raises:
          StorageBucketCreateError: If bucket creation fails
          StorageBucketGetError: If fetching existing bucket fails
          StorageInitError: If general initialization fails.
        """
        pass

    def _validate(self) -> None:
        """Runs validation checks on class args"""
        pass

    def upload(self) -> None:
        """Uploads source to the store bucket

        Upload must be called by the Storage handler - it is not called on
        Store initialization.
        """
        raise NotImplementedError

    def delete(self) -> None:
        """Removes the Storage from the cloud."""
        raise NotImplementedError

    def _delete_sub_path(self) -> None:
        """Removes objects from the sub path in the bucket."""
        raise NotImplementedError

    def get_handle(self) -> StorageHandle:
        """Returns the storage handle for use by the execution backend to attach
        to VM/containers
        """
        raise NotImplementedError

    def download_remote_dir(self, local_path: str) -> None:
        """Downloads directory from remote bucket to the specified
        local_path

        Args:
          local_path: Local path on user's device
        """
        raise NotImplementedError

    def _download_file(self, remote_path: str, local_path: str) -> None:
        """Downloads file from remote to local on Store

        Args:
          remote_path: str; Remote file path on Store
          local_path: str; Local file path on user's device
        """
        raise NotImplementedError

    def mount_command(self, mount_path: str, read_only: bool = False) -> str:
        """Returns the command to mount the Store to the specified mount_path.

        This command is used for MOUNT mode. Includes the setup commands to
        install mounting tools.

        Args:
          mount_path: str; Mount path on remote server
          read_only: bool; Whether to mount as read-only
        """
        raise NotImplementedError

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        """Returns the command to mount the Store to the specified mount_path.

        This command is used for MOUNT_CACHED mode. Includes the setup commands
        to install mounting tools.

        Args:
          mount_path: str; Mount path on remote server
        """
        raise exceptions.NotSupportedError(
            f'{StorageMode.MOUNT_CACHED.value} is '
            f'not supported for {self.name}.')

    def __deepcopy__(self, memo):
        # S3 Client and GCS Client cannot be deep copied, hence the
        # original Store object is returned
        return self

    def _validate_existing_bucket(self):
        """Validates the storage fields for existing buckets."""
        # Check if 'source' is None, this is only allowed when Storage is in
        # either MOUNT mode or COPY mode with sky-managed storage.
        # Note: In COPY mode, a 'source' being None with non-sky-managed
        # storage is already handled as an error in _validate_storage_spec.
        if self.source is None:
            # Retrieve a handle associated with the storage name.
            # This handle links to sky managed storage if it exists.
            handle = global_user_state.get_handle_from_storage_name(self.name)
            # If handle is None, it implies the bucket is created
            # externally and not managed by Skypilot. For mounting such
            # externally created buckets, users must provide the
            # bucket's URL as 'source'.
            if handle is None:
                source_endpoint = StoreType.get_endpoint_url(store=self,
                                                             path=self.name)
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageSpecError(
                        'Attempted to mount a non-sky managed bucket '
                        f'{self.name!r} without specifying the storage source.'
                        f' Bucket {self.name!r} already exists. \n'
                        '    • To create a new bucket, specify a unique name.\n'
                        '    • To mount an externally created bucket (e.g., '
                        'created through cloud console or cloud cli), '
                        'specify the bucket URL in the source field '
                        'instead of its name. I.e., replace '
                        f'`name: {self.name}` with '
                        f'`source: {source_endpoint}`.')


# Imported late to avoid a circular dependency: the extracted HF backend
# builds on the shared storage primitives defined above and is re-exported
# here to preserve the existing public import path.
# pylint: disable=wrong-import-position,ungrouped-imports
from sky.data import storage_huggingface

# pylint: enable=wrong-import-position,ungrouped-imports

HuggingFaceStore = storage_huggingface.HuggingFaceStore
hf_storage_cloud_enabled = _is_storage_cloud_enabled
HF_BUCKET_FAIL_TO_CONNECT_MESSAGE = _BUCKET_FAIL_TO_CONNECT_MESSAGE
HF_STORAGE_LOG_FILE_NAME = _STORAGE_LOG_FILE_NAME
oci_storage_cloud_enabled: Callable[..., bool] = _is_storage_cloud_enabled
OCI_BUCKET_FAIL_TO_CONNECT_MESSAGE: str = _BUCKET_FAIL_TO_CONNECT_MESSAGE
OCI_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE: str = (
    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
OCI_STORAGE_LOG_FILE_NAME: str = _STORAGE_LOG_FILE_NAME


class Storage:
    """Storage objects handle persistent and large volume storage in the sky.

    Storage represents an abstract data store containing large data files
    required by the task. Compared to file_mounts, storage is faster and
    can persist across runs, requiring fewer uploads from your local machine.

    A storage object can be used in either MOUNT mode or COPY mode. In MOUNT
    mode (the default), the backing store is directly "mounted" to the remote
    VM, and files are fetched when accessed by the task and files written to the
    mount path are also written to the remote store. In COPY mode, the files are
    pre-fetched and cached on the local disk and writes are not replicated on
    the remote store.

    Behind the scenes, storage automatically uploads all data in the source
    to a backing object store in a particular cloud (S3/GCS/Azure Blob).

      Typical Usage: (See examples/playground/storage_playground.py)
        storage = Storage(name='imagenet-bucket', source='~/Documents/imagenet')

        # Move data to S3
        storage.add_store('S3')

        # Move data to Google Cloud Storage
        storage.add_store('GCS')

        # Delete Storage for both S3 and GCS
        storage.delete()
    """

    class StorageMetadata:
        """A pickle-able tuple of:

        - (required) Storage name.
        - (required) Source
        - (optional) Storage mode.
        - (optional) Set of stores managed by sky added to the Storage object
        """
        # If any fields changed, increment the version. For backwards
        # compatibility, modify the __setstate__ method to handle the old
        # version.
        _VERSION = 2

        def __init__(
            self,
            *,
            storage_name: str | None,
            source: SourceType | None,
            mode: StorageMode | None = None,
            sky_stores: dict[StoreType, AbstractStore.StoreMetadata] |
            None = None,
            mount_cached_config: MountCachedConfig | None = None,
            file_mount_type: Optional['FileMountType'] = None,
            mount_config: MountConfig | None = None,
        ):
            self._version = self._VERSION

            assert storage_name is not None or source is not None
            self.storage_name = storage_name
            self.source = source
            self.mode = mode
            # Only stores managed by sky are stored here in the
            # global_user_state
            self.sky_stores = {} if sky_stores is None else sky_stores

            self.mount_cached_config = mount_cached_config
            self.file_mount_type = file_mount_type
            if self.file_mount_type or self.mount_cached_config:
                assert self.mode == StorageMode.MOUNT_CACHED

            self.mount_config = mount_config
            if self.mount_config:
                assert self.mode == StorageMode.MOUNT

        def __setstate__(self, state):
            self._version = self._VERSION

            version = state.pop('_version', None)
            # Handle old version(s) here.
            if version is None:
                version = -1
            if version < 0:
                self.mount_cached_config = None
            if version < 1:
                self.file_mount_type = None
            if version < 2:
                self.mount_config = None

            self.__dict__.update(state)

        def __repr__(self):
            return (f'StorageMetadata('
                    f'\n\tstorage_name={self.storage_name},'
                    f'\n\tsource={self.source},'
                    f'\n\tmode={self.mode},'
                    f'\n\tstores={self.sky_stores})')

        def add_store(self, store: AbstractStore) -> None:
            storetype = StoreType.from_store(store)
            self.sky_stores[storetype] = store.get_metadata()

        def remove_store(self, store: AbstractStore) -> None:
            storetype = StoreType.from_store(store)
            if storetype in self.sky_stores:
                del self.sky_stores[storetype]

    def __init__(
        self,
        name: str | None = None,
        source: SourceType | None = None,
        stores: list[StoreType] | None = None,
        persistent: bool | None = True,
        mode: StorageMode = DEFAULT_STORAGE_MODE,
        sync_on_reconstruction: bool = True,
        # pylint: disable=invalid-name
        _is_sky_managed: bool | None = None,
        # pylint: disable=invalid-name
        _bucket_sub_path: str | None = None,
        # pylint: disable=invalid-name
        _store_region: str | None = None,
        mount_cached_config: MountCachedConfig | None = None,
        file_mount_type: FileMountType | None = None,
        mount_config: MountConfig | None = None,
    ) -> None:
        """Initializes a Storage object.

        Three fields are required: the name of the storage, the source
        path where the data is initially located, and the default mount
        path where the data will be mounted to on the cloud.

        Storage object validation depends on the name, source and mount mode.
        There are four combinations possible for name and source inputs:

        - name is None, source is None: Underspecified storage object.
        - name is not None, source is None: If MOUNT mode, provision an empty
            bucket with name <name>. If COPY mode, raise error since source is
            required.
        - name is None, source is not None: If source is local, raise error
            since name is required to create destination bucket. If source is
            a bucket URL, use the source bucket as the backing store (if
            permissions allow, else raise error).
        - name is not None, source is not None: If source is local, upload the
            contents of the source path to <name> bucket. Create new bucket if
            required. If source is bucket url - raise error. Name should not be
            specified if the source is a URL; name will be inferred from source.

        Args:
          name: str; Name of the storage object. Typically used as the
            bucket name in backing object stores.
          source: str, List[str]; File path where the data is initially stored.
            Can be a single local path, a list of local paths, or a cloud URI
            (s3://, gs://, etc.). Local paths do not need to be absolute.
          stores: Optional; Specify pre-initialized stores (S3Store, GcsStore).
          persistent: bool; Whether to persist across sky launches.
          mode: StorageMode; Specify how the storage object is manifested on
            the remote VM. Can be either MOUNT or COPY. Defaults to MOUNT.
          sync_on_reconstruction: bool; Whether to sync the data if the storage
            object is found in the global_user_state and reconstructed from
            there. This is set to false when the Storage object is created not
            for direct use, e.g. for 'sky storage delete', or the storage is
            being re-used, e.g., for `sky start` on a stopped cluster.
          _is_sky_managed: Optional[bool]; Indicates if the storage is managed
            by Sky. Without this argument, the controller's behavior differs
            from the local machine. For example, if a bucket does not exist:
            Local Machine (is_sky_managed=True) →
            Controller (is_sky_managed=False).
            With this argument, the controller aligns with the local machine,
            ensuring it retains the is_sky_managed information from the YAML.
            During teardown, if is_sky_managed is True, the controller should
            delete the bucket. Otherwise, it might mistakenly delete only the
            sub-path, assuming is_sky_managed is False.
          _bucket_sub_path: Optional[str]; The subdirectory to use for the
            storage object.
          _store_region: Optional[str]; Internal pre-creation store region.
            This makes a planned store exactly reconstructable if the process
            exits after creating the remote bucket but before persisting its
            normal store handle.
        """
        self.name = name
        self.source = source
        self.persistent = persistent
        self.mode = mode
        assert mode in StorageMode
        self.stores: dict[StoreType, AbstractStore | None] = {}
        if stores is not None:
            for store in stores:
                self.stores[store] = None
        self.sync_on_reconstruction = sync_on_reconstruction
        self._is_sky_managed = _is_sky_managed
        self._bucket_sub_path = _bucket_sub_path
        self._store_region = _store_region

        self._constructed = False
        # TODO(romilb, zhwu): This is a workaround to support storage deletion
        # for managed jobs. Once sky storage supports forced management for
        # external buckets, this can be deprecated.
        self.force_delete = False

        self.mount_cached_config = mount_cached_config
        self.file_mount_type = file_mount_type
        self.mount_config = mount_config

        if (self.file_mount_type is not None and
                self.mode != StorageMode.MOUNT_CACHED):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageSpecError(
                    f'type can only be specified when '
                    f'mode is {StorageMode.MOUNT_CACHED.value}. '
                    f'Got mode={self.mode.value}.')
        if (self.mount_cached_config is not None and
                self.mode != StorageMode.MOUNT_CACHED):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageSpecError(
                    'config.mount_cached can only be specified when '
                    f'mode is {StorageMode.MOUNT_CACHED.value}. '
                    f'Got mode={self.mode.value}.')
        if (self.mount_config is not None and self.mode != StorageMode.MOUNT):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageSpecError(
                    'config.mount can only be specified when '
                    f'mode is {StorageMode.MOUNT.value}. '
                    f'Got mode={self.mode.value}.')

    def resolve_mount_cached_config(self) -> MountCachedConfig | None:
        """Resolve file_mount_type + overrides into a final MountCachedConfig.

        If a file_mount_type is set, merges type defaults with any explicit
        mount_cached_config overrides. If no file_mount_type, returns
        mount_cached_config as-is. Called at mount-command generation
        time, not at parse time, so the type name is preserved for
        YAML serialization.
        """
        if self.file_mount_type is not None:
            return merge_mount_cached_config(self.file_mount_type,
                                             self.mount_cached_config)
        return self.mount_cached_config

    def construct(self):
        """Constructs the storage object.

        The Storage object is lazily initialized, so that when a user
        initializes a Storage object on client side, it does not trigger the
        actual storage creation on the client side.

        Instead, once the specification of the storage object is uploaded to the
        SkyPilot API server side, the server side should use this construct()
        method to actually create the storage object. The construct() method
        will:

        1. Set the stores field if not specified
        2. Create the bucket or check the existence of the bucket
        3. Sync the data from the source to the bucket if necessary
        """
        if self._constructed:
            return
        self._constructed = True

        # Validate and correct inputs if necessary
        self._validate_storage_spec(self.name)

        # Logic to rebuild Storage if it is in global user state
        handle = global_user_state.get_handle_from_storage_name(self.name)
        if handle is not None:
            self.handle = handle
            # Reconstruct the Storage object from the global_user_state
            logger.debug('Detected existing storage object, '
                         f'loading Storage: {self.name}')
            self._add_store_from_metadata(self.handle.sky_stores)

            # When a storage object is reconstructed from global_user_state,
            # the user may have specified a new store type in the yaml file that
            # was not used with the storage object. We should error out in this
            # case, as we don't support having multiple stores for the same
            # storage object.
            if any(s is None for s in self.stores.values()):
                new_store_type = None
                previous_store_type = None
                for store_type, store in self.stores.items():
                    if store is not None:
                        previous_store_type = store_type
                    else:
                        new_store_type = store_type
                if previous_store_type is None or new_store_type is None:
                    # This should not happen if the condition above is true,
                    # but add check for type safety
                    raise exceptions.StorageBucketCreateError(
                        f'Bucket {self.name} has inconsistent store types.')
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketCreateError(
                        f'Bucket {self.name} was previously created for '
                        f'{previous_store_type.value.lower()!r}, but a new '
                        f'store type {new_store_type.value.lower()!r} is '
                        'requested. This is not supported yet. Please specify '
                        'the same store type: '
                        f'{previous_store_type.value.lower()!r}.')

            # TODO(romilb): This logic should likely be in add_store to move
            # syncing to file_mount stage..
            if self.sync_on_reconstruction:
                msg = ''
                if (self.source and
                    (isinstance(self.source, list) or
                     not data_utils.is_cloud_store_url(self.source))):
                    msg = ' and uploading from source'
                logger.info(f'Verifying bucket{msg} for storage {self.name}')
                self.sync_all_stores()
            # Update MOUNT_CACHED configuration to the new one.
            self.handle.mount_cached_config = self.mount_cached_config
            self.handle.file_mount_type = self.file_mount_type
            # Update MOUNT configuration to the new one.
            self.handle.mount_config = self.mount_config
        else:
            # Storage does not exist in global_user_state, create new stores
            # Sky optimizer either adds a storage object instance or selects
            # from existing ones
            input_stores = self.stores
            self.stores = {}
            self.handle = self.StorageMetadata(
                storage_name=self.name,
                source=self.source,
                mode=self.mode,
                mount_cached_config=self.mount_cached_config,
                file_mount_type=self.file_mount_type,
                mount_config=self.mount_config)

            for store_type in input_stores:
                self.add_store(store_type, self._store_region)

            if self.source is not None:
                # If source is a pre-existing bucket, connect to the bucket
                # If the bucket does not exist, this will error out
                if isinstance(self.source, str):
                    if self.source.startswith('gs://'):
                        self.add_store(StoreType.GCS)
                    elif data_utils.is_az_container_endpoint(self.source):
                        self.add_store(StoreType.AZURE)
                    elif self.source.startswith('cos://'):
                        self.add_store(StoreType.IBM)
                    elif self.source.startswith('oci://'):
                        self.add_store(StoreType.OCI)
                    elif data_utils.is_hf_path(self.source):
                        self.add_store(StoreType.HF)

                    s3_compatible_store_type: StoreType | None = (
                        StoreType.find_s3_compatible_config_by_prefix(
                            self.source))
                    if s3_compatible_store_type:
                        self.add_store(s3_compatible_store_type)

    def get_bucket_sub_path_prefix(self, blob_path: str) -> str:
        """Adds the bucket sub path prefix to the blob path."""
        if self._bucket_sub_path is not None:
            return f'{blob_path}/{self._bucket_sub_path}'
        return blob_path

    @staticmethod
    def _validate_source(
            source: SourceType, mode: StorageMode,
            sync_on_reconstruction: bool) -> tuple[SourceType, bool]:
        """Validates the source path.

        Args:
          source: str; File path where the data is initially stored. Can be a
            local path or a cloud URI (s3://, gs://, r2:// etc.).
            Local paths do not need to be absolute.
          mode: StorageMode; StorageMode of the storage object

        Returns:
          Tuple[source, is_local_source]
          source: str; The source path.
          is_local_path: bool; Whether the source is a local path. False if URI.
        """

        def _check_basename_conflicts(source_list: list[str]) -> None:
            """Checks if two paths in source_list have the same basename."""
            basenames = [os.path.basename(s) for s in source_list]
            conflicts = {x for x in basenames if basenames.count(x) > 1}
            if conflicts:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageSourceError(
                        'Cannot have multiple files or directories with the '
                        'same name in source. Conflicts found for: '
                        f'{", ".join(conflicts)}')

        def _validate_local_source(local_source):
            if local_source.endswith('/'):
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageSourceError(
                        'Storage source paths cannot end with a slash '
                        '(try "/mydir: /mydir" or "/myfile: /myfile"). '
                        f'Found source={local_source}')
            # Local path, check if it exists
            full_src = os.path.abspath(os.path.expanduser(local_source))
            # Only check if local source exists if it is synced to the bucket
            if not os.path.exists(full_src) and sync_on_reconstruction:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageSourceError(
                        'Local source path does not'
                        f' exist: {local_source}')
            # Raise warning if user's path is a symlink
            elif os.path.islink(full_src):
                logger.warning(f'Source path {source} is a symlink. '
                               'Referenced contents are uploaded, matching '
                               'the default behavior for S3 and GCS syncing.')

        # Check if source is a list of paths
        if isinstance(source, list):
            # Check for conflicts in basenames
            _check_basename_conflicts(source)
            # Validate each path
            for local_source in source:
                _validate_local_source(local_source)
            is_local_source = True
        else:
            # Check if str source is a valid local/remote URL
            split_path = urllib.parse.urlsplit(source)
            if split_path.scheme == '':
                _validate_local_source(source)
                # Check if source is a file - throw error if it is
                full_src = os.path.abspath(os.path.expanduser(source))
                if os.path.isfile(full_src):
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageSourceError(
                            'Storage source path cannot be a file - only'
                            ' directories are supported as a source. '
                            'To upload a single file, specify it in a list '
                            f'by writing source: [{source}]. Note '
                            'that the file will be uploaded to the root of the '
                            'bucket and will appear at <destination_path>/'
                            f'{os.path.basename(source)}. Alternatively, you '
                            'can directly upload the file to the VM without '
                            'using a bucket by writing <destination_path>: '
                            f'{source} in the file_mounts section of your YAML')
                is_local_source = True
            elif split_path.scheme in [
                    's3', 'gs', 'https', 'r2', 'cos', 'oci', 'nebius', 'cw',
                    'vastdata', 'hf'
            ]:
                is_local_source = False
                # Storage mounting does not support mounting specific files from
                # cloud store - ensure path points to only a directory.
                # ``hf://`` is exempt because hf-mount supports mounting
                # sub-paths within a bucket or repo.
                if (mode in MOUNTABLE_STORAGE_MODES and
                        split_path.scheme != 'hf'):
                    if (split_path.scheme != 'https' and
                        ((split_path.scheme != 'cos' and
                          split_path.path.strip('/') != '') or
                         (split_path.scheme == 'cos' and
                          not re.match(r'^/[-\w]+(/\s*)?$', split_path.path)))):
                        # regex allows split_path.path to include /bucket
                        # or /bucket/optional_whitespaces while considering
                        # cos URI's regions (cos://region/bucket_name)
                        with ux_utils.print_exception_no_traceback():
                            raise exceptions.StorageModeError(
                                'MOUNT mode does not support'
                                ' mounting specific files from cloud'
                                ' storage. Please use COPY mode or'
                                ' specify only the bucket name as'
                                ' the source.')
            else:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageSourceError(
                        f'Supported paths: local, s3://, gs://, https://, '
                        f'r2://, cos://, oci://, nebius://, cw://, '
                        f'vastdata://, hf://. Got: {source}')
        return source, is_local_source

    def _validate_storage_spec(self, name: str | None) -> None:
        """Validates the storage spec and updates local fields if necessary."""

        def validate_name(name):
            """ Checks for validating the storage name.

            Checks if the name starts the s3, gcs or r2 prefix and raise error
            if it does. Store specific validation checks (e.g., S3 specific
            rules) happen in the corresponding store class.
            """
            prefix = name.split('://')[0]
            prefix = prefix.lower()
            if prefix in [
                    's3',
                    'gs',
                    'https',
                    'r2',
                    'cos',
                    'oci',
                    'nebius',
                    'cw',
                    'vastdata',
                    'hf',
            ]:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageNameError(
                        'Prefix detected: `name` cannot start with '
                        f'{prefix}://. If you are trying to use an existing '
                        'bucket created outside of SkyPilot, please specify it '
                        'using the `source` field (e.g. '
                        '`source: s3://mybucket/`). If you are trying to '
                        'create a new bucket, please use the `store` field to '
                        'specify the store type (e.g. `store: s3`).')

        if self.source is None:
            # If the mode is COPY, the source must be specified
            if self.mode == StorageMode.COPY:
                # Check if a Storage object already exists in global_user_state
                # (e.g. used as scratch previously). Such storage objects can be
                # mounted in copy mode even though they have no source in the
                # yaml spec (the name is the source).
                handle = global_user_state.get_handle_from_storage_name(name)
                if handle is None:
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageSourceError(
                            'New storage object: source must be specified when '
                            'using COPY mode.')
            else:
                # If source is not specified in COPY mode, the intent is to
                # create a bucket and use it as scratch disk. Name must be
                # specified to create bucket.
                if not name:
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageSpecError(
                            'Storage source or storage name must be specified.')
            assert name is not None, self
            validate_name(name)
            self.name = name
            return
        elif self.source is not None:
            source, is_local_source = Storage._validate_source(
                self.source, self.mode, self.sync_on_reconstruction)
            if not name:
                if is_local_source:
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageNameError(
                            'Storage name must be specified if the source is '
                            'local.')
                else:
                    assert isinstance(source, str)
                    # Set name to source bucket name and continue
                    if data_utils.is_hf_path(source):
                        if data_utils.is_hf_bucket_path(source):
                            name, sub_path = data_utils.split_hf_path(source)
                        else:
                            repo_type, repo_id, _, sub_path = (
                                data_utils.split_hf_repo_path(source))
                            name = HuggingFaceStore.hf_id_from_repo_parts(
                                repo_type, repo_id)
                        if sub_path and self._bucket_sub_path is None:
                            self._bucket_sub_path = sub_path
                    elif source.startswith('cos://'):
                        # cos url requires custom parsing
                        name = data_utils.split_cos_path(source)[0]
                    elif data_utils.is_az_container_endpoint(source):
                        _, name, _ = data_utils.split_az_path(source)
                    else:
                        name = urllib.parse.urlsplit(source).netloc
                    assert name is not None, source
                    self.name = name
                    return
            else:
                if is_local_source:
                    # If name is specified and source is local, upload to bucket
                    assert name is not None, source
                    validate_name(name)
                    self.name = name
                    return
                else:
                    # Both name and source should not be specified if the source
                    # is a URI. Name will be inferred from the URI.
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageSpecError(
                            'Storage name should not be specified if the '
                            'source is a remote URI.')
        raise exceptions.StorageSpecError(
            f'Validation failed for storage source {self.source}, name '
            f'{self.name} and mode {self.mode}. Please check the arguments.')

    def _add_store_from_metadata(
            self, sky_stores: dict[StoreType,
                                   AbstractStore.StoreMetadata]) -> None:
        """Reconstructs Storage.stores from sky_stores.

        Reconstruct AbstractStore objects from sky_store's metadata and
        adds them into Storage.stores
        """
        for s_type, s_metadata in sky_stores.items():
            # When initializing from global_user_state, we override the
            # source from the YAML
            try:
                if s_type.value in _S3_COMPATIBLE_STORES:
                    store_class = _S3_COMPATIBLE_STORES[s_type.value]
                    store = store_class.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.S3:
                    store = S3Store.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.R2:
                    store = R2Store.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.GCS:
                    store = GcsStore.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.AZURE:
                    assert isinstance(s_metadata,
                                      AzureBlobStore.AzureBlobStoreMetadata)
                    store = AzureBlobStore.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.IBM:
                    store = IBMCosStore.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.OCI:
                    oci_store_cls: type[AbstractStore] = (
                        OciS3CompatibleStore
                        if oci_s3.use_s3_api() else OciStore)
                    store = oci_store_cls.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.HF:
                    store = HuggingFaceStore.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.NEBIUS:
                    store = NebiusStore.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                elif s_type == StoreType.COREWEAVE:
                    store = CoreWeaveStore.from_metadata(
                        s_metadata,
                        source=self.source,
                        sync_on_reconstruction=self.sync_on_reconstruction,
                        _bucket_sub_path=self._bucket_sub_path)
                else:
                    with ux_utils.print_exception_no_traceback():
                        raise ValueError(f'Unknown store type: {s_type}')
            # Following error is caught when an externally removed storage
            # is attempted to be fetched.
            except exceptions.StorageExternalDeletionError as e:
                if isinstance(e, exceptions.NonExistentStorageAccountError):
                    assert isinstance(s_metadata,
                                      AzureBlobStore.AzureBlobStoreMetadata)
                    logger.debug(f'Storage object {self.name!r} was attempted '
                                 'to be reconstructed while the corresponding '
                                 'storage account '
                                 f'{s_metadata.storage_account_name!r} does '
                                 'not exist.')
                else:
                    logger.debug(f'Storage object {self.name!r} was attempted '
                                 'to be reconstructed while the corresponding '
                                 'bucket was externally deleted.')
                continue
            self._add_store(store, is_reconstructed=True)

    @classmethod
    def from_metadata(cls, metadata: StorageMetadata,
                      **override_args) -> 'Storage':
        """Create Storage from StorageMetadata object.

        Used when reconstructing Storage object and AbstractStore objects from
        global_user_state.
        """
        # Name should not be specified if the source is a cloud store URL.
        source = override_args.get('source', metadata.source)
        name = override_args.get('name', metadata.storage_name)
        # If the source is a list, it consists of local paths
        if not isinstance(source,
                          list) and data_utils.is_cloud_store_url(source):
            name = None

        storage_obj = cls(name=name,
                          source=source,
                          sync_on_reconstruction=override_args.get(
                              'sync_on_reconstruction', True))

        if metadata.mode is not None:
            storage_obj.mode = override_args.get('mode', metadata.mode)

        if metadata.mount_cached_config is not None:
            storage_obj.mount_cached_config = metadata.mount_cached_config

        if metadata.file_mount_type is not None:
            storage_obj.file_mount_type = metadata.file_mount_type

        if metadata.mount_config is not None:
            storage_obj.mount_config = metadata.mount_config

        return storage_obj

    def add_store(self,
                  store_type: str | StoreType,
                  region: str | None = None) -> AbstractStore:
        """Initializes and adds a new store to the storage.

        Invoked by the optimizer after it has selected a store to
        add it to Storage.

        Args:
          store_type: StoreType; Type of the storage [S3, GCS, AZURE, R2, IBM]
          region: str; Region to place the bucket in. Caller must ensure that
            the region is valid for the chosen store_type.
        """
        assert self._constructed, self
        assert self.name is not None, self

        if isinstance(store_type, str):
            store_type = StoreType(store_type)

        if self.stores.get(store_type) is not None:
            if store_type == StoreType.AZURE:
                azure_store_obj = self.stores[store_type]
                assert isinstance(azure_store_obj, AzureBlobStore)
                storage_account_name = azure_store_obj.storage_account_name
                logger.info(f'Storage type {store_type} already exists under '
                            f'storage account {storage_account_name!r}.')
            else:
                logger.info(f'Storage type {store_type} already exists.')
            store = self.stores[store_type]
            assert store is not None, self
            return store

        store_cls: type[AbstractStore]
        # First check if it's a registered S3-compatible store
        if store_type.value in _S3_COMPATIBLE_STORES:
            store_cls = _S3_COMPATIBLE_STORES[store_type.value]
        elif store_type == StoreType.GCS:
            store_cls = GcsStore
        elif store_type == StoreType.AZURE:
            store_cls = AzureBlobStore
        elif store_type == StoreType.IBM:
            store_cls = IBMCosStore
        elif store_type == StoreType.OCI:
            store_cls = (OciS3CompatibleStore
                         if oci_s3.use_s3_api() else OciStore)
        elif store_type == StoreType.HF:
            store_cls = HuggingFaceStore
        else:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageSpecError(
                    f'{store_type} not supported as a Store.')
        try:
            store = store_cls(
                name=self.name,
                source=self.source,
                region=region,
                sync_on_reconstruction=self.sync_on_reconstruction,
                is_sky_managed=self._is_sky_managed,
                _bucket_sub_path=self._bucket_sub_path)
        except exceptions.StorageBucketCreateError:
            # Creation failed, so this must be sky managed store. Add failure
            # to state.
            logger.error(f'Could not create {store_type} store '
                         f'with name {self.name}.')
            try:
                global_user_state.set_storage_status(self.name,
                                                     StorageStatus.INIT_FAILED)
            except ValueError as e:
                logger.error(f'Error setting storage status: {e}')
            raise
        except exceptions.StorageBucketGetError:
            # Bucket get failed, so this is not sky managed. Do not update state
            logger.error(f'Could not get {store_type} store '
                         f'with name {self.name}.')
            raise
        except exceptions.StorageInitError:
            logger.error(f'Could not initialize {store_type} store with '
                         f'name {self.name}. General initialization error.')
            raise
        except exceptions.StorageSpecError:
            logger.error(f'Could not mount externally created {store_type} '
                         f'store with name {self.name!r}.')
            raise

        # Add store to storage
        self._add_store(store)

        # Upload source to store
        self._sync_store(store)

        return store

    def _add_store(self, store: AbstractStore, is_reconstructed: bool = False):
        # Adds a store object to the storage
        store_type = StoreType.from_store(store)
        self.stores[store_type] = store
        # If store initialized and is sky managed, add to state
        if store.is_sky_managed:
            self.handle.add_store(store)
            if not is_reconstructed:
                assert self.name is not None, self
                global_user_state.add_or_update_storage(self.name, self.handle,
                                                        StorageStatus.INIT)

    def delete(self, store_type: StoreType | None = None) -> None:
        """Deletes data for all sky-managed storage objects.

        If a storage is not managed by sky, it is not deleted from the cloud.
        User must manually delete any object stores created outside of sky.

        Args:
            store_type: StoreType; Specific cloud store to remove from the list
              of backing stores.
        """
        if not self.stores:
            logger.info('No backing stores found. Deleting storage.')
            assert self.name is not None
            global_user_state.remove_storage(self.name)
        if store_type is not None:
            assert self.name is not None
            store = self.stores[store_type]
            assert store is not None, self
            is_sky_managed = store.is_sky_managed
            # We delete a store from the cloud if it's sky managed. Else just
            # remove handle and return
            if is_sky_managed:
                self.handle.remove_store(store)
                store.delete()
                # Check remaining stores - if none is sky managed, remove
                # the storage from global_user_state.
                delete = True
                for store in self.stores.values():
                    assert store is not None, self
                    if store.is_sky_managed:
                        delete = False
                        break
                if delete:
                    global_user_state.remove_storage(self.name)
                else:
                    global_user_state.set_storage_handle(self.name, self.handle)
            elif self.force_delete:
                store.delete()
            # Remove store from bookkeeping
            del self.stores[store_type]
        else:
            for _, store in self.stores.items():
                assert store is not None, self
                if store.is_sky_managed:
                    self.handle.remove_store(store)
                    store.delete()
                elif self.force_delete:
                    store.delete()
            self.stores = {}
            # Remove storage from global_user_state if present
            if self.name is not None:
                global_user_state.remove_storage(self.name)

    def sync_all_stores(self):
        """Syncs the source and destinations of all stores in the Storage"""
        for _, store in self.stores.items():
            assert store is not None, self
            self._sync_store(store)

    def _sync_store(self, store: AbstractStore):
        """Runs the upload routine for the store and handles failures"""
        assert self._constructed, self
        assert self.name is not None, self

        def warn_for_git_dir(source: str):
            if os.path.isdir(os.path.join(source, '.git')):
                logger.warning(f'\'.git\' directory under \'{self.source}\' '
                               'is excluded during sync.')

        try:
            if self.source is not None:
                if isinstance(self.source, str):
                    warn_for_git_dir(self.source)
                else:
                    for source in self.source:
                        warn_for_git_dir(source)
            store.upload()
        except exceptions.StorageUploadError:
            logger.error(f'Could not upload {self.source!r} to store '
                         f'name {store.name!r}.')
            if store.is_sky_managed:
                global_user_state.set_storage_status(
                    self.name, StorageStatus.UPLOAD_FAILED)
            raise

        # Upload succeeded - update state
        if store.is_sky_managed:
            global_user_state.set_storage_status(self.name, StorageStatus.READY)

    @classmethod
    def from_handle(cls, handle: StorageHandle) -> 'Storage':
        """Create Storage from StorageHandle object.
        """
        obj = cls(name=handle.storage_name,
                  source=handle.source,
                  sync_on_reconstruction=False)
        obj.handle = handle
        obj._add_store_from_metadata(handle.sky_stores)
        return obj

    @classmethod
    def from_yaml_config(cls, config: dict[str, Any]) -> 'Storage':
        common_utils.validate_schema(config, schemas.get_storage_schema(),
                                     'Invalid storage YAML: ')
        name = config.pop('name', None)
        source = config.pop('source', None)
        store = config.pop('store', None)
        mode_str = config.pop('mode', None)
        type_str = config.pop('type', None)
        force_delete = config.pop('_force_delete', None)
        # pylint: disable=invalid-name
        _is_sky_managed = config.pop('_is_sky_managed', None)
        # pylint: disable=invalid-name
        _bucket_sub_path = config.pop('_bucket_sub_path', None)
        # pylint: disable=invalid-name
        _store_region = config.pop('_store_region', None)
        if force_delete is None:
            force_delete = False

        if isinstance(mode_str, str):
            # Make mode case insensitive, if specified
            mode = StorageMode(mode_str.upper())
        else:
            mode = DEFAULT_STORAGE_MODE
        persistent = config.pop('persistent', None)
        if persistent is None:
            persistent = True

        storage_config = config.pop('config', None)

        # Parse file mount type enum if present
        file_mount_type = None
        if isinstance(type_str, str):
            file_mount_type = FileMountType(type_str.upper())

        # Parse mount_cached config if present
        mount_cached_config = None
        if storage_config is not None:
            mount_cached_dict = storage_config.get('mount_cached')
            if mount_cached_dict is not None:
                mount_cached_config = MountCachedConfig.from_yaml_config(
                    mount_cached_dict)

        # Parse mount config if present
        mount_config = None
        if storage_config is not None:
            mount_dict = storage_config.get('mount')
            if mount_dict is not None:
                mount_config = MountConfig.from_yaml_config(mount_dict)

        assert not config, f'Invalid storage args: {config.keys()}'

        # Validation of the config object happens on instantiation.
        if store is not None:
            stores = [StoreType(store.upper())]
        else:
            stores = None
        storage_obj = cls(name=name,
                          source=source,
                          persistent=persistent,
                          mode=mode,
                          stores=stores,
                          _is_sky_managed=_is_sky_managed,
                          _bucket_sub_path=_bucket_sub_path,
                          _store_region=_store_region,
                          mount_cached_config=mount_cached_config,
                          file_mount_type=file_mount_type,
                          mount_config=mount_config)

        # Add force deletion flag
        storage_obj.force_delete = force_delete
        return storage_obj

    def to_yaml_config(self) -> dict[str, Any]:
        config = {}

        def add_if_not_none(key: str, value: Any | None):
            if value is not None:
                config[key] = value

        name = None
        if (self.source is None or not isinstance(self.source, str) or
                not data_utils.is_cloud_store_url(self.source)):
            # Remove name if source is a cloud store URL
            name = self.name
        add_if_not_none('name', name)
        add_if_not_none('source', self.source)

        stores = None
        is_sky_managed = self._is_sky_managed
        if self.stores:
            stores = ','.join([store.value for store in self.stores])
            store = list(self.stores.values())[0]
            if store is not None:
                is_sky_managed = store.is_sky_managed
        add_if_not_none('store', stores)
        add_if_not_none('_is_sky_managed', is_sky_managed)
        add_if_not_none('persistent', self.persistent)
        add_if_not_none('mode', self.mode.value)
        if self.file_mount_type is not None:
            config['type'] = self.file_mount_type.value
        if self.force_delete:
            config['_force_delete'] = True
        if self._bucket_sub_path is not None:
            config['_bucket_sub_path'] = self._bucket_sub_path
        if self._store_region is not None:
            config['_store_region'] = self._store_region
        storage_config_dict: dict[str, Any] = {}
        if self.mount_cached_config is not None:
            mount_cached_dict = self.mount_cached_config.to_yaml_config()
            if mount_cached_dict:
                storage_config_dict['mount_cached'] = mount_cached_dict
        if self.mount_config is not None:
            mount_dict = self.mount_config.to_yaml_config()
            if mount_dict:
                storage_config_dict['mount'] = mount_dict
        if storage_config_dict:
            config['config'] = storage_config_dict
        return config


# Imported late to avoid circular dependencies: the extracted provider
# backends build on shared storage primitives defined above and are re-exported
# here to preserve the existing public and pickle import paths.
# pylint: disable=wrong-import-position,ungrouped-imports
gcs_storage_cloud_enabled: Callable[..., bool] = _is_storage_cloud_enabled
GCS_MAX_CONCURRENT_UPLOADS: int = _MAX_CONCURRENT_UPLOADS
GCS_BUCKET_FAIL_TO_CONNECT_MESSAGE: str = _BUCKET_FAIL_TO_CONNECT_MESSAGE
GCS_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE: str = (
    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
GCS_STORAGE_LOG_FILE_NAME: str = _STORAGE_LOG_FILE_NAME
azure_storage_cloud_enabled: Callable[..., bool] = _is_storage_cloud_enabled
AZURE_MAX_CONCURRENT_UPLOADS: int = _MAX_CONCURRENT_UPLOADS
AZURE_BUCKET_FAIL_TO_CONNECT_MESSAGE: str = _BUCKET_FAIL_TO_CONNECT_MESSAGE
AZURE_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE: str = (
    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
AZURE_STORAGE_LOG_FILE_NAME: str = _STORAGE_LOG_FILE_NAME
IBM_MAX_CONCURRENT_UPLOADS: int = _MAX_CONCURRENT_UPLOADS
IBM_BUCKET_FAIL_TO_CONNECT_MESSAGE: str = _BUCKET_FAIL_TO_CONNECT_MESSAGE
IBM_STORAGE_LOG_FILE_NAME: str = _STORAGE_LOG_FILE_NAME
s3_storage_cloud_enabled: Callable[..., bool] = _is_storage_cloud_enabled
S3_MAX_CONCURRENT_UPLOADS: int = _MAX_CONCURRENT_UPLOADS
S3_BUCKET_FAIL_TO_CONNECT_MESSAGE: str = _BUCKET_FAIL_TO_CONNECT_MESSAGE
S3_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE: str = (
    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
S3_STORAGE_LOG_FILE_NAME: str = _STORAGE_LOG_FILE_NAME

from sky.data import storage_azure
from sky.data import storage_gcs
from sky.data import storage_ibm

AzureBlobStore = storage_azure.AzureBlobStore
GcsStore = storage_gcs.GcsStore
IBMCosStore = storage_ibm.IBMCosStore

from sky.data import storage_s3

_S3_COMPATIBLE_STORES = storage_s3.S3_COMPATIBLE_STORES
register_s3_compatible_store = storage_s3.register_s3_compatible_store
S3CompatibleConfig = storage_s3.S3CompatibleConfig
S3CompatibleStore = storage_s3.S3CompatibleStore
S3Store = storage_s3.S3Store
R2Store = storage_s3.R2Store
NebiusStore = storage_s3.NebiusStore
CoreWeaveStore = storage_s3.CoreWeaveStore
VastDataStore = storage_s3.VastDataStore
# Keep historical module patch points used by provider tests and integrations.
mounting_utils = storage_s3.mounting_utils
subprocess = storage_s3.subprocess

from sky.data import storage_oci

# pylint: enable=wrong-import-position,ungrouped-imports

OciStore = storage_oci.OciStore
OciS3CompatibleStore = storage_oci.OciS3CompatibleStore
