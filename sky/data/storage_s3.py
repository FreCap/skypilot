"""S3-compatible object storage backend implementations."""
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
import dataclasses
import logging
import os
import re
import shlex
import subprocess
import typing
from typing import Any

import colorama

from sky import clouds
from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import aws
from sky.adaptors import cloudflare
from sky.adaptors import coreweave
from sky.adaptors import nebius
from sky.adaptors import vastdata
from sky.data import data_transfer
from sky.data import data_utils
from sky.data import mounting_utils
from sky.data import storage as storage_lib
from sky.data import storage_azure
from sky.data import storage_gcs
from sky.data import storage_ibm
from sky.data import storage_utils
from sky.provision import constants as provision_constants
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    import mypy_boto3_s3

StorageHandle = storage_lib.StorageHandle
Path = storage_lib.Path
AbstractStore = storage_lib.AbstractStore
MountCachedConfig = storage_lib.MountCachedConfig
GcsStore = storage_gcs.GcsStore
AzureBlobStore = storage_azure.AzureBlobStore
IBMCosStore = storage_ibm.IBMCosStore
logger: logging.Logger = storage_lib.logger
_is_storage_cloud_enabled = storage_lib.s3_storage_cloud_enabled
_MAX_CONCURRENT_UPLOADS = storage_lib.S3_MAX_CONCURRENT_UPLOADS
_BUCKET_FAIL_TO_CONNECT_MESSAGE = (
    storage_lib.S3_BUCKET_FAIL_TO_CONNECT_MESSAGE)
_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE = (
    storage_lib.S3_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
_STORAGE_LOG_FILE_NAME = storage_lib.S3_STORAGE_LOG_FILE_NAME

# Registry for S3-compatible stores
_S3_COMPATIBLE_STORES = {}


def _quote_cli_path(path: str) -> str:
    """Expand a configured path before making it shell-safe."""
    return shlex.quote(os.path.expandvars(os.path.expanduser(path)))


def register_s3_compatible_store(store_class):
    """Decorator to automatically register S3-compatible stores."""
    store_type = store_class.get_store_type()
    _S3_COMPATIBLE_STORES[store_type] = store_class
    return store_class


@dataclasses.dataclass
class S3CompatibleConfig:
    """Configuration for S3-compatible storage providers."""
    # Provider identification
    store_type: str  # Store type identifier (e.g., "S3", "R2", "MINIO")
    url_prefix: str  # URL prefix (e.g., "s3://", "r2://", "minio://")

    # Client creation
    client_factory: Callable[[str | None], Any]
    resource_factory: Callable[[str], StorageHandle]
    split_path: Callable[[str], tuple[str, str]]
    verify_bucket: Callable[[str], bool]

    # CLI configuration
    aws_profile: str | None = None
    get_endpoint_url: Callable[[], str] | None = None
    credentials_file: str | None = None
    config_file: str | None = None
    extra_cli_args: list[str] | None = None
    # Extra environment variables to prefix onto the AWS CLI upload commands.
    # Used by OCI to disable aws-chunked uploads (see OciS3CompatibleStore).
    extra_cli_env: dict[str, str] | None = None

    # Provider-specific settings
    cloud_name: str = ''
    default_region: str | None = None
    access_denied_message: str = 'Access Denied'

    # Mounting
    mount_cmd_factory: Callable | None = None
    mount_cached_cmd_factory: Callable | None = None

    def __post_init__(self):
        if self.extra_cli_args is None:
            self.extra_cli_args = []
        if self.extra_cli_env is None:
            self.extra_cli_env = {}


class S3CompatibleStore(AbstractStore, ABC):
    """Base class for S3-compatible object storage providers.

    This class provides a unified interface for all S3-compatible storage
    providers (AWS S3, Cloudflare R2, Nebius, MinIO, CoreWeave, etc.) by
    leveraging a configuration-driven approach that eliminates code duplication

    ## Adding a New S3-Compatible Store

    To add a new S3-compatible storage provider (e.g., MinIO),
    follow these steps:

    ### 1. Add Store Type to Enum
    First, add your store type to the StoreType enum:
    ```python
    class StoreType(enum.Enum):
        # ... existing entries ...
        MINIO = 'MINIO'
    ```

    ### 2. Create Store Class
    Create a new store class that inherits from S3CompatibleStore:
    ```python
    @register_s3_compatible_store
    class MinIOStore(S3CompatibleStore):
        '''MinIOStore for MinIO object storage.'''

        @classmethod
        def get_config(cls) -> S3CompatibleConfig:
            '''Return the configuration for MinIO.'''
            return S3CompatibleConfig(
                store_type='MINIO',
                url_prefix='minio://',
                client_factory=lambda region:\
                    data_utils.create_minio_client(region),
                resource_factory=lambda name:\
                    minio.resource('s3').Bucket(name),
                split_path=data_utils.split_minio_path,
                aws_profile='minio',
                get_endpoint_url=lambda: minio.get_endpoint_url(),
                cloud_name='minio',
                default_region='us-east-1',
                mount_cmd_factory=mounting_utils.get_minio_mount_cmd,
            )
    ```

    ### 3. Implement Required Utilities
    Create the necessary utility functions:

    #### In `sky/data/data_utils.py`:
    ```python
    def create_minio_client(region: Optional[str] = None):
        '''Create MinIO S3 client.'''
        return boto3.client('s3',
                          endpoint_url=minio.get_endpoint_url(),
                          aws_access_key_id=minio.get_access_key(),
                          aws_secret_access_key=minio.get_secret_key(),
                          region_name=region or 'us-east-1')

    def split_minio_path(minio_path: str) -> Tuple[str, str]:
        '''Split minio://bucket/key into (bucket, key).'''
        path_parts = minio_path.replace('minio://', '').split('/', 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ''
        return bucket, key
    ```

    #### In `sky/utils/mounting_utils.py`:
    ```python
    def get_minio_mount_cmd(profile: str, bucket_name: str, endpoint_url: str,
                           mount_path: str,
                           bucket_sub_path: Optional[str]) -> str:
        '''Generate MinIO mount command using s3fs.'''
        # Implementation similar to other S3-compatible mount commands
        pass
    ```

    ### 4. Create Adapter Module (if needed)
    Create `sky/adaptors/minio.py` for MinIO-specific configuration:
    ```python
    '''MinIO adapter for SkyPilot.'''

    MINIO_PROFILE_NAME = 'minio'

    def get_endpoint_url() -> str:
        '''Get MinIO endpoint URL from configuration.'''
        # Read from ~/.minio/config or environment variables
        pass

    def resource(resource_name: str):
        '''Get MinIO resource.'''
        # Implementation for creating MinIO resources
        pass
    ```

    """

    _ACCESS_DENIED_MESSAGE = 'Access Denied'

    def __init__(self,
                 name: str,
                 source: str,
                 region: str | None = None,
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool = True,
                 _bucket_sub_path: str | None = None):
        # Initialize configuration first to get defaults
        self.config = self.__class__.get_config()

        # Use provider's default region if not specified
        if region is None:
            region = self.config.default_region

        # Initialize S3CompatibleStore specific attributes
        self.client: mypy_boto3_s3.Client
        self.bucket: StorageHandle

        # Call parent constructor
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    @classmethod
    @abstractmethod
    def get_config(cls) -> S3CompatibleConfig:
        """Return the configuration for this S3-compatible provider."""
        pass

    @classmethod
    def get_store_type(cls) -> str:
        """Return the store type identifier from configuration."""
        return cls.get_config().store_type

    @property
    def provider_prefixes(self) -> set:
        """Dynamically get all provider prefixes from registered stores."""
        prefixes = set()

        # Get prefixes from all registered S3-compatible stores
        for store_class in _S3_COMPATIBLE_STORES.values():
            config = store_class.get_config()
            prefixes.add(config.url_prefix)

        # Add hardcoded prefixes for non-S3-compatible stores
        prefixes.update({
            'gs://',  # GCS
            'https://',  # Azure
            'cos://',  # IBM COS
            'oci://',  # OCI
        })

        return prefixes

    def _validate(self):
        if self.source is not None and isinstance(self.source, str):
            if self.source.startswith(self.config.url_prefix):
                bucket_name, _ = self.config.split_path(self.source)
                assert self.name == bucket_name, (
                    f'{self.config.store_type} Bucket is specified as path, '
                    f'the name should be the same as {self.config.store_type} '
                    f'bucket.')
                # Only verify if this is NOT the same store type as the source
                if self.__class__.get_store_type() != self.config.store_type:
                    assert self.config.verify_bucket(self.name), (
                        f'Source specified as {self.source},'
                        f'a {self.config.store_type} '
                        f'bucket. {self.config.store_type} Bucket should exist.'
                    )
            elif self.source.startswith('gs://'):
                assert self.name == data_utils.split_gcs_path(self.source)[0], (
                    'GCS Bucket is specified as path, the name should be '
                    'the same as GCS bucket.')
                if not isinstance(self, GcsStore):
                    assert data_utils.verify_gcs_bucket(self.name), (
                        f'Source specified as {self.source}, a GCS bucket. ',
                        'GCS Bucket should exist.')
            elif data_utils.is_az_container_endpoint(self.source):
                storage_account_name, container_name, _ = (
                    data_utils.split_az_path(self.source))
                assert self.name == container_name, (
                    'Azure bucket is specified as path, the name should be '
                    'the same as Azure bucket.')
                if not isinstance(self, AzureBlobStore):
                    assert data_utils.verify_az_bucket(
                        storage_account_name, self.name
                    ), (f'Source specified as {self.source}, an Azure bucket. '
                        'Azure bucket should exist.')
            elif self.source.startswith('cos://'):
                assert self.name == data_utils.split_cos_path(self.source)[0], (
                    'COS Bucket is specified as path, the name should be '
                    'the same as COS bucket.')
                if not isinstance(self, IBMCosStore):
                    assert data_utils.verify_ibm_cos_bucket(self.name), (
                        f'Source specified as {self.source}, a COS bucket. ',
                        'COS Bucket should exist.')
            elif self.source.startswith('oci://'):
                raise NotImplementedError(
                    f'Moving data from OCI to {self.source} is ',
                    'currently not supported.')

        # Validate name
        self.name = self.validate_name(self.name)

        # Check if the storage is enabled
        if not _is_storage_cloud_enabled(self.config.cloud_name):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesUnavailableError(
                    f'Storage "store: {self.config.store_type.lower()}" '
                    f'specified, but '
                    f'{self.config.cloud_name} access is disabled. '
                    'To fix, enable '
                    f'{self.config.cloud_name} by running `sky check`.')

    @classmethod
    def validate_name(cls, name: str) -> str:
        """Validates the name of the S3 store.

        Source for rules: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html # pylint: disable=line-too-long
        """

        def _raise_no_traceback_name_error(err_str):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageNameError(err_str)

        if name is not None and isinstance(name, str):
            if not 3 <= len(name) <= 63:
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must be between 3 (min) '
                    'and 63 (max) characters long.')

            # Check for valid characters and start/end with a letter or number
            pattern = r'^[a-z0-9][-a-z0-9.]*[a-z0-9]$'
            if not re.match(pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} can consist only of '
                    'lowercase letters, numbers, dots (.), and hyphens (-). '
                    'It must begin and end with a letter or number.')

            # Check for two adjacent periods
            if '..' in name:
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must not contain '
                    'two adjacent periods.')

            # Check for IP address format
            ip_pattern = r'^(?:\d{1,3}\.){3}\d{1,3}$'
            if re.match(ip_pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must not be formatted as '
                    'an IP address (for example, 192.168.5.4).')

            # Check for 'xn--' prefix
            if name.startswith('xn--'):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must not start with the '
                    'prefix "xn--".')

            # Check for '-s3alias' suffix
            if name.endswith('-s3alias'):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must not end with the '
                    'suffix "-s3alias".')

            # Check for '--ol-s3' suffix
            if name.endswith('--ol-s3'):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must not end with the '
                    'suffix "--ol-s3".')
        else:
            _raise_no_traceback_name_error('Store name must be specified.')
        return name

    def initialize(self):
        """Initializes the S3 store object on the cloud.

        Initialization involves fetching bucket if exists, or creating it if
        it does not.

        Raises:
          StorageBucketCreateError: If bucket creation fails
          StorageBucketGetError: If fetching existing bucket fails
          StorageInitError: If general initialization fails.
        """
        self.client = self.config.client_factory(self.region)
        self.bucket, is_new_bucket = self._get_bucket()
        if self.is_sky_managed is None:
            # If is_sky_managed is not specified, then this is a new storage
            # object (i.e., did not exist in global_user_state) and we should
            # set the is_sky_managed property.
            # If is_sky_managed is specified, then we take no action.
            self.is_sky_managed = is_new_bucket

    def upload(self):
        """Uploads source to store bucket.

        Upload must be called by the Storage handler - it is not called on
        Store initialization.

        Raises:
            StorageUploadError: if upload fails.
        """
        try:
            if isinstance(self.source, list):
                self.batch_aws_rsync(self.source, create_dirs=True)
            elif self.source is not None:
                if self._is_same_provider_source():
                    pass  # No transfer needed
                elif self._needs_cross_provider_transfer():
                    self._transfer_from_other_provider()
                else:
                    self.batch_aws_rsync([self.source])
        except exceptions.StorageUploadError:
            raise
        except Exception as e:
            raise exceptions.StorageUploadError(
                f'Upload failed for store {self.name}') from e

    def _is_same_provider_source(self) -> bool:
        """Check if source is from the same provider."""
        return isinstance(self.source, str) and self.source.startswith(
            self.config.url_prefix)

    def _needs_cross_provider_transfer(self) -> bool:
        """Check if source needs cross-provider transfer."""
        if not isinstance(self.source, str):
            return False
        return any(
            self.source.startswith(prefix) for prefix in self.provider_prefixes)

    def _detect_source_type(self) -> str:
        """Detect the source provider type from URL."""
        if not isinstance(self.source, str):
            return 'unknown'

        for provider in self.provider_prefixes:
            if self.source.startswith(provider):
                return provider[:-len('://')]
        return ''

    def _transfer_from_other_provider(self):
        """Transfer data from another cloud to this S3-compatible store."""
        source_type = self._detect_source_type()
        target_type = self.config.store_type.lower()

        if hasattr(data_transfer, f'{source_type}_to_{target_type}'):
            transfer_func = getattr(data_transfer,
                                    f'{source_type}_to_{target_type}')
            transfer_func(self.name, self.name)
        else:
            with ux_utils.print_exception_no_traceback():
                raise NotImplementedError(
                    f'Transfer from {source_type} to {target_type} '
                    'is not yet supported.')

    def delete(self) -> None:
        """Delete the bucket or sub-path."""
        if self._bucket_sub_path is not None and not self.is_sky_managed:
            return self._delete_sub_path()

        deleted_by_skypilot = self._delete_bucket(self.name)
        provider = self.config.store_type
        if deleted_by_skypilot:
            msg_str = f'Deleted {provider} bucket {self.name}.'
        else:
            msg_str = f'{provider} bucket {self.name} may have been deleted ' \
                      f'externally. Removing from local state.'
        logger.info(f'{colorama.Fore.GREEN}{msg_str}{colorama.Style.RESET_ALL}')

    def get_handle(self) -> StorageHandle:
        """Get storage handle using provider's resource factory."""
        return self.config.resource_factory(self.name)

    def _download_file(self, remote_path: str, local_path: str) -> None:
        """Download file using S3 API."""
        self.bucket.download_file(remote_path, local_path)

    def mount_command(self, mount_path: str, read_only: bool = False) -> str:
        """Get mount command using provider's mount factory."""
        if self.config.mount_cmd_factory is None:
            raise exceptions.NotSupportedError(
                f'Mounting not supported for {self.config.store_type}')

        install_cmd = mounting_utils.get_s3_mount_install_cmd()
        mount_cmd = self.config.mount_cmd_factory(self.bucket.name,
                                                  mount_path,
                                                  self._bucket_sub_path,
                                                  read_only=read_only)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cmd)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        """Get cached mount command. Can be overridden by subclasses."""
        if self.config.mount_cached_cmd_factory is None:
            raise exceptions.NotSupportedError(
                f'Cached mounting not supported for {self.config.store_type}')

        install_cmd = mounting_utils.get_rclone_install_cmd()
        mount_cmd = self.config.mount_cached_cmd_factory(
            self.bucket.name, mount_path, self._bucket_sub_path, config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cmd)

    def batch_aws_rsync(self,
                        source_path_list: list[Path],
                        create_dirs: bool = False) -> None:
        """Generic S3-compatible rsync using AWS CLI."""
        sub_path = f'/{self._bucket_sub_path}' if self._bucket_sub_path else ''

        def get_file_sync_command(base_dir_path, file_names):
            includes = ' '.join([
                f'--include {shlex.quote(file_name)}'
                for file_name in file_names
            ])
            base_dir_path = shlex.quote(base_dir_path)

            # Build AWS CLI command with provider-specific configuration
            cmd_parts = ['aws s3 sync --no-follow-symlinks --exclude="*"']
            cmd_parts.append(f'{includes} {base_dir_path}')
            target_uri = f's3://{self.name}{sub_path}'
            cmd_parts.append(shlex.quote(target_uri))

            # Add provider-specific arguments
            if self.config.get_endpoint_url:
                cmd_parts.append(
                    f'--endpoint-url '
                    f'{shlex.quote(self.config.get_endpoint_url())}')
            if self.config.aws_profile:
                cmd_parts.append(
                    f'--profile {shlex.quote(self.config.aws_profile)}')
            if self.config.extra_cli_args:
                cmd_parts.extend(
                    shlex.quote(arg) for arg in self.config.extra_cli_args)

            # Handle credentials file via environment
            cmd = ' '.join(cmd_parts)
            if self.config.credentials_file:
                cmd = 'AWS_SHARED_CREDENTIALS_FILE=' + \
                f'{_quote_cli_path(self.config.credentials_file)} {cmd}'
            if self.config.config_file:
                cmd = 'AWS_CONFIG_FILE=' + \
                f'{_quote_cli_path(self.config.config_file)} {cmd}'
            for env_key, env_val in (self.config.extra_cli_env or {}).items():
                cmd = f'{env_key}={shlex.quote(env_val)} {cmd}'

            return cmd

        def get_dir_sync_command(src_dir_path, dest_dir_name):
            # we exclude .git directory from the sync
            excluded_list = storage_utils.get_excluded_files(src_dir_path)
            excluded_list.append('.git/*')

            # Process exclusion patterns to make them work correctly with aws
            # s3 sync - this logic is from S3Store2 to ensure compatibility
            processed_excludes = []
            for excluded_path in excluded_list:
                # Check if the path is a directory exclusion pattern
                # For AWS S3 sync, directory patterns need to end with "/*" to
                # exclude all contents
                if (excluded_path.endswith('/') or os.path.isdir(
                        os.path.join(src_dir_path, excluded_path.rstrip('/')))):
                    # Remove any trailing slash and add '/*' to exclude all
                    # contents
                    processed_excludes.append(f'{excluded_path.rstrip("/")}/*')
                else:
                    processed_excludes.append(excluded_path)

            excludes = ' '.join([
                f'--exclude {shlex.quote(file_name)}'
                for file_name in processed_excludes
            ])
            src_dir_path = shlex.quote(src_dir_path)

            cmd_parts = ['aws s3 sync --no-follow-symlinks']
            cmd_parts.append(f'{excludes} {src_dir_path}')
            target_uri = f's3://{self.name}{sub_path}/{dest_dir_name}'
            cmd_parts.append(shlex.quote(target_uri))

            if self.config.get_endpoint_url:
                cmd_parts.append(
                    f'--endpoint-url '
                    f'{shlex.quote(self.config.get_endpoint_url())}')
            if self.config.aws_profile:
                cmd_parts.append(
                    f'--profile {shlex.quote(self.config.aws_profile)}')
            if self.config.extra_cli_args:
                cmd_parts.extend(
                    shlex.quote(arg) for arg in self.config.extra_cli_args)

            cmd = ' '.join(cmd_parts)
            if self.config.credentials_file:
                cmd = 'AWS_SHARED_CREDENTIALS_FILE=' + \
                f'{_quote_cli_path(self.config.credentials_file)} {cmd}'
            if self.config.config_file:
                cmd = 'AWS_CONFIG_FILE=' + \
                f'{_quote_cli_path(self.config.config_file)} {cmd}'
            for env_key, env_val in (self.config.extra_cli_env or {}).items():
                cmd = f'{env_key}={shlex.quote(env_val)} {cmd}'

            return cmd

        # Generate message for upload
        if len(source_path_list) > 1:
            source_message = f'{len(source_path_list)} paths'
        else:
            source_message = source_path_list[0]

        provider_prefix = self.config.url_prefix
        log_path = sky_logging.generate_tmp_logging_file_path(
            _STORAGE_LOG_FILE_NAME)
        sync_path = (f'{source_message} -> '
                     f'{provider_prefix}{self.name}{sub_path}/')

        with rich_utils.safe_status(
                ux_utils.spinner_message(f'Syncing {sync_path}',
                                         log_path=log_path)):
            data_utils.parallel_upload(
                source_path_list,
                get_file_sync_command,
                get_dir_sync_command,
                log_path,
                self.name,
                self.config.access_denied_message,
                create_dirs=create_dirs,
                max_concurrent_uploads=_MAX_CONCURRENT_UPLOADS)

        logger.info(
            ux_utils.finishing_message(f'Storage synced: {sync_path}',
                                       log_path))

    def _get_bucket(self) -> tuple[StorageHandle, bool]:
        """Get or create bucket using S3 API."""
        bucket = self.config.resource_factory(self.name)

        try:
            # Try Public bucket case.
            self.client.head_bucket(Bucket=self.name)
            self._validate_existing_bucket()
            return bucket, False
        except aws.botocore_exceptions().ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '403':
                command = f'aws s3 ls s3://{self.name}'
                if self.config.aws_profile:
                    command += (
                        f' --profile {shlex.quote(self.config.aws_profile)}')
                if self.config.get_endpoint_url:
                    command += f' --endpoint-url '\
                        f'{shlex.quote(self.config.get_endpoint_url())}'
                if self.config.credentials_file:
                    command = (
                        f'AWS_SHARED_CREDENTIALS_FILE='
                        f'{_quote_cli_path(self.config.credentials_file)} '
                        f'{command}')
                if self.config.config_file:
                    command = 'AWS_CONFIG_FILE=' + \
                    f'{_quote_cli_path(self.config.config_file)} {command}'
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        _BUCKET_FAIL_TO_CONNECT_MESSAGE.format(name=self.name) +
                        f' To debug, consider running `{command}`.') from e

        if isinstance(self.source, str) and self.source.startswith(
                self.config.url_prefix):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketGetError(
                    'Attempted to use a non-existent bucket as a source: '
                    f'{self.source}.')

        # If bucket cannot be found, create it if needed
        if self.sync_on_reconstruction:
            bucket = self._create_bucket(self.name)
            return bucket, True
        else:
            raise exceptions.StorageExternalDeletionError(
                'Attempted to fetch a non-existent bucket: '
                f'{self.name}')

    def _create_bucket(self, bucket_name: str) -> StorageHandle:
        """Create bucket using S3 API."""
        bucket_created = False
        try:
            create_bucket_config: dict[str, Any] = {'Bucket': bucket_name}
            if self.region is not None and self.region != 'us-east-1':
                create_bucket_config['CreateBucketConfiguration'] = {
                    'LocationConstraint': self.region
                }
            self.client.create_bucket(**create_bucket_config)
            bucket_created = True
            logger.info(
                f'  {colorama.Style.DIM}Created S3 bucket {bucket_name!r} in '
                f'{self.region or "us-east-1"}{colorama.Style.RESET_ALL}')

            # Add AWS tags configured in config.yaml to the bucket.
            # This is useful for cost tracking and external cleanup.
            configured_bucket_tags = (
                skypilot_config.get_effective_region_config(
                    cloud=self.config.cloud_name,
                    region=None,
                    keys=('labels',),
                    default_value={}))
            bucket_tags = dict(configured_bucket_tags or {})
            if self.config.cloud_name == str(clouds.AWS()):
                bucket_tags[provision_constants.TAG_SKYPILOT_MANAGED] = (
                    provision_constants.SKYPILOT_MANAGED_TAG_VALUE)
            if bucket_tags:
                self.client.put_bucket_tagging(
                    Bucket=bucket_name,
                    Tagging={
                        'TagSet': [{
                            'Key': k,
                            'Value': v
                        } for k, v in bucket_tags.items()]
                    })
        except aws.botocore_exceptions().ClientError as e:
            if bucket_created:
                try:
                    # S3 does not support tags in CreateBucket. Roll back the
                    # still-empty bucket rather than leave unmarked storage.
                    self.client.delete_bucket(Bucket=bucket_name)
                except aws.botocore_exceptions().ClientError as cleanup_error:
                    logger.warning(
                        f'Failed to clean up untagged S3 bucket {bucket_name!r} '
                        f'after bucket tagging failed: {cleanup_error}')
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketCreateError(
                    f'Attempted to create S3 bucket {self.name} but failed.'
                ) from e
        return self.config.resource_factory(bucket_name)

    def _delete_bucket(self, bucket_name: str) -> bool:
        """Delete bucket using AWS CLI."""
        cmd_parts = [f'aws s3 rb {shlex.quote(f"s3://{bucket_name}")} --force']

        if self.config.aws_profile:
            cmd_parts.append(
                f'--profile {shlex.quote(self.config.aws_profile)}')
        if self.config.get_endpoint_url:
            cmd_parts.append(f'--endpoint-url '
                             f'{shlex.quote(self.config.get_endpoint_url())}')

        remove_command = ' '.join(cmd_parts)

        if self.config.credentials_file:
            remove_command = (
                f'AWS_SHARED_CREDENTIALS_FILE='
                f'{_quote_cli_path(self.config.credentials_file)} '
                f'{remove_command}')
        if self.config.config_file:
            remove_command = 'AWS_CONFIG_FILE=' + \
            f'{_quote_cli_path(self.config.config_file)} {remove_command}'
        return self._execute_remove_command(
            remove_command, bucket_name,
            f'Deleting {self.config.store_type} bucket {bucket_name}',
            (f'Failed to delete {self.config.store_type} bucket '
             f'{bucket_name}.'))

    def _execute_remove_command(self, command: str, bucket_name: str,
                                hint_operating: str, hint_failed: str) -> bool:
        """Execute bucket removal command."""
        try:
            with rich_utils.safe_status(
                    ux_utils.spinner_message(hint_operating)):
                subprocess.check_output(command,
                                        stderr=subprocess.STDOUT,
                                        shell=True)
        except subprocess.CalledProcessError as e:
            if 'NoSuchBucket' in e.output.decode('utf-8'):
                logger.debug(
                    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE.format(
                        bucket_name=bucket_name))
                return False
            else:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketDeleteError(
                        f'{hint_failed}'
                        f'Detailed error: {e.output}')
        return True

    def _delete_sub_path(self) -> None:
        """Remove objects from the sub path in the bucket."""
        assert self._bucket_sub_path is not None, 'bucket_sub_path is not set'
        deleted_by_skypilot = self._delete_bucket_sub_path(
            self.name, self._bucket_sub_path)
        provider = self.config.store_type
        if deleted_by_skypilot:
            msg_str = (f'Removed objects from {provider} bucket '
                       f'{self.name}/{self._bucket_sub_path}.')
        else:
            msg_str = (f'Failed to remove objects from {provider} bucket '
                       f'{self.name}/{self._bucket_sub_path}.')
        logger.info(f'{colorama.Fore.GREEN}{msg_str}{colorama.Style.RESET_ALL}')

    def _delete_bucket_sub_path(self, bucket_name: str, sub_path: str) -> bool:
        """Delete objects in the sub path from the bucket."""
        target_uri = f's3://{bucket_name}/{sub_path}/'
        cmd_parts = [f'aws s3 rm {shlex.quote(target_uri)} --recursive']

        if self.config.aws_profile:
            cmd_parts.append(
                f'--profile {shlex.quote(self.config.aws_profile)}')
        if self.config.get_endpoint_url:
            cmd_parts.append(f'--endpoint-url '
                             f'{shlex.quote(self.config.get_endpoint_url())}')

        remove_command = ' '.join(cmd_parts)

        if self.config.credentials_file:
            remove_command = (
                f'AWS_SHARED_CREDENTIALS_FILE='
                f'{_quote_cli_path(self.config.credentials_file)} '
                f'{remove_command}')
        if self.config.config_file:
            remove_command = 'AWS_CONFIG_FILE=' + \
            f'{_quote_cli_path(self.config.config_file)} {remove_command}'
        return self._execute_remove_command(
            remove_command, bucket_name,
            (f'Removing objects from {self.config.store_type} bucket '
             f'{bucket_name}/{sub_path}'),
            (f'Failed to remove objects from {self.config.store_type} '
             f'bucket {bucket_name}/{sub_path}.'))


@register_s3_compatible_store
class S3Store(S3CompatibleStore):
    """S3Store inherits from S3CompatibleStore and represents the backend
    for S3 buckets.
    """

    _DEFAULT_REGION = 'us-east-1'
    _CUSTOM_ENDPOINT_REGIONS = [
        'ap-east-1', 'me-south-1', 'af-south-1', 'eu-south-1', 'eu-south-2',
        'eu-central-2', 'ap-south-2', 'ap-southeast-3', 'ap-southeast-4',
        'me-central-1', 'il-central-1'
    ]

    def __init__(self,
                 name: str,
                 source: str,
                 region: str | None = None,
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool = True,
                 _bucket_sub_path: str | None = None):
        # TODO(romilb): This is purely a stopgap fix for
        #  https://github.com/skypilot-org/skypilot/issues/3405
        # We should eventually make all opt-in regions also work for S3 by
        # passing the right endpoint flags.
        if region in self._CUSTOM_ENDPOINT_REGIONS:
            logger.warning('AWS opt-in regions are not supported for S3. '
                           f'Falling back to default region '
                           f'{self._DEFAULT_REGION} for bucket {name!r}.')
            region = self._DEFAULT_REGION
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    @classmethod
    def get_config(cls) -> S3CompatibleConfig:
        """Return the configuration for AWS S3."""
        return S3CompatibleConfig(
            store_type='S3',
            url_prefix='s3://',
            client_factory=data_utils.create_s3_client,
            resource_factory=lambda name: aws.resource('s3').Bucket(name),
            split_path=data_utils.split_s3_path,
            verify_bucket=data_utils.verify_s3_bucket,
            cloud_name=str(clouds.AWS()),
            default_region=cls._DEFAULT_REGION,
            mount_cmd_factory=mounting_utils.get_s3_mount_cmd,
        )

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.S3.get_profile_name(self.name))
        rclone_config = data_utils.Rclone.RcloneStores.S3.get_config(
            rclone_profile_name=rclone_profile_name)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.bucket.name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)


@register_s3_compatible_store
class R2Store(S3CompatibleStore):
    """R2Store inherits from S3CompatibleStore and represents the backend
    for R2 buckets.
    """

    def __init__(self,
                 name: str,
                 source: str,
                 region: str | None = 'auto',
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool = True,
                 _bucket_sub_path: str | None = None):
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    @classmethod
    def get_config(cls) -> S3CompatibleConfig:
        """Return the configuration for Cloudflare R2."""
        return S3CompatibleConfig(
            store_type='R2',
            url_prefix='r2://',
            client_factory=lambda region: data_utils.create_r2_client(region or
                                                                      'auto'),
            resource_factory=lambda name: cloudflare.resource('s3').Bucket(name
                                                                          ),
            split_path=data_utils.split_r2_path,
            verify_bucket=data_utils.verify_r2_bucket,
            credentials_file=cloudflare.R2_CREDENTIALS_PATH,
            aws_profile=cloudflare.R2_PROFILE_NAME,
            get_endpoint_url=lambda: cloudflare.create_endpoint(),  # pylint: disable=unnecessary-lambda
            extra_cli_args=['--checksum-algorithm', 'CRC32'],  # R2 specific
            cloud_name=cloudflare.NAME,
            default_region='auto',
            mount_cmd_factory=cls._get_r2_mount_cmd,
        )

    @classmethod
    def _get_r2_mount_cmd(cls,
                          bucket_name: str,
                          mount_path: str,
                          bucket_sub_path: str | None,
                          read_only: bool = False) -> str:
        """Factory method for R2 mount command."""
        endpoint_url = cloudflare.create_endpoint()
        return mounting_utils.get_r2_mount_cmd(cloudflare.R2_CREDENTIALS_PATH,
                                               cloudflare.R2_PROFILE_NAME,
                                               endpoint_url,
                                               bucket_name,
                                               mount_path,
                                               bucket_sub_path,
                                               read_only=read_only)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        """R2-specific cached mount implementation using rclone."""
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.R2.get_profile_name(self.name))
        rclone_config = data_utils.Rclone.RcloneStores.R2.get_config(
            rclone_profile_name=rclone_profile_name)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.bucket.name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)


@register_s3_compatible_store
class NebiusStore(S3CompatibleStore):
    """NebiusStore inherits from S3CompatibleStore and represents the backend
    for Nebius Object Storage buckets.
    """

    @classmethod
    def get_config(cls) -> S3CompatibleConfig:
        """Return the configuration for Nebius Object Storage."""
        return S3CompatibleConfig(
            store_type='NEBIUS',
            url_prefix='nebius://',
            client_factory=lambda region: data_utils.create_nebius_client(),
            resource_factory=lambda name: nebius.resource('s3').Bucket(name),
            split_path=data_utils.split_nebius_path,
            verify_bucket=data_utils.verify_nebius_bucket,
            aws_profile=nebius.NEBIUS_PROFILE_NAME,
            cloud_name=str(clouds.Nebius()),
            mount_cmd_factory=cls._get_nebius_mount_cmd,
        )

    @classmethod
    def _get_nebius_mount_cmd(cls,
                              bucket_name: str,
                              mount_path: str,
                              bucket_sub_path: str | None,
                              read_only: bool = False) -> str:
        """Factory method for Nebius mount command."""
        # We need to get the endpoint URL, but since this is a static method,
        # we'll need to create a client to get it
        client = data_utils.create_nebius_client()
        endpoint_url = client.meta.endpoint_url
        return mounting_utils.get_nebius_mount_cmd(nebius.NEBIUS_PROFILE_NAME,
                                                   bucket_name,
                                                   endpoint_url,
                                                   mount_path,
                                                   bucket_sub_path,
                                                   read_only=read_only)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        """Nebius-specific cached mount implementation using rclone."""
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.NEBIUS.get_profile_name(self.name))
        rclone_config = data_utils.Rclone.RcloneStores.NEBIUS.get_config(
            rclone_profile_name=rclone_profile_name)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.bucket.name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)


@register_s3_compatible_store
class CoreWeaveStore(S3CompatibleStore):
    """CoreWeaveStore inherits from S3CompatibleStore and represents the backend
    for CoreWeave Object Storage buckets.
    """

    @classmethod
    def get_config(cls) -> S3CompatibleConfig:
        """Return the configuration for CoreWeave Object Storage."""
        return S3CompatibleConfig(
            store_type='COREWEAVE',
            url_prefix='cw://',
            client_factory=lambda region: data_utils.create_coreweave_client(),
            resource_factory=lambda name: coreweave.resource('s3').Bucket(name),
            split_path=data_utils.split_coreweave_path,
            verify_bucket=data_utils.verify_coreweave_bucket,
            aws_profile=coreweave.COREWEAVE_PROFILE_NAME,
            get_endpoint_url=coreweave.get_endpoint,
            credentials_file=coreweave.COREWEAVE_CREDENTIALS_PATH,
            config_file=coreweave.COREWEAVE_CONFIG_PATH,
            cloud_name=coreweave.NAME,
            default_region=coreweave.DEFAULT_REGION,
            mount_cmd_factory=cls._get_coreweave_mount_cmd,
        )

    def _get_bucket(self) -> tuple[StorageHandle, bool]:
        """Get or create bucket using CoreWeave's S3 API"""
        bucket = self.config.resource_factory(self.name)

        # Use our custom bucket verification instead of head_bucket
        if data_utils.verify_coreweave_bucket(self.name):
            self._validate_existing_bucket()
            return bucket, False

        # TODO(hailong): Enable the bucket creation for CoreWeave
        # Disable this to avoid waiting too long until the following
        # issue is resolved:
        # https://github.com/skypilot-org/skypilot/issues/7736
        raise exceptions.StorageBucketGetError(
            f'Bucket {self.name!r} does not exist. CoreWeave buckets can take'
            ' a long time to become accessible after creation, so SkyPilot'
            ' does not create them automatically. Please create the bucket'
            ' manually in CoreWeave and wait for it to be accessible before'
            ' using it.')

        # # Check if this is a source with URL prefix (existing bucket case)
        # if isinstance(self.source, str) and self.source.startswith(
        #         self.config.url_prefix):
        #     with ux_utils.print_exception_no_traceback():
        #         raise exceptions.StorageBucketGetError(
        #             'Attempted to use a non-existent bucket as a source: '
        #             f'{self.source}.')

        # # If bucket cannot be found, create it if needed
        # if self.sync_on_reconstruction:
        #     bucket = self._create_bucket(self.name)
        #     return bucket, True
        # else:
        #     raise exceptions.StorageExternalDeletionError(
        #         'Attempted to fetch a non-existent bucket: '
        #         f'{self.name}')

    @classmethod
    def _get_coreweave_mount_cmd(cls,
                                 bucket_name: str,
                                 mount_path: str,
                                 bucket_sub_path: str | None,
                                 read_only: bool = False) -> str:
        """Factory method for CoreWeave mount command."""
        endpoint_url = coreweave.get_endpoint()
        return mounting_utils.get_coreweave_mount_cmd(
            coreweave.COREWEAVE_CREDENTIALS_PATH,
            coreweave.COREWEAVE_PROFILE_NAME,
            bucket_name,
            endpoint_url,
            mount_path,
            bucket_sub_path,
            read_only=read_only)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        """CoreWeave-specific cached mount implementation using rclone."""
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.COREWEAVE.get_profile_name(
                self.name))
        rclone_config = data_utils.Rclone.RcloneStores.COREWEAVE.get_config(
            rclone_profile_name=rclone_profile_name)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.bucket.name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)

    def _create_bucket(self, bucket_name: str) -> StorageHandle:
        """Create bucket using S3 API with timing handling for CoreWeave."""
        result = super()._create_bucket(bucket_name)
        # Ensure bucket is created
        # The newly created bucket ever takes about 18min to be accessible,
        # here we just retry for 36 times (5s * 36 = 180s) to avoid waiting
        # too long
        # TODO(hailong): Update the logic here when the following
        # issue is resolved:
        # https://github.com/skypilot-org/skypilot/issues/7736
        data_utils.verify_coreweave_bucket(bucket_name, retry=36)

        return result


@register_s3_compatible_store
class VastDataStore(S3CompatibleStore):
    """VastDataStore inherits from S3CompatibleStore and represents the backend
    for VastData S3-compatible object storage buckets.

    VastData is a separate company from Vast.ai (compute). This store
    provides storage-only integration with VastData's S3-compatible API.
    """

    @classmethod
    def get_config(cls) -> S3CompatibleConfig:
        """Return the configuration for VastData Object Storage."""
        return S3CompatibleConfig(
            store_type='VASTDATA',
            url_prefix='vastdata://',
            client_factory=lambda region: data_utils.create_vastdata_client(),
            resource_factory=lambda name: vastdata.resource('s3').Bucket(name),
            split_path=data_utils.split_vastdata_path,
            verify_bucket=data_utils.verify_vastdata_bucket,
            aws_profile=vastdata.VASTDATA_PROFILE_NAME,
            get_endpoint_url=vastdata.get_endpoint,
            credentials_file=vastdata.VASTDATA_CREDENTIALS_PATH,
            config_file=vastdata.VASTDATA_CONFIG_PATH,
            cloud_name=vastdata.NAME,
            default_region=vastdata.DEFAULT_REGION,
            mount_cmd_factory=cls._get_vastdata_mount_cmd,
        )

    @classmethod
    def _get_vastdata_mount_cmd(cls,
                                bucket_name: str,
                                mount_path: str,
                                bucket_sub_path: str | None,
                                read_only: bool = False) -> str:
        """Factory method for VastData mount command."""
        endpoint_url = vastdata.get_endpoint()
        return mounting_utils.get_vastdata_mount_cmd(
            vastdata.VASTDATA_CREDENTIALS_PATH,
            vastdata.VASTDATA_PROFILE_NAME,
            bucket_name,
            endpoint_url,
            mount_path,
            bucket_sub_path,
            read_only=read_only)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        """VastData-specific cached mount implementation using rclone."""
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.VASTDATA.get_profile_name(self.name))
        rclone_config = data_utils.Rclone.RcloneStores.VASTDATA.get_config(
            rclone_profile_name=rclone_profile_name)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.bucket.name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)


S3_COMPATIBLE_STORES = _S3_COMPATIBLE_STORES

# Keep public and serialized identities stable through the storage facade.
register_s3_compatible_store.__module__ = storage_lib.__name__
for _public_class in (
        S3CompatibleConfig,
        S3CompatibleStore,
        S3Store,
        R2Store,
        NebiusStore,
        CoreWeaveStore,
        VastDataStore,
):
    _public_class.__module__ = storage_lib.__name__
del _public_class
