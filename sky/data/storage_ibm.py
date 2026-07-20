"""IBM Cloud Object Storage backend implementation."""
import re
import shlex
import typing
from typing import Any

import colorama

from sky import exceptions
from sky import sky_logging
from sky.adaptors import ibm
from sky.data import data_utils
from sky.data import mounting_utils
from sky.data import storage as storage_lib
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from google.cloud import storage

StorageHandle = storage_lib.StorageHandle
Path = storage_lib.Path
AbstractStore = storage_lib.AbstractStore
logger = storage_lib.logger
_MAX_CONCURRENT_UPLOADS = storage_lib.IBM_MAX_CONCURRENT_UPLOADS
_BUCKET_FAIL_TO_CONNECT_MESSAGE = (
    storage_lib.IBM_BUCKET_FAIL_TO_CONNECT_MESSAGE)
_STORAGE_LOG_FILE_NAME = storage_lib.IBM_STORAGE_LOG_FILE_NAME


class IBMCosStore(AbstractStore):
    """IBMCosStore inherits from Storage Object and represents the backend
    for COS buckets.
    """
    _ACCESS_DENIED_MESSAGE = 'Access Denied'

    def __init__(self,
                 name: str,
                 source: str,
                 region: str | None = 'us-east',
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool = True,
                 _bucket_sub_path: str | None = None):
        self.client: storage.Client
        self.bucket: StorageHandle
        self.rclone_profile_name = (
            data_utils.Rclone.RcloneStores.IBM.get_profile_name(self.name))
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    def _validate(self):
        if self.source is not None and isinstance(self.source, str):
            if self.source.startswith('s3://'):
                assert self.name == data_utils.split_s3_path(self.source)[0], (
                    'S3 Bucket is specified as path, the name should be the'
                    ' same as S3 bucket.')
                assert data_utils.verify_s3_bucket(self.name), (
                    f'Source specified as {self.source}, a S3 bucket. ',
                    'S3 Bucket should exist.')
            elif self.source.startswith('gs://'):
                assert self.name == data_utils.split_gcs_path(self.source)[0], (
                    'GCS Bucket is specified as path, the name should be '
                    'the same as GCS bucket.')
                assert data_utils.verify_gcs_bucket(self.name), (
                    f'Source specified as {self.source}, a GCS bucket. ',
                    'GCS Bucket should exist.')
            elif data_utils.is_az_container_endpoint(self.source):
                storage_account_name, container_name, _ = (
                    data_utils.split_az_path(self.source))
                assert self.name == container_name, (
                    'Azure bucket is specified as path, the name should be '
                    'the same as Azure bucket.')
                assert data_utils.verify_az_bucket(
                    storage_account_name, self.name), (
                        f'Source specified as {self.source}, an Azure bucket. '
                        'Azure bucket should exist.')
            elif self.source.startswith('r2://'):
                assert self.name == data_utils.split_r2_path(self.source)[0], (
                    'R2 Bucket is specified as path, the name should be '
                    'the same as R2 bucket.')
                assert data_utils.verify_r2_bucket(self.name), (
                    f'Source specified as {self.source}, a R2 bucket. ',
                    'R2 Bucket should exist.')
            elif self.source.startswith('nebius://'):
                assert self.name == data_utils.split_nebius_path(
                    self.source)[0], (
                        'Nebius Object Storage is specified as path, the name '
                        'should be the same as Nebius Object Storage bucket.')
                assert data_utils.verify_nebius_bucket(self.name), (
                    f'Source specified as {self.source}, a Nebius Object '
                    f'Storage  bucket. Nebius Object Storage Bucket should '
                    f'exist.')
            elif self.source.startswith('cos://'):
                assert self.name == data_utils.split_cos_path(self.source)[0], (
                    'COS Bucket is specified as path, the name should be '
                    'the same as COS bucket.')
            elif self.source.startswith('cw://'):
                raise NotImplementedError(
                    'Moving data from CoreWeave Object Storage to COS is '
                    'currently not supported.')
        # Validate name
        self.name = IBMCosStore.validate_name(self.name)

    @classmethod
    def validate_name(cls, name: str) -> str:
        """Validates the name of a COS bucket.

        Rules source: https://ibm.github.io/ibm-cos-sdk-java/com/ibm/cloud/objectstorage/services/s3/model/Bucket.html  # pylint: disable=line-too-long
        """

        def _raise_no_traceback_name_error(err_str):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageNameError(err_str)

        if name is not None and isinstance(name, str):
            if not 3 <= len(name) <= 63:
                _raise_no_traceback_name_error(
                    f'Invalid store name: {name} must be between 3 (min) '
                    'and 63 (max) characters long.')

            # Check for valid characters and start/end with a letter or number
            pattern = r'^[a-z0-9][-a-z0-9.]*[a-z0-9]$'
            if not re.match(pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: {name} can consist only of '
                    'lowercase letters, numbers, dots (.), and dashes (-). '
                    'It must begin and end with a letter or number.')

            # Check for two adjacent periods or dashes
            if any(substring in name for substring in ['..', '--']):
                _raise_no_traceback_name_error(
                    f'Invalid store name: {name} must not contain '
                    'two adjacent periods/dashes')

            # Check for IP address format
            ip_pattern = r'^(?:\d{1,3}\.){3}\d{1,3}$'
            if re.match(ip_pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: {name} must not be formatted as '
                    'an IP address (for example, 192.168.5.4).')

            if any(substring in name for substring in ['.-', '-.']):
                _raise_no_traceback_name_error(
                    f'Invalid store name: {name} must '
                    'not allow substrings: ".-", "-." .')
        else:
            _raise_no_traceback_name_error('Store name must be specified.')
        return name

    def initialize(self):
        """Initializes the cos store object on the cloud.

        Initialization involves fetching bucket if exists, or creating it if
        it does not.

        Raises:
          StorageBucketCreateError: If bucket creation fails
          StorageBucketGetError: If fetching existing bucket fails
          StorageInitError: If general initialization fails.
        """
        if self.region is None:
            raise exceptions.StorageInitError(
                'Region must be specified for IBM COS store.')
        self.client = ibm.get_cos_client(self.region)
        self.s3_resource = ibm.get_cos_resource(self.region)
        self.bucket, is_new_bucket = self._get_bucket()
        if self.is_sky_managed is None:
            # If is_sky_managed is not specified, then this is a new storage
            # object (i.e., did not exist in global_user_state) and we should
            # set the is_sky_managed property.
            # If is_sky_managed is specified, then we take no action.
            self.is_sky_managed = is_new_bucket

    def upload(self):
        """Uploads files from local machine to bucket.

        Upload must be called by the Storage handler - it is not called on
        Store initialization.

        Raises:
            StorageUploadError: if upload fails.
        """
        try:
            if isinstance(self.source, list):
                self.batch_ibm_rsync(self.source, create_dirs=True)
            elif self.source is not None:
                if self.source.startswith('cos://'):
                    # cos bucket used as a dest, can't be used as source.
                    pass
                elif self.source.startswith('s3://'):
                    raise Exception('IBM COS currently not supporting'
                                    'data transfers between COS and S3')
                elif self.source.startswith('nebius://'):
                    raise Exception('IBM COS currently not supporting'
                                    'data transfers between COS and Nebius')
                elif self.source.startswith('gs://'):
                    raise Exception('IBM COS currently not supporting'
                                    'data transfers between COS and GS')
                elif self.source.startswith('r2://'):
                    raise Exception('IBM COS currently not supporting'
                                    'data transfers between COS and r2')
                elif self.source.startswith('cw://'):
                    raise Exception('IBM COS currently not supporting'
                                    'data transfers between COS and CoreWeave')
                else:
                    self.batch_ibm_rsync([self.source])

        except Exception as e:
            raise exceptions.StorageUploadError(
                f'Upload failed for store {self.name}') from e

    def delete(self) -> None:
        if self._bucket_sub_path is not None and not self.is_sky_managed:
            return self._delete_sub_path()

        self._delete_cos_bucket()
        logger.info(f'{colorama.Fore.GREEN}Deleted COS bucket {self.name}.'
                    f'{colorama.Style.RESET_ALL}')

    def _delete_sub_path(self) -> None:
        assert self._bucket_sub_path is not None, 'bucket_sub_path is not set'
        bucket = self.s3_resource.Bucket(self.name)
        try:
            self._delete_cos_bucket_objects(bucket, self._bucket_sub_path + '/')
        except ibm.ibm_botocore.exceptions.ClientError as e:
            if e.__class__.__name__ == 'NoSuchBucket':
                logger.debug('bucket already removed')

    def get_handle(self) -> StorageHandle:
        return self.s3_resource.Bucket(self.name)

    def batch_ibm_rsync(self,
                        source_path_list: list[Path],
                        create_dirs: bool = False) -> None:
        """Invokes rclone copy to batch upload a list of local paths to cos

        Since rclone does not support batch operations, we construct
        multiple commands to be run in parallel.

        Args:
            source_path_list: List of paths to local files or directories
            create_dirs: If the local_path is a directory and this is set to
                False, the contents of the directory are directly uploaded to
                root of the bucket. If the local_path is a directory and this is
                set to True, the directory is created in the bucket root and
                contents are uploaded to it.
        """
        sub_path = (f'/{self._bucket_sub_path}'
                    if self._bucket_sub_path else '')
        remote_prefix = f'{self.rclone_profile_name}:{self.name}{sub_path}'

        def get_dir_sync_command(src_dir_path, dest_dir_name) -> str:
            """returns an rclone command that copies a complete folder
              from 'src_dir_path' to bucket/'dest_dir_name'.

            `rclone copy` copies files from source path to target.
            files with identical names at won't be copied over, unless
            their modification date is more recent.
            works similarly to `aws sync` (without --delete).

            Args:
                src_dir_path (str): local source path from which to copy files.
                dest_dir_name (str): remote target path files are copied to.

            Returns:
                str: bash command using rclone to sync files. Executed remotely.
            """

            # .git directory is excluded from the sync
            # wrapping src_dir_path with "" to support path with spaces
            remote_path = f'{remote_prefix}/{dest_dir_name}'
            sync_command = ('rclone copy --exclude ".git/*" '
                            f'{shlex.quote(src_dir_path)} '
                            f'{shlex.quote(remote_path)}')
            return sync_command

        def get_file_sync_command(base_dir_path, file_names) -> str:
            """returns an rclone command that copies files: 'file_names'
               from base directory: `base_dir_path` to bucket.

            `rclone copy` copies files from source path to target.
            files with identical names at won't be copied over, unless
            their modification date is more recent.
            works similarly to `aws sync` (without --delete).

            Args:
                base_dir_path (str): local path from which to copy files.
                file_names (List): specific file names to copy.

            Returns:
                str: bash command using rclone to sync files
            """

            # wrapping file_name with "" to support spaces
            includes = ' '.join([
                f'--include {shlex.quote(file_name)}'
                for file_name in file_names
            ])
            sync_command = ('rclone copy '
                            f'{includes} {shlex.quote(base_dir_path)} '
                            f'{shlex.quote(remote_prefix)}')
            return sync_command

        # Generate message for upload
        if len(source_path_list) > 1:
            source_message = f'{len(source_path_list)} paths'
        else:
            source_message = source_path_list[0]

        log_path = sky_logging.generate_tmp_logging_file_path(
            _STORAGE_LOG_FILE_NAME)
        sync_path = (
            f'{source_message} -> cos://{self.region}/{self.name}{sub_path}/')
        with rich_utils.safe_status(
                ux_utils.spinner_message(f'Syncing {sync_path}',
                                         log_path=log_path)):
            data_utils.parallel_upload(
                source_path_list,
                get_file_sync_command,
                get_dir_sync_command,
                log_path,
                self.name,
                self._ACCESS_DENIED_MESSAGE,
                create_dirs=create_dirs,
                max_concurrent_uploads=_MAX_CONCURRENT_UPLOADS)
        logger.info(
            ux_utils.finishing_message(f'Storage synced: {sync_path}',
                                       log_path))

    def _get_bucket(self) -> tuple[StorageHandle, bool]:
        """returns IBM COS bucket object if exists, otherwise creates it.

        Returns:
          StorageHandle(str): bucket name
          bool: indicates whether a new bucket was created.

        Raises:
            StorageSpecError: If externally created bucket is attempted to be
                mounted without specifying storage source.
            StorageBucketCreateError: If bucket creation fails.
            StorageBucketGetError: If fetching a bucket fails
            StorageExternalDeletionError: If externally deleted storage is
                attempted to be fetched while reconstructing the storage for
                'sky storage delete' or 'sky start'
        """

        bucket_profile_name = (data_utils.Rclone.RcloneStores.IBM.value +
                               self.name)
        try:
            bucket_region = data_utils.get_ibm_cos_bucket_region(self.name)
        except exceptions.StorageBucketGetError as e:
            with ux_utils.print_exception_no_traceback():
                command = f'rclone lsd {bucket_profile_name}: '
                raise exceptions.StorageBucketGetError(
                    _BUCKET_FAIL_TO_CONNECT_MESSAGE.format(name=self.name) +
                    f' To debug, consider running `{command}`.') from e

        try:
            uri_region = data_utils.split_cos_path(
                self.source)[2]  # type: ignore
        except ValueError:
            # source isn't a cos uri
            uri_region = ''

        # bucket's region doesn't match specified region in URI
        if bucket_region and uri_region and uri_region != bucket_region\
              and self.sync_on_reconstruction:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketGetError(
                    f'Bucket {self.name} exists in '
                    f'region {bucket_region}, '
                    f'but URI specified region {uri_region}.')

        if not bucket_region and uri_region:
            # bucket doesn't exist but source is a bucket URI
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketGetError(
                    'Attempted to use a non-existent bucket as a source: '
                    f'{self.name} by providing URI. Consider using '
                    '`rclone lsd <remote>` on relevant remotes returned '
                    'via `rclone listremotes` to debug.')

        data_utils.Rclone.store_rclone_config(
            self.name,
            data_utils.Rclone.RcloneStores.IBM,
            self.region,  # type: ignore
        )

        if not bucket_region and self.sync_on_reconstruction:
            # bucket doesn't exist
            return self._create_cos_bucket(self.name, self.region), True
        elif not bucket_region and not self.sync_on_reconstruction:
            # Raised when Storage object is reconstructed for sky storage
            # delete or to re-mount Storages with sky start but the storage
            # is already removed externally.
            raise exceptions.StorageExternalDeletionError(
                'Attempted to fetch a non-existent bucket: '
                f'{self.name}')
        else:
            # bucket exists
            bucket = self.s3_resource.Bucket(self.name)
            self._validate_existing_bucket()
            return bucket, False

    def _download_file(self, remote_path: str, local_path: str) -> None:
        """Downloads file from remote to local on s3 bucket
        using the boto3 API

        Args:
          remote_path: str; Remote path on S3 bucket
          local_path: str; Local path on user's device
        """
        self.client.download_file(self.name, local_path, remote_path)

    def mount_command(self, mount_path: str, read_only: bool = False) -> str:
        """Returns the command to mount the bucket to the mount_path.

        Uses rclone to mount the bucket.
        Source: https://github.com/rclone/rclone

        Args:
          mount_path: str; Path to mount the bucket to.
          read_only: bool; Whether to mount as read-only.
        """
        # install rclone if not installed.
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_config = data_utils.Rclone.RcloneStores.IBM.get_config(
            rclone_profile_name=self.rclone_profile_name, region=self.region)
        mount_cmd = (mounting_utils.get_cos_mount_cmd(
            rclone_config,
            self.rclone_profile_name,
            self.bucket.name,
            mount_path,
            self._bucket_sub_path,
            read_only=read_only,
        ))
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cmd)

    def _create_cos_bucket(self,
                           bucket_name: str,
                           region='us-east') -> StorageHandle:
        """Creates IBM COS bucket with specific name in specific region

        Args:
          bucket_name: str; Name of bucket
          region: str; Region name, e.g. us-east, us-south
        Raises:
          StorageBucketCreateError: If bucket creation fails.
        """
        try:
            self.client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': f'{region}-smart'
                })
            logger.info(f'  {colorama.Style.DIM}Created IBM COS bucket '
                        f'{bucket_name!r} in {region} '
                        'with storage class smart tier'
                        f'{colorama.Style.RESET_ALL}')
            self.bucket = self.s3_resource.Bucket(bucket_name)

        except ibm.ibm_botocore.exceptions.ClientError as e:  # pylint: disable=line-too-long
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketCreateError(
                    f'Failed to create bucket: '
                    f'{bucket_name}') from e

        s3_bucket_exists_waiter = self.client.get_waiter('bucket_exists')
        s3_bucket_exists_waiter.wait(Bucket=bucket_name)

        return self.bucket

    def _delete_cos_bucket_objects(self,
                                   bucket: Any,
                                   prefix: str | None = None) -> None:
        bucket_versioning = self.s3_resource.BucketVersioning(bucket.name)
        if bucket_versioning.status == 'Enabled':
            if prefix is not None:
                res = list(
                    bucket.object_versions.filter(Prefix=prefix).delete())
            else:
                res = list(bucket.object_versions.delete())
        else:
            if prefix is not None:
                res = list(bucket.objects.filter(Prefix=prefix).delete())
            else:
                res = list(bucket.objects.delete())
        logger.debug(f'Deleted bucket\'s content:\n{res}, prefix: {prefix}')

    def _delete_cos_bucket(self) -> None:
        bucket = self.s3_resource.Bucket(self.name)
        try:
            self._delete_cos_bucket_objects(bucket)
            bucket.delete()
            bucket.wait_until_not_exists()
        except ibm.ibm_botocore.exceptions.ClientError as e:
            if e.__class__.__name__ == 'NoSuchBucket':
                logger.debug('bucket already removed')
        data_utils.Rclone.delete_rclone_bucket_profile(
            self.name, data_utils.Rclone.RcloneStores.IBM)


# Preserve the historical public and pickle identity while storage.py remains
# the stable facade for the extracted backend.
IBMCosStore.__module__ = storage_lib.__name__
