"""Google Cloud Storage backend implementation."""
from collections.abc import Callable
import logging
import os
import re
import shlex
import subprocess
import typing

import colorama

from sky import clouds
from sky import exceptions
from sky import sky_logging
from sky.adaptors import gcp
from sky.data import data_transfer
from sky.data import data_utils
from sky.data import mounting_utils
from sky.data import storage as storage_lib
from sky.data import storage_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from google.cloud import storage

StorageHandle = storage_lib.StorageHandle
Path = storage_lib.Path
AbstractStore = storage_lib.AbstractStore
MountCachedConfig = storage_lib.MountCachedConfig
logger: logging.Logger = storage_lib.logger
_is_storage_cloud_enabled: Callable[..., bool] = (
    storage_lib.gcs_storage_cloud_enabled)
_MAX_CONCURRENT_UPLOADS: int = storage_lib.GCS_MAX_CONCURRENT_UPLOADS
_BUCKET_FAIL_TO_CONNECT_MESSAGE: str = (
    storage_lib.GCS_BUCKET_FAIL_TO_CONNECT_MESSAGE)
_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE: str = (
    storage_lib.GCS_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
_STORAGE_LOG_FILE_NAME: str = storage_lib.GCS_STORAGE_LOG_FILE_NAME


class GcsStore(AbstractStore):
    """GcsStore inherits from Storage Object and represents the backend
    for GCS buckets.
    """

    _ACCESS_DENIED_MESSAGE = 'AccessDeniedException'

    def __init__(self,
                 name: str,
                 source: str,
                 region: str | None = 'us-central1',
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool | None = True,
                 _bucket_sub_path: str | None = None):
        self.client: storage.Client
        self.bucket: StorageHandle
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    def _validate(self):
        if self.source is not None and isinstance(self.source, str):
            if self.source.startswith('s3://'):
                assert self.name == data_utils.split_s3_path(self.source)[0], (
                    'S3 Bucket is specified as path, the name should be the'
                    ' same as S3 bucket.')
                assert data_utils.verify_s3_bucket(self.name), (
                    f'Source specified as {self.source}, an S3 bucket. ',
                    'S3 Bucket should exist.')
            elif self.source.startswith('gs://'):
                assert self.name == data_utils.split_gcs_path(self.source)[0], (
                    'GCS Bucket is specified as path, the name should be '
                    'the same as GCS bucket.')
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
                        'should be the same as R2 bucket.')
                assert data_utils.verify_nebius_bucket(self.name), (
                    f'Source specified as {self.source}, a Nebius Object '
                    f'Storage bucket. Nebius Object Storage Bucket should '
                    f'exist.')
            elif self.source.startswith('cos://'):
                assert self.name == data_utils.split_cos_path(self.source)[0], (
                    'COS Bucket is specified as path, the name should be '
                    'the same as COS bucket.')
                assert data_utils.verify_ibm_cos_bucket(self.name), (
                    f'Source specified as {self.source}, a COS bucket. ',
                    'COS Bucket should exist.')
            elif self.source.startswith('oci://'):
                raise NotImplementedError(
                    'Moving data from OCI to GCS is currently not supported.')
            elif self.source.startswith('cw://'):
                raise NotImplementedError(
                    'Moving data from CoreWeave Object Storage to GCS is'
                    ' currently not supported.')
        # Validate name
        self.name = self.validate_name(self.name)
        # Check if the storage is enabled
        if not _is_storage_cloud_enabled(str(clouds.GCP())):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesUnavailableError(
                    'Storage \'store: gcs\' specified, but '
                    'GCP access is disabled. To fix, enable '
                    'GCP by running `sky check`. '
                    'More info: https://docs.skypilot.co/en/latest/getting-started/installation.html.')  # pylint: disable=line-too-long

    @classmethod
    def validate_name(cls, name: str) -> str:
        """Validates the name of the GCS store.

        Source for rules: https://cloud.google.com/storage/docs/buckets#naming
        """

        def _raise_no_traceback_name_error(err_str):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageNameError(err_str)

        if name is not None and isinstance(name, str):
            # Check for overall length
            if not 3 <= len(name) <= 222:
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must contain 3-222 '
                    'characters.')

            # Check for valid characters and start/end with a number or letter
            pattern = r'^[a-z0-9][-a-z0-9._]*[a-z0-9]$'
            if not re.match(pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} can only contain '
                    'lowercase letters, numeric characters, dashes (-), '
                    'underscores (_), and dots (.). Spaces are not allowed. '
                    'Names must start and end with a number or letter.')

            # Check for 'goog' prefix and 'google' in the name
            if name.startswith('goog') or any(
                    s in name
                    for s in ['google', 'g00gle', 'go0gle', 'g0ogle']):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} cannot begin with the '
                    '"goog" prefix or contain "google" in various forms.')

            # Check for dot-separated components length
            components = name.split('.')
            if any(len(component) > 63 for component in components):
                _raise_no_traceback_name_error(
                    'Invalid store name: Dot-separated components in name '
                    f'{name} can be no longer than 63 characters.')

            if '..' in name or '.-' in name or '-.' in name:
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must not contain two '
                    'adjacent periods or a dot next to a hyphen.')

            # Check for IP address format
            ip_pattern = r'^(?:\d{1,3}\.){3}\d{1,3}$'
            if re.match(ip_pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} cannot be represented as '
                    'an IP address in dotted-decimal notation '
                    '(for example, 192.168.5.4).')
        else:
            _raise_no_traceback_name_error('Store name must be specified.')
        return name

    def initialize(self):
        """Initializes the GCS store object on the cloud.

        Initialization involves fetching bucket if exists, or creating it if
        it does not.

        Raises:
          StorageBucketCreateError: If bucket creation fails
          StorageBucketGetError: If fetching existing bucket fails
          StorageInitError: If general initialization fails.
        """
        self.client = gcp.storage_client()
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
                self.batch_gsutil_rsync(self.source, create_dirs=True)
            elif self.source is not None:
                if self.source.startswith('gs://'):
                    pass
                elif self.source.startswith('s3://'):
                    self._transfer_to_gcs()
                elif self.source.startswith('r2://'):
                    self._transfer_to_gcs()
                elif self.source.startswith('oci://'):
                    self._transfer_to_gcs()
                else:
                    # If a single directory is specified in source, upload
                    # contents to root of bucket by suffixing /*.
                    self.batch_gsutil_rsync([self.source])
        except exceptions.StorageUploadError:
            raise
        except Exception as e:
            raise exceptions.StorageUploadError(
                f'Upload failed for store {self.name}') from e

    def delete(self) -> None:
        if self._bucket_sub_path is not None and not self.is_sky_managed:
            return self._delete_sub_path()

        deleted_by_skypilot = self._delete_gcs_bucket(self.name)
        if deleted_by_skypilot:
            msg_str = f'Deleted GCS bucket {self.name}.'
        else:
            msg_str = f'GCS bucket {self.name} may have been deleted ' \
                      f'externally. Removing from local state.'
        logger.info(f'{colorama.Fore.GREEN}{msg_str}'
                    f'{colorama.Style.RESET_ALL}')

    def _delete_sub_path(self) -> None:
        assert self._bucket_sub_path is not None, 'bucket_sub_path is not set'
        deleted_by_skypilot = self._delete_gcs_bucket(self.name,
                                                      self._bucket_sub_path)
        if deleted_by_skypilot:
            msg_str = f'Deleted objects in GCS bucket ' \
                      f'{self.name}/{self._bucket_sub_path}.'
        else:
            msg_str = f'GCS bucket {self.name} may have ' \
                      'been deleted externally.'
        logger.info(f'{colorama.Fore.GREEN}{msg_str}'
                    f'{colorama.Style.RESET_ALL}')

    def get_handle(self) -> StorageHandle:
        return self.client.get_bucket(self.name)

    def batch_gsutil_cp(self,
                        source_path_list: list[Path],
                        create_dirs: bool = False) -> None:
        """Invokes gsutil cp -n to batch upload a list of local paths

        -n flag to gsutil cp checks the existence of an object before uploading,
        making it similar to gsutil rsync. Since it allows specification of a
        list of files, it is faster than calling gsutil rsync on each file.
        However, unlike rsync, files are compared based on just their filename,
        and any updates to a file would not be copied to the bucket.
        """
        # Generate message for upload
        if len(source_path_list) > 1:
            source_message = f'{len(source_path_list)} paths'
        else:
            source_message = source_path_list[0]

        # If the source_path list contains a directory, then gsutil cp -n
        # copies the dir as is to the root of the bucket. To copy the
        # contents of directory to the root, add /* to the directory path
        # e.g., /mydir/*
        source_path_list = [
            str(path) + '/*' if
            (os.path.isdir(path) and not create_dirs) else str(path)
            for path in source_path_list
        ]
        copy_list = '\n'.join(
            os.path.abspath(os.path.expanduser(p)) for p in source_path_list)
        gsutil_alias, alias_gen = data_utils.get_gsutil_command()
        sub_path = (f'/{self._bucket_sub_path}'
                    if self._bucket_sub_path else '')
        sync_command = (f'{alias_gen}; echo "{copy_list}" | {gsutil_alias} '
                        f'cp -e -n -r -I gs://{self.name}{sub_path}')
        log_path = sky_logging.generate_tmp_logging_file_path(
            _STORAGE_LOG_FILE_NAME)
        sync_path = f'{source_message} -> gs://{self.name}{sub_path}/'
        with rich_utils.safe_status(
                ux_utils.spinner_message(f'Syncing {sync_path}',
                                         log_path=log_path)):
            data_utils.run_upload_cli(sync_command,
                                      self._ACCESS_DENIED_MESSAGE,
                                      bucket_name=self.name,
                                      log_path=log_path)
        logger.info(
            ux_utils.finishing_message(f'Storage synced: {sync_path}',
                                       log_path))

    def batch_gsutil_rsync(self,
                           source_path_list: list[Path],
                           create_dirs: bool = False) -> None:
        """Invokes gsutil rsync to batch upload a list of local paths

        Since gsutil rsync does not support include commands, We use negative
        look-ahead regex to exclude everything else than the path(s) we want to
        upload.

        Since gsutil rsync does not support batch operations, we construct
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

        def get_file_sync_command(base_dir_path, file_names):
            sync_format = '|'.join(file_names)
            gsutil_alias, alias_gen = data_utils.get_gsutil_command()
            base_dir_path = shlex.quote(base_dir_path)
            sync_command = (f'{alias_gen}; {gsutil_alias} '
                            f'rsync -e -x \'^(?!{sync_format}$).*\' '
                            f'{base_dir_path} gs://{self.name}{sub_path}')
            return sync_command

        def get_dir_sync_command(src_dir_path, dest_dir_name):
            excluded_list = storage_utils.get_excluded_files(src_dir_path)
            # we exclude .git directory from the sync
            excluded_list.append(r'^\.git/.*$')
            excludes = '|'.join(excluded_list)
            gsutil_alias, alias_gen = data_utils.get_gsutil_command()
            src_dir_path = shlex.quote(src_dir_path)
            sync_command = (f'{alias_gen}; {gsutil_alias} '
                            f'rsync -e -r -x \'({excludes})\' {src_dir_path} '
                            f'gs://{self.name}{sub_path}/{dest_dir_name}')
            return sync_command

        # Generate message for upload
        if len(source_path_list) > 1:
            source_message = f'{len(source_path_list)} paths'
        else:
            source_message = source_path_list[0]

        log_path = sky_logging.generate_tmp_logging_file_path(
            _STORAGE_LOG_FILE_NAME)
        sync_path = f'{source_message} -> gs://{self.name}{sub_path}/'
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

    def _transfer_to_gcs(self) -> None:
        if isinstance(self.source, str) and self.source.startswith('s3://'):
            data_transfer.s3_to_gcs(self.name, self.name)
        elif isinstance(self.source, str) and self.source.startswith('r2://'):
            data_transfer.r2_to_gcs(self.name, self.name)

    def _get_bucket(self) -> tuple[StorageHandle, bool]:
        """Obtains the GCS bucket.
        If the bucket exists, this method will connect to the bucket.

        If the bucket does not exist, there are three cases:
          1) Raise an error if the bucket source starts with gs://
          2) Return None if bucket has been externally deleted and
             sync_on_reconstruction is False
          3) Create and return a new bucket otherwise

        Raises:
            StorageSpecError: If externally created bucket is attempted to be
                mounted without specifying storage source.
            StorageBucketCreateError: If creating the bucket fails
            StorageBucketGetError: If fetching a bucket fails
            StorageExternalDeletionError: If externally deleted storage is
                attempted to be fetched while reconstructing the storage for
                'sky storage delete' or 'sky start'
        """
        try:
            bucket = self.client.get_bucket(self.name)
            self._validate_existing_bucket()
            return bucket, False
        except gcp.not_found_exception() as e:
            if isinstance(self.source, str) and self.source.startswith('gs://'):
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        'Attempted to use a non-existent bucket as a source: '
                        f'{self.source}') from e
            else:
                # If bucket cannot be found (i.e., does not exist), it is to be
                # created by Sky. However, creation is skipped if Store object
                # is being reconstructed for deletion or re-mount with
                # sky start, and error is raised instead.
                if self.sync_on_reconstruction:
                    bucket = self._create_gcs_bucket(self.name, self.region)
                    return bucket, True
                else:
                    # This is raised when Storage object is reconstructed for
                    # sky storage delete or to re-mount Storages with sky start
                    # but the storage is already removed externally.
                    raise exceptions.StorageExternalDeletionError(
                        'Attempted to fetch a non-existent bucket: '
                        f'{self.name}') from e
        except gcp.forbidden_exception():
            # Try public bucket to see if bucket exists
            logger.info(
                'External Bucket detected; Connecting to external bucket...')
            try:
                a_client = gcp.anonymous_storage_client()
                bucket = a_client.bucket(self.name)
                # Check if bucket can be listed/read from
                next(bucket.list_blobs())
                return bucket, False
            except (gcp.not_found_exception(), ValueError) as e:
                command = f'gsutil ls gs://{self.name}'
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        _BUCKET_FAIL_TO_CONNECT_MESSAGE.format(name=self.name) +
                        f' To debug, consider running `{command}`.') from e

    def mount_command(self, mount_path: str, read_only: bool = False) -> str:
        """Returns the command to mount the bucket to the mount_path.

        Uses gcsfuse to mount the bucket.

        Args:
          mount_path: str; Path to mount the bucket to.
          read_only: bool; Whether to mount as read-only.
        """
        install_cmd = mounting_utils.get_gcs_mount_install_cmd()
        mount_cmd = mounting_utils.get_gcs_mount_cmd(self.bucket.name,
                                                     mount_path,
                                                     self._bucket_sub_path,
                                                     read_only=read_only)
        version_check_cmd = (
            f'gcsfuse --version | grep -q {mounting_utils.GCSFUSE_VERSION}')
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cmd, version_check_cmd)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.GCS.get_profile_name(self.name))
        rclone_config = data_utils.Rclone.RcloneStores.GCS.get_config(
            rclone_profile_name=rclone_profile_name)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.bucket.name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)

    def _download_file(self, remote_path: str, local_path: str) -> None:
        """Downloads file from remote to local on GS bucket

        Args:
          remote_path: str; Remote path on GS bucket
          local_path: str; Local path on user's device
        """
        blob = self.bucket.blob(remote_path)
        blob.download_to_filename(local_path, timeout=None)

    def _create_gcs_bucket(self,
                           bucket_name: str,
                           region='us-central1') -> StorageHandle:
        """Creates GCS bucket with specific name in specific region

        Args:
          bucket_name: str; Name of bucket
          region: str; Region name, e.g. us-central1, us-west1
        """
        try:
            bucket = self.client.bucket(bucket_name)
            bucket.storage_class = 'STANDARD'
            new_bucket = self.client.create_bucket(bucket, location=region)
        except Exception as e:  # pylint: disable=broad-except
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketCreateError(
                    f'Attempted to create GCS bucket {self.name} but failed.'
                ) from e
        logger.info(
            f'  {colorama.Style.DIM}Created GCS bucket {new_bucket.name!r} in '
            f'{new_bucket.location} with storage class '
            f'{new_bucket.storage_class}{colorama.Style.RESET_ALL}')
        return new_bucket

    def _delete_gcs_bucket(
        self,
        bucket_name: str,
        # pylint: disable=invalid-name
        _bucket_sub_path: str | None = None) -> bool:
        """Deletes objects in GCS bucket

        Args:
          bucket_name: str; Name of bucket
          _bucket_sub_path: str; Sub path in the bucket, if provided only
            objects in the sub path will be deleted, else the whole bucket will
            be deleted

        Returns:
         bool; True if bucket was deleted, False if it was deleted externally.

        Raises:
            StorageBucketDeleteError: If deleting the bucket fails.
            PermissionError: If the bucket is external and the user is not
                allowed to delete it.
        """
        if _bucket_sub_path is not None:
            command_suffix = f'/{_bucket_sub_path}'
            hint_text = 'objects in '
        else:
            command_suffix = ''
            hint_text = ''
        with rich_utils.safe_status(
                ux_utils.spinner_message(
                    f'Deleting {hint_text}GCS bucket '
                    f'[green]{bucket_name}{command_suffix}[/]')):
            try:
                self.client.get_bucket(bucket_name)
            except gcp.forbidden_exception() as e:
                # Try public bucket to see if bucket exists
                with ux_utils.print_exception_no_traceback():
                    raise PermissionError(
                        'External Bucket detected. User not allowed to delete '
                        'external bucket.') from e
            except gcp.not_found_exception():
                # If bucket does not exist, it may have been deleted externally.
                # Do a no-op in that case.
                logger.debug(
                    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE.format(
                        bucket_name=bucket_name))
                return False
            try:
                gsutil_alias, alias_gen = data_utils.get_gsutil_command()
                remove_obj_command = (
                    f'{alias_gen};{gsutil_alias} '
                    f'rm -r gs://{bucket_name}{command_suffix}')
                subprocess.check_output(remove_obj_command,
                                        stderr=subprocess.STDOUT,
                                        shell=True,
                                        executable='/bin/bash')
                return True
            except subprocess.CalledProcessError as e:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketDeleteError(
                        f'Failed to delete {hint_text}GCS bucket '
                        f'{bucket_name}{command_suffix}.'
                        f'Detailed error: {e.output}')


# Preserve the historical public and pickle identity while storage.py remains
# the stable facade for the extracted backend.
GcsStore.__module__ = storage_lib.__name__
