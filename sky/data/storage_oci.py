"""OCI storage backend implementations."""
from collections.abc import Callable
import logging
import os
import re
import shlex
import subprocess
from typing import Any

import colorama

from sky import clouds
from sky import exceptions
from sky import sky_logging
from sky.adaptors import oci
from sky.adaptors import oci_s3
from sky.data import data_utils
from sky.data import mounting_utils
from sky.data import storage as storage_lib
from sky.data import storage_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

SourceType = storage_lib.SourceType
StorageHandle = storage_lib.StorageHandle
Path = storage_lib.Path
AbstractStore = storage_lib.AbstractStore
MountCachedConfig = storage_lib.MountCachedConfig
S3CompatibleConfig = storage_lib.S3CompatibleConfig
S3CompatibleStore = storage_lib.S3CompatibleStore
logger: logging.Logger = storage_lib.logger
_is_storage_cloud_enabled: Callable[..., bool] = (
    storage_lib.oci_storage_cloud_enabled)
_BUCKET_FAIL_TO_CONNECT_MESSAGE: str = (
    storage_lib.OCI_BUCKET_FAIL_TO_CONNECT_MESSAGE)
_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE: str = (
    storage_lib.OCI_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
_STORAGE_LOG_FILE_NAME: str = storage_lib.OCI_STORAGE_LOG_FILE_NAME


class OciStore(AbstractStore):
    """OciStore inherits from Storage Object and represents the backend
    for OCI buckets.
    """

    _ACCESS_DENIED_MESSAGE = 'AccessDeniedException'

    def __init__(self,
                 name: str,
                 source: SourceType | None,
                 region: str | None = None,
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool | None = True,
                 _bucket_sub_path: str | None = None):
        self.client: Any
        self.bucket: StorageHandle
        self.oci_config_file: str
        self.config_profile: str
        self.compartment: str
        self.namespace: str

        # Region is from the specified name in <bucket>@<region> format.
        # Another case is name can also be set by the source, for example:
        #   /datasets-storage:
        #       source: oci://RAGData@us-sanjose-1
        # The name in above mount will be set to RAGData@us-sanjose-1
        region_in_name = None
        if name is not None and '@' in name:
            self._validate_bucket_expr(name)
            name, region_in_name = name.split('@')

        # Region is from the specified source in oci://<bucket>@<region> format
        region_in_source = None
        if isinstance(source,
                      str) and source.startswith('oci://') and '@' in source:
            self._validate_bucket_expr(source)
            source, region_in_source = source.split('@')

        if region_in_name is not None and region_in_source is not None:
            # This should never happen because name and source will never be
            # the remote bucket at the same time.
            assert region_in_name == region_in_source, (
                f'Mismatch region specified. Region in name {region_in_name}, '
                f'but region in source is {region_in_source}')

        if region_in_name is not None:
            region = region_in_name
        elif region_in_source is not None:
            region = region_in_source

        # Default region set to what specified in oci config.
        if region is None:
            region = oci.get_oci_config()['region']

        # So far from now on, the name and source are canonical, means there
        # is no region (@<region> suffix) associated with them anymore.

        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)
        # TODO(zpoint): add _bucket_sub_path to the sync/mount/delete commands

    def _validate_bucket_expr(self, bucket_expr: str):
        pattern = r'^(\w+://)?[A-Za-z0-9-._]+(@\w{2}-\w+-\d{1})$'
        if not re.match(pattern, bucket_expr):
            raise ValueError(
                'The format for the bucket portion is <bucket>@<region> '
                'when specify a region with a bucket.')

    def _validate(self):
        if self.source is not None and isinstance(self.source, str):
            if self.source.startswith('oci://'):
                assert self.name == data_utils.split_oci_path(self.source)[0], (
                    'OCI Bucket is specified as path, the name should be '
                    'the same as OCI bucket.')
            elif not re.search(r'^\w+://', self.source):
                # Treat it as local path.
                pass
            else:
                raise NotImplementedError(
                    f'Moving data from {self.source} to OCI is not supported.')

        # Validate name
        self.name = self.validate_name(self.name)
        # Check if the storage is enabled
        if not _is_storage_cloud_enabled(str(clouds.OCI())):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesUnavailableError(
                    'Storage \'store: oci\' specified, but ' \
                    'OCI access is disabled. To fix, enable '\
                    'OCI by running `sky check`. '\
                    'More info: https://docs.skypilot.co/en/latest/getting-started/installation.html.' # pylint: disable=line-too-long
                    )

    @classmethod
    def validate_name(cls, name) -> str:
        """Validates the name of the OCI store.

        Source for rules: https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/managingbuckets.htm#Managing_Buckets # pylint: disable=line-too-long
        """

        def _raise_no_traceback_name_error(err_str):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageNameError(err_str)

        if name is not None and isinstance(name, str):
            # Check for overall length
            if not 1 <= len(name) <= 256:
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must contain 1-256 '
                    'characters.')

            # Check for valid characters and start/end with a number or letter
            pattern = r'^[A-Za-z0-9-._]+$'
            if not re.match(pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} can only contain '
                    'upper or lower case letters, numeric characters, hyphens '
                    '(-), underscores (_), and dots (.). Spaces are not '
                    'allowed. Names must start and end with a number or '
                    'letter.')
        else:
            _raise_no_traceback_name_error('Store name must be specified.')
        return name

    def initialize(self):
        """Initializes the OCI store object on the cloud.

        Initialization involves fetching bucket if exists, or creating it if
        it does not.

        Raises:
          StorageBucketCreateError: If bucket creation fails
          StorageBucketGetError: If fetching existing bucket fails
          StorageInitError: If general initialization fails.
        """
        # pylint: disable=import-outside-toplevel
        from sky.clouds.utils import oci_utils
        from sky.provision.oci.query_utils import query_helper

        self.oci_config_file = oci.get_config_file()
        self.config_profile = oci_utils.oci_config.get_profile()

        ## pylint: disable=line-too-long
        # What's compartment? See thttps://docs.oracle.com/en/cloud/foundation/cloud_architecture/governance/compartments.html
        self.compartment = query_helper.find_compartment(self.region)
        self.client = oci.get_object_storage_client(region=self.region,
                                                    profile=self.config_profile)
        self.namespace = self.client.get_namespace(
            compartment_id=oci.get_oci_config()['tenancy']).data

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
                self.batch_oci_rsync(self.source, create_dirs=True)
            elif self.source is not None:
                if self.source.startswith('oci://'):
                    pass
                else:
                    self.batch_oci_rsync([self.source])
        except exceptions.StorageUploadError:
            raise
        except Exception as e:
            raise exceptions.StorageUploadError(
                f'Upload failed for store {self.name}') from e

    def delete(self) -> None:
        # When the bucket is user-provided (not Sky-managed) and only a
        # sub-path was mounted, delete only that sub-path -- never the whole
        # bucket. This mirrors the guard every other store has; without it,
        # tearing down a job/service that used an existing `oci://` bucket via
        # `jobs.bucket` / `serve.bucket` would empty and delete the entire
        # user-owned bucket (and all sibling data in it).
        if self._bucket_sub_path is not None and not self.is_sky_managed:
            return self._delete_sub_path()

        deleted_by_skypilot = self._delete_oci_bucket(self.name)
        if deleted_by_skypilot:
            msg_str = f'Deleted OCI bucket {self.name}.'
        else:
            msg_str = (f'OCI bucket {self.name} may have been deleted '
                       f'externally. Removing from local state.')
        logger.info(f'{colorama.Fore.GREEN}{msg_str}'
                    f'{colorama.Style.RESET_ALL}')

    def _delete_sub_path(self) -> None:
        """Removes objects under the mounted sub-path, leaving the bucket."""
        assert self._bucket_sub_path is not None, 'bucket_sub_path is not set'
        deleted_by_skypilot = self._delete_oci_bucket_sub_path(
            self.name, self._bucket_sub_path)
        if deleted_by_skypilot:
            msg_str = (f'Removed objects from OCI bucket '
                       f'{self.name}/{self._bucket_sub_path}.')
        else:
            msg_str = (f'OCI bucket {self.name} may have been deleted '
                       f'externally. Removing from local state.')
        logger.info(f'{colorama.Fore.GREEN}{msg_str}'
                    f'{colorama.Style.RESET_ALL}')

    def get_handle(self) -> StorageHandle:
        return self.client.get_bucket(namespace_name=self.namespace,
                                      bucket_name=self.name).data

    def batch_oci_rsync(self,
                        source_path_list: list[Path],
                        create_dirs: bool = False) -> None:
        """Invokes oci sync to batch upload a list of local paths to Bucket

        Use OCI bulk operation to batch process the file upload

        Args:
            source_path_list: List of paths to local files or directories
            create_dirs: If the local_path is a directory and this is set to
                False, the contents of the directory are directly uploaded to
                root of the bucket. If the local_path is a directory and this is
                set to True, the directory is created in the bucket root and
                contents are uploaded to it.
        """
        sub_path = (f'{self._bucket_sub_path}/'
                    if self._bucket_sub_path else '')

        @oci.with_oci_env
        def get_file_sync_command(base_dir_path, file_names):
            includes = ' '.join(
                [f'--include "{file_name}"' for file_name in file_names])
            prefix_arg = ''
            if sub_path:
                prefix_arg = f'--object-prefix "{sub_path.strip("/")}"'
            sync_command = (
                'oci os object bulk-upload --no-follow-symlinks --overwrite '
                f'--bucket-name {self.name} --namespace-name {self.namespace} '
                f'--region {self.region} --src-dir "{base_dir_path}" '
                f'{prefix_arg} '
                f'{includes}')

            return sync_command

        @oci.with_oci_env
        def get_dir_sync_command(src_dir_path, dest_dir_name):
            if dest_dir_name and not str(dest_dir_name).endswith('/'):
                dest_dir_name = f'{dest_dir_name}/'

            excluded_list = storage_utils.get_excluded_files(src_dir_path)
            excluded_list.append('.git/*')
            excludes = ' '.join([
                f'--exclude {shlex.quote(file_name)}'
                for file_name in excluded_list
            ])

            # we exclude .git directory from the sync
            sync_command = (
                'oci os object bulk-upload --no-follow-symlinks --overwrite '
                f'--bucket-name {self.name} --namespace-name {self.namespace} '
                f'--region {self.region} '
                f'--object-prefix "{sub_path}{dest_dir_name}" '
                f'--src-dir "{src_dir_path}" {excludes}')

            return sync_command

        # Generate message for upload
        if len(source_path_list) > 1:
            source_message = f'{len(source_path_list)} paths'
        else:
            source_message = source_path_list[0]

        log_path = sky_logging.generate_tmp_logging_file_path(
            _STORAGE_LOG_FILE_NAME)
        sync_path = f'{source_message} -> oci://{self.name}/{sub_path}'
        with rich_utils.safe_status(
                ux_utils.spinner_message(f'Syncing {sync_path}',
                                         log_path=log_path)):
            data_utils.parallel_upload(
                source_path_list=source_path_list,
                filesync_command_generator=get_file_sync_command,
                dirsync_command_generator=get_dir_sync_command,
                log_path=log_path,
                bucket_name=self.name,
                access_denied_message=self._ACCESS_DENIED_MESSAGE,
                create_dirs=create_dirs,
                max_concurrent_uploads=1)

            logger.info(
                ux_utils.finishing_message(f'Storage synced: {sync_path}',
                                           log_path))

    def _get_bucket(self) -> tuple[StorageHandle, bool]:
        """Obtains the OCI bucket.
        If the bucket exists, this method will connect to the bucket.

        If the bucket does not exist, there are three cases:
          1) Raise an error if the bucket source starts with oci://
          2) Return None if bucket has been externally deleted and
             sync_on_reconstruction is False
          3) Create and return a new bucket otherwise

        Return tuple (Bucket, Boolean): The first item is the bucket
        json payload from the OCI API call, the second item indicates
        if this is a new created bucket(True) or an existing bucket(False).

        Raises:
            StorageBucketCreateError: If creating the bucket fails
            StorageBucketGetError: If fetching a bucket fails
        """
        try:
            get_bucket_response = self.client.get_bucket(
                namespace_name=self.namespace, bucket_name=self.name)
            bucket = get_bucket_response.data
            return bucket, False
        except oci.service_exception() as e:
            if e.status == 404:  # Not Found
                if isinstance(self.source,
                              str) and self.source.startswith('oci://'):
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageBucketGetError(
                            'Attempted to connect to a non-existent bucket: '
                            f'{self.source}') from e
                else:
                    # If bucket cannot be found (i.e., does not exist), it is
                    # to be created by Sky. However, creation is skipped if
                    # Store object is being reconstructed for deletion.
                    if self.sync_on_reconstruction:
                        bucket = self._create_oci_bucket(self.name)
                        return bucket, True
                    else:
                        return None, False
            elif e.status == 401:  # Unauthorized
                # AccessDenied error for buckets that are private and not
                # owned by user.
                command = (
                    f'oci os object list --namespace-name {self.namespace} '
                    f'--bucket-name {self.name}')
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        _BUCKET_FAIL_TO_CONNECT_MESSAGE.format(name=self.name) +
                        f' To debug, consider running `{command}`.') from e
            else:
                # Unknown / unexpected error happened. This might happen when
                # Object storage service itself functions not normal (e.g.
                # maintainance event causes internal server error or request
                # timeout, etc).
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        f'Failed to connect to OCI bucket {self.name}') from e

    def mount_command(self, mount_path: str, read_only: bool = False) -> str:
        """Returns the command to mount the bucket to the mount_path.

        Uses Rclone to mount the bucket.

        Args:
          mount_path: str; Path to mount the bucket to.
          read_only: bool; Whether to mount as read-only.
        """
        install_cmd = mounting_utils.get_rclone_install_cmd()
        mount_cmd = mounting_utils.get_oci_mount_cmd(
            mount_path=mount_path,
            store_name=self.name,
            region=str(self.region),
            namespace=self.namespace,
            compartment=self.bucket.compartment_id,
            config_file=self.oci_config_file,
            config_profile=self.config_profile,
            read_only=read_only)
        version_check_cmd = mounting_utils.get_rclone_version_check_cmd()

        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cmd, version_check_cmd)

    def _download_file(self, remote_path: str, local_path: str) -> None:
        """Downloads file from remote to local on OCI bucket

        Args:
          remote_path: str; Remote path on OCI bucket
          local_path: str; Local path on user's device
        """
        if remote_path.startswith(f'/{self.name}'):
            # If the remote path is /bucket_name, we need to
            # remove the leading /
            remote_path = remote_path.lstrip('/')

        filename = os.path.basename(remote_path)
        if not local_path.endswith(filename):
            local_path = os.path.join(local_path, filename)

        @oci.with_oci_env
        def get_file_download_command(remote_path, local_path):
            download_command = (f'oci os object get --bucket-name {self.name} '
                                f'--namespace-name {self.namespace} '
                                f'--region {self.region} --name {remote_path} '
                                f'--file {local_path}')

            return download_command

        download_command = get_file_download_command(remote_path, local_path)

        try:
            with rich_utils.safe_status(
                    f'[bold cyan]Downloading: {remote_path} -> {local_path}[/]'
            ):
                subprocess.check_output(download_command,
                                        stderr=subprocess.STDOUT,
                                        shell=True)
        except subprocess.CalledProcessError as e:
            logger.error(f'Download failed: {remote_path} -> {local_path}.\n'
                         f'Detail errors: {e.output}')
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketDeleteError(
                    f'Failed download file {self.name}:{remote_path}.') from e

    def _create_oci_bucket(self, bucket_name: str) -> StorageHandle:
        """Creates OCI bucket with specific name in specific region

        Args:
          bucket_name: str; Name of bucket
          region: str; Region name, e.g. us-central1, us-west1
        """
        logger.debug(f'_create_oci_bucket: {bucket_name}')
        try:
            create_bucket_response = self.client.create_bucket(
                namespace_name=self.namespace,
                create_bucket_details=oci.oci.object_storage.models.
                CreateBucketDetails(
                    name=bucket_name,
                    compartment_id=self.compartment,
                ))
            bucket = create_bucket_response.data
            return bucket
        except oci.service_exception() as e:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketCreateError(
                    f'Failed to create OCI bucket: {self.name}') from e

    def _delete_oci_bucket(self, bucket_name: str) -> bool:
        """Deletes OCI bucket, including all objects in bucket

        Args:
          bucket_name: str; Name of bucket

        Returns:
         bool; True if bucket was deleted, False if it was deleted externally.
        """
        logger.debug(f'_delete_oci_bucket: {bucket_name}')

        @oci.with_oci_env
        def get_bucket_delete_command(bucket_name):
            remove_command = (f'oci os bucket delete '
                              f'--bucket-name {bucket_name} '
                              f'--region {self.region} '
                              f'--empty --force')

            return remove_command

        remove_command = get_bucket_delete_command(bucket_name)

        try:
            with rich_utils.safe_status(
                    f'[bold cyan]Deleting OCI bucket {bucket_name}[/]'):
                # `with_oci_env` returns a single `&&`-joined shell command
                # (venv setup + `source` + the oci call), so it must run
                # through a shell.
                subprocess.check_output(remove_command,
                                        stderr=subprocess.STDOUT,
                                        shell=True)
        except subprocess.CalledProcessError as e:
            if 'BucketNotFound' in e.output.decode('utf-8'):
                logger.debug(
                    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE.format(
                        bucket_name=bucket_name))
                return False
            else:
                logger.error(e.output)
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketDeleteError(
                        f'Failed to delete OCI bucket {bucket_name}.')
        return True

    def _delete_oci_bucket_sub_path(self, bucket_name: str,
                                    sub_path: str) -> bool:
        """Deletes objects under a prefix in an OCI bucket.

        Unlike `_delete_oci_bucket`, this removes only the objects under
        `sub_path` and leaves the (user-owned) bucket itself intact.

        Args:
          bucket_name: str; Name of bucket
          sub_path: str; Prefix whose objects should be removed

        Returns:
         bool; True if the objects were deleted, False if the bucket was
         deleted externally.
        """
        logger.debug(f'_delete_oci_bucket_sub_path: {bucket_name}/{sub_path}')
        prefix = sub_path.strip('/')

        @oci.with_oci_env
        def get_bulk_delete_command(bucket_name, prefix):
            remove_command = (f'oci os object bulk-delete '
                              f'--namespace-name {self.namespace} '
                              f'--bucket-name {bucket_name} '
                              f'--region {self.region} '
                              f'--prefix "{prefix}/" --force')

            return remove_command

        remove_command = get_bulk_delete_command(bucket_name, prefix)

        try:
            with rich_utils.safe_status(
                    f'[bold cyan]Deleting objects under prefix {prefix} in OCI '
                    f'bucket {bucket_name}[/]'):
                # `with_oci_env` returns a single `&&`-joined shell command
                # (venv setup + `source` + the oci call), so it must run
                # through a shell -- the same way `_download_file` runs its
                # wrapped command.
                subprocess.check_output(remove_command,
                                        stderr=subprocess.STDOUT,
                                        shell=True)
        except subprocess.CalledProcessError as e:
            if 'BucketNotFound' in e.output.decode('utf-8'):
                logger.debug(
                    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE.format(
                        bucket_name=bucket_name))
                return False
            else:
                logger.error(e.output)
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketDeleteError(
                        f'Failed to delete objects under prefix {sub_path} in '
                        f'OCI bucket {bucket_name}.')
        return True


class OciS3CompatibleStore(S3CompatibleStore):
    """OciS3CompatibleStore represents the backend for OCI buckets accessed
    via OCI Object Storage's Amazon S3 Compatibility API.

    https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi.htm

    It shares StoreType.OCI and the oci:// prefix with the native OciStore;
    which class serves a request is decided at the StoreType.OCI dispatch
    sites based on oci_s3.use_s3_api(). It must therefore not be decorated
    with @register_s3_compatible_store: the registry is consulted before
    the StoreType.OCI dispatch branches, and registering under 'OCI' would
    take over all OCI dispatch unconditionally.
    """

    def __init__(self,
                 name: str,
                 source: str,
                 region: str | None = None,
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool = True,
                 _bucket_sub_path: str | None = None):
        # The native OciStore supports a <bucket>@<region> suffix. The
        # S3-compatible endpoint is pinned to a single region, so a region
        # suffix cannot be honored here. Only reject @ in source when it is
        # an oci:// URI; a local path containing @ is valid.
        for bucket_expr in (name, source):
            if not isinstance(bucket_expr, str):
                continue
            is_oci_uri = bucket_expr.startswith('oci://')
            if is_oci_uri:
                bucket_expr = data_utils.split_oci_path(bucket_expr)[0]
            if (bucket_expr == name or is_oci_uri) and '@' in bucket_expr:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageNameError(
                        f'<bucket>@<region> ({bucket_expr!r}) is not '
                        'supported when accessing OCI via the S3-compatible '
                        'API; the region is determined by the endpoint in '
                        f'{oci_s3.OCI_S3_CONFIG_PATH}.')
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    @classmethod
    def get_config(cls) -> S3CompatibleConfig:
        """Return the configuration for the OCI S3-compatible API."""
        return S3CompatibleConfig(
            store_type='OCI',
            url_prefix='oci://',
            client_factory=lambda region: data_utils.create_oci_s3_client(),
            resource_factory=lambda name: oci_s3.resource('s3').Bucket(name),
            split_path=data_utils.split_oci_path,
            verify_bucket=data_utils.verify_oci_s3_bucket,
            aws_profile=oci_s3.OCI_S3_PROFILE_NAME,
            get_endpoint_url=oci_s3.get_endpoint,
            credentials_file=oci_s3.OCI_S3_CREDENTIALS_PATH,
            config_file=oci_s3.OCI_S3_CONFIG_PATH,
            # OCI returns 501 for aws-chunked uploads, which the AWS CLI
            # enables by default to carry a trailing checksum. Disable it so
            # `aws s3 sync` uploads are sent as plain (non-chunked) requests.
            # Response validation is relaxed to match.
            extra_cli_env={
                'AWS_REQUEST_CHECKSUM_CALCULATION': 'when_required',
                'AWS_RESPONSE_CHECKSUM_VALIDATION': 'when_required',
            },
            cloud_name=str(clouds.OCI()),
            default_region=oci_s3.get_region(),
            mount_cmd_factory=cls._get_oci_s3_mount_cmd,
        )

    @classmethod
    def validate_name(cls, name: str) -> str:
        """Validates the store name using OCI bucket naming rules.

        OCI bucket names allow uppercase letters and underscores, which the
        generic S3 rules would reject; buckets created via the native API
        must remain accessible via the S3-compatible API.
        """
        return OciStore.validate_name(name)

    @classmethod
    def _get_oci_s3_mount_cmd(cls,
                              bucket_name: str,
                              mount_path: str,
                              bucket_sub_path: str | None,
                              read_only: bool = False) -> str:
        """Factory method for the OCI S3-compatible mount command."""
        endpoint_url = oci_s3.get_endpoint()
        return mounting_utils.get_oci_s3_mount_cmd(
            oci_s3.OCI_S3_CREDENTIALS_PATH,
            oci_s3.OCI_S3_PROFILE_NAME,
            bucket_name,
            endpoint_url,
            mount_path,
            region=oci_s3.get_region(),
            _bucket_sub_path=bucket_sub_path,
            read_only=read_only)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        """OCI S3-compatible cached mount implementation using rclone."""
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.OCI.get_profile_name(self.name))
        rclone_config = data_utils.Rclone.RcloneStores.OCI.get_config(
            rclone_profile_name=rclone_profile_name)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.bucket.name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)


# Preserve the historical public and pickle identities while storage.py remains
# the stable facade for both implementations.
OciStore.__module__ = storage_lib.__name__
OciS3CompatibleStore.__module__ = storage_lib.__name__
