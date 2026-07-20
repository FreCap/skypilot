"""Azure Blob Storage backend implementation."""
import hashlib
import re
import shlex
import time
import typing
from typing import Any

import colorama

from sky import clouds
from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky.adaptors import azure
from sky.data import data_utils
from sky.data import mounting_utils
from sky.data import storage as storage_lib
from sky.data import storage_utils
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from google.cloud import storage

StorageHandle = storage_lib.StorageHandle
Path = storage_lib.Path
SourceType = storage_lib.SourceType
AbstractStore = storage_lib.AbstractStore
MountCachedConfig = storage_lib.MountCachedConfig
logger = storage_lib.logger
_is_storage_cloud_enabled = storage_lib.azure_storage_cloud_enabled
_MAX_CONCURRENT_UPLOADS = storage_lib.AZURE_MAX_CONCURRENT_UPLOADS
_BUCKET_FAIL_TO_CONNECT_MESSAGE = (
    storage_lib.AZURE_BUCKET_FAIL_TO_CONNECT_MESSAGE)
_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE = (
    storage_lib.AZURE_BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE)
_STORAGE_LOG_FILE_NAME = storage_lib.AZURE_STORAGE_LOG_FILE_NAME


class AzureBlobStore(AbstractStore):
    """Represents the backend for Azure Blob Storage Container."""

    _ACCESS_DENIED_MESSAGE = 'Access Denied'
    DEFAULT_RESOURCE_GROUP_NAME = 'sky{user_hash}'
    # Unlike resource group names, which only need to be unique within the
    # subscription, storage account names must be globally unique across all of
    # Azure users. Hence, the storage account name includes the subscription
    # hash as well to ensure its uniqueness.
    DEFAULT_STORAGE_ACCOUNT_NAME = (
        'sky{region_hash}{user_hash}{subscription_hash}')
    _SUBSCRIPTION_HASH_LENGTH = 4
    _REGION_HASH_LENGTH = 4

    class AzureBlobStoreMetadata(AbstractStore.StoreMetadata):
        """A pickle-able representation of Azure Blob Store.

        Allows store objects to be written to and reconstructed from
        global_user_state.
        """

        def __init__(self,
                     *,
                     name: str,
                     storage_account_name: str,
                     source: SourceType | None,
                     region: str | None = None,
                     is_sky_managed: bool | None = None):
            self.storage_account_name = storage_account_name
            super().__init__(name=name,
                             source=source,
                             region=region,
                             is_sky_managed=is_sky_managed)

        def __repr__(self):
            return (f'AzureBlobStoreMetadata('
                    f'\n\tname={self.name},'
                    f'\n\tstorage_account_name={self.storage_account_name},'
                    f'\n\tsource={self.source},'
                    f'\n\tregion={self.region},'
                    f'\n\tis_sky_managed={self.is_sky_managed})')

    def __init__(self,
                 name: str,
                 source: str,
                 storage_account_name: str = '',
                 region: str | None = 'eastus',
                 is_sky_managed: bool | None = None,
                 sync_on_reconstruction: bool = True,
                 _bucket_sub_path: str | None = None):
        self.storage_client: storage.Client
        self.resource_client: storage.Client
        self.container_name: str
        # storage_account_name is not None when initializing only
        # when it is being reconstructed from the handle(metadata).
        self.storage_account_name = storage_account_name
        self.storage_account_key: str | None = None
        self.resource_group_name: str | None = None
        if region is None:
            region = 'eastus'
        super().__init__(name, source, region, is_sky_managed,
                         sync_on_reconstruction, _bucket_sub_path)

    @classmethod
    def from_metadata(cls, metadata: AbstractStore.StoreMetadata,
                      **override_args) -> 'AzureBlobStore':
        """Creates AzureBlobStore from a AzureBlobStoreMetadata object.

        Used when reconstructing Storage and Store objects from
        global_user_state.

        Args:
            metadata: Metadata object containing AzureBlobStore information.

        Returns:
            An instance of AzureBlobStore.
        """
        assert isinstance(metadata, AzureBlobStore.AzureBlobStoreMetadata)
        # TODO: this needs to be kept in sync with the abstract
        # AbstractStore.from_metadata.
        return cls(
            name=override_args.get('name', metadata.name),
            storage_account_name=override_args.get(
                'storage_account', metadata.storage_account_name),
            # TODO(cooperc): fix the types for mypy 1.16
            # Azure store expects a string path; metadata.source may be a Path
            # or List[Path].
            source=override_args.get('source',
                                     metadata.source),  # type: ignore[arg-type]
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

    def get_metadata(self) -> AzureBlobStoreMetadata:
        return self.AzureBlobStoreMetadata(
            name=self.name,
            storage_account_name=self.storage_account_name,
            source=self.source,
            region=self.region,
            is_sky_managed=self.is_sky_managed)

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
                _, container_name, _ = data_utils.split_az_path(self.source)
                assert self.name == container_name, (
                    'Azure bucket is specified as path, the name should be '
                    'the same as Azure bucket.')
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
                    'Moving data from OCI to AZureBlob is not supported.')
            elif self.source.startswith('cw://'):
                raise NotImplementedError(
                    'Moving data from CoreWeave Object Storage to AzureBlob is'
                    ' currently not supported.')
        # Validate name
        self.name = self.validate_name(self.name)

        # Check if the storage is enabled
        if not _is_storage_cloud_enabled(str(clouds.Azure())):
            with ux_utils.print_exception_no_traceback():
                raise exceptions.ResourcesUnavailableError(
                    'Storage "store: azure" specified, but '
                    'Azure access is disabled. To fix, enable '
                    'Azure by running `sky check`. More info: '
                    'https://docs.skypilot.co/en/latest/getting-started/installation.html.'  # pylint: disable=line-too-long
                )

    @classmethod
    def validate_name(cls, name: str) -> str:
        """Validates the name of the AZ Container.

        Source for rules: https://learn.microsoft.com/en-us/rest/api/storageservices/Naming-and-Referencing-Containers--Blobs--and-Metadata#container-names # pylint: disable=line-too-long

        Args:
            name: Name of the container

        Returns:
            Name of the container

        Raises:
            StorageNameError: if the given container name does not follow the
                naming convention
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
            pattern = r'^[a-z0-9][-a-z0-9]*[a-z0-9]$'
            if not re.match(pattern, name):
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} can consist only of '
                    'lowercase letters, numbers, and hyphens (-). '
                    'It must begin and end with a letter or number.')

            # Check for two adjacent hyphens
            if '--' in name:
                _raise_no_traceback_name_error(
                    f'Invalid store name: name {name} must not contain '
                    'two adjacent hyphens.')

        else:
            _raise_no_traceback_name_error('Store name must be specified.')
        return name

    def initialize(self):
        """Initializes the AZ Container object on the cloud.

        Initialization involves fetching container if exists, or creating it if
        it does not. Also, it checks for the existence of the storage account
        if provided by the user and the resource group is inferred from it.
        If not provided, both are created with a default naming conventions.

        Raises:
            StorageBucketCreateError: If container creation fails or storage
                account attempted to be created already exists.
            StorageBucketGetError: If fetching existing container fails.
            StorageInitError: If general initialization fails.
            NonExistentStorageAccountError: When storage account provided
                either through config.yaml or local db does not exist under
                user's subscription ID.
        """
        self.storage_client = data_utils.create_az_client('storage')
        self.resource_client = data_utils.create_az_client('resource')
        self._update_storage_account_name_and_resource()

        self.container_name, is_new_bucket = self._get_bucket()
        if self.is_sky_managed is None:
            # If is_sky_managed is not specified, then this is a new storage
            # object (i.e., did not exist in global_user_state) and we should
            # set the is_sky_managed property.
            # If is_sky_managed is specified, then we take no action.
            self.is_sky_managed = is_new_bucket

    def _update_storage_account_name_and_resource(self):
        self.storage_account_name, self.resource_group_name = (
            self._get_storage_account_and_resource_group())

        # resource_group_name is set to None when using non-sky-managed
        # public container or private container without authorization.
        if self.resource_group_name is not None:
            self.storage_account_key = data_utils.get_az_storage_account_key(
                self.storage_account_name, self.resource_group_name,
                self.storage_client, self.resource_client)

    def update_storage_attributes(self, **kwargs: dict[str, Any]):
        assert 'storage_account_name' in kwargs, (
            'only storage_account_name supported')
        assert isinstance(kwargs['storage_account_name'],
                          str), ('storage_account_name must be a string')
        self.storage_account_name = kwargs['storage_account_name']
        self._update_storage_account_name_and_resource()

    @staticmethod
    def get_default_storage_account_name(region: str | None) -> str:
        """Generates a unique default storage account name.

        The subscription ID is included to avoid conflicts when user switches
        subscriptions. The length of region_hash, user_hash, and
        subscription_hash are adjusted to ensure the storage account name
        adheres to the 24-character limit, as some region names can be very
        long. Using a 4-character hash for the region helps keep the name
        concise and prevents potential conflicts.
        Reference: https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules#microsoftstorage # pylint: disable=line-too-long

        Args:
            region: Name of the region to create the storage account/container.

        Returns:
            Name of the default storage account.
        """
        assert region is not None
        subscription_id = azure.get_subscription_id()
        subscription_hash_obj = hashlib.md5(subscription_id.encode('utf-8'),
                                            usedforsecurity=False)
        subscription_hash = subscription_hash_obj.hexdigest(
        )[:AzureBlobStore._SUBSCRIPTION_HASH_LENGTH]
        region_hash_obj = hashlib.md5(region.encode('utf-8'),
                                      usedforsecurity=False)
        region_hash = region_hash_obj.hexdigest()[:AzureBlobStore.
                                                  _REGION_HASH_LENGTH]

        storage_account_name = (
            AzureBlobStore.DEFAULT_STORAGE_ACCOUNT_NAME.format(
                region_hash=region_hash,
                user_hash=common_utils.get_user_hash(),
                subscription_hash=subscription_hash))

        return storage_account_name

    def _get_storage_account_and_resource_group(self) -> tuple[str, str | None]:
        """Get storage account and resource group to be used for AzureBlobStore

        Storage account name and resource group name of the container to be
        used for AzureBlobStore object is obtained from this function. These
        are determined by either through the metadata, source, config.yaml, or
        default name:

        1) If self.storage_account_name already has a set value, this means we
        are reconstructing the storage object using metadata from the local
        state.db to reuse sky managed storage.

        2) Users provide externally created non-sky managed storage endpoint
        as a source from task yaml. Then, storage account is read from it and
        the resource group is inferred from it.

        3) Users provide the storage account, which they want to create the
        sky managed storage, through config.yaml. Then, resource group is
        inferred from it.

        4) If none of the above are true, default naming conventions are used
        to create the resource group and storage account for the users.

        Returns:
            str: The storage account name.
            Optional[str]: The resource group name, or None if not found.

        Raises:
            StorageBucketCreateError: If storage account attempted to be
                created already exists.
            NonExistentStorageAccountError: When storage account provided
                either through config.yaml or local db does not exist under
                user's subscription ID.
        """
        # self.storage_account_name already has a value only when it is being
        # reconstructed with metadata from local db.
        if self.storage_account_name:
            resource_group_name = azure.get_az_resource_group(
                self.storage_account_name)
            if resource_group_name is None:
                # If the storage account does not exist, the containers under
                # the account does not exist as well.
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.NonExistentStorageAccountError(
                        f'The storage account {self.storage_account_name!r} '
                        'read from local db does not exist under your '
                        'subscription ID. The account may have been externally'
                        ' deleted.')
            storage_account_name = self.storage_account_name
        # Using externally created container
        elif (isinstance(self.source, str) and
              data_utils.is_az_container_endpoint(self.source)):
            storage_account_name, container_name, _ = data_utils.split_az_path(
                self.source)
            assert self.name == container_name
            resource_group_name = azure.get_az_resource_group(
                storage_account_name)
        # Creates new resource group and storage account or use the
        # storage_account provided by the user through config.yaml
        else:
            config_storage_account = (
                skypilot_config.get_effective_region_config(
                    cloud='azure',
                    region=None,
                    keys=('storage_account',),
                    default_value=None))
            if config_storage_account is not None:
                # using user provided storage account from config.yaml
                storage_account_name = config_storage_account
                resource_group_name = azure.get_az_resource_group(
                    storage_account_name)
                # when the provided storage account does not exist under user's
                # subscription id.
                if resource_group_name is None:
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.NonExistentStorageAccountError(
                            'The storage account '
                            f'{storage_account_name!r} specified in '
                            'config.yaml does not exist under the user\'s '
                            'subscription ID. Provide a storage account '
                            'through config.yaml only when creating a '
                            'container under an already existing storage '
                            'account within your subscription ID.')
            else:
                # If storage account name is not provided from config, then
                # use default resource group and storage account names.
                storage_account_name = self.get_default_storage_account_name(
                    self.region)
                resource_group_name = (self.DEFAULT_RESOURCE_GROUP_NAME.format(
                    user_hash=common_utils.get_user_hash()))
                try:
                    # obtains detailed information about resource group under
                    # the user's subscription. Used to check if the name
                    # already exists
                    self.resource_client.resource_groups.get(
                        resource_group_name)
                except azure.exceptions().ResourceNotFoundError:
                    with rich_utils.safe_status(
                            ux_utils.spinner_message(
                                f'Setting up resource group: '
                                f'{resource_group_name}')):
                        self.resource_client.resource_groups.create_or_update(
                            resource_group_name, {'location': self.region})
                    logger.info('  Created Azure resource group '
                                f'{resource_group_name!r}.')
                # check if the storage account name already exists under the
                # given resource group name.
                try:
                    self.storage_client.storage_accounts.get_properties(
                        resource_group_name, storage_account_name)
                except azure.exceptions().ResourceNotFoundError:
                    with rich_utils.safe_status(
                            ux_utils.spinner_message(
                                f'Setting up storage account: '
                                f'{storage_account_name}')):
                        self._create_storage_account(resource_group_name,
                                                     storage_account_name)
                        # wait until new resource creation propagates to Azure.
                        time.sleep(1)
                    logger.info('  Created Azure storage account '
                                f'{storage_account_name!r}.')

        return storage_account_name, resource_group_name

    def _create_storage_account(self, resource_group_name: str,
                                storage_account_name: str) -> None:
        """Creates new storage account and assign Storage Blob Data Owner role.

        Args:
            resource_group_name: Name of the resource group which the storage
                account will be created under.
            storage_account_name: Name of the storage account to be created.

        Raises:
            StorageBucketCreateError: If storage account attempted to be
                created already exists or fails to assign role to the create
                storage account.
        """
        try:
            creation_response = (
                self.storage_client.storage_accounts.begin_create(
                    resource_group_name, storage_account_name, {
                        'sku': {
                            'name': 'Standard_GRS'
                        },
                        'kind': 'StorageV2',
                        'location': self.region,
                        'encryption': {
                            'services': {
                                'blob': {
                                    'key_type': 'Account',
                                    'enabled': True
                                }
                            },
                            'key_source': 'Microsoft.Storage'
                        },
                    }).result())
        except azure.exceptions().ResourceExistsError as error:
            with ux_utils.print_exception_no_traceback():
                raise exceptions.StorageBucketCreateError(
                    'Failed to create storage account '
                    f'{storage_account_name!r}. You may be '
                    'attempting to create a storage account '
                    'already being in use. Details: '
                    f'{common_utils.format_exception(error, use_bracket=True)}')

        # It may take some time for the created storage account to propagate
        # to Azure, we reattempt to assign the role for several times until
        # storage account creation fully propagates.
        role_assignment_start = time.time()
        retry = 0

        while (time.time() - role_assignment_start
               < constants.WAIT_FOR_STORAGE_ACCOUNT_CREATION):
            try:
                azure.assign_storage_account_iam_role(
                    storage_account_name=storage_account_name,
                    storage_account_id=creation_response.id)
                return
            except AttributeError as e:
                if 'signed_session' in str(e):
                    if retry % 5 == 0:
                        logger.info(
                            'Retrying role assignment due to propagation '
                            'delay of the newly created storage account. '
                            f'Retry count: {retry}.')
                    time.sleep(1)
                    retry += 1
                    continue
                with ux_utils.print_exception_no_traceback():
                    role_assignment_failure_error_msg = (
                        constants.ROLE_ASSIGNMENT_FAILURE_ERROR_MSG.format(
                            storage_account_name=storage_account_name))
                    raise exceptions.StorageBucketCreateError(
                        f'{role_assignment_failure_error_msg}'
                        'Details: '
                        f'{common_utils.format_exception(e, use_bracket=True)}')

    def upload(self):
        """Uploads source to store bucket.

        Upload must be called by the Storage handler - it is not called on
        Store initialization.

        Raises:
            StorageUploadError: if upload fails.
        """
        try:
            if isinstance(self.source, list):
                self.batch_az_blob_sync(self.source, create_dirs=True)
            elif self.source is not None:
                error_message = (
                    'Moving data directly from {cloud} to Azure is currently '
                    'not supported. Please specify a local source for the '
                    'storage object.')
                if data_utils.is_az_container_endpoint(self.source):
                    pass
                elif self.source.startswith('s3://'):
                    raise NotImplementedError(error_message.format('S3'))
                elif self.source.startswith('gs://'):
                    raise NotImplementedError(error_message.format('GCS'))
                elif self.source.startswith('r2://'):
                    raise NotImplementedError(error_message.format('R2'))
                elif self.source.startswith('cos://'):
                    raise NotImplementedError(error_message.format('IBM COS'))
                elif self.source.startswith('oci://'):
                    raise NotImplementedError(error_message.format('OCI'))
                elif self.source.startswith('nebius://'):
                    raise NotImplementedError(error_message.format('NEBIUS'))
                elif self.source.startswith('cw://'):
                    raise NotImplementedError(error_message.format('CoreWeave'))
                else:
                    self.batch_az_blob_sync([self.source])
        except exceptions.StorageUploadError:
            raise
        except Exception as e:
            raise exceptions.StorageUploadError(
                f'Upload failed for store {self.name}') from e

    def delete(self) -> None:
        """Deletes the storage."""
        if self._bucket_sub_path is not None and not self.is_sky_managed:
            return self._delete_sub_path()

        deleted_by_skypilot = self._delete_az_bucket(self.name)
        if deleted_by_skypilot:
            msg_str = (f'Deleted AZ Container {self.name!r} under storage '
                       f'account {self.storage_account_name!r}.')
        else:
            msg_str = (f'AZ Container {self.name} may have '
                       'been deleted externally. Removing from local state.')
        logger.info(f'{colorama.Fore.GREEN}{msg_str}'
                    f'{colorama.Style.RESET_ALL}')

    def _delete_sub_path(self) -> None:
        assert self._bucket_sub_path is not None, 'bucket_sub_path is not set'
        try:
            container_url = data_utils.AZURE_CONTAINER_URL.format(
                storage_account_name=self.storage_account_name,
                container_name=self.name)
            container_client = data_utils.create_az_client(
                client_type='container',
                container_url=container_url,
                storage_account_name=self.storage_account_name,
                resource_group_name=self.resource_group_name)
            # List and delete blobs in the specified directory
            blobs = container_client.list_blobs(
                name_starts_with=self._bucket_sub_path + '/')
            for blob in blobs:
                container_client.delete_blob(blob.name)
            logger.info(
                f'Deleted objects from sub path {self._bucket_sub_path} '
                f'in container {self.name}.')
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f'Failed to delete objects from sub path '
                f'{self._bucket_sub_path} in container {self.name}. '
                f'Details: {common_utils.format_exception(e, use_bracket=True)}'
            )

    def get_handle(self) -> StorageHandle:
        """Returns the Storage Handle object."""
        return self.storage_client.blob_containers.get(
            self.resource_group_name, self.storage_account_name, self.name)

    def batch_az_blob_sync(self,
                           source_path_list: list[Path],
                           create_dirs: bool = False) -> None:
        """Invokes az storage blob sync to batch upload a list of local paths.

        Args:
            source_path_list: List of paths to local files or directories
            create_dirs: If the local_path is a directory and this is set to
                False, the contents of the directory are directly uploaded to
                root of the bucket. If the local_path is a directory and this is
                set to True, the directory is created in the bucket root and
                contents are uploaded to it.
        """
        if self.storage_account_key is None:
            raise RuntimeError('Azure storage account key is not initialized.')
        storage_account_key = self.storage_account_key
        container_path = (f'{self.container_name}/{self._bucket_sub_path}'
                          if self._bucket_sub_path else self.container_name)

        def get_file_sync_command(base_dir_path, file_names) -> str:
            includes_list = ';'.join(file_names)
            includes = f'--include-pattern {shlex.quote(includes_list)}'
            sync_command = (f'az storage blob sync '
                            f'--account-name '
                            f'{shlex.quote(self.storage_account_name)} '
                            f'--account-key '
                            f'{shlex.quote(storage_account_key)} '
                            f'{includes} '
                            '--delete-destination false '
                            f'--source {shlex.quote(base_dir_path)} '
                            f'--container {shlex.quote(container_path)}')
            return sync_command

        def get_dir_sync_command(src_dir_path, dest_dir_name) -> str:
            # we exclude .git directory from the sync
            excluded_list = storage_utils.get_excluded_files(src_dir_path)
            excluded_list.append('.git/')
            excludes_list = ';'.join(
                [file_name.rstrip('*') for file_name in excluded_list])
            excludes = f'--exclude-path {shlex.quote(excludes_list)}'
            if dest_dir_name:
                target_container_path = f'{container_path}/{dest_dir_name}'
            else:
                target_container_path = container_path
            sync_command = (f'az storage blob sync '
                            f'--account-name '
                            f'{shlex.quote(self.storage_account_name)} '
                            f'--account-key '
                            f'{shlex.quote(storage_account_key)} '
                            f'{excludes} '
                            '--delete-destination false '
                            f'--source {shlex.quote(src_dir_path)} '
                            f'--container '
                            f'{shlex.quote(target_container_path)}')
            return sync_command

        # Generate message for upload
        assert source_path_list
        if len(source_path_list) > 1:
            source_message = f'{len(source_path_list)} paths'
        else:
            source_message = source_path_list[0]
        container_endpoint = data_utils.AZURE_CONTAINER_URL.format(
            storage_account_name=self.storage_account_name,
            container_name=container_path)
        log_path = sky_logging.generate_tmp_logging_file_path(
            _STORAGE_LOG_FILE_NAME)
        sync_path = f'{source_message} -> {container_endpoint}/'
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

    def _get_bucket(self) -> tuple[str, bool]:
        """Obtains the AZ Container.

        Buckets for Azure Blob Storage are referred as Containers.
        If the container exists, this method will return the container.
        If the container does not exist, there are three cases:
          1) Raise an error if the container source starts with https://
          2) Return None if container has been externally deleted and
             sync_on_reconstruction is False
          3) Create and return a new container otherwise

        Returns:
            str: name of the bucket(container)
            bool: represents either or not the bucket is managed by skypilot

        Raises:
            StorageBucketCreateError: If creating the container fails
            StorageBucketGetError: If fetching a container fails
            StorageExternalDeletionError: If externally deleted container is
                attempted to be fetched while reconstructing the Storage for
                'sky storage delete' or 'sky start'
        """
        try:
            container_url = data_utils.AZURE_CONTAINER_URL.format(
                storage_account_name=self.storage_account_name,
                container_name=self.name)
            try:
                container_client = data_utils.create_az_client(
                    client_type='container',
                    container_url=container_url,
                    storage_account_name=self.storage_account_name,
                    resource_group_name=self.resource_group_name)
            except azure.exceptions().ClientAuthenticationError as e:
                if 'ERROR: AADSTS50020' in str(e):
                    # Caught when failing to obtain container client due to
                    # lack of permission to passed given private container.
                    if self.resource_group_name is None:
                        with ux_utils.print_exception_no_traceback():
                            raise exceptions.StorageBucketGetError(
                                _BUCKET_FAIL_TO_CONNECT_MESSAGE.format(
                                    name=self.name))
                raise
            if container_client.exists():
                is_private = (True if
                              container_client.get_container_properties().get(
                                  'public_access', None) is None else False)
                # when user attempts to use private container without
                # access rights
                if self.resource_group_name is None and is_private:
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageBucketGetError(
                            _BUCKET_FAIL_TO_CONNECT_MESSAGE.format(
                                name=self.name))
                self._validate_existing_bucket()
                return container_client.container_name, False
            # when the container name does not exist under the provided
            # storage account name and credentials, and user has the rights to
            # access the storage account.
            else:
                # when this if statement is not True, we let it to proceed
                # farther and create the container.
                if (isinstance(self.source, str) and
                        self.source.startswith('https://')):
                    with ux_utils.print_exception_no_traceback():
                        raise exceptions.StorageBucketGetError(
                            'Attempted to use a non-existent container as a '
                            f'source: {self.source}. Please check if the '
                            'container name is correct.')
        except azure.exceptions().ServiceRequestError as e:
            # raised when storage account name to be used does not exist.
            error_message = e.message
            if 'Name or service not known' in error_message:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        'Attempted to fetch the container from non-existent '
                        'storage account '
                        f'name: {self.storage_account_name}. Please check '
                        'if the name is correct.')
            else:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketGetError(
                        'Failed to fetch the container from storage account '
                        f'{self.storage_account_name!r}.'
                        'Details: '
                        f'{common_utils.format_exception(e, use_bracket=True)}')

        # If the container cannot be found in both private and public settings,
        # the container is to be created by Sky. However, creation is skipped
        # if Store object is being reconstructed for deletion or re-mount with
        # sky start, and error is raised instead.
        if self.sync_on_reconstruction:
            container = self._create_az_bucket(self.name)
            return container.name, True

        # Raised when Storage object is reconstructed for sky storage
        # delete or to re-mount Storages with sky start but the storage
        # is already removed externally.
        with ux_utils.print_exception_no_traceback():
            raise exceptions.StorageExternalDeletionError(
                f'Attempted to fetch a non-existent container: {self.name}')

    def mount_command(self, mount_path: str, read_only: bool = False) -> str:
        """Returns the command to mount the container to the mount_path.

        Uses blobfuse2 to mount the container.

        Args:
            mount_path: Path to mount the container to
            read_only: Whether to mount as read-only

        Returns:
            str: a heredoc used to setup the AZ Container mount
        """
        install_cmd = mounting_utils.get_az_mount_install_cmd()
        mount_cmd = mounting_utils.get_az_mount_cmd(self.container_name,
                                                    self.storage_account_name,
                                                    mount_path,
                                                    self.storage_account_key,
                                                    self._bucket_sub_path,
                                                    read_only=read_only)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cmd)

    def mount_cached_command(self,
                             mount_path: str,
                             config: MountCachedConfig | None = None) -> str:
        install_cmd = mounting_utils.get_rclone_install_cmd()
        rclone_profile_name = (
            data_utils.Rclone.RcloneStores.AZURE.get_profile_name(self.name))
        rclone_config = data_utils.Rclone.RcloneStores.AZURE.get_config(
            rclone_profile_name=rclone_profile_name,
            storage_account_name=self.storage_account_name,
            storage_account_key=self.storage_account_key)
        mount_cached_cmd = mounting_utils.get_mount_cached_cmd(
            rclone_config, rclone_profile_name, self.container_name, mount_path,
            config)
        return mounting_utils.get_mounting_command(mount_path, install_cmd,
                                                   mount_cached_cmd)

    def _create_az_bucket(self, container_name: str) -> StorageHandle:
        """Creates AZ Container.

        Args:
            container_name: Name of bucket(container)

        Returns:
            StorageHandle: Handle to interact with the container

        Raises:
            StorageBucketCreateError: If container creation fails.
        """
        try:
            # Container is created under the region which the storage account
            # belongs to.
            container = self.storage_client.blob_containers.create(
                self.resource_group_name,
                self.storage_account_name,
                container_name,
                blob_container={})
            logger.info(f'  {colorama.Style.DIM}Created AZ Container '
                        f'{container_name!r} in {self.region!r} under storage '
                        f'account {self.storage_account_name!r}.'
                        f'{colorama.Style.RESET_ALL}')
        except azure.exceptions().ResourceExistsError as e:
            if 'container is being deleted' in e.error.message:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketCreateError(
                        f'The container {self.name!r} is currently being '
                        'deleted. Please wait for the deletion to complete'
                        'before attempting to create a container with the '
                        'same name. This may take a few minutes.')
            else:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketCreateError(
                        f'Failed to create the container {self.name!r}. '
                        'Details: '
                        f'{common_utils.format_exception(e, use_bracket=True)}')
        return container

    def _delete_az_bucket(self, container_name: str) -> bool:
        """Deletes AZ Container, including all objects in Container.

        Args:
            container_name: Name of bucket(container).

        Returns:
            bool: True if container was deleted, False if it's deleted
                externally.

        Raises:
            StorageBucketDeleteError: If deletion fails for reasons other than
                the container not existing.
        """
        try:
            with rich_utils.safe_status(
                    ux_utils.spinner_message(
                        f'Deleting Azure container {container_name}')):
                # Check for the existence of the container before deletion.
                self.storage_client.blob_containers.get(
                    self.resource_group_name,
                    self.storage_account_name,
                    container_name,
                )
                self.storage_client.blob_containers.delete(
                    self.resource_group_name,
                    self.storage_account_name,
                    container_name,
                )
        except azure.exceptions().ResourceNotFoundError as e:
            if 'Code: ContainerNotFound' in str(e):
                logger.debug(
                    _BUCKET_EXTERNALLY_DELETED_DEBUG_MESSAGE.format(
                        bucket_name=container_name))
                return False
            else:
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.StorageBucketDeleteError(
                        f'Failed to delete Azure container {container_name}. '
                        f'Detailed error: {e}')
        return True


# Preserve the historical public and pickle identities while storage.py remains
# the stable facade for the extracted backend.
AzureBlobStore.__module__ = storage_lib.__name__
AzureBlobStore.AzureBlobStoreMetadata.__module__ = storage_lib.__name__
