"""Translate controller task mounts for remote execution."""
from collections.abc import Callable
import os
import tempfile
import typing
from typing import Any
import uuid

import colorama

from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky.data import data_utils
from sky.data import storage as storage_lib
from sky.server.blob import blob_storage as bs
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky import task as task_lib

# Preserve the historical logger name so structural extraction does not change
# log attribution.
logger = sky_logging.init_logger('sky.utils.controller_utils')


def _generate_run_uuid() -> str:
    """Generates a unique run id for the job."""
    return common_utils.base36_encode(uuid.uuid4().hex)[:8]


def translate_local_file_mounts_to_two_hop(task: 'task_lib.Task',
                                           run_id: str | None = None
                                          ) -> dict[str, str]:
    """Translates local->VM mounts into two-hop file mounts.

    This strategy will upload the local files to the controller first, using a
    normal rsync as part of sky.launch() for the controller. Then, when the
    controller launches the task, it will also use local file_mounts from the
    destination path of the first hop.

    Local machine/API server        Controller              Job cluster
    ------------------------  -----------------------  --------------------
    |      local path  ----|--|-> controller path --|--|-> job dst path   |
    ------------------------  -----------------------  --------------------

    Returns:
        A dict mapping from controller file mount path to local file mount path
          for the first hop. The task is updated in-place to do the second hop.
    """
    first_hop_file_mounts = {}
    second_hop_file_mounts = {}

    if run_id is None:
        run_id = _generate_run_uuid()
    base_tmp_dir = os.path.join(constants.FILE_MOUNTS_CONTROLLER_TMP_BASE_PATH,
                                run_id)

    # Use a simple counter to create unique paths within the base_tmp_dir for
    # each mount.
    file_mount_id = 0

    file_mounts_to_translate = task.file_mounts or {}
    if task.workdir is not None and isinstance(task.workdir, str):
        file_mounts_to_translate[constants.SKY_REMOTE_WORKDIR] = task.workdir
        task.workdir = None

    for job_cluster_path, local_path in file_mounts_to_translate.items():
        if data_utils.is_cloud_store_url(
                local_path) or data_utils.is_cloud_store_url(job_cluster_path):
            raise exceptions.NotSupportedError(
                'Cloud-based file_mounts are specified, but no cloud storage '
                'is available. Please specify local file_mounts only.')

        controller_path = os.path.join(base_tmp_dir, f'{file_mount_id}')
        file_mount_id += 1
        first_hop_file_mounts[controller_path] = local_path
        second_hop_file_mounts[job_cluster_path] = controller_path

    # Use set_file_mounts to override existing file mounts, if they exist.
    task.set_file_mounts(second_hop_file_mounts)

    # Return the first hop info so that it can be added to the jobs-controller
    # YAML.
    return first_hop_file_mounts


# (maybe translate local file mounts) and (sync up)
def maybe_translate_local_file_mounts_and_sync_up(
    task: 'task_lib.Task',
    task_type: str,
    run_id: str | None = None,
    on_storage_mounts_prepared: Callable[['task_lib.Task'], None] | None = None
) -> None:
    """Translates local->VM mounts into Storage->VM, then syncs up any Storage.

    Eagerly syncing up local->Storage ensures Storage->VM would work at task
    launch time.

    If there are no local source paths to be translated, this function would
    still sync up any storage mounts with local source paths (which do not
    undergo translation).

    When jobs.bucket or serve.bucket is not specified, an intermediate storage
    dedicated for the job is created for the workdir and local file mounts and
    the storage is deleted when the job finishes. We don't share the storage
    between jobs, because jobs might have different resources requirements, and
    sharing storage between jobs may cause egress costs or slower transfer
    speeds.

    ``on_storage_mounts_prepared`` runs after every generated storage identity
    has been added to ``task`` but before ``sync_storage_mounts()`` performs an
    external write. Serve uses this boundary to persist crash-safe cleanup
    inventory; other callers retain the existing behavior by leaving it unset.
    """

    # ================================================================
    # Translate the workdir and local file mounts to cloud file mounts.
    # ================================================================

    def _sub_path_join(sub_path: str | None, path: str) -> str:
        if sub_path is None:
            return path
        return os.path.join(sub_path, path).strip('/')

    # We use uuid to generate a unique run id for the job, so that the bucket/
    # subdirectory name is unique across different jobs/services.
    if run_id is None:
        run_id = _generate_run_uuid()
    user_hash = common_utils.get_user_hash()
    original_file_mounts = task.file_mounts if task.file_mounts else {}
    original_storage_mounts = task.storage_mounts if task.storage_mounts else {}

    copy_mounts = task.get_local_to_remote_file_mounts()
    if copy_mounts is None:
        copy_mounts = {}

    has_local_source_paths_file_mounts = bool(copy_mounts)
    has_local_source_paths_workdir = (task.workdir is not None and
                                      isinstance(task.workdir, str))

    msg = None
    if has_local_source_paths_workdir and has_local_source_paths_file_mounts:
        msg = 'workdir and file_mounts with local source paths'
    elif has_local_source_paths_file_mounts:
        msg = 'file_mounts with local source paths'
    elif has_local_source_paths_workdir:
        msg = 'workdir'
    if msg:
        logger.info(
            ux_utils.starting_message(f'Translating {msg} to '
                                      'SkyPilot Storage...'))
        rich_utils.force_update_status(
            ux_utils.spinner_message(
                f'Translating {msg} to SkyPilot Storage...'))

    # Get the bucket name for the workdir and file mounts,
    # we store all these files in same bucket from config.
    bucket_wth_prefix = skypilot_config.get_nested((task_type, 'bucket'), None)
    store_kwargs: dict[str, Any] = {}
    if bucket_wth_prefix is None:
        store_type = sub_path = None
        storage_account_name = region = None
        bucket_name = constants.FILE_MOUNTS_BUCKET_NAME.format(
            username=common_utils.get_cleaned_username(),
            user_hash=user_hash,
            id=run_id)
    else:
        (store_type, bucket_name, sub_path, storage_account_name, region) = (
            storage_lib.StoreType.get_fields_from_store_url(bucket_wth_prefix))
        cloud_str = store_type.to_cloud()
        if (cloud_str not in storage_lib.
                get_cached_enabled_storage_cloud_names_or_refresh()):
            with ux_utils.print_exception_no_traceback():
                raise ValueError(
                    f'`{task_type}.bucket` is specified in SkyPilot config '
                    f'with {bucket_wth_prefix}, but {cloud_str} is not '
                    'enabled. Please avoid specifying the bucket or enable the '
                    f'cloud by: sky check {cloud_str}')

        if storage_account_name is not None:
            store_kwargs['storage_account_name'] = storage_account_name
        if region is not None:
            store_kwargs['region'] = region

    # Step 1: Translate the workdir to SkyPilot storage.
    new_storage_mounts = {}
    if task.workdir is not None and isinstance(task.workdir, str):
        workdir = task.workdir
        task.workdir = None
        if (constants.SKY_REMOTE_WORKDIR in original_file_mounts or
                constants.SKY_REMOTE_WORKDIR in original_storage_mounts):
            raise ValueError(
                f'Cannot mount {constants.SKY_REMOTE_WORKDIR} as both the '
                'workdir and file_mounts contains it as the target.')
        bucket_sub_path = _sub_path_join(
            sub_path,
            constants.FILE_MOUNTS_WORKDIR_SUBPATH.format(run_id=run_id))
        stores = None
        if store_type is not None:
            stores = [store_type]

        storage_obj = storage_lib.Storage(
            name=bucket_name,
            source=workdir,
            persistent=False,
            mode=storage_lib.StorageMode.COPY,
            stores=stores,
            # Set `_is_sky_managed` to False when `bucket_with_prefix` is
            # specified, so that the storage is not deleted when job finishes,
            # but only the sub path is deleted.
            _is_sky_managed=bucket_wth_prefix is None,
            _bucket_sub_path=bucket_sub_path)
        # This generated storage is owned by the operation even when it uses a
        # configured shared bucket. In that case teardown must remove its
        # unique scoped subpath (not the bucket). Persist this before the
        # pre-upload callback so a crash after upload remains cleanable.
        storage_obj.force_delete = True
        new_storage_mounts[constants.SKY_REMOTE_WORKDIR] = storage_obj
        # Check of the existence of the workdir in file_mounts is done in
        # the task construction.
        logger.info(f'  {colorama.Style.DIM}Workdir: {workdir!r} '
                    f'-> storage: {bucket_name!r}.{colorama.Style.RESET_ALL}')

    # Step 2: Translate the local file mounts with folder in src to SkyPilot
    # storage.
    # TODO(zhwu): Optimize this by:
    # 1. Use the same bucket for all the mounts.
    # 2. When the src is the same, use the same bucket.
    copy_mounts_with_file_in_src = {}
    for i, (dst, src) in enumerate(copy_mounts.items()):
        assert task.file_mounts is not None
        task.file_mounts.pop(dst)
        if os.path.isfile(os.path.abspath(os.path.expanduser(src))):
            copy_mounts_with_file_in_src[dst] = src
            continue
        bucket_sub_path = _sub_path_join(
            sub_path, constants.FILE_MOUNTS_SUBPATH.format(i=i, run_id=run_id))
        stores = None
        if store_type is not None:
            stores = [store_type]
        storage_obj = storage_lib.Storage(name=bucket_name,
                                          source=src,
                                          persistent=False,
                                          mode=storage_lib.StorageMode.COPY,
                                          stores=stores,
                                          _is_sky_managed=not bucket_wth_prefix,
                                          _bucket_sub_path=bucket_sub_path)
        storage_obj.force_delete = True
        new_storage_mounts[dst] = storage_obj
        logger.info(f'  {colorama.Style.DIM}Folder : {src!r} '
                    f'-> storage: {bucket_name!r}.{colorama.Style.RESET_ALL}')

    # Step 3: Translate local file mounts with file in src to SkyPilot storage.
    # Hard link the files in src to a temporary directory, and upload folder.
    file_mounts_tmp_subpath = _sub_path_join(
        sub_path, constants.FILE_MOUNTS_TMP_SUBPATH.format(run_id=run_id))
    base_tmp_dir = bs.get_blob_storage().file_mounts_tmp_dir()
    src_to_file_id = {}
    os.makedirs(base_tmp_dir, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=base_tmp_dir) as temp_path:
        local_fm_path = os.path.join(
            temp_path, constants.FILE_MOUNTS_LOCAL_TMP_DIR.format(id=run_id))
        os.makedirs(local_fm_path, exist_ok=True)
        file_mount_remote_tmp_dir = constants.FILE_MOUNTS_REMOTE_TMP_DIR.format(
            task_type)
        if copy_mounts_with_file_in_src:
            for i, src in enumerate(set(copy_mounts_with_file_in_src.values())):
                src_to_file_id[src] = i
                os.link(os.path.abspath(os.path.expanduser(src)),
                        os.path.join(local_fm_path, f'file-{i}'))
            stores = None
            if store_type is not None:
                stores = [store_type]
            storage_obj = storage_lib.Storage(
                name=bucket_name,
                source=local_fm_path,
                persistent=False,
                mode=storage_lib.DEFAULT_STORAGE_MODE,
                stores=stores,
                _is_sky_managed=not bucket_wth_prefix,
                _bucket_sub_path=file_mounts_tmp_subpath)
            storage_obj.force_delete = True

            new_storage_mounts[file_mount_remote_tmp_dir] = storage_obj
            if file_mount_remote_tmp_dir in original_storage_mounts:
                with ux_utils.print_exception_no_traceback():
                    raise ValueError(
                        'Failed to translate file mounts, due to the default '
                        f'destination {file_mount_remote_tmp_dir} '
                        'being taken.')
            sources = list(src_to_file_id.keys())
            sources_str = '\n    '.join(sources)
            logger.info(f'  {colorama.Style.DIM}Files (listed below) '
                        f' -> storage: {bucket_name}:'
                        f'\n    {sources_str}{colorama.Style.RESET_ALL}')

        rich_utils.force_update_status(
            ux_utils.spinner_message(
                'Uploading translated local files/folders'))
        task.update_storage_mounts(new_storage_mounts)

        if on_storage_mounts_prepared is not None:
            on_storage_mounts_prepared(task)

        # Step 4: Upload storage from sources
        # Upload the local source to a bucket. The task will not be executed
        # locally, so we need to upload the files/folders to the bucket manually
        # here before sending the task to the remote jobs controller.  This will
        # also upload any storage mounts that are not translated.  After
        # sync_storage_mounts, we will also have file_mounts in the task, but
        # these aren't used since the storage_mounts for the same paths take
        # precedence.
        if task.storage_mounts:
            # There may be existing (non-translated) storage mounts, so log this
            # whenever task.storage_mounts is non-empty.
            rich_utils.force_update_status(
                ux_utils.spinner_message(
                    'Uploading local sources to storage[/]  '
                    '[dim]View storages: sky storage ls'))
        try:
            task.sync_storage_mounts(
                on_storage_plan_prepared=on_storage_mounts_prepared)
        except (ValueError, exceptions.NoCloudAccessError) as e:
            if 'No enabled cloud for storage' in str(e) or isinstance(
                    e, exceptions.NoCloudAccessError):
                data_src = None
                if has_local_source_paths_file_mounts:
                    data_src = 'file_mounts'
                if has_local_source_paths_workdir:
                    if data_src:
                        data_src += ' and workdir'
                    else:
                        data_src = 'workdir'
                store_enabled_clouds = ', '.join(
                    storage_lib.STORE_ENABLED_CLOUDS)
                with ux_utils.print_exception_no_traceback():
                    raise exceptions.NotSupportedError(
                        f'Unable to use {data_src} - no cloud with object '
                        'store support is enabled. Please enable at least one '
                        'cloud with object store support '
                        f'({store_enabled_clouds}) by running `sky check`, or '
                        f'remove {data_src} from your task.'
                        '\nHint: If you do not have any cloud access, you may '
                        'still download data and code over the network using '
                        'curl or other tools in the `setup` section of the '
                        'task.') from None

    # Step 5: Add the file download into the file mounts, such as
    #  /original-dst: s3://spot-fm-file-only-bucket-name/file-0
    new_file_mounts = {}
    if copy_mounts_with_file_in_src:
        # file_mount_remote_tmp_dir will only exist when there are files in
        # the src for copy mounts.
        storage_obj = task.storage_mounts[file_mount_remote_tmp_dir]
        assert storage_obj.stores, (storage_obj.__dict__, task.to_yaml_config())
        curr_store_type = list(storage_obj.stores.keys())[0]
        store_object = storage_obj.stores[curr_store_type]
        assert store_object is not None, (storage_obj.__dict__,
                                          task.to_yaml_config())
        bucket_url = storage_lib.StoreType.get_endpoint_url(
            store_object, bucket_name)
        bucket_url += f'/{file_mounts_tmp_subpath}'
        for dst, src in copy_mounts_with_file_in_src.items():
            file_id = src_to_file_id[src]
            new_file_mounts[dst] = bucket_url + f'/file-{file_id}'
    task.update_file_mounts(new_file_mounts)

    # Step 6: Replace the source field that is local path in all storage_mounts
    # with bucket URI and remove the name field.
    for storage_obj in task.storage_mounts.values():
        if (storage_obj.source is not None and
                not data_utils.is_cloud_store_url(storage_obj.source)):
            # Need to replace the local path with bucket URI, and remove the
            # name field, so that the storage mount can work on the jobs
            # controller.
            store_types = list(storage_obj.stores.keys())
            assert len(store_types) == 1, (
                'We only support one store type for now.', storage_obj.stores)
            curr_store_type = store_types[0]
            store_object = storage_obj.stores[curr_store_type]
            assert store_object is not None and storage_obj.name is not None, (
                store_object, storage_obj.name)
            storage_obj.source = storage_lib.StoreType.get_endpoint_url(
                store_object, storage_obj.name)
            storage_obj.force_delete = True

    # Step 7: Convert all `MOUNT` mode storages which don't specify a source
    # to specifying a source. If the source is specified with a local path,
    # it was handled in step 6.
    updated_mount_storages = {}
    for storage_path, storage_obj in task.storage_mounts.items():
        if (storage_obj.mode in storage_lib.MOUNTABLE_STORAGE_MODES and
                not storage_obj.source):
            # Construct source URL with first store type and storage name
            # E.g., s3://my-storage-name
            store_types = list(storage_obj.stores.keys())
            assert len(store_types) == 1, (
                'We only support one store type for now.', storage_obj.stores)
            curr_store_type = store_types[0]
            store_object = storage_obj.stores[curr_store_type]
            assert store_object is not None and storage_obj.name is not None, (
                store_object, storage_obj.name)
            source = storage_lib.StoreType.get_endpoint_url(
                store_object, storage_obj.name)
            assert store_object is not None and storage_obj.name is not None, (
                store_object, storage_obj.name)
            new_storage = storage_lib.Storage.from_yaml_config({
                'source': source,
                'persistent': storage_obj.persistent,
                'mode': storage_obj.mode.value,
                # We enable force delete to allow the controller to delete
                # the object store in case persistent is set to False.
                '_force_delete': True
            })
            updated_mount_storages[storage_path] = new_storage
    task.update_storage_mounts(updated_mount_storages)
    if msg:
        logger.info(ux_utils.finishing_message('Uploaded local files/folders.'))
