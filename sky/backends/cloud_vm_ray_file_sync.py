"""File synchronization helpers for the CloudVmRay backend."""

import os
import time
import typing
from typing import Any

import colorama

from sky import cloud_stores
from sky import sky_logging
from sky.backends import backend_utils
from sky.data import data_utils
from sky.skylet import constants
from sky.utils import command_runner
from sky.utils import rich_utils
from sky.utils import subprocess_utils
from sky.utils import ux_utils

if typing.TYPE_CHECKING:
    from sky.backends import cloud_vm_ray_backend

Path = str

_PATH_SIZE_MEGABYTES_WARN_THRESHOLD = 256
_SKY_REMOTE_WORKDIR = constants.SKY_REMOTE_WORKDIR

# Preserve the existing logger identity so moving these helpers does not alter
# log filtering or presentation.
logger = sky_logging.init_logger('sky.backends.cloud_vm_ray_backend')


def sync_workdir(
    handle: 'cloud_vm_ray_backend.CloudVmRayResourceHandle',
    workdir: Path | dict[str, Any],
    envs_and_secrets: dict[str, str],
    log_dir: str,
) -> None:
    """Synchronize a local path or Git workdir to every cluster node."""
    if handle.provision_runtime_metadata.workdir_synced:
        logger.info('Skipping workdir sync: provisioner reported ready.')
        return

    # Even though provision() takes care of it, there may be cases where this
    # function is called in isolation, without calling provision(), e.g., in
    # CLI. So we should rerun rsync_up.
    if isinstance(workdir, dict):
        _sync_git_workdir(handle, envs_and_secrets, log_dir)
    else:
        _sync_path_workdir(handle, workdir, log_dir)


def _sync_git_workdir(
    handle: 'cloud_vm_ray_backend.CloudVmRayResourceHandle',
    envs_and_secrets: dict[str, str],
    log_dir: str,
) -> None:
    style = colorama.Style
    ip_list = handle.external_ips()
    assert ip_list is not None, 'external_ips is not cached in handle'

    log_path = os.path.join(log_dir, 'workdir_sync.log')

    # TODO(zhwu): refactor this with backend_utils.parallel_cmd_with_rsync
    runners = handle.get_command_runners()

    def _sync_git_workdir_node(runner: command_runner.CommandRunner) -> None:
        # Type assertion to help mypy understand the type.
        assert hasattr(
            runner, 'git_clone'
        ), f'CommandRunner should have git_clone method, got {type(runner)}'
        runner.git_clone(
            target_dir=_SKY_REMOTE_WORKDIR,
            log_path=log_path,
            stream_logs=False,
            max_retry=3,
            envs_and_secrets=envs_and_secrets,
        )

    num_nodes = handle.launched_nodes
    plural = 's' if num_nodes > 1 else ''
    logger.info(f'  {style.DIM}Syncing workdir (to {num_nodes} node{plural}): '
                f'{_SKY_REMOTE_WORKDIR}{style.RESET_ALL}')
    os.makedirs(os.path.expanduser(log_dir), exist_ok=True)
    os.system(f'touch {log_path}')
    num_threads = subprocess_utils.get_parallel_threads(
        str(handle.launched_resources.cloud))
    with rich_utils.safe_status(
            ux_utils.spinner_message('Syncing workdir', log_path)):
        subprocess_utils.run_in_parallel(_sync_git_workdir_node, runners,
                                         num_threads)
    logger.info(ux_utils.finishing_message('Synced workdir.', log_path))


def _sync_path_workdir(
    handle: 'cloud_vm_ray_backend.CloudVmRayResourceHandle',
    workdir: Path,
    log_dir: str,
) -> None:
    fore = colorama.Fore
    style = colorama.Style
    ip_list = handle.external_ips()
    assert ip_list is not None, 'external_ips is not cached in handle'
    full_workdir = os.path.abspath(os.path.expanduser(workdir))

    # These asserts have been validated at Task construction time.
    assert os.path.exists(full_workdir), f'{full_workdir} does not exist'
    if os.path.islink(full_workdir):
        logger.warning(f'{fore.YELLOW}Workdir {workdir!r} is a symlink. '
                       f'Symlink contents are not uploaded.{style.RESET_ALL}')
    else:
        assert os.path.isdir(
            full_workdir), f'{full_workdir} should be a directory.'

    # Raise warning if directory is too large.
    dir_size = backend_utils.path_size_megabytes(full_workdir)
    if dir_size >= _PATH_SIZE_MEGABYTES_WARN_THRESHOLD:
        logger.warning(
            f'  {fore.YELLOW}The size of workdir {workdir!r} '
            f'is {dir_size} MB. Try to keep workdir small or use '
            '.skyignore to exclude large files, as large sizes will slow '
            f'down rsync.{style.RESET_ALL}')

    log_path = os.path.join(log_dir, 'workdir_sync.log')

    # TODO(zhwu): refactor this with backend_utils.parallel_cmd_with_rsync
    runners = handle.get_command_runners()

    def _sync_workdir_node(runner: command_runner.CommandRunner) -> None:
        runner.rsync(
            source=workdir,
            target=_SKY_REMOTE_WORKDIR,
            up=True,
            log_path=log_path,
            stream_logs=False,
        )

    num_nodes = handle.launched_nodes
    plural = 's' if num_nodes > 1 else ''
    logger.info(f'  {style.DIM}Syncing workdir (to {num_nodes} node{plural}): '
                f'{workdir} -> {_SKY_REMOTE_WORKDIR}{style.RESET_ALL}')
    os.makedirs(os.path.expanduser(log_dir), exist_ok=True)
    os.system(f'touch {log_path}')
    num_threads = subprocess_utils.get_parallel_threads(
        str(handle.launched_resources.cloud))
    with rich_utils.safe_status(
            ux_utils.spinner_message('Syncing workdir', log_path)):
        subprocess_utils.run_in_parallel(_sync_workdir_node, runners,
                                         num_threads)
    logger.info(ux_utils.finishing_message('Synced workdir.', log_path))


def download_file(handle: 'cloud_vm_ray_backend.CloudVmRayResourceHandle',
                  local_file_path: str, remote_file_path: str) -> None:
    """Synchronize a file from the cluster head to the local machine."""
    runners = handle.get_command_runners()
    head_runner = runners[0]
    head_runner.rsync(
        source=local_file_path,
        target=remote_file_path,
        up=False,
        stream_logs=False,
    )


def execute_file_mounts(
    handle: 'cloud_vm_ray_backend.CloudVmRayResourceHandle',
    file_mounts: dict[Path, Path] | None,
    log_dir: str,
) -> None:
    """Synchronize local files and remote-store objects to cluster nodes."""
    # File mounts handling for remote paths possibly without write access:
    # (1) in file_mounts sections, add <prefix> to these target paths;
    # (2) create symlinks from '/.../file' to '<prefix>/.../file'.
    if file_mounts is None or not file_mounts:
        return
    symlink_commands = []
    fore = colorama.Fore
    style = colorama.Style
    start = time.time()
    runners = handle.get_command_runners()
    log_path = os.path.join(log_dir, 'file_mounts.log')
    num_threads = subprocess_utils.get_max_workers_for_file_mounts(
        file_mounts, str(handle.launched_resources.cloud))

    # Check the files and warn.
    for _, src in file_mounts.items():
        if not data_utils.is_cloud_store_url(src):
            full_src = os.path.abspath(os.path.expanduser(src))
            # Checked during Task.set_file_mounts().
            assert os.path.exists(
                full_src), f'{full_src} does not exist. {file_mounts}'
            src_size = backend_utils.path_size_megabytes(full_src)
            if src_size >= _PATH_SIZE_MEGABYTES_WARN_THRESHOLD:
                logger.warning(
                    f'  {fore.YELLOW}The size of file mount src {src!r} '
                    f'is {src_size} MB. Try to keep src small or use '
                    '.skyignore to exclude large files, as large sizes will '
                    f'slow down rsync. {style.RESET_ALL}')
            if os.path.islink(full_src):
                logger.warning(
                    f'  {fore.YELLOW}Source path {src!r} is a symlink. '
                    f'Symlink contents are not uploaded.{style.RESET_ALL}')

    os.makedirs(os.path.expanduser(log_dir), exist_ok=True)
    os.system(f'touch {log_path}')

    rich_utils.force_update_status(
        ux_utils.spinner_message('Syncing file mounts', log_path))

    for dst, src in file_mounts.items():
        # TODO: room for improvement. Here there are many moving parts
        # (download gsutil on remote, run gsutil on remote). Consider
        # alternatives (smart_open, each provider's own sdk), a data-transfer
        # container etc.
        if not os.path.isabs(dst) and not dst.startswith('~/'):
            dst = f'{_SKY_REMOTE_WORKDIR}/{dst}'
        # Sync src to wrapped_dst, a safe-to-write wrapped path.
        wrapped_dst = dst
        if not dst.startswith('~/') and not dst.startswith('/tmp/'):
            wrapped_dst = backend_utils.FileMountHelper.wrap_file_mount(dst)
            cmd = backend_utils.FileMountHelper.make_safe_symlink_command(
                source=dst, target=wrapped_dst)
            symlink_commands.append(cmd)

        if not data_utils.is_cloud_store_url(src):
            full_src = os.path.abspath(os.path.expanduser(src))

            if os.path.isfile(full_src):
                mkdir_for_wrapped_dst = (
                    f'mkdir -p {os.path.dirname(wrapped_dst)}')
            else:
                mkdir_for_wrapped_dst = f'mkdir -p {wrapped_dst}'

            # TODO(mluo): Fix method so that mkdir and rsync run together.
            backend_utils.parallel_data_transfer_to_nodes(
                runners,
                source=src,
                target=wrapped_dst,
                cmd=mkdir_for_wrapped_dst,
                run_rsync=True,
                action_message='Syncing',
                log_path=log_path,
                stream_logs=False,
                num_threads=num_threads,
            )
            continue

        storage = cloud_stores.get_storage_from_path(src)
        if storage.is_directory(src):
            sync_cmd = storage.make_sync_dir_command(source=src,
                                                     destination=wrapped_dst)
            mkdir_for_wrapped_dst = f'mkdir -p {wrapped_dst}'
        else:
            sync_cmd = storage.make_sync_file_command(source=src,
                                                      destination=wrapped_dst)
            mkdir_for_wrapped_dst = (f'mkdir -p {os.path.dirname(wrapped_dst)}')

        command = ' && '.join([mkdir_for_wrapped_dst, sync_cmd])
        # dst is only used for message printing.
        backend_utils.parallel_data_transfer_to_nodes(
            runners,
            source=src,
            target=dst,
            cmd=command,
            run_rsync=False,
            action_message='Syncing',
            log_path=log_path,
            stream_logs=False,
            # Cloud-specific CLI or SDK tools may require PATH from bashrc.
            source_bashrc=True,
            num_threads=num_threads,
        )

    symlink_command = ' && '.join(symlink_commands)
    if symlink_command:
        # ALIAS_SUDO_TO_EMPTY_FOR_ROOT_CMD sets sudo to empty string for root.
        # This is needed because commands do not source bashrc here.
        symlink_command = (
            f'{command_runner.ALIAS_SUDO_TO_EMPTY_FOR_ROOT_CMD} && '
            f'{symlink_command}')

        def _symlink_node(runner: command_runner.CommandRunner):
            returncode = runner.run(symlink_command, log_path=log_path)
            subprocess_utils.handle_returncode(
                returncode, symlink_command,
                'Failed to create symlinks. The target destination '
                f'may already exist. Log: {log_path}')

        subprocess_utils.run_in_parallel(_symlink_node, runners, num_threads)
    end = time.time()
    logger.debug(f'File mount sync took {end - start} seconds.')
    logger.info(ux_utils.finishing_message('Synced file_mounts.', log_path))
