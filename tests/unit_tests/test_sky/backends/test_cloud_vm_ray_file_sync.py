"""Characterization tests for CloudVmRayBackend file synchronization."""

import contextlib
from pathlib import Path
from unittest import mock

from sky.backends import cloud_vm_ray_backend
from sky.backends import cloud_vm_ray_file_sync


def _run_sequentially(function, args, num_threads=None):
    del num_threads
    return [function(arg) for arg in args]


def _make_handle(*, workdir_synced=False, file_mounts_synced=False):
    handle = mock.MagicMock()
    handle.provision_runtime_metadata.workdir_synced = workdir_synced
    handle.provision_runtime_metadata.file_mounts_synced = file_mounts_synced
    handle.external_ips.return_value = ['203.0.113.1', '203.0.113.2']
    handle.launched_nodes = 2
    handle.launched_resources = mock.Mock(unsafe=True)
    handle.launched_resources.cloud = 'test-cloud'
    return handle


def test_sync_workdir_skips_when_provisioner_already_synced():
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = _make_handle(workdir_synced=True)

    backend.sync_workdir(handle, '/tmp/workdir', {})

    handle.external_ips.assert_not_called()
    handle.get_command_runners.assert_not_called()


@mock.patch.object(cloud_vm_ray_file_sync.os, 'system', return_value=0)
@mock.patch.object(cloud_vm_ray_file_sync.rich_utils,
                   'safe_status',
                   side_effect=lambda *args, **kwargs: contextlib.nullcontext())
@mock.patch.object(cloud_vm_ray_file_sync.subprocess_utils,
                   'get_parallel_threads',
                   return_value=2)
@mock.patch.object(cloud_vm_ray_file_sync.subprocess_utils,
                   'run_in_parallel',
                   side_effect=_run_sequentially)
def test_sync_git_workdir_clones_on_every_node(run_in_parallel, _threads,
                                               _status, _system, tmp_path):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    backend.log_dir = str(tmp_path / 'logs')
    handle = _make_handle()
    runners = [mock.MagicMock(), mock.MagicMock()]
    handle.get_command_runners.return_value = runners
    envs_and_secrets = {'TOKEN': 'secret'}

    backend.sync_workdir(handle, {'url': 'https://example.test/repo.git'},
                         envs_and_secrets)

    log_path = str(tmp_path / 'logs' / 'workdir_sync.log')
    for runner in runners:
        runner.git_clone.assert_called_once_with(
            target_dir=cloud_vm_ray_backend.SKY_REMOTE_WORKDIR,
            log_path=log_path,
            stream_logs=False,
            max_retry=3,
            envs_and_secrets=envs_and_secrets,
        )
    run_in_parallel.assert_called_once()


@mock.patch.object(cloud_vm_ray_file_sync.os, 'system', return_value=0)
@mock.patch.object(cloud_vm_ray_file_sync.rich_utils,
                   'safe_status',
                   side_effect=lambda *args, **kwargs: contextlib.nullcontext())
@mock.patch.object(cloud_vm_ray_file_sync.backend_utils,
                   'path_size_megabytes',
                   return_value=1)
@mock.patch.object(cloud_vm_ray_file_sync.subprocess_utils,
                   'get_parallel_threads',
                   return_value=2)
@mock.patch.object(cloud_vm_ray_file_sync.subprocess_utils,
                   'run_in_parallel',
                   side_effect=_run_sequentially)
def test_sync_path_workdir_rsyncs_on_every_node(run_in_parallel, _threads,
                                                _path_size, _status, _system,
                                                tmp_path):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    backend.log_dir = str(tmp_path / 'logs')
    handle = _make_handle()
    runners = [mock.MagicMock(), mock.MagicMock()]
    handle.get_command_runners.return_value = runners
    workdir = tmp_path / 'workdir'
    workdir.mkdir()

    backend.sync_workdir(handle, str(workdir), {})

    log_path = str(tmp_path / 'logs' / 'workdir_sync.log')
    for runner in runners:
        runner.rsync.assert_called_once_with(
            source=str(workdir),
            target=cloud_vm_ray_backend.SKY_REMOTE_WORKDIR,
            up=True,
            log_path=log_path,
            stream_logs=False,
        )
    run_in_parallel.assert_called_once()


@mock.patch.object(cloud_vm_ray_file_sync.os, 'system', return_value=0)
@mock.patch.object(cloud_vm_ray_file_sync.rich_utils, 'force_update_status')
@mock.patch.object(cloud_vm_ray_backend.rich_utils,
                   'safe_status',
                   side_effect=lambda *args, **kwargs: contextlib.nullcontext())
@mock.patch.object(cloud_vm_ray_backend.controller_utils,
                   'replace_skypilot_config_path_in_file_mounts')
@mock.patch.object(cloud_vm_ray_file_sync.backend_utils,
                   'path_size_megabytes',
                   return_value=1)
@mock.patch.object(cloud_vm_ray_file_sync.subprocess_utils,
                   'get_max_workers_for_file_mounts',
                   return_value=1)
@mock.patch.object(cloud_vm_ray_file_sync.backend_utils,
                   'parallel_data_transfer_to_nodes')
def test_sync_file_mounts_transfers_local_file(transfer, _workers, _path_size,
                                               replace_config_mount, _status,
                                               _force_status, _system,
                                               tmp_path):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    backend.log_dir = str(tmp_path / 'logs')
    handle = _make_handle()
    runner = mock.MagicMock()
    runner.run.return_value = 0
    handle.get_command_runners.return_value = [runner]
    handle.launched_resources.assert_launchable.return_value.cloud = (
        'test-cloud')
    source = tmp_path / 'source.txt'
    source.write_text('payload')
    file_mounts = {'~/data/source.txt': str(source)}

    with mock.patch.object(backend, '_execute_storage_mounts') as storage, \
         mock.patch.object(backend, '_set_storage_mounts_metadata') as metadata:
        backend.sync_file_mounts(handle, file_mounts, {})

    replace_config_mount.assert_called_once_with('test-cloud', file_mounts)
    transfer.assert_called_once_with(
        [runner],
        source=str(source),
        target='~/data/source.txt',
        cmd='mkdir -p ~/data',
        run_rsync=True,
        action_message='Syncing',
        log_path=str(tmp_path / 'logs' / 'file_mounts.log'),
        stream_logs=False,
        num_threads=1,
    )
    storage.assert_called_once_with(handle, {})
    metadata.assert_called_once_with(handle.cluster_name, {})


@mock.patch.object(cloud_vm_ray_file_sync.os, 'system', return_value=0)
@mock.patch.object(cloud_vm_ray_file_sync.rich_utils, 'force_update_status')
@mock.patch.object(cloud_vm_ray_backend.rich_utils,
                   'safe_status',
                   side_effect=lambda *args, **kwargs: contextlib.nullcontext())
@mock.patch.object(cloud_vm_ray_backend.controller_utils,
                   'replace_skypilot_config_path_in_file_mounts')
@mock.patch.object(cloud_vm_ray_file_sync.subprocess_utils,
                   'get_max_workers_for_file_mounts',
                   return_value=1)
@mock.patch.object(cloud_vm_ray_file_sync.subprocess_utils,
                   'run_in_parallel',
                   side_effect=_run_sequentially)
@mock.patch.object(cloud_vm_ray_file_sync.backend_utils,
                   'parallel_data_transfer_to_nodes')
@mock.patch.object(cloud_vm_ray_file_sync.cloud_stores, 'get_storage_from_path')
def test_sync_file_mounts_downloads_cloud_directory(get_storage, transfer,
                                                    _run_in_parallel, _workers,
                                                    replace_config_mount,
                                                    _status, _force_status,
                                                    _system, tmp_path):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    backend.log_dir = str(tmp_path / 'logs')
    handle = _make_handle()
    runner = mock.MagicMock()
    runner.run.return_value = 0
    handle.get_command_runners.return_value = [runner]
    handle.launched_resources.assert_launchable.return_value.cloud = (
        'test-cloud')
    source = 's3://test-bucket/models'
    destination = '/models'
    file_mounts = {destination: source}
    storage = get_storage.return_value
    storage.is_directory.return_value = True
    storage.make_sync_dir_command.return_value = 'sync-cloud-directory'
    wrapped_destination = (cloud_vm_ray_file_sync.backend_utils.FileMountHelper.
                           wrap_file_mount(destination))

    with mock.patch.object(backend, '_execute_storage_mounts'), \
         mock.patch.object(backend, '_set_storage_mounts_metadata'):
        backend.sync_file_mounts(handle, file_mounts, {})

    replace_config_mount.assert_called_once_with('test-cloud', file_mounts)
    get_storage.assert_called_once_with(source)
    storage.make_sync_dir_command.assert_called_once_with(
        source=source, destination=wrapped_destination)
    transfer.assert_called_once_with(
        [runner],
        source=source,
        target=destination,
        cmd=f'mkdir -p {wrapped_destination} && sync-cloud-directory',
        run_rsync=False,
        action_message='Syncing',
        log_path=str(tmp_path / 'logs' / 'file_mounts.log'),
        stream_logs=False,
        source_bashrc=True,
        num_threads=1,
    )
    runner.run.assert_called_once()
    symlink_command = runner.run.call_args.args[0]
    assert destination in symlink_command
    assert wrapped_destination in symlink_command


def test_download_file_uses_head_runner_only(tmp_path):
    backend = cloud_vm_ray_backend.CloudVmRayBackend()
    handle = _make_handle()
    head_runner, worker_runner = mock.MagicMock(), mock.MagicMock()
    handle.get_command_runners.return_value = [head_runner, worker_runner]
    local_path = str(Path(tmp_path) / 'download.txt')

    backend.download_file(handle, local_path, '/remote/source.txt')

    head_runner.rsync.assert_called_once_with(
        source=local_path,
        target='/remote/source.txt',
        up=False,
        stream_logs=False,
    )
    worker_runner.rsync.assert_not_called()
