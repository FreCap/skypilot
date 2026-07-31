"""Characterization tests for the ``sky image`` command family."""

import ast
import hashlib
import inspect
import json
import pathlib
import types
from unittest import mock

import click
from click import testing as click_testing
import pytest

from sky.client.cli import command
from sky.client.cli import images

_COMMAND_NAMES = (
    'image',
    'image_publish',
    'image_status',
    'image_prepare',
    'image_retry',
    'image_profile',
    'image_profile_qualify',
    'image_profile_canary',
)
_BODY_HASHES = {
    'image': 'd8941125789cae216e8b9b4aa756972dc9ef8341295f72fe8d32972f37bd7273',
    'image_publish': 'a2064359cafbc1c5d08af7e3d6bdc3ee096299ed262d70b4f0ed8c05e9a0b7ab',
    'image_status': '7e447c205e9b27f0e49833c45fcc66aafeb3efaca13e0d422411684a4712e298',
    'image_prepare': '844008f75f057a6afc1721bc79bd0451b18459944ccb57b859bd397bd4f9412f',
    'image_retry': 'd99cb8e1a4798a0bb55517a26ee35336c97594ec9c4dc14fa4e1cc6340489ab5',
    'image_profile': 'bfaee79e74c907d8f4ca3c1e92823beea3577f235b93f1ebc5a730985d064e3f',
    'image_profile_qualify': '5ea5716a25c03ee6a06e4991d86e7194bf5e2e66b3ac94175f839ec1e58140c7',
    'image_profile_canary': 'ccaa899ea7b459004b98d68ddcfc8525fb76a78c29ef249a3a1591985e44c032',
}
_HELP_HASHES = {
    ('image',): '8fa44ebb61178eb4c65ddc6da584747e5e1cf73ec0a9e19c5608567876fab1ae',
    ('image', 'publish'): '71cd0792c25203a16eac024f29a93476a3f16c96183b8a8153ebcbc560f55079',
    ('image', 'status'): '82329fe0fe3abd1b48f7a985df843fa6843ac5deabd3c56ea2502f1ad860630a',
    ('image', 'prepare'): 'd3444527dd58af0561488883bae7d8a1b4b720cf568c588fee5daf0930bddd2d',
    ('image', 'retry'): 'ff9024387339534f05bba72db2f46970040738fbeecfa556f244d8fd09f328cf',
    ('image', 'profile'): 'cd1f50d9c0e5e598ca82ce74a678678d39db159ba4f49934c79e630d052638e5',
    ('image', 'profile', 'qualify'): 'b398d956e82c79b56363c6d60b321e6c2902907c2dfe5f5cf806957791080de2',
    ('image', 'profile', 'canary'): '3669a3a4afb6127fcdc21225a9aa63a75b8b8b1530e0ff5561eb8efcb89c9f50',
}


def _callback(name: str):
    callback = getattr(command, name).callback
    assert callback is not None
    return inspect.unwrap(callback)


def _body_hash(name: str) -> str:
    tree = ast.parse(inspect.getsource(_callback(name)))
    function = next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)))
    for node in ast.walk(function):
        if hasattr(node, 'type_params'):
            node.type_params = []
    body = ast.Module(body=function.body, type_ignores=[])
    return hashlib.sha256(ast.dump(body).encode()).hexdigest()


def test_image_command_hierarchy_and_facade_metadata() -> None:
    root_commands = command.cli.list_commands(click.Context(command.cli))
    image_index = root_commands.index('image')

    assert root_commands[image_index - 1:image_index +
                         2] == ['storage', 'image', 'volumes']
    assert command.cli.commands['image'] is command.image
    assert command.image.list_commands(click.Context(
        command.image)) == ['publish', 'status', 'prepare', 'retry', 'profile']
    assert command.image_profile.list_commands(
        click.Context(command.image_profile)) == ['qualify', 'canary']
    assert command.image.commands['publish'] is command.image_publish
    assert command.image.commands['status'] is command.image_status
    assert command.image.commands['prepare'] is command.image_prepare
    assert command.image.commands['retry'] is command.image_retry
    assert command.image.commands['profile'] is command.image_profile
    assert command.image_profile.commands[
        'qualify'] is command.image_profile_qualify
    assert command.image_profile.commands[
        'canary'] is command.image_profile_canary
    for name in _COMMAND_NAMES:
        callback = getattr(command, name).callback
        assert callback is not None
        assert callback.__module__ == command.__name__
        assert callback.__qualname__ == name


def test_image_commands_are_direct_facade_aliases() -> None:
    for name in _COMMAND_NAMES:
        assert getattr(command, name) is getattr(images, name)
    assert command.container_images_sdk is images.container_images_sdk
    assert command.container_image_models is images.container_image_models


@pytest.mark.parametrize('name', _COMMAND_NAMES)
def test_image_callback_body_is_unchanged(name: str) -> None:
    assert _body_hash(name) == _BODY_HASHES[name]


@pytest.mark.parametrize('path, expected_hash', _HELP_HASHES.items())
def test_image_help_is_unchanged(path: tuple[str, ...],
                                 expected_hash: str) -> None:
    result = click_testing.CliRunner().invoke(command.cli, [*path, '--help'])

    assert result.exit_code == 0, result.output
    assert hashlib.sha256(result.output.encode()).hexdigest() == expected_hash


def test_image_publish_projects_digest_and_wait(monkeypatch) -> None:
    publish = mock.Mock(return_value=object())
    formatter = mock.Mock(return_value='published')
    monkeypatch.setattr(command.container_images_sdk, 'publish', publish)
    monkeypatch.setattr(command.table_utils, 'format_container_image_mutation',
                        formatter)

    _callback('image_publish')(f'registry/repo@sha256:{"a" * 64}', 'release-a',
                               'dist-a', 'linux/arm64', 'source-a', True,
                               'workspace-a')

    publish.assert_called_once_with(f'registry/repo@sha256:{"a" * 64}',
                                    'release-a',
                                    'dist-a',
                                    workspace='workspace-a',
                                    platform='linux/arm64',
                                    source_auth='source-a',
                                    wait=False)
    formatter.assert_called_once_with(publish.return_value)


def test_image_status_and_prepare_project_sdk_results(monkeypatch) -> None:
    artifact = types.SimpleNamespace(id='artifact-a')
    status = mock.Mock(return_value=[artifact])
    prepare = mock.Mock(return_value=object())
    table_formatter = mock.Mock(return_value='table')
    mutation_formatter = mock.Mock(return_value='prepared')
    monkeypatch.setattr(command.container_images_sdk, 'status', status)
    monkeypatch.setattr(command.container_images_sdk, 'prepare', prepare)
    monkeypatch.setattr(command.table_utils, 'format_container_image_table',
                        table_formatter)
    monkeypatch.setattr(command.table_utils, 'format_container_image_mutation',
                        mutation_formatter)

    _callback('image_status')('release=release-a', 'workspace-a')
    _callback('image_prepare')('release=release-a', 'target-a', 'dist-a', True,
                               'workspace-a')

    status.assert_has_calls([
        mock.call('release=release-a', workspace='workspace-a'),
        mock.call('release=release-a', workspace='workspace-a'),
    ])
    prepare.assert_called_once_with('artifact-a',
                                    'dist-a',
                                    'target-a',
                                    workspace='workspace-a',
                                    wait=False)
    table_formatter.assert_called_once_with([artifact])
    mutation_formatter.assert_called_once_with(prepare.return_value)


def test_image_retry_prefers_one_failed_publication(monkeypatch) -> None:
    page = types.SimpleNamespace(items=[{
        'id': 'publication-a',
        'state': 'FAILED'
    }])
    publications = mock.Mock(return_value=page)
    retry_publication = mock.Mock(return_value=object())
    status = mock.Mock()
    monkeypatch.setattr(command.container_images_sdk, 'publications',
                        publications)
    monkeypatch.setattr(command.container_images_sdk, 'retry_publication',
                        retry_publication)
    monkeypatch.setattr(command.container_images_sdk, 'status', status)
    monkeypatch.setattr(command.table_utils, 'format_container_image_mutation',
                        mock.Mock())

    _callback('image_retry')('release=release-a', 'target-a', 'dist-a', True,
                             'workspace-a')

    publications.assert_called_once_with(workspace='workspace-a',
                                         release='release-a',
                                         limit=100)
    retry_publication.assert_called_once_with('publication-a',
                                              workspace='workspace-a',
                                              wait=False)
    status.assert_not_called()


def test_image_retry_projects_one_retryable_location(monkeypatch) -> None:
    artifact_id = '12345678-1234-4567-8234-567812345678'
    artifact = types.SimpleNamespace(id=artifact_id)
    location = {
        'id': 'location-a',
        'distribution': 'dist-a',
        'target_id': 'target-a',
        'state': 'MISSING',
    }
    monkeypatch.setattr(command.container_images_sdk, 'status',
                        mock.Mock(return_value=[artifact]))
    monkeypatch.setattr(
        command.container_images_sdk, 'locations',
        mock.Mock(return_value=types.SimpleNamespace(items=[location])))
    retry_location = mock.Mock(return_value=object())
    monkeypatch.setattr(command.container_images_sdk, 'retry_location',
                        retry_location)
    monkeypatch.setattr(command.table_utils, 'format_container_image_mutation',
                        mock.Mock())

    _callback('image_retry')(f'artifact_id={artifact_id}', 'target-a', 'dist-a',
                             False, 'workspace-a')

    retry_location.assert_called_once_with('location-a',
                                           workspace='workspace-a',
                                           wait=True)


def test_image_profile_qualify_reads_json_object(
        monkeypatch, tmp_path: pathlib.Path) -> None:
    manifest = tmp_path / 'qualification.json'
    manifest.write_text(json.dumps({'target': 'qualified'}))
    qualify = mock.Mock(return_value=object())
    monkeypatch.setattr(command.container_images_sdk, 'qualify', qualify)
    monkeypatch.setattr(command.table_utils, 'format_container_image_mutation',
                        mock.Mock())

    _callback('image_profile_qualify')('profile-a', manifest)

    qualify.assert_called_once_with('profile-a', {'target': 'qualified'})


def test_image_profile_canary_projects_confirmation_and_wait(
        monkeypatch) -> None:
    canary = mock.Mock(return_value=object())
    confirm = mock.Mock()
    monkeypatch.setattr(command.container_images_sdk, 'canary', canary)
    monkeypatch.setattr(command.click, 'confirm', confirm)
    monkeypatch.setattr(command.table_utils, 'format_container_image_mutation',
                        mock.Mock())

    _callback('image_profile_canary')('profile-a', 'target-a', 'aws_eks',
                                      'context-a', 'workspace-a', True, True)

    canary.assert_called_once_with('profile-a',
                                   'target-a',
                                   'aws_eks',
                                   workspace='workspace-a',
                                   runtime_id='context-a',
                                   wait=False)
    confirm.assert_not_called()
