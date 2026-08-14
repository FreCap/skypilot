"""Characterization tests for the ``sky image`` command family."""

import ast
import contextvars
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
from sky.usage import usage_lib

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
    'image': 'afb61f10bb719b0de8255317938009a7ae8f3394523dac73f17c06fe0d56925b',
    'image_publish': 'c801ead794a362a175871588f3a720aaa8b45382b64e62fbbea5600788044d5c',
    'image_status': 'aca7623bad0ab4e0802fdfdf735f5c6268f48b35c38d580f51646a78100f07aa',
    'image_prepare': '9daf98b1f5d90ce8c6c7e0d7ea10999826e829a76d83d152d54cba858e53a923',
    'image_retry': 'f374bf8a7d7fb79d2cfc8fa1767c838987e2f46f63967d561ed3b0770bc10fd4',
    'image_profile': '594522f94db2f00455444d6abe571d623879c14f63ada1644916c8b18a9d44f1',
    'image_profile_qualify': 'cec45982a49d73ee8b7e20641541a33df818e6a4861ac202c48063cb17f1d02a',
    'image_profile_canary': '7ce2227863fc39571348069c1afd7c80d1f775688bb5ed12c31b32d91e580928',
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
    return hashlib.sha256(repr(_stable_ast(function.body)).encode()).hexdigest()


def _stable_ast(value: object) -> object:
    if isinstance(value, ast.AST):
        fields = []
        for field, child in ast.iter_fields(value):
            if field == 'type_params':
                continue
            fields.append((field, _stable_ast(child)))
        return type(value).__name__, tuple(fields)
    if isinstance(value, list):
        return tuple(_stable_ast(item) for item in value)
    return value


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
        assert inspect.unwrap(callback).__module__ == command.__name__


def test_image_usage_entrypoint_preserves_facade_identity(monkeypatch) -> None:
    monkeypatch.setenv('SKYPILOT_DISABLE_USAGE_COLLECTION', '1')
    monkeypatch.setattr(command.container_images_sdk, 'status',
                        mock.Mock(return_value=[]))
    monkeypatch.setattr(command.table_utils, 'format_container_image_table',
                        mock.Mock(return_value=''))

    def _invoke() -> str | None:
        usage_lib.install_fresh_messages_for_current_context()
        callback = command.image_status.callback
        assert callback is not None
        callback(None, None)
        return usage_lib.messages.usage.entrypoint

    assert contextvars.Context().run(
        _invoke) == 'sky.client.cli.command.image_status'


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
