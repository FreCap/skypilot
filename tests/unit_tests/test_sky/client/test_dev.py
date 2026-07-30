"""Tests for the client-only development environment manifest."""

from __future__ import annotations

import pathlib

import pytest

from sky.client import dev
from sky.utils import dag_utils


def _manifest(dev_fields: str = '',
              task_fields: str = 'resources:\n  cpus: 2\n') -> str:
    return (f'dev:\n'
            f'  version: 1\n'
            f'  name: test-dev\n'
            f'{dev_fields}'
            f'{task_fields}')


def test_parse_manifest_defaults_and_strips_dev() -> None:
    manifest = dev.parse_manifest(_manifest())

    assert manifest.config == dev.DevConfig(version=1, name='test-dev')
    assert manifest.task.workdir == '.'
    assert manifest.task.run is None
    assert 'dev:' not in manifest.stripped_yaml
    assert 'dev:' not in manifest.task._user_specified_yaml  # pylint: disable=protected-access
    serialized = dag_utils.dump_dag_to_yaml_str(
        dag_utils.convert_entrypoint_to_dag(manifest.task))
    assert 'dev:' not in serialized


@pytest.mark.parametrize('yaml_str, expected', [
    ('resources:\n  cpus: 2\n', 'must contain a dev section'),
    ('dev:\n  version: 2\n  name: test-dev\nresources:\n  cpus: 2\n',
     'integer 1'),
    ('dev:\n  version: 1\nresources:\n  cpus: 2\n', 'non-empty cluster name'),
    (_manifest('  surprise: true\n'), 'unknown field'),
    (_manifest(task_fields='run:\n'), 'do not support the run field'),
    (_manifest(task_fields='service:\n'), 'do not support the service field'),
    (_manifest(task_fields='pool:\n'), 'do not support the pool field'),
    (_manifest(task_fields='num_nodes: 2\n'), 'exactly one node'),
    (_manifest() + '---\nresources:\n  cpus: 4\n', 'exactly one YAML document'),
    ('- dev\n- resources\n', 'non-empty mapping'),
    ('', 'non-empty mapping'),
    (_manifest('  name: duplicate\n'), 'Duplicate key name'),
    (_manifest('  remote_path: relative/path\n'), 'absolute or start with ~/'),
])
def test_parse_manifest_rejects_unsupported_shapes(yaml_str: str,
                                                   expected: str) -> None:
    with pytest.raises(ValueError, match=expected):
        dev.parse_manifest(yaml_str)


@pytest.mark.parametrize('yaml_str, secret_marker', [
    (_manifest('  bad\\x1bfield: true\n'), 'bad'),
    ('dev:\n  version: 1\n  name: "secret\\x1bname"\n', 'secret'),
])
def test_dev_validation_control_errors_are_value_free(
        yaml_str: str, secret_marker: str) -> None:
    with pytest.raises(ValueError) as exc_info:
        dev.parse_manifest(yaml_str)
    assert secret_marker not in str(exc_info.value)


def test_invalid_name_error_is_value_free() -> None:
    invalid_name = '-private-secret'
    with pytest.raises(ValueError) as exc_info:
        dev.parse_manifest(
            f'dev:\n  version: 1\n  name: {invalid_name}\nresources: {{}}\n')
    assert invalid_name not in str(exc_info.value)


def test_manifest_size_is_bounded() -> None:
    oversized = _manifest(task_fields=f'setup: {"x" * (1024 * 1024)}\n')
    with pytest.raises(ValueError, match='1 MiB'):
        dev.parse_manifest(oversized)


def test_file_loader_preserves_yaml_directory_git_metadata(
        tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_path = tmp_path / 'dev.yaml'
    manifest_path.write_text(_manifest(task_fields='workdir: null\n'),
                             encoding='utf-8')
    monkeypatch.setattr(dev.common_utils, 'get_git_commit',
                        lambda path: 'abc123')

    manifest = dev.load_manifest(str(manifest_path))

    assert manifest.task.metadata['git_commit'] == 'abc123'


@pytest.mark.parametrize('autostop_yaml, expected', [
    ('', False),
    ('  autostop: false\n', False),
    ('  autostop:\n    idle_minutes: 30\n', True),
])
def test_all_resources_have_autostop(autostop_yaml: str,
                                     expected: bool) -> None:
    task_fields = ('resources:\n'
                   '  cpus: 2\n'
                   f'{autostop_yaml}')
    manifest = dev.parse_manifest(_manifest(task_fields=task_fields))
    assert dev.all_resources_have_autostop(manifest.task) is expected


def test_one_unprotected_resource_alternative_warns() -> None:
    manifest = dev.parse_manifest(
        _manifest(task_fields=('resources:\n'
                               '  any_of:\n'
                               '    - cpus: 2\n'
                               '      autostop:\n'
                               '        idle_minutes: 30\n'
                               '    - cpus: 4\n')))
    assert not dev.all_resources_have_autostop(manifest.task)


@pytest.mark.parametrize('editor, expected_prefix', [
    (dev.DevEditor.VSCODE, 'vscode://vscode-remote/ssh-remote+test-dev'),
    (dev.DevEditor.CURSOR, 'cursor://vscode-remote/ssh-remote+test-dev'),
    (dev.DevEditor.WINDSURF, 'windsurf://vscode-remote/ssh-remote+test-dev'),
    (dev.DevEditor.ZED, 'zed://ssh/test-dev'),
])
def test_build_editor_uri(editor: dev.DevEditor, expected_prefix: str) -> None:
    uri = dev.build_editor_uri(editor, 'test-dev',
                               '/home/sky user/work#one?two%three')
    assert uri == (
        f'{expected_prefix}/home/sky%20user/work%23one%3Ftwo%25three')
    assert 'credential' not in uri


def test_none_editor_has_no_uri() -> None:
    assert dev.build_editor_uri(dev.DevEditor.NONE, 'test-dev',
                                '/home/sky') is None


def test_resolve_remote_path() -> None:
    assert dev.resolve_remote_path('~/sky work',
                                   '/home/sky') == ('/home/sky/sky work')
    assert dev.resolve_remote_path('~', '/root') == '/root'
    assert dev.resolve_remote_path('/workspace', '/root') == '/workspace'
