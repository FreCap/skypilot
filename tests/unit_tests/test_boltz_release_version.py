"""Tests for deterministic Boltz artifact release versions."""

import importlib.util
import pathlib
import subprocess

import pytest


def _load_release_version_module():
    path = pathlib.Path(
        __file__).resolve().parents[2] / 'boltz' / 'release_version.py'
    spec = importlib.util.spec_from_file_location('boltz_release_version', path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Unable to load release version module from {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


release_version = _load_release_version_module()


def _git(repo: pathlib.Path, *args: str) -> str:
    result = subprocess.run(['git', '-C', str(repo), *args],
                            capture_output=True,
                            text=True,
                            check=True)
    return result.stdout.strip()


def _commit(repo: pathlib.Path, relative_path: str, content: str,
            message: str) -> str:
    path = repo / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')
    _git(repo, 'add', '--', relative_path)
    _git(repo, 'commit', '-m', message)
    return _git(repo, 'rev-parse', 'HEAD')


@pytest.fixture(name='git_repo')
def _git_repo(tmp_path: pathlib.Path):
    _git(tmp_path, 'init')
    _git(tmp_path, 'config', 'user.email', 'release-version@example.com')
    _git(tmp_path, 'config', 'user.name', 'Release Version Test')
    base = _commit(tmp_path, 'unrelated.txt', 'base\n', 'base')
    epoch = _commit(tmp_path, 'unrelated.txt', 'epoch\n', 'release epoch')
    branch = _git(tmp_path, 'branch', '--show-current')
    return tmp_path, base, epoch, branch


def _version(repo: pathlib.Path, epoch: str, ref: str = 'HEAD') -> str:
    return release_version.calculate_release_version(ref,
                                                     repo_root=repo,
                                                     epoch_commit=epoch,
                                                     epoch_patch=0)


def test_counts_every_first_parent_commit(git_repo):
    repo, _, epoch, _ = git_repo

    _commit(repo, 'tests/unit_tests/test_unrelated.py', 'irrelevant\n',
            'test-only change')
    assert _version(repo, epoch) == '1.1.1'

    _commit(repo, 'boltz/build-overlay.sh', 'image input\n', 'image change')
    assert _version(repo, epoch) == '1.1.2'

    _commit(repo, 'charts/skypilot/Chart.yaml', 'chart input\n', 'chart change')
    assert _version(repo, epoch) == '1.1.3'


def test_feature_commit_and_merge_have_same_version(git_repo):
    repo, _, epoch, main_branch = git_repo
    _git(repo, 'checkout', '-b', 'release-change')
    feature_commit = _commit(repo, 'sky/release_input.py', 'changed = True\n',
                             'release change')

    assert _version(repo, epoch, feature_commit) == '1.1.1'

    _git(repo, 'checkout', main_branch)
    _git(repo, 'merge', '--no-ff', 'release-change', '-m',
         'merge release change')
    merge_commit = _git(repo, 'rev-parse', 'HEAD')

    assert _version(repo, epoch, merge_commit) == '1.1.1'


def test_continues_from_epoch_patch(git_repo):
    repo, _, epoch, _ = git_repo

    assert release_version.calculate_release_version('HEAD',
                                                     repo_root=repo,
                                                     epoch_commit=epoch,
                                                     epoch_patch=19) == '1.1.19'
    _commit(repo, 'docs/release.md', 'merged\n', 'next merge')
    assert release_version.calculate_release_version('HEAD',
                                                     repo_root=repo,
                                                     epoch_commit=epoch,
                                                     epoch_patch=19) == '1.1.20'


def test_rejects_ref_before_epoch(git_repo):
    repo, base, epoch, _ = git_repo

    with pytest.raises(release_version.ReleaseVersionError,
                       match='is not descended from release epoch'):
        _version(repo, epoch, base)


def test_cli_prints_only_version(monkeypatch, capsys):
    monkeypatch.setattr(release_version, 'calculate_release_version',
                        lambda ref: '1.1.7')

    assert release_version.main(['--ref', 'example']) == 0
    captured = capsys.readouterr()
    assert captured.out == '1.1.7\n'
    assert captured.err == ''
