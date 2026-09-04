"""Tests for the exact release identity composed into the Boltz image."""

import importlib.util
import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).parents[2]
_MODULE_PATH = _REPO_ROOT / 'boltz' / 'stamp_image_release.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('stamp_image_release',
                                                  _MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_release_tree(root: pathlib.Path) -> None:
    sky = root / 'sky'
    sky.mkdir(parents=True)
    (sky / '__init__.py').write_text(
        "_SKYPILOT_COMMIT_SHA = '{{SKYPILOT_COMMIT_SHA}}'\n"
        "_SKYPILOT_COMMIT_TIMESTAMP = '{{SKYPILOT_COMMIT_TIMESTAMP}}'\n"
        "_SKYPILOT_COMMIT_COUNT = '{{SKYPILOT_COMMIT_COUNT}}'\n"
        "__version__ = '1.1.0'\n",
        encoding='utf-8')
    policy = root / 'boltz' / 'reserved_fill_reclaim_policy'
    package = policy / 'src' / 'boltz_reserved_fill_reclaim_policy'
    package.mkdir(parents=True)
    (policy / 'pyproject.toml').write_text('version = "0.0.0"\n',
                                           encoding='utf-8')
    (package / '__init__.py').write_text(
        "__version__ = '0.0.0'\nPOLICY_REVISION = 'reviewed-v1'\n",
        encoding='utf-8')


def _identity(module):
    return module.ReleaseIdentity(
        version='1.1.1700',
        commit='0123456789abcdef0123456789abcdef01234567',
        commit_timestamp='2026-09-04T17:30:00+00:00',
        commit_count='1700')


def test_stamp_release_updates_both_distributions_atomically(tmp_path):
    module = _load_module()
    _write_release_tree(tmp_path)

    assert module.main([
        '--root',
        str(tmp_path), '--version', '1.1.1700', '--commit',
        '0123456789abcdef0123456789abcdef01234567', '--commit-timestamp',
        '2026-09-04T17:30:00+00:00', '--commit-count', '1700',
        '--install-policy', 'true'
    ]) == 0

    sky_init = (tmp_path / 'sky' / '__init__.py').read_text(encoding='utf-8')
    assert "_SKYPILOT_COMMIT_SHA = '0123456789abcdef0123456789abcdef01234567'" in sky_init
    assert "_SKYPILOT_COMMIT_TIMESTAMP = '2026-09-04T17:30:00+00:00'" in sky_init
    assert "_SKYPILOT_COMMIT_COUNT = '1700'" in sky_init
    assert "__version__ = '1.1.1700'" in sky_init
    assert ((tmp_path / 'boltz' / 'reserved_fill_reclaim_policy' /
             'pyproject.toml').read_text(
                 encoding='utf-8') == 'version = "1.1.1700"\n')
    policy_init = (tmp_path / 'boltz' / 'reserved_fill_reclaim_policy' / 'src' /
                   'boltz_reserved_fill_reclaim_policy' /
                   '__init__.py').read_text(encoding='utf-8')
    assert "__version__ = '1.1.1700'" in policy_init
    assert "POLICY_REVISION = 'reviewed-v1'" in policy_init


@pytest.mark.parametrize(('field', 'value'),
                         [('version', '1.1.dev1'), ('commit', 'not-a-commit'),
                          ('commit_timestamp', '2026-09-04T17:30:00'),
                          ('commit_count', '0')])
def test_release_identity_rejects_invalid_fields(field, value):
    module = _load_module()
    values = {
        'version': '1.1.1700',
        'commit': '0123456789abcdef0123456789abcdef01234567',
        'commit_timestamp': '2026-09-04T17:30:00+00:00',
        'commit_count': '1700',
    }
    values[field] = value

    with pytest.raises(ValueError):
        module.ReleaseIdentity(**values)


def test_failed_validation_leaves_all_files_unchanged(tmp_path):
    module = _load_module()
    _write_release_tree(tmp_path)
    policy_init = (tmp_path / 'boltz' / 'reserved_fill_reclaim_policy' / 'src' /
                   'boltz_reserved_fill_reclaim_policy' / '__init__.py')
    policy_init.write_text("POLICY_REVISION = 'reviewed-v1'\n",
                           encoding='utf-8')
    before = {
        path: path.read_bytes()
        for path in tmp_path.rglob('*')
        if path.is_file()
    }

    with pytest.raises(RuntimeError, match='could not stamp'):
        module.stamp_release(tmp_path, _identity(module), install_policy=True)

    assert {path: path.read_bytes() for path in before} == before


def test_duplicate_identity_projection_fails_closed(tmp_path):
    module = _load_module()
    _write_release_tree(tmp_path)
    sky_init = tmp_path / 'sky' / '__init__.py'
    sky_init.write_text(sky_init.read_text(encoding='utf-8') +
                        "__version__ = 'duplicate'\n",
                        encoding='utf-8')
    before = sky_init.read_bytes()

    with pytest.raises(RuntimeError, match='could not stamp'):
        module.stamp_release(tmp_path, _identity(module), install_policy=True)

    assert sky_init.read_bytes() == before


def test_empty_cli_identity_is_a_noop_for_generic_image_build(tmp_path):
    module = _load_module()
    _write_release_tree(tmp_path)
    before = (tmp_path / 'sky' / '__init__.py').read_bytes()

    assert module.main([
        '--root',
        str(tmp_path), '--version', '', '--commit', '', '--commit-timestamp',
        '', '--commit-count', '', '--install-policy', 'false'
    ]) == 0

    assert (tmp_path / 'sky' / '__init__.py').read_bytes() == before
