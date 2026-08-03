"""Tests for the boltz overlay's root-module source projection."""

import importlib.util
import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).parents[2]
_MODULE_SPEC = importlib.util.spec_from_file_location(
    'boltz_overlay_source_manifest',
    _REPO_ROOT / 'boltz' / 'overlay_source_manifest.py')
assert _MODULE_SPEC is not None
assert _MODULE_SPEC.loader is not None
overlay_source_manifest = importlib.util.module_from_spec(_MODULE_SPEC)
_MODULE_SPEC.loader.exec_module(overlay_source_manifest)


def _write_setup(root: pathlib.Path, py_modules: str) -> pathlib.Path:
    setup_path = root / 'setup.py'
    setup_path.write_text(
        'import setuptools\n'
        f'setuptools.setup(py_modules={py_modules})\n',
        encoding='utf-8')
    return setup_path


def test_declared_py_module_sources_match_release_setup():
    sources = overlay_source_manifest.declared_py_module_sources(_REPO_ROOT /
                                                                 'setup.py')

    assert sources == (
        pathlib.Path('skypilot_serve_system_oom_recovery_authorization.py'),)


def test_declared_py_module_sources_support_nested_modules(tmp_path):
    setup_path = _write_setup(tmp_path, "['bootstrap', 'internal.guard']")
    (tmp_path / 'bootstrap.py').write_text('', encoding='utf-8')
    (tmp_path / 'internal').mkdir()
    (tmp_path / 'internal' / 'guard.py').write_text('', encoding='utf-8')

    assert overlay_source_manifest.declared_py_module_sources(setup_path) == (
        pathlib.Path('bootstrap.py'), pathlib.Path('internal/guard.py'))


@pytest.mark.parametrize('declaration', [
    '[]',
    "['missing']",
    "['duplicate', 'duplicate']",
    "['../escape']",
    'dynamic_modules',
])
def test_declared_py_module_sources_fail_closed(tmp_path, declaration):
    setup_path = _write_setup(tmp_path, declaration)
    (tmp_path / 'duplicate.py').write_text('', encoding='utf-8')

    with pytest.raises(ValueError):
        overlay_source_manifest.declared_py_module_sources(setup_path)
