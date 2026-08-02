"""PEP 517 backend for the deterministic worker-runtime wheel."""

import importlib.util
import pathlib
from typing import Any


def _packaging_module() -> Any:
    project_root = pathlib.Path(__file__).resolve().parent
    repo_root = project_root.parents[2]
    module_path = (repo_root / 'sky' / 'setup_files' /
                   'worker_runtime_packaging.py')
    spec = importlib.util.spec_from_file_location(
        '_skypilot_worker_runtime_packaging', module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f'cannot load worker runtime packager: {module_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def get_requires_for_build_wheel(config_settings=None):
    del config_settings
    return []


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    del config_settings, metadata_directory
    project_root = pathlib.Path(__file__).resolve().parent
    repo_root = project_root.parents[2]
    wheel = _packaging_module().build_worker_runtime_wheel(
        repo_root, pathlib.Path(wheel_directory))
    return wheel.name
