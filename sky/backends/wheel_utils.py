"""Build and validate SkyPilot's internal two-wheel worker bundle.

The bundle is transferred to cluster nodes as one directory.  It contains the
internal SkyPilot wheel, the independently owned worker-runtime wheel, and a
canonical manifest that binds both wheel hashes.
"""

import hashlib
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile

import colorama
import filelock
from packaging import version

import sky
from sky import sky_logging
from sky.backends import backend_utils
from sky.server import common
from sky.setup_files import dependencies
from sky.setup_files import worker_runtime_packaging as runtime_packaging
from sky.utils import directory_utils

logger = sky_logging.init_logger(__name__)

# Local wheel path is same as the remote path.
WHEEL_DIR = pathlib.Path(os.path.expanduser(backend_utils.SKY_REMOTE_PATH))
_WHEEL_LOCK_PATH = WHEEL_DIR.parent / '.wheels_lock'
SKY_PACKAGE_PATH = pathlib.Path(directory_utils.get_sky_dir())

# NOTE: keep the same as setup.py's setuptools.setup(name=..., ...).
_PACKAGE_WHEEL_NAME = 'skypilot'
_WHEEL_PATTERN = (f'{_PACKAGE_WHEEL_NAME}-'
                  f'{version.parse(sky.__version__)}-*.whl')
_WORKER_RUNTIME_PROJECT = (SKY_PACKAGE_PATH.parent / 'addons' /
                           'submission-containment' / 'python-runtime')
_WORKER_RUNTIME_RESOURCE = (SKY_PACKAGE_PATH / 'skylet' / 'runtime_wheels' /
                            'v1')
_INTERNAL_SOURCE_DIGEST_DOMAIN = b'SKYPILOT_INTERNAL_SKY_SOURCE_V1\0'
_CACHE_DIRECTORY_PATTERN = re.compile(
    r'(?:[0-9a-f]{32}|[0-9a-f]{64}|main-[0-9a-f]{64})')


def _remove_stale_wheels(latest_wheel_dir: pathlib.Path) -> None:
    """Remove all cached artifacts except the latest complete bundle."""
    if not WHEEL_DIR.exists():
        return
    for path in WHEEL_DIR.iterdir():
        if path == latest_wheel_dir:
            continue
        if (path.is_dir() and not path.is_symlink() and
                _CACHE_DIRECTORY_PATTERN.fullmatch(path.name) is not None):
            shutil.rmtree(path, ignore_errors=True)


def _stamped_init_content() -> str:
    content = (SKY_PACKAGE_PATH / '__init__.py').read_text()
    content = re.sub(r'_SKYPILOT_COMMIT_SHA = [\'\"](.*?)[\'\"]',
                     f'_SKYPILOT_COMMIT_SHA = \'{sky.__commit__}\'', content)
    commit_timestamp = sky.__commit_timestamp__ or ''
    content = re.sub(r'_SKYPILOT_COMMIT_TIMESTAMP = [\'\"](.*?)[\'\"]',
                     f'_SKYPILOT_COMMIT_TIMESTAMP = \'{commit_timestamp}\'',
                     content)
    if sky.__build__ is not None:
        content = re.sub(r'_SKYPILOT_COMMIT_COUNT = [\'\"](.*?)[\'\"]',
                         f'_SKYPILOT_COMMIT_COUNT = \'{sky.__build__}\'',
                         content)
    return content


def _normalized_setup_content() -> str:
    setup_path = SKY_PACKAGE_PATH / 'setup_files' / 'setup.py'
    # Internal workers always install the stable distribution identity, even
    # when the API server itself came from skypilot-nightly.
    return re.sub(r'\bname=[\'\"](.*?)[\'\"],',
                  f'name=\'{_PACKAGE_WHEEL_NAME}\',', setup_path.read_text())


def _internal_manifest_content() -> str:
    manifest_path = SKY_PACKAGE_PATH / 'setup_files' / 'MANIFEST.in'
    release_only_paths = ('sky/dashboard/out', 'sky/skylet/runtime_wheels')
    lines = [
        line for line in manifest_path.read_text().splitlines(keepends=True)
        if not any(path in line for path in release_only_paths)
    ]
    lines.append('prune sky/skylet/runtime_wheels\n')
    return ''.join(lines)


def _stage_internal_sky_source(build_root: pathlib.Path) -> None:
    """Stages the main wheel while excluding its nested runtime resource."""
    staged_sky = build_root / 'sky'
    staged_sky.mkdir()
    for item in SKY_PACKAGE_PATH.iterdir():
        target = staged_sky / item.name
        if item.name == '__init__.py':
            continue
        if item.name == 'skylet' and _WORKER_RUNTIME_RESOURCE.exists():
            target.mkdir()
            for skylet_item in item.iterdir():
                if skylet_item.name == 'runtime_wheels':
                    continue
                (target / skylet_item.name).symlink_to(
                    skylet_item, target_is_directory=skylet_item.is_dir())
        else:
            target.symlink_to(item, target_is_directory=item.is_dir())

    templates = SKY_PACKAGE_PATH.parent / 'sky_templates'
    if templates.exists():
        (build_root / 'sky_templates').symlink_to(templates,
                                                  target_is_directory=True)

    (build_root / 'setup.py').write_text(_normalized_setup_content())
    setup_files = SKY_PACKAGE_PATH / 'setup_files'
    for source in setup_files.iterdir():
        if not source.is_file() or source.name == 'setup.py':
            continue
        destination = build_root / source.name
        if source.name == 'MANIFEST.in':
            destination.write_text(_internal_manifest_content())
        else:
            shutil.copyfile(source, destination)
    (staged_sky / '__init__.py').write_text(_stamped_init_content())


def _run_pip_wheel(build_root: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Runs the existing main-wheel build with deterministic process inputs."""
    env = os.environ.copy()
    env['SOURCE_DATE_EPOCH'] = '0'
    env['PYTHONHASHSEED'] = '0'
    try:
        subprocess.run([
            sys.executable, '-m', 'pip', 'wheel', '--no-deps',
            str(build_root) + os.sep, '--wheel-dir',
            str(output_dir)
        ],
                       capture_output=True,
                       check=True,
                       text=True,
                       env=env)
    except subprocess.CalledProcessError as error:
        error_msg = error.stderr
        if 'No module named pip' in error_msg:
            if shutil.which('uv'):
                msg = ('pip module not found. Since you have UV installed, '
                       'you can install pip by running:\n'
                       '  uv pip install pip')
            elif shutil.which('conda'):
                msg = ('pip module not found. Since you have conda installed, '
                       'you can install pip by running:\n'
                       '  conda install pip')
            else:
                msg = ('pip module not found. Please install pip for your '
                       f'Python environment ({sys.executable}).')
        else:
            msg = f'pip wheel command failed. Error: {error_msg}'
        raise RuntimeError('Failed to build pip wheel for SkyPilot.\n' +
                           msg) from error
    except FileNotFoundError as error:
        raise RuntimeError(
            f'Failed to build pip wheel for SkyPilot. '
            f'Python executable not found: {sys.executable}') from error


def _run_build_py(build_root: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Projects exactly the files setuptools will place in the main wheel."""
    env = os.environ.copy()
    env['PYTHONHASHSEED'] = '0'
    try:
        subprocess.run([
            sys.executable, 'setup.py', '--no-user-cfg', 'build_py', '--force',
            '--build-lib',
            str(output_dir)
        ],
                       cwd=build_root,
                       capture_output=True,
                       check=True,
                       text=True,
                       env=env)
    except (subprocess.CalledProcessError, FileNotFoundError) as error:
        stderr = getattr(error, 'stderr', None)
        raise RuntimeError('Failed to project SkyPilot wheel inputs. '
                           f'Error: {stderr or error}') from error


def _check_running_version() -> None:
    version_on_disk = common.get_skypilot_version_on_disk()
    if version_on_disk == sky.__version__:
        return
    logger.warning(
        'Wheel build: The installed SkyPilot version is different from the '
        'running code.\n'
        f'{colorama.Style.DIM}'
        f'running version: {sky.__version__}\n'
        f'installed version: {version_on_disk}\n'
        f'{colorama.Style.RESET_ALL}'
        f'{colorama.Fore.YELLOW}'
        'Please restart the local API server by running:\n'
        f'{colorama.Style.BRIGHT}sky api stop; sky api start'
        f'{colorama.Style.RESET_ALL}')
    raise RuntimeError('The installed SkyPilot version is different from '
                       'the running code. Please restart the SkyPilot API '
                       'server with: sky api stop; sky api start')


def _build_internal_sky_wheel(output_dir: pathlib.Path) -> pathlib.Path:
    """Builds an internal main wheel with no nested standalone-wheel copy."""
    _check_running_version()
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as build_root_str:
        build_root = pathlib.Path(build_root_str)
        _stage_internal_sky_source(build_root)
        _run_pip_wheel(build_root, output_dir)

    wheels = list(output_dir.glob(_WHEEL_PATTERN))
    if len(wheels) != 1:
        raise RuntimeError(
            f'Failed to find exactly one SkyPilot wheel under {output_dir} '
            f'with glob pattern {_WHEEL_PATTERN!r}. '
            f'Found: {list(map(str, output_dir.glob("*")))}.')
    wheel_path = wheels[0]
    try:
        with zipfile.ZipFile(wheel_path) as wheel:
            if any(
                    name.startswith('sky/skylet/runtime_wheels/')
                    for name in wheel.namelist()):
                raise RuntimeError(
                    'Internal SkyPilot wheel contains the opaque runtime '
                    'resource')
    except zipfile.BadZipFile as error:
        raise RuntimeError(
            'Internal SkyPilot wheel is not a valid ZIP') from error
    return wheel_path


def _build_sky_wheel() -> pathlib.Path:
    """Builds one internal main wheel for private compatibility callers."""
    WHEEL_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as output_dir_str:
        wheel = _build_internal_sky_wheel(pathlib.Path(output_dir_str))
        digest = runtime_packaging.sha256_file(wheel)
        destination_dir = WHEEL_DIR / f'main-{digest}'
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / wheel.name
        if not destination.exists():
            shutil.copyfile(wheel, destination)
    return destination


def _normalized_internal_source_records() -> list[tuple[str, bytes]]:
    """Returns the normalized, release-shaped internal-wheel inputs."""
    with tempfile.TemporaryDirectory() as scratch_str:
        scratch = pathlib.Path(scratch_str)
        build_root = scratch / 'build-root'
        build_root.mkdir()
        _stage_internal_sky_source(build_root)
        projection = scratch / 'build-py'
        _run_build_py(build_root, projection)

        records: list[tuple[str, bytes]] = []
        for path in sorted(projection.rglob('*')):
            if path.is_dir():
                continue
            if path.is_symlink() or not path.is_file():
                raise RuntimeError(
                    f'Invalid projected internal wheel input: {path}')
            records.append(
                (f'build-py/{path.relative_to(projection).as_posix()}',
                 path.read_bytes()))

        # build_py captures packages and package data.  These two normalized
        # root files additionally bind metadata, package discovery, entrypoints,
        # and the package-data selection that controls the final wheel.
        for name in ('setup.py', 'MANIFEST.in'):
            path = build_root / name
            records.append((f'build-control/{name}', path.read_bytes()))
        return sorted(records, key=lambda item: item[0])


def _normalized_internal_source_digest(
        worker_record: runtime_packaging.WheelRecord,
        worker_provenance: bytes) -> str:
    digest = hashlib.sha256(_INTERNAL_SOURCE_DIGEST_DOMAIN)
    records = _normalized_internal_source_records()
    records.extend([
        ('worker-runtime-provenance', worker_provenance),
        ('worker-runtime-record',
         runtime_packaging.canonical_json_bytes(worker_record.to_dict())),
    ])
    for name, contents in sorted(records, key=lambda item: item[0]):
        name_bytes = name.encode()
        digest.update(len(name_bytes).to_bytes(8, 'big'))
        digest.update(name_bytes)
        digest.update(len(contents).to_bytes(8, 'big'))
        digest.update(contents)
    return digest.hexdigest()


def _resolve_worker_runtime_wheel(
    output_dir: pathlib.Path
) -> tuple[pathlib.Path, runtime_packaging.WheelRecord, bytes]:
    expected_version = (dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION)
    if _WORKER_RUNTIME_PROJECT.is_dir():
        repo_root = SKY_PACKAGE_PATH.parent
        wheel = runtime_packaging.build_worker_runtime_wheel(
            repo_root, output_dir)
        lock_path = (_WORKER_RUNTIME_PROJECT /
                     runtime_packaging.WORKER_RUNTIME_LOCK_FILENAME)
        record = runtime_packaging.verify_worker_runtime_lock(
            repo_root, wheel, lock_path)
        provenance = runtime_packaging.canonical_json_bytes({
            'filename': record.filename,
            'sha256': record.sha256,
            'size': record.size,
            'version': expected_version,
        })
        return wheel, record, provenance

    wheel = runtime_packaging.load_release_worker_runtime_artifact(
        _WORKER_RUNTIME_RESOURCE, expected_version)
    manifest_path = (_WORKER_RUNTIME_RESOURCE /
                     runtime_packaging.WORKER_RUNTIME_RESOURCE_MANIFEST)
    return (wheel, runtime_packaging.WheelRecord.from_path(wheel),
            manifest_path.read_bytes())


def _find_cached_bundle(
    source_digest: str,
    worker_record: runtime_packaging.WheelRecord,
) -> pathlib.Path | None:
    if not WHEEL_DIR.exists():
        return None
    matches = []
    for candidate in sorted(WHEEL_DIR.iterdir()):
        manifest_path = candidate / runtime_packaging.INTERNAL_BUNDLE_MANIFEST
        if not manifest_path.is_file():
            continue
        manifest = runtime_packaging.InternalBundleManifest.from_bytes(
            manifest_path.read_bytes())
        if (manifest.source_input_sha256 != source_digest or
                manifest.wheels[1] != worker_record):
            continue
        runtime_packaging.verify_internal_bundle(
            candidate,
            expected_digest=candidate.name,
            expected_source_input_sha256=source_digest,
            expected_worker_record=worker_record)
        matches.append(candidate)
    if len(matches) > 1:
        raise ValueError('multiple cached bundles match the same source inputs')
    return matches[0] if matches else None


def build_sky_wheel() -> tuple[pathlib.Path, str]:
    """Builds or reuses the exact three-file internal worker bundle.

    Caller is responsible for removing the returned temporary directory.

    Returns:
        A tuple of (bundle directory, bundle SHA-256).  The directory contains
        exactly the internal main wheel, standalone runtime wheel, and
        canonical manifest.
    """
    with filelock.FileLock(_WHEEL_LOCK_PATH):  # pylint: disable=E0110
        WHEEL_DIR.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory() as build_dir_str:
            build_dir = pathlib.Path(build_dir_str)
            worker_wheel, worker_record, worker_provenance = (
                _resolve_worker_runtime_wheel(build_dir))
            source_digest = _normalized_internal_source_digest(
                worker_record, worker_provenance)
            bundle = _find_cached_bundle(source_digest, worker_record)
            if bundle is None:
                main_wheel = _build_internal_sky_wheel(build_dir / 'main')
                manifest = runtime_packaging.make_internal_bundle_manifest(
                    source_digest, main_wheel, worker_wheel)
                bundle = runtime_packaging.materialize_internal_bundle(
                    WHEEL_DIR, manifest, main_wheel, worker_wheel)

        _, wheel_hash = runtime_packaging.verify_internal_bundle(
            bundle,
            expected_digest=bundle.name,
            expected_source_input_sha256=source_digest,
            expected_worker_record=worker_record)
        _remove_stale_wheels(bundle)

        temp_wheel_dir = pathlib.Path(tempfile.gettempdir()) / wheel_hash
        if temp_wheel_dir.exists():
            runtime_packaging.verify_internal_bundle(
                temp_wheel_dir,
                expected_digest=wheel_hash,
                expected_source_input_sha256=source_digest,
                expected_worker_record=worker_record)
        else:
            shutil.copytree(bundle, temp_wheel_dir)
            runtime_packaging.verify_internal_bundle(
                temp_wheel_dir,
                expected_digest=wheel_hash,
                expected_source_input_sha256=source_digest,
                expected_worker_record=worker_record)

    return temp_wheel_dir.absolute(), wheel_hash
