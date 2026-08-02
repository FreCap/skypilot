import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
from unittest import mock
import zipfile

import pytest

import sky
from sky.backends import wheel_utils
from sky.server import common
from sky.setup_files import dependencies
from sky.setup_files import worker_runtime_packaging
from sky.skylet import constants

_REPO_ROOT = Path(__file__).resolve().parents[1]
_WORKER_RUNTIME_PROJECT = (_REPO_ROOT / 'addons' / 'submission-containment' /
                           'python-runtime')

pytestmark = pytest.mark.xdist_group(name='wheel_builds')


def _load_setup_module():
    setup_path = Path('setup.py').absolute()
    spec = importlib.util.spec_from_file_location('_skypilot_setup_test',
                                                  setup_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def _isolated_bundle_cache(tmp_path, monkeypatch):
    cache = tmp_path / 'cache'
    transfer = tmp_path / 'transfer'
    transfer.mkdir()
    monkeypatch.setattr(wheel_utils, 'WHEEL_DIR', cache)
    monkeypatch.setattr(wheel_utils, '_WHEEL_LOCK_PATH',
                        tmp_path / 'cache.lock')
    monkeypatch.setattr(wheel_utils.tempfile, 'gettempdir',
                        lambda: str(transfer))
    return cache, transfer


def _build_prior_v40_wheel(output_dir: Path) -> Path:
    """Builds a same-package-version v40 distribution for replacement."""
    output_dir.mkdir()
    wheel_path = output_dir / 'skypilot-1.1.0-py3-none-any.whl'
    dist_info = 'skypilot-1.1.0.dist-info'
    files = {
        'sky/__init__.py': "__version__ = '1.1.0'\n",
        'sky/skylet/__init__.py': '',
        'sky/skylet/constants.py':
            ("SKYLET_VERSION = '40'\nSKYLET_LIB_VERSION = 7\n"),
        'sky/v40_only.py': 'PRIOR_VERSION_SENTINEL = True\n',
        f'{dist_info}/METADATA':
            ('Metadata-Version: 2.1\nName: skypilot\nVersion: 1.1.0\n'),
        f'{dist_info}/WHEEL': ('Wheel-Version: 1.0\nGenerator: SkyPilot test\n'
                               'Root-Is-Purelib: true\nTag: py3-none-any\n'),
    }
    record_path = f'{dist_info}/RECORD'
    files[record_path] = ''.join(
        f'{name},,\n' for name in (*files, record_path))
    with zipfile.ZipFile(wheel_path, 'w') as wheel:
        for name, contents in files.items():
            wheel.writestr(name, contents)
    return wheel_path


def _run_installed_distribution_probe(
        installed: Path,
        main_wheel: Path,
        runtime_wheel: Path,
        shadow_root: Path | None = None) -> subprocess.CompletedProcess:
    probe = worker_runtime_packaging.render_installed_distribution_probe(
        str(sky.__version__),
        dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION)
    path_setup = f'import sys; sys.path.insert(0, {str(installed)!r});'
    if shadow_root is not None:
        path_setup += f'sys.path.insert(0, {str(shadow_root)!r});'
    return subprocess.run([
        sys.executable, '-I', '-c', path_setup + probe,
        str(main_wheel),
        str(runtime_wheel)
    ],
                          capture_output=True,
                          text=True,
                          check=False)


def test_worker_runtime_dependency_remains_dormant():
    """S0b1a0 must not make ordinary source installs resolve a new index."""
    assert dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION == '1.0.0'
    assert all(worker_runtime_packaging.WORKER_RUNTIME_DISTRIBUTION not in
               requirement.lower()
               for requirement in dependencies.install_requires)


def test_worker_runtime_wheel_is_deterministic_and_isolated(tmp_path):
    first = worker_runtime_packaging.build_worker_runtime_wheel(
        _REPO_ROOT, tmp_path / 'first')
    second = worker_runtime_packaging.build_worker_runtime_wheel(
        _REPO_ROOT, tmp_path / 'second')

    assert first.read_bytes() == second.read_bytes()
    version = dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION
    record = worker_runtime_packaging.verify_worker_runtime_wheel(
        first, version)
    assert record.filename == first.name

    dist_info = f'skypilot_worker_runtime_v1-{version}.dist-info'
    with zipfile.ZipFile(first) as wheel:
        assert wheel.namelist() == [
            'skypilot_worker_runtime/__init__.py',
            f'{dist_info}/METADATA',
            f'{dist_info}/WHEEL',
            f'{dist_info}/RECORD',
        ]
        assert not any(name.startswith('sky/') for name in wheel.namelist())
        assert f'{dist_info}/entry_points.txt' not in wheel.namelist()

    probe = ('import sys; '
             f'sys.path.insert(0, {str(first)!r}); '
             'import skypilot_worker_runtime; '
             'assert not any(name == "sky" or name.startswith("sky.") '
             'for name in sys.modules)')
    completed = subprocess.run([sys.executable, '-I', '-c', probe],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr


def test_worker_runtime_pep517_backend_matches_direct_build(tmp_path):
    direct = worker_runtime_packaging.build_worker_runtime_wheel(
        _REPO_ROOT, tmp_path / 'direct')
    pep517_dir = tmp_path / 'pep517'
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'wheel', '--no-deps', '--wheel-dir',
        str(pep517_dir),
        str(_WORKER_RUNTIME_PROJECT)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    pep517 = next(pep517_dir.glob('*.whl'))
    assert pep517.read_bytes() == direct.read_bytes()


def test_worker_runtime_build_does_not_require_git(tmp_path):
    copied_repo = tmp_path / 'source-without-git'
    copied_project = (copied_repo / 'addons' / 'submission-containment' /
                      'python-runtime')
    copied_project.parent.mkdir(parents=True)
    shutil.copytree(_WORKER_RUNTIME_PROJECT, copied_project)
    copied_setup_files = copied_repo / 'sky' / 'setup_files'
    copied_setup_files.mkdir(parents=True)
    shutil.copy2(_REPO_ROOT / 'sky/setup_files/dependencies.py',
                 copied_setup_files)

    wheel = worker_runtime_packaging.build_worker_runtime_wheel(
        copied_repo, tmp_path / 'wheel')
    worker_runtime_packaging.verify_worker_runtime_wheel(
        wheel, dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION)


def test_worker_runtime_lock_and_release_resource_match():
    version = dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION
    resource_dir = _REPO_ROOT / 'sky/skylet/runtime_wheels/v1'
    wheel = worker_runtime_packaging.load_release_worker_runtime_artifact(
        resource_dir, version)
    record = worker_runtime_packaging.verify_worker_runtime_lock(
        _REPO_ROOT, wheel, _WORKER_RUNTIME_PROJECT /
        worker_runtime_packaging.WORKER_RUNTIME_LOCK_FILENAME)
    assert record.sha256 == (
        '2903fbc9eb98bc728efffb42e6b4067ec0a645e0225d0373beabfa14934a6f23')


def test_worker_runtime_source_rebuild_matches_release_resource(tmp_path):
    rebuilt = (
        worker_runtime_packaging.verify_source_and_release_worker_runtime(
            _REPO_ROOT, tmp_path))
    release = worker_runtime_packaging.load_release_worker_runtime_artifact(
        _REPO_ROOT / 'sky/skylet/runtime_wheels/v1',
        dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION)
    assert rebuilt.read_bytes() == release.read_bytes()


@pytest.mark.parametrize('workflow_name',
                         ['release-publish.yml', 'nightly-build.yml'])
def test_publication_workflows_verify_runtime_outside_dist(workflow_name):
    workflow = (_REPO_ROOT / '.github/workflows' / workflow_name).read_text()
    assert 'verify-source-resource' in workflow
    assert '--output-dir "${RUNNER_TEMP}/worker-runtime"' in workflow
    assert '--output-dir dist' not in workflow
    assert 'path: dist/' in workflow


def test_release_wheel_embeds_runtime_without_publishing_it(tmp_path):
    publication_dir = tmp_path / 'publication'
    runtime_build_dir = tmp_path / 'runtime-build'
    runtime_wheel = worker_runtime_packaging.build_worker_runtime_wheel(
        _REPO_ROOT, runtime_build_dir)
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'wheel', '--no-build-isolation',
        '--no-deps', '--wheel-dir',
        str(publication_dir),
        str(_REPO_ROOT)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    assert [path.name for path in publication_dir.iterdir()
           ] == [f'skypilot-{sky.__version__}-py3-none-any.whl']

    main_wheel = next(publication_dir.glob('skypilot-*.whl'))
    nested_name = ('sky/skylet/runtime_wheels/v1/' + runtime_wheel.name)
    with zipfile.ZipFile(main_wheel) as wheel:
        names = wheel.namelist()
        assert wheel.read(nested_name) == runtime_wheel.read_bytes()
        assert not any(
            name.startswith('skypilot_worker_runtime/') for name in names)
        metadata_name = next(
            name for name in names if name.endswith('.dist-info/METADATA'))
        metadata = wheel.read(metadata_name).decode('utf-8')
        assert ('Requires-Dist: skypilot-worker-runtime-v1' not in metadata)


def test_release_sdist_preserves_opaque_runtime_only(tmp_path):
    distribution_dir = tmp_path / 'dist'
    completed = subprocess.run([
        sys.executable, 'setup.py', 'sdist', '--dist-dir',
        str(distribution_dir)
    ],
                               cwd=_REPO_ROOT,
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    sdist = next(distribution_dir.glob('*.tar.gz'))
    runtime_name = worker_runtime_packaging.worker_runtime_wheel_filename(
        dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION)
    with tarfile.open(sdist, mode='r:gz') as archive:
        members = archive.getnames()
        nested_member = next(
            name for name in members
            if name.endswith(f'sky/skylet/runtime_wheels/v1/{runtime_name}'))
        assert archive.extractfile(nested_member).read() == (
            _REPO_ROOT / 'sky/skylet/runtime_wheels/v1' /
            runtime_name).read_bytes()
        assert not any(
            '/addons/submission-containment/python-runtime/src/' in name
            for name in members)

    rebuilt_dir = tmp_path / 'rebuilt'
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'wheel', '--no-build-isolation',
        '--no-deps', '--wheel-dir',
        str(rebuilt_dir),
        str(sdist)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    rebuilt = next(rebuilt_dir.glob('skypilot-*.whl'))
    with zipfile.ZipFile(rebuilt) as wheel:
        assert wheel.read('sky/skylet/runtime_wheels/v1/' +
                          runtime_name) == (_REPO_ROOT /
                                            'sky/skylet/runtime_wheels/v1' /
                                            runtime_name).read_bytes()


def test_internal_bundle_exact_inventory_and_cache_reuse(
        _isolated_bundle_cache):
    bundle, digest = wheel_utils.build_sky_wheel()
    manifest, verified_digest = (
        worker_runtime_packaging.verify_internal_bundle(bundle,
                                                        expected_digest=digest))
    assert verified_digest == digest
    assert len(digest) == 64
    assert {path.name for path in bundle.iterdir()} == {
        'manifest.json', manifest.wheels[0].filename,
        manifest.wheels[1].filename
    }
    with zipfile.ZipFile(bundle / manifest.wheels[0].filename) as main_wheel:
        assert not any(
            name.startswith('sky/skylet/runtime_wheels/')
            for name in main_wheel.namelist())

    with mock.patch.object(
            wheel_utils,
            '_build_internal_sky_wheel',
            side_effect=AssertionError('cache reuse rebuilt the main wheel')):
        cached_bundle, cached_digest = wheel_utils.build_sky_wheel()
    assert cached_bundle == bundle
    assert cached_digest == digest


def test_internal_bundle_gc_preserves_control_files(_isolated_bundle_cache):
    cache, _ = _isolated_bundle_cache
    current = cache / ('a' * 64)
    stale = cache / ('b' * 64)
    legacy = cache / ('c' * 32)
    compatibility = cache / ('main-' + 'd' * 64)
    unknown = cache / 'operator-owned'
    for directory in (current, stale, legacy, compatibility, unknown):
        directory.mkdir(parents=True)
    marker = cache / 'current_sky_wheel_hash'
    old_marker = cache / 'current_sky_wheel_hash.old'
    marker.write_text('current')
    old_marker.write_text('old')

    wheel_utils._remove_stale_wheels(  # pylint: disable=protected-access
        current)

    assert current.is_dir()
    assert not stale.exists()
    assert not legacy.exists()
    assert not compatibility.exists()
    assert unknown.is_dir()
    assert marker.read_text() == 'current'
    assert old_marker.read_text() == 'old'


@pytest.mark.parametrize('tamper', ['wheel', 'manifest', 'extra'])
def test_internal_bundle_cache_rejects_tampering(_isolated_bundle_cache,
                                                 tamper):
    cache, _ = _isolated_bundle_cache
    _, digest = wheel_utils.build_sky_wheel()
    cached_bundle = cache / digest
    manifest = worker_runtime_packaging.InternalBundleManifest.from_bytes(
        (cached_bundle / 'manifest.json').read_bytes())
    if tamper == 'wheel':
        with (cached_bundle / manifest.wheels[0].filename).open('ab') as wheel:
            wheel.write(b'tampered')
    elif tamper == 'manifest':
        (cached_bundle / 'manifest.json').write_bytes(b'{}')
    else:
        (cached_bundle / 'unexpected').write_text('unexpected')

    with pytest.raises(ValueError):
        wheel_utils.build_sky_wheel()


def test_installed_release_reconstructs_identical_bundle(
        _isolated_bundle_cache, tmp_path):
    source_bundle, source_digest = wheel_utils.build_sky_wheel()
    source_manifest = (source_bundle / 'manifest.json').read_bytes()

    release_dir = tmp_path / 'release'
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'wheel', '--no-build-isolation',
        '--no-deps', '--wheel-dir',
        str(release_dir),
        str(_REPO_ROOT)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    release_wheel = next(release_dir.glob('skypilot-*.whl'))
    installed_root = tmp_path / 'installed-api'
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'install', '--no-index', '--no-deps',
        '--target',
        str(installed_root),
        str(release_wheel)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr

    installed_cache = tmp_path / 'installed-cache'
    installed_transfer = tmp_path / 'installed-transfer'
    installed_transfer.mkdir()
    build_script = f'''
import json
import pathlib
import sys
from unittest import mock
sys.path.insert(0, {str(installed_root)!r})
import sky
from sky.backends import wheel_utils
from sky.server import common
wheel_utils.WHEEL_DIR = pathlib.Path({str(installed_cache)!r})
wheel_utils._WHEEL_LOCK_PATH = pathlib.Path({str(tmp_path / 'installed-cache.lock')!r})
wheel_utils.tempfile.gettempdir = lambda: {str(installed_transfer)!r}
with mock.patch.object(common, 'get_skypilot_version_on_disk', return_value=sky.__version__):
    bundle, digest = wheel_utils.build_sky_wheel()
print('BUNDLE_RESULT=' + json.dumps({{
    'digest': digest,
    'manifest': (bundle / 'manifest.json').read_text(),
    'sky_path': str(wheel_utils.SKY_PACKAGE_PATH),
}}, sort_keys=True))
'''
    completed = subprocess.run([sys.executable, '-I', '-c', build_script],
                               cwd=tmp_path,
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    result_line = next(line for line in completed.stdout.splitlines()
                       if line.startswith('BUNDLE_RESULT='))
    result = json.loads(result_line.removeprefix('BUNDLE_RESULT='))
    assert result['sky_path'].startswith(str(installed_root))
    assert result['digest'] == source_digest
    assert result['manifest'].encode() == source_manifest


def test_two_local_wheels_replace_v40_offline_on_v42_baseline(
        _isolated_bundle_cache, tmp_path):
    bundle, digest = wheel_utils.build_sky_wheel()
    manifest, _ = worker_runtime_packaging.verify_internal_bundle(
        bundle, expected_digest=digest)
    installed = tmp_path / 'installed'
    prior_wheel = _build_prior_v40_wheel(tmp_path / 'prior-wheel')
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'install', '--no-index', '--no-deps',
        '--target',
        str(installed),
        str(prior_wheel),
        str(bundle / manifest.wheels[1].filename)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    prior_probe = (f'import sys; sys.path.insert(0, {str(installed)!r});'
                   'from sky.skylet import constants; import sky.v40_only;'
                   "assert constants.SKYLET_VERSION == '40';"
                   'assert constants.SKYLET_LIB_VERSION == 7;'
                   'assert sky.v40_only.PRIOR_VERSION_SENTINEL is True')
    completed = subprocess.run([sys.executable, '-I', '-c', prior_probe],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    main_wheel = bundle / manifest.wheels[0].filename
    runtime_wheel = bundle / manifest.wheels[1].filename
    completed = _run_installed_distribution_probe(installed, main_wheel,
                                                  runtime_wheel)
    assert completed.returncode != 0
    assert 'installed skypilot file' in completed.stderr
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'install', '--no-index', '--no-deps',
        '--upgrade', '--force-reinstall', '--target',
        str(installed),
        str(main_wheel),
        str(runtime_wheel)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    baseline_probe = (
        'from sky.skylet import constants;'
        "assert constants.SKYLET_VERSION == '42';"
        'assert constants.SKYLET_LIB_VERSION == 9;'
        'import importlib.util;'
        "assert importlib.util.find_spec('sky.v40_only') is None;")
    completed = _run_installed_distribution_probe(installed, main_wheel,
                                                  runtime_wheel)
    assert completed.returncode == 0, completed.stderr
    assert not list(installed.rglob('__pycache__'))
    completed = subprocess.run([
        sys.executable, '-I', '-c',
        f'import sys; sys.path.insert(0, {str(installed)!r});' + baseline_probe
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr


def test_installed_distribution_probe_hardens_active_package_identity(
        _isolated_bundle_cache, tmp_path):
    bundle, digest = wheel_utils.build_sky_wheel()
    manifest, _ = worker_runtime_packaging.verify_internal_bundle(
        bundle, expected_digest=digest)
    main_wheel = bundle / manifest.wheels[0].filename
    runtime_wheel = bundle / manifest.wheels[1].filename
    installed = tmp_path / 'installed'
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'install', '--no-index', '--no-deps',
        '--target',
        str(installed),
        str(main_wheel),
        str(runtime_wheel)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr

    completed = _run_installed_distribution_probe(installed, main_wheel,
                                                  runtime_wheel)
    assert completed.returncode == 0, completed.stderr
    assert not list(installed.rglob('__pycache__'))

    main_dist_info = next(installed.glob('skypilot-*.dist-info'))
    duplicate_dist_info = installed / 'renamed-skypilot-1.1.0.dist-info'
    shutil.copytree(main_dist_info, duplicate_dist_info)
    completed = _run_installed_distribution_probe(installed, main_wheel,
                                                  runtime_wheel)
    assert completed.returncode != 0
    assert 'distribution metadata is ambiguous' in completed.stderr
    shutil.rmtree(duplicate_dist_info)

    bad_wheel_dir = tmp_path / 'data-wheel'
    bad_wheel_dir.mkdir()
    data_wheel = bad_wheel_dir / main_wheel.name
    shutil.copy2(main_wheel, data_wheel)
    data_member = zipfile.ZipInfo('skypilot-1.1.0.data/scripts/sky')
    data_member.external_attr = 0o100644 << 16
    with zipfile.ZipFile(data_wheel, 'a') as wheel:
        wheel.writestr(data_member, '#!/bin/sh\n')
    completed = _run_installed_distribution_probe(installed, data_wheel,
                                                  runtime_wheel)
    assert completed.returncode != 0
    assert 'uses unsupported .data' in completed.stderr

    unexpected_link = installed / 'sky/unexpected_link.py'
    unexpected_link.symlink_to(installed / 'sky/__init__.py')
    completed = _run_installed_distribution_probe(installed, main_wheel,
                                                  runtime_wheel)
    assert completed.returncode != 0
    assert 'unexpected file' in completed.stderr
    unexpected_link.unlink()

    shadow_root = tmp_path / 'shadow'
    shadow_package = shadow_root / 'sky'
    shadow_package.mkdir(parents=True)
    (shadow_package / '__init__.py').write_text("__version__ = '1.1.0'\n")
    completed = _run_installed_distribution_probe(installed,
                                                  main_wheel,
                                                  runtime_wheel,
                                                  shadow_root=shadow_root)
    assert completed.returncode != 0
    assert 'distribution metadata is ambiguous' in completed.stderr

    bytecode_root = installed / 'sky/__pycache__'
    bytecode_root.mkdir()
    (bytecode_root / 'forged.cpython-311.pyc').write_bytes(b'forged')
    completed = _run_installed_distribution_probe(installed, main_wheel,
                                                  runtime_wheel)
    assert completed.returncode == 0, completed.stderr
    assert not bytecode_root.exists()

    uv = shutil.which('uv')
    if uv is not None:
        uv_installed = tmp_path / 'uv-installed'
        completed = subprocess.run([
            uv, 'pip', 'install', '--target',
            str(uv_installed), '--no-index', '--no-deps',
            str(main_wheel),
            str(runtime_wheel)
        ],
                                   capture_output=True,
                                   text=True,
                                   check=False)
        assert completed.returncode == 0, completed.stderr
        completed = _run_installed_distribution_probe(uv_installed, main_wheel,
                                                      runtime_wheel)
        assert completed.returncode == 0, completed.stderr
        assert not list(uv_installed.rglob('__pycache__'))


def test_remote_installer_verifies_before_mutation_and_marks_last():
    expected_digest = '0' * 64
    command = constants.SKYPILOT_WHEEL_INSTALLATION_COMMANDS.replace(
        '{sky_wheel_hash}', expected_digest).replace('{cloud}', 'aws')
    assert '\n' not in command
    assert len(command.encode('utf-8')) <= 16 * 1024
    assert 'skypilot-*.whl' not in command
    local_install = ('install "${SKYPILOT_MAIN_WHEEL}[aws, remote]" '
                     '"${SKYPILOT_RUNTIME_WHEEL}"')
    assert local_install in command
    verifier_capture = ('_sky_bundle_exports="$(')
    assert command.index(verifier_capture) < command.index(
        'eval "$_sky_bundle_exports"')
    assert f'"$_sky_bundle_dir" "{expected_digest}"' in command
    assert command.count(constants._installed_distribution_probe) == 1
    probe_arguments = ('"$SKYPILOT_MAIN_WHEEL" '
                       '"$SKYPILOT_RUNTIME_WHEEL"')
    assert command.count(probe_arguments) == 1
    uninstall = command.index('uninstall skypilot skypilot-worker-runtime-v1')
    assert command.index('eval "$_sky_bundle_exports"') < command.index(
        '_sky_probe;') < uninstall
    assert command.index(local_install) < command.rindex('_sky_probe &&')
    assert command.rindex('_sky_probe &&') < command.index('_sky_marker_tmp=')
    completed = subprocess.run(['bash', '-n'],
                               input=command,
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr


def test_encoded_remote_bundle_verifier_accepts_exact_bundle(
        _isolated_bundle_cache):
    bundle, digest = wheel_utils.build_sky_wheel()
    manifest, _ = worker_runtime_packaging.verify_internal_bundle(
        bundle, expected_digest=digest)
    encoded = shlex.split(constants._wheel_bundle_verifier)
    assert len(encoded) == 1

    completed = subprocess.run(
        [sys.executable, '-c', encoded[0],
         str(bundle), digest],
        capture_output=True,
        text=True,
        check=False)
    assert completed.returncode == 0, completed.stderr
    assignments = dict(
        line.split('=', 1) for line in completed.stdout.splitlines())
    assert shlex.split(assignments['SKYPILOT_MAIN_WHEEL']) == [
        str(bundle / manifest.wheels[0].filename)
    ]
    assert shlex.split(assignments['SKYPILOT_RUNTIME_WHEEL']) == [
        str(bundle / manifest.wheels[1].filename)
    ]

    completed = subprocess.run(
        [sys.executable, '-c', encoded[0],
         str(bundle), '0' * 64],
        capture_output=True,
        text=True,
        check=False)
    assert completed.returncode != 0
    assert 'bundle digest mismatch' in completed.stderr


def test_rendered_remote_installer_accepts_valid_marked_bundle(
        _isolated_bundle_cache, tmp_path):
    bundle, digest = wheel_utils.build_sky_wheel()
    manifest, _ = worker_runtime_packaging.verify_internal_bundle(
        bundle, expected_digest=digest)
    home = tmp_path / 'home'
    remote_bundle = home / '.sky/wheels' / digest
    remote_bundle.parent.mkdir(parents=True)
    shutil.copytree(bundle, remote_bundle)
    marker = remote_bundle.parent / 'current_sky_wheel_hash'
    marker.write_text(f'{digest}\n')

    runtime_env = home / 'skypilot-runtime'
    subprocess.run([sys.executable, '-m', 'venv', str(runtime_env)], check=True)
    runtime_python = runtime_env / 'bin/python'
    host_purelib = subprocess.check_output([
        sys.executable, '-c',
        'import sysconfig; print(sysconfig.get_paths()["purelib"])'
    ],
                                           text=True).strip()
    runtime_purelib = subprocess.check_output([
        runtime_python, '-c',
        'import sysconfig; print(sysconfig.get_paths()["purelib"])'
    ],
                                              text=True).strip()
    (Path(runtime_purelib) /
     'host-test-dependencies.pth').write_text(f'{host_purelib}\n')
    completed = subprocess.run([
        runtime_python, '-m', 'pip', 'install', '--no-index', '--no-deps',
        '--force-reinstall',
        str(remote_bundle / manifest.wheels[0].filename),
        str(remote_bundle / manifest.wheels[1].filename)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    python_path = home / '.sky/python_path'
    python_path.write_text(f'{runtime_python}\n')
    fake_uv = home / '.local/bin/uv'
    fake_uv.parent.mkdir(parents=True)
    fake_uv.write_text('#!/bin/sh\n'
                       'if [ "$1" = "-V" ]; then exit 0; fi\n'
                       'touch "$HOME/runtime-mutated"\n'
                       'exit 97\n')
    fake_uv.chmod(0o755)

    command = constants.SKYPILOT_WHEEL_INSTALLATION_COMMANDS.replace(
        '{sky_wheel_hash}', digest).replace('{cloud}', 'aws')
    env = os.environ.copy()
    env['HOME'] = str(home)
    env.pop('SKY_RUNTIME_DIR', None)
    env.pop('VIRTUAL_ENV', None)
    completed = subprocess.run(['bash', '-c', command],
                               cwd=tmp_path,
                               env=env,
                               capture_output=True,
                               text=True,
                               check=False)

    assert completed.returncode == 0, completed.stderr
    assert not (home / 'runtime-mutated').exists()
    assert marker.read_text() == f'{digest}\n'


def test_remote_python_script_encoding_is_yaml_safe_and_round_trips():
    script = "value = {'unsafe': lambda: 1}\nassert value['unsafe']() == 1"
    encoded = constants._quote_python_script(script)

    assert '\n' not in encoded
    assert ': ' not in encoded
    arguments = shlex.split(encoded)
    assert len(arguments) == 1
    completed = subprocess.run([sys.executable, '-c', arguments[0]],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr


def test_remote_installer_verification_failure_precedes_mutation(tmp_path):
    home = tmp_path / 'home'
    fake_uv = home / '.local/bin/uv'
    fake_uv.parent.mkdir(parents=True)
    fake_uv.write_text('#!/bin/sh\n'
                       'if [ "$1" = "-V" ]; then exit 0; fi\n'
                       'touch "$HOME/runtime-mutated"\n'
                       'exit 0\n')
    fake_uv.chmod(0o755)
    marker = home / '.sky/wheels/current_sky_wheel_hash'
    marker.parent.mkdir(parents=True)
    marker.write_text('prior-qualified-bundle\n')
    command = constants.SKYPILOT_WHEEL_INSTALLATION_COMMANDS.replace(
        '{sky_wheel_hash}', '0' * 64).replace('{cloud}', 'aws')
    env = os.environ.copy()
    env['HOME'] = str(home)

    completed = subprocess.run(['bash', '-c', command],
                               cwd=tmp_path,
                               env=env,
                               capture_output=True,
                               text=True,
                               check=False)

    assert completed.returncode != 0
    assert not (home / 'runtime-mutated').exists()
    assert marker.read_text() == 'prior-qualified-bundle\n'


def test_verified_runtime_replaces_same_version_install(tmp_path):
    uv = shutil.which('uv')
    if uv is None:
        pytest.skip('uv is required to qualify remote replacement semantics')

    copied_repo = tmp_path / 'unqualified-source'
    copied_project = (copied_repo / 'addons' / 'submission-containment' /
                      'python-runtime')
    copied_project.parent.mkdir(parents=True)
    shutil.copytree(_WORKER_RUNTIME_PROJECT, copied_project)
    copied_setup_files = copied_repo / 'sky/setup_files'
    copied_setup_files.mkdir(parents=True)
    shutil.copy2(_REPO_ROOT / 'sky/setup_files/dependencies.py',
                 copied_setup_files)
    unqualified_init = (copied_project / 'src' /
                        'skypilot_worker_runtime/__init__.py')
    unqualified_init.write_text(unqualified_init.read_text() +
                                "UNQUALIFIED_SENTINEL = True\n")
    unqualified_wheel = worker_runtime_packaging.build_worker_runtime_wheel(
        copied_repo, tmp_path / 'unqualified-wheel')
    qualified_wheel = (
        worker_runtime_packaging.load_release_worker_runtime_artifact(
            _REPO_ROOT / 'sky/skylet/runtime_wheels/v1',
            dependencies.COORDINATED_WORKER_RUNTIME_PACKAGE_VERSION))

    runtime_env = tmp_path / 'runtime-env'
    subprocess.run([sys.executable, '-m', 'venv', str(runtime_env)], check=True)
    runtime_python = runtime_env / 'bin/python'
    subprocess.run([
        uv, 'pip', 'install', '--python',
        str(runtime_python), '--no-deps',
        str(unqualified_wheel)
    ],
                   check=True,
                   capture_output=True,
                   text=True)
    subprocess.run([
        runtime_python, '-I', '-c',
        'import skypilot_worker_runtime as runtime; '
        'assert runtime.UNQUALIFIED_SENTINEL is True'
    ],
                   check=True)

    subprocess.run([
        uv, 'pip', 'uninstall', '--python',
        str(runtime_python), 'skypilot',
        worker_runtime_packaging.WORKER_RUNTIME_DISTRIBUTION
    ],
                   check=True,
                   capture_output=True,
                   text=True)
    subprocess.run([
        uv, 'pip', 'install', '--python',
        str(runtime_python), '--no-deps',
        str(qualified_wheel)
    ],
                   check=True,
                   capture_output=True,
                   text=True)
    subprocess.run([
        runtime_python, '-I', '-c',
        'import skypilot_worker_runtime as runtime; '
        'assert not hasattr(runtime, "UNQUALIFIED_SENTINEL")'
    ],
                   check=True)


@pytest.fixture
def _current_version_guard():
    """Keep subprocess mocks focused on the pip invocation under test."""
    with mock.patch.object(common,
                           'get_skypilot_version_on_disk',
                           return_value=sky.__version__):
        yield


def test_wheel_build_version_guard_uses_canonical_version():
    assert common.get_skypilot_version_on_disk() == sky.__version__
    assert not hasattr(sky, '__display_version__')

    pip_error = subprocess.CalledProcessError(
        returncode=1,
        cmd=[sys.executable, '-m', 'pip', 'wheel'],
        stderr='intentional regression-test failure')
    with mock.patch('subprocess.run', side_effect=pip_error):
        with pytest.raises(RuntimeError, match='pip wheel command failed'):
            wheel_utils._build_sky_wheel()


def test_wheel_build_version_guard_rejects_stale_version():
    """A running server must not build a wheel from a newer checkout."""
    with mock.patch.object(common,
                           'get_skypilot_version_on_disk',
                           return_value='1.999999.0'), \
         mock.patch('subprocess.run') as mock_run:
        with pytest.raises(RuntimeError,
                           match='installed SkyPilot version is different'):
            wheel_utils._build_sky_wheel()
        mock_run.assert_not_called()


def test_build_wheels(_isolated_bundle_cache):
    shutil.rmtree(wheel_utils.WHEEL_DIR, ignore_errors=True)
    start = time.time()
    wheel_path, _ = wheel_utils.build_sky_wheel()
    assert wheel_path.exists()
    if sky.__build__ is not None:
        built_wheel = next(wheel_path.glob('skypilot-*.whl'))
        with zipfile.ZipFile(built_wheel) as wheel:
            init_content = wheel.read('sky/__init__.py').decode('utf-8')
        assert (f"_SKYPILOT_COMMIT_COUNT = '{sky.__build__}'" in init_content)
        if sky.__commit_timestamp__ is not None:
            assert (f"_SKYPILOT_COMMIT_TIMESTAMP = "
                    f"'{sky.__commit_timestamp__}'" in init_content)
    duration = time.time() - start

    start = time.time()
    wheel_path, _ = wheel_utils.build_sky_wheel()
    assert wheel_path.exists()
    duration_cached = time.time() - start

    assert duration_cached < duration

    # simulate uncleaned symlinks due to interruption
    shutil.rmtree(wheel_utils.WHEEL_DIR, ignore_errors=True)
    wheel_utils.WHEEL_DIR.mkdir()
    (wheel_utils.WHEEL_DIR / 'sky').symlink_to(wheel_utils.SKY_PACKAGE_PATH,
                                               target_is_directory=True)
    for root, _, _ in os.walk(str(wheel_utils.WHEEL_DIR)):
        # set file date to 1970-01-01 00:00 UTC
        os.utime(root, (0, 0))
    wheel_path, _ = wheel_utils.build_sky_wheel()
    assert wheel_path.exists()

    shutil.rmtree(wheel_utils.WHEEL_DIR, ignore_errors=True)


def test_build_wheel_without_commit_timestamp_ignores_ambient_git(tmp_path):
    isolated_wheel_dir = tmp_path / 'wheels'
    isolated_lock_path = tmp_path / 'wheels.lock'
    with mock.patch.object(wheel_utils, 'WHEEL_DIR', isolated_wheel_dir), \
         mock.patch.object(wheel_utils, '_WHEEL_LOCK_PATH',
                           isolated_lock_path), \
         mock.patch.object(sky, '__commit_timestamp__', None):
        built_wheel = wheel_utils._build_sky_wheel()

    unrelated_repo = tmp_path / 'unrelated'
    unrelated_repo.mkdir()
    subprocess.run(['git', 'init', '--quiet'], cwd=unrelated_repo, check=True)
    (unrelated_repo / 'marker').write_text('unrelated', encoding='utf-8')
    subprocess.run(['git', 'add', 'marker'], cwd=unrelated_repo, check=True)
    commit_env = os.environ.copy()
    commit_env.update({
        'GIT_AUTHOR_NAME': 'SkyPilot Test',
        'GIT_AUTHOR_EMAIL': 'test@skypilot.co',
        'GIT_COMMITTER_NAME': 'SkyPilot Test',
        'GIT_COMMITTER_EMAIL': 'test@skypilot.co',
    })
    subprocess.run(['git', 'commit', '--quiet', '-m', 'unrelated'],
                   cwd=unrelated_repo,
                   env=commit_env,
                   check=True)

    import_env = os.environ.copy()
    import_env['PYTHONPATH'] = str(built_wheel)
    imported = subprocess.run([
        sys.executable, '-c', 'import sky; print(sky.__file__); '
        'print(repr(sky.__commit_timestamp__))'
    ],
                              cwd=unrelated_repo,
                              env=import_env,
                              capture_output=True,
                              text=True,
                              check=False)
    assert imported.returncode == 0, imported.stderr
    output_lines = imported.stdout.strip().splitlines()
    assert str(built_wheel) in output_lines[-2]
    assert output_lines[-1] == 'None'


def test_setup_uses_explicit_absent_timestamp_without_git(tmp_path):
    setup_module = _load_setup_module()
    init_path = tmp_path / '__init__.py'
    init_path.write_text(
        "_SKYPILOT_COMMIT_TIMESTAMP = '{{SKYPILOT_COMMIT_TIMESTAMP}}'\n",
        encoding='utf-8')

    with mock.patch.object(setup_module, 'INIT_FILE_PATH', str(init_path)), \
         mock.patch.object(setup_module.subprocess,
                           'check_output',
                           side_effect=FileNotFoundError('git unavailable')):
        assert setup_module.get_commit_timestamp() == ''


def test_pip_missing_uv_environment(_current_version_guard):
    """Test error handling when pip module is not found in UV environment."""
    with mock.patch('subprocess.run') as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=[sys.executable, '-m', 'pip'],
            stderr='/path/to/python: No module named pip')

        with mock.patch('shutil.which') as mock_which:
            # Simulate UV is available
            mock_which.return_value = '/usr/bin/uv'

            with pytest.raises(RuntimeError) as exc_info:
                wheel_utils._build_sky_wheel()

            error_msg = str(exc_info.value)
            assert 'pip module not found' in error_msg
            assert 'Since you have UV installed' in error_msg
            assert 'uv pip install pip' in error_msg


def test_pip_missing_conda_environment(_current_version_guard):
    """Test error handling when pip module is not found in conda environment."""
    with mock.patch('subprocess.run') as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=[sys.executable, '-m', 'pip'],
            stderr='/path/to/python: No module named pip')

        with mock.patch('shutil.which') as mock_which:

            def which_side_effect(cmd):
                if cmd == 'uv':
                    return None
                elif cmd == 'conda':
                    return '/usr/bin/conda'
                return None

            mock_which.side_effect = which_side_effect

            with pytest.raises(RuntimeError) as exc_info:
                wheel_utils._build_sky_wheel()

            error_msg = str(exc_info.value)
            assert 'pip module not found' in error_msg
            assert 'Since you have conda installed' in error_msg
            assert 'conda install pip' in error_msg


def test_pip_missing_no_package_manager(_current_version_guard):
    """Test error handling when pip is missing with no known package manager."""
    with mock.patch('subprocess.run') as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=[sys.executable, '-m', 'pip'],
            stderr='/path/to/python: No module named pip')

        with mock.patch('shutil.which') as mock_which:
            mock_which.return_value = None

            with pytest.raises(RuntimeError) as exc_info:
                wheel_utils._build_sky_wheel()

            error_msg = str(exc_info.value)
            assert 'pip module not found' in error_msg
            assert 'Please install pip for your Python environment' in error_msg
            assert sys.executable in error_msg


def test_pip_command_other_failure(_current_version_guard):
    """Test error handling when pip command fails for non-module reasons."""
    with mock.patch('subprocess.run') as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=[sys.executable, '-m', 'pip', 'wheel'],
            stderr=
            'ERROR: Directory /tmp/something is not installable. Neither setup.py nor pyproject.toml found.'
        )

        with pytest.raises(RuntimeError) as exc_info:
            wheel_utils._build_sky_wheel()

        error_msg = str(exc_info.value)
        assert 'pip wheel command failed' in error_msg
        assert 'Directory /tmp/something is not installable' in error_msg


def test_python_executable_not_found(_current_version_guard):
    """Test error handling when Python executable is not found (rare case)."""
    with mock.patch('subprocess.run') as mock_run:
        mock_run.side_effect = FileNotFoundError(
            "[Errno 2] No such file or directory: '/nonexistent/python'")

        with pytest.raises(RuntimeError) as exc_info:
            wheel_utils._build_sky_wheel()

        error_msg = str(exc_info.value)
        assert 'Python executable not found' in error_msg
        assert sys.executable in error_msg


def test_python_m_pip_usage(_current_version_guard):
    """Test that we use 'python -m pip' instead of 'pip3'."""
    # This is a simpler test that just verifies the command format
    # The actual wheel building is tested in test_wheels.py

    # Create a minimal mock that captures the subprocess.run call
    with mock.patch('subprocess.run') as mock_run:
        # Make subprocess.run fail with a specific error so we can catch it
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=[sys.executable, '-m', 'pip'],
            stderr='Intentional test failure')

        try:
            wheel_utils._build_sky_wheel()
        except RuntimeError:
            # We expect this to fail, we just want to check the command
            pass

        # Verify that subprocess.run was called with python -m pip
        mock_run.assert_called()
        call_args = mock_run.call_args[0][0]

        # Check the command format
        assert call_args[0] == sys.executable
        assert call_args[1:3] == ['-m', 'pip']
        assert 'wheel' in call_args
        assert '--no-deps' in call_args

        # Verify we're NOT using pip3
        assert 'pip3' not in call_args


def test_wheel_build_reproducible(tmp_path):
    """Test that wheel builds are reproducible across separate processes.

    PYTHONHASHSEED is randomized per-process, so without the fix (setting
    SOURCE_DATE_EPOCH and PYTHONHASHSEED in the pip subprocess env), two
    separate processes building the same source can produce different wheel
    hashes due to non-deterministic metadata ordering and zip timestamps.

    We simulate this by running each build in a separate subprocess,
    just like different API server replicas would. Each subprocess gets
    a naturally randomized PYTHONHASHSEED.
    """
    build_root = tmp_path / 'reproducible'
    build_script = f'''
import pathlib
import shutil
import tempfile
from sky.backends import wheel_utils
root = pathlib.Path({str(build_root)!r})
wheel_utils.WHEEL_DIR = root / 'cache'
wheel_utils._WHEEL_LOCK_PATH = root / 'cache.lock'
tempfile.gettempdir = lambda: str(root / 'transfer')
(root / 'transfer').mkdir(parents=True, exist_ok=True)
shutil.rmtree(wheel_utils.WHEEL_DIR, ignore_errors=True)
_, digest = wheel_utils.build_sky_wheel()
print(digest)
'''

    try:
        result1 = subprocess.run([sys.executable, '-c', build_script],
                                 capture_output=True,
                                 text=True,
                                 check=True)
        hash1 = result1.stdout.strip().splitlines()[-1]

        result2 = subprocess.run([sys.executable, '-c', build_script],
                                 capture_output=True,
                                 text=True,
                                 check=True)
        hash2 = result2.stdout.strip().splitlines()[-1]

        assert hash1 == hash2, (
            f'Wheel build is not reproducible across processes: '
            f'{hash1} != {hash2}. '
            'Check that SOURCE_DATE_EPOCH and PYTHONHASHSEED are set in the '
            'pip wheel subprocess environment in _build_sky_wheel().')
    finally:
        shutil.rmtree(build_root, ignore_errors=True)
