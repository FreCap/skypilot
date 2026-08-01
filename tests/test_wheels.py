import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from unittest import mock
import zipfile

import pytest

import sky
from sky.backends import wheel_utils
from sky.server import common


def _load_setup_module():
    setup_path = Path('setup.py').absolute()
    spec = importlib.util.spec_from_file_location('_skypilot_setup_test',
                                                  setup_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def test_build_wheels():
    shutil.rmtree(wheel_utils.WHEEL_DIR, ignore_errors=True)
    start = time.time()
    wheel_path, _ = wheel_utils.build_sky_wheel()
    assert wheel_path.exists()
    if sky.__build__ is not None:
        built_wheel = next(wheel_path.glob('*.whl'))
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


def test_wheel_build_reproducible():
    """Test that wheel builds are reproducible across separate processes.

    PYTHONHASHSEED is randomized per-process, so without the fix (setting
    SOURCE_DATE_EPOCH and PYTHONHASHSEED in the pip subprocess env), two
    separate processes building the same source can produce different wheel
    hashes due to non-deterministic metadata ordering and zip timestamps.

    We simulate this by running each build in a separate subprocess,
    just like different API server replicas would. Each subprocess gets
    a naturally randomized PYTHONHASHSEED.
    """
    build_script = (
        'import shutil, os; '
        'from sky.backends import wheel_utils; '
        'shutil.rmtree(str(wheel_utils.WHEEL_DIR), ignore_errors=True); '
        '_, h = wheel_utils.build_sky_wheel(); '
        'print(h)')

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
        shutil.rmtree(wheel_utils.WHEEL_DIR, ignore_errors=True)
