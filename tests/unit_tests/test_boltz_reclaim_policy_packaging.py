"""Packaging boundary tests for the deployment-only reclaim plugin."""

import pathlib
import shutil
import subprocess
import sys
import tomllib
import zipfile

_REPO_ROOT = pathlib.Path(__file__).parents[2]
_PROJECT = _REPO_ROOT / 'boltz' / 'reserved_fill_reclaim_policy'
_GROUP = 'skypilot.reserved_fill_reclaim_policy'


def test_generic_distribution_stays_entry_point_free():
    setup = (_REPO_ROOT / 'sky' / 'setup_files' /
             'setup.py').read_text(encoding='utf-8')
    root_setup = (_REPO_ROOT / 'setup.py').read_text(encoding='utf-8')

    assert _GROUP not in setup
    assert _GROUP not in root_setup


def test_policy_project_declares_exactly_one_entry_point():
    document = tomllib.loads(
        (_PROJECT / 'pyproject.toml').read_text(encoding='utf-8'))

    assert document['project']['entry-points'] == {
        _GROUP: {
            'boltz': ('boltz_reserved_fill_reclaim_policy.policy:'
                      'BoltzReservedFillReclaimPolicy')
        }
    }
    assert document['tool']['setuptools']['package-data'] == {
        'boltz_reserved_fill_reclaim_policy': ['fleet_bundle.json']
    }


def test_policy_wheel_contains_bundle_and_only_policy_entry_point(tmp_path):
    project_copy = tmp_path / 'policy-project'
    shutil.copytree(_PROJECT, project_copy)
    completed = subprocess.run([
        sys.executable, '-m', 'pip', 'wheel', '--no-deps',
        '--no-build-isolation', '--wheel-dir',
        str(tmp_path),
        str(project_copy)
    ],
                               capture_output=True,
                               text=True,
                               check=False)
    assert completed.returncode == 0, completed.stderr
    wheel_path = next(tmp_path.glob('*.whl'))
    with zipfile.ZipFile(wheel_path) as wheel:
        names = wheel.namelist()
        assert ('boltz_reserved_fill_reclaim_policy/fleet_bundle.json' in names)
        entry_points = next(name for name in names
                            if name.endswith('.dist-info/entry_points.txt'))
        assert wheel.read(entry_points).decode('utf-8') == (
            f'[{_GROUP}]\n'
            'boltz = boltz_reserved_fill_reclaim_policy.policy:'
            'BoltzReservedFillReclaimPolicy\n')
        assert not any(name.startswith('sky/') for name in names)


def test_overlay_builds_and_installs_both_distributions():
    dockerfile = (_REPO_ROOT / 'boltz' /
                  'Dockerfile.overlay').read_text(encoding='utf-8')
    script = (_REPO_ROOT / 'boltz' /
              'build-overlay.sh').read_text(encoding='utf-8')

    assert '/tmp/reserved-fill-reclaim-policy' in dockerfile
    assert '--wheel-dir /tmp/policy-wheels' in dockerfile
    assert '/tmp/policy-wheels/*.whl' in dockerfile
    assert "git ls-tree -r --name-only HEAD -- 'boltz/reserved_fill_reclaim_policy'" in script
    assert 'policy.policy_identity().policy_revision' in script
    assert "group='skypilot.reserved_fill_reclaim_policy'" in script
