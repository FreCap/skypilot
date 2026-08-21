"""Packaging boundary tests for the deployment-only reclaim plugin."""

import ast
import hashlib
import pathlib
import shutil
import subprocess
import sys
import tomllib
import zipfile

_REPO_ROOT = pathlib.Path(__file__).parents[2]
_PROJECT = _REPO_ROOT / 'boltz' / 'reserved_fill_reclaim_policy'
_GROUP = 'skypilot.reserved_fill_reclaim_policy'
_POLICY_PACKAGE = (_PROJECT / 'src' / 'boltz_reserved_fill_reclaim_policy')


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


def test_policy_contract_revision_is_independent_from_artifact_version():
    package = (_POLICY_PACKAGE / '__init__.py').read_text(encoding='utf-8')
    bundle = (_POLICY_PACKAGE / 'bundle.py').read_text(encoding='utf-8')

    package_tree = ast.parse(package)
    assert (isinstance(package_tree.body[0], ast.Expr) and
            isinstance(package_tree.body[0].value, ast.Constant) and
            isinstance(package_tree.body[0].value.value, str))
    assignments = {}
    for statement in package_tree.body[1:]:
        assert (isinstance(statement, ast.Assign) and
                len(statement.targets) == 1 and
                isinstance(statement.targets[0], ast.Name) and
                isinstance(statement.value, ast.Constant) and
                isinstance(statement.value.value, str))
        assignments[statement.targets[0].id] = statement.value.value
    # The package initializer stays side-effect free. The build may stamp only
    # __version__; executable policy authority remains review-owned source.
    assert assignments == {
        '__version__': '0.0.0',
        'POLICY_REVISION': '1.1.1422',
    }
    # This is the exact already-authorized production policy contract.  An
    # executable policy change must deliberately advance it; ordinary overlay
    # releases must not.
    assert ('from boltz_reserved_fill_reclaim_policy import POLICY_REVISION'
            in bundle)
    assert '__version__' not in bundle

    implementation_digest = hashlib.sha256()
    for path in sorted(_POLICY_PACKAGE.glob('*.py')):
        if path.name == '__init__.py':
            continue
        relative = path.relative_to(_POLICY_PACKAGE).as_posix().encode()
        content = path.read_bytes()
        implementation_digest.update(len(relative).to_bytes(4, 'big'))
        implementation_digest.update(relative)
        implementation_digest.update(len(content).to_bytes(8, 'big'))
        implementation_digest.update(content)
    reviewed_revisions = {
        '273e97f99668a2639b1d4898503864e716231e23bf46bdf53e3095e357170c45': '1.1.1358',
        'c24eee0b822482c34d6177583094da756ce4ddef7d07f5783b3683bfeb81ef37': '1.1.1386',
        'f1669f55fa671cf037835867a773389792fba7871b2ffa03922b7d9e2ddd41e0': '1.1.1386',
        'c7a7c52526f96eb6dc5e1abab069fc3b0eec9f2cd2804f879f543ee4d030e831': '1.1.1415',
        '1835a060709064448fc5bc6560ebd5b7b265857f0a0ce20fe7eef09baa4944a1': '1.1.1416',
        'f7a8962ecbc54f4327a446cee1685c56cf9bb84963696d4734a2192a0a556de2': '1.1.1422',
    }
    assert reviewed_revisions[implementation_digest.hexdigest()] == (
        assignments['POLICY_REVISION'])


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
    assert 'boltz_reserved_fill_reclaim_policy.POLICY_REVISION' in script
    assert ("'boltz-reserved-fill-reclaim-policy/' + sky.__version__"
            not in script)
    assert "group='skypilot.reserved_fill_reclaim_policy'" in script
