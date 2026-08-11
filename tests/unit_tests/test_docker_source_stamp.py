"""Tests for recording source identity in the production Dockerfile."""

from __future__ import annotations

import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest

_DOCKER_IGNORED_TRACKED_ROOTS = ('.github', 'docs', 'examples', 'llm', 'tests')
_PRESERVED_DOCKER_TEST_PATH = Path('tests/smoke_tests/docker')


def _extract_process_source_run(dockerfile: Path) -> str:
    contents = dockerfile.read_text(encoding='utf-8')
    start_marker = 'RUN cd /skypilot && \\\n'
    end_marker = '\n\n\n# Stage 3: Main image'
    start = contents.index(start_marker)
    end = contents.index(end_marker, start)
    instruction = contents[start + len('RUN '):end]

    # Docker removes escaped newlines before invoking the shell. Do the same
    # while dropping Dockerfile-only comment lines so this test executes the
    # production instruction rather than a copied approximation.
    shell_lines = []
    for line in instruction.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        if stripped.endswith('\\'):
            stripped = stripped[:-1].rstrip()
        shell_lines.append(stripped)
    return ' '.join(shell_lines)


def _clone_simulated_build_context(
        tmp_path: Path) -> tuple[Path, str, set[Path]]:
    repo_root = Path(__file__).resolve().parents[2]
    build_context = tmp_path / 'repo'
    subprocess.run(
        [
            'git', 'clone', '--quiet', '--shared',
            str(repo_root),
            str(build_context)
        ],
        check=True,
    )
    expected_sha = subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'],
        cwd=build_context,
        text=True,
    ).strip()

    preserved_source = build_context / _PRESERVED_DOCKER_TEST_PATH
    preserved_copy = tmp_path / 'preserved-docker-tests'
    shutil.copytree(preserved_source, preserved_copy)
    for root in _DOCKER_IGNORED_TRACKED_ROOTS:
        shutil.rmtree(build_context / root)
    preserved_source.parent.mkdir(parents=True)
    shutil.copytree(preserved_copy, preserved_source)

    deleted_output = subprocess.check_output(
        [
            'git', 'ls-files', '--deleted', '-z', '--',
            *_DOCKER_IGNORED_TRACKED_ROOTS
        ],
        cwd=build_context,
    )
    deleted_paths = {
        Path(os.fsdecode(path)) for path in deleted_output.split(b'\0') if path
    }
    assert deleted_paths
    assert not any(path == _PRESERVED_DOCKER_TEST_PATH or
                   _PRESERVED_DOCKER_TEST_PATH in path.parents
                   for path in deleted_paths)
    return build_context, expected_sha, deleted_paths


@pytest.mark.parametrize('dirty', (False, True))
def test_source_build_records_exact_clean_or_dirty_commit(
        tmp_path: Path, dirty: bool) -> None:
    build_context, expected_sha, deleted_paths = (
        _clone_simulated_build_context(tmp_path))
    if dirty:
        readme = build_context / 'README.md'
        readme.write_text(readme.read_text(encoding='utf-8') +
                          '\nAudit dirty marker.\n',
                          encoding='utf-8')

    repo_root = Path(__file__).resolve().parents[2]
    shell_instruction = _extract_process_source_run(repo_root / 'Dockerfile')
    shell_instruction = shell_instruction.replace(
        '/tmp/skypilot-docker-omitted-files', '"${SKYPILOT_TEST_OMITTED_FILE}"')
    shell_instruction = shell_instruction.replace(
        '/skypilot', '"${SKYPILOT_TEST_BUILD_CONTEXT}"')
    env = os.environ.copy()
    env.update({
        'INSTALL_FROM_SOURCE': 'true',
        'PATH': f'{Path(sys.executable).parent}{os.pathsep}{env["PATH"]}',
        'SKYPILOT_TEST_BUILD_CONTEXT': str(build_context),
        'SKYPILOT_TEST_OMITTED_FILE': str(tmp_path / 'omitted-files'),
    })
    subprocess.run(['bash', '-ceu', shell_instruction], check=True, env=env)

    init_contents = (build_context /
                     'sky/__init__.py').read_text(encoding='utf-8')
    match = re.search(r"^_SKYPILOT_COMMIT_SHA = '([^']+)'$", init_contents,
                      re.MULTILINE)
    assert match is not None
    expected_stamp = f'{expected_sha}-dirty' if dirty else expected_sha
    assert match.group(1) == expected_stamp
    assert not (build_context / '.git').exists()
    assert all(not (build_context / path).exists() for path in deleted_paths)
    assert (build_context / _PRESERVED_DOCKER_TEST_PATH).is_dir()
