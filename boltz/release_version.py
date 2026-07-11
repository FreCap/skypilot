#!/usr/bin/env python3
"""Compute the deterministic release version for Boltz SkyPilot artifacts.

The source tree intentionally keeps ``sky.__version__`` at the release-series
version (1.1.0).  Published image and chart versions use a patch number derived
from Git history instead: every commit on the integration branch's first-parent
history after the epoch advances the patch once.
"""

import argparse
import pathlib
import subprocess
import sys
import typing

VERSION_PREFIX = '1.1'
# v1.1.19 is the last release from the legacy path-filtered counter. From this
# point forward, every first-parent commit consumes exactly one patch version.
EPOCH_COMMIT = '931e89d768075bc58773cd97e25cec9e4aaa032a'
EPOCH_PATCH = 19
REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class ReleaseVersionError(RuntimeError):
    """Raised when a deterministic release version cannot be computed."""


def _run_git(repo_root: pathlib.Path,
             *args: str,
             check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(['git', '-C', str(repo_root), *args],
                            capture_output=True,
                            text=True,
                            check=False)
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        if detail:
            detail = f': {detail}'
        command = ' '.join(args)
        raise ReleaseVersionError(
            f'git {command} failed with exit code {result.returncode}{detail}')
    return result


def _resolve_commit(repo_root: pathlib.Path, ref: str) -> str:
    result = _run_git(repo_root, 'rev-parse', '--verify', '--end-of-options',
                      f'{ref}^{{commit}}')
    return result.stdout.strip()


def calculate_release_version(ref: str = 'HEAD',
                              *,
                              repo_root: typing.Union[str,
                                                      pathlib.Path] = REPO_ROOT,
                              epoch_commit: str = EPOCH_COMMIT,
                              epoch_patch: int = EPOCH_PATCH) -> str:
    """Return the deterministic ``1.1.N`` artifact version for ``ref``."""
    root = pathlib.Path(repo_root)
    resolved_epoch = _resolve_commit(root, epoch_commit)
    resolved_ref = _resolve_commit(root, ref)

    ancestry = _run_git(root,
                        'merge-base',
                        '--is-ancestor',
                        resolved_epoch,
                        resolved_ref,
                        check=False)
    if ancestry.returncode == 1:
        raise ReleaseVersionError(
            f'Ref {ref!r} ({resolved_ref}) is not descended from release '
            f'epoch {epoch_commit} ({resolved_epoch}).')
    if ancestry.returncode != 0:
        detail = ancestry.stderr.strip() or ancestry.stdout.strip()
        if detail:
            detail = f': {detail}'
        raise ReleaseVersionError(
            'Unable to verify release epoch ancestry'
            f' (git exited {ancestry.returncode}){detail}')

    count = _run_git(root, 'rev-list', '--first-parent', '--count',
                     f'{resolved_epoch}..{resolved_ref}').stdout.strip()
    try:
        patch = epoch_patch + int(count)
    except ValueError as exc:
        raise ReleaseVersionError(
            f'Git returned an invalid release commit count: {count!r}') from exc
    return f'{VERSION_PREFIX}.{patch}'


def main(argv: typing.Optional[typing.Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description='Compute the deterministic Boltz artifact release version.')
    parser.add_argument('--ref',
                        default='HEAD',
                        help='Git ref to version (default: HEAD)')
    args = parser.parse_args(argv)

    try:
        version = calculate_release_version(args.ref)
    except ReleaseVersionError as exc:
        print(f'release_version.py: error: {exc}', file=sys.stderr)
        return 1

    print(version)
    return 0


if __name__ == '__main__':
    sys.exit(main())
