"""Stamp one exact release identity into the canonical source image."""

import argparse
import dataclasses
import datetime
import pathlib
import re
import typing


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class ReleaseIdentity:
    """Immutable provenance shared by Python and OCI image metadata."""

    version: str
    commit: str
    commit_timestamp: str
    commit_count: str

    def __post_init__(self) -> None:
        if re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', self.version) is None:
            raise ValueError(f'invalid release version: {self.version!r}')
        if re.fullmatch(r'[0-9a-f]{40,64}(?:-dirty)?', self.commit) is None:
            raise ValueError(f'invalid release commit: {self.commit!r}')
        parsed_timestamp = datetime.datetime.fromisoformat(
            self.commit_timestamp)
        if parsed_timestamp.tzinfo is None:
            raise ValueError('release commit timestamp must be timezone-aware')
        if (not self.commit_count.isdigit() or int(self.commit_count) <= 0):
            raise ValueError(
                f'invalid release commit count: {self.commit_count!r}')


def _replace_exact(content: str, pattern: str, replacement: str,
                   path: pathlib.Path) -> str:
    compiled = re.compile(pattern, flags=re.MULTILINE)
    if len(tuple(compiled.finditer(content))) != 1:
        raise RuntimeError(f'could not stamp release identity in {path}')
    return compiled.sub(replacement, content, count=1)


def stamp_release(root: pathlib.Path, identity: ReleaseIdentity, *,
                  install_policy: bool) -> None:
    """Validate every projection, then stamp them from one identity.

    No file is changed until every required pattern has been validated.  This
    prevents a malformed source tree from producing mixed distribution
    metadata after an otherwise recoverable build failure.
    """
    sky_init = root / 'sky' / '__init__.py'
    policy_root = root / 'boltz' / 'reserved_fill_reclaim_policy'
    policy_project = policy_root / 'pyproject.toml'
    policy_init = (policy_root / 'src' / 'boltz_reserved_fill_reclaim_policy' /
                   '__init__.py')
    inputs = {sky_init: sky_init.read_text(encoding='utf-8')}
    if install_policy:
        inputs[policy_project] = policy_project.read_text(encoding='utf-8')
        inputs[policy_init] = policy_init.read_text(encoding='utf-8')

    sky_content = inputs[sky_init]
    for name, value in (
        ('_SKYPILOT_COMMIT_SHA', identity.commit),
        ('_SKYPILOT_COMMIT_TIMESTAMP', identity.commit_timestamp),
        ('_SKYPILOT_COMMIT_COUNT', identity.commit_count),
    ):
        sky_content = _replace_exact(sky_content,
                                     rf'^{name} = [\'\"][^\'\"]*[\'\"]',
                                     f'{name} = {value!r}', sky_init)
    sky_content = _replace_exact(sky_content,
                                 r'^__version__ = [\'\"][^\'\"]*[\'\"]',
                                 f'__version__ = {identity.version!r}',
                                 sky_init)
    outputs = {sky_init: sky_content}

    if install_policy:
        outputs[policy_project] = _replace_exact(
            inputs[policy_project], r'^version = "0\.0\.0"$',
            f'version = "{identity.version}"', policy_project)
        outputs[policy_init] = _replace_exact(
            inputs[policy_init], r"^__version__ = '0\.0\.0'$",
            f'__version__ = {identity.version!r}', policy_init)

    for path, content in outputs.items():
        path.write_text(content, encoding='utf-8')


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=pathlib.Path, required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--commit-timestamp', required=True)
    parser.add_argument('--commit-count', required=True)
    parser.add_argument('--install-policy',
                        choices=('true', 'false'),
                        required=True)
    return parser


def main(argv: typing.Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    metadata = (args.version, args.commit, args.commit_timestamp,
                args.commit_count)
    install_policy = args.install_policy == 'true'
    if not any(metadata):
        if install_policy:
            raise ValueError('the Boltz policy requires an exact identity')
        return 0
    if not all(metadata):
        raise ValueError('release identity arguments must be supplied together')
    identity = ReleaseIdentity(version=args.version,
                               commit=args.commit,
                               commit_timestamp=args.commit_timestamp,
                               commit_count=args.commit_count)
    stamp_release(args.root, identity, install_policy=install_policy)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
