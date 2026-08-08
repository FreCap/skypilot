"""Resolve non-package Python sources required by the boltz overlay wheel."""

import argparse
import ast
from collections.abc import Sequence
import pathlib
import re

_MODULE_NAME = re.compile(r'^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$')


def declared_py_module_sources(
        setup_path: pathlib.Path) -> tuple[pathlib.Path, ...]:
    """Return setup.py's literal ``py_modules`` entries as source paths."""
    source = setup_path.read_text(encoding='utf-8')
    tree = ast.parse(source, filename=str(setup_path))
    declarations: list[object] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != 'py_modules':
                continue
            try:
                declarations.append(ast.literal_eval(keyword.value))
            except (ValueError, TypeError, SyntaxError) as error:
                raise ValueError(
                    'setup.py py_modules must be a literal sequence') from error

    if len(declarations) != 1:
        raise ValueError('setup.py must declare py_modules exactly once')
    modules = declarations[0]
    if (not isinstance(modules, (list, tuple)) or not modules or
            any(not isinstance(module, str) or
                _MODULE_NAME.fullmatch(module) is None for module in modules)):
        raise ValueError('setup.py py_modules is invalid')
    if len(modules) != len(set(modules)):
        raise ValueError('setup.py py_modules contains duplicates')

    # setup.py is a repository-root symlink to sky/setup_files/setup.py.  Wheel
    # module paths are relative to its invocation directory, not its symlink
    # target.
    root = setup_path.absolute().parent
    sources = tuple(
        pathlib.Path(*module.split('.')).with_suffix('.py')
        for module in modules)
    for relative_path in sources:
        source_path = root / relative_path
        if not source_path.is_file():
            raise ValueError(f'declared py_module source is missing: '
                             f'{relative_path.as_posix()}')
    return sources


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--setup',
                        type=pathlib.Path,
                        default=pathlib.Path('setup.py'))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    for path in declared_py_module_sources(args.setup):
        print(path.as_posix())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
