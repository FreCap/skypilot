"""Pre-import entrypoint for authorization-v3 bootstrap.

This module must remain standard-library-only.  Importing ``sky`` selects and
loads process-wide configuration, so the central-server marker and read-only
migration mode have to be installed before that first import.  The complete
import and command execution also run behind process-descriptor redirection;
only the closed result returned by the implementation is emitted afterwards.
"""

from collections.abc import Iterator
from collections.abc import Sequence
import contextlib
import importlib
import io
import logging
import os
import sys
import tempfile

_DB_CONNECTION_URI_ENV_VAR = 'SKYPILOT_DB_CONNECTION_URI'
_IS_SKYPILOT_SERVER_ENV_VAR = 'IS_SKYPILOT_SERVER'
_STATE_DB_MIGRATION_MODE_ENV_VAR = 'SKYPILOT_STATE_DB_MIGRATION_MODE'
_IMPLEMENTATION_MODULE = 'sky.serve.system_oom_recovery_authorization'
_MISSING_DATABASE_ERROR = (
    'authorization-v3 bootstrap failed: Central PostgreSQL configuration is '
    'unavailable.')
_INTERNAL_ERROR = (
    'authorization-v3 bootstrap failed: internal validation failed.')


@contextlib.contextmanager
def _suppress_process_output() -> Iterator[None]:
    """Suppress Python and file-descriptor output until a closed result exists."""
    stdout_fd = os.dup(1)
    stderr_fd = os.dup(2)
    previous_logging_disable = logging.root.manager.disable
    redirected_stdout = io.StringIO()
    redirected_stderr = io.StringIO()
    try:
        if sys.__stdout__ is not None:
            sys.__stdout__.flush()
        if sys.__stderr__ is not None:
            sys.__stderr__.flush()
        with tempfile.TemporaryFile(mode='w+b') as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
            logging.disable(sys.maxsize)
            with contextlib.redirect_stdout(redirected_stdout), \
                    contextlib.redirect_stderr(redirected_stderr):
                yield
            if sys.__stdout__ is not None:
                sys.__stdout__.flush()
            if sys.__stderr__ is not None:
                sys.__stderr__.flush()
    finally:
        logging.disable(previous_logging_disable)
        os.dup2(stdout_fd, 1)
        os.dup2(stderr_fd, 2)
        os.close(stdout_fd)
        os.close(stderr_fd)


def _emit(output: str, *, is_error: bool) -> None:
    stream = sys.stderr if is_error else sys.stdout
    print(output, file=stream)


def entrypoint(argv: Sequence[str] | None = None) -> int:
    """Select central read-only state before importing the implementation."""
    if not os.environ.get(_DB_CONNECTION_URI_ENV_VAR):
        _emit(_MISSING_DATABASE_ERROR, is_error=True)
        return 1

    os.environ[_IS_SKYPILOT_SERVER_ENV_VAR] = 'true'
    os.environ[_STATE_DB_MIGRATION_MODE_ENV_VAR] = 'verify'
    result: tuple[int, str, bool] | None = None
    try:
        with _suppress_process_output():
            implementation = importlib.import_module(_IMPLEMENTATION_MODULE)
            candidate = implementation.run_cli(argv)
            if (not isinstance(candidate, tuple) or len(candidate) != 3 or
                    type(candidate[0]) is not int or
                    candidate[0] not in (0, 1) or
                    not isinstance(candidate[1], str) or not candidate[1] or
                    type(candidate[2]) is not bool):
                raise TypeError('authorization bootstrap result is invalid')
            result = candidate
    except BaseException:  # pylint: disable=broad-except
        result = (1, _INTERNAL_ERROR, True)

    assert result is not None
    exit_code, output, is_error = result
    _emit(output, is_error=is_error)
    return exit_code


if __name__ == '__main__':
    sys.exit(entrypoint())
