"""Standard-library bootstrap for one managed-job ControllerManager.

This file is executed by path.  Keep every top-level import in the standard
library: the bootstrap must protect and consume the raw controller capability
before importing the SkyPilot package or its plugin module.
"""

import asyncio
import ctypes
import importlib.util
import os
import pathlib
import site
import sys

_PR_GET_DUMPABLE = 3
_PR_SET_DUMPABLE = 4
_CAPABILITY_FD_ENV_VAR = (
    'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD')
_CAPABILITY_ENV_VAR = 'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY'
_CAPABILITY_PATH_ENV_VAR = (
    'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH')
_CAPABILITY_MODULE_NAME = 'sky.utils.controller_capability'


def _make_process_non_dumpable() -> None:
    if not sys.platform.startswith('linux'):
        raise OSError('Managed-job controller capability requires Linux.')
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    dumpable = libc.prctl(_PR_GET_DUMPABLE, 0, 0, 0, 0)
    if dumpable < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if dumpable != 0:
        raise OSError('Kernel did not disable ControllerManager dumps.')


def _capability_fd_from_environment() -> int:
    raw_fd = os.environ.pop(_CAPABILITY_FD_ENV_VAR, None)
    os.environ.pop(_CAPABILITY_ENV_VAR, None)
    os.environ.pop(_CAPABILITY_PATH_ENV_VAR, None)
    if (raw_fd is None or not raw_fd.isascii() or not raw_fd.isdecimal() or
        (len(raw_fd) > 1 and raw_fd.startswith('0'))):
        raise RuntimeError(
            'ControllerManager capability transport channel is invalid.')
    capability_fd = int(raw_fd)
    if capability_fd < 0:
        raise RuntimeError(
            'ControllerManager capability transport channel is invalid.')
    return capability_fd


def _load_capability_primitives():
    """Load the stdlib-only authority module without importing ``sky``."""
    module_path = (pathlib.Path(__file__).parents[1] / 'utils' /
                   'controller_capability.py')
    spec = importlib.util.spec_from_file_location(_CAPABILITY_MODULE_NAME,
                                                  module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Could not load controller capability primitives.')
    module = importlib.util.module_from_spec(spec)
    sys.modules[_CAPABILITY_MODULE_NAME] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(_CAPABILITY_MODULE_NAME, None)
        raise
    return module


def main() -> None:
    if not sys.flags.no_site:
        raise RuntimeError(
            'ControllerManager bootstrap must run with Python -S.')
    if len(sys.argv) != 4:
        raise RuntimeError('ControllerManager requires UUID, slot ID, and '
                           'slot-attempt arguments.')
    _make_process_non_dumpable()
    capability_fd = _capability_fd_from_environment()
    capability_primitives = _load_capability_primitives()
    capability_primitives.install_process_local_from_fd(capability_fd)

    # Populate site-packages and run sitecustomize only after non-dumpability
    # and process-local authority are proven.  ``-S`` prevents either from
    # running during interpreter startup while the raw pipe is still readable.
    site.main()
    from sky.jobs import controller  # pylint: disable=import-outside-toplevel

    asyncio.run(controller.main(sys.argv[1], int(sys.argv[2]), sys.argv[3]))


if __name__ == '__main__':
    main()
