"""Managed job runner: abstraction + registry.

A ``ManagedJobRunner`` is the strategy object that the server's managed-job
entry points (queue, tail_logs) delegate to. The registered runner decides
*how* the operation executes — the default runner generates Python code and
runs it on the controller via subprocess, while a plugin-provided runner
might call the managed jobs DB directly when the controller is in-process.

At most one runner is registered at a time. If nothing has registered,
``current()`` lazily constructs ``_DefaultManagedJobRunner`` from
``sky.jobs.server.core`` so there's always a usable runner regardless of
import ordering. Plugins override the default by calling
``register(MyRunner())`` in their ``install()`` phase.

Thread-safety: ``register()`` is only expected to be called during
server/plugin startup, before request handling begins. The module-level
reference is written and read atomically under the GIL, and only the
default + plugin-provided runner are registered (sequentially), so no
lock is needed.
"""
import typing
from typing import Any, Protocol

from sky import sky_logging

if typing.TYPE_CHECKING:
    from sky import backends
    from sky.jobs import utils as managed_job_utils

logger = sky_logging.init_logger(__name__)


class ManagedJobRunner(Protocol):
    """Strategy interface for managed job controller operations."""

    def fetch_managed_job_table(
        self,
        *,
        handle: 'backends.CloudVmRayResourceHandle',
        backend: 'backends.CloudVmRayBackend',
        skip_finished: bool,
        accessible_workspaces: list[str],
        job_ids: list[int] | None,
        workspace_match: str | None,
        name_match: str | None,
        pool_match: str | None,
        page: int | None,
        limit: int | None,
        user_hashes: list[str | None] | None,
        statuses: list[str] | None,
        fields: list[str] | None,
        sort_by: str | None,
        sort_order: str | None,
        submitted_after: float | None,
        submitted_before: float | None,
    ) -> tuple[list[dict[str, Any]], int,
               'managed_job_utils.ManagedJobQueueResultType', int, dict[str,
                                                                        int]]:
        ...

    def tail_managed_job_logs(
        self,
        *,
        handle: 'backends.CloudVmRayResourceHandle',
        backend: 'backends.CloudVmRayBackend',
        job_id: int | None,
        job_name: str | None,
        follow: bool,
        controller: bool,
        tail: int | None,
        tail_offset: int | None = None,
        task: str | int | None,
    ) -> int:
        ...


_current: ManagedJobRunner | None = None


def register(runner: ManagedJobRunner) -> None:
    """Install ``runner`` as the currently-active managed job runner.

    Last registration wins. Plugins override the default in ``install()``.
    """
    # pylint: disable=global-statement
    global _current
    _current = runner
    logger.debug('Registered ManagedJobRunner: %s', type(runner).__name__)


def current() -> ManagedJobRunner:
    """Return the registered runner, falling back to the default.

    If nothing has been registered, constructs and installs
    ``_DefaultManagedJobRunner`` so there's always a usable runner
    regardless of import ordering.
    """
    # pylint: disable=global-statement
    global _current
    if _current is None:
        # pylint: disable=import-outside-toplevel
        from sky.jobs.server.core import _DefaultManagedJobRunner
        _current = _DefaultManagedJobRunner()
    return _current
