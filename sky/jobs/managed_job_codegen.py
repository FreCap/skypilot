"""Remote managed-job utility command generation."""

import shlex
import textwrap
import typing

from sky import skypilot_config
from sky.backends import backend_utils
from sky.dag import DEFAULT_EXECUTION
from sky.skylet import constants
from sky.utils import common_utils

if typing.TYPE_CHECKING:
    from sky import dag as dag_lib


class ManagedJobCodeGen:
    """Code generator for managed job utility functions.

    Usage:

      >> codegen = ManagedJobCodeGen.show_jobs(...)
    """
    _PREFIX = textwrap.dedent("""\
        import sys
        from sky.jobs import utils
        from sky.jobs import state as managed_job_state
        from sky.jobs import constants as managed_job_constants

        managed_job_version = managed_job_constants.MANAGED_JOBS_VERSION

        # Plugins are only loaded for managed jobs version 13 and above.
        # Context-aware loading (PluginContext) was introduced in version 20.
        if managed_job_version >= 20:
            from sky import sky_logging as _sky_logging
            from sky.server import plugins
            # Suppress logging during plugin loading to prevent installation
            # logs from leaking into codegen output.
            with _sky_logging.silent():
                plugins.load_plugins(plugins.ExtensionContext(
                    context=plugins.PluginContext.CONTROLLER))
        elif managed_job_version >= 13:
            from sky import sky_logging as _sky_logging
            from sky.server import plugins
            with _sky_logging.silent():
                plugins.load_plugins(plugins.ExtensionContext())
        """)

    @classmethod
    def get_job_table(
        cls,
        skip_finished: bool = False,
        accessible_workspaces: list[str] | None = None,
        job_ids: list[int] | None = None,
        workspace_match: str | None = None,
        name_match: str | None = None,
        pool_match: str | None = None,
        page: int | None = None,
        limit: int | None = None,
        user_hashes: list[str | None] | None = None,
        statuses: list[str] | None = None,
        fields: list[str] | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        submitted_after: float | None = None,
        submitted_before: float | None = None,
    ) -> str:
        code = textwrap.dedent(f"""\
        # Filter out is_primary_in_job_group for older controllers (< 15)
        _fields = {fields!r}
        if managed_job_version < 15 and _fields is not None:
            _fields = [f for f in _fields if f != 'is_primary_in_job_group']
        # Filter out batch fields for older controllers (< 18)
        _BATCH_FIELDS = {{'is_batch', 'batch_total_batches', 'batch_completed_batches'}}
        if managed_job_version < 18 and _fields is not None:
            _fields = [f for f in _fields if f not in _BATCH_FIELDS]
        if managed_job_version < 9:
            # For backward compatibility, since filtering is not supported
            # before #6652.
            # TODO(hailong): Remove compatibility before 0.12.0
            job_table = utils.dump_managed_job_queue()
        elif managed_job_version < 10:
            job_table = utils.dump_managed_job_queue(
                                skip_finished={skip_finished},
                                accessible_workspaces={accessible_workspaces!r},
                                job_ids={job_ids!r},
                                workspace_match={workspace_match!r},
                                name_match={name_match!r},
                                pool_match={pool_match!r},
                                page={page!r},
                                limit={limit!r},
                                user_hashes={user_hashes!r})
        elif managed_job_version < 12:
            job_table = utils.dump_managed_job_queue(
                                skip_finished={skip_finished},
                                accessible_workspaces={accessible_workspaces!r},
                                job_ids={job_ids!r},
                                workspace_match={workspace_match!r},
                                name_match={name_match!r},
                                pool_match={pool_match!r},
                                page={page!r},
                                limit={limit!r},
                                user_hashes={user_hashes!r},
                                statuses={statuses!r})
        elif managed_job_version < 14:
            job_table = utils.dump_managed_job_queue(
                                skip_finished={skip_finished},
                                accessible_workspaces={accessible_workspaces!r},
                                job_ids={job_ids!r},
                                workspace_match={workspace_match!r},
                                name_match={name_match!r},
                                pool_match={pool_match!r},
                                page={page!r},
                                limit={limit!r},
                                user_hashes={user_hashes!r},
                                statuses={statuses!r},
                                fields=_fields)
        elif managed_job_version < 22:
            job_table = utils.dump_managed_job_queue(
                                skip_finished={skip_finished},
                                accessible_workspaces={accessible_workspaces!r},
                                job_ids={job_ids!r},
                                workspace_match={workspace_match!r},
                                name_match={name_match!r},
                                pool_match={pool_match!r},
                                page={page!r},
                                limit={limit!r},
                                user_hashes={user_hashes!r},
                                statuses={statuses!r},
                                fields=_fields,
                                sort_by={sort_by!r},
                                sort_order={sort_order!r})
        else:
            job_table = utils.dump_managed_job_queue(
                                skip_finished={skip_finished},
                                accessible_workspaces={accessible_workspaces!r},
                                job_ids={job_ids!r},
                                workspace_match={workspace_match!r},
                                name_match={name_match!r},
                                pool_match={pool_match!r},
                                page={page!r},
                                limit={limit!r},
                                user_hashes={user_hashes!r},
                                statuses={statuses!r},
                                fields=_fields,
                                sort_by={sort_by!r},
                                sort_order={sort_order!r},
                                submitted_after={submitted_after!r},
                                submitted_before={submitted_before!r})
        print(job_table, flush=True)
        """)
        return cls._build(code)

    @classmethod
    def cancel_managed_jobs(
        cls,
        *,
        name: str | None = None,
        job_ids: list[int] | None = None,
        pool: str | None = None,
        all: bool = False,  # pylint: disable=redefined-builtin
        all_users: bool = False,
        graceful: bool = False,
        graceful_timeout: int | None = None,
    ) -> str:
        """Unified cancel codegen.

        Legacy codegen remains available for selector-only cancellation. Any
        graceful cancellation request is intentionally modern-only: the legacy
        path raises instead of silently dropping graceful fields on older
        controllers.
        """
        active_workspace = skypilot_config.get_active_workspace()

        if graceful or graceful_timeout is not None:
            legacy_block = textwrap.indent(
                textwrap.dedent("""\
                raise RuntimeError(
                    'Graceful managed job cancellation requires a jobs '
                    'controller with the gRPC `cancel_managed_jobs` endpoint. '
                    'Please upgrade the jobs controller and retry.')
            """).rstrip(), '    ')
        elif all_users or all or job_ids:
            legacy_call_lines = [
                'if managed_job_version < 2:',
                f'    msg = utils.cancel_jobs_by_id({job_ids!r})',
                'elif managed_job_version < 4:',
                f'    msg = utils.cancel_jobs_by_id({job_ids!r}, '
                f'all_users={all_users!r})',
                'else:',
                f'    msg = utils.cancel_jobs_by_id({job_ids!r}, '
                f'all_users={all_users!r}, '
                f'current_workspace={active_workspace!r})',
            ]
            legacy_block = '\n'.join(
                f'    {line}' for line in legacy_call_lines)
        elif name is not None:
            legacy_call_lines = [
                'if managed_job_version < 4:',
                f'    msg = utils.cancel_job_by_name({name!r})',
                'else:',
                f'    msg = utils.cancel_job_by_name({name!r}, '
                f'{active_workspace!r})',
            ]
            legacy_block = '\n'.join(
                f'    {line}' for line in legacy_call_lines)
        else:
            assert pool is not None, (job_ids, name, pool, all)
            legacy_block = (f'    msg = utils.cancel_jobs_by_pool({pool!r}, '
                            f'{active_workspace!r})')

        code = (f'if managed_job_version < 19:\n'
                f'{legacy_block}\n'
                f'else:\n'
                f'    msg = utils.cancel_managed_jobs(\n'
                f'        name={name!r},\n'
                f'        job_ids={job_ids!r},\n'
                f'        pool={pool!r},\n'
                f'        all={all!r},\n'
                f'        all_users={all_users!r},\n'
                f'        graceful={graceful!r},\n'
                f'        graceful_timeout={graceful_timeout!r},\n'
                f'        current_workspace={active_workspace!r},\n'
                f'    )\n'
                f'print(msg, end="", flush=True)\n')
        return cls._build(code)

    @classmethod
    def get_version_and_job_table(cls) -> str:
        """Generate code to get controller version and raw job table."""
        code = textwrap.dedent("""\
        from sky.skylet import constants as controller_constants

        # Get controller version
        controller_version = controller_constants.SKYLET_VERSION
        print(f"controller_version:{controller_version}", flush=True)

        # Get and print raw job table (load_managed_job_queue can parse this directly)
        job_table = utils.dump_managed_job_queue()
        print(job_table, flush=True)
        """)
        return cls._build(code)

    @classmethod
    def get_version(cls) -> str:
        """Generate code to get controller version."""
        code = textwrap.dedent("""\
        from sky.skylet import constants as controller_constants

        # Get controller version
        controller_version = controller_constants.SKYLET_VERSION
        print(f"controller_version:{controller_version}", flush=True)
        """)
        return cls._build(code)

    @classmethod
    def get_all_job_ids_by_name(cls, job_name: str | None) -> str:
        code = textwrap.dedent(f"""\
        from sky.utils import message_utils
        job_id = managed_job_state.get_all_job_ids_by_name({job_name!r})
        print(message_utils.encode_payload(job_id), end="", flush=True)
        """)
        return cls._build(code)

    @classmethod
    def get_debug_dump_manifest(cls, job_ids: list[int]) -> str:
        code = textwrap.dedent(f"""\
        from sky.utils import message_utils
        if managed_job_version >= 17:
            result = utils.collect_debug_dump_manifest({job_ids!r})
            print(message_utils.encode_payload(result), end="", flush=True)
        else:
            print(message_utils.encode_payload({{
                'inline_data': [], 'file_paths': [], 'errors': [
                {{'component': 'managed_jobs', 'resource': 'debug_dump',
                  'error': 'Controller version too old (requires >= 17)'}}
            ]}}), end="", flush=True)
        """)
        return cls._build(code)

    @classmethod
    def stream_logs(cls,
                    job_name: str | None,
                    job_id: int | None,
                    follow: bool = True,
                    controller: bool = False,
                    tail: int | None = None,
                    tail_offset: int | None = None,
                    task: str | int | None = None) -> str:
        code = textwrap.dedent(f"""\
        if managed_job_version < 6:
            # Versions before 6 did not support tail parameter
            result = utils.stream_logs(job_id={job_id!r}, job_name={job_name!r},
                                    follow={follow}, controller={controller})
        elif managed_job_version < 15:
            # Versions before 15 did not support task parameter
            result = utils.stream_logs(job_id={job_id!r}, job_name={job_name!r},
                                    follow={follow}, controller={controller}, tail={tail!r})
        elif managed_job_version < 21:
            # Versions before 21 did not support tail_offset parameter
            result = utils.stream_logs(job_id={job_id!r}, job_name={job_name!r},
                                    follow={follow}, controller={controller}, tail={tail!r},
                                    task={task!r})
        else:
            result = utils.stream_logs(job_id={job_id!r}, job_name={job_name!r},
                                    follow={follow}, controller={controller}, tail={tail!r},
                                    tail_offset={tail_offset!r}, task={task!r})
        if managed_job_version < 3:
            # Versions 2 and older did not return a retcode, so we just print
            # the result.
            # TODO: Remove compatibility before 0.12.0
            print(result, flush=True)
        else:
            msg, retcode = result
            print(msg, flush=True)
            sys.exit(retcode)
        """)
        return cls._build(code)

    @classmethod
    def set_pending(cls,
                    job_id: int,
                    managed_job_dag: 'dag_lib.Dag',
                    workspace: str,
                    entrypoint: str,
                    user_hash: str | None = None) -> str:
        dag_name = managed_job_dag.name
        pool = managed_job_dag.pool
        # Execution mode: 'parallel' for job groups, 'serial' for pipelines and
        # single jobs
        execution = (managed_job_dag.execution.value
                     if managed_job_dag.execution else DEFAULT_EXECUTION.value)
        # Add the managed job to queue table.
        code = textwrap.dedent(f"""\
            set_job_info_kwargs = {{'workspace': {workspace!r}}}
            if managed_job_version < 4:
                set_job_info_kwargs = {{}}
            if managed_job_version >= 5:
                set_job_info_kwargs['entrypoint'] = {entrypoint!r}
            if managed_job_version >= 8:
                from sky.serve import serve_state
                pool_hash = None
                if {pool!r} != None:
                    pool_hash = serve_state.get_service_hash({pool!r})
                set_job_info_kwargs['pool'] = {pool!r}
                set_job_info_kwargs['pool_hash'] = pool_hash
            if managed_job_version >= 11:
                set_job_info_kwargs['user_hash'] = {user_hash!r}
            if managed_job_version >= 15:
                set_job_info_kwargs['execution'] = {execution!r}
            managed_job_state.set_job_info(
                {job_id}, {dag_name!r}, **set_job_info_kwargs)
            """)
        for task_id, task in enumerate(managed_job_dag.tasks):
            resources_str = backend_utils.get_task_resources_str(
                task, is_managed_job=True)
            # For job groups, determine which tasks are primary vs auxiliary.
            # For non-job-groups, is_primary_in_job_group=None for all tasks.
            is_primary_in_job_group: bool | None = None
            if managed_job_dag.is_job_group():
                is_primary_in_job_group = (
                    managed_job_dag.primary_tasks is None or
                    task.name in managed_job_dag.primary_tasks)
            code += textwrap.dedent(f"""\
                if managed_job_version < 7:
                    managed_job_state.set_pending({job_id}, {task_id},
                                    {task.name!r}, {resources_str!r})
                elif managed_job_version < 15:
                    managed_job_state.set_pending({job_id}, {task_id},
                                    {task.name!r}, {resources_str!r},
                                    {task.metadata_json!r})
                else:
                    managed_job_state.set_pending({job_id}, {task_id},
                                    {task.name!r}, {resources_str!r},
                                    {task.metadata_json!r},
                                    {is_primary_in_job_group!r})
                """)
        return cls._build(code)

    @classmethod
    def _build(cls, code: str) -> str:
        generated_code = cls._PREFIX + '\n' + code
        # Use the local user id to make sure the operation goes to the correct
        # user.
        return (
            f'export {constants.USER_ID_ENV_VAR}='
            f'"{common_utils.get_user_hash()}"; '
            f'{constants.SKY_PYTHON_CMD} -u -c {shlex.quote(generated_code)}')
