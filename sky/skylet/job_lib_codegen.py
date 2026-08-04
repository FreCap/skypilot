"""Remote job utility command generation."""

import shlex

from sky.skylet import constants


class JobLibCodeGen:
    """Code generator for job utility functions.

    Usage:

      >> codegen = JobLibCodeGen.add_job(...)
    """

    _PREFIX = [
        'import os',
        'import getpass',
        'import sys',
        'from sky import exceptions',
        'from sky.skylet import log_lib, job_lib, constants',
    ]

    @classmethod
    def add_job(cls, job_name: str | None, username: str, run_timestamp: str,
                resources_str: str, metadata: str) -> str:
        if job_name is None:
            job_name = '-'
        code = [
            # We disallow job submission when SKYLET_VERSION is older than 9, as
            # it was using ray job submit before #4318, and switched to raw
            # process. Using the old skylet version will cause the job status
            # to be stuck in PENDING state or transition to FAILED_DRIVER state.
            '\nif int(constants.SKYLET_VERSION) < 9: '
            'raise RuntimeError("SkyPilot runtime is too old, which does not '
            'support submitting jobs.")',
            '\nresult = None',
            '\nif int(constants.SKYLET_VERSION) < 15: '
            '\n result = job_lib.add_job('
            f'{job_name!r},'
            f'{username!r},'
            f'{run_timestamp!r},'
            f'{resources_str!r})',
            '\nelse: '
            '\n result = job_lib.add_job('
            f'{job_name!r},'
            f'{username!r},'
            f'{run_timestamp!r},'
            f'{resources_str!r},'
            f'metadata={metadata!r})',
            ('\nif isinstance(result, tuple):'
             '\n  print("Job ID: " + str(result[0]), flush=True)'
             '\n  print("Log Dir: " + str(result[1]), flush=True)'
             '\nelse:'
             '\n  print("Job ID: " + str(result), flush=True)'),
        ]
        return cls._build(code)

    @classmethod
    def set_job_info_without_job_id(cls,
                                    name: str,
                                    workspace: str,
                                    entrypoint: str,
                                    pool: str | None,
                                    pool_hash: str | None,
                                    user_hash: str | None,
                                    task_ids: list[int],
                                    task_names: list[str],
                                    resources_str: str,
                                    metadata_jsons: list[str],
                                    is_primary_in_job_groups: list[bool | None],
                                    execution: str,
                                    num_jobs: int = 1,
                                    is_batch: bool = False) -> str:
        pool_str = f'{pool!r}' if pool is not None else 'None'
        pool_hash_str = f'{pool_hash!r}' if pool_hash is not None else 'None'
        user_hash_str = f'{user_hash!r}' if user_hash is not None else 'None'
        # Build the tasks data as Python code
        task_ids_str = '[' + ','.join(str(tid) for tid in task_ids) + ']'
        task_names_str = ('[' + ','.join(f'{name!r}' for name in task_names) +
                          ']')
        metadata_jsons_str = ('[' +
                              ','.join(f'{md!r}' for md in metadata_jsons) +
                              ']')
        is_primary_in_job_groups_str = ('[' + ','.join(
            str(is_primary) for is_primary in is_primary_in_job_groups) + ']')
        # Build the set_job_info_without_job_id call, gating the is_batch
        # parameter behind a SKYLET_VERSION check so that old controllers
        # (< 35) that don't have the parameter still work for non-batch
        # jobs, and batch jobs get a clear error instead of silently
        # succeeding as an empty task.
        base_kwargs = (f'name={name!r},'
                       f'workspace={workspace!r},'
                       f'entrypoint={entrypoint!r},'
                       f'pool={pool_str},'
                       f'pool_hash={pool_hash_str},'
                       f'user_hash={user_hash_str},'
                       f'execution={execution!r}')
        if is_batch:
            set_job_info_code = (
                '\n  if int(constants.SKYLET_VERSION) < 36:'
                '\n    raise RuntimeError('
                '"The jobs controller does not support batch jobs. '
                'Please update it with: sky jobs controller up --yes")'
                '\n  job_id = managed_job_state.set_job_info_without_job_id('
                f'{base_kwargs},'
                f'is_batch={is_batch!r})')
        else:
            set_job_info_code = (
                '\n  if int(constants.SKYLET_VERSION) < 36:'
                '\n    job_id = managed_job_state.set_job_info_without_job_id('
                f'{base_kwargs})'
                '\n  else:'
                '\n    job_id = managed_job_state.set_job_info_without_job_id('
                f'{base_kwargs},'
                f'is_batch={is_batch!r})')
        code = [
            '\nfrom sky.jobs import state as managed_job_state',
            f'\nnum_jobs = {num_jobs}',
            f'\ntask_ids = {task_ids_str}',
            f'\ntask_names = {task_names_str}',
            f'\nresources_str = {resources_str!r}',
            f'\nmetadata_jsons = {metadata_jsons_str}',
            f'\nis_primary_in_job_groups = {is_primary_in_job_groups_str}',
            '\njob_ids = []',
            '\nfor _ in range(num_jobs):' + set_job_info_code,
            '\n  job_ids.append(job_id)',
            '\n  # Set pending state for all tasks',
            '\n  for task_id, task_name, metadata_json, is_primary_in_job_group in zip('  # pylint: disable=line-too-long
            '\n      task_ids, task_names, metadata_jsons, is_primary_in_job_groups):'  # pylint: disable=line-too-long
            '\n    managed_job_state.set_pending('
            '\n      job_id, task_id, task_name, resources_str, metadata_json, is_primary_in_job_group)',  # pylint: disable=line-too-long
            '\nprint("Job IDs: " + ",".join(map(str, job_ids)), flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def queue_job(cls, job_id: int, cmd: str) -> str:
        code = [
            'job_lib.scheduler.queue('
            f'{job_id!r},'
            f'{cmd!r})',
        ]
        return cls._build(code)

    @classmethod
    def wait_for_job(cls, job_id: int) -> str:
        code = [
            # TODO(kevin): backward compatibility, remove in 0.13.0.
            (f'job_lib.wait_for_job_completion({job_id!r}) if '
             'hasattr(job_lib, "wait_for_job_completion") else None'),
        ]
        return cls._build(code)

    @classmethod
    def update_status(cls) -> str:
        code = ['job_lib.update_status()']
        return cls._build(code)

    @classmethod
    def get_job_queue(cls, user_hash: str | None, all_jobs: bool) -> str:
        # TODO(SKY-1214): combine get_job_queue with get_job_statuses.
        code = [
            'job_queue = job_lib.dump_job_queue('
            f'{user_hash!r}, {all_jobs})',
            'print(job_queue, flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def cancel_jobs(cls,
                    job_ids: list[int] | None,
                    cancel_all: bool = False,
                    user_hash: str | None = None) -> str:
        """See job_lib.cancel_jobs()."""
        code = [
            (f'cancelled = job_lib.cancel_jobs_encoded_results('
             f'jobs={job_ids!r}, cancel_all={cancel_all}, '
             f'user_hash={user_hash!r})'),
            # Print cancelled IDs. Caller should parse by decoding.
            'print(cancelled, flush=True)',
        ]
        # TODO(zhwu): Backward compatibility, remove after 0.12.0.
        if user_hash is None:
            code = [
                (f'cancelled = job_lib.cancel_jobs_encoded_results('
                 f' {job_ids!r}, {cancel_all})'),
                # Print cancelled IDs. Caller should parse by decoding.
                'print(cancelled, flush=True)',
            ]
        return cls._build(code)

    @classmethod
    def fail_all_jobs_in_progress(cls) -> str:
        # Used only for restarting a cluster.
        code = ['job_lib.fail_all_jobs_in_progress()']
        return cls._build(code)

    @classmethod
    def tail_logs(cls,
                  job_id: int | None,
                  managed_job_id: int | None,
                  follow: bool = True,
                  tail: int = 0,
                  tail_offset: int | None = None) -> str:
        # pylint: disable=line-too-long

        # tail_offset is gated on SKYLET_VERSION 37+ — older skylets reject
        # the kwarg. We omit it entirely on the old branch so the codegen
        # signature stays identical to what those skylets expect.
        tail_logs_call = (
            f'log_lib.tail_logs(job_id=job_id, log_dir=log_dir, managed_job_id={managed_job_id!r}, follow={follow}, tail={tail})'
        )
        if tail_offset is not None:
            tail_logs_call = (
                f'if int(constants.SKYLET_VERSION) < 37:'
                f'\n  log_lib.tail_logs(job_id=job_id, log_dir=log_dir, managed_job_id={managed_job_id!r}, follow={follow}, tail={tail})'
                f'\nelse:'
                f'\n  log_lib.tail_logs(job_id=job_id, log_dir=log_dir, managed_job_id={managed_job_id!r}, follow={follow}, tail={tail}, tail_offset={tail_offset})'
            )
        code = [
            # We use != instead of is not because 1 is not None will print a warning:
            # <stdin>:1: SyntaxWarning: "is not" with a literal. Did you mean "!="?
            f'job_id = {job_id} if {job_id} != None else job_lib.get_latest_job_id()',
            # For backward compatibility, use the legacy generation rule for
            # jobs submitted before 0.11.0.
            ('log_dir = None\n'
             'if hasattr(job_lib, "get_log_dir_for_job"):\n'
             '  log_dir = job_lib.get_log_dir_for_job(job_id)\n'
             'if log_dir is None:\n'
             '  run_timestamp = job_lib.get_run_timestamp(job_id)\n'
             f'  log_dir = None if run_timestamp is None else os.path.join({constants.SKY_LOGS_DIRECTORY!r}, run_timestamp)'
            ),
            # Add a newline to leave the if indent block above.
            '\n' + tail_logs_call,
            # After tailing, check the job status and exit with appropriate
            # code. The leading '\n' resets indentation back to column 0;
            # without it, ';'.join() in _build below would paste these onto
            # the last line of the if/else above and the statements would be
            # absorbed into the `else:` suite (so the if-branch falls through
            # without sys.exit-ing).
            '\njob_status = job_lib.get_status(job_id)',
            'exit_code = exceptions.JobExitCode.from_job_status(job_status)',
            # Fix for dashboard: When follow=False and job is still running (NOT_FINISHED=101),
            # exit with success (0) since fetching current logs is a successful operation.
            # This prevents shell wrappers from printing "command terminated with exit code 101".
            f'exit_code = 0 if not {follow} and exit_code == 101 else exit_code',
            'sys.exit(exit_code)',
        ]
        return cls._build(code)

    @classmethod
    def get_job_status(cls, job_ids: list[int] | None = None) -> str:
        # Prints "Job <id> <status>" for UX; caller should parse the last token.
        code = [
            f'job_ids = {job_ids} if {job_ids} is not None '
            'else [job_lib.get_latest_job_id()]',
            'job_statuses = job_lib.get_statuses_payload(job_ids)',
            'print(job_statuses, flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def get_job_status_with_system_recovery(cls,
                                            job_ids: list[int] | None = None
                                           ) -> str:
        """Gets statuses plus optional recovery details on new runtimes."""
        code = [
            f'job_ids = {job_ids} if {job_ids} is not None '
            'else [job_lib.get_latest_job_id()]',
            ('job_statuses = '
             'job_lib.get_statuses_with_system_recovery_payload(job_ids) '
             'if (int(constants.SKYLET_VERSION) >= 42 and '
             'int(constants.SKYLET_LIB_VERSION) >= 9) '
             'else job_lib.get_statuses_payload(job_ids)'),
            'print(job_statuses, flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def get_job_submitted_or_ended_timestamp_payload(
            cls,
            job_id: int | None = None,
            get_ended_time: bool = False) -> str:
        code = [
            f'job_id = {job_id} if {job_id} is not None '
            'else job_lib.get_latest_job_id()',
            'job_time = '
            'job_lib.get_job_submitted_or_ended_timestamp_payload('
            f'job_id, {get_ended_time})',
            'print(job_time, flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def get_log_dirs_for_jobs(cls, job_ids: list[str] | None) -> str:
        code = [
            f'job_ids = {job_ids} if {job_ids} is not None '
            'else [job_lib.get_latest_job_id()]',
            # TODO(aylei): backward compatibility, remove after 0.12.0.
            'log_dirs = job_lib.get_log_dir_for_jobs(job_ids) if '
            'hasattr(job_lib, "get_log_dir_for_jobs") else '
            'job_lib.run_timestamp_with_globbing_payload(job_ids)',
            'print(log_dirs, flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def get_job_exit_codes(cls, job_id: int | None = None) -> str:
        """Generate shell command to retrieve exit codes."""
        code = [
            f'job_id = {job_id} if {job_id} is not None else job_lib.get_latest_job_id()',  # pylint: disable=line-too-long
            'exit_codes = job_lib.get_exit_codes(job_id) if job_id is not None and int(constants.SKYLET_VERSION) >= 28 else {}',  # pylint: disable=line-too-long
            'print(exit_codes, flush=True)',
        ]
        return cls._build(code)

    @classmethod
    def _build(cls, code: list[str]) -> str:
        code = cls._PREFIX + code
        code = ';'.join(code)
        return (f'{constants.ACTIVATE_SKY_REMOTE_PYTHON_ENV}; '
                f'{constants.SKY_PYTHON_CMD} -u -c {shlex.quote(code)}')
