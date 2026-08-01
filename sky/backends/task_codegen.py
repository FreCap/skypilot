"""Code generator for task execution."""

import copy
import inspect
import json
import math
import os
import shlex
import textwrap

import colorama

from sky import sky_logging
from sky.skylet import constants
from sky.skylet import log_lib
from sky.skylet import system_oom_recovery
from sky.utils import accelerator_registry
from sky.utils import ux_utils

# Unset RAY_RAYLET_PID to prevent the Ray cluster in the SkyPilot runtime
# from interfering with the Ray cluster in the user's task (if any).
UNSET_RAY_ENV_VARS = ['RAY_RAYLET_PID']

logger = sky_logging.init_logger(__name__)


class TaskCodeGen:
    """Base code generator for task execution on Ray and Slurm."""

    def __init__(self) -> None:
        # Code generated so far, to be joined via '\n'.
        self._code: list[str] = []
        # Guard method calling order.
        self._has_prologue: bool = False
        self._has_epilogue: bool = False
        self._has_setup: bool = False
        # Job ID is used to identify the job (also this generated code).
        self.job_id: int | None = None

    def _add_common_imports(self) -> None:
        """Add common imports for both Ray and Slurm execution."""
        self._code.append(
            textwrap.dedent("""\
            import functools
            import getpass
            import hashlib
            import io
            import os
            import pathlib
            import selectors
            import shlex
            import subprocess
            import sys
            import tempfile
            import textwrap
            import time
            from typing import Dict, List, Optional, Tuple, Union
            """))

    def _add_skylet_imports(self) -> None:
        """Add SkyPilot skylet imports."""
        self._code.append(
            textwrap.dedent("""\
            from sky.skylet import autostop_lib
            from sky.skylet import constants
            from sky.skylet import job_lib
            from sky.utils import log_utils
            from sky.utils import subprocess_utils
            """))

    def _add_logging_functions(self) -> None:
        """Add log streaming functions from log_lib."""
        self._code += [
            # FIXME: This is a hack to make sure that the functions can be found
            # by ray.remote. This should be removed once we have a better way to
            # specify dependencies for ray.
            inspect.getsource(log_lib._ProcessingArgs),  # pylint: disable=protected-access
            inspect.getsource(log_lib._get_context),  # pylint: disable=protected-access
            inspect.getsource(log_lib._handle_io_stream),  # pylint: disable=protected-access
            inspect.getsource(log_lib.process_subprocess_stream),
            inspect.getsource(log_lib.run_with_log),
            inspect.getsource(log_lib.make_task_bash_script),
            inspect.getsource(log_lib.add_ray_env_vars),
            inspect.getsource(log_lib.run_bash_command_with_log),
            inspect.getsource(log_lib.run_bash_command_with_log_and_return_pid),
        ]

    def _add_waiting_for_resources_msg(self, num_nodes: int) -> None:
        self._code.append(
            textwrap.dedent(f"""\
            plural = 's' if {num_nodes} > 1 else ''
            node_str = f'{num_nodes} node{{plural}}'
            message = ('{ux_utils.INDENT_SYMBOL}{colorama.Style.DIM}'
                       'Waiting for task resources on '
                       f'{{node_str}}.{colorama.Style.RESET_ALL}')
            print(message, flush=True)"""))

    def _get_job_started_msg(self) -> str:
        """Returns the 'Job started' streaming message with ANSI formatting."""
        return (
            f'{ux_utils.INDENT_LAST_SYMBOL}Job started. Streaming logs... '
            f'{colorama.Style.DIM}(Ctrl-C to exit log streaming; job will not '
            f'be killed){colorama.Style.RESET_ALL}')

    def _add_job_started_msg(self) -> None:
        streaming_message = self._get_job_started_msg()
        self._code.append(f'print({streaming_message!r}, flush=True)')

    def _get_accelerator_details(
        self,
        resources_dict: dict[str, float],
    ) -> tuple[str | None, float]:
        resources_copy = resources_dict.copy()
        resources_copy.pop('CPU', None)

        if not resources_copy:
            return None, 0.0

        assert len(resources_copy) == 1, (
            'There can only be one type of accelerator per instance. '
            f'Found: {resources_copy}.')

        acc_name, acc_count = list(resources_copy.items())[0]
        return acc_name, float(acc_count)

    def _add_constants(self) -> None:
        self._code.append(
            textwrap.dedent(f"""\
            SKY_REMOTE_WORKDIR = {constants.SKY_REMOTE_WORKDIR!r}

            CANCELLED_RETURN_CODE = 137
            """))

    @staticmethod
    def get_rclone_flush_script() -> str:
        """Generate rclone flush script for cached storage mounts.

        This script blocks job completion until all storage mounted with
        CACHED_MOUNT mode is uploaded to remote.

        Returns:
            Bash script as string
        """
        return system_oom_recovery.build_rclone_flush_script()

    @staticmethod
    def build_task_bash_script(bash_script: str,
                               env_prefix: str | None = None) -> str:
        """Build the complete bash script for a task.

        Prepends env_prefix (if any) and appends the rclone flush script.

        Args:
            bash_script: The user's bash command.
            env_prefix: Optional shell commands to prepend (e.g. unsetting
                Ray env vars).

        Returns:
            Complete bash string ready to execute on the cluster.
        """
        if env_prefix:
            bash_script = f'{env_prefix}; {bash_script}'
        bash_script += TaskCodeGen.get_rclone_flush_script()
        return bash_script

    def add_prologue(self, job_id: int) -> None:
        """Initialize code generator and add prologue code.

        Args:
            job_id: SkyPilot internal job ID
        """
        raise NotImplementedError

    def add_setup(
        self,
        num_nodes: int,
        resources_dict: dict[str, float],
        stable_cluster_internal_ips: list[str],
        env_vars: dict[str, str],
        log_dir: str,
        setup_cmd: str | None = None,
    ) -> None:
        """Generates code to set up the task on each node.

        stable_cluster_internal_ips is used to ensure that the
        SKYPILOT_NODE_RANK environment variable is assigned in a
        deterministic order whenever a new task is added.
        """
        raise NotImplementedError

    def add_task(
        self,
        num_nodes: int,
        bash_script: str | None,
        task_name: str | None,
        resources_dict: dict[str, float],
        log_dir: str,
        env_vars: dict[str, str] | None = None,
    ) -> None:
        """Generates code to run the bash command on all num_nodes nodes."""
        raise NotImplementedError

    def add_epilogue(self) -> None:
        """Generate code that checks return codes and updates job status."""
        assert self._has_prologue, 'Call add_prologue() before add_epilogue().'
        assert not self._has_epilogue, 'add_epilogue() called twice?'
        self._has_epilogue = True

        failed_status_update = self._build_final_status_update(
            'job_lib.JobStatus.FAILED')
        succeeded_status_update = self._build_final_status_update(
            'job_lib.JobStatus.SUCCEEDED')

        epilogue = textwrap.dedent(f"""\
            if sum(returncodes) != 0:
                # Save exit codes to job metadata for potential recovery logic
                if int(constants.SKYLET_VERSION) >= 28:
                    job_lib.set_exit_codes({self.job_id!r}, returncodes)
                __FAILED_STATUS_UPDATE__
                # Schedule the next pending job immediately to make the job
                # scheduling more efficient.
                job_lib.scheduler.schedule_step()
                # This waits for all streaming logs to finish.
                time.sleep(0.5)
                reason = ''
                # 139 is the return code of SIGSEGV, i.e. Segmentation Fault.
                if any(r == 139 for r in returncodes):
                    reason = '(likely due to Segmentation Fault)'
                if any(r == 137 for r in returncodes):
                    # Find the first non-137 return code
                    non_137 = next(r for r in returncodes if r != 137)
                    reason = f'(A Worker failed with return code {{non_137}}, SkyPilot cleaned up the processes on other nodes with return code 137)'
                print('ERROR: {colorama.Fore.RED}Job {self.job_id} failed with '
                      'return code list:{colorama.Style.RESET_ALL}',
                      returncodes,
                      reason,
                      flush=True)
                # Need this to set the job status in ray job to be FAILED.
                sys.exit(1)
            else:
                __SUCCEEDED_STATUS_UPDATE__
                # Schedule the next pending job immediately to make the job
                # scheduling more efficient.
                job_lib.scheduler.schedule_step()
                # This waits for all streaming logs to finish.
                time.sleep(0.5)
            """)
        epilogue = epilogue.replace(
            '__FAILED_STATUS_UPDATE__',
            failed_status_update.replace('\n', '\n    '))
        epilogue = epilogue.replace(
            '__SUCCEEDED_STATUS_UPDATE__',
            succeeded_status_update.replace('\n', '\n    '))
        self._code.append(epilogue)

    def _build_final_status_update(self, status_expression: str) -> str:
        """Return generated code that records one final local job status."""
        return f'job_lib.set_status({self.job_id!r}, {status_expression})'

    def build(self) -> str:
        """Returns the entire generated program."""
        assert self._has_epilogue, 'Call add_epilogue() before build().'
        return '\n'.join(self._code)


class RayCodeGen(TaskCodeGen):
    """Code generator of a Ray program that executes a sky.Task.

    Usage:

      >> codegen = RayCodegen()
      >> codegen.add_prologue()

      >> codegen.add_task(...)
      >> codegen.add_task(...)

      >> codegen.add_epilogue()
      >> code = codegen.build()
    """

    def __init__(
        self,
        system_oom_recovery_plan: (system_oom_recovery.RecoveryLaunchPlan |
                                   None) = None,
    ) -> None:
        super().__init__()
        if (system_oom_recovery_plan is not None and
                not isinstance(system_oom_recovery_plan,
                               system_oom_recovery.RecoveryLaunchPlan)):
            raise TypeError(
                'system_oom_recovery_plan must be a RecoveryLaunchPlan')
        if (system_oom_recovery_plan is not None and
                system_oom_recovery_plan.execution_envelope is not None and
                system_oom_recovery_plan.execution_envelope.environment):
            raise ValueError(
                'RayCodeGen requires an unbound recovery execution envelope')
        self._system_oom_recovery_plan = system_oom_recovery_plan
        self._num_run_tasks = 0

    def add_prologue(self, job_id: int) -> None:
        assert not self._has_prologue, 'add_prologue() called twice?'
        self._has_prologue = True
        self.job_id = job_id
        # Should use 'auto' or 'ray://<internal_head_ip>:10001' rather than
        # 'ray://localhost:10001', or 'ray://127.0.0.1:10001', for public cloud.
        # Otherwise, ray will fail to get the placement group because of a bug
        # in ray job.
        ray_address = 'auto'

        # Add common imports
        self._add_common_imports()

        # Add Ray-specific setup
        self._code.append(
            textwrap.dedent("""\
            # Set the environment variables to avoid deduplicating logs and
            # scheduler events. This should be set in driver code, since we are
            # not using `ray job submit` anymore, and the environment variables
            # from the ray cluster is not inherited.
            os.environ['RAY_DEDUP_LOGS'] = '0'
            os.environ['RAY_SCHEDULER_EVENTS'] = '0'

            import ray
            import ray.util as ray_util
            """))

        self._add_skylet_imports()
        if self._system_oom_recovery_plan is not None:
            self._code.append('from sky.skylet import system_oom_recovery')
            self._code.append(
                'system_oom_recovery.validate_runtime_capability()')
            plan_dict = self._system_oom_recovery_plan.to_dict()
            self._code.append(
                'system_oom_recovery_plan = '
                'system_oom_recovery.RecoveryLaunchPlan.from_dict('
                f'{plan_dict!r}, allow_bound=False)')

        self._add_constants()

        # Add Ray configuration
        self._code.append(
            textwrap.dedent(f"""\
            kwargs = dict()
            # Only set the `_temp_dir` to SkyPilot's ray cluster directory when
            # the directory exists for backward compatibility for the VM
            # launched before #1790.
            if os.path.exists({constants.SKY_REMOTE_RAY_TEMPDIR!r}):
                kwargs['_temp_dir'] = {constants.SKY_REMOTE_RAY_TEMPDIR!r}
            ray.init(
                address={ray_address!r},
                namespace='__sky__{job_id}__',
                log_to_driver=True,
                **kwargs
            )
            def get_or_fail(futures, pg) -> List[int]:
                \"\"\"Wait for tasks, if any fails, cancel all unready.\"\"\"
                if not futures:
                    return [], []
                returncodes = [1] * len(futures)
                pids = [None] * len(futures)
                failed = False
                # Wait for 1 task to be ready.
                ready = []
                # Keep invoking ray.wait if ready is empty. This is because
                # ray.wait with timeout=None will only wait for 10**6 seconds,
                # which will cause tasks running for more than 12 days to return
                # before becoming ready.
                # (Such tasks are common in serving jobs.)
                # Reference: https://github.com/ray-project/ray/blob/ray-2.9.3/python/ray/_private/worker.py#L2845-L2846

                def handle_ready_tasks(tasks: List[ray.ObjectRef]) -> None:
                    nonlocal returncodes, pids, failed
                    for task in tasks:
                        idx = futures.index(task)
                        res = ray.get(task)
                        returncodes[idx] = res['return_code']
                        pids[idx] = res['pid']
                        if res['return_code'] != 0:
                            failed = True

                while not ready:
                    ready, unready = ray.wait(futures)
                handle_ready_tasks(ready)
                while unready:
                    if failed:
                        for task in unready:
                            # ray.cancel without force fails to kill tasks.
                            # We use force=True to kill unready tasks.
                            ray.cancel(task, force=True)
                            # Use SIGKILL=128+9 to indicate the task is forcely
                            # killed.
                            idx = futures.index(task)
                            returncodes[idx] = CANCELLED_RETURN_CODE
                        break
                    ready, unready = ray.wait(unready)
                    handle_ready_tasks(ready)
                # Remove the placement group after all tasks are done, so that
                # the next job can be scheduled on the released resources
                # immediately.
                ray_util.remove_placement_group(pg)
                sys.stdout.flush()
                return returncodes, pids

            futures = []
            """))

        self._add_logging_functions()
        recovery_logging_function = None
        if self._system_oom_recovery_plan is not None:
            recovery_logging_function = (
                'run_bash_command_with_log_and_return_pid_with_system_oom_recovery'
            )
            self._code.append(
                inspect.getsource(getattr(log_lib, recovery_logging_function)))

        self._code += [
            'run_bash_command_with_log = run_bash_command_with_log',
            'run_bash_command_with_log_and_return_pid = \
                ray.remote(run_bash_command_with_log_and_return_pid)',
            'autostop_lib.set_last_active_time_to_now()',
            f'job_lib.set_status({job_id!r}, job_lib.JobStatus.PENDING)',
        ]
        if recovery_logging_function is not None:
            self._code += [
                f'{recovery_logging_function} = ray.remote('
                f'{recovery_logging_function})',
                'task_submitters = []',
                'attempt_contexts = []',
            ]

    def add_setup(
        self,
        num_nodes: int,
        resources_dict: dict[str, float],
        stable_cluster_internal_ips: list[str],
        env_vars: dict[str, str],
        log_dir: str,
        setup_cmd: str | None = None,
    ) -> None:
        assert self._has_prologue, ('Call add_prologue() before '
                                    'add_setup().')
        self._has_setup = True

        setup_log_path = os.path.join(log_dir, 'setup.log')

        bundles = [copy.copy(resources_dict) for _ in range(num_nodes)]
        # Set CPU to avoid ray hanging the resources allocation
        # for remote functions, since the task will request 1 CPU
        # by default.
        task_cpu_demand = resources_dict.pop('CPU')

        if resources_dict:
            assert len(resources_dict) == 1, (
                'There can only be one type of accelerator per instance. '
                f'Found: {resources_dict}.')
            acc_name, acc_count = list(resources_dict.items())[0]
            gpu_dict = {'GPU': acc_count}
            # gpu_dict should be empty when the accelerator is not GPU.
            # TODO(zongheng,zhanghao): an alternative is to start the remote
            # cluster with custom resource 'GPU': <n> even if the accelerator(s)
            # are not GPU. We opt for the current solution for now.
            if accelerator_registry.is_schedulable_non_gpu_accelerator(
                    acc_name):
                gpu_dict = {}
            for bundle in bundles:
                bundle.update({
                    # Set the GPU to avoid ray hanging the resources allocation
                    **gpu_dict,
                })

        self._code.append(
            f'pg = ray_util.placement_group({json.dumps(bundles)}, '
            f'\'STRICT_SPREAD\')')
        self._add_waiting_for_resources_msg(num_nodes)
        self._code.append(
            textwrap.dedent("""\
            # FIXME: This will print the error message from autoscaler if
            # it is waiting for other task to finish. We should hide the
            # error message.
            ray.get(pg.ready())"""))
        self._add_job_started_msg()

        job_id = self.job_id
        if setup_cmd is not None:
            setup_envs = env_vars.copy()
            setup_envs[constants.SKYPILOT_NUM_NODES] = str(num_nodes)
            self._code += [
                textwrap.dedent(f"""\
                setup_cmd = {setup_cmd!r}
                _SETUP_CPUS = 0.0001
                # The setup command will be run as a ray task with num_cpus=_SETUP_CPUS as the
                # requirement; this means Ray will set CUDA_VISIBLE_DEVICES to an empty string.
                # We unset it so that user setup command may properly use this env var.
                setup_cmd = 'unset CUDA_VISIBLE_DEVICES; ' + setup_cmd
                job_lib.set_status({job_id!r}, job_lib.JobStatus.SETTING_UP)

                # The schedule_step should be called after the job status is set to non-PENDING,
                # otherwise, the scheduler will think the current job is not submitted yet, and
                # skip the scheduling step.
                job_lib.scheduler.schedule_step()

                # If some nodes are down and then new nodes are added after launching again,
                # the result of `ray.nodes()` will include all the nodes, so we need to get
                # the alive nodes.
                alive_nodes = [n for n in ray.nodes() if 'Alive' in n and n['Alive']]
                total_num_nodes = len(alive_nodes)
                setup_bundles = [{{"CPU": _SETUP_CPUS}} for _ in range(total_num_nodes)]
                setup_pg = ray.util.placement_group(setup_bundles, strategy='STRICT_SPREAD')
                setup_workers = [run_bash_command_with_log_and_return_pid \\
                    .options(
                        name='setup',
                        num_cpus=_SETUP_CPUS,
                        scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                            placement_group=setup_pg,
                            placement_group_bundle_index=i)
                    ) \\
                    .remote(
                        setup_cmd,
                        os.path.expanduser({setup_log_path!r}),
                        env_vars={setup_envs!r},
                        stream_logs=True,
                        with_ray=True,
                    ) for i in range(total_num_nodes)]
                setup_returncodes, setup_pids = get_or_fail(setup_workers, setup_pg)
                success = True
                failed_workers_and_returncodes = []
                for i in range(len(setup_returncodes)):
                    returncode = setup_returncodes[i]
                    pid = setup_pids[i]
                    if pid == None:
                        pid = os.getpid()
                    if returncode != 0 and returncode != CANCELLED_RETURN_CODE:
                        success = False
                        failed_workers_and_returncodes.append((pid, returncode))
                if not success:
                    msg = f'ERROR: {colorama.Fore.RED}Job {self.job_id}\\'s setup failed. '
                    msg += f'Failed workers: ' + ', '.join([f'(pid={{pid}}, returncode={{returncode}})' for pid, returncode in failed_workers_and_returncodes])
                    msg += f'. See error logs above for more details.{colorama.Style.RESET_ALL}'
                    print(msg, flush=True)
                    if int(constants.SKYLET_VERSION) >= 28:
                        job_lib.set_exit_codes({self.job_id!r}, setup_returncodes)
                    job_lib.set_status({self.job_id!r}, job_lib.JobStatus.FAILED_SETUP)
                    # This waits for all streaming logs to finish.
                    time.sleep(1)
                    # Need this to set the job status in ray job to be FAILED.
                    sys.exit(1)
                """)
            ]

        self._code.append(f'job_lib.set_job_started({self.job_id!r})')
        if setup_cmd is None:
            # Need to call schedule_step() to make sure the scheduler
            # schedule the next pending job.
            self._code.append('job_lib.scheduler.schedule_step()')

        # Export IP and node rank to the environment variables.
        self._code += [
            textwrap.dedent(f"""\
                @ray.remote
                def check_ip():
                    return ray.util.get_node_ip_address()
                gang_scheduling_id_to_ip = ray.get([
                    check_ip.options(
                            num_cpus={task_cpu_demand},
                            scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                                placement_group=pg,
                                placement_group_bundle_index=i
                            )).remote()
                    for i in range(pg.bundle_count)
                ])

                cluster_ips_to_node_id = {{ip: i for i, ip in enumerate({stable_cluster_internal_ips!r})}}
                job_ip_rank_list = sorted(gang_scheduling_id_to_ip, key=cluster_ips_to_node_id.get)
                job_ip_rank_map = {{ip: i for i, ip in enumerate(job_ip_rank_list)}}
                job_ip_list_str = '\\n'.join(job_ip_rank_list)
                """),
        ]

    def add_task(self,
                 num_nodes: int,
                 bash_script: str | None,
                 task_name: str | None,
                 resources_dict: dict[str, float],
                 log_dir: str,
                 env_vars: dict[str, str] | None = None) -> None:
        # TODO(zhwu): The resources limitation for multi-node ray.tune and
        # horovod should be considered.
        if self._system_oom_recovery_plan is not None:
            assert num_nodes == 1, (
                'System OOM recovery only supports one run task.')
        for i in range(num_nodes):
            # Ray's per-node resources, to constrain scheduling each command to
            # the corresponding node, represented by private IPs.
            self._add_ray_task(bash_script=bash_script,
                               task_name=task_name,
                               resources_dict=resources_dict.copy(),
                               log_dir=log_dir,
                               env_vars=env_vars,
                               gang_scheduling_id=i)

    def _add_ray_task(self,
                      bash_script: str | None,
                      task_name: str | None,
                      resources_dict: dict[str, float],
                      log_dir: str,
                      env_vars: dict[str, str] | None = None,
                      gang_scheduling_id: int = 0) -> None:
        """Generates code for a ray remote task that runs a bash command."""
        assert self._has_setup, 'Call add_setup() before add_task().'
        task_index = self._num_run_tasks
        self._num_run_tasks += 1
        if self._system_oom_recovery_plan is not None:
            assert task_index == 0, (
                'System OOM recovery only supports one run task.')

        task_cpu_demand = resources_dict.pop('CPU')
        # Build remote_task.options(...)
        #   resources=...
        #   num_gpus=...
        options = []
        options.append(f'num_cpus={task_cpu_demand}')

        acc_name, acc_count = self._get_accelerator_details(resources_dict)
        num_gpus = 0.0
        if acc_name is not None:
            assert resources_dict, ('There can only be one type of accelerator '
                                    'per instance.')
            options.append(f'resources={json.dumps(resources_dict)}')
            if not accelerator_registry.is_schedulable_non_gpu_accelerator(
                    acc_name):
                num_gpus = acc_count
                options.append(f'num_gpus={num_gpus}')
        options.append(
            'scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy('  # pylint: disable=line-too-long
            'placement_group=pg, '
            f'placement_group_bundle_index={gang_scheduling_id})')
        if self._system_oom_recovery_plan is not None:
            # Keep the generated driver as the only retry authority.  A Ray
            # retry here could overlap the replacement submitted after the
            # old command scope has been positively cleaned up.
            options.append('max_retries=0')

        sky_env_vars_dict_str = [
            textwrap.dedent(f"""\
            sky_env_vars_dict = {{}}
            sky_env_vars_dict['{constants.SKYPILOT_NODE_IPS}'] = job_ip_list_str
            sky_env_vars_dict['{constants.SKYPILOT_NUM_NODES}'] = len(job_ip_rank_list)
            """)
        ]

        if env_vars is not None:
            sky_env_vars_dict_str.extend(f'sky_env_vars_dict[{k!r}] = {v!r}'
                                         for k, v in env_vars.items())
        sky_env_vars_dict_str = '\n'.join(sky_env_vars_dict_str)

        options_str = ', '.join(options)
        logger.debug('Added Task with options: '
                     f'{options_str}')
        unset_ray_env_vars = ' && '.join(
            [f'unset {var}' for var in UNSET_RAY_ENV_VARS])
        if (self._system_oom_recovery_plan is not None and
                self._system_oom_recovery_plan.profile_version
                == system_oom_recovery.PROFILE_VERSION_OWNED_CONTAINER):
            spec = self._system_oom_recovery_plan.owned_container_spec
            assert spec is not None
            if bash_script != spec.render():
                raise ValueError(
                    'v2 system-OOM task.run is not the canonical owned spec')
            # The v2 supervisor applies the typed prelude and postlude. It must
            # never receive or execute an inferred shell representation.
            task_bash_script = None
        else:
            task_bash_script = (self.build_task_bash_script(
                bash_script, env_prefix=unset_ray_env_vars)
                                if bash_script is not None else None)
        if self._system_oom_recovery_plan is not None:
            submitter_name = f'submit_task_attempt_{task_index}'
            submission_code = textwrap.indent(
                textwrap.dedent(f"""\
                def {submitter_name}(attempt_number, attempt_context):
                    future = run_bash_command_with_log_and_return_pid_with_system_oom_recovery \\
                        .options(name=name_str, {options_str}) \\
                        .remote(
                            script,
                            log_path,
                            env_vars=sky_env_vars_dict,
                            stream_logs=True,
                            with_ray=True,
                            recovery_context=attempt_context,
                            recovery_plan=system_oom_recovery_plan,
                        )
                    return future

                task_submitters.append({submitter_name})
                task_attempt_context = system_oom_recovery.new_attempt_context(
                    {self.job_id!r}, {task_index!r}, 0,
                    system_oom_recovery_plan)
                task_future = {submitter_name}(0, task_attempt_context)
                futures.append(task_future)
                attempt_contexts.append(task_attempt_context)"""), ' ' * 12)
        else:
            submission_code = textwrap.indent(
                textwrap.dedent(f"""\
                futures.append(run_bash_command_with_log_and_return_pid \\
                        .options(name=name_str, {options_str}) \\
                        .remote(
                            script,
                            log_path,
                            env_vars=sky_env_vars_dict,
                            stream_logs=True,
                            with_ray=True,
                        ))"""), ' ' * 12)
        script_guard = 'script is not None'
        if self._system_oom_recovery_plan is not None:
            script_guard += (
                ' or system_oom_recovery_plan.profile_version == '
                'system_oom_recovery.PROFILE_VERSION_OWNED_CONTAINER')
        self._code += [
            sky_env_vars_dict_str,
            textwrap.dedent(f"""\
        script = {task_bash_script!r}

        if {script_guard}:
            sky_env_vars_dict['{constants.SKYPILOT_NUM_GPUS_PER_NODE}'] = {int(math.ceil(num_gpus))!r}

            ip = gang_scheduling_id_to_ip[{gang_scheduling_id!r}]
            rank = job_ip_rank_map[ip]

            if len(cluster_ips_to_node_id) == 1: # Single-node task on single-node cluter
                name_str = '{task_name},' if {task_name!r} != None else 'task,'
                log_path = os.path.expanduser(os.path.join({log_dir!r}, 'run.log'))
            else: # Single-node or multi-node task on multi-node cluster
                idx_in_cluster = cluster_ips_to_node_id[ip]
                if cluster_ips_to_node_id[ip] == 0:
                    node_name = 'head'
                else:
                    node_name = f'worker{{idx_in_cluster}}'
                name_str = f'{{node_name}}, rank={{rank}},'
                log_path = os.path.expanduser(os.path.join({log_dir!r}, f'{{rank}}-{{node_name}}.log'))
            sky_env_vars_dict['{constants.SKYPILOT_NODE_RANK}'] = rank

            sky_env_vars_dict['SKYPILOT_INTERNAL_JOB_ID'] = {self.job_id}

{submission_code}""")
        ]

    def add_epilogue(self) -> None:
        """Generates code that waits for all tasks, then exits."""
        if self._system_oom_recovery_plan is not None:
            assert self._num_run_tasks == 1, (
                'System OOM recovery requires exactly one run task.')
            self._code.append(
                textwrap.dedent(f"""\
                returncodes, _ = system_oom_recovery.get_or_fail_with_recovery(
                    ray,
                    ray_util,
                    futures[0],
                    pg,
                    task_submitters[0],
                    attempt_contexts[0],
                    {self.job_id!r},
                    system_oom_recovery_plan,
                )"""))
        else:
            self._code.append('returncodes, _ = get_or_fail(futures, pg)')
        super().add_epilogue()

    def _build_final_status_update(self, status_expression: str) -> str:
        if self._system_oom_recovery_plan is None:
            return super()._build_final_status_update(status_expression)
        # Cancellation and structured EXHAUSTED+FAILED are terminal decisions
        # made under the same per-job lock as replay submission.  The generated
        # driver's generic epilogue must not overwrite either one after the
        # recovery helper returns.
        return textwrap.dedent(f"""\
            with job_lib.job_status_lock({self.job_id!r}):
                current_job_status = job_lib.get_status_no_lock({self.job_id!r})
                if (current_job_status is not None and
                        not current_job_status.is_terminal()):
                    job_lib._set_status_no_lock(  # pylint: disable=protected-access
                        {self.job_id!r}, {status_expression})""")


class SlurmCodeGen(TaskCodeGen):
    """Code generator for task execution on Slurm using native srun."""

    def __init__(
        self,
        slurm_job_id: str,
        container_name: str | None,
    ):
        """Initialize SlurmCodeGen.

        Args:
            slurm_job_id: The Slurm job ID, i.e. SLURM_JOB_ID
            container_name: pyxis container name, or None
        """
        super().__init__()
        self._slurm_job_id = slurm_job_id
        self._container_name = container_name

    def add_prologue(self, job_id: int) -> None:
        assert not self._has_prologue, 'add_prologue() called twice?'
        self._has_prologue = True
        self.job_id = job_id

        self._add_common_imports()

        self._code.append(
            textwrap.dedent("""\
            import colorama
            import copy
            import json
            import multiprocessing
            import signal
            import threading
            from sky.backends import backend_utils
            """))
        self._add_skylet_imports()

        self._add_constants()

        self._add_logging_functions()

        self._code.append(
            textwrap.dedent(f"""\
            def _cancel_slurm_job_steps():
                slurm_job_id = {self._slurm_job_id!r}
                assert slurm_job_id is not None, 'SLURM_JOB_ID is not set'
                try:
                    # Query steps for this job: squeue -s -j JOBID -h -o "%i %j"
                    # Output format: "JOBID.STEPID STEPNAME"
                    # TODO(kevin): This assumes that compute node is able
                    # to run client commands against the controller.
                    # Validate this assumption.
                    result = subprocess.run(
                        ['squeue', '-s', '-j', slurm_job_id, '-h', '-o', '%i %j'],
                        capture_output=True, text=True, check=False)
                    for line in result.stdout.strip().split('\\n'):
                        if not line:
                            continue
                        parts = line.split()
                        assert len(parts) >= 2, 'Expected at least 2 parts'
                        step_id, step_name = parts[0], parts[1]
                        if step_name == f'sky-{self.job_id}':
                            subprocess.run(['scancel', step_id],
                                            check=False, capture_output=True)
                except Exception as e:
                    print(f'Error in _cancel_slurm_job_steps: {{e}}', flush=True)
                    pass

            def _slurm_cleanup_handler(signum, _frame):
                _cancel_slurm_job_steps()
                # Re-raise to let default handler terminate.
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)

            signal.signal(signal.SIGTERM, _slurm_cleanup_handler)
            """))

        self._code += [
            'autostop_lib.set_last_active_time_to_now()',
            f'job_lib.set_status({job_id!r}, job_lib.JobStatus.PENDING)',
        ]

        self._setup_cmd: str | None = None
        self._setup_envs: dict[str, str] | None = None
        self._setup_log_dir: str | None = None
        self._setup_num_nodes: int | None = None

    def add_setup(
        self,
        num_nodes: int,
        resources_dict: dict[str, float],
        stable_cluster_internal_ips: list[str],
        env_vars: dict[str, str],
        log_dir: str,
        setup_cmd: str | None = None,
    ) -> None:
        assert self._has_prologue, ('Call add_prologue() before add_setup().')
        self._has_setup = True
        self._cluster_num_nodes = len(stable_cluster_internal_ips)
        self._stable_cluster_ips = stable_cluster_internal_ips

        self._add_waiting_for_resources_msg(num_nodes)

        # Store setup information for use in add_task().
        if setup_cmd is not None:
            setup_envs = env_vars.copy()
            setup_envs[constants.SKYPILOT_NUM_NODES] = str(num_nodes)
            self._setup_cmd = setup_cmd
            self._setup_envs = setup_envs
            self._setup_log_dir = log_dir
            self._setup_num_nodes = num_nodes

    def add_task(
        self,
        num_nodes: int,
        bash_script: str | None,
        task_name: str | None,
        resources_dict: dict[str, float],
        log_dir: str,
        env_vars: dict[str, str] | None = None,
    ) -> None:
        """Generates code for invoking a bash command
        using srun within sbatch allocation.
        """
        assert self._has_setup, 'Call add_setup() before add_task().'
        env_vars = env_vars or {}
        task_name = task_name if task_name is not None else 'task'

        acc_name, acc_count = self._get_accelerator_details(resources_dict)
        num_gpus = 0
        if (acc_name is not None and
                not accelerator_registry.is_schedulable_non_gpu_accelerator(
                    acc_name)):
            num_gpus = int(math.ceil(acc_count))

        # Slurm does not support fractional CPUs.
        task_cpu_demand = int(math.ceil(resources_dict.pop('CPU')))

        sky_env_vars_dict_str = [
            textwrap.dedent(f"""\
            sky_env_vars_dict = {{}}
            sky_env_vars_dict['SKYPILOT_INTERNAL_JOB_ID'] = {self.job_id}
            """)
        ]

        if env_vars:
            sky_env_vars_dict_str.extend(f'sky_env_vars_dict[{k!r}] = {v!r}'
                                         for k, v in env_vars.items())
        sky_env_vars_dict_str = '\n'.join(sky_env_vars_dict_str)

        has_setup_cmd = self._setup_cmd is not None
        task_bash_script = (self.build_task_bash_script(bash_script or '')
                            if bash_script or has_setup_cmd else '')
        streaming_msg = self._get_job_started_msg()

        container_flags = ''
        if self._container_name is not None:
            # --container-remap-root must be passed on every srun to get
            # correct $HOME
            container_flags = (
                ' --container-remap-root'
                f' --container-name={shlex.quote(self._container_name)}:exec')

        self._code += [
            sky_env_vars_dict_str,
            textwrap.dedent(f"""\
            script = {task_bash_script!r}

            if script or {has_setup_cmd!r}:
                sky_env_vars_dict['{constants.SKYPILOT_NUM_GPUS_PER_NODE}'] = {num_gpus}

                # Signal files for setup/run synchronization:
                # 1. alloc_signal_file: srun has acquired allocation
                # 2. setup_done_signal_file: Driver has finished setup, run can proceed
                #
                # Signal files are stored in home directory, which is
                # assumed to be on a shared NFS mount accessible by all nodes.
                # To support clusters with non-NFS home directories, we would
                # need to let users specify an NFS-backed "working directory"
                # or use a different coordination mechanism.
                alloc_signal_file = f'~/.sky_alloc_{self._slurm_job_id}_{self.job_id}'
                alloc_signal_file = os.path.expanduser(alloc_signal_file)
                setup_done_signal_file = f'~/.sky_setup_done_{self._slurm_job_id}_{self.job_id}'
                setup_done_signal_file = os.path.expanduser(setup_done_signal_file)

                # Start exclusive srun in a thread to reserve allocation (similar to ray.get(pg.ready()))
                gpu_arg = f'--gpus-per-node={num_gpus}'

                def build_task_runner_cmd(user_script, extra_flags, log_dir, env_vars_dict,
                                          task_name=None, is_setup=False,
                                          alloc_signal=None, setup_done_signal=None):
                    env_vars_json = json.dumps(env_vars_dict)

                    log_dir = shlex.quote(log_dir)
                    env_vars = shlex.quote(env_vars_json)
                    cluster_ips = shlex.quote(",".join({self._stable_cluster_ips!r}))

                    cluster_home = shlex.quote(os.path.expanduser('~'))
                    runner_args = f'--log-dir={{log_dir}} --env-vars={{env_vars}} --cluster-num-nodes={self._cluster_num_nodes} --cluster-ips={{cluster_ips}} --cluster-home-dir={{cluster_home}}'

                    if task_name is not None:
                        runner_args += f' --task-name={{shlex.quote(task_name)}}'

                    if is_setup:
                        runner_args += ' --is-setup'

                    if alloc_signal is not None:
                        runner_args += f' --alloc-signal-file={{shlex.quote(alloc_signal)}}'

                    if setup_done_signal is not None:
                        runner_args += f' --setup-done-signal-file={{shlex.quote(setup_done_signal)}}'

                    script_path = None
                    prefix = 'sky_setup_' if is_setup else 'sky_task_'
                    if backend_utils.is_command_length_over_limit(user_script):
                        with tempfile.NamedTemporaryFile('w', prefix=prefix, suffix='.sh', delete=False) as f:
                            f.write(user_script)
                            script_path = f.name
                        runner_args += f' --script-path={{shlex.quote(script_path)}}'
                    else:
                        runner_args += f' --script={{shlex.quote(user_script)}}'

                    # Use /usr/bin/env explicitly to work around a Slurm quirk where
                    # srun's execvp() doesn't check execute permissions, failing when
                    # $HOME/.local/bin/env (non-executable, from uv installation)
                    # shadows /usr/bin/env.
                    job_suffix = '-setup' if is_setup else ''
                    # Unset SLURM_* environment variables before running srun.
                    # When this srun runs inside another srun (from
                    # SlurmCommandRunner.run), inherited variables like
                    # SLURM_CPU_BIND, SLURM_NNODES, and SLURM_NODELIST constrain
                    # the inner srun to the parent step's allocation. This causes
                    # "CPU binding outside of job step allocation" errors.
                    # Unsetting SLURM_* variables allows this srun to access the
                    # full job allocation. See:
                    # https://support.schedmd.com/show_bug.cgi?id=14298
                    # https://github.com/huggingface/datatrove/issues/248
                    #
                    # Preserve SLURM_CONF* (SLURM_CONF, SLURM_CONF_SERVER): srun
                    # needs these to locate slurmctld when /etc/slurm/slurm.conf
                    # is not present and DNS SRV discovery is unavailable. On
                    # sites that distribute the config via SLURM_CONF, stripping
                    # it causes srun to fail with:
                    #   resolve_ctls_from_dns_srv: res_nsearch error: Unknown host
                    #   fetch_config: DNS SRV lookup failed
                    #   fatal: Could not establish a configuration source
                    cmd_parts = []
                    # Only unset SKY_RUNTIME_DIR for container runs. For non-container
                    # runs, we want to inherit the node-local SKY_RUNTIME_DIR set by
                    # SlurmCommandRunner to avoid SQLite WAL issues on shared filesystems.
                    if {True if container_flags else False}:
                        cmd_parts.append('unset SKY_RUNTIME_DIR;')
                    cmd_parts.extend([
                        constants.SKY_SLURM_PYTHON_CMD,
                        '-m sky.skylet.executor.slurm',
                        runner_args,
                    ])
                    bash_cmd = shlex.quote(' '.join(cmd_parts))
                    srun_cmd = (
                        "unset $(env | awk -F= '/^SLURM_/ && $1 !~ /^SLURM_CONF/ {{print $1}}') && "
                        f'srun --export=ALL --quiet --unbuffered --kill-on-bad-exit --jobid={self._slurm_job_id} '
                        f'--job-name=sky-{self.job_id}{{job_suffix}} --ntasks-per-node=1{container_flags} {{extra_flags}} '
                        f'/bin/bash -c {{bash_cmd}}'
                    )

                    def cleanup():
                        if script_path is not None:
                            os.remove(script_path)

                    return srun_cmd, cleanup

                def run_thread_func():
                    # This blocks until Slurm allocates resources (--exclusive)
                    # --mem=0 to match RayCodeGen's behavior where we don't explicitly request memory.
                    run_flags = f'--nodes={num_nodes} --cpus-per-task={task_cpu_demand} --mem=0 {{gpu_arg}} --exclusive'
                    srun_cmd, cleanup = build_task_runner_cmd(
                        script, run_flags, {log_dir!r}, sky_env_vars_dict,
                        task_name={task_name!r},
                        alloc_signal=alloc_signal_file,
                        setup_done_signal=setup_done_signal_file
                    )

                    proc = subprocess.Popen(srun_cmd, shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT,
                                          text=True)
                    for line in proc.stdout:
                        print(line, end='', flush=True)
                    proc.wait()

                    cleanup()
                    return {{'return_code': proc.returncode, 'pid': proc.pid}}

                run_thread_result = {{'result': None}}
                def run_thread_wrapper():
                    run_thread_result['result'] = run_thread_func()

                run_thread = threading.Thread(target=run_thread_wrapper)
                run_thread.start()

                # Wait for allocation signal from inside srun
                while not os.path.exists(alloc_signal_file):
                    if not run_thread.is_alive():
                        # srun failed before creating the signal file.
                        run_thread.join()
                        result = run_thread_result['result']
                        returncode = int(result.get('return_code', 1))
                        pid = result.get('pid', os.getpid())
                        msg = f'ERROR: {colorama.Fore.RED}Job {self.job_id}\\'s setup failed with return code {{returncode}} (pid={{pid}}).'
                        msg += f' See error logs above for more details.{colorama.Style.RESET_ALL}'
                        print(msg, flush=True)
                        returncodes = [returncode]
                        if int(constants.SKYLET_VERSION) >= 28:
                            job_lib.set_exit_codes({self.job_id!r}, returncodes)
                        job_lib.set_status({self.job_id!r}, job_lib.JobStatus.FAILED_SETUP)
                        sys.exit(1)
                    time.sleep(0.1)

                print({streaming_msg!r}, flush=True)

                if {has_setup_cmd!r}:
                    job_lib.set_status({self.job_id!r}, job_lib.JobStatus.SETTING_UP)

                    # The schedule_step should be called after the job status is set to
                    # non-PENDING, otherwise, the scheduler will think the current job
                    # is not submitted yet, and skip the scheduling step.
                    job_lib.scheduler.schedule_step()

                    # --overlap as we have already secured allocation with the srun for the run section,
                    # and otherwise this srun would get blocked and deadlock.
                    setup_flags = f'--overlap --nodes={self._setup_num_nodes}'
                    setup_srun, setup_cleanup = build_task_runner_cmd(
                        {self._setup_cmd!r}, setup_flags, {self._setup_log_dir!r}, {self._setup_envs!r},
                        is_setup=True
                    )

                    # Run setup srun directly, streaming output to driver stdout
                    setup_proc = subprocess.Popen(setup_srun, shell=True,
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.STDOUT,
                                                 text=True)
                    for line in setup_proc.stdout:
                        print(line, end='', flush=True)
                    setup_proc.wait()

                    setup_cleanup()

                    setup_returncode = setup_proc.returncode
                    if setup_returncode != 0:
                        setup_pid = setup_proc.pid
                        msg = f'ERROR: {colorama.Fore.RED}Job {self.job_id}\\'s setup failed with return code {{setup_returncode}} (pid={{setup_pid}}).'
                        msg += f' See error logs above for more details.{colorama.Style.RESET_ALL}'
                        print(msg, flush=True)
                        job_lib.set_status({self.job_id!r}, job_lib.JobStatus.FAILED_SETUP)
                        # Cancel the srun spawned by run_thread_func.
                        _cancel_slurm_job_steps()
                        sys.exit(1)

                job_lib.set_job_started({self.job_id!r})
                if not {has_setup_cmd!r}:
                    # Need to call schedule_step() to make sure the scheduler
                    # schedule the next pending job.
                    job_lib.scheduler.schedule_step()

                # Signal run thread to proceed.
                pathlib.Path(setup_done_signal_file).touch()

                # Wait for run thread to complete.
                run_thread.join()
                result = run_thread_result['result']

                # Cleanup signal files
                if os.path.exists(alloc_signal_file):
                    os.remove(alloc_signal_file)
                if os.path.exists(setup_done_signal_file):
                    os.remove(setup_done_signal_file)

                returncodes = [int(result.get('return_code', 1))]
            else:
                returncodes = [0]
            """),
        ]
