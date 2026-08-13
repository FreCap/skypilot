"""Scheduler for managed jobs.

Once managed jobs are submitted via submit_job, the scheduler is responsible for
the business logic of deciding when they are allowed to start, and choosing the
right one to start. The scheduler will also schedule jobs that are already live
but waiting to launch a new task or recover.

The scheduler is not its own process - instead, maybe_schedule_next_jobs() can
be called from any code running on the managed jobs controller instance to
trigger scheduling of new jobs if possible. This function should be called
immediately after any state change that could result in jobs newly being able to
be scheduled. If the job is running in a pool, the scheduler will only schedule
jobs for the same pool, because the resources limitations are per-pool (see the
following section for more details).

The scheduling logic limits #running jobs according to three limits:
1. The number of jobs that can be launching (that is, STARTING or RECOVERING) at
   once, based on the number of CPUs. This the most compute-intensive part of
   the job lifecycle, which is why we have an additional limit.
   See sky/utils/controller_utils.py::_get_launch_parallelism.
2. The number of jobs that can be running at any given time, based on the amount
   of memory. Since the job controller is doing very little once a job starts
   (just checking its status periodically), the most significant resource it
   consumes is memory.
   See sky/utils/controller_utils.py::_get_job_parallelism.
3. The number of jobs that can be running in a pool at any given time, based on
   the number of ready workers in the pool. (See _can_start_new_job.)

The state of the scheduler is entirely determined by the schedule_state column
of all the jobs in the job_info table. This column should only be modified via
the functions defined in this file. We will always hold the lock while modifying
this state. See state.ManagedJobScheduleState.

Nomenclature:
- job: same as managed job (may include multiple tasks)
- launch/launching: launching a cluster (sky.launch) as part of a job
- start/run: create the job controller process for a job
- schedule: transition a job to the LAUNCHING state, whether a new job or a job
  that is already alive
- alive: a job controller exists (includes multiple schedule_states: ALIVE,
  ALIVE_WAITING, LAUNCHING)
"""

from argparse import ArgumentParser
import asyncio
import contextlib
import os
import typing

from sky import exceptions
from sky import sky_logging
from sky import skypilot_config
from sky.jobs import state
from sky.skylet import constants
from sky.utils import asyncio_utils
from sky.utils import controller_utils
from sky.utils import dag_utils

logger = sky_logging.init_logger('sky.jobs.controller')

# Managed-job controller capacity is owned by fixed runtime slots in
# sky.jobs.controller_slots.  This module contains only durable scheduling.


def submit_jobs(job_ids: list[int],
                dag_yaml_path: str,
                original_user_yaml_path: str,
                env_file_path: str,
                priority: int,
                priority_class: str | None = None) -> None:
    """Submit multiple existing jobs to the scheduler.

    This should be called after jobs are created in the `spot` table as
    PENDING. It will tell the scheduler to try and start the job controllers, if
    there are resources available.

    The user hash should be set (e.g. via SKYPILOT_USER_ID) before calling this.
    """
    job_ids = list(dict.fromkeys(job_ids))
    if not job_ids:
        return

    with open(dag_yaml_path, encoding='utf-8') as dag_file:
        dag_yaml_content = dag_file.read()
    with open(original_user_yaml_path,
              encoding='utf-8') as original_user_yaml_file:
        original_user_yaml_content = original_user_yaml_file.read()
    with open(env_file_path, encoding='utf-8') as env_file:
        env_file_content = env_file.read()

    # Read config file if SKYPILOT_CONFIG env var is set
    config_file_content: str | None = None
    config_file_path = os.environ.get(skypilot_config.ENV_VAR_SKYPILOT_CONFIG)
    if config_file_path:
        config_file_path = os.path.expanduser(config_file_path)
        if os.path.exists(config_file_path):
            with open(config_file_path, encoding='utf-8') as config_file:
                config_file_content = config_file.read()

    config_bytes = (len(config_file_content) if config_file_content else 0)
    logger.debug(f'Storing jobs {job_ids} file contents in database '
                 f'(DAG bytes={len(dag_yaml_content)}, '
                 f'original user yaml bytes={len(original_user_yaml_content)}, '
                 f'env bytes={len(env_file_content)}, '
                 f'config bytes={config_bytes}).')

    # Submit all jobs
    state.scheduler_set_waiting(job_ids, dag_yaml_content,
                                original_user_yaml_content, env_file_content,
                                config_file_content, priority, priority_class)


@asyncio_utils.shield
async def _release_launch_slot(job_id: int, starting: set[int],
                               starting_lock: asyncio.Lock,
                               starting_signal: asyncio.Condition) -> None:
    """Release one launch slot even under repeated cancellation."""
    async with starting_lock:
        starting.remove(job_id)
        starting_signal.notify()


async def _complete_launch_state_transition(
        transition: typing.Coroutine[typing.Any, typing.Any, None]) -> None:
    """Finish a durable launch outcome before honoring cancellation."""
    transition_task = asyncio.create_task(transition)
    cancelled = False
    while True:
        try:
            await asyncio.shield(transition_task)
            break
        except asyncio.CancelledError:  # noqa: ASYNC103
            # Repeated cancellation must not release the in-memory launch slot
            # while the durable row still says LAUNCHING.
            cancelled = True
            if transition_task.done():
                break  # noqa: ASYNC104

    # Preserve transition failures instead of misreporting them as caller
    # cancellation.
    transition_task.result()
    if cancelled:
        raise asyncio.CancelledError()


async def job_resumed(job_id: int) -> None:
    """Restore ALIVE after reclaiming an already-running managed job.

    A controller resume deliberately skips the provider launch context, so it
    must complete the same durable LAUNCHING-to-ALIVE transition explicitly.
    The state writer retains the outer-generation predicate and this shield
    keeps cancellation from stranding the row in LAUNCHING.
    """
    await _complete_launch_state_transition(
        state.scheduler_set_alive_async(job_id))


@contextlib.asynccontextmanager
async def scheduled_launch(
    job_id: int,
    starting: set[int],
    starting_lock: asyncio.Lock,
    starting_signal: asyncio.Condition,
):
    """Launch as part of an ongoing job.

    A newly started job will already be LAUNCHING, and this will immediately
    enter the context.

    If a job is ongoing (ALIVE schedule_state), there are two scenarios where we
    may need to call sky.launch again during the course of a job controller:
    - for tasks after the first task
    - for recovery

    This function will mark the job as ALIVE_WAITING, which indicates to the
    scheduler that it wants to transition back to LAUNCHING. Then, it will wait
    until the scheduler transitions the job state, before entering the context.

    On exiting the context, the job will transition to ALIVE.

    This should only be used within the job controller for the given job_id. If
    multiple uses of this context are nested, behavior is undefined. Don't do
    that.
    """
    # Both values are fixed at submission, so one async read of the job_info
    # row covers them without blocking the shared controller event loop the
    # way the previous two synchronous single-column reads did.
    pool, execution = await state.get_pool_and_execution_from_job_id_async(
        job_id)
    # For pool, since there is no execution.launch, we don't need to have all
    # the ALIVE_WAITING state. The state transition will be
    # WAITING -> ALIVE -> DONE without any intermediate transitions.
    if pool is not None:
        yield
        return

    # For JobGroups, multiple tasks share the same job_id but each launches
    # a different cluster in parallel. We handle scheduler state at the group
    # level in _run_job_group(), so bypass per-task scheduling here.
    # JobGroup-ness is fixed at submission and recorded in the slim
    # job_info.execution column ('parallel' == JobGroup), so read that instead
    # of fetching + re-parsing the full DAG YAML on every launch/recovery
    # attempt. The column can be NULL (writers pass execution=None explicitly,
    # bypassing the 'serial' server_default, and legacy-version codegen omits
    # it), but no NULL row can be a JobGroup: every JobGroup-capable writer
    # records 'parallel', and JobGroups did not exist before the migration
    # that added the column. is_job_group_execution(None) is False, so NULL
    # rows and unknown job_ids match the previous behavior
    # (get_job_dag_content returned None -> not a JobGroup).
    # TODO(zhwu): make JobGroup scheduler aware.
    if dag_utils.is_job_group_execution(execution):
        yield
        return

    assert starting_lock == starting_signal._lock, (  # type: ignore #pylint: disable=protected-access
        'starting_lock and starting_signal must use the same lock')

    # The capacity check and the slot claim must happen under a single lock
    # acquisition: with them split, every coroutine woken between another
    # waiter's check and its add also sees a free slot, and the launch cap is
    # exceeded.
    while True:
        async with starting_lock:
            # ControllerManager preclaims the initial slot before handing the
            # job to its background coroutine. Honor that ownership even when
            # this job fills the cap; later launches must still atomically
            # claim genuinely free capacity.
            if (job_id in starting or
                    len(starting) < controller_utils.LAUNCHES_PER_WORKER):
                starting.add(job_id)
                break
            logger.info('Too many jobs starting, waiting for a slot')
            await starting_signal.wait()

    logger.info(f'Starting job {job_id}')

    try:
        # Inside the try so a failure here (e.g. a transient DB error) still
        # releases the slot below instead of leaking it for the lifetime of
        # the controller process.
        await state.scheduler_set_launching_async(job_id)
        yield
    except exceptions.NoClusterLaunchedError:
        # Cancellation intentionally wins over the launch error, but only
        # after the backoff row is durable.
        await _complete_launch_state_transition(  # noqa: ASYNC120
            state.scheduler_set_backoff_async(job_id))
        raise
    else:
        await _complete_launch_state_transition(
            state.scheduler_set_alive_async(job_id))
    finally:
        await _release_launch_slot(job_id, starting, starting_lock,
                                   starting_signal)


def job_done(job_id: int, idempotent: bool = False) -> None:
    """Transition a job to DONE.

    If idempotent is True, this will not raise an error if the job is already
    DONE.

    The job could be in any terminal ManagedJobStatus. However, once DONE, it
    should never transition back to another state.

    This is only called by utils.update_managed_jobs_statuses which is sync.
    """
    state.scheduler_set_done(job_id, idempotent)


async def job_done_async(job_id: int, idempotent: bool = False):
    """Async version of job_done."""
    await state.scheduler_set_done_async(job_id, idempotent)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('dag_yaml',
                        type=str,
                        help='The path to the user job yaml file.')
    parser.add_argument('--user-yaml-path',
                        type=str,
                        help='The path to the original user job yaml file.')
    parser.add_argument(
        '--job-id',
        type=int,
        nargs='+',
        help='Job id(s) for the controller job(s). Can specify multiple.')
    parser.add_argument('--env-file',
                        type=str,
                        help='The path to the controller env file.')
    parser.add_argument('--pool',
                        type=str,
                        required=False,
                        default=None,
                        help='The pool to use for the controller job.')
    parser.add_argument(
        '--priority',
        type=int,
        default=constants.DEFAULT_PRIORITY,
        help=
        f'Job priority ({constants.MIN_PRIORITY} to {constants.MAX_PRIORITY}).'
        f' Default: {constants.DEFAULT_PRIORITY}.')
    parser.add_argument('--priority-class',
                        type=str,
                        default=None,
                        help='Named priority class for the job.')
    args = parser.parse_args()

    submit_jobs(args.job_id, args.dag_yaml, args.user_yaml_path, args.env_file,
                args.priority, args.priority_class)
