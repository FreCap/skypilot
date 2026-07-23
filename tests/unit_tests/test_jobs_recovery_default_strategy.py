"""Tests for the default recovery-strategy selection at job launch.

`fill_default_config_in_dag_for_job_launch` is the single place an
unspecified strategy is resolved (the controller's StrategyExecutor.make
always receives an explicit one afterwards). EAGER_NEXT_REGION (the
registry default) immediately terminates the whole cluster and relaunches
in a different region on any recovery; for a multi-node job whose common
failure mode is losing a single node that turns every flap into a
full-fleet teardown, so unspecified-strategy multi-node jobs must default
to FAILOVER (same-cluster relaunch first). An explicit user choice always
wins, and pool jobs are excluded.
"""
import concurrent.futures
from unittest import mock

import sky
from sky import skypilot_config
from sky.jobs import recovery_strategy
from sky.resources import Resources
from sky.utils import dag_utils
from sky.utils import registry


def _filled_strategy(num_nodes, job_recovery, pool=None):
    task = sky.Task(run='echo hi', num_nodes=num_nodes)
    task.set_resources({Resources(job_recovery=job_recovery)})
    dag = sky.Dag()
    dag.add(task)
    dag_utils.fill_default_config_in_dag_for_job_launch(dag, pool=pool)
    filled = list(dag.tasks[0].resources)[0].job_recovery
    assert isinstance(filled, dict)
    return filled['strategy']


# Resources normalizes the strategy casing, so compare case-insensitively
# against the registry default.
_REGISTRY_DEFAULT = registry.JOBS_RECOVERY_STRATEGY_REGISTRY.default.upper()


def test_multinode_defaults_to_failover():
    assert _filled_strategy(num_nodes=500, job_recovery=None) == 'FAILOVER'


def test_multinode_dict_without_strategy_defaults_to_failover():
    # A dict with only tuning knobs (no 'strategy') still counts as
    # "strategy unspecified".
    strategy = _filled_strategy(num_nodes=2,
                                job_recovery={'max_restarts_on_errors': 3})
    assert strategy == 'FAILOVER'


def test_single_node_keeps_registry_default():
    assert _filled_strategy(num_nodes=1,
                            job_recovery=None).upper() == _REGISTRY_DEFAULT


def test_explicit_strategy_wins_over_multinode_default():
    strategy = _filled_strategy(num_nodes=500,
                                job_recovery={'strategy': _REGISTRY_DEFAULT})
    assert strategy.upper() == _REGISTRY_DEFAULT


def test_explicit_string_strategy_wins_over_multinode_default():
    strategy = _filled_strategy(num_nodes=500, job_recovery=_REGISTRY_DEFAULT)
    assert strategy.upper() == _REGISTRY_DEFAULT


def test_pool_jobs_keep_registry_default():
    strategy = _filled_strategy(num_nodes=2, job_recovery=None, pool='my-pool')
    assert strategy.upper() == _REGISTRY_DEFAULT


def test_refill_is_idempotent():
    # The CLI fills defaults client-side and the server fills again; the
    # second pass must see the explicit strategy and keep it.
    task = sky.Task(run='echo hi', num_nodes=500)
    task.set_resources({Resources(job_recovery=None)})
    dag = sky.Dag()
    dag.add(task)
    dag_utils.fill_default_config_in_dag_for_job_launch(dag)
    dag_utils.fill_default_config_in_dag_for_job_launch(dag)
    filled = list(dag.tasks[0].resources)[0].job_recovery
    assert filled['strategy'] == 'FAILOVER'


def test_recovery_sdk_calls_enter_workspace_inside_worker_thread():
    executor = recovery_strategy.StrategyExecutor.__new__(
        recovery_strategy.StrategyExecutor)
    executor.workspace = 'research'

    def _active_workspace(*_args, **_kwargs):
        return skypilot_config.get_active_workspace()

    def _launch_from_outer_workspace():
        with skypilot_config.local_active_workspace_ctx('outer'):
            result = executor._launch_in_workspace()  # pylint: disable=protected-access
            return result, skypilot_config.get_active_workspace()

    def _exec_from_outer_workspace():
        with skypilot_config.local_active_workspace_ctx('outer'):
            result = executor._exec_in_workspace()  # pylint: disable=protected-access
            return result, skypilot_config.get_active_workspace()

    with mock.patch.object(recovery_strategy.sdk,
                           'launch',
                           side_effect=_active_workspace), \
         mock.patch.object(recovery_strategy.sdk,
                           'exec',
                           side_effect=_active_workspace), \
         concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        launched_workspace, after_launch = pool.submit(
            _launch_from_outer_workspace).result()
        exec_workspace, after_exec = pool.submit(
            _exec_from_outer_workspace).result()

    assert launched_workspace == 'research'
    assert exec_workspace == 'research'
    assert after_launch == 'outer'
    assert after_exec == 'outer'
