"""Focused tests for Kubernetes cluster status aggregation."""
import contextlib
from types import SimpleNamespace
from unittest import mock

import pytest

from sky import core


def _controller(name, user):
    pod = SimpleNamespace(metadata=SimpleNamespace(name=name))
    return SimpleNamespace(user=user, pods=[pod])


@contextlib.contextmanager
def _status_mocks(controllers, queue_side_effect):
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(core.kubernetes_utils,
                              'get_current_kube_config_context_name',
                              return_value='test-context'))
        stack.enter_context(
            mock.patch.object(core.kubernetes_utils,
                              'get_skypilot_pods',
                              return_value=[]))
        stack.enter_context(
            mock.patch.object(core.kubernetes_utils,
                              'process_skypilot_pods',
                              return_value=([], controllers, [])))
        queue = stack.enter_context(
            mock.patch.object(core.managed_jobs_core,
                              'queue_from_kubernetes_pod',
                              side_effect=queue_side_effect))
        stack.enter_context(
            mock.patch.object(core.responses,
                              'ManagedJobRecord',
                              side_effect=lambda **job: job))
        stack.enter_context(
            mock.patch.object(core.rich_utils,
                              'safe_status',
                              return_value=contextlib.nullcontext()))
        yield queue


def test_status_kubernetes_queries_job_controllers_concurrently():
    controllers = [
        _controller(f'controller-{index}', f'user-{index}')
        for index in range(4)
    ]

    def _queue(pod_name, context):
        assert context == 'test-context'
        index = int(pod_name.rsplit('-', 1)[1])
        return [{'job_name': f'job-{index}', 'job_id': index}]

    with _status_mocks(controllers, _queue), mock.patch.object(
            core.subprocess_utils,
            'run_in_parallel',
            wraps=core.subprocess_utils.run_in_parallel) as parallel:
        _, _, jobs, _ = core.status_kubernetes()

    parallel.assert_called_once()
    assert parallel.call_args.args[1] == controllers
    assert [(job['job_name'], job['user']) for job in jobs
           ] == [(f'job-{index}', f'user-{index}') for index in range(4)]


def test_status_kubernetes_preserves_controller_order_and_users():
    controllers = [
        _controller('controller-b', 'user-b'),
        _controller('controller-a', 'user-a'),
    ]
    queues = {
        'controller-a': [
            {
                'job_name': 'a-1',
                'job_id': 1
            },
            {
                'job_name': 'a-2',
                'job_id': 2
            },
        ],
        'controller-b': [{
            'job_name': 'b-1',
            'job_id': 1
        }],
    }
    with _status_mocks(controllers, lambda pod_name, context: queues[pod_name]):
        _, _, jobs, _ = core.status_kubernetes()

    assert [(job['job_name'], job['user']) for job in jobs] == [
        ('b-1', 'user-b'),
        ('a-1', 'user-a'),
        ('a-2', 'user-a'),
    ]


def test_status_kubernetes_isolates_controller_failure(caplog):
    controllers = [
        _controller('healthy', 'healthy-user'),
        _controller('unreachable', 'failed-user'),
    ]

    def _queue(pod_name, context):
        del context
        if pod_name == 'unreachable':
            raise RuntimeError('pod disappeared')
        return [{'job_name': 'healthy-job', 'job_id': 1}]

    with _status_mocks(controllers, _queue):
        _, _, jobs, _ = core.status_kubernetes()

    assert [(job['job_name'], job['user']) for job in jobs
           ] == [('healthy-job', 'healthy-user')]
    assert 'Failed to get managed jobs from controller unreachable' in caplog.text


@pytest.mark.parametrize('controllers', [[], [_controller('only', 'user')]])
def test_status_kubernetes_empty_and_single_controller_fast_paths(controllers):
    with _status_mocks(
            controllers, lambda pod_name, context: [{
                'job_name': pod_name,
                'job_id': 1
            }]) as queue, mock.patch.object(
                core.subprocess_utils,
                'ContextThreadPoolExecutor',
                side_effect=AssertionError(
                    'thread pool should not be created')):
        _, _, jobs, _ = core.status_kubernetes()

    assert queue.call_count == len(controllers)
    assert len(jobs) == len(controllers)
