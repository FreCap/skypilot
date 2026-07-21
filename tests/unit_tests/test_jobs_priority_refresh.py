"""Tests for the slim priority refresh on managed-job recovery.

The controller re-reads only the priority scalars from the persisted DAG
YAML before a recovery relaunch (out-of-band priority changes rewrite that
YAML). The refresh must not require rehydrating the full DAG: it has to
work even when the rest of the spec cannot be reconstructed, and must never
raise into the recovery path.
"""
import textwrap

import pytest

import sky
from sky.jobs import file_content_utils
from sky.jobs import recovery_strategy
from sky.resources import Resources
from sky.utils import dag_utils

CHAIN_WITH_HEADER = textwrap.dedent("""\
    name: pipeline
    ---
    name: t0
    resources:
      cpus: 2
      priority: 500
    run: echo hi
    """)

CHAIN_NO_HEADER = textwrap.dedent("""\
    name: t0
    resources:
      priority: -7
      priority_class: high
    run: echo hi
    """)

ORDERED_RESOURCES = textwrap.dedent("""\
    name: t0
    resources:
      ordered:
        - cpus: 2
          priority: 300
        - cpus: 4
          priority: 300
    """)

ANY_OF_SHARED_PRIORITY = textwrap.dedent("""\
    name: t0
    resources:
      priority: 42
      any_of:
        - cpus: 2
        - cpus: 4
    """)

JOB_GROUP = textwrap.dedent("""\
    name: group
    execution: parallel
    ---
    name: j0
    resources:
      priority: 100
    ---
    name: j1
    resources:
      priority: 200
    """)

# A spec whose full rehydration would fail (unknown cloud), but whose
# priority scalars are perfectly readable.
UNREHYDRATABLE = textwrap.dedent("""\
    name: t0
    resources:
      cloud: not-a-real-cloud
      priority: 5
    """)


@pytest.mark.parametrize(
    'yaml_str,task_id,expected',
    [
        (CHAIN_WITH_HEADER, 0, (500, None)),
        (CHAIN_NO_HEADER, 0, (-7, 'high')),
        (ORDERED_RESOURCES, 0, (300, None)),
        (ANY_OF_SHARED_PRIORITY, 0, (42, None)),
        (JOB_GROUP, 0, (100, None)),
        (JOB_GROUP, 1, (200, None)),
        (UNREHYDRATABLE, 0, (5, None)),
        # No resources section: priority is unset, not an error.
        ('name: t0\nrun: echo hi\n', 0, (None, None)),
        # Resources without priority.
        ('name: t0\nresources:\n  cpus: 2\n', 0, (None, None)),
        # Empty YAML parses to one trivial task (matches the full loader).
        ('', 0, (None, None)),
    ],
)
def test_extract_priority_shapes(yaml_str, task_id, expected):
    assert dag_utils.extract_task_priority_from_yaml_str(yaml_str,
                                                         task_id) == expected


@pytest.mark.parametrize(
    'yaml_str,task_id',
    [
        (CHAIN_WITH_HEADER, 1),  # task index out of range
        (CHAIN_WITH_HEADER, -1),
        ('{unparseable', 0),  # invalid yaml
        ('name: t0\nresources:\n  priority: true\n', 0),  # bool priority
        ('name: t0\nresources:\n  priority: soon\n', 0),  # non-int priority
        ('name: t0\nresources:\n  priority_class: [a]\n', 0),  # non-str class
    ],
)
def test_extract_priority_not_extractable(yaml_str, task_id):
    assert dag_utils.extract_task_priority_from_yaml_str(yaml_str,
                                                         task_id) is None


def _make_executor(priority=None, resources_container=set):
    executor = recovery_strategy.StrategyExecutor.__new__(
        recovery_strategy.StrategyExecutor)
    task = sky.Task(run='echo hi')
    task.set_resources(resources_container([Resources(priority=priority)]))
    dag = sky.Dag()
    dag.add(task)
    executor.dag = dag
    executor.job_id = 1
    executor.task_id = 0
    return executor, task


def _set_dag_content(monkeypatch, content):
    monkeypatch.setattr(file_content_utils, 'get_job_dag_content',
                        lambda job_id: content)


def _task_priority(task):
    resources = list(task.resources)
    assert len(resources) == 1
    return resources[0].priority, resources[0].priority_class


def test_refresh_applies_new_priority(monkeypatch):
    executor, task = _make_executor(priority=10)
    _set_dag_content(monkeypatch, CHAIN_WITH_HEADER)
    executor._refresh_priority_from_persisted_dag()
    assert _task_priority(task) == (500, None)


def test_refresh_clears_priority_when_yaml_drops_it(monkeypatch):
    executor, task = _make_executor(priority=10)
    _set_dag_content(monkeypatch, 'name: t0\nrun: echo hi\n')
    executor._refresh_priority_from_persisted_dag()
    assert _task_priority(task) == (None, None)


def test_refresh_works_on_unrehydratable_spec(monkeypatch):
    # The old implementation loaded the full DAG and silently kept the
    # stale priority when rehydration failed; the slim extraction must
    # still pick up the change.
    executor, task = _make_executor(priority=10)
    _set_dag_content(monkeypatch, UNREHYDRATABLE)
    executor._refresh_priority_from_persisted_dag()
    assert _task_priority(task) == (5, None)


@pytest.mark.parametrize('content', [
    None,
    '{unparseable',
    'name: t0\nresources:\n  priority: 100000\n',
])
def test_refresh_keeps_priority_on_bad_content(monkeypatch, content):
    executor, task = _make_executor(priority=10)
    _set_dag_content(monkeypatch, content)
    executor._refresh_priority_from_persisted_dag()
    assert _task_priority(task) == (10, None)


def test_refresh_keeps_priority_when_read_raises(monkeypatch):
    executor, task = _make_executor(priority=10)

    def _raise(job_id):
        raise RuntimeError('db down')

    monkeypatch.setattr(file_content_utils, 'get_job_dag_content', _raise)
    executor._refresh_priority_from_persisted_dag()
    assert _task_priority(task) == (10, None)


@pytest.mark.parametrize('container', [set, list])
def test_refresh_preserves_resources_container_type(monkeypatch, container):
    executor, task = _make_executor(priority=10, resources_container=container)
    _set_dag_content(monkeypatch, CHAIN_WITH_HEADER)
    executor._refresh_priority_from_persisted_dag()
    assert isinstance(task.resources, container)
    assert _task_priority(task) == (500, None)


def test_refresh_reads_correct_job_group_task(monkeypatch):
    executor, task = _make_executor(priority=10)
    executor.task_id = 1
    _set_dag_content(monkeypatch, JOB_GROUP)
    executor._refresh_priority_from_persisted_dag()
    assert _task_priority(task) == (200, None)
