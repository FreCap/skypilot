"""Characterization tests for the managed-jobs queue CLI boundary."""

# pylint: disable=protected-access

import ast
import hashlib
import inspect
import json
import subprocess
import sys
import textwrap

import click
import pytest

from sky.client.cli import command
from sky.client.cli import managed_jobs_queue

_BODY_HASHES = {
    '_handle_jobs_queue_request': '37653e2596bc27c3923ccdbc6fb1daee93734b29ae9c81da88fd6910429c121c',
    'StatusList.convert': 'a3bd1a40cf9309b4591037d9365cc7364946b5ad2398e1ef40323b0b0a27b2aa',
    '_parse_datetime_to_epoch': 'ffbfba258bc1cd28f624d15f2452bc76b12778d28505c7e6b78571fe81473a73',
    'jobs_queue': '7809f57f40e496e8d88d09d24131c4c64cdc7b89b04ba86a997125842588afe4',
}
_HELP_HASHES = {
    ('jobs',): 'fac38fe34b946293dcd62e9de6f96d40cf57b3e95372d45e89425eddf05372b4',
    ('jobs', 'queue'): '236ec7fc0e010c9dc67250294712cf1183039adf7a11b50c4543f2fdb60dabfe',
}
_DEFAULT_FIELDS = [
    'job_id', 'task_id', 'workspace', 'job_name', 'task_name', 'resources',
    'submitted_at', 'end_at', 'job_duration', 'recovery_count', 'status',
    'pool', 'is_primary_in_job_group', 'batch_total_batches',
    'batch_completed_batches'
]
_VERBOSE_FIELDS = _DEFAULT_FIELDS + [
    'current_cluster_name', 'job_id_on_pool_cluster', 'start_at', 'infra',
    'cloud', 'region', 'zone', 'cluster_resources', 'schedule_state', 'details',
    'failure_reason', 'metadata'
]


def _stable_ast(value: object) -> object:
    # Treat a local variable annotation as the runtime-equivalent assignment.
    # The extraction makes the existing count invariant explicit for static
    # analysis without changing the executed expression.
    if (isinstance(value, ast.AnnAssign) and
            isinstance(value.target, ast.Name) and
            value.target.id == 'num_in_progress_jobs'):
        return ('Assign', (('targets', (_stable_ast(value.target),)),
                           ('value', _stable_ast(value.value)), ('type_comment',
                                                                 None)))
    if isinstance(value, ast.AST):
        fields = []
        for field, child in ast.iter_fields(value):
            if field == 'type_params':
                continue
            fields.append((field, _stable_ast(child)))
        return type(value).__name__, tuple(fields)
    if isinstance(value, list):
        return tuple(_stable_ast(item) for item in value)
    return value


def _body_hash(value: object) -> str:
    source = textwrap.dedent(inspect.getsource(value))
    tree = ast.parse(source)
    function = next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)))
    return hashlib.sha256(repr(_stable_ast(function.body)).encode()).hexdigest()


def _callback() -> object:
    callback = command.jobs_queue.callback
    assert callback is not None
    return inspect.unwrap(callback)


def test_jobs_queue_command_hierarchy_and_facade_metadata() -> None:
    assert command.jobs.list_commands(click.Context(command.jobs)) == [
        'launch', 'queue', 'cancel', 'logs', 'dashboard', 'pool'
    ]
    assert command.jobs.commands['queue'] is command.jobs_queue

    callback = command.jobs_queue.callback
    assert callback is not None
    assert callback.__module__ == command.__name__
    assert callback.__qualname__ == 'jobs_queue'
    for name in ('_handle_jobs_queue_request', 'StatusList',
                 '_parse_datetime_to_epoch'):
        value = getattr(command, name)
        assert value.__module__ == command.__name__
        assert value.__qualname__ == name


def test_jobs_queue_symbols_are_direct_facade_aliases() -> None:
    assert command.jobs_queue is managed_jobs_queue.jobs_queue
    assert (command._handle_jobs_queue_request
            is managed_jobs_queue._handle_jobs_queue_request)
    assert command.StatusList is managed_jobs_queue.StatusList
    assert (command._parse_datetime_to_epoch
            is managed_jobs_queue._parse_datetime_to_epoch)
    assert (command._DEFAULT_MANAGED_JOB_FIELDS_TO_GET
            is managed_jobs_queue._DEFAULT_MANAGED_JOB_FIELDS_TO_GET)
    assert (command._VERBOSE_MANAGED_JOB_FIELDS_TO_GET
            is managed_jobs_queue._VERBOSE_MANAGED_JOB_FIELDS_TO_GET)
    assert command.datetime is managed_jobs_queue.datetime
    assert command.traceback is managed_jobs_queue.traceback
    assert command.env_options is managed_jobs_queue.env_options


@pytest.mark.parametrize(
    'name,value',
    [
        ('_handle_jobs_queue_request', command._handle_jobs_queue_request),
        ('StatusList.convert', command.StatusList.convert),
        ('_parse_datetime_to_epoch', command._parse_datetime_to_epoch),
        ('jobs_queue', _callback()),
    ],
)
def test_jobs_queue_bodies_are_unchanged(name: str, value: object) -> None:
    assert _body_hash(value) == _BODY_HASHES[name]


@pytest.mark.parametrize('path,expected_hash', _HELP_HASHES.items())
def test_jobs_queue_help_is_unchanged(path: tuple[str, ...],
                                      expected_hash: str) -> None:
    script = '''
import json
import sys
from click import testing
from sky.client.cli import command

result = testing.CliRunner().invoke(
    command.cli, [*sys.argv[1:], '--help'], terminal_width=80)
if result.exit_code != 0:
    raise result.exception or RuntimeError(result.output)
print('__SKYPILOT_HELP__' + json.dumps(result.output))
'''
    result = subprocess.run([sys.executable, '-c', script, *path],
                            check=True,
                            capture_output=True,
                            text=True)
    help_output = json.loads(result.stdout.rsplit('__SKYPILOT_HELP__', 1)[1])

    assert hashlib.sha256(help_output.encode()).hexdigest() == expected_hash


def test_jobs_queue_projection_constants_are_unchanged() -> None:
    assert command._NUM_MANAGED_JOBS_TO_SHOW_IN_STATUS == 5
    assert command._NUM_MANAGED_JOBS_TO_SHOW == 50
    assert command._DEFAULT_MANAGED_JOB_FIELDS_TO_GET == _DEFAULT_FIELDS
    assert command._VERBOSE_MANAGED_JOB_FIELDS_TO_GET == _VERBOSE_FIELDS
    assert command._USER_NAME_FIELD == ['user_name']
    assert command._USER_HASH_FIELD == ['user_hash']
