"""Characterization tests for managed-job remote command generation."""

import hashlib
import inspect
import pickle
import shlex
from types import SimpleNamespace
from typing import Any

import pytest

from sky import jobs
from sky.jobs import managed_job_codegen
from sky.jobs import utils as managed_job_utils
from sky.skylet import constants


@pytest.fixture(autouse=True)
def _stable_codegen_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        managed_job_utils.skypilot_config,
        'get_active_workspace',
        lambda: 'workspace A',
    )
    monkeypatch.setattr(
        managed_job_utils.common_utils,
        'get_user_hash',
        lambda: 'user-hash',
    )
    monkeypatch.setattr(
        managed_job_utils.backend_utils,
        'get_task_resources_str',
        lambda task, is_managed_job=False:
        (f'resources:{task.name}:{is_managed_job}'),
    )


def _extract_generated_python(command: str) -> str:
    python_prefix = f'{constants.SKY_PYTHON_CMD} -u -c '
    assert python_prefix in command
    payload = shlex.split(command.split(python_prefix, 1)[1])
    assert len(payload) == 1
    return payload[0]


_CODEGEN_CASES = [
    ('get_job_table', (), {},
     '45c1ea40cb92b92969a7fc5db23c3c4a818d5300850aaaa9bdac401e61e36181'),
    ('get_job_table', (), {
        'skip_finished': True,
        'accessible_workspaces': ['a', 'b'],
        'job_ids': [1, 2],
        'workspace_match': 'ws.*',
        'name_match': 'job',
        'pool_match': 'pool',
        'page': 2,
        'limit': 7,
        'user_hashes': ['u', None],
        'statuses': ['RUNNING'],
        'fields': ['job_id', 'is_batch'],
        'sort_by': 'submitted_at',
        'sort_order': 'desc',
        'submitted_after': 1.25,
        'submitted_before': 9.5,
    }, '9c9a68ee14ab4fff5259b607951684e6cf1ef3f1b305e5e96ed66baaf97c8861'),
    ('cancel_managed_jobs', (), {
        'job_ids': [1, 2],
        'all_users': True,
        'graceful': True,
        'graceful_timeout': 30,
    }, '8cfb362c5db217363fbd71e9a1f211d99bf1d7ca4ad21f427f0cee3bf68d2afc'),
    ('cancel_managed_jobs', (), {
        'name': 'job name',
        'graceful': True,
        'graceful_timeout': 12,
    }, 'da6f0d0b3d25cd407448dc9ec0cfd5050bee084cbfffe141b9e8a2c44b8d2743'),
    ('cancel_managed_jobs', (), {
        'pool': 'pool A',
    }, 'ec34494a27b122a5b7b25e41afa9d804792eebc50b2ab22348f98c15b46d2cce'),
    ('get_version_and_job_table', (), {},
     '5bd3151698eb24547aaa0f106609fd00fe5361d8919b179a31138df79ab508a5'),
    ('get_version', (), {},
     '0af24ab4e205941bb002d024245b166dedeb69b34d3cd5013f291b6dc2c8d117'),
    ('get_all_job_ids_by_name', ('job name',), {},
     '4dcf2dcf7e6eefc72c47da0a28791e4ec5ad78e2029858da414d9447fd6a5204'),
    ('get_debug_dump_manifest', ([1, 3],), {},
     'bdb39cd9ab6cf66c4919c75685c9b3bb3b1ff671534c7f24262691f42a97f8c6'),
    ('stream_logs', ('job name', None), {
        'follow': False,
        'controller': True,
        'tail': 20,
        'tail_offset': 4,
        'task': 'task A',
    }, '057cb74e4161bedfe9eab37b4f0694831d1c4b62b7b5da61cc7458e71ddf208e'),
]


@pytest.mark.parametrize('method_name,args,kwargs,expected_digest',
                         _CODEGEN_CASES)
def test_managed_job_codegen_output_is_stable(method_name: str,
                                              args: tuple[Any, ...],
                                              kwargs: dict[str, Any],
                                              expected_digest: str) -> None:
    command = getattr(managed_job_utils.ManagedJobCodeGen,
                      method_name)(*args, **kwargs)

    assert hashlib.sha256(command.encode()).hexdigest() == expected_digest
    compile(_extract_generated_python(command), '<managed-job-code>', 'exec')


def test_set_pending_codegen_output_is_stable() -> None:
    dag = SimpleNamespace(
        name='dag',
        pool='pool',
        execution=SimpleNamespace(value='parallel'),
        tasks=[
            SimpleNamespace(name='a', metadata_json='{}'),
            SimpleNamespace(name='b', metadata_json='{"x":1}'),
        ],
        primary_tasks=['a'],
        is_job_group=lambda: True,
    )

    command = managed_job_utils.ManagedJobCodeGen.set_pending(
        7, dag, 'workspace A', 'python train.py', 'user-hash')

    assert hashlib.sha256(command.encode()).hexdigest() == (
        '260a0d7364197543a454015ce59ae3ba3b18a1914b1f8b9165a02fed8b0e1c20')
    compile(_extract_generated_python(command), '<managed-job-code>', 'exec')


def test_cancel_codegen_legacy_graceful_guard_is_embedded() -> None:
    command = managed_job_utils.ManagedJobCodeGen.cancel_managed_jobs(
        name='job name', graceful=True, graceful_timeout=12)
    code = _extract_generated_python(command)

    assert 'if managed_job_version < 19:' in code
    assert 'raise RuntimeError(' in code
    assert '`cancel_managed_jobs` endpoint.' in code
    assert 'Please upgrade the jobs controller and retry.' in code


def test_managed_job_codegen_facade_contract() -> None:
    codegen = managed_job_utils.ManagedJobCodeGen

    assert jobs.ManagedJobCodeGen is codegen
    assert managed_job_codegen.ManagedJobCodeGen is codegen
    assert codegen.__module__ == managed_job_utils.__name__
    assert codegen.__qualname__ == 'ManagedJobCodeGen'
    assert pickle.loads(pickle.dumps(codegen)) is codegen
    assert tuple(inspect.signature(codegen.get_job_table).parameters) == (
        'skip_finished',
        'accessible_workspaces',
        'job_ids',
        'workspace_match',
        'name_match',
        'pool_match',
        'page',
        'limit',
        'user_hashes',
        'statuses',
        'fields',
        'sort_by',
        'sort_order',
        'submitted_after',
        'submitted_before',
    )
