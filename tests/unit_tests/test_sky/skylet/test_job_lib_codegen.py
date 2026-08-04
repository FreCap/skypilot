"""Characterization tests for remote job utility command generation."""

import hashlib
import inspect
import pickle
import shlex
from typing import Any
from unittest import mock

import pytest

from sky.skylet import constants
from sky.skylet import job_lib
from sky.skylet import job_lib_codegen


def _extract_generated_python(command: str) -> str:
    prefix = (f'{constants.ACTIVATE_SKY_REMOTE_PYTHON_ENV}; '
              f'{constants.SKY_PYTHON_CMD} -u -c ')
    assert command.startswith(prefix)
    payload = shlex.split(command.removeprefix(prefix))
    assert len(payload) == 1
    return payload[0]


_CODEGEN_CASES = [
    ('add_job', (), {
        'job_name': None,
        'username': 'user hash',
        'run_timestamp': 'run ts',
        'resources_str': 'A100:1',
        'metadata': '{"k":"v"}',
    }, '7722e28217a11c493ab75db34bb3b664a342b89c1ea788976224b8aa5d597469'),
    ('set_job_info_without_job_id', (), {
        'name': 'job',
        'workspace': 'ws',
        'entrypoint': 'python train.py',
        'pool': None,
        'pool_hash': None,
        'user_hash': None,
        'task_ids': [0, 2],
        'task_names': ['a', 'b'],
        'resources_str': 'A100:1',
        'metadata_jsons': ['{}', '{"x":1}'],
        'is_primary_in_job_groups': [None, False],
        'execution': 'cloudvm',
        'num_jobs': 2,
        'is_batch': False,
    }, '73d73747e4e0896371cd15c38e08a90760336309dea548af5280f896da22c7ab'),
    ('set_job_info_without_job_id', (), {
        'name': 'batch',
        'workspace': 'ws',
        'entrypoint': 'run',
        'pool': 'pool',
        'pool_hash': 'ph',
        'user_hash': 'uh',
        'task_ids': [1],
        'task_names': ['task'],
        'resources_str': 'CPU:2',
        'metadata_jsons': ['{}'],
        'is_primary_in_job_groups': [True],
        'execution': 'cloudvm',
        'is_batch': True,
    }, '7f8df1e72f193acf3806d616e2a84763bf1b327e7196f023a1281ff36f53306c'),
    ('queue_job', (7, "echo 'hello'"), {},
     '92cebcc3dbd661e8127eb90461fdac9d6086ecd4eae10814d5f8aa9c2ed10c3a'),
    ('wait_for_job', (7,), {},
     '7a6f7301e0f6cde18881823950629442203e7c2752cc18813d8ffecf28dce1b9'),
    ('update_status', (), {},
     '821371bd860bb145f17bb319fef97c44c4e825847e097cd82772fa5cd3f2fc80'),
    ('get_job_queue', ('user', True), {},
     'abe4dd86e1e6526cf447fb23e3400bfeff65d899403b5f46755f9c3aa4b8e642'),
    ('cancel_jobs', ([1, 2],), {
        'user_hash': 'user'
    }, '02415c56345c6774b454ee3ea5fcbaed8e470217b0af9d3424d6c143c6320f48'),
    ('cancel_jobs', (None,), {
        'cancel_all': True
    }, 'cc073a56b801e9b24ed3e371ee474779fa784d55544e3e4129520f72587ff4b9'),
    ('fail_all_jobs_in_progress', (), {},
     'a17bbd46ee080cb3d173b4e854cd1fd4da21c4db1b137364781d39112debfb4e'),
    ('tail_logs', (3, None), {
        'follow': False,
        'tail': 20,
    }, 'e7ac78510b649dec2dcfd5cbc9c612503ec85c0ff05d30de86d7a797770d183f'),
    ('tail_logs', (None, 9), {
        'follow': True,
        'tail': 4,
        'tail_offset': 12,
    }, 'bb00a3509da2374d407c20c952c64ff31db75a209d1de8dd47d1d28d3c8909e6'),
    ('get_job_status', ([1, 3],), {},
     '30a9e3ecbc901f9eba640b26a5e8faa6e2bfd18eecac829ca00992396977588a'),
    ('get_job_status', (), {},
     '8b6f9826b2e3421826c2e87c322fab20d9fb2e81d50ac6a20f08585eccbbeba3'),
    ('get_job_status_with_system_recovery', ([1, 3],), {},
     '10f2a5c9a154abc712b0c90c99a41d64f0ed1de1946c4b3bd9b6e10106128c94'),
    ('get_job_status_with_system_recovery', (), {},
     '153557bb740605c115ec2902109e252509ca9a9c8d89e7287853096637ed74f2'),
    ('get_job_submitted_or_ended_timestamp_payload', (5,), {},
     '1e680decf0ed8c706245e58441b6c48dcd105d7ce0762bc937914ca3eb73597a'),
    ('get_job_submitted_or_ended_timestamp_payload', (), {
        'get_ended_time': True
    }, 'b73b5311f407209d4b589c9cbe2755986ca42350ac7bfb7a1e1771689186a540'),
    ('get_log_dirs_for_jobs', (['1', '*'],), {},
     '512074e63b2fae3b2e726dedb7f1dd8feab7a08273b377fc29469bd4572b479e'),
    ('get_log_dirs_for_jobs', (None,), {},
     '8da229dce5d26ee0ad7cbcf08e5156b8edcc3224ac49d9b8f2c507f1f28a6a29'),
    ('get_job_exit_codes', (8,), {},
     '3d128e97e81a9ea6b25a4da93c0fe2b8edf04745fad22a2e9234449f38502fda'),
]


@pytest.mark.parametrize('method_name,args,kwargs,expected_digest',
                         _CODEGEN_CASES)
def test_job_lib_codegen_output_is_stable(method_name: str, args: tuple[Any,
                                                                        ...],
                                          kwargs: dict[str, Any],
                                          expected_digest: str) -> None:
    method = getattr(job_lib.JobLibCodeGen, method_name)
    command = method(*args, **kwargs)

    assert hashlib.sha256(command.encode()).hexdigest() == expected_digest
    compile(_extract_generated_python(command), '<generated-job-code>', 'exec')


def test_job_lib_codegen_facade_contract() -> None:
    codegen = job_lib.JobLibCodeGen

    assert codegen is job_lib_codegen.JobLibCodeGen
    assert codegen.__module__ == job_lib.__name__
    assert codegen.__qualname__ == 'JobLibCodeGen'
    assert pickle.loads(pickle.dumps(codegen)) is codegen
    assert tuple(inspect.signature(codegen.tail_logs).parameters) == (
        'job_id',
        'managed_job_id',
        'follow',
        'tail',
        'tail_offset',
    )


def test_recovery_status_codegen_falls_back_for_skewed_job_lib(
        monkeypatch, capsys) -> None:
    monkeypatch.setattr(constants, 'SKYLET_VERSION', '42')
    monkeypatch.setattr(constants, 'SKYLET_LIB_VERSION', 8)
    legacy = mock.Mock(return_value='legacy-status-payload')
    structured = mock.Mock(return_value='structured-status-payload')
    monkeypatch.setattr(job_lib, 'get_statuses_payload', legacy)
    monkeypatch.setattr(job_lib, 'get_statuses_with_system_recovery_payload',
                        structured)
    generated = _extract_generated_python(
        job_lib.JobLibCodeGen.get_job_status_with_system_recovery([7]))

    exec(compile(generated, '<generated-job-code>', 'exec'), {})

    legacy.assert_called_once_with([7])
    structured.assert_not_called()
    assert capsys.readouterr().out.strip() == 'legacy-status-payload'
