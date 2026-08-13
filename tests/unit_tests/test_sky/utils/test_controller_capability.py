"""Tests for controller-origin capability primitives."""

import json
import os
import pathlib
import subprocess
import sys

import pytest

from sky.utils import controller_capability


@pytest.fixture(autouse=True)
def _clear_process_local_capability():
    controller_capability.clear_process_local()
    try:
        yield
    finally:
        controller_capability.clear_process_local()


def _authority(instance_id: str, generation: int,
               capability: str) -> dict[str, object]:
    pid = os.getpid()
    return {
        'controller_instance_id': instance_id,
        'controller_generation': generation,
        'origin_capability_sha256':
            controller_capability.digest_hex(capability),
        'owner_pid': pid,
        'owner_process_start_time_ticks':
            controller_capability._read_process_start_time_ticks(pid),
    }


def test_capability_is_canonical_and_hash_only_authority_verifies(tmp_path):
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    capability = controller_capability.generate()
    authority_path = tmp_path / 'authority.json'
    authority_path.write_text(json.dumps(_authority(instance_id, 22,
                                                    capability)),
                              encoding='utf-8')
    authority_path.chmod(0o600)

    assert len(capability) == 43
    assert len(controller_capability.digest(capability)) == 32
    assert capability not in authority_path.read_text(encoding='utf-8')
    assert controller_capability.verify_local_authority(str(authority_path),
                                                        instance_id, 22,
                                                        capability)
    assert not controller_capability.verify_local_authority(
        str(authority_path), instance_id, 22, controller_capability.generate())
    assert not controller_capability.verify_local_authority(
        str(authority_path), instance_id, 23, capability)


def test_local_authority_path_is_owner_scoped(tmp_path, monkeypatch):
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))

    assert controller_capability.local_authority_path(instance_id) == str(
        tmp_path / '.sky/locks/managed_job_controller_origin_authority' /
        f'{instance_id}.json')


def test_persisted_origin_uses_hash_only_live_owner_authority(
        tmp_path, monkeypatch):
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    capability = controller_capability.generate()
    monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))
    authority_path = pathlib.Path(
        controller_capability.local_authority_path(instance_id))
    authority_path.parent.mkdir(parents=True, mode=0o700)
    authority_path.write_text(json.dumps(_authority(instance_id, 22,
                                                    capability)),
                              encoding='utf-8')
    authority_path.chmod(0o600)

    assert controller_capability.local_authority_owner_is_current(
        instance_id, 22)
    assert not controller_capability.local_authority_owner_is_current(
        instance_id, 23)
    assert not controller_capability.local_authority_owner_is_current(
        'not-a-uuid', 22)
    assert not controller_capability.local_authority_owner_is_current(
        instance_id, True)

    authority_path.chmod(0o644)
    assert not controller_capability.local_authority_owner_is_current(
        instance_id, 22)


def test_persisted_origin_rejects_symlink_and_reused_pid(tmp_path, monkeypatch):
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    capability = controller_capability.generate()
    monkeypatch.setenv('SKY_RUNTIME_DIR', str(tmp_path))
    authority_path = pathlib.Path(
        controller_capability.local_authority_path(instance_id))
    authority_path.parent.mkdir(parents=True, mode=0o700)
    real_path = authority_path.with_name('real.json')
    real_path.write_text(json.dumps(_authority(instance_id, 22, capability)),
                         encoding='utf-8')
    real_path.chmod(0o600)
    authority_path.symlink_to(real_path)
    assert not controller_capability.local_authority_owner_is_current(
        instance_id, 22)

    authority_path.unlink()
    authority = _authority(instance_id, 22, capability)
    authority['owner_process_start_time_ticks'] = int(
        authority['owner_process_start_time_ticks']) + 1
    authority_path.write_text(json.dumps(authority), encoding='utf-8')
    authority_path.chmod(0o600)
    assert not controller_capability.local_authority_owner_is_current(
        instance_id, 22)


def test_local_authority_rejects_stale_or_overbroad_file(tmp_path):
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    capability = controller_capability.generate()
    authority = _authority(instance_id, 22, capability)
    authority_path = tmp_path / 'authority.json'
    authority_path.write_text(json.dumps(authority), encoding='utf-8')
    authority_path.chmod(0o644)
    assert not controller_capability.verify_local_authority(
        str(authority_path), instance_id, 22, capability)

    authority_path.chmod(0o600)
    authority['owner_process_start_time_ticks'] = int(
        authority['owner_process_start_time_ticks']) + 1
    authority_path.write_text(json.dumps(authority), encoding='utf-8')
    authority_path.chmod(0o600)
    assert not controller_capability.verify_local_authority(
        str(authority_path), instance_id, 22, capability)


def test_local_authority_rejects_overbroad_parent_directory(tmp_path):
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    capability = controller_capability.generate()
    authority_directory = tmp_path / 'authority'
    authority_directory.mkdir(mode=0o700)
    authority_path = authority_directory / 'authority.json'
    authority_path.write_text(json.dumps(_authority(instance_id, 22,
                                                    capability)),
                              encoding='utf-8')
    authority_path.chmod(0o600)
    authority_directory.chmod(0o755)

    assert not controller_capability.verify_local_authority(
        str(authority_path), instance_id, 22, capability)


def test_capability_rejects_noncanonical_encodings():
    for value in ('guess', 'A' * 42, 'A' * 44, '+' * 43, 'A' * 42 + '='):
        try:
            controller_capability.digest(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f'accepted noncanonical capability {value!r}')


def test_process_local_capability_is_bound_to_installing_pid(monkeypatch):
    capability = controller_capability.generate()
    installing_pid = os.getpid()

    controller_capability.install_process_local(capability)
    assert controller_capability.get_process_local() == capability
    controller_capability.install_process_local(capability)

    monkeypatch.setattr(controller_capability.os, 'getpid',
                        lambda: installing_pid + 1)
    assert controller_capability.get_process_local() is None
    with pytest.raises(RuntimeError, match='Another process-local'):
        controller_capability.install_process_local(capability)


def test_capability_fd_is_bounded_consumed_and_closed():
    capability = controller_capability.generate()
    read_fd, write_fd = os.pipe()
    os.write(write_fd, capability.encode('ascii'))
    os.close(write_fd)

    controller_capability.install_process_local_from_fd(read_fd)

    assert controller_capability.get_process_local() == capability
    with pytest.raises(OSError):
        os.fstat(read_fd)


def test_capability_fd_rejects_extra_bytes_and_closes():
    capability = controller_capability.generate()
    read_fd, write_fd = os.pipe()
    os.write(write_fd, capability.encode('ascii') + b'x')
    os.close(write_fd)

    with pytest.raises(ValueError, match='transport payload'):
        controller_capability.install_process_local_from_fd(read_fd)
    with pytest.raises(OSError):
        os.fstat(read_fd)


def test_exec_child_cannot_retrieve_process_local_capability():
    capability = controller_capability.generate()
    controller_capability.install_process_local(capability)
    environment = dict(os.environ)
    for name in ('SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY',
                 'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY_AUTHORITY_PATH',
                 'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD'):
        environment.pop(name, None)
    script = ('import json, os, sys; '
              'from sky.utils import controller_capability; '
              'print(json.dumps({'
              '"capability": controller_capability.get_process_local(), '
              '"environment": dict(os.environ), "argv": sys.argv}))')

    result = subprocess.run([sys.executable, '-c', script],
                            env=environment,
                            capture_output=True,
                            text=True,
                            check=True)
    proof = json.loads(result.stdout.splitlines()[-1])

    assert proof['capability'] is None
    assert capability not in result.stdout
    assert all('CAPABILITY' not in key for key in proof['environment'])
    assert capability not in '\x00'.join(proof['argv'])


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='non-dumpable process protection is Linux-only')
def test_make_process_non_dumpable_in_disposable_child():
    script = ('import ctypes; '
              'from sky.utils import controller_capability; '
              'controller_capability.make_process_non_dumpable(); '
              'print(ctypes.CDLL(None).prctl(3, 0, 0, 0, 0))')

    result = subprocess.run([sys.executable, '-c', script],
                            capture_output=True,
                            text=True,
                            check=True)

    assert result.stdout.splitlines()[-1] == '0'
