# pylint: disable=missing-module-docstring,protected-access,import-outside-toplevel,missing-class-docstring,unused-argument,redefined-outer-name,reimported,confusing-with-statement
import contextlib
import contextvars
import hashlib
import os
import pathlib
import shlex
import subprocess
import sys
import tempfile
import threading
import types
from unittest import mock
import uuid

import pytest
import requests.exceptions as requests_exceptions
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm

from sky import clouds
from sky import exceptions
from sky import global_user_state
from sky import skypilot_config
from sky.resources import Resources
from sky.serve import constants
from sky.serve import controller_transport
from sky.serve import demand_state
from sky.serve import maintenance
from sky.serve import ordinary_launch_binding
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.server.requests import postgres as request_postgres
from sky.server.requests import requests as api_requests
from sky.skylet import constants as skylet_constants
from sky.utils import common_utils
from sky.utils import thread_utils

# String path for mock.patch — can't use the constant directly because
# mock.patch needs the dotted path to the attribute being patched.
_SIGNAL_FILE_CONST = (
    'sky.jobs.constants.JOBS_CONSOLIDATION_RELOADED_SIGNAL_FILE')


@pytest.mark.parametrize(('value', 'expected'),
                         [(None, False), ('false', False), ('true', True)])
def test_serve_controller_hold_requires_explicit_boolean(
        monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR,
                           raising=False)
    else:
        monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, value)

    assert maintenance.is_controller_hold_active() is expected


def test_serve_controller_hold_rejects_malformed_value(monkeypatch):
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'TRUE')

    with pytest.raises(RuntimeError, match='must be exactly'):
        maintenance.is_controller_hold_active()


def test_serve_controller_hold_blocks_ha_recovery_before_state_reads(
        monkeypatch):
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')
    with mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner, \
         mock.patch.object(serve_utils.serve_state,
                           'get_glob_service_names') as get_names:
        serve_utils.ha_recovery_for_consolidation_mode(pool=False)

    runner.assert_not_called()
    get_names.assert_not_called()


def test_serve_controller_hold_blocks_termination_before_state_reads(
        monkeypatch):
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')
    with mock.patch.object(serve_utils.serve_state,
                           'get_glob_service_names') as get_names, \
         pytest.raises(RuntimeError, match='termination and purge'):
        serve_utils.terminate_services(['svc'], purge=True, pool=False)

    get_names.assert_not_called()


def test_serve_controller_hold_does_not_block_pool_termination(monkeypatch):
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')
    with mock.patch.object(serve_utils.serve_state,
                           'get_glob_service_names',
                           return_value=[]):
        message = serve_utils.terminate_services([], purge=False, pool=True)

    assert message == 'No pool to terminate.'


def test_update_config_capability_rejects_old_controller_before_mutation():
    response = mock.Mock(status_code=404)
    with mock.patch.object(serve_utils,
                           '_get_to_controller_with_retry',
                           return_value=response):
        with pytest.raises(RuntimeError, match='does not support atomic'):
            serve_utils.require_update_config_snapshot_capability(
                'svc', 'incarnation-a')


def test_update_config_capability_accepts_matching_protocol():
    response = mock.Mock(status_code=200)
    response.json.return_value = {
        'config_snapshot_protocol_version':
            constants.SERVE_UPDATE_CONFIG_SNAPSHOT_PROTOCOL_VERSION,
    }
    with mock.patch.object(serve_utils,
                           '_get_to_controller_with_retry',
                           return_value=response):
        serve_utils.require_update_config_snapshot_capability(
            'svc', 'incarnation-a')


@pytest.mark.parametrize('malformed_version', [True, 1.0])
def test_update_config_capability_rejects_non_integer_protocol(
        malformed_version):
    response = mock.Mock(status_code=200)
    response.json.return_value = {
        'config_snapshot_protocol_version': malformed_version,
    }
    with mock.patch.object(serve_utils,
                           '_get_to_controller_with_retry',
                           return_value=response), \
         pytest.raises(RuntimeError, match='incompatible'):
        serve_utils.require_update_config_snapshot_capability(
            'svc', 'incarnation-a')


def test_update_config_snapshot_uses_new_endpoint_and_exact_digest():
    digest = 'a' * 64
    snapshot_id = 'c' * 64
    response = mock.Mock(status_code=200)
    response.json.return_value = {
        'message': 'update accepted',
        'config_snapshot_id': snapshot_id,
    }
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={'hash': 'incarnation-a'}), \
         mock.patch.object(serve_utils,
                           '_post_to_controller_with_retry',
                           return_value=response) as post:
        serve_utils.update_service_encoded(
            'svc',
            2,
            'rolling',
            pool=False,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=7,
            has_config_snapshot=True,
            expected_config_snapshot_digest=digest,
            config_snapshot_id=snapshot_id)
    assert post.call_args.args[2] == (
        constants.CONTROLLER_CONFIG_UPDATE_ENDPOINT_PATH)


def test_update_config_snapshot_rejects_stale_snapshot_ack():
    response = mock.Mock(status_code=200)
    response.json.return_value = {
        'message': 'update accepted',
        'config_snapshot_id': 'b' * 64,
    }
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={'hash': 'incarnation-a'}), \
         mock.patch.object(serve_utils,
                           '_post_to_controller_with_retry',
                           return_value=response), \
         pytest.raises(RuntimeError, match='different config snapshot'):
        serve_utils.update_service_encoded(
            'svc',
            2,
            'rolling',
            pool=False,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=7,
            has_config_snapshot=True,
            expected_config_snapshot_digest='a' * 64,
            config_snapshot_id='c' * 64)


def test_secure_staged_controller_config_verifies_digest_and_tightens_mode(
        tmp_path):
    staged = tmp_path / 'config.yaml.staged'
    config_bytes = b'active_workspace: research\n'
    staged.write_bytes(config_bytes)
    staged.chmod(0o644)

    result = serve_utils.secure_staged_controller_config(
        str(staged),
        hashlib.sha256(config_bytes).hexdigest())

    assert result == config_bytes
    assert staged.stat().st_mode & 0o777 == 0o600


def test_secure_staged_controller_config_rejects_digest_mismatch(tmp_path):
    staged = tmp_path / 'config.yaml.staged'
    staged.write_bytes(b'active_workspace: research\n')
    staged.chmod(0o644)

    with pytest.raises(RuntimeError, match='digest does not match'):
        serve_utils.secure_staged_controller_config(str(staged), '0' * 64)

    # Tighten the raw snapshot before parsing or reporting a digest failure.
    assert staged.stat().st_mode & 0o777 == 0o600


def test_secure_staged_controller_config_rejects_symlink(tmp_path):
    target = tmp_path / 'outside.yaml'
    target.write_bytes(b'active_workspace: research\n')
    staged = tmp_path / 'config.yaml.staged'
    staged.symlink_to(target)

    with pytest.raises(RuntimeError, match='not a regular file'):
        serve_utils.secure_staged_controller_config(
            str(staged),
            hashlib.sha256(target.read_bytes()).hexdigest())


@pytest.mark.parametrize('nonregular_kind', ['directory', 'fifo'])
def test_secure_staged_controller_config_rejects_nonregular_without_blocking(
        tmp_path, nonregular_kind):
    staged = tmp_path / 'config.yaml.staged'
    if nonregular_kind == 'directory':
        staged.mkdir()
    else:
        os.mkfifo(staged)

    with pytest.raises(RuntimeError, match='not a regular file'):
        serve_utils.secure_staged_controller_config(str(staged), '0' * 64)


def test_secure_staged_controller_config_rejects_oversize(tmp_path):
    staged = tmp_path / 'config.yaml.staged'
    config_bytes = b'x' * (1024 * 1024 + 1)
    staged.write_bytes(config_bytes)

    with pytest.raises(RuntimeError, match='exceeds the 1MiB limit'):
        serve_utils.secure_staged_controller_config(
            str(staged),
            hashlib.sha256(config_bytes).hexdigest())


def test_orphaned_config_stage_gc_is_nonce_and_commit_safe(
        tmp_path, monkeypatch):
    config_path = tmp_path / 'config.yaml'
    monkeypatch.setattr(serve_utils, 'generate_remote_config_yaml_file_name',
                        lambda *_args, **_kwargs: str(config_path))
    now = 10_000.0
    old_mtime = (now - constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS - 1)
    fresh_mtime = (now - constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS + 1)

    def _write_stage(version, snapshot_id, mtime):
        path = pathlib.Path(
            serve_utils.generate_staged_config_yaml_file_name(
                'svc', version, 'scope-a', snapshot_id=snapshot_id))
        path.write_text('credential: raw\n', encoding='utf-8')
        receipt = pathlib.Path(
            serve_utils.generate_config_snapshot_receipt_file_name(str(path)))
        receipt.write_text('receipt', encoding='utf-8')
        os.utime(path, (mtime, mtime))
        os.utime(receipt, (mtime, mtime))
        return path, receipt

    orphan_a = _write_stage(2, 'a' * 64, old_mtime)
    orphan_b = _write_stage(2, 'b' * 64, old_mtime)
    committed = _write_stage(3, 'c' * 64, old_mtime)
    fresh = _write_stage(4, 'd' * 64, fresh_mtime)
    missing_row = _write_stage(5, 'e' * 64, old_mtime)
    legacy = _write_stage(6, None, old_mtime)
    unrelated = tmp_path / 'config.yaml.v7.not-a-stage'
    unrelated.write_text('preserve', encoding='utf-8')
    monkeypatch.setattr(
        serve_state, 'get_yaml_contents', lambda _service, _versions: {
            2: None,
            3: 'service: committed',
            4: None,
            6: None,
        })

    removed = serve_utils.gc_orphaned_staged_controller_configs('svc',
                                                                'scope-a',
                                                                now=now)

    assert removed == [2, 6]
    for stage, receipt in (orphan_a, orphan_b, legacy):
        assert not stage.exists()
        assert not receipt.exists()
    for stage, receipt in (committed, fresh, missing_row):
        assert stage.exists()
        assert receipt.exists()
    assert unrelated.read_text(encoding='utf-8') == 'preserve'


def test_orphaned_config_stage_gc_preserves_concurrently_refreshed_path(
        tmp_path, monkeypatch):
    config_path = tmp_path / 'config.yaml'
    monkeypatch.setattr(serve_utils, 'generate_remote_config_yaml_file_name',
                        lambda *_args, **_kwargs: str(config_path))
    snapshot_id = 'a' * 64
    stage = pathlib.Path(
        serve_utils.generate_staged_config_yaml_file_name(
            'svc', 2, 'scope-a', snapshot_id=snapshot_id))
    stage.write_text('old raw bytes', encoding='utf-8')
    now = 10_000.0
    old_mtime = (now - constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS - 1)
    os.utime(stage, (old_mtime, old_mtime))

    def _refresh_during_db_read(_service, _versions):
        stage.write_text('new request bytes', encoding='utf-8')
        return {2: None}

    monkeypatch.setattr(serve_state, 'get_yaml_contents',
                        _refresh_during_db_read)

    removed = serve_utils.gc_orphaned_staged_controller_configs('svc',
                                                                'scope-a',
                                                                now=now)

    assert removed == []
    assert stage.read_text(encoding='utf-8') == 'new request bytes'


def test_orphaned_config_stage_gc_preserves_on_database_error(
        tmp_path, monkeypatch):
    config_path = tmp_path / 'config.yaml'
    monkeypatch.setattr(serve_utils, 'generate_remote_config_yaml_file_name',
                        lambda *_args, **_kwargs: str(config_path))
    stage = pathlib.Path(
        serve_utils.generate_staged_config_yaml_file_name('svc',
                                                          2,
                                                          'scope-a',
                                                          snapshot_id='a' * 64))
    stage.write_text('credential: raw\n', encoding='utf-8')
    now = 10_000.0
    old_mtime = (now - constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS - 1)
    os.utime(stage, (old_mtime, old_mtime))
    monkeypatch.setattr(serve_state, 'get_yaml_contents',
                        mock.Mock(side_effect=RuntimeError('database down')))

    with pytest.raises(RuntimeError, match='database down'):
        serve_utils.gc_orphaned_staged_controller_configs('svc',
                                                          'scope-a',
                                                          now=now)

    assert stage.exists()


def test_orphaned_receipt_temp_gc_cleans_crashed_writer_only_after_age_gate(
        tmp_path, monkeypatch):
    config_path = tmp_path / 'config.yaml'
    monkeypatch.setattr(serve_utils, 'generate_remote_config_yaml_file_name',
                        lambda *_args, **_kwargs: str(config_path))
    old_receipt_temp = tmp_path / ('.config-receipt-' + 'a' * 32 + '.tmp')
    fresh_receipt_temp = tmp_path / ('.config-receipt-' + 'b' * 32 + '.tmp')
    old_receipt_temp.write_bytes(b'{"source_digest":"offline-verifier"}')
    fresh_receipt_temp.write_bytes(b'{"source_digest":"in-flight"}')
    malformed_neighbor = tmp_path / '.config-receipt-not-ours'
    malformed_neighbor.write_text('preserve', encoding='utf-8')
    now = 10_000.0
    old_mtime = now - constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS - 1
    fresh_mtime = now - constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS + 1
    os.utime(old_receipt_temp, (old_mtime, old_mtime))
    os.utime(fresh_receipt_temp, (fresh_mtime, fresh_mtime))
    get_yaml_contents = mock.Mock(side_effect=AssertionError(
        'receipt temporaries do not require a database lookup'))
    monkeypatch.setattr(serve_state, 'get_yaml_contents', get_yaml_contents)

    removed = serve_utils.gc_orphaned_staged_controller_configs('svc',
                                                                'scope-a',
                                                                now=now)

    assert removed == []
    assert not old_receipt_temp.exists()
    assert fresh_receipt_temp.exists()
    assert malformed_neighbor.read_text(encoding='utf-8') == 'preserve'
    get_yaml_contents.assert_not_called()


def test_orphaned_receipt_temp_gc_preserves_concurrently_refreshed_path(
        tmp_path, monkeypatch):
    config_path = tmp_path / 'config.yaml'
    monkeypatch.setattr(serve_utils, 'generate_remote_config_yaml_file_name',
                        lambda *_args, **_kwargs: str(config_path))
    receipt_temp = tmp_path / ('.config-receipt-' + 'c' * 32 + '.tmp')
    receipt_temp.write_text('old verifier', encoding='utf-8')
    now = 10_000.0
    old_mtime = now - constants.ORPHANED_CONFIG_STAGE_MIN_AGE_SECONDS - 1
    os.utime(receipt_temp, (old_mtime, old_mtime))
    real_stat = os.stat
    refreshed = False

    def _refresh_before_recheck(path, *, follow_symlinks=True):
        nonlocal refreshed
        if os.fspath(path) == str(receipt_temp) and not refreshed:
            refreshed = True
            receipt_temp.write_text('new in-flight verifier', encoding='utf-8')
        return real_stat(path, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(os, 'stat', _refresh_before_recheck)

    removed = serve_utils.gc_orphaned_staged_controller_configs('svc',
                                                                'scope-a',
                                                                now=now)

    assert removed == []
    assert receipt_temp.read_text(encoding='utf-8') == 'new in-flight verifier'


def test_version_controller_config_requires_custom_workspace_definition():
    config_bytes = (b'active_workspace: research\n'
                    b'workspaces: {}\n'
                    b'kubernetes: {allowed_contexts: [east, phx]}\n')

    with pytest.raises(RuntimeError,
                       match="does not define durable workspace 'research'"):
        serve_utils.parse_and_validate_version_controller_config(
            config_bytes, 'research', 'deleted workspace test')


def test_version_controller_config_allows_implicit_default_workspace():
    parsed = serve_utils.parse_and_validate_version_controller_config(
        b'active_workspace: default\n', 'default', 'default workspace test')

    assert parsed.get_nested(('active_workspace',), None) == 'default'


@pytest.mark.parametrize('launch', [
    'python \\\n -u -m sky.serve.service --service-name svc',
    '/usr/bin/python3.11 -u -m sky.serve.service --service-name svc',
    (f'{skylet_constants.SKY_PYTHON_CMD} \\\n'
     ' -u -m sky.serve.service \\\n --service-name svc'),
])
def test_ha_recovery_controller_launch_locator_accepts_generated_grammar(
        launch):
    lines = ['# unrelated setup', *launch.splitlines()]

    launch_index = (
        serve_utils._find_ha_recovery_controller_launch_index(lines))

    assert launch_index == 1


def test_ha_recovery_controller_launch_locator_fails_closed_on_duplicates():
    lines = [
        'python -u -m sky.serve.service --service-name first',
        '/usr/bin/python3 -m sky.serve.service --service-name second',
    ]

    with pytest.raises(ValueError, match='exactly one generated'):
        serve_utils._find_ha_recovery_controller_launch_index(lines)


@pytest.mark.parametrize('invalid_launch', [
    'python -c "print(1)" -m sky.serve.service',
    'python -u -m sky.serve.service \\',
    ('python -m sky.serve.service ; '
     'python -m sky.serve.service'),
])
def test_ha_recovery_controller_launch_locator_rejects_nonlaunch_grammar(
        invalid_launch):
    with pytest.raises(ValueError, match='exactly one generated'):
        serve_utils._find_ha_recovery_controller_launch_index(
            invalid_launch.splitlines())


@pytest.mark.parametrize('retained_receipt', ['missing', 'stale'])
def test_ha_recovery_config_snapshot_receipt_is_jit_bound(retained_receipt):
    config_path = '/tmp/python-config.yaml'
    config_bytes = b'active_workspace: research\n'
    script = serve_utils.strip_legacy_ha_recovery_config_payload(
        '# misleading python -m sky.serve.service comment\n'
        "export NOTE='misleading python -m sky.serve.service export'\n"
        'python \\\n -u -m sky.serve.service --service-name svc\n', config_path)
    if retained_receipt == 'stale':
        stale_receipt = (skypilot_config.internal_config_snapshot_environment(
            skypilot_config.INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE,
            '/tmp/old.yaml', b'active_workspace: stale\n'))
        script = '\n'.join(
            f'export {name}={shlex.quote(value)}'
            for name, value in stale_receipt.items()) + '\n' + script

    bound = serve_utils.bind_ha_recovery_config_snapshot_receipt(
        script, config_path=config_path, config_bytes=config_bytes)
    bound = serve_utils.bind_ha_recovery_owner_fence(
        bound,
        service_hash='incarnation-a',
        lifecycle_epoch=8,
        controller_pid=None,
        controller_ip=None,
        status=serve_state.ServiceStatus.CONTROLLER_FAILED,
        recovery_version=7)

    expected = skypilot_config.internal_config_snapshot_environment(
        skypilot_config.INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE, config_path,
        config_bytes)
    lines = bound.splitlines()
    launch_index = lines.index('python \\')
    marker_index = lines.index(serve_utils._VERSIONED_HA_CONFIG_MARKER)
    assert lines[marker_index + 1] == (f'export SKYPILOT_CONFIG={config_path}')
    expected_exports = [
        f'export {name}={shlex.quote(value)}'
        for name, value in expected.items()
    ]
    owner_prefix = f'export {constants.HA_RECOVERY_OWNER_FENCE_ENV_VAR}='
    assert lines[launch_index - 1].startswith(owner_prefix)
    assert lines[launch_index - len(expected_exports) - 1:launch_index -
                 1] == expected_exports
    assert all(
        sum(line.startswith(f'export {name}=')
            for line in lines) == 1
        for name in expected)
    assert lines[0].startswith('# misleading python')
    assert lines[1].startswith('export NOTE=')


def test_bound_ha_recovery_script_admits_fresh_guarded_child(tmp_path):
    config_path = tmp_path / 'python-config.yaml'
    config_bytes = b'active_workspace: default\n'
    config_path.write_bytes(config_bytes)
    child_code = (
        'from sky import skypilot_config as config; '
        'assert config._guarded_ha_scoped_child_snapshot() == "serve"; '
        'assert config.get_active_workspace() == "default"; '
        'print("guarded-child-import-ok")')
    python_wrapper = tmp_path / 'python'
    python_wrapper.write_text(
        '#!/bin/sh\n'
        f'exec {shlex.quote(sys.executable)} -c '
        f'{shlex.quote(child_code)}\n',
        encoding='utf-8')
    python_wrapper.chmod(0o700)
    script = (f'export {skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER}=1\n'
              f'export {skypilot_config.ENV_VAR_SERVER_CONFIG_MODE}='
              f'{skypilot_config.SERVER_CONFIG_MODE_POSTGRES}\n'
              f'export {skylet_constants.IS_SKYPILOT_SERVE_CONTROLLER}=true\n'
              '# misleading python -m sky.serve.service comment\n'
              "export NOTE='misleading python -m sky.serve.service export'\n"
              f'{shlex.quote(str(python_wrapper))} -u '
              '-m sky.serve.service --service-name harmless-test\n')
    script = serve_utils.strip_legacy_ha_recovery_config_payload(
        script, str(config_path))
    script = serve_utils.bind_ha_recovery_config_snapshot_receipt(
        script, config_path=str(config_path), config_bytes=config_bytes)

    child_env = dict(os.environ)
    child_env['PYTHONPATH'] = os.pathsep.join(
        path for path in (os.getcwd(), child_env.get('PYTHONPATH')) if path)
    completed = subprocess.run(['/bin/bash', '-c', script],
                               cwd=os.getcwd(),
                               env=child_env,
                               check=False,
                               capture_output=True,
                               text=True,
                               timeout=30)

    assert completed.returncode == 0, completed.stderr
    assert 'guarded-child-import-ok' in completed.stdout


def test_ha_recovery_owner_fence_is_inserted_immediately_before_launch():
    script = ('export EXISTING=value\n'
              'python \\\n'
              ' -u -m sky.serve.service --service-name svc\n')

    bound = serve_utils.bind_ha_recovery_owner_fence(
        script,
        service_hash='incarnation-a',
        lifecycle_epoch=8,
        controller_pid=123,
        controller_ip='10.4.0.1',
        status=serve_state.ServiceStatus.CONTROLLER_FAILED,
        recovery_version=7)

    lines = bound.splitlines()
    launch_index = lines.index('python \\')
    fence_line = lines[launch_index - 1]
    prefix = f'export {constants.HA_RECOVERY_OWNER_FENCE_ENV_VAR}='
    assert fence_line.startswith(prefix)
    encoded = shlex.split(fence_line[len('export '):].split('=', 1)[1])[0]
    assert serve_utils.parse_ha_recovery_owner_fence(encoded) == {
        'service_hash': 'incarnation-a',
        'lifecycle_epoch': 8,
        'controller_pid': 123,
        'controller_ip': '10.4.0.1',
        'status': serve_state.ServiceStatus.CONTROLLER_FAILED,
        'recovery_version': 7,
    }


def test_ha_recovery_owner_fence_rejects_partial_or_malformed_payload():
    with pytest.raises(ValueError, match='invalid schema'):
        serve_utils.parse_ha_recovery_owner_fence('{}')
    with pytest.raises(ValueError, match='invalid version'):
        serve_utils.parse_ha_recovery_owner_fence(
            '{"service_hash":"i","lifecycle_epoch":1,'
            '"controller_pid":null,"controller_ip":null,'
            '"status":"CONTROLLER_FAILED","recovery_version":true}')


@pytest.mark.parametrize('forged_location', ['staged', 'live'])
def test_restore_uses_exact_db_bytes_over_forged_local_files(
        tmp_path, forged_location):
    live_path = str(tmp_path / 'config.yaml')
    staged_path = str(tmp_path / 'config.yaml.v2.staged')
    snapshot_id = 'c' * 64
    durable = (b'active_workspace: research\n'
               b'workspaces: {research: {}}\n'
               b'kubernetes: {allowed_contexts: [east, phx]}\n')
    forged = (b'active_workspace: research\n'
              b'workspaces: {research: {}}\n'
              b'kubernetes: {allowed_contexts: [attacker]}\n')
    pathlib.Path(live_path).write_bytes(forged if forged_location ==
                                        'live' else b'stale live config\n')
    pathlib.Path(staged_path).write_bytes(
        forged if forged_location == 'staged' else b'stale staged config\n')
    live_receipt = pathlib.Path(
        serve_utils.generate_config_snapshot_receipt_file_name(live_path))
    staged_receipt = pathlib.Path(
        serve_utils.generate_config_snapshot_receipt_file_name(staged_path))
    live_receipt.write_text('forged live receipt', encoding='utf-8')
    staged_receipt.write_text('forged staged receipt', encoding='utf-8')

    snapshot = (durable, hashlib.sha256(durable).hexdigest(), snapshot_id)
    with mock.patch.object(serve_state,
                           'get_version_controller_config',
                           return_value=snapshot) as get_snapshot:
        restored = serve_utils.restore_version_controller_config(
            'svc', 2, live_path, staged_path)

    get_snapshot.assert_called_once_with('svc', 2)
    assert restored == durable
    assert pathlib.Path(live_path).read_bytes() == durable
    assert pathlib.Path(live_path).stat().st_mode & 0o777 == 0o600
    assert not pathlib.Path(staged_path).exists()
    assert not live_receipt.exists()
    assert not staged_receipt.exists()
    assert b'attacker' not in pathlib.Path(live_path).read_bytes()


def test_recovery_scrubs_raw_live_configs_but_preserves_stages(
        tmp_path, monkeypatch):
    base_path = tmp_path / 'config.yaml'
    monkeypatch.setattr(serve_utils, 'generate_remote_config_yaml_file_name',
                        lambda *_args, **_kwargs: str(base_path))
    preserved_safe = tmp_path / 'config.yaml.v2'
    paths = {
        'config.yaml': b'credential: initial-secret\n',
        'config.yaml.receipt': b'initial-source-digest',
        'config.yaml.v1': b'credential: old-secret\n',
        'config.yaml.v1.receipt': b'old-source-digest',
        'config.yaml.v2': b'active_workspace: research\n',
        'config.yaml.v2.receipt': b'current-source-digest',
        'config.yaml.v3': b'credential: newer-secret\n',
        'config.yaml.v3.receipt': b'newer-source-digest',
        'config.yaml.v4.' + 'a' * 64 + '.staged': b'fresh-request',
        'config.yaml.v4.' + 'a' * 64 + '.staged.receipt': b'fresh-receipt',
        'config.yaml.v7.not-a-stage': b'unrelated',
        '.config-receipt-' + 'b' * 32 + '.tmp': b'orphaned-source-digest',
        '.config-receipt-not-ours': b'unrelated-dotfile',
    }
    for name, content in paths.items():
        (tmp_path / name).write_bytes(content)

    removed = serve_utils.scrub_obsolete_controller_config_files(
        'svc', 2, 'scope-a')

    assert removed == sorted([
        'config.yaml',
        'config.yaml.receipt',
        'config.yaml.v1',
        'config.yaml.v1.receipt',
        'config.yaml.v2.receipt',
        'config.yaml.v3',
        'config.yaml.v3.receipt',
        '.config-receipt-' + 'b' * 32 + '.tmp',
    ])
    assert preserved_safe.read_bytes() == b'active_workspace: research\n'
    assert (tmp_path / ('config.yaml.v4.' + 'a' * 64 + '.staged')
           ).read_bytes() == b'fresh-request'
    assert (tmp_path / ('config.yaml.v4.' + 'a' * 64 + '.staged.receipt')
           ).read_bytes() == b'fresh-receipt'
    assert (tmp_path /
            'config.yaml.v7.not-a-stage').read_bytes() == b'unrelated'
    assert (tmp_path /
            '.config-receipt-not-ours').read_bytes() == (b'unrelated-dotfile')


def test_full_pod_ha_restores_versioned_db_config_before_runner(tmp_path):
    service_dir = tmp_path / 'service-dir'
    live_path = tmp_path / 'python-config.yaml'
    staged_path = tmp_path / 'config.yaml.v7.staged'
    live_path.write_text('active_workspace: stale\n', encoding='utf-8')
    durable = (b'active_workspace: research\n'
               b'workspaces: {research: {}}\n'
               b'kubernetes: {allowed_contexts: [east, phx]}\n')
    durable_digest = hashlib.sha256(durable).hexdigest()
    recovery_script = serve_utils.strip_legacy_ha_recovery_config_payload(
        '# misleading python -m sky.serve.service comment\n'
        "export NOTE='misleading python -m sky.serve.service export'\n"
        'export SKYPILOT_CONFIG=/old/config.yaml\n'
        '/usr/bin/python -u -m sky.serve.service --service-name svc\n',
        '/old/config.yaml')
    receipt_names = (
        skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_KIND,
        skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_PATH,
        skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_DIGEST,
        skypilot_config.ENV_VAR_INTERNAL_CONFIG_SNAPSHOT_IDENTITY,
    )
    assert all(
        f'export {name}=' not in recovery_script for name in receipt_names)
    record = {
        'name': 'svc',
        'hash': 'incarnation-a',
        'lifecycle_epoch': 8,
        'workspace': 'research',
        'resource_scope': 'scope-a',
        'controller_pid': None,
        'controller_ip': None,
        'status': serve_state.ServiceStatus.CONTROLLER_FAILED,
        'yaml_content': 'service: {}',
        'recovery_version': 7,
        'config_protocol_active': True,
    }
    config_snapshot = (durable, durable_digest, 'c' * 64)
    recovery_snapshot = {
        **record,
        'service_name': 'svc',
        'controller_config_snapshot': config_snapshot,
        'ha_recovery_script': recovery_script,
    }
    runner = mock.Mock()
    child_reached_guarded_config = False

    def _run(script, require_outputs):
        nonlocal child_reached_guarded_config
        assert require_outputs is True
        assert live_path.read_bytes() == durable
        assert live_path.stat().st_mode & 0o777 == 0o600
        assert f'export SKYPILOT_CONFIG={live_path}' in script
        assert 'base64 -d' not in script
        lines = script.splitlines()
        launch_index = lines.index(
            '/usr/bin/python -u -m sky.serve.service --service-name svc')
        marker_index = lines.index(serve_utils._VERSIONED_HA_CONFIG_MARKER)
        assert lines[marker_index +
                     1] == (f'export SKYPILOT_CONFIG={live_path}')
        launch_environment = {}
        for line in lines[:launch_index]:
            if not line.startswith('export '):
                continue
            tokens = shlex.split(line)
            if len(tokens) != 2 or '=' not in tokens[1]:
                continue
            name, value = tokens[1].split('=', 1)
            launch_environment[name] = value
        expected_receipt = (
            skypilot_config.internal_config_snapshot_environment(
                skypilot_config.INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE,
                str(live_path), durable))
        assert all(launch_environment[name] == value
                   for name, value in expected_receipt.items())
        assert all(
            sum(line.startswith(f'export {name}=')
                for line in lines) == 1
            for name in receipt_names)
        assert lines[launch_index - 1].startswith(
            f'export {constants.HA_RECOVERY_OWNER_FENCE_ENV_VAR}=')

        # This is the import-time boundary that kept a recovered
        # SHUTTING_DOWN controller from reaching its existing teardown/ACK
        # path.  Validate both receipt scope and the restored bytes using the
        # exact environment passed to the child.
        guarded_environment = {
            **launch_environment,
            skypilot_config.ENV_VAR_SERVER_CONFIG_MODE:
                skypilot_config.SERVER_CONFIG_MODE_POSTGRES,
            skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER: '1',
            skylet_constants.IS_SKYPILOT_SERVE_CONTROLLER: 'true',
        }
        with mock.patch.dict(os.environ, guarded_environment, clear=True):
            snapshot_kind = (
                skypilot_config._guarded_ha_scoped_child_snapshot())
            assert snapshot_kind == (
                skypilot_config.INTERNAL_CONFIG_SNAPSHOT_KIND_SERVE)
            contextvars.Context().run(
                skypilot_config._reload_config_from_guarded_child_snapshot,
                launch_environment[skypilot_config.ENV_VAR_SKYPILOT_CONFIG],
                snapshot_kind)
        child_reached_guarded_config = True
        fence_prefix = (f'export {constants.HA_RECOVERY_OWNER_FENCE_ENV_VAR}=')
        fence_line = next(line for line in script.splitlines()
                          if line.startswith(fence_prefix))
        encoded_fence = shlex.split(fence_line[len('export '):].split('=',
                                                                      1)[1])[0]
        assert serve_utils.parse_ha_recovery_owner_fence(
            encoded_fence)['recovery_version'] == 7
        return 0, '', ''

    runner.run.side_effect = _run
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[record]), \
         mock.patch.object(serve_state,
                           'get_latest_committed_versions',
                           return_value={}), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hashes',
                           return_value={}), \
         mock.patch.object(serve_state,
                           'get_ha_recovery_script',
                           return_value='legacy lookup must not run') as legacy, \
         mock.patch.object(
             serve_state,
             'get_service_ha_recovery_snapshot',
             return_value=recovery_snapshot) as authorize, \
         mock.patch.object(serve_state,
                           'get_recovery_version_spec') as recovery_lookup, \
         mock.patch.object(
             serve_state,
             'get_version_controller_config') as get_snapshot, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(serve_utils,
                           'generate_remote_service_dir_name',
                           return_value=str(service_dir)), \
         mock.patch.object(serve_utils,
                           'generate_versioned_config_yaml_file_name',
                           return_value=str(live_path)), \
         mock.patch.object(serve_utils,
                           'generate_staged_config_yaml_file_name',
                           return_value=str(staged_path)), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner',
                           return_value=runner), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')):
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    runner.run.assert_called_once()
    assert child_reached_guarded_config
    authorize.assert_called_once_with('svc',
                                      expected_service_hash='incarnation-a')
    legacy.assert_not_called()
    recovery_lookup.assert_not_called()
    get_snapshot.assert_not_called()


def test_full_pod_ha_refuses_corrupt_versioned_db_config(tmp_path):
    live_path = tmp_path / 'config.yaml'
    record = {
        'name': 'svc',
        'hash': 'incarnation-a',
        'lifecycle_epoch': 8,
        'workspace': 'research',
        'resource_scope': 'scope-a',
        'controller_pid': None,
        'controller_ip': None,
        'status': serve_state.ServiceStatus.CONTROLLER_FAILED,
        'yaml_content': 'service: {}',
        'recovery_version': 7,
        'config_protocol_active': True,
    }
    corrupt = b'active_workspace: attacker\nworkspaces: {attacker: {}}\n'
    recovery_script = serve_utils.strip_legacy_ha_recovery_config_payload(
        'export SKYPILOT_CONFIG=/old/config.yaml\n'
        '/usr/bin/python -u -m sky.serve.service --service-name svc\n',
        '/old/config.yaml')
    recovery_snapshot = {
        **record,
        'service_name': 'svc',
        'controller_config_snapshot':
            (corrupt, hashlib.sha256(corrupt).hexdigest(), 'c' * 64),
        'ha_recovery_script': recovery_script,
    }
    runner = mock.Mock()
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[record]), \
         mock.patch.object(serve_state,
                           'get_latest_committed_versions',
                           return_value={}), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hashes',
                           return_value={}), \
         mock.patch.object(
             serve_state,
             'get_service_ha_recovery_snapshot',
             return_value=recovery_snapshot) as authorize, \
         mock.patch.object(serve_state,
                           'get_version_controller_config') as get_snapshot, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(serve_utils,
                           'generate_remote_service_dir_name',
                           return_value=str(tmp_path / 'service-dir')), \
         mock.patch.object(serve_utils,
                           'generate_versioned_config_yaml_file_name',
                           return_value=str(live_path)), \
         mock.patch.object(serve_utils,
                           'generate_staged_config_yaml_file_name',
                           return_value=str(tmp_path / 'config.staged')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner',
                           return_value=runner), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')):
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    runner.run.assert_not_called()
    authorize.assert_called_once_with('svc',
                                      expected_service_hash='incarnation-a')
    get_snapshot.assert_not_called()
    assert not live_path.exists()
    assert "belongs to workspace 'attacker', expected 'research'" in (
        tmp_path / 'recovery_pool_.log').read_text(encoding='utf-8')


def test_full_pod_ha_refuses_protocol_row_without_selected_config(tmp_path):
    record = {
        'name': 'svc',
        'hash': 'incarnation-a',
        'lifecycle_epoch': 8,
        'workspace': 'research',
        'resource_scope': 'scope-a',
        'controller_pid': None,
        'controller_ip': None,
        'status': serve_state.ServiceStatus.CONTROLLER_FAILED,
        'yaml_content': 'service: {}',
        'recovery_version': 7,
        'config_protocol_active': True,
    }
    recovery_snapshot = {
        **record,
        'service_name': 'svc',
        'controller_config_snapshot': None,
        'ha_recovery_script': 'recover',
    }
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[record]), \
         mock.patch.object(
             serve_state,
             'get_service_ha_recovery_snapshot',
             return_value=recovery_snapshot) as authorize, \
         mock.patch.object(serve_state,
                           'get_version_controller_config') as get_snapshot, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(serve_utils,
                           'generate_remote_service_dir_name',
                           return_value=str(tmp_path / 'service-dir')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls, \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')):
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    get_snapshot.assert_not_called()
    authorize.assert_called_once_with('svc',
                                      expected_service_hash='incarnation-a')
    runner_cls.return_value.run.assert_not_called()
    assert 'has no complete controller config snapshot' in (
        tmp_path / 'recovery_pool_.log').read_text(encoding='utf-8')


def test_guarded_full_pod_ha_never_uses_legacy_recovery_script(
        tmp_path, monkeypatch):
    record = {
        'name': 'svc',
        'hash': 'incarnation-a',
        'lifecycle_epoch': 8,
        'workspace': 'research',
        'resource_scope': 'scope-a',
        'controller_pid': None,
        'controller_ip': None,
        'status': serve_state.ServiceStatus.CONTROLLER_FAILED,
        'yaml_content': 'service: {}',
    }
    monkeypatch.setenv('SKYPILOT_API_REQUEST_BACKEND', 'postgres')
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'controller')
    monkeypatch.setenv('SKYPILOT_API_SERVER_STORAGE_ENABLED', 'false')
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[record]), \
         mock.patch.object(
             serve_state,
             'get_ha_recovery_script',
             side_effect=AssertionError('legacy script lookup')) as legacy, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(
             serve_utils,
             'generate_remote_service_dir_name',
             side_effect=AssertionError('predecessor directory derived')) \
             as service_dir, \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls, \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')):
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    legacy.assert_not_called()
    service_dir.assert_not_called()
    runner_cls.return_value.run.assert_not_called()
    assert 'no PostgreSQL recovery-protocol marker' in (
        tmp_path / 'recovery_pool_.log').read_text(encoding='utf-8')


def test_full_pod_ha_skips_owner_changed_after_liveness_sweep(tmp_path):
    record = {
        'name': 'svc',
        'hash': 'incarnation-a',
        'lifecycle_epoch': 8,
        'workspace': 'research',
        'resource_scope': 'scope-a',
        'controller_pid': None,
        'controller_ip': None,
        'status': serve_state.ServiceStatus.CONTROLLER_FAILED,
        'yaml_content': 'service: {}',
        'recovery_version': 7,
        'config_protocol_active': True,
    }
    current_snapshot = {
        **record,
        'service_name': 'svc',
        'controller_pid': 9876,
        'controller_ip': '10.4.0.8',
        'controller_config_snapshot': None,
        'ha_recovery_script': 'must not run',
    }
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[record]), \
         mock.patch.object(
             serve_state,
             'get_service_ha_recovery_snapshot',
             return_value=current_snapshot) as authorize, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(serve_utils,
                           'generate_remote_service_dir_name') as service_dir, \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls, \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')):
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    authorize.assert_called_once_with('svc',
                                      expected_service_hash='incarnation-a')
    service_dir.assert_not_called()
    runner_cls.return_value.run.assert_not_called()
    assert 'changed recovery owner metadata (controller_pid, controller_ip)' in (
        tmp_path / 'recovery_pool_.log').read_text(encoding='utf-8')


def test_cleanup_staged_config_update_uses_nonce_and_lifecycle_fence():
    response = mock.Mock(status_code=200)
    response.json.return_value = {'removed': True}
    with mock.patch.object(serve_utils,
                           '_post_to_controller_with_retry',
                           return_value=response) as post:
        removed = serve_utils.cleanup_staged_config_update_encoded(
            'svc', 'incarnation-a', 2, 7, 'c' * 64)

    assert removed
    assert post.call_args.args[2] == (
        constants.CONTROLLER_CONFIG_CLEANUP_ENDPOINT_PATH)
    assert post.call_args.kwargs['json'] == {
        'version': 2,
        'expected_lifecycle_epoch': 7,
        'config_snapshot_id': 'c' * 64,
    }


def test_cleanup_staged_config_update_fails_closed_on_controller_error():
    response = mock.Mock(status_code=409, text='version already committed')
    with mock.patch.object(serve_utils,
                           '_post_to_controller_with_retry',
                           return_value=response), \
         pytest.raises(RuntimeError, match='could not safely clean'):
        serve_utils.cleanup_staged_config_update_encoded(
            'svc', 'incarnation-a', 2, 7, 'c' * 64)


def test_cleanup_history_mode_is_strong_for_v2_and_malformed_rows():
    info = mock.Mock()
    with mock.patch(
            'sky.serve.reserved_capacity.parse_protocol_v2_cleanup_fence',
            return_value=None):
        assert not serve_utils.replica_cleanup_requires_terminal_history([info])
    with mock.patch(
            'sky.serve.reserved_capacity.parse_protocol_v2_cleanup_fence',
            return_value=types.SimpleNamespace()):
        assert serve_utils.replica_cleanup_requires_terminal_history([info])
    with mock.patch(
            'sky.serve.reserved_capacity.parse_protocol_v2_cleanup_fence',
            side_effect=exceptions.KubernetesPhysicalClusterIdentityError(
                'partial v2 state')):
        assert serve_utils.replica_cleanup_requires_terminal_history([info])


@contextlib.contextmanager
def _count_sql_statements(engine):
    counts = {'n': 0}

    def _count(*args, **kwargs):
        del args, kwargs
        counts['n'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _count)
    try:
        yield counts
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', _count)


@pytest.fixture
def _mock_serve_db(tmp_path, monkeypatch):
    """Point serve_state at a fresh sqlite DB for the duration of one test."""
    db_path = tmp_path / 'serve_utils_testing.db'
    engine = create_engine(f'sqlite:///{db_path}')

    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    serve_state.Base.metadata.create_all(engine)
    yield engine


def _insert_version_spec(engine, service_name: str, version: int,
                         min_replicas: int) -> None:
    spec = types.SimpleNamespace(min_replicas=min_replicas)
    with orm.Session(engine) as session:
        session.execute(serve_state.version_specs_table.insert().values(
            service_name=service_name,
            version=version,
            spec=serve_state.pickle.dumps(spec),
            yaml_content=f'yaml: v{version}',
        ))
        session.commit()


def _insert_orphan_service_row(engine, name: str) -> None:
    """Insert a services row with no version row."""
    with orm.Session(engine) as session:
        session.execute(serve_state.services_table.insert().values(
            name=name,
            controller_job_id=1,
            status=serve_state.ServiceStatus.CONTROLLER_INIT.value,
            requested_resources_str='1x[CPU:1+]',
            pool=0,
            controller_pid=12345,
            hash='orphan',
            entrypoint='entry'))
        session.commit()


def test_lifecycle_lock_detection_failure_is_fail_closed():
    with mock.patch.object(serve_utils.global_user_state,
                           'initialize_and_get_db',
                           side_effect=RuntimeError('postgres unavailable')), \
         mock.patch.object(serve_utils.locks, 'get_lock') as get_lock:
        with pytest.raises(RuntimeError, match='postgres unavailable'):
            serve_utils.get_service_lifecycle_lock('svc')
    get_lock.assert_not_called()


def test_resolve_legacy_service_workspace_from_replica_evidence():
    replicas = [
        types.SimpleNamespace(cluster_name='svc-r1'),
        types.SimpleNamespace(cluster_name='svc-r2'),
    ]
    cluster_records = {
        'svc-r1': {
            'workspace': 'research'
        },
        'svc-r2': {
            'workspace': 'research'
        },
    }
    with mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=replicas), \
         mock.patch.object(serve_utils.global_user_state,
                           'get_clusters_from_names',
                           return_value=cluster_records) as get_clusters, \
         mock.patch.object(serve_state,
                           'set_service_workspace_if_owner',
                           return_value=True) as set_workspace:
        workspace = serve_utils.resolve_service_workspace(
            'svc', {
                'workspace': None,
                'hash': 'incarnation-a'
            }, 'research')

    assert workspace == 'research'
    get_clusters.assert_called_once_with(['svc-r1', 'svc-r2'])
    set_workspace.assert_called_once_with('svc', 'research', 'incarnation-a')


def test_resolve_legacy_service_workspace_rejects_conflicting_evidence():
    replicas = [
        types.SimpleNamespace(cluster_name='svc-r1'),
        types.SimpleNamespace(cluster_name='svc-r2'),
    ]
    with mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=replicas), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_clusters_from_names',
             return_value={
                 'svc-r1': {
                     'workspace': 'research'
                 },
                 'svc-r2': {
                     'workspace': 'production'
                 },
             }), \
         mock.patch.object(serve_state,
                           'set_service_workspace_if_owner') as set_workspace, \
         pytest.raises(RuntimeError, match='multiple workspaces'):
        serve_utils.resolve_service_workspace('svc', {
            'workspace': None,
            'hash': 'incarnation-a'
        })
    set_workspace.assert_not_called()


def test_resolve_legacy_service_workspace_requires_trusted_empty_hint():
    record = {'workspace': None, 'hash': 'incarnation-a'}
    with mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=[]), \
         mock.patch.object(serve_utils.global_user_state,
                           'get_clusters_from_names',
                           return_value={}), \
         mock.patch.object(serve_state,
                           'set_service_workspace_if_owner',
                           return_value=True) as set_workspace:
        with pytest.raises(RuntimeError, match='replica-cluster'):
            serve_utils.resolve_service_workspace('svc', record, 'research')
        workspace = serve_utils.resolve_service_workspace(
            'svc', record, 'research', trusted_recovery_hint=True)

    assert workspace == 'research'
    set_workspace.assert_called_once_with('svc', 'research', 'incarnation-a')


def test_resolve_service_workspace_rechecks_with_status_snapshot_only():
    replicas = [types.SimpleNamespace(cluster_name='svc-r1')]
    cluster_records = {
        'svc-r1': {
            'workspace': 'research'
        },
    }
    with mock.patch.object(serve_state,
                           'get_replica_infos',
                           return_value=replicas), \
         mock.patch.object(serve_utils.global_user_state,
                           'get_clusters_from_names',
                           return_value=cluster_records), \
         mock.patch.object(serve_state,
                           'set_service_workspace_if_owner',
                           return_value=False), \
         mock.patch.object(serve_state,
                           'get_service_status_snapshot',
                           return_value={
                               'hash': 'incarnation-a',
                               'workspace': 'research',
                           }) as get_snapshot, \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'joined service read used')):
        workspace = serve_utils.resolve_service_workspace(
            'svc', {
                'workspace': None,
                'hash': 'incarnation-a'
            }, 'research')

    assert workspace == 'research'
    get_snapshot.assert_called_once_with('svc')


def test_resolve_service_workspace_rejects_stored_workspace_mismatch():
    with pytest.raises(RuntimeError, match="belongs to workspace 'research'"):
        serve_utils.resolve_service_workspace('svc', {
            'workspace': 'research',
            'hash': 'incarnation-a'
        }, 'production')


def test_get_yaml_content_uses_status_snapshot_for_resource_scope_fallback(
        tmp_path):
    yaml_path = tmp_path / 'svc.yaml'
    yaml_path.write_text('service: yaml\n', encoding='utf-8')

    with mock.patch.object(serve_state,
                           'get_yaml_content',
                           return_value=None), \
         mock.patch.object(serve_state,
                           'get_service_status_snapshot',
                           return_value={
                               'resource_scope': 'scope-a',
                           }) as get_snapshot, \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'joined service read used')), \
         mock.patch.object(serve_utils,
                           'generate_task_yaml_file_name',
                           return_value=str(yaml_path)) as get_yaml_path:
        content = serve_utils.get_yaml_content('svc', 7)

    assert content == 'service: yaml\n'
    get_snapshot.assert_called_once_with('svc')
    get_yaml_path.assert_called_once_with('svc', 7, resource_scope='scope-a')


def test_launch_quiesce_awaits_execution_receipt_after_cancellation():
    replicas = [
        mock.Mock(cluster_name='svc-a-r1', created_at=100.0),
        mock.Mock(cluster_name='svc-a-r2', created_at=100.0),
    ]
    cancellation_completed = False
    quiescence_polls = 0
    events = []

    def _request(status, *, quiesced_generation=None, quiesced_at=None):
        return types.SimpleNamespace(
            request_id='launch-request',
            name='sky.launch',
            cluster_name='svc-a-r1',
            execution_generation=7,
            status=status,
            execution_quiescence_required=True,
            execution_quiesced_generation=quiesced_generation,
            execution_quiesced_at=quiesced_at)

    def _query(req_filter):
        nonlocal quiescence_polls
        if req_filter.request_ids is None:
            assert req_filter == api_requests.RequestTaskFilter(
                cluster_names=['svc-a-r1', 'svc-a-r2'],
                include_request_names=['sky.launch'],
                execution_quiescence_candidates_only=True,
                fields=[
                    'request_id', 'name', 'cluster_name',
                    'execution_generation', 'status',
                    'execution_quiescence_required',
                    'execution_quiesced_generation', 'execution_quiesced_at'
                ],
                sort=True)
            events.append('discovery-status')
            if cancellation_completed:
                return []
            return [_request(api_requests.RequestStatus.RUNNING)]
        assert req_filter == api_requests.RequestTaskFilter(
            request_ids=['launch-request'],
            fields=[
                'request_id', 'name', 'cluster_name', 'status',
                'execution_generation', 'execution_quiescence_required',
                'execution_quiesced_generation', 'execution_quiesced_at'
            ],
            sort=True)
        events.append('quiescence-status')
        quiescence_polls += 1
        if quiescence_polls == 1:
            return [_request(api_requests.RequestStatus.CANCELLED)]
        return [
            _request(api_requests.RequestStatus.CANCELLED,
                     quiesced_generation=7,
                     quiesced_at=1.0)
        ]

    def _cancel(request_ids, *, user_id):
        nonlocal cancellation_completed
        assert request_ids == ['launch-request']
        assert user_id is None
        events.append('cancel-committed')
        cancellation_completed = True
        return request_ids

    with mock.patch.object(api_requests,
                           'get_request_tasks', side_effect=_query) as status, \
         mock.patch.object(api_requests,
                           'kill_requests_exact', side_effect=_cancel), \
         mock.patch.object(
             request_postgres,
             'execution_quiescence_backend_guard_enabled',
             return_value=True), \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends'), \
         mock.patch.object(serve_utils.sdk,
                           'api_status', side_effect=AssertionError), \
         mock.patch.object(serve_utils.sdk,
                           'api_cancel', side_effect=AssertionError), \
         mock.patch.object(serve_utils.sdk,
                           'stream_and_get', side_effect=AssertionError), \
         mock.patch.object(serve_utils, '_LAUNCH_QUIESCE_POLL_SECONDS', 0):
        quiesced = serve_utils.quiesce_service_replica_launch_requests(
            'svc',
            replicas,
            continue_guard=lambda: True,
            include_terminal_history=False)

    assert quiesced
    assert events == [
        'discovery-status', 'cancel-committed', 'quiescence-status',
        'quiescence-status', 'discovery-status'
    ]
    assert status.call_count == 4


@pytest.mark.parametrize('terminal_status', ['SUCCEEDED', 'FAILED'])
def test_launch_quiesce_accepts_handler_terminal_race(terminal_status):
    replica = mock.Mock(cluster_name='svc-a-r1', created_at=100.0)
    active_request = types.SimpleNamespace(
        request_id='launch-request',
        name='sky.launch',
        cluster_name='svc-a-r1',
        created_at=101.0,
        execution_generation=7,
        status=api_requests.RequestStatus.RUNNING,
        execution_quiescence_required=True,
        execution_quiesced_generation=None,
        execution_quiesced_at=None)
    terminal_request = types.SimpleNamespace(
        request_id='launch-request',
        name='sky.launch',
        cluster_name='svc-a-r1',
        created_at=101.0,
        status=api_requests.RequestStatus(terminal_status),
        execution_generation=7,
        execution_quiescence_required=True,
        execution_quiesced_generation=7,
        execution_quiesced_at=1.0)
    status_results = [[active_request], [terminal_request], []]
    with mock.patch.object(api_requests,
                           'get_request_tasks', side_effect=status_results), \
         mock.patch.object(api_requests,
                           'kill_requests_exact'), \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends'):
        assert serve_utils.quiesce_service_replica_launch_requests(
            'svc', [replica],
            continue_guard=lambda: True,
            include_terminal_history=True)


def test_launch_quiesce_missing_terminal_request_fails_closed():
    replica = mock.Mock(cluster_name='svc-a-r1', created_at=100.0)
    active_request = types.SimpleNamespace(
        request_id='launch-request',
        name='sky.launch',
        cluster_name='svc-a-r1',
        created_at=101.0,
        execution_generation=7,
        status=api_requests.RequestStatus.RUNNING,
        execution_quiescence_required=True,
        execution_quiesced_generation=None,
        execution_quiesced_at=None)
    with mock.patch.object(api_requests,
                           'get_request_tasks',
                           side_effect=[[active_request], []]), \
         mock.patch.object(api_requests,
                           'kill_requests_exact'), \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends'):
        assert not serve_utils.quiesce_service_replica_launch_requests(
            'svc', [replica],
            continue_guard=lambda: True,
            include_terminal_history=True)


def test_launch_quiesce_indeterminate_backend_capability_fails_closed():
    replica = mock.Mock(cluster_name='svc-a-r1', created_at=100.0)
    with mock.patch.object(api_requests,
                           'get_request_tasks') as status, \
         mock.patch.object(api_requests,
                           'kill_requests_exact') as cancel, \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends',
             side_effect=RuntimeError('request backend indeterminate')):
        assert not serve_utils.quiesce_service_replica_launch_requests(
            'svc', [replica],
            continue_guard=lambda: True,
            include_terminal_history=True)
    status.assert_not_called()
    cancel.assert_not_called()


def test_launch_quiesce_history_catches_arbitrarily_old_cancelled_handler():
    replica = mock.Mock(cluster_name='svc-a-r1', created_at=100.0)
    unproven = types.SimpleNamespace(
        request_id='launch-request',
        name='sky.launch',
        cluster_name='svc-a-r1',
        created_at=-1_000_000.0,
        status=api_requests.RequestStatus.CANCELLED,
        execution_generation=7,
        execution_quiescence_required=True,
        execution_quiesced_generation=None,
        execution_quiesced_at=None)
    proven = types.SimpleNamespace(request_id='launch-request',
                                   name='sky.launch',
                                   cluster_name='svc-a-r1',
                                   created_at=-1_000_000.0,
                                   status=api_requests.RequestStatus.CANCELLED,
                                   execution_generation=7,
                                   execution_quiescence_required=True,
                                   execution_quiesced_generation=7,
                                   execution_quiesced_at=1.0)
    with mock.patch.object(api_requests,
                           'get_request_tasks',
                           side_effect=[[unproven], [proven], []]) as status, \
         mock.patch.object(api_requests,
                           'kill_requests_exact') as cancel, \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends'):
        assert serve_utils.quiesce_service_replica_launch_requests(
            'svc', [replica],
            continue_guard=lambda: True,
            include_terminal_history=True)

    assert status.call_args_list[0] == mock.call(
        api_requests.RequestTaskFilter(
            cluster_names=['svc-a-r1'],
            include_request_names=['sky.launch'],
            execution_quiescence_candidates_only=True,
            fields=[
                'request_id', 'name', 'cluster_name', 'execution_generation',
                'status', 'execution_quiescence_required',
                'execution_quiesced_generation', 'execution_quiesced_at'
            ],
            sort=True))
    cancel.assert_not_called()


def test_launch_quiesce_guarded_store_is_independent_of_public_oauth():
    replicas = [
        mock.Mock(cluster_name=f'svc-a-r{replica_id}')
        for replica_id in range(2159)
    ]
    unrelated = mock.Mock(request_id='other-launch',
                          name='sky.launch',
                          cluster_name='another-service-r1')
    with mock.patch.object(
            request_postgres,
            'execution_quiescence_backend_guard_enabled',
            return_value=True), \
         mock.patch.object(api_requests,
                           'get_request_tasks', return_value=[unrelated]) \
         as status, \
         mock.patch.object(api_requests,
                           'kill_requests_exact') as cancel, \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends'), \
         mock.patch.object(serve_utils.sdk,
                           'api_status',
                           side_effect=ValueError('OAuth returned HTML')) \
         as public_status, \
         mock.patch.object(serve_utils.sdk,
                           'api_cancel', side_effect=AssertionError), \
         mock.patch.object(serve_utils.sdk,
                           'stream_and_get', side_effect=AssertionError):
        quiesced = serve_utils.quiesce_service_replica_launch_requests(
            'svc', replicas, continue_guard=lambda: True)

    assert quiesced
    status.assert_called_once_with(
        api_requests.RequestTaskFilter(
            cluster_names=sorted(
                f'svc-a-r{replica_id}' for replica_id in range(2159)),
            include_request_names=['sky.launch'],
            execution_quiescence_candidates_only=True,
            fields=[
                'request_id', 'name', 'cluster_name', 'execution_generation',
                'status', 'execution_quiescence_required',
                'execution_quiesced_generation', 'execution_quiesced_at'
            ],
            sort=True))
    cancel.assert_not_called()
    public_status.assert_not_called()


def test_launch_quiesce_indeterminate_request_store_fails_closed():
    replica = mock.Mock(cluster_name='svc-a-r1')
    with mock.patch.object(
            request_postgres,
            'execution_quiescence_backend_guard_enabled',
            return_value=True), \
         mock.patch.object(
             api_requests,
             'get_request_tasks',
             side_effect=RuntimeError('request database unavailable')), \
         mock.patch.object(api_requests,
                           'kill_requests_exact') as cancel, \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends'):
        assert not serve_utils.quiesce_service_replica_launch_requests(
            'svc', [replica], continue_guard=lambda: True)
    cancel.assert_not_called()


def test_launch_quiesce_legacy_remote_controller_keeps_sdk_compatibility(
        monkeypatch):
    replica = mock.Mock(cluster_name='svc-a-r1')
    launch_request = mock.Mock(request_id='launch-request',
                               cluster_name='svc-a-r1')
    launch_request.name = 'sky.launch'
    monkeypatch.setenv(serve_utils.skylet_constants.OVERRIDE_CONSOLIDATION_MODE,
                       'true')
    monkeypatch.delenv(
        request_postgres.EXECUTION_QUIESCENCE_BACKEND_GUARD_ENV_VAR,
        raising=False)
    assert serve_utils.is_consolidation_mode()
    with mock.patch.object(
            request_postgres,
            'execution_quiescence_backend_guard_enabled',
            return_value=False), \
         mock.patch.object(serve_utils.sdk,
                           'api_status', side_effect=[[launch_request], []]), \
         mock.patch.object(serve_utils.sdk,
                           'api_cancel', return_value='cancel-request') \
         as cancel, \
         mock.patch.object(serve_utils.sdk, 'stream_and_get') as wait, \
         mock.patch.object(api_requests,
                           'get_request_tasks', side_effect=AssertionError), \
         mock.patch.object(
             request_postgres,
             'require_builtin_execution_quiescence_backends',
             side_effect=AssertionError):
        assert serve_utils.quiesce_service_replica_launch_requests(
            'svc', [replica], continue_guard=lambda: True)

    cancel.assert_called_once_with(['launch-request'],
                                   all_users=True,
                                   silent=True)
    wait.assert_called_once_with('cancel-request')


def test_incarnation_scopes_files_and_replica_clusters():
    dir_a = serve_utils.generate_remote_service_dir_name('svc', 'hash-a')
    dir_b = serve_utils.generate_remote_service_dir_name('svc', 'hash-b')
    assert dir_a != dir_b
    assert dir_a != serve_utils.generate_remote_service_dir_name('svc')
    collision_names = ('svc-a', 'svc_a', 'Svc.A')
    collision_dirs = {
        serve_utils.generate_remote_service_dir_name(name, 'same-scope')
        for name in collision_names
    }
    assert len(collision_dirs) == len(collision_names)

    cluster_a = serve_utils.generate_replica_cluster_name(
        's' * 63, 123, 'hash-a')
    cluster_b = serve_utils.generate_replica_cluster_name(
        's' * 63, 123, 'hash-b')
    assert cluster_a != cluster_b
    assert len(cluster_a) <= 63
    collision_clusters = {
        serve_utils.generate_replica_cluster_name(name, 1, 'same-scope')
        for name in collision_names
    }
    assert len(collision_clusters) == len(collision_names)


def test_postgres_lifecycle_epoch_uses_lock_owning_session():
    pg_lock = mock.MagicMock(spec=serve_utils.locks.PostgresLock)
    pg_lock.is_session_alive.return_value = True
    connection = object()
    pg_lock.run_in_lock_session.side_effect = lambda operation: operation(
        connection)
    lifecycle_lock = serve_utils.ServiceLifecycleLock('svc', pg_lock)

    with mock.patch.object(serve_utils.serve_state,
                           'claim_service_lifecycle_epoch',
                           return_value=7) as claim:
        lifecycle_lock.acquire()

    claim.assert_called_once_with('svc', connection)
    assert lifecycle_lock.epoch == 7


def test_postgres_controller_preserving_lock_reads_current_epoch():
    pg_lock = mock.MagicMock(spec=serve_utils.locks.PostgresLock)
    pg_lock.is_session_alive.return_value = True
    connection = object()
    pg_lock.run_in_lock_session.side_effect = lambda operation: operation(
        connection)
    lifecycle_lock = serve_utils.ServiceLifecycleLock('svc',
                                                      pg_lock,
                                                      advance_epoch=False)

    with mock.patch.object(serve_utils.serve_state,
                           'read_service_lifecycle_epoch',
                           return_value=7) as read, \
         mock.patch.object(serve_utils.serve_state,
                           'claim_service_lifecycle_epoch') as claim:
        lifecycle_lock.acquire()

    read.assert_called_once_with('svc', connection)
    claim.assert_not_called()
    assert lifecycle_lock.epoch == 7


def test_postgres_deferred_lifecycle_lock_retains_on_lock_session():
    pg_lock = mock.MagicMock(spec=serve_utils.locks.PostgresLock)
    pg_lock.is_session_alive.return_value = True
    connection = object()
    pg_lock.run_in_lock_session.side_effect = lambda operation: operation(
        connection)
    lifecycle_lock = serve_utils.ServiceLifecycleLock('svc',
                                                      pg_lock,
                                                      advance_epoch=None)

    with mock.patch.object(serve_utils.serve_state,
                           'read_service_lifecycle_epoch',
                           return_value=7) as read, \
         mock.patch.object(serve_utils.serve_state,
                           'claim_service_lifecycle_epoch') as claim:
        lifecycle_lock.acquire()
        assert lifecycle_lock.epoch is None
        read.assert_not_called()
        claim.assert_not_called()
        assert serve_utils.retain_service_lifecycle_epoch(lifecycle_lock) == 7

    read.assert_called_once_with('svc', connection)
    claim.assert_not_called()
    assert lifecycle_lock.epoch == 7


def test_postgres_deferred_lifecycle_lock_advances_on_lock_session():
    pg_lock = mock.MagicMock(spec=serve_utils.locks.PostgresLock)
    pg_lock.is_session_alive.return_value = True
    connection = object()
    pg_lock.run_in_lock_session.side_effect = lambda operation: operation(
        connection)
    lifecycle_lock = serve_utils.ServiceLifecycleLock('svc',
                                                      pg_lock,
                                                      advance_epoch=None)

    with mock.patch.object(serve_utils.serve_state,
                           'read_service_lifecycle_epoch') as read, \
         mock.patch.object(serve_utils.serve_state,
                           'claim_service_lifecycle_epoch',
                           return_value=8) as claim:
        lifecycle_lock.acquire()
        assert lifecycle_lock.epoch is None
        read.assert_not_called()
        claim.assert_not_called()
        assert serve_utils.advance_service_lifecycle_epoch(lifecycle_lock) == 8

    claim.assert_called_once_with('svc', connection)
    read.assert_not_called()
    assert lifecycle_lock.epoch == 8


def test_lifecycle_epoch_cancellation_releases_lock():
    pg_lock = mock.MagicMock(spec=serve_utils.locks.PostgresLock)
    pg_lock.run_in_lock_session.side_effect = KeyboardInterrupt
    lifecycle_lock = serve_utils.ServiceLifecycleLock('svc', pg_lock)

    with pytest.raises(KeyboardInterrupt):
        lifecycle_lock.acquire()

    pg_lock.release.assert_called_once()


@pytest.mark.parametrize('status', [
    serve_state.ServiceStatus.SHUTTING_DOWN,
    serve_state.ServiceStatus.FAILED_CLEANUP,
])
def test_wait_registration_aborts_terminal_service_without_polling(status):
    record = {
        'controller_job_id': 7,
        'status': status,
        'load_balancer_port': None,
    }
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=record), \
         mock.patch.object(serve_utils.time, 'sleep') as sleep, \
         pytest.raises(RuntimeError, match='terminal status'):
        serve_utils.wait_service_registration('svc', 7, pool=False)
    sleep.assert_not_called()


@pytest.mark.parametrize('status', [
    status for status in serve_utils.job_lib.JobStatus if status.is_terminal()
])
def test_wait_registration_aborts_terminal_controller_job_without_polling(
        status):
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(serve_utils.job_lib,
                           'get_status',
                           return_value=status) as get_status, \
         mock.patch.object(serve_utils,
                           '_get_service_status') as get_service_status, \
         mock.patch.object(serve_utils.time, 'sleep') as sleep, \
         pytest.raises(RuntimeError, match=f'terminal status {status.value}'):
        serve_utils.wait_service_registration('svc', 7, pool=False)

    get_status.assert_called_once_with(7)
    get_service_status.assert_not_called()
    sleep.assert_not_called()


@pytest.mark.parametrize('status', [
    serve_utils.job_lib.JobStatus.INIT,
    serve_utils.job_lib.JobStatus.PENDING,
    serve_utils.job_lib.JobStatus.SETTING_UP,
])
def test_wait_registration_keeps_waiting_for_controller_setup_states(status):
    clock = mock.MagicMock()
    clock.monotonic.side_effect = [100.0, 401.0]
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(serve_utils.job_lib,
                           'get_status',
                           return_value=status) as get_status, \
         mock.patch.object(serve_utils,
                           '_get_service_status') as get_service_status, \
         mock.patch.object(serve_utils, 'time', clock), \
         pytest.raises(RuntimeError, match='controller process'):
        serve_utils.wait_service_registration('svc', 7, pool=False)

    get_status.assert_called_once_with(7)
    get_service_status.assert_not_called()
    clock.sleep.assert_not_called()


def test_wait_registration_running_controller_polls_service_immediately():
    record = {
        'controller_job_id': 7,
        'status': serve_state.ServiceStatus.READY,
        'load_balancer_port': 8080,
    }
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(
             serve_utils.job_lib,
             'get_status',
             return_value=serve_utils.job_lib.JobStatus.RUNNING) as get_status, \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=record) as get_service_status, \
         mock.patch.object(serve_utils.time, 'sleep') as sleep:
        payload = serve_utils.wait_service_registration('svc', 7, pool=False)

    assert serve_utils.load_service_initialization_result(payload) == 8080
    get_status.assert_called_once_with(7)
    get_service_status.assert_called_once_with('svc',
                                               pool=False,
                                               with_replica_info=False,
                                               with_yaml=False,
                                               status_snapshot_only=True)
    sleep.assert_not_called()


def test_wait_registration_reads_scoped_log_before_row_exists():
    log_path = '/tmp/scoped/controller.log'
    log_contents = constants.MAX_NUMBER_OF_SERVICES_REACHED_ERROR
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=None), \
         mock.patch.object(
             serve_utils,
             'generate_remote_controller_log_file_name',
             return_value=log_path) as generate_log_path, \
         mock.patch.object(serve_utils.os.path,
                           'exists',
                           return_value=True), \
         mock.patch('builtins.open',
                    mock.mock_open(read_data=log_contents)), \
         mock.patch.object(
             serve_utils.controller_utils,
             'get_max_services_error_message',
             return_value='at capacity'), \
         pytest.raises(RuntimeError, match='at capacity'):
        serve_utils.wait_service_registration(
            'svc', 7, pool=False, expected_resource_scope='incarnation-a')

    generate_log_path.assert_called_once_with('svc', 'incarnation-a')


def test_wait_registration_setup_timeout_uses_monotonic_deadline():
    """Wall clock rollback must not extend controller setup."""
    clock = mock.MagicMock()
    clock.time.side_effect = [1000.0, 999.0, 998.0]
    clock.monotonic.side_effect = [100.0, 401.0]
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(serve_utils.job_lib,
                           'get_status',
                           return_value=None) as get_status, \
         mock.patch.object(serve_utils,
                           '_get_service_status') as get_service_status, \
         mock.patch.object(serve_utils, 'time', clock), \
         mock.patch.object(
             clock,
             'sleep',
             side_effect=AssertionError('setup exceeded its deadline')) as sleep, \
         pytest.raises(RuntimeError, match='controller process'):
        serve_utils.wait_service_registration('svc', 7, pool=False)

    get_status.assert_called_once_with(7)
    get_service_status.assert_not_called()
    sleep.assert_not_called()
    clock.time.assert_not_called()
    assert clock.monotonic.call_count == 2


def test_wait_registration_service_timeout_uses_monotonic_deadline():
    """Wall clock rollback must not extend service registration."""
    clock = mock.MagicMock()
    clock.time.side_effect = [1000.0, 999.0, 998.0]
    clock.monotonic.side_effect = [100.0, 200.0, 621.0]
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=None) as get_service_status, \
         mock.patch.object(serve_utils.os.path,
                           'exists',
                           return_value=False), \
         mock.patch.object(serve_utils, 'time', clock), \
         mock.patch.object(
             clock,
             'sleep',
             side_effect=AssertionError(
                 'registration exceeded its deadline')) as sleep, \
         pytest.raises(ValueError, match='Failed to register service'):
        serve_utils.wait_service_registration('svc', 7, pool=False)

    get_service_status.assert_called_once_with('svc',
                                               pool=False,
                                               with_replica_info=False,
                                               with_yaml=False,
                                               status_snapshot_only=True)
    sleep.assert_not_called()
    clock.time.assert_not_called()
    assert clock.monotonic.call_count == 3


def test_wait_registration_accepts_result_at_deadline_boundary():
    """A result at the existing strict timeout boundary still succeeds."""
    pending = None
    registered = {
        'controller_job_id': 7,
        'status': serve_state.ServiceStatus.READY,
        'load_balancer_port': 8080,
    }
    clock = mock.MagicMock()
    clock.monotonic.side_effect = [100.0, 200.0, 620.0]
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           side_effect=[pending, registered]) as get_status, \
         mock.patch.object(serve_utils.os.path,
                           'exists',
                           return_value=False), \
         mock.patch.object(serve_utils, 'time', clock):
        payload = serve_utils.wait_service_registration('svc', 7, pool=False)

    assert serve_utils.load_service_initialization_result(payload) == 8080
    assert get_status.call_count == 2
    clock.sleep.assert_called_once_with(1)
    clock.time.assert_not_called()
    assert clock.monotonic.call_count == 3


def test_external_lb_service_spec_rejects_task_tls():
    spec = mock.Mock(tls_credential=mock.Mock())
    with pytest.raises(ValueError, match='Terminate TLS at the'):
        serve_utils.validate_external_lb_service_spec(spec)


def test_external_lb_service_spec_accepts_ingress_terminated_tls():
    spec = mock.Mock(tls_credential=None)
    serve_utils.validate_external_lb_service_spec(spec)


def test_task_fits():
    # Test exact fit.
    task_resources = Resources(cpus=1, memory=1, cloud=clouds.AWS())
    free_resources = Resources(cpus=1, memory=1, cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is True

    # Test less CPUs than free.
    task_resources = Resources(cpus=1, memory=1, cloud=clouds.AWS())
    free_resources = Resources(cpus=2, memory=1, cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is True

    # Test more CPUs than free.
    task_resources = Resources(cpus=2, memory=1, cloud=clouds.AWS())
    free_resources = Resources(cpus=1, memory=1, cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is False

    # Test less  memory than free.
    task_resources = Resources(cpus=1, memory=1, cloud=clouds.AWS())
    free_resources = Resources(cpus=1, memory=2, cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is True

    # Test more memory than free.
    task_resources = Resources(cpus=1, memory=2, cloud=clouds.AWS())
    free_resources = Resources(cpus=1, memory=1, cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is False

    # Test GPU exact fit.
    task_resources = Resources(accelerators='A10:1', cloud=clouds.AWS())
    free_resources = Resources(accelerators='A10:1', cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is True

    # Test GPUs less than free.
    task_resources = Resources(accelerators='A10:1', cloud=clouds.AWS())
    free_resources = Resources(accelerators='A10:2', cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is True

    # Test GPUs more than free.
    task_resources = Resources(accelerators='A10:2', cloud=clouds.AWS())
    free_resources = Resources(accelerators='A10:1', cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is False

    # Test resources exhausted.
    task_resources = Resources(cpus=1, memory=1, cloud=clouds.AWS())
    free_resources = Resources(cpus=None, memory=None, cloud=clouds.AWS())
    assert serve_utils._task_fits(task_resources, free_resources) is False


def test_serve_preemption_skips_autostopping():
    """Verify serve preemption logic treats AUTOSTOPPING like UP (not preempted)."""
    from sky.utils import status_lib

    # AUTOSTOPPING should be treated as UP-like (not preempted)
    # is_cluster_up() should return True for AUTOSTOPPING
    up_status = status_lib.ClusterStatus.UP
    autostopping_status = status_lib.ClusterStatus.AUTOSTOPPING
    stopped_status = status_lib.ClusterStatus.STOPPED

    # AUTOSTOPPING should be in the same category as UP for preemption purposes
    not_preempted_statuses = {
        up_status,
        autostopping_status,
    }

    assert up_status in not_preempted_statuses
    assert autostopping_status in not_preempted_statuses
    assert stopped_status not in not_preempted_statuses


def test_get_provider_configs_for_handles_fans_out_distinct_and_shared():
    """Each key must receive the provider for ITS OWN cluster_yaml.

    The shipped #900 tests only exercise the all-shared case, where every key
    resolves to the same provider and a cross-wired fan-out would pass
    unnoticed. This covers the realistic mixed case: two replicas share cluster
    ``a`` while a third uses cluster ``b``, and handles without a ``str``
    cluster_yaml are skipped. It also proves the batched read is deduplicated to
    a single call over the unique paths in first-occurrence order.
    """
    handles_by_key = {
        1: types.SimpleNamespace(cluster_yaml='/p/a.yaml'),
        2: types.SimpleNamespace(cluster_yaml='/p/b.yaml'),
        3: types.SimpleNamespace(cluster_yaml='/p/a.yaml'),
        4: types.SimpleNamespace(cluster_yaml=None),
        5: types.SimpleNamespace(cluster_yaml=object()),
    }
    provider_a = {'context': 'a'}
    provider_b = {'context': 'b'}
    yamls_by_path = {
        '/p/a.yaml': 'provider:\n  context: a\n',
        '/p/b.yaml': 'provider:\n  context: b\n',
    }
    failed_keys = set()

    with mock.patch.object(
            serve_utils.global_user_state,
            'get_cluster_yaml_str_multiple',
            side_effect=lambda paths: [yamls_by_path[path] for path in paths],
    ) as get_yamls:
        result = serve_utils.get_provider_configs_for_handles(
            handles_by_key, failed_keys=failed_keys)

    # One batched read over the unique paths, in first-occurrence order.
    get_yamls.assert_called_once_with(['/p/a.yaml', '/p/b.yaml'])
    # Every key resolves to the provider for ITS OWN yaml; skipped handles
    # (None / non-str cluster_yaml) are absent from the result.
    assert result == {1: provider_a, 2: provider_b, 3: provider_a}
    # The fan-out shares the exact parsed provider object across shared keys.
    assert result[1] is result[3]
    assert failed_keys == {4, 5}


def test_provider_config_batch_failure_never_amplifies_to_singleton_reads():
    handles = {
        replica_id: types.SimpleNamespace(cluster_yaml=f'/p/{replica_id}.yaml')
        for replica_id in range(800)
    }
    failed_keys = set()
    with mock.patch.object(
            serve_utils.global_user_state,
            'get_cluster_yaml_str_multiple',
            side_effect=RuntimeError('database unavailable')) as read_batch, \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_cluster_yaml_str',
             side_effect=AssertionError('N-read fallback used')) as read_one:
        result = serve_utils.get_provider_configs_for_handles(
            handles, failed_keys=failed_keys)

    assert result == {}
    assert failed_keys == set(handles)
    read_batch.assert_called_once_with(
        [f'/p/{replica_id}.yaml' for replica_id in range(800)])
    read_one.assert_not_called()


def test_bounded_teardown_empty_admission_fails_closed_without_hanging():
    info = types.SimpleNamespace(
        replica_id=1,
        replica_record_id='00000000-0000-4000-8000-000000000001',
        status_property=types.SimpleNamespace(
            sky_down_status=common_utils.ProcessStatus.SCHEDULED))
    worker = mock.Mock(spec=thread_utils.SafeThread)
    worker.is_alive.return_value = False
    worker.ident = None
    reserve = mock.Mock(return_value={})
    restore = mock.Mock()
    succeeded = mock.Mock()
    failed = mock.Mock()

    with pytest.raises(RuntimeError, match='made no progress'):
        serve_utils.run_bounded_serve_teardown_threads(
            [(info, worker)],
            pool=False,
            reserve_running=reserve,
            restore_never_started=restore,
            handle_success=succeeded,
            handle_failure=failed,
            continue_guard=lambda: True,
            max_concurrent_per_service=1,
            poll_interval_seconds=0,
            max_no_progress_polls=2)

    assert reserve.call_count == 2
    worker.start.assert_not_called()
    restore.assert_not_called()
    succeeded.assert_not_called()
    failed.assert_not_called()


class TestIsConsolidationMode:
    """Tests for serve_utils.is_consolidation_mode(pool=...).

    Pool consolidation shares a cluster with managed jobs and must track the
    jobs signal file, not the `jobs.controller.consolidation_mode` config key.
    Serve consolidation (pool=False) is independent and remains config-driven.
    """

    def setup_method(self):
        serve_utils.is_consolidation_mode.cache_clear()

    @pytest.mark.parametrize('helper_result', [True, False])
    def test_pool_delegates_to_controller_utils_helper(self, helper_result,
                                                       monkeypatch):
        """pool=True routes through controller_utils.is_jobs_consolidation_mode
        with the pool extra validator, so the two readers share one source."""
        monkeypatch.delenv('IS_SKYPILOT_SERVER', raising=False)
        monkeypatch.delenv('IS_SKYPILOT_JOB_CONTROLLER', raising=False)
        with mock.patch('sky.utils.controller_utils.is_jobs_consolidation_mode',
                        return_value=helper_result) as mock_helper:
            assert serve_utils.is_consolidation_mode(pool=True) is helper_result
            mock_helper.assert_called_once_with(
                extra_validator=serve_utils._pool_consolidation_extra_validator)

    @pytest.mark.parametrize('arg,should_validate', [
        (False, True),
        (True, False),
    ])
    def test_pool_extra_validator_runs_pool_validator_only_when_off(
            self, arg, should_validate):
        """The extra validator supplied to the helper fires the pool-specific
        validator only when consolidation is off. The consolidated case is
        already covered by the jobs validator inside the helper."""
        validate_path = ('sky.serve.serve_utils.'
                         '_validate_consolidation_mode_config')
        with mock.patch(validate_path) as mock_validate:
            serve_utils._pool_consolidation_extra_validator(arg)
            if should_validate:
                mock_validate.assert_called_once_with(arg, pool=True)
            else:
                mock_validate.assert_not_called()

    @pytest.mark.parametrize('pool,count,noun', [
        (False, 3, 'services'),
        (True, 2, 'pools'),
    ])
    def test_disabled_validation_uses_mode_scoped_count(self, pool, count,
                                                        noun):
        with mock.patch('sky.serve.serve_utils.serve_state.get_num_services',
                        return_value=count) as get_num_services, \
                mock.patch('sky.serve.serve_utils.serve_state.get_services',
                           side_effect=AssertionError(
                               'validation must not materialize services')), \
                mock.patch('sky.serve.serve_utils.logger.warning') as warning:
            serve_utils._validate_consolidation_mode_config(False, pool=pool)

        get_num_services.assert_called_once_with(pool=pool)
        warning.assert_called_once()
        assert f'still {count} {noun} running' in warning.call_args.args[0]

    def test_disabled_validation_skips_warning_when_mode_is_empty(self):
        with mock.patch('sky.serve.serve_utils.serve_state.get_num_services',
                        return_value=0) as get_num_services, \
                mock.patch('sky.serve.serve_utils.logger.warning') as warning:
            serve_utils._validate_consolidation_mode_config(False, pool=False)

        get_num_services.assert_called_once_with(pool=False)
        warning.assert_not_called()

    @pytest.mark.parametrize('config_value,expected', [(True, True),
                                                       (False, False)])
    def test_serve_reads_config_only(self, config_value, expected, monkeypatch):
        """pool=False: reads serve config key; signal file must not affect."""
        monkeypatch.delenv('IS_SKYPILOT_JOB_CONTROLLER', raising=False)
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            signal_file.touch()  # signal file present should not matter
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)), \
                    mock.patch('sky.serve.serve_utils.skypilot_config'
                              ) as mock_config:
                mock_config.get_nested.return_value = config_value
                assert serve_utils.is_consolidation_mode(pool=False) is expected
                mock_config.get_nested.assert_called_once_with(
                    ('serve', 'controller', 'consolidation_mode'),
                    default_value=False)

    @mock.patch.dict('os.environ', {'IS_SKYPILOT_JOB_CONTROLLER': '1'},
                     clear=False)
    def test_override_env_forces_true_for_serve(self):
        """OVERRIDE_CONSOLIDATION_MODE forces True in the serve (pool=False)
        branch. Pool case goes through the helper which has its own OVERRIDE
        short-circuit tested in controller_utils."""
        with mock.patch('sky.serve.serve_utils.skypilot_config'):
            assert serve_utils.is_consolidation_mode(pool=False) is True


# ---------------------------------------------------------------------------
# Tests for HA leader-aware controller URL routing
# ---------------------------------------------------------------------------
# pylint: disable=protected-access


class TestGetControllerUrl:
    _HASH = 'incarnation-a'

    def _record(self, **overrides):
        record = {
            'hash': self._HASH,
            'status': serve_state.ServiceStatus.READY,
            'controller_pid': 1234,
            'controller_port': 20001,
            'controller_ip': None,
        }
        record.update(overrides)
        return record

    def _patch_record(self, **overrides):
        return mock.patch(
            'sky.serve.serve_utils.serve_state.'
            'get_service_controller_owner',
            return_value=self._record(**overrides))

    def test_no_record_fails_closed(self):
        with mock.patch(
                'sky.serve.serve_utils.serve_state.'
                'get_service_controller_owner',
                return_value=None):
            with pytest.raises(serve_utils.ControllerOwnerError):
                serve_utils._get_controller_url('svc', self._HASH)

    def test_controller_ip_none_returns_localhost(self):
        with self._patch_record():
            url, fingerprint = serve_utils._get_controller_url(
                'svc', self._HASH)
        assert url == 'http://localhost:20001'
        assert fingerprint == serve_utils.make_controller_owner_fingerprint(
            self._HASH, 1234, None, 20001)

    def test_controller_ip_equals_self_returns_localhost(self, monkeypatch):
        monkeypatch.setenv('POD_IP', '10.0.0.5')
        with self._patch_record(controller_ip='10.0.0.5'):
            url, _ = serve_utils._get_controller_url('svc', self._HASH)
        assert url == 'http://localhost:20001'

    def test_controller_ip_differs_returns_pod_ip(self, monkeypatch):
        monkeypatch.setenv('POD_IP', '10.0.0.5')
        with self._patch_record(controller_ip='10.0.0.7'):
            url, _ = serve_utils._get_controller_url('svc', self._HASH)
        assert url == 'http://10.0.0.7:20001'

    def test_no_pod_ip_env_routes_via_recorded_ip(self, monkeypatch):
        monkeypatch.delenv('POD_IP', raising=False)
        with self._patch_record(controller_ip='10.0.0.7'):
            url, _ = serve_utils._get_controller_url('svc', self._HASH)
        assert url == 'http://10.0.0.7:20001'

    def test_ipv6_literal_is_bracketed(self, monkeypatch):
        monkeypatch.setenv('POD_IP', '2001:db8::2')
        with self._patch_record(controller_ip='2001:0db8::1'):
            url, fingerprint = serve_utils._get_controller_url(
                'svc', self._HASH)
        assert url == 'http://[2001:db8::1]:20001'
        assert fingerprint == serve_utils.make_controller_owner_fingerprint(
            self._HASH, 1234, '2001:db8::1', 20001)

    def test_same_name_successor_is_rejected(self):
        with self._patch_record(hash='incarnation-b'):
            with pytest.raises(serve_utils.ControllerOwnerError):
                serve_utils._get_controller_url('svc', self._HASH)

    @pytest.mark.parametrize('overrides', [
        {
            'controller_pid': None
        },
        {
            'controller_port': 0
        },
        {
            'controller_ip': 'not-an-ip'
        },
        {
            'status': serve_state.ServiceStatus.SHUTTING_DOWN
        },
    ])
    def test_invalid_owner_is_rejected(self, overrides):
        with self._patch_record(**overrides):
            with pytest.raises(serve_utils.ControllerOwnerError):
                serve_utils._get_controller_url('svc', self._HASH)


class TestControllerHttpRetry:

    _HASH = 'incarnation-a'

    def _record(self, **overrides):
        record = {
            'hash': self._HASH,
            'status': serve_state.ServiceStatus.READY,
            'controller_pid': 1234,
            'controller_port': 20001,
            'controller_ip': None,
        }
        record.update(overrides)
        return record

    def _patch_record(self, **overrides):
        return mock.patch(
            'sky.serve.serve_utils.serve_state.'
            'get_service_controller_owner',
            return_value=self._record(**overrides))

    def test_post_succeeds_first_try(self):
        with self._patch_record():
            with mock.patch('sky.serve.serve_utils.requests.post',
                            return_value=mock.Mock(status_code=200)) as m:
                resp = serve_utils._post_to_controller_with_retry(
                    'svc', self._HASH, '/controller/update_service', json={})
                assert resp.status_code == 200
                assert m.call_count == 1

    def test_admin_ring_falls_back_only_after_401(self, monkeypatch, tmp_path):
        ring = tmp_path / 'admin.tokens'
        ring.write_text('primary\noverlap\n', encoding='utf-8')
        monkeypatch.setenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                           str(ring))
        responses = [mock.Mock(status_code=401), mock.Mock(status_code=200)]
        with self._patch_record(), \
             mock.patch('sky.serve.serve_utils.requests.post',
                        side_effect=responses) as request:
            response = serve_utils._post_to_controller_with_retry(
                'svc', self._HASH, '/controller/update_service', json={})

        assert response.status_code == 200
        assert [
            call.kwargs['headers']['Authorization']
            for call in request.call_args_list
        ] == ['Bearer primary', 'Bearer overlap']

    def test_admin_ring_does_not_fallback_on_non_401(self, monkeypatch,
                                                     tmp_path):
        ring = tmp_path / 'admin.tokens'
        ring.write_text('primary\noverlap\n', encoding='utf-8')
        monkeypatch.setenv(constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                           str(ring))
        with self._patch_record(), \
             mock.patch('sky.serve.serve_utils.requests.post',
                        return_value=mock.Mock(status_code=500)) as request:
            response = serve_utils._post_to_controller_with_retry(
                'svc', self._HASH, '/controller/update_service', json={})

        assert response.status_code == 500
        request.assert_called_once()
        assert request.call_args.kwargs['headers']['Authorization'] == (
            'Bearer primary')

    def test_internal_request_never_uses_lb_sync_ring(self, monkeypatch,
                                                      tmp_path):
        sync_ring = tmp_path / 'sync.tokens'
        sync_ring.write_text('sync-only\n', encoding='utf-8')
        monkeypatch.setenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                           str(sync_ring))
        monkeypatch.delenv(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
                           raising=False)
        with self._patch_record(), \
             mock.patch('sky.serve.serve_utils.requests.get',
                        return_value=mock.Mock(status_code=200)) as request:
            serve_utils._get_to_controller_with_retry('svc', self._HASH,
                                                      '/autoscaler/info')

        headers = request.call_args.kwargs['headers']
        assert 'Authorization' not in headers
        assert headers[constants.CONTROLLER_OWNER_HEADER] == (
            serve_utils.make_controller_owner_fingerprint(
                self._HASH, 1234, None, 20001))

    def test_post_retries_then_succeeds(self):
        # First 2 calls raise, 3rd succeeds. The default attempt count is
        # tightened to 1 (see lazy-handle PR), but the retry mechanism
        # itself still needs end-to-end coverage — patch the constant up
        # to 3 just for this test.
        side = [
            requests_exceptions.ConnectionError('refused'),
            requests_exceptions.ConnectionError('refused'),
            mock.Mock(status_code=200)
        ]
        with self._patch_record(), \
             mock.patch.object(controller_transport,
                               '_CONTROLLER_HTTP_RETRY_ATTEMPTS', 3), \
             mock.patch('sky.serve.serve_utils.time.sleep'), \
             mock.patch('sky.serve.serve_utils.requests.post',
                        side_effect=side) as m:
            resp = serve_utils._post_to_controller_with_retry(
                'svc', self._HASH, '/controller/update_service', json={})
            assert resp.status_code == 200
            assert m.call_count == 3

    def test_post_exhausts_retries_and_raises(self):
        with self._patch_record(), \
             mock.patch('sky.serve.serve_utils.time.sleep'), \
             mock.patch('sky.serve.serve_utils.requests.post',
                        side_effect=requests_exceptions.ConnectionError('refused')) as m:
            with pytest.raises(requests_exceptions.ConnectionError):
                serve_utils._post_to_controller_with_retry(
                    'svc', self._HASH, '/controller/update_service', json={})
            assert m.call_count == serve_utils._CONTROLLER_HTTP_RETRY_ATTEMPTS

    def test_get_succeeds_first_try(self):
        with self._patch_record():
            with mock.patch('sky.serve.serve_utils.requests.get',
                            return_value=mock.Mock(status_code=200)) as m:
                resp = serve_utils._get_to_controller_with_retry(
                    'svc', self._HASH, '/autoscaler/info')
                assert resp.status_code == 200
                assert m.call_count == 1

    def test_get_retries_url_is_re_resolved_each_attempt(self):
        """Between retries we re-call _get_controller_url so that if DB
        finished flipping during the backoff, we route to the new owner on
        the next try."""

        # Simulate DB flip mid-retry: first lookup says 10.0.0.7, second
        # lookup says 10.0.0.8.
        records = [
            {
                'hash': self._HASH,
                'status': serve_state.ServiceStatus.READY,
                'controller_pid': 1,
                'controller_port': 20001,
                'controller_ip': '10.0.0.7'
            },
            {
                'hash': self._HASH,
                'status': serve_state.ServiceStatus.READY,
                'controller_pid': 2,
                'controller_port': 20002,
                'controller_ip': '10.0.0.8'
            },
        ]
        urls_called = []
        owner_headers = []

        def capture_get(url, **kwargs):
            urls_called.append(url)
            owner_headers.append(
                kwargs['headers'][constants.CONTROLLER_OWNER_HEADER])
            if len(urls_called) == 1:
                raise requests_exceptions.ConnectionError('refused')
            return mock.Mock(status_code=200)

        with mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_service_controller_owner',
                        side_effect=records), \
             mock.patch.object(controller_transport,
                               '_CONTROLLER_HTTP_RETRY_ATTEMPTS', 3), \
             mock.patch('sky.serve.serve_utils.time.sleep'), \
             mock.patch('sky.serve.serve_utils.requests.get',
                        side_effect=capture_get):
            serve_utils._get_to_controller_with_retry('svc', self._HASH,
                                                      '/autoscaler/info')
        assert urls_called[0] == 'http://10.0.0.7:20001/autoscaler/info'
        assert urls_called[1] == 'http://10.0.0.8:20002/autoscaler/info'
        assert owner_headers == [
            serve_utils.make_controller_owner_fingerprint(
                self._HASH, 1, '10.0.0.7', 20001),
            serve_utils.make_controller_owner_fingerprint(
                self._HASH, 2, '10.0.0.8', 20002),
        ]

    def test_same_name_successor_is_never_contacted(self):
        with self._patch_record(hash='incarnation-b'), \
             mock.patch('sky.serve.serve_utils.requests.post') as request:
            with pytest.raises(serve_utils.ControllerOwnerError):
                serve_utils._post_to_controller_with_retry(
                    'svc', self._HASH, '/controller/update_service', json={})
        request.assert_not_called()

    def test_log_levels_one_warn_per_cycle(self):
        with self._patch_record(), \
             mock.patch('sky.serve.serve_utils.time.sleep'), \
             mock.patch(
                 'sky.serve.serve_utils.requests.get',
                 side_effect=requests_exceptions.ConnectionError('refused')), \
             mock.patch.object(serve_utils.logger, 'warning') as warn, \
             mock.patch.object(serve_utils.logger, 'debug') as debug:
            with pytest.raises(requests_exceptions.ConnectionError):
                serve_utils._get_to_controller_with_retry(
                    'svc', self._HASH, '/autoscaler/info')
        # Final-attempt failure → exactly one WARN.
        assert warn.call_count == 1, (
            f'expected exactly 1 WARN call, got {warn.call_count}: '
            f'{warn.call_args_list}')
        # Intermediate retry attempts log at DEBUG (N-1 of them). Filter
        # by message content so the assertion stays robust to other
        # DEBUG lines emitted on the same path (e.g. `_get_controller_url`
        # also emits one DEBUG per URL resolution — those are routing
        # diagnostics, not retry signals).
        retry_debug_calls = [
            c for c in debug.call_args_list
            if 'Connection to controller' in (c.args[0] if c.args else '')
        ]
        assert len(retry_debug_calls) == (
            serve_utils._CONTROLLER_HTTP_RETRY_ATTEMPTS -
            1), (f'expected {serve_utils._CONTROLLER_HTTP_RETRY_ATTEMPTS - 1} '
                 f'retry DEBUG calls, got {len(retry_debug_calls)}: '
                 f'{retry_debug_calls}')

    def test_default_timeout_is_passed_to_requests(self):
        """Without an explicit timeout, `requests` blocks forever. Cross-pod
        TCP connect to a dead remote pod can hang for tens of seconds, which
        is why `sky jobs pool status` was hanging. Verify we always inject
        the default timeout if caller didn't provide one."""
        captured = {}

        def capture(url, **kwargs):
            captured.update(kwargs)
            return mock.Mock(status_code=200)

        with self._patch_record(), \
             mock.patch('sky.serve.serve_utils.requests.get',
                        side_effect=capture):
            serve_utils._get_to_controller_with_retry('svc', self._HASH,
                                                      '/autoscaler/info')
        assert 'timeout' in captured
        assert captured['timeout'] == (
            serve_utils._CONTROLLER_HTTP_TIMEOUT_SECONDS)

    def test_caller_supplied_timeout_wins(self):
        """If a call site explicitly passes timeout, don't override it."""
        captured = {}

        def capture(url, **kwargs):
            captured.update(kwargs)
            return mock.Mock(status_code=200)

        with self._patch_record(), \
             mock.patch('sky.serve.serve_utils.requests.get',
                        side_effect=capture):
            serve_utils._get_to_controller_with_retry('svc',
                                                      self._HASH,
                                                      '/autoscaler/info',
                                                      timeout=42)
        assert captured['timeout'] == 42

    def test_timeout_exception_triggers_retry(self):
        """`requests.exceptions.Timeout` (raised on connect/read timeout)
        must go through the same retry path as ConnectionError. Otherwise
        the first slow connect would propagate immediately and the user
        would see a hang from the timeout itself rather than a fast
        retry-and-fail."""
        side = [
            requests_exceptions.Timeout('connect timed out'),
            requests_exceptions.Timeout('connect timed out'),
            mock.Mock(status_code=200),
        ]
        # Patch the attempt count up to 3 so the retry path is actually
        # exercised; the production default is 1 (see lazy-handle PR).
        with self._patch_record(), \
             mock.patch.object(controller_transport,
                               '_CONTROLLER_HTTP_RETRY_ATTEMPTS', 3), \
             mock.patch('sky.serve.serve_utils.time.sleep'), \
             mock.patch('sky.serve.serve_utils.requests.get',
                        side_effect=side) as m:
            resp = serve_utils._get_to_controller_with_retry(
                'svc', self._HASH, '/autoscaler/info')
            assert resp.status_code == 200
            assert m.call_count == 3


class TestTerminateShuttingDownPurge:
    """SHUTTING_DOWN zombies (e.g. controller subprocess SIGKILL'd between
    `_cleanup`'s first step and the row removal) must be reachable via
    `--purge`. Plain `down` keeps its previous skip-already-scheduled
    behavior."""

    def _service_record(self, status):
        return {
            'name': 'svc',
            'status': status,
            'controller_pid': 1234,
            'controller_port': 20001,
            'controller_ip': None,
            'pool': True,
            'hash': 'incarnation-a',
        }

    def test_purge_calls_terminate_failed_services_for_shutting_down(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state
        with mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_glob_service_names',
                        return_value=['svc']), \
             mock.patch('sky.serve.serve_utils._get_service_status',
                        return_value=self._service_record(
                            serve_state.ServiceStatus.SHUTTING_DOWN)), \
             mock.patch(
                 'sky.serve.serve_utils.managed_job_state.'
                 'get_nonterminal_job_ids_by_pool',
                 return_value=[]), \
             mock.patch('sky.serve.serve_utils._terminate_failed_services',
                        return_value=serve_utils._PurgeResult(True)
                       ) as mock_purge:
            serve_utils.terminate_services(['svc'], purge=True, pool=True)
            mock_purge.assert_called_once()
            args = mock_purge.call_args[0]
            assert args[0] == 'svc'
            assert args[1] == 'incarnation-a'
            assert args[2] == serve_state.ServiceStatus.SHUTTING_DOWN

    def test_no_purge_skips_shutting_down_unchanged(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state
        with mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_glob_service_names',
                        return_value=['svc']), \
             mock.patch('sky.serve.serve_utils._get_service_status',
                        return_value=self._service_record(
                            serve_state.ServiceStatus.SHUTTING_DOWN)), \
             mock.patch(
                 'sky.serve.serve_utils.managed_job_state.'
                 'get_nonterminal_job_ids_by_pool',
                 return_value=[]), \
             mock.patch('sky.serve.serve_utils._terminate_failed_services'
                       ) as mock_purge:
            serve_utils.terminate_services(['svc'], purge=False, pool=True)
            mock_purge.assert_not_called()


class TestTerminateOrphanedServiceRowPurge:
    """A `services` row with no `version_specs` row (an interrupted first-run
    registration) is invisible to the latest-version join, so
    `_get_service_status` returns None. `--purge` must clean it up -- but only
    when the raw row belongs to the requested mode, since `_get_service_status`
    also returns None for a healthy service of the *other* mode."""

    def _run(self, *, purge, raw_pool, requested_pool):
        with mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_glob_service_names',
                        return_value=['svc']), \
             mock.patch('sky.serve.serve_utils._get_service_status',
                        return_value=None), \
             mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_service_mode_and_hash',
                        return_value=(raw_pool, 'orphan-hash')
                        if raw_pool is not None else None), \
             mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_orphaned_service_child_mode',
                        return_value=None), \
             mock.patch('sky.serve.serve_utils._terminate_failed_services',
                        return_value=serve_utils._PurgeResult(True)
                       ) as mock_purge:
            serve_utils.terminate_services(['svc'],
                                           purge=purge,
                                           pool=requested_pool)
        return mock_purge

    def test_purges_orphan_of_requested_mode(self):
        mock_purge = self._run(purge=True, raw_pool=False, requested_pool=False)
        mock_purge.assert_called_once()
        # Called with a None status (no version row -> no real status).
        assert mock_purge.call_args[0] == ('svc', 'orphan-hash', None)

    def test_does_not_purge_wrong_mode_row(self):
        # raw row is a jobs-pool (pool=True) but the command is `serve down`
        # (pool=False): must NOT be removed by the wrong mode.
        mock_purge = self._run(purge=True, raw_pool=True, requested_pool=False)
        mock_purge.assert_not_called()

    def test_does_not_purge_when_row_absent(self):
        mock_purge = self._run(purge=True, raw_pool=None, requested_pool=False)
        mock_purge.assert_not_called()

    def test_no_purge_leaves_orphan_untouched(self):
        mock_purge = self._run(purge=False,
                               raw_pool=False,
                               requested_pool=False)
        mock_purge.assert_not_called()


def test_child_only_purge_mode_mismatch_is_not_reported_as_completed():
    lifecycle_lock = mock.MagicMock()
    lifecycle_lock.epoch = 9
    with mock.patch.object(serve_state,
                           'get_glob_service_names', return_value=[]), \
         mock.patch.object(serve_state,
                           'get_orphaned_service_child_names',
                           return_value=['svc']), \
         mock.patch.object(serve_utils,
                           '_get_service_status', return_value=None), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hash', return_value=None), \
         mock.patch.object(serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=lifecycle_lock), \
         mock.patch.object(serve_state,
                           'get_orphaned_service_child_mode',
                           return_value=True), \
         mock.patch.object(serve_state,
                           'remove_orphaned_service_children') as remove:
        message = serve_utils.terminate_services(['svc'],
                                                 purge=True,
                                                 pool=False)

    assert 'belongs to a pool, not a service' in message
    assert 'No service to terminate.' in message
    assert 'scheduled to be terminated' not in message
    remove.assert_not_called()


def test_child_only_purge_skips_absent_clusters_with_one_inventory_snapshot():
    lifecycle_lock = mock.MagicMock(epoch=9)
    replica_infos = [
        mock.Mock(replica_id=replica_id, cluster_name=f'orphan-r{replica_id}')
        for replica_id in range(2159)
    ]
    with mock.patch.object(serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=lifecycle_lock), \
         mock.patch.object(serve_utils,
                           'lifecycle_lock_is_valid', return_value=True), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hash', return_value=None), \
         mock.patch.object(serve_state,
                           'get_orphaned_service_child_mode',
                           return_value=True), \
         mock.patch.object(serve_state,
                           'get_replica_infos', return_value=replica_infos), \
         mock.patch.object(
             serve_utils,
             'quiesce_service_replica_launch_requests', return_value=True), \
         mock.patch.object(serve_state,
                           'get_ephemeral_storage_cleanup_intents',
                           return_value=[]), \
         mock.patch.object(serve_utils.global_user_state,
                           'get_cluster_status_fields', return_value={}
                          ) as cluster_snapshot, \
         mock.patch('sky.serve.replica_managers.terminate_cluster'
                   ) as terminate, \
         mock.patch.object(serve_state,
                           'remove_orphaned_service_children',
                           return_value=True) as remove:
        message = serve_utils._terminate_orphaned_service_children_impl(
            'orphan', True)

    assert message is None
    cluster_snapshot.assert_called_once()
    assert cluster_snapshot.call_args.args[0] == [
        info.cluster_name for info in replica_infos
    ]
    terminate.assert_not_called()
    remove.assert_called_once_with('orphan', 9)


def test_child_only_purge_retains_absent_protocol_v2_cluster():
    lifecycle_lock = mock.MagicMock(epoch=9)
    replica = mock.Mock(replica_id=1, cluster_name='orphan-r1')
    cleanup_fence = types.SimpleNamespace(kubernetes_context='phx-context',
                                          physical_cluster_uid='phx-uid')
    with mock.patch.object(serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=lifecycle_lock), \
         mock.patch.object(serve_utils,
                           'lifecycle_lock_is_valid', return_value=True), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hash', return_value=None), \
         mock.patch.object(serve_state,
                           'get_orphaned_service_child_mode',
                           return_value=True), \
         mock.patch.object(serve_state,
                           'get_replica_infos', return_value=[replica]), \
         mock.patch.object(
             serve_utils,
             'quiesce_service_replica_launch_requests',
             return_value=True) as quiesce, \
         mock.patch.object(serve_state,
                           'get_ephemeral_storage_cleanup_intents',
                           return_value=[]), \
         mock.patch.object(serve_utils.global_user_state,
                           'get_cluster_status_fields', return_value={}), \
         mock.patch(
             'sky.serve.reserved_capacity.'
             'parse_protocol_v2_cleanup_fence',
             return_value=cleanup_fence), \
         mock.patch('sky.serve.replica_managers.terminate_cluster'
                   ) as terminate, \
         mock.patch.object(serve_state,
                           'remove_orphaned_service_children') as remove:
        message = serve_utils._terminate_orphaned_service_children_impl(
            'orphan', True)

    assert message is not None and 'cluster termination failed' in message
    assert quiesce.call_args.kwargs['include_terminal_history'] is True
    terminate.assert_not_called()
    remove.assert_not_called()


def test_orphaned_service_cluster_fields_require_consolidation():
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_managed_cluster_status_fields') as get_candidates, \
         mock.patch.object(serve_state,
                           'get_replica_cluster_names') as get_owners:
        result = serve_utils.get_orphaned_service_cluster_status_fields()

    assert result == {}
    get_candidates.assert_not_called()
    get_owners.assert_not_called()


def test_orphaned_service_cluster_fields_use_exact_replica_ownership():
    candidates = {
        'predecessor-r1': global_user_state.ManagedClusterStatusFields(
            'UP', 1, 'old-hash'),
        'current-r1': global_user_state.ManagedClusterStatusFields(
            'UP', 2, 'new-hash'),
        'failed-launch-r2': global_user_state.ManagedClusterStatusFields(
            'INIT', 3, 'failed-hash'),
    }
    with mock.patch.object(serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_managed_cluster_status_fields',
             return_value=candidates) as get_candidates, \
         mock.patch.object(
             serve_state,
             'get_replica_cluster_names',
             return_value={'current-r1'}) as get_owners:
        result = serve_utils.get_orphaned_service_cluster_status_fields()

    assert result == {
        'predecessor-r1': candidates['predecessor-r1'],
        'failed-launch-r2': candidates['failed-launch-r2'],
    }
    get_candidates.assert_called_once_with('service')
    get_owners.assert_called_once_with()


def test_child_only_purge_termination_failure_retains_inventory():
    lifecycle_lock = mock.MagicMock(epoch=9)
    replica_infos = [
        mock.Mock(replica_id=1,
                  replica_record_id='00000000-0000-4000-8000-000000000001',
                  cluster_name='orphan-r1',
                  status_property=types.SimpleNamespace(sky_down_status=None)),
        mock.Mock(replica_id=2,
                  replica_record_id='00000000-0000-4000-8000-000000000002',
                  cluster_name='orphan-r2',
                  status_property=types.SimpleNamespace(sky_down_status=None)),
    ]

    def _terminate(cluster_name, **_kwargs):
        if cluster_name == 'orphan-r2':
            raise RuntimeError('down failed')

    def _reserve(_service_name, candidates, **_kwargs):
        candidate_ids = {replica_id for replica_id, _ in candidates}
        reserved = {}
        for info in replica_infos:
            if info.replica_id not in candidate_ids:
                continue
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.RUNNING)
            reserved[info.replica_id] = info
        return reserved

    with mock.patch.object(serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=lifecycle_lock), \
         mock.patch.object(serve_utils,
                           'lifecycle_lock_is_valid', return_value=True), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hash', return_value=None), \
         mock.patch.object(serve_state,
                           'get_orphaned_service_child_mode',
                           return_value=True), \
         mock.patch.object(serve_state,
                           'get_replica_infos', return_value=replica_infos), \
         mock.patch.object(
             serve_utils,
             'quiesce_service_replica_launch_requests', return_value=True), \
         mock.patch.object(serve_state,
                           'get_ephemeral_storage_cleanup_intents',
                           return_value=[]), \
         mock.patch.object(
             serve_utils.global_user_state,
             'get_cluster_status_fields',
             return_value={
                 info.cluster_name: (None, None) for info in replica_infos
             }), \
         mock.patch.object(
             serve_state,
             'get_replica_resource_action_identities',
             return_value={info.replica_id: None for info in replica_infos}), \
         mock.patch.object(serve_state,
                           'add_or_update_replica', return_value=True), \
         mock.patch.object(
             serve_state,
             'reserve_replica_teardowns_running_if_capacity',
             side_effect=_reserve), \
         mock.patch.object(
             serve_state,
             'restore_never_started_replica_teardown_to_scheduled'), \
         mock.patch('sky.serve.replica_managers.terminate_cluster',
                    side_effect=_terminate) as terminate, \
         mock.patch.object(serve_state,
                           'remove_orphaned_service_children') as remove:
        message = serve_utils._terminate_orphaned_service_children_impl(
            'orphan', True)

    assert message is not None and 'cluster termination failed' in message
    assert terminate.call_count == 2
    remove.assert_not_called()


def test_all_down_uses_controller_distributed_lifecycle_fence(tmp_path):
    """The controller-side ``--all`` path must serialize with update."""
    record = {
        'name': 'svc',
        'status': serve_state.ServiceStatus.READY,
        'pool': False,
        'hash': 'incarnation-a',
    }
    lifecycle_lock = mock.MagicMock()
    lifecycle_lock.epoch = 23
    signal_template = str(tmp_path / '{}.signal')
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=record) as get_status, \
         mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=lifecycle_lock) as get_lock, \
         mock.patch.object(serve_utils,
                           'lifecycle_lock_is_valid',
                           return_value=True), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_hash',
             return_value=True) as set_status, \
         mock.patch.object(constants, 'SIGNAL_FILE_PATH', signal_template):
        message = serve_utils.terminate_services(None, purge=False, pool=False)

    assert 'scheduled to be terminated' in message
    get_lock.assert_called_once_with('svc', advance_epoch=False)
    get_status.assert_called_once()  # initial classification only
    set_status.assert_called_once_with('svc',
                                       'incarnation-a',
                                       serve_state.ServiceStatus.SHUTTING_DOWN,
                                       expected_lifecycle_epoch=23)


@pytest.mark.parametrize('bound_mode', [True, False])
def test_down_uses_atomic_begin_or_unsupported_legacy_fallback(
        tmp_path, bound_mode):
    """Bound down cannot queue behind its request's provider retry guard."""
    record = {
        'name': 'svc',
        'status': serve_state.ServiceStatus.READY,
        'pool': False,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }
    lifecycle_lock = mock.MagicMock(epoch=23)
    authority = object() if bound_mode else None
    teardown_result = ordinary_launch_binding.ServiceTeardownResult(
        (ordinary_launch_binding.ServiceTeardownDisposition.MARKED_BOUND
         if bound_mode else
         ordinary_launch_binding.ServiceTeardownDisposition.UNSUPPORTED),
        authority)
    signal_template = str(tmp_path / '{}.signal')
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=record), \
         mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=lifecycle_lock), \
         mock.patch.object(serve_utils,
                           'lifecycle_lock_is_valid',
                           return_value=True), \
         mock.patch(
             'sky.serve.ordinary_launch_binding.'
             'begin_service_teardown_if_owner',
             return_value=teardown_result) as begin_teardown, \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_hash',
             return_value=True) as legacy_set_status, \
         mock.patch.object(constants, 'SIGNAL_FILE_PATH', signal_template):
        message = serve_utils.terminate_services(['svc'],
                                                 purge=False,
                                                 pool=False)

    assert 'scheduled to be terminated' in message
    begin_teardown.assert_called_once_with('svc', 'incarnation-a',
                                           (123, '10.0.0.1'))
    if bound_mode:
        legacy_set_status.assert_not_called()
    else:
        legacy_set_status.assert_called_once_with(
            'svc',
            'incarnation-a',
            serve_state.ServiceStatus.SHUTTING_DOWN,
            expected_lifecycle_epoch=23)


class TestPoolStatusBatchedQuery:
    """`_get_service_status(pool=True)` must batch its per-replica job lookups
    into a single grouped query. The previous per-replica fan-out scaled with
    pool replica count and ran a full scan over the job_info table
    each iteration.
    """

    def _replica(self, name, status):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state
        info = mock.Mock()
        # cluster_name is what _get_service_status reads when building the
        # batched names list. Setting it as a real attribute prevents
        # MagicMock from returning a separate Mock per access.
        info.cluster_name = name
        info.to_info_dict.return_value = {
            'name': name,
            'status': (status if isinstance(status, serve_state.ReplicaStatus)
                       else serve_state.ReplicaStatus[status]),
        }
        return info

    def _patch_environment(self,
                           replicas,
                           grouped_jobs,
                           cluster_records=None,
                           job_status_counts=None):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state

        record = {
            'name': 'pool-a',
            'pool': True,
            'version': 1,
            'controller_port': 20001,
        }
        # Default: every replica's cluster row exists. Tests that want to
        # simulate "dead" replicas can override by passing cluster_records
        # with explicit None values.
        if cluster_records is None:
            cluster_records = {
                r.cluster_name: {
                    'launched_at': 0,
                    'handle': None,
                } for r in replicas
            }
        return (
            mock.patch(
                'sky.serve.serve_utils.serve_state.get_service_from_name',
                return_value=record),
            mock.patch('sky.serve.serve_utils.serve_state.get_replica_infos',
                       return_value=replicas),
            mock.patch('sky.serve.serve_utils._get_to_controller_with_retry',
                       side_effect=requests_exceptions.RequestException()),
            mock.patch('sky.serve.serve_utils.get_yaml_content',
                       side_effect=Exception('skip yaml')),
            mock.patch(
                'sky.serve.serve_utils.managed_job_state.'
                'get_nonterminal_job_status_counts_by_pool',
                return_value=job_status_counts or {}),
            mock.patch(
                'sky.serve.serve_utils.managed_job_state.'
                'get_nonterminal_job_ids_by_pool_grouped',
                return_value=grouped_jobs),
            mock.patch('sky.serve.serve_utils.managed_job_state.'
                       'get_nonterminal_job_ids_by_pool'),
            mock.patch(
                'sky.serve.serve_utils.global_user_state.'
                'get_clusters_from_names',
                return_value=cluster_records),
            serve_state,  # returned for callers to use as needed
        )

    def test_pool_status_uses_grouped_query_once(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state

        replicas = [
            self._replica('replica-1', serve_state.ReplicaStatus.READY),
            self._replica('replica-2', serve_state.ReplicaStatus.READY),
            self._replica('replica-3', serve_state.ReplicaStatus.PROVISIONING),
        ]
        # job 10: batch coordinator (no cluster_name) — should appear on all
        # READY replicas.
        # jobs 20, 21: bound to replica-1 — must not leak to replica-2.
        # job 30: bound to replica-2 — must not leak to replica-1.
        grouped_jobs = {
            None: [10],
            'replica-1': [20, 21],
            'replica-2': [30],
        }
        (svc_patch, replica_patch, ctrl_patch, yaml_patch, counts_patch,
         grouped_patch, legacy_patch, clusters_patch,
         _) = self._patch_environment(replicas, grouped_jobs)
        with svc_patch, replica_patch, ctrl_patch, yaml_patch, \
             counts_patch as mock_counts, grouped_patch as mock_grouped, \
             legacy_patch as mock_legacy, \
             clusters_patch:
            record = serve_utils._get_service_status('pool-a', pool=True)

        assert record is not None
        # Exactly one DB round-trip — no N+1.
        mock_counts.assert_called_once_with('pool-a')
        mock_grouped.assert_called_once_with('pool-a')
        mock_legacy.assert_not_called()

        used_by = {r['name']: r['used_by'] for r in record['replica_info']}
        # READY workers see (pool-level coordinator jobs) + (their own slice).
        # They must NOT see jobs bound to other replicas: that was a latent
        # bug in master where every READY worker reported every nonterminal
        # job in the pool.
        assert used_by['replica-1'] == [10, 20, 21]
        assert used_by['replica-2'] == [10, 30]
        # Non-READY workers only see jobs assigned to them; replica-3 has none.
        assert used_by['replica-3'] == []

    def test_pool_status_non_ready_only_sees_own_jobs(self):
        """A non-READY replica with assigned jobs sees only those, not the
        rest of the pool."""
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state

        replicas = [
            self._replica('replica-init',
                          serve_state.ReplicaStatus.PROVISIONING),
        ]
        grouped_jobs = {
            None: [1],
            'replica-init': [2, 3],
            'replica-other': [4],
        }
        (svc_patch, replica_patch, ctrl_patch, yaml_patch, counts_patch,
         grouped_patch, legacy_patch, clusters_patch,
         _) = self._patch_environment(replicas, grouped_jobs)
        with svc_patch, replica_patch, ctrl_patch, yaml_patch, \
             counts_patch, grouped_patch, legacy_patch, clusters_patch:
            record = serve_utils._get_service_status('pool-a', pool=True)

        assert record is not None
        used_by = {r['name']: r['used_by'] for r in record['replica_info']}
        assert used_by['replica-init'] == [2, 3]

    def test_pool_status_uses_batched_cluster_lookups(self):
        """The per-replica `get_cluster_from_name` call inside to_info_dict
        used to dominate pool_status latency on pools with long failure
        history. `_get_service_status` now pre-fetches all records in one
        batched call and passes them through to to_info_dict.

        There is no separate handle-fallback round-trip: handle is just a
        column on the same cluster_table row, so when ``cluster_record`` is
        None the handle is also None and we skip ``self.handle()`` entirely.
        """
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state

        replicas = [
            self._replica(f'r-{i}', serve_state.ReplicaStatus.READY)
            for i in range(5)
        ]
        # First three "alive" (cluster row exists), last two "dead" (cluster
        # row gone). The dead ones must not trigger any extra DB lookups.
        cluster_records = {
            'r-0': {
                'launched_at': 1,
                'handle': None
            },
            'r-1': {
                'launched_at': 2,
                'handle': None
            },
            'r-2': {
                'launched_at': 3,
                'handle': None
            },
            'r-3': None,
            'r-4': None,
        }
        (svc_patch, replica_patch, ctrl_patch, yaml_patch, counts_patch,
         grouped_patch, legacy_patch, clusters_patch,
         _) = self._patch_environment(replicas, {None: []},
                                      cluster_records=cluster_records)
        # No mock for get_handles_from_cluster_names — the test fails if
        # _get_service_status reintroduces a redundant call to it.
        with mock.patch('sky.serve.serve_utils.global_user_state.'
                        'get_handles_from_cluster_names') as mock_handles:
            with svc_patch, replica_patch, ctrl_patch, yaml_patch, \
                 counts_patch, grouped_patch, legacy_patch, \
                 clusters_patch as mock_clusters:
                record = serve_utils._get_service_status('pool-a', pool=True)

        assert record is not None
        # Batched cluster lookup happens exactly once and gets every name.
        mock_clusters.assert_called_once()
        passed_names = mock_clusters.call_args.args[0]
        assert sorted(passed_names) == [f'r-{i}' for i in range(5)]
        # Handle lookup must not be reintroduced: missing cluster_record
        # implies missing handle, so the second batched call would be a
        # guaranteed-empty waste.
        mock_handles.assert_not_called()
        # to_info_dict was called per replica with the pre-fetched record
        # supplied, so it must not re-fetch on its own.
        for replica_mock in replicas:
            replica_mock.to_info_dict.assert_called_once()
            kwargs = replica_mock.to_info_dict.call_args.kwargs
            assert 'cluster_record' in kwargs

    def test_pool_status_no_handles_lookup_call(self):
        """All replicas alive: there should never be a fallback handle
        batched query (deleted by design)."""
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state

        replicas = [
            self._replica('r-0', serve_state.ReplicaStatus.READY),
            self._replica('r-1', serve_state.ReplicaStatus.READY),
        ]
        cluster_records = {
            'r-0': {
                'launched_at': 1,
                'handle': None
            },
            'r-1': {
                'launched_at': 2,
                'handle': None
            },
        }
        (svc_patch, replica_patch, ctrl_patch, yaml_patch, counts_patch,
         grouped_patch, legacy_patch, clusters_patch,
         _) = self._patch_environment(replicas, {None: []},
                                      cluster_records=cluster_records)
        with mock.patch('sky.serve.serve_utils.global_user_state.'
                        'get_handles_from_cluster_names') as mock_handles:
            with svc_patch, replica_patch, ctrl_patch, yaml_patch, \
                 counts_patch, grouped_patch, legacy_patch, clusters_patch:
                serve_utils._get_service_status('pool-a', pool=True)

        # The handle-fallback batched query was removed entirely.
        mock_handles.assert_not_called()

    def test_pool_status_includes_grouped_job_status_counts(self):
        # pylint: disable=import-outside-toplevel
        from sky.serve import serve_state

        replicas = [self._replica('replica-1', serve_state.ReplicaStatus.READY)]
        job_status_counts = {'RUNNING': 2, 'PENDING': 1}
        (svc_patch, replica_patch, ctrl_patch, yaml_patch, counts_patch,
         grouped_patch, legacy_patch, clusters_patch,
         _) = self._patch_environment(replicas, {None: []},
                                      job_status_counts=job_status_counts)

        with svc_patch, replica_patch, ctrl_patch, yaml_patch, \
             counts_patch as mock_counts, grouped_patch, legacy_patch, \
             clusters_patch:
            record = serve_utils._get_service_status('pool-a', pool=True)

        assert record is not None
        mock_counts.assert_called_once_with('pool-a')
        assert record['job_status_counts'] == job_status_counts


class TestServiceStatusEndpointSnapshot:
    """Full service status should reuse the batched cluster snapshot for
    endpoint resolution."""

    def _replica(self, name):
        # pylint: disable=import-outside-toplevel
        from sky.serve import replica_managers

        info = replica_managers.ReplicaInfo(replica_id=int(name.split('-')[-1]),
                                            cluster_name=name,
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        info.status_property.to_replica_status = lambda: (serve_state.
                                                          ReplicaStatus.READY)
        handle = mock.MagicMock()
        handle.launched_resources = None
        info.handle = mock.Mock(return_value=handle)
        return info, handle

    def _v2_replica(self, name):
        # pylint: disable=import-outside-toplevel
        from sky import backends
        from sky.serve import replica_managers
        from sky.serve import reserved_capacity_broker

        info = replica_managers.ReplicaInfo(replica_id=int(name.split('-')[-1]),
                                            cluster_name=name,
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        info.status_property.sky_launch_status = (
            replica_managers.common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.service_ready_now = True
        info.status_property.first_ready_time = 1.0
        info.reserved_fill = True
        info.reserved_fill_pool_key = reserved_capacity_broker.make_pool_key(
            'phx-context',
            'H200',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='physical-uid')
        info.reserved_fill_service_generation = 7
        info.reserved_fill_physical_cluster_uid = 'physical-uid'
        info.reserved_fill_kubernetes_context = 'phx-context'
        info.location = {
            'cloud': 'Kubernetes',
            'region': 'phx-context',
            'zone': None,
            'accelerators': {
                'H200': 1,
            },
        }
        info.resources_override = {
            'cloud': 'Kubernetes',
            'region': 'phx-context',
            'accelerators': {
                'H200': 1,
            },
        }
        handle = mock.Mock(spec=backends.CloudVmRayResourceHandle)
        handle.cluster_name = name
        handle.cluster_yaml = '/tmp/phx.yaml'
        handle.launched_resources = Resources(
            cloud=clouds.Kubernetes(),
            instance_type=('4CPU--16GB--H200:1'),
            region='phx-context',
            accelerators={'H200': 1})
        handle.launched_nodes = 1
        return info, handle

    def test_summary_reports_logical_and_physical_capacity_counts(self):
        service_record = {
            'name': 'svc-a',
            'pool': False,
            'hash': 'incarnation-a',
            'logical_replica_semantics': True,
        }
        expected = {
            'replica_unit': 'logical_slot',
            # The controller's observed router capacity wins over persisted
            # planned capacity for the live ready count.
            'ready_replicas': 7,
            'total_replicas': 12,
            'failed_replicas': 4,
            'physical_ready_replicas': 1,
            'physical_total_replicas': 2,
            'physical_failed_replicas': 1,
        }
        response = mock.Mock()
        response.json.return_value = {
            'target_num_replicas': 1,
            'ready_replicas': 7,
            'report_age_seconds': 4.0,
        }
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_service_from_name',
                return_value=service_record), \
             mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_replica_status_and_capacity_counts',
                        return_value=({
                            'READY': 1,
                            'PROVISIONING': 1,
                            'FAILED_PROVISION': 1,
                        }, {
                            'READY': 8,
                            'PROVISIONING': 4,
                            'FAILED_PROVISION': 4,
                        })), \
             mock.patch('sky.serve.serve_utils.'
                        '_get_to_controller_with_retry',
                        return_value=response), \
             mock.patch('sky.serve.serve_utils.demand_state.'
                        'get_request_summary',
                        return_value=demand_state.unavailable_request_summary(
                            'test_controller_capacity')):
            status = serve_utils._get_service_status(
                'svc-a',
                pool=False,
                with_replica_info=False,
                with_replica_counts=True,
                with_yaml=False,
                with_target_num_replicas=True)

        assert status is not None
        for key, value in expected.items():
            assert status[key] == value
        assert status['observed_ready_replicas_fresh'] is True
        assert status['observed_ready_replicas_age_seconds'] == 4.0
        assert status['request_stats_age_seconds'] is None

    def test_stale_observed_logical_capacity_does_not_replace_replica_state(
            self):
        service_record = {
            'name': 'svc-a',
            'pool': False,
            'hash': 'incarnation-a',
            'logical_replica_semantics': True,
        }
        response = mock.Mock()
        response.json.return_value = {
            'target_num_replicas': 0,
            'ready_replicas': 262,
            'report_age_seconds': 700.0,
        }
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_service_from_name',
                return_value=service_record), \
             mock.patch('sky.serve.serve_utils.serve_state.'
                        'get_replica_status_and_capacity_counts',
                        return_value=({
                            'READY': 62,
                            'PROVISIONING': 2,
                        }, {
                            'READY': 62,
                            'PROVISIONING': 2,
                        })), \
             mock.patch('sky.serve.serve_utils.'
                        '_get_to_controller_with_retry',
                        return_value=response), \
             mock.patch('sky.serve.serve_utils.demand_state.'
                        'get_request_summary',
                        return_value={
                            **demand_state.unavailable_request_summary(
                                'test_controller_capacity'),
                            'request_telemetry_state': 'fresh',
                            'request_telemetry_reason': 'complete',
                            'request_telemetry_compatibility_complete': True,
                            'request_reporter_count': 1,
                            'request_stats_age_seconds': 1.0,
                        }):
            status = serve_utils._get_service_status(
                'svc-a',
                pool=False,
                with_replica_info=False,
                with_replica_counts=True,
                with_yaml=False,
                with_target_num_replicas=True)

        assert status is not None
        assert status['ready_replicas'] == 62
        assert status['total_replicas'] == 64
        assert status['observed_ready_replicas'] == 262
        assert status['observed_ready_replicas_fresh'] is False
        assert status['observed_ready_replicas_age_seconds'] == 700.0
        assert status['request_stats_age_seconds'] == 1.0

    def test_service_status_propagates_reserved_fill_reconciliation(self):
        service_record = {
            'name': 'svc-a',
            'pool': False,
            'hash': 'incarnation-a',
        }
        reconciliation = {
            'enabled': True,
            'authority_mode': 'sequenced',
            'allocation_current': True,
            'allocation_generation': 5,
            'allocation_input_sha256': 'a' * 64,
            'allocation_claim_generation': 11,
            'pools': {},
        }
        response = mock.Mock()
        response.json.return_value = {
            'target_num_replicas': 1,
            'reserved_fill_reconciliation': reconciliation,
        }
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_service_from_name',
                return_value=service_record), \
             mock.patch('sky.serve.serve_utils.'
                        '_get_to_controller_with_retry',
                        return_value=response), \
             mock.patch('sky.serve.serve_utils.demand_state.'
                        'get_request_summary',
                        return_value=demand_state.unavailable_request_summary(
                            'test_controller_capacity')):
            status = serve_utils._get_service_status(
                'svc-a',
                pool=False,
                with_replica_info=False,
                with_yaml=False,
                with_target_num_replicas=True)

        assert status is not None
        assert status['reserved_fill_reconciliation'] == reconciliation

    def test_service_status_reuses_batched_cluster_snapshot_for_endpoints(self):
        replicas_and_handles = [self._replica(f'r-{i}') for i in (1, 2)]
        replicas = [info for info, _ in replicas_and_handles]
        cluster_records = {
            info.cluster_name: {
                'launched_at': idx,
                'handle': handle,
            } for idx, (info, handle) in enumerate(replicas_and_handles, start=1)
        }
        endpoint_calls = []

        def _get_endpoints(cluster, port, **kwargs):
            endpoint_calls.append((cluster, port, kwargs))
            return {port: f'{cluster}.svc:{port}'}

        record = {
            'name': 'svc-a',
            'pool': False,
            'version': 1,
        }
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_service_from_name',
                return_value=record), \
             mock.patch('sky.serve.serve_utils.serve_state.get_replica_infos',
                        return_value=replicas), \
             mock.patch('sky.serve.serve_utils.global_user_state.'
                        'get_clusters_from_names',
                        return_value=cluster_records) as mock_clusters, \
             mock.patch('sky.serve.replica_managers.backend_utils.'
                        'get_endpoints',
                        side_effect=_get_endpoints):
            status = serve_utils._get_service_status(
                'svc-a', pool=False, with_target_num_replicas=False)

        assert status is not None
        mock_clusters.assert_called_once_with(['r-1', 'r-2'])
        assert [replica['endpoint'] for replica in status['replica_info']
               ] == ['http://r-1.svc:8080', 'http://r-2.svc:8080']
        assert [
            replica['planned_capacity'] for replica in status['replica_info']
        ] == [1, 1]
        assert endpoint_calls == [
            ('r-1', 8080, {
                'cluster_record': cluster_records['r-1']
            }),
            ('r-2', 8080, {
                'cluster_record': cluster_records['r-2']
            }),
        ]
        for info, _ in replicas_and_handles:
            info.handle.assert_called_once_with(
                cluster_records[info.cluster_name])

    def test_service_status_tolerates_missing_cluster_record_in_snapshot(self):
        replicas_and_handles = [self._replica(f'r-{i}') for i in (1, 2)]
        replicas = [info for info, _ in replicas_and_handles]
        cluster_records = {
            replicas[0].cluster_name: {
                'launched_at': 1,
                'handle': replicas_and_handles[0][1],
            },
        }
        endpoint_calls = []

        def _get_endpoints(cluster, port, **kwargs):
            endpoint_calls.append((cluster, port, kwargs))
            return {port: f'{cluster}.svc:{port}'}

        record = {
            'name': 'svc-a',
            'pool': False,
            'version': 1,
        }
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_service_from_name',
                return_value=record), \
             mock.patch('sky.serve.serve_utils.serve_state.get_replica_infos',
                        return_value=replicas), \
             mock.patch('sky.serve.serve_utils.global_user_state.'
                        'get_clusters_from_names',
                        return_value=cluster_records) as mock_clusters, \
             mock.patch('sky.serve.replica_managers.global_user_state.'
                        'get_cluster_from_name',
                        side_effect=AssertionError(
                            'per-replica cluster reread used')), \
             mock.patch('sky.serve.replica_managers.backend_utils.'
                        'get_endpoints',
                        side_effect=_get_endpoints):
            status = serve_utils._get_service_status(
                'svc-a',
                pool=False,
                with_yaml=False,
                with_target_num_replicas=False)

        assert status is not None
        mock_clusters.assert_called_once_with(['r-1', 'r-2'])
        assert [replica['endpoint'] for replica in status['replica_info']
               ] == ['http://r-1.svc:8080', None]
        assert endpoint_calls == [
            ('r-1', 8080, {
                'cluster_record': cluster_records['r-1']
            }),
        ]
        replicas[0].handle.assert_called_once_with(cluster_records['r-1'])
        replicas[1].handle.assert_not_called()

    def test_v2_status_group_uses_one_uid_read_for_all_replicas(self):
        replicas_and_handles = [
            self._v2_replica(f'r-{index}') for index in (1, 2)
        ]
        replicas = [info for info, _ in replicas_and_handles]
        cluster_records = {
            info.cluster_name: {
                'name': info.cluster_name,
                'launched_at': index,
                'handle': handle,
            } for index, (info,
                         handle) in enumerate(replicas_and_handles, start=1)
        }
        depth = 0
        uid_reads = 0

        @contextlib.contextmanager
        def _physical_fence(context, physical_uid):
            nonlocal depth, uid_reads
            assert (context, physical_uid) == ('phx-context', 'physical-uid')
            if depth == 0:
                uid_reads += 1
            depth += 1
            try:
                yield
            finally:
                depth -= 1

        record = {'name': 'svc-a', 'pool': False, 'version': 1}
        with mock.patch.object(serve_state,
                               'get_service_from_name',
                               return_value=record), \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=replicas), \
             mock.patch.object(serve_utils.global_user_state,
                               'get_clusters_from_names',
                               return_value=cluster_records), \
             mock.patch(
                 'sky.adaptors.kubernetes.physical_cluster_uid_fence',
                 side_effect=_physical_fence), \
             mock.patch('sky.backends.backend_utils.get_endpoints',
                        return_value={8080: '10.0.0.1:8080'}):
            status = serve_utils._get_service_status(
                'svc-a', pool=False, with_target_num_replicas=False)

        assert status is not None
        assert uid_reads == 1
        assert [item['endpoint'] for item in status['replica_info']] == [
            'http://10.0.0.1:8080',
            'http://10.0.0.1:8080',
        ]

    def test_v2_pool_status_group_still_proves_uid_once(self):
        replicas_and_handles = [
            self._v2_replica(f'r-{index}') for index in (1, 2)
        ]
        replicas = [info for info, _ in replicas_and_handles]
        for info in replicas:
            info.replica_port = '-'
        cluster_records = {
            info.cluster_name: {
                'name': info.cluster_name,
                'launched_at': index,
                'handle': handle,
            } for index, (info,
                         handle) in enumerate(replicas_and_handles, start=1)
        }
        depth = 0
        uid_reads = 0

        @contextlib.contextmanager
        def _physical_fence(context, physical_uid):
            nonlocal depth, uid_reads
            assert (context, physical_uid) == ('phx-context', 'physical-uid')
            if depth == 0:
                uid_reads += 1
            depth += 1
            try:
                yield
            finally:
                depth -= 1

        record = {'name': 'pool-a', 'pool': True, 'version': 1}
        with mock.patch.object(serve_state,
                               'get_service_from_name',
                               return_value=record), \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=replicas), \
             mock.patch.object(serve_utils.global_user_state,
                               'get_clusters_from_names',
                               return_value=cluster_records), \
             mock.patch(
                 'sky.adaptors.kubernetes.physical_cluster_uid_fence',
                 side_effect=_physical_fence), \
             mock.patch('sky.backends.backend_utils.get_endpoints') as endpoint, \
             mock.patch.object(
                 serve_utils.managed_job_state,
                 'get_nonterminal_job_status_counts_by_pool',
                 return_value={}), \
             mock.patch.object(
                 serve_utils.managed_job_state,
                 'get_nonterminal_job_ids_by_pool_grouped',
                 return_value={}):
            status = serve_utils._get_service_status(
                'pool-a',
                pool=True,
                with_yaml=False,
                with_target_num_replicas=False)

        assert status is not None
        assert uid_reads == 1
        assert [item['endpoint'] for item in status['replica_info']
               ] == [None, None]
        endpoint.assert_not_called()

    def test_v2_status_uid_mismatch_omits_replacement_provider_data(self):
        info, handle = self._v2_replica('r-1')
        cluster_record = {
            'name': info.cluster_name,
            'launched_at': 9,
            'handle': handle,
        }
        provider_fence = mock.MagicMock()
        provider_fence.return_value.__enter__.side_effect = (
            exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))
        record = {'name': 'svc-a', 'pool': False, 'version': 1}

        with mock.patch.object(serve_state,
                               'get_service_from_name',
                               return_value=record), \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_utils.global_user_state,
                               'get_clusters_from_names',
                               return_value={info.cluster_name: cluster_record}), \
             mock.patch(
                 'sky.adaptors.kubernetes.physical_cluster_uid_fence',
                 provider_fence), \
             mock.patch('sky.backends.backend_utils.get_endpoints') as endpoint:
            status = serve_utils._get_service_status(
                'svc-a', pool=False, with_target_num_replicas=False)

        assert status is not None
        replica = status['replica_info'][0]
        assert replica['status'] is serve_state.ReplicaStatus.UNKNOWN
        assert replica['endpoint'] is None
        assert replica['handle'] is None
        assert replica['launched_at'] is None
        assert replica['provider_identity_uncertain'] is True
        assert replica['cloud'] == 'Kubernetes'
        assert 'hourly_cost' not in replica
        endpoint.assert_not_called()

    def test_malformed_v2_status_is_unknown_without_provider_admission(self):
        info, handle = self._v2_replica('r-1')
        info.reserved_fill_kubernetes_context = 'contradictory-context'
        cluster_record = {
            'name': info.cluster_name,
            'launched_at': 9,
            'handle': handle,
        }
        record = {'name': 'svc-a', 'pool': False, 'version': 1}

        with mock.patch.object(serve_state,
                               'get_service_from_name',
                               return_value=record), \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(
                 serve_utils.global_user_state,
                 'get_clusters_from_names',
                 return_value={info.cluster_name: cluster_record}), \
             mock.patch.object(serve_utils.provider_phase,
                               'provider_phase') as phase, \
             mock.patch(
                 'sky.adaptors.kubernetes.physical_cluster_uid_fence') as uid, \
             mock.patch('sky.backends.backend_utils.get_endpoints') as endpoint:
            status = serve_utils._get_service_status(
                'svc-a',
                pool=False,
                with_yaml=False,
                with_target_num_replicas=False)

        assert status is not None
        replica = status['replica_info'][0]
        assert replica['status'] is serve_state.ReplicaStatus.UNKNOWN
        assert replica['provider_identity_uncertain'] is True
        assert replica['endpoint'] is None
        assert replica['handle'] is None
        assert replica['launched_at'] is None
        # Durable placement remains useful and is not replacement-provider
        # evidence; live endpoint/handle/cost metadata stays absent.
        assert replica['cloud'] == 'Kubernetes'
        assert 'hourly_cost' not in replica
        phase.assert_not_called()
        uid.assert_not_called()
        endpoint.assert_not_called()


class TestTerminalStatuses:
    """`terminal_statuses` includes SHUTTING_DOWN so that callers like
    apply() can refuse to update a row that's either dying or already
    broken (CONTROLLER_FAILED / FAILED_CLEANUP / SHUTTING_DOWN)."""

    def test_includes_shutting_down(self):
        statuses = serve_state.ServiceStatus.terminal_statuses()
        assert serve_state.ServiceStatus.SHUTTING_DOWN in statuses
        assert serve_state.ServiceStatus.FAILED_CLEANUP in statuses
        assert serve_state.ServiceStatus.CONTROLLER_FAILED in statuses
        # Healthy states must NOT be in here, otherwise apply() would refuse
        # to update healthy pools.
        assert serve_state.ServiceStatus.READY not in statuses
        assert serve_state.ServiceStatus.CONTROLLER_INIT not in statuses


class TestServiceReplicaSummary:
    """Service headers prefer public capacity but support old servers."""

    def test_prefers_authoritative_logical_capacity(self):
        assert serve_utils._get_replicas({
            'ready_replicas': 8,
            'total_replicas': 12,
            'replica_info': [{
                'status': serve_state.ReplicaStatus.READY,
            }],
        }) == '8/12'

    def test_old_server_falls_back_to_physical_rows(self):
        assert serve_utils._get_replicas({
            'replica_info': [{
                'status': serve_state.ReplicaStatus.READY,
            }, {
                'status': serve_state.ReplicaStatus.PROVISIONING,
            }, {
                'status': serve_state.ReplicaStatus.FAILED_PROVISION,
            }],
        }) == '1/2'


class TestStreamReplicaLogsPhysicalIdentityFence:

    def test_remote_tail_runs_inside_exact_replica_fence(self, tmp_path):

        class _FakeHandle:
            pass

        launch_log = tmp_path / 'replica_1_launch.log'
        launch_log.write_text('launch complete\n')
        info = types.SimpleNamespace(replica_id=1,
                                     cluster_name='replica-cluster',
                                     status=serve_state.ReplicaStatus.READY)
        handle = _FakeHandle()
        entered = False
        phase_entered = False

        @contextlib.contextmanager
        def _phase():
            nonlocal phase_entered
            phase_entered = True
            try:
                yield mock.sentinel.admission
            finally:
                phase_entered = False

        @contextlib.contextmanager
        def _fence():
            nonlocal entered
            assert phase_entered
            entered = True
            try:
                yield
            finally:
                entered = False

        backend = mock.Mock()

        def _tail_logs(*args, **kwargs):
            del args, kwargs
            assert entered, 'remote log command escaped its physical fence'
            return (0, 'remote output\n')

        backend.tail_logs.side_effect = _tail_logs
        with mock.patch(
                'sky.serve.serve_utils._get_healthy_service_log_owner_record',
                return_value=({
                    'pool': True,
                    'resource_scope': None,
                    'status': serve_state.ServiceStatus.READY,
                }, None)), \
             mock.patch(
                 'sky.serve.serve_utils.generate_replica_launch_log_file_name',
                 return_value=str(launch_log)), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_replica_info_from_id',
                 return_value=info), \
             mock.patch(
                 'sky.serve.serve_utils.global_user_state.'
                 'get_handle_from_cluster_name',
                 return_value=handle), \
             mock.patch.object(serve_utils.backends,
                               'CloudVmRayResourceHandle', _FakeHandle), \
             mock.patch.object(serve_utils.backends,
                               'CloudVmRayBackend', return_value=backend), \
             mock.patch(
                 'sky.serve.reserved_capacity.parse_protocol_v2_cleanup_fence',
                 return_value=mock.sentinel.cleanup_fence), \
             mock.patch(
                 'sky.serve.reserved_capacity.protocol_v2_provider_fence',
                 return_value=_fence()) as provider_fence, \
             mock.patch.object(
                 serve_utils.provider_phase,
                 'provider_phase', return_value=_phase()) as phase:
            serve_utils.stream_replica_logs('svc',
                                            replica_id=1,
                                            follow=False,
                                            tail=1,
                                            pool=True)

        provider_fence.assert_called_once_with(info,
                                               handle,
                                               include_provider_phase=False)
        phase.assert_called_once_with(
            serve_utils.provider_phase.ProviderPhaseMode.V2_FENCED)
        backend.tail_logs.assert_called_once_with(handle,
                                                  job_id=None,
                                                  follow=False,
                                                  tail=1,
                                                  stream_logs=False,
                                                  require_outputs=True,
                                                  process_stream=True)

    def test_interactive_v2_follow_holds_fence_without_provider_phase(
            self, tmp_path):

        class _FakeHandle:
            pass

        launch_log = tmp_path / 'replica_1_launch.log'
        launch_log.write_text('launch complete\n')
        info = types.SimpleNamespace(replica_id=1,
                                     cluster_name='replica-cluster',
                                     status=serve_state.ReplicaStatus.READY)
        handle = _FakeHandle()
        fence_entered = False

        @contextlib.contextmanager
        def _fence():
            nonlocal fence_entered
            fence_entered = True
            try:
                yield
            finally:
                fence_entered = False

        backend = mock.Mock()

        def _tail_logs(*_args, **_kwargs):
            assert fence_entered
            return 0

        backend.tail_logs.side_effect = _tail_logs
        with mock.patch(
                'sky.serve.serve_utils._get_healthy_service_log_owner_record',
                return_value=({
                    'pool': True,
                    'resource_scope': None,
                    'status': serve_state.ServiceStatus.READY,
                }, None)), \
             mock.patch(
                 'sky.serve.serve_utils.generate_replica_launch_log_file_name',
                 return_value=str(launch_log)), \
             mock.patch(
                 'sky.serve.serve_utils._follow_logs_with_provision_expanding',
                 return_value=iter(())), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_replica_info_from_id',
                 return_value=info), \
             mock.patch(
                 'sky.serve.serve_utils.global_user_state.'
                 'get_handle_from_cluster_name',
                 return_value=handle), \
             mock.patch.object(serve_utils.backends,
                               'CloudVmRayResourceHandle', _FakeHandle), \
             mock.patch.object(serve_utils.backends,
                               'CloudVmRayBackend', return_value=backend), \
             mock.patch(
                 'sky.serve.reserved_capacity.parse_protocol_v2_cleanup_fence',
                 return_value=mock.sentinel.cleanup_fence), \
             mock.patch(
                 'sky.serve.reserved_capacity.protocol_v2_provider_fence',
                 return_value=_fence()), \
             mock.patch.object(serve_utils.provider_phase,
                               'provider_phase') as phase:
            result = serve_utils.stream_replica_logs('svc',
                                                     replica_id=1,
                                                     follow=True,
                                                     tail=None,
                                                     pool=True)

        assert result == ''
        phase.assert_not_called()
        backend.tail_logs.assert_called_once_with(handle,
                                                  job_id=None,
                                                  follow=True)


class TestStartInFlight:
    """`_start_in_flight` is the defense-in-depth dedup used by
    `ha_recovery_for_consolidation_mode` to avoid firing a duplicate
    recovery script while a previously-spawned _start is still in its
    0-60s boot window (during which DB controller_pid may still be the
    stale pre-recovery value).
    """

    def _make_proc(self, cmdline, status='running'):
        """Helper: build a mock proc whose .info dict matches what
        psutil.process_iter yields with the attrs list."""
        proc = mock.Mock()
        proc.info = {'cmdline': cmdline, 'status': status}
        return proc

    def test_returns_true_when_start_process_present(self):
        proc = self._make_proc([
            'python', '-m', 'sky.serve.service', '--service-name', 'pool-a',
            '--job-id', '5'
        ])
        with mock.patch('sky.serve.serve_utils.psutil.process_iter',
                        return_value=[proc]):
            assert serve_utils._start_in_flight('pool-a') is True

    def test_returns_false_when_no_matching_process(self):
        proc = self._make_proc([
            'python', '-m', 'sky.serve.service', '--service-name', 'other-pool'
        ])
        with mock.patch('sky.serve.serve_utils.psutil.process_iter',
                        return_value=[proc]):
            assert serve_utils._start_in_flight('pool-a') is False

    def test_returns_false_when_no_processes(self):
        with mock.patch('sky.serve.serve_utils.psutil.process_iter',
                        return_value=[]):
            assert serve_utils._start_in_flight('pool-a') is False

    def test_skips_zombie_processes(self):
        """A zombie `_start` (crashed but not yet reaped by init) must NOT
        block recovery — otherwise the pool would be stuck in a
        permanent "recovery in flight" loop until something reaps the
        zombie, which may never happen in pods without a proper init.
        """
        # pylint: disable=import-outside-toplevel
        import psutil
        zombie = self._make_proc(
            ['python', '-m', 'sky.serve.service', '--service-name', 'pool-a'],
            status=psutil.STATUS_ZOMBIE)
        with mock.patch('sky.serve.serve_utils.psutil.process_iter',
                        return_value=[zombie]):
            assert serve_utils._start_in_flight('pool-a') is False

    def test_swallows_per_process_psutil_errors(self):
        """If iterating one process raises NoSuchProcess / AccessDenied
        (race with proc exit), the iteration must continue and check
        the rest, not raise.
        """
        # pylint: disable=import-outside-toplevel
        import psutil

        # First proc raises on attribute access; second one is a legit match.
        bad = mock.Mock()
        type(bad).info = mock.PropertyMock(
            side_effect=psutil.NoSuchProcess(99999))
        good = self._make_proc(
            ['python', '-m', 'sky.serve.service', '--service-name', 'pool-a'])
        with mock.patch('sky.serve.serve_utils.psutil.process_iter',
                        return_value=[bad, good]):
            assert serve_utils._start_in_flight('pool-a') is True

    def test_no_prefix_false_positive(self):
        """Service name 'pool-a' must NOT match a process for 'pool-abc'.
        The implementation does exact argv-list matching on the
        `--service-name <value>` pair (not substring on a joined string),
        so the prefix collision is rejected.
        """
        proc = self._make_proc(
            ['python', '-m', 'sky.serve.service', '--service-name', 'pool-abc'])
        with mock.patch('sky.serve.serve_utils.psutil.process_iter',
                        return_value=[proc]):
            assert serve_utils._start_in_flight('pool-a') is False

    def test_no_service_name_arg_returns_false(self):
        """A `sky.serve.service` process with no `--service-name` flag
        at all (defensive case, shouldn't happen in practice) must not
        match — index() returning ValueError is caught."""
        proc = self._make_proc([
            'python', '-m', 'sky.serve.service', '--some-other-flag', 'whatever'
        ])
        with mock.patch('sky.serve.serve_utils.psutil.process_iter',
                        return_value=[proc]):
            assert serve_utils._start_in_flight('pool-a') is False

    def test_controller_liveness_requires_exact_scoped_incarnation(self):
        proc = mock.Mock()
        proc.is_running.return_value = True
        proc.cmdline.return_value = [
            'python', '-m', 'sky.serve.service', '--service-name', 'svc',
            '--service-incarnation', 'incarnation-a'
        ]
        with mock.patch('sky.serve.serve_utils.psutil.Process',
                        return_value=proc):
            assert serve_utils._controller_process_alive(123,
                                                         'svc',
                                                         'incarnation-a',
                                                         allow_legacy=False)
            assert not serve_utils._controller_process_alive(
                123, 'svc', 'incarnation-b', allow_legacy=False)

    def test_scoped_row_rejects_legacy_name_only_process(self):
        proc = mock.Mock()
        proc.is_running.return_value = True
        proc.cmdline.return_value = [
            'python', '-m', 'sky.serve.service', '--service-name', 'svc'
        ]
        with mock.patch('sky.serve.serve_utils.psutil.Process',
                        return_value=proc):
            assert not serve_utils._controller_process_alive(
                123, 'svc', 'incarnation-b', allow_legacy=False)
            assert serve_utils._controller_process_alive(123,
                                                         'svc',
                                                         'legacy-hash',
                                                         allow_legacy=True)


class TestHaRecoverySkipsWhenStartInFlight:
    """When the in-flight snapshot contains the service name,
    `ha_recovery_for_consolidation_mode` must NOT invoke the recovery
    script for that service this round. Otherwise the daemon piles up
    multiple `_start` instances during the 0-60s controller boot window.
    """

    def test_skip_when_start_in_flight(self, tmp_path, monkeypatch):
        monkeypatch.setenv('POD_IP', '10.4.0.1')
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_glob_service_names',
                return_value=['pool-a']), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_service_liveness_snapshots',
                 return_value=[{'name': 'pool-a',
                                'controller_pid': 1234,
                                'controller_ip': '10.4.0.1',
                                'status': 'READY',
                                'yaml_content': 'yaml: v1'}]), \
             mock.patch(
                 'sky.serve.serve_utils._controller_process_alive',
                 return_value=False), \
             mock.patch(
                 'sky.serve.serve_utils.'
                 '_snapshot_in_flight_start_service_incarnations',
                 return_value={('pool-a', None)}) as mock_snapshot, \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_ha_recovery_script',
                 return_value='dummy script') as mock_script, \
             mock.patch(
                 'sky.serve.serve_utils.command_runner.'
                 'LocalProcessCommandRunner') as mock_runner_cls, \
             mock.patch(
                 'sky.serve.serve_utils.skylet_constants.'
                 'HA_PERSISTENT_RECOVERY_LOG_PATH',
                 str(tmp_path / 'recovery_log_{}.log')):
            serve_utils.ha_recovery_for_consolidation_mode(pool=True)
            # Snapshot is taken once per daemon iteration, not once per
            # service, so it's exactly one call regardless of N services.
            assert mock_snapshot.call_count == 1
            # Script lookup AND runner.run must be skipped — we bailed
            # before reaching either.
            mock_script.assert_not_called()
            mock_runner_cls.return_value.run.assert_not_called()

    def test_stale_incarnation_process_does_not_suppress_successor_recovery(
            self, tmp_path):
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_glob_service_names',
                return_value=['svc']), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_service_liveness_snapshots',
                 return_value=[{
                     'name': 'svc',
                     'hash': 'incarnation-b',
                     'resource_scope': 'incarnation-b',
                     'controller_pid': 1234,
                     'controller_ip': '10.4.0.1',
                     'status': 'READY',
                     'yaml_content': 'yaml: v1',
                 }]), \
             mock.patch(
                 'sky.serve.serve_utils._controller_process_alive',
                 return_value=False), \
             mock.patch(
                 'sky.serve.serve_utils.'
                 '_snapshot_in_flight_start_service_incarnations',
                 return_value={('svc', 'incarnation-a')}), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_ha_recovery_script',
                 return_value='dummy script'), \
             mock.patch(
                 'sky.serve.serve_utils.command_runner.'
                 'LocalProcessCommandRunner') as runner_cls, \
             mock.patch(
                 'sky.serve.serve_utils.skylet_constants.'
                 'HA_PERSISTENT_RECOVERY_LOG_PATH',
                 str(tmp_path / 'recovery_log_{}.log')):
            runner_cls.return_value.run.return_value = (0, '', '')
            serve_utils.ha_recovery_for_consolidation_mode(pool=False)

        runner_cls.return_value.run.assert_called_once_with(
            'dummy script', require_outputs=True)


class TestHaRecoveryUsesSingleLivenessSnapshot:
    """The sweep must issue ONE slim snapshot query for the whole iteration
    instead of a per-service joined read (`_get_service_status` →
    `get_service_from_name`), which re-joins version_specs and deserializes
    the latest spec once per service every ~20s daemon tick."""

    def test_no_per_service_joined_reads(self, tmp_path, monkeypatch):
        monkeypatch.setenv('POD_IP', '10.4.0.1')
        names = [f'svc-{i}' for i in range(5)]
        records = [{
            'name': name,
            'controller_pid': None,
            'controller_ip': '10.4.0.1',
            'status': 'READY',
            'hash': f'{name}-hash',
            'resource_scope': f'{name}-hash',
            'yaml_content': 'yaml: v1',
        } for name in names]
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_glob_service_names',
                return_value=names), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_service_liveness_snapshots',
                 return_value=records) as snapshot, \
             mock.patch(
                 'sky.serve.serve_utils._get_service_status') as joined, \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_service_from_name'
                 ) as from_name, \
             mock.patch(
                 'sky.serve.serve_utils.'
                 '_snapshot_in_flight_start_service_incarnations',
                 return_value=set()), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_ha_recovery_script',
                 return_value='dummy script'), \
             mock.patch(
                 'sky.serve.serve_utils.command_runner.'
                 'LocalProcessCommandRunner') as runner_cls, \
             mock.patch(
                 'sky.serve.serve_utils.skylet_constants.'
                 'HA_PERSISTENT_RECOVERY_LOG_PATH',
                 str(tmp_path / 'recovery_log_{}.log')):
            runner_cls.return_value.run.return_value = (0, '', '')
            serve_utils.ha_recovery_for_consolidation_mode(pool=True)

        # One snapshot for N services; the joined per-service read never runs.
        snapshot.assert_called_once_with(pool=True)
        joined.assert_not_called()
        from_name.assert_not_called()
        assert runner_cls.return_value.run.call_count == len(names)


class TestHaRecoveryRetiresUnbootableRows:

    def test_missing_committed_version_is_marked_for_purge(
            self, tmp_path, monkeypatch):
        monkeypatch.setenv('POD_IP', '10.4.0.1')
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_glob_service_names',
                return_value=['svc']), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_service_liveness_snapshots',
                 return_value=[]), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_latest_committed_versions',
                 return_value={}), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_service_mode_and_hashes',
                 return_value={
                     'svc': (False, 'incarnation-a')
                 }), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'mark_unrecoverable_service_for_cleanup',
                 return_value=True) as mark, \
             mock.patch(
                 'sky.serve.serve_utils.'
                 '_snapshot_in_flight_start_service_incarnations',
                 return_value=set()), \
             mock.patch(
                 'sky.serve.serve_utils.command_runner.'
                 'LocalProcessCommandRunner') as runner_cls, \
             mock.patch(
                 'sky.serve.serve_utils.skylet_constants.'
                 'HA_PERSISTENT_RECOVERY_LOG_PATH',
                 str(tmp_path / 'recovery_log_{}.log')):
            serve_utils.ha_recovery_for_consolidation_mode(pool=False)

        mark.assert_called_once_with('svc', 'incarnation-a', False)
        runner_cls.return_value.run.assert_not_called()


def test_ha_recovery_retires_raw_row_with_no_committed_version(tmp_path):
    """HA must stop preserving a recovery script that cannot ever boot."""
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[]), \
         mock.patch.object(serve_state,
                           'get_latest_committed_versions', return_value={}), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hashes',
                           return_value={
                               'svc': (True, 'orphan-hash')
                           }), \
         mock.patch.object(
             serve_state,
             'mark_unrecoverable_service_for_cleanup',
             return_value=True) as retire, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls:
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    retire.assert_called_once_with('svc', 'orphan-hash', True)
    runner_cls.return_value.run.assert_not_called()
    assert 'marked it for purge' in (tmp_path / 'recovery_pool_.log').read_text(
        encoding='utf-8')


def test_ha_recovery_retires_placeholder_without_committed_version(tmp_path):
    """A NULL-yaml version row is visible to the join but cannot boot."""
    placeholder = {
        'name': 'svc',
        'yaml_content': None,
        'controller_pid': None,
        'controller_ip': None,
        'hash': 'placeholder-hash',
        'resource_scope': 'placeholder-hash',
        'status': serve_state.ServiceStatus.CONTROLLER_INIT,
    }
    with mock.patch.object(serve_state,
                           'get_glob_service_names', return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[placeholder]), \
         mock.patch.object(serve_state,
                           'get_latest_committed_versions', return_value={}), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hashes') as identities, \
         mock.patch.object(
             serve_state,
             'mark_unrecoverable_service_for_cleanup',
             return_value=True) as retire, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls:
        serve_utils.ha_recovery_for_consolidation_mode(pool=False)

    identities.assert_not_called()
    retire.assert_called_once_with('svc', 'placeholder-hash', False)
    runner_cls.return_value.run.assert_not_called()


def test_ha_recovery_batches_placeholder_and_raw_identity_fallback_reads(
        tmp_path):
    placeholder = {
        'name': 'placeholder-svc',
        'yaml_content': None,
        'controller_pid': None,
        'controller_ip': None,
        'hash': 'placeholder-hash',
        'resource_scope': 'placeholder-hash',
        'status': serve_state.ServiceStatus.CONTROLLER_INIT,
    }
    with mock.patch.object(
            serve_state,
            'get_glob_service_names',
            return_value=['orphan-svc', 'placeholder-svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[placeholder]), \
         mock.patch.object(
             serve_state,
             'get_latest_committed_versions',
             return_value={}) as committed_versions, \
         mock.patch.object(
             serve_state,
             'get_service_mode_and_hashes',
             return_value={
                 'orphan-svc': (True, 'orphan-hash'),
                 'placeholder-svc': (False, 'placeholder-hash'),
             }) as identities, \
         mock.patch.object(
             serve_state,
             'mark_unrecoverable_service_for_cleanup',
             return_value=True) as retire, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls:
        serve_utils.ha_recovery_for_consolidation_mode(pool=False)

    committed_versions.assert_called_once_with(
        ['orphan-svc', 'placeholder-svc'])
    identities.assert_called_once_with(['orphan-svc'])
    retire.assert_called_once_with('placeholder-svc', 'placeholder-hash', False)
    runner_cls.return_value.run.assert_not_called()


def test_ha_recovery_preserves_placeholder_with_committed_version(tmp_path):
    placeholder = {
        'name': 'svc',
        'yaml_content': None,
        'controller_pid': None,
        'controller_ip': None,
        'hash': 'incarnation-a',
        'resource_scope': 'incarnation-a',
        'status': serve_state.ServiceStatus.CONTROLLER_INIT,
    }
    with mock.patch.object(serve_state,
                           'get_glob_service_names', return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[placeholder]), \
         mock.patch.object(
             serve_state,
             'get_latest_committed_versions',
             return_value={
                 'svc': 1
             }) as committed_versions, \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hashes') as identities, \
         mock.patch.object(
             serve_state,
             'mark_unrecoverable_service_for_cleanup') as retire, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(serve_state,
                           'get_ha_recovery_script',
                           return_value='recover'), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls:
        runner_cls.return_value.run.return_value = (0, '', '')
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    committed_versions.assert_called_once_with(['svc'])
    identities.assert_not_called()
    retire.assert_not_called()
    runner_cls.return_value.run.assert_called_once_with('recover',
                                                        require_outputs=True)


class TestHaRecoveryDefensiveOnAliveCheckException:
    """`ha_recovery_for_consolidation_mode` calls `_controller_process_alive`
    to decide whether to respawn the controller. If that call raises a
    transient psutil exception (AccessDenied / cmdline read race / etc.),
    the previous code FELL THROUGH to running the recovery script —
    effectively replacing a possibly-alive controller every iteration that
    hit the exception. The fix is to skip recovery for that round and
    revisit next iteration.
    """

    def test_skip_when_alive_check_raises(self, tmp_path, monkeypatch):
        # pylint: disable=import-outside-toplevel
        import psutil

        # Pretend pool 'svc' has a controller_pid recorded; alive check
        # raises AccessDenied (transient). Recovery script must NOT run.
        monkeypatch.setenv('POD_IP', '10.4.0.1')
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_glob_service_names',
                return_value=['svc']), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_service_liveness_snapshots',
                 return_value=[{'name': 'svc',
                                'controller_pid': 1234,
                                'controller_ip': '10.4.0.1',
                                'status': 'READY',
                                'yaml_content': 'yaml: v1'}]), \
             mock.patch(
                 'sky.serve.serve_utils._controller_process_alive',
                 side_effect=psutil.AccessDenied(1234)) as mock_alive, \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_ha_recovery_script',
                 return_value='dummy script') as mock_script, \
             mock.patch(
                 'sky.serve.serve_utils.command_runner.'
                 'LocalProcessCommandRunner') as mock_runner_cls, \
             mock.patch(
                 'sky.serve.serve_utils.skylet_constants.'
                 'HA_PERSISTENT_RECOVERY_LOG_PATH',
                 str(tmp_path / 'recovery_log_{}.log')):
            serve_utils.ha_recovery_for_consolidation_mode(pool=True)
            # alive was probed
            assert mock_alive.called
            # recovery script lookup or run must NOT happen — we skipped early
            mock_script.assert_not_called()
            mock_runner_cls.return_value.run.assert_not_called()


class _FakeReplicaInfo:
    """Minimal stand-in exposing the fields the status computation reads."""

    def __init__(self, status, version):
        self.status = status
        self.version = version

    @property
    def is_ready(self):
        return self.status == serve_state.ReplicaStatus.READY


def test_ha_recovery_retires_placeholder_through_liveness_snapshot_identity(
        _mock_serve_db, tmp_path):
    """The joined liveness snapshot carries the same ``services.hash`` the
    raw identity reread used to fetch, so a NULL-yaml placeholder retires
    through the snapshot with zero raw identity reads.  Only a raw orphan
    (no version row at all) still needs the fallback read, and a placeholder
    whose row has no hash is never retired by either path."""
    with orm.Session(_mock_serve_db) as session:
        for name, service_hash in (('placeholder', 'placeholder-hash'),
                                   ('hashless', None)):
            session.execute(serve_state.services_table.insert().values(
                name=name,
                controller_job_id=1,
                status=serve_state.ServiceStatus.CONTROLLER_INIT.value,
                requested_resources_str='1x[CPU:1+]',
                pool=0,
                controller_pid=None,
                hash=service_hash,
                entrypoint='entry'))
            session.execute(serve_state.version_specs_table.insert().values(
                service_name=name,
                version=1,
                spec=serve_state.pickle.dumps(
                    types.SimpleNamespace(min_replicas=1)),
                yaml_content=None))
        session.commit()
    _insert_orphan_service_row(_mock_serve_db, 'orphan')

    with mock.patch.object(
            serve_state,
            'get_service_mode_and_hashes',
            wraps=serve_state.get_service_mode_and_hashes) as identities, \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner') as runner_cls:
        serve_utils.ha_recovery_for_consolidation_mode(pool=False)

    identities.assert_called_once_with(['orphan'])
    runner_cls.return_value.run.assert_not_called()
    with orm.Session(_mock_serve_db) as session:
        statuses = dict(
            session.execute(
                sqlalchemy.select(serve_state.services_table.c.name,
                                  serve_state.services_table.c.status)).all())
    assert statuses == {
        'placeholder': serve_state.ServiceStatus.FAILED_CLEANUP.value,
        'orphan': serve_state.ServiceStatus.FAILED_CLEANUP.value,
        'hashless': serve_state.ServiceStatus.CONTROLLER_INIT.value,
    }


@pytest.mark.parametrize(
    'replica_statuses,expected_service_status',
    [
        # Replicas exist but none ready/failed -> REPLICA_INIT.
        ([
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING
        ], serve_state.ServiceStatus.REPLICA_INIT),
        # Some replica failed, none ready -> FAILED.
        ([
            serve_state.ReplicaStatus.FAILED,
            serve_state.ReplicaStatus.PROVISIONING
        ], serve_state.ServiceStatus.FAILED),
        # No replicas at all -> NO_REPLICA.
        ([], serve_state.ServiceStatus.NO_REPLICA),
        # A ready replica wins over a failed one -> READY.
        ([serve_state.ReplicaStatus.FAILED, serve_state.ReplicaStatus.READY
         ], serve_state.ServiceStatus.READY),
    ])
def test_set_service_status_from_replica_uses_all_replicas(
        replica_statuses, expected_service_status):
    """Service status must be derived from ALL replicas, not only READY ones.

    Feeding only ready replicas into ServiceStatus.from_replica_statuses makes
    FAILED and REPLICA_INIT unreachable (any non-empty input contains READY),
    so a fully-failed or still-initializing service would misreport as
    NO_REPLICA.
    """
    replica_infos = [
        _FakeReplicaInfo(status, version=1) for status in replica_statuses
    ]
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        serve_utils.set_service_status_and_active_versions_from_replica(
            'svc', replica_infos, serve_utils.UpdateMode.ROLLING)
    set_st.assert_called_once()
    assert set_st.call_args.args[4] == expected_service_status
    assert set_st.call_args.kwargs['expected_status'] == (
        serve_state.ServiceStatus.READY)


def test_set_service_status_from_replica_active_versions_ready_only():
    """active_versions must still come from the READY replicas only."""
    replica_infos = [
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.PROVISIONING, version=3),
    ]
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        serve_utils.set_service_status_and_active_versions_from_replica(
            'svc', replica_infos, serve_utils.UpdateMode.ROLLING)
    set_st.assert_called_once()
    assert set_st.call_args.args[4] == serve_state.ServiceStatus.READY
    assert set_st.call_args.kwargs['active_versions'] == [2]


@pytest.mark.parametrize(
    ('replica_statuses', 'target_num_replicas', 'expected_service_status'), [
        ([serve_state.ReplicaStatus.FAILED_PROVISION
         ], 0, serve_state.ServiceStatus.NO_REPLICA),
        ([
            serve_state.ReplicaStatus.FAILED,
            serve_state.ReplicaStatus.FAILED_INITIAL_DELAY,
            serve_state.ReplicaStatus.FAILED_PROBING,
            serve_state.ReplicaStatus.FAILED_PROVISION,
        ], 0, serve_state.ServiceStatus.FAILED),
        ([serve_state.ReplicaStatus.FAILED
         ], 0, serve_state.ServiceStatus.FAILED),
        ([serve_state.ReplicaStatus.FAILED_INITIAL_DELAY
         ], 0, serve_state.ServiceStatus.FAILED),
        ([serve_state.ReplicaStatus.FAILED_PROBING
         ], 0, serve_state.ServiceStatus.FAILED),
        ([serve_state.ReplicaStatus.FAILED_PROVISION
         ], 1, serve_state.ServiceStatus.FAILED),
        ([serve_state.ReplicaStatus.FAILED_PROVISION
         ], None, serve_state.ServiceStatus.FAILED),
        ([serve_state.ReplicaStatus.FAILED_CLEANUP
         ], 0, serve_state.ServiceStatus.FAILED),
        ([serve_state.ReplicaStatus.UNKNOWN
         ], 0, serve_state.ServiceStatus.FAILED),
        ([
            serve_state.ReplicaStatus.READY,
            serve_state.ReplicaStatus.FAILED_PROVISION,
        ], 0, serve_state.ServiceStatus.READY),
        ([
            serve_state.ReplicaStatus.FAILED_PROVISION,
            serve_state.ReplicaStatus.PROVISIONING,
        ], 0, serve_state.ServiceStatus.FAILED),
    ])
def test_set_service_status_from_replica_distinguishes_idle_failure_history(
        replica_statuses, target_num_replicas, expected_service_status):
    replica_infos = [
        _FakeReplicaInfo(status, version=1) for status in replica_statuses
    ]
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        serve_utils.set_service_status_and_active_versions_from_replica(
            'svc',
            replica_infos,
            serve_utils.UpdateMode.ROLLING,
            target_num_replicas=target_num_replicas)

    set_st.assert_called_once()
    assert set_st.call_args.args[4] == expected_service_status


def test_idle_failed_provision_status_transition_is_persisted(_mock_serve_db):
    _insert_orphan_service_row(_mock_serve_db, 'svc-idle')
    _insert_version_spec(_mock_serve_db, 'svc-idle', 1, min_replicas=0)
    replica_infos = [
        _FakeReplicaInfo(serve_state.ReplicaStatus.FAILED_PROVISION, version=1)
    ]

    serve_utils.set_service_status_and_active_versions_from_replica(
        'svc-idle',
        replica_infos,
        serve_utils.UpdateMode.ROLLING,
        target_num_replicas=0)
    assert serve_state.get_service_controller_owner(
        'svc-idle')['status'] == serve_state.ServiceStatus.NO_REPLICA

    serve_utils.set_service_status_and_active_versions_from_replica(
        'svc-idle',
        replica_infos,
        serve_utils.UpdateMode.ROLLING,
        target_num_replicas=1)
    assert serve_state.get_service_controller_owner(
        'svc-idle')['status'] == serve_state.ServiceStatus.FAILED


def test_get_latest_version_with_min_replicas_batches_spec_reads(
        _mock_serve_db):
    _insert_version_spec(_mock_serve_db, 'svc', 1, min_replicas=1)
    _insert_version_spec(_mock_serve_db, 'svc', 2, min_replicas=2)
    _insert_version_spec(_mock_serve_db, 'svc', 3, min_replicas=4)
    replica_infos = [
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=1),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
    ]

    with _count_sql_statements(_mock_serve_db) as counts:
        chosen = serve_utils.get_latest_version_with_min_replicas(
            'svc', replica_infos)

    assert chosen == 2
    assert counts['n'] == 1, counts


def test_get_latest_version_with_min_replicas_falls_back_to_oldest_ready(
        _mock_serve_db):
    _insert_version_spec(_mock_serve_db, 'svc', 1, min_replicas=2)
    _insert_version_spec(_mock_serve_db, 'svc', 2, min_replicas=4)
    replica_infos = [
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=1),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
    ]

    chosen = serve_utils.get_latest_version_with_min_replicas(
        'svc', replica_infos)

    assert chosen == 1


def test_set_service_status_from_replica_blue_green_uses_chosen_version(
        _mock_serve_db):
    _insert_version_spec(_mock_serve_db, 'svc', 1, min_replicas=1)
    _insert_version_spec(_mock_serve_db, 'svc', 2, min_replicas=3)
    _insert_version_spec(_mock_serve_db, 'svc', 3, min_replicas=5)
    replica_infos = [
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=2),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
        _FakeReplicaInfo(serve_state.ReplicaStatus.READY, version=3),
        _FakeReplicaInfo(serve_state.ReplicaStatus.PROVISIONING, version=4),
    ]
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        serve_utils.set_service_status_and_active_versions_from_replica(
            'svc', replica_infos, serve_utils.UpdateMode.BLUE_GREEN)

    set_st.assert_called_once()
    assert set_st.call_args.args[4] == serve_state.ServiceStatus.READY
    assert set_st.call_args.kwargs['active_versions'] == [2]


def test_stale_controller_cannot_authenticate_status_as_replacement_owner():
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'incarnation-a',
        'controller_pid': 200,
        'controller_ip': '10.0.0.2',
    }
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        serve_utils.set_service_status_and_active_versions_from_replica(
            'svc', [],
            serve_utils.UpdateMode.ROLLING,
            expected_service_hash='incarnation-a',
            expected_controller_owner=(100, '10.0.0.1'))
    set_st.assert_not_called()


def test_versionless_status_writer_rejects_orphan_in_one_query(_mock_serve_db):
    _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')

    with _count_sql_statements(_mock_serve_db) as counts:
        with pytest.raises(ValueError, match='old version'):
            serve_utils.set_service_status_and_active_versions_from_replica(
                'svc-orphan', [], serve_utils.UpdateMode.ROLLING)

    assert counts['n'] == 1, counts


def test_replica_status_writer_cannot_erase_interleaved_shutdown():
    db_record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }
    read_complete = threading.Event()
    resume = threading.Event()

    def _read(_service_name, require_version=False):
        assert require_version
        snapshot = dict(db_record)
        read_complete.set()
        assert resume.wait(timeout=2)
        return snapshot

    def _cas(_name, expected_hash, expected_pid, expected_ip, status, **kwargs):
        if (db_record['hash'] != expected_hash or
                db_record['controller_pid'] != expected_pid or
                db_record['controller_ip'] != expected_ip or
                db_record['status'] != kwargs['expected_status']):
            return False
        db_record['status'] = status
        return True

    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           side_effect=_read), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner',
             side_effect=_cas):
        writer = threading.Thread(
            target=serve_utils.
            set_service_status_and_active_versions_from_replica,
            args=('svc', [], serve_utils.UpdateMode.ROLLING))
        writer.start()
        assert read_complete.wait(timeout=2)
        db_record['status'] = serve_state.ServiceStatus.SHUTTING_DOWN
        resume.set()
        writer.join(timeout=2)

    assert not writer.is_alive()
    assert db_record['status'] == serve_state.ServiceStatus.SHUTTING_DOWN


class TestTerminateFailedServices:
    """`_terminate_failed_services` must terminate replica clusters that
    still exist only after exact-owner controller teardown acknowledgement and
    BEFORE deleting their DB rows.

    Once the child is durably gone, no down thread will run for its replicas.
    Deleting the rows without terminating the clusters permanently orphaned
    them: nothing referenced the clusters anymore, so they kept billing until
    manually downed.
    """

    def _run(self,
             replica_infos,
             exists,
             terminate_side_effect=None,
             lb_side_effect=None,
             resource_scope=None,
             teardown_identities=None,
             bound_authority=None,
             bound_settle_side_effect=None,
             quiesce_side_effect=None,
             exact_absence=False):
        terminated = []
        self.termination_kwargs = []

        def _terminate(cluster_name, **kwargs):
            terminated.append(cluster_name)
            self.termination_kwargs.append(kwargs)
            if terminate_side_effect is not None:
                terminate_side_effect(cluster_name)

        def _reserve_cleanup(_service_name, candidates, **_kwargs):
            candidate_ids = {replica_id for replica_id, _ in candidates}
            reserved = {}
            for info in replica_infos:
                if info.replica_id not in candidate_ids:
                    continue
                info.status_property.sky_down_status = (
                    common_utils.ProcessStatus.RUNNING)
                reserved[info.replica_id] = info
            return reserved

        self.exact_terminations = []

        def _terminate_exact(*args, **kwargs):
            self.exact_terminations.append((args, kwargs))

        self.cluster_snapshot_calls = []

        def _cluster_snapshot(cluster_names):
            self.cluster_snapshot_calls.append(list(cluster_names))
            return {
                cluster_name: (None, None)
                for cluster_name in cluster_names
                if exists(cluster_name)
            }

        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17
        teardown_result = ordinary_launch_binding.ServiceTeardownResult(
            (ordinary_launch_binding.ServiceTeardownDisposition.MARKED_BOUND
             if bound_authority is not None else
             ordinary_launch_binding.ServiceTeardownDisposition.UNSUPPORTED),
            bound_authority)
        # CPython lowers a multi-item ``with`` into nested blocks and rejects
        # this safety harness once it exceeds the static nesting limit. Keep
        # the teardown boundary explicit without weakening any mock.
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.get_replica_infos',
                    return_value=replica_infos))
            quiesce = stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.'
                    'quiesce_service_replica_launch_requests',
                    return_value=True,
                    side_effect=quiesce_side_effect))
            stack.enter_context(
                mock.patch(
                    'sky.serve.ordinary_launch_binding.'
                    'begin_service_teardown_if_owner',
                    return_value=teardown_result))
            settle_bound = stack.enter_context(
                mock.patch(
                    'sky.serve.service.'
                    '_settle_bound_ordinary_launches_for_teardown',
                    side_effect=bound_settle_side_effect,
                    return_value=types.SimpleNamespace(
                        provider_present_cleanup_contexts={},
                        provider_reconciliation_failures={})))
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.global_user_state.'
                    'get_cluster_status_fields',
                    side_effect=_cluster_snapshot))
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'get_replica_resource_action_identities',
                    side_effect=lambda _service_name, replica_ids: ({
                        replica_id: None for replica_id in replica_ids
                    } if teardown_identities is None else teardown_identities)))
            stack.enter_context(
                mock.patch('sky.serve.replica_managers.terminate_cluster',
                           side_effect=_terminate))
            stack.enter_context(
                mock.patch(
                    'sky.serve.replica_managers.'
                    'terminate_bound_non_pool_provider_present_cluster',
                    side_effect=_terminate_exact))
            stack.enter_context(
                mock.patch('sky.serve.serve_utils.get_service_lifecycle_lock',
                           return_value=lifecycle_lock))
            stack.enter_context(
                mock.patch('sky.serve.serve_utils.lifecycle_lock_is_valid',
                           return_value=True))
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.service_owner_matches',
                    return_value=True))
            persist = stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'add_or_update_replica',
                    return_value=True))
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'reserve_replica_teardowns_running_if_capacity',
                    side_effect=_reserve_cleanup))
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'restore_never_started_replica_teardown_to_scheduled'))
            exact_probe = stack.enter_context(
                mock.patch(
                    'sky.serve.kueue_lane_observer.'
                    'project_exact_pod_absence_after_teardown',
                    return_value=exact_absence))
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'set_service_status_and_active_versions_if_hash',
                    return_value=True))
            set_owner_status = stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'set_service_status_and_active_versions_if_owner',
                    return_value=True))
            stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'get_service_controller_owner',
                    return_value={
                        'hash': 'incarnation-a',
                        'resource_scope': resource_scope,
                        'controller_pid': 101,
                        'controller_ip': '10.0.0.1',
                        'controller_port':
                            constants.CONTROLLER_TEARDOWN_ACK_PORT,
                    }))
            remove_service = stack.enter_context(
                mock.patch(
                    'sky.serve.serve_utils.serve_state.'
                    'remove_service_completely',
                    return_value=True))
            remove_directory = stack.enter_context(
                mock.patch('sky.serve.serve_utils.remove_service_directory'))
            stack.enter_context(
                mock.patch('sky.serve.lb_k8s.get_api_deployment_owner_uid',
                           return_value='api-deployment-uid'))
            delete_lb = stack.enter_context(
                mock.patch('sky.serve.lb_k8s.delete_lb_objects',
                           side_effect=lb_side_effect))
            result = serve_utils._terminate_failed_services(
                'svc', 'incarnation-a', None)
        self.quiesce = quiesce
        self.settle_bound = settle_bound
        self.persist_replica = persist
        self.exact_absence_probe = exact_probe
        return (terminated, remove_service, delete_lb, result.message,
                set_owner_status, remove_directory)

    @staticmethod
    def _replica(replica_id, cluster_name):
        info = mock.Mock()
        info.replica_id = replica_id
        info.replica_record_id = str(uuid.UUID(int=replica_id))
        info.cluster_name = cluster_name
        return info

    def test_existing_clusters_are_terminated_before_row_removal(self):
        infos = [self._replica(1, 'svc-1'), self._replica(2, 'svc-2')]
        terminated, remove_service, _, message, _, _ = self._run(
            infos, exists=lambda name: name == 'svc-1')
        # Only the still-existing cluster is downed; both rows are removed
        # and the service row is cleared.
        assert terminated == ['svc-1']
        assert self.cluster_snapshot_calls == [['svc-1', 'svc-2']]
        remove_service.assert_called_once_with('svc',
                                               'incarnation-a',
                                               expected_lifecycle_epoch=17)
        assert message is None

    def test_bound_launches_settle_before_generic_quiescence(self):
        events = []
        authority = object()
        info = self._replica(1, 'svc-1')

        def _settle(*_args):
            events.append('exact-settle')
            return types.SimpleNamespace(provider_present_cleanup_contexts={},
                                         provider_reconciliation_failures={})

        def _quiesce(*_args, **_kwargs):
            events.append('generic-quiesce')
            return True

        _, remove_service, _, message, _, _ = self._run(
            [info],
            exists=lambda _name: False,
            bound_authority=authority,
            bound_settle_side_effect=_settle,
            quiesce_side_effect=_quiesce)

        assert message is None
        assert events == ['exact-settle', 'exact-settle', 'generic-quiesce']
        assert self.settle_bound.call_args_list == [
            mock.call(authority, [info]),
            mock.call(authority, [info]),
        ]
        remove_service.assert_called_once()

    def test_provider_present_marker_uses_exact_failed_purge_path(self):
        authority = ordinary_launch_binding.ControllerBindingAuthority(
            service_name='svc',
            service_hash='incarnation-a',
            service_workspace='default',
            service_lifecycle_epoch=17,
            controller_pid=123,
            controller_ip='10.0.0.2',
            controller_incarnation=uuid.UUID(
                '00000000-0000-4000-8000-000000000123'),
            controller_owner_epoch=7,
            capable=True,
            binding_mode=ordinary_launch_binding.BindingMode.BOUND,
            binding_epoch=2,
            non_pool_capable=True,
            non_pool_binding_protocol_version=(
                ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
            non_pool_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            non_pool_capability_cohort_epoch=(
                ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
            non_pool_receipt_protocol_version=(
                ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
        physical_uid = 'physical-cluster-a'
        kubernetes_context = 'on-prem-a'
        pool_key = reserved_capacity_broker.make_pool_key(
            kubernetes_context,
            'L4',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid=physical_uid)
        status = types.SimpleNamespace(
            sky_launch_status=(
                serve_utils.common_utils.ProcessStatus.INTERRUPTED),
            sky_down_status=serve_utils.common_utils.ProcessStatus.SCHEDULED,
            service_ready_now=False,
            is_scale_down=True,
            preempted=False,
            purged=False,
            failed_spot_availability=False,
            wait_for_idle_before_termination=False,
            drain_cap_seconds=0,
            drain_started_at=None,
            logical_retirement_version=None,
            logical_retirement_controller_epoch=None,
            logical_retirement_generation=None,
            logical_retirement_target_capacity=None,
            logical_retirement_confirmed_generation=None,
            logical_retirement_bounded_deadline=False,
            logical_retirement_committed=False)
        record_uuid = uuid.UUID('22222222-2222-4222-8222-222222222222')
        record_id = str(record_uuid)
        profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
            authorization_reference='reserved-fill:test',
            authorization_generation=7,
            authorization_payload={'pool_key': pool_key})
        context = ordinary_launch_binding.BoundNonPoolLaunchContext(
            association_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
            request_id='request-1',
            service_name='svc',
            replica_id=3,
            replica_record_id=record_uuid,
            launch_generation=1,
            input_digest='a' * 64,
            profile=profile,
            capability_cohort_epoch=(
                ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
            capability_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            receipt_protocol_version=(
                ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))
        info = types.SimpleNamespace(
            replica_id=3,
            replica_record_id=record_id,
            cluster_name='svc-3',
            reserved_fill=True,
            reserved_fill_pool_key=pool_key,
            reserved_fill_service_generation=7,
            reserved_fill_physical_cluster_uid=physical_uid,
            reserved_fill_kubernetes_context=kubernetes_context,
            location={
                'cloud': 'Kubernetes',
                'region': kubernetes_context,
                'accelerators': {
                    'L4': 1,
                },
            },
            resources_override={
                'cloud': 'Kubernetes',
                'region': kubernetes_context,
                'accelerators': {
                    'L4': 1,
                },
            },
            is_zero_cost=True,
            service_job_id=None,
            paid_capacity_pool_key=None,
            zero_cost_materialization_sequence=None,
            status_property=status)
        assert (
            ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
                info, require_scheduled=True))

        def _settle(_authority, _infos):
            return types.SimpleNamespace(provider_present_cleanup_contexts={
                (info.replica_id, info.replica_record_id): context
            },
                                         provider_reconciliation_failures={})

        with mock.patch(
                'sky.serve.service.request_postgres.'
                'bound_non_pool_provider_present_cleanup_is_authorized',
                return_value=True):
            terminated, remove_service, _, message, _, _ = self._run(
                [info],
                exists=lambda _name: True,
                bound_authority=authority,
                bound_settle_side_effect=_settle)

        assert message is None
        assert not terminated
        assert len(self.exact_terminations) == 1
        args, kwargs = self.exact_terminations[0]
        assert args[:3] == (context, info, authority)
        assert callable(args[3])
        assert args[4] == info.cluster_name
        assert kwargs['cleanup_fence'] == (
            reserved_capacity.ProtocolV2CleanupFence(
                kubernetes_context=kubernetes_context,
                physical_cluster_uid=physical_uid))
        remove_service.assert_called_once()

    def test_bound_settlement_failure_retains_all_cleanup_rows(self):
        authority = object()
        info = self._replica(1, 'svc-1')
        terminated, remove_service, delete_lb, message, _, _ = self._run(
            [info],
            exists=lambda _name: True,
            bound_authority=authority,
            bound_settle_side_effect=RuntimeError('owner changed'))

        assert message is not None and 'could not be cancelled and settled' in (
            message)
        assert not terminated
        self.quiesce.assert_not_called()
        delete_lb.assert_not_called()
        remove_service.assert_not_called()

    def test_provider_reconciliation_failure_isolated_from_peer_cleanup(self):
        authority = object()
        failed_info = self._replica(1, 'svc-1')
        healthy_info = self._replica(2, 'svc-2')
        failed_key = (failed_info.replica_id, failed_info.replica_record_id)

        def _settle(_authority, _infos):
            return types.SimpleNamespace(
                provider_present_cleanup_contexts={},
                provider_reconciliation_failures={
                    failed_key: 'AWS census remains unproven'
                })

        terminated, remove_service, _, message, _, _ = self._run(
            [failed_info, healthy_info],
            exists=lambda _name: True,
            bound_authority=authority,
            bound_settle_side_effect=_settle)

        assert terminated == ['svc-2']
        assert message is not None and 'some replica clusters' in message
        remove_service.assert_not_called()

    def test_action_owned_cluster_termination_uses_exact_record_uuid(self):
        info = self._replica(1, 'svc-1')
        cluster_record_uuid = uuid.UUID('33333333-3333-4333-8333-333333333333')
        identity = serve_state.ReplicaResourceActionIdentity(
            replica_id=1,
            cluster_name='svc-1',
            replica_incarnation=uuid.UUID(
                '11111111-1111-4111-8111-111111111111'),
            desired_generation=2,
            sky_cluster_record_uuid=cluster_record_uuid)

        _, _, _, message, _, _ = self._run([info],
                                           exists=lambda _name: True,
                                           teardown_identities={1: identity})

        assert message is None
        assert self.termination_kwargs == [{
            'continue_guard': mock.ANY,
            'expected_cluster_record_uuid': str(cluster_record_uuid),
        }]

    def test_large_absent_inventory_uses_one_cluster_snapshot(self):
        infos = [
            self._replica(replica_id, f'svc-{replica_id}')
            for replica_id in range(2159)
        ]
        terminated, remove_service, _, message, _, _ = self._run(
            infos, exists=lambda _name: False)

        assert not terminated
        assert self.cluster_snapshot_calls == [[
            info.cluster_name for info in infos
        ]]
        remove_service.assert_called_once()
        assert message is None

    def test_cluster_inventory_uncertainty_retains_cleanup_rows(self):

        def _inventory_failure(_name):
            raise RuntimeError('cluster DB unavailable')

        infos = [self._replica(1, 'svc-1')]
        terminated, remove_service, _, message, _, _ = self._run(
            infos, exists=_inventory_failure)

        assert not terminated
        remove_service.assert_not_called()
        assert message is not None and 'could not be verified' in message

    def test_unquiesced_launch_keeps_rows_and_clusters_for_retry(self):
        info = self._replica(1, 'svc-1')
        lifecycle_lock = mock.MagicMock(epoch=17)
        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid', return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches', return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(
                 serve_state,
                 'get_service_controller_owner',
                 return_value={
                     'hash': 'incarnation-a',
                     'controller_port': constants.CONTROLLER_TEARDOWN_ACK_PORT,
                     'resource_scope': 'incarnation-a',
                 }), \
             mock.patch.object(serve_state,
                               'get_replica_infos', return_value=[info]), \
             mock.patch.object(
                 serve_utils,
                 'quiesce_service_replica_launch_requests',
                 return_value=False) as quiesce, \
             mock.patch.object(serve_state,
                               'remove_service_completely') as remove, \
             mock.patch.object(serve_utils.global_user_state,
                               'cluster_with_name_exists') as exists:
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert message is not None and 'could not be quiesced' in message
        quiesce.assert_called_once()
        exists.assert_not_called()
        remove.assert_not_called()

    def test_scoped_directory_is_removed_after_final_cas(self):
        (_, _, _, message, _,
         remove_directory) = self._run([],
                                       exists=lambda _name: False,
                                       resource_scope='incarnation-a')
        assert message is None
        remove_directory.assert_called_once()

    def test_external_lb_is_deleted_before_replica_teardown(self):
        events = []
        infos = [self._replica(1, 'svc-1')]

        def _lb_deleted(*_args, **_kwargs):
            events.append('lb-deleted')

        def _replica_terminated(_cluster_name):
            events.append('replica-terminated')

        _, _, _, message, _, _ = self._run(
            infos,
            exists=lambda name: True,
            terminate_side_effect=_replica_terminated,
            lb_side_effect=_lb_deleted)
        assert message is None
        assert events == ['lb-deleted', 'replica-terminated']

    def test_termination_failure_retains_name_and_cleanup_metadata(self):
        infos = [self._replica(1, 'svc-1'), self._replica(2, 'svc-2')]

        def _fail_svc_2(cluster_name):
            if cluster_name == 'svc-2':
                raise RuntimeError('down failed')

        terminated, remove_service, _, message, _, _ = self._run(
            infos, exists=lambda name: True, terminate_side_effect=_fail_svc_2)
        assert sorted(terminated) == ['svc-1', 'svc-2']
        remove_service.assert_not_called()
        assert message is not None and 'could not be purged' in message

    def test_cluster_down_failure_blocks_name_until_retry_succeeds(self):
        infos = [self._replica(1, 'svc-1')]
        attempts = 0

        def _fail_once(_cluster_name):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError('down failed')

        (_, first_remove, first_lb, first_message, first_status,
         first_remove_directory) = self._run(infos,
                                             exists=lambda name: True,
                                             terminate_side_effect=_fail_once)
        assert first_message is not None and 'could not be purged' in (
            first_message)
        first_remove.assert_not_called()
        first_lb.assert_called_once_with(
            'svc',
            expected_service_hash='incarnation-a',
            require_runtime=True,
            expected_api_deployment_uid='api-deployment-uid',
            high_availability=False)
        first_status.assert_called_once()
        assert (first_status.call_args.args[4] ==
                serve_state.ServiceStatus.FAILED_CLEANUP)
        assert first_status.call_args.kwargs['expected_status'] == (
            serve_state.ServiceStatus.SHUTTING_DOWN)
        first_remove_directory.assert_not_called()

        (_, second_remove, _, second_message, _,
         second_remove_directory) = self._run(infos,
                                              exists=lambda name: True,
                                              terminate_side_effect=_fail_once)
        assert second_message is None
        second_remove.assert_called_once_with('svc',
                                              'incarnation-a',
                                              expected_lifecycle_epoch=17)
        # Legacy NULL-scope directories are intentionally retained: their
        # lossy name mapping cannot prove exclusive ownership.
        second_remove_directory.assert_not_called()

    def test_no_existing_clusters_skips_termination(self):
        infos = [self._replica(1, 'svc-1')]
        terminated, remove_service, _, message, _, _ = self._run(
            infos, exists=lambda name: False)
        assert not terminated
        remove_service.assert_called_once_with('svc',
                                               'incarnation-a',
                                               expected_lifecycle_epoch=17)
        assert message is None

    def test_protocol_v2_unproven_absence_retains_parent_and_history_barrier(
            self):
        info = self._replica(1, 'svc-1')
        cleanup_fence = types.SimpleNamespace(kubernetes_context='phx-context',
                                              physical_cluster_uid='phx-uid')
        with mock.patch(
                'sky.serve.reserved_capacity.'
                'parse_protocol_v2_cleanup_fence',
                return_value=cleanup_fence), \
             mock.patch(
                 'sky.serve.reserved_capacity.'
                 'probe_physical_replica_presence',
                 return_value=(reserved_capacity.
                               PhysicalReplicaPresence.UNPROVEN)):
            (terminated, remove_service, _, message, set_owner_status,
             _) = self._run([info], exists=lambda _name: False)

        assert not terminated
        remove_service.assert_not_called()
        assert message is not None and 'could not be purged' in message
        assert set_owner_status.call_args.args[4] == (
            serve_state.ServiceStatus.FAILED_CLEANUP)
        assert self.quiesce.call_args.kwargs['include_terminal_history'] is True

    def test_protocol_v2_provider_absence_removes_parent_and_rows(self):
        info = self._replica(1, 'svc-1')
        cleanup_fence = types.SimpleNamespace(kubernetes_context='phx-context',
                                              physical_cluster_uid='phx-uid')
        with mock.patch(
                'sky.serve.reserved_capacity.'
                'parse_protocol_v2_cleanup_fence',
                return_value=cleanup_fence), \
             mock.patch(
                 'sky.serve.reserved_capacity.'
                 'probe_physical_replica_presence',
                 return_value=(reserved_capacity.
                               PhysicalReplicaPresence.ABSENT)) as probe:
            (terminated, remove_service, _, message, set_owner_status,
             _) = self._run([info], exists=lambda _name: False)

        assert not terminated
        probe.assert_called_once_with(cleanup_fence, info.cluster_name)
        remove_service.assert_called_once_with('svc',
                                               'incarnation-a',
                                               expected_lifecycle_epoch=17)
        set_owner_status.assert_not_called()
        assert message is None
        assert self.quiesce.call_args.kwargs['include_terminal_history'] is True

    def test_protocol_v2_exact_admitted_pod_absence_skips_name_census(self):
        info = self._replica(1, 'svc-1')
        info.replica_record_id = '00000000-0000-4000-8000-000000000001'
        info.status_property = types.SimpleNamespace(sky_down_status=None)
        cleanup_fence = types.SimpleNamespace(kubernetes_context='phx-context',
                                              physical_cluster_uid='phx-uid')
        with mock.patch(
                'sky.serve.reserved_capacity.'
                'parse_protocol_v2_cleanup_fence',
                return_value=cleanup_fence), \
             mock.patch(
                 'sky.serve.reserved_capacity.'
                 'probe_physical_replica_presence') as census:
            _, remove_service, _, message, _, _ = self._run(
                [info], exists=lambda _name: False, exact_absence=True)

        assert message is None
        census.assert_not_called()
        self.exact_absence_probe.assert_called_once_with(
            'svc', info.replica_id, info.replica_record_id)
        self.persist_replica.assert_called_once_with(
            'svc',
            info.replica_id,
            info,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=17,
            expected_controller_owner=(101, '10.0.0.1'),
            expected_replica_exists=True)
        remove_service.assert_called_once()

    def test_orphan_child_partition_keeps_legacy_census_without_service_row(
            self):
        info = self._replica(1, 'svc-1')
        cleanup_fence = types.SimpleNamespace(kubernetes_context='phx-context',
                                              physical_cluster_uid='phx-uid')
        with mock.patch(
                'sky.serve.reserved_capacity.'
                'parse_protocol_v2_cleanup_fence',
                return_value=cleanup_fence), \
             mock.patch(
                 'sky.serve.reserved_capacity.'
                 'probe_physical_replica_presence',
                 return_value=(reserved_capacity.
                               PhysicalReplicaPresence.ABSENT)) as census, \
             mock.patch(
                 'sky.serve.kueue_lane_observer.'
                 'project_exact_pod_absence_after_teardown') as exact_probe:
            to_terminate, unresolved = (
                serve_utils._partition_replica_cleanup_targets([info], set()))

        assert to_terminate == []
        assert unresolved == []
        census.assert_called_once_with(cleanup_fence, info.cluster_name)
        exact_probe.assert_not_called()

    def test_protocol_v2_present_cluster_forwards_exact_cleanup_fence(self):
        info = self._replica(1, 'svc-1')
        cleanup_fence = types.SimpleNamespace(kubernetes_context='phx-context',
                                              physical_cluster_uid='phx-uid')
        with mock.patch(
                'sky.serve.reserved_capacity.'
                'parse_protocol_v2_cleanup_fence',
                return_value=cleanup_fence):
            _, remove_service, _, message, _, _ = self._run(
                [info], exists=lambda _name: True)

        assert message is None
        remove_service.assert_called_once()
        assert self.termination_kwargs == [{
            'continue_guard': mock.ANY,
            'expected_cluster_record_uuid': None,
            'cleanup_fence': cleanup_fence,
        }]
        assert self.quiesce.call_args.kwargs['include_terminal_history'] is True

    def test_failed_final_cas_never_restores_over_successor(self, tmp_path):
        incarnation_a = tmp_path / 'svc-inc-a'
        incarnation_b = tmp_path / 'svc-inc-b'
        incarnation_a.mkdir()
        (incarnation_a / 'owned-by-a').write_text('a', encoding='utf-8')
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17

        def _lose_final_cas(*_args, **_kwargs):
            # B acquires the name before A learns that its final epoch CAS
            # failed. B's directory is disjoint, so A needs no restore/rename.
            incarnation_b.mkdir()
            (incarnation_b / 'owned-by-b').write_text('b', encoding='utf-8')
            return False

        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': constants.CONTROLLER_TEARDOWN_ACK_PORT,
        }
        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(serve_state,
                               'get_service_controller_owner',
                               return_value=owner), \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(serve_utils,
                               'generate_remote_service_dir_name',
                               return_value=str(incarnation_a)), \
             mock.patch.object(serve_state,
                               'remove_service_completely',
                               side_effect=_lose_final_cas):
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', True, lifecycle_lock)

        assert message is not None and 'compare-and-delete' in message
        assert (incarnation_a / 'owned-by-a').read_text(encoding='utf-8') == 'a'
        assert (incarnation_b / 'owned-by-b').read_text(encoding='utf-8') == 'b'
        assert not list(tmp_path.glob('*.teardown-*'))

    def test_deletes_external_lb_objects(self):
        # The failed-service purge path must also reap the controller-owned
        # external LB (Deployment + Service); otherwise it leaks.
        infos = [self._replica(1, 'svc-1')]
        _, _, mock_delete_lb, message, _, _ = self._run(
            infos, exists=lambda name: False)
        assert message is None
        mock_delete_lb.assert_called_once_with(
            'svc',
            expected_service_hash='incarnation-a',
            require_runtime=True,
            expected_api_deployment_uid='api-deployment-uid',
            high_availability=False)

    def test_lb_delete_failure_retains_purge_row(self):
        # Never drop the service row while an old Ready LB may still hold
        # cached routes; same-name re-up must remain blocked until deletion is
        # retried successfully.
        _, remove_service, _, message, _, _ = self._run(
            [],
            exists=lambda name: False,
            lb_side_effect=RuntimeError('forbidden'))
        assert message is not None
        assert 'could not be purged' in message
        remove_service.assert_not_called()

    def test_failed_lb_delete_is_retryable_from_shutting_down(self):
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': None,
        }

        def _claim(*_args, **_kwargs):
            owner.update({
                'controller_pid': os.getpid(),
                'controller_ip': os.environ.get('POD_IP'),
                'controller_port': constants.CONTROLLER_TEARDOWN_ACK_PORT,
            })
            return True

        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(serve_state,
                               'get_service_controller_owner',
                               side_effect=lambda _name, **_kwargs: dict(owner)), \
             mock.patch.object(serve_state,
                               'get_ha_recovery_script',
                               return_value=None), \
             mock.patch.object(serve_state,
                               'claim_orphaned_service_teardown',
                               side_effect=_claim) as claim, \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(serve_utils, 'remove_service_directory'), \
             mock.patch.object(serve_state,
                               'remove_service_completely',
                               return_value=True) as remove_service, \
             mock.patch(
                 'sky.serve.lb_k8s.get_api_deployment_owner_uid',
                 return_value='api-deployment-uid'), \
             mock.patch(
                 'sky.serve.lb_k8s.delete_lb_objects',
                 side_effect=[RuntimeError('apiserver unavailable'), None]
             ) as delete_lb:
            first = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)
            second = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert first is not None and 'could not be purged' in first
        assert second is None
        claim.assert_called_once()
        assert delete_lb.call_count == 2
        remove_service.assert_called_once_with('svc',
                                               'incarnation-a',
                                               expected_lifecycle_epoch=17)

    def test_recovery_preclaim_none_port_is_not_teardown_ack(self):
        # HA recovery deliberately clears controller_port before the new
        # controller child is ready. Treating None as acknowledgement lets a
        # purger race that boot and down its clusters.
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': None,
        }
        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(serve_state,
                               'get_service_controller_owner',
                               return_value=owner), \
             mock.patch.object(serve_state,
                               'get_ha_recovery_script',
                               return_value='recovery command'), \
             mock.patch.object(serve_state,
                               'get_latest_committed_version',
                               return_value=1), \
             mock.patch.object(serve_utils.time,
                               'monotonic',
                               side_effect=[0, 11]), \
             mock.patch.object(serve_utils.time, 'sleep'), \
             mock.patch.object(serve_state,
                               'get_replica_infos') as get_replicas, \
             mock.patch.object(serve_state,
                               'remove_service_completely') as remove_service:
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert message is not None
        assert 'has not yet acknowledged' in message
        get_replicas.assert_not_called()
        remove_service.assert_not_called()

    def test_controller_ack_wait_uses_monotonic_bounded_sleep(self):
        """Clock changes cannot extend the wait or oversleep its deadline."""
        lifecycle_lock = mock.MagicMock(epoch=17)
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': None,
        }
        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(serve_state,
                               'get_service_controller_owner',
                               return_value=owner), \
             mock.patch.object(serve_state,
                               'get_ha_recovery_script',
                               return_value='recovery command'), \
             mock.patch.object(serve_state,
                               'get_latest_committed_version',
                               return_value=1), \
             mock.patch.object(serve_utils.time,
                               'time',
                               side_effect=AssertionError(
                                   'wall clock must not drive the deadline')), \
             mock.patch.object(serve_utils.time,
                               'monotonic',
                               side_effect=[0, 9.9, 10]), \
             mock.patch.object(serve_utils.time, 'sleep') as sleep, \
             mock.patch.object(serve_state,
                               'get_replica_infos') as get_replicas:
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert message is not None
        assert 'has not yet acknowledged' in message
        sleep.assert_called_once_with(pytest.approx(0.1))
        get_replicas.assert_not_called()

    def test_orphan_without_recovery_script_can_claim_teardown(self):
        # Legacy FAILED_CLEANUP rows have no live/recoverable controller to
        # write the new sentinel. Under the lifecycle lock, absence of the HA
        # script permits an exact-owner claim so the orphan stays purgeable.
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17
        old_owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': None,
        }
        claimed_owner = {
            **old_owner,
            'controller_pid': os.getpid(),
            'controller_ip': os.environ.get('POD_IP'),
            'controller_port': constants.CONTROLLER_TEARDOWN_ACK_PORT,
        }
        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(
                 serve_state,
                 'get_service_controller_owner',
                 side_effect=[old_owner, claimed_owner]), \
             mock.patch.object(serve_state,
                               'get_ha_recovery_script',
                               return_value=None), \
             mock.patch.object(
                 serve_state,
                 'claim_orphaned_service_teardown',
                 return_value=True) as claim, \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(serve_utils, 'remove_service_directory'), \
             mock.patch.object(serve_state,
                               'remove_service_completely',
                               return_value=True), \
             mock.patch(
                 'sky.serve.lb_k8s.get_api_deployment_owner_uid',
                 return_value='api-deployment-uid'), \
             mock.patch('sky.serve.lb_k8s.delete_lb_objects'):
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert message is None
        claim.assert_called_once_with('svc',
                                      'incarnation-a',
                                      101,
                                      '10.0.0.1',
                                      os.getpid(),
                                      os.environ.get('POD_IP'),
                                      expected_lifecycle_epoch=17)

    def test_bound_orphan_settles_then_rotates_authority_before_claim(self):
        lifecycle_lock = mock.MagicMock(epoch=17)
        old_owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': None,
        }
        purge_owner = (os.getpid(), os.environ.get('POD_IP'))
        claimed_owner = {
            **old_owner,
            'controller_pid': purge_owner[0],
            'controller_ip': purge_owner[1],
            'controller_port': constants.CONTROLLER_TEARDOWN_ACK_PORT,
        }
        old_authority = object()
        claimed_authority = object()
        events = []

        def _settle(*_args):
            events.append('exact-settle')
            return types.SimpleNamespace(provider_present_cleanup_contexts={},
                                         provider_reconciliation_failures={})

        def _rotate(*_args, **_kwargs):
            events.append('rotate-authority')
            return claimed_authority

        def _claim(*_args, **_kwargs):
            events.append('claim-orphan')
            return True

        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash') as legacy, \
             mock.patch.object(
                 serve_state,
                 'get_service_controller_owner',
                 side_effect=[old_owner, claimed_owner]), \
             mock.patch(
                 'sky.serve.ordinary_launch_binding.'
                 'begin_service_teardown_if_owner',
                 side_effect=[
                     ordinary_launch_binding.ServiceTeardownResult(
                         ordinary_launch_binding.ServiceTeardownDisposition.
                         MARKED_BOUND, old_authority),
                     ordinary_launch_binding.ServiceTeardownResult(
                         ordinary_launch_binding.ServiceTeardownDisposition.
                         MARKED_BOUND, claimed_authority),
                 ]), \
             mock.patch(
                 'sky.serve.service.'
                 '_settle_bound_ordinary_launches_for_teardown',
                 side_effect=_settle) as settle, \
             mock.patch(
                 'sky.serve.ordinary_launch_binding.'
                 'claim_controller_incarnation',
                 side_effect=_rotate) as rotate, \
             mock.patch.object(serve_state,
                               'get_ha_recovery_script',
                               return_value=None), \
             mock.patch.object(serve_state,
                               'claim_orphaned_service_teardown',
                               side_effect=_claim) as claim, \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 serve_utils,
                 'quiesce_service_replica_launch_requests',
                 return_value=True), \
             mock.patch.object(serve_utils, 'remove_service_directory'), \
             mock.patch.object(serve_state,
                               'remove_service_completely',
                               return_value=True), \
             mock.patch(
                 'sky.serve.lb_k8s.get_api_deployment_owner_uid',
                 return_value='api-deployment-uid'), \
             mock.patch('sky.serve.lb_k8s.delete_lb_objects'):
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert message is None
        assert events == [
            'exact-settle', 'rotate-authority', 'claim-orphan', 'exact-settle'
        ]
        assert settle.call_args_list == [
            mock.call(old_authority, []),
            mock.call(claimed_authority, []),
        ]
        rotate.assert_called_once_with(
            'svc',
            'incarnation-a', (101, '10.0.0.1'),
            mock.ANY,
            new_parent_owner=purge_owner,
            expected_lifecycle_epoch=17,
            expected_status=serve_state.ServiceStatus.SHUTTING_DOWN,
            wait_for_authority=False)
        claim.assert_called_once_with('svc',
                                      'incarnation-a',
                                      purge_owner[0],
                                      purge_owner[1],
                                      purge_owner[0],
                                      purge_owner[1],
                                      expected_lifecycle_epoch=17)
        legacy.assert_not_called()

    def test_orphan_with_unbootable_recovery_script_can_claim_teardown(self):
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17
        old_owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': None,
        }
        claimed_owner = {
            **old_owner,
            'controller_pid': os.getpid(),
            'controller_ip': os.environ.get('POD_IP'),
            'controller_port': constants.CONTROLLER_TEARDOWN_ACK_PORT,
        }
        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(
                 serve_state,
                 'get_service_controller_owner',
                 side_effect=[old_owner, claimed_owner]), \
             mock.patch.object(serve_state,
                               'get_ha_recovery_script',
                               return_value='impossible script'), \
             mock.patch.object(serve_state,
                               'get_latest_committed_version',
                               return_value=None), \
             mock.patch.object(
                 serve_state,
                 'claim_unrecoverable_service_teardown',
                 return_value=True) as claim, \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(serve_utils, 'remove_service_directory'), \
             mock.patch.object(serve_state,
                               'remove_service_completely',
                               return_value=True), \
             mock.patch(
                 'sky.serve.lb_k8s.get_api_deployment_owner_uid',
                 return_value='api-deployment-uid'), \
             mock.patch('sky.serve.lb_k8s.delete_lb_objects'):
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert message is None
        claim.assert_called_once_with('svc',
                                      'incarnation-a',
                                      101,
                                      '10.0.0.1',
                                      os.getpid(),
                                      os.environ.get('POD_IP'),
                                      expected_lifecycle_epoch=17)

    def test_orphan_with_unbootable_script_can_claim_teardown(self):
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 17
        old_owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'controller_port': None,
        }
        claimed_owner = {
            **old_owner,
            'controller_pid': os.getpid(),
            'controller_ip': os.environ.get('POD_IP'),
            'controller_port': constants.CONTROLLER_TEARDOWN_ACK_PORT,
        }
        with mock.patch.object(serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(serve_state,
                               'service_owner_matches',
                               return_value=True), \
             mock.patch.object(
                 serve_state,
                 'set_service_status_and_active_versions_if_hash',
                 return_value=True), \
             mock.patch.object(
                 serve_state,
                 'get_service_controller_owner',
                 side_effect=[old_owner, claimed_owner]), \
             mock.patch.object(serve_state,
                               'get_ha_recovery_script',
                               return_value='unbootable script'), \
             mock.patch.object(serve_state,
                               'get_latest_committed_version',
                               return_value=None), \
             mock.patch.object(
                 serve_state,
                 'claim_unrecoverable_service_teardown',
                 return_value=True) as claim, \
             mock.patch.object(serve_state,
                               'claim_orphaned_service_teardown') as legacy, \
             mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(serve_utils, 'remove_service_directory'), \
             mock.patch.object(serve_state,
                               'remove_service_completely',
                               return_value=True), \
             mock.patch(
                 'sky.serve.lb_k8s.get_api_deployment_owner_uid',
                 return_value='api-deployment-uid'), \
             mock.patch('sky.serve.lb_k8s.delete_lb_objects'):
            message = serve_utils._terminate_failed_services_locked(
                'svc', 'incarnation-a', False, lifecycle_lock)

        assert message is None
        claim.assert_called_once_with('svc',
                                      'incarnation-a',
                                      101,
                                      '10.0.0.1',
                                      os.getpid(),
                                      os.environ.get('POD_IP'),
                                      expected_lifecycle_epoch=17)
        legacy.assert_not_called()


class TestHaRecoveryFencesOnLeadershipLoss:
    """`ha_recovery_for_consolidation_mode` must re-check `still_leader`
    right before each recovery launch and abort the sweep on loss.

    The consolidation leader lock is a session-scoped PG advisory lock; it
    can die mid-sweep (RDS failover, idle timeout), at which point another
    pod may already be running its own recovery. Launching more controllers
    without leadership would split-brain.
    """

    def _run(self, still_leader, tmp_path, monkeypatch):
        monkeypatch.setenv('POD_IP', '10.4.0.1')
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_glob_service_names',
                return_value=['svc-a', 'svc-b']), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_service_liveness_snapshots',
                 return_value=[{'name': name,
                                'controller_pid': 1234,
                                'controller_ip': '10.4.0.1',
                                'status': 'READY',
                                'yaml_content': 'yaml: v1'}
                               for name in ('svc-a', 'svc-b')]), \
             mock.patch(
                 'sky.serve.serve_utils._controller_process_alive',
                 return_value=False), \
             mock.patch(
                 'sky.serve.serve_utils.'
                 '_snapshot_in_flight_start_service_incarnations',
                 return_value=set()), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_ha_recovery_script',
                 return_value='dummy script'), \
             mock.patch(
                 'sky.serve.serve_utils.command_runner.'
                 'LocalProcessCommandRunner') as mock_runner_cls, \
             mock.patch(
                 'sky.serve.serve_utils.skylet_constants.'
                 'HA_PERSISTENT_RECOVERY_LOG_PATH',
                 str(tmp_path / 'recovery_log_{}.log')):
            mock_runner_cls.return_value.run.return_value = (0, '', '')
            serve_utils.ha_recovery_for_consolidation_mode(
                pool=True, still_leader=still_leader)
            return mock_runner_cls.return_value.run

    def test_leadership_lost_aborts_sweep(self, tmp_path, monkeypatch):
        run = self._run(lambda: False, tmp_path, monkeypatch)
        run.assert_not_called()

    def test_leadership_lost_midway_stops_remaining(self, tmp_path,
                                                    monkeypatch):
        # Leader through the first launch's preflight and final fence, then
        # lost before the second service begins recovery.
        answers = iter([True, True, False])
        run = self._run(lambda: next(answers), tmp_path, monkeypatch)
        assert run.call_count == 1

    def test_leadership_held_runs_all(self, tmp_path, monkeypatch):
        run = self._run(lambda: True, tmp_path, monkeypatch)
        assert run.call_count == 2

    def test_no_probe_runs_all(self, tmp_path, monkeypatch):
        run = self._run(None, tmp_path, monkeypatch)
        assert run.call_count == 2


class TestHaRecoveryRecreatesServiceDir:
    """HA recovery must recreate the service working directory before
    running the recovery script.

    The directory lives on pod-local storage (emptyDir): a pod replacement
    wipes it while the durable service row and recovery script survive in
    the DB. The stored script redirects output into that directory, so
    without the mkdir it dies instantly and recovery retries forever while
    the service stays headless with replicas still billing.
    """

    def test_service_dir_created_before_script_runs(self, tmp_path,
                                                    monkeypatch):
        monkeypatch.setenv('POD_IP', '10.4.0.1')
        service_dir = tmp_path / 'servedir' / 'svc'
        assert not service_dir.exists()
        order = []

        def _run(script, require_outputs):
            del script, require_outputs
            order.append(('run', service_dir.exists()))
            return 0, '', ''

        runner = mock.Mock()
        runner.run.side_effect = _run
        with mock.patch(
                'sky.serve.serve_utils.serve_state.get_glob_service_names',
                return_value=['svc']), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.'
                 'get_service_liveness_snapshots',
                 return_value=[{'name': 'svc',
                                'controller_pid': 1234,
                                'controller_ip': '10.4.0.1',
                                'status': 'READY',
                                'yaml_content': 'yaml: v1'}]), \
             mock.patch(
                 'sky.serve.serve_utils._controller_process_alive',
                 return_value=False), \
             mock.patch(
                 'sky.serve.serve_utils.'
                 '_snapshot_in_flight_start_service_incarnations',
                 return_value=set()), \
             mock.patch(
                 'sky.serve.serve_utils.serve_state.get_ha_recovery_script',
                 return_value='dummy script'), \
             mock.patch(
                 'sky.serve.serve_utils.generate_remote_service_dir_name',
                 return_value=str(service_dir)), \
             mock.patch(
                 'sky.serve.serve_utils.command_runner.'
                 'LocalProcessCommandRunner',
                 return_value=runner), \
             mock.patch(
                 'sky.serve.serve_utils.skylet_constants.'
                 'HA_PERSISTENT_RECOVERY_LOG_PATH',
                 str(tmp_path / 'recovery_log_{}.log')):
            serve_utils.ha_recovery_for_consolidation_mode(pool=True)
        # The script ran exactly once, and the service dir existed by then.
        assert order == [('run', True)]
