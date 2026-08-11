"""Regression tests for external-only service-controller supervision."""
# pylint: disable=protected-access
import hashlib
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.serve import constants
from sky.serve import serve_state
from sky.serve import service
from sky.utils import subprocess_utils

_PORT = 20005
_HASH = 'incarnation-a'
_DEFAULT_SNAPSHOT = object()


class _FakeProc:
    """Minimal controllable multiprocessing.Process test double."""

    def __init__(self,
                 alive: bool,
                 pid: int = 100,
                 join_error: Exception | None = None,
                 events=None):
        self._alive = alive
        self.pid = pid
        self.exitcode = None if alive else 1
        self.join_error = join_error
        self.join_calls = []
        self.events = events

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout=None):
        self.join_calls.append(timeout)
        if self.events is not None:
            self.events.append('join')
        if self.join_error is not None:
            raise self.join_error


class _DummyLock:

    def __init__(self, *unused_args, **unused_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *unused_args):
        return False


def _spec():
    return SimpleNamespace(pool=False)


def _setup(monkeypatch,
           *,
           new_controller,
           latest_snapshot=_DEFAULT_SNAPSHOT,
           killed=None,
           owns_row=True,
           events=None):
    monkeypatch.setattr(service.filelock, 'FileLock', _DummyLock)
    controller_socket = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(service, '_reserve_controller_socket', lambda unused:
                        (controller_socket, _PORT))
    monkeypatch.setattr(serve_state, 'set_service_controller_port_if_owner',
                        lambda name, service_hash, pid, ip, port: owns_row)
    if latest_snapshot is _DEFAULT_SNAPSHOT:
        latest_snapshot = (1, _spec())
    monkeypatch.setattr(serve_state, 'get_recovery_version_spec',
                        lambda unused_name: latest_snapshot)
    monkeypatch.setattr(service.ordinary_launch_binding,
                        'claim_controller_incarnation',
                        lambda *unused_args, **unused_kwargs: None)

    spawn_calls = []

    def _spawn_controller(unused_name, spec, version, unused_host, port,
                          service_hash, controller_ip, **kwargs):
        if events is not None:
            events.append('spawn')
        spawn_calls.append({
            'spec': spec,
            'version': version,
            'port': port,
            'service_hash': service_hash,
            'controller_ip': controller_ip,
            'enforce_launch_fence': kwargs.get('enforce_launch_fence', False),
            'controller_binding_authority':
                kwargs.get('controller_binding_authority'),
        })
        if isinstance(new_controller, BaseException):
            raise new_controller
        return new_controller

    monkeypatch.setattr(service, '_spawn_controller', _spawn_controller)
    monkeypatch.setattr(service, '_wait_for_controller_ready',
                        lambda *unused_args, **unused_kwargs: None)
    if killed is None:
        killed = []
    monkeypatch.setattr(
        subprocess_utils,
        'kill_children_processes',
        lambda parent_pids, force=False: killed.extend(parent_pids))
    return spawn_calls, killed


def test_respawn_recreates_only_controller_on_fresh_port(monkeypatch):
    events = []
    dead = _FakeProc(False, 111, events=events)
    replacement = _FakeProc(True, 333)
    spawn_calls, killed = _setup(monkeypatch,
                                 new_controller=replacement,
                                 killed=[],
                                 events=events)

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         dead,
                                         service_hash=_HASH)

    assert result == (replacement, _PORT)
    assert len(spawn_calls) == 1
    assert spawn_calls[0]['version'] == 1
    assert spawn_calls[0]['port'] == _PORT
    assert spawn_calls[0]['service_hash'] == _HASH
    assert dead.join_calls == [service._DEAD_CHILD_REAP_TIMEOUT_SECONDS]
    assert events == ['join', 'spawn']
    assert 111 not in killed
    # There is intentionally no in-pod LB process to restart: the stable API
    # proxy resolves the newly published port on its next request.


def test_controller_hold_blocks_service_respawn_before_reap(monkeypatch):
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')
    monkeypatch.setattr(serve_state, 'get_service_mode_and_hash',
                        lambda unused_name: (False, _HASH))
    monkeypatch.setattr(service, '_reap_dead_controller_for_respawn',
                        lambda *unused_args: pytest.fail('reaped held child'))

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash=_HASH)

    assert result is None


def test_controller_hold_preserves_pool_respawn(monkeypatch):
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')
    monkeypatch.setattr(serve_state, 'get_service_mode_and_hash',
                        lambda unused_name: (True, _HASH))
    dead = _FakeProc(False, 111)
    replacement = _FakeProc(True, 333)
    _setup(monkeypatch, new_controller=replacement)

    result = service._respawn_controller('pool-a',
                                         '127.0.0.1',
                                         dead,
                                         service_hash=_HASH)

    assert result == (replacement, _PORT)


def test_respawn_releases_port_lock_before_readiness_wait(monkeypatch):
    events = []
    dead = _FakeProc(False, 111, events=events)
    replacement = _FakeProc(True, 333)
    _setup(monkeypatch, new_controller=replacement, events=events)

    class _EventLock:
        """File-lock double that records its held interval."""

        def __init__(self, *unused_args, **unused_kwargs):
            pass

        def __enter__(self):
            events.append('lock_enter')
            return self

        def __exit__(self, *unused_args):
            events.append('lock_exit')
            return False

    controller_socket = SimpleNamespace(
        close=lambda: events.append('socket_close'))
    monkeypatch.setattr(service.filelock, 'FileLock', _EventLock)
    monkeypatch.setattr(
        service, '_reserve_controller_socket', lambda unused:
        (events.append('reserve') or controller_socket, _PORT))
    monkeypatch.setattr(
        service, '_wait_for_controller_ready',
        lambda *unused_args, **unused_kwargs: events.append('wait'))
    monkeypatch.setattr(serve_state, 'set_service_controller_port_if_owner',
                        lambda *unused_args: events.append('publish') or True)

    assert service._respawn_controller('svc',
                                       '127.0.0.1',
                                       dead,
                                       service_hash=_HASH) == (replacement,
                                                               _PORT)
    assert events == [
        'join', 'lock_enter', 'reserve', 'spawn', 'lock_exit', 'wait',
        'publish', 'socket_close'
    ]


def test_respawn_port_publish_fences_hash_pid_and_ip(monkeypatch):
    replacement = _FakeProc(True, 333)
    _setup(monkeypatch, new_controller=replacement)
    owner_calls = []
    monkeypatch.setattr(serve_state, 'set_service_controller_port_if_owner',
                        lambda *args: owner_calls.append(args) or True)

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash='incarnation-a',
                                         controller_ip='10.0.0.2')

    assert result == (replacement, _PORT)
    assert owner_calls == [('svc', 'incarnation-a', service.os.getpid(),
                            '10.0.0.2', _PORT)]


def test_same_parent_respawn_claims_fresh_child_authority(monkeypatch):
    replacement = _FakeProc(True, 333)
    spawn_calls, _ = _setup(monkeypatch, new_controller=replacement)
    claims = []
    publications = []

    def _claim(service_name, service_hash, expected_parent_owner,
               incarnation_uuid, **kwargs):
        claims.append((service_name, service_hash, expected_parent_owner,
                       incarnation_uuid, kwargs))
        return service.ordinary_launch_binding.ControllerBindingAuthority(
            service_name=service_name,
            service_hash=service_hash,
            service_workspace='workspace-a',
            service_lifecycle_epoch=4,
            controller_pid=service.os.getpid(),
            controller_ip='10.0.0.2',
            controller_incarnation=incarnation_uuid,
            controller_owner_epoch=6 + len(claims),
            capable=True,
            binding_mode=(service.ordinary_launch_binding.BindingMode.BOUND),
            binding_epoch=5)

    monkeypatch.setattr(service.ordinary_launch_binding,
                        'claim_controller_incarnation', _claim)
    monkeypatch.setattr(
        service.ordinary_launch_binding, 'publish_controller_port_if_authority',
        lambda authority, port: publications.append((authority, port)) or True)

    for dead_pid in (111, 112):
        assert service._respawn_controller(
            'svc',
            '127.0.0.1',
            _FakeProc(False, dead_pid),
            service_hash=_HASH,
            controller_ip='10.0.0.2') == (replacement, _PORT)

    assert [claim[2] for claim in claims] == [(service.os.getpid(), '10.0.0.2'),
                                              (service.os.getpid(), '10.0.0.2')]
    assert claims[0][3] != claims[1][3]
    assert all(claim[4]['new_parent_owner'] == claim[2] for claim in claims)
    assert all(claim[4]['wait_for_authority'] is False for claim in claims)
    assert [
        call['controller_binding_authority'].controller_owner_epoch
        for call in spawn_calls
    ] == [7, 8]
    assert [authority for authority, _ in publications
           ] == [call['controller_binding_authority'] for call in spawn_calls]


def test_respawn_defers_instead_of_waiting_for_provider_authority(monkeypatch):
    spawn_calls, _ = _setup(monkeypatch, new_controller=_FakeProc(True, 333))
    monkeypatch.setattr(
        service.ordinary_launch_binding, 'claim_controller_incarnation',
        mock.Mock(side_effect=(service.ordinary_launch_binding.
                               OrdinaryLaunchBindingBusy('provider active'))))

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash=_HASH,
                                         controller_ip='10.0.0.2')

    assert result is None
    assert not spawn_calls


def test_respawn_reloads_latest_committed_spec(monkeypatch):
    latest_spec = _spec()
    spawn_calls, _ = _setup(monkeypatch,
                            new_controller=_FakeProc(True, 333),
                            latest_snapshot=(7, latest_spec))

    service._respawn_controller('svc',
                                '127.0.0.1',
                                _FakeProc(False, 111),
                                service_hash=_HASH)

    assert spawn_calls[0]['version'] == 7
    assert spawn_calls[0]['spec'] is latest_spec


def test_respawn_restores_db_config_before_child_spawn(monkeypatch, tmp_path):
    events = []
    replacement = _FakeProc(True, 333)
    spawn_calls, _ = _setup(monkeypatch,
                            new_controller=replacement,
                            latest_snapshot=(7, _spec()),
                            events=events)
    base_path = tmp_path / 'config.yaml'
    historical_path = tmp_path / 'config.yaml.v6'
    live_path = str(tmp_path / 'config.yaml.v7')
    staged_path = str(tmp_path / ('config.yaml.v7.' + 'c' * 64 + '.staged'))
    credential_sentinel_one = b'aws_secret_access_key: base-secret\n'
    credential_sentinel_two = b'api_token: historical-secret\n'
    old_bytes = b'active_workspace: old\nworkspaces: {old: {}}\n'
    new_bytes = (b'active_workspace: research\n'
                 b'workspaces: {research: {}}\n'
                 b'kubernetes: {allowed_contexts: [east, phx]}\n')
    base_path.write_bytes(credential_sentinel_one)
    historical_path.write_bytes(credential_sentinel_two)
    (tmp_path / 'config.yaml.v6.receipt').write_text('historical source digest',
                                                     encoding='utf-8')
    (tmp_path / 'config.yaml.v7').write_bytes(old_bytes)
    (tmp_path / 'config.yaml.v7.receipt').write_text('current source digest',
                                                     encoding='utf-8')
    snapshot_id = 'c' * 64
    durable_bytes = service.serve_utils.sanitize_ha_recovery_config_bytes(
        new_bytes)
    durable_digest = hashlib.sha256(durable_bytes).hexdigest()
    monkeypatch.setattr(
        serve_state, 'get_version_controller_config', lambda *unused_args:
        (durable_bytes, durable_digest, snapshot_id))
    monkeypatch.setattr(serve_state, 'get_service_config_recovery_identity',
                        lambda unused_name: (_HASH, 'research'))
    monkeypatch.setattr(service.serve_utils,
                        'generate_versioned_config_yaml_file_name',
                        lambda *unused_args: live_path)
    monkeypatch.setattr(service.serve_utils,
                        'generate_remote_config_yaml_file_name',
                        lambda *unused_args: str(base_path))
    monkeypatch.setattr(service.serve_utils,
                        'generate_staged_config_yaml_file_name',
                        lambda *unused_args, **unused_kwargs: staged_path)

    def _install(unused_config, unused_path):
        events.append('publish_config')

    monkeypatch.setattr(service.skypilot_config,
                        'install_internal_config_snapshot', _install)

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111, events=events),
                                         service_hash=_HASH)

    assert result == (replacement, _PORT)
    assert spawn_calls[0]['version'] == 7
    assert (tmp_path / 'config.yaml.v7').read_bytes() == durable_bytes
    assert not base_path.exists()
    assert not historical_path.exists()
    assert not (tmp_path / 'config.yaml.v6.receipt').exists()
    assert not (tmp_path / 'config.yaml.v7.receipt').exists()
    assert not (tmp_path / ('config.yaml.v7.' + 'c' * 64 + '.staged')).exists()
    assert events.index('publish_config') < events.index('spawn')


def test_respawn_preserves_authoritative_launch_fence_bit(monkeypatch):
    spawn_calls, _ = _setup(monkeypatch, new_controller=_FakeProc(True, 333))

    service._respawn_controller('svc',
                                '127.0.0.1',
                                _FakeProc(False, 111),
                                service_hash=_HASH,
                                enforce_launch_fence=True)

    assert spawn_calls[0]['enforce_launch_fence'] is True


def test_respawn_db_error_retries_without_stale_spec(monkeypatch):
    spawn_calls, _ = _setup(monkeypatch, new_controller=_FakeProc(True, 333))

    def _db_error(unused_name):
        raise RuntimeError('db unavailable')

    monkeypatch.setattr(serve_state, 'get_recovery_version_spec', _db_error)
    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash=_HASH)
    assert result is None
    assert not spawn_calls


def test_respawn_without_committed_spec_retries_without_stale_fallback(
        monkeypatch):
    spawn_calls, _ = _setup(monkeypatch,
                            new_controller=_FakeProc(True, 333),
                            latest_snapshot=None)

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash=_HASH)

    assert result is None
    assert not spawn_calls


def test_respawn_failure_is_contained(monkeypatch):
    _, killed = _setup(monkeypatch,
                       new_controller=OSError('no memory'),
                       killed=[])
    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash=_HASH)
    assert result is None
    # The previous child is joined before any replacement attempt. SIGKILL is
    # reserved for a replacement that fails or loses ownership.
    assert 111 not in killed


def test_respawn_refuses_live_child_before_any_side_effect(monkeypatch):
    live = _FakeProc(True, 111)
    spawn_calls, killed = _setup(monkeypatch,
                                 new_controller=_FakeProc(True, 333),
                                 killed=[])
    snapshot_calls = []
    monkeypatch.setattr(
        serve_state, 'get_recovery_version_spec',
        lambda unused_name: snapshot_calls.append(unused_name) or (1, _spec()))

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         live,
                                         service_hash=_HASH)

    assert result is None
    assert not live.join_calls
    assert not snapshot_calls
    assert not spawn_calls
    assert not killed


def test_respawn_refuses_missing_child_handle_before_any_side_effect(
        monkeypatch):
    spawn_calls, killed = _setup(monkeypatch,
                                 new_controller=_FakeProc(True, 333),
                                 killed=[])
    snapshot_calls = []
    monkeypatch.setattr(
        serve_state, 'get_recovery_version_spec',
        lambda unused_name: snapshot_calls.append(unused_name) or (1, _spec()))

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         None,
                                         service_hash=_HASH)

    assert result is None
    assert not snapshot_calls
    assert not spawn_calls
    assert not killed


def test_lb_backoff_does_not_delay_first_dead_child_respawn(monkeypatch):
    now = 100.0
    backoff = service._ControllerSupervisionBackoff(degraded_retry_at=now + 300)
    dead = _FakeProc(False, 111)
    replacement = _FakeProc(True, 333)
    spawn_calls, _ = _setup(monkeypatch, new_controller=replacement)

    result = None
    if backoff.respawn_is_due('svc', dead, now):
        result = service._respawn_controller('svc',
                                             '127.0.0.1',
                                             dead,
                                             service_hash=_HASH)

    assert backoff.degraded_retry_at > now
    assert result == (replacement, _PORT)
    assert len(spawn_calls) == 1


def test_respawn_join_failure_is_fail_closed(monkeypatch):
    dead = _FakeProc(False, 111, join_error=RuntimeError('cannot reap'))
    spawn_calls, killed = _setup(monkeypatch,
                                 new_controller=_FakeProc(True, 333),
                                 killed=[])
    snapshot_calls = []
    monkeypatch.setattr(
        serve_state, 'get_recovery_version_spec',
        lambda unused_name: snapshot_calls.append(unused_name) or (1, _spec()))

    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         dead,
                                         service_hash=_HASH)

    assert result is None
    assert dead.join_calls == [service._DEAD_CHILD_REAP_TIMEOUT_SECONDS]
    assert not snapshot_calls
    assert not spawn_calls
    assert not killed


def test_respawn_dead_replacement_is_reaped(monkeypatch):
    replacement = _FakeProc(False, 333)
    _, killed = _setup(monkeypatch, new_controller=replacement, killed=[])
    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash=_HASH)
    assert result is None
    assert 333 in killed


def test_respawn_lost_ownership_discards_replacement(monkeypatch):
    replacement = _FakeProc(True, 333)
    _, killed = _setup(monkeypatch,
                       new_controller=replacement,
                       killed=[],
                       owns_row=False)
    result = service._respawn_controller('svc',
                                         '127.0.0.1',
                                         _FakeProc(False, 111),
                                         service_hash=_HASH)
    assert result is None
    assert 333 in killed


def test_wait_for_controller_ready_fails_fast_on_dead_process(monkeypatch):
    attempts = []

    def _connect(*unused_args, **unused_kwargs):
        attempts.append(1)
        raise ConnectionRefusedError()

    monkeypatch.setattr(service.socket, 'create_connection', _connect)
    with pytest.raises(RuntimeError):
        service._wait_for_controller_ready('127.0.0.1',
                                           _PORT,
                                           timeout=30,
                                           process=_FakeProc(False))
    assert not attempts
