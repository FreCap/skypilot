"""Tests for sky/serve/server/impl.py.

Focused on `apply()` rejecting terminal-state rows so callers don't blindly
hit a dead controller HTTP listener and get an opaque ECONNREFUSED. This
also makes the user-visible failure mode "go run --purge" instead of "look
at the connection-refused traceback and figure it out."
"""
# pylint: disable=invalid-name,protected-access
import contextlib
from unittest import mock

import pytest

from sky import backends
from sky.serve import serve_state
from sky.serve.server import impl


def _backend_mock():
    """A mock that passes `isinstance(_, backends.CloudVmRayBackend)`."""
    return mock.MagicMock(spec=backends.CloudVmRayBackend)


class TestApplyRefusesTerminalStates:
    """`apply` should refuse to update a row that's in a terminal state.
    The previous behavior was to call `update()` regardless, which would
    POST to the (likely-dead) controller HTTP listener and surface a
    confusing ECONNREFUSED to the user."""

    def _service_record(self, status):
        return {
            'name': 'svc',
            'status': status,
            'controller_pid': 1234,
            'controller_port': 20001,
            'controller_ip': None,
            'pool': True,
        }

    def _common_patches(self, status):
        # Pretend the controller cluster is accessible (consolidation mode
        # is_controller_accessible is essentially a no-op anyway).
        return [
            mock.patch(
                'sky.serve.server.impl.serve_utils.get_service_filelock_path',
                return_value='/tmp/test_apply_lock'),
            mock.patch('sky.serve.server.impl.controller_utils.'
                       'get_controller_for_pool'),
            mock.patch(
                'sky.serve.server.impl.backend_utils.'
                'is_controller_accessible',
                return_value=mock.Mock()),
            mock.patch(
                'sky.serve.server.impl.backend_utils.'
                'get_backend_from_handle',
                return_value=_backend_mock()),
            mock.patch('sky.serve.server.impl._get_service_record',
                       return_value=self._service_record(status)),
        ]

    def _run_apply_with_status(self, status, pool):
        patches = self._common_patches(status)
        with mock.patch('sky.serve.server.impl._update_impl') as mock_update, \
             mock.patch('sky.serve.server.impl.up') as mock_up:
            for p in patches:
                p.start()
            try:
                impl.apply(task=mock.Mock(),
                           workers=None,
                           service_name='svc',
                           pool=pool)
            finally:
                for p in patches:
                    p.stop()
            return mock_update, mock_up

    def test_refuses_shutting_down(self):
        # SHUTTING_DOWN gets a friendlier "wait for shutdown" message that
        # still mentions --purge as a fallback for stuck cleanups, so users
        # who just ran `down` and re-applied aren't pushed straight to purge.
        with pytest.raises(RuntimeError,
                           match='shutting down.*Wait for shutdown.*--purge'):
            self._run_apply_with_status(serve_state.ServiceStatus.SHUTTING_DOWN,
                                        pool=True)

    def test_refuses_failed_cleanup(self):
        with pytest.raises(RuntimeError, match='FAILED_CLEANUP'):
            self._run_apply_with_status(
                serve_state.ServiceStatus.FAILED_CLEANUP, pool=True)

    def test_refuses_controller_failed(self):
        with pytest.raises(RuntimeError, match='CONTROLLER_FAILED'):
            self._run_apply_with_status(
                serve_state.ServiceStatus.CONTROLLER_FAILED, pool=False)

    def test_error_message_includes_purge_hint_for_pool(self):
        with pytest.raises(RuntimeError,
                           match='sky jobs pool down svc --purge'):
            self._run_apply_with_status(serve_state.ServiceStatus.SHUTTING_DOWN,
                                        pool=True)

    def test_error_message_includes_purge_hint_for_serve(self):
        with pytest.raises(RuntimeError, match='sky serve down svc --purge'):
            self._run_apply_with_status(serve_state.ServiceStatus.SHUTTING_DOWN,
                                        pool=False)

    def test_ready_does_not_raise_and_calls_update(self):
        """Sanity check: healthy READY rows still go through to update()."""
        mock_update, mock_up = self._run_apply_with_status(
            serve_state.ServiceStatus.READY, pool=True)
        mock_update.assert_called_once()
        mock_up.assert_not_called()

    def test_no_existing_record_calls_up(self):
        """When no row exists, apply should fall through to up() (create new),
        not raise."""
        patches = [
            mock.patch(
                'sky.serve.server.impl.serve_utils.get_service_filelock_path',
                return_value='/tmp/test_apply_lock'),
            mock.patch('sky.serve.server.impl.controller_utils.'
                       'get_controller_for_pool'),
            mock.patch(
                'sky.serve.server.impl.backend_utils.'
                'is_controller_accessible',
                return_value=mock.Mock()),
            mock.patch(
                'sky.serve.server.impl.backend_utils.'
                'get_backend_from_handle',
                return_value=_backend_mock()),
            mock.patch('sky.serve.server.impl._get_service_record',
                       return_value=None),
        ]
        with mock.patch('sky.serve.server.impl._update_impl') as mock_update, \
             mock.patch('sky.serve.server.impl.up') as mock_up:
            for p in patches:
                p.start()
            try:
                impl.apply(task=mock.Mock(),
                           workers=None,
                           service_name='svc',
                           pool=True)
            finally:
                for p in patches:
                    p.stop()
        mock_up.assert_called_once()
        mock_update.assert_not_called()


class TestHaRecoveryRestoreCmds:
    """The stored HA recovery script must recreate the controller config on
    a replacement pod (fresh emptyDir): content embedded base64 with a
    dirname mkdir, paths shell-quoted, and credential-capable config
    subtrees stripped before the embed."""

    def test_embeds_contents_with_home_spliced_quoting(self):
        import base64 as b64
        import shlex as shlex_mod
        content = b'active_workspace: mt_native\n'
        cmds = impl._ha_recovery_restore_cmds(
            {'~/.sky/serve/svc/config.yaml': content})
        assert len(cmds) == 1
        assert b64.b64encode(content).decode() in cmds[0]
        # Home-relative paths must expand at runtime: the leading ~ is
        # spliced to an unquoted "$HOME" with only the remainder quoted
        # (shlex leaves this metacharacter-free remainder unquoted).
        expected_path = '"$HOME"' + shlex_mod.quote(
            '/.sky/serve/svc/config.yaml')
        assert f'mkdir -p -- "$(dirname -- {expected_path})"' in cmds[0]
        assert cmds[0].endswith(f'> {expected_path}')

    def test_hostile_paths_are_quoted_inert(self):
        import shlex as shlex_mod
        hostile = '/tmp/a b; rm -rf $HOME/pwn'
        cmds = impl._ha_recovery_restore_cmds({hostile: b'x: 1\n'})
        assert len(cmds) == 1
        assert shlex_mod.quote(hostile) in cmds[0]
        assert '; rm -rf' not in cmds[0].replace(shlex_mod.quote(hostile), '')

    def test_oversized_content_skipped(self):
        cmds = impl._ha_recovery_restore_cmds(
            {'~/x/big.bin': b'x' * (1024 * 1024 + 1)})
        assert cmds == []

    def test_empty(self):
        assert impl._ha_recovery_restore_cmds(None) == []
        assert impl._ha_recovery_restore_cmds({}) == []


class TestSanitizedConfigBytes:
    """Credential-capable config subtrees must never reach the durable
    ha_recovery_script DB row."""

    def test_strips_vast_create_instance_kwargs(self, tmp_path):
        import yaml
        cfg = tmp_path / 'config.yaml'
        cfg.write_text('active_workspace: mt_native\n'
                       'workspaces:\n  mt_native: {}\n'
                       'vast:\n'
                       '  datacenter_only: true\n'
                       '  create_instance_kwargs:\n'
                       '    registry_password: hunter2\n')
        out = impl._sanitized_config_bytes(str(cfg))
        assert out is not None
        parsed = yaml.safe_load(out)
        assert b'hunter2' not in out
        assert 'create_instance_kwargs' not in parsed.get('vast', {})
        # Everything identity-relevant survives.
        assert parsed['active_workspace'] == 'mt_native'
        assert 'mt_native' in parsed['workspaces']
        assert parsed['vast']['datacenter_only'] is True

    def test_unreadable_returns_none(self, tmp_path):
        assert impl._sanitized_config_bytes(str(
            tmp_path / 'nope.yaml')) is (None)

    def test_unparsable_returns_none(self, tmp_path):
        cfg = tmp_path / 'bad.yaml'
        cfg.write_text('{: not yaml :')
        assert impl._sanitized_config_bytes(str(cfg)) is None

    def test_strips_pod_config_including_per_context(self, tmp_path):
        import yaml
        cfg = tmp_path / 'config.yaml'
        cfg.write_text(
            'active_workspace: mt_native\n'
            'kubernetes:\n'
            '  allowed_contexts: [ctx-a, ctx-b]\n'
            '  pod_config:\n'
            '    spec:\n'
            '      containers:\n'
            '        - env:\n'
            '            - {name: REGISTRY_PASSWORD, value: hunter2}\n'
            '  context_configs:\n'
            '    ctx-a:\n'
            '      provision_timeout: 10\n'
            '      pod_config:\n'
            '        spec: {imagePullSecrets: [{name: sekret}]}\n'
            'ssh:\n'
            '  pod_config: {spec: {x: topsecret}}\n')
        out = impl._sanitized_config_bytes(str(cfg))
        assert out is not None
        assert b'hunter2' not in out
        assert b'sekret' not in out
        assert b'topsecret' not in out
        parsed = yaml.safe_load(out)
        # Non-credential neighbors survive.
        assert parsed['kubernetes']['allowed_contexts'] == ['ctx-a', 'ctx-b']
        assert parsed['kubernetes']['context_configs']['ctx-a'][
            'provision_timeout'] == 10


class TestRejectExternalLbModeFlip:
    """`_reject_external_lb_mode_flip` must reject an update that would flip the
    service's external_load_balancer mode (no live migration between in-pod and
    external LB), and let a same-mode update proceed. `existing` is the server's
    current effective mode; `new` is the mode under the update's applied config.
    """

    def _run(self, existing, new):
        with mock.patch.object(impl.serve_utils,
                               'is_external_load_balancer_mode',
                               side_effect=[existing, new]), \
             mock.patch.object(impl.skypilot_config,
                               'replace_skypilot_config',
                               return_value=contextlib.nullcontext()):
            impl._reject_external_lb_mode_flip(mock.MagicMock())

    def test_inpod_to_external_raises(self):
        with pytest.raises(ValueError):
            self._run(existing=False, new=True)

    def test_external_to_inpod_raises(self):
        with pytest.raises(ValueError):
            self._run(existing=True, new=False)

    def test_same_mode_external_ok(self):
        self._run(existing=True, new=True)

    def test_same_mode_inpod_ok(self):
        self._run(existing=False, new=False)


class TestLifecycleLocking:
    """update()/down() must serialize on the same per-service filelock as
    apply(): an update racing a down on the same service can launch
    replicas mid-teardown, leaving orphaned (billable) clusters."""

    def test_update_locks_before_impl(self):
        calls = []
        lock = mock.MagicMock()
        lock.__enter__ = mock.Mock(side_effect=lambda *a: calls.append('lock'))
        lock.__exit__ = mock.Mock(side_effect=lambda *a: calls.append('unlock'))
        with mock.patch('sky.serve.server.impl.filelock.FileLock',
                        return_value=lock) as mock_lock_cls, \
             mock.patch('sky.serve.server.impl.serve_utils.'
                        'get_service_filelock_path',
                        return_value='/tmp/svc.lock'), \
             mock.patch('sky.serve.server.impl._update_impl',
                        side_effect=lambda *a, **k: calls.append('impl')):
            impl.update(task=mock.Mock(), service_name='svc')
        mock_lock_cls.assert_called_once_with('/tmp/svc.lock')
        assert calls == ['lock', 'impl', 'unlock']

    def _run_down(self, service_names, all=False):  # pylint: disable=redefined-builtin
        locked = []
        lock = mock.MagicMock()
        lock.__enter__ = mock.Mock()
        lock.__exit__ = mock.Mock()

        def _lock_path(name):
            return f'/tmp/{name}.lock'

        def _make_lock(path):
            locked.append(path)
            return lock

        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        with mock.patch('sky.serve.server.impl.filelock.FileLock',
                        side_effect=_make_lock), \
             mock.patch('sky.serve.server.impl.serve_utils.'
                        'get_service_filelock_path',
                        side_effect=_lock_path), \
             mock.patch('sky.serve.server.impl.controller_utils.'
                        'get_controller_for_pool'), \
             mock.patch('sky.serve.server.impl.backend_utils.'
                        'is_controller_accessible',
                        return_value=handle), \
             mock.patch('sky.serve.server.impl._terminate_services',
                        return_value='done') as mock_term:
            impl.down(service_names=service_names, all=all)
        return locked, mock_term

    def test_down_locks_each_named_service_sorted(self):
        locked, mock_term = self._run_down(['svc-b', 'svc-a'])
        assert locked == ['/tmp/svc-a.lock', '/tmp/svc-b.lock']
        mock_term.assert_called_once()

    def test_down_all_takes_no_per_service_locks(self):
        locked, mock_term = self._run_down(None, all=True)
        assert locked == []
        mock_term.assert_called_once()
