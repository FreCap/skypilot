"""Tests for sky/serve/server/impl.py.

Focused on `apply()` rejecting terminal-state rows so callers don't blindly
hit a dead controller HTTP listener and get an opaque ECONNREFUSED. This
also makes the user-visible failure mode "go run --purge" instead of "look
at the connection-refused traceback and figure it out."
"""
# pylint: disable=invalid-name,protected-access
import base64
import shlex
from unittest import mock

import pytest
import yaml

from sky import backends
from sky.data import storage as storage_lib
from sky.serve import constants
from sky.serve import serve_state
from sky.serve.server import impl


def _backend_mock():
    """A mock that passes `isinstance(_, backends.CloudVmRayBackend)`."""
    return mock.MagicMock(spec=backends.CloudVmRayBackend)


class TestGetServiceRecord:
    """Update fences must not materialize a large replica inventory."""

    def test_consolidation_reads_shared_db_without_status_rpc(self):
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        record = {
            'name': 'svc',
            'pool': False,
            'status': serve_state.ServiceStatus.READY,
            'workspace': 'ws-a',
        }
        with mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=True), \
             mock.patch.object(impl.serve_state,
                               'get_service_status_snapshot',
                               return_value=record) as get_service, \
             mock.patch.object(
                 impl.serve_rpc_utils.RpcRunner,
                 'get_service_status') as get_status, \
             mock.patch.object(impl.serve_utils,
                               'get_yaml_content') as get_yaml_content, \
             mock.patch.object(impl.serve_utils.ServeCodeGen,
                               'get_service_status') as codegen:
            result = impl._get_service_record('svc', False, handle, backend)

        assert result is record
        get_service.assert_called_once_with('svc', require_version=True)
        get_status.assert_not_called()
        get_yaml_content.assert_not_called()
        codegen.assert_not_called()
        backend.run_on_head.assert_not_called()

    def test_consolidation_fetches_yaml_only_when_requested(self):
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        record = {
            'name': 'pool-a',
            'pool': True,
            'status': serve_state.ServiceStatus.READY,
            'resource_scope': 'scope-a',
            'version': 7,
            'workspace': 'ws-a',
        }
        with mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=True), \
             mock.patch.object(impl.serve_state,
                               'get_service_status_snapshot',
                               return_value=record.copy()) as get_service, \
             mock.patch.object(impl.serve_utils,
                               'get_yaml_content',
                               return_value='yaml-a') as get_yaml_content:
            result = impl._get_service_record('pool-a',
                                              True,
                                              handle,
                                              backend,
                                              include_yaml=True)

        assert result == {
            **record,
            'yaml_content': 'yaml-a',
        }
        get_service.assert_called_once_with('pool-a', require_version=True)
        get_yaml_content.assert_called_once_with('pool-a', 7, 'scope-a')

    def test_legacy_status_fallback_is_summary_only(self):
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        handle.is_grpc_enabled_with_flag = False
        backend = _backend_mock()
        backend.run_on_head.return_value = (0, b'payload', '')
        record = {
            'name': 'svc',
            'pool': False,
            'status': serve_state.ServiceStatus.READY,
        }
        with mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=False), \
             mock.patch.object(impl.serve_utils.ServeCodeGen,
                               'get_service_status',
                               return_value='code') as codegen, \
             mock.patch.object(impl.serve_utils,
                               'load_service_status',
                               return_value=[record]):
            result = impl._get_service_record('svc', False, handle, backend)

        assert result is record
        codegen.assert_called_once_with(
            ['svc'],
            pool=False,
            summary_only=True,
            include_target_num_replicas=False,
        )

    def test_rpc_status_fallback_is_summary_only(self):
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        handle.is_grpc_enabled_with_flag = True
        backend = _backend_mock()
        record = {
            'name': 'svc',
            'pool': False,
            'status': serve_state.ServiceStatus.READY,
        }
        with mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=False), \
             mock.patch.object(
                 impl.serve_rpc_utils.RpcRunner,
                 'get_service_status',
                 return_value=[record]) as get_status:
            result = impl._get_service_record('svc', False, handle, backend)

        assert result is record
        get_status.assert_called_once_with(
            handle,
            ['svc'],
            False,
            summary_only=True,
            include_target_num_replicas=False,
        )
        backend.run_on_head.assert_not_called()


def test_service_request_example_is_plain_when_data_auth_disabled():
    with mock.patch.object(impl.serve_utils,
                           'is_lb_data_plane_auth_enabled',
                           return_value=False):
        assert impl._service_test_request_command(
            'http://service') == 'curl http://service'


def test_service_request_example_shows_dedicated_header_when_enabled():
    with mock.patch.object(impl.serve_utils,
                           'is_lb_data_plane_auth_enabled',
                           return_value=True):
        command = impl._service_test_request_command('http://service')
    assert constants.LB_AUTHORIZATION_HEADER in command
    assert 'Bearer <token>' in command
    assert command.endswith('http://service')


def test_consolidated_registration_wait_carries_resource_scope():
    handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
    backend = _backend_mock()
    with mock.patch.object(impl.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(
             impl.serve_utils,
             'wait_service_registration',
             return_value='encoded-port') as wait_registration, \
         mock.patch.object(
             impl.serve_utils,
             'load_service_initialization_result') as load_result, \
         mock.patch.object(
             impl.serve_rpc_utils.RpcRunner,
             'wait_service_registration') as rpc_wait:
        impl._wait_for_service_registration(handle, backend, 'svc', 7, False,
                                            'incarnation-a')

    wait_registration.assert_called_once_with(
        'svc', 7, False, expected_resource_scope='incarnation-a')
    load_result.assert_called_once_with('encoded-port')
    rpc_wait.assert_not_called()
    backend.run_on_head.assert_not_called()


def test_scoped_storage_metadata_is_independent_of_local_intent_db():
    resource_scope = 'incarnation-a'
    generation = 'generation-a'
    scope_id = impl.serve_utils.generate_ephemeral_storage_scope_id(
        resource_scope, generation)
    storage = storage_lib.Storage(name=f'bucket-{scope_id}',
                                  persistent=False,
                                  _is_sky_managed=True)
    task = mock.MagicMock()
    task.metadata = {}
    task.storage_mounts = {'/data': storage}

    impl._record_scoped_ephemeral_storage(task, resource_scope, scope_id,
                                          generation, set())

    assert task.metadata[constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY] == {
        'resource_scope': resource_scope,
        'scope_id': scope_id,
        'storage_generation': generation,
        'storage_mounts': ['/data'],
    }


class TestCleanupProvisionalStorageIntents:
    """Regression coverage for failed up/update storage-intent cleanup."""

    @staticmethod
    def _task_with_scope(resource_scope, storage_generation):
        metadata = {
            constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY: {
                'resource_scope': resource_scope,
                'storage_generation': storage_generation,
            }
        }
        return mock.Mock(metadata=metadata)

    def test_scans_committed_versions_once_and_after_lifecycle_fence(self):
        intents = [
            {
                'resource_scope': 'scope-a',
                'storage_generation': 'gen-a',
                'yaml_content': 'yaml-a',
            },
            {
                'resource_scope': 'scope-b',
                'storage_generation': 'gen-b',
                'yaml_content': 'yaml-b',
            },
            {
                'resource_scope': 'scope-c',
                'storage_generation': 'gen-c',
                'yaml_content': 'yaml-c',
            },
        ]
        calls = []
        with mock.patch.object(impl.serve_state,
                               'get_ephemeral_storage_cleanup_intents',
                               return_value=intents), \
             mock.patch.object(
                 impl.serve_utils,
                 'advance_service_lifecycle_epoch',
                 side_effect=lambda lock: calls.append('advance') or 99), \
             mock.patch.object(
                 impl.serve_state,
                 'get_version_yaml_contents',
                 side_effect=lambda service_name: calls.append('versions') or
                 {
                     1: 'yaml-v1',
                     2: 'yaml-v2',
                 }) as get_version_yamls, \
             mock.patch.object(
                 impl.service_lib,
                 'load_task_for_storage_cleanup',
                 side_effect=lambda yaml_content: {
                     'yaml-v1': self._task_with_scope('scope-a', 'gen-a'),
                     'yaml-v2': self._task_with_scope('scope-z', 'gen-z'),
                 }[yaml_content]) as load_cleanup_task, \
             mock.patch.object(impl.service_lib,
                               'cleanup_storage',
                               return_value=True) as cleanup_storage, \
             mock.patch.object(
                 impl.serve_state,
                 'remove_provisional_ephemeral_storage_cleanup_intents',
                 return_value=True) as remove_intents:
            impl._cleanup_provisional_storage_intents('svc', 7,
                                                      mock.MagicMock())

        assert calls == ['advance', 'versions']
        assert get_version_yamls.call_count == 1
        assert load_cleanup_task.call_count == 2
        cleanup_storage.assert_has_calls([
            mock.call('yaml-b', 'scope-b'),
            mock.call('yaml-c', 'scope-c'),
        ])
        assert cleanup_storage.call_count == 2
        remove_intents.assert_has_calls([
            mock.call('svc', 'scope-b', 7, 99),
            mock.call('svc', 'scope-c', 7, 99),
        ],
                                        any_order=True)
        assert remove_intents.call_count == 2

    def test_unreadable_committed_yaml_retains_all_intents(self):
        intents = [{
            'resource_scope': 'scope-a',
            'storage_generation': 'gen-a',
            'yaml_content': 'yaml-a',
        }]
        with mock.patch.object(impl.serve_state,
                               'get_ephemeral_storage_cleanup_intents',
                               return_value=intents), \
             mock.patch.object(impl.serve_utils,
                               'advance_service_lifecycle_epoch',
                               return_value=99), \
             mock.patch.object(impl.serve_state,
                               'get_version_yaml_contents',
                               return_value={1: 'broken-yaml'}), \
             mock.patch.object(impl.service_lib,
                               'load_task_for_storage_cleanup',
                               side_effect=ValueError('bad yaml')), \
             mock.patch.object(impl.service_lib,
                               'cleanup_storage') as cleanup_storage, \
             mock.patch.object(
                 impl.serve_state,
                 'remove_provisional_ephemeral_storage_cleanup_intents'
             ) as remove_intents:
            impl._cleanup_provisional_storage_intents('svc', 7,
                                                      mock.MagicMock())

        cleanup_storage.assert_not_called()
        remove_intents.assert_not_called()

    def test_legacy_per_gpu_policy_is_readable_storage_metadata(self):
        legacy_yaml = """
resources:
  accelerators: A100:1
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 8
    target_concurrency_per_replica: 2
    spot_placer: dynamic_fallback_per_gpu
run: echo hi
"""
        with mock.patch.object(impl.serve_state,
                               'get_version_yaml_contents',
                               return_value={7: legacy_yaml}):
            assert impl._get_committed_storage_generations('svc') == set()

    def test_removal_is_grouped_by_resource_scope(self):
        intents = [
            {
                'resource_scope': 'scope-a',
                'storage_generation': 'gen-a',
                'yaml_content': 'yaml-a',
            },
            {
                'resource_scope': 'scope-a',
                'storage_generation': 'gen-b',
                'yaml_content': 'yaml-b',
            },
        ]
        with mock.patch.object(impl.serve_state,
                               'get_ephemeral_storage_cleanup_intents',
                               return_value=intents), \
             mock.patch.object(impl.serve_utils,
                               'advance_service_lifecycle_epoch',
                               return_value=99), \
             mock.patch.object(impl.serve_state,
                               'get_version_yaml_contents',
                               return_value={}), \
             mock.patch.object(impl.service_lib,
                               'cleanup_storage',
                               return_value=True) as cleanup_storage, \
             mock.patch.object(
                 impl.serve_state,
                 'remove_provisional_ephemeral_storage_cleanup_intents',
                 return_value=True) as remove_intents:
            impl._cleanup_provisional_storage_intents('svc', 7,
                                                      mock.MagicMock())

        assert cleanup_storage.call_count == 2
        remove_intents.assert_called_once_with('svc', 'scope-a', 7, 99)


class TestExternalOnlyTopologyPreflight:
    """Unsupported service layouts fail before mounts or cloud provisioning."""

    @staticmethod
    def _task(tls_credential=None):
        task = mock.Mock()
        task.service = mock.Mock(tls_credential=tls_credential)
        return task

    def test_pool_bypasses_lb_runtime(self):
        with mock.patch.object(
                impl.lb_k8s,
                'require_external_lb_runtime') as runtime_check, \
             mock.patch.object(impl.serve_utils,
                               'is_external_load_balancer_mode',
                               return_value=False), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode') as consolidation:
            impl._require_supported_service_topology(self._task(), pool=True)
        runtime_check.assert_not_called()
        consolidation.assert_not_called()

    def test_external_deployment_rejects_nonconsolidated_pool(self):
        with mock.patch.object(impl.serve_utils,
                               'is_external_load_balancer_mode',
                               return_value=True), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=False), \
             pytest.raises(RuntimeError,
                           match='jobs.controller.consolidation_mode=true'):
            impl._require_supported_service_topology(self._task(), pool=True)

    def test_task_level_tls_is_rejected_before_runtime_work(self):
        with mock.patch.object(
                impl.lb_k8s,
                'require_external_lb_runtime') as runtime_check, \
             pytest.raises(ValueError, match='Terminate TLS at the'):
            impl._require_supported_service_topology(
                self._task(tls_credential=mock.Mock()), pool=False)
        runtime_check.assert_not_called()

    def test_dedicated_controller_vm_is_rejected(self):
        with mock.patch.object(impl.lb_k8s,
                               'require_external_lb_runtime'), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=False), \
             pytest.raises(RuntimeError, match='dedicated controller VMs'):
            impl._require_supported_service_topology(self._task(), pool=False)

    def test_consolidated_external_runtime_is_accepted(self):
        with mock.patch.object(
                impl.lb_k8s,
                'require_external_lb_runtime') as runtime_check, \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=True) as consolidation:
            impl._require_supported_service_topology(self._task(), pool=False)
        runtime_check.assert_called_once_with()
        consolidation.assert_called_once_with(pool=False)

    def test_persisted_config_cannot_enable_missing_capability(
            self, monkeypatch):
        monkeypatch.delenv(constants.EXTERNAL_LB_ENABLED_ENV_VAR, raising=False)
        with mock.patch.object(
                impl.skypilot_config, 'get_nested', return_value=True), \
             pytest.raises(RuntimeError,
                           match='serve.externalLoadBalancer.enabled'):
            impl._require_supported_service_topology(self._task(), pool=False)


class TestExternalCapabilityMutationPaths:
    """Both create and update use the same environment-backed preflight."""

    @staticmethod
    def _task():
        task = mock.MagicMock()
        task.service = mock.MagicMock(pool=False)
        return task

    def test_up_runs_capability_preflight(self):
        task = self._task()
        dag = mock.MagicMock(tasks=[task])
        with mock.patch.object(impl.serve_utils, 'validate_service_task'), \
             mock.patch.object(impl, '_validate_service_name'), \
             mock.patch.object(impl.dag_utils,
                               'convert_entrypoint_to_dag',
                               return_value=dag), \
             mock.patch.object(impl.admin_policy_utils,
                               'apply',
                               return_value=(dag, mock.MagicMock())), \
             mock.patch.object(
                 impl,
                 '_require_supported_service_topology',
                 side_effect=RuntimeError('capability gate')) as preflight, \
             pytest.raises(RuntimeError, match='capability gate'):
            impl.up(task, service_name='svc')
        preflight.assert_called_once_with(task, False)

    def test_update_ignores_legacy_config_and_runs_preflight(self):
        task = self._task()
        dag = mock.MagicMock(tasks=[task])
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        legacy_config = mock.MagicMock()
        legacy_config.get_nested.side_effect = AssertionError(
            'legacy capability config read')
        service_record = {
            'status': serve_state.ServiceStatus.READY,
            'hash': 'incarnation-a',
            'workspace': 'research',
        }
        lifecycle_lock = mock.MagicMock(epoch=1)
        with mock.patch.object(impl.controller_utils,
                               'get_controller_for_pool'), \
             mock.patch.object(impl.backend_utils,
                               'is_controller_accessible',
                               return_value=handle), \
             mock.patch.object(impl.backend_utils,
                               'get_backend_from_handle',
                               return_value=backend), \
             mock.patch.object(impl,
                               '_get_service_record',
                               return_value=service_record), \
             mock.patch.object(impl.skypilot_config,
                               'get_active_workspace',
                               return_value='research'), \
             mock.patch.object(impl.serve_utils, 'validate_service_task'), \
             mock.patch.object(impl.admin_policy_utils,
                               'apply',
                               return_value=(dag, legacy_config)), \
             mock.patch.object(
                 impl,
                 '_require_supported_service_topology',
                 side_effect=RuntimeError('capability gate')) as preflight, \
             pytest.raises(RuntimeError, match='capability gate'):
            impl._update_impl(task, 'svc', lifecycle_lock=lifecycle_lock)
        legacy_config.get_nested.assert_not_called()
        preflight.assert_called_once_with(task, False)


class TestServiceNameValidation:
    """Service naming must respect LB and cluster-name constraints."""

    def test_external_service_label_boundary(self):
        impl._validate_service_name('s' * 63, pool=False)
        with pytest.raises(ValueError, match='at most 63 characters'):
            impl._validate_service_name('s' * 64, pool=False)

    def test_pool_is_not_limited_by_lb_label_boundary(self):
        impl._validate_service_name('p' * 64, pool=True)

    def test_cluster_name_grammar_still_applies(self):
        with pytest.raises(ValueError, match='is invalid'):
            impl._validate_service_name('bad/service', pool=False)


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
                'sky.serve.server.impl.serve_utils.'
                'get_service_lifecycle_lock',
                return_value=mock.MagicMock()),
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
             mock.patch('sky.serve.server.impl._up_impl') as mock_up:
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
                'sky.serve.server.impl.serve_utils.'
                'get_service_lifecycle_lock',
                return_value=mock.MagicMock()),
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
             mock.patch('sky.serve.server.impl._up_impl') as mock_up:
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
        content = b'active_workspace: mt_native\n'
        cmds = impl._ha_recovery_restore_cmds(
            {'~/.sky/serve/svc/config.yaml': content})
        assert len(cmds) == 1
        assert base64.b64encode(content).decode() in cmds[0]
        # Home-relative paths must expand at runtime: the leading ~ is
        # spliced to an unquoted "$HOME" with only the remainder quoted
        # (shlex leaves this metacharacter-free remainder unquoted).
        expected_path = '"$HOME"' + shlex.quote('/.sky/serve/svc/config.yaml')
        assert f'mkdir -p -- "$(dirname -- {expected_path})"' in cmds[0]
        assert cmds[0].endswith(f'> {expected_path}')

    def test_hostile_paths_are_quoted_inert(self):
        hostile = '/tmp/a b; rm -rf $HOME/pwn'
        cmds = impl._ha_recovery_restore_cmds({hostile: b'x: 1\n'})
        assert len(cmds) == 1
        assert shlex.quote(hostile) in cmds[0]
        assert '; rm -rf' not in cmds[0].replace(shlex.quote(hostile), '')

    def test_oversized_content_skipped(self):
        cmds = impl._ha_recovery_restore_cmds(
            {'~/x/big.bin': b'x' * (1024 * 1024 + 1)})
        assert not cmds

    def test_empty(self):
        assert not impl._ha_recovery_restore_cmds(None)
        assert not impl._ha_recovery_restore_cmds({})


class TestSanitizedConfigBytes:
    """Credential-capable config subtrees must never reach the durable
    ha_recovery_script DB row."""

    def test_strips_vast_create_instance_kwargs(self, tmp_path):
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
        assert impl._sanitized_config_bytes(str(tmp_path /
                                                'nope.yaml')) is (None)

    def test_unparsable_returns_none(self, tmp_path):
        cfg = tmp_path / 'bad.yaml'
        cfg.write_text('{: not yaml :')
        assert impl._sanitized_config_bytes(str(cfg)) is None

    def test_strips_pod_config_including_per_context(self, tmp_path):
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


class TestLifecycleLocking:
    """Updates use the cross-pod lifecycle lock; named down dispatch does not
    hold it because controller-side purge/finalization acquires it itself."""

    def test_update_locks_before_impl(self):
        calls = []
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.__enter__ = mock.Mock(
            side_effect=lambda *a: calls.append('lifecycle-lock'))
        lifecycle_lock.__exit__ = mock.Mock(
            side_effect=lambda *a: calls.append('lifecycle-unlock'))
        file_lock = mock.MagicMock()
        file_lock.__enter__ = mock.Mock(
            side_effect=lambda *a: calls.append('file-lock'))
        file_lock.__exit__ = mock.Mock(
            side_effect=lambda *a: calls.append('file-unlock'))
        with mock.patch('sky.serve.server.impl.filelock.FileLock',
                        return_value=file_lock) as mock_lock_cls, \
             mock.patch('sky.serve.server.impl.serve_utils.'
                        'get_service_lifecycle_lock',
                        return_value=lifecycle_lock), \
             mock.patch('sky.serve.server.impl.serve_utils.'
                        'get_service_filelock_path',
                        return_value='/tmp/svc.lock'), \
             mock.patch('sky.serve.server.impl._update_impl',
                        side_effect=lambda *a, **k: calls.append('impl')):
            impl.update(task=mock.Mock(), service_name='svc')
        mock_lock_cls.assert_called_once_with('/tmp/svc.lock')
        assert calls == [
            'file-lock', 'lifecycle-lock', 'impl', 'lifecycle-unlock',
            'file-unlock'
        ]

    def test_up_locks_before_any_name_scoped_work(self):
        calls = []
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.__enter__ = mock.Mock(
            side_effect=lambda *a: calls.append('lock'))
        lifecycle_lock.__exit__ = mock.Mock(
            side_effect=lambda *a: calls.append('unlock'))
        with mock.patch.object(impl.serve_utils,
                               'get_service_lifecycle_lock',
                               return_value=lifecycle_lock), \
             mock.patch.object(
                 impl,
                 '_up_impl',
                 side_effect=lambda *a, **k: calls.append('impl') or
                 ('svc', 'endpoint')):
            assert impl.up(mock.Mock(), 'svc') == ('svc', 'endpoint')
        assert calls == ['lock', 'impl', 'unlock']

    def test_nonconsolidated_pool_failure_never_cleans_api_local_intents(self):
        lifecycle_lock = mock.MagicMock()
        lifecycle_lock.epoch = 7
        with mock.patch.object(impl,
                               '_up_impl_body',
                               side_effect=RuntimeError('remote failure')), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=False), \
             mock.patch.object(
                 impl,
                 '_cleanup_provisional_storage_intents') as cleanup_intents, \
             pytest.raises(RuntimeError, match='remote failure'):
            impl._up_impl(mock.MagicMock(), 'pool', True, lifecycle_lock)
        cleanup_intents.assert_not_called()

    def test_second_same_name_up_fails_before_canonical_mutation(self):
        task = mock.MagicMock()
        lifecycle_lock = mock.MagicMock()
        with mock.patch.object(impl.serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=True), \
             mock.patch.object(impl.serve_state,
                               'get_service_hash',
                               return_value='incarnation-a'), \
             pytest.raises(RuntimeError, match='already exists'):
            impl._up_impl(task, 'svc', False, lifecycle_lock)
        task.validate.assert_not_called()

    def test_orphan_children_fail_before_task_or_storage_mutation(self):
        task = mock.MagicMock()
        lifecycle_lock = mock.MagicMock()
        with mock.patch.object(impl.serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=True), \
             mock.patch.object(impl.serve_state,
                               'get_service_hash', return_value=None), \
             mock.patch.object(impl.serve_state,
                               'get_orphaned_service_child_names',
                               return_value=['svc']), \
             mock.patch.object(impl.serve_state,
                               'get_orphaned_service_child_mode',
                               return_value=True), \
             mock.patch.object(
                 impl,
                 '_prepare_scoped_ephemeral_storage') as prepare_storage, \
             pytest.raises(RuntimeError,
                           match='sky jobs pool down svc --purge'):
            impl._up_impl_body(task, 'svc', False, lifecycle_lock)
        task.validate.assert_not_called()
        prepare_storage.assert_not_called()

    def test_update_fence_rejects_same_name_successor(self):
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        lifecycle_lock = mock.MagicMock()
        successor = {
            'name': 'svc',
            'hash': 'incarnation-b',
            'status': serve_state.ServiceStatus.READY,
        }
        with mock.patch.object(impl.serve_utils,
                               'lifecycle_lock_is_valid',
                               return_value=True), \
             mock.patch.object(impl,
                               '_get_service_record',
                               return_value=successor), \
             pytest.raises(RuntimeError, match='changed incarnation'):
            impl._assert_service_update_fence('svc', False, handle, backend,
                                              'incarnation-a', lifecycle_lock,
                                              'adding a version')

    @pytest.mark.parametrize('stored_workspace', [None, ''])
    def test_update_rejects_legacy_service_without_workspace(
            self, stored_workspace):
        record = {
            'name': 'svc',
            'hash': 'incarnation-a',
            'status': serve_state.ServiceStatus.READY,
            'workspace': stored_workspace,
        }
        with mock.patch.object(
                impl.serve_utils,
                'resolve_service_workspace',
                side_effect=RuntimeError('without a durable workspace')), \
             pytest.raises(RuntimeError, match='durable workspace'):
            impl._require_service_update_workspace(record, 'svc', 'service')

    def test_update_rejects_caller_from_different_workspace_before_mutation(
            self):
        task = mock.MagicMock()
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        record = {
            'name': 'svc',
            'hash': 'incarnation-a',
            'status': serve_state.ServiceStatus.READY,
            'workspace': 'research',
        }
        with mock.patch.object(impl.controller_utils,
                               'get_controller_for_pool'), \
             mock.patch.object(impl.backend_utils,
                               'is_controller_accessible',
                               return_value=handle), \
             mock.patch.object(impl.backend_utils,
                               'get_backend_from_handle',
                               return_value=backend), \
             mock.patch.object(impl,
                               '_get_service_record',
                               return_value=record), \
             mock.patch.object(impl.skypilot_config,
                               'get_active_workspace',
                               return_value='other-workspace'), \
             mock.patch.object(
                 impl.serve_utils,
                 'snapshot_service_container_images') as snapshot, \
             pytest.raises(RuntimeError, match='different workspace'):
            impl._update_impl_body(task, 'svc', lifecycle_lock=mock.MagicMock())
        task.validate.assert_not_called()
        snapshot.assert_not_called()

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
        assert not locked
        mock_term.assert_called_once()
