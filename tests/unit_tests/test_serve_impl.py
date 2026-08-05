"""Tests for sky/serve/server/impl.py.

Focused on `apply()` rejecting terminal-state rows so callers don't blindly
hit a dead controller HTTP listener and get an opaque ECONNREFUSED. This
also makes the user-visible failure mode "go run --purge" instead of "look
at the connection-refused traceback and figure it out."
"""
# pylint: disable=invalid-name,protected-access
import base64
import contextlib
from unittest import mock

import pytest
import yaml

from sky import backends
from sky.data import storage as storage_lib
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils
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

    def test_update_accepts_admitted_config_and_runs_topology_preflight(self):
        task = self._task()
        dag = mock.MagicMock(tasks=[task])
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        admitted_config = {'active_workspace': 'research'}
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
                               return_value=(dag, admitted_config)), \
             mock.patch.object(
                 impl,
                 '_require_supported_service_topology',
                 side_effect=RuntimeError('capability gate')) as preflight, \
             pytest.raises(RuntimeError, match='capability gate'):
            impl._update_impl(task, 'svc', lifecycle_lock=lifecycle_lock)
        preflight.assert_called_once_with(task, False)

    def test_old_controller_fails_before_storage_sync_or_version_allocation(
            self):
        task = self._task()
        dag = mock.MagicMock(tasks=[task])
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
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
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=True), \
             mock.patch.object(impl.serve_utils, 'validate_service_task'), \
             mock.patch.object(impl.serve_utils,
                               'snapshot_service_container_images'), \
             mock.patch.object(impl.admin_policy_utils,
                               'apply',
                               return_value=(dag, {
                                   'active_workspace': 'research'
                               })), \
             mock.patch.object(impl,
                               '_require_supported_service_topology'), \
             mock.patch.object(impl,
                               '_assert_service_update_fence'), \
             mock.patch.object(
                 impl.serve_utils,
                 'require_update_config_snapshot_capability',
                 side_effect=RuntimeError('old controller')) as capability, \
             mock.patch.object(
                 impl,
                 '_prepare_scoped_ephemeral_storage') as prepare_storage, \
             mock.patch.object(
                 impl.controller_utils,
                 'maybe_translate_local_file_mounts_and_sync_up') as sync, \
             mock.patch.object(impl.serve_state,
                               'add_version') as add_version, \
             pytest.raises(RuntimeError, match='old controller'):
            impl._update_impl(task, 'svc', lifecycle_lock=lifecycle_lock)
        capability.assert_called_once_with('svc', 'incarnation-a')
        prepare_storage.assert_not_called()
        sync.assert_not_called()
        add_version.assert_not_called()

    def test_oversized_config_fails_before_version_or_storage_mutation(self):
        task = self._task()
        dag = mock.MagicMock(tasks=[task])
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        service_record = {
            'status': serve_state.ServiceStatus.READY,
            'hash': 'incarnation-a',
            'workspace': 'research',
        }
        admitted_config = {
            'active_workspace': 'research',
            'docker': {
                'run_options': 'x' * (1024 * 1024)
            },
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
                               return_value=service_record), \
             mock.patch.object(impl.skypilot_config,
                               'get_active_workspace',
                               return_value='research'), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=True), \
             mock.patch.object(impl.serve_utils, 'validate_service_task'), \
             mock.patch.object(impl.admin_policy_utils,
                               'apply',
                               return_value=(dag, admitted_config)), \
             mock.patch.object(
                 impl,
                 '_prepare_scoped_ephemeral_storage') as prepare_storage, \
             mock.patch.object(impl.serve_state,
                               'add_version') as add_version, \
             pytest.raises(ValueError, match='1MiB'):
            impl._update_impl(task,
                              'svc',
                              lifecycle_lock=mock.MagicMock(epoch=1))
        prepare_storage.assert_not_called()
        add_version.assert_not_called()

    def test_nonconsolidated_update_does_not_enter_snapshot_protocol(self):
        task = self._task()
        dag = mock.MagicMock(tasks=[task])
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        service_record = {
            'status': serve_state.ServiceStatus.READY,
            'hash': 'incarnation-a',
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
                               return_value=service_record), \
             mock.patch.object(impl.skypilot_config,
                               'get_active_workspace',
                               return_value='research'), \
             mock.patch.object(impl.serve_utils,
                               'is_consolidation_mode',
                               return_value=False), \
             mock.patch.object(impl.serve_utils, 'validate_service_task'), \
             mock.patch.object(impl.admin_policy_utils,
                               'apply',
                               return_value=(dag, {
                                   'active_workspace': 'research'
                               })), \
             mock.patch.object(
                 impl.serve_utils,
                 'sanitize_ha_recovery_config_bytes') as sanitize, \
             mock.patch.object(
                 impl,
                 '_require_supported_service_topology',
                 side_effect=RuntimeError('stop after legacy preflight')), \
             pytest.raises(RuntimeError, match='legacy preflight'):
            impl._update_impl(task,
                              'svc',
                              lifecycle_lock=mock.MagicMock(epoch=1))
        sanitize.assert_not_called()


class TestAtomicConfigUpdateCleanup:
    """Raw config staging has distinct pre- and post-delivery cleanup."""

    @staticmethod
    def _task():
        task = mock.MagicMock()
        task.service = mock.MagicMock(pool=False)
        task.to_yaml_config.return_value = {'service': {}}
        return task

    def _run_update(self,
                    *,
                    fence_side_effect=None,
                    update_error=None,
                    secure_returncode=0):
        task = self._task()
        dag = mock.MagicMock(tasks=[task])
        handle = mock.MagicMock(spec=backends.CloudVmRayResourceHandle)
        backend = _backend_mock()
        events = []

        def _sync(*unused_args, **unused_kwargs):
            events.append('sync')

        def _run_on_head(unused_handle, code, **unused_kwargs):
            events.append(code)
            return 0, '', ''

        def _secure_stage(*unused_args, **unused_kwargs):
            events.append('secure')
            if secure_returncode:
                raise RuntimeError('stage verification failed')

        backend.sync_file_mounts.side_effect = _sync
        backend.run_on_head.side_effect = _run_on_head
        service_record = {
            'status': serve_state.ServiceStatus.READY,
            'hash': 'incarnation-a',
            'workspace': 'research',
            'resource_scope': 'scope-a',
        }
        lifecycle_lock = mock.MagicMock(epoch=7)
        if fence_side_effect is None:
            fence_side_effect = service_record
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(impl.controller_utils,
                                  'get_controller_for_pool'))
            stack.enter_context(
                mock.patch.object(impl.backend_utils,
                                  'is_controller_accessible',
                                  return_value=handle))
            stack.enter_context(
                mock.patch.object(impl.backend_utils,
                                  'get_backend_from_handle',
                                  return_value=backend))
            stack.enter_context(
                mock.patch.object(impl,
                                  '_get_service_record',
                                  return_value=service_record))
            stack.enter_context(
                mock.patch.object(impl.skypilot_config,
                                  'get_active_workspace',
                                  return_value='research'))
            stack.enter_context(
                mock.patch.object(impl.serve_utils,
                                  'is_consolidation_mode',
                                  return_value=True))
            stack.enter_context(
                mock.patch.object(impl.serve_utils, 'validate_service_task'))
            stack.enter_context(
                mock.patch.object(impl.serve_utils,
                                  'snapshot_service_container_images'))
            stack.enter_context(
                mock.patch.object(impl.admin_policy_utils,
                                  'apply',
                                  return_value=(dag, {
                                      'active_workspace': 'research'
                                  })))
            stack.enter_context(
                mock.patch.object(impl.controller_utils,
                                  'controller_config_snapshot',
                                  return_value={
                                      'active_workspace': 'research',
                                      'workspaces': {
                                          'research': {}
                                      },
                                  }))
            stack.enter_context(
                mock.patch.object(impl, '_require_supported_service_topology'))
            stack.enter_context(
                mock.patch.object(impl,
                                  '_assert_service_update_fence',
                                  side_effect=fence_side_effect))
            stack.enter_context(
                mock.patch.object(impl.serve_utils,
                                  'require_update_config_snapshot_capability'))
            stack.enter_context(
                mock.patch.object(impl,
                                  '_prepare_scoped_ephemeral_storage',
                                  return_value=('scope-id', 'generation-a',
                                                set())))
            stack.enter_context(
                mock.patch.object(impl, '_record_scoped_ephemeral_storage'))
            stack.enter_context(
                mock.patch.object(impl,
                                  '_persist_scoped_ephemeral_storage_intent'))
            stack.enter_context(
                mock.patch.object(
                    impl.controller_utils,
                    'maybe_translate_local_file_mounts_and_sync_up'))
            stack.enter_context(
                mock.patch.object(impl.serve_state,
                                  'add_version',
                                  return_value=2))
            stack.enter_context(
                mock.patch.object(impl.secrets,
                                  'token_hex',
                                  return_value='c' * 64))

            def _update(*unused_args, **unused_kwargs):
                events.append('submit')
                if update_error is not None:
                    raise update_error

            update = stack.enter_context(
                mock.patch.object(impl.serve_utils,
                                  'update_service_encoded',
                                  side_effect=_update))
            serialized = stack.enter_context(
                mock.patch.object(impl.serve_utils,
                                  'cleanup_staged_config_update_encoded'))
            cleanup_codegen = stack.enter_context(
                mock.patch.object(impl.serve_utils.ServeCodeGen,
                                  'remove_uncommitted_staged_controller_config',
                                  return_value='cleanup-code'))
            secure_stage = stack.enter_context(
                mock.patch.object(impl.serve_utils,
                                  'secure_staged_controller_config',
                                  side_effect=_secure_stage))
            stack.enter_context(
                mock.patch.object(impl, '_cleanup_provisional_storage_intents'))
            expected_error = ('test failure' if secure_returncode == 0 else
                              'Failed to secure staged controller config')
            with pytest.raises(RuntimeError, match=expected_error):
                impl._update_impl(task, 'svc', lifecycle_lock=lifecycle_lock)
        return (backend, update, serialized, cleanup_codegen, secure_stage,
                events)

    def test_pre_submit_failure_removes_exact_remote_stage(self):
        service_record = {
            'status': serve_state.ServiceStatus.READY,
            'hash': 'incarnation-a',
            'workspace': 'research',
            'resource_scope': 'scope-a',
        }
        fences = [
            service_record,
            service_record,
            service_record,
            RuntimeError('test failure before submit'),
        ]

        (backend, update, serialized, cleanup_codegen, secure_stage,
         events) = self._run_update(fence_side_effect=fences)

        backend.sync_file_mounts.assert_called_once()
        synced_paths = backend.sync_file_mounts.call_args.args[1]
        assert any(
            path.endswith(f'.v2.{"c" * 64}.staged') for path in synced_paths)
        update.assert_not_called()
        serialized.assert_not_called()
        secure_stage.assert_called_once_with(mock.ANY, mock.ANY)
        cleanup_codegen.assert_called_once_with('svc', 2, 'scope-a', 'c' * 64)
        assert [call.args[1] for call in backend.run_on_head.call_args_list
               ] == ['cleanup-code']
        source_digest = secure_stage.call_args.args[1]
        assert all(source_digest not in call.args[1]
                   for call in backend.run_on_head.call_args_list)
        assert events == ['sync', 'secure', 'cleanup-code']

    def test_ambiguous_submit_uses_serialized_controller_cleanup(self):
        (backend, update, serialized, cleanup_codegen, secure_stage,
         events) = self._run_update(
             update_error=RuntimeError('test failure after submit'))

        backend.sync_file_mounts.assert_called_once()
        update.assert_called_once()
        serialized.assert_called_once_with('svc', 'incarnation-a', 2, 7,
                                           'c' * 64)
        cleanup_codegen.assert_not_called()
        secure_stage.assert_called_once_with(mock.ANY, mock.ANY)
        backend.run_on_head.assert_not_called()
        assert events == ['sync', 'secure', 'submit']

    def test_stage_verification_failure_cleans_before_submission(self):
        (backend, update, serialized, cleanup_codegen, secure_stage,
         events) = self._run_update(secure_returncode=1)

        backend.sync_file_mounts.assert_called_once()
        update.assert_not_called()
        serialized.assert_not_called()
        secure_stage.assert_called_once_with(mock.ANY, mock.ANY)
        cleanup_codegen.assert_called_once_with('svc', 2, 'scope-a', 'c' * 64)
        assert [call.args[1] for call in backend.run_on_head.call_args_list
               ] == ['cleanup-code']
        source_digest = secure_stage.call_args.args[1]
        assert source_digest not in backend.run_on_head.call_args.args[1]
        assert events == ['sync', 'secure', 'cleanup-code']


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


class TestStripLegacyHaRecoveryConfigPayload:
    """Historical scripts must not retain config bytes in argv or the DB."""

    _REMOTE_PATH = '~/.sky/serve/svc/config.yaml'
    _LAUNCH = ('/usr/bin/python \\\n'
               '  -u -m sky.serve.service \\\n'
               '  --service-name svc\n')

    def test_removes_production_one_line_restore_and_retains_export(self):
        secret_config = b'docker:\n  password: durable-secret\n'
        payload = base64.b64encode(secret_config).decode('ascii')
        script = (
            f'export SKYPILOT_CONFIG={self._REMOTE_PATH}\n'
            'mkdir -p -- "$(dirname -- "$HOME"/.sky/serve/svc/config.yaml)" '
            f'&& printf %s {payload} | base64 -d > '
            '"$HOME"/.sky/serve/svc/config.yaml\n' + self._LAUNCH)

        scrubbed = serve_utils.strip_legacy_ha_recovery_config_payload(
            script, self._REMOTE_PATH)

        assert payload not in scrubbed
        assert 'durable-secret' not in scrubbed
        assert 'base64 -d' not in scrubbed
        assert scrubbed.count(
            f"export SKYPILOT_CONFIG='{self._REMOTE_PATH}'") == 1
        assert scrubbed.count(serve_utils._VERSIONED_HA_CONFIG_MARKER) == 1  # pylint: disable=protected-access
        assert self._LAUNCH in scrubbed

    def test_removes_marked_payload_and_inserts_export_idempotently(self):
        script = (
            '# SKY_SERVE_CONFIG_SNAPSHOT_BEGIN version=9\n'
            'printf %s credential-payload | base64 -d > /tmp/config.yaml\n'
            '# SKY_SERVE_CONFIG_SNAPSHOT_END\n' + self._LAUNCH)

        scrubbed = serve_utils.strip_legacy_ha_recovery_config_payload(
            script, self._REMOTE_PATH)
        scrubbed_again = serve_utils.strip_legacy_ha_recovery_config_payload(
            scrubbed, self._REMOTE_PATH)

        assert scrubbed_again == scrubbed
        assert 'credential-payload' not in scrubbed
        assert 'SKY_SERVE_CONFIG_SNAPSHOT_' not in scrubbed
        assert scrubbed.count(serve_utils._VERSIONED_HA_CONFIG_MARKER) == 1  # pylint: disable=protected-access
        assert scrubbed.index('export SKYPILOT_CONFIG=') < scrubbed.index(
            '/usr/bin/python')

    def test_preserves_unrelated_base64_decode_and_marker_text(self):
        script = ('echo dXNlci1lbnRyeXBvaW50 | base64 -d | bash\n'
                  '# SKY_SERVE_VERSIONED_CONFIG_RECOVERY_V1\n' + self._LAUNCH)

        scrubbed = serve_utils.strip_legacy_ha_recovery_config_payload(
            script, self._REMOTE_PATH)

        assert 'echo dXNlci1lbnRyeXBvaW50 | base64 -d | bash' in scrubbed
        # The unrelated marker remains, alongside the generated marker/export
        # grammar. It is never used to select the recovery protocol.
        assert scrubbed.splitlines().count(
            serve_utils._VERSIONED_HA_CONFIG_MARKER) == 2  # pylint: disable=protected-access

    @pytest.mark.parametrize('markers', [
        '# SKY_SERVE_CONFIG_SNAPSHOT_BEGIN version=9\n',
        '# SKY_SERVE_CONFIG_SNAPSHOT_END\n',
        ('# SKY_SERVE_CONFIG_SNAPSHOT_BEGIN version=9\n'
         '# SKY_SERVE_CONFIG_SNAPSHOT_BEGIN version=10\n'
         '# SKY_SERVE_CONFIG_SNAPSHOT_END\n'
         '# SKY_SERVE_CONFIG_SNAPSHOT_END\n'),
        ('prefix # SKY_SERVE_CONFIG_SNAPSHOT_BEGIN version=9\n'
         '# SKY_SERVE_CONFIG_SNAPSHOT_END\n'),
    ])
    def test_malformed_markers_fail_closed(self, markers):
        with pytest.raises(ValueError, match='Malformed legacy Serve HA'):
            serve_utils.strip_legacy_ha_recovery_config_payload(
                markers + self._LAUNCH, self._REMOTE_PATH)


class TestSanitizeHaRecoveryConfigBytes:
    """Only the safe per-version controller projection reaches PostgreSQL."""

    def test_strips_vast_create_instance_kwargs(self):
        config = (b'active_workspace: mt_native\n'
                  b'workspaces:\n  mt_native: {}\n'
                  b'vast:\n'
                  b'  datacenter_only: true\n'
                  b'  create_instance_kwargs:\n'
                  b'    registry_password: hunter2\n')
        out = serve_utils.sanitize_ha_recovery_config_bytes(config)
        parsed = yaml.safe_load(out)
        assert b'hunter2' not in out
        assert 'create_instance_kwargs' not in parsed.get('vast', {})
        # Everything identity-relevant survives.
        assert parsed['active_workspace'] == 'mt_native'
        assert 'mt_native' in parsed['workspaces']
        assert parsed['vast']['datacenter_only'] is True

    @pytest.mark.parametrize('config',
                             [b'{: not yaml :', b'- not\n- a\n- mapping\n'])
    def test_invalid_or_nonmapping_config_fails_closed(self, config):
        with pytest.raises(ValueError, match='valid YAML|YAML mapping'):
            serve_utils.sanitize_ha_recovery_config_bytes(config)

    def test_strips_pod_config_including_per_context(self):
        config = (b'active_workspace: mt_native\n'
                  b'kubernetes:\n'
                  b'  allowed_contexts: [ctx-a, ctx-b]\n'
                  b'  pod_config:\n'
                  b'    spec:\n'
                  b'      containers:\n'
                  b'        - env:\n'
                  b'            - {name: REGISTRY_PASSWORD, value: hunter2}\n'
                  b'  context_configs:\n'
                  b'    ctx-a:\n'
                  b'      provision_timeout: 10\n'
                  b'      pod_config:\n'
                  b'        spec: {imagePullSecrets: [{name: sekret}]}\n'
                  b'ssh:\n'
                  b'  pod_config: {spec: {x: topsecret}}\n')
        out = serve_utils.sanitize_ha_recovery_config_bytes(config)
        assert b'hunter2' not in out
        assert b'sekret' not in out
        assert b'topsecret' not in out
        parsed = yaml.safe_load(out)
        # Non-credential neighbors survive.
        assert parsed['kubernetes']['allowed_contexts'] == ['ctx-a', 'ctx-b']
        assert parsed['kubernetes']['context_configs']['ctx-a'][
            'provision_timeout'] == 10

    def test_strips_extensions_and_keeps_safe_jobs_policy(self):
        config = (
            b'active_workspace: research\n'
            b'docker:\n  run_options: ["--env", "TOKEN=docker-secret"]\n'
            b'plugins:\n  arbitrary: {token: plugin-secret}\n'
            b'aws:\n  ssh_proxy_command: "proxy --token proxy-secret"\n'
            b'  labels: {token: cloud-label-secret}\n'
            b'jobs:\n'
            b'  bucket: https://user:pass@storage.example/jobs?sig=sas-secret\n'
            b'  force_disable_cloud_bucket: true\n'
            b'  plugin_extension: {token: jobs-extension-secret}\n'
            b'  controller:\n'
            b'    autostop: 30\n'
            b'    high_availability: true\n'
            b'    plugin_extension: {token: controller-extension-secret}\n'
            b'    resources:\n'
            b'      _docker_login_config:\n'
            b'        username: jobs-user\n'
            b'        password: jobs-controller-password\n'
            b'        server: registry.example\n'
            b'serve:\n'
            b'  controller:\n'
            b'    high_availability: true\n'
            b'    resources:\n'
            b'      _docker_login_config:\n'
            b'        username: serve-user\n'
            b'        password: serve-controller-password\n'
            b'        server: registry.example\n'
            b'      _cluster_config_overrides:\n'
            b'        arbitrary: serve-controller-override-secret\n'
            b'kubernetes:\n'
            b'  allowed_contexts: [east, phx]\n'
            b'  plugin_extension: {token: extension-secret}\n'
            b'workspaces:\n'
            b'  research:\n'
            b'    private: true\n'
            b'    allowed_users: [member@example.com]\n'
            b'    kubernetes:\n'
            b'      disabled: true\n'
            b'      allowed_contexts: [east, phx]\n'
            b'      plugin_extension: {token: workspace-secret}\n'
            b'  unrelated-tenant:\n'
            b'    aws: {security_group_name: tenant-private-policy}\n'
            b'container_registries:\n'
            b'  access_bindings:\n'
            b'    runtime:\n'
            b'      kind: aws_assume_role\n'
            b'      authority: arn:aws:iam::123456789012:role/runtime\n'
            b'      external_id: bounded-external-id\n'
            b'      purposes: [runtime_pull]\n')
        out = serve_utils.sanitize_ha_recovery_config_bytes(config)
        for sentinel in (b'docker-secret', b'plugin-secret',
                         b'extension-secret', b'workspace-secret',
                         b'jobs-extension-secret',
                         b'controller-extension-secret', b'member@example.com',
                         b'proxy-secret', b'jobs-controller-password',
                         b'cloud-label-secret', b'serve-controller-password',
                         b'serve-controller-override-secret', b'sas-secret',
                         b'pass@storage', b'bounded-external-id',
                         b'tenant-private-policy'):
            assert sentinel not in out
        parsed = yaml.safe_load(out)
        assert 'docker' not in parsed
        assert 'plugins' not in parsed
        assert parsed['jobs'] == {
            'controller': {
                'autostop': 30,
                'high_availability': True,
            },
        }
        assert parsed['serve'] == {
            'controller': {
                'high_availability': True,
            },
        }
        assert parsed['kubernetes']['allowed_contexts'] == ['east', 'phx']
        assert parsed['workspaces']['research']['kubernetes'][
            'allowed_contexts'] == ['east', 'phx']
        assert parsed['workspaces']['research']['kubernetes'][
            'disabled'] is True
        assert set(parsed['workspaces']) == {'research'}
        # Registry policy is validated as secret-free and is required to keep
        # managed-image placement stable after recovery.
        assert parsed['container_registries'] == {
            'access_bindings': {
                'runtime': {
                    'authority': 'arn:aws:iam::123456789012:role/runtime',
                    'kind': 'aws_assume_role',
                    'purposes': ['runtime_pull'],
                },
            },
        }

    def test_rejects_cyclic_yaml_aliases(self):
        config = (b'active_workspace: research\n'
                  b'workspaces: {research: {}}\n'
                  b'aws: &aws\n'
                  b'  labels: {token: secret}\n'
                  b'  loop: *aws\n')
        with pytest.raises(ValueError, match='cyclic YAML alias'):
            serve_utils.sanitize_ha_recovery_config_bytes(config)

    def test_rejects_snapshot_over_size_cap(self):
        with pytest.raises(ValueError, match='1MiB'):
            serve_utils.sanitize_ha_recovery_config_bytes(b'x' *
                                                          (1024 * 1024 + 1))


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
