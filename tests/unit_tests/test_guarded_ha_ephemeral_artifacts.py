"""Fail-closed tests for the fresh PostgreSQL guarded-HA profile."""
# pylint: disable=protected-access,redefined-outer-name

import ast
import builtins
import contextlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import fastapi
import pytest

from sky import core as sky_core
from sky import execution
from sky.jobs.server import core as jobs_core
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service as serve_service
from sky.serve.server import impl as serve_impl
from sky.server import common as server_common
from sky.server import file_mount_uploads
from sky.server import runtime as server_runtime
from sky.server import runtime_profile
from sky.server import server as api_server
from sky.server.requests import cutover
from sky.server.requests import executor
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.ssh_node_pools import server as ssh_node_pool_server

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GUARDED_ROUTE_INVENTORY = {
    'sky/server/file_mount_uploads.py': (
        'upload_zip_file',
        'check_blob_exists',
        'upload_blob',
    ),
    'sky/server/server.py': (
        'logs',
        'download_logs',
        'download',
        'provision_logs',
        'hook_logs',
        'stream',
        'create_debug_dump',
        'download_debug_dump',
    ),
    'sky/jobs/server/server.py': (
        'logs',
        'download_logs',
        'pool_tail_logs',
        'pool_download_logs',
    ),
    'sky/serve/server/server.py': (
        'tail_logs',
        'download_logs',
    ),
    'sky/ssh_node_pools/server.py': (
        'update_ssh_node_pools',
        'delete_ssh_node_pool',
        'upload_ssh_key',
    ),
}


@pytest.fixture
def guarded_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('SKYPILOT_API_REQUEST_BACKEND', 'postgres')
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'api')
    monkeypatch.setenv('SKYPILOT_API_SERVER_STORAGE_ENABLED', 'false')


@pytest.mark.parametrize(
    ('request_backend', 'role', 'storage_enabled', 'expected'), (
        ('postgres', 'api', 'false', True),
        ('postgres', 'executor', 'false', True),
        ('postgres', 'controller', 'false', True),
        ('sqlite', 'api', 'false', False),
        ('postgres', 'all', 'false', False),
        ('postgres', 'api', 'true', False),
    ))
def test_guarded_profile_is_derived_from_exact_runtime_facts(
        monkeypatch: pytest.MonkeyPatch, request_backend: str, role: str,
        storage_enabled: str, expected: bool) -> None:
    monkeypatch.setenv('SKYPILOT_API_REQUEST_BACKEND', request_backend)
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', role)
    monkeypatch.setenv('SKYPILOT_API_SERVER_STORAGE_ENABLED', storage_enabled)

    assert runtime_profile.guarded_ha_ephemeral_artifacts_enabled() is expected


def _first_executable_statement(
        function: ast.FunctionDef | ast.AsyncFunctionDef) -> ast.stmt:
    statements = function.body
    if (statements and isinstance(statements[0], ast.Expr) and
            isinstance(statements[0].value, ast.Constant) and
            isinstance(statements[0].value.value, str)):
        statements = statements[1:]
    assert statements
    return statements[0]


def test_local_byte_route_inventory_guards_before_body_or_path_access() -> None:
    """Keep the complete guarded route inventory fail-closed at statement 1."""
    for relative_path, names in _GUARDED_ROUTE_INVENTORY.items():
        source = (_REPO_ROOT / relative_path).read_text(encoding='utf-8')
        module = ast.parse(source, filename=relative_path)
        functions = {
            node.name: node
            for node in module.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert set(names) <= functions.keys(), relative_path
        for name in names:
            statement = _first_executable_statement(functions[name])
            assert isinstance(statement, ast.Expr), (relative_path, name)
            call = statement.value
            assert isinstance(call, ast.Call), (relative_path, name)
            assert isinstance(call.func, ast.Attribute), (relative_path, name)
            assert call.func.attr == 'reject_local_artifact_operation', (
                relative_path, name)


@pytest.mark.asyncio
async def test_upload_protocols_reject_without_validation_or_body_read(
        guarded_env: None) -> None:
    del guarded_env
    request = SimpleNamespace(stream=mock.AsyncMock(
        side_effect=AssertionError('body must not be consumed')))
    with mock.patch.object(file_mount_uploads.re,
                           'match',
                           side_effect=AssertionError(
                               'identifier validation must not run')) as match:
        with pytest.raises(runtime_profile.GuardedHALocalArtifactError):
            await file_mount_uploads.upload_zip_file(request, 'user', 'invalid',
                                                     0, 1)
        with pytest.raises(runtime_profile.GuardedHALocalArtifactError):
            await file_mount_uploads.check_blob_exists(request, 'user',
                                                       'invalid')
        with pytest.raises(runtime_profile.GuardedHALocalArtifactError):
            await file_mount_uploads.upload_blob(request, 'user', 'invalid', 0,
                                                 1)
    match.assert_not_called()
    request.stream.assert_not_awaited()


@pytest.mark.asyncio
async def test_ssh_publication_rejects_without_body_read(
        guarded_env: None) -> None:
    del guarded_env
    request = SimpleNamespace(
        json=mock.AsyncMock(side_effect=AssertionError('JSON body read')),
        form=mock.AsyncMock(side_effect=AssertionError('form body read')),
    )

    with pytest.raises(runtime_profile.GuardedHALocalArtifactError):
        await ssh_node_pool_server.update_ssh_node_pools(request)
    with pytest.raises(runtime_profile.GuardedHALocalArtifactError):
        await ssh_node_pool_server.upload_ssh_key(request)

    request.json.assert_not_awaited()
    request.form.assert_not_awaited()


def test_old_client_blob_id_rejected_before_request_admission(
        guarded_env: None) -> None:
    del guarded_env
    body = SimpleNamespace(file_mounts_blob_id='legacy-upload')
    with mock.patch.object(
            executor.role_filter,
            'reject_non_admin_pod_config',
            side_effect=AssertionError('request admission continued')) as role:
        with pytest.raises(runtime_profile.GuardedHALocalArtifactError):
            executor._build_request('request-id', None, body, lambda: None)
    role.assert_not_called()


class _Task:
    """Minimal task projection consumed by the guarded artifact validator."""

    def __init__(
        self,
        workdir: object,
        file_mounts: dict[str, object] | None = None,
        storage_mounts: dict[str, object] | None = None,
        service: object | None = None,
        file_mounts_mapping: dict[str, str] | None = None,
    ) -> None:
        self.workdir = workdir
        self.file_mounts = file_mounts or {}
        self.storage_mounts = storage_mounts or {}
        self.service = service
        self.file_mounts_mapping = file_mounts_mapping


def test_final_task_guard_accepts_remote_inputs(guarded_env: None) -> None:
    del guarded_env
    remote_task = _Task(
        {'url': 'https://example.test/repository.git'},
        file_mounts={'/weights': 's3://models/weights'},
        storage_mounts={
            '/data': SimpleNamespace(source=['gs://datasets/data'])
        })
    runtime_profile.validate_task_artifact_inputs(
        [remote_task], product='SkyServe', modified_catalogs_present=False)
    runtime_profile.validate_task_artifact_inputs(
        [_Task({'url': 'git@github.com:boltz-bio/example.git'})],
        product='SkyServe',
        modified_catalogs_present=False)


@pytest.mark.parametrize(('task', 'modified_catalogs', 'operation'), (
    (_Task('/local/workdir'), False, 'local workdir'),
    (_Task({'url': '/local/workdir'}), False, 'local workdir'),
    (_Task({'url': 'file:///local/workdir'}), False, 'local workdir'),
    (_Task({'url': 'file://server/local/workdir'}), False, 'local workdir'),
    (_Task({'url': 'http://localhost/repository.git'}), False, 'local workdir'),
    (_Task({'url': '//example.test/repository.git'}), False, 'local workdir'),
    (_Task({'url': 'http://['}), False, 'local workdir'),
    (_Task(None, {'/remote': '/local/file'}), False, 'local file mounts'),
    (_Task(None,
           storage_mounts={'/data': SimpleNamespace(source=['/local/dataset'])
                          }), False, 'local storage sources'),
    (_Task(None, service=SimpleNamespace(tls_credential=object())), False,
     'local TLS credentials'),
    (_Task(None), True, 'modified service catalogs'),
))
def test_final_task_guard_rejects_process_local_inputs(guarded_env: None,
                                                       task: _Task,
                                                       modified_catalogs: bool,
                                                       operation: str) -> None:
    del guarded_env
    with pytest.raises(runtime_profile.GuardedHALocalArtifactError,
                       match=operation):
        runtime_profile.validate_task_artifact_inputs(
            [task],
            product='SkyServe',
            modified_catalogs_present=modified_catalogs)


@pytest.mark.parametrize(('config', 'operation'), (
    ({
        'workdir': {
            'url': '/local/workdir'
        }
    }, 'local workdir'),
    ({
        'workdir': {
            'url': 'file:///local/workdir'
        }
    }, 'local workdir'),
    ({
        'workdir': {
            'url': 'file://server/local/workdir'
        }
    }, 'local workdir'),
    ({
        'workdir': {
            'url': 'http://127.0.0.1/repository.git'
        }
    }, 'local workdir'),
    ({
        'workdir': {
            'url': '//example.test/repository.git'
        }
    }, 'local workdir'),
    ({
        'workdir': {
            'url': 'http://['
        }
    }, 'local workdir'),
    ({
        'file_mounts': {
            's3://bucket/destination': '/local/source'
        }
    }, 'local file or storage mounts'),
    ({
        'file_mounts': {
            '/data': 'file://server/local/source'
        }
    }, 'local file or storage mounts'),
    ({
        'file_mounts': {
            '/data': 'custom://bucket/source'
        }
    }, 'local file or storage mounts'),
    ({
        'file_mounts': {
            '/data': {
                'source': ['s3://bucket/remote', '/local/source']
            }
        }
    }, 'local file or storage mounts'),
    ({
        'service': {
            'tls': {
                'keyfile': '/local/key',
                'certfile': '/local/cert'
            }
        }
    }, 'local TLS credentials'),
))
def test_serialized_guard_rejects_local_inputs_before_task_parsing(
        guarded_env: None, config: dict[str, object], operation: str) -> None:
    del guarded_env
    with pytest.raises(runtime_profile.GuardedHALocalArtifactError,
                       match=operation):
        runtime_profile.validate_serialized_task_artifact_inputs(
            [config], product='API request')


def test_serialized_guard_accepts_remote_storage_sources(
        guarded_env: None) -> None:
    del guarded_env
    runtime_profile.validate_serialized_task_artifact_inputs(
        [{
            'workdir': {
                'url': 'https://example.test/repository.git'
            },
            'file_mounts': {
                '/data': {
                    'name': 'dataset',
                    'source': ['s3://bucket/a', 'gs://bucket/b']
                }
            }
        }],
        product='API request')


def test_guarded_request_parses_remote_only_yaml_without_staging(
        guarded_env: None) -> None:
    del guarded_env
    task_yaml = ('resources:\n'
                 '  cloud: aws\n'
                 'file_mounts:\n'
                 '  /weights: s3://models/weights\n'
                 'run: echo ready\n')
    with mock.patch.object(
            Path,
            'mkdir',
            side_effect=AssertionError('staging directory created')), \
         mock.patch.object(
             Path,
             'write_text',
             side_effect=AssertionError('task bytes staged')):
        dag = server_common.process_mounts_in_task_on_api_server(
            task_yaml, {}, workdir_only=False)

    assert len(dag.tasks) == 1
    assert dag.tasks[0].file_mounts == {'/weights': 's3://models/weights'}


def test_guarded_request_rejects_legacy_mapping_without_staging(
        guarded_env: None) -> None:
    del guarded_env
    task_yaml = ('file_mounts_mapping:\n'
                 '  /local/file: uploaded/file\n'
                 'file_mounts:\n'
                 '  /remote/file: /local/file\n'
                 'run: echo rejected\n')
    with mock.patch.object(
            Path,
            'mkdir',
            side_effect=AssertionError('staging directory created')), \
         mock.patch.object(
             Path,
             'write_text',
             side_effect=AssertionError('task bytes staged')), \
         pytest.raises(runtime_profile.GuardedHALocalArtifactError,
                       match='local file mounts'):
        server_common.process_mounts_in_task_on_api_server(task_yaml, {},
                                                           workdir_only=False)


def test_guarded_yaml_lookup_never_reads_predecessor_path(
        guarded_env: None) -> None:
    del guarded_env
    with mock.patch.object(serve_state,
                           'get_yaml_content',
                           return_value=None), \
         mock.patch.object(
             serve_utils,
             'generate_task_yaml_file_name',
             side_effect=AssertionError('predecessor path derived')) as path, \
         pytest.raises(RuntimeError, match='committed PostgreSQL task YAML'):
        serve_utils.get_yaml_content('service', 3)
    path.assert_not_called()


def test_fresh_guarded_runtime_never_stats_legacy_cutover_gate(
        guarded_env: None) -> None:
    del guarded_env
    with mock.patch.object(
            cutover,
            'gate_path',
            side_effect=AssertionError('legacy gate path was derived')) as gate:
        cutover.require_completed_cutover_backend(postgres_configured=True,
                                                  postgres_backend=True,
                                                  sqlite_backend=False)
    gate.assert_not_called()


def test_guarded_controller_does_not_start_local_blob_cleanup_daemons(
        guarded_env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    del guarded_env
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'controller')
    created_tasks = []

    class _BackgroundLoop:
        """Capture scheduled daemon identities without starting a thread."""

        def create_task(self, task: object) -> None:
            created_tasks.append(task)

        def start(self) -> None:
            pass

    monkeypatch.setattr(server_runtime, '_BackgroundLoop', _BackgroundLoop)
    monkeypatch.setattr(server_runtime, '_uses_postgres_requests', lambda: True)
    monkeypatch.setattr(server_runtime, '_singleton_task', lambda name, factory:
                        (name, factory))

    server_runtime._start_background_loop('controller')

    names = {name for name, _ in created_tasks}
    assert 'unreferenced-file-mounts' not in names
    assert 'upload-staging-cleanup' not in names
    assert 'download-staging-cleanup' not in names


def test_guarded_error_has_stable_http_contract(guarded_env: None) -> None:
    del guarded_env
    error = runtime_profile.GuardedHALocalArtifactError('test operation')
    response = api_server.handle_guarded_ha_local_artifact_error(None, error)

    assert response.status_code == 501
    payload = json.loads(response.body)
    assert payload['detail']['code'] == (
        runtime_profile.GUARDED_HA_LOCAL_ARTIFACT_ERROR_CODE)
    assert payload['detail']['operation'] == 'test operation'


@pytest.mark.asyncio
async def test_api_get_reads_terminal_postgres_result_without_local_paths(
        guarded_env: None) -> None:
    """The durable result API remains available in the guarded profile."""
    del guarded_env
    encoded = object()
    request_record = mock.Mock(should_retry=False)
    request_record.get_error.return_value = None
    request_record.encode.return_value = encoded
    access_scope = SimpleNamespace(owner_user_id='owner')

    with mock.patch.object(api_server,
                           '_request_access_scope',
                           new=mock.AsyncMock(return_value=access_scope)), \
         mock.patch.object(api_server,
                           'get_expanded_request_id',
                           new=mock.AsyncMock(return_value='request-id')), \
         mock.patch.object(
             requests_lib,
             'get_request_status_async',
             new=mock.AsyncMock(return_value=requests_lib.StatusWithMsg(
                 status=requests_lib.RequestStatus.SUCCEEDED))), \
         mock.patch.object(
             requests_lib,
             'get_request_async',
             new=mock.AsyncMock(return_value=request_record)), \
         mock.patch.object(
             api_server,
             '_resolve_stream_log_path',
             side_effect=AssertionError('local log path resolved')) as logs, \
         mock.patch.object(
             server_common,
             'prepare_download_tmp_dir',
             side_effect=AssertionError('local download path created')) \
             as download:
        result = await api_server.api_get(SimpleNamespace(), 'request')

    assert result is encoded
    logs.assert_not_called()
    download.assert_not_called()


def test_policy_mutated_jobs_dag_rejects_before_mount_preparation(
        guarded_env: None) -> None:
    del guarded_env
    original = SimpleNamespace(metadata={})
    mutated_task = _Task('/policy/introduced/local/workdir')
    dag = mock.MagicMock(tasks=[mutated_task])

    with mock.patch.object(jobs_core.dag_utils,
                           'convert_entrypoint_to_dag',
                           return_value=dag), \
         mock.patch.object(jobs_core.admin_policy_utils,
                           'apply',
                           return_value=(dag, {})) as apply_policy, \
         mock.patch.object(
             jobs_core.service_catalog_common,
             'get_modified_catalog_file_mounts',
             return_value={}), \
         pytest.raises(runtime_profile.GuardedHALocalArtifactError,
                       match='local workdir'):
        jobs_core.launch(original)

    apply_policy.assert_called_once()
    dag.resolve_and_validate_volumes.assert_not_called()
    dag.pre_mount_volumes.assert_not_called()


def test_policy_mutated_optimize_dag_rejects_before_volume_resolution(
        guarded_env: None) -> None:
    del guarded_env
    mutated_task = _Task('/policy/introduced/local/workdir')
    dag = mock.MagicMock(tasks=[mutated_task])

    with mock.patch.object(
            sky_core.admin_policy_utils,
            'apply_and_use_config_in_current_request',
            return_value=contextlib.nullcontext(dag)), \
         mock.patch.object(
             runtime_profile.service_catalog_common,
             'get_modified_catalog_file_mounts',
             return_value={}), \
         pytest.raises(runtime_profile.GuardedHALocalArtifactError,
                       match='local workdir'):
        sky_core.optimize(dag)

    dag.resolve_and_validate_volumes.assert_not_called()


def test_policy_mutated_launch_dag_rejects_before_local_or_provider_work(
        guarded_env: None) -> None:
    del guarded_env
    mutated_task = _Task('/policy/introduced/local/workdir')
    resource = mock.MagicMock()
    resource.autostop_config = None
    mutated_task.resources = [resource]
    dag = mock.MagicMock(tasks=[mutated_task])

    with mock.patch.object(execution.dag_utils,
                           'convert_entrypoint_to_dag',
                           return_value=dag), \
         mock.patch.object(
             execution.admin_policy_utils,
             'apply_and_use_config_in_current_request',
             return_value=contextlib.nullcontext(dag)), \
         mock.patch.object(
             runtime_profile.service_catalog_common,
             'get_modified_catalog_file_mounts',
             return_value={}), \
         mock.patch.object(
             execution,
             '_execute_dag',
             side_effect=AssertionError('provider execution reached')) \
             as execute_dag, \
         pytest.raises(runtime_profile.GuardedHALocalArtifactError,
                       match='local workdir'):
        execution._execute(  # pylint: disable=protected-access
            mock.sentinel.entrypoint,
            _request_name=request_names.AdminPolicyRequestName.CLUSTER_LAUNCH)

    dag.resolve_and_validate_volumes.assert_not_called()
    dag.pre_mount_volumes.assert_not_called()
    execute_dag.assert_not_called()


@pytest.mark.asyncio
async def test_policy_mutated_validate_dag_rejects_before_volume_resolution(
        guarded_env: None) -> None:
    del guarded_env
    original_dag = mock.MagicMock()
    mutated_task = _Task('/policy/introduced/local/workdir')
    mutated_dag = mock.MagicMock(tasks=[mutated_task])
    request_context = SimpleNamespace(override_envs=mock.Mock())
    body = SimpleNamespace(dag='run: echo rejected',
                           env_vars={},
                           get_request_options=lambda: None)

    with mock.patch.object(api_server.context, 'initialize'), \
         mock.patch.object(api_server.context,
                           'get',
                           return_value=request_context), \
         mock.patch.object(api_server.dag_utils,
                           'load_dag_from_yaml_str',
                           return_value=original_dag), \
         mock.patch.object(
             api_server.admin_policy_utils,
             'apply_and_use_config_in_current_request',
             return_value=contextlib.nullcontext(mutated_dag)), \
         mock.patch.object(
             runtime_profile.service_catalog_common,
             'get_modified_catalog_file_mounts',
             return_value={}), \
         pytest.raises(fastapi.HTTPException) as error:
        await api_server.validate(body)

    assert error.value.status_code == 400
    mutated_dag.resolve_and_validate_volumes.assert_not_called()
    mutated_dag.validate.assert_not_called()


def test_policy_mutated_serve_dag_rejects_before_mount_or_provider_work(
        guarded_env: None) -> None:
    del guarded_env
    initial_task = mock.MagicMock()
    initial_task.service = SimpleNamespace(pool=False)
    mutated_task = _Task('/policy/introduced/local/workdir')
    dag = mock.MagicMock(tasks=[mutated_task])
    lifecycle_lock = mock.MagicMock()

    with mock.patch.object(serve_impl.serve_utils,
                           'lifecycle_lock_is_valid',
                           return_value=True), \
         mock.patch.object(serve_impl.serve_utils,
                           'is_consolidation_mode',
                           return_value=False), \
         mock.patch.object(serve_impl.serve_utils,
                           'get_service_lifecycle_epoch',
                           return_value=1), \
         mock.patch.object(serve_impl.serve_utils,
                           'validate_service_task'), \
         mock.patch.object(serve_impl.dag_utils,
                           'convert_entrypoint_to_dag',
                           return_value=dag), \
         mock.patch.object(serve_impl.admin_policy_utils,
                           'apply',
                           return_value=(dag, {})) as apply_policy, \
         mock.patch.object(
             serve_impl.service_catalog_common,
             'get_modified_catalog_file_mounts',
             return_value={}), \
         mock.patch.object(
             serve_impl.lb_k8s,
             'require_external_lb_runtime',
             side_effect=AssertionError('provider validation reached')) \
             as provider, \
         pytest.raises(runtime_profile.GuardedHALocalArtifactError,
                       match='local workdir'):
        serve_impl._up_impl_body(initial_task, 'service', False, lifecycle_lock)

    apply_policy.assert_called_once()
    dag.resolve_and_validate_volumes.assert_not_called()
    dag.pre_mount_volumes.assert_not_called()
    provider.assert_not_called()


def test_guarded_service_restart_recovers_only_from_postgres(
        guarded_env: None) -> None:
    """A service created after cutover survives loss of every pod-local file."""
    del guarded_env
    durable_yaml = ('service:\n'
                    '  readiness_probe: /health\n'
                    'resources:\n'
                    '  cloud: aws\n'
                    'run: echo ready\n')
    durable_config = b'active_workspace: default\nworkspaces: {default: {}}\n'
    recovery_spec = object()
    service_record = {
        'hash': 'incarnation-a',
        'lifecycle_epoch': 3,
        'controller_pid': 17,
        'controller_ip': '10.0.0.1',
        'status': serve_state.ServiceStatus.READY,
        'resource_scope': 'incarnation-a',
        'workspace': 'default',
    }
    stop = RuntimeError('stop after durable recovery inputs')
    file_lock = mock.MagicMock()

    with mock.patch.object(serve_service.maintenance,
                           'is_controller_hold_active',
                           return_value=False), \
         mock.patch.object(serve_service.auth_utils,
                           'get_or_generate_keys'), \
         mock.patch.object(
             serve_service,
             '_service_owner_from_launch_environment',
             return_value=('owner-id', 'owner')), \
         mock.patch.object(serve_service.serve_state,
                           'get_service_from_name',
                           return_value=service_record), \
         mock.patch.object(serve_service,
                           '_validate_recovery_target'), \
         mock.patch.object(serve_service.serve_utils,
                           'resolve_service_workspace',
                           return_value='default'), \
         mock.patch.object(
             serve_service.serve_state,
             'get_recovery_version_spec',
             return_value=(2, recovery_spec)), \
         mock.patch.object(
             serve_service.serve_utils,
             'generate_versioned_config_yaml_file_name',
             return_value='/ephemeral/config.yaml'), \
         mock.patch.object(
             serve_service.serve_utils,
             'generate_staged_config_yaml_file_name',
             return_value='/ephemeral/config.staged'), \
         mock.patch.object(
             serve_service.serve_state,
             'get_version_controller_config',
             return_value=(durable_config, 'digest', 'snapshot')), \
         mock.patch.object(serve_service.filelock,
                           'FileLock',
                           return_value=file_lock), \
         mock.patch.object(
             serve_service.serve_utils,
             'restore_version_controller_config',
             return_value=durable_config) as restore_config, \
         mock.patch.object(
             serve_service.serve_utils,
             'parse_and_validate_version_controller_config',
             return_value={'active_workspace': 'default'}), \
         mock.patch.object(
             serve_service.skypilot_config,
             'install_internal_config_snapshot'), \
         mock.patch.object(
             serve_service.serve_utils,
             'scrub_obsolete_controller_config_files'), \
         mock.patch.object(serve_service.serve_state,
                           'get_yaml_content',
                           return_value=durable_yaml) as get_yaml, \
         mock.patch.object(
             serve_service.replica_managers,
             'load_task_with_service_spec',
             side_effect=stop) as load_task, \
         mock.patch.object(
             builtins,
             'open',
             side_effect=AssertionError('predecessor file read')) as open_file, \
         pytest.raises(RuntimeError,
                       match='stop after durable recovery inputs'):
        serve_service._start('service', '/missing/task.yaml', 1, 'entrypoint')

    restore_config.assert_called_once()
    get_yaml.assert_called_once_with('service', 2)
    load_task.assert_called_once_with(durable_yaml, recovery_spec)
    open_file.assert_not_called()
