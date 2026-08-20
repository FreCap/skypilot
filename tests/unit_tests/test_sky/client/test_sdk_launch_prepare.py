"""Focused tests for the client SDK launch preparation boundary."""

import contextlib
import json
from types import SimpleNamespace
from unittest import mock

import pytest

import sky
from sky.client import sdk
from sky.server import common as server_common
from sky.skylet import autostop_lib
from sky.usage import usage_lib
from sky.utils import context as sky_context
from sky.utils import dag_utils


def _response(request_id: str = 'request-id') -> mock.Mock:
    response = mock.Mock()
    response.status_code = 200
    response.headers = {'X-Skypilot-Request-ID': request_id}
    return response


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value,
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


@pytest.fixture
def prepare_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sdk, 'validate', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sdk.click, 'secho', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sdk.versions, 'get_remote_api_version', lambda: 13)
    monkeypatch.setattr(server_common, 'check_server_healthy_or_start_fn',
                        lambda *_args, **_kwargs: None)


def _prepare(
    task: sky.Task,
    *,
    cluster_name: str = 'test-cluster',
    extra_launch_context: dict[str, object] | None = None
) -> sdk.PreparedLaunchRequest:
    return sdk.prepare_launch_request(
        task,
        cluster_name=cluster_name,
        _file_mounts_blob_id='existing-blob',
        _extra_launch_context=extra_launch_context)


@pytest.mark.usefixtures('prepare_dependencies')
def test_public_launch_prepares_policy_and_mount_mutations(
        monkeypatch: pytest.MonkeyPatch) -> None:
    task = sky.Task(run='original')
    prepared_requests: list[sdk.PreparedLaunchRequest] = []

    @contextlib.contextmanager
    def _apply_policy(dag, **_kwargs):
        dag.tasks[0].run = 'after-policy'
        yield dag

    def _upload_mounts(dag):
        dag.tasks[0].update_envs({'MOUNT_MUTATION': 'included'})
        return dag, 'uploaded-blob'

    def _capture_submit(prepared):
        prepared_requests.append(prepared)
        return 'request-id'

    monkeypatch.setattr(sdk.admin_policy_utils,
                        'apply_and_use_config_in_current_request',
                        _apply_policy)
    monkeypatch.setattr(sdk.client_common, 'upload_mounts_to_api_server',
                        _upload_mounts)
    monkeypatch.setattr(sdk, 'submit_prepared_launch_request', _capture_submit)
    monkeypatch.setattr(server_common, 'check_server_healthy_or_start_fn',
                        lambda *_args, **_kwargs: None)

    assert sdk.launch(task, cluster_name='test-cluster') == 'request-id'

    assert len(prepared_requests) == 1
    prepared = prepared_requests[0]
    prepared_dag = dag_utils.load_dag_from_yaml_str(prepared.body.task)
    assert prepared_dag.tasks[0].run == 'after-policy'
    assert prepared_dag.tasks[0].envs['MOUNT_MUTATION'] == 'included'
    assert prepared.body.file_mounts_blob_id == 'uploaded-blob'


@pytest.mark.usefixtures('prepare_dependencies')
def test_public_launch_submits_the_identical_prepared_instance(
        monkeypatch: pytest.MonkeyPatch) -> None:
    prepared = object()
    prepare = mock.Mock(return_value=contextlib.nullcontext(prepared))

    def _submit(candidate):
        assert candidate is prepared
        return 'request-id'

    monkeypatch.setattr(sdk, '_prepared_launch_request_in_current_context',
                        prepare)
    monkeypatch.setattr(sdk, 'submit_prepared_launch_request', _submit)

    assert sdk.launch(sky.Task(run='echo hello'),
                      cluster_name='test-cluster') == 'request-id'
    prepare.assert_called_once()


@pytest.mark.usefixtures('prepare_dependencies')
def test_public_launch_submits_inside_policy_config_scope(
        monkeypatch: pytest.MonkeyPatch) -> None:
    policy_scope_active = False

    @contextlib.contextmanager
    def _apply_policy(dag, **_kwargs):
        nonlocal policy_scope_active
        policy_scope_active = True
        try:
            yield dag
        finally:
            policy_scope_active = False

    def _request(method, path, **_kwargs):
        assert policy_scope_active
        assert (method, path) == ('POST', '/launch')
        return _response()

    monkeypatch.setattr(sdk.admin_policy_utils,
                        'apply_and_use_config_in_current_request',
                        _apply_policy)
    monkeypatch.setattr(sdk.client_common, 'upload_mounts_to_api_server',
                        lambda dag: (dag, None))
    monkeypatch.setattr(server_common, 'make_authenticated_request', _request)

    assert sdk.launch(sky.Task(run='echo hello'),
                      cluster_name='test-cluster') == 'request-id'
    assert not policy_scope_active


def test_direct_prepare_preserves_launch_client_boundaries(
        monkeypatch: pytest.MonkeyPatch) -> None:
    prepared = object()
    health_check = mock.Mock()

    def _prepare_in_context(*_args, **_kwargs):
        assert sky_context.get() is not None
        assert usage_lib.messages.usage.entrypoint == 'sky.client.sdk.launch'
        return contextlib.nullcontext(prepared)

    monkeypatch.setattr(server_common, 'check_server_healthy_or_start_fn',
                        health_check)
    monkeypatch.setattr(sdk, '_prepared_launch_request_in_current_context',
                        _prepare_in_context)
    monkeypatch.setattr(usage_lib.messages.usage, 'entrypoint', None)

    assert sdk.prepare_launch_request(sky.Task(run='echo hello')) is prepared
    health_check.assert_called_once_with(False, '127.0.0.1')


def test_server_controller_prepare_is_local_and_uses_current_api(
        monkeypatch: pytest.MonkeyPatch) -> None:
    forbidden = mock.Mock(side_effect=AssertionError('ambient I/O is forbidden'))
    upload = mock.Mock(side_effect=AssertionError('upload is forbidden'))
    remote_version = mock.Mock(
        side_effect=AssertionError('remote version lookup is forbidden'))
    authenticated_request = mock.Mock(
        side_effect=AssertionError('HTTP is forbidden'))
    public_validate = mock.Mock(
        side_effect=AssertionError('HTTP validation is forbidden'))
    monkeypatch.setattr(sdk.admin_policy_utils,
                        'apply_and_use_config_in_current_request', forbidden)
    monkeypatch.setattr(sdk.client_common, 'upload_mounts_to_api_server',
                        upload)
    monkeypatch.setattr(sdk.client_common.server_common,
                        'is_api_server_local', forbidden)
    monkeypatch.setattr(sdk.payloads.common, 'is_api_server_local', forbidden)
    monkeypatch.setattr(sdk.payloads, 'request_body_env_vars', forbidden)
    monkeypatch.setattr(sdk.payloads,
                        'get_override_skypilot_config_from_client', forbidden)
    monkeypatch.setattr(sdk.payloads,
                        'get_override_skypilot_config_path_from_client',
                        forbidden)
    monkeypatch.setattr(sdk.versions, 'get_remote_api_version', remote_version)
    monkeypatch.setattr(server_common, 'make_authenticated_request',
                        authenticated_request)
    monkeypatch.setattr(sdk, 'validate', public_validate)

    prepared = sdk.prepare_launch_request_for_server_controller(
        sky.Task(run='echo hello'),
        'reserved-fill-1',
        workspace='default',
        extra_launch_context={'service_name': 'svc'})

    upload.assert_not_called()
    forbidden.assert_not_called()
    remote_version.assert_not_called()
    authenticated_request.assert_not_called()
    public_validate.assert_not_called()
    assert prepared.body.is_launched_by_sky_serve_controller
    assert prepared.body.client_api_version == sdk.server_constants.API_VERSION
    assert prepared.body.override_skypilot_config['active_workspace'] == (
        'default')
    assert prepared.body.extra_launch_context == {'service_name': 'svc'}


@pytest.mark.parametrize('local_input', ('workdir', 'file_mount', 'mapping',
                                         'storage', 'tls'))
def test_server_controller_prepare_rejects_local_inputs_before_http(
        monkeypatch: pytest.MonkeyPatch, tmp_path, local_input: str) -> None:
    monkeypatch.setattr(
        sdk.admin_policy_utils, 'apply_and_use_config_in_current_request',
        mock.Mock(side_effect=AssertionError('policy I/O is forbidden')))
    authenticated_request = mock.Mock(
        side_effect=AssertionError('HTTP is forbidden'))
    monkeypatch.setattr(server_common, 'make_authenticated_request',
                        authenticated_request)
    local_file = tmp_path / 'input.txt'
    local_file.write_text('data', encoding='utf-8')
    task = sky.Task(run='echo hello')
    if local_input == 'workdir':
        task.workdir = str(tmp_path)
    elif local_input == 'file_mount':
        task.file_mounts = {'/input': str(local_file)}
    elif local_input == 'mapping':
        task.file_mounts_mapping = {str(local_file): str(local_file)}
    elif local_input == 'storage':
        task.storage_mounts = {
            '/data': sky.Storage(name='server-controller-local-input',
                                 source=str(tmp_path))
        }
    else:
        task._service = SimpleNamespace(  # pylint: disable=protected-access
            tls_credential=SimpleNamespace(keyfile=str(local_file),
                                           certfile=str(local_file)))

    with pytest.raises(ValueError, match='cannot upload local inputs'):
        sdk.prepare_launch_request_for_server_controller(task,
                                                         'reserved-fill-1',
                                                         workspace='default')

    authenticated_request.assert_not_called()


@pytest.mark.usefixtures('prepare_dependencies')
def test_high_level_preparation_runs_mutable_steps_once_in_order(
        monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    request_options = []
    original_convert = sdk.dag_utils.convert_entrypoint_to_dag
    original_override = sky.Resources.override_autostop_config
    # pylint: disable=protected-access
    original_freeze = sdk._freeze_launch_request

    def _convert(entrypoint):
        events.append('convert')
        return original_convert(entrypoint)

    def _override(resource, *args, **kwargs):
        events.append('autostop')
        return original_override(resource, *args, **kwargs)

    @contextlib.contextmanager
    def _apply_policy(dag, **kwargs):
        events.append('policy-enter')
        request_options.append(kwargs['request_options'])
        resource = next(iter(dag.tasks[0].resources))
        assert resource.autostop_config is not None
        assert resource.autostop_config.idle_minutes == 7
        assert resource.autostop_config.down
        assert resource.autostop_config.wait_for == autostop_lib.AutostopWaitFor.JOBS
        dag.tasks[0].run = 'after-policy'
        try:
            yield dag
        finally:
            events.append('policy-exit')

    def _validate(*_args, **_kwargs):
        events.append('validate')

    def _upload(dag):
        events.append('upload')
        assert dag.tasks[0].run == 'after-policy'
        return dag, 'blob-id'

    def _freeze(*args, **kwargs):
        events.append('freeze-enter')
        prepared = original_freeze(*args, **kwargs)
        events.append('freeze-exit')
        return prepared

    monkeypatch.setattr(sdk.dag_utils, 'convert_entrypoint_to_dag', _convert)
    monkeypatch.setattr(sky.Resources, 'override_autostop_config', _override)
    monkeypatch.setattr(sdk.admin_policy_utils,
                        'apply_and_use_config_in_current_request',
                        _apply_policy)
    monkeypatch.setattr(sdk, 'validate', _validate)
    monkeypatch.setattr(sdk.client_common, 'upload_mounts_to_api_server',
                        _upload)
    monkeypatch.setattr(sdk, '_freeze_launch_request', _freeze)

    prepared = sdk.prepare_launch_request(
        sky.Task(run='before-policy'),
        cluster_name='test-cluster',
        idle_minutes_to_autostop=7,
        wait_for=autostop_lib.AutostopWaitFor.JOBS,
        down=True)

    assert events == [
        'convert', 'autostop', 'policy-enter', 'freeze-enter', 'validate',
        'upload', 'freeze-exit', 'policy-exit'
    ]
    assert len(request_options) == 1
    assert request_options[0].idle_minutes_to_autostop == 7
    assert request_options[0].down
    assert prepared.body.file_mounts_blob_id == 'blob-id'


@pytest.mark.usefixtures('prepare_dependencies')
@pytest.mark.parametrize(('remote_api_version', 'expected_wait_for'),
                         [(12, None), (13, autostop_lib.AutostopWaitFor.JOBS)])
def test_high_level_preparation_preserves_wait_for_compatibility(
        monkeypatch: pytest.MonkeyPatch, remote_api_version: int,
        expected_wait_for: autostop_lib.AutostopWaitFor | None) -> None:
    monkeypatch.setattr(sdk.versions, 'get_remote_api_version',
                        lambda: remote_api_version)
    prepared = sdk.prepare_launch_request(
        sky.Task(run='echo hello'),
        cluster_name='test-cluster',
        idle_minutes_to_autostop=9,
        wait_for=autostop_lib.AutostopWaitFor.JOBS,
        _file_mounts_blob_id='existing-blob')

    dag = dag_utils.load_dag_from_yaml_str(prepared.body.task)
    resource = next(iter(dag.tasks[0].resources))
    assert resource.autostop_config is not None
    assert resource.autostop_config.idle_minutes == 9
    assert resource.autostop_config.wait_for == expected_wait_for


@pytest.mark.usefixtures('prepare_dependencies')
def test_policy_scoped_config_is_frozen_before_scope_exit(
        monkeypatch: pytest.MonkeyPatch) -> None:
    policy_scope_active = False

    @contextlib.contextmanager
    def _apply_policy(dag, **_kwargs):
        nonlocal policy_scope_active
        policy_scope_active = True
        try:
            yield dag
        finally:
            policy_scope_active = False

    def _policy_config():
        assert policy_scope_active
        return {'kubernetes': {'allowed_contexts': ['policy-context']}}

    monkeypatch.setattr(sdk.admin_policy_utils,
                        'apply_and_use_config_in_current_request',
                        _apply_policy)
    monkeypatch.setattr(sdk.payloads,
                        'get_override_skypilot_config_from_client',
                        _policy_config)
    prepared = sdk.prepare_launch_request(sky.Task(run='echo hello'),
                                          cluster_name='test-cluster',
                                          _file_mounts_blob_id='existing-blob')

    assert not policy_scope_active
    payload = json.loads(prepared.submitted_json)
    assert payload['override_skypilot_config'] == {
        'kubernetes': {
            'allowed_contexts': ['policy-context']
        }
    }


@pytest.mark.usefixtures('prepare_dependencies')
def test_prepared_launch_commits_exact_submitted_json(
        monkeypatch: pytest.MonkeyPatch) -> None:
    prepared = _prepare(sky.Task(run='echo hello'))
    request = mock.Mock(return_value=_response())
    monkeypatch.setattr(server_common, 'make_authenticated_request', request)

    assert sdk.submit_prepared_launch_request(prepared) == 'request-id'

    submitted_json = request.call_args.kwargs['json']
    assert prepared.submitted_json.encode('utf-8') == prepared.submitted_bytes
    assert json.loads(prepared.submitted_json) == submitted_json
    assert _canonical_bytes(submitted_json) == prepared.submitted_bytes


@pytest.mark.usefixtures('prepare_dependencies')
def test_prepared_launch_is_detached_from_source_task() -> None:
    task = sky.Task(run='before-prepare')
    task.update_envs({'VALUE': 'before'})
    extra_launch_context = {'nested': {'value': 'before'}}
    prepared = _prepare(task, extra_launch_context=extra_launch_context)
    committed_bytes = prepared.submitted_bytes
    committed_task = prepared.body.task

    task.run = 'after-prepare'
    task.update_envs({'VALUE': 'after'})
    extra_launch_context['nested']['value'] = 'after'

    assert prepared.body.task == committed_task
    assert 'before-prepare' in prepared.body.task
    assert 'after-prepare' not in prepared.body.task
    assert prepared.body.extra_launch_context == {'nested': {'value': 'before'}}
    assert prepared.submitted_bytes == committed_bytes


@pytest.mark.usefixtures('prepare_dependencies')
def test_public_launch_submits_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sdk.admin_policy_utils,
                        'apply_and_use_config_in_current_request',
                        lambda dag, **_kwargs: contextlib.nullcontext(dag))
    monkeypatch.setattr(sdk.client_common, 'upload_mounts_to_api_server',
                        lambda dag: (dag, None))
    monkeypatch.setattr(server_common, 'check_server_healthy_or_start_fn',
                        lambda *_args, **_kwargs: None)
    request = mock.Mock(return_value=_response())
    monkeypatch.setattr(server_common, 'make_authenticated_request', request)

    assert sdk.launch(sky.Task(run='echo hello'),
                      cluster_name='test-cluster') == 'request-id'

    request.assert_called_once()
    assert request.call_args.args == ('POST', '/launch')


@pytest.mark.usefixtures('prepare_dependencies')
def test_prepared_launch_replays_equivalent_fresh_json(
        monkeypatch: pytest.MonkeyPatch) -> None:
    prepared = _prepare(sky.Task(run='echo hello'))
    inspected_body = prepared.body
    inspected_body.task = 'mutated inspection'
    inspected_body.extra_launch_context['mutated'] = True
    submitted: list[bytes] = []

    def _submit(_method, _path, **kwargs):
        submitted_json = kwargs['json']
        assert kwargs['timeout'] == 5
        submitted.append(_canonical_bytes(submitted_json))
        # Simulate a transport mutating its input.  The next submission must
        # still be decoded afresh from the immutable prepared bytes.
        submitted_json.clear()
        return _response(f'request-{len(submitted)}')

    monkeypatch.setattr(server_common, 'make_authenticated_request', _submit)

    assert sdk.submit_prepared_launch_request(prepared) == 'request-1'
    assert sdk.submit_prepared_launch_request(prepared) == 'request-2'
    assert submitted == [prepared.submitted_bytes, prepared.submitted_bytes]
    assert prepared.body.task != 'mutated inspection'
    assert 'mutated' not in prepared.body.extra_launch_context


@pytest.mark.usefixtures('prepare_dependencies')
def test_prepared_launch_rejects_inconsistent_direct_construction() -> None:
    prepared = _prepare(sky.Task(run='echo hello'))
    noncanonical = json.dumps(json.loads(prepared.submitted_json),
                              indent=2).encode('utf-8')
    with pytest.raises(ValueError, match='not canonical JSON'):
        sdk.PreparedLaunchRequest(submitted_bytes=noncanonical)

    unknown_field_payload = json.loads(prepared.submitted_json)
    unknown_field_payload['not_a_launch_body_field'] = True
    with pytest.raises(ValueError, match='does not exactly match'):
        sdk.PreparedLaunchRequest(
            submitted_bytes=_canonical_bytes(unknown_field_payload))
