"""Focused tests for the client SDK launch preparation boundary."""

import contextlib
import json
from unittest import mock

import pytest

import sky
from sky import admin_policy
from sky.client import sdk
from sky.server import common as server_common
from sky.utils import dag_utils


def _request_options(cluster_name: str) -> admin_policy.RequestOptions:
    return admin_policy.RequestOptions(cluster_name=cluster_name,
                                       idle_minutes_to_autostop=None,
                                       down=False,
                                       dryrun=False)


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


def _prepare(
    task: sky.Task,
    *,
    cluster_name: str = 'test-cluster',
    extra_launch_context: dict[str, object] | None = None
) -> sdk.PreparedLaunchRequest:
    dag = dag_utils.convert_entrypoint_to_dag(task)
    return sdk.prepare_launch_request(
        dag,
        cluster_name,
        _request_options(cluster_name),
        _file_mounts_blob_id='existing-blob',
        _extra_launch_context=extra_launch_context)


@pytest.mark.usefixtures('prepare_dependencies')
def test_public_launch_prepares_policy_and_mount_mutations(
        monkeypatch: pytest.MonkeyPatch) -> None:
    task = sky.Task(run='original')
    prepared_requests: list[sdk.PreparedLaunchRequest] = []
    original_prepare = sdk.prepare_launch_request

    @contextlib.contextmanager
    def _apply_policy(dag, **_kwargs):
        dag.tasks[0].run = 'after-policy'
        yield dag

    def _upload_mounts(dag):
        dag.tasks[0].update_envs({'MOUNT_MUTATION': 'included'})
        return dag, 'uploaded-blob'

    def _capture_prepare(*args, **kwargs):
        prepared = original_prepare(*args, **kwargs)
        prepared_requests.append(prepared)
        return prepared

    monkeypatch.setattr(sdk.admin_policy_utils,
                        'apply_and_use_config_in_current_request',
                        _apply_policy)
    monkeypatch.setattr(sdk.client_common, 'upload_mounts_to_api_server',
                        _upload_mounts)
    monkeypatch.setattr(sdk, 'prepare_launch_request', _capture_prepare)
    monkeypatch.setattr(server_common, 'check_server_healthy_or_start_fn',
                        lambda *_args, **_kwargs: None)
    request = mock.Mock(return_value=_response())
    monkeypatch.setattr(server_common, 'make_authenticated_request', request)

    assert sdk.launch(task, cluster_name='test-cluster') == 'request-id'

    assert len(prepared_requests) == 1
    prepared = prepared_requests[0]
    prepared_dag = dag_utils.load_dag_from_yaml_str(prepared.body.task)
    assert prepared_dag.tasks[0].run == 'after-policy'
    assert prepared_dag.tasks[0].envs['MOUNT_MUTATION'] == 'included'
    assert prepared.body.file_mounts_blob_id == 'uploaded-blob'


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
