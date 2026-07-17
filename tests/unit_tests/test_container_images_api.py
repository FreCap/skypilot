"""API, SDK, and serialization tests for managed container images."""
# pylint: disable=comparison-with-callable,protected-access,redefined-outer-name

import asyncio
import logging
import pickle
import traceback
import types
from unittest import mock

from click.testing import CliRunner
import fastapi
from fastapi import testclient as fastapi_testclient
import orjson
import pytest

from sky import clouds
from sky import exceptions
from sky import resources as resources_lib
from sky import task as task_lib
from sky.client import sdk as sky_sdk
from sky.client.cli import command as cli_command
from sky.container_images import client
from sky.container_images import core
from sky.container_images import models
from sky.container_images import server
from sky.provision import docker_utils
from sky.schemas.api import responses
from sky.server import constants as server_constants
from sky.server import server as api_server
from sky.server import uvicorn as skyuvicorn
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.server.requests.serializers import decoders
from sky.server.requests.serializers import encoders
from sky.server.requests.serializers import return_value_serializers
from sky.utils import dag_utils
from sky.utils import registry

_DIGEST = 'sha256:' + 'a' * 64
_SOURCE = f'ghcr.io/boltz-bio/boltz@{_DIGEST}'
_ARTIFACT_ID = '11111111-1111-4111-8111-111111111111'
_LOCATION_ID = '22222222-2222-4222-8222-222222222222'


def _task_yaml_with_container_image(image: str, shape: str) -> str:
    if shape == 'direct':
        return f'resources:\n  container_image: {image}\n'
    assert shape in ('any_of', 'ordered')
    return (f'resources:\n  {shape}:\n'
            f'    - container_image: {image}\n')


def _task_yaml_with_legacy_docker_image(image: str, shape: str,
                                        form: str) -> str:
    if form == 'string':
        fields = f'image_id: docker:{image}'
    elif form == 'docker_key':
        fields = f'image_id:\n  docker: {image}'
    elif form == 'region_value':
        fields = f'image_id:\n  us-west-2: docker:{image}'
    elif form == 'unspecified':
        fields = f'image_id: {image}'
    elif form == 'wildcard':
        fields = f"infra: '*'\nimage_id: {image}"
    elif form == 'wildcard_region':
        fields = f"infra: '*/us-east-1'\nimage_id: {image}"
    elif form == 'ssh_infra':
        fields = f'infra: ssh/pool-a\nimage_id: {image}'
    elif form == 'ssh_cloud':
        fields = f'cloud: ssh\nregion: pool-a\nimage_id: {image}'
    else:
        assert form == 'kubernetes'
        fields = f'cloud: kubernetes\nimage_id: {image}'

    lines = fields.splitlines()
    if shape == 'direct':
        return 'resources:\n' + ''.join(f'  {line}\n' for line in lines)
    assert shape in ('any_of', 'ordered')
    candidate = f'    - {lines[0]}\n'
    candidate += ''.join(f'      {line}\n' for line in lines[1:])
    return f'resources:\n  {shape}:\n{candidate}'


def _task_yaml_with_inherited_kubernetes_image(image: str, shape: str,
                                               inheritance: str) -> str:
    assert shape in ('any_of', 'ordered')
    if inheritance == 'image_id_from_base':
        return (f'resources:\n  image_id: {image}\n  {shape}:\n'
                '    - cloud: kubernetes\n')
    assert inheritance == 'kubernetes_from_base'
    return (f'resources:\n  cloud: kubernetes\n  {shape}:\n'
            f'    - image_id: {image}\n')


def _task_yaml_with_nested_inherited_kubernetes_image(image: str,
                                                      outer_shape: str,
                                                      inner_shape: str) -> str:
    assert outer_shape in ('any_of', 'ordered')
    assert inner_shape in ('any_of', 'ordered')
    return (f'resources:\n  cloud: kubernetes\n  {outer_shape}:\n'
            f'    - image_id: {image}\n'
            f'      {inner_shape}:\n'
            '        - image_id: ubuntu:latest\n')


def _task_yaml_with_inherited_azure_image(image: str, shape: str) -> str:
    if shape == 'direct':
        return f'resources:\n  cloud: azure\n  image_id: {image}\n'
    assert shape in ('any_of', 'ordered')
    return (f'resources:\n  cloud: azure\n  {shape}:\n'
            f'    - image_id: {image}\n')


def _task_yaml_with_inline_docker_credentials(secret: str) -> str:
    return (f'resources:\n'
            f'  container_image: {_SOURCE}\n'
            f'  _docker_login_config:\n'
            f'    username: inline-user\n'
            f'    password: {secret}\n'
            f'    server: registry.example.com\n'
            f'secrets:\n'
            f'  SKYPILOT_DOCKER_USERNAME: inline-user\n'
            f'  SKYPILOT_DOCKER_PASSWORD: {secret}\n'
            f'  SKYPILOT_DOCKER_SERVER: registry.example.com\n')


def _record() -> responses.ContainerImageRecord:
    location = responses.ContainerImageLocationRecord(
        id=_LOCATION_ID,
        image_id=_ARTIFACT_ID,
        distribution='managed',
        target_id='aws-us-west-2',
        target_fingerprint='e' * 64,
        policy_fingerprint='f' * 64,
        profile_revision=1,
        canonical=False,
        target_ref=f'ecr-west.example/repo@{_DIGEST}',
        expected_digest=_DIGEST,
        state='READY',
        attempt_count=1,
        last_verified_at=100,
        last_used_at=101,
        auto_evict=True,
        updated_at=100,
    )
    return responses.ContainerImageRecord(
        id=_ARTIFACT_ID,
        workspace='research',
        source_ref=_SOURCE,
        resolved_source_ref=_SOURCE,
        source_digest=_DIGEST,
        sources=[_SOURCE],
        releases=['boltz-2.1.0'],
        producer_kind='external_oci',
        platforms=['linux/amd64'],
        compressed_size_bytes=100,
        created_at=100,
        updated_at=100,
        locations=[location],
    )


def _request() -> mock.MagicMock:
    request = mock.MagicMock()
    request.state.request_id = 'request-id'
    request.state.auth_user = mock.MagicMock(id='user-1')
    return request


def _unwrap(function):
    while hasattr(function, '__wrapped__'):
        function = function.__wrapped__
    return function


@pytest.fixture
def image_request_database(tmp_path):
    database = tmp_path / 'requests.db'
    logs = tmp_path / 'logs'
    logs.mkdir()
    with mock.patch.object(server_constants, 'API_SERVER_REQUEST_DB_PATH',
                           str(database)), mock.patch.object(
                               server_constants, 'REQUEST_LOG_PATH_PREFIX',
                               str(logs)):
        requests_lib._DB = None
        yield
        requests_lib._DB = None


def test_image_payload_rejects_credential_fields_before_persistence():
    with pytest.raises(ValueError, match='unsupported fields') as error:
        payloads.ImagePrepareBody(
            image={
                'ref': _SOURCE,
                'password': 'must-not-be-persisted',
            },
            targets=['canonical'],
        )
    assert 'must-not-be-persisted' not in str(error.value)
    with pytest.raises(ValueError, match='release'):
        payloads.ImagePrepareBody(
            image={
                'ref': _SOURCE,
                'release': 'two words',
            },
            targets=['canonical'],
        )


def test_legacy_docker_image_validation_precedes_value_free_warning(caplog):
    secret = 'direct-construction-secret'
    sky_logger = logging.getLogger('sky')
    sky_logger.addHandler(caplog.handler)
    try:
        with pytest.raises(ValueError) as error:
            resources_lib.Resources(
                image_id=(f'docker:user:{secret}@registry.example.com/repo'))
        assert secret not in str(error.value)
        assert secret not in caplog.text
        assert 'deprecated' not in caplog.text

        caplog.clear()
        resources_lib.Resources(image_id=f'docker:{_SOURCE}')
        assert 'Using image_id for a Docker image is deprecated' in caplog.text
        assert _SOURCE not in caplog.text
    finally:
        sky_logger.removeHandler(caplog.handler)


def test_local_dag_parser_never_logs_rejected_legacy_docker_image(caplog):
    secret = 'local-dag-secret'
    task_yaml = _task_yaml_with_legacy_docker_image(
        f'user:{secret}@registry.example.com/repo', 'direct', 'string')
    sky_logger = logging.getLogger('sky')
    sky_logger.addHandler(caplog.handler)
    try:
        with pytest.raises(ValueError) as error:
            dag_utils.load_chain_dag_from_yaml_str(task_yaml)
        assert secret not in str(error.value)
        assert secret not in caplog.text
        assert 'deprecated' not in caplog.text
    finally:
        sky_logger.removeHandler(caplog.handler)


@pytest.mark.parametrize('image_id', [
    {
        'docker': 'ubuntu:latest',
        'us-east-1': 'docker:user:ambiguous-secret@registry.example.com/repo',
    },
    {
        'us-east-1': 'docker:user:ambiguous-secret@registry.example.com/repo',
        'us-west-2': 'docker:ubuntu:latest',
    },
])
def test_legacy_docker_ambiguity_errors_do_not_reflect_values(image_id):
    with pytest.raises(ValueError) as error:
        resources_lib.Resources(cloud='aws', image_id=image_id)
    assert 'ambiguous-secret' not in str(error.value)


def test_explicit_container_image_rejects_inline_docker_credentials_locally():
    secret = 'local-inline-docker-secret'
    login = docker_utils.DockerLoginConfig(username='inline-user',
                                           password=secret,
                                           server='registry.example.com')
    with pytest.raises(ValueError) as resource_error:
        resources_lib.Resources(container_image=_SOURCE,
                                _docker_login_config=login)
    assert secret not in str(resource_error.value)

    credentials = {
        'SKYPILOT_DOCKER_USERNAME': 'inline-user',
        'SKYPILOT_DOCKER_PASSWORD': secret,
        'SKYPILOT_DOCKER_SERVER': 'registry.example.com',
    }
    for update_method in ('update_envs', 'update_secrets'):
        task = task_lib.Task(run='echo safe')
        task.set_resources(resources_lib.Resources(container_image=_SOURCE))
        with pytest.raises(ValueError) as task_error:
            getattr(task, update_method)(credentials)
        assert secret not in str(task_error.value)
        assert 'SKYPILOT_DOCKER_PASSWORD' not in task.envs
        assert 'SKYPILOT_DOCKER_PASSWORD' not in task.secrets

    task = task_lib.Task(run='echo safe')
    task.set_resources(resources_lib.Resources(container_image=_SOURCE))
    task._envs.update(credentials)
    with pytest.raises(ValueError) as serialization_error:
        task.to_yaml_config()
    assert secret not in str(serialization_error.value)

    sibling_with_login = resources_lib.Resources(cloud='aws',
                                                 image_id='ami-safe',
                                                 _docker_login_config=login)
    sibling_task = task_lib.Task(run='echo safe')
    with pytest.raises(ValueError) as sibling_error:
        sibling_task.set_resources({
            resources_lib.Resources(container_image=_SOURCE),
            sibling_with_login,
        })
    assert secret not in str(sibling_error.value)


def test_all_task_bodies_reject_inline_container_image_credentials():
    secret = 'eight-body-inline-secret'
    task_yaml = _task_yaml_with_inline_docker_credentials(secret)
    constructors = (
        lambda: payloads.ValidateBody(dag=task_yaml, request_options=None),
        lambda: payloads.OptimizeBody(dag=task_yaml, request_options=None),
        lambda: payloads.LaunchBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.ExecBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.JobsLaunchBody(task=task_yaml, name=None),
        lambda: payloads.ServeUpBody(task=task_yaml, service_name='service'),
        lambda: payloads.ServeUpdateBody(
            task=task_yaml, service_name='service', mode='rolling'),
        lambda: payloads.JobsPoolApplyBody(
            task=task_yaml, pool_name='pool', mode='rolling'),
    )
    for constructor in constructors:
        with pytest.raises(ValueError) as error:
            constructor()
        assert secret not in str(error.value)


@pytest.mark.parametrize('field', ['envs', 'secrets'])
def test_task_preflight_rejects_top_level_inline_docker_credentials(field):
    secret = 'top-level-inline-secret'
    task_yaml = (f'resources:\n  container_image: {_SOURCE}\n'
                 f'{field}:\n'
                 '  SKYPILOT_DOCKER_USERNAME: inline-user\n'
                 f'  SKYPILOT_DOCKER_PASSWORD: {secret}\n'
                 '  SKYPILOT_DOCKER_SERVER: registry.example.com\n')
    with pytest.raises(ValueError) as error:
        payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert secret not in str(error.value)


@pytest.mark.parametrize('shape', ['any_of', 'ordered'])
@pytest.mark.parametrize('credentials_in_parent', [False, True])
def test_task_preflight_rejects_inherited_inline_docker_credentials(
        shape, credentials_in_parent):
    secret = 'inherited-inline-secret'
    image = f'container_image: {_SOURCE}'
    login = ('_docker_login_config:\n'
             '  username: inline-user\n'
             f'  password: {secret}\n'
             '  server: registry.example.com')
    parent = login if credentials_in_parent else image
    child = image if credentials_in_parent else login
    task_yaml = ('resources:\n' +
                 ''.join(f'  {line}\n' for line in parent.splitlines()) +
                 f'  {shape}:\n' + f'    - {child.splitlines()[0]}\n' +
                 ''.join(f'      {line}\n' for line in child.splitlines()[1:]))
    with pytest.raises(ValueError) as error:
        payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert secret not in str(error.value)


@pytest.mark.parametrize('shape', ['any_of', 'ordered'])
def test_task_preflight_rejects_sibling_inline_docker_credentials(shape):
    secret = 'sibling-inline-secret'
    task_yaml = (f'resources:\n  {shape}:\n'
                 f'    - container_image: {_SOURCE}\n'
                 '    - _docker_login_config:\n'
                 '        username: inline-user\n'
                 f'        password: {secret}\n'
                 '        server: registry.example.com\n')
    with pytest.raises(ValueError) as error:
        payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert secret not in str(error.value)


@pytest.mark.parametrize(
    'path,serialized_field,extra_body',
    [
        ('/validate', 'dag', {
            'request_options': None
        }),
        ('/optimize', 'dag', {
            'request_options': None
        }),
        ('/launch', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/exec', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/jobs/launch', 'task', {
            'name': None
        }),
        ('/serve/up', 'task', {
            'service_name': 'service'
        }),
        ('/serve/update', 'task', {
            'service_name': 'service',
            'mode': 'rolling'
        }),
        ('/jobs/pool_apply', 'task', {
            'pool_name': 'pool',
            'mode': 'rolling'
        }),
    ],
)
def test_task_routes_never_schedule_inline_container_image_credentials(
        path, serialized_field, extra_body):
    secret = 'router-inline-docker-secret'
    task_yaml = _task_yaml_with_inline_docker_credentials(secret)
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule):
        response = test_client.post(path,
                                    json={
                                        serialized_field: task_yaml,
                                        **extra_body
                                    })
    assert response.status_code == 422
    assert secret not in response.text
    assert task_yaml not in response.text
    schedule.assert_not_awaited()


@pytest.mark.parametrize(
    'path,serialized_field,extra_body',
    [
        ('/validate', 'dag', {
            'request_options': None
        }),
        ('/optimize', 'dag', {
            'request_options': None
        }),
        ('/launch', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/exec', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/jobs/launch', 'task', {
            'name': None
        }),
        ('/serve/up', 'task', {
            'service_name': 'service'
        }),
        ('/serve/update', 'task', {
            'service_name': 'service',
            'mode': 'rolling'
        }),
        ('/jobs/pool_apply', 'task', {
            'pool_name': 'pool',
            'mode': 'rolling'
        }),
    ],
)
@pytest.mark.parametrize(
    'encoded_key,value',
    [
        ('container\\u005fimage',
         'https://user:encoded-secret@registry.example/repo'),
        ('image\\u005fid', 'docker:user:encoded-secret@registry.example/repo'),
    ],
)
def test_task_routes_scrub_encoded_image_keys_before_reflecting_errors(
        path, serialized_field, extra_body, encoded_key, value):
    task_yaml = f'resources:\n  "{encoded_key}": "{value}"\n'
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule):
        response = test_client.post(path,
                                    json={
                                        serialized_field: task_yaml,
                                        **extra_body
                                    })
    assert response.status_code == 422
    assert 'encoded-secret' not in response.text
    assert task_yaml not in response.text
    schedule.assert_not_awaited()


@pytest.mark.asyncio
async def test_request_row_revalidates_inline_container_image_credentials(
        image_request_database):
    del image_request_database
    secret = 'request-row-inline-secret'
    task_yaml = _task_yaml_with_inline_docker_credentials(secret)
    # Bypass Pydantic deliberately to exercise the final durable-row fence.
    unsafe_body = payloads.LaunchBody.model_construct(task=task_yaml,
                                                      cluster_name='cluster')
    request = requests_lib.Request(
        request_id='inline-container-image-credentials',
        name=(server_constants.REQUEST_NAME_PREFIX +
              request_names.RequestName.CLUSTER_LAUNCH.value),
        entrypoint=core.status,
        request_body=unsafe_body,
        status=requests_lib.RequestStatus.PENDING,
        created_at=0,
        user_id='user-1')
    with pytest.raises(ValueError) as error:
        await requests_lib.create_if_not_exists_async(request)
    assert secret not in str(error.value)
    assert requests_lib.get_request(request.request_id) is None


@pytest.mark.asyncio
async def test_request_row_strips_server_owned_client_config_before_persistence(
        image_request_database):
    del image_request_database
    secret = 'server-owned-override-secret'
    body = payloads.ImageStatusBody(
        image='safe-release',
        override_skypilot_config={
            'container_registries': {
                'credential': secret,
            },
            'workspaces': {
                'research': {
                    'container_images': secret,
                },
            },
            'jobs': {
                'controller': {
                    'consolidation_mode': secret,
                    'remote_identity': 'allowed-client-value',
                },
            },
            'aws': {
                'use_internal_ips': True,
            },
        })
    request = requests_lib.Request(
        request_id='server-owned-config-fence',
        name=(server_constants.REQUEST_NAME_PREFIX +
              request_names.RequestName.IMAGE_STATUS.value),
        entrypoint=core.status,
        request_body=body,
        status=requests_lib.RequestStatus.PENDING,
        created_at=0,
        user_id='user-1')

    assert await requests_lib.create_if_not_exists_async(request)
    stored = requests_lib.get_request(request.request_id)
    assert stored is not None
    stored_override = stored.request_body.override_skypilot_config
    assert stored_override == {
        'jobs': {
            'controller': {
                'remote_identity': 'allowed-client-value',
            },
        },
        'aws': {
            'use_internal_ips': True,
        },
    }
    assert secret not in repr(stored.request_body)


def test_request_row_task_fence_ignores_non_yaml_task_identifiers():
    body = payloads.JobsLogsBody(name=None, job_id=1, task='worker-name')
    payloads.validate_task_request_body_for_persistence(body)


def test_task_preflight_preserves_opaque_scalar_payload():
    serialized_task = 'test_task_yaml'
    body = payloads.LaunchBody(task=serialized_task, cluster_name='cluster')

    assert body.task == serialized_task
    assert not payloads.serialized_task_uses_container_image(serialized_task)


def test_task_preflight_rejects_non_mapping_sequence():
    secret = 'sequence-image-secret'
    serialized_task = ('- resources:\n'
                       '    container_image: '
                       f'https://user:{secret}@registry.example/repo\n')

    with pytest.raises(ValueError) as error:
        payloads.LaunchBody(task=serialized_task, cluster_name='cluster')
    assert secret not in str(error.value)


@pytest.mark.parametrize('resource_shape', ['direct', 'any_of', 'ordered'])
def test_task_payloads_reject_credential_bearing_container_images(
        resource_shape):
    secret = 'supersecret'
    task_yaml = _task_yaml_with_container_image(
        f'https://user:{secret}@registry.example.com/repo', resource_shape)
    constructors = (
        lambda: payloads.LaunchBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.ExecBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.OptimizeBody(dag=task_yaml, request_options=None),
        lambda: payloads.JobsLaunchBody(task=task_yaml, name=None),
        lambda: payloads.ServeUpBody(task=task_yaml, service_name='service'),
        lambda: payloads.ServeUpdateBody(
            task=task_yaml, service_name='service', mode='rolling'),
        lambda: payloads.JobsPoolApplyBody(
            task=task_yaml, pool_name='pool', mode='rolling'),
    )
    for constructor in constructors:
        with pytest.raises(ValueError) as error:
            constructor()
        assert secret not in str(error.value)


@pytest.mark.parametrize('resource_shape', ['direct', 'any_of', 'ordered'])
def test_task_payloads_accept_valid_container_image_resource_forms(
        resource_shape):
    task_yaml = _task_yaml_with_container_image(_SOURCE, resource_shape)
    body = payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert body.task == task_yaml


@pytest.mark.parametrize('resource_shape', ['direct', 'any_of', 'ordered'])
@pytest.mark.parametrize('legacy_form', [
    'string', 'docker_key', 'region_value', 'kubernetes', 'unspecified',
    'wildcard', 'wildcard_region', 'ssh_infra', 'ssh_cloud'
])
def test_task_payloads_reject_credential_bearing_legacy_docker_image_ids(
        resource_shape, legacy_form):
    secret = 'supersecret'
    task_yaml = _task_yaml_with_legacy_docker_image(
        f'user:{secret}@registry.example.com/repo', resource_shape, legacy_form)
    constructors = (
        lambda: payloads.LaunchBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.ExecBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.OptimizeBody(dag=task_yaml, request_options=None),
        lambda: payloads.JobsLaunchBody(task=task_yaml, name=None),
        lambda: payloads.ServeUpBody(task=task_yaml, service_name='service'),
        lambda: payloads.ServeUpdateBody(
            task=task_yaml, service_name='service', mode='rolling'),
        lambda: payloads.JobsPoolApplyBody(
            task=task_yaml, pool_name='pool', mode='rolling'),
    )
    for constructor in constructors:
        with pytest.raises(ValueError) as error:
            constructor()
        assert secret not in str(error.value)


@pytest.mark.parametrize('resource_shape', ['direct', 'any_of', 'ordered'])
@pytest.mark.parametrize('legacy_form', [
    'string', 'docker_key', 'region_value', 'kubernetes', 'unspecified',
    'wildcard', 'wildcard_region', 'ssh_infra', 'ssh_cloud'
])
def test_task_payloads_accept_valid_legacy_docker_image_id_forms(
        resource_shape, legacy_form):
    task_yaml = _task_yaml_with_legacy_docker_image(_SOURCE, resource_shape,
                                                    legacy_form)
    body = payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert body.task == task_yaml
    assert payloads.serialized_task_uses_container_image(task_yaml)


@pytest.mark.parametrize('resource_shape', ['any_of', 'ordered'])
@pytest.mark.parametrize('inheritance',
                         ['image_id_from_base', 'kubernetes_from_base'])
def test_task_payloads_validate_effective_inherited_kubernetes_image_id(
        resource_shape, inheritance):
    secret = 'supersecret'
    invalid_task = _task_yaml_with_inherited_kubernetes_image(
        f'user:{secret}@registry.example.com/repo', resource_shape, inheritance)
    with pytest.raises(ValueError) as error:
        payloads.LaunchBody(task=invalid_task, cluster_name='cluster')
    assert secret not in str(error.value)

    valid_task = _task_yaml_with_inherited_kubernetes_image(
        _SOURCE, resource_shape, inheritance)
    body = payloads.LaunchBody(task=valid_task, cluster_name='cluster')
    assert body.task == valid_task
    assert payloads.serialized_task_uses_container_image(valid_task)


@pytest.mark.parametrize('outer_shape', ['any_of', 'ordered'])
@pytest.mark.parametrize('inner_shape', ['any_of', 'ordered'])
def test_task_payloads_reject_nested_inherited_kubernetes_image_id(
        outer_shape, inner_shape):
    secret = 'topsecret'
    task_yaml = _task_yaml_with_nested_inherited_kubernetes_image(
        f'user:{secret}@registry.example/repo', outer_shape, inner_shape)
    constructors = (
        lambda: payloads.LaunchBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.ExecBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.OptimizeBody(dag=task_yaml, request_options=None),
        lambda: payloads.JobsLaunchBody(task=task_yaml, name=None),
        lambda: payloads.ServeUpBody(task=task_yaml, service_name='service'),
        lambda: payloads.ServeUpdateBody(
            task=task_yaml, service_name='service', mode='rolling'),
        lambda: payloads.JobsPoolApplyBody(
            task=task_yaml, pool_name='pool', mode='rolling'),
    )
    for constructor in constructors:
        with pytest.raises(ValueError) as error:
            constructor()
        assert secret not in str(error.value)


def test_task_payload_does_not_classify_cloud_vm_image_id_as_container_image():
    task_yaml = 'resources:\n  cloud: aws\n  image_id: ami-0123456789abcdef0\n'
    body = payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert body.task == task_yaml
    assert not payloads.serialized_task_uses_container_image(task_yaml)


def test_task_payload_classifies_unscoped_image_id_before_cloud_selection():
    task_yaml = f'resources:\n  image_id: {_SOURCE}\n'
    body = payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert body.task == task_yaml
    assert payloads.serialized_task_uses_container_image(task_yaml)


@pytest.mark.parametrize('infra', ['*', '*/us-east-1'])
def test_task_payload_classifies_wildcard_infra_image_id(infra):
    task_yaml = f"resources:\n  infra: '{infra}'\n  image_id: {_SOURCE}\n"
    body = payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
    assert body.task == task_yaml
    assert payloads.serialized_task_uses_container_image(task_yaml)


def test_preflight_covers_every_registered_kubernetes_subclass():
    container_clouds = {
        name for name, cloud in registry.CLOUD_REGISTRY.items()
        if isinstance(cloud, clouds.Kubernetes)
    }
    assert {'kubernetes', 'ssh'} <= container_clouds
    for cloud_name in container_clouds:
        task_yaml = (f'resources:\n  cloud: {cloud_name}\n'
                     f'  image_id: {_SOURCE}\n')
        body = payloads.LaunchBody(task=task_yaml, cluster_name='cluster')
        assert body.task == task_yaml
        assert payloads.serialized_task_uses_container_image(task_yaml)


@pytest.mark.parametrize('resource_shape', ['direct', 'any_of', 'ordered'])
@pytest.mark.parametrize('image_id', [
    'publisher:offer:sku:version',
    '/CommunityGalleries/gallery-name/Images/image-name',
])
def test_task_payloads_accept_inherited_azure_vm_image_id(
        resource_shape, image_id):
    task_yaml = _task_yaml_with_inherited_azure_image(image_id, resource_shape)
    constructors = (
        lambda: payloads.ValidateBody(dag=task_yaml, request_options=None),
        lambda: payloads.OptimizeBody(dag=task_yaml, request_options=None),
        lambda: payloads.LaunchBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.ExecBody(task=task_yaml, cluster_name='cluster'),
        lambda: payloads.JobsLaunchBody(task=task_yaml, name=None),
        lambda: payloads.ServeUpBody(task=task_yaml, service_name='service'),
        lambda: payloads.ServeUpdateBody(
            task=task_yaml, service_name='service', mode='rolling'),
        lambda: payloads.JobsPoolApplyBody(
            task=task_yaml, pool_name='pool', mode='rolling'),
    )
    for constructor in constructors:
        constructor()
    assert not payloads.serialized_task_uses_container_image(task_yaml)
    dag = dag_utils.load_dag_from_yaml_str(task_yaml)
    assert len(dag.tasks) == 1


@pytest.mark.parametrize(
    'path,serialized_field,extra_body',
    [
        ('/validate', 'dag', {
            'request_options': None
        }),
        ('/optimize', 'dag', {
            'request_options': None
        }),
        ('/launch', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/exec', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/jobs/launch', 'task', {
            'name': None
        }),
        ('/serve/up', 'task', {
            'service_name': 'service'
        }),
        ('/serve/update', 'task', {
            'service_name': 'service',
            'mode': 'rolling'
        }),
        ('/jobs/pool_apply', 'task', {
            'pool_name': 'pool',
            'mode': 'rolling'
        }),
    ],
)
@pytest.mark.parametrize('resource_shape', ['direct', 'any_of', 'ordered'])
def test_task_routes_never_persist_or_reflect_rejected_container_image(
        path, serialized_field, extra_body, resource_shape):
    secret = 'supersecret'
    task_yaml = _task_yaml_with_container_image(
        f'https://user:{secret}@registry.example.com/repo', resource_shape)
    body = {serialized_field: task_yaml, **extra_body}
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule):
        response = test_client.post(path, json=body)

    assert response.status_code == 422
    assert response.json()['detail']
    assert secret not in response.text
    assert task_yaml not in response.text
    schedule.assert_not_awaited()


def test_task_route_rejects_server_managed_pull_plan_before_persistence():
    secret = 'server-plan-secret'
    task_yaml = f'''\
resources:
  cloud: aws
  _resolved_container_image:
    image_id: {_ARTIFACT_ID}
    location_id: {_LOCATION_ID}
    reference: user:{secret}@registry.example.com/repo@{_DIGEST}
    target_id: canonical
    distribution: managed
    profile_revision: 1
    policy_fingerprint: {'b' * 64}
    digest: {_DIGEST}
    auth_strategy: anonymous
run: echo rejected
'''
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule):
        response = test_client.post('/launch',
                                    json={
                                        'task': task_yaml,
                                        'cluster_name': 'cluster',
                                    })

    assert response.status_code == 422
    assert response.json()['detail']
    assert secret not in response.text
    assert task_yaml not in response.text
    schedule.assert_not_awaited()


@pytest.mark.parametrize(
    'path,serialized_field,extra_body',
    [
        ('/validate', 'dag', {
            'request_options': None
        }),
        ('/optimize', 'dag', {
            'request_options': None
        }),
        ('/launch', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/exec', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/jobs/launch', 'task', {
            'name': None
        }),
        ('/serve/up', 'task', {
            'service_name': 'service'
        }),
        ('/serve/update', 'task', {
            'service_name': 'service',
            'mode': 'rolling'
        }),
        ('/jobs/pool_apply', 'task', {
            'pool_name': 'pool',
            'mode': 'rolling'
        }),
    ],
)
@pytest.mark.parametrize('resource_shape', ['direct', 'any_of', 'ordered'])
@pytest.mark.parametrize('legacy_form', [
    'string', 'docker_key', 'region_value', 'kubernetes', 'unspecified',
    'wildcard', 'wildcard_region', 'ssh_infra', 'ssh_cloud'
])
def test_task_routes_never_persist_or_reflect_rejected_legacy_docker_image_id(
        path, serialized_field, extra_body, resource_shape, legacy_form):
    secret = 'supersecret'
    task_yaml = _task_yaml_with_legacy_docker_image(
        f'user:{secret}@registry.example.com/repo', resource_shape, legacy_form)
    body = {serialized_field: task_yaml, **extra_body}
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule):
        response = test_client.post(path, json=body)

    assert response.status_code == 422
    assert response.json()['detail']
    assert secret not in response.text
    assert task_yaml not in response.text
    schedule.assert_not_awaited()


@pytest.mark.parametrize(
    'path,serialized_field,extra_body',
    [
        ('/validate', 'dag', {
            'request_options': None
        }),
        ('/optimize', 'dag', {
            'request_options': None
        }),
        ('/launch', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/exec', 'task', {
            'cluster_name': 'cluster'
        }),
        ('/jobs/launch', 'task', {
            'name': None
        }),
        ('/serve/up', 'task', {
            'service_name': 'service'
        }),
        ('/serve/update', 'task', {
            'service_name': 'service',
            'mode': 'rolling'
        }),
        ('/jobs/pool_apply', 'task', {
            'pool_name': 'pool',
            'mode': 'rolling'
        }),
    ],
)
def test_task_routes_reject_nested_inherited_kubernetes_image_id(
        path, serialized_field, extra_body):
    secret = 'topsecret'
    task_yaml = _task_yaml_with_nested_inherited_kubernetes_image(
        f'user:{secret}@registry.example/repo', 'any_of', 'any_of')
    body = {serialized_field: task_yaml, **extra_body}
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule):
        response = test_client.post(path, json=body)

    assert response.status_code == 422
    assert secret not in response.text
    assert task_yaml not in response.text
    schedule.assert_not_awaited()


@pytest.mark.asyncio
async def test_image_terminal_errors_are_value_free_in_request_db_and_api(
        image_request_database):
    del image_request_database
    secret = 'supersecret'
    unsafe_error = ValueError(f'provider token={secret}')
    names = (
        request_names.RequestName.IMAGE_PUBLISH,
        request_names.RequestName.IMAGE_REGISTER,
        request_names.RequestName.IMAGE_PREPARE,
        request_names.RequestName.IMAGE_STATUS,
        request_names.RequestName.IMAGE_RETRY,
    )
    for index, name in enumerate(names):
        request_id = f'image-secret-{index}'
        request = requests_lib.Request(
            request_id=request_id,
            name=server_constants.REQUEST_NAME_PREFIX + name.value,
            entrypoint=core.status,
            request_body=payloads.ImageStatusBody(image='safe-release',
                                                  workspace='research'),
            status=requests_lib.RequestStatus.RUNNING,
            created_at=0,
            user_id='user-1')
        assert await requests_lib.create_if_not_exists_async(request)
        if index % 2:
            await requests_lib.set_request_failed_async(request_id,
                                                        unsafe_error)
        else:
            requests_lib.set_request_failed(request_id, unsafe_error)

        stored = requests_lib.get_request(request_id)
        assert stored is not None
        error = stored.get_error()
        assert error is not None
        assert error['message'] == (
            requests_lib._CONTAINER_IMAGE_REQUEST_ERROR_MESSAGE)
        assert secret not in str(error['object'])
        assert secret not in getattr(error['object'], 'stacktrace', '')

        with pytest.raises(fastapi.HTTPException) as response:
            await api_server.api_get(request_id)
        assert response.value.status_code == 500
        api_request = requests_lib.Request.decode(
            payloads.RequestPayload(**response.value.detail))
        api_error = api_request.get_error()
        assert api_error is not None
        assert api_error['message'] == (
            requests_lib._CONTAINER_IMAGE_REQUEST_ERROR_MESSAGE)
        assert secret not in str(api_error['object'])
        assert secret not in getattr(api_error['object'], 'stacktrace', '')


@pytest.mark.parametrize('task_yaml', [
    f'resources:\n  container_image: {_SOURCE}\n',
    f'resources:\n  image_id: docker:{_SOURCE}\n',
    f'resources:\n  cloud: kubernetes\n  image_id: {_SOURCE}\n',
    f'resources:\n  image_id: {_SOURCE}\n',
    f"resources:\n  infra: '*'\n  image_id: {_SOURCE}\n",
    f"resources:\n  infra: '*/us-east-1'\n  image_id: {_SOURCE}\n",
    f'resources:\n  infra: ssh/pool-a\n  image_id: {_SOURCE}\n',
    f'resources:\n  cloud: ssh\n  region: pool-a\n  image_id: {_SOURCE}\n',
])
@pytest.mark.asyncio
async def test_task_container_image_terminal_errors_are_value_free(
        image_request_database, task_yaml):
    del image_request_database
    secret = 'supersecret'
    request_id = 'launch-image-secret'
    request = requests_lib.Request(
        request_id=request_id,
        name=(server_constants.REQUEST_NAME_PREFIX +
              request_names.RequestName.CLUSTER_LAUNCH.value),
        entrypoint=core.status,
        request_body=payloads.LaunchBody(task=task_yaml,
                                         cluster_name='cluster'),
        status=requests_lib.RequestStatus.RUNNING,
        created_at=0,
        user_id='user-1')
    assert await requests_lib.create_if_not_exists_async(request)

    try:
        raise RuntimeError(f'provider token={secret}')
    except RuntimeError as unsafe_error:
        await requests_lib.set_request_failed_async(request_id, unsafe_error)

    stored = requests_lib.get_request(request_id)
    assert stored is not None
    error = stored.get_error()
    assert error is not None
    assert error['message'] == (
        requests_lib._CONTAINER_IMAGE_REQUEST_ERROR_MESSAGE)
    assert secret not in str(error['object'])
    assert secret not in getattr(error['object'], 'stacktrace', '')

    with pytest.raises(fastapi.HTTPException) as response:
        await api_server.api_get(request_id)
    api_request = requests_lib.Request.decode(
        payloads.RequestPayload(**response.value.detail))
    api_error = api_request.get_error()
    assert api_error is not None
    assert api_error['message'] == (
        requests_lib._CONTAINER_IMAGE_REQUEST_ERROR_MESSAGE)
    assert secret not in str(api_error['object'])
    assert secret not in getattr(api_error['object'], 'stacktrace', '')


@pytest.mark.parametrize('malicious', [
    'user:supersecret@registry.example.com/repo',
    'user:supersecret@repo',
    'password=supersecret',
])
def test_image_routes_never_reflect_rejected_reference_input(
        monkeypatch, malicious):
    monkeypatch.setattr(server.workspaces_core, 'resolve_workspace_for_user',
                        lambda *_: types.SimpleNamespace(workspace='research'))
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule):
        responses_by_route = [
            test_client.post('/images/publish', json={'image': malicious}),
            test_client.post('/images/register', json={'image': malicious}),
            test_client.post('/images/prepare',
                             json={
                                 'image': malicious,
                                 'targets': ['canonical'],
                             }),
            test_client.get('/images', params={'image': malicious}),
            test_client.post('/images/retry',
                             json={
                                 'image': malicious,
                                 'target': 'canonical',
                             }),
        ]
    for response in responses_by_route:
        assert response.status_code == 422
        assert response.json()['detail']
        assert malicious not in response.text
        assert 'supersecret' not in response.text
    schedule.assert_not_awaited()

    with pytest.raises(ValueError) as core_error:
        core.status(malicious, workspace='research')
    assert malicious not in str(core_error.value)
    assert 'supersecret' not in str(core_error.value)


def test_uvicorn_access_logs_drop_unvalidated_query_values():
    secret = 'access-log-secret'
    record = logging.LogRecord(
        name='uvicorn.access',
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='%s - "%s %s HTTP/%s" %d',
        args=('127.0.0.1:1234', 'GET',
              f'/images?image=https://user:{secret}@registry.example/repo&'
              f'workspace={secret}', '1.1', 422),
        exc_info=None)
    assert skyuvicorn._ACCESS_LOG_QUERY_FILTER.filter(record)
    rendered = record.getMessage()
    assert rendered == '127.0.0.1:1234 - "GET /images HTTP/1.1" 422'
    assert secret not in rendered


def test_image_routes_never_reflect_denied_workspace(monkeypatch, caplog):
    workspace = 'workspace-secret-120'

    def deny_workspace(*_):
        raise exceptions.PermissionDeniedError(
            f'permission denied for {workspace}')

    resolve = mock.Mock(side_effect=deny_workspace)
    monkeypatch.setattr(server.workspaces_core, 'resolve_workspace_for_user',
                        resolve)
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule), \
         caplog.at_level(logging.DEBUG):
        responses_by_route = [
            test_client.post('/images/publish',
                             json={
                                 'image': _SOURCE,
                                 'workspace': workspace,
                             }),
            test_client.post('/images/register',
                             json={
                                 'image': _SOURCE,
                                 'workspace': workspace,
                             }),
            test_client.post('/images/prepare',
                             json={
                                 'image': _SOURCE,
                                 'targets': ['canonical'],
                                 'workspace': workspace,
                             }),
            test_client.get('/images',
                            params={
                                'image': _SOURCE,
                                'workspace': workspace,
                            }),
            test_client.post('/images/retry',
                             json={
                                 'image': _SOURCE,
                                 'target': 'canonical',
                                 'workspace': workspace,
                             }),
        ]
    for response in responses_by_route:
        assert response.status_code == 403
        assert response.json() == {
            'detail': 'Container image workspace access denied.'
        }
        assert workspace not in response.text
    sky_logs = '\n'.join(record.getMessage()
                         for record in caplog.records
                         if record.name.startswith('sky'))
    assert workspace not in sky_logs
    assert resolve.call_count == 5
    schedule.assert_not_awaited()


def test_image_routes_never_reflect_invalid_workspace(monkeypatch, caplog):
    workspace = 'https://user:workspace-secret-120@workspace.example'
    resolve = mock.Mock()
    monkeypatch.setattr(server.workspaces_core, 'resolve_workspace_for_user',
                        resolve)
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    schedule = mock.AsyncMock()
    with mock.patch.object(executor, 'schedule_request_async', schedule), \
         caplog.at_level(logging.DEBUG):
        responses_by_route = [
            test_client.post('/images/publish',
                             json={
                                 'image': _SOURCE,
                                 'workspace': workspace,
                             }),
            test_client.post('/images/register',
                             json={
                                 'image': _SOURCE,
                                 'workspace': workspace,
                             }),
            test_client.post('/images/prepare',
                             json={
                                 'image': _SOURCE,
                                 'targets': ['canonical'],
                                 'workspace': workspace,
                             }),
            test_client.get('/images',
                            params={
                                'image': _SOURCE,
                                'workspace': workspace,
                            }),
            test_client.post('/images/retry',
                             json={
                                 'image': _SOURCE,
                                 'target': 'canonical',
                                 'workspace': workspace,
                             }),
        ]
    for response in responses_by_route:
        assert response.status_code == 422
        assert response.json()['detail']
        assert workspace not in response.text
        assert 'workspace-secret-120' not in response.text
    sky_logs = '\n'.join(record.getMessage()
                         for record in caplog.records
                         if record.name.startswith('sky'))
    assert 'workspace-secret-120' not in sky_logs
    resolve.assert_not_called()
    schedule.assert_not_awaited()


@pytest.mark.parametrize(
    'malicious,secret',
    [
        ('https://user:supersecret@registry.example.com', 'supersecret'),
        ('\x1b]52;c;YXR0YWNrZXI=\x07release', 'YXR0YWNrZXI='),
    ],
)
def test_image_routes_never_persist_or_reflect_rejected_release(
        monkeypatch, malicious, secret):
    monkeypatch.setattr(server.workspaces_core, 'resolve_workspace_for_user',
                        lambda *_: types.SimpleNamespace(workspace='research'))
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    for route in ('/images/publish', '/images/register'):
        response = test_client.post(route,
                                    json={
                                        'image': {
                                            'ref': _SOURCE,
                                            'release': malicious,
                                        },
                                    })
        assert response.status_code == 422
        assert response.json()['detail']
        assert malicious not in response.text
        assert secret not in response.text


@pytest.mark.parametrize(
    'malicious,secret',
    [
        ('https://user:supersecret@example.com/repo', 'supersecret'),
        ('\x1b]52;c;YXR0YWNrZXI=\x07name', 'YXR0YWNrZXI='),
    ],
)
def test_image_routes_never_persist_or_reflect_rejected_identifiers(
        monkeypatch, malicious, secret):
    monkeypatch.setattr(server.workspaces_core, 'resolve_workspace_for_user',
                        lambda *_: types.SimpleNamespace(workspace='research'))
    test_client = fastapi_testclient.TestClient(api_server.app,
                                                raise_server_exceptions=False)
    responses_by_boundary = [
        test_client.get('/images',
                        params={'image': f'artifact_id={malicious}'}),
        test_client.post('/images/prepare',
                         json={
                             'image': {
                                 'artifact_id': malicious,
                             },
                             'targets': ['canonical'],
                         }),
        test_client.post('/images/prepare',
                         json={
                             'image': {
                                 'release': 'safe-release',
                             },
                             'targets': [malicious],
                         }),
        test_client.post('/images/prepare',
                         json={
                             'image': {
                                 'release': 'safe-release',
                             },
                             'targets': ['canonical'],
                             'distribution': malicious,
                         }),
        test_client.post('/images/retry',
                         json={
                             'image': 'safe-release',
                             'target': malicious,
                         }),
        test_client.post('/images/retry',
                         json={
                             'image': 'safe-release',
                             'target': 'canonical',
                             'distribution': malicious,
                         }),
    ]
    for response in responses_by_boundary:
        assert response.status_code == 422
        assert response.json()['detail']
        assert malicious not in response.text
        assert secret not in response.text


def test_image_payload_normalizes_compatibility_aliases():
    body = payloads.ImagePrepareBody(image={
        'ref': _SOURCE,
        'profile': 'managed',
        'version': 'boltz-2.1.0',
    },
                                     targets=['canonical'])
    assert not isinstance(body.image, str)
    assert body.image.distribution == 'managed'
    assert body.image.release == 'boltz-2.1.0'
    assert body.image.profile is None
    assert body.image.version is None
    assert body.model_dump(exclude_none=True)['image'] == {
        'ref': _SOURCE,
        'release': 'boltz-2.1.0',
        'distribution': 'managed',
    }


@pytest.mark.parametrize('body_factory', [
    lambda image: payloads.ImagePublishBody(image=image),
    lambda image: payloads.ImagePrepareBody(image=image, targets=['canonical']),
])
def test_structured_image_unknown_fields_are_rejected_value_free(body_factory):
    secret = 'structured-image-key-secret'
    with pytest.raises(ValueError) as error:
        body_factory({'ref': _SOURCE, secret: 'ignored'})
    rendered = ''.join(
        traceback.format_exception(type(error.value), error.value,
                                   error.value.__traceback__))
    assert secret not in rendered
    assert secret not in repr(error.value.errors())
    assert 'unsupported fields' in str(error.value)


def test_container_image_launch_requires_api_v62():
    dag = types.SimpleNamespace(tasks=[
        types.SimpleNamespace(resources=[
            types.SimpleNamespace(container_image=mock.sentinel.image)
        ])
    ])
    with mock.patch.object(sky_sdk.versions,
                           'get_remote_api_version',
                           return_value=61), \
         pytest.raises(exceptions.APINotSupportedError,
                       match='requires API server version 62'):
        sky_sdk._check_container_image_api_support(dag)

    with mock.patch.object(sky_sdk.versions,
                           'get_remote_api_version',
                           return_value=62):
        sky_sdk._check_container_image_api_support(dag)

    legacy_resources = __import__('sky').Resources(image_id='docker:ubuntu')
    legacy_dag = types.SimpleNamespace(
        tasks=[types.SimpleNamespace(resources=[legacy_resources])])
    with mock.patch.object(sky_sdk.versions,
                           'get_remote_api_version',
                           return_value=59):
        sky_sdk._check_container_image_api_support(legacy_dag)
    assert legacy_resources.to_yaml_config()['image_id'] == {'docker': 'ubuntu'}
    with pytest.raises(ValueError, match='inline userinfo'):
        payloads.ImagePrepareBody(
            image=f'user:secret@registry.example.com/repo@{_DIGEST}',
            targets=['canonical'],
        )
    with pytest.raises(ValueError, match='query|fragment|percent'):
        payloads.ImagePrepareBody(
            image=f'registry.example/repo?token=secret@{_DIGEST}',
            targets=['canonical'],
        )
    with pytest.raises(ValueError, match='artifact UUID|release label'):
        payloads.ImageStatusBody(image='Release+Prod')
    assert payloads.ImageStatusBody(image=_ARTIFACT_ID).image == _ARTIFACT_ID
    assert payloads.ImageStatusBody(image=_SOURCE).image == _SOURCE
    release_selector = f'release={"a" * 36}'
    assert payloads.ImageStatusBody(
        image=release_selector).image == (release_selector)
    assert payloads.ImageRetryBody(image=release_selector,
                                   target='canonical').image == release_selector


def test_prepare_endpoint_resolves_workspace_before_scheduling():
    request = _request()
    body = payloads.ImagePrepareBody(image=_SOURCE,
                                     targets=['aws-us-west-2'],
                                     workspace='requested')
    schedule = mock.AsyncMock()
    resolution = types.SimpleNamespace(workspace='research')
    with mock.patch.object(server.workspaces_core,
                           'resolve_workspace_for_user',
                           return_value=resolution) as resolve, \
         mock.patch.object(executor, 'schedule_request_async', schedule):
        asyncio.run(server.image_prepare(request, body))

    resolve.assert_called_once_with(request.state.auth_user, 'requested')
    assert body.workspace == 'research'
    kwargs = schedule.call_args.kwargs
    assert kwargs['request_id'] == 'request-id'
    assert kwargs['request_name'] == request_names.RequestName.IMAGE_PREPARE
    assert kwargs['request_body'] is body
    assert kwargs['func'] == core.prepare
    assert kwargs['schedule_type'] == requests_lib.ScheduleType.SHORT
    assert kwargs['auth_user'] is request.state.auth_user


def test_publish_endpoint_resolves_workspace_before_scheduling():
    request = _request()
    body = payloads.ImagePublishBody(image={
        'ref': _SOURCE,
        'release': 'boltz-2.1.0',
    },
                                     workspace='requested')
    schedule = mock.AsyncMock()
    resolution = types.SimpleNamespace(workspace='research')
    with mock.patch.object(server.workspaces_core,
                           'resolve_workspace_for_user',
                           return_value=resolution) as resolve, \
         mock.patch.object(executor, 'schedule_request_async', schedule):
        asyncio.run(server.image_publish(request, body))

    resolve.assert_called_once_with(request.state.auth_user, 'requested')
    assert body.workspace == 'research'
    kwargs = schedule.call_args.kwargs
    assert kwargs['request_name'] == request_names.RequestName.IMAGE_PUBLISH
    assert kwargs['request_body'] is body
    assert kwargs['func'] == core.publish
    assert kwargs['schedule_type'] == requests_lib.ScheduleType.SHORT
    assert kwargs['auth_user'] is request.state.auth_user


def test_register_endpoint_preserves_compat_request_name():
    request = _request()
    body = payloads.ImagePublishBody(image={
        'ref': _SOURCE,
        'release': 'boltz-2.1.0',
    },
                                     workspace='requested')
    schedule = mock.AsyncMock()
    resolution = types.SimpleNamespace(workspace='research')
    with mock.patch.object(server.workspaces_core,
                           'resolve_workspace_for_user',
                           return_value=resolution), \
         mock.patch.object(executor, 'schedule_request_async', schedule):
        asyncio.run(server.image_register(request, body))

    kwargs = schedule.call_args.kwargs
    assert kwargs['request_name'] == request_names.RequestName.IMAGE_REGISTER
    assert kwargs['request_body'] is body
    assert kwargs['func'] == core.publish

    # Simulate a register-only client predating the publish spelling. The new
    # server must return the old dispatcher name so that client still selects
    # its typed terminal decoder rather than the default dict decoder.
    register_name = (server_constants.REQUEST_NAME_PREFIX +
                     request_names.RequestName.IMAGE_REGISTER.value)
    publish_name = (server_constants.REQUEST_NAME_PREFIX +
                    request_names.RequestName.IMAGE_PUBLISH.value)
    old_client_handlers = {
        server_constants.DEFAULT_HANDLER_NAME: decoders.default_decode_handler,
        register_name: decoders.decode_image_record,
    }
    assert publish_name not in old_client_handlers
    decoder = old_client_handlers.get(
        register_name,
        old_client_handlers[server_constants.DEFAULT_HANDLER_NAME])
    assert decoder(encoders.encode_image_record(_record())) == _record()


def test_status_endpoint_scopes_query_to_resolved_workspace():
    request = _request()
    schedule = mock.AsyncMock()
    resolution = types.SimpleNamespace(workspace='research')
    with mock.patch.object(server.workspaces_core,
                           'resolve_workspace_for_user',
                           return_value=resolution), \
         mock.patch.object(executor, 'schedule_request_async', schedule):
        asyncio.run(
            server.image_status(request,
                                image='image-id',
                                workspace='requested'))

    body = schedule.call_args.kwargs['request_body']
    assert isinstance(body, payloads.ImageStatusBody)
    assert body.image == 'image-id'
    assert body.workspace == 'research'
    assert (schedule.call_args.kwargs['request_name'] ==
            request_names.RequestName.IMAGE_STATUS)


def test_image_record_serialization_round_trip():
    record = _record()
    encoded = encoders.encode_image_record(record)
    assert encoded['locations'][0]['distribution'] == 'managed'
    assert 'profile' not in encoded['locations'][0]
    decoded = decoders.decode_image_record(encoded)
    assert decoded == record
    assert decoders.decode_image_status(encoders.encode_image_status(
        [record])) == [record]

    legacy_location = dict(encoded['locations'][0])
    legacy_location['profile'] = legacy_location.pop('distribution')
    legacy_encoded = {**encoded, 'locations': [legacy_location]}
    assert decoders.decode_image_record(legacy_encoded) == record


def test_publish_terminal_result_uses_registered_dispatcher_codecs():
    request_name = (server_constants.REQUEST_NAME_PREFIX +
                    request_names.RequestName.IMAGE_PUBLISH.value)
    encoder = encoders.get_encoder(request_name)
    decoder = decoders.get_decoder(request_name)
    assert encoder is encoders.encode_image_record
    assert decoder is decoders.decode_image_record

    encoded = encoder(_record())
    assert encoded['locations'][0]['distribution'] == 'managed'
    assert 'profile' not in encoded['locations'][0]
    persisted_wire_value = return_value_serializers.get_serializer(
        request_name)(encoded)
    restored = decoder(orjson.loads(persisted_wire_value))
    assert restored == _record()


def test_prepare_sdk_preserves_release_version_in_typed_payload():
    raw_prepare = _unwrap(client.prepare)
    with mock.patch.object(client.server_common,
                           'make_authenticated_request',
                           return_value='response') as request, \
         mock.patch.object(client.server_common,
                           'get_request_id',
                           return_value='request-id'):
        result = raw_prepare({
            'ref': _SOURCE,
            'release': 'boltz-2.1.0',
        }, ['aws-us-west-2'], 'research', 'managed')

    assert result == 'request-id'
    args, kwargs = request.call_args
    assert args == ('POST', '/images/prepare')
    assert kwargs['json']['image'] == {
        'ref': _SOURCE,
        'release': 'boltz-2.1.0',
        'artifact_id': None,
        'distribution': None,
    }
    assert kwargs['json']['targets'] == ['aws-us-west-2']
    assert kwargs['json']['distribution'] == 'managed'
    assert kwargs['json']['workspace'] == 'research'


def test_publish_sdk_uses_registration_transaction():
    raw_publish = _unwrap(client.publish)
    with mock.patch.object(client.server_common,
                           'make_authenticated_request',
                           return_value='response') as request, \
         mock.patch.object(client.server_common,
                           'get_request_id',
                           return_value='request-id'):
        result = raw_publish(
            {
                'ref': _SOURCE,
                'release': 'boltz-2.1.0',
                'distribution': 'managed',
            }, 'research')

    assert result == 'request-id'
    assert request.call_args.args == ('POST', '/images/publish')
    request_json = request.call_args.kwargs['json']
    assert request_json['image'] == {
        'ref': _SOURCE,
        'release': 'boltz-2.1.0',
        'artifact_id': None,
        'distribution': 'managed',
    }
    assert request_json['workspace'] == 'research'


@pytest.mark.parametrize(
    'image,workspace',
    [
        ('user:status-query-secret@registry.example.com/repo', 'research'),
        ('safe-release', 'workspace/status-query-secret'),
    ],
)
def test_status_sdk_rejects_sensitive_query_values_before_http(
        image, workspace):
    raw_status = _unwrap(client.status)
    with mock.patch.object(client.server_common,
                           'make_authenticated_request') as request:
        with pytest.raises(ValueError) as error:
            raw_status(image, workspace)
    request.assert_not_called()
    assert 'status-query-secret' not in str(error.value)
    assert 'status-query-secret' not in traceback.format_exc()


def test_publish_cli_separates_logical_publish_from_regional_prepare():
    with mock.patch.object(cli_command.container_images_sdk,
                           'publish',
                           return_value='request-id') as publish, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=_record()), \
         mock.patch.object(cli_command.table_utils,
                           'format_container_image_table',
                           return_value='table'):
        result = CliRunner().invoke(cli_command.image_publish, [
            _SOURCE,
            '--release',
            'boltz-2.1.0',
            '--distribution',
            'managed',
        ])

    assert result.exit_code == 0, result.output
    publish.assert_called_once_with(
        {
            'ref': _SOURCE,
            'release': 'boltz-2.1.0',
            'distribution': 'managed',
        },
        workspace=None)


def test_image_cli_passes_workspace_through_all_sdk_calls():
    workspace = 'requested'
    with mock.patch.object(cli_command.skypilot_config,
                           'apply_cli_config') as apply_config, \
         mock.patch.object(cli_command.container_images_sdk,
                           'publish',
                           return_value='publish-request') as publish, \
         mock.patch.object(cli_command.container_images_sdk,
                           'status',
                           return_value='status-request') as status, \
         mock.patch.object(cli_command.container_images_sdk,
                           'prepare',
                           return_value='prepare-request') as prepare, \
         mock.patch.object(cli_command.container_images_sdk,
                           'retry',
                           return_value='retry-request') as retry, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=_record()), \
         mock.patch.object(cli_command.table_utils,
                           'format_container_image_table',
                           return_value='table'):
        results = (
            CliRunner().invoke(cli_command.image_publish,
                               [_SOURCE, '--workspace', workspace]),
            CliRunner().invoke(cli_command.image_status,
                               [_SOURCE, '--workspace', workspace]),
            CliRunner().invoke(
                cli_command.image_prepare,
                [_SOURCE, '--targets', 'canonical', '--workspace', workspace]),
            CliRunner().invoke(
                cli_command.image_retry,
                [_SOURCE, '--target', 'canonical', '--workspace', workspace]),
        )

    assert all(result.exit_code == 0 for result in results), [
        result.output for result in results
    ]
    publish.assert_called_once_with(_SOURCE, workspace=workspace)
    status.assert_called_once_with(_SOURCE, workspace=workspace)
    prepare.assert_called_once_with(_SOURCE, ['canonical'],
                                    workspace=workspace,
                                    distribution=None)
    retry.assert_called_once_with(_SOURCE,
                                  'canonical',
                                  workspace=workspace,
                                  distribution=None)
    assert apply_config.call_args_list == [
        mock.call([f'active_workspace={workspace}'])
    ] * 4


def test_displayed_artifact_id_round_trips_through_cli_operations():
    artifact_id = '019f5a80-8bc9-7cf2-9fa8-0123456789ab'
    record = _record().model_copy(update={'id': artifact_id})
    table = cli_command.table_utils.format_container_image_table([record])
    assert artifact_id in table
    selector = f'artifact_id={artifact_id}'

    with mock.patch.object(cli_command.container_images_sdk,
                           'status',
                           return_value='status-request') as status, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=[record]):
        result = CliRunner().invoke(cli_command.image_status, [selector])
    assert result.exit_code == 0, result.output
    status.assert_called_once_with(selector, workspace=None)

    with mock.patch.object(cli_command.container_images_sdk,
                           'prepare',
                           return_value='prepare-request') as prepare, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=record):
        result = CliRunner().invoke(cli_command.image_prepare,
                                    [selector, '--targets', 'canonical'])
    assert result.exit_code == 0, result.output
    prepare.assert_called_once_with(selector, ['canonical'],
                                    workspace=None,
                                    distribution=None)

    with mock.patch.object(cli_command.container_images_sdk,
                           'retry',
                           return_value='retry-request') as retry, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=record):
        result = CliRunner().invoke(cli_command.image_retry,
                                    [selector, '--target', 'canonical'])
    assert result.exit_code == 0, result.output
    retry.assert_called_once_with(selector,
                                  'canonical',
                                  workspace=None,
                                  distribution=None)


def test_publish_cli_rejects_mutable_source_before_request():
    with mock.patch.object(cli_command.container_images_sdk,
                           'publish') as publish:
        result = CliRunner().invoke(cli_command.image_publish, [
            'ghcr.io/boltz-bio/boltz:latest',
            '--release',
            'boltz-production',
        ])

    assert result.exit_code != 0
    publish.assert_not_called()


def test_modern_reference_identity_survives_every_public_boundary():
    reference = f'docker:repo@{_DIGEST}'
    resource = resources_lib.Resources(container_image=reference)
    assert resource.container_image.ref == reference
    assert resource.to_yaml_config()['container_image'] == reference
    restored = list(
        resources_lib.Resources.from_yaml_config(resource.to_yaml_config()))[0]
    assert restored.container_image.ref == reference
    assert pickle.loads(pickle.dumps(resource)).container_image.ref == reference
    assert payloads.ImagePublishBody(image=reference).image == reference
    assert payloads.ImagePrepareBody(image=reference,
                                     targets=['canonical']).image == reference
    assert models.validate_operational_image_selector(reference) == reference
    assert (models.parse_explicit_image_selector(f'ref={reference}').ref ==
            reference)

    with mock.patch.object(cli_command.container_images_sdk,
                           'publish',
                           return_value='request-id') as publish, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=_record()), \
         mock.patch.object(cli_command.table_utils,
                           'format_container_image_table',
                           return_value='table'):
        result = CliRunner().invoke(cli_command.image_publish, [reference])
    assert result.exit_code == 0, result.output
    publish.assert_called_once_with(reference, workspace=None)


def test_prepare_cli_exposes_release_version():
    with mock.patch.object(cli_command.container_images_sdk,
                           'prepare',
                           return_value='request-id') as prepare, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=_record()), \
         mock.patch.object(cli_command.table_utils,
                           'format_container_image_table',
                           return_value='table'):
        result = CliRunner().invoke(cli_command.image_prepare, [
            _SOURCE,
            '--targets',
            'canonical',
            '--distribution',
            'managed',
            '--release',
            'boltz-2.1.0',
        ])
    assert result.exit_code == 0, result.output
    prepare.assert_called_once_with({
        'ref': _SOURCE,
        'release': 'boltz-2.1.0',
    }, ['canonical'],
                                    workspace=None,
                                    distribution='managed')


def test_prepare_cli_preserves_bare_selector_with_distribution():
    with mock.patch.object(cli_command.container_images_sdk,
                           'prepare',
                           return_value='request-id') as prepare, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=_record()), \
         mock.patch.object(cli_command.table_utils,
                           'format_container_image_table',
                           return_value='table'):
        result = CliRunner().invoke(cli_command.image_prepare, [
            'boltz-2.1.0',
            '--targets',
            'canonical',
            '--distribution',
            'managed',
        ])
    assert result.exit_code == 0, result.output
    prepare.assert_called_once_with('boltz-2.1.0', ['canonical'],
                                    workspace=None,
                                    distribution='managed')


@pytest.mark.parametrize('selector',
                         ['release=old', f'artifact_id={_ARTIFACT_ID}'])
def test_prepare_cli_release_binding_rejects_non_source_selector(selector):
    with mock.patch.object(cli_command.container_images_sdk,
                           'prepare') as prepare:
        result = CliRunner().invoke(cli_command.image_prepare, [
            selector,
            '--targets',
            'canonical',
            '--release',
            'new',
        ])
    assert result.exit_code == 2
    assert 'can bind only a source reference' in result.output
    prepare.assert_not_called()


def test_retry_sdk_cli_and_endpoint_preserve_distribution():
    raw_retry = _unwrap(client.retry)
    with mock.patch.object(client.server_common,
                           'make_authenticated_request',
                           return_value='response') as request, \
         mock.patch.object(client.server_common,
                           'get_request_id',
                           return_value='request-id'):
        assert raw_retry('image-id', 'canonical', 'research',
                         'global-gpu') == 'request-id'
    assert request.call_args.args == ('POST', '/images/retry')
    request_json = request.call_args.kwargs['json']
    assert {
        key: request_json[key]
        for key in ('image', 'target', 'distribution', 'workspace')
    } == {
        'image': 'image-id',
        'target': 'canonical',
        'distribution': 'global-gpu',
        'workspace': 'research',
    }

    request_obj = _request()
    body = payloads.ImageRetryBody(image='image-id',
                                   target='canonical',
                                   distribution='global-gpu',
                                   workspace='requested')
    schedule = mock.AsyncMock()
    with mock.patch.object(
            server.workspaces_core,
            'resolve_workspace_for_user',
            return_value=types.SimpleNamespace(workspace='research')), \
         mock.patch.object(executor, 'schedule_request_async', schedule):
        asyncio.run(server.image_retry(request_obj, body))
    assert body.workspace == 'research'
    assert body.distribution == 'global-gpu'
    assert schedule.call_args.kwargs['func'] == core.retry

    with mock.patch.object(cli_command.container_images_sdk,
                           'retry',
                           return_value='request-id') as retry, \
         mock.patch.object(cli_command.sdk,
                           'stream_and_get',
                           return_value=_record()), \
         mock.patch.object(cli_command.table_utils,
                           'format_container_image_table',
                           return_value='table'):
        result = CliRunner().invoke(cli_command.image_retry, [
            'image-id', '--target', 'canonical', '--distribution', 'global-gpu'
        ])
    assert result.exit_code == 0, result.output
    retry.assert_called_once_with('image-id',
                                  'canonical',
                                  workspace=None,
                                  distribution='global-gpu')
