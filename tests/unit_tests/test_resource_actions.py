"""Pure contract tests for durable resource actions."""

import dataclasses
import uuid

import pytest

from sky import core
from sky.server.requests import payloads
from sky.server.requests import requests
from sky.server.requests import resource_actions as actions


def _identity() -> actions.ResourceActionIdentity:
    return actions.ResourceActionIdentity(
        service_hash='svc',
        service_incarnation=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        replica_id=0,
        replica_incarnation=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        desired_generation=1,
        action_kind=actions.ActionKind.LAUNCH,
    )


def _request(action_id: uuid.UUID, attempt: int = 1) -> requests.Request:
    body = payloads.StopOrDownBody(
        cluster_name='replica-0',
        env_vars={},
        entrypoint='',
        entrypoint_command='',
        using_remote_api_server=False,
        override_skypilot_config={},
        override_skypilot_config_path=None,
        file_mounts_blob_id=None,
        client_api_version=None,
    )
    return requests.Request(
        request_id=actions.request_id_for_attempt(action_id, attempt),
        name='sky.down',
        entrypoint=core.down,
        request_body=body,
        status=requests.RequestStatus.PENDING,
        created_at=0,
        user_id='u',
        schedule_type=requests.ScheduleType.SHORT,
        cluster_name='replica-0',
        should_enqueue=True,
        producer_version='1.2.3',
    )


def test_identity_v1_golden_bytes_hash_and_uuids() -> None:
    identity = actions.ResourceActionIdentity(
        service_hash='cafe\u0301',
        service_incarnation=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        replica_id=7,
        replica_incarnation=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        desired_generation=3,
        action_kind='launch',
    )
    assert actions.RESOURCE_ACTION_NAMESPACE == uuid.UUID(
        'ffa24895-49b7-5f76-9a32-ff22809e4dff')
    assert actions.canonical_json_bytes(identity.canonical_value()) == (
        b'{"action_kind":"launch","desired_generation":3,'
        b'"domain":"serve","replica_id":7,"replica_incarnation":'
        b'"22222222-2222-4222-8222-222222222222","resource_type":'
        b'"replica","service_hash":"caf\xc3\xa9","service_incarnation":'
        b'"11111111-1111-4111-8111-111111111111","version":1}')
    assert identity.resource_identity == (
        '{"replica_id":7,"replica_incarnation":'
        '"22222222-2222-4222-8222-222222222222","service_hash":"caf\u00e9",'
        '"service_incarnation":"11111111-1111-4111-8111-111111111111",'
        '"version":1}')
    assert identity.action_id == uuid.UUID(
        '48176b2d-1f16-59d0-93cf-d44301ecfa9c')
    assert actions.canonical_sha256(identity.canonical_value()) == (
        '26a1f058a13ce1684f5c966868a61f5a4cd529f6b99b231a194640a2371ba799')
    assert actions.request_id_for_attempt(
        identity.action_id, 1) == ('612a7335-1e4d-508f-8bfe-e252a295b411')


def test_canonical_json_normalizes_all_text_and_rejects_floats() -> None:
    assert actions.canonical_json_bytes({'e\u0301': ['e\u0301']
                                        }) == (b'{"\xc3\xa9":["\xc3\xa9"]}')
    with pytest.raises(ValueError, match='duplicate'):
        actions.canonical_json_bytes({'\u00e9': 1, 'e\u0301': 2})
    with pytest.raises(TypeError, match='forbids floats'):
        actions.canonical_json_bytes({'delay': 1.0})


@pytest.mark.parametrize('field,value', [('replica_id', True),
                                         ('replica_id', '0'),
                                         ('desired_generation', False),
                                         ('desired_generation', '1')])
def test_identity_rejects_non_integer_numeric_fields(field, value) -> None:
    kwargs = dataclasses.asdict(_identity())
    kwargs[field] = value
    with pytest.raises(ValueError, match='integer'):
        actions.ResourceActionIdentity(**kwargs)


def test_request_input_v1_golden_bytes_and_hash() -> None:
    identity = _identity()
    request_input = actions.ActionRequestInput.from_request(
        identity.action_id, 1, _request(identity.action_id))
    assert identity.action_id == uuid.UUID(
        'd646c88b-3eb1-5384-9324-c2b357fba89f')
    assert request_input.request_id == ('c065df88-c4be-58bf-8913-dd2df865d4d2')
    assert actions.canonical_json_bytes(request_input.value) == (
        b'{"action_id":"d646c88b-3eb1-5384-9324-c2b357fba89f",'
        b'"attempt":1,"cluster_name":"replica-0","execution_class":"normal",'
        b'"file_mounts_blob_id":null,"handler_name":"sky.core:'
        b'down","ignore_return_value":false,"initial_status":"PENDING",'
        b'"name":"sky.down","payload_format":"pydantic-json",'
        b'"payload_json":{"client_api_version":null,"cluster_name":'
        b'"replica-0","entrypoint":"","entrypoint_command":"",'
        b'"env_vars":{},"file_mounts_blob_id":null,"graceful":false,'
        b'"graceful_timeout":null,'
        b'"override_skypilot_config":{},"override_skypilot_config_path":'
        b'null,"purge":false,"using_remote_api_server":false},'
        b'"payload_type":"sky.server.requests.payloads:StopOrDownBody",'
        b'"payload_version":1,"precondition_deadline":null,'
        b'"precondition_payload":null,"precondition_type":null,'
        b'"producer_version":"1.2.3","queue_priority":0,"request_id":'
        b'"c065df88-c4be-58bf-8913-dd2df865d4d2","retryable":false,'
        b'"schedule_type":"short","should_enqueue":true,"user_id":"u",'
        b'"version":1}')
    assert request_input.sha256 == (
        'eae0d35a2fed61c9be737b7a9ddc5c8be4c911555b2024fb84102ad4a9b516fa')
    request_input.validate()


def test_request_input_requires_pristine_normal_request() -> None:
    identity = _identity()
    request = _request(identity.action_id)
    request.retryable = True
    with pytest.raises(ValueError, match='non-retryable'):
        actions.ActionRequestInput.from_request(identity.action_id, 1, request)
    request = _request(identity.action_id)
    request.execution_generation = 1
    with pytest.raises(ValueError, match='retry/claim'):
        actions.ActionRequestInput.from_request(identity.action_id, 1, request)
    request = _request(identity.action_id)
    request.name = 'sky.enabled_clouds'
    request.entrypoint = core.enabled_clouds
    request.request_body = payloads.EnabledCloudsBody(
        workspace=None,
        expand=False,
        env_vars={},
        entrypoint='',
        entrypoint_command='',
        using_remote_api_server=False,
        override_skypilot_config={},
        override_skypilot_config_path=None,
        file_mounts_blob_id=None,
        client_api_version=None,
    )
    with pytest.raises(ValueError, match='ReplayPolicy.NEVER'):
        actions.ActionRequestInput.from_request(identity.action_id, 1, request)


def test_request_input_validate_rejects_noncanonical_or_forged_value() -> None:
    identity = _identity()
    request_input = actions.ActionRequestInput.from_request(
        identity.action_id, 1, _request(identity.action_id))
    noncanonical = dict(request_input.value)
    noncanonical['user_id'] = 'e\u0301'
    forged = dataclasses.replace(request_input,
                                 value=noncanonical,
                                 sha256=actions.canonical_sha256(noncanonical))
    with pytest.raises(ValueError, match='not canonical'):
        forged.validate()
    with pytest.raises(ValueError, match='SHA-256'):
        dataclasses.replace(request_input, sha256='0' * 64).validate()


def test_request_input_validate_rejects_closed_preimage_mutations() -> None:
    identity = _identity()
    request_input = actions.ActionRequestInput.from_request(
        identity.action_id, 1, _request(identity.action_id))
    mutations = {
        'missing key': lambda value: value.pop('name'),
        'extra key': lambda value: value.update({'extra': None}),
        'version': lambda value: value.update({'version': 2}),
        'action identity': lambda value: value.update({
            'action_id': str(uuid.UUID('33333333-3333-4333-8333-333333333333'))
        }),
        'attempt identity': lambda value: value.update({'attempt': 2}),
        'request identity': lambda value: value.update({
            'request_id': str(uuid.UUID('44444444-4444-4444-8444-444444444444'))
        }),
        'executor': lambda value: value.update(
            {'execution_class': 'controller'}),
        'replay policy': lambda value: value.update(
            {'handler_name': 'sky.core:enabled_clouds'}),
        'retryability': lambda value: value.update({'retryable': True}),
        'precondition': lambda value: value.update(
            {'precondition_type': 'test'}),
        'initial status': lambda value: value.update(
            {'initial_status': 'RUNNING'}),
        'queue intent': lambda value: value.update({'should_enqueue': False}),
        'queue priority': lambda value: value.update({'queue_priority': 1}),
        'payload shape': lambda value: value.update({'payload_json': []}),
    }
    for name, mutate in mutations.items():
        value = dict(request_input.value)
        mutate(value)
        forged = dataclasses.replace(request_input,
                                     value=value,
                                     sha256=actions.canonical_sha256(value))
        try:
            forged.validate()
        except ValueError:
            continue
        pytest.fail(f'closed request input accepted mutation: {name}')


def test_action_reduction_shape_is_closed() -> None:
    retry = actions.ActionReduction(actions.KernelState.READY, {
        'version': 1
    }, {
        'version': 1
    },
                                    retry_after_seconds=7).normalized()
    assert retry.retry_after_seconds == 7
    terminal = actions.ActionReduction(
        actions.KernelState.TERMINAL, {
            'version': 1
        }, {
            'version': 1
        },
        terminal_disposition='succeeded').normalized()
    assert terminal.terminal_disposition == 'succeeded'
    with pytest.raises(ValueError, match='retry delay'):
        actions.ActionReduction(actions.KernelState.READY, {}, {}).normalized()
    with pytest.raises(ValueError, match='requires a disposition'):
        actions.ActionReduction(actions.KernelState.TERMINAL, {},
                                {}).normalized()
