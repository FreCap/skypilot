"""Pure contracts for the no-enqueue launch identity boundary."""

import dataclasses
import hashlib
import uuid

import pytest
import serve_resource_action_test_fixtures as authority_fixtures

from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions

_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_CAPABILITY = '12' * 32


class _StringSubclass(str):
    pass


class _IntegerSubclass(int):
    pass


def _resource_identity() -> dict[str, object]:
    return {
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': 3,
    }


def _input() -> dict[str, object]:
    return {
        'version': 1,
        'contract': 'api_server_effective_launch_identity_v1',
        'service_name': 'svc',
        'resource_identity': _resource_identity(),
        'prepared_original_user': 'prepared@example.com',
        'prepared_user_hash': 'prepared-hash',
    }


def _context() -> dict[str, object]:
    typed_input = (
        actions.ProviderLaunchIdentityCanonicalizationInputV1.from_value(
            _input()))
    decision_id = typed_input.resource_identity.action_identity(
        kernel_actions.ActionKind.LAUNCH).action_id
    return {
        'version': 1,
        'decision_id': str(decision_id),
        'cohort_id': authority_fixtures.COHORT_ID,
        'action_type': 'launch',
        'controller_owner_fence': '123:10.0.0.1',
        'lifecycle_epoch': 4,
        'preparation_reference_revision': 1,
        'reference_state': 'PREPARING',
        'preparation_capability_sha256': hashlib.sha256(
            bytes.fromhex(_CAPABILITY)).hexdigest(),
        'input': typed_input.canonical_value(),
        'input_sha256': typed_input.sha256,
    }


def _request() -> dict[str, object]:
    context = actions.ProviderLaunchIdentityCanonicalizationContextV1.from_value(
        _context())
    return {
        'version': 1,
        'context': context.canonical_value(),
        'context_sha256': context.sha256,
        'preparation_capability': _CAPABILITY,
    }


def _proof() -> dict[str, object]:
    context = actions.ProviderLaunchIdentityCanonicalizationContextV1.from_value(
        _context())
    return {
        'version': 1,
        'boundary': 'api_server_post_auth_no_enqueue',
        'context': context.canonical_value(),
        'context_sha256': context.sha256,
        'effective_original_user': 'effective@example.com',
        'effective_user_hash': 'User-Hash',
    }


def _response() -> dict[str, object]:
    proof = actions.ProviderLaunchIdentityCanonicalizationProofV1.from_value(
        _proof())
    return {
        'version': 1,
        'decision_id': str(proof.context.decision_id),
        'context_sha256': proof.context_sha256,
        'proof': proof.canonical_value(),
        'proof_sha256': proof.sha256,
    }


@pytest.mark.parametrize('contract_type,value', [
    (actions.ProviderLaunchIdentityCanonicalizationInputV1, _input),
    (actions.ProviderLaunchIdentityCanonicalizationContextV1, _context),
    (actions.ProviderLaunchIdentityCanonicalizationRequestV1, _request),
    (actions.ProviderLaunchIdentityCanonicalizationProofV1, _proof),
    (actions.ProviderLaunchIdentityCanonicalizationResponseV1, _response),
])
def test_launch_identity_contracts_round_trip_exactly(contract_type,
                                                      value) -> None:
    raw = value()
    parsed = contract_type.from_value(raw)
    assert parsed.canonical_value() == raw
    assert contract_type.from_value(parsed.canonical_value()) == parsed
    assert parsed.sha256 == hashlib.sha256(parsed.canonical_bytes).hexdigest()


@pytest.mark.parametrize('contract_type,value,byte_size,sha256', [
    (actions.ProviderLaunchIdentityCanonicalizationInputV1, _input, 408,
     'd25567c62210d31dc19f99ba67926d5472ff39a5dba85c0a51682b666908f8f3'),
    (actions.ProviderLaunchIdentityCanonicalizationContextV1, _context, 938,
     '46737a3ae039bfdb069d06d6d76ae0f17576382ec1e8c699ecc032061c0b8a34'),
    (actions.ProviderLaunchIdentityCanonicalizationRequestV1, _request, 1138,
     'd13c4e8d88cd4c32187d6859f267c5321ddf6f68bb1cfdad2ee7ef75259b5ecd'),
    (actions.ProviderLaunchIdentityCanonicalizationProofV1, _proof, 1175,
     '212c1a55e590d9e7887a76206dbf1b32511eabb63be421372af9fec350c49b28'),
    (actions.ProviderLaunchIdentityCanonicalizationResponseV1, _response, 1416,
     '0edd61893f40e526c7a73fd0328e29018cb20838dc79051c77b5520d77ecc73c'),
])
def test_launch_identity_contracts_have_fixed_canonical_fixtures(
        contract_type, value, byte_size: int, sha256: str) -> None:
    parsed = contract_type.from_value(value())
    assert len(parsed.canonical_bytes) == byte_size
    assert parsed.sha256 == sha256


@pytest.mark.parametrize('contract_type,value', [
    (actions.ProviderLaunchIdentityCanonicalizationInputV1, _input),
    (actions.ProviderLaunchIdentityCanonicalizationContextV1, _context),
    (actions.ProviderLaunchIdentityCanonicalizationRequestV1, _request),
    (actions.ProviderLaunchIdentityCanonicalizationProofV1, _proof),
    (actions.ProviderLaunchIdentityCanonicalizationResponseV1, _response),
])
def test_launch_identity_contracts_are_closed(contract_type, value) -> None:
    raw = value()
    raw['extra'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        contract_type.from_value(raw)
    raw = value()
    raw.pop(next(iter(raw)))
    with pytest.raises(ValueError, match='unknown or missing'):
        contract_type.from_value(raw)


def test_launch_identity_hash_chain_and_decision_are_recomputed() -> None:
    context = _context()
    context['input_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='input hash does not match'):
        actions.ProviderLaunchIdentityCanonicalizationContextV1.from_value(
            context)

    context = _context()
    context['decision_id'] = str(uuid.uuid4())
    with pytest.raises(ValueError, match='decision ID does not match'):
        actions.ProviderLaunchIdentityCanonicalizationContextV1.from_value(
            context)

    request = _request()
    request['context_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='context hash does not match'):
        actions.ProviderLaunchIdentityCanonicalizationRequestV1.from_value(
            request)

    proof = _proof()
    proof['context_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='context hash does not match'):
        actions.ProviderLaunchIdentityCanonicalizationProofV1.from_value(proof)

    response = _response()
    response['proof_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='proof hash does not match'):
        actions.ProviderLaunchIdentityCanonicalizationResponseV1.from_value(
            response)


@pytest.mark.parametrize('field,value,match', [
    ('action_type', 'down', 'requires launch'),
    ('reference_state', 'SHADOW_ACTIVE', 'PREPARING'),
    ('preparation_reference_revision', 2, 'integer 1'),
])
def test_launch_identity_context_freezes_preparing_launch_literal(
        field: str, value: object, match: str) -> None:
    context = _context()
    context[field] = value
    with pytest.raises(ValueError, match=match):
        actions.ProviderLaunchIdentityCanonicalizationContextV1.from_value(
            context)


def test_launch_identity_request_separates_syntax_from_secret_validation(
) -> None:
    request = _request()
    request['preparation_capability'] = '34' * 32
    parsed = actions.ProviderLaunchIdentityCanonicalizationRequestV1.from_value(
        request)
    assert parsed.preparation_capability == '34' * 32

    for invalid in ('A' * 64, 'a' * 63, 'g' * 64, None):
        request = _request()
        request['preparation_capability'] = invalid
        with pytest.raises(ValueError, match='64 lowercase'):
            actions.ProviderLaunchIdentityCanonicalizationRequestV1.from_value(
                request)


@pytest.mark.parametrize('mutate', [
    lambda value: value.update({'service_name': _StringSubclass('svc')}),
    lambda value: value.update(
        {'prepared_original_user': _StringSubclass('user')}),
    lambda value: value['resource_identity'].update(
        {'replica_id': _IntegerSubclass(7)}),
    lambda value: value['resource_identity'].update(
        {'service_hash': _StringSubclass(_SERVICE_UUID)}),
])
def test_launch_identity_input_rejects_scalar_subclasses(mutate) -> None:
    value = _input()
    mutate(value)
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderLaunchIdentityCanonicalizationInputV1.from_value(value)


def test_launch_identity_input_service_name_has_exact_bound() -> None:
    value = _input()
    value['service_name'] = 's' * 256
    assert actions.ProviderLaunchIdentityCanonicalizationInputV1.from_value(
        value).service_name == 's' * 256
    value['service_name'] = 's' * 257
    with pytest.raises(ValueError, match='1..256'):
        actions.ProviderLaunchIdentityCanonicalizationInputV1.from_value(value)


@pytest.mark.parametrize('original_user,user_hash,match', [
    ('é', 'user-hash', 'ASCII'),
    ('user', '-leading', 'invalid'),
    ('user', 'hash_' + 'x', 'invalid'),
    ('user', 'valid-hash\n', 'invalid'),
    ('user', 'x' * 32, '1..31'),
])
def test_launch_identity_proof_bounds_effective_pair(original_user: str,
                                                     user_hash: str,
                                                     match: str) -> None:
    proof = _proof()
    proof['effective_original_user'] = original_user
    proof['effective_user_hash'] = user_hash
    with pytest.raises(ValueError, match=match):
        actions.ProviderLaunchIdentityCanonicalizationProofV1.from_value(proof)


def test_launch_identity_proof_never_retains_raw_capability() -> None:
    request = actions.ProviderLaunchIdentityCanonicalizationRequestV1.from_value(
        _request())
    proof = actions.ProviderLaunchIdentityCanonicalizationProofV1.from_value(
        _proof())
    response = (
        actions.ProviderLaunchIdentityCanonicalizationResponseV1.from_value(
            _response()))
    assert _CAPABILITY.encode() in request.canonical_bytes
    assert _CAPABILITY.encode() not in proof.canonical_bytes
    assert _CAPABILITY.encode() not in response.canonical_bytes


def test_launch_identity_direct_typed_children_require_exact_types() -> None:
    context = actions.ProviderLaunchIdentityCanonicalizationContextV1.from_value(
        _context())

    class _ContextSubclass(
            actions.ProviderLaunchIdentityCanonicalizationContextV1):
        pass

    context_subclass = _ContextSubclass(
        **{
            field.name: getattr(context, field.name)
            for field in dataclasses.fields(context)
        })
    with pytest.raises(TypeError, match='context has an invalid type'):
        actions.ProviderLaunchIdentityCanonicalizationRequestV1(
            version=1,
            context=context_subclass,
            context_sha256=context.sha256,
            preparation_capability=_CAPABILITY)
