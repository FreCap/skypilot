"""Pure contract tests for durable SkyServe resource actions."""
# pylint: disable=protected-access

import copy
import dataclasses
import uuid

import pytest
import test_serve_resource_action_down_execution_config
import test_serve_resource_action_launch_execution_config

from sky.serve import resource_action_state
from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions

down_config_fixtures = test_serve_resource_action_down_execution_config
launch_config_fixtures = test_serve_resource_action_launch_execution_config
_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_CLUSTER_UUID = '33333333-3333-4333-8333-333333333333'


class _EqualitySpoofingString(str):
    """Text whose Python equality lies about its canonical value."""

    def __eq__(self, other: object) -> bool:
        del other
        return True

    def __ne__(self, other: object) -> bool:
        del other
        return False

    __hash__ = str.__hash__


class _HashSpoofingString(str):
    """Text whose hash differs from its canonical string hash."""

    def __hash__(self) -> int:
        return super().__hash__() ^ 1


class _LengthSpoofingString(str):
    """Text whose direct length understates its canonical content."""

    def __len__(self) -> int:
        return 1


class _LengthSpoofingBytes(bytes):
    """Encoded bytes whose length understates their canonical content."""

    def __len__(self) -> int:
        return 1


class _BoundSpoofingString(str):
    """Text whose encoded bytes try to evade byte-size bounds."""

    def encode(self, encoding: str = 'utf-8', errors: str = 'strict') -> bytes:
        return _LengthSpoofingBytes(super().encode(encoding, errors))


_SPOOFING_STRING_TYPES = (_EqualitySpoofingString, _HashSpoofingString,
                          _LengthSpoofingString, _BoundSpoofingString)


def _identity(generation: int = 1) -> dict:
    return {
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': generation,
    }


def _target() -> dict:
    return launch_config_fixtures._target()


def _resources() -> dict:
    return launch_config_fixtures._resource_snapshot()


def _launch_invocation(generation: int = 1,
                       *,
                       workspace: str = 'boltz-test') -> dict:
    identity = _identity(generation)
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'redaction_profile': 'provider_lifecycle_redaction_v1',
        'action_kind': 'launch',
        'resource_identity': identity,
        'requested_target': _target(),
        'launch': launch_config_fixtures.launch_payload(identity,
                                                        _target(),
                                                        _resources(),
                                                        workspace=workspace),
        'down': None,
    }


def _down_invocation() -> dict:
    return copy.deepcopy(
        down_config_fixtures.down_invocation_payload(generation=2))


def _launch_plan() -> dict:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.launch is not None
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'action_kind': 'launch',
        'resource_identity': _identity(),
        'placement_decision_sha256': 'e' * 64,
        'resources_snapshot_sha256': invocation.launch.resources.sha256,
        'workspace_identity_sha256': 'f' * 64,
        'requested_target': _target(),
        'prior_launch_basis_sha256': None,
        'prior_cleanup_target_sha256': None,
        'request_payload_sha256': invocation.sha256,
        'redaction_profile': 'provider_lifecycle_redaction_v1',
    }


def _resolved_target() -> dict:
    raw = down_config_fixtures._progress_resolved_target()
    raw['resolved_at'] = '2026-08-01T01:02:03.000004Z'
    return raw


def _down_plan() -> dict:
    return copy.deepcopy(down_config_fixtures.down_plan_payload(generation=2))


def _launch_spec() -> dict:
    return {
        'version': 1,
        'provider_plan': _launch_plan(),
        'invocation': _launch_invocation(),
    }


def _down_spec() -> dict:
    return {
        'version': 1,
        'provider_plan': _down_plan(),
        'invocation': _down_invocation(),
    }


def _shadow_projection() -> dict:
    return {
        'version': 1,
        'action_kind': 'launch',
        'row_disposition': 'retained',
        'replica_status': 'READY',
        'capacity_outcome': 'success',
        'action_disposition': 'succeeded',
        'resolved_target': _resolved_target(),
    }


def _observation() -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    resolved = _resolved_target()
    return {
        'version': 1,
        'target_sha256': target.sha256,
        'state': 'present',
        'certainty': 'authoritative',
        'observed_provider_operation_id': None,
        'observed_provider_resource_id': resolved['provider_resource_id'],
        'observed_cluster_record_uuid': _CLUSTER_UUID,
        'observed_workload_uid': resolved['workload_uid'],
        'observed_replica_incarnation_label': _REPLICA_UUID,
        'resolved_target': resolved,
        'ready': True,
        'evidence_sha256': '4' * 64,
        'observed_at': '2026-08-01T01:02:04.000005Z',
    }


def _absent_observation() -> dict:
    observation = _observation()
    observation.update({
        'state': 'absent',
        'certainty': 'authoritative',
        'observed_provider_operation_id': None,
        'observed_provider_resource_id': None,
        'observed_cluster_record_uuid': None,
        'observed_workload_uid': None,
        'observed_replica_incarnation_label': None,
        'resolved_target': None,
        'ready': None,
    })
    return observation


def _outcome(*,
             disposition: str = 'succeeded',
             certainty: str = 'observed',
             observation: dict | None = None) -> dict:
    retryable = disposition in ('retryable', 'uncertain')
    return {
        'disposition': disposition,
        'certainty': certainty,
        'provider_operation_id': None,
        'provider_code': None,
        'retry_class':
            ('observation_required' if disposition == 'uncertain' else
             ('transient' if retryable else None)),
        'retry_after_seconds': (1 if retryable else None),
        'observation': observation,
        'normalized_message': None,
    }


def test_launch_invocation_literal_golden_bytes_hash_and_action_id() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.canonical_bytes == actions.canonical_json_bytes(
        invocation.canonical_value())
    assert len(invocation.canonical_bytes) == 57_040
    assert invocation.sha256 == (
        'ce8c9b9c2195bc49cc5b8f6122c154b3cadde1b095d2a3a4af19c551879440ef')
    assert invocation.action_id == uuid.UUID(
        'a1fa64dd-eea2-59db-b7b6-733d8001a086')
    assert invocation.launch is not None
    assert invocation.launch.resources.sha256 == (
        'c4f2e770236bb1fc6d903d93676bfc65a0d137d093643d278920fc9ee30e90a1')


def test_down_invocation_literal_golden_bytes_hash_and_action_id() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _down_invocation())
    assert invocation.canonical_bytes == actions.canonical_json_bytes(
        invocation.canonical_value())
    assert len(invocation.canonical_bytes) == 44_986
    assert invocation.sha256 == (
        'c2c8348e397772df91c8347accac0199bb05edefb42da5b400bf9b9444e90b2f')
    assert invocation.action_id == uuid.UUID(
        '324a4cdd-4640-57ae-aea8-b3f65851f735')


def test_provider_plan_commits_invocation_resource_and_target() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.launch is not None
    value = _launch_plan()
    plan = actions.ProviderLifecyclePlanV1.from_value(value)
    plan.validate_invocation(invocation)
    assert plan.action_id == invocation.action_id
    assert len(plan.canonical_bytes) < 65_536

    forged = copy.deepcopy(value)
    forged['request_payload_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='payload hash'):
        actions.ProviderLifecyclePlanV1.from_value(forged).validate_invocation(
            invocation)


@pytest.mark.parametrize(('value_factory', 'parser', 'field_name'), [
    (_launch_invocation, actions.ProviderLifecycleInvocationV1.from_value,
     'invocation.action_kind'),
    (_launch_plan, actions.ProviderLifecyclePlanV1.from_value,
     'plan.action_kind'),
    (_shadow_projection, actions.ServeShadowProjectionV1.from_value,
     'projection.action_kind'),
])
@pytest.mark.parametrize('spoof_type',
                         _SPOOFING_STRING_TYPES,
                         ids=('equality', 'hash', 'length', 'bound'))
def test_action_kind_wire_rejects_string_subclass_before_normalization(
        value_factory, parser, field_name: str, spoof_type: type[str]) -> None:
    value = value_factory()
    value['action_kind'] = spoof_type('launch')

    with pytest.raises(TypeError) as exc_info:
        parser(value)
    assert str(exc_info.value) == f'{field_name} must be text.'


@pytest.mark.parametrize(('value_factory', 'parser', 'error_message'), [
    (_launch_invocation, actions.ProviderLifecycleInvocationV1.from_value,
     'invocation action kind is unsupported.'),
    (_launch_plan, actions.ProviderLifecyclePlanV1.from_value,
     'provider plan action kind is unsupported.'),
    (_shadow_projection, actions.ServeShadowProjectionV1.from_value,
     'shadow projection action kind is unsupported.'),
])
@pytest.mark.parametrize('spoof_type',
                         _SPOOFING_STRING_TYPES,
                         ids=('equality', 'hash', 'length', 'bound'))
def test_action_kind_direct_constructors_are_exact(
        value_factory, parser, error_message: str,
        spoof_type: type[str]) -> None:
    parsed = parser(value_factory())

    accepted = dataclasses.replace(parsed,
                                   action_kind=kernel_actions.ActionKind.LAUNCH)
    assert accepted.action_kind is kernel_actions.ActionKind.LAUNCH
    assert accepted.canonical_bytes == parsed.canonical_bytes

    with pytest.raises(ValueError) as exc_info:
        dataclasses.replace(parsed, action_kind=spoof_type('launch'))
    assert str(exc_info.value) == error_message


def test_provider_resource_identity_action_identity_exact_kind_gate() -> None:
    identity = actions.ProviderResourceIdentityV1.from_value(_identity())

    from_text = identity.action_identity('launch')  # type: ignore[arg-type]
    from_member = identity.action_identity(kernel_actions.ActionKind.LAUNCH)
    assert from_text.action_id == from_member.action_id

    spoofed_values = tuple(
        spoof_type('launch') for spoof_type in _SPOOFING_STRING_TYPES)
    for invalid in (*spoofed_values, 'restart', object()):
        with pytest.raises(ValueError) as exc_info:
            identity.action_identity(invalid)  # type: ignore[arg-type]
        assert str(exc_info.value) == 'action_kind must be launch or down.'


def test_locator_and_plan_literal_golden_bytes_and_hashes() -> None:
    locator = actions.ProviderLocatorV1.from_value(_target())
    assert locator.canonical_bytes == actions.canonical_json_bytes(
        locator.canonical_value())
    assert len(locator.canonical_bytes) == 2_949
    assert locator.sha256 == (
        'deff1707400aeb62ae3b693108c46501054331dab69faa5a5498278263986bf6')

    plan = actions.ProviderLifecyclePlanV1.from_value(_launch_plan())
    assert plan.canonical_bytes == actions.canonical_json_bytes(
        plan.canonical_value())
    assert len(plan.canonical_bytes) == 3_767
    assert plan.sha256 == (
        '10a96fce5a883932cfd10ec8419d8d3026f51b8053e6b73758ba585e034fdc57')


def test_action_spec_literal_golden_bytes_hashes_and_action_ids() -> None:
    launch = actions.ServeReplicaActionSpecV1.from_value(_launch_spec())
    expected_launch = (b'{"invocation":' + launch.invocation.canonical_bytes +
                       b',"provider_plan":' +
                       launch.provider_plan.canonical_bytes + b',"version":1}')
    assert launch.canonical_bytes == expected_launch
    assert len(launch.canonical_bytes) == 60_851
    assert launch.sha256 == (
        '81a770595947e61f0c2095a84ab746e72aed08b554f5acf5e458debd3264b0a7')
    assert launch.action_id == uuid.UUID('a1fa64dd-eea2-59db-b7b6-733d8001a086')
    # The complete P2a release manifest is intentionally retained in the
    # frozen capsule for this dark tranche. It still fits the absolute parser
    # contract, but fails the stricter activation qualification budget. P2b
    # must replace it with a hash-checked durable cohort reference before any
    # represented admission is enabled.
    assert 60_000 < len(launch.canonical_bytes) <= 65_536

    down = actions.ServeReplicaActionSpecV1.from_value(_down_spec())
    assert (len(down.canonical_bytes), down.sha256) == (
        48_919,
        '5c6d4203ab01bf6dd548f3603ffb02446fcfa13504bd149f9409bfb6ff432bb0')
    assert len(down.canonical_bytes) <= 60_000


@pytest.mark.parametrize('mutate,match', [
    (lambda value: value.update({'unexpected': None}), 'unknown or missing'),
    (lambda value: value.update({'version': 1.0}), 'forbids floats'),
    (lambda value: value['provider_plan'].update(
        {'request_payload_sha256': '0' * 64}), 'payload hash'),
    (lambda value: value['provider_plan'].update(
        {'resources_snapshot_sha256': '0' * 64}), 'resource snapshot'),
    (lambda value: value['invocation']['launch'].update(
        {'retry_until_up': False}), 'launch options'),
])
def test_action_spec_rejects_unknown_float_and_unlinked_mutations(
        mutate, match: str) -> None:
    value = _launch_spec()
    mutate(value)
    with pytest.raises((TypeError, ValueError), match=match):
        actions.ServeReplicaActionSpecV1.from_value(value)


@pytest.mark.parametrize('member', ['provider_plan', 'invocation'])
def test_action_spec_rejects_identity_mismatch(member: str) -> None:
    value = _launch_spec()
    if member == 'invocation':
        value[member] = _launch_invocation(generation=2)
    else:
        identity = value[member]['resource_identity']
        replacement = '55555555-5555-4555-8555-555555555555'
        identity['service_hash'] = replacement
        identity['service_incarnation'] = replacement
    with pytest.raises(ValueError, match='action IDs differ'):
        actions.ServeReplicaActionSpecV1.from_value(value)


def test_action_spec_requires_typed_members_and_exact_parent_plan_copy(
) -> None:
    value = _launch_spec()
    plan = actions.ProviderLifecyclePlanV1.from_value(value['provider_plan'])
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        value['invocation'])
    with pytest.raises(TypeError, match='provider_plan'):
        actions.ServeReplicaActionSpecV1(1, value['provider_plan'], invocation)
    with pytest.raises(TypeError, match='invocation'):
        actions.ServeReplicaActionSpecV1(1, plan, value['invocation'])

    spec = actions.ServeReplicaActionSpecV1(1, plan, invocation)
    spec.validate_parent_provider_plan(plan)
    changed_plan_value = copy.deepcopy(value['provider_plan'])
    changed_plan_value['placement_decision_sha256'] = '2' * 64
    changed_plan = actions.ProviderLifecyclePlanV1.from_value(
        changed_plan_value)
    with pytest.raises(ValueError, match='not byte-equal'):
        spec.validate_parent_provider_plan(changed_plan)
    with pytest.raises(TypeError, match='invalid type'):
        spec.validate_parent_provider_plan(changed_plan_value)


def test_persisted_plan_and_spec_graph_rejects_subclasses() -> None:
    plan = actions.ProviderLifecyclePlanV1.from_value(_launch_plan())
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    spec = actions.ServeReplicaActionSpecV1(1, plan, invocation)

    class EvilPlan(actions.ProviderLifecyclePlanV1):

        def canonical_value(self) -> dict:
            value = super().canonical_value()
            value['injected'] = 'accepted-but-unreadable'
            return value

    evil_plan = EvilPlan(**{
        field.name: getattr(plan, field.name)
        for field in dataclasses.fields(plan)
    })
    with pytest.raises(TypeError, match='provider_plan'):
        actions.ServeReplicaActionSpecV1(1, evil_plan, invocation)
    with pytest.raises(TypeError, match='parent provider_plan'):
        spec.validate_parent_provider_plan(evil_plan)

    class EvilInvocation(actions.ProviderLifecycleInvocationV1):

        def canonical_value(self) -> dict:
            value = super().canonical_value()
            value['injected'] = 'accepted-but-unreadable'
            return value

    evil_invocation = EvilInvocation(
        **{
            field.name: getattr(invocation, field.name)
            for field in dataclasses.fields(invocation)
        })
    with pytest.raises(TypeError, match='invocation'):
        actions.ServeReplicaActionSpecV1(1, plan, evil_invocation)
    with pytest.raises(TypeError, match='invocation'):
        plan.validate_invocation(evil_invocation)

    class EvilSpec(actions.ServeReplicaActionSpecV1):

        def canonical_value(self) -> dict:
            value = super().canonical_value()
            value['injected'] = 'accepted-but-unreadable'
            return value

    evil_spec = EvilSpec(1, plan, invocation)
    with pytest.raises(TypeError, match='immutable_spec'):
        resource_action_state.NewShadowSample(
            service_name='svc',
            immutable_spec=evil_spec,
            provider_plan=plan,
            profile_eligibility=actions.ProfileEligibility.UNSUPPORTED)
    with pytest.raises(TypeError, match='provider_plan'):
        resource_action_state.NewShadowSample(
            service_name='svc',
            immutable_spec=spec,
            provider_plan=evil_plan,
            profile_eligibility=actions.ProfileEligibility.UNSUPPORTED)

    refined = invocation.as_launch()
    spec.validate_shadow_child_invocation(
        actions.ShadowRequestRole.PRIMARY_LAUNCH, refined)


def test_persisted_plan_rejects_nested_contract_subclasses() -> None:
    plan = actions.ProviderLifecyclePlanV1.from_value(_launch_plan())

    class EvilIdentity(actions.ProviderResourceIdentityV1):
        pass

    identity = plan.resource_identity
    evil_identity = EvilIdentity(
        **{
            field.name: getattr(identity, field.name)
            for field in dataclasses.fields(identity)
        })
    with pytest.raises(TypeError, match='resource identity'):
        dataclasses.replace(plan, resource_identity=evil_identity)

    class EvilLocator(actions.ProviderLocatorV1):
        pass

    locator = plan.requested_target
    evil_locator = EvilLocator(
        **{
            field.name: getattr(locator, field.name)
            for field in dataclasses.fields(locator)
        })
    with pytest.raises(TypeError, match='requested target'):
        dataclasses.replace(plan, requested_target=evil_locator)

    down_plan = actions.ProviderLifecyclePlanV1.from_value(_down_plan())
    with pytest.raises(ValueError, match='prior_launch_basis_sha256'):
        dataclasses.replace(down_plan,
                            prior_launch_basis_sha256=_EqualitySpoofingString(
                                '0' * 64))
    with pytest.raises(ValueError, match='prior_cleanup_target_sha256'):
        dataclasses.replace(down_plan,
                            prior_cleanup_target_sha256=_EqualitySpoofingString(
                                '0' * 64))


def test_action_spec_primary_invocation_is_an_exact_byte_copy() -> None:
    spec = actions.ServeReplicaActionSpecV1.from_value(_launch_spec())
    spec.validate_shadow_child_invocation(
        actions.ShadowRequestRole.PRIMARY_LAUNCH, spec.invocation)

    changed_value = _launch_invocation(workspace='another-workspace')
    changed = actions.ProviderLifecycleInvocationV1.from_value(changed_value)
    with pytest.raises(ValueError, match='not byte-equal'):
        spec.validate_shadow_child_invocation(
            actions.ShadowRequestRole.PRIMARY_LAUNCH, changed)
    with pytest.raises(ValueError, match='role does not match'):
        spec.validate_shadow_child_invocation(
            actions.ShadowRequestRole.PRIMARY_DOWN, spec.invocation)
    with pytest.raises(TypeError, match='invalid type'):
        spec.validate_shadow_child_invocation(
            actions.ShadowRequestRole.PRIMARY_LAUNCH, changed_value)


def test_action_spec_cleanup_down_is_the_only_child_invocation_exception(
) -> None:
    spec = actions.ServeReplicaActionSpecV1.from_value(_launch_spec())
    cleanup = spec.launch_cleanup_down_invocation()
    assert cleanup.effect_kind.value == 'down'
    assert cleanup.request_role is actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN
    assert cleanup.parent_launch_action_id == spec.action_id
    assert cleanup.parent_launch_request_payload_sha256 == spec.invocation.sha256
    assert cleanup.resource_identity == spec.invocation.resource_identity
    assert cleanup.requested_target == spec.invocation.requested_target
    assert cleanup.legacy_down_request.workspace == 'boltz-test'
    assert cleanup.sha256 == (
        'c917c535a983057ea14dcc8cb4926782b18e124313d4ffb996ae11d43c57961e')
    spec.validate_shadow_child_invocation(
        actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN, cleanup)

    changed_value = cleanup.canonical_value()
    changed_value['legacy_down_request']['workspace'] = 'another-workspace'
    changed = actions.ServeLegacyLaunchCleanupDownInvocationV1.from_value(
        changed_value)
    with pytest.raises(ValueError, match='not byte-equal'):
        spec.validate_shadow_child_invocation(
            actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN, changed)

    changed_parent_hash = cleanup.canonical_value()
    changed_parent_hash['parent_launch_request_payload_sha256'] = '0' * 64
    changed_identity = (actions.ServeLegacyLaunchCleanupDownInvocationV1.
                        from_value(changed_parent_hash))
    with pytest.raises(ValueError, match='not byte-equal'):
        spec.validate_shadow_child_invocation(
            actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN, changed_identity)

    forbidden = cleanup.canonical_value()
    forbidden['prior_launch_basis'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ServeLegacyLaunchCleanupDownInvocationV1.from_value(forbidden)


def test_action_spec_completed_down_stays_below_rollout_and_parser_bounds(
) -> None:
    value = _down_spec()
    spec = actions.ServeReplicaActionSpecV1.from_value(value)

    assert spec.canonical_bytes == actions.canonical_json_bytes(value)
    assert len(spec.canonical_bytes) == 48_919
    assert len(spec.canonical_bytes) <= 60_000
    assert len(spec.canonical_bytes) <= 65_536


@pytest.mark.parametrize('mutate,match', [
    (lambda value: value.update({'credentials': 'secret'}),
     'unknown or missing'),
    (lambda value: value['launch'].update(
        {'private_key': '-----BEGIN PRIVATE KEY-----'}), 'unknown or missing'),
    (lambda value: value['launch']['replica_env'].update(
        {'API_TOKEN': 'secret'}), 'unknown or missing'),
    (lambda value: value['launch']['resources'].update({'kubeconfig': 'secret'}
                                                      ), 'unknown or missing'),
    (lambda value: value['resource_identity'].update(
        {'service_incarnation': _CLUSTER_UUID}), 'service_hash'),
    (lambda value: value['requested_target']['kubernetes'].update(
        {'replica_incarnation_label': _CLUSTER_UUID}),
     'topology identity labels'),
    (lambda value: value['requested_target'].update(
        {'sky_cluster_record_uuid': _CLUSTER_UUID.replace('-', '')}),
     'lowercase hyphenated'),
    (lambda value: value['launch']['resources'].update({'disk_size_gb': 100.0}),
     'forbids floating-point values'),
    (lambda value: value['launch']['resources'].update(
        {'ports': ['8001', '8000']}), 'sorted and unique'),
    (lambda value: value['launch']['resources'].update({
        'labels': [{
            'key': 'replica',
            'value': '7'
        }, {
            'key': 'app',
            'value': 'serve'
        }]
    }), 'sorted by unique key'),
    (lambda value: value['launch']['source']['content'].update(
        {'service_name': 'x' * 1025}), '1..1024'),
    (lambda value: value['launch']['replica_env'].update(
        {'SKYPILOT_SERVE_REPLICA_ID': '1' * 1025}), '1..1024'),
    (lambda value: value['launch']['source']['content'].update(
        {'service_name': 'e\u0301'}), 'not canonical|NFC-normalized'),
    (lambda value: value['requested_target'].update({'version': 2}),
     'integer 1'),
])
def test_invocation_rejects_closed_shape_identity_bounds_and_secrets(
        mutate, match: str) -> None:
    value = _launch_invocation()
    mutate(value)
    with pytest.raises((TypeError, ValueError), match=match):
        actions.ProviderLifecycleInvocationV1.from_value(value)


def test_redaction_profile_marks_opaque_material_outside_first_cohort() -> None:
    value = _launch_invocation()
    value['launch']['tls_material_ref'] = 'secret/tls-material-v1'
    with pytest.raises(ValueError, match='tls_material_ref must be null'):
        actions.ProviderLifecycleInvocationV1.from_value(value)
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.launch is not None
    assert invocation.launch.first_authority_cohort_redacted


def test_provider_evidence_contracts_are_closed_and_cross_checked() -> None:
    error = actions.ProviderErrorV1.from_value({
        'category': 'quota',
        'provider_code': 'TooManyPods',
        'retry_after_seconds': 3,
        'normalized_message': 'quota unavailable',
    })
    submission = actions.ProviderSubmissionV1.from_value({
        'disposition': 'ambiguous',
        'provider_operation_id': None,
        'normalized_response_sha256': None,
        'normalized_error': error.canonical_value(),
    })
    assert submission.normalized_error == error

    target = actions.ProviderLocatorV1.from_value(_target())
    resolved = actions.ResolvedProviderTargetV1.from_value(_resolved_target())
    resolved.validate_requested_target(target)
    observation = actions.ProviderLifecycleObservationV1.from_value(
        _observation())
    observation.validate_target(target)
    assert observation.resolved_target == resolved

    bad = _observation()
    bad['target_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='resolved target hash'):
        actions.ProviderLifecycleObservationV1.from_value(bad)
    bad = _observation()
    bad['observed_at'] = '2026-08-01T01:02:04Z'
    with pytest.raises(ValueError, match='six fractional'):
        actions.ProviderLifecycleObservationV1.from_value(bad)
    with pytest.raises(ValueError, match='submission evidence'):
        actions.ProviderSubmissionV1.from_value({
            'disposition': 'not_submitted',
            'provider_operation_id': 'operation-1',
            'normalized_response_sha256': None,
            'normalized_error': None,
        })


def test_authoritative_present_observation_requires_exact_identity() -> None:
    target = actions.ProviderLocatorV1.from_value(_target())
    missing = _observation()
    for field in ('observed_cluster_record_uuid',
                  'observed_replica_incarnation_label', 'observed_workload_uid',
                  'resolved_target'):
        missing[field] = None
    with pytest.raises(ValueError, match='complete resolved identity'):
        actions.ProviderLifecycleObservationV1.from_value(missing)

    mismatched_cluster = _observation()
    mismatched_cluster['observed_cluster_record_uuid'] = _SERVICE_UUID
    with pytest.raises(ValueError, match='frozen target identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            mismatched_cluster).validate_target(target)

    mismatched_label = _observation()
    mismatched_label['observed_replica_incarnation_label'] = _SERVICE_UUID
    with pytest.raises(ValueError, match='frozen target identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            mismatched_label).validate_target(target)

    mismatched_native = _observation()
    mismatched_native['observed_workload_uid'] = 'replacement-uid'
    with pytest.raises(ValueError, match='resolved identity conflicts'):
        actions.ProviderLifecycleObservationV1.from_value(mismatched_native)

    missing_resolved_native = _observation()
    missing_resolved_native['resolved_target']['provider_resource_id'] = None
    with pytest.raises(ValueError, match='complete resolved identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            missing_resolved_native)

    missing_resolved_operation = _observation()
    missing_resolved_operation['observed_provider_operation_id'] = 'op-new'
    with pytest.raises(ValueError, match='complete resolved identity'):
        actions.ProviderLifecycleObservationV1.from_value(
            missing_resolved_operation)

    conflict = mismatched_native
    conflict['state'] = 'conflict'
    conflict['certainty'] = 'authoritative'
    conflict['ready'] = None
    actions.ProviderLifecycleObservationV1.from_value(conflict).validate_target(
        target)


def test_direct_construction_rejects_float_versions_and_is_immutable() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    assert invocation.launch is not None
    objects = [
        invocation.requested_target,
        actions.ResolvedProviderTargetV1.from_value(_resolved_target()),
        invocation.launch.resources,
        invocation,
        actions.ProviderLifecyclePlanV1.from_value(_launch_plan()),
        actions.ProviderLifecycleObservationV1.from_value(_observation()),
        actions.ServeShadowProjectionV1.from_value({
            'version': 1,
            'action_kind': 'launch',
            'row_disposition': 'retained',
            'replica_status': 'READY',
            'capacity_outcome': 'success',
            'action_disposition': 'succeeded',
            'resolved_target': _resolved_target(),
        }),
        actions.ServeShadowRetryDecisionV1.from_value({
            'version': 1,
            'decision': 'observe',
            'retry_class': 'observation_required',
            'delay_seconds': 5,
            'logical_attempt': 2,
        }),
    ]
    for value in objects:
        with pytest.raises(ValueError, match='integer 1'):
            dataclasses.replace(value, version=1.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        invocation.profile = actions.ProviderProfile.POD_CLUSTER_V1


def test_collection_and_complete_object_bounds() -> None:
    too_many_ports = _launch_invocation()
    too_many_ports['launch']['resources']['ports'] = [
        f'{port:03d}' for port in range(257)
    ]
    with pytest.raises(ValueError, match='at most 256'):
        actions.ProviderLifecycleInvocationV1.from_value(too_many_ports)

    oversized = _launch_invocation()
    oversized['launch']['resources']['labels'] = [{
        'key': f'{index:03d}' + 'k' * 250,
        'value': 'v' * 253,
    } for index in range(256)]
    with pytest.raises(ValueError, match='exceeds 65536'):
        actions.ProviderLifecycleInvocationV1.from_value(oversized)


def test_outcome_projection_and_retry_contract_combinations() -> None:
    outcome = actions.ServeReplicaActionOutcomeV1.from_value({
        'disposition': 'succeeded',
        'certainty': 'observed',
        'provider_operation_id': None,
        'provider_code': None,
        'retry_class': None,
        'retry_after_seconds': None,
        'observation': _observation(),
        'normalized_message': None,
    })
    projection = actions.ServeShadowProjectionV1.from_value({
        'version': 1,
        'action_kind': 'launch',
        'row_disposition': 'retained',
        'replica_status': 'READY',
        'capacity_outcome': 'success',
        'action_disposition': 'succeeded',
        'resolved_target': _resolved_target(),
    })
    retry = actions.ServeShadowRetryDecisionV1.from_value({
        'version': 1,
        'decision': 'observe',
        'retry_class': 'observation_required',
        'delay_seconds': 5,
        'logical_attempt': 2,
    })
    assert outcome.disposition is actions.ServeActionDisposition.SUCCEEDED
    assert projection.replica_status is actions.ReplicaStatusValue.READY
    assert retry.decision is actions.ShadowRetryDecision.OBSERVE

    bad_outcome = outcome.canonical_value()
    bad_outcome['retry_class'] = 'transient'
    with pytest.raises(ValueError, match='terminal outcome'):
        actions.ServeReplicaActionOutcomeV1.from_value(bad_outcome)
    with pytest.raises(ValueError, match='capacity_outcome'):
        actions.ServeShadowProjectionV1.from_value({
            **projection.canonical_value(),
            'action_kind': 'down',
        })
    with pytest.raises(ValueError, match='cannot contain retry'):
        actions.ServeShadowRetryDecisionV1.from_value({
            'version': 1,
            'decision': 'terminal',
            'retry_class': 'transient',
            'delay_seconds': 1,
            'logical_attempt': 1,
        })


def test_succeeded_launch_outcome_requires_observed_ready_present_proof(
) -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    outcome = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(observation=_observation()))

    outcome.validate_for_invocation(invocation)


def test_succeeded_down_and_launch_cleanup_require_observed_absence() -> None:
    outcome = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(observation=_absent_observation()))
    primary_down = actions.ProviderLifecycleInvocationV1.from_value(
        _down_invocation())
    cleanup_down = actions.ServeReplicaActionSpecV1.from_value(
        _launch_spec()).launch_cleanup_down_invocation()

    outcome.validate_for_invocation(primary_down)
    outcome.validate_for_invocation(cleanup_down)


def test_succeeded_outcome_rejects_missing_or_acknowledged_proof() -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    missing = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(observation=None))
    acknowledged = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(certainty='provider_acknowledged', observation=None))

    with pytest.raises(ValueError, match='requires an observation'):
        missing.validate_for_invocation(invocation)
    with pytest.raises(ValueError,
                       match='acknowledgement is not success proof'):
        acknowledged.validate_for_invocation(invocation)


def test_non_success_outcomes_accept_only_matching_optional_observation(
) -> None:
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())
    retryable = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(disposition='retryable',
                 certainty='unknown',
                 observation=_observation()))
    terminal = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(disposition='terminal_error',
                 certainty='unknown',
                 observation=None))

    retryable.validate_for_invocation(invocation)
    terminal.validate_for_invocation(invocation)


@pytest.mark.parametrize(('observation_update', 'message'), [
    ({
        'certainty': 'eventually_consistent'
    }, 'authoritative'),
    ({
        'ready': False
    }, 'ready=True'),
    ({
        'state': 'absent',
        'ready': None,
        'resolved_target': None,
        'observed_provider_operation_id': None,
        'observed_provider_resource_id': None,
        'observed_cluster_record_uuid': None,
        'observed_workload_uid': None,
        'observed_replica_incarnation_label': None,
    }, 'PRESENT'),
])
def test_succeeded_launch_rejects_incomplete_or_wrong_observation(
        observation_update: dict, message: str) -> None:
    observation = _observation()
    observation.update(observation_update)
    outcome = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(observation=observation))
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())

    with pytest.raises(ValueError, match=message):
        outcome.validate_for_invocation(invocation)


@pytest.mark.parametrize(('observation_update', 'message'), [
    ({
        'certainty': 'eventually_consistent'
    }, 'authoritative'),
    (_observation(), 'ABSENT'),
])
def test_succeeded_down_rejects_eventual_or_present_observation(
        observation_update: dict, message: str) -> None:
    observation = _absent_observation()
    observation.update(observation_update)
    outcome = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(observation=observation))
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _down_invocation())

    with pytest.raises(ValueError, match=message):
        outcome.validate_for_invocation(invocation)


def test_outcome_proof_rejects_wrong_target_and_invocation_type() -> None:
    wrong_target = _target()
    wrong_target['sky_cluster_record_uuid'] = _SERVICE_UUID
    wrong_target['kubernetes']['cluster_record_uuid_label'] = _SERVICE_UUID
    for mutable_object in wrong_target['kubernetes']['topology'][
            'mutable_objects']:
        for label in mutable_object['labels']:
            if label['key'] == 'skypilot.co/cluster-record-uuid':
                label['value'] = _SERVICE_UUID
    wrong_target_hash = actions.ProviderLocatorV1.from_value(
        wrong_target).sha256
    observation = _observation()
    observation['target_sha256'] = wrong_target_hash
    observation['resolved_target'][
        'requested_target_sha256'] = wrong_target_hash
    outcome = actions.ServeReplicaActionOutcomeV1.from_value(
        _outcome(disposition='retryable',
                 certainty='unknown',
                 observation=observation))
    invocation = actions.ProviderLifecycleInvocationV1.from_value(
        _launch_invocation())

    with pytest.raises(ValueError, match='does not match requested target'):
        outcome.validate_for_invocation(invocation)
    with pytest.raises(TypeError, match='invocation has an invalid type'):
        outcome.validate_for_invocation(  # type: ignore[arg-type]
            _launch_invocation())


def test_shadow_state_role_eligibility_parity_and_divergence_vocabularies(
) -> None:
    assert {value.value for value in actions.ShadowParentPhase} == {
        'PENDING', 'RUNNING', 'COMPLETE', 'ABANDONED_PRE_SUBMIT', 'AMBIGUOUS'
    }
    assert {value.value for value in actions.ShadowAttemptPhase} == {
        'PRE_SUBMIT', 'REQUEST_BOUND', 'COMPLETE', 'ABANDONED_PRE_SUBMIT',
        'REQUEST_ASSOCIATION_UNKNOWN'
    }
    assert {value.value for value in actions.ShadowRequestRole
           } == {'PRIMARY_LAUNCH', 'PRIMARY_DOWN', 'LAUNCH_CLEANUP_DOWN'}
    assert {value.value for value in actions.ProfileEligibility
           } == {'ELIGIBLE', 'UNSUPPORTED'}
    assert actions.ShadowDivergenceClass.IDENTITY_MISMATCH.parity_class is (
        actions.ShadowParityClass.IDENTITY_MISMATCH)
    assert actions.PlannedExecutionKind.LEGACY_DIRECT_DOWN.value == (
        'legacy_direct_down')
