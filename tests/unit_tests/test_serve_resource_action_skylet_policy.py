"""Pure tests for the Skylet submission and policy-proof leaf contracts."""

import copy
import dataclasses
import json

import pytest

from sky.serve import resource_actions as actions
from tests.unit_tests import test_serve_resource_action_launch_execution_config

launch_config_fixtures = test_serve_resource_action_launch_execution_config
_SUBMISSION_KEY = '11111111-1111-4111-8111-111111111111'
_OTHER_SUBMISSION_KEY = '22222222-2222-4222-8222-222222222222'
_STATE_STORE_UUID = '33333333-3333-4333-8333-333333333333'
_MAX_BIGINT = 2**63 - 1


def _source() -> dict:
    return {
        'store': 'serve_version_specs',
        'service_name': 'svc',
        'service_incarnation': '44444444-4444-4444-8444-444444444444',
        'service_version': 3,
        'yaml_content_sha256': 'a' * 64,
        'workspace': 'workspace-a',
    }


def _job_spec(replica_id: str = '7') -> dict:
    return {
        'version': 1,
        'schema_id': 'skypilot.serve.prebooted-canary-job.v1',
        'source': _source(),
        'command_profile': 'image_serve_canary_entrypoint_v1',
        'entrypoint_artifact_role': 'serve_canary_entrypoint',
        'replica_id': replica_id,
        'environment': {
            'SKYPILOT_SERVE_REPLICA_ID': replica_id,
        },
        'working_directory': None,
        'setup': None,
        'mounts': [],
        'secrets': [],
        'lifecycle': 'long_running_until_pod_delete',
        'restart_policy': 'same_pod_same_logical_job',
    }


def _submit_request(*,
                    submission_key: str = _SUBMISSION_KEY,
                    replica_id: str = '7') -> dict:
    job_spec = _job_spec(replica_id)
    return {
        'protocol': 'skylet_idempotent_submit_v1',
        'submission_key': submission_key,
        'job_contract_sha256': 'b' * 64,
        'job_spec': job_spec,
        'job_spec_sha256': actions.canonical_sha256(job_spec),
    }


def _job_evidence(disposition: str = 'present',
                  *,
                  durable_state: str = 'RUNNING') -> dict:
    submit_request = _submit_request()
    has_record = disposition in ('present', 'conflict')
    return {
        'protocol': 'skylet_idempotent_submit_v1',
        'submission_key': _SUBMISSION_KEY,
        'job_contract_sha256': submit_request['job_contract_sha256'],
        'job_spec_sha256': submit_request['job_spec_sha256'],
        'retained_submit_request': submit_request if has_record else None,
        'state_store_uuid': _STATE_STORE_UUID,
        'read_disposition': disposition,
        'durable_state': durable_state if has_record else None,
        'job_id': 19 if has_record else None,
        'run_epoch': 0 if has_record else None,
        'record_revision': 2 if has_record else None,
        'observed_at': '2026-08-01T12:34:56.123456Z',
    }


def _policy_modes() -> dict:
    return {
        'admin_policy_entrypoint': None,
        'admin_policy_applied': False,
        'managed_secrets_provider': None,
        'managed_secret_reference_count': 0,
    }


def _policy_proof(boundary: str = 'serve_controller_prepare') -> dict:
    return {
        'version': 1,
        'boundary': boundary,
        'config_projection_sha256': 'c' * 64,
        'modes': _policy_modes(),
        'policy_subject_sha256': 'd' * 64,
        'projection_before_sha256': 'e' * 64,
        'projection_after_sha256': 'e' * 64,
        'projections_equal': True,
    }


@pytest.mark.parametrize(('factory', 'parser'), [
    (_job_spec, actions.ProviderSkyletJobSpecV1.from_value),
    (_submit_request, actions.ProviderSkyletSubmitRequestV1.from_value),
    (_job_evidence, actions.ProviderSkyletJobEvidenceV1.from_value),
    (_policy_proof, actions.ProviderPolicyBoundaryProofV1.from_value),
])
def test_leaf_round_trip_and_canonical_bytes(factory, parser) -> None:
    raw = factory()
    parsed = parser(raw)

    assert parsed.canonical_value() == raw
    assert parsed.canonical_bytes == actions.canonical_json_bytes(raw)
    assert parser(json.loads(parsed.canonical_bytes)) == parsed


@pytest.mark.parametrize(('factory', 'parser'), [
    (_job_spec, actions.ProviderSkyletJobSpecV1.from_value),
    (_submit_request, actions.ProviderSkyletSubmitRequestV1.from_value),
    (_job_evidence, actions.ProviderSkyletJobEvidenceV1.from_value),
    (_policy_proof, actions.ProviderPolicyBoundaryProofV1.from_value),
])
def test_leaf_contracts_reject_unknown_top_level_keys(factory, parser) -> None:
    raw = factory()
    raw['unknown'] = None

    with pytest.raises(ValueError, match='unknown or missing fields'):
        parser(raw)


def test_job_spec_direct_construction_uses_tuples_but_wire_uses_lists() -> None:
    raw = _job_spec()
    source = actions.ProviderLaunchContentSourceV1.from_value(raw['source'])
    direct = actions.ProviderSkyletJobSpecV1(
        version=raw['version'],
        schema_id=raw['schema_id'],
        source=source,
        command_profile=raw['command_profile'],
        entrypoint_artifact_role=raw['entrypoint_artifact_role'],
        replica_id=raw['replica_id'],
        environment_replica_id=raw['environment']['SKYPILOT_SERVE_REPLICA_ID'],
        working_directory=None,
        setup=None,
        mounts=(),
        secrets=(),
        lifecycle=raw['lifecycle'],
        restart_policy=raw['restart_policy'])

    assert direct.canonical_value() == raw

    class DeceptiveTuple(tuple):

        def __bool__(self) -> bool:
            return False

    for field in ('mounts', 'secrets'):
        with pytest.raises(TypeError, match=f'{field} must be a tuple'):
            dataclasses.replace(direct, **{field: []})
        with pytest.raises(TypeError, match=f'{field} must be a tuple'):
            dataclasses.replace(direct, **{field: DeceptiveTuple(('hidden',))})
        wire = _job_spec()
        wire[field] = ()
        with pytest.raises((TypeError, ValueError)):
            actions.ProviderSkyletJobSpecV1.from_value(wire)


@pytest.mark.parametrize('replica_id', ['0', str(_MAX_BIGINT)])
def test_decimal_integer_text_accepts_signed_int64_endpoints(
        replica_id: str) -> None:
    parsed = actions.ProviderSkyletJobSpecV1.from_value(_job_spec(replica_id))
    assert parsed.replica_id == replica_id
    assert parsed.environment_replica_id == replica_id


@pytest.mark.parametrize('replica_id', [
    '00',
    '01',
    '+1',
    '-1',
    '1.0',
    '1e3',
    '1 ',
    '\u0661',
    '1' * 20,
    str(_MAX_BIGINT + 1),
    1,
])
def test_decimal_integer_text_rejects_noncanonical_or_out_of_bounds_values(
        replica_id) -> None:
    raw = _job_spec()
    raw['replica_id'] = replica_id
    raw['environment']['SKYPILOT_SERVE_REPLICA_ID'] = replica_id

    with pytest.raises((TypeError, ValueError)):
        actions.ProviderSkyletJobSpecV1.from_value(raw)


def test_launch_policy_subject_reuses_signed_int64_decimal_bound() -> None:
    subject = launch_config_fixtures._subject()
    maximum = dataclasses.replace(subject, replica_id_text=str(_MAX_BIGINT))
    assert maximum.replica_id_text == str(_MAX_BIGINT)
    with pytest.raises(ValueError, match='canonical decimal integer text'):
        dataclasses.replace(subject, replica_id_text=str(_MAX_BIGINT + 1))


@pytest.mark.parametrize(('mutate', 'match'), [
    (lambda value: value.update({'version': 2}), 'integer 1'),
    (lambda value: value.update({'schema_id': 'other'}), 'schema_id'),
    (lambda value: value.update({'command_profile': 'shell'}),
     'command_profile'),
    (lambda value: value.update({'entrypoint_artifact_role': 'other'}),
     'entrypoint_artifact_role'),
    (lambda value: value.update({'working_directory': '/tmp'}),
     'working_directory'),
    (lambda value: value.update({'setup': 'echo setup'}), 'setup'),
    (lambda value: value.update({'mounts': ['/tmp']}), 'mounts'),
    (lambda value: value.update({'secrets': ['TOKEN']}), 'secrets'),
    (lambda value: value.update({'lifecycle': 'finite'}), 'lifecycle'),
    (lambda value: value.update({'restart_policy': 'never'}), 'restart_policy'),
    (lambda value: value['source'].update({'store': 'ambient'}),
     'launch source store'),
])
def test_job_spec_rejects_every_nonfixed_literal(mutate, match: str) -> None:
    raw = _job_spec()
    mutate(raw)

    with pytest.raises((TypeError, ValueError), match=match):
        actions.ProviderSkyletJobSpecV1.from_value(raw)


def test_job_spec_rejects_replica_copy_mismatch_and_environment_unknown_key(
) -> None:
    mismatch = _job_spec()
    mismatch['environment']['SKYPILOT_SERVE_REPLICA_ID'] = '8'
    with pytest.raises(ValueError, match='byte-equal'):
        actions.ProviderSkyletJobSpecV1.from_value(mismatch)

    unknown = _job_spec()
    unknown['environment']['OTHER'] = '7'
    with pytest.raises(ValueError, match='unknown or missing fields'):
        actions.ProviderSkyletJobSpecV1.from_value(unknown)


def test_job_spec_direct_construction_requires_typed_source() -> None:
    parsed = actions.ProviderSkyletJobSpecV1.from_value(_job_spec())
    with pytest.raises(TypeError, match='source has an invalid type'):
        dataclasses.replace(parsed, source=_source())


def test_submit_request_checks_internal_spec_hash_only() -> None:
    raw = _submit_request(submission_key=_OTHER_SUBMISSION_KEY)
    raw['job_contract_sha256'] = 'f' * 64
    parsed = actions.ProviderSkyletSubmitRequestV1.from_value(raw)

    assert str(parsed.submission_key) == _OTHER_SUBMISSION_KEY
    assert parsed.job_contract_sha256 == 'f' * 64
    assert parsed.job_spec_sha256 == parsed.job_spec.sha256

    raw['job_spec_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='does not match job_spec'):
        actions.ProviderSkyletSubmitRequestV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('protocol', 'other'),
    ('submission_key', '11111111-1111-4111-8111-11111111111A'),
    ('job_contract_sha256', 'A' * 64),
    ('job_spec_sha256', 'short'),
])
def test_submit_request_rejects_invalid_protocol_uuid_and_hash_shapes(
        field: str, value) -> None:
    raw = _submit_request()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderSkyletSubmitRequestV1.from_value(raw)


@pytest.mark.parametrize('disposition',
                         ['present', 'conflict', 'not_found', 'uncertain'])
def test_job_evidence_accepts_exact_disposition_nullability_matrix(
        disposition: str) -> None:
    raw = _job_evidence(disposition)
    parsed = actions.ProviderSkyletJobEvidenceV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert (parsed.retained_submit_request
            is not None) == (disposition in ('present', 'conflict'))


@pytest.mark.parametrize('disposition', ['present', 'conflict'])
@pytest.mark.parametrize('field', [
    'retained_submit_request',
    'durable_state',
    'job_id',
    'run_epoch',
    'record_revision',
])
def test_present_and_conflict_require_complete_retained_record(
        disposition: str, field: str) -> None:
    raw = _job_evidence(disposition)
    raw[field] = None
    with pytest.raises(ValueError, match='complete retained record'):
        actions.ProviderSkyletJobEvidenceV1.from_value(raw)


@pytest.mark.parametrize('disposition', ['not_found', 'uncertain'])
@pytest.mark.parametrize(('field', 'value'), [
    ('retained_submit_request', _submit_request()),
    ('durable_state', 'RUNNING'),
    ('job_id', 19),
    ('run_epoch', 0),
    ('record_revision', 2),
])
def test_not_found_and_uncertain_require_null_retained_record_values(
        disposition: str, field: str, value) -> None:
    raw = _job_evidence(disposition)
    raw[field] = copy.deepcopy(value)
    with pytest.raises(ValueError, match='null retained record'):
        actions.ProviderSkyletJobEvidenceV1.from_value(raw)


@pytest.mark.parametrize('durable_state', [
    'COMMITTED_PENDING_START',
    'START_INTENT',
    'START_COMMITTED',
    'RUNNING',
    'RECOVERY_PENDING',
    'SUCCEEDED',
    'FAILED',
    'BLOCKED',
])
def test_conflict_leaf_allows_every_generic_nonnull_durable_state(
        durable_state: str) -> None:
    parsed = actions.ProviderSkyletJobEvidenceV1.from_value(
        _job_evidence('conflict', durable_state=durable_state))
    assert parsed.durable_state.value == durable_state


def test_job_evidence_requires_retained_request_submission_key_relation(
) -> None:
    raw = _job_evidence('present')
    raw['retained_submit_request']['submission_key'] = _OTHER_SUBMISSION_KEY
    with pytest.raises(ValueError, match='top-level submission key'):
        actions.ProviderSkyletJobEvidenceV1.from_value(raw)


def test_job_evidence_validates_retained_request_internal_hash() -> None:
    raw = _job_evidence('present')
    raw['retained_submit_request']['job_spec_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='does not match job_spec'):
        actions.ProviderSkyletJobEvidenceV1.from_value(raw)


def test_job_evidence_does_not_compare_contextual_expected_hashes() -> None:
    raw = _job_evidence('present')
    raw['job_contract_sha256'] = 'e' * 64
    raw['job_spec_sha256'] = 'f' * 64
    parsed = actions.ProviderSkyletJobEvidenceV1.from_value(raw)

    assert parsed.retained_submit_request is not None
    assert (parsed.job_contract_sha256
            != parsed.retained_submit_request.job_contract_sha256)
    assert parsed.job_spec_sha256 != parsed.retained_submit_request.job_spec_sha256


def test_conflict_leaf_allows_equal_hash_unequal_request_bytes(
        monkeypatch: pytest.MonkeyPatch) -> None:
    mocked_digest = '6' * 64
    monkeypatch.setattr(actions, 'canonical_sha256',
                        lambda unused_value: mocked_digest)
    expected_request = _submit_request()
    retained_request = copy.deepcopy(expected_request)
    retained_request['job_spec']['source']['workspace'] = 'workspace-b'
    assert (actions.canonical_json_bytes(expected_request)
            != actions.canonical_json_bytes(retained_request))
    assert (expected_request['job_contract_sha256'] ==
            retained_request['job_contract_sha256'])
    assert (expected_request['job_spec_sha256'] ==
            retained_request['job_spec_sha256'] == mocked_digest)
    expected = actions.ProviderSkyletSubmitRequestV1.from_value(
        expected_request)
    retained = actions.ProviderSkyletSubmitRequestV1.from_value(
        retained_request)
    assert expected.canonical_bytes != retained.canonical_bytes

    raw = _job_evidence('conflict')
    raw['job_contract_sha256'] = expected_request['job_contract_sha256']
    raw['job_spec_sha256'] = expected_request['job_spec_sha256']
    raw['retained_submit_request'] = retained_request
    parsed = actions.ProviderSkyletJobEvidenceV1.from_value(raw)

    assert parsed.retained_submit_request is not None
    assert (parsed.job_contract_sha256 ==
            parsed.retained_submit_request.job_contract_sha256)
    assert parsed.job_spec_sha256 == parsed.retained_submit_request.job_spec_sha256
    assert 'expected_submit_request' not in parsed.canonical_value()


@pytest.mark.parametrize(('field', 'value'), [
    ('job_id', 0),
    ('job_id', _MAX_BIGINT + 1),
    ('job_id', True),
    ('run_epoch', -1),
    ('run_epoch', _MAX_BIGINT + 1),
    ('run_epoch', True),
    ('record_revision', 0),
    ('record_revision', _MAX_BIGINT + 1),
    ('record_revision', True),
])
def test_job_evidence_rejects_integer_values_outside_exact_bounds(
        field: str, value) -> None:
    raw = _job_evidence('present')
    raw[field] = value
    with pytest.raises(ValueError):
        actions.ProviderSkyletJobEvidenceV1.from_value(raw)


def test_job_evidence_accepts_integer_bounds() -> None:
    raw = _job_evidence('present')
    raw['job_id'] = _MAX_BIGINT
    raw['run_epoch'] = _MAX_BIGINT
    raw['record_revision'] = _MAX_BIGINT
    parsed = actions.ProviderSkyletJobEvidenceV1.from_value(raw)
    assert parsed.job_id == _MAX_BIGINT
    assert parsed.run_epoch == _MAX_BIGINT
    assert parsed.record_revision == _MAX_BIGINT


@pytest.mark.parametrize(('field', 'value'), [
    ('protocol', 'other'),
    ('submission_key', 'not-a-uuid'),
    ('job_contract_sha256', 'A' * 64),
    ('job_spec_sha256', 'short'),
    ('state_store_uuid', 'not-a-uuid'),
    ('read_disposition', 'absent'),
    ('durable_state', 'QUEUED'),
    ('observed_at', '2026-08-01T12:34:56Z'),
])
def test_job_evidence_rejects_invalid_scalar_shapes(field: str, value) -> None:
    raw = _job_evidence('present')
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderSkyletJobEvidenceV1.from_value(raw)


@pytest.mark.parametrize('boundary',
                         ['serve_controller_prepare', 'api_executor_pre_io'])
def test_policy_boundary_proof_accepts_exact_boundaries(boundary: str) -> None:
    raw = _policy_proof(boundary)
    assert actions.ProviderPolicyBoundaryProofV1.from_value(
        raw).canonical_value() == raw


@pytest.mark.parametrize(('field', 'value'), [
    ('admin_policy_entrypoint', 'policy.module:apply'),
    ('admin_policy_applied', True),
    ('managed_secrets_provider', 'vault'),
    ('managed_secret_reference_count', 1),
])
def test_policy_boundary_proof_requires_absent_policy_modes(field: str,
                                                            value) -> None:
    raw = _policy_proof()
    raw['modes'][field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderPolicyBoundaryProofV1.from_value(raw)


@pytest.mark.parametrize('field', [
    'config_projection_sha256',
    'policy_subject_sha256',
    'projection_before_sha256',
    'projection_after_sha256',
])
def test_policy_boundary_proof_requires_lowercase_sha256_shape(
        field: str) -> None:
    raw = _policy_proof()
    raw[field] = 'A' * 64
    with pytest.raises(ValueError, match='lowercase SHA-256'):
        actions.ProviderPolicyBoundaryProofV1.from_value(raw)


def test_policy_boundary_proof_requires_equal_projection_hashes_and_true_flag(
) -> None:
    unequal = _policy_proof()
    unequal['projection_after_sha256'] = 'f' * 64
    with pytest.raises(ValueError, match='equal hashes'):
        actions.ProviderPolicyBoundaryProofV1.from_value(unequal)

    false_flag = _policy_proof()
    false_flag['projections_equal'] = False
    with pytest.raises(ValueError, match='must be true'):
        actions.ProviderPolicyBoundaryProofV1.from_value(false_flag)

    non_boolean = _policy_proof()
    non_boolean['projections_equal'] = 1
    with pytest.raises(TypeError, match='Boolean'):
        actions.ProviderPolicyBoundaryProofV1.from_value(non_boolean)


def test_policy_boundary_proof_is_context_free_beyond_local_equality() -> None:
    raw = _policy_proof()
    raw['config_projection_sha256'] = '1' * 64
    raw['policy_subject_sha256'] = '2' * 64
    raw['projection_before_sha256'] = '3' * 64
    raw['projection_after_sha256'] = '3' * 64

    parsed = actions.ProviderPolicyBoundaryProofV1.from_value(raw)
    assert parsed.canonical_value() == raw


def test_policy_boundary_proof_rejects_unknown_boundary_and_nested_mode_key(
) -> None:
    unknown_boundary = _policy_proof('controller')
    with pytest.raises(ValueError, match='boundary is unsupported'):
        actions.ProviderPolicyBoundaryProofV1.from_value(unknown_boundary)

    unknown_mode = _policy_proof()
    unknown_mode['modes']['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing fields'):
        actions.ProviderPolicyBoundaryProofV1.from_value(unknown_mode)
