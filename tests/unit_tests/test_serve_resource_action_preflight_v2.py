"""Closed Serve039 authority-preflight V2 value tests."""
# pylint: disable=protected-access,too-many-locals

import copy
import uuid

import pytest
import test_serve_resource_action_down_execution_config as down_config_fixtures
import test_serve_resource_action_provider_preflight as v1_preflight_fixtures
import test_serve_resource_action_v2_identity as v2_fixtures

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_cleanup_v2 as cleanup_v2
from sky.serve import resource_action_preflight_v2 as preflight
from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions

_CONTRACT = 'provider_kubernetes_preflight_v2'
_NONCE = '12345678-1234-4234-8234-123456789abc'


def _launch_request() -> preflight.ProviderAuthorityPreflightRequestV2:
    spec = actions.serve_replica_action_spec_from_value_v2(
        v2_fixtures._v2_launch_spec())
    launch = spec.invocation.require_launch()
    capsule = launch.execution_config.capsule
    target = spec.invocation.requested_target
    kubernetes = target.kubernetes
    assert kubernetes is not None
    seed = preflight.ProviderLaunchPreflightSeedV2(
        version=2,
        resource_identity=spec.invocation.resource_identity,
        workspace=launch.source.content.workspace,
        source=launch.source,
        requested_target=target,
        requested_cloud='kubernetes',
        context_mode='in_cluster',
        target_namespace=kubernetes.namespace,
        resources=launch.resources,
        topology=launch.topology,
        replica_id=spec.invocation.resource_identity.replica_id,
        retry_until_up=launch.retry_until_up,
        request_identity=capsule.request_identity,
        config_projection=capsule.config_projection)
    return preflight.ProviderAuthorityPreflightRequestV2.create(
        action_kind=kernel_actions.ActionKind.LAUNCH,
        nonce=_NONCE,
        seed=seed,
        expected_cohort_manifest=v2_fixtures._v2_authority_cohort().manifest)


def _down_request_from_spec(
    spec: actions.ServeReplicaActionSpecV2,
) -> preflight.ProviderAuthorityPreflightRequestV2:
    down = spec.invocation.require_down()
    capsule = down.execution_config.capsule
    seed = preflight.ProviderDownPreflightSeedV2(
        version=2,
        resource_identity=spec.invocation.resource_identity,
        workspace=down.workspace,
        requested_target=spec.invocation.requested_target,
        prior_launch_basis=down.prior_launch_basis,
        prior_launch_basis_sha256=down.prior_launch_basis.sha256,
        cleanup_target=capsule.cleanup_target,
        cleanup_target_sha256=capsule.cleanup_target.sha256,
        context_mode='in_cluster',
        config_projection=capsule.config_projection)
    return preflight.ProviderAuthorityPreflightRequestV2.create(
        action_kind=kernel_actions.ActionKind.DOWN,
        nonce=_NONCE,
        seed=seed,
        expected_cohort_manifest=v2_fixtures._v2_authority_cohort().manifest)


def _down_request() -> preflight.ProviderAuthorityPreflightRequestV2:
    launch_spec = actions.serve_replica_action_spec_from_value_v2(
        v2_fixtures._v2_launch_spec())
    spec = actions.serve_replica_action_spec_from_value_v2(
        v2_fixtures._v2_down_spec(launch_spec))
    return _down_request_from_spec(spec)


def _candidate_max_partial_down(
) -> tuple[preflight.ProviderAuthorityPreflightRequestV2,
           actions.ServeReplicaActionSpecV2]:
    case = next(case for case in down_config_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'endpoint_resolved_exact_handle')
    basis, cleanup, _, _ = down_config_fixtures._partial_source_for_case(
        case, fixture_member='candidate_maximal')
    old = actions.ServeReplicaActionSpecV1.from_value(
        down_config_fixtures._down_spec_payload_for_basis(basis, cleanup))
    spec = actions.serve_replica_action_spec_from_value_v2(
        v2_fixtures._v2_down_from_v1(old))
    return _down_request_from_spec(spec), spec


def _worker() -> authority.ProviderAuthorityWorkerIdentityV2:
    cohort = v2_fixtures._v2_authority_cohort()
    manifest = cohort.manifest
    pod_uid = uuid.UUID('11111111-2222-4333-8444-555555555555')
    replica_set_name = f'{manifest.deployment_name}-{str(pod_uid)[:8]}'
    replica_set_uid = f'replicaset-{pod_uid}'
    return authority.ProviderAuthorityWorkerIdentityV2(
        version=2,
        namespace=manifest.namespace,
        pod_name=f'worker-{pod_uid}',
        pod_uid=pod_uid,
        pod_resource_version='101',
        pod_service_account_name=manifest.service_account_name,
        pod_controller_owner=actions.ProviderKubernetesControllerOwnerV1(
            api_version='apps/v1',
            kind='ReplicaSet',
            name=replica_set_name,
            uid=replica_set_uid),
        replica_set_name=replica_set_name,
        replica_set_uid=replica_set_uid,
        replica_set_resource_version='102',
        replica_set_controller_owner=actions.
        ProviderKubernetesControllerOwnerV1(api_version='apps/v1',
                                            kind='Deployment',
                                            name=manifest.deployment_name,
                                            uid=cohort.deployment_uid),
        deployment_name=manifest.deployment_name,
        deployment_uid=cohort.deployment_uid,
        deployment_generation=5,
        deployment_observed_generation=5,
        pod_template_contract_sha256=manifest.pod_template_contract.sha256,
        image=actions.ProviderAuthorityWorkerImageV1.from_value({
            'qualification': manifest.image.canonical_value(),
            'runtime': {
                'raw_image_id': 'containerd://sha256:' + '2' * 64,
                'runtime_image_id_scheme': 'containerd',
                'runtime_image_id_digest': 'sha256:' + '2' * 64,
                'qualified_oci_manifest_digest': 'sha256:' + '1' * 64,
                'qualified_oci_config_digest': 'sha256:' + '2' * 64,
                'qualification_artifact_sha256':
                    manifest.image.qualification_artifact.sha256,
                'runtime_id_contract': 'qualified_oci_config_digest_v1',
            },
        }),
        service_account_uid=cohort.service_account_uid,
        artifact_inventory_sha256=manifest.artifact_inventory.sha256,
        callable_inventory_sha256=manifest.callable_inventory.sha256,
        handler_allowlist_sha256=actions.canonical_sha256(
            list(manifest.handler_allowlist)),
        observed_at='2026-08-01T01:01:00.000000Z')


def _complete_response(
    request: preflight.ProviderAuthorityPreflightRequestV2,
    *,
    down_spec: actions.ServeReplicaActionSpecV2 | None = None,
) -> preflight.ProviderAuthorityPreflightResponseV2:
    cohort = v2_fixtures._v2_authority_cohort()
    if request.action_kind is kernel_actions.ActionKind.LAUNCH:
        spec = actions.serve_replica_action_spec_from_value_v2(
            v2_fixtures._v2_launch_spec())
        config = spec.invocation.require_launch().execution_config
        return preflight.ProviderLaunchAuthorityPreflightResponseV2(
            version=2,
            contract=_CONTRACT,
            action_kind=kernel_actions.ActionKind.LAUNCH,
            nonce=request.nonce,
            request_sha256=request.request_sha256,
            disposition=preflight.ProviderAuthorityPreflightDispositionV2.
            COMPLETE,
            reason=None,
            resolved_cohort=cohort,
            execution_capsule=config.capsule,
            executor_policy_proof=config.executor,
            worker_identity=_worker())
    if down_spec is None:
        launch_spec = actions.serve_replica_action_spec_from_value_v2(
            v2_fixtures._v2_launch_spec())
        down_spec = actions.serve_replica_action_spec_from_value_v2(
            v2_fixtures._v2_down_spec(launch_spec))
    config = down_spec.invocation.require_down().execution_config
    return preflight.ProviderDownAuthorityPreflightResponseV2(
        version=2,
        contract=_CONTRACT,
        action_kind=kernel_actions.ActionKind.DOWN,
        nonce=request.nonce,
        request_sha256=request.request_sha256,
        disposition=preflight.ProviderAuthorityPreflightDispositionV2.COMPLETE,
        reason=None,
        resolved_cohort=cohort,
        execution_capsule=config.capsule,
        executor_policy_proof=config.executor,
        worker_identity=_worker())


@pytest.mark.parametrize('request_factory,request_golden,response_golden', [
    (_launch_request,
     (13_228,
      '31860fb57c206fece8567c09bfe5061e2eb645b1bd66c8ebc23f4329695226c8'),
     (45_087,
      '133cbdf688665dbeace99221806cbc789c52675a417584e45097c48eb652c06b')),
    (_down_request,
     (28_641,
      'ce3ba423988fe6451cf3b7ac220e195ecc3cce17c1f5374431822b61a7bc460a'),
     (32_570,
      '92a03e3a924cf49dcabf4b8d827a100c9b82e446a78f71a49d51e5f8af36f3dd')),
])
def test_v2_request_and_complete_response_exact_round_trip_goldens(
        request_factory, request_golden, response_golden) -> None:
    request = request_factory()
    response = _complete_response(request)
    parsed_request = preflight.ProviderAuthorityPreflightRequestV2.from_value(
        request.canonical_value())
    parsed_response = (
        preflight.provider_authority_preflight_response_from_value_v2(
            response.canonical_value()))

    assert parsed_request.canonical_bytes == request.canonical_bytes
    assert parsed_response.canonical_bytes == response.canonical_bytes
    parsed_response.validate_request(parsed_request)
    assert (len(request.canonical_bytes), request.sha256) == request_golden
    assert (len(response.canonical_bytes), response.sha256) == response_golden
    assert len(request.canonical_bytes) <= 65_536
    assert len(response.canonical_bytes) <= 65_536


def test_candidate_max_partial_down_preflight_exact_size_hash_goldens() -> None:
    request, spec = _candidate_max_partial_down()
    response = _complete_response(request, down_spec=spec)

    response.validate_request(request)
    assert (len(request.canonical_bytes), request.sha256) == (
        32_156,
        '07bfd838a5a7fccb422a49904493e1107252a19c8ffb865260f7e8a6f624e87d')
    assert (len(response.canonical_bytes), response.sha256) == (
        38_624,
        'e7a2bf49367e0f6afabcf4ee162aa94b8a7aee0a8dc479466feb4dfbc772fdeb')
    assert len(request.canonical_bytes) <= 65_536
    assert len(response.canonical_bytes) <= 65_536


def test_v2_request_hash_is_exact_nonrecursive_preimage() -> None:
    request = _launch_request()
    assert request.request_sha256 == actions.canonical_sha256(
        request.preimage_value())
    crossed = copy.deepcopy(request.canonical_value())
    crossed['request_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='complete preimage'):
        preflight.ProviderAuthorityPreflightRequestV2.from_value(crossed)


def test_v2_contract_and_down_content_hashes_are_recomputed() -> None:
    request = _launch_request().canonical_value()
    request['contract'] = 'provider_kubernetes_preflight_v1'
    request['request_sha256'] = actions.canonical_sha256({
        key: value for key, value in request.items() if key != 'request_sha256'
    })
    with pytest.raises(ValueError, match='contract is unsupported'):
        preflight.ProviderAuthorityPreflightRequestV2.from_value(request)

    down = _down_request().canonical_value()
    down['seed']['prior_launch_basis_sha256'] = '0' * 64
    down['request_sha256'] = actions.canonical_sha256({
        key: value for key, value in down.items() if key != 'request_sha256'
    })
    with pytest.raises(ValueError, match='complete preimage'):
        preflight.ProviderAuthorityPreflightRequestV2.from_value(down)


def test_v2_down_seed_uses_the_shared_cleanup_binding_validator(
        monkeypatch: pytest.MonkeyPatch) -> None:
    seed = _down_request().seed
    calls: list[tuple[bytes, bytes]] = []
    validator = (
        cleanup_v2.validate_provider_kubernetes_cleanup_target_binding_v2)

    def _record_shared_validation(basis, cleanup_target) -> None:
        calls.append((basis.canonical_bytes, cleanup_target.canonical_bytes))
        validator(basis, cleanup_target)

    monkeypatch.setattr(
        cleanup_v2, 'validate_provider_kubernetes_cleanup_target_binding_v2',
        _record_shared_validation)

    parsed = preflight.ProviderDownPreflightSeedV2.from_value(
        seed.canonical_value())

    assert parsed.canonical_bytes == seed.canonical_bytes
    assert calls == [(seed.prior_launch_basis.canonical_bytes,
                      seed.cleanup_target.canonical_bytes)]


def test_v1_and_v2_envelopes_and_kind_parsers_are_disjoint() -> None:
    v1_launch = v1_preflight_fixtures._launch_request()
    v2_launch = _launch_request()
    with pytest.raises((TypeError, ValueError)):
        preflight.ProviderAuthorityPreflightRequestV2.from_value(
            v1_launch.canonical_value())
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderAuthorityPreflightRequestV1.from_value(
            v2_launch.canonical_value())
    with pytest.raises((TypeError, ValueError),
                       match='integer 2|wrong|unknown or missing'):
        preflight.provider_lifecycle_preflight_seed_from_value_v2(
            v2_launch.seed.canonical_value(), kernel_actions.ActionKind.DOWN)

    v1_response = (actions.ProviderLaunchAuthorityPreflightResponseV1.
                   unavailable(v1_launch))
    v2_response = (preflight.ProviderLaunchAuthorityPreflightResponseV2.
                   unavailable(v2_launch))
    with pytest.raises((TypeError, ValueError)):
        preflight.provider_authority_preflight_response_from_value_v2(
            v1_response.canonical_value())
    with pytest.raises((TypeError, ValueError)):
        actions.provider_authority_preflight_response_from_value_v1(
            v2_response.canonical_value())


@pytest.mark.parametrize('bad_version', [None, True, False, 1, 2.0, '2'])
def test_v2_parsers_reject_missing_bool_float_and_noninteger_versions(
        bad_version) -> None:
    request_value = _launch_request().canonical_value()
    if bad_version is None:
        del request_value['version']
    else:
        request_value['version'] = bad_version
    with pytest.raises((TypeError, ValueError)):
        preflight.ProviderAuthorityPreflightRequestV2.from_value(request_value)

    response_value = (
        preflight.ProviderLaunchAuthorityPreflightResponseV2.unavailable(
            _launch_request()).canonical_value())
    if bad_version is None:
        del response_value['version']
    else:
        response_value['version'] = bad_version
    with pytest.raises((TypeError, ValueError)):
        preflight.provider_authority_preflight_response_from_value_v2(
            response_value)


def test_v2_closed_objects_reject_extra_fields_nonce_and_response_hash_drift(
) -> None:
    request = _launch_request()
    extra = request.canonical_value()
    extra['extra'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        preflight.ProviderAuthorityPreflightRequestV2.from_value(extra)

    response = (preflight.ProviderLaunchAuthorityPreflightResponseV2.
                unavailable(request))
    crossed_nonce = response.canonical_value()
    crossed_nonce['nonce'] = '87654321-4321-4321-8321-cba987654321'
    parsed = preflight.provider_authority_preflight_response_from_value_v2(
        crossed_nonce)
    with pytest.raises(ValueError, match='request envelope'):
        parsed.validate_request(request)

    crossed_hash = response.canonical_value()
    crossed_hash['request_sha256'] = '0' * 64
    parsed = preflight.provider_authority_preflight_response_from_value_v2(
        crossed_hash)
    with pytest.raises(ValueError, match='request envelope'):
        parsed.validate_request(request)


def test_complete_and_not_representable_evidence_sets_are_exclusive() -> None:
    request = _launch_request()
    complete = _complete_response(request).canonical_value()
    for field in ('resolved_cohort', 'execution_capsule',
                  'executor_policy_proof', 'worker_identity'):
        partial = copy.deepcopy(complete)
        partial[field] = None
        with pytest.raises(TypeError, match='all four'):
            preflight.provider_authority_preflight_response_from_value_v2(
                partial)

    complete_with_reason = copy.deepcopy(complete)
    complete_with_reason['reason'] = 'preflight_unavailable_or_invalid'
    with pytest.raises(ValueError, match='reason must be null'):
        preflight.provider_authority_preflight_response_from_value_v2(
            complete_with_reason)

    unavailable = (preflight.ProviderLaunchAuthorityPreflightResponseV2.
                   unavailable(request).canonical_value())
    unavailable['resolved_cohort'] = complete['resolved_cohort']
    with pytest.raises(ValueError, match='entirely null'):
        preflight.provider_authority_preflight_response_from_value_v2(
            unavailable)

    unavailable = (preflight.ProviderLaunchAuthorityPreflightResponseV2.
                   unavailable(request).canonical_value())
    unavailable['reason'] = 'cleanup_target_mismatch'
    with pytest.raises(ValueError, match='unsupported'):
        preflight.provider_authority_preflight_response_from_value_v2(
            unavailable)

    crossed_capsule = copy.deepcopy(complete)
    down_response = _complete_response(_down_request())
    assert down_response.execution_capsule is not None
    crossed_capsule[
        'execution_capsule'] = down_response.execution_capsule.canonical_value(
        )
    with pytest.raises((TypeError, ValueError)):
        preflight.provider_authority_preflight_response_from_value_v2(
            crossed_capsule)


def test_complete_binding_rejects_manifest_capsule_worker_and_proof_drift(
) -> None:
    request = _launch_request()
    response = _complete_response(request)

    crossed_manifest = request.canonical_value()
    crossed_manifest['expected_cohort_manifest']['artifact_inventory'][
        'sha256'] = '0' * 64
    crossed_manifest['request_sha256'] = actions.canonical_sha256({
        key: value
        for key, value in crossed_manifest.items()
        if key != 'request_sha256'
    })
    crossed_request = preflight.ProviderAuthorityPreflightRequestV2.from_value(
        crossed_manifest)
    crossed_response_value = response.canonical_value()
    crossed_response_value['request_sha256'] = crossed_request.request_sha256
    crossed_response = (
        preflight.provider_authority_preflight_response_from_value_v2(
            crossed_response_value))
    with pytest.raises(ValueError, match='manifest differs'):
        crossed_response.validate_request(crossed_request)

    crossed_capsule = response.canonical_value()
    crossed_capsule['execution_capsule']['executor_cohort'][
        'cohort_identity_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='parsed locked V2 worker cohort'):
        preflight.provider_authority_preflight_response_from_value_v2(
            crossed_capsule)

    crossed_worker = response.canonical_value()
    crossed_worker['worker_identity']['service_account_uid'] = 'other-uid'
    with pytest.raises(ValueError, match='does not match its cohort'):
        preflight.provider_authority_preflight_response_from_value_v2(
            crossed_worker)

    crossed_proof = response.canonical_value()
    crossed_proof['executor_policy_proof']['boundary'] = (
        'serve_controller_prepare')
    with pytest.raises(ValueError, match='does not bind'):
        preflight.provider_authority_preflight_response_from_value_v2(
            crossed_proof)


def test_request_and_response_outer_canonical_limits_reject_65537_bytes(
) -> None:
    request = _launch_request().canonical_value()
    request['seed']['workspace'] = 'w' * 65_537
    assert len(actions.canonical_json_bytes(request)) > 65_536
    with pytest.raises(ValueError, match='65536 bytes'):
        preflight.ProviderAuthorityPreflightRequestV2.from_value(request)

    response = (
        preflight.ProviderLaunchAuthorityPreflightResponseV2.unavailable(
            _launch_request()).canonical_value())
    response['reason'] = 'x' * 65_537
    assert len(actions.canonical_json_bytes(response)) > 65_536
    with pytest.raises(ValueError, match='65536 bytes'):
        preflight.provider_authority_preflight_response_from_value_v2(response)
