"""Focused native-V2 renderer value and construction tests."""

# pylint: disable=protected-access

import ast
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import serve_resource_action_test_fixtures as authority_fixtures
import test_serve_resource_action_cleanup_v2 as cleanup_fixtures
import test_serve_resource_action_down_execution_config as down_fixtures
import test_serve_resource_action_launch_execution_config as launch_fixtures

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_cleanup_v2 as cleanup_v2
from sky.serve import resource_action_provider_artifacts as provider_artifacts
from sky.serve import resource_action_renderer as renderer_v1
from sky.serve import resource_action_renderer_v2 as renderer
from sky.serve import resource_actions as actions

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ARTIFACT_ROOT = (_REPO_ROOT / 'sky' / 'serve' / 'resource_action_artifacts' /
                  'kubernetes_renderer_v1')
_V2_ARTIFACT_ROOT = (_REPO_ROOT / 'sky' / 'serve' /
                     'resource_action_artifacts' / 'kubernetes_renderer_v2')


def _artifact_ref(path: str, raw_bytes: bytes) -> dict[str, Any]:
    return {
        'repo_path': path,
        'byte_size': len(raw_bytes),
        'sha256': hashlib.sha256(raw_bytes).hexdigest(),
    }


def _canonical_artifact(value: dict[str, Any]) -> bytes:
    return actions.canonical_json_bytes(value) + b'\n'


def _cohort() -> authority.ProviderAuthorityWorkerCohortV2:
    manifest_value = authority_fixtures.authority_manifest_value()
    manifest_value['version'] = 2
    manifest_value['claim_contract'] = 'frozen_action_cohort_join_v2'
    manifest = authority.ProviderAuthorityWorkerCohortManifestV2.from_value(
        manifest_value)
    return authority.ProviderAuthorityWorkerCohortV2(
        version=2,
        manifest=manifest,
        manifest_sha256=manifest.sha256,
        deployment_uid='deployment-uid-v2',
        # The launch fixture's scope and principal deliberately retain this
        # independently copied UID.
        service_account_uid='service-account-uid-v1')


def _cohort_reference() -> actions.ProviderAuthorityWorkerCohortReferenceV1:
    cohort = _cohort()
    return actions.ProviderAuthorityWorkerCohortReferenceV1(
        version=1,
        cohort_id=cohort.cohort_id,
        cohort_identity_sha256=cohort.sha256)


def _v2_binding_value() -> dict[str, Any]:
    value = json.loads(
        (_ARTIFACT_ROOT / 'binding_schema.json').read_text(encoding='utf-8'))
    value['input_contract'] = 'validated_launch_spec_v2'
    value['schema'] = 'skypilot.serve.prebooted-direct-pod.bindings.v2'
    return value


def _config_inventory_value() -> dict[str, Any]:
    value = json.loads(
        (_V2_ARTIFACT_ROOT /
         'config_access_inventory.json').read_text(encoding='utf-8'))
    assert type(value) is dict
    return value


def _resolved_artifacts(
) -> renderer.ResolvedProviderKubernetesRendererArtifactSetV2:
    outer_path = (_ARTIFACT_ROOT / 'outer_template.json')
    node_path = (_ARTIFACT_ROOT / 'node_fragment.json')
    normalization_path = (_ARTIFACT_ROOT / 'admitted_object_normalization.json')
    outer_bytes = outer_path.read_bytes()
    node_bytes = node_path.read_bytes()
    normalization_bytes = normalization_path.read_bytes()
    binding_bytes = _canonical_artifact(_v2_binding_value())
    inventory_bytes = _canonical_artifact(_config_inventory_value())

    outer_ref = actions.ProviderRepoArtifactRefV1.from_value(
        _artifact_ref(
            ('sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
             'outer_template.json'), outer_bytes))
    node_ref = actions.ProviderRepoArtifactRefV1.from_value(
        _artifact_ref(
            ('sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
             'node_fragment.json'), node_bytes))
    binding_ref = actions.ProviderRepoArtifactRefV1.from_value(
        _artifact_ref(
            ('sky/serve/resource_action_artifacts/kubernetes_renderer_v2/'
             'binding_schema.json'), binding_bytes))
    inventory_ref = actions.ProviderRepoArtifactRefV1.from_value(
        _artifact_ref(
            ('sky/serve/resource_action_artifacts/kubernetes_renderer_v2/'
             'config_access_inventory.json'), inventory_bytes))
    normalization_ref = actions.ProviderRepoArtifactRefV1.from_value(
        _artifact_ref(
            ('sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
             'admitted_object_normalization.json'), normalization_bytes))

    outer_raw = (provider_artifacts.RawCanonicalRendererArtifactBytesV1.
                 from_verified_bytes(outer_ref, outer_bytes))
    node_raw = (provider_artifacts.RawCanonicalRendererArtifactBytesV1.
                from_verified_bytes(node_ref, node_bytes))
    binding_raw = (provider_artifacts.RawCanonicalRendererArtifactBytesV1.
                   from_verified_bytes(binding_ref, binding_bytes))
    inventory_raw = (provider_artifacts.RawCanonicalRendererArtifactBytesV1.
                     from_verified_bytes(inventory_ref, inventory_bytes))
    return renderer.ResolvedProviderKubernetesRendererArtifactSetV2(
        outer_template=(
            renderer_v1.ResolvedProviderKubernetesOuterTemplateArtifactV1(
                artifact_ref=outer_ref,
                raw_artifact=outer_raw,
                template=renderer_v1.ProviderKubernetesOuterTemplateArtifactV1.
                from_value(outer_raw.canonical_value()))),
        node_fragment=(
            renderer_v1.ResolvedProviderKubernetesNodeFragmentArtifactV1(
                artifact_ref=node_ref,
                raw_artifact=node_raw,
                fragment=renderer_v1.ProviderKubernetesNodeFragmentArtifactV1.
                from_value(node_raw.canonical_value()))),
        binding_schema=(
            renderer.ResolvedProviderKubernetesBindingSchemaArtifactV2(
                artifact_ref=binding_ref,
                raw_artifact=binding_raw,
                schema=renderer.ProviderKubernetesBindingSchemaArtifactV2(
                    binding_raw.canonical_value()))),
        config_access_inventory=(
            renderer.ResolvedProviderKubernetesConfigAccessInventoryArtifactV2(
                artifact_ref=inventory_ref,
                raw_artifact=inventory_raw,
                inventory=renderer.ProviderKubernetesConfigAccessInventoryV2(
                    inventory_raw.canonical_value()))),
        admitted_object_normalization=(
            provider_artifacts.ResolvedProviderKubernetesNormalizationArtifactV1
            .from_verified_bytes(normalization_ref, normalization_bytes)))


def _renderer_input_raw(*,
                        use_v1_renderer_artifacts: bool = False
                       ) -> dict[str, Any]:
    capsule = launch_fixtures._capsule_raw()
    expected_objects = capsule.pop('objects')
    capsule['version'] = 2
    capsule['executor_cohort'] = _cohort_reference().canonical_value()
    if not use_v1_renderer_artifacts:
        artifacts = _resolved_artifacts()
        renderer_refs = (
            artifacts.outer_template.artifact_ref,
            artifacts.node_fragment.artifact_ref,
            artifacts.binding_schema.artifact_ref,
            artifacts.config_access_inventory.artifact_ref,
            artifacts.admitted_object_normalization.artifact_ref,
        )
        for field, reference in zip(
            ('outer_template', 'node_fragment', 'binding_schema',
             'config_access_inventory', 'admitted_object_normalization'),
                renderer_refs):
            capsule['renderer'][field] = reference.canonical_value()
        capsule['config_projection']['config_access_inventory'] = (
            artifacts.config_access_inventory.artifact_ref.canonical_value())
        capsule['config_projection_sha256'] = actions.canonical_sha256(
            capsule['config_projection'])
    target = launch_fixtures._target()
    return {
        'version': 2,
        'contract': 'validated_launch_spec_v2',
        'resource_identity': launch_fixtures._resource_identity(),
        'sky_cluster_name': target['sky_cluster_name'],
        'sky_cluster_record_uuid': target['sky_cluster_record_uuid'],
        'name_basis': target['kubernetes']['name_basis'],
        'seed': capsule,
        'retained_source': launch_fixtures._content_source(),
        '_expected_objects': expected_objects,
    }


def _renderer_input(
    *,
    use_v1_renderer_artifacts: bool = False
) -> renderer.ProviderKubernetesRendererInputV2:
    value = _renderer_input_raw(
        use_v1_renderer_artifacts=use_v1_renderer_artifacts)
    value.pop('_expected_objects')
    return renderer.ProviderKubernetesRendererInputV2.from_value(value)


def _down_input(
) -> tuple[renderer.ProviderKubernetesDownExecutionCapsuleInputV2,
           cleanup_v2.ProviderKubernetesCompletedCleanupRederivationInputV2,
           actions.ProviderKubernetesCleanupTargetV1]:
    basis = actions.CompletedLaunchBasisV1.from_value(
        down_fixtures.completed_basis_payload())
    old = down_fixtures._down_capsule(basis)
    value = old.canonical_value()
    cleanup_target = old.cleanup_target
    value['version'] = 2
    value['executor_cohort'] = _cohort_reference().canonical_value()
    del value['cleanup_target']
    del value['cleanup_target_sha256']
    cleanup_rederivation_input, expected_target = (
        cleanup_fixtures._completed_input())
    assert cleanup_target.canonical_bytes == expected_target.canonical_bytes
    return (renderer.ProviderKubernetesDownExecutionCapsuleInputV2.from_value(
        value), cleanup_rederivation_input, expected_target)


def test_native_v2_values_are_closed_round_trippable_and_disjoint() -> None:
    value = _renderer_input_raw()
    expected_objects = value.pop('_expected_objects')
    renderer_input = renderer.ProviderKubernetesRendererInputV2.from_value(
        value)
    down_input, _, _ = _down_input()

    assert renderer_input.canonical_value() == value
    assert renderer.ProviderKubernetesRendererInputV2.from_value(
        renderer_input.canonical_value()) == renderer_input
    assert renderer.ProviderKubernetesDownExecutionCapsuleInputV2.from_value(
        down_input.canonical_value()) == down_input
    assert set(renderer_input.seed.canonical_value()) == (
        set(actions.ProviderKubernetesExecutionCapsuleV2._KEYS) - {'objects'})
    assert set(down_input.canonical_value()) == (
        set(actions.ProviderKubernetesDownExecutionCapsuleV2._KEYS) -
        {'cleanup_target', 'cleanup_target_sha256'})
    assert expected_objects

    unknown = copy.deepcopy(value)
    unknown['seed']['objects'] = expected_objects
    with pytest.raises(ValueError, match='unknown or missing'):
        renderer.ProviderKubernetesRendererInputV2.from_value(unknown)
    crossed = copy.deepcopy(value)
    crossed['version'] = 1
    with pytest.raises(ValueError, match='integer 2'):
        renderer.ProviderKubernetesRendererInputV2.from_value(crossed)


def test_native_v2_launch_root_renders_and_contextually_revalidates(
        monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _renderer_input_raw()
    expected_objects = raw.pop('_expected_objects')
    renderer_input = renderer.ProviderKubernetesRendererInputV2.from_value(raw)
    artifacts = _resolved_artifacts()
    monkeypatch.setattr(renderer, '_read_renderer_artifacts_v2',
                        lambda unused: artifacts)

    capsule = renderer.construct_provider_kubernetes_execution_capsule_v2(
        renderer_input, _cohort())

    assert type(capsule) is actions.ProviderKubernetesExecutionCapsuleV2
    assert [item.canonical_value() for item in capsule.objects
           ] == expected_objects
    assert capsule.executor_cohort == _cohort_reference()
    assert renderer.validate_provider_kubernetes_execution_capsule_context_v2(
        capsule, _cohort()) is capsule


def test_native_v2_down_root_uses_only_rederived_target() -> None:
    down_input, cleanup_rederivation_input, expected_target = _down_input()
    rederived_target = (
        cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(
            cleanup_rederivation_input))

    capsule = renderer.construct_provider_kubernetes_down_execution_capsule_v2(
        down_input, _cohort(), cleanup_rederivation_input)

    assert type(capsule) is actions.ProviderKubernetesDownExecutionCapsuleV2
    assert capsule.cleanup_target.canonical_bytes == expected_target.canonical_bytes
    assert capsule.cleanup_target_sha256 == expected_target.sha256
    assert (
        renderer.validate_provider_kubernetes_down_execution_capsule_context_v2(
            capsule, _cohort(), rederived_target) is capsule)


def test_native_v2_down_root_accepts_partial_rederivation_input() -> None:
    down_input, _, _ = _down_input()
    case = next(case for case in down_fixtures._PARTIAL_DOWN_CASES
                if case.case_id == 'objects_partial_1_not_found')
    cleanup_rederivation_input, expected_target = (
        cleanup_fixtures._partial_input(case))

    capsule = renderer.construct_provider_kubernetes_down_execution_capsule_v2(
        down_input, _cohort(), cleanup_rederivation_input)

    assert capsule.cleanup_target.canonical_bytes == expected_target.canonical_bytes
    assert capsule.cleanup_target_sha256 == expected_target.sha256


def test_native_v2_down_root_rejects_direct_cleanup_target_injection() -> None:
    down_input, _, expected_target = _down_input()
    target_value = expected_target.canonical_value()
    target_value['observed_at'] = '2026-01-02T03:04:05.000000Z'
    injected_target = actions.ProviderKubernetesCleanupTargetV1.from_value(
        target_value)

    with pytest.raises(TypeError,
                       match='rederivation input has an invalid type'):
        renderer.construct_provider_kubernetes_down_execution_capsule_v2(
            down_input, _cohort(), injected_target)  # type: ignore[arg-type]


def test_external_cohort_and_cleanup_context_drift_fail_closed(
        monkeypatch: pytest.MonkeyPatch) -> None:
    renderer_input = _renderer_input()
    artifacts = _resolved_artifacts()
    monkeypatch.setattr(renderer, '_read_renderer_artifacts_v2',
                        lambda unused: artifacts)
    crossed_reference = renderer_input.seed.executor_cohort.canonical_value()
    crossed_reference['cohort_identity_sha256'] = '0' * 64
    crossed_seed = renderer_input.seed.canonical_value()
    crossed_seed['executor_cohort'] = crossed_reference
    crossed_value = renderer_input.canonical_value()
    crossed_value['seed'] = crossed_seed
    crossed = renderer.ProviderKubernetesRendererInputV2.from_value(
        crossed_value)
    with pytest.raises(ValueError, match='parsed locked V2 worker cohort'):
        renderer.construct_provider_kubernetes_execution_capsule_v2(
            crossed, _cohort())

    down_input, cleanup_rederivation_input, cleanup_target = _down_input()
    capsule = renderer.construct_provider_kubernetes_down_execution_capsule_v2(
        down_input, _cohort(), cleanup_rederivation_input)
    target_value = cleanup_target.canonical_value()
    target_value['observed_at'] = '2026-01-02T03:04:05.000000Z'
    other_target = actions.ProviderKubernetesCleanupTargetV1.from_value(
        target_value)
    with pytest.raises(ValueError, match='byte-equal'):
        renderer.validate_provider_kubernetes_down_execution_capsule_context_v2(
            capsule, _cohort(), other_target)


@pytest.mark.parametrize('crossing', [
    'resource_service_incarnation',
    'cluster_record_uuid',
    'replica_incarnation',
    'retained_source',
    'scope',
    'principal',
])
def test_launch_external_identity_and_context_crossings_fail_closed(
        crossing: str) -> None:
    raw = _renderer_input_raw()
    raw.pop('_expected_objects')
    other_uuid = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'
    if crossing == 'resource_service_incarnation':
        raw['resource_identity']['service_incarnation'] = other_uuid
        raw['resource_identity']['service_hash'] = other_uuid
    elif crossing == 'cluster_record_uuid':
        raw['sky_cluster_record_uuid'] = other_uuid
    elif crossing == 'replica_incarnation':
        raw['resource_identity']['replica_incarnation'] = other_uuid
    elif crossing == 'retained_source':
        raw['retained_source']['workspace'] = 'crossed-workspace'
    elif crossing == 'scope':
        raw['seed']['scope']['namespace'] = 'crossed-namespace'
    else:
        assert crossing == 'principal'
        raw['seed']['principals']['workload']['uid'] = 'crossed-workload-uid'

    with pytest.raises(ValueError):
        renderer_input = renderer.ProviderKubernetesRendererInputV2.from_value(
            raw)
        renderer.validate_provider_kubernetes_renderer_input_v2(
            renderer_input, _cohort())


def test_installed_v1_binding_and_inventory_paths_fail_before_rendering(
        monkeypatch: pytest.MonkeyPatch) -> None:
    renderer_input = _renderer_input(use_v1_renderer_artifacts=True)

    def _must_not_render(*unused: Any) -> None:
        raise AssertionError('V1 evidence reached the V2 renderer')

    monkeypatch.setattr(renderer, '_render_provider_kubernetes_objects_v2',
                        _must_not_render)
    with pytest.raises(ValueError, match='binding_schema path is not exact'):
        renderer.construct_provider_kubernetes_execution_capsule_v2(
            renderer_input, _cohort())


def test_production_v2_source_never_calls_v1_capsule_or_renderer_roots(
) -> None:
    module_path = (_REPO_ROOT / 'sky' / 'serve' /
                   'resource_action_renderer_v2.py')
    module = ast.parse(module_path.read_text(encoding='utf-8'))
    forbidden = {
        'ProviderKubernetesExecutionCapsuleSeedV1',
        'ProviderKubernetesRendererInputV1',
        'ProviderKubernetesExecutionCapsuleV1',
        'ProviderKubernetesDownExecutionCapsuleV1',
        'ProviderKubernetesExecutionConfigV1',
        'ProviderKubernetesDownExecutionConfigV1',
        'ProviderLaunchInvocationV1',
        'ProviderDownInvocationV1',
        'ProviderLifecycleInvocationV1',
        'ProviderLaunchLifecycleInvocationV1',
        'ProviderDownLifecycleInvocationV1',
        'ProviderLifecyclePlanV1',
        'ServeReplicaActionSpecV1',
        'serve_replica_action_spec_from_value_v1',
        'construct_provider_kubernetes_execution_capsule_v1',
        'validate_provider_kubernetes_renderer_input_v1',
        'resolve_provider_kubernetes_renderer_artifacts_v1',
    }
    invoked = set()
    for node in ast.walk(module):
        if type(node) is not ast.Call:
            continue
        function = node.func
        if type(function) is ast.Name:
            invoked.add(function.id)
        elif type(function) is ast.Attribute:
            invoked.add(function.attr)
    assert invoked.isdisjoint(forbidden)
