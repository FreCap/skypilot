"""Closed contracts and loader tests for API007 qualification trust."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
import uuid

import pytest

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_qualification_policy as policy_loader
from sky.serve import resource_actions

_ZERO = '0' * 64
_ONE = '1' * 64
_TWO = '2' * 64
_ARTIFACT_PATH = (Path(authority.__file__).parent /
                  'resource_action_artifacts/provider_authority_v1/'
                  'crash_canary_inventory.json')
_CRASH_INVENTORY_SHA256 = (
    '045ac5f7078fd3856f69a1c297f16b5b1b3e5cd73e11f91de4eea6171c2abad7')
_TEST_POLICY_SHA256 = (
    '4efe9c6f82a548a6e0db0b27b288f313cb3fef98396ee3addd359ba31277f71f')
_TEST_API_POD_TEMPLATE_SHA256 = (
    'f318e712440e8160708d0860f5886e26bc3e5e32f602be22819e89d2332f695e')
_TEST_CANDIDATE_BINDING_SHA256 = (
    'bfe73d5a975110408ed6dda58fe21d3858847b36b09caa085f7cfc40342ba036')


class _TextSubclass(str):
    pass


class _IntSubclass(int):
    pass


def _cohort() -> authority.ApprovedAuthorityCohortArtifactV1:
    return authority.ApprovedAuthorityCohortArtifactV1(
        cohort_id='qualified-v2',
        oci_manifest_digest='sha256:' + _ONE,
        oci_config_digest='sha256:' + _TWO,
        manifest_sha256=_ZERO,
        qualification_artifact_sha256=_ONE,
        pod_template_contract_sha256=_TWO,
        pod_template_binding_sha256=_ZERO,
        artifact_inventory_sha256=_ONE,
        callable_inventory_sha256=_TWO,
        handler_allowlist_sha256=_ZERO,
        claim_contract='frozen_action_cohort_join_v2')


def _crash_inventory(
) -> authority.ResourceActionRequiredCrashCanaryInventoryV1:
    return authority.ResourceActionRequiredCrashCanaryInventoryV1.required()


def _policy() -> authority.ResourceActionQualificationPolicyV1:
    role_images = tuple(
        authority.ApprovedRoleImageV1(role=role,
                                      oci_manifest_digest='sha256:' + _ONE,
                                      source_commit='a' * 40,
                                      artifact_inventory_sha256=_TWO)
        for role in (authority.ApprovedRole.API,
                     authority.ApprovedRole.ORDINARY_EXECUTOR,
                     authority.ApprovedRole.CONTROLLER))
    return authority.ResourceActionQualificationPolicyV1(
        version=1,
        api_requests_head='007',
        serve_head='035',
        global_user_state_head='028',
        candidate_minimum_seconds=86_400,
        minimum_clean_launches=100,
        minimum_clean_downs=100,
        approved_role_images=role_images,
        approved_cohorts=(_cohort(),),
        crash_canary_inventory_contract=
        'resource_action_crash_canary_inventory_v1',
        required_crash_canary_inventory_sha256=_crash_inventory().sha256)


def _deployment(
    role: authority.ApprovedRole,
    index: int,
) -> authority.QualifiedResourceActionRoleDeploymentV1:
    pod_template = (authority.QualifiedResourceActionRolePodTemplateV1.
                    from_deployment_template_value({
                        'metadata': {
                            'annotations': None,
                            'creationTimestamp': None,
                            'labels': {
                                'app': f'skypilot-role-{index}',
                            },
                        },
                        'spec': {
                            'automountServiceAccountToken': True,
                            'containers': [{
                                'env': [],
                                'image': 'registry.example/sky@sha256:' + _ONE,
                                'name': role.value,
                            }],
                            'restartPolicy': 'Always',
                        },
                    }))
    return authority.QualifiedResourceActionRoleDeploymentV1(
        version=1,
        role=role,
        namespace='skypilot',
        deployment_name=f'skypilot-role-{index}',
        deployment_uid=f'deployment-uid-{index}',
        generation=7,
        observed_generation=7,
        desired_replicas=2,
        updated_replicas=2,
        ready_replicas=2,
        available_replicas=2,
        unavailable_replicas=0,
        pod_template=pod_template,
        pod_template_sha256=pod_template.sha256,
        oci_manifest_digest='sha256:' + _ONE,
        source_commit='a' * 40,
        artifact_inventory_sha256=_TWO)


def _deployment_inventory() -> authority.ResourceActionDeploymentInventoryV1:
    return authority.ResourceActionDeploymentInventoryV1(
        version=1,
        contract='resource_action_deployment_inventory_v1',
        deployments=tuple(
            _deployment(role, index) for index, role in enumerate((
                authority.ApprovedRole.API,
                authority.ApprovedRole.ORDINARY_EXECUTOR,
                authority.ApprovedRole.CONTROLLER,
            ),
                                                                  start=1)))


def _candidate_binding(
    live_replica_identity_inventory: authority.HashedCanonicalObjectV1 |
    None = None,
) -> authority.ResourceActionCandidateBindingV1:
    policy = _policy()
    deployments = _deployment_inventory()
    crash_inventory = _crash_inventory()
    profile = (resource_actions.ServeActionCapacityProfileV1.
               ordinary_ondemand_physical_width1())
    elected_identity = resource_actions.ServeServiceVersionSpecIdentityV1(
        version=1,
        service_name='qualification-service',
        service_incarnation=uuid.UUID('00000000-0000-4000-8000-000000000001'),
        service_version=1,
        effective_service_config_sha256=_ZERO,
        effective_task_config_sha256=_ONE,
        capacity_profile=profile,
        provider_profile='pod_cluster_v1')
    binding = authority.ResourceActionCandidateBindingV1(
        version=1,
        qualification_policy_sha256=policy.sha256,
        schema_heads=authority.AuthoritySchemaHeadsV1(
            api_requests_head='007',
            serve_head='035',
            global_user_state_head='028'),
        deployment_inventory=deployments,
        deployment_inventory_sha256=deployments.sha256,
        selected_cohort=policy.approved_cohorts[0],
        selected_cohort_sha256=policy.approved_cohorts[0].sha256,
        capacity_profile=profile,
        capacity_profile_sha256=profile.sha256,
        elected_version_identity=elected_identity,
        elected_version_identity_sha256=elected_identity.sha256,
        live_replica_identity_inventory=(
            authority.HashedCanonicalObjectV1.from_object({
                'contract': 'serve_live_replica_identity_inventory_v1',
                'replicas': [],
                'version': 1,
            }) if live_replica_identity_inventory is None else
            live_replica_identity_inventory),
        required_crash_canary_inventory=crash_inventory,
        required_crash_canary_inventory_sha256=crash_inventory.sha256)
    binding.validate_for_policy(policy)
    return binding


def test_required_crash_inventory_matches_exact_checked_in_golden() -> None:
    inventory = _crash_inventory()
    assert len(inventory.requirements) == 20
    assert inventory.sha256 == _CRASH_INVENTORY_SHA256
    # Repository JSON artifacts retain exactly one LF; the policy and binding
    # hash the parsed contract's canonical bytes, not presentation whitespace.
    assert _ARTIFACT_PATH.read_bytes() == inventory.canonical_bytes + b'\n'
    parsed = authority.ResourceActionRequiredCrashCanaryInventoryV1.from_value(
        json.loads(inventory.canonical_bytes))
    assert parsed.canonical_bytes == inventory.canonical_bytes
    manifest = (Path(authority.__file__).parents[1] / 'setup_files/MANIFEST.in')
    assert ('recursive-include sky/serve/resource_action_artifacts/'
            'provider_authority_v1 *.json') in manifest.read_text(
                encoding='utf-8')


def test_crash_and_deployment_inventories_reject_partial_or_drifted_values(
) -> None:
    required = _crash_inventory()
    with pytest.raises(ValueError, match='20-boundary'):
        authority.ResourceActionRequiredCrashCanaryInventoryV1(
            version=1,
            contract='resource_action_crash_canary_inventory_v1',
            requirements=required.requirements[:-1])
    with pytest.raises(ValueError, match='20-boundary'):
        authority.ResourceActionRequiredCrashCanaryInventoryV1(
            version=1,
            contract='resource_action_crash_canary_inventory_v1',
            requirements=tuple(reversed(required.requirements)))
    deployment = _deployment(authority.ApprovedRole.API, 1)
    with pytest.raises(ValueError, match='fully observed'):
        dataclasses.replace(deployment, observed_generation=6)
    with pytest.raises(ValueError, match='fully updated'):
        dataclasses.replace(deployment, ready_replicas=1)
    with pytest.raises(ValueError, match='Pod-template digest'):
        dataclasses.replace(deployment, pod_template_sha256=_ZERO)
    with pytest.raises(ValueError, match='deployment inventory contract'):
        dataclasses.replace(
            _deployment_inventory(),
            contract=_TextSubclass('resource_action_deployment_inventory_v1'))
    with pytest.raises(ValueError, match='crash inventory contract'):
        dataclasses.replace(
            required,
            contract=_TextSubclass('resource_action_crash_canary_inventory_v1'))


def test_qualification_trust_contracts_reject_scalar_subclasses() -> None:
    policy = _policy()
    with pytest.raises(ValueError, match='schema heads are not exact'):
        dataclasses.replace(policy, api_requests_head=_TextSubclass('007'))
    with pytest.raises(ValueError, match='crash inventory contract'):
        dataclasses.replace(policy,
                            crash_canary_inventory_contract=_TextSubclass(
                                'resource_action_crash_canary_inventory_v1'))
    with pytest.raises(ValueError, match='claim contract'):
        dataclasses.replace(
            _cohort(),
            claim_contract=_TextSubclass('frozen_action_cohort_join_v2'))
    with pytest.raises(ValueError, match='policy path is not exact'):
        dataclasses.replace(
            authority.ResourceActionQualificationPolicyRefV1.for_policy(policy),
            path=_TextSubclass(
                authority.RESOURCE_ACTION_QUALIFICATION_POLICY_PATH_V1))
    with pytest.raises(ValueError, match='schema heads are not exact'):
        authority.AuthoritySchemaHeadsV1(api_requests_head=_TextSubclass('007'),
                                         serve_head='035',
                                         global_user_state_head='028')


def test_ordinary_role_pod_template_has_one_exact_immutable_projection(
) -> None:
    projection = _deployment(authority.ApprovedRole.API, 1).pod_template
    assert projection.sha256 == _TEST_API_POD_TEMPLATE_SHA256
    value = projection.template_value
    assert value['metadata'] == {
        'annotations': {},
        'labels': {
            'app': 'skypilot-role-1',
        },
    }
    assert authority.QualifiedResourceActionRolePodTemplateV1.from_value(
        projection.canonical_value(
        )).canonical_bytes == projection.canonical_bytes

    reordered = {
        'spec': value['spec'],
        'metadata': {
            'labels': value['metadata']['labels'],
            'creationTimestamp': None,
        },
    }
    assert (authority.QualifiedResourceActionRolePodTemplateV1.
            from_deployment_template_value(
                reordered).canonical_bytes == projection.canonical_bytes)

    drifted = projection.template_value
    drifted['metadata']['name'] = 'caller-supplied-name'
    with pytest.raises(ValueError, match='metadata has unknown'):
        (authority.QualifiedResourceActionRolePodTemplateV1.
         from_deployment_template_value(drifted))

    controller_label = projection.template_value
    controller_label['metadata']['labels']['pod-template-hash'] = 'mutable'
    with pytest.raises(ValueError, match='controller-owned'):
        (authority.QualifiedResourceActionRolePodTemplateV1.
         from_deployment_template_value(controller_label))

    with pytest.raises(ValueError, match='exact canonical JSON'):
        dataclasses.replace(projection,
                            template_json='\n' + projection.template_json)
    with pytest.raises(TypeError, match='canonical JSON text'):
        dataclasses.replace(projection,
                            template_json=_TextSubclass(
                                projection.template_json))
    with pytest.raises(ValueError, match='contract is unsupported'):
        dataclasses.replace(
            projection,
            contract=_TextSubclass(
                'qualified_resource_action_role_pod_template_v1'))

    subclass_value = projection.template_value
    subclass_value['spec']['containers'][0]['image'] = _TextSubclass(
        subclass_value['spec']['containers'][0]['image'])
    with pytest.raises(TypeError, match='subclass value'):
        (authority.QualifiedResourceActionRolePodTemplateV1.
         from_deployment_template_value(subclass_value))

    out_of_range_integer = projection.template_value
    out_of_range_integer['spec']['terminationGracePeriodSeconds'] = 2**63
    with pytest.raises(ValueError, match='outside signed-int64'):
        (authority.QualifiedResourceActionRolePodTemplateV1.
         from_deployment_template_value(out_of_range_integer))


def test_candidate_binding_is_closed_and_recomputes_every_nested_hash() -> None:
    binding = _candidate_binding()
    assert binding.sha256 == _TEST_CANDIDATE_BINDING_SHA256
    parsed = authority.ResourceActionCandidateBindingV1.from_value(
        binding.canonical_value())
    assert parsed.canonical_bytes == binding.canonical_bytes
    assert parsed.sha256 == binding.sha256

    with pytest.raises(ValueError, match='deployment_inventory digest'):
        dataclasses.replace(binding, deployment_inventory_sha256=_ZERO)
    with pytest.raises(ValueError, match='policy digest'):
        dataclasses.replace(
            binding,
            qualification_policy_sha256=_ZERO).validate_for_policy(_policy())


def test_candidate_binding_snapshots_and_revalidates_delegated_inventory(
) -> None:
    delegated = authority.HashedCanonicalObjectV1.from_object({
        'contract': 'serve_live_replica_identity_inventory_v1',
        'replicas': [],
        'version': 1,
    })
    binding = _candidate_binding(delegated)
    canonical_before = binding.canonical_bytes

    delegated.value['replicas'].append({'replica_id': 1})
    assert binding.canonical_bytes == canonical_before

    binding.live_replica_identity_inventory.value['replicas'].append(
        {'replica_id': 2})
    with pytest.raises(ValueError, match='mutated after validation'):
        binding.validate_for_policy(_policy())
    with pytest.raises(ValueError, match='mutated after validation'):
        _ = binding.canonical_bytes

    rehashed_binding = _candidate_binding()
    delegated_snapshot = rehashed_binding.live_replica_identity_inventory
    delegated_snapshot.value['replicas'].append({'replica_id': 3})
    object.__setattr__(delegated_snapshot, 'value_sha256',
                       authority.canonical_sha256(delegated_snapshot.value))
    with pytest.raises(ValueError, match='construction snapshot'):
        rehashed_binding.validate_for_policy(_policy())


def test_hashed_delegated_object_uses_strict_recursive_json_validation(
) -> None:
    with pytest.raises(TypeError, match='subclass value'):
        authority.HashedCanonicalObjectV1.from_object(
            {'nested': {
                'text': _TextSubclass('value'),
            }})
    with pytest.raises(TypeError, match='subclass value'):
        authority.HashedCanonicalObjectV1.from_object(
            {'nested': {
                'count': _IntSubclass(1),
            }})

    shared: list[object] = []
    with pytest.raises(ValueError, match='shared container'):
        authority.HashedCanonicalObjectV1.from_object({
            'first': shared,
            'second': shared,
        })

    cycle: list[object] = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match='cycle'):
        authority.HashedCanonicalObjectV1.from_object({'cycle': cycle})

    too_deep: dict[str, object] = {}
    cursor = too_deep
    for index in range(32):
        child: dict[str, object] = {}
        cursor[str(index)] = child
        cursor = child
    with pytest.raises(ValueError, match='depth bound'):
        authority.HashedCanonicalObjectV1.from_object(too_deep)

    with pytest.raises(ValueError, match='member bound'):
        authority.HashedCanonicalObjectV1.from_object(
            {'members': list(range(8_192))})


def test_loader_returns_only_exact_fixed_path_canonical_policy(
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    policy = _policy()
    path = tmp_path / 'qualification-policy.json'
    path.write_bytes(policy.canonical_bytes)
    monkeypatch.setattr(policy_loader, '_QUALIFICATION_POLICY_PATH', str(path))

    loaded = policy_loader.load_resource_action_qualification_policy_v1()
    assert loaded.policy.canonical_bytes == policy.canonical_bytes
    assert loaded.policy.sha256 == _TEST_POLICY_SHA256
    assert loaded.reference.canonical_value() == {
        'path': '/etc/skypilot/resource-actions/qualification-policy.json',
        'byte_size': len(policy.canonical_bytes),
        'sha256': policy.sha256,
    }


@pytest.mark.parametrize('mutator', [
    lambda raw: raw + b'\n',
    lambda raw: raw.replace(b'"version":1', b'"version":1.0', 1),
    lambda raw: raw.replace(b'{', b'{"version":1,', 1),
    lambda raw: raw.replace(b'{', b'{"unknown":1,', 1),
    lambda raw: b'\xff' + raw,
])
def test_loader_rejects_noncanonical_duplicate_or_invalid_bytes(
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mutator) -> None:
    path = tmp_path / 'qualification-policy.json'
    path.write_bytes(mutator(_policy().canonical_bytes))
    monkeypatch.setattr(policy_loader, '_QUALIFICATION_POLICY_PATH', str(path))

    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader.load_resource_action_qualification_policy_v1()


def test_loader_fails_closed_for_missing_symlink_and_oversized_file(
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing = tmp_path / 'missing.json'
    monkeypatch.setattr(policy_loader, '_QUALIFICATION_POLICY_PATH',
                        str(missing))
    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader.load_resource_action_qualification_policy_v1()

    target = tmp_path / 'target.json'
    target.write_bytes(_policy().canonical_bytes)
    symlink = tmp_path / 'policy-link.json'
    symlink.symlink_to(target)
    monkeypatch.setattr(policy_loader, '_QUALIFICATION_POLICY_PATH',
                        str(symlink))
    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader.load_resource_action_qualification_policy_v1()

    real_directory = tmp_path / 'real-directory'
    real_directory.mkdir()
    (real_directory / 'qualification-policy.json').write_bytes(
        _policy().canonical_bytes)
    linked_directory = tmp_path / 'linked-directory'
    linked_directory.symlink_to(real_directory, target_is_directory=True)
    monkeypatch.setattr(policy_loader, '_QUALIFICATION_POLICY_PATH',
                        str(linked_directory / 'qualification-policy.json'))
    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader.load_resource_action_qualification_policy_v1()

    oversized = tmp_path / 'oversized.json'
    oversized.write_bytes(b'x' * 65_537)
    monkeypatch.setattr(policy_loader, '_QUALIFICATION_POLICY_PATH',
                        str(oversized))
    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader.load_resource_action_qualification_policy_v1()
