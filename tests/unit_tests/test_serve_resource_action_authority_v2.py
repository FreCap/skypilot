"""Closed Serve039/040 authority policy contract tests."""

from collections.abc import Callable
import dataclasses
import itertools
import uuid

import pytest

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_qualification_policy as policy_loader
from sky.serve import resource_actions

_ZERO = '0' * 64
_ONE = '1' * 64
_TWO = '2' * 64


def _cohort(marker: str) -> authority.ApprovedAuthorityCohortArtifactV1:
    return authority.ApprovedAuthorityCohortArtifactV1(
        cohort_id=f'qualified-v2-{marker}',
        oci_manifest_digest='sha256:' + marker * 64,
        oci_config_digest='sha256:' + _TWO,
        manifest_sha256=_ZERO,
        qualification_artifact_sha256=_ONE,
        pod_template_contract_sha256=_TWO,
        pod_template_binding_sha256=_ZERO,
        artifact_inventory_sha256=_ONE,
        callable_inventory_sha256=_TWO,
        handler_allowlist_sha256=_ZERO,
        claim_contract='frozen_action_cohort_join_v2')


def _deployment_set(marker: str) -> authority.ApprovedAuthorityDeploymentSetV1:
    role_images = tuple(
        authority.ApprovedRoleImageV1(role=role,
                                      oci_manifest_digest='sha256:' +
                                      marker * 64,
                                      source_commit=marker * 40,
                                      artifact_inventory_sha256=marker * 64)
        for role in (authority.ApprovedRole.API,
                     authority.ApprovedRole.ORDINARY_EXECUTOR,
                     authority.ApprovedRole.CONTROLLER))
    return authority.ApprovedAuthorityDeploymentSetV1(
        version=1, role_images=role_images, approved_cohorts=(_cohort(marker),))


def _policy(
    set_count: int = 1,
    *,
    serve_head: str = '039',
) -> authority.ResourceActionQualificationPolicyV2:
    markers = ('a', 'b')[:set_count]
    bindings = tuple(
        sorted((authority.ApprovedAuthorityDeploymentSetBindingV1.
                for_deployment_set(_deployment_set(marker))
                for marker in markers),
               key=lambda item: item.deployment_set_sha256))
    set_hashes = tuple(item.deployment_set_sha256 for item in bindings)
    compatibility = (authority.ResourceActionDeploymentCompatibilityInventoryV1.
                     for_deployment_set_hashes(set_hashes))
    elected = set_hashes[-1]
    rollback = set_hashes[0]
    return authority.ResourceActionQualificationPolicyV2(
        version=2,
        api_requests_head='008',
        serve_head=serve_head,
        global_user_state_head='028',
        candidate_minimum_seconds=86_400,
        minimum_clean_launches=100,
        minimum_clean_downs=100,
        approved_deployment_sets=bindings,
        elected_deployment_set_sha256=elected,
        rollback_deployment_set_sha256=rollback,
        deployment_compatibility_inventory=compatibility,
        deployment_compatibility_inventory_sha256=compatibility.sha256,
        crash_canary_inventory_contract=
        'resource_action_crash_canary_inventory_v1',
        required_crash_canary_inventory_sha256=(
            authority.ResourceActionRequiredCrashCanaryInventoryV1.required(
            ).sha256))


def _deployment(
    role: authority.ApprovedRole,
    index: int,
    image: authority.ApprovedRoleImageV1,
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
                                'image': 'registry.example/sky@' +
                                         image.oci_manifest_digest,
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
        oci_manifest_digest=image.oci_manifest_digest,
        source_commit=image.source_commit,
        artifact_inventory_sha256=image.artifact_inventory_sha256)


def _deployment_inventory(
    deployment_set: authority.ApprovedAuthorityDeploymentSetV1,
) -> authority.ResourceActionDeploymentInventoryV1:
    return authority.ResourceActionDeploymentInventoryV1(
        version=1,
        contract='resource_action_deployment_inventory_v1',
        deployments=tuple(
            _deployment(image.role, index, image)
            for index, image in enumerate(deployment_set.role_images, start=1)))


def _candidate_binding(
    policy: authority.ResourceActionQualificationPolicyV2 | None = None,
) -> authority.ResourceActionCandidateBindingV2:
    if policy is None:
        policy = _policy()
    selection = policy.deployment_compatibility_inventory.selections[0]
    ordinary_set = policy.deployment_set_by_hash(
        selection.api_deployment_set_sha256)
    deployments = _deployment_inventory(ordinary_set)
    cohort_set = policy.deployment_set_by_hash(
        selection.authority_cohort_deployment_set_sha256)
    selected_cohort = cohort_set.approved_cohorts[0]
    crash_inventory = (
        authority.ResourceActionRequiredCrashCanaryInventoryV1.required())
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
    binding = authority.ResourceActionCandidateBindingV2(
        version=2,
        qualification_policy_sha256=policy.sha256,
        schema_heads=policy.schema_heads,
        deployment_inventory=deployments,
        deployment_inventory_sha256=deployments.sha256,
        deployment_selection=selection,
        deployment_selection_sha256=selection.sha256,
        selected_cohort=selected_cohort,
        selected_cohort_sha256=selected_cohort.sha256,
        capacity_profile=profile,
        capacity_profile_sha256=profile.sha256,
        elected_version_identity=elected_identity,
        elected_version_identity_sha256=elected_identity.sha256,
        live_replica_identity_inventory=authority.HashedCanonicalObjectV1.
        from_object({
            'contract': 'serve_live_replica_identity_inventory_v1',
            'replicas': [],
            'version': 1,
        }),
        required_crash_canary_inventory=crash_inventory,
        required_crash_canary_inventory_sha256=crash_inventory.sha256)
    binding.validate_for_policy(policy)
    return binding


def _v1_policy() -> authority.ResourceActionQualificationPolicyV1:
    deployment_set = _deployment_set('a')
    return authority.ResourceActionQualificationPolicyV1(
        version=1,
        api_requests_head='007',
        serve_head='035',
        global_user_state_head='028',
        candidate_minimum_seconds=86_400,
        minimum_clean_launches=100,
        minimum_clean_downs=100,
        approved_role_images=deployment_set.role_images,
        approved_cohorts=deployment_set.approved_cohorts,
        crash_canary_inventory_contract=
        'resource_action_crash_canary_inventory_v1',
        required_crash_canary_inventory_sha256=(
            authority.ResourceActionRequiredCrashCanaryInventoryV1.required(
            ).sha256))


@pytest.mark.parametrize('serve_head', ('039', '040'))
@pytest.mark.parametrize('set_count,selection_count', ((1, 1), (2, 16)))
def test_v2_policy_exact_round_trip(serve_head: str, set_count: int,
                                    selection_count: int) -> None:
    policy = _policy(set_count, serve_head=serve_head)

    assert policy.schema_heads == authority.AuthoritySchemaHeadsV2(
        api_requests_head='008',
        serve_head=serve_head,
        global_user_state_head='028')
    assert len(
        policy.deployment_compatibility_inventory.selections) == selection_count
    assert authority.ResourceActionQualificationPolicyV2.from_value(
        policy.canonical_value()).canonical_bytes == policy.canonical_bytes
    assert len(policy.canonical_bytes) <= 65_536
    assert policy.deployment_set_by_hash(
        policy.elected_deployment_set_sha256).sha256 == (
            policy.elected_deployment_set_sha256)


def test_two_set_inventory_is_exact_sorted_cartesian_product() -> None:
    policy = _policy(2)
    set_hashes = tuple(
        item.deployment_set_sha256 for item in policy.approved_deployment_sets)
    expected = tuple(itertools.product(set_hashes, repeat=4))

    assert tuple(
        item.sort_key for item in
        policy.deployment_compatibility_inventory.selections) == expected


def test_every_two_set_selection_resolves_roles_and_cohort_independently(
) -> None:
    policy = _policy(2)
    for selection in policy.deployment_compatibility_inventory.selections:
        deployments = []
        for index, role in enumerate((authority.ApprovedRole.API,
                                      authority.ApprovedRole.ORDINARY_EXECUTOR,
                                      authority.ApprovedRole.CONTROLLER),
                                     start=1):
            deployment_set = policy.deployment_set_by_hash(
                selection.deployment_set_sha256_for_role(role))
            image = next(item for item in deployment_set.role_images
                         if item.role is role)
            deployments.append(_deployment(role, index, image))
        inventory = authority.ResourceActionDeploymentInventoryV1(
            version=1,
            contract='resource_action_deployment_inventory_v1',
            deployments=tuple(deployments))
        policy.validate_deployment_inventory(selection, inventory)
        cohort_set = policy.deployment_set_by_hash(
            selection.authority_cohort_deployment_set_sha256)
        policy.validate_selected_cohort(selection,
                                        cohort_set.approved_cohorts[0])


def test_v1_and_v2_policy_and_head_codecs_cross_reject() -> None:
    v2 = _policy()
    v1_heads = authority.AuthoritySchemaHeadsV1(api_requests_head='007',
                                                serve_head='035',
                                                global_user_state_head='028')

    with pytest.raises(ValueError):
        authority.AuthoritySchemaHeadsV2.from_value(v1_heads.canonical_value())
    with pytest.raises(ValueError):
        authority.AuthoritySchemaHeadsV1.from_value(
            v2.schema_heads.canonical_value())
    with pytest.raises(ValueError):
        authority.ResourceActionQualificationPolicyV1.from_value(
            v2.canonical_value())


@pytest.mark.parametrize('field,value', (
    ('version', True),
    ('version', 1),
    ('api_requests_head', '007'),
    ('serve_head', '035'),
    ('serve_head', '038'),
    ('serve_head', '041'),
    ('global_user_state_head', '027'),
    ('candidate_minimum_seconds', 86_399),
    ('minimum_clean_launches', 99),
    ('minimum_clean_downs', 101),
    ('crash_canary_inventory_contract', 'other'),
))
def test_v2_policy_fixed_fields_reject(field: str, value: object) -> None:
    policy_value = _policy().canonical_value()
    policy_value[field] = value
    with pytest.raises(ValueError):
        authority.ResourceActionQualificationPolicyV2.from_value(policy_value)


def test_v2_policy_rejects_unknown_missing_and_nonlist_inventories() -> None:
    value = _policy().canonical_value()
    with pytest.raises(ValueError):
        authority.ResourceActionQualificationPolicyV2.from_value({
            **value, 'unknown': None
        })
    missing = dict(value)
    del missing['approved_deployment_sets']
    with pytest.raises(ValueError):
        authority.ResourceActionQualificationPolicyV2.from_value(missing)
    with pytest.raises(ValueError):
        authority.ResourceActionQualificationPolicyV2.from_value({
            **value, 'approved_deployment_sets': tuple(
                value['approved_deployment_sets'])
        })


def test_deployment_set_binding_recomputes_digest() -> None:
    deployment_set = _deployment_set('a')
    binding = (authority.ApprovedAuthorityDeploymentSetBindingV1.
               for_deployment_set(deployment_set))
    assert binding.deployment_set_sha256 == deployment_set.sha256
    with pytest.raises(ValueError):
        dataclasses.replace(binding, deployment_set_sha256=_ZERO)


def test_deployment_set_rejects_wrong_role_and_cohort_order() -> None:
    deployment_set = _deployment_set('a')
    with pytest.raises(ValueError):
        dataclasses.replace(deployment_set,
                            role_images=tuple(
                                reversed(deployment_set.role_images)))
    second = _cohort('b')
    with pytest.raises(ValueError):
        dataclasses.replace(
            deployment_set,
            approved_cohorts=(second, deployment_set.approved_cohorts[0]))


def test_policy_rejects_set_cardinality_order_and_selection_shape() -> None:
    one = _policy()
    two = _policy(2)
    with pytest.raises(ValueError):
        dataclasses.replace(one, approved_deployment_sets=())
    with pytest.raises(ValueError):
        dataclasses.replace(two,
                            approved_deployment_sets=tuple(
                                reversed(two.approved_deployment_sets)))
    with pytest.raises(ValueError):
        dataclasses.replace(
            two,
            elected_deployment_set_sha256=two.rollback_deployment_set_sha256)
    with pytest.raises(ValueError):
        dataclasses.replace(one, rollback_deployment_set_sha256=_ZERO)


def test_policy_rejects_crossed_or_incomplete_compatibility_inventory() -> None:
    policy = _policy(2)
    crossed = (authority.ResourceActionDeploymentCompatibilityInventoryV1.
               for_deployment_set_hashes((_ZERO, _ONE)))
    with pytest.raises(ValueError):
        dataclasses.replace(
            policy,
            deployment_compatibility_inventory=crossed,
            deployment_compatibility_inventory_sha256=crossed.sha256)
    with pytest.raises(ValueError):
        dataclasses.replace(policy,
                            deployment_compatibility_inventory_sha256=_ZERO)


def test_compatibility_inventory_rejects_duplicate_or_unsorted_values() -> None:
    inventory = _policy(2).deployment_compatibility_inventory
    with pytest.raises(ValueError):
        dataclasses.replace(inventory,
                            selections=(inventory.selections[0],) * 16)
    with pytest.raises(ValueError):
        dataclasses.replace(inventory,
                            selections=tuple(reversed(inventory.selections)))


def test_v2_candidate_binding_round_trips_and_validates_policy() -> None:
    policy = _policy()
    binding = _candidate_binding(policy)

    parsed = authority.ResourceActionCandidateBindingV2.from_value(
        binding.canonical_value())
    parsed.validate_for_policy(policy)
    assert parsed.canonical_bytes == binding.canonical_bytes
    assert len(parsed.canonical_bytes) <= 65_536


def test_v2_candidate_binding_rejects_nested_digest_drift() -> None:
    binding = _candidate_binding()
    for field in ('deployment_inventory_sha256', 'deployment_selection_sha256',
                  'selected_cohort_sha256', 'capacity_profile_sha256',
                  'elected_version_identity_sha256',
                  'required_crash_canary_inventory_sha256'):
        with pytest.raises(ValueError):
            dataclasses.replace(binding,
                                **{field: _ZERO})  # type: ignore[arg-type]


def test_v2_candidate_binding_rejects_crossed_policy_selection_and_cohort(
) -> None:
    policy = _policy()
    binding = _candidate_binding(policy)
    other_set = (
        authority.ApprovedAuthorityDeploymentSetBindingV1.for_deployment_set(
            _deployment_set('b')))
    other_selection = authority.ApprovedAuthorityDeploymentSelectionV1(
        api_deployment_set_sha256=other_set.deployment_set_sha256,
        ordinary_executor_deployment_set_sha256=other_set.deployment_set_sha256,
        controller_deployment_set_sha256=other_set.deployment_set_sha256,
        authority_cohort_deployment_set_sha256=other_set.deployment_set_sha256)
    crossed_selection = dataclasses.replace(
        binding,
        deployment_selection=other_selection,
        deployment_selection_sha256=other_selection.sha256)
    with pytest.raises(ValueError, match='not approved'):
        crossed_selection.validate_for_policy(policy)

    crossed_cohort_value = _cohort('b')
    crossed_cohort = dataclasses.replace(
        binding,
        selected_cohort=crossed_cohort_value,
        selected_cohort_sha256=crossed_cohort_value.sha256)
    with pytest.raises(ValueError, match='selected cohort'):
        crossed_cohort.validate_for_policy(policy)


def test_v2_candidate_binding_rejects_policy_head_and_artifact_drift() -> None:
    policy = _policy()
    binding = _candidate_binding(policy)
    with pytest.raises(ValueError, match='policy digest'):
        dataclasses.replace(
            binding,
            qualification_policy_sha256=_ZERO).validate_for_policy(policy)
    head_040 = authority.AuthoritySchemaHeadsV2(api_requests_head='008',
                                                serve_head='040',
                                                global_user_state_head='028')
    with pytest.raises(ValueError, match='schema heads'):
        dataclasses.replace(binding,
                            schema_heads=head_040).validate_for_policy(policy)
    deployment = binding.deployment_inventory.deployments[0]
    drifted_inventory = dataclasses.replace(
        binding.deployment_inventory,
        deployments=(dataclasses.replace(deployment,
                                         artifact_inventory_sha256=_ZERO),) +
        binding.deployment_inventory.deployments[1:])
    drifted_binding = dataclasses.replace(
        binding,
        deployment_inventory=drifted_inventory,
        deployment_inventory_sha256=drifted_inventory.sha256)
    with pytest.raises(ValueError, match='not approved'):
        drifted_binding.validate_for_policy(policy)


def test_v1_candidate_codec_rejects_v2_candidate() -> None:
    value = _candidate_binding().canonical_value()
    with pytest.raises((TypeError, ValueError)):
        authority.ResourceActionCandidateBindingV1.from_value(value)


def test_v2_fixed_path_loader_round_trips_exact_bytes(
        monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _policy(2)
    monkeypatch.setattr(policy_loader, '_read_policy_bytes',
                        lambda: policy.canonical_bytes)

    loaded = policy_loader.load_resource_action_qualification_policy_v2()
    assert loaded.policy.canonical_bytes == policy.canonical_bytes
    assert loaded.reference == (
        authority.ResourceActionQualificationPolicyRefV1.for_policy_v2(policy))


def test_v1_and_v2_fixed_path_policy_loaders_cross_reject() -> None:
    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader._parse_policy_bytes_v2(_v1_policy().canonical_bytes)
    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader._parse_policy_bytes(_policy().canonical_bytes)


@pytest.mark.parametrize(
    'mutator',
    (
        lambda raw: raw + b'\n',
        lambda raw: raw.replace(b'"version":2', b'"version":2.0', 1),
        lambda raw: raw.replace(b'{', b'{"version":2,', 1),
        lambda raw: b'\xff' + raw,
    ),
)
def test_v2_loader_rejects_noncanonical_duplicate_or_noninteger_bytes(
        mutator: Callable[[bytes], bytes]) -> None:
    raw = _policy().canonical_bytes
    with pytest.raises(
            policy_loader.ResourceActionQualificationPolicyUnavailable):
        policy_loader._parse_policy_bytes_v2(mutator(raw))
