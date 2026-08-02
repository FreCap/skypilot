"""Launch source, policy-free capsule, and frozen config composition tests."""

# pylint: disable=protected-access

import copy
import dataclasses
import hashlib

import pytest

from sky.serve import resource_actions as actions
from sky.utils import common_utils
from tests.unit_tests import test_serve_resource_action_capsule_leaves as leaves
from tests.unit_tests import (
    test_serve_resource_action_execution_foundation as foundation)
from tests.unit_tests import (
    test_serve_resource_action_kubernetes_scope as scope_fixtures)
from tests.unit_tests import (
    test_serve_resource_action_launch_identity as identity_fixtures)
from tests.unit_tests import (
    test_serve_resource_action_provider_plan as plan_fixtures)
from tests.unit_tests import (
    test_serve_resource_action_provider_values as value_fixtures)
from tests.unit_tests import (
    test_serve_resource_action_runtime_endpoint as runtime_fixtures)

_CLUSTER_UUID = '33333333-3333-4333-8333-333333333333'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'


def _content_source(*, workspace: str = 'workspace-a') -> dict:
    return {
        'store': 'serve_version_specs',
        'service_name': 'svc',
        'service_incarnation': '11111111-1111-4111-8111-111111111111',
        'service_version': 3,
        'yaml_content_sha256': 'b' * 64,
        'workspace': workspace,
    }


def _resource_identity(generation: int = 3) -> dict:
    value = identity_fixtures._resource_identity()
    value['desired_generation'] = generation
    return value


def _identity_proof(
    resource_identity: dict | None = None,
) -> actions.ProviderLaunchIdentityCanonicalizationProofV1:
    resource_identity = (_resource_identity() if resource_identity is None else
                         copy.deepcopy(resource_identity))
    input_value = identity_fixtures._input()
    input_value['resource_identity'] = resource_identity
    typed_input = (actions.ProviderLaunchIdentityCanonicalizationInputV1.
                   from_value(input_value))
    context_value = identity_fixtures._context()
    context_value['decision_id'] = str(
        typed_input.resource_identity.action_identity('launch').action_id)
    context_value['input'] = typed_input.canonical_value()
    context_value['input_sha256'] = typed_input.sha256
    typed_context = (actions.ProviderLaunchIdentityCanonicalizationContextV1.
                     from_value(context_value))
    raw = identity_fixtures._proof()
    raw['context'] = typed_context.canonical_value()
    raw['context_sha256'] = typed_context.sha256
    raw['effective_user_hash'] = 'user-hash'
    return actions.ProviderLaunchIdentityCanonicalizationProofV1.from_value(raw)


def _source(
    resource_identity: dict | None = None,
    *,
    workspace: str = 'workspace-a',
) -> actions.ProviderLaunchSourceV1:
    return actions.project_provider_launch_source_v1(
        actions.ProviderLaunchContentSourceV1.from_value(
            _content_source(workspace=workspace)),
        _identity_proof(resource_identity))


def _request_identity() -> actions.ProviderKubernetesRequestIdentityV1:
    proof = _identity_proof()
    name_basis = actions.ProviderWorkloadNameBasisV1.from_value(_name_basis())
    return actions.project_provider_kubernetes_request_identity_v1(
        proof.effective_original_user, name_basis)


def _name_basis() -> dict:
    return {
        'version': 1,
        'display_name': 'svc-7',
        'frozen_user_hash': 'user-hash',
        'max_length': 42,
        'cluster_name_hash_length': 8,
    }


def _topology() -> dict:
    raw = foundation._topology()
    basis = actions.ProviderWorkloadNameBasisV1.from_value(_name_basis())
    names = (f'{basis.workload_name}-ssh', basis.workload_name,
             basis.workload_name)
    for item, name in zip(raw['mutable_objects'], names):
        item['name'] = name
        labels = {label['key']: label['value'] for label in item['labels']}
        labels['skypilot-cluster-name'] = basis.provider_cluster_name
        labels['skypilot.co/cluster-record-uuid'] = _CLUSTER_UUID
        labels['skypilot.co/serve-replica-incarnation'] = _REPLICA_UUID
        labels['skypilot-user'] = 'effectiveexamplecom'
        if item['role'] == 'head_pod':
            labels['component'] = name
        item['labels'] = [{
            'key': key,
            'value': value
        } for key, value in sorted(labels.items())]
    return raw


def _set_prerequisite_projection(prerequisite: dict, projection: dict) -> None:
    prerequisite['namespace'] = projection['namespace']
    prerequisite['name'] = projection['name']
    prerequisite['uid'] = projection['uid']
    prerequisite['resource_version'] = projection['resource_version']
    prerequisite['spec']['projection'] = copy.deepcopy(projection)
    prerequisite['spec_sha256'] = actions.canonical_sha256(prerequisite['spec'])


def _canonical_prerequisites(principals: dict, cohort: dict) -> list[dict]:
    prerequisites = plan_fixtures._prerequisite_inventory()
    by_role = {item['role']: item for item in prerequisites}
    authority_namespace = by_role['authority_release_namespace']
    authority_namespace['name'] = cohort['manifest']['namespace']
    for role in ('serve_lb_slot_0_namespace', 'serve_lb_slot_1_namespace'):
        alias = by_role[role]
        alias.update({
            key: copy.deepcopy(value)
            for key, value in authority_namespace.items()
            if key != 'role'
        })
    _set_prerequisite_projection(by_role['caller_service_account'],
                                 principals['caller'])
    _set_prerequisite_projection(by_role['workload_service_account'],
                                 principals['workload'])
    for role in ('serve_lb_slot_0_service_account',
                 'serve_lb_slot_1_service_account'):
        item = by_role[role]
        item['namespace'] = authority_namespace['name']
        item['spec']['projection']['namespace'] = authority_namespace['name']
        item['spec_sha256'] = actions.canonical_sha256(item['spec'])
    return prerequisites


def _canonical_cohort() -> dict:

    def artifact(path: str, digest_character: str) -> dict:
        return {
            'repo_path': path,
            'byte_size': 17,
            'sha256': digest_character * 64,
        }

    manifest = {
        'version': 1,
        'cohort_id': 'authority-v1',
        'namespace': 'skypilot-system',
        'deployment_name': 'skypilot-authority-v1',
        'service_account_name': 'authority-worker',
        'container_name': 'skypilot-authority-worker',
        'image': {
            'requested_reference':
                ('registry.example/authority@sha256:' + '1' * 64),
            'oci_manifest_digest': 'sha256:' + '1' * 64,
            'oci_config_digest': 'sha256:' + '2' * 64,
            'qualification_artifact': artifact('images/authority.json', '3'),
        },
        'pod_template_contract': artifact('charts/worker.yaml', '4'),
        'artifact_inventory': artifact('inventories/artifacts.json', '5'),
        'callable_inventory': artifact('inventories/callables.json', '6'),
        'claim_contract': 'frozen_action_cohort_join_v1',
        'handler_allowlist': list(
            actions.PROVIDER_AUTHORITY_WORKER_HANDLER_ALLOWLIST_V1),
    }
    return {
        'version': 1,
        'manifest': manifest,
        'manifest_sha256': actions.canonical_sha256(manifest),
        'deployment_uid': 'deployment-uid-v1',
        'service_account_uid': 'uid-authority-worker',
    }


def _canonical_object_plan(role: str, topology_item: dict,
                           request_identity: dict, resources: dict) -> dict:
    item = plan_fixtures._object_plan(role)
    item['namespace'] = 'serve-canary'
    item['name'] = topology_item['name']
    topology_labels = {
        label['key']: label['value'] for label in topology_item['labels']
    }
    item['required_identity_labels'] = [{
        'key': key,
        'value': topology_labels[key]
    } for key in (
        'skypilot-cluster-name',
        'skypilot.co/cluster-record-uuid',
        'skypilot.co/serve-replica-incarnation',
    )]
    metadata = {
        'namespace': 'serve-canary',
        'name': topology_item['name'],
        'labels': topology_labels,
    }
    if role == 'head_pod':
        metadata['annotations'] = {
            'skypilot-user': request_identity['original_user']
        }
        spec = {
            'serviceAccountName': 'serve-workload',
            'automountServiceAccountToken': False,
            'containers': [{
                'name': 'ray-node',
                'image': resources['image']['qualification']
                         ['requested_reference'],
                'imagePullPolicy': resources['image_pull_policy'],
                'resources': {
                    'requests': {
                        'cpu': resources['pod_cpu_request'],
                        'memory': resources['pod_memory_request'],
                    },
                    'limits': {
                        'cpu': resources['pod_cpu_limit'],
                        'memory': resources['pod_memory_limit'],
                    },
                },
                'env': [{
                    'name': 'SKYPILOT_SERVE_REPLICA_ID',
                    'value': '7',
                }],
                'ports': [{
                    'containerPort': port
                } for port in (10001, 10002, 10003, 10004, 46590)],
            }],
        }
    elif role == 'head_ssh_service':
        spec = {
            'ports': [{
                'protocol': 'TCP',
                'port': 22,
                'targetPort': 22,
            }]
        }
    else:
        spec = {'clusterIP': 'None'}
    request_body = {
        'apiVersion': 'v1',
        'kind': topology_item['kind'],
        'metadata': metadata,
        'spec': spec,
    }
    requested_semantic = copy.deepcopy(request_body)
    requested_semantic['admissionDefaults'] = {'explicit': True}
    item['request_body'] = request_body
    item['request_body_sha256'] = actions.canonical_sha256(request_body)
    item['requested_semantic'] = requested_semantic
    item['requested_semantic_sha256'] = actions.canonical_sha256(
        requested_semantic)
    return item


def _capsule_raw(*, workspace: str = 'workspace-a') -> dict:
    content = _content_source(workspace=workspace)
    request_identity = _request_identity().canonical_value()
    resources = value_fixtures._resources()
    resources['image']['qualification']['requested_reference'] = (
        'registry.example/runtime@sha256:' + '1' * 64)
    topology = _topology()
    renderer = plan_fixtures._renderer()
    renderer['source'] = copy.deepcopy(content)
    config_projection = foundation._config_projection()
    config_projection['workspace'] = workspace
    config_projection['config_access_inventory'] = copy.deepcopy(
        renderer['config_access_inventory'])
    post_provision = runtime_fixtures._post_provision()
    post_provision['job_submission']['run_source'] = copy.deepcopy(content)
    objects = [
        _canonical_object_plan(role, topology_item, request_identity, resources)
        for role, topology_item in zip((
            'head_ssh_service', 'head_service',
            'head_pod'), topology['mutable_objects'])
    ]
    for item in objects:
        item['normalization_profile'] = copy.deepcopy(
            renderer['admitted_object_normalization'])
    cohort = _canonical_cohort()
    principals = foundation._principals()
    prerequisites = _canonical_prerequisites(principals, cohort)
    endpoint = runtime_fixtures._endpoint()
    by_role = {item['role']: item for item in prerequisites}
    endpoint_roles = ('endpoint_network_policy', 'serve_lb_slot_0_namespace',
                      'serve_lb_slot_0_service_account',
                      'serve_lb_slot_1_namespace',
                      'serve_lb_slot_1_service_account')
    endpoint['prerequisite_projection'] = [
        copy.deepcopy(by_role[role]) for role in endpoint_roles
    ]
    for caller, namespace_role, service_account_role in zip(
            endpoint['required_callers'],
        ('serve_lb_slot_0_namespace', 'serve_lb_slot_1_namespace'),
        ('serve_lb_slot_0_service_account', 'serve_lb_slot_1_service_account')):
        namespace = by_role[namespace_role]
        service_account = by_role[service_account_role]
        caller['namespace'] = namespace['name']
        caller['namespace_uid'] = namespace['uid']
        caller['service_account_name'] = service_account['name']
        caller['service_account_uid'] = service_account['uid']
        caller['workload']['namespace'] = namespace['name']
        caller['workload']['service_account_name'] = service_account['name']
    for artifact in post_provision['runtime_artifacts']:
        artifact['workload_image_digest'] = resources['image']['qualification'][
            'oci_manifest_digest']
    return {
        'version': 1,
        'implementation_contract': 'kubernetes_serve_prebooted_runtime_v1',
        'executor_cohort': cohort,
        'config_projection': config_projection,
        'config_projection_sha256': actions.canonical_sha256(config_projection),
        'scope': scope_fixtures._scope(),
        'principals': principals,
        'prerequisites': prerequisites,
        'request_identity': request_identity,
        'resources': resources,
        'renderer': renderer,
        'objects': objects,
        'post_provision': post_provision,
        'endpoint': endpoint,
        'scheduling': leaves._scheduling(),
        'storage': leaves._storage(),
        'metadata': leaves._metadata(),
        'security': leaves._security(),
        'topology': topology,
        'mutation_contract': leaves._launch_mutation(),
    }


def _capsule(
    *,
    workspace: str = 'workspace-a'
) -> actions.ProviderKubernetesExecutionCapsuleV1:
    return actions.ProviderKubernetesExecutionCapsuleV1.from_value(
        _capsule_raw(workspace=workspace))


def _target() -> dict:
    scope = scope_fixtures._scope()
    scope_sha256 = actions.ProviderKubernetesScopeV1.from_value(scope).sha256
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'cloud': 'kubernetes',
        'region': None,
        'zone': None,
        'sky_cluster_name': 'svc-7',
        'sky_cluster_record_uuid': '33333333-3333-4333-8333-333333333333',
        'kubernetes': {
            'scope': scope,
            'cluster_fingerprint_sha256': scope_sha256,
            'namespace': 'serve-canary',
            'name_basis': _name_basis(),
            'provider_cluster_name': 'svc-7-user-hash',
            'workload_kind': 'Pod',
            'workload_name': 'svc-7-user-hash-head',
            'cluster_record_uuid_label': '33333333-3333-4333-8333-333333333333',
            'replica_incarnation_label': '22222222-2222-4222-8222-222222222222',
            'topology': _topology(),
        },
    }


def _resource_snapshot() -> dict:
    scope_sha256 = actions.ProviderKubernetesScopeV1.from_value(
        scope_fixtures._scope()).sha256
    return {
        'version': 1,
        'cloud': 'kubernetes',
        'cluster_fingerprint_sha256': scope_sha256,
        'namespace': 'serve-canary',
        'instance_type': '0.5CPU--1.23GB',
        'accelerator': None,
        'cpus': '0.5',
        'memory': '1.23',
        'image_id': ('registry.example/runtime@sha256:' + '1' * 64),
        'disk_size_gb': 100,
        'disk_tier': None,
        'ports': ['8080'],
        'labels': [],
        'use_spot': False,
    }


def _subject(
    capsule: actions.ProviderKubernetesExecutionCapsuleV1 | None = None,
    source: actions.ProviderLaunchSourceV1 | None = None,
    resource_identity: dict | None = None,
    requested_target: dict | None = None,
    resources: dict | None = None,
) -> actions.ProviderLaunchPolicySubjectV1:
    capsule = _capsule() if capsule is None else capsule
    resource_identity = (_resource_identity()
                         if resource_identity is None else resource_identity)
    source = _source(resource_identity) if source is None else source
    return actions.project_provider_launch_policy_subject_v1(
        actions.ProviderResourceIdentityV1.from_value(resource_identity),
        source,
        actions.ProviderLocatorV1.from_value(_target() if requested_target is
                                             None else requested_target),
        actions.ProviderPodResourceSnapshotV1.from_value(
            _resource_snapshot() if resources is None else resources),
        capsule.topology, 7, True, capsule)


def _policy_proof(
    boundary: str,
    capsule: actions.ProviderKubernetesExecutionCapsuleV1,
    subject: actions.ProviderLaunchPolicySubjectV1,
) -> actions.ProviderPolicyBoundaryProofV1:
    return actions.ProviderPolicyBoundaryProofV1(
        version=1,
        boundary=boundary,
        config_projection_sha256=capsule.config_projection_sha256,
        modes=actions.ProviderPolicyModeEvidenceV1(
            admin_policy_entrypoint=None,
            admin_policy_applied=False,
            managed_secrets_provider=None,
            managed_secret_reference_count=0),
        policy_subject_sha256=subject.sha256,
        projection_before_sha256=subject.sha256,
        projection_after_sha256=subject.sha256,
        projections_equal=True)


def _config(
    resource_identity: dict | None = None,
    requested_target: dict | None = None,
    resources: dict | None = None,
    *,
    workspace: str = 'workspace-a',
) -> actions.ProviderKubernetesExecutionConfigV1:
    resource_identity = (_resource_identity()
                         if resource_identity is None else resource_identity)
    capsule = _capsule(workspace=workspace)
    source = _source(resource_identity, workspace=workspace)
    subject = _subject(capsule, source, resource_identity, requested_target,
                       resources)
    return actions.ProviderKubernetesExecutionConfigV1(
        version=1,
        capsule=capsule,
        execution_capsule_sha256=capsule.sha256,
        policy_subject=subject,
        policy_subject_sha256=subject.sha256,
        controller=_policy_proof('serve_controller_prepare', capsule, subject),
        executor=_policy_proof('api_executor_pre_io', capsule, subject))


def _execution_config_from_parts(
    capsule: actions.ProviderKubernetesExecutionCapsuleV1,
    subject: actions.ProviderLaunchPolicySubjectV1,
) -> actions.ProviderKubernetesExecutionConfigV1:
    return actions.ProviderKubernetesExecutionConfigV1(
        version=1,
        capsule=capsule,
        execution_capsule_sha256=capsule.sha256,
        policy_subject=subject,
        policy_subject_sha256=subject.sha256,
        controller=_policy_proof('serve_controller_prepare', capsule, subject),
        executor=_policy_proof('api_executor_pre_io', capsule, subject))


def launch_payload(
    resource_identity: dict,
    requested_target: dict,
    resources: dict,
    *,
    workspace: str,
) -> dict:
    """Build the exact launch member used by adjacent action/store fixtures."""

    config = _config(resource_identity,
                     requested_target,
                     resources,
                     workspace=workspace)
    subject = config.policy_subject
    return {
        'source': subject.source.canonical_value(),
        'resources': subject.resources.canonical_value(),
        'topology': subject.topology.canonical_value(),
        'execution_config': config.canonical_value(),
        'replica_env': {
            'SKYPILOT_SERVE_REPLICA_ID': subject.replica_id_text
        },
        'security_group_scope': subject.security_group_scope,
        'admin_policy_mode': subject.admin_policy_mode,
        'managed_secrets_mode': subject.managed_secrets_mode,
        'retry_until_up': subject.retry_until_up,
        'exact_resources_override': subject.exact_resources_override,
        'backend': subject.backend,
        'optimize_target': subject.optimize_target,
        'dryrun': subject.dryrun,
        'no_setup': subject.no_setup,
        'clone_disk_from': subject.clone_disk_from,
        'fast': subject.fast,
        'file_mounts_blob_id': subject.file_mounts_blob_id,
        'tls_material_ref': subject.tls_material_ref,
    }


def test_explicit_username_and_request_identity_projectors_are_pure() -> None:
    assert actions.clean_username_for_explicit_user_v1(
        '1SkY-PiLot2-') == 'sky-pilot2'
    assert actions.clean_username_for_explicit_user_v1(
        'Alice.Example@example.com') == 'aliceexampleexamplecom'
    basis = actions.ProviderWorkloadNameBasisV1(version=1,
                                                display_name='svc-7',
                                                frozen_user_hash='user-hash',
                                                max_length=42,
                                                cluster_name_hash_length=8)
    projected = actions.project_provider_kubernetes_request_identity_v1(
        'Alice.Example@example.com', basis)
    assert projected.canonical_value() == {
        'cleaned_user': 'aliceexampleexamplecom',
        'original_user': 'Alice.Example@example.com',
        'frozen_user_hash': 'user-hash',
    }
    for invalid in ('---123', 'é', 'a' * 62 + '-x'):
        with pytest.raises(ValueError):
            actions.clean_username_for_explicit_user_v1(invalid)
    with pytest.raises(TypeError, match='must be text'):
        actions.clean_username_for_explicit_user_v1(7)  # type: ignore[arg-type]
    for username in ('Alice.Example@example.com', '1SkY-PiLot2-', 'user_name'):
        assert actions.clean_username_for_explicit_user_v1(username) == (
            common_utils.clean_username_for_explicit_user_v1(username))


def test_launch_source_wrapper_is_exact_and_content_only_leaves_stay_closed(
) -> None:
    source = _source()
    raw = source.canonical_value()
    assert actions.ProviderLaunchSourceV1.from_value(raw) == source
    assert source.identity_canonicalization_sha256 == (
        source.identity_canonicalization.sha256)
    assert hashlib.sha256(source.canonical_bytes).hexdigest() == source.sha256
    assert len(source.canonical_bytes) == 1467
    assert source.sha256 == (
        'e72f1acdc989235f357ea1f4b76fa2aa5c39e64683e49338d80241e0f8f96845')
    assert set(raw) == {
        'content', 'identity_canonicalization',
        'identity_canonicalization_sha256'
    }

    wrong_hash = copy.deepcopy(raw)
    wrong_hash['identity_canonicalization_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='hash does not match'):
        actions.ProviderLaunchSourceV1.from_value(wrong_hash)

    crossed = copy.deepcopy(raw)
    crossed['content']['service_name'] = 'other-service'
    with pytest.raises(ValueError, match='service name does not match'):
        actions.ProviderLaunchSourceV1.from_value(crossed)

    leaked = _content_source()
    leaked['identity_canonicalization'] = raw['identity_canonicalization']
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderLaunchContentSourceV1.from_value(leaked)


def test_launch_capsule_and_execution_config_have_fixed_canonical_bytes(
) -> None:
    capsule = _capsule()
    config = _config()
    assert actions.ProviderKubernetesExecutionCapsuleV1.from_value(
        capsule.canonical_value()) == capsule
    assert actions.ProviderKubernetesExecutionConfigV1.from_value(
        config.canonical_value()) == config
    assert len(capsule.canonical_bytes) == 34362
    assert capsule.sha256 == (
        '0d9e435486120d9e95dfee2590ff473045cbe1f3980998961fdc635c83debfeb')
    assert len(config.canonical_bytes) == 42664
    assert config.sha256 == (
        '2d194ab2e98b682025f3939492b6b8e17b5b0a5f66fd1a7f0a2230eb00e38017')


@pytest.mark.parametrize('mutation,match', [
    (lambda raw: raw.update({'config_projection_sha256': '0' * 64}),
     'config projection hash'),
    (lambda raw: raw['post_provision']['job_submission']['run_source'].update(
        {'workspace': 'crossed-workspace'}), 'renderer and run source'),
    (lambda raw: raw['config_projection']['config_access_inventory'].update(
        {'sha256': 'f' * 64}), 'config-access inventory'),
    (lambda raw: raw['objects'][1]['normalization_profile'].update(
        {'sha256': 'e' * 64}), 'object normalization profiles'),
    (lambda raw: raw['objects'].reverse(), 'exact create order'),
])
def test_launch_capsule_rejects_every_owned_cross_field_binding(
        mutation, match: str) -> None:
    raw = _capsule_raw()
    mutation(raw)
    if match == 'config-access inventory':
        raw['config_projection_sha256'] = actions.canonical_sha256(
            raw['config_projection'])
    with pytest.raises(ValueError, match=match):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)


def test_capsule_rejects_closed_shape_and_raw_collection_abuse() -> None:
    raw = _capsule_raw()
    raw['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)
    raw = _capsule_raw()
    raw['objects'] = [object()] * 10_000
    with pytest.raises(ValueError, match='at most 256'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)
    raw = _capsule_raw()
    raw['prerequisites'] = [object()] * 10_000
    with pytest.raises(ValueError, match='at most 256'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)


def test_launch_policy_projector_owns_variable_preimage_bindings() -> None:
    capsule = _capsule()
    subject = _subject(capsule)
    source = subject.source
    assert subject.execution_capsule_sha256 == capsule.sha256
    assert subject.replica_id_text == '7'
    assert subject.security_group_scope == 'not_applicable:kubernetes'
    assert subject.admin_policy_mode == 'absent_controller_and_executor'
    assert subject.managed_secrets_mode == 'absent'

    with pytest.raises(ValueError, match='replica ID does not match'):
        actions.project_provider_launch_policy_subject_v1(
            source.identity_canonicalization.context.input.resource_identity,
            source, subject.requested_target, subject.resources,
            capsule.topology, 8, True, capsule)

    changed_topology = dataclasses.replace(capsule.topology,
                                           application_port='8081',
                                           resources_ports=('8081',))
    with pytest.raises(ValueError, match='topology does not match'):
        actions.project_provider_launch_policy_subject_v1(
            source.identity_canonicalization.context.input.resource_identity,
            source, subject.requested_target, subject.resources,
            changed_topology, 7, True, capsule)

    crossed_identity = _resource_identity()
    crossed_identity['replica_incarnation'] = (
        '99999999-9999-4999-8999-999999999999')
    crossed_source = _source(crossed_identity)
    with pytest.raises(ValueError, match='request identity does not match'):
        actions.project_provider_launch_policy_subject_v1(
            actions.ProviderResourceIdentityV1.from_value(crossed_identity),
            crossed_source, subject.requested_target, subject.resources,
            capsule.topology, 7, True, capsule)

    context = dataclasses.replace(source.identity_canonicalization.context,
                                  cohort_id='crossed-cohort')
    proof = dataclasses.replace(source.identity_canonicalization,
                                context=context,
                                context_sha256=context.sha256)
    crossed_cohort_source = dataclasses.replace(
        source,
        identity_canonicalization=proof,
        identity_canonicalization_sha256=proof.sha256)
    with pytest.raises(ValueError, match='request identity does not match'):
        actions.project_provider_launch_policy_subject_v1(
            source.identity_canonicalization.context.input.resource_identity,
            crossed_cohort_source, subject.requested_target, subject.resources,
            capsule.topology, 7, True, capsule)


def test_locator_fingerprint_is_the_complete_scope_hash() -> None:
    target = _target()
    parsed = actions.ProviderLocatorV1.from_value(target)
    assert parsed.kubernetes is not None
    assert parsed.kubernetes.cluster_fingerprint_sha256 == (
        parsed.kubernetes.scope.sha256)
    target['kubernetes']['cluster_fingerprint_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='fingerprint does not match'):
        actions.ProviderLocatorV1.from_value(target)


@pytest.mark.parametrize('mutation,match', [
    (lambda raw: raw.update({'execution_capsule_sha256': '0' * 64}),
     'capsule hash does not match'),
    (lambda raw: raw['policy_subject'].update(
        {'execution_capsule_sha256': '0' * 64}), 'not bound to its capsule'),
    (lambda raw: raw.update({'policy_subject_sha256': '0' * 64}),
     'policy subject hash does not match'),
    (lambda raw: raw['policy']['controller'].update(
        {'boundary': 'api_executor_pre_io'}), 'wrong boundary slot'),
    (lambda raw: raw['policy']['executor'].update(
        {'config_projection_sha256': '0' * 64}),
     'config projection hash does not match'),
    (lambda raw: raw['policy']['executor'].update(
        {'policy_subject_sha256': '0' * 64}),
     'policy subject hash does not match'),
    (lambda raw: raw['policy']['executor'].update({
        'projection_before_sha256': '0' * 64,
        'projection_after_sha256': '0' * 64,
    }), 'projection hashes do not match'),
])
def test_execution_config_recomputes_every_content_addressed_edge(
        mutation, match: str) -> None:
    raw = _config().canonical_value()
    mutation(raw)
    with pytest.raises(ValueError, match=match):
        actions.ProviderKubernetesExecutionConfigV1.from_value(raw)


def test_execution_config_policy_object_is_closed_without_compatibility_shim(
) -> None:
    raw = _config().canonical_value()
    raw['policy']['legacy'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesExecutionConfigV1.from_value(raw)
    flattened = _config().canonical_value()
    flattened['controller'] = flattened['policy']['controller']
    flattened['executor'] = flattened['policy']['executor']
    del flattened['policy']
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesExecutionConfigV1.from_value(flattened)


@pytest.mark.parametrize('mutation,match', [
    (lambda pod: pod['spec']['containers'][0]['env'].append({
        'name': 'AWS_TOKEN',
        'valueFrom': {
            'secretKeyRef': {
                'name': 'raw-secret',
                'key': 'token'
            }
        }
    }), 'environment entry is not exact'),
    (lambda pod: pod['spec']['containers'][0]['env'].append('raw-secret'),
     'environment entry is not exact'),
    (lambda pod: pod['spec']['containers'][0]['ports'].append(
        {'containerPort': 12345}), 'application ports are invalid'),
    (lambda pod: pod['metadata'].update({'finalizers': ['evil']}),
     'metadata is not exact'),
])
def test_capsule_rejects_unreviewed_pod_request_body_extensions(
        mutation, match: str) -> None:
    raw = _capsule_raw()
    pod_plan = raw['objects'][2]
    mutation(pod_plan['request_body'])
    pod_plan['request_body_sha256'] = actions.canonical_sha256(
        pod_plan['request_body'])
    with pytest.raises(ValueError, match=match):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)


def test_capsule_rejects_crossed_namespace_endpoint_and_resource_projections(
) -> None:
    raw = _capsule_raw()
    raw['config_projection']['target_namespace'] = 'crossed-namespace'
    raw['config_projection_sha256'] = actions.canonical_sha256(
        raw['config_projection'])
    with pytest.raises(ValueError, match='target namespaces'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)

    raw = _capsule_raw()
    raw['endpoint']['prerequisite_projection'][0]['resource_version'] = '999'
    with pytest.raises(ValueError, match='prerequisite projection'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)

    raw = _capsule_raw()
    raw['resources']['application_port'] = '8081'
    raw['resources']['resources_ports'] = ['8081']
    with pytest.raises(ValueError, match='resource, topology'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)


@pytest.mark.parametrize('reserved_port',
                         ('22', '10001', '10002', '10003', '10004', '46590'))
def test_capsule_rejects_application_port_collisions(
        reserved_port: str) -> None:
    raw = _capsule_raw()
    raw['resources']['application_port'] = reserved_port
    raw['resources']['resources_ports'] = [reserved_port]
    raw['endpoint']['application_port'] = reserved_port
    raw['topology']['application_port'] = reserved_port
    raw['topology']['resources_ports'] = [reserved_port]
    with pytest.raises(ValueError, match='renderer-owned port'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)


def test_capsule_rejects_caller_selected_role_final_label_maps() -> None:
    raw = _capsule_raw()
    for label in raw['topology']['mutable_objects'][0]['labels']:
        if label['key'] == 'service-role':
            label['value'] = 'caller-controlled'
    raw['objects'][0]['request_body']['metadata']['labels'][
        'service-role'] = 'caller-controlled'
    raw['objects'][0]['request_body_sha256'] = actions.canonical_sha256(
        raw['objects'][0]['request_body'])
    with pytest.raises(ValueError, match='role label map is not exact'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)

    raw = _capsule_raw()
    raw['topology']['mutable_objects'][2]['labels'].append({
        'key': 'zz-caller',
        'value': 'raw-input'
    })
    raw['objects'][2]['request_body']['metadata']['labels'][
        'zz-caller'] = 'raw-input'
    raw['objects'][2]['request_body_sha256'] = actions.canonical_sha256(
        raw['objects'][2]['request_body'])
    with pytest.raises(ValueError, match='role label map is not exact'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(raw)


def test_execution_config_replays_projection_for_crossed_source_graph() -> None:
    base = _config()
    crossed_source = _source(workspace='crossed-workspace')
    crossed_subject = dataclasses.replace(base.policy_subject,
                                          source=crossed_source)
    with pytest.raises(ValueError, match='source does not match'):
        _execution_config_from_parts(base.capsule, crossed_subject)


def test_execution_config_replays_projected_request_identity() -> None:
    raw = _capsule_raw()
    raw['request_identity'] = {
        'cleaned_user': 'otheruser',
        'original_user': 'Other.User',
        'frozen_user_hash': 'other-hash',
    }
    for topology_item, object_plan in zip(raw['topology']['mutable_objects'],
                                          raw['objects']):
        for label in topology_item['labels']:
            if label['key'] == 'skypilot-user':
                label['value'] = 'otheruser'
        object_plan['request_body']['metadata']['labels']['skypilot-user'] = (
            'otheruser')
        if object_plan['role'] == 'head_pod':
            object_plan['request_body']['metadata']['annotations'][
                'skypilot-user'] = 'Other.User'
        object_plan['request_body_sha256'] = actions.canonical_sha256(
            object_plan['request_body'])
    crossed_capsule = actions.ProviderKubernetesExecutionCapsuleV1.from_value(
        raw)
    base_subject = _subject()
    crossed_subject = dataclasses.replace(
        base_subject, execution_capsule_sha256=crossed_capsule.sha256)
    with pytest.raises(ValueError, match='request identity does not match'):
        _execution_config_from_parts(crossed_capsule, crossed_subject)


def test_authoritative_direct_construction_rejects_nested_subclasses() -> None:
    capsule = _capsule()

    class EvilAuthorization(actions.ProviderKubernetesAuthorizationEvidenceV1):

        def canonical_value(self) -> dict:
            value = super().canonical_value()
            value['identity_proof'] = 'injected'
            return value

    authorization = capsule.principals.caller_authorization
    evil_authorization = EvilAuthorization(
        **{
            field.name: getattr(authorization, field.name)
            for field in dataclasses.fields(authorization)
        })
    with pytest.raises(TypeError, match='caller authorization'):
        dataclasses.replace(capsule.principals,
                            caller_authorization=evil_authorization)

    class EvilJobSubmission(actions.ProviderKubernetesJobSubmissionV1):

        def canonical_value(self) -> dict:
            value = super().canonical_value()
            value['credential'] = 'injected'
            return value

    job_submission = capsule.post_provision.job_submission
    evil_submission = EvilJobSubmission(
        **{
            field.name: getattr(job_submission, field.name)
            for field in dataclasses.fields(job_submission)
        })
    with pytest.raises(TypeError, match='job_submission'):
        dataclasses.replace(capsule.post_provision,
                            job_submission=evil_submission)

    class EvilArtifact(actions.ProviderRepoArtifactRefV1):

        def canonical_value(self) -> dict:
            value = super().canonical_value()
            value['secret'] = 'injected'
            return value

    binding = capsule.post_provision.runtime_artifacts[0]
    artifact = binding.source_manifest
    evil_artifact = EvilArtifact(
        **{
            field.name: getattr(artifact, field.name)
            for field in dataclasses.fields(artifact)
        })
    with pytest.raises(TypeError, match='source_manifest'):
        dataclasses.replace(binding, source_manifest=evil_artifact)

    class EvilTransport(actions.ProviderKubernetesTransportIdentityV1):

        def canonical_value(self) -> dict:
            value = super().canonical_value()
            value['credential_locator'] = 'injected'
            return value

    transport = capsule.scope.transport
    evil_transport = EvilTransport(
        **{
            field.name: getattr(transport, field.name)
            for field in dataclasses.fields(transport)
        })
    with pytest.raises(TypeError, match='scope transport'):
        dataclasses.replace(capsule.scope, transport=evil_transport)


def test_authoritative_direct_construction_rejects_tuple_subclasses() -> None:
    capsule = _capsule()

    class DeceptiveTuple(tuple):

        def __bool__(self) -> bool:
            return False

        def __eq__(self, other: object) -> bool:
            del other
            return True

    hidden = DeceptiveTuple(('hidden',))
    for field in capsule.config_projection._EMPTY_FIELDS:
        with pytest.raises(TypeError, match=f'{field} must be a tuple'):
            dataclasses.replace(capsule.config_projection, **{field: hidden})

    authorization = capsule.principals.caller_authorization
    with pytest.raises(TypeError, match='extra_keys must be a tuple'):
        dataclasses.replace(authorization.identity, extra_keys=hidden)

    nonresource_rule = authorization.rules.non_resource_rules[0]
    for field in ('urls', 'verbs'):
        with pytest.raises(TypeError, match='fields must be tuples'):
            dataclasses.replace(nonresource_rule, **{field: hidden})

    with pytest.raises(TypeError, match='handler allowlist must be a tuple'):
        dataclasses.replace(capsule.executor_cohort.manifest,
                            handler_allowlist=hidden)


def test_complete_launch_graph_rejects_cycles_without_recursion() -> None:
    source = _source().canonical_value()
    source['content']['workspace'] = cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match='reference cycle'):
        actions.ProviderLaunchSourceV1.from_value(source)

    subject = _subject().canonical_value()
    subject['replica_env']['SKYPILOT_SERVE_REPLICA_ID'] = cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match='reference cycle'):
        actions.ProviderLaunchPolicySubjectV1.from_value(subject)

    capsule = _capsule_raw()
    request_body = capsule['objects'][2]['request_body']
    request_body['cycle'] = request_body
    with pytest.raises(ValueError, match='reference cycle'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(capsule)
