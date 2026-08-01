"""Pure immutable Kubernetes execution-foundation contract tests."""

import dataclasses

import pytest

from sky.serve import resource_actions as actions

_CLUSTER_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'


def _artifact(path: str = 'contracts/config-access.json') -> dict:
    return {'repo_path': path, 'byte_size': 17, 'sha256': 'a' * 64}


def _qualification() -> dict:
    return {
        'requested_reference': 'registry.example/runtime:approved@sha256:' +
                               '1' * 64,
        'oci_manifest_digest': 'sha256:' + '1' * 64,
        'oci_config_digest': 'sha256:' + '2' * 64,
        'qualification_artifact': _artifact('images/runtime.json'),
    }


def _image() -> dict:
    return {
        'source': 'explicit',
        'qualification': _qualification(),
        'auth_strategy': 'anonymous',
        'implementation_contract': 'kubernetes_serve_prebooted_runtime_v1',
    }


def _identity_labels(*, role: str) -> list[dict]:
    labels = [{
        'key': 'skypilot-cluster-name',
        'value': 'svc-replica',
    }, {
        'key': 'skypilot.co/cluster-record-uuid',
        'value': _CLUSTER_UUID,
    }, {
        'key': 'skypilot.co/serve-replica-incarnation',
        'value': _REPLICA_UUID,
    }]
    if role == 'head_pod':
        labels.insert(0, {'key': 'component', 'value': 'svc-replica-head'})
    else:
        labels.insert(0, {'key': 'service-role', 'value': role})
    return labels


def _topology() -> dict:
    return {
        'version': 1,
        'kind': 'single_direct_pod_two_services',
        'node_count': 1,
        'application_port': '8080',
        'resources_ports': ['8080'],
        'mutable_objects': [{
            'kind': 'Service',
            'role': 'head_ssh_service',
            'name': 'svc-replica-head-ssh',
            'labels': _identity_labels(role='head_ssh_service'),
        }, {
            'kind': 'Service',
            'role': 'head_service',
            'name': 'svc-replica-head',
            'labels': _identity_labels(role='head_service'),
        }, {
            'kind': 'Pod',
            'role': 'head_pod',
            'name': 'svc-replica-head',
            'labels': _identity_labels(role='head_pod'),
        }],
        'shared_prerequisites': 'preexisting_read_only',
    }


def _config_projection() -> dict:
    return {
        'version': 1,
        'workspace': 'workspace-a',
        'context_mode': 'in_cluster',
        'target_namespace': 'serve-canary',
        'port_mode': 'podip',
        'built_in_provider': True,
        'custom_provider_implementation': None,
        'custom_provisioner': None,
        'custom_template': None,
        'custom_pod_config': None,
        'custom_metadata': [],
        'global_labels': [],
        'runtime_class_name': None,
        'priority_class_name': None,
        'queue': None,
        'kueue': False,
        'dws': False,
        'autoscaler': None,
        'detected_network_type': 'default',
        'persistent_volumes': [],
        'object_stores': [],
        'file_mounts': [],
        'workdir': None,
        'fuse': False,
        'docker_cache': False,
        'auto_mounts': False,
        'tls_material': None,
        'managed_secrets': [],
        'task_secrets': [],
        'service_account_bootstrap': False,
        'rbac_bootstrap': False,
        'config_access_inventory': _artifact(),
    }


def _policy_modes() -> dict:
    return {
        'admin_policy_entrypoint': None,
        'admin_policy_applied': False,
        'managed_secrets_provider': None,
        'managed_secret_reference_count': 0,
    }


def _service_account(*, caller: bool) -> dict:
    return {
        'namespace': 'skypilot-system' if caller else 'serve-canary',
        'name': 'authority-worker' if caller else 'serve-workload',
        'uid': 'uid-authority-worker' if caller else 'uid-serve-workload',
        'resource_version': '123',
        'labels': [{
            'key': 'app',
            'value': 'authority' if caller else 'serve'
        }],
        'annotations': [{
            'key': 'example.com/long-annotation',
            'value': 'x' * 300,
        }],
        'automount_service_account_token': caller,
        'image_pull_secrets': ['pull-a'] if caller else [],
        'legacy_secret_refs': ['legacy-a'] if caller else [],
    }


def _identity() -> dict:
    return {
        'username': 'system:serviceaccount:skypilot-system:authority-worker',
        'uid': 'uid-authority-worker',
        'groups': [
            'system:authenticated', 'system:serviceaccounts',
            'system:serviceaccounts:skypilot-system'
        ],
        'extra_keys': [],
    }


def _rules() -> dict:
    return {
        'namespace': 'serve-canary',
        'incomplete': False,
        'evaluation_error': False,
        'resource_rules': [{
            'api_groups': [''],
            'resources': ['pods', 'services'],
            'resource_names': [],
            'verbs': ['create', 'delete', 'get'],
        }],
        'non_resource_rules': [{
            'urls': ['/version'],
            'verbs': ['get']
        }],
    }


def _decisions() -> list[dict]:
    return [{
        'check_sequence': 0,
        'resource': {
            'api_group': '',
            'resource': 'pods',
            'subresource': None,
            'verb': 'create',
            'namespace': 'serve-canary',
            'name': None,
        },
        'non_resource': None,
        'expected_allowed': True,
        'observed_allowed': True,
        'observed_denied': False,
        'evaluation_error': False,
    }, {
        'check_sequence': 1,
        'resource': None,
        'non_resource': {
            'verb': 'get',
            'path': '/version'
        },
        'expected_allowed': False,
        'observed_allowed': False,
        'observed_denied': False,
        'evaluation_error': False,
    }]


def _authorization() -> dict:
    rules = _rules()
    decisions = _decisions()
    return {
        'identity': _identity(),
        'rules': rules,
        'rules_sha256': actions.canonical_sha256(rules),
        'access_matrix_contract': _artifact('contracts/access-matrix.json'),
        'access_decisions': decisions,
        'access_decisions_sha256': actions.canonical_sha256(decisions),
    }


def _principals() -> dict:
    return {
        'caller': _service_account(caller=True),
        'workload': _service_account(caller=False),
        'caller_authorization': _authorization(),
    }


@pytest.mark.parametrize(('value', 'expected'), [
    ('1', '1'),
    ('65535', '65535'),
])
def test_decimal_port_boundaries(value: str, expected: str) -> None:
    topology = _topology()
    topology['application_port'] = value
    topology['resources_ports'] = [value]
    assert actions.ProviderPodTopologyV1.from_value(
        topology).application_port == expected


@pytest.mark.parametrize('value', [
    '', '0', '00', '01', '+1', '-1', ' 1', '65536', '100000', 8080, 8.0, False
])
def test_decimal_port_rejects_noncanonical_values(value) -> None:
    topology = _topology()
    topology['application_port'] = value
    topology['resources_ports'] = [value]
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderPodTopologyV1.from_value(topology)


def test_foundation_literals_roundtrip_and_hashes() -> None:
    topology = actions.ProviderPodTopologyV1.from_value(_topology())
    image = actions.ProviderPodImageV1.from_value(_image())
    projection = actions.ProviderKubernetesConfigProjectionV1.from_value(
        _config_projection())
    modes = actions.ProviderPolicyModeEvidenceV1.from_value(_policy_modes())
    principals = actions.ProviderKubernetesPrincipalsV1.from_value(
        _principals())

    for parsed, parser in (
        (topology, actions.ProviderPodTopologyV1.from_value),
        (image, actions.ProviderPodImageV1.from_value),
        (projection, actions.ProviderKubernetesConfigProjectionV1.from_value),
        (modes, actions.ProviderPolicyModeEvidenceV1.from_value),
        (principals, actions.ProviderKubernetesPrincipalsV1.from_value),
    ):
        roundtripped = parser(parsed.canonical_value())
        assert roundtripped.canonical_bytes == parsed.canonical_bytes
        assert roundtripped.sha256 == actions.canonical_sha256(
            parsed.canonical_value())

    assert topology.mutable_objects[0].role.value == 'head_ssh_service'
    assert topology.mutable_objects[2].kind.value == 'Pod'
    assert image.qualification.oci_manifest_digest == 'sha256:' + '1' * 64
    assert principals.caller_authorization.rules_sha256 == (
        principals.caller_authorization.rules.sha256)


def test_closed_enum_values_and_api_group_resource_map() -> None:
    assert tuple(item.value for item in actions.ProviderObjectRoleV1) == (
        'head_ssh_service', 'head_service', 'head_pod')
    assert tuple(
        item.value for item in actions.ProviderKubernetesApiGroupV1) == (
            '', 'apps', 'networking.k8s.io', 'admissionregistration.k8s.io',
            'authentication.k8s.io', 'authorization.k8s.io')
    assert tuple(
        item.value for item in actions.ProviderKubernetesResourceV1) == (
            'namespaces', 'serviceaccounts', 'pods', 'services', 'replicasets',
            'deployments', 'networkpolicies', 'validatingadmissionpolicies',
            'validatingadmissionpolicybindings', 'selfsubjectreviews',
            'selfsubjectrulesreviews', 'selfsubjectaccessreviews')
    assert tuple(item.value for item in actions.ProviderKubernetesVerbV1) == (
        'get', 'create', 'delete', 'list', 'watch', 'patch', 'update',
        'deletecollection')
    resource = actions.ProviderKubernetesResourceV1
    api_group = actions.ProviderKubernetesApiGroupV1
    assert actions.PROVIDER_KUBERNETES_API_GROUP_RESOURCE_MAP_V1 == {
        api_group.CORE: frozenset({
            resource.NAMESPACES, resource.PODS, resource.SERVICE_ACCOUNTS,
            resource.SERVICES
        }),
        api_group.ADMISSION_REGISTRATION: frozenset({
            resource.VALIDATING_ADMISSION_POLICIES,
            resource.VALIDATING_ADMISSION_POLICY_BINDINGS
        }),
        api_group.APPS: frozenset({resource.DEPLOYMENTS,
                                   resource.REPLICA_SETS}),
        api_group.AUTHENTICATION: frozenset({resource.SELF_SUBJECT_REVIEWS}),
        api_group.AUTHORIZATION: frozenset({
            resource.SELF_SUBJECT_ACCESS_REVIEWS,
            resource.SELF_SUBJECT_RULES_REVIEWS
        }),
        api_group.NETWORKING: frozenset({resource.NETWORK_POLICIES}),
    }


@pytest.mark.parametrize(('path', 'replacement'), [
    (('kind',), 'other'),
    (('node_count',), 2),
    (('resources_ports',), ['8081']),
    (('shared_prerequisites',), 'mutable'),
    (('mutable_objects', 0, 'kind'), 'Pod'),
    (('mutable_objects', 0, 'role'), 'head_service'),
    (('mutable_objects', 0, 'name'), 'wrong-ssh'),
    (('mutable_objects', 2, 'name'), 'wrong-pod'),
    (('mutable_objects', 0, 'labels', 1, 'value'), 'wrong-display'),
    (('mutable_objects', 0, 'labels', 2, 'value'), _REPLICA_UUID),
])
def test_topology_rejects_fixed_order_name_and_identity_drift(
        path, replacement) -> None:
    value = _topology()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderPodTopologyV1.from_value(value)


def test_topology_rejects_unsorted_labels_and_direct_lists() -> None:
    value = _topology()
    value['mutable_objects'][0]['labels'].reverse()
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderPodTopologyV1.from_value(value)

    parsed = actions.ProviderPodTopologyV1.from_value(_topology())
    with pytest.raises(TypeError, match='tuple'):
        dataclasses.replace(
            parsed, resources_ports=list(
                parsed.resources_ports))  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError), match='three typed'):
        dataclasses.replace(
            parsed,
            mutable_objects=list(  # type: ignore[arg-type]
                parsed.mutable_objects))


@pytest.mark.parametrize(('left', 'right'), [(0, 1), (0, 2), (1, 2)])
def test_topology_rejects_equal_complete_role_label_maps(left: int,
                                                         right: int) -> None:
    value = _topology()
    value['mutable_objects'][right]['labels'] = value['mutable_objects'][left][
        'labels']
    with pytest.raises(ValueError, match='pairwise distinct'):
        actions.ProviderPodTopologyV1.from_value(value)


@pytest.mark.parametrize(('field', 'replacement'), [
    ('source', 'default'),
    ('auth_strategy', 'registry_secret'),
    ('implementation_contract', 'generic_runtime'),
])
def test_pod_image_rejects_nonliteral_modes(field: str,
                                            replacement: str) -> None:
    value = _image()
    value[field] = replacement
    with pytest.raises(ValueError):
        actions.ProviderPodImageV1.from_value(value)


def test_pod_image_accepts_tag_plus_digest_but_binds_digest() -> None:
    parsed = actions.ProviderPodImageV1.from_value(_image())
    assert ':approved@sha256:' in parsed.qualification.requested_reference

    mismatch = _image()
    mismatch['qualification']['oci_manifest_digest'] = 'sha256:' + 'f' * 64
    with pytest.raises(ValueError, match='requested reference digest'):
        actions.ProviderPodImageV1.from_value(mismatch)


@pytest.mark.parametrize(('field', 'replacement'), [
    ('context_mode', 'kubeconfig'),
    ('port_mode', 'loadbalancer'),
    ('built_in_provider', False),
    ('custom_template', 'template'),
    ('custom_metadata', ['x']),
    ('runtime_class_name', 'nvidia'),
    ('kueue', True),
    ('detected_network_type', 'custom'),
    ('persistent_volumes', ['pvc']),
    ('fuse', True),
    ('tls_material', 'secret'),
    ('managed_secrets', ['secret']),
    ('rbac_bootstrap', True),
])
def test_config_projection_rejects_every_nondefault_mode(
        field: str, replacement) -> None:
    value = _config_projection()
    value[field] = replacement
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesConfigProjectionV1.from_value(value)


def test_config_projection_direct_construction_requires_tuples() -> None:
    parsed = actions.ProviderKubernetesConfigProjectionV1.from_value(
        _config_projection())
    with pytest.raises(TypeError, match='tuple'):
        dataclasses.replace(
            parsed,
            custom_metadata=[]  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(('field', 'replacement'), [
    ('admin_policy_entrypoint', 'policy'),
    ('admin_policy_applied', True),
    ('managed_secrets_provider', 'provider'),
    ('managed_secret_reference_count', 1),
    ('managed_secret_reference_count', False),
])
def test_policy_modes_are_exact_absence_evidence(field: str,
                                                 replacement) -> None:
    value = _policy_modes()
    value[field] = replacement
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderPolicyModeEvidenceV1.from_value(value)


def test_annotation_uses_text_bounds_distinct_from_labels() -> None:
    annotation = actions.ProviderAnnotationV1.from_value({
        'key': 'k' * 1024,
        'value': 'v' * 1024,
    })
    assert len(annotation.key) == 1024
    with pytest.raises(ValueError, match='253'):
        actions.ProviderLabelV1.from_value({
            'key': 'k' * 254,
            'value': 'v',
        })
    with pytest.raises(ValueError, match='1024'):
        actions.ProviderAnnotationV1.from_value({
            'key': 'k' * 1025,
            'value': 'v',
        })


def test_service_account_sets_are_sorted_and_directly_typed() -> None:
    value = _service_account(caller=True)
    value['image_pull_secrets'] = ['z', 'a']
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesServiceAccountProjectionV1.from_value(value)

    parsed = actions.ProviderKubernetesServiceAccountProjectionV1.from_value(
        _service_account(caller=True))
    with pytest.raises(TypeError, match='tuple'):
        dataclasses.replace(
            parsed,
            annotations=list(  # type: ignore[arg-type]
                parsed.annotations))


@pytest.mark.parametrize(('field', 'replacement'), [
    ('api_groups', []),
    ('api_groups', ['', 'apps']),
    ('resources', []),
    ('resources', ['services', 'pods']),
    ('resources', ['deployments']),
    ('resource_names', ['z', 'a']),
    ('verbs', []),
    ('verbs', ['get', 'get']),
    ('verbs', ['*']),
])
def test_resource_rules_enforce_cardinality_order_and_group_map(
        field: str, replacement) -> None:
    rule = _rules()['resource_rules'][0]
    rule[field] = replacement
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesResourceRuleV1.from_value(rule)


def test_rules_review_requires_sorted_rules_and_exact_nonresource_rule(
) -> None:
    rules = _rules()
    second = {
        'api_groups': ['apps'],
        'resources': ['deployments'],
        'resource_names': [],
        'verbs': ['get'],
    }
    rules['resource_rules'] = [rules['resource_rules'][0], second]
    parsed = [
        actions.ProviderKubernetesResourceRuleV1.from_value(rule)
        for rule in rules['resource_rules']
    ]
    if parsed[0].canonical_bytes < parsed[1].canonical_bytes:
        rules['resource_rules'].reverse()
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesRulesReviewV1.from_value(rules)

    bad_nonresource = _rules()
    bad_nonresource['non_resource_rules'][0]['urls'] = ['/healthz']
    with pytest.raises(ValueError, match='GET /version'):
        actions.ProviderKubernetesRulesReviewV1.from_value(bad_nonresource)


@pytest.mark.parametrize(('resource', 'non_resource'), [
    (None, None),
    (_decisions()[0]['resource'], {
        'verb': 'get',
        'path': '/version'
    }),
])
def test_access_decision_discriminator_is_exclusive(resource,
                                                    non_resource) -> None:
    decision = _decisions()[0]
    decision['resource'] = resource
    decision['non_resource'] = non_resource
    with pytest.raises(ValueError, match='exactly one'):
        actions.ProviderKubernetesAccessDecisionV1.from_value(decision)


def test_access_decision_preserves_denied_and_matches_expectation() -> None:
    denied = _decisions()[1]
    denied['observed_denied'] = True
    assert actions.ProviderKubernetesAccessDecisionV1.from_value(
        denied).observed_denied is True

    mismatch = _decisions()[0]
    mismatch['observed_allowed'] = False
    with pytest.raises(ValueError, match='differs'):
        actions.ProviderKubernetesAccessDecisionV1.from_value(mismatch)

    contradictory = _decisions()[0]
    contradictory['observed_denied'] = True
    with pytest.raises(ValueError, match='cannot also'):
        actions.ProviderKubernetesAccessDecisionV1.from_value(contradictory)


def test_resource_access_rejects_cross_group_resource() -> None:
    access = _decisions()[0]['resource']
    access['api_group'] = 'apps'
    with pytest.raises(ValueError, match='outside its API group'):
        actions.ProviderKubernetesResourceAccessV1.from_value(access)


@pytest.mark.parametrize('field', ['rules_sha256', 'access_decisions_sha256'])
def test_authorization_recomputes_embedded_hashes(field: str) -> None:
    authorization = _authorization()
    authorization[field] = 'f' * 64
    with pytest.raises(ValueError, match='hash does not match'):
        actions.ProviderKubernetesAuthorizationEvidenceV1.from_value(
            authorization)


def test_authorization_requires_contiguous_nonempty_decisions() -> None:
    empty = _authorization()
    empty['access_decisions'] = []
    empty['access_decisions_sha256'] = actions.canonical_sha256([])
    with pytest.raises(ValueError, match='1..256'):
        actions.ProviderKubernetesAuthorizationEvidenceV1.from_value(empty)

    skipped = _authorization()
    skipped['access_decisions'][1]['check_sequence'] = 2
    skipped['access_decisions_sha256'] = actions.canonical_sha256(
        skipped['access_decisions'])
    with pytest.raises(ValueError, match='contiguous'):
        actions.ProviderKubernetesAuthorizationEvidenceV1.from_value(skipped)


@pytest.mark.parametrize(('path', 'replacement'), [
    (('caller', 'automount_service_account_token'), False),
    (('workload', 'automount_service_account_token'), True),
    (('workload', 'image_pull_secrets'), ['pull']),
    (('workload', 'legacy_secret_refs'), ['legacy']),
    (('caller_authorization', 'identity', 'username'), 'other'),
    (('caller_authorization', 'identity', 'uid'), 'other'),
    (('caller_authorization', 'identity', 'groups', 2),
     'system:serviceaccounts:other'),
    (('caller_authorization', 'rules', 'namespace'), 'other'),
])
def test_principals_enforce_caller_workload_and_identity_contract(
        path, replacement) -> None:
    value = _principals()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement
    authorization = value['caller_authorization']
    authorization['rules_sha256'] = actions.canonical_sha256(
        authorization['rules'])
    with pytest.raises(ValueError):
        actions.ProviderKubernetesPrincipalsV1.from_value(value)


@pytest.mark.parametrize(('factory', 'parser'), [
    (_topology, actions.ProviderPodTopologyV1.from_value),
    (_image, actions.ProviderPodImageV1.from_value),
    (_config_projection,
     actions.ProviderKubernetesConfigProjectionV1.from_value),
    (_policy_modes, actions.ProviderPolicyModeEvidenceV1.from_value),
    (_principals, actions.ProviderKubernetesPrincipalsV1.from_value),
])
def test_closed_foundation_objects_reject_unknown_keys(factory, parser) -> None:
    value = factory()
    value['unknown'] = 'forbidden'
    with pytest.raises(ValueError, match='unknown or missing'):
        parser(value)


def test_foundation_contracts_are_frozen_and_bounded() -> None:
    topology = actions.ProviderPodTopologyV1.from_value(_topology())
    with pytest.raises(dataclasses.FrozenInstanceError):
        topology.application_port = '9000'

    oversized = _config_projection()
    oversized['workspace'] = 'x' * 1025
    with pytest.raises(ValueError, match='1024'):
        actions.ProviderKubernetesConfigProjectionV1.from_value(oversized)

    oversized_topology = _topology()
    labels = []
    for index in range(256):
        suffix = f'{index:03d}'
        labels.append({
            'key': suffix + 'k' * 250,
            'value': 'v' * 253,
        })
    oversized_topology['mutable_objects'][0]['labels'] = labels
    with pytest.raises(ValueError, match='exceeds 65536 bytes'):
        actions.ProviderPodTopologyV1.from_value(oversized_topology)
