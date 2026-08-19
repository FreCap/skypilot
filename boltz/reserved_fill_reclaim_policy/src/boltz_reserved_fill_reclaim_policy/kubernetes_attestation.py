"""Exact Kubernetes/Kueue enforcement attestation for reserved fill."""

import base64
from collections.abc import Mapping
import contextlib
import dataclasses
import math
import threading
import time
from typing import Any, Iterator

from sky.adaptors import aws as aws_adaptor
from sky.adaptors import common as adaptor_common
from sky.adaptors import kubernetes as kubernetes_adaptor

yaml = adaptor_common.LazyImport('yaml')
botocore_credentials = adaptor_common.LazyImport('botocore.credentials')
botocore_signers = adaptor_common.LazyImport('botocore.signers')

_KUEUE_GROUP = 'kueue.x-k8s.io'
_KUEUE_API_VERSIONS = ('v1beta2', 'v1beta1')
_IRSA_ANNOTATION = 'eks.amazonaws.com/role-arn'
_KUEUE_MANAGED_LABEL = 'boltz.bio/kueue-managed'
_EKS_TOKEN_PREFIX = 'k8s-aws-v1.'
_EKS_TOKEN_TTL_SECONDS = 60
_KUBERAY_GENERATED_JOB_VARIABLE = (
    'object.kind == "Job" && '
    'request.namespace in '
    '["hyperpod-ns-research","boltz-research"] && '
    'request.resource.group == "batch" && '
    'request.resource.version == "v1" && '
    'request.resource.resource == "jobs" && '
    'request.userInfo.username == '
    '"system:serviceaccount:kuberay-system:kuberay-operator" && '
    'has(object.metadata.labels) && '
    "'ray.io/originated-from-cr-name' in object.metadata.labels && "
    "object.metadata.labels['ray.io/originated-from-cr-name'] == "
    'object.metadata.name && '
    "'ray.io/originated-from-crd' in object.metadata.labels && "
    "object.metadata.labels['ray.io/originated-from-crd'] == \"RayJob\" && "
    "'app.kubernetes.io/created-by' in object.metadata.labels && "
    "object.metadata.labels['app.kubernetes.io/created-by'] == "
    '"kuberay-operator" && '
    'has(object.spec.template.metadata.labels) && '
    "'kueue.x-k8s.io/queue-name' in "
    'object.spec.template.metadata.labels && '
    "object.spec.template.metadata.labels['kueue.x-k8s.io/queue-name'] "
    "!= '' && "
    'has(object.metadata.ownerReferences) && '
    'object.metadata.ownerReferences.size() == 1 && '
    'object.metadata.ownerReferences.exists(ref, '
    'ref.kind == "RayJob" && '
    'ref.apiVersion == "ray.io/v1" && '
    'ref.name == object.metadata.name && '
    'has(ref.controller) && ref.controller && '
    'has(ref.blockOwnerDeletion) && ref.blockOwnerDeletion)')
_QUEUE_NAME_VARIABLES = {
    'isKubeRayOperatorGeneratedJob': _KUBERAY_GENERATED_JOB_VARIABLE,
}
_QUEUE_NAME_VALIDATION = (
    'variables.isKubeRayOperatorGeneratedJob || '
    "(has(object.metadata.labels) && 'kueue.x-k8s.io/queue-name' in "
    'object.metadata.labels && '
    "object.metadata.labels['kueue.x-k8s.io/queue-name'] != '')")
_QUEUE_NAME_VALIDATION_MESSAGE = (
    "The label 'kueue.x-k8s.io/queue-name' is either missing or does not "
    'have a value set.')

_ResourceRuleSignature = tuple[tuple[str, ...], tuple[str, ...],
                               tuple[str, ...], tuple[str, ...], str]
_QUEUE_NAME_RESOURCE_RULES: frozenset[_ResourceRuleSignature] = frozenset({
    (('batch',), ('v1',), ('CREATE', 'UPDATE'), ('jobs',), '*'),
    (('kubeflow.org',), ('v1', 'v1beta1'), ('CREATE', 'UPDATE'),
     ('jaxjobs', 'mpijobs', 'paddlejobs', 'pytorchjobs', 'tfjobs',
      'xgboostjobs'), '*'),
    (('ray.io',), ('v1', 'v1alpha1'), ('CREATE', 'UPDATE'), ('rayclusters',
                                                             'rayjobs'), '*'),
    (('jobset.x-k8s.io',), ('v1alpha2',), ('CREATE', 'UPDATE'), ('jobsets',),
     '*'),
    (('workload.codeflare.dev',), ('v1alpha1',), ('CREATE', 'UPDATE'),
     ('appwrappers',), '*'),
    (('',), ('v1',), ('CREATE', 'UPDATE'), ('pods',), '*'),
    (('apps',), ('v1',), ('CREATE', 'UPDATE'), ('deployments', 'statefulsets'),
     '*'),
    (('leaderworkerset.x-k8s.io',), ('v1alpha1',), ('CREATE', 'UPDATE'),
     ('leaderworkersets',), '*'),
    (('sagemaker.amazonaws.com',), ('v1',), ('CREATE', 'UPDATE'),
     ('hyperpodpytorchjobs',), '*'),
})


class KubernetesAttestationError(RuntimeError):
    """Kubernetes did not prove the exact reclaim enforcement topology."""


@dataclasses.dataclass(frozen=True)
class NodeFlavorProof:
    """Safe summary of one provider-owned flavor's current Nodes."""

    flavor: str
    non_deleting_node_count: int
    product_label_value: str
    resource_name: str
    capacity_per_node: int


@dataclasses.dataclass(frozen=True)
class KubernetesContextProof:
    """Safe machine-readable result of one Kubernetes proof."""

    kubernetes_context: str
    physical_cluster_uid: str
    namespace_uid: str
    kueue_managed: bool
    local_queue_name: str | None
    cluster_queue_name: str | None
    pod_identity_irsa_annotation_absent: bool
    assign_queue_labels_for_pods: bool | None
    topology_aware_scheduling: bool | None
    custom_scheduler_deployment_proven: bool
    resource_flavor_topology_names: tuple[tuple[str, str | None], ...]
    node_flavors: tuple[NodeFlavorProof, ...]


def _remaining(deadline_monotonic: float,
               cancellation: threading.Event) -> float:
    if cancellation.is_set():
        raise KubernetesAttestationError(
            'Kubernetes attestation was cancelled.')
    remaining = deadline_monotonic - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise KubernetesAttestationError(
            'Kubernetes attestation exceeded its deadline.')
    return remaining


def _client_timeout(deadline_monotonic: float,
                    cancellation: threading.Event) -> float:
    remaining = _remaining(deadline_monotonic, cancellation)
    return min(1.0, remaining / 3)


def _eks_bearer_token(audit_session: Any, *, region: str, cluster_name: str,
                      deadline_monotonic: float,
                      cancellation: threading.Event) -> str:
    """Sign one bounded EKS token with the exact assumed audit session."""
    timeout = _client_timeout(deadline_monotonic, cancellation)
    config = aws_adaptor.botocore_config().Config(
        connect_timeout=timeout,
        read_timeout=timeout,
        retries={'total_max_attempts': 1})
    sts = audit_session.client('sts', region_name=region, config=config)
    credentials = audit_session.get_credentials()
    if credentials is None:
        raise KubernetesAttestationError(
            'The audit role returned no signing credentials.')
    # RequestSigner freezes its credential provider internally. boto3 Sessions
    # normally expose a refreshable botocore Credentials object, but the
    # deployment's bounded assumed-role session deliberately exposes an
    # already-frozen ReadOnlyCredentials value. Reconstruct the latter as an
    # immutable Credentials provider so the signer has its required interface
    # without gaining a refresh path or another identity source.
    freeze = getattr(credentials, 'get_frozen_credentials', None)
    signing_credentials = credentials
    if not callable(freeze):
        access_key = getattr(credentials, 'access_key', None)
        secret_key = getattr(credentials, 'secret_key', None)
        token = getattr(credentials, 'token', None)
        if (not isinstance(access_key, str) or not access_key or
                not isinstance(secret_key, str) or not secret_key or
            (token is not None and not isinstance(token, str))):
            raise KubernetesAttestationError(
                'The audit role returned invalid signing credentials.')
        signing_credentials = botocore_credentials.Credentials(
            access_key=access_key,
            secret_key=secret_key,
            token=token,
        )
    signer = botocore_signers.RequestSigner(
        sts.meta.service_model.service_id,
        region,
        'sts',
        'v4',
        signing_credentials,
        sts.meta.events,
    )
    endpoint = sts.meta.endpoint_url
    if not isinstance(endpoint, str) or not endpoint.startswith('https://'):
        raise KubernetesAttestationError(
            'STS returned an invalid regional endpoint.')
    request = {
        'method': 'GET',
        'url': (f'{endpoint.rstrip("/")}/?Action=GetCallerIdentity&'
                'Version=2011-06-15'),
        'body': {},
        'headers': {
            'x-k8s-aws-id': cluster_name,
        },
        'context': {},
    }
    signed_url = signer.generate_presigned_url(
        request,
        region_name=region,
        expires_in=_EKS_TOKEN_TTL_SECONDS,
        operation_name='',
    )
    _remaining(deadline_monotonic, cancellation)
    if not isinstance(signed_url, str) or not signed_url:
        raise KubernetesAttestationError(
            'STS returned an invalid EKS authentication signature.')
    encoded = base64.urlsafe_b64encode(
        signed_url.encode('utf-8')).decode('ascii').rstrip('=')
    return f'{_EKS_TOKEN_PREFIX}{encoded}'


@contextlib.contextmanager
def _audit_api_client(
    provider_context: Mapping[str, Any],
    audit_session: Any,
    *,
    deadline_monotonic: float,
    cancellation: threading.Event,
) -> Iterator[Any]:
    """Build and retire one isolated client authenticated as the audit role."""
    eks_contract = _dict(provider_context.get('eks'), 'EKS contract')
    region = eks_contract.get('region')
    cluster_name = eks_contract.get('cluster_name')
    cluster_arn = eks_contract.get('cluster_arn')
    if not all(
            isinstance(value, str) and value
            for value in (region, cluster_name, cluster_arn)):
        raise KubernetesAttestationError(
            'The EKS audit contract is incomplete.')
    timeout = _client_timeout(deadline_monotonic, cancellation)
    config = aws_adaptor.botocore_config().Config(
        connect_timeout=timeout,
        read_timeout=timeout,
        retries={'total_max_attempts': 1})
    eks = audit_session.client('eks', region_name=region, config=config)
    response = eks.describe_cluster(name=cluster_name)
    if not isinstance(response, Mapping):
        raise KubernetesAttestationError(
            'EKS returned an invalid cluster response.')
    cluster = response.get('cluster')
    if not isinstance(cluster, Mapping):
        raise KubernetesAttestationError('EKS returned no cluster inventory.')
    certificate_authority = cluster.get('certificateAuthority')
    if not isinstance(certificate_authority, Mapping):
        raise KubernetesAttestationError(
            'EKS returned no cluster certificate authority.')
    endpoint = cluster.get('endpoint')
    ca_data = certificate_authority.get('data')
    if (cluster.get('name') != cluster_name or
            cluster.get('arn') != cluster_arn or
            cluster.get('status') != 'ACTIVE' or
            not isinstance(endpoint, str) or
            not endpoint.startswith('https://') or
            not isinstance(ca_data, str) or not ca_data):
        raise KubernetesAttestationError(
            'EKS did not return the exact active cluster connection.')
    token = _eks_bearer_token(
        audit_session,
        region=region,
        cluster_name=cluster_name,
        deadline_monotonic=deadline_monotonic,
        cancellation=cancellation,
    )
    context_name = 'reserved-fill-reclaim-audit'
    config_document = {
        'apiVersion': 'v1',
        'kind': 'Config',
        'clusters': [{
            'name': context_name,
            'cluster': {
                'server': endpoint,
                'certificate-authority-data': ca_data,
            },
        }],
        'contexts': [{
            'name': context_name,
            'context': {
                'cluster': context_name,
                'user': context_name,
            },
        }],
        'current-context': context_name,
        'users': [{
            'name': context_name,
            'user': {
                'token': token,
            },
        }],
    }
    configuration = kubernetes_adaptor.kubernetes.client.Configuration()
    loader = kubernetes_adaptor.kubernetes.config.kube_config.KubeConfigLoader(
        config_dict=config_document,
        active_context=context_name,
    )
    loader.load_and_set(configuration)
    configuration.refresh_api_key_hook = None
    client = kubernetes_adaptor.kubernetes.client.ApiClient(
        configuration=configuration)
    config_document['users'][0]['user'].clear()
    token = ''
    try:
        _remaining(deadline_monotonic, cancellation)
        yield client
    finally:
        configuration.api_key.clear()
        configuration.api_key_prefix.clear()
        close = getattr(client, 'close', None)
        if callable(close):
            close()


def _require_physical_cluster_uid(client: Any, expected_uid: str) -> None:
    namespace = kubernetes_adaptor.kubernetes.client.CoreV1Api(
        api_client=client).read_namespace(
            'kube-system', _request_timeout=kubernetes_adaptor.API_TIMEOUT)
    metadata = getattr(namespace, 'metadata', None)
    observed_uid = getattr(metadata, 'uid', None)
    if observed_uid != expected_uid:
        raise KubernetesAttestationError(
            'Kubernetes physical-cluster identity changed before attestation.')


def _dict(value: object, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KubernetesAttestationError(
            f'Kubernetes returned invalid {path} data.')
    return value


def _list(value: object, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise KubernetesAttestationError(
            f'Kubernetes returned invalid {path} data.')
    return value


def _metadata(value: Mapping[str, Any],
              *,
              name: str,
              namespace: str | None = None) -> Mapping[str, Any]:
    metadata = _dict(value.get('metadata'), f'{name} metadata')
    if (metadata.get('name') != name or
        (namespace is not None and metadata.get('namespace') != namespace) or
            metadata.get('deletionTimestamp') is not None):
        raise KubernetesAttestationError(
            f'Kubernetes object {name!r} is not the exact current object.')
    return metadata


def _require_active(value: Mapping[str, Any],
                    *,
                    name: str,
                    namespace: str | None = None) -> None:
    metadata = _metadata(value, name=name, namespace=namespace)
    generation = metadata.get('generation')
    status = _dict(value.get('status'), f'{name} status')
    conditions = _list(status.get('conditions'), f'{name} conditions')
    active = [
        condition for condition in conditions
        if isinstance(condition, Mapping) and condition.get('type') == 'Active'
    ]
    if (len(active) != 1 or active[0].get('status') != 'True' or
            generation is None or
            active[0].get('observedGeneration') != generation):
        raise KubernetesAttestationError(
            f'Kueue object {name!r} is not current and Active.')


def _preemption(spec: Mapping[str, Any]) -> dict[str, str]:
    raw = _dict(spec.get('preemption'), 'ClusterQueue preemption')
    borrow = raw.get('borrowWithinCohort')
    if isinstance(borrow, Mapping):
        borrow = borrow.get('policy')
    result = {
        'borrow_within_cohort': borrow,
        'reclaim_within_cohort': raw.get('reclaimWithinCohort'),
        'within_cluster_queue': raw.get('withinClusterQueue'),
    }
    if not all(isinstance(value, str) for value in result.values()):
        raise KubernetesAttestationError(
            'ClusterQueue preemption policy is incomplete.')
    return result  # type: ignore[return-value]


def _resource_groups(spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Normalize the complete strict ClusterQueue resource topology."""
    result: list[dict[str, Any]] = []
    groups = _list(spec.get('resourceGroups'), 'ClusterQueue resource groups')
    covered_inventory: set[str] = set()
    group_signatures: set[frozenset[str]] = set()
    quota_atoms: set[tuple[str, str]] = set()
    for group in groups:
        group_mapping = _dict(group, 'ClusterQueue resource group')
        if set(group_mapping) != {'coveredResources', 'flavors'}:
            raise KubernetesAttestationError(
                'ClusterQueue resource group schema is not exact.')
        covered_resources = _list(group_mapping.get('coveredResources'),
                                  'ClusterQueue covered resources')
        if (not covered_resources or any(not isinstance(item, str) or not item
                                         for item in covered_resources) or
                len(covered_resources) != len(set(covered_resources))):
            raise KubernetesAttestationError(
                'ClusterQueue covered resources are invalid.')
        signature = frozenset(covered_resources)
        if (signature in group_signatures or
                covered_inventory.intersection(signature)):
            raise KubernetesAttestationError(
                'ClusterQueue has duplicate or overlapping resource groups.')
        group_signatures.add(signature)
        covered_inventory.update(signature)
        normalized_flavors: list[dict[str, Any]] = []
        flavors = _list(group_mapping.get('flavors'), 'ClusterQueue flavors')
        if not flavors:
            raise KubernetesAttestationError(
                'ClusterQueue resource group has no flavors.')
        flavor_names: set[str] = set()
        for flavor in flavors:
            flavor_mapping = _dict(flavor, 'ClusterQueue flavor')
            if set(flavor_mapping) != {'name', 'resources'}:
                raise KubernetesAttestationError(
                    'ClusterQueue flavor schema is not exact.')
            flavor_name = flavor_mapping.get('name')
            if not isinstance(flavor_name, str) or not flavor_name:
                raise KubernetesAttestationError(
                    'ClusterQueue flavor name is invalid.')
            if flavor_name in flavor_names:
                raise KubernetesAttestationError(
                    'ClusterQueue has duplicate flavors in one group.')
            flavor_names.add(flavor_name)
            normalized_resources: list[dict[str, str]] = []
            resource_names: set[str] = set()
            for resource in _list(flavor_mapping.get('resources'),
                                  'ClusterQueue flavor resources'):
                resource_mapping = _dict(resource,
                                         'ClusterQueue flavor resource')
                if set(resource_mapping) != {
                        'name', 'nominalQuota', 'borrowingLimit'
                }:
                    raise KubernetesAttestationError(
                        'ClusterQueue quota atom schema is not exact.')
                resource_name = resource_mapping.get('name')
                nominal = resource_mapping.get('nominalQuota')
                borrowing = resource_mapping.get('borrowingLimit')
                if (not isinstance(resource_name, str) or not resource_name or
                        not isinstance(nominal, str) or not nominal or
                        not isinstance(borrowing, str) or not borrowing):
                    raise KubernetesAttestationError(
                        'ClusterQueue quota atom is incomplete.')
                atom = (flavor_name, resource_name)
                if resource_name in resource_names or atom in quota_atoms:
                    raise KubernetesAttestationError(
                        'ClusterQueue has duplicate quota atoms.')
                resource_names.add(resource_name)
                quota_atoms.add(atom)
                normalized_resources.append({
                    'resource_name': resource_name,
                    'nominal_quota': nominal,
                    'borrowing_limit': borrowing,
                })
            if resource_names != signature:
                raise KubernetesAttestationError(
                    'ClusterQueue flavor does not cover its exact resource '
                    'group.')
            normalized_resources.sort(key=lambda item: item['resource_name'])
            normalized_flavors.append({
                'name': flavor_name,
                'resources': normalized_resources,
            })
        normalized_flavors.sort(key=lambda item: item['name'])
        result.append({
            'covered_resources': sorted(covered_resources),
            'flavors': normalized_flavors,
        })
    result.sort(key=lambda item: tuple(item['covered_resources']))
    return result


def _validate_cluster_queue(
        value: Mapping[str, Any], *, name: str, namespace: str, cohort: str,
        expected_preemption: Mapping[str, str],
        expected_resource_groups: list[Mapping[str, Any]]) -> None:
    _require_active(value, name=name)
    spec = _dict(value.get('spec'), f'{name} spec')
    expected_selector = {
        'matchLabels': {
            'kubernetes.io/metadata.name': namespace,
        }
    }
    if (spec.get('cohortName') != cohort or
            spec.get('namespaceSelector') != expected_selector or
            spec.get('stopPolicy') not in (None, 'None') or
            _preemption(spec) != dict(expected_preemption) or
            _resource_groups(spec)
            != [dict(item) for item in expected_resource_groups]):
        raise KubernetesAttestationError(
            f'ClusterQueue {name!r} does not match the reviewed reclaim '
            'contract.')


def _validate_deployment(value: Mapping[str, Any], *, name: str, namespace: str,
                         replicas: int, expected_images: Mapping[str,
                                                                 str]) -> None:
    metadata = _metadata(value, name=name, namespace=namespace)
    spec = _dict(value.get('spec'), f'{name} deployment spec')
    status = _dict(value.get('status'), f'{name} deployment status')
    template = _dict(spec.get('template'), f'{name} Pod template')
    pod_spec = _dict(template.get('spec'), f'{name} Pod spec')
    observed_images: dict[str, str] = {}
    for container in _list(pod_spec.get('containers'), f'{name} containers'):
        container_mapping = _dict(container, f'{name} container')
        container_name = container_mapping.get('name')
        image = container_mapping.get('image')
        if (not isinstance(container_name, str) or not container_name or
                not isinstance(image, str) or not image or
                container_name in observed_images):
            raise KubernetesAttestationError(
                f'Deployment {name!r} has an invalid container inventory.')
        observed_images[container_name] = image
    if (spec.get('replicas') != replicas or
            status.get('observedGeneration') != metadata.get('generation') or
            status.get('readyReplicas') != replicas or
            status.get('availableReplicas') != replicas or
            status.get('updatedReplicas') != replicas or
            status.get('unavailableReplicas') not in (None, 0) or
            observed_images != dict(expected_images)):
        raise KubernetesAttestationError(
            f'Deployment {namespace}/{name} is not the exact ready inventory.')


def _manager_config(config_map: Mapping[str, Any]) -> Mapping[str, Any]:
    data = _dict(config_map.get('data'), 'Kueue manager ConfigMap data')
    candidates: list[Mapping[str, Any]] = []
    for encoded in data.values():
        if not isinstance(encoded, str):
            continue
        try:
            decoded = yaml.safe_load(encoded)
        except Exception as error:  # pylint: disable=broad-except
            raise KubernetesAttestationError(
                'Kueue manager configuration is invalid YAML.') from error
        if (isinstance(decoded, Mapping) and
            ('integrations' in decoded or 'featureGates' in decoded)):
            candidates.append(decoded)
    if len(candidates) != 1:
        raise KubernetesAttestationError(
            'Kueue manager configuration is missing or ambiguous.')
    return candidates[0]


def _normalized_cel(value: object, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KubernetesAttestationError(
            f'Kubernetes returned invalid {path} data.')
    return ' '.join(value.split())


def _resource_rule_values(value: object,
                          path: str,
                          *,
                          allow_empty_string: bool = False) -> tuple[str, ...]:
    items = _list(value, path)
    if (not items or any(
            not isinstance(item, str) or (not allow_empty_string and not item)
            for item in items) or len(set(items)) != len(items)):
        raise KubernetesAttestationError(
            'The queue-name admission policy resource rules are not exact.')
    return tuple(sorted(items))


def _resource_rule_signature(value: object,
                             path: str) -> _ResourceRuleSignature:
    rule = _dict(value, path)
    if set(rule) != {
            'apiGroups', 'apiVersions', 'operations', 'resources', 'scope'
    }:
        raise KubernetesAttestationError(
            'The queue-name admission policy resource rules are not exact.')
    scope = rule.get('scope')
    if not isinstance(scope, str) or not scope:
        raise KubernetesAttestationError(
            'The queue-name admission policy resource rules are not exact.')
    return (
        _resource_rule_values(rule.get('apiGroups'),
                              f'{path} apiGroups',
                              allow_empty_string=True),
        _resource_rule_values(rule.get('apiVersions'), f'{path} apiVersions'),
        _resource_rule_values(rule.get('operations'), f'{path} operations'),
        _resource_rule_values(rule.get('resources'), f'{path} resources'),
        scope,
    )


def _validate_admission_policy(policy: Mapping[str, Any], binding: Mapping[str,
                                                                           Any],
                               *, name: str, binding_name: str, label_key: str,
                               label_value: str) -> None:
    _metadata(policy, name=name)
    policy_spec = _dict(policy.get('spec'), f'{name} policy spec')
    if 'matchConditions' in policy_spec:
        _list(policy_spec.get('matchConditions'), f'{name} match conditions')
        raise KubernetesAttestationError(
            'The queue-name admission policy must define no match conditions.')
    if set(policy_spec) != {
            'failurePolicy', 'variables', 'matchConstraints', 'validations'
    }:
        raise KubernetesAttestationError(
            'The queue-name admission policy schema is not exact.')
    constraints = _dict(policy_spec.get('matchConstraints'),
                        f'{name} match constraints')
    if (set(constraints) != {
            'matchPolicy', 'namespaceSelector', 'objectSelector',
            'resourceRules'
    } or constraints.get('matchPolicy') != 'Equivalent' or
            constraints.get('namespaceSelector') != {} or
            constraints.get('objectSelector') != {}):
        raise KubernetesAttestationError(
            'The queue-name admission policy match scope is not exact.')
    rules = _list(constraints.get('resourceRules'), f'{name} resource rules')
    observed_rules = {
        _resource_rule_signature(rule, f'{name} resource rule {index}')
        for index, rule in enumerate(rules)
    }
    if (len(observed_rules) != len(rules) or
            observed_rules != _QUEUE_NAME_RESOURCE_RULES):
        raise KubernetesAttestationError(
            'The queue-name admission policy resource rules are not exact.')

    variables = _list(policy_spec.get('variables'), f'{name} variables')
    observed_variables: dict[str, str] = {}
    for index, raw_variable in enumerate(variables):
        variable = _dict(raw_variable, f'{name} variable {index}')
        variable_name = variable.get('name')
        if (set(variable) != {'name', 'expression'} or
                not isinstance(variable_name, str) or not variable_name or
                variable_name in observed_variables):
            raise KubernetesAttestationError(
                'The queue-name admission policy variables are not exact.')
        observed_variables[variable_name] = _normalized_cel(
            variable.get('expression'), f'{name} {variable_name} expression')
    reviewed_variables = {
        variable_name:
            _normalized_cel(expression, f'{name} reviewed {variable_name}')
        for variable_name, expression in _QUEUE_NAME_VARIABLES.items()
    }
    if observed_variables != reviewed_variables:
        raise KubernetesAttestationError(
            'The queue-name admission policy variables are not exact.')

    validations = _list(policy_spec.get('validations'), f'{name} validations')
    if len(validations) != 1:
        raise KubernetesAttestationError(
            'The queue-name admission policy validation is not exact.')
    validation = _dict(validations[0], f'{name} validation')
    if (set(validation) != {'expression', 'message'} or _normalized_cel(
            validation.get('expression'),
            f'{name} validation expression') != _normalized_cel(
                _QUEUE_NAME_VALIDATION, f'{name} reviewed validation') or
            _normalized_cel(validation.get('message'),
                            f'{name} validation message') != _normalized_cel(
                                _QUEUE_NAME_VALIDATION_MESSAGE,
                                f'{name} reviewed validation message') or
            policy_spec.get('failurePolicy') != 'Fail'):
        raise KubernetesAttestationError(
            'The queue-name admission policy validation is not exact and '
            'fail closed.')

    _metadata(binding, name=binding_name)
    binding_spec = _dict(binding.get('spec'), f'{binding_name} binding spec')
    match_resources = _dict(binding_spec.get('matchResources'),
                            f'{binding_name} match resources')
    selector = _dict(match_resources.get('namespaceSelector'),
                     f'{binding_name} namespace selector')
    if (set(binding_spec)
            != {'policyName', 'validationActions', 'matchResources'} or
            set(match_resources)
            != {'matchPolicy', 'namespaceSelector', 'objectSelector'} or
            binding_spec.get('policyName') != name or
            binding_spec.get('validationActions') != ['Deny'] or
            match_resources.get('matchPolicy') != 'Equivalent' or
            match_resources.get('objectSelector') != {} or selector != {
                'matchLabels': {
                    label_key: label_value
                }
            }):
        raise KubernetesAttestationError(
            'The queue-name admission-policy binding is not exact and Deny.')


def _validate_webhook(value: Mapping[str, Any], *, contract: Mapping[str, Any],
                      service_name: str, service_port: int, namespace: str,
                      mutating: bool) -> None:
    configuration_name = contract['configuration_name']
    _metadata(value, name=configuration_name)
    webhooks = _list(value.get('webhooks'), f'{configuration_name} webhooks')
    matching = []
    for webhook in webhooks:
        webhook_mapping = _dict(webhook, f'{configuration_name} webhook')
        if webhook_mapping.get('name') == contract['webhook_name']:
            matching.append(webhook_mapping)
    if len(matching) != 1:
        raise KubernetesAttestationError(
            f'Webhook configuration {configuration_name!r} does not contain '
            'one exact Kueue Pod admission webhook.')

    webhook = matching[0]
    client_config = _dict(webhook.get('clientConfig'),
                          f'{configuration_name} Pod webhook client')
    service = _dict(client_config.get('service'),
                    f'{configuration_name} Pod webhook service')
    expected_service = {
        'name': service_name,
        'namespace': namespace,
        'path': contract['path'],
        'port': service_port,
    }
    ca_bundle = client_config.get('caBundle')
    expected_selector = {
        'matchExpressions': [{
            'key': 'kubernetes.io/metadata.name',
            'operator': 'NotIn',
            'values': ['kube-system', namespace],
        }]
    }
    expected_rule = [{
        'apiGroups': [''],
        'apiVersions': ['v1'],
        'operations': contract['operations'],
        'resources': ['pods'],
        'scope': '*',
    }]
    expected_reinvocation = 'Never' if mutating else None
    if (service != expected_service or client_config.get('url') is not None or
            type(ca_bundle) is not str or not ca_bundle or
            webhook.get('failurePolicy') != 'Fail' or
            webhook.get('matchPolicy') != 'Equivalent' or
            webhook.get('sideEffects') != 'None' or
            webhook.get('timeoutSeconds') != 10 or
            webhook.get('admissionReviewVersions') != ['v1'] or
            webhook.get('reinvocationPolicy') != expected_reinvocation or
            webhook.get('namespaceSelector') != expected_selector or
            webhook.get('objectSelector') != {} or
            webhook.get('matchConditions') not in (None, []) or
            webhook.get('rules') != expected_rule):
        raise KubernetesAttestationError(
            f'Webhook configuration {configuration_name!r} does not route '
            'the exact fail-closed Pod admission contract to Kueue.')


def _validate_node_inventory(
        provider_context: Mapping[str, Any],
        snapshot: Mapping[str, Any]) -> tuple[NodeFlavorProof, ...]:
    observed_lists = _dict(snapshot.get('nodes'), 'Node inventory')
    expected = {
        item['flavor']: item for item in provider_context['node_inventory']
    }
    if set(observed_lists) != set(expected):
        raise KubernetesAttestationError(
            'The Node inventory does not cover every reviewed flavor.')
    proofs: list[NodeFlavorProof] = []
    for flavor in sorted(expected):
        contract = expected[flavor]
        node_list = _dict(observed_lists[flavor], f'{flavor} NodeList')
        nodes = _list(node_list.get('items'), f'{flavor} Nodes')
        names: set[str] = set()
        non_deleting_count = 0
        for raw_node in nodes:
            node = _dict(raw_node, f'{flavor} Node')
            metadata = _dict(node.get('metadata'), f'{flavor} Node metadata')
            name = metadata.get('name')
            if (not isinstance(name, str) or not name or name in names):
                raise KubernetesAttestationError(
                    f'The {flavor!r} Node inventory contains an invalid '
                    'identity.')
            names.add(name)
            labels = _dict(metadata.get('labels'), f'{name} Node labels')
            if labels.get(contract['selector_label_key']
                         ) != contract['selector_label_value']:
                raise KubernetesAttestationError(
                    f'Node {name!r} does not match the reviewed instance '
                    'selector.')
            if metadata.get('deletionTimestamp') is not None:
                continue
            capacity = _dict(
                _dict(node.get('status'),
                      f'{name} Node status').get('capacity'),
                f'{name} Node capacity')
            if (labels.get(contract['product_label_key'])
                    != contract['product_label_value'] or
                    capacity.get(contract['resource_name']) != str(
                        contract['capacity_per_node'])):
                raise KubernetesAttestationError(
                    f'Node {name!r} does not match the reviewed GPU product '
                    'and capacity contract.')
            non_deleting_count += 1
        if non_deleting_count == 0:
            raise KubernetesAttestationError(
                f'Flavor {flavor!r} has no non-deleting Node.')
        proofs.append(
            NodeFlavorProof(flavor=flavor,
                            non_deleting_node_count=non_deleting_count,
                            product_label_value=contract['product_label_value'],
                            resource_name=contract['resource_name'],
                            capacity_per_node=contract['capacity_per_node']))
    return tuple(proofs)


def _validate_kueue_snapshot(fleet_context: Mapping[str, Any],
                             provider_context: Mapping[str, Any],
                             snapshot: Mapping[str, Any]) -> tuple[str, str]:
    admission = _dict(fleet_context.get('kueue_admission'),
                      'Kueue admission contract')
    enforcement = _dict(provider_context.get('kueue_enforcement'),
                        'Kueue enforcement contract')
    namespace_name = fleet_context['namespace']
    workload_priority = _dict(snapshot.get('workload_priority_class'),
                              'WorkloadPriorityClass')
    workload_priority_name = admission['workload_priority_class_name']
    _metadata(workload_priority, name=workload_priority_name)
    if workload_priority.get('value') != admission['workload_priority_value']:
        raise KubernetesAttestationError(
            'The WorkloadPriorityClass reclaim contract is invalid.')

    queue_contract = _dict(admission['queues'], 'Kueue queue contract')
    local_queue_name = admission['local_queue_name']
    local_queue = _dict(snapshot.get('local_queue'), 'LocalQueue')
    _require_active(local_queue,
                    name=local_queue_name,
                    namespace=namespace_name)
    local_queue_spec = _dict(local_queue.get('spec'), 'LocalQueue spec')
    cluster_queue_name = queue_contract['inference_cluster_queue']
    if (local_queue_spec.get('clusterQueue') != cluster_queue_name or
            local_queue_spec.get('stopPolicy') not in (None, 'None')):
        raise KubernetesAttestationError(
            'The inference LocalQueue target is invalid.')
    _validate_cluster_queue(
        _dict(snapshot.get('inference_cluster_queue'),
              'inference ClusterQueue'),
        name=cluster_queue_name,
        namespace=namespace_name,
        cohort=queue_contract['cohort'],
        expected_preemption=queue_contract['inference_preemption'],
        expected_resource_groups=(queue_contract['inference_resource_groups']))
    _validate_cluster_queue(
        _dict(snapshot.get('research_cluster_queue'), 'research ClusterQueue'),
        name=queue_contract['research_cluster_queue'],
        namespace=queue_contract['research_namespace'],
        cohort=queue_contract['cohort'],
        expected_preemption=queue_contract['research_preemption'],
        expected_resource_groups=queue_contract['research_resource_groups'])

    controller = _dict(enforcement['controller'], 'Kueue controller contract')
    _validate_deployment(_dict(snapshot.get('kueue_controller'),
                               'Kueue controller Deployment'),
                         name=controller['deployment'],
                         namespace=controller['namespace'],
                         replicas=controller['replicas'],
                         expected_images=controller['images'])
    config_map = _dict(snapshot.get('kueue_config'), 'Kueue ConfigMap')
    _metadata(config_map,
              name=controller['config_map'],
              namespace=controller['namespace'])
    manager_config = _manager_config(config_map)
    integrations = _dict(manager_config.get('integrations'),
                         'Kueue integrations')
    frameworks = _list(integrations.get('frameworks'),
                       'Kueue integration frameworks')
    feature_gates = _dict(manager_config.get('featureGates'),
                          'Kueue feature gates')
    required_feature_gates = _dict(controller['required_feature_gates'],
                                   'required Kueue feature gates')
    if ('pod' not in frameworks or any(
            feature_gates.get(name) is not enabled
            for name, enabled in required_feature_gates.items())):
        raise KubernetesAttestationError(
            'Kueue Pod integration or a required feature gate is disabled.')

    admission_policy = _dict(enforcement['admission_policy'],
                             'admission policy contract')
    _validate_admission_policy(
        _dict(snapshot.get('admission_policy'), 'admission policy'),
        _dict(snapshot.get('admission_policy_binding'),
              'admission policy binding'),
        name=admission_policy['name'],
        binding_name=admission_policy['binding_name'],
        label_key=admission_policy['namespace_label_key'],
        label_value=admission_policy['namespace_label_value'])
    webhooks = _dict(enforcement['webhooks'], 'Kueue webhook contract')
    _validate_webhook(_dict(snapshot.get('validating_webhook'),
                            'validating webhook'),
                      contract=webhooks['validating'],
                      service_name=webhooks['service_name'],
                      service_port=webhooks['service_port'],
                      namespace=controller['namespace'],
                      mutating=False)
    _validate_webhook(_dict(snapshot.get('mutating_webhook'),
                            'mutating webhook'),
                      contract=webhooks['mutating'],
                      service_name=webhooks['service_name'],
                      service_port=webhooks['service_port'],
                      namespace=controller['namespace'],
                      mutating=True)
    return local_queue_name, cluster_queue_name


def validate_snapshot(fleet_context: Mapping[str, Any],
                      provider_context: Mapping[str, Any],
                      snapshot: Mapping[str, Any]) -> KubernetesContextProof:
    """Validate one serialized set of exact API reads."""
    namespace_name = fleet_context['namespace']
    namespace = _dict(snapshot.get('namespace'), 'Namespace')
    namespace_metadata = _metadata(namespace, name=namespace_name)
    kueue_admission = fleet_context['kueue_admission']
    kueue_enforcement = provider_context['kueue_enforcement']
    managed = kueue_admission is not None
    if managed != (kueue_enforcement is not None):
        raise KubernetesAttestationError(
            'Kueue admission and enforcement contracts disagree.')
    labels = _dict(namespace_metadata.get('labels', {}), 'Namespace labels')
    if (namespace_metadata.get('uid') != provider_context['namespace_uid'] or
            _dict(namespace.get('status'),
                  'Namespace status').get('phase') != 'Active'):
        raise KubernetesAttestationError(
            'The inference Namespace identity is invalid.')
    if managed:
        enforcement = _dict(kueue_enforcement, 'Kueue enforcement contract')
        admission_contract = _dict(enforcement['admission_policy'],
                                   'Kueue admission policy contract')
        if labels.get(admission_contract['namespace_label_key']) != (
                admission_contract['namespace_label_value']):
            raise KubernetesAttestationError(
                'The inference Namespace managed admission label is invalid.')
    elif _KUEUE_MANAGED_LABEL in labels:
        raise KubernetesAttestationError(
            'The unmanaged inference Namespace carries the managed Kueue '
            'label.')

    service_account = _dict(snapshot.get('service_account'), 'ServiceAccount')
    service_account_metadata = _metadata(
        service_account,
        name=fleet_context['service_account_name'],
        namespace=namespace_name)
    annotations = _dict(service_account_metadata.get('annotations', {}),
                        'ServiceAccount annotations')
    if _IRSA_ANNOTATION in annotations:
        raise KubernetesAttestationError(
            'The worker ServiceAccount has an unreviewed IRSA identity.')

    priority = _dict(snapshot.get('priority_class'), 'PriorityClass')
    _metadata(priority, name=fleet_context['priority_class']['name'])
    if (priority.get('value') != fleet_context['priority_class']['value'] or
            priority.get('globalDefault') not in (None, False) or
            priority.get('preemptionPolicy')
            != fleet_context['priority_class']['preemption_policy']):
        raise KubernetesAttestationError(
            'The Pod PriorityClass reclaim contract is invalid.')
    local_queue_name: str | None = None
    cluster_queue_name: str | None = None
    if managed:
        local_queue_name, cluster_queue_name = _validate_kueue_snapshot(
            fleet_context, provider_context, snapshot)

    observed_flavors = _dict(snapshot.get('resource_flavors'),
                             'ResourceFlavor inventory')
    expected_flavors = {
        item['name']: item for item in provider_context['resource_flavors']
    }
    if set(observed_flavors) != set(expected_flavors):
        raise KubernetesAttestationError(
            'The ResourceFlavor inventory is incomplete.')
    topology_names: list[tuple[str, str | None]] = []
    for name, expected_contract in expected_flavors.items():
        flavor = _dict(observed_flavors[name], f'ResourceFlavor {name}')
        _metadata(flavor, name=name)
        spec = _dict(flavor.get('spec'), f'ResourceFlavor {name} spec')
        labels = _dict(spec.get('nodeLabels'),
                       f'ResourceFlavor {name} node labels')
        expected_labels = expected_contract['node_labels']
        if any(
                labels.get(key) != value
                for key, value in expected_labels.items()) or spec.get(
                    'topologyName') != expected_contract['topology_name']:
            raise KubernetesAttestationError(
                f'ResourceFlavor {name!r} does not bind the reviewed '
                'provider-owned instance selector and topology.')
        topology_names.append((name, expected_contract['topology_name']))
    node_flavors = _validate_node_inventory(provider_context, snapshot)

    scheduler = provider_context['scheduler']
    if scheduler is not None:
        _validate_deployment(_dict(snapshot.get('scheduler'),
                                   'scheduler Deployment'),
                             name=scheduler['deployment'],
                             namespace=scheduler['namespace'],
                             replicas=scheduler['replicas'],
                             expected_images=scheduler['containers'])
    return KubernetesContextProof(
        kubernetes_context=fleet_context['kubernetes_context'],
        physical_cluster_uid=fleet_context['physical_cluster_uid'],
        namespace_uid=provider_context['namespace_uid'],
        kueue_managed=managed,
        local_queue_name=local_queue_name,
        cluster_queue_name=cluster_queue_name,
        pod_identity_irsa_annotation_absent=True,
        assign_queue_labels_for_pods=True if managed else None,
        topology_aware_scheduling=True if managed else None,
        custom_scheduler_deployment_proven=scheduler is not None,
        resource_flavor_topology_names=tuple(sorted(topology_names)),
        node_flavors=node_flavors)


def _serialized(client: Any, value: object) -> Mapping[str, Any]:
    serialized = client.sanitize_for_serialization(value)
    return _dict(serialized, 'serialized API response')


def _get_kueue_object(custom: Any,
                      *,
                      plural: str,
                      name: str,
                      namespace: str | None = None) -> Mapping[str, Any]:
    last_error: Exception | None = None
    for version in _KUEUE_API_VERSIONS:
        try:
            if namespace is None:
                return custom.get_cluster_custom_object(
                    group=_KUEUE_GROUP,
                    version=version,
                    plural=plural,
                    name=name,
                    _request_timeout=kubernetes_adaptor.API_TIMEOUT)
            return custom.get_namespaced_custom_object(
                group=_KUEUE_GROUP,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name,
                _request_timeout=kubernetes_adaptor.API_TIMEOUT)
        except kubernetes_adaptor.api_exception() as error:
            last_error = error
            if getattr(error, 'status', None) != 404:
                raise
    if last_error is not None:
        raise last_error
    raise KubernetesAttestationError('No supported Kueue API version exists.')


def attest_context(fleet_context: Mapping[str,
                                          Any], provider_context: Mapping[str,
                                                                          Any],
                   *, deadline_monotonic: float, cancellation: threading.Event,
                   audit_session: Any) -> KubernetesContextProof:
    """Read and validate one context inside an exact physical-UID fence."""
    expected_uid = fleet_context['physical_cluster_uid']
    with kubernetes_adaptor.api_call_deadline(deadline_monotonic, cancellation):
        with _audit_api_client(provider_context,
                               audit_session,
                               deadline_monotonic=deadline_monotonic,
                               cancellation=cancellation) as client:
            _require_physical_cluster_uid(client, expected_uid)
            core = kubernetes_adaptor.kubernetes.client.CoreV1Api(
                api_client=client)
            custom = kubernetes_adaptor.kubernetes.client.CustomObjectsApi(
                api_client=client)
            apps = kubernetes_adaptor.kubernetes.client.AppsV1Api(
                api_client=client)
            scheduling = kubernetes_adaptor.kubernetes.client.SchedulingV1Api(
                api_client=client)
            namespace = fleet_context['namespace']
            scheduler = provider_context['scheduler']
            snapshot: dict[str, Any] = {
                'namespace': _serialized(
                    client,
                    core.read_namespace(
                        namespace,
                        _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                'service_account': _serialized(
                    client,
                    core.read_namespaced_service_account(
                        fleet_context['service_account_name'],
                        namespace,
                        _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                'priority_class': _serialized(
                    client,
                    scheduling.read_priority_class(
                        fleet_context['priority_class']['name'],
                        _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                'resource_flavors': {
                    flavor['name']: _get_kueue_object(custom,
                                                      plural='resourceflavors',
                                                      name=flavor['name'])
                    for flavor in provider_context['resource_flavors']
                },
                'nodes': {
                    node['flavor']: _serialized(
                        client,
                        core.list_node(
                            label_selector=(f"{node['selector_label_key']}="
                                            f"{node['selector_label_value']}"),
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT))
                    for node in provider_context['node_inventory']
                },
            }
            if scheduler is not None:
                snapshot['scheduler'] = _serialized(
                    client,
                    apps.read_namespaced_deployment(
                        scheduler['deployment'],
                        scheduler['namespace'],
                        _request_timeout=kubernetes_adaptor.API_TIMEOUT))
            raw_kueue_admission = fleet_context['kueue_admission']
            if raw_kueue_admission is not None:
                kueue_admission = _dict(raw_kueue_admission,
                                        'Kueue admission contract')
                queues = _dict(kueue_admission['queues'],
                               'Kueue queue contract')
                enforcement = _dict(provider_context['kueue_enforcement'],
                                    'Kueue enforcement contract')
                controller = _dict(enforcement['controller'],
                                   'Kueue controller contract')
                policy = _dict(enforcement['admission_policy'],
                               'Kueue admission policy contract')
                webhooks = _dict(enforcement['webhooks'],
                                 'Kueue webhook contract')
                admission_api = (kubernetes_adaptor.kubernetes.client.
                                 AdmissionregistrationV1Api(api_client=client))
                snapshot.update({
                    'workload_priority_class': _get_kueue_object(
                        custom,
                        plural='workloadpriorityclasses',
                        name=(kueue_admission['workload_priority_class_name'])),
                    'local_queue': _get_kueue_object(
                        custom,
                        plural='localqueues',
                        name=kueue_admission['local_queue_name'],
                        namespace=namespace),
                    'inference_cluster_queue': _get_kueue_object(
                        custom,
                        plural='clusterqueues',
                        name=queues['inference_cluster_queue']),
                    'research_cluster_queue': _get_kueue_object(
                        custom,
                        plural='clusterqueues',
                        name=queues['research_cluster_queue']),
                    'kueue_controller': _serialized(
                        client,
                        apps.read_namespaced_deployment(
                            controller['deployment'],
                            controller['namespace'],
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                    'kueue_config': _serialized(
                        client,
                        core.read_namespaced_config_map(
                            controller['config_map'],
                            controller['namespace'],
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                    'admission_policy': _serialized(
                        client,
                        admission_api.read_validating_admission_policy(
                            policy['name'],
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                    'admission_policy_binding': _serialized(
                        client,
                        admission_api.read_validating_admission_policy_binding(
                            policy['binding_name'],
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                    'validating_webhook': _serialized(
                        client,
                        admission_api.read_validating_webhook_configuration(
                            webhooks['validating']['configuration_name'],
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                    'mutating_webhook': _serialized(
                        client,
                        admission_api.read_mutating_webhook_configuration(
                            webhooks['mutating']['configuration_name'],
                            _request_timeout=kubernetes_adaptor.API_TIMEOUT)),
                })
            proof = validate_snapshot(fleet_context, provider_context, snapshot)
            kubernetes_adaptor.raise_if_api_call_deadline_exceeded()
            return proof
