"""Exact Kubernetes/Kueue enforcement attestation for reserved fill."""

import base64
from collections.abc import Callable
from collections.abc import Mapping
import contextlib
import dataclasses
import math
import threading
import time
from typing import Any, Iterator

from sky.adaptors import common as adaptor_common

aws_adaptor = adaptor_common.LazyImport('sky.adaptors.aws')
kubernetes_adaptor = adaptor_common.LazyImport('sky.adaptors.kubernetes')
kueue_admission_lib = adaptor_common.LazyImport(
    'sky.provision.kubernetes.kueue_admission')
yaml = adaptor_common.LazyImport('yaml')
botocore_credentials = adaptor_common.LazyImport('botocore.credentials')
botocore_signers = adaptor_common.LazyImport('botocore.signers')

_KUEUE_GROUP = 'kueue.x-k8s.io'
_KUEUE_API_VERSIONS = ('v1beta2', 'v1beta1')
_IRSA_ANNOTATION = 'eks.amazonaws.com/role-arn'
_EKS_TOKEN_PREFIX = 'k8s-aws-v1.'
_EKS_TOKEN_TTL_SECONDS = 60


class KubernetesAttestationError(RuntimeError):
    """Kubernetes did not prove the exact reclaim enforcement topology."""


class KubernetesAttestationNonconformanceError(KubernetesAttestationError):
    """Complete Kubernetes reads disproved the reclaim topology."""


class KubernetesAttestationIndeterminateError(KubernetesAttestationError):
    """A Kubernetes response could not be interpreted as a complete fact."""


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


def _request_timeout(
    deadline_monotonic: float,
    cancellation: threading.Event,
) -> tuple[float, float]:
    """Return the generated client's explicit connect/read timeout pair."""
    timeout = _client_timeout(deadline_monotonic, cancellation)
    return timeout, timeout


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
    # The generated client otherwise installs urllib3's default retry policy.
    # Every provider-proof read is a single attempt bounded by the exact
    # absolute deadline; the next daemon round owns retry.
    configuration.retries = 0
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


def _require_physical_cluster_uid(
    client: Any,
    expected_uid: str,
    *,
    deadline_monotonic: float,
    cancellation: threading.Event,
) -> None:
    try:
        namespace = kubernetes_adaptor.kubernetes.client.CoreV1Api(
            api_client=client).read_namespace('kube-system',
                                              _request_timeout=_request_timeout(
                                                  deadline_monotonic,
                                                  cancellation))
    except kubernetes_adaptor.api_exception() as error:
        if getattr(error, 'status', None) == 404:
            raise KubernetesAttestationNonconformanceError(
                'The physical-cluster identity Namespace is absent.') from error
        raise
    metadata = getattr(namespace, 'metadata', None)
    observed_uid = getattr(metadata, 'uid', None)
    if observed_uid != expected_uid:
        raise KubernetesAttestationNonconformanceError(
            'Kubernetes physical-cluster identity changed before attestation.')


def _dict(value: object, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KubernetesAttestationIndeterminateError(
            f'Kubernetes returned invalid {path} data.')
    return value


def _list(value: object, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise KubernetesAttestationIndeterminateError(
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


def _validate_external_cluster_queue(
        value: Mapping[str, Any], *, name: str, namespace: str,
        namespace_labels: Mapping[str, str]) -> None:
    """Prove only the existing external lane needed for safe submission."""
    _require_active(value, name=name)
    spec = _dict(value.get('spec'), f'{name} spec')
    namespace_selector = spec.get('namespaceSelector')
    if (not kueue_admission_lib.namespace_matches_selector(
            namespace_selector, namespace_labels) or
            spec.get('stopPolicy') not in (None, 'None')):
        raise KubernetesAttestationError(
            f'ClusterQueue {name!r} does not admit the external namespace '
            f'{namespace!r}.')


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
    resource_flavors = {
        item['name']: item for item in provider_context['resource_flavors']
    }
    if set(observed_lists) != set(expected):
        raise KubernetesAttestationError(
            'The Node inventory does not cover every reviewed flavor.')
    proofs: list[NodeFlavorProof] = []
    for flavor in sorted(expected):
        contract = expected[flavor]
        selector_labels = _dict(resource_flavors[flavor]['node_labels'],
                                f'{flavor} ResourceFlavor node labels')
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
            if metadata.get('deletionTimestamp') is not None:
                continue
            if any(
                    labels.get(key) != value
                    for key, value in selector_labels.items()):
                # The API read was issued with this exact selector.  A
                # non-deleting out-of-selector item is therefore a malformed
                # completed inventory, not evidence that the flavor is
                # currently empty.
                raise KubernetesAttestationError(
                    f'Node {name!r} does not match the complete reviewed '
                    'ResourceFlavor instance selector.')
            capacity = _dict(
                _dict(node.get('status'),
                      f'{name} Node status').get('capacity'),
                f'{name} Node capacity')
            if (labels.get(contract['product_label_key']) !=
                    contract['product_label_value'] or
                    capacity.get(contract['resource_name']) != str(
                        contract['capacity_per_node'])):
                raise KubernetesAttestationError(
                    f'Node {name!r} does not match the reviewed GPU product '
                    'and capacity contract.')
            non_deleting_count += 1
        proofs.append(
            NodeFlavorProof(flavor=flavor,
                            non_deleting_node_count=non_deleting_count,
                            product_label_value=contract['product_label_value'],
                            resource_name=contract['resource_name'],
                            capacity_per_node=contract['capacity_per_node']))
    return tuple(proofs)


def _validate_kueue_snapshot(
        fleet_context: Mapping[str, Any], provider_context: Mapping[str, Any],
        snapshot: Mapping[str, Any],
        namespace_labels: Mapping[str, str]) -> tuple[str, str]:
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

    local_queue_name = admission['local_queue_name']
    local_queue = _dict(snapshot.get('local_queue'), 'LocalQueue')
    _require_active(local_queue,
                    name=local_queue_name,
                    namespace=namespace_name)
    local_queue_spec = _dict(local_queue.get('spec'), 'LocalQueue spec')
    cluster_queue_name = admission['cluster_queue_name']
    if (local_queue_spec.get('clusterQueue') != cluster_queue_name or
            local_queue_spec.get('stopPolicy') not in (None, 'None')):
        raise KubernetesAttestationError(
            'The inference LocalQueue target is invalid.')
    _validate_external_cluster_queue(_dict(snapshot.get('cluster_queue'),
                                           'ClusterQueue'),
                                     name=cluster_queue_name,
                                     namespace=namespace_name,
                                     namespace_labels=namespace_labels)

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


def _validate_snapshot(fleet_context: Mapping[str, Any],
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
    if (namespace_metadata.get('uid') != provider_context['namespace_uid'] or
            _dict(namespace.get('status'),
                  'Namespace status').get('phase') != 'Active'):
        raise KubernetesAttestationError(
            'The inference Namespace identity is invalid.')

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
            priority.get('preemptionPolicy') !=
            fleet_context['priority_class']['preemption_policy']):
        raise KubernetesAttestationError(
            'The Pod PriorityClass reclaim contract is invalid.')
    local_queue_name: str | None = None
    cluster_queue_name: str | None = None
    if managed:
        namespace_labels = _dict(namespace_metadata.get('labels', {}),
                                 'Namespace labels')
        local_queue_name, cluster_queue_name = _validate_kueue_snapshot(
            fleet_context, provider_context, snapshot, namespace_labels)

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
        expected_spec_keys = {'nodeLabels'}
        if expected_contract['topology_name'] is not None:
            expected_spec_keys.add('topologyName')
        if (set(spec) != expected_spec_keys or
                labels != expected_contract['node_labels'] or
                spec.get('topologyName') != expected_contract['topology_name']):
            raise KubernetesAttestationError(
                f'ResourceFlavor {name!r} does not have the exact reviewed '
                'provider-owned instance selector and topology spec.')
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


def validate_snapshot(fleet_context: Mapping[str, Any],
                      provider_context: Mapping[str, Any],
                      snapshot: Mapping[str, Any]) -> KubernetesContextProof:
    """Classify a complete snapshot mismatch separately from malformed I/O."""
    try:
        return _validate_snapshot(fleet_context, provider_context, snapshot)
    except KubernetesAttestationIndeterminateError:
        raise
    except KubernetesAttestationNonconformanceError:
        raise
    except KubernetesAttestationError as error:
        raise KubernetesAttestationNonconformanceError(
            'The completed Kubernetes inventory does not match the reviewed '
            f'reclaim topology: {error}') from error


def _serialized(client: Any, value: object) -> Mapping[str, Any]:
    serialized = client.sanitize_for_serialization(value)
    return _dict(serialized, 'serialized API response')


def _get_kueue_object(custom: Any,
                      *,
                      plural: str,
                      name: str,
                      deadline_monotonic: float,
                      cancellation: threading.Event,
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
                    _request_timeout=_request_timeout(deadline_monotonic,
                                                      cancellation))
            return custom.get_namespaced_custom_object(
                group=_KUEUE_GROUP,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name,
                _request_timeout=_request_timeout(deadline_monotonic,
                                                  cancellation))
        except kubernetes_adaptor.api_exception() as error:
            last_error = error
            if getattr(error, 'status', None) != 404:
                raise
    if last_error is not None:
        raise KubernetesAttestationNonconformanceError(
            f'The required Kueue object {name!r} does not exist in a '
            'supported API version.') from last_error
    raise KubernetesAttestationError('No supported Kueue API version exists.')


def _read_required(read: Callable[[], Any], *, subject: str) -> Any:
    """Classify an authenticated 404 as a completed negative observation."""
    try:
        return read()
    except kubernetes_adaptor.api_exception() as error:
        if getattr(error, 'status', None) == 404:
            raise KubernetesAttestationNonconformanceError(
                f'The required Kubernetes {subject} does not exist.') from error
        raise


def _resource_flavor_node_selector(provider_context: Mapping[str, Any],
                                   flavor: str) -> str:
    """Return the complete reviewed ResourceFlavor selector for a Node list."""
    flavors = {
        item['name']: item for item in provider_context['resource_flavors']
    }
    labels = _dict(flavors[flavor]['node_labels'],
                   f'{flavor} ResourceFlavor node labels')
    return ','.join(f'{key}={labels[key]}' for key in sorted(labels))


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
            _require_physical_cluster_uid(client,
                                          expected_uid,
                                          deadline_monotonic=deadline_monotonic,
                                          cancellation=cancellation)
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
                    _read_required(lambda: core.read_namespace(
                        namespace,
                        _request_timeout=_request_timeout(
                            deadline_monotonic, cancellation)),
                                   subject=f'Namespace {namespace!r}')),
                'service_account': _serialized(
                    client,
                    _read_required(lambda: core.read_namespaced_service_account(
                        fleet_context['service_account_name'],
                        namespace,
                        _request_timeout=_request_timeout(
                            deadline_monotonic, cancellation)),
                                   subject='ServiceAccount')),
                'priority_class': _serialized(
                    client,
                    _read_required(lambda: scheduling.read_priority_class(
                        fleet_context['priority_class']['name'],
                        _request_timeout=_request_timeout(
                            deadline_monotonic, cancellation)),
                                   subject='Pod PriorityClass')),
                'resource_flavors': {
                    flavor['name']:
                    _get_kueue_object(custom,
                                      plural='resourceflavors',
                                      name=flavor['name'],
                                      deadline_monotonic=(deadline_monotonic),
                                      cancellation=(cancellation))
                    for flavor in provider_context['resource_flavors']
                },
                'nodes': {
                    node['flavor']: _serialized(
                        client,
                        core.list_node(
                            label_selector=_resource_flavor_node_selector(
                                provider_context, node['flavor']),
                            _request_timeout=_request_timeout(
                                deadline_monotonic, cancellation)))
                    for node in provider_context['node_inventory']
                },
            }
            if scheduler is not None:
                snapshot['scheduler'] = _serialized(
                    client,
                    _read_required(lambda: apps.read_namespaced_deployment(
                        scheduler['deployment'],
                        scheduler['namespace'],
                        _request_timeout=_request_timeout(
                            deadline_monotonic, cancellation)),
                                   subject='scheduler Deployment'))
            raw_kueue_admission = fleet_context['kueue_admission']
            if raw_kueue_admission is not None:
                kueue_admission = _dict(raw_kueue_admission,
                                        'Kueue admission contract')
                enforcement = _dict(provider_context['kueue_enforcement'],
                                    'Kueue enforcement contract')
                controller = _dict(enforcement['controller'],
                                   'Kueue controller contract')
                webhooks = _dict(enforcement['webhooks'],
                                 'Kueue webhook contract')
                admission_api = (kubernetes_adaptor.kubernetes.client.
                                 AdmissionregistrationV1Api(api_client=client))
                snapshot.update({
                    'workload_priority_class': _get_kueue_object(
                        custom,
                        plural='workloadpriorityclasses',
                        name=(kueue_admission['workload_priority_class_name']),
                        deadline_monotonic=deadline_monotonic,
                        cancellation=cancellation),
                    'local_queue': _get_kueue_object(
                        custom,
                        plural='localqueues',
                        name=kueue_admission['local_queue_name'],
                        deadline_monotonic=deadline_monotonic,
                        cancellation=cancellation,
                        namespace=namespace),
                    'cluster_queue': _get_kueue_object(
                        custom,
                        plural='clusterqueues',
                        name=kueue_admission['cluster_queue_name'],
                        deadline_monotonic=deadline_monotonic,
                        cancellation=cancellation),
                    'kueue_controller': _serialized(
                        client,
                        _read_required(lambda: apps.read_namespaced_deployment(
                            controller['deployment'],
                            controller['namespace'],
                            _request_timeout=_request_timeout(
                                deadline_monotonic, cancellation)),
                                       subject='Kueue controller Deployment')),
                    'kueue_config': _serialized(
                        client,
                        _read_required(lambda: core.read_namespaced_config_map(
                            controller['config_map'],
                            controller['namespace'],
                            _request_timeout=_request_timeout(
                                deadline_monotonic, cancellation)),
                                       subject='Kueue controller ConfigMap')),
                    'validating_webhook': _serialized(
                        client,
                        _read_required(
                            lambda: admission_api.
                            read_validating_webhook_configuration(
                                webhooks['validating']['configuration_name'],
                                _request_timeout=_request_timeout(
                                    deadline_monotonic, cancellation)),
                            subject='Kueue validating webhook')),
                    'mutating_webhook': _serialized(
                        client,
                        _read_required(
                            lambda: admission_api.
                            read_mutating_webhook_configuration(
                                webhooks['mutating']['configuration_name'],
                                _request_timeout=_request_timeout(
                                    deadline_monotonic, cancellation)),
                            subject='Kueue mutating webhook')),
                })
            proof = validate_snapshot(fleet_context, provider_context, snapshot)
            kubernetes_adaptor.raise_if_api_call_deadline_exceeded()
            return proof
