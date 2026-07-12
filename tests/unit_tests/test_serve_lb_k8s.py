"""Logic tests for the controller-owned external LB lifecycle."""
# pylint: disable=protected-access
import os
import re
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.serve import constants
from sky.serve import lb_k8s

_RFC1123 = re.compile(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?$')
_DIGEST_A = f'sha256:{"a" * 64}'
_DIGEST_B = f'sha256:{"b" * 64}'
_DEPLOYMENT_PATCH_PATH = (
    '/apis/apps/v1/namespaces/{namespace}/deployments/{name}')
_SERVICE_PATCH_PATH = '/api/v1/namespaces/{namespace}/services/{name}'


class _ApiException(Exception):

    def __init__(self, status):
        super().__init__(f'status={status}')
        self.status = status


def _owner_reference(owner_name='skypilot-api-server',
                     owner_uid='api-deployment-uid'):
    return SimpleNamespace(api_version='apps/v1',
                           kind='Deployment',
                           name=owner_name,
                           uid=owner_uid,
                           controller=False,
                           block_owner_deletion=False)


def _owned_object(service_hash='incarnation-a',
                  uid='uid-a',
                  rv='7',
                  service_name='svc',
                  object_name=None,
                  owner_name='skypilot-api-server',
                  owner_uid='api-deployment-uid'):
    return SimpleNamespace(metadata=SimpleNamespace(
        labels={
            lb_k8s.SERVE_LB_LABEL_KEY: service_name,
            lb_k8s.SERVICE_HASH_LABEL_KEY: service_hash,
        },
        name=object_name,
        uid=uid,
        resource_version=rv,
        owner_references=[_owner_reference(owner_name, owner_uid)]))


def _volume(name, secret):
    return {
        'name': name,
        'projected': {
            'sources': [{
                'secret': {
                    'name': secret,
                    'items': [{
                        'key': 'tokens',
                        'path': 'tokens'
                    }],
                }
            }]
        },
    }


def _mount(name, path):
    return {'name': name, 'mountPath': path, 'readOnly': True}


def _patch_calls(patch_api, resource_path):
    return [
        call for call in patch_api.call_api.call_args_list
        if call.args[:2] == (resource_path, 'PATCH')
    ]


def _install(monkeypatch,
             *,
             apps_api=None,
             core_api=None,
             external=True,
             incluster=True,
             namespace='skypilot',
             pod_name='api-pod-0',
             pod_namespace='skypilot',
             image='repo/skypilot:moving',
             image_policy='Always',
             image_id=f'repo/skypilot@{_DIGEST_A}',
             pod_security_context=None,
             container_security_context=None,
             resources=None,
             node_selector=None,
             tolerations=None,
             affinity=None,
             runtime_class_name=None,
             priority_class_name=None,
             scheduler_name=None,
             image_pull_secrets=({
                 'name': 'registry-credentials'
             },),
             data_auth=True,
             wait_for_endpoint=False,
             api_deployment_name='skypilot-api-server',
             api_deployment_uid='api-deployment-uid',
             release_name='skypilot',
             db_service_names=(),
             patch_api=None):
    monkeypatch.setattr(lb_k8s.serve_utils, 'is_external_load_balancer_mode',
                        lambda: external)
    monkeypatch.setattr(lb_k8s.kubernetes_utils,
                        'is_incluster_config_available', lambda: incluster)
    monkeypatch.setattr(lb_k8s.kubernetes_utils,
                        'get_kube_config_context_namespace',
                        lambda unused_context: namespace)
    monkeypatch.setattr(lb_k8s.kubernetes, 'in_cluster_context_name',
                        lambda: 'in-cluster')
    monkeypatch.setattr(lb_k8s.kubernetes, 'api_exception',
                        lambda: _ApiException)
    monkeypatch.setattr(lb_k8s.serve_utils,
                        'get_lb_sync_auth_tokens',
                        lambda required=False: ('sync-current', 'sync-old'))
    monkeypatch.setattr(lb_k8s.serve_utils,
                        'get_controller_admin_auth_tokens',
                        lambda required=False: ('admin-current',))
    monkeypatch.setattr(lb_k8s.serve_utils,
                        'get_lb_auth_tokens',
                        lambda required=False: ('data-current', 'data-old'))

    api_service_url = 'http://sky-api.skypilot.svc.cluster.local'
    lb_sync_tokens_file = '/etc/skypilot/serve-auth/lb-sync/tokens'
    controller_admin_tokens_file = (
        '/etc/skypilot/serve-auth/controller-admin/tokens')
    controller_admin_env_var = (
        constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR)
    env = {
        'SKYPILOT_SERVE_API_SERVICE_URL': api_service_url,
        constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR: lb_sync_tokens_file,
        controller_admin_env_var: controller_admin_tokens_file,
        constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR: str(data_auth).lower(),
    }
    if data_auth:
        env[constants.LB_AUTH_TOKENS_FILE_ENV_VAR] = (
            '/etc/skypilot/serve-auth/lb-data-plane/tokens')
    else:
        monkeypatch.delenv(constants.LB_AUTH_TOKENS_FILE_ENV_VAR, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    if pod_name is None:
        monkeypatch.delenv(constants.POD_NAME_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(constants.POD_NAME_ENV_VAR, pod_name)
    if api_deployment_name is None:
        monkeypatch.delenv(constants.API_DEPLOYMENT_NAME_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(constants.API_DEPLOYMENT_NAME_ENV_VAR,
                           api_deployment_name)
    if release_name is None:
        monkeypatch.delenv(constants.RELEASE_NAME_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(constants.RELEASE_NAME_ENV_VAR, release_name)
    if pod_namespace is None:
        monkeypatch.delenv(constants.POD_NAMESPACE_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(constants.POD_NAMESPACE_ENV_VAR, pod_namespace)

    apps_api = apps_api or mock.MagicMock()
    core_api = core_api or mock.MagicMock()
    patch_api = patch_api or mock.MagicMock()
    effective_api_deployment_name = (
        api_deployment_name or
        (f'{release_name}-api-server' if release_name else None))
    read_service = core_api.read_namespaced_service
    if (read_service.side_effect is None and
            isinstance(read_service.return_value, mock.MagicMock)):
        read_service.return_value = SimpleNamespace(
            metadata=SimpleNamespace(
                resource_version='lb-service-rv',
                annotations={},
                labels={
                    lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                    lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
                },
                owner_references=[
                    SimpleNamespace(api_version='apps/v1',
                                    kind='Deployment',
                                    name=effective_api_deployment_name,
                                    uid=api_deployment_uid,
                                    controller=False,
                                    block_owner_deletion=False)
                ]),
            spec=SimpleNamespace(
                type='LoadBalancer',
                selector={'app': lb_k8s.lb_deployment_name('svc')},
                ports=[
                    SimpleNamespace(
                        port=constants.LOAD_BALANCER_PORT_START,
                        target_port=constants.LOAD_BALANCER_PORT_START,
                        protocol='TCP')
                ]),
            status=SimpleNamespace(load_balancer=SimpleNamespace(
                ingress=[SimpleNamespace(hostname='lb.example', ip=None)])))
    read_deployment = apps_api.read_namespaced_deployment
    original_side_effect = read_deployment.side_effect
    existing_deployment = read_deployment.return_value
    if isinstance(existing_deployment, mock.MagicMock):
        existing_deployment = SimpleNamespace(metadata=SimpleNamespace(
            generation=1,
            resource_version='lb-deployment-rv',
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[
                SimpleNamespace(api_version='apps/v1',
                                kind='Deployment',
                                name=effective_api_deployment_name,
                                uid=api_deployment_uid,
                                controller=False,
                                block_owner_deletion=False)
            ]),
                                              spec=SimpleNamespace(replicas=1),
                                              status=SimpleNamespace(
                                                  observed_generation=1,
                                                  updated_replicas=1,
                                                  available_replicas=1,
                                                  unavailable_replicas=0))

    def _read_deployment(name, unused_namespace):
        if name == effective_api_deployment_name:
            return SimpleNamespace(metadata=SimpleNamespace(
                uid=api_deployment_uid))
        if original_side_effect is None:
            return existing_deployment
        if isinstance(original_side_effect, BaseException):
            raise original_side_effect
        if callable(original_side_effect):
            return original_side_effect(name, unused_namespace)
        result = next(original_side_effect)
        if isinstance(result, BaseException):
            raise result
        return result

    read_deployment.side_effect = _read_deployment
    monkeypatch.setattr(lb_k8s.kubernetes,
                        'apps_api',
                        lambda unused_context=None: apps_api)
    monkeypatch.setattr(lb_k8s.kubernetes,
                        'core_api',
                        lambda unused_context=None: core_api)
    monkeypatch.setattr(lb_k8s.kubernetes,
                        'api_client',
                        lambda unused_context=None: patch_api)
    if not wait_for_endpoint:
        monkeypatch.setattr(lb_k8s, '_wait_for_lb_service_endpoint',
                            lambda *unused_args, **unused_kwargs: None)

    volume_mounts = [
        _mount(lb_k8s.LB_SYNC_AUTH_VOLUME_NAME,
               '/etc/skypilot/serve-auth/lb-sync'),
        _mount('skypilot-serve-controller-admin-auth',
               '/etc/skypilot/serve-auth/controller-admin'),
    ]
    volumes = [
        _volume(lb_k8s.LB_SYNC_AUTH_VOLUME_NAME, 'sync-secret'),
        _volume('skypilot-serve-controller-admin-auth', 'admin-secret'),
    ]
    if data_auth:
        volume_mounts.append(
            _mount(lb_k8s.LB_DATA_PLANE_AUTH_VOLUME_NAME,
                   '/etc/skypilot/serve-auth/lb-data-plane'))
        volumes.append(
            _volume(lb_k8s.LB_DATA_PLANE_AUTH_VOLUME_NAME, 'data-secret'))
    container = SimpleNamespace(image=image,
                                image_pull_policy=image_policy,
                                security_context=container_security_context,
                                resources=resources,
                                volume_mounts=volume_mounts)
    status = SimpleNamespace(image_id=image_id)
    pod = SimpleNamespace(
        spec=SimpleNamespace(containers=[container],
                             security_context=pod_security_context,
                             node_selector=node_selector,
                             tolerations=tolerations,
                             affinity=affinity,
                             runtime_class_name=runtime_class_name,
                             priority_class_name=priority_class_name,
                             scheduler_name=scheduler_name,
                             image_pull_secrets=list(image_pull_secrets),
                             volumes=volumes),
        status=SimpleNamespace(container_statuses=[status] if image_id else []))
    core_api.read_namespaced_pod.return_value = pod

    live = set(db_service_names)
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_from_name', lambda name: {
            'name': name,
            'controller_pid': os.getpid(),
            'hash': 'incarnation',
        } if name in live else None)
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_controller_owner', lambda name: {
            'controller_pid': os.getpid(),
            'controller_ip': None,
            'hash': 'incarnation',
            'resource_scope': None,
            'status': None,
            'controller_port': None,
            'lifecycle_epoch': None,
        } if name in live else None)
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_hash',
                        lambda name: 'incarnation' if name in live else None)
    monkeypatch.setattr(lb_k8s.serve_state,
                        'service_owner_matches',
                        lambda name, service_hash, owner=None:
                        (name in live and service_hash == 'incarnation' and
                         (owner is None or owner == (os.getpid(), None))))
    return apps_api, core_api


def test_name_helpers_are_unique_rfc1123():
    names = ['my-service', 'My_Service', 'svc-a', 'svc_a', 'x' * 200, '___']
    rendered = [lb_k8s.lb_base_name(name) for name in names]
    assert len(set(rendered)) == len(rendered)
    assert all(
        _RFC1123.fullmatch(name) and len(name) <= 63 for name in rendered)


def test_lb_names_are_incarnation_scoped():
    legacy = lb_k8s.lb_base_name('svc')
    incarnation_a = lb_k8s.lb_base_name('svc', 'hash-a')
    incarnation_b = lb_k8s.lb_base_name('svc', 'hash-b')
    assert len({legacy, incarnation_a, incarnation_b}) == 3
    assert all(
        _RFC1123.fullmatch(name) and len(name) <= 63
        for name in (incarnation_a, incarnation_b))


def test_external_runtime_fails_closed(monkeypatch):
    _install(monkeypatch, external=False)
    with pytest.raises(RuntimeError, match='external load balancer'):
        lb_k8s.require_external_lb_runtime()

    _install(monkeypatch, incluster=False)
    with pytest.raises(RuntimeError, match='in-cluster'):
        lb_k8s.require_external_lb_runtime()


@pytest.mark.parametrize(('external', 'incluster'), [(False, True),
                                                     (True, False)])
def test_lb_service_endpoint_unavailable_without_external_runtime(
        monkeypatch, external, incluster):
    _install(monkeypatch, external=external, incluster=incluster)
    assert lb_k8s.lb_service_endpoint_or_none('svc') is None


def test_load_balancer_service_requires_data_plane_auth(monkeypatch):
    _install(monkeypatch, data_auth=False)
    with pytest.raises(RuntimeError, match='require.*lbDataPlane'):
        lb_k8s.require_external_lb_runtime()


def test_load_balancer_endpoint_resolves_published_hostname(monkeypatch):
    core = mock.MagicMock()
    core.read_namespaced_service.return_value = SimpleNamespace(
        status=SimpleNamespace(load_balancer=SimpleNamespace(
            ingress=[SimpleNamespace(hostname='lb.example', ip=None)])))
    _install(monkeypatch, core_api=core)

    assert lb_k8s.lb_service_endpoint_or_none('svc') == 'lb.example:30001'
    core.read_namespaced_service.assert_called_once_with(
        lb_k8s.lb_service_name('svc'), 'skypilot')


def test_load_balancer_endpoint_is_unavailable_before_publication(monkeypatch):
    core = mock.MagicMock()
    core.read_namespaced_service.return_value = {
        'status': {
            'loadBalancer': {
                'ingress': []
            }
        }
    }
    _install(monkeypatch, core_api=core)
    assert lb_k8s.lb_service_endpoint_or_none('svc') is None


def test_load_balancer_endpoint_brackets_ipv6():
    service = {'status': {'loadBalancer': {'ingress': [{'ip': 'fd00::1'}]}}}
    assert lb_k8s._service_load_balancer_address(service) == '[fd00::1]'


def test_external_runtime_requires_projected_files(monkeypatch):
    _install(monkeypatch)
    monkeypatch.delenv(constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR)
    with pytest.raises(RuntimeError, match='projected Secret'):
        lb_k8s.require_external_lb_runtime()


def test_external_runtime_requires_pod_namespace(monkeypatch):
    _install(monkeypatch, pod_namespace=None)
    with pytest.raises(RuntimeError, match=constants.POD_NAMESPACE_ENV_VAR):
        lb_k8s.require_external_lb_runtime()


def test_external_runtime_requires_owner_or_release_name(monkeypatch):
    _install(monkeypatch, api_deployment_name=None, release_name=None)
    with pytest.raises(RuntimeError, match=constants.RELEASE_NAME_ENV_VAR):
        lb_k8s.require_external_lb_runtime()


def test_legacy_release_name_supports_preflight_and_owner_resolution(
        monkeypatch):
    apps, _ = _install(monkeypatch,
                       api_deployment_name=None,
                       release_name='legacy-release')

    lb_k8s.require_external_lb_runtime()
    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    assert deployment['metadata']['ownerReferences'][0][
        'name'] == 'legacy-release-api-server'
    owner_reads = [
        call.args[:2]
        for call in apps.read_namespaced_deployment.call_args_list
        if call.args[0] == 'legacy-release-api-server'
    ]
    assert owner_reads == [('legacy-release-api-server', 'skypilot')]


def test_explicit_api_deployment_name_precedes_legacy_release_name(monkeypatch):
    apps, _ = _install(monkeypatch,
                       api_deployment_name='explicit-api-owner',
                       release_name='legacy-release')

    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    assert deployment['metadata']['ownerReferences'][0][
        'name'] == 'explicit-api-owner'
    assert apps.read_namespaced_deployment.call_args_list[0].args[:2] == (
        'explicit-api-owner', 'skypilot')


def test_create_builds_proxy_deployment_and_service(monkeypatch):
    apps, core = _install(monkeypatch)
    lb_k8s.create_lb_deployment_and_service('svc-a', 225, 'incarnation')

    namespace, deployment = apps.create_namespaced_deployment.call_args.args
    assert namespace == 'skypilot'
    expected_owner = [{
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'name': 'skypilot-api-server',
        'uid': 'api-deployment-uid',
        'controller': False,
        'blockOwnerDeletion': False,
    }]
    assert deployment['metadata']['ownerReferences'] == expected_owner
    pod_spec = deployment['spec']['template']['spec']
    container = pod_spec['containers'][0]
    assert container['image'] == f'repo/skypilot@{_DIGEST_A}'
    assert pod_spec['automountServiceAccountToken'] is False
    assert pod_spec['imagePullSecrets'] == [{'name': 'registry-credentials'}]
    args = container['args']
    controller_addr = args[args.index('--controller-addr') + 1]
    assert controller_addr == (
        'http://sky-api.skypilot.svc.cluster.local/api/internal/serve/svc-a')
    assert '10.' not in controller_addr
    assert ':200' not in controller_addr
    assert args[args.index('--service-hash') + 1] == 'incarnation'
    assert pod_spec['terminationGracePeriodSeconds'] == 225
    assert container['startupProbe']['httpGet'][
        'path'] == constants.LB_LIVENESS_ENDPOINT_PATH
    assert container['livenessProbe']['httpGet'][
        'path'] == constants.LB_LIVENESS_ENDPOINT_PATH
    assert container['readinessProbe']['httpGet'][
        'path'] == constants.LB_HEALTH_ENDPOINT_PATH

    env = {entry['name']: entry for entry in container['env']}
    assert constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR in env
    assert constants.LB_AUTH_TOKENS_FILE_ENV_VAR in env
    assert constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR not in env
    assert env[constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR]['value'] == 'true'
    assert env[constants.EXTERNAL_LB_ENABLED_ENV_VAR]['value'] == 'true'
    assert env[constants.LB_POD_UID_ENV_VAR]['valueFrom']['fieldRef'][
        'fieldPath'] == 'metadata.uid'

    volume_names = {volume['name'] for volume in pod_spec['volumes']}
    assert volume_names == {
        lb_k8s.LB_SYNC_AUTH_VOLUME_NAME,
        lb_k8s.LB_DATA_PLANE_AUTH_VOLUME_NAME,
    }
    serialized = repr(deployment)
    assert 'admin-secret' not in serialized
    assert 'sync-current' not in serialized
    assert 'data-current' not in serialized

    _, service = core.create_namespaced_service.call_args.args
    assert service['metadata']['ownerReferences'] == expected_owner
    assert service['spec']['type'] == 'LoadBalancer'
    assert service['spec']['ports'][0]['port'] == \
        constants.LOAD_BALANCER_PORT_START


def test_create_builds_provider_default_load_balancer_and_waits(monkeypatch):
    core = mock.MagicMock()
    core.read_namespaced_service.return_value = {
        'status': {
            'loadBalancer': {
                'ingress': [{
                    'hostname': 'lb.example'
                }]
            }
        }
    }
    _install(monkeypatch, core_api=core, wait_for_endpoint=True)

    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    _, service = core.create_namespaced_service.call_args.args
    assert 'annotations' not in service['metadata']
    assert service['spec']['type'] == 'LoadBalancer'
    assert 'loadBalancerSourceRanges' not in service['spec']
    core.read_namespaced_service.assert_called_once_with(
        lb_k8s.lb_service_name('svc'), 'skypilot')


def test_create_times_out_until_load_balancer_endpoint_is_published(
        monkeypatch):
    core = mock.MagicMock()
    core.read_namespaced_service.return_value = {
        'status': {
            'loadBalancer': {
                'ingress': []
            }
        }
    }
    _install(monkeypatch, core_api=core, wait_for_endpoint=True)
    clock = [0.0]

    def _advance_clock(seconds):
        clock[0] += seconds

    monkeypatch.setattr(lb_k8s.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(lb_k8s.time, 'sleep', _advance_clock)
    monkeypatch.setattr(constants, 'LB_SERVICE_ENDPOINT_READY_TIMEOUT_SECONDS',
                        2)
    with pytest.raises(RuntimeError, match='did not publish an endpoint'):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')


def test_create_mirrors_only_safe_nonroot_volume_access(monkeypatch):
    apps, core = _install(monkeypatch,
                          pod_security_context={
                              'runAsUser': 10001,
                              'runAsGroup': 10001,
                              'fsGroup': 10001,
                              'fsGroupChangePolicy': 'OnRootMismatch',
                          },
                          container_security_context={
                              'runAsUser': 10001,
                              'runAsNonRoot': True,
                              'allowPrivilegeEscalation': False,
                              'readOnlyRootFilesystem': True,
                              'privileged': True,
                              'capabilities': {
                                  'add': ['SYS_ADMIN'],
                                  'drop': ['ALL']
                              },
                          },
                          resources={
                              'requests': {
                                  'cpu': '250m',
                                  'memory': '256Mi'
                              },
                              'limits': {
                                  'cpu': '1',
                                  'memory': '1Gi'
                              },
                          })
    # Host/service-account identity belongs to the API Pod and must not leak
    # into the lower-trust data-plane Pod even when present on the source.
    source_spec = core.read_namespaced_pod.return_value.spec
    source_spec.host_network = True
    source_spec.host_pid = True
    source_spec.service_account_name = 'api-admin'

    lb_k8s.create_lb_deployment_and_service('svc-a', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    pod_spec = deployment['spec']['template']['spec']
    container = pod_spec['containers'][0]
    assert pod_spec['securityContext'] == {
        'runAsUser': 10001,
        'runAsGroup': 10001,
        'fsGroup': 10001,
        'fsGroupChangePolicy': 'OnRootMismatch',
    }
    assert container['securityContext'] == {
        'runAsUser': 10001,
        'runAsNonRoot': True,
        'allowPrivilegeEscalation': False,
        'readOnlyRootFilesystem': True,
        'capabilities': {
            'drop': ['ALL']
        },
    }
    assert container['resources'] == lb_k8s._DEFAULT_LB_RESOURCES
    assert 'privileged' not in container['securityContext']
    assert 'add' not in container['securityContext']['capabilities']
    assert pod_spec['automountServiceAccountToken'] is False
    assert 'hostNetwork' not in pod_spec
    assert 'hostPID' not in pod_spec
    assert 'serviceAccountName' not in pod_spec


def test_create_mirrors_tainted_pool_and_runtime_scheduling(monkeypatch):
    affinity = {
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'pool',
                        'operator': 'In',
                        'values': ['control-plane'],
                    }]
                }]
            }
        }
    }
    tolerations = [{
        'key': 'dedicated',
        'operator': 'Equal',
        'value': 'control-plane',
        'effect': 'NoSchedule',
    }]
    apps, _ = _install(monkeypatch,
                       node_selector={'pool': 'control-plane'},
                       tolerations=tolerations,
                       affinity=affinity,
                       runtime_class_name='gvisor',
                       priority_class_name='platform-critical',
                       scheduler_name='custom-scheduler')

    lb_k8s.create_lb_deployment_and_service('svc-a', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    pod_spec = deployment['spec']['template']['spec']
    assert pod_spec['nodeSelector'] == {'pool': 'control-plane'}
    assert pod_spec['tolerations'] == tolerations
    assert 'affinity' not in pod_spec
    assert pod_spec['runtimeClassName'] == 'gvisor'
    assert 'priorityClassName' not in pod_spec
    assert pod_spec['schedulerName'] == 'custom-scheduler'


def test_api_pod_namespace_wins_over_workload_context(monkeypatch):
    apps, core = _install(monkeypatch,
                          namespace='workloads',
                          pod_namespace='control-plane')
    lb_k8s.create_lb_deployment_and_service('svc-a', 225, 'incarnation')

    assert apps.create_namespaced_deployment.call_args.args[
        0] == 'control-plane'
    assert core.create_namespaced_service.call_args.args[0] == 'control-plane'
    assert core.read_namespaced_pod.call_args_list[0].args[1] == 'control-plane'
    assert lb_k8s.lb_service_endpoint_or_none('svc-a') == 'lb.example:30001'


def test_image_pull_secret_refs_are_name_only(monkeypatch):
    apps, _ = _install(monkeypatch,
                       image_pull_secrets=({
                           'name': 'registry-credentials',
                           'unexpected': 'must-not-propagate',
                       },))
    lb_k8s.create_lb_deployment_and_service('svc-a', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    pod_spec = deployment['spec']['template']['spec']
    assert pod_spec['imagePullSecrets'] == [{'name': 'registry-credentials'}]
    assert 'must-not-propagate' not in repr(deployment)


def test_controller_owner_change_does_not_change_lb_template(monkeypatch):
    apps, _ = _install(monkeypatch)
    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')
    first = apps.create_namespaced_deployment.call_args.args[1]
    monkeypatch.setenv('POD_IP', '10.99.0.5')
    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')
    second = apps.create_namespaced_deployment.call_args.args[1]
    assert first['spec']['template'] == second['spec']['template']


def test_rollout_readiness_rejects_old_available_surge_pod():
    deployment = SimpleNamespace(metadata=SimpleNamespace(generation=2),
                                 spec=SimpleNamespace(replicas=1),
                                 status=SimpleNamespace(observed_generation=2,
                                                        replicas=2,
                                                        updated_replicas=1,
                                                        available_replicas=1,
                                                        unavailable_replicas=0))
    assert not lb_k8s._lb_deployment_is_ready(deployment)


def test_create_409_reconciles_exactly_owned_deployment(monkeypatch):
    apps = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _install(monkeypatch, apps_api=apps, patch_api=patch_api)
    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')
    apps.patch_namespaced_deployment.assert_not_called()
    patch_api.call_api.assert_called_once()
    deployment_patch_call = patch_api.call_api.call_args
    assert deployment_patch_call.args == (_DEPLOYMENT_PATCH_PATH, 'PATCH')
    assert deployment_patch_call.kwargs['path_params'] == {
        'name': lb_k8s.lb_deployment_name('svc'),
        'namespace': 'skypilot',
    }
    assert deployment_patch_call.kwargs['header_params']['Content-Type'] == (
        'application/strategic-merge-patch+json')
    assert deployment_patch_call.kwargs['response_type'] == 'V1Deployment'
    patched = deployment_patch_call.kwargs['body']
    args = patched['spec']['template']['spec']['containers'][0]['args']
    assert '/api/internal/serve/svc' in args[1]
    assert patched['metadata']['resourceVersion'] == 'lb-deployment-rv'
    assert 'ownerReferences' not in patched['metadata']


def test_create_409_reconciles_exactly_owned_objects_without_adoption(
        monkeypatch):
    apps = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            generation=1,
            resource_version='deployment-rv',
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(replicas=1),
        status=SimpleNamespace(observed_generation=1,
                               updated_replicas=1,
                               available_replicas=1,
                               unavailable_replicas=0))
    core = mock.MagicMock()
    core.create_namespaced_service.side_effect = _ApiException(409)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='service-rv',
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(
            selector={
                'app': lb_k8s.lb_deployment_name('svc'),
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            ports=[
                SimpleNamespace(port=constants.LOAD_BALANCER_PORT_START,
                                target_port=constants.LOAD_BALANCER_PORT_START,
                                protocol='TCP')
            ]))
    _install(monkeypatch, apps_api=apps, core_api=core, patch_api=patch_api)

    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    deployment_reconciles = _patch_calls(patch_api, _DEPLOYMENT_PATCH_PATH)
    assert len(deployment_reconciles) == 1
    deployment_reconcile = deployment_reconciles[0].kwargs['body']
    assert 'spec' in deployment_reconcile
    assert 'ownerReferences' not in deployment_reconcile['metadata']
    assert deployment_reconcile['metadata'][
        'resourceVersion'] == 'deployment-rv'

    core.patch_namespaced_service.assert_not_called()
    service_reconciles = _patch_calls(patch_api, _SERVICE_PATCH_PATH)
    assert len(service_reconciles) == 1
    service_reconcile_call = service_reconciles[0]
    assert service_reconcile_call.kwargs['header_params']['Content-Type'] == (
        'application/strategic-merge-patch+json')
    assert service_reconcile_call.kwargs['response_type'] == 'V1Service'
    service_reconcile = service_reconcile_call.kwargs['body']
    assert service_reconcile['spec']['type'] == 'LoadBalancer'
    assert service_reconcile['spec']['selector'][
        lb_k8s.SERVICE_HASH_LABEL_KEY] == 'incarnation'
    assert service_reconcile['metadata']['resourceVersion'] == 'service-rv'
    assert 'ownerReferences' not in service_reconcile['metadata']


def test_create_refuses_reconcile_without_resource_version(monkeypatch):
    apps = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(labels={
            lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
        },
                                 owner_references=[_owner_reference()]))
    _, core = _install(monkeypatch, apps_api=apps, patch_api=patch_api)

    with pytest.raises(RuntimeError, match='no resourceVersion'):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    patch_api.call_api.assert_not_called()
    core.create_namespaced_service.assert_not_called()


def test_create_refuses_reconcile_when_api_deployment_uid_changes(monkeypatch):
    apps = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _, core = _install(monkeypatch, apps_api=apps, patch_api=patch_api)
    monkeypatch.setattr(lb_k8s, '_live_deployment_owner_uid',
                        lambda *unused_args: 'replacement-api-uid')

    with pytest.raises(RuntimeError, match='changed from UID'):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    patch_api.call_api.assert_not_called()
    core.create_namespaced_service.assert_not_called()


@pytest.mark.parametrize(('owner_references', 'labels', 'error'), [
    ([], {
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation'
    }, 'not owned exactly'),
    ([_owner_reference(owner_uid='stale-api-uid')], {
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation'
    }, 'not owned exactly'),
    ([_owner_reference()], {
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'another-incarnation'
    }, 'service incarnation label'),
])
def test_create_refuses_foreign_deployment_collision(monkeypatch,
                                                     owner_references, labels,
                                                     error):
    apps = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='lb-rv',
                                 labels=labels,
                                 owner_references=owner_references))
    _, core = _install(monkeypatch, apps_api=apps, patch_api=patch_api)

    with pytest.raises(RuntimeError, match=error):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    patch_api.call_api.assert_not_called()
    core.create_namespaced_service.assert_not_called()


@pytest.mark.parametrize(('owner_references', 'labels', 'error'), [
    ([], {
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation'
    }, 'not owned exactly'),
    ([_owner_reference(owner_uid='stale-api-uid')], {
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation'
    }, 'not owned exactly'),
    ([_owner_reference()], {
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'another-incarnation'
    }, 'service incarnation label'),
])
def test_create_refuses_foreign_service_collision(monkeypatch, owner_references,
                                                  labels, error):
    core = mock.MagicMock()
    patch_api = mock.MagicMock()
    core.create_namespaced_service.side_effect = _ApiException(409)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='service-rv',
                                 labels=labels,
                                 owner_references=owner_references),
        spec=SimpleNamespace(selector={}))
    apps, _ = _install(monkeypatch, core_api=core, patch_api=patch_api)

    with pytest.raises(RuntimeError, match=error):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    core.patch_namespaced_service.assert_not_called()
    patch_api.call_api.assert_not_called()


def test_legacy_data_plane_auth_disabled_omits_projection(monkeypatch):
    apps, _ = _install(monkeypatch, data_auth=False)

    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    pod_spec = deployment['spec']['template']['spec']
    container = pod_spec['containers'][0]
    env = {entry['name']: entry for entry in container['env']}
    assert env[constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR]['value'] == 'false'
    assert constants.LB_AUTH_TOKENS_FILE_ENV_VAR not in env
    assert lb_k8s.LB_DATA_PLANE_AUTH_VOLUME_NAME not in {
        mount['name'] for mount in container['volumeMounts']
    }
    assert lb_k8s.LB_DATA_PLANE_AUTH_VOLUME_NAME not in {
        volume['name'] for volume in pod_spec['volumes']
    }
    assert '$patch' not in repr(deployment)


def test_data_plane_auth_disable_patch_deletes_stale_projection(monkeypatch):
    apps = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _install(monkeypatch, apps_api=apps, data_auth=False, patch_api=patch_api)

    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    patch = _patch_calls(patch_api, _DEPLOYMENT_PATCH_PATH)[0].kwargs['body']
    pod_spec = patch['spec']['template']['spec']
    container = pod_spec['containers'][0]
    assert {
        'name': constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
        '$patch': 'delete',
    } in container['env']
    assert {
        'mountPath': lb_k8s._LB_DATA_PLANE_AUTH_MOUNT_PATH,
        '$patch': 'delete',
    } in container['volumeMounts']
    assert {
        'name': lb_k8s.LB_DATA_PLANE_AUTH_VOLUME_NAME,
        '$patch': 'delete',
    } in pod_spec['volumes']


def test_same_name_recreation_fences_old_service_before_reconcile(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    patch_api = mock.MagicMock()
    core.create_namespaced_service.side_effect = _ApiException(409)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='old-service-rv',
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'new-incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(
            selector={
                'app': lb_k8s.lb_deployment_name('svc'),
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'old-incarnation',
            }))
    _install(monkeypatch, apps_api=apps, core_api=core, patch_api=patch_api)

    lb_k8s.create_lb_deployment_and_service('svc',
                                            225,
                                            service_hash='new-incarnation')

    core.patch_namespaced_service.assert_not_called()
    service_patch_calls = _patch_calls(patch_api, _SERVICE_PATCH_PATH)
    assert len(service_patch_calls) == 2
    fence = service_patch_calls[0].kwargs['body']
    assert fence['spec'] == {
        'selector': {
            'app': lb_k8s.lb_deployment_name('svc'),
            lb_k8s.SERVICE_HASH_LABEL_KEY: 'new-incarnation',
        }
    }
    assert fence['metadata']['resourceVersion'] == 'old-service-rv'
    final = service_patch_calls[1].kwargs['body']
    assert final['spec']['ports'][0][
        'targetPort'] == constants.LOAD_BALANCER_PORT_START
    assert final['metadata']['resourceVersion'] == 'old-service-rv'


def test_create_fails_until_updated_lb_pod_is_ready(monkeypatch):
    apps = mock.MagicMock()
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(generation=2),
        spec=SimpleNamespace(replicas=1),
        status=SimpleNamespace(observed_generation=2,
                               updated_replicas=0,
                               available_replicas=0,
                               unavailable_replicas=1))
    _install(monkeypatch, apps_api=apps)
    clock = [0.0]

    def _advance_clock(seconds):
        clock[0] += seconds

    monkeypatch.setattr(lb_k8s.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(lb_k8s.time, 'sleep', _advance_clock)
    monkeypatch.setattr(constants, 'LB_DEPLOYMENT_READY_TIMEOUT_SECONDS', 2)
    with pytest.raises(RuntimeError, match='did not become ready'):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')


def test_create_requires_chart_pod_contract(monkeypatch):
    _install(monkeypatch, pod_name=None)
    with pytest.raises(RuntimeError, match=constants.POD_NAME_ENV_VAR):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')


def test_image_policy_and_digest_are_pinned(monkeypatch):
    _install(monkeypatch, image_policy='Always')
    image, policy, digest = lb_k8s._resolve_lb_image('skypilot', 'in-cluster')
    assert image == f'repo/skypilot@{_DIGEST_A}'
    assert policy == 'Always'
    assert digest == f'repo/skypilot@{_DIGEST_A}'


def test_declared_digest_is_accepted_before_runtime_status(monkeypatch):
    declared = f'repo/skypilot@{_DIGEST_A}'
    _install(monkeypatch, image=declared, image_id=None)
    image, policy, digest = lb_k8s._resolve_lb_image('skypilot', 'in-cluster')
    assert (image, policy, digest) == (declared, 'Always', declared)


def test_lb_resources_are_configurable(monkeypatch):
    monkeypatch.setenv(constants.LB_RESOURCES_ENV_VAR,
                       '{"requests":{"cpu":"250m","memory":"256Mi"}}')
    assert lb_k8s._lb_resources() == {
        'requests': {
            'cpu': '250m',
            'memory': '256Mi'
        }
    }


def test_lb_resources_accept_legacy_json_null(monkeypatch):
    monkeypatch.setenv(constants.LB_RESOURCES_ENV_VAR, 'null')
    assert lb_k8s._lb_resources() == {}


@pytest.mark.parametrize(('declared_image', 'image_id', 'expected'), [
    ('repo/skypilot:moving',
     f'docker-pullable://registry.example/repo/skypilot@{_DIGEST_A}',
     f'registry.example/repo/skypilot@{_DIGEST_A}'),
    ('registry.example:5000/repo/skypilot:moving', f'containerd://{_DIGEST_B}',
     f'registry.example:5000/repo/skypilot@{_DIGEST_B}'),
    ('repo/skypilot:moving', f'repo/skypilot@{_DIGEST_A}',
     f'repo/skypilot@{_DIGEST_A}'),
])
def test_runtime_image_id_formats_are_pinned(monkeypatch, declared_image,
                                             image_id, expected):
    _install(monkeypatch, image=declared_image, image_id=image_id)
    image, policy, digest = lb_k8s._resolve_lb_image('skypilot', 'in-cluster')
    assert image == expected
    assert policy == 'Always'
    assert digest == expected


@pytest.mark.parametrize('image_id', [
    None,
    'repo/skypilot@sha256:abc',
    f'unknown-runtime://{_DIGEST_A}',
    'not-a-digest',
])
def test_unparseable_runtime_image_id_fails_closed(monkeypatch, image_id):
    _install(monkeypatch, image='repo/skypilot:moving', image_id=image_id)
    with pytest.raises(RuntimeError, match='Cannot pin'):
        lb_k8s._resolve_lb_image('skypilot', 'in-cluster')


def test_digest_only_image_id_with_unsafe_declared_image_fails_closed(
        monkeypatch):
    _install(monkeypatch,
             image='https://registry.example/repo/skypilot:moving',
             image_id=f'containerd://{_DIGEST_A}')
    with pytest.raises(RuntimeError, match='Cannot pin'):
        lb_k8s._resolve_lb_image('skypilot', 'in-cluster')


def test_termination_grace_budget():
    assert lb_k8s.lb_termination_grace_period_seconds(120, None) == 165
    assert lb_k8s.lb_termination_grace_period_seconds(120, 600) == 645
    assert lb_k8s.lb_termination_grace_period_seconds(0.5, None) == 46


@pytest.mark.parametrize(
    ('stream_timeout', 'graceful_drain'), [(-1, None), (float('nan'), None),
                                           (float('inf'), None), (1, -0.1),
                                           (1, float('nan')), (1, float('inf')),
                                           (True, None), (1, False)])
def test_termination_grace_budget_rejects_invalid_numbers(
        stream_timeout, graceful_drain):
    with pytest.raises(ValueError, match='finite, nonnegative'):
        lb_k8s.lb_termination_grace_period_seconds(stream_timeout,
                                                   graceful_drain)


def test_ensure_missing_object_is_ownership_fenced(monkeypatch):
    apps = mock.MagicMock()
    apps.read_namespaced_deployment.side_effect = _ApiException(404)
    _install(monkeypatch, apps_api=apps, db_service_names=())
    with mock.patch.object(lb_k8s,
                           'create_lb_deployment_and_service') as create:
        lb_k8s.ensure_lb_objects_exist('svc', 225, 'incarnation')
    create.assert_not_called()

    _install(monkeypatch, apps_api=apps, db_service_names=('svc',))
    with mock.patch.object(lb_k8s,
                           'create_lb_deployment_and_service') as create:
        lb_k8s.ensure_lb_objects_exist('svc', 225, 'incarnation')
    assert create.call_args.args == ('svc', 225, 'incarnation')
    assert create.call_args.kwargs['continue_guard']()


def test_ensure_reconciles_updated_termination_budget(monkeypatch):
    apps = mock.MagicMock()
    apps.read_namespaced_deployment.return_value = {
        'spec': {
            'template': {
                'spec': {
                    'terminationGracePeriodSeconds': 165
                }
            }
        }
    }
    _install(monkeypatch, apps_api=apps, db_service_names=('svc',))
    with mock.patch.object(lb_k8s,
                           'create_lb_deployment_and_service') as create:
        lb_k8s.ensure_lb_objects_exist('svc', 645, 'incarnation')
    assert create.call_args.args == ('svc', 645, 'incarnation')
    assert create.call_args.kwargs['continue_guard']()


def test_ensure_reports_existing_crashloop_as_unhealthy(monkeypatch):
    apps = mock.MagicMock()
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(generation=1),
        spec=SimpleNamespace(
            replicas=1,
            template=SimpleNamespace(
                metadata=SimpleNamespace(
                    labels={lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation'}),
                spec=SimpleNamespace(termination_grace_period_seconds=225))),
        status=SimpleNamespace(observed_generation=1,
                               updated_replicas=1,
                               available_replicas=0,
                               unavailable_replicas=1))
    _, core = _install(monkeypatch, apps_api=apps, db_service_names=('svc',))
    core.read_namespaced_service.return_value = SimpleNamespace(
        spec=SimpleNamespace(
            type='LoadBalancer',
            selector={
                'app': lb_k8s.lb_deployment_name('svc'),
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            ports=[
                SimpleNamespace(port=constants.LOAD_BALANCER_PORT_START,
                                target_port=constants.LOAD_BALANCER_PORT_START,
                                protocol='TCP')
            ]))
    assert not lb_k8s.ensure_lb_objects_exist('svc', 225, 'incarnation')


@pytest.mark.parametrize(
    ('ingress', 'expected_healthy'),
    [([], False), ([SimpleNamespace(hostname='lb.example', ip=None)], True)])
def test_ensure_requires_published_provider_endpoint(monkeypatch, ingress,
                                                     expected_healthy):
    apps = mock.MagicMock()
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(generation=1),
        spec=SimpleNamespace(
            replicas=1,
            template=SimpleNamespace(
                metadata=SimpleNamespace(
                    labels={lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation'}),
                spec=SimpleNamespace(termination_grace_period_seconds=225))),
        status=SimpleNamespace(observed_generation=1,
                               replicas=1,
                               updated_replicas=1,
                               available_replicas=1,
                               unavailable_replicas=0))
    _, core = _install(monkeypatch, apps_api=apps, db_service_names=('svc',))
    core.read_namespaced_service.return_value = SimpleNamespace(
        spec=SimpleNamespace(
            type='LoadBalancer',
            selector={
                'app': lb_k8s.lb_deployment_name('svc'),
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            ports=[
                SimpleNamespace(port=constants.LOAD_BALANCER_PORT_START,
                                target_port=constants.LOAD_BALANCER_PORT_START,
                                protocol='TCP')
            ]),
        status=SimpleNamespace(load_balancer=SimpleNamespace(ingress=ingress)))

    assert (lb_k8s.ensure_lb_objects_exist('svc', 225, 'incarnation') is
            expected_healthy)


def _lb_pod(uid, phase='Running', deleting=False, ready=True):
    return SimpleNamespace(metadata=SimpleNamespace(
        uid=uid, deletion_timestamp='now' if deleting else None),
                           status=SimpleNamespace(
                               phase=phase,
                               conditions=[
                                   SimpleNamespace(
                                       type='Ready',
                                       status=('True' if ready else 'False'))
                               ]))


def test_pod_authority_splits_ready_from_live_with_one_listing(monkeypatch):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[
        _lb_pod('new'),
        _lb_pod('old', deleting=True),
        _lb_pod('unready', ready=False),
        _lb_pod('pending', phase='Pending'),
        _lb_pod('done', phase='Succeeded'),
        _lb_pod('failed', phase='Failed'),
    ])
    assert lb_k8s.get_lb_pod_authority('svc') == lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'new'},
        live_uids={'new', 'old', 'unready', 'pending'})
    core.list_namespaced_pod.assert_called_once()
    assert core.list_namespaced_pod.call_args.kwargs['label_selector'] == (
        f'app={lb_k8s.lb_deployment_name("svc")},'
        f'{lb_k8s.SERVICE_HASH_LABEL_KEY}=incarnation')


def test_pod_authority_uses_controller_owner_lookup(monkeypatch):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_from_name', lambda unused_name:
        (_ for _ in
         ()).throw(AssertionError('full service read should not be used')))
    core.list_namespaced_pod.return_value = SimpleNamespace(
        items=[_lb_pod('new')])

    assert lb_k8s.get_lb_pod_authority('svc') == lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'new'}, live_uids={'new'})


def test_pod_authority_query_failure_is_unknown(monkeypatch):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    core.list_namespaced_pod.side_effect = RuntimeError('apiserver down')
    assert lb_k8s.get_lb_pod_authority('svc') is None


def test_pod_authority_missing_live_uid_fails_closed(monkeypatch):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[
        _lb_pod('known'),
        _lb_pod(None, ready=False),
    ])
    assert lb_k8s.get_lb_pod_authority('svc') is None


def test_external_lb_logs_come_from_current_pod(monkeypatch, capsys):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    older = _lb_pod('old')
    older.metadata.name = 'lb-old'
    older.metadata.creation_timestamp = '2026-01-01T00:00:00Z'
    newer = _lb_pod('new')
    newer.metadata.name = 'lb-new'
    newer.metadata.creation_timestamp = '2026-01-02T00:00:00Z'
    core.list_namespaced_pod.return_value = SimpleNamespace(
        items=[older, newer])
    core.read_namespaced_pod_log.return_value = 'line one\nline two\n'

    assert lb_k8s.stream_lb_logs('svc', follow=False, tail=2) == ''

    assert capsys.readouterr().out == 'line one\nline two\n'
    assert core.read_namespaced_pod_log.call_args.kwargs['name'] == 'lb-new'
    assert core.read_namespaced_pod_log.call_args.kwargs['tail_lines'] == 2


def test_delete_is_idempotent(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.read_namespaced_deployment.side_effect = _ApiException(404)
    core.read_namespaced_service.side_effect = _ApiException(404)
    _install(monkeypatch, apps_api=apps, core_api=core)
    lb_k8s.delete_lb_objects('svc', 'incarnation-a')
    apps.delete_namespaced_deployment.assert_not_called()
    core.delete_namespaced_service.assert_not_called()


def test_required_delete_fails_without_incluster_identity(monkeypatch):
    monkeypatch.setattr(lb_k8s.kubernetes_utils,
                        'is_incluster_config_available', lambda: False)
    with pytest.raises(RuntimeError, match='in-cluster Kubernetes'):
        lb_k8s.delete_lb_objects('svc', 'incarnation-a', require_runtime=True)


def test_required_delete_fails_without_namespace(monkeypatch):
    _install(monkeypatch)
    monkeypatch.setattr(lb_k8s, '_cleanup_lb_namespace', lambda: None)
    with pytest.raises(RuntimeError, match='namespace'):
        lb_k8s.delete_lb_objects('svc', 'incarnation-a', require_runtime=True)


def test_delete_is_hash_uid_and_resource_version_fenced(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.read_namespaced_deployment.side_effect = [
        _owned_object(uid='deployment-a', rv='11'),
        _ApiException(404),
    ]
    core.read_namespaced_service.side_effect = [
        _owned_object(uid='service-a', rv='9'),
        _ApiException(404),
    ]
    _install(monkeypatch, apps_api=apps, core_api=core)

    lb_k8s.delete_lb_objects('svc', 'incarnation-a')

    service_body = core.delete_namespaced_service.call_args.kwargs['body']
    assert service_body['preconditions'] == {
        'uid': 'service-a',
        'resourceVersion': '9'
    }
    deployment_body = apps.delete_namespaced_deployment.call_args.kwargs['body']
    assert deployment_body['preconditions'] == {
        'uid': 'deployment-a',
        'resourceVersion': '11'
    }
    assert deployment_body['propagationPolicy'] == 'Foreground'


def test_foreground_delete_timeout_covers_long_pod_drain_grace():
    deployment = {
        'spec': {
            'template': {
                'spec': {
                    'terminationGracePeriodSeconds': 300,
                },
            },
        },
    }
    assert lb_k8s._lb_object_deletion_timeout_seconds(  # pylint: disable=protected-access
        deployment, 'Deployment') >= 330


def test_stale_a_delete_refuses_successor_b_objects(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.read_namespaced_deployment.return_value = _owned_object(
        'incarnation-b', 'deployment-b', '12')
    core.read_namespaced_service.return_value = _owned_object(
        'incarnation-b', 'service-b', '10')
    _install(monkeypatch, apps_api=apps, core_api=core)

    with pytest.raises(RuntimeError, match='expected incarnation'):
        lb_k8s.delete_lb_objects('svc', 'incarnation-a')
    apps.delete_namespaced_deployment.assert_not_called()
    core.delete_namespaced_service.assert_not_called()


def test_delete_refuses_replaced_api_deployment_owner(monkeypatch):
    apps, core = _install(monkeypatch, api_deployment_uid='replacement-api-uid')

    with pytest.raises(RuntimeError, match='changed from UID'):
        lb_k8s.delete_lb_objects('svc',
                                 'incarnation-a',
                                 expected_api_deployment_uid='original-api-uid')

    apps.delete_namespaced_deployment.assert_not_called()
    core.delete_namespaced_service.assert_not_called()


def test_delete_refuses_object_owned_by_another_release(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.read_namespaced_deployment.return_value = _owned_object(
        owner_name='other-release-api-server', owner_uid='other-release-uid')
    core.read_namespaced_service.return_value = _owned_object(
        owner_name='other-release-api-server', owner_uid='other-release-uid')
    _install(monkeypatch, apps_api=apps, core_api=core)

    with pytest.raises(RuntimeError, match='expected exact API Deployment'):
        lb_k8s.delete_lb_objects(
            'svc',
            'incarnation-a',
            expected_api_deployment_uid='api-deployment-uid')

    apps.delete_namespaced_deployment.assert_not_called()
    core.delete_namespaced_service.assert_not_called()


def test_create_retries_when_reaper_wins_409_to_patch_window(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = [_ApiException(409), None]
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            generation=1,
            resource_version='lb-deployment-rv',
            labels={
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation-a',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(replicas=1),
        status=SimpleNamespace(observed_generation=1,
                               updated_replicas=1,
                               available_replicas=1,
                               unavailable_replicas=0))
    patch_api.call_api.side_effect = _ApiException(404)
    _install(monkeypatch, apps_api=apps, core_api=core, patch_api=patch_api)

    lb_k8s.create_lb_deployment_and_service('svc',
                                            30,
                                            service_hash='incarnation-a')

    assert apps.create_namespaced_deployment.call_count == 2
    assert len(_patch_calls(patch_api, _DEPLOYMENT_PATCH_PATH)) == 1


def test_create_retries_while_old_deployment_is_terminating(monkeypatch):
    apps = mock.MagicMock()
    patch_api = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = [
        _ApiException(409),
        _ApiException(409),
    ]
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            generation=1,
            resource_version='lb-deployment-rv',
            labels={
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation-a',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(replicas=1),
        status=SimpleNamespace(observed_generation=1,
                               updated_replicas=1,
                               available_replicas=1,
                               unavailable_replicas=0))
    patch_api.call_api.side_effect = [
        _ApiException(409),
        None,
    ]
    _install(monkeypatch, apps_api=apps, patch_api=patch_api)
    monkeypatch.setattr(lb_k8s.time, 'sleep', lambda _: None)

    lb_k8s.create_lb_deployment_and_service('svc', 30, 'incarnation-a')

    assert apps.create_namespaced_deployment.call_count == 2
    assert len(_patch_calls(patch_api, _DEPLOYMENT_PATCH_PATH)) == 2


def test_create_waits_for_terminating_service_uid(monkeypatch):
    core = mock.MagicMock()
    patch_api = mock.MagicMock()
    core.create_namespaced_service.side_effect = [_ApiException(409), None]
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(deletion_timestamp='now'),
        spec=SimpleNamespace(selector={}))
    _install(monkeypatch, core_api=core, patch_api=patch_api)
    monkeypatch.setattr(lb_k8s.time, 'sleep', lambda _: None)

    lb_k8s.create_lb_deployment_and_service('svc', 30, 'incarnation-a')

    assert core.create_namespaced_service.call_count == 2
    core.patch_namespaced_service.assert_not_called()
    patch_api.call_api.assert_not_called()


def test_stale_owner_stops_after_terminating_service_wait(monkeypatch):
    core = mock.MagicMock()
    patch_api = mock.MagicMock()
    core.create_namespaced_service.side_effect = _ApiException(409)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(deletion_timestamp='now'),
        spec=SimpleNamespace(selector={}))
    _install(monkeypatch, core_api=core, patch_api=patch_api)
    monkeypatch.setattr(lb_k8s.time, 'sleep', lambda _: None)
    ownership = iter([True, True, False])

    with pytest.raises(RuntimeError, match='Lost service ownership'):
        lb_k8s.create_lb_deployment_and_service(
            'svc', 30, 'incarnation-a', continue_guard=lambda: next(ownership))

    # First create observed the terminating old UID. Ownership was rechecked
    # before retry, so stale A never mutates/recreates successor B.
    core.create_namespaced_service.assert_called_once()
    core.patch_namespaced_service.assert_not_called()
    patch_api.call_api.assert_not_called()


def test_final_service_recreate_retries_create_conflict(monkeypatch):
    core = mock.MagicMock()
    patch_api = mock.MagicMock()
    core.create_namespaced_service.side_effect = [
        _ApiException(409),
        _ApiException(409),
    ]
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            deletion_timestamp=None,
            resource_version='lb-service-rv',
            labels={
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation-a',
            },
            owner_references=[
                SimpleNamespace(api_version='apps/v1',
                                kind='Deployment',
                                name='skypilot-api-server',
                                uid='api-deployment-uid',
                                controller=False,
                                block_owner_deletion=False)
            ]),
        spec=SimpleNamespace(
            selector={
                'app': lb_k8s.lb_deployment_name('svc'),
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation-a',
            }))
    patch_api.call_api.side_effect = [
        _ApiException(404),
        None,
    ]
    _install(monkeypatch, core_api=core, patch_api=patch_api)
    monkeypatch.setattr(lb_k8s.time, 'sleep', lambda _: None)

    lb_k8s.create_lb_deployment_and_service('svc', 30, 'incarnation-a')

    assert core.create_namespaced_service.call_count == 2
    assert len(_patch_calls(patch_api, _SERVICE_PATCH_PATH)) == 2


def test_delete_precondition_conflict_fails_closed(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.read_namespaced_deployment.side_effect = _ApiException(404)
    core.read_namespaced_service.return_value = _owned_object(
        'incarnation-a', 'service-a', '9')
    core.delete_namespaced_service.side_effect = _ApiException(409)
    _install(monkeypatch, apps_api=apps, core_api=core)

    with pytest.raises(_ApiException):
        lb_k8s.delete_lb_objects('svc', 'incarnation-a')
    assert core.delete_namespaced_service.call_args.kwargs['body'][
        'preconditions']['resourceVersion'] == '9'


def test_delete_wait_rejects_replacement_uid(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.read_namespaced_deployment.side_effect = _ApiException(404)
    core.read_namespaced_service.side_effect = [
        _owned_object('incarnation-a', 'service-a', '9'),
        _owned_object('incarnation-b', 'service-b', '1'),
    ]
    _install(monkeypatch, apps_api=apps, core_api=core)

    with pytest.raises(RuntimeError, match='was replaced'):
        lb_k8s.delete_lb_objects('svc', 'incarnation-a')
    core.delete_namespaced_service.assert_called_once()


def test_reaper_db_null_then_successor_appears_fails_closed(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[])
    core.list_namespaced_service.return_value = SimpleNamespace(
        items=[_owned_object('incarnation-a')])
    apps.read_namespaced_deployment.side_effect = _ApiException(404)
    core.read_namespaced_service.return_value = _owned_object(
        'incarnation-b', 'service-b', '1')
    _install(monkeypatch, apps_api=apps, core_api=core)
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_hash',
                        lambda name: None)

    with pytest.raises(RuntimeError, match='expected incarnation'):
        lb_k8s.reconcile_lb_objects(set())
    core.delete_namespaced_service.assert_not_called()


def test_cleanup_uses_service_account_namespace_when_feature_disabled(
        monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[])
    apps.read_namespaced_deployment.side_effect = [
        _owned_object(uid='deployment-a'),
        _ApiException(404),
    ]
    core.read_namespaced_service.side_effect = [
        _owned_object(uid='service-a'),
        _ApiException(404),
    ]
    _install(monkeypatch,
             apps_api=apps,
             core_api=core,
             external=False,
             namespace='workloads',
             pod_namespace=None)

    with mock.patch('builtins.open',
                    mock.mock_open(read_data='control-plane\n')):
        lb_k8s.delete_lb_objects('svc', 'incarnation-a')
        lb_k8s.reconcile_lb_objects(set())

    assert core.delete_namespaced_service.call_args.args[1] == 'control-plane'
    assert apps.delete_namespaced_deployment.call_args.args[
        1] == 'control-plane'
    assert apps.list_namespaced_deployment.call_args.args[0] == 'control-plane'
    assert core.list_namespaced_service.call_args.args[0] == 'control-plane'


def test_reconcile_reaps_only_db_confirmed_orphans(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[
        _owned_object('live-hash', service_name='live'),
        _owned_object('gone-hash', service_name='gone'),
    ])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[])
    _install(monkeypatch,
             apps_api=apps,
             core_api=core,
             db_service_names=('live',))
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_hash',
                        lambda name: 'live-hash' if name == 'live' else None)
    with mock.patch.object(lb_k8s, 'delete_lb_objects') as delete:
        lb_k8s.reconcile_lb_objects(set())
    delete.assert_called_once_with(
        'gone', 'gone-hash', expected_api_deployment_uid='api-deployment-uid')


def test_reconcile_reaps_service_only_orphan(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[
        _owned_object('service-only-hash', service_name='service-only'),
    ])
    _install(monkeypatch, apps_api=apps, core_api=core)

    with mock.patch.object(lb_k8s, 'delete_lb_objects') as delete:
        lb_k8s.reconcile_lb_objects(set())

    delete.assert_called_once_with(
        'service-only',
        'service-only-hash',
        expected_api_deployment_uid='api-deployment-uid')
    assert core.list_namespaced_service.call_args.args[0] == 'skypilot'
    assert core.list_namespaced_service.call_args.kwargs[
        'label_selector'] == lb_k8s.LB_SELECTOR_LABEL


def test_reconcile_reaps_scoped_predecessor_beside_live_successor(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[
        _owned_object('incarnation-a',
                      object_name=lb_k8s.lb_deployment_name(
                          'svc', 'incarnation-a'))
    ])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[])
    _install(monkeypatch, apps_api=apps, core_api=core)
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_hash',
                        lambda name: 'incarnation-b')

    with mock.patch.object(lb_k8s, 'delete_lb_objects') as delete:
        lb_k8s.reconcile_lb_objects({'svc'})

    delete.assert_called_once_with(
        'svc',
        'incarnation-a',
        resource_scope='incarnation-a',
        expected_api_deployment_uid=('api-deployment-uid'))


def test_reconcile_ignores_another_release_lb(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[
        _owned_object('incarnation-a',
                      service_name='foreign-svc',
                      owner_name='other-release-api-server',
                      owner_uid='other-release-uid')
    ])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[])
    _install(monkeypatch, apps_api=apps, core_api=core)

    with mock.patch.object(lb_k8s, 'delete_lb_objects') as delete:
        lb_k8s.reconcile_lb_objects(set())

    delete.assert_not_called()
