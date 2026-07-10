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


class _ApiException(Exception):

    def __init__(self, status):
        super().__init__(f'status={status}')
        self.status = status


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
             api_deployment_name='skypilot-api-server',
             api_deployment_uid='api-deployment-uid',
             release_name='skypilot',
             db_service_names=()):
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

    env = {
        'SKYPILOT_SERVE_API_SERVICE_URL': 'http://sky-api.skypilot.svc.cluster.local',
        constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR: '/etc/skypilot/serve-auth/lb-sync/tokens',
        constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR: '/etc/skypilot/serve-auth/controller-admin/tokens',
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
    effective_api_deployment_name = (
        api_deployment_name or
        (f'{release_name}-api-server' if release_name else None))
    read_service = core_api.read_namespaced_service
    if (read_service.side_effect is None and
            isinstance(read_service.return_value, mock.MagicMock)):
        read_service.return_value = SimpleNamespace(
            metadata=SimpleNamespace(resource_version='lb-service-rv',
                                     owner_references=[
                                         SimpleNamespace(
                                             api_version='apps/v1',
                                             kind='Deployment',
                                             name=effective_api_deployment_name,
                                             uid=api_deployment_uid,
                                             controller=False,
                                             block_owner_deletion=False)
                                     ]),
            spec=SimpleNamespace(
                selector={'app': lb_k8s.lb_deployment_name('svc')},
                ports=[
                    SimpleNamespace(
                        port=constants.LOAD_BALANCER_PORT_START,
                        target_port=constants.LOAD_BALANCER_PORT_START,
                        protocol='TCP')
                ]))
    read_deployment = apps_api.read_namespaced_deployment
    if read_deployment.side_effect is None:
        existing_deployment = read_deployment.return_value
        if isinstance(existing_deployment, mock.MagicMock):
            existing_deployment = SimpleNamespace(
                metadata=SimpleNamespace(
                    generation=1,
                    resource_version='lb-deployment-rv',
                    owner_references=[
                        SimpleNamespace(api_version='apps/v1',
                                        kind='Deployment',
                                        name=effective_api_deployment_name,
                                        uid=api_deployment_uid,
                                        controller=False,
                                        block_owner_deletion=False)
                    ]),
                spec=SimpleNamespace(replicas=1),
                status=SimpleNamespace(observed_generation=1,
                                       updated_replicas=1,
                                       available_replicas=1,
                                       unavailable_replicas=0))

        def _read_deployment(name, unused_namespace):
            if name == effective_api_deployment_name:
                return SimpleNamespace(metadata=SimpleNamespace(
                    uid=api_deployment_uid))
            return existing_deployment

        read_deployment.side_effect = _read_deployment
    monkeypatch.setattr(lb_k8s.kubernetes,
                        'apps_api',
                        lambda unused_context=None: apps_api)
    monkeypatch.setattr(lb_k8s.kubernetes,
                        'core_api',
                        lambda unused_context=None: core_api)

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
    return apps_api, core_api


def test_name_helpers_are_unique_rfc1123():
    names = ['my-service', 'My_Service', 'svc-a', 'svc_a', 'x' * 200, '___']
    rendered = [lb_k8s.lb_base_name(name) for name in names]
    assert len(set(rendered)) == len(rendered)
    assert all(
        _RFC1123.fullmatch(name) and len(name) <= 63 for name in rendered)


def test_external_runtime_fails_closed(monkeypatch):
    _install(monkeypatch, external=False)
    with pytest.raises(RuntimeError, match='external load balancer'):
        lb_k8s.require_external_lb_runtime()

    _install(monkeypatch, incluster=False)
    with pytest.raises(RuntimeError, match='in-cluster'):
        lb_k8s.require_external_lb_runtime()


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
    assert service['spec']['ports'][0]['port'] == \
        constants.LOAD_BALANCER_PORT_START


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
    assert (lb_k8s.lb_service_endpoint_or_none('svc-a') ==
            f'{lb_k8s.lb_service_name("svc-a")}.control-plane.svc:30001')


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


def test_create_409_patches_legacy_deployment(monkeypatch):
    apps = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _install(monkeypatch, apps_api=apps)
    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')
    apps.patch_namespaced_deployment.assert_called_once()
    patched = apps.patch_namespaced_deployment.call_args.args[2]
    args = patched['spec']['template']['spec']['containers'][0]['args']
    assert '/api/internal/serve/svc' in args[1]


def test_create_409_adopts_objects_with_metadata_only_patch(monkeypatch):
    apps = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(generation=1,
                                 resource_version='deployment-rv',
                                 owner_references=[]),
        spec=SimpleNamespace(replicas=1),
        status=SimpleNamespace(observed_generation=1,
                               updated_replicas=1,
                               available_replicas=1,
                               unavailable_replicas=0))
    core = mock.MagicMock()
    core.create_namespaced_service.side_effect = _ApiException(409)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='service-rv',
                                 owner_references=[]),
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
    _install(monkeypatch, apps_api=apps, core_api=core)

    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    assert apps.patch_namespaced_deployment.call_count == 2
    deployment_adoption = apps.patch_namespaced_deployment.call_args_list[0]
    assert deployment_adoption.args[2] == [{
        'op': 'test',
        'path': '/metadata/resourceVersion',
        'value': 'deployment-rv',
    }, {
        'op': 'add',
        'path': '/metadata/ownerReferences',
        'value': [{
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'name': 'skypilot-api-server',
            'uid': 'api-deployment-uid',
            'controller': False,
            'blockOwnerDeletion': False,
        }],
    }]
    deployment_reconcile = apps.patch_namespaced_deployment.call_args_list[1]
    assert 'spec' in deployment_reconcile.args[2]
    assert 'ownerReferences' not in deployment_reconcile.args[2]['metadata']

    assert core.patch_namespaced_service.call_count == 2
    service_adoption = core.patch_namespaced_service.call_args_list[0]
    assert all(operation['path'].startswith('/metadata/')
               for operation in service_adoption.args[2])
    service_reconcile = core.patch_namespaced_service.call_args_list[1]
    assert service_reconcile.args[2]['spec']['selector'][
        lb_k8s.SERVICE_HASH_LABEL_KEY] == 'incarnation'


def test_create_refuses_unguarded_adoption_without_resource_version(
        monkeypatch):
    apps = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    apps.read_namespaced_deployment.return_value = SimpleNamespace(
        metadata=SimpleNamespace(owner_references=[]))
    _, core = _install(monkeypatch, apps_api=apps)

    with pytest.raises(RuntimeError, match='no resourceVersion'):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    core.create_namespaced_service.assert_not_called()


def test_create_refuses_adoption_when_api_deployment_uid_changes(monkeypatch):
    apps = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _, core = _install(monkeypatch, apps_api=apps)
    generated_name = lb_k8s.lb_deployment_name('svc')
    owner_uids = iter(('api-deployment-uid', 'replacement-api-uid'))

    def _read_deployment(name, unused_namespace):
        if name == 'skypilot-api-server':
            return SimpleNamespace(metadata=SimpleNamespace(
                uid=next(owner_uids)))
        if name == generated_name:
            return SimpleNamespace(metadata=SimpleNamespace(
                resource_version='lb-rv',
                owner_references=[
                    SimpleNamespace(api_version='apps/v1',
                                    kind='Deployment',
                                    name='skypilot-api-server',
                                    uid='old-api-uid')
                ]))
        raise AssertionError(name)

    apps.read_namespaced_deployment.side_effect = _read_deployment

    with pytest.raises(RuntimeError, match='changed from UID'):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    core.create_namespaced_service.assert_not_called()


def test_create_refuses_deployment_owned_by_another_live_release(monkeypatch):
    apps = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _, core = _install(monkeypatch, apps_api=apps)
    generated_name = lb_k8s.lb_deployment_name('svc')

    def _read_deployment(name, unused_namespace):
        if name == 'skypilot-api-server':
            return SimpleNamespace(metadata=SimpleNamespace(
                uid='api-deployment-uid'))
        if name == generated_name:
            return SimpleNamespace(metadata=SimpleNamespace(
                resource_version='lb-rv',
                owner_references=[
                    SimpleNamespace(api_version='apps/v1',
                                    kind='Deployment',
                                    name='other-release-api-server',
                                    uid='other-live-uid')
                ]))
        if name == 'other-release-api-server':
            return SimpleNamespace(metadata=SimpleNamespace(
                uid='other-live-uid'))
        raise AssertionError(name)

    apps.read_namespaced_deployment.side_effect = _read_deployment

    with pytest.raises(RuntimeError, match='owned by live Deployment'):
        lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    apps.patch_namespaced_deployment.assert_not_called()
    core.create_namespaced_service.assert_not_called()


def test_data_plane_auth_disabled_omits_projection(monkeypatch):
    apps, _ = _install(monkeypatch, data_auth=False)

    lb_k8s.require_external_lb_runtime()
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
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _install(monkeypatch, apps_api=apps, data_auth=False)

    lb_k8s.create_lb_deployment_and_service('svc', 225, 'incarnation')

    patch = apps.patch_namespaced_deployment.call_args.args[2]
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
    core.create_namespaced_service.side_effect = _ApiException(409)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(resource_version='old-service-rv'),
        spec=SimpleNamespace(
            selector={
                'app': lb_k8s.lb_deployment_name('svc'),
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'old-incarnation',
            }))
    _install(monkeypatch, apps_api=apps, core_api=core)

    lb_k8s.create_lb_deployment_and_service('svc',
                                            225,
                                            service_hash='new-incarnation')

    assert core.patch_namespaced_service.call_count == 3
    adoption = core.patch_namespaced_service.call_args_list[0]
    assert all(operation['path'].startswith('/metadata/')
               for operation in adoption.args[2])
    fence = core.patch_namespaced_service.call_args_list[1].args[2]
    assert fence['spec'] == {
        'selector': {
            'app': lb_k8s.lb_deployment_name('svc'),
            lb_k8s.SERVICE_HASH_LABEL_KEY: 'new-incarnation',
        }
    }
    final = core.patch_namespaced_service.call_args_list[2].args[2]
    assert final['spec']['ports'][0][
        'targetPort'] == constants.LOAD_BALANCER_PORT_START


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
    monkeypatch.setattr(lb_k8s.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(
        lb_k8s.time, 'sleep',
        lambda seconds: clock.__setitem__(0, clock[0] + seconds))
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
    create.assert_called_once_with('svc', 225, 'incarnation')


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
    create.assert_called_once_with('svc', 645, 'incarnation')


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
    apps.delete_namespaced_deployment.side_effect = _ApiException(404)
    core.delete_namespaced_service.side_effect = _ApiException(404)
    _install(monkeypatch, apps_api=apps, core_api=core)
    lb_k8s.delete_lb_objects('svc')
    apps.delete_namespaced_deployment.assert_called_once()
    core.delete_namespaced_service.assert_called_once()


def test_cleanup_uses_service_account_namespace_when_feature_disabled(
        monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[])
    _install(monkeypatch,
             apps_api=apps,
             core_api=core,
             external=False,
             namespace='workloads',
             pod_namespace=None)

    with mock.patch('builtins.open',
                    mock.mock_open(read_data='control-plane\n')):
        lb_k8s.delete_lb_objects('svc')
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
        SimpleNamespace(metadata=SimpleNamespace(
            labels={lb_k8s.SERVE_LB_LABEL_KEY: 'live'})),
        SimpleNamespace(metadata=SimpleNamespace(
            labels={lb_k8s.SERVE_LB_LABEL_KEY: 'gone'})),
    ])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[])
    _install(monkeypatch,
             apps_api=apps,
             core_api=core,
             db_service_names=('live',))
    with mock.patch.object(lb_k8s, 'delete_lb_objects') as delete:
        lb_k8s.reconcile_lb_objects(set())
    delete.assert_called_once_with('gone')


def test_reconcile_reaps_service_only_orphan(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value = SimpleNamespace(items=[])
    core.list_namespaced_service.return_value = SimpleNamespace(items=[
        SimpleNamespace(metadata=SimpleNamespace(
            labels={lb_k8s.SERVE_LB_LABEL_KEY: 'service-only'})),
    ])
    _install(monkeypatch, apps_api=apps, core_api=core)

    with mock.patch.object(lb_k8s, 'delete_lb_objects') as delete:
        lb_k8s.reconcile_lb_objects(set())

    delete.assert_called_once_with('service-only')
    assert core.list_namespaced_service.call_args.args[0] == 'skypilot'
    assert core.list_namespaced_service.call_args.kwargs[
        'label_selector'] == lb_k8s.LB_SELECTOR_LABEL
