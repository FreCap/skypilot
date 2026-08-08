"""Logic tests for the controller-owned external LB lifecycle."""
# pylint: disable=protected-access,unexpected-keyword-arg
import contextvars
import os
import re
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

from sky.serve import constants
from sky.serve import lb_ha
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
             lb_priority_class_name=None,
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
             patch_api=None,
             policy_api=None):
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
    if lb_priority_class_name is None:
        monkeypatch.delenv(constants.LB_PRIORITY_CLASS_NAME_ENV_VAR,
                           raising=False)
    else:
        monkeypatch.setenv(constants.LB_PRIORITY_CLASS_NAME_ENV_VAR,
                           lb_priority_class_name)
    if pod_namespace is None:
        monkeypatch.delenv(constants.POD_NAMESPACE_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(constants.POD_NAMESPACE_ENV_VAR, pod_namespace)

    apps_api = apps_api or mock.MagicMock()
    core_api = core_api or mock.MagicMock()
    patch_api = patch_api or mock.MagicMock()
    policy_api = policy_api or mock.MagicMock()
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

    def _read_deployment(name, unused_namespace, **kwargs):
        if name == effective_api_deployment_name:
            return SimpleNamespace(metadata=SimpleNamespace(
                uid=api_deployment_uid))
        if original_side_effect is None:
            return existing_deployment
        if isinstance(original_side_effect, BaseException):
            raise original_side_effect
        if callable(original_side_effect):
            return original_side_effect(name, unused_namespace, **kwargs)
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
    monkeypatch.setattr(lb_k8s.kubernetes,
                        'policy_api',
                        lambda unused_context=None: policy_api)
    if not wait_for_endpoint:
        monkeypatch.setattr(lb_k8s, '_wait_for_lb_service_endpoint',
                            lambda *unused_args, **unused_kwargs: None)
    monkeypatch.setattr(lb_k8s, '_retry_obsolete_lb_topology_cleanup',
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
        lb_k8s.serve_state, 'get_service_controller_owner',
        lambda name, **_kwargs: {
            'controller_pid': os.getpid(),
            'controller_ip': None,
            'hash': 'incarnation',
            'resource_scope': None,
            'status': None,
            'controller_port': None,
            'lifecycle_epoch': None,
            'lb_ha_enabled': False,
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


def test_ha_runtime_requires_explicit_chart_rbac_marker(monkeypatch):
    _install(monkeypatch)
    monkeypatch.delenv(constants.LB_HA_RBAC_READY_ENV_VAR, raising=False)
    with pytest.raises(RuntimeError, match='PodDisruptionBudget RBAC'):
        lb_k8s.require_lb_ha_runtime()

    monkeypatch.setenv(constants.LB_HA_RBAC_READY_ENV_VAR, 'true')
    lb_k8s.require_lb_ha_runtime()


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


def test_ha_create_builds_two_warm_slots_stable_service_and_pdb(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    policy = mock.MagicMock()

    def _missing_slot_deployment(_name, _namespace):
        raise _ApiException(404)

    apps.read_namespaced_deployment.side_effect = _missing_slot_deployment
    _install(monkeypatch,
             apps_api=apps,
             core_api=core,
             policy_api=policy,
             lb_priority_class_name='skypilot-serve-lb')
    state = lb_ha.LbCutoverState(enabled=True,
                                 active_slot=lb_ha.LbSlot.A,
                                 generation=1,
                                 pending_slot=None,
                                 phase=lb_ha.LbCutoverPhase.STABLE,
                                 lifecycle_epoch=9)
    monkeypatch.setattr(lb_k8s.serve_state, 'get_lb_cutover_state',
                        lambda _name: state)
    monkeypatch.setattr(lb_k8s, '_wait_for_lb_deployment_ready',
                        lambda *_args, **_kwargs: None)

    lb_k8s.create_lb_deployment_and_service('svc',
                                            225,
                                            'incarnation',
                                            high_availability=True)

    deployments = [
        call.args[1]
        for call in apps.create_namespaced_deployment.call_args_list
    ]
    assert {
        deployment['spec']['template']['metadata']['labels'][
            lb_k8s.LB_SLOT_LABEL_KEY] for deployment in deployments
    } == {'a', 'b'}
    revisions = {
        deployment['spec']['template']['metadata']['annotations'][
            lb_k8s.LB_RUNTIME_REVISION_ANNOTATION] for deployment in deployments
    }
    assert len(revisions) == 1
    for deployment in deployments:
        assert (deployment['spec']['template']['spec']['priorityClassName'] ==
                'skypilot-serve-lb')
        required = deployment['spec']['template']['spec']['affinity'][
            'podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution']
        assert required[-1]['topologyKey'] == 'kubernetes.io/hostname'

    service = core.create_namespaced_service.call_args.args[1]
    assert service['spec']['selector'] == {
        lb_k8s.LB_SLOT_LABEL_KEY: 'a',
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
    }
    assert service['metadata']['annotations'][
        lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY] in revisions
    pdb = policy.create_namespaced_pod_disruption_budget.call_args.args[1]
    assert pdb['spec']['minAvailable'] == 1
    assert pdb['spec']['selector']['matchLabels'][
        lb_k8s.SERVICE_HASH_LABEL_KEY] == 'incarnation'


def test_ha_reconcile_self_heals_only_missing_standby(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    policy = mock.MagicMock()
    active_name = lb_k8s.lb_slot_deployment_name('svc', lb_ha.LbSlot.A)

    def _active_exists(name, _namespace):
        if name == active_name:
            return _owned_object('incarnation', object_name=name)
        raise _ApiException(404)

    apps.read_namespaced_deployment.side_effect = _active_exists
    _install(monkeypatch, apps_api=apps, core_api=core, policy_api=policy)
    state = lb_ha.LbCutoverState(enabled=True,
                                 active_slot=lb_ha.LbSlot.A,
                                 generation=4,
                                 pending_slot=None,
                                 phase=lb_ha.LbCutoverPhase.STABLE,
                                 lifecycle_epoch=9)
    monkeypatch.setattr(lb_k8s.serve_state, 'get_lb_cutover_state',
                        lambda _name: state)
    monkeypatch.setattr(lb_k8s, '_wait_for_lb_deployment_ready',
                        lambda *_args, **_kwargs: None)

    lb_k8s.create_lb_deployment_and_service('svc',
                                            225,
                                            'incarnation',
                                            high_availability=True)

    created = [
        call.args[1]['metadata']['name']
        for call in apps.create_namespaced_deployment.call_args_list
    ]
    assert created == [lb_k8s.lb_slot_deployment_name('svc', lb_ha.LbSlot.B)]


def test_supervision_service_reconcile_rejects_stale_cutover_snapshot():
    state = lb_ha.LbCutoverState(enabled=True,
                                 active_slot=lb_ha.LbSlot.A,
                                 generation=4,
                                 pending_slot=None,
                                 phase=lb_ha.LbCutoverPhase.STABLE,
                                 lifecycle_epoch=9)
    guard = mock.MagicMock()
    guard.return_value.__enter__.return_value = False
    reconcile = mock.Mock(return_value=True)

    with mock.patch.object(lb_k8s.serve_state, 'lb_cutover_kubernetes_guard',
                           guard), pytest.raises(RuntimeError,
                                                 match='authority changed'):
        lb_k8s._run_ha_service_reconcile_guarded('svc', 'incarnation', state,
                                                 (123, '10.0.0.1'), reconcile)

    guard.assert_called_once_with('svc', 'incarnation', (123, '10.0.0.1'), 9,
                                  lb_ha.LbSlot.A, 4,
                                  lb_ha.LbCutoverPhase.STABLE, None)
    reconcile.assert_not_called()


def test_transition_reconcile_patches_only_desired_revision(monkeypatch):
    core = mock.MagicMock()
    patch_api = mock.MagicMock()
    core.create_namespaced_service.side_effect = _ApiException(409)
    core.read_namespaced_service.return_value = _owned_object(
        'incarnation', rv='11', object_name=lb_k8s.lb_service_name('svc'))
    _install(monkeypatch, core_api=core, patch_api=patch_api)
    owner = {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'name': 'skypilot-api-server',
        'uid': 'api-deployment-uid',
        'controller': False,
        'blockOwnerDeletion': False,
    }
    desired_revision = 'a' * 64
    service = lb_k8s._build_service_dict(
        'svc',
        lb_k8s.lb_service_name('svc'),
        lb_k8s.lb_slot_deployment_name('svc', lb_ha.LbSlot.A),
        'incarnation',
        owner,
        active_slot=lb_ha.LbSlot.A,
        cutover_generation=7,
        desired_runtime_revision=desired_revision)

    assert lb_k8s._reconcile_ha_service('in-cluster', 'skypilot', service,
                                        owner, 'incarnation', True,
                                        lambda _phase: None)

    body = _patch_calls(patch_api, _SERVICE_PATCH_PATH)[0].kwargs['body']
    assert body == {
        'metadata': {
            'resourceVersion': '11',
            'annotations': {
                lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
            },
        },
    }


def test_owned_pdb_spec_drift_fails_closed_without_patch(monkeypatch):
    policy = mock.MagicMock()
    policy.create_namespaced_pod_disruption_budget.side_effect = _ApiException(
        409)
    _install(monkeypatch, policy_api=policy)
    owner = {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'name': 'skypilot-api-server',
        'uid': 'api-deployment-uid',
        'controller': False,
        'blockOwnerDeletion': False,
    }
    desired = lb_k8s._build_pdb_dict('svc', 'svc-pdb', 'incarnation', owner)
    existing = {
        'metadata': {
            'resourceVersion': '7',
            'labels': desired['metadata']['labels'],
            'ownerReferences': [owner],
        },
        'spec': {
            **desired['spec'],
            'minAvailable': 2,
        },
    }
    policy.read_namespaced_pod_disruption_budget.return_value = existing

    with pytest.raises(RuntimeError, match='immutable specification drift'):
        lb_k8s._reconcile_owned_pdb('in-cluster', 'skypilot', desired, owner,
                                    'incarnation', lambda _phase: None)


def test_owned_pdb_ignores_server_defaults_when_contract_matches(monkeypatch):
    policy = mock.MagicMock()
    policy.create_namespaced_pod_disruption_budget.side_effect = _ApiException(
        409)
    _install(monkeypatch, policy_api=policy)
    owner = {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'name': 'skypilot-api-server',
        'uid': 'api-deployment-uid',
        'controller': False,
        'blockOwnerDeletion': False,
    }
    desired = lb_k8s._build_pdb_dict('svc', 'svc-pdb', 'incarnation', owner)
    policy.read_namespaced_pod_disruption_budget.return_value = {
        'metadata': {
            'resourceVersion': '7',
            'labels': desired['metadata']['labels'],
            'ownerReferences': [owner],
        },
        'spec': {
            **desired['spec'],
            'unhealthyPodEvictionPolicy': None,
        },
    }

    lb_k8s._reconcile_owned_pdb('in-cluster', 'skypilot', desired, owner,
                                'incarnation', lambda _phase: None)


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


@pytest.mark.parametrize('lb_priority_class_name', [None, ''])
def test_create_omits_empty_server_owned_lb_priority_class(
        monkeypatch, lb_priority_class_name):
    apps, _ = _install(monkeypatch,
                       priority_class_name='source-controller-priority',
                       lb_priority_class_name=lb_priority_class_name)

    lb_k8s.create_lb_deployment_and_service('svc-a', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    assert 'priorityClassName' not in deployment['spec']['template']['spec']


def test_create_uses_exact_server_owned_lb_priority_class(monkeypatch):
    apps, _ = _install(monkeypatch,
                       priority_class_name='source-controller-priority',
                       lb_priority_class_name='skypilot-serve-lb')

    lb_k8s.create_lb_deployment_and_service('svc-a', 225, 'incarnation')

    deployment = apps.create_namespaced_deployment.call_args.args[1]
    assert (deployment['spec']['template']['spec']['priorityClassName'] ==
            'skypilot-serve-lb')


def test_lb_priority_class_changes_ha_runtime_revision():
    compatibility_revision = lb_k8s._lb_runtime_revision(
        _DIGEST_A, 225, 'incarnation')
    assert compatibility_revision == lb_k8s._lb_runtime_revision(
        _DIGEST_A, 225, 'incarnation', '')
    assert compatibility_revision != lb_k8s._lb_runtime_revision(
        _DIGEST_A, 225, 'incarnation', 'skypilot-serve-lb')


def test_empty_lb_priority_class_patch_removes_previous_value(monkeypatch):
    monkeypatch.setenv('SKYPILOT_SERVE_API_SERVICE_URL',
                       'http://sky-api.skypilot.svc.cluster.local')
    deployment = lb_k8s._build_deployment_dict('svc', 'deploy', 'image:tag', [],
                                               [], [], [], {}, {},
                                               'IfNotPresent', 30)

    assert 'priorityClassName' not in deployment['spec']['template']['spec']
    patch = lb_k8s._deployment_patch_body(deployment, True)
    assert patch['spec']['template']['spec']['priorityClassName'] is None


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
            '$patch': 'replace',
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


def test_ha_ensure_passes_parent_owner_to_selector_reconcile(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    policy = mock.MagicMock()
    apps.read_namespaced_deployment.side_effect = _ApiException(404)
    core.read_namespaced_service.side_effect = _ApiException(404)
    policy.read_namespaced_pod_disruption_budget.side_effect = _ApiException(
        404)
    _install(monkeypatch,
             apps_api=apps,
             core_api=core,
             policy_api=policy,
             db_service_names=('svc',))
    state = lb_ha.LbCutoverState(enabled=True,
                                 active_slot=lb_ha.LbSlot.A,
                                 generation=4,
                                 pending_slot=None,
                                 phase=lb_ha.LbCutoverPhase.STABLE,
                                 lifecycle_epoch=9)
    monkeypatch.setattr(lb_k8s.serve_state, 'get_lb_cutover_state',
                        lambda _name: state)
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_controller_owner',
        lambda *_args, **_kwargs: {
            'controller_pid': os.getpid(),
            'controller_ip': '10.0.0.1',
            'hash': 'incarnation',
            'lifecycle_epoch': 9,
            'lb_ha_enabled': True,
        })

    with mock.patch.object(lb_k8s, '_create_ha_lb_objects') as create:
        assert lb_k8s.ensure_lb_objects_exist('svc',
                                              225,
                                              'incarnation',
                                              controller_ip='10.0.0.1',
                                              high_availability=True)

    assert create.call_args.kwargs['expected_controller_owner'] == (os.getpid(),
                                                                    '10.0.0.1')


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

    assert (lb_k8s.ensure_lb_objects_exist('svc', 225, 'incarnation')
            is expected_healthy)


def test_stable_legacy_ensure_retries_obsolete_ha_cleanup(monkeypatch):
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
        status=SimpleNamespace(load_balancer=SimpleNamespace(
            ingress=[SimpleNamespace(hostname='lb.example', ip=None)])))

    with mock.patch.object(lb_k8s,
                           '_retry_obsolete_lb_topology_cleanup') as cleanup:
        assert lb_k8s.ensure_lb_objects_exist('svc', 225, 'incarnation')

    cleanup.assert_called_once_with('svc', 'incarnation', False, None)


def test_obsolete_topology_cleanup_failure_is_retried_not_raised(
        monkeypatch, caplog):
    monkeypatch.setattr(
        lb_k8s, 'cleanup_lb_mode_transition',
        mock.Mock(side_effect=RuntimeError('transient delete failure')))

    lb_k8s._retry_obsolete_lb_topology_cleanup('svc', 'incarnation', True, None)

    assert 'will retry' in caplog.text


def test_migration_selector_patch_replaces_legacy_keys_and_is_rv_fenced(
        monkeypatch):
    _install(monkeypatch, db_service_names=('svc',))
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_controller_owner',
        lambda *_args, **_kwargs: {
            'hash': 'incarnation',
            'resource_scope': None,
            'lb_ha_enabled': True,
        })
    routing = lb_k8s.LbServiceTransitionRouting(None, True, None, 'rv-8')
    guard = mock.MagicMock()
    guard.return_value.__enter__.return_value = True
    with mock.patch.object(lb_k8s.serve_state, 'lb_cutover_kubernetes_guard',
                           guard), mock.patch.object(
                               lb_k8s,
                               'get_lb_service_transition_routing',
                               return_value=routing), mock.patch.object(
                                   lb_k8s, '_strategic_merge_patch') as patch:
        assert lb_k8s.patch_lb_service_migration_to_slot(
            'svc', 'incarnation', (7, '10.0.0.7'), 11)

    body = patch.call_args.args[-1]
    assert body['metadata']['resourceVersion'] == 'rv-8'
    assert body['spec']['selector'] == {
        '$patch': 'replace',
        lb_k8s.LB_SLOT_LABEL_KEY: 'a',
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
    }
    assert lb_k8s.APP_LABEL_KEY not in body['spec']['selector']


def test_selector_patch_conflict_and_failed_database_guard_do_not_commit(
        monkeypatch):
    _install(monkeypatch, db_service_names=('svc',))
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_controller_owner',
        lambda *_args, **_kwargs: {
            'hash': 'incarnation',
            'resource_scope': None,
            'lb_ha_enabled': True,
        })
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')
    guard = mock.MagicMock()
    guard.return_value.__enter__.return_value = False
    with mock.patch.object(lb_k8s.serve_state, 'lb_cutover_kubernetes_guard',
                           guard), mock.patch.object(
                               lb_k8s, '_strategic_merge_patch') as patch:
        assert not lb_k8s.patch_lb_service_active_slot(
            'svc', 'incarnation',
            (7, '10.0.0.7'), 11, lb_ha.LbSlot.A, 1, lb_ha.LbSlot.B, 2)
    patch.assert_not_called()

    guard.return_value.__enter__.return_value = True
    with mock.patch.object(lb_k8s.serve_state, 'lb_cutover_kubernetes_guard',
                           guard), mock.patch.object(
                               lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing), mock.patch.object(
                                   lb_k8s,
                                   '_strategic_merge_patch',
                                   side_effect=_ApiException(409)):
        assert not lb_k8s.patch_lb_service_active_slot(
            'svc', 'incarnation',
            (7, '10.0.0.7'), 11, lb_ha.LbSlot.A, 1, lb_ha.LbSlot.B, 2)


def _lb_pod(uid,
            phase='Running',
            deleting=False,
            ready=True,
            labels=None,
            annotations=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid,
                                 deletion_timestamp='now' if deleting else None,
                                 labels=labels or {},
                                 annotations=annotations or {}),
        status=SimpleNamespace(phase=phase,
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


@pytest.mark.parametrize('deleting', [False, True])
def test_ha_pod_authority_keeps_stable_legacy_migration_tail(
        monkeypatch, deleting):
    apps, core = _install(monkeypatch, db_service_names=('svc',))
    desired_revision = 'a' * 64
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_controller_owner',
        lambda *_args, **_kwargs: {
            'hash': 'incarnation',
            'resource_scope': None,
            'lb_ha_enabled': True,
            'lb_cutover_phase': lb_ha.LbCutoverPhase.STABLE.value,
        })
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='lb-service-rv',
            annotations={
                lb_k8s.ACTIVE_SLOT_ANNOTATION_KEY: lb_ha.LbSlot.A.value,
                lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY: '1',
                lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
            },
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(
            selector={
                lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            }))
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[
        _lb_pod('slot-a',
                labels={
                    lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
                }),
        _lb_pod('legacy',
                deleting=deleting,
                labels={
                    lb_k8s.APP_LABEL_KEY: lb_k8s.lb_deployment_name('svc'),
                }),
    ])

    assert lb_k8s.get_lb_pod_authority('svc') == lb_k8s.LbPodAuthority(
        ready_nonterminating_uids=({'slot-a'}
                                   if deleting else {'slot-a', 'legacy'}),
        live_uids={'slot-a', 'legacy'},
        slot_by_uid={'slot-a': lb_ha.LbSlot.A},
        selected_slot=lb_ha.LbSlot.A,
        digest_by_uid={
            'slot-a': None,
            'legacy': None,
        },
        revision_by_uid={
            'slot-a': None,
            'legacy': None,
        },
        legacy_uids={'legacy'},
        terminating_uids=({'legacy'} if deleting else set()))
    # The read-only HA authority path should validate the Service's owner
    # identity with one live Deployment UID check, not a duplicate pre-read.
    assert apps.read_namespaced_deployment.call_count == 1

    # A slotless Pod without the exact legacy Deployment label is malformed
    # and still fails closed instead of joining HA authority.
    core.list_namespaced_pod.return_value.items[1].metadata.labels = {
        lb_k8s.APP_LABEL_KEY: 'unexpected-deployment',
    }
    assert lb_k8s.get_lb_pod_authority('svc') is None


def test_ha_pod_authority_fails_closed_when_owner_deployment_is_replaced(
        monkeypatch):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    desired_revision = 'a' * 64
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_controller_owner',
        lambda *_args, **_kwargs: {
            'hash': 'incarnation',
            'resource_scope': None,
            'lb_ha_enabled': True,
            'lb_cutover_phase': lb_ha.LbCutoverPhase.STABLE.value,
        })
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='lb-service-rv',
            annotations={
                lb_k8s.ACTIVE_SLOT_ANNOTATION_KEY: lb_ha.LbSlot.A.value,
                lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY: '1',
                lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
            },
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(
            selector={
                lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            }))
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[
        _lb_pod('slot-a',
                labels={
                    lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
                }),
    ])
    monkeypatch.setattr(lb_k8s, '_live_deployment_owner_uid',
                        lambda *_args: 'replacement-uid')

    assert lb_k8s.get_lb_pod_authority('svc') is None


def test_role_snapshot_reuses_owner_pods_and_service(monkeypatch):
    apps, core = _install(monkeypatch, db_service_names=('svc',))
    desired_revision = 'a' * 64
    owner = {
        'hash': 'incarnation',
        'resource_scope': None,
        'lb_ha_enabled': True,
        'lb_cutover_phase': lb_ha.LbCutoverPhase.STABLE.value,
        'lifecycle_epoch': 7,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lb_active_slot': lb_ha.LbSlot.A.value,
        'lb_cutover_generation': 3,
        'lb_pending_slot': None,
    }
    owner_read = mock.Mock(return_value=owner)
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_controller_owner',
                        owner_read)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='lb-service-rv',
            annotations={
                lb_k8s.ACTIVE_SLOT_ANNOTATION_KEY: lb_ha.LbSlot.A.value,
                lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY: '3',
                lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
            },
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(external_traffic_policy='Cluster',
                             selector={
                                 lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
                                 lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
                             }))
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[
        _lb_pod('slot-a',
                labels={
                    lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
                }),
        _lb_pod('slot-b',
                labels={
                    lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.B.value,
                }),
    ])
    timings = {}
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    state = lb_ha.LbCutoverState(True, lb_ha.LbSlot.A, 3, None,
                                 lb_ha.LbCutoverPhase.STABLE, 7)

    snapshot = lb_k8s.get_lb_role_snapshot('svc', fence, state, owner, timings)

    assert snapshot is not None
    assert snapshot.routing == lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 3,
                                                       'lb-service-rv',
                                                       desired_revision)
    assert snapshot.authority.slot_by_uid == {
        'slot-a': lb_ha.LbSlot.A,
        'slot-b': lb_ha.LbSlot.B,
    }
    owner_read.assert_not_called()
    core.list_namespaced_pod.assert_called_once()
    core.read_namespaced_service.assert_called_once()
    expected_timeout = constants.LB_ROLE_SNAPSHOT_TIMEOUT_SECONDS
    assert (core.list_namespaced_pod.call_args.kwargs['_request_timeout'] ==
            expected_timeout)
    assert (core.read_namespaced_service.call_args.kwargs['_request_timeout'] ==
            expected_timeout)
    # The Service supplies the expected owner identity.  One subsequent live
    # Deployment read proves that exact UID without an earlier duplicate GET.
    assert apps.read_namespaced_deployment.call_count == 1
    assert (apps.read_namespaced_deployment.call_args.kwargs['_request_timeout']
            == expected_timeout)
    assert set(timings) == {
        'snapshot_pod_list',
        'snapshot_service_read',
        'snapshot_ownership_validation',
        'snapshot_parse_routing',
        'snapshot_parse_pods',
    }


def test_role_snapshot_joins_independent_kubernetes_reads(monkeypatch):
    apps, core = _install(monkeypatch, db_service_names=('svc',))
    desired_revision = 'a' * 64
    owner = {
        'hash': 'incarnation',
        'resource_scope': None,
        'lb_ha_enabled': True,
        'lb_cutover_phase': lb_ha.LbCutoverPhase.STABLE.value,
        'lifecycle_epoch': 7,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lb_active_slot': lb_ha.LbSlot.A.value,
        'lb_cutover_generation': 3,
        'lb_pending_slot': None,
    }
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_controller_owner',
                        lambda *_args, **_kwargs: owner)
    service = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='lb-service-rv',
            annotations={
                lb_k8s.ACTIVE_SLOT_ANNOTATION_KEY: lb_ha.LbSlot.A.value,
                lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY: '3',
                lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
            },
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(external_traffic_policy='Cluster',
                             selector={
                                 lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
                                 lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
                             }))
    pods = SimpleNamespace(items=[
        _lb_pod('slot-a',
                labels={lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value}),
        _lb_pod('slot-b',
                labels={lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.B.value}),
    ])
    deployment = SimpleNamespace(metadata=SimpleNamespace(
        uid='api-deployment-uid'))
    reads_started = threading.Barrier(2, timeout=5)
    deployment_call_lock = threading.Lock()
    deployment_calls = 0
    caller_context = contextvars.ContextVar('role_snapshot_caller_context')
    caller_context_token = caller_context.set('controller-request')

    def list_pods(*_args, **_kwargs):
        assert caller_context.get() == 'controller-request'
        reads_started.wait()
        return pods

    def read_service(*_args, **_kwargs):
        assert caller_context.get() == 'controller-request'
        reads_started.wait()
        return service

    def read_deployment(*_args, **_kwargs):
        nonlocal deployment_calls
        with deployment_call_lock:
            deployment_calls += 1
        assert caller_context.get() == 'controller-request'
        return deployment

    core.list_namespaced_pod.side_effect = list_pods
    core.read_namespaced_service.side_effect = read_service
    apps.read_namespaced_deployment.side_effect = read_deployment
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    state = lb_ha.LbCutoverState(True, lb_ha.LbSlot.A, 3, None,
                                 lb_ha.LbCutoverPhase.STABLE, 7)

    try:
        snapshot = lb_k8s.get_lb_role_snapshot('svc', fence, state, owner)
    finally:
        caller_context.reset(caller_context_token)

    assert snapshot is not None
    assert snapshot.routing == lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 3,
                                                       'lb-service-rv',
                                                       desired_revision)
    assert snapshot.authority.slot_by_uid == {
        'slot-a': lb_ha.LbSlot.A,
        'slot-b': lb_ha.LbSlot.B,
    }
    # The Service carries the expected identity; one post-join live read is the
    # final owner-replacement linearization point.
    assert deployment_calls == 1


def test_role_snapshot_fails_closed_on_malformed_shared_service(monkeypatch):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    owner = {
        'hash': 'incarnation',
        'resource_scope': None,
        'lb_ha_enabled': True,
        'lb_cutover_phase': lb_ha.LbCutoverPhase.STABLE.value,
        'lifecycle_epoch': 7,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lb_active_slot': lb_ha.LbSlot.A.value,
        'lb_cutover_generation': 3,
        'lb_pending_slot': None,
    }
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_controller_owner',
                        lambda *_args, **_kwargs: owner)
    core.read_namespaced_service.return_value.spec.external_traffic_policy = (
        'Local')
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[])

    fence = ('incarnation', (123, '10.0.0.1'), 7)
    state = lb_ha.LbCutoverState(True, lb_ha.LbSlot.A, 3, None,
                                 lb_ha.LbCutoverPhase.STABLE, 7)
    with pytest.raises(lb_k8s.LbRoleSnapshotRoutingError):
        lb_k8s.get_lb_role_snapshot('svc', fence, state, owner)


def test_role_snapshot_rejects_owner_row_state_mismatch_before_kubernetes(
        monkeypatch):
    apps, core = _install(monkeypatch, db_service_names=('svc',))
    owner = {
        'hash': 'incarnation',
        'resource_scope': None,
        'lb_ha_enabled': True,
        'lb_cutover_phase': lb_ha.LbCutoverPhase.STABLE.value,
        'lifecycle_epoch': 7,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lb_active_slot': lb_ha.LbSlot.A.value,
        'lb_cutover_generation': 4,
        'lb_pending_slot': None,
    }
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_controller_owner',
                        lambda *_args, **_kwargs: owner)
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    stale_state = lb_ha.LbCutoverState(True, lb_ha.LbSlot.A, 3, None,
                                       lb_ha.LbCutoverPhase.STABLE, 7)

    with pytest.raises(lb_k8s.LbRoleSnapshotStateMismatchError):
        lb_k8s.get_lb_role_snapshot('svc', fence, stale_state, owner)

    core.list_namespaced_pod.assert_not_called()
    core.read_namespaced_service.assert_not_called()
    apps.read_namespaced_deployment.assert_not_called()


def test_role_snapshot_fails_closed_when_owner_deployment_is_replaced(
        monkeypatch):
    _, core = _install(monkeypatch, db_service_names=('svc',))
    desired_revision = 'a' * 64
    owner = {
        'hash': 'incarnation',
        'resource_scope': None,
        'lb_ha_enabled': True,
        'lb_cutover_phase': lb_ha.LbCutoverPhase.STABLE.value,
        'lifecycle_epoch': 7,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lb_active_slot': lb_ha.LbSlot.A.value,
        'lb_cutover_generation': 3,
        'lb_pending_slot': None,
    }
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_controller_owner',
                        lambda *_args, **_kwargs: owner)
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='lb-service-rv',
            annotations={
                lb_k8s.ACTIVE_SLOT_ANNOTATION_KEY: lb_ha.LbSlot.A.value,
                lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY: '3',
                lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
            },
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(external_traffic_policy='Cluster',
                             selector={
                                 lb_k8s.LB_SLOT_LABEL_KEY: 'a',
                                 lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
                             }))
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    monkeypatch.setattr(lb_k8s, '_live_deployment_owner_uid',
                        lambda *_args: 'replacement-uid')
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    state = lb_ha.LbCutoverState(True, lb_ha.LbSlot.A, 3, None,
                                 lb_ha.LbCutoverPhase.STABLE, 7)

    with pytest.raises(lb_k8s.LbRoleSnapshotRoutingError):
        lb_k8s.get_lb_role_snapshot('svc', fence, state, owner)


@pytest.mark.parametrize('owner_references', [
    [],
    [_owner_reference(owner_name='other-api')],
    [_owner_reference(owner_uid='')],
    [_owner_reference(), _owner_reference()],
])
def test_role_snapshot_live_owner_validation_rejects_malformed_identity(
        monkeypatch, owner_references):
    existing = _owned_object(service_hash='incarnation')
    existing.metadata.owner_references = owner_references
    live_owner_read = mock.Mock(return_value='api-deployment-uid')
    monkeypatch.setattr(lb_k8s, '_live_deployment_owner_uid', live_owner_read)

    with pytest.raises(RuntimeError):
        lb_k8s._require_existing_lb_object_live_ownership(
            'context', 'namespace', 'service', existing, 'incarnation')

    live_owner_read.assert_not_called()


@pytest.mark.parametrize(
    'phase',
    [lb_ha.LbCutoverPhase.MIGRATING, lb_ha.LbCutoverPhase.ROLLING_BACK])
@pytest.mark.parametrize('legacy_selected', [False, True])
def test_role_snapshot_transition_routing_matches_existing_contract(
        monkeypatch, phase, legacy_selected):
    apps, core = _install(monkeypatch, db_service_names=('svc',))
    desired_revision = 'b' * 64
    owner = {
        'hash': 'incarnation',
        'resource_scope': None,
        'lb_ha_enabled': True,
        'lb_cutover_phase': phase.value,
        'lifecycle_epoch': 7,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lb_active_slot': lb_ha.LbSlot.A.value,
        'lb_cutover_generation': 3,
        'lb_pending_slot': None,
    }
    monkeypatch.setattr(lb_k8s.serve_state, 'get_service_controller_owner',
                        lambda *_args, **_kwargs: owner)
    selector = ({
        lb_k8s.APP_LABEL_KEY: lb_k8s.lb_deployment_name('svc'),
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
    } if legacy_selected else {
        lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value,
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
    })
    annotations = {
        lb_k8s.DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
    }
    if not legacy_selected:
        annotations.update({
            lb_k8s.ACTIVE_SLOT_ANNOTATION_KEY: lb_ha.LbSlot.A.value,
            lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY: '3',
        })
    core.read_namespaced_service.return_value = SimpleNamespace(
        metadata=SimpleNamespace(
            resource_version='lb-service-rv',
            annotations=annotations,
            labels={
                lb_k8s.SERVE_LB_LABEL_KEY: 'svc',
                lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
            },
            owner_references=[_owner_reference()]),
        spec=SimpleNamespace(selector=selector))
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[
        _lb_pod('slot-a',
                labels={lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.A.value}),
        _lb_pod('slot-b',
                labels={lb_k8s.LB_SLOT_LABEL_KEY: lb_ha.LbSlot.B.value}),
    ])
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    state = lb_ha.LbCutoverState(True, lb_ha.LbSlot.A, 3, None, phase, 7)

    snapshot = lb_k8s.get_lb_role_snapshot('svc', fence, state, owner)

    assert snapshot is not None
    assert snapshot.routing == lb_k8s.LbServiceTransitionRouting(
        None if legacy_selected else lb_ha.LbSlot.A, legacy_selected,
        None if legacy_selected else 3, 'lb-service-rv', desired_revision)
    assert '_request_timeout' not in core.list_namespaced_pod.call_args.kwargs
    assert ('_request_timeout'
            not in core.read_namespaced_service.call_args.kwargs)
    assert ('_request_timeout'
            not in apps.read_namespaced_deployment.call_args.kwargs)


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


def test_delete_wait_timeout_uses_monotonic_deadline(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.read_namespaced_deployment.side_effect = _ApiException(404)
    _install(monkeypatch, apps_api=apps, core_api=core)
    monkeypatch.setattr(lb_k8s, '_lb_object_deletion_timeout_seconds',
                        lambda obj, kind: 0.3)
    wall_clock = mock.Mock(
        side_effect=AssertionError('elapsed timeout consulted wall clock'))
    monotonic_clock = [10.0]
    sleeps = []
    poll_timeouts = []
    monotonic = mock.Mock(side_effect=lambda: monotonic_clock[0])

    def _read_service(name, namespace, **kwargs):
        del name, namespace
        if core.read_namespaced_service.call_count > 1:
            poll_timeouts.append(kwargs.get('_request_timeout'))
            monotonic_clock[0] += 0.15
        return _owned_object('incarnation-a', 'service-a', '9')

    def _sleep(delay):
        sleeps.append(delay)
        monotonic_clock[0] += delay

    core.read_namespaced_service.side_effect = _read_service
    monkeypatch.setattr(
        lb_k8s, 'time',
        SimpleNamespace(time=wall_clock, monotonic=monotonic, sleep=_sleep))

    with pytest.raises(TimeoutError, match='Timed out waiting'):
        lb_k8s.delete_lb_objects('svc', 'incarnation-a')

    wall_clock.assert_not_called()
    assert monotonic.call_count == 4
    core.delete_namespaced_service.assert_called_once()
    assert core.read_namespaced_service.call_count == 2
    assert poll_timeouts == pytest.approx([0.3])
    assert sleeps == pytest.approx([0.15])
    assert monotonic_clock[0] == pytest.approx(10.3)


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


def _https_env(monkeypatch,
               cert: str | None = 'arn:aws:acm:us-east-1:1234:certificate/abc',
               suffix: str | None = 'int.example.test',
               policy: str | None = None,
               https_only: str | None = None) -> None:
    for var, value in (
        (constants.EXTERNAL_LB_HTTPS_CERT_ARN_ENV_VAR, cert),
        (constants.EXTERNAL_LB_HTTPS_DNS_SUFFIX_ENV_VAR, suffix),
        (constants.EXTERNAL_LB_HTTPS_SSL_POLICY_ENV_VAR, policy),
        (constants.EXTERNAL_LB_HTTPS_ONLY_ENV_VAR, https_only),
    ):
        if value is None:
            monkeypatch.delenv(var, raising=False)
        else:
            monkeypatch.setenv(var, value)


def test_service_dict_unchanged_without_https_config(monkeypatch):
    _https_env(monkeypatch, cert=None, suffix=None)
    service = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    assert 'annotations' not in service['metadata']
    assert service['spec']['ports'] == [{
        'port': constants.LOAD_BALANCER_PORT_START,
        'targetPort': constants.LOAD_BALANCER_PORT_START,
        'protocol': 'TCP',
    }]


def test_service_dict_adds_tls_listener_and_hostname(monkeypatch):
    _https_env(monkeypatch)
    service = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    annotations = service['metadata']['annotations']
    assert annotations[lb_k8s._AWS_LB_SSL_CERT_ANNOTATION] == (
        'arn:aws:acm:us-east-1:1234:certificate/abc')
    assert annotations[lb_k8s._AWS_LB_SSL_PORTS_ANNOTATION] == 'https'
    assert annotations[lb_k8s._AWS_LB_SSL_POLICY_ANNOTATION] == (
        constants.DEFAULT_EXTERNAL_LB_SSL_POLICY)
    assert annotations[lb_k8s._EXTERNAL_DNS_HOSTNAME_ANNOTATION] == (
        f'{lb_k8s.lb_base_name("svc")}.int.example.test')
    # Both listeners during migration, and every port named.
    assert service['spec']['ports'] == [
        {
            'name': 'http',
            'port': constants.LOAD_BALANCER_PORT_START,
            'targetPort': constants.LOAD_BALANCER_PORT_START,
            'protocol': 'TCP',
        },
        {
            'name': 'https',
            'port': 443,
            'targetPort': constants.LOAD_BALANCER_PORT_START,
            'protocol': 'TCP',
        },
    ]


def test_https_only_drops_the_plaintext_listener(monkeypatch):
    _https_env(monkeypatch, https_only='true')
    service = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    assert [port['port'] for port in service['spec']['ports']] == [443]


def test_https_hostname_survives_incarnation_change(monkeypatch):
    _https_env(monkeypatch)
    first = lb_k8s._build_service_dict(
        'svc', lb_k8s.lb_service_name('svc', 'incarnation-1'), 'deploy')
    second = lb_k8s._build_service_dict(
        'svc', lb_k8s.lb_service_name('svc', 'incarnation-2'), 'deploy')
    hostname_key = lb_k8s._EXTERNAL_DNS_HOSTNAME_ANNOTATION
    # The Service object name is incarnation-scoped, but a consumer-facing
    # hostname that moved on every down/up would be unusable.
    assert first['metadata']['name'] != second['metadata']['name']
    assert (first['metadata']['annotations'][hostname_key] == second['metadata']
            ['annotations'][hostname_key])


@pytest.mark.parametrize(('cert', 'suffix'),
                         [('arn:aws:acm:us-east-1:1234:certificate/abc', None),
                          (None, 'int.example.test')])
def test_partial_https_config_fails_closed(monkeypatch, cert, suffix):
    _https_env(monkeypatch, cert=cert, suffix=suffix)
    with pytest.raises(ValueError):
        lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                   'deploy')


def test_tls_does_not_disturb_the_ha_selector(monkeypatch):
    """TLS must not perturb selector-only cutover."""
    _https_env(monkeypatch, cert=None, suffix=None)
    plain = lb_k8s._build_service_dict('svc',
                                       lb_k8s.lb_service_name('svc'),
                                       'deploy',
                                       'incarnation',
                                       active_slot=lb_ha.LbSlot.A,
                                       cutover_generation=3)
    _https_env(monkeypatch)
    secured = lb_k8s._build_service_dict('svc',
                                         lb_k8s.lb_service_name('svc'),
                                         'deploy',
                                         'incarnation',
                                         active_slot=lb_ha.LbSlot.A,
                                         cutover_generation=3)
    assert plain['spec']['selector'] == secured['spec']['selector']
    for key in (lb_k8s.ACTIVE_SLOT_ANNOTATION_KEY,
                lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY):
        assert (plain['metadata']['annotations'][key] == secured['metadata']
                ['annotations'][key])


def test_routing_reconciles_a_live_service_missing_tls(monkeypatch):
    _https_env(monkeypatch)
    desired = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    live_plaintext = {
        'metadata': {
            'annotations': {}
        },
        'spec': {
            'type': 'LoadBalancer',
            'externalTrafficPolicy': 'Cluster',
            'selector': desired['spec']['selector'],
            'ports': [{
                'port': constants.LOAD_BALANCER_PORT_START,
                'targetPort': constants.LOAD_BALANCER_PORT_START,
                'protocol': 'TCP',
            }],
        },
    }
    assert not lb_k8s._service_has_desired_routing(live_plaintext, desired)

    reconciled = {
        'metadata': {
            'annotations': {
                **desired['metadata']['annotations'],
                # The AWS controller injects its own annotations; a subset
                # comparison must tolerate them instead of churning forever.
                'service.beta.kubernetes.io/aws-load-balancer-type': 'external',
            }
        },
        'spec': {
            **desired['spec'],
        },
    }
    assert lb_k8s._service_has_desired_routing(reconciled, desired)


def test_ports_patch_deletes_the_listener_we_dropped(monkeypatch):
    """Omitting a port from a strategic merge KEEPS it; convergence needs a
    delete directive, or the reconciler re-runs forever in both directions."""
    _https_env(monkeypatch, https_only='true')
    desired = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    patch_ports = lb_k8s._service_ports_patch(desired['spec']['ports'])
    assert {
        'port': constants.LOAD_BALANCER_PORT_START,
        '$patch': 'delete',
    } in patch_ports
    assert any(
        port.get('port') == 443 and '$patch' not in port
        for port in patch_ports)


def test_ports_patch_deletes_tls_on_rollback(monkeypatch):
    _https_env(monkeypatch, cert=None, suffix=None)
    desired = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    patch_ports = lb_k8s._service_ports_patch(desired['spec']['ports'])
    assert {'port': 443, '$patch': 'delete'} in patch_ports


def test_ports_patch_is_inert_while_dual_listening(monkeypatch):
    _https_env(monkeypatch)
    desired = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    patch_ports = lb_k8s._service_ports_patch(desired['spec']['ports'])
    assert patch_ports == desired['spec']['ports']


def test_ports_patch_never_deletes_a_port_we_do_not_own():
    patch_ports = lb_k8s._service_ports_patch([{'port': 9999}])
    assert {'port': 9999} in patch_ports
    deleted = {p['port'] for p in patch_ports if p.get('$patch') == 'delete'}
    assert 9999 not in deleted


def test_ports_patch_leaves_a_foreign_port_name_alone():
    """The name clear is scoped to owned ports; an operator name is untouched."""
    patch_ports = lb_k8s._service_ports_patch([{'port': 9999}])
    foreign = next(p for p in patch_ports if p.get('port') == 9999)
    assert 'name' not in foreign


def _apply_service_ports_strategic_merge(live_ports: list,
                                         patch_ports: list) -> list:
    """Model a Kubernetes strategic-merge patch of ``Service.spec.ports``.

    ``ports`` carries ``patchMergeKey=port`` / ``patchStrategy=merge``: patch
    elements are matched to live elements by ``port`` and merged field by field.
    A field the patch omits is RETAINED, an explicit ``None`` (JSON null)
    DELETES the field, and ``{'$patch': 'delete'}`` removes the whole element.
    That retention is exactly what the production reconciler rides on; the other
    port tests mock ``_strategic_merge_patch`` and so never round-trip it.
    """
    merged: dict = {}
    order: list = []
    for port in live_ports:
        merged[port['port']] = dict(port)
        order.append(port['port'])
    for entry in patch_ports:
        number = entry.get('port')
        if entry.get('$patch') == 'delete':
            merged.pop(number, None)
            continue
        if number not in merged:
            order.append(number)
        current = merged.get(number, {})
        for key, value in entry.items():
            if key == '$patch':
                continue
            if value is None:
                current.pop(key, None)
            else:
                current[key] = value
        merged[number] = current
    return [merged[number] for number in order if number in merged]


def test_ports_patch_converges_after_disabling_a_dual_listen_service(
        monkeypatch):
    """Dropping the renamed plaintext listener must converge in one patch.

    Enabling TLS renames the pre-existing plaintext port to ``http`` (>1 port
    needs names). Fully disabling HTTPS again wants it unnamed, but strategic
    merge keeps any field the patch omits, so without an explicit ``name`` clear
    the live port keeps ``http`` and ``_service_has_desired_routing`` reports
    drift and re-patches forever -- the same non-convergence the port delete
    directive fixes, on the ``name`` field this PR added to the drift tuple.
    """
    # Live state left by an earlier dual-listen reconcile: renamed plaintext
    # port plus the TLS listener.
    live_ports = [
        {
            'name': constants.EXTERNAL_LB_HTTP_PORT_NAME,
            'port': constants.LOAD_BALANCER_PORT_START,
            'targetPort': constants.LOAD_BALANCER_PORT_START,
            'protocol': 'TCP',
        },
        {
            'name': constants.EXTERNAL_LB_HTTPS_PORT_NAME,
            'port': constants.EXTERNAL_LB_HTTPS_PORT,
            'targetPort': constants.LOAD_BALANCER_PORT_START,
            'protocol': 'TCP',
        },
    ]
    # Operator unsets the HTTPS config -> desired reverts to one unnamed port.
    _https_env(monkeypatch, cert=None, suffix=None)
    desired = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    patch_ports = lb_k8s._service_ports_patch(desired['spec']['ports'])
    # Mechanism (model-independent): the owned plaintext port carries an
    # explicit name clear so the merge can drop the stale ``http``.
    assert {
        'name': None,
        'port': constants.LOAD_BALANCER_PORT_START,
        'targetPort': constants.LOAD_BALANCER_PORT_START,
        'protocol': 'TCP',
    } in patch_ports

    # Property: applying the patch to the live Service clears the name and the
    # drift check then reports convergence, so the reconciler stops re-patching.
    merged_ports = _apply_service_ports_strategic_merge(live_ports, patch_ports)
    live_service = {
        'metadata': {
            'annotations': dict(desired['metadata'].get('annotations', {})),
        },
        'spec': {
            'type': desired['spec']['type'],
            'externalTrafficPolicy': desired['spec']['externalTrafficPolicy'],
            'selector': desired['spec']['selector'],
            'ports': merged_ports,
        },
    }
    assert lb_k8s._service_has_desired_routing(live_service, desired)


def _lb_container(monkeypatch) -> dict:
    """The LB container spec, with the many runtime args defaulted."""
    monkeypatch.setenv('SKYPILOT_SERVE_API_SERVICE_URL',
                       'http://sky-api.skypilot.svc.cluster.local')
    deployment = lb_k8s._build_deployment_dict('svc', 'deploy', 'image:tag', [],
                                               [], [], [], {}, {},
                                               'IfNotPresent', 30)
    return deployment['spec']['template']['spec']['containers'][0]


# "Within the instance it is ok, but between instances it should be https."
# The LB pod's own hop from the NLB is between machines, so under HTTPS_ONLY
# the NLB re-encrypts to the pod and the pod serves TLS. All three kubelet
# probes must follow, or every LB pod CrashLoops and every Service empties.


def test_backend_stays_plaintext_while_dual_listening(monkeypatch):
    """The annotation is per-Service, so it cannot coexist with 30001."""
    _https_env(monkeypatch)
    service = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    assert (constants.AWS_LB_BACKEND_PROTOCOL_ANNOTATION
            not in service['metadata']['annotations'])
    container = _lb_container(monkeypatch)
    for probe in ('startupProbe', 'readinessProbe', 'livenessProbe'):
        assert 'scheme' not in container[probe]['httpGet']


def test_backend_reencrypts_and_probes_follow_under_https_only(monkeypatch):
    _https_env(monkeypatch, https_only='true')
    service = lb_k8s._build_service_dict('svc', lb_k8s.lb_service_name('svc'),
                                         'deploy')
    assert (service['metadata']['annotations'][
        constants.AWS_LB_BACKEND_PROTOCOL_ANNOTATION] ==
            constants.AWS_LB_BACKEND_PROTOCOL_SSL)
    container = _lb_container(monkeypatch)
    for probe in ('startupProbe', 'readinessProbe', 'livenessProbe'):
        assert container[probe]['httpGet']['scheme'] == 'HTTPS', probe
    # A TLS handshake per probe does not fit the plaintext budget, and
    # readiness has failureThreshold 1.
    assert container['readinessProbe']['timeoutSeconds'] > 1
