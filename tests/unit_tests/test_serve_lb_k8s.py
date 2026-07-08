"""Logic-only tests for the controller-owned external LB lifecycle.

These tests exercise only name/endpoint logic and which k8s API calls are
issued (create/delete/list) under mocked clients. They never assert on log or
exception message text.
"""
import re
import unittest
from unittest import mock

import pytest

from sky.serve import constants
from sky.serve import lb_k8s

# RFC1123 subdomain-ish check for object names: lowercase alnum + '-', starts
# and ends alphanumeric, <= 63 chars.
_RFC1123 = re.compile(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?$')


class _ApiException(Exception):
    """Stand-in for kubernetes.client.rest.ApiException with a status."""

    def __init__(self, status):
        super().__init__(f'status={status}')
        self.status = status


def _install(monkeypatch,
             apps_api=None,
             core_api=None,
             external=True,
             incluster=True,
             namespace='skypilot',
             token=None,
             token_secret=None,
             lb_token=None,
             lb_token_secret=None,
             pod_name='controller-pod-0',
             image='my-repo/skypilot:v1',
             db_service_names=None):
    """Patch environment probes + the k8s adaptor for lb_k8s."""
    monkeypatch.setattr(lb_k8s.serve_utils, 'is_external_load_balancer_mode',
                        lambda: external)
    # reconcile re-checks the DB before reaping an orphan; by default every
    # service is treated as absent (truly gone). Tests can pass a set of names
    # the DB should still report as live.
    live_in_db = set(db_service_names or ())
    monkeypatch.setattr(
        lb_k8s.serve_state, 'get_service_from_name',
        lambda name: {'name': name} if name in live_in_db else None)
    monkeypatch.setattr(lb_k8s.kubernetes_utils,
                        'is_incluster_config_available', lambda: incluster)
    monkeypatch.setattr(lb_k8s.kubernetes_utils,
                        'get_kube_config_context_namespace',
                        lambda ctx: namespace)
    monkeypatch.setattr(lb_k8s.kubernetes, 'in_cluster_context_name',
                        lambda: 'in-cluster')
    monkeypatch.setattr(lb_k8s.serve_utils, 'get_controller_auth_token',
                        lambda: token)
    monkeypatch.setattr(lb_k8s.serve_utils, 'get_lb_auth_token',
                        lambda: lb_token)
    if pod_name is not None:
        monkeypatch.setenv(constants.POD_NAME_ENV_VAR, pod_name)
    else:
        monkeypatch.delenv(constants.POD_NAME_ENV_VAR, raising=False)

    apps_api = apps_api if apps_api is not None else mock.MagicMock()
    core_api = core_api if core_api is not None else mock.MagicMock()

    # read_namespaced_pod(...).spec.containers[0] -> image + (optionally) the
    # controller's auth-token envs, which _resolve_lb_auth_envs mirrors onto the
    # LB. *_secret=(name, key) makes the controller carry that token as a
    # secretKeyRef (the prod/ESO shape); when None but the token is set, the
    # controller carries it inline (the fallback path).
    def _mk_env(name, value, secret):
        ev = mock.MagicMock()
        ev.name = name
        if secret is not None:
            ev.value = None
            skr = mock.MagicMock()
            skr.name, skr.key = secret
            skr.optional = None
            ev.value_from.secret_key_ref = skr
        else:
            ev.value = value
            ev.value_from = None
        return ev

    pod = mock.MagicMock()
    container = mock.MagicMock()
    container.image = image
    env_list = []
    if token is not None:
        env_list.append(
            _mk_env(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, token,
                    token_secret))
    if lb_token is not None:
        env_list.append(
            _mk_env(constants.LB_AUTH_TOKEN_ENV_VAR, lb_token, lb_token_secret))
    container.env = env_list
    pod.spec.containers = [container]
    core_api.read_namespaced_pod.return_value = pod

    monkeypatch.setattr(lb_k8s.kubernetes,
                        'apps_api',
                        lambda ctx=None: apps_api)
    monkeypatch.setattr(lb_k8s.kubernetes,
                        'core_api',
                        lambda ctx=None: core_api)
    monkeypatch.setattr(lb_k8s.kubernetes, 'api_exception',
                        lambda: _ApiException)
    return apps_api, core_api


# --------------------------------------------------------------------------- #
# Name helpers
# --------------------------------------------------------------------------- #
def test_name_helpers_rfc1123_simple():
    dep = lb_k8s.lb_deployment_name('my-service')
    svc = lb_k8s.lb_service_name('my-service')
    assert dep == svc  # Deployment and Service share the base name.
    assert _RFC1123.match(dep)
    assert len(dep) <= 63


@pytest.mark.parametrize('name', [
    'My_Service',
    'UPPER',
    'has spaces & symbols!!',
    'a' * 200,
    '____',
    'trailing-dash-',
])
def test_name_helpers_sanitize(name):
    dep = lb_k8s.lb_deployment_name(name)
    assert _RFC1123.match(dep), dep
    assert len(dep) <= 63
    # Deterministic.
    assert dep == lb_k8s.lb_deployment_name(name)


def test_long_name_gets_hash_suffix_and_is_unique():
    a = lb_k8s.lb_deployment_name('x' * 100 + '-alpha')
    b = lb_k8s.lb_deployment_name('x' * 100 + '-beta')
    assert a != b
    assert len(a) <= 63 and len(b) <= 63


def test_sanitize_collision_gets_distinct_names():
    # Distinct originals that sanitize to the same string ('svc-a') must NOT
    # collide: the hash of the ORIGINAL name keeps them apart.
    a = lb_k8s.lb_deployment_name('svc_a')
    b = lb_k8s.lb_deployment_name('svc-a')
    assert a != b
    # Deterministic + RFC1123 for both.
    assert a == lb_k8s.lb_deployment_name('svc_a')
    assert _RFC1123.match(a) and _RFC1123.match(b)
    assert len(a) <= 63 and len(b) <= 63


def test_lb_service_endpoint_format():
    ep = lb_k8s.lb_service_endpoint('my-service', 'skypilot')
    name = lb_k8s.lb_service_name('my-service')
    assert ep == (f'{name}.skypilot.svc.cluster.local'
                  f':{constants.LOAD_BALANCER_PORT_START}')
    assert '://' not in ep  # No scheme.


# --------------------------------------------------------------------------- #
# create_lb_deployment_and_service
# --------------------------------------------------------------------------- #
def test_create_builds_both_objects(monkeypatch):
    apps, core = _install(monkeypatch)

    lb_k8s.create_lb_deployment_and_service('svc-a', controller_port=20005)

    apps.create_namespaced_deployment.assert_called_once()
    core.create_namespaced_service.assert_called_once()

    ns_arg, dep = apps.create_namespaced_deployment.call_args.args
    assert ns_arg == 'skypilot'
    assert dep['metadata']['name'] == lb_k8s.lb_deployment_name('svc-a')
    container = dep['spec']['template']['spec']['containers'][0]
    # Controller address wires the shared controller Service + the port.
    args = container['args']
    controller_addr = args[args.index('--controller-addr') + 1]
    assert lb_k8s.CONTROLLER_SERVICE_NAME in controller_addr
    assert '20005' in controller_addr
    # Must carry the http:// scheme: the LB POSTs directly to this address and
    # the HTTP client rejects a schemeless URL (regression from live testing).
    assert controller_addr.startswith('http://')
    # LB listens on LOAD_BALANCER_PORT_START.
    lb_port = args[args.index('--load-balancer-port') + 1]
    assert lb_port == str(constants.LOAD_BALANCER_PORT_START)
    assert container['image'] == 'my-repo/skypilot:v1'

    ns_svc, svc = core.create_namespaced_service.call_args.args
    assert ns_svc == 'skypilot'
    assert svc['metadata']['name'] == lb_k8s.lb_service_name('svc-a')
    assert svc['spec']['ports'][0]['port'] == constants.LOAD_BALANCER_PORT_START


def test_create_auth_token_mirrors_secret_ref(monkeypatch):
    # Prod/ESO shape: the controller carries the token as a secretKeyRef, so the
    # LB must reference the SAME Secret -- never a plaintext value in its spec.
    apps, _ = _install(monkeypatch,
                       token='secret-token',
                       token_secret=('skypilot-serve-token', 'token'))
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    _, dep = apps.create_namespaced_deployment.call_args.args
    env = dep['spec']['template']['spec']['containers'][0].get('env', [])
    entry = next(
        e for e in env if e['name'] == constants.CONTROLLER_AUTH_TOKEN_ENV_VAR)
    assert 'value' not in entry  # No plaintext token in the manifest.
    ref = entry['valueFrom']['secretKeyRef']
    assert ref['name'] == 'skypilot-serve-token'
    assert ref['key'] == 'token'


def test_create_auth_token_inline_fallback(monkeypatch):
    # When the controller carries the token inline (non-ESO / test), the LB
    # mirrors the resolved value rather than a secretKeyRef.
    apps, _ = _install(monkeypatch, token='secret-token', token_secret=None)
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    _, dep = apps.create_namespaced_deployment.call_args.args
    env = dep['spec']['template']['spec']['containers'][0].get('env', [])
    names = {e['name']: e.get('value') for e in env}
    assert names.get(constants.CONTROLLER_AUTH_TOKEN_ENV_VAR) == 'secret-token'


def test_create_mirrors_both_tokens(monkeypatch):
    # Regression: the LB pod must receive BOTH the control-plane token (to reach
    # the controller) AND the inbound LB token (or its auth middleware is a
    # silent no-op in the real pod). Both mirrored as secretKeyRef.
    apps, _ = _install(monkeypatch,
                       token='ctrl-tok',
                       token_secret=('skypilot-serve-token', 'controller'),
                       lb_token='lb-tok',
                       lb_token_secret=('skypilot-serve-token', 'lb'))
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    _, dep = apps.create_namespaced_deployment.call_args.args
    env = dep['spec']['template']['spec']['containers'][0].get('env', [])
    by_name = {e['name']: e for e in env}
    assert set(by_name) == {
        constants.CONTROLLER_AUTH_TOKEN_ENV_VAR, constants.LB_AUTH_TOKEN_ENV_VAR
    }
    for entry in by_name.values():
        assert 'value' not in entry  # No plaintext token in the manifest.
        assert entry['valueFrom']['secretKeyRef']['name'] == \
            'skypilot-serve-token'
    assert by_name[constants.LB_AUTH_TOKEN_ENV_VAR]['valueFrom'][
        'secretKeyRef']['key'] == 'lb'


def test_create_injects_inbound_token_even_without_controller_token(
        monkeypatch):
    # Inbound data-plane auth must activate independently of control-plane auth.
    apps, _ = _install(monkeypatch, token=None, lb_token='lb-tok')
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    _, dep = apps.create_namespaced_deployment.call_args.args
    env = dep['spec']['template']['spec']['containers'][0].get('env', [])
    names = {e['name'] for e in env}
    assert names == {constants.LB_AUTH_TOKEN_ENV_VAR}


def test_create_no_auth_token_omits_env(monkeypatch):
    apps, _ = _install(monkeypatch, token=None)
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    _, dep = apps.create_namespaced_deployment.call_args.args
    assert 'env' not in dep['spec']['template']['spec']['containers'][0]


def test_create_swallows_409(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    core.create_namespaced_service.side_effect = _ApiException(409)
    _install(monkeypatch, apps_api=apps, core_api=core)

    # Must not raise.
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    apps.create_namespaced_deployment.assert_called_once()
    core.create_namespaced_service.assert_called_once()


def test_create_409_patches_deployment(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(409)
    _install(monkeypatch, apps_api=apps, core_api=core)

    # 409 on create -> patch the existing Deployment to the desired spec so
    # image/arg bumps roll out. Must not raise.
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    apps.patch_namespaced_deployment.assert_called_once()
    name_arg, ns_arg, body = apps.patch_namespaced_deployment.call_args.args
    assert name_arg == lb_k8s.lb_deployment_name('svc-a')
    assert ns_arg == 'skypilot'
    assert body['metadata']['name'] == lb_k8s.lb_deployment_name('svc-a')


def test_deployment_has_readiness_probe_and_rolling_update(monkeypatch):
    apps, _ = _install(monkeypatch)
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    _, dep = apps.create_namespaced_deployment.call_args.args
    container = dep['spec']['template']['spec']['containers'][0]
    # Readiness gated on the LB's drain-aware health route.
    probe = container['readinessProbe']
    assert probe['httpGet']['path'] == '/_lb/health'
    assert probe['httpGet']['port'] == constants.LOAD_BALANCER_PORT_START
    # Rolling update keeps the old pod until the new one is Ready (no gap).
    strategy = dep['spec']['strategy']
    assert strategy['type'] == 'RollingUpdate'
    assert strategy['rollingUpdate']['maxUnavailable'] == 0


def test_create_reraises_non_409(monkeypatch):
    apps = mock.MagicMock()
    apps.create_namespaced_deployment.side_effect = _ApiException(500)
    _install(monkeypatch, apps_api=apps)

    with pytest.raises(_ApiException):
        lb_k8s.create_lb_deployment_and_service('svc-a', 20005)


def test_create_requires_pod_name(monkeypatch):
    _install(monkeypatch, pod_name=None)
    with pytest.raises(RuntimeError):
        lb_k8s.create_lb_deployment_and_service('svc-a', 20005)


# --------------------------------------------------------------------------- #
# delete_lb_objects
# --------------------------------------------------------------------------- #
def test_delete_both_objects(monkeypatch):
    apps, core = _install(monkeypatch)
    lb_k8s.delete_lb_objects('svc-a')
    apps.delete_namespaced_deployment.assert_called_once()
    core.delete_namespaced_service.assert_called_once()
    dep_name, _ = apps.delete_namespaced_deployment.call_args.args
    assert dep_name == lb_k8s.lb_deployment_name('svc-a')


def test_delete_swallows_404(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.delete_namespaced_deployment.side_effect = _ApiException(404)
    core.delete_namespaced_service.side_effect = _ApiException(404)
    _install(monkeypatch, apps_api=apps, core_api=core)

    lb_k8s.delete_lb_objects('svc-a')  # Must not raise.
    apps.delete_namespaced_deployment.assert_called_once()
    core.delete_namespaced_service.assert_called_once()


def test_delete_reraises_non_404(monkeypatch):
    apps = mock.MagicMock()
    apps.delete_namespaced_deployment.side_effect = _ApiException(500)
    _install(monkeypatch, apps_api=apps)
    with pytest.raises(_ApiException):
        lb_k8s.delete_lb_objects('svc-a')


# --------------------------------------------------------------------------- #
# reconcile_lb_objects
# --------------------------------------------------------------------------- #
def _deployment_with_service_label(service_name):
    dep = mock.MagicMock()
    dep.metadata.labels = {lb_k8s.SERVE_LB_LABEL_KEY: service_name}
    return dep


def test_reconcile_deletes_only_orphans(monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    listed = mock.MagicMock()
    listed.items = [
        _deployment_with_service_label('A'),
        _deployment_with_service_label('B'),
    ]
    apps.list_namespaced_deployment.return_value = listed
    _install(monkeypatch, apps_api=apps, core_api=core)

    lb_k8s.reconcile_lb_objects({'A'})

    # B is orphaned -> deleted; A is live -> untouched.
    deleted = [
        c.args[0] for c in apps.delete_namespaced_deployment.call_args_list
    ]
    assert deleted == [lb_k8s.lb_deployment_name('B')]


def test_reconcile_no_orphans_deletes_nothing(monkeypatch):
    apps = mock.MagicMock()
    listed = mock.MagicMock()
    listed.items = [
        _deployment_with_service_label('A'),
        _deployment_with_service_label('B'),
    ]
    apps.list_namespaced_deployment.return_value = listed
    _install(monkeypatch, apps_api=apps)

    lb_k8s.reconcile_lb_objects({'A', 'B'})
    apps.delete_namespaced_deployment.assert_not_called()


def test_reconcile_rechecks_db_before_deleting(monkeypatch):
    apps = mock.MagicMock()
    listed = mock.MagicMock()
    listed.items = [
        _deployment_with_service_label('A'),
        _deployment_with_service_label('B'),
    ]
    apps.list_namespaced_deployment.return_value = listed
    # B is absent from the stale snapshot but STILL present in the DB (created
    # after the snapshot was taken) -> must NOT be reaped. A is in the snapshot.
    _install(monkeypatch, apps_api=apps, db_service_names={'B'})

    lb_k8s.reconcile_lb_objects({'A'})
    apps.delete_namespaced_deployment.assert_not_called()


def test_reconcile_deletes_when_db_confirms_gone(monkeypatch):
    apps = mock.MagicMock()
    listed = mock.MagicMock()
    listed.items = [_deployment_with_service_label('B')]
    apps.list_namespaced_deployment.return_value = listed
    # B absent from both the snapshot and the DB -> genuinely gone -> reaped.
    _install(monkeypatch, apps_api=apps, db_service_names=set())

    lb_k8s.reconcile_lb_objects({'A'})
    deleted = [
        c.args[0] for c in apps.delete_namespaced_deployment.call_args_list
    ]
    assert deleted == [lb_k8s.lb_deployment_name('B')]


# --------------------------------------------------------------------------- #
# Guards: no-op unless external-LB + in-cluster
# --------------------------------------------------------------------------- #
def test_not_external_mode_noop(monkeypatch):
    apps, core = _install(monkeypatch, external=False)
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    lb_k8s.delete_lb_objects('svc-a')
    lb_k8s.reconcile_lb_objects({'svc-a'})
    assert lb_k8s.lb_service_endpoint_or_none('svc-a') is None
    apps.create_namespaced_deployment.assert_not_called()
    apps.delete_namespaced_deployment.assert_not_called()
    apps.list_namespaced_deployment.assert_not_called()
    core.create_namespaced_service.assert_not_called()


def test_not_in_cluster_noop(monkeypatch):
    apps, core = _install(monkeypatch, incluster=False)
    lb_k8s.create_lb_deployment_and_service('svc-a', 20005)
    lb_k8s.delete_lb_objects('svc-a')
    lb_k8s.reconcile_lb_objects({'svc-a'})
    assert lb_k8s.lb_service_endpoint_or_none('svc-a') is None
    apps.create_namespaced_deployment.assert_not_called()
    apps.delete_namespaced_deployment.assert_not_called()
    apps.list_namespaced_deployment.assert_not_called()
    core.create_namespaced_service.assert_not_called()


def test_endpoint_or_none_active(monkeypatch):
    _install(monkeypatch)
    ep = lb_k8s.lb_service_endpoint_or_none('svc-a')
    assert ep == lb_k8s.lb_service_endpoint('svc-a', 'skypilot')


class TestLbImagePullPolicyMirror(unittest.TestCase):
    """The LB must mirror the controller pod's imagePullPolicy.

    The platform deploys a moving tag with Always; an LB hardcoding
    IfNotPresent pins whatever digest its node cached — controller and LB
    then run DIFFERENT code from the SAME tag (observed live: the LB
    lacked /_lb/capacity and proxied it to the model server).
    """

    def _pod(self, image='ecr/skypilot:tag', pull_policy='Always'):
        container = mock.Mock()
        container.image = image
        container.image_pull_policy = pull_policy
        pod = mock.Mock()
        pod.spec.containers = [container]
        return pod

    def test_mirrors_always_from_controller(self):
        with mock.patch.dict(lb_k8s.os.environ,
                             {constants.POD_NAME_ENV_VAR: 'ctrl-pod'}), \
             mock.patch.object(lb_k8s.kubernetes, 'core_api') as mock_api:
            mock_api.return_value.read_namespaced_pod.return_value = (self._pod(
                pull_policy='Always'))
            image, policy = lb_k8s._resolve_lb_image('ns', 'ctx')
        self.assertEqual(image, 'ecr/skypilot:tag')
        self.assertEqual(policy, 'Always')

    def test_defaults_when_controller_policy_unset(self):
        with mock.patch.dict(lb_k8s.os.environ,
                             {constants.POD_NAME_ENV_VAR: 'ctrl-pod'}), \
             mock.patch.object(lb_k8s.kubernetes, 'core_api') as mock_api:
            mock_api.return_value.read_namespaced_pod.return_value = (self._pod(
                pull_policy=None))
            _, policy = lb_k8s._resolve_lb_image('ns', 'ctx')
        self.assertEqual(policy, 'IfNotPresent')

    def test_deployment_dict_carries_policy(self):
        deployment = lb_k8s._build_deployment_dict('svc', 'dep', 'img', 'ns',
                                                   30001, [], 'Always')
        container = deployment['spec']['template']['spec']['containers'][0]
        self.assertEqual(container['imagePullPolicy'], 'Always')
