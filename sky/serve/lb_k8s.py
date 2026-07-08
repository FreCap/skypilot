"""Controller-owned external load balancer lifecycle (in-cluster k8s).

In external-load-balancer mode the SkyServe controller runs in-cluster (inside
the api-server pod) and OWNS a per-service Kubernetes Deployment + Service for
the load balancer. The LB pods reach the in-pod controller through one shared,
platform-provided Service (``CONTROLLER_SERVICE_NAME``) that exposes the
controller port range; the controller does NOT create that Service.

This module builds and reconciles those per-service objects:

- ``create_lb_deployment_and_service`` — called from up()/_start once the
  controller has bound its stable port, so the LB exists before up() reports
  the endpoint. Idempotent (409 == already exists is treated as success).
- ``delete_lb_objects`` — called on real teardown (down/TERMINATE).
- ``reconcile_lb_objects`` — called from HA recovery to reap orphaned LB
  objects whose service no longer exists.
- ``lb_service_endpoint`` — the W4 endpoint: the LB Service's in-cluster DNS
  ``host:port`` (no scheme; the caller adds http/https).

Every public function is a no-op unless external-load-balancer mode is enabled
AND we are running with in-cluster config -- mirroring lb_rbac_preflight's
guard.
"""
import hashlib
import os
import re
from typing import Optional, Set, Tuple

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils

logger = sky_logging.init_logger(__name__)

# Shared, platform-provided Service that exposes the controller port range so
# LB pods can reach the in-pod controller. The controller does NOT create this.
CONTROLLER_SERVICE_NAME = 'skypilot-serve-controller'

# Labels stamped on every LB object the controller owns.
#   parent=skypilot                 -> ownership marker (shared convention).
#   skypilot-serve-lb=<service>     -> distinguishing label; reconcile lists by
#                                      this key and maps back to the service.
PARENT_LABEL_KEY = 'parent'
PARENT_LABEL_VALUE = 'skypilot'
SERVE_LB_LABEL_KEY = 'skypilot-serve-lb'
# Pod selector label: app=<lb_deployment_name>.
APP_LABEL_KEY = 'app'
# Label-key selector used by reconcile to list all LB Deployments.
LB_SELECTOR_LABEL = SERVE_LB_LABEL_KEY

# RFC1123 name constraints for k8s object names.
_MAX_NAME_LEN = 63
_LB_NAME_PREFIX = 'skypilot-lb-'
_HASH_LEN = 8

# Readiness route served by the load balancer (see sky/serve/load_balancer.py).
# It returns 503 while the LB is draining (SIGTERM / rolling update), so a
# readinessProbe on it pulls a draining pod out of the Service endpoints before
# the pod terminates -- no traffic to a pod that is going away.
_LB_HEALTH_PATH = constants.LB_HEALTH_ENDPOINT_PATH


def _sanitize(service_name: str) -> str:
    """Lowercase and collapse non-[a-z0-9-] runs into single dashes."""
    return re.sub(r'[^a-z0-9-]+', '-', service_name.lower()).strip('-')


def lb_base_name(service_name: str) -> str:
    """Deterministic RFC1123-compliant base name for the LB objects.

    Lowercase, only ``[a-z0-9-]``, starts/ends alphanumeric, <=63 chars. A short
    stable hash of the ORIGINAL service name is ALWAYS appended: sanitizing is
    lossy (e.g. ``svc_a`` and ``svc-a`` both sanitize to ``svc-a``), so without
    the hash two distinct services could collide on the same object name and one
    would receive the other's LB traffic. The Deployment and its Service share
    this base name.
    """
    sanitized = _sanitize(service_name)
    digest = hashlib.sha1(service_name.encode()).hexdigest()[:_HASH_LEN]
    # Reserve room for the '-<digest>' suffix within the 63-char budget.
    budget = _MAX_NAME_LEN - len(_LB_NAME_PREFIX) - 1 - len(digest)
    truncated = sanitized[:budget].strip('-')
    if not truncated:
        # Sanitized to empty: the hash alone keeps the name valid and unique.
        return f'{_LB_NAME_PREFIX}{digest}'
    return f'{_LB_NAME_PREFIX}{truncated}-{digest}'


def lb_deployment_name(service_name: str) -> str:
    """RFC1123 name of the LB Deployment for ``service_name``."""
    return lb_base_name(service_name)


def lb_service_name(service_name: str) -> str:
    """RFC1123 name of the LB Service for ``service_name``."""
    return lb_base_name(service_name)


def lb_service_endpoint(service_name: str, namespace: str) -> str:
    """In-cluster DNS ``host:port`` of the LB Service (no scheme)."""
    return (f'{lb_service_name(service_name)}.{namespace}.svc.cluster.local'
            f':{constants.LOAD_BALANCER_PORT_START}')


def _controller_addr(namespace: str, controller_port: int) -> str:
    """In-cluster URL the LB uses to reach the controller.

    Must include the ``http://`` scheme -- the load balancer POSTs directly to
    ``{controller_addr}/controller/load_balancer_sync`` and the HTTP client
    rejects a schemeless URL (matches the in-pod path's
    ``f'http://{controller_host}:{controller_port}'`` in service.py).
    """
    return (f'http://{CONTROLLER_SERVICE_NAME}.{namespace}.svc.cluster.local'
            f':{controller_port}')


def _lb_mode_active() -> bool:
    """Whether controller-owned LB lifecycle applies in this process."""
    return (serve_utils.is_external_load_balancer_mode() and
            kubernetes_utils.is_incluster_config_available())


def lb_service_endpoint_or_none(service_name: str) -> Optional[str]:
    """The LB Service endpoint (host:port, no scheme), or None if inactive.

    Returns None when external-LB mode is off or we lack in-cluster config, so
    callers fall back to the in-pod / controller endpoint.
    """
    if not _lb_mode_active():
        return None
    context = kubernetes.in_cluster_context_name()
    namespace = kubernetes_utils.get_kube_config_context_namespace(context)
    return lb_service_endpoint(service_name, namespace)


def _object_labels(service_name: str) -> dict:
    return {
        PARENT_LABEL_KEY: PARENT_LABEL_VALUE,
        SERVE_LB_LABEL_KEY: service_name,
    }


def _resolve_lb_image(namespace: str, context: str) -> Tuple[str, str]:
    """Mirror the controller's container image AND pull policy onto the LB.

    Reads the controller pod (name from ``POD_NAME_ENV_VAR``) and returns its
    first container's (image, imagePullPolicy). The pull policy MUST be
    mirrored together with the image: the platform deploys a moving tag
    (``-improvements``) with ``Always``, and an LB Deployment hardcoding
    ``IfNotPresent`` silently pins whatever digest its node had cached — the
    controller and its LB then run DIFFERENT code from the SAME tag (observed
    live: an LB missing the /_lb/capacity route the controller image carried,
    so the request fell through to the catch-all and was proxied to the model
    server). A digest-pinned deployment keeps its own policy unchanged.
    Raises if the env var is unset -- that injection is part of the platform
    contract.
    """
    pod_name = os.environ.get(constants.POD_NAME_ENV_VAR)
    if not pod_name:
        raise RuntimeError(
            'Cannot resolve the load balancer image: environment variable '
            f'{constants.POD_NAME_ENV_VAR!r} is not set. The platform must '
            'inject the controller pod name (downward API metadata.name) in '
            'external load balancer mode.')
    pod = kubernetes.core_api(context).read_namespaced_pod(pod_name, namespace)
    container = pod.spec.containers[0]
    return container.image, (container.image_pull_policy or 'IfNotPresent')


def _mirror_pod_env(pod_containers, env_name: str, token_value: str) -> dict:
    """Manifest env entry for ``env_name``, mirrored from the controller pod.

    If the controller pod carries ``env_name`` as a ``secretKeyRef`` (the
    platform/ESO shape), reference the SAME Secret so the token never lands as
    plaintext in the LB Deployment spec. Falls back to an inline value only when
    the env is not sourced from a Secret on the pod (non-k8s / test / inline
    helm value -- already plaintext in the controller's own spec, so no new
    exposure).
    """
    for container in (pod_containers or []):
        for env in (container.env or []):
            if env.name != env_name:
                continue
            secret_key_ref = getattr(getattr(env, 'value_from', None),
                                     'secret_key_ref', None)
            if secret_key_ref is not None:
                ref = {'name': secret_key_ref.name, 'key': secret_key_ref.key}
                if secret_key_ref.optional is not None:
                    ref['optional'] = secret_key_ref.optional
                return {'name': env_name, 'valueFrom': {'secretKeyRef': ref}}
            # Present but inline (or an unsupported source): mirror the resolved
            # value below rather than the raw env spec.
            break
    return {'name': env_name, 'value': token_value}


def _resolve_lb_auth_envs(namespace: str, context: str) -> list:
    """LB container env entries for BOTH shared bearer tokens (manifest dicts).

    - ``CONTROLLER_AUTH_TOKEN_ENV_VAR``: the LB presents it to the controller on
      every sync (control-plane auth).
    - ``LB_AUTH_TOKEN_ENV_VAR``: the LB validates INBOUND inference requests
      against it (data-plane auth). Without this the spawned LB pod would never
      see the inbound token and its auth middleware would be a silent no-op.

    Each is included only when set on this (controller) process, and is mirrored
    from the controller pod's own env source (a ``secretKeyRef``) so tokens are
    never written as plaintext into the LB Deployment spec -- symmetric with
    :func:`_resolve_lb_image`. Returns ``[]`` when neither token is set.
    """
    wanted = [
        (constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
         serve_utils.get_controller_auth_token()),
        (constants.LB_AUTH_TOKEN_ENV_VAR, serve_utils.get_lb_auth_token()),
    ]
    active = [(name, tok) for name, tok in wanted if tok is not None]
    if not active:
        return []
    pod_containers = None
    pod_name = os.environ.get(constants.POD_NAME_ENV_VAR)
    if pod_name:
        pod = kubernetes.core_api(context).read_namespaced_pod(
            pod_name, namespace)
        pod_containers = pod.spec.containers
    return [_mirror_pod_env(pod_containers, name, tok) for name, tok in active]


def _build_deployment_dict(service_name: str, deployment_name: str, image: str,
                           namespace: str, controller_port: int,
                           auth_envs: list, image_pull_policy: str) -> dict:
    container = {
        'name': 'load-balancer',
        'image': image,
        'imagePullPolicy': image_pull_policy,
        'command': ['python', '-m', 'sky.serve.load_balancer'],
        'args': [
            '--controller-addr',
            _controller_addr(namespace, controller_port),
            '--load-balancer-port',
            str(constants.LOAD_BALANCER_PORT_START),
        ],
        'ports': [{
            'containerPort': constants.LOAD_BALANCER_PORT_START
        }],
        # Gate the Service endpoints on the LB's drain-aware health route: on
        # SIGTERM / rolling update the route flips to 503, so k8s removes the
        # draining pod from the endpoints before it exits.
        'readinessProbe': {
            'httpGet': {
                'path': _LB_HEALTH_PATH,
                'port': constants.LOAD_BALANCER_PORT_START,
            },
            'periodSeconds': 2,
            'failureThreshold': 1,
        },
    }
    # The bearer tokens (control-plane + inbound LB) are mirrored from the
    # controller pod's own env sources (secretKeyRef into the platform Secret)
    # by _resolve_lb_auth_envs, so they never land as plaintext in this spec.
    if auth_envs:
        container['env'] = auth_envs
    # TODO(fcapponi): no TLS handling in this pass -- TLS terminates at the
    # ingress/ALB (see the LB ingress+auth design), not at the LB pod.
    pod_labels = {APP_LABEL_KEY: deployment_name}
    pod_labels.update(_object_labels(service_name))
    return {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {
            'name': deployment_name,
            'labels': _object_labels(service_name),
        },
        'spec': {
            'replicas': 1,
            # Roll the new LB pod up (Ready via the readinessProbe) before the
            # old one is torn down, so an image/arg bump never leaves a gap with
            # no LB pod backing the Service.
            'strategy': {
                'type': 'RollingUpdate',
                'rollingUpdate': {
                    'maxSurge': 1,
                    'maxUnavailable': 0,
                },
            },
            'selector': {
                'matchLabels': {
                    APP_LABEL_KEY: deployment_name
                }
            },
            'template': {
                'metadata': {
                    'labels': pod_labels
                },
                'spec': {
                    'containers': [container]
                },
            },
        },
    }


def _build_service_dict(service_name: str, service_name_k8s: str,
                        deployment_name: str) -> dict:
    return {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {
            'name': service_name_k8s,
            'labels': _object_labels(service_name),
        },
        'spec': {
            'type': 'ClusterIP',
            'selector': {
                APP_LABEL_KEY: deployment_name
            },
            'ports': [{
                'port': constants.LOAD_BALANCER_PORT_START,
                'targetPort': constants.LOAD_BALANCER_PORT_START,
                'protocol': 'TCP',
            }],
        },
    }


def create_lb_deployment_and_service(service_name: str,
                                     controller_port: int) -> None:
    """Create the per-service LB Deployment + Service (idempotent).

    No-op outside external-LB + in-cluster mode. A 409 (already exists) from
    either create is treated as success so the call is safe on recovery.
    """
    if not _lb_mode_active():
        return
    context = kubernetes.in_cluster_context_name()
    namespace = kubernetes_utils.get_kube_config_context_namespace(context)
    deployment_name = lb_deployment_name(service_name)
    service_name_k8s = lb_service_name(service_name)
    image, image_pull_policy = _resolve_lb_image(namespace, context)
    auth_envs = _resolve_lb_auth_envs(namespace, context)

    deployment_dict = _build_deployment_dict(service_name, deployment_name,
                                             image, namespace, controller_port,
                                             auth_envs, image_pull_policy)
    service_dict = _build_service_dict(service_name, service_name_k8s,
                                       deployment_name)

    try:
        kubernetes.apps_api(context).create_namespaced_deployment(
            namespace, deployment_dict)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 409:
            raise
        # Already exists: patch it to the desired spec so image/arg bumps (e.g.
        # a service update or a controller image roll) actually roll out. The
        # RollingUpdate strategy keeps the old LB pod serving until the new one
        # is Ready.
        logger.debug(f'LB Deployment {deployment_name} already exists; '
                     'patching it to the desired spec.')
        kubernetes.apps_api(context).patch_namespaced_deployment(
            deployment_name, namespace, deployment_dict)
    try:
        kubernetes.core_api(context).create_namespaced_service(
            namespace, service_dict)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 409:
            raise
        # Leave an existing Service as-is: its spec (selector + ports) is stable
        # across respawns, and patching it risks disturbing the allocated
        # clusterIP for no benefit. Only the Deployment carries mutable spec.
        logger.debug(f'LB Service {service_name_k8s} already exists.')


def delete_lb_objects(service_name: str) -> None:
    """Delete the per-service LB Deployment + Service (idempotent).

    No-op outside external-LB + in-cluster mode. A 404 (already gone) from
    either delete is ignored.
    """
    if not _lb_mode_active():
        return
    context = kubernetes.in_cluster_context_name()
    namespace = kubernetes_utils.get_kube_config_context_namespace(context)
    deployment_name = lb_deployment_name(service_name)
    service_name_k8s = lb_service_name(service_name)

    try:
        kubernetes.apps_api(context).delete_namespaced_deployment(
            deployment_name, namespace)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 404:
            raise
        logger.debug(f'LB Deployment {deployment_name} already deleted.')
    try:
        kubernetes.core_api(context).delete_namespaced_service(
            service_name_k8s, namespace)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 404:
            raise
        logger.debug(f'LB Service {service_name_k8s} already deleted.')


def reconcile_lb_objects(live_service_names: Set[str]) -> None:
    """Reap LB objects whose owning service is no longer live.

    No-op outside external-LB + in-cluster mode. Lists LB Deployments by the
    distinguishing label, maps each back to its service via the label value,
    and deletes any whose service is not in ``live_service_names``. Only
    deletes orphans -- create-if-missing for live services is handled by the
    up()/recovery path.

    ``live_service_names`` is a stale snapshot (taken before the recovery
    sweep), so a service created after the snapshot would look absent here. To
    avoid deleting a live service's LB, re-check the DB at delete time and only
    reap an LB whose owning service is genuinely gone.
    """
    if not _lb_mode_active():
        return
    context = kubernetes.in_cluster_context_name()
    namespace = kubernetes_utils.get_kube_config_context_namespace(context)

    deployments = kubernetes.apps_api(context).list_namespaced_deployment(
        namespace, label_selector=LB_SELECTOR_LABEL)
    for deployment in deployments.items:
        labels = deployment.metadata.labels or {}
        owning_service = labels.get(SERVE_LB_LABEL_KEY)
        if owning_service is None:
            continue
        if owning_service in live_service_names:
            continue
        # Not in the stale snapshot -- confirm the service is truly gone at
        # delete time before reaping its LB (the snapshot predates any service
        # created during recovery).
        if serve_state.get_service_from_name(owning_service) is not None:
            continue
        delete_lb_objects(owning_service)
