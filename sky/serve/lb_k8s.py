"""Controller-owned external load balancer lifecycle (in-cluster k8s).

The SkyServe controller runs in-cluster (inside an API-server pod) and owns a
per-service Kubernetes Deployment + Service for the load balancer. Each LB
syncs through a route implemented by every API-server pod. That stable proxy
reads the authoritative controller owner tuple from the database and forwards
once, so controller failover never changes or rolls the LB Deployment.

This module builds and reconciles those per-service objects:

- ``create_lb_deployment_and_service`` — called from up()/_start once the
  controller has published its owner tuple, so the LB exists before up() reports
  the endpoint. Idempotent (409 == already exists is treated as success).
- ``delete_lb_objects`` — called on real teardown (down/TERMINATE).
- ``reconcile_lb_objects`` — called from HA recovery to reap orphaned LB
  objects whose service no longer exists.
- ``lb_service_endpoint`` — the W4 endpoint: the LB Service's in-cluster DNS
  ``host:port`` (no scheme; the caller adds http/https).

Lifecycle/reaper helpers remain no-ops when the platform feature is disabled;
starting a real service calls :func:`require_external_lb_runtime` and fails
closed instead of falling back to an in-pod LB.
"""
import copy
import hashlib
import json
import math
import os
import re
import sys
import time
from typing import Any, Dict, NamedTuple, Optional, Set, Tuple
import urllib.parse

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils

logger = sky_logging.init_logger(__name__)

# Pod-template annotation carrying the controller's resolved image digest —
# the LB Deployment's rollout trigger (see _resolve_lb_image).
CONTROLLER_DIGEST_ANNOTATION = 'skypilot.co/controller-image-digest'

# Labels stamped on every LB object the controller owns.
#   parent=skypilot                 -> ownership marker (shared convention).
#   skypilot-serve-lb=<service>     -> distinguishing label; reconcile lists by
#                                      this key and maps back to the service.
PARENT_LABEL_KEY = 'parent'
PARENT_LABEL_VALUE = 'skypilot'
SERVE_LB_LABEL_KEY = 'skypilot-serve-lb'
SERVICE_HASH_LABEL_KEY = 'skypilot-serve-incarnation'
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
_LB_TERMINATION_MARGIN_SECONDS = 30

# Stable projected-volume names rendered by the Helm chart. The LB receives
# only the sync and data-plane rings; the controller-admin ring must never be
# copied into this lower-trust pod.
LB_SYNC_AUTH_VOLUME_NAME = 'skypilot-serve-lb-sync-auth'
LB_DATA_PLANE_AUTH_VOLUME_NAME = 'skypilot-serve-lb-auth'
_LB_DATA_PLANE_AUTH_MOUNT_PATH = ('/etc/skypilot/serve-auth/lb-data-plane')

_SHA256_DIGEST_RE = re.compile(r'^sha256:[0-9a-fA-F]{64}$')
_RUNTIME_IMAGE_ID_PREFIXES = ('docker-pullable://', 'containerd://',
                              'docker://')
_SERVICE_ACCOUNT_NAMESPACE_PATH = (
    '/var/run/secrets/kubernetes.io/serviceaccount/namespace')
_API_CONTAINER_NAME = 'skypilot-api'
_DEFAULT_LB_RESOURCES = {
    'requests': {
        'cpu': '100m',
        'memory': '128Mi',
    },
    'limits': {
        'memory': '512Mi',
    },
}


class LbPodAuthority(NamedTuple):
    """Kubernetes-owned identities used to validate one LB report.

    ``ready_nonterminating_uids`` identifies the only Pod(s) that can receive
    new Service traffic. ``live_uids`` is deliberately broader and retains
    terminating Pods because they may still own long-running streams after
    readiness has been withdrawn.
    """

    ready_nonterminating_uids: Set[str]
    live_uids: Set[str]


def lb_termination_grace_period_seconds(
        stream_timeout_seconds: float,
        graceful_drain_seconds: Optional[float]) -> int:
    """Kubelet SIGKILL budget for an external LB pod.

    The pod first needs time to leave Service endpoints, then Uvicorn must be
    allowed to finish active ASGI responses. The stream timeout is the best
    service-level bound available today; a longer replica-drain policy also
    extends the LB budget so a coordinated rollout cannot kill the routing
    layer before the replicas it is draining.
    """

    def _validate(value: float, field_name: str) -> float:
        if (isinstance(value, bool) or not isinstance(value, (int, float)) or
                not math.isfinite(value) or value < 0):
            raise ValueError(f'{field_name} must be a finite, nonnegative '
                             f'number; got {value!r}.')
        return float(value)

    stream_timeout = _validate(stream_timeout_seconds, 'stream_timeout_seconds')
    graceful_drain = (0.0 if graceful_drain_seconds is None else _validate(
        graceful_drain_seconds, 'graceful_drain_seconds'))
    request_budget = max(stream_timeout, graceful_drain)
    # Kubernetes requires this field to be an integer. Round up so accepting a
    # fractional stream timeout never shortens the caller's requested budget.
    return math.ceil(request_budget + constants.LB_DRAIN_GRACE_SECONDS +
                     _LB_TERMINATION_MARGIN_SECONDS)


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
    return (f'{lb_service_name(service_name)}.{namespace}.svc'
            f':{constants.LOAD_BALANCER_PORT_START}')


def _controller_addr(service_name: str) -> str:
    """Stable API-service proxy base URL used by the external LB.

    ``load_balancer.py`` appends ``/controller/load_balancer_sync``. Encoding
    the service name here keeps it a single path segment even if a legacy name
    contains punctuation.
    """
    api_service_url = os.environ.get('SKYPILOT_SERVE_API_SERVICE_URL')
    if not api_service_url:
        raise RuntimeError(
            'External load balancer mode requires '
            'SKYPILOT_SERVE_API_SERVICE_URL. Install/upgrade the SkyPilot '
            'Helm chart with serve.externalLoadBalancer.enabled=true.')
    encoded_name = urllib.parse.quote(service_name, safe='')
    return (f'{api_service_url.rstrip("/")}/api/internal/serve/'
            f'{encoded_name}')


def _lb_mode_active() -> bool:
    """Whether controller-owned LB lifecycle applies in this process."""
    return (serve_utils.is_external_load_balancer_mode() and
            kubernetes_utils.is_incluster_config_available())


def get_lb_namespace() -> str:
    """Namespace that owns the API pod and controller-created LB objects."""
    namespace = os.environ.get(constants.POD_NAMESPACE_ENV_VAR)
    if not namespace:
        raise RuntimeError(
            'External load balancer mode requires '
            f'{constants.POD_NAMESPACE_ENV_VAR}. Install/upgrade the SkyPilot '
            'Helm chart so the API pod namespace is injected from the '
            'downward API (metadata.namespace).')
    return namespace


def _cleanup_lb_namespace() -> Optional[str]:
    """Resolve the owner namespace even after external LB is disabled.

    Helm may remove feature-specific configuration before the recovery sweep
    deletes old LB objects. The pod namespace env remains the primary source;
    the Kubernetes service-account namespace file is the only safe fallback.
    In particular, do not use the configured workload namespace here.
    """
    namespace = os.environ.get(constants.POD_NAMESPACE_ENV_VAR)
    if namespace:
        return namespace
    try:
        with open(_SERVICE_ACCOUNT_NAMESPACE_PATH, encoding='utf-8') as f:
            namespace = f.read().strip()
    except OSError as e:
        logger.error(
            'Cannot clean up external load balancer objects: %s is '
            'unavailable: %s', _SERVICE_ACCOUNT_NAMESPACE_PATH, e)
        return None
    if not namespace:
        logger.error(
            'Cannot clean up external load balancer objects: %s is '
            'empty.', _SERVICE_ACCOUNT_NAMESPACE_PATH)
        return None
    return namespace


def require_external_lb_runtime() -> None:
    """Fail unless the external-only SkyServe platform contract is ready."""
    if not serve_utils.is_external_load_balancer_mode():
        raise RuntimeError(
            'SkyServe services require the external load balancer. Enable '
            'serve.controller.external_load_balancer in the API-server '
            'configuration; the in-pod load balancer is no longer supported.')
    if not kubernetes_utils.is_incluster_config_available():
        raise RuntimeError(
            'SkyServe services require an in-cluster Kubernetes API server '
            'deployment; the in-pod load balancer is no longer supported.')
    # Validate the stable rendezvous address before any service row is created.
    _controller_addr('runtime-check')
    if not os.environ.get(constants.POD_NAME_ENV_VAR):
        raise RuntimeError(
            f'External load balancer mode requires '
            f'{constants.POD_NAME_ENV_VAR}. Install/upgrade the SkyPilot Helm '
            'chart with serve.externalLoadBalancer.enabled=true.')
    get_lb_namespace()
    # File contents are read afresh on every request; this boot-time check only
    # prevents publishing a service that cannot authenticate its first sync or
    # (when enabled) inference request.
    serve_utils.get_lb_sync_auth_tokens(required=True)
    serve_utils.get_controller_admin_auth_tokens(required=True)
    data_plane_auth_enabled = serve_utils.is_lb_data_plane_auth_enabled()
    required_file_env_names = [
        constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
        constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
    ]
    if data_plane_auth_enabled:
        serve_utils.get_lb_auth_tokens(required=True)
        required_file_env_names.append(constants.LB_AUTH_TOKENS_FILE_ENV_VAR)
    for env_name in required_file_env_names:
        if not os.environ.get(env_name):
            raise RuntimeError(
                'External load balancer mode requires projected Secret '
                f'files; {env_name} is not set. Legacy inline token env vars '
                'are accepted only for compatibility outside this topology.')


def lb_service_endpoint_or_none(service_name: str) -> Optional[str]:
    """The LB Service endpoint (host:port, no scheme), or None if inactive.

    Returns None outside the installed external-LB platform. Real service
    startup fails closed through :func:`require_external_lb_runtime`; this
    optional form remains useful to status code inspecting old/pool rows.
    """
    if not _lb_mode_active():
        return None
    return lb_service_endpoint(service_name, get_lb_namespace())


def _object_labels(service_name: str,
                   service_hash: Optional[str] = None) -> dict:
    labels = {
        PARENT_LABEL_KEY: PARENT_LABEL_VALUE,
        SERVE_LB_LABEL_KEY: service_name,
    }
    if service_hash:
        labels[SERVICE_HASH_LABEL_KEY] = service_hash
    return labels


def _read_controller_pod(namespace: str, context: str):
    pod_name = os.environ.get(constants.POD_NAME_ENV_VAR)
    if not pod_name:
        raise RuntimeError(
            'Cannot inspect the API pod for the external load balancer: '
            f'environment variable {constants.POD_NAME_ENV_VAR!r} is not set.')
    return kubernetes.core_api(context).read_namespaced_pod(pod_name, namespace)


def _resolve_lb_image(namespace: str,
                      context: str,
                      pod=None) -> Tuple[str, str, Optional[str]]:
    """Resolve the controller image used for the external LB.

    Reads the controller pod (name from ``POD_NAME_ENV_VAR``) and returns its
    first container's resolved image and imagePullPolicy. When Kubernetes has
    populated the container's runtime ``imageID``, common runtime forms are
    normalized to an immutable ``repository@sha256:...`` reference and that
    reference is used as the LB container image itself. The pull policy MUST
    be mirrored together with the image: the platform deploys a moving tag
    (``-improvements``) with ``Always``, and an LB Deployment hardcoding
    ``IfNotPresent`` silently pins whatever digest its node had cached — the
    controller and its LB then run DIFFERENT code from the SAME tag (observed
    live: an LB missing the /_lb/capacity route the controller image carried,
    so the request fell through to the catch-all and was proxied to the model
    server). A digest-pinned deployment keeps its own policy unchanged.
    Raises if the pod-name env var is unavailable; it is part of the platform
    contract used to mirror both image identity and projected auth volumes.
    """
    if pod is None:
        pod = _read_controller_pod(namespace, context)
    container = _controller_container(pod)
    # The RESOLVED digest of the running controller image (imageID from the
    # container status; None while the status is not yet populated). Pinning
    # the actual image field is critical: an annotation-only rollout still
    # lets a moving tag resolve to a different digest on the LB node.
    declared_digest_reference = _resolved_image_reference(
        container.image, container.image)
    digest_reference = declared_digest_reference
    status = _controller_container_status(pod, container)
    if status is not None and getattr(status, 'image_id', None):
        image_id = status.image_id
        digest_reference = _resolved_image_reference(container.image, image_id)
        if digest_reference is None:
            raise RuntimeError(
                f'Cannot pin the external load balancer image: controller '
                f'imageID {image_id!r} is not a valid sha256 reference.')
    if digest_reference is None:
        raise RuntimeError(
            'Cannot pin the external load balancer image: Kubernetes has not '
            f'published a sha256 imageID for mutable image '
            f'{container.image!r}. '
            'Retry after the API container status is ready or deploy the API '
            'container with an immutable digest reference.')
    return (digest_reference, (container.image_pull_policy or
                               'IfNotPresent'), digest_reference)


def _resolved_image_reference(image: str, image_id: str) -> Optional[str]:
    """Return an immutable image reference from a runtime ``imageID``.

    Kubernetes runtimes commonly report either a full pullable reference
    (``docker-pullable://repo@sha256:...``) or only the content digest
    (``containerd://sha256:...``). In the latter case, retain the repository
    from the declared container image while dropping its mutable tag. Unknown
    schemes, malformed repositories, and non-SHA256/short digests fail closed
    to ``None`` so callers can safely retain the declared image.
    """
    if not isinstance(image_id, str):
        return None
    candidate = image_id.strip()
    if not candidate or any(char.isspace() for char in candidate):
        return None

    for prefix in _RUNTIME_IMAGE_ID_PREFIXES:
        if candidate.startswith(prefix):
            candidate = candidate[len(prefix):]
            break
    else:
        # Never pass an unknown runtime scheme through to a Pod image field.
        if '://' in candidate:
            return None

    if '@' in candidate:
        repository, digest = candidate.rsplit('@', 1)
        repository = _valid_image_repository(repository)
    else:
        repository = _image_repository(image)
        digest = candidate

    if repository is None or _SHA256_DIGEST_RE.fullmatch(digest) is None:
        return None
    return f'{repository}@{digest.lower()}'


def _image_repository(image: str) -> Optional[str]:
    """Extract a repository from a declared image without confusing ports."""
    if not isinstance(image, str):
        return None
    reference = image.strip()
    if (not reference or '://' in reference or
            any(char.isspace() for char in reference)):
        return None
    reference = reference.split('@', 1)[0]
    last_slash = reference.rfind('/')
    last_colon = reference.rfind(':')
    if last_colon > last_slash:
        reference = reference[:last_colon]
    return _valid_image_repository(reference)


def _valid_image_repository(repository: str) -> Optional[str]:
    """Validate the minimal repository grammar needed for safe composition."""
    if (not repository or repository.startswith('/') or
            repository.endswith('/') or '://' in repository or
            '@' in repository or any(char.isspace() for char in repository)):
        return None
    return repository


def _serialize_k8s_object(obj):
    """Convert a Kubernetes model (or already-built dict) to a manifest."""
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_serialize_k8s_object(item) for item in obj]
    return kubernetes.kubernetes.client.ApiClient().sanitize_for_serialization(
        obj)


def _find_named(items, name: str):
    for item in items or []:
        item_name = item.get('name') if isinstance(item, dict) else getattr(
            item, 'name', None)
        if item_name == name:
            return item
    return None


def _controller_container(pod):
    """Select the API container, never an admission-injected sidecar."""
    containers = list(getattr(pod.spec, 'containers', None) or [])
    named = [
        container for container in containers
        if getattr(container, 'name', None) == _API_CONTAINER_NAME
    ]
    if named:
        return named[0]
    if len(containers) == 1:
        return containers[0]
    raise RuntimeError(f'Controller Pod must contain a uniquely identifiable '
                       f'{_API_CONTAINER_NAME!r} container.')


def _controller_container_status(pod, container):
    statuses = list(getattr(pod.status, 'container_statuses', None) or [])
    if not statuses:
        return None
    container_name = getattr(container, 'name', None)
    named = [
        status for status in statuses
        if container_name and getattr(status, 'name', None) == container_name
    ]
    if named:
        return named[0]
    if len(statuses) == 1:
        return statuses[0]
    raise RuntimeError(f'Controller Pod status does not identify the '
                       f'{_API_CONTAINER_NAME!r} container.')


def _portable_lb_runtime_contract(pod_spec,
                                  controller_container) -> Tuple[dict, dict]:
    """Return the whitelisted Pod/container runtime fields for the LB.

    The spawned LB must remain schedulable wherever the owning API Pod runs and
    must retain its non-root/projected-volume access contract. Copy only
    portable scheduling and security-identity fields. In particular, do not
    inherit host networking/namespace identity, DNS/hostname identity, service
    account settings, affinity, priority/resources, privileged/added-capability
    settings, or arbitrary volumes from the higher-trust API Pod.
    """
    pod_fields = {}
    pod_security = getattr(pod_spec, 'security_context', None)
    if pod_security is not None:
        serialized = _serialize_k8s_object(pod_security)
        allowed = ('runAsUser', 'runAsGroup', 'fsGroup', 'fsGroupChangePolicy',
                   'supplementalGroups', 'seccompProfile')
        pod_sanitized = {
            key: serialized[key] for key in allowed if key in serialized
        }
        if serialized.get('runAsNonRoot') is True:
            pod_sanitized['runAsNonRoot'] = True
        if pod_sanitized:
            pod_fields['securityContext'] = pod_sanitized
    for attribute, manifest_key in (('node_selector', 'nodeSelector'),
                                    ('tolerations', 'tolerations')):
        value = getattr(pod_spec, attribute, None)
        if value is not None:
            serialized = _serialize_k8s_object(value)
            if serialized:
                pod_fields[manifest_key] = serialized
    for attribute, manifest_key in (
        ('runtime_class_name', 'runtimeClassName'),
        ('scheduler_name', 'schedulerName'),
    ):
        value = getattr(pod_spec, attribute, None)
        if value:
            pod_fields[manifest_key] = value

    container_fields = {}
    container_security = getattr(controller_container, 'security_context', None)
    if container_security is not None:
        serialized = _serialize_k8s_object(container_security)
        container_sanitized: Dict[str, Any] = {
            key: serialized[key]
            for key in ('runAsUser', 'runAsGroup')
            if key in serialized
        }
        for key in ('runAsNonRoot', 'readOnlyRootFilesystem'):
            if serialized.get(key) is True:
                container_sanitized[key] = True
        if serialized.get('allowPrivilegeEscalation') is False:
            container_sanitized['allowPrivilegeEscalation'] = False
        seccomp_profile = serialized.get('seccompProfile')
        if isinstance(seccomp_profile, dict):
            container_sanitized['seccompProfile'] = seccomp_profile
        capabilities = serialized.get('capabilities')
        if isinstance(capabilities, dict):
            dropped = capabilities.get('drop')
            if isinstance(dropped, list) and dropped:
                # Never inherit added capabilities from the API container.
                container_sanitized['capabilities'] = {'drop': dropped}
        if container_sanitized:
            container_fields['securityContext'] = container_sanitized
    return pod_fields, container_fields


def _resolve_lb_auth_projection(
        namespace: str,
        context: str,
        pod=None) -> Tuple[list, list, list, list, dict, dict, bool]:
    """Return LB auth, image-pull, and portable API-Pod runtime fields.

    The controller-admin projection is deliberately not copied. Projected
    Secret files update in place and the LB reads them on every request/sync,
    so overlap-token rotation does not require a pod rollout. Image pull
    Secrets are name-only references, not mounted credentials, and are needed
    for the LB to pull the controller's digest from a private registry.
    """
    serve_utils.get_lb_sync_auth_tokens(required=True)
    data_plane_auth_enabled = serve_utils.is_lb_data_plane_auth_enabled()
    file_env_names = [constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR]
    auth_volume_names = [LB_SYNC_AUTH_VOLUME_NAME]
    if data_plane_auth_enabled:
        serve_utils.get_lb_auth_tokens(required=True)
        file_env_names.append(constants.LB_AUTH_TOKENS_FILE_ENV_VAR)
        auth_volume_names.append(LB_DATA_PLANE_AUTH_VOLUME_NAME)
    envs = []
    for env_name in file_env_names:
        path = os.environ.get(env_name)
        if not path:
            raise RuntimeError(
                f'External load balancer mode requires file-backed auth: '
                f'{env_name} is not set.')
        envs.append({'name': env_name, 'value': path})
    envs.append({
        'name': constants.LB_DATA_PLANE_AUTH_ENABLED_ENV_VAR,
        'value': str(data_plane_auth_enabled).lower(),
    })

    pod_name = os.environ.get(constants.POD_NAME_ENV_VAR)
    if pod is None:
        pod = _read_controller_pod(namespace, context)
    if not pod_name:
        raise RuntimeError(f'Cannot mirror external LB auth volumes: '
                           f'{constants.POD_NAME_ENV_VAR} is not set.')
    controller_container = _controller_container(pod)
    volumes = []
    mounts = []
    for volume_name in auth_volume_names:
        volume = _find_named(pod.spec.volumes, volume_name)
        mount = _find_named(controller_container.volume_mounts, volume_name)
        if volume is None or mount is None:
            raise RuntimeError(
                f'Controller pod {pod_name!r} is missing projected auth '
                f'volume/mount {volume_name!r}. Install/upgrade the SkyPilot '
                'Helm chart with external LB auth configured.')
        volumes.append(_serialize_k8s_object(volume))
        mounts.append(_serialize_k8s_object(mount))

    image_pull_secrets = []
    for secret_ref in getattr(pod.spec, 'image_pull_secrets', None) or []:
        serialized = _serialize_k8s_object(secret_ref)
        name = serialized.get('name')
        if not isinstance(name, str) or not name:
            raise RuntimeError(
                f'Controller pod {pod_name!r} has an invalid imagePullSecret '
                f'reference: {serialized!r}.')
        # V1LocalObjectReference has only ``name``. Keep this name-only even
        # when tests or alternate clients return an over-specified dict.
        image_pull_secrets.append({'name': name})
    pod_runtime_fields, container_runtime_fields = (
        _portable_lb_runtime_contract(pod.spec, controller_container))
    return (envs, volumes, mounts, image_pull_secrets, pod_runtime_fields,
            container_runtime_fields, data_plane_auth_enabled)


def _lb_resources() -> dict:
    """Return validated resources for each generated LB container."""
    raw = os.environ.get(constants.LB_RESOURCES_ENV_VAR)
    if not raw:
        return _DEFAULT_LB_RESOURCES
    try:
        resources = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f'{constants.LB_RESOURCES_ENV_VAR} must contain JSON: {e}') from e
    if not isinstance(resources, dict):
        raise RuntimeError(
            f'{constants.LB_RESOURCES_ENV_VAR} must contain a JSON object.')
    for key in resources:
        if key not in ('requests', 'limits'):
            raise RuntimeError(
                f'{constants.LB_RESOURCES_ENV_VAR} contains unsupported key '
                f'{key!r}.')
        if not isinstance(resources[key], dict):
            raise RuntimeError(
                f'{constants.LB_RESOURCES_ENV_VAR}.{key} must be an object.')
    return resources


def _build_deployment_dict(service_name: str,
                           deployment_name: str,
                           image: str,
                           auth_envs: list,
                           auth_volumes: list,
                           auth_volume_mounts: list,
                           image_pull_secrets: list,
                           pod_runtime_fields: dict,
                           container_runtime_fields: dict,
                           image_pull_policy: str,
                           termination_grace_period_seconds: int,
                           controller_image_digest: Optional[str] = None,
                           service_hash: Optional[str] = None,
                           resources: Optional[dict] = None) -> dict:
    container = {
        'name': 'load-balancer',
        'image': image,
        'imagePullPolicy': image_pull_policy,
        'command': ['python', '-m', 'sky.serve.load_balancer'],
        'args': [
            '--controller-addr',
            _controller_addr(service_name),
            '--load-balancer-port',
            str(constants.LOAD_BALANCER_PORT_START),
            '--service-hash',
            service_hash,
        ],
        'ports': [{
            'containerPort': constants.LOAD_BALANCER_PORT_START
        }],
        # Gate the Service endpoints on the LB's drain-aware health route: on
        # SIGTERM / rolling update the route flips to 503, so k8s removes the
        # draining pod from the endpoints before it exits.
        'startupProbe': {
            'httpGet': {
                'path': constants.LB_LIVENESS_ENDPOINT_PATH,
                'port': constants.LOAD_BALANCER_PORT_START,
            },
            'periodSeconds': 2,
            'failureThreshold': 60,
            'timeoutSeconds': 1,
        },
        'readinessProbe': {
            'httpGet': {
                'path': _LB_HEALTH_PATH,
                'port': constants.LOAD_BALANCER_PORT_START,
            },
            'periodSeconds': 2,
            'failureThreshold': 1,
            'timeoutSeconds': 1,
        },
        'livenessProbe': {
            'httpGet': {
                'path': constants.LB_LIVENESS_ENDPOINT_PATH,
                'port': constants.LOAD_BALANCER_PORT_START,
            },
            'periodSeconds': 10,
            'failureThreshold': 3,
            'timeoutSeconds': 1,
        },
    }
    # The LB gets only the sync and data-plane projected files. Its pod UID is
    # the durable session identity used to make rollout-overlap drain proofs
    # fail closed until Kubernetes has actually removed the old pod.
    container['env'] = auth_envs + [
        {
            'name': constants.EXTERNAL_LB_ENABLED_ENV_VAR,
            'value': 'true',
        },
        {
            'name': constants.LB_POD_UID_ENV_VAR,
            'valueFrom': {
                'fieldRef': {
                    'fieldPath': 'metadata.uid'
                }
            },
        },
    ]
    container['volumeMounts'] = auth_volume_mounts
    container['resources'] = resources or _DEFAULT_LB_RESOURCES
    # The sanitized identity/filesystem context keeps projected 0400 auth files
    # readable under the same fsGroup/runAsUser. Privileged/capability fields
    # from the higher-trust API container are deliberately excluded.
    container.update(container_runtime_fields)
    # TODO(fcapponi): no TLS handling in this pass -- TLS terminates at the
    # ingress/ALB (see the LB ingress+auth design), not at the LB pod.
    pod_labels = {APP_LABEL_KEY: deployment_name}
    pod_labels.update(_object_labels(service_name, service_hash))
    template_metadata = {'labels': pod_labels}
    if controller_image_digest:
        template_metadata['annotations'] = {
            CONTROLLER_DIGEST_ANNOTATION: controller_image_digest
        }
    pod_spec = {
        **pod_runtime_fields,
        'terminationGracePeriodSeconds': termination_grace_period_seconds,
        # LB pods call only the stable HTTP controller proxy. They never need
        # Kubernetes credentials, so do not expose the namespace-scoped
        # service-account token to this data-plane process.
        'automountServiceAccountToken': False,
        'containers': [container],
        'volumes': auth_volumes,
        **({
            'imagePullSecrets': image_pull_secrets,
        } if image_pull_secrets else {}),
    }
    return {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {
            'name': deployment_name,
            'labels': _object_labels(service_name, service_hash),
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
                    APP_LABEL_KEY: deployment_name,
                }
            },
            'template': {
                # The digest annotation rolls the LB with the resolved API
                # image, while an absent digest is impossible in production.
                'metadata': template_metadata,
                'spec': pod_spec,
            },
        },
    }


def _build_service_dict(service_name: str,
                        service_name_k8s: str,
                        deployment_name: str,
                        service_hash: Optional[str] = None) -> dict:
    return {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {
            'name': service_name_k8s,
            'labels': _object_labels(service_name, service_hash),
        },
        'spec': {
            'type': 'ClusterIP',
            'selector': {
                APP_LABEL_KEY: deployment_name,
                **({
                    SERVICE_HASH_LABEL_KEY: service_hash
                } if service_hash else {}),
            },
            'ports': [{
                'port': constants.LOAD_BALANCER_PORT_START,
                'targetPort': constants.LOAD_BALANCER_PORT_START,
                'protocol': 'TCP',
            }],
        },
    }


def _service_has_desired_routing(service, desired: dict) -> bool:
    """Whether mutable Service routing fields match the desired contract."""
    if isinstance(service, dict):
        spec = service.get('spec', {})
        selector = spec.get('selector', {}) or {}
        ports = spec.get('ports', []) or []
    else:
        spec = getattr(service, 'spec', None)
        selector = getattr(spec, 'selector', {}) or {}
        ports = getattr(spec, 'ports', None) or []

    def _port_tuple(port) -> Tuple[Any, Any, Any]:
        if isinstance(port, dict):
            return (port.get('port'), port.get('targetPort'),
                    port.get('protocol', 'TCP'))
        return (getattr(port, 'port', None), getattr(port, 'target_port', None),
                getattr(port, 'protocol', None) or 'TCP')

    desired_spec = desired['spec']
    return (selector == desired_spec['selector'] and
            [_port_tuple(port) for port in ports
            ] == [_port_tuple(port) for port in desired_spec['ports']])


def _lb_deployment_is_ready(deployment) -> bool:
    if isinstance(deployment, dict):
        metadata = deployment.get('metadata', {})
        spec = deployment.get('spec', {})
        status = deployment.get('status', {})
        generation = metadata.get('generation')
        observed = status.get('observedGeneration')
        desired = spec.get('replicas') or 1
        total = status.get('replicas') or 0
        updated = status.get('updatedReplicas') or 0
        available = status.get('availableReplicas') or 0
        unavailable = status.get('unavailableReplicas') or 0
    else:
        metadata = deployment.metadata
        spec = deployment.spec
        status = deployment.status
        generation = getattr(metadata, 'generation', None)
        observed = getattr(status, 'observed_generation', None)
        desired = getattr(spec, 'replicas', None) or 1
        total = getattr(status, 'replicas', None) or 0
        updated = getattr(status, 'updated_replicas', None) or 0
        available = getattr(status, 'available_replicas', None) or 0
        unavailable = getattr(status, 'unavailable_replicas', None) or 0
    generation_observed = (generation is not None and observed is not None and
                           observed >= generation)
    # During maxSurge an old Ready Pod can keep available_replicas at the
    # desired count while the updated Pod is still unready. Requiring every
    # extant replica to belong to the updated ReplicaSet prevents that false
    # positive and makes Service selector changes safe after this returns.
    return (generation_observed and updated >= desired and total <= updated and
            available >= desired and unavailable == 0)


def _wait_for_lb_deployment_ready(context: str, namespace: str,
                                  deployment_name: str) -> None:
    """Wait until the desired LB rollout has an available updated Pod."""
    deadline = time.monotonic() + constants.LB_DEPLOYMENT_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        deployment = kubernetes.apps_api(context).read_namespaced_deployment(
            deployment_name, namespace)
        if _lb_deployment_is_ready(deployment):
            return
        time.sleep(constants.LB_DEPLOYMENT_READY_POLL_SECONDS)
    raise RuntimeError(
        f'External load balancer Deployment {deployment_name!r} did not '
        f'become ready within '
        f'{constants.LB_DEPLOYMENT_READY_TIMEOUT_SECONDS}s. Check '
        f'`kubectl describe deployment/{deployment_name} -n {namespace}` and '
        'the LB Pod logs.')


def create_lb_deployment_and_service(service_name: str,
                                     termination_grace_period_seconds: int,
                                     service_hash: str) -> None:
    """Create the per-service LB Deployment + Service (idempotent).

    A 409 (already exists) patches the Deployment to the desired proxy/auth/
    shutdown contract, making the call safe for recovery and upgrades from the
    old direct-IP/shared-Service topology.
    """
    if not _lb_mode_active():
        return
    if not service_hash:
        raise RuntimeError('External load balancer requires a service hash.')
    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    deployment_name = lb_deployment_name(service_name)
    service_name_k8s = lb_service_name(service_name)
    controller_pod = _read_controller_pod(namespace, context)
    image, image_pull_policy, controller_digest = _resolve_lb_image(
        namespace, context, pod=controller_pod)
    (auth_envs, auth_volumes, auth_mounts, image_pull_secrets,
     pod_runtime_fields, container_runtime_fields,
     data_plane_auth_enabled) = _resolve_lb_auth_projection(namespace,
                                                            context,
                                                            pod=controller_pod)

    deployment_dict = _build_deployment_dict(
        service_name, deployment_name, image, auth_envs, auth_volumes,
        auth_mounts, image_pull_secrets, pod_runtime_fields,
        container_runtime_fields, image_pull_policy,
        termination_grace_period_seconds, controller_digest, service_hash,
        _lb_resources())
    service_dict = _build_service_dict(service_name, service_name_k8s,
                                       deployment_name, service_hash)

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
        deployment_patch = deployment_dict
        if not data_plane_auth_enabled:
            # Strategic-merge omission does not delete named list entries.
            # Explicitly remove projections left by a prior auth-enabled
            # Deployment while keeping the create body valid Kubernetes.
            deployment_patch = copy.deepcopy(deployment_dict)
            container = deployment_patch['spec']['template']['spec'][
                'containers'][0]
            container['env'].append({
                'name': constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                '$patch': 'delete',
            })
            container['volumeMounts'].append({
                # volumeMounts uses mountPath (not name) as its strategic
                # merge key. Omitting it makes the API reject the patch.
                'mountPath': _LB_DATA_PLANE_AUTH_MOUNT_PATH,
                '$patch': 'delete',
            })
            deployment_patch['spec']['template']['spec']['volumes'].append({
                'name': LB_DATA_PLANE_AUTH_VOLUME_NAME,
                '$patch': 'delete',
            })
        kubernetes.apps_api(context).patch_namespaced_deployment(
            deployment_name, namespace, deployment_patch)
    service_existed = False
    service_was_fenced = False
    try:
        kubernetes.core_api(context).create_namespaced_service(
            namespace, service_dict)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 409:
            raise
        service_existed = True
        existing_service = kubernetes.core_api(context).read_namespaced_service(
            service_name_k8s, namespace)
        if isinstance(existing_service, dict):
            existing_selector = existing_service.get('spec', {}).get(
                'selector', {}) or {}
        else:
            existing_selector = getattr(existing_service.spec, 'selector',
                                        {}) or {}
        if (service_hash and
                existing_selector.get(SERVICE_HASH_LABEL_KEY) != service_hash):
            # Same-name recreation must fail closed immediately: withdraw old
            # incarnation endpoints while the new LB rolls out.
            kubernetes.core_api(context).patch_namespaced_service(
                service_name_k8s, namespace, {
                    'metadata': {
                        'labels': service_dict['metadata']['labels']
                    },
                    'spec': {
                        'selector': service_dict['spec']['selector'],
                    },
                })
            service_was_fenced = True

    # Object existence is not endpoint readiness. Do not let service.py publish
    # load_balancer_port (which unblocks `sky serve up`) until the desired
    # rollout has an updated Pod that passed the LB's sync-backed readiness
    # probe. Bad image/Secret/runtime contracts now fail startup visibly rather
    # than advertising a dead endpoint.
    _wait_for_lb_deployment_ready(context, namespace, deployment_name)
    if service_existed:
        logger.debug(f'LB Service {service_name_k8s} already exists; '
                     f'reconciling it after the desired rollout is ready '
                     f'(fenced={service_was_fenced}).')
        # Patch only mutable fields, preserving the allocated ClusterIP.
        kubernetes.core_api(context).patch_namespaced_service(
            service_name_k8s, namespace, {
                'metadata': {
                    'labels': service_dict['metadata']['labels']
                },
                'spec': {
                    'selector': service_dict['spec']['selector'],
                    'ports': service_dict['spec']['ports'],
                },
            })


def ensure_lb_objects_exist(service_name: str,
                            termination_grace_period_seconds: int,
                            service_hash: str,
                            controller_ip: Optional[str] = None) -> bool:
    """Recreate the per-service LB Deployment + Service if either is missing.

    Self-heal for out-of-band deletion: the k8s Deployment only heals its own
    *pod*, and nothing recreates a deleted Deployment/Service until the next
    HA recovery -- so the per-service supervision loop calls this
    periodically. Reads first and mutates only when an object is missing or the
    service's termination budget changed, so steady state is two GETs with no
    patch churn. Image/auth inputs change on an API pod restart, whose recovery
    path runs the full reconcile.

    No-op outside external-LB + in-cluster mode. Raises on k8s API errors
    other than 404; callers treat this as best-effort and retry.
    """
    if not _lb_mode_active():
        return False
    if not service_hash:
        raise RuntimeError('External load balancer requires a service hash.')
    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()

    def _read_or_missing(read_fn, name: str):
        try:
            return read_fn(name, namespace), False
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 404:
                raise
            return None, True

    deployment, deployment_missing = _read_or_missing(
        kubernetes.apps_api(context).read_namespaced_deployment,
        lb_deployment_name(service_name))
    service, service_missing = _read_or_missing(
        kubernetes.core_api(context).read_namespaced_service,
        lb_service_name(service_name))
    desired_service = _build_service_dict(service_name,
                                          lb_service_name(service_name),
                                          lb_deployment_name(service_name),
                                          service_hash)

    grace_drifted = False
    if deployment is not None:
        if isinstance(deployment, dict):
            existing_grace = deployment.get('spec', {}).get('template', {}).get(
                'spec', {}).get('terminationGracePeriodSeconds')
        else:
            existing_grace = getattr(
                getattr(getattr(deployment.spec, 'template', None), 'spec',
                        None), 'termination_grace_period_seconds', None)
        grace_drifted = existing_grace != termination_grace_period_seconds
    hash_drifted = False
    if service_hash and deployment is not None:
        if isinstance(deployment, dict):
            labels = deployment.get('spec', {}).get('template', {}).get(
                'metadata', {}).get('labels', {}) or {}
        else:
            labels = getattr(getattr(deployment, 'spec', None), 'template',
                             None)
            labels = getattr(getattr(labels, 'metadata', None), 'labels',
                             {}) or {}
        hash_drifted = labels.get(SERVICE_HASH_LABEL_KEY) != service_hash
    routing_drifted = (
        service is not None and
        not _service_has_desired_routing(service, desired_service))
    if (not deployment_missing and not service_missing and not grace_drifted and
            not hash_drifted and not routing_drifted):
        assert deployment is not None
        return _lb_deployment_is_ready(deployment)
    # The objects may be missing because the service is being torn down or
    # taken over concurrently. Re-check OWNERSHIP (not mere row existence)
    # right before mutating -- the create-time mirror of reconcile's
    # delete-time re-check: a row that is gone means down/purge won, and a
    # row carrying another controller_pid means a same-name successor or an
    # HA takeover owns the service now. Without the pid check, a stale
    # controller could 409-patch the successor's fresh Deployment back to its
    # own (stale) controller port -- and the periodic ensure deliberately
    # never repairs drift, so that would stick until the next recovery. A
    # residual TOCTOU window remains (the row can flip between this read and
    # the k8s create; the two stores cannot be updated transactionally), but
    # it is milliseconds wide, requires losing ownership in exactly that
    # window, and is bounded: the next recovery's reconcile reaps a dead
    # service's LB, and the live owner's own ensure/recovery converges a
    # same-name successor's objects.
    record = serve_state.get_service_from_name(service_name)
    if (record is None or record.get('controller_pid') != os.getpid() or
        (service_hash and record.get('hash') != service_hash) or
        (controller_ip and record.get('controller_ip') != controller_ip)):
        logger.info(f'External LB objects for {service_name!r} are missing '
                    'but this process no longer owns the service '
                    '(row gone or taken over); skipping recreation.')
        return False
    logger.warning(f'External LB objects for {service_name!r} require '
                   f'reconciliation (deployment_missing={deployment_missing}, '
                   f'service_missing={service_missing}, '
                   f'grace_drifted={grace_drifted}, '
                   f'hash_drifted={hash_drifted}, '
                   f'routing_drifted={routing_drifted}); applying desired '
                   'state.')
    create_lb_deployment_and_service(service_name,
                                     termination_grace_period_seconds,
                                     service_hash)
    return True


def get_lb_pod_authority(service_name: str) -> Optional[LbPodAuthority]:
    """Return the Ready and live LB Pod identities from one Kubernetes list.

    The query is scoped to this service's collision-resistant Deployment
    label. A Pod is demand-authoritative only while Running, Ready, and not
    terminating. A terminating Pod remains live until Kubernetes removes it:
    it may still be finishing a streaming response after readiness is
    withdrawn. Any API or malformed-Pod failure returns ``None`` so both
    authority levels fail closed.
    """
    if not _lb_mode_active():
        return None
    try:
        context = kubernetes.in_cluster_context_name()
        namespace = get_lb_namespace()
        record = serve_state.get_service_from_name(service_name)
        service_hash = record.get('hash') if record else None
        if not service_hash:
            logger.warning(f'Cannot determine the active incarnation for '
                           f'{service_name!r}; load balancer reports will '
                           'fail closed.')
            return None
        label_selector = (f'{APP_LABEL_KEY}={lb_deployment_name(service_name)},'
                          f'{SERVICE_HASH_LABEL_KEY}={service_hash}')
        pods = kubernetes.core_api(context).list_namespaced_pod(
            namespace, label_selector=label_selector)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Failed to list load balancer pods for '
                       f'{service_name!r}: {e}; load balancer reports will '
                       'fail closed.')
        return None
    live_uids: Set[str] = set()
    ready_nonterminating_uids: Set[str] = set()
    try:
        for pod in pods.items:
            metadata = getattr(pod, 'metadata', None)
            status = getattr(pod, 'status', None)
            phase = getattr(status, 'phase', None)
            if phase in ('Succeeded', 'Failed'):
                continue
            uid = getattr(metadata, 'uid', None)
            if not uid:
                # Silently dropping an unidentifiable live Pod could make a
                # second Pod appear to be the sole authority.
                raise ValueError('live load balancer Pod is missing its UID')
            uid = str(uid)
            live_uids.add(uid)
            terminating = getattr(metadata, 'deletion_timestamp', None)
            conditions = getattr(status, 'conditions', None) or []
            ready = any(
                getattr(condition, 'type', None) == 'Ready' and
                getattr(condition, 'status', None) == 'True'
                for condition in conditions)
            if phase == 'Running' and terminating is None and ready:
                ready_nonterminating_uids.add(uid)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Failed to parse load balancer pods for '
                       f'{service_name!r}: {e}; load balancer reports will '
                       'fail closed.')
        return None
    return LbPodAuthority(ready_nonterminating_uids, live_uids)


def stream_lb_logs(service_name: str, follow: bool, tail: Optional[int]) -> str:
    """Print logs from the current external LB Pod.

    The former in-pod implementation wrote ``load_balancer.log`` beside the
    controller. External-only SkyServe must read the Kubernetes Pod log
    instead, or ``sky serve logs --load-balancer`` silently tails a stale or
    nonexistent file.
    """
    require_external_lb_runtime()
    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    record = serve_state.get_service_from_name(service_name)
    service_hash = record.get('hash') if record else None
    if not service_hash:
        return (f'Cannot determine the active service incarnation for '
                f'{service_name!r}.')
    pods = kubernetes.core_api(context).list_namespaced_pod(
        namespace,
        label_selector=(f'{APP_LABEL_KEY}={lb_deployment_name(service_name)},'
                        f'{SERVICE_HASH_LABEL_KEY}={service_hash}'))
    candidates = [
        pod for pod in pods.items
        if getattr(pod.status, 'phase', None) not in ('Succeeded', 'Failed')
    ]
    if not candidates:
        return f'No live external load balancer pod found for {service_name!r}.'
    # During maxSurge overlap the newest Pod is the replacement/current one.
    candidates.sort(key=lambda pod: str(
        getattr(pod.metadata, 'creation_timestamp', '') or ''),
                    reverse=True)
    pod_name = candidates[0].metadata.name
    kwargs = {
        'name': pod_name,
        'namespace': namespace,
        'follow': follow,
    }
    if tail is not None:
        kwargs['tail_lines'] = tail
    if not follow:
        log_text = kubernetes.core_api(context).read_namespaced_pod_log(
            **kwargs)
        if log_text:
            print(log_text, end='' if log_text.endswith('\n') else '\n')
        return ''

    response = kubernetes.core_api(context).read_namespaced_pod_log(
        **kwargs, _preload_content=False)
    try:
        for chunk in response.stream():
            if isinstance(chunk, bytes):
                chunk = chunk.decode('utf-8', errors='replace')
            print(chunk, end='', flush=True)
    except KeyboardInterrupt:
        return ''
    finally:
        response.close()
    # Keep stdout explicitly live for callers piping this process to a file.
    sys.stdout.flush()
    return ''


def delete_lb_objects(service_name: str) -> None:
    """Delete the per-service LB Deployment + Service (idempotent).

    No-op outside in-cluster mode. Cleanup deliberately does not consult the
    feature flag, so a process that retains in-cluster credentials can remove
    residual objects after a configuration change. The Helm contract still
    requires all services to be downed before disabling the capability. A 404
    (already gone) is ignored.
    """
    if not kubernetes_utils.is_incluster_config_available():
        return
    context = kubernetes.in_cluster_context_name()
    namespace = _cleanup_lb_namespace()
    if namespace is None:
        return
    deployment_name = lb_deployment_name(service_name)
    service_name_k8s = lb_service_name(service_name)

    errors = []
    # Remove the Service first. Once this succeeds, no new inference request
    # can reach cached replica routes while controller/replica teardown runs.
    try:
        kubernetes.core_api(context).delete_namespaced_service(
            service_name_k8s, namespace)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 404:
            errors.append(e)
        else:
            logger.debug(f'LB Service {service_name_k8s} already deleted.')
    try:
        kubernetes.apps_api(context).delete_namespaced_deployment(
            deployment_name, namespace)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 404:
            errors.append(e)
        else:
            logger.debug(f'LB Deployment {deployment_name} already deleted.')
    if errors:
        raise errors[0]


def reconcile_lb_objects(live_service_names: Set[str]) -> None:
    """Reap LB objects whose owning service is no longer live.

    No-op outside in-cluster mode and independent of the feature flag. It can
    therefore clean residual objects after a config change when the process
    still has Kubernetes credentials. Lists both LB Deployments and Services
    by the distinguishing label, maps each back to its service via the label
    value, and deletes any whose service is not in
    ``live_service_names``. Listing both kinds is required because a partial
    teardown may leave only the Service behind. Only deletes orphans --
    create-if-missing for live services is handled by the up()/recovery path.

    ``live_service_names`` is a stale snapshot (taken before the recovery
    sweep), so a service created after the snapshot would look absent here. To
    avoid deleting a live service's LB, re-check the DB at delete time and only
    reap an LB whose owning service is genuinely gone.
    """
    if not kubernetes_utils.is_incluster_config_available():
        return
    context = kubernetes.in_cluster_context_name()
    namespace = _cleanup_lb_namespace()
    if namespace is None:
        return

    deployments = kubernetes.apps_api(context).list_namespaced_deployment(
        namespace, label_selector=LB_SELECTOR_LABEL)
    services = kubernetes.core_api(context).list_namespaced_service(
        namespace, label_selector=LB_SELECTOR_LABEL)
    owning_services = set()
    for lb_object in list(deployments.items) + list(services.items):
        labels = lb_object.metadata.labels or {}
        owning_service = labels.get(SERVE_LB_LABEL_KEY)
        if owning_service is not None:
            owning_services.add(owning_service)

    for owning_service in owning_services:
        if owning_service in live_service_names:
            continue
        # Not in the stale snapshot -- confirm the service is truly gone at
        # delete time before reaping its LB (the snapshot predates any service
        # created during recovery).
        if serve_state.get_service_from_name(owning_service) is not None:
            continue
        delete_lb_objects(owning_service)
