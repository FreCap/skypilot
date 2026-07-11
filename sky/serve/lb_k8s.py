"""Controller-owned external load balancer lifecycle (in-cluster k8s).

The SkyServe controller runs in-cluster (inside an API-server pod) and owns a
per-service Kubernetes Deployment + Service for the load balancer. Kubernetes
owner references tie both objects to the stable Helm API Deployment, never its
rotating Pod or ReplicaSet. Each LB syncs through a route implemented by every
API-server pod. That stable proxy reads the authoritative controller owner
tuple from the database and forwards once, so controller failover never changes
or rolls the LB Deployment.

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
from typing import Any, Callable, Dict, NamedTuple, Optional, Set, Tuple
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

_OWNER_API_VERSION = 'apps/v1'
_OWNER_KIND = 'Deployment'

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
_LB_OBJECT_DELETION_TIMEOUT_SECONDS = 60
_LB_FOREGROUND_GC_MARGIN_SECONDS = 30
_LB_OBJECT_RECONCILIATION_TIMEOUT_SECONDS = 60
_LB_OBJECT_RECONCILIATION_POLL_SECONDS = 0.2

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


def lb_base_name(service_name: str,
                 resource_scope: Optional[str] = None) -> str:
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
    scope_suffix = ''
    if resource_scope is not None:
        scope_digest = hashlib.sha256(resource_scope.encode()).hexdigest()[:10]
        scope_suffix = f'-{scope_digest}'
    # Reserve room for the stable service digest and optional incarnation
    # digest within the 63-char budget.
    suffix = f'-{digest}{scope_suffix}'
    budget = _MAX_NAME_LEN - len(_LB_NAME_PREFIX) - len(suffix)
    truncated = sanitized[:budget].strip('-')
    if not truncated:
        # Sanitized to empty: the hash alone keeps the name valid and unique.
        return f'{_LB_NAME_PREFIX}{digest}{scope_suffix}'
    return f'{_LB_NAME_PREFIX}{truncated}{suffix}'


def lb_deployment_name(service_name: str,
                       resource_scope: Optional[str] = None) -> str:
    """RFC1123 name of the LB Deployment for ``service_name``."""
    return lb_base_name(service_name, resource_scope)


def lb_service_name(service_name: str,
                    resource_scope: Optional[str] = None) -> str:
    """RFC1123 name of the LB Service for ``service_name``."""
    return lb_base_name(service_name, resource_scope)


def lb_service_endpoint(service_name: str,
                        namespace: str,
                        resource_scope: Optional[str] = None) -> str:
    """In-cluster DNS ``host:port`` of the LB Service (no scheme)."""
    return (f'{lb_service_name(service_name, resource_scope)}.{namespace}.svc'
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


def _api_deployment_name() -> str:
    """Resolve the stable owner name across old and new Helm charts."""
    deployment_name = os.environ.get(constants.API_DEPLOYMENT_NAME_ENV_VAR)
    if deployment_name:
        return deployment_name
    release_name = os.environ.get(constants.RELEASE_NAME_ENV_VAR)
    if release_name:
        return f'{release_name}-api-server'
    raise RuntimeError(
        'External load balancer mode requires either '
        f'{constants.API_DEPLOYMENT_NAME_ENV_VAR} or '
        f'{constants.RELEASE_NAME_ENV_VAR}. Install/upgrade the SkyPilot Helm '
        'chart with serve.externalLoadBalancer.enabled=true.')


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
            'serve.externalLoadBalancer.enabled in the SkyPilot Helm release; '
            'the in-pod load balancer is no longer supported.')
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
    _api_deployment_name()
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


def lb_service_endpoint_or_none(
        service_name: str,
        resource_scope: Optional[str] = None) -> Optional[str]:
    """The LB Service endpoint (host:port, no scheme), or None if inactive.

    Returns None outside the installed external-LB platform. Real service
    startup fails closed through :func:`require_external_lb_runtime`; this
    optional form remains useful to status code inspecting old/pool rows.
    """
    if not _lb_mode_active():
        return None
    return lb_service_endpoint(service_name, get_lb_namespace(), resource_scope)


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


def _metadata_value(obj, dict_key: str, attr_name: str):
    """Read one metadata field from a Kubernetes model or response dict."""
    metadata = (obj.get('metadata') if isinstance(obj, dict) else getattr(
        obj, 'metadata', None))
    if metadata is None:
        return None
    if isinstance(metadata, dict):
        return metadata.get(dict_key)
    return getattr(metadata, attr_name, None)


def _owner_reference_value(owner_reference, dict_key: str, attr_name: str):
    if isinstance(owner_reference, dict):
        return owner_reference.get(dict_key)
    return getattr(owner_reference, attr_name, None)


def _api_deployment_owner_reference(context: str, namespace: str) -> dict:
    """Resolve the stable Helm Deployment identity used for LB ownership."""
    deployment_name = _api_deployment_name()
    try:
        deployment = kubernetes.apps_api(context).read_namespaced_deployment(
            deployment_name, namespace)
    except kubernetes.api_exception() as e:
        raise RuntimeError(
            f'Cannot resolve external load balancer owner Deployment '
            f'{namespace}/{deployment_name}: {e}') from e
    uid = _metadata_value(deployment, 'uid', 'uid')
    if not uid:
        raise RuntimeError(
            f'Cannot resolve external load balancer owner Deployment '
            f'{namespace}/{deployment_name}: Kubernetes returned no UID.')
    return {
        'apiVersion': _OWNER_API_VERSION,
        'kind': _OWNER_KIND,
        'name': deployment_name,
        'uid': str(uid),
        'controller': False,
        'blockOwnerDeletion': False,
    }


def _owner_reference_identity(owner_reference) -> Tuple[Any, Any, Any, Any]:
    return (
        _owner_reference_value(owner_reference, 'apiVersion', 'api_version'),
        _owner_reference_value(owner_reference, 'kind', 'kind'),
        _owner_reference_value(owner_reference, 'name', 'name'),
        str(_owner_reference_value(owner_reference, 'uid', 'uid') or ''),
    )


def _live_deployment_owner_uid(context: str, namespace: str,
                               deployment_name: str) -> Optional[str]:
    """Return a referenced Deployment's live UID, or None after deletion."""
    try:
        deployment = kubernetes.apps_api(context).read_namespaced_deployment(
            deployment_name, namespace)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) == 404:
            return None
        raise RuntimeError(
            f'Cannot verify owner Deployment {namespace}/{deployment_name}: '
            f'{e}') from e
    uid = _metadata_value(deployment, 'uid', 'uid')
    if not uid:
        raise RuntimeError(
            f'Cannot verify owner Deployment {namespace}/{deployment_name}: '
            'Kubernetes returned no UID.')
    return str(uid)


def _require_existing_lb_object_ownership(context: str, namespace: str,
                                          object_name: str, existing,
                                          owner_reference: dict,
                                          service_hash: str) -> str:
    """Prove an existing LB object belongs to this exact incarnation.

    Same-name objects are never adopted.  The returned resourceVersion must be
    included in the caller's mutation so a concurrent ownership or incarnation
    change fails with a Kubernetes conflict instead of modifying that object.
    """
    desired_identity = _owner_reference_identity(owner_reference)
    owner_references = _metadata_value(existing, 'ownerReferences',
                                       'owner_references') or []
    owner_identities = [
        _owner_reference_identity(reference) for reference in owner_references
    ]
    if owner_identities != [desired_identity]:
        raise RuntimeError(
            f'Refusing to reconcile external load balancer object '
            f'{namespace}/{object_name}: it is not owned exactly by the '
            f'current API Deployment identity {desired_identity!r}.')

    labels = _metadata_value(existing, 'labels', 'labels') or {}
    actual_service_hash = labels.get(SERVICE_HASH_LABEL_KEY)
    if actual_service_hash != service_hash:
        raise RuntimeError(
            f'Refusing to reconcile external load balancer object '
            f'{namespace}/{object_name}: service incarnation label is '
            f'{actual_service_hash!r}, expected {service_hash!r}.')

    resource_version = _metadata_value(existing, 'resourceVersion',
                                       'resource_version')
    if not resource_version:
        raise RuntimeError(
            f'Refusing to reconcile external load balancer object '
            f'{namespace}/{object_name}: Kubernetes returned no '
            'resourceVersion for the existing object.')

    desired_owner_uid = str(owner_reference['uid'])
    live_desired_owner_uid = _live_deployment_owner_uid(context, namespace,
                                                        owner_reference['name'])
    if live_desired_owner_uid != desired_owner_uid:
        raise RuntimeError(
            f'Refusing to reconcile external load balancer object '
            f'{namespace}/{object_name}: desired owner Deployment '
            f'{namespace}/{owner_reference["name"]} changed from UID '
            f'{desired_owner_uid} to {live_desired_owner_uid!r}.')
    return str(resource_version)


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
    # Older chart versions rendered an explicitly allowed ``resources: null``
    # value as JSON null. Treat that the same as the new chart's empty object
    # so an image-first upgrade remains compatible.
    if resources is None:
        return {}
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
                           resources: Optional[dict] = None,
                           owner_reference: Optional[dict] = None) -> dict:
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
            **({
                'ownerReferences': [owner_reference]
            } if owner_reference else {}),
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
                        service_hash: Optional[str] = None,
                        owner_reference: Optional[dict] = None) -> dict:
    return {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {
            'name': service_name_k8s,
            'labels': _object_labels(service_name, service_hash),
            **({
                'ownerReferences': [owner_reference]
            } if owner_reference else {}),
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


def _wait_for_lb_deployment_ready(
        context: str,
        namespace: str,
        deployment_name: str,
        continue_guard: Optional[Callable[[], bool]] = None) -> None:
    """Wait until the desired LB rollout has an available updated Pod."""
    deadline = time.monotonic() + constants.LB_DEPLOYMENT_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if continue_guard is not None and not continue_guard():
            raise RuntimeError(
                f'Lost service ownership while waiting for LB Deployment '
                f'{deployment_name!r}.')
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


def create_lb_deployment_and_service(
        service_name: str,
        termination_grace_period_seconds: int,
        service_hash: str,
        continue_guard: Optional[Callable[[], bool]] = None,
        resource_scope: Optional[str] = None) -> None:
    """Create the per-service LB Deployment + Service (idempotent).

    New objects are created with the stable Helm API Deployment as their
    Kubernetes owner. On a 409, normal desired-spec reconciliation is allowed
    only when the existing object already has that exact owner identity and the
    exact service-incarnation label. Every mutation is resourceVersion-guarded;
    same-name foreign, legacy, or stale-incarnation objects are never adopted.
    """
    if not _lb_mode_active():
        return
    if not service_hash:
        raise RuntimeError('External load balancer requires a service hash.')

    def _assert_continues(phase: str) -> None:
        if continue_guard is not None and not continue_guard():
            raise RuntimeError(f'Lost service ownership before {phase} for '
                               f'{service_name!r}; aborting LB reconciliation.')

    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    deployment_name = lb_deployment_name(service_name, resource_scope)
    service_name_k8s = lb_service_name(service_name, resource_scope)
    owner_reference = _api_deployment_owner_reference(context, namespace)
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
        _lb_resources(), owner_reference)
    service_dict = _build_service_dict(service_name, service_name_k8s,
                                       deployment_name, service_hash,
                                       owner_reference)

    deployment_patch = deployment_dict
    if not data_plane_auth_enabled:
        # Strategic-merge omission does not delete named list entries.
        # Explicitly remove projections left by a prior auth-enabled
        # Deployment while keeping the create body valid Kubernetes.
        deployment_patch = copy.deepcopy(deployment_dict)
        container = deployment_patch['spec']['template']['spec']['containers'][
            0]
        container['env'].append({
            'name': constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
            '$patch': 'delete',
        })
        container['volumeMounts'].append({
            # volumeMounts uses mountPath (not name) as its strategic merge
            # key. Omitting it makes the API reject the patch.
            'mountPath': _LB_DATA_PLANE_AUTH_MOUNT_PATH,
            '$patch': 'delete',
        })
        deployment_patch['spec']['template']['spec']['volumes'].append({
            'name': LB_DATA_PLANE_AUTH_VOLUME_NAME,
            '$patch': 'delete',
        })

    def _retry_reconciliation_or_raise(kind: str, name: str, deadline: float,
                                       error: Exception) -> None:
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f'LB {kind} {name!r} stayed terminating or disappearing '
                'during reconciliation.') from error
        time.sleep(_LB_OBJECT_RECONCILIATION_POLL_SECONDS)

    apps_api = kubernetes.apps_api(context)
    deployment_deadline = (time.monotonic() +
                           _LB_OBJECT_RECONCILIATION_TIMEOUT_SECONDS)
    while True:
        _assert_continues('creating the LB Deployment')
        try:
            apps_api.create_namespaced_deployment(namespace, deployment_dict)
            break
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 409:
                raise
        # The orphan reaper can win after our create gets 409 but before this
        # patch. A 404 means retry CREATE. A 409 means the old UID is commonly
        # still terminating and cannot be patched; wait for it to disappear
        # instead of failing same-name up.
        logger.debug(f'LB Deployment {deployment_name} already exists; '
                     'patching it to the desired spec.')
        _assert_continues('patching the LB Deployment')
        try:
            existing_deployment = apps_api.read_namespaced_deployment(
                deployment_name, namespace)
            metadata = (existing_deployment.get('metadata', {})
                        if isinstance(existing_deployment, dict) else getattr(
                            existing_deployment, 'metadata', None))
            deletion_timestamp = (metadata.get('deletionTimestamp')
                                  if isinstance(metadata, dict) else getattr(
                                      metadata, 'deletion_timestamp', None))
            if deletion_timestamp is not None:
                _retry_reconciliation_or_raise(
                    'Deployment', deployment_name, deployment_deadline,
                    RuntimeError('existing Deployment is terminating'))
                continue
            resource_version = _require_existing_lb_object_ownership(
                context, namespace, deployment_name, existing_deployment,
                owner_reference, service_hash)
            # Never rewrite ownerReferences. The resourceVersion precondition
            # ensures an ownership change after the validation above conflicts.
            desired_patch = copy.deepcopy(deployment_patch)
            desired_patch['metadata'].pop('ownerReferences', None)
            desired_patch['metadata']['resourceVersion'] = resource_version
            apps_api.patch_namespaced_deployment(deployment_name, namespace,
                                                 desired_patch)
            break
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) not in (404, 409):
                raise
            _retry_reconciliation_or_raise('Deployment', deployment_name,
                                           deployment_deadline, e)
    service_existed = False
    service_was_fenced = False
    core_api = kubernetes.core_api(context)
    service_deadline = (time.monotonic() +
                        _LB_OBJECT_RECONCILIATION_TIMEOUT_SECONDS)
    while True:
        _assert_continues('creating the LB Service')
        try:
            core_api.create_namespaced_service(namespace, service_dict)
            break
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 409:
                raise
        try:
            existing_service = core_api.read_namespaced_service(
                service_name_k8s, namespace)
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 404:
                raise
            _retry_reconciliation_or_raise('Service', service_name_k8s,
                                           service_deadline, e)
            continue
        metadata = (existing_service.get('metadata', {})
                    if isinstance(existing_service, dict) else getattr(
                        existing_service, 'metadata', None))
        deletion_timestamp = (metadata.get('deletionTimestamp') if isinstance(
            metadata, dict) else getattr(metadata, 'deletion_timestamp', None))
        if deletion_timestamp is not None:
            _retry_reconciliation_or_raise(
                'Service', service_name_k8s, service_deadline,
                RuntimeError('existing Service is terminating'))
            continue
        _assert_continues('validating the existing LB Service')
        try:
            resource_version = _require_existing_lb_object_ownership(
                context, namespace, service_name_k8s, existing_service,
                owner_reference, service_hash)
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) not in (404, 409):
                raise
            _retry_reconciliation_or_raise('Service', service_name_k8s,
                                           service_deadline, e)
            continue
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
            _assert_continues('fencing the old LB Service')
            try:
                core_api.patch_namespaced_service(
                    service_name_k8s, namespace, {
                        'metadata': {
                            'labels': service_dict['metadata']['labels'],
                            'resourceVersion': resource_version,
                        },
                        'spec': {
                            'selector': service_dict['spec']['selector'],
                        },
                    })
            except kubernetes.api_exception() as e:
                if getattr(e, 'status', None) not in (404, 409):
                    raise
                _retry_reconciliation_or_raise('Service', service_name_k8s,
                                               service_deadline, e)
                continue
            service_was_fenced = True
        service_existed = True
        break

    # Object existence is not endpoint readiness. Do not let service.py publish
    # load_balancer_port (which unblocks `sky serve up`) until the desired
    # rollout has an updated Pod that passed the LB's sync-backed readiness
    # probe. Bad image/Secret/runtime contracts now fail startup visibly rather
    # than advertising a dead endpoint.
    _wait_for_lb_deployment_ready(context,
                                  namespace,
                                  deployment_name,
                                  continue_guard=continue_guard)
    if service_existed:
        logger.debug(f'LB Service {service_name_k8s} already exists; '
                     f'reconciling it after the desired rollout is ready '
                     f'(fenced={service_was_fenced}).')
        # Patch only mutable fields, preserving the allocated ClusterIP.
        final_deadline = (time.monotonic() +
                          _LB_OBJECT_RECONCILIATION_TIMEOUT_SECONDS)
        while True:
            _assert_continues('finalizing the LB Service')
            try:
                existing_service = core_api.read_namespaced_service(
                    service_name_k8s, namespace)
                metadata = (existing_service.get('metadata', {}) if isinstance(
                    existing_service, dict) else getattr(
                        existing_service, 'metadata', None))
                deletion_timestamp = (metadata.get('deletionTimestamp') if
                                      isinstance(metadata, dict) else getattr(
                                          metadata, 'deletion_timestamp', None))
                if deletion_timestamp is not None:
                    _retry_reconciliation_or_raise(
                        'Service', service_name_k8s, final_deadline,
                        RuntimeError('existing Service is terminating'))
                    continue
                resource_version = _require_existing_lb_object_ownership(
                    context, namespace, service_name_k8s, existing_service,
                    owner_reference, service_hash)
                core_api.patch_namespaced_service(
                    service_name_k8s, namespace, {
                        'metadata': {
                            'labels': service_dict['metadata']['labels'],
                            'resourceVersion': resource_version,
                        },
                        'spec': {
                            'selector': service_dict['spec']['selector'],
                            'ports': service_dict['spec']['ports'],
                        },
                    })
                break
            except kubernetes.api_exception() as e:
                status = getattr(e, 'status', None)
                if status not in (404, 409):
                    raise
                if status == 404:
                    # Reaper deletion won after readiness. Recreate the
                    # mutable Service; the Deployment is already Ready. A 409
                    # here means the terminating old UID is still present, so
                    # fall back to the bounded patch/create loop.
                    try:
                        _assert_continues('recreating the LB Service')
                        core_api.create_namespaced_service(
                            namespace, service_dict)
                        break
                    except kubernetes.api_exception() as create_error:
                        if getattr(create_error, 'status', None) != 409:
                            raise
                        e = create_error
                _retry_reconciliation_or_raise('Service', service_name_k8s,
                                               final_deadline, e)


def ensure_lb_objects_exist(service_name: str,
                            termination_grace_period_seconds: int,
                            service_hash: str,
                            controller_ip: Optional[str] = None,
                            resource_scope: Optional[str] = None) -> bool:
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
        lb_deployment_name(service_name, resource_scope))
    service, service_missing = _read_or_missing(
        kubernetes.core_api(context).read_namespaced_service,
        lb_service_name(service_name, resource_scope))
    desired_service = _build_service_dict(
        service_name, lb_service_name(service_name, resource_scope),
        lb_deployment_name(service_name, resource_scope), service_hash)

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

    def _still_owns() -> bool:
        return serve_state.service_owner_matches(service_name, service_hash,
                                                 (os.getpid(), controller_ip))

    create_lb_deployment_and_service(service_name,
                                     termination_grace_period_seconds,
                                     service_hash,
                                     continue_guard=_still_owns,
                                     resource_scope=resource_scope)
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
        resource_scope = record.get('resource_scope') if record else None
        if not service_hash:
            logger.warning(f'Cannot determine the active incarnation for '
                           f'{service_name!r}; load balancer reports will '
                           'fail closed.')
            return None
        label_selector = (f'{APP_LABEL_KEY}='
                          f'{lb_deployment_name(service_name, resource_scope)},'
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
    resource_scope = record.get('resource_scope') if record else None
    if not service_hash:
        return (f'Cannot determine the active service incarnation for '
                f'{service_name!r}.')
    pods = kubernetes.core_api(context).list_namespaced_pod(
        namespace,
        label_selector=
        (f'{APP_LABEL_KEY}={lb_deployment_name(service_name, resource_scope)},'
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


def _lb_object_metadata_value(obj: Any, field: str) -> Any:
    """Read one Kubernetes metadata field from a model or dict."""
    if isinstance(obj, dict):
        return (obj.get('metadata') or {}).get(field)
    metadata = getattr(obj, 'metadata', None)
    attr = 'resource_version' if field == 'resourceVersion' else field
    return getattr(metadata, attr, None)


def _lb_object_deletion_timeout_seconds(obj: Any, kind: str) -> float:
    """Bound deletion by base API latency plus the Pod's real drain grace."""
    if kind != 'Deployment':
        return _LB_OBJECT_DELETION_TIMEOUT_SECONDS
    if isinstance(obj, dict):
        grace = (obj.get('spec', {}).get('template', {}).get(
            'spec', {}).get('terminationGracePeriodSeconds'))
    else:
        template = getattr(getattr(obj, 'spec', None), 'template', None)
        pod_spec = getattr(template, 'spec', None)
        grace = getattr(pod_spec, 'termination_grace_period_seconds', None)
    try:
        grace_seconds = max(0.0, float(grace))
    except (TypeError, ValueError):
        grace_seconds = 0.0
    return max(_LB_OBJECT_DELETION_TIMEOUT_SECONDS,
               grace_seconds + _LB_FOREGROUND_GC_MARGIN_SECONDS)


def get_api_deployment_owner_uid(
        require_runtime: bool = False) -> Optional[str]:
    """Resolve the current Helm API Deployment UID for fenced LB deletion."""
    if not kubernetes_utils.is_incluster_config_available():
        if require_runtime:
            raise RuntimeError(
                'Cannot prove external LB ownership without in-cluster '
                'Kubernetes credentials.')
        return None
    namespace = _cleanup_lb_namespace()
    if namespace is None:
        if require_runtime:
            raise RuntimeError(
                'Cannot prove external LB ownership without its Kubernetes '
                'namespace.')
        return None
    context = kubernetes.in_cluster_context_name()
    return str(_api_deployment_owner_reference(context, namespace)['uid'])


def _delete_lb_object_if_owned(read_fn, delete_fn, name: str, namespace: str,
                               expected_service_hash: str,
                               expected_api_deployment_name: str,
                               expected_api_deployment_uid: str, context: str,
                               kind: str) -> None:
    """GET then owner/UID/resourceVersion-precondition DELETE one object."""
    live_owner_uid = _live_deployment_owner_uid(context, namespace,
                                                expected_api_deployment_name)
    if live_owner_uid != expected_api_deployment_uid:
        raise RuntimeError(
            f'Refusing to delete LB {kind} {name!r}: API Deployment '
            f'{namespace}/{expected_api_deployment_name} changed from UID '
            f'{expected_api_deployment_uid!r} to {live_owner_uid!r}.')
    try:
        obj = read_fn(name, namespace)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) == 404:
            logger.debug(f'LB {kind} {name} already deleted.')
            return
        raise
    labels = _lb_object_metadata_value(obj, 'labels') or {}
    actual_hash = labels.get(SERVICE_HASH_LABEL_KEY)
    if actual_hash != expected_service_hash:
        raise RuntimeError(
            f'Refusing to delete LB {kind} {name!r}: expected incarnation '
            f'{expected_service_hash!r}, found {actual_hash!r}.')
    owner_references = _metadata_value(obj, 'ownerReferences',
                                       'owner_references') or []
    expected_owner_identity = (_OWNER_API_VERSION, _OWNER_KIND,
                               expected_api_deployment_name,
                               expected_api_deployment_uid)
    owner_identities = [
        _owner_reference_identity(reference) for reference in owner_references
    ]
    if owner_identities != [expected_owner_identity]:
        raise RuntimeError(
            f'Refusing to delete LB {kind} {name!r}: expected exact API '
            f'Deployment owner {expected_owner_identity!r}, found '
            f'{owner_identities!r}.')
    uid = _lb_object_metadata_value(obj, 'uid')
    resource_version = _lb_object_metadata_value(obj, 'resourceVersion')
    deletion_timeout_seconds = _lb_object_deletion_timeout_seconds(obj, kind)
    if not uid or not resource_version:
        raise RuntimeError(
            f'Refusing to delete LB {kind} {name!r} without Kubernetes UID '
            'and resourceVersion preconditions.')
    body = {
        'apiVersion': 'v1',
        'kind': 'DeleteOptions',
        'preconditions': {
            'uid': str(uid),
            'resourceVersion': str(resource_version),
        },
    }
    if kind == 'Deployment':
        # Wait for ReplicaSets/Pods to finish their configured drain grace
        # before replica teardown starts. Background propagation can make the
        # Deployment UID disappear while an LB Pod still owns live streams.
        body['propagationPolicy'] = 'Foreground'
    try:
        delete_fn(name, namespace, body=body)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) != 404:
            raise
        logger.debug(f'LB {kind} {name} already deleted.')
        return

    # Kubernetes DELETE success means accepted, not necessarily disappeared.
    # Keep the service DB row (and therefore the same-name up guard) until the
    # exact UID is gone; otherwise a successor can 409 against a terminating
    # object and accidentally adopt or patch the old incarnation.
    deadline = time.time() + deletion_timeout_seconds
    while True:
        try:
            remaining = read_fn(name, namespace)
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) == 404:
                return
            raise
        remaining_uid = _lb_object_metadata_value(remaining, 'uid')
        if remaining_uid != uid:
            raise RuntimeError(
                f'LB {kind} {name!r} was replaced while waiting for exact '
                f'UID {uid!r} to disappear (found {remaining_uid!r}).')
        if time.time() >= deadline:
            raise TimeoutError(
                f'Timed out waiting for LB {kind} {name!r} UID {uid!r} to '
                'be deleted.')
        time.sleep(0.2)


def delete_lb_objects(
        service_name: str,
        expected_service_hash: str,
        resource_scope: Optional[str] = None,
        require_runtime: bool = False,
        expected_api_deployment_uid: Optional[str] = None) -> None:
    """Delete one incarnation's LB objects with Kubernetes preconditions.

    No-op outside in-cluster mode. Cleanup deliberately does not consult the
    feature flag, so a process that retains in-cluster credentials can remove
    residual objects after a configuration change. The Helm contract still
    requires all services to be downed before disabling the capability. A 404
    (already gone) is ignored. A hash mismatch, missing object identity, or
    UID/resourceVersion conflict fails closed so a delayed A teardown cannot
    delete replacement B's same-name objects.
    """
    if not expected_service_hash:
        raise ValueError('LB deletion requires an expected service hash.')
    if not kubernetes_utils.is_incluster_config_available():
        if require_runtime:
            raise RuntimeError(
                'Cannot prove external LB deletion without in-cluster '
                'Kubernetes credentials.')
        return
    context = kubernetes.in_cluster_context_name()
    namespace = _cleanup_lb_namespace()
    if namespace is None:
        if require_runtime:
            raise RuntimeError(
                'Cannot prove external LB deletion without its Kubernetes '
                'namespace.')
        return
    expected_api_deployment_name = _api_deployment_name()
    if expected_api_deployment_uid is None:
        expected_api_deployment_uid = get_api_deployment_owner_uid(
            require_runtime=require_runtime)
    if not expected_api_deployment_uid:
        raise RuntimeError(
            'LB deletion requires the expected API Deployment owner UID.')
    deployment_name = lb_deployment_name(service_name, resource_scope)
    service_name_k8s = lb_service_name(service_name, resource_scope)

    errors = []
    # Remove the Service first. Once this succeeds, no new inference request
    # can reach cached replica routes while controller/replica teardown runs.
    try:
        core_api = kubernetes.core_api(context)
        _delete_lb_object_if_owned(
            core_api.read_namespaced_service,
            core_api.delete_namespaced_service, service_name_k8s, namespace,
            expected_service_hash, expected_api_deployment_name,
            expected_api_deployment_uid, context, 'Service')
    except Exception as e:  # pylint: disable=broad-except
        errors.append(e)
    try:
        apps_api = kubernetes.apps_api(context)
        _delete_lb_object_if_owned(
            apps_api.read_namespaced_deployment,
            apps_api.delete_namespaced_deployment, deployment_name, namespace,
            expected_service_hash, expected_api_deployment_name,
            expected_api_deployment_uid, context, 'Deployment')
    except Exception as e:  # pylint: disable=broad-except
        errors.append(e)
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
    # Kept for API compatibility with the recovery caller. Incarnation-scoped
    # resources require the fresh per-object hash check below; a name-only
    # snapshot cannot distinguish live successor B from orphan predecessor A.
    del live_service_names
    if not kubernetes_utils.is_incluster_config_available():
        return
    context = kubernetes.in_cluster_context_name()
    namespace = _cleanup_lb_namespace()
    if namespace is None:
        return
    expected_api_deployment_uid = get_api_deployment_owner_uid()
    if expected_api_deployment_uid is None:
        return
    expected_api_deployment_name = _api_deployment_name()

    deployments = kubernetes.apps_api(context).list_namespaced_deployment(
        namespace, label_selector=LB_SELECTOR_LABEL)
    services = kubernetes.core_api(context).list_namespaced_service(
        namespace, label_selector=LB_SELECTOR_LABEL)
    owning_services = set()
    for lb_object in list(deployments.items) + list(services.items):
        labels = lb_object.metadata.labels or {}
        owning_service = labels.get(SERVE_LB_LABEL_KEY)
        service_hash = labels.get(SERVICE_HASH_LABEL_KEY)
        if owning_service is not None and service_hash:
            owner_identities = [
                _owner_reference_identity(reference) for reference in (
                    getattr(lb_object.metadata, 'owner_references', None) or [])
            ]
            expected_owner_identity = (_OWNER_API_VERSION, _OWNER_KIND,
                                       expected_api_deployment_name,
                                       expected_api_deployment_uid)
            if owner_identities != [expected_owner_identity]:
                logger.warning(
                    f'Refusing to reap LB object for {owning_service!r}: '
                    'it is not owned by this API Deployment identity.')
                continue
            object_name = getattr(lb_object.metadata, 'name', None)
            if object_name is None:
                # Lightweight test/fake Kubernetes objects may omit name;
                # real API objects always have it. Preserve legacy behavior
                # for those fixtures.
                resource_scope = None
            elif object_name == lb_base_name(owning_service, service_hash):
                resource_scope = service_hash
            elif object_name == lb_base_name(owning_service):
                resource_scope = None
            else:
                logger.warning(
                    f'Refusing to reap LB object {object_name!r} for '
                    f'{owning_service!r}: name matches neither its legacy nor '
                    'incarnation-scoped identity.')
                continue
            owning_services.add((owning_service, service_hash, resource_scope))
        elif owning_service is not None:
            logger.warning(f'Refusing to reap legacy LB objects for '
                           f'{owning_service!r} without an incarnation label.')

    for (owning_service, expected_service_hash,
         resource_scope) in owning_services:
        # Name reuse can leave A's scoped objects beside live successor B's.
        # Protect only the exact live incarnation; a different current hash is
        # positive proof that this object belongs to the predecessor and is
        # safe to reap.  The stale name snapshot remains only a cheap hint.
        current_hash = serve_state.get_service_hash(owning_service)
        if current_hash == expected_service_hash:
            continue
        if resource_scope is None:
            delete_lb_objects(
                owning_service,
                expected_service_hash,
                expected_api_deployment_uid=expected_api_deployment_uid)
        else:
            delete_lb_objects(
                owning_service,
                expected_service_hash,
                resource_scope=resource_scope,
                expected_api_deployment_uid=(expected_api_deployment_uid))
