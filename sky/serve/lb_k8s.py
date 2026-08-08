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
- ``lb_service_endpoint_or_none`` — the W4 endpoint published by the cloud
  provider for the LB Service.

Lifecycle/reaper helpers remain no-ops when the platform feature is disabled;
starting a real service calls :func:`require_external_lb_runtime` and fails
closed instead of falling back to an in-pod LB.
"""
from collections.abc import Callable
from collections.abc import Mapping
import concurrent.futures
import contextvars
import copy
import hashlib
import ipaddress
import json
import math
import os
import re
import sys
import time
from typing import Any, NamedTuple
import urllib.parse

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import serve_state
from sky.serve import serve_utils

logger = sky_logging.init_logger(__name__)

# Pod-template annotation carrying the controller's resolved image digest —
# the LB Deployment's rollout trigger (see _resolve_lb_image).
CONTROLLER_DIGEST_ANNOTATION = 'skypilot.co/controller-image-digest'
LB_RUNTIME_REVISION_ANNOTATION = 'skypilot.co/lb-runtime-revision'

# Labels stamped on every LB object the controller owns.
#   parent=skypilot                 -> ownership marker (shared convention).
#   skypilot-serve-lb=<service>     -> distinguishing label; reconcile lists by
#                                      this key and maps back to the service.
PARENT_LABEL_KEY = 'parent'
PARENT_LABEL_VALUE = 'skypilot'
SERVE_LB_LABEL_KEY = 'skypilot-serve-lb'
SERVICE_HASH_LABEL_KEY = 'skypilot-serve-incarnation'
LB_SLOT_LABEL_KEY = 'skypilot-serve-lb-slot'
ACTIVE_SLOT_ANNOTATION_KEY = 'skypilot.co/serve-lb-active-slot'
CUTOVER_GENERATION_ANNOTATION_KEY = ('skypilot.co/serve-lb-cutover-generation')
DESIRED_RUNTIME_REVISION_ANNOTATION_KEY = (
    'skypilot.co/serve-lb-desired-runtime-revision')
# Pod selector label: app=<lb_deployment_name>.
APP_LABEL_KEY = 'app'
# Label-key selector used by reconcile to list all LB Deployments.
LB_SELECTOR_LABEL = SERVE_LB_LABEL_KEY

_OWNER_API_VERSION = 'apps/v1'
_OWNER_KIND = 'Deployment'

# AWS Load Balancer Controller TLS annotations. Only these keys are written, so
# the controller's own injected annotations (notably spec.loadBalancerClass's
# companions) are left alone; reconciliation compares desired annotations as a
# subset for the same reason.
_AWS_LB_SSL_CERT_ANNOTATION = ('service.beta.kubernetes.io/'
                               'aws-load-balancer-ssl-cert')
_AWS_LB_SSL_PORTS_ANNOTATION = ('service.beta.kubernetes.io/'
                                'aws-load-balancer-ssl-ports')
_AWS_LB_SSL_POLICY_ANNOTATION = ('service.beta.kubernetes.io/'
                                 'aws-load-balancer-ssl-negotiation-policy')
_EXTERNAL_DNS_HOSTNAME_ANNOTATION = ('external-dns.alpha.kubernetes.io/'
                                     'hostname')

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
_LB_MIN_READY_SECONDS = 5
_STRATEGIC_MERGE_PATCH_CONTENT_TYPE = ('application/strategic-merge-patch+json')

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

    ready_nonterminating_uids: set[str]
    live_uids: set[str]
    slot_by_uid: dict[str, lb_ha.LbSlot] | None = None
    selected_slot: lb_ha.LbSlot | None = None
    digest_by_uid: dict[str, str | None] | None = None
    revision_by_uid: dict[str, str | None] | None = None
    legacy_uids: set[str] | None = None
    terminating_uids: set[str] | None = None


class LbServiceRouting(NamedTuple):
    """Fenced routing state read from the stable Kubernetes Service."""

    active_slot: lb_ha.LbSlot
    generation: int
    resource_version: str
    desired_runtime_revision: str | None = None


class LbServiceTransitionRouting(NamedTuple):
    """Routing evidence while migrating between legacy and slot selectors."""

    active_slot: lb_ha.LbSlot | None
    legacy_selected: bool
    generation: int | None
    resource_version: str
    desired_runtime_revision: str | None = None


class LbRoleSnapshot(NamedTuple):
    """One fail-closed Kubernetes authority snapshot for an HA role report."""

    authority: LbPodAuthority
    routing: LbServiceRouting | LbServiceTransitionRouting


class LbRoleSnapshotStateMismatchError(RuntimeError):
    """The snapshot owner row no longer matches the controller state read."""


class LbRoleSnapshotRoutingError(RuntimeError):
    """The shared Service failed the phase-appropriate routing contract."""


def _strategic_merge_patch(context: str, resource_path: str, response_type: str,
                           name: str, namespace: str, body: dict[str,
                                                                 Any]) -> Any:
    """Patch a Kubernetes object with an explicit strategic-merge type.

    kubernetes-python 35.0.0 selects the first advertised PATCH content type,
    JSON Patch, even when the typed client's body is a dict.  The LB
    reconciliation bodies use strategic-merge directives and named-list merge
    keys, so issue the same generated-client call explicitly with the required
    content type.
    """
    return kubernetes.api_client(context).call_api(
        resource_path,
        'PATCH',
        path_params={
            'name': name,
            'namespace': namespace,
        },
        query_params=[],
        header_params={
            'Accept': 'application/json',
            'Content-Type': _STRATEGIC_MERGE_PATCH_CONTENT_TYPE,
        },
        body=body,
        post_params=[],
        files={},
        response_type=response_type,
        auth_settings=['BearerToken'],
        async_req=False,
        _return_http_data_only=True,
        collection_formats={},
        _preload_content=True,
        _request_timeout=None)


def lb_termination_grace_period_seconds(
        stream_timeout_seconds: float,
        graceful_drain_seconds: float | None) -> int:
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


def lb_base_name(service_name: str, resource_scope: str | None = None) -> str:
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
                       resource_scope: str | None = None) -> str:
    """RFC1123 name of the LB Deployment for ``service_name``."""
    return lb_base_name(service_name, resource_scope)


def lb_service_name(service_name: str,
                    resource_scope: str | None = None) -> str:
    """RFC1123 name of the LB Service for ``service_name``."""
    return lb_base_name(service_name, resource_scope)


def lb_slot_deployment_name(service_name: str,
                            slot: lb_ha.LbSlot,
                            resource_scope: str | None = None) -> str:
    """Incarnation-safe Deployment name for one immutable HA slot."""
    slot_scope = f'{resource_scope or "legacy"}:lb-slot:{slot.value}'
    return lb_base_name(service_name, slot_scope)


def lb_pdb_name(service_name: str, resource_scope: str | None = None) -> str:
    """Incarnation-safe PodDisruptionBudget name for both HA slots."""
    return lb_base_name(service_name, f'{resource_scope or "legacy"}:lb-pdb')


def _service_load_balancer_address(service: Any) -> str | None:
    """Return the first hostname/IP published on a LoadBalancer Service."""
    if isinstance(service, dict):
        status = service.get('status', {}) or {}
        load_balancer = status.get('loadBalancer', {}) or {}
        ingress = load_balancer.get('ingress', []) or []
    else:
        status = getattr(service, 'status', None)
        load_balancer = getattr(status, 'load_balancer', None)
        ingress = getattr(load_balancer, 'ingress', None) or []

    for entry in ingress:
        if isinstance(entry, dict):
            address = entry.get('hostname') or entry.get('ip')
        else:
            address = (getattr(entry, 'hostname', None) or
                       getattr(entry, 'ip', None))
        if not isinstance(address, str) or not address:
            continue
        try:
            parsed_ip = ipaddress.ip_address(address)
        except ValueError:
            return address
        return f'[{address}]' if parsed_ip.version == 6 else address
    return None


def _controller_addr(service_name: str) -> str:
    """Stable API-service proxy base URL used by the external LB.

    ``load_balancer.py`` appends a shared controller-path constant. Encoding
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


def _cleanup_lb_namespace() -> str | None:
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
    if not data_plane_auth_enabled:
        raise RuntimeError(
            'External SkyServe load balancers require '
            'serve.externalLoadBalancer.auth.lbDataPlane to be configured.')


def require_lb_ha_runtime() -> None:
    """Require the chart generation that installs per-service PDB RBAC."""
    require_external_lb_runtime()
    if os.environ.get(constants.LB_HA_RBAC_READY_ENV_VAR) != 'true':
        raise RuntimeError(
            'External load balancer high availability requires an updated '
            'SkyPilot Helm chart with per-service PodDisruptionBudget RBAC. '
            'Upgrade the chart before the API image or enabling HA.')


def lb_service_endpoint_or_none(service_name: str,
                                resource_scope: str |
                                None = None) -> str | None:
    """The LB Service endpoint (host:port, no scheme), or None if unavailable.

    Returns None outside the installed external-LB platform. Real service
    startup fails closed through :func:`require_external_lb_runtime`; this
    optional form remains useful to status code inspecting old/pool rows.
    """
    if not _lb_mode_active():
        return None
    namespace = get_lb_namespace()
    context = kubernetes.in_cluster_context_name()
    service_name_k8s = lb_service_name(service_name, resource_scope)
    try:
        service = kubernetes.core_api(context).read_namespaced_service(
            service_name_k8s, namespace)
    except kubernetes.api_exception() as e:
        if getattr(e, 'status', None) == 404:
            return None
        raise
    address = _service_load_balancer_address(service)
    if address is None:
        return None
    return f'{address}:{constants.LOAD_BALANCER_PORT_START}'


def _object_labels(service_name: str, service_hash: str | None = None) -> dict:
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
                      pod=None) -> tuple[str, str, str | None]:
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


def _resolved_image_reference(image: str, image_id: str) -> str | None:
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


def _image_repository(image: str) -> str | None:
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


def _valid_image_repository(repository: str) -> str | None:
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


def _lb_runtime_revision(controller_image_digest: str | None,
                         termination_grace_period_seconds: int,
                         service_hash: str | None,
                         priority_class_name: str | None = None) -> str:
    """Fingerprint the active-capable Pod fields that require slot rotation."""
    revision_fields = {
        'controller_image_digest': controller_image_digest,
        'termination_grace_period_seconds': termination_grace_period_seconds,
        'service_hash': service_hash,
    }
    # Preserve the historical revision exactly when the compatibility value is
    # empty. A configured class is part of the immutable active-slot runtime
    # identity and therefore participates in standby-first rotation.
    if priority_class_name:
        revision_fields['priority_class_name'] = priority_class_name
    payload = json.dumps(revision_fields, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()


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


def _owner_reference_identity(owner_reference) -> tuple[Any, Any, Any, Any]:
    return (
        _owner_reference_value(owner_reference, 'apiVersion', 'api_version'),
        _owner_reference_value(owner_reference, 'kind', 'kind'),
        _owner_reference_value(owner_reference, 'name', 'name'),
        str(_owner_reference_value(owner_reference, 'uid', 'uid') or ''),
    )


def _live_deployment_owner_uid(
        context: str,
        namespace: str,
        deployment_name: str,
        request_timeout_seconds: float | None = None) -> str | None:
    """Return a referenced Deployment's live UID, or None after deletion."""
    request_kwargs = ({
        '_request_timeout': request_timeout_seconds
    } if request_timeout_seconds is not None else {})
    try:
        deployment = kubernetes.apps_api(context).read_namespaced_deployment(
            deployment_name, namespace, **request_kwargs)
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


def _require_existing_lb_object_live_ownership(
        context: str,
        namespace: str,
        object_name: str,
        existing,
        service_hash: str,
        request_timeout_seconds: float | None = None) -> str:
    """Validate a read-only LB snapshot at one live owner linearization point.

    Unlike mutation callers, a role snapshot does not need to construct a new
    ownerReference.  Its Service already carries the exact owner identity, so
    reading the API Deployment before validating that identity is redundant.
    Read the live Deployment once *after* the Service and require its UID to
    equal the Service ownerReference.  Replacement before that read fails
    closed; replacement after it has the same boundary as the second read in
    ``_require_existing_lb_object_ownership``.
    """
    expected_owner_name = _api_deployment_name()
    owner_references = _metadata_value(existing, 'ownerReferences',
                                       'owner_references') or []
    owner_identities = [
        _owner_reference_identity(reference) for reference in owner_references
    ]
    if len(owner_identities) != 1:
        raise RuntimeError(
            f'Refusing to read external load balancer object '
            f'{namespace}/{object_name}: it does not have exactly one API '
            'Deployment owner identity.')
    owner_identity = owner_identities[0]
    expected_prefix = (_OWNER_API_VERSION, _OWNER_KIND, expected_owner_name)
    if owner_identity[:3] != expected_prefix or not owner_identity[3]:
        raise RuntimeError(
            f'Refusing to read external load balancer object '
            f'{namespace}/{object_name}: owner identity '
            f'{owner_identity!r} is not the expected API Deployment.')

    labels = _metadata_value(existing, 'labels', 'labels') or {}
    actual_service_hash = labels.get(SERVICE_HASH_LABEL_KEY)
    if actual_service_hash != service_hash:
        raise RuntimeError(
            f'Refusing to read external load balancer object '
            f'{namespace}/{object_name}: service incarnation label is '
            f'{actual_service_hash!r}, expected {service_hash!r}.')

    resource_version = _metadata_value(existing, 'resourceVersion',
                                       'resource_version')
    if not resource_version:
        raise RuntimeError(f'Refusing to read external load balancer object '
                           f'{namespace}/{object_name}: Kubernetes returned no '
                           'resourceVersion for the existing object.')

    live_owner_uid = _live_deployment_owner_uid(context, namespace,
                                                expected_owner_name,
                                                request_timeout_seconds)
    if live_owner_uid != owner_identity[3]:
        raise RuntimeError(
            f'Refusing to read external load balancer object '
            f'{namespace}/{object_name}: owner Deployment '
            f'{namespace}/{expected_owner_name} changed from UID '
            f'{owner_identity[3]} to {live_owner_uid!r}.')
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
                                  controller_container) -> tuple[dict, dict]:
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
        container_sanitized: dict[str, Any] = {
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
        pod=None) -> tuple[list, list, list, list, dict, dict, bool]:
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


def _lb_priority_class_name() -> str | None:
    """Return the exact server-owned PriorityClass, or compatibility-empty."""
    priority_class_name = os.environ.get(
        constants.LB_PRIORITY_CLASS_NAME_ENV_VAR)
    return priority_class_name or None


def _lb_pod_runtime_fields(pod_runtime_fields: dict, service_name: str,
                           service_hash: str | None,
                           slot: lb_ha.LbSlot | None) -> dict:
    """Merge LB placement without discarding controller runtime constraints."""
    runtime_fields = copy.deepcopy(pod_runtime_fields)
    selector_labels = {
        SERVE_LB_LABEL_KEY: service_name,
        **({
            SERVICE_HASH_LABEL_KEY: service_hash
        } if service_hash else {}),
    }
    runtime_fields['topologySpreadConstraints'] = [{
        'maxSkew': 1,
        'topologyKey': 'kubernetes.io/hostname',
        'whenUnsatisfiable': 'ScheduleAnyway',
        'labelSelector': {
            'matchLabels': selector_labels,
        },
    }]
    if slot is None:
        return runtime_fields

    affinity = copy.deepcopy(runtime_fields.get('affinity') or {})
    pod_anti_affinity = copy.deepcopy(affinity.get('podAntiAffinity') or {})
    required = list(
        pod_anti_affinity.get('requiredDuringSchedulingIgnoredDuringExecution')
        or [])
    required.append({
        'labelSelector': {
            'matchLabels': selector_labels,
        },
        'topologyKey': 'kubernetes.io/hostname',
    })
    preferred = list(
        pod_anti_affinity.get('preferredDuringSchedulingIgnoredDuringExecution')
        or [])
    preferred.append({
        'weight': 100,
        'podAffinityTerm': {
            'labelSelector': {
                'matchLabels': selector_labels,
            },
            'topologyKey': 'topology.kubernetes.io/zone',
        },
    })
    pod_anti_affinity['requiredDuringSchedulingIgnoredDuringExecution'] = (
        required)
    pod_anti_affinity['preferredDuringSchedulingIgnoredDuringExecution'] = (
        preferred)
    affinity['podAntiAffinity'] = pod_anti_affinity
    runtime_fields['affinity'] = affinity
    return runtime_fields


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
                           controller_image_digest: str | None = None,
                           service_hash: str | None = None,
                           resources: dict | None = None,
                           owner_reference: dict | None = None,
                           slot: lb_ha.LbSlot | None = None,
                           priority_class_name: str | None = None) -> dict:
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
        ] + (['--lb-slot', slot.value] if slot is not None else []),
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
                **_probe_scheme(),
            },
            'periodSeconds': 2,
            'failureThreshold': 60,
            'timeoutSeconds': 1,
        },
        'readinessProbe': {
            'httpGet': {
                'path': _LB_HEALTH_PATH,
                'port': constants.LOAD_BALANCER_PORT_START,
                **_probe_scheme(),
            },
            'periodSeconds': 2,
            'failureThreshold': 1,
            # A TLS handshake on every probe needs more than the plaintext
            # budget, and readiness has failureThreshold 1: one slow handshake
            # would pull a healthy pod out of the Service endpoints.
            'timeoutSeconds': 3 if _pod_serves_tls() else 1,
        },
        'livenessProbe': {
            'httpGet': {
                'path': constants.LB_LIVENESS_ENDPOINT_PATH,
                'port': constants.LOAD_BALANCER_PORT_START,
                **_probe_scheme(),
            },
            'periodSeconds': 10,
            'failureThreshold': 3,
            'timeoutSeconds': 3 if _pod_serves_tls() else 1,
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
    ] + _replica_tls_envs() + ([{
        'name': constants.LB_IMAGE_DIGEST_ENV_VAR,
        'value': controller_image_digest,
    }] if controller_image_digest is not None else []) + ([{
        'name': constants.LB_SLOT_ENV_VAR,
        'value': slot.value,
    }] if slot is not None else [])
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
    if slot is not None:
        pod_labels[LB_SLOT_LABEL_KEY] = slot.value
    template_metadata = {'labels': pod_labels}
    template_annotations = {}
    if controller_image_digest:
        template_annotations[
            CONTROLLER_DIGEST_ANNOTATION] = controller_image_digest
    if slot is not None:
        template_annotations[LB_RUNTIME_REVISION_ANNOTATION] = (
            _lb_runtime_revision(controller_image_digest,
                                 termination_grace_period_seconds, service_hash,
                                 priority_class_name))
    if template_annotations:
        template_metadata['annotations'] = template_annotations
    pod_spec = {
        **_lb_pod_runtime_fields(pod_runtime_fields, service_name, service_hash, slot),
        'terminationGracePeriodSeconds': termination_grace_period_seconds,
        # LB pods call only the stable HTTP controller proxy. They never need
        # Kubernetes credentials, so do not expose the namespace-scoped
        # service-account token to this data-plane process.
        'automountServiceAccountToken': False,
        **({
            'priorityClassName': priority_class_name,
        } if priority_class_name else {}),
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
            'strategy': ({
                'type': 'Recreate',
            } if slot is not None else {
                'type': 'RollingUpdate',
                'rollingUpdate': {
                    'maxSurge': 1,
                    'maxUnavailable': 0,
                },
            }),
            **({
                'minReadySeconds': _LB_MIN_READY_SECONDS
            } if slot is not None else {}),
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


def _pod_serves_tls() -> bool:
    """Whether the LB pod terminates TLS on its own port.

    True only under HTTPS_ONLY, because the backend-protocol annotation is
    per-Service: during the dual-listen window the plaintext listener would
    otherwise forward cleartext into a TLS-only socket.
    """
    config = external_https_config()
    return config is not None and config.https_only


def _probe_scheme() -> dict[str, str]:
    """``scheme`` for the LB pod's kubelet probes.

    Forgetting this is not a small bug: a TLS-serving pod probed over plaintext
    fails startup, readiness and liveness, so every LB pod CrashLoops and every
    Service loses all its endpoints.
    """
    return {'scheme': 'HTTPS'} if _pod_serves_tls() else {}


def _replica_tls_envs() -> list[dict[str, Any]]:
    """Propagate replica-TLS settings from the controller to the LB pod.

    The LB dials replicas, so it needs the mode and (for pinning) the
    certificate. Only the certificate travels here; the private key goes to
    replicas alone. Forwarding the controller's own values rather than letting
    the LB read config independently is what keeps the two ends in agreement
    about whether replicas speak TLS.
    """
    mode = serve_utils.replica_tls_mode()
    if mode == constants.REPLICA_TLS_MODE_OFF:
        return []
    envs: list[dict[str, Any]] = [{
        'name': constants.REPLICA_TLS_MODE_ENV_VAR,
        'value': mode,
    }]
    certificate_pem = os.environ.get(constants.REPLICA_TLS_CERT_ENV_VAR,
                                     '').strip()
    if certificate_pem:
        envs.append({
            'name': constants.REPLICA_TLS_CERT_ENV_VAR,
            'value': certificate_pem,
        })
    return envs


class ExternalHttpsConfig(NamedTuple):
    """Helm-rendered TLS termination settings for the LB Service."""
    certificate_arn: str
    dns_suffix: str
    ssl_policy: str
    https_only: bool


def external_https_config() -> ExternalHttpsConfig | None:
    """TLS settings for the LB Service, or None when not configured.

    Fails closed on a partial configuration rather than emitting a Service
    that advertises a hostname it cannot serve over TLS, or a certificate on
    a listener nobody can address by name.
    """
    certificate_arn = os.environ.get(
        constants.EXTERNAL_LB_HTTPS_CERT_ARN_ENV_VAR, '').strip()
    dns_suffix = os.environ.get(constants.EXTERNAL_LB_HTTPS_DNS_SUFFIX_ENV_VAR,
                                '').strip().strip('.')
    if not certificate_arn and not dns_suffix:
        return None
    if not certificate_arn or not dns_suffix:
        raise ValueError(
            'External load balancer HTTPS requires both '
            f'{constants.EXTERNAL_LB_HTTPS_CERT_ARN_ENV_VAR} and '
            f'{constants.EXTERNAL_LB_HTTPS_DNS_SUFFIX_ENV_VAR}; got '
            f'certificate={"set" if certificate_arn else "unset"}, '
            f'suffix={"set" if dns_suffix else "unset"}.')
    ssl_policy = os.environ.get(constants.EXTERNAL_LB_HTTPS_SSL_POLICY_ENV_VAR,
                                '').strip()
    https_only = os.environ.get(constants.EXTERNAL_LB_HTTPS_ONLY_ENV_VAR,
                                '').strip().lower() == 'true'
    return ExternalHttpsConfig(certificate_arn=certificate_arn,
                               dns_suffix=dns_suffix,
                               ssl_policy=ssl_policy or
                               constants.DEFAULT_EXTERNAL_LB_SSL_POLICY,
                               https_only=https_only)


def external_https_hostname(config: ExternalHttpsConfig,
                            service_name: str) -> str:
    """Stable public hostname for one service's TLS endpoint.

    Keyed on the incarnation-independent base name, so the hostname survives
    ``serve update`` and a ``down``/``up`` cycle. That is deliberately the same
    identity function as the in-cluster endpoint contract, and it is why this
    hostname is a safe thing for a consumer to hardcode -- unlike the
    generated ``*.elb.amazonaws.com`` name, which changes with the Service.
    """
    return f'{lb_base_name(service_name)}.{config.dns_suffix}'


def _build_service_dict(service_name: str,
                        service_name_k8s: str,
                        deployment_name: str,
                        service_hash: str | None = None,
                        owner_reference: dict | None = None,
                        active_slot: lb_ha.LbSlot | None = None,
                        cutover_generation: int | None = None,
                        desired_runtime_revision: str | None = None) -> dict:
    selector = ({
        LB_SLOT_LABEL_KEY: active_slot.value,
        **({
            SERVICE_HASH_LABEL_KEY: service_hash
        } if service_hash else {}),
    } if active_slot is not None else {
        APP_LABEL_KEY: deployment_name,
        **({
            SERVICE_HASH_LABEL_KEY: service_hash
        } if service_hash else {}),
    })
    annotations = {}
    if active_slot is not None:
        if cutover_generation is None:
            raise ValueError('HA LB Service requires a cutover generation.')
        annotations = {
            ACTIVE_SLOT_ANNOTATION_KEY: active_slot.value,
            CUTOVER_GENERATION_ANNOTATION_KEY: str(cutover_generation),
            **({
                DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_runtime_revision,
            } if desired_runtime_revision is not None else {}),
        }
    https_config = external_https_config()
    ports: list[dict[str, Any]] = [{
        'port': constants.LOAD_BALANCER_PORT_START,
        'targetPort': constants.LOAD_BALANCER_PORT_START,
        'protocol': 'TCP',
    }]
    if https_config is not None:
        # The NLB terminates TLS on 443 and forwards TCP to the pod's existing
        # plaintext port, so the LB process is untouched and no certificate is
        # mounted into any pod. Kubernetes requires every port to be named once
        # a Service has more than one.
        https_port = {
            'name': constants.EXTERNAL_LB_HTTPS_PORT_NAME,
            'port': constants.EXTERNAL_LB_HTTPS_PORT,
            'targetPort': constants.LOAD_BALANCER_PORT_START,
            'protocol': 'TCP',
        }
        if https_config.https_only:
            # Enforcement step: the plaintext listener disappears, so the only
            # way in is TLS. Deliberately separate from enabling TLS so it can
            # be reverted on its own.
            ports = [https_port]
        else:
            ports[0]['name'] = constants.EXTERNAL_LB_HTTP_PORT_NAME
            ports.append(https_port)
        annotations = {
            **annotations,
            _AWS_LB_SSL_CERT_ANNOTATION: https_config.certificate_arn,
            _AWS_LB_SSL_PORTS_ANNOTATION: constants.EXTERNAL_LB_HTTPS_PORT_NAME,
            _AWS_LB_SSL_POLICY_ANNOTATION: https_config.ssl_policy,
            _EXTERNAL_DNS_HOSTNAME_ANNOTATION: external_https_hostname(
                https_config, service_name),
        }
        if https_config.https_only:
            # Only once the plaintext listener is gone: the annotation applies
            # to every target group on the Service, so it cannot coexist with
            # a cleartext 30001 listener.
            annotations[constants.AWS_LB_BACKEND_PROTOCOL_ANNOTATION] = (
                constants.AWS_LB_BACKEND_PROTOCOL_SSL)
    return {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {
            'name': service_name_k8s,
            'labels': _object_labels(service_name, service_hash),
            **({
                'annotations': annotations
            } if annotations else {}),
            **({
                'ownerReferences': [owner_reference]
            } if owner_reference else {}),
        },
        'spec': {
            'type': 'LoadBalancer',
            # Selector-only cutover keeps the allocated ClusterIP and cloud NLB.
            # Cluster policy is part of the qualified failover contract.
            'externalTrafficPolicy': 'Cluster',
            'selector': selector,
            'ports': ports,
        },
    }


def _build_pdb_dict(service_name: str, pdb_name: str, service_hash: str,
                    owner_reference: dict) -> dict:
    """Keep at least one synchronized LB slot during voluntary disruption."""
    return {
        'apiVersion': 'policy/v1',
        'kind': 'PodDisruptionBudget',
        'metadata': {
            'name': pdb_name,
            'labels': _object_labels(service_name, service_hash),
            'ownerReferences': [owner_reference],
        },
        'spec': {
            'minAvailable': 1,
            'selector': {
                'matchLabels': {
                    SERVE_LB_LABEL_KEY: service_name,
                    SERVICE_HASH_LABEL_KEY: service_hash,
                },
            },
        },
    }


def _service_ports_patch(desired_ports: list[dict[str, Any]]) -> list[dict]:
    """Desired Service ports plus deletions for the ones we no longer want.

    ``v1.ServicePort`` merges on ``port``, so a strategic-merge body that simply
    omits a port *keeps* it. Without an explicit deletion, dropping the
    plaintext listener would never converge: the drift check compares the port
    list exactly, sees the stale port, and re-runs the whole create path every
    reconcile interval, forever. The same wedge applies in reverse on rollback.

    The same field-retention wedge applies to ``name``. Adding the TLS listener
    renames the pre-existing plaintext port to ``http`` (Kubernetes requires a
    name once a Service has more than one port). Fully disabling HTTPS again
    wants that port back to unnamed, but a merge body that omits ``name`` keeps
    the stale ``http`` -- so ``_service_has_desired_routing`` sees ``http`` vs
    ``None`` and re-patches forever. An owned port that is desired-but-unnamed
    therefore carries an explicit ``name: None`` so the merge clears it, exactly
    as the dropped port carries an explicit delete.

    Only ports this feature owns are ever touched, so an operator-added port is
    left alone.
    """
    owned = (constants.LOAD_BALANCER_PORT_START,
             constants.EXTERNAL_LB_HTTPS_PORT)
    desired_numbers = {port.get('port') for port in desired_ports}
    patched: list[dict] = []
    for port in desired_ports:
        entry = dict(port)
        if entry.get('port') in owned and 'name' not in entry:
            entry['name'] = None
        patched.append(entry)
    return patched + [{
        'port': port,
        '$patch': 'delete',
    } for port in owned if port not in desired_numbers]


def _service_has_desired_routing(service, desired: dict) -> bool:
    """Whether mutable Service routing fields match the desired contract."""
    if isinstance(service, dict):
        metadata = service.get('metadata', {}) or {}
        spec = service.get('spec', {})
        selector = spec.get('selector', {}) or {}
        ports = spec.get('ports', []) or []
        service_type = spec.get('type') or 'ClusterIP'
        external_traffic_policy = spec.get('externalTrafficPolicy') or 'Cluster'
        annotations = metadata.get('annotations', {}) or {}
    else:
        metadata = getattr(service, 'metadata', None)
        spec = getattr(service, 'spec', None)
        selector = getattr(spec, 'selector', {}) or {}
        ports = getattr(spec, 'ports', None) or []
        service_type = getattr(spec, 'type', None) or 'ClusterIP'
        external_traffic_policy = (getattr(spec, 'external_traffic_policy',
                                           None) or 'Cluster')
        annotations = getattr(metadata, 'annotations', {}) or {}

    def _port_tuple(port) -> tuple[Any, Any, Any, Any]:
        # ``name`` participates so that adding the TLS listener also reconciles
        # the rename of the pre-existing unnamed plaintext port; Kubernetes
        # requires names once a Service has more than one port.
        if isinstance(port, dict):
            return (port.get('name'), port.get('port'), port.get('targetPort'),
                    port.get('protocol', 'TCP'))
        return (getattr(port, 'name', None), getattr(port, 'port', None),
                getattr(port, 'target_port',
                        None), getattr(port, 'protocol', None) or 'TCP')

    desired_spec = desired['spec']
    desired_annotations = desired.get('metadata', {}).get('annotations', {})
    return (service_type == desired_spec['type'] and external_traffic_policy
            == desired_spec.get('externalTrafficPolicy', 'Cluster') and
            selector == desired_spec['selector'] and all(
                annotations.get(key) == value
                for key, value in desired_annotations.items()) and
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
        continue_guard: Callable[[], bool] | None = None) -> None:
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


def _wait_for_lb_service_endpoint(
        core_api: Any,
        namespace: str,
        service_name: str,
        continue_guard: Callable[[], bool] | None = None) -> None:
    """Wait until a LoadBalancer Service publishes a routable address."""
    deadline = (time.monotonic() +
                constants.LB_SERVICE_ENDPOINT_READY_TIMEOUT_SECONDS)
    while time.monotonic() < deadline:
        if continue_guard is not None and not continue_guard():
            raise RuntimeError(
                f'Lost service ownership while waiting for LB Service '
                f'{service_name!r}.')
        try:
            service = core_api.read_namespaced_service(service_name, namespace)
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 404:
                raise
        else:
            if _service_load_balancer_address(service) is not None:
                return
        time.sleep(constants.LB_SERVICE_ENDPOINT_READY_POLL_SECONDS)
    raise RuntimeError(
        f'External load balancer Service {service_name!r} did not publish an '
        f'endpoint within '
        f'{constants.LB_SERVICE_ENDPOINT_READY_TIMEOUT_SECONDS}s. Check '
        f'`kubectl describe service/{service_name} -n {namespace}` and the '
        'cloud load balancer controller logs.')


def _deployment_patch_body(deployment_dict: dict,
                           data_plane_auth_enabled: bool) -> dict:
    """Return a create-compatible strategic patch for one LB Deployment."""
    deployment_patch = copy.deepcopy(deployment_dict)
    pod_spec = deployment_patch['spec']['template']['spec']
    if 'priorityClassName' not in pod_spec:
        # Strategic-merge omission retains an old scalar. Explicit null makes
        # a configured -> compatibility-empty transition remove the class,
        # while the separate create body remains valid and omits the field.
        pod_spec['priorityClassName'] = None
    if data_plane_auth_enabled:
        return deployment_patch
    # Strategic-merge omission does not delete named list entries. Explicitly
    # remove projections left by a prior auth-enabled Deployment.
    container = deployment_patch['spec']['template']['spec']['containers'][0]
    container['env'].append({
        'name': constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
        '$patch': 'delete',
    })
    container['volumeMounts'].append({
        # volumeMounts uses mountPath, not name, as its merge key.
        'mountPath': _LB_DATA_PLANE_AUTH_MOUNT_PATH,
        '$patch': 'delete',
    })
    deployment_patch['spec']['template']['spec']['volumes'].append({
        'name': LB_DATA_PLANE_AUTH_VOLUME_NAME,
        '$patch': 'delete',
    })
    return deployment_patch


def _reconcile_owned_deployment(
    context: str,
    namespace: str,
    deployment_dict: dict,
    deployment_patch: dict,
    owner_reference: dict,
    service_hash: str,
    assert_continues: Callable[[str], None],
) -> None:
    """Create or fenced-patch one LB Deployment."""
    deployment_name = deployment_dict['metadata']['name']
    apps_api = kubernetes.apps_api(context)
    deadline = time.monotonic() + _LB_OBJECT_RECONCILIATION_TIMEOUT_SECONDS
    while True:
        assert_continues(f'creating LB Deployment {deployment_name!r}')
        try:
            apps_api.create_namespaced_deployment(namespace, deployment_dict)
            return
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 409:
                raise
        try:
            existing = apps_api.read_namespaced_deployment(
                deployment_name, namespace)
            metadata = (existing.get('metadata', {}) if isinstance(
                existing, dict) else getattr(existing, 'metadata', None))
            deletion_timestamp = (metadata.get('deletionTimestamp')
                                  if isinstance(metadata, dict) else getattr(
                                      metadata, 'deletion_timestamp', None))
            if deletion_timestamp is not None:
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f'LB Deployment {deployment_name!r} stayed '
                        'terminating during reconciliation.')
                time.sleep(_LB_OBJECT_RECONCILIATION_POLL_SECONDS)
                continue
            resource_version = _require_existing_lb_object_ownership(
                context, namespace, deployment_name, existing, owner_reference,
                service_hash)
            desired_patch = copy.deepcopy(deployment_patch)
            desired_patch['metadata'].pop('ownerReferences', None)
            desired_patch['metadata']['resourceVersion'] = resource_version
            _strategic_merge_patch(
                context,
                '/apis/apps/v1/namespaces/{namespace}/deployments/{name}',
                'V1Deployment', deployment_name, namespace, desired_patch)
            return
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) not in (404, 409):
                raise
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f'LB Deployment {deployment_name!r} stayed terminating '
                    'or disappearing during reconciliation.') from e
            time.sleep(_LB_OBJECT_RECONCILIATION_POLL_SECONDS)


def _reconcile_owned_pdb(context: str, namespace: str, pdb_dict: dict,
                         owner_reference: dict, service_hash: str,
                         assert_continues: Callable[[str], None]) -> None:
    """Create the HA PDB or validate its immutable owned specification."""
    name = pdb_dict['metadata']['name']
    policy_api = kubernetes.policy_api(context)
    deadline = time.monotonic() + _LB_OBJECT_RECONCILIATION_TIMEOUT_SECONDS
    while True:
        assert_continues(f'creating LB PodDisruptionBudget {name!r}')
        try:
            policy_api.create_namespaced_pod_disruption_budget(
                namespace, pdb_dict)
            return
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 409:
                raise
        try:
            existing = policy_api.read_namespaced_pod_disruption_budget(
                name, namespace)
            _require_existing_lb_object_ownership(context, namespace, name,
                                                  existing, owner_reference,
                                                  service_hash)
            existing_spec = _serialize_k8s_object(existing).get('spec', {})
            existing_selector = existing_spec.get('selector', {})
            existing_contract = {
                'minAvailable': existing_spec.get(
                    'minAvailable', existing_spec.get('min_available')),
                'matchLabels': existing_selector.get(
                    'matchLabels', existing_selector.get('match_labels')),
            }
            desired_contract = {
                'minAvailable': pdb_dict['spec']['minAvailable'],
                'matchLabels': pdb_dict['spec']['selector']['matchLabels'],
            }
            if existing_contract != desired_contract:
                raise RuntimeError(
                    f'Owned LB PodDisruptionBudget {name!r} has immutable '
                    'specification drift; refusing an unsafe in-place patch.')
            return
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) not in (404, 409):
                raise
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f'LB PodDisruptionBudget {name!r} stayed terminating or '
                    'disappearing during reconciliation.') from e
            time.sleep(_LB_OBJECT_RECONCILIATION_POLL_SECONDS)


def _reconcile_ha_service(
    context: str,
    namespace: str,
    service_dict: dict,
    owner_reference: dict,
    service_hash: str,
    preserve_existing_selector: bool,
    assert_continues: Callable[[str], None],
) -> bool:
    """Create the stable Service or patch its mutable routing in place."""
    name = service_dict['metadata']['name']
    core_api = kubernetes.core_api(context)
    deadline = time.monotonic() + _LB_OBJECT_RECONCILIATION_TIMEOUT_SECONDS
    while True:
        assert_continues(f'creating stable LB Service {name!r}')
        try:
            core_api.create_namespaced_service(namespace, service_dict)
            return False
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 409:
                raise
        try:
            existing = core_api.read_namespaced_service(name, namespace)
            resource_version = _require_existing_lb_object_ownership(
                context, namespace, name, existing, owner_reference,
                service_hash)
            if preserve_existing_selector:
                desired_revision = service_dict['metadata'].get(
                    'annotations',
                    {}).get(DESIRED_RUNTIME_REVISION_ANNOTATION_KEY)
                if desired_revision is not None:
                    body: dict[str, Any] = {
                        'metadata': {
                            'resourceVersion': resource_version,
                            'annotations': {
                                DESIRED_RUNTIME_REVISION_ANNOTATION_KEY: desired_revision,
                            },
                        },
                    }
                    _strategic_merge_patch(
                        context,
                        '/api/v1/namespaces/{namespace}/services/{name}',
                        'V1Service', name, namespace, body)
                return True
            body = {
                'metadata': {
                    'labels': service_dict['metadata']['labels'],
                    'annotations': service_dict['metadata'].get(
                        'annotations', {}),
                    'resourceVersion': resource_version,
                },
                'spec': {
                    'type': service_dict['spec']['type'],
                    'externalTrafficPolicy': service_dict['spec']
                                             ['externalTrafficPolicy'],
                    # A strategic merge recursively merges maps. Replace the
                    # selector explicitly so a legacy ``app`` key cannot
                    # survive migration and combine with the slot labels into
                    # a selector that matches no Pod.
                    'selector': {
                        '$patch': 'replace',
                        **service_dict['spec']['selector'],
                    },
                    'ports': _service_ports_patch(service_dict['spec']['ports']
                                                 ),
                },
            }
            _strategic_merge_patch(
                context, '/api/v1/namespaces/{namespace}/services/{name}',
                'V1Service', name, namespace, body)
            return True
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) not in (404, 409):
                raise
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f'LB Service {name!r} stayed terminating or disappearing '
                    'during reconciliation.') from e
            time.sleep(_LB_OBJECT_RECONCILIATION_POLL_SECONDS)


def _run_ha_service_reconcile_guarded(
    service_name: str,
    service_hash: str,
    cutover_state: lb_ha.LbCutoverState,
    expected_controller_owner: tuple[int | None, str | None],
    reconcile: Callable[[], bool],
) -> bool:
    """Run one supervision Service reconcile under the durable row fence."""
    if (cutover_state.active_slot is None or
            cutover_state.lifecycle_epoch is None):
        raise RuntimeError('HA LB state lacks selector fencing authority.')
    with serve_state.lb_cutover_kubernetes_guard(
            service_name, service_hash, expected_controller_owner,
            cutover_state.lifecycle_epoch, cutover_state.active_slot,
            cutover_state.generation, cutover_state.phase,
            cutover_state.pending_slot) as guarded:
        if not guarded:
            raise RuntimeError(
                f'HA LB selector authority changed before supervision could '
                f'reconcile {service_name!r}; retrying from fresh state.')
        return reconcile()


def _create_ha_lb_objects(
    service_name: str,
    termination_grace_period_seconds: int,
    service_hash: str,
    cutover_state: lb_ha.LbCutoverState,
    continue_guard: Callable[[], bool] | None,
    resource_scope: str | None,
    expected_controller_owner: tuple[int | None, str | None] | None = None,
) -> None:
    """Reconcile two warm slots without rolling the selected active first."""
    if cutover_state.active_slot is None:
        raise RuntimeError('HA LB state has no active slot.')

    def _assert_continues(phase: str) -> None:
        if continue_guard is not None and not continue_guard():
            raise RuntimeError(f'Lost service ownership before {phase} for '
                               f'{service_name!r}; aborting LB reconciliation.')

    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    owner_reference = _api_deployment_owner_reference(context, namespace)
    controller_pod = _read_controller_pod(namespace, context)
    image, image_pull_policy, controller_digest = _resolve_lb_image(
        namespace, context, pod=controller_pod)
    priority_class_name = _lb_priority_class_name()
    desired_runtime_revision = _lb_runtime_revision(
        controller_digest, termination_grace_period_seconds, service_hash,
        priority_class_name)
    (auth_envs, auth_volumes, auth_mounts, image_pull_secrets,
     pod_runtime_fields, container_runtime_fields,
     data_plane_auth_enabled) = _resolve_lb_auth_projection(namespace,
                                                            context,
                                                            pod=controller_pod)
    resources = _lb_resources()

    apps_api = kubernetes.apps_api(context)
    existing_by_slot: dict[lb_ha.LbSlot, Any | None] = {}
    for slot in lb_ha.LbSlot:
        name = lb_slot_deployment_name(service_name, slot, resource_scope)
        try:
            existing_by_slot[slot] = apps_api.read_namespaced_deployment(
                name, namespace)
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) != 404:
                raise
            existing_by_slot[slot] = None

    # A new service has no selected workload yet, so both slots can be
    # created. During recovery or software upgrades, mutate only the standby;
    # the controller promotes it before the former active is rolled.
    slots_to_reconcile = list(lb_ha.LbSlot)
    if existing_by_slot[cutover_state.active_slot] is not None:
        slots_to_reconcile.remove(cutover_state.active_slot)
    for slot in slots_to_reconcile:
        deployment_name = lb_slot_deployment_name(service_name, slot,
                                                  resource_scope)
        deployment_dict = _build_deployment_dict(
            service_name,
            deployment_name,
            image,
            auth_envs,
            auth_volumes,
            auth_mounts,
            image_pull_secrets,
            pod_runtime_fields,
            container_runtime_fields,
            image_pull_policy,
            termination_grace_period_seconds,
            controller_digest,
            service_hash,
            resources,
            owner_reference,
            slot=slot,
            priority_class_name=priority_class_name)
        _reconcile_owned_deployment(
            context, namespace, deployment_dict,
            _deployment_patch_body(deployment_dict, data_plane_auth_enabled),
            owner_reference, service_hash, _assert_continues)

    service_name_k8s = lb_service_name(service_name, resource_scope)
    service_dict = _build_service_dict(
        service_name,
        service_name_k8s,
        lb_slot_deployment_name(service_name, cutover_state.active_slot,
                                resource_scope),
        service_hash,
        owner_reference,
        active_slot=cutover_state.active_slot,
        cutover_generation=cutover_state.generation,
        desired_runtime_revision=desired_runtime_revision)

    def _reconcile_service() -> bool:
        return _reconcile_ha_service(
            context,
            namespace,
            service_dict,
            owner_reference,
            service_hash,
            preserve_existing_selector=(cutover_state.phase
                                        in (lb_ha.LbCutoverPhase.PREPARING,
                                            lb_ha.LbCutoverPhase.MIGRATING,
                                            lb_ha.LbCutoverPhase.ROLLING_BACK)),
            assert_continues=_assert_continues)

    if expected_controller_owner is None:
        # Initial creation and explicit mode transitions either have no
        # selected HA Service yet or preserve its existing selector. Periodic
        # supervision supplies the durable owner fence below because it can
        # race a live cutover from a stale state snapshot.
        service_existed = _reconcile_service()
    else:
        service_existed = _run_ha_service_reconcile_guarded(
            service_name, service_hash, cutover_state,
            expected_controller_owner, _reconcile_service)
    _reconcile_owned_pdb(
        context, namespace,
        _build_pdb_dict(service_name, lb_pdb_name(service_name, resource_scope),
                        service_hash, owner_reference), owner_reference,
        service_hash, _assert_continues)

    _wait_for_lb_deployment_ready(context,
                                  namespace,
                                  lb_slot_deployment_name(
                                      service_name, cutover_state.active_slot,
                                      resource_scope),
                                  continue_guard=continue_guard)
    # Initial publication waits for the standby too. A later bad standby image
    # must not take down an already selected known-good active.
    if (not service_existed or
            cutover_state.phase is lb_ha.LbCutoverPhase.MIGRATING):
        _wait_for_lb_deployment_ready(context,
                                      namespace,
                                      lb_slot_deployment_name(
                                          service_name,
                                          cutover_state.active_slot.other,
                                          resource_scope),
                                      continue_guard=continue_guard)
    _wait_for_lb_service_endpoint(kubernetes.core_api(context),
                                  namespace,
                                  service_name_k8s,
                                  continue_guard=continue_guard)


def create_lb_deployment_and_service(
        service_name: str,
        termination_grace_period_seconds: int,
        service_hash: str,
        continue_guard: Callable[[], bool] | None = None,
        resource_scope: str | None = None,
        high_availability: bool = False,
        preserve_existing_service_selector: bool = False) -> None:
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
    cutover_state = (serve_state.get_lb_cutover_state(service_name)
                     if high_availability else None)
    if high_availability and (cutover_state is None or
                              not cutover_state.enabled):
        raise RuntimeError(
            f'Service {service_name!r} requested HA LB objects without '
            'durable HA cutover authority.')
    if cutover_state is not None:
        _create_ha_lb_objects(service_name, termination_grace_period_seconds,
                              service_hash, cutover_state, continue_guard,
                              resource_scope)
        return

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
    priority_class_name = _lb_priority_class_name()

    deployment_dict = _build_deployment_dict(
        service_name,
        deployment_name,
        image,
        auth_envs,
        auth_volumes,
        auth_mounts,
        image_pull_secrets,
        pod_runtime_fields,
        container_runtime_fields,
        image_pull_policy,
        termination_grace_period_seconds,
        controller_digest,
        service_hash,
        _lb_resources(),
        owner_reference,
        priority_class_name=priority_class_name)
    service_dict = _build_service_dict(service_name, service_name_k8s,
                                       deployment_name, service_hash,
                                       owner_reference)

    deployment_patch = _deployment_patch_body(deployment_dict,
                                              data_plane_auth_enabled)

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
            _strategic_merge_patch(
                context,
                '/apis/apps/v1/namespaces/{namespace}/deployments/{name}',
                'V1Deployment', deployment_name, namespace, desired_patch)
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
                _strategic_merge_patch(
                    context, '/api/v1/namespaces/{namespace}/services/{name}',
                    'V1Service', service_name_k8s, namespace, {
                        'metadata': {
                            'labels': service_dict['metadata']['labels'],
                            'resourceVersion': resource_version,
                        },
                        'spec': {
                            'selector': {
                                '$patch': 'replace',
                                **service_dict['spec']['selector'],
                            },
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
        if preserve_existing_service_selector:
            _wait_for_lb_service_endpoint(core_api,
                                          namespace,
                                          service_name_k8s,
                                          continue_guard=continue_guard)
            return
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
                _strategic_merge_patch(
                    context, '/api/v1/namespaces/{namespace}/services/{name}',
                    'V1Service', service_name_k8s, namespace, {
                        'metadata': {
                            'labels': service_dict['metadata']['labels'],
                            'resourceVersion': resource_version,
                        },
                        'spec': {
                            'type': service_dict['spec']['type'],
                            'externalTrafficPolicy': service_dict['spec']
                                                     ['externalTrafficPolicy'],
                            'selector': {
                                '$patch': 'replace',
                                **service_dict['spec']['selector'],
                            },
                            'ports': _service_ports_patch(
                                service_dict['spec']['ports']),
                        },
                    })
                break
            except kubernetes.api_exception() as e:
                reconciliation_error = e
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
                        reconciliation_error = create_error
                _retry_reconciliation_or_raise('Service', service_name_k8s,
                                               final_deadline,
                                               reconciliation_error)

    _wait_for_lb_service_endpoint(core_api,
                                  namespace,
                                  service_name_k8s,
                                  continue_guard=continue_guard)


def prepare_lb_mode_transition(
    service_name: str,
    termination_grace_period_seconds: int,
    service_hash: str,
    enable_ha: bool,
    continue_guard: Callable[[], bool] | None = None,
    resource_scope: str | None = None,
) -> None:
    """Create the unselected target topology and wait for sync readiness."""
    if enable_ha:
        create_lb_deployment_and_service(service_name,
                                         termination_grace_period_seconds,
                                         service_hash,
                                         continue_guard=continue_guard,
                                         resource_scope=resource_scope,
                                         high_availability=True)
        return
    create_lb_deployment_and_service(service_name,
                                     termination_grace_period_seconds,
                                     service_hash,
                                     continue_guard=continue_guard,
                                     resource_scope=resource_scope,
                                     high_availability=False,
                                     preserve_existing_service_selector=True)


def cleanup_lb_mode_transition(service_name: str,
                               service_hash: str,
                               enabled_ha: bool,
                               resource_scope: str | None = None) -> None:
    """Remove only the obsolete workload after stable-Service cutover."""
    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    expected_api_deployment_name = _api_deployment_name()
    expected_api_deployment_uid = get_api_deployment_owner_uid(
        require_runtime=True)
    if expected_api_deployment_uid is None:
        raise RuntimeError('LB transition cleanup requires API owner UID.')
    errors: list[Exception] = []
    apps_api = kubernetes.apps_api(context)
    if enabled_ha:
        deployment_names = [lb_deployment_name(service_name, resource_scope)]
    else:
        deployment_names = [
            lb_slot_deployment_name(service_name, slot, resource_scope)
            for slot in lb_ha.LbSlot
        ]
    for deployment_name in deployment_names:
        try:
            _delete_lb_object_if_owned(apps_api.read_namespaced_deployment,
                                       apps_api.delete_namespaced_deployment,
                                       deployment_name, namespace, service_hash,
                                       expected_api_deployment_name,
                                       expected_api_deployment_uid, context,
                                       'Deployment')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
    if not enabled_ha:
        try:
            policy_api = kubernetes.policy_api(context)
            _delete_lb_object_if_owned(
                policy_api.read_namespaced_pod_disruption_budget,
                policy_api.delete_namespaced_pod_disruption_budget,
                lb_pdb_name(service_name, resource_scope), namespace,
                service_hash, expected_api_deployment_name,
                expected_api_deployment_uid, context, 'PodDisruptionBudget')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
    if errors:
        raise errors[0]


def _retry_obsolete_lb_topology_cleanup(service_name: str, service_hash: str,
                                        enabled_ha: bool,
                                        resource_scope: str | None) -> None:
    """Best-effort cleanup retried by every stable supervision pass."""
    try:
        cleanup_lb_mode_transition(service_name, service_hash, enabled_ha,
                                   resource_scope)
    except Exception as e:  # pylint: disable=broad-except
        obsolete = 'legacy deployment' if enabled_ha else 'HA slot topology'
        logger.warning(f'Failed to clean obsolete {obsolete} for '
                       f'{service_name!r}; will retry: {e}')


def ensure_lb_objects_exist(service_name: str,
                            termination_grace_period_seconds: int,
                            service_hash: str,
                            controller_ip: str | None = None,
                            resource_scope: str | None = None,
                            high_availability: bool = False) -> bool:
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
    desired_priority_class_name = _lb_priority_class_name()

    cutover_state = (serve_state.get_lb_cutover_state(service_name)
                     if high_availability else None)
    if high_availability and (cutover_state is None or
                              not cutover_state.enabled):
        raise RuntimeError(
            f'Service {service_name!r} requested HA LB supervision without '
            'durable HA cutover authority.')
    if cutover_state is not None:
        if cutover_state.active_slot is None:
            raise RuntimeError('HA LB state has no active slot.')
        if cutover_state.phase is lb_ha.LbCutoverPhase.ROLLING_BACK:
            prepare_lb_mode_transition(
                service_name,
                termination_grace_period_seconds,
                service_hash,
                False,
                continue_guard=lambda: serve_state.service_owner_matches(
                    service_name, service_hash, (os.getpid(), controller_ip)),
                resource_scope=resource_scope)

        def _read_or_missing_ha(read_fn, name: str):
            try:
                return read_fn(name, namespace), False
            except kubernetes.api_exception() as e:
                if getattr(e, 'status', None) != 404:
                    raise
                return None, True

        deployments: dict[lb_ha.LbSlot, Any] = {}
        digest_by_slot: dict[lb_ha.LbSlot, str | None] = {}
        missing_slots: list[lb_ha.LbSlot] = []
        grace_or_hash_drifted = False
        for slot in lb_ha.LbSlot:
            deployment, missing = _read_or_missing_ha(
                kubernetes.apps_api(context).read_namespaced_deployment,
                lb_slot_deployment_name(service_name, slot, resource_scope))
            if missing:
                missing_slots.append(slot)
                continue
            deployments[slot] = deployment
            if isinstance(deployment, dict):
                pod_spec = deployment.get('spec', {}).get('template', {})
                pod_metadata = pod_spec.get('metadata', {}) or {}
                pod_spec = pod_spec.get('spec', {}) or {}
                existing_grace = pod_spec.get('terminationGracePeriodSeconds')
                existing_priority_class_name = pod_spec.get('priorityClassName')
                labels = pod_metadata.get('labels', {}) or {}
                annotations = pod_metadata.get('annotations', {}) or {}
            else:
                template = getattr(getattr(deployment, 'spec', None),
                                   'template', None)
                existing_grace = getattr(getattr(template, 'spec', None),
                                         'termination_grace_period_seconds',
                                         None)
                existing_priority_class_name = getattr(
                    getattr(template, 'spec', None), 'priority_class_name',
                    None)
                labels = getattr(getattr(template, 'metadata', None), 'labels',
                                 {}) or {}
                annotations = getattr(getattr(template, 'metadata', None),
                                      'annotations', {}) or {}
            digest_by_slot[slot] = annotations.get(CONTROLLER_DIGEST_ANNOTATION)
            grace_or_hash_drifted = grace_or_hash_drifted or (
                existing_grace != termination_grace_period_seconds or
                existing_priority_class_name != desired_priority_class_name or
                labels.get(SERVICE_HASH_LABEL_KEY) != service_hash or
                labels.get(LB_SLOT_LABEL_KEY) != slot.value)

        service, service_missing = _read_or_missing_ha(
            kubernetes.core_api(context).read_namespaced_service,
            lb_service_name(service_name, resource_scope))
        _, pdb_missing = _read_or_missing_ha(
            kubernetes.policy_api(
                context).read_namespaced_pod_disruption_budget,
            lb_pdb_name(service_name, resource_scope))
        desired_service = _build_service_dict(
            service_name,
            lb_service_name(service_name, resource_scope),
            lb_slot_deployment_name(service_name, cutover_state.active_slot,
                                    resource_scope),
            service_hash,
            active_slot=cutover_state.active_slot,
            cutover_generation=cutover_state.generation)
        routing_drifted = (
            service is not None and
            cutover_state.phase is not lb_ha.LbCutoverPhase.PREPARING and
            not _service_has_desired_routing(service, desired_service))
        active = deployments.get(cutover_state.active_slot)
        standby = deployments.get(cutover_state.active_slot.other)
        active_digest = digest_by_slot.get(cutover_state.active_slot)
        standby_digest = digest_by_slot.get(cutover_state.active_slot.other)
        digest_drifted = (cutover_state.phase is lb_ha.LbCutoverPhase.STABLE and
                          active_digest is not None and
                          standby_digest != active_digest)
        if (not missing_slots and not service_missing and not pdb_missing and
                not grace_or_hash_drifted and not digest_drifted and
                not routing_drifted):
            assert service is not None
            ready = (active is not None and standby is not None and
                     _lb_deployment_is_ready(active) and
                     _lb_deployment_is_ready(standby) and
                     _service_load_balancer_address(service) is not None)
            if (ready and cutover_state.phase is lb_ha.LbCutoverPhase.STABLE):
                _retry_obsolete_lb_topology_cleanup(service_name, service_hash,
                                                    True, resource_scope)
            return ready

        owner = serve_state.get_service_controller_owner(service_name,
                                                         include_lb_state=True)
        if (owner is None or owner.get('controller_pid') != os.getpid() or
                owner.get('hash') != service_hash or
            (controller_ip and owner.get('controller_ip') != controller_ip)):
            logger.info(
                f'HA LB objects for {service_name!r} require repair '
                'but this process no longer owns the service; skipping.')
            return False
        logger.warning(
            f'HA LB objects for {service_name!r} require reconciliation '
            f'(missing_slots={[slot.value for slot in missing_slots]}, '
            f'service_missing={service_missing}, pdb_missing={pdb_missing}, '
            f'grace_or_hash_drifted={grace_or_hash_drifted}, '
            f'digest_drifted={digest_drifted}, '
            f'routing_drifted={routing_drifted}).')

        def _still_owns_ha() -> bool:
            return serve_state.service_owner_matches(
                service_name, service_hash, (os.getpid(), controller_ip))

        expected_controller_owner = (owner.get('controller_pid'),
                                     owner.get('controller_ip'))
        _create_ha_lb_objects(
            service_name,
            termination_grace_period_seconds,
            service_hash,
            cutover_state,
            _still_owns_ha,
            resource_scope,
            expected_controller_owner=expected_controller_owner)
        if cutover_state.phase is lb_ha.LbCutoverPhase.STABLE:
            _retry_obsolete_lb_topology_cleanup(service_name, service_hash,
                                                True, resource_scope)
        return True

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
    priority_class_drifted = False
    if deployment is not None:
        if isinstance(deployment, dict):
            existing_pod_spec = deployment.get('spec',
                                               {}).get('template',
                                                       {}).get('spec', {})
            existing_grace = existing_pod_spec.get(
                'terminationGracePeriodSeconds')
            existing_priority_class_name = existing_pod_spec.get(
                'priorityClassName')
        else:
            existing_pod_spec = getattr(
                getattr(deployment.spec, 'template', None), 'spec', None)
            existing_grace = getattr(existing_pod_spec,
                                     'termination_grace_period_seconds', None)
            existing_priority_class_name = getattr(existing_pod_spec,
                                                   'priority_class_name', None)
        grace_drifted = existing_grace != termination_grace_period_seconds
        priority_class_drifted = (existing_priority_class_name
                                  != desired_priority_class_name)
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
            not priority_class_drifted and not hash_drifted and
            not routing_drifted):
        assert deployment is not None
        assert service is not None
        deployment_ready = _lb_deployment_is_ready(deployment)
        endpoint_ready = _service_load_balancer_address(service) is not None
        ready = deployment_ready and endpoint_ready
        if ready:
            _retry_obsolete_lb_topology_cleanup(service_name, service_hash,
                                                False, resource_scope)
        return ready
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
    owner = serve_state.get_service_controller_owner(service_name,
                                                     include_lb_state=True)
    if (owner is None or owner.get('controller_pid') != os.getpid() or
        (service_hash and owner.get('hash') != service_hash) or
        (controller_ip and owner.get('controller_ip') != controller_ip)):
        logger.info(f'External LB objects for {service_name!r} are missing '
                    'but this process no longer owns the service '
                    '(row gone or taken over); skipping recreation.')
        return False
    logger.warning(f'External LB objects for {service_name!r} require '
                   f'reconciliation (deployment_missing={deployment_missing}, '
                   f'service_missing={service_missing}, '
                   f'grace_drifted={grace_drifted}, '
                   f'priority_class_drifted={priority_class_drifted}, '
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
                                     resource_scope=resource_scope,
                                     high_availability=False)
    _retry_obsolete_lb_topology_cleanup(service_name, service_hash, False,
                                        resource_scope)
    return True


def _parse_lb_role_pod_authority(
        pods: Any, service_name: str, resource_scope: str | None,
        selected_slot: lb_ha.LbSlot | None) -> LbPodAuthority:
    """Parse an incarnation-scoped HA Pod list without dropping unknown Pods."""
    live_uids: set[str] = set()
    ready_nonterminating_uids: set[str] = set()
    slot_by_uid: dict[str, lb_ha.LbSlot] = {}
    digest_by_uid: dict[str, str | None] = {}
    revision_by_uid: dict[str, str | None] = {}
    legacy_uids: set[str] = set()
    terminating_uids: set[str] = set()
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
        if terminating is not None:
            terminating_uids.add(uid)
        labels = getattr(metadata, 'labels', {}) or {}
        slot = lb_ha.parse_slot(labels.get(LB_SLOT_LABEL_KEY))
        if slot is None:
            legacy_labeled = labels.get(APP_LABEL_KEY) == lb_deployment_name(
                service_name, resource_scope)
            if not legacy_labeled:
                raise ValueError('live HA load balancer Pod is missing its '
                                 'slot label')
            # A legacy migration/rollback tail remains a possible stream
            # owner even after the stable Service selects an HA slot.
            legacy_uids.add(uid)
        else:
            slot_by_uid[uid] = slot
        annotations = getattr(metadata, 'annotations', {}) or {}
        digest = annotations.get(CONTROLLER_DIGEST_ANNOTATION)
        digest_by_uid[uid] = str(digest) if digest is not None else None
        revision = annotations.get(LB_RUNTIME_REVISION_ANNOTATION)
        revision_by_uid[uid] = (str(revision) if revision is not None else None)
        conditions = getattr(status, 'conditions', None) or []
        ready = any(
            getattr(condition, 'type', None) == 'Ready' and
            getattr(condition, 'status', None) == 'True'
            for condition in conditions)
        if phase == 'Running' and terminating is None and ready:
            ready_nonterminating_uids.add(uid)
    return LbPodAuthority(ready_nonterminating_uids, live_uids, slot_by_uid,
                          selected_slot, digest_by_uid, revision_by_uid,
                          legacy_uids, terminating_uids)


def _lb_service_fields(
        service: Any) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    if isinstance(service, dict):
        spec = service.get('spec', {}) or {}
        annotations = service.get('metadata', {}).get('annotations', {}) or {}
        selector = spec.get('selector', {}) or {}
    else:
        spec = service.spec
        annotations = getattr(service.metadata, 'annotations', {}) or {}
        selector = getattr(spec, 'selector', {}) or {}
    return spec, annotations, selector


def _parse_lb_service_routing(service: Any,
                              resource_version: str) -> LbServiceRouting:
    spec, annotations, selector = _lb_service_fields(service)
    traffic_policy = (spec.get('externalTrafficPolicy') if isinstance(
        spec, dict) else getattr(spec, 'external_traffic_policy', None))
    active_slot = lb_ha.parse_slot(selector.get(LB_SLOT_LABEL_KEY))
    annotated_slot = lb_ha.parse_slot(
        annotations.get(ACTIVE_SLOT_ANNOTATION_KEY))
    raw_generation = annotations.get(CUTOVER_GENERATION_ANNOTATION_KEY)
    desired_runtime_revision = annotations.get(
        DESIRED_RUNTIME_REVISION_ANNOTATION_KEY)
    try:
        generation = int(str(raw_generation))
    except (TypeError, ValueError) as e:
        raise RuntimeError(
            'HA LB Service has a malformed cutover generation.') from e
    if (traffic_policy != 'Cluster' or active_slot is None or
            active_slot != annotated_slot or generation < 1):
        raise RuntimeError(
            'HA LB Service routing authority is malformed or unsupported.')
    if (not isinstance(desired_runtime_revision, str) or
            not re.fullmatch(r'[0-9a-f]{64}', desired_runtime_revision)):
        raise RuntimeError('HA LB Service has a malformed desired runtime '
                           'revision.')
    return LbServiceRouting(active_slot, generation, resource_version,
                            desired_runtime_revision)


def _parse_lb_service_transition_routing(
        service_name: str, resource_scope: str | None, service_hash: str,
        service: Any, resource_version: str) -> LbServiceTransitionRouting:
    _, annotations, selector = _lb_service_fields(service)
    desired_runtime_revision = annotations.get(
        DESIRED_RUNTIME_REVISION_ANNOTATION_KEY)
    if (not isinstance(desired_runtime_revision, str) or
            not re.fullmatch(r'[0-9a-f]{64}', desired_runtime_revision)):
        raise RuntimeError('LB Service has a malformed desired runtime '
                           'revision.')
    active_slot = lb_ha.parse_slot(selector.get(LB_SLOT_LABEL_KEY))
    legacy_selected = selector == {
        APP_LABEL_KEY: lb_deployment_name(service_name, resource_scope),
        SERVICE_HASH_LABEL_KEY: service_hash,
    }
    generation = None
    if active_slot is not None:
        annotated_slot = lb_ha.parse_slot(
            annotations.get(ACTIVE_SLOT_ANNOTATION_KEY))
        raw_generation = annotations.get(CUTOVER_GENERATION_ANNOTATION_KEY)
        try:
            generation = int(str(raw_generation))
        except (TypeError, ValueError) as e:
            raise RuntimeError(
                'HA LB Service has a malformed cutover generation.') from e
        if (active_slot is not annotated_slot or generation < 1 or selector != {
                LB_SLOT_LABEL_KEY: active_slot.value,
                SERVICE_HASH_LABEL_KEY: service_hash,
        }):
            raise RuntimeError('HA LB Service routing authority is malformed.')
    if active_slot is None and not legacy_selected:
        raise RuntimeError('LB Service has neither an exact legacy nor HA '
                           'selector.')
    return LbServiceTransitionRouting(active_slot, legacy_selected, generation,
                                      resource_version,
                                      desired_runtime_revision)


def get_lb_role_snapshot(
        service_name: str,
        expected_fence: tuple[str, tuple[int | None, str | None], int],
        expected_state: lb_ha.LbCutoverState,
        owner: Mapping[str, Any],
        timings: dict[str, float] | None = None) -> LbRoleSnapshot | None:
    """Read one fail-closed Pod and Service authority snapshot for a role.

    The caller supplies the owner and complete cutover state from one database
    row read.  This function performs one Pod list, one Service read, and one
    live Deployment UID validation after the Service read.  The independent
    Pod and Service reads are fully joined before any decision.  The existing
    Service supplies the expected owner identity, so no earlier Deployment
    read is needed to construct that same identity.
    """
    if not _lb_mode_active():
        return None

    def timed(phase: str, function: Callable, *args: Any, **kwargs: Any) -> Any:
        started_at = time.monotonic()
        try:
            return function(*args, **kwargs)
        finally:
            if timings is not None:
                timings[phase] = (timings.get(phase, 0.0) + time.monotonic() -
                                  started_at)

    try:
        expected_hash, expected_owner, expected_epoch = expected_fence
        owner_active_slot = (lb_ha.parse_slot(owner.get('lb_active_slot'))
                             if owner is not None else None)
        owner_pending_slot = (lb_ha.parse_slot(owner.get('lb_pending_slot'))
                              if owner is not None else None)
        owner_phase = (lb_ha.parse_phase(owner.get('lb_cutover_phase'))
                       if owner is not None else None)
        owner_identity = ((owner.get('controller_pid'),
                           owner.get('controller_ip'))
                          if owner is not None else None)
        if (owner is None or not owner.get('lb_ha_enabled') or
                str(owner.get('hash')) != expected_hash or
                owner_identity != expected_owner or
                owner.get('lifecycle_epoch') != expected_epoch or
                expected_state.lifecycle_epoch != expected_epoch or
                owner_active_slot is not expected_state.active_slot or
                owner.get('lb_cutover_generation') != expected_state.generation
                or owner_pending_slot is not expected_state.pending_slot or
                owner_phase is not expected_state.phase):
            raise LbRoleSnapshotStateMismatchError(
                'HA role snapshot owner row changed after the controller '
                'state fence was read.')
        service_hash = expected_hash
        resource_scope = owner.get('resource_scope')
        context = kubernetes.in_cluster_context_name()
        namespace = get_lb_namespace()
        core_api = kubernetes.core_api(context)
        request_timeout = (constants.LB_ROLE_SNAPSHOT_TIMEOUT_SECONDS
                           if expected_state.phase
                           is lb_ha.LbCutoverPhase.STABLE else None)
        label_selector = (f'{SERVE_LB_LABEL_KEY}={service_name},'
                          f'{SERVICE_HASH_LABEL_KEY}={service_hash}')
        name = lb_service_name(service_name, resource_scope)

        def list_pods():
            if request_timeout is None:
                return timed('snapshot_pod_list',
                             core_api.list_namespaced_pod,
                             namespace,
                             label_selector=label_selector)
            return timed('snapshot_pod_list',
                         core_api.list_namespaced_pod,
                         namespace,
                         label_selector=label_selector,
                         _request_timeout=request_timeout)

        def read_service():
            if request_timeout is None:
                return timed('snapshot_service_read',
                             core_api.read_namespaced_service, name, namespace)
            return timed('snapshot_service_read',
                         core_api.read_namespaced_service,
                         name,
                         namespace,
                         _request_timeout=request_timeout)

        # These reads are independent but all are required for one authority
        # snapshot.  Joining them removes their sum from the serialized role
        # path without adding a cache or extending the snapshot's freshness
        # window.  Resolve futures in the historical fail-closed order so a
        # concurrent multi-failure retains deterministic outcome mapping.
        parent_context = contextvars.copy_context()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            pods_future = executor.submit(parent_context.copy().run, list_pods)
            service_future = executor.submit(parent_context.copy().run,
                                             read_service)
            pods = pods_future.result()
            try:
                service = service_future.result()
            except Exception as e:  # pylint: disable=broad-except
                raise LbRoleSnapshotRoutingError(str(e)) from e
        try:
            resource_version = timed(
                'snapshot_ownership_validation',
                _require_existing_lb_object_live_ownership, context, namespace,
                name, service, service_hash, request_timeout)
            if expected_state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                                        lb_ha.LbCutoverPhase.ROLLING_BACK):
                routing: (LbServiceRouting |
                          LbServiceTransitionRouting) = timed(
                              'snapshot_parse_routing',
                              _parse_lb_service_transition_routing,
                              service_name, resource_scope, service_hash,
                              service, resource_version)
            else:
                routing = timed('snapshot_parse_routing',
                                _parse_lb_service_routing, service,
                                resource_version)
        except Exception as e:  # pylint: disable=broad-except
            raise LbRoleSnapshotRoutingError(str(e)) from e
        authority = timed('snapshot_parse_pods', _parse_lb_role_pod_authority,
                          pods, service_name, resource_scope,
                          routing.active_slot)
        return LbRoleSnapshot(authority, routing)
    except (LbRoleSnapshotStateMismatchError, LbRoleSnapshotRoutingError):
        raise
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Failed to read load balancer role authority for '
                       f'{service_name!r}: {e}; role reports will fail closed.')
        return None


def get_lb_pod_authority(service_name: str) -> LbPodAuthority | None:
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
        owner = serve_state.get_service_controller_owner(service_name,
                                                         include_lb_state=True)
        service_hash = owner.get('hash') if owner else None
        resource_scope = owner.get('resource_scope') if owner else None
        ha_enabled = bool(owner and owner.get('lb_ha_enabled'))
        cutover_phase = (lb_ha.parse_phase(owner.get('lb_cutover_phase'))
                         if owner else None)
        if not service_hash:
            logger.warning(f'Cannot determine the active incarnation for '
                           f'{service_name!r}; load balancer reports will '
                           'fail closed.')
            return None
        core_api = kubernetes.core_api(context)
        if ha_enabled:
            label_selector = (f'{SERVE_LB_LABEL_KEY}={service_name},'
                              f'{SERVICE_HASH_LABEL_KEY}={service_hash}')
        else:
            label_selector = (
                f'{APP_LABEL_KEY}='
                f'{lb_deployment_name(service_name, resource_scope)},'
                f'{SERVICE_HASH_LABEL_KEY}={service_hash}')
        pods = core_api.list_namespaced_pod(namespace,
                                            label_selector=label_selector)
        selected_slot: lb_ha.LbSlot | None = None
        legacy_selected = False
        if ha_enabled:
            service_name_k8s = lb_service_name(service_name, resource_scope)
            service = core_api.read_namespaced_service(service_name_k8s,
                                                       namespace)
            _require_existing_lb_object_live_ownership(context, namespace,
                                                       service_name_k8s,
                                                       service, service_hash)
            if isinstance(service, dict):
                selector = service.get('spec', {}).get('selector', {}) or {}
                annotations = service.get('metadata', {}).get(
                    'annotations', {}) or {}
            else:
                selector = getattr(service.spec, 'selector', {}) or {}
                annotations = getattr(service.metadata, 'annotations', {}) or {}
            selected_slot = lb_ha.parse_slot(selector.get(LB_SLOT_LABEL_KEY))
            legacy_selected = selector.get(APP_LABEL_KEY) == lb_deployment_name(
                service_name, resource_scope)
            annotated_slot = lb_ha.parse_slot(
                annotations.get(ACTIVE_SLOT_ANNOTATION_KEY))
            generation = annotations.get(CUTOVER_GENERATION_ANNOTATION_KEY)
            desired_runtime_revision = annotations.get(
                DESIRED_RUNTIME_REVISION_ANNOTATION_KEY)
            transitional_legacy = (cutover_phase
                                   in (lb_ha.LbCutoverPhase.MIGRATING,
                                       lb_ha.LbCutoverPhase.ROLLING_BACK) and
                                   legacy_selected and selected_slot is None)
            if (not isinstance(desired_runtime_revision, str) or
                    not re.fullmatch(r'[0-9a-f]{64}',
                                     desired_runtime_revision)):
                raise ValueError('malformed desired LB runtime revision')
            if (not transitional_legacy and
                (selected_slot is None or selected_slot != annotated_slot or
                 generation is None or not str(generation).isdigit())):
                raise ValueError('malformed HA Service selector or annotations')
    except Exception as e:  # pylint: disable=broad-except
        logger.warning(f'Failed to list load balancer pods for '
                       f'{service_name!r}: {e}; load balancer reports will '
                       'fail closed.')
        return None
    live_uids: set[str] = set()
    ready_nonterminating_uids: set[str] = set()
    slot_by_uid: dict[str, lb_ha.LbSlot] = {}
    digest_by_uid: dict[str, str | None] = {}
    revision_by_uid: dict[str, str | None] = {}
    legacy_uids: set[str] = set()
    terminating_uids: set[str] = set()
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
            if terminating is not None:
                terminating_uids.add(uid)
            if ha_enabled:
                labels = getattr(metadata, 'labels', {}) or {}
                slot = lb_ha.parse_slot(labels.get(LB_SLOT_LABEL_KEY))
                if slot is None:
                    legacy_labeled = labels.get(
                        APP_LABEL_KEY) == lb_deployment_name(
                            service_name, resource_scope)
                    if not legacy_labeled:
                        raise ValueError('live HA load balancer Pod is missing '
                                         'its slot label')
                    # Migration commits the stable HA selector before the
                    # parent supervisor asynchronously deletes the obsolete
                    # legacy Deployment.  During that bounded cleanup window
                    # its Pod is still Running and Ready without a slot label,
                    # but the HA Service no longer selects it.  Keep it as a
                    # legacy stream owner so drain evidence fails closed;
                    # rejecting the whole authority snapshot would also fence
                    # both valid HA slots from role heartbeats and syncs.
                    # Outside a transition this exception is restricted to
                    # the exact legacy Deployment label and incarnation-scoped
                    # list above.  A different slotless Pod remains malformed.
                    legacy_uids.add(uid)
                else:
                    slot_by_uid[uid] = slot
                annotations = getattr(metadata, 'annotations', {}) or {}
                digest = annotations.get(CONTROLLER_DIGEST_ANNOTATION)
                digest_by_uid[uid] = (str(digest)
                                      if digest is not None else None)
                revision = annotations.get(LB_RUNTIME_REVISION_ANNOTATION)
                revision_by_uid[uid] = (str(revision)
                                        if revision is not None else None)
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
    return LbPodAuthority(ready_nonterminating_uids, live_uids,
                          slot_by_uid if ha_enabled else None, selected_slot,
                          digest_by_uid if ha_enabled else None,
                          revision_by_uid if ha_enabled else None,
                          legacy_uids if ha_enabled else None,
                          terminating_uids if ha_enabled else None)


def get_lb_service_routing(service_name: str) -> LbServiceRouting:
    """Read the exact stable-Service selector authority for an HA service."""
    if not _lb_mode_active():
        raise RuntimeError('External load balancer runtime is unavailable.')
    owner = serve_state.get_service_controller_owner(service_name,
                                                     include_lb_state=True)
    if owner is None or not owner.get('lb_ha_enabled') or not owner.get('hash'):
        raise RuntimeError(f'Service {service_name!r} is not HA-enabled.')
    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    name = lb_service_name(service_name, owner.get('resource_scope'))
    service = kubernetes.core_api(context).read_namespaced_service(
        name, namespace)
    owner_reference = _api_deployment_owner_reference(context, namespace)
    resource_version = _require_existing_lb_object_ownership(
        context, namespace, name, service, owner_reference, owner['hash'])
    return _parse_lb_service_routing(service, resource_version)


def get_lb_service_transition_routing(
        service_name: str) -> LbServiceTransitionRouting:
    """Read either the exact legacy selector or a valid HA slot selector."""
    owner = serve_state.get_service_controller_owner(service_name,
                                                     include_lb_state=True)
    if owner is None or not owner.get('hash'):
        raise RuntimeError(f'Service {service_name!r} has no LB authority.')
    service_hash = str(owner['hash'])
    resource_scope = owner.get('resource_scope')
    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    name = lb_service_name(service_name, resource_scope)
    service = kubernetes.core_api(context).read_namespaced_service(
        name, namespace)
    owner_reference = _api_deployment_owner_reference(context, namespace)
    resource_version = _require_existing_lb_object_ownership(
        context, namespace, name, service, owner_reference, service_hash)
    return _parse_lb_service_transition_routing(service_name, resource_scope,
                                                service_hash, service,
                                                resource_version)


def patch_lb_service_active_slot(service_name: str, expected_service_hash: str,
                                 expected_controller_owner: tuple[int | None,
                                                                  str | None],
                                 expected_lifecycle_epoch: int,
                                 expected_active_slot: lb_ha.LbSlot,
                                 expected_generation: int,
                                 target_slot: lb_ha.LbSlot,
                                 target_generation: int) -> bool:
    """Atomically move only the stable Service selector to an armed slot."""
    if target_slot is not expected_active_slot.other:
        raise ValueError('HA Service target must be the opposite slot.')
    if target_generation != expected_generation + 1:
        raise ValueError('HA Service cutover generation must increase by one.')
    with serve_state.lb_cutover_kubernetes_guard(
            service_name, expected_service_hash, expected_controller_owner,
            expected_lifecycle_epoch, expected_active_slot, target_generation,
            lb_ha.LbCutoverPhase.PREPARING, target_slot) as guarded:
        if not guarded:
            return False
        owner = serve_state.get_service_controller_owner(service_name,
                                                         include_lb_state=True)
        if (owner is None or owner.get('hash') != expected_service_hash or
                not owner.get('lb_ha_enabled')):
            return False
        routing = get_lb_service_routing(service_name)
        if (routing.active_slot is not expected_active_slot or
                routing.generation != expected_generation):
            return False
        context = kubernetes.in_cluster_context_name()
        namespace = get_lb_namespace()
        name = lb_service_name(service_name, owner.get('resource_scope'))
        try:
            _strategic_merge_patch(
                context, '/api/v1/namespaces/{namespace}/services/{name}',
                'V1Service', name, namespace, {
                    'metadata': {
                        'annotations': {
                            ACTIVE_SLOT_ANNOTATION_KEY: target_slot.value,
                            CUTOVER_GENERATION_ANNOTATION_KEY:
                                str(target_generation),
                        },
                        'resourceVersion': routing.resource_version,
                    },
                    'spec': {
                        'externalTrafficPolicy': 'Cluster',
                        'selector': {
                            '$patch': 'replace',
                            LB_SLOT_LABEL_KEY: target_slot.value,
                            SERVICE_HASH_LABEL_KEY: expected_service_hash,
                        },
                    },
                })
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) == 409:
                return False
            raise
    return True


def patch_lb_service_aborted_generation(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    target_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Advance Service evidence before aborting an unselected generation."""
    with serve_state.lb_cutover_kubernetes_guard(
            service_name, expected_service_hash, expected_controller_owner,
            expected_lifecycle_epoch, active_slot, generation,
            lb_ha.LbCutoverPhase.PREPARING, target_slot) as guarded:
        if not guarded:
            return False
        owner = serve_state.get_service_controller_owner(service_name,
                                                         include_lb_state=True)
        if owner is None or owner.get('hash') != expected_service_hash:
            return False
        routing = get_lb_service_routing(service_name)
        if (routing.active_slot is not active_slot or
                routing.generation != generation - 1):
            return False
        context = kubernetes.in_cluster_context_name()
        namespace = get_lb_namespace()
        name = lb_service_name(service_name, owner.get('resource_scope'))
        try:
            _strategic_merge_patch(
                context, '/api/v1/namespaces/{namespace}/services/{name}',
                'V1Service', name, namespace, {
                    'metadata': {
                        'annotations': {
                            ACTIVE_SLOT_ANNOTATION_KEY: active_slot.value,
                            CUTOVER_GENERATION_ANNOTATION_KEY: str(generation),
                        },
                        'resourceVersion': routing.resource_version,
                    },
                    'spec': {
                        'externalTrafficPolicy': 'Cluster',
                        'selector': {
                            '$patch': 'replace',
                            LB_SLOT_LABEL_KEY: active_slot.value,
                            SERVICE_HASH_LABEL_KEY: expected_service_hash,
                        },
                    },
                })
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) == 409:
                return False
            raise
    return True


def patch_lb_service_migration_to_slot(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> bool:
    """Move the stable legacy Service to the prepared slot A."""
    active_slot = lb_ha.LbSlot.A
    with serve_state.lb_cutover_kubernetes_guard(
            service_name, expected_service_hash, expected_controller_owner,
            expected_lifecycle_epoch, active_slot, 1,
            lb_ha.LbCutoverPhase.MIGRATING, None) as guarded:
        if not guarded:
            return False
        routing = get_lb_service_transition_routing(service_name)
        if not routing.legacy_selected:
            return routing.active_slot is active_slot and routing.generation == 1
        owner = serve_state.get_service_controller_owner(service_name,
                                                         include_lb_state=True)
        if owner is None:
            return False
        context = kubernetes.in_cluster_context_name()
        namespace = get_lb_namespace()
        name = lb_service_name(service_name, owner.get('resource_scope'))
        try:
            _strategic_merge_patch(
                context, '/api/v1/namespaces/{namespace}/services/{name}',
                'V1Service', name, namespace, {
                    'metadata': {
                        'annotations': {
                            ACTIVE_SLOT_ANNOTATION_KEY: active_slot.value,
                            CUTOVER_GENERATION_ANNOTATION_KEY: '1',
                        },
                        'resourceVersion': routing.resource_version,
                    },
                    'spec': {
                        'externalTrafficPolicy': 'Cluster',
                        'selector': {
                            '$patch': 'replace',
                            LB_SLOT_LABEL_KEY: active_slot.value,
                            SERVICE_HASH_LABEL_KEY: expected_service_hash,
                        },
                    },
                })
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) == 409:
                return False
            raise
    return True


def patch_lb_service_rollback_to_legacy(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Move the stable Service to a synchronized legacy deployment."""
    with serve_state.lb_cutover_kubernetes_guard(
            service_name, expected_service_hash, expected_controller_owner,
            expected_lifecycle_epoch, active_slot, generation,
            lb_ha.LbCutoverPhase.ROLLING_BACK, None) as guarded:
        if not guarded:
            return False
        routing = get_lb_service_transition_routing(service_name)
        if routing.legacy_selected:
            return True
        if (routing.active_slot is not active_slot or
                routing.generation != generation):
            return False
        owner = serve_state.get_service_controller_owner(service_name,
                                                         include_lb_state=True)
        if owner is None:
            return False
        resource_scope = owner.get('resource_scope')
        context = kubernetes.in_cluster_context_name()
        namespace = get_lb_namespace()
        name = lb_service_name(service_name, resource_scope)
        try:
            _strategic_merge_patch(
                context, '/api/v1/namespaces/{namespace}/services/{name}',
                'V1Service', name, namespace, {
                    'metadata': {
                        'annotations': {
                            ACTIVE_SLOT_ANNOTATION_KEY: None,
                            CUTOVER_GENERATION_ANNOTATION_KEY: None,
                        },
                        'resourceVersion': routing.resource_version,
                    },
                    'spec': {
                        'externalTrafficPolicy': 'Cluster',
                        'selector': {
                            '$patch': 'replace',
                            APP_LABEL_KEY: lb_deployment_name(
                                service_name, resource_scope),
                            SERVICE_HASH_LABEL_KEY: expected_service_hash,
                        },
                    },
                })
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) == 409:
                return False
            raise
    return True


def stream_lb_logs(service_name: str, follow: bool, tail: int | None) -> str:
    """Print logs from the current external LB Pod.

    The former in-pod implementation wrote ``load_balancer.log`` beside the
    controller. External-only SkyServe must read the Kubernetes Pod log
    instead, or ``sky serve logs --load-balancer`` silently tails a stale or
    nonexistent file.
    """
    require_external_lb_runtime()
    context = kubernetes.in_cluster_context_name()
    namespace = get_lb_namespace()
    owner = serve_state.get_service_controller_owner(service_name,
                                                     include_lb_state=True)
    service_hash = owner.get('hash') if owner else None
    resource_scope = owner.get('resource_scope') if owner else None
    ha_enabled = bool(owner and owner.get('lb_ha_enabled'))
    if not service_hash:
        return (f'Cannot determine the active service incarnation for '
                f'{service_name!r}.')
    selector = (f'{SERVE_LB_LABEL_KEY}={service_name},'
                f'{SERVICE_HASH_LABEL_KEY}={service_hash}'
                if ha_enabled else f'{APP_LABEL_KEY}='
                f'{lb_deployment_name(service_name, resource_scope)},'
                f'{SERVICE_HASH_LABEL_KEY}={service_hash}')
    pods = kubernetes.core_api(context).list_namespaced_pod(
        namespace, label_selector=selector)
    candidates = [
        pod for pod in pods.items
        if getattr(pod.status, 'phase', None) not in ('Succeeded', 'Failed')
    ]
    if ha_enabled:
        authority = get_lb_pod_authority(service_name)
        if authority is None or authority.selected_slot is None:
            return f'Cannot determine the active LB slot for {service_name!r}.'
        candidates = [
            pod for pod in candidates
            if lb_ha.parse_slot((getattr(pod.metadata, 'labels', {}) or {}).get(
                LB_SLOT_LABEL_KEY)) == authority.selected_slot
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


def get_api_deployment_owner_uid(require_runtime: bool = False) -> str | None:
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
    deadline = time.monotonic() + deletion_timeout_seconds

    def _remaining_seconds() -> float:
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise TimeoutError(
                f'Timed out waiting for LB {kind} {name!r} UID {uid!r} to '
                'be deleted.')
        return remaining_seconds

    while True:
        try:
            remaining = read_fn(name,
                                namespace,
                                _request_timeout=_remaining_seconds())
        except kubernetes.api_exception() as e:
            if getattr(e, 'status', None) == 404:
                return
            raise
        remaining_uid = _lb_object_metadata_value(remaining, 'uid')
        if remaining_uid != uid:
            raise RuntimeError(
                f'LB {kind} {name!r} was replaced while waiting for exact '
                f'UID {uid!r} to disappear (found {remaining_uid!r}).')
        time.sleep(min(0.2, _remaining_seconds()))


def delete_lb_objects(service_name: str,
                      expected_service_hash: str,
                      resource_scope: str | None = None,
                      require_runtime: bool = False,
                      expected_api_deployment_uid: str | None = None,
                      high_availability: bool = False) -> None:
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
    # Remove the disruption guard before deleting either slot. The stable
    # Service is already gone, so no new request can enter while Pods drain.
    if high_availability:
        try:
            policy_api = kubernetes.policy_api(context)
            _delete_lb_object_if_owned(
                policy_api.read_namespaced_pod_disruption_budget,
                policy_api.delete_namespaced_pod_disruption_budget,
                lb_pdb_name(service_name, resource_scope), namespace,
                expected_service_hash, expected_api_deployment_name,
                expected_api_deployment_uid, context, 'PodDisruptionBudget')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
    apps_api = kubernetes.apps_api(context)
    deployment_names: list[str] = []
    if high_availability:
        deployment_names.extend(
            lb_slot_deployment_name(service_name, slot, resource_scope)
            for slot in lb_ha.LbSlot)
    # Also remove the pre-HA Deployment during migration/rollback cleanup.
    deployment_names.append(lb_deployment_name(service_name, resource_scope))
    for deployment_name in deployment_names:
        try:
            _delete_lb_object_if_owned(
                apps_api.read_namespaced_deployment,
                apps_api.delete_namespaced_deployment, deployment_name,
                namespace, expected_service_hash, expected_api_deployment_name,
                expected_api_deployment_uid, context, 'Deployment')
        except Exception as e:  # pylint: disable=broad-except
            errors.append(e)
    if errors:
        raise errors[0]


def reconcile_lb_objects(live_service_names: set[str]) -> None:
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
    try:
        pdbs = kubernetes.policy_api(
            context).list_namespaced_pod_disruption_budget(
                namespace, label_selector=LB_SELECTOR_LABEL)
        pdb_items = list(pdbs.items)
    except Exception as e:  # pylint: disable=broad-except
        # Orphan cleanup is best-effort and repeats on every recovery. Do not
        # make a transient policy API failure prevent reaping Services and
        # Deployments, but preserve the failure in logs.
        logger.warning('Failed to list orphan LB PodDisruptionBudgets: %s', e)
        pdb_items = []
    ha_by_incarnation: dict[tuple[str, str, str | None], bool] = {}
    for lb_object in (list(deployments.items) + list(services.items) +
                      pdb_items):
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
            else:
                matched_scope: str | None = None
                matched = False
                for candidate_scope in (service_hash, None):
                    candidate_names = {
                        lb_base_name(owning_service, candidate_scope),
                        lb_pdb_name(owning_service, candidate_scope),
                        *(lb_slot_deployment_name(owning_service, slot,
                                                  candidate_scope)
                          for slot in lb_ha.LbSlot),
                    }
                    if object_name in candidate_names:
                        matched_scope = candidate_scope
                        matched = True
                        break
                if matched:
                    resource_scope = matched_scope
                else:
                    logger.warning(
                        f'Refusing to reap LB object {object_name!r} for '
                        f'{owning_service!r}: name matches neither its legacy '
                        'nor incarnation-scoped identity.')
                    continue
            key = (owning_service, service_hash, resource_scope)
            ha_names = {
                lb_pdb_name(owning_service, resource_scope),
                *(lb_slot_deployment_name(owning_service, slot, resource_scope)
                  for slot in lb_ha.LbSlot),
            }
            ha_by_incarnation[key] = (ha_by_incarnation.get(key, False) or
                                      object_name in ha_names)
        elif owning_service is not None:
            logger.warning(f'Refusing to reap legacy LB objects for '
                           f'{owning_service!r} without an incarnation label.')

    for ((owning_service, expected_service_hash, resource_scope),
         high_availability) in ha_by_incarnation.items():
        # Name reuse can leave A's scoped objects beside live successor B's.
        # Protect only the exact live incarnation; a different current hash is
        # positive proof that this object belongs to the predecessor and is
        # safe to reap.  The stale name snapshot remains only a cheap hint.
        current_hash = serve_state.get_service_hash(owning_service)
        if current_hash == expected_service_hash:
            continue
        if resource_scope is None:
            kwargs: dict[str, Any] = {
                'expected_api_deployment_uid': expected_api_deployment_uid,
            }
            if high_availability:
                kwargs['high_availability'] = True
            delete_lb_objects(owning_service, expected_service_hash, **kwargs)
        else:
            kwargs = {
                'resource_scope': resource_scope,
                'expected_api_deployment_uid': expected_api_deployment_uid,
            }
            if high_availability:
                kwargs['high_availability'] = True
            delete_lb_objects(owning_service, expected_service_hash, **kwargs)
