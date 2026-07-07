"""Startup RBAC preflight for the external load balancer.

In external-load-balancer mode the SkyServe controller runs in-cluster (inside
the api-server pod) and creates a per-service Deployment + Service for the load
balancer using the pod's ServiceAccount. If that ServiceAccount lacks the
required RBAC verbs, the controller would otherwise come up cleanly and only
fail later, silently, when it tries to create the load balancer -- leaving no
reachable LB.

This module issues SelfSubjectAccessReview checks at controller boot so a
misconfigured cluster fails fast with an actionable error pointing at the helm
`namespaceRules` that grant the missing permissions.
"""
from typing import List, Tuple

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.serve import serve_utils

logger = sky_logging.init_logger(__name__)

# (group, resource, verbs) the controller must be able to exercise for the load
# balancer. Empty group is the core API group (services and pods live there).
# Deployments/services span the full create/reconcile/teardown lifecycle; pods
# only need `get`, because image resolution reads the controller pod
# (read_namespaced_pod) to mirror its image onto the LB Deployment.
_LIFECYCLE_VERBS: List[str] = ['create', 'get', 'list', 'delete']
_REQUIRED_CHECKS: List[Tuple[str, str, List[str]]] = [
    ('apps', 'deployments', _LIFECYCLE_VERBS),
    ('', 'services', _LIFECYCLE_VERBS),
    ('', 'pods', ['get']),
]


def check_lb_rbac_preflight() -> None:
    """Verify the in-cluster ServiceAccount can manage the LB objects.

    No-op unless external-load-balancer mode is enabled AND we are running with
    in-cluster config. Behavior:

    - For each required (verb, group, resource) in the serve namespace, issue a
      SelfSubjectAccessReview. If any review explicitly returns
      ``allowed == False``, raise RuntimeError naming the missing permissions
      and pointing at the helm ``namespaceRules`` fix (fail fast).
    - Graceful degradation: if issuing the review itself fails (e.g. we lack
      ``selfsubjectaccessreviews: create``, or any connection/API error), log a
      warning and return -- fall back to letting the real create attempt surface
      any error later. Only an explicit ``allowed == False`` is fatal.
    """
    if not serve_utils.is_external_load_balancer_mode():
        return
    if not kubernetes_utils.is_incluster_config_available():
        # Not in-cluster: the controller is not using the pod ServiceAccount, so
        # the SelfSubjectAccessReview would not describe the runtime identity.
        return

    context = kubernetes.in_cluster_context_name()
    namespace = kubernetes_utils.get_kube_config_context_namespace(context)

    missing: List[Tuple[str, str]] = []
    for group, resource, verbs in _REQUIRED_CHECKS:
        for verb in verbs:
            resource_attributes = kubernetes.kubernetes.client.\
                V1ResourceAttributes(namespace=namespace,
                                     verb=verb,
                                     group=group,
                                     resource=resource)
            body = kubernetes.kubernetes.client.V1SelfSubjectAccessReview(
                spec=kubernetes.kubernetes.client.V1SelfSubjectAccessReviewSpec(
                    resource_attributes=resource_attributes))
            try:
                review = kubernetes.authz_api(
                    context).create_self_subject_access_review(body=body)
            except Exception as e:  # pylint: disable=broad-except
                # Could not even run the review (no selfsubjectaccessreviews
                # create permission, or a connection error). Degrade gracefully
                # rather than blocking controller startup.
                logger.warning(
                    'Skipping load balancer RBAC preflight: unable to issue '
                    'SelfSubjectAccessReview '
                    f'({type(e).__name__}: {e}). The controller will fall back '
                    'to surfacing any permission error at LB creation time.')
                return
            if review.status is None or not review.status.allowed:
                missing.append((verb, resource))

    if missing:
        missing_str = ', '.join(
            f'{verb} {resource}' for verb, resource in missing)
        raise RuntimeError(
            'External load balancer RBAC preflight failed: the in-cluster '
            f'ServiceAccount is missing the following permissions in namespace '
            f'{namespace!r}: {missing_str}. Grant these verbs on '
            "'apps/deployments', 'services' and 'pods' via the helm chart "
            "'namespaceRules' and redeploy.")
