"""Arm-time capability preflight for idle-timer teardown.

``sky launch -i <n> --down`` is a promise: the node deletes itself after
<n> idle minutes. The node keeps that promise from the inside --
``StopEvent`` calls ``provision.terminate_instances`` with whatever
identity the node runs as. When that identity may not delete the node,
the teardown raises every 60s forever: ``SkyletEvent.run`` swallows the
exception to keep the skylet alive, and ``_stop_cluster`` clears the
autostopping indicator on the way out, so the API server never observes
``AUTOSTOPPING`` and keeps reporting a serene ``AUTOSTOP  1h (down)``
while the cluster bills indefinitely.

On Kubernetes the node identity is the pod's ServiceAccount, and
SkyPilot only provisions the pod-management RBAC when the pod uses
SkyPilot's own default ServiceAccount -- see
``sky/provision/kubernetes/config.py``, which states the contract: "If
the user has requested a different service account (via pod_config in
~/.sky/config.yaml), we assume they have already set up the necessary
roles and role bindings." A deployment that pins a custom
``kubernetes.pod_config.spec.serviceAccountName`` without that RBAC
loses autodown entirely, silently, for every cluster it launches.

The node is the only party that can answer "may I delete myself?". The
API server's own credentials describe the API server, not the pod, and
``SubjectAccessReview`` -- which would let it ask on the pod's behalf --
is itself a privileged verb it commonly lacks, so a server-side check
degrades to "unknown" exactly where it is needed. A
``SelfSubjectAccessReview`` issued from inside the pod describes the
runtime identity precisely, and every Kubernetes cluster grants
``create selfsubjectaccessreviews`` to ``system:authenticated`` through
the built-in ``system:basic-user`` ClusterRole.

This module is a pure capability probe and holds no policy: it answers
"could this node execute the teardown?" and nothing else. Whether a "no"
should fail the launch is decided by the caller in ``sky/execution.py``,
which is the only place that knows whether the user asked for this
autodown or SkyPilot added it as a leak backstop.

See ``docs/designs/kubernetes-autodown-capability-preflight.md``.
"""
import os
from typing import Any

from sky import sky_logging
from sky.adaptors import kubernetes
from sky.utils import cluster_utils
from sky.utils import yaml_utils

logger = sky_logging.init_logger(__name__)

# Same directory `context_utils.is_incluster_config_available()` probes; we
# read both the token (presence => in-cluster auth) and the namespace, so
# the paths are derived from one constant here rather than split across a
# helper import.
_SERVICE_ACCOUNT_DIR = '/var/run/secrets/kubernetes.io/serviceaccount'
_TOKEN_FILE = os.path.join(_SERVICE_ACCOUNT_DIR, 'token')
_NAMESPACE_FILE = os.path.join(_SERVICE_ACCOUNT_DIR, 'namespace')

# What the idle-timer teardown ultimately needs: autodown on Kubernetes
# deletes the cluster's pods.
_REQUIRED_VERB = 'delete'
_REQUIRED_RESOURCE = 'pods'


def autodown_denial_reason(idle_minutes: int, down: bool) -> str | None:
    """Why this node provably cannot autodown itself, or None.

    ``None`` means "allowed, not applicable, or undeterminable". Only an
    explicit authorization denial is reported: a cluster must never lose
    its autostop because a probe could not be issued.

    Args:
        idle_minutes: the idle threshold being armed. Negative cancels
            autostop, which needs no teardown capability.
        down: whether the armed teardown is an autodown. Plain autostop is
            already rejected for Kubernetes upstream of this check.
    """
    if not down or idle_minutes < 0:
        return None
    if not os.path.exists(_TOKEN_FILE):
        # Not a pod with in-cluster auth: no identity to review.
        return None
    if not _teardown_goes_through_kubernetes():
        return None

    namespace = _read_namespace()
    if namespace is None:
        return None

    try:
        resource_attributes = kubernetes.kubernetes.client.V1ResourceAttributes(
            namespace=namespace,
            verb=_REQUIRED_VERB,
            group='',
            resource=_REQUIRED_RESOURCE)
        body = kubernetes.kubernetes.client.V1SelfSubjectAccessReview(
            spec=kubernetes.kubernetes.client.V1SelfSubjectAccessReviewSpec(
                resource_attributes=resource_attributes))
        review = kubernetes.authz_api(kubernetes.in_cluster_context_name(
        )).create_self_subject_access_review(body=body)
    except Exception as e:  # pylint: disable=broad-except
        # Could not issue the review (no `selfsubjectaccessreviews: create`,
        # a connection error, ...). Degrade to the pre-existing behavior and
        # let the teardown attempt surface any error later.
        logger.warning(
            'Skipping autodown capability preflight: unable to issue '
            f'SelfSubjectAccessReview ({type(e).__name__}: {e}).')
        return None

    status = getattr(review, 'status', None)
    if status is None or status.allowed is None or status.allowed:
        return None
    return (f'the pod ServiceAccount is not allowed to {_REQUIRED_VERB} '
            f'{_REQUIRED_RESOURCE} in namespace {namespace!r}, so the '
            'cluster cannot tear itself down when it goes idle')


def _teardown_goes_through_kubernetes() -> bool:
    """Whether an idle teardown here would use the Kubernetes provisioner.

    Read from the node's own cluster YAML -- the same file
    ``StopEvent._stop_cluster`` reads to pick the cloud.
    """
    config = _read_node_cluster_config()
    if config is None:
        return False
    try:
        return cluster_utils.get_provider_name(config) == 'kubernetes'
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('[autodown-preflight] cannot determine the provider of '
                     f'this node: {type(e).__name__}: {e}')
        return False


def _read_node_cluster_config() -> dict[str, Any] | None:
    """The node's own cluster YAML, or None if it is unreadable."""
    path = os.path.abspath(
        os.path.expanduser(cluster_utils.SKY_CLUSTER_YAML_REMOTE_PATH))
    try:
        config = yaml_utils.read_yaml(path)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(f'[autodown-preflight] cannot read {path}: '
                     f'{type(e).__name__}: {e}')
        return None
    return config if isinstance(config, dict) else None


def _read_namespace() -> str | None:
    try:
        with open(_NAMESPACE_FILE, 'r', encoding='utf-8') as f:
            namespace = f.read().strip()
    except OSError as e:
        logger.debug(f'[autodown-preflight] cannot read {_NAMESPACE_FILE}: '
                     f'{e}')
        return None
    return namespace or None
