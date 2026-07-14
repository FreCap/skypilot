"""Kubernetes pod status diagnosis and remediation hints."""

import typing
from typing import Any

from sky.adaptors import kubernetes

if typing.TYPE_CHECKING:
    from kubernetes.client import models as kubernetes_models


def _iter_terminated_states(cs):
    """Yield a container status's current then previous terminated state.

    Skips states that are absent. Both are checked because an OOMKilled
    container that has since restarted records the kill only in ``last_state``.
    """
    for term in (cs.state.terminated if cs.state else None,
                 cs.last_state.terminated if cs.last_state else None):
        if term is not None:
            yield term


def get_condensed_pod_reason(pod: 'kubernetes_models.V1Pod') -> str:
    """Condense a pod failure into a single-line user-facing summary.

    Checks pod conditions and container statuses to produce a concise reason
    string suitable for display (e.g. 'OOMKilled (exit code 137)'). Always
    returns a string; falls back to 'Terminated unexpectedly' when no specific
    cause is found.
    """
    if pod.status is None:
        return 'Terminated unexpectedly'
    # Check pod conditions for preemption/disruption (highest priority).
    if pod.status.conditions:
        for condition in pod.status.conditions:
            reason = condition.reason or 'Unknown reason'
            message = condition.message or ''
            if condition.type == 'TerminationTarget':
                summary = f'Preempted by Kueue: {reason}'
                if message:
                    summary += f' ({message})'
                return summary
            if condition.type == 'DisruptionTarget':
                summary = f'Disrupted: {reason}'
                if message:
                    summary += f' ({message})'
                return summary

    # Pod-level kubelet reason (e.g. 'Evicted' for ephemeral-storage / disk /
    # memory pressure, 'Preempted', 'Shutdown'). This is the authoritative
    # cause when set; container-level failures (e.g. OOMKilled) do not populate
    # it, so they still fall through to the container checks below.
    pod_status_reason = getattr(pod.status, 'reason', None)
    if pod_status_reason:
        pod_status_message = getattr(pod.status, 'message', None) or ''
        return f'{pod_status_reason}: {pod_status_message}'.rstrip(': ')

    # Check container statuses for waiting states (ImagePullBackOff, etc.).
    if pod.status.container_statuses:
        for cs in pod.status.container_statuses:
            if cs.state.waiting is not None:
                waiting = cs.state.waiting
                if waiting.reason and waiting.reason not in (
                        'ContainerCreating', 'PodInitializing'):
                    msg = waiting.message or ''
                    return f'{waiting.reason}: {msg}'.rstrip(': ')

    # Check container statuses for terminated states (OOMKilled, Error, etc.),
    # both the current state and the previous run (last_state).
    if pod.status.container_statuses:
        for cs in pod.status.container_statuses:
            for term in _iter_terminated_states(cs):
                if term.exit_code != 0:
                    if term.reason:
                        return f'{term.reason} (exit code {term.exit_code})'
                    return f'Terminated with exit code {term.exit_code}'

    return 'Terminated unexpectedly'


def pod_terminated_abnormally(pod: 'kubernetes_models.V1Pod') -> bool:
    """True if the pod failed or any container terminated with a nonzero exit.

    Used to avoid emitting a spurious reason for a pod that merely finished
    successfully (e.g. an exec lost a race against a clean Completed pod).
    """
    if pod.status is None:
        return False
    if pod.status.phase == 'Failed':
        return True
    for cs in (pod.status.container_statuses or []):
        if any(term.exit_code != 0 for term in _iter_terminated_states(cs)):
            return True
        # OOMKilled while restarting shows up as a waiting CrashLoopBackOff.
        waiting = cs.state.waiting if cs.state else None
        if waiting is not None and waiting.reason == 'CrashLoopBackOff':
            return True
    return False


class NodeHealthInfo:
    """Health info for a single Kubernetes node."""

    def __init__(self, issue: str, pods: list[str]):
        self.issue = issue
        self.pods = pods


def _get_pod_health_issues(pod: Any) -> str | None:
    """Check a Running pod for health issues.

    Examines pod conditions and container statuses to detect problems
    that would explain why the pod is Running but not functioning
    (e.g., Ready=False, CrashLoopBackOff).

    Returns None if the pod appears healthy, or a descriptive reason string.
    """
    pod_status = getattr(pod, 'status', None)
    conditions = getattr(pod_status, 'conditions', None)
    if not conditions:
        return None

    ready_condition = None
    for condition in conditions:
        if condition.type == 'Ready':
            ready_condition = condition
            break

    if ready_condition is None or ready_condition.status == 'True':
        return None

    # Pod is not ready — build a reason string
    ready_reason = ready_condition.reason or 'Unknown'
    parts = [f'pod not ready ({ready_reason})']

    # Check container statuses for more specific info
    container_statuses = getattr(pod_status, 'container_statuses', None) or []
    container_issues = []
    for cs in container_statuses:
        if cs.ready:
            continue
        waiting = getattr(cs.state, 'waiting', None)
        terminated = getattr(cs.state, 'terminated', None)
        # A container that was OOMKilled (or otherwise died) and is now
        # restarting records the failure in last_state, not the current state
        # (which may be a generic 'waiting' or already running-again). Surface
        # it so an OOM that briefly blips the cluster into recovery is not
        # masked as a generic 'ray cluster is unhealthy' message.
        last_terminated = cs.last_state.terminated if cs.last_state else None
        prior = None
        if (last_terminated is not None and last_terminated.exit_code != 0 and
                last_terminated.reason):
            prior = (f'{last_terminated.reason} '
                     f'(exit code {last_terminated.exit_code})')
        if waiting and waiting.reason:
            issue = waiting.reason
            if prior is not None:
                issue += f'; previously {prior}'
            container_issues.append(issue)
        elif terminated and terminated.exit_code != 0:
            container_issues.append(f'{terminated.reason or "terminated"}'
                                    f' (exit code {terminated.exit_code})')
        elif prior is not None:
            container_issues.append(prior)

    if container_issues:
        parts.append('; '.join(container_issues))

    return '; '.join(parts)


def _unmask_crashloopbackoff_reason(cs: Any) -> str | None:
    """Return `last_state.terminated.reason` iff cs is in CrashLoopBackOff
    and a previous terminated reason is available; else None.

    Used to surface OOMKilled / Error / etc. instead of bare CrashLoopBackOff.
    """
    waiting = cs.state.waiting if cs.state else None
    if waiting is None or waiting.reason != 'CrashLoopBackOff':
        return None
    last_term = cs.last_state.terminated if cs.last_state else None
    if last_term is None or not last_term.reason:
        return None
    return last_term.reason


def _get_pod_pending_reason_from_container_status(pod: Any) -> str | None:
    """Tier-1 sweep: derive a pending reason from pod.status.container_statuses.

    For each container in turn:
      1. state.waiting: on ContainerCreating/PodInitializing, fall through to
         checks 2 and 3 on the *same* container (a transient-waiting current
         state can coexist with a prior bad termination — surface the prior
         fault); on CrashLoopBackOff, unmask via last_state.terminated; else
         return the waiting reason.
      2. state.terminated: if exit_code != 0, return terminated.reason.
      3. last_state.terminated: if exit_code != 0 and reason present, return
         it (race-window: container restarted between iterations).
    If a container matches none of (1)-(3), advance to the next container.
    Returns None only after exhausting all containers.

    Returns a bare reason string (e.g. "OOMKilled", not
    "OOMKilled (exit 137)") -- the exit-code suffix is intentionally omitted
    because it adds cardinality that defeats nop_if_duplicate dedup on the
    LAUNCH_PROGRESS event.
    """
    container_statuses = getattr(getattr(pod, 'status', None),
                                 'container_statuses', None) or []
    for cs in container_statuses:
        # 1. state.waiting
        waiting = cs.state.waiting if cs.state else None
        if waiting is not None:
            if waiting.reason in ('ContainerCreating', 'PodInitializing'):
                # Transient; fall through to checks 2/3 on this container.
                pass
            elif waiting.reason == 'CrashLoopBackOff':
                unmasked = _unmask_crashloopbackoff_reason(cs)
                return unmasked or 'CrashLoopBackOff'
            else:
                return waiting.reason

        # 2. state.terminated (currently terminated, between restarts)
        terminated = cs.state.terminated if cs.state else None
        if terminated is not None and terminated.exit_code != 0:
            return terminated.reason or 'Terminated'

        # 3. last_state.terminated (previous run terminated badly)
        last_term = cs.last_state.terminated if cs.last_state else None
        if (last_term is not None and last_term.exit_code is not None and
                last_term.exit_code != 0 and last_term.reason):
            return last_term.reason

    return None


# Canonical Kubernetes failure-reason -> remediation hint table, shared by the
# provision-failure formatter (sky/backends/cloud_vm_ray_backend.py, via
# match_kubernetes_failure_hint) and the pod-OOM diagnosis path
# (diagnose_terminated_pod). Each entry maps a list of case-sensitive
# substrings (matched against a failure reason) to a hint. A hint may contain a
# literal `{dashboard_url}` token; callers that can resolve the dashboard URL
# substitute the real URL, others fall back to a generic phrase.
KUBERNETES_FAILURE_HINTS: list[tuple[list[str], str]] = [
    (['ImagePullBackOff', 'ErrImagePull'],
     'Verify the image tag exists and registry credentials are configured.'),
    (['OOMKilled'],
     'The container ran out of memory. Increase the memory request with '
     '`resources.memory` in your task YAML; if `kubernetes.'
     'set_pod_resource_limits` is set, the memory limit scales with it.'),
    # 'ephemeral' must precede 'Evicted': an ephemeral-storage eviction reason
    # contains both, and the first match wins.
    (['ephemeral'],
     'The pod exceeded its ephemeral (local) storage limit and was evicted. '
     'Increase `resources.ephemeral_storage` in your task YAML.'),
    (['Evicted'],
     'The pod was evicted by the node under resource pressure. Increase the '
     'relevant request (`resources.memory` or `resources.ephemeral_storage`) '
     'in your task YAML.'),
    (['Insufficient'],
     'The cluster does not have enough free resources. View node '
     'allocations at {dashboard_url} or run `kubectl describe nodes`.'),
]


def match_kubernetes_failure_hint(reason: str) -> str | None:
    """Return the remediation hint whose substrings match `reason`, or None.

    The returned hint may contain a literal `{dashboard_url}` token for the
    caller to substitute.
    """
    for substrings, hint in KUBERNETES_FAILURE_HINTS:
        if any(s in reason for s in substrings):
            return hint
    return None


def match_kubernetes_failure_hint_text(reason: str) -> str | None:
    """Like match_kubernetes_failure_hint, but ready to display.

    Resolves the `{dashboard_url}` token to a generic phrase, for callers
    without a server context to build a real URL. Returns the hint or None.
    """
    hint = match_kubernetes_failure_hint(reason)
    if hint is None:
        return None
    return hint.replace('{dashboard_url}', 'the SkyPilot dashboard infra page')


def get_failure_hint_reasons() -> list[str]:
    """The reason substrings KUBERNETES_FAILURE_HINTS recognizes, flattened.

    A reason matching one of these names a specific failure cause (since we
    carry a remediation hint for it). Callers that gate work on "is the cause
    already specific" can derive from this instead of duplicating the list.
    """
    return [s for substrings, _ in KUBERNETES_FAILURE_HINTS for s in substrings]


# Substrings that already name a specific failure cause; when a status-derived
# reason contains one, consulting events would add nothing. Every reason we
# carry a remediation hint for is specific by definition, so derive those from
# the canonical hint table; add the few specific reasons that have no hint
# (CrashLoopBackOff and the Kueue/disruption conditions).
_SPECIFIC_FAILURE_REASON_SUBSTRINGS = tuple(
    get_failure_hint_reasons()) + ('CrashLoopBackOff', 'Preempted', 'Disrupted')


def _reason_lacks_specific_cause(reason: str | None) -> bool:
    """Whether `reason` does not already name a specific failure cause."""
    return not reason or not any(s in reason
                                 for s in _SPECIFIC_FAILURE_REASON_SUBSTRINGS)


def diagnose_terminated_pod(context: str | None, namespace: str,
                            pod_name: str) -> str | None:
    """Best-effort diagnosis of a pod that an exec/attach found already gone.

    Reads the pod and, if it terminated abnormally, returns a user-facing
    message including the condensed reason (e.g. OOMKilled) and a remediation
    hint when one applies. Returns None if the pod is healthy, finished
    cleanly, missing, or cannot be read -- this is purely additive context, so
    it must never raise.
    """
    try:
        pod = kubernetes.core_api(context).read_namespaced_pod(
            pod_name, namespace, _request_timeout=kubernetes.API_TIMEOUT)
    except Exception:  # pylint: disable=broad-except
        return None
    if not pod_terminated_abnormally(pod):
        return None
    reason = get_condensed_pod_reason(pod)
    msg = f'Pod {pod_name} terminated: {reason}.'
    hint = match_kubernetes_failure_hint_text(reason)
    if hint is not None:
        msg += f'\nHint: {hint}'
    return msg
