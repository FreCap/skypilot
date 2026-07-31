"""Pure Kubernetes context-selection policy.

Provider discovery stays at the call-site boundary.  This module owns the
deterministic policy that combines one already effective configuration value,
one context inventory, and one environment-options snapshot.  Both legacy
cloud selection and the placement-offer path can therefore use the same
filtering rules without rereading ambient process state or duplicating
configuration precedence.
"""

from collections.abc import Sequence
import dataclasses


@dataclasses.dataclass(frozen=True)
class KubernetesAllowedContextsResolution:
    """Context-policy output, including ordered warning evidence."""

    existing_contexts: tuple[str, ...]
    skipped_contexts: tuple[str, ...]


def resolve_kubernetes_allowed_contexts(
    *,
    effective_allowed_contexts: str | Sequence[str] | None,
    available_contexts: Sequence[str],
    current_context: str | None,
    in_cluster_available: bool,
    in_cluster_context: str | None,
    allow_all_contexts: bool,
    include_in_cluster: bool,
) -> KubernetesAllowedContextsResolution:
    """Resolve allowed contexts from explicitly captured inputs.

    The set conversion and explicit-list iteration intentionally preserve the
    legacy behavior.  In particular, ``allowed_contexts: all`` uses the
    process's set iteration order, while explicit lists preserve order and
    duplicates.  Missing explicit contexts are returned separately so the
    caller can retain legacy warning and ``silent`` behavior without adding a
    logging side effect to this policy owner.
    """
    if not available_contexts:
        return KubernetesAllowedContextsResolution((), ())

    allowed_contexts = effective_allowed_contexts

    contexts_explicitly_set = (allowed_contexts is not None and
                               allowed_contexts != 'all')

    available_context_set = set(available_contexts)
    non_ssh_contexts = [
        context for context in available_context_set
        if not context.startswith('ssh-')
    ]
    existing_context_set = set(non_ssh_contexts)

    allow_all = allowed_contexts == 'all' or (allowed_contexts is None and
                                              allow_all_contexts)
    if allow_all:
        allowed_contexts = non_ssh_contexts

    if allowed_contexts is None:
        selected_context = current_context
        if ((selected_context is None or selected_context.startswith('ssh-'))
                and in_cluster_available):
            if in_cluster_context is None:
                raise ValueError('in-cluster context name is required when '
                                 'in-cluster configuration is available')
            selected_context = in_cluster_context
        allowed_contexts = []
        if selected_context is not None:
            allowed_contexts = [selected_context]

    existing_contexts = []
    skipped_contexts = []
    for context in allowed_contexts:
        if context in existing_context_set:
            existing_contexts.append(context)
        elif not context.startswith('ssh-'):
            skipped_contexts.append(context)

    if (not contexts_explicitly_set and not include_in_cluster):
        if in_cluster_context is None:
            raise ValueError('in-cluster context name is required when '
                             'derived contexts exclude in-cluster')
        existing_contexts = [
            context for context in existing_contexts
            if context != in_cluster_context
        ]

    return KubernetesAllowedContextsResolution(tuple(existing_contexts),
                                               tuple(skipped_contexts))
